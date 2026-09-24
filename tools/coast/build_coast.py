"""Coastline data for the overlay's land clip, built from GSHHG 2.3.7 (native binary files).

The browser clips the wave-height and peak-period overlays to the ocean using these polygons,
so the field stops exactly at the coastline. Two tiers, both in the `coast-v1` binary format:

  <out>/index.json            what exists: tier-0 file, tier-1 cells (absent cell = no land)
  <out>/world-i.bin           tier 0: GSHHG intermediate (~1 km), clipped to 30-degree cells,
                              one file; the client uses it for tile zoom <= 6
  <out>/f/<lat>_<lon>.bin     tier 1: GSHHG full resolution clipped to one 5-degree cell whose
                              south-west corner is (lat, lon); fetched on demand for zoom >= 7

Land = GSHHG level 1 (land) + level 5 (Antarctic ice front). Lakes (level 2) are deliberately
land: GFS-Wave carries no wave data on lakes or most inland seas. Polygons are normalised to
-180..180 (rings unwrapped, split at the antimeridian, the Antarctic ring closed through the
pole), oriented counter-clockwise (lon east, lat north), clipped to their cells with exact
boundary points (so neighbouring cells join without seams under a nonzero fill), and quantised
to 1e-4 degree.

coast-v1 file layout (little-endian):
  header (40 B): magic b"CST1", u16 cell_deg, u16 0, u32 q, u32 pieces, u32 rings,
                 u32 vertices, i32 bbox[4] (minx, miny, maxx, maxy in 1/q degree)
  then one LEB128 varint stream; zz(v) = zigzag(v):
     per piece: zz(minx) zz(miny) width height nrings
        per ring: n, then n pairs zz(dx) zz(dy); the first pair is relative to (minx, miny),
                  each next pair to the previous vertex; rings are implicitly closed.

Usage:
  python tools/coast/build_coast.py --zip gshhg-bin-2.3.7.zip --out build/coast/v1
  python tools/coast/build_coast.py --check build/coast/v1
  python tools/coast/build_coast.py --out build/coast/v1 --upload static/coast/v1   (after a build;
         env R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET). The upload refuses a
         prefix that already holds a DIFFERENT build (objects are immutable for a year in browsers
         and at the edge): a changed build goes under a new prefix, or --replace says so explicitly.
         LICENSE.txt (tools/coast/LICENSE-GSHHG.txt) is published beside the data.

GSHHG: Wessel, P., and W. H. F. Smith (1996), A global, self-consistent, hierarchical,
high-resolution shoreline database, J. Geophys. Res., 101(B4), 8741-8743. LGPL-3.0; the derived
files are redistributed under the same licence (see tools/coast/README.md).
"""
import argparse
import hashlib
import json
import os
import struct
import sys
import time
import zipfile

import numpy as np

SOURCE = "GSHHG 2.3.7"
LICENSE = "LGPL-3.0-or-later; Wessel & Smith (1996), J. Geophys. Res. 101(B4) 8741-8743; notice in LICENSE.txt beside this index"
LICENSE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "LICENSE-GSHHG.txt")
GSHHG_HEADER = struct.Struct(">IIIiiiiIIii")          # id n flag west east south north area area_full container ancestor
FILE_HEADER = struct.Struct("<4sHHIIIIiiii")          # magic cell_deg 0 q pieces rings vertices bbox[4]
MAGIC = b"CST1"
Q = 10000                                             # 1e-4 degree (~11 m)
LAND_LEVELS = (1, 5)                                  # land, Antarctic ice front
TIER0 = {"res": "i", "cell": 30, "file": "world-i.bin", "max_zoom": 6, "min_area_km2": 0.1}
TIER1 = {"res": "f", "cell": 5, "dir": "f", "min_zoom": 7, "min_area_km2": 0.0}
MIN_RING_AREA = 1e-12                                 # square degrees; below this a clipped piece is a sliver
# Everything that changes the bytes of a build: a different key means a different prefix version.
FORMAT_KEY = "coast-v1|q=%d|levels=%s|min_area=%g|tier0=%s/%d/%g|tier1=%s/%d/%g" % (
    Q, ",".join(map(str, LAND_LEVELS)), MIN_RING_AREA, TIER0["res"], TIER0["cell"], TIER0["min_area_km2"],
    TIER1["res"], TIER1["cell"], TIER1["min_area_km2"])


# ---------------------------------------------------------------- GSHHG reader

def read_gshhg(buf, levels=LAND_LEVELS):
    """Yield dict(id, level, area_km2, greenwich, x, y) for every polygon of the wanted levels;
    x/y float64 degrees exactly as stored (0..360 or -180..180 depending on the polygon)."""
    off, end = 0, len(buf)
    version = None
    while off < end:
        if end - off < GSHHG_HEADER.size:
            raise ValueError("truncated GSHHG header at byte %d" % off)
        pid, n, flag, _w, _e, _s, _n, area, _af, _cont, _anc = GSHHG_HEADER.unpack_from(buf, off)
        off += GSHHG_HEADER.size
        ver = (flag >> 8) & 255
        if version is None:
            version = ver
        if ver != version or n > 20_000_000 or end - off < 8 * n:
            raise ValueError("unexpected GSHHG record (byte order or format) at polygon id %d" % pid)
        level = flag & 255
        if level in levels:
            pts = np.frombuffer(buf, dtype=">i4", count=2 * n, offset=off).reshape(n, 2).astype(np.float64) / 1e6
            yield {"id": pid, "level": level, "area_km2": area / 10.0 ** (flag >> 26),
                   "greenwich": (flag >> 16) & 3, "x": pts[:, 0].copy(), "y": pts[:, 1].copy()}
        off += 8 * n


# ---------------------------------------------------------------- ring geometry

def signed_area(x, y):
    """Shoelace area in square degrees; > 0 = counter-clockwise (x east, y north)."""
    return 0.5 * float(np.dot(x, np.roll(y, -1)) - np.dot(np.roll(x, -1), y))


def normalize_ring(x, y):
    """-> list of (x, y) rings inside -180..180, CCW, not explicitly closed. Unwraps the ring,
    closes a ring that winds around a pole (the Antarctic ice front) through that pole, shifts
    it so its west edge lies in [-180, 180) and splits what crosses +180."""
    x = np.asarray(x, np.float64); y = np.asarray(y, np.float64)
    if len(x) > 1 and x[0] == x[-1] and y[0] == y[-1]:
        x, y = x[:-1], y[:-1]
    if len(x) < 3:
        return []
    dx = np.diff(x)
    dx -= 360.0 * np.round(dx / 360.0)
    x = np.concatenate([[x[0]], x[0] + np.cumsum(dx)])
    near = np.abs(np.abs(x) - 180.0) < 1e-9             # vertices on the dateline land exactly on it
    x[near] = np.sign(x[near]) * 180.0
    if abs(x[0] - x[-1]) > 180.0:                      # the closing edge would jump: winds around a pole
        pole = -90.0 if float(np.mean(y)) < 0 else 90.0
        x = np.concatenate([x, [x[-1], x[0]]])
        y = np.concatenate([y, [pole, pole]])
    x = x - 360.0 * np.floor((x.min() + 180.0) / 360.0)
    if signed_area(x, y) < 0:
        x, y = x[::-1].copy(), y[::-1].copy()
    if x.max() <= 180.0:
        return [(x, y)]
    out = []
    for part in (clip_half(x, y, 0, 180.0, True), clip_half(x - 360.0, y, 0, -180.0, False)):
        if part is not None:
            out.append(part)
    return out


def clip_half(x, y, axis, c, keep_le):
    """Sutherland-Hodgman against one axis-aligned line: keep coord <= c (keep_le) or >= c.
    Vectorised; intersection points lie exactly on the line. Concave rings come back as one
    ring whose separate lobes are joined by zero-width edges along the line (zero area under a
    nonzero fill). Returns (x, y) or None when nothing (or only a sliver) remains."""
    a = x if axis == 0 else y
    ins = (a <= c) if keep_le else (a >= c)
    if ins.all():
        return x, y
    if not ins.any():
        return None
    xn, yn, an, insn = np.roll(x, -1), np.roll(y, -1), np.roll(a, -1), np.roll(ins, -1)
    cross = ins != insn
    emit_p = insn                                     # the edge's end point is kept
    count = cross.astype(np.int64) + emit_p.astype(np.int64)
    start = np.concatenate([[0], np.cumsum(count)[:-1]])
    total = int(count.sum())
    ox = np.empty(total); oy = np.empty(total)
    with np.errstate(divide="ignore", invalid="ignore"):   # t is only used on crossing edges (an != a)
        t = (c - a) / (an - a)
        ix = x + t * (xn - x); iy = y + t * (yn - y)
    if axis == 0:
        ix = np.full_like(ix, c)
    else:
        iy = np.full_like(iy, c)
    # entering (outside -> inside): intersection, then the end point; exiting: intersection only
    idx = np.nonzero(cross)[0]
    ox[start[idx]] = ix[idx]; oy[start[idx]] = iy[idx]
    idx = np.nonzero(emit_p)[0]
    pos = start[idx] + cross[idx]
    ox[pos] = xn[idx]; oy[pos] = yn[idx]
    if total < 3 or abs(signed_area(ox, oy)) < MIN_RING_AREA:
        return None
    return ox, oy


def cell_range(lo, hi, cell):
    """Index range [i0, i1] of the cells a closed interval [lo, hi] actually covers."""
    i0 = int(np.floor(lo / cell + 1e-12))
    i1 = int(np.ceil(hi / cell - 1e-12)) - 1
    return i0, max(i0, i1)


def split_cells(x, y, cell):
    """Clip one normalised ring into its cells by recursive bisection along cell boundaries.
    -> list of (lat0, lon0, x, y) with lat0/lon0 the cell's south-west corner in degrees."""
    out = []
    stack = [(x, y)]
    while stack:
        px, py = stack.pop()
        cx0, cx1 = cell_range(px.min(), px.max(), cell)
        cy0, cy1 = cell_range(py.min(), py.max(), cell)
        if cx0 == cx1 and cy0 == cy1:
            out.append((cy0 * cell, cx0 * cell, px, py))
            continue
        if cx1 - cx0 >= cy1 - cy0:
            m = (cx0 + (cx1 - cx0 + 1) // 2) * cell
            parts = (clip_half(px, py, 0, m, True), clip_half(px, py, 0, m, False))
        else:
            m = (cy0 + (cy1 - cy0 + 1) // 2) * cell
            parts = (clip_half(px, py, 1, m, True), clip_half(px, py, 1, m, False))
        stack.extend(p for p in parts if p is not None)
    return out


def quantize_ring(x, y, q=Q):
    """Integer ring: consecutive duplicates (incl. the implicit closure) removed; None if < 3
    distinct vertices or zero area remain."""
    ix = np.round(x * q).astype(np.int64); iy = np.round(y * q).astype(np.int64)
    keep = np.ones(len(ix), bool)
    keep[1:] = (ix[1:] != ix[:-1]) | (iy[1:] != iy[:-1])
    ix, iy = ix[keep], iy[keep]
    while len(ix) > 1 and ix[-1] == ix[0] and iy[-1] == iy[0]:
        ix, iy = ix[:-1], iy[:-1]
    if len(ix) < 3 or signed_area(ix.astype(np.float64), iy.astype(np.float64)) == 0:
        return None
    return ix, iy


# ---------------------------------------------------------------- coast-v1 encoding

def zigzag(v):
    v = np.asarray(v, np.int64)
    return ((v << 1) ^ (v >> 63)).astype(np.uint64)


def varints(values):
    """LEB128 of an array of non-negative integers (< 2**35), vectorised."""
    v = np.asarray(values, np.uint64)
    nb = np.ones(len(v), np.int64)
    for k in range(1, 5):
        nb += v >= (np.uint64(1) << np.uint64(7 * k))
    pos = np.concatenate([[0], np.cumsum(nb)[:-1]])
    out = np.zeros(int(nb.sum()), np.uint8)
    for k in range(5):
        m = nb > k
        byte = ((v[m] >> np.uint64(7 * k)) & np.uint64(0x7F)).astype(np.uint8)
        byte |= np.where(nb[m] > k + 1, 0x80, 0).astype(np.uint8)
        out[pos[m] + k] = byte
    return out.tobytes()


def encode_file(pieces, cell_deg, q=Q):
    """pieces: list of rings-lists, each ring an (ix, iy) int64 pair (quantised). -> bytes."""
    stream, n_rings, n_verts = [], 0, 0
    bx0 = by0 = 2 ** 31 - 1; bx1 = by1 = -2 ** 31
    for rings in pieces:
        minx = min(int(r[0].min()) for r in rings); miny = min(int(r[1].min()) for r in rings)
        maxx = max(int(r[0].max()) for r in rings); maxy = max(int(r[1].max()) for r in rings)
        bx0, by0, bx1, by1 = min(bx0, minx), min(by0, miny), max(bx1, maxx), max(by1, maxy)
        stream.append(zigzag([minx, miny]))
        stream.append(np.array([maxx - minx, maxy - miny, len(rings)], np.uint64))
        for ix, iy in rings:
            dx = np.diff(np.concatenate([[minx], ix])); dy = np.diff(np.concatenate([[miny], iy]))
            pairs = np.empty(2 * len(ix), np.int64); pairs[0::2] = dx; pairs[1::2] = dy
            stream.append(np.array([len(ix)], np.uint64)); stream.append(zigzag(pairs))
            n_rings += 1; n_verts += len(ix)
    if not pieces:
        bx0 = by0 = bx1 = by1 = 0
    body = varints(np.concatenate(stream)) if stream else b""
    return FILE_HEADER.pack(MAGIC, cell_deg, 0, q, len(pieces), n_rings, n_verts, bx0, by0, bx1, by1) + body


def decode_file(buf):
    """-> dict(cell, q, bbox, pieces=[{"b": [minx, miny, maxx, maxy], "rings": [(ix, iy)]}])."""
    magic, cell, _z, q, n_pieces, n_rings, n_verts, *bbox = FILE_HEADER.unpack_from(buf, 0)
    if magic != MAGIC:
        raise ValueError("not a coast-v1 file")
    data = np.frombuffer(buf, np.uint8, offset=FILE_HEADER.size)
    # decode every varint at once: ends are bytes without the continuation bit
    ends = np.nonzero(data < 0x80)[0]
    starts = np.concatenate([[0], ends[:-1] + 1])
    vals = np.zeros(len(ends), np.uint64)
    for k in range(5):
        m = starts + k <= ends
        vals[m] |= (data[starts[m] + k].astype(np.uint64) & np.uint64(0x7F)) << np.uint64(7 * k)
    if len(ends) and ends[-1] != len(data) - 1:
        raise ValueError("trailing bytes after the last varint")

    def unzz(v):
        v = v.astype(np.int64)
        return (v >> 1) ^ -(v & 1)

    i, pieces, rings_seen, verts_seen = 0, [], 0, 0
    for _ in range(n_pieces):
        minx, miny = (int(v) for v in unzz(vals[i:i + 2])); w, h, nr = (int(v) for v in vals[i + 2:i + 5]); i += 5
        rings = []
        for _ in range(nr):
            n = int(vals[i]); i += 1
            d = unzz(vals[i:i + 2 * n]); i += 2 * n
            ix = minx + np.cumsum(d[0::2]); iy = miny + np.cumsum(d[1::2])
            rings.append((ix, iy)); rings_seen += 1; verts_seen += n
        pieces.append({"b": [minx, miny, minx + w, miny + h], "rings": rings})
    if i != len(vals) or rings_seen != n_rings or verts_seen != n_verts:
        raise ValueError("coast-v1 counts do not match the stream")
    return {"cell": cell, "q": q, "bbox": bbox, "pieces": pieces}


# ---------------------------------------------------------------- build

def build_tier(polys, cell, min_area_km2, q=Q):
    """-> {(lat0, lon0): [rings-list per piece]} for one resolution."""
    cells = {}
    for p in polys:
        if p["area_km2"] < min_area_km2:
            continue
        for x, y in normalize_ring(p["x"], p["y"]):
            for lat0, lon0, cx, cy in split_cells(x, y, cell):
                r = quantize_ring(cx, cy, q)
                if r is not None:
                    cells.setdefault((lat0, lon0), []).append([r])
    return cells


def cell_name(lat0, lon0):
    return "%d_%d" % (lat0, lon0)


def build(zip_path, out_dir, log=print):
    t0 = time.time()
    z = zipfile.ZipFile(zip_path)
    os.makedirs(os.path.join(out_dir, TIER1["dir"]), exist_ok=True)
    index = {"format": "coast-v1", "format_key": FORMAT_KEY, "source": SOURCE, "license": LICENSE, "q": Q,
             "land_levels": list(LAND_LEVELS), "built_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
             "source_sha256": hashlib.sha256(open(zip_path, "rb").read()).hexdigest()}
    polys = list(read_gshhg(z.read("gshhs_%s.b" % TIER0["res"])))
    cells0 = build_tier(polys, TIER0["cell"], TIER0["min_area_km2"])
    pieces0 = [pc for key in sorted(cells0) for pc in cells0[key]]
    body = encode_file(pieces0, TIER0["cell"])
    open(os.path.join(out_dir, TIER0["file"]), "wb").write(body)
    index["tier0"] = {"file": TIER0["file"], "res": TIER0["res"], "cell": TIER0["cell"], "max_zoom": TIER0["max_zoom"],
                      "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                      "pieces": len(pieces0), "vertices": sum(len(r[0]) for pc in pieces0 for r in pc)}
    log("tier 0: %d polygons -> %d pieces, %d vertices, %d bytes (%.1fs)" % (
        len(polys), len(pieces0), index["tier0"]["vertices"], len(body), time.time() - t0))
    del polys
    polys = list(read_gshhg(z.read("gshhs_%s.b" % TIER1["res"])))
    cells1 = build_tier(polys, TIER1["cell"], TIER1["min_area_km2"])
    listing, total_bytes, total_verts = {}, 0, 0
    for (lat0, lon0) in sorted(cells1):
        pieces = cells1[(lat0, lon0)]
        body = encode_file(pieces, TIER1["cell"])
        name = cell_name(lat0, lon0)
        open(os.path.join(out_dir, TIER1["dir"], name + ".bin"), "wb").write(body)
        nv = sum(len(r[0]) for pc in pieces for r in pc)
        listing[name] = [len(body), nv]
        total_bytes += len(body); total_verts += nv
    index["tier1"] = {"dir": TIER1["dir"], "res": TIER1["res"], "cell": TIER1["cell"], "min_zoom": TIER1["min_zoom"],
                      "cells": listing, "bytes": total_bytes, "vertices": total_verts}
    big = sorted(listing.items(), key=lambda kv: -kv[1][0])[:5]
    log("tier 1: %d polygons -> %d cells, %d vertices, %d bytes; largest %s (%.1fs)" % (
        len(polys), len(listing), total_verts, total_bytes, big, time.time() - t0))
    with open(os.path.join(out_dir, "index.json"), "w") as fh:
        json.dump(index, fh, separators=(",", ":"), sort_keys=True)
    return index


# ---------------------------------------------------------------- check

def check_file(buf, cell_expected=None, cell_box=None):
    """Problems (strings) for one coast-v1 file; cell_box = (lat0, lon0, cell) for tier-1 cells."""
    problems = []
    try:
        d = decode_file(buf)
    except Exception as exc:                         # noqa: BLE001
        return ["decode failed: %s" % exc]
    if cell_expected is not None and d["cell"] != cell_expected:
        problems.append("cell size %d != %d" % (d["cell"], cell_expected))
    q = d["q"]
    for k, pc in enumerate(d["pieces"]):
        for ix, iy in pc["rings"]:
            if len(ix) < 3:
                problems.append("piece %d: ring with %d vertices" % (k, len(ix)))
            if signed_area(ix.astype(np.float64), iy.astype(np.float64)) <= 0:
                problems.append("piece %d: ring not counter-clockwise" % k)
            if ix.min() < -180 * q or ix.max() > 180 * q or iy.min() < -90 * q or iy.max() > 90 * q:
                problems.append("piece %d: coordinates out of range" % k)
            if cell_box is not None:
                lat0, lon0, cell = cell_box
                if ix.min() < lon0 * q or ix.max() > (lon0 + cell) * q or iy.min() < lat0 * q or iy.max() > (lat0 + cell) * q:
                    problems.append("piece %d: outside its cell %s" % (k, cell_name(lat0, lon0)))
        b = [min(int(r[0].min()) for r in pc["rings"]), min(int(r[1].min()) for r in pc["rings"]),
             max(int(r[0].max()) for r in pc["rings"]), max(int(r[1].max()) for r in pc["rings"])]
        if b != pc["b"]:
            problems.append("piece %d: bbox %s != decoded %s" % (k, pc["b"], b))
        if len(problems) > 20:
            break
    return problems


def point_in_pieces(lon, lat, pieces, q=Q):
    """Nonzero winding of (lon, lat) over decoded pieces (integer rings): True = land."""
    px, py = lon * q, lat * q
    w = 0
    for pc in pieces:
        b = pc["b"]
        if px < b[0] or px > b[2] or py < b[1] or py > b[3]:
            continue
        for ix, iy in pc["rings"]:
            x = ix.astype(np.float64); y = iy.astype(np.float64)
            xn, yn = np.roll(x, -1), np.roll(y, -1)
            up = (y <= py) & (yn > py); down = (y > py) & (yn <= py)
            cross = (xn - x) * (py - y) - (px - x) * (yn - y)
            w += int(np.count_nonzero(up & (cross > 0))) - int(np.count_nonzero(down & (cross < 0)))
    return w != 0


# Sanity probes for a finished build: (lon, lat, land?) — a level filter that dropped a continent,
# a broken pole closure or a wrong orientation would fail one of these.
PROBES = [(-158.281, 21.575, False), (-157.86, 21.31, True), (-158.0, 21.45, True), (0.0, -89.9, True),
          (-169.65, 66.083, True), (180.0, -74.0, False), (0.0, -80.0, True), (139.7, 35.7, True), (5.3, 60.39, True),
          (-40.0, -70.0, False), (-90.0, 0.0, False)]
WORLD_LAND_SQDEG = 22100.0                              # tier-1 land area from GSHHG levels 1 + 5
WORLD_MIN_CELLS = 1000                                  # a real build has ~1,470 tier-1 cells; a synthetic test world far fewer


def check(out_dir, log=print):
    index = json.load(open(os.path.join(out_dir, "index.json")))
    problems = []
    if index.get("format") != "coast-v1" or index.get("q") != Q:
        problems.append("index format/q")
    if index.get("format_key") != FORMAT_KEY:
        problems.append("index format_key %r != this builder's %r" % (index.get("format_key"), FORMAT_KEY))
    t0 = index["tier0"]
    buf = open(os.path.join(out_dir, t0["file"]), "rb").read()
    if len(buf) != t0["bytes"] or hashlib.sha256(buf).hexdigest() != t0["sha256"]:
        problems.append("tier 0 size/hash mismatch")
    problems += ["tier 0: " + p for p in check_file(buf, t0["cell"])]
    d0 = decode_file(buf)
    if sum(len(r[0]) for pc in d0["pieces"] for r in pc["rings"]) != t0["vertices"]:
        problems.append("tier 0: vertex count != index")
    if not any(pc["b"][1] == -90 * Q for pc in d0["pieces"]):
        problems.append("tier 0: no piece reaches the South Pole (Antarctica missing?)")
    t1 = index["tier1"]
    world = len(t1["cells"]) >= WORLD_MIN_CELLS       # a real build; the area/landmark checks mean nothing for a synthetic test world
    area0 = sum(signed_area(ix / Q, iy / Q) for pc in d0["pieces"] for ix, iy in pc["rings"])
    if world and abs(area0 - WORLD_LAND_SQDEG) > 0.02 * WORLD_LAND_SQDEG:
        problems.append("tier 0: land area %.0f sq deg, expected ~%.0f" % (area0, WORLD_LAND_SQDEG))
    for lon, lat, land in (PROBES if world else []):
        if point_in_pieces(lon, lat, d0["pieces"]) != land:
            problems.append("tier 0: probe %s,%s should be %s" % (lon, lat, "land" if land else "water"))
    present = set(n[:-4] for n in os.listdir(os.path.join(out_dir, t1["dir"])) if n.endswith(".bin"))
    if present != set(t1["cells"]):
        problems.append("tier 1: files %d vs index %d" % (len(present), len(t1["cells"])))
    area1 = 0.0
    for name, (nbytes, nverts) in t1["cells"].items():
        buf = open(os.path.join(out_dir, t1["dir"], name + ".bin"), "rb").read()
        if len(buf) != nbytes:
            problems.append("tier 1 %s: size mismatch" % name)
        lat0, lon0 = (int(v) for v in name.split("_"))
        problems += ["tier 1 %s: %s" % (name, p) for p in check_file(buf, t1["cell"], (lat0, lon0, t1["cell"]))]
        try:
            d = decode_file(buf)
        except Exception:                             # noqa: BLE001  (already reported by check_file)
            continue
        if not d["pieces"]:
            problems.append("tier 1 %s: empty cell file" % name)
        if sum(len(r[0]) for pc in d["pieces"] for r in pc["rings"]) != nverts:
            problems.append("tier 1 %s: vertex count != index" % name)
        area1 += sum(signed_area(ix / Q, iy / Q) for pc in d["pieces"] for ix, iy in pc["rings"])
        for lon, lat, land in (PROBES if world else []):
            if lat0 <= lat < lat0 + t1["cell"] and lon0 <= lon < lon0 + t1["cell"] and point_in_pieces(lon, lat, d["pieces"]) != land:
                problems.append("tier 1 %s: probe %s,%s should be %s" % (name, lon, lat, "land" if land else "water"))
        if len(problems) > 60:
            break
    if world and abs(area1 - WORLD_LAND_SQDEG) > 0.02 * WORLD_LAND_SQDEG:
        problems.append("tier 1: land area %.0f sq deg, expected ~%.0f" % (area1, WORLD_LAND_SQDEG))
    log("check: tier 0 %d pieces / %d vertices / %d B; tier 1 %d cells / %d vertices / %d B; %d problems" % (
        t0["pieces"], t0["vertices"], t0["bytes"], len(t1["cells"]), t1["vertices"], t1["bytes"], len(problems)))
    for p in problems[:50]:
        log("  " + p)
    return problems


# ---------------------------------------------------------------- upload

def redact(message):
    """No account id (the R2 endpoint host), key id or bucket name in a public workflow log."""
    import re
    s = re.sub(r"https?://[^ \t\"'<>]*[.]r2[.]cloudflarestorage[.]com[^ \t\"'<>]*", "<r2-endpoint>", str(message))
    s = re.sub(r"(?<![0-9a-fA-F])[0-9a-f]{32}(?![0-9a-fA-F])", "<redacted>", s)
    b = os.environ.get("R2_BUCKET")
    return s.replace(b, "<bucket>") if b else s


def r2_client():
    import boto3
    from botocore.config import Config
    return boto3.client("s3", endpoint_url="https://%s.r2.cloudflarestorage.com" % os.environ["R2_ACCOUNT_ID"],
                        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto",
                        config=Config(retries={"max_attempts": 5, "mode": "standard"},
                                      request_checksum_calculation="when_required",
                                      response_checksum_validation="when_required"))


def published_index(s3, bucket, prefix):
    """The index.json already under <prefix>/, or None."""
    try:
        r = s3.get_object(Bucket=bucket, Key="%s/index.json" % prefix)
    except Exception as exc:                          # noqa: BLE001
        code = getattr(exc, "response", {}).get("Error", {}).get("Code", "") if hasattr(exc, "response") else ""
        if code in ("NoSuchKey", "404", "NotFound") or exc.__class__.__name__ in ("NoSuchKey", "KeyError"):
            return None
        raise
    return json.loads(r["Body"].read().decode("utf-8"))


def same_build(a, b):
    """Two indexes describe the same bytes: same tier-0 hash and the same tier-1 cell map."""
    return bool(a and b and a.get("tier0", {}).get("sha256") == b.get("tier0", {}).get("sha256") and
                a.get("tier1", {}).get("cells") == b.get("tier1", {}).get("cells"))


def upload(out_dir, prefix, replace=False, log=print, s3=None, bucket=None):
    """Upload LICENSE.txt + the tier files + index.json (last) under <prefix>/ with immutable
    caching. A prefix that already holds the same build gets only LICENSE.txt and index.json
    refreshed; one that holds a different build is refused unless replace=True (the objects are
    immutable for a year in browsers and at the edge: a changed build belongs under a new prefix)."""
    s3 = s3 or r2_client()
    bucket = bucket or os.environ["R2_BUCKET"]
    immutable = "public, max-age=31536000, immutable"
    index = json.load(open(os.path.join(out_dir, "index.json")))
    try:
        existing = published_index(s3, bucket, prefix)
        if existing is not None and not same_build(existing, index):
            if not replace:
                log("refusing: %s/ already holds a different build (tier0 %s vs %s); use a new prefix, or --replace" % (
                    prefix, str(existing.get("tier0", {}).get("sha256"))[:12], index["tier0"]["sha256"][:12]))
                return False
            log("REPLACING a different build under %s/ (--replace)" % prefix)
        data_files = [] if (existing is not None and same_build(existing, index)) else \
            [index["tier0"]["file"]] + ["%s/%s.bin" % (index["tier1"]["dir"], n) for n in sorted(index["tier1"]["cells"])]
        if not data_files:
            log("%s/ already holds this build: refreshing LICENSE.txt and index.json only" % prefix)
        s3.put_object(Bucket=bucket, Key="%s/LICENSE.txt" % prefix, Body=open(LICENSE_FILE, "rb").read(),
                      ContentType="text/plain; charset=utf-8", CacheControl=immutable)
        for i, rel in enumerate(data_files):
            s3.put_object(Bucket=bucket, Key="%s/%s" % (prefix, rel), Body=open(os.path.join(out_dir, rel), "rb").read(),
                          ContentType="application/octet-stream", CacheControl=immutable)
            if i % 200 == 0:
                log("uploaded %d/%d" % (i + 1, len(data_files)))
        s3.put_object(Bucket=bucket, Key="%s/index.json" % prefix, Body=open(os.path.join(out_dir, "index.json"), "rb").read(),
                      ContentType="application/json", CacheControl=immutable)
        back = published_index(s3, bucket, prefix)
        if not same_build(back, index):
            raise RuntimeError("index.json read back does not match the upload")
        log("published %d data files + LICENSE.txt + index.json under %s/ (index read back: tier0 %s, %d cells)" % (
            len(data_files), prefix, back["tier0"]["sha256"][:12], len(back["tier1"]["cells"])))
        return True
    except Exception as exc:                          # noqa: BLE001
        raise RuntimeError("upload failed: %s: %s" % (exc.__class__.__name__, redact(exc))) from None


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--zip", help="gshhg-bin-2.3.7.zip")
    ap.add_argument("--out", help="output directory (build) or directory to upload")
    ap.add_argument("--check", metavar="DIR", help="validate a built directory")
    ap.add_argument("--upload", metavar="PREFIX", help="upload --out to the R2 bucket under PREFIX")
    ap.add_argument("--replace", action="store_true", help="allow overwriting a different build under PREFIX (immutable objects!)")
    a = ap.parse_args(argv)
    if a.check:
        return 1 if check(a.check) else 0
    if a.upload:
        if not a.out:
            ap.error("--upload needs --out")
        if check(a.out):
            print("refusing to upload a directory that fails --check")
            return 1
        return 0 if upload(a.out, a.upload.strip("/"), replace=a.replace) else 1
    if not (a.zip and a.out):
        ap.error("--zip and --out are required to build")
    build(a.zip, a.out)
    return 1 if check(a.out) else 0


if __name__ == "__main__":
    sys.exit(main())
