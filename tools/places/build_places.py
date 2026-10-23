"""Borders and place names for the map's location aids (plan section 51), built from Natural Earth and GeoNames.

Two kinds of data, each in two tiers, all published under static/places/v2/ on the frames bucket (v1: the first build,
2026-10-23, without the name pass; kept, the objects are immutable):

  <out>/index.json              what exists, the sources and their hashes, the counts
  <out>/world-lines.bin         tier 0 (tile zoom <= 7): country borders + the state/province borders Natural Earth
                                shows below zoom 7.5, simplified to SIMPLIFY_DEG, cut into 10-degree pieces
  <out>/world-places.bin        tier 0: the Natural Earth places shown below zoom 7.5 (min_zoom < 7.5)
  <out>/c/<lat>_<lon>.l.bin     tier 1 (tile zoom >= 8): every kept border line in one 10-degree cell, unsimplified
  <out>/c/<lat>_<lon>.p.bin     tier 1: the Natural Earth places from zoom 7.5 and the GeoNames towns in the cell
                                (<lat>_<lon> = the cell's south-west corner; a cell without lines or places has no file)

Lines: Natural Earth 10m admin-0 land boundary lines as kind 0 (settled) or kind 2 (disputed, indefinite, line of
control, frontier: LINE_CLASSES) and 10m admin-1 lines as kind 1, each line with Natural Earth's MIN_ZOOM (x 10) so the
client shows a line from the zoom Natural Earth intends. State lines across water ("boundary indicator") are left
out. Lines are cut at the antimeridian and at every 10-degree cell line with the exact crossing point in both runs,
so neighbouring pieces join without a gap.

Names (V7-3, owner 2026-10-23: "fix spelling errors only"): a Natural Earth place keeps its NAME (local spellings:
Kobenhavn stays), except where its NAME_EN is spelled exactly as GeoNames spells the place (its GEONAMESID entry,
within NAME_FIX_KM) and differs from NAME by at most NAME_FIX_EDITS letters (Natural Earth typos and transliteration
variants: Shenyeng -> Shenyang, Poitier -> Poitiers); plus the hand list NAME_FIXES. The index counts both.

Places: Natural Earth populated places (min_zoom from Natural Earth) and GeoNames cities1000 towns (feature codes
GN_CODES, population >= GN_MIN_POP, no " / " in the name; min_zoom 8 / 9 / 10 by population, GN_ZOOM10). A GeoNames town
is dropped when it IS a Natural Earth place (its id is the place's GEONAMESID), or when a Natural Earth place within
DEDUPE_NAME_KM has its name or ascii name, or one within DEDUPE_KM one of its alternate names (all folded), or when
a larger GeoNames town of the same folded name lies within GN_TWIN_KM. Rank 0 national capital, 1 state capital, 2-5
by population (RANK_POP); records sorted by rank, then population. At most CELL_CAP places per cell (the largest
kept).

Tier 0 stores positions in 1/TIER0_Q degree, tier 1 in 1/Q degree (each file's header carries its q).

lines-v1 (little-endian): header 40 B: b"LNS1", u16 cell_deg, u16 kinds mask, u32 q, u32 pieces, u32 lines,
  u32 vertices, i32 bbox[4] (minx, miny, maxx, maxy in 1/q degree); then one LEB128 varint stream, zz(v) = zigzag:
    per piece: zz(minx) zz(miny) width height kind nlines
      per line: n mz10, then n pairs zz(dx) zz(dy), each relative to the previous vertex of the piece (the first pair of
      a piece relative to (minx, miny); the first pair of each next line relative to the last vertex of the line before).
places-v1 (little-endian): header 36 B: b"PLC1", u16 cell_deg, u16 0, u32 q, u32 count, u32 stream bytes,
  i32 bbox[4]; then a LEB128 stream of count x (zz(lat) zz(lon) rank mz10 pop flags) (1/q degree), then the names,
  UTF-8, joined by "\\n" (in record order; a name holds no control character and at most NAME_MAX bytes).

Usage:
  python tools/places/build_places.py --ne DIR --geonames cities1000.zip --out build/places/v2
  python tools/places/build_places.py --check build/places/v2
  python tools/places/build_places.py --out build/places/v2 --upload static/places/v2   (env R2_ACCOUNT_ID
         R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET; refuses a prefix holding a different build, --replace overrides)
LICENSE.txt (tools/places/LICENSE-PLACES.txt: Natural Earth public domain, GeoNames CC BY 4.0) is published beside the data.
"""
import argparse
import hashlib
import json
import math
import os
import re
import struct
import sys
import time
import unicodedata
import zipfile

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "coast"))
import build_coast as C  # noqa: E402  (zigzag, varints, cells_digest, the upload)

FORMAT = "places-v1"
Q = 10000                                            # 1e-4 degree (tier 1)
TIER0_Q = 1000                                       # 1e-3 degree in tier 0: 111 m, a tenth of a pixel at zoom 7
CELL = 10                                            # degrees per piece / tier-1 cell
TIER0_MAX_ZOOM = 7                                   # tile zoom <= 7 draws tier 0
TIER1_MIN_ZOOM = 8
MZ_SPLIT = 75                                        # min_zoom x 10: below it tier 0 (map zoom < 7.5 = tile zoom <= 7)
SIMPLIFY_DEG = 0.004                                 # tier-0 lines: under a pixel up to zoom 7.49 (~0.0055 degree of latitude a pixel at 45 N)
LINES_TIER0, PLACES_TIER0, CELL_DIR = "world-lines.bin", "world-places.bin", "c"
LINES_HEADER = struct.Struct("<4sHHIIIIiiii")       # magic cell kinds q pieces lines vertices bbox[4]
PLACES_HEADER = struct.Struct("<4sHHIIIiiii")       # magic cell 0 q count stream_bytes bbox[4]
LINES_MAGIC, PLACES_MAGIC = b"LNS1", b"PLC1"
KIND_ADMIN0, KIND_ADMIN1, KIND_DISPUTED = 0, 1, 2
KINDS = (KIND_ADMIN0, KIND_ADMIN1, KIND_DISPUTED)
# Admin-0 classes -> kind. Settled borders are kind 0; disputed, indefinite, ceasefire and frontier lines kind 2 (the
# client draws them differently). Overlay limits, lease limits and unrecognised lines are left out.
ADMIN0_CLASSES = {"International boundary (verify)": KIND_ADMIN0, "Disputed (please verify)": KIND_DISPUTED,
                  "Indefinite (please verify)": KIND_DISPUTED, "Line of control (please verify)": KIND_DISPUTED,
                  "Indeterminant frontier": KIND_DISPUTED}
# Natural Earth classes an admin-1 line by the highest boundary it also marks (a US state line on a census-region edge is
# "statistical", an Italian province line on a region edge is "region"): every class is a state/province line. The
# "boundary indicator" lines (US state lines across Chesapeake Bay, Long Island Sound, the Columbia mouth ...) cross open
# water and are left out (owner, G34a F5).
ADMIN1_CLASSES = {"Admin-1 boundary": KIND_ADMIN1, "Admin-1 region boundary": KIND_ADMIN1,
                  "Admin-1 statistical boundary": KIND_ADMIN1, "Admin-1 statistical meta bounds": KIND_ADMIN1}
NE_COMMIT = "ca96624a56bd078437bca8184e78163e5039ad19"   # nvkelso/natural-earth-vector (2022-06-02, v5.1.2 data)
NE_FILES = {
    "admin0": ("ne_10m_admin_0_boundary_lines_land.geojson", "74d9c16229c095fde65943a9919e337682f044bcebccb120764f38edf3b70f4a"),
    "admin1": ("ne_10m_admin_1_states_provinces_lines.geojson", "1a1f30ccaaf4cc9c4bde34266f0b8cbb955d3a4cf254b756912255f2ec7c75b6"),
    "places": ("ne_10m_populated_places.geojson", "9b8e3de09048ef00dfc70357dbb9fa324493f214b5e0ae4daf1aa79a8d10116b"),
}
GN_FILE = "cities1000.zip"                           # changes daily: its sha256 and date are recorded in the index
GN_CODES = frozenset(("PPLC", "PPLA", "PPLA2", "PPLA3", "PPLA4", "PPLA5", "PPL", "PPLF", "PPLG", "PPLL", "PPLR"))
GN_MIN_POP = 1000
GN_ZOOM10 = ((20000, 80), (5000, 90), (0, 100))      # population -> the zoom (x 10) from which a GeoNames town shows
RANK_POP = ((1000000, 2), (100000, 3), (10000, 4))   # population -> rank; smaller towns rank 5
FLAG_CAPITAL, FLAG_GEONAMES = 1, 2
DEDUPE_KM = 5.0                                      # a Natural Earth place this close with one of the town's alternate names
DEDUPE_NAME_KM = 15.0                                # ... or this close with its own name (Natural Earth points sit up to ~15 km off)
GN_TWIN_KM = 2.0                                     # GeoNames towns of one folded name closer than this: the larger kept
NAME_FIX_KM = 25.0                                   # V7-3: a place's GeoNames entry (by GEONAMESID) counts only this close
NAME_FIX_EDITS = 2                                   # ... and NAME_EN replaces NAME only within this many letters
NAME_FIXES = {"Starsy Oskol": "Stary Oskol"}         # Natural Earth typos the agreement rule misses (GeoNames: "Staryy Oskol")
CELL_CAP = 8000
NAME_MAX = 60                                        # UTF-8 bytes
LICENSE_FILE = os.path.join(HERE, "LICENSE-PLACES.txt")
LICENSE = "Natural Earth: public domain; GeoNames: CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/); notice in LICENSE.txt"
FORMAT_KEY = ("places-v1|q=%d/%d|cell=%d|split=%d|simplify=%g|admin0=%s|admin1=%s|ne=%s|gn=%s/%d/%s|dedupe=id+names/%g|"
              "twin=%g|slash=1|cap=%d|name=%d|rank=%s|namefix=%g/%d/%s"
              % (Q, TIER0_Q, CELL, MZ_SPLIT, SIMPLIFY_DEG, ";".join("%s:%d" % kv for kv in sorted(ADMIN0_CLASSES.items())),
                 ";".join("%s:%d" % kv for kv in sorted(ADMIN1_CLASSES.items())), NE_COMMIT, ",".join(sorted(GN_CODES)),
                 GN_MIN_POP, ",".join("%d:%d" % z for z in GN_ZOOM10), DEDUPE_KM, GN_TWIN_KM, CELL_CAP, NAME_MAX,
                 ",".join("%d:%d" % r for r in RANK_POP), NAME_FIX_KM, NAME_FIX_EDITS,
                 ";".join("%s>%s" % kv for kv in sorted(NAME_FIXES.items()))))
# A real build (the pinned Natural Earth files): their counts, and landmarks that must come through.
NE_PLACES_RANGE = (7000, 7600)
GN_MIN_KEPT = 120000
LINE_PROBES = [(KIND_ADMIN0, -100.0, 49.0), (KIND_ADMIN0, -117.1, 32.54), (KIND_ADMIN1, -120.0, 42.0),
               (KIND_ADMIN1, -109.05, 37.0), (KIND_ADMIN0, 8.6, 47.6),
               (KIND_DISPUTED, 74.5, 34.4)]   # (kind, lon, lat): a vertex of that kind within PROBE_DEG (last: Kashmir LoC)
NO_LINE_PROBES = [(KIND_ADMIN1, -76.1, 37.6)]      # (kind, lon, lat): no vertex of that kind here (Chesapeake Bay)
PROBE_DEG = 0.3
PLACE_PROBES = [("tier0", "honolulu", 1, 60), ("tier0", "tokyo", 0, 20), ("tier0", "paris", 0, 30),
                ("20_-160", "haleiwa", 5, 100), ("20_-160", "kailua", 4, 80)]   # (where, folded name, max rank, max mz10)
# (Natural Earth name, GeoNames name, km): the place must exist, and no GeoNames twin of it may lie this close (G34a F1).
TWIN_PROBES = [("tokyo", "tokyo", 20), ("newyork", "newyorkcity", 20), ("stpetersburg", "saintpetersburg", 20),
               ("kobenhavn", "copenhagen", 20), ("jakarta", "jakarta", 20), ("buenosaires", "buenosaires", 20)]
# The parts of a real build that depend only on the pinned Natural Earth files and this code: any change of their bytes
# must be deliberate (update the pin). Tier 0 = world-lines.bin + world-places.bin (index tier0.sha256); tier-1 lines =
# cells_digest over the .l files.
PIN_TIER0_SHA256 = "1c226275066a7be89afba3876fa9e171c569cb04ddd6813ed7571bec25709162"
PIN_LINES1_SHA256 = "f2c45438b7186f1b62b95ca5c74f82d5a246ab0ab5b8ec258d2fa217b5848877"
CELLS_RANGE = (270, 310)                             # tier-1 cells of a real build
NAME_FIX_RANGE = (220, 250)                          # names the agreement rule fixes in a real build (236 measured)
NAME_PROBES = [("Shenyang", True), ("Shenyeng", False), ("Nizhnevartovsk", True), ("Poitiers", True), ("Stary Oskol", True),
               ("Starsy Oskol", False), ("København", True), ("Montréal", True)]   # (Natural Earth name, present)


# ---------------------------------------------------------------- names

def clean_name(s):
    """One line of text, at most NAME_MAX UTF-8 bytes (cut at a character boundary); '' if nothing is left."""
    s = "".join(" " if unicodedata.category(ch)[0] == "C" else ch for ch in str(s or ""))
    s = re.sub(r"\s+", " ", s).strip()
    b = s.encode("utf-8")
    if len(b) > NAME_MAX:
        s = b[:NAME_MAX].decode("utf-8", "ignore").rstrip()
    return s


FOLD_LETTERS = str.maketrans({"ø": "o", "æ": "ae", "œ": "oe", "ł": "l", "đ": "d", "ð": "d", "þ": "th", "ı": "i",
                               "ħ": "h", "ŧ": "t", "ŋ": "ng"})   # letters NFKD does not split into base + accent


def fold(s):
    """Lower-case letters only, accents and apostrophes removed: the name key of the dedupe and the probes."""
    s = unicodedata.normalize("NFKD", str(s or "").casefold().translate(FOLD_LETTERS))
    return re.sub(r"[^a-z0-9]", "", "".join(ch for ch in s if not unicodedata.combining(ch)))


def rank_of(pop, capital=False, state_capital=False):
    if capital:
        return 0
    if state_capital:
        return 1
    for lim, r in RANK_POP:
        if pop >= lim:
            return r
    return 5


# ---------------------------------------------------------------- lines

def read_lines(path, classes):
    """-> [(x, y, kind, mz10)] for every LineString part of the features whose FEATURECLA is a key of classes (the
    value is the line's kind)."""
    data = json.load(open(path, encoding="utf-8"))
    out = []
    for f in data.get("features", []):
        p = f.get("properties") or {}
        g = f.get("geometry")
        if not g or p.get("FEATURECLA") not in classes:
            continue
        kind = classes[p["FEATURECLA"]]
        mz = p.get("MIN_ZOOM")
        mz10 = 0 if mz is None else min(255, max(0, int(round(float(mz) * 10))))
        parts = [g["coordinates"]] if g["type"] == "LineString" else g["coordinates"] if g["type"] == "MultiLineString" else []
        for c in parts:
            if len(c) < 2:
                continue
            a = np.asarray([pt[:2] for pt in c], np.float64)
            if not np.isfinite(a).all():
                continue
            out.append((a[:, 0].copy(), a[:, 1].copy(), kind, mz10))
    return out


def split_dateline(x, y):
    """A line whose step jumps more than 180 degrees in longitude crosses the antimeridian there: cut it, ending the
    first run on the line (+-180) at the crossing latitude and starting the next on the other side. -> [(x, y)]."""
    x = np.asarray(x, np.float64); y = np.asarray(y, np.float64)
    jumps = np.nonzero(np.abs(np.diff(x)) > 180.0)[0]
    if not len(jumps):
        return [(x, y)]
    runs, start, head = [], 0, None
    for j in jumps:
        x0, y0, x1, y1 = x[j], y[j], x[j + 1], y[j + 1]
        edge = 180.0 if x0 > 0 else -180.0
        x1u = x1 + 360.0 if x0 > 0 else x1 - 360.0
        ym = y0 + (edge - x0) / (x1u - x0) * (y1 - y0)
        rx, ry = x[start:j + 1], y[start:j + 1]
        if head is not None:
            rx, ry = np.r_[head[0], rx], np.r_[head[1], ry]
        runs.append((np.r_[rx, edge], np.r_[ry, ym]))
        head, start = (-edge, ym), j + 1
    rx, ry = x[start:], y[start:]
    runs.append((np.r_[head[0], rx], np.r_[head[1], ry]))
    return runs


def simplify(x, y, tol):
    """Douglas-Peucker in degrees (both ends kept)."""
    n = len(x)
    if n < 3 or tol <= 0:
        return x, y
    keep = np.zeros(n, bool)
    keep[0] = keep[-1] = True
    stack = [(0, n - 1)]
    while stack:
        i, j = stack.pop()
        if j - i < 2:
            continue
        xs, ys = x[i + 1:j], y[i + 1:j]
        dx, dy = x[j] - x[i], y[j] - y[i]
        length = math.hypot(dx, dy)
        d = np.hypot(xs - x[i], ys - y[i]) if length == 0 else np.abs(dy * (xs - x[i]) - dx * (ys - y[i])) / length
        k = int(np.argmax(d))
        if d[k] > tol:
            m = i + 1 + k
            keep[m] = True
            stack.append((i, m)); stack.append((m, j))
    return x[keep], y[keep]


def split_line_cells(x, y, cell=CELL):
    """Cut a line (inside -180..180) at every cell line it crosses. The crossing point is inserted exactly on the cell
    line and ends one run and starts the next. Each run lies in one cell, decided by its segments' midpoints.
    -> [(lat0, lon0, x, y)] (south-west corner of the cell)."""
    x = np.asarray(x, np.float64); y = np.asarray(y, np.float64)
    x0, x1, y0, y1 = x[:-1], x[1:], y[:-1], y[1:]
    kx_lo = np.floor(np.minimum(x0, x1) / cell) + 1; kx_hi = np.ceil(np.maximum(x0, x1) / cell) - 1
    ky_lo = np.floor(np.minimum(y0, y1) / cell) + 1; ky_hi = np.ceil(np.maximum(y0, y1) / cell) - 1
    need = np.nonzero((kx_lo <= kx_hi) | (ky_lo <= ky_hi))[0]
    if len(need):
        px, py, prev = [], [], 0
        for i in need:
            px.append(x[prev:i + 1]); py.append(y[prev:i + 1])
            cuts = []
            for k in range(int(kx_lo[i]), int(kx_hi[i]) + 1):
                t = (k * cell - x0[i]) / (x1[i] - x0[i])
                cuts.append((t, float(k * cell), y0[i] + t * (y1[i] - y0[i])))
            for k in range(int(ky_lo[i]), int(ky_hi[i]) + 1):
                t = (k * cell - y0[i]) / (y1[i] - y0[i])
                cuts.append((t, x0[i] + t * (x1[i] - x0[i]), float(k * cell)))
            cuts.sort()
            px.append(np.array([c[1] for c in cuts])); py.append(np.array([c[2] for c in cuts]))
            prev = i + 1
        px.append(x[prev:]); py.append(y[prev:])
        x, y = np.concatenate(px), np.concatenate(py)
    if len(x) < 2:
        return []
    mx, my = (x[:-1] + x[1:]) / 2, (y[:-1] + y[1:]) / 2
    cx = np.clip(np.floor(mx / cell), -180 // cell, 180 // cell - 1).astype(np.int64)
    cy = np.clip(np.floor(my / cell), -90 // cell, 90 // cell - 1).astype(np.int64)
    key = cy * 1000 + cx
    breaks = np.nonzero(key[1:] != key[:-1])[0] + 1
    out = []
    for a, b in zip(np.r_[0, breaks], np.r_[breaks, len(key)]):       # segments a..b-1 -> points a..b
        out.append((int(cy[a]) * cell, int(cx[a]) * cell, x[a:b + 1], y[a:b + 1]))
    return out


def quantize_line(x, y, q=Q):
    """Integer line, consecutive duplicates removed; None if fewer than 2 distinct points remain."""
    ix = np.round(np.asarray(x) * q).astype(np.int64); iy = np.round(np.asarray(y) * q).astype(np.int64)
    keep = np.ones(len(ix), bool)
    keep[1:] = (ix[1:] != ix[:-1]) | (iy[1:] != iy[:-1])
    ix, iy = ix[keep], iy[keep]
    return (ix, iy) if len(ix) >= 2 else None


def cell_lines(lines, simplify_deg=0.0, cell=CELL, q=Q):
    """-> {(lat0, lon0): {kind: [(ix, iy, mz10)]}} after the dateline cut, the optional simplification and the cell cut."""
    cells = {}
    for x, y, kind, mz10 in lines:
        for rx, ry in split_dateline(x, y):
            if simplify_deg > 0:
                rx, ry = simplify(rx, ry, simplify_deg)
            for lat0, lon0, cx, cy in split_line_cells(rx, ry, cell):
                r = quantize_line(cx, cy, q)
                if r is not None:
                    cells.setdefault((lat0, lon0), {}).setdefault(kind, []).append((r[0], r[1], mz10))
    for kinds in cells.values():
        for kind in kinds:
            kinds[kind] = chain_lines(kinds[kind])
    return cells


def chain_lines(lines):
    """Join lines of the same min_zoom that share an end point into longer lines (the same points, fewer lines: Natural
    Earth cuts borders at every junction and into many parts). Order: by min_zoom, then as found."""
    out = []
    for mz in sorted({l[2] for l in lines}):
        group = [(l[0], l[1]) for l in lines if l[2] == mz]
        ends = {}
        for i, (ix, iy) in enumerate(group):
            ends.setdefault((int(ix[0]), int(iy[0])), []).append(i)
            ends.setdefault((int(ix[-1]), int(iy[-1])), []).append(i)
        used = [False] * len(group)

        def take(pt):
            for j in ends.get(pt, ()):
                if not used[j]:
                    used[j] = True
                    jx, jy = group[j]
                    if (int(jx[0]), int(jy[0])) == pt:
                        return jx, jy
                    return jx[::-1], jy[::-1]
            return None
        for i in range(len(group)):
            if used[i]:
                continue
            used[i] = True
            xs, ys = [group[i][0]], [group[i][1]]
            while True:                                  # forward from the end
                nxt = take((int(xs[-1][-1]), int(ys[-1][-1])))
                if nxt is None:
                    break
                xs.append(nxt[0][1:]); ys.append(nxt[1][1:])
            hx, hy = [], []
            while True:                                  # backward from the start
                start = (int(hx[-1][0]) if hx else int(xs[0][0]), int(hy[-1][0]) if hy else int(ys[0][0]))
                nxt = take(start)
                if nxt is None:
                    break
                hx.append(nxt[0][::-1][:-1]); hy.append(nxt[1][::-1][:-1])
            x = np.concatenate(hx[::-1] + xs); y = np.concatenate(hy[::-1] + ys)
            out.append((x, y, mz))
    return out


# ---------------------------------------------------------------- lines-v1

def encode_lines(pieces, cell_deg=CELL, q=Q):
    """pieces: [(kind, [(ix, iy, mz10)])] -> bytes."""
    stream, n_lines, n_verts, kinds = [], 0, 0, 0
    bx0 = by0 = 2 ** 31 - 1; bx1 = by1 = -2 ** 31
    for kind, lines in pieces:
        minx = min(int(l[0].min()) for l in lines); miny = min(int(l[1].min()) for l in lines)
        maxx = max(int(l[0].max()) for l in lines); maxy = max(int(l[1].max()) for l in lines)
        bx0, by0, bx1, by1 = min(bx0, minx), min(by0, miny), max(bx1, maxx), max(by1, maxy)
        kinds |= 1 << kind
        stream.append(C.zigzag([minx, miny]))
        stream.append(np.array([maxx - minx, maxy - miny, kind, len(lines)], np.uint64))
        px, py = minx, miny
        for ix, iy, mz10 in lines:
            dx = np.diff(np.concatenate([[px], ix])); dy = np.diff(np.concatenate([[py], iy]))
            px, py = int(ix[-1]), int(iy[-1])
            pairs = np.empty(2 * len(ix), np.int64); pairs[0::2] = dx; pairs[1::2] = dy
            stream.append(np.array([len(ix), mz10], np.uint64)); stream.append(C.zigzag(pairs))
            n_lines += 1; n_verts += len(ix)
    if not pieces:
        bx0 = by0 = bx1 = by1 = 0
    body = C.varints(np.concatenate(stream)) if stream else b""
    return LINES_HEADER.pack(LINES_MAGIC, cell_deg, kinds, q, len(pieces), n_lines, n_verts, bx0, by0, bx1, by1) + body


def decode_varints(data):
    """Every LEB128 value of a byte string (at most 5 bytes each) -> uint64 array; ValueError on a cut-off value."""
    data = np.frombuffer(data, np.uint8)
    if not len(data):
        return np.zeros(0, np.uint64)
    if data[-1] >= 0x80:
        raise ValueError("stream ends inside a varint")
    ends = np.nonzero(data < 0x80)[0]
    starts = np.concatenate([[0], ends[:-1] + 1])
    if np.any(ends - starts > 4):
        raise ValueError("varint longer than 5 bytes")
    vals = np.zeros(len(ends), np.uint64)
    for k in range(5):
        m = starts + k <= ends
        vals[m] |= (data[starts[m] + k].astype(np.uint64) & np.uint64(0x7F)) << np.uint64(7 * k)
    return vals


def unzz(v):
    v = np.asarray(v, np.uint64).astype(np.int64)
    return (v >> 1) ^ -(v & 1)


def decode_lines(buf):
    """-> dict(cell, kinds, q, bbox, pieces=[{"b": [minx, miny, maxx, maxy], "kind", "lines": [(ix, iy, mz10)]}])."""
    if len(buf) < LINES_HEADER.size:
        raise ValueError("lines file too short")
    magic, cell, kinds, q, n_pieces, n_lines, n_verts, *bbox = LINES_HEADER.unpack_from(buf, 0)
    if magic != LINES_MAGIC:
        raise ValueError("not a lines-v1 file")
    vals = decode_varints(buf[LINES_HEADER.size:])
    i, pieces, lines_seen, verts_seen = 0, [], 0, 0
    try:
        for _ in range(n_pieces):
            minx, miny = (int(v) for v in unzz(vals[i:i + 2])); w, h, kind, nl = (int(v) for v in vals[i + 2:i + 6]); i += 6
            if kind not in KINDS:
                raise ValueError("unknown line kind %d" % kind)
            lines, px, py = [], minx, miny
            for _ in range(nl):
                n, mz10 = int(vals[i]), int(vals[i + 1]); i += 2
                if n < 2 or i + 2 * n > len(vals):
                    raise ValueError("bad line length")
                d = unzz(vals[i:i + 2 * n]); i += 2 * n
                ix, iy = px + np.cumsum(d[0::2]), py + np.cumsum(d[1::2])
                px, py = int(ix[-1]), int(iy[-1])
                lines.append((ix, iy, mz10))
                lines_seen += 1; verts_seen += n
            pieces.append({"b": [minx, miny, minx + w, miny + h], "kind": kind, "lines": lines})
    except IndexError:
        raise ValueError("lines-v1 stream shorter than its counts") from None
    if i != len(vals) or lines_seen != n_lines or verts_seen != n_verts:
        raise ValueError("lines-v1 counts do not match the stream")
    return {"cell": cell, "kinds": kinds, "q": q, "bbox": bbox, "pieces": pieces}


# ---------------------------------------------------------------- places

def prop(p, key):
    """A Natural Earth property by its upper-case name (the full file), else its lower-case name (the simple file)."""
    v = p.get(key)
    return p.get(key.lower()) if v is None else v


def read_ne_places(path):
    """-> [place] from the Natural Earth populated places (name, lat, lon, pop, mz10, rank, flags, GeoNames id)."""
    data = json.load(open(path, encoding="utf-8"))
    out = []
    for f in data.get("features", []):
        p = f.get("properties") or {}
        g = f.get("geometry") or {}
        if g.get("type") != "Point":
            continue
        lon, lat = (float(v) for v in g["coordinates"][:2])
        name = clean_name(prop(p, "NAME") or prop(p, "NAMEASCII"))
        if not name or not (math.isfinite(lat) and math.isfinite(lon)):
            continue
        pop = max(0, int(prop(p, "POP_MAX") or 0))
        cla = str(prop(p, "FEATURECLA") or "")
        cap = int(prop(p, "ADM0CAP") or 0) == 1 or cla.startswith("Admin-0 capital")
        state = cla.startswith("Admin-1 capital")
        mz = prop(p, "MIN_ZOOM")
        mz10 = min(255, max(0, int(round(float(mz) * 10)))) if mz is not None else MZ_SPLIT
        gid = prop(p, "GEONAMESID")
        lat, lon = qpos(lat, lon)
        out.append({"name": name, "name_en": clean_name(prop(p, "NAME_EN")), "lat": lat, "lon": lon, "pop": pop, "mz10": mz10,
                    "rank": rank_of(pop, cap, state), "flags": FLAG_CAPITAL if cap else 0,
                    "gnid": int(gid) if gid and int(gid) > 0 else None,
                    "keys": {fold(name), fold(prop(p, "NAMEASCII"))} - {""}})
    return out


def read_geonames(zip_path):
    """-> ([place], file date, names left out for " / ") from cities1000.zip: feature class P, codes GN_CODES,
    population >= GN_MIN_POP. Rows are split on "\\n" only (str.splitlines would also split inside a row on U+2028,
    U+2029 or U+0085 and lose the town)."""
    z = zipfile.ZipFile(zip_path)
    member = z.getinfo("cities1000.txt")
    out, slashes = [], 0
    for line in z.read(member).decode("utf-8").split("\n"):
        f = line.rstrip("\r").split("\t")
        if len(f) < 15 or f[6] != "P" or f[7] not in GN_CODES:
            continue
        try:
            lat, lon, pop = float(f[4]), float(f[5]), int(f[14] or 0)
        except ValueError:
            continue
        name = clean_name(f[1])
        if pop < GN_MIN_POP or not name or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        if " / " in name:                            # merged areas ("Aliamanu / Salt Lakes / Foster Village")
            slashes += 1
            continue
        mz10 = next(z10 for lim, z10 in GN_ZOOM10 if pop >= lim)
        lat, lon = qpos(lat, lon)
        out.append({"name": name, "lat": lat, "lon": lon, "pop": pop, "mz10": mz10,
                    "rank": rank_of(pop, f[7] == "PPLC", f[7] == "PPLA"),
                    "flags": FLAG_GEONAMES | (FLAG_CAPITAL if f[7] == "PPLC" else 0),
                    "id": int(f[0]) if f[0].isdigit() else None, "alt": f[3],
                    "keys": {fold(name), fold(f[2])} - {""}})
    return out, "%04d-%02d-%02d" % member.date_time[:3], slashes


def read_geonames_names(zip_path):
    """-> {geonameid: (name, lat, lon)} for every row of cities1000 (the name pass reads any row, not only the towns kept)."""
    out = {}
    for line in zipfile.ZipFile(zip_path).read("cities1000.txt").decode("utf-8").split("\n"):
        f = line.rstrip("\r").split("\t")
        if len(f) < 6 or not f[0].isdigit():
            continue
        try:
            out[int(f[0])] = (clean_name(f[1]), float(f[4]), float(f[5]))
        except ValueError:
            continue
    return out


def edits(a, b):
    """The Levenshtein distance (insertions, deletions, substitutions)."""
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def fix_names(ne, gnames, km=NAME_FIX_KM, max_edits=NAME_FIX_EDITS, hand=NAME_FIXES):
    """V7-3: correct Natural Earth spelling errors in place (see the module docstring). A fixed place keeps its old
    folded name among its dedupe keys. -> {"agreed": n, "hand": n}."""
    stats = {"agreed": 0, "hand": 0}
    for p in ne:
        old, new = p["name"], None
        if old in hand:
            new = hand[old]
            stats["hand"] += 1
        else:
            en, g = p.get("name_en"), gnames.get(p.get("gnid")) if p.get("gnid") else None
            if (en and en != old and g and g[0] == en and haversine_km(p["lat"], p["lon"], g[1], g[2]) <= km
                    and edits(old.lower(), en.lower()) <= max_edits):
                new = en
                stats["agreed"] += 1
        if new:
            p["name"] = new
            p["keys"] = (set(p["keys"]) | {fold(new)}) - {""}
    return stats


def qpos(lat, lon, q=Q):
    """A position on the 1/q-degree grid the files store (longitude 180 written as -180): cells are chosen from it."""
    lat, lon = round(lat * q) / q, round(lon * q) / q
    return lat, (-180.0 if lon >= 180.0 else lon)


def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * math.asin(min(1.0, math.sqrt(a)))


def grid_near(grid, lat, lon, km):
    """Every entry of a 0.1-degree grid that can lie within km of (lat, lon): enough buckets of latitude (11.1 km each)
    and of longitude (11.1 km x cos(latitude) each, so more of them towards the poles), longitude wrapped."""
    gy, gx = math.floor(lat * 10), math.floor(lon * 10)
    ry = int(math.ceil(km / 11.1)) + 1
    rx = min(1800, int(math.ceil(km / (11.1 * max(math.cos(math.radians(min(abs(lat) + 0.2 * ry, 90.0))), 1e-3)))) + 1)
    for dy in range(-ry, ry + 1):
        for dx in range(-rx, rx + 1):
            yield from grid.get((gy + dy, (gx + dx) % 3600), ())


def grid_add(grid, p):
    grid.setdefault((math.floor(p["lat"] * 10), math.floor(p["lon"] * 10) % 3600), []).append(p)


def alt_keys(g):
    """The folded alternate names of a GeoNames row (computed only when a Natural Earth place is near)."""
    return {k for k in (fold(a) for a in str(g.get("alt") or "").split(",")) if len(k) >= 2}


def dedupe(ne, gn, km=DEDUPE_KM, name_km=DEDUPE_NAME_KM, twin_km=GN_TWIN_KM):
    """The GeoNames towns that are not a Natural Earth place: not its GEONAMESID, no Natural Earth place within name_km
    of the same name or ascii name, none within km named as one of the town's alternate names (all folded); then, of
    GeoNames towns with one folded name within twin_km of each other, the first by rank and population.
    -> (kept, {"id", "name", "twin": counts})."""
    ids = {p["gnid"] for p in ne if p.get("gnid")}
    grid = {}
    for p in ne:
        grid_add(grid, p)
    stats = {"id": 0, "name": 0, "twin": 0}
    first = []
    for g in gn:
        if g.get("id") is not None and g["id"] in ids:
            stats["id"] += 1
            continue
        near = [(p, haversine_km(p["lat"], p["lon"], g["lat"], g["lon"]))
                for p in grid_near(grid, g["lat"], g["lon"], max(km, name_km))]
        same = any(d <= name_km and p["keys"] & g["keys"] for p, d in near)
        if not same and any(d <= km for p, d in near):
            alt = alt_keys(g)
            same = any(d <= km and p["keys"] & alt for p, d in near)
        if same:
            stats["name"] += 1
            continue
        first.append(g)
    kept, seen = [], {}
    for g in sorted(first, key=lambda g: (g["rank"], -g["pop"], g["name"], g["lat"], g["lon"])):
        if any(t["keys"] & g["keys"] and haversine_km(t["lat"], t["lon"], g["lat"], g["lon"]) <= twin_km
               for t in grid_near(seen, g["lat"], g["lon"], twin_km)):
            stats["twin"] += 1
            continue
        grid_add(seen, g)
        kept.append(g)
    return kept, stats


def sort_places(places):
    return sorted(places, key=lambda p: (p["rank"], -p["pop"], p["name"], p["lat"], p["lon"]))


def place_cell(p, cell=CELL):
    lon = -180.0 if p["lon"] >= 180.0 else p["lon"]
    lat = min(p["lat"], 90.0 - 1e-9)
    return int(math.floor(lat / cell)) * cell, int(math.floor(lon / cell)) * cell


# ---------------------------------------------------------------- places-v1

def encode_places(places, cell_deg=CELL, q=Q):
    """places in record order -> bytes."""
    lat = np.array([int(round(p["lat"] * q)) for p in places], np.int64)
    lon = np.array([int(round((-180.0 if p["lon"] >= 180.0 else p["lon"]) * q)) for p in places], np.int64)
    if places:
        vals = np.empty(6 * len(places), np.uint64)
        vals[0::6] = C.zigzag(lat); vals[1::6] = C.zigzag(lon)
        vals[2::6] = [p["rank"] for p in places]; vals[3::6] = [p["mz10"] for p in places]
        vals[4::6] = [p["pop"] for p in places]; vals[5::6] = [p["flags"] for p in places]
        stream = C.varints(vals)
        bbox = (int(lon.min()), int(lat.min()), int(lon.max()), int(lat.max()))
    else:
        stream, bbox = b"", (0, 0, 0, 0)
    names = "\n".join(p["name"] for p in places).encode("utf-8")
    return PLACES_HEADER.pack(PLACES_MAGIC, cell_deg, 0, q, len(places), len(stream), *bbox) + stream + names


def decode_places(buf):
    """-> dict(cell, q, bbox, places=[{"lat", "lon" (1/q degree ints), "rank", "mz10", "pop", "flags", "name"}])."""
    if len(buf) < PLACES_HEADER.size:
        raise ValueError("places file too short")
    magic, cell, _z, q, count, nstream, *bbox = PLACES_HEADER.unpack_from(buf, 0)
    if magic != PLACES_MAGIC:
        raise ValueError("not a places-v1 file")
    end = PLACES_HEADER.size + nstream
    if end > len(buf):
        raise ValueError("places stream longer than the file")
    vals = decode_varints(buf[PLACES_HEADER.size:end])
    if len(vals) != 6 * count:
        raise ValueError("places counts do not match the stream")
    text = bytes(buf[end:]).decode("utf-8")          # strict: a bad byte raises UnicodeDecodeError (a ValueError)
    names = text.split("\n") if count else ([] if text == "" else None)
    if names is None or len(names) != count:
        raise ValueError("places names do not match the count")
    lat, lon = unzz(vals[0::6]), unzz(vals[1::6])
    out = [{"lat": int(lat[k]), "lon": int(lon[k]), "rank": int(vals[6 * k + 2]), "mz10": int(vals[6 * k + 3]),
            "pop": int(vals[6 * k + 4]), "flags": int(vals[6 * k + 5]), "name": names[k]} for k in range(count)]
    return {"cell": cell, "q": q, "bbox": bbox, "places": out}


# ---------------------------------------------------------------- build

def sha(b):
    return hashlib.sha256(b).hexdigest()


def tier0_digest(lines_sha, places_sha):
    """One hash over both tier-0 files: the index's tier0.sha256 (what the upload guard compares)."""
    return sha(("lines:%s\nplaces:%s\n" % (lines_sha, places_sha)).encode("ascii"))


def cell_name(lat0, lon0):
    return "%d_%d" % (lat0, lon0)


def pieces_of(cellmap):
    """{(lat0, lon0): {kind: lines}} -> [(kind, lines)] ordered by cell, then kind."""
    return [(kind, cellmap[key][kind]) for key in sorted(cellmap) for kind in sorted(cellmap[key])]


def build(ne_dir, gn_zip, out_dir, log=print):
    t0 = time.time()
    os.makedirs(os.path.join(out_dir, CELL_DIR), exist_ok=True)
    src = {k: os.path.join(ne_dir, f) for k, (f, _h) in NE_FILES.items()}
    index = {"format": FORMAT, "format_key": FORMAT_KEY, "q": Q, "cell": CELL, "license": LICENSE,
             "built_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
             "sources": {"natural_earth": {"commit": NE_COMMIT,
                                           "files": {f: sha(open(src[k], "rb").read()) for k, (f, _h) in NE_FILES.items()}},
                         "geonames": {"file": GN_FILE, "sha256": sha(open(gn_zip, "rb").read())}}}
    a0 = read_lines(src["admin0"], ADMIN0_CLASSES)
    a1 = read_lines(src["admin1"], ADMIN1_CLASSES)
    lines0 = a0 + [l for l in a1 if l[3] < MZ_SPLIT]
    cells0 = cell_lines(lines0, SIMPLIFY_DEG, q=TIER0_Q)
    body_l = encode_lines(pieces_of(cells0), q=TIER0_Q)
    open(os.path.join(out_dir, LINES_TIER0), "wb").write(body_l)
    d0 = decode_lines(body_l)
    ne = read_ne_places(src["places"])
    fixes = fix_names(ne, read_geonames_names(gn_zip))
    gn, gn_date, gn_slashes = read_geonames(gn_zip)
    gn_kept, gn_stats = dedupe(ne, gn)
    index["sources"]["geonames"]["date"] = gn_date
    tier0_places = sort_places([p for p in ne if p["mz10"] < MZ_SPLIT])
    body_p = encode_places(tier0_places, q=TIER0_Q)
    open(os.path.join(out_dir, PLACES_TIER0), "wb").write(body_p)
    index["tier0"] = {"max_zoom": TIER0_MAX_ZOOM,
                      "lines": {"file": LINES_TIER0, "bytes": len(body_l), "sha256": sha(body_l), "pieces": len(d0["pieces"]),
                                "lines": sum(len(pc["lines"]) for pc in d0["pieces"]),
                                "vertices": sum(len(l[0]) for pc in d0["pieces"] for l in pc["lines"])},
                      "places": {"file": PLACES_TIER0, "bytes": len(body_p), "sha256": sha(body_p), "count": len(tier0_places)}}
    index["tier0"]["sha256"] = tier0_digest(sha(body_l), sha(body_p))
    log("tier 0: %d border lines (%d vertices) %d B; %d places %d B (%.1fs)" % (
        index["tier0"]["lines"]["lines"], index["tier0"]["lines"]["vertices"], len(body_l), len(tier0_places), len(body_p),
        time.time() - t0))
    cells1 = cell_lines(a0 + a1)
    by_cell = {}
    for p in [p for p in ne if p["mz10"] >= MZ_SPLIT] + gn_kept:
        by_cell.setdefault(place_cell(p), []).append(p)
    listing, digests, total, capped = {}, {}, 0, 0
    for key in sorted(set(cells1) | set(by_cell)):
        name = cell_name(*key)
        lb = pb = 0
        if key in cells1:
            body = encode_lines([(k, cells1[key][k]) for k in sorted(cells1[key])])
            open(os.path.join(out_dir, CELL_DIR, name + ".l.bin"), "wb").write(body)
            lb, digests[name + ".l"] = len(body), sha(body)
        if key in by_cell:
            ps = sort_places(by_cell[key])
            capped += max(0, len(ps) - CELL_CAP)
            body = encode_places(ps[:CELL_CAP])
            open(os.path.join(out_dir, CELL_DIR, name + ".p.bin"), "wb").write(body)
            pb, digests[name + ".p"] = len(body), sha(body)
        listing[name] = [lb, pb]
        total += lb + pb
    index["tier1"] = {"dir": CELL_DIR, "cell": CELL, "min_zoom": TIER1_MIN_ZOOM, "cells": listing, "bytes": total,
                      "sha256": C.cells_digest(digests),
                      "lines_sha256": C.cells_digest({k: v for k, v in digests.items() if k.endswith(".l")})}
    index["counts"] = {"admin0_lines": len(a0), "admin1_lines": len(a1), "ne_places": len(ne), "geonames_read": len(gn),
                       "geonames_slashes": gn_slashes, "geonames_kept": len(gn_kept), "geonames_dupes_id": gn_stats["id"],
                       "geonames_dupes_name": gn_stats["name"], "geonames_twins": gn_stats["twin"], "capped": capped,
                       "name_fixes_agreed": fixes["agreed"], "name_fixes_hand": fixes["hand"]}
    big = sorted(listing.items(), key=lambda kv: -max(kv[1]))[:5]
    log("tier 1: %d cells, %d B; largest %s; counts %s (%.1fs)" % (len(listing), total, big, index["counts"], time.time() - t0))
    with open(os.path.join(out_dir, "index.json"), "w") as fh:
        json.dump(index, fh, separators=(",", ":"), sort_keys=True)
    return index


def files_of(index):
    """The data files of a build, for the upload."""
    out = [index["tier0"]["lines"]["file"], index["tier0"]["places"]["file"]]
    for name in sorted(index["tier1"]["cells"]):
        lb, pb = index["tier1"]["cells"][name]
        out += (["%s/%s.l.bin" % (index["tier1"]["dir"], name)] if lb else []) + \
               (["%s/%s.p.bin" % (index["tier1"]["dir"], name)] if pb else [])
    return out


# ---------------------------------------------------------------- check

def check_lines(d, cell_box=None):
    """Problems of one decoded lines file; cell_box = (lat0, lon0, cell) for a tier-1 file. Every piece must fit one
    CELL-degree cell (tier 0) or its named cell (tier 1); coordinates in range, kinds known."""
    problems = []
    q = d["q"]
    for k, pc in enumerate(d["pieces"]):
        b = pc["b"]
        if not pc["lines"]:
            problems.append("piece %d: no lines" % k)
            continue
        bb = [min(int(l[0].min()) for l in pc["lines"]), min(int(l[1].min()) for l in pc["lines"]),
              max(int(l[0].max()) for l in pc["lines"]), max(int(l[1].max()) for l in pc["lines"])]
        if bb != b:
            problems.append("piece %d: bbox %s != decoded %s" % (k, b, bb))
        if b[0] < -180 * q or b[2] > 180 * q or b[1] < -90 * q or b[3] > 90 * q:
            problems.append("piece %d: coordinates out of range" % k)
        if cell_box is not None:
            lat0, lon0, cell = cell_box
        else:
            cell = CELL
            lat0 = int(math.floor(b[1] / q / cell)) * cell
            lon0 = int(math.floor(b[0] / q / cell)) * cell
        if b[0] < lon0 * q or b[2] > (lon0 + cell) * q or b[1] < lat0 * q or b[3] > (lat0 + cell) * q:
            problems.append("piece %d: outside its cell %s" % (k, cell_name(lat0, lon0)))
        for ix, iy, mz10 in pc["lines"]:
            if len(ix) < 2 or np.any((np.diff(ix) == 0) & (np.diff(iy) == 0)):
                problems.append("piece %d: a line with < 2 points or a repeated point" % k)
            if mz10 > 255:
                problems.append("piece %d: min_zoom %d out of range" % (k, mz10))
        if len(problems) > 20:
            break
    return problems


def check_places(d, cell_box=None, tier0=False):
    problems = []
    q = d["q"]
    prev = None
    for k, p in enumerate(d["places"]):
        if not p["name"] or len(p["name"].encode("utf-8")) > NAME_MAX or clean_name(p["name"]) != p["name"]:
            problems.append("place %d: bad name %r" % (k, p["name"][:80]))
        if abs(p["lat"]) > 90 * q or not -180 * q <= p["lon"] < 180 * q:
            problems.append("place %d: position out of range" % k)
        if p["rank"] > 5 or p["mz10"] > 255 or p["flags"] > 3:
            problems.append("place %d: rank/min_zoom/flags out of range" % k)
        if tier0 and p["mz10"] >= MZ_SPLIT:
            problems.append("place %d (%s): min_zoom %d belongs in a cell" % (k, p["name"], p["mz10"]))
        if not tier0 and p["mz10"] < MZ_SPLIT:
            problems.append("place %d (%s): min_zoom %d belongs in tier 0" % (k, p["name"], p["mz10"]))
        if cell_box is not None:
            lat0, lon0, cell = cell_box
            if not (lat0 * q <= p["lat"] <= (lat0 + cell) * q and lon0 * q <= p["lon"] < (lon0 + cell) * q):
                problems.append("place %d (%s): outside its cell %s" % (k, p["name"], cell_name(lat0, lon0)))
        key = (p["rank"], -p["pop"])
        if prev is not None and key < prev:
            problems.append("place %d (%s): out of rank order" % (k, p["name"]))
        prev = key
        if len(problems) > 20:
            break
    if cell_box is not None and len(d["places"]) > CELL_CAP:
        problems.append("%d places, more than the cap %d" % (len(d["places"]), CELL_CAP))
    return problems


def check(out_dir, log=print):
    index = json.load(open(os.path.join(out_dir, "index.json")))
    problems = []
    if index.get("format") != FORMAT or index.get("q") != Q or index.get("cell") != CELL:
        problems.append("index format/q/cell")
    if index.get("format_key") != FORMAT_KEY:
        problems.append("index format_key %r != this builder's %r" % (index.get("format_key"), FORMAT_KEY))
    t0 = index["tier0"]
    bl = open(os.path.join(out_dir, t0["lines"]["file"]), "rb").read()
    bp = open(os.path.join(out_dir, t0["places"]["file"]), "rb").read()
    if len(bl) != t0["lines"]["bytes"] or sha(bl) != t0["lines"]["sha256"]:
        problems.append("tier 0 lines size/hash mismatch")
    if len(bp) != t0["places"]["bytes"] or sha(bp) != t0["places"]["sha256"]:
        problems.append("tier 0 places size/hash mismatch")
    if t0.get("sha256") != tier0_digest(sha(bl), sha(bp)):
        problems.append("tier 0: content hash != index (tier0.sha256)")
    lines_found = {}                                  # kind -> [(ix, iy)] for the probes (tier 1)
    lines0 = {}                                       # the same for tier 0
    try:
        dl = decode_lines(bl)
        if dl["q"] != TIER0_Q or dl["cell"] != CELL:
            problems.append("tier 0 lines: header q %d / cell %d, expected %d / %d" % (dl["q"], dl["cell"], TIER0_Q, CELL))
        problems += ["tier 0 lines: " + p for p in check_lines(dl)]
        for pc in dl["pieces"]:
            lines0.setdefault(pc["kind"], []).extend((l[0], l[1]) for l in pc["lines"])
        if sum(len(pc["lines"]) for pc in dl["pieces"]) != t0["lines"]["lines"]:
            problems.append("tier 0 lines: line count != index")
    except ValueError as exc:
        problems.append("tier 0 lines: decode failed: %s" % exc)
    tier0_places = []
    try:
        dp = decode_places(bp)
        tier0_places = dp["places"]
        if dp["q"] != TIER0_Q:
            problems.append("tier 0 places: header q %d, expected %d" % (dp["q"], TIER0_Q))
        problems += ["tier 0 places: " + p for p in check_places(dp, tier0=True)]
        if len(tier0_places) != t0["places"]["count"]:
            problems.append("tier 0 places: count != index")
    except ValueError as exc:
        problems.append("tier 0 places: decode failed: %s" % exc)
    t1 = index["tier1"]
    cdir = os.path.join(out_dir, t1["dir"])
    present = set(os.listdir(cdir))
    expect = set()
    for name, (lb, pb) in t1["cells"].items():
        expect |= ({name + ".l.bin"} if lb else set()) | ({name + ".p.bin"} if pb else set())
    if present != expect:
        problems.append("tier 1: files %d vs index %d" % (len(present), len(expect)))
    digests, cell_places = {}, {}
    for name, (lb, pb) in sorted(t1["cells"].items()):
        lat0, lon0 = (int(v) for v in name.split("_"))
        for ext, nbytes in ((".l", lb), (".p", pb)):
            if not nbytes:
                continue
            path = os.path.join(cdir, name + ext + ".bin")
            if not os.path.exists(path):
                continue
            buf = open(path, "rb").read()
            digests[name + ext] = sha(buf)
            if len(buf) != nbytes:
                problems.append("tier 1 %s%s: size mismatch" % (name, ext))
            try:
                if ext == ".l":
                    d = decode_lines(buf)
                    if d["q"] != Q or d["cell"] != t1["cell"]:
                        problems.append("tier 1 %s.l: header q %d / cell %d" % (name, d["q"], d["cell"]))
                    problems += ["tier 1 %s.l: %s" % (name, p) for p in check_lines(d, (lat0, lon0, t1["cell"]))]
                    for pc in d["pieces"]:
                        lines_found.setdefault(pc["kind"], []).extend((l[0], l[1]) for l in pc["lines"])
                else:
                    d = decode_places(buf)
                    if d["q"] != Q:
                        problems.append("tier 1 %s.p: header q %d" % (name, d["q"]))
                    problems += ["tier 1 %s.p: %s" % (name, p) for p in check_places(d, (lat0, lon0, t1["cell"]))]
                    cell_places[name] = d["places"]
            except ValueError as exc:
                problems.append("tier 1 %s%s: decode failed: %s" % (name, ext, exc))
        if len(problems) > 60:
            break
    if present == expect and t1.get("sha256") != C.cells_digest(digests):
        problems.append("tier 1: content hash != index (tier1.sha256)")
    lines1 = C.cells_digest({k: v for k, v in digests.items() if k.endswith(".l")})
    if present == expect and t1.get("lines_sha256") != lines1:
        problems.append("tier 1: lines hash != index (tier1.lines_sha256)")
    real = is_real(index)
    if real:
        problems += real_checks(index, tier0_places, cell_places, lines0, lines_found, lines1)
    log("check: tier 0 %d B lines + %d B places (%d); tier 1 %d cells %d B; real-build gates %s; %d problems" % (
        len(bl), len(bp), len(tier0_places), len(t1["cells"]), t1.get("bytes", 0), "on" if real else "OFF", len(problems)))
    for p in problems[:50]:
        log("  " + p)
    return problems


def is_real(index):
    """Built from the pinned Natural Earth files (never judged from the result)."""
    files = index.get("sources", {}).get("natural_earth", {}).get("files", {})
    return all(files.get(f) == h for f, h in NE_FILES.values())


def degrees(places, q):
    return [(p["lat"] / q, p["lon"] / q, p) for p in places]


def real_checks(index, tier0_places, cell_places, lines0, lines1, lines1_sha):
    """The gates of a real build. Counts come from the DECODED files, not from the index (G34a F2)."""
    problems = []
    counts = index.get("counts", {})
    t1 = index["tier1"]
    if PIN_TIER0_SHA256 and index["tier0"].get("sha256") != PIN_TIER0_SHA256:
        problems.append("pin: tier 0 %s != the pinned %s (a deliberate change updates PIN_TIER0_SHA256)" % (
            str(index["tier0"].get("sha256"))[:12], PIN_TIER0_SHA256[:12]))
    if PIN_LINES1_SHA256 and lines1_sha != PIN_LINES1_SHA256:
        problems.append("pin: tier-1 lines %s != the pinned %s (a deliberate change updates PIN_LINES1_SHA256)" % (
            lines1_sha[:12], PIN_LINES1_SHA256[:12]))
    ne_cells = sum(1 for ps in cell_places.values() for p in ps if not p["flags"] & FLAG_GEONAMES)
    gn_cells = sum(1 for ps in cell_places.values() for p in ps if p["flags"] & FLAG_GEONAMES)
    ne_all = len(tier0_places) + ne_cells
    if not NE_PLACES_RANGE[0] <= ne_all <= NE_PLACES_RANGE[1] or ne_all != counts.get("ne_places"):
        problems.append("counts: %d Natural Earth places decoded (index %s), expected %d-%d and equal" % (
            ne_all, counts.get("ne_places"), *NE_PLACES_RANGE))
    if gn_cells < GN_MIN_KEPT or gn_cells != counts.get("geonames_kept", 0) - counts.get("capped", 0):
        problems.append("counts: %d GeoNames towns decoded, expected >= %d and kept - capped (%s)" % (
            gn_cells, GN_MIN_KEPT, counts.get("geonames_kept", 0) - counts.get("capped", 0)))
    if not CELLS_RANGE[0] <= len(t1["cells"]) <= CELLS_RANGE[1]:
        problems.append("counts: %d tier-1 cells, expected %d-%d" % (len(t1["cells"]), *CELLS_RANGE))
    for tier, found, q in (("tier 0", lines0, TIER0_Q), ("tier 1", lines1, Q)):
        for kind, lon, lat in LINE_PROBES:
            if not any(np.any((np.abs(ix / q - lon) <= PROBE_DEG) & (np.abs(iy / q - lat) <= PROBE_DEG))
                       for ix, iy in found.get(kind, ())):
                problems.append("probe: %s has no kind-%d border vertex within %g degree of %s,%s" % (tier, kind, PROBE_DEG, lon, lat))
        for kind, lon, lat in NO_LINE_PROBES:
            if any(np.any((np.abs(ix / q - lon) <= PROBE_DEG) & (np.abs(iy / q - lat) <= PROBE_DEG))
                   for ix, iy in found.get(kind, ())):
                problems.append("probe: %s has a kind-%d line near %s,%s, where none belongs" % (tier, kind, lon, lat))
    if not NAME_FIX_RANGE[0] <= counts.get("name_fixes_agreed", -1) <= NAME_FIX_RANGE[1] or counts.get("name_fixes_hand") != len(NAME_FIXES):
        problems.append("counts: %s names fixed by agreement (expected %d-%d), %s by hand (expected %d)" % (
            counts.get("name_fixes_agreed"), NAME_FIX_RANGE[0], NAME_FIX_RANGE[1], counts.get("name_fixes_hand"), len(NAME_FIXES)))
    ne_names = {p["name"] for p in tier0_places} | {p["name"] for ps in cell_places.values() for p in ps if not p["flags"] & FLAG_GEONAMES}
    for name, present in NAME_PROBES:
        if (name in ne_names) != present:
            problems.append("name probe: Natural Earth place %r %s" % (name, "missing" if present else "still there"))
    for where, key, max_rank, max_mz in PLACE_PROBES:
        pool = tier0_places if where == "tier0" else cell_places.get(where, [])
        if not any(fold(p["name"]) == key and p["rank"] <= max_rank and p["mz10"] <= max_mz for p in pool):
            problems.append("probe: no place %r (rank <= %d, min_zoom <= %d) in %s" % (key, max_rank, max_mz, where))
    every = degrees(tier0_places, TIER0_Q) + [x for ps in cell_places.values() for x in degrees(ps, Q)]
    for ne_key, gn_key, km in TWIN_PROBES:
        ne = [x for x in every if not x[2]["flags"] & FLAG_GEONAMES and fold(x[2]["name"]) == ne_key]
        if not ne:
            problems.append("twin probe: no Natural Earth place %r" % ne_key)
            continue
        for lat, lon, _p in ne:
            for glat, glon, g in every:
                if g["flags"] & FLAG_GEONAMES and fold(g["name"]) == gn_key and haversine_km(lat, lon, glat, glon) <= km:
                    problems.append("twin probe: GeoNames %r %.1f km from Natural Earth %r" % (
                        g["name"], haversine_km(lat, lon, glat, glon), ne_key))
    return problems


# ---------------------------------------------------------------- main

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--ne", help="directory holding the Natural Earth GeoJSON files (NE_FILES)")
    ap.add_argument("--geonames", help="cities1000.zip")
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
        if not is_real(json.load(open(os.path.join(a.out, "index.json")))):
            print("refusing to upload a build whose Natural Earth sources are not the pinned ones (real-build gates off)")
            return 1
        return 0 if C.upload(a.out, a.upload.strip("/"), replace=a.replace, files=files_of,
                             license_file=LICENSE_FILE) else 1
    if not (a.ne and a.geonames and a.out):
        ap.error("--ne, --geonames and --out are required to build")
    build(a.ne, a.geonames, a.out)
    return 1 if check(a.out) else 0


if __name__ == "__main__":
    sys.exit(main())
