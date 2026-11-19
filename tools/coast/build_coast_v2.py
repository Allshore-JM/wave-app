"""Coast set v2 (plan section 103, stage S1 Hawaii): the coast-v1 land set with GSHHG replaced, inside the Hawaii
bathymetry regions, by the bathymetry's own MHHW water edge.

Inputs: a coast-v1 build (static/coast/v1, from build_coast.py) and the published bathymetry tiles of the regions in
REGIONS at zooms zmin-15 (static/bathy/<region>/v<N>/, tools/bathy). Output: a directory in the same coast-v1 format
(the client and the alert builder read it unchanged), its index marked "kind": "land2".

Rule 1, the pixel class at zoom 15, per pixel inside each region box (the 256 x 256 core of a tile; the apron is ignored):
  * the zoom-15 tile when it exists (code > 0 = water, code 0 + source LAND_ID = land, code 0 + source 0 = outside);
  * else the nearest existing ancestor tile (zoom 14 down to the region's zmin), upsampled: all-land tiles are never
    written at 15, and the 1/3" CUDEM water lives at zooms 12-14 only;
  * where no tile exists at any zoom, or the class is "outside", GSHHG v1 tier 1 decides (pixel centre inside its land).
Rule 2, the trace: exact pixel-edge polygons of the land class per block (a zoom-12 tile = 8 x 8 zoom-15 tiles; runs of
land pixels merged into rectangles and unioned with shapely), all blocks unioned in global zoom-15 Web-Mercator pixel
space (integer grid lines join exactly), land rings and holes under SPECK_PX2 dropped (counted), then Douglas-Peucker
at TOL_PX with preserve_topology (holes stay inside their shells), then lon/lat, clipped to the region box.
Rule 3, the seam: each tier-1 cell that a region box overlaps AND whose content changes = union(GSHHG v1 outside the
boxes, v2 inside the boxes), split on the cell's own 5-degree lines, quantised at Q2 = 1e5 (land counter-clockwise, sea
water enclosed by land clockwise: the client's one nonzero fill and build_coast.point_in_pieces both read that right).
Every other tier-1 cell and the tier-0 file are copied from v1 byte for byte (the index proves it: base_v1).

Usage:
  python tools/coast/build_coast_v2.py --v1 build/coast/v1 --tiles build/bathy-tiles --fetch --out build/coast/v2
         (--fetch downloads the pinned bathymetry tiles, and the published v1 set when --v1 holds no index.json)
  python tools/coast/build_coast_v2.py --check build/coast/v2
  python tools/coast/build_coast_v2.py --out build/coast/v2 --upload static/coast/v2   (R2 env as build_coast.py;
         refuses a build that fails --check, is not from the pinned inputs, or differs from PIN_TIER1_SHA256)

Needs numpy, shapely and Pillow. Data: GSHHG 2.3.7 (LGPL-3.0) as in v1; NOAA NCEI CUDEM and NOAA NGS lidar (US
government, public domain) through the bathymetry tiles. LICENSE.txt beside the data carries both.
"""
import argparse
import hashlib
import json
import math
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import shapely
from shapely.geometry import Polygon, box as sbox

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "bathy"))
import build_coast as BC   # noqa: E402
import bathy_tiles as T    # noqa: E402

KIND = "land2"
Z = 15                                   # the class zoom (CUDEM 1/9" = zoom 15)
BLOCK = 8                                # a block = one zoom-12 tile = 8 x 8 zoom-15 tiles
BPX = BLOCK * T.TILE                     # 2048 px
WPX = T.TILE * 2 ** Z                    # the world in zoom-15 px
TOL_PX = 0.7                             # Douglas-Peucker tolerance, zoom-15 px (~3 m in Hawaii)
SPECK_PX2 = 2.0                          # land rings and holes smaller than this (zoom-15 px^2, ~40 m^2) are dropped
Q2 = 100000                              # the changed cells' quantum (1e-5 degree, ~1.1 m)
OUT, LAND, WATER = 0, 1, 2
PUBLIC = "https://models.allshoresurf.com"
UA = "allshore-coast-build/2 (+https://allshoresurf.com)"   # the edge refuses Python's default User-Agent

# The bathymetry regions replacing GSHHG (tools/bathy/bathy_sources.REGIONS: the same boxes; the tests compare them),
# pinned by the published index's digest (bathy index.json "digest") and by the sha256 of the zoom-zmin..15 tiles read
# (tiles_digest), so a real build always reads the same bytes.
REGIONS = {
    "hawaii": dict(v=2, box=(-160.5, 18.5, -154.5, 22.5), zmin=8,
                   digest="d5bdaf1b73a66d886654d93facc2e719067e0fd2484212ec0187cfaa9f5cadd1",
                   tiles_sha256="4d30de1c827b8afd23e386e22b31080c0f29fc119fe0221244f5f7e9a38c07e2"),
    "nwhi-lidar": dict(v=1, box=(-168.5, 22.5, -160.5, 24.5), zmin=8,
                       digest="0ae9561cfccb63f6724bd481fb65e8bf12ac5ddb3615ca72bebe9750fc32985e",
                       tiles_sha256="2c040fd2e5ca9172701ed3e578ba29c67f91aa854b43e3094d7f64d46b1e09b8"),
}
# The published v1 set this build starts from (static/coast/v1 index.json tier0.sha256 / tier1.sha256).
V1_PIN_TIER0_SHA256 = "aabbff516107953c42915a11c70941682030ee3aecc29b0d0456d33b3ef9cebc"
V1_PIN_TIER1_SHA256 = "a4b97403aac09c0a4ca88f50d77d8e339eb061342204a0dd227294e71cf001c6"
# The tier-1 cells a real build changes (every other cell is v1's bytes).
EXPECTED_CHANGED = ["15_-155", "15_-160", "20_-160", "20_-165", "20_-170"]
# The exact bytes of a real build (deterministic from the pinned inputs, this code and the pinned shapely): a
# deliberate change updates this pin. Tier 0 is v1's (V1_PIN_TIER0_SHA256).
PIN_TIER1_SHA256 = None   # set from the runner's build (its artifact's index.json): the laptop (Windows) differs by a few dozen vertices
# Land area of EXPECTED_CHANGED in v1 (sq deg, build_coast.signed_area of the q=1e4 rings, unweighted as in
# build_coast.check): the v2 total stays within AREA_TOL of it. Judged on the total only: per cell, Niihau (20_-165)
# loses ~6 % and French Frigate Shoals (20_-170, GSHHG draws the reef as land) ~85 % (step-1 report).
V1_CHANGED_SQDEG = 1.456635                      # a real v2 build: 1.43924 (-1.19 %)
AREA_TOL = 0.03
# Hawaii probes (lon, lat, land?): rule 1 at zoom 15 with a margin; picked from the data in step 1.
HAWAII_PROBES = [
    (-158.11281, 21.58959, False),      # Haleiwa: the 250-300 m band GSHHG drew as land, 62 m off the MHHW edge
    (-158.04895, 21.66794, False),      # Sunset Beach: the same band
    (-157.805, 21.315, True),           # Honolulu inland (Manoa), class from a zoom-12 ancestor, 2.2 km inland
    (-157.958, 21.372, False),          # Pearl Harbor, East Loch (GSHHG: land)
    (-157.757, 21.385, True),           # Kawainui Marsh: below MHHW but not joined to the sea
    (-155.468, 19.821, True),           # Mauna Kea: the class from a zoom-9 ancestor
    (-155.00417, 19.33659, False),      # Puna: an 8-px pit (to 51 m deep, so "sea" by the water rule) = a CW hole
    (-166.20, 23.78, False),            # French Frigate Shoals lagoon (GSHHG: water too)
    (-161.922, 23.061, True),           # Nihoa (GSHHG's Nihoa is offset: water here)
]
FORMAT_KEY = ("coast-v1-land2|base=coast-v1|z=%d|block=%d|tol=%g|speck=%g|q=%d|rule=tile>ancestor>gshhg|boxes=%s|"
              "shapely=%s" % (Z, BLOCK, TOL_PX, SPECK_PX2, Q2,
                              ";".join("%s:v%d:%s" % (n, r["v"], ",".join("%g" % v for v in r["box"]))
                                       for n, r in sorted(REGIONS.items())),
                              ".".join(shapely.__version__.split(".")[:2])))
SOURCE = "GSHHG 2.3.7 + NOAA bathymetry MHHW water edge (allshoresurf bathy regions %s)" % ", ".join(sorted(REGIONS))
LICENSE = BC.LICENSE + "; NOAA NCEI CUDEM and NOAA NGS lidar (public domain) inside the bathymetry regions"
NOAA_CREDIT = """
Inside the bathymetry regions (Hawaii: 160.5-154.5 W, 18.5-22.5 N; the NW Hawaiian Islands: 168.5-160.5 W,
22.5-24.5 N) the coastline is the mean-higher-high-water edge of the allshoresurf.com bathymetry tiles, built from
NOAA NCEI Continuously Updated Digital Elevation Models (CUDEM, 1/9 and 1/3 arc-second, Hawaii) and the NOAA NGS 2021
Northwestern Hawaiian Islands topobathy lidar (NOAA Digital Coast). These US government data are in the public domain.
Not for navigation.
"""


def log(msg):
    print(msg, flush=True)


# ---------------------------------------------------------------- inputs

def _ssl_context():
    """certifi's list, not the system's (plan section 57: a compromised chain stayed trusted in the runner's list)."""
    import certifi
    return ssl.create_default_context(cafile=certifi.where())


def _get(url, timeout=60, tries=4):
    ctx = _ssl_context()
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as exc:
            if exc.code in (403, 404) or i == tries - 1:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            if i == tries - 1:
                raise
        time.sleep(1 + 2 * i)


def _idx_offsets(zmax):
    """build_bathy.idx_offsets: each zoom's bitset inside an idx file (zooms 11..zmax)."""
    out, off = {}, 0
    for z in range(11, zmax + 1):
        n = ((2 ** (z - 10)) ** 2 + 7) // 8
        out[z] = (off, n)
        off += n
    return out


def listed_tiles(index, idx_bytes):
    """[(z, x, y)] of the zooms zmin..Z a bathy index lists: zooms <= 10 from index["levels"], finer ones from the idx
    files (idx_bytes: {"x_y": bytes})."""
    import base64
    out = []
    for z, L in index.get("levels", {}).items():
        if int(z) > Z:
            continue
        m = T.unpack_bits(base64.b64decode(L["bits"]), (L["h"], L["w"]))
        for r, c in zip(*np.nonzero(m)):
            out.append((int(z), L["x0"] + int(c), L["y0"] + int(r)))
    offs = _idx_offsets(int(index["zmax"])) if index.get("idx") else {}
    for cell in index.get("t10") or ():
        cx, cy = (int(v) for v in cell.split("_"))
        data = idx_bytes[cell]
        for z in range(11, min(Z, int(index["zmax"])) + 1):
            k = 2 ** (z - 10)
            o, n = offs[z]
            m = T.unpack_bits(data[o:o + n], (k, k))
            for r, c in zip(*np.nonzero(m)):
                out.append((z, cx * k + int(c), cy * k + int(r)))
    return sorted(out)


def fetch_tiles(tiles_dir, regions=None, workers=16):
    """Download the pinned regions' index, idx files and tiles of zooms zmin..15 into tiles_dir/<region>/v<N>/."""
    regions = regions or REGIONS
    for name, reg in sorted(regions.items()):
        base = "%s/static/bathy/%s/v%d" % (PUBLIC, name, reg["v"])
        d = os.path.join(tiles_dir, name, "v%d" % reg["v"])
        os.makedirs(os.path.join(d, "idx"), exist_ok=True)
        raw = _get(base + "/index.json")
        index = json.loads(raw.decode("utf-8"))
        if index.get("digest") != reg["digest"]:
            raise ValueError("%s v%d: published digest %s is not the pinned %s" % (
                name, reg["v"], str(index.get("digest"))[:12], reg["digest"][:12]))
        open(os.path.join(d, "index.json"), "wb").write(raw)
        idx = {}
        for cell in index.get("t10") or ():
            p = os.path.join(d, "idx", cell + ".bin")
            if not os.path.exists(p):
                open(p, "wb").write(_get(base + "/idx/%s.bin" % cell))
            idx[cell] = open(p, "rb").read()
        want = listed_tiles(index, idx)

        def one(t):
            z, x, y = t
            p = os.path.join(d, "t", str(z), str(x), "%d.png" % y)
            if os.path.exists(p) and os.path.getsize(p) > 0:
                return 0
            os.makedirs(os.path.dirname(p), exist_ok=True)
            data = _get(base + "/t/%d/%d/%d.png" % t)
            with open(p + ".part", "wb") as f:
                f.write(data)
            os.replace(p + ".part", p)
            return 1
        t0 = time.time()
        with ThreadPoolExecutor(workers) as ex:
            n = sum(ex.map(one, want))
        log("fetch %s v%d: %d tiles listed at zooms %d-%d, %d downloaded (%.0f s)" % (
            name, reg["v"], len(want), reg["zmin"], Z, n, time.time() - t0))


def fetch_v1(v1_dir):
    """Download the published static/coast/v1 set (index, tier 0, every tier-1 cell) into v1_dir."""
    base = PUBLIC + "/static/coast/v1"
    raw = _get(base + "/index.json")
    index = json.loads(raw.decode("utf-8"))
    os.makedirs(os.path.join(v1_dir, index["tier1"]["dir"]), exist_ok=True)
    rels = [index["tier0"]["file"]] + ["%s/%s.bin" % (index["tier1"]["dir"], n) for n in sorted(index["tier1"]["cells"])]

    def one(rel):
        p = os.path.join(v1_dir, rel)
        if not os.path.exists(p):
            open(p, "wb").write(_get(base + "/" + rel))
    with ThreadPoolExecutor(16) as ex:
        list(ex.map(one, rels))
    open(os.path.join(v1_dir, "index.json"), "wb").write(raw)
    log("fetch v1: %d files" % len(rels))


class Tiles:
    """One region's tiles on disk (tiles_dir/<region>/v<N>/t/z/x/y.png + index.json): presence, classes, ancestors."""

    def __init__(self, tiles_dir, name, reg):
        self.name, self.reg = name, reg
        self.dir = os.path.join(tiles_dir, name, "v%d" % reg["v"])
        self.index = json.load(open(os.path.join(self.dir, "index.json")))
        if int(self.index.get("version", -1)) != reg["v"] or self.index.get("region", name) != name:
            raise ValueError("%s: index is version %s, expected %d" % (name, self.index.get("version"), reg["v"]))
        self.present = {}
        for z in range(reg["zmin"], Z + 1):
            s = set()
            zd = os.path.join(self.dir, "t", str(z))
            if os.path.isdir(zd):
                for x in os.listdir(zd):
                    for f in os.listdir(os.path.join(zd, x)):
                        if f.endswith(".png"):
                            s.add((int(x), int(f[:-4])))
            self.present[z] = s
            want = int((self.index.get("tiles") or {}).get(str(z), 0))
            if len(s) != want:
                raise ValueError("%s: zoom %d holds %d tiles, the index lists %d (incomplete download?)"
                                 % (name, z, len(s), want))
        self._anc = {}
        self.anomalies = 0          # code 0 with a source that is neither LAND_ID nor 0, or water with LAND_ID

    def digest(self):
        """sha256 over the tiles read (zooms zmin..15): 'z/x/y.png' + each file's sha256, sorted."""
        h = hashlib.sha256()
        n = 0
        for z in sorted(self.present):
            for (x, y) in sorted(self.present[z]):
                rel = "%d/%d/%d.png" % (z, x, y)
                h.update(rel.encode() + b"\0" + hashlib.sha256(open(os.path.join(self.dir, "t", rel), "rb").read()).digest())
                n += 1
        return h.hexdigest(), n

    def cls(self, z, x, y):
        with open(os.path.join(self.dir, "t", str(z), str(x), "%d.png" % y), "rb") as f:
            code, sf = T.unpack_tile(f.read())
        code, src = code[1:1 + T.TILE, 1:1 + T.TILE], sf[1:1 + T.TILE, 1:1 + T.TILE] & T.SRC_BITS
        c = np.full(code.shape, OUT, np.uint8)
        c[code > 0] = WATER
        c[(code == 0) & (src == T.LAND_ID)] = LAND
        self.anomalies += int(((code == 0) & (src != T.LAND_ID) & (src != 0)).sum())
        self.anomalies += int(((code > 0) & (src == T.LAND_ID)).sum())
        return c

    def ancestor(self, x, y):
        for za in range(Z - 1, self.reg["zmin"] - 1, -1):
            sh = Z - za
            if (x >> sh, y >> sh) in self.present.get(za, ()):
                return za, x >> sh, y >> sh
        return None

    def anc_slice(self, x, y, a):
        """The k x k slice (k = 256 >> (15 - za)) of ancestor a covering zoom-15 tile x/y."""
        za, xa, ya = a
        if a not in self._anc:
            if len(self._anc) > 3000:
                self._anc.clear()
            self._anc[a] = self.cls(za, xa, ya)
        sh = Z - za
        k = T.TILE >> sh
        ox, oy = (x - (xa << sh)) * k, (y - (ya << sh)) * k
        return self._anc[a][oy:oy + k, ox:ox + k]


# ---------------------------------------------------------------- geometry helpers

def px_lon(X):
    return np.asarray(X, np.float64) / WPX * 360.0 - 180.0


def px_lat(Y):
    return np.degrees(np.arctan(np.sinh(np.pi * (1.0 - 2.0 * np.asarray(Y, np.float64) / WPX))))


def lonlat_px(lon, lat):
    s = np.sin(np.radians(lat))
    return (np.asarray(lon, np.float64) + 180.0) / 360.0 * WPX, (0.5 - np.log((1 + s) / (1 - s)) / (4 * np.pi)) * WPX


def to_lonlat(geom):
    return shapely.transform(geom, lambda a: np.column_stack([px_lon(a[:, 0]), px_lat(a[:, 1])]))


def polygons(geom):
    """The Polygon parts of any geometry (intersections may also yield lines and points)."""
    if geom is None or geom.is_empty:
        return []
    if geom.geom_type == "Polygon":
        return [geom]
    out = []
    for g in getattr(geom, "geoms", ()):
        out.extend(polygons(g))
    return out


def mask_polygons(mask, X0=0, Y0=0):
    """Exact pixel-edge polygons of a boolean mask (row 0 at Y0, column 0 at X0, y down): runs of True pixels per row,
    equal runs of consecutive rows merged into rectangles, all unioned. Pixels touching only at a corner stay apart
    (or meet at a single point)."""
    h, w = mask.shape
    if not mask.any():
        return []
    m = np.zeros((h, w + 2), np.int8)
    m[:, 1:-1] = mask
    d = np.diff(m, axis=1)
    rs, c0 = np.nonzero(d == 1)
    _, c1 = np.nonzero(d == -1)
    order = np.lexsort((rs, c1, c0))
    r, a, b = rs[order], c0[order], c1[order]
    new = np.ones(len(r), bool)
    new[1:] = (a[1:] != a[:-1]) | (b[1:] != b[:-1]) | (r[1:] != r[:-1] + 1)
    starts = np.nonzero(new)[0]
    ends = np.append(starts[1:], len(r)) - 1
    boxes = shapely.box(X0 + a[starts], Y0 + r[starts], X0 + b[starts], Y0 + r[ends] + 1)
    return polygons(shapely.union_all(boxes))


def drop_specks(geom, min_area):
    """-> (polygons, land specks dropped, hole specks filled)."""
    kept, n_land, n_hole = [], 0, 0
    for p in polygons(geom):
        if p.area < min_area:
            n_land += 1
            continue
        holes = []
        for r in p.interiors:
            if Polygon(r).area < min_area:
                n_hole += 1
            else:
                holes.append(r)
        kept.append(Polygon(p.exterior, holes) if len(holes) != len(p.interiors) else p)
    return kept, n_land, n_hole


# ---------------------------------------------------------------- v1

def load_v1(v1_dir):
    index = json.load(open(os.path.join(v1_dir, "index.json")))
    if index.get("format") != "coast-v1" or index.get("kind") is not None or index.get("q") != BC.Q:
        raise ValueError("--v1 %s is not a coast-v1 land set" % v1_dir)
    return index


def v1_cell_geom(v1_dir, index, name, cache={}):   # noqa: B006 (a per-process cache)
    """(raw bytes or None, shapely union of the cell's land in lon/lat or None, rings as lon/lat arrays)."""
    key = (v1_dir, name)
    if key not in cache:
        if name not in index["tier1"]["cells"]:
            cache[key] = (None, None, [])
        else:
            buf = open(os.path.join(v1_dir, index["tier1"]["dir"], name + ".bin"), "rb").read()
            d = BC.decode_file(buf)
            rings, polys = [], []
            for pc in d["pieces"]:
                ps = [np.column_stack([ix / d["q"], iy / d["q"]]) for ix, iy in pc["rings"]]
                rings.extend(ps)
                p = Polygon(ps[0], ps[1:])
                polys.append(p if p.is_valid else shapely.make_valid(p))
            cache[key] = (buf, shapely.union_all(polys) if polys else None, rings)
    return cache[key]


def cells_overlapping(box_, cell=5):
    lon0, lat0, lon1, lat1 = box_
    out = []
    for la in range(int(math.floor(lat0 / cell)) * cell, int(math.ceil(lat1 / cell)) * cell, cell):
        for lo in range(int(math.floor(lon0 / cell)) * cell, int(math.ceil(lon1 / cell)) * cell, cell):
            out.append(BC.cell_name(la, lo))
    return out


def gshhg_near(v1_dir, index, lonlat_box):
    """The v1 land overlapping a lon/lat box (prepared), or None."""
    parts = []
    for name in cells_overlapping(lonlat_box):
        g = v1_cell_geom(v1_dir, index, name)[1]
        if g is not None and g.intersects(sbox(*lonlat_box)):
            parts.append(g)
    if not parts:
        return None
    g = shapely.union_all(parts)
    shapely.prepare(g)
    return g


# ---------------------------------------------------------------- trace

def block_class(tiles, bx, by, v1_dir, v1_index, stats):
    """The zoom-15 class raster (BPX x BPX) of block bx/by by rule 1, or None when it can hold no land."""
    reg = tiles.reg
    X0, Y0 = bx * BPX, by * BPX
    lon_w, lon_e = float(px_lon(X0)), float(px_lon(X0 + BPX))
    lat_n, lat_s = float(px_lat(Y0)), float(px_lat(Y0 + BPX))
    b = reg["box"]
    if lon_e <= b[0] or lon_w >= b[2] or lat_n <= b[1] or lat_s >= b[3]:
        return None
    parts, any_land, any_out = [], False, False
    for j in range(BLOCK):
        for i in range(BLOCK):
            x, y = bx * BLOCK + i, by * BLOCK + j
            if (x, y) in tiles.present[Z]:
                c = tiles.cls(Z, x, y)
                kind = "z15"
            else:
                a = tiles.ancestor(x, y)
                c = None if a is None else tiles.anc_slice(x, y, a)
                kind = "none" if a is None else "ancestor"
            stats[kind] += 1
            if c is None:
                any_out = True
            else:
                any_land = any_land or bool((c == LAND).any())
                any_out = any_out or bool((c == OUT).any())
            parts.append((i, j, c))
    gs = None
    if any_out:
        gs = gshhg_near(v1_dir, v1_index, (max(lon_w, b[0]), max(lat_s, b[1]), min(lon_e, b[2]), min(lat_n, b[3])))
    if not any_land and gs is None:
        return None
    cls = np.full((BPX, BPX), OUT, np.uint8)
    for i, j, c in parts:
        if c is None:
            continue
        rep = T.TILE // c.shape[0]
        if rep > 1:
            c = np.repeat(np.repeat(c, rep, 0), rep, 1)
        cls[j * T.TILE:(j + 1) * T.TILE, i * T.TILE:(i + 1) * T.TILE] = c
    lons = px_lon(X0 + np.arange(BPX) + 0.5)
    lats = px_lat(Y0 + np.arange(BPX) + 0.5)
    inx = (lons >= b[0]) & (lons <= b[2])
    iny = (lats >= b[1]) & (lats <= b[3])
    inbox = iny[:, None] & inx[None, :]
    cls[~inbox] = OUT                          # outside the box: GSHHG's, added at the cell stage
    if gs is not None:
        rr, cc = np.nonzero((cls == OUT) & inbox)
        if len(rr):
            land = shapely.contains_xy(gs, lons[cc], lats[rr])
            cls[rr[land], cc[land]] = LAND
            stats["gshhg_px"] += int(land.sum())
    cls[~inbox] = OUT
    return cls


def trace_region(tiles, v1_dir, v1_index, log=log):
    """-> (the region's land as [(shell, [holes])] rings of lon/lat arrays (shell CCW, holes CW), stats).

    Every GEOS operation runs on integer zoom-15 pixel coordinates (the union of pixel rectangles, the speck filter, the
    simplifier, which keeps a subset of its input vertices): exact on any platform. The lon/lat conversion is then point
    by point. (A first version clipped and unioned in lon/lat: the runner's build differed from the laptop's by a few
    dozen vertices, so its hash could not be pinned. Land never reaches a box edge in S1: the raster already stops at the
    box, by pixel centre.)"""
    t0 = time.time()
    reg = tiles.reg
    stats = dict(z15=0, ancestor=0, none=0, blocks=0, blocks_traced=0, gshhg_px=0)
    x0, y0, x1, y1 = T.tiles_in_box(reg["box"], Z - 3)
    polys = []
    for bx in range(x0, x1 + 1):
        for by in range(y0, y1 + 1):
            stats["blocks"] += 1
            cls = block_class(tiles, bx, by, v1_dir, v1_index, stats)
            if cls is None:
                continue
            land = cls == LAND
            if land.any():
                stats["blocks_traced"] += 1
                polys.extend(mask_polygons(land, bx * BPX, by * BPX))
    stats["t_blocks_s"] = round(time.time() - t0, 1)
    u = shapely.union_all(polys) if polys else shapely.MultiPolygon()
    kept, stats["speck_land"], stats["speck_holes"] = drop_specks(u, SPECK_PX2)
    simp = shapely.MultiPolygon(kept).simplify(TOL_PX, preserve_topology=True) if kept else shapely.MultiPolygon()
    out = []
    for p in polygons(simp):
        rings = [np.asarray(p.exterior.coords, np.float64)[:-1]] + [np.asarray(r.coords, np.float64)[:-1] for r in p.interiors]
        ll = [(px_lon(r[:, 0]), px_lat(r[:, 1])) for r in rings]
        sh = ll[0] if BC.signed_area(*ll[0]) > 0 else (ll[0][0][::-1].copy(), ll[0][1][::-1].copy())
        holes = [h if BC.signed_area(*h) < 0 else (h[0][::-1].copy(), h[1][::-1].copy()) for h in ll[1:]]
        out.append((sh, holes))
    stats["anomaly_px"] = tiles.anomalies
    stats["t_total_s"] = round(time.time() - t0, 1)
    stats["polygons"] = len(out)
    stats["holes"] = sum(len(h) for _, h in out)
    log("trace %s: %s" % (tiles.name, stats))
    return out, stats


# ---------------------------------------------------------------- cells

def ring_q(coords, ccw):
    a = np.asarray(coords, np.float64)[:-1]
    r = BC.quantize_ring(a[:, 0], a[:, 1], Q2)
    if r is None:
        return None
    if (BC.signed_area(r[0].astype(np.float64), r[1].astype(np.float64)) > 0) != ccw:
        r = (r[0][::-1].copy(), r[1][::-1].copy())
    return r


def clip_to_cell(x, y, lo, la, deg):
    """One ring clipped to a cell with build_coast's exact Sutherland-Hodgman (the v1 cells' own clip), or None."""
    for axis, c, keep_le in ((0, lo, False), (0, lo + deg, True), (1, la, False), (1, la + deg, True)):
        a = x if axis == 0 else y
        if (a >= c).all() if not keep_le else (a <= c).all():
            continue
        r = BC.clip_half(x, y, axis, c, keep_le)
        if r is None:
            return None
        x, y = r
    return x, y


def cell_pieces_exact(parts, lo, la, deg):
    """Quantised pieces of a cell from [(shell, holes)] lon/lat rings: rings wholly inside the cell as they are, the rest
    clipped ring by ring (a hole goes with its shell). Arithmetic only, no GEOS: the same bytes on every platform."""
    pieces = []
    for sh, holes in parts:
        if sh[0].max() <= lo or sh[0].min() >= lo + deg or sh[1].max() <= la or sh[1].min() >= la + deg:
            continue
        c = clip_to_cell(sh[0], sh[1], lo, la, deg)
        if c is None:
            continue
        shq = BC.quantize_ring(c[0], c[1], Q2)
        if shq is None or BC.signed_area(shq[0].astype(np.float64), shq[1].astype(np.float64)) <= 0:
            continue
        rings = [shq]
        for h in holes:
            ch = clip_to_cell(h[0], h[1], lo, la, deg)
            hq = BC.quantize_ring(ch[0], ch[1], Q2) if ch is not None else None
            if hq is not None and BC.signed_area(hq[0].astype(np.float64), hq[1].astype(np.float64)) < 0:
                rings.append(hq)
        pieces.append(rings)
    return pieces


def sort_pieces(pieces):
    pieces.sort(key=lambda rs: (int(rs[0][0].min()), int(rs[0][1].min()), len(rs[0][0]), int(rs[0][0][0]), int(rs[0][1][0])))
    return pieces


def cell_pieces(geom):
    """Quantised pieces (shell CCW, holes CW) of a lon/lat geometry, sorted for determinism."""
    pieces = []
    for p in polygons(geom):
        sh = ring_q(p.exterior.coords, True)
        if sh is None:
            continue
        rings = [sh]
        for h in p.interiors:
            r = ring_q(h.coords, False)
            if r is not None:
                rings.append(r)
        pieces.append(rings)
    pieces.sort(key=lambda rs: (int(rs[0][0].min()), int(rs[0][1].min()), len(rs[0][0]), int(rs[0][0][0]), int(rs[0][1][0])))
    return pieces


def build(v1_dir, tiles_dir, out_dir, regions=None, log=log):
    regions = regions or REGIONS
    t0 = time.time()
    v1 = load_v1(v1_dir)
    os.makedirs(os.path.join(out_dir, v1["tier1"]["dir"]), exist_ok=True)
    region_geom, bathy, rstats = {}, {}, {}
    for name, reg in sorted(regions.items()):
        tiles = Tiles(tiles_dir, name, reg)
        sha, n = tiles.digest()
        region_geom[name], rstats[name] = trace_region(tiles, v1_dir, v1, log)
        bathy[name] = {"v": reg["v"], "z": Z, "zmin": reg["zmin"], "box": list(reg["box"]),
                       "digest": tiles.index.get("digest"), "tiles": n, "sha256": sha}
    boxes = shapely.union_all([sbox(*r["box"]) for r in regions.values()])
    boxes_line = boxes.boundary
    v2_parts = [pt for name in sorted(region_geom) for pt in region_geom[name]]   # the boxes touch at a corner only
    candidates = sorted({c for r in regions.values() for c in cells_overlapping(r["box"])})
    listing, digests, changed, base_changed, crossing = {}, {}, [], {}, 0
    tot_bytes = tot_verts = 0
    cell_stats = {}
    deg = v1["tier1"]["cell"]
    for name in sorted(set(v1["tier1"]["cells"]) | set(candidates)):
        src = os.path.join(v1_dir, v1["tier1"]["dir"], name + ".bin")
        buf = None
        if name in candidates:
            la, lo = (int(v) for v in name.split("_"))
            cbox = sbox(lo, la, lo + deg, la + deg)
            raw, g1, rings = v1_cell_geom(v1_dir, v1, name)
            n_cross = sum(1 for r in rings if shapely.LinearRing(r).intersects(boxes_line))
            crossing += n_cross
            v2_here = cell_pieces_exact(v2_parts, lo, la, deg)
            d1 = BC.decode_file(raw) if raw is not None else {"pieces": [], "q": BC.Q}
            v1_hit = [bool(shapely.intersects(Polygon(np.column_stack([pc["rings"][0][0], pc["rings"][0][1]]) / d1["q"]), boxes))
                      for pc in d1["pieces"]]
            if v2_here or any(v1_hit):
                if n_cross == 0:
                    # the exact path: v2's pieces + the v1 pieces outside every box, v1's integers scaled to the new quantum
                    k = Q2 // d1["q"]
                    pieces = sort_pieces(v2_here + [[(ix * k, iy * k) for ix, iy in pc["rings"]]
                                                    for pc, hit in zip(d1["pieces"], v1_hit) if not hit])
                else:
                    # a GSHHG ring crosses a box edge (none in S1): GSHHG outside the boxes and v2 inside joined by GEOS
                    log("cell %s: %d v1 rings cross a box edge: joined with GEOS (not bit-exact across platforms)" % (name, n_cross))
                    v2g = [Polygon(np.column_stack(sh), [np.column_stack(h) for h in hs]) for sh, hs in v2_parts]
                    geom = shapely.union_all([shapely.union_all(v2g).intersection(boxes)] + ([g1.difference(boxes)] if g1 is not None else []))
                    pieces = cell_pieces(geom.intersection(cbox))
                changed.append(name)
                base_changed[name] = hashlib.sha256(raw).hexdigest() if raw is not None else None
                nh = sum(len(pc) - 1 for pc in pieces)
                cell_stats[name] = {"pieces": len(pieces), "holes": nh, "v1_rings_crossing_box": n_cross}
                if pieces:
                    buf = BC.encode_file(pieces, deg, q=Q2)
                    log("cell %s: %d pieces, %d holes, %d vertices, %d bytes (v1 %s bytes)" % (
                        name, len(pieces), nh, sum(len(r[0]) for pc in pieces for r in pc), len(buf),
                        len(raw) if raw is not None else "-"))
        if buf is None and name not in changed and name in v1["tier1"]["cells"]:
            buf = open(src, "rb").read()
        if buf is None:
            continue
        open(os.path.join(out_dir, v1["tier1"]["dir"], name + ".bin"), "wb").write(buf)
        d = BC.decode_file(buf)
        nv = sum(len(r[0]) for pc in d["pieces"] for r in pc["rings"])
        listing[name] = [len(buf), nv]
        digests[name] = hashlib.sha256(buf).hexdigest()
        tot_bytes += len(buf)
        tot_verts += nv
    t0f = v1["tier0"]["file"]
    tier0 = open(os.path.join(v1_dir, t0f), "rb").read()
    open(os.path.join(out_dir, t0f), "wb").write(tier0)
    index = {k: v for k, v in v1.items() if k not in ("tier1",)}
    index.update({
        "kind": KIND, "format": "coast-v1", "format_key": FORMAT_KEY, "q": Q2, "source": SOURCE, "license": LICENSE,
        "built_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "base_v1": {"tier0_sha": v1["tier0"]["sha256"], "tier1_sha": v1["tier1"]["sha256"], "changed_sha": base_changed},
        "bathy": bathy, "changed": changed, "v1_rings_crossing_box": crossing,
        "stats": {"regions": rstats, "cells": cell_stats},
        "versions": {"shapely": shapely.__version__, "numpy": np.__version__},
        "tier1": {"dir": v1["tier1"]["dir"], "res": v1["tier1"]["res"], "cell": deg, "min_zoom": v1["tier1"]["min_zoom"],
                  "cells": listing, "bytes": tot_bytes, "vertices": tot_verts, "sha256": BC.cells_digest(digests)},
    })
    with open(os.path.join(out_dir, "index.json"), "w") as fh:
        json.dump(index, fh, separators=(",", ":"), sort_keys=True)
    with open(os.path.join(out_dir, "LICENSE.txt"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write(open(BC.LICENSE_FILE, encoding="utf-8").read().rstrip("\n") + "\n" + NOAA_CREDIT)
    log("v2: %d cells changed %s, %d v1 rings cross a box edge; tier 1 %d cells, %d vertices, %d bytes, sha %s (%.0f s)"
        % (len(changed), changed, crossing, len(listing), tot_verts, tot_bytes, index["tier1"]["sha256"][:12],
           time.time() - t0))
    return index


# ---------------------------------------------------------------- check

def _pinned(value):
    return value is not None


def is_real(index, regions=None):
    """Built from the pinned inputs (never judged from the result)."""
    regions = regions or REGIONS
    b = index.get("bathy") or {}
    base = index.get("base_v1") or {}
    if set(b) != set(regions) or base.get("tier0_sha") != V1_PIN_TIER0_SHA256 or base.get("tier1_sha") != V1_PIN_TIER1_SHA256:
        return False
    for name, reg in regions.items():
        e = b[name]
        if e.get("v") != reg["v"] or e.get("digest") != reg["digest"]:
            return False
        if _pinned(reg["tiles_sha256"]) and e.get("sha256") != reg["tiles_sha256"]:
            return False
    return True


def check(out_dir, log=log, regions=None):
    regions = regions or REGIONS
    index = json.load(open(os.path.join(out_dir, "index.json")))
    problems = []
    if index.get("kind") != KIND:
        return ["index kind %r is not %r" % (index.get("kind"), KIND)]
    if index.get("format") != "coast-v1" or index.get("q") != Q2:
        problems.append("index format/q")
    if index.get("format_key") != FORMAT_KEY:
        problems.append("index format_key %r != this builder's %r" % (index.get("format_key"), FORMAT_KEY))
    real = is_real(index, regions)
    base = index.get("base_v1") or {}
    t0 = index["tier0"]
    buf = open(os.path.join(out_dir, t0["file"]), "rb").read()
    sha0 = hashlib.sha256(buf).hexdigest()
    if len(buf) != t0["bytes"] or sha0 != t0["sha256"]:
        problems.append("tier 0 size/hash mismatch")
    if sha0 != base.get("tier0_sha"):
        problems.append("tier 0 is not v1's bytes")
    d0 = BC.decode_file(buf)
    for lon, lat, land in BC.PROBES if real else []:
        if BC.point_in_pieces(lon, lat, d0["pieces"], d0["q"]) != land:
            problems.append("tier 0: probe %s,%s should be %s" % (lon, lat, "land" if land else "water"))
    t1 = index["tier1"]
    changed = set(index.get("changed") or ())
    present = set(n[:-4] for n in os.listdir(os.path.join(out_dir, t1["dir"])) if n.endswith(".bin"))
    if present != set(t1["cells"]):
        problems.append("tier 1: files %d vs index %d" % (len(present), len(t1["cells"])))
    if real and sorted(changed) != sorted(EXPECTED_CHANGED):
        problems.append("changed cells %s != the expected %s" % (sorted(changed), EXPECTED_CHANGED))
    digests, v1_digests, area = {}, dict(base.get("changed_sha") or {}), 0.0
    n_holes = 0
    for name, (nbytes, nverts) in sorted(t1["cells"].items()):
        buf = open(os.path.join(out_dir, t1["dir"], name + ".bin"), "rb").read()
        digests[name] = hashlib.sha256(buf).hexdigest()
        if name not in changed:
            v1_digests[name] = digests[name]
        if len(buf) != nbytes:
            problems.append("tier 1 %s: size mismatch" % name)
        lat0, lon0 = (int(v) for v in name.split("_"))
        problems += ["tier 1 %s: %s" % (name, p) for p in BC.check_file(buf, t1["cell"], (lat0, lon0, t1["cell"]),
                                                                         ccw_only=False)]
        try:
            d = BC.decode_file(buf)
        except Exception:                             # noqa: BLE001  (already reported by check_file)
            continue
        if sum(len(r[0]) for pc in d["pieces"] for r in pc["rings"]) != nverts:
            problems.append("tier 1 %s: vertex count != index" % name)
        want_q = Q2 if name in changed else BC.Q
        if d["q"] != want_q:
            problems.append("tier 1 %s: q %d, expected %d" % (name, d["q"], want_q))
        if name in changed:
            q = d["q"]
            for k, pc in enumerate(d["pieces"]):
                rings = pc["rings"]
                if BC.signed_area(rings[0][0].astype(np.float64), rings[0][1].astype(np.float64)) <= 0:
                    problems.append("tier 1 %s: piece %d shell not counter-clockwise" % (name, k))
                shell = None
                for ix, iy in rings[1:]:
                    shell = shell or Polygon(np.column_stack([rings[0][0], rings[0][1]])).buffer(0.5)
                    n_holes += 1
                    if BC.signed_area(ix.astype(np.float64), iy.astype(np.float64)) >= 0:
                        problems.append("tier 1 %s: piece %d hole not clockwise" % (name, k))
                    if not shell.covers(Polygon(np.column_stack([ix, iy]))):
                        problems.append("tier 1 %s: piece %d hole outside its shell" % (name, k))
                area += sum(BC.signed_area(ix / q, iy / q) for ix, iy in rings)
                if len(problems) > 60:
                    break
        for lon, lat, land in (BC.PROBES + HAWAII_PROBES) if real else []:
            if lat0 <= lat < lat0 + t1["cell"] and lon0 <= lon < lon0 + t1["cell"] and \
                    BC.point_in_pieces(lon, lat, d["pieces"], d["q"]) != land:
                problems.append("tier 1 %s: probe %s,%s should be %s" % (name, lon, lat, "land" if land else "water"))
    for lon, lat, land in (BC.PROBES + HAWAII_PROBES) if real else []:
        name = BC.cell_name(int(np.floor(lat / t1["cell"])) * t1["cell"], int(np.floor(lon / t1["cell"])) * t1["cell"])
        if land and name not in t1["cells"]:
            problems.append("tier 1: probe %s,%s should be land but its cell %s is absent" % (lon, lat, name))
    # unchanged cells are v1's bytes: v1's own tier-1 digest rebuilt from them + the replaced cells' v1 hashes
    v1_digests = {k: v for k, v in v1_digests.items() if v is not None}
    if BC.cells_digest(v1_digests) != base.get("tier1_sha"):
        problems.append("tier 1: the unchanged cells + base_v1.changed_sha do not rebuild v1's tier-1 hash")
    if present == set(t1["cells"]) and t1.get("sha256") != BC.cells_digest(digests):
        problems.append("tier 1: content hash != index (tier1.sha256)")
    if real:
        for name, reg in regions.items():
            if not _pinned(reg["tiles_sha256"]):
                log("  note: %s tiles_sha256 not pinned yet (%s)" % (name, index["bathy"][name]["sha256"]))
        if index.get("v1_rings_crossing_box") != 0:
            problems.append("%s v1 rings cross a box edge (expected 0 for S1)" % index.get("v1_rings_crossing_box"))
        if _pinned(V1_CHANGED_SQDEG):
            if abs(area - V1_CHANGED_SQDEG) > AREA_TOL * V1_CHANGED_SQDEG:
                problems.append("changed cells: land area %.5f sq deg, v1 %.5f (more than %d %% apart)"
                                % (area, V1_CHANGED_SQDEG, AREA_TOL * 100))
        else:
            log("  note: V1_CHANGED_SQDEG not pinned yet")
        if _pinned(PIN_TIER1_SHA256):
            if t1.get("sha256") != PIN_TIER1_SHA256:
                problems.append("pin: tier 1 %s != the pinned %s (a deliberate change updates PIN_TIER1_SHA256)"
                                % (str(t1.get("sha256"))[:12], PIN_TIER1_SHA256[:12]))
        else:
            log("  note: PIN_TIER1_SHA256 not pinned yet (%s)" % t1.get("sha256"))
    log("check (land2): tier 0 = v1 %s; tier 1 %d cells (%d changed, %d holes) / %d vertices / %d B; changed area "
        "%.5f sq deg; real-build gates %s; %d problems" % (
            sha0[:12], len(t1["cells"]), len(changed), n_holes, t1["vertices"], t1["bytes"], area,
            "on" if real else "OFF", len(problems)))
    for p in problems[:50]:
        log("  " + p)
    return problems


# ---------------------------------------------------------------- main

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--v1", help="a coast-v1 land set directory (build_coast.py output, or fetched with --fetch)")
    ap.add_argument("--tiles", help="bathymetry tiles directory: <region>/v<N>/{index.json,t/z/x/y.png}")
    ap.add_argument("--fetch", action="store_true", help="download the pinned tiles (and v1 when --v1 is empty) first")
    ap.add_argument("--out", help="output directory (build) or directory to upload")
    ap.add_argument("--check", metavar="DIR", help="validate a built v2 directory")
    ap.add_argument("--upload", metavar="PREFIX", help="upload --out to the R2 bucket under PREFIX")
    ap.add_argument("--replace", action="store_true", help="allow overwriting a different build under PREFIX")
    a = ap.parse_args(argv)
    if a.check:
        return 1 if check(a.check) else 0
    if a.upload:
        if not a.out:
            ap.error("--upload needs --out")
        if check(a.out):
            print("refusing to upload a directory that fails --check")
            return 1
        index = json.load(open(os.path.join(a.out, "index.json")))
        if not is_real(index) or not all(_pinned(r["tiles_sha256"]) for r in REGIONS.values()):
            print("refusing to upload a build that is not from the pinned bathymetry and v1 inputs")
            return 1
        if not _pinned(PIN_TIER1_SHA256) or index["tier1"]["sha256"] != PIN_TIER1_SHA256:
            print("refusing to upload a build whose tier-1 hash is not PIN_TIER1_SHA256")
            return 1
        return 0 if BC.upload(a.out, a.upload.strip("/"), replace=a.replace,
                              license_file=os.path.join(a.out, "LICENSE.txt")) else 1
    if not (a.v1 and a.tiles and a.out):
        ap.error("--v1, --tiles and --out are required to build")
    if a.fetch:
        if not os.path.exists(os.path.join(a.v1, "index.json")):
            fetch_v1(a.v1)
        fetch_tiles(a.tiles)
    build(a.v1, a.tiles, a.out)
    return 1 if check(a.out) else 0


if __name__ == "__main__":
    sys.exit(main())
