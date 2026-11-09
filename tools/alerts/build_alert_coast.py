"""The coastline of every NWS coastal forecast zone, for the coastal hazards layer (plan section 90).

NWS coastal products (High Surf, Rip Current, Beach Hazards, Coastal / Lakeshore Flood, Storm Surge) name forecast
zones (UGC ids such as HIZ007 "Oahu North Shore"), areas of land without a line to draw. This tool gives each zone the
stretch of coastline it covers, cut from the site's own GSHHG coastline (models.allshoresurf.com/static/coast/v1 and
lakeoutlines-v1: the lines the map's coastline clip and the wind layer's lake outlines use):

1. The zones are read from NWS's public zone shapefile (www.weather.gov/gis/PublicZones; ZONES_FILE, pinned by sha256).
   A zone can be stored as several rows (pieces split by a river); every piece counts for its zone.
2. Every coastline point (the GSHHG edges densified every STEP degrees, in ring order) is given to the zone whose
   polygon holds it. A point outside every zone (just offshore of a zone drawn to the shore, or on water NWS leaves
   out) joins the zone whose OUTER edge (an edge no other zone row shares: the coast, or a border) passes nearest,
   within NEAR_KM, unless the way there crosses water (a foreign shore across a strait or a river) or runs along
   the coast (the coast going on past the zone's end at a border).
3. Only the OPEN coast is kept (owner, 2026-11-08: a surf site; hazards belong on the shore that faces the open sea
   or a Great Lake, never on a lagoon, sound, bay or the Intracoastal Waterway): a point stays when at least one
   straight line from it, within FAN_DEG of straight out to sea, runs OPEN_KM over water without touching land.
   Short gaps and short pieces are evened out (GAP_KM, OPEN_MIN_KM).
4. Consecutive points of one zone make a line; lines are simplified (Douglas-Peucker, TOL degrees), lines shorter
   than MIN_KM dropped, and each line stored as a Google encoded polyline (precision 5, lat / lon pairs).

Output: alert_coast.json at the repository root: {"format": "alert-coast-v1", "zones_file", "zones_sha256", "coast",
"built_utc", "rules", "zones": {"HIZ007": {"name", "cwa", "lines": ["<encoded>", ...]}}}. A zone missing from it gets
no outline (the server reports it). Rebuild when NWS publishes a new zone file (a few times a year: bump ZONES_FILE
and ZONES_SHA256):
    python -I tools/alerts/build_alert_coast.py [--zones-zip z_ddmmyy.zip] [--cache DIR]
    python -I tools/alerts/build_alert_coast.py --check
Needs numpy and matplotlib (matplotlib.path for point-in-polygon: this tool only, not the site).
"""
import argparse
import collections
import hashlib
import io
import json
import math
import os
import struct
import sys
import urllib.request
import zipfile
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
OUT = os.path.join(ROOT, "alert_coast.json")
FORMAT = "alert-coast-v1"
ZONES_FILE = "z_16ap26"                       # NWS public forecast zones, effective 16 April 2026
ZONES_URL = "https://www.weather.gov/source/gis/Shapefiles/WSOM/" + ZONES_FILE + ".zip"
ZONES_SHA256 = "010dc5eab64b6e326c969e252bd016f56a037b0444df8303658707bca4cc7fd8"
COAST_BASE = "https://models.allshoresurf.com/static/coast"
# the sea coast; lakes of 100 km2 and more (the Great Lakes among them); the Great Lakes' smaller islands (lakes-v1:
# only its rings lakeoutlines-v1 does not hold, see lake_islands)
COAST_SETS = ("v1", "lakeoutlines-v1", "lakes-v1")
SEAWARD = {"v1": 1, "lakeoutlines-v1": -1, "lakes-v1": -1}    # water on the right of a ring's direction (+1) or left
UA = {"User-Agent": "allshoresurf.com coastal hazards tool (https://allshoresurf.com)"}
# states and territories with a sea or Great Lakes coast (NWS state codes; MP Northern Marianas, AS American Samoa)
COASTAL = frozenset("AK AL AS CA CT DC DE FL GA GU HI IL IN LA MA MD ME MI MN MP MS NC NH NJ NY OH OR PA PR RI SC TX "
                    "VA VI VT WA WI".split())
STEP = 0.001        # coastline points this far apart (deg, ~100 m)
NEAR_KM = 1.5       # an offshore point joins a zone whose outer edge is this close
NEAR_ALONG = 0.7    # ... unless the way there runs along the coast (|cos| of the angle with the coast above this)
NEAR_WATER_T = (0.2, 0.4, 0.6, 0.8)   # ... or crosses water at one of these fractions of the way
GRID = 0.005        # the near rule's lookup grid (deg)
LAND_RES = 0.005    # the land raster the open-sea test reads (deg, ~500 m)
NEAR_RES = 0.001    # the land raster the near rule's water test reads (deg, ~100 m)
NEAR_END_KM = 0.15  # the water test looks only at points this far from both ends (the raster's error near a shore)
OPEN_KM = 100.0     # open coast: a line this long reaches out over water ...
FAN_DEG = 60        # ... within this angle of straight out to sea
FAN_STEP = 5        # (lines tried every FAN_STEP degrees)
RAY_FIRST_KM = 0.8  # the first sample of a line (past the shore's own raster cell)
RAY_STEP_KM = 0.45  # sample spacing along a line
OPEN_EVERY = 5      # every 5th coast point is tested; the rest take the nearest tested point's answer
TANGENT_PTS = 3     # the coast's direction at a point: from the points this many before and after it
GAP_KM = 1.0        # a stretch judged closed, shorter than this, between open stretches: open (a jetty, a cove)
OPEN_MIN_KM = 1.0   # an open stretch shorter than this: closed (a speck inside a lagoon)
TOL = 0.0005        # Douglas-Peucker tolerance (deg, ~50 m)
MIN_KM = 0.3        # shorter pieces are dropped (a speck the near rule caught)
Q = 1e6             # zone vertices compared at this precision (the shapefile shares edges exactly)
EPS = 1e-9


def ugc(row):
    """The zone's id as the alerts name it: "HIZ007" from STATE "HI" + ZONE "007"."""
    return f"{row['STATE']}Z{row['ZONE']}"


# ---------------------------------------------------------------- the zone shapefile
def read_shapefile(zip_bytes):
    """-> (rows, shapes): the .dbf rows as dicts; per row a list of rings (n x 2 float arrays, lon / lat).
    Polygon shapes only (type 5, also 15 / 25 read by their x / y). ValueError for anything else."""
    import numpy as np
    try:
        z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as e:
        raise ValueError(f"zone file: {e}") from None
    names ={n.rsplit(".", 1)[-1].lower(): n for n in z.namelist()}
    if "dbf" not in names or "shp" not in names:
        raise ValueError("zone file: no .shp / .dbf")
    dbf, shp = z.read(names["dbf"]), z.read(names["shp"])
    if len(dbf) < 33:
        raise ValueError("zone file: dbf too short")
    n, hlen, rlen = struct.unpack_from("<IHH", dbf, 4)
    fields = []
    for i in range(32, hlen - 1, 32):
        fields.append((dbf[i:i + 11].split(b"\0")[0].decode("ascii"), dbf[i + 16]))
    if sum(ln for _, ln in fields) + 1 != rlen or hlen + n * rlen > len(dbf):
        raise ValueError("zone file: dbf layout")
    rows = []
    for r in range(n):
        rec = dbf[hlen + r * rlen + 1: hlen + (r + 1) * rlen]
        p, row = 0, {}
        for name, ln in fields:
            row[name] = rec[p:p + ln].decode("latin-1").strip()
            p += ln
        rows.append(row)
    if len(shp) < 100 or struct.unpack_from(">I", shp, 0)[0] != 9994:
        raise ValueError("zone file: not a shapefile")
    shapes, off = [], 100
    while off < len(shp):
        if off + 12 > len(shp):
            raise ValueError("zone file: record header cut short")
        _num, clen = struct.unpack_from(">II", shp, off)
        body = off + 8
        st = struct.unpack_from("<i", shp, body)[0]
        if st == 0:
            shapes.append([])
        elif st in (5, 15, 25):
            nparts, npts = struct.unpack_from("<ii", shp, body + 36)
            if nparts < 1 or npts < 3 or body + 44 + 4 * nparts + 16 * npts > len(shp):
                raise ValueError("zone file: polygon record")
            parts = list(struct.unpack_from("<%di" % nparts, shp, body + 44)) + [npts]
            if parts[0] != 0 or any(parts[k + 1] < parts[k] for k in range(nparts)):
                raise ValueError("zone file: polygon parts")
            pts = np.frombuffer(shp, dtype="<f8", count=2 * npts, offset=body + 44 + 4 * nparts).reshape(-1, 2)
            shapes.append([pts[parts[k]:parts[k + 1]] for k in range(nparts) if parts[k + 1] - parts[k] >= 3])
        else:
            raise ValueError("zone file: shape type %d" % st)
        off = body + 2 * clen
    if len(shapes) != len(rows):
        raise ValueError("zone file: %d records, %d shapes" % (len(rows), len(shapes)))
    return rows, shapes


def _edge_keys(ring):
    """The ring's edges as undirected vertex pairs at Q precision (zero-length edges left out)."""
    import numpy as np
    q = np.round(ring * Q).astype(np.int64)
    a, b = q[:-1], q[1:]
    keep = ~(a == b).all(axis=1)
    a, b = a[keep], b[keep]
    first = (a[:, 0] < b[:, 0]) | ((a[:, 0] == b[:, 0]) & (a[:, 1] < b[:, 1]))
    lo = np.where(first[:, None], a, b)
    hi = np.where(first[:, None], b, a)
    return keep, list(zip(map(tuple, lo), map(tuple, hi)))


def edge_counts(shapes):
    """How many zone rows use each edge, over ALL rows of the file (an edge two rows share is not an outer edge)."""
    cnt = collections.Counter()
    for s in shapes:
        for r in s:
            cnt.update(_edge_keys(r)[1])
    return cnt


def near_map(pieces, cnt):
    """The coastal zone rows' OUTER edges (edges used by one row only) as a lookup: (segs, cells). segs: m x 5 array
    (zone index, x0, y0, x1, y1); cells: {grid cell: [segment index]} for every GRID cell a segment passes."""
    import numpy as np
    segs, cells = [], {}
    for zi, rings in pieces:
        for r in rings:
            keep, keys = _edge_keys(r)
            idx = np.flatnonzero(keep)
            for k, key in zip(idx, keys):
                if cnt[key] != 1:
                    continue
                x0, y0 = r[k]
                x1, y1 = r[k + 1]
                si = len(segs)
                segs.append((zi, x0, y0, x1, y1))
                n = max(1, int(math.ceil(math.hypot(x1 - x0, y1 - y0) / (GRID / 2))))
                t = np.linspace(0.0, 1.0, n + 1)
                gx = np.floor((x0 + (x1 - x0) * t + 180) / GRID).astype(int)
                gy = np.floor((y0 + (y1 - y0) * t + 90) / GRID).astype(int)
                for cell in set(zip(gx.tolist(), gy.tolist())):
                    cells.setdefault(cell, []).append(si)
    return np.array(segs, float).reshape(-1, 5), cells


def near_zone(near, x, y, km=NEAR_KM):
    """The zone whose outer edge passes nearest (x, y) (true distance, km), within km: (zone index, qx, qy, d km),
    (qx, qy) the nearest point of that edge; ties go to the lower zone index. None when no outer edge is that close."""
    import numpy as np
    segs, cells = near
    k = max(0.05, math.cos(math.radians(y)))
    gx, gy = int((x + 180) // GRID), int((y + 90) // GRID)
    rx, ry = int(km / (111.2 * k * GRID)) + 1, int(km / (111.2 * GRID)) + 1
    idx = set()
    for dx in range(-rx, rx + 1):
        for dy in range(-ry, ry + 1):
            idx.update(cells.get((gx + dx, gy + dy), ()))
    if not idx:
        return None
    S = segs[sorted(idx)]
    ax, ay = (S[:, 1] - x) * k * 111.2, (S[:, 2] - y) * 111.2
    bx, by = (S[:, 3] - x) * k * 111.2, (S[:, 4] - y) * 111.2
    dx, dy = bx - ax, by - ay
    L2 = dx * dx + dy * dy
    t = np.where(L2 > 0, np.clip(-(ax * dx + ay * dy) / np.where(L2 > 0, L2, 1), 0, 1), 0)
    d = np.hypot(ax + t * dx, ay + t * dy)
    best = d.min()
    if best > km:
        return None
    cand = np.flatnonzero(d <= best + 1e-9)
    j = cand[np.argmin(S[cand, 0])]
    qx = S[j, 1] + t[j] * (S[j, 3] - S[j, 1])
    qy = S[j, 2] + t[j] * (S[j, 4] - S[j, 2])
    return int(S[j, 0]), float(qx), float(qy), float(d[j])


# ---------------------------------------------------------------- the coastline
def _cache_get(url, path):
    if os.path.exists(path):
        with open(path, "rb") as f:
            return f.read()
    data = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60).read()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)
    return data


def coast_index(set_name, cache):
    data = _cache_get(f"{COAST_BASE}/{set_name}/index.json", os.path.join(cache, "coast_" + set_name, "index.json"))
    return json.loads(data)


def coast_cell(set_name, cell, cache):
    return _cache_get(f"{COAST_BASE}/{set_name}/f/{cell}.bin", os.path.join(cache, "coast_" + set_name, cell + ".bin"))


def ring_starts(edges):
    """Where each closed ring of a cell's edges begins (an edge whose first vertex is not the last one's end)."""
    import numpy as np
    xi, yi, xj, yj = edges
    if not len(xi):
        return np.zeros(0, int)
    joins = (np.abs(xj[1:] - xi[:-1]) < EPS) & (np.abs(yj[1:] - yi[:-1]) < EPS)
    return np.flatnonzero(np.concatenate(([True], ~joins)))


def _undirected(e, a, b):
    import numpy as np
    xi, yi, xj, yj = (v[a:b] for v in e)
    p = np.round(np.stack([xi, yi], 1) * 1e4).astype(np.int64)
    q = np.round(np.stack([xj, yj], 1) * 1e4).astype(np.int64)
    f = (p[:, 0] < q[:, 0]) | ((p[:, 0] == q[:, 0]) & (p[:, 1] < q[:, 1]))
    lo, hi = np.where(f[:, None], p, q), np.where(f[:, None], q, p)
    return set(map(tuple, np.concatenate([lo, hi], 1).tolist()))


def lake_islands(lakes, outlines):
    """The Great Lakes' smaller islands: the rings of lakes-v1 (the Great Lakes water set, with its 139 islands) that
    share no edge with lakeoutlines-v1 (which holds the lakes and their islands of 100 km2 and more). A ring of
    lakes-v1 is either wholly in lakeoutlines-v1 or wholly not (checked on every Great Lakes cell, 2026-11-08)."""
    import numpy as np
    if lakes is None or not len(lakes[0]):
        return None
    have = _undirected(outlines, 0, len(outlines[0])) if outlines is not None and len(outlines[0]) else set()
    st = list(ring_starts(lakes)) + [len(lakes[0])]
    keep = np.zeros(len(lakes[0]), bool)
    for a, b in zip(st[:-1], st[1:]):
        if not (_undirected(lakes, a, b) & have):
            keep[a:b] = True
    return tuple(v[keep] for v in lakes)


def raster(edges, la, lo, res=LAND_RES, size=5):
    """Even-odd fill of a 5-degree cell's rings on the size-degree square at (la, lo) (row 0 = the south edge), on a
    grid of res-degree cells, by cell centre, with land_parity's half-open crossing rule. -> n x n bool."""
    import numpy as np
    n = int(round(size / res))
    out = np.zeros((n, n), bool)
    if edges is None or not len(edges[0]):
        return out
    xi, yi, xj, yj = edges
    ymin, ymax = np.minimum(yi, yj), np.maximum(yi, yj)
    r0 = np.clip(np.ceil((ymin - la) / res - 0.5), 0, n).astype(np.int64)
    r1 = np.clip(np.ceil((ymax - la) / res - 0.5), 0, n).astype(np.int64)
    cnt = r1 - r0
    m = cnt > 0
    xi, yi, xj, yj, r0, cnt = xi[m], yi[m], xj[m], yj[m], r0[m], cnt[m]
    if not cnt.size:
        return out
    e = np.repeat(np.arange(cnt.size), cnt)
    rows = r0[e] + (np.arange(e.size) - np.repeat(np.cumsum(cnt) - cnt, cnt))
    yc = la + (rows + 0.5) * res
    x = xi[e] + (xj[e] - xi[e]) * (yc - yi[e]) / (yj[e] - yi[e])
    k = np.clip(np.ceil((x - lo) / res - 0.5), 0, n).astype(np.int64)
    T = np.zeros((n, n + 1), np.int32)
    np.add.at(T, (rows, k), 1)
    R = np.cumsum(T[:, ::-1], axis=1)[:, ::-1]                       # crossings east of each cell centre
    return (R[:, 1:] & 1).astype(bool)


class Land:
    """Land on a LAND_RES raster, 5-degree cell by cell, built as asked: GSHHG land (lakes count as land in coast v1)
    less the lakes (lakeoutlines-v1, the Great Lakes' smaller islands back as land). Bit 1: land by the polygons;
    bit 2: a cell the coastline passes through (counted as land by the open-sea test, so a barrier island or a spit
    narrower than a raster cell still stops a line). load(cell name) -> (land edges, lake edges, (x, y) of the cell's
    coastline points), None for what is not there."""

    def __init__(self, load, res=LAND_RES):
        self.load, self.res, self.cells = load, res, {}

    def cell(self, la, lo):
        import numpy as np
        g = self.cells.get((la, lo))
        if g is None:
            land, lake, pts = self.load(f"{la}_{lo}")
            n = int(round(5 / self.res))
            poly = raster(land, la, lo, self.res) & ~raster(lake, la, lo, self.res)
            g = poly.astype(np.uint8)
            if pts is not None and len(pts[0]):
                c = np.floor((pts[0] - lo) / self.res).astype(int)
                r = np.floor((pts[1] - la) / self.res).astype(int)
                ok = (c >= 0) & (c < n) & (r >= 0) & (r < n)
                g[r[ok], c[ok]] |= 2
            self.cells[(la, lo)] = g
        return g

    def at(self, lon, lat, shore=True):
        """bool array: land at (lon, lat); shore=True also counts the cells the coastline passes through."""
        import numpy as np
        lon = (np.asarray(lon, float) + 180) % 360 - 180
        lat = np.asarray(lat, float)
        la = (np.floor(lat / 5) * 5).astype(int)
        lo = (np.floor(lon / 5) * 5).astype(int)
        code = (la + 90) * 1000 + (lo + 180)
        out = np.zeros(lon.shape, bool)
        mask = 3 if shore else 1
        for c in np.unique(code):
            m = code == c
            L, O = int(c // 1000) - 90, int(c % 1000) - 180
            if L >= 85 or L < -90:
                continue
            g = self.cell(L, O)
            n = g.shape[0]
            r = np.clip(((lat[m] - L) / self.res).astype(int), 0, n - 1)
            k = np.clip(((lon[m] - O) / self.res).astype(int), 0, n - 1)
            out[m] = (g[r, k] & mask) > 0
        return out


class FineLand:
    """Land on a NEAR_RES raster in 1-degree tiles (the near rule's water test: a 500-m cell is too coarse beside a
    shore). load: as Land's (only the edges are read). water(lon, lat) -> bool array."""

    def __init__(self, load, res=NEAR_RES):
        self.load, self.res, self.tiles, self.src = load, res, {}, {}

    def tile(self, la, lo):
        g = self.tiles.get((la, lo))
        if g is None:
            c5 = (la // 5 * 5, lo // 5 * 5)
            if c5 not in self.src:
                land, lake, _pts = self.load(f"{c5[0]}_{c5[1]}")
                self.src[c5] = (land, lake)
            land, lake = self.src[c5]
            g = raster(land, la, lo, self.res, 1) & ~raster(lake, la, lo, self.res, 1)
            self.tiles[(la, lo)] = g
        return g

    def water(self, lon, lat):
        import numpy as np
        lon = (np.asarray(lon, float) + 180) % 360 - 180
        lat = np.asarray(lat, float)
        la, lo = np.floor(lat).astype(int), np.floor(lon).astype(int)
        out = np.ones(lon.shape, bool)
        code = (la + 90) * 1000 + (lo + 180)
        for c in np.unique(code):
            m = code == c
            L, O = int(c // 1000) - 90, int(c % 1000) - 180
            if L >= 85 or L < -90:
                continue
            g = self.tile(L, O)
            n = g.shape[0]
            r = np.clip(((lat[m] - L) / self.res).astype(int), 0, n - 1)
            k = np.clip(((lon[m] - O) / self.res).astype(int), 0, n - 1)
            out[m] = ~g[r, k]
        return out


def tangents(x, y, start, pts=TANGENT_PTS):
    """The coast's direction at each point (unit, km east / km north): from the point pts before to the point pts
    after it, never across a ring's start."""
    import numpy as np
    if not x.size:
        return np.zeros(0), np.zeros(0)
    i = np.arange(x.size)
    first = start.copy()
    first[0] = True
    last = np.concatenate((first[1:], [True]))
    r0 = np.maximum.accumulate(np.where(first, i, 0))
    r1 = np.minimum.accumulate(np.where(last, i, x.size - 1)[::-1])[::-1]
    a = np.maximum(i - pts, r0)
    b = np.minimum(i + pts, r1)
    k = np.cos(np.radians(y))
    tx, ty = (x[b] - x[a]) * k, y[b] - y[a]
    L = np.hypot(tx, ty)
    L[L == 0] = 1
    return tx / L, ty / L


def open_sea(land, x, y, nx, ny, reach=OPEN_KM, fan=FAN_DEG, fstep=FAN_STEP):
    """bool per point: a straight line from it, within fan degrees of (nx, ny) (unit, km east / north: out to sea),
    runs reach km without touching land (land.at, shore cells counted). The straight-out line is tried first."""
    import numpy as np
    out = np.zeros(x.size, bool)
    d = np.arange(RAY_FIRST_KM, reach + 1e-9, RAY_STEP_KM)
    angs = [0]
    for a in range(fstep, fan + 1, fstep):
        angs += [a, -a]
    k = np.maximum(0.05, np.cos(np.radians(y)))
    for a in angs:
        act = np.flatnonzero(~out)
        if not act.size:
            break
        ca, sa = math.cos(math.radians(a)), math.sin(math.radians(a))
        ex, ey = nx[act] * ca - ny[act] * sa, nx[act] * sa + ny[act] * ca
        for b in range(0, act.size, 2000):
            s = act[b:b + 2000]
            lon = x[s, None] + d[None, :] * ex[b:b + 2000, None] / (111.2 * k[s, None])
            lat = y[s, None] + d[None, :] * ey[b:b + 2000, None] / 111.2
            out[s[~land.at(lon, lat).any(axis=1)]] = True
    return out


def even_out(flag, x, y, start, elig, gap=GAP_KM, short=OPEN_MIN_KM):
    """Smooth the open / closed answers along each ring (eligible points only; the others break a stretch): a closed
    stretch shorter than gap km with open on both sides becomes open; then an open stretch shorter than short km
    becomes closed. -> a new bool array."""
    import numpy as np
    flag = flag.copy()
    if not x.size:
        return flag
    k = np.cos(np.radians(y))
    seg = np.concatenate(([0.0], 111.2 * np.hypot(np.diff(x) * k[1:], np.diff(y))))
    for value, limit in ((False, gap), (True, short)):
        cut = start.copy()
        cut[0] = True
        cut[1:] |= (flag[1:] != flag[:-1]) | (elig[1:] != elig[:-1])
        b = list(np.flatnonzero(cut)) + [x.size]
        for a, e in zip(b[:-1], b[1:]):
            if not elig[a] or bool(flag[a]) != value:
                continue
            if float(seg[a + 1:e].sum()) >= limit:
                continue
            if not value:
                before = a > 0 and not start[a] and elig[a - 1] and flag[a - 1]
                after = e < x.size and not start[e] and elig[e] and flag[e]
                if before and after:
                    flag[a:e] = True
            else:
                flag[a:e] = False
    return flag


def classify_open(land, x, y, start, skip, zone, sign, every=OPEN_EVERY):
    """bool per point: open coast. open_sea on every every-th eligible point (a zone's, not on a cell line), the rest
    take the nearest tested point's answer, then even_out. sign: SEAWARD of the set (+1: water on the right)."""
    import numpy as np
    elig = (zone >= 0) & ~skip
    out = np.zeros(x.size, bool)
    idx = np.flatnonzero(elig)
    if not idx.size:
        return out
    tx, ty = tangents(x, y, start)
    test = idx[::every]
    got = open_sea(land, x[test], y[test], sign * ty[test], -sign * tx[test])
    pos = np.clip(np.round(np.arange(idx.size) / every).astype(int), 0, test.size - 1)
    out[idx] = got[pos]
    return even_out(out, x, y, start, elig)


def densify(edges, step=STEP):
    """A coast cell's ring edges (point_forecast.decode_coast: edge = vertex j -> vertex i) as points in ring order:
    -> (x, y, start, skip). start = a new line begins at this point (its edge does not continue the previous one);
    skip = the point lies on an edge along a 5-degree cell line (the builder's clip, not coast)."""
    import numpy as np
    xi, yi, xj, yj = edges
    if not len(xi):
        z = np.zeros(0)
        return z, z, np.zeros(0, bool), np.zeros(0, bool)
    on_line = ((np.abs(xi - xj) < EPS) & (np.abs(np.round(xi / 5) * 5 - xi) < EPS)) | \
              ((np.abs(yi - yj) < EPS) & (np.abs(np.round(yi / 5) * 5 - yi) < EPS))
    n = np.maximum(1, np.ceil(np.hypot(xi - xj, yi - yj) / step)).astype(int)
    rep = np.repeat(np.arange(len(n)), n)
    t = (np.arange(rep.size) - np.repeat(np.cumsum(n) - n, n)) / n[rep]
    x = xj[rep] + (xi[rep] - xj[rep]) * t
    y = yj[rep] + (yi[rep] - yj[rep]) * t
    first = np.concatenate(([True], rep[1:] != rep[:-1]))
    joins = np.concatenate(([False], (np.abs(xj[1:] - xi[:-1]) < EPS) & (np.abs(yj[1:] - yi[:-1]) < EPS)))
    start = first & ~joins[rep]
    return x, y, start, on_line[rep]


def near_ok(x, y, q, tangent=None, water=None):
    """May coast point (x, y) join the zone whose outer edge passes at q = (zone, qx, qy, d km)? Not when the way to
    the edge runs along the coast (|cos| of its angle with tangent = (tx, ty) above NEAR_ALONG: the coast goes on
    past the zone's end, a border on the shore) or crosses water (water(lons, lats) at NEAR_WATER_T of the way: a
    shore across a strait, a river or a lake). Within 50 m the edge is taken as is."""
    import numpy as np
    _zi, qx, qy, d = q
    if d <= 0.05:
        return True
    k = max(0.05, math.cos(math.radians(y)))
    ux, uy = (qx - x) * k * 111.2 / d, (qy - y) * 111.2 / d
    if tangent is not None and abs(ux * tangent[0] + uy * tangent[1]) > NEAR_ALONG:
        return False
    if water is not None:
        t = np.array(NEAR_WATER_T)
        t = t[(t * d >= NEAR_END_KM) & ((1 - t) * d >= NEAR_END_KM)]
        if t.size and water(x + (qx - x) * t, y + (qy - y) * t).any():
            return False
    return True


def assign(x, y, skip, pieces, paths, boxes, near, start=None, water=None):
    """Each point's zone index (-1: none). A point inside two rows (they never overlap in NWS's file) takes the first.
    A point outside every row takes the nearest outer edge's zone when near_ok (start: the ring starts, for the
    coast's direction; water: lons, lats -> bool; None skips that test)."""
    import numpy as np
    zone = np.full(x.size, -1)
    if not x.size:
        return zone
    cand = np.flatnonzero((boxes[:, 0] <= x.max()) & (boxes[:, 1] >= x.min()) &
                          (boxes[:, 2] <= y.max()) & (boxes[:, 3] >= y.min()))
    for pi in cand:
        m = (zone < 0) & ~skip & (x >= boxes[pi, 0]) & (x <= boxes[pi, 1]) & (y >= boxes[pi, 2]) & (y <= boxes[pi, 3])
        idx = np.flatnonzero(m)
        if idx.size:
            zone[idx[paths[pi].contains_points(np.stack([x[idx], y[idx]], 1))]] = pieces[pi][0]
    rest = np.flatnonzero((zone < 0) & ~skip)
    if rest.size:
        tx, ty = tangents(x, y, start) if start is not None else (None, None)
        for k in rest:
            q = near_zone(near, float(x[k]), float(y[k]))
            if q is not None and near_ok(float(x[k]), float(y[k]), q,
                                         None if tx is None else (tx[k], ty[k]), water):
                zone[k] = q[0]
    return zone


def runs(x, y, start, skip, zone, step=STEP):
    """[(zone index, n x 2 array lon / lat)]: the points of one zone in a row, never across a line start or a skip.
    A closed ring whose first and last runs belong to one zone gives one line across the ring's start point."""
    import numpy as np
    if not x.size:
        return []
    cut = start.copy()
    cut[1:] |= (skip[1:] != skip[:-1]) | (zone[1:] != zone[:-1])
    cut[0] = True
    bounds = list(np.flatnonzero(cut)) + [x.size]
    rings = list(np.flatnonzero(start)) + [x.size]
    if rings[0] != 0:
        rings.insert(0, 0)
    out = []
    for r0, r1 in zip(rings[:-1], rings[1:]):
        spans = [(a, b) for a, b in zip(bounds[:-1], bounds[1:]) if r0 <= a < r1]
        lines = [(int(zone[a]), a, b) for a, b in spans if zone[a] >= 0 and not skip[a]]
        if (len(lines) >= 2 and lines[0][1] == r0 and lines[-1][2] == r1 and lines[0][0] == lines[-1][0]
                and math.hypot(x[r1 - 1] - x[r0], y[r1 - 1] - y[r0]) <= 1.5 * step):
            z, a0, b0 = lines.pop(0)
            _, a1, b1 = lines.pop()
            P = np.concatenate([np.stack([x[a1:b1], y[a1:b1]], 1), np.stack([x[a0:b0], y[a0:b0]], 1)])
            out.append((z, P))
        for z, a, b in lines:
            if b - a >= 2:
                out.append((z, np.stack([x[a:b], y[a:b]], 1)))
    return out


def simplify(P, tol=TOL):
    """Douglas-Peucker on an n x 2 array (degrees); the ends are kept."""
    import numpy as np
    if len(P) < 3:
        return P
    keep = np.zeros(len(P), bool)
    keep[0] = keep[-1] = True
    stack = [(0, len(P) - 1)]
    while stack:
        a, b = stack.pop()
        if b <= a + 1:
            continue
        seg = P[b] - P[a]
        L = math.hypot(seg[0], seg[1])
        D = P[a + 1:b] - P[a]
        d = np.abs(seg[0] * D[:, 1] - seg[1] * D[:, 0]) / L if L > 0 else np.hypot(D[:, 0], D[:, 1])
        k = int(np.argmax(d))
        if d[k] > tol:
            keep[a + 1 + k] = True
            stack += [(a, a + 1 + k), (a + 1 + k, b)]
    return P[keep]


def length_km(P):
    import numpy as np
    k = np.cos(np.radians(P[:, 1]))
    return float(np.sum(111.2 * np.hypot(np.diff(P[:, 0]) * (k[1:] + k[:-1]) / 2, np.diff(P[:, 1]))))


def encode(P):
    """Google encoded polyline (precision 5) of lon / lat points, written as lat / lon pairs."""
    out, plat, plon = [], 0, 0
    for lon, lat in P:
        ilat, ilon = int(round(lat * 1e5)), int(round(lon * 1e5))
        for v in (ilat - plat, ilon - plon):
            v = ~(v << 1) if v < 0 else (v << 1)
            while v >= 0x20:
                out.append(chr((0x20 | (v & 0x1F)) + 63))
                v >>= 5
            out.append(chr(v + 63))
        plat, plon = ilat, ilon
    return "".join(out)


def decode(s):
    """The inverse of encode(): [(lon, lat)]. ValueError on a malformed string."""
    pts, i, lat, lon = [], 0, 0, 0
    while i < len(s):
        vals = []
        for _ in range(2):
            shift = res = 0
            while True:
                if i >= len(s):
                    raise ValueError("polyline cut short")
                b = ord(s[i]) - 63
                i += 1
                if b < 0 or b > 63:
                    raise ValueError("polyline character")
                res |= (b & 0x1F) << shift
                shift += 5
                if shift > 35:
                    raise ValueError("polyline value too long")
                if b < 0x20:
                    break
            vals.append(~(res >> 1) if res & 1 else res >> 1)
        lat += vals[0]
        lon += vals[1]
        pts.append((lon / 1e5, lat / 1e5))
    return pts


# ---------------------------------------------------------------- build
# NWS's own time-zone letter in the zone file (WSOM) -> IANA. "CE" = a zone split between Central and Eastern: the first.
TZ_CODES = {"A": "America/Anchorage", "h": "America/Adak", "H": "Pacific/Honolulu", "P": "America/Los_Angeles",
            "M": "America/Denver", "m": "America/Phoenix", "C": "America/Chicago", "E": "America/New_York",
            "V": "America/Puerto_Rico", "G": "Pacific/Guam", "S": "Pacific/Pago_Pago"}
TZ_STATE = {("MP", "G"): "Pacific/Saipan", ("VI", "V"): "America/St_Thomas"}


def zone_tz(row):
    """The zone's civil time zone (IANA) from NWS's own TIME_ZONE letter, so an alert's times can be shown in the
    coast's own clock. None for a letter not in TZ_CODES."""
    code = (row.get("TIME_ZONE") or "")[:1]
    return TZ_STATE.get((row.get("STATE"), code)) or TZ_CODES.get(code)


def coastal_pieces(rows, shapes):
    """-> (ids, meta, pieces): the coastal states' zone ids (sorted), their name / office, and every row as
    (zone index, rings)."""
    ids = sorted({ugc(r) for r, s in zip(rows, shapes) if r.get("STATE") in COASTAL and s})
    index = {z: i for i, z in enumerate(ids)}
    meta, pieces = {}, []
    for r, s in zip(rows, shapes):
        if r.get("STATE") in COASTAL and s:
            zid = ugc(r)
            meta.setdefault(zid, {"name": r.get("NAME", ""), "cwa": r.get("CWA", ""), "tz": zone_tz(r)})
            pieces.append((index[zid], s))
    return ids, meta, pieces


def build(zip_bytes, cache, log=print):
    import numpy as np
    from matplotlib.path import Path
    sys.path.insert(0, ROOT)
    from point_forecast import decode_coast
    rows, shapes = read_shapefile(zip_bytes)
    cnt = edge_counts(shapes)
    ids, meta, pieces = coastal_pieces(rows, shapes)
    paths, boxes = [], []
    for _zi, rings in pieces:
        verts = np.concatenate(rings)
        codes = np.full(len(verts), Path.LINETO, np.uint8)
        o = 0
        for r in rings:
            codes[o] = Path.MOVETO
            o += len(r)
        paths.append(Path(verts, codes))
        boxes.append((verts[:, 0].min(), verts[:, 0].max(), verts[:, 1].min(), verts[:, 1].max()))
    boxes = np.array(boxes)
    near = near_map(pieces, cnt)
    log(f"{len(rows)} zone rows in the file, {len(pieces)} rows / {len(ids)} zones in coastal states, "
        f"{len(near[0])} outer edges")
    index = {s: coast_index(s, cache) for s in COAST_SETS}
    coast_ids = {s: index[s]["tier1"].get("sha256") for s in COAST_SETS}
    memo = {}

    def edges(set_name, cell):
        """The cell's edges of a coast set (lakes-v1: its islands only); None when the set has nothing there."""
        key = (set_name, cell)
        if key not in memo:
            e = None
            if cell in index[set_name]["tier1"]["cells"]:
                e = decode_coast(coast_cell(set_name, cell, cache))
                if set_name == "lakes-v1":
                    e = lake_islands(e, edges("lakeoutlines-v1", cell))
            memo[key] = e
        return memo[key]

    def load(cell):
        land = edges("v1", cell)
        lakes = [e for e in (edges("lakeoutlines-v1", cell), edges("lakes-v1", cell)) if e is not None]
        lake = tuple(np.concatenate(v) for v in zip(*lakes)) if lakes else None
        px, py = [], []
        for e in (land, lake):
            if e is not None and len(e[0]):
                x, y, _st, sk = densify(e)
                px.append(x[~sk])
                py.append(y[~sk])
        return land, lake, ((np.concatenate(px), np.concatenate(py)) if px else None)

    land = Land(load)
    water = FineLand(load).water

    cells = set()
    for w, e, s, n in boxes:
        for la in range(int(math.floor(s / 5)) * 5, int(math.floor(n / 5)) * 5 + 1, 5):
            for lo in range(int(math.floor(w / 5)) * 5, int(math.floor(e / 5)) * 5 + 1, 5):
                cells.add(f"{min(la, 85)}_{(lo + 180) % 360 - 180}")
    lines = collections.defaultdict(list)
    points = assigned = kept = 0
    for set_name in COAST_SETS:
        for cell in sorted(cells):
            e = edges(set_name, cell)
            if e is None or not len(e[0]):
                continue
            x, y, start, skip = densify(e)
            points += x.size
            zone = assign(x, y, skip, pieces, paths, boxes, near, start=start, water=water)
            assigned += int((zone >= 0).sum())
            zone = np.where(classify_open(land, x, y, start, skip, zone, SEAWARD[set_name]), zone, -1)
            kept += int((zone >= 0).sum())
            for zi, P in runs(x, y, start, skip, zone):
                P = simplify(P)
                if length_km(P) >= MIN_KM:
                    lines[ids[zi]].append(P)
    log(f"{points} coast points, {assigned} in a zone, {kept} on the open coast; {len(lines)} zones with a coastline; "
        f"{len(land.cells)} land cells")
    out = {}
    for zid in sorted(lines):
        out[zid] = dict(meta[zid], lines=[encode(P) for P in lines[zid]])
    return out, coast_ids


RULES = {"step_deg": STEP, "near_km": NEAR_KM, "near_along": NEAR_ALONG, "tol_deg": TOL, "min_km": MIN_KM,
         "open_km": OPEN_KM, "fan_deg": FAN_DEG, "land_res_deg": LAND_RES, "gap_km": GAP_KM,
         "open_min_km": OPEN_MIN_KM}


def write(zones, coast_ids, zones_sha, path=OUT):
    doc = {"format": FORMAT, "zones_file": ZONES_FILE, "zones_sha256": zones_sha, "coast": coast_ids,
           "built_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "rules": RULES,
           "zones": zones}
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(doc, f, separators=(",", ":"), sort_keys=True)
        f.write("\n")
    return doc


# ---------------------------------------------------------------- check
# (zone, lat, lon, km): the zone's line must pass this close to a known shore point
PROBES = [
    ("HIZ007", 21.6418, -158.0660, 0.5),    # Waimea Bay, Oahu North Shore
    ("HIZ007", 21.6650, -158.0530, 0.5),    # Banzai Pipeline
    ("HIZ006", 21.4500, -158.1960, 0.5),    # Makaha, Waianae Coast
    ("HIZ033", 21.2760, -157.8270, 0.5),    # Waikiki, Honolulu Metro
    ("CAZ354", 34.2740, -119.2900, 0.6),    # Ventura pier, Ventura County Beaches
    ("ORZ103", 44.6330, -124.0610, 0.6),    # Newport, Central Coast of Oregon
    ("FLZ747", 28.3200, -80.6070, 0.5),     # Cocoa Beach, the ocean beach of the Southern Brevard Barrier Islands
    ("NCZ205", 35.2500, -75.5200, 0.6),     # Cape Hatteras, the ocean side of Hatteras Island
]
# (zone, lat, lon, km): the zone must NOT draw a line within km of this point (another zone's shore, a foreign
# shore across water, the coast past a border: G59 F1)
FAR_PROBES = [
    ("HIZ007", 21.2760, -157.8270, 5.0),    # Waikiki is not North Shore
    ("HIZ033", 21.6418, -158.0660, 5.0),    # Waimea is not Honolulu Metro
    ("MIZ076", 42.3170, -83.0400, 1.0),     # Windsor, Ontario, across the Detroit River
    ("NYZ007", 44.1500, -76.3000, 1.0),     # Wolfe Island, Ontario
    ("WAZ310", 49.0200, -123.1000, 1.0),    # Tsawwassen, British Columbia
    ("MEZ030", 44.8780, -66.9650, 1.0),     # Campobello Island, New Brunswick
    ("VIZ001", 18.3890, -64.7200, 1.0),     # Great Thatch, British Virgin Islands
    ("TXZ455", 25.9350, -97.1440, 1.0),     # Playa Bagdad, Tamaulipas, past the Rio Grande
    ("CAZ043", 32.5100, -117.1240, 1.0),    # Playas de Tijuana, past the border
]
# (lat, lon, km): no zone may draw a line within km of this point (a lagoon, a harbour: not the open coast)
NO_LINE = [
    (28.3600, -80.7250, 0.5),               # the Indian River Lagoon's mainland shore at Cocoa, Florida
    (21.3700, -157.9700, 0.5),              # Pearl Harbor, Oahu
]


def nearest_km(lines, lat, lon):
    """The distance (km) from (lat, lon) to the nearest segment of the decoded lines; inf without any."""
    import numpy as np
    best = math.inf
    k = math.cos(math.radians(lat))
    for P in lines:
        P = np.asarray(P, float)
        if len(P) < 2:
            continue
        a, b = P[:-1], P[1:]
        ax, ay = (a[:, 0] - lon) * k * 111.2, (a[:, 1] - lat) * 111.2
        bx, by = (b[:, 0] - lon) * k * 111.2, (b[:, 1] - lat) * 111.2
        dx, dy = bx - ax, by - ay
        L2 = dx * dx + dy * dy
        t = np.where(L2 > 0, np.clip(-(ax * dx + ay * dy) / np.where(L2 > 0, L2, 1), 0, 1), 0)
        d = np.hypot(ax + t * dx, ay + t * dy)
        best = min(best, float(d.min()))
    return best


def check(doc, log=print):
    """Problems as text (an empty list = good)."""
    probs = []
    if doc.get("format") != FORMAT:
        probs.append("format")
    zones = doc.get("zones") or {}
    if not 330 <= len(zones) <= 500:
        probs.append(f"{len(zones)} zones (expected 330-500)")
    pts = 0
    decoded = {}
    for zid, z in zones.items():
        if not isinstance(z.get("tz"), str) or "/" not in z["tz"]:
            probs.append(f"{zid}: no time zone")
        if not isinstance(z.get("lines"), list) or not z["lines"]:
            probs.append(f"{zid}: no lines")
            continue
        ls = []
        for s in z["lines"]:
            try:
                P = decode(s)
            except ValueError as e:
                probs.append(f"{zid}: {e}")
                continue
            if len(P) < 2:
                probs.append(f"{zid}: a line of {len(P)} point")
            pts += len(P)
            ls.append(P)
        decoded[zid] = ls
    for zid, lat, lon, km in PROBES:
        if zid not in decoded:
            probs.append(f"probe {zid}: missing")
            continue
        d = nearest_km(decoded[zid], lat, lon)
        if d > km:
            probs.append(f"probe {zid} {lat},{lon}: nearest line {d:.2f} km (limit {km})")
    for zid, lat, lon, km in FAR_PROBES:
        if zid in decoded and nearest_km(decoded[zid], lat, lon) < km:
            probs.append(f"far probe {zid} {lat},{lon}: a line within {km} km")
    for lat, lon, km in NO_LINE:
        hit = [zid for zid, ls in decoded.items() if nearest_km(ls, lat, lon) < km]
        if hit:
            probs.append(f"no-line probe {lat},{lon}: {', '.join(sorted(hit))} within {km} km")
    by_state = collections.Counter(zid[:2] for zid in zones)
    log(f"{len(zones)} zones, {pts} points; by state: {dict(sorted(by_state.items()))}")
    return probs


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--zones-zip", help="a local copy of the NWS zone file (else downloaded)")
    ap.add_argument("--cache", default=os.path.join(HERE, ".cache"), help="where coast cells are kept")
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--check", action="store_true", help="check the written file only")
    a = ap.parse_args(argv)
    if a.check:
        with open(a.out, encoding="utf-8") as f:
            probs = check(json.load(f))
        for p in probs:
            print("PROBLEM", p)
        return 1 if probs else 0
    if a.zones_zip:
        with open(a.zones_zip, "rb") as f:
            data = f.read()
    else:
        data = urllib.request.urlopen(urllib.request.Request(ZONES_URL, headers=UA), timeout=120).read()
    sha = hashlib.sha256(data).hexdigest()
    if sha != ZONES_SHA256:
        print(f"PROBLEM zone file sha256 {sha} is not the pinned {ZONES_SHA256} (a new NWS file: bump the pins)")
        return 1
    zones, coast_ids = build(data, a.cache)
    doc = write(zones, coast_ids, sha, a.out)
    probs = check(doc)
    for p in probs:
        print("PROBLEM", p)
    print(f"wrote {a.out} ({os.path.getsize(a.out)} B)")
    return 1 if probs else 0


if __name__ == "__main__":
    sys.exit(main())
