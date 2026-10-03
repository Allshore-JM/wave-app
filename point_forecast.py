"""A forecast for any ocean point, read from the forecast-point product the points job publishes
(tools/model_frames/points.py; format in tools/model_frames/pointfmt.py; plan section 31).

Four parts:
  - the point's id: "pt_21667N_158054W" (latitude and longitude in thousandths of a degree), one spelling per point;
  - the coast: the site's published GSHHG coastlines (static/coast/v1, the files the swell-exposure tool reads) say
    whether the point is water and whether a model cell can be reached from it over water (owner, 2026-10-02: land
    is refused, and so is water whose only model cells lie beyond land);
  - PointSource: pointer -> manifest -> the grids' sea masks -> the tile of the point's model cell, with small
    caches (the web service is one worker: nothing here may hold much or fetch twice);
  - pure functions that turn the cell's 15 planes into the forecast table's rows: at every step the wind sea and
    the swells in rank order (rank_groups: height squared x peak period, the rule of every forecast table of the site).

numpy and pointfmt are imported on first use (numpy is usually loaded already: the time-zone lookup behind the
live-buoy list imports it); pointfmt costs the web process only once somebody asks for a point.
"""
import importlib.util
import json
import math
import os
import re
import struct
import sys
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta

PREFIX = "gfswave/points/v1"
REACH_KM = 40.0                # how far the nearest sea cell may be from the point (1.4 cells of 1/4 degree, 2.2 of 1/6)
NCOL = 6                       # the table's swell columns (the rows' shape, shared with the bulletins)
USED = 4                       # ... of which a point fills four: NOAA gives the wind sea and three swells at most
M_TO_FT = 3.28084

POINTER_TTL_S = 300            # the pointer's own max-age
POINTER_RETRY_S = 60           # after a failed read: not again before this (no request waits on a dead bucket twice)
POINTER_KEEP_S = 6 * 3600      # how long the last manifest is served when the pointer cannot be read
POINTER_WAIT_S = 2.0           # how long a request waits for another's read of the pointer (the service has four threads)
MAX_JSON_BYTES = 256 * 1024
MAX_MASK_BYTES = 2 * 1024 * 1024
MAX_TILE_BYTES = 4 * 1024 * 1024     # compressed; a real tile is under 1 MB
TILE_CACHE_BYTES = 24 * 1024 * 1024
CELL_CACHE_MAX = 256                 # one cell's series is ~6 KB

COAST_PREFIX = "static/coast/v1"     # the coastlines the page's exposure tool reads (tools/coast/build_coast.py)
COAST_CELL_DEG = 5                   # tier 1: full resolution in 5-degree cells
COAST_Q = 10000
MAX_COAST_BYTES = 2 * 1024 * 1024    # one cell; the largest is 0.4 MB
COAST_CACHE_BYTES = 16 * 1024 * 1024
SHORE_M = 300.0                      # "land" this close to water is the shore: a beach, a pier, the data's own error
SHORE_STEP_M = 50.0                  # the shore band is searched for water on rings this far apart ...
SHORE_DIRS = 32                      # ... in this many directions
PATH_LAND_KM = 0.1                   # this much land on the straight path = land lies between (a rock, a reef flat is less)

REFUSALS = {                         # reason -> what the visitor is told (final answers: asking again changes nothing)
    "land": ("That point is land or inland water in the site's coastline data. If it is the sea, try a point a little "
             "farther from the shore."),
    "sheltered": "No forecast here: the wave model's nearest points lie beyond land (sheltered water).",
    "nodata": "The wave model has no data here: sea ice, or outside its coverage.",
}

_PF = None


def pointfmt():
    """tools/model_frames/pointfmt.py, the format's own module (the job writes with it; this reads with it)."""
    global _PF
    if _PF is None:
        mod = sys.modules.get("pointfmt")
        if mod is None:
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools", "model_frames", "pointfmt.py")
            spec = importlib.util.spec_from_file_location("pointfmt", path)
            mod = importlib.util.module_from_spec(spec)
            sys.modules["pointfmt"] = mod
            spec.loader.exec_module(mod)
        _PF = mod
    return _PF


class PointError(Exception):
    """Something the user can be told (the message is shown as it is)."""


# ------------------------------- the point's id ---------------------------------------

_ID_RE = re.compile(r"pt_(\d{1,5})([NS])_(\d{1,6})([EW])")


def is_point_id(station):
    return isinstance(station, str) and station.startswith("pt_")


def point_id(lat, lon):
    """The id of the point at lat / lon (degrees; any longitude), rounded to a thousandth of a degree. A zero is
    N or E, the antimeridian is 180 W: one spelling per point."""
    lat_m = int(math.floor(abs(float(lat)) * 1000 + 0.5))
    lon = float(lon)
    if not -180.0 <= lon < 180.0:                                    # only then: the arithmetic costs the last digits
        lon = (lon + 180.0) % 360.0 - 180.0
    lon_m = int(math.floor(abs(lon) * 1000 + 0.5))
    if lat_m > 90000:
        raise ValueError("latitude out of range")
    ns = "S" if lat < 0 and lat_m else "N"
    ew = "W" if (lon < 0 and lon_m) or lon_m == 180000 else "E"
    return f"pt_{lat_m}{ns}_{lon_m}{ew}"


def parse_point_id(station):
    """(lat, lon) of a point id, or None for anything that is not the one spelling point_id() gives."""
    m = _ID_RE.fullmatch(station or "")
    if not m:
        return None
    lat_m, lon_m = int(m.group(1)), int(m.group(3))
    if lat_m > 90000 or lon_m > 180000:
        return None
    lat = lat_m / 1000.0 * (-1 if m.group(2) == "S" else 1)
    lon = lon_m / 1000.0 * (-1 if m.group(4) == "W" else 1)
    return (lat, lon) if point_id(lat, lon) == station else None


def zone_label(name):
    """A time zone as a visitor reads it: the nautical "Etc/GMT+11" means UTC-11 (POSIX's sign, backwards), so it is
    shown as "UTC-11" with a true minus sign (the page's zoneLabel does the same); every other name as it is."""
    z = "" if name is None else str(name)
    m = re.fullmatch(r"Etc/GMT([+-])(\d{1,2})", z)
    if m:
        sign = "−" if m.group(1) == "+" else "+"
        return f"UTC{sign}{int(m.group(2))}" if int(m.group(2)) else "UTC"
    return "UTC" if re.fullmatch(r"Etc/(GMT|UTC|UCT|Universal|Zulu|Greenwich)(0|[+-]0)?", z) else z


def fmt_coord(lat, lon, places=3):
    """'21.667N 158.054W'."""
    return f"{abs(lat):.{places}f}{'N' if lat >= 0 else 'S'} {abs(lon):.{places}f}{'E' if lon >= 0 else 'W'}"


# ------------------------------- the manifest -----------------------------------------

_RUN_RE = re.compile(r"\d{10}")
_GRID_RE = re.compile(r"[a-z0-9]{1,8}")
_COAST_NAME_RE = re.compile(r"-?\d{1,2}_-?\d{1,3}")


def _is_int(v, lo, hi):
    return type(v) is int and lo <= v <= hi


def check_manifest(man):
    """The manifest reduced to what this reader uses, every value checked: run, steps, grids. Raises PointError
    for a product this code does not read (another format, other fields or scales, a grid it cannot place)."""
    PF = pointfmt()
    try:
        if not isinstance(man, dict) or man.get("format") != PF.FORMAT or man.get("complete") is not True:
            raise ValueError("format")
        run = man["run"]
        if not isinstance(run, str) or not _RUN_RE.fullmatch(run):
            raise ValueError("run")
        run_dt = datetime.strptime(run, "%Y%m%d%H")
        steps = man["steps"]
        if (not isinstance(steps, list) or not 0 < len(steps) <= 1024 or not all(_is_int(s, 0, 2000) for s in steps)
                or any(b <= a for a, b in zip(steps, steps[1:]))):
            raise ValueError("steps")
        fields = man["fields"]
        names = [f.get("name") for f in fields] if isinstance(fields, list) and all(isinstance(f, dict) for f in fields) else None
        if names != list(PF.FIELD_NAMES) or man.get("missing") != PF.MISSING:
            raise ValueError("fields")
        for f in fields:
            kind = PF.FIELD_KIND[f["name"]]
            if f.get("kind") != kind or f.get("scale") != PF.KINDS[kind]["scale"]:
                raise ValueError("scales")
        grids = []
        if not isinstance(man["grids"], list) or not 0 < len(man["grids"]) <= 8:
            raise ValueError("grids")
        for g in man["grids"]:
            name = g.get("name") if isinstance(g, dict) else None
            if not isinstance(name, str) or not _GRID_RE.fullmatch(name) or any(name == x["name"] for x in grids):
                raise ValueError("grid name")
            if not (_is_int(g.get("ni"), 1, PF.MAX_MASK_NI) and _is_int(g.get("nj"), 1, PF.MAX_MASK_NJ)
                    and _is_int(g.get("per_deg"), 1, 120) and _is_int(g.get("tile"), 1, 64)):
                raise ValueError("grid size")
            lat0 = g.get("lat0")
            if type(lat0) not in (int, float) or not -90.0 <= lat0 <= 90.0 or g.get("lon0") not in (0, 0.0):
                raise ValueError("grid origin")
            if g["ni"] != 360 * g["per_deg"]:                       # a whole circle of longitude, columns from 0 E
                raise ValueError("grid longitudes")
            rows = g.get("data_rows")
            if (not isinstance(rows, list) or len(rows) != 2 or not all(_is_int(r, 0, g["nj"] - 1) for r in rows)
                    or rows[0] > rows[1]):
                raise ValueError("grid rows")
            grids.append({"name": name, "ni": g["ni"], "nj": g["nj"], "lat0": float(lat0), "per_deg": g["per_deg"],
                          "tile": g["tile"], "data_rows": (rows[0], rows[1])})
        published = man.get("published_utc")
        return {"run": run, "run_dt": run_dt, "steps": list(steps), "grids": grids,
                "published_utc": published if isinstance(published, str) else None}
    except PointError:
        raise
    except Exception as exc:                                         # noqa: BLE001  KeyError, TypeError, ValueError ...
        raise PointError("Forecast points are temporarily unavailable") from exc


# ------------------------------- the nearest sea cell ---------------------------------

def _km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371.0 * 2 * math.asin(min(1.0, math.sqrt(a)))


def grid_in_reach(grid, lat, reach_km=REACH_KM):
    """Whether a grid's rows with data come within reach of this latitude (so its mask is worth fetching)."""
    north = grid["lat0"] - grid["data_rows"][0] / grid["per_deg"]
    south = grid["lat0"] - grid["data_rows"][1] / grid["per_deg"]
    pad = reach_km / 111.0
    return south - pad <= lat <= north + pad


def sea_cells_in_reach(grids, masks, lat, lon, reach_km=REACH_KM):
    """Every sea cell of any grid within reach_km of the point, nearest first (on a tie the earlier grid, then the
    scan's order: nearest_sea_cell's choice comes first). -> [{"grid", "row", "col", "lat", "lon", "km", "half"}]
    (half = half a cell in degrees)."""
    found = []
    for grid in grids:
        mask = masks.get(grid["name"])
        if mask is None:
            continue
        per, ni, nj = grid["per_deg"], grid["ni"], grid["nj"]
        r0 = int(round((grid["lat0"] - lat) * per))
        c0 = int(round((lon % 360.0) * per))
        kr = int(math.ceil(reach_km / 111.0 * per)) + 1
        coslat = max(0.05, math.cos(math.radians(min(89.0, abs(lat) + reach_km / 111.0))))
        kc = min(ni // 2, int(math.ceil(reach_km / (111.0 * coslat) * per)) + 1)
        for r in range(max(0, r0 - kr), min(nj - 1, r0 + kr) + 1):
            cell_lat = grid["lat0"] - r / per
            for c in range(c0 - kc, c0 + kc + 1):
                cc = c % ni
                if not mask[r, cc]:
                    continue
                cell_lon = cc / per
                km = _km(lat, lon, cell_lat, cell_lon)
                if km <= reach_km:
                    found.append({"grid": grid["name"], "row": r, "col": cc, "lat": cell_lat,
                                  "lon": cell_lon - 360.0 if cell_lon >= 180.0 else cell_lon, "km": km, "half": 0.5 / per})
    best = nearest_sea_cell(grids, masks, lat, lon, reach_km)
    found.sort(key=lambda c: c["km"])                               # stable: ties stay in grid, then scan order
    if best:                                                         # the tie rule of nearest_sea_cell, exactly
        first = next(i for i, c in enumerate(found) if (c["grid"], c["row"], c["col"]) == (best["grid"], best["row"], best["col"]))
        found.insert(0, found.pop(first))
    return found


# ------------------------------- the coast: land, and what lies between ---------------

def coast_cell_name(lat, lon):
    """The 5-degree coast cell that holds the point: "20_-160" (its south-west corner)."""
    lon = (lon + 180.0) % 360.0 - 180.0
    la = min(90 - COAST_CELL_DEG, int(math.floor(lat / COAST_CELL_DEG)) * COAST_CELL_DEG)
    lo = int(math.floor(lon / COAST_CELL_DEG)) * COAST_CELL_DEG
    return f"{la}_{lo}"


def decode_coast(buf):
    """A coast-v1 cell (tools/coast/build_coast.py; static_ui/tools.js decodeCoastLL reads the same bytes) as its
    ring edges: (xi, yi, xj, yj) float64 arrays in degrees, edge = previous vertex j -> vertex i of each closed ring.
    ValueError for anything that is not a whole, consistent cell."""
    import numpy as np
    if not isinstance(buf, (bytes, bytearray)) or not 40 <= len(buf) <= MAX_COAST_BYTES or bytes(buf[:4]) != b"CST1":
        raise ValueError("coast cell")
    q, n_pieces, n_rings, n_verts = struct.unpack_from("<IIII", buf, 8)
    if (q != COAST_Q or n_pieces > 200000 or n_rings > 400000 or n_verts > 4000000 or n_rings < n_pieces
            or n_verts < 3 * n_rings or 5 * n_pieces + n_rings + 2 * n_verts > len(buf) - 40):
        raise ValueError("coast cell counts")
    a = np.frombuffer(bytes(buf), dtype=np.uint8, offset=40)
    if a.size == 0:
        if n_pieces or n_rings or n_verts:
            raise ValueError("coast cell length")
        z = np.zeros(0)
        return z, z, z, z                                            # a cell without land
    if a[-1] & 128:
        raise ValueError("coast cell varints")
    ends = np.flatnonzero(a < 128)
    starts = np.concatenate(([0], ends[:-1] + 1))
    lens = ends - starts + 1
    if int(lens.max()) > 5:
        raise ValueError("coast cell varints")
    place = np.arange(a.size) - np.repeat(starts, lens)
    vals = np.add.reduceat((a & 127).astype(np.int64) << (7 * place), starts)
    signed = np.where(vals & 1, -((vals + 1) // 2), vals // 2)
    # the structure first (plain integers), then every ring's vertices at once
    i, total, verts = 0, len(vals), 0
    at, count, base_x, base_y = [], [], [], []                       # per ring: where its deltas start, how many vertices, its piece's origin
    for _ in range(n_pieces):
        if i + 5 > total:
            raise ValueError("coast cell pieces")
        minx, miny, nr = int(signed[i]), int(signed[i + 1]), int(vals[i + 4])
        i += 5
        if len(at) + nr > n_rings:
            raise ValueError("coast cell rings")
        for _k in range(nr):
            if i >= total:
                raise ValueError("coast cell rings")
            n = int(vals[i])
            i += 1
            if n < 3 or verts + n > n_verts or i + 2 * n > total:
                raise ValueError("coast cell ring")
            at.append(i)
            count.append(n)
            base_x.append(minx)
            base_y.append(miny)
            i += 2 * n
            verts += n
    if len(at) != n_rings or verts != n_verts or i != total:
        raise ValueError("coast cell length")
    inv = 1.0 / q                                                    # as the page: x * (1 / q)
    if not at:
        z = np.zeros(0)
        return z, z, z, z
    count = np.array(count)
    first = np.concatenate(([0], np.cumsum(count)[:-1]))             # each ring's first vertex in the flat arrays
    ring_of = np.repeat(np.arange(len(count)), count)
    src = np.repeat(np.array(at), count) + 2 * (np.arange(n_verts) - np.repeat(first, count))
    out = []
    for off, base, lim in ((0, base_x, 180 * q + 1), (1, base_y, 90 * q + 1)):
        run = np.cumsum(signed[src + off])
        before = np.concatenate(([0], run[:-1]))[first]              # the running sum just before each ring
        v = run - before[ring_of] + np.array(base)[ring_of]
        if int(np.abs(v).max()) > lim:
            raise ValueError("coast cell coordinates")
        prev = np.concatenate(([0], v[:-1]))
        prev[first] = v[first + count - 1]                           # a ring's first vertex follows its last
        out.append((v * inv, prev * inv))
    return out[0][0], out[1][0], out[0][1], out[1][1]


def land_parity(edges, lons, lats):
    """Even-odd point-in-land for points inside ONE coast cell (the page's inLand: the same half-open crossing rule,
    the same arithmetic). edges: decode_coast()'s; lons / lats: float arrays. -> bool array."""
    import numpy as np
    xi, yi, xj, yj = edges
    out = np.zeros(len(lats), dtype=bool)
    if not len(xi) or not len(lats):
        return out
    near = (np.minimum(yi, yj) <= lats.max()) & (np.maximum(yi, yj) > lats.min())   # only these can be crossed
    xi, yi, xj, yj = xi[near], yi[near], xj[near], yj[near]
    if not len(xi):
        return out
    for a in range(0, len(lats), 64):                                # 64 points x the edges at a time
        la, lo = lats[a:a + 64, None], lons[a:a + 64, None]
        cross = (yi > la) != (yj > la)
        with np.errstate(divide="ignore", invalid="ignore"):
            hit = cross & (lo < (xj - xi) * (la - yi) / (yj - yi) + xi)
        out[a:a + 64] = (hit.sum(axis=1) & 1).astype(bool)
    return out


def water_origin(land, lat, lon):
    """Where the point's path to its model cell starts: the point itself when the coast data says water; else the
    nearest water within SHORE_M (rings every SHORE_STEP_M, SHORE_DIRS directions; the nearest ring first, then
    clockwise from north): the point stands on the shore band (a beach, a pier, the data's own error); else None (land).
    land(lats, lons) -> bool array. -> (lat, lon) or None."""
    import numpy as np
    if not land(np.array([lat]), np.array([lon]))[0]:
        return lat, lon
    coslat = max(0.05, math.cos(math.radians(lat)))
    rings = np.arange(1, int(round(SHORE_M / SHORE_STEP_M)) + 1) * SHORE_STEP_M
    bearing = np.radians(np.arange(SHORE_DIRS) * 360.0 / SHORE_DIRS)
    m, b = np.repeat(rings, SHORE_DIRS), np.tile(bearing, len(rings))
    lats = np.clip(lat + m * np.cos(b) / 111195.0, -90.0, 90.0)      # no sample beyond a pole (G22 R-A20)
    lons = lon + m * np.sin(b) / (111195.0 * coslat)
    water = ~land(lats, lons)
    if not water.any():
        return None
    k = int(np.flatnonzero(water)[0])
    return float(lats[k]), float(lons[k])


def coast_cells_over(west, east, south, north):
    """The names of the coast cells a box meets (longitudes within -180..180)."""
    c = COAST_CELL_DEG
    names = set()
    for la in range(int(math.floor(south / c)) * c, int(math.floor(north / c)) * c + 1, c):
        for lo in range(int(math.floor(west / c)) * c, int(math.floor(east / c)) * c + 1, c):
            names.add(coast_cell_name(la + c / 2.0, lo + c / 2.0))
    return sorted(names)


def path_crossings(edges, x0, y0, x1, y1):
    """Where the straight path (x0, y0) -> (x1, y1) (degrees; the lat / lon plane) crosses the coast edges: sorted
    t in [0, 1] along the path. Each edge counts from its first vertex up to (not including) its second, so a path
    through a vertex crosses once."""
    import numpy as np
    xi, yi, xj, yj = edges
    if not len(xi):
        return np.zeros(0)
    ex, ey = xi - xj, yi - yj                                        # edge: vertex j -> vertex i
    dx, dy = x1 - x0, y1 - y0
    den = dx * ey - dy * ex
    qx, qy = xj - x0, yj - y0
    with np.errstate(divide="ignore", invalid="ignore"):
        t = (qx * ey - qy * ex) / den
        u = (qx * dy - qy * dx) / den
    hit = (den != 0) & (t >= 0) & (t <= 1) & (u >= 0) & (u < 1)
    return np.sort(t[hit])


def land_runs(ts, start_land, end_land):
    """The stretches of the path on land, [(t0, t1)], from its crossings and the land test at its two ends; runs that
    touch (a cell line the builder cut a polygon at is crossed twice) are one run. None when the crossings disagree
    with the ends (a graze the arithmetic cannot settle)."""
    state, prev, out = bool(start_land), 0.0, []
    for t in ts:
        t = float(t)
        if state:
            if out and prev - out[-1][1] <= 1e-12:
                out[-1] = (out[-1][0], t)
            else:
                out.append((prev, t))
        state, prev = not state, t
    if state:
        if out and prev - out[-1][1] <= 1e-12:
            out[-1] = (out[-1][0], 1.0)
        else:
            out.append((prev, 1.0))
    return out if state == bool(end_land) else None


def path_blocked(runs, km, half_km):
    """Whether land lies between the point and its cell: a stretch of PATH_LAND_KM or more anywhere on the path. The
    one stretch that reaches the cell's centre is forgiven up to half a cell (half_km): some sea cells have their
    centre on an islet or a headland. runs: land_runs()'s (None = cannot tell: blocked)."""
    if runs is None:
        return True
    for t0, t1 in runs:
        length = (t1 - t0) * km
        if t1 == 1.0 and length <= half_km:
            continue
        if length >= PATH_LAND_KM:
            return True
    return False


def nearest_sea_cell(grids, masks, lat, lon, reach_km=REACH_KM):
    """The nearest sea cell of any grid within reach_km of the point, or None. grids: check_manifest()'s, in
    priority order; masks: {grid name: bool [nj, ni]} (a grid without a mask is not looked at). On a tie the
    earlier grid wins. -> {"grid", "row", "col", "lat", "lon", "km"}."""
    best = None
    for grid in grids:
        mask = masks.get(grid["name"])
        if mask is None:
            continue
        per, ni, nj = grid["per_deg"], grid["ni"], grid["nj"]
        r0 = int(round((grid["lat0"] - lat) * per))
        c0 = int(round((lon % 360.0) * per))
        kr = int(math.ceil(reach_km / 111.0 * per)) + 1
        coslat = max(0.05, math.cos(math.radians(min(89.0, abs(lat) + reach_km / 111.0))))
        kc = min(ni // 2, int(math.ceil(reach_km / (111.0 * coslat) * per)) + 1)
        for r in range(max(0, r0 - kr), min(nj - 1, r0 + kr) + 1):
            cell_lat = grid["lat0"] - r / per
            for c in range(c0 - kc, c0 + kc + 1):
                cc = c % ni
                if not mask[r, cc]:
                    continue
                cell_lon = cc / per
                km = _km(lat, lon, cell_lat, cell_lon)
                if km <= reach_km and (best is None or km < best["km"] - 1e-9):
                    best = {"grid": grid["name"], "row": r, "col": cc, "lat": cell_lat,
                            "lon": cell_lon - 360.0 if cell_lon >= 180.0 else cell_lon, "km": km}
    return best


# ------------------------------- the source (network + caches) ------------------------

class PointSource:
    """Reads the product through `fetch(url, max_bytes) -> bytes` (raises on anything but a whole 200 body) from
    `base()` (the points prefix's URL, or "" when the product is not configured). Thread-safe."""

    def __init__(self, base, fetch, clock=time.time):
        self._base, self._fetch, self._clock = base, fetch, clock
        self._lock = threading.Lock()
        self._manifest_lock = threading.Lock()
        self._manifest = None            # (checked manifest, read at, next read at)
        self._fail_until = 0.0           # no manifest and the last read failed: fail at once until then
        self._masks = {}                 # (run, grid) -> bool array
        self._tiles = OrderedDict()      # (run, grid, tr, tc) -> blob
        self._tile_bytes = 0
        self._cells = OrderedDict()      # (run, grid, row, col) -> uint16 [field, step]
        self._inflight = {}              # key -> Lock
        self._coast_index = None         # {cell name: bytes}: the coast cells that hold land
        self._coast = OrderedDict()      # cell name -> (edges, bytes held)
        self._coast_bytes = 0

    def _url(self, key):
        base = self._base()
        if not base:
            raise PointError("Forecast points are not available on this server")
        return f"{base}/{key}"

    def _get(self, key, max_bytes):
        try:
            return self._fetch(self._url(key), max_bytes)
        except PointError:
            raise
        except Exception as exc:                                     # noqa: BLE001
            raise PointError("Forecast points are temporarily unavailable") from exc

    def manifest(self):
        """The live run's manifest (checked). Read again when the pointer's five minutes are over; if that
        fails, the last one is served for a while (a run stays in the bucket for about twelve hours)."""
        if not self._base():
            raise PointError("Forecast points are not available on this server")

        def fresh():
            with self._lock:
                have, fail_until = self._manifest, self._fail_until
            now = self._clock()
            if have and now < have[2]:
                return have, have[0]
            if not have and now < fail_until:
                raise PointError("Forecast points are temporarily unavailable")
            return have, None
        have, man = fresh()
        if man is not None:
            return man
        if not self._manifest_lock.acquire(timeout=POINTER_WAIT_S):   # somebody is reading it, slowly: do not queue up behind
            if have:
                return have[0]
            raise PointError("Forecast points are temporarily unavailable")
        try:
            have, man = fresh()                                      # read by the caller we waited for?
            if man is not None:
                return man
            try:
                pointer = json.loads(self._get(f"{PREFIX}/latest.json", MAX_JSON_BYTES).decode("utf-8"))
                run = pointer.get("run") if isinstance(pointer, dict) else None
                mkey = pointer.get("manifest") if isinstance(pointer, dict) else None
                if (not isinstance(run, str) or not _RUN_RE.fullmatch(run) or not isinstance(mkey, str)
                        or not re.fullmatch(rf"{PREFIX}/{run}/manifest-[0-9TZ]{{1,32}}\.json", mkey)):
                    raise PointError("Forecast points are temporarily unavailable")
                if have and have[0]["run"] == run:
                    man = have[0]                                    # the same run: its manifest never changes
                else:
                    man = check_manifest(json.loads(self._get(mkey, MAX_JSON_BYTES).decode("utf-8")))
                    if man["run"] != run:
                        raise PointError("Forecast points are temporarily unavailable")
            except (PointError, ValueError, RecursionError):         # RecursionError: JSON nested too deep (G22 A-10)
                now = self._clock()
                with self._lock:
                    if have and now - have[1] < POINTER_KEEP_S:
                        self._manifest = (have[0], have[1], now + POINTER_RETRY_S)
                        return have[0]
                    self._manifest, self._fail_until = None, now + POINTER_RETRY_S
                raise PointError("Forecast points are temporarily unavailable") from None
            with self._lock:
                now = self._clock()
                self._manifest = (man, now, now + POINTER_TTL_S)
                if not have or have[0]["run"] != man["run"]:         # a new run: nothing of the old one is asked for again
                    self._masks = {k: v for k, v in self._masks.items() if k[0] == man["run"]}
                    for cache in (self._tiles, self._cells):
                        for k in [k for k in cache if k[0] != man["run"]]:
                            if cache is self._tiles:
                                self._tile_bytes -= len(cache[k])
                            del cache[k]
            return man
        finally:
            self._manifest_lock.release()

    def _once(self, key, cached, build):
        """build() for this key in one thread at a time; the others wait and take what it stored."""
        hit = cached()
        if hit is not None:
            return hit
        with self._lock:
            flock = self._inflight.setdefault(key, threading.Lock())
        with flock:
            try:
                hit = cached()
                return hit if hit is not None else build()
            finally:
                with self._lock:
                    self._inflight.pop(key, None)

    def mask(self, man, grid):
        key = (man["run"], grid["name"])

        def cached():
            with self._lock:
                return self._masks.get(key)

        def build():
            PF = pointfmt()
            blob = self._get(f"{PREFIX}/{man['run']}/{grid['name']}/mask.bin", MAX_MASK_BYTES)
            try:
                header, mask = PF.decode_mask(blob)
            except ValueError as exc:
                raise PointError("Forecast points are temporarily unavailable") from exc
            if (header.get("run"), header.get("grid")) != key or mask.shape != (grid["nj"], grid["ni"]):
                raise PointError("Forecast points are temporarily unavailable")
            with self._lock:
                if self._keeps(man["run"]):
                    self._masks[key] = mask
            return mask
        return self._once(("mask",) + key, cached, build)

    def current_run(self):
        """The run of the manifest in hand (None before the first read)."""
        with self._lock:
            return self._manifest[0]["run"] if self._manifest else None

    def _keeps(self, run):
        """Whether objects of this run may go into the caches: only the run in hand (a request that still holds an
        older manifest at a run change must not fill them with the old run again; G22 A-11). Under self._lock."""
        return self._manifest is None or self._manifest[0]["run"] == run

    def _coast_cells(self):
        """{cell name: bytes} of the coast cells that hold land (the index of static/coast/v1). Kept once read."""
        def cached():
            with self._lock:
                return self._coast_index

        def build():
            try:
                idx = json.loads(self._get(f"{COAST_PREFIX}/index.json", MAX_JSON_BYTES).decode("utf-8"))
                tier = idx["tier1"]
                if idx.get("format") != "coast-v1" or idx.get("q") != COAST_Q or tier.get("cell") != COAST_CELL_DEG or tier.get("dir") != "f":
                    raise ValueError("coast index")
                cells = {}
                for name, entry in tier["cells"].items():
                    if (not _COAST_NAME_RE.fullmatch(name) or not isinstance(entry, list) or not entry
                            or not _is_int(entry[0], 40, MAX_COAST_BYTES)):
                        raise ValueError("coast index entry")
                    cells[name] = entry[0]
                if not cells:
                    raise ValueError("coast index empty")
            except PointError:
                raise
            except Exception as exc:                                 # noqa: BLE001  bad JSON, a missing key, RecursionError ...
                raise PointError("Forecast points are temporarily unavailable") from exc
            with self._lock:
                self._coast_index = cells
            return cells
        return self._once(("coast-index",), cached, build)

    def _coast_cell(self, name):
        """The edges of one coast cell, or None for a cell without land. A cell that cannot be read is an error:
        a point is never served untested."""
        nbytes = self._coast_cells().get(name)
        if nbytes is None:
            return None

        def cached():
            with self._lock:
                hit = self._coast.get(name)
                if hit is not None:
                    self._coast.move_to_end(name)
                    return hit[0]
                return None

        def build():
            blob = self._get(f"{COAST_PREFIX}/f/{name}.bin", nbytes)
            try:
                if len(blob) != nbytes:
                    with self._lock:                                 # the index and the cell disagree: read both again
                        self._coast_index = None                     # next time (G22 R-A19)
                    raise ValueError("coast cell size")
                edges = decode_coast(blob)
                la, lo = (int(v) for v in name.split("_"))
                top = 90 if la == 90 - COAST_CELL_DEG else la + COAST_CELL_DEG
                if len(edges[0]) and (edges[0].min() < lo - 1e-9 or edges[0].max() > lo + COAST_CELL_DEG + 1e-9
                                      or edges[1].min() < la - 1e-9 or edges[1].max() > top + 1e-9):
                    raise ValueError("coast cell of another place")      # a cell must lie in its own box (G22 R-A19)
            except ValueError as exc:
                raise PointError("Forecast points are temporarily unavailable") from exc
            held = sum(int(e.nbytes) for e in edges)
            with self._lock:
                if name not in self._coast:
                    self._coast[name] = (edges, held)
                    self._coast_bytes += held
                    while self._coast_bytes > COAST_CACHE_BYTES and len(self._coast) > 1:
                        _old, dropped = self._coast.popitem(last=False)
                        self._coast_bytes -= dropped[1]
            return edges
        return self._once(("coast", name), cached, build)

    def land(self, lats, lons):
        """Whether each point is land by the coast data (numpy arrays in, a bool array out)."""
        import numpy as np
        lons = (lons + 180.0) % 360.0 - 180.0
        out = np.zeros(len(lats), dtype=bool)
        names = [coast_cell_name(la, lo) for la, lo in zip(lats.tolist(), lons.tolist())]
        for name in set(names):
            edges = self._coast_cell(name)
            if edges is None:
                continue
            pick = np.array([n == name for n in names])
            out[pick] = land_parity(edges, lons[pick], lats[pick])
        return out

    def edges_over(self, west, east, south, north):
        """The coast edges whose box meets [west, east] x [south, north] (degrees; the longitudes may run past +-180:
        the edges of the cells beyond come shifted by 360 to meet them)."""
        import numpy as np
        parts = []
        for shift in (-360.0, 0.0, 360.0):                           # the coast cells lie in -180..180
            a, b = west - shift, east - shift
            if b < -180.0 or a >= 180.0:
                continue
            for name in coast_cells_over(max(a, -180.0), min(b, 179.9999999), south, north):
                edges = self._coast_cell(name)
                if edges is None or not len(edges[0]):
                    continue
                xi, yi, xj, yj = edges
                keep = ((np.maximum(xi, xj) + shift >= west) & (np.minimum(xi, xj) + shift <= east)
                        & (np.maximum(yi, yj) >= south) & (np.minimum(yi, yj) <= north))
                if keep.any():
                    parts.append((xi[keep] + shift, yi[keep], xj[keep] + shift, yj[keep]))
        if not parts:
            z = np.zeros(0)
            return z, z, z, z
        return tuple(np.concatenate([p[k] for p in parts]) for k in range(4))

    def locate(self, man, lat, lon):
        """The point's model cell: the nearest sea cell within reach whose straight path from the point holds no land
        (path_blocked; the path starts at the nearest water when the point stands on the shore band).
        -> (cell, None), or (None, reason): "land" (the point is on land beyond the shore band, or on the band with
        no cell reachable from its water), "nodata" (no sea cell within reach: ice, a sea the model does not have),
        "sheltered" (water, and every cell within reach lies beyond land)."""
        import numpy as np
        origin = water_origin(self.land, lat, lon)
        if origin is None:
            return None, "land"
        masks = {g["name"]: self.mask(man, g) for g in man["grids"] if grid_in_reach(g, lat)}
        cells = sea_cells_in_reach(man["grids"], masks, lat, lon) if masks else []
        if not cells:
            return None, "nodata"
        olat, olon = origin
        x0, y0 = olon + 3.7e-9, olat + 2.9e-9                        # off the data's 1e-4 grid: no path starts on a vertex
        ends = [(olon + ((c["lon"] - olon + 180.0) % 360.0 - 180.0) + 1.3e-9, c["lat"] - 2.1e-9) for c in cells]
        xs, ys = [x0] + [e[0] for e in ends], [y0] + [e[1] for e in ends]
        edges = self.edges_over(min(xs), max(xs), max(-90.0, min(ys)), min(90.0, max(ys)))
        states = self.land(np.array(ys), np.array(xs))
        xi, yi, xj, yj = edges
        for k, cell in enumerate(cells):
            x1, y1 = ends[k]
            keep = ((np.maximum(xi, xj) >= min(x0, x1)) & (np.minimum(xi, xj) <= max(x0, x1))
                    & (np.maximum(yi, yj) >= min(y0, y1)) & (np.minimum(yi, yj) <= max(y0, y1)))
            ts = path_crossings((xi[keep], yi[keep], xj[keep], yj[keep]), x0, y0, x1, y1)
            km = _km(olat, olon, cell["lat"], cell["lon"])
            if not path_blocked(land_runs(ts, states[0], states[k + 1]), km, cell["half"] * 111.2):
                return cell, None
        return None, ("sheltered" if origin == (lat, lon) else "land")   # a click on the shore band's land: land (G22 R-B7)

    def _tile(self, man, grid, tr, tc):
        key = (man["run"], grid["name"], tr, tc)

        def cached():
            with self._lock:
                blob = self._tiles.get(key)
                if blob is not None:
                    self._tiles.move_to_end(key)
                return blob

        def build():
            blob = self._get(f"{PREFIX}/{man['run']}/{grid['name']}/{tr}_{tc}.bin", MAX_TILE_BYTES)
            with self._lock:
                if key not in self._tiles and self._keeps(man["run"]):
                    self._tiles[key] = blob
                    self._tile_bytes += len(blob)
                    while self._tile_bytes > TILE_CACHE_BYTES and len(self._tiles) > 1:
                        _old, dropped = self._tiles.popitem(last=False)
                        self._tile_bytes -= len(dropped)
            return blob
        return self._once(("tile",) + key, cached, build)

    def _drop_tile(self, key, blob):
        with self._lock:
            if self._tiles.get(key) is blob:
                del self._tiles[key]
                self._tile_bytes -= len(blob)

    def series(self, man, cell):
        """The cell's values: uint16 [field, step] (pointfmt.FIELD_NAMES order; pointfmt.MISSING = no value)."""
        key = (man["run"], cell["grid"], cell["row"], cell["col"])
        with self._lock:
            hit = self._cells.get(key)
            if hit is not None:
                self._cells.move_to_end(key)
                return hit
        PF = pointfmt()
        grid = next(g for g in man["grids"] if g["name"] == cell["grid"])
        t = grid["tile"]
        tr, tc = cell["row"] // t, cell["col"] // t
        blob = self._tile(man, grid, tr, tc)
        try:
            try:
                header, bitmap, planes = PF.decode_tile(blob, steps=len(man["steps"]), fields=PF.FIELD_NAMES)
            except ValueError as exc:
                raise PointError("Forecast points are temporarily unavailable") from exc
            if (header["run"], header["grid"], header["tile"], header["row0"], header["col0"]) != (man["run"], grid["name"], [tr, tc], tr * t, tc * t):
                raise PointError("Forecast points are temporarily unavailable")
        except PointError:
            self._drop_tile((man["run"], grid["name"], tr, tc), blob)   # a bad body is not kept: the next request fetches again (G22 A-3)
            raise
        i = PF.cell_index(bitmap, cell["row"] - tr * t, cell["col"] - tc * t)
        if i < 0:                                                    # the mask says sea, the tile does not
            raise PointError("Forecast points are temporarily unavailable")
        codes = planes[:, :, i].copy()
        with self._lock:
            if not self._keeps(man["run"]):
                return codes
            self._cells[key] = codes
            while len(self._cells) > CELL_CACHE_MAX:
                self._cells.popitem(last=False)
        return codes

    def stats(self):
        with self._lock:
            return {"masks": len(self._masks), "tiles": len(self._tiles), "tile_bytes": self._tile_bytes, "cells": len(self._cells),
                    "coast": len(self._coast), "coast_bytes": self._coast_bytes}


# ------------------------------- partitions -> columns --------------------------------

def partitions_at(codes, si):
    """The step's wave systems: [(height m, peak period s, direction deg FROM, is the wind sea), ...] for the wind
    sea and the three swells, each only when all three of its values are there. codes: [field][step] (ints)."""
    PF = pointfmt()
    out = []
    for n, names in enumerate(PF.PARTITIONS):
        h, t, d = (int(codes[PF.FIELD_NAMES.index(name)][si]) for name in names)
        if PF.MISSING in (h, t, d) or t == 0:
            continue
        out.append((h / PF.KINDS["height"]["scale"], t / PF.KINDS["period"]["scale"], d / PF.KINDS["direction"]["scale"], n == 0))
    return out


def swell_power(hs, tp):
    """What ranks a wave system: height squared x peak period. In deep water that is the energy arriving per metre
    of crest and second, and the only wave quantity in the usual breaker-height formula: the system that makes the
    most surf comes first (owner, 2026-10-02: one rule for every forecast table of the site)."""
    return (hs or 0.0) ** 2 * (tp or 0.0)


def rank_groups(groups):
    """One row's wave systems [(height, peak period, direction), ...] in rank order: the most powerful first; equal
    power: the higher first, then the source's order. A group without a height but with a period or a direction
    keeps its values, after the ranked ones (no source sends one today; G22 R-A23); empty groups last."""
    live = [g for g in groups if g[0] is not None]
    live.sort(key=lambda g: (-swell_power(g[0], g[1]), -g[0]))       # stable
    part = [tuple(g) for g in groups if g[0] is None and any(v is not None for v in g[1:])]
    return live + part + [(None, None, None)] * (len(groups) - len(live) - len(part))


# ------------------------------- the table's rows --------------------------------------

def _labels(run_dt, hour, tz):
    """(date, time) of run + hour in the pytz zone, as the bulletin parsers write them."""
    import pytz
    local = pytz.utc.localize(run_dt + timedelta(hours=hour)).astimezone(tz)
    return f"{local:%A, %B} {local.day}, {local.year}", local.strftime("%I:%M %p").lstrip("0")


def hour_slots(steps, run_dt, tz):
    """Every hour from the first step to the last, for the graphs' time axis: [(date, time, index of the step's row
    or None)]. A point's rows are hourly to +120 h and 3-hourly after; a graph with one slot per ROW would draw the
    later days at a third of their width (G22 B-1)."""
    at = {h: i for i, h in enumerate(steps)}
    return [_labels(run_dt, h, tz) + (at.get(h),) for h in range(steps[0], steps[-1] + 1)]


def point_rows(codes, steps, run_dt, tz):
    """The forecast rows in the bulletin parsers' 23-column shape: [date, time, 6 x (height ft, peak period s,
    direction deg FROM), wind m/s, wind direction deg FROM, combined height ft], times in the pytz zone `tz`.
    codes: [field][step]; steps: forecast hours; run_dt: the cycle (naive UTC). The systems of a row are in rank
    order (rank_groups), packed from the left."""
    PF = pointfmt()
    ix = {n: i for i, n in enumerate(PF.FIELD_NAMES)}
    rows = []
    for si, hour in enumerate(steps):
        row = list(_labels(run_dt, hour, tz))
        groups = [(round(p[0] * M_TO_FT, 2), round(p[1], 1), int(round(p[2])) % 360) for p in partitions_at(codes, si)]
        for g in rank_groups(groups + [(None, None, None)] * (NCOL - len(groups))):
            row.extend(g)
        wind, wdir = int(codes[ix["wind"]][si]), int(codes[ix["wdir"]][si])
        if wind == PF.MISSING:
            row.extend((None, None))
        else:
            row.extend((wind / PF.KINDS["speed"]["scale"], None if wdir == PF.MISSING else wdir % 360))
        hs = int(codes[ix["hs"]][si])
        row.append(None if hs == PF.MISSING else round(hs / PF.KINDS["height"]["scale"] * M_TO_FT, 2))
        rows.append(row)
    return rows


def point_headers(run, lat, lon, cell):
    """(cycle line, location line) in the bulletins' style, so the page's header code reads them unchanged."""
    return (f"Cycle : {run[:8]} {run[8:]} UTC",
            f"Location : {fmt_coord(lat, lon)} (nearest model point {fmt_coord(cell['lat'], cell['lon'], 2)}, {cell['km']:.0f} km away)")
