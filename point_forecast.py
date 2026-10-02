"""A forecast for any ocean point, read from the forecast-point product the points job publishes
(tools/model_frames/points.py; format in tools/model_frames/pointfmt.py; plan section 31).

Three parts:
  - the point's id: "pt_21667N_158054W" (latitude and longitude in thousandths of a degree), one spelling per point;
  - PointSource: pointer -> manifest -> the grids' sea masks -> the tile of the point's model cell, with small
    caches (the web service is one worker: nothing here may hold much or fetch twice);
  - pure functions that turn the cell's 15 planes into the forecast table's rows. NOAA's gridded files give the
    wind sea and three swell partitions per step, ordered by height at each step, so partition 1 at one hour is
    not the same swell train as partition 1 at the next: track_partitions() follows the trains through time and
    gives each one a column, as the point bulletins do.

numpy and pointfmt are loaded on first use: the web process pays for them only once somebody asks for a point.
"""
import importlib.util
import json
import math
import os
import re
import sys
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta

PREFIX = "gfswave/points/v1"
REACH_KM = 40.0                # how far the nearest sea cell may be from the point (1.4 cells of 1/4 degree, 2.2 of 1/6)
NCOL = 6                       # the table's swell columns (the rows' shape, shared with the bulletins)
USED = 4                       # ... of which a point fills four: NOAA gives the wind sea and three swells at most
DIE_H = 12                     # hours a swell train may go unseen and still be the same train when it returns
WEEK_H = 168                   # the columns are ordered by their energy over the first seven days
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


def fmt_coord(lat, lon, places=3):
    """'21.667N 158.054W'."""
    return f"{abs(lat):.{places}f}{'N' if lat >= 0 else 'S'} {abs(lon):.{places}f}{'E' if lon >= 0 else 'W'}"


# ------------------------------- the manifest -----------------------------------------

_RUN_RE = re.compile(r"\d{10}")
_GRID_RE = re.compile(r"[a-z0-9]{1,8}")


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
            except (PointError, ValueError):
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
                self._masks[key] = mask
            return mask
        return self._once(("mask",) + key, cached, build)

    def locate(self, man, lat, lon):
        """The point's model cell (nearest_sea_cell), or None: land, ice, or outside every grid."""
        masks = {g["name"]: self.mask(man, g) for g in man["grids"] if grid_in_reach(g, lat)}
        return nearest_sea_cell(man["grids"], masks, lat, lon) if masks else None

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
                if key not in self._tiles:
                    self._tiles[key] = blob
                    self._tile_bytes += len(blob)
                    while self._tile_bytes > TILE_CACHE_BYTES and len(self._tiles) > 1:
                        _old, dropped = self._tiles.popitem(last=False)
                        self._tile_bytes -= len(dropped)
            return blob
        return self._once(("tile",) + key, cached, build)

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
            header, bitmap, planes = PF.decode_tile(blob, steps=len(man["steps"]), fields=PF.FIELD_NAMES)
        except ValueError as exc:
            raise PointError("Forecast points are temporarily unavailable") from exc
        if (header["run"], header["grid"], header["tile"], header["row0"], header["col0"]) != (man["run"], grid["name"], [tr, tc], tr * t, tc * t):
            raise PointError("Forecast points are temporarily unavailable")
        i = PF.cell_index(bitmap, cell["row"] - tr * t, cell["col"] - tc * t)
        if i < 0:                                                    # the mask says sea, the tile does not
            raise PointError("Forecast points are temporarily unavailable")
        codes = planes[:, :, i].copy()
        with self._lock:
            self._cells[key] = codes
            while len(self._cells) > CELL_CACHE_MAX:
                self._cells.popitem(last=False)
        return codes

    def stats(self):
        with self._lock:
            return {"masks": len(self._masks), "tiles": len(self._tiles), "tile_bytes": self._tile_bytes, "cells": len(self._cells)}


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


def _match_cost(part, track, gap_h):
    """How far a partition is from where a train was last seen (0 = the same), or None when it cannot be that
    train: the peak period within 15 % (at least 1 s), the direction within 30 degrees (45 for short periods,
    which turn with the wind); both gates open up with the hours since the train was seen (x 1.7 across a 3-hour
    step, x 2 at most). The wind sea is the wind sea: while NOAA calls both the train's last partition and this
    one the wind sea, they are the same train whatever the period did (its peak jumps as the wind rises and
    falls), up to a quarter turn of direction. A wind sea that the model re-labels as swell (the wind dropped)
    is followed by the ordinary gates, so it keeps its column."""
    widen = min(2.0, max(1.0, math.sqrt(gap_h)))
    if len(part) > 3 and part[3] and track.get("wind"):
        gate_t, gate_d = 8.0, 90.0
    else:
        gate_t = max(1.0, 0.15 * track["tp"]) * widen
        gate_d = (45.0 if min(part[1], track["tp"]) < 7.0 else 30.0) * widen
    dt = abs(part[1] - track["tp"])
    dd = abs((part[2] - track["dir"] + 180.0) % 360.0 - 180.0)
    if dt > gate_t or dd > gate_d:
        return None
    return dt / gate_t + dd / gate_d


def _assign(parts, live, hour):
    """The best pairing of this step's partitions with the live trains: as many pairs as possible, then the
    least total cost. -> [index into live, or None] per partition."""
    costs = [[_match_cost(p, t, hour - t["hour"]) for t in live] for p in parts]
    best = {"score": None, "pick": [None] * len(parts)}

    def walk(i, used, pick, pairs, total):
        if i == len(parts):
            score = (-pairs, total)
            if best["score"] is None or score < best["score"]:
                best["score"], best["pick"] = score, list(pick)
            return
        for j in range(len(live)):
            if j not in used and costs[i][j] is not None:
                walk(i + 1, used | {j}, pick + [j], pairs + 1, total + costs[i][j])
        walk(i + 1, used, pick + [None], pairs, total)
    walk(0, frozenset(), [], 0, 0.0)
    return best["pick"]


def track_partitions(hours, parts):
    """Follow the swell trains through the run. hours: each step's forecast hour (increasing); parts: each step's
    partitions_at(). -> per step a list of NCOL entries, a partition or None: one of the first USED columns per
    train while it lives. A train unseen for more than DIE_H hours is over and its column is free again; a new
    train takes the free column that has been free the longest; when every column is held, the train unseen the
    longest gives its column up (if it returns, it is a new train). The columns are then ordered by their energy
    over the first WEEK_H hours (column 1 = the most energetic)."""
    tracks, out = [], []
    last_used = [None] * NCOL                                        # the hour each column last held a partition
    for hour, step in zip(hours, parts):
        live = [t for t in tracks if hour - t["hour"] <= DIE_H]
        pick = _assign(step, live, hour)
        row = [None] * NCOL
        new = []
        for p, j in zip(step, pick):
            if j is None:
                new.append(p)
                continue
            t = live[j]
            t.update(tp=p[1], dir=p[2], hour=hour, wind=len(p) > 3 and p[3])
            row[t["col"]] = p
        for p in sorted(new, key=lambda p: -p[0]):                   # the bigger new train chooses first
            held = {t["col"] for t in live}
            free = [c for c in range(USED) if c not in held]
            if free:
                col = min(free, key=lambda c: (last_used[c] is not None, last_used[c] or 0, c))
            else:                                                    # every column holds a live train: the stalest one goes
                victim = min((t for t in live if row[t["col"]] is None), key=lambda t: t["hour"], default=None)
                if victim is None:
                    continue                                         # more partitions in one step than columns: a fifth is dropped
                live.remove(victim)
                tracks.remove(victim)
                col = victim["col"]
            t = {"col": col, "tp": p[1], "dir": p[2], "hour": hour, "wind": len(p) > 3 and p[3]}
            tracks.append(t)
            live.append(t)
            row[col] = p
        for c in range(NCOL):
            if row[c] is not None:
                last_used[c] = hour
        tracks = [t for t in tracks if hour - t["hour"] <= DIE_H]
        out.append(row)
    # order the columns by energy over the first week (height squared x the hours the step stands for)
    energy = [0.0] * NCOL
    for i, (hour, row) in enumerate(zip(hours, out)):
        if hour - hours[0] > WEEK_H:
            break
        span = (hours[i + 1] - hour) if i + 1 < len(hours) else 1
        for c in range(NCOL):
            if row[c] is not None:
                energy[c] += row[c][0] ** 2 * span
    order = sorted(range(NCOL), key=lambda c: (-energy[c], c))
    return [[row[c] for c in order] for row in out]


# ------------------------------- the table's rows --------------------------------------

def point_rows(codes, steps, run_dt, tz):
    """The forecast rows in the bulletin parsers' 23-column shape: [date, time, 6 x (height ft, peak period s,
    direction deg FROM), wind m/s, wind direction deg FROM, combined height ft], times in the pytz zone `tz`.
    codes: [field][step]; steps: forecast hours; run_dt: the cycle (naive UTC)."""
    import pytz
    PF = pointfmt()
    ix = {n: i for i, n in enumerate(PF.FIELD_NAMES)}
    parts = [partitions_at(codes, si) for si in range(len(steps))]
    columns = track_partitions(steps, parts)
    rows = []
    for si, hour in enumerate(steps):
        local = pytz.utc.localize(run_dt + timedelta(hours=hour)).astimezone(tz)
        row = [f"{local:%A, %B} {local.day}, {local.year}", local.strftime("%I:%M %p").lstrip("0")]
        for p in columns[si]:
            row.extend((None, None, None) if p is None else (round(p[0] * M_TO_FT, 2), round(p[1], 1), int(round(p[2])) % 360))
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
            f"Location : {fmt_coord(lat, lon)} (model cell {fmt_coord(cell['lat'], cell['lon'], 2)}, {cell['km']:.0f} km away)")
