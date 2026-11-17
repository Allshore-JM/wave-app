"""NOAA RRFS wind (and weather) for the site's forecast tables, read from the RRFS job's objects (plan section 99).

The job (tools/model_frames/rrfs.py) publishes under rrfs/v1/ for every hourly cycle (+1..+18 h) and every 84-h run
(00/06/12/18Z, 0..+84 h): a small station-winds table, and one cell-major file per fine grid (hi, pr, conus, ak) that
holds every cell's series, 14 B a step, so ONE ranged GET reads a point. `latest.json` lists the kept sources newest
first.

The merge rule (plan section 99, "every update cycle"): per forecast hour the NEWEST source that covers it wins; the
tables use the newest USE["cycle"] hourly cycles and USE["run"] runs (the ones the job keeps cell files for), so the
next ~16 h come from the newest hourly cycle and the rest to +84 h from the newest run; GFS takes over after.

No network at import; the caller injects `get_bytes(url, max_bytes) -> bytes` (a whole 200) and
`get_range(url, start, end) -> bytes` (exactly those bytes, a 206). Thread-safe.

  station(sid)        -> {naive UTC hour: (speed m/s, dir deg FROM | None, gust m/s | None, label)}
  point(lat, lon)     -> the same for any point inside a fine grid ({} outside)
  point_cells(lat, lon) -> {hour: {field: value}, ...} (every field, for the land-weather RRFS tab)
  newest()            -> the newest source's cycle name (for the tables' cache keys) or None
  health()            -> counters for /healthz
"""
import hashlib
import json
import struct
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta

# numpy and rrfs_grid load on the first POINT read only: importing the app must not load numpy (the web process pays
# its ~25 MB only when somebody asks for a point; tests/test_point_forecast.py pins it). The stations need neither.

PREFIX = "rrfs/v1"
USE = {"cycle": 3, "run": 2}               # sources a table uses (the job keeps cell files for exactly these)
POINTER_TTL_S = 120
POINTER_FAIL_TTL_S = 60
SOURCE_FAIL_TTL_S = 60
POINTER_MAX = 256 * 1024
INDEX_MAX = 256 * 1024
STATIONS_MAX = 2 * 1024 * 1024
MISSING = 65535
MISSING8 = 255
CELL_FIELDS = [("spd", "<u2"), ("dir", "<u2"), ("gust", "<u2"), ("tmp", "<u2"), ("dpt", "<u2"), ("apcp", "<u2"),
               ("cloud", "u1"), ("type", "u1")]
CELL_BYTES = 14


def _np():
    import numpy
    return numpy


def _grid():
    import rrfs_grid
    return rrfs_grid
LRU_INDEXES = 32
LRU_STATIONS = 8
LRU_CELLS = 1024


def label(cycle):
    """The table's name of a source: 'RRFS 13Z'."""
    return f"RRFS {cycle[8:10]}Z"


def cycle_dt(cycle):
    return datetime.strptime(cycle, "%Y%m%d%H")


class _Lru:
    def __init__(self, n):
        self.n, self.d = n, OrderedDict()

    def get(self, k):
        v = self.d.get(k)
        if v is not None:
            self.d.move_to_end(k)
        return v

    def put(self, k, v):
        self.d[k] = v
        self.d.move_to_end(k)
        while len(self.d) > self.n:
            self.d.popitem(last=False)


class RrfsWinds:
    def __init__(self, root, get_bytes, get_range, clock=time.monotonic):
        """root: () -> the public bucket's base URL ('' = none configured: everything answers empty)."""
        self._root, self._get, self._range, self._clock = root, get_bytes, get_range, clock
        self._lock = threading.Lock()
        self._pointer = None                       # (expires, doc or None)
        self._indexes = _Lru(LRU_INDEXES)          # key -> doc
        self._stations = _Lru(LRU_STATIONS)        # cycle -> (doc, bytes)
        self._cells = _Lru(LRU_CELLS)              # (cycle, grid, j, i) -> structured array
        self._failed = {}                          # key -> retry-after clock
        self.stats = {"pointer_ok": 0, "pointer_fail": 0, "reads_ok": 0, "reads_fail": 0, "last_error": None}

    # ------------------------------------------------------------------------------------------ fetching + checks

    def _url(self, key):
        root = (self._root() or "").rstrip("/")
        return f"{root}/{key}" if root else None

    def _fail(self, key, exc):
        with self._lock:
            self._failed[key] = self._clock() + SOURCE_FAIL_TTL_S
            self.stats["reads_fail"] += 1
            self.stats["last_error"] = f"{key}: {exc.__class__.__name__}: {str(exc)[:160]}"

    def _failing(self, key):
        with self._lock:
            t = self._failed.get(key)
            return t is not None and self._clock() < t

    def pointer(self):
        """The pointer document (cached POINTER_TTL_S; a failure remembered POINTER_FAIL_TTL_S), or None."""
        with self._lock:
            if self._pointer and self._clock() < self._pointer[0]:
                return self._pointer[1]
        url = self._url(f"{PREFIX}/latest.json")
        doc = None
        if url:
            try:
                doc = json.loads(self._get(url, POINTER_MAX))
                if doc.get("format") != "rrfs-pointer-v1" or not isinstance(doc.get("sources"), list):
                    raise ValueError("not an RRFS pointer")
                for s in doc["sources"]:
                    if not (isinstance(s.get("cycle"), str) and len(s["cycle"]) == 10 and s["cycle"].isdigit()
                            and s.get("kind") in ("run", "cycle")):
                        raise ValueError(f"bad source {str(s)[:80]}")
            except Exception as exc:                 # noqa: BLE001 - the tables fall back to GFS
                doc = None
                with self._lock:
                    self.stats["pointer_fail"] += 1
                    self.stats["last_error"] = f"pointer: {exc.__class__.__name__}: {str(exc)[:160]}"
            else:
                with self._lock:
                    self.stats["pointer_ok"] += 1
        with self._lock:
            self._pointer = (self._clock() + (POINTER_TTL_S if doc else POINTER_FAIL_TTL_S), doc)
        return doc

    def sources(self, need_cells=False):
        """The sources a table uses, newest first: the newest USE[kind] of each kind (with cells when asked)."""
        doc = self.pointer()
        if not doc:
            return []
        seen = {"run": 0, "cycle": 0}
        out = []
        for s in sorted(doc["sources"], key=lambda s: s["cycle"], reverse=True):
            if need_cells and not s.get("cells"):
                continue
            seen[s["kind"]] += 1
            if seen[s["kind"]] <= USE[s["kind"]]:
                out.append(s)
        return out

    def newest(self):
        srcs = self.sources()
        return srcs[0]["cycle"] if srcs else None

    def _index(self, key, check):
        with self._lock:
            hit = self._indexes.get(key)
        if hit is not None:
            return hit
        doc = json.loads(self._get(self._url(key), INDEX_MAX))
        check(doc)
        with self._lock:
            self._indexes.put(key, doc)
        return doc

    # ------------------------------------------------------------------------------------------------ stations

    def _station_table(self, src):
        cyc = src["cycle"]
        with self._lock:
            hit = self._stations.get(cyc)
        if hit is not None:
            return hit

        def check(doc):
            if doc.get("format") != "rrfs-stations-v1" or doc.get("cycle") != cyc:
                raise ValueError(f"stations index of {cyc}: {doc.get('format')} {doc.get('cycle')}")
            if doc.get("record_bytes") != 6 * len(doc.get("steps") or []):
                raise ValueError(f"stations index of {cyc}: record_bytes {doc.get('record_bytes')}")

        doc = self._index(src["stations"], check)
        body = self._get(self._url(src["stations"][:-len(".json")] + ".bin"), STATIONS_MAX)
        if len(body) != doc["count"] * doc["record_bytes"] or hashlib.sha256(body).hexdigest() != doc.get("sha256"):
            raise ValueError(f"stations.bin of {cyc}: {len(body)} B, sha256 mismatch or wrong length")
        with self._lock:
            self._stations.put(cyc, (doc, body))
        return doc, body

    def station(self, sid):
        """{UTC hour: (speed, dir | None, gust | None, label)}, newest source first per hour; {} when none."""
        out = {}
        for src in self.sources():
            key = ("stations", src["cycle"])
            if self._failing(key):
                continue
            try:
                doc, body = self._station_table(src)
            except Exception as exc:                 # noqa: BLE001
                self._fail(key, exc)
                continue
            with self._lock:
                self.stats["reads_ok"] += 1
            ent = doc["stations"].get(sid)
            if not ent:
                continue
            slot, steps, rb = ent[0], doc["steps"], doc["record_bytes"]
            flat = struct.unpack(f"<{3 * len(steps)}H", body[slot * rb:(slot + 1) * rb])
            base = cycle_dt(src["cycle"])
            lab = label(src["cycle"])
            for h, spd, d, gu in zip(steps, flat[0::3], flat[1::3], flat[2::3]):
                if spd == MISSING:
                    continue
                t = base + timedelta(hours=int(h))
                if t not in out:
                    out[t] = (spd / 100, None if d == MISSING else int(round(d / 10)) % 360,
                              None if gu == MISSING else gu / 100, lab)
        return out

    # --------------------------------------------------------------------------------------------------- points

    def _cell_series(self, src, grid, j, i):
        cyc = src["cycle"]
        ck = (cyc, grid, j, i)
        with self._lock:
            hit = self._cells.get(ck)
        if hit is not None:
            return hit
        key = src["cells"][grid]
        g = _grid().GRIDS[grid]

        def check(doc):
            ok = (doc.get("format") == "rrfs-cells-v1" and doc.get("cycle") == cyc and doc.get("grid") == grid
                  and doc.get("nx") == g["Nx"] and doc.get("ny") == g["Ny"]
                  and doc.get("record_bytes") == CELL_BYTES * len(doc.get("steps") or [])
                  and doc.get("bytes") == doc["record_bytes"] * g["Nx"] * g["Ny"])
            if not ok:
                raise ValueError(f"cells index {key} does not match grid {grid}")

        doc = self._index(key, check)
        rb = doc["record_bytes"]
        off = (j * g["Nx"] + i) * rb
        body = self._range(self._url(key[:-len(".json")] + ".bin"), off, off + rb - 1)
        if len(body) != rb:
            raise ValueError(f"{len(body)} of {rb} bytes")
        np = _np()
        arr = np.frombuffer(body, dtype=np.dtype(CELL_FIELDS)).copy()
        series = (doc["steps"], arr)
        with self._lock:
            self._cells.put(ck, series)
        return series

    def point_cells(self, lat, lon):
        """{UTC hour: dict of every field (decoded units) + 'label'}, newest source first per hour; {} outside."""
        hit = _grid().locate(lat, lon)
        if hit is None:
            return {}
        grid, j, i = hit
        out = {}
        for src in self.sources(need_cells=True):
            if grid not in (src.get("cells") or {}):
                continue
            key = ("cells", src["cycle"], grid)
            if self._failing(key):
                continue
            try:
                steps, arr = self._cell_series(src, grid, j, i)
            except Exception as exc:                 # noqa: BLE001
                self._fail(key, exc)
                continue
            with self._lock:
                self.stats["reads_ok"] += 1
            base = cycle_dt(src["cycle"])
            lab = label(src["cycle"])
            for h, r in zip(steps, arr):
                if r["spd"] == MISSING:
                    continue
                t = base + timedelta(hours=int(h))
                if t in out:
                    continue
                u = lambda v, s: None if v == MISSING else v / s       # noqa: E731
                out[t] = {"spd": r["spd"] / 100, "dir": None if r["dir"] == MISSING else int(round(r["dir"] / 10)) % 360,
                          "gust": u(r["gust"], 100), "tmp_k": u(r["tmp"], 10), "dpt_k": u(r["dpt"], 10),
                          "apcp_mm": u(r["apcp"], 100), "cloud": None if r["cloud"] == MISSING8 else int(r["cloud"]),
                          "type": None if r["type"] == MISSING8 else int(r["type"]), "label": lab,
                          "kind": src["kind"], "cycle": src["cycle"]}
        return out

    def point(self, lat, lon):
        """{UTC hour: (speed, dir | None, gust | None, label)} at a point inside a fine grid; {} outside."""
        return {t: (v["spd"], v["dir"], v["gust"], v["label"]) for t, v in self.point_cells(lat, lon).items()}

    def health(self):
        with self._lock:
            ptr = self._pointer[1] if self._pointer else None
            return dict(self.stats, newest=(max((s["cycle"] for s in ptr["sources"]), default=None) if ptr else None),
                        pointer_published=(ptr or {}).get("published_utc"), cached_cells=len(self._cells.d),
                        cached_stations=len(self._stations.d))


def merge(gfs, rrfs):
    """The table's wind map: GFS's hours as (speed, dir, None, None), RRFS's hours over them."""
    out = {t: (v[0], v[1], None, None) for t, v in (gfs or {}).items()}
    out.update(rrfs or {})
    return out
