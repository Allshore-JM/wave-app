"""NOAA RRFS wind and weather for the forecast tables, forecast points and the land-weather tab (plan section 99).

RRFS (operational since 2026-11-03) runs every hour. The four 84-h runs a day (00/06/12/18Z) post hourly files
f000-f084 (`rrfs.tHHz.2dfld.<res>.fNNN.<grid>.grib2`); the other 20 cycles post only 15-min "subh" files f001-f018
(`rrfs.tHHz.2dfld.<res>.subh.fNNN.<grid>.grib2`), of which this job reads the whole-hour records. Every source is
published as soon as its last file lands (each tick builds at most the newest cycle and the newest run), so the site
always has the newest hours. NOAA's AWS copy (noaa-rrfs-ops-pds), byte ranges of the records named by each file's .idx.

Published under rrfs/v1/ (everything immutable but the pointer, written last):

  <CYCLE>/stations.bin    per station: steps x (speed 0.01 m/s, direction 0.1 deg FROM true, gust 0.01 m/s) uint16 LE;
                          65535 = missing
  <CYCLE>/stations.json   {"format": "rrfs-stations-v1", "cycle", "kind", "steps", "stations": {sid: [slot, grid, j,
                          i]}, "record_bytes", "missing", "sha256", ...}
  <CYCLE>/<grid>.bin      CELL-MAJOR series of every cell of the grid (cell = j * Nx + i, row 0 south): per cell
                          `steps` records of CELL_DTYPE (14 B), so the site reads one cell with ONE ranged GET
  <CYCLE>/<grid>.json     {"format": "rrfs-cells-v1", "grid", "cycle", "kind", "steps", "nx", "ny", "record_bytes",
                          "fields", "sha256", ...}
  latest.json             {"format": "rrfs-pointer-v1", "published_utc", "sources": [newest first: {"cycle", "kind",
                          "steps", "stations", "cells": {grid: json key} or null}]}

Exit codes (the frames jobs' contract): 0 published or nothing new; 3 nothing complete yet / not served / budget (the
next tick retries); 1 a failure (failed/<CYCLE>.json, retried after FAILED_RETRY_AFTER_S, at most FAILED_MAX_ATTEMPTS)
or a stale alarm.

  python tools/model_frames/rrfs.py                       # the newest complete cycle and run, if not live yet
  python tools/model_frames/rrfs.py --cycle 2026111713    # that cycle (a run when its hour is 00/06/12/18)
  python tools/model_frames/rrfs.py --dry-run --out DIR   # build, publish nothing
"""
import argparse
import hashlib
import json
import os
import shutil
import struct
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)
import publish as P  # noqa: E402
import rrfs_grid as G  # noqa: E402

S3 = "https://noaa-rrfs-ops-pds.s3.amazonaws.com"
PREFIX = P.RRFS_PREFIX
RES = {"hi": "2p5km", "pr": "2p5km", "conus": "3km", "ak": "3km"}
RUN_HOURS = (0, 6, 12, 18)
RUN_STEPS = list(range(0, 85))
CYCLE_STEPS = list(range(1, 19))
RUN_LOOKBACK = 4                     # synoptic cycles looked at
CYCLE_LOOKBACK = 8                   # hourly cycles looked at
KEEP_STATIONS = {"run": 2, "cycle": 8}
KEEP_CELLS = {"run": 2, "cycle": 3}
MISSING = 65535
MISSING8 = 255
BUILD_BUDGET_S = 25 * 60
RUN_MIN_LEFT_S = 15 * 60             # an 84-h run is started in a tick only with this much of the budget left (G66a P2-1)
STALE_H = {"cycle": 8, "run": 24}    # hours after its cycle time the newest source of a kind may be (G66a P1-1): an hourly
                                     # cycle lands ~2.5 h after its hour, a run ~3.7 h; so 8 h = ~5 cycles missed and
                                     # 24 h = 3 runs missed
NOTREADY_WARN = 6                    # NotReady / budget exits in a row for one cycle before the run fails loud (run.py's rule)
FAILED_RETRY_AFTER_S = 1800
FAILED_MAX_ATTEMPTS = 6
WORKERS = 8
STALL_S = 60
PART_BYTES = 100 * 1024 * 1024
UA = {"User-Agent": "allshoresurf.com RRFS job (https://allshoresurf.com)"}

CELL_DTYPE = np.dtype([("spd", "<u2"), ("dir", "<u2"), ("gust", "<u2"), ("tmp", "<u2"), ("dpt", "<u2"),
                       ("apcp", "<u2"), ("cloud", "u1"), ("type", "u1")])
assert CELL_DTYPE.itemsize == 14
FIELDS = {"spd": "0.01 m/s", "dir": "0.1 deg FROM, true north", "gust": "0.01 m/s", "tmp": "0.1 K", "dpt": "0.1 K",
          "apcp": "0.01 mm in the hour ending at the step", "cloud": "% (255 = none: hourly cycles have no cloud)",
          "type": "0 none (< 0.1 mm), 1 rain, 2 snow, 3 mix, 4 ice, 5 thunder; 255 = unknown"}
TYPE_NONE, TYPE_RAIN, TYPE_SNOW, TYPE_MIX, TYPE_ICE, TYPE_THUNDER = 0, 1, 2, 3, 4, 5


class NotReady(Exception):
    """NOAA does not serve a file the discovery found: exit 3, the next tick retries."""


class TransportError(Exception):
    """A read failed after its tries: a failure record."""


class BudgetExceeded(Exception):
    """The build ran past BUILD_BUDGET_S: exit 3, the next tick tries again."""


class RrfsSource:
    """What run._stale_check needs of this job: the pointer of the newest source (an hourly cycle every hour)."""
    name = "NOAA"
    tag = "RRFS "
    prefix = PREFIX
    stale_fail_h = 6


SRC = RrfsSource()


# ----------------------------------------------------------------------------------------------------- names and keys

def is_run(cycle_dt):
    return cycle_dt.hour in RUN_HOURS


def kind_of(cycle_dt):
    return "run" if is_run(cycle_dt) else "cycle"


def steps_of(cycle_dt):
    return RUN_STEPS if is_run(cycle_dt) else CYCLE_STEPS


def cycle_name(cycle_dt):
    return f"{cycle_dt:%Y%m%d%H}"


def file_key(cycle_dt, grid, step):
    """The S3 key of one step's file (subh for an hourly cycle)."""
    sub = "" if is_run(cycle_dt) else "subh."
    return (f"rrfs.{cycle_dt:%Y%m%d}/{cycle_dt:%H}/rrfs.t{cycle_dt:%H}z.2dfld.{RES[grid]}.{sub}f{step:03d}."
            f"{grid}.grib2")


def stations_key(cycle):
    return f"{PREFIX}/{cycle}/stations.bin"


def stations_index_key(cycle):
    return f"{PREFIX}/{cycle}/stations.json"


def cells_key(cycle, grid):
    return f"{PREFIX}/{cycle}/{grid}.bin"


def cells_index_key(cycle, grid):
    return f"{PREFIX}/{cycle}/{grid}.json"


# -------------------------------------------------------------------------------------------------------- the records

def wanted(cycle_dt, step):
    """[(name, idx match)] of the records a step needs: the match is (variable, level, forecast label) as the .idx
    writes them. f000 of a run is labelled "anl" and has no hourly precipitation or lightning."""
    lab = "anl" if step == 0 else f"{step} hour fcst"
    out = [("u", ("UGRD", "10 m above ground", lab)), ("v", ("VGRD", "10 m above ground", lab)),
           ("gust", ("GUST", "surface", lab)), ("tmp", ("TMP", "2 m above ground", lab)),
           ("dpt", ("DPT", "2 m above ground", lab)), ("crain", ("CRAIN", "surface", lab)),
           ("csnow", ("CSNOW", "surface", lab)), ("cfrzr", ("CFRZR", "surface", lab)),
           ("cicep", ("CICEP", "surface", lab))]
    if is_run(cycle_dt):
        out.append(("tcdc", ("TCDC", "entire atmosphere (considered as a single layer)", lab)))
        if step > 0:
            out.append(("apcp", ("APCP", "surface", f"{step - 1}-{step} hour acc fcst")))
            out.append(("ltng", ("LTNG", "entire atmosphere", f"{step - 1}-{step} hour max fcst")))
    else:
        out.append(("apcp_total", ("APCP", "surface", f"0-{step} hour acc fcst")))
    return out


def parse_idx(text, size=None):
    """[(n, offset, end_inclusive, d, var, level, label)] of an .idx (CRLF or LF; blank lines skipped). The last
    record's end is None unless the file size is given."""
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        f = line.split(":")
        if len(f) < 6:
            raise ValueError(f"idx line not understood: {line[:80]!r}")
        rows.append([int(f[0]), int(f[1]), None, f[2], f[3], f[4], f[5]])
    for k, r in enumerate(rows):
        nxt = rows[k + 1][1] if k + 1 < len(rows) else size
        r[2] = None if nxt is None else nxt - 1
        if r[2] is not None and r[2] < r[1]:
            raise ValueError(f"idx record {r[0]} has no bytes")
    return [tuple(r) for r in rows]


def select_records(rows, cycle_dt, step):
    """{name: (offset, end)} of the wanted records; ValueError when one is missing, twice, or of another cycle."""
    d = f"d={cycle_name(cycle_dt)}"
    out = {}
    for name, (var, level, lab) in wanted(cycle_dt, step):
        hits = [r for r in rows if r[4] == var and r[5] == level and r[6] == lab]
        if len(hits) != 1:
            raise ValueError(f"{var}:{level}:{lab}: {len(hits)} records in the idx of {cycle_name(cycle_dt)} f{step:03d}")
        if hits[0][3] != d:
            raise ValueError(f"{var}:{level}:{lab}: {hits[0][3]} in the idx of {cycle_name(cycle_dt)}")
        out[name] = (hits[0][1], hits[0][2])
    return out


def ranges_of(records):
    """Merge byte-adjacent records into as few ranges as possible: [(start, end, [names in order])]."""
    out = []
    for name, (a, b) in sorted(records.items(), key=lambda kv: kv[1][0]):
        if out and out[-1][1] + 1 == a:
            out[-1] = (out[-1][0], b, out[-1][2] + [name])
        else:
            out.append((a, b, [name]))
    return out


def split_grib(buf, names):
    """The GRIB2 messages of one range, in order, by their section-0 lengths (a cut or extra byte fails)."""
    out, pos = {}, 0
    for name in names:
        if buf[pos:pos + 4] != b"GRIB" or len(buf) < pos + 16:
            raise ValueError(f"{name}: no GRIB message at {pos}")
        n = struct.unpack(">Q", buf[pos + 8:pos + 16])[0]
        if buf[pos + n - 4:pos + n] != b"7777":
            raise ValueError(f"{name}: message of {n} B has no 7777 end")
        out[name] = buf[pos:pos + n]
        pos += n
    if pos != len(buf):
        raise ValueError(f"{len(buf) - pos} bytes after the last message")
    return out


# ------------------------------------------------------------------------------------------------------------ network

def http_get(url, rng=None, tries=4, opener=urllib.request.urlopen):
    """Bytes of url (a range: exactly that many bytes with HTTP 206). 403/404 -> NotReady; others retried, then
    TransportError."""
    h = dict(UA)
    if rng:
        h["Range"] = f"bytes={rng[0]}-{rng[1]}"
    last = None
    for attempt in range(tries):
        try:
            with opener(urllib.request.Request(url, headers=h), timeout=STALL_S) as r:
                body = r.read()
                status = getattr(r, "status", 200)
            if rng and (status != 206 or len(body) != rng[1] - rng[0] + 1):
                raise TransportError(f"{url}: range {rng} answered {status} with {len(body)} B")
            return body
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                raise NotReady(f"{e.code} {url}") from None
            last = f"HTTP {e.code}"
            wait = 5 * (attempt + 1) if e.code in (429, 503) else 2 * (attempt + 1)
        except TransportError:
            raise
        except Exception as e:                       # noqa: BLE001 - timeouts, resets
            last = repr(e)
            wait = 2 * (attempt + 1)
        if attempt + 1 < tries:
            time.sleep(wait)
    raise TransportError(f"GET {url}: {last} after {tries} tries")


def head_ok(url, opener=urllib.request.urlopen):
    """True when the object is there (200), False on 403/404; TransportError otherwise after 3 tries."""
    last = None
    for attempt in range(3):
        try:
            with opener(urllib.request.Request(url, headers=UA, method="HEAD"), timeout=30):
                return True
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return False
            last = f"HTTP {e.code}"
        except Exception as e:                       # noqa: BLE001
            last = repr(e)
        if attempt < 2:
            time.sleep(2 * (attempt + 1))
    raise TransportError(f"HEAD {url}: {last}")


def content_length(url, opener=urllib.request.urlopen, tries=3):
    """An object's size (HEAD); NotReady on 403/404; retried, then TransportError."""
    last = None
    for attempt in range(tries):
        try:
            with opener(urllib.request.Request(url, headers=UA, method="HEAD"), timeout=30) as r:
                return int(r.headers["Content-Length"])
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                raise NotReady(f"{e.code} {url}") from None
            last = f"HTTP {e.code}"
        except Exception as e:                       # noqa: BLE001
            last = repr(e)
        if attempt + 1 < tries:
            time.sleep(2 * (attempt + 1))
    raise TransportError(f"HEAD {url}: {last}")


def complete(cycle_dt, head=head_ok):
    """A source is complete when the .idx of its LAST step exists on all four grids (S3 objects appear whole; the
    files of a cycle land in step order)."""
    last = steps_of(cycle_dt)[-1]
    return all(head(f"{S3}/{file_key(cycle_dt, g, last)}.idx") for g in G.ORDER)


def newest_complete(now, kind, head=head_ok):
    """The newest complete cycle of `kind` ("run" / "cycle") in its lookback, or None."""
    now = now.replace(minute=0, second=0, microsecond=0)
    n = 0
    for back in range(0, 48):
        c = now - timedelta(hours=back)
        if kind_of(c) != kind:
            continue
        n += 1
        if n > (RUN_LOOKBACK if kind == "run" else CYCLE_LOOKBACK):
            return None
        if complete(c, head):
            return c
    return None


# ------------------------------------------------------------------------------------------------------------ decoding

# What each record must say about itself (eccodes keys, steps in hours), so a .idx that does not match its GRIB file
# can never put one field in another's place (G66a P2-2): name -> (shortName, level, span) where span "hour" = over the
# hour ending at the step, "total" = from the cycle's start, None = instantaneous at the step.
IDENTITY = {"u": ("10u", 10, None), "v": ("10v", 10, None), "gust": ("gust", 0, None), "tmp": ("2t", 2, None),
            "dpt": ("2d", 2, None), "crain": ("crain", 0, None), "csnow": ("csnow", 0, None),
            "cfrzr": ("cfrzr", 0, None), "cicep": ("cicep", 0, None), "tcdc": ("tcc", 0, None),
            "apcp": ("tp", 0, "hour"), "ltng": ("ltng", 0, "hour"), "apcp_total": ("tp", 0, "total")}


def check_identity(g, ec, name, cycle_dt, step):
    """Raise ValueError unless the open message g is field `name` of cycle_dt at `step` (hours)."""
    short, level, span = IDENTITY[name]
    ec.codes_set(g, "stepUnits", "h")
    got = (ec.codes_get(g, "shortName"), ec.codes_get(g, "level"), ec.codes_get(g, "startStep"),
           ec.codes_get(g, "endStep"), ec.codes_get(g, "dataDate"), ec.codes_get(g, "dataTime"))
    start = step - 1 if span == "hour" else 0 if span == "total" else step
    want = (short, level, start, step, int(f"{cycle_dt:%Y%m%d}"), cycle_dt.hour * 100)
    if got != want:
        raise ValueError(f"record {name} of {cycle_name(cycle_dt)} f{step:03d} is {got}, not {want}")


def decode(msg, grid, expect=None):
    """values (ny, nx) float64, NaN for missing, of one GRIB message, its geometry checked against the pin and, with
    expect = (name, cycle_dt, step), its identity (field, level, step range, cycle)."""
    import eccodes as ec
    g = ec.codes_new_from_message(msg)
    try:
        if expect is not None:
            check_identity(g, ec, *expect)
        meta = {}
        for k in ("gridType", "Nx", "Ny", "latitudeOfFirstGridPointInDegrees", "longitudeOfFirstGridPointInDegrees",
                  "LaDInDegrees", "LoVInDegrees", "Latin1InDegrees", "Latin2InDegrees", "DxInMetres", "DyInMetres",
                  "scanningMode", "shapeOfTheEarth", "orientationOfTheGridInDegrees"):
            try:
                meta[k] = ec.codes_get(g, k)
            except Exception:                        # noqa: BLE001 - a key the grid type does not have
                pass
        G.check_geometry(grid, meta)
        vals = ec.codes_get_double_array(g, "values")
        if ec.codes_get(g, "bitmapPresent"):
            miss = ec.codes_get(g, "missingValue")
            vals = np.where(vals == miss, np.nan, vals)
        return vals.reshape(meta["Ny"], meta["Nx"])
    finally:
        ec.codes_release(g)


def grid_lonlat(msg, grid):
    """eccodes' latitude / longitude arrays of a grid (once per build), self-checked against rrfs_grid."""
    import eccodes as ec
    g = ec.codes_new_from_message(msg)
    try:
        ny, nx = ec.codes_get(g, "Ny"), ec.codes_get(g, "Nx")
        la = ec.codes_get_double_array(g, "latitudes").reshape(ny, nx)
        lo = ec.codes_get_double_array(g, "longitudes").reshape(ny, nx)
    finally:
        ec.codes_release(g)
    G.self_check(grid, la, lo)
    return la, lo


def _u16(x, scale, lo=0.0):
    """Scaled, rounded, missing (NaN / out of range) = 65535."""
    y = np.round((np.asarray(x, dtype=float) - lo) * scale)
    bad = ~np.isfinite(y) | (y < 0) | (y >= MISSING)
    return np.where(bad, MISSING, y).astype("<u2")


def precip_type(apcp, crain, csnow, cfrzr, cicep, ltng=None):
    """Per cell: 0 under 0.1 mm, else thunder (lightning > 0) > ice (freezing rain / ice pellets) > mix (rain and
    snow) > snow > rain (plan section 99, the section-91 order). 255 where the hour's amount is unknown."""
    apcp = np.asarray(apcp, dtype=float)
    t = np.full(apcp.shape, TYPE_RAIN, dtype="u1")
    rain, snow = crain >= 0.5, csnow >= 0.5
    t[snow] = TYPE_SNOW
    t[rain & snow] = TYPE_MIX
    t[(cfrzr >= 0.5) | (cicep >= 0.5)] = TYPE_ICE
    if ltng is not None:
        t[np.nan_to_num(ltng) > 0] = TYPE_THUNDER
    t[apcp < 0.1] = TYPE_NONE
    t[~np.isfinite(apcp)] = MISSING8
    return t


def step_record(f, grid, lon, prev_total=None):
    """(CELL_DTYPE array (ny*nx,), the APCP run total for the next hourly-cycle step) of one step's decoded fields."""
    ue, ve = G.rotate(grid, f["u"], f["v"], lon)
    spd, dirn = G.speed_dir(ue, ve)
    rec = np.empty(spd.size, dtype=CELL_DTYPE)
    rec["spd"] = _u16(spd, 100).ravel()
    rec["dir"] = _u16(np.where(np.round(dirn * 10) >= 3600, 0.0, dirn), 10).ravel()
    rec["dir"][rec["spd"] == 0] = MISSING            # a calm has no direction (atan2(-0, -0) would say 180)
    rec["gust"] = _u16(f["gust"], 100).ravel()
    rec["tmp"] = _u16(f["tmp"], 10).ravel()
    rec["dpt"] = _u16(f["dpt"], 10).ravel()
    total = None
    if "apcp" in f:
        apcp = f["apcp"]
    elif "apcp_total" in f:
        total = f["apcp_total"]
        apcp = np.maximum(total - (prev_total if prev_total is not None else 0.0), 0.0)
    else:
        apcp = np.full(spd.shape, np.nan)            # f000 of a run: no hour behind it
    rec["apcp"] = _u16(apcp, 100).ravel()
    if "tcdc" in f:
        c = np.round(f["tcdc"])
        rec["cloud"] = np.where(np.isfinite(c) & (c >= 0) & (c <= 100), c, MISSING8).astype("u1").ravel()
    else:
        rec["cloud"] = MISSING8
    rec["type"] = precip_type(apcp, f["crain"], f["csnow"], f["cfrzr"], f["cicep"], f.get("ltng")).ravel()
    return rec, total


# ------------------------------------------------------------------------------------------------------------- build

def fetch_step(cycle_dt, grid, step, get=http_get, size_of=None):
    """{name: GRIB message bytes} of one step of one grid (the .idx, then one ranged read per adjacent group). The file
    size is asked (HEAD) only when a wanted record is the file's last."""
    url = f"{S3}/{file_key(cycle_dt, grid, step)}"
    idx = get(url + ".idx").decode("ascii", "replace")
    rows = parse_idx(idx)
    recs = select_records(rows, cycle_dt, step)
    if any(b is None for _, b in recs.values()):
        size = (size_of or content_length)(url)
        rows = parse_idx(idx, size)
        recs = select_records(rows, cycle_dt, step)
    out = {}
    for a, b, names in ranges_of(recs):
        out.update(split_grib(get(url, (a, b)), names))
    return out


def build_grid(cycle_dt, grid, stations, work, deadline, get=http_get, log=print, workers=WORKERS):
    """Fetch and decode every step of one grid; write the cell-major series to work/<grid>.bin. Returns (path,
    {sid: [steps x (spd, dir, gust)]}) for the stations on this grid.

    The series is built in RAM (CONUS run 2.3 GB; one grid at a time on a 16 GB runner) and written once: a file-backed
    map would dirty every page of the file at every step (14 B at a 1,190-B stride), G66a P1-2. Fetches run ahead of
    the decoding in a window of 2 x workers steps; decoding (eccodes) stays on this thread, in step order (hourly
    cycles difference their precipitation totals)."""
    steps = steps_of(cycle_dt)
    gpin = G.GRIDS[grid]
    ny, nx = gpin["Ny"], gpin["Nx"]
    path = os.path.join(work, f"{grid}.bin")
    arr = np.empty((ny * nx, len(steps)), dtype=CELL_DTYPE)
    t0 = time.monotonic()

    def one(step):
        if time.monotonic() > deadline:
            raise BudgetExceeded("the build budget ran out")
        return fetch_step(cycle_dt, grid, step, get)

    lon, total, waits = None, None, 0.0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        ahead = deque()
        pending = iter(steps)

        def fill():
            while len(ahead) < 2 * workers:
                s = next(pending, None)
                if s is None:
                    return
                ahead.append((s, ex.submit(one, s)))

        fill()
        try:
            for k, step in enumerate(steps):
                s, fut = ahead.popleft()
                if s != step:
                    raise RuntimeError(f"step order {s} != {step}")
                w = time.monotonic()
                m = fut.result()
                waits += time.monotonic() - w
                fill()
                if lon is None:
                    lon = grid_lonlat(m["u"], grid)[1]
                fields = {name: decode(b, grid, (name, cycle_dt, step)) for name, b in m.items()}
                rec, total = step_record(fields, grid, lon, total)
                arr[:, k] = rec
        finally:
            for _, fut in ahead:
                fut.cancel()
    with open(path, "wb") as fh:
        arr.tofile(fh)
    st = {sid: np.array(arr[j * nx + i][["spd", "dir", "gust"]]) for sid, (j, i) in stations.items()}
    del arr
    log(f"{cycle_name(cycle_dt)} {grid}: {len(steps)} steps, {os.path.getsize(path)} B, {len(stations)} stations, "
        f"{time.monotonic() - t0:.0f} s ({waits:.0f} s waiting for downloads)")
    return path, st


def station_cells(stations_coords):
    """{grid: {sid: (j, i)}} of the stations inside a fine grid."""
    out = {g: {} for g in G.ORDER}
    for sid, (lat, lon) in sorted(stations_coords.items()):
        hit = G.locate(lat, lon)
        if hit:
            out[hit[0]][sid] = (hit[1], hit[2])
    return out


def load_stations(root=ROOT):
    """{sid: (lat, lon)}: the listed stations (station_list.json + station_coords.json) and the NWPS stations."""
    with open(os.path.join(root, "station_list.json"), encoding="utf-8-sig") as f:
        listed = json.load(f)
    with open(os.path.join(root, "station_coords.json"), encoding="utf-8-sig") as f:
        coords = json.load(f)
    with open(os.path.join(root, "nwps_stations.json"), encoding="utf-8-sig") as f:
        nwps = json.load(f)["stations"]
    out = {}
    for sid in listed:
        c = coords.get(sid)
        if c:
            out[sid] = (float(c["lat"]), float(c["lon"]))
    for sid, c in nwps.items():
        out.setdefault(sid, (float(c["lat"]), float(c["lon"])))
    return out


def build(cycle_dt, work, stations_coords, deadline, get=http_get, log=print, grids=G.ORDER):
    """Build every product of one source into `work`: {grid}.bin + the documents. Returns (files, docs) where files =
    {key: local path} and docs = {key: dict} (the indexes, sha256 filled)."""
    cyc = cycle_name(cycle_dt)
    steps = steps_of(cycle_dt)
    where = station_cells(stations_coords)
    files, docs = {}, {}
    st_all = {}
    for grid in grids:
        path, st = build_grid(cycle_dt, grid, where[grid], work, deadline, get, log)
        files[cells_key(cyc, grid)] = path
        docs[cells_index_key(cyc, grid)] = {
            "format": "rrfs-cells-v1", "grid": grid, "cycle": cyc, "kind": kind_of(cycle_dt), "steps": steps,
            "nx": G.GRIDS[grid]["Nx"], "ny": G.GRIDS[grid]["Ny"], "record_bytes": CELL_DTYPE.itemsize * len(steps),
            "step_bytes": CELL_DTYPE.itemsize,
            "layout": [[n, CELL_DTYPE.fields[n][0].str] for n in CELL_DTYPE.names], "fields": FIELDS,
            "missing": MISSING, "missing8": MISSING8, "bytes": os.path.getsize(path), "sha256": _sha256(path),
            "built_utc": _now(), "source": f"NOAA RRFS {file_key(cycle_dt, grid, steps[0])} ..."}
        for sid, recs in st.items():
            st_all[sid] = (grid, where[grid][sid], recs)
    body = bytearray()
    index = {}
    for slot, sid in enumerate(sorted(st_all)):
        grid, (j, i), recs = st_all[sid]
        flat = np.empty(len(steps) * 3, dtype="<u2")
        flat[0::3], flat[1::3], flat[2::3] = recs["spd"], recs["dir"], recs["gust"]
        body += flat.tobytes()
        index[sid] = [slot, grid, int(j), int(i)]
    spath = os.path.join(work, "stations.bin")
    with open(spath, "wb") as fh:
        fh.write(body)
    files[stations_key(cyc)] = spath
    docs[stations_index_key(cyc)] = {
        "format": "rrfs-stations-v1", "cycle": cyc, "kind": kind_of(cycle_dt), "steps": steps, "stations": index,
        "count": len(index), "record_bytes": 6 * len(steps), "missing": MISSING,
        "units": {"speed": "0.01 m/s", "direction": "0.1 deg, FROM, true north", "gust": "0.01 m/s"},
        "sha256": hashlib.sha256(bytes(body)).hexdigest(), "built_utc": _now()}
    return files, docs


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ----------------------------------------------------------------------------------------------------------- publish

def put_file(store, key, path):
    """A large object by multipart upload (boto3's managed transfer); a store without a client (tests) gets put()."""
    client = getattr(store, "client", None)
    if client is not None and hasattr(client, "upload_file"):
        from boto3.s3.transfer import TransferConfig
        client.upload_file(path, store.bucket, key, ExtraArgs={"ContentType": "application/octet-stream",
                                                               "CacheControl": P.IMMUTABLE},
                           Config=TransferConfig(multipart_threshold=PART_BYTES, multipart_chunksize=PART_BYTES,
                                                 max_concurrency=4))
    else:
        with open(path, "rb") as f:
            store.put(key, f.read(), "application/octet-stream", P.IMMUTABLE)


def publish(store, cycle_dt, files, docs, latest):
    """The data objects, then the documents, then the pointer (last). Returns the new pointer."""
    for key, path in files.items():
        put_file(store, key, path)
    for key, doc in docs.items():
        store.put(key, json.dumps(doc, separators=(",", ":")).encode(), "application/json", P.IMMUTABLE)
    cyc = cycle_name(cycle_dt)
    entry = {"cycle": cyc, "kind": kind_of(cycle_dt), "steps": steps_of(cycle_dt),
             "stations": stations_index_key(cyc),
             "cells": {g: cells_index_key(cyc, g) for g in G.ORDER if cells_index_key(cyc, g) in docs} or None}
    sources = [s for s in (latest.get("sources") or []) if s.get("cycle") != cyc] + [entry]
    sources = keep_sources(sources)
    pointer = {"format": "rrfs-pointer-v1", "published_utc": _now(), "newest": newest(sources), "sources": sources}
    store.put(P.latest_key(PREFIX), json.dumps(pointer, sort_keys=True).encode(), "application/json", P.POINTER)
    return pointer


def newest(sources):
    return max((s["cycle"] for s in sources), default=None)


def keep_sources(sources):
    """Newest first; KEEP_STATIONS per kind kept, and their cells only for the newest KEEP_CELLS per kind."""
    sources = sorted(sources, key=lambda s: s["cycle"], reverse=True)
    out, seen = [], {"run": 0, "cycle": 0}
    for s in sources:
        k = s["kind"]
        seen[k] += 1
        if seen[k] > KEEP_STATIONS[k]:
            continue
        s = dict(s)
        if seen[k] > KEEP_CELLS[k]:
            s["cells"] = None
        out.append(s)
    return out


def prune(store, pointer, previous=None):
    """Delete every object under <PREFIX>/<10 digits>/ that neither the new pointer nor the one it replaced names
    (a site may still hold the previous pointer for a few minutes: G66a P2-3); a cycle newer than the pointer's newest
    is left alone (a later build may still be writing). Old failed/notready records go too."""
    named = set()
    for s in (pointer.get("sources") or []) + ((previous or {}).get("sources") or []):
        named.add(s["stations"])
        named.add(s["stations"].replace("stations.json", "stations.bin"))
        for key in (s.get("cells") or {}).values():
            named.add(key)
            named.add(key[:-len(".json")] + ".bin")
    top = pointer.get("newest") or ""
    dead = []
    for k in store.list_keys(PREFIX + "/"):
        name, _, rest = k[len(PREFIX) + 1:].partition("/")
        if not (name.isdigit() and len(name) == 10 and rest):
            continue
        if name > top or k in named:
            continue
        dead.append(k)
    if dead:
        store.delete_keys(dead, "old RRFS objects")
    oldest = min((s["cycle"] for s in pointer.get("sources", [])), default=None)
    if oldest:
        old = [k for d in ("failed", "notready") for k in store.list_keys(f"{PREFIX}/{d}/")
               if k.rsplit("/", 1)[-1][:10].isdigit() and k.rsplit("/", 1)[-1][:10] < oldest]
        if old:
            store.delete_keys(old, "old records")
    return dead


# --------------------------------------------------------------------------------------------------------------- main

def _skip_failed(store, cyc):
    rec = store.get_json(P.failed_key(cyc, PREFIX))
    if not rec:
        return False
    if rec.get("attempts", 0) >= FAILED_MAX_ATTEMPTS:
        return True
    last = datetime.strptime(rec["last_attempt_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).total_seconds() < FAILED_RETRY_AFTER_S


def _store_from_env():
    return P.Store(P.r2_client(os.environ["R2_ACCOUNT_ID"], os.environ["R2_ACCESS_KEY_ID"],
                               os.environ["R2_SECRET_ACCESS_KEY"]), os.environ["R2_BUCKET"])


def _summary(line):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as fh:
            fh.write(line + "\n")


def stale_kinds(latest, now):
    """[(kind, newest cycle or None, age h or None)] of the kinds whose newest live source is older than STALE_H[kind]
    hours after its cycle time (or missing). Every hourly cycle refreshes the pointer, so its publish time cannot tell a
    run that stopped (G66a P1-1)."""
    out = []
    for kind in ("cycle", "run"):
        cycles = [s["cycle"] for s in latest.get("sources") or [] if s.get("kind") == kind]
        if not cycles:
            out.append((kind, None, None))
            continue
        newest_c = max(cycles)
        age = (now - datetime.strptime(newest_c, "%Y%m%d%H").replace(tzinfo=timezone.utc)).total_seconds() / 3600
        if age >= STALE_H[kind]:
            out.append((kind, newest_c, age))
    return out


def _stale(store, latest, situation, now=None):
    """1 (the workflow fails, GitHub mails the owner) with a summary WARNING when a kind is stale, at most once every
    run.STALE_ALARM_EVERY_H hours (one record under the prefix, run.py's rule); else None."""
    import run as R
    now = now or datetime.now(timezone.utc)
    stale = stale_kinds(latest, now)
    if not stale:
        return None
    key = R.stale_alarm_key(PREFIX)
    rec = store.get_json(key) or {}
    try:
        last = datetime.strptime(rec["last_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if (now - last).total_seconds() < R.STALE_ALARM_EVERY_H * 3600:
            print(f"stale RRFS already reported at {rec['last_utc']}")
            return None
    except (KeyError, TypeError, ValueError):
        pass
    what = "; ".join(f"no {k} live" if c is None else f"newest {k} {c} is {a:.0f} h old (limit {STALE_H[k]} h)"
                     for k, c, a in stale)
    msg = f"{situation}: {what}: NOAA's RRFS files may have moved or stopped, or the build cannot finish"
    print(msg)
    _summary("WARNING: " + msg)
    store.put(key, json.dumps({"last_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "message": msg},
                              sort_keys=True).encode(), "application/json", P.POINTER)
    return 1


def todo(latest, now, head=head_ok):
    """The sources to build this tick, newest-kind first: the newest complete hourly cycle and the newest complete run,
    each only when newer than the newest live source of its kind."""
    live = {"run": "", "cycle": ""}
    for s in latest.get("sources") or []:
        live[s["kind"]] = max(live[s["kind"]], s["cycle"])
    out = []
    for kind in ("cycle", "run"):
        c = newest_complete(now, kind, head)
        if c is not None and cycle_name(c) > live[kind]:
            out.append(c)
    return out


def build_one(cycle_dt, store, latest, live, out_dir, t0, get, log=print, coords=None, grids=G.ORDER):
    """Build + publish one source. Returns (exit code, latest)."""
    cyc = cycle_name(cycle_dt)
    if live and _skip_failed(store, cyc):
        log(f"{cyc} failed recently: waiting before another try")
        return 3, latest
    work = tempfile.mkdtemp(prefix=f"rrfs-{cyc}-", dir=out_dir or None)
    try:
        files, docs = build(cycle_dt, work, coords if coords is not None else load_stations(),
                            deadline=t0 + BUILD_BUDGET_S, get=get, log=log, grids=grids)
        line = (f"RRFS {cyc} ({kind_of(cycle_dt)}): {len(steps_of(cycle_dt))} steps, "
                f"{sum(os.path.getsize(p) for p in files.values())} B, "
                f"{docs[stations_index_key(cyc)]['count']} stations, {time.monotonic() - t0:.0f} s")
        log(line)
        _summary(line)
        if not live:
            if out_dir:
                for key, doc in docs.items():
                    with open(os.path.join(work, key.rsplit("/", 1)[-1]), "w", encoding="utf-8") as f:
                        json.dump(doc, f)
                log(f"dry run: files kept in {work}")
                work = None
            return 0, latest
        previous = latest
        latest = publish(store, cycle_dt, files, docs, latest)
        deleted = P.safe_prune(lambda: prune(store, latest, previous), store.bucket, _summary)
        log(f"published {cyc}; pruned {len(deleted or [])} objects")
        return 0, latest
    except (NotReady, BudgetExceeded) as exc:
        log(f"{cyc}: {exc.__class__.__name__}: {exc}; the next tick tries again")
        if live:
            rec = P.record_notready(store, cyc, str(exc), PREFIX)
            if rec["count"] == NOTREADY_WARN:
                _summary(f"WARNING: RRFS {cyc} not built {rec['count']} times in a row: {exc}")
                return 1, latest
        return 3, latest
    except Exception as exc:                         # noqa: BLE001 - a layout change, a bad record, a stalled read
        if live and P.is_storage_busy(exc):
            log(f"{cyc}: storage busy ({exc}); the next tick tries again")
            return 3, latest
        log(f"{cyc}: build failed: {exc.__class__.__name__}: {exc}")
        if live:
            P.record_failure(store, cyc, f"{exc.__class__.__name__}: {exc}", PREFIX)
        _summary(f"FAILED: RRFS {cyc}: {exc.__class__.__name__}")
        return 1, latest
    finally:
        if work:
            shutil.rmtree(work, ignore_errors=True)


def main(argv=None, get=None, head=None, store=None, now=None, coords=None, log=print):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--cycle", help="YYYYMMDDHH (default: the newest complete hourly cycle and run)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--out", help="with --dry-run: keep the built files in a folder under this directory")
    ap.add_argument("--grids", help="with --dry-run: a comma list of grids (default all four)")
    a = ap.parse_args(argv)
    t0 = time.monotonic()
    get = get or http_get
    head = head or head_ok
    live = not a.dry_run
    if live and (a.out or a.grids):
        log("--out and --grids only with --dry-run")
        return 2
    grids = tuple(a.grids.split(",")) if a.grids else G.ORDER
    if any(g not in G.ORDER for g in grids):
        log(f"--grids: one of {G.ORDER}")
        return 2
    if live and store is None:
        store = _store_from_env()
    latest = (store.get_json(P.latest_key(PREFIX)) or {}) if live else {}
    try:
        if a.cycle:
            if len(a.cycle) != 10 or not a.cycle.isdigit():
                log(f"{a.cycle}: not YYYYMMDDHH")
                return 2
            c = datetime.strptime(a.cycle, "%Y%m%d%H").replace(tzinfo=timezone.utc)
            if not complete(c, head):
                log(f"{a.cycle}: not complete on NOAA's AWS copy")
                return 3
            if live and any(s["cycle"] == a.cycle for s in latest.get("sources") or []):
                log(f"{a.cycle} is already live: its objects are immutable, nothing rewritten")
                return 0
            plan = [c]
        else:
            plan = todo(latest, now or datetime.now(timezone.utc), head)
    except TransportError as exc:
        log(f"discovery failed: {exc}")
        return 1
    clock = now or datetime.now(timezone.utc)
    if not plan:
        log("nothing new")
        return (_stale(store, latest, "no newer RRFS source", clock) or 0) if live else 0
    codes = []
    for c in plan:
        left = BUILD_BUDGET_S - (time.monotonic() - t0)
        if codes and is_run(c) and left < RUN_MIN_LEFT_S:
            log(f"{cycle_name(c)}: {left:.0f} s of the budget left after the hourly cycle; the run waits for the next "
                f"tick (G66a P2-1)")
            continue
        code, latest = build_one(c, store, latest, live, a.out, t0, get, log, coords, grids)
        codes.append(code)
    rc = 1 if 1 in codes else 3 if 3 in codes else 0
    if live and rc != 1:
        rc = _stale(store, latest, "RRFS", clock) or rc
    return rc


if __name__ == "__main__":
    sys.exit(main())
