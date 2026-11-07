"""Radar job (plan section 85): the newest precipitation radar over the US, Hawaii, Puerto Rico / the Caribbean, Guam and Europe
-> R2 under radar/v1/, a frame every 10 minutes, kept 24 h.

Sources: NOAA MRMS SeamlessHSR (mrms.py) and EUMETNET OPERA maximum reflectivity (opera.py). A slot is a nominal 10-minute time
("YYYYMMDDHHMM"). Europe's frame is the OPERA label at the slot (a MAX over the 10 minutes ending there); each MRMS frame is the
file at slot + 4 minutes (else + 2, + 0): it is out by about :x5:36, the Worker's tick is :x5, and no slot is taken before slot + 4:30, so
a run on time and a run late pick the same file nearly always. Each region's entry says its offset.

Every tier sits on one lattice: pixels of 1/ppd degree from whole-degree region boxes, row 0 at the box's north edge. Codes u8:
0 = no radar coverage, 1 = covered, no echo (below FLOOR_DBZ), code >= 2: dBZ = FLOOR_DBZ + (code - 2) * STEP_DBZ, up to MAX_CODE.
  t0  10 pixels a degree, the whole region, codes 0 / 1 / 2+ (the client's tint and "no coverage" always come from here)
  t1  25 pixels a degree, 20-degree cells, echo only (code < 2 stored as 0)
  t2 100 pixels a degree,  5-degree cells, echo only
A coarser pixel takes the k-th strongest fine pixel under it (k = COARSE_K), so a storm core stays and one stray pixel cannot paint
11 km; it counts as covered when any fine pixel is. A cell named "<lat0>_<lon0>" (its south-west corner) covers lat0 <= lat <
lat0 + cell, lon0 <= lon < lon0 + cell, in a picture of n + 2 pixels a side (a one-pixel apron from the neighbouring cells, so a
point in the cell finds its bilinear neighbours in the cell's own picture): row r is centred at lat0 + cell + 0.5/ppd - r/ppd,
column c at lon0 - 0.5/ppd + c/ppd. Only cells holding echo are written (T1_MIN, T2_MIN pixels at MIN_DBZ or more): most of the
map is dry. A threshold above one made light showers flip between tiers from frame to frame in a playing loop (owner, 2026-11-06).

Layout (immutable but the pointer):
  radar/v1/<slot>/<region>/t0.png, t1/<lat0>_<lon0>.png, t2/<lat0>_<lon0>.png
  radar/v1/latest.json  max-age 60, written after each slot's frames, only when something changed. Cell lists are hex bitsets over
  regions' fixed cell tables (cell_table; four bits a character, least significant first). A slot record can hold no frames yet
  (`r` empty, `x` counting a file's failed tries): the client must not count it as a frame.
  radar/v1/stale-alarm.json

Failure mail stays rare (plan section 83): a source that times out or errors is a summary line and exit 0, asked again next tick. A
region with nothing newer than SOURCE_STALE_H, a region whose files keep failing to read (a changed layout) with nothing newer than
FORMAT_ALARM_MIN, or old slots that cannot be deleted, are one alarm per 12 hours (exit 1), and every other region still publishes.
A condition no tick can cure and no alarm can record (a pointer that is not this job's, a bucket refusing writes) fails the run only
around 00:00 and 12:00 UTC. An older slot left unfilled (after an outage, or a new prefix) is filled newest first while the
tick's backfill budget lasts. A file NOAA or EUMETNET never publishes, or a damaged one with newer good frames after it, is asked
MAX_TRIES times, then left a gap.
Exit codes: 0 published / nothing new / a quiet outage; 1 an alarm; 2 bad arguments or a pointer that is not this job's.
"""
import argparse
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import alt as A          # noqa: E402  (fetch, alarms, summary, store from the environment)
import encode as E       # noqa: E402  (to_png)
import mrms              # noqa: E402
import opera             # noqa: E402
import publish as P      # noqa: E402

PREFIX = P.RADAR_PREFIX
FORMAT = "radar-v1"
POINTER_CACHE = "public, max-age=60"
JSON = "application/json"
FLOOR_DBZ, STEP_DBZ = 5.0, 0.5
MAX_DBZ = 75.0
MAX_CODE = 2 + int(round((MAX_DBZ - FLOOR_DBZ) / STEP_DBZ))     # 142
MIN_DBZ = 10.0                          # the echo a cell must hold to be written
MIN_CODE = 2 + int(round((MIN_DBZ - FLOOR_DBZ) / STEP_DBZ))
T1_MIN, T2_MIN = 1, 1                   # pixels at MIN_DBZ or more (measured: ~10 more files a slot than 6 and 40, 5 KB)
PPD_T0, PPD_T1, PPD_T2 = 10, 25, 100
CELL_T1, CELL_T2 = 20, 5
COARSE_K = {PPD_T1: 2, PPD_T0: 3}       # the k-th strongest fine pixel under a coarse pixel
SLOT_MINUTES = 10
KEEP_SLOTS = 145                        # 24 h of 10-minute frames (+ the newest)
GRACE_MIN = 60                          # a slot that left the pointer stays this long for open tabs, then goes
LOOKBACK = 3                            # slots always tried on a tick
BACKFILL_S = 150                        # older slots are started only this long into a tick
DEADLINE_S = 300                        # no slot, region or delete is started past this
HARD_S = 420                            # an upload still going stops here (the workflow's timeout: 12 min)
MAX_TRIES = 3                           # a file answering 404 on a slot older than an hour: then a gap
SOURCE_STALE_H = 6                      # a region with nothing newer than this: one alarm
FORMAT_ALARM_MIN = 30                   # a region's files failing to read with nothing newer than this: one alarm
BAD_PER_TICK = 2                        # bad files of one region before the tick leaves that region alone
PRUNE_PER_TICK = 6                      # old slots deleted at most per tick (one retires a tick; the rest is a backlog)
GONE_ALARM_H = 6                        # old slots still undeleted this long after retiring: one alarm
MRMS_OFFSETS = (4, 2, 0)                # minutes after the slot
MRMS_READY_S = 270                      # no MRMS frame is taken for a slot before slot + 4:30 (a run on a punctual cron would pick + 2)
UPLOAD_WORKERS = 8
FETCH_TIMEOUT_S = 45                    # per socket read
FETCH_MAX_S = 120                       # a whole file
READ_CHUNK = 256 * 1024
UA = "allshoresurf.com radar (https://allshoresurf.com)"
R2_CONNECT_S, R2_READ_S, R2_ATTEMPTS = 10, 30, 2      # the bucket answers in time or waits for the next tick: a tick is 10 minutes
CREDIT = ("Radar: NOAA Multi-Radar/Multi-Sensor System (MRMS) via AWS Open Data; EUMETNET OPERA composite, CC BY 4.0 "
          "(https://creativecommons.org/licenses/by/4.0/), property rights EUMETNET; both modified: regridded, quantised, thinned "
          "and rendered by Allshore Surf")
# name -> (label, source, MRMS domain, south, west, north, east, MRMS pool)
REGIONS = {
    "conus":  ("US", "mrms", "CONUS", 20, -130, 55, -60, 1),
    "alaska": ("Alaska", "mrms", "ALASKA", 50, -176, 72, -126, 1),
    "hawaii": ("Hawaii", "mrms", "HAWAII", 15, -164, 26, -151, 2),
    "carib":  ("Puerto Rico and the Caribbean", "mrms", "CARIB", 10, -90, 25, -60, 1),
    "guam":   ("Guam", "mrms", "GUAM", 9, 140, 18, 150, 2),
    "europe": ("Europe", "opera", None, 31, -40, 75, 58, 1),
}


class Missing(Exception):
    """The source has not (or never) published this file: 404."""


class Down(Exception):
    """The source did not answer: asked again next tick, the source left alone for the rest of this one."""


# ---------------------------------------------------------------- names and geometry
def box(region):
    r = REGIONS[region]
    return r[3], r[4], r[5], r[6]


def regions_of(source):
    return [k for k, v in REGIONS.items() if v[1] == source]


def slot_of(dt):
    return dt.replace(minute=dt.minute - dt.minute % SLOT_MINUTES, second=0, microsecond=0)


def slot_name(dt):
    return dt.strftime("%Y%m%d%H%M")


def parse_slot(s):
    return datetime.strptime(s, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


def t0_key(slot, region):
    return f"{PREFIX}/{slot}/{region}/t0.png"


def cell_key(slot, region, tier, lat0, lon0):
    return f"{PREFIX}/{slot}/{region}/{tier}/{lat0}_{lon0}.png"


def _floor_to(v, step):
    return (v // step) * step


def cell_table(region, cell):
    """[(lat0, lon0)] of the cells a region's box touches: south to north, then west to east (the bitsets' order)."""
    s, w, n, e = box(region)
    out = []
    lat = _floor_to(s, cell)
    while lat < n:
        lon = _floor_to(w, cell)
        while lon < e:
            out.append((lat, lon))
            lon += cell
        lat += cell
    return out


def bits_hex(flags):
    """Booleans -> hex, four bits a character, least significant first, trailing zeros off."""
    chars = []
    for i in range(0, len(flags), 4):
        v = sum(1 << j for j, f in enumerate(flags[i:i + 4]) if f)
        chars.append(format(v, "x"))
    return "".join(chars).rstrip("0")


def hex_bits(text, n):
    out = []
    for ch in text:
        v = int(ch, 16)
        out += [bool(v >> j & 1) for j in range(4)]
    return (out + [False] * n)[:n]


# ---------------------------------------------------------------- codes and tiers
def code_table(values):
    """Codes of an array of dBZ (NaN or below -900 = no coverage; -inf, -99 or below the floor = no echo)."""
    v = np.asarray(values, dtype=np.float32)
    out = np.ones(v.shape, dtype=np.uint8)
    out[np.isnan(v) | (v < -900.0) & ~np.isneginf(v)] = 0
    echo = np.isfinite(v) & (v >= FLOOR_DBZ)
    out[echo] = np.clip(2 + np.round((v[echo] - FLOOR_DBZ) / STEP_DBZ), 2, MAX_CODE).astype(np.uint8)
    return out


def pool_max(a, b):
    """The strongest of each b x b block."""
    r, c = a.shape
    if r % b or c % b:
        raise ValueError(f"{a.shape} does not divide by {b}")
    return a.reshape(r // b, b, c // b, b).max(axis=(1, 3))


def pool_kth(a, b, k):
    """Each b x b block's k-th strongest echo; a block with fewer echoes than k reads as covered (1) when any pixel is covered."""
    r, c = a.shape
    if r % b or c % b:
        raise ValueError(f"{a.shape} does not divide by {b}")
    blocks = a.reshape(r // b, b, c // b, b).transpose(0, 2, 1, 3)
    top = blocks.max(axis=(2, 3))
    out = np.minimum(top, 1).astype(np.uint8)
    rows, cols = np.nonzero(top >= 2)
    if len(rows):
        vals = blocks[rows, cols].reshape(len(rows), b * b)
        kth = np.partition(vals, b * b - k, axis=1)[:, b * b - k]
        out[rows[kth >= 2], cols[kth >= 2]] = kth[kth >= 2]
    return out


def cell_picture(arr, region, ppd, cell, lat0, lon0):
    """A cell's n + 2 pixel picture from a region's lattice (echo only; zero outside the region's box)."""
    s, w, n_lat, e = box(region)
    n = cell * ppd
    out = np.zeros((n + 2, n + 2), dtype=np.uint8)
    r0 = (n_lat - (lat0 + cell)) * ppd - 1              # the region row of picture row 0
    c0 = (lon0 - w) * ppd - 1                           # the region column of picture column 0
    rr0, cc0 = max(r0, 0), max(c0, 0)
    rr1, cc1 = min(r0 + n + 2, arr.shape[0]), min(c0 + n + 2, arr.shape[1])
    if rr1 > rr0 and cc1 > cc0:
        part = arr[rr0:rr1, cc0:cc1]
        out[rr0 - r0:rr1 - r0, cc0 - c0:cc1 - c0] = np.where(part >= 2, part, 0)
    return out


def inner_echo(pic):
    """Pixels at MIN_DBZ or more inside the cell (the apron belongs to the neighbours)."""
    return int((pic[1:-1, 1:-1] >= MIN_CODE).sum())


def tiers_of(t2_codes, region):
    """A region's 100-per-degree lattice -> [(key parts, png bytes)] and its entry's bitsets."""
    t1 = pool_kth(t2_codes, PPD_T2 // PPD_T1, COARSE_K[PPD_T1])
    t0 = pool_kth(t2_codes, PPD_T2 // PPD_T0, COARSE_K[PPD_T0])
    objects = [(("t0", None, None), t0)]
    flags = {}
    for name, arr, ppd, cell, need in (("t1", t1, PPD_T1, CELL_T1, T1_MIN), ("t2", t2_codes, PPD_T2, CELL_T2, T2_MIN)):
        bits = []
        for lat0, lon0 in cell_table(region, cell):
            pic = cell_picture(arr, region, ppd, cell, lat0, lon0)
            keep = inner_echo(pic) >= need
            bits.append(keep)
            if keep:
                objects.append(((name, lat0, lon0), pic))
        flags[name] = bits_hex(bits)
    return objects, flags, t0


# ---------------------------------------------------------------- sources
def _get(url, headers=None, timeout=60):
    req = urllib.request.Request(url, headers=dict({"User-Agent": UA}, **(headers or {})))
    return urllib.request.urlopen(req, timeout=timeout)


def _read_whole(url, get, max_bytes, timeout=None, max_s=None):
    """Read to the end within max_s; a body shorter than its Content-Length is a dropped connection, not a damaged file
    (http.client returns what it has on a clean close, without an error)."""
    timeout, max_s = timeout or FETCH_TIMEOUT_S, max_s or FETCH_MAX_S
    began = time.monotonic()
    parts, size = [], 0
    with get(url, timeout=timeout) as r:
        headers = getattr(r, "headers", None)
        want = headers.get("Content-Length") if headers is not None else None
        while True:
            chunk = r.read(READ_CHUNK)
            if not chunk:
                break
            size += len(chunk)
            if size > max_bytes:
                raise ValueError(f"{url}: more than {max_bytes} bytes")
            if time.monotonic() - began > max_s:
                raise TimeoutError(f"{url}: not finished in {max_s} s")
            parts.append(chunk)
    if want is not None and str(want).isdigit() and size != int(want):
        raise ConnectionError(f"{url}: {size} of {want} bytes")
    return b"".join(parts)


def fetch(url, get, max_bytes, timeout=None, max_s=None):
    """The whole body. 404 = Missing (both buckets answer 404 for an absent key); other answers, time-outs and dropped connections
    = Down: a refusal or an outage must never count as "never published"."""
    last = None
    for attempt in range(2):
        try:
            return _read_whole(url, get, max_bytes, timeout=timeout, max_s=max_s)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise Missing(f"HTTP {exc.code}") from None
            last = exc
        except ValueError:
            raise
        except Exception as exc:                                       # noqa: BLE001
            last = exc
        if attempt == 0:
            time.sleep(2)
    raise Down(f"{last.__class__.__name__}: {P.redact(str(last))}")


def _reading(what, fn):
    """Whatever a file does to a reader is a damaged or changed file (ValueError): zlib, Pillow and h5py each have their own errors."""
    try:
        return fn()
    except ValueError:
        raise
    except Exception as exc:                                           # noqa: BLE001
        raise ValueError(f"{what}: {exc.__class__.__name__}: {exc}") from None


def mrms_lattice(region, msg):
    """A decoded MRMS message -> the region's 100-per-degree lattice of codes. A grid that is not the expected one is a ValueError."""
    label, _src, domain, s, w, n, e, pool = REGIONS[region]
    step = 0.01 / pool
    if msg["nj"] != round((n - s) / step) or msg["ni"] != round((e - w) / step) \
            or abs(msg["di"] - step) > 1e-5 or abs(msg["dj"] - step) > 1e-5 \
            or abs(msg["lat1"] - (n - step / 2)) > 0.6 * step or abs(msg["lon1"] - (w + step / 2)) > 0.6 * step:
        raise ValueError(f"{domain} grid {msg['ni']} x {msg['nj']} from {msg['lat1']}, {msg['lon1']} is not the expected one")
    codes = code_table(mrms.value_table(msg))[msg["x"]]
    return pool_max(codes, pool) if pool > 1 else codes


def mrms_frame(region, slot_dt, now, get):
    """(lattice, offset) of the newest MRMS file at slot + MRMS_OFFSETS not later than now."""
    domain = REGIONS[region][2]
    for off in MRMS_OFFSETS:
        when = slot_dt + timedelta(minutes=off)
        if when > now:
            continue
        try:
            body = fetch(mrms.url_of(domain, when), get, mrms.MAX_GZ_BYTES)
        except Missing:
            continue
        return _reading(domain, lambda: mrms_lattice(region, mrms.read(mrms.gunzip(body)))), off
    raise Missing(f"no {domain} file for {slot_name(slot_dt)}")


_OPERA_MAP = {}


def opera_map():
    """(target flat indices, source flat indices) of Europe's lattice: computed once a run (forward projection of 43 M points)."""
    if "pairs" not in _OPERA_MAP:
        s, w, n, e = box(regions_of("opera")[0])
        ppd = PPD_T2
        lats = n - (np.arange((n - s) * ppd) + 0.5) / ppd
        lons = w + (np.arange((e - w) * ppd) + 0.5) / ppd
        tgt, src = [], []
        for r, lat in enumerate(lats):
            idx = opera.source_index(np.float64(lat), lons)
            cols = np.nonzero(idx >= 0)[0]
            if len(cols):
                tgt.append(r * len(lons) + cols)
                src.append(idx[cols])
        _OPERA_MAP["pairs"] = (np.concatenate(tgt).astype(np.int32), np.concatenate(src).astype(np.int32),
                               (len(lats), len(lons)))
    return _OPERA_MAP["pairs"]


def opera_lattice(values):
    tgt, src, shape = opera_map()
    codes = code_table(values).ravel()
    out = np.zeros(shape[0] * shape[1], dtype=np.uint8)
    out[tgt] = codes[src]
    return out.reshape(shape)


def opera_frame(slot_dt, get):
    body = fetch(opera.url_of(slot_dt), get, opera.MAX_BYTES)
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "f.h5")
        with open(path, "wb") as fh:
            fh.write(body)
        data = _reading("OPERA", lambda: opera.read(path))
    if data["time"] != slot_name(slot_dt):
        raise ValueError(f"the file labelled {slot_name(slot_dt)} says {data['time']}")
    return _reading("OPERA", lambda: opera_lattice(data["values"])), 0, data["radars"]


def _read_mrms(region, dt, now, get):
    lattice, offset = mrms_frame(region, dt, now, get)
    return lattice, offset, None


def _read_opera(region, dt, now, get):
    lattice, offset, radars = opera_frame(dt, get)
    return lattice, offset, f"{radars} radars"


# A source and its clock, in the order a tick runs them. read(region, slot datetime, now, get) -> (lattice, minutes after the slot, a
# log line or None). ready_s: no frame is taken for a slot younger than this. reach_h: slots older than this are never asked for (None:
# the whole window). backfill_s: older slots are started only this long into the budget's start; spend_s, when set, makes that start
# the source's own turn and also stops the whole source that long after it, so a slow source cannot take the time of the next. MRMS
# and OPERA count from the tick's start, so a hung MRMS still leaves Europe's newest frame.
SOURCE = {
    "mrms":  {"read": _read_mrms, "ready_s": MRMS_READY_S, "reach_h": None, "backfill_s": BACKFILL_S / 2, "spend_s": None},
    "opera": {"read": _read_opera, "ready_s": 0, "reach_h": None, "backfill_s": BACKFILL_S, "spend_s": None},
}
SOURCES = tuple(SOURCE)


# ---------------------------------------------------------------- publishing
def store_region(store, slot, region, lattice, offset, stop=None):
    """Every object of one region and slot -> its pointer entry, with sizes for the summary. Past `stop` the rest is left (Down: the
    pointer never named them, and the next tick writes them again)."""
    objects, flags, _t0 = tiers_of(lattice, region)

    def put(item):
        if stop is not None and stop():
            raise Down("the bucket is too slow for this tick")
        (tier, lat0, lon0), arr = item
        key = t0_key(slot, region) if tier == "t0" else cell_key(slot, region, tier, lat0, lon0)
        png = E.to_png(arr)
        store.put(key, png, "image/png", P.IMMUTABLE)
        return tier, len(png)

    with ThreadPoolExecutor(UPLOAD_WORKERS) as pool:
        sizes = list(pool.map(put, objects))
    entry = {"o": offset}
    for t in ("t1", "t2"):
        if flags[t]:
            entry[t] = flags[t]
    stats = {t: (sum(1 for x, _n in sizes if x == t), sum(n for x, n in sizes if x == t)) for t in ("t0", "t1", "t2")}
    return entry, stats


def regions_block():
    return {k: {"name": v[0], "src": v[1], "box": [v[3], v[4], v[5], v[6]]} for k, v in REGIONS.items()}


def empty_pointer():
    return {"format": FORMAT, "floor_dbz": FLOOR_DBZ, "step_dbz": STEP_DBZ, "max_code": MAX_CODE,
            "tiers": {"t0": {"ppd": PPD_T0}, "t1": {"ppd": PPD_T1, "cell": CELL_T1}, "t2": {"ppd": PPD_T2, "cell": CELL_T2}},
            "regions": regions_block(), "slots": [], "gone": [], "newest": {}, "credit": CREDIT}


def newest_of(pointer):
    """{region: its newest slot in the pointer}."""
    out = {}
    for s in pointer["slots"]:
        for region in s["r"]:
            if s["t"] > out.get(region, ""):
                out[region] = s["t"]
    return out


def note_began(pointer, now):
    """When each region joined the job: a region added later must not read as silent since the job's first tick and alarm at once. A
    region the pointer already lists counts from `started`. -> whether anything was added."""
    began = pointer.setdefault("began", {})
    listed = pointer.get("regions", {})
    added = False
    for region in REGIONS:
        if region not in began:
            began[region] = pointer["started"] if region in listed else A.utc(now)
            added = True
    return added


def drop_unlisted(pointer):
    """Slot entries of regions this job does not write: a client refuses a pointer naming a region it does not list, so a job rolled
    back past a region's merge must not leave them for the 24 h those slots live. Their files go with their slots. -> whether any went."""
    known, dropped = set(REGIONS), False
    for slot in pointer["slots"]:
        for region in list(slot["r"]):
            if region not in known:
                del slot["r"][region]
                dropped = True
    return dropped


def write_pointer(store, pointer, now):
    pointer["slots"].sort(key=lambda x: x["t"])
    pointer["published_utc"] = A.utc(now)
    store.put(f"{PREFIX}/latest.json", json.dumps(pointer, separators=(",", ":"), sort_keys=True, allow_nan=False).encode(),
              JSON, POINTER_CACHE)


def plan(pointer, now, source, keep=KEEP_SLOTS):
    """[(slot name, datetime, [regions still to do])] of `source`, newest first: slots not yet filled and not given up."""
    have = {s["t"]: s for s in pointer["slots"]}
    newest = slot_of(now)
    cfg = SOURCE[source]
    out = []
    for i in range(keep):
        dt = newest - timedelta(minutes=SLOT_MINUTES * i)
        if (now - dt).total_seconds() < cfg["ready_s"]:
            continue
        if cfg["reach_h"] is not None and now - dt > timedelta(hours=cfg["reach_h"]):
            break
        name = slot_name(dt)
        rec = have.get(name, {})
        todo = [r for r in regions_of(source) if r not in rec.get("r", {}) and rec.get("x", {}).get(r, 0) < MAX_TRIES]
        if todo:
            out.append((name, dt, todo))
    return out


def slot_record(pointer, name):
    for s in pointer["slots"]:
        if s["t"] == name:
            return s
    rec = {"t": name, "r": {}}
    pointer["slots"].append(rec)
    return rec


def retire_old(pointer, now, keep=KEEP_SLOTS):
    """Slots older than the window leave the list (kept in `gone` for GRACE_MIN). -> whether anything changed."""
    cutoff = slot_name(slot_of(now) - timedelta(minutes=SLOT_MINUTES * (keep - 1)))
    old = [s for s in pointer["slots"] if s["t"] < cutoff]
    for s in old:
        pointer["slots"].remove(s)
        pointer["gone"].append({"t": s["t"], "at": A.utc(now)})
    return bool(old)


def prune_gone(store, pointer, now, over=None):
    """Delete the slots whose grace is over (per slot: a whole-prefix listing would cost hundreds of list calls a day), at most
    PRUNE_PER_TICK and none past `over`. A failure leaves them in `gone` and ends the deleting for this tick. -> whether `gone`
    changed."""
    left, changed, tries = [], False, 0
    for g in pointer["gone"]:
        if tries < PRUNE_PER_TICK and not (over is not None and over()):
            try:
                if (now - A.parse_iso(g["at"])).total_seconds() >= GRACE_MIN * 60:
                    tries += 1
                    store.delete_prefix(f"{PREFIX}/{g['t']}/")
                    changed = True
                    continue
            except Exception as exc:                                   # noqa: BLE001
                A._summary(f"WARNING: radar prune of {g['t']} failed ({exc.__class__.__name__}: {P.redact(str(exc), store.bucket)})")
                tries = PRUNE_PER_TICK
        left.append(g)
    pointer["gone"] = left
    return changed


class _Alarm:
    """What alt.alarm_once needs of a product."""
    prefix = PREFIX

    @staticmethod
    def stale_key():
        return f"{PREFIX}/stale-alarm.json"


def rated(rc, now):
    """A condition no tick can cure and no alarm can record: a failed run (so a mail) only around 00:00 and 12:00 UTC."""
    return rc if now.hour % 12 == 0 and now.minute < 10 else 0


def alarm(store, line, now, kind):
    """1 or None as alt.alarm_once; a bucket that cannot record it is a rated failure, never an uncaught error on every tick."""
    try:
        return A.alarm_once(store, _Alarm, line, now=now, kind=kind)
    except Exception as exc:                                           # noqa: BLE001
        A._summary(f"{line} (not recorded: {exc.__class__.__name__}: {P.redact(str(exc), getattr(store, 'bucket', None))})")
        return rated(1, now) or None


def _quiet_since(pointer, region, newest):
    """The newest frame of `region` (or, with none, when the region joined the job) as (label, datetime)."""
    t = newest.get(region)
    if t:
        return t, parse_slot(t)
    return "none", A.parse_iso(pointer.get("began", {}).get(region) or pointer["started"])


def state_alarms(store, pointer, now, sources):
    """One alarm per region with nothing newer than SOURCE_STALE_H; one when old slots cannot be deleted."""
    rc = 0
    newest = newest_of(pointer)
    for src in sources:
        for region in regions_of(src):
            label, since = _quiet_since(pointer, region, newest)
            age_h = (now - since).total_seconds() / 3600
            if age_h > SOURCE_STALE_H:
                rc = alarm(store, f"STALE: radar region {region}: newest frame {label} ({age_h:.1f} h old)", now, f"region_{region}") or rc
    if pointer["gone"]:
        age_h = (now - A.parse_iso(pointer["gone"][0]["at"])).total_seconds() / 3600
        if age_h > GONE_ALARM_H:
            rc = alarm(store, f"radar: {len(pointer['gone'])} old slots cannot be deleted, the oldest retired {age_h:.1f} h ago", now, "prune") or rc
    return rc


# ---------------------------------------------------------------- one tick
def run_source(source, store, pointer, now, clock, started, get, log, budget, keep=KEEP_SLOTS, max_slots=None, deadline=DEADLINE_S):
    """Fill `source`'s missing frames, newest first, within the time budgets (seconds since `started`). A file that cannot be read
    costs its own region at most BAD_PER_TICK tries this tick and nothing else. -> (frames done, per-tier stats, {region: alarm line})."""
    done, stats, down, out_of_time = 0, {"t0": [0, 0], "t1": [0, 0], "t2": [0, 0]}, False, False
    newest = newest_of(pointer)
    bad, bad_lines = {}, {}
    stop = lambda: clock() - started > HARD_S                          # noqa: E731
    read = SOURCE[source]["read"]
    for i, (name, dt, todo) in enumerate(plan(pointer, now, source, keep=keep)):
        if max_slots is not None and i >= max_slots:
            break
        elapsed = clock() - started
        recent = dt >= slot_of(now) - timedelta(minutes=SLOT_MINUTES * (LOOKBACK - 1))
        if elapsed > deadline or (not recent and elapsed > budget):
            log(f"{source}: time budget used ({elapsed:.0f} s), {name} and older left for later ticks")
            break
        dirty = False
        for region in todo:
            if bad.get(region, 0) >= BAD_PER_TICK:
                continue
            if clock() - started > deadline:
                log(f"{source}: time budget used, {name} {region} and older left for later ticks")
                out_of_time = True
                break
            try:
                lattice, offset, note = read(region, dt, now, get)
                if note:
                    log(f"{name} {region}: {note}")
                entry, st = store_region(store, name, region, lattice, offset, stop=stop)
            except Missing as exc:
                log(f"{name} {region}: not published ({exc})")
                if dt < slot_of(now) - timedelta(hours=1):             # a recent slot is simply not out yet
                    x = slot_record(pointer, name).setdefault("x", {})
                    x[region] = x.get(region, 0) + 1
                    dirty = True
                continue
            except Down as exc:
                A._summary(f"radar: {source} did not answer ({exc}); asked again next tick")
                down = True
                break
            except ValueError as exc:
                bad[region] = bad.get(region, 0) + 1
                bad_lines[region] = f"radar: {source} {region} {name}: the file cannot be read (a damaged file or a changed layout): {P.redact(str(exc))}"
                log(bad_lines[region])
                if name < newest.get(region, ""):                      # newer frames of the region read well: this one object is bad
                    x = slot_record(pointer, name).setdefault("x", {})
                    x[region] = x.get(region, 0) + 1
                    dirty = True
                continue
            rec = slot_record(pointer, name)
            rec["r"][region] = entry
            dirty = True
            done += 1
            for t, (n, b) in st.items():
                stats[t][0] += n
                stats[t][1] += b
            if name > pointer["newest"].get(source, ""):
                pointer["newest"][source] = name
            if name > newest.get(region, ""):
                newest[region] = name
            log(f"{name} {region}: +{offset} min, cells t1 {st['t1'][0]} t2 {st['t2'][0]}, {sum(b for _n, b in st.values()) // 1024} KB")
        if dirty:
            write_pointer(store, pointer, now)                         # each slot's frames go live as they finish
        if down or out_of_time:
            break
    # a layout alarm only when the region has gone quiet: one damaged file among good ones is a line in the log and, soon, a gap
    alarms = {}
    for region, line in bad_lines.items():
        _label, since = _quiet_since(pointer, region, newest)
        if (now - since).total_seconds() > FORMAT_ALARM_MIN * 60:
            alarms[region] = line
    return done, stats, alarms


class LocalStore:
    """--local: the objects under a folder, as they would be in the bucket."""
    bucket = "local"

    def __init__(self, root):
        self.root = root

    def _path(self, key):
        return os.path.join(self.root, *key.split("/"))

    def put(self, key, body, content_type, cache_control):
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fh:
            fh.write(body)

    def get_json(self, key):
        try:
            with open(self._path(key), "rb") as fh:
                return json.loads(fh.read().decode("utf-8"))
        except FileNotFoundError:
            return None

    def delete_prefix(self, prefix):
        import shutil
        shutil.rmtree(self._path(prefix.rstrip("/")), ignore_errors=True)


class NullStore(LocalStore):
    """--dry-run: nothing kept."""

    def __init__(self):
        super().__init__("")
        self.count = 0

    def put(self, key, body, content_type, cache_control):
        self.count += 1

    def get_json(self, key):
        return None

    def delete_prefix(self, prefix):
        pass


def good_pointer(p):
    """The fields this job reads, down to their types: a pointer this job did not write stops it rather than break it each tick."""
    try:
        return p["format"] == FORMAT and isinstance(p["newest"], dict) \
            and all(isinstance(v, str) and parse_slot(v) for v in p["newest"].values()) \
            and all(isinstance(s["t"], str) and parse_slot(s["t"]) and isinstance(s["r"], dict) and isinstance(s.get("x", {}), dict)
                    for s in p["slots"]) \
            and all(isinstance(g["t"], str) and parse_slot(g["t"]) and A.parse_iso(g["at"]) for g in p["gone"]) \
            and (p.get("started") is None or bool(A.parse_iso(p["started"]))) \
            and isinstance(p.get("began", {}), dict) and all(A.parse_iso(v) for v in p.get("began", {}).values())
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def _store_from_env():
    env = os.environ
    client = P.r2_client(env["R2_ACCOUNT_ID"], env["R2_ACCESS_KEY_ID"], env["R2_SECRET_ACCESS_KEY"], max_attempts=R2_ATTEMPTS,
                         connect_timeout=R2_CONNECT_S, read_timeout=R2_READ_S)
    return P.Store(client, env["R2_BUCKET"])


def main(argv=None, get=_get, store=None, now=None, clock=time.monotonic):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="fetch and encode; store nothing, read no pointer")
    ap.add_argument("--local", default=None, help="write under this folder instead of the bucket")
    ap.add_argument("--slots", type=int, default=None, help="at most this many slots per source (a test or a dry run)")
    ap.add_argument("--sources", default=",".join(SOURCES), help="comma list of sources")
    a = ap.parse_args(argv)
    sources = [s for s in a.sources.split(",") if s]
    if any(s not in SOURCES for s in sources) or (a.slots is not None and a.slots < 1):
        print("bad arguments")
        return 2
    now = now or datetime.now(timezone.utc)
    live = not a.dry_run and a.local is None
    if store is None:
        store = NullStore() if a.dry_run else LocalStore(a.local) if a.local else _store_from_env()
    started = clock()
    try:
        pointer = store.get_json(f"{PREFIX}/latest.json")
    except ValueError as exc:
        print(f"the pointer cannot be read ({exc}): refusing to touch it")
        return rated(2, now)
    except Exception as exc:                                           # noqa: BLE001
        A._summary(f"radar: the bucket did not answer ({exc.__class__.__name__}: {P.redact(str(exc), getattr(store, 'bucket', None))})")
        return 0
    if pointer is None:
        pointer = empty_pointer()
    elif not good_pointer(pointer):
        print("the pointer is not a radar-v1 pointer: refusing to touch it")
        return rated(2, now)
    rc = 0
    lines = []

    def log(line):
        print(line)

    try:
        first = "started" not in pointer                               # the alarms' clock: a region never seen is silent since then
        pointer.setdefault("started", A.utc(now))
        joined = note_began(pointer, now)
        pointer.update(regions=regions_block(), credit=CREDIT)
        dropped = drop_unlisted(pointer)
        retired = retire_old(pointer, now)
        for src in sources:
            cfg = SOURCE[src]
            try:
                budget, deadline = cfg["backfill_s"], DEADLINE_S       # MRMS's backfill cannot starve Europe's newest frame
                if cfg["spend_s"] is not None:
                    turn = clock() - started
                    budget, deadline = turn + cfg["backfill_s"], min(DEADLINE_S, turn + cfg["spend_s"])
                done, stats, alarms = run_source(src, store, pointer, now, clock, started, get, log, budget,
                                                 max_slots=a.slots, deadline=deadline)
            except Exception as exc:                                   # noqa: BLE001   one source's fault must not cost the others their frames
                A._summary(f"radar: {src} stopped on {exc.__class__.__name__}: {P.redact(str(exc), getattr(store, 'bucket', None))}; "
                           "the other sources go on")
                lines.append(f"{src}: stopped")
                continue
            lines.append(f"{src}: {done} frames, t0 {stats['t0'][1] // 1024} KB, t1 {stats['t1'][0]} cells {stats['t1'][1] // 1024} KB, "
                         f"t2 {stats['t2'][0]} cells {stats['t2'][1] // 1024} KB")
            for region, line in alarms.items():
                rc = alarm(store, line, now, f"format_{region}") or rc
        if prune_gone(store, pointer, now, over=lambda: clock() - started > DEADLINE_S) or retired or first or joined or dropped:
            write_pointer(store, pointer, now)
    except Exception as exc:                                           # noqa: BLE001   the bucket went away mid-tick
        A._summary(f"radar: stopped on {exc.__class__.__name__}: {P.redact(str(exc), getattr(store, 'bucket', None))}; the next tick goes on")
    A._summary("### radar: " + "; ".join(lines) + f"; {clock() - started:.0f} s, newest {pointer['newest']}"
               + (" (dry run)" if a.dry_run else ""))
    if live and a.slots is None and "started" in pointer:
        try:
            rc = state_alarms(store, pointer, now, sources) or rc
        except Exception as exc:                                       # noqa: BLE001
            A._summary(f"radar: the alarms could not be judged ({exc.__class__.__name__}: {P.redact(str(exc), getattr(store, 'bucket', None))})")
            rc = rated(1, now) or rc
    return rc


if __name__ == "__main__":
    sys.exit(main())
