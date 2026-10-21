"""NOAA GLWU (Great Lakes Wave Unstructured) access for the Great Lakes frame job (plan section 50): run discovery, one
ranged GRIB read per forecast step, and the point bulletins.

Source: NCEP's operational Great Lakes WAVEWATCH III run, on NOMADS only (no AWS mirror exists, checked 2026-10-21):
  <BASE>/glwu.<YYYYMMDD>/glwu.grlc_2p5km.t<HH>z.grib2 (+ .idx)    long run: hourly 0..149 h, 150 steps x 19 records
  <BASE>/glwu.<YYYYMMDD>/bulls.t<HH>z/glwu.<id>.bull              point bulletins (the GFS-Wave "day & hour" layout)
Cycles 01/07/13/19Z run to +149 h (the other hours are a 48-h short run, not used). A cycle is posted ~26 min after its
hour; it counts as complete when its .idx lists every needed record of every step (the .idx is written after the GRIB).
Each step's records 1-7 are WIND WDIR UGRD VGRD HTSGW PERPW DIRPW, adjacent: ONE byte range per step (~300 KB) gives
the waves and the winds the job samples at the stations. The winds are NDFD forecaster grids, not GFS.

NOMADS limits a client to ~120 requests a minute (it once blocked the site's server address, plan section 40): every
request here goes through one pacer (one at a time, at least PACE_S apart); throttling (429 / 503) is retried with
growing pauses, then raised as Throttled (a NotReady: exit 3, no failure record), like the ECMWF job.
"""
import re
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import fetch as F

BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/glwu/prod"
CYCLES = (1, 7, 13, 19)
STEPS = list(range(0, 150))
STEP_SCHEDULE = [[0, 149, 1]]                   # [first, last, every] in hours, for the manifest
RECORDS = ("WIND:surface", "WDIR:surface", "UGRD:surface", "VGRD:surface",
           "HTSGW:surface", "PERPW:surface", "DIRPW:surface")
PACE_S = 0.6                                    # at most ~100 requests a minute, one at a time
THROTTLE_CODES = (429, 503)
THROTTLE_WAITS_S = (5, 10, 20, 30, 45, 60)
RETRY_AFTER_MAX_S = 60
OTHER_WAITS_S = (2, 4, 8)
UA = "allshoresurf.com model frames (https://allshoresurf.com)"
# The bulletin points (2026-10-21's bulls.t13z; the rip-current points left out), with the position each bulletin's
# "Location :" header gives: the job checks every bulletin against it and samples the station's wind there. 45195 is on
# Lake Champlain (GLWU's separate small run): it has a bulletin but lies outside the Great Lakes grid (no wind).
STATIONS = {
    "0Y2W3": (44.79, -87.31),
    "45001": (48.06, -87.79),
    "45002": (45.34, -86.41),
    "45003": (45.35, -82.84),
    "45004": (47.58, -86.58),
    "45005": (41.68, -82.40),
    "45006": (47.33, -89.79),
    "45007": (42.67, -87.03),
    "45008": (44.28, -82.42),
    "45012": (43.62, -77.41),
    "45014": (44.80, -87.76),
    "45022": (45.40, -85.09),
    "45023": (47.27, -88.61),
    "45024": (43.98, -86.56),
    "45025": (46.97, -88.40),
    "45026": (41.98, -86.62),
    "45027": (46.86, -91.93),
    "45028": (46.81, -91.84),
    "45029": (42.90, -86.27),
    "45132": (42.46, -81.21),
    "45135": (43.78, -76.87),
    "45136": (48.53, -86.95),
    "45137": (45.54, -81.02),
    "45139": (43.25, -79.54),
    "45142": (42.74, -79.29),
    "45143": (44.94, -80.63),
    "45147": (42.43, -82.68),
    "45149": (43.54, -82.08),
    "45154": (46.05, -82.64),
    "45159": (43.77, -78.98),
    "45160": (43.42, -79.63),
    "45161": (43.18, -86.36),
    "45163": (43.99, -83.59),
    "45165": (41.70, -83.26),
    "45167": (42.19, -80.14),
    "45168": (42.40, -86.33),
    "45169": (41.59, -81.79),
    "45170": (41.76, -86.97),
    "45171": (46.72, -87.41),
    "45172": (46.74, -85.98),
    "45173": (46.57, -86.57),
    "45174": (42.13, -87.66),
    "45195": (44.49, -73.34),
    "BARN6": (42.35, -79.59),
    "C58W3": (44.15, -87.56),
    "CAPEVINCEN": (44.10, -76.39),
    "DISW3": (47.08, -90.73),
    "ELONGPOINT": (42.53, -79.79),
    "GSLM4": (44.02, -83.54),
    "IndDunesNP": (41.63, -87.21),
    "IndDunesSP": (41.67, -87.07),
    "LITTLESABL": (43.65, -86.59),
    "LSCM4": (42.47, -82.76),
    "N_GREENBAY": (45.53, -87.02),
    "N_HAMLIN": (43.59, -77.99),
    "N_MICHIGAN": (45.87, -86.08),
    "N_OLCOTT": (43.57, -78.79),
    "OLCN6": (43.34, -78.72),
    "OSGN6": (43.46, -76.51),
    "PILM4": (48.22, -88.37),
    "ROAM4": (47.87, -89.31),
    "RPRN6": (43.26, -77.60),
    "SBIO1": (41.63, -82.84),
    "SBLM4": (43.81, -83.72),
    "SGNW3": (43.75, -87.69),
    "Silver_Bch": (42.11, -86.51),
    "SODUSBAY": (43.29, -76.97),
    "STDM4": (47.18, -87.22),
    "STURGEONPT": (42.70, -79.04),
    "S_GREENBAY": (44.98, -87.50),
    "WhihalaBch": (41.76, -87.50),
    "YGNN6": (43.26, -79.06),
}
BULLETIN_MAX_MISSING = 7                        # more bulletins than this missing or bad fails the build (about 10 %)

_PACE = threading.Lock()
_last = [0.0]

NotReady = F.NotReady
TransportError = F.TransportError


class Throttled(F.NotReady):
    """Every try of a request was throttled (429 / 503): not ready now, try again next tick."""


def run_dir(run_dt):
    return f"{BASE}/glwu.{run_dt:%Y%m%d}"


def grib_url(run_dt):
    return f"{run_dir(run_dt)}/glwu.grlc_2p5km.t{run_dt:%H}z.grib2"


def bull_url(run_dt, sid):
    return f"{run_dir(run_dt)}/bulls.t{run_dt:%H}z/glwu.{sid}.bull"


def _open(url, rng=None, timeout=60):
    headers = {"User-Agent": UA}
    if rng:
        headers["Range"] = rng
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=timeout) as r:
        return r.status, r.read()


def _retry_after(err):
    try:
        return min(RETRY_AFTER_MAX_S, max(0.0, float(err.headers.get("Retry-After"))))
    except (AttributeError, TypeError, ValueError):
        return None


def _paced_open(url, rng=None):
    """One request at a time, at least PACE_S after the previous one ended."""
    with _PACE:
        wait = _last[0] + PACE_S - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        try:
            return _open(url, rng)
        finally:
            _last[0] = time.monotonic()


def request(url, rng=None):
    """GET through the pacer: 403/404 -> NotReady; 429/503 -> retried after THROTTLE_WAITS_S (or Retry-After), then
    Throttled; any other error -> len(OTHER_WAITS_S) + 1 tries, then TransportError."""
    throttled = others = 0
    while True:
        try:
            return _paced_open(url, rng)
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                raise F.NotReady(f"{e.code} {url}") from None
            if e.code in THROTTLE_CODES:
                if throttled >= len(THROTTLE_WAITS_S):
                    raise Throttled(f"{url}: HTTP {e.code} (throttled) after {throttled + 1} tries") from None
                wait = _retry_after(e)
                time.sleep(THROTTLE_WAITS_S[throttled] if wait is None else wait)
                throttled += 1
                continue
            last = f"HTTP {e.code}"
        except Exception as e:                       # noqa: BLE001 - timeouts, resets, DNS
            last = repr(e)
        others += 1
        if others > len(OTHER_WAITS_S):
            raise F.TransportError(f"{url}: {last} after {others} tries")
        time.sleep(OTHER_WAITS_S[others - 1])


_IDX_RE = re.compile(r"^(\d+):(\d+):d=(\d{10}):([^:]+):([^:]+):([^:]+):")


def _step_of(label):
    if label == "anl":
        return 0
    m = re.fullmatch(r"(\d+) hour fcst", label)
    return int(m.group(1)) if m else None


def parse_idx(text):
    """The .idx -> [(offset, "VAR:level", step, YYYYMMDDHH)] in file order (step None for other time labels)."""
    recs = []
    for line in text.splitlines():
        m = _IDX_RE.match(line.strip())
        if not m:
            if line.strip():
                raise ValueError(f"unexpected .idx line: {line[:80]!r}")
            continue
        recs.append((int(m.group(2)), f"{m.group(4)}:{m.group(5)}", _step_of(m.group(6)), m.group(3)))
    return recs


def is_complete(recs, run_dt):
    """The run's .idx lists every needed record of every step, for this cycle."""
    have = {(key, step) for _off, key, step, d in recs if d == f"{run_dt:%Y%m%d%H}"}
    return all((key, s) in have for s in STEPS for key in RECORDS)


def span(recs, step):
    """(first byte, last byte) of the step's records WIND..DIRPW: adjacent, in RECORDS' order, and followed by another
    record (whose offset bounds the range)."""
    idx = [i for i, r in enumerate(recs) if r[2] == step and r[1] in RECORDS]
    keys = [recs[i][1] for i in idx]
    if keys != list(RECORDS) or idx != list(range(idx[0], idx[0] + len(RECORDS))):
        raise RuntimeError(f"step {step}: records not adjacent in the expected order: {keys}")
    end = idx[-1] + 1
    if end >= len(recs):
        raise RuntimeError(f"step {step}: no record after DIRPW to bound the range")
    return recs[idx[0]][0], recs[end][0] - 1


def split_messages(blob):
    """The GRIB2 messages of a byte string (section 0 carries each message's total length; each ends with 7777)."""
    out, i = [], 0
    while i < len(blob):
        if blob[i:i + 4] != b"GRIB" or len(blob) < i + 16 or blob[i + 7] != 2:
            raise RuntimeError(f"no GRIB2 message at byte {i}")
        n = int.from_bytes(blob[i + 8:i + 16], "big")
        if n < 16 or i + n > len(blob) or blob[i + n - 4:i + n] != b"7777":
            raise RuntimeError(f"bad GRIB2 length {n} at byte {i}")
        out.append(blob[i:i + n])
        i += n
    return out


def candidate_runs(now=None, lookback=4):
    """The newest `lookback` long-run cycles at or before `now`, newest first."""
    t = (now or datetime.now(timezone.utc)).replace(minute=0, second=0, microsecond=0)
    out = []
    while len(out) < lookback:
        if t.hour in CYCLES:
            out.append(t)
        t -= timedelta(hours=1)
    return out


def fetch_idx(run_dt):
    _status, body = request(grib_url(run_dt) + ".idx")
    return parse_idx(body.decode("ascii", "replace"))


def latest_complete_run(now=None):
    """(run_dt, parsed .idx) of the newest long-run cycle whose .idx lists every step, else (None, None). A cycle not
    posted yet (404) is skipped; transport failures and throttling raise."""
    for cand in candidate_runs(now):
        try:
            recs = fetch_idx(cand)
        except Throttled:
            raise
        except F.NotReady:
            continue
        if is_complete(recs, cand):
            return cand, recs
    return None, None


def fetch_step(run_dt, recs, step):
    """{"VAR:level": GRIB message} for the step's records WIND..DIRPW (one ranged read)."""
    lo, hi = span(recs, step)
    status, blob = request(grib_url(run_dt), rng=f"bytes={lo}-{hi}")
    if status != 206 or len(blob) != hi - lo + 1:
        raise RuntimeError(f"step {step}: expected 206 with {hi - lo + 1} bytes, got {status} / {len(blob)}")
    msgs = split_messages(blob)
    if len(msgs) != len(RECORDS):
        raise RuntimeError(f"step {step}: {len(msgs)} messages in the range, expected {len(RECORDS)}")
    return dict(zip(RECORDS, msgs))


def fetch_bulletin(run_dt, sid):
    _status, body = request(bull_url(run_dt, sid))
    return body.decode("ascii", "replace")
