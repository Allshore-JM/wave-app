"""ECMWF open-data access for the ECMWF overlay job (plan section 43): run discovery + ranged GRIB record reads.

Source: ECMWF's free real-time open data (CC BY 4.0), IFS 0.25 deg, mirrored on the AWS Open Data bucket
`ecmwf-forecasts` (anonymous ListObjectsV2 + ranged GETs, like NOAA's bucket). Keys:
  <YYYYMMDD>/<HH>z/ifs/0p25/<stream>/<YYYYMMDDHH>0000-<step>h-<stream>-fc.grib2   (+ .index)
streams `wave` (swh, pp1d, mwd ...) and `oper` (10u, 10v ...). The .index is JSON lines, one per GRIB message, with
`param`, `levtype`, `step`, `date`, `time`, `_offset`, `_length`. The same relative path is served by
data.ecmwf.int/forecasts/, used as a second host for the ranged reads only.

Runs: only the 00Z and 12Z cycles (owner decision 2026-10-14): they run to +360 h (3-hourly to 144, 6-hourly after:
85 steps); 06Z/18Z stop at +144 h. Every step of a run appears at once ~7 h 34 min after the cycle.
Completeness: every needed .grib2 AND .index of BOTH streams present in one listing per stream (the GFS rule).
"""
import json
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import fetch as F

S3 = "https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com"
HTTP = "https://data.ecmwf.int/forecasts"
STEPS = list(range(0, 145, 3)) + list(range(150, 361, 6))
STEP_SCHEDULE = [[0, 144, 3], [150, 360, 6]]    # [first, last, every] in hours, for the manifest
CYCLES = (0, 12)
WAVE_PARAMS = ("swh", "pp1d", "mwd")
OPER_PARAMS = ("10u", "10v", "msl")
STREAMS = {"wave": WAVE_PARAMS, "oper": OPER_PARAMS}
AWS_TRIES = 2                                   # quick tries on the mirror for a NON-throttling error before the second host
AWS_COOLDOWN_S = 600                            # after an AWS outage (not throttling), reads go straight to data.ecmwf.int this long
_aws_down_until = [0.0]                         # (G30a F6: a failing mirror cost every request the whole ladder: > 60 min a run)
# A THROTTLED mirror (2026-10-15 12Z: "503 Slow Down" on most reads for over half an hour after the release, while
# data.ecmwf.int answered at once): the read goes to data.ecmwf.int straight away and the mirror is left alone this long.
# The 12Z build that waited on the mirror instead published 16 of 85 steps in 37 min (the job's 60-min limit killed it).
AWS_THROTTLE_COOLDOWN_S = 300
_aws_slow_until = [0.0]
# Throttling (2026-10-15: the first 00Z build failed at 07:41 UTC). The mirror answers "503 Slow Down" often (6 of ~20
# requests in a quiet check), most of all right after a cycle is released; data.ecmwf.int answers 429 to several reads at
# once. Throttling is never an outage. A throttled MIRROR read moves to data.ecmwf.int at once (and the mirror rests
# AWS_THROTTLE_COOLDOWN_S); on data.ecmwf.int (two reads at a time) a throttled read is retried with a growing pause
# (Retry-After honoured, capped). A read still throttled there after the whole ladder raises Throttled, a NotReady: the
# job exits 3 and the next tick (10 min) starts again, with no failure record and no back-off.
THROTTLE_CODES = (429, 503)
THROTTLE_WAITS_S = (5, 10, 20, 30, 45, 60)      # pauses between throttled tries of one read (~170 s in all)
RETRY_AFTER_MAX_S = 60
OTHER_WAITS_S = (2, 4)                          # pauses between tries of any other transport error
HTTP_SLOTS = threading.BoundedSemaphore(2)      # concurrent reads on data.ecmwf.int
UA = "allshoresurf.com model frames (https://allshoresurf.com)"


class Throttled(F.NotReady):
    """Every try of a read was throttled (429 / 503) on every host: not ready now, try again next tick."""
READ_WORKERS = 5                                # one step's records are read at once: the mirror is in Frankfurt, each read
                                                # is latency- and per-connection-bound (sequential: ~8 s a step on the runner)

NotReady = F.NotReady
TransportError = F.TransportError


def _prefix(run_dt, stream):
    return f"{run_dt:%Y%m%d}/{run_dt:%H}z/ifs/0p25/{stream}/"


def stem(run_dt, step, stream):
    """Object key without the extension."""
    return f"{_prefix(run_dt, stream)}{run_dt:%Y%m%d%H}0000-{step}h-{stream}-fc"


def needed_keys(run_dt):
    keys = []
    for s in STEPS:
        for stream in STREAMS:
            k = stem(run_dt, s, stream)
            keys += [k + ".grib2", k + ".index"]
    return keys


def missing_objects(run_dt):
    present = set()
    for stream in STREAMS:
        present |= set(F.list_keys(_prefix(run_dt, stream), s3=S3, request=_request))
    return [k for k in needed_keys(run_dt) if k not in present]


def run_is_complete(run_dt):
    return not missing_objects(run_dt)


def candidate_runs(now=None, lookback_cycles=4):
    """The newest `lookback_cycles` 00Z / 12Z cycles at or before `now`, newest first."""
    now = (now or datetime.now(timezone.utc)).replace(minute=0, second=0, microsecond=0)
    cand = now.replace(hour=12 if now.hour >= 12 else 0)
    return [cand - timedelta(hours=12 * k) for k in range(lookback_cycles)]


def latest_complete_run(now=None, lookback_cycles=4):
    """Newest 00Z/12Z cycle with every needed object present. Transport failures raise (never 'absent')."""
    for cand in candidate_runs(now, lookback_cycles):
        if run_is_complete(cand):
            return cand
    return None


def parse_index(text):
    """The .index's JSON lines -> list of dicts (blank lines skipped; a malformed line raises)."""
    out = []
    for line in text.splitlines():
        if line.strip():
            rec = json.loads(line)
            if not isinstance(rec, dict):
                raise ValueError("index line is not an object")
            out.append(rec)
    return out


def select(index, params, run_dt, step, stream):
    """{param: (offset, length)} for the requested params. Guards: each param exactly once, a surface record of
    this cycle, step and stream, a sane span."""
    hits = {}
    for rec in index:
        p = rec.get("param")
        if p not in params:
            continue
        if p in hits:
            raise RuntimeError(f"duplicate record for {p} in the {stream} index of step {step}")
        if (str(rec.get("date")) != f"{run_dt:%Y%m%d}" or str(rec.get("time")) != f"{run_dt:%H}00"
                or str(rec.get("step")) != str(step) or rec.get("stream") != stream or rec.get("levtype") != "sfc"
                or rec.get("type") != "fc"):
            raise RuntimeError(f"index record for {p} is not {stream} fc sfc {run_dt:%Y%m%d %H}z step {step}: {rec}")
        off, n = rec.get("_offset"), rec.get("_length")
        if not (isinstance(off, int) and isinstance(n, int) and off >= 0 and 0 < n < 64 * 2 ** 20):
            raise RuntimeError(f"bad span for {p}: {off}/{n}")
        hits[p] = (off, n)
    missing = [p for p in params if p not in hits]
    if missing:
        raise RuntimeError(f"records not in the {stream} index of step {step}: {missing}")
    return hits


def _open(url, rng=None, timeout=60):
    """One HTTP attempt -> (status, body). Raises urllib's errors (the retry policy is _request's)."""
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


def _request(url, rng=None, other_tries=None, throttle_waits=THROTTLE_WAITS_S):
    """GET with ECMWF's retry policy: 403/404 -> NotReady at once; 429/503 -> retried after `throttle_waits` (or the
    server's Retry-After), then Throttled (no waits: Throttled at the first 429/503); any other HTTP or transport error ->
    `other_tries` tries (default len(OTHER_WAITS_S) + 1), then TransportError."""
    other_tries = len(OTHER_WAITS_S) + 1 if other_tries is None else other_tries
    throttled = others = 0
    while True:
        try:
            return _open(url, rng)
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                raise F.NotReady(f"{e.code} {url}") from None
            if e.code in THROTTLE_CODES:
                if throttled >= len(throttle_waits):
                    raise Throttled(f"{url}: HTTP {e.code} (throttled) after {throttled + 1} tries") from None
                wait = _retry_after(e)
                time.sleep(throttle_waits[throttled] if wait is None else wait)
                throttled += 1
                continue
            last = f"HTTP {e.code}"
        except Exception as e:                       # noqa: BLE001 - timeouts, resets, DNS
            last = repr(e)
        others += 1
        if others >= other_tries:
            raise F.TransportError(f"{url}: {last} after {others} tries")
        time.sleep(OTHER_WAITS_S[min(others - 1, len(OTHER_WAITS_S) - 1)])


def _get(rel, rng=None):
    """GET a relative path from the AWS mirror, then from data.ecmwf.int (two reads at a time there); the first error
    wins when both fail (a 404/403 is NotReady and a throttled read Throttled: the job exits 3). The mirror gets no
    throttle ladder: a throttled mirror read goes to data.ecmwf.int at once and the mirror is skipped for
    AWS_THROTTLE_COOLDOWN_S; after an AWS outage it is skipped for AWS_COOLDOWN_S (its error is then not seen; the second
    host's is raised)."""
    first = None
    now = time.monotonic()
    hosts = (HTTP,) if now < max(_aws_down_until[0], _aws_slow_until[0]) else (S3, HTTP)
    for host in hosts:
        try:
            if host == S3:
                return _request(f"{host}/{rel}", rng=rng, other_tries=AWS_TRIES, throttle_waits=())
            with HTTP_SLOTS:
                return _request(f"{host}/{rel}", rng=rng)
        except (F.NotReady, F.TransportError) as exc:
            if host == S3 and isinstance(exc, F.TransportError):
                _aws_down_until[0] = time.monotonic() + AWS_COOLDOWN_S
            elif host == S3 and isinstance(exc, Throttled):
                _aws_slow_until[0] = time.monotonic() + AWS_THROTTLE_COOLDOWN_S
            first = first or exc
    raise first


def fetch_records(run_dt, step, stream, params):
    """{param: GRIB message bytes} for one step of one stream (ranged reads from the .index's exact spans)."""
    rel = stem(run_dt, step, stream)
    _status, body = _get(rel + ".index")
    spans = select(parse_index(body.decode()), params, run_dt, step, stream)
    def read(item):
        p, (off, n) = item
        status, blob = _get(rel + ".grib2", rng=f"bytes={off}-{off + n - 1}")
        if status != 206:
            raise RuntimeError(f"expected 206 for {p} at step {step}, got {status}")
        if len(blob) != n:
            raise RuntimeError(f"short/long read for {p}: {len(blob)} != {n}")
        if blob[:4] != b"GRIB" or blob[-4:] != b"7777":
            raise RuntimeError(f"record {p} is not a single GRIB message")
        return p, blob
    with ThreadPoolExecutor(min(READ_WORKERS, len(spans))) as pool:
        return dict(pool.map(read, spans.items()))
