"""NOAA open-data access for the overlay job: run discovery + GRIB record byte-range reads.

The anonymous S3 bucket is used. NOMADS (which rate-limits at 120 hits/min) only fills a gap: a run that is complete on
S3 but for at most NOMADS_MAX_GAPS objects, at least NOMADS_GRACE_H hours after its cycle, whose missing objects NOMADS
has, reads those few steps from NOMADS (2026-11-17: S3 lacked the 12Z run's atmos f119 while NOMADS had it, so the
overlay stayed on the 06Z run until 18Z was complete).

Completeness rule (G1 review): NOAA does not publish steps in order -- the first steps routinely
land 13-16 minutes after their neighbours -- so a run counts as complete only when EVERY needed
.idx AND .grib2 object is present for BOTH products (one ListObjectsV2 per product, verified from
the listing, not from HEADs on the last step). The full horizon (f384) is complete ~40 min after
f240 (27-47 min over five cycles measured 2026-09-26); a run goes live only when all of it is.
"""
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

S3 = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"
NOMADS = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
NOMADS_GRACE_H = 6                 # S3 completes a run ~4.5-5 h after its cycle: past this a gap is not lag but a hole
NOMADS_MAX_GAPS = 4                # objects (grib + idx) a run may borrow; more is an outage, not a hole
UA = {"User-Agent": "allshoresurf.com model frames job (https://allshoresurf.com)"}
BORROWED = {}                      # run name -> keys read from NOMADS (set by latest_complete_run)
# The models' full output (plan section 22): GFS-Wave 0.25 and GFS pgrb2 0.25 are hourly to +120 h, then every
# 3 h to +384 h (16 days). 209 frames.
STEPS = list(range(0, 121)) + list(range(123, 385, 3))
STEP_SCHEDULE = [[0, 120, 1], [123, 384, 3]]    # [first, last, every] in hours, for the manifest
MAX_STEP = STEPS[-1]
_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


class NotReady(Exception):
    """A record/object the run needs is not (yet) on the bucket. Retry later, not an error."""


class TransportError(Exception):
    """Persistent network/5xx failure. Fail loud; never treat as 'absent'."""


def _wave_prefix(run_dt):
    d, h = run_dt.strftime("%Y%m%d"), run_dt.strftime("%H")
    return f"gfs.{d}/{h}/wave/gridded/gfswave.t{h}z.global.0p25.f"


def _atmos_prefix(run_dt):
    d, h = run_dt.strftime("%Y%m%d"), run_dt.strftime("%H")
    return f"gfs.{d}/{h}/atmos/gfs.t{h}z.pgrb2.0p25.f"


def _host(run_dt, key):
    """S3, or NOMADS for an object of this run that S3 lacks (BORROWED); the grib and its .idx go together."""
    gaps = BORROWED.get(f"{run_dt:%Y%m%d%H}", ())
    return NOMADS if (key in gaps or key + ".idx" in gaps) else S3


def wave_url(run_dt, step):
    key = f"{_wave_prefix(run_dt)}{step:03d}.grib2"
    return f"{_host(run_dt, key)}/{key}"


def atmos_url(run_dt, step):
    key = f"{_atmos_prefix(run_dt)}{step:03d}"
    return f"{_host(run_dt, key)}/{key}"


def needed_keys(run_dt):
    """Every object key (grib + idx) the job will read for this run."""
    keys = []
    for s in STEPS:
        w, a = f"{_wave_prefix(run_dt)}{s:03d}.grib2", f"{_atmos_prefix(run_dt)}{s:03d}"
        keys += [w, w + ".idx", a, a + ".idx"]
    return keys


def _request(url, rng=None, timeout=60, tries=4, method="GET"):
    """HTTP with retries on transport errors and 5xx. Returns (status, body). 404 -> NotReady."""
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers=dict(UA, **({"Range": rng} if rng else {})), method=method)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, (r.read() if method == "GET" else b"")
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                raise NotReady(f"{e.code} {url}") from None
            last = f"HTTP {e.code}"
        except Exception as e:                       # noqa: BLE001 - timeouts, resets, DNS
            last = repr(e)
        time.sleep(2 * (attempt + 1))
    raise TransportError(f"{url}: {last} after {tries} tries")


def list_keys(prefix, max_keys=1000, s3=S3, request=None):
    """Object keys under a prefix (ListObjectsV2, paginated) of a public bucket (NOAA's by default; the ECMWF job
    passes its own, plan section 43)."""
    keys, token = [], None
    while True:
        url = f"{s3}/?list-type=2&prefix={prefix}&max-keys={max_keys}"
        if token:
            from urllib.parse import quote
            url += "&continuation-token=" + quote(token, safe="")
        _status, body = (request or _request)(url)
        root = ET.fromstring(body)
        keys += [c.find(_NS + "Key").text for c in root.findall(_NS + "Contents")]
        nxt = root.find(_NS + "NextContinuationToken")
        if root.find(_NS + "IsTruncated").text == "true" and nxt is not None:
            token = nxt.text
        else:
            return keys


def missing_objects(run_dt):
    """Needed keys that are NOT on the bucket yet (both products)."""
    present = set(list_keys(_wave_prefix(run_dt))) | set(list_keys(_atmos_prefix(run_dt)))
    return [k for k in needed_keys(run_dt) if k not in present]


def run_is_complete(run_dt):
    return not missing_objects(run_dt)


def candidate_runs(now=None, lookback_cycles=8):
    now = now or datetime.now(timezone.utc)
    now = now.replace(minute=0, second=0, microsecond=0)
    out = []
    for back in range(lookback_cycles):
        cand = now - timedelta(hours=back * 6)
        out.append(cand.replace(hour=(cand.hour // 6) * 6))
    return out


def nomads_has(key, request=None):
    """True when NOMADS answers 200 to a HEAD of the object; False on 403/404 or any failure (a gap stays a gap)."""
    try:
        status, _ = (request or _request)(f"{NOMADS}/{key}", method="HEAD", tries=2)
        return status == 200
    except Exception:                                # noqa: BLE001
        return False


def latest_complete_run(now=None, lookback_cycles=8, has=nomads_has):
    """Newest cycle with every needed object present on S3, or (NOMADS_GRACE_H after its cycle) every one but at most
    NOMADS_MAX_GAPS that NOMADS has: those are recorded in BORROWED and read from NOMADS. Transport failures raise
    (never 'absent')."""
    now = now or datetime.now(timezone.utc)
    for cand in candidate_runs(now, lookback_cycles):
        if run_is_complete(cand):
            return cand
        if now - cand < timedelta(hours=NOMADS_GRACE_H):
            continue                                 # still arriving: lag, not a hole
        miss = missing_objects(cand)
        if 0 < len(miss) <= NOMADS_MAX_GAPS and all(has(k) for k in miss):
            BORROWED[f"{cand:%Y%m%d%H}"] = set(miss)
            print(f"{cand:%Y%m%d%H}: S3 lacks {len(miss)} object(s), read from NOMADS instead: {', '.join(miss)}")
            return cand
    return None


def parse_idx(text):
    """[(byte_offset, 'VAR:level')] in file order."""
    out = []
    for line in text.splitlines():
        p = line.split(":")
        if len(p) < 5:
            continue
        out.append((int(p[1]), f"{p[3]}:{p[4]}"))
    return out


def fetch_records(url, keys):
    """{key: grib message bytes} for the records whose 'VAR:level' is in keys.

    Guards (G1): a key must match exactly ONE record; the ranged GET must answer 206 with
    exactly the span the index promised; the payload must be one GRIB message.
    """
    _status, body = _request(url + ".idx")
    idx = parse_idx(body.decode())
    hits = {}
    for i, (off, key) in enumerate(idx):
        if key in keys:
            if key in hits:
                raise RuntimeError(f"duplicate record for {key} in {url}.idx")
            hits[key] = i
    missing = [k for k in keys if k not in hits]
    if missing:
        raise RuntimeError(f"records not in index of {url}: {missing}")
    out = {}
    for key, i in hits.items():
        off = idx[i][0]
        end = idx[i + 1][0] - 1 if i + 1 < len(idx) else None
        rng = f"bytes={off}-{end if end is not None else ''}"
        status, blob = _request(url, rng=rng)
        if status != 206:
            raise RuntimeError(f"expected 206 for {rng} on {url}, got {status}")
        if end is not None and len(blob) != end - off + 1:
            raise RuntimeError(f"short/long read for {key}: {len(blob)} != {end - off + 1}")
        if blob[:4] != b"GRIB" or blob[-4:] != b"7777":
            raise RuntimeError(f"record {key} is not a single GRIB message")
        out[key] = blob
    return out
