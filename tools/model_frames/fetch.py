"""NOAA open-data access for the overlay job: run discovery + GRIB record byte-range reads.

Only the anonymous S3 bucket is used (never NOMADS, which rate-limits at 120 hits/min).

Completeness rule (G1 review): NOAA does not publish steps in order -- 0-10 of the 81 steps
routinely land 13-16 minutes after their neighbours -- so a run counts as complete only when
EVERY needed .idx AND .grib2 object is present for BOTH products (one ListObjectsV2 per
product, verified from the listing, not from HEADs on the last step).
"""
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

S3 = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"
STEPS = list(range(0, 241, 3))          # 81 frames, 0..+240 h every 3 h
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


def wave_url(run_dt, step):
    return f"{S3}/{_wave_prefix(run_dt)}{step:03d}.grib2"


def atmos_url(run_dt, step):
    return f"{S3}/{_atmos_prefix(run_dt)}{step:03d}"


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
            req = urllib.request.Request(url, headers={"Range": rng} if rng else {}, method=method)
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


def list_keys(prefix, max_keys=1000):
    """Object keys under a prefix (ListObjectsV2, paginated)."""
    keys, token = [], None
    while True:
        url = f"{S3}/?list-type=2&prefix={prefix}&max-keys={max_keys}"
        if token:
            from urllib.parse import quote
            url += "&continuation-token=" + quote(token, safe="")
        _status, body = _request(url)
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


def latest_complete_run(now=None, lookback_cycles=8):
    """Newest cycle with every needed object present. Transport failures raise (never 'absent')."""
    for cand in candidate_runs(now, lookback_cycles):
        if run_is_complete(cand):
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
