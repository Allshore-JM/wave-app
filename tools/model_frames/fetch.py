"""NOAA open-data access for the overlay job: run discovery + GRIB record byte-range reads.

Only the anonymous S3 bucket is used (never NOMADS, which rate-limits at 120 hits/min).
"""
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

S3 = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"
STEPS = list(range(0, 241, 3))          # 81 frames, 0..+240 h every 3 h
MAX_STEP = STEPS[-1]


def wave_url(run_dt, step):
    d, h = run_dt.strftime("%Y%m%d"), run_dt.strftime("%H")
    return f"{S3}/gfs.{d}/{h}/wave/gridded/gfswave.t{h}z.global.0p25.f{step:03d}.grib2"


def atmos_url(run_dt, step):
    d, h = run_dt.strftime("%Y%m%d"), run_dt.strftime("%H")
    return f"{S3}/gfs.{d}/{h}/atmos/gfs.t{h}z.pgrb2.0p25.f{step:03d}"


def _get(url, rng=None, timeout=60, tries=4):
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={"Range": rng} if rng else {})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise
            last = e
        except Exception as e:                       # noqa: BLE001 - transient network
            last = e
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"fetch failed after {tries} tries: {url} ({last})")


def exists(url):
    try:
        urllib.request.urlopen(urllib.request.Request(url, method="HEAD"), timeout=30)
        return True
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            return False
        raise
    except Exception:                                # noqa: BLE001
        return False


def run_is_complete(run_dt):
    """A run is usable only when BOTH products expose the LAST step we need."""
    return exists(wave_url(run_dt, MAX_STEP) + ".idx") and exists(atmos_url(run_dt, MAX_STEP) + ".idx")


def latest_complete_run(now=None, lookback_cycles=8):
    now = now or datetime.now(timezone.utc)
    now = now.replace(minute=0, second=0, microsecond=0)
    for back in range(lookback_cycles):
        cand = now - timedelta(hours=back * 6)
        cand = cand.replace(hour=(cand.hour // 6) * 6)
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
    """{key: grib message bytes} for the records whose 'VAR:level' is in keys."""
    idx = parse_idx(_get(url + ".idx").decode())
    out = {}
    for i, (off, key) in enumerate(idx):
        if key in keys and key not in out:
            end = idx[i + 1][0] - 1 if i + 1 < len(idx) else ""
            out[key] = _get(url, rng=f"bytes={off}-{end}")
    missing = [k for k in keys if k not in out]
    if missing:
        raise RuntimeError(f"records not in index of {url}: {missing}")
    return out
