"""GFS winds at the forecast stations, from NOAA's spectra archive (plan section 53 B).

The forecast table's Wind column is GFS's 10 m wind at the station, the one quoted line per hour of the station's
WAVEWATCH III spectra file (`gfswave.<id>.spec`). NOAA's AWS copy (noaa-gfs-bdp-pds) keeps those files only inside the
run's archive `gfs.YYYYMMDD/HH/wave/station/gfswave.tHHz.spec_tar.gz` (~1.7 GB gzip of a ~7.1 GB tar, 918 members of
7.75 MB: 385 hourly steps x 50 x 36 spectral values, plus the point line with U10 and its direction). The site used to
download each station's whole .spec from NOMADS, so a NOMADS block of the server's address blanked the column.

This job streams the archive once per run (gzip -> tar in stream mode, one member in memory at a time), keeps the point
line of every step of every member (the site's own parse rule, `spec_point_winds`), and publishes under
gfswave/winds/v1/ (all immutable but the pointer, written last):

  <RUN>/winds.bin    count x 385 x 2 little-endian uint16: speed in 0.01 m/s, direction (FROM, degrees true) in
                     0.1 deg; 65535 = missing. The .spec prints 2 and 1 decimals, so the values round-trip exactly.
  <RUN>/index.json   {"format": "gfswinds-v1", "run", "steps": [0..384], "stations": {sid: slot}, "count",
                     "record_bytes": 1540, "missing": 65535, "units", "sha256" (of winds.bin), "members",
                     "left_out", "built_utc", "source"}
  latest.json        {"run", "published_utc", "index", "stations", "steps"}  (max-age 300)

Exit codes (the frames jobs' contract): 0 published or already live; 3 no complete run yet (or NOAA not serving it,
or the build budget ran out: the next tick retries); 1 a failure (recorded under failed/<RUN>.json, retried after an
hour, at most 6 times) or a stale alarm (the live run 18 h old or more: at most once every 6 h).

  python tools/model_frames/gfs_winds.py                    # the newest complete run, if not live yet
  python tools/model_frames/gfs_winds.py --run 2026102512   # that run
  python tools/model_frames/gfs_winds.py --dry-run          # build, publish nothing (a summary only)
  python tools/model_frames/gfs_winds.py --local DIR        # build from DIR/<archive name> (no network), dry run
"""
import argparse
import gzip
import hashlib
import json
import os
import re
import struct
import sys
import tarfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import publish as P  # noqa: E402

S3 = "https://noaa-gfs-bdp-pds.s3.amazonaws.com"
PREFIX = P.GFSWINDS_PREFIX
FORMAT = "gfswinds-v1"
STEPS = 385                                  # hourly 0..384
MISSING = 65535
RECORD_BYTES = STEPS * 2 * 2
KEEP = 4                                     # the SWAN back-fill reads a run 6-12 h old
LOOKBACK_CYCLES = 4
MIN_ARCHIVE_BYTES = 10 ** 9                  # the real archive is ~1.7 GB; anything far smaller is not a complete run
MEMBER_MIN, MEMBER_MAX = 1, 32 * 1024 * 1024
MIN_MEMBERS = 500                            # 918 today; a much shorter archive is a layout change, not a run
BUILD_BUDGET_S = 20 * 60
STALL_S = 60                                 # a read that waits this long fails (socket timeout)
FAILED_RETRY_AFTER_S = 3600
FAILED_MAX_ATTEMPTS = 6
UA = {"User-Agent": "allshoresurf.com GFS winds job (https://allshoresurf.com)"}
NAME_RE = re.compile(r"\./gfswave\.([A-Za-z0-9_-]{1,32})\.spec")
# A datetime line, any blank lines, then the first quoted line: '51201     '  21.67-158.12  472.7  8.09  71.6 ...
POINT_RE = re.compile(rb"^[ \t]*(\d{8})[ \t]+(\d{6})[ \t]*\r?\n(?:[ \t]*\r?\n)*[ \t]*'[^'\n]*'([^\n]*)", re.M)
FLOAT_RE = re.compile(rb"-?\d+\.\d+")


class NotReady(Exception):
    """NOAA has not (or no longer) the archive: exit 3, the next tick retries."""


class TransportError(Exception):
    """The download failed after its tries: a failure record."""


class GfsWindsSource:
    """What run._stale_check needs to know of this job."""
    name = "NOAA"
    tag = "GFS winds "
    prefix = PREFIX
    stale_fail_h = 18                        # three missed cycles


SRC = GfsWindsSource()


def archive_key(run_dt):
    return f"gfs.{run_dt:%Y%m%d}/{run_dt:%H}/wave/station/gfswave.t{run_dt:%H}z.spec_tar.gz"


def run_name(run_dt):
    return f"{run_dt:%Y%m%d%H}"


def index_key(run):
    return f"{PREFIX}/{run}/index.json"


def winds_key(run):
    return f"{PREFIX}/{run}/winds.bin"


def candidate_runs(now=None, lookback=LOOKBACK_CYCLES):
    now = (now or datetime.now(timezone.utc)).replace(minute=0, second=0, microsecond=0)
    out = []
    for back in range(lookback):
        c = now - timedelta(hours=6 * back)
        out.append(c.replace(hour=(c.hour // 6) * 6))
    return out


def head_size(url, tries=3, opener=urllib.request.urlopen):
    """The object's size, or None when NOAA answers 403 / 404 (not there). Raises TransportError after its tries."""
    last = None
    for attempt in range(tries):
        try:
            with opener(urllib.request.Request(url, headers=UA, method="HEAD"), timeout=30) as r:
                return int(r.headers.get("Content-Length") or 0)
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return None
            last = f"HTTP {e.code}"
        except Exception as e:                       # noqa: BLE001 - timeouts, resets, DNS
            last = repr(e)
        time.sleep(2 * (attempt + 1))
    raise TransportError(f"HEAD {url}: {last} after {tries} tries")


def newest_complete(now=None, head=head_size, s3=S3):
    """(run_dt, size) of the newest cycle whose spectra archive is there and plausibly whole, or (None, None).
    NOAA writes the archive whole or not at all (an S3 object appears complete); the size floor guards anyway."""
    for cand in candidate_runs(now):
        size = head(f"{s3}/{archive_key(cand)}")
        if size is not None and size >= MIN_ARCHIVE_BYTES:
            return cand, size
    return None, None


def spec_point_winds(data):
    """{naive UTC datetime: (u10 m/s, direction FROM deg)} from one .spec file's bytes: the first quoted line after each
    `YYYYMMDD HHMMSS` line, its 4th and 5th floats (the site's app._parse_spec_wind_text, pinned by a parity test).
    A later duplicate time wins, as in a dict built line by line."""
    out = {}
    for m in POINT_RE.finditer(data):
        vals = FLOAT_RE.findall(m.group(3))
        if len(vals) < 5:
            continue
        try:
            when = datetime.strptime((m.group(1) + m.group(2)).decode(), "%Y%m%d%H%M%S")
            u, d = float(vals[3]), float(vals[4])
        except ValueError:
            continue
        if u == u and d == d and abs(u) != float("inf") and abs(d) != float("inf"):
            out[when] = (u, d)
    return out


def encode_station(winds, run_dt):
    """(record bytes, steps present): the 385 steps of one station, speed and direction per step."""
    base = run_dt.replace(tzinfo=None)
    vals = [MISSING] * (STEPS * 2)
    present = 0
    for when, (u, d) in winds.items():
        h = (when - base).total_seconds() / 3600
        if h != int(h) or not 0 <= h < STEPS:
            continue
        su, sd = round(u * 100), round(d * 10)
        if not (0 <= su < MISSING and 0 <= sd <= 3600):
            continue
        i = int(h) * 2
        if vals[i] == MISSING:
            present += 1
        vals[i], vals[i + 1] = su, sd
    return struct.pack(f"<{STEPS * 2}H", *vals), present


def decode_record(rec):
    """[(speed m/s, dir deg) or None] * 385 of one station's record (tests and the read-back check)."""
    vals = struct.unpack(f"<{STEPS * 2}H", rec)
    return [None if vals[2 * i] == MISSING else (vals[2 * i] / 100, vals[2 * i + 1] / 10) for i in range(STEPS)]


class _Counting:
    def __init__(self, raw):
        self.raw, self.bytes_read = raw, 0

    def read(self, n=-1):
        b = self.raw.read(n)
        self.bytes_read += len(b)
        return b


def build(stream, run_dt, deadline=None, log=print):
    """(winds.bin bytes, index document without sha256) from one archive stream (gzip of a tar). ValueError for a
    member the job cannot trust (name, type, size), TimeoutError past `deadline` (time.monotonic())."""
    tf = tarfile.open(fileobj=gzip.GzipFile(fileobj=stream), mode="r|")
    body, stations, left_out, members = bytearray(), {}, [], 0
    for m in tf:
        if deadline is not None and time.monotonic() > deadline:
            raise TimeoutError("the build budget ran out")
        hit = NAME_RE.fullmatch(m.name)
        if not m.isreg() or not hit:
            raise ValueError(f"{m.name}: not a regular file ./gfswave.<id>.spec")
        if not MEMBER_MIN <= m.size <= MEMBER_MAX:
            raise ValueError(f"{m.name}: size {m.size}")
        sid = hit.group(1)
        if sid in stations:
            raise ValueError(f"{m.name}: twice in the archive")
        data = tf.extractfile(m).read()
        if len(data) != m.size:
            raise ValueError(f"{m.name}: {len(data)} of {m.size} bytes")
        members += 1
        rec, present = encode_station(spec_point_winds(data), run_dt)
        if present == 0:
            left_out.append(sid)
            continue
        stations[sid] = len(stations)
        body += rec
    tf.close()
    if members < MIN_MEMBERS:
        raise ValueError(f"{members} members: not a whole spectra archive")
    log(f"{run_name(run_dt)}: {members} members, {len(stations)} stations with wind, {len(left_out)} without")
    doc = {"format": FORMAT, "run": run_name(run_dt), "steps": list(range(STEPS)), "stations": stations,
           "count": len(stations), "record_bytes": RECORD_BYTES, "missing": MISSING,
           "units": {"speed": "0.01 m/s", "direction": "0.1 deg, FROM, true north"},
           "members": members, "left_out": sorted(left_out),
           "built_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": "GFS 10 m wind from the GFS-Wave station spectra (gfswave.tHHz.spec_tar.gz, NOAA's AWS copy)"}
    return bytes(body), doc


def open_archive(url, opener=urllib.request.urlopen):
    """The archive's response stream, or NotReady (403 / 404) / TransportError."""
    try:
        return opener(urllib.request.Request(url, headers=UA), timeout=STALL_S)
    except urllib.error.HTTPError as e:
        if e.code in (403, 404):
            raise NotReady(f"{e.code} {url}") from None
        raise TransportError(f"{url}: HTTP {e.code}") from None
    except Exception as e:                           # noqa: BLE001
        raise TransportError(f"{url}: {e!r}") from None


def publish(store, run, body, doc):
    """winds.bin, index.json, then the pointer (last)."""
    doc = dict(doc, sha256=hashlib.sha256(body).hexdigest())
    store.put(winds_key(run), body, "application/octet-stream", P.IMMUTABLE)
    store.put(index_key(run), json.dumps(doc, separators=(",", ":")).encode(), "application/json", P.IMMUTABLE)
    pointer = {"run": run, "published_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "index": index_key(run), "stations": doc["count"], "steps": STEPS}
    store.put(P.latest_key(PREFIX), json.dumps(pointer, sort_keys=True).encode(), "application/json", P.POINTER)
    return pointer


def prune(store, keep=KEEP):
    """Keep the newest `keep` runs that have an index (and the one the pointer names); delete the others' objects.
    Only <PREFIX>/<10 digits>/ is ever touched."""
    latest = store.get_json(P.latest_key(PREFIX)) or {}
    runs = {}
    for k in store.list_keys(PREFIX + "/"):
        name, _, rest = k[len(PREFIX) + 1:].partition("/")
        if name.isdigit() and len(name) == 10 and rest:
            runs.setdefault(name, False)
            if rest == "index.json":
                runs[name] = True
    whole = sorted(r for r, ok in runs.items() if ok)
    protect = set(whole[-keep:]) | ({latest["run"]} if latest.get("run") else set())
    newest = whole[-1] if whole else None
    deleted = []
    for run in sorted(runs):
        if run in protect or (not runs[run] and newest and run > newest):
            continue                                 # a later build may still be writing
        store.delete_prefix(f"{PREFIX}/{run}/")
        deleted.append(run)
    return deleted


def _skip_failed(store, run):
    rec = store.get_json(P.failed_key(run, PREFIX))
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


def _stale(store, latest, situation):
    import run as R                                  # the frames jobs' stale alarm (one record, at most once every 6 h)
    return R._stale_check(store, SRC, latest, situation)


def _write_out(out, body, doc):
    if not out:
        return
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "winds.bin"), "wb") as f:
        f.write(body)
    with open(os.path.join(out, "index.json"), "w", encoding="utf-8") as f:
        json.dump(dict(doc, sha256=hashlib.sha256(body).hexdigest()), f)


def main(argv=None, opener=urllib.request.urlopen, head=None, store=None, now=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--run", help="YYYYMMDDHH (default: the newest complete run)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--local", help="a directory holding the archive file (no network; implies --dry-run)")
    ap.add_argument("--out", help="with --dry-run / --local: write winds.bin and index.json into this directory")
    a = ap.parse_args(argv)
    t0 = time.monotonic()
    head = head or (lambda url: head_size(url, opener=opener))
    if a.run and (len(a.run) != 10 or not a.run.isdigit() or a.run[8:] not in ("00", "06", "12", "18")):
        print(f"{a.run}: not YYYYMMDDHH of a GFS cycle")
        return 2
    if a.local:
        run_dt = datetime.strptime(a.run, "%Y%m%d%H").replace(tzinfo=timezone.utc) if a.run else None
        if run_dt is None:
            print("--local needs --run")
            return 2
        with open(os.path.join(a.local, os.path.basename(archive_key(run_dt))), "rb") as f:
            body, doc = build(f, run_dt)
        _write_out(a.out, body, doc)
        print(f"local build: {doc['count']} stations, {len(body)} B (nothing published)")
        return 0
    live = not a.dry_run
    if live and store is None:
        store = _store_from_env()
    latest = (store.get_json(P.latest_key(PREFIX)) or {}) if live else {}
    try:
        if a.run:
            run_dt = datetime.strptime(a.run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
            size = head(f"{S3}/{archive_key(run_dt)}")
            if size is None or size < MIN_ARCHIVE_BYTES:
                print(f"{a.run}: no complete spectra archive on NOAA's AWS copy")
                return 3
        else:
            run_dt, size = newest_complete(now, head)
    except TransportError as exc:
        print(f"run discovery failed: {exc}")
        return 1
    if run_dt is None:
        print("no complete GFS spectra archive in the last 4 cycles")
        return (_stale(store, latest, "no complete GFS winds run found") or 3) if live else 3
    run = run_name(run_dt)
    if live and not a.run:
        if latest.get("run") == run:
            print(f"{run} already live")
            return _stale(store, latest, "the newest GFS winds run is the live one") or 0
        if latest.get("run", "") > run:
            print(f"{run} is older than the live run {latest['run']}: nothing to do")
            return 0
        if _skip_failed(store, run):
            print(f"{run} failed recently: waiting before another try")
            return 3
    print(f"{run}: building from {S3}/{archive_key(run_dt)} ({size} B)")
    try:
        raw = open_archive(f"{S3}/{archive_key(run_dt)}", opener)
        with raw:
            src = _Counting(raw)
            body, doc = build(src, run_dt, deadline=t0 + BUILD_BUDGET_S)
    except NotReady as exc:
        print(f"{run}: not served yet ({exc})")
        if live:
            P.record_notready(store, run, str(exc), PREFIX)
        return 3
    except TimeoutError as exc:
        print(f"{run}: {exc} after {time.monotonic() - t0:.0f} s; the next tick tries again")
        return 3
    except Exception as exc:                         # noqa: BLE001 - a layout change, a cut stream, a dead socket
        print(f"{run}: build failed: {exc.__class__.__name__}: {exc}")
        if live:
            P.record_failure(store, run, f"{exc.__class__.__name__}: {exc}", PREFIX)
        _summary(f"FAILED: GFS winds {run}: {exc.__class__.__name__}")
        return 1
    secs = time.monotonic() - t0
    line = (f"GFS winds {run}: {doc['count']} stations ({len(doc['left_out'])} without wind), {len(body)} B, "
            f"archive {src.bytes_read} B read, {secs:.0f} s")
    print(line)
    _summary(line)
    if not live:
        _write_out(a.out, body, doc)
        print("dry run: nothing published")
        return 0
    publish(store, run, body, doc)
    deleted = prune(store)
    print(f"published {run}; pruned {deleted or 'nothing'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
