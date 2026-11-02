"""The "GFS Spectral" tables' product: our own swell split of every listed station's GFS-Wave spectra, per run
(plan section 77).

NOAA's bulletin merges two swells into one in a few percent of hours (a watershed on a smoothed spectrum). This job
streams the run's spectra archive (the same `gfswave.tHHz.spec_tar.gz` the winds job reads, ~1.7 GB, NOAA's AWS copy),
splits every listed station's 385 hourly spectra with `partition2d.partition_v2` in a process pool, and publishes one
binary per run under gfswave/spectral/v1/ (all immutable but the pointer, written last):

  <RUN>/spectral.bin   count x 385 x 20 little-endian uint16 per station: per step [hs_total 0.01 m, n parts, then
                       6 x (hs 0.01 m, tp 0.1 s, dir FROM 1 deg | flags << 12 (bit 12 = wind sea))]; 65535 = no part
                       (a step without a spectrum: hs_total 65535 too). The six most powerful parts (Hs^2 x Tp)
                       of 0.05 m or more.
  <RUN>/index.json     {"format": "gfsspec-v1", "run", "steps": [0..384], "stations": {sid: slot}, "count",
                       "record_bytes": 15400, "missing": 65535, "units", "rules" (the split's settings), "sha256"
                       (of spectral.bin), "members", "left_out": {sid: why}, "nproc", "workers", "build_s",
                       "built_utc", "source"}
  latest.json          {"run", "published_utc", "index", "stations", "steps"}  (max-age 300)

Exit codes as the winds job: 0 published / already live; 3 no complete run yet, NOAA not serving it, or the budget
ran out (the next tick retries); 1 a failure (failed/<RUN>.json, retried after an hour, at most 6 times) or the stale
alarm. The winds job is untouched: its pointer flips first (its build is a minute), this one 5-15 minutes later.

  python tools/model_frames/gfs_spectral.py                    # the newest complete run, if not live yet
  python tools/model_frames/gfs_spectral.py --run 2026110112   # that run
  python tools/model_frames/gfs_spectral.py --dry-run --out D  # build, publish nothing, write the files to D
  python tools/model_frames/gfs_spectral.py --local DIR        # build from DIR/<archive name> (no network)
"""
import argparse
import hashlib
import json
import multiprocessing
import os
import struct
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import gfs_winds as W  # noqa: E402
import publish as P  # noqa: E402

PREFIX = P.GFSSPEC_PREFIX
FORMAT = "gfsspec-v1"
STEPS = W.STEPS
MISSING = 65535
PARTS = 6
VALUES_PER_STEP = 2 + PARTS * 3
RECORD_BYTES = STEPS * VALUES_PER_STEP * 2
FLAG_SHIFT = 12                              # the direction keeps bits 0-11 (0..359), the flags bits 12-15
FLAG_WIND_SEA = 1
MIN_PART_HS = 0.05
KEEP = 4
BUILD_BUDGET_S = 25 * 60
MAX_LEFT_OUT_SHARE = 0.05                    # more listed members unreadable than this = a failure, not a product
MAX_SHIFTED_SHARE = 0.10                     # members not starting at the run's hour = another run's archive (G36a P3-3)
TASK_MAX_S = 300                             # a member's split takes seconds; no answer in this long = the worker is gone
STATIONS_PATH = os.path.join(os.path.dirname(os.path.dirname(HERE)), "station_list.json")
S3 = W.S3
NotReady, TransportError, BudgetExceeded = W.NotReady, W.TransportError, W.BudgetExceeded


class GfsSpectralSource:
    name = "NOAA"
    tag = "GFS spectral "
    prefix = PREFIX
    stale_fail_h = 18


SRC = GfsSpectralSource()


def index_key(run):
    return f"{PREFIX}/{run}/index.json"


def bin_key(run):
    return f"{PREFIX}/{run}/spectral.bin"


def listed_stations(path=STATIONS_PATH):
    with open(path, encoding="utf-8") as f:
        ids = json.load(f)
    if not isinstance(ids, list) or not all(isinstance(s, str) and s for s in ids):
        raise ValueError(f"{path}: not a list of station ids")
    return set(ids)


# ---------------------------------------------------------------- one station

def power(p):
    return p["hs"] * p["hs"] * p["tp"]


def encode_steps(rows, run_dt):
    """rows = {naive datetime: (hs_total, [part dicts])} -> (the station's record bytes, steps present). Parts beyond
    the six most powerful, or under MIN_PART_HS, are left out; a step the member lacks is all MISSING."""
    import partition2d as P2  # noqa: F401 (the worker imports it; here only for the rules' sake)
    base = run_dt.replace(tzinfo=None)
    vals = [MISSING] * (STEPS * VALUES_PER_STEP)
    present = 0
    for when, (hs_total, parts) in rows.items():
        h = (when - base).total_seconds() / 3600
        if h != int(h) or not 0 <= h < STEPS:
            continue
        keep = sorted((p for p in parts if p["hs"] >= MIN_PART_HS), key=lambda p: (-power(p), p["label"]))[:PARTS]
        i = int(h) * VALUES_PER_STEP
        st = min(MISSING - 1, round(hs_total * 100))
        vals[i] = max(0, st)
        vals[i + 1] = len(keep)
        for k, p in enumerate(keep):
            hs = min(MISSING - 1, max(0, round(p["hs"] * 100)))
            tp = min(MISSING - 1, max(0, round(p["tp"] * 10)))
            d = int(round(p["dir"])) % 360
            flags = FLAG_WIND_SEA if p["ws"] else 0
            j = i + 2 + 3 * k
            vals[j], vals[j + 1], vals[j + 2] = hs, tp, d | (flags << FLAG_SHIFT)
        present += 1
    return struct.pack(f"<{STEPS * VALUES_PER_STEP}H", *vals), present


def decode_record(rec):
    """[(hs_total m, [(hs, tp, dir, wind_sea) ...]) or None] * 385 (the site's reader and the tests)."""
    vals = struct.unpack(f"<{STEPS * VALUES_PER_STEP}H", rec)
    out = []
    for s in range(STEPS):
        i = s * VALUES_PER_STEP
        if vals[i] == MISSING:
            out.append(None)
            continue
        parts = []
        for k in range(vals[i + 1]):
            j = i + 2 + 3 * k
            hs, tp, df = vals[j], vals[j + 1], vals[j + 2]
            parts.append((hs / 100, tp / 10, df & ((1 << FLAG_SHIFT) - 1), bool((df >> FLAG_SHIFT) & FLAG_WIND_SEA)))
        out.append((vals[i] / 100, parts))
    return out


def station_record(sid, data, run_dt):
    """The pool's work: (sid, record bytes or None, steps present, error text or None, the member's first step time
    or None). The first time lets the build refuse another run's archive under this run's name (G48a P2-1)."""
    try:
        import partition2d as P2
        import ww3spec
        sp = ww3spec.read(data)
        rows = {}
        for when, E, u, ud in sp.steps:
            parts, _ = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud))
            rows[when] = (ww3spec.hs_of(sp.freqs, sp.dirs, E), parts)
        rec, present = encode_steps(rows, run_dt)
        return sid, rec, present, None, (min(rows) if rows else None)
    except Exception as exc:                 # noqa: BLE001 - reported per station, never fatal to the build
        return sid, None, 0, f"{exc.__class__.__name__}: {exc}"[:300], None


# ---------------------------------------------------------------- the build

class _Bounded:
    """apply_async behind a bounded number of outstanding members, so the archive stream (one member in memory)
    never runs ahead of the pool by more than `cap` members (the Plan critique: an unbounded feeder would hold the
    whole 5.6 GB stream). The callback releases a slot; `peak` is the most outstanding at once (a test pins it)."""

    def __init__(self, pool, cap):
        self.pool, self.sem, self.lock = pool, threading.BoundedSemaphore(cap), threading.Lock()
        self.results, self.outstanding, self.peak, self.pending, self.lost = [], 0, 0, [], 0

    def submit(self, func, args, deadline=None):
        while not self.sem.acquire(timeout=1.0):     # a slot; the deadline is honoured while waiting (G48a P3-2)
            if deadline is not None and time.monotonic() > deadline:
                raise BudgetExceeded("the build budget ran out")
        with self.lock:
            self.outstanding += 1
            self.peak = max(self.peak, self.outstanding)

        def done(res):
            with self.lock:
                self.results.append(res)
                self.outstanding -= 1
            self.sem.release()

        def failed(exc):                     # only a worker that died: the function itself never raises
            done((args[0], None, 0, f"worker failed: {exc!r}"[:300], None))

        self.pending.append((args[0], self.pool.apply_async(func, args, callback=done, error_callback=failed)))

    def drain(self, deadline=None):
        """Every answer, in submission order. A task with no answer within TASK_MAX_S of its turn (a worker killed by
        the OOM killer or a segfault: multiprocessing then calls neither callback, G48a P3-1) is reported as that
        station's failure and left out; past the deadline the budget is spent."""
        for sid, r in self.pending:
            left = None if deadline is None else max(0.0, deadline - time.monotonic())
            r.wait(TASK_MAX_S if left is None else min(left, TASK_MAX_S))
            if r.ready():
                continue
            if deadline is not None and time.monotonic() > deadline:
                raise BudgetExceeded("the build budget ran out")
            with self.lock:
                self.results.append((sid, None, 0, f"worker failed: no answer within {TASK_MAX_S} s", None))
                self.lost += 1
        return list(self.results)


def default_pool(workers):
    return multiprocessing.Pool(workers)


def build(stream, run_dt, listed, deadline=None, workers=None, pool_factory=default_pool, log=print):
    """(spectral.bin bytes, index document without sha256). Members of unlisted stations are skipped unread;
    listed ones go through the pool; a station the worker could not read is left out (recorded)."""
    t0 = time.monotonic()
    workers = workers or os.cpu_count() or 1
    pool = pool_factory(workers)
    box = _Bounded(pool, 2 * workers)
    count = 0
    seen = []
    try:
        for sid, data in W.members(stream, deadline):
            count += 1
            if sid not in listed:
                continue
            seen.append(sid)
            box.submit(station_record, (sid, data, run_dt), deadline)
        results = box.drain(deadline)
    except BaseException:
        pool.terminate()
        raise
    if box.lost:                                       # a lost task stays in the pool's cache: join() would never return
        pool.terminate()
    else:
        pool.close()
        pool.join()
    if count < W.MIN_MEMBERS:
        raise ValueError(f"{count} members: not a whole spectra archive")
    by_sid = {r[0]: r for r in results}
    base = run_dt.replace(tzinfo=None)
    shifted = sum(1 for r in results if r[4] is not None and r[4] != base)
    if shifted > MAX_SHIFTED_SHARE * max(1, len(seen)):   # another run's archive under this run's name (G48a P2-1)
        raise ValueError(f"{shifted} of {len(seen)} listed members do not start at the run {W.run_name(run_dt)}")
    body, stations, left_out, steps_present = bytearray(), {}, {}, 0
    for sid in seen:                                   # the archive's order
        _, rec, present, err, _ = by_sid[sid]
        if rec is None or present == 0:
            left_out[sid] = err or "no steps of the run"
            continue
        stations[sid] = len(stations)
        body += rec
        steps_present += present
    missing_listed = sorted(listed - set(seen))
    if len(left_out) > MAX_LEFT_OUT_SHARE * max(1, len(seen)):
        raise ValueError(f"{len(left_out)} of {len(seen)} listed members unreadable: {sorted(left_out)[:5]} ...")
    if not stations:
        raise ValueError("no listed station in the archive")
    log(f"{W.run_name(run_dt)}: {count} members, {len(stations)} listed stations split, {len(left_out)} left out, "
        f"{len(missing_listed)} listed ids not in the archive")
    import partition2d as P2
    doc = {"format": FORMAT, "run": W.run_name(run_dt), "steps": list(range(STEPS)), "stations": stations,
           "count": len(stations), "record_bytes": RECORD_BYTES, "missing": MISSING, "parts": PARTS,
           "units": {"hs": "0.01 m", "tp": "0.1 s", "dir": "1 deg FROM true north, bits 0-11",
                     "flags": f"bits {FLAG_SHIFT}-15 of the direction value: bit 0 = wind sea"},
           "rules": P2.RULES, "members": count, "left_out": left_out, "not_in_archive": missing_listed,
           "steps_present": steps_present, "nproc": os.cpu_count(), "workers": workers,
           "build_s": round(time.monotonic() - t0, 1),
           "built_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "source": "GFS-Wave station spectra (gfswave.tHHz.spec_tar.gz, NOAA's AWS copy) split by partition_v2"}
    return bytes(body), doc


# ---------------------------------------------------------------- publishing

def publish(store, run, body, doc):
    doc = dict(doc, sha256=hashlib.sha256(body).hexdigest())
    store.put(bin_key(run), body, "application/octet-stream", P.IMMUTABLE)
    store.put(index_key(run), json.dumps(doc, separators=(",", ":")).encode(), "application/json", P.IMMUTABLE)
    pointer = {"run": run, "published_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "index": index_key(run), "stations": doc["count"], "steps": STEPS}
    store.put(P.latest_key(PREFIX), json.dumps(pointer, sort_keys=True).encode(), "application/json", P.POINTER)
    return pointer


def prune(store, keep=KEEP):
    """The winds job's rule under this prefix: keep the newest `keep` runs with an index (+ the pointer's)."""
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
            continue
        store.delete_prefix(f"{PREFIX}/{run}/")
        deleted.append(run)
    oldest = min(protect) if protect else None
    if oldest:
        old = [k for d in ("failed", "notready") for k in store.list_keys(f"{PREFIX}/{d}/")
               if k.rsplit("/", 1)[-1][:10].isdigit() and k.rsplit("/", 1)[-1][:10] < oldest]
        if old:
            store.delete_keys(old, "old records")
    return deleted


def _skip_failed(store, run):
    rec = store.get_json(P.failed_key(run, PREFIX))
    if not rec:
        return False
    if rec.get("attempts", 0) >= W.FAILED_MAX_ATTEMPTS:
        return True
    last = datetime.strptime(rec["last_attempt_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).total_seconds() < W.FAILED_RETRY_AFTER_S


def _stale(store, latest, situation):
    import run as R
    return R._stale_check(store, SRC, latest, situation)


def _not_now(store, latest, live, situation):
    return (_stale(store, latest, situation) or 3) if live else 3


def _write_out(out, body, doc):
    if not out:
        return
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "spectral.bin"), "wb") as f:
        f.write(body)
    with open(os.path.join(out, "index.json"), "w", encoding="utf-8") as f:
        json.dump(dict(doc, sha256=hashlib.sha256(body).hexdigest()), f)


def main(argv=None, opener=urllib.request.urlopen, head=None, store=None, now=None, listed=None,
         pool_factory=default_pool):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--run", help="YYYYMMDDHH (default: the newest complete run)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--local", help="a directory holding the archive file (no network; implies --dry-run)")
    ap.add_argument("--out", help="with --dry-run / --local: write spectral.bin and index.json into this directory")
    ap.add_argument("--workers", type=int, default=int(os.environ.get("GFSSPEC_WORKERS") or 0) or None)
    a = ap.parse_args(argv)
    t0 = time.monotonic()
    head = head or (lambda url: W.head_size(url, opener=opener))
    listed = listed if listed is not None else listed_stations()
    if a.run and (len(a.run) != 10 or not a.run.isdigit() or a.run[8:] not in ("00", "06", "12", "18")):
        print(f"{a.run}: not YYYYMMDDHH of a GFS cycle")
        return 2
    if a.local:
        if not a.run:
            print("--local needs --run")
            return 2
        run_dt = datetime.strptime(a.run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
        with open(os.path.join(a.local, os.path.basename(W.archive_key(run_dt))), "rb") as f:
            body, doc = build(f, run_dt, listed, workers=a.workers, pool_factory=pool_factory)
        _write_out(a.out, body, doc)
        print(f"local build: {doc['count']} stations, {len(body)} B, {doc['build_s']} s (nothing published)")
        return 0
    live = not a.dry_run
    if live and a.out:
        print("--out only with --dry-run or --local")
        return 2
    if live and store is None:
        store = W._store_from_env()
    latest = (store.get_json(P.latest_key(PREFIX)) or {}) if live else {}
    try:
        if a.run:
            run_dt = datetime.strptime(a.run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
            size = head(f"{S3}/{W.archive_key(run_dt)}")
            if size is None or size < W.MIN_ARCHIVE_BYTES:
                print(f"{a.run}: no complete spectra archive on NOAA's AWS copy")
                return 3
        else:
            run_dt, size = W.newest_complete(now, head)
    except TransportError as exc:
        print(f"run discovery failed: {exc}")
        return 1
    if run_dt is None:
        print("no complete GFS spectra archive in the last 4 cycles")
        return (_stale(store, latest, "no complete GFS spectral run found") or 3) if live else 3
    run = W.run_name(run_dt)
    if live and a.run:
        if latest.get("run", "") > run:
            print(f"{run} is older than the live run {latest['run']}: refused (the pointer never moves back)")
            return 2
        if store.get_json(index_key(run)) is not None:
            print(f"{run} is already published: its objects are immutable, nothing rewritten")
            return 0
    if live and not a.run:
        if latest.get("run") == run:
            print(f"{run} already live")
            return _stale(store, latest, "the newest GFS spectral run is the live one") or 0
        if latest.get("run", "") > run:
            print(f"{run} is older than the live run {latest['run']}: nothing to do")
            return 0
        if _skip_failed(store, run):
            print(f"{run} failed recently: waiting before another try")
            return _not_now(store, latest, live, f"the newest GFS spectral run {run} keeps failing")
    print(f"{run}: building from {S3}/{W.archive_key(run_dt)} ({size} B), {os.cpu_count()} cpus")
    try:
        raw = W.open_archive(f"{S3}/{W.archive_key(run_dt)}", opener)
        with raw:
            src = W._Counting(raw)
            body, doc = build(src, run_dt, listed, deadline=t0 + BUILD_BUDGET_S, workers=a.workers,
                              pool_factory=pool_factory)
    except NotReady as exc:
        print(f"{run}: not served yet ({exc})")
        if live:
            P.record_notready(store, run, str(exc), PREFIX)
        return _not_now(store, latest, live, f"the newest GFS spectral run {run} is not served")
    except BudgetExceeded as exc:
        print(f"{run}: {exc} after {time.monotonic() - t0:.0f} s; the next tick tries again")
        if live:
            P.record_notready(store, run, str(exc), PREFIX)
        return _not_now(store, latest, live, f"the newest GFS spectral run {run} cannot be built in time")
    except Exception as exc:                         # noqa: BLE001
        print(f"{run}: build failed: {exc.__class__.__name__}: {exc}")
        if live:
            P.record_failure(store, run, f"{exc.__class__.__name__}: {exc}", PREFIX)
        W._summary(f"FAILED: GFS spectral {run}: {exc.__class__.__name__}")
        return 1
    secs = time.monotonic() - t0
    line = (f"GFS spectral {run}: {doc['count']} stations ({len(doc['left_out'])} left out), {len(body)} B, "
            f"archive {src.bytes_read} B read, {doc['workers']} workers of {doc['nproc']} cpus, {secs:.0f} s")
    print(line)
    W._summary(line)
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
