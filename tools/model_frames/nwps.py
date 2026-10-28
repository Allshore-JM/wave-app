"""NWPS forecast tables (plan section 59): the NWS offices' nearshore SWAN spectra at our stations, split into swells and
published to R2 under nwps/v1/ for the site's NWPS tables.

Each tick, office by office (nwps_stations.json names which office serves each station): the office's cycles of today
and yesterday on NOMADS are listed (paced); the newest cycle whose CG1 directory is settled (it holds the office's CG1
grib and its newest file is SETTLE_S old) is the office's candidate run. When it is newer than the office's live run,
the station spectra files of that run are downloaded, read (spc2d), split hour by hour (partition2d) and published:

  nwps/v1/<office>/<RUN>/<sid>.json   {"format": "nwps-v1", "office", "region", "run", "station", "point": {lat, lon},
                                       "swan_version", "start_utc", "hours": [0..144], "hs": [...], "n": [...],
                                       "parts": [[[hs m, tp s, dir deg FROM], ...] per hour] (null = no spectrum),
                                       "built_utc", "source"}
  nwps/v1/<office>/<RUN>/index.json   {"format", "office", "run", "stations": {sid: {"bytes", "sha256"}}, "left_out"}
  nwps/v1/latest.json                 {"format": "nwps-pointer-v1", "checked_utc", "offices": {office: {"run",
                                       "region", "published_utc", "stations": [...], "runs": [kept runs, newest first]}}}
All immutable but the pointer, which is written after each office's objects (a crash leaves the previous pointer).
RUN is the cycle NOAA names the run for (YYYYMMDDHH); the spectra's first time is that hour (checked).

Exit codes (the publishers' contract): 0 something published, or nothing new; 3 nothing could be built this tick (NOMADS
throttled or not serving, the budget ran out: the next tick retries); 1 a failure of every office tried (recorded per
office and run under failed/<office>-<RUN>.json, retried after an hour, at most 6 times) or the stale alarm (no office
updated in STALE_FAIL_H: at most once every 6 h). An office with no new run in OFFICE_WARN_H gets a summary WARNING.

  python tools/model_frames/nwps.py                        # every office's newest run, if not live yet
  python tools/model_frames/nwps.py --office hfo           # one office
  python tools/model_frames/nwps.py --office hfo --run 2026102800
  python tools/model_frames/nwps.py --dry-run [--out DIR]  # build, publish nothing (DIR: the station files)
"""
import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import nwps_fetch as N  # noqa: E402
import partition2d as PT  # noqa: E402
import publish as P  # noqa: E402
import spc2d  # noqa: E402

PREFIX = P.NWPS_PREFIX
FORMAT = "nwps-v1"
POINTER_FORMAT = "nwps-pointer-v1"
STATIONS_PATH = os.path.join(os.path.dirname(os.path.dirname(HERE)), "nwps_stations.json")
KEEP = 4                                    # runs kept per office
SETTLE_S = 300                              # a CG1 directory whose newest file is younger may still be filling
LOOKBACK_DAYS = 2
MAX_HOURS = 145                             # 0..144
BUILD_BUDGET_S = 20 * 60
FAILED_RETRY_AFTER_S = 3600
FAILED_MAX_ATTEMPTS = 6
LIVE = "live"                               # candidate(): nothing newer than the live run
STATION_TRANSPORT_MAX = 2                   # station files failing in one office build before the tick stops
HEARTBEAT_S = 3600                          # the pointer's checked_utc is rewritten at least this often
OFFICE_WARN_H = 48
STALE_FAIL_H = 24
SOURCE = "NOAA/NWS NWPS (nearshore SWAN), CG1 point spectra, partitioned by Allshore Surf"


class BudgetExceeded(Exception):
    """The tick ran past BUILD_BUDGET_S: what is built is published, the rest waits for the next tick."""


class NwpsSource:
    """What run._stale_check needs to know of this job."""
    name = "NOMADS"
    tag = "NWPS "
    prefix = PREFIX
    stale_fail_h = STALE_FAIL_H


SRC = NwpsSource()


def now_utc():
    return datetime.now(timezone.utc)


def stamp(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def run_name(day, hh):
    return f"{day:%Y%m%d}{hh:02d}"


def station_key(office, run, sid):
    return f"{PREFIX}/{office}/{run}/{sid}.json"


def index_key(office, run):
    return f"{PREFIX}/{office}/{run}/index.json"


def record_name(office, run):
    return f"{office}-{run}"


def load_stations(path=STATIONS_PATH):
    """{office: {sid: entry}} from nwps_stations.json."""
    doc = json.load(open(path, encoding="utf-8"))
    by = {}
    for sid, e in doc["stations"].items():
        by.setdefault(e["office"], {})[sid] = e
    return by


# ------------------------------------------------------------------------------------------------- discovery

def candidate(office, region, now, request=None, after="", deadline=None):
    """(day, hh, {sid: (modified, size)}) of the office's newest settled CG1 run in the last LOOKBACK_DAYS that is
    newer than `after` (the live run); LIVE when the newest settled cycle is the live run or older; None when no settled
    run was found. A newer cycle that is not settled yet is skipped (the next tick takes
    it); the cycles at or before the live run are never listed (they cannot be published). BudgetExceeded when the
    tick's deadline passes."""
    for day in N.recent_days(now, LOOKBACK_DAYS):
        if deadline is not None and time.monotonic() > deadline:
            raise BudgetExceeded(f"budget ran out listing {office}")
        for hh in sorted(N.cycles(region, day, office, request), reverse=True):
            if after and run_name(day, hh) <= after:
                return LIVE                   # this cycle and every older one is live already or older
            if deadline is not None and time.monotonic() > deadline:
                raise BudgetExceeded(f"budget ran out listing {office}")
            files = N.spectra_listing(region, day, office, hh, request)
            if not files or "" not in files:
                continue                              # no CG1 grib yet: the run is still being written
            newest = max(t for t, _ in files.values())
            if (now - newest).total_seconds() < SETTLE_S:
                continue
            return day, hh, files
    return None


# ------------------------------------------------------------------------------------------------- the build

def station_doc(text, office, region, run_dt, sid, built):
    """The published document of one station's spectra file; ValueError when the file is not the run's."""
    sp = spc2d.read(text)
    start = sp.steps[0][0]
    if start != run_dt.replace(tzinfo=None):
        raise ValueError(f"first time {start:%Y%m%d%H} is not the run {run_dt:%Y%m%d%H}")
    hours, hs, n, parts = [], [], [], []
    for when, rows in sp.steps:
        h = (when - start).total_seconds() / 3600
        if h != int(h) or not 0 <= h < MAX_HOURS:
            continue
        hours.append(int(h))
        if rows is None:
            hs.append(None)
            n.append(None)
            parts.append(None)
            continue
        r = PT.partition(sp.freqs, sp.dirs, np.array(rows, float))
        hs.append(round(r["hs"], 2))
        n.append(r["n"])
        parts.append([[round(p["hs"], 2), round(p["tp"], 1), int(round(p["dir"])) % 360] for p in r["parts"]])
    if not hours or all(v is None for v in hs):
        raise ValueError("no spectrum in the file")
    version = ""
    for line in (text.decode("ascii", "replace") if isinstance(text, bytes) else text).splitlines()[:6]:
        if "SWAN version" in line:
            version = line.split("SWAN version", 1)[1].strip()
    return {"format": FORMAT, "office": office, "region": region, "run": f"{run_dt:%Y%m%d%H}", "station": sid,
            "point": {"lat": round(sp.lat, 4), "lon": round(sp.lon, 4)}, "swan_version": version,
            "start_utc": stamp(start.replace(tzinfo=timezone.utc)), "hours": hours, "hs": hs, "n": n, "parts": parts,
            "built_utc": built, "source": SOURCE}


def build_office(office, region, day, hh, files, stations, request, deadline):
    """{sid: (bytes, doc)} and {sid: reason left out} for one office's run."""
    run_dt = datetime(day.year, day.month, day.day, hh, tzinfo=timezone.utc)
    built = stamp(now_utc())
    out, left = {}, {}
    transport = 0
    for sid in sorted(stations):
        if time.monotonic() > deadline:
            raise BudgetExceeded(f"budget ran out at {office} {sid}")
        if sid not in files:
            left[sid] = "no spectra file in this run"
            continue
        try:
            _status, body = request(N.spc_url(region, day, office, hh, sid))
        except N.Throttled:
            raise
        except N.NotReady:
            left[sid] = "not served"
            continue
        except N.TransportError as exc:
            transport += 1
            if transport >= STATION_TRANSPORT_MAX:
                raise                                 # NOMADS itself is failing: the tick stops
            left[sid] = f"not served: {exc}"        # one file that keeps failing never holds the office back
            continue
        try:
            doc = station_doc(body, office, region, run_dt, sid, built)
        except ValueError as exc:
            left[sid] = f"unreadable: {exc}"
            continue
        out[sid] = (json.dumps(doc, separators=(",", ":")).encode(), doc)
    return out, left


# ------------------------------------------------------------------------------------------------- publishing

def publish_office(store, pointer, office, region, run, built, left):
    """The station files, the index, then the pointer (its office entry: run, stations, kept runs)."""
    stations = {}
    for sid, (body, _doc) in sorted(built.items()):
        store.put(station_key(office, run, sid), body, "application/json", P.IMMUTABLE)
        stations[sid] = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    idx = {"format": FORMAT + "-index", "office": office, "run": run, "stations": stations, "left_out": left,
           "built_utc": stamp(now_utc())}
    store.put(index_key(office, run), json.dumps(idx, sort_keys=True).encode(), "application/json", P.IMMUTABLE)
    prev = pointer["offices"].get(office, {})
    runs = [run] + [r for r in prev.get("runs", []) if r != run]
    runs = sorted(set(runs), reverse=True)[:KEEP]
    pointer["offices"][office] = {"run": run, "region": region, "published_utc": stamp(now_utc()),
                                  "stations": sorted(stations), "runs": runs}
    write_pointer(store, pointer)


def write_pointer(store, pointer):
    pointer["format"] = POINTER_FORMAT
    pointer["checked_utc"] = stamp(now_utc())
    store.put(P.latest_key(PREFIX), json.dumps(pointer, sort_keys=True).encode(), "application/json", P.POINTER)


def prune_office(store, office, keep_runs):
    """Delete the office's runs that the pointer no longer keeps (complete or not, older than the oldest kept), and the
    office's failed / notready records older than it. Only <PREFIX>/<office>/<10 digits>/ is ever touched."""
    if not keep_runs:
        return []
    oldest = min(keep_runs)
    runs = set()
    for k in store.list_keys(f"{PREFIX}/{office}/"):
        name = k[len(PREFIX) + len(office) + 2:].split("/", 1)[0]
        if name.isdigit() and len(name) == 10:
            runs.add(name)
    deleted = []
    for run in sorted(runs):
        if run in keep_runs or run > oldest:
            continue
        store.delete_prefix(f"{PREFIX}/{office}/{run}/")
        deleted.append(run)
    old = []
    for d in ("failed", "notready"):
        for k in store.list_keys(f"{PREFIX}/{d}/{office}-"):
            run = k.rsplit("/", 1)[-1][len(office) + 1:len(office) + 11]
            if run.isdigit() and run < oldest:
                old.append(k)
    if old:
        store.delete_keys(old, "old records")
    return deleted


def _skip_failed(store, office, run):
    rec = store.get_json(P.failed_key(record_name(office, run), PREFIX))
    if not rec:
        return False
    if rec.get("attempts", 0) >= FAILED_MAX_ATTEMPTS:
        return True
    last = datetime.strptime(rec["last_attempt_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (now_utc() - last).total_seconds() < FAILED_RETRY_AFTER_S


def _store_from_env():
    return P.Store(P.r2_client(os.environ["R2_ACCOUNT_ID"], os.environ["R2_ACCESS_KEY_ID"],
                               os.environ["R2_SECRET_ACCESS_KEY"]), os.environ["R2_BUCKET"])


def _summary(line):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as fh:
            fh.write(line + "\n")


def _stale(store, pointer, situation):
    """run._stale_check on the newest office publication: the alarm when no office has published in STALE_FAIL_H."""
    import run as R
    pubs = [o.get("published_utc") for o in pointer.get("offices", {}).values() if o.get("published_utc")]
    latest = {"run": "any office", "published_utc": max(pubs)} if pubs else {}
    return R._stale_check(store, SRC, latest, situation)


def _office_warnings(pointer, now, offices=None):
    out = []
    for office, o in sorted(pointer.get("offices", {}).items()):
        if offices is not None and office not in offices:
            continue                                  # an office no longer serving a station
        try:
            run_dt = datetime.strptime(o["run"], "%Y%m%d%H").replace(tzinfo=timezone.utc)
        except (KeyError, ValueError):
            continue
        if (now - run_dt).total_seconds() > OFFICE_WARN_H * 3600:
            out.append(f"WARNING: NWPS {office}: no new run since {o['run']} ({OFFICE_WARN_H} h)")
    return out


def _write_out(out, office, run, built):
    if not out:
        return
    d = os.path.join(out, office, run)
    os.makedirs(d, exist_ok=True)
    for sid, (body, _doc) in built.items():
        with open(os.path.join(d, f"{sid}.json"), "wb") as f:
            f.write(body)


def main(argv=None, request=None, store=None, now=None, stations=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--office", help="one office (default: every office serving a station)")
    ap.add_argument("--run", help="YYYYMMDDHH of that office's run (needs --office)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--out", help="with --dry-run: write the station files into this directory")
    a = ap.parse_args(argv)
    t0 = time.monotonic()
    deadline = t0 + BUILD_BUDGET_S
    request = request or N.request
    by_office = stations if stations is not None else load_stations()
    if a.office and a.office not in by_office:
        print(f"{a.office}: not an office serving a station")
        return 2
    if a.run and (not a.office or len(a.run) != 10 or not a.run.isdigit()):
        print("--run needs --office and YYYYMMDDHH")
        return 2
    live = not a.dry_run
    if live and a.out:
        print("--out only with --dry-run")
        return 2
    if live and store is None:
        store = _store_from_env()
    pointer = (store.get_json(P.latest_key(PREFIX)) or {}) if live else {}
    pointer.setdefault("offices", {})
    now = now or now_utc()
    offices = [a.office] if a.office else sorted(by_office, key=lambda o: pointer["offices"].get(o, {}).get("run", ""))
    published, failures, waiting, built_any = [], [], [], False
    throttled = budget = unreachable = False
    for office in offices:
        if time.monotonic() > deadline:
            print(f"{office}: the budget ran out; the rest waits for the next tick")
            budget = True
            break
        region = N.OFFICES[office]
        live_run = pointer["offices"].get(office, {}).get("run", "")
        day = hh = None
        try:
            if a.run:
                day = datetime.strptime(a.run[:8], "%Y%m%d").date()
                hh = int(a.run[8:])
                files = N.spectra_listing(region, day, office, hh, request)
                if not files:
                    print(f"{office} {a.run}: not on NOMADS")
                    waiting.append(office)
                    continue
            else:
                cand = candidate(office, region, now, request, after=live_run if live else "", deadline=deadline)
                if cand == LIVE:
                    print(f"{office}: no settled run newer than the live {live_run}")
                    continue
                if cand is None:
                    print(f"{office}: no settled run in the last {LOOKBACK_DAYS} days")
                    waiting.append(office)
                    continue
                day, hh, files = cand
            run = run_name(day, hh)
            if live and run <= live_run:
                print(f"{office}: {run} " + ("already live" if run == live_run else f"older than the live {live_run}"))
                continue
            if live and _skip_failed(store, office, run):
                print(f"{office}: {run} failed recently: waiting before another try")
                waiting.append(office)
                continue
            built, left = build_office(office, region, day, hh, files, by_office[office], request, deadline)
        except N.Throttled as exc:
            print(f"{office}: NOMADS throttled ({exc}); the rest waits for the next tick")
            throttled = True
            break
        except BudgetExceeded as exc:
            print(f"{office}: {exc}; the rest waits for the next tick")
            budget = True
            break
        except N.TransportError as exc:              # a silent or failing NOMADS: every request would wait minutes
            print(f"{office}: NOMADS did not answer ({exc}); the rest waits for the next tick")
            unreachable = True
            break
        except N.NotReady as exc:
            print(f"{office}: not on NOMADS ({exc})")
            waiting.append(office)
            continue
        except Exception as exc:                     # noqa: BLE001 - a layout change: recorded, retried later
            print(f"{office}: build failed: {exc.__class__.__name__}: {exc}")
            if live and day is not None:
                P.record_failure(store, record_name(office, run_name(day, hh)), f"{exc.__class__.__name__}: {exc}",
                                 PREFIX)
            failures.append(office)
            continue
        line = f"NWPS {office} {run}: {len(built)} stations" + (f", left out {left}" if left else "")
        print(line)
        _summary(line)
        if not built:
            waiting.append(office)
            continue
        built_any = True
        if not live:
            _write_out(a.out, office, run, built)
            continue
        publish_office(store, pointer, office, region, run, built, left)
        prune_office(store, office, set(pointer["offices"][office]["runs"]))
        published.append(f"{office} {run}")
    secs = time.monotonic() - t0
    print(f"tick: {len(published)} published, {len(failures)} failed, {len(waiting)} waiting, {secs:.0f} s")
    if not live:
        print("dry run: nothing published")
        return 0 if built_any else 3
    if failures:
        _summary(f"FAILED: NWPS {', '.join(failures)}")
    for w in _office_warnings(pointer, now, by_office):
        print(w)
        _summary(w)
    if not published:
        checked = pointer.get("checked_utc")
        try:
            age = (now_utc() - datetime.strptime(checked, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                   ).total_seconds()
        except (TypeError, ValueError):
            age = None
        if pointer["offices"] and (age is None or age >= HEARTBEAT_S):
            write_pointer(store, pointer)
    if published:
        _summary(f"NWPS published: {', '.join(published)} ({secs:.0f} s)")
        return 0
    stale = _stale(store, pointer, "no NWPS office published a new run")
    if stale:
        return stale
    if failures and not waiting and not throttled and not budget and not unreachable:
        return 1
    return 3 if (throttled or budget or unreachable or waiting or failures) else 0


if __name__ == "__main__":
    sys.exit(main())
