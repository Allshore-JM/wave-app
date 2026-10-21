"""Overlay frame job: render + publish the newest COMPLETE GFS-Wave/GFS run.

python tools/model_frames/run.py            # env: R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET
Options:
  --dry-run          no uploads at all
  --steps 0,3,6      subset (testing). Frames + a partial manifest are uploaded ONLY with
                     --allow-partial; the pointer is never flipped for a subset.
  --force            re-publish even if the run is current or older than the live run
  --keep N           complete runs to retain (default 4)
The ECMWF job (tools/model_frames/ecmwf.py, plan section 43) runs this same main with its own Source: another
model's fetch and decode, steps, manifest model block and bucket prefix; everything else (encoding, fill, pipeline,
guards, retention) is shared.
Exit codes: 0 published / already current / newer run live / pointer repaired; 3 no complete run
available or a needed object vanished mid-run (not an error for the schedule); 1 build/publish
failure; 2 bad arguments, or a refusal to rewrite a published run's immutable frames with a
different coastal fill (see fill_guard).
Rollback of the coastal fill: set "fill": False for hs, tp and pdir in encode.FIELDS (the manifest then
carries no fill block and the guard keeps protecting the filled runs) and adjust the fill tests in
tests/model_frames/test_job.py in the same commit. The filled live run stays live until the next
cycle (up to ~6 h); to drop it at once, point latest.json at an older unfilled run by hand. Never
`git revert` the fill commit: that removes the guard too.
"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch as F        # noqa: E402
import decode as D       # noqa: E402
import encode as E       # noqa: E402
import publish as P      # noqa: E402

WAVE_KEYS = {"hs": "HTSGW:surface", "tp": "PERPW:surface", "pdir": "DIRPW:surface"}
ATMOS_KEYS = ("UGRD:10 m above ground", "VGRD:10 m above ground", "PRMSL:mean sea level")
FAILED_RETRY_AFTER_S = 3 * 3600         # a cycle that failed to build is retried after 3 h, max 3 times
FAILED_MAX_ATTEMPTS = 3
NOTREADY_WARN = 6                       # NotReady exits in a row for one cycle before the summary warns
# The build is a pipeline (output bytes are unchanged): the next step's records download while this step is
# decoded and encoded, the fields encode in parallel (numpy / zlib release the GIL), and the PNGs upload in a
# pool that runs behind the build. GRIB decoding stays on this thread: eccodes is not thread-safe.
ENCODE_WORKERS = 5
UPLOAD_WORKERS = 8
UPLOAD_BACKLOG = 20                     # field uploads (2 PUTs each) allowed to trail the build: bounds memory
PDIR_WAVES_M = 0.1                      # below this height a missing wave direction is a calm, not a mismatch (no particle is drawn there)


def _summary(line):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as fh:
            fh.write(line + "\n")
MODEL = {
    "name": "NOAA/NCEP GFS-Wave (WAVEWATCH III) + GFS", "grid": "global 0.25 deg (1440x721)",
    "attribution": ("Source: NOAA/NCEP GFS-Wave (WAVEWATCH III) and GFS via NOAA Open Data "
                    "Dissemination; rendered by Allshore Surf. Not an official NWS product."),
    "fields": {
        "hs":   {"label": "Wave height", "grib": "HTSGW", "definition": "Significant height of combined wind waves and swell"},
        "tp":   {"label": "Peak period", "grib": "PERPW", "definition": "Peak wave period Tp = 1/fp from WAVEWATCH III (GRIB table name 'Primary wave mean period')"},
        "wind": {"label": "Wind speed", "grib": "UGRD/VGRD 10 m above ground", "definition": "GFS 10 m wind speed (sqrt(u^2+v^2))"},
        "pdir": {"label": "Wave direction", "grib": "DIRPW",
                 "definition": "Primary (peak) wave direction from WAVEWATCH III: degrees true, the direction the dominant waves come FROM"},
        "wdir": {"label": "Wind direction", "grib": "UGRD/VGRD 10 m above ground",
                 "definition": "GFS 10 m wind direction: degrees true, the direction the wind blows FROM (atan2(-u, -v))"},
        "pres": {"label": "Sea-level pressure", "grib": "PRMSL mean sea level",
                 "definition": "GFS pressure reduced to mean sea level, hPa (the isobars drawn over the wind overlay)"},
    },
}
FIELD_KEYS = ("lo", "hi", "legend", "units", "interpolation", "resolutions", "circular", "convention")
GRID_FULL = {"cols": D.NI, "rows": D.NJ, "lon0": -180.0, "lat0": 90.0, "dlon": 0.25, "dlat": -0.25,
             "registration": "center", "lon_periodic": True}
GRID_HALF = {"cols": 720, "rows": 361, "lon0": -180.0, "lat0": 90.0, "dlon": 0.5, "dlat": -0.5,
             "registration": "center", "lon_periodic": True, "derivation": "full[::2, ::2]"}


class GfsSource:
    """What the job needs from a model: discovery, one step's records, their decode into the site's fields, the
    steps / schedule / manifest model block and the bucket prefix. The GFS source looks fetch / decode up at call
    time (the tests replace those module functions)."""
    name = "NOAA"                                       # in log lines: "NOAA listing failed"
    tag = ""                                            # the step summary's run line: "### run ..." (ECMWF: "### ECMWF run ...")
    prefix = P.PREFIX
    model = MODEL
    prune_legacy = True                                 # the one-time pre-v1 cleanup belongs to the GFS prefix
    stale_fail_h = None                                 # GFS: "no complete run" stays exit 3 whatever the live run's age
    failed_retry_after_s = FAILED_RETRY_AFTER_S
    failed_max_attempts = FAILED_MAX_ATTEMPTS

    @property
    def steps(self):
        return F.STEPS

    @property
    def schedule(self):
        return F.STEP_SCHEDULE

    def latest_complete_run(self):
        return F.latest_complete_run()

    def fetch(self, run_dt, step):
        return (F.fetch_records(F.wave_url(run_dt, step), list(WAVE_KEYS.values())),
                F.fetch_records(F.atmos_url(run_dt, step), list(ATMOS_KEYS)))

    def decode(self, records, run_dt, step):
        """{hs, tp, pdir, wind, wdir, pres} on the site grid."""
        wave, atmos = records
        grids = {name: D.decode(wave[key], key, run_dt, step)[0] for name, key in WAVE_KEYS.items()}
        u, _ = D.decode(atmos[ATMOS_KEYS[0]], ATMOS_KEYS[0], run_dt, step)
        v, _ = D.decode(atmos[ATMOS_KEYS[1]], ATMOS_KEYS[1], run_dt, step)
        grids["wind"] = D.wind_speed(u, v)
        grids["wdir"] = D.wind_dir_from(u, v)
        grids["pres"] = D.hpa(D.decode(atmos[ATMOS_KEYS[2]], ATMOS_KEYS[2], run_dt, step)[0])
        return grids


GFS = GfsSource()


def src_fields(src):
    """The fields a source publishes (encode.FIELDS by default; the Great Lakes source: a subset at one resolution)."""
    return getattr(src, "fields", None) or E.FIELDS


def src_fill(src):
    """The source's fill block for the manifest and the rebuild guard (encode.FILL_INFO by default)."""
    return getattr(src, "fill_info", E.FILL_INFO)


def build_and_publish(store, run_dt, steps, upload=True, log=print, src=GFS):
    """Optional Source hooks (plan section 50; the defaults are the GFS / ECMWF behaviour, byte for byte): `fields`,
    `fill_info`, `encode(grid, name)`, `grid_blocks()` (the manifest's grid blocks + the files' resolution map) and
    `before_manifest(store, run_dt, run, upload)` (extra objects published after every frame and before the manifest;
    returns keys to add to the manifest)."""
    run = P.run_key(run_dt)
    frames, stats_frames = [], []
    t0 = time.time()
    fields, fill_info = src_fields(src), src_fill(src)
    encode = getattr(src, "encode", None) or E.encode_frame
    if fill_info and encode is E.encode_frame:
        E.fill_allow()                              # load + verify the mask once, before the encode threads
    fetch_pool, encode_pool = ThreadPoolExecutor(1), ThreadPoolExecutor(ENCODE_WORKERS)
    upload_pool = ThreadPoolExecutor(UPLOAD_WORKERS) if upload else None
    pending, nxt = [], None

    def fetch(step):
        return src.fetch(run_dt, step)

    def drain(limit):
        # the first upload failure is raised here (the build stops); at most `limit` uploads trail it. Each
        # future is asked done() ONCE and then either checked or kept: asking twice would drop a failure that
        # completed between the two questions, unseen (G12 P0-1), and the pointer would flip over a hole.
        keep = []
        for f in pending:
            if f.done():
                f.result()
            else:
                keep.append(f)
        pending[:] = keep
        while len(pending) > limit:
            pending.pop(0).result()

    try:
        nxt = fetch_pool.submit(fetch, steps[0]) if steps else None
        for n, step in enumerate(steps):
            t = time.time()
            records = nxt.result()
            nxt = fetch_pool.submit(fetch, steps[n + 1]) if n + 1 < len(steps) else None
            grids = src.decode(records, run_dt, step)
            entry = {"step": step, "valid_utc": (run_dt + timedelta(hours=step)).strftime("%Y-%m-%dT%H:%M:%SZ")}
            # on the MODEL grids the direction must exist wherever there are waves, and nowhere without a height
            # (the client draws particles only on drawn water with at least PDIR_WAVES_M of wave height); a calm
            # below that (hs == 0 is NaN in DIRPW; a 0.01 m cell on the ice edge set the warning off at f120 of
            # 2026092518) is counted apart. After the coastal fill the published frames can differ by a few
            # calm-seeded cells (hs code 1 = 0 m, Arctic pack), where nothing is drawn.
            hs_, pd_ = grids["hs"], grids["pdir"]
            mismatch = int(np.count_nonzero((np.isnan(pd_) & (hs_ >= PDIR_WAVES_M)) | (np.isnan(hs_) & ~np.isnan(pd_))))
            calm = int(np.count_nonzero(np.isnan(pd_) & (hs_ < PDIR_WAVES_M)))
            stat = {"step": step, "fields": {}, "pdir_mask_mismatch": mismatch, "pdir_missing_calm": calm}
            names = list(grids)
            encoded = list(encode_pool.map(lambda name: encode(grids[name], name), names))
            for name, enc in zip(names, encoded):
                stat["fields"][name] = dict(enc["stats"], bytes_full=len(enc.get("full", b"")), bytes_half=len(enc.get("half", b"")))
                if upload:
                    pending.append(upload_pool.submit(P.publish_frame, store, run, name, step, enc, src.prefix))
            if upload:
                drain(UPLOAD_BACKLOG)
            frames.append(entry)
            stats_frames.append(stat)
            filled = " ".join(f"{n}={stat['fields'][n]['filled_points']}" for n in (fill_info or {}).get("fields", []))
            log(f"f{step:03d} done in {time.time() - t:.1f}s" + (f" (filled {filled})" if filled else "")
                + (f" WARNING pdir/hs mask mismatch {mismatch}" if mismatch else ""))
        if upload:
            drain(0)                                # every frame is stored before the manifest and the pointer
        extra = src.before_manifest(store, run_dt, run, upload) if hasattr(src, "before_manifest") else {}
    except BaseException:
        for f in pending:
            f.cancel()
        if nxt is not None:
            nxt.cancel()
        raise
    finally:
        fetch_pool.shutdown(wait=False, cancel_futures=True)
        encode_pool.shutdown(wait=False, cancel_futures=True)
        if upload_pool is not None:
            upload_pool.shutdown(wait=False, cancel_futures=True)
    complete = steps == src.steps and len(frames) == len(src.steps)
    blocks = src.grid_blocks() if hasattr(src, "grid_blocks") else {
        "res": {"full": "", "half": "half/"}, "grid": GRID_FULL, "grid_half": GRID_HALF}
    manifest = {
        "schema": 3, "run": run, "files": {"template": P.files_template(run, src.prefix), "res": blocks["res"]}, "run_utc": run_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), "model": src.model,
        "encoding": E.ENCODING, "encoding_spec": E.ENCODING_SPEC,
        **{k: v for k, v in blocks.items() if k != "res"},
        "fields": {n: {k: (list(f[k]) if isinstance(f[k], tuple) else f[k]) for k in FIELD_KEYS if k in f}
                   for n, f in fields.items()},
        "frame_schedule": src.schedule, "expected_frames": len(src.steps),
        "frames": frames, "complete": complete,
        "published_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_seconds": round(time.time() - t0, 1),
    }
    if fill_info:
        manifest["fill"] = fill_info
    manifest.update(extra)
    stats = {"run": run, "frames": stats_frames}
    if upload:
        P.publish_manifest(store, run, manifest, stats, prefix=src.prefix)
    manifest["_stats"] = stats                      # in-memory only (never serialized to the bucket)
    return manifest


def _store_from_env():
    return P.Store(P.r2_client(os.environ["R2_ACCOUNT_ID"], os.environ["R2_ACCESS_KEY_ID"],
                               os.environ["R2_SECRET_ACCESS_KEY"]), os.environ["R2_BUCKET"])


def ranges_key(fields):
    """The encode ranges a run's frames were built with: ((name, lo, hi, circular), ...) from a manifest's
    "fields" block (or encode.FIELDS); None when the block is missing (then only the fill is compared)."""
    if not isinstance(fields, dict):
        return None
    out = []
    for name in sorted(fields):
        f = fields[name] if isinstance(fields[name], dict) else {}
        try:
            out.append((name, round(float(f.get("lo")), 9), round(float(f.get("hi")), 9), bool(f.get("circular"))))
        except (TypeError, ValueError):
            out.append((name, None, None, bool(f.get("circular"))))
    return tuple(out)


ADDED_FIELDS = ("pres",)                # fields a run published before them may lack (plan section 46)


def comparable_ranges(old, new):
    """(old, new) ranges to compare for the rebuild guard. A field in ADDED_FIELDS that the OLD manifest lacks is left
    out on both sides (plan section 46: a --force rebuild of a run published before sea-level pressure existed adds its
    frames and rewrites the others byte for byte). Anything else stays: a field missing from the old block, an extra or
    renamed one, an empty block, or a changed lo / hi / circular all compare unequal (G32a A-2)."""
    old_names = {t[0] for t in old}
    skip = {t[0] for t in new if t[0] in ADDED_FIELDS and t[0] not in old_names}
    return tuple(old), tuple(t for t in new if t[0] not in skip)


def fill_guard(store, run, live, partial, src=GFS):
    """None to go ahead, else the exit code. A run already published (complete manifest) with a
    different fill OR different encode ranges (plan section 27, G19-B P1: frames decoded with the other
    range read 1.5x too high or too low) is never rebuilt: its frame keys are immutable and cached for a
    year, so browsers would mix differently built frames. If that run is simply not pointed to (the pointer write
    failed, or latest.json was lost) and it is newer than the live run, the pointer is repaired to
    its existing manifest instead of failing on every tick."""
    try:
        mkey, man = P.newest_manifest(store, run, prefix=src.prefix)
    except ValueError as exc:                                 # the content (json.JSONDecodeError is a ValueError)
        msg = f"run {run}: its published manifest is not a valid manifest ({exc.__class__.__name__}); not rewriting its frames"
        print(msg)
        _summary("WARNING: " + msg)
        return 2
    except Exception as exc:                                  # noqa: BLE001  transport: the next tick retries
        print(f"run {run}: could not read its published manifest ({exc.__class__.__name__}); retrying next tick")
        return 1
    fill_info = src_fill(src)
    same_fill = E.fill_key(man.get("fill")) == E.fill_key(fill_info) if mkey is not None else True
    old_ranges = ranges_key(man.get("fields")) if mkey is not None else None
    same_ranges = old_ranges is None or len(set(comparable_ranges(old_ranges, ranges_key(src_fields(src))))) == 1
    if mkey is None or (same_fill and same_ranges):
        return None
    what = ("fill " + str(E.fill_key(man.get("fill")))) if not same_fill else "other encode ranges"
    usable = (man.get("run") == run and man.get("encoding") == E.ENCODING and man.get("complete") is True
              and isinstance(man.get("frames"), list) and len(man["frames"]) == len(src.steps)
              and isinstance(man.get("published_utc"), str))
    if not partial and usable and (not live or run > live):
        P.point_to(store, run, mkey, man, prefix=src.prefix)
        msg = f"run {run} already has a complete manifest built with {what}; pointer repaired to {mkey}"
        print(msg)
        _summary(msg)
        return 0
    msg = (f"run {run} was published with {what}; refusing to rewrite its frames "
           f"with fill {E.fill_key(fill_info)} and the current encode ranges")
    print(msg)
    _summary("WARNING: " + msg)
    return 2


def pressure_summary(frames):
    """The run summary's line for the isobars' field (plan section 46; None without pressure frames): the run's lowest
    and highest sea-level pressure and how many cells of the 0.25-deg grid reached the encode range's ends (a deeper
    typhoon eye, the reduction over the Antarctic plateau, a winter Siberian high)."""
    pf = [f["fields"]["pres"] for f in frames if "pres" in f.get("fields", {})]
    lows = [p["min"] for p in pf if p.get("min") is not None]
    highs = [p["max"] for p in pf if p.get("max") is not None]
    if not lows:
        return None
    f = E.FIELDS["pres"]
    return (f"sea-level pressure {min(lows):.1f}-{max(highs):.1f} hPa (encode {f['lo']:.0f}-{f['hi']:.0f}; "
            f"{sum(p['clamped_low'] for p in pf)} low / {sum(p['clamped_high'] for p in pf)} high clamped cell-frames)")


STALE_ALARM_EVERY_H = 6                 # a stale source fails the workflow at most once in this many hours (GitHub mails each failure)


def stale_alarm_key(prefix):
    return f"{prefix}/stale-alarm.json"


def _stale_check(store, src, latest, situation):
    """G30a F2 / re-check R3: a source whose files can move (ECMWF changes its path per resolution: 0p4-beta, then
    0p25) or stop must not freeze silently with every workflow run green. When the live run (`latest`, its pointer) was
    published src.stale_fail_h or more hours ago, or nothing is live, this returns 1 (the workflow fails, GitHub mails the
    owner) with a summary WARNING -- at most once every STALE_ALARM_EVERY_H (a small record under the prefix; between
    alarms it returns None and the caller carries on as usual: exit 3 for "no run", 0 for "already live"). It is called
    both when no complete run is found and when the newest complete run is the live one, so the alarm comes at
    stale_fail_h, not when the discovery window (48 h) runs out. None while the live run is fresh."""
    try:
        pub = datetime.strptime(latest["published_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        age_h = (datetime.now(timezone.utc) - pub).total_seconds() / 3600
    except (KeyError, TypeError, ValueError):
        age_h = None
    if age_h is not None and age_h < src.stale_fail_h:
        return None
    key = stale_alarm_key(src.prefix)
    rec = store.get_json(key) or {}
    try:
        last = datetime.strptime(rec["last_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if (datetime.now(timezone.utc) - last).total_seconds() < STALE_ALARM_EVERY_H * 3600:
            print(f"stale source already reported at {rec['last_utc']}; next alarm after {STALE_ALARM_EVERY_H} h")
            return None
    except (KeyError, TypeError, ValueError):
        pass
    what = f"the live run {latest.get('run')} is {age_h:.0f} h old" if age_h is not None else "nothing is live"
    msg = (f"{situation} and {what} (limit {src.stale_fail_h} h): the source's files may have moved (a new path "
           f"or resolution) or stopped")
    print(msg)
    _summary("WARNING: " + msg)
    store.put(key, json.dumps({"last_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                               "message": msg}, sort_keys=True).encode(), "application/json", P.POINTER)
    return 1


def _no_run(src):
    """No complete run found (a real run, src.stale_fail_h set): 1 when stale (see _stale_check), else 3. A pointer
    that cannot be read is a failure (1): the next tick retries."""
    try:
        store = _store_from_env()
        latest = store.get_json(P.latest_key(src.prefix)) or {}
    except Exception as exc:                                  # noqa: BLE001
        print(f"could not read the live pointer ({exc.__class__.__name__})")
        return 1
    return _stale_check(store, src, latest, f"no complete {src.tag}run found") or 3


def _skip_failed(store, run, src=GFS):
    rec = store.get_json(P.failed_key(run, src.prefix))
    if not rec:
        return False
    if rec.get("attempts", 0) >= src.failed_max_attempts:
        return True
    last = datetime.strptime(rec["last_attempt_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).total_seconds() < src.failed_retry_after_s


def main(argv=None, src=GFS):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--steps", default=None, help=f"comma list of forecast hours, default all {len(src.steps)}")
    ap.add_argument("--allow-partial", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--keep", type=int, default=4)
    a = ap.parse_args(argv)
    if a.keep < 1:
        print("--keep must be >= 1")
        return 2
    steps = [int(s) for s in a.steps.split(",")] if a.steps else src.steps
    if any(s not in src.steps for s in steps):
        sch = src.schedule
        first = "hourly" if sch[0][2] == 1 else f"every {sch[0][2]} h"
        then = f", then every {sch[1][2]} h to {sch[1][1]}" if len(sch) > 1 else ""
        print(f"--steps must be forecast hours of the model output: {first} {sch[0][0]}..{sch[0][1]}{then}")
        return 2
    partial = steps != src.steps
    if partial and not a.dry_run and not a.allow_partial:
        print("a subset of steps is never published live; add --dry-run or --allow-partial")
        return 2

    try:
        run_dt = src.latest_complete_run()
    except F.TransportError as exc:
        print(f"{src.name} listing failed: {exc}")
        return 1
    if run_dt is None:
        print(f"no complete {src.tag}run {getattr(src, 'where', 'on S3 in the last 48 h')}")
        if src.stale_fail_h and not a.dry_run:
            return _no_run(src)
        return 3
    run = P.run_key(run_dt)

    store = None
    if not a.dry_run:
        store = _store_from_env()
        latest = store.get_json(P.latest_key(src.prefix)) or {}
        live = latest.get("run")
        if live == run and latest.get("complete") and not a.force:
            print(f"run {run} already live; nothing to do")
            if src.stale_fail_h:
                return _stale_check(store, src, latest, f"no newer {src.tag}run than the live {run}") or 0
            return 0
        if live and run < live and not a.force:
            print(f"newer run {live} is live; not regressing to {run}")
            return 0
        if not partial and not a.force and _skip_failed(store, run, src):
            print(f"run {run} failed recently; waiting before retrying")
            return 0
        rc = fill_guard(store, run, live, partial, src)
        if rc is not None:
            return rc
    print(f"publishing run {run} ({len(steps)} steps){' [dry-run]' if a.dry_run else ''}{' [partial]' if partial else ''}")

    try:
        manifest = build_and_publish(store, run_dt, steps, upload=not a.dry_run, src=src)
    except F.NotReady as exc:
        print(f"object vanished/not ready mid-run: {exc}")
        if store is not None and not partial:
            rec = P.record_notready(store, run, exc, prefix=src.prefix)
            print(f"not-ready count for {run}: {rec['count']}")
            if rec["count"] >= NOTREADY_WARN:
                _summary(f"WARNING: run {run} not ready {rec['count']} times in a row: {exc}")
        return 3
    except Exception as exc:                                  # noqa: BLE001
        if store is not None and not partial:
            rec = P.record_failure(store, run, exc, prefix=src.prefix)
            print(f"build failed (attempt {rec['attempts']}): {exc!r}")
        raise
    print(json.dumps({k: manifest[k] for k in ("run", "complete", "build_seconds")}))
    if store is not None and manifest["complete"]:
        print("pruned:", P.prune(store, keep=a.keep, prefix=src.prefix))
        if src.prune_legacy:
            print("legacy objects removed:", P.prune_legacy(store))
    tot = sum(v["bytes_full"] + v["bytes_half"] for f in manifest["_stats"]["frames"] for v in f["fields"].values())
    published = datetime.strptime(manifest["published_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    lag_h = (published - run_dt).total_seconds() / 3600
    _summary(f"### {src.tag}run {run}: {len(manifest['frames'])} frames, complete={manifest['complete']}, "
             f"{tot/1e6:.1f} MB stored, {manifest['build_seconds']} s, published {lag_h:.1f} h after the cycle")
    fill_info = src_fill(src)
    if fill_info and manifest["_stats"]["frames"]:
        f0 = manifest["_stats"]["frames"][0]
        _summary(f"coastal fill (v{fill_info['version']}, {fill_info['cells']} cells) at f{f0['step']:03d}: " +
                 ", ".join(f"{n} {f0['fields'][n]['filled_points']} cells" for n in fill_info["fields"]))
    # how often a field reached its encode top (the readout then says ">= top"): the ranges are chosen to make this rare
    tops = {n: sum(f["fields"].get(n, {}).get("clamped_high", 0) or 0 for f in manifest["_stats"]["frames"])
            for n in ("hs", "tp", "wind") if n in src_fields(src)}
    peaks = {n: max((f["fields"].get(n, {}).get("max") or 0) for f in manifest["_stats"]["frames"]) for n in ("hs", "tp", "wind")}
    site = {"hs": (1 / 0.3048, "ft"), "tp": (1.0, "s"), "wind": (1 / E.KT, "kt")}
    _summary("field peaks: " + ", ".join(
        f"{n} {peaks[n] * site[n][0]:.1f} {site[n][1]} (top {E.FIELDS[n]['hi'] * site[n][0]:.1f} {site[n][1]}, {tops[n]} clamped cell-frames)"
        for n in tops))
    line = pressure_summary(manifest["_stats"]["frames"])
    if line:
        _summary(line)
    bad = [f["step"] for f in manifest["_stats"]["frames"] if f.get("pdir_mask_mismatch")]
    if bad:
        _summary(f"WARNING: wave direction and wave height cover different cells at {len(bad)} steps (first f{bad[0]:03d})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
