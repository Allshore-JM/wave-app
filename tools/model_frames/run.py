"""Overlay frame job: render + publish the newest COMPLETE GFS-Wave/GFS run.

python tools/model_frames/run.py            # env: R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET
Options:
  --dry-run          no uploads at all
  --steps 0,3,6      subset (testing). Frames + a partial manifest are uploaded ONLY with
                     --allow-partial; the pointer is never flipped for a subset.
  --force            re-publish even if the run is current or older than the live run
  --keep N           complete runs to retain (default 4)
Exit codes: 0 published / already current / newer run live / pointer repaired; 3 no complete run
available or a needed object vanished mid-run (not an error for the schedule); 1 build/publish
failure; 2 bad arguments, or a refusal to rewrite a published run's immutable frames with a
different coastal fill (see fill_guard).
Rollback of the coastal fill: set "fill": False for hs and tp in encode.FIELDS (the manifest then
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
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch as F        # noqa: E402
import decode as D       # noqa: E402
import encode as E       # noqa: E402
import publish as P      # noqa: E402

WAVE_KEYS = {"hs": "HTSGW:surface", "tp": "PERPW:surface", "pdir": "DIRPW:surface"}
ATMOS_KEYS = ("UGRD:10 m above ground", "VGRD:10 m above ground")
FRAME_HOURS = 3
FAILED_RETRY_AFTER_S = 3 * 3600         # a cycle that failed to build is retried after 3 h, max 3 times
FAILED_MAX_ATTEMPTS = 3
NOTREADY_WARN = 6                       # NotReady exits in a row for one cycle before the summary warns


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
    },
}
FIELD_KEYS = ("lo", "hi", "legend", "units", "interpolation", "resolutions", "circular", "convention")
GRID_FULL = {"cols": D.NI, "rows": D.NJ, "lon0": -180.0, "lat0": 90.0, "dlon": 0.25, "dlat": -0.25,
             "registration": "center", "lon_periodic": True}
GRID_HALF = {"cols": 720, "rows": 361, "lon0": -180.0, "lat0": 90.0, "dlon": 0.5, "dlat": -0.5,
             "registration": "center", "lon_periodic": True, "derivation": "full[::2, ::2]"}


def build_and_publish(store, run_dt, steps, upload=True, log=print):
    run = P.run_key(run_dt)
    frames, stats_frames = [], []
    t0 = time.time()
    for step in steps:
        t = time.time()
        wave = F.fetch_records(F.wave_url(run_dt, step), list(WAVE_KEYS.values()))
        atmos = F.fetch_records(F.atmos_url(run_dt, step), list(ATMOS_KEYS))
        grids = {name: D.decode(wave[key], key, run_dt, step)[0] for name, key in WAVE_KEYS.items()}
        u, _ = D.decode(atmos[ATMOS_KEYS[0]], ATMOS_KEYS[0], run_dt, step)
        v, _ = D.decode(atmos[ATMOS_KEYS[1]], ATMOS_KEYS[1], run_dt, step)
        grids["wind"] = D.wind_speed(u, v)
        grids["wdir"] = D.wind_dir_from(u, v)
        entry = {"step": step, "valid_utc": (run_dt + timedelta(hours=step)).strftime("%Y-%m-%dT%H:%M:%SZ")}
        # the direction must exist wherever there are waves, and nowhere without a height (the client draws
        # arrows only on drawn water); a flat calm (hs == 0, 19 cells at f024 of 2026092512) has no direction
        hs_, pd_ = grids["hs"], grids["pdir"]
        mismatch = int(np.count_nonzero((np.isnan(pd_) & (hs_ > 0)) | (np.isnan(hs_) & ~np.isnan(pd_))))
        calm = int(np.count_nonzero(np.isnan(pd_) & (hs_ == 0)))
        stat = {"step": step, "fields": {}, "pdir_mask_mismatch": mismatch, "pdir_missing_calm": calm}
        for name, grid in grids.items():
            enc = E.encode_frame(grid, name)
            stat["fields"][name] = dict(enc["stats"], bytes_full=len(enc.get("full", b"")), bytes_half=len(enc.get("half", b"")))
            if upload:
                P.publish_frame(store, run, name, step, enc)
        frames.append(entry)
        stats_frames.append(stat)
        filled = " ".join(f"{n}={stat['fields'][n]['filled_points']}" for n in (E.FILL_INFO or {}).get("fields", []))
        log(f"f{step:03d} done in {time.time() - t:.1f}s" + (f" (filled {filled})" if filled else "")
            + (f" WARNING pdir/hs mask mismatch {mismatch}" if mismatch else ""))
    complete = steps == F.STEPS and len(frames) == len(F.STEPS)
    manifest = {
        "schema": 3, "run": run, "files": {"template": P.files_template(run), "res": {"full": "", "half": "half/"}}, "run_utc": run_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), "model": MODEL,
        "encoding": E.ENCODING, "encoding_spec": E.ENCODING_SPEC,
        "grid": GRID_FULL, "grid_half": GRID_HALF,
        "fields": {n: {k: (list(f[k]) if isinstance(f[k], tuple) else f[k]) for k in FIELD_KEYS if k in f}
                   for n, f in E.FIELDS.items()},
        "frame_hours": FRAME_HOURS, "expected_frames": len(F.STEPS),
        "frames": frames, "complete": complete,
        "published_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_seconds": round(time.time() - t0, 1),
    }
    if E.FILL_INFO:
        manifest["fill"] = E.FILL_INFO
    stats = {"run": run, "frames": stats_frames}
    if upload:
        P.publish_manifest(store, run, manifest, stats)
    manifest["_stats"] = stats                      # in-memory only (never serialized to the bucket)
    return manifest


def _store_from_env():
    return P.Store(P.r2_client(os.environ["R2_ACCOUNT_ID"], os.environ["R2_ACCESS_KEY_ID"],
                               os.environ["R2_SECRET_ACCESS_KEY"]), os.environ["R2_BUCKET"])


def fill_guard(store, run, live, partial):
    """None to go ahead, else the exit code. A run already published (complete manifest) with a
    different fill is never rebuilt: its frame keys are immutable and cached for a year, so browsers
    would mix differently built frames. If that run is simply not pointed to (the pointer write
    failed, or latest.json was lost) and it is newer than the live run, the pointer is repaired to
    its existing manifest instead of failing on every tick."""
    try:
        mkey, man = P.newest_manifest(store, run)
    except ValueError as exc:                                 # the content (json.JSONDecodeError is a ValueError)
        msg = f"run {run}: its published manifest is not a valid manifest ({exc.__class__.__name__}); not rewriting its frames"
        print(msg)
        _summary("WARNING: " + msg)
        return 2
    except Exception as exc:                                  # noqa: BLE001  transport: the next tick retries
        print(f"run {run}: could not read its published manifest ({exc.__class__.__name__}); retrying next tick")
        return 1
    if mkey is None or E.fill_key(man.get("fill")) == E.fill_key(E.FILL_INFO):
        return None
    usable = (man.get("run") == run and man.get("encoding") == E.ENCODING and man.get("complete") is True
              and isinstance(man.get("frames"), list) and len(man["frames"]) == len(F.STEPS)
              and isinstance(man.get("published_utc"), str))
    if not partial and usable and (not live or run > live):
        P.point_to(store, run, mkey, man)
        msg = f"run {run} already has a complete manifest built with fill {E.fill_key(man.get('fill'))}; pointer repaired to {mkey}"
        print(msg)
        _summary(msg)
        return 0
    msg = (f"run {run} was published with fill {E.fill_key(man.get('fill'))}; refusing to rewrite its frames "
           f"with fill {E.fill_key(E.FILL_INFO)}")
    print(msg)
    _summary("WARNING: " + msg)
    return 2


def _skip_failed(store, run):
    rec = store.get_json(P.failed_key(run))
    if not rec:
        return False
    if rec.get("attempts", 0) >= FAILED_MAX_ATTEMPTS:
        return True
    last = datetime.strptime(rec["last_attempt_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - last).total_seconds() < FAILED_RETRY_AFTER_S


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--steps", default=None, help="comma list, default all 81")
    ap.add_argument("--allow-partial", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--keep", type=int, default=4)
    a = ap.parse_args(argv)
    if a.keep < 1:
        print("--keep must be >= 1")
        return 2
    steps = [int(s) for s in a.steps.split(",")] if a.steps else F.STEPS
    if any(s not in F.STEPS for s in steps):
        print(f"--steps must be a subset of {F.STEPS[0]}..{F.STEPS[-1]} step 3")
        return 2
    partial = steps != F.STEPS
    if partial and not a.dry_run and not a.allow_partial:
        print("a subset of steps is never published live; add --dry-run or --allow-partial")
        return 2

    try:
        run_dt = F.latest_complete_run()
    except F.TransportError as exc:
        print(f"NOAA listing failed: {exc}")
        return 1
    if run_dt is None:
        print("no complete run on S3 in the last 48 h")
        return 3
    run = P.run_key(run_dt)

    store = None
    if not a.dry_run:
        store = _store_from_env()
        latest = store.get_json(P.LATEST_KEY) or {}
        live = latest.get("run")
        if live == run and latest.get("complete") and not a.force:
            print(f"run {run} already live; nothing to do")
            return 0
        if live and run < live and not a.force:
            print(f"newer run {live} is live; not regressing to {run}")
            return 0
        if not partial and not a.force and _skip_failed(store, run):
            print(f"run {run} failed recently; waiting before retrying")
            return 0
        rc = fill_guard(store, run, live, partial)
        if rc is not None:
            return rc
    print(f"publishing run {run} ({len(steps)} steps){' [dry-run]' if a.dry_run else ''}{' [partial]' if partial else ''}")

    try:
        manifest = build_and_publish(store, run_dt, steps, upload=not a.dry_run)
    except F.NotReady as exc:
        print(f"object vanished/not ready mid-run: {exc}")
        if store is not None and not partial:
            rec = P.record_notready(store, run, exc)
            print(f"not-ready count for {run}: {rec['count']}")
            if rec["count"] >= NOTREADY_WARN:
                _summary(f"WARNING: run {run} not ready {rec['count']} times in a row: {exc}")
        return 3
    except Exception as exc:                                  # noqa: BLE001
        if store is not None and not partial:
            rec = P.record_failure(store, run, exc)
            print(f"build failed (attempt {rec['attempts']}): {exc!r}")
        raise
    print(json.dumps({k: manifest[k] for k in ("run", "complete", "build_seconds")}))
    if store is not None and manifest["complete"]:
        print("pruned:", P.prune(store, keep=a.keep))
        print("legacy objects removed:", P.prune_legacy(store))
    tot = sum(v["bytes_full"] + v["bytes_half"] for f in manifest["_stats"]["frames"] for v in f["fields"].values())
    published = datetime.strptime(manifest["published_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    lag_h = (published - run_dt).total_seconds() / 3600
    _summary(f"### run {run}: {len(manifest['frames'])} frames, complete={manifest['complete']}, "
             f"{tot/1e6:.1f} MB stored, {manifest['build_seconds']} s, published {lag_h:.1f} h after the cycle")
    if E.FILL_INFO and manifest["_stats"]["frames"]:
        f0 = manifest["_stats"]["frames"][0]
        _summary(f"coastal fill (v{E.FILL_VERSION}, {E.FILL_CELLS} cells) at f{f0['step']:03d}: " +
                 ", ".join(f"{n} {f0['fields'][n]['filled_points']} cells" for n in E.FILL_INFO["fields"]))
    bad = [f["step"] for f in manifest["_stats"]["frames"] if f.get("pdir_mask_mismatch")]
    if bad:
        _summary(f"WARNING: wave direction and wave height cover different cells at {len(bad)} steps (first f{bad[0]:03d})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
