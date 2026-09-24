"""Overlay frame job: render + publish the newest COMPLETE GFS-Wave/GFS run.

python tools/model_frames/run.py            # env: R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET
Options:
  --dry-run          no uploads at all
  --steps 0,3,6      subset (testing). Frames + a partial manifest are uploaded ONLY with
                     --allow-partial; the pointer is never flipped for a subset.
  --force            re-publish even if the run is current or older than the live run
  --keep N           complete runs to retain (default 4)
Exit codes: 0 published / already current / newer run live; 3 no complete run available or a
needed object vanished mid-run (not an error for the schedule); 1 build/publish failure.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch as F        # noqa: E402
import decode as D       # noqa: E402
import encode as E       # noqa: E402
import publish as P      # noqa: E402

WAVE_KEYS = {"hs": "HTSGW:surface", "tp": "PERPW:surface"}
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
    },
}
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
        entry = {"step": step, "valid_utc": (run_dt + timedelta(hours=step)).strftime("%Y-%m-%dT%H:%M:%SZ")}
        stat = {"step": step, "fields": {}}
        for name, grid in grids.items():
            enc = E.encode_frame(grid, name)
            stat["fields"][name] = dict(enc["stats"], bytes_full=len(enc["full"]), bytes_half=len(enc["half"]))
            if upload:
                P.publish_frame(store, run, name, step, enc)
        frames.append(entry)
        stats_frames.append(stat)
        filled = " ".join(f"{n}={stat['fields'][n]['filled_points']}" for n in E.FILL_INFO["fields"])
        log(f"f{step:03d} done in {time.time() - t:.1f}s (filled {filled})")
    complete = steps == F.STEPS and len(frames) == len(F.STEPS)
    manifest = {
        "schema": 3, "run": run, "files": {"template": P.files_template(run), "res": {"full": "", "half": "half/"}}, "run_utc": run_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), "model": MODEL,
        "encoding": E.ENCODING, "encoding_spec": E.ENCODING_SPEC,
        "grid": GRID_FULL, "grid_half": GRID_HALF,
        "fields": {n: {"lo": f["lo"], "hi": f["hi"], "legend": f["legend"], "units": f["units"],
                       "interpolation": f["interpolation"]} for n, f in E.FIELDS.items()},
        "fill": E.FILL_INFO,
        "frame_hours": FRAME_HOURS, "expected_frames": len(F.STEPS),
        "frames": frames, "complete": complete,
        "published_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_seconds": round(time.time() - t0, 1),
    }
    stats = {"run": run, "frames": stats_frames}
    if upload:
        P.publish_manifest(store, run, manifest, stats)
    manifest["_stats"] = stats                      # in-memory only (never serialized to the bucket)
    return manifest


def _store_from_env():
    return P.Store(P.r2_client(os.environ["R2_ACCOUNT_ID"], os.environ["R2_ACCESS_KEY_ID"],
                               os.environ["R2_SECRET_ACCESS_KEY"]), os.environ["R2_BUCKET"])


def published_fill(store, run):
    """(True, fill block) of the newest COMPLETE manifest already published for `run`, else (False, None)."""
    keys = sorted(k for k in store.list_keys(f"{P.PREFIX}/{run}/manifest-"))
    if not keys:
        return False, None
    return True, (store.get_json(keys[-1]) or {}).get("fill")


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
        done, fill = published_fill(store, run)
        if done and fill != E.FILL_INFO:
            # frame keys are immutable and cached for a year: never rewrite a published run's frames
            # with differently built bytes (browsers would mix filled and unfilled frames)
            print(f"run {run} was published with fill {fill!r}; refusing to re-publish it with {E.FILL_INFO!r}")
            return 2
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
