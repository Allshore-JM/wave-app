"""Overlay frame job: render + publish the newest COMPLETE GFS-Wave/GFS run.

python tools/model_frames/run.py            # env: R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET
Options: --dry-run (no uploads), --steps 0,3,6 (subset, for tests), --force (re-publish even if current).
Exit codes: 0 published or already current; 3 no complete run available (not an error for the schedule).
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


def build_and_publish(store, run_dt, steps, dry_run=False, log=print):
    run = P.run_key(run_dt)
    frames = []
    t0 = time.time()
    for step in steps:
        t = time.time()
        wave = F.fetch_records(F.wave_url(run_dt, step), list(WAVE_KEYS.values()))
        atmos = F.fetch_records(F.atmos_url(run_dt, step), list(ATMOS_KEYS))
        grids = {name: D.decode(wave[key])[0] for name, key in WAVE_KEYS.items()}
        u, _ = D.decode(atmos[ATMOS_KEYS[0]])
        v, _ = D.decode(atmos[ATMOS_KEYS[1]])
        grids["wind"] = D.wind_speed(u, v)
        entry = {"step": step, "valid_utc": (run_dt + timedelta(hours=step)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                 "files": {}, "stats": {}}
        for name, grid in grids.items():
            enc = E.encode_frame(grid, name)
            entry["files"][name] = {"full": P.frame_key(run, name, step), "half": P.frame_key(run, name, step, half=True),
                                    "bytes_full": len(enc["full"]), "bytes_half": len(enc["half"])}
            entry["stats"][name] = enc["stats"]
            if not dry_run:
                P.publish_frame(store, run, name, step, enc)
        frames.append(entry)
        log(f"f{step:03d} done in {time.time() - t:.1f}s")
    manifest = {
        "schema": 1, "run": run, "run_utc": run_dt.strftime("%Y-%m-%dT%H:%M:%SZ"), "model": MODEL,
        "encoding": E.ENCODING, "grid": {"cols": D.NI, "rows": D.NJ, "lon0": -180.0, "lat0": 90.0, "step": 0.25},
        "fields": {n: {"lo": lo, "hi": hi, "units": units, "interpolation": interp}
                   for n, (lo, hi, units, interp) in E.FIELDS.items()},
        "frames": frames, "complete": len(frames) == len(F.STEPS) and steps == F.STEPS,
        "published_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "build_seconds": round(time.time() - t0, 1),
    }
    if not dry_run:
        P.publish_manifest_then_pointer(store, run, manifest)
    return manifest


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--steps", default=None, help="comma list, default all 81")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--keep", type=int, default=4)
    a = ap.parse_args(argv)
    steps = [int(s) for s in a.steps.split(",")] if a.steps else F.STEPS
    run_dt = F.latest_complete_run()
    if run_dt is None:
        print("no complete run on S3 in the last 48 h")
        return 3
    run = P.run_key(run_dt)
    store = None
    if not a.dry_run:
        store = P.Store(P.r2_client(os.environ["R2_ACCOUNT_ID"], os.environ["R2_ACCESS_KEY_ID"],
                                    os.environ["R2_SECRET_ACCESS_KEY"]), os.environ["R2_BUCKET"])
        latest = store.get_json(P.LATEST_KEY) or {}
        if latest.get("run") == run and not a.force:
            print(f"run {run} already published; nothing to do")
            return 0
    print(f"publishing run {run} ({len(steps)} steps){' [dry-run]' if a.dry_run else ''}")
    manifest = build_and_publish(store, run_dt, steps, dry_run=a.dry_run)
    print(json.dumps({k: manifest[k] for k in ("run", "complete", "build_seconds")}))
    if not a.dry_run and manifest["complete"]:
        removed = P.prune(store, keep=a.keep)
        print("pruned:", removed)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        tot = sum(f["files"][n]["bytes_full"] + f["files"][n]["bytes_half"] for f in manifest["frames"] for n in f["files"])
        open(summary, "a").write(f"### run {run}: {len(manifest['frames'])} frames, complete={manifest['complete']}, "
                                 f"{tot/1e6:.1f} MB stored, {manifest['build_seconds']} s\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
