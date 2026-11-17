"""Capture the RRFS fixtures (plan section 99) into tests/fixtures/rrfs/ from NOAA's AWS copy (read-only, paced).

- hi_f006_<VAR>.grib2: every record the job reads for step 6 of the 84-h run 2026111712 (Hawaii), + its .idx
- hi_subh_t13z_f006_<VAR>.grib2: the same for step 6 of the hourly cycle 2026111713 (subh file), + its .idx
- pr_f006_<VAR>.grib2: the wind records of Puerto Rico, run 2026111712 step 6, + its .idx
- conus_f006.idx, ak_f006.idx
- nodes_<grid>.json: 50 nodes (the 4 corners + 46 random) of eccodes' latitudes / longitudes per grid

NOAA keeps a cycle on AWS for a long time, but if these are gone pass --run / --cycle of newer ones (and update the
tests' cycle names). python tests/capture_rrfs_fixtures.py
"""
import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "tools", "model_frames"))
sys.path.insert(0, os.path.dirname(HERE))
import rrfs as J  # noqa: E402

OUT = os.path.join(HERE, "fixtures", "rrfs")
VAR = {"u": "UGRD", "v": "VGRD", "gust": "GUST", "tmp": "TMP", "dpt": "DPT", "crain": "CRAIN", "csnow": "CSNOW",
       "cfrzr": "CFRZR", "cicep": "CICEP", "tcdc": "TCDC", "apcp": "APCP", "apcp_total": "APCP", "ltng": "LTNG"}


def get(url, rng=None):
    time.sleep(0.3)
    return J.http_get(url, rng)


def capture(cycle_dt, grid, step, prefix, only=None):
    url = f"{J.S3}/{J.file_key(cycle_dt, grid, step)}"
    idx = get(url + ".idx")
    with open(os.path.join(OUT, f"{prefix}.idx"), "wb") as f:
        f.write(idx)
    rows = J.parse_idx(idx.decode("ascii"), J.content_length(url))
    recs = J.select_records(rows, cycle_dt, step)
    for name, (a, b) in recs.items():
        if only and name not in only:
            continue
        with open(os.path.join(OUT, f"{prefix}_{VAR[name]}.grib2"), "wb") as f:
            f.write(get(url, (a, b)))
    return recs


def nodes(cycle_dt, grid):
    import eccodes as ec
    url = f"{J.S3}/{J.file_key(cycle_dt, grid, 6)}"
    rows = J.parse_idx(get(url + ".idx").decode("ascii"))
    a, b = J.select_records(rows, cycle_dt, 6)["u"]
    g = ec.codes_new_from_message(get(url, (a, b)))
    try:
        ny, nx = ec.codes_get(g, "Ny"), ec.codes_get(g, "Nx")
        la = ec.codes_get_double_array(g, "latitudes").reshape(ny, nx)
        lo = ec.codes_get_double_array(g, "longitudes").reshape(ny, nx)
    finally:
        ec.codes_release(g)
    rng = np.random.default_rng(99)
    pts = [(0, 0), (0, nx - 1), (ny - 1, 0), (ny - 1, nx - 1)] + [
        (int(j), int(i)) for j, i in zip(rng.integers(0, ny, 46), rng.integers(0, nx, 46))]
    with open(os.path.join(OUT, f"nodes_{grid}.json"), "w") as f:
        json.dump({"grid": grid, "nodes": [[j, i, round(float(la[j, i]), 6), round(float(lo[j, i]), 6)]
                                           for j, i in pts]}, f, indent=0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default="2026111712")
    ap.add_argument("--cycle", default="2026111713")
    a = ap.parse_args()
    run = datetime.strptime(a.run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    cyc = datetime.strptime(a.cycle, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    os.makedirs(OUT, exist_ok=True)
    capture(run, "hi", 6, "hi_f006")
    capture(cyc, "hi", 6, "hi_subh_t13z_f006")
    capture(run, "pr", 6, "pr_f006", only=("u", "v", "gust"))
    for grid in ("conus", "ak"):
        url = f"{J.S3}/{J.file_key(run, grid, 6)}.idx"
        with open(os.path.join(OUT, f"{grid}_f006.idx"), "wb") as f:
            f.write(get(url))
    for grid in J.G.ORDER:
        nodes(run, grid)
    print("captured into", OUT)


if __name__ == "__main__":
    main()
