"""Builds the nearshore job's data folder (plan section 109): table.npz, atlas.json, tides.json, manifest.json.
Run on the machine that holds the model folder; the folder is then committed to the data branch.

    python tools/model_frames/nearshore_data.py <model root> <out folder> <version> [--bins wide|pilot64] [--atlas Q.npz]

- table.npz   nearshore_table.pack of the 10 m pilot grid's first ROWS rows.
- atlas.json  the best-swell text per 30 m cell (nearshore_atlas), with the grid's frame so a browser can find the cell
              under a lon/lat. `calibrated` stays false until the scores are tuned on observed spot windows.
- tides.json  NOAA CO-OPS predicted highs and lows at Haleiwa (1612668, MLLW), YEARS years from this month.
- manifest.json  version and the three files' sha256 (the job refuses a folder that does not match)."""
import argparse
import hashlib
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import nearshore_atlas as A  # noqa: E402
import nearshore_engine as E  # noqa: E402
import nearshore_table as T  # noqa: E402

ROWS = 200
DX = 10.0
LON0, LAT0 = -158.09, 21.62          # the local frame the pilot grid was built in (the model folder's grid.py)
STATION = "1612668"
YEARS = 3
BINS = {"wide": (T.F, T.D),
        "pilot64": ([0.05, 0.0556, 0.0625, 0.0667, 0.0714, 0.0833, 0.1, 0.125], [270, 285, 300, 315, 330, 345, 0, 15])}
COOPS = ("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product=predictions&application=allshoresurf"
         "&begin_date={b}&end_date={e}&datum=MLLW&station={s}&time_zone=gmt&units=metric&interval=hilo&format=json")


def fetch_tides(start_year, years=YEARS, station=STATION, opener=urllib.request.urlopen):
    hilo = []
    for y in range(start_year, start_year + years):
        with opener(COOPS.format(b=f"{y}0101", e=f"{y}1231", s=station), timeout=60) as r:
            doc = json.load(r)
        for p in doc["predictions"]:
            t = datetime.strptime(p["t"], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp()
            hilo.append([int(t), round(float(p["v"]), 3)])
    hilo.sort()
    if len(hilo) < 1300 * years:
        raise ValueError(f"only {len(hilo)} highs and lows for {years} years")
    return {"station": station, "datum": "MLLW", "msl_minus_mllw": 0.2835, "source": "NOAA CO-OPS predictions (hilo)", "hilo": hilo}


def atlas_doc(table, version, Q, FL, calibrated=False):
    cells = A.summarise(Q, FL, A.conditions())
    texts, index, rows = [], {}, []
    for c in cells:
        t = A.text(c)
        if t not in index:
            index[t] = len(texts); texts.append(t)
        rows.append([c["cy"], c["cx"], index[t], c["best"][4]])
    g = table.geo
    return {"version": version, "calibrated": calibrated, "cell_m": A.CELL_M, "shape": list(A.cell_shape(table)),
            "frame": {"lon0": g["lon0"], "lat0": g["lat0"], "xpc": g["xpc"], "ypc": g["ypc"], "alpc": table.alpc},
            "texts": texts, "cells": rows,
            "note": "Best buoy swell per place from single test swells. Modelled estimate." + ("" if calibrated else " Not yet calibrated.")}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("root"); ap.add_argument("out"); ap.add_argument("version")
    ap.add_argument("--bins", default="wide", choices=sorted(BINS))
    ap.add_argument("--atlas", help="a saved Q/FL npz (nearshore_atlas.build) to reuse instead of running the matrix")
    ap.add_argument("--tides", help="an existing tides.json to reuse instead of asking NOAA")
    a = ap.parse_args(argv)
    os.makedirs(a.out, exist_ok=True)
    F, D = BINS[a.bins]
    info = T.pack(a.root, DX, ROWS, os.path.join(a.out, "table.npz"), a.version, LON0, LAT0, F=F, D=D)
    print("table:", json.dumps(info))
    table = E.Table.from_npz(os.path.join(a.out, "table.npz"))
    if a.atlas:
        z = np.load(a.atlas); Q, FL = z["Q"], z["FL"]
    else:
        Q, FL, _ = A.build(table, progress=lambda n, m: print(f"  atlas {n}/{m}", flush=True))
        np.savez_compressed(os.path.join(a.out, "..", f"atlas_Q_{a.version}.npz"), Q=Q, FL=FL)
    doc = atlas_doc(table, a.version, Q, FL)
    with open(os.path.join(a.out, "atlas.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(doc, f, separators=(",", ":"))
    print(f"atlas: {len(doc['cells'])} cells, {len(doc['texts'])} texts")
    if a.tides:
        tides = json.load(open(a.tides, encoding="utf-8"))
    else:
        tides = fetch_tides(datetime.now(timezone.utc).year)
    with open(os.path.join(a.out, "tides.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(tides, f, separators=(",", ":"))
    print(f"tides: {len(tides['hilo'])} highs and lows")
    sha = {n: hashlib.sha256(open(os.path.join(a.out, n), "rb").read()).hexdigest() for n in ("table.npz", "atlas.json", "tides.json")}
    with open(os.path.join(a.out, "manifest.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump({"version": a.version, "bins": a.bins, "rows": ROWS, "dx": DX, "sha256": sha,
                   "built_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}, f, indent=1)
    print("manifest written:", {n: os.path.getsize(os.path.join(a.out, n)) for n in sha})
    return 0


if __name__ == "__main__":
    sys.exit(main())
