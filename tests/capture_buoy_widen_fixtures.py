"""Capture the plan-section-63 fixtures (NDBC bulk report, GLOS Seagull, IOOS sensors ERDDAP) from the
real feeds, cut to a few stations each. Run once by hand from the repo root:
    python -I tests/capture_buoy_widen_fixtures.py
Writes tests/fixtures/buoys_widen/. Nothing here runs in the test suite."""
import csv
import io
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "buoys_widen")
HTTP = requests.Session()
HTTP.headers["User-Agent"] = "allshoresurf.com (fixture capture)"

# kept stations: Canada (no direction), a Carolinas nearshore buoy, a spectral NDBC buoy (left out
# by the provider), a KMA placeholder, a wind-only row, Lake Michigan, a UK platform
NDBC_KEEP = ("44137", "41110", "45007", "46026", "22101", "62145", "45029", "KPTN6", "41009")
GLOS_KEEP_DATASETS = (3, 139, 260, 2)          # Port Sheldon, Marquette Spotter, a silent one, Atwater
IOOS_KEEP = ("ism-secoora-41110-ilm2wave-ilm2w", "cap2wave-capers-nearshore-wave",
             "ism-secoora-cap2wave-capers-near", "gov-ndbc-45135", "edu_fit_sipf1", "mil_army_usace_frfn7")


def get(url, **kw):
    time.sleep(1.2)
    r = HTTP.get(url, timeout=90, **kw)
    r.raise_for_status()
    return r


def write(name, data):
    path = os.path.join(OUT, name)
    mode = "w" if isinstance(data, str) else "wb"
    with open(path, mode, **({"encoding": "utf-8", "newline": "\n"} if mode == "w" else {})) as f:
        f.write(data)
    print("wrote", name, len(data))


def main():
    os.makedirs(OUT, exist_ok=True)
    # NDBC latest_obs.txt: the two header lines + the kept stations
    lines = get("https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt").text.splitlines()
    keep = lines[:2] + [ln for ln in lines[2:] if ln.split() and ln.split()[0] in NDBC_KEEP]
    write("ndbc_latest_obs.txt", "\n".join(keep) + "\n")
    rt = get("https://www.ndbc.noaa.gov/data/realtime2/44137.txt").text.splitlines()
    write("ndbc_realtime2_44137.txt", "\n".join(rt[:62]) + "\n")
    # GLOS: roster cut to the kept datasets + one recovered and one wave-less platform
    roster = get("https://seagull-api.glos.org/api/v1/obs-datasets.geojson").json()
    feats = roster["features"]
    kept = [f for f in feats if f["properties"]["obs_dataset_id"] in GLOS_KEEP_DATASETS]
    rec = [f for f in feats if f["properties"]["platform_event"] == "recovered"][:1]
    nowave = [f for f in feats if f["properties"]["platform_event"] == "activated" and not any(
        p.get("standard_name") == "sea_surface_wave_significant_height" for p in f["properties"]["parameters"])][:1]
    write("glos_roster.geojson", json.dumps({"type": roster.get("type", "FeatureCollection"),
                                             "features": kept + rec + nowave}))
    start = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    obs = {}
    pids = set()
    for ds in (3, 139):
        ans = get("https://seagull-api.glos.org/api/v1/obs?obsDatasetId=%d&startDate=%s" % (ds, start)).json()
        obs[ds] = ans
        for block in ans:
            for par in block["parameters"]:
                pids.add(int(par["parameter_id"]))
                par["observations"] = par["observations"][:40]       # newest first: the last ~7 h at 10 min
    for ds, ans in obs.items():
        write("glos_obs_%d.json" % ds, json.dumps(ans))
    params = get("https://seagull-api.glos.org/api/v1/parameters").json()
    write("glos_parameters.json", json.dumps([p for p in params if int(p["parameter_id"]) in pids]))
    mirror = get("https://seagull-erddap.glos.org/erddap/tabledap/allDatasets.csv?datasetID,maxTime"
                 "&datasetID=~%22obs_.*%22").text.splitlines()
    write("glos_mirror.csv", "\n".join(mirror[:2] + [ln for ln in mirror[2:] if ln.split(",")[0] in
                                                     {"obs_%d" % d for d in GLOS_KEEP_DATASETS}]) + "\n")
    # IOOS: the wave-height index and allDatasets cut to the kept datasets, one dataset's 24 h
    base = "https://erddap.sensors.ioos.us/erddap/"
    idx = list(csv.reader(io.StringIO(get(
        base + "categorize/standard_name/sea_surface_wave_significant_height/index.csv?page=1&itemsPerPage=5000").text)))
    ix = {c: i for i, c in enumerate(idx[0])}
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows([idx[0]] + [r for r in idx[1:] if r[ix["Dataset ID"]] in IOOS_KEEP])
    write("ioos_index.csv", buf.getvalue())
    al = list(csv.reader(io.StringIO(get(
        base + "tabledap/allDatasets.csv?datasetID,institution,minLongitude,maxLongitude,minLatitude,maxLatitude,"
               "maxTime,title&maxTime%3E=now-2days").text)))
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows(al[:2] + [r for r in al[2:] if r[0] in IOOS_KEEP])
    write("ioos_alldatasets.csv", buf.getvalue())
    one = get(base + "tabledap/cap2wave-capers-nearshore-wave.csv?time,sea_surface_wave_significant_height,"
                     "sea_surface_wave_significant_height_qc_agg,"
                     "sea_surface_wave_period_at_variance_spectral_density_maximum,sea_surface_wave_mean_period,"
                     "sea_surface_wave_from_direction,sea_water_temperature"
                     "&time%3E=now-24hours&orderBy(%22time%22)").text
    write("ioos_cap2wave.csv", one)


if __name__ == "__main__":
    sys.exit(main())
