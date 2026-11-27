"""FES2022b tide predictions at the forecast points that have no tide station near (plan section 112, step 4).

    python -I tools/tides/fes_points.py --fes-dir FES2022B_OCEAN_EXTRAPOLATED --out build/tides-fes \
        [--consts consts.json] [--upload]

FES2022b (CNES / LEGOS / NOVELTIS / CLS, distributed by AVISO+) is licensed to the registered user: heights derived
from it may be published with credit, the model's files may NOT be re-shared. So this job runs on GitHub Actions with
the owner's AVISO+ credentials and publishes PREDICTIONS only; the constants it extracts stay in the Actions cache
(`--consts`, never uploaded).

Steps:
  1. points: the forecast stations (station_coords.json, zones station_timezones.json) farther than STATION_KM from
     every tide station the layer already has (NOAA tide_stations.json, TICON tide_global.json, the agencies'
     tide_national.json);
  2. constants (only when --consts is missing or was made for other points): per FES constituent file
     ("<name>_fes2022.nc": amplitude cm, phase deg, lat x lon, 1/30 deg, extrapolated over the coast), the complex
     value bilinearly from the 4 surrounding cells when all are ocean, else the nearest ocean cell within NEAREST_KM;
     a point is kept only if land (a cell without a value) lies within COAST_KM (the forecast stations far at sea need
     no tide) and an ocean cell within NEAREST_KM;
  3. LAT per point over 18.61 years (tide_harmonic, convention 'fes');
  4. predictions: from 00:00 UTC two days back, DAYS days at 30 minutes + the highs and lows (6-min series), heights
     above LAT in cm -> out/p/<id>.json; the index out/points.json (id, name, lat, lon, zone);
  5. --upload: R2 (env R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET) under tides/fes/v1/, the point
     files first, the index last.
"""
import argparse
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)
import tide_harmonic as H  # noqa: E402
import build_global as BG  # noqa: E402

PREFIX = "tides/fes/v1"
STATION_KM = 10.0
COAST_KM = 30.0
NEAREST_KM = 15.0
DAYS = 47
STEP_S = 1800
EX_STEP_S = 360
# FES2022b's 34 ocean-tide constituents (file name stem -> tide_harmonic name)
FES_NAMES = {"2n2": "2n2", "eps2": "eps2", "j1": "j1", "k1": "k1", "k2": "k2", "l2": "l2", "lambda2": "lambda2",
             "m2": "m2", "m3": "m3", "m4": "m4", "m6": "m6", "m8": "m8", "mf": "mf", "mks2": "mks2", "mm": "mm",
             "mn4": "mn4", "ms4": "ms4", "msf": "msf", "msqm": "msqm", "mtm": "mtm", "mu2": "mu2", "n2": "n2",
             "n4": "n4", "nu2": "nu2", "o1": "o1", "p1": "p1", "q1": "q1", "r2": "r2", "s1": "s1", "s2": "s2",
             "s4": "s4", "sa": "sa", "ssa": "ssa", "t2": "t2"}
CREDIT = ("FES2022b was produced by NOVELTIS, LEGOS, CLS Space Oceanography Division and CNES; it is distributed by "
          "AVISO+, with support from CNES (https://www.aviso.altimetry.fr/)")


def km(a_lat, a_lon, b_lat, b_lon):
    return BG.km(a_lat, a_lon, b_lat, b_lon)


def forecast_points():
    coords = json.load(open(os.path.join(ROOT, "station_coords.json"), encoding="utf-8"))
    zones = json.load(open(os.path.join(ROOT, "station_timezones.json"), encoding="utf-8"))
    taken = []
    noaa = json.load(open(os.path.join(ROOT, "tide_stations.json"), encoding="utf-8"))
    f = noaa["fields"]
    taken += [(r[f.index("lat")], r[f.index("lon")]) for r in noaa["stations"]]
    for name in ("tide_global.json", "tide_national.json"):
        path = os.path.join(ROOT, name)
        if os.path.exists(path):
            d = json.load(open(path, encoding="utf-8"))
            f = d["fields"]
            taken += [(r[f.index("lat")], r[f.index("lon")]) for r in d["stations"]]
    grid = BG.Grid(taken)
    out = []
    for sid, c in sorted(coords.items()):
        lat, lon = float(c["lat"]), (float(c["lon"]) + 180.0) % 360.0 - 180.0
        if grid.near(lat, lon, STATION_KM):
            continue
        out.append({"id": "model:" + sid.lower(), "src": sid, "lat": lat, "lon": lon, "tz": zones.get(sid, "UTC")})
    return out


def _points_hash(points):
    return hashlib.sha256(json.dumps([(p["id"], p["lat"], p["lon"]) for p in points]).encode()).hexdigest()


def read_grid(path):
    """(lat[], lon[] in 0..360, amplitude m, phase deg) of one FES2022b file (NetCDF-4 = HDF5)."""
    import h5py
    with h5py.File(path, "r") as h:
        lat = np.asarray(h["lat"][:], dtype=float)
        lon = np.asarray(h["lon"][:], dtype=float) % 360.0
        amp = np.asarray(h["amplitude"][:], dtype=np.float32)
        pha = np.asarray(h["phase"][:], dtype=np.float32)
        fill = h["amplitude"].attrs.get("_FillValue")
    bad = ~np.isfinite(amp) | (amp > 1e5) | (amp < 0)
    if fill is not None:
        bad |= amp == np.float32(np.asarray(fill).ravel()[0])
    amp = np.where(bad, np.nan, amp / 100.0)                       # cm -> m
    return lat, lon, amp, np.where(bad, np.nan, pha)


def sample(lat, lon, amp, pha, plat, plon):
    """(complex value, km to the cell used, land within COAST_KM) at one point, or (None, None, coast)."""
    dlat, dlon = lat[1] - lat[0], lon[1] - lon[0]
    i = (plat - lat[0]) / dlat
    j = ((plon % 360.0) - lon[0]) / dlon
    i0, j0 = int(math.floor(i)), int(math.floor(j))
    n_lat, n_lon = len(lat), len(lon)
    r_cells = int(math.ceil(COAST_KM / (111.2 * abs(dlat)))) + 1
    ii = np.arange(max(0, i0 - r_cells), min(n_lat, i0 + r_cells + 2))
    jj = np.arange(j0 - r_cells, j0 + r_cells + 2) % n_lon
    win = amp[np.ix_(ii, jj)]
    coast = bool(np.isnan(win).any())
    corners = [(i0, j0), (i0 + 1, j0), (i0, j0 + 1), (i0 + 1, j0 + 1)]
    if 0 <= i0 and i0 + 1 < n_lat:
        vals = [amp[a, b % n_lon] * np.exp(-1j * np.radians(pha[a, b % n_lon])) for a, b in corners]
        if all(np.isfinite(v) for v in vals):
            fi, fj = i - i0, j - j0
            v = (vals[0] * (1 - fi) * (1 - fj) + vals[1] * fi * (1 - fj) + vals[2] * (1 - fi) * fj + vals[3] * fi * fj)
            return v, 0.0, coast
    best = None
    for a in ii:
        for b in jj:
            if np.isfinite(amp[a, b]):
                d = km(plat, plon, lat[a], ((lon[b] + 180.0) % 360.0) - 180.0)
                if d <= NEAREST_KM and (best is None or d < best[0]):
                    best = (d, a, b)
    if best is None:
        return None, None, coast
    d, a, b = best
    return amp[a, b] * np.exp(-1j * np.radians(pha[a, b])), d, coast


def extract(points, fes_dir, log=print):
    consts = {p["id"]: {"c": [], "km": 0.0, "coast": False} for p in points}
    dead = set()
    for stem, name in sorted(FES_NAMES.items()):
        path = os.path.join(fes_dir, "%s_fes2022.nc" % stem)
        if not os.path.exists(path):
            log("missing %s: left out" % path)
            continue
        lat, lon, amp, pha = read_grid(path)
        for p in points:
            if p["id"] in dead:
                continue
            v, d, coast = sample(lat, lon, amp, pha, p["lat"], p["lon"])
            c = consts[p["id"]]
            if stem == "m2":
                c["coast"] = coast
            if v is None:
                if stem == "m2":
                    dead.add(p["id"])
                continue
            c["km"] = max(c["km"], d)
            c["c"].append([name, round(float(abs(v)), 5), round(float(np.degrees(-np.angle(v))) % 360.0, 2)])
        log("%s: done" % stem)
    return {k: v for k, v in consts.items() if k not in dead and v["coast"] and v["c"]}


def predict_point(prep, lat0, begin, days=DAYS):
    t = begin + STEP_S * np.arange(days * 86400 // STEP_S + 1)
    v = H.predict(prep, t) + lat0
    te = np.arange(begin, t[-1] + 1, EX_STEP_S, dtype=float)
    ex = H.extremes(te, H.predict(prep, te) + lat0)
    return {"begin": int(begin), "step": STEP_S, "v": [int(round(x * 100)) for x in v],
            "hilo": [[int(round(tt / 60.0)) * 60, int(round(h * 100)), k] for tt, h, k in ex]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fes-dir")
    ap.add_argument("--out", required=True)
    ap.add_argument("--consts", default=os.path.join(HERE, ".fes_consts.json"))
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--geonames")
    args = ap.parse_args()
    points = forecast_points()
    ph = _points_hash(points)
    cached = None
    if os.path.exists(args.consts):
        cached = json.load(open(args.consts, encoding="utf-8"))
        if cached.get("points") != ph:
            cached = None
    if cached is None:
        if not args.fes_dir:
            raise SystemExit("no constants cached for these points and no --fes-dir")
        consts = extract(points, args.fes_dir)
        tab = H.table()
        keep = [p for p in points if p["id"] in consts]
        stations = [{"consts_m": [tuple(c) for c in consts[p["id"]]["c"]]} for p in keep]
        lat0 = _lat_offsets_fes(stations, tab)
        for p, z in zip(keep, lat0):
            consts[p["id"]]["lat0"] = round(float(z), 4)
        cached = {"points": ph, "made": datetime.now(timezone.utc).isoformat(), "consts": consts}
        with open(args.consts, "w", encoding="utf-8") as fh:              # stays in the Actions cache, never uploaded
            json.dump(cached, fh, separators=(",", ":"))
    consts = cached["consts"]
    keep = [p for p in points if p["id"] in consts]
    places = BG.load_geonames(args.geonames)
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    begin = int((today - timedelta(days=2)).timestamp())
    os.makedirs(os.path.join(args.out, "p"), exist_ok=True)
    index = []
    for p in keep:
        c = consts[p["id"]]
        prep = H.prepare([tuple(x) for x in c["c"]], convention="fes")
        doc = predict_point(prep, c["lat0"], begin)
        doc["id"] = p["id"]
        with open(os.path.join(args.out, "p", p["id"].split(":", 1)[1] + ".json"), "w", encoding="utf-8") as fh:
            json.dump(doc, fh, separators=(",", ":"))
        near = places.near(p["lat"], p["lon"], 40.0)
        name = ("Off %s" % near[0][1][2]) if near else "Sea %.2f, %.2f" % (p["lat"], p["lon"])
        index.append([p["id"], name, round(p["lat"], 4), round(p["lon"], 4), p["tz"]])
    meta = {"made": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"), "begin": begin, "days": DAYS,
            "units": "cm above LAT", "credit": CREDIT, "fields": ["id", "name", "lat", "lon", "tz"], "points": index}
    with open(os.path.join(args.out, "points.json"), "w", encoding="utf-8") as fh:
        json.dump(meta, fh, separators=(",", ":"), ensure_ascii=False)
    print("points: %d forecast points, %d with a model tide" % (len(points), len(index)))
    if args.upload:
        upload(args.out, index)


def _lat_offsets_fes(stations, tab):
    """build_global.lat_offsets with the FES convention (pyTMD's SA)."""
    saved = H.CONVENTIONS.get("ticon")
    H.CONVENTIONS["ticon"] = H.CONVENTIONS["fes"]                  # lat_offsets prepares with 'ticon'
    try:
        return BG.lat_offsets(stations, tab)
    finally:
        H.CONVENTIONS["ticon"] = saved


def upload(out_dir, index):
    import boto3
    from botocore.config import Config
    s3 = boto3.client("s3", endpoint_url="https://%s.r2.cloudflarestorage.com" % os.environ["R2_ACCOUNT_ID"],
                      aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                      aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto",
                      config=Config(retries={"max_attempts": 5, "mode": "standard"},
                                    request_checksum_calculation="when_required",
                                    response_checksum_validation="when_required"))
    bucket = os.environ["R2_BUCKET"]
    for row in index:
        local = row[0].split(":", 1)[1]
        with open(os.path.join(out_dir, "p", local + ".json"), "rb") as fh:
            s3.put_object(Bucket=bucket, Key="%s/p/%s.json" % (PREFIX, local), Body=fh.read(),
                          ContentType="application/json", CacheControl="public, max-age=21600")
    with open(os.path.join(out_dir, "points.json"), "rb") as fh:          # the index last
        s3.put_object(Bucket=bucket, Key="%s/points.json" % PREFIX, Body=fh.read(),
                      ContentType="application/json", CacheControl="public, max-age=3600")
    print("uploaded %d point files + points.json under %s/" % (len(index), PREFIX))


if __name__ == "__main__":
    main()
