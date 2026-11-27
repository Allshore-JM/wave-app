"""FES2022b tide predictions at the forecast points that have no tide station near (plan section 112, step 4), and
along the coasts with no tide point near (plan section 113 C: tools/tides/coast_points.json, every 25 km).

    python -I tools/tides/fes_points.py --fes-dir FES2022B_OCEAN_EXTRAPOLATED --mask mask_fes2022B.nc \
        --out build/tides-fes [--consts consts.json] [--upload]

FES2022b (CNES / LEGOS / NOVELTIS / CLS, distributed by AVISO+) is licensed to the registered user: heights derived
from it may be published with credit, the model's files may NOT be re-shared. So this job runs on GitHub Actions with
the owner's AVISO+ credentials and publishes PREDICTIONS only; the constants it extracts stay in the Actions cache
(`--consts`, never uploaded).

Steps:
  1. points: the forecast stations (station_coords.json, zones station_timezones.json) farther than STATION_KM from
     every tide station the layer already has (NOAA tide_stations.json, TICON tide_global.json, the agencies'
     tide_national.json), and the coast points (tools/tides/coast_points.json, built by build_coast_points.py: 15 km
     clear of those stations and of the forecast points);
  2. constants (only when --consts is missing or was made for other points): per FES constituent file
     ("<name>_fes2022.nc": amplitude cm, phase deg, lat x lon, 1/30 deg, extrapolated over the coast), the complex
     value bilinearly from the 4 surrounding cells when all are ocean, else the nearest ocean cell within NEAREST_KM;
     a point is kept only if land (a cell without a value) lies within COAST_KM (the forecast stations far at sea need
     no tide) and an ocean cell within NEAREST_KM. The extrapolated grids carry values over land too, so "ocean" is
     AVISO+'s mask (`--mask mask_fes2022B.nc`, 0 = native ocean, 1 = extrapolated, 2 = land, 3 = lake): every cell
     not 0 is read as no value (run 45958228397 had no mask: no point saw land, ~no constants);
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
import re
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
MIN_KEPT = 0.05          # fewer points than this share with a model tide = a broken land test: stop, publish nothing
                         # (run 46037126451 kept 452 of 3958 = 11.4%; the broken run kept 2)
MIN_KEPT_COAST = 0.50    # the coast points lie on the coast: most must get a model tide (section 113 C)
COAST_PATH = os.path.join(HERE, "coast_points.json")
COAST_NEAREST_KM = 6.0   # a coast point takes the sea's tide only from ocean this near (G80-3: not across a lagoon's
                         # barrier beach or from the sea beyond a lake's shore); no land test (G80-7: small islands)
UPLOAD_THREADS = 8
DELETE_BATCH = 250       # R2 delete_objects per request (tools/model_frames/publish.py's size after R2's 2026-11-03 timeouts)
CONST_VERSION = "2"      # bump whenever the way constants are made changes (the cache key: --cache-key)
COAST_ID_RE = re.compile(r"model:c\d{10}")
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


def coast_points(path=COAST_PATH, required=True):
    """The coast points (section 113 C) as fes_points' points ("set": "coast", with their names). Without the file:
    SystemExit (G80-5: a missing file must not empty the coast set and delete its files), or [] when not required."""
    if not os.path.exists(path):
        if required:
            raise SystemExit("%s is missing: no coast points (rebuild it with build_coast_points.py)" % path)
        return []
    d = json.load(open(path, encoding="utf-8"))
    f = d["fields"]
    out = []
    for row in d["points"]:
        r = dict(zip(f, row))
        out.append({"id": str(r["id"]), "src": "coast", "lat": float(r["lat"]), "lon": float(r["lon"]),
                    "tz": r.get("tz") or "UTC", "set": "coast", "name": r.get("name")})
    return out


def all_points(coast_path=COAST_PATH, required=True):
    """The forecast points, then the coast points (an id both have: the forecast point's)."""
    pts = forecast_points()
    seen = {p["id"] for p in pts}
    return pts + [p for p in coast_points(coast_path, required) if p["id"] not in seen]


def cache_key(points):
    """The Actions cache key of the constants: the points' positions and CONST_VERSION only, so a change of names or
    of code that does not change the constants keeps the cache (G80-1)."""
    return "fes-consts-v2-" + hashlib.sha256((_points_hash(points) + "|" + CONST_VERSION).encode()).hexdigest()[:40]


def _points_hash(points):
    return hashlib.sha256(json.dumps([(p["id"], p["lat"], p["lon"]) for p in points]).encode()).hexdigest()


def read_mask(path):
    """(lat[], lon[] in 0..360, ocean bool[lat, lon]) from AVISO+'s mask_fes2022B.nc: only native ocean (0) is ocean;
    extrapolated (1), land (2), lake (3) and fill are not."""
    import h5py
    with h5py.File(path, "r") as h:
        lat = np.asarray(h["lat"][:], dtype=float)
        lon = np.asarray(h["lon"][:], dtype=float) % 360.0
        m = np.asarray(h["mask"][:], dtype=np.float32)
    return lat, lon, m == 0


def read_grid(path, mask=None):
    """(lat[], lon[] in 0..360, amplitude m, phase deg) of one FES2022b file (NetCDF-4 = HDF5); with `mask` (from
    read_mask, the same grid or the file is refused) every cell that is not native ocean has no value."""
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
    if mask is not None:
        mlat, mlon, ocean = mask
        if (ocean.shape != amp.shape or not np.allclose(mlat, lat, atol=1e-6)
                or not np.allclose(mlon, lon, atol=1e-6)):
            raise SystemExit("the mask's grid is not %s's grid" % os.path.basename(path))
        bad |= ~ocean
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
    # the nearest cell with a value within NEAREST_KM: the window's distances at once (section 113 C: ~17,000 coast
    # points x 34 files; the first of equal distances in row order, as the cell-by-cell loop it replaces chose)
    ok = np.isfinite(win)
    if not ok.any():
        return None, None, coast
    p1, p2 = math.radians(plat), np.radians(lat[ii])[:, None]
    dl = np.radians(((lon[jj] + 180.0) % 360.0 - 180.0) - plon)[None, :]
    h = np.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    dist = np.where(ok, 12742.0 * np.arcsin(np.minimum(1.0, np.sqrt(h))), np.inf)
    k = int(np.argmin(dist))
    d = float(dist.flat[k])
    if not d <= NEAREST_KM:
        return None, None, coast
    a, b = ii[k // len(jj)], jj[k % len(jj)]
    return amp[a, b] * np.exp(-1j * np.radians(pha[a, b])), d, coast


def new_partial(points):
    return {"points": _points_hash(points), "done": [], "dead": [],
            "consts": {p["id"]: {"c": [], "km": 0.0, "coast": False} for p in points}}


def add_file(points, partial, path, log=print, mask=None):
    """One FES constituent file into the partial constants (the workflow downloads, adds and deletes one file at a
    time, so the runner's disk holds one global grid, not 34: G76 P2-9)."""
    stem = os.path.basename(path).split("_fes2022")[0].lower()
    if stem not in FES_NAMES:
        raise SystemExit("not a FES2022b constituent file: %s" % path)
    if stem in partial["done"]:
        return partial
    lat, lon, amp, pha = read_grid(path, mask)
    dead = set(partial["dead"])
    name = FES_NAMES[stem]
    for p in points:
        if p["id"] in dead:
            continue
        v, d, coast = sample(lat, lon, amp, pha, p["lat"], p["lon"])
        c = partial["consts"][p["id"]]
        is_coast = p.get("set") == "coast"
        if stem == "m2":
            c["coast"] = True if is_coast else coast    # a coast point lies on the coast (G80-7: small islands)
            if is_coast and v is not None and d > COAST_NEAREST_KM:
                v = None                                # no sea at the point itself: a lake, a lagoon (G80-3)
        if v is None:
            if stem == "m2":
                dead.add(p["id"])
            continue
        c["km"] = max(c["km"], d)
        c["c"].append([name, round(float(abs(v)), 5), round(float(np.degrees(-np.angle(v))) % 360.0, 2)])
    partial["dead"] = sorted(dead)
    partial["done"].append(stem)
    if stem == "m2":
        alive = [p["id"] for p in points if p["id"] not in dead]
        log("m2: %d of %d points with an ocean value, %d of them with land within %g km"
            % (len(alive), len(points), sum(partial["consts"][k]["coast"] for k in alive), COAST_KM))
        coast = [p for p in points if p.get("set") == "coast"]
        log("m2: coast points: %d of %d with ocean within %g km" % (sum(p["id"] not in dead for p in coast), len(coast),
                                                                       COAST_NEAREST_KM))
    log("%s: done" % stem)
    return partial


def finished(partial):
    """The points worth a model tide: an M2 value, land within COAST_KM, at least one constituent."""
    if "m2" not in partial["done"]:
        raise SystemExit("M2 was never added: no constants")
    dead = set(partial["dead"])
    return {k: v for k, v in partial["consts"].items() if k not in dead and v["coast"] and v["c"]}


def extract(points, fes_dir, log=print, mask=None):
    partial = new_partial(points)
    for stem in sorted(FES_NAMES):
        path = os.path.join(fes_dir, "%s_fes2022.nc" % stem)
        if not os.path.exists(path):
            log("missing %s: left out" % path)
            continue
        add_file(points, partial, path, log, mask)
    return finished(partial)


def check_kept(n_kept, n_points, share=MIN_KEPT, what="points"):
    """Refuse a result where almost no point got a model tide (the extrapolated grids read without the mask gave
    exactly that, and the job passed: run 45958228397)."""
    if n_points and n_kept < share * n_points:
        raise SystemExit("only %d of %d %s have a model tide (< %d%%): the land test is broken; nothing published"
                         % (n_kept, n_points, what, round(share * 100)))


SETS = (("forecast", MIN_KEPT), ("coast", MIN_KEPT_COAST))


def set_failures(points, kept_ids):
    """{set name: message} for the sets with too few points kept (check_kept's test, per set, without raising)."""
    out = {}
    for name, share in SETS:
        mine = [p for p in points if p.get("set", "forecast") == name]
        try:
            check_kept(sum(p["id"] in kept_ids for p in mine), len(mine), share, name + " points")
        except SystemExit as exc:
            out[name] = str(exc)
    return out


def check_sets(points, kept_ids):
    """check_kept for the forecast points (MIN_KEPT: most are at sea) and the coast points (MIN_KEPT_COAST)."""
    failed = set_failures(points, kept_ids)
    if failed:
        raise SystemExit("; ".join(failed.values()))


def drop_coast_near_forecast(keep, near_km=None):
    """A coast point within NEAR_KM (build_coast_points) of a forecast point that has a model tide is left out: that
    stretch of coast has its tide point (G80-7: a forecast point without one leaves its coast points in)."""
    import build_coast_points as C
    near_km = C.NEAR_KM if near_km is None else near_km
    grid = BG.Grid([(p["lat"], p["lon"]) for p in keep if p.get("set", "forecast") == "forecast"])
    return [p for p in keep if p.get("set") != "coast" or not grid.near(p["lat"], p["lon"], near_km)]


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
    ap.add_argument("--add-file", help="add one FES constituent file to --partial and stop")
    ap.add_argument("--mask", help="AVISO+'s mask_fes2022B.nc (needed to make constants from the extrapolated grids)")
    ap.add_argument("--partial", default=os.path.join(HERE, ".fes_partial.json"))
    ap.add_argument("--out")
    ap.add_argument("--consts", default=os.path.join(HERE, ".fes_consts.json"))
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--geonames")
    ap.add_argument("--cache-key", action="store_true", help="print key=<the constants' Actions cache key> and stop")
    args = ap.parse_args()
    points = all_points()
    ph = _points_hash(points)
    if args.cache_key:
        print("key=" + cache_key(points))
        return
    if (args.add_file or args.fes_dir) and not args.mask:
        raise SystemExit("--mask is needed: the extrapolated grids have values over land")
    mask = read_mask(args.mask) if (args.add_file or args.fes_dir) else None
    if args.add_file:                                                   # the workflow's one-file-at-a-time step
        partial = json.load(open(args.partial, encoding="utf-8")) if os.path.exists(args.partial) else None
        if partial is None or partial.get("points") != ph:
            partial = new_partial(points)
        add_file(points, partial, args.add_file, mask=mask)
        with open(args.partial, "w", encoding="utf-8") as fh:            # never uploaded (the Actions runner only)
            json.dump(partial, fh, separators=(",", ":"))
        return
    if not args.out:
        raise SystemExit("--out is needed")
    cached = None
    if os.path.exists(args.consts):
        cached = json.load(open(args.consts, encoding="utf-8"))
        if cached.get("points") != ph:
            cached = None
    if cached is None:
        if args.fes_dir:
            consts = extract(points, args.fes_dir, mask=mask)
        elif os.path.exists(args.partial):
            partial = json.load(open(args.partial, encoding="utf-8"))
            if partial.get("points") != ph:
                raise SystemExit("the partial constants were made for other points")
            consts = finished(partial)
            os.remove(args.partial)
        else:
            raise SystemExit("no constants cached for these points, no --fes-dir and no partial file")
        tab = H.table()
        keep = [p for p in points if p["id"] in consts]
        if len(set_failures(points, set(consts))) == len(SETS):        # every set short: a broken land test, nothing
            check_sets(points, set(consts))                             # cached (run 45958228397); one set short: cached
        stations = [{"consts_m": [tuple(c) for c in consts[p["id"]]["c"]]} for p in keep]
        lat0 = _lat_offsets_fes(stations, tab)
        for p, z in zip(keep, lat0):
            consts[p["id"]]["lat0"] = round(float(z), 4)
        cached = {"points": ph, "made": datetime.now(timezone.utc).isoformat(), "consts": consts}
        with open(args.consts, "w", encoding="utf-8") as fh:              # stays in the Actions cache, never uploaded
            json.dump(cached, fh, separators=(",", ":"))
    consts = cached["consts"]
    keep = [p for p in points if p["id"] in consts]
    # G80-2: each set stands on its own: a set short of its share is not published (its previous rows are carried over
    # from the index online) and the job fails at the end, after the other set is published and the constants cached
    failed = set_failures(points, set(consts))
    if len(failed) == len(SETS):
        check_sets(points, set(consts))
    keep = drop_coast_near_forecast([p for p in keep if p.get("set", "forecast") not in failed])
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
        name = point_name(p, places)
        index.append([p["id"], name, round(p["lat"], 4), round(p["lon"], 4), p["tz"], p.get("set", "forecast")])
    meta = {"made": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"), "begin": begin, "days": DAYS,
            "units": "cm above LAT", "credit": CREDIT, "fields": ["id", "name", "lat", "lon", "tz", "set"],
            "points": index}
    with open(os.path.join(args.out, "points.json"), "w", encoding="utf-8") as fh:
        json.dump(meta, fh, separators=(",", ":"), ensure_ascii=False)
    nf = sum(1 for p in points if p.get("set", "forecast") == "forecast")
    kf = sum(1 for p in keep if p.get("set", "forecast") == "forecast")
    print("points: %d forecast points, %d with a model tide" % (nf, kf))
    print("coast points: %d, %d with a model tide" % (len(points) - nf, len(index) - kf))
    if args.upload:
        upload(args.out, index, carry=set(failed))
    if failed:
        raise SystemExit("not published: " + "; ".join(failed.values()))


def point_name(p, places):
    """A point's name: a coast point's own (made by build_coast_points.py: G80-1); else "Off <the nearest GeoNames town
    within 40 km>" (a proper place name, never a surf spot: owner, 2026-11-27), else its position ("Sea 12.34, 56.78")."""
    if p.get("name"):
        return p["name"]
    near = places.near(p["lat"], p["lon"], 40.0)
    if near:
        return "Off %s" % near[0][1][2]
    return ("Coast %.2f, %.2f" if p.get("set") == "coast" else "Sea %.2f, %.2f") % (p["lat"], p["lon"])


def _lat_offsets_fes(stations, tab):
    """build_global.lat_offsets with the FES convention (pyTMD's SA)."""
    saved = H.CONVENTIONS.get("ticon")
    H.CONVENTIONS["ticon"] = H.CONVENTIONS["fes"]                  # lat_offsets prepares with 'ticon'
    try:
        return BG.lat_offsets(stations, tab)
    finally:
        H.CONVENTIONS["ticon"] = saved


def row_set(row):
    """An index row's set: its "set" column, else by id (indexes before section 113 C had none)."""
    if len(row) > 5 and row[5] in ("forecast", "coast"):
        return row[5]
    return "coast" if COAST_ID_RE.fullmatch(str(row[0])) else "forecast"


def upload(out_dir, index, carry=(), s3=None, bucket=None):
    """The point files (UPLOAD_THREADS at a time), then the index (rows of a set in `carry` come from the index online:
    G80-2), then the point files that neither the old nor the new index lists are deleted (G80-5: a point dropped now
    goes at the next run, after the site, the edge and open pages have moved on), DELETE_BATCH a request, errors raise."""
    if s3 is None:
        import boto3
        from botocore.config import Config
        s3 = boto3.client("s3", endpoint_url="https://%s.r2.cloudflarestorage.com" % os.environ["R2_ACCOUNT_ID"],
                          aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                          aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto",
                          config=Config(retries={"max_attempts": 5, "mode": "standard"},
                                        request_checksum_calculation="when_required",
                                        response_checksum_validation="when_required"))
        bucket = os.environ["R2_BUCKET"]
    old = old_index(s3, bucket)
    if carry:
        index = list(index) + [r for r in old if row_set(r) in carry]
        path = os.path.join(out_dir, "points.json")
        meta = json.load(open(path, encoding="utf-8"))
        meta["points"] = index
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(meta, fh, separators=(",", ":"), ensure_ascii=False)

    def put(row):
        local = row[0].split(":", 1)[1]
        with open(os.path.join(out_dir, "p", local + ".json"), "rb") as fh:
            s3.put_object(Bucket=bucket, Key="%s/p/%s.json" % (PREFIX, local), Body=fh.read(),
                          ContentType="application/json", CacheControl="public, max-age=21600")

    from concurrent.futures import ThreadPoolExecutor
    fresh = [r for r in index if row_set(r) not in carry]
    with ThreadPoolExecutor(UPLOAD_THREADS) as pool:                  # ~17,000 files: a few at a time (section 113 C)
        list(pool.map(put, fresh))                                    # an error raises here: the index is not sent
    with open(os.path.join(out_dir, "points.json"), "rb") as fh:          # the index last
        s3.put_object(Bucket=bucket, Key="%s/points.json" % PREFIX, Body=fh.read(),
                      ContentType="application/json", CacheControl="public, max-age=3600")
    print("uploaded %d point files + points.json under %s/ (%d rows carried over)" % (len(fresh), PREFIX,
                                                                                     len(index) - len(fresh)))
    gone = stale_keys(s3, bucket, index, old)
    for i in range(0, len(gone), DELETE_BATCH):
        res = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in gone[i:i + DELETE_BATCH]],
                                                       "Quiet": True})
        if res.get("Errors"):
            raise SystemExit("R2 refused to delete %d point files: %s" % (len(res["Errors"]), res["Errors"][:3]))
    print("removed %d point files neither index lists" % len(gone))


def old_index(s3, bucket):
    """The rows of the index online, or [] when there is none yet."""
    try:
        body = s3.get_object(Bucket=bucket, Key="%s/points.json" % PREFIX)["Body"].read()
    except Exception as exc:                                         # noqa: BLE001
        if "NoSuchKey" in type(exc).__name__ or "NoSuchKey" in str(exc) or "404" in str(exc):
            return []
        raise
    return json.loads(body.decode("utf-8")).get("points") or []


def stale_keys(s3, bucket, index, old=()):
    """The point files under PREFIX/p/ that neither the new index nor the old one lists."""
    want = {"%s/p/%s.json" % (PREFIX, row[0].split(":", 1)[1]) for row in list(index) + list(old)}
    gone, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": PREFIX + "/p/"}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        gone += [o["Key"] for o in page.get("Contents", []) if o["Key"] not in want]
        if not page.get("IsTruncated"):
            return gone
        token = page.get("NextContinuationToken")


if __name__ == "__main__":
    main()
