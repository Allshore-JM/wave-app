"""Build tide_global.json: the worldwide tide-gauge stations for the Tide layer (plan section 112).

    python -I tools/tides/build_global.py --ticon4 TICON_4.csv --ticon3 TICON_3.zip [--geonames cities1000.zip]

Sources (both CC BY 4.0, anonymous):
  TICON-4  Hart-Davis, Dettmering & Seitz (DGFI-TUM), SEANOE 2025, https://www.seanoe.org/data/00980/109129/
           (4,838 gauges x 50 constituents; amplitudes in cm, Greenwich phase lags in degrees; from GESLA-4)
  TICON-3  the same group, PANGAEA 2022, https://doi.pangaea.de/10.1594/PANGAEA.951610 (gauges TICON-4 lacks)
  GeoNames cities1000 (CC BY 4.0) names the gauges whose file name is only a code.

Rules:
  * coastal gauges only (TICON's type); records flagged for "quality control issues" are left out (a datum flag is
    kept: the layer measures from its own computed LAT);
  * one station per gauge: TICON-4 records within DEDUP_KM keep the best (no flag, then the longest record), TICON-3
    adds a gauge only farther than DEDUP_KM from every TICON-4 one;
  * a gauge within NOAA_KM of a NOAA station (tide_stations.json) is left out: NOAA's own predictions are shown there;
  * constituents with an amplitude under MIN_AMP_CM are dropped (zeroed ones too: TICON zeroes unreliable ones);
  * each station's LAT: the lowest hourly height over 18.61 years from 2020 (tide_harmonic, convention 'ticon'),
    stored as `lat0` (metres below the constants' mean level) so heights above LAT = prediction + lat0;
  * zones: the live buoys' nearest-civil-zone rule (app._nearest_civil_tz).
"""
import argparse
import csv
import io
import json
import math
import os
import re
import sys
import urllib.request
import zipfile
from datetime import datetime, timezone

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)
import tide_harmonic as H  # noqa: E402

OUT = os.path.join(ROOT, "tide_global.json")
NOAA = os.path.join(ROOT, "tide_stations.json")
GEONAMES_URL = "https://download.geonames.org/export/dump/cities1000.zip"
DEDUP_KM = 3.0                     # two records of one harbour (Andenes, Vardo: 1-3 km apart) are one station
NOAA_KM = 2.0
NAME_KM = 15.0
MIN_AMP_CM = 0.5
LAT_START = datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp()
LAT_YEARS = 18.61
QUALITY_OK = ("No obvious issues", "Possible datum issues")
SMALL = {"of", "de", "la", "le", "du", "des", "del", "da", "do", "das", "dos", "di", "el", "en", "y", "and", "on", "am",
         "an", "der", "den", "van", "aan", "sur", "et"}
UPPER = {"bc", "nb", "ns", "pe", "nl", "qc", "on", "uk", "usa", "pr", "nz"}


def km(a_lat, a_lon, b_lat, b_lon):
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    dp, dl = p2 - p1, math.radians(b_lon - a_lon)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 12742.0 * math.asin(min(1.0, math.sqrt(h)))


class Grid:
    """Points bucketed by 1-degree cell for 'anything within d km' questions."""

    def __init__(self, pts):
        self.cells = {}
        for p in pts:
            self.cells.setdefault((math.floor(p[0]), math.floor(p[1])), []).append(p)

    def near(self, lat, lon, d_km):
        out = []
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                for p in self.cells.get((math.floor(lat) + dy, (math.floor(lon) + dx + 180) % 360 - 180), []):
                    dist = km(lat, lon, p[0], p[1])
                    if dist <= d_km:
                        out.append((dist, p))
        out.sort(key=lambda x: x[0])
        return out


def code_name(token):
    """A TICON file-name token that is only a code (Copernicus 'maretg_60minute', 'algecirastg', digits): None.
    Otherwise the readable name ('point_atkinson_bc' -> 'Point Atkinson BC')."""
    t = token.lower()
    t = re.sub(r"_\d+minute$", "", t)
    if re.search(r"\d", t) or len(t) < 3 or t.endswith("tg"):
        return None
    t = re.sub(r"^(rmn|rmlv|rmn_)_?", "", t) if t.startswith("rmn_") else t
    words = [w for w in t.split("_") if w]
    out = []
    for i, w in enumerate(words):
        if w in UPPER and i > 0:
            out.append(w.upper())
        elif w in SMALL and i > 0:
            out.append(w)
        else:
            out.append(w[:1].upper() + w[1:])
    return " ".join(out)


def load_geonames(path):
    if not path:
        cache = os.path.join(HERE, ".cities1000.zip")
        if not os.path.exists(cache):
            print("downloading", GEONAMES_URL)
            with urllib.request.urlopen(urllib.request.Request(GEONAMES_URL, headers={"User-Agent": "allshoresurf.com tools"}),
                                        timeout=300) as r:
                open(cache, "wb").write(r.read())
        path = cache
    z = zipfile.ZipFile(path)
    pts = []
    for line in io.TextIOWrapper(z.open("cities1000.txt"), encoding="utf-8"):
        f = line.rstrip("\n").split("\t")
        if len(f) > 14 and f[6] == "P":
            pts.append((float(f[4]), float(f[5]), f[1], int(f[14] or 0)))
    return Grid(pts)


def read_ticon4(path):
    gauges = {}
    with open(path, encoding="utf-8") as fh:
        for d in csv.DictReader(fh):
            g = gauges.setdefault(d["tide_gauge_name"], {
                "key": d["tide_gauge_name"], "lat": float(d["lat"]), "lon": float(d["lon"]), "type": d["type"],
                "country": d["country"], "quality": d["record_quality"], "years": float(d["years_of_obs"] or 0),
                "source": d["gesla_source"], "consts": [], "ticon": 4})
            g["consts"].append((d["con"], float(d["amp"]), float(d["pha"])))
    return list(gauges.values())


def read_ticon3(path):
    z = zipfile.ZipFile(path)
    gauges = {}
    for line in io.TextIOWrapper(z.open("TICON_3.txt"), encoding="utf-8"):
        f = line.split()                       # columns separated by runs of tabs
        if len(f) < 14:
            continue
        try:
            float(f[0]), float(f[1]), float(f[3]), float(f[4])
        except ValueError:
            continue
        key = (f[0].strip(), f[1].strip(), f[12].strip())
        lon = (float(f[1]) + 180.0) % 360.0 - 180.0             # TICON-3 gives 0-360
        g = gauges.setdefault(key, {"key": "ticon3_%s_%s" % (f[0].strip(), f[1].strip()), "lat": float(f[0]),
                                    "lon": lon, "type": f[13].strip(), "country": "", "quality": "No obvious issues",
                                    "years": float(f[9] or 0), "source": f[12].strip(), "consts": [], "ticon": 3})
        g["consts"].append((f[2].strip(), float(f[3]), float(f[4])))
    return list(gauges.values())


def choose(t4, t3, noaa_pts, log):
    coastal = [g for g in t4 if g["type"] == "Coastal" and g["quality"] in QUALITY_OK]
    log["t4_gauges"], log["t4_coastal_ok"] = len(t4), len(coastal)
    coastal.sort(key=lambda g: (g["quality"] != "No obvious issues", -g["years"]))
    kept = []
    grid = Grid([])
    for g in coastal:
        if grid.near(g["lat"], g["lon"], DEDUP_KM):
            continue
        kept.append(g)
        grid.cells.setdefault((math.floor(g["lat"]), math.floor(g["lon"])), []).append((g["lat"], g["lon"]))
    log["t4_after_dedup"] = len(kept)
    # a TICON-3 gauge is added only where TICON-4 has no gauge at all (any type or quality): where it has one, its
    # judgement (river, lake, quality flag) stands
    all4 = Grid([(g["lat"], g["lon"]) for g in t4])
    extra = 0
    for g in sorted([g for g in t3 if g["type"] == "Coastal"], key=lambda g: -g["years"]):
        if all4.near(g["lat"], g["lon"], DEDUP_KM) or grid.near(g["lat"], g["lon"], DEDUP_KM):
            continue
        kept.append(g)
        grid.cells.setdefault((math.floor(g["lat"]), math.floor(g["lon"])), []).append((g["lat"], g["lon"]))
        extra += 1
    log["t3_added"] = extra
    ng = Grid(noaa_pts)
    out = [g for g in kept if not ng.near(g["lat"], g["lon"], NOAA_KM)]
    log["near_noaa_dropped"] = len(kept) - len(out)
    return out


def lat_offsets(stations, tab):
    """The LAT (metres below the mean level, positive) of every station: min over LAT_YEARS of hourly heights, by year
    chunks: E = f * exp(i (G + u)) for every constituent once, then E @ (A exp(-i g)) for all stations."""
    preps = [H.prepare(s["consts_m"], tab, convention="ticon") for s in stations]
    cmat = np.zeros((len(tab.names), len(stations)), dtype=complex)
    for j, (idx, amp, ph, _) in enumerate(preps):
        cmat[idx, j] = amp * np.exp(-1j * ph)
    lows = np.full(len(stations), np.inf)
    n = int(LAT_YEARS * 365.2422 * 24)
    for a in range(0, n, 8766):
        ep = LAT_START + 3600.0 * np.arange(a, min(n, a + 8766))
        mjd = ep / 86400.0 + 40587.0
        s, h, p, nn, ps = H.mean_longitudes(mjd)
        tau = 15.0 * 24.0 * np.mod(mjd, 1.0) - s + h
        fargs = np.stack([tau, s, h, p, nn, ps, np.full_like(mjd, 90.0)], axis=1)
        g = np.radians(fargs @ tab.coef)
        f = np.stack([np.interp(mjd, tab.mjd, tab.f[:, c]) for c in range(len(tab.names))], axis=1)
        u = np.stack([np.interp(mjd, tab.mjd, tab.u[:, c]) for c in range(len(tab.names))], axis=1)
        e = f * np.exp(1j * (g + u))
        for b in range(0, len(stations), 400):
            hts = (e @ cmat[:, b:b + 400]).real
            lows[b:b + 400] = np.minimum(lows[b:b + 400], hts.min(axis=0))
    return -lows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticon4", required=True)
    ap.add_argument("--ticon3", required=True)
    ap.add_argument("--geonames")
    args = ap.parse_args()
    import app as A                                                # the live buoys' nearest-civil-zone rule
    tab = H.table()
    log = {}
    noaa = json.load(open(NOAA, encoding="utf-8"))
    fi = noaa["fields"]
    noaa_pts = [(r[fi.index("lat")], r[fi.index("lon")]) for r in noaa["stations"]]
    stations = choose(read_ticon4(args.ticon4), read_ticon3(args.ticon3), noaa_pts, log)
    places = load_geonames(args.geonames)
    dropped = {}
    for s in stations:
        s["consts_m"] = []
        for n, a, g in s["consts"]:
            if a < MIN_AMP_CM:
                continue
            if H.prepare([(n, 1.0, 0.0)], tab, convention="ticon")[3]:
                dropped[n] = dropped.get(n, 0) + 1
                continue
            s["consts_m"].append((n, a / 100.0, g % 360.0))
        token = s["key"].split("-")[0] if s["ticon"] == 4 else ""
        name = code_name(token) if token else None
        if not name:
            near = places.near(s["lat"], s["lon"], NAME_KM)
            name = ("%s (tide gauge)" % near[0][1][2]) if near else "Tide gauge %.2f, %.2f" % (s["lat"], s["lon"])
        s["name"] = name
        s["tz"] = A._nearest_civil_tz(s["lat"], s["lon"])
    stations = [s for s in stations if s["consts_m"]]
    lat0 = lat_offsets(stations, tab)
    names = sorted({H._norm(n) for s in stations for n, _, _ in s["consts_m"]})
    ni = {n: i for i, n in enumerate(names)}
    rows = []
    for s, z in sorted(zip(stations, lat0), key=lambda x: (x[0]["country"], x[0]["name"])):
        slug = re.sub(r"[^a-z0-9]+", "-", s["key"].lower()).strip("-")[:60]
        rows.append(["ticon:" + slug, s["name"], round(s["lat"], 5), round(s["lon"], 5), s["country"], s["tz"],
                     round(float(z), 3), round(s["years"], 1), s["source"], s["ticon"],
                     [[ni[H._norm(n)], int(round(a * 10000)), int(round(g * 10))] for n, a, g in s["consts_m"]]])
    doc = {
        "source": ("TICON-4 (Hart-Davis, Dettmering & Seitz, DGFI-TUM, SEANOE 2025) and TICON-3 (PANGAEA 2022), "
                   "CC BY 4.0, from GESLA tide-gauge records; names partly GeoNames (CC BY 4.0)"),
        "captured": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "convention": "ticon",
        "constituents": names,
        "fields": ["id", "name", "lat", "lon", "cc", "tz", "lat0", "years", "src", "ticon", "c"],
        "units": {"lat0": "m", "c": "[constituent index, amplitude 0.1 mm, Greenwich phase 0.1 deg]"},
        "stations": rows,
    }
    with open(OUT, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, separators=(",", ":"), ensure_ascii=False)
    log["stations"] = len(rows)
    log["constituents_dropped"] = dropped
    print(json.dumps(log, indent=1))
    print("wrote", OUT, os.path.getsize(OUT), "bytes")


if __name__ == "__main__":
    main()
