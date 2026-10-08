"""Build tide_stations.json: NOAA CO-OPS tide-prediction stations for the map's tide layer (plan section 38).

Per station: id, name, lat, lon, type (R harmonic / S subordinate), ref (a subordinate's reference station), tz (the
nearest civil zone, the live buoys' rule), obs (the station also reports its water level), oh / ol (a subordinate's
published time offsets in minutes for high and low tide: they pair its extremes with its reference's).

Sources (public domain): mdapi stations.json?type=tidepredictions, ?type=waterlevels, and one tidepredoffsets.json per
subordinate station (~2,250 small requests, four at a time; --offsets-cache keeps them between runs).

Run:  python tools/tides/fetch_stations.py            (writes ../../tide_stations.json)
NOAA's list changes rarely; the site never asks mdapi at runtime, so re-run it now and then and commit the file."""
import argparse
import json
import os
import string
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
OUT = os.path.join(ROOT, "tide_stations.json")
MD = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi"
FIELDS = ["id", "name", "lat", "lon", "type", "ref", "tz", "obs", "oh", "ol"]
KEEP_UPPER = {"ICWW", "USCG", "NOAA", "US", "RR", "AFB", "NAS", "II", "III", "IV", "SW", "SE", "NW", "NE"}


def get(url, tries=3):
    for k in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                return json.load(r)
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(1 + 2 * k)


def clean_name(name):
    """NOAA writes some names in capitals ("HONOLULU", "PAGO PAGO Harbor", "NEW YORK (The Battery)"): every word of
    three or more capital letters (or "ST.") is capitalised, abbreviations kept (ICWW, USCG ...; "B.C."); other words
    as written."""
    name = " ".join(str(name or "").split())
    words = []
    for w in name.split(" "):
        core = w.strip(string.punctuation)
        if (len(core) >= 3 or w.endswith(".")) and core.isalpha() and core.isupper() and core not in KEEP_UPPER:
            w = w.replace(core, core.capitalize())
        words.append(w)
    return " ".join(words)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--offsets-cache", default=None, help="JSON file keeping the per-station offsets between runs")
    args = ap.parse_args(argv)

    os.environ.setdefault("LIVE_BACKGROUND", "0")
    sys.path.insert(0, ROOT)
    import app as A                                            # the live buoys' nearest-civil-zone rule

    preds = get(MD + "/stations.json?type=tidepredictions")["stations"]
    gauges = {s["id"] for s in get(MD + "/stations.json?type=waterlevels")["stations"]}
    print(f"{len(preds)} prediction stations, {len(gauges)} water-level stations", flush=True)

    cache = {}
    if args.offsets_cache and os.path.exists(args.offsets_cache):
        cache = json.load(open(args.offsets_cache))
    todo = [s["id"] for s in preds if s.get("type") == "S" and s["id"] not in cache]

    def offsets(sid):
        d = get(f"{MD}/stations/{sid}/tidepredoffsets.json")
        time.sleep(0.1)
        return sid, [d.get("timeOffsetHighTide"), d.get("timeOffsetLowTide")]

    with ThreadPoolExecutor(4) as pool:
        for n, (sid, off) in enumerate(pool.map(offsets, todo), 1):
            cache[sid] = off
            if n % 200 == 0:
                print(f"  offsets {n}/{len(todo)}", flush=True)
                if args.offsets_cache:
                    json.dump(cache, open(args.offsets_cache, "w"))
    if args.offsets_cache:
        json.dump(cache, open(args.offsets_cache, "w"))

    ids = {s["id"] for s in preds}
    rows, dropped = [], []
    for s in sorted(preds, key=lambda s: s["id"]):
        if s.get("type") == "S" and (s.get("reference_id") in (None, "", s["id"]) or s.get("reference_id") not in ids):
            dropped.append(s["id"])                            # NOAA answers nothing for these (Malakal Harbor: its own ref)
            continue
        lat, lon = round(float(s["lat"]), 5), round(float(s["lng"]), 5)
        kind = "S" if s.get("type") == "S" else "R"
        oh, ol = cache.get(s["id"], [None, None]) if kind == "S" else (None, None)
        rows.append([s["id"], clean_name(s["name"]), lat, lon, kind, (s.get("reference_id") or None) if kind == "S" else None,
                     A._nearest_civil_tz(lat, lon), s["id"] in gauges, oh, ol])
    doc = {"source": "NOAA CO-OPS metadata API (tidesandcurrents.noaa.gov), public domain",
           "captured": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "fields": FIELDS, "stations": rows}
    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        f.write('{"source":%s,"captured":%s,"fields":%s,"stations":[\n' % (
            json.dumps(doc["source"]), json.dumps(doc["captured"]), json.dumps(FIELDS)))
        f.write(",\n".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in rows))
        f.write("\n]}\n")
    r = sum(1 for x in rows if x[4] == "R")
    print(f"wrote {args.out}: {len(rows)} stations ({r} R, {len(rows) - r} S), "
          f"{sum(1 for x in rows if x[7])} with observations; left out (no valid reference): {dropped}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
