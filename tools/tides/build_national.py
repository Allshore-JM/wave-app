"""Build tide_national.json: the agencies' official tide stations for the Tide layer (plan section 112, step 3).

    python -I tools/tides/build_national.py [--only chs,jma,...]

Runs every agency module's build_station_list(get) (tide_providers/, see its __init__ for the interface) against the
real services, adds each station's nearest civil time zone (the live buoys' rule, app._nearest_civil_tz) and writes
the committed snapshot the site reads (it never asks the agencies for their station lists at runtime). With --only, the
other agencies' rows are kept from the snapshot in hand.
"""
import argparse
import importlib
import json
import os
import re
import sys
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)
import tide_global  # noqa: E402

OUT = os.path.join(ROOT, "tide_national.json")
UA = "allshoresurf.com tide stations (https://allshoresurf.com)"
ID_RE = re.compile(r"[a-z]+:[a-z0-9][a-z0-9._-]{0,79}")


def get(url, headers=None, data=None):
    h = {"User-Agent": UA}
    h.update(headers or {})
    req = urllib.request.Request(url, headers=h, data=data)
    with urllib.request.urlopen(req, timeout=120) as r:
        if r.status != 200:
            raise RuntimeError("HTTP %s from %s" % (r.status, url))
        return r.read()


def readable_names(prefix, stations):
    """JMA names its tide-table stations in Japanese only ("東京 (TK)"): give each a romanised name from the nearest TICON
    gauge within 3 km (TICON's names are romanised), else the nearest GeoNames town within 15 km, keeping the Japanese
    name in brackets ("Tokyo (東京)")."""
    if prefix != "jma":
        return
    import build_global as BG                                       # tools/tides/build_global.py: Grid, GeoNames
    g = json.load(open(os.path.join(ROOT, "tide_global.json"), encoding="utf-8"))
    gi = g["fields"]
    ticon = BG.Grid([(r[gi.index("lat")], r[gi.index("lon")], r[gi.index("name")]) for r in g["stations"]
                     if r[gi.index("cc")] == "JPN" and "(tide gauge)" not in r[gi.index("name")]])
    places = BG.load_geonames(None)
    for s in stations:
        ja = re.sub(r"\s*\([A-Za-z0-9]+\)\s*$", "", s["name"]).strip()
        near = ticon.near(s["lat"], s["lon"], 3.0)
        roman = near[0][1][2] if near else None
        if not roman:
            near = places.near(s["lat"], s["lon"], 15.0)
            roman = near[0][1][2] if near else None
        s["name"] = "%s (%s)" % (roman, ja) if roman and ja else (roman or s["name"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated prefixes (chs, kv, jma, hko, linz, mi, qld)")
    args = ap.parse_args()
    only = set(args.only.split(",")) if args.only else set(tide_global.PROVIDERS)
    import app as A
    rows, counts = [], {}
    old = {}
    if os.path.exists(OUT):
        doc = json.load(open(OUT, encoding="utf-8"))
        for r in doc["stations"]:
            old.setdefault(r[0].split(":", 1)[0], []).append(r)
    for prefix, modname in tide_global.PROVIDERS.items():
        if prefix not in only:
            rows += old.get(prefix, [])
            counts[prefix] = len(old.get(prefix, []))
            continue
        mod = importlib.import_module("tide_providers." + modname)
        got = mod.build_station_list(get)
        readable_names(prefix, got)
        seen = set()
        for s in got:
            if not ID_RE.fullmatch(s["id"]) or not s["id"].startswith(prefix + ":") or s["id"] in seen:
                raise SystemExit("%s: bad or duplicate id %r" % (prefix, s["id"]))
            seen.add(s["id"])
            lat, lon = float(s["lat"]), float(s["lon"])
            rows.append([s["id"], s["name"], round(lat, 5), round(lon, 5), A._nearest_civil_tz(lat, lon),
                         s.get("local") or {}])
        counts[prefix] = len(got)
        print("%s: %d stations" % (prefix, len(got)))
    doc = {"source": "official tide predictions of national agencies (tide_providers/)",
           "captured": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
           "fields": ["id", "name", "lat", "lon", "tz", "local"], "counts": counts, "stations": rows}
    with open(OUT, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, separators=(",", ":"), ensure_ascii=False)
    print("wrote", OUT, len(rows), "stations", counts)


if __name__ == "__main__":
    main()
