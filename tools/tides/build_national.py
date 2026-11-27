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


# CWA's place types (plan section 113 B): the id's first letter, the name's Chinese prefix, the English word. Type O
# is CWA's surf list: the site names a place by its proper (formal) name, never as a surf spot (owner, 2026-11-27), so
# those read as "coast" with the place's own geographic name (CWA's name without the 衝浪 "surf" prefix).
CWA_TYPES = {"A": ("海水浴場", "beach"), "B": ("海釣", "fishing spot"), "I": ("漁港", "harbour"), "N": ("潛點", "dive spot"),
             "O": ("衝浪", "coast")}


# Taiwan's counties and cities by their official English names (a township's CWA name starts with one)
TW_COUNTIES = {"基隆市": "Keelung City", "臺北市": "Taipei City", "新北市": "New Taipei City", "桃園市": "Taoyuan City",
               "新竹縣": "Hsinchu County", "新竹市": "Hsinchu City", "苗栗縣": "Miaoli County", "臺中市": "Taichung City",
               "彰化縣": "Changhua County", "雲林縣": "Yunlin County", "嘉義縣": "Chiayi County", "嘉義市": "Chiayi City",
               "臺南市": "Tainan City", "高雄市": "Kaohsiung City", "屏東縣": "Pingtung County", "臺東縣": "Taitung County",
               "花蓮縣": "Hualien County", "宜蘭縣": "Yilan County", "澎湖縣": "Penghu County", "金門縣": "Kinmen County",
               "連江縣": "Lienchiang County"}


def cwa_names(stations, places=None):
    """CWA names its places in Chinese only. A township: its county's English name, the Chinese after it ("Lienchiang
    County (連江縣南竿鄉)"). Any other place: its type, its own name and the county of the nearest CWA township, e.g.
    "Harbour (永安), Taoyuan City" (G79-4: a GeoNames town nearby named the wrong place too often). The type's Chinese
    prefix (海水浴場, 漁港, 衝浪 ...) is dropped: no name calls a place a surf spot (owner, 2026-11-27); a name with a
    surf word left in it stops the build (G79-5)."""
    towns = []
    for t in stations:
        if str((t.get("local") or {}).get("code") or "")[:1].isdigit():
            county = TW_COUNTIES.get(t["name"][:3].replace("台", "臺"))
            if county:
                towns.append((t["lat"], t["lon"], county))
    for s in stations:
        code = str((s.get("local") or {}).get("code") or "")
        zh = s["name"]
        if code[:1].isdigit():
            county = TW_COUNTIES.get(zh[:3].replace("台", "臺"))
            s["name"] = "%s (%s)" % (county, zh) if county else zh
        else:
            word = "Place"
            if code[:1] in CWA_TYPES:
                pre, word = CWA_TYPES[code[:1]]
                if zh.startswith(pre) and len(zh) > len(pre):
                    zh = zh[len(pre):]
            near = min(towns, key=lambda t: (t[0] - s["lat"]) ** 2 + ((t[1] - s["lon"]) * 0.92) ** 2) if towns else None
            s["name"] = "%s (%s)%s" % (word[:1].upper() + word[1:], zh, ", " + near[2] if near else "")
        if "衝浪" in s["name"] or "surf" in s["name"].lower():
            raise SystemExit("cwa: a surf word in a name: %r (owner, 2026-11-27: places by their proper names)" % s["name"])


def readable_names(prefix, stations):
    """JMA names its tide-table stations in Japanese only ("東京 (TK)"): give each a romanised name from the nearest TICON
    gauge within 3 km (TICON's names are romanised), else the nearest GeoNames town within 15 km, keeping the Japanese
    name in brackets ("Tokyo (東京)"). CWA's Chinese names: cwa_names."""
    if prefix == "cwa":
        cwa_names(stations)
        return
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
    ap.add_argument("--only", help="comma-separated prefixes (chs, kv, jma, hko, linz, mi, qld, cwa)")
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
            rows.append([s["id"], s["name"], round(lat, 5), round(lon, 5), s.get("tz") or A._nearest_civil_tz(lat, lon),
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
