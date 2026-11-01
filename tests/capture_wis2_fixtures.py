"""Capture plan section 73's fixtures (WMO WIS2 nodes, Hong Kong Observatory, SMN Argentina) into tests/fixtures/wind/more.

Real answers, cut to a few stations so the files stay small; run once by hand (network), never by the tests:
    python -I tests/capture_wis2_fixtures.py
"""
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import wind_more as M                                   # noqa: E402

OUT = os.path.join(ROOT, "tests", "fixtures", "wind", "more")
UA = {"User-Agent": "allshoresurf.com wind stations (https://allshoresurf.com)"}
KEEP = {"saws": ["0-20000-0-68916", "0-20000-0-68920", "0-20000-0-68163"],   # Cape Point, Cape Agulhas, an inland one
        "imd": ["0-20000-0-43369", "0-20000-0-43057", "0-20000-0-42077"]}    # Minicoy, Colaba, Una (calm reports)


def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=90) as r:
        b = r.read()
    time.sleep(1.2)
    return b


def cut(doc, keep):
    doc = json.loads(doc)
    doc["features"] = [f for f in doc.get("features") or []
                       if (f.get("properties") or {}).get("wigos_station_identifier") in keep]
    doc["numberMatched"] = doc["numberReturned"] = len(doc["features"])
    doc["links"] = []
    return doc


def save(name, data):
    path = os.path.join(OUT, name)
    with open(path, "wb") as f:
        f.write(data if isinstance(data, bytes) else json.dumps(data, ensure_ascii=False, indent=0).encode("utf-8"))
    print("wrote", path, os.path.getsize(path))


def main():
    since3 = (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    since24 = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for node in ("saws", "imd"):
        base, coll, _n, _s, gust = M.WIS2_NODES[node]
        for name in ["wind_speed", "wind_direction"] + (["maximum_wind_gust_speed"] if gust else []):
            save(f"wis2_{node}_{name}.json", cut(get(M.WIS2_ITEMS.format(base=base, coll=coll, name=name, since=since3)),
                                                 KEEP[node]))
        st = json.loads(get(M.WIS2_STATIONS.format(base=base)))
        st["features"] = [f for f in st["features"] if (f.get("properties") or {}).get("wigos_station_identifier") in KEEP[node]]
        save(f"wis2_{node}_stations.json", st)
    base, coll = M.WIS2_NODES["saws"][:2]
    hist = cut(get(M.WIS2_STATION.format(base=base, coll=coll, wid="0-20000-0-68916", since=since24)), ["0-20000-0-68916"])
    hist["features"] = [f for f in hist["features"] if f["properties"].get("name") in set(M.WIS2_VARS) | {"present_weather"}]
    hist["numberMatched"] = hist["numberReturned"] = len(hist["features"])   # the wind and weather variables, and one other
    save("wis2_saws_history_68916.json", hist)
    save("hko_latest_10min_wind.csv", get(M.HKO_CSV))
    save("smn_tiepre.zip", get(M.SMN_NOW))
    save("smn_estaciones.zip", get(M.SMN_STATIONS))


if __name__ == "__main__":
    main()
