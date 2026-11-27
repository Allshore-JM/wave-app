"""Capture LINZ, Marine Institute and Queensland (MSQ) answers for tests/test_tide_linz.py,
test_tide_marine_institute.py and test_tide_queensland.py (plan section 112, step 3). Run once
(python -I tests/capture_tide_linz_mi_qld_fixtures.py); the files are committed. Each agency's folder has index.json:
{url: file} for the tests' fake get.

The window is BEGIN 2026-12-30 00:00 UTC to END 2027-01-02 00:00 UTC (three days across the new year).
  linz/  tp.json: LINZ's place list cut to six places (Auckland, Akaroa, Castlepoint: "Daily Predictions"; Ahipara Bay,
         Anita Bay: "Offset"; Scott Base); Auckland 2026 + 2027 (the year boundary), Castlepoint 2026 (an extreme at
         02:28 on 5 April, the hour daylight saving repeats).
  mi/    the station lists of both datasets, the lowest 2027 lows (orderByMin), 3 hours of both heights at every
         station (the LAT offset), Lahinch's highs and lows and its half-hourly curve over the window.
  qld/   the CKAN package_search answers cut to four gauges (Abbot Point, Karumba, Military Jetty: predictions end 2025,
         Half Tide Tug Harbour: no published position) and to the fields read; Abbot Point's DataStore rows (curve +
         highs/lows, 2026 and 2027) over the window.
"""
import json
import os
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tide_providers import linz as L, marine_institute as M, queensland as Q  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "tides_national")
UA = {"User-Agent": "allshoresurf.com tide stations (https://allshoresurf.com)"}
BEGIN = 1798588800                     # 2026-12-30 00:00 UTC
END = BEGIN + 3 * 86400                # 2027-01-02 00:00 UTC


def get(url, headers=None, data=None):
    req = urllib.request.Request(url, headers=dict(UA, **(headers or {})), data=data)
    with urllib.request.urlopen(req, timeout=60) as r:
        if r.status != 200:
            raise RuntimeError("HTTP %d %s" % (r.status, url))
        return r.read()


class Saver:
    def __init__(self, agency):
        self.dir = os.path.join(FIX, agency)
        os.makedirs(self.dir, exist_ok=True)
        self.index = {}

    def put(self, name, url, body):
        with open(os.path.join(self.dir, name), "wb") as f:
            f.write(body)
        self.index[url] = name
        print(name, len(body))

    def grab(self, name, url):
        self.put(name, url, get(url))

    def close(self):
        with open(os.path.join(self.dir, "index.json"), "w", encoding="utf-8", newline="\n") as f:
            json.dump(self.index, f, indent=1, sort_keys=True, ensure_ascii=False)
            f.write("\n")


def linz():
    s = Saver("linz")
    keep = ("Auckland", "Akaroa", "Castlepoint", "Ahipara Bay", "Anita Bay", "Scott Base")
    doc = [p for p in json.loads(get(L.LIST_URL)) if p.get("name") in keep]
    s.put("tp.json", L.LIST_URL, json.dumps(doc, ensure_ascii=False, indent=0).encode("utf-8"))
    for file, year in (("Auckland", 2026), ("Auckland", 2027), ("Castlepoint", 2026)):
        s.grab("%s_%d.csv" % (file.lower(), year), L.csv_url(file, year))
    s.close()


def mi():
    s = Saver("mi")
    v = ["stationID", "longitude", "latitude"]
    s.grab("hl_stations.csv", M.query(M.HILO_DS, v, "distinct()"))
    s.grab("curve_stations.csv", M.query(M.CURVE_DS, v, "distinct()"))
    y = M.LOW_YEAR
    s.grab("lowest_%d.csv" % y, M.query(M.HILO_DS, ["stationID", "time", "Water_Level_ODMalin"],
                                        "time>=%d-01-01T00:00:00Z" % y, "time<%d-01-01T00:00:00Z" % (y + 1),
                                        'tide_time_category="LOW"', 'orderByMin("stationID,Water_Level_ODMalin")'))
    s.grab("lat_span.csv", M.query(M.CURVE_DS, ["stationID", "Water_Level", "Water_Level_ODM"],
                                   "time>=" + M.LAT_SPAN[0], "time<=" + M.LAT_SPAN[1]))
    span = ("time>=" + M.stamp(BEGIN), "time<=" + M.stamp(END))
    s.grab("lahinch_hilo.csv", M.query(M.HILO_DS, ["time", "tide_time_category", "Water_Level_ODMalin"],
                                       'stationID="Lahinch"', *span))
    s.grab("lahinch_curve.csv", M.query(M.CURVE_DS, ["time", "Water_Level_ODM"], 'stationID="Lahinch_MODELLED"',
                                        *span, 'orderByClosest("time/30minutes")'))
    s.close()


def qld():
    s = Saver("qld")
    keep = ("abbot-point", "karumba", "military-jetty-pumicestone-passage", "half-tide-tug-harbour")
    rkeys = ("id", "name", "format", "datastore_active")
    for suffix, name in ((Q.INTERVAL_SUFFIX, "search_interval.json"), (Q.HILO_SUFFIX, "search_hilo.json")):
        url = Q.search_url(suffix)
        doc = json.loads(get(url))
        res = doc["result"]
        res["results"] = [{"name": p["name"], "title": p["title"],
                           "resources": [{k: r.get(k) for k in rkeys} for r in p["resources"]]}
                          for p in res["results"] if p["name"][:-len(suffix)] in keep]
        res["count"] = len(res["results"])
        res.pop("facets", None)
        res.pop("search_facets", None)
        s.put(name, url, json.dumps(doc, ensure_ascii=False, indent=0).encode("utf-8"))
    st = next(x for x in Q.build_station_list(get, min_year=2026) if x["id"] == "qld:abbot-point")
    days = {"2026": ["30/12/2026", "31/12/2026"], "2027": ["01/01/2027", "02/01/2027"]}
    for year, dates in days.items():
        s.grab("abbot_curve_%s.json" % year, Q.records_url(st["local"]["curve"][year], dates,
                                                          ["Date", "Time", "Reading"]))
        s.grab("abbot_hilo_%s.json" % year, Q.records_url(st["local"]["hilo"][year], dates,
                                                         ["Date", "Time", "Ind", "Reading"]))
    s.close()


if __name__ == "__main__":
    linz()
    mi()
    qld()
