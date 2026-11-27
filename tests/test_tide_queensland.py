"""Plan section 112, step 3: the Queensland (MSQ) reader (tide_providers/queensland.py) on captured CKAN answers
(tests/capture_tide_linz_mi_qld_fixtures.py): the station list, the DataStore rows, AEST -> UTC, the year boundary,
derived highs and lows, failures."""
import json
import math
import os
import re
import sys
from datetime import datetime, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from tide_providers import TideFetchError, queensland as Q  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "tides_national", "qld")
BEGIN = 1798588800                     # 2026-12-30 00:00 UTC (10:00 AEST)
END = BEGIN + 3 * 86400                # 2027-01-02 00:00 UTC
ID_RE = re.compile(r"[a-z]+:[a-z0-9][a-z0-9._-]{0,79}")
C26, C27 = "54791c1c-3710-4ac1-8703-a474151dff39", "3c364f05-c44b-402d-a14c-61cf409b9a18"
H26, H27 = "47e590ab-8624-4a4c-a32a-a598ff5e1419", "4fb76039-7cbd-47fc-a304-4237eb1d9f60"
ABBOT = {"id": "qld:abbot-point", "name": "Abbot Point", "lat": -19.85, "lon": 148.11667,
         "local": {"dataset": "abbot-point-tide-gauge-predicted-interval-data", "curve": {"2026": C26, "2027": C27},
                   "hilo": {"2026": H26, "2027": H27}}}
CURVE_F = ["Date", "Time", "Reading"]
HILO_F = ["Date", "Time", "Ind", "Reading"]
DAYS = {"2026": ["30/12/2026", "31/12/2026"], "2027": ["01/01/2027", "02/01/2027"]}


def _ep(s):
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())


class FakeGet:
    """get(url, headers=None, data=None) serving the captured answers by URL; anything else raises (the portal's
    download links answer a WAF challenge, never data)."""

    def __init__(self, extra=None):
        with open(os.path.join(FIX, "index.json"), encoding="utf-8") as f:
            self.index = json.load(f)
        self.extra = extra or {}
        self.calls = []

    def __call__(self, url, headers=None, data=None):
        self.calls.append((url, headers))
        if url in self.extra:
            v = self.extra[url]
            if isinstance(v, Exception):
                raise v
            return v
        if url not in self.index:
            raise RuntimeError("HTTP 404 " + url)
        with open(os.path.join(FIX, self.index[url]), "rb") as f:
            return f.read()


def _answer(records, success=True):
    return json.dumps({"success": success, "result": {"records": records}}).encode()


# ---------------------------------------------------------------------------------------------- the station list

def test_station_list():
    get = FakeGet()
    got = Q.build_station_list(get, min_year=2026)
    assert [u for u, _ in get.calls] == [Q.search_url(Q.INTERVAL_SUFFIX), Q.search_url(Q.HILO_SUFFIX)]
    assert all(h["User-Agent"] == "allshoresurf.com tide stations (https://allshoresurf.com)" for _, h in get.calls)
    # Military Jetty's predictions end with 2025; Half Tide Tug Harbour has no published position: left out
    assert [s["id"] for s in got] == ["qld:abbot-point", "qld:karumba"]
    a, k = got
    assert a["name"] == "Abbot Point" and (a["lat"], a["lon"]) == (-19.85, 148.11667)       # 19°51'S 148°07'E
    assert a["local"]["curve"] == {"2025": "b1144491-80a8-49b1-a347-376eb5490e11", "2026": C26, "2027": C27}
    assert a["local"]["hilo"] == {"2026": H26, "2027": H27}
    assert (k["lat"], k["lon"]) == (-17.48333, 140.81667)                                    # 17°29'S 140°49'E
    assert "2024" not in k["local"]["curve"]                     # older years (and non-DataStore files) not kept
    for s in got:
        assert ID_RE.fullmatch(s["id"])
        json.dumps(s)
    assert Q.page_url(a) == "https://www.data.qld.gov.au/dataset/abbot-point-tide-gauge-predicted-interval-data"
    assert Q.build_station_list(FakeGet(), min_year=2028) == []  # no 2028 predictions published yet


def test_place_names():
    assert Q.place_name("Coconut Island (Poruma) tide gauge—predicted interval data") == "Coconut Island (Poruma)"
    assert Q.place_name("Abbot Point tide gauge predicted interval data") == "Abbot Point"
    assert Q.place_name("Golding Channel—predicted interval data") == "Golding Channel"
    assert Q.place_name("Fisherman's Landing tide gauge - predicted interval data") == "Fisherman's Landing"


def test_every_position_lies_on_the_queensland_coast():
    for slug, dms in Q.POSITIONS.items():
        lat, lon = Q.position(dms)
        assert -29 < lat < -9 and 138 < lon < 154, slug
        assert dms[1] < 60 and dms[3] < 60


def test_station_list_failures_raise():
    with pytest.raises(TideFetchError):
        Q.build_station_list(FakeGet({Q.search_url(Q.INTERVAL_SUFFIX): RuntimeError("HTTP 503")}))
    with pytest.raises(TideFetchError):
        Q.build_station_list(FakeGet({Q.search_url(Q.INTERVAL_SUFFIX): b'{"success": false, "error": {}}'}))
    with pytest.raises(TideFetchError):
        Q.build_station_list(FakeGet({Q.search_url(Q.INTERVAL_SUFFIX): b""}))


# ---------------------------------------------------------------------------------------------- fetch

def test_fetch_across_the_new_year():
    get = FakeGet()
    got = Q.fetch(ABBOT, BEGIN, END, get)
    assert [u for u, _ in get.calls] == [Q.records_url(C26, DAYS["2026"], CURVE_F),
                                         Q.records_url(H26, DAYS["2026"], HILO_F),
                                         Q.records_url(C27, DAYS["2027"], CURVE_F),
                                         Q.records_url(H27, DAYS["2027"], HILO_F)]
    assert len(get.calls) == 4 <= Q.MAX_REQUESTS
    curve, hilo = got["curve"], got["hilo"]
    # the 10-minute curve, AEST (UTC+10) -> UTC, both ends of the window included, no gap at the year boundary
    assert curve[0][0] == BEGIN and curve[-1][0] == END and len(curve) == 3 * 144 + 1
    assert all(b[0] - a[0] == 600 for a, b in zip(curve, curve[1:]))
    cur = dict(curve)
    assert cur[_ep("2026-12-31 13:50")] == 0.769 and cur[_ep("2026-12-31 14:00")] == 0.794   # '31/12/2026,23:50,0.769'
    #                                                       (the 2026 file), '01/01/2027,00:00,0.794' (the 2027 file)
    # MSQ's published highs and lows = the 2027 Queensland Tide Tables, Abbot Point, 1 January 2027:
    # 0627 2.59, 1215 1.37, 1754 2.34; 2 January 0023 0.70 (AEST)
    for t, h, k in (("2026-12-31 20:27", 2.59, "H"), ("2027-01-01 02:15", 1.37, "L"), ("2027-01-01 07:54", 2.34, "H"),
                    ("2027-01-01 14:23", 0.70, "L"), ("2026-12-30 19:18", 2.32, "H")):
        assert (_ep(t), h, k) in hilo
    assert all(BEGIN <= t <= END for t, _, _ in hilo) and all(a[2] != b[2] for a, b in zip(hilo, hilo[1:]))


def test_derived_highs_and_lows_match_the_published_ones():
    st = dict(ABBOT, local=dict(ABBOT["local"], hilo={"2026": H26}))         # no 2027 high/low file
    get = FakeGet()
    got = Q.fetch(st, BEGIN, END, get)
    assert len(get.calls) == 3
    pub = Q.fetch(ABBOT, BEGIN, END, FakeGet())["hilo"]
    der = [e for e in got["hilo"] if e[0] >= _ep("2026-12-31 14:00")]
    ref = [e for e in pub if e[0] >= _ep("2026-12-31 14:00")]
    assert len(der) == len(ref) >= 4
    for (t1, h1, k1), (t2, h2, k2) in zip(der, ref):
        assert k1 == k2 and abs(t1 - t2) <= 4 * 60 and abs(h1 - h2) <= 0.01


def test_extremes_refine_with_a_parabola_and_drop_wiggles():
    period, top = 12.42 * 3600, 1000 + 3 * 3600 + 7 * 60       # a high at 03:07, between the 10-minute samples
    curve = [(t, round(1.5 + math.cos(2 * math.pi * (t - top) / period), 3)) for t in range(0, 2 * 86400, 600)]
    ext = Q.extremes(curve)
    assert [k for *_, k in ext][:3] == ["H", "L", "H"]
    assert abs(ext[0][0] - top) <= 60 and abs(ext[0][1] - 2.5) <= 0.002
    # a 1-mm wiggle on a stand is not a high and a low
    flat = [(i * 600, v) for i, v in enumerate([0.1, 0.5, 1.0, 1.2, 1.201, 1.2, 1.201, 1.2, 1.0, 0.5, 0.1, 0.5])]
    assert [k for *_, k in Q.extremes(flat)] == ["H", "L"]
    # no extreme read across a gap in the series
    assert Q.extremes([(0, 1.0), (600, 2.0), (3000, 1.0)]) == []


def test_missing_values_are_skipped_and_gaps_refused():
    recs = ("30/12/2026,10:00,1.000\n30/12/2026,10:10,-99\n30/12/2026,10:20,\n30/12/2026,10:30,x\n"
            "31/13/2026,10:40,1.0\n30/12/2026,10:50,1.050\n")
    got = Q.parse_records({"records": recs}, CURVE_F)
    assert got == [[_ep("2026-12-30 00:00"), 1.0], [_ep("2026-12-30 00:50"), 1.05]]
    short = _answer("30/12/2026,10:00,1.000\n30/12/2026,10:10,1.010\n")
    with pytest.raises(TideFetchError):
        Q.fetch(ABBOT, BEGIN, END, FakeGet({Q.records_url(C26, DAYS["2026"], CURVE_F): short}))


def test_fetch_failures_raise():
    u = Q.records_url(C26, DAYS["2026"], CURVE_F)
    for bad in (RuntimeError("HTTP 202 WAF challenge"), b"", b"<html>challenge</html>", _answer("", success=False),
                json.dumps({"success": True, "result": {"records": [{"Date": "30/12/2026"}]}}).encode()):
        with pytest.raises(TideFetchError):
            Q.fetch(ABBOT, BEGIN, END, FakeGet({u: bad}))
    with pytest.raises(TideFetchError):                           # a year MSQ has not published for this gauge
        Q.fetch(dict(ABBOT, local=dict(ABBOT["local"], curve={"2026": C26})), BEGIN, END, FakeGet())
    with pytest.raises(TideFetchError):
        Q.fetch(ABBOT, END, BEGIN, FakeGet())
    with pytest.raises(TideFetchError):                           # too few published highs and lows
        Q.fetch(ABBOT, BEGIN, END, FakeGet({Q.records_url(H26, DAYS["2026"], HILO_F): _answer("")}))


def test_records_url_filters_on_the_dates():
    u = Q.records_url(C26, ["30/12/2026", "31/12/2026"], CURVE_F)
    assert u.startswith("https://www.data.qld.gov.au/api/3/action/datastore_search?resource_id=" + C26)
    assert "filters=%7B%22Date%22%3A%5B%2230%2F12%2F2026%22%2C%2231%2F12%2F2026%22%5D%7D" in u
    assert "records_format=csv" in u and "fields=Date%2CTime%2CReading" in u


def test_g76_a_year_missing_from_the_snapshot_is_looked_up_once_a_day(monkeypatch):
    """The snapshot knows 2026 only: 2027 comes from package_show (curve and highs/lows datasets), kept a day."""
    monkeypatch.setattr(Q, "_YEARS_CACHE", {})
    st = dict(ABBOT, local=dict(ABBOT["local"], curve={"2026": C26}, hilo={"2026": H26}))
    ds = st["local"]["dataset"]
    show = lambda name, cid: json.dumps({"success": True, "result": {"resources": [
        {"name": "Abbot Point 2027 predicted", "format": "CSV", "datastore_active": True, "id": cid}]}}).encode()
    extra = {Q.API + "package_show?id=" + ds: show(ds, C27),
             Q.API + "package_show?id=" + ds[:-len(Q.INTERVAL_SUFFIX)] + Q.HILO_SUFFIX: show(ds, H27)}
    get = FakeGet(extra)
    got = Q.fetch(st, BEGIN, END, get)
    assert got["curve"][-1][0] == END and len(get.calls) == 6
    get2 = FakeGet(extra)
    Q.fetch(st, BEGIN, END, get2)
    assert len(get2.calls) == 4                                      # the years found are kept a day
