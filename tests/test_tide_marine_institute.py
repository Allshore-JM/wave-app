"""Plan section 112, step 3: the Marine Institute reader (tide_providers/marine_institute.py) on captured ERDDAP
answers (tests/capture_tide_linz_mi_qld_fixtures.py): the station list, the datum offset, highs and lows, the curve,
failures."""
import json
import os
import re
import sys
from datetime import datetime, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from tide_providers import TideFetchError, marine_institute as M  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "tides_national", "mi")
BEGIN = 1798588800                     # 2026-12-30 00:00 UTC
END = BEGIN + 3 * 86400                # 2027-01-02 00:00 UTC
ID_RE = re.compile(r"[a-z]+:[a-z0-9][a-z0-9._-]{0,79}")
LAHINCH = {"id": "mi:lahinch", "name": "Lahinch", "lat": 52.911, "lon": -9.3899,
           "local": {"station": "Lahinch", "curve": "Lahinch_MODELLED", "offset": 2.68}}


def _ep(s):
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())


class FakeGet:
    """get(url, headers=None, data=None) serving the captured answers by URL; anything else is ERDDAP's 404 (raises)."""

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


def _fix(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return f.read()


def _url_of(name):
    return next(u for u, f in FakeGet().index.items() if f == name)


# ---------------------------------------------------------------------------------------------- the station list

def test_station_list():
    get = FakeGet()
    got = M.build_station_list(get)
    assert len(got) == 38 and len(get.calls) == 4
    assert all(h == M.HEADERS for _, h in get.calls)
    assert M.HEADERS["User-Agent"] == "allshoresurf.com tide stations (https://allshoresurf.com)"
    by = {s["id"]: s for s in got}
    assert by["mi:lahinch"] == {"id": "mi:lahinch", "name": "Lahinch", "lat": 52.911, "lon": -9.3899,
                                "local": {"station": "Lahinch", "curve": "Lahinch_MODELLED", "offset": 2.68,
                                          "datum": "agency LAT", "lat_odm": 2.68, "lowest_low_odm": -2.597,
                                          "low_year": 2027}}
    assert by["mi:galway"]["local"]["curve"] == "Galway" and by["mi:galway"]["local"]["offset"] == 2.949
    assert by["mi:arklow"]["local"]["offset"] == 0.891               # a small-range east-coast station
    assert by["mi:tom_clarke_bridge"]["name"] == "Tom Clarke Bridge"
    assert sum(1 for s in got if s["local"]["curve"].endswith("_MODELLED")) == 12
    for s in got:
        assert ID_RE.fullmatch(s["id"]) and s["id"] == "mi:" + s["local"]["station"].lower()
        assert 51 < s["lat"] < 56 and -11 < s["lon"] < -5
        # the MI's LAT lies within 20 cm of the lowest low of 2027 (below it but at three modelled stations)
        assert -0.03 <= s["local"]["lat_odm"] + s["local"]["lowest_low_odm"] <= 0.2
        json.dumps(s)
    assert [s["id"] for s in got] == sorted(s["id"] for s in got)


def test_lowest_low_datum_mode(monkeypatch):
    monkeypatch.setattr(M, "DATUM_MODE", "lowest")
    by = {s["id"]: s for s in M.build_station_list(FakeGet())}
    assert by["mi:lahinch"]["local"]["offset"] == 2.597 and by["mi:lahinch"]["local"]["datum"] == "lowest low 2027"


def test_station_without_a_curve_falls_back_to_the_lowest_low():
    url = _url_of("curve_stations.csv")
    body = b"\n".join(l for l in _fix("curve_stations.csv").splitlines() if not l.startswith(b"Lahinch"))
    by = {s["id"]: s for s in M.build_station_list(FakeGet({url: body}))}
    lah = by["mi:lahinch"]["local"]
    assert lah["curve"] is None and lah["offset"] == 2.597 and lah["datum"] == "lowest low 2027"
    # a curve station under another name at the same position is still paired
    body = _fix("curve_stations.csv").replace(b"Galway,", b"Galway_Docks,")
    by = {s["id"]: s for s in M.build_station_list(FakeGet({url: body}))}
    assert by["mi:galway"]["local"]["curve"] == "Galway_Docks"


def test_station_list_failures_raise():
    with pytest.raises(TideFetchError):
        M.build_station_list(FakeGet({_url_of("hl_stations.csv"): RuntimeError("HTTP 500")}))
    with pytest.raises(TideFetchError):
        M.build_station_list(FakeGet({_url_of("hl_stations.csv"): b"stationID,lon\n,degrees_east\nX,1\n"}))


def test_queries_are_percent_encoded():
    for u in FakeGet().index:
        q = u.split("?", 1)[1]
        assert not re.search(r'[<>"() ]', q), u                   # ERDDAP answers 400 to a raw one
    u = M.query(M.HILO_DS, ["time"], 'stationID="Lahinch"', "time>=2026-11-27T00:00:00Z")
    assert u == (M.BASE + "IMI_TidePrediction_HighLow.csv?time&stationID=%22Lahinch%22"
                 "&time%3E=2026-11-27T00%3A00%3A00Z")
    assert M.page_url(LAHINCH).startswith(M.BASE + "IMI_TidePrediction_HighLow.htmlTable?")
    assert "%22Lahinch%22" in M.page_url(LAHINCH) and M.page_url({"local": {}}) is None


# ---------------------------------------------------------------------------------------------- fetch

def test_fetch_adds_the_datum_offset():
    get = FakeGet()
    got = M.fetch(LAHINCH, BEGIN, END, get)
    assert len(get.calls) == M.MAX_REQUESTS == 2
    hilo, curve = got["hilo"], got["curve"]
    # '2026-12-30T03:10:00Z,LOW,-1.484' (OD Malin) + 2.68 = 1.196 m above the MI's LAT
    assert hilo[0] == (_ep("2026-12-30 03:10"), 1.196, "L")
    assert hilo[1] == (_ep("2026-12-30 09:25"), 4.275, "H")       # 1.595 + 2.68
    assert len(hilo) == 11 and all(a[2] != b[2] for a, b in zip(hilo, hilo[1:]))
    assert all(BEGIN <= t <= END for t, _, _ in hilo)
    assert min(v for _, v, _ in hilo) > 0                         # above LAT
    # the curve: every half hour, both ends included; 0.13 OD Malin + 2.68 at the first
    assert curve[0] == (BEGIN, 2.81) and curve[-1][0] == END
    assert all(b[0] - a[0] == 1800 for a, b in zip(curve, curve[1:]))
    # the curve passes through the highs and lows (within its own centimetre steps and the half-hour sampling)
    cur = dict(curve)
    for t, h, k in hilo:
        near = [cur[x] for x in (t // 1800 * 1800, t // 1800 * 1800 + 1800) if x in cur]
        assert min(abs(v - h) for v in near) < 0.25


def test_fetch_without_a_curve_station_is_one_request():
    st = dict(LAHINCH, local=dict(LAHINCH["local"], curve=None))
    get = FakeGet()
    got = M.fetch(st, BEGIN, END, get)
    assert len(get.calls) == 1 and got["curve"] == [] and len(got["hilo"]) == 11


def test_missing_values_are_skipped():
    body = (b"time,tide_time_category,Water_Level_ODMalin\nUTC,,metres\n"
            b"2026-12-30T03:10:00Z,LOW,-1.484\n2026-12-30T09:25:00Z,HIGH,NaN\n2026-12-30T15:50:00Z,LOW,\n"
            b"2026-12-30T22:00:00Z,HIGH,1.6\n2026-12-31T04:00:00Z,SLACK,0.1\nbad,LOW,-1.0\n")
    assert M.parse_hilo(body, 1.0) == [(_ep("2026-12-30 03:10"), -0.484, "L"), (_ep("2026-12-30 22:00"), 2.6, "H")]
    body = (b"time,Water_Level_ODM\nUTC,metres\n2026-12-30T00:00:00Z,0.13\n2026-12-30T00:30:00Z,NaN\n"
            b"2026-12-30T00:35:00Z,0.2\n2026-12-30T01:00:00Z,-0.63\n")
    assert M.parse_curve(body, 2.68) == [(_ep("2026-12-30 00:00"), 2.81), (_ep("2026-12-30 01:00"), 2.05)]


def test_fetch_failures_raise():
    hl, cu = _url_of("lahinch_hilo.csv"), _url_of("lahinch_curve.csv")
    for extra in ({hl: RuntimeError("HTTP 404")}, {cu: RuntimeError("HTTP 500")},
                  {hl: b"<html><body>Error</body></html>"},
                  {hl: b"time,Water_Level_ODMalin\nUTC,metres\n2026-12-30T03:10:00Z,-1.4\n"},   # no category
                  {hl: b"time,tide_time_category,Water_Level_ODMalin\nUTC,,metres\n"},           # no rows
                  {cu: b"time,Water_Level_ODM\nUTC,metres\n2026-12-30T00:00:00Z,0.13\n"}):       # a short curve
        with pytest.raises(TideFetchError):
            M.fetch(LAHINCH, BEGIN, END, FakeGet(extra))
    with pytest.raises(TideFetchError):
        M.fetch(dict(LAHINCH, local={"station": "Lahinch"}), BEGIN, END, FakeGet())          # no offset
    with pytest.raises(TideFetchError):
        M.fetch(dict(LAHINCH, local={"curve": "X", "offset": 1.0}), BEGIN, END, FakeGet())   # no station
    with pytest.raises(TideFetchError):
        M.fetch(LAHINCH, END, BEGIN, FakeGet())
