"""Wind stations (plan section 39), step 2: the feeds' parsers, the three providers, the merged table, the history
service, the routes, the scheduler hook and /healthz. No test reaches the network: every fetch is a fake over the
captured fixtures (tests/fixtures/wind, tests/capture_wind_fixtures.py)."""
import calendar
import gzip
import json
import os
import sys
import threading
import time as _time
import zlib

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import buoy_sources as B  # noqa: E402
import fake_buoy_providers as F  # noqa: E402
import wind_sources as W  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "wind")


def fx(name, mode="rb"):
    with open(os.path.join(FIX, name), mode, **({} if mode == "rb" else {"encoding": "utf-8"})) as f:
        return f.read()


def first_t(text):
    return W.parse_realtime2(text, 0, 1)[0]["t"]


STATIONS = {
    "coops:1612340": {"id": "coops:1612340", "name": "Honolulu", "lat": 21.30333, "lon": -157.86453, "kind": "gauge",
                      "src": "coops", "tz": "Pacific/Honolulu", "alias": "OOUH1", "wx": "awp"},
    "coops:1611400": {"id": "coops:1611400", "name": "Nawiliwili", "lat": 21.9544, "lon": -159.3561, "kind": "gauge",
                      "src": "coops", "tz": "Pacific/Honolulu", "alias": "NWWH1", "wx": "awph"},
    "coops:1612401": {"id": "coops:1612401", "name": "Pearl Harbor", "lat": 21.3675, "lon": -157.9639, "kind": "gauge",
                      "src": "coops", "tz": "Pacific/Honolulu", "alias": None, "wx": None},
    "ndbc:51003": {"id": "ndbc:51003", "name": "Western Hawaii", "lat": 19.151, "lon": -160.617, "kind": "buoy",
                   "src": "ndbc", "tz": "Pacific/Honolulu", "alias": None},
    "ndbc:HRRH1": {"id": "ndbc:HRRH1", "name": "Lono Circle", "lat": 21.43, "lon": -157.8, "kind": "station",
                   "src": "ndbc", "tz": "Pacific/Honolulu", "alias": None},
    "metar:PHNL": {"id": "metar:PHNL", "name": "Honolulu Intl", "lat": 21.315, "lon": -157.924, "kind": "airport",
                   "src": "metar", "tz": "Pacific/Honolulu", "alias": None},
    "metar:KSFO": {"id": "metar:KSFO", "name": "San Francisco Intl", "lat": 37.62, "lon": -122.37, "kind": "airport",
                   "src": "metar", "tz": "America/Los_Angeles", "alias": None},
    "metar:KGMU": {"id": "metar:KGMU", "name": "Greenville", "lat": 34.85, "lon": -82.35, "kind": "airport",
                   "src": "metar", "tz": "America/New_York", "alias": None},
    "nws:001HE": {"id": "nws:001HE", "name": "MECO Kealaloloa Ridge", "lat": 20.80393, "lon": -156.54018, "kind": "utility",
                  "src": "nws", "tz": "Pacific/Honolulu", "alias": None},
    "nws:029HI": {"id": "nws:029HI", "name": "Keahuolu", "lat": 19.6687, "lon": -155.9575, "kind": "mesonet",
                  "src": "nws", "tz": "Pacific/Honolulu", "alias": None},
}
NWS_IDS = {k.split(":")[1]: v for k, v in STATIONS.items() if k.startswith("nws:")}


class FakeFetch:
    """fetch(url, max_bytes, headers) over a table of url -> bytes (or an Exception to raise, or a callable)."""

    def __init__(self, table=None):
        self.table = dict(table or {})
        self.calls = []

    def __call__(self, url, max_bytes, headers=None):
        self.calls.append((url, max_bytes, dict(headers or {})))
        for key, val in self.table.items():
            if url.startswith(key) if key.endswith("?") or key.endswith("=") else url == key:
                if isinstance(val, Exception):
                    raise val
                return val(url, max_bytes, headers) if callable(val) else val
        raise IOError("no fixture for " + url)

    def count(self, part):
        return sum(1 for u, _, _ in self.calls if part in u)


def coops_table():
    """CO-OPS answers per gauge: Honolulu a reading, Nawiliwili NOAA's error, Pearl Harbor unreachable."""
    return {W.coops_url("1612340", date="latest"): fx("coops_latest.json"),
            W.coops_url("1611400", date="latest"): fx("coops_error.json"),
            W.coops_url("1612401", date="latest"): IOError("timeout")}


def nws_table():
    """NWS API answers per station (the url up to its query): 001HE readings, 029HI nothing in the window."""
    return {W.NWS_API + "/stations/001HE/observations?": fx("nws_obs.json"),
            W.NWS_API + "/stations/029HI/observations?": b'{"@context": {}, "@graph": []}'}


def make_fakes(now_epoch):
    """The four providers over STATIONS, their fetches answered by the fixtures (the NDBC file, the METAR cache as
    plain CSV, CO-OPS per gauge, the NWS API per station), on a frozen clock."""
    fetch = FakeFetch({W.NDBC_LATEST_URL: fx("latest_obs.txt"), W.METAR_CACHE_URL: fx("metars.csv")})
    fetch.table.update(coops_table())
    fetch.table.update(nws_table())
    provs = W.make_providers(STATIONS, fetch, coops_workers=2)
    return provs, fetch


@pytest.fixture
def clock(monkeypatch):
    newest = max(r["t"] for r in W.parse_latest_obs(fx("latest_obs.txt", "r")).values())
    t = F.FrozenTime(newest + 600)                                     # ten minutes after the newest NDBC row
    monkeypatch.setattr(B, "time", t)
    return t


# ------------------------------------------------------------------ readings and parsers

def test_reading_checks_its_fields():
    assert W.reading(1791651240, "1.0", "3.5", "74.0") == {"t": 1791651240, "s": 1.0, "g": 3.5, "d": 74}
    assert W.reading(1, 0.04, None, 120) == {"t": 1, "s": 0.0, "g": None, "d": None}, "a speed that rounds to 0 is calm (step 5c D)"
    assert W.reading(1, 0.06, None, 120) == {"t": 1, "s": 0.1, "g": None, "d": 120}
    assert W.reading(1791651240, 2.26, None, 359.6) == {"t": 1791651240, "s": 2.3, "g": None, "d": 0}
    assert W.reading("x", 1, 1, 1) is None and W.reading(1, "MM", 1, 1) is None and W.reading(1, -1, 1, 1) is None
    assert W.reading(1, 200, 1, 1) is None                                  # above MAX_SPEED_MS
    assert W.reading(1, 1, "MM", "MM") == {"t": 1, "s": 1.0, "g": None, "d": None}
    assert W.reading(1, 1, 1, 361)["d"] is None and W.reading(1, 1, 1, -1)["d"] is None
    assert W.reading(1, float("nan"), 1, 1) is None and W.reading(1, 1, 1, float("inf"))["d"] is None


LATEST = """#STN       LAT      LON  YYYY MM DD hh mm WDIR WSPD   GST WVHT
#text      deg      deg   yr mo day hr mn degT  m/s   m/s   m
51003    19.151 -160.617 2026 10 10 01 10 120   4.0   5.0   MM
51201    21.671 -158.118 2026 10 10 00 56  MM    MM    MM  1.8
OOUH1    21.303 -157.865 2026 10 10 00 54 190   3.6    MM   MM
BADT1    21.303 -157.865 2026 13 10 00 54 190   3.6   6.2   MM
short 1 2
"""


def test_parse_latest_obs_rows_with_a_speed():
    got = W.parse_latest_obs(LATEST)
    assert got == {"51003": {"t": calendar.timegm((2026, 10, 10, 1, 10, 0)), "s": 4.0, "g": 5.0, "d": 120},
                   "OOUH1": {"t": calendar.timegm((2026, 10, 10, 0, 54, 0)), "s": 3.6, "g": None, "d": 190}}


def test_parse_latest_obs_fixture():
    got = W.parse_latest_obs(fx("latest_obs.txt", "r"))
    assert {"51003", "OOUH1", "ILOH1"} <= set(got) and "51201" not in got    # 51201 reports waves, no wind
    for r in got.values():
        assert isinstance(r["t"], int) and r["s"] >= 0 and (r["g"] is None or r["g"] >= 0)
        assert r["d"] is None or 0 <= r["d"] < 360


RT2 = """#YY  MM DD hh mm WDIR WSPD GST  WVHT
#yr  mo dy hr mn degT m/s  m/s     m
2026 10 10 16 18  50  1.5  3.6    MM
2026 10 10 16 12  MM   MM   MM    MM
2026 10 10 16 06  60  2.0  4.0    MM
2026 10 09 16 06  70  2.5  4.5    MM
2026 10 09 15 00  80  3.0  5.0    MM
"""


def test_parse_realtime2_stops_at_since_and_caps():
    t0 = calendar.timegm((2026, 10, 10, 16, 18, 0))
    rows = W.parse_realtime2(RT2, t0 - 86400)
    assert [r["t"] for r in rows] == [t0, t0 - 720]                        # 16:18, 16:06; the MM row skipped
    rows = W.parse_realtime2(RT2, t0 - 86400 - 720)                         # a row exactly at since is kept
    assert [r["t"] for r in rows] == [t0, t0 - 720, t0 - 86400 - 720] and rows[2]["d"] == 70
    assert len(W.parse_realtime2(RT2, 0, 2)) == 2
    assert W.parse_realtime2(RT2, t0 + 1) == []


def test_weather_helpers():
    assert W._temp("27.1") == 27.1 and W._temp("MM") is None and W._temp(70) is None and W._temp(-95) is None
    assert W._pct(85.0) == 85 and isinstance(W._pct(85.0), int) and W._pct(101) is None
    assert W._hpa("1014.1") == 1014.1 and W._hpa(700) is None and W._hpa_delta("-1.3") == -1.3 and W._hpa_delta(70) is None
    assert W._km("10") == 10.0 and W._km(-1) is None and W._words("  light   rain ") == "light rain" and W._words("") is None
    assert W.with_weather({"t": 1}, at=20.0, wt=None, zz=5) == {"t": 1, "at": 20.0} and W.with_weather(None, at=1) is None
    assert W.humidity_from(25.0, 20.0) == 74 and W.humidity_from(30.0, 30.0) == 100 and W.humidity_from(None, 20.0) is None
    assert W.humidity_from(-40.0, -40.0) == 100 and 1 <= W.humidity_from(40.0, -40.0) <= 2


def test_parse_realtime2_carries_the_weather_columns():
    rows = W.parse_realtime2(fx("realtime2_51003.txt", "r"), 0)
    r = rows[0]
    assert r["at"] == 27.1 and r["wt"] == 27.5 and r["dp"] == 24.5 and r["p"] == 1012.4 and "pt" not in r and "vis" not in r
    rows = W.parse_realtime2(fx("realtime2_OOUH1.txt", "r"), 0)
    assert rows[0]["wt"] == 29.0 and rows[0]["p"] == 1014.1 and "at" not in rows[0], "the gauge's relay carries water temperature and pressure only"
    text = ("#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE\n"
            "#yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft\n"
            "2026 10 10 16 18  50  1.5  3.6    MM    MM    MM  MM 1014.1  25.0  29.0  20.0  5.0 -1.2    MM\n")
    r = W.parse_realtime2(text, 0)[0]
    assert r["vis"] == 9.3 and r["pt"] == -1.2 and r["at"] == 25.0, "VIS in nautical miles -> km; PTDY the 3-hour change"


def test_parse_realtime2_fixture_is_24_hours_newest_first():
    text = fx("realtime2_OOUH1.txt", "r")
    t0 = first_t(text)
    rows = W.parse_realtime2(text, t0 - W.HISTORY_S)
    assert 150 <= len(rows) <= W.HISTORY_ROWS                               # 6-minute rows: ~240 in a day
    assert all(a["t"] > b["t"] for a, b in zip(rows, rows[1:])) and rows[-1]["t"] >= t0 - W.HISTORY_S


def test_parse_coops_series_and_the_weather_fixtures():
    air = W.parse_coops_series(json.loads(fx("coops_air_temperature.json")))
    assert len(air) >= 200 and all(isinstance(t, int) and 20 < v < 35 for t, v in air.items())
    assert W.parse_coops_series(json.loads(fx("coops_error.json"))) == {} and W.parse_coops_series([]) == {}
    assert W.parse_coops_series({"data": [{"t": "2026-10-10 16:42", "v": "x"}, {"t": "bad", "v": "1"}, {"t": "2026-10-10 16:48", "v": "1016.2"}]}) == {1791650880: 1016.2}


def test_parse_coops_wind_and_error():
    latest = W.parse_coops_wind(json.loads(fx("coops_latest.json")))
    assert len(latest) == 1 and latest[0]["s"] >= 0 and 0 <= latest[0]["d"] < 360
    day = W.parse_coops_wind(json.loads(fx("coops_24.json")))
    assert 200 <= len(day) <= 241 and all(a["t"] < b["t"] for a, b in zip(day, day[1:]))
    assert W.coops_error(json.loads(fx("coops_error.json"))).startswith("No data")
    assert W.coops_error(json.loads(fx("coops_latest.json"))) is None
    assert W.parse_coops_wind({"data": [{"t": "bad", "s": "1"}, "x", {"t": "2026-10-10 16:48", "s": "x"}]}) == []
    assert W.coops_url("1612340", date="latest").startswith(W.COOPS_API + "?product=wind&station=1612340&")
    assert "application=allshoresurf.com" in W.coops_url("1612340", range=24)


@pytest.mark.parametrize("dir_v,spd,gst,want", [
    (150, 11, "", (5.7, None, 150)),
    (360, 10, 15, (5.1, 7.7, 0)),              # north is written 360
    (0, 5, 11, (2.6, 5.7, None)),               # 0 with a speed: variable
    ("VRB", 5, None, (2.6, None, None)),        # the API's spelling
    (0, 0, "", (0.0, None, None)),              # calm
    (90, 0, "", (0.0, None, None)),             # calm with a direction given: none
    ("", 7, "", (3.6, None, None)),
    (90, "", "", None),                         # no speed: no reading
])
def test_metar_reading_rules(dir_v, spd, gst, want):
    r = W._metar_reading(1791651180, dir_v, spd, gst)
    assert (r is None and want is None) or (r["s"], r["g"], r["d"]) == want


METAR_CSV = ("raw_text,station_id,observation_time,latitude,longitude,wind_dir_degrees,wind_speed_kt,wind_gust_kt\n"
             '"METAR PHNL 101653Z 05005KT",PHNL,2026-10-10T16:53:00.000Z,21.3,-157.9,50,5,\n'
             '"METAR PHNL 101553Z 04004KT",PHNL,2026-10-10T15:53:00.000Z,21.3,-157.9,40,4,\n'
             '"METAR K3L4 101635Z VRB05G11KT",K3L4,2026-10-10T16:35:00.000Z,40.1,-75.2,0,5,11\n'
             '"METAR YKSC 101703Z 00000KT",YKSC,2026-10-10T17:03:00.000Z,-30,140,0,0,\n'
             '"METAR KGMU 101700Z",KGMU,2026-10-10T17:00:00.000Z,34,-82,,,\n'
             '"METAR BAD",BAD,not-a-time,1,1,90,5,\n')


def test_parse_metar_cache_newest_per_station_plain_and_gzip():
    want = {"PHNL": {"t": 1791651180, "s": 2.6, "g": None, "d": 50},
            "K3L4": {"t": 1791650100, "s": 2.6, "g": 5.7, "d": None},
            "YKSC": {"t": 1791651780, "s": 0.0, "g": None, "d": None}}
    assert W.parse_metar_cache(METAR_CSV.encode()) == want
    assert W.parse_metar_cache(gzip.compress(METAR_CSV.encode())) == want
    with pytest.raises(ValueError):
        W.parse_metar_cache(b"a,b\n1,2\n")


def test_parse_metar_cache_fixture():
    got = W.parse_metar_cache(fx("metars.csv"))
    assert {"PHNL", "KSFO", "EGLL"} <= set(got) and "KGMU" not in got        # KGMU reported no wind
    assert got["KSFO"]["g"] is not None and got["KSFO"]["g"] >= got["KSFO"]["s"]
    assert got["YKSC"] == {"t": got["YKSC"]["t"], "s": 0.0, "g": None, "d": None}   # calm


@pytest.mark.parametrize("wx,cover,want", [
    ("-SHRA BR", "BKN", "light showers of rain, mist"), ("VCTS", None, "thunderstorm nearby"), (None, "FEW", "A few clouds"),
    ("+TSRA", "OVC", "heavy thunderstorm with rain"), ("FZFG", "", "freezing fog"), ("BCFG", None, "patches of fog"),
    ("RA SN", None, "rain, snow"), ("NSW", "CLR", "Clear"), ("", "", None), ("-RASN", None, "light rain and snow"),
    ("ZZ", None, "zz"), (None, "OVX", "Sky obscured")])
def test_metar_weather_words(wx, cover, want):
    assert W.metar_weather_words(wx, cover) == want


def test_metar_visibility_km():
    assert W.metar_visibility_km("10+") == 16.1 and W.metar_visibility_km("1/2") == 0.8 and W.metar_visibility_km(3) == 4.8
    assert W.metar_visibility_km(None) is None and W.metar_visibility_km("") is None and W.metar_visibility_km("x") is None
    assert W.metar_visibility_km("20+") == 32.2 and W.metar_visibility_km("1/0") is None


def test_parse_metar_history_carries_the_weather():
    rows = W.parse_metar_history(json.loads(fx("metar_history.json")))
    r = rows[0]
    assert r["at"] == 25.6 and r["dp"] == 21.1 and r["p"] == 1014.3 and r["vis"] == 16.1 and r["wx"] == "A few clouds"
    assert r["vg"] == 1                                                    # "10+": 10 statute miles OR MORE (G28 A-3)
    rows = W.parse_metar_history([{"obsTime": 1791651180, "wdir": 50, "wspd": 5, "temp": 20, "dewp": 10, "altim": 1013.0, "visib": "6", "wxString": "-RA"}])
    assert rows[0]["p"] == 1013.0 and rows[0]["wx"] == "light rain" and rows[0]["vis"] == 9.7, "altim when slp is missing"


def test_parse_nws_observations_carries_the_weather():
    rows = W.parse_nws_observations(json.loads(fx("nws_history.json")))
    assert rows[0]["at"] == 24.7 and rows[0]["dp"] == 22.0 and rows[0]["rh"] == 85 and "p" not in rows[0]
    q = lambda v, u, qc="S": {"unitCode": u, "value": v, "qualityControl": qc}
    doc = {"@graph": [{"timestamp": "2026-10-10T20:50:00+00:00", "windSpeed": q(3.6, "wmoUnit:km_h-1"), "windDirection": q(90, "wmoUnit:degree_(angle)"),
                       "temperature": q(77, "wmoUnit:degF"), "dewpoint": q(290.15, "wmoUnit:K"), "relativeHumidity": q(60.4, "wmoUnit:percent"),
                       "seaLevelPressure": q(101520, "wmoUnit:Pa"), "barometricPressure": q(100000, "wmoUnit:Pa"), "visibility": q(16090, "wmoUnit:m"),
                       "textDescription": "Mostly Cloudy"},
                      {"timestamp": "2026-10-10T20:40:00+00:00", "windSpeed": q(3.6, "wmoUnit:km_h-1"), "windDirection": q(90, "wmoUnit:degree_(angle)"),
                       "temperature": q(20, "wmoUnit:degC", "X"), "barometricPressure": q(100000, "wmoUnit:Pa"), "visibility": q(2, "wmoUnit:furlong")}]}
    a, b = W.parse_nws_observations(doc)
    assert a["at"] == 25.0 and a["dp"] == 17.0 and a["rh"] == 60 and a["p"] == 1015.2 and a["vis"] == 16.1 and a["wx"] == "Mostly Cloudy"
    assert "at" not in b and b["p"] == 1000.0 and "vis" not in b, "a rejected temperature is dropped; the barometric pressure stands in; an unknown unit is nothing"


def test_conditions_newest_within_three_hours_and_the_pressure_trend():
    rows = [{"t": 0, "s": 1.0, "at": 20.0, "p": 1010.0, "wx": "rain"}, {"t": 10800 - 600, "s": 1.0, "p": 1011.0},
            {"t": 10800, "s": 1.0, "at": 21.0}, {"t": 21600, "s": 1.0, "p": 1013.5, "dp": 15.0}]
    c = W.conditions(rows)
    assert c["at"] == [10800, 21.0] and c["p"] == [21600, 1013.5] and c["dp"] == [21600, 15.0] and "rh" not in c
    assert "wx" not in c, "6 hours old: not current (3 hours exactly still is: the air temperature)"
    assert c["trend"] == 2.5, "the pressure 3 h earlier (within half an hour of it)"
    assert W.conditions([{"t": 5, "s": 1.0, "p": 1000.0, "pt": -1.5}])["trend"] == -1.5, "the report's own 3-hour change first"
    assert W.conditions([{"t": 5, "s": 1.0, "p": 1000.0}])["trend"] is None and W.conditions([]) == {}


def test_parse_metar_history_fixture_and_shapes():
    rows = W.parse_metar_history(json.loads(fx("metar_history.json")))
    assert 20 <= len(rows) <= 60 and all(isinstance(r["t"], int) for r in rows)
    assert W.parse_metar_history([{"obsTime": 1, "wdir": "VRB", "wspd": 3}, {"obsTime": 2}, "x", {"obsTime": 3, "wdir": 90, "wspd": 0}]) == [
        {"t": 1, "s": 1.5, "g": None, "d": None}, {"t": 3, "s": 0.0, "g": None, "d": None}]
    assert W.parse_metar_history({"not": "a list"}) == []


def test_parse_nws_observations_units_quality_and_calm():
    row = lambda ts, s, d, g=None, qs="S", qd="S", unit="wmoUnit:km_h-1": {
        "timestamp": ts, "windSpeed": {"unitCode": unit, "value": s, "qualityControl": qs},
        "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": d, "qualityControl": qd},
        "windGust": {"unitCode": unit, "value": g, "qualityControl": "S"}}
    doc = {"@graph": [row("2026-10-10T20:50:00+00:00", 6.876, 203.6, 14.796),         # km/h -> m/s
                      row("2026-10-10T20:40:00Z", None, None, 13.0, qs="Z", qd="Z"),   # a gust-only row: no reading
                      row("2026-10-10T20:30:00+00:00", 0, 90),                         # calm: no direction
                      row("2026-10-10T20:20:00+00:00", 36.0, 180, qs="X"),             # rejected by quality control
                      row("2026-10-10T20:10:00+00:00", 5.0, 180, qd="B"),              # a bad direction only
                      row("2026-10-10T20:00:00+00:00", 10.0, 90, unit="wmoUnit:m_s-1"),
                      row("2026-10-10T19:50:00+00:00", 10.0, 90, unit="wmoUnit:[kn_i]"),
                      row("2026-10-10T19:40:00+00:00", 10.0, 90, unit="wmoUnit:furlong"),   # unknown unit: no reading
                      row("2026-10-10T19:30:00Z", 3.6, 270),                       # a "Z" time: the same instant as +00:00
                      row("bad time", 5.0, 90), "not a row"]}
    got = W.parse_nws_observations(doc)
    assert got == [{"t": 1791665400, "s": 1.9, "g": 4.1, "d": 204}, {"t": 1791664200, "s": 0.0, "g": None, "d": None},
                   {"t": 1791663000, "s": 1.4, "g": None, "d": None}, {"t": 1791662400, "s": 10.0, "g": None, "d": 90},
                   {"t": 1791661800, "s": 5.1, "g": None, "d": 90}, {"t": 1791660600, "s": 1.0, "g": None, "d": 270}]
    assert W._nws_epoch("2026-10-10T19:30:00Z") == W._nws_epoch("2026-10-10T19:30:00+00:00") == 1791660600
    assert W._nws_epoch("2026-10-10T09:30:00-10:00") == 1791660600 and W._nws_epoch(None) is None
    feats = {"features": [{"properties": row("2026-10-10T20:50:00+00:00", 3.6, 10)}]}       # GeoJSON: the same rows
    assert W.parse_nws_observations(feats) == [{"t": 1791665400, "s": 1.0, "g": None, "d": 10}]
    assert W.parse_nws_observations({}) == [] and W.parse_nws_observations([]) == []


def test_parse_nws_observations_fixtures():
    obs = W.parse_nws_observations(json.loads(fx("nws_obs.json")))
    assert 10 <= len(obs) <= 12 and all(a["t"] > b["t"] for a, b in zip(obs, obs[1:]))     # newest first; one gust-only row
    assert all(0 <= r["s"] < 30 and (r["d"] is None or 0 <= r["d"] < 360) for r in obs)
    hist = W.parse_nws_observations(json.loads(fx("nws_history.json")))
    assert 30 <= len(hist) <= 40 and all(r["g"] is None or r["g"] >= r["s"] for r in hist)


def test_gunzip_bounded_cuts_a_bomb():
    assert W.gunzip_bounded(gzip.compress(b"x" * 1000), 1000) == b"x" * 1000
    with pytest.raises(ValueError):
        W.gunzip_bounded(gzip.compress(b"x" * 1001), 1000)
    bomb = gzip.compress(b"\0" * (4 * 1024 * 1024))                         # 4 MB of zeros gzips to ~4 KB
    with pytest.raises(ValueError):
        W.gunzip_bounded(bomb, 1024 * 1024)


# ------------------------------------------------------------------ the snapshot

def test_snapshot_loads_and_ids_validate():
    stations, doc = W.load_stations(os.path.join(os.path.dirname(HERE), "wind_stations.json"))
    assert len(stations) > 2000 and all(W.valid_id(s) for s in stations)
    lst = W.client_list(doc)
    assert lst["fields"] == doc["fields"] and len(lst["stations"]) == len(stations)
    assert not W.valid_id("coops:1612340/x") and not W.valid_id("ndbc:oouh1") and not W.valid_id(5)
    assert set(W.by_source(stations)) == {"coops", "ndbc", "metar", "nws"}


# ------------------------------------------------------------------ providers

def test_make_providers_and_aliases():
    provs = W.make_providers(STATIONS, FakeFetch(), coops_workers=20)
    assert [p.source for p in provs] == ["NDBC", "METAR", "COOPS", "NWS"]
    ndbc, metar, coops, nws = provs
    assert set(nws.stations) == {"001HE", "029HI"} and nws.workers == 2 and nws.per_refresh == W.NWS_PER_REFRESH == 25
    assert nws.list_ttl_sec == W.NWS_TTL_S == 60 and nws.keep_s == W.NWS_KEEP_S and nws.refused_codes == ("403", "429")
    assert W.make_providers(STATIONS, FakeFetch(), nws_workers=9, nws_per_refresh=0)[3].workers == 8
    assert W.make_providers(STATIONS, FakeFetch(), nws_workers=9, nws_per_refresh=0)[3].per_refresh == 1
    assert set(ndbc.stations) == {"51003", "HRRH1"} and set(metar.stations) == {"PHNL", "KSFO", "KGMU"}
    assert set(coops.stations) == {"1612340", "1611400", "1612401"} and coops.workers == 8
    assert ndbc.aliases["OOUH1"]["id"] == "coops:1612340" and ndbc.aliases["NWWH1"]["id"] == "coops:1611400"
    assert all(p.extra_keys == ("wind", "alias_of") for p in provs)
    assert ndbc.list_ttl_sec == 300 and metar.list_ttl_sec == 300 and coops.list_ttl_sec == W.COOPS_TTL_S == 60
    assert coops.per_refresh == W.COOPS_PER_REFRESH == 24 and W.CoopsWindProvider({}, FakeFetch(), 0, 9999).per_refresh == 500
    assert W.make_providers(STATIONS, FakeFetch())[2].workers == 2, "two at a time by default: a trickle, not a burst"


def test_ndbc_provider_lists_its_stations_and_the_relays(clock):
    provs, fetch = make_fakes(clock.time())
    lst, version, _ = provs[0].list_stations_versioned()
    by = {e["id"]: e for e in lst}
    assert version == 1 and "ndbc:51003" in by and "ndbc:HRRH1" in by and "ndbc:51201" not in by and "ndbc:ILOH1" not in by
    e = by["ndbc:51003"]
    assert e["wind"][0] >= 0 and e["latest_time"].endswith("Z") and e["is_stale"] is False and e["lat"] == 19.151
    relay = by["ndbc:OOUH1"]
    assert relay["alias_of"] == "coops:1612340" and relay["lat"] == 21.30333 and relay["name"] == "Honolulu"
    assert "alias_of" not in e
    assert fetch.calls[0][2]["User-Agent"] == W.USER_AGENT and fetch.calls[0][1] == W.NDBC_LATEST_MAX
    assert provs[0].latest("51003") == {"time_utc": e["latest_time"], "wind": e["wind"]}
    assert provs[0].latest("51201") is None


def test_stale_readings_are_flagged_past_two_hours(clock):
    provs, _ = make_fakes(clock.time())
    clock.now += 3 * 3600
    lst = provs[0].list_stations_versioned()[0]
    assert all(e["is_stale"] for e in lst)                                   # every fixture row is now > 2 h old


def test_metar_provider_lists_the_snapshot_airports_with_wind(clock):
    provs, fetch = make_fakes(clock.time())
    lst = provs[1].list_stations_versioned()[0]
    ids = {e["id"] for e in lst}
    assert ids == {"metar:PHNL", "metar:KSFO"}                               # KGMU reported no wind; EGLL not in the snapshot
    assert fetch.calls[-1][2]["User-Agent"] == W.USER_AGENT


def test_coops_provider_one_failure_is_a_missing_reading_all_failures_fail(clock):
    provs, fetch = make_fakes(clock.time())
    coops = provs[2]
    lst, version, _ = coops.list_stations_versioned()
    assert [e["id"] for e in lst] == ["coops:1612340"] and version == 1    # 1611400 no data, 1612401 unreachable
    assert fetch.count("station=1612340") == 1 and fetch.count("station=1612401") == 1
    assert "application=allshoresurf.com" in fetch.calls[-1][0]
    # every request failing: the fetch fails and the last good list is kept
    fetch.table = {k: IOError("down") for k in fetch.table}
    clock.now += coops.list_ttl_sec + 1
    lst2, version2, _ = coops.list_stations_versioned()
    assert lst2 == lst and version2 == 1 and coops.status()["last_error"].startswith("RuntimeError: every CO-OPS")


COOPS_IDS = {k.split(":")[1]: v for k, v in STATIONS.items() if k.startswith("coops:")}


def test_coops_provider_asks_a_slice_per_refresh_in_turn_and_keeps_the_readings(clock):
    """Step 5 F1: NOAA blocked the server for ~60 s after 232 requests in a few seconds. The feed now asks per_refresh
    gauges per refresh, round robin, and merges: a gauge keeps its reading until asked again; a failed request keeps it;
    NOAA's "no data" drops it; a reading older than COOPS_KEEP_S is dropped."""
    fetch = FakeFetch(coops_table())
    fetch.table[W.coops_url("1612401", date="latest")] = fx("coops_latest.json")   # all three answer (1611400: no data)
    coops = W.CoopsWindProvider(COOPS_IDS, fetch, workers=2, per_refresh=1)
    assert coops._ids == ["1611400", "1612340", "1612401"]
    ids = lambda: [e["id"] for e in coops.list_stations_versioned()[0]]
    assert ids() == [] and fetch.count("station=") == 1 and fetch.count("1611400") == 1    # the first slice: NOAA has no data
    clock.now += 60; assert ids() == ["coops:1612340"] and fetch.count("1612340") == 1
    clock.now += 60; assert ids() == ["coops:1612340", "coops:1612401"] and fetch.count("1612401") == 1
    clock.now += 60; assert ids() == ["coops:1612340", "coops:1612401"] and fetch.count("1611400") == 2, "round robin: the first gauge again"
    st = coops.status()
    assert st["readings"] == 2 and st["per_refresh"] == 1 and st["cursor"] == 1 and st["paused_s"] == 0
    # a request that fails keeps the gauge's reading (and, alone in its slice, fails the fetch: keep-last)
    fetch.table[W.coops_url("1612340", date="latest")] = IOError("timeout")
    v0 = coops.list_stations_versioned()[1]
    clock.now += 60; assert ids() == ["coops:1612340", "coops:1612401"] and fetch.count("1612340") == 2
    assert coops.list_stations_versioned()[1] == v0 and coops.status()["last_error"].startswith("RuntimeError: every CO-OPS")
    # NOAA's "no data" for a gauge drops its reading
    fetch.table[W.coops_url("1612340", date="latest")] = fx("coops_latest.json")
    fetch.table[W.coops_url("1612401", date="latest")] = fx("coops_error.json")
    clock.now += 60; ids()                                                            # 1612401 asked: no data
    assert ids() == ["coops:1612340"]
    # a reading older than COOPS_KEEP_S goes when the feed next runs (the gauge then shows "no reading")
    clock.now += W.COOPS_KEEP_S + 3600
    clock.now += 60; assert ids() == [] and coops.status()["readings"] == 0


def test_coops_provider_pauses_two_minutes_after_a_403(clock):
    fetch = FakeFetch(coops_table())
    coops = W.CoopsWindProvider(COOPS_IDS, fetch, workers=2, per_refresh=24)
    lst, v, _ = coops.list_stations_versioned()
    assert [e["id"] for e in lst] == ["coops:1612340"] and v == 1
    fetch.table[W.coops_url("1612340", date="latest")] = IOError("HTTP 403")
    clock.now += 60
    lst2, v2, _ = coops.list_stations_versioned()
    assert lst2 == lst and v2 == 1, "the readings in hand stay"
    assert coops.status()["last_error"].startswith("RuntimeError: NOAA refused") and coops.status()["paused_s"] == W.COOPS_PAUSE_S
    n = fetch.count("station=")
    clock.now += 60
    coops.list_stations_versioned()
    assert fetch.count("station=") == n and coops.status()["last_error"].startswith("RuntimeError: paused"), "no request during the pause"
    assert 0 < coops.status()["paused_s"] <= 60
    fetch.table[W.coops_url("1612340", date="latest")] = fx("coops_latest.json")
    clock.now += 61
    assert coops.list_stations_versioned()[1] == 2 and fetch.count("station=") > n and coops.status()["paused_s"] == 0


def test_nws_provider_asks_a_window_per_station_paced_and_keeps_readings_through_empty_answers(clock):
    """Step 5c: the NWS API feed is the CO-OPS feed's pacing over api.weather.gov: one observations request per
    station (the last 2 hours, the newest 6 rows, ld+json), the newest row with a speed is the reading; an answer
    without one keeps the reading in hand until NWS_KEEP_S (decision 12; G28 A-10); a 429 (or 403) pauses the feed."""
    fetch = FakeFetch(nws_table())
    nws = W.NwsProvider(NWS_IDS, fetch, workers=2, per_refresh=1)
    assert nws._ids == ["001HE", "029HI"]
    ids = lambda: [e["id"] for e in nws.list_stations_versioned()[0]]
    assert ids() == ["nws:001HE"]
    url, max_bytes, headers = fetch.calls[-1]
    start = W.iso_z(clock.time() - W.NWS_FEED_WINDOW_S)
    assert url == W.NWS_OBS_URL % ("001HE", start, W.NWS_FEED_LIMIT) and max_bytes == W.NWS_FEED_MAX
    assert headers == {"User-Agent": W.USER_AGENT, "Accept": "application/ld+json"}
    e = nws.list_stations_versioned()[0][0]
    newest = W.parse_nws_observations(json.loads(fx("nws_obs.json")))[0]
    assert e["wind"] == [newest["s"], newest["g"], newest["d"]] and e["latest_time"] == W.iso_z(newest["t"])
    assert e["name"] == "MECO Kealaloloa Ridge" and e["lat"] == 20.80393
    clock.now += 60; assert ids() == ["nws:001HE"] and fetch.count("029HI") == 1       # 029HI: nothing in the window
    # the station's answer turns empty (nothing in its 2-hour window): the reading in hand stays (G28 A-10)
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = b'{"@graph": []}'
    clock.now += 60; assert ids() == ["nws:001HE"] and nws.status()["readings"] == 1
    assert nws.status()["last_error"] is None
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = fx("nws_obs.json")
    clock.now += 60; ids()                                                            # 029HI's turn: still nothing
    assert nws.status()["readings"] == 1 and fetch.count("029HI") == 2
    clock.now += 60; assert ids() == ["nws:001HE"] and nws.status()["readings"] == 1   # 001HE asked again
    # a 429 pauses the feed for NWS_PAUSE_S with the readings kept
    fetch.table[W.NWS_API + "/stations/029HI/observations?"] = IOError("HTTP 429")
    v = nws.list_stations_versioned()[1]
    clock.now += 60
    assert ids() == ["nws:001HE"] and nws.list_stations_versioned()[1] == v
    assert nws.status()["last_error"] == "RuntimeError: the NWS API refused a request (HTTP 429): paused for %d s" % W.NWS_PAUSE_S
    assert nws.status()["paused_s"] == W.NWS_PAUSE_S
    n = fetch.count("observations")
    clock.now += 60; ids()
    assert fetch.count("observations") == n and nws.status()["last_error"].startswith("RuntimeError: paused after the NWS API's HTTP 403/429")
    for sid in ("001HE", "029HI"):
        fetch.table[W.NWS_API + "/stations/%s/observations?" % sid] = IOError("timeout")
    clock.now += W.NWS_PAUSE_S
    ids()                                                                            # the pause over: asked again
    assert fetch.count("observations") == n + 1 and nws.status()["last_error"] == "RuntimeError: every NWS API request failed (OSError: timeout)"
    assert ids() == ["nws:001HE"], "keep-last"


def test_coops_provider_takes_the_newest_row_of_an_answer(monkeypatch):
    monkeypatch.setattr(W.buoy_sources.time, "time", lambda: 1791650880 + 600)   # the reading is 10 min old, on any day
    two = json.dumps({"data": [{"t": "2026-10-10 16:42", "s": "2.0", "d": "80", "g": "3.0"},
                               {"t": "2026-10-10 16:48", "s": "1.0", "d": "74", "g": "3.5"}]}).encode()
    coops = W.CoopsWindProvider({"1612340": STATIONS["coops:1612340"]}, FakeFetch({W.coops_url("1612340", date="latest"): two}))
    assert coops._one("1612340") == ("1612340", {"t": 1791650880, "s": 1.0, "g": 3.5, "d": 74}, None)
    assert coops._fetch_stations()[0]["wind"] == [1.0, 3.5, 74]
    assert W.CoopsWindProvider({}, FakeFetch())._fetch_stations() == []


# ------------------------------------------------------------------ the merged table

def test_build_latest_rows_relays_and_missing():
    t = "2026-10-10T16:48:00Z"
    ndbc = [{"id": "ndbc:51003", "wind": [6.0, 8.0, 130], "latest_time": t},
            {"id": "ndbc:OOUH1", "wind": [3.6, 6.2, 190], "latest_time": t, "alias_of": "coops:1612340"},
            {"id": "ndbc:NWWH1", "wind": [2.0, 3.0, 90], "latest_time": "2026-10-10T16:00:00Z", "alias_of": "coops:1611400"},
            {"id": "ndbc:ZZZZZ", "wind": [1.0, None, None], "latest_time": t},              # not in the snapshot
            {"id": "ndbc:HRRH1", "wind": "bad", "latest_time": t},                          # malformed: skipped
            {"id": "ndbc:HRRH1", "wind": [0.0, None, None]}]                                # no time: skipped
    coops = [{"id": "coops:1612340", "wind": [1.0, 3.5, 74], "latest_time": t}]
    out = W.build_latest(STATIONS, {"ndbc": ndbc, "coops": coops, "metar": None}, 1791651300.5)
    assert out["fields"] == ["id", "t", "s", "g", "d"] and out["now"] == 1791651300 and out["stale_s"] == W.STALE_S
    rows = {r[0]: r for r in out["rows"]}
    assert [r[0] for r in out["rows"]] == sorted(rows)
    assert rows["coops:1612340"] == ["coops:1612340", 1791650880, 1.0, 3.5, 74]           # CO-OPS's own reading wins
    assert rows["coops:1611400"] == ["coops:1611400", 1791648000, 2.0, 3.0, 90]           # the relay fills the gap
    assert rows["ndbc:51003"][1:] == [1791650880, 6.0, 8.0, 130]
    assert "ndbc:ZZZZZ" not in rows and "ndbc:HRRH1" not in rows
    assert out["missing"] == ["coops:1612401", "ndbc:HRRH1"]                 # metar has no list yet: not missing


def test_build_latest_counts_a_listed_empty_source_as_missing():
    out = W.build_latest(STATIONS, {"coops": [], "ndbc": None, "metar": None}, 0)
    assert out["rows"] == [] and out["missing"] == sorted(s for s in STATIONS if s.startswith("coops:"))
    assert W.build_latest({}, {}, 0)["rows"] == []


# ------------------------------------------------------------------ the history service

class Clock:
    def __init__(self, t):
        self.t = float(t)

    def __call__(self):
        return self.t


def history_fetch():
    return FakeFetch({W.NDBC_RT2_URL % "51003": fx("realtime2_51003.txt"),
                      W.NDBC_RT2_URL % "OOUH1": fx("realtime2_OOUH1.txt"),
                      W.coops_url("1612340", range=24): fx("coops_24.json"),
                      W.coops_url("1612340", product="air_temperature", range=24): fx("coops_air_temperature.json"),
                      W.coops_url("1612340", product="water_temperature", range=24): fx("coops_water_temperature.json"),
                      W.coops_url("1612340", product="air_pressure", range=24): fx("coops_air_pressure.json"),
                      W.coops_url("1611400", range=24): fx("coops_error.json"),
                      W.METAR_API_URL % "PHNL": fx("metar_history.json")})


def test_history_ndbc_is_the_last_24_hours_ascending_and_cached():
    fetch = history_fetch()
    now = first_t(fx("realtime2_51003.txt", "r")) + 60
    clock = Clock(now)
    svc = W.WindHistory(STATIONS, fetch, now=clock)
    status, p = svc.history("ndbc:51003")
    assert status == "ok" and p["id"] == "ndbc:51003" and p["kind"] == "buoy" and p["tz"] == "Pacific/Honolulu"
    assert p["hours"] == 24 and p["source"] == W.ATTRIBUTION["ndbc"] and p["via"] is None and p["note"] is None
    assert len(p["t"]) == len(p["s"]) == len(p["g"]) == len(p["d"]) and 0 < len(p["t"]) <= W.HISTORY_ROWS
    assert all(a < b for a, b in zip(p["t"], p["t"][1:])) and p["t"][0] >= now - W.HISTORY_S and p["t"][-1] == now - 60
    assert fetch.calls[0][1] == W.NDBC_RT2_MAX and fetch.calls[0][2]["User-Agent"] == W.USER_AGENT
    assert svc.history("ndbc:51003")[1] is p and fetch.count("51003") == 1   # cached
    clock.t += svc.ok_ttl + 1
    svc.history("ndbc:51003")
    assert fetch.count("51003") == 2 and svc.entries() == 1


def test_history_coops_and_metar_and_notes():
    fetch = history_fetch()
    rows = W.parse_coops_wind(json.loads(fx("coops_24.json")))
    svc = W.WindHistory(STATIONS, fetch, now=Clock(rows[-1]["t"] + 60))
    status, p = svc.history("coops:1612340")
    assert status == "ok" and p["src"] == "coops" and p["alias"] == "OOUH1" and len(p["t"]) >= 200 and p["via"] is None
    status, p = svc.history("coops:1611400")                                # NOAA: no data -> ok, empty, a note
    assert status == "ok" and p["t"] == [] and p["note"] == W.NO_HISTORY
    hist = W.parse_metar_history(json.loads(fx("metar_history.json")))
    svc = W.WindHistory(STATIONS, fetch, now=Clock(max(r["t"] for r in hist) + 60))
    status, p = svc.history("metar:PHNL")
    assert status == "ok" and p["kind"] == "airport" and 20 <= len(p["t"]) <= 60 and p["source"] == W.ATTRIBUTION["metar"]
    assert fetch.calls[-1][2]["User-Agent"] == W.USER_AGENT


def test_history_weather_fields_and_conditions():
    fetch = history_fetch()
    rows = W.parse_coops_wind(json.loads(fx("coops_24.json")))
    svc = W.WindHistory(STATIONS, fetch, now=Clock(rows[-1]["t"] + 60))
    status, p = svc.history("coops:1612340")
    assert status == "ok" and fetch.count("product=air_temperature") == 1 and fetch.count("product=water_temperature") == 1
    assert fetch.count("product=air_pressure") == 1 and fetch.count("product=humidity") == 0, "only the gauge's wx letters (awp)"
    assert len(p["at"]) == len(p["t"]) and sum(1 for v in p["at"] if v is not None) >= 150 and "dp" not in p and "rh" not in p
    assert p["units"]["at"] == "degC" and p["units"]["p"] == "hPa" and p["units"]["vis"] == "km"
    c = p["conditions"]
    assert c["at"][0] == p["t"][-1] or c["at"][0] >= p["t"][-1] - W.CONDITIONS_S
    assert 20 < c["at"][1] < 35 and 20 < c["wt"][1] < 35 and 990 < c["p"][1] < 1040 and c["trend"] is not None
    # a product that fails or has no data adds nothing; the window still answers
    fetch.table[W.coops_url("1612340", product="air_temperature", range=24)] = IOError("down")
    fetch.table[W.coops_url("1612340", product="water_temperature", range=24)] = fx("coops_error.json")
    svc = W.WindHistory(STATIONS, fetch, now=Clock(rows[-1]["t"] + 60))
    status, p = svc.history("coops:1612340")
    assert status == "ok" and "at" not in p and "wt" not in p and "p" in p and "at" not in p["conditions"]
    # the other sources carry their weather from the answer already fetched; humidity from the dew point
    svc = W.WindHistory(STATIONS, fetch, now=Clock(first_t(fx("realtime2_51003.txt", "r")) + 60))
    status, p = svc.history("ndbc:51003")
    assert p["at"][-1] == 27.1 and p["wt"][-1] == 27.5 and p["dp"][-1] == 24.5 and p["p"][-1] == 1012.4 and p["rh"][-1] == 86
    assert p["conditions"]["rh"][1] == 86 and "wx" not in p, "NDBC has no weather words"
    hist = W.parse_metar_history(json.loads(fx("metar_history.json")))
    svc = W.WindHistory(STATIONS, fetch, now=Clock(max(r["t"] for r in hist) + 60))
    status, p = svc.history("metar:PHNL")
    assert p["conditions"]["wx"][1] == "A few clouds" and p["conditions"]["vis"][1] == 16.1 and p["rh"][-1] == W.humidity_from(p["at"][-1], p["dp"][-1])
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = fx("nws_history.json")
    nws = W.parse_nws_observations(json.loads(fx("nws_history.json")))
    svc = W.WindHistory(STATIONS, fetch, now=Clock(nws[0]["t"] + 60))
    status, p = svc.history("nws:001HE")
    assert p["rh"][-1] == 85 and p["at"][-1] == 24.7 and p["conditions"]["rh"] == [nws[0]["t"], 85]


def test_history_gauge_falls_back_on_its_ndbc_relay():
    fetch = history_fetch()
    fetch.table[W.coops_url("1612340", range=24)] = IOError("NOAA down")
    now = first_t(fx("realtime2_OOUH1.txt", "r")) + 60
    svc = W.WindHistory(STATIONS, fetch, now=Clock(now))
    status, p = svc.history("coops:1612340")
    assert status == "ok" and p["via"] == "ndbc" and p["source"] == W.ATTRIBUTION["ndbc"] and len(p["t"]) >= 150
    assert fetch.count("OOUH1") == 1
    fetch.table[W.coops_url("1612401", range=24)] = IOError("NOAA down")   # no relay: an error, cached a minute
    status, p = svc.history("coops:1612401")
    assert status == "error" and p == {"error": W.UNAVAILABLE}
    assert svc.history("coops:1612401")[0] == "error" and fetch.count("1612401") == 1
    assert fetch.count("realtime2") == 1                                     # no relay: NDBC was not asked for one


def test_history_nws_cut_at_24_hours_behind_its_own_bucket():
    fetch = history_fetch()
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = fx("nws_history.json")
    hist = W.parse_nws_observations(json.loads(fx("nws_history.json")))
    now = hist[0]["t"] + 60
    svc = W.WindHistory(STATIONS, fetch, now=Clock(now))
    status, p = svc.history("nws:001HE")
    assert status == "ok" and p["src"] == "nws" and p["kind"] == "utility" and p["source"] == W.ATTRIBUTION["nws"]
    assert p["t"] == sorted(r["t"] for r in hist) and p["t"][-1] == now - 60 and p["via"] is None and p["note"] is None
    url, max_bytes, headers = fetch.calls[-1]
    assert url == W.NWS_OBS_URL % ("001HE", W.iso_z(now - W.HISTORY_S), W.NWS_HISTORY_LIMIT) and max_bytes == W.NWS_HISTORY_MAX
    assert headers["Accept"] == "application/ld+json" and headers["User-Agent"] == W.USER_AGENT
    # rows older than 24 h are cut; an empty answer is a known "no readings"
    svc2 = W.WindHistory(STATIONS, fetch, now=Clock(hist[-1]["t"] + W.HISTORY_S + 60))
    assert svc2.history("nws:001HE")[1]["t"] == sorted(r["t"] for r in hist if r["t"] >= hist[-1]["t"] + 60)
    fetch.table[W.NWS_API + "/stations/029HI/observations?"] = b'{"@graph": []}'
    assert svc.history("nws:029HI")[1]["note"] == W.NO_HISTORY
    # its own token bucket: the METAR bucket is untouched
    bucket = W.TokenBucket(rate=1, now=lambda: 0.0)
    svc3 = W.WindHistory(STATIONS, fetch, now=Clock(now), nws_bucket=bucket)
    assert svc3.history("nws:001HE")[0] == "ok" and svc3.history("nws:029HI") == ("busy", {"error": W.BUSY})
    assert svc3.history("metar:PHNL")[0] == "ok" and svc3.bucket.rate == W.METAR_BUCKET_PER_MIN and bucket.rate == 1
    assert W.WindHistory(STATIONS, fetch).nws_bucket.rate == W.NWS_BUCKET_PER_MIN == 30


def test_history_unknown_errors_and_ttls():
    fetch = history_fetch()
    fetch.table[W.NDBC_RT2_URL % "51003"] = IOError("timeout")
    clock = Clock(1791651300)
    svc = W.WindHistory(STATIONS, fetch, now=clock)
    assert svc.history("nope")[0] == "final" and svc.history("ndbc:51201")[0] == "final" and fetch.calls == []
    assert svc.history("ndbc:51003") == ("error", {"error": W.UNAVAILABLE})
    clock.t += svc.error_ttl - 1
    svc.history("ndbc:51003")
    assert fetch.count("51003") == 1
    clock.t += 2
    svc.history("ndbc:51003")
    assert fetch.count("51003") == 2
    fetch.table[W.NDBC_RT2_URL % "51003"] = b"\xff\xfe not a feed"           # unreadable: no rows, a note
    clock.t += svc.error_ttl + 1
    assert svc.history("ndbc:51003")[1]["note"] == W.NO_HISTORY


def test_history_ndbc_without_a_24_hour_file_is_a_known_answer(clock):
    """Step 5 F4: NDBC publishes no realtime2 file for eight Korean buoys (HTTP 404): an ok answer with no rows and its
    own note, cached like any ok answer (not retried every minute); the page shows the flag's own reading. The relay
    of a gauge alike."""
    fetch = history_fetch()
    fetch.table[W.NDBC_RT2_URL % "51003"] = IOError("HTTP 404")
    fetch.table[W.coops_url("1612340", range=24)] = IOError("timeout")
    fetch.table[W.NDBC_RT2_URL % "OOUH1"] = IOError("HTTP 404")
    c = Clock(1791651300)
    svc = W.WindHistory(STATIONS, fetch, now=c)
    status, p = svc.history("ndbc:51003")
    assert status == "ok" and p["t"] == [] and p["note"] == W.NDBC_NO_FILE and p["via"] is None
    c.t += svc.error_ttl + 1
    svc.history("ndbc:51003")
    assert fetch.count("51003") == 1, "cached as an ok answer"
    status, p = svc.history("coops:1612340")
    assert status == "ok" and p["t"] == [] and p["note"] == W.NDBC_NO_FILE and p["via"] == "ndbc"
    fetch.table[W.NDBC_RT2_URL % "51003"] = IOError("HTTP 500")
    c.t += svc.ok_ttl + 1
    assert svc.history("ndbc:51003") == ("error", {"error": W.UNAVAILABLE}), "any other failure is a passing one"


def test_history_coops_and_metar_cut_at_24_hours():
    old, new = "2026-10-09 10:00", "2026-10-10 16:48"                      # 30 h and a few minutes before now
    doc = json.dumps({"data": [{"t": old, "s": "9.0", "d": "180", "g": "12.0"}, {"t": new, "s": "1.0", "d": "74", "g": "3.5"}]}).encode()
    fetch = FakeFetch({W.coops_url("1612340", range=24): doc,
                       W.METAR_API_URL % "PHNL": json.dumps([{"obsTime": 1791651180, "wdir": 50, "wspd": 5},
                                                             {"obsTime": 1791651180 - 30 * 3600, "wdir": 50, "wspd": 20}]).encode()})
    svc = W.WindHistory(STATIONS, fetch, now=Clock(1791651180 + 600))
    assert svc.history("coops:1612340")[1]["t"] == [1791650880]
    assert svc.history("metar:PHNL")[1]["t"] == [1791651180]


def test_history_dedupes_sorts_and_caps():
    rows = [{"t": 10 + k, "s": 1.0, "g": None, "d": None} for k in range(400)] + [{"t": 10, "s": 2.0, "g": None, "d": 5}]
    svc = W.WindHistory(STATIONS, FakeFetch(), now=Clock(10 + 400))
    svc._ndbc = lambda local, since: list(reversed(rows))
    status, p = svc.history("ndbc:51003")
    assert status == "ok" and len(p["t"]) == W.HISTORY_ROWS and p["t"] == list(range(110, 410))
    assert p["t"][0] > 10                                                    # the oldest rows are the ones cut


def test_history_lru_and_token_bucket():
    fetch = history_fetch()
    bucket = W.TokenBucket(rate=2, now=lambda: 0.0)
    svc = W.WindHistory(STATIONS, fetch, now=Clock(1791700000), max_entries=2, bucket=bucket)
    assert svc.history("metar:PHNL")[0] == "ok" and svc.history("metar:KSFO")[0] == "error"   # no fixture for KSFO
    fetch.table[W.METAR_API_URL % "KGMU"] = b"[]"
    assert svc.history("metar:KGMU") == ("busy", {"error": W.BUSY})          # the third request within the minute
    assert fetch.count("KGMU") == 0 and svc.entries() == 2
    assert svc.history("metar:KGMU")[0] == "busy"                             # busy is never cached: asked again ...
    bucket._now = lambda: 60.0                                                # ... and answered once the bucket refills
    assert svc.history("metar:KGMU")[1]["note"] == W.NO_HISTORY and fetch.count("KGMU") == 1
    assert svc.entries() == 2 and svc._get(("h", "metar:PHNL")) is None       # LRU: the oldest entry went


def test_token_bucket_refills_at_its_rate():
    t = {"v": 0.0}
    b = W.TokenBucket(rate=60, now=lambda: t["v"])
    assert all(b.take() for _ in range(60)) and not b.take()
    t["v"] = 1.0
    assert b.take() and not b.take()                                         # one a second
    t["v"] = 1000.0
    assert sum(1 for _ in range(100) if b.take()) == 60                      # never more than the rate held


def test_history_singleflight_and_busy():
    gate = threading.Event()

    def slow(url, max_bytes, headers=None):
        gate.wait(5)
        return fx("realtime2_51003.txt")
    fetch = FakeFetch({W.NDBC_RT2_URL % "51003": slow, W.NDBC_RT2_URL % "OOUH1": slow})
    svc = W.WindHistory(STATIONS, fetch, now=Clock(first_t(fx("realtime2_51003.txt", "r")) + 60), builds=1, wait_s=0.3)
    out = {}

    def ask(sid, k):
        out[k] = svc.history(sid)
    ts = [threading.Thread(target=ask, args=("ndbc:51003", k)) for k in range(4)]
    for th in ts:
        th.start()
    _time.sleep(0.1)
    status, _ = svc.history("coops:1612340")                                 # another key while the one build runs
    assert status == "busy"
    gate.set()
    for th in ts:
        th.join(5)
    assert fetch.count("51003") == 1 and {v[0] for v in out.values()} <= {"ok", "busy"}
    assert sum(1 for v in out.values() if v[0] == "ok") >= 1
    # the waiters on the one key gave up after wait_s (the build held its lock longer): busy, not a long wait
    assert sum(1 for v in out.values() if v[0] == "busy") >= 2


def test_after_fork_resets_the_service_locks():
    svc = W.WindHistory(STATIONS, FakeFetch(), now=Clock(0))
    lock, sem = svc._lock, svc._builds
    svc._inflight["x"] = [threading.Lock(), 1]
    W._after_fork()
    assert svc._lock is not lock and svc._builds is not sem and svc._inflight == {}


# ------------------------------------------------------------------ the routes

@pytest.fixture
def winds(monkeypatch, clock):
    """Wind stations ON over the fake providers and a fake history service (nothing reaches the network)."""
    provs, fetch = make_fakes(clock.time())
    hfetch = history_fetch()
    svc = W.WindHistory(STATIONS, hfetch, now=Clock(first_t(fx("realtime2_51003.txt", "r")) + 60))
    doc = {"captured": "2026-10-10T00:00:00Z", "source": "test", "fields": ["id", "name", "lat", "lon", "kind", "src", "tz", "alias", "wx"],
           "stations": [[s.get(k) for k in ("id", "name", "lat", "lon", "kind", "src", "tz", "alias", "wx")] for s in STATIONS.values()]}
    monkeypatch.setattr(A, "WIND_ENABLED", True)
    monkeypatch.setattr(A, "_WINDS", {"svc": svc, "list": A._json_payload_and_etag(W.client_list(doc)),
                                      "providers": provs, "stations": dict(STATIONS)})
    monkeypatch.setattr(A, "_WIND_MEMO", {"key": None, "payload": None, "etag": None, "built_ts": None, "build_s": None})
    monkeypatch.setattr(A, "_WIND_BG", {"ticks": 0, "prebuilds": 0, "errors": 0, "last_error": None, "last_tick_ts": None})
    return provs, fetch, hfetch, A.app.test_client()


def test_route_station_list_is_revalidated_on_every_load(winds):
    _, _, _, c = winds
    r = c.get("/api/wind/stations")
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, no-cache"
    assert r.headers["CDN-Cache-Control"] == "no-store" and len(r.get_json()["stations"]) == len(STATIONS)
    r2 = c.get("/api/wind/stations", headers={"If-None-Match": r.headers["ETag"]})
    assert r2.status_code == 304 and r2.headers["Cache-Control"] == "public, no-cache"
    assert r2.headers["CDN-Cache-Control"] == "no-store" and r2.headers["ETag"] == r.headers["ETag"]


def test_route_latest_inline_path_builds_once(winds):
    provs, fetch, _, c = winds
    r = c.get("/api/wind/latest")
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=120"
    assert r.headers["CDN-Cache-Control"] == "no-store" and "X-Wind-Stations-Partial" not in r.headers
    d = r.get_json()
    ids = {row[0] for row in d["rows"]}
    assert {"coops:1612340", "ndbc:51003", "ndbc:HRRH1", "metar:PHNL", "metar:KSFO"} <= ids
    assert "coops:1611400" in d["missing"] and "coops:1612401" in d["missing"] and "metar:KGMU" in d["missing"]
    n = len(fetch.calls)
    r2 = c.get("/api/wind/latest")
    assert r2.headers["ETag"] == r.headers["ETag"] and len(fetch.calls) == n   # lists fresh: no fetch, memo hit


def test_route_history_contract(winds):
    _, _, hfetch, c = winds
    r = c.get("/api/wind/coops:9999999/history")
    assert r.status_code == 404 and r.headers["Cache-Control"] == "no-store" and hfetch.calls == []
    assert c.get("/api/wind/ndbc:51201/history").status_code == 404        # NDBC serves it; not in the snapshot
    assert c.get("/api/wind/bad%20id/history").status_code == 404
    r = c.get("/api/wind/ndbc:51003/history")
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=60" and r.get_json()["src"] == "ndbc"   # G28 B-3
    hfetch.table[W.coops_url("1612401", range=24)] = IOError("down")
    r = c.get("/api/wind/coops:1612401/history")
    assert r.status_code == 503 and r.headers["Retry-After"] == "5" and r.get_json()["retry"] is True
    assert r.headers["Cache-Control"] == "no-store"


def test_routes_off_say_so(winds, monkeypatch):
    _, _, _, c = winds
    monkeypatch.setattr(A, "WIND_ENABLED", False)
    for u in ("/api/wind/stations", "/api/wind/latest", "/api/wind/ndbc:51003/history"):
        r = c.get(u)
        assert r.status_code == 503 and r.get_json()["error"] == A.WIND_OFF and r.headers["Cache-Control"] == "no-store"
    assert A.get_wind_providers() == [] and c.get("/healthz").get_json()["wind"] == {"enabled": False}


def test_routes_without_the_snapshot_say_so(monkeypatch):
    monkeypatch.setattr(A, "WIND_ENABLED", True)
    monkeypatch.setattr(A, "WIND_STATIONS_PATH", os.path.join(HERE, "nope.json"))
    monkeypatch.setattr(A, "_WINDS", {"svc": None, "list": None, "providers": None, "stations": None})
    c = A.app.test_client()
    assert c.get("/api/wind/latest").status_code == 503 and A.get_wind_providers() == []
    assert c.get("/healthz").get_json()["wind"]["error"]


# ------------------------------------------------------------------ the scheduler and /healthz

class RecordingRunner:
    def __init__(self):
        self.jobs, self.queued, self.running, self.waiting, self.workers = [], 0, 0, 0, 0

    def submit(self, fn, name="x"):
        self.jobs.append((name, fn))

    def run(self, n=None):
        jobs, self.jobs = (self.jobs[:n], self.jobs[n:]) if n else (self.jobs, [])
        for _, fn in jobs:
            fn()
        return [name for name, _ in jobs]


@pytest.fixture
def bg(winds, monkeypatch):
    """The background service ON with fake buoy providers beside the fake wind providers, a recording runner."""
    bprovs = F.make_providers()
    monkeypatch.setattr(A, "get_buoy_providers", lambda: bprovs)
    monkeypatch.setattr(A, "_buoy_tz_cached", F.fake_tz)
    monkeypatch.setattr(A, "_LIVE_STATIONS_MEMO", {"key": None, "payload": None, "etag": None})
    monkeypatch.setattr(A, "_LIVE_BG", {"started": False, "thread": None, "started_ts": None, "warm_ts": None, "ticks": 0,
                                        "last_tick_ts": None, "last_tick_s": None, "prebuilds": 0, "errors": 0,
                                        "last_error": None, "tz_loaded": False, "build_failures": 0})
    monkeypatch.setattr(A, "LIVE_BACKGROUND", True)
    monkeypatch.setattr(A, "start_live_background", lambda: True)
    rec = RecordingRunner()
    prev = B.set_refresh_runner(rec)
    prev_w = W.set_wind_runner(rec)              # the wind feeds' own runner (G28 A-5), recorded in the same list here
    yield bprovs, winds[0], rec, winds[3]
    B.set_refresh_runner(prev)
    W.set_wind_runner(prev_w)


def test_scheduler_pass_queues_the_wind_feeds_after_the_buoys_and_prebuilds(bg):
    bprovs, wprovs, rec, c = bg
    out = A._live_tick(bprovs)
    names = [n for n, _ in rec.jobs]
    assert names[:len(bprovs)] == ["buoy-refresh-%s" % p.source for p in A._live_providers_ordered(bprovs)]
    assert names[len(bprovs):] == ["buoy-refresh-NDBC", "buoy-refresh-METAR", "buoy-refresh-COOPS", "buoy-refresh-NWS"]
    assert out["warm"] is False and A._WIND_BG["ticks"] == 1 and A._WIND_BG["prebuilds"] == 1   # an empty table
    rec.run()
    A._live_tick(bprovs)
    assert A._WIND_BG["prebuilds"] == 2 and A._WIND_MEMO["key"] == (("NDBC", 1), ("METAR", 1), ("COOPS", 1), ("NWS", 1))
    assert A._live_tick(bprovs)["built"] is False and A._WIND_BG["prebuilds"] == 2   # nothing new: no build


def test_a_failing_wind_pass_never_breaks_the_buoy_pass(bg, monkeypatch):
    bprovs, _, rec, _ = bg
    monkeypatch.setattr(A, "_wind_tick", lambda providers=None: 1 / 0)
    out = A._live_tick(bprovs)
    assert out["built"] is True and A._LIVE_BG["ticks"] == 1 and A._LIVE_BG["errors"] == 0
    assert A._WIND_BG["errors"] == 1 and A._WIND_BG["last_error"].startswith("tick:")


def test_route_latest_with_the_service_on_never_waits(bg):
    bprovs, wprovs, rec, c = bg
    r = c.get("/api/wind/latest")
    assert r.status_code == 200 and r.get_json()["rows"] == [] and r.headers["Cache-Control"] == "no-store"
    assert r.headers["X-Wind-Stations-Partial"] == "NDBC,METAR,COOPS,NWS"
    assert [n for n, _ in rec.jobs] == ["buoy-refresh-NDBC", "buoy-refresh-METAR", "buoy-refresh-COOPS", "buoy-refresh-NWS"]  # cold: queued
    rec.run(2)                                                               # NDBC and METAR published
    A._live_tick(bprovs)                                                     # the scheduler builds them
    r = c.get("/api/wind/latest")
    assert r.headers["X-Wind-Stations-Partial"] == "COOPS,NWS" and r.headers["Cache-Control"] == "no-store"
    d = r.get_json()
    rows = {row[0]: row for row in d["rows"]}
    assert "ndbc:51003" in rows and "metar:PHNL" in rows
    assert "coops:1612340" in rows and "coops:1611400" not in rows           # Honolulu from its NDBC relay meanwhile
    relay = rows["coops:1612340"]
    assert not any(s.startswith("coops:") for s in d["missing"])              # CO-OPS has no list yet: not "missing"
    rec.run()
    A._LIVE_WAKE.clear()
    r = c.get("/api/wind/latest")                                            # COOPS + NWS published, not built yet: old table
    assert r.headers["X-Wind-Stations-Partial"] == "COOPS,NWS" and A._LIVE_WAKE.is_set()   # ... and the scheduler is woken
    A._live_tick(bprovs)
    r = c.get("/api/wind/latest")
    assert "X-Wind-Stations-Partial" not in r.headers and r.headers["Cache-Control"] == "public, max-age=120"
    d = r.get_json()
    assert "nws:001HE" in {row[0] for row in d["rows"]} and "nws:029HI" in d["missing"]
    own = next(row for row in d["rows"] if row[0] == "coops:1612340")
    latest = W.parse_coops_wind(json.loads(fx("coops_latest.json")))[0]
    assert own != relay and own[1:] == [latest["t"], latest["s"], latest["g"], latest["d"]]   # CO-OPS's own reading now
    assert "coops:1611400" in d["missing"] and "coops:1612401" in d["missing"]


def test_healthz_reports_the_wind_feeds_without_touching_warm(bg):
    bprovs, wprovs, rec, c = bg
    rec.run()                                                                # nothing queued yet: no-op
    A._live_tick(bprovs)
    rec.run(len(bprovs))                                                     # the buoy feeds only
    A._live_tick(bprovs)
    body = c.get("/healthz").get_json()
    assert body["warm"] is True and body["missing"] == [] and body["ok"] is True   # the wind feeds are still cold ...
    w = body["wind"]
    assert w["enabled"] is True and w["missing"] == ["NDBC", "METAR", "COOPS", "NWS"] and w["memo"]["complete"] is False
    assert [s["source"] for s in w["providers"]] == ["NDBC", "METAR", "COOPS", "NWS"] and w["ticks"] == 2
    rec.run()
    A._live_tick(bprovs)
    w = c.get("/healthz").get_json()["wind"]
    assert w["missing"] == [] and w["memo"]["complete"] is True and w["memo"]["current"] is True and w["memo"]["bytes"] > 100
    assert w["history_entries"] == 0 and w["errors"] == 0


# ------------------------------------------------------------------ the MADIS backup and the stall note (plan section 52 D)

MADIS_T = calendar.timegm((2026, 10, 24, 21, 10, 0))                      # 001HE's report in the fixture


@pytest.fixture
def madis_clock(monkeypatch):
    t = F.FrozenTime(MADIS_T + 300)
    monkeypatch.setattr(B, "time", t)
    return t


def madis_fetch(body=None):
    return FakeFetch({W.MADIS_HI_URL: fx("madis_hi.html") if body is None else body})


def test_parse_madis_csv_the_real_answer():
    """The viewer's answer as captured (tests/capture_wind_fixtures.py cuts it): K -> C, the altimeter in hPa, missing
    values left out, the newest of a station's records, ids upper case."""
    o = W.parse_madis_csv(fx("madis_hi.html").decode("latin-1"))
    assert sorted(o) == ["001HE", "016HE", "029HI", "PHJR", "PHNL"]
    assert o["001HE"] == {"t": MADIS_T, "s": 6.5, "g": 9.7, "d": 25, "at": 22.1, "dp": 20.9, "rh": 93}, "no altimeter: no p"
    assert o["029HI"] == {"t": MADIS_T - 600, "s": 1.0, "g": 2.6, "d": 225, "at": 24.7, "dp": 21.7, "rh": 83, "p": 1016.5}
    assert o["PHJR"]["t"] == MADIS_T + 300 and o["PHJR"]["d"] == 40, "listed twice: the newer record (the older has no direction)"
    assert o["PHNL"]["g"] is None, "a gust of -99999 is none"


MADIS_HEAD = (" STAID     ,OBDATE    ,OBTIME,PVDR     ,SUBPVDR    ,TD           ,D,RH           ,D,T            ,D,DD"
              "           ,D,FF           ,D,FFGUST       ,D,ALTSE        ,D,\n")


def madis_line(sid, ff="5.0", ffq="S", dd="90.0", ddq="S", t="295.15", tq="S", time="21:10"):
    return (" %s ,10/24/2026,%s,MesoWest  ,HECO       ,   290.15,S, 70.0,S, %s,%s, %s,%s, %s,%s, 7.0,S, 101500.0,S,\n"
            % (sid, time, t, tq, dd, ddq, ff, ffq))


def test_parse_madis_csv_quality_control_and_shapes():
    L = madis_line
    o = W.parse_madis_csv("<PRE>" + MADIS_HEAD + L("AAA01") + L("BBB01", ffq="X") + L("CCC01", ddq="B") + L("DDD01", tq="X")
                          + L("EEE01", ff="-99999.0", ffq="Z") + L("FFF01", ff="0.0", dd="270.0") + L("bad id!")
                          + L("GGG01", time="2x:10") + "</PRE>")
    assert sorted(o) == ["AAA01", "CCC01", "DDD01", "FFF01"], "a rejected or missing speed: no reading; a bad id or time: skipped"
    assert o["AAA01"] == {"t": MADIS_T, "s": 5.0, "g": 7.0, "d": 90, "at": 22.0, "dp": 17.0, "rh": 70, "p": 1015.0}
    assert o["CCC01"]["d"] is None and o["DDD01"].get("at") is None, "a rejected value (QC X or B) is left out"
    assert o["FFF01"]["s"] == 0.0 and o["FFF01"]["d"] is None, "calm: no direction"
    # a record 10+ minutes in the future is dropped (a bad clock upstream)
    assert W.parse_madis_csv("<PRE>" + MADIS_HEAD + L("AAA01") + "</PRE>", now=MADIS_T - 601) == {}
    assert "AAA01" in W.parse_madis_csv("<PRE>" + MADIS_HEAD + L("AAA01") + "</PRE>", now=MADIS_T - 599)
    # columns found by name: another order reads the same
    swapped = MADIS_HEAD.replace("FF           ,D,FFGUST", "FFGUST       ,D,FF")
    assert W.parse_madis_csv("<PRE>" + swapped + L("AAA01") + "</PRE>")["AAA01"]["s"] == 7.0
    # an error page / no header: a failed answer (keep-last)
    for bad in ("", "<html><body>Service unavailable</body></html>", " FOO,BAR\n AAA,1\n", " STAID,OBDATE,OBTIME\n"):
        with pytest.raises(ValueError):
            W.parse_madis_csv(bad)
    assert W.parse_madis_csv("<PRE>" + MADIS_HEAD + "</PRE>") == {}, "the header and no station: an empty answer, not a failure"


def test_madis_provider_relays_the_nws_stations_and_keeps_readings(madis_clock):
    p = W.MadisProvider(NWS_IDS, madis_fetch())
    lst, ver, _ = p.list_stations_versioned()
    assert [e["id"] for e in lst] == ["madis:001HE", "madis:029HI"], "only the snapshot's NWS stations (not 016HE, not the airports)"
    assert all(e["alias_of"] == "nws:" + e["id"].split(":")[1] for e in lst)
    assert lst[0]["wind"] == [6.5, 9.7, 25] and lst[0]["name"] == NWS_IDS["001HE"]["name"]
    assert p.row("001HE") == {"t": MADIS_T, "s": 6.5, "g": 9.7, "d": 25, "at": 22.1, "dp": 20.9, "rh": 93}
    assert p.row("nope") is None
    r = p.row("001HE")
    r["s"] = 99
    assert p.row("001HE")["s"] == 6.5, "a copy"
    # a later answer without 029HI keeps its reading; 6 hours on it is dropped
    p._fetch = madis_fetch(fx("madis_hi.html").replace(b" 029HI", b" XXXXX"))
    assert {e["local_id"] for e in p._fetch_stations()} == {"001HE", "029HI"}
    madis_clock.now = MADIS_T + W.MADIS_KEEP_S + 601
    assert p._fetch_stations() == []
    # an unreadable answer raises (the base class keeps the last list)
    p._fetch = madis_fetch(b"<html>Service unavailable</html>")
    with pytest.raises(ValueError):
        p._fetch_stations()
    assert p.source == "MADIS" and p.list_ttl_sec == W.MADIS_TTL_S == 300 and p.attribution_text.endswith(W.ATTRIBUTION["madis"])
    assert p.extra_keys == ("wind", "alias_of")


def test_madis_provider_newest_counts_and_a_windless_answer(madis_clock, caplog):
    """G35 F5: the feed's newest reading (two stations at different times: the newer is kept, D26's pattern), its counts
    in /healthz, and a warning when an answer has station records but not one wind reading."""
    p = W.MadisProvider(NWS_IDS, madis_fetch())
    assert p.newest_t() is None and p.status()["newest_age_s"] is None and p.status()["readings"] == 0
    with caplog.at_level("WARNING", logger=W.__name__):
        p._fetch_stations()
    assert not [r for r in caplog.records if r.name == W.__name__], "a good answer warns of nothing (re-check W3)"
    assert p.newest_t() == MADIS_T, "001HE at MADIS_T, 029HI 10 min before: the newer"
    st = p.status(MADIS_T + 300)
    assert st["readings"] == 2 and st["newest_age_s"] == 300 and st["last_rows"] >= 5 and st["last_wind"] == 5
    p._fetch = madis_fetch(("<PRE>" + MADIS_HEAD + madis_line("001HE", ff="-99999.0", ffq="Z")
                            + madis_line("029HI", ff="-99999.0", ffq="Z") + "</PRE>").encode())
    caplog.clear()
    with caplog.at_level("WARNING", logger=W.__name__):
        assert {e["local_id"] for e in p._fetch_stations()} == {"001HE", "029HI"}, "the kept readings stay"
    assert any("not one wind reading" in r.getMessage() for r in caplog.records)
    assert p.status()["last_rows"] == 2 and p.status()["last_wind"] == 0 and p.newest_t() == MADIS_T
    # the newest time is the newest EVER: an older answer later does not move it back
    p._fetch = madis_fetch(("<PRE>" + MADIS_HEAD + madis_line("029HI", time="21:05") + "</PRE>").encode())
    p._fetch_stations()
    assert p.newest_t() == MADIS_T


def test_madis_provider_warns_of_an_answer_with_no_station_records(madis_clock, caplog):
    """Re-check R1-2 (seen on the test site 2026-10-25 04:50 UTC, `last_rows 0`): the header and not one record. The
    kept readings stay, and the log says so (before: silent, no error, no warning)."""
    p = W.MadisProvider(NWS_IDS, madis_fetch())
    p._fetch_stations()
    p._fetch = madis_fetch(("<PRE>" + MADIS_HEAD + "</PRE>").encode())
    caplog.clear()
    with caplog.at_level("WARNING", logger=W.__name__):
        assert {e["local_id"] for e in p._fetch_stations()} == {"001HE", "029HI"}, "the kept readings stay"
    msgs = [r.getMessage() for r in caplog.records if r.name == W.__name__]
    assert msgs == ["MADIS: an answer with no station records (the kept readings stay)"], msgs
    assert p.status()["last_rows"] == 0 and p.status()["readings"] == 2


def test_madis_provider_drops_a_report_from_the_future(madis_clock):
    """G35 D27: the provider passes its clock to the parser (a record 10+ minutes ahead of it is a bad clock)."""
    madis_clock.now = MADIS_T - 601
    p = W.MadisProvider(NWS_IDS, madis_fetch())
    assert {e["local_id"] for e in p._fetch_stations()} == {"029HI"}, "001HE's report lies 601 s ahead"


# ------------------------------------------------------------------ 24 hours of MADIS readings (plan section 55)

LIVE_T = calendar.timegm((2026, 10, 26, 21, 48, 0))          # madis_live.html: every record received in the hour before
WINDOW_END = calendar.timegm((2026, 10, 26, 9, 0, 0))        # madis_window.html: the records observed in (07:00, 09:00]


@pytest.fixture
def live_clock(monkeypatch):
    t = F.FrozenTime(LIVE_T)
    monkeypatch.setattr(B, "time", t)
    return t


def test_parse_madis_rows_every_record_of_a_station():
    """The live poll's answer (receipt time, every record): a station's records ascending, one per time; the newest
    view (parse_madis_csv) is the last of them."""
    st = {}
    o = W.parse_madis_rows(fx("madis_live.html").decode("latin-1"), LIVE_T, st)
    assert sorted(o) == ["001HE", "011HI", "016HE", "029HI", "AU956", "PHNL"] and st == {"rows": 49, "wind": 6}
    t = [r["t"] for r in o["001HE"]]
    assert t == [LIVE_T - 3480 + 600 * i for i in range(5)], "20:50 .. 21:30 UTC, 10 minutes apart"
    assert o["001HE"][-1] == {"t": LIVE_T - 1080, "s": 2.6, "g": 5.5, "d": 44, "at": 22.5, "dp": 20.3, "rh": 88}
    assert o["AU956"][-1]["s"] == 0.0 and o["AU956"][-1]["d"] is None, "calm: no direction"
    assert W.parse_madis_csv(fx("madis_live.html").decode("latin-1"), LIVE_T) == {k: v[-1] for k, v in o.items()}
    w = W.parse_madis_rows(fx("madis_window.html").decode("latin-1"))
    assert [len(w[k]) for k in ("001HE", "029HI", "AU956")] == [12, 24, 18]
    assert w["001HE"][0]["t"] == WINDOW_END - 6600 and w["001HE"][-1]["t"] == WINDOW_END, "(end - 2 h, end]"
    # a station's two lines of one time: the later line (a corrected report); out of order lines come back sorted
    L = madis_line
    o = W.parse_madis_rows("<PRE>" + MADIS_HEAD + L("AAA01", time="21:20") + L("AAA01", time="21:10", ff="3.0")
                           + L("AAA01", time="21:10", ff="4.0") + "</PRE>")
    assert [(r["t"], r["s"]) for r in o["AAA01"]] == [(MADIS_T, 4.0), (MADIS_T + 600, 5.0)]


def test_madis_window_url_keeps_the_query_and_asks_by_observation_time():
    u = W.madis_window_url(W.MADIS_HI_URL, WINDOW_END)
    base, q = u.split("?", 1)
    assert base == W.MADIS_HI_URL.split("?", 1)[0]
    pairs = [p.split("=", 1) for p in q.split("&")]
    d = dict(pairs)
    assert len(d) == len(pairs), "each parameter once"
    assert (d["time"], d["minbck"], d["minfwd"], d["recwin"], d["timefilter"]) == ("20261026_0900", "-119", "0", "4", "0")
    live = dict(p.split("=", 1) for p in W.MADIS_HI_URL.split("?", 1)[1].split("&"))
    assert {k: v for k, v in d.items() if k not in ("time", "minbck", "minfwd", "recwin", "timefilter")} == \
        {k: v for k, v in live.items() if k not in ("time", "minbck", "minfwd", "recwin", "timefilter")}, "state, variables, CSV form"
    assert (live["time"], live["recwin"], live["timefilter"]) == ("0", "4", "1"), "the live poll: every record by receipt time"
    # across 00Z: the date moves with the hour
    assert "time=20261027_0000" in W.madis_window_url(W.MADIS_HI_URL, calendar.timegm((2026, 10, 27, 0, 0, 0)))
    assert "time=20261026_2200" in W.madis_window_url(W.MADIS_HI_URL, calendar.timegm((2026, 10, 26, 22, 0, 0)))
    # a url without those parameters gets them
    assert W.madis_window_url("https://madis.test/x?state=HI", WINDOW_END) == \
        "https://madis.test/x?state=HI&time=20261026_0900&minbck=-119&minfwd=0&recwin=4&timefilter=0"


def test_madis_series_keeps_readings_in_order():
    ser = W.MadisSeries()
    assert ser.add({"t": 200, "s": 2.0, "g": None, "d": 90, "at": 20.5, "rh": 70}) is True
    assert ser.add({"t": 100, "s": 1.0, "g": 1.5, "d": None}) is True
    assert ser.add({"t": 300, "s": 3.0, "g": None, "d": 10, "p": 1013.2}) is True
    assert ser.add({"t": 200, "s": 2.5, "g": 3.0, "d": 95}) is False, "a time already held: replaced"
    assert len(ser) == 3 and list(ser.t) == [100.0, 200.0, 300.0]
    assert ser.rows(0) == [{"t": 100, "s": 1.0, "g": 1.5, "d": None},
                           {"t": 200, "s": 2.5, "g": 3.0, "d": 95},
                           {"t": 300, "s": 3.0, "g": None, "d": 10, "p": 1013.2}]
    assert isinstance(ser.rows(300)[0]["d"], int) and isinstance(ser.rows(300)[0]["t"], int)
    assert [r["t"] for r in ser.rows(150)] == [200, 300]
    assert ser.prune(250) == 2 and ser.rows(0) == [{"t": 300, "s": 3.0, "g": None, "d": 10, "p": 1013.2}]
    assert len(ser.v) == len(ser.t) * len(W.MADIS_HIST_FIELDS)
    r = {"t": 400, "s": 1.0, "g": None, "d": None, "rh": 55}
    ser.add(r)
    assert ser.rows(400)[0] == r and isinstance(ser.rows(400)[0]["rh"], int)


def madis_hist_fetch(windows=None):
    """The live poll's answer and, per window end (epoch), the window's answer (default: madis_window.html for every
    window)."""
    table = {W.MADIS_HI_URL: fx("madis_live.html")}
    top = LIVE_T // 3600 * 3600
    for k in range(W.MADIS_BACKFILL_WINDOWS + 2):
        end = top - k * W.MADIS_WINDOW_S
        body = (windows or {}).get(end, fx("madis_window.html") if end == WINDOW_END else ("<PRE>" + MADIS_HEAD + "</PRE>").encode())
        table[W.madis_window_url(W.MADIS_HI_URL, end)] = body
    return FakeFetch(table)


def window_end_of(url):
    q = dict(p.split("=", 1) for p in url.split("?", 1)[1].split("&"))
    return calendar.timegm(time_strptime(q["time"]))


def time_strptime(s):
    import datetime as _dt
    return _dt.datetime.strptime(s, "%Y%m%d_%H%M").timetuple()


def test_madis_backfill_order_one_request_a_refresh_and_the_ttl(live_clock):
    """After a start: the live poll first, then the 12 two-hour windows of the 24 hours before it, newest first, one a
    refresh, each refresh due 30 s after the last; the live poll keeps its turn every 270 s; then back to 5 minutes."""
    fetch = madis_hist_fetch()
    p = W.MadisProvider(NWS_IDS, fetch, backfill=True)
    assert p.filling() is True and p.status()["backfill"] == "0 of 0", "nothing asked yet: filling"
    assert p.refresh_due()
    p.refresh()
    assert [u for u, _, _ in fetch.calls] == [W.MADIS_HI_URL]
    top = LIVE_T // 3600 * 3600
    assert [w[0] for w in p._bf] == [top - 7200 * k for k in range(12)], "21:00 .. 23:00 the day before, newest first"
    assert p._bf[-1][0] > LIVE_T - W.HISTORY_S and p._bf[-1][0] - 7200 < LIVE_T - W.HISTORY_S, "the 24 hours, no more"
    assert p.status()["backfill"] == "0 of 12" and p.filling()
    live_clock.now = LIVE_T + 29
    assert not p.refresh_due()
    asked = []
    for i in range(20):
        live_clock.now = LIVE_T + 30 * (i + 1)
        assert p.refresh_due(), i
        n = len(fetch.calls)
        p.schedule_refresh()                              # the scheduler's path (inline in the tests)
        assert len(fetch.calls) == n + 1, "one request a refresh"
        asked.append(fetch.calls[-1][0])
        if not p._bf:
            break
    windows = [window_end_of(u) for u in asked if u != W.MADIS_HI_URL]
    assert windows == [top - 7200 * k for k in range(12)], "every window once, newest first"
    lives = [i for i, u in enumerate(asked) if u == W.MADIS_HI_URL]
    assert lives == [8], "the live poll's turn 270 s after the last (the 9th refresh, 270 s on)"
    assert p.status()["backfill"] == "done" and p.status()["backfill_left"] == 0 and not p.filling()
    live_clock.now += 30
    assert not p.refresh_due(), "done: the live poll's 5 minutes again"
    live_clock.now += W.MADIS_TTL_S
    assert p.refresh_due()
    assert p.list_ttl_sec == W.MADIS_TTL_S
    # the history: the live readings and the window's, snapshot stations only
    assert [r["t"] for r in p.hist("001HE", 0)][:12] == [WINDOW_END - 6600 + 600 * i for i in range(12)]
    assert len(p.hist("001HE", 0)) == 17 and len(p.hist("029HI", 0)) == 33
    assert p.hist("016HE", 0) == [] and p.hist("PHNL", 0) == [], "not the snapshot's NWS stations: not kept"
    st = p.status(LIVE_T + 600)
    assert st["hist_stations"] == 2 and st["hist_rows"] == 50 and st["hist_oldest_age_s"] == LIVE_T + 600 - (WINDOW_END - 6900)


def test_madis_backfill_failures_are_retried_then_given_up(live_clock, caplog):
    top = LIVE_T // 3600 * 3600
    fetch = madis_hist_fetch({top - 7200: IOError("timeout")})
    p = W.MadisProvider(NWS_IDS, fetch, backfill=True)
    p.refresh()
    with caplog.at_level("INFO", logger=W.__name__):
        for i in range(1, 40):
            live_clock.now = LIVE_T + 30 * i
            p.schedule_refresh()
            if not p._bf:
                break
    asked = [window_end_of(u) for u, _, _ in fetch.calls if u != W.MADIS_HI_URL]
    assert asked.count(top - 7200) == W.MADIS_BACKFILL_TRIES == 3 and asked[:4] == [top, top - 7200, top - 7200, top - 7200]
    st = p.status()
    assert st["backfill"] == "done" and st["backfill_gave_up"] == 1
    assert any("given up after 3 tries" in r.getMessage() for r in caplog.records if r.levelname == "WARNING")
    assert p.list_stations_versioned()[0], "a failed window never empties the list"
    assert p._last_error is None, "a failed window is not a failed refresh"


def test_madis_live_failure_keeps_the_list_and_the_backfill_goes_on(live_clock):
    fetch = madis_hist_fetch()
    p = W.MadisProvider(NWS_IDS, fetch, backfill=True)
    p.refresh()
    v = p.list_stations_versioned()[1]
    fetch.table[W.MADIS_HI_URL] = IOError("down")
    live_clock.now = LIVE_T + 300
    p.schedule_refresh()                                  # the live poll's turn: it fails, the list is kept
    assert p._last_error and p.list_stations_versioned()[1] == v
    live_clock.now += 30
    assert p.refresh_due(), "the windows go on 30 s later (the live poll waits for its retry)"
    n = len(fetch.calls)
    p.schedule_refresh()
    assert fetch.calls[n][0] != W.MADIS_HI_URL and p._last_error is None


def test_madis_gap_between_live_polls_is_asked_again(live_clock):
    """Live polls further apart than MADIS_GAP_S (failures, a stalled runner): the hours between them are queued."""
    fetch = madis_hist_fetch()
    p = W.MadisProvider(NWS_IDS, fetch, backfill=True)
    p.refresh()
    p._bf = []                                            # the start's back-fill done
    live_clock.now = LIVE_T + W.MADIS_GAP_S - 60
    p._live_tried = LIVE_T - 600                          # the live poll's turn
    p._fetch_stations()
    assert p._bf == [], "a poll within 50 minutes of the last: no gap"
    live_clock.now = LIVE_T + 4 * 3600 + 120
    p._live_tried = LIVE_T                                # the polls failed for hours: the last one that answered
    p._live_ok = LIVE_T + W.MADIS_GAP_S - 60
    p._fetch_stations()
    top = (LIVE_T + 4 * 3600 + 120) // 3600 * 3600
    assert [w[0] for w in p._bf] == [top, top - 7200], "the hours since the last good poll (an hour's margin)"
    assert p.filling()


def test_madis_gap_from_two_real_live_polls(live_clock):
    """The same without setting the state by hand: a live poll that answers, then (the windows done) polls that fail
    for 3 hours, then one that answers: the hours since the first are queued."""
    fetch = madis_hist_fetch()
    p = W.MadisProvider(NWS_IDS, fetch, backfill=True)
    p._fetch_stations()                                   # answers at LIVE_T
    assert p._live_ok == LIVE_T
    p._bf = []
    fetch.table[W.MADIS_HI_URL] = IOError("down")
    for k in range(1, 4):
        live_clock.now = LIVE_T + 3600 * k
        with pytest.raises(IOError):
            p._fetch_stations()
    assert p._bf == [] and p._live_ok == LIVE_T, "a failed poll moves nothing"
    # an answer whose every wind field is missing (seen 2026-10-24 and 2026-10-26): no answer for the gap rule either
    fetch.table[W.MADIS_HI_URL] = ("<PRE>" + MADIS_HEAD + madis_line("001HE", ff="-99999.0", ffq="Z") + "</PRE>").encode()
    live_clock.now = LIVE_T + 3 * 3600 + 200
    p._fetch_stations()
    assert p._bf == [] and p._live_ok == LIVE_T
    fetch.table[W.MADIS_HI_URL] = fx("madis_live.html")
    live_clock.now = LIVE_T + 3 * 3600 + 300
    p._fetch_stations()
    top = (LIVE_T + 3 * 3600 + 300) // 3600 * 3600
    assert [w[0] for w in p._bf] == [top, top - 7200] and p._live_ok == live_clock.now


def test_madis_history_prunes_after_25_hours_and_keeps_only_snapshot_stations(live_clock):
    fetch = madis_hist_fetch()
    p = W.MadisProvider(NWS_IDS, fetch, backfill=True)
    p.refresh()
    p._take(W.parse_madis_rows(fx("madis_window.html").decode("latin-1")), LIVE_T)
    assert len(p.hist("001HE", 0)) == 17
    live_clock.now = WINDOW_END + W.MADIS_HIST_S - 1800      # the window's first hour past the cut
    p._prune(live_clock.now)
    assert [r["t"] for r in p.hist("001HE", 0)][0] == WINDOW_END - 1800, "older than 25 h: dropped"
    live_clock.now = LIVE_T + W.MADIS_HIST_S + 1
    p._prune(live_clock.now)
    assert p.hist("001HE", 0) == [] and p.status()["hist_stations"] == 0 and p.status()["hist_oldest_age_s"] is None
    # a reading older than the cut never enters
    assert p._take({"001HE": [{"t": LIVE_T, "s": 1.0, "g": None, "d": 10}]}, live_clock.now) == 0
    assert p.hist("001HE", 0) == []


def test_madis_windows_use_their_own_fetch(live_clock):
    """Step 2 V2-1: the windows go through window_fetch (the app's has a longer read timeout: MADIS answers a window
    only when its query is done), the live poll through fetch; without window_fetch both use fetch."""
    live_fetch, win_fetch = madis_hist_fetch(), madis_hist_fetch()
    p = W.MadisProvider(NWS_IDS, live_fetch, backfill=True, window_fetch=win_fetch)
    p._fetch_stations()
    for i in range(3):
        live_clock.now = LIVE_T + 30 * (i + 1)
        p._fetch_stations()
    assert [u for u, _, _ in live_fetch.calls] == [W.MADIS_HI_URL], "the live poll: fetch"
    assert len(win_fetch.calls) == 3 and all(u != W.MADIS_HI_URL for u, _, _ in win_fetch.calls), "the windows: window_fetch"
    assert all(h == W.HEADERS and n == W.MADIS_MAX for _, n, h in win_fetch.calls)
    one = madis_hist_fetch()
    q = W.MadisProvider(NWS_IDS, one, backfill=True)
    q._fetch_stations()
    live_clock.now += 30
    q._fetch_stations()
    assert [u == W.MADIS_HI_URL for u, _, _ in one.calls] == [True, False], "no window_fetch: the one fetch for both"


def test_madis_without_backfill_is_the_live_poll_alone(live_clock):
    """The provider's default (the tests above section 55 build it so): every refresh is the live poll, nothing is
    queued, filling() is False; the history still keeps what the polls bring."""
    fetch = madis_hist_fetch()
    p = W.MadisProvider(NWS_IDS, fetch)
    for i in range(3):
        live_clock.now = LIVE_T + 30 * i
        p._fetch_stations()
    assert [u for u, _, _ in fetch.calls] == [W.MADIS_HI_URL] * 3 and p._bf == [] and p.filling() is False
    assert p.status()["backfill"] is None and len(p.hist("001HE", 0)) == 5


def test_madis_history_memory_stays_small():
    """~300 stations x 288 five-minute readings (more than MADIS brings: ~48k): well under 12 MB."""
    import tracemalloc
    tracemalloc.start()
    base = tracemalloc.get_traced_memory()[0]
    store = {}
    for k in range(300):
        ser = store[k] = W.MadisSeries()
        for i in range(288):
            ser.add({"t": 1_793_000_000 + 300 * i, "s": 3.4, "g": 5.1, "d": 90, "at": 24.1, "dp": 19.0, "rh": 73, "p": 1014.2})
    used = tracemalloc.get_traced_memory()[0] - base
    tracemalloc.stop()
    assert sum(len(s) for s in store.values()) == 86_400
    assert used < 8 * 1024 * 1024, used


def madis_hist_svc(fetch, now, latest, hist, filling=False):
    svc = madis_svc(fetch, now, latest)
    svc.madis_hist = lambda local, since: [dict(r) for r in hist.get(local, []) if r["t"] >= since]
    svc.madis_filling = lambda: filling
    return svc


def test_history_merges_the_madis_hours_with_the_nws_api():
    fetch = history_fetch()
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = b'{"@graph": []}'
    w = W.parse_madis_rows(fx("madis_window.html").decode("latin-1"))
    live = W.parse_madis_rows(fx("madis_live.html").decode("latin-1"))
    hist = {"001HE": w["001HE"] + live["001HE"]}
    latest = {"001HE": live["001HE"][-1]}
    status, p = madis_hist_svc(fetch, LIVE_T, latest, hist).history("nws:001HE")
    assert status == "ok" and p["via"] == "madis" and p["source"] == W.ATTRIBUTION["madis"] and p["note"] is None
    assert p["t"] == [r["t"] for r in hist["001HE"]] and len(p["t"]) == 17, "MADIS's 24 hours, the latest once"
    assert p["at"][0] == w["001HE"][0]["at"] and "filling" not in p
    assert p["disclaimer"] == W.MESONET_DISCLAIMER
    # the NWS API's rows join: one row per time, the API's where both have one
    rows = W.parse_nws_observations(json.loads(fx("nws_history.json")))
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = fx("nws_history.json")
    newest = max(r["t"] for r in rows)
    both = dict(rows[0], s=99.0)
    hist2 = {"001HE": [both, dict(rows[0], t=newest + 300, s=1.2)]}
    p = madis_hist_svc(fetch, newest + 900, {}, hist2).history("nws:001HE")[1]
    assert p["via"] == "nws+madis" and p["t"] == sorted({r["t"] for r in rows} | {newest + 300})
    assert p["s"][p["t"].index(rows[0]["t"])] == rows[0]["s"] != 99.0, "a time both have: the NWS API's reading"
    assert p["source"] == W.ATTRIBUTION["nws+madis"] and "latest reading" not in p["source"]
    # the cap: the newest HISTORY_ROWS
    many = {"001HE": [{"t": LIVE_T - 60 * i, "s": 1.0, "g": None, "d": 10} for i in range(400)]}
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = b'{"@graph": []}'
    p = madis_hist_svc(fetch, LIVE_T, {}, many).history("nws:001HE")[1]
    assert len(p["t"]) == W.HISTORY_ROWS and p["t"][-1] == LIVE_T


def test_history_while_madis_is_filling_says_so_and_is_asked_again_in_a_minute():
    fetch = history_fetch()
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = b'{"@graph": []}'
    live = W.parse_madis_rows(fx("madis_live.html").decode("latin-1"))
    svc = madis_hist_svc(fetch, LIVE_T, {}, {"001HE": live["001HE"]}, filling=True)
    status, p = svc.history("nws:001HE")
    assert status == "ok" and p["filling"] is True and p["via"] == "madis"
    assert svc._cache[("h", "nws:001HE")][0] == LIVE_T + svc.error_ttl, "cached a minute (then the hours MADIS added)"
    svc.madis_filling = lambda: False
    svc._cache.clear()
    svc.history("nws:001HE")
    assert svc._cache[("h", "nws:001HE")][0] == LIVE_T + svc.ok_ttl
    # filling, nothing from MADIS yet and the NWS API down: the failure stands (no empty "ok")
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = IOError("timeout")
    svc = madis_hist_svc(fetch, LIVE_T, {}, {}, filling=True)
    assert svc.history("nws:001HE")[0] == "error"
    # other kinds never ask MADIS
    svc = madis_hist_svc(fetch, LIVE_T, {}, {"001HE": live["001HE"]}, filling=True)
    svc.madis_hist = lambda *a: (_ for _ in ()).throw(AssertionError("asked"))
    svc.history("metar:PHNL")


def test_make_providers_with_the_madis_backup():
    fetch, slow = FakeFetch(), FakeFetch()
    provs = W.make_providers(STATIONS, fetch)
    assert [p.source for p in provs] == ["NDBC", "METAR", "COOPS", "NWS"], "without a url: as before"
    provs = W.make_providers(STATIONS, fetch, madis_url="https://madis.test/x", madis_fetch=slow)
    m = provs[4]
    assert m.source == "MADIS" and m.url == "https://madis.test/x" and m._fetch is slow and sorted(m.stations) == sorted(NWS_IDS)
    assert m.backfill is True, "the app's MADIS back-fills the 24 hours (plan section 55)"
    assert m._wfetch is slow, "no window fetch given: the MADIS fetch"
    win = FakeFetch()
    assert W.make_providers(STATIONS, fetch, madis_url="u", madis_fetch=slow, madis_window_fetch=win)[4]._wfetch is win
    assert W.make_providers(STATIONS, fetch, madis_url="u")[4]._fetch is fetch
    assert W.make_providers(STATIONS, fetch, madis_url="u", madis_backfill=False)[4].backfill is False


def test_build_latest_madis_relay_fed_by_and_newest():
    def nws(local, t, s):
        return {"id": "nws:" + local, "wind": [s, None, 90], "latest_time": W.iso_z(t)}

    def madis(local, t, s):
        return {"id": "madis:" + local, "wind": [s, 1.0, 45], "latest_time": W.iso_z(t), "alias_of": "nws:" + local}
    lists = {"nws": [nws("001HE", MADIS_T - 7200, 3.0), nws("029HI", MADIS_T + 60, 4.0)],
             "madis": [madis("001HE", MADIS_T, 6.5), madis("029HI", MADIS_T, 1.0)]}
    d = W.build_latest(STATIONS, lists, MADIS_T + 300, {"nws": MADIS_T - 7200, "coops": None})
    rows = {r[0]: r for r in d["rows"]}
    assert rows["nws:001HE"] == ["nws:001HE", MADIS_T, 6.5, 1.0, 45], "the newer MADIS report wins"
    assert rows["nws:029HI"] == ["nws:029HI", MADIS_T + 60, 4.0, None, 90], "the NWS API's own when it is newer"
    assert d["fed_by"] == {"nws:001HE": "madis"} and d["newest"] == {"nws": MADIS_T - 7200}
    # the NWS feed with no list at all (a stall past its keep time): every station from MADIS
    d = W.build_latest(STATIONS, {"nws": [], "madis": lists["madis"]}, MADIS_T)
    assert d["fed_by"] == {"nws:001HE": "madis", "nws:029HI": "madis"} and d["newest"] == {} and "nws:001HE" not in d["missing"]
    # a gauge's NDBC relay is named too; of two relays of one station the newer wins, whatever the order
    nd = {"id": "ndbc:OOUH1", "wind": [2.0, 3.0, 10], "latest_time": W.iso_z(MADIS_T), "alias_of": "coops:1612340"}
    old = dict(nd, id="ndbc:OLD", latest_time=W.iso_z(MADIS_T - 60), wind=[9.0, 9.0, 9])
    for order in ([nd, old], [old, nd]):
        d = W.build_latest(STATIONS, {"coops": [], "ndbc": order}, MADIS_T)
        assert d["fed_by"] == {"coops:1612340": "ndbc"} and d["rows"][0][2] == 2.0
    assert W.build_latest(STATIONS, {}, 0)["fed_by"] == {} and W.build_latest(STATIONS, {}, 0)["newest"] == {}


def test_paced_feeds_remember_their_newest_reading(clock):
    fetch = FakeFetch(nws_table())
    p = W.NwsProvider(NWS_IDS, fetch, per_refresh=10)
    assert p.newest_t() is None and p.status()["newest_age_s"] is None
    newest = max(r["t"] for r in W.parse_nws_observations(json.loads(fx("nws_obs.json"))))
    clock.now = newest + 600
    p._fetch_stations()
    assert p.newest_t() == newest and p.status()["newest_age_s"] == 600
    # two stations at different times: the newer is the feed's newest (G35 D26: never the older)
    older = newest - 1800
    fetch.table[W.NWS_API + "/stations/029HI/observations?"] = json.dumps({"@graph": [{
        "timestamp": W.iso_z(older), "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 10.0, "qualityControl": "V"},
        "windDirection": {"unitCode": "wmoUnit:degree_(angle)", "value": 90.0, "qualityControl": "V"},
        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": None, "qualityControl": "Z"}}]}).encode()
    for order in (["029HI", "001HE"], ["001HE", "029HI"]):
        q = W.NwsProvider({k: NWS_IDS[k] for k in order}, fetch, per_refresh=10)
        q._fetch_stations()
        assert q.newest_t() == newest, order
    # the readings age out after keep_s; the newest time stays (the stall note's "since")
    clock.now = newest + W.NWS_KEEP_S + 10
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = b'{"@graph": []}'
    assert p._fetch_stations() == [] and p.newest_t() == newest


def madis_svc(fetch, now, rows):
    svc = W.WindHistory(STATIONS, fetch, now=Clock(now))
    svc.madis = lambda local: dict(rows[local]) if local in rows else None
    return svc


def test_history_nws_falls_back_on_madis():
    fetch = history_fetch()
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = b'{"@graph": []}'
    madis = {"001HE": {"t": MADIS_T, "s": 6.5, "g": 9.7, "d": 25, "at": 22.1, "dp": 20.9, "rh": 93}}
    assert W.WindHistory(STATIONS, fetch, now=Clock(MADIS_T + 300)).history("nws:001HE")[1]["note"] == W.NO_HISTORY, "no backup: as before"
    status, p = madis_svc(fetch, MADIS_T + 300, madis).history("nws:001HE")
    assert status == "ok" and p["via"] == "madis" and p["note"] is None and p["source"] == W.ATTRIBUTION["madis"]
    assert p["disclaimer"] == W.MESONET_DISCLAIMER, "mesonet observations: NOAA's disclaimer, whichever feed"
    assert p["t"] == [MADIS_T] and p["s"] == [6.5] and p["g"] == [9.7] and p["d"] == [25]
    assert p["at"] == [22.1] and p["rh"] == [93] and p["conditions"]["at"] == [MADIS_T, 22.1]
    # the NWS API with older rows: MADIS's newer report is added after them
    rows = W.parse_nws_observations(json.loads(fx("nws_history.json")))
    newest = max(r["t"] for r in rows)
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = fx("nws_history.json")
    madis["001HE"]["t"] = newest + 600
    p = madis_svc(fetch, newest + 900, madis).history("nws:001HE")[1]
    assert p["via"] == "nws+madis" and p["t"][-1] == newest + 600 and p["t"][:-1] == sorted(r["t"] for r in rows)
    assert p["source"] == W.ATTRIBUTION["nws+madis"] and p["source"].count("HECO") == 1, "the networks named once (G35 B-13)"
    assert p["disclaimer"] == W.MESONET_DISCLAIMER
    # MADIS older than the API's newest row: added in its place (plan section 55: one row per time); at a time the API
    # has: the API's row (MADIS adds nothing); older than 24 h: never
    madis["001HE"]["t"] = newest - 60
    p = madis_svc(fetch, newest + 900, madis).history("nws:001HE")[1]
    assert p["via"] == "nws+madis" and p["t"] == sorted([r["t"] for r in rows] + [newest - 60])
    madis["001HE"]["t"] = newest
    p = madis_svc(fetch, newest + 900, madis).history("nws:001HE")[1]
    i = p["t"].index(newest)
    assert p["via"] is None and p["s"][i] == next(r["s"] for r in rows if r["t"] == newest), "the NWS API's row wins a tie"
    assert p["disclaimer"] == W.MESONET_DISCLAIMER, "the NWS API's rows alone: the disclaimer too (W6)"
    p = W.WindHistory(STATIONS, fetch, now=Clock(newest + 900)).history("nws:001HE")[1]
    assert p["via"] is None and p["t"] and p["disclaimer"] == W.MESONET_DISCLAIMER, "no MADIS at all: the same"
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = b'{"@graph": []}'
    madis["001HE"]["t"] = MADIS_T - W.HISTORY_S - 1
    assert madis_svc(fetch, MADIS_T, madis).history("nws:001HE")[1]["note"] == W.NO_HISTORY
    # the API failing: MADIS answers when it has the station, else the failure stands
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = IOError("timeout")
    fetch.table[W.NWS_API + "/stations/029HI/observations?"] = IOError("timeout")
    madis["001HE"]["t"] = MADIS_T
    svc = madis_svc(fetch, MADIS_T + 60, madis)
    assert svc.history("nws:001HE")[1]["via"] == "madis"
    assert svc.history("nws:029HI")[0] == "error"
    # the NWS budget used up: busy (asked again in a moment), never MADIS's one row cached as ok (G35 D15)
    svc = madis_svc(fetch, MADIS_T + 60, madis)
    svc.nws_bucket = W.TokenBucket(rate=1, now=lambda: 0.0)
    assert svc.nws_bucket.take()
    assert svc.history("nws:001HE") == ("busy", {"error": W.BUSY})
    # the disclaimer is for mesonet observations only
    assert "disclaimer" not in madis_svc(history_fetch(), MADIS_T, {}).history("ndbc:51003")[1]


def test_app_wires_the_madis_backup(monkeypatch):
    assert A.WIND_WARM_ORDER == ["NDBC", "METAR", "MADIS", "COOPS", "NWS"]
    assert A.WIND_MADIS_URL == W.MADIS_HI_URL, "on by default (env WIND_MADIS_URL; empty = off)"
    calls = []

    class Resp:
        raw = None                                                         # (no socket to shut down)

        def __init__(self, status, chunks):
            self.status_code, self.chunks = status, chunks

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def iter_content(self, n):
            return iter(self.chunks)
    monkeypatch.setattr(A._POINT_HTTP, "get", lambda url, **k: calls.append((url, k)) or Resp(200, [b"ab", b"cd"]))
    assert A._madis_fetch("https://m.test/x", 10, {"User-Agent": "u"}) == b"abcd"
    assert calls[0][1]["timeout"] == A.WIND_MADIS_TIMEOUT == (10, 45) and calls[0][1]["headers"] == {"User-Agent": "u"}
    assert calls[0][1]["stream"] is True and A.WIND_MADIS_MAX_S == 90
    # the back-fill windows (plan section 55, step 2 V2-1): a longer read timeout and whole-answer limit
    calls.clear()
    assert A._madis_window_fetch("https://m.test/w", 10, {"User-Agent": "u"}) == b"abcd"
    assert calls[0][1]["timeout"] == A.WIND_MADIS_WINDOW_TIMEOUT == (10, 100) and A.WIND_MADIS_WINDOW_MAX_S == 120
    assert calls[0][1]["stream"] is True and calls[0][1]["headers"] == {"User-Agent": "u"}
    with pytest.raises(IOError, match="too large"):
        A._madis_fetch("u", 3, None)                                       # over the cap
    monkeypatch.setattr(A._POINT_HTTP, "get", lambda url, **k: Resp(503, []))
    with pytest.raises(IOError, match="HTTP 503"):
        A._madis_fetch("u", 10, None)

    class Slow(Resp):                                                      # G35 F3: an answer that trickles is cut at
        def iter_content(self, n):                                         # WIND_MADIS_MAX_S (it held a wind worker)
            for _ in range(50):
                _time.sleep(0.02)
                yield b"a"
    monkeypatch.setattr(A, "WIND_MADIS_MAX_S", 0.1)
    monkeypatch.setattr(A._POINT_HTTP, "get", lambda url, **k: Slow(200, []))
    t0 = _time.time()
    with pytest.raises(IOError, match="too slow"):
        A._madis_fetch("u", 1000, None)
    assert _time.time() - t0 < 0.8
    # _winds(): the provider built with the slow fetch, its rows handed to the history service
    monkeypatch.setattr(A, "_WINDS", {"svc": None, "list": None, "providers": None, "stations": None})
    w = A._winds()
    m = [p for p in w["providers"] if p.source == "MADIS"][0]
    assert m._fetch is A._madis_fetch and w["svc"].madis == m.row and m.url == A.WIND_MADIS_URL
    # plan section 55: the 24 hours and the filling flag handed over too; the provider back-fills
    assert w["svc"].madis_hist == m.hist and w["svc"].madis_filling == m.filling and m.backfill is True
    assert m._wfetch is A._madis_window_fetch, "the windows: their own fetch (step 2 V2-1)"
    monkeypatch.setattr(A, "WIND_MADIS_URL", "")
    monkeypatch.setattr(A, "_WINDS", {"svc": None, "list": None, "providers": None, "stations": None})
    w = A._winds()
    assert [p.source for p in w["providers"]] == ["NDBC", "METAR", "COOPS", "NWS"] and w["svc"].madis is None
    assert w["svc"].madis_hist is None and w["svc"].madis_filling is None


def test_the_table_and_healthz_name_the_rows_madis_gave(winds, madis_clock):
    provs, fetch, _, c = winds
    provs.append(W.MadisProvider(NWS_IDS, madis_fetch()))
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = b'{"@graph": []}'
    d = c.get("/api/wind/latest").get_json()
    assert d["fed_by"] == {"nws:001HE": "madis", "nws:029HI": "madis"}, "the NWS API brought nothing: MADIS feeds both"
    assert {r[0]: r for r in d["rows"]}["nws:001HE"][1:] == [MADIS_T, 6.5, 9.7, 25]
    assert "nws" not in d["newest"] and "coops" in d["newest"], "the NWS feed never brought a reading; CO-OPS did"
    h = A._wind_health(_time.time())
    assert h["fed_by"] == {"madis": 2} and any(s["source"] == "MADIS" for s in h["providers"])
    ms = [s for s in h["providers"] if s["source"] == "MADIS"][0]
    assert ms["hist_stations"] == 2 and ms["hist_rows"] == 2 and ms["hist_oldest_age_s"] is not None
    assert ms["backfill"] is None and ms["backfill_left"] == 0 and ms["backfill_gave_up"] == 0, "built without back-fill here"
