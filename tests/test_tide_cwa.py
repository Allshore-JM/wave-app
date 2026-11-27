"""Taiwan CWA tide forecasts (tide_providers/cwa.py, plan section 113 B): both answer shapes (the REST API's records and
the daily file), XML-converted single items, +08:00 -> UTC, heights above chart datum, the key (only in the URL, never
in an error, the places hidden without it), the station list, and the window built from CWA's highs and lows.

Fixtures: tests/fixtures/tides_national/cwa: CWA's daily file of 2026-11-28 (F-A0021-001, Open Government Data License)
trimmed to three places: Donggang township (10013030, one tide on its third day), Nangan township (09007010) and Fulong
harbour (I03800); and a REST-shaped answer for I03800 (records.TideForecasts)."""
import json
import math
import sys
import os
from datetime import datetime, timezone

import pytest

import tide_global
from tide_providers import TideFetchError
from tide_providers import cwa

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "tides_national", "cwa")
KEY = "CWA-TEST-KEY-0123456789"
FULONG = {"id": "cwa:i03800", "name": "Harbour (福隆)", "lat": 25.021, "lon": 121.951, "tz": "Asia/Taipei",
          "local": {"code": "I03800"}}


def utc(*a):
    return int(datetime(*a, tzinfo=timezone.utc).timestamp())


def body(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return f.read()


class FakeGet:
    def __init__(self, pages=None, fail=None):
        self.pages, self.fail, self.calls = pages or {}, fail, []

    def __call__(self, url, headers=None, data=None):
        self.calls.append((url, headers))
        if self.fail is not None:
            raise self.fail
        for k, v in self.pages.items():
            if k in url:
                return v
        raise RuntimeError("HTTP 404 from %s" % url)


@pytest.fixture(autouse=True)
def _fresh_module_state():
    """cwa keeps earlier highs and lows and a daily count in the module: each test starts empty."""
    cwa._held.clear()
    cwa._count.update(day=None, n=0)
    yield
    cwa._held.clear()
    cwa._count.update(day=None, n=0)


def daily_doc():
    return json.loads(body("F-A0021-001-trimmed.json").decode("utf-8"))


def test_both_answer_shapes_and_single_items():
    file_locs = cwa.forecasts(daily_doc())
    assert [e["Location"]["LocationId"] for e in file_locs] == ["10013030", "09007010", "I03800"]
    rest = json.loads(body("rest-I03800.json").decode("utf-8"))
    assert [e["Location"]["LocationId"] for e in cwa.forecasts(rest)] == ["I03800"]
    one = {"records": {"TideForecasts": rest["records"]["TideForecasts"][0]}}          # one place: an object
    assert [e["Location"]["LocationId"] for e in cwa.forecasts(one)] == ["I03800"]
    with pytest.raises(TideFetchError):
        cwa.forecasts({"success": "false"})
    with pytest.raises(TideFetchError):
        cwa.forecasts(["not", "a", "dict"])


def test_events_utc_kinds_and_chart_datum():
    loc = cwa.forecasts(daily_doc())[2]["Location"]                                    # Fulong harbour
    assert loc["LocationId"] == "I03800"
    ev = cwa.parse_events(loc)
    raw = loc["TimePeriods"]["Daily"][0]["Time"]
    raw = raw if isinstance(raw, list) else [raw]
    first = raw[0]
    t = datetime.fromisoformat(first["DateTime"])
    assert ev[0][0] == int(t.timestamp()) and t.utcoffset().total_seconds() == 8 * 3600
    assert ev[0][1] == round(int(first["TideHeights"]["AboveChartDatum"]) / 100.0, 3)
    assert ev[0][2] == {"滿潮": "H", "乾潮": "L"}[first["Tide"]]
    assert len(ev) > 100 and [e[0] for e in ev] == sorted({e[0] for e in ev})
    kinds = [k for _, _, k in ev]
    assert sum(a == b for a, b in zip(kinds, kinds[1:])) <= 2, "highs and lows alternate"
    assert all(-1 < h < 4 for _, h, _ in ev), "metres above chart datum (Fulong's range is under 2 m)"
    matsu = cwa.parse_events(cwa.forecasts(daily_doc())[1]["Location"])               # Nangan: a 6-m range
    assert max(h for _, h, _ in matsu) > 5 and min(h for _, h, _ in matsu) > -1


def test_a_day_with_one_tide_is_read():
    loc = cwa.forecasts(daily_doc())[0]["Location"]                                    # Donggang: day 3 has one tide
    day = loc["TimePeriods"]["Daily"][2]
    assert isinstance(day["Time"], dict)
    ev = cwa.parse_events(loc)
    t = int(datetime.fromisoformat(day["Time"]["DateTime"]).timestamp())
    assert t in [e[0] for e in ev]


def test_bad_events_are_skipped():
    loc = {"TimePeriods": {"Daily": [{"Date": "2026-11-28", "Time": [
        {"DateTime": "2026-11-28T06:26:00+08:00", "Tide": "乾潮", "TideHeights": {"AboveChartDatum": "49"}},
        {"DateTime": "2026-11-28T12:49:00", "Tide": "滿潮", "TideHeights": {"AboveChartDatum": "657"}},      # no zone
        {"DateTime": "2026-11-28T18:58:00+08:00", "Tide": "?", "TideHeights": {"AboveChartDatum": "60"}},  # no kind
        {"DateTime": "2026-11-29T00:58:00+08:00", "Tide": "滿潮", "TideHeights": {"AboveChartDatum": None}},
        {"DateTime": "2026-11-29T06:58:00+08:00", "Tide": "乾潮", "TideHeights": {"AboveChartDatum": "x"}},
        {"DateTime": "2026-11-28T06:26:00+08:00", "Tide": "乾潮", "TideHeights": {"AboveChartDatum": "49"}}]}]}}
    assert cwa.parse_events(loc) == [(utc(2026, 11, 27, 22, 26), 0.49, "L")]


def test_station_list_from_the_daily_file():
    get = FakeGet({cwa.FILE_URL: body("F-A0021-001-trimmed.json")})
    got = cwa.build_station_list(get)
    assert [s["id"] for s in got] == ["cwa:09007010", "cwa:10013030", "cwa:i03800"]
    assert all(s["tz"] == "Asia/Taipei" and s["local"]["code"] for s in got)
    assert all(tide_global.valid_id(s["id"]) for s in got)
    assert get.calls[0][0] == cwa.FILE_URL and "Authorization" not in get.calls[0][0], "no key for the list"
    with pytest.raises(TideFetchError):
        cwa.build_station_list(FakeGet(fail=RuntimeError("boom")))


def test_fetch_needs_the_key_puts_it_only_in_the_url_and_never_in_an_error(monkeypatch):
    begin, end = utc(2026, 11, 28), utc(2026, 12, 20)
    monkeypatch.delenv("CWA_API_KEY", raising=False)
    assert not cwa.available()
    with pytest.raises(TideFetchError, match="no CWA key"):
        cwa.fetch(FULONG, begin, end, FakeGet({"LocationId=I03800": body("rest-I03800.json")}))
    monkeypatch.setenv("CWA_API_KEY", " %s " % KEY)
    assert cwa.available()
    get = FakeGet({"LocationId=I03800": body("rest-I03800.json")})
    got = cwa.fetch(FULONG, begin, end, get)
    url, headers = get.calls[0]
    assert url.startswith("https://opendata.cwa.gov.tw/api/v1/rest/datastore/F-A0021-001?")
    assert "Authorization=%s" % KEY in url and "LocationId=I03800" in url and "allshoresurf.com" in headers["User-Agent"]
    assert got["curve"] == [] and got["hilo"] and all(begin <= t <= end for t, _, _ in got["hilo"])
    assert {k for _, _, k in got["hilo"]} == {"H", "L"}
    # an error whose text carries the URL (and so the key): only its type goes on
    with pytest.raises(TideFetchError) as e:
        cwa.fetch(FULONG, begin, end, FakeGet(fail=RuntimeError("HTTP 500 from %s" % (cwa.API % (KEY, "I03800")))))
    assert KEY not in str(e.value) and e.value.__cause__ is None and e.value.__context__ is None and "RuntimeError" in str(e.value)
    # another place's answer, a window outside CWA's month, garbage
    with pytest.raises(TideFetchError, match="0 highs and lows"):
        cwa.fetch(dict(FULONG, local={"code": "I03900"}), begin, end, FakeGet({"LocationId=I03900": body("rest-I03800.json")}))
    with pytest.raises(TideFetchError, match="does not reach"):
        cwa.fetch(FULONG, utc(2027, 3, 1), utc(2027, 3, 30), get)
    with pytest.raises(TideFetchError):
        cwa.fetch(FULONG, begin, end, FakeGet({"LocationId=I03800": b"<html>busy</html>"}))


def test_places_are_listed_only_with_a_key(tmp_path, monkeypatch):
    doc = {"fields": ["id", "name", "lat", "lon", "tz", "local"], "stations": [
        ["cwa:i03800", "Harbour (福隆)", 25.021, 121.951, "Asia/Taipei", {"code": "I03800"}],
        ["linz:raglan", "Raglan", -37.8, 174.87, "Pacific/Auckland", {"file": "Raglan"}]]}
    p = tmp_path / "national.json"
    p.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    monkeypatch.delenv("CWA_API_KEY", raising=False)
    assert sorted(tide_global.load_national(str(p))) == ["linz:raglan"]
    monkeypatch.setenv("CWA_API_KEY", KEY)
    assert sorted(tide_global.load_national(str(p))) == ["cwa:i03800", "linz:raglan"]


def test_the_window_from_cwas_highs_and_lows(monkeypatch):
    monkeypatch.setenv("CWA_API_KEY", KEY)
    begin = utc(2026, 11, 28)
    import tide_sources as T
    lo, hi = begin - 2 * T.LEAD_S, begin + T.SPAN_S + 2 * T.LEAD_S
    got = cwa.fetch(FULONG, lo, hi, FakeGet({"LocationId=I03800": body("rest-I03800.json")}))
    out = tide_global.official_payload(FULONG, begin, cwa, got)
    assert out["method"] == "official-cosine" and out["datum"] == cwa.DATUM and "below zero" in cwa.DATUM
    assert out["agency"] == "Central Weather Administration (Taiwan)" and out["link"] == cwa.PAGE
    assert "https://data.gov.tw/license" in out["source"] and "交通部中央氣象署" in out["source"]
    assert any(v is not None for v in out["v"]) and out["hilo"]
    assert "cwa" in tide_global.PROVIDERS and tide_global.valid_id("cwa:i03800")


def test_the_committed_snapshot_lists_taiwan():
    doc = json.load(open(tide_global.NATIONAL_PATH, encoding="utf-8"))
    rows = [dict(zip(doc["fields"], r)) for r in doc["stations"] if r[0].startswith("cwa:")]
    assert len(rows) >= 250 and all(r["tz"] == "Asia/Taipei" for r in rows)
    assert all(20 <= r["lat"] <= 27.5 and 116 <= r["lon"] <= 123 and r["local"]["code"] for r in rows)
    # owner, 2026-11-27: places by their proper names, never as surf spots: CWA's surf list reads "coast" with the
    # place's own name, and no name says "surf"
    coast = [r for r in rows if r["id"].startswith("cwa:o")]
    assert 10 <= len(coast) <= 13 and all(r["name"].startswith("Coast (") for r in coast)
    assert not any("surf" in r["name"].lower() or "衝浪" in r["name"] for r in rows)


# ---------------------------------------------------------------------------------------------- G79 fixes

def _place(code, start, n, gap_cd_msl=100, step_h=6.2, wobble=None, lat="25.0"):
    """A synthetic CWA place: n alternating extremes every step_h hours from `start` (an aware datetime string)."""
    t0 = datetime.fromisoformat(start)
    days = {}
    for i in range(n):
        t = datetime.fromtimestamp(t0.timestamp() + i * step_h * 3600, t0.tzinfo)
        msl = 80 if i % 2 == 0 else -80
        gap = gap_cd_msl + (wobble[i % len(wobble)] if wobble else 0)
        days.setdefault(t.date().isoformat(), []).append(
            {"DateTime": t.isoformat(), "Tide": "滿潮" if i % 2 == 0 else "乾潮",
             "TideHeights": {"AboveLocalMSL": str(msl), "AboveChartDatum": str(msl + gap), "AboveTWVD": None}})
    return {"LocationId": code, "LocationName": "漁港測試", "Latitude": lat, "Longitude": "121.9",
            "TimePeriods": {"Daily": [{"Date": d, "Time": v if len(v) > 1 else v[0]} for d, v in sorted(days.items())]}}


def _rest(loc):
    return json.dumps({"success": "true", "records": {"TideForecasts": [{"Location": loc}]}}, ensure_ascii=False).encode()


def test_g79_1_a_place_keeps_its_earlier_highs_and_lows_so_the_window_starts_filled(monkeypatch):
    monkeypatch.setenv("CWA_API_KEY", KEY)
    st = dict(FULONG, local={"code": "I99999"})
    import tide_sources as T
    # day 1: CWA's month from Taipei's midnight 2026-11-28 (16:00 UTC on the 27th); day 2: from the 29th's midnight
    one = _place("I99999", "2026-11-28T03:00:00+08:00", 120)
    two = _place("I99999", "2026-11-29T02:30:00+08:00", 120)
    cwa.fetch(st, utc(2026, 11, 26), utc(2026, 12, 30), FakeGet({"LocationId=I99999": _rest(one)}))
    now = utc(2026, 11, 28, 17, 0)                                   # after CWA's rebuild for the 29th
    begin = T.begin_of(now)                                          # 00:00 UTC the 27th
    lo, hi = begin - 2 * T.LEAD_S, begin + T.SPAN_S + 2 * T.LEAD_S
    got = cwa.fetch(st, lo, hi, FakeGet({"LocationId=I99999": _rest(two)}))
    assert got["hilo"][0][0] == int(datetime.fromisoformat("2026-11-28T03:00:00+08:00").timestamp()), "day 1's first"
    out = tide_global.official_payload(st, begin, cwa, got)
    i = (now - out["begin"]) // out["step"]
    assert out["v"][i] is not None, "a height at now (17:00 UTC, before the new month's first extreme)"
    # a fresh server (nothing held): the new month only (the window says it starts late)
    cwa._held.clear()
    cold = cwa.fetch(st, lo, hi, FakeGet({"LocationId=I99999": _rest(two)}))
    assert cold["hilo"][0][0] == int(datetime.fromisoformat("2026-11-29T02:30:00+08:00").timestamp())
    # what is held is trimmed to HOLD_S
    cwa._with_held("X", [(1000, 1.0, "H")], now=1000 + cwa.HOLD_S + 10)
    assert cwa._held["X"] == []


def test_g79_2_cwa_tides_has_its_own_off_switch(monkeypatch):
    monkeypatch.setenv("CWA_API_KEY", KEY)
    assert cwa.available()
    monkeypatch.setenv("CWA_TIDES", "0")
    assert not cwa.available()
    monkeypatch.setenv("CWA_TIDES", "1")
    assert cwa.available()


def test_g79_3_inconsistent_chart_datum_heights_come_from_msl(monkeypatch):
    steady = _place("I00001", "2026-11-28T03:00:00+08:00", 40, gap_cd_msl=120, wobble=[0, 1, -1])
    wild = _place("I00002", "2026-11-28T03:00:00+08:00", 40, gap_cd_msl=114, wobble=[-31, 33, 0], lat="25.1")
    assert cwa.chart_gap(steady) == (120, 2)
    med, spread = cwa.chart_gap(wild)
    assert med == 114 and spread == 64
    doc = {"cwaopendata": {"Resources": {"Resource": {"Data": {"TideForecasts": [{"Location": steady}, {"Location": wild}]}}}}}
    got = {s["local"]["code"]: s["local"] for s in cwa.build_station_list(FakeGet({cwa.FILE_URL: json.dumps(doc).encode()}))}
    assert "msl_gap_cm" not in got["I00001"] and got["I00002"]["msl_gap_cm"] == 114
    ev = cwa.parse_events(wild, 114)
    assert {h for _, h, _ in ev} == {1.94, 0.34}, "MSL +-80 cm + 114: one range, 1.60 m"
    monkeypatch.setenv("CWA_API_KEY", KEY)
    st = {"id": "cwa:i00002", "name": "x", "lat": 25, "lon": 121.9, "tz": "Asia/Taipei", "local": got["I00002"]}
    out = cwa.fetch(st, utc(2026, 11, 27), utc(2026, 12, 30), FakeGet({"LocationId=I00002": _rest(wild)}))
    assert {h for _, h, _ in out["hilo"]} == {1.94, 0.34}


def test_g79_6_a_daily_request_cap(monkeypatch):
    monkeypatch.setattr(cwa, "DAILY_MAX", 2)
    day = utc(2026, 11, 28, 5)
    cwa._spend(day); cwa._spend(day + 60)
    with pytest.raises(TideFetchError, match="budget"):
        cwa._spend(day + 120)
    cwa._spend(utc(2026, 11, 29, 0, 1))                              # a new UTC day
    monkeypatch.setenv("CWA_API_KEY", KEY)
    monkeypatch.setattr(cwa, "DAILY_MAX", 0)
    cwa._count.update(day=None, n=0)
    get = FakeGet({"LocationId=I03800": body("rest-I03800.json")})
    with pytest.raises(TideFetchError, match="budget"):
        cwa.fetch(FULONG, utc(2026, 11, 28), utc(2026, 12, 20), get)
    assert get.calls == [], "no request past the cap"


def test_g79_9_one_place_per_spot():
    a = {"id": "cwa:a1", "lat": 25.0, "lon": 121.9, "local": {"code": "A01000"}}
    b = {"id": "cwa:b1", "lat": 25.0001, "lon": 121.9, "local": {"code": "B01100"}}           # 11 m: merged
    t = {"id": "cwa:t1", "lat": 25.0002, "lon": 121.9, "local": {"code": "10014070"}}         # a township: first
    far = {"id": "cwa:n1", "lat": 25.0009, "lon": 121.9, "local": {"code": "N00100"}}         # 78 m from the township
    kept = {s["id"] for s in cwa._one_per_spot([a, b, t, far])}
    assert kept == {"cwa:t1", "cwa:n1"}


def test_g79_4_5_names_by_county_and_never_a_surf_word():
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools", "tides"))
    import build_national as BN
    st = [{"name": "桃園市新屋區", "lat": 25.0, "lon": 121.05, "local": {"code": "68000040"}},
          {"name": "基隆市中正區", "lat": 25.15, "lon": 121.77, "local": {"code": "10017010"}},
          {"name": "漁港永安", "lat": 24.99, "lon": 121.01, "local": {"code": "I04000"}},
          {"name": "漁港望海巷", "lat": 25.14, "lon": 121.80, "local": {"code": "I02800"}},
          {"name": "衝浪南灣", "lat": 25.1, "lon": 121.78, "local": {"code": "O00700"}}]
    BN.cwa_names(st)
    assert [s["name"] for s in st] == ["Taoyuan City (桃園市新屋區)", "Keelung City (基隆市中正區)", "Harbour (永安), Taoyuan City",
                                       "Harbour (望海巷), Keelung City", "Coast (南灣), Keelung City"]
    with pytest.raises(SystemExit, match="surf"):
        BN.cwa_names([{"name": "海水浴場衝浪區", "lat": 25.0, "lon": 121.0, "local": {"code": "A09999"}}])


def test_g79_9_the_station_list_keeps_one_place_per_spot():
    same = [_place("A01000", "2026-11-28T03:00:00+08:00", 40), _place("B01100", "2026-11-28T03:00:00+08:00", 40),
            _place("I05000", "2026-11-28T03:00:00+08:00", 40, lat="25.2")]
    doc = {"cwaopendata": {"Resources": {"Resource": {"Data": {"TideForecasts": [{"Location": x} for x in same]}}}}}
    got = cwa.build_station_list(FakeGet({cwa.FILE_URL: json.dumps(doc).encode()}))
    assert [s["local"]["code"] for s in got] == ["A01000", "I05000"], "the beach before the fishing spot at one spot"
    snap = json.load(open(tide_global.NATIONAL_PATH, encoding="utf-8"))
    rows = [dict(zip(snap["fields"], r)) for r in snap["stations"] if r[0].startswith("cwa:")]
    for i, a in enumerate(rows):
        for b in rows[i + 1:]:
            dy = (a["lat"] - b["lat"]) * 111195.0
            dx = (a["lon"] - b["lon"]) * 111195.0 * math.cos(math.radians(a["lat"]))
            assert math.hypot(dx, dy) >= cwa.SPOT_M, (a["id"], b["id"])
