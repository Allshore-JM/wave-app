"""Taiwan CWA tide forecasts (tide_providers/cwa.py, plan section 113 B): both answer shapes (the REST API's records and
the daily file), XML-converted single items, +08:00 -> UTC, heights above chart datum, the key (only in the URL, never
in an error, the places hidden without it), the station list, and the window built from CWA's highs and lows.

Fixtures: tests/fixtures/tides_national/cwa: CWA's daily file of 2026-11-28 (F-A0021-001, Open Government Data License)
trimmed to three places: Donggang township (10013030, one tide on its third day), Nangan township (09007010) and Fulong
harbour (I03800); and a REST-shaped answer for I03800 (records.TideForecasts)."""
import json
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
    assert KEY not in str(e.value) and e.value.__cause__ is None and "RuntimeError" in str(e.value)
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
    assert out["method"] == "official-cosine" and out["datum"] == "chart datum (lowest low water)"
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
    assert len(coast) == 13 and all(r["name"].lower().startswith(("coast (", )) or " coast (" in r["name"] for r in coast)
    assert not any("surf" in r["name"].lower() or "衝浪" in r["name"] for r in rows)
