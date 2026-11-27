"""Hong Kong Observatory tide predictions (tide_providers/hko.py, plan section 112 step 3): the HLT / HHOT answers,
HKT -> UTC, hour "24", telling highs from lows, the New-Year fetch (4 requests), failures, and the station list.

Fixtures: tests/fixtures/tides_national/hko (captured by tests/capture_tide_jma_hko_fixtures.py): Quarry Bay's 2026
year answers trimmed to 12-15 .. 12-31 and its 2027 ones to 01-01 .. 01-15, HKO's prediction-page station table, a
one-day probe and the HTML answer for an unknown station."""
import json
import os
import re
from datetime import datetime, timezone

import pytest

from tide_providers import TideFetchError
from tide_providers import hko

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "tides_national", "hko")
ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,79}")
UA = "allshoresurf.com tide stations (https://allshoresurf.com)"
QUB = {"id": "hko:qub", "name": "Quarry Bay", "lat": 22.29111, "lon": 114.21333, "local": {"code": "QUB"}}
CODES = ["CLK", "CCH", "CMW", "KLW", "KCT", "LOP", "MWC", "PT1", "QUB", "SPW", "TAO", "TMW", "TPK", "TBT", "WAG"]


def utc(*a):
    return int(datetime(*a, tzinfo=timezone.utc).timestamp())


def body(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return f.read()


class FakeGet:
    """get(url, headers=None, data=None) serving fixtures by URL; any other URL fails like a non-200 answer."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def __call__(self, url, headers=None, data=None):
        self.calls.append((url, headers))
        if url not in self.pages:
            raise RuntimeError("HTTP 404 for " + url)
        return self.pages[url]


def year_pages():
    return {hko.url(k, "QUB", y): body("%s_QUB_%d.json" % (k, y)) for k in ("HLT", "HHOT") for y in (2026, 2027)}


# ------------------------------------------------------------------------------------------ parsers

def test_url_shapes():
    assert hko.url("HLT", "QUB", 2026) == ("https://data.weather.gov.hk/weatherAPI/opendata/opendata.php"
                                           "?dataType=HLT&lang=en&rformat=json&station=QUB&year=2026")
    assert hko.url("HLT", "QUB", 2026, 11, 27).endswith("&year=2026&month=11&day=27")


def test_hourly_heights_hkt_to_utc_and_hour_24():
    c26 = hko.parse_hourly(body("HHOT_QUB_2026.json"), 2026)
    c27 = hko.parse_hourly(body("HHOT_QUB_2027.json"), 2027)
    assert len(c26) == 17 * 24 and len(c27) == 15 * 24
    # 2026-12-15 column "01" = 01:00 HKT = 12-14 17:00 UTC, 2.18 m; column "24" of 12-31 = 2027-01-01 00:00 HKT
    assert c26[0] == (utc(2026, 12, 14, 17), 2.18)
    assert c26[-1] == (utc(2026, 12, 31, 16), 1.47)
    assert c27[0] == (utc(2026, 12, 31, 17), 1.47)                          # 2027-01-01 01:00 HKT
    allc = c26 + c27
    assert all(b[0] - a[0] == 3600 for a, b in zip(allc, allc[1:]))       # no gap, no double hour at New Year


def test_extremes_read_and_classified():
    ext = hko.parse_extremes(body("HLT_QUB_2026.json"), 2026)
    # 2026-12-15: 07:31 0.80 and 23:57 2.13 HKT, the other two pairs empty ("")
    assert ext[:2] == [(utc(2026, 12, 14, 23, 31), 0.80), (utc(2026, 12, 15, 15, 57), 2.13)]
    hilo = hko.classify(ext, hko.parse_hourly(body("HHOT_QUB_2026.json"), 2026))
    assert len(hilo) == len(ext)
    assert [k for *_, k in hilo[:2]] == ["L", "H"]
    assert not any(a[2] == b[2] for a, b in zip(hilo, hilo[1:]))           # highs and lows alternate
    # 2026-12-31: 03:00 1.81 H, 08:38 0.98 L, 16:22 1.87 H, 21:36 1.40 L (HKT)
    d31 = [p for p in hilo if utc(2026, 12, 30, 16) <= p[0] < utc(2026, 12, 31, 16)]
    assert d31 == [(utc(2026, 12, 30, 19, 0), 1.81, "H"), (utc(2026, 12, 31, 0, 38), 0.98, "L"),
                   (utc(2026, 12, 31, 8, 22), 1.87, "H"), (utc(2026, 12, 31, 13, 36), 1.40, "L")]


def test_classify_falls_back_to_the_hourly_heights():
    t0 = utc(2026, 1, 1)
    curve = [(t0 + i * 3600, v) for i, v in enumerate([1.0, 1.5, 2.0, 1.5, 1.0, 0.5, 0.2, 0.5, 1.0])]
    # 2.0 at 02:00 is a high, but its neighbours (a missing low) do not alternate: 2.0 vs 2.1 and 0.2
    ext = [(t0 - 6 * 3600, 2.1), (t0 + 2 * 3600, 2.0), (t0 + 6 * 3600, 0.2)]
    got = hko.classify(ext, curve)
    assert (t0 + 2 * 3600, 2.0, "H") in got and (t0 + 6 * 3600, 0.2, "L") in got
    assert hko.classify([(t0, 1.0), (t0 + 3600, 1.0)], []) == []           # nothing settles equal heights: left out


@pytest.mark.parametrize("raw", [
    b"",
    b"Please include valid parameters in API request.",
    b'{"fields": ["Month"], "data": "x"}',
    b'{"data": []}',
    b'{"fields": ["Month","Date","Time","Height(m)"], "data": [["12","15","07:31","0.80"]]}',
    b'{"fields": ["Month","Date","Time","Height(m)"], "data": [["12","15","0731","n/a"]]}',
    b'{"fields": ["Month","Date","Time","Height(m)"], "data": [["02","30","0731","0.80"]]}',
])
def test_malformed_extremes_raise(raw):
    with pytest.raises(TideFetchError):
        hko.parse_extremes(raw, 2026)


def test_malformed_hourly_raise():
    with pytest.raises(TideFetchError):
        hko.parse_hourly(body("unknown.html"), 2026)
    with pytest.raises(TideFetchError):
        hko.parse_hourly(b'{"fields": [], "data": [["12","15","1.0"]]}', 2026)


# ------------------------------------------------------------------------------------------ fetch

def test_fetch_across_new_year():
    g = FakeGet(year_pages())
    begin, end = utc(2026, 12, 20), utc(2027, 1, 10)
    got = hko.fetch(QUB, begin, end, g)
    assert sorted(u for u, _ in g.calls) == sorted(year_pages())
    assert len(g.calls) == hko.MAX_REQUESTS == 4
    assert all(h == {"User-Agent": UA} for _, h in g.calls)
    c = got["curve"]
    assert c[0][0] == begin - hko.CURVE_PAD_S and c[-1][0] == end + hko.CURVE_PAD_S
    assert all(b[0] - a[0] == 3600 for a, b in zip(c, c[1:]))
    h = got["hilo"]
    assert all(begin <= t <= end for t, _, _ in h)
    assert not any(a[2] == b[2] for a, b in zip(h, h[1:]))
    # 2027-01-01 (HKT): 04:26 1.57 H, 09:13 1.12 L, 17:10 1.98 H
    assert [(utc(2026, 12, 31, 20, 26), 1.57, "H"), (utc(2027, 1, 1, 1, 13), 1.12, "L"),
            (utc(2027, 1, 1, 9, 10), 1.98, "H")] == [p for p in h
                                                     if utc(2026, 12, 31, 16) <= p[0] < utc(2027, 1, 1, 16)]
    # the highs and lows sit on the hourly curve (same hour convention): within 3 cm
    for t, v, _ in h:
        assert abs(hko._interp(c, t) - v) < 0.03


def test_fetch_inside_one_year_takes_two_requests():
    g = FakeGet(year_pages())
    got = hko.fetch(QUB, utc(2026, 12, 16), utc(2026, 12, 30), g)
    assert [u for u, _ in g.calls] == [hko.url("HLT", "QUB", 2026), hko.url("HHOT", "QUB", 2026)]
    assert got["curve"] and got["hilo"]


def test_fetch_failures_raise():
    begin, end = utc(2026, 12, 20), utc(2027, 1, 10)
    pages = year_pages()
    del pages[hko.url("HHOT", "QUB", 2027)]                                  # 404
    with pytest.raises(TideFetchError):
        hko.fetch(QUB, begin, end, FakeGet(pages))
    pages = year_pages()
    pages[hko.url("HLT", "QUB", 2027)] = body("unknown.html")               # HKO's HTML answer, HTTP 200
    with pytest.raises(TideFetchError):
        hko.fetch(QUB, begin, end, FakeGet(pages))
    pages = year_pages()
    pages[hko.url("HLT", "QUB", 2026)] = json.dumps({"fields": [], "data": []}).encode()
    pages[hko.url("HLT", "QUB", 2027)] = json.dumps({"fields": [], "data": []}).encode()
    with pytest.raises(TideFetchError):                                      # no highs and lows at all
        hko.fetch(QUB, begin, end, FakeGet(pages))
    with pytest.raises(TideFetchError):                                      # the window outside the answers' days
        hko.fetch(QUB, utc(2026, 6, 1), utc(2026, 6, 20), FakeGet(year_pages()))


# ------------------------------------------------------------------------------------------ the station list

def list_pages(missing=()):
    pages = {hko.LIST_URL: body("predtide_uc.js")}
    for c in CODES:
        if c not in missing:
            pages[hko.url("HLT", c, 2026, 1, 1)] = body("probe_QUB_20260101.json")
    return pages


def test_station_list_reads_the_prediction_page_table():
    g = FakeGet(list_pages(missing=("TAO",)))
    st = hko.build_station_list(g, year=2026)
    assert g.calls[0] == (hko.LIST_URL, {"User-Agent": UA})
    assert all(h == {"User-Agent": UA} for _, h in g.calls)
    by = {s["local"]["code"]: s for s in st}
    # a station without predictions is left out; so are the two whose gauges closed in 1997 / 1999 (HKO: "large
    # errors may exist"; plan section 112)
    assert sorted(by) == sorted(set(CODES) - {"TAO", "CMW", "LOP"})
    q = by["QUB"]
    assert q["id"] == "hko:qub" and q["name"] == "Quarry Bay" and q["local"]["name_zh"] == "鰂魚涌"
    # latlon in the script is [lon, lat]; the Observatory's station table: 22°17'28" N, 114°12'48" E
    assert q["lat"] == pytest.approx(22.29111, abs=1e-4) and q["lon"] == pytest.approx(114.21333, abs=1e-4)
    assert q["lat"] == pytest.approx(22 + 17 / 60 + 28 / 3600, abs=1e-3)
    assert q["lon"] == pytest.approx(114 + 12 / 60 + 48 / 3600, abs=1e-3)
    for s in st:
        prefix, local = s["id"].split(":", 1)
        assert prefix == hko.PREFIX and ID_RE.fullmatch(local)
        assert 22.0 < s["lat"] < 22.6 and 113.8 < s["lon"] < 114.4
        assert set(s) == {"id", "name", "lat", "lon", "local"}


def test_station_list_failures_raise():
    with pytest.raises(TideFetchError):
        hko.build_station_list(FakeGet({}), year=2026)
    with pytest.raises(TideFetchError):
        hko.build_station_list(FakeGet({hko.LIST_URL: b"var x = 1;"}), year=2026)


def test_page_url_and_constants():
    assert hko.page_url(QUB) == "https://www.hko.gov.hk/en/tide/predtide.htm?s=QUB"
    assert hko.PREFIX == "hko"
    assert "Hong Kong Observatory" in hko.CREDIT and "DATA.GOV.HK" in hko.CREDIT and "Government" in hko.CREDIT
    assert "LAT" in hko.DATUM
