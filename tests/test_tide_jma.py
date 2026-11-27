"""JMA tide tables (tide_providers/jma.py, plan section 112 step 3): the fixed-width year files, JST -> UTC, cm -> m,
the 9999 / 999 "none" markers, the New-Year two-file fetch, failures, and the station list.

Fixtures: tests/fixtures/tides_national/jma (captured by tests/capture_tide_jma_hko_fixtures.py): Tokyo's 2026 file
trimmed to 12-15 .. 12-31 and its 2027 file to 01-01 .. 01-15, and station2026.php trimmed to five rows."""
import os
import re
from datetime import datetime, timezone

import pytest

from tide_providers import TideFetchError
from tide_providers import jma

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "tides_national", "jma")
ID_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,79}")
UA = "allshoresurf.com tide stations (https://allshoresurf.com)"
TK = {"id": "jma:tk", "name": "東京 (TK)", "lat": 35.65, "lon": 139.76667, "local": {"code": "TK"}}


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


def year_url(year, code="TK"):
    return jma.TXT_URL.format(year=year, code=code)


def fake():
    return FakeGet({year_url(2026): body("TK_2026.txt"), year_url(2027): body("TK_2027.txt"),
                    jma.LIST_URL.format(year=2026): body("station2026.html")})


# ------------------------------------------------------------------------------------------ the year files

def test_year_file_reads_hourly_heights_in_jst_as_utc_metres():
    curve, hilo = jma.parse_year_file(body("TK_2027.txt"), "TK", 2027)
    assert len(curve) == 15 * 24
    # readme's example line (Tokyo, 2027-01-01): 00 h 125 cm, 01 h 118 cm ... 23 h 115 cm
    # (00:00 JST = 15:00 UTC the day before)
    assert curve[0] == (utc(2026, 12, 31, 15), 1.25)
    assert curve[1] == (utc(2026, 12, 31, 16), 1.18)
    assert curve[23] == (utc(2027, 1, 1, 14), 1.15)
    assert all(b[0] - a[0] == 3600 for a, b in zip(curve, curve[1:]))
    # the same day's highs and lows: high 10:34 163 cm; lows 04:21 95 cm, 18:30 69 cm (JST)
    day = [p for p in hilo if p[0] < utc(2027, 1, 1, 15)]
    assert day == [(utc(2026, 12, 31, 19, 21), 0.95, "L"), (utc(2027, 1, 1, 1, 34), 1.63, "H"),
                   (utc(2027, 1, 1, 9, 30), 0.69, "L")]


def test_none_markers_and_space_padded_times():
    _, hilo = jma.parse_year_file(body("TK_2026.txt"), "TK", 2026)
    # 2026-12-18: one high (11:12 162 cm), the other three high slots and two low slots are 9999 / 999
    d18 = [p for p in hilo if utc(2026, 12, 17, 15) <= p[0] < utc(2026, 12, 18, 15)]
    assert d18 == [(utc(2026, 12, 17, 19, 18), 0.81, "L"), (utc(2026, 12, 18, 2, 12), 1.62, "H"),
                   (utc(2026, 12, 18, 9, 29), 0.80, "L")]
    # 2026-12-31: "23 1" = 23:01 and "17 0" = 17:00
    d31 = [p for p in hilo if utc(2026, 12, 30, 15) <= p[0] < utc(2026, 12, 31, 15)]
    assert (utc(2026, 12, 31, 14, 1), 1.27, "H") in d31 and (utc(2026, 12, 31, 8, 0), 0.80, "L") in d31
    assert all(h != 9.99 and h < 5 for _, h, _ in hilo)                     # no 999 slipped through as a height
    assert not any(a[2] == b[2] for a, b in zip(hilo, hilo[1:]))           # Tokyo's tide alternates


def test_negative_heights_read():
    line = ("-13 -8 19 60106147176188185170150132120121134155178194196182153114 72 33261127TK"
            " 716189173919799999999999999 015-14122811999999999999999\n")
    curve, hilo = jma.parse_year_file(line.encode(), "TK", 2026)
    assert curve[0] == (utc(2026, 11, 26, 15), -0.13) and curve[1][1] == -0.08
    assert (utc(2026, 11, 26, 15, 15), -0.14, "L") in hilo                 # 00:15 JST, -14 cm (JMA's page: 0:15 -14)
    assert [k for *_, k in hilo] == ["L", "H", "L", "H"]


@pytest.mark.parametrize("text", [
    "",                                                                     # empty
    "<html><body>Not Found</body></html>\n",                                # an error page with HTTP 200
    "abc 118109101 96 97105120137153162162154140123106 90 77 70 70 77 8910311527 1 1TK"
    "1034163999999999999999999999 421 951830 6999999999999999\n",          # a letter in an hourly height
    "125118109101 96 97105120137153162162154140123106 90 77 70 70 77 8910311527 1 1OS"
    "1034163999999999999999999999 421 951830 6999999999999999\n",          # another station's line
    "125118109101 96 97105120137153162162154140123106 90 77 70 70 77 8910311527 230TK"
    "1034163999999999999999999999 421 951830 6999999999999999\n",          # 30 February
    "125118109101 96 97105120137153162162154140123106 90 77 70 70 77 8910311527 1 1TK"
    "1074163999999999999999999999 421 951830 6999999999999999\n",          # 10:74
])
def test_malformed_files_raise(text):
    with pytest.raises(TideFetchError):
        jma.parse_year_file(text.encode(), "TK", 2027)


def test_a_file_of_another_year_raises():
    with pytest.raises(TideFetchError):
        jma.parse_year_file(body("TK_2027.txt"), "TK", 2026)


# ------------------------------------------------------------------------------------------ fetch

def test_fetch_across_new_year_reads_two_files():
    g = fake()
    begin, end = utc(2026, 12, 20), utc(2027, 1, 10)
    got = jma.fetch(TK, begin, end, g)
    assert [u for u, _ in g.calls] == [year_url(2026), year_url(2027)]
    assert all(h == {"User-Agent": UA} for _, h in g.calls)
    assert len(g.calls) <= jma.MAX_REQUESTS
    c = got["curve"]
    assert c[0][0] == begin - jma.CURVE_PAD_S and c[-1][0] == end + jma.CURVE_PAD_S
    assert all(b[0] - a[0] == 3600 for a, b in zip(c, c[1:]))              # no seam at New Year
    seam = {t: v for t, v in c}
    assert seam[utc(2026, 12, 31, 14)] == 1.27 and seam[utc(2026, 12, 31, 15)] == 1.25   # 23 h JST 31 Dec / 00 h 1 Jan
    h = got["hilo"]
    assert all(begin <= t <= end for t, _, _ in h)
    assert (utc(2026, 12, 31, 14, 1), 1.27, "H") in h and (utc(2027, 1, 1, 1, 34), 1.63, "H") in h
    assert {k for *_, k in h} == {"H", "L"}
    assert all(isinstance(t, int) and isinstance(v, float) for t, v in c)


def test_fetch_inside_one_year_reads_one_file():
    g = fake()
    got = jma.fetch(TK, utc(2026, 12, 16), utc(2026, 12, 30), g)
    assert [u for u, _ in g.calls] == [year_url(2026)]
    assert got["curve"] and got["hilo"]


def test_fetch_failures_raise():
    begin, end = utc(2026, 12, 20), utc(2027, 1, 10)
    g = FakeGet({year_url(2026): body("TK_2026.txt")})                    # 2027 answers 404
    with pytest.raises(TideFetchError):
        jma.fetch(TK, begin, end, g)
    g = FakeGet({year_url(2026): b"<html>error</html>", year_url(2027): body("TK_2027.txt")})
    with pytest.raises(TideFetchError):
        jma.fetch(TK, begin, end, g)
    with pytest.raises(TideFetchError):                                     # another station's file under TK's URL
        jma.fetch({"id": "jma:os", "local": {"code": "OS"}}, utc(2026, 12, 16), utc(2026, 12, 30),
                  FakeGet({year_url(2026, "OS"): body("TK_2026.txt")}))
    with pytest.raises(TideFetchError):                                     # the window outside the file's days
        jma.fetch(TK, utc(2026, 6, 1), utc(2026, 6, 20), fake())


def test_years_for_pads_the_window_in_jst():
    assert jma.years_for(utc(2026, 11, 25), utc(2026, 12, 29)) == [2026]
    assert jma.years_for(utc(2026, 12, 1), utc(2026, 12, 31, 12)) == [2026, 2027]   # 31 Dec 12 UTC + 3 h = 1 Jan JST
    assert jma.years_for(utc(2027, 1, 1), utc(2027, 2, 1)) == [2027]                # 1 Jan 00 UTC - 3 h = 1 Jan 06 JST
    assert jma.years_for(utc(2026, 12, 31, 14), utc(2027, 2, 1)) == [2026, 2027]    # - 3 h = 31 Dec 20 JST


# ------------------------------------------------------------------------------------------ the station list

def test_station_list_reads_positions_names_and_codes():
    g = fake()
    st = jma.build_station_list(g, year=2026)
    assert [u for u, _ in g.calls] == [jma.LIST_URL.format(year=2026)] and g.calls[0][1] == {"User-Agent": UA}
    by = {s["local"]["code"]: s for s in st}
    assert sorted(by) == ["CC", "MC", "NH", "TK", "WN"]
    tk = by["TK"]
    assert tk["id"] == "jma:tk" and tk["name"] == "東京 (TK)" and tk["local"]["name_ja"] == "東京"
    assert tk["lat"] == pytest.approx(35 + 39 / 60, abs=1e-5) and tk["lon"] == pytest.approx(139 + 46 / 60, abs=1e-5)
    assert tk["local"]["msl_cm"] == 120.0
    assert by["MC"]["lat"] == pytest.approx(24 + 17 / 60, abs=1e-5)                  # Minamitorishima 24°17'
    assert by["MC"]["lon"] == pytest.approx(153 + 59 / 60, abs=1e-5)                 # 153°59'
    for s in st:
        prefix, local = s["id"].split(":", 1)
        assert prefix == jma.PREFIX and ID_RE.fullmatch(local)
        assert set(s) == {"id", "name", "lat", "lon", "local"}
        assert isinstance(s["lat"], float) and isinstance(s["lon"], float)


def test_parse_position_forms():
    assert jma.parse_position("45゜24'") == pytest.approx(45.4)
    assert jma.parse_position("141°41'") == pytest.approx(141 + 41 / 60)
    assert jma.parse_position("-") is None and jma.parse_position("35゜75'") is None


def test_station_list_failures_raise():
    with pytest.raises(TideFetchError):
        jma.build_station_list(FakeGet({}), year=2026)
    with pytest.raises(TideFetchError):
        jma.build_station_list(FakeGet({jma.LIST_URL.format(year=2026): b"<html>maintenance</html>"}), year=2026)


def test_page_url_and_constants():
    assert jma.page_url(TK) == "https://www.data.jma.go.jp/kaiyou/db/tide/suisan/suisan.php?stn=TK"
    assert jma.PREFIX == "jma" and jma.MAX_REQUESTS == 2
    assert "気象庁ホームページ" in jma.CREDIT and "Japan Meteorological Agency website" in jma.CREDIT
    assert "加工" in jma.CREDIT and "edited" in jma.CREDIT
    assert jma.DATUM.startswith("tide-table datum") and "LAT" not in jma.DATUM   # G76: heights reach -0.34 m, not LAT
