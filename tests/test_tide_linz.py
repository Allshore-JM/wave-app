"""Plan section 112, step 3: the LINZ reader (tide_providers/linz.py) on captured answers
(tests/capture_tide_linz_mi_qld_fixtures.py): the place list, the yearly CSVs, NZ local time -> UTC across daylight
saving, the year boundary, failures."""
import json
import os
import re
import sys
from datetime import datetime, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from tide_providers import TideFetchError, linz as L  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "tides_national", "linz")
BEGIN = 1798588800                     # 2026-12-30 00:00 UTC (13:00 NZDT)
END = BEGIN + 3 * 86400                # 2027-01-02 00:00 UTC
ID_RE = re.compile(r"[a-z]+:[a-z0-9][a-z0-9._-]{0,79}")
AKL = {"id": "linz:auckland", "name": "Auckland", "lat": -36.84307, "lon": 174.76946, "local": {"file": "Auckland"}}
CPT = {"id": "linz:castlepoint", "name": "Castlepoint", "lat": -40.9, "lon": 176.2, "local": {"file": "Castlepoint"}}


def _ep(s):
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())


class FakeGet:
    """get(url, headers=None, data=None) serving the captured answers by URL; anything else is a 403 (raises)."""

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
            raise RuntimeError("HTTP 403 " + url)
        with open(os.path.join(FIX, self.index[url]), "rb") as f:
            return f.read()


def _csv(rows, year=2026):
    """A LINZ-shaped CSV from [(month, day, [(hh:mm, h), ...])]."""
    lines = ["﻿070,Test,36°51'S,174°46'E", "Based on constituent set with reference date:,31-Dec-2014",
             "Local Std or Daylight Time,Tidal heights in metres."]
    for m, d, pairs in rows:
        cells = [str(d), "Xx", str(m), str(year)]
        for t, h in pairs:
            cells += [t, h]
        cells += [""] * (12 - len(cells))
        lines.append(",".join(cells))
    return ("\r\n".join(lines) + "\r\n").encode("utf-8")


# ---------------------------------------------------------------------------------------------- the place list

def test_station_list_keeps_daily_prediction_places_with_a_loading_file():
    get = FakeGet()
    got = L.build_station_list(get, check_year=2026)
    assert get.calls[0] == (L.LIST_URL, L.HEADERS)
    assert all(h["User-Agent"] == "allshoresurf.com tide stations (https://allshoresurf.com)" for _, h in get.calls)
    # Akaroa and Scott Base are "Daily Predictions" too, but their 2026 CSV is not among the fixtures (403): left out;
    # Ahipara Bay and Anita Bay are "Offset" places (PDF offsets only): never asked
    assert [s["id"] for s in got] == ["linz:auckland", "linz:castlepoint"]
    assert got[0] == {"id": "linz:auckland", "name": "Auckland", "lat": -36.84307, "lon": 174.76946,
                      "local": {"file": "Auckland"}}
    asked = [u for u, _ in get.calls[1:]]
    assert L.csv_url("Auckland", 2026) in asked and not any("Ahipara" in u or "Anita" in u for u in asked)
    for s in got:
        assert ID_RE.fullmatch(s["id"]) and len(s["id"]) <= 80
        json.dumps(s)


def test_station_list_without_the_file_check():
    get = FakeGet()
    got = L.build_station_list(get, check_year=None)
    assert len(get.calls) == 1
    by = {s["id"]: s for s in got}
    assert sorted(by) == ["linz:akaroa", "linz:auckland", "linz:castlepoint", "linz:scott-base"]
    assert by["linz:akaroa"]["lat"] == -43.80873 and by["linz:akaroa"]["lon"] == 172.95897
    assert by["linz:scott-base"]["lat"] < -77                     # Antarctica (NZ time)


def test_station_list_leaves_out_bad_rows():
    doc = [{"name": "Ākitio River Entrance", "field_location": "-40.59865,176.42341",
            "field_tide_type": "Daily Predictions", "field_tide_prediction_file": "Akitio"},
           {"name": "No Position", "field_location": "", "field_tide_type": "Daily Predictions",
            "field_tide_prediction_file": "X"},
           {"name": "Bad Position", "field_location": "-95,176", "field_tide_type": "Daily Predictions",
            "field_tide_prediction_file": "X"},
           {"name": "No File", "field_location": "-40,176", "field_tide_type": "Daily Predictions",
            "field_tide_prediction_file": ""},
           {"name": "An Offset", "field_location": "-40,176", "field_tide_type": "Offset",
            "field_tide_prediction_file": "Y"},
           {"name": "akitio river entrance", "field_location": "-40,176", "field_tide_type": "Daily Predictions",
            "field_tide_prediction_file": "Again"},
           {"name": "Chatham Islands", "field_location": "-43.95,-176.56", "field_tide_type": "daily predictions",
            "field_tide_prediction_file": "Chatham"},
           "not a dict"]
    got = L.build_station_list(FakeGet({L.LIST_URL: json.dumps(doc).encode()}), check_year=None)
    assert [(s["id"], s["name"], s["local"]["file"]) for s in got] == [
        ("linz:akitio-river-entrance", "Ākitio River Entrance", "Akitio"),
        ("linz:chatham-islands", "Chatham Islands", "Chatham")]
    assert got[1]["lon"] == -176.56                               # east of the antimeridian stays negative


def test_station_list_failures_raise():
    with pytest.raises(TideFetchError):
        L.build_station_list(FakeGet({L.LIST_URL: RuntimeError("HTTP 500")}))
    with pytest.raises(TideFetchError):
        L.build_station_list(FakeGet({L.LIST_URL: b"<html>maintenance</html>"}))
    with pytest.raises(TideFetchError):
        L.build_station_list(FakeGet({L.LIST_URL: b'{"not": "a list"}'}))


def test_slug_and_urls():
    assert L.slug("Ōhope Wharf (Whakatāne)") == "ohope-wharf-whakatane"
    assert L.csv_url("Auckland", 2026) == ("https://static.charts.linz.govt.nz/tide-tables/maj-ports/csv/"
                                           "Auckland%202026.csv")
    assert L.page_url(AKL).startswith("https://www.linz.govt.nz/")


# ---------------------------------------------------------------------------------------------- the CSV

def test_parse_year_reads_the_whole_year_in_utc():
    with open(os.path.join(FIX, "auckland_2026.csv"), "rb") as f:
        ext = L.parse_year(f.read())
    assert 1400 <= len(ext) <= 1420
    # '1,Th,1,2026,05:47,3.1,11:51,0.9,...': NZDT (UTC+13)
    assert ext[0] == (_ep("2025-12-31 16:47"), 3.1, "H") and ext[1] == (_ep("2025-12-31 22:51"), 0.9, "L")
    # '29,Tu,12,2026,00:02,3.4,...' (NZDT again in December)
    assert (_ep("2026-12-28 11:02"), 3.4, "H") in ext
    # '4,Sa,7,2026,...' winter: NZST (UTC+12)
    jul = [e for e in ext if _ep("2026-07-03 12:00") <= e[0] < _ep("2026-07-04 12:00")]
    assert len(jul) in (3, 4)
    assert all(b[0] > a[0] for a, b in zip(ext, ext[1:]))
    assert all(a[2] != b[2] for a, b in zip(ext, ext[1:]))
    assert min(h for _, h, k in ext if k == "H") > max(h for _, h, k in ext if k == "L")


def test_missing_cells_and_bad_values_are_skipped():
    body = _csv([(1, 1, [("05:47", "3.1"), ("11:51", "0.9"), ("18:06", "3.1")]),
                 (1, 2, [("00:19", "0.6"), ("06:49", ""), ("25:00", "3.2"), ("12:51", "0.8")]),
                 (1, 3, [("01:19", "x"), ("07:48", "3.4"), ("13:50", "0.7"), ("20:10", "3.2")])])
    rows = L.parse_rows(body)
    assert [r[0].strftime("%d %H:%M") for r in rows] == ["01 05:47", "01 11:51", "01 18:06", "02 00:19", "02 12:51",
                                                         "03 07:48", "03 13:50", "03 20:10"]
    with pytest.raises(TideFetchError):
        L.parse_rows(b"<?xml version='1.0'?><Error><Code>AccessDenied</Code></Error>")
    with pytest.raises(TideFetchError):
        L.parse_rows(_csv([]))
    with pytest.raises(TideFetchError):
        L.parse_rows(b"\xff\xfe\x00garbage")


def test_kinds_follow_the_alternation_even_with_equal_heights():
    # Mana Marina's double highs: 1.2 H, 1.2 L, 1.2 H between ordinary tides (heights alone cannot say which)
    ext = [(0, 1.3), (1, 1.0), (2, 1.2), (3, 1.2), (4, 1.2), (5, 1.0), (6, 1.3), (7, 0.9)]
    assert [k for *_, k in L.kinds(ext)] == ["H", "L", "H", "L", "H", "L", "H", "L"]
    with pytest.raises(TideFetchError):
        L.kinds([(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0), (4, 5.0)])      # not alternating: not a table of extremes


# ---------------------------------------------------------------------------------------------- local time -> UTC

def _series(first_utc, n=12, step=6 * 3600 + 12 * 60 + 30):
    """True extremes every ~6 h 12.5 min, alternating 2.0 / 0.5 m; returned as (truth, rows of local wall time)."""
    truth = [(first_utc + i * step, 2.0 if i % 2 == 0 else 0.5) for i in range(n)]
    rows = [[datetime.fromtimestamp(t, L.TZ).replace(tzinfo=None, second=0), h] for t, h in truth]
    return [(t // 60 * 60, h) for t, h in truth], rows


@pytest.mark.parametrize("hit", ["2026-04-04 13:20", "2026-04-04 14:20", "2027-04-03 13:40", "2027-04-03 14:05"])
def test_the_repeated_autumn_hour_goes_where_the_rhythm_puts_it(hit):
    """5 April 2026 / 4 April 2027, NZDT -> NZST at 03:00: 02:00-02:59 happens twice. An extreme at 13:20 UTC is the
    first 02:20 (NZDT), at 14:20 UTC the second (NZST); both are read back exactly."""
    t0 = _ep(hit) - 5 * (6 * 3600 + 12 * 60 + 30)
    truth, rows = _series(t0)
    local = rows[5][0]
    assert local.hour == 2 and len(L._candidates(local)) == 2       # really ambiguous
    assert L._to_utc(rows) == truth


def test_spring_forward_and_ordinary_days():
    # 27 September 2026, NZST -> NZDT at 02:00 (the clock jumps to 03:00)
    truth, rows = _series(_ep("2026-09-26 06:00"))
    assert L._to_utc(rows) == truth
    truth, rows = _series(_ep("2026-11-26 06:00"))
    assert L._to_utc(rows) == truth
    # a time in the spring gap (LINZ prints none) is read as standard time
    gap = datetime(2026, 9, 27, 2, 30)
    assert L._candidates(gap) == [_ep("2026-09-26 14:30")]


def test_castlepoint_extreme_in_the_repeated_hour():
    """'5,Su,4,2026,02:28,0.3,07:38,1.4,...': the 02:28 low is the first one (NZDT, 13:28 UTC on the 4th): the lows
    either side are 12 h 24 min before (14:04 NZDT) and 12 h 20 min after (13:48 NZST)."""
    get = FakeGet()
    got = L.fetch(CPT, _ep("2026-04-04 00:00"), _ep("2026-04-06 00:00"), get)
    assert get.calls == [(L.csv_url("Castlepoint", 2026), L.HEADERS)]
    hilo = got["hilo"]
    assert (_ep("2026-04-04 07:16"), 1.4, "H") in hilo           # 20:16 NZDT on the 4th
    assert (_ep("2026-04-04 13:28"), 0.3, "L") in hilo           # 02:28 NZDT on the 5th
    assert (_ep("2026-04-04 19:38"), 1.4, "H") in hilo           # 07:38 NZST on the 5th
    assert (_ep("2026-04-05 01:48"), 0.3, "L") in hilo           # 13:48 NZST
    assert all(b[0] - a[0] > 5 * 3600 for a, b in zip(hilo, hilo[1:]))


# ---------------------------------------------------------------------------------------------- fetch

def test_fetch_across_the_new_year_reads_both_files():
    get = FakeGet()
    got = L.fetch(AKL, BEGIN, END, get)
    assert [u for u, _ in get.calls] == [L.csv_url("Auckland", 2026), L.csv_url("Auckland", 2027)]
    assert len(get.calls) <= L.MAX_REQUESTS
    assert got["curve"] == []
    hilo = got["hilo"]
    assert all(BEGIN <= t <= END for t, _, _ in hilo) and len(hilo) == 12
    assert hilo[0] == (_ep("2026-12-30 00:18"), 3.4, "H")          # '30,We,12,2026,...,13:18,3.4,...' NZDT
    assert (_ep("2026-12-31 07:24"), 0.7, "L") in hilo             # '31,Th,12,2026,...,20:24,0.7'
    assert (_ep("2026-12-31 13:42"), 3.0, "H") in hilo             # '1,Fr,1,2027,02:42,3.0': the 2027 file, 2026 UTC
    assert hilo[-1] == (_ep("2027-01-01 20:43"), 1.0, "L")         # '2,Sa,1,2027,...,09:43,1.0'
    assert all(a[2] != b[2] for a, b in zip(hilo, hilo[1:]))


def test_fetch_failures_raise():
    with pytest.raises(TideFetchError):                           # 2028 not in the fixtures: the 403 of a missing year
        L.fetch(AKL, _ep("2028-03-01 00:00"), _ep("2028-03-05 00:00"), FakeGet())
    with pytest.raises(TideFetchError):
        L.fetch(AKL, BEGIN, END, FakeGet({L.csv_url("Auckland", 2027): b"<html>Access denied</html>"}))
    with open(os.path.join(FIX, "auckland_2026.csv"), "rb") as f:
        y2026 = f.read()
    with pytest.raises(TideFetchError):                           # the 2027 URL answering the 2026 table
        L.fetch(AKL, BEGIN, END, FakeGet({L.csv_url("Auckland", 2027): y2026}))
    with pytest.raises(TideFetchError):                           # a truncated file
        L.fetch(AKL, BEGIN, END, FakeGet({L.csv_url("Auckland", 2027): y2026[:3000]}))
    with pytest.raises(TideFetchError):
        L.fetch({"id": "linz:x", "local": {}}, BEGIN, END, FakeGet())
    with pytest.raises(TideFetchError):
        L.fetch(AKL, END, BEGIN, FakeGet())


def test_fetch_refuses_a_table_that_does_not_cover_the_window(monkeypatch):
    monkeypatch.setattr(L, "MIN_YEAR_EXTREMES", 2)
    body = _csv([(12, 30, [("01:00", "3.0"), ("07:10", "0.5"), ("13:20", "3.1"), ("19:30", "0.6")])])
    get = FakeGet({L.csv_url("Auckland", 2026): body})
    with pytest.raises(TideFetchError):
        L.fetch(AKL, _ep("2026-12-29 00:00"), _ep("2026-12-30 06:00"), get)


def test_g76_chatham_islands_keep_their_own_clock():
    """The Chatham Islands' tables are in Chatham time (UTC+12:45 / +13:45): read as mainland NZ time they ran 45 min
    late against TICON (G76 re-check)."""
    from datetime import datetime
    local = datetime(2026, 11, 27, 12, 0)
    nz = L._to_utc([[local, 1.0]])[0][0]
    ch = L._to_utc([[local, 1.0]], L.station_zone({"id": "linz:waitangi-chatham-island", "tz": "Pacific/Chatham"}))[0][0]
    assert nz - ch == 45 * 60 and L.station_zone({"id": "linz:auckland", "tz": "Pacific/Auckland"}) is L.TZ
