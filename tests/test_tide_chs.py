"""Plan section 112, step 3: the Canadian Hydrographic Service reader (tide_providers/chs.py) on captured IWLS answers
(tests/capture_tide_chs_kartverket_fixtures.py)."""
import json
import os
import re
import sys
from datetime import datetime, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from tide_providers import TideFetchError, chs  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "tides_national", "chs")
BEGIN = 1795651200                     # 2026-11-26 00:00 UTC (the capture's window: 2 days)
END = BEGIN + 2 * 86400
DAY = 86400
ID_RE = re.compile(r"[a-z]+:[a-z0-9][a-z0-9._-]{0,79}")
PA = {"id": "chs:07795", "name": "Point Atkinson", "lat": 49.3375, "lon": -123.253583,
      "local": {"uuid": "5cebf1de3d0f4a073c4bb94c", "code": "07795"}}


def _ep(s):
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())


class FakeGet:
    """get(url, headers=None, data=None) serving the captured answers by URL; anything else is a 404 (raises)."""

    def __init__(self, extra=None):
        with open(os.path.join(FIX, "index.json"), encoding="utf-8") as f:
            self.index = json.load(f)
        self.extra = extra or {}
        self.calls = []

    def __call__(self, url, headers=None, data=None):
        self.calls.append((url, headers))
        if url in self.extra:
            return self.extra[url]
        if url not in self.index:
            raise RuntimeError("HTTP 404 " + url)
        with open(os.path.join(FIX, self.index[url]), "rb") as f:
            return f.read()


@pytest.fixture
def day_chunks(monkeypatch):
    monkeypatch.setattr(chs, "CURVE_CHUNK_S", DAY)        # the capture's pieces


# ---------------------------------------------------------------------------------------------- the station list

def test_station_list_keeps_curve_and_hilo_stations_only():
    get = FakeGet()
    got = chs.build_station_list(get)
    assert [s["id"] for s in got] == ["chs:00490", "chs:01825", "chs:03104", "chs:04140", "chs:07795", "chs:08074"]
    by = {s["id"]: s for s in got}
    assert by["chs:07795"] == PA
    assert by["chs:03104"]["name"] == "Bouée K136"               # accents arrive intact
    assert "chs:03365" not in by and "chs:00560" not in by           # a curve without highs/lows; observations only
    for s in got:
        assert ID_RE.fullmatch(s["id"]) and len(s["id"]) <= 80
        assert isinstance(s["lat"], float) and isinstance(s["lon"], float)
        json.dumps(s)
    assert get.calls[0][1]["User-Agent"] == "allshoresurf.com tide stations (https://allshoresurf.com)"


def test_station_list_leaves_out_bad_rows():
    ts = [{"code": "wlp"}, {"code": "wlp-hilo"}]
    good = {"id": "5cebf1de3d0f4a073c4bb94c", "code": "07795", "officialName": "A", "latitude": 49.0,
            "longitude": -123.0, "timeSeries": ts}
    rows = [good,
            dict(good, code="07796", latitude=None),                 # no position
            dict(good, code="07797", latitude=95.0),                 # off the globe
            dict(good, code="07795"),                                # a second 07795
            dict(good, code="7798"),                                 # not five digits
            dict(good, code="07799", id="not-a-uuid"),
            dict(good, code="07800", officialName=" "),
            dict(good, code="07801", timeSeries=[{"code": "wlp-hilo"}]),
            "junk"]
    url = chs.API + "/stations"
    got = chs.build_station_list(FakeGet({url: json.dumps(rows).encode()}))
    assert [s["id"] for s in got] == ["chs:07795"]


def test_station_list_failures_raise():
    url = chs.API + "/stations"
    for body in (b"<html>", b'{"status": "500"}'):
        with pytest.raises(TideFetchError):
            chs.build_station_list(FakeGet({url: body}))

    def down(url, headers=None, data=None):
        raise RuntimeError("HTTP 503")
    with pytest.raises(TideFetchError):
        chs.build_station_list(down)


def test_page_url():
    assert chs.page_url(PA) == "https://tides.gc.ca/en/stations/07795"
    assert chs.page_url({"local": {}}) is None


# ---------------------------------------------------------------------------------------------- fetch on the capture

def test_fetch_point_atkinson(day_chunks):
    get = FakeGet()
    got = chs.fetch(PA, BEGIN, END, get)
    assert len(get.calls) == 3 <= chs.MAX_REQUESTS
    assert all(h["User-Agent"].startswith("allshoresurf.com") for _, h in get.calls)
    hilo = got["hilo"]
    # CHS's own page (tides.gc.ca/en/stations/07795, PT = UTC-7) lists 2026-11-26: 00:57 0.011, 08:41 4.927,
    # 13:43 3.743, 18:05 4.446
    assert hilo[:5] == [(_ep("2026-11-26 00:19"), 4.538, "H"), (_ep("2026-11-26 07:57"), 0.011, "L"),
                        (_ep("2026-11-26 15:41"), 4.927, "H"), (_ep("2026-11-26 20:43"), 3.743, "L"),
                        (_ep("2026-11-27 01:05"), 4.446, "H")]
    assert all(BEGIN <= t <= END for t, _, _ in hilo) and len(hilo) == 8
    assert [k for _, _, k in hilo] == ["H", "L"] * 4                 # mixed tide: highs and lows still alternate
    curve = got["curve"]
    ts = [t for t, _ in curve]
    assert ts[0] == BEGIN and ts[-1] == END and len(ts) == 2 * 96 + 1  # the shared midnight kept once
    assert all(b - a == 900 for a, b in zip(ts, ts[1:]))
    assert curve[0] == (BEGIN, 4.525)
    # the curve passes near each extreme (15-minute samples: within a few cm of the low at 07:57)
    near = min(curve, key=lambda p: abs(p[0] - _ep("2026-11-26 07:57")))
    assert abs(near[1] - 0.011) < 0.05


def test_fetch_asks_the_documented_urls(day_chunks):
    get = FakeGet()
    chs.fetch(PA, BEGIN, END, get)
    urls = [u for u, _ in get.calls]
    assert "time-series-code=wlp-hilo&from=2026-11-25T11%3A00%3A00Z&to=2026-11-28T13%3A00%3A00Z" in urls[0]
    assert "resolution" not in urls[0]
    assert urls[1].endswith("time-series-code=wlp&from=2026-11-26T00%3A00%3A00Z&to=2026-11-27T00%3A00%3A00Z"
                            "&resolution=FIFTEEN_MINUTES")
    assert "from=2026-11-27T00%3A00%3A00Z&to=2026-11-28T00%3A00%3A00Z" in urls[2]


# ---------------------------------------------------------------------------------------------- the 7-day split

def test_chunks_cover_the_range_in_seven_day_pieces():
    assert chs.CURVE_CHUNK_S == 7 * DAY
    b = BEGIN - DAY
    e = b + 34 * DAY                                                 # what the site asks
    got = chs.chunks(b, e)
    assert got == [(b, b + 7 * DAY), (b + 7 * DAY, b + 14 * DAY), (b + 14 * DAY, b + 21 * DAY),
                   (b + 21 * DAY, b + 28 * DAY), (b + 28 * DAY, e)]
    assert chs.chunks(b, b + 7 * DAY) == [(b, b + 7 * DAY)]
    assert chs.chunks(b, b + 7 * DAY + 1) == [(b, b + 7 * DAY), (b + 7 * DAY, b + 7 * DAY + 1)]
    assert chs.chunks(b, b + 3600) == [(b, b + 3600)]
    assert chs.MAX_REQUESTS == 6


def _synthetic(url, headers=None, data=None):
    """A CHS stand-in: a cosine curve every 15 min (or every extreme) between the URL's from and to."""
    q = dict(p.split("=", 1) for p in url.split("?", 1)[1].split("&"))
    a = chs.parse_time(q["from"].replace("%3A", ":"))
    b = chs.parse_time(q["to"].replace("%3A", ":"))
    import math
    if q["time-series-code"] == "wlp-hilo":
        half = 22357                                                 # M2 / 2
        t0 = (a // half + 1) * half
        rows = [{"eventDate": chs.iso(t), "value": 2.0 + (1.5 if (t // half) % 2 else -1.5)}
                for t in range(t0, b + 1, half)]
    else:
        rows = [{"eventDate": chs.iso(t), "value": 2.0 + 1.5 * math.cos(t / 44714.0 * 2 * math.pi)}
                for t in range(a, b + 1, 900)]
    return json.dumps(rows).encode()


def test_fetch_34_days_takes_five_curve_requests():
    calls = []

    def get(url, headers=None, data=None):
        calls.append(url)
        return _synthetic(url)
    b, e = BEGIN - DAY, BEGIN + 33 * DAY
    got = chs.fetch(PA, b, e, get)
    curve_urls = [u for u in calls if "time-series-code=wlp&" in u]
    assert len(calls) == 6 == chs.MAX_REQUESTS and len(curve_urls) == 5
    assert all("resolution=FIFTEEN_MINUTES" in u for u in curve_urls)
    froms = [chs.parse_time(re.search(r"from=([^&]+)", u).group(1).replace("%3A", ":")) for u in curve_urls]
    tos = [chs.parse_time(re.search(r"to=([^&]+)", u).group(1).replace("%3A", ":")) for u in curve_urls]
    assert froms[0] == b and tos[-1] == e and froms[1:] == tos[:-1]
    assert all(0 < t - f <= 7 * DAY for f, t in zip(froms, tos))
    ts = [t for t, _ in got["curve"]]
    assert ts[0] == b and ts[-1] == e and len(ts) == len(set(ts)) == 34 * 96 + 1
    assert got["hilo"] and all(b <= t <= e for t, _, _ in got["hilo"])


def test_fetch_refuses_a_span_beyond_max():
    with pytest.raises(TideFetchError):
        chs.fetch(PA, BEGIN, BEGIN + chs.MAX_SPAN_S + 1, _synthetic)
    with pytest.raises(TideFetchError):
        chs.fetch(PA, BEGIN, BEGIN, _synthetic)


# ---------------------------------------------------------------------------------------------- highs and lows

def test_classify_from_the_neighbours():
    t = [_ep("2026-11-26 00:00") + i * 22000 for i in range(6)]
    got = chs.classify([(t[0], 4.5), (t[1], 0.0), (t[2], 4.9), (t[3], 3.7), (t[4], 4.4), (t[5], 0.1)])
    assert [k for _, _, k in got] == ["H", "L", "H", "L", "H", "L"]     # 3.7: a low above most highs' neighbours
    # the ends compare with their one neighbour
    assert [k for _, _, k in chs.classify([(t[0], 0.2), (t[1], 3.0)])] == ["L", "H"]
    assert [k for _, _, k in chs.classify([(t[0], 3.0), (t[1], 0.2)])] == ["H", "L"]
    # unsorted input is sorted
    assert [k for _, _, k in chs.classify([(t[1], 0.2), (t[0], 3.0)])] == ["H", "L"]


def test_classify_ambiguous_extremes_alternate():
    t = [i * 22000 for i in range(5)]
    # 1.0 < 2.0 < 3.0: the middle one is neither; it follows the extreme before it (a low -> high)
    got = chs.classify([(t[0], 1.0), (t[1], 0.5), (t[2], 1.0), (t[3], 2.0), (t[4], 3.0)])
    assert [k for _, _, k in got] == ["H", "L", "H", "L", "H"]
    # a level start takes the opposite of the extreme after it
    got = chs.classify([(t[0], 1.0), (t[1], 1.0), (t[2], 0.0)])
    assert [k for _, _, k in got] == ["L", "H", "L"]
    assert chs.classify([]) == []
    with pytest.raises(TideFetchError):
        chs.classify([(t[0], 1.0)])                                  # a lone extreme cannot be told


# ---------------------------------------------------------------------------------------------- times, failures

def test_times_are_utc():
    assert chs.parse_time("2026-11-26T07:57:00Z") == _ep("2026-11-26 07:57")
    assert chs.parse_time("2026-11-26T00:57:00-07:00") == _ep("2026-11-26 07:57")
    with pytest.raises(ValueError):
        chs.parse_time("2026-11-26T07:57:00")
    assert chs.iso(BEGIN) == "2026-11-26T00:00:00Z"


def test_parse_series_skips_bad_rows_and_refuses_error_documents():
    body = json.dumps([{"eventDate": "2026-11-26T00:15:00Z", "value": 1.0},
                       {"eventDate": "2026-11-26T00:00:00Z", "value": "2.5"},
                       {"eventDate": "2026-11-26T00:30:00Z", "value": None},
                       {"eventDate": "garbage", "value": 1.0},
                       {"value": 1.0},
                       {"eventDate": "2026-11-26T00:45:00Z", "value": "NaN"}]).encode()
    assert chs.parse_series(body) == [(BEGIN, 2.5), (BEGIN + 900, 1.0)]
    err = json.dumps({"status": "400 BAD_REQUEST", "message": "Constraints not respected",
                      "errors": ["date interval should not be bigger than 7 days"]}).encode()
    with pytest.raises(TideFetchError, match="7 days"):
        chs.parse_series(err)
    for body in (b"", b"not json", b"\xff\xfe"):
        with pytest.raises(TideFetchError):
            chs.parse_series(body)


@pytest.mark.parametrize("which", ["hilo", "curve"])
def test_fetch_failures_raise(day_chunks, which):
    index = FakeGet().index
    target = next(u for u, f in index.items() if f == ("pa_hilo.json" if which == "hilo" else "pa_wlp_2.json"))
    for body in (b"[]", b"{}", b"<html>oops</html>"):
        with pytest.raises(TideFetchError):
            chs.fetch(PA, BEGIN, END, FakeGet({target: body}))

    def get(url, headers=None, data=None):                           # a non-200 from the real get
        if url == target:
            raise RuntimeError("HTTP 400")
        return FakeGet()(url, headers)
    with pytest.raises(TideFetchError):
        chs.fetch(PA, BEGIN, END, get)


def test_fetch_needs_the_uuid():
    with pytest.raises(TideFetchError):
        chs.fetch({"id": "chs:07795", "local": {"code": "07795"}}, BEGIN, END, FakeGet())


def test_interface_constants():
    assert chs.PREFIX == "chs" and chs.NAME and chs.DATUM == "chart datum (≈ LAT)"
    assert chs.CREDIT.startswith("This product is not to be used for navigation.")
    assert "allshoresurf.com" in chs.CREDIT and "Canadian Hydrographic Service" in chs.CREDIT
    assert isinstance(chs.MAX_REQUESTS, int) and chs.MAX_REQUESTS >= 1 + -(-34 * DAY // chs.CURVE_CHUNK_S)
