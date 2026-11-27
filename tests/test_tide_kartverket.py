"""Plan section 112, step 3: the Kartverket reader (tide_providers/kartverket.py) on captured tideapi.php answers
(tests/capture_tide_chs_kartverket_fixtures.py)."""
import json
import os
import re
import sys
from datetime import datetime, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from tide_providers import TideFetchError, kartverket as kv  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "tides_national", "kartverket")
BEGIN = 1795651200                     # 2026-11-26 00:00 UTC (the capture's window: 2 days)
END = BEGIN + 2 * 86400
ID_RE = re.compile(r"[a-z]+:[a-z0-9][a-z0-9._-]{0,79}")
SVG = {"id": "kv:svg", "name": "Stavanger", "lat": 58.974339, "lon": 5.730121, "local": {"code": "SVG"}}


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


def _url_of(name):
    return next(u for u, f in FakeGet().index.items() if f == name)


# ---------------------------------------------------------------------------------------------- the station list

def test_station_list():
    get = FakeGet()
    got = kv.build_station_list(get)
    assert len(got) == 10
    assert get.calls[0] == (kv.STATIONLIST_URL, kv.HEADERS)
    assert get.calls[0][1]["User-Agent"] == "allshoresurf.com tide stations (https://allshoresurf.com)"
    by = {s["id"]: s for s in got}
    assert by["kv:bgo"] == {"id": "kv:bgo", "name": "Bergen", "lat": 60.398046, "lon": 5.320487,
                            "local": {"code": "BGO"}}
    assert by["kv:boo"]["name"] == "Bodø"                        # UTF-8 names intact
    for s in got:
        assert ID_RE.fullmatch(s["id"]) and s["id"] == "kv:" + s["local"]["code"].lower()
        json.dumps(s)
    assert [s["id"] for s in got] == sorted(s["id"] for s in got)


def test_station_list_leaves_out_bad_rows():
    xml = ('<tide><stationinfo>'
           '<location name="Good" code="AAA" latitude="60.1" longitude="5.2" type="PERM"/>'
           '<location name="Temp" code="BBB" latitude="60.1" longitude="5.2" type="TEMP"/>'
           '<location name="NoPos" code="CCC" latitude="" longitude="5.2" type="PERM"/>'
           '<location name="Off" code="DDD" latitude="91" longitude="5.2" type="PERM"/>'
           '<location name="Again" code="AAA" latitude="60.1" longitude="5.2" type="PERM"/>'
           '<location name="Long" code="ABCD" latitude="60.1" longitude="5.2" type="PERM"/>'
           '<location name="" code="EEE" latitude="60.1" longitude="5.2" type="PERM"/>'
           '<location name="lower" code="fff" latitude="60.1" longitude="5.2" type="perm"/>'
           '</stationinfo></tide>').encode()
    got = kv.build_station_list(FakeGet({kv.STATIONLIST_URL: xml}))
    assert [(s["id"], s["local"]["code"]) for s in got] == [("kv:aaa", "AAA"), ("kv:fff", "FFF")]


def test_page_url():
    assert kv.page_url(SVG) == ("https://www.kartverket.no/en/at-sea/se-havniva/result?latitude=58.974339"
                                "&longitude=5.730121")
    assert kv.page_url({"id": "kv:x"}) is None


# ---------------------------------------------------------------------------------------------- fetch on the capture

def test_fetch_stavanger():
    get = FakeGet()
    got = kv.fetch(SVG, BEGIN, END, get)
    assert len(get.calls) == 2 == kv.MAX_REQUESTS
    assert all(h == kv.HEADERS for _, h in get.calls)
    # Kartverket's own page (Se havniva, Stavanger) shows 26 Nov: high 106 cm, low 49 cm, high 104 cm, low 51 cm at
    # 10:22, 16:28, 22:49 UTC, 04:44 UTC on the 27th (shown in the viewer's zone)
    assert got["hilo"] == [(_ep("2026-11-26 04:01"), 0.494, "L"), (_ep("2026-11-26 10:22"), 1.058, "H"),
                           (_ep("2026-11-26 16:28"), 0.486, "L"), (_ep("2026-11-26 22:49"), 1.043, "H"),
                           (_ep("2026-11-27 04:44"), 0.513, "L"), (_ep("2026-11-27 11:08"), 1.049, "H"),
                           (_ep("2026-11-27 17:14"), 0.498, "L"), (_ep("2026-11-27 23:37"), 1.015, "H")]
    curve = got["curve"]
    ts = [t for t, _ in curve]
    assert ts[0] == BEGIN and ts[-1] == END and len(ts) == 2 * 144 + 1
    assert all(b - a == 600 for a, b in zip(ts, ts[1:]))
    assert curve[:2] == [(BEGIN, 0.88), (BEGIN + 600, 0.867)]           # 88.0 cm, 86.7 cm
    near = min(curve, key=lambda p: abs(p[0] - _ep("2026-11-26 10:22")))
    assert abs(near[1] - 1.058) < 0.02


def test_fetch_asks_utc_chart_datum_cm():
    get = FakeGet()
    kv.fetch(SVG, BEGIN, END, get)
    tab, pre = get.calls[0][0], get.calls[1][0]
    for u in (tab, pre):
        assert u.startswith("https://vannstand.kartverket.no/tideapi.php?tide_request=stationdata&stationcode=SVG&")
        assert "fromtime=2026-11-26T00%3A00&totime=2026-11-28T00%3A00" in u
        assert "&refcode=cd&" in u and "&tzone=0" in u and "&dst=0&" in u
    assert "datatype=tab" in tab and "interval" not in tab
    assert "datatype=pre" in pre and pre.endswith("&interval=10")


def test_stamp_covers_the_range():
    assert kv.stamp(BEGIN + 59) == "2026-11-26T00:00"
    assert kv.stamp(BEGIN + 1, up=True) == "2026-11-26T00:01"
    assert kv.stamp(BEGIN, up=True) == "2026-11-26T00:00"


def test_times_honour_the_offset():
    assert kv.parse_time("2026-11-26T04:01:00+00:00") == _ep("2026-11-26 04:01")
    assert kv.parse_time("2026-11-26T05:01:00+01:00") == _ep("2026-11-26 04:01")   # what tzone=1 sends
    with pytest.raises(ValueError):
        kv.parse_time("2026-11-26T04:01:00")


def test_cm_to_m_and_bad_rows():
    xml = ('<tide><stationdata><location name="S" code="SVG"><data type="prediction" unit="cm" reflevelcode="CD">'
           '<waterlevel value="123.4" time="2026-11-26T00:10:00+00:00" flag="pre"/>'
           '<waterlevel value="-5" time="2026-11-26T00:00:00+00:00" flag="pre"/>'
           '<waterlevel value="" time="2026-11-26T00:20:00+00:00" flag="pre"/>'
           '<waterlevel value="nan" time="2026-11-26T00:30:00+00:00" flag="pre"/>'
           '<waterlevel value="7" time="nonsense" flag="pre"/>'
           '</data><data type="observation" unit="cm"><waterlevel value="999" time="2026-11-26T00:40:00+00:00"/>'
           '</data></location></stationdata></tide>').encode()
    assert kv.parse_levels(xml, "SVG") == [(BEGIN, -0.05, "pre"), (BEGIN + 600, 1.234, "pre")]


# ---------------------------------------------------------------------------------------------- failures

def test_error_answers_raise():
    get = FakeGet()
    with pytest.raises(TideFetchError, match="No stations found"):
        kv.parse_levels(get(_url_of("error_code.xml")), "XXQ")
    with pytest.raises(TideFetchError, match="Invalid time"):
        kv.parse_levels(get(_url_of("error_time.xml")))
    with pytest.raises(TideFetchError):
        kv.fetch({"id": "kv:xxq", "local": {"code": "XXQ"}}, BEGIN, END, get)


@pytest.mark.parametrize("name", ["svg_tab.xml", "svg_pre.xml"])
def test_fetch_failures_raise(name):
    target = _url_of(name)
    other_station = open(os.path.join(FIX, name), "rb").read().replace(b'code="SVG"', b'code="BGO"')
    for body in (b"", b"not xml", b"<tide><stationdata/></tide>", other_station,
                 open(os.path.join(FIX, name), "rb").read().replace(b'unit="cm"', b'unit="m"')):
        with pytest.raises(TideFetchError):
            kv.fetch(SVG, BEGIN, END, FakeGet({target: body}))

    def get(url, headers=None, data=None):                           # a non-200 from the real get
        if url == target:
            raise RuntimeError("HTTP 500")
        return FakeGet()(url, headers)
    with pytest.raises(TideFetchError):
        kv.fetch(SVG, BEGIN, END, get)


def test_fetch_refuses_bad_input():
    with pytest.raises(TideFetchError):
        kv.fetch({"id": "kv:svg", "local": {}}, BEGIN, END, FakeGet())
    with pytest.raises(TideFetchError):
        kv.fetch(SVG, BEGIN, BEGIN + 367 * 86400, FakeGet())
    with pytest.raises(TideFetchError):
        kv.fetch(SVG, END, BEGIN, FakeGet())


def test_interface_constants():
    assert kv.PREFIX == "kv" and kv.NAME and kv.DATUM == "chart datum (= LAT)"
    assert "Norwegian Mapping Authority" in kv.CREDIT and "CC BY 4.0" in kv.CREDIT
    assert kv.MAX_REQUESTS == 2
