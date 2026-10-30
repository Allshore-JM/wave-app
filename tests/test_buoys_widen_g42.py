"""Plan section 63, the G42 review's fixes: kept lists keep their windows, no parameter download on a visitor's
thread, one reading check (future rows, impossible values, GLOS time offsets), probes that never fail a list,
a stalled GLOS mirror hides nothing, capped and timed downloads, the CEFAS aliases, and the reviewer's mutants."""
import http.server
import os
import sys
import threading
import time

import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import buoy_sources as B  # noqa: E402
from test_buoys_widen import (CAPTURED, GLOS_PROBE_YES, IOOS_PROBE_YES, NO_MATCH, FakeHttp, Resp,  # noqa: E402
                              fixture, glos, glos_http, ioos, ioos_http, ndbc_http, ndbc_list, st)

UNKNOWN_DS = Resp('Error {\n    code=404;\n    message="Not Found: Currently unknown datasetID=obs_88";\n}\n', 404)


# ------------------------------------------------------------------ F1: a kept list keeps its windows

def test_glos_empty_refresh_keeps_the_markers_and_their_windows():
    http = glos_http()
    p, by, ok, err = glos(http=http)
    assert ok and "glos:45029" in by
    held = dict(p._dataset_by_id)
    http.routes[1] = ("obs-datasets.geojson", '{"type": "FeatureCollection", "features": []}')
    lst, ok, err = p._build_station_list()
    assert not ok and "nothing" in err
    assert p._dataset_by_id == held
    assert p.detail("45029")["latest"] is not None


def test_ioos_empty_refresh_keeps_the_markers_and_their_windows():
    http = ioos_http()
    p, by, ok, err = ioos(http=http)
    held = dict(p._ds)
    assert ok and held
    http.routes.insert(0, ("orderByMax", NO_MATCH))
    p._probe = {}
    lst, ok, err = p._build_station_list()
    assert not ok and "nothing" in err
    assert p._ds == held and p.detail("41065")["latest"] is not None


def test_glos_first_empty_list_is_an_ordinary_empty_list():
    p, by, ok, err = glos(http=glos_http(roster='{"type": "FeatureCollection", "features": []}'))
    assert ok and by == {}


# ------------------------------------------------------------------ F2: the parameter list

def test_glos_list_needs_its_parameter_map():
    p, by, ok, err = glos(http=glos_http(params=Resp("busy", 503)))
    assert not ok and "parameter" in err and by == {}


def test_glos_window_never_fetches_the_parameter_list():
    http = glos_http()
    p, by, ok, err = glos(http=http)
    p._params = {}
    n = sum("api/v1/parameters" in u for u in http.calls)
    p.detail("45029")
    p.detail("SPOT-1360")
    assert sum("api/v1/parameters" in u for u in http.calls) == n


def test_glos_empty_parameter_answer_keeps_the_good_map():     # reviewer's X16
    http = glos_http()
    p, by, ok, err = glos(http=http)
    good = dict(p._params)
    p._params_ts = 0
    http.routes[2] = ("api/v1/parameters", "[]")
    lst, ok, err = p._build_station_list()
    assert ok and p._params == good


# ------------------------------------------------------------------ F3: one reading check

def test_sane_reading_rules():
    now = CAPTURED
    base = {"time_utc": "2026-10-30T04:00:00Z", "hs_m": 1.0, "tp_s": 9.0, "dir_deg": 120.0, "sst_c": 15.0}
    assert B._sane_reading(dict(base), now) == base
    assert B._sane_reading(dict(base, time_utc="2026-10-30T05:30:00Z"), now) is None      # > 1 h ahead
    assert B._sane_reading(dict(base, time_utc="2026-10-30T05:20:00Z"), now) is not None
    for bad in (-5.0, 99.0, -9999.0, 9.96921e36, float("nan")):
        assert B._sane_reading(dict(base, hs_m=bad), now) is None
    o = B._sane_reading(dict(base, tp_s=0.0, dir_deg=400.0, sst_c=288.0), now)
    assert o["tp_s"] is None and o["dir_deg"] is None and o["sst_c"] is None and o["hs_m"] == 1.0
    o = B._sane_reading(dict(base, partitions=[{"hs_m": 99.0}, {"hs_m": 0.5, "period_s": 0.0, "dir_deg": -3.0}]), now)
    assert o["partitions"] == [{"hs_m": 0.5, "period_s": None, "dir_deg": None}]
    assert "partitions" not in B._sane_reading(dict(base, partitions=[{"hs_m": -1.0}]), now)
    assert B._sane_reading(dict(base, time_utc="garbage"), now) is None


def test_ndbc_latest_leaves_out_future_and_impossible_rows():
    rows = fixture("ndbc_latest_obs.txt").rstrip("\n").split("\n")
    rows.append("ZZZ01    40.000  -70.000  2031 10 30 03 00  MM    MM    MM  1.0   8   MM  MM     MM    MM    MM    MM    MM   MM     MM")
    rows.append("ZZZ02    40.000  -70.500  2026 10 30 03 00  MM    MM    MM 99.0   8   MM  MM     MM    MM    MM    MM    MM   MM     MM")
    rows.append("ZZZ03    40.000  -71.000  2026 10 30 03 00  MM    MM    MM  1.0  99   MM  MM     MM    MM    MM    MM    MM   MM     MM")
    table = B.parse_ndbc_latest_waves("\n".join(rows) + "\n", now=CAPTURED)
    assert "ZZZ01" not in table and "ZZZ02" not in table
    assert table["ZZZ03"]["obs"]["tp_s"] is None


def test_ndbc_realtime_future_row_never_becomes_the_newest():
    lines = fixture("ndbc_realtime2_44137.txt").split("\n")
    lines.insert(2, "2027 10 30 04 00 220  7.0  9.0 99.0    5    MM  MM 1019.5  15.2  14.2    MM   MM -2.0    MM")
    obs = B.parse_ndbc_realtime_waves(lines, now=CAPTURED)
    assert obs[-1]["time_utc"] == "2026-10-30T04:00:00Z" and len(obs) > 10


def test_glos_timestamps_with_an_offset_are_read_as_utc():
    f = B._glos_naive_utc
    assert f("2026-10-30T01:20:00-04:00") == "2026-10-30T05:20:00"
    assert f("2026-10-30T05:20:00Z") == "2026-10-30T05:20:00"
    assert f("2026-10-30T05:20:00.500") == "2026-10-30T05:20:00"
    assert f("2026-10-30T05:20:00") == "2026-10-30T05:20:00"
    assert f("not a time") is None


def test_glos_window_drops_future_and_impossible_heights():
    p, by, ok, err = glos()
    hs_id = [k for k, v in p._params.items() if v[0] == B.GlosProvider.HS][0]
    answer = [{"obs_dataset_id": 3, "parameters": [{"parameter_id": hs_id, "observations": [
        {"timestamp": "2026-10-30T03:00:00", "value": 1.2},
        {"timestamp": "2026-10-30T03:10:00", "value": -9999},
        {"timestamp": "2030-10-30T00:20:00", "value": 1.5},
        {"timestamp": "2026-10-29T23:20:00-04:00", "value": 1.1}]}]}]
    obs = p._obs_from(answer)
    assert [o["time_utc"] for o in obs] == ["2026-10-30T03:00:00Z", "2026-10-30T03:20:00Z"]


def test_ioos_rows_in_the_future_impossible_or_without_a_time_left_out():    # reviewer's X21
    p, by, ok, err = ioos()
    head = fixture("ioos_cap2wave.csv").splitlines()[0].split(",")
    hi = {c: i for i, c in enumerate(head)}

    def row(t, hs):
        r = ["NaN"] * len(head)
        r[hi["time"]] = t
        r[hi[B.IoosSensorsProvider.HS]] = hs
        return r
    assert p._obs(row("2026-10-30T03:00:00Z", "1.2"), hi)["hs_m"] == 1.2
    assert p._obs(row("2099-01-01T00:00:00Z", "1.2"), hi) is None
    assert p._obs(row("2026-10-30T03:00:00Z", "-999"), hi) is None
    assert p._obs(row("2026-10-30T03:00:00Z", "9.96921e+36"), hi) is None
    assert p._obs(row("yesterday-Z", "1.2"), hi) is None
    assert p._obs(row("2026-10-30T03:00:00", "1.2"), hi) is None


def test_ioos_empty_partitions_never_added():                       # reviewer's X33
    p, by, ok, err = ioos()
    head = ["time", B.IoosSensorsProvider.HS, "sea_surface_swell_wave_significant_height",
            "sea_surface_swell_wave_period"]
    hi = {c: i for i, c in enumerate(head)}
    o = p._obs(["2026-10-30T03:00:00Z", "1.2", "NaN", "NaN"], hi)
    assert "partitions" not in o
    o = p._obs(["2026-10-30T03:00:00Z", "1.2", "0.8", "11"], hi)
    assert o["partitions"] == [{"label": "Swell", "hs_m": 0.8, "period_s": 11.0, "dir_deg": None}]


def test_probe_ignores_future_and_impossible_rows():
    def f(body):
        return B._erddap_wave_newest(FakeHttp([("x", body)]), "http://x", 5, now=CAPTURED)
    future = "time,sea_surface_wave_significant_height\nUTC,m\n2031-01-01T00:00:00Z,1.0\n"
    assert f(future) == ("no", None)
    mixed = ("time,sea_surface_wave_significant_height\nUTC,m\n2026-10-30T01:00:00Z,1.0\n"
             "2026-10-30T02:00:00Z,-999\n2031-01-01T00:00:00Z,1.0\n")
    assert f(mixed) == ("yes", "2026-10-30T01:00:00Z")


# ------------------------------------------------------------------ F4, F6, F7: probes never fail a list

def test_probe_unreadable_body_is_unknown_and_the_list_goes_on():
    big = "time,sea_surface_wave_significant_height\nUTC,m\n\"" + "x" * 200000 + "\",1\n"
    assert B._erddap_wave_newest(FakeHttp([("x", big)]), "http://x", 5) == ("unknown", None)
    p, by, ok, err = ioos(http=ioos_http(probe=big))
    assert ok and "ioos:41065" in by


def test_probe_a_raising_answer_is_unknown():
    class Boom(FakeHttp):
        def get(self, url, timeout=None, **kw):
            raise KeyError("odd")
    res = B._wave_probes(Boom([]), {"a": "http://a", "b": "http://b"}, 5)
    assert res == {"a": ("unknown", None), "b": ("unknown", None)}


def test_probe_other_404_is_unknown():
    assert B._erddap_wave_newest(FakeHttp([("x", UNKNOWN_DS)]), "http://x", 5) == ("unknown", None)
    assert B._erddap_wave_newest(FakeHttp([("x", NO_MATCH)]), "http://x", 5) == ("no", None)


def test_probe_batch_deadline():
    class Slow(FakeHttp):
        def get(self, url, timeout=None, **kw):
            time.sleep(1.5)
            return Resp(IOOS_PROBE_YES)
    t0 = time.monotonic()
    res = B._wave_probes(Slow([]), {"a": "http://a", "b": "http://b"}, 5, deadline_s=0.2)
    assert time.monotonic() - t0 < 1.0
    assert res == {"a": ("unknown", None), "b": ("unknown", None)}


def test_probe_byte_cap():                                            # reviewer's X24
    body = "time,sea_surface_wave_significant_height\nUTC,m\n" + "2026-10-30T01:00:00Z,1.0\n" * 50000
    assert len(body) > B.PROBE_CAP
    assert B._erddap_wave_newest(FakeHttp([("x", body)]), "http://x", 5) == ("unknown", None)


def test_probe_urls_look_back_a_few_days():                           # reviewer's X37
    assert "now-3days" in B.GlosProvider.PROBE_URL and "now-4days" in B.IoosSensorsProvider.PROBE_SUFFIX


# ------------------------------------------------------------------ F5: a stalled mirror judges nothing

def test_glos_stalled_mirror_lists_every_activated_platform_and_asks_no_probe():
    old = "datasetID,maxTime\n,UTC\n" + "".join("obs_%d,2026-10-26T00:00:00Z\n" % i for i in (2, 3, 88, 139, 260))
    http = glos_http(mirror=old, probe=NO_MATCH)
    p, by, ok, err = glos(http=http)
    assert ok and "glos:45204" in by and "glos:45029" in by
    assert not any("tabledap/obs_" in u for u in http.calls)


def _many(n):
    return {"L%d" % i: 1000 + i for i in range(n)}


def test_glos_probe_batch_of_nearly_all_no_is_not_believed():
    p = B.GlosProvider(http=FakeHttp([("tabledap/obs_", NO_MATCH)]))
    p.clock = staticmethod(lambda: CAPTURED)
    assert p._wave_verdicts(_many(12)) == set()
    assert not any(v[1] == "no" for v in p._probe.values())


def test_glos_probe_batch_with_some_no_leaves_those_out():
    def probe(url):
        return NO_MATCH if any("obs_%d." % (1000 + i) in url for i in (0, 1, 2)) else GLOS_PROBE_YES
    p = B.GlosProvider(http=FakeHttp([("tabledap/obs_", probe)]))
    p.clock = staticmethod(lambda: CAPTURED)
    assert p._wave_verdicts(_many(12)) == {"L0", "L1", "L2"}


def test_glos_quiet_platform_window_starts_before_its_last_wave_height():   # F11
    old_yes = "time,sea_surface_wave_significant_height\nUTC,m\n2026-10-27T00:00:00Z,0.4\n"
    http = glos_http(probe=old_yes)
    p, by, ok, err = glos(http=http)
    p.detail("45013")
    assert any("obsDatasetId=2&startDate=2026-10-26" in u for u in http.calls)


# ------------------------------------------------------------------ F8: capped, timed downloads (a real socket)

class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path == "/big":
            body = b"x" * 300000
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/nolen":
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for _ in range(40):
                chunk = b"y" * 10000
                self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
            self.wfile.write(b"0\r\n\r\n")
        elif self.path == "/slow":
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            try:
                for _ in range(30):
                    self.wfile.write(b"1\r\nz\r\n")
                    self.wfile.flush()
                    time.sleep(0.1)
                self.wfile.write(b"0\r\n\r\n")
            except OSError:
                pass
        else:
            body = b"hello, world"
            self.send_response(404 if self.path == "/missing" else 200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)


@pytest.fixture
def server():
    s = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    th = threading.Thread(target=s.serve_forever, daemon=True)
    th.start()
    yield "http://127.0.0.1:%d" % s.server_address[1]
    s.shutdown()
    s.server_close()


def test_capped_fetch_on_a_real_socket(server):
    sess = requests.Session()
    r = B._capped_fetch(sess, server + "/ok", 5, 1000)
    assert r.status_code == 200 and r.text == "hello, world"
    assert B._capped_fetch(sess, server + "/missing", 5, 1000, one_shot=True).status_code == 404
    with pytest.raises(RuntimeError, match="announced"):
        B._capped_fetch(sess, server + "/big", 5, 1000)
    with pytest.raises(RuntimeError, match="more than"):
        B._capped_fetch(sess, server + "/nolen", 5, 50000)
    t0 = time.monotonic()
    with pytest.raises(RuntimeError, match="longer"):
        B._capped_fetch(sess, server + "/slow", 5, 1000, max_s=0.5)
    assert time.monotonic() - t0 < 2.0
    with pytest.raises(RuntimeError, match="HTTP 404"):
        B._size_capped_get(sess, server + "/missing", 5, 1000)


def test_ndbcw_window_history_size_cap():                             # reviewer's X09
    big = fixture("ndbc_realtime2_44137.txt") + ("#" + "x" * 100 + "\n") * 45000
    p, by, ok, err = ndbc_list(http=ndbc_http(rt=big))
    d = p.detail("44137")
    assert d["recent"] == [] and d["latest"]["hs_m"] == 0.9


# ------------------------------------------------------------------ F10, F13, F14, F15, F16

def test_cefas_alias_merges_with_ndbcs_number():
    cef = st("CEFAS", "10", 53.0, 1.80)
    nd = st("NDBCW", "62144", 53.05, 1.85)                            # ~6.6 km apart: the Clipper platform
    kept = B.merge_stations([[cef], [nd]])
    assert [k["id"] for k in kept] == ["cefas:10"] and kept[0]["also_sources"] == ["NDBCW"]
    assert B._buoy_number(st("CEFAS", "11", 0, 0)) == ""


def test_glos_positions_out_of_range_left_out():
    roster = fixture("glos_roster.geojson").replace("[-87.37195, 46.59825]", "[-87.37195, 4659.825]", 1)
    p, by, ok, err = glos(http=glos_http(roster=roster))
    assert ok and "glos:SPOT-1360" not in by


def test_ndbcw_window_of_an_unlisted_id_asks_nothing():
    http = ndbc_http()
    p, by, ok, err = ndbc_list(http=http)
    n = len(http.calls)
    assert p.detail("99999") == {"latest": None, "recent": []}
    assert len([u for u in http.calls[n:] if "realtime2" in u]) == 0


def test_ndbcw_placeholder_period_rows_left_out():
    rows = fixture("ndbc_latest_obs.txt").rstrip("\n").split("\n")
    rows.append("22109    35.000   129.000  2026 10 30 03 00 270   5.0    MM  0.5   0   MM  MM     MM    MM  17.4  19.3    MM   MM     MM")
    p, by, ok, err = ndbc_list(http=ndbc_http(latest="\n".join(rows) + "\n"))
    assert ok and "ndbcw:22109" not in by and "ndbcw:44137" in by


def test_ndbcw_names_trimmed():
    p, by, ok, err = ndbc_list(names={"44137": "  East Scotia Slope   "})
    assert by["ndbcw:44137"]["name"] == "East Scotia Slope"


def test_licence_labels_name_the_operators_terms():
    assert "operators' terms" in B.NdbcBulkProvider.license_label
    assert "operator" in B.IoosSensorsProvider.license_label
    assert "Free to use and redistribute" not in B.IoosSensorsProvider._attribution({"institution": "X"})


# ------------------------------------------------------------------ the reviewer's other survivors

def test_ndbc_cells_nan_and_short_rows():                             # X01, X02
    assert B._ndbc_num("nan") is None and B._ndbc_num("inf") is None and B._ndbc_num("1.5") == 1.5
    rows = fixture("ndbc_latest_obs.txt").rstrip("\n").split("\n")
    rows.append("ZZZ09    40.000  -70.000  2026 10 30 03 00  MM    MM    MM  1.0")
    assert "ZZZ09" not in B.parse_ndbc_latest_waves("\n".join(rows) + "\n", now=CAPTURED)


def test_detail_cache_ttl_and_size(monkeypatch):                     # X06, X07
    now = [1000.0]
    monkeypatch.setattr(B.time, "time", lambda: now[0])
    c = B._DetailCache(300, size=2)
    c.put("a", 1)
    now[0] += 299
    assert c.get("a") == 1
    now[0] += 2
    assert c.get("a") is None
    c.put("a", 1)
    c.put("b", 2)
    c.put("c", 3)
    assert c.get("a") is None and c.get("b") == 2 and c.get("c") == 3


def test_ioos_dataset_ids_that_could_break_the_url_left_out():       # X17
    bad_id = "cap2wave-capers-nearshore-wave/../../x?y"
    idx = fixture("ioos_index.csv").replace("cap2wave-capers-nearshore-wave", bad_id)
    live = fixture("ioos_alldatasets.csv").replace("cap2wave-capers-nearshore-wave", bad_id)
    p, by, ok, err = ioos(http=ioos_http(index=idx, live=live))
    assert ok and all("/" not in d["dataset"] and "?" not in d["dataset"] for d in p._ds.values())


def test_ioos_window_asks_the_qc_column_when_the_dataset_has_it():   # X22
    p = B.IoosSensorsProvider(http=FakeHttp([]))
    cols = p._columns([B.IoosSensorsProvider.HS, B.IoosSensorsProvider.HS + "_qc_agg", "time"])
    assert B.IoosSensorsProvider.HS + "_qc_agg" in cols
    assert B.IoosSensorsProvider.HS + "_qc_agg" not in p._columns([B.IoosSensorsProvider.HS, "time"])
