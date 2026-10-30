"""Plan section 67, the G44 review's fix round: each finding pinned (docs/reviews/wind-more-G44-adversarial.md), and the
reviewer's mutation survivors."""
import json
import os
import sys
from datetime import datetime, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools", "wind"))
import buoy_sources  # noqa: E402
import wind_more as M  # noqa: E402
import wind_sources as W  # noqa: E402

NOW = 1793394000.0                                   # 2026-10-30T23:00Z
FX = os.path.join(ROOT, "tests", "fixtures", "wind", "more")


def fx(name):
    with open(os.path.join(FX, name), encoding="utf-8") as f:
        return f.read()


class Fetch:
    def __init__(self, table):
        self.table, self.asked = dict(table), []

    def __call__(self, url, max_bytes, headers=None):
        self.asked.append(url)
        for k, v in self.table.items():
            if k in url:
                if isinstance(v, Exception):
                    raise v
                return v.encode("utf-8") if isinstance(v, str) else v
        raise IOError("HTTP 404")


@pytest.fixture
def clock(monkeypatch):
    t = {"now": NOW}
    monkeypatch.setattr(buoy_sources.time, "time", lambda: t["now"])
    return t


def _st(ids, **extra):
    return {i: dict({"id": i, "name": i, "lat": 1.0, "lon": 1.0}, **extra) for i in ids}


def _ts(s):
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp())


# ---------------------------------------------------------------------------------------------- P1-2 IPMA class 9

def test_ipma_class_9_is_north():
    feats = [{"properties": {"idEstacao": 1, "time": "2026-10-30T18:00:00", "intensidadeVento": 3.0, "idDireccVento": c}}
             for c in (9,)]
    assert M.parse_ipma_obs({"features": feats})["1"][0]["d"] == 0
    hist = {"2026-10-30T18:00": {"1": {"intensidadeVento": 1.7, "idDireccVento": 9}}}
    assert M.parse_ipma_history(hist)["1"][0]["d"] == 0
    assert M.parse_ipma_history({"2026-10-30T18:00": {"1": {"intensidadeVento": 1.7, "idDireccVento": 0}}})["1"][0]["d"] is None


# ---------------------------------------------------------------------------------------------- P2-1 hostile windows

HOSTILE = ["[1, 2]", '"x"', "42", "null", '{"data": [1]}', '{"features": [1]}', '{"station": [1]}',
           "[" * 5000 + "]" * 5000, "<html>503</html>", ""]
HIST = {"jma": "point/", "iem": "obhistory", "smhi": "latest-day", "fmi": "fmisid", "dmi": "stationId", "dwd": "BEOB"}


@pytest.mark.parametrize("src", sorted(HIST))
@pytest.mark.parametrize("body", HOSTILE)
def test_a_window_of_an_unexpected_shape_is_an_answer(src, body, clock):
    sid = src + ":X1"
    st = {sid: {"id": sid, "name": "n", "kind": "met", "src": src, "tz": "UTC", "net": "XX_RWIS"}}
    svc = W.WindHistory(st, Fetch({HIST[src]: body}), now=lambda: NOW)
    status, out = svc.history(sid)[:2]               # an exception here would be an HTTP 500 from the route
    assert status in ("ok", "error")
    if status == "error":
        assert out.get("error")


def test_history_shape_errors_are_wind_errors():
    with pytest.raises(W.WindError, match="unexpected shape|not JSON"):
        M.history("smhi", "1", {}, int(NOW) - 86400, Fetch({"latest-day": "[1, 2]"}), NOW)
    with pytest.raises(W.WindError, match="not JSON"):
        M.history("dmi", "1", {}, int(NOW) - 86400, Fetch({"stationId": "[" * 5000 + "]" * 5000}), NOW)


# ---------------------------------------------------------------------------------------------- P2-2 IEM local days

def test_iem_window_asks_the_stations_local_days():
    asked = []

    def fetch(url, cap, headers=None):
        asked.append(url)
        return b'{"data": []}'
    now = 1793415600.0                               # 2026-10-31T01:00Z = 2026-10-30 18:00 PDT
    M.history("iem", "ODT27", {"net": "OR_RWIS", "tz": "America/Los_Angeles"}, int(now) - 86400, fetch, now)
    assert sorted(u.split("date=")[1] for u in asked) == ["2026-10-29", "2026-10-30"]
    assert all("network=OR_RWIS" in u for u in asked)
    assert M.local_days(now - 86400, now, "Bad/Zone") == M.local_days(now - 86400, now, None)   # unknown -> UTC
    assert [d.isoformat() for d in M.local_days(now - 86400, now, "UTC")] == ["2026-10-30", "2026-10-31"]


def test_history_keeps_only_the_window():
    rows = [{"t": t} for t in (100, 200, 300)]
    orig = M._history
    try:
        M._history = lambda *a: list(rows)
        assert [r["t"] for r in M.history("dwd", "1", {}, 200, None, 400)] == [200, 300]
    finally:
        M._history = orig


# ---------------------------------------------------------------------------------------------- P3-3 a row ahead

def test_one_row_ahead_hides_only_itself(clock):
    now = clock["now"]

    class P(M.MoreProvider):
        source, key, series = "IPMA", "ipma", True

        def _readings(self):
            return {"A": [W.reading(now - 3600 * h, 3.0, None, 90) for h in range(1, 5)]
                    + [W.reading(now + 7200, 9.0, None, 90)]}
    p = P(_st(["A"]), Fetch({}))
    lst = p._fetch_stations()
    assert [e["local_id"] for e in lst] == ["A"] and lst[0]["latest_time"] == W.iso_z(now - 3600)
    assert [r["t"] for r in p.hist("A", 0)] == [now - 3600 * h for h in (4, 3, 2, 1)]


def test_merge_keeps_the_newer_reading(clock):
    now = clock["now"]
    seq = [[W.reading(now - 60, 5.0, None, 90)], [W.reading(now - 3000, 1.0, None, 90)]]

    class P(M.MoreProvider):
        source, key = "SMHI", "smhi"

        def _readings(self):
            return {"A": seq.pop(0)}
    p = P(_st(["A"]), Fetch({}))
    p._fetch_stations()
    assert p._fetch_stations()[0]["latest_time"] == W.iso_z(now - 60)      # an older answer never replaces a newer reading


# ---------------------------------------------------------------------------------------------- P3-4 / P3-8 paced feeds

def _paced(clock, one, n=4, workers=1, **kw):
    class P(M.PacedMore):
        source, key, per_refresh = "DWD", "dwd", n

        def one(self, u):
            return one(u)
    P.workers = workers
    return P(_st([chr(65 + i) for i in range(n)]), Fetch({}), **kw)


def test_a_refusal_keeps_what_answered_and_asks_the_rest_first(clock):
    calls = []

    def one(u):
        calls.append(u)
        if u == "C":
            raise IOError("HTTP 429")
        return {u: [W.reading(clock["now"] - 60, 3.0, None, 90)]}
    p = _paced(clock, one)
    with pytest.raises(RuntimeError, match="refused"):
        p._fetch_stations()
    assert sorted(p._kept) == ["A", "B"]             # A and B answered before the refusal: kept
    assert calls == ["A", "B", "C"]                  # nothing asked after the refusal (one worker)
    assert p._cursor == 2                            # C (refused) and D (not asked) first after the pause
    assert p.status()["paused_s"] > 0
    with pytest.raises(RuntimeError, match="paused"):
        p._fetch_stations()


def test_a_refresh_asks_nothing_new_after_its_time(clock):
    def one(u):
        clock["now"] += M.REFRESH_S / 2 + 1          # each request takes half the refresh's time
        return {u: [W.reading(clock["now"] - 60, 3.0, None, 90)]}
    p = _paced(clock, one)
    lst = p._fetch_stations()
    assert [e["local_id"] for e in lst] == ["A", "B"]
    assert p._cursor == 2                            # C and D first next time; no error, no pause
    assert p.status()["paused_s"] == 0


def test_every_request_failing_is_a_failed_refresh(clock):
    p = _paced(clock, lambda u: (_ for _ in ()).throw(IOError("HTTP 500")))
    with pytest.raises(RuntimeError, match="every request failed"):
        p._fetch_stations()


def test_eccc_time_bound_and_every_file_failing(clock):
    files = ["CAAA", "CBBB", "CCCC"]
    listing = "".join('<a href="%s-AUTO-swob.xml">%s-AUTO-swob.xml</a> 2026-10-30 22:%02d  10K\n' % (s, s, i)
                      for i, s in enumerate(files))
    swob = fx("swob_cweb_auto.xml")

    def fetch(url, cap, headers=None):
        if url.endswith("/latest/"):
            return listing.encode()
        clock["now"] += M.REFRESH_S + 1              # the first file outlasts the refresh's time
        return swob.encode()
    p = M.EcccProvider(_st(files), fetch)
    p.workers = 1
    p._readings()
    assert sorted(p._seen) == ["CAAA"]               # the other two were not asked: next time

    def failing(url, cap, headers=None):
        if url.endswith("/latest/"):
            return listing.encode()
        raise IOError("HTTP 500")
    q = M.EcccProvider(_st(files), failing)
    with pytest.raises(RuntimeError, match="every SWOB file failed"):
        q._readings()


# ---------------------------------------------------------------------------------------------- P3-5 empty answers

@pytest.mark.parametrize("cls,key", [(M.SmhiProvider, "latest-hour"), (M.DmiProvider, "metObs"),
                                     (M.IpmaProvider, "observations.json")])
@pytest.mark.parametrize("body", ["{}", "null", '{"error": "x"}'])
def test_an_answer_with_no_reading_is_a_failed_refresh(cls, key, body, clock):
    p = cls(_st(["1"]), Fetch({key: body}))
    out, ok, err = p._build_station_list()
    assert not ok and "no reading" in err


# ---------------------------------------------------------------------------------------------- P3-7 restart windows

def test_kept_window_right_after_a_restart(clock):
    sid = "eccc:CWEB"
    st = {sid: {"id": sid, "name": "n", "kind": "met", "src": "eccc", "tz": "UTC"}}
    svc = W.WindHistory(st, Fetch({}), now=lambda: NOW)
    svc.more_hist["eccc"] = lambda local, since: []
    svc.more_started["eccc"] = NOW - 600
    got = svc._build(sid, st[sid])
    assert got[0] == "ok" and got[2] == svc.error_ttl            # asked again in a minute: the feed may read it soon
    assert got[1]["note"] == W.MORE_RESTART_EMPTY and got[1]["partial"]
    svc.more_started["eccc"] = NOW - 2 * 86400                   # running a day: plainly no readings
    got = svc._build(sid, st[sid])
    assert len(got) == 2 and got[1]["note"] == W.NO_HISTORY
    svc.more_hist["eccc"] = lambda local, since: [W.reading(NOW - 600, 3.0, None, 90)]
    svc.more_started["eccc"] = NOW - 3600
    got = svc._build(sid, st[sid])
    assert len(got) == 2 and got[1]["note"] == W.MORE_RESTART_NOTE


def test_every_kept_source_is_kept():
    assert set(W.MORE_KEPT) == {"eccc", "ipma", "mei"}
    assert all(M.PROVIDERS[k].series for k in W.MORE_KEPT)
    assert not any(M.PROVIDERS[k].series for k in M.KEYS if k not in W.MORE_KEPT)


def test_agency_window_budget(clock):
    assert M.BUCKET_PER_MIN == 30
    sid = "dwd:10637"
    st = {sid: {"id": sid, "name": "n", "kind": "met", "src": "dwd", "tz": "UTC"}}
    svc = W.WindHistory(st, Fetch({"BEOB": fx("dwd_10637.csv")}), now=lambda: NOW)
    for _ in range(M.BUCKET_PER_MIN):
        svc._more("dwd", "10637", st[sid], int(NOW) - 86400, NOW)
    with pytest.raises(W.WindBusy):
        svc._more("dwd", "10637", st[sid], int(NOW) - 86400, NOW)


# ---------------------------------------------------------------------------------------------- P3-9 licences

def test_licence_wording_and_links(clock):
    for k in ("smhi", "fmi", "dmi", "dwd", "mei"):
        assert "CC BY 4.0" in W.ATTRIBUTION[k] and "converted" in W.ATTRIBUTION[k]
        assert W.LICENCE_URL[k] == W.CC_BY
        assert "CC BY 4.0" in M.SOURCES[k]["licence"]
    assert "converted and thinned" in W.ATTRIBUTION["jma"]
    sid = "dwd:10637"
    st = {sid: {"id": sid, "name": "n", "kind": "met", "src": "dwd", "tz": "UTC"}}
    out = W.WindHistory(st, Fetch({"BEOB": fx("dwd_10637.csv")}), now=lambda: NOW)._build(sid, st[sid])[1]
    assert out["licence_url"] == W.CC_BY
    sid = "iem:X"
    st = {sid: {"id": sid, "name": "n", "kind": "road", "src": "iem", "tz": "UTC", "net": "XX_RWIS"}}
    out = W.WindHistory(st, Fetch({"obhistory": '{"data": []}'}), now=lambda: NOW)._build(sid, st[sid])[1]
    assert "licence_url" not in out


# ---------------------------------------------------------------------------------------------- mutation survivors

def test_jma_compact_stamps_are_jst():
    assert M.jma_time("20261031061000") == _ts("2026-10-30T21:10:00")
    assert M.jma_time("2026-10-31T06:10:00+09:00") == _ts("2026-10-30T21:10:00")
    assert M.jma_map_name(_ts("2026-10-30T21:10:00")) == "20261031061000"


def test_jma_quasi_normal_values_are_kept():
    e = {"wind": [3.0, 1], "windDirection": [4, 1], "temp": [20.0, 1]}
    r = M._jma_reading(100, e)
    assert r["s"] == 3.0 and r["d"] == 90.0 and r["at"] == 20.0
    assert M._jma_reading(100, {"wind": [3.0, 5], "windDirection": [4, 1]}) is None or \
        M._jma_reading(100, {"wind": [3.0, 5], "windDirection": [4, 1]})["s"] is None


def test_smhi_times_are_milliseconds():
    doc = {"station": [{"key": "1", "value": [{"date": 1793394000000, "value": "4.0", "quality": "G"}]}]}
    assert M.parse_smhi_param(doc) == {"1": [(1793394000, 4.0)]}


def test_dmi_gust_and_direction_are_joined_right():
    s = {"1": [(100, 5.0, 0, 0)]}
    d = {"1": [(100, 270.0, 0, 0)]}
    g = {"1": [(100, 9.0, 0, 0)]}
    r = M.dmi_readings(s, d, g)["1"][0]
    assert (r["s"], r["g"], r["d"]) == (5.0, 9.0, 270.0)


def test_dwd_feed_keeps_the_newest_rows(clock):
    p = M.DwdProvider(_st(["10637"]), Fetch({"BEOB": fx("dwd_10637.csv")}))
    rows = M.parse_dwd_poi(fx("dwd_10637.csv"))
    got = p.one("10637")["10637"]
    assert got == rows[-3:] and got[-1]["t"] == max(r["t"] for r in rows)


def test_met_eireann_local_time_in_summer():
    doc = [{"date": "15-07-2026", "reportTime": "13:00", "windSpeed": 36, "windGust": 54, "windDirection": 270}]
    r = M.parse_mei_today(doc)[0]
    assert r["t"] == _ts("2026-07-15T12:00:00")      # IST = UTC+1 in summer
    assert abs(r["s"] - 10.0) < 1e-9 and r["d"] == 270


def test_iem_units_and_network_filter(clock):
    e = {"station": "X1", "utc_valid": "2026-10-30T22:00:00Z", "sknt": 10, "gust": 20, "drct": 90, "tmpf": 50,
         "dwpf": 41, "alti": 30.0}
    r = M._iem_reading(e)
    assert abs(r["s"] - 5.1) < 1e-9 and abs(r["at"] - 10.0) < 1e-9 and abs(r["dp"] - 5.0) < 1e-9
    assert r["p"] == 1015.9                         # inches of mercury x 33.8639, rounded
    doc = json.dumps({"data": [dict(e, station="X1"), dict(e, station="Y1")]})
    p = M.IemProvider({"X1": {"id": "X1", "net": "AA_RWIS", "lat": 1, "lon": 1},
                       "Y1": {"id": "Y1", "net": "BB_RWIS", "lat": 1, "lon": 1}}, Fetch({"network=AA_RWIS": doc}))
    assert sorted(p.one("AA_RWIS")) == ["X1"]        # Y1 belongs to another network's answer


def test_swob_quality_boundary():
    el = {"x": ("km/h", "36", 15), "y": ("km/h", "36", 14)}
    assert M._swob_value(el, ("x",), "speed") == pytest.approx(10.0)
    assert M._swob_value(el, ("y",), "speed") is None


# ---------------------------------------------------------------------------------------------- the snapshot rules

def test_snapshot_one_flag_per_canadian_airfield_and_ndbc_twins():
    import fetch_stations as FS
    with open(os.path.join(ROOT, "wind_stations.json"), encoding="utf-8") as f:
        rows = json.load(f)["stations"]
    eccc = [r for r in rows if r[5] == "eccc"]
    cy = [r for r in eccc if r[0].startswith("eccc:CY")]
    for c in cy:
        for r in eccc:
            if r is not c:
                assert FS.km_between(c[2], c[3], r[2], r[3]) > 1.5, (c[0], r[0])
    ndbc = {r[0][5:] for r in rows if r[5] == "ndbc"}
    for r in rows:
        if r[5] == "iem" and len(r[0]) == 7:
            assert not any(p + r[0][4:] in ndbc for p in "KPT"), r[0]


def test_snapshot_fixed_zones():
    import more_stations as MS
    with open(os.path.join(ROOT, "wind_stations.json"), encoding="utf-8") as f:
        rows = json.load(f)["stations"]
    for r in rows:
        if r[5] in MS.FIXED_TZ:
            assert r[6] == MS.FIXED_TZ[r[5]], r[0]
