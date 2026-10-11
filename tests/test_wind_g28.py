"""Wind stations (plan section 39), step 7a: the G28 review's server fixes, each pinned. No network: every fetch is a
fake over the captured fixtures (tests/fixtures/wind) or a hand-written answer."""
import json
import os
import sys
import threading
import time as _time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import buoy_sources as B  # noqa: E402
import tide_sources as T  # noqa: E402
import wind_sources as W  # noqa: E402
from test_wind_sources import STATIONS, Clock, FakeFetch, clock, first_t, fx, history_fetch  # noqa: E402,F401

def wx_requests(fetch):
    return sum(fetch.count("product=" + k) for k in ("air_temperature", "water_temperature", "air_pressure", "humidity"))


THROTTLE = json.dumps({"error": {"message": "Too many requests; please slow down"}}).encode()


def coops_rows_now():
    rows = W.parse_coops_wind(json.loads(fx("coops_24.json")))
    return rows[-1]["t"] + 60


# ------------------------------------------------------------------ A-1: a CO-OPS budget for the windows

def test_a1_a_gauge_window_takes_its_whole_coops_budget_first_or_answers_busy():
    fetch = history_fetch()
    budget = W.TokenBucket(rate=30, now=lambda: 0.0, burst=5)
    svc = W.WindHistory(STATIONS, fetch, now=Clock(coops_rows_now()), coops_bucket=budget)
    assert svc.history("coops:1612340")[0] == "ok"                      # wx "awp": 1 + 3 tokens
    assert fetch.count("1612340") == 4
    # one token left: Nawiliwili (wx "awph") needs 5 -> busy, and NOT ONE request went to NOAA
    assert svc.history("coops:1611400") == ("busy", {"error": W.BUSY})
    assert fetch.count("1611400") == 0
    assert svc.history("coops:1611400") == ("busy", {"error": W.BUSY}), "busy is never cached"


def test_a1_the_budget_is_shared_with_the_tide_windows(monkeypatch):
    budget = W.TokenBucket(rate=30, now=lambda: 0.0, burst=1)
    tide_calls = []

    def tide_fetch(url, max_bytes):
        tide_calls.append(url)
        raise AssertionError("no request may be sent without a token")

    tides = T.TideService({"1612340": {"id": "1612340", "type": "R", "lat": 21.3, "lon": -157.9}}, tide_fetch,
                          bucket=budget)
    assert budget.take()                                               # a wind window took the only token
    assert tides.forecast("1612340") == ("busy", {"error": T.BUSY}) and tide_calls == []
    assert A._TIDES is not None and A._COOPS_BUDGET.rate == W.COOPS_ON_DEMAND_PER_MIN
    assert A._COOPS_BUDGET.burst == W.COOPS_ON_DEMAND_BURST == 15 and W.COOPS_ON_DEMAND_PER_MIN == 30


def test_a1_ndbc_histories_have_a_budget_too():
    fetch = history_fetch()
    svc = W.WindHistory(STATIONS, fetch, now=Clock(first_t(fx("realtime2_51003.txt", "r")) + 60),
                        ndbc_bucket=W.TokenBucket(rate=1, now=lambda: 0.0))
    assert svc.history("ndbc:51003")[0] == "ok"
    assert svc.history("ndbc:HRRH1") == ("busy", {"error": W.BUSY}) and fetch.count("HRRH1") == 0
    assert W.WindHistory(STATIONS, fetch).ndbc_bucket.rate == W.NDBC_HISTORY_PER_MIN


def test_a1_token_bucket_burst_and_n_tokens():
    t = [0.0]
    b = W.TokenBucket(rate=60, now=lambda: t[0], burst=3)
    assert b.take(3) and not b.take() and not b.take(2)
    t[0] += 1.0                                                        # one a second
    assert b.take() and not b.take()
    t[0] += 100.0
    assert b.take(3) and not b.take(), "never more than the burst"
    assert not W.TokenBucket(rate=10, now=lambda: 0.0, burst=2).take(3), "n above the burst: never"


# ------------------------------------------------------------------ A-6: one build cap, the products together

def test_a6_points_tides_and_wind_histories_share_one_build_cap():
    assert A._POINT_BUILDS is A._REQUEST_BUILDS and A._REQUEST_BUILDS.n == 2
    svc_w, _ = A._winds()["svc"], None
    assert svc_w._builds is A._REQUEST_BUILDS and svc_w.coops_bucket is A._COOPS_BUDGET
    tides = A._tides()[0]
    assert tides._builds is A._REQUEST_BUILDS and tides.bucket is A._COOPS_BUDGET


def test_a6_a_held_cap_makes_the_other_service_answer_busy_quickly():
    cap = B.SharedCap(1)
    release = threading.Event()
    fetch = history_fetch()

    def slow(url, max_bytes, headers=None):
        release.wait(5)
        return fx("realtime2_51003.txt")

    fetch.table[W.NDBC_RT2_URL % "51003"] = slow
    wind = W.WindHistory(STATIONS, fetch, now=Clock(first_t(fx("realtime2_51003.txt", "r")) + 60), cap=cap, wait_s=0.2)
    tides = T.TideService({"1612340": {"id": "1612340", "type": "R"}}, lambda u, m: b"{}", cap=cap, wait_s=0.2)
    th = threading.Thread(target=wind.history, args=("ndbc:51003",))
    th.start()
    _time.sleep(0.1)
    t0 = _time.monotonic()
    assert tides.forecast("1612340") == ("busy", {"error": T.BUSY}) and _time.monotonic() - t0 < 1.0
    release.set()
    th.join(5)
    assert cap.acquire(blocking=False), "the slot is free again"
    cap.release()


def test_a6_a_gauges_weather_products_are_asked_together_under_one_deadline(monkeypatch):
    monkeypatch.setattr(W, "WX_DEADLINE_S", 0.3)
    fetch = history_fetch()
    gate = threading.Event()

    def slow_water(url, max_bytes, headers=None):
        gate.wait(3)
        return fx("coops_water_temperature.json")

    fetch.table[W.coops_url("1612340", product="water_temperature", range=24)] = slow_water
    svc = W.WindHistory(STATIONS, fetch, now=Clock(coops_rows_now()))
    t0 = _time.monotonic()
    status, p = svc.history("coops:1612340")
    took = _time.monotonic() - t0
    gate.set()
    assert status == "ok" and took < 1.5, took                         # not 3 s: the late product is left out
    assert "wt" not in p and "at" in p and "p" in p                    # the others in time are merged
    assert wx_requests(fetch) == 3


def test_a6_no_products_are_asked_without_wind_rows():
    fetch = history_fetch()
    fetch.table[W.coops_url("1612340", range=24)] = fx("coops_error.json")      # NOAA: no data for the wind
    svc = W.WindHistory(STATIONS, fetch, now=Clock(coops_rows_now()))
    status, p = svc.history("coops:1612340")
    assert status == "ok" and p["note"] == W.NO_HISTORY and wx_requests(fetch) == 0


def test_shared_cap_api_and_fork_reset():
    cap = B.SharedCap(2)
    assert cap.acquire(blocking=False) and cap.acquire(timeout=0.05) and not cap.acquire(timeout=0.05)
    B._reset_after_fork()
    assert cap.acquire(blocking=False), "a forked child gets a fresh cap"
    cap.release()


# ------------------------------------------------------------------ A-5: the wind feeds' own runner

def test_a5_wind_refreshes_go_to_their_own_runner():
    class Rec:
        def __init__(self):
            self.jobs = []

        def submit(self, fn, name):
            self.jobs.append(name)

    live, wind = Rec(), Rec()
    prev, prev_w = B.set_refresh_runner(live), W.set_wind_runner(wind)
    try:
        provs = W.make_providers(STATIONS, FakeFetch())
        for p in provs:
            assert p.refresh_runner() is wind
            p.schedule_refresh()
        assert len(wind.jobs) == 4 and live.jobs == []
        class Live(B.BuoyProvider):
            source = "X"
        Live().schedule_refresh()
        assert live.jobs == ["buoy-refresh-X"] and len(wind.jobs) == 4
    finally:
        B.set_refresh_runner(prev)
        W.set_wind_runner(prev_w)


def test_a5_the_default_wind_runner_and_its_fork_reset(monkeypatch):
    prev = W.set_wind_runner(None)
    try:
        monkeypatch.setenv("WIND_REFRESH_WORKERS", "9")
        r = W.get_wind_runner()
        assert isinstance(r, B.ThreadRunner) and r.workers == 4 and r is not B.get_refresh_runner()
        W._after_fork()
        monkeypatch.setenv("WIND_REFRESH_WORKERS", "x")
        r2 = W.get_wind_runner()
        assert r2 is not r and r2.workers == W.WIND_REFRESH_WORKERS == 2
    finally:
        W.set_wind_runner(prev)


def test_a5_healthz_reports_a_stuck_wind_runner_without_failing(monkeypatch):
    monkeypatch.setattr(A, "WIND_ENABLED", True)
    provs = W.make_providers(STATIONS, FakeFetch())
    svc = W.WindHistory(STATIONS, FakeFetch())
    monkeypatch.setitem(A._WINDS, "providers", provs)
    monkeypatch.setitem(A._WINDS, "svc", svc)
    monkeypatch.setitem(A._WINDS, "stations", STATIONS)
    runner = B.ThreadRunner(2)
    prev = W.set_wind_runner(runner)
    try:
        h = A._wind_health(_time.time())
        assert h["runner"]["workers"] == 2 and h["fetch_stuck"] is False
        for p in provs[:2]:
            monkeypatch.setattr(p, "status", lambda now=None, s=p.source: {"source": s, "version": 1,
                                                                           "in_flight_s": A.LIVE_FETCH_STUCK_SEC + 1})
        assert A._wind_health(_time.time())["fetch_stuck"] is True
    finally:
        W.set_wind_runner(prev)


# ------------------------------------------------------------------ A-2: the first refusal stops the slice

def refusing_fetch(code_at=1, code="403"):
    calls = []

    def f(url, max_bytes, headers=None):
        calls.append(url)
        if len(calls) >= code_at:
            raise IOError("HTTP %s from api.tidesandcurrents.noaa.gov" % code)
        return fx("coops_latest.json")
    return f, calls


def gauges(n):
    return {"G%03d" % i: {"id": "coops:G%03d" % i, "name": "g%d" % i, "lat": 20.0, "lon": -157.0 + i * 0.01}
            for i in range(n)}


def test_a2_a_403_stops_the_rest_of_the_slice(clock):
    fetch, calls = refusing_fetch(code_at=1)
    p = W.CoopsWindProvider(gauges(30), fetch, workers=1, per_refresh=24)
    with pytest.raises(RuntimeError, match="403"):
        p._fetch_stations()
    assert len(calls) == 1, "one request, not 24"
    assert p._cursor == 1, "the stations not asked go first after the pause"
    assert p.status()["paused_s"] > 0


def test_a2_with_two_workers_at_most_one_more_goes_out(clock):
    fetch, calls = refusing_fetch(code_at=3, code="429")
    p = W.NwsProvider({k: dict(v, id="nws:" + k) for k, v in gauges(30).items()}, fetch, workers=2, per_refresh=25)
    with pytest.raises(RuntimeError, match="429"):
        p._fetch_stations()
    assert 3 <= len(calls) <= 4 and p._cursor <= 4


# ------------------------------------------------------------------ A-4: only "no data was found" is no data

def test_a4_a_throttle_message_keeps_the_gauges_reading_and_counts(clock):
    table = {W.coops_url("1612340", date="latest"): fx("coops_latest.json")}
    fetch = FakeFetch(table)
    one = {"1612340": STATIONS["coops:1612340"]}
    p = W.CoopsWindProvider(one, fetch, workers=1, per_refresh=1)
    assert len(p._fetch_stations()) == 1
    table[W.coops_url("1612340", date="latest")] = THROTTLE
    fetch.table = table
    with pytest.raises(RuntimeError, match="every CO-OPS request failed"):     # the only gauge failed: keep-last
        p._fetch_stations()
    assert p.status()["readings"] == 1, "a passing message keeps the reading"
    table[W.coops_url("1612340", date="latest")] = fx("coops_error.json")      # "No data was found": dropped
    assert p._fetch_stations() == [] and p.status()["readings"] == 0
    assert W.coops_no_data("No data was found. This product may not be offered") and not W.coops_no_data("Too many")


def test_a4_a_throttle_message_in_a_history_is_an_error_and_tries_the_relay():
    fetch = history_fetch()
    fetch.table[W.coops_url("1612340", range=24)] = THROTTLE
    now = first_t(fx("realtime2_OOUH1.txt", "r")) + 60
    svc = W.WindHistory(STATIONS, fetch, now=Clock(now))
    status, p = svc.history("coops:1612340")
    assert status == "ok" and p["via"] == "ndbc" and p["note"] is None and len(p["t"]) > 100
    fetch.table[W.coops_url("1612401", range=24)] = THROTTLE                  # no relay: an error for a minute
    assert svc.history("coops:1612401") == ("error", {"error": W.UNAVAILABLE})
    assert svc._cache[("h", "coops:1612401")][0] <= now + svc.error_ttl


def test_a4_a_product_with_a_throttle_message_adds_nothing_and_is_logged(caplog):
    fetch = history_fetch()
    fetch.table[W.coops_url("1612340", product="air_temperature", range=24)] = THROTTLE
    svc = W.WindHistory(STATIONS, fetch, now=Clock(coops_rows_now()))
    with caplog.at_level("INFO", logger="wind_sources"):
        status, p = svc.history("coops:1612340")
    assert status == "ok" and "at" not in p and "wt" in p
    assert any("NOAA says Too many" in r.getMessage() for r in caplog.records)


# ------------------------------------------------------------------ B-4: NDBC keeps a station's reading

def latest_obs(lines):
    head = ("#STN       LAT      LON  YYYY MM DD hh mm WDIR WSPD  GST WVHT  DPD APD MWD   PRES  PTDY  ATMP  WTMP  DEWP  VIS   TIDE\n"
            "#text      deg      deg   yr mo dy hr mn degT  m/s  m/s    m  sec sec degT   hPa   hPa  degC  degC  degC  nmi     ft\n")
    return (head + "".join(lines)).encode()


def test_b4_a_wave_only_newest_line_keeps_the_buoys_reading(clock):
    t = _time.gmtime(clock.time() - 1800)
    stamp = "%04d %02d %02d %02d %02d" % (t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min)
    t2 = _time.gmtime(clock.time() - 600)
    stamp2 = "%04d %02d %02d %02d %02d" % (t2.tm_year, t2.tm_mon, t2.tm_mday, t2.tm_hour, t2.tm_min)
    wind = latest_obs(["51003  19.151 -160.617 %s  90  4.0  5.0   MM   MM  MM  MM     MM    MM    MM    MM    MM   MM     MM\n" % stamp])
    waves = latest_obs(["51003  19.151 -160.617 %s  MM   MM   MM  1.9  12  MM  MM     MM    MM    MM    MM    MM   MM     MM\n" % stamp2])
    fetch = FakeFetch({W.NDBC_LATEST_URL: wind})
    p = W.NdbcWindProvider({"51003": STATIONS["ndbc:51003"]}, fetch)
    first = p._fetch_stations()
    assert [e["local_id"] for e in first] == ["51003"] and first[0]["wind"] == [4.0, 5.0, 90]
    fetch.table[W.NDBC_LATEST_URL] = waves                                     # the newest line: waves only
    again = p._fetch_stations()
    assert [e["local_id"] for e in again] == ["51003"] and again[0]["wind"] == [4.0, 5.0, 90], "kept, not dropped"
    clock.now += W.NDBC_KEEP_S
    assert p._fetch_stations() == [], "dropped once it is NDBC_KEEP_S old"


def test_b4_an_older_line_never_replaces_a_newer_reading(clock):
    p = W.NdbcWindProvider({"51003": STATIONS["ndbc:51003"]}, FakeFetch())
    p._kept["51003"] = ({"t": int(clock.time()), "s": 9.0, "g": None, "d": 10}, None)
    old = _time.gmtime(clock.time() - 3600)
    stamp = "%04d %02d %02d %02d %02d" % (old.tm_year, old.tm_mon, old.tm_mday, old.tm_hour, old.tm_min)
    p._fetch = lambda *a, **k: latest_obs(["51003  19.151 -160.617 %s  90  4.0  5.0 MM MM MM MM MM MM MM MM MM MM MM\n" % stamp])
    assert p._fetch_stations()[0]["wind"] == [9.0, None, 10]


# ------------------------------------------------------------------ A-3 / A-8: METAR visibility and pressure

def test_a3_metar_visibility_keeps_its_or_more():
    assert W.metar_visibility("6+") == (9.7, True) and W.metar_visibility("10+") == (16.1, True)
    assert W.metar_visibility("1 1/2") == (None, False) or W.metar_visibility("1/2") == (0.8, False)
    assert W.metar_visibility("") == (None, False) and W.metar_visibility(None) == (None, False)
    assert W.metar_visibility_km("6") == 9.7 and W.metar_visibility_km("0") == 0.0, "0 km (fog) is a value (A-9 W03)"
    rows = W.parse_metar_history([{"obsTime": 1, "wdir": 90, "wspd": 5, "visib": "6+", "altim": 1013.2},
                                  {"obsTime": 2, "wdir": 90, "wspd": 5, "visib": "3", "slp": 1012.0}])
    assert rows[0]["vis"] == 9.7 and rows[0]["vg"] == 1 and rows[0]["p"] == 1013.2
    assert rows[1]["vis"] == 4.8 and "vg" not in rows[1] and rows[1]["p"] == 1012.0, "slp only without an altimeter"


def test_a3_conditions_take_the_or_more_from_the_visibility_row():
    rows = [{"t": 0, "vis": 9.7, "vg": 1}, {"t": 600, "vis": 4.8}]
    c = W.conditions(rows)
    assert c["vis"] == [600, 4.8] and "vg" not in c, "the older row's 'or more' is not the newest visibility's"
    c = W.conditions([{"t": 0, "vis": 4.8}, {"t": 600, "vis": 9.7, "vg": 1}])
    assert c["vis"] == [600, 9.7] and c["vg"] == [600, 1]


def test_a3_the_history_carries_vg_only_when_some_row_has_it():
    fetch = history_fetch()
    hist = W.parse_metar_history(json.loads(fx("metar_history.json")))
    svc = W.WindHistory(STATIONS, fetch, now=Clock(max(r["t"] for r in hist) + 60))
    p = svc.history("metar:PHNL")[1]
    assert p["vg"] and all(v == 1 for v in p["vg"]) and p["conditions"]["vg"][1] == 1      # "10+" everywhere
    svc = W.WindHistory(STATIONS, fetch, now=Clock(first_t(fx("realtime2_51003.txt", "r")) + 60))
    assert "vg" not in svc.history("ndbc:51003")[1]


# ------------------------------------------------------------------ A-10 server items and A-9 gaps

def test_a10_a_newer_relay_reading_wins_over_an_older_coops_one():
    st = {"coops:1612340": STATIONS["coops:1612340"]}
    coops = [{"id": "coops:1612340", "wind": [1.0, None, 90], "latest_time": W.iso_z(1000)}]
    relay = [{"id": "ndbc:OOUH1", "alias_of": "coops:1612340", "wind": [2.0, 3.0, 180], "latest_time": W.iso_z(2000)}]
    out = W.build_latest(st, {"coops": coops, "ndbc": relay}, 3000)
    assert out["rows"] == [["coops:1612340", 2000, 2.0, 3.0, 180]]
    relay[0]["latest_time"] = W.iso_z(500)
    assert W.build_latest(st, {"coops": coops, "ndbc": relay}, 3000)["rows"] == [["coops:1612340", 1000, 1.0, None, 90]]


def test_a10_an_intensity_sign_alone_is_no_weather():
    assert W.metar_weather_words("+", "SCT") == "Scattered clouds" and W.metar_weather_words("VC", None) is None
    assert W.metar_weather_words("+TSRA", None) == "heavy thunderstorm with rain"


def test_a10_a_buckets_lock_is_reset_after_a_fork():
    b = W.TokenBucket(rate=5)
    lock = b._lock
    W._after_fork()
    assert b._lock is not lock and b.take()


def test_a9_a_reported_humidity_is_never_overwritten():
    fetch = history_fetch()
    fetch.table[W.NWS_API + "/stations/001HE/observations?"] = fx("nws_history.json")
    nws = W.parse_nws_observations(json.loads(fx("nws_history.json")))
    reported = [r for r in nws if r.get("rh") is not None and r.get("at") is not None and r.get("dp") is not None]
    assert reported, "the fixture has rows with all three"
    svc = W.WindHistory(STATIONS, fetch, now=Clock(nws[0]["t"] + 60))
    p = svc.history("nws:001HE")[1]
    by_t = dict(zip(p["t"], p["rh"]))
    assert all(by_t[r["t"]] == r["rh"] for r in reported)


def test_a9_conditions_cut_at_three_hours_and_the_trend_window():
    rows = [{"t": 0, "at": 20.0, "p": 1010.0}, {"t": W.CONDITIONS_S + 60, "s": 1}]
    assert "at" not in W.conditions(rows) and W.conditions(rows)["trend"] is None
    t = 10 * 3600
    base = [{"t": t, "p": 1015.0}]
    assert W.conditions([{"t": t - W.TREND_S - 1800, "p": 1010.0}] + base)["trend"] == 5.0
    assert W.conditions([{"t": t - W.TREND_S - 1801, "p": 1010.0}] + base)["trend"] is None, "only within 30 min"


def test_a9_healthz_memo_complete_needs_every_feed(monkeypatch):
    monkeypatch.setattr(A, "WIND_ENABLED", True)
    provs = W.make_providers(STATIONS, FakeFetch())
    monkeypatch.setitem(A._WINDS, "providers", provs)
    monkeypatch.setitem(A._WINDS, "svc", W.WindHistory(STATIONS, FakeFetch()))
    monkeypatch.setitem(A._WINDS, "stations", STATIONS)
    monkeypatch.setattr(A, "_WIND_MEMO", dict(A._WIND_MEMO, key=(("NDBC", 1), ("METAR", None), ("COOPS", 2), ("NWS", 3))))
    assert A._wind_health(_time.time())["memo"]["complete"] is False
