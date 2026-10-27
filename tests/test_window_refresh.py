"""Plan section 56, step 1: the station windows' refresh button on the server. A request with the header
X-Allshore-Refresh: 1 rebuilds a per-station answer the server cached at least REFRESH_MIN_AGE_S (60 s) ago; a
younger one is served; a refresh that fails leaves the good answer for everyone; without the header nothing changes.
The four places: the wind history (WindHistory), the tide water level (TideService.observed), the NDBC spectra
(NDBC_COMPONENT_CACHE) and the AODN spectra (AODNProvider.spectrum). Nothing reaches the network."""
import os
import sys
import threading
import time

import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import buoy_sources as B  # noqa: E402
import tide_sources as T  # noqa: E402
import wind_sources as W  # noqa: E402
import test_tides as TT  # noqa: E402
import test_wind_sources as TW  # noqa: E402

FRESH = A.REFRESH_MIN_AGE_S
HDR = {A.REFRESH_HEADER: "1"}


def test_the_rule_is_one_minute_and_one_header():
    assert A.REFRESH_HEADER == "X-Allshore-Refresh" and A.REFRESH_MIN_AGE_S == 60
    with A.app.test_request_context(headers=HDR):
        assert A._refresh_s() == 60
    for value in ("0", "", "true", "yes", "2"):
        with A.app.test_request_context(headers={A.REFRESH_HEADER: value}):
            assert A._refresh_s() is None, value
    with A.app.test_request_context():
        assert A._refresh_s() is None


# ------------------------------------------------------------------ the wind history

def wind_service():
    fetch = TW.history_fetch()
    clock = TW.Clock(TW.first_t(TW.fx("realtime2_51003.txt", "r")) + 60)
    return W.WindHistory(TW.STATIONS, fetch, now=clock), fetch, clock


def test_wind_refresh_rebuilds_an_answer_a_minute_old_and_serves_a_younger_one():
    svc, fetch, clock = wind_service()
    first = svc.history("ndbc:51003")
    assert first[0] == "ok" and fetch.count("51003") == 1
    clock.t += FRESH - 1
    assert svc.history("ndbc:51003", FRESH)[1] is first[1] and fetch.count("51003") == 1   # 59 s: served
    clock.t += 1
    again = svc.history("ndbc:51003", FRESH)
    assert again[0] == "ok" and again[1] is not first[1] and fetch.count("51003") == 2    # 60 s: asked again
    assert svc.history("ndbc:51003", FRESH)[1] is again[1] and fetch.count("51003") == 2   # a second click: served
    assert svc.history("ndbc:51003")[1] is again[1]                                       # everyone gets the new one
    clock.t += FRESH
    assert svc.history("ndbc:51003")[1] is again[1] and fetch.count("51003") == 2          # no header: the 10-min rule


def test_wind_without_the_header_nothing_changes():
    svc, fetch, clock = wind_service()
    svc.history("ndbc:51003")
    clock.t += svc.ok_ttl - 1
    svc.history("ndbc:51003")
    assert fetch.count("51003") == 1
    clock.t += 2
    svc.history("ndbc:51003")
    assert fetch.count("51003") == 2


def test_wind_a_failed_refresh_keeps_the_good_answer_and_counts_for_the_floor():
    svc, fetch, clock = wind_service()
    good = svc.history("ndbc:51003")
    clock.t += FRESH
    fetch.table[W.NDBC_RT2_URL % "51003"] = IOError("NDBC down")
    status, payload = svc.history("ndbc:51003", FRESH)
    assert status == "error" and payload["error"] == W.UNAVAILABLE and fetch.count("51003") == 2
    assert svc.history("ndbc:51003") == good                                  # everyone else: the good answer
    clock.t += FRESH - 1
    assert svc.history("ndbc:51003", FRESH) == good and fetch.count("51003") == 2   # the failed try counts: served
    fetch.table[W.NDBC_RT2_URL % "51003"] = TW.fx("realtime2_51003.txt")
    clock.t += 1
    assert svc.history("ndbc:51003", FRESH)[0] == "ok" and fetch.count("51003") == 3


def test_wind_a_busy_refresh_is_not_counted_and_never_replaces_the_answer():
    svc, fetch, clock = wind_service()
    good = svc.history("ndbc:51003")
    clock.t += FRESH

    def busy():
        raise W.WindBusy("budget")
    assert svc._answer(("h", "ndbc:51003"), busy, FRESH)[0] == "busy"
    assert svc.history("ndbc:51003") == good
    assert svc.history("ndbc:51003", FRESH)[0] == "ok" and fetch.count("51003") == 2   # asked again at once


def test_wind_four_refreshes_at_once_ask_the_agency_once():
    gate = threading.Event()
    body = TW.fx("realtime2_51003.txt")

    def slow(url, max_bytes, headers=None):
        gate.wait(5)
        return body
    svc, fetch, clock = wind_service()
    svc.history("ndbc:51003")
    fetch.table[W.NDBC_RT2_URL % "51003"] = slow
    svc.wait_s = 5.0
    clock.t += FRESH
    out = []
    ts = [threading.Thread(target=lambda: out.append(svc.history("ndbc:51003", FRESH))) for _ in range(4)]
    for th in ts:
        th.start()
    time.sleep(0.2)
    gate.set()
    for th in ts:
        th.join(5)
    assert fetch.count("51003") == 2 and len(out) == 4 and {o[0] for o in out} == {"ok"}
    assert len({id(o[1]) for o in out}) == 1                                  # the one rebuild, served to all four


def test_wind_an_error_answer_older_than_a_minute_is_asked_again_by_a_refresh():
    svc, fetch, clock = wind_service()
    fetch.table[W.NDBC_RT2_URL % "51003"] = IOError("down")
    assert svc.history("ndbc:51003")[0] == "error"
    fetch.table[W.NDBC_RT2_URL % "51003"] = TW.fx("realtime2_51003.txt")
    clock.t += 30
    assert svc.history("ndbc:51003", FRESH)[0] == "error" and fetch.count("51003") == 1   # 30 s: the error stands
    clock.t += 31
    assert svc.history("ndbc:51003", FRESH)[0] == "ok"


# ------------------------------------------------------------------ the tide water level

def test_tide_refresh_asks_the_water_level_again_after_a_minute():
    svc, noaa, clock = TT.service()
    first = svc.observed("1612340")
    assert first[0] == "ok" and len(noaa.calls) == 1
    clock.t += FRESH - 1
    assert svc.observed("1612340", FRESH)[1] is first[1] and len(noaa.calls) == 1
    clock.t += 1
    assert svc.observed("1612340", FRESH)[1] is not first[1] and len(noaa.calls) == 2
    assert svc.observed("1612340", FRESH) and len(noaa.calls) == 2
    n = len(noaa.calls)
    assert svc.forecast("1612340")[0] == "ok"
    clock.t += FRESH
    svc.observed("1612340")
    assert len(noaa.calls) == n + 2                                           # the forecast's two; no water level


def test_tide_a_failed_refresh_keeps_the_water_level():
    svc, noaa, clock = TT.service()
    good = svc.observed("1612340")
    clock.t += FRESH
    noaa.fail = True
    assert svc.observed("1612340", FRESH)[0] == "error"
    assert svc.observed("1612340") == good
    assert svc.observed("1612340", FRESH) == good and len(noaa.calls) == 2   # within the minute of the failed try
    noaa.fail = False
    clock.t += FRESH
    status, p = svc.observed("1612340", FRESH)
    assert status == "ok" and p is not good[1] and len(noaa.calls) == 3


def test_tide_a_station_without_a_gauge_never_asks():
    svc, noaa, _ = TT.service()
    assert svc.observed("1611401", FRESH)[0] == "final" and noaa.calls == []


def test_tide_the_forecast_entries_keep_their_shape():
    svc, noaa, clock = TT.service()
    svc.forecast("1612340")
    for entry in svc._cache.values():
        assert len(entry) == 5 and entry[3] == entry[4] == clock.t


# ------------------------------------------------------------------ the routes pass the rule on

@pytest.fixture
def record(monkeypatch):
    seen = []
    tsvc, _, _ = TT.service()
    monkeypatch.setitem(A._TIDES, "svc", tsvc)
    monkeypatch.setitem(A._TIDES, "list", ("{}", "e"))
    monkeypatch.setattr(tsvc, "observed", lambda sid, fresh_s=None: seen.append(("tide", fresh_s)) or ("ok", {"t": []}))
    wsvc, _, _ = wind_service()
    monkeypatch.setattr(A, "WIND_ENABLED", True)
    monkeypatch.setattr(A, "_WINDS", {"svc": wsvc, "list": ("{}", "e"), "providers": [], "stations": dict(TW.STATIONS)})
    monkeypatch.setattr(wsvc, "history", lambda sid, fresh_s=None: seen.append(("wind", fresh_s)) or ("ok", {"t": []}))
    return A.app.test_client(), seen


def test_routes_pass_the_header_on(record):
    c, seen = record
    assert c.get("/api/tides/1612340/observed", headers=HDR).status_code == 200
    assert c.get("/api/tides/1612340/observed").status_code == 200
    assert c.get("/api/wind/ndbc:51003/history", headers=HDR).status_code == 200
    assert c.get("/api/wind/ndbc:51003/history", headers={A.REFRESH_HEADER: "0"}).status_code == 200
    assert seen == [("tide", 60), ("tide", None), ("wind", 60), ("wind", None)]


def test_a_refresh_answer_keeps_the_routes_cache_headers(record):
    c, _ = record
    r = c.get("/api/wind/ndbc:51003/history", headers=HDR)
    assert r.headers["Cache-Control"] == "public, max-age=60" and r.headers["CDN-Cache-Control"] == "no-store"
    r = c.get("/api/tides/1612340/observed", headers=HDR)
    assert r.headers["Cache-Control"] == "public, max-age=300" and r.headers["CDN-Cache-Control"] == "no-store"


# ------------------------------------------------------------------ the NDBC spectra

@pytest.fixture
def ndbc(monkeypatch):
    calls = []

    def down(url, timeout=25):
        calls.append(url)
        raise IOError("NDBC down")
    monkeypatch.setattr(A, "_fetch_text", down)
    monkeypatch.setattr(A, "get_live_ndbc_wave_stations", lambda: [])
    monkeypatch.setattr(A, "NDBC_COMPONENT_CACHE", {})
    return A.app.test_client(), calls


def plant(age, asked_age=None):
    entry = {"timestamp": time.time() - age, "data": {"station": "51201", "components": [], "kept": True}}
    if asked_age is not None:
        entry["asked"] = time.time() - asked_age
    A.NDBC_COMPONENT_CACHE["51201"] = entry
    return entry


def test_ndbc_a_refresh_asks_again_after_a_minute_and_keeps_the_spectra_when_it_fails(ndbc):
    c, calls = ndbc
    plant(FRESH - 5)
    r = c.get("/api/ndbc/station/51201/components", headers=HDR)
    assert r.status_code == 200 and r.get_json()["kept"] is True and calls == []          # 55 s old: served
    entry = plant(FRESH + 5)
    assert c.get("/api/ndbc/station/51201/components").get_json()["kept"] is True and calls == []   # no header
    r = c.get("/api/ndbc/station/51201/components", headers=HDR)
    assert r.status_code == 404 and len(calls) == 1                                        # asked; NDBC failed
    assert A.NDBC_COMPONENT_CACHE["51201"] is entry and time.time() - entry["asked"] < 5   # the spectra stay
    assert c.get("/api/ndbc/station/51201/components").get_json()["kept"] is True
    r = c.get("/api/ndbc/station/51201/components", headers=HDR)
    assert r.get_json()["kept"] is True and len(calls) == 1                                # the floor counts the try


def test_ndbc_the_floor_counts_the_last_ask(ndbc):
    c, calls = ndbc
    plant(FRESH + 300, asked_age=FRESH - 5)
    assert c.get("/api/ndbc/station/51201/components", headers=HDR).get_json()["kept"] is True and calls == []
    plant(FRESH + 300, asked_age=FRESH + 1)
    assert c.get("/api/ndbc/station/51201/components", headers=HDR).status_code == 404 and len(calls) == 1


def test_ndbc_an_expired_entry_is_rebuilt_with_or_without_the_header(ndbc):
    c, calls = ndbc
    plant(A.NDBC_COMPONENT_TTL_SECONDS + 1)
    assert c.get("/api/ndbc/station/51201/components").status_code == 404 and len(calls) == 1


# ------------------------------------------------------------------ the AODN spectra

class CountingHttp:
    def __init__(self):
        self.calls = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        raise requests.ConnectionError("THREDDS down")


def aodn():
    p = B.AODNProvider(http=CountingHttp())
    site = sorted(p.SPECTRA_SITES)[0]
    spec = {"freqs": [0.1] * 39, "steps": [{"time_utc": "x"}]}
    return p, site, spec


def test_aodn_a_refresh_asks_again_after_a_minute_and_keeps_the_spectra(monkeypatch):
    p, site, spec = aodn()
    p._spec_cache[(site, 25)] = (time.time() - (FRESH - 5), spec)
    assert p.spectrum(site, fresh_s=FRESH) is spec and p.http.calls == []
    p._spec_cache[(site, 25)] = (time.time() - (FRESH + 5), spec)
    assert p.spectrum(site) is spec and p.http.calls == []                     # no refresh: the 20-min rule
    assert p.spectrum(site, fresh_s=FRESH) is None and len(p.http.calls) == 1   # asked again; THREDDS failed
    assert p.spectrum(site) is spec                                            # kept for everyone
    assert p.spectrum(site, fresh_s=FRESH) is spec and len(p.http.calls) == 1   # within a minute of the try


def test_aodn_route_passes_the_header_on(monkeypatch):
    seen = []

    class P:
        source = "AODN"

        def spectrum(self, local, fresh_s=None):
            seen.append((local, fresh_s))
            return None
    monkeypatch.setattr(A, "get_buoy_providers", lambda: [P()])
    c = A.app.test_client()
    assert c.get("/api/buoys/aodn:X1/components", headers=HDR).status_code == 404
    assert c.get("/api/buoys/aodn:X1/components").status_code == 404
    assert seen == [("X1", 60), ("X1", None)]
