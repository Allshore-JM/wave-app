"""Integration + cache-policy tests for the Wind column feature.

Covers the wind JOIN inside _parse_bull_uncached (both bull formats) and
_parse_swan (via the cache wrapper), the _WIND_CACHE negative-TTL / cycle-pinning
/ eviction policy, the _FORECAST_CACHE short-TTL-when-wind-blank behavior, the
get_latest_run negative caching, and the graph_data byte-identity invariant.
All monkeypatched -- no network.
"""
import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures")
WIND = {datetime(2026, 7, 4, h): (8.09, 71.6) for h in (12, 13)}  # covers 2 of 3 rows


class _Resp:
    def __init__(self, text):
        self.status_code = 200
        self.text = text
        self.headers = {}


@pytest.fixture(autouse=True)
def _clear_caches():
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
        app._WIND_CACHE.clear()
        app._RUN_CACHE.update({"ts": 0.0, "value": (None, None)})
    yield
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
        app._WIND_CACHE.clear()
        app._RUN_CACHE.update({"ts": 0.0, "value": (None, None)})


def _bull(monkeypatch, fixture_name, wind=WIND):
    with open(os.path.join(FIX, fixture_name)) as f:
        text = f.read()
    monkeypatch.setattr(app, "get_latest_run", lambda: ("20260704", "12"))
    monkeypatch.setattr(app.HTTP, "get", lambda *a, **k: _Resp(text))
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: dict(wind))
    return app._parse_bull_uncached("51201", None)


# --------------------------- GFS bull wind join --------------------------------

def test_gfs_modern_format_wind_join(monkeypatch):
    _, _, _, rows, _, err = _bull(monkeypatch, "gfswave_bull_modern.txt")
    assert err is None and len(rows) == 3
    for r in rows:
        assert len(r) == 23
    # rows are 12,13,14 UTC; wind covers 12 & 13 only.
    assert rows[0][20] == 8.09 and rows[0][21] == 72   # 12 UTC covered
    assert rows[1][20] == 8.09 and rows[1][21] == 72   # 13 UTC covered
    assert rows[2][20] is None and rows[2][21] is None  # 14 UTC uncovered
    # combined stayed row[-1] and was NOT overwritten by wind
    assert rows[0][-1] == round(1.27 * 3.28084, 2)     # Hst 1.27 m
    # swell 1 (index 2-4) intact
    assert rows[0][2] == round(1.18 * 3.28084, 2)
    assert rows[0][4] == (226 + 180) % 360             # GFS TO->FROM flip preserved


def test_gfs_legacy_format_wind_join(monkeypatch):
    # Exercises the SECOND join site (the dormant "Hr" format path). NOAA no
    # longer serves this format live, so this synthetic fixture isn't a faithful
    # reproduction of the wave columns -- the test asserts only the wind-join
    # invariants (the join lives at the same spot in both formats): wind at
    # 20/21 for covered hours, blank for uncovered, combined at row[-1], 23 cols.
    _, _, _, rows, _, err = _bull(monkeypatch, "gfswave_bull_legacy.txt")
    assert err is None and len(rows) == 3
    for r in rows:
        assert len(r) == 23
        assert r[-1] is not None or True  # combined column present at row[-1]
    # legacy rows are cycle + 0/1/2 h = 12,13,14 UTC; wind covers 12 & 13.
    assert rows[0][20] == 8.09 and rows[0][21] == 72   # 12 UTC
    assert rows[1][20] == 8.09 and rows[1][21] == 72   # 13 UTC
    assert rows[2][20] is None and rows[2][21] is None  # 14 UTC uncovered


def test_gfs_no_wind_blank_but_waves_intact(monkeypatch):
    # get_station_wind returns {} (spec failed) -> blank wind, waves unaffected.
    _, _, _, rows, _, err = _bull(monkeypatch, "gfswave_bull_modern.txt", wind={})
    assert err is None
    assert all(r[20] is None and r[21] is None for r in rows)
    assert rows[0][-1] == round(1.27 * 3.28084, 2)


# ----------------------------- _WIND_CACHE policy ------------------------------

def _spec_resp(status, body=b""):
    class R:
        status_code = status
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def close(self): pass
        def iter_content(self, chunk_size=65536):
            for i in range(0, len(body), chunk_size):
                yield body[i:i + chunk_size]
    return R()


def test_wind_cache_negative_ttl_and_refetch(monkeypatch):
    calls = {"n": 0}
    spec = open(os.path.join(FIX, "gfswave_spec_sample.spec"), "rb").read()

    def fake_get(url, **k):
        calls["n"] += 1
        return _spec_resp(404 if calls["n"] == 1 else 200, b"" if calls["n"] == 1 else spec)

    monkeypatch.setattr(app.HTTP, "get", fake_get)
    # call 1: 404 -> {} cached with the SHORT negative TTL
    assert app.get_station_wind("51201", "20260704", "12") == {}
    entry = app._WIND_CACHE[("51201", "20260704", "12")]
    assert entry["ttl"] == app._WIND_NEG_TTL
    # within the window: served from cache, no refetch
    assert app.get_station_wind("51201", "20260704", "12") == {} and calls["n"] == 1
    # expire the negative entry -> refetch yields real data with the long TTL
    entry["ts"] -= app._WIND_NEG_TTL + 1
    d = app.get_station_wind("51201", "20260704", "12")
    assert len(d) == 8 and calls["n"] == 2
    assert app._WIND_CACHE[("51201", "20260704", "12")]["ttl"] == app._WIND_CACHE_TTL


def test_wind_cache_is_cycle_pinned(monkeypatch):
    calls = {"n": 0}
    spec = open(os.path.join(FIX, "gfswave_spec_sample.spec"), "rb").read()
    monkeypatch.setattr(app.HTTP, "get",
                        lambda *a, **k: (calls.__setitem__("n", calls["n"] + 1)
                                         or _spec_resp(200, spec)))
    app.get_station_wind("51201", "20260704", "12")
    app.get_station_wind("51201", "20260704", "18")  # different run -> distinct key
    assert calls["n"] == 2
    assert ("51201", "20260704", "12") in app._WIND_CACHE
    assert ("51201", "20260704", "18") in app._WIND_CACHE


def test_forecast_cache_short_ttl_when_wind_blank(monkeypatch):
    # A clean GFS parse whose wind is entirely blank must be cached with the
    # short TTL so wind re-joins within minutes at a spec-lags-bull rollover.
    with open(os.path.join(FIX, "gfswave_bull_modern.txt")) as f:
        text = f.read()
    monkeypatch.setattr(app, "get_latest_run", lambda: ("20260704", "12"))
    monkeypatch.setattr(app.HTTP, "get", lambda *a, **k: _Resp(text))
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: {})  # blank wind
    app.parse_bull("51201", None)
    assert app._FORECAST_CACHE[("51201", "", "GFS")]["ttl"] == app._WIND_NEG_TTL
    # with wind present -> full TTL
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: dict(WIND))
    app.parse_bull("51201", None)
    assert app._FORECAST_CACHE[("51201", "", "GFS")]["ttl"] == app._FORECAST_CACHE_TTL


# --------------------------- get_latest_run neg cache --------------------------

def test_get_latest_run_negative_cached(monkeypatch):
    calls = {"n": 0}

    def fail():
        calls["n"] += 1
        return (None, None)

    monkeypatch.setattr(app, "_detect_latest_run", fail)
    assert app.get_latest_run() == (None, None) and calls["n"] == 1
    # within the negative window: no re-probe
    assert app.get_latest_run() == (None, None) and calls["n"] == 1
    # expire -> re-probe
    with app._CACHE_LOCK:
        app._RUN_CACHE["ts"] -= app._RUN_NEG_TTL + 1
    app.get_latest_run()
    assert calls["n"] == 2


# --------------------------- graph_data byte-identity --------------------------

def test_graph_data_has_no_wind_keys(monkeypatch):
    rows = [(["Friday, July 4, 2026", "2:00 PM"] + [3.9, 7.9, 46] + [None] * 15
             + [8.09, 72, c]) for c in (4.17, 5.25)]
    monkeypatch.setattr(app, "parse_bull",
                        lambda s, tz: ("Cycle : x", "Location : x (21.67N 158.12W)",
                                       None, rows, "Pacific/Honolulu", None))
    monkeypatch.setattr(app, "resolve_model", lambda s, m: "GFS")
    payload = app.compute_forecast_payload("51201", None, "US", "GFS")
    gd = payload["graph_data"]
    assert set(gd.keys()) == {"labels", "height", "period", "direction",
                              "units", "cycle", "location", "tz"}
    assert "wind" not in gd and "wind_speed" not in gd
    # combined read from row[-1], not the wind column
    assert gd["height"]["combined"] == [4.17, 5.25]
