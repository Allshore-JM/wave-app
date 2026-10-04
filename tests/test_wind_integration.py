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

def test_bulletin_rows_come_out_in_rank_order(monkeypatch):
    """NOAA lists a row's systems by height; the site ranks them by height squared x period (owner, 2026-10-02: one
    rule for stations and points), packed from the left. The numbers of a row stay."""
    seen = {}
    real = app.rank_rows
    monkeypatch.setattr(app, "rank_rows", lambda rows: seen.setdefault("before", [list(r) for r in rows]) and real(rows))
    _, _, _, rows, _, err = _bull(monkeypatch, "gfswave_bull_modern.txt")
    assert err is None and len(rows) == len(seen["before"]) == 3
    for r, b in zip(rows, seen["before"]):
        live = [g for g in range(6) if r[2 + 3 * g] is not None]
        power = [r[2 + 3 * g] ** 2 * r[3 + 3 * g] for g in live]
        assert live == list(range(len(live))) and power == sorted(power, reverse=True)
        groups = lambda x: sorted((x[2 + 3 * g], x[3 + 3 * g], x[4 + 3 * g]) for g in range(6) if x[2 + 3 * g] is not None)   # noqa: E731
        assert groups(r) == groups(b) and r[:2] == b[:2] and r[20:] == b[20:]

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
    # combined = the header's Hst column (it was the line's last number: swell 6's direction)
    assert [r[-1] for r in rows] == [round(v * 3.28084, 2) for v in (1.27, 1.28, 1.28)]
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
    # wind for some rows only -> still the short TTL (G19-A P2-1: a gap heals within minutes)
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: dict(WIND))
    app.parse_bull("51201", None)
    assert app._FORECAST_CACHE[("51201", "", "GFS")]["ttl"] == app._WIND_NEG_TTL
    # wind for every row -> full TTL
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
    full = {datetime(2026, 7, 4, h): (8.09, 71.6) for h in (12, 13, 14)}
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: dict(full))
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
                              "units", "cycle", "location", "tz", "swells",   # swells: plan section 26
                              "sky"}                                         # plan section 35: the window's only
    assert gd["sky"] is None                                                 # the classic payload computes no sky
    assert "wind" not in gd and "wind_speed" not in gd
    # combined read from row[-1], not the wind column
    assert gd["height"]["combined"] == [4.17, 5.25]

# --------------------------- SWAN back-fill (plan section 27) ------------------

SWAN_FIX = os.path.join(FIX, "swan_buoy_sample.table")   # shown rows 12Z-20Z July 3 2026 (after the 6 h spin-up)


def _swan(monkeypatch, latest, older, station="51201"):
    """parse_swan with the latest GFS run covering 18Z onward and the pinned 12Z run (if given) covering 12Z-20Z."""
    text = open(SWAN_FIX).read()
    calls = []
    monkeypatch.setattr(app.HTTP, "get", lambda *a, **k: _Resp(text))

    def fake_wind(sid, date_str=None, run_str=None, keep_s=None):
        calls.append((sid, date_str, run_str, keep_s))
        return dict(latest) if date_str is None else dict(older)
    monkeypatch.setattr(app, "get_station_wind", fake_wind)
    return app.parse_swan(station, None), calls


LATEST = {datetime(2026, 7, 3, h): (9.5, 45.0) for h in range(18, 24)}
OLDER = {datetime(2026, 7, 3, h): (6.0, 90.0) for h in range(12, 24)}


@pytest.mark.parametrize("station", sorted(app.SWAN_STATIONS))
def test_swan_rows_before_the_latest_gfs_run_get_wind_from_the_earlier_run(monkeypatch, station):
    (_, _, _, rows, _, err), calls = _swan(monkeypatch, LATEST, OLDER, station)
    assert err is None and rows
    assert all(r[20] is not None and r[21] is not None for r in rows), "every SWAN row has wind"
    assert rows[0][20] == 6.0 and rows[0][21] == 90, "12Z: the earlier run"
    assert rows[-1][20] == 9.5 and rows[-1][21] == 45, "20Z: the latest run wins where both cover it"
    assert (station, "20260703", "12", app._WIND_PAST_RUN_TTL) in calls, "the cycle containing the first shown row, kept for hours"


def test_swan_back_fill_fails_soft_and_is_skipped_when_not_needed(monkeypatch):
    (_, _, _, rows, _, err), calls = _swan(monkeypatch, LATEST, {})
    assert err is None
    assert rows[0][20] is None and rows[-1][20] == 9.5, "no earlier run: the early rows stay blank as before"
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
    covering = {datetime(2026, 7, 3, h): (7.0, 10.0) for h in range(12, 24)}
    (_, _, _, rows, _, _), calls = _swan(monkeypatch, covering, OLDER)
    assert len(calls) == 1 and rows[0][20] == 7.0, "the latest run already covers the first row: no second fetch"
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
    (_, _, _, rows, _, _), calls = _swan(monkeypatch, {}, OLDER)
    assert len(calls) == 1 and all(r[20] is None for r in rows), "the latest run failed: no back-fill attempt (fail soft)"
    assert app._swan_first_row_utc("% header\n\n20260703.060000 1 2\n") == datetime(2026, 7, 3, 12)
    assert app._swan_first_row_utc("% only comments\n") is None


def test_wind_keep_s_sets_the_positive_ttl_and_non_200_is_logged(monkeypatch, caplog):
    spec = open(os.path.join(FIX, "gfswave_spec_sample.spec"), "rb").read()
    monkeypatch.setattr(app.HTTP, "get", lambda *a, **k: _spec_resp(200, spec))
    app.get_station_wind("51201", "20260703", "12", keep_s=app._WIND_PAST_RUN_TTL)
    assert app._WIND_CACHE[("51201", "20260703", "12")]["ttl"] == app._WIND_PAST_RUN_TTL
    monkeypatch.setattr(app.HTTP, "get", lambda *a, **k: _spec_resp(404, b""))
    with caplog.at_level("WARNING"):
        assert app.get_station_wind("51202", "20260703", "12", keep_s=app._WIND_PAST_RUN_TTL) == {}
    assert app._WIND_CACHE[("51202", "20260703", "12")]["ttl"] == app._WIND_NEG_TTL, "a failure is never kept for hours"
    assert any("wind spec HTTP 404 for 51202" in r.getMessage() for r in caplog.records)


def test_payload_reports_whether_every_row_has_wind(monkeypatch):
    text = open(SWAN_FIX).read()
    monkeypatch.setattr(app.HTTP, "get", lambda *a, **k: _Resp(text))
    monkeypatch.setattr(app, "get_station_wind", lambda sid, d=None, r=None, keep_s=None: dict(LATEST if d is None else OLDER))
    monkeypatch.setattr(app, "resolve_model", lambda s, m: "SWAN")
    assert app.compute_forecast_payload("51201", None, "US", "SWAN", compact=True)["wind_complete"] is True
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
    monkeypatch.setattr(app, "get_station_wind", lambda sid, d=None, r=None, keep_s=None: dict(LATEST if d is None else {}))
    assert app.compute_forecast_payload("51201", None, "US", "SWAN", compact=True)["wind_complete"] is False
