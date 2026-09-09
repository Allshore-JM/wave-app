"""Release B: NDBC component optional files download concurrently; results are byte-identical.

Golden: tests/fixtures/ndbc_components_golden.json captured on the unchanged code (serial loop)
by tests/capture_ndbc_components_golden.py with the same fixtures and pinned clock.
"""
import functools
import json
import os
import sys
import threading
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import capture_ndbc_components_golden as G  # noqa: E402

GOLDEN = json.load(open(os.path.join(HERE, "fixtures", "ndbc_components_golden.json"), encoding="utf-8"))


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(A, "NDBC_COMPONENT_CACHE", {})
    monkeypatch.setattr(A, "get_live_ndbc_wave_stations", lambda: list(G.META.values()))
    monkeypatch.setattr(A, "get_station_tz", lambda sid: "Pacific/Honolulu")
    monkeypatch.setattr(A, "_ndbc_row_is_recent", functools.partial(A._ndbc_row_is_recent, now_utc=G.NOW))
    yield


def test_golden_scenarios_byte_identical(monkeypatch):
    saved = (A._fetch_text, A._ndbc_row_is_recent, A.get_live_ndbc_wave_stations, A.get_station_tz)
    try:
        rec = G.run_scenarios(A)
    finally:
        A._fetch_text, A._ndbc_row_is_recent, A.get_live_ndbc_wave_stations, A.get_station_tz = saved
    assert set(rec) == set(GOLDEN)
    for name, old in GOLDEN.items():
        new = rec[name]
        assert new["status"] == old["status"], name
        assert new["body"] == old["body"], "raw body differs in scenario %s" % name
        assert new["content_type"] == old["content_type"], name
        assert new["cache_control"] == old["cache_control"], name
        assert sorted(new["fetched"]) == sorted(old["fetched"]), name   # same SET of upstream files


def test_optional_files_download_concurrently(monkeypatch):
    calls = []
    monkeypatch.setattr(A, "_fetch_text", G.make_fetch(calls=calls, delay=0.15))
    c = A.app.test_client()
    t = time.time()
    r = c.get("/api/ndbc/station/51201/components")
    wall = time.time() - t
    assert r.status_code == 200 and len(calls) == 6
    assert wall < 0.15 * 6 * 0.6, wall                # serial would be >= 0.9 s; expect ~0.3 s


def test_required_failure_starts_no_optional_downloads(monkeypatch):
    calls = []
    monkeypatch.setattr(A, "_fetch_text", G.make_fetch(fail_suffixes=("data_spec",), calls=calls))
    r = A.app.test_client().get("/api/ndbc/station/51201/components")
    assert r.status_code == 404 and calls == ["51201.data_spec"]
    assert r.get_data(as_text=True) == GOLDEN["no_required"]["body"]


def test_slow_but_successful_optional_is_kept(monkeypatch):
    """No overall deadline: a slow optional file still lands (would have serially)."""
    real = G.make_fetch()

    def fetch(url, timeout=25):
        if url.endswith(".swdir"):
            time.sleep(0.5)
        return real(url, timeout)
    monkeypatch.setattr(A, "_fetch_text", fetch)
    r = A.app.test_client().get("/api/ndbc/station/51201/components")
    assert r.get_data(as_text=True) == GOLDEN["full"]["body"]     # swdir data present, not None


def test_repeated_concurrent_taps_do_not_leak_threads_or_queue(monkeypatch):
    monkeypatch.setattr(A, "_fetch_text", G.make_fetch(delay=0.3))
    c = A.app.test_client()
    base = threading.active_count()
    walls = []

    def tap():
        A.NDBC_COMPONENT_CACHE.clear()                  # force a miss every time
        t = time.time()
        r = c.get("/api/ndbc/station/51201/components")
        walls.append(time.time() - t)
        assert r.status_code == 200
    for _ in range(2):                                   # two rounds of 5 concurrent taps
        ts = [threading.Thread(target=tap) for _ in range(5)]
        [t.start() for t in ts]
        [t.join() for t in ts]
    assert len(walls) == 10
    assert max(walls) < 0.3 * 2 * 1.5, walls             # ~0.6 s each (required + parallel optional), never cumulative
    time.sleep(0.2)
    assert threading.active_count() <= base + 1          # per-request executors are torn down
