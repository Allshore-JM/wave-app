"""Forecast singleflight: concurrent identical cold requests share one build.

Behaviour that must NOT change: successful results are cached with the same TTL rule, failed
results are never cached (each caller retries), different keys build independently, and the
returned object is exactly what the cache holds. Parsing itself is untouched (the existing
parse/wind/SWAN tests cover the bytes).
"""
import os
import sys
import threading
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import app as A  # noqa: E402

OK = ("cycle", "loc", "run", [["d", "t"] + [None] * 21], "Pacific/Honolulu", None)
ERR = (None, None, None, None, "Pacific/Honolulu", "No .bull found")


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(A, "_FORECAST_CACHE", {})
    monkeypatch.setattr(A, "_FORECAST_INFLIGHT", {})
    yield


def _par(fn, n=4):
    out = []
    ts = [threading.Thread(target=lambda: out.append(fn())) for _ in range(n)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return out


def test_concurrent_cold_requests_build_once(monkeypatch):
    calls = []

    def slow(station, tz=None):
        calls.append(station); time.sleep(0.2); return OK
    monkeypatch.setattr(A, "_parse_bull_uncached", slow)
    t = time.time()
    results = _par(lambda: A.parse_bull("51201"))
    assert len(calls) == 1
    assert all(r is OK for r in results)                         # same object, not a copy
    assert A._FORECAST_CACHE[("51201", "", "GFS")]["data"] is OK
    assert time.time() - t < 0.6
    assert A._FORECAST_INFLIGHT == {}                            # lock released and dropped


def test_failed_build_is_not_cached_and_each_caller_retries(monkeypatch):
    calls = []

    def failing(station, tz=None):
        calls.append(station); time.sleep(0.05); return ERR
    monkeypatch.setattr(A, "_parse_bull_uncached", failing)
    results = _par(lambda: A.parse_bull("51201"))
    assert all(r is ERR for r in results)
    assert len(calls) == 4                                       # as before: every caller tried
    assert ("51201", "", "GFS") not in A._FORECAST_CACHE


def test_different_keys_do_not_wait_on_each_other(monkeypatch):
    def slow(station, tz=None):
        time.sleep(0.3); return OK
    monkeypatch.setattr(A, "_parse_bull_uncached", slow)
    t = time.time()
    _par(lambda: A.parse_bull("51201"), 1) if False else None
    ts = [threading.Thread(target=lambda s=s: A.parse_bull(s)) for s in ("51201", "51001", "51002")]
    [x.start() for x in ts]; [x.join() for x in ts]
    assert time.time() - t < 0.55                                # parallel, not 0.9 s serial


def test_swan_path_uses_the_same_singleflight(monkeypatch):
    calls = []

    def slow(station, tz=None):
        calls.append(station); time.sleep(0.2); return OK
    monkeypatch.setattr(A, "_parse_swan_uncached", slow)
    results = _par(lambda: A.parse_swan("51201"))
    assert len(calls) == 1 and all(r is OK for r in results)
    assert A._FORECAST_CACHE[("51201", "", "SWAN")]["data"] is OK


def test_cache_hit_and_ttl_rule_unchanged(monkeypatch):
    monkeypatch.setattr(A, "_parse_bull_uncached", lambda s, tz=None: OK)
    A.parse_bull("51201")
    entry = A._FORECAST_CACHE[("51201", "", "GFS")]
    assert entry["ttl"] == A._forecast_entry_ttl(OK)             # same TTL rule as before
    monkeypatch.setattr(A, "_parse_bull_uncached", lambda s, tz=None: (_ for _ in ()).throw(AssertionError("no rebuild")))
    assert A.parse_bull("51201") is OK                           # served from cache
    entry["ts"] = 0                                              # expired -> rebuild path
    monkeypatch.setattr(A, "_parse_bull_uncached", lambda s, tz=None: ERR)
    assert A.parse_bull("51201") is ERR
