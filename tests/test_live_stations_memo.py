"""/api/buoys/live-stations memo + provider singleflight: same bytes, same refresh timing, less work.

Golden bytes come from tests/fixtures/live_stations_golden.json, captured by
tests/capture_live_stations_golden.py on the UNCHANGED implementation (commit 6c7c0e4) with the
same fake providers, frozen clock, and deterministic tz stub. Nothing here compares the new
code against itself.
"""
import copy
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
import buoy_sources as B  # noqa: E402
import fake_buoy_providers as F  # noqa: E402
from capture_live_stations_golden import run_sequence  # noqa: E402

GOLDEN = json.load(open(os.path.join(HERE, "fixtures", "live_stations_golden.json"), encoding="utf-8"))


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """Fresh memo, frozen clock inside buoy_sources, deterministic tz; restored afterwards."""
    monkeypatch.setattr(A, "_LIVE_STATIONS_MEMO", {"key": None, "payload": None, "etag": None})
    monkeypatch.setattr(B, "time", F.FrozenTime())
    monkeypatch.setattr(A, "_buoy_tz_cached", F.fake_tz)
    yield


def _install(monkeypatch, provs):
    monkeypatch.setattr(A, "get_buoy_providers", lambda: provs)
    return A.app.test_client()


# ------------------------------- golden replay ----------------------------------

def test_golden_sequence_byte_identical(monkeypatch):
    saved_rows = F.FakeAODN.rows
    try:
        rec = run_sequence(A, B)          # patches A/B attrs; fixture restores them
    finally:
        F.FakeAODN.rows = saved_rows
    assert len(rec["steps"]) == len(GOLDEN["steps"])
    for new, old in zip(rec["steps"], GOLDEN["steps"]):
        assert new["step"] == old["step"]
        assert new["status"] == old["status"], new["step"]
        assert new["body"] == old["body"], "raw body differs at step %s" % new["step"]
        assert new["etag"] == old["etag"], new["step"]
        assert new["cache_control"] == old["cache_control"], new["step"]
        assert new["content_type"] == old["content_type"], new["step"]
        if "fetch_calls" in old:          # upstream fetch schedule identical too
            assert new["fetch_calls"] == old["fetch_calls"], new["step"]


# ------------------------------- memo behaviour ----------------------------------

def test_two_gets_build_once(monkeypatch):
    provs = F.make_providers()
    c = _install(monkeypatch, provs)
    builds = []
    real = A._build_live_stations_payload
    monkeypatch.setattr(A, "_build_live_stations_payload", lambda lists: builds.append(1) or real(lists))
    r1 = c.get("/api/buoys/live-stations")
    r2 = c.get("/api/buoys/live-stations")
    assert r1.status_code == r2.status_code == 200
    assert r1.data == r2.data and r1.headers["ETag"] == r2.headers["ETag"]
    assert len(builds) == 1


def test_conditional_get_is_304_without_a_build(monkeypatch):
    provs = F.make_providers()
    c = _install(monkeypatch, provs)
    et = c.get("/api/buoys/live-stations").headers["ETag"]
    builds = []
    monkeypatch.setattr(A, "_build_live_stations_payload", lambda lists: builds.append(1) or ("x", "y"))
    r = c.get("/api/buoys/live-stations", headers={"If-None-Match": et})
    assert r.status_code == 304 and r.headers["ETag"] == et and builds == []


def test_same_content_republished_rebuilds_but_keeps_etag(monkeypatch):
    provs = F.make_providers()
    c = _install(monkeypatch, provs)
    r1 = c.get("/api/buoys/live-stations")
    aodn = next(p for p in provs if p.source == "AODN")
    aodn._list_ts = 0.0                     # expired -> inline refresh with identical data
    builds = []
    real = A._build_live_stations_payload
    monkeypatch.setattr(A, "_build_live_stations_payload", lambda lists: builds.append(1) or real(lists))
    r2 = c.get("/api/buoys/live-stations")
    assert F.FakeAODN.fetch_calls == 2 and len(builds) == 1     # refreshed + rebuilt ...
    assert r2.data == r1.data and r2.headers["ETag"] == r1.headers["ETag"]   # ... same bytes


def test_changed_content_changes_etag(monkeypatch):
    provs = F.make_providers()
    c = _install(monkeypatch, provs)
    r1 = c.get("/api/buoys/live-stations")
    monkeypatch.setattr(F.FakeAODN, "rows", F.FakeAODN.rows + [
        {"local_id": "HOB", "name": "Hobart", "lat": -42.9, "lon": 147.4, "latest_time": F.FRESH}])
    next(p for p in provs if p.source == "AODN")._list_ts = 0.0
    r2 = c.get("/api/buoys/live-stations")
    assert r2.headers["ETag"] != r1.headers["ETag"]
    assert any(s["id"] == "aodn:HOB" for s in r2.get_json())


def test_raising_and_empty_providers_yield_empty_lists(monkeypatch):
    provs = F.make_providers()
    c = _install(monkeypatch, provs)
    body = c.get("/api/buoys/live-stations").get_json()
    assert not any(s["source"] in ("SMHI", "QLD") for s in body)
    smhi = next(p for p in provs if p.source == "SMHI")
    qld = next(p for p in provs if p.source == "QLD")
    assert smhi._list_cache == [] and smhi._list_version == 1     # failed fetch still publishes []
    assert qld._list_cache == [] and qld._list_version == 1       # successful empty list, same shape
    # a later successful empty publish is a new version and served normally
    qld._list_ts = 0.0
    c.get("/api/buoys/live-stations")
    assert qld._list_version == 2 and F.FakeQLD.fetch_calls == 2


def test_provider_caches_stay_pristine(monkeypatch):
    provs = F.make_providers()
    c = _install(monkeypatch, provs)
    c.get("/api/buoys/live-stations")
    before = {p.source: copy.deepcopy(p._list_cache) for p in provs}
    for _ in range(3):
        c.get("/api/buoys/live-stations")
    after = {p.source: copy.deepcopy(p._list_cache) for p in provs}
    assert after == before
    for p in provs:
        for s in p._list_cache or []:
            assert "also_sources" not in s and "tz" not in s and s["dup_of"] is None


# ------------------------------- concurrency -------------------------------------

def test_burst_of_same_key_misses_builds_once(monkeypatch):
    provs = F.make_providers()
    c = _install(monkeypatch, provs)
    builds = []
    real = A._build_live_stations_payload

    def slow_build(lists):
        builds.append(1)
        time.sleep(0.2)
        return real(lists)
    monkeypatch.setattr(A, "_build_live_stations_payload", slow_build)
    results = []

    def hit():
        results.append(c.get("/api/buoys/live-stations"))
    ts = [threading.Thread(target=hit) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(builds) == 1
    assert len({r.headers["ETag"] for r in results}) == 1 and all(r.status_code == 200 for r in results)


def test_provider_refresh_singleflight(monkeypatch):
    """Two concurrent callers of an EXPIRED provider: one upstream fetch, both get its result."""
    F.FakeCDIP.fetch_calls = 0
    p = F.FakeCDIP(http=None)
    orig = p._fetch_stations

    def slow(*a):
        time.sleep(0.2)
        return orig()
    monkeypatch.setattr(p, "_fetch_stations", slow)
    out = []
    ts = [threading.Thread(target=lambda: out.append(p.list_stations_versioned())) for _ in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert F.FakeCDIP.fetch_calls == 1
    assert len({v for _, v, _ in out}) == 1 and all(len(lst) == 2 for lst, _, _ in out)


def test_failed_refresh_with_waiters_all_get_empty_and_lock_is_released(monkeypatch):
    p = F.FakeSMHI(http=None)

    def slow_fail(*a):
        time.sleep(0.2)
        raise RuntimeError("down")
    monkeypatch.setattr(p, "_fetch_stations", slow_fail)
    out = []
    ts = [threading.Thread(target=lambda: out.append(p.list_stations_versioned())) for _ in range(3)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert all(lst == [] for lst, _, _ in out) and len({v for _, v, _ in out}) == 1
    assert not p._refresh_lock.locked()
    p._list_ts = 0.0                                  # next expiry refreshes again (today's semantics)
    lst, ver, _ = p.list_stations_versioned()
    assert lst == [] and ver == 2


def test_refresh_boundary_race_never_stores_old_list_under_new_key(monkeypatch):
    """Thread 1 snapshots version N; the provider re-publishes N+1 with different content; thread 2
    snapshots N+1. Each response must match ITS snapshot, and the memo must end holding N+1's bytes
    under N+1's key (never N's bytes under N+1's key)."""
    provs = F.make_providers()
    c = _install(monkeypatch, provs)
    aodn = next(p for p in provs if p.source == "AODN")
    first = c.get("/api/buoys/live-stations")               # memo = version-1 world
    real = A._build_live_stations_payload
    gate = threading.Event()

    def build_then_wait(lists):
        res = real(lists)
        gate.wait(2)                                         # hold the build while a refresh lands
        return res
    monkeypatch.setattr(A, "_build_live_stations_payload", build_then_wait)
    # Force a miss for thread 1 by clearing the memo (as if a different provider had bumped).
    A._LIVE_STATIONS_MEMO.update(key=None, payload=None, etag=None)
    r1 = {}
    t1 = threading.Thread(target=lambda: r1.update(resp=c.get("/api/buoys/live-stations")))
    t1.start()
    time.sleep(0.1)                                          # t1 is inside build_then_wait
    monkeypatch.setattr(F.FakeAODN, "rows", F.FakeAODN.rows + [
        {"local_id": "HOB", "name": "Hobart", "lat": -42.9, "lon": 147.4, "latest_time": F.FRESH}])
    aodn._list_ts = 0.0
    aodn.list_stations_versioned()                           # publishes version 2 while t1 builds
    gate.set()
    t1.join()
    monkeypatch.setattr(A, "_build_live_stations_payload", real)
    r2 = c.get("/api/buoys/live-stations")                   # thread 2: snapshot version 2
    assert r1["resp"].data == first.data                     # t1 served what it snapshotted
    assert any(s["id"] == "aodn:HOB" for s in r2.get_json())  # t2 sees the new world
    assert '"%s"' % A._LIVE_STATIONS_MEMO["etag"] == r2.headers["ETag"]
    assert "aodn:HOB" in A._LIVE_STATIONS_MEMO["payload"]        # memo holds version-2 bytes ...
    assert dict(A._LIVE_STATIONS_MEMO["key"])["AODN"] == 2        # ... under version-2's key
