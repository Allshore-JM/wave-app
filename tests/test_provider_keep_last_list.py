"""Bug fix: a FAILED provider refresh no longer replaces a good station list with [] for a whole
TTL. The last good list is kept (same version -> the live-stations memo serves identical bytes),
the provider is retried after retry_after_sec, and a legitimately EMPTY successful fetch still
replaces the list as before. First-ever failure (no good list yet) is unchanged: [] for the TTL.

Golden: tests/fixtures/live_stations_golden.json (unchanged code) must still replay byte for byte
-- its SMHI provider fails from the start, i.e. the unchanged first-failure path.
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import buoy_sources as B  # noqa: E402
import fake_buoy_providers as F  # noqa: E402
from test_live_stations_memo import test_golden_sequence_byte_identical as _golden  # noqa: E402,F401


@pytest.fixture
def clock(monkeypatch):
    frozen = F.FrozenTime()
    monkeypatch.setattr(B, "time", frozen)
    return frozen


def _flaky(p, fail):
    """Make p's fetch fail while fail[0] is True; counts EVERY attempt in fail[1]."""
    orig = p._fetch_stations
    fail.append(0)

    def fetch():
        fail[1] += 1
        if fail[0]:
            raise RuntimeError("upstream down")
        return orig()
    p._fetch_stations = fetch


def test_golden_replay_still_identical(monkeypatch):
    _golden(monkeypatch)


def test_failed_refresh_keeps_last_good_list_and_version(clock):
    p = F.FakeCDIP(http=None)
    fail = [False]
    _flaky(p, fail)
    lst1, v1, _ = p.list_stations_versioned()
    assert len(lst1) == 2 and v1 == 1
    clock.now += p.list_ttl_sec + 1                  # expired
    fail[0] = True
    lst2, v2, ts2 = p.list_stations_versioned()
    assert lst2 == lst1 and v2 == v1                 # kept, same publish -> same memo key/bytes
    # retried after retry_after_sec, not after a full TTL
    clock.now += p.retry_after_sec - 1
    assert p.list_stations_versioned()[1] == v1 and p._list_cache == lst1
    assert fail[1] == 2                              # no new attempt yet
    clock.now += 2
    p.list_stations_versioned()
    assert fail[1] == 3                              # retried (still failing) -> still kept
    assert p._list_cache == lst1
    fail[0] = False
    clock.now += p.retry_after_sec + 1
    lst3, v3, _ = p.list_stations_versioned()
    assert lst3 == lst1 and v3 == v1 + 1             # success publishes a new version


def test_first_failure_is_unchanged_empty_for_ttl(clock):
    p = F.FakeSMHI(http=None)                        # raises from the start
    lst, v, _ = p.list_stations_versioned()
    assert lst == [] and v == 1
    clock.now += p.retry_after_sec + 1
    assert p.list_stations_versioned()[1] == 1       # no retry before the full TTL (as before)
    clock.now += p.list_ttl_sec
    assert p.list_stations_versioned()[1] == 2


def test_successful_empty_list_still_replaces(clock):
    p = F.FakeCDIP(http=None)
    p.list_stations_versioned()
    clock.now += p.list_ttl_sec + 1
    p._fetch_stations = lambda: []                   # legitimately empty
    lst, v, _ = p.list_stations_versioned()
    assert lst == [] and v == 2


def test_retention_is_bounded(clock):
    p = F.FakeCDIP(http=None)
    fail = [False]
    _flaky(p, fail)
    lst1, _, _ = p.list_stations_versioned()
    fail[0] = True
    clock.now += p.keep_on_failure_sec + 1           # good list now too old to keep
    lst, v, _ = p.list_stations_versioned()
    assert lst == [] and v == 2                      # old fail-soft behaviour resumes


def test_route_serves_identical_bytes_through_a_failed_refresh(monkeypatch, clock):
    provs = F.make_providers()
    monkeypatch.setattr(A, "get_buoy_providers", lambda: provs)
    monkeypatch.setattr(A, "_buoy_tz_cached", F.fake_tz)
    monkeypatch.setattr(A, "_LIVE_STATIONS_MEMO", {"key": None, "payload": None, "etag": None})
    c = A.app.test_client()
    r1 = c.get("/api/buoys/live-stations")
    aodn = next(p for p in provs if p.source == "AODN")
    aodn._fetch_stations = lambda: (_ for _ in ()).throw(RuntimeError("AODN down"))
    aodn._list_ts = 0.0                              # expire ONLY AODN
    builds = []
    real = A._build_live_stations_payload
    monkeypatch.setattr(A, "_build_live_stations_payload", lambda lists: builds.append(1) or real(lists))
    r2 = c.get("/api/buoys/live-stations")
    assert r2.data == r1.data and r2.headers["ETag"] == r1.headers["ETag"]
    assert builds == []                              # memo hit: versions unchanged
    assert any(s["source"] == "AODN" for s in r2.get_json())   # markers still on the map
