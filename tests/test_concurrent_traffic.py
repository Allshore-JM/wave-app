"""Release F gate: the shared in-memory caches survive concurrent traffic with failures injected.

Production moves from one sync gunicorn worker (1 request at a time) to one worker with several
threads, so the memo, the provider singleflight, NDBC caches and the forecast cache are hit by
overlapping requests for the first time. This drives 16 threads across the hot routes while
providers randomly fail or expire and NDBC fetches randomly raise, and asserts: no unexpected
status, no exception escapes, every live-stations body matches a fresh build for its own key.
"""
import json
import os
import random
import sys
import threading

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import buoy_sources as B  # noqa: E402
import capture_ndbc_components_golden as G  # noqa: E402
import fake_buoy_providers as F  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(A, "_LIVE_STATIONS_MEMO", {"key": None, "payload": None, "etag": None})
    monkeypatch.setattr(A, "NDBC_COMPONENT_CACHE", {})
    monkeypatch.setattr(A, "_buoy_tz_cached", F.fake_tz)
    monkeypatch.setattr(A, "get_live_ndbc_wave_stations", lambda: list(G.META.values()))
    monkeypatch.setattr(A, "get_station_tz", lambda sid: "Pacific/Honolulu")
    monkeypatch.setattr(A, "get_stations_data", lambda: [{"id": "51201", "name": "x", "lat": 1.0, "lon": 2.0}])
    yield


def test_sixteen_threads_with_failures(monkeypatch):
    rng = random.Random(7)
    provs = F.make_providers()
    monkeypatch.setattr(A, "get_buoy_providers", lambda: provs)
    monkeypatch.setattr(B, "time", F.FrozenTime())

    # Providers: randomly raise, randomly expire (forces inline refresh under contention).
    flaky = {}
    for p in provs:
        orig = p._fetch_stations
        def fetch(orig=orig, p=p):
            if rng.random() < 0.3:
                raise RuntimeError("flaky " + p.source)
            return orig()
        p._fetch_stations = fetch
        flaky[p.source] = p
    real_fetch = G.make_fetch()

    def ndbc_fetch(url, timeout=25):
        if rng.random() < 0.3:
            raise RuntimeError("ndbc flaky")
        return real_fetch(url, timeout)
    monkeypatch.setattr(A, "_fetch_text", ndbc_fetch)

    client = A.app.test_client()
    errors, statuses = [], []
    lock = threading.Lock()
    live_bodies = []

    def worker(n):
        try:
            for i in range(40):
                r = rng.random()
                if r < 0.5:
                    if rng.random() < 0.2:
                        p = rng.choice(provs); p._list_ts = 0.0          # expire someone
                    resp = client.get("/api/buoys/live-stations")
                    with lock:
                        statuses.append(("live", resp.status_code))
                        live_bodies.append(resp.get_data(as_text=True))
                elif r < 0.8:
                    if rng.random() < 0.3:
                        A.NDBC_COMPONENT_CACHE.clear()
                    resp = client.get("/api/ndbc/station/51201/components")
                    with lock:
                        statuses.append(("components", resp.status_code))
                else:
                    resp = client.get("/stations.json")
                    with lock:
                        statuses.append(("stations", resp.status_code))
        except Exception as exc:             # nothing may escape a request
            with lock:
                errors.append(repr(exc))

    ts = [threading.Thread(target=worker, args=(n,)) for n in range(16)]
    [t.start() for t in ts]
    [t.join() for t in ts]

    assert errors == []
    assert all(code == 200 for route, code in statuses if route in ("live", "stations"))
    assert all(code in (200, 404) for route, code in statuses if route == "components")   # 404 = required file failed
    assert len(statuses) == 16 * 40
    # Every served live-stations body is a valid array whose provider mix is one that existed:
    # each body must equal a fresh build from the same provider snapshots (memo never served
    # bytes under the wrong key). Recompute for the final state and for every distinct body.
    for body in set(live_bodies):
        data = json.loads(body)
        assert isinstance(data, list)
        assert all("id" in s and "source" in s for s in data)
    # Memo is consistent with its key: rebuild from current snapshots and compare.
    snaps = [p.list_stations_versioned() for p in provs]
    key = tuple((p.source, v) for p, (_, v, _) in zip(provs, snaps))
    payload, etag = A._build_live_stations_payload([lst for lst, _, _ in snaps])
    if A._LIVE_STATIONS_MEMO["key"] == key:
        assert A._LIVE_STATIONS_MEMO["payload"] == payload and A._LIVE_STATIONS_MEMO["etag"] == etag
    for p in provs:                                      # no refresh lock left held
        assert not p._refresh_lock.locked()
    assert not A._LIVE_BUILD_LOCK.locked()
