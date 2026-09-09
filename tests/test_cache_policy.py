"""Release D: every response states its cache policy; live data bypasses the edge by default;
the optional edge lifetime for live-stations is derived from the exact provider snapshots.
"""
import os
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import buoy_sources as B  # noqa: E402
import fake_buoy_providers as F  # noqa: E402

LIVE = "/api/buoys/live-stations"
NO_STORE = "no-store"


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(A, "_LIVE_STATIONS_MEMO", {"key": None, "payload": None, "etag": None})
    monkeypatch.setattr(A, "_buoy_tz_cached", F.fake_tz)
    monkeypatch.delenv("LIVE_STATIONS_EDGE_TTL", raising=False)
    yield


def _client(monkeypatch, provs=None, frozen=None):
    provs = provs or F.make_providers()
    monkeypatch.setattr(A, "get_buoy_providers", lambda: provs)
    monkeypatch.setattr(B, "time", frozen or F.FrozenTime())
    return A.app.test_client(), provs


# ------------------------------ default policy: not shareable ------------------------------

@pytest.mark.parametrize("method,path", [
    ("GET", "/"), ("GET", "/?station=51201&view=Graph"), ("POST", "/"),
    ("GET", "/api/forecast?station=51201"),
    ("GET", "/api/ndbc/station/51201/wave-summary"), ("GET", "/api/ndbc/station/51201/components"),
    ("GET", "/api/buoys/cdip:106/latest"), ("GET", "/api/buoys/nosuch:1/latest"),
    ("GET", "/no/such/path"),
])
def test_headerless_routes_get_the_private_default(monkeypatch, method, path):
    c, _ = _client(monkeypatch)
    monkeypatch.setattr(A, "_fetch_text", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("offline")))
    monkeypatch.setattr(A, "compute_forecast_payload", lambda *a, **k: {"error": "offline", "table_html": None,
                        "tz_label": "", "lat": None, "lon": None, "graph_data": None, "graph_header": None})
    monkeypatch.setattr(A, "_forecast_is_cached", lambda *a, **k: True)
    r = c.open(path, method=method, data={"station": "51201"} if method == "POST" else None)
    cc = r.headers.get("Cache-Control")
    if path == "/api/buoys/cdip:106/latest":
        assert cc == "public, max-age=300"                 # browser policy kept exactly ...
        assert r.headers["CDN-Cache-Control"] == "no-store"  # ... but never shared at the edge
    else:
        assert cc == A._DEFAULT_CACHE_CONTROL, (path, r.status_code, cc)
    assert "Set-Cookie" not in r.headers


def test_explicit_public_headers_are_unchanged(monkeypatch):
    c, _ = _client(monkeypatch)
    monkeypatch.setattr(A, "get_stations_data", lambda: [{"id": "51201", "name": "x", "lat": 1.0, "lon": 2.0}])
    r = c.get("/stations.json")
    assert r.headers["Cache-Control"] == "public, max-age=3600"
    assert r.headers["CDN-Cache-Control"] == "max-age=3600"
    assert c.get("/favicon.ico").headers["Cache-Control"] == "public, max-age=604800"
    r = c.get(LIVE)
    assert r.headers["Cache-Control"] == "public, max-age=900"
    r = c.get("/api/ndbc/live-wave-stations")
    assert r.headers["CDN-Cache-Control"] == "no-store"      # legacy list route: live data too


# ------------------------------ live-stations: BYPASS by default ---------------------------

def test_live_stations_edge_bypass_by_default(monkeypatch):
    c, _ = _client(monkeypatch)
    r = c.get(LIVE)
    assert r.headers["CDN-Cache-Control"] == NO_STORE
    r304 = c.get(LIVE, headers={"If-None-Match": r.headers["ETag"]})
    assert r304.status_code == 304 and r304.headers["CDN-Cache-Control"] == NO_STORE


# ------------------------------ flag on: lifetime from the snapshots -----------------------

def _snaps(provs, ages):
    """(list, version, ts) per provider with the given snapshot ages in seconds."""
    return [([], 1, F.NOW_EPOCH - age) for age in ages]


def test_edge_ttl_is_earliest_provider_remaining_validity(monkeypatch):
    provs = F.make_providers()          # TTLs: 1800,3600,21600,1800,1800,1800,1800,3600,21600,10800
    ages = [0] * len(provs)
    assert A._live_stations_edge_ttl(provs, _snaps(provs, ages), now=F.NOW_EPOCH) == 900   # capped
    ages[3] = 1800 - 60                 # AODN due in 60 s
    assert A._live_stations_edge_ttl(provs, _snaps(provs, ages), now=F.NOW_EPOCH) == 60
    ages[3] = 1800 - 1
    assert A._live_stations_edge_ttl(provs, _snaps(provs, ages), now=F.NOW_EPOCH) == 1
    ages[3] = 1800                      # due now
    assert A._live_stations_edge_ttl(provs, _snaps(provs, ages), now=F.NOW_EPOCH) == 0
    ages[3] = 1800 + 500                # past due (stale snapshot)
    assert A._live_stations_edge_ttl(provs, _snaps(provs, ages), now=F.NOW_EPOCH) < 0
    ages = [1800 - 120] * len(provs); ages[9] = 10800 - 30   # CMEMS earliest at 30 s
    assert A._live_stations_edge_ttl(provs, _snaps(provs, ages), now=F.NOW_EPOCH) == 30
    ages = [0] * len(provs)
    snaps = _snaps(provs, ages); snaps[5] = ([], None, None)   # a provider whose fetch raised
    assert A._live_stations_edge_ttl(provs, snaps, now=F.NOW_EPOCH) == 0


def test_flag_on_headers_follow_the_snapshots_and_never_extend(monkeypatch):
    monkeypatch.setenv("LIVE_STATIONS_EDGE_TTL", "1")
    frozen = F.FrozenTime()
    c, provs = _client(monkeypatch, frozen=frozen)
    monkeypatch.setattr(A.time, "time", lambda: frozen.now)        # route clock == provider clock
    r = c.get(LIVE)                                                  # every snapshot age 0
    assert r.headers["CDN-Cache-Control"] == "max-age=900"
    aodn = next(p for p in provs if p.source == "AODN")
    aodn._list_ts = frozen.now - (1800 - 60)                         # AODN due in 60 s
    r = c.get(LIVE)
    assert r.headers["CDN-Cache-Control"] == "max-age=60"
    r304 = c.get(LIVE, headers={"If-None-Match": r.headers["ETag"]})
    assert r304.status_code == 304 and r304.headers["CDN-Cache-Control"] == "max-age=60"
    # just before expiry: 1 s left; just after: the route refreshed AODN inline -> fresh snapshot
    frozen.now += 59
    assert c.get(LIVE).headers["CDN-Cache-Control"] == "max-age=1"
    fetches_before = F.FakeAODN.fetch_calls
    frozen.now += 1
    r = c.get(LIVE)
    assert F.FakeAODN.fetch_calls == fetches_before + 1              # refreshed exactly at expiry
    assert r.headers["CDN-Cache-Control"] == "max-age=900"           # new snapshot, full lifetime
    # a provider that raised inside the route -> not stored
    monkeypatch.setattr(aodn, "list_stations_versioned", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert c.get(LIVE).headers["CDN-Cache-Control"] == NO_STORE


def test_flag_off_ignores_snapshot_math(monkeypatch):
    c, _ = _client(monkeypatch)
    assert c.get(LIVE).headers["CDN-Cache-Control"] == NO_STORE
