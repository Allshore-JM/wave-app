"""Release D: every response states its cache policy; live data bypasses the edge by default.
Since plan section 36 the optional edge lifetime for live-stations is a NUMBER OF SECONDS
(LIVE_STATIONS_EDGE_TTL, 0 = no-store, capped at LIVE_STATIONS_EDGE_TTL_MAX): the lists are
refreshed in the background, so an edge HIT can no longer skip a refresh. A partial answer (a
provider still warming up) is never stored anywhere.
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
    monkeypatch.delenv("MODEL_OVERLAYS", raising=False)
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
    ("GET", "/no/such/path"), ("GET", "/overlay/overlay.js"),
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


# ------------------------------ flag on: a number of seconds, capped ------------------------

@pytest.mark.parametrize("raw,ttl", [
    ("", 0), ("0", 0), ("1", 1), ("60", 60), ("300", 300), ("900", 300), ("-5", 0), ("abc", 0), (" 120 ", 120),
])
def test_edge_ttl_is_seconds_capped(monkeypatch, raw, ttl):
    monkeypatch.setenv("LIVE_STATIONS_EDGE_TTL", raw)
    assert A._live_stations_edge_ttl() == ttl
    want = {"CDN-Cache-Control": "max-age=%d" % ttl} if ttl else {"CDN-Cache-Control": NO_STORE}
    assert A._live_stations_cdn_headers() == want
    assert A._live_stations_cdn_headers(partial=True) == {"CDN-Cache-Control": NO_STORE}


def test_flag_on_headers_on_200_and_304(monkeypatch):
    monkeypatch.setenv("LIVE_STATIONS_EDGE_TTL", "120")
    c, provs = _client(monkeypatch)
    r = c.get(LIVE)
    assert r.headers["CDN-Cache-Control"] == "max-age=120"
    assert r.headers["Cache-Control"] == "public, max-age=900"
    r304 = c.get(LIVE, headers={"If-None-Match": r.headers["ETag"]})
    assert r304.status_code == 304 and r304.headers["CDN-Cache-Control"] == "max-age=120"


def test_partial_answers_are_never_stored(monkeypatch):
    """With the background service on, a provider still missing makes the answer partial:
    no-store for the browser AND the edge, whatever the flag says."""
    monkeypatch.setenv("LIVE_STATIONS_EDGE_TTL", "120")
    c, provs = _client(monkeypatch)
    monkeypatch.setattr(A, "LIVE_BACKGROUND", True)
    monkeypatch.setattr(A, "start_live_background", lambda: True)   # no real scheduler thread
    monkeypatch.setattr(A, "_LIVE_BG", dict(A._LIVE_BG, ticks=0, prebuilds=0, errors=0, warm_ts=None))

    class Recorder:
        jobs = []

        def submit(self, fn, name="x"):
            self.jobs.append(fn)
    prev = B.set_refresh_runner(Recorder())
    try:
        r = c.get(LIVE)
        assert r.headers["X-Live-Stations-Partial"] and r.headers["CDN-Cache-Control"] == NO_STORE
        assert r.headers["Cache-Control"] == NO_STORE
        for fn in Recorder.jobs:
            fn()
        A._live_tick(provs)                                          # the scheduler's build
        r = c.get(LIVE)
        assert "X-Live-Stations-Partial" not in r.headers and r.headers["CDN-Cache-Control"] == "max-age=120"
    finally:
        B.set_refresh_runner(prev)


def test_flag_off_is_no_store(monkeypatch):
    c, _ = _client(monkeypatch)
    assert c.get(LIVE).headers["CDN-Cache-Control"] == NO_STORE


def test_healthz_is_never_cached(monkeypatch):
    c, _ = _client(monkeypatch)
    r = c.get("/healthz")
    assert r.headers["Cache-Control"] == NO_STORE and r.headers["CDN-Cache-Control"] == NO_STORE
