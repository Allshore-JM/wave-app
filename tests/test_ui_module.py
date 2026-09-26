"""The page's own client module (static_ui/, the forecast window, plan section 25): its versioned route,
its pinned bytes, its Node unit tests, and the /api/forecast fields it relies on."""
import glob
import hashlib
import json
import os
import shutil
import subprocess

import pytest

import app as A

HERE = os.path.dirname(os.path.abspath(__file__))
UI = os.path.join(os.path.dirname(HERE), "static_ui")
NODE = shutil.which("node")


def test_ui_asset_route_versioning_headers_and_containment(monkeypatch):
    c = A.app.test_client()
    v = A.UI_ASSET_VERSION
    for flag in (None, "1"):                                                   # served whatever the overlay flag says
        if flag:
            monkeypatch.setenv("MODEL_OVERLAYS", flag)
        else:
            monkeypatch.delenv("MODEL_OVERLAYS", raising=False)
        r = c.get("/ui/forecast.js?v=" + v)
        assert r.status_code == 200 and r.headers["Content-Type"].startswith("application/javascript")
        assert r.headers["Cache-Control"] == "public, max-age=31536000, immutable"
        assert r.headers["CDN-Cache-Control"] == "max-age=31536000"
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        r304 = c.get("/ui/forecast.js?v=" + v, headers={"If-None-Match": r.headers["ETag"]})
        assert r304.status_code == 304 and r304.headers["Cache-Control"] == "public, max-age=31536000, immutable"
    for bad_v in ("", "?v=", "?v=1", "?v=" + v + "x"):                        # any other version: a 404 nobody may cache
        r = c.get("/ui/forecast.js" + bad_v)
        assert r.status_code == 404 and r.headers["Cache-Control"] == "no-store", bad_v
    for bad in ("../app.py", "app.py", "forecast.js/../../app.py", "nope.js", "overlay.js"):
        r = c.get("/ui/" + bad + "?v=" + v)
        assert r.status_code == 404, bad
    r = c.get("/ui/nope.js?v=" + v)                                            # an unknown name: the site's default 404
    assert r.headers["Content-Type"].startswith("text/html") and r.headers["Cache-Control"] == A._DEFAULT_CACHE_CONTROL


def test_ui_asset_version_bumped_with_the_assets():
    """Served immutable for a year under ?v=UI_ASSET_VERSION: any change to static_ui/* must come with a new
    version. tests/fixtures/ui_assets.json pins version -> sha256."""
    h = hashlib.sha256()
    for name in sorted(A._UI_ASSETS):
        h.update(open(os.path.join(UI, name), "rb").read().replace(b"\r\n", b"\n"))
    pinned = json.load(open(os.path.join(HERE, "fixtures", "ui_assets.json"), encoding="utf-8"))
    assert pinned["version"] == A.UI_ASSET_VERSION, "UI_ASSET_VERSION changed: update tests/fixtures/ui_assets.json"
    assert pinned["sha256"] == h.hexdigest(), ("static_ui/* changed: bump UI_ASSET_VERSION in app.py and "
                                              "update tests/fixtures/ui_assets.json (version + sha256)")


def test_ui_module_syntax_and_unit_tests():
    if not NODE:
        pytest.skip("node not available")
    for name in A._UI_ASSETS:
        r = subprocess.run(["node", "--check", os.path.join(UI, name)], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
    files = sorted(glob.glob(os.path.join(HERE, "ui", "*.test.js")))
    assert files
    r = subprocess.run(["node", "--test"] + files, capture_output=True, text=True, cwd=os.path.dirname(HERE))
    assert r.returncode == 0, (r.stdout[-3000:], r.stderr[-3000:])


def _stub_parsers(monkeypatch, calls):
    def fake(name):
        def parse(station, tz):
            calls.append((name, station))
            return (f"Cycle : 20260926 06 UTC", "Location : 21.67N 158.12W", "", None, "Pacific/Honolulu", "stub")
        return parse
    monkeypatch.setattr(A, "parse_bull", fake("GFS"))
    monkeypatch.setattr(A, "parse_swan", fake("SWAN"))


@pytest.mark.parametrize("station, asked, used, avail", [
    ("51201", "SWAN", "SWAN", True),
    ("51201", "", "GFS", True),
    ("51201", "gfs", "GFS", True),
    ("46001", "SWAN", "GFS", False),                                           # not a SWAN station: falls back to GFS
    ("46001", "", "GFS", False),
])
def test_api_forecast_reports_the_model_used_and_swan_availability(monkeypatch, station, asked, used, avail):
    calls = []
    _stub_parsers(monkeypatch, calls)
    d = A.app.test_client().get(f"/api/forecast?station={station}&model={asked}").get_json()
    assert d["model"] == used and d["swan_available"] is avail
    assert calls == [(used, station)], "the parser of the reported model ran"


def test_api_forecast_error_fallback_carries_the_new_fields(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("down")
    monkeypatch.setattr(A, "compute_forecast_payload", boom)
    d = A.app.test_client().get("/api/forecast?station=51202&model=SWAN").get_json()
    assert d["error"] == "Forecast temporarily unavailable"
    assert d["model"] == "SWAN" and d["swan_available"] is True
