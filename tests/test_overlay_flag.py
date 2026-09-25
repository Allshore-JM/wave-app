"""Model overlays: the page is byte-identical with the feature off; assets and markup only with it on.

Golden: tests/fixtures/index_golden.json captured by tests/capture_index_golden.py on the code
BEFORE any overlay change (fixed forecast payload + station list, no network). Never recapture it
after an overlay commit (regenerate from Live-Buoy-Update @ 4c4760a or earlier if it must change).
"""
import glob
import json
import os
import re
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import capture_index_golden as G  # noqa: E402

GOLDEN = json.load(open(os.path.join(HERE, "fixtures", "index_golden.json"), encoding="utf-8"))
STATIC = os.path.join(os.path.dirname(HERE), "static_overlay")
NODE = subprocess.run(["node", "--version"], capture_output=True).returncode == 0
# the whole gated block: a <style> for the selector and the bootstrap <script>
GATED = re.compile(r"  <style>\n    /\* Optional model overlays.*?</script>\n\n", re.S)


def _on(monkeypatch, base="https://frames.example/gfswave/0p25/v1/"):
    monkeypatch.setenv("MODEL_OVERLAYS", "1")
    monkeypatch.setenv("MODEL_FRAMES_BASE", base)
    monkeypatch.setattr(A, "get_station_tz", lambda sid: "Pacific/Honolulu")


def test_flag_off_page_is_byte_identical(monkeypatch):
    monkeypatch.delenv("MODEL_OVERLAYS", raising=False)
    monkeypatch.setenv("MODEL_FRAMES_BASE", "https://frames.example/gfswave/0p25/v1")
    rec = G.run_scenarios(A)
    for name, old in GOLDEN.items():
        assert rec[name]["status"] == old["status"], name
        assert rec[name]["body"] == old["body"], "flag-off HTML changed in scenario %s" % name
        assert rec[name]["headers"] == old["headers"], name
        assert "/overlay/" not in rec[name]["body"] and "modelPane" not in rec[name]["body"]


def test_flag_on_without_a_frames_base_is_off(monkeypatch):
    monkeypatch.setenv("MODEL_OVERLAYS", "1")
    monkeypatch.setenv("MODEL_FRAMES_BASE", "")
    rec = G.run_scenarios(A)
    for name, old in GOLDEN.items():
        assert rec[name]["body"] == old["body"], name


def test_flag_on_adds_only_the_gated_block(monkeypatch):
    _on(monkeypatch)
    rec = G.run_scenarios(A)
    for name, old in GOLDEN.items():
        body = rec[name]["body"]
        assert 'id="ovField"' in body and "AllshoreOverlay" in body and "/overlay/overlay.js?v=" in body
        assert "createPane" not in body                                      # the pane is created by the module on first use
        assert 'autocomplete="off"' in body
        # the chosen layer survives a reload in this tab (A1): read back from the module's session key and
        # restored after load, only while visible; a user's pick mounts without the saved time
        assert "'allshore.overlay.v1'" in body and "choose(again, true)" in body and "overlay.mount(field, restore)" in body
        assert "choose(v, pending && v !== ''); remember(v, fresh)" in body and "document.hidden" in body
        # G8: the back/forward cache re-syncs the save; the restore waits for a deferred table (3 s at most)
        assert "'pageshow'" in body and "'forecastLoading'" in body and "setTimeout(start, 5000)" in body
        assert '"https://frames.example/gfswave/0p25/v1"' in body           # trailing slash stripped
        assert "var VERSION = %s;" % json.dumps(A.OVERLAY_ASSET_VERSION) in body
        assert GATED.search(body) is not None, name
        # removing the gated block gives back exactly the golden page
        assert GATED.sub("", body, count=1) == old["body"], name


def test_forecast_tz_follows_the_table_rule(monkeypatch):
    _on(monkeypatch)
    rec = G.run_scenarios(A)
    assert 'var TZ = "HST";' in rec["table_inline"]["body"]                 # the rendered table's own label (fixture payload)
    assert 'var TZ = "HST";' in rec["graph"]["body"]
    assert 'var TZ = "Pacific/Honolulu";' in rec["table_deferred"]["body"]  # no payload: the station's zone
    saved = {k: getattr(A, k) for k in ("_forecast_is_cached", "get_station_list")}
    A.get_station_list = lambda: list(G.STATIONS)
    A._forecast_is_cached = lambda *a, **k: False                          # deferred table: the parsers' rule is mirrored
    try:
        c = A.app.test_client()
        page = lambda q: c.get("/?station=51201" + q).get_data(as_text=True)  # noqa: E731
        assert 'var TZ = "Europe/Lisbon";' in page("&tz=Europe/Lisbon")       # valid explicit zone wins
        assert 'var TZ = "Pacific/Honolulu";' in page("&tz=Nowhere/Land")     # invalid one falls back to the station
        assert 'var TZ = "Europe/Lisbon";' in page("&tz=europe/lisbon")       # canonical IANA spelling (pytz is lenient)
        monkeypatch.setattr(A, "get_station_tz", lambda sid: None)
        monkeypatch.setattr(A, "load_station_coords", lambda: {"51201": {"lat": 21.67, "lon": -158.12}})
        monkeypatch.setattr(A, "_safe_tzname_for_latlon", lambda lat, lon: "Pacific/Honolulu")
        assert 'var TZ = "Pacific/Honolulu";' in page("")                     # unmapped station: coordinate lookup
        monkeypatch.setattr(A, "load_station_coords", lambda: {})
        assert 'var TZ = "UTC";' in page("")
    finally:
        for k, v in saved.items():
            setattr(A, k, v)


def test_asset_route_versioning_headers_and_containment(monkeypatch):
    c = A.app.test_client()
    v = A.OVERLAY_ASSET_VERSION
    monkeypatch.delenv("MODEL_OVERLAYS", raising=False)
    r = c.get("/overlay/overlay.js?v=" + v)                                  # feature off: nothing served
    assert r.status_code == 404 and r.headers["Cache-Control"] == A._DEFAULT_CACHE_CONTROL
    assert r.headers["Content-Type"].startswith("text/html")                # the site's default 404, not a JSON one
    assert r.get_data() == c.get("/no/such/path").get_data()
    _on(monkeypatch)
    for name, ct in (("overlay.js", "application/javascript"), ("overlay.css", "text/css")):
        r = c.get("/overlay/%s?v=%s" % (name, v))
        assert r.status_code == 200 and r.headers["Content-Type"].startswith(ct)
        assert r.headers["Cache-Control"] == "public, max-age=31536000, immutable"
        assert r.headers["CDN-Cache-Control"] == "max-age=31536000"
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        r304 = c.get("/overlay/%s?v=%s" % (name, v), headers={"If-None-Match": r.headers["ETag"]})
        assert r304.status_code == 304
        assert r304.headers["Cache-Control"] == "public, max-age=31536000, immutable"
        assert r304.headers["CDN-Cache-Control"] == "max-age=31536000"
    for bad_v in ("", "?v=", "?v=1", "?v=" + v + "x", "?v=2.0.4"):          # any other version: 404 nobody may cache
        r = c.get("/overlay/overlay.js" + bad_v)
        assert r.status_code == 404 and r.headers["Cache-Control"] == "no-store", bad_v
    for bad in ("../app.py", "app.py", "overlay.js/../../app.py", "nope.js"):
        assert c.get("/overlay/" + bad + "?v=" + v).status_code == 404


def test_asset_version_bumped_with_the_assets():
    """The assets are served immutable for a year under ?v=OVERLAY_ASSET_VERSION: any change to them
    must come with a new version. tests/fixtures/overlay_assets.json pins version -> sha256."""
    import hashlib
    h = hashlib.sha256()
    for name in ("overlay.js", "overlay.css"):
        h.update(open(os.path.join(STATIC, name), "rb").read().replace(b"\r\n", b"\n"))
    pinned = json.load(open(os.path.join(HERE, "fixtures", "overlay_assets.json"), encoding="utf-8"))
    assert pinned["version"] == A.OVERLAY_ASSET_VERSION, "OVERLAY_ASSET_VERSION changed: update tests/fixtures/overlay_assets.json"
    assert pinned["sha256"] == h.hexdigest(), ("static_overlay/* changed: bump OVERLAY_ASSET_VERSION in app.py and "
                                              "update tests/fixtures/overlay_assets.json (version + sha256)")


def test_overlay_js_syntax_and_contract_strings():
    js = open(os.path.join(STATIC, "overlay.js"), encoding="utf-8").read()
    for needle in ("'modelPane'", "pointerEvents", "createImageBitmap", "valueAt", "unmount", "AllshoreOverlay",
                   "_code", "tileCodes", "validateGrid", "validateManifest", "u8-linear-v2", "{step:03d}",
                   "(hover: hover) and (pointer: fine)", "(pointer: coarse)", "ov-sheet-open", "--ov-sheet-left",
                   "--ov-attr-max", "removeAttribution", "clientHeight", "snapToPixel",
                   "decodeCoast", "composeTile", "readoutAt", "contourTile", "legendPos", "GSHHG", "static/coast/v1", "LICENSE.txt",
                   "smoothBlock", "'pagehide'", "_pendingRestore", "'Contours, every '", "below zoom 4"):
        assert needle in js, needle
    assert "innerHTML" not in js                                             # every label is text (manifest strings never HTML)
    if NODE:
        r = subprocess.run(["node", "--check", os.path.join(STATIC, "overlay.js")], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr


def test_overlay_module_unit_tests():
    if not NODE:
        pytest.skip("node not available")
    files = sorted(glob.glob(os.path.join(HERE, "overlay", "*.test.js")))
    assert files
    r = subprocess.run(["node", "--test"] + files, capture_output=True, text=True, cwd=os.path.dirname(HERE))
    assert r.returncode == 0, (r.stdout[-3000:], r.stderr[-3000:])


def test_bootstrap_node_test_runs_the_rendered_script(monkeypatch):
    """tests/overlay/bootstrap.test.js runs the gated <script> taken from the template with its three
    {{ ...|tojson }} expressions substituted; this pins that to exactly what Flask renders."""
    base = "https://frames.example/gfswave/0p25/v1"
    _on(monkeypatch, base + "/")
    rec = G.run_scenarios(A)
    tpl = open(os.path.join(os.path.dirname(HERE), "templates", "index.html"), encoding="utf-8").read().replace("\r\n", "\n")
    a = tpl.index("{%- if model_overlays %}")
    block = tpl[a:tpl.index("{%- endif %}", a)]
    src = block[block.index("<script>") + len("<script>"):block.index("</script>")]
    for name, tz in (("table_inline", "HST"), ("table_deferred", "Pacific/Honolulu")):
        body = rec[name]["body"].replace("\r\n", "\n")
        got = [b for b in re.findall(r"<script(?![^>]*src=)[^>]*>(.*?)</script>", body, flags=re.S) if "Optional model overlays" in b]
        assert len(got) == 1, name
        want = (src.replace("{{ model_frames_base|tojson }}", json.dumps(base))
                   .replace("{{ overlay_asset_version|tojson }}", json.dumps(A.OVERLAY_ASSET_VERSION))
                   .replace("{{ forecast_tz_name|tojson }}", json.dumps(tz)))
        assert got[0] == want, name
    node_src = open(os.path.join(HERE, "overlay", "bootstrap.test.js"), encoding="utf-8").read()
    assert "{%- if model_overlays %}" in node_src and "|tojson }}" in node_src          # the same extraction


def test_rendered_inline_scripts_parse_with_flag_on(monkeypatch):
    if not NODE:
        pytest.skip("node not available")
    _on(monkeypatch, "https://frames.example/x")
    rec = G.run_scenarios(A)
    for name in ("table_inline", "graph", "swan_station_model"):
        blocks = re.findall(r"<script(?![^>]*src=)[^>]*>(.*?)</script>", rec[name]["body"], flags=re.S)
        assert len(blocks) >= 4
        assert any("Optional model overlays" in b for b in blocks)
        for i, b in enumerate(blocks):
            path = os.path.join(HERE, f"_blk_{name}_{i}.js")
            open(path, "w", encoding="utf-8").write(b)
            try:
                r = subprocess.run(["node", "--check", path], capture_output=True, text=True)
                assert r.returncode == 0, (name, i, r.stderr[:300])
            finally:
                os.remove(path)
