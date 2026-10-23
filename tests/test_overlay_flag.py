"""Model overlays: the page is byte-identical with the feature off; assets and markup only with it on.

Golden: tests/fixtures/index_golden.json captured by tests/capture_index_golden.py (fixed forecast
payload + station list, no network). It is re-baselined ONLY by a commit that changes the flag-off page
on purpose (last: UI 1.25.0, the location aids' gear rows and script, 2026-10-23); never recapture it
after an overlay-only commit. Whatever the baseline, three invariants hold: flag off == golden with no
'/overlay/' in it; flag on with no frames base == flag off; flag on == flag off + exactly the gated block.
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
    monkeypatch.delenv("COAST_BASE", raising=False)
    monkeypatch.delenv("POINTS_ROOT", raising=False)
    monkeypatch.delenv("MODEL_OVERLAY_MODELS", raising=False)
    monkeypatch.delenv("SST_OVERLAY", raising=False)
    monkeypatch.delenv("GLWU_OVERLAY", raising=False)
    monkeypatch.delenv("GEO_BASE", raising=False)
    monkeypatch.delenv("PLACES_BASE", raising=False)
    monkeypatch.setenv("MODEL_OVERLAYS", "1")
    monkeypatch.setenv("MODEL_FRAMES_BASE", base)
    monkeypatch.setattr(A, "get_station_tz", lambda sid: "Pacific/Honolulu")


def test_flag_off_page_is_byte_identical(monkeypatch):
    monkeypatch.delenv("COAST_BASE", raising=False)                     # a developer's COAST_BASE would change the page
    monkeypatch.delenv("MODEL_OVERLAYS", raising=False)
    monkeypatch.delenv("POINTS_ROOT", raising=False)
    monkeypatch.delenv("GEO_BASE", raising=False)                       # the geo Worker (section 46): the golden has GEO ""
    monkeypatch.delenv("PLACES_BASE", raising=False)                    # the location aids (section 51): the golden derives it
    monkeypatch.setenv("MODEL_FRAMES_BASE", "https://frames.example/gfswave/0p25/v1")
    rec = G.run_scenarios(A)
    for name, old in GOLDEN.items():
        assert rec[name]["status"] == old["status"], name
        assert rec[name]["body"] == old["body"], "flag-off HTML changed in scenario %s" % name
        assert rec[name]["headers"] == old["headers"], name
        assert "/overlay/" not in rec[name]["body"] and "modelPane" not in rec[name]["body"]


def test_flag_on_without_a_frames_base_is_off(monkeypatch):
    monkeypatch.delenv("COAST_BASE", raising=False)                     # a developer's COAST_BASE would change the page
    monkeypatch.setenv("MODEL_OVERLAYS", "1")
    monkeypatch.setenv("MODEL_FRAMES_BASE", "")
    monkeypatch.setenv("POINTS_ROOT", "https://frames.example")         # forecast points stay on: this test is the overlay's
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
        assert "'allshore:forecast'" in body and "overlay.opts.tz = lastTz" in body and "tz: lastTz" in body   # G16: the zone follows the window, first mount included
        assert '"https://frames.example/gfswave/0p25/v1"' in body           # trailing slash stripped
        assert "var VERSION = %s;" % json.dumps(A.OVERLAY_ASSET_VERSION) in body
        assert "version: VERSION" in body                                    # the module serves its illustrations at its version
        assert 'aria-label="Map overlay"' in body and "Model overlay <select" not in body   # no visible heading (section 37)
        assert 'var MODELS = ["gfswave"];' in body and "models: MODELS" in body              # section 43: the offered models
        assert "var SST = false;" in body and "sst: SST" in body and 'value="sst"' in body     # section 44: offered only with SST_OVERLAY=1 (the option is built by script)
        assert "var GLWU = false;" in body and "glwu: GLWU" in body and "return { model: 'GLWU' }" in body   # section 50: the Great Lakes under GFS (GLWU_OVERLAY=1)
        assert "'allshore.isobars.v1'" in body and "'allshore.particles.v1'" in body               # section 46: the gear's two rows, kept in this browser
        assert "isobars: ISOBARS, particles: PARTICLES / 100" in body and "'ovIsobars'" in body and "'ovParticles'" in body
        assert "overlay.setIsobars(ISOBARS)" in body and "overlay.setParticleOpacity(v / 100)" in body and "'Pressure isobars (wind)'" in body
        assert "'allshore.lakeOutlines.v1'" in body and "lakeOutlines: LAKE_OUTLINES" in body and "'ovLakeOutlines'" in body   # section 51: the lakes' shores under wind
        assert "overlay.setLakeOutlines(LAKE_OUTLINES)" in body and "'Lake outlines (wind)'" in body
        assert GATED.search(body) is not None, name
        # removing the gated block gives back exactly the golden page
        assert GATED.sub("", body, count=1) == old["body"], name


def test_forecast_tz_follows_the_table_rule(monkeypatch):
    _on(monkeypatch)
    rec = G.run_scenarios(A)
    assert 'var TZ = "HST";' in rec["render_full"]["body"]                  # the rendered table's own label (fixture payload)
    assert 'var TZ = "Pacific/Honolulu";' in rec["graph"]["body"]           # a shell (the window fetches): the station's zone
    assert 'var TZ = "Pacific/Honolulu";' in rec["default"]["body"]
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
    for name, ct in (("overlay.js", "application/javascript"), ("overlay.css", "text/css"), ("snail.png", "image/png"),
                     ("fish.png", "image/png"), ("shark.png", "image/png"), ("terrain_half.png", "image/png")):
        r = c.get("/overlay/%s?v=%s" % (name, v))
        assert r.status_code == 200 and r.headers["Content-Type"].startswith(ct)
        if name.endswith(".png"):
            assert r.get_data()[:8] == b"\x89PNG\r\n\x1a\n", name
        assert r.headers["Cache-Control"] == "public, max-age=31536000, immutable"
        assert r.headers["CDN-Cache-Control"] == "max-age=31536000"
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        r304 = c.get("/overlay/%s?v=%s" % (name, v), headers={"If-None-Match": r.headers["ETag"]})
        assert r304.status_code == 304
        assert r304.headers["Cache-Control"] == "public, max-age=31536000, immutable"
        assert r304.headers["CDN-Cache-Control"] == "max-age=31536000"
    for bad_v in ("", "?v=", "?v=1", "?v=" + v + "x", "?v=2.0.4"):          # any other version: 404 nobody may cache
        for name in ("overlay.js", "fish.png"):
            r = c.get("/overlay/" + name + bad_v)
            assert r.status_code == 404 and r.headers["Cache-Control"] == "no-store", (name, bad_v)
    for bad in ("../app.py", "app.py", "overlay.js/../../app.py", "nope.js"):
        assert c.get("/overlay/" + bad + "?v=" + v).status_code == 404


def test_asset_version_bumped_with_the_assets():
    """The assets are served immutable for a year under ?v=OVERLAY_ASSET_VERSION: any change to them
    must come with a new version. tests/fixtures/overlay_assets.json pins version -> sha256."""
    import hashlib
    h = hashlib.sha256()
    for name in ("overlay.js", "overlay.css"):
        h.update(open(os.path.join(STATIC, name), "rb").read().replace(b"\r\n", b"\n"))
    for name in OVERLAY_IMAGES:                                               # binary: hashed raw (the PNG signature holds \r\n)
        h.update(open(os.path.join(STATIC, name), "rb").read())
    assert sorted(A._OVERLAY_ASSETS) == sorted(("overlay.js", "overlay.css") + OVERLAY_IMAGES), "every served overlay asset is pinned"
    pinned = json.load(open(os.path.join(HERE, "fixtures", "overlay_assets.json"), encoding="utf-8"))
    assert pinned["version"] == A.OVERLAY_ASSET_VERSION, "OVERLAY_ASSET_VERSION changed: update tests/fixtures/overlay_assets.json"
    assert pinned["sha256"] == h.hexdigest(), ("static_overlay/* changed: bump OVERLAY_ASSET_VERSION in app.py and "
                                              "update tests/fixtures/overlay_assets.json (version + sha256 of the js, the css "
                                              "and the PNGs)")


OVERLAY_ANIMALS = ("snail.png", "fish.png", "shark.png")
OVERLAY_IMAGES = OVERLAY_ANIMALS + ("terrain_half.png",)


def test_terrain_mask_is_the_half_grid_as_8_bit_grey():
    """The isobars' high-ground mask (section 46, fix round 3): tools/model_frames/make_terrain_mask.py -> the frames' half
    grid (720 x 361), one grey byte per node (20 m each), small enough for one immutable fetch; the Big Island high, the sea 0."""
    import struct
    raw = open(os.path.join(STATIC, "terrain_half.png"), "rb").read()
    assert raw[:8] == b"\x89PNG\r\n\x1a\n"
    w, h, depth, ctype = struct.unpack(">IIBB", raw[16:26])
    assert (w, h, depth, ctype) == (720, 361, 8, 0), (w, h, depth, ctype)
    assert len(raw) < 120000, len(raw)
    try:
        from PIL import Image
    except ImportError:                                    # the CI job without Pillow pins the header only
        return
    import io as _io
    px = Image.open(_io.BytesIO(raw)).load()
    node = lambda lat, lon: px[int(round((lon + 180) / 0.5)) % 720, int(round((90 - lat) / 0.5))]   # noqa: E731
    assert node(19.5, -155.5) * 20 >= 2000, "the Big Island"
    assert node(32.0, 88.0) * 20 >= 4000, "Tibet"
    assert node(21.0, -157.5) * 20 < 500 and node(52.5, 5.0) == 0 and node(0.0, -140.0) == 0, "the sea"


def test_speed_illustrations_are_small_transparent_square_pngs():
    """The owner's art for the speed selector (section 37): RGBA PNGs, square, small enough for an immutable
    first-load fetch of three icons."""
    import struct
    for name in OVERLAY_ANIMALS:
        raw = open(os.path.join(STATIC, name), "rb").read()
        assert raw[:8] == b"\x89PNG\r\n\x1a\n", name
        w, h, depth, ctype = struct.unpack(">IIBB", raw[16:26])
        assert w == h and 96 <= w <= 256, (name, w, h)
        assert ctype == 6 or b"tRNS" in raw, (name, "no transparency")
        assert len(raw) < 60000, (name, len(raw))


def test_overlay_js_syntax_and_contract_strings():
    js = open(os.path.join(STATIC, "overlay.js"), encoding="utf-8").read()
    for needle in ("'modelPane'", "pointerEvents", "createImageBitmap", "valueAt", "unmount", "AllshoreOverlay",
                   "_code", "tileCodes", "validateGrid", "validateManifest", "u8-linear-v2", "{step:03d}",
                   "(hover: hover) and (pointer: fine)", "(pointer: coarse)", "ov-sheet-open", "--ov-sheet-left",
                   "--ov-attr-max", "removeAttribution", "clientHeight", "snapToPixel",
                   "decodeCoast", "composeTile", "readoutAt", "contourTile", "legendPos", "GSHHG", "static/coast/v1", "LICENSE.txt",
                   "smoothBlock", "'pagehide'", "_pendingRestore",
                   "FlowAnimator", "'ovAnimPane'", "flowField", "leaflet-zoom-hide",
                   "FIXED_OPACITY",
                   "frameAtHour", "'Updated: '", "'Next Update: '", "'expected shortly'", "'a newer run is available'",
                   "SPEED_ANIMALS", "'snail.png'", "'fish.png'", "'shark.png'", "'listbox'", "'option'", "ov-playing", "speedOf",
                   "ov-ribbon", "'slider'", "aria-valuetext", "_scrubTo", "ribbonLayout", "RibbonState", "weekday: 'short'", "WEEKDAY_NAMES",
                   "_summaryLine", "stepAvailable", "ResizeObserver", "'orientationchange'", "ov-play-head", "dotMonth", "_ctlTop", "ovSpeedMenu", "lineTimer",
                   "RIBBON_AXIS_PX", "_focusedPart", "_refocus", "_say", "'status'", "ov-sr", "_pendingFocus", "_focusLost", "hasPointerCapture", "SAY_FIRST_MS", "getComputedStyle",
                   "World_Hillshade", "mixBlendMode", "coastEdges", "_syncLook",
                   "_paintWait", "PAINT_FALLBACK_MS", "TRAIL_SPAN_MS", "BUSY_STUCK_MS", "maxTouchPoints", "_flowData", "copyWithin", "spent", "ovdecode=worker",   # section 42 / G29
                   "MODEL_DEFS", "'ecmwf/0p25/v1'", "ov-model-sel", "'Overlay model'", "setModel", "_dropRun", "_carry", "_modelBase", "CC BY 4.0",   # section 43
                   "hasModel", "_switchT", "_fetchJson", "LOAD_STALL_MS", "/notice/ecmwf",                                                       # G30 fix round
                   "IsobarLayer", "'ovIsoPane'", "traceIsolines", "isoBlur", "findCentres", "projectCentres", "placeLabels", "viewOf", "placeCanvas", "PRES_WAIT_MS",                        # section 46 step 3
                   "PRES_FIELD", "pcache", "_wantPres", "_kindReady", "_startKind", "_syncIsobars", "setIsobars", "' hPa'",                  # section 46 step 4
                   "particleOpacityOf", "setParticleOpacity", "_paneOpacity", "opts.particles",                                            # section 46 step 5
                   "prominenceMap", "ISO_HL_PROM_CODES", "'terrain_half.png'", "ISO_TERRAIN_M", "setTerrain", "_hpaText", "self._hpaText(latlng.lat, latlng.lng)", "destination-out", "_presWait", "ISO_CLAMP_PATCH_MAX",
                   "LAKEOUT_TIER0", "'lakeoutlines-i.bin'", "'static/coast/lakeoutlines-v1'", "setLakeOutlineCoast", "_lakeOutFor", "_syncLakeOutlines", "setLakeOutlines", "self._syncLakeOutlines(); });"):   # section 51 step 3   # fix round 3 (the readout's call site: its closure is not driven by the Node harness)
        assert needle in js, needle
    for gone in ("'Opacity '", "Overlay opacity", "' Contours'", "' Animation'", "opacityWind",   # owner 2026-09-26: no settings row
                 "'live since '", "next update about ", "'First frame'", "'Previous frame'", "'Next frame'", "'Last frame'",
                 "[0.5, 1, 2, 4]", "'Valid: '", "forecastSpanText", "-day forecast",
                 "' h → +'", "'NOAA GFS-Wave'", "SHEET_OPEN_PX", "RIBBON_SWIPE_PX", "aria-live", "'Forecast model'", "ISO_HL_MIN_HPA"):                                                                   # owner 2026-10-07: one summary line, no agency prefix                                                       # section 37: one play button, animals, the ribbon
        assert gone not in js, gone
    assert "innerHTML" not in js                                             # every label is text (manifest strings never HTML)
    if NODE:
        r = subprocess.run(["node", "--check", os.path.join(STATIC, "overlay.js")], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr


def test_overlay_module_unit_tests():
    if not NODE:
        pytest.skip("node not available")
    files = sorted(glob.glob(os.path.join(HERE, "overlay", "*.test.js")))
    assert files
    r = subprocess.run(["node", "--test", "--test-timeout=20000"] + files, capture_output=True, text=True, cwd=os.path.dirname(HERE))
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
    for name, tz in (("render_full", "HST"), ("default", "Pacific/Honolulu")):
        body = rec[name]["body"].replace("\r\n", "\n")
        got = [b for b in re.findall(r"<script(?![^>]*src=)[^>]*>(.*?)</script>", body, flags=re.S) if "Optional model overlays" in b]
        assert len(got) == 1, name
        want = (src.replace("{{ model_frames_base|tojson }}", json.dumps(base))
                   .replace("{{ overlay_asset_version|tojson }}", json.dumps(A.OVERLAY_ASSET_VERSION))
                   .replace("{{ overlay_models|tojson }}", json.dumps(["gfswave"]))
                   .replace("{{ overlay_sst|tojson }}", json.dumps(False))
                   .replace("{{ overlay_glwu|tojson }}", json.dumps(False))
                   .replace("{{ forecast_tz_name|tojson }}", json.dumps(tz)))
        assert got[0] == want, name
    node_src = open(os.path.join(HERE, "overlay", "bootstrap.test.js"), encoding="utf-8").read()
    assert "{%- if model_overlays %}" in node_src and "|tojson }}" in node_src          # the same extraction


def test_rendered_inline_scripts_parse_with_flag_on(monkeypatch):
    if not NODE:
        pytest.skip("node not available")
    _on(monkeypatch, "https://frames.example/x")
    rec = G.run_scenarios(A)
    for name in ("render_full", "graph", "swan_station_model"):
        blocks = re.findall(r"<script(?![^>]*(?:src=|application/ld\+json))[^>]*>(.*?)</script>", rec[name]["body"], flags=re.S)
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


def test_overlay_models_env_and_the_page(monkeypatch):
    """MODEL_OVERLAY_MODELS (section 43): a comma list in order, known names only, deduplicated; unset or useless -> the
    GFS frames alone. The page carries the list; the flag-off page never does."""
    _on(monkeypatch)
    for env, want in (("", ["gfswave"]), ("gfswave", ["gfswave"]), ("gfswave,ecmwf", ["gfswave", "ecmwf"]), (" ECMWF , gfswave,ecmwf ,icon", ["ecmwf", "gfswave"]),
                      ("icon,", ["gfswave"]), ("ecmwf", ["ecmwf"])):
        monkeypatch.setenv("MODEL_OVERLAY_MODELS", env)
        assert A._overlay_models() == want, env
    assert A.OVERLAY_MODELS == ("gfswave", "ecmwf")
    monkeypatch.setenv("MODEL_OVERLAY_MODELS", "gfswave,ecmwf")
    rec = G.run_scenarios(A)
    for name, old in GOLDEN.items():
        body = rec[name]["body"]
        assert 'var MODELS = ["gfswave", "ecmwf"];' in body, name
        assert GATED.sub("", body, count=1) == old["body"], name
    monkeypatch.delenv("MODEL_OVERLAYS", raising=False)                     # flag off: the env changes nothing
    rec = G.run_scenarios(A)
    for name, old in GOLDEN.items():
        assert rec[name]["body"] == old["body"], name


def test_the_ecmwf_notice_the_credit_links():
    """G30-4 (owner: a notice, linked from the credit): ECMWF's terms and CC BY 4.0 section 3(a) - the copyright with this
    year, the source, the licence and its link, ECMWF's disclaimer, no endorsement, and what Allshore Surf changed."""
    from datetime import datetime, timezone
    r = A.app.test_client().get("/notice/ecmwf")
    assert r.status_code == 200 and r.mimetype == "text/plain" and r.headers["Cache-Control"] == "public, max-age=86400"
    body = r.get_data(as_text=True)
    for need in ("© %d European Centre for Medium-Range Weather Forecasts (ECMWF)." % datetime.now(timezone.utc).year,
                 "Source: www.ecmwf.int", "Creative Commons Attribution 4.0 International (CC BY 4.0)",
                 "https://creativecommons.org/licenses/by/4.0/", "based on data and products of", "modified by Allshore Surf",
                 "ECMWF does not accept any liability whatsoever", "ECMWF does not endorse this product"):
        assert need in body, need
    js = open(os.path.join(STATIC, "overlay.js"), encoding="utf-8").read()
    assert 'href="/notice/ecmwf"' in js, "the credit's ECMWF link opens this notice"


def test_glwu_overlay_env_and_the_page(monkeypatch):
    """GLWU_OVERLAY (section 50): "1" hands the Great Lakes setting to the module (the page's GLWU); the flag-on page
    differs from the default only in that line; the flag-off page never carries it."""
    _on(monkeypatch)
    monkeypatch.setenv("GLWU_OVERLAY", "1")
    on = G.run_scenarios(A)
    monkeypatch.delenv("GLWU_OVERLAY")
    off = G.run_scenarios(A)
    for name in GOLDEN:
        assert "var GLWU = true;" in on[name]["body"], name
        assert on[name]["body"].replace("var GLWU = true;", "var GLWU = false;") == off[name]["body"], name
    monkeypatch.delenv("MODEL_OVERLAYS")
    monkeypatch.setenv("GLWU_OVERLAY", "1")
    for name, rec in G.run_scenarios(A).items():
        assert "var GLWU" not in rec["body"], name


def test_sst_overlay_env_and_the_page(monkeypatch):
    """SST_OVERLAY (section 44): "1" offers "Sea surface temperature" (the page's SST, handed to the module); anything
    else does not. The flag-off page never carries it; the flag-on page differs from the default only in that line."""
    _on(monkeypatch)
    for env, want in (("1", True), ("0", False), ("", False), ("true", False), ("yes", False)):
        monkeypatch.setenv("SST_OVERLAY", env)
        assert A._sst_overlay_enabled() is want, env
    monkeypatch.setenv("SST_OVERLAY", "1")
    on = G.run_scenarios(A)
    monkeypatch.delenv("SST_OVERLAY")
    off = G.run_scenarios(A)
    for name in GOLDEN:
        assert "var SST = true;" in on[name]["body"], name
        assert on[name]["body"].replace("var SST = true;", "var SST = false;") == off[name]["body"], name
    monkeypatch.delenv("MODEL_OVERLAYS")
    monkeypatch.setenv("SST_OVERLAY", "1")
    for name, rec in G.run_scenarios(A).items():
        assert "var SST" not in rec["body"] and "Sea surface temperature" not in rec["body"], name
