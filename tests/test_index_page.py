"""The page after the forecast window restructure (plan section 25): the top bar, the window markup,
the state handed to static_ui/forecast.js, the no-JS path. The exact bytes are pinned by the golden in
tests/test_overlay_flag.py; these tests say WHY the page looks the way it does."""
import json
import os
import re

import pytest

import app as A
import capture_index_golden as G


@pytest.fixture
def client(monkeypatch):
    monkeypatch.delenv("MODEL_OVERLAYS", raising=False)
    monkeypatch.delenv("POINTS_ROOT", raising=False)                      # no points bucket unless a test sets one
    monkeypatch.delenv("MODEL_FRAMES_BASE", raising=False)
    monkeypatch.setattr(A, "get_station_list", lambda: list(G.STATIONS))
    monkeypatch.setattr(A, "compute_forecast_payload", lambda *a, **k: dict(G.PAYLOAD, **({"model": "SWAN"} if k.get("model") == "SWAN" or (len(a) > 3 and a[3] == "SWAN") else {})))
    return A.app.test_client()


def initial(body):
    return json.loads(re.search(r"window\.__initial = (\{.*?\});", body).group(1))


def test_top_bar_and_window_markup(client):
    body = client.get("/?station=51201").get_data(as_text=True)
    for needle in ('id="pageHead"', 'id="brand" class="brand"', 'id="settingsHost"', 'id="stationTrigger"', 'id="settingsBtn"', 'id="settingsPanel"',
                   'id="tz" name="tz"', 'id="unit" name="unit"', 'id="forecastWin" class="fwin forecast-win fw-min"',
                   'id="fwTitle" class="station-field"', 'id="station" name="station" form="controlForm"',
                   'id="liveBuoyPanel" class="fwin live-win" hidden', 'id="lwHeader"', 'id="lwMin"', 'id="lwClose"', 'id="lwBody"', 'id="lwResize"',
                   "map.addControl(new BrandControl())", "map.addControl(new SettingsControl())", "map.addControl(new AttrToggle())",
                   "map.attributionControl.setPosition('bottomleft')", "zoomControl: false,", "AllshoreForecast.createLiveWindow(",
                   "liveWin.close()", "liveWin.open()", "document.addEventListener('allshore:livewin'",
                   'id="fwHeader"', 'id="fwMin"', 'id="fwMax"', 'id="viewBar"', 'id="modelBar"', 'id="rangeBar"',
                   'id="fwBody"', 'id="fwError"', 'id="forecastMeta"', 'id="forecastTable"', 'id="forecastLoading"',
                   'id="graphs"', 'id="heightChart"', 'id="periodChart"', 'id="directionChart"', 'id="fwResize"',
                   '/ui/forecast.js?v=%s' % A.UI_ASSET_VERSION, "window.AllshoreForecast.init(", "document.documentElement.classList.add('js')"):
        assert needle in body, needle
    for gone in ('id="updateBtn"', 'id="model" name="model"', 'id="view" name="view"', "sticky-top-bar", "nav-hidden", 'id="topBar"', 'id="lwMax"',
                 "container-fluid", ".top-bar", "--topbar-h", 'class="card-header', "onclick=\"closeLiveBuoyPanel()\"", "panel.style.display = 'block'",
                 '<script src="https://cdn.jsdelivr.net/npm/chart.js', "allshore.focusStation", "controlForm').submit()"):
        assert gone not in body, gone
    # the settings live in the gear's panel, hidden until opened; the no-JS Go button only inside <noscript>
    assert re.search(r'id="settingsPanel"[^>]*\bhidden\b', body)
    assert re.search(r"<noscript>\s*<button type=\"submit\" class=\"btn btn-sm btn-primary\" form=\"controlForm\">Go</button>", body)
    assert body.index('id="brand"') < body.index('id="map"') < body.index('id="forecastWin"') < body.index('id="liveBuoyPanel"')


def test_model_bar_only_for_swan_stations_and_the_state_echoes_the_request(client):
    b = client.get("/?station=51201&tz=Europe/Lisbon&unit=Metric&model=SWAN&view=Graph").get_data(as_text=True)
    st = initial(b)
    assert st["station"] == "51201" and st["tz"] == "Europe/Lisbon" and st["unit"] == "Metric"
    assert st["model"] == "SWAN" and st["view"] == "Graph" and st["swan_available"] is True and st["inline"] is False
    assert "51201" in st["swan_stations"] and "46001" not in st["swan_stations"]
    assert 'id="modelBar"' in b and not re.search(r'id="modelBar" hidden', b)
    assert re.search(r'data-model="SWAN" aria-pressed="true"', b) and re.search(r'data-view="Graph" aria-pressed="true"', b)
    assert re.search(r'id="forecastTable" hidden', b) and not re.search(r'id="graphs" hidden', b)
    b2 = client.get("/?station=13130&model=SWAN").get_data(as_text=True)
    st2 = initial(b2)
    assert st2["model"] == "GFS" and st2["swan_available"] is False, "SWAN off the SWAN stations resolves to GFS"
    assert re.search(r'id="modelBar" hidden', b2)
    assert re.search(r'id="graphs" hidden', b2) and not re.search(r'id="forecastTable" hidden', b2)


def test_every_js_page_is_a_shell_and_render_full_inlines_the_compact_table(client):
    shell = client.get("/?station=51201").get_data(as_text=True)
    assert 'id="forecastLoading"' in shell and "<table id='golden'>" not in shell
    assert initial(shell)["inline"] is False and "graph_data" not in initial(shell)
    assert re.search(r'<noscript>\s*<meta http-equiv="refresh" content="0;url=\?station=51201&amp;tz=&amp;unit=US&amp;model=GFS&amp;view=Table&amp;render=full">', shell)
    graph = client.get("/?station=51201&view=Graph").get_data(as_text=True)
    assert 'id="forecastLoading"' in graph and "view=Table&amp;render=full" in graph, "the Graph view is a shell too (its no-JS refresh goes to the Table view)"
    full = client.get("/?station=51201&render=full").get_data(as_text=True)
    assert "<table id='golden'>" in full and 'id="forecastLoading"' not in full and "<noscript>\n            <meta" not in full
    st = initial(full)
    assert st["inline"] is True and st["graph_header"]["cycle"] == "20260922 12 UTC" and st["graph_data"]["units"] == "ft"
    assert "<strong>Cycle : </strong>20260922 12 UTC" in full and "Time Zone: </strong>HST" in full


def test_render_full_asks_for_the_compact_table(monkeypatch):
    monkeypatch.delenv("MODEL_OVERLAYS", raising=False)
    monkeypatch.setattr(A, "get_station_list", lambda: list(G.STATIONS))
    calls = []

    def fake(station, tz, unit, model="GFS", *, compact=False):
        calls.append((station, tz, unit, model, compact))
        return dict(G.PAYLOAD)
    monkeypatch.setattr(A, "compute_forecast_payload", fake)
    c = A.app.test_client()
    c.get("/?station=51201")
    assert calls == [], "the shell computes no forecast (the window fetches it)"
    c.get("/?station=51201&render=full&unit=Metric")
    assert calls == [("51201", None, "Metric", "GFS", True)]


def test_post_and_the_server_error_are_still_handled(client, monkeypatch):
    r = client.post("/", data={"station": "46001", "unit": "US", "tz": "", "model": "GFS", "view": "Table"})
    assert r.status_code == 200 and initial(r.get_data(as_text=True))["station"] == "46001"
    ok = client.get("/?station=51201&render=full").get_data(as_text=True)
    assert re.search(r'id="fwError"[^>]*hidden', ok), "no error: the box is hidden"
    monkeypatch.setattr(A, "compute_forecast_payload", lambda *a, **k: dict(G.PAYLOAD, table_html=None, error="No SWAN forecast available for 51201"))
    err = client.get("/?station=51201&render=full").get_data(as_text=True)
    m = re.search(r'<div id="fwError"([^>]*)>([^<]*)</div>', err)
    assert m and "hidden" not in m.group(1) and m.group(2) == "No SWAN forecast available for 51201"
    assert initial(err)["error"] == "No SWAN forecast available for 51201"


def test_the_gated_overlay_block_reads_the_run_from_the_meta_line(monkeypatch, client):
    monkeypatch.setenv("MODEL_OVERLAYS", "1")
    monkeypatch.setenv("MODEL_FRAMES_BASE", "https://frames.example/gfswave/0p25/v1")
    monkeypatch.setattr(A, "get_station_tz", lambda sid: "Pacific/Honolulu")
    body = client.get("/?station=51201").get_data(as_text=True)
    assert "document.getElementById('forecastMeta'), document.getElementById('forecastTable'), document.getElementById('graphs')" in body

def test_the_pages_own_script_keeps_its_bridges_and_rules(client):
    """The inline map script has no harness; these pins name the behaviours that make the no-reload page work
    (G16-A): the map re-applies its stored view only on a width change, saves it on every move, the marker click
    and the favourites pick go through 'allshore:station', the form never submits with JS."""
    body = client.get("/?station=51201").get_data(as_text=True)
    assert "if (lastEnforcedWidth === w) {" in body and "lastEnforcedWidth = w;" in body
    assert re.search(r"function handleMapMoveEnd\(\) \{.*?saveMapView\(map\)", body, re.S)   # (the body has its own try blocks now)
    assert "new CustomEvent('allshore:station', { detail: { sid: String(s.id), source: 'map' } })" in body
    assert "new CustomEvent('allshore:station', { detail: { sid: sid, source: 'picker' } })" in body
    assert "if (d.source === 'picker' && d.sid) focusStation(String(d.sid));" in body
    assert "addEventListener('submit', (e) => { e.preventDefault(); })" in body
    assert "select.value = String(d.sid); refreshCurrent();" in body


def test_phone_sheets_sit_above_the_map_controls_and_the_no_js_page_shows_its_controls(client):
    """CSS pins (G16 + plan section 26): in phone mode both full-screen windows outrank the map's top-right corner
    (z 2600, so the gear panel is never under a window on desktops); the live bar stacks above the forecast bar; the
    no-JS layout shows the head row, the settings and the table whatever the [hidden] attributes say; the refresh is
    to the Table view."""
    body = client.get("/?station=51201&view=Graph").get_data(as_text=True)
    corner = re.search(r"\.leaflet-container\.settings-open \.leaflet-top\.leaflet-right \{ z-index: (\d+); \}", body).group(1)
    assert ".leaflet-container .leaflet-top.leaflet-right {" not in body, "the corner outranks the windows ONLY while the gear panel is open (owner: window buttons never under the legend)"
    assert "top: calc(var(--map-topright-h, 120px) + 8px) !important" in body[body.index(".fwin.fw-max {"):], "the maximised window starts below the legend corner"
    assert "function measureTopRight()" in body and "classList.toggle('settings-open', panels.some(function (p) { return !p.hidden; }) || !!(tmenu && !tmenu.hidden))" in body
    # plan section 46: the gear's panel (now with the overlay rows) never runs past the map's bottom edge: it scrolls inside
    assert "p.style.maxHeight = Math.max(120, Math.floor(room)) + 'px';" in body and "window.addEventListener('resize', fit);" in body
    assert re.search(r"\.settings-panel \{[^}]*width: 260px;[^}]*overflow-y: auto;", body), "fixed width, scrolls inside"
    phone = body[body.index("@media (max-width: 500px), (max-height: 500px) {"):]
    sheet = re.search(r"\.fwin:not\(\.fw-min\) \{ z-index: (\d+); \}", phone).group(1)
    assert int(sheet) > int(corner) > 2100 and int(corner) == 2600
    assert ".live-win.fw-min { bottom: var(--fw-bar-h, 48px) !important; }" in phone
    assert "html:not(.js) .settings-panel[hidden] { display: flex !important; }" in body
    assert "html:not(.js) #pageHead { display: flex;" in body and ".js #pageHead { display: none; }" in body
    assert "html:not(.js) #forecastTable[hidden] { display: block !important; }" in body
    assert "html:not(.js) .forecast-win { position: static !important; width: auto !important; max-width: none;" in body
    assert "html:not(.js) .live-win, html:not(.js) .tide-win { display: none !important; }" in body
    assert "view=Table&amp;render=full" in body and "view=Graph&amp;render=full" not in body
    assert ".fwin[hidden] { display: none !important; }" in body
    assert "#forecastTable[hidden] { display: block !important; } #graphs, #forecastSummary { display: none !important; }" in body[body.index("@media print"):]
    assert "#map, #pageHead, .fw-btn, .fw-resize, .fw-edge, .live-win, .tide-win, .sr-star, .station-caret, .tools-host, .tools-bar { display: none !important; }" in body[body.index("@media print"):]
    assert ".leaflet-container:not(.attr-open) .leaflet-control-attribution { display: none; }" in body

def test_the_map_extent_block_pans_by_whole_pixels_and_the_load_sequence_is_wired_before_the_first_view(client):
    """The latitude clamp pans by whole pixels and ignores its own moveend: the old degree-based clamp called setView
    with a sub-pixel correction, which Leaflet truncated to nothing, fired moveend again and recursed until the stack
    overflowed. With a saved view near a pole that happened inside the map's first view, so every layer waiting for
    'load', the station picker and the live buoys never came. tests/ui/mapclamp.test.js runs the block itself."""
    body = client.get("/?station=51201").get_data(as_text=True)
    block = body[body.index("// ---- map extent"):body.index("// ---- end map extent ----")]
    assert "map.panBy([0, dy], { animate: false })" in block and "setView" not in block
    assert "if (clampingLatitude) return;" in block and "catch (e) { console.error('latitude clamp', e); }" in block
    assert "map.project([LAT_LIMIT_SOUTH, 0], 0).y - map.project([LAT_LIMIT_NORTH, 0], 0).y" in block   # the limits are the farthest extent
    assert "const LAT_LIMIT_NORTH = 84;" in body and "const LAT_LIMIT_SOUTH = -79;" in body   # just past Greenland's tip / Antarctica's ice fronts (owner)
    assert body.index("map.on('moveend', handleMapMoveEnd)") < body.index("enforceSingleWorld();")

def test_pr_c_polish_markup(client):
    """PR C: the <head> starts the forecast request (tests/ui/early.test.js proves it asks for the module's own first
    query); the resize handle is keyboard-operable; the panel touched last is on top (the rule sits before the phone
    rules, which keep their own full-screen order); the live panel announces that it opened."""
    body = client.get("/?station=51201").get_data(as_text=True)
    head = body[:body.index("</head>")]
    assert "// ---- early forecast" in head and "fetch('/api/forecast?' + q)" in head and "window.__early.forecast = { q: q, p: f };" in head
    assert "early: window.__early" in body
    assert re.search(r'id="fwResize" class="fw-resize" tabindex="0" role="img" aria-label="[^"]+" aria-keyshortcuts="', body)
    assert 'swan = ["' in head, "the head knows the SWAN stations (a SWAN link elsewhere is fetched as GFS, like the loader)"
    assert body.index(".forecast-win.fw-front { z-index: 2100; }") < body.index(".fwin:not(.fw-min) { z-index: 3500; }")
    assert "new CustomEvent('allshore:livepanel')" in body and "document.addEventListener('allshore:livepanel', function () { front(1); });" in body

def test_plan_26_d1_page_rules(client):
    """The maximised window is capped by the table width (inline max-width): it must not be pinned to both edges;
    the Leaflet prefix goes, the credits stay."""
    body = client.get("/?station=51201").get_data(as_text=True)
    mx = body[body.index(".fwin.fw-max {"):]
    mx = mx[:mx.index("}")]
    assert "right: auto !important" in mx and "width: calc(100vw - 16px) !important" in mx
    assert "map.attributionControl.setPrefix(false)" in body and "Esri &amp; contributors" in body.replace("Esri & contributors", "Esri &amp; contributors")

def test_gridlines_are_loaded_and_offered_in_the_settings(client):
    """Plan section 26 item 6: static_ui/graticule.js (served by /ui/, versioned with the module) loads after Leaflet and
    before the map script; the gear panel offers the setting, checked by default; without JS the checkbox is hidden."""
    body = client.get("/?station=51201").get_data(as_text=True)
    leaflet = body.index("leaflet@1.9.4/dist/leaflet.js")
    tag = body.index('<script src="/ui/graticule.js?v=%s"></script>' % A.UI_ASSET_VERSION)
    assert leaflet < tag < body.index("const map = L.map(")
    panel = body[body.index('id="settingsPanel"'):]
    panel = panel[:panel.index("</div>")]
    assert '<input class="form-check-input" type="checkbox" id="gridlines" checked>' in panel
    assert "html:not(.js) .grid-setting { display: none; }" in body
    assert "window.AllshoreGraticule.create(map, { storage: st || undefined })" in body
    r = A.app.test_client().get("/ui/graticule.js?v=" + A.UI_ASSET_VERSION)
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=31536000, immutable"

def test_the_map_is_the_page_and_the_windows_stack_by_touch(client):
    """Plan section 26 (D2): the map's height comes from the viewport minus the phone bars (no top bar, no wrapper
    padding); the width rule of enforceSingleWorld is untouched; the picker list is position: fixed and placed from
    its trigger; the live window's phone bar drives a re-measure."""
    body = client.get("/?station=51201").get_data(as_text=True)
    assert "const liveBar = lw && !lw.hidden && lw.classList.contains('fw-min') ? lw.offsetHeight : 0;" in body
    assert "const reserve = phone ? phoneBarH + liveBar + tideBar + windBar : 0;" in body and "if (lastEnforcedWidth === w) {" in body
    assert ".live-win.fw-min { bottom: var(--fw-bar-h, 48px) !important; }" in body and ".fwin.live-win {" in body
    assert "html.js body { margin: 0; overflow: hidden; }" in body and "#map { height: 100vh; height: 100dvh; background: #0b2536; }" in body
    assert "position: fixed; z-index: 2600; top: auto; left: auto;" in body[body.index(".station-results {"):]
    assert "function place() {" in body and "window.addEventListener('resize', close);" in body
    assert "liveOpen: function () { return !!(liveWin && liveWin.isOpen() && liveWin.window.mode !== 'min'); }" in body
    assert "map.zoomControl." not in body, "no zoom control exists any more (zoomControl: false): nothing may touch it"
    assert ".fw-titles .station-field .station-picker { flex: 1 1 auto; min-width: 0; }" in body, "the picker box shrinks: it never covers the run text (owner)"
    assert "max-width: min(800px, calc(60vw - 40px));" in body and "max-width: min(420px, calc(40vw - 40px));" in body, "the chips never meet"
    assert body.index("let liveWin = null;") < body.index("const map = L.map("), "declared before anything that can throw (a later script assigns it)"

def test_g18b_pins(client):
    """G18b: the lines whose loss no other test would notice (reviewer A's surviving mutants), and the two P2s: the
    chips shrink below the window minimum so they never meet, and the forecast window's default box rises above a
    parked live chip (its resize corner stays reachable)."""
    body = client.get("/?station=51201").get_data(as_text=True)
    for needle in (
        "liveWin = window.AllshoreForecast.createLiveWindow({ onClose: function () { liveDetailSeq++; liveWindowClosed(); },",
        "if (fwEl && fwEl.classList.contains('fw-min') && fwEl.offsetHeight > 0) phoneBarH = fwEl.offsetHeight;",
        "document.documentElement.style.setProperty('--fw-bar-h', phoneBarH + 'px');",
        "function open() { seen = new Map(); nextRank = 0; renderFavs(); place(); results.hidden = false;",
        "if (below >= 220 || below >= above) { s.top = (r.bottom + 2) + 'px'; s.bottom = 'auto';",
        "if (window.innerWidth <= 500 || window.innerHeight <= 500) enforceSingleWorld();",
        ".fwin.live-win { right: 18px; bottom: 18px; width: min(820px, calc(100vw - 136px)); height: min(76vh, 800px); z-index: 2000; }",
        "panels.forEach(function (p) { new MutationObserver(sync).observe(p, { attributes: true, attributeFilter: ['hidden'] }); });",
        ".fw-maxbtn { display: none; }",
        "options: { position: 'topleft' },",
        ".fwin.fw-min { height: auto !important; min-height: 0; min-width: 0; }",
        ".live-chip .forecast-win:not(.fw-min):not(.fw-max) { bottom: 70px; }",
        "document.body.classList.toggle('live-chip', !!(d.open && d.mode === 'min'));",
        "onMode: function () { if (window.innerWidth <= 500 || window.innerHeight <= 500) enforceSingleWorld(); },",
    ):
        assert needle in body, needle
    gear = body[body.index("const SettingsControl = L.Control.extend({"):]
    gear = gear[:gear.index("map.addControl(new SettingsControl())")]
    assert "L.DomEvent.disableClickPropagation(c);" in gear and "L.DomEvent.disableScrollPropagation(c);" in gear
    assert body.count("options: { position: 'topleft' },") == 1 and body.index("map.addControl(new BrandControl())") < body.index("L.control.layers(")

def test_both_windows_stretch_from_every_side(client):
    """Plan section 27: each window has four edges and three corners (data-edge) besides its focusable bottom-right
    grip; they are hidden wherever the grip is (minimised, maximised, phones, no JS, print); the body keeps a 5 px margin
    so its scrollbars are never under an edge."""
    body = client.get("/?station=51201").get_data(as_text=True)
    for k in ("n", "s", "e", "w", "ne", "nw", "sw"):
        assert body.count('<div class="fw-edge fw-edge-%s" data-edge="%s" aria-hidden="true"></div>' % (k, k)) == 4, k   # four windows (sections 38, 39)
    for rule in (".fwin.fw-min .fw-resize, .fwin.fw-min .fw-edge { display: none; }", ".fwin.fw-max .fw-resize, .fwin.fw-max .fw-edge { display: none; }",
                 "html:not(.js) .fw-edge,", ".fw-resize, .fw-edge { display: none; }",
                 "@media (min-width: 501px) and (min-height: 501px) { .fwin:not(.fw-min) .fw-body { margin: 0 5px 5px; } }"):
        assert rule in body, rule
    assert body.index('data-edge="sw"') < body.index('id="fwResize"')

def test_the_new_wordmark_is_the_brand_with_no_box_around_it(client):
    """Owner (2026-09-27): the Allshore Surf wordmark replaces the mark and the title, overlaid on the map at the same
    place (top-left, above the overlay selector) with no border or white space; served immutable from /ui/."""
    body = client.get("/?station=51201").get_data(as_text=True)
    assert '<img class="brand-logo" src="/ui/logo.png?v=%s" alt="Allshore Surf" width="194" height="60">' % A.UI_ASSET_VERSION in body
    assert 'class="app-logo"' not in body and 'class="app-title"' not in body
    assert ".leaflet-control-brand .brand { background: none; border: 0; padding: 0; box-shadow: none; line-height: 0; }" in body
    assert "rgba(255,255,255,.92)" not in body
    r = A.app.test_client().get("/ui/logo.png?v=" + A.UI_ASSET_VERSION)
    assert r.status_code == 200 and r.headers["Content-Type"] == "image/png" and r.headers["Cache-Control"] == "public, max-age=31536000, immutable"
    from PIL import Image
    import io as _io
    im = Image.open(_io.BytesIO(r.get_data()))
    assert im.size == (387, 120), "2x the 60 px display height"
    assert im.convert("RGBA").getpixel((0, 0))[3] == 0, "transparent around the lettering"


def test_the_site_icon_is_the_owners_monogram(client):
    """Owner (2026-09-27): the browser tab and Google's result show the owner's "AS" monogram, not the old wave mark.
    Stable icon URLs (Google), linked with ?v= so browsers drop the cached old icon; a week in the cache."""
    body = client.get("/?station=51201").get_data(as_text=True)
    v = A.ICON_VERSION
    assert '<link rel="icon" href="/favicon.ico?v=%s" sizes="16x16 32x32 48x48">' % v in body
    assert '<link rel="icon" type="image/png" href="/icon-192.png?v=%s" sizes="192x192">' % v in body
    assert '<link rel="apple-touch-icon" href="/apple-touch-icon.png?v=%s">' % v in body
    from PIL import Image
    import io as _io
    for path, ctype, sizes in (("/favicon.ico", "image/x-icon", {(16, 16), (32, 32), (48, 48)}),
                               ("/icon-192.png", "image/png", {(192, 192)}),
                               ("/apple-touch-icon.png", "image/png", {(180, 180)})):
        r = A.app.test_client().get(path + "?v=" + v)
        assert r.status_code == 200 and r.headers["Content-Type"] == ctype, path
        assert r.headers["Cache-Control"] == "public, max-age=604800" and r.headers["X-Content-Type-Options"] == "nosniff"
        im = Image.open(_io.BytesIO(r.get_data()))
        assert (im.info.get("sizes") or {im.size}) == sizes, path
        assert b"<svg" not in r.get_data(), "not the old inline wave mark"


def test_search_and_sharing_metadata(client):
    """Owner (2026-09-27): the Google result reads "Allshore Surf | Surf Forecast & Live Buoys" with a written
    description (never "free": the site may be monetised), the site name Allshore Surf, a sharing card, the station
    picker kept out of snippets; robots.txt + sitemap.xml on allshoresurf.com, the Render addresses never indexed."""
    import json as _json
    body = client.get("/?station=51201").get_data(as_text=True)
    assert "<title>Allshore Surf | Surf Forecast &amp; Live Buoys</title>" in body
    assert '<meta name="description" content="%s">' % A.SITE_DESCRIPTION in body
    assert "free" not in A.SITE_DESCRIPTION.lower()
    assert '<link rel="canonical" href="https://allshoresurf.com/">' in body
    assert '<meta property="og:image" content="https://allshoresurf.com/og-image.jpg?v=%s">' % A.ICON_VERSION in body
    assert '<meta name="twitter:card" content="summary_large_image">' in body
    ld = re.search(r'<script type="application/ld\+json">(.*?)</script>', body, re.S).group(1)
    graph = _json.loads(ld)["@graph"]
    assert {g["@type"]: g["name"] for g in graph} == {"WebSite": "Allshore Surf", "Organization": "Allshore Surf"}
    assert '<div id="fwTitle" class="station-field" data-nosnippet>' in body
    r = A.app.test_client().get("/og-image.jpg")
    from PIL import Image
    import io as _io
    assert r.status_code == 200 and Image.open(_io.BytesIO(r.get_data())).size == (1200, 630)
    pub = A.app.test_client().get("/robots.txt", base_url="https://allshoresurf.com")
    assert pub.get_data(as_text=True) == "User-agent: *\nAllow: /\n\nSitemap: https://allshoresurf.com/sitemap.xml\n"
    for host in ("https://wave-app-clean.onrender.com", "https://wave-app.onrender.com"):
        assert A.app.test_client().get("/robots.txt", base_url=host).get_data(as_text=True) == "User-agent: *\nDisallow: /\n"
    sm = A.app.test_client().get("/sitemap.xml")
    assert sm.headers["Content-Type"].startswith("application/xml") and "<loc>https://allshoresurf.com/</loc>" in sm.get_data(as_text=True)


def test_legend_button_in_the_top_right_row(client):
    """Plan section 54: the layer legend is a key button, first in one row with the tools and the gear (owner: Legend,
    Tools, Settings); Leaflet's layer control moves into the key's panel (closed on load, the gear's toggle); a dot and
    the notes in the key's label while a legend note exists; quick hover / focus labels, never on touch or over an open
    panel; the corner's measure counts its own controls only."""
    body = client.get("/?station=51201").get_data(as_text=True)
    assert body.index('id="legendHost"') < body.index('id="toolsHost"') < body.index('id="settingsHost"')
    assert ('<button type="button" id="legendBtn" class="icon-btn" aria-label="Legend" aria-expanded="false" '
            'aria-controls="legendPanel" data-tip="Legend">') in body
    assert '<div id="legendPanel" class="settings-panel legend-panel" hidden role="group" aria-label="Legend"></div>' in body
    assert '<span class="legend-dot" hidden></span>' in body
    assert 'aria-controls="toolsMenu" data-tip="Tools">' in body and 'aria-controls="settingsPanel" data-tip="Settings">' in body
    # the row: legend first, then the tools, then the gear; the layer control into the legend's panel
    on_add = body[body.index("const SettingsControl = L.Control.extend({"):]
    assert on_add.index("if (legend) c.appendChild(legend);") < on_add.index("if (tools) c.appendChild(tools);") < on_add.index("if (host) c.appendChild(host);")
    assert "lc = layersControl.getContainer && layersControl.getContainer();" in body and "if (lp && lc) lp.appendChild(lc);" in body
    assert "collapsed: false" in body                                        # the list itself is always expanded (in its panel)
    # the row below the longitude labels at the map's top edge (owner): labels at 4 px, about 13 px tall; the row from 26 px
    assert ".leaflet-top.leaflet-right .leaflet-control-settings { margin-top: 26px; }" in body
    # no line gap under the buttons (their hosts as tall as they are): every panel opens 4 px below its button
    assert "vertical-align: top;   /* no line gap under the button" in body[body.index("    .icon-btn {"):]
    # the corner: its own controls only (the layer control inside the open panel never counts)
    assert "box.querySelectorAll('.leaflet-top.leaflet-right > .leaflet-control')" in body
    assert "'.leaflet-top.leaflet-right .leaflet-control'" not in body
    # the dot and the label from the three notes, painted with every note
    fn = body[body.index("    function syncLegendNote() {"):]
    fn = fn[:fn.index("\n    }\n") + 6]
    for needle in ("const live = map.hasLayer(liveBuoyLayer) ? liveNoteText : '';",   # only while its layer is on (G37 F3)
                   "[['Live buoys', live], ['Tide stations', tideNoteText], ['Wind stations', windNoteText]]",
                   "btn.setAttribute('data-tip', tip); btn.setAttribute('aria-label', tip);", "if (dot) dot.hidden = !notes.length;"):
        assert needle in fn, needle
    assert body.count("      syncLegendNote();\n      const el = document.querySelector('.leaflet-control-layers [data-") == 3
    assert "map.on('overlayadd overlayremove', function (e) { if (e.layer === liveBuoyLayer) syncLegendNote(); });" in body
    # both panels lift the corner over the windows and fit the map while open (G37 F6)
    assert "const panels = legend ? [panel, legend] : [panel];" in body
    # CSS: hidden without JS; the panel 180 px (G37 F5); the dot gone while open; quick labels
    for needle in ("html:not(.js) .legend-host { display: none; }",
                   ".settings-panel.legend-panel { width: 180px; padding: 6px 10px; }",
                   # a label shows over the windows: the corner lifted while a button is hovered or keyboard-focused (G37 F1)
                   "@media (hover: hover) { .leaflet-container .leaflet-top.leaflet-right:has(.icon-btn[data-tip]:hover) { z-index: 2600; } }",
                   ".leaflet-container .leaflet-top.leaflet-right:has(.icon-btn[data-tip]:focus-visible) { z-index: 2600; }",
                   '#legendBtn[aria-expanded="true"] .legend-dot { display: none; }',
                   '@media (forced-colors: active) { .legend-dot { forced-color-adjust: none; background: Highlight; box-shadow: 0 0 0 1.5px Canvas; } }',
                   "@media (hover: hover) { .icon-btn[data-tip]:hover::after { display: block; } }",
                   ".icon-btn[data-tip]:focus-visible::after { display: block; }",
                   '.icon-btn[data-tip][aria-expanded="true"]::after { display: none !important; }',
                   "#settingsBtn[data-tip]::after { left: auto; right: 0; transform: none; }"):
        assert needle in body, needle
    assert ".icon-btn[data-tip]:hover::after { display: block; }" not in body.replace("@media (hover: hover) { .icon-btn[data-tip]:hover::after { display: block; } }", "")


def test_map_tools_beside_the_gear(client, monkeypatch):
    """Plan section 29: a tools button (JS only) beside the gear with three tools; the module script and its init;
    the swell-exposure coast data derived from the frames base (or COAST_BASE), empty without either; while a tool
    is active the forecast-point and live-buoy clicks go to the tool; hidden in print."""
    monkeypatch.delenv("COAST_BASE", raising=False)
    monkeypatch.delenv("MODEL_FRAMES_BASE", raising=False)
    body = client.get("/?station=51201").get_data(as_text=True)
    assert body.index('id="toolsHost"') < body.index('id="settingsHost"')
    for tool in ("point", "distance", "area", "exposure"):
        assert 'data-tool="%s"' % tool in body
    assert body.index('data-tool="point"') < body.index('data-tool="distance"')        # Forecast point first (plan section 31)
    assert '<script src="/ui/tools.js?v=%s"></script>' % A.UI_ASSET_VERSION in body
    assert "coastBase: \"\" || window.__allshoreCoastBase || ''" in body
    assert "html:not(.js) .tools-host { display: none; }" in body
    assert ".station-caret, .tools-host, .tools-bar { display: none !important; }" in body
    assert "if (window.AllshoreTools && window.AllshoreTools.active()) return;" in body
    assert "if (window.AllshoreTools && window.AllshoreTools.active()) { window.AllshoreTools.click(e.latlng, e.originalEvent); return; }" in body
    assert "if (tools) c.appendChild(tools);" in body
    # G20: the settings control stacks above the tool bar (its menus drop over it); a steady bar height; the windows
    # are obstacles for the fan and an expanded one over the bar is minimised when a tool starts
    assert ".leaflet-top.leaflet-right .leaflet-control-settings { z-index: 801; }" in body
    assert ".tools-bar-body .tools-sector { margin-top: 4px; min-height: 4.5em; white-space: pre-line; }" in body   # grows, never clips (G21 B-5)
    assert "overflow: hidden; white-space: pre-line" not in body
    assert ".tools-bar-actions button.is-on {" in body and '.tools-bar-actions button[aria-pressed="true"]' not in body   # Lock: no aria-pressed (G21 B-11)
    assert ".tools-x, .tools-fold { min-width: 36px; min-height: 36px; }" in body[body.index("@media (max-width: 576px)"):]
    assert "obstacles: function () { return [document.getElementById('forecastWin'), document.getElementById('liveBuoyPanel'), document.getElementById('tideWin'), document.getElementById('windWin')]; }," in body
    assert "[['liveBuoyPanel', 'lwMin'], ['tideWin', 'twMin'], ['windWin', 'wwMin'], ['forecastWin', 'fwMin']].forEach(function (w) {" in body
    assert "if (!(r.left < b.right && r.right > b.left && r.top < b.bottom && r.bottom > b.top)) return;" in body
    # G20 re-review: the bar is capped at the map (its body scrolls, the actions stay near the top); a window is
    # minimised whenever the bar GROWS over it; the overlay's details fold when the exposure tool starts on a short map
    assert "display: flex; flex-direction: column; }" in body and ".tools-bar-body { overflow-y: auto; min-height: 0; }" in body
    assert "onLayout: function (bar, grew) {" in body and "if (!grew) return;" in body
    assert "if (tool !== 'exposure' || map.getContainer().clientHeight >= 550) return;" in body
    assert "map.getContainer().querySelector('.ov-toggle[aria-expanded=\"true\"]');" in body
    # G20 re-check: a page minimise keeps keyboard focus where it was (the tool keeps Escape / Backspace)
    assert "if (had && had !== document.body && document.contains(had)) try { had.focus({ preventScroll: true }); }" in body
    assert "if (document.activeElement !== had && document.activeElement && document.activeElement !== document.body) document.activeElement.blur();" in body
    # the frames address alone never changes a flag-off page; with the overlay on, its gated block derives the coast
    monkeypatch.setenv("MODEL_FRAMES_BASE", "https://models.example.com/gfswave/0p25/v1/")
    assert A._coast_base() == "" and "__allshoreCoastBase =" not in client.get("/?station=51201").get_data(as_text=True)
    monkeypatch.setenv("MODEL_OVERLAYS", "1")
    on = client.get("/?station=51201").get_data(as_text=True)
    assert r"if (/\/gfswave\/0p25\/v1$/.test(BASE)) window.__allshoreCoastBase = BASE.replace(/\/gfswave\/0p25\/v1$/, '') + '/static/coast/v1';" in on
    assert on.index("window.__allshoreCoastBase =") < on.index("AllshoreTools.init(")
    monkeypatch.delenv("MODEL_OVERLAYS")
    monkeypatch.setenv("COAST_BASE", "https://coast.example.com/v9/")
    assert A._coast_base() == "https://coast.example.com/v9"
    assert "coastBase: \"https://coast.example.com/v9\" || window.__allshoreCoastBase" in client.get("/?station=51201").get_data(as_text=True)
    monkeypatch.delenv("COAST_BASE")
    r = A.app.test_client().get("/ui/tools.js?v=" + A.UI_ASSET_VERSION)
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=31536000, immutable"
    # the projected window (plan section 30) ends its rays at the map's own latitude limits
    js, page = r.get_data(as_text=True), client.get("/?station=51201").get_data(as_text=True)
    assert "REACH_LAT_N = 84, REACH_LAT_S = -79" in js and "const LAT_LIMIT_NORTH = 84;" in page and "const LAT_LIMIT_SOUTH = -79;" in page


def test_my_points_on_the_page(client):
    """Plan section 31: the visitor's forecast points. The map tool hands its click to the page block; the points are
    options of the station select (an optgroup the block adds), markers of their own in a legend entry of their own,
    and a group in the picker's list with Rename and Remove; the picker's star keeps a point; a picker pick of a point
    recentres the map on it. The tool asks the server first (G22): only a forecast opens the window and keeps the
    point; a refusal stays in the tool bar."""
    body = client.get("/?station=pt_21667N_158054W").get_data(as_text=True)
    # the tool only where a points bucket is set (G22 R-A18): the test app has none
    assert "onPoint: false && window.__allshorePoints ? function (ll) { return window.__allshorePoints.add(ll); } : null," in body
    block = body[body.index("window.__initial = "):body.index("window.AllshoreForecast.init({")]
    assert "var store = F.createPointStore(storage), refusedIds = {};" in block
    assert "setItem: function () { throw new Error('no storage'); }" in block                    # no storage: 'unsaved', never "kept"
    assert "group.id = 'myPointsGroup'; group.label = 'My points';" in block
    assert "return F.prefetch(id).then(function (d) {" in block and "if (d && d.table_html) return { ok: true, open: function () { open(id); } };" in block
    assert "window.__allshorePoints = { store: store, sync: sync, add: add, say: say, refused: function (id) { return !!refusedIds[id]; } };" in block
    assert "window.addEventListener('storage', function (e) { if (!e.key || e.key === 'allshore.points.v1') sync(); });" in block
    assert '<div id="fwNote" class="fw-note" role="status" hidden></div>' in body
    # the notes sit between the toolbar and the scrolling body: the table's scroll to the current hour never hides them
    # (test-site check V7-3); several kept, one per line
    assert body.index('<div id="fwToolbar"') < body.index('<div id="fwNote"') < body.index('<div id="fwBody"')
    assert ".forecast-win > #fwNote { flex: 0 0 auto; margin: 8px 10px 0; white-space: pre-line; }" in body
    assert ".fwin.fw-min > #fwNote { display: none; }" in body and ".fw-body > #fwNote" not in body
    printing = body[body.index("    @media print {\n      #map, #pageHead"):]
    printing = printing[:printing.index("\n    }\n")]
    assert ".forecast-win.fw-min > #fwNote:not([hidden]) { display: block; }" in printing   # a minimised window prints its notes too (V7c-1)
    assert "function tellOnPage(msg) {" in body and "tellOnPage(retiredDefaultNote(early));" in body
    # a link moves the focus into the window it opened, whatever pressed it (V7-2); the stall note again when the list lands (V7-4)
    assert "function linkFocus(headerId) { if (typeof focusWindowHeader === 'function') focusWindowHeader(headerId); }" in body
    assert "        windListNote = '';\n        workOutWindStall();\n        rebuildWindMarkers(true);" in body
    assert block.index("window.__allshorePoints =") < block.rindex("try { sync(); } catch (e) {}")      # damaged storage cannot stop the window (G22 R-A13)
    assert "if (cur !== asked && d && d.table_html) return { ok: false, cancel: true };" in block           # a late answer (G22 R-A6)
    assert "if (d.ok && refusedIds[d.station]) { delete refusedIds[d.station]; sync(); return; }" in block   # a refusal forgotten (R-A8)
    assert "'<span class=\"lc-dot lc-point\"></span>My points': pointLayer" in body
    assert "points: saved.points !== false" in body and "points: map.hasLayer(pointLayer)" in body
    assert "const pointIcon = L.divIcon({ className: 'my-pt', iconSize: [14, 14], iconAnchor: [7, 7] });" in body
    assert ".my-pt { background: #fff; border: 2.5px solid #d6007a;" in body
    assert "rebuildPointMarkers(true);" in body and "rebuildPointMarkers(false);" in body
    assert "const pt = window.AllshoreForecast && window.AllshoreForecast.parsePointId(sid);" in body   # focusStation
    assert "document.addEventListener('allshore:points', function () {" in body and "focusBack(m);                                                             // (G22 R-A16)" in body
    assert "ren.type = 'button'; ren.className = 'pt-act pt-ren';" in body and "rem.setAttribute('aria-label', 'Remove point: ' + label);" in body
    assert "const r = P.store.add(sid, goneNames.get(sid) || '');" in body and "P.say(P.store.remove(sid) ? 'removed' : 'unremoved', sid); P.sync();" in body   # the top star keeps / lets go (owner, 2026-10-19)
    assert "if (r === 'added' && typeof showPointLayer === 'function') showPointLayer();" in body            # the star shows the layer (R-A15)
    assert "function showPointLayer() { if (!map.hasLayer(pointLayer)) { pointLayer.addTo(map); saveLayerVisibility(); } }" in body   # (X34)
    assert "if (P.refused(sid)) { announce('There is no forecast here, so the point is not kept.'); return; }" in body
    # names and provider labels in tooltips are text (G22 K-8, A-20); point markers pass a tool's click on (B-8)
    assert "mk.bindTooltip(textTip(p.label)," in body and "marker.bindTooltip(textTip(label)," in body
    assert body.count("bindTooltip(") == 5 and "mk.bindTooltip(s.id," in body                     # the third: a station id; the fourth: a tide station; the fifth: a wind station
    assert "{ window.AllshoreTools.click(e.latlng, e.originalEvent); return; }   // markers do not pass clicks to the map (G22 B-8)" in body
    # the no-script page names the point in its select (B-5); the script takes that option over
    assert '<option value="pt_21667N_158054W" data-point selected>21.667N 158.054W</option>' in body
    assert client.get("/?station=51201").get_data(as_text=True).count('<option value="pt_') == 0
    assert ".sr-star, .pt-act, .sr-ic { min-width: 44px; min-height: 44px; }" in body                   # phone targets
    assert "createPane" not in body                                                              # (the overlay's pin: no pane from the page)
    # the page names the point in its title (the server renders the shell; the window fills in the coordinates)
    assert '<span id="stationCurrent" class="station-current">pt_21667N_158054W</span>' in body


def test_the_fix_round_2_page_rules(client, monkeypatch):
    """G22 re-check: an id that is no station keeps its own option and name without JavaScript (R-B12, R-A11); the
    no-script meta line never shows a raw "Etc/" zone (R-A17); the tool shows where a points bucket is set (R-A18);
    on phones the run text gives way first to a point's coordinates (R-B3)."""
    body = client.get("/?station=NOPE9").get_data(as_text=True)
    assert '<option value="NOPE9" data-unknown selected>NOPE9</option>' in body
    assert "filter(function (o) { return !o.hasAttribute('data-unknown'); })" in body
    assert "writeLabel(currentEl, c ? c.label : (sid || 'Select a station'));" in body
    assert client.get("/?station=51201").get_data(as_text=True).count("data-unknown selected") == 0
    assert client.get("/?station=pt_21667N_158054W").get_data(as_text=True).count("data-unknown selected") == 0
    assert "{{ graph_header.tz|zone_label }}" in open(os.path.join(A.app.root_path, "templates", "index.html"), encoding="utf-8").read()
    monkeypatch.setenv("POINTS_ROOT", "https://bucket.example")
    on = client.get("/?station=51201").get_data(as_text=True)
    assert "onPoint: true && window.__allshorePoints ?" in on
    assert ".fwin .fw-titles .fw-cycle { flex: 0 1000 auto; min-width: 0;" in body
    # the overlay's and the live panel's zone label: a nautical zone as the window writes it (R-B4, found on the test site)
    assert "if (/^Etc\\//.test(tz || '') && window.AllshoreForecast && window.AllshoreForecast.zoneLabel) return window.AllshoreForecast.zoneLabel(tz);" in body


def test_forecast_table_upgrade_markup_and_css(client):
    """Plan section 35: the Detailed | Summary buttons and the summary container, the table that fills its window
    (no width cap), the sky tints, the now row, the day dividers, the sticky Date / Time columns, the arrows."""
    body = client.get("/?station=51201").get_data(as_text=True)
    assert 'id="modeBar" hidden' in body
    assert 'data-mode="detailed" aria-pressed="true">Detailed</button>' in body
    assert 'data-mode="summary" aria-pressed="false">Summary</button>' in body
    assert '<div id="forecastSummary" class="forecast-summary" hidden></div>' in body
    for css in ("container-type: inline-size; container-name: fwbody;",
                "#forecastTable table.forecast-compact { font-size: 12.5px; line-height: 1.25; width: 100%; font-variant-numeric: tabular-nums; }",
                "@container fwbody (min-width: 1500px) {",
                "background-image: linear-gradient(var(--tint, transparent), var(--tint, transparent)); }",
                "#forecastTable tr.sky-night { --tint:", "#forecastTable tr.now-row { --tint:",
                "#forecastTable table.forecast-compact tr.day-first > td { border-top: 2px solid #8a949e; }",
                "position: sticky; z-index: 1; background-color: #fff;",
                "#forecastTable table.forecast-compact td.col-time, #forecastTable thead th.col-time { left: var(--date-w, 0px); }",
                "#forecastTable thead th.col-date, #forecastTable thead th.col-time { position: sticky; z-index: 3; }",
                ".dir-arrow { display: inline-block;", ".forecast-summary table { width: 100%;",
                "#graphs, #forecastSummary { display: none !important; }"):
        assert css in body, css
    assert "width: auto; }" not in body.split("#forecastTable table.forecast-compact {")[1].split("\n")[0]
    assert "fitWidth" not in body and "max-width, the table's width" not in body
    # the frozen header and the sticky columns at offset 0: no body padding above or beside the table (a sticky table
    # header at a negative offset let a few pixels of the scrolled rows show above it)
    assert ".forecast-win .fw-body { padding: 0 0 8px; }" in body
    assert "#forecastTable thead { position: sticky; top: 0; z-index: 2; }" in body and "top: -8px" not in body
    assert ".forecast-win #forecastMeta { padding-top: 8px; }" in body
    # Owner (step 6): no twilight tint (first light to last light reads as daylight), daylight rows bold with solid
    # black borders, night rows dashed grey, the Date column not bold on its own. The borders are separate so the
    # frozen cells carry their own (Chromium paints a frozen cell's collapsed borders where the cell would have been:
    # found at 1:1 on the test site as 1-px slivers of the scrolled rows beside the frozen Date / Time columns).
    for css in ("#forecastTable table.forecast-compact { border-collapse: separate; border-spacing: 0; }",
                "#forecastTable table.forecast-compact th, #forecastTable table.forecast-compact td { border: 0 solid #dee2e6; border-top-width: 1px; border-left-width: 1px; }",
                "#forecastTable table.forecast-compact .col-time, #forecastTable table.forecast-compact .col-last { border-right-width: 1px; }",
                "#forecastTable table.forecast-compact tbody > tr:last-child > td { border-bottom-width: 1px; }",
                "#forecastTable .sun-ev.ev-dawn, #forecastTable .sun-ev.ev-dusk { color: #6f5816; }",
                "#forecastTable .sun-ev.ev-sunrise, #forecastTable .sun-ev.ev-sunset { color: #934207; }",
                "#forecastTable table.forecast-compact .col-time + *, #forecastTable table.forecast-compact thead > tr:nth-child(2) > :first-child { border-left-width: 0; }",
                "#forecastTable table.forecast-compact thead > tr:last-child > *, #forecastTable table.forecast-compact thead > tr:first-child > [rowspan] { border-bottom-width: 1px; }",
                "#forecastTable table.forecast-compact tr.sky-day > td { font-weight: 700; border-color: #000; }",
                "#forecastTable table.forecast-compact tr.sky-night > td { border-color: #999; border-style: dashed; }",
                "#forecastTable table.forecast-compact :where(tr.sky-day) + tr.sky-night > td { border-top-style: solid; border-top-color: #000; }",
                "#forecastTable table.forecast-compact tbody > tr:first-child > td { border-top-width: 0; }"):
        assert css in body, css
    for gone in ("sky-twilight", "#forecastTable td.col-date { font-weight: 700; }", "box-shadow: inset 1px 0 0 #dee2e6",
                 "inset 0 1px 0 #dee2e6", "#forecastTable thead tr:first-child { border-top-width: 0; }",
                 "border-right: 1px solid #dee2e6; border-bottom: 1px solid #dee2e6",       # the table's own grey edges (G24 B-5)
                 "#b4530a", "#8a6d1f", ".forecast-summary .trend-peak { color: #1d6fd6; }"):  # G24 B-7 (B-6: the sunrise rule above has no weight)
        assert gone not in body, gone


def test_render_full_inlines_the_summary(monkeypatch, client):
    monkeypatch.setattr(A, "compute_forecast_payload", lambda *a, **k: dict(G.PAYLOAD, summary_html='<table class="forecast-summary"><tr><td>x</td></tr></table>'))
    body = client.get("/?station=51201&render=full").get_data(as_text=True)
    assert '<div id="forecastSummary" class="forecast-summary" hidden><table class="forecast-summary">' in body


def test_the_live_list_loads_through_its_module(client):
    """Plan section 36: static_ui/livelist.js (served by /ui/, versioned with the module) loads before the map
    script; the layer legend carries the list's state as text; the head still starts the request early and the
    module takes it once; the first markers are marked for measurement; without the module the plain request."""
    body = client.get("/?station=51201").get_data(as_text=True)
    tag = body.index('<script src="/ui/livelist.js?v=%s"></script>' % A.UI_ASSET_VERSION)
    assert body.index("leaflet@1.9.4/dist/leaflet.js") < tag < body.index("const map = L.map(")
    assert "live: fetch('/api/buoys/live-stations')" in body                     # the <head> request
    assert "'<span class=\"lc-dot lc-live\"></span>Live buoys<span class=\"lc-note\" data-live-note aria-hidden=\"true\"></span>': liveBuoyLayer" in body   # a visual hint (G25 B-4)
    assert ".lc-note:empty { display: none; }" in body
    assert ".lc-note { display: block; padding-left: 21px;" in body          # its own line: the legend never widens (phone)
    assert ("if (el && el.textContent !== liveNoteText) {" + chr(10) + "        el.textContent = liveNoteText;" + chr(10)
            + "        measureTopRight();") in body   # text, never HTML; the corner re-measured
    assert "layersControl._update = function ()" in body and "paintLiveNote(); return r;" in body
    loader = body[body.index("function addLiveBuoyLayer() {"):body.index("addLiveBuoyLayer();\n")]
    for needle in ("window.AllshoreLiveList.create({", "early: (window.__early && window.__early.live) || null",
                   "if (window.__early) window.__early.live = null;", "rebuildLiveBuoyMarkers(!info.same);",
                   "performance.mark('allshore:live-first-markers'", "document.visibilityState !== 'hidden'",
                   "try { storage = window.localStorage; } catch (e) {}", "if (!window.AllshoreLiveList) {"):
        assert needle in loader, needle
    r = A.app.test_client().get("/ui/livelist.js?v=" + A.UI_ASSET_VERSION)
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=31536000, immutable"
    assert r.headers["Content-Type"].startswith("application/javascript")


def test_tide_stations_on_the_page(client):
    """Plan section 38: the tide layer (markers from zoom 9, the legend entry with its note, the credit), the third
    window with its own ids, chip and phone bar stacking, the Escape order, the unit / zone hooks, the module's tag."""
    body = client.get("/?station=51201").get_data(as_text=True)
    tag = body.index('<script src="/ui/tides.js?v=%s"></script>' % A.UI_ASSET_VERSION)
    assert body.index('<script src="/ui/livelist.js?v=%s"></script>' % A.UI_ASSET_VERSION) < tag < body.index("const map = L.map(")
    r = client.get("/ui/tides.js?v=" + A.UI_ASSET_VERSION)
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=31536000, immutable"
    for needle in (
        # the layer and its gate
        "const TIDE_MIN_ZOOM = 9;", "function tideVisible() { return map.hasLayer(tideLayer) && map.getZoom() >= TIDE_MIN_ZOOM; }",
        "const sig = (on ? '1' : '0') + '|' + (activeTideId || '') + '|' + renderSignature(vis) + '|' + nudgeSignature(nudges);",
        "const want = on && !tideStationsData.length ? tideListNote : '';", "tides: saved.tides !== false", "tides: map.hasLayer(tideLayer)",
        "'<span class=\"lc-dot lc-tide\"></span>Tide stations<span class=\"lc-note\" data-tide-note aria-hidden=\"true\"></span>': tideLayer",
        "layersControl._update = function () { const r = lcUpdate.apply(this, arguments); paintTideNote(); paintWindNote(); paintLiveNote(); return r; };",
        "map.on('overlayadd overlayremove', function (e) { if (e.layer === tideLayer) rebuildTideMarkers(true); });",
        "rebuildTideMarkers(true);\n        return;", "rebuildTideMarkers(false);\n      }, REFRESH_DEBOUNCE_MS);",
        "const TIDE_CREDIT = 'Tides: <a href=\"https://tidesandcurrents.noaa.gov/\" target=\"_blank\" rel=\"noopener\">NOAA CO-OPS</a>';",
        "marker.bindTooltip(textTip(label), { permanent: false, direction: 'top', offset: [0, -8] });",
        "if (window.AllshoreTools && window.AllshoreTools.active()) { window.AllshoreTools.click(e.latlng, e.originalEvent); return; }   // markers do not pass clicks to the map\n          activeTideId = s.id;",
        ".tide-icon { background: url(\"data:image/svg+xml,", ".tide-icon.tide-icon-active { background-image:",
        "L.divIcon({ className: 'tide-icon', html: '', iconSize: [18, 18], iconAnchor: [9, 9] })",
        "const all = on ? visibleCopies(tideStationsData) : [], vis = on ? thinTideCopies(all) : [];", "keyOpens(marker, 't:' + s.id + '#' + c.o, 'twHeader');", "keyOpens(marker, 'l:' + s.id + '#' + c.o, 'lwHeader');",
        "autoPanOnFocus: false", "if (marker.on) marker.on('add', bind);", "inset: calc(-1 * var(--tide-tap, 10px));",
        "contain: inline-size;", "const TIDE_CELL_PX = 32;", "body.fw-chip .tide-win:not(.fw-min):not(.fw-max) {",
        "map.on('overlayadd', function (e) { if (e.layer === liveBuoyLayer) rebuildLiveBuoyMarkers(true); });",
        ".live-win.fw-min .fw-titles .fw-cycle, .tide-win.fw-min .fw-titles .fw-cycle { flex: 0 1000 auto;", ".tide-table th.tide-lab { text-align: left; }", "if (h && h.focus && (!a || a === document.body || (w && w.contains && w.contains(a)))) h.focus({ preventScroll: true });",
        # the tablet chip rule's pieces no behaviour test can reach (fresh check T23, T30, T31)
        "if (min && fw.offsetHeight > 0) document.documentElement.style.setProperty('--fw-chip-h', fw.offsetHeight + 'px');",
        "@media (min-width: 501px) and (max-width: 1179.98px) and (min-height: 501px) {",
        "body.fw-chip.live-chip .tide-win:not(.fw-min):not(.fw-max) { --tw-bottom: max(calc(20px + var(--fw-chip-h, 44px)), calc(26px + var(--lw-chip-h, 40px))); }",
        "body.fw-chip.tide-chip .live-win:not(.fw-min):not(.fw-max) { --lw-bottom: max(calc(20px + var(--fw-chip-h, 44px)), calc(26px + var(--tw-chip-h, 40px))); }", "const TIDE_ALL_ZOOM = 11;",
        # the window
        '<section id="tideWin" class="fwin tide-win" hidden role="region" aria-label="Tide station">',
        'id="twHeader"', 'id="twMin"', 'id="twClose"', 'id="twBody"', 'id="twResize"', 'id="tideStrip" class="tide-strip"',
        'id="tideMeta"', 'id="tideRetry"', ".tide-table .tide-lab { position: sticky; left: 0; z-index: 3;",
        ".tide-scroll { overflow-x: auto; overflow-y: hidden;", "strip: $('tideStrip'), meta: $('tideMeta') },",
        ".fwin.tide-win { right: 18px; bottom: 18px; width: min(880px, calc(100vw - 76px)); height: min(70vh, 760px); z-index: 2000; }",
        "body.live-chip .tide-win.fw-min { bottom: calc(20px + var(--lw-chip-h, 40px)) !important; }",
        ".tide-win.fw-min { bottom: calc(var(--fw-bar-h, 48px) + var(--lw-bar-h, 0px)) !important; }",
        "document.documentElement.style.setProperty('--lw-bar-h', liveBar + 'px');",
        ".live-chip.tide-chip .forecast-win:not(.fw-min):not(.fw-max) { bottom: calc(78px + var(--lw-chip-h, 40px)); }",
        ".live-chip .tide-win:not(.fw-min):not(.fw-max) { bottom: calc(26px + var(--lw-chip-h, 40px)); }",
        ".tide-chip .live-win:not(.fw-min):not(.fw-max) { bottom: calc(26px + var(--tw-chip-h, 40px)); }",
        "[['liveBuoyPanel', '--lw-chip-h'], ['tideWin', '--tw-chip-h'], ['windWin', '--ww-chip-h']].forEach(function (c) {",
        ".live-win.fw-front, .tide-win.fw-front { z-index: 2100; }",
        "function front(which) { wins.forEach(function (w, i) { if (w) w.classList.toggle('fw-front', i === which); }); }",
        "document.addEventListener('allshore:tidepanel', function () { front(2); });",
        "tideWin = window.AllshoreForecast.createTideWindow({ onClose: tideWindowClosed, onResize: function () { if (tideView) tideView.resize(); },",
        "document.body.classList.toggle('tide-chip', !!(d.open && d.mode === 'min'));",
        "closeTidePanel: closeTideWindow,", "tideOpen: function () { return !!(tideWin && tideWin.isOpen() && tideWin.window.mode !== 'min'); },",
        "if (tideView) tideView.setUnit(getSelectedUnit());", "if (tideView) tideView.setZone(tzSel.value || '');",
        "function tideZone(station) { const sel = document.getElementById('tz'); return (sel && sel.value) || (station && station.tz) || 'UTC'; }",
        "let tideWin = null;",
    ):
        assert needle in body, needle
    assert body.index("let tideWin = null;") < body.index("const map = L.map(")
    assert body.count("options: { position: 'topleft' },") == 1
    assert 'id="tideWin"' in body[body.index('id="liveBuoyPanel"'):body.index("leaflet@1.9.4/dist/leaflet.js")]   # after the live window, before the map script
    assert "tideDetailSeq" not in body                                              # the module keeps the sequence (tides.js)



def test_parked_chips_stay_below_the_expanded_windows(client):
    """G27 B-P3-8 (accepted at the test-site check): a chip is never raised above the station windows; drawn above, the
    forecast chip covered the tide window's lower-left corner (its content), worse than two of its own buttons covered."""
    body = client.get("/?station=51201").get_data(as_text=True)
    assert ".fwin.fw-min { z-index" not in body


def test_no_window_covers_another_whole_at_their_default_boxes(client):
    """Plan section 38, step 4: the three windows open at the bottom-right corner. Widths fall forecast > tide > live and
    heights rise the other way (caps included), so on every desktop size the window behind shows an edge of 24 px or
    more to click, whichever is in front. A raised box (a chip below it) stops below the top-right corner."""
    body = client.get("/?station=51201").get_data(as_text=True)
    def rule(sel):
        m = re.search(re.escape(sel) + r" \{ right: (\d+)px; bottom: (\d+)px; width: min\((\d+)px, calc\(100vw - (\d+)px\)\); "
                      r"height: min\((\d+)vh, (\d+)px\)", body)
        assert m, sel
        return tuple(int(g) for g in m.groups())
    rules = {"forecast": rule(".forecast-win"), "tide": rule(".fwin.tide-win"), "live": rule(".fwin.live-win")}
    def box(r, W, H):                                    # (left, top, right, bottom) in the viewport
        right, bottom, wpx, wm, vh, cap = r
        w, h = min(wpx, W - wm), min(vh * H / 100.0, cap)
        return (W - right - w, H - bottom - h, W - right, H - bottom)
    # the wind window (section 39 step 5b, owner): at the top-right corner below the legend / tools / gear (the corner
    # measures about 180 px on a desktop: --map-topright-h), as tall as its content (up to ~530 px for a gauge with its
    # weather; 540 here as the bound), never lower than 42 px above the bottom edge. Its box (G28): its left edge right
    # of the top-left controls (the logo and the overlay panel end ~360 px from the left; ~204 px below 576 px, where the
    # overlay is a sheet: --map-topleft-r, B-7); while the live or tide window is open, its right edge WW_GAP in from the
    # margin (--ww-gap) and, when its top does not show WW_ABOVE above one of them, WW_OUT wider than that window
    # (--ww-min-w): every open window keeps a strip to click whichever is in front (B-13; the page measures both).
    m = re.search(r"\.fwin\.wind-win \{ right: calc\((\d+)px \+ var\(--ww-gap, 0px\)\); top: calc\(var\(--ww-top, var\(--map-topright-h, 120px\)\) \+ (\d+)px\); "
                  r"bottom: auto; height: auto;\n"
                  r"\s+--ww-w: min\(calc\(100vw - (\d+)px\), max\(var\(--ww-min-w, 0px\), min\((\d+)px, calc\(100vw - \3px\), "
                  r"calc\(100vw - (\d+)px - var\(--map-topleft-r, 0px\)\)\)\)\);\n"
                  r"\s+width: calc\(var\(--ww-w\) - var\(--ww-gap, 0px\)\);\n"
                  r"\s+max-height: calc\(100vh - var\(--ww-top, var\(--map-topright-h, 120px\)\) - (\d+)px - var\(--chips-h, 0px\)\); z-index: 2000; \}", body)
    assert m, "the wind window's box"
    w_right, w_gap, w_margin, w_px, w_left_gap, w_reserve = (int(g) for g in m.groups())
    k = re.search(r"var WW_GAP = (\d+), WW_OUT = (\d+), WW_ABOVE = (\d+);", body)
    assert k, "the wind window's constants"
    GAP, OUT, ABOVE = (int(g) for g in k.groups())
    for needle in ("if (wtop !== null && r.top - wtop < WW_ABOVE) minW = Math.max(minW, Math.round(r.width) + WW_OUT);",
                   "root.style.setProperty('--ww-gap', (open ? WW_GAP : 0) + 'px');",
                   "if (minW) root.style.setProperty('--ww-min-w', minW + 'px'); else root.style.removeProperty('--ww-min-w');"):
        assert needle in body, needle
    # the corner: the row of buttons alone ~64 px (70 on phones; plan section 54); 180 px = the map tools' bar shown, or the old legend
    WIND_CONTENT_PX = 540
    def topleft_r(W):
        return 204 if W < 576 else 360
    # from 1280 px (the wide forecast box from 96 px down) the wind window's top is floored (--ww-top): with the row alone
    # (~64 px, plan section 54) it would start above the forecast window and, with every window open, hide it whole
    fl = re.search(r"\.fwin\.wind-win \{ --ww-top: max\(var\(--map-topright-h, 120px\), (\d+)px\); \}", body)
    assert fl, "the wind window's floor on wide screens"
    WW_FLOOR = int(fl.group(1))
    def wind_box(W, H, others, corner=180):
        corner = max(corner, WW_FLOOR) if W >= 1280 else corner
        top = corner + w_gap
        base = min(w_px, W - w_margin, W - w_left_gap - topleft_r(W))
        open_ = [o for o in ("live", "tide") if o in others]
        stick = [others[o][2] - others[o][0] + OUT for o in open_ if others[o][1] - top < ABOVE]
        ww = min(W - w_margin, max(max(stick or [0]), base))
        gap = GAP if open_ else 0
        h = min(WIND_CONTENT_PX, H - corner - w_reserve)
        right = W - w_right - gap
        return (right - (ww - gap), top, right, top + h)
    wb = re.search(r"@media \(min-width: (\d+)px\) and \(min-height: 501px\) \{\n\s+\.forecast-win:not\(\.fw-min\):not\(\.fw-max\) \{\n"
                   r"\s+left: calc\(var\(--map-topleft-r, 360px\) \+ (\d+)px\); right: calc\(var\(--map-topright-w, 170px\) \+ (\d+)px\); "
                   r"top: (\d+)px; width: auto; height: auto; \}", body)
    assert wb, "the wide forecast box"
    WIDE_MIN, WIDE_L, WIDE_R, WIDE_TOP = (int(g) for g in wb.groups())
    assert WIDE_MIN == 1280
    def wide(W, H, tl, tr, bottom):                      # the wide box: between the corners, from WIDE_TOP to `bottom`
        return (tl + WIDE_L, WIDE_TOP, W - tr - WIDE_R, H - bottom)
    def minus(rect, cut):                                # rect minus cut: up to four rectangles, each as large as it can be
        l, t, r, b = rect                                # (they overlap: a free corner is never split into two thin strips)
        cl, ct, cr, cb = cut
        if cr <= l or cl >= r or cb <= t or ct >= b:
            return [rect]
        out = []
        if ct > t: out.append((l, t, r, ct))
        if cb < b: out.append((l, cb, r, b))
        if cl > l: out.append((l, t, cl, b))
        if cr < r: out.append((cr, t, r, b))
        return out
    def clickable(rect, above):                          # a 24 x 24 px area of rect outside every window above it
        parts = [rect]
        for c in above:
            parts = [q for p in parts for q in minus(p, c)]
        return any(p[2] - p[0] >= 24 and p[3] - p[1] >= 24 for p in parts)
    ORDER = ["forecast", "live", "tide", "wind"]         # the page order: a later window lies above an earlier one
    # plan section 46: from 1280 px the forecast window opens between the top-left controls (the logo alone 204 px, with
    # the overlay panel 360 px: --map-topleft-r) and the top-right row (--map-topright-w, about 136 px; up to ~200 with the old
    # legend or ~300 with the tools bar: plan section 54), from 96 px down
    for W in (520, 600, 768, 820, 900, 956, 1024, 1180, 1280, 1366, 1440, 1536, 1600, 1920, 2560):
        for H in (501, 560, 600, 700, 768, 800, 864, 900, 1024, 1080, 1200, 1440):
          for TL, TR in (((360, 170),) if W < 1280 else ((204, 136), (360, 136), (204, 150), (360, 150), (204, 200), (360, 200))):
           for CORNER in (50, 64, 70, 180):               # the row alone (section 54: desktop 64, phone 70), or the tools bar under it
            b = {n: box(r, W, H) for n, r in rules.items()}
            if W >= 1280:
                b["forecast"] = wide(W, H, TL, TR, 16)
            b["wind"] = wind_box(W, H, b, CORNER)
            for front in ORDER:
                stack = [n for n in ORDER if n != front] + [front]      # bottom to top
                for i, n in enumerate(stack[:-1]):
                    if front == "forecast" and n == "wind" and H < 570:
                        continue                         # a short desktop screen: Escape closes it (known since 5b)
                    assert clickable(b[n], [b[x] for x in stack[i + 1:]]), (W, H, TL, TR, CORNER, "front", front, "hidden", n, b)
    # only the forecast and wind windows open: the wind window keeps its full width at the margin (the rule is for the
    # live / tide window)
    full = wind_box(1920, 1080, {"forecast": box(rules["forecast"], 1920, 1080)})
    assert full[2] - full[0] == w_px and full[2] == 1920 - w_right
    # B-7 holds whenever the live / tide window leaves room: from 1366 px wide the wind window never reaches over the
    # top-left controls; below that, with a live or tide window open, it can (the strip rule wins: owner, step 7b)
    for W in (1366, 1440, 1536, 1600, 1920, 2560):
        for H in (501, 600, 768, 900, 1080, 1440):
            b = {n: box(r, W, H) for n, r in rules.items()}
            assert wind_box(W, H, b)[0] >= topleft_r(W), (W, H)
    for needle in (                                      # the raised boxes give way in height below the corner
        "height: min(60vh, 720px, calc(100vh - 142px - var(--map-topright-h, 0px))); }",
        "height: min(60vh, 720px, calc(100vh - 150px - var(--lw-chip-h, 40px) - var(--map-topright-h, 0px))); }",
        ".live-chip .tide-win:not(.fw-min):not(.fw-max) { height: min(70vh, 760px, calc(100vh - 34px - var(--lw-chip-h, 40px) - var(--map-topright-h, 0px))); }",
        ".tide-chip .live-win:not(.fw-min):not(.fw-max) { height: min(76vh, 800px, calc(100vh - 34px - var(--tw-chip-h, 40px) - var(--map-topright-h, 0px))); }"):
        assert needle in body, needle


def _largest_visible(rect, above):
    """The side of the largest square of rect outside every rect in `above` (exact: compressed coordinates)."""
    xs = sorted({rect[0], rect[2]} | {v for a in above for v in (a[0], a[2]) if rect[0] < v < rect[2]})
    ys = sorted({rect[1], rect[3]} | {v for a in above for v in (a[1], a[3]) if rect[1] < v < rect[3]})
    vis = [[not any(a[0] < (xs[i] + xs[i + 1]) / 2 < a[2] and a[1] < (ys[j] + ys[j + 1]) / 2 < a[3] for a in above)
            for i in range(len(xs) - 1)] for j in range(len(ys) - 1)]
    best = 0.0
    for i0 in range(len(xs) - 1):
        for i1 in range(i0 + 1, len(xs)):
            for j0 in range(len(ys) - 1):
                for j1 in range(j0 + 1, len(ys)):
                    if all(vis[j][i] for j in range(j0, j1) for i in range(i0, i1)):
                        best = max(best, min(xs[i1] - xs[i0], ys[j1] - ys[j0]))
    return best


def test_live_and_tide_raised_together_keep_every_window_clickable(client):
    """G28 re-check R2-2: where one rule raises both station windows (the forecast window parked below 1180 px; the
    wind window parked at any width) their bottoms are equal; the tide window's height cap is 32 px below the live
    window's, so the wider tide window never hides the live one, and a raised forecast box stops 64 px below the
    station windows' highest top (also with the live or the tide window parked: section 38's raised boxes), so the
    forecast window in front never hides them on short screens. Each open window keeps a 24-px square to click,
    whichever is in front (exact largest visible square; chip heights at their CSS defaults)."""
    body = client.get("/?station=51201").get_data(as_text=True)
    for needle in (
        "bottom: var(--tw-bottom); height: min(70vh, 760px, calc(100vh - 40px - var(--tw-bottom) - var(--map-topright-h, 0px))); }",
        "bottom: var(--lw-bottom); height: min(76vh, 800px, calc(100vh - 8px - var(--lw-bottom) - var(--map-topright-h, 0px))); }",
        "      height: min(70vh, 760px, calc(100vh - 66px - var(--chips-h, 0px) - var(--ww-chip-h, 40px) - var(--map-topright-h, 0px))); }",
        "      height: min(76vh, 800px, calc(100vh - 34px - var(--chips-h, 0px) - var(--ww-chip-h, 40px) - var(--map-topright-h, 0px))); }"):
        assert needle in body, needle
    CORNER, FW_CHIP, WW_CHIP, GAP, OUT, ABOVE = 180, 44, 45, 60, 30, 30

    def box(right, bottom, w, h, W, H):
        return (W - right - w, H - bottom - h, W - right, H - bottom)

    def wind(W, H, others):                                      # the measured box (syncWindWidth), as in the sweep above
        top = CORNER + 8
        base = min(1030, W - 44, W - 26 - (204 if W < 576 else 360))
        stick = [o[2] - o[0] + OUT for o in others if o[1] - top < ABOVE]
        ww = min(W - 44, max(max(stick or [0]), base))
        h = min(540, H - CORNER - 50 - 2)
        return (W - 18 - GAP - (ww - GAP), top, W - 18 - GAP, top + h)

    fails = []
    for W in (600, 768, 820, 900, 956, 1024, 1100, 1152, 1179):                  # A: the forecast window parked
        for H in (501, 560, 600, 640, 700, 768, 800, 840, 900, 1024, 1200, 1440):
            bot = max(20 + FW_CHIP, 18)
            live = box(18, bot, min(820, W - 136), min(0.76 * H, 800, H - 8 - bot - CORNER), W, H)
            tide = box(18, bot, min(880, W - 76), min(0.70 * H, 760, H - 40 - bot - CORNER), W, H)
            for wins in ({"live": live, "tide": tide}, {"live": live, "tide": tide, "wind": wind(W, H, [live, tide])}):
                order = [n for n in ("live", "tide", "wind") if n in wins]
                for front in order:
                    stack = [n for n in order if n != front] + [front]
                    for i, n in enumerate(stack[:-1]):
                        k = _largest_visible(wins[n], [wins[x] for x in stack[i + 1:]])
                        if k < 24:
                            fails.append(("A", W, H, front, n, round(k, 1)))
    for W in (1024, 1180, 1280, 1366, 1440, 1536, 1600, 1920, 2560):              # B: the wind window parked
        for H in (501, 560, 600, 640, 700, 768, 800, 840, 864, 900, 1024, 1080, 1200, 1440):
            bot = 26 + WW_CHIP
            live = box(18, bot, min(820, W - 136), min(0.76 * H, 800, H - 34 - WW_CHIP - CORNER), W, H)
            tide = box(18, bot, min(880, W - 76), min(0.70 * H, 760, H - 66 - WW_CHIP - CORNER), W, H)
            fc = box(16, 30 + WW_CHIP, min(1180, W - 32), min(0.60 * H, 720, H - 102 - WW_CHIP - CORNER), W, H)
            if W >= 1280:                                                          # the wide box (plan section 46), raised
                fc = (360 + 16, 96, W - 170 - 16, H - 30 - WW_CHIP)
            wins = {"forecast": fc, "live": live, "tide": tide}
            for front in ("forecast", "live", "tide"):
                stack = [n for n in ("forecast", "live", "tide") if n != front] + [front]
                for i, n in enumerate(stack[:-1]):
                    k = _largest_visible(wins[n], [wins[x] for x in stack[i + 1:]])
                    if k < 24:
                        fails.append(("B", W, H, front, n, round(k, 1)))
    LW_CHIP = TW_CHIP = 40
    for W in (1180, 1280, 1366, 1440, 1536, 1920, 2560):                          # C / D: the live / the tide window parked
        for H in (501, 560, 600, 640, 700, 768, 800, 840, 900, 1024, 1080, 1440):
            fc = box(16, 70, min(1180, W - 32), min(0.60 * H, 720, H - 142 - CORNER), W, H)
            if W >= 1280:                                                          # the wide box (plan section 46), raised
                fc = (360 + 16, 96, W - 170 - 16, H - 70)
            tide = box(18, 26 + LW_CHIP, min(880, W - 76), min(0.70 * H, 760, H - 34 - LW_CHIP - CORNER), W, H)
            live = box(18, 26 + TW_CHIP, min(820, W - 136), min(0.76 * H, 800, H - 34 - TW_CHIP - CORNER), W, H)
            for label, station in (("C", ("tide", tide)), ("D", ("live", live))):
                for with_wind in (False, True):
                    wins = {"forecast": fc, station[0]: station[1]}
                    if with_wind:
                        wins["wind"] = wind(W, H, [station[1]])
                    order = [n for n in ("forecast", "live", "tide", "wind") if n in wins]
                    for front in order:
                        stack = [n for n in order if n != front] + [front]
                        for i, n in enumerate(stack[:-1]):
                            k = _largest_visible(wins[n], [wins[x] for x in stack[i + 1:]])
                            if k < 24 and not (front == "forecast" and n == "wind" and H < 570):   # K-9 (Escape)
                                fails.append((label, W, H, front, n, round(k, 1)))
    assert fails == [], fails[:20]


def test_basemap_draws_missing_esri_tiles_from_coarser_ones(client):
    """Plan section 39 step 5b: the map zooms to 18; Esri serves "Map data not yet available" where it has no tile (the
    open ocean from about zoom 14). static_ui/basemap.js looks tiles up in Esri's tilemap and draws a missing one from the
    closest coarser tile; the page builds its imagery with it (Leaflet's own layer when the module is missing)."""
    body = client.get("/?station=51201").get_data(as_text=True)
    tag = body.index('<script src="/ui/basemap.js?v=%s"></script>' % A.UI_ASSET_VERSION)
    assert body.index("leaflet@1.9.4/dist/leaflet.js") < tag < body.index("const mapKey = 'mapView';")
    assert ("(window.AllshoreBasemap ? window.AllshoreBasemap.tileLayer : L.tileLayer)(\n"
            "      'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',") in body
    assert "const MAX_MAP_ZOOM = 18;" in body
    r = client.get("/ui/basemap.js?v=" + A.UI_ASSET_VERSION)
    assert r.status_code == 200 and "AllshoreBasemap" in r.get_data(as_text=True)


def test_wind_stations_on_the_page(client):
    """Plan section 39: the wind layer (flags from zoom 9, the legend entry with its note, the credit), the fourth window
    (#windWin: its box, its chip on top of the station chips through the measured --chips-h, its phone bar above the
    tide bar, a parked wind chip raising the other windows' boxes), the hooks (unit, zone, Escape, the tools' lists),
    and the module's script tag."""
    body = client.get("/?station=51201").get_data(as_text=True)
    tag = body.index('<script src="/ui/winds.js?v=%s"></script>' % A.UI_ASSET_VERSION)
    assert body.index('<script src="/ui/tides.js?v=') < tag < body.index("const mapKey = 'mapView';")
    r = client.get("/ui/winds.js?v=" + A.UI_ASSET_VERSION)
    assert r.status_code == 200 and "AllshoreWinds" in r.get_data(as_text=True)
    for needle in (
        "const WIND_MIN_ZOOM = 9;", "function windVisible() { return map.hasLayer(windLayer) && map.getZoom() >= WIND_MIN_ZOOM; }",
        "const WIND_GAP_PX = 40;", "const WIND_CELL_PX = 64;", "const WIND_ALL_ZOOM = 13;", "const MAX_MAP_ZOOM = 18;",
        "function windRank(s) { return s.kind === 'gauge' || s.kind === 'buoy' ? 0 : s.kind === 'airport' || s.kind === 'cman' || s.kind === 'station' ? 1 : s.kind === 'cwop' ? 3 : 2; }",
        "const sig = (on ? '1' : '0') + '|' + (activeWindId || '') + '|' + renderSignature(vis) + '|h:' + Array.from(hollow, function (e) { return e[0] + '=' + windDotState(e[1]); }).sort().join(',');",
        "function windDotState(e) { return (e.disc !== undefined ? 'd' + e.disc + ':' : '') + e.kind + ':' + e.f.id + ':' + e.r; }",
        "if (disc !== null && flag.style && flag.style.setProperty) flag.style.setProperty('--wind-disc', disc + 'px');   // R2-3",
        "const WIND_DISC_PX = 13, WIND_RING_R = 5;", "const WIND_DOT_R = { forecast: 4, point: 7 };",
        "map.on('overlayadd overlayremove', function (e) { if (e.layer === forecastLayer || e.layer === pointLayer) rebuildWindMarkers(false); });",
        "syncWindWidth();                                     // the boxes just moved: measure them now (G28 re-check R2-1)",
        "syncWindWidth();                                   // the raised boxes move with the forecast chip (R2-1)",
        "if (window.ResizeObserver && typeof map !== 'undefined' && map && map.getContainer) {",
        "const WIND_DOT_HIT_PX = 8, WIND_DOT_HIT_TOUCH_PX = 12;", "if (dot && flag.classList) flag.classList.add('wind-hollow');",
        "function windTapPx() { return windTouch() ? WIND_DOT_HIT_TOUCH_PX : WIND_DOT_HIT_PX; }",
        "function windTouch() { return !!(window.matchMedia && window.matchMedia('(any-pointer: coarse)').matches); }   // any touch screen (G35 B-7)",
        "if (typeof rebuildWindMarkers === 'function') rebuildWindMarkers(false);   // a flag over a new point turns hollow (B-5)",
        "const hit = windDotHit(drawn, e && e.originalEvent);", "if (hit) { pickDot(hit); return; }",
        "let windByKey = new Map();", "windByKey.forEach(function (d) { windLayer.removeLayer(d.marker); });",
        "if (old && old.state === state && old.lng === c.lng) { windByKey.delete(key); next.set(key, old); drawnNow.push(old); return; }",
        "if (typeof tideTargets === 'function') tideTargets();", "const pad = shared ? 0 : Math.max(0, Math.min(10, Math.floor(c / 2 - p.h)));",
        "windView.load(station, { unit: getSelectedUnit(), zone: windZone(station), reading: windReading(station.id) });",
        ".leaflet-marker-icon.wind-marker { background: transparent; border: 0; cursor: pointer; pointer-events: none; }",
        ".wind-arrow path { fill: currentColor; stroke: #fff; stroke-width: 1.2; paint-order: stroke; pointer-events: auto; }",
        "@media (any-pointer: coarse) { .wind-ring::after { content: ''; position: absolute; inset: calc(-1 * var(--wind-disc, 13px)); border-radius: 50%; } }",
        ".wind-hollow .wind-ring { left: 13px; top: 13px; width: 14px; height: 14px; background: transparent; border: 2px solid currentColor;",
        ".fw-titles strong { flex: 0 0 auto; max-width: 100%; overflow: hidden; text-overflow: ellipsis; }",
        "if (!map.hasLayer(windLayer) || !on) return '';", "wind: saved.wind !== false", "wind: map.hasLayer(windLayer)",
        "'<span class=\"lc-dot lc-wind\"></span>Wind stations<span class=\"lc-note\" data-wind-note aria-hidden=\"true\"></span>': windLayer",
        "map.on('overlayadd overlayremove', function (e) { if (e.layer === windLayer) rebuildWindMarkers(true); });",
        "rebuildWindMarkers(true);\n        rebuildTideMarkers(true);", "rebuildWindMarkers(false);\n        rebuildTideMarkers(false);",
        "const WIND_CREDIT = 'Wind: <a href=\"https://www.ndbc.noaa.gov/\" target=\"_blank\" rel=\"noopener\">NOAA NDBC</a>",
        "W.updateFlag(d.flag, r, now, unit, windStaleS);", "const flag = W ? W.buildFlag(document, r, now, unit, windStaleS) : document.createElement('div');",
        "L.divIcon({ className: 'wind-marker' + (active ? ' wind-marker-active' : ''), html: flag, iconSize: [40, 40], iconAnchor: [20, 20], tooltipAnchor: [0, -14] })",
        "zIndexOffset: active ? 350 : 300, autoPanOnFocus: false", "keyOpens(marker, 'w:' + s.id + '#' + c.o, 'wwHeader');",
        "refocusMarker(windDrawn.map(function (d) { return d.marker; }), focusKey, 'w:');",
        "if (window.AllshoreTools && window.AllshoreTools.active()) { window.AllshoreTools.click(e.latlng, e.originalEvent); return; }   // markers do not pass clicks to the map\n          const hit = windDotHit(drawn, e && e.originalEvent);",
        "windFeed = window.AllshoreWinds.createWindFeed({", "feed.start({", "        feed.stop();\n        // what the stopped feed brought is forgotten", "windLastFeed = null; windStallText = '';", "syncWindFeed(on);",
        ".wind-arrow { position: absolute; left: 0; top: 0; transform-origin: 20px 20px;",
        ".wind-num { position: absolute; transform: translate(-50%, -50%);", ".wind-flag.wind-stale, .wind-cur-dot.wind-stale { color: #9aa3ad; }",
        ".wind-marker-active .wind-ring { box-shadow: 0 0 0 2.5px #ff6b5a; }", ".leaflet-container.tools-active .wind-marker { cursor: crosshair; }",
        '<section id="windWin" class="fwin wind-win" hidden role="region" aria-label="Wind station">',
        'id="wwHeader"', 'id="wwMin"', 'id="wwClose"', 'id="wwBody"', 'id="wwResize"', 'id="windCurrent"', 'id="windChart"', 'id="windArrows"',
        'id="windTable"', 'id="windMeta"', 'id="windRetry"', 'id="windConditions"', 'id="windTemp"',
        "current: $('windCurrent'), conditions: $('windConditions'), chart: $('windChart'), arrows: $('windArrows'),\n               temp: $('windTemp'), table: $('windTable'), meta: $('windMeta') },",
        ".fwin.wind-win { right: calc(18px + var(--ww-gap, 0px)); top: calc(var(--ww-top, var(--map-topright-h, 120px)) + 8px); bottom: auto; height: auto;",
        ".fwin.wind-win { max-height: none; }                       /* the full screen, like the others; its bar never capped (B-2) */",
        "window.addEventListener('resize', syncWindWidth);",
        "['allshore:livewin', 'allshore:tidewin', 'allshore:windwin'].forEach(function (n) { document.addEventListener(n, syncWindWidth); });",
        ".wind-win.fw-min {\n        right: 12px !important; left: auto !important; top: auto !important; bottom: calc(12px + var(--chips-h, 0px)) !important;",
        ".wind-win.fw-min { bottom: calc(var(--fw-bar-h, 48px) + var(--lw-bar-h, 0px) + var(--tw-bar-h, 0px)) !important; }",
        "document.documentElement.style.setProperty('--tw-bar-h', tideBar + 'px');",
        "body.wind-chip .fwin.tide-win:not(.fw-min):not(.fw-max) {   /* 32 px below the live window's top (R2-2) */\n      bottom: calc(26px + var(--chips-h, 0px) + var(--ww-chip-h, 40px));\n"
        "      height: min(70vh, 760px, calc(100vh - 66px - var(--chips-h, 0px) - var(--ww-chip-h, 40px) - var(--map-topright-h, 0px))); }",
        "body.wind-chip .fwin.live-win:not(.fw-min):not(.fw-max) {\n      bottom: calc(26px + var(--chips-h, 0px) + var(--ww-chip-h, 40px));",
        "body.wind-chip .fwin.forecast-win:not(.fw-min):not(.fw-max) {   /* 64 px below the live window's top (re-check) */\n      bottom: calc(30px + var(--chips-h, 0px) + var(--ww-chip-h, 40px));\n"
        "      height: min(60vh, 720px, calc(100vh - 102px - var(--chips-h, 0px) - var(--ww-chip-h, 40px) - var(--map-topright-h, 0px))); }",
        "body.fw-chip .fwin.wind-win:not(.fw-min):not(.fw-max) {   /* above the parked forecast chip too (it spans the width here) */\n        max-height: calc(100vh - var(--map-topright-h, 120px) - 50px - max(var(--chips-h, 0px), calc(var(--fw-chip-h, 44px) + 2px))); }",
        "body.fw-chip.wind-chip .fwin.tide-win:not(.fw-min):not(.fw-max) { --tw-bottom: max(calc(20px + var(--fw-chip-h, 44px)), calc(26px + var(--chips-h, 0px) + var(--ww-chip-h, 40px))); }",
        "body.fw-chip.wind-chip .fwin.live-win:not(.fw-min):not(.fw-max) { --lw-bottom: max(calc(20px + var(--fw-chip-h, 44px)), calc(26px + var(--chips-h, 0px) + var(--ww-chip-h, 40px))); }",
        "if (h && c[0] !== 'windWin') below += h + 8;", "document.documentElement.style.setProperty('--chips-h', below + 'px');",
        ".wind-win.fw-front { z-index: 2100; }", "html:not(.js) .wind-win { display: none !important; }",
        ".wind-win.fw-min .fw-titles .fw-cycle { flex: 0 1000 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; }",
        "document.addEventListener('allshore:windpanel', function () { front(3); });",
        "windWin = window.AllshoreForecast.createWindWindow({ onClose: windWindowClosed, onResize: function () { if (windView) windView.resize(); },",
        "document.body.classList.toggle('wind-chip', !!(d.open && d.mode === 'min'));",
        "closeWindPanel: closeWindWindow,", "windOpen: function () { return !!(windWin && windWin.isOpen() && windWin.window.mode !== 'min'); },",
        "if (windView) windView.setUnit(getSelectedUnit());", "refreshWindFlags();", "if (windView) windView.setZone(tzSel.value || '');",
        "function windZone(station) { const sel = document.getElementById('tz'); return (sel && sel.value) || (station && station.tz) || 'UTC'; }",
        "let windWin = null;", "document.getElementById('windSubtitle').textContent = windSubtitle(station);"):
        assert needle in body, needle
    assert body.index("let windWin = null;") < body.index("const map = L.map(")
    assert 'id="windWin"' in body[body.index('id="tideWin"'):body.index("leaflet@1.9.4/dist/leaflet.js")]   # after the tide window, before the map script
    assert body.index("const WIND_MIN_ZOOM = 9;") < body.index("function refreshWrappedMarkerCopies(")   # the state before the first rebuild
    assert body.index("// ---------------- Wind stations (plan section 39) ----------------") > body.index("map.on('zoomend', clampMapLatitude);")
    assert body.index("// ---------------- (end of the wind stations block) ----------------") < body.index("enforceSingleWorld();\n")
    assert ".wind-win { display: none !important; }" in body[body.index("@media print"):]
    assert "windDetailSeq" not in body                                              # the module keeps the sequence (winds.js)
    # owner, 2026-10-10: no "zoom in" text in the legend at any zoom (tide and wind layers alike)
    assert "zoom in to see" not in body and "zoom in for more" not in body and "hiddenInView" not in body


def test_no_home_button_and_the_forecast_window_opens_between_the_corners(client):
    """Plan section 46 (owner): the Home view button is gone (the (i) stays bottom-left; the view it saved is no longer
    read and its key is removed); from 1280 px the expanded forecast window opens right of the logo / overlay panel and
    left of the legend / gear, from 96 px down; a parked chip still raises its bottom (the raised rules' heights give
    way there, so top + bottom decide), maximised / minimised / phones / narrower desktops are unchanged, and the
    corner's width is measured without the map tools' result bar, also on a resize of the corner."""
    body = client.get("/?station=51201").get_data(as_text=True)
    for gone in ("home-control", "home-btn", "home-menu", "HomeControl", "loadHomeView", "saveHomeView", "HOME_PRESETS",
                 "data-preset", "Home view menu", "writeSessionView", "clearSessionView"):
        assert gone not in body, gone
    assert "try { localStorage.removeItem('allshore.mapHome.v1'); } catch (e) {}" in body
    assert "const view = validSaved ? saved : null;" in body
    assert "map.addControl(new AttrToggle());" in body and "options: { position: 'bottomleft' }," in body
    css = body[body.index("@media (min-width: 1280px) and (min-height: 501px) {"):]
    css = css[:css.index("\n    }\n")]
    assert "top: 96px; width: auto; height: auto; }" in css
    for raised in (".live-chip .forecast-win:not(.fw-min):not(.fw-max)", ".tide-chip .forecast-win:not(.fw-min):not(.fw-max)",
                   ".live-chip.tide-chip .forecast-win:not(.fw-min):not(.fw-max)", "body.wind-chip .fwin.forecast-win:not(.fw-min):not(.fw-max)"):
        assert raised in css, raised
    assert css.rstrip().endswith("{ height: auto; }")
    # the wide box comes after every raised rule (same specificity, the later one wins)
    assert body.index("@media (min-width: 1280px) and (min-height: 501px) {") > body.index("body.wind-chip .fwin.forecast-win:not(.fw-min):not(.fw-max) {")
    assert ".forecast-win { right: 16px; bottom: 16px; width: min(1180px, calc(100vw - 32px)); height: min(60vh, 720px); }" in body
    fn = body[body.index("function measureTopRight() {"):]
    fn = fn[:fn.index("// Esri imagery.")]
    # the tools bar counts while it is shown: the wide box narrows beside it instead of being minimised (owner, G32 B-1); a
    # hidden bar measures 0 wide; the box's right edge (W - w - 16) is then always left of the bar's left edge (W - w)
    assert "if (r.width > 0) w = Math.max(w, base.right - r.left);" in fn and "tools-bar" not in fn
    assert "document.documentElement.style.setProperty('--map-topright-w', Math.round(w) + 'px');" in fn
    assert "new ResizeObserver(function () { measureTopRight(); }).observe(trCorner);" in fn


def test_geo_base_reaches_the_head_and_the_first_view_block_is_in_place(client, monkeypatch):
    """Plan section 46 (owner: the site opens where the visitor is, from the IP address, nothing fixed): env GEO_BASE
    (the geo Worker, tools/geo) reaches the head's early block as GEO; empty = the page opens as before. The head's
    rule and the module's are pinned by tests/ui/early.test.js and tests/ui/window.test.js; the first-view block by
    tests/ui/firstview.test.js. Here: the plumbing, the markers, the CSS that hides the map pane while the race runs,
    and the module's deferral (never a fetch before the head's answer)."""
    monkeypatch.delenv("GEO_BASE", raising=False)
    assert A._geo_base() == ""
    body = client.get("/?station=51201").get_data(as_text=True)
    assert 'var GEO = "";' in body
    head = body[body.index("// ---- early forecast"):body.index("// ---- end early forecast ----")]
    for needle in ("window.__early.place = place;", "window.__early.station = place.then(function (pl) { return start(pl && pl.sid ? pl.sid : '51201'); });",
                   "fetch(GEO + '/', { credentials: 'omit' })", "setTimeout(function () { res(null); }, 2000);",
                   "'allshore.defaultStation.v1'", "if (fixed || !GEO) { start(fixed || '51201'); return; }", "r.clone().json()"):
        assert needle in head, needle
    assert body.index("// ---- first view") < body.index("// ---------------- Favorite-station picker") and "// ---- end first view ----" in body
    fv = body[body.index("// ---- first view"):body.index("// ---- end first view ----")]
    for needle in ("el.classList.add('geo-pending');", "enforceSingleWorld();", "if (hadView) return;", "var place = window.__early && window.__early.place;",
                   "['pointerdown', 'wheel', 'keydown', 'touchstart']", "document.addEventListener('allshore:station', act, { once: true });",
                   "z = Math.floor(fit * 10) / 10;", "forgetMapView();", "t.closest('.leaflet-control, .ov-sheet')", "acted = true; el.classList.remove('geo-pending');"):
        assert needle in fv, needle
    assert "\n    enforceSingleWorld();\n" not in body, "the first view is set inside the block, not before it"
    assert ".leaflet-container.geo-pending .leaflet-map-pane { visibility: hidden; }" in body
    assert "window.addEventListener('resize', enforceSingleWorld);" in body
    # no view is kept while the race runs (a reload then asks again: G32 B-6); a retired default is forgotten (A-P3-3)
    save = body[body.index("function saveMapView(map) {"):]
    assert save.index("if (map.getContainer && map.getContainer().classList.contains('geo-pending')) return;") < save.index("sessionStorage.setItem(")
    assert "function forgetMapView() { try { sessionStorage.removeItem(mapKey); } catch (e) { /* storage blocked */ } }" in body
    assert "forgetRetiredDefault(forecastStationsData);" in body
    assert "if (!Array.isArray(list) || list.length < 100) return;" in body   # the server's 10-buoy fallback list decides nothing (G32 re-check P-1)
    # a default point is decided only once forecast.js has run (the list can land first: found 2026-10-18)
    assert "if (id.indexOf('pt_') === 0 && !(F && F.parsePointId)) {" in body
    assert "if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { forgetRetiredDefault(list); }, { once: true });" in body
    assert "document.addEventListener('allshore:stationshown', function () { refreshCurrent(); });" in body
    monkeypatch.setenv("GEO_BASE", "https://geo.example.test/")
    assert A._geo_base() == "https://geo.example.test"
    on = client.get("/?station=51201").get_data(as_text=True)
    assert 'var GEO = "https://geo.example.test";' in on
    assert on.replace('var GEO = "https://geo.example.test";', 'var GEO = "";') == body, "GEO_BASE changes one line of the page"
    monkeypatch.delenv("GEO_BASE")
    js = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static_ui", "forecast.js"), encoding="utf-8").read()
    for needle in ("var DEFAULT_KEY = 'allshore.defaultStation.v1';", "function resolveInitialState(search, stored, server, dflt)",
                   "function started() { return seq > 0; }", "var deferred = !initial.inline && !urlStation && !dflt && !!earlyStation && typeof earlyStation.then === 'function';",
                   "holdUrl = deferred;", "replaceState: function (url) { if (holdUrl && !loader.started()) return; holdUrl = false;",
                   "Promise.resolve(earlyStation).then(", "if (loader.started()) return;", "state.station = sid; syncSelects(); holdUrl = false;", "loader.load({});"):
        assert needle in js, needle


def test_an_oahu_visitor_opens_on_waimea_bay(client, monkeypatch):
    """Owner, 2026-10-18: a visitor whose IP places them on Oahu opens on Waimea Bay 51201, though another station may
    be nearer (Mililani -> 51211 before); anywhere else the nearest station. The behaviour is pinned by
    tests/ui/early.test.js; here the rule's place in the head's early block."""
    monkeypatch.setenv("GEO_BASE", "https://geo.allshoresurf.com")
    body = client.get("/").get_data(as_text=True)
    head = body[body.index("// ---- early forecast"):body.index("// ---- end early forecast ----")]
    assert "var PREFER = [{ box: [21.2, 21.76, -158.32, -157.6], id: '51201' }];" in head
    assert "var pref = preferred(lat, lon, list);" in head and "if (pref) return pref;" in head
    assert head.index("function preferred(") < head.index("function nearest(")


def test_the_shaka_and_the_star_set_the_default_and_the_favorites(client):
    """Plan section 46 (owner, 2026-10-18/19): the top of the forecast window and every saved row carry the same independent
    toggles: the shaka (the default, one at a time: localStorage allshore.defaultStation.v1, the key the head, the module
    and the first view read; listed first) and the star (a favorite; for a point: kept in My points); a row's ✕ and a removed
    point clear the default too; a forecast without a pick and another tab's change refresh the pair. The behaviour is pinned
    by tests/ui/mypoints.test.js; here the markup and the key names."""
    body = client.get("/?station=51201").get_data(as_text=True)
    assert "sr-default" not in body                                             # no blue star any more: the default is the shaka
    top = body[body.index('<div class="station-trigger">'):body.index('id="stationTrigger"')]
    assert top.index('id="stationDefaultToggle"') < top.index('id="stationFavToggle"'), "the shaka, then the star, then the name"
    for needle in ('id="stationDefaultToggle" class="sr-star sr-dflt" aria-pressed="false"', 'title="Default: site opens here"',
                   'id="stationFavToggle" class="sr-star" aria-pressed="false"', 'title="Favorite">☆</button>'):
        assert needle in top, needle
    assert '.sr-star[aria-pressed="false"] { color: #6c757d; }' in body     # hollow and grey when off, as each saved row
    assert ".sr-dflt:disabled svg { opacity: .35; }" in body
    assert '.fav-select::before' not in body                                    # the default shows by its shaka, not a mark before the name
    picker = body[body.index("(function initStationPicker() {"):body.index("// the picker just made the forecast bar taller")]
    for needle in ("const DEFAULT_KEY = 'allshore.defaultStation.v1';", "function writeDefault(sid, label) {", "function clearDefault() {",
                   "const dflToggle = document.getElementById('stationDefaultToggle');", "if (dflToggle) dflToggle.innerHTML = SHAKA;",
                   "b.setAttribute('aria-pressed', fav ? 'true' : 'false');", "b.setAttribute('aria-pressed', dflt ? 'true' : 'false');",
                   "[favToggle, fwFav].forEach(function (b) {", "[dflToggle, fwDflt].forEach(function (b) {",
                   "[dflToggle, fwDflt].forEach(function (b) { if (b) b.addEventListener('click', dflClick); });",
                   "[favToggle, fwFav].forEach(function (b) { if (b) b.addEventListener('click', favClick); });",
                   "if (isDefault(sid)) { if (!clearDefault()) { announce(DFLT_REFUSED); return; }",
                   "const r = P.store.add(sid, goneNames.get(sid) || '');",
                   "if (isFav(c.sid) && !removeFav(c.sid)) { announce(FAV_REFUSED); return; }",
                   "if (isDefault(c.sid) && !clearDefault()) { announce(DFLT_REFUSED); return; }", "if (gone && isDefault(p.id)) clearDefault();",
                   "if (d.ok && d.station) served.add(String(d.station));",
                   "if (!e.key || e.key === DEFAULT_KEY || e.key === FAV_KEY)",
                   "typeof forecastStationsData !== 'undefined' && Array.isArray(forecastStationsData)"):
        assert needle in picker, needle
    assert "var cur = (new URLSearchParams(location.search).get('station') || '').trim() || dflt0Id || (window.__initial || {}).station || '';" in body
    # the dropdown's saved rows (owner, 2026-10-18/19): the shaka (the default), the star (a favorite), ✕ (remove); hollow when
    # off, coloured when on (the shaka skin toned, the owner's own drawing); no hint line under the rows
    for needle in ("function toggles(sid, label, point, p) {", "sk.title = 'Default: site opens here';", "st.title = 'Favorite';",
                   "sk.setAttribute('aria-pressed', dflt ? 'true' : 'false');", "st.setAttribute('aria-pressed', fav ? 'true' : 'false');",
                   "rem.title = 'Remove';", "const acts = toggles(c.sid, c.label, false, null);", "const acts = toggles(p.id, label, true, p);",
                   "s.left = Math.max(8, Math.min(r.left, vw - 8 - 420)) + 'px';"):
        assert needle in picker, needle
    assert "sr-hint" not in body and "function choice(" not in picker and "sr-seg" not in body
    for css in ('.sr-fav[aria-pressed="true"] { color: #f0ad00; }',
                '.sr-dflt[aria-pressed="true"] .sk-h { fill: #f3c79c; stroke: #a86a43; }',
                ".sr-star, .pt-act, .sr-ic { min-width: 44px; min-height: 44px; }",            # phone targets
                "max-height: 340px; overflow-y: auto; min-width: 260px; max-width: 420px;",
                ".station-results li.sr-saved { flex-wrap: wrap; }",                       # phones: the label whole on its line
                ".station-results li.sr-saved .fav-select { flex: 1 0 100%; }",
                ".station-results li.sr-saved .sr-acts { margin-left: auto; }"):
        assert css in body, css
    # one key, three readers + the writer
    assert body.count("'allshore.defaultStation.v1'") == 7   # + forgetRetiredDefault's read and removal (G32 A-P3-3), the head's removal of a removed station (G35 B-8)
    # fix round 5 (the short fresh check): phones get the pair in the window's toolbar (owner, U1), the trigger's pair hidden
    # there; one state for both; a point's pair waits for its forecast (U3); one name memory (U4); the focus restored by
    # role and row (U5, U9); refused writes said (U6); a rank counter (U2). Behaviour: tests/ui/mypoints.test.js.
    bar = body[body.index('<div id="fwToolbar" class="fw-toolbar">'):body.index('id="viewBar"')]
    for needle in ('<span id="fwPair" class="fw-pair" role="group" aria-label="This station">',
                   'id="fwDefaultToggle" class="sr-star sr-dflt" aria-pressed="false"', 'id="fwFavToggle" class="sr-star" aria-pressed="false"'):
        assert needle in bar, needle
    assert bar.index('id="fwDefaultToggle"') < bar.index('id="fwFavToggle"'), "the shaka, then the star"
    assert ".fw-pair { display: none; align-items: center; }" in body
    phone = body[body.index("@media (max-width: 500px), (max-height: 500px) {"):]
    phone = phone[:phone.index("\n    }")]
    assert ".station-trigger .sr-star { display: none; }" in phone and ".fw-pair.on { display: inline-flex; }" in phone, "phones only"
    for needle in ("if (fwDflt) fwDflt.innerHTML = SHAKA;", "if (fwPair) fwPair.classList.add('on');",
                   "const served = new Set();", "const goneNames = new Map();", "let seen = new Map(), nextRank = 0;",
                   "seen.set(sid, { rank: nextRank++, p: p || null })", "seen = new Map(); nextRank = 0;",
                   "const ROLES = ['fav-select', 'sr-dflt', 'sr-fav', 'pt-ren', 'sr-rem'];", "function focusMark() {", "function focusBack(m) {",
                   "const m = focusMark(); refreshCurrent(); if (!results.hidden) renderFavs(); focusBack(m);",
                   "return writeFavs(f);", "localStorage.setItem(FAV_KEY, JSON.stringify(arr.slice(0, 100))); return true; } catch (e) { return false; }"):
        assert needle in picker, needle
    assert "rank: seen.size" not in picker and "acts.indexOf(a)" not in picker


def test_location_aids_on_the_page(client, monkeypatch):
    """Plan section 51: the two gear rows after the gridlines row ("Borders" for country and state lines together,
    "City names"; on by default; hidden when the module cannot run; owner, 2026-10-23), the module's script after the map
    tools' with the base from PLACES_BASE or the frames' neighbour, the label CSS in the module's font with the text centred
    on its place and no dot (plan section 52), the labels left out of print."""
    monkeypatch.delenv("PLACES_BASE", raising=False)
    body = client.get("/?station=51201").get_data(as_text=True)
    grid = body.index('id="gridlines"')
    rows = [body.index('id="%s" checked' % k) for k in ("borders", "places")]
    assert grid < rows[0] < rows[1] < body.index('</div>', rows[1])
    assert '<span class="form-check-label">Borders</span>' in body and '<span class="form-check-label">City names</span>' in body
    assert 'id="states"' not in body and "Country borders" not in body and "State/province" not in body
    tag = body.index('<script src="/ui/places.js?v=%s"></script>' % A.UI_ASSET_VERSION)
    assert body.index('<script src="/ui/tools.js?v=') < tag < body.index("Cloudflare Web Analytics")
    r = client.get("/ui/places.js?v=" + A.UI_ASSET_VERSION)
    assert r.status_code == 200 and "AllshorePlaces" in r.get_data(as_text=True)
    for needle in (
        "pl = window.AllshorePlaces.create(map, { base: \"\" || window.__allshorePlacesBase || '', storage: st || undefined,",
        "waitFor: function () { return window.__allshoreOverlayWait || null; } });",   # the overlay's first picture first (G34 B-6)
        # a wind station without a recent reading: a hollow grey ring, never a town's white dot (G34 B-3)
        ".wind-none .wind-ring { left: 15px; top: 15px; width: 10px; height: 10px; background: transparent; border: 2px solid #b8c1cb;",
        "if (!pl) { box.closest('.place-setting').hidden = true; return; }",
        "box.addEventListener('change', function () { pl.set(id, box.checked); });",
        'font: 600 11px/14px system-ui, -apple-system, "Segoe UI", sans-serif;',
        ".place-labels { display: none !important; }",
        "position: absolute; transform: translate(-50%, -50%); white-space: nowrap;",   # the name centred on its place
        "var ids = ['borders', 'places'], pl = null;",
        "try { st && st.removeItem('allshore.states.v1'); } catch (e) {}",
    ):
        assert needle in body, needle
    # no dot before a name and no left-side variant (section 52): neither in the page's CSS nor in the module
    for gone in (".place-label::before", "place-left", "padding-left: 6px"):
        assert gone not in body, gone
    # the module's measuring font = the page's label font
    js = open(os.path.join(os.path.dirname(__file__), "..", "static_ui", "places.js"), encoding="utf-8").read()
    assert "LABEL_FONT = '600 11px system-ui, -apple-system, \"Segoe UI\", sans-serif'" in js
    assert "DOT_R" not in js and "LABEL_DX" not in js and "place-left" not in js
    assert "createPane" not in body.split("<script src=\"/ui/places.js")[1].split("</script>", 2)[1]
    monkeypatch.setenv("PLACES_BASE", "https://example.test/places/v1/")
    body = client.get("/?station=51201").get_data(as_text=True)
    assert "base: \"https://example.test/places/v1\" || window.__allshorePlacesBase" in body
    # with the overlay on, the gated block derives the base beside the frames (the coast data's rule)
    tpl = open(os.path.join(os.path.dirname(__file__), "..", "templates", "index.html"), encoding="utf-8").read()
    gated = tpl[tpl.index("{%- if model_overlays %}"):]
    assert r"window.__allshorePlacesBase = BASE.replace(/\/gfswave\/0p25\/v1$/, '') + '/static/places/v2';" in gated


def test_buoy_links_on_the_page(client):
    """Plan section 52 B: the live window's "Forecast for this point" (first in its body) and the forecast window's
    "Live buoy" (after the phone pair in the toolbar), both hidden until a link is found; the forecast dots' touch
    tolerance on the map's canvas renderer; the live dots' per-dot touch pad; on phones the window opened or touched
    last on top; the link button left out of print. The behaviour is in tests/ui/buoylinks.test.js."""
    body = client.get("/?station=51201").get_data(as_text=True)
    lw = body.index('<div id="lwLinks" class="lw-links" hidden>')
    assert body.index('<div id="lwBody" class="fw-body">') < lw < body.index('id="liveBuoyLoading"')
    fw = body.index('<button type="button" id="fwLive" class="btn btn-sm btn-outline-primary" hidden>Live buoy</button>')
    assert body.index('id="fwPair"') < fw < body.index('id="viewBar"')
    for needle in (
        "preferCanvas: true,",
        "renderer: mapRenderer,", "const mapRenderer = new NearestCanvas({ tolerance: coarsePointer() ? COARSE_TOL : 0 });",
        "@media (any-pointer: coarse) { .live-buoy-dot::after { content: ''; position: absolute; inset: calc(-2px - var(--live-tap, 0px)); border-radius: 50%; } }",
        ".live-buoy-dot { position: relative; }",
        "#fwLive { display: none !important; }",
        "    // ---- buoy links (plan section 52; tests/ui/buoylinks.test.js runs this block) ----",
        "    // ---- end buoy links ----",
        "const LINK_KM = 1.0, LINK_ID_KM = 400, LINK_PLAIN_KM = 2.0;",
        "const LIVE_TAP_MAX = 8, LIVE_TWIN_UNDER = 3;",
        "    // ---- nearest path (tests/ui/nearest.test.js runs this block) ----",
        "@media (any-pointer: coarse) { #lwForecast, #fwLive { min-height: 44px; } }",
        "var RETIRED = {{ retired_stations|tojson }};".replace("{{ retired_stations|tojson }}", '["44039", "44040", "44043", "44058", "44061", "44062", "44063", "44064", "44072", "44087", "46088"]'),
    ):
        assert needle in body, needle
    assert body.index(".fwin:not(.fw-min) { z-index: 3500; }") < body.index(".fwin.fw-front:not(.fw-min) { z-index: 3510; }")
    # the block sits outside every template cut the page harnesses use (after the tide block, before the wind block)
    a = body.index("    // ---- buoy links (plan section 52")
    assert body.index("    map.on('zoomend', clampMapLatitude);") < a < body.index("    // ---------------- Wind stations (plan section 39)")


def test_station_window_refresh_buttons(client):
    """Plan section 56: a refresh button before minimise in the live-buoy, tide and wind windows (a status line for
    screen readers beside it), styled as the header buttons, turning while it runs (still under reduced motion),
    hidden in a chip / phone bar; each window's onRefresh reaches its own view; the live loaders take a refresh."""
    body = client.get("/?station=51201").get_data(as_text=True)
    for pre, label in (("lw", "live buoy"), ("tw", "tide station"), ("ww", "wind station")):
        btn = '<button type="button" id="%sRefresh" class="fw-btn fw-refresh" aria-label="Refresh %s" title="Refresh">' % (pre, label)
        assert btn in body, pre
        status = '<span id="%sRefreshStatus" class="visually-hidden" role="status" aria-live="polite"></span>' % pre
        assert body.index(status) < body.index(btn) < body.index('id="%sMin"' % pre) < body.index('id="%sClose"' % pre)
        assert body.index('id="%sHeader"' % pre) < body.index(btn)
    assert body.count('class="fw-refresh-icon"') == 3
    for css in ('.fwin.fw-min .fw-refresh { display: none; }',
                '.fw-refresh[aria-busy="true"] .fw-refresh-icon { animation: fw-spin .9s linear infinite; }',
                '@media (prefers-reduced-motion: reduce) { .fw-refresh[aria-busy="true"] .fw-refresh-icon { animation: none; opacity: .4; } }',
                '.fw-refresh.fw-refresh-failed {'):
        assert css in body, css
    assert "onRefresh: function () { return liveShownStation ? loadLiveBuoyDetails(liveShownStation, { refresh: true }) : null; } });" in body
    assert "onRefresh: function () { return tideView ? tideView.refreshNow() : null; } });" in body
    assert "onRefresh: function () { return windView ? windView.refreshNow() : null; } });" in body
    assert "onLoaded: function () { if (tideWin) tideWin.markLoaded(); }," in body
    assert "onLoaded: function () { if (windWin) windWin.markLoaded(); }," in body
    assert "return refresh ? { cache: 'reload', headers: { 'X-Allshore-Refresh': '1' } } : undefined;" in body
    assert "X-Allshore-Refresh" in body and A.REFRESH_HEADER == "X-Allshore-Refresh"   # the page and the server agree



def test_section_59_the_nwps_model_on_the_page(client):
    """The model bar shows GFS and NWPS at an NWPS station (the SWAN button hidden), NWPS off its stations resolves to
    GFS, the state carries the NWPS stations, the head's early request applies the same rule, the overlay names an NWPS
    table's run."""
    b = client.get("/?station=51003&model=NWPS").get_data(as_text=True)
    st = initial(b)
    assert st["model"] == "NWPS" and st["nwps_available"] is True and st["swan_available"] is False
    assert "51003" in st["nwps_stations"] and len(st["nwps_stations"]) == 112 and "51201" not in st["nwps_stations"]
    assert not re.search(r'id="modelBar" hidden', b)
    assert re.search(r'data-model="NWPS" aria-pressed="true"', b) and re.search(r'data-model="GFS" aria-pressed="false"', b)
    assert re.search(r'data-model="SWAN" aria-pressed="false" hidden', b)
    assert re.search(r'<option value="NWPS" selected>NWPS</option>', b) and '<option value="SWAN"' not in b
    b2 = client.get("/?station=51201&model=NWPS").get_data(as_text=True)
    st2 = initial(b2)
    assert st2["model"] == "GFS" and st2["nwps_available"] is False and st2["swan_available"] is True
    assert re.search(r'data-model="NWPS" aria-pressed="false" hidden', b2) and '<option value="NWPS"' not in b2
    b3 = client.get("/?station=46001").get_data(as_text=True)
    assert re.search(r'id="modelBar" hidden', b3) is None and initial(b3)["nwps_available"] is True     # 46001 is an NWPS station
    src = open(os.path.join(A.app.root_path, "templates", "index.html"), encoding="utf-8").read()
    assert "var nwps = {{ initial_state.nwps_stations|tojson }};" in src
    assert "if (/NWPS/.test(m[1])) return { model: 'NWPS' };" in src
    assert src.index("if (/NWPS/.test(m[1]))") < src.index("if (/SWAN/.test(m[1]))"), "NWPS before SWAN: NOAA NWPS is a SWAN model too"
    ov = open(os.path.join(A.app.root_path, "static_overlay", "overlay.js"), encoding="utf-8").read()
    assert "pc.model === 'NWPS' ? ' — the forecast table is a NOAA NWPS (nearshore) run'" in ov
