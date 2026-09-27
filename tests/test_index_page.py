"""The page after the forecast window restructure (plan section 25): the top bar, the window markup,
the state handed to static_ui/forecast.js, the no-JS path. The exact bytes are pinned by the golden in
tests/test_overlay_flag.py; these tests say WHY the page looks the way it does."""
import json
import re

import pytest

import app as A
import capture_index_golden as G


@pytest.fixture
def client(monkeypatch):
    monkeypatch.delenv("MODEL_OVERLAYS", raising=False)
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
    b2 = client.get("/?station=46001&model=SWAN").get_data(as_text=True)
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
    assert "function measureTopRight()" in body and "classList.toggle('settings-open', !panel.hidden)" in body
    phone = body[body.index("@media (max-width: 500px), (max-height: 500px) {"):]
    sheet = re.search(r"\.fwin:not\(\.fw-min\) \{ z-index: (\d+); \}", phone).group(1)
    assert int(sheet) > int(corner) > 2100 and int(corner) == 2600
    assert ".live-win.fw-min { bottom: var(--fw-bar-h, 48px) !important; }" in phone
    assert "html:not(.js) .settings-panel[hidden] { display: flex !important; }" in body
    assert "html:not(.js) #pageHead { display: flex;" in body and ".js #pageHead { display: none; }" in body
    assert "html:not(.js) #forecastTable[hidden] { display: block !important; }" in body
    assert "html:not(.js) .forecast-win { position: static !important; width: auto !important; max-width: none;" in body
    assert "html:not(.js) .live-win { display: none !important; }" in body
    assert "view=Table&amp;render=full" in body and "view=Graph&amp;render=full" not in body
    assert ".fwin[hidden] { display: none !important; }" in body
    assert "#forecastTable[hidden] { display: block !important; } #graphs { display: none !important; }" in body[body.index("@media print"):]
    assert "#map, #pageHead, .fw-btn, .fw-resize, .fw-edge, .live-win, .sr-star, .station-caret { display: none !important; }" in body[body.index("@media print"):]
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
    assert "new CustomEvent('allshore:livepanel')" in body and "document.addEventListener('allshore:livepanel', function () { front(false); });" in body

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
    assert "const reserve = phone ? phoneBarH + liveBar : 0;" in body and "if (lastEnforcedWidth === w) {" in body
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
        "liveWin = window.AllshoreForecast.createLiveWindow({ onClose: function () { liveDetailSeq++; } });",
        "if (fwEl && fwEl.classList.contains('fw-min') && fwEl.offsetHeight > 0) phoneBarH = fwEl.offsetHeight;",
        "document.documentElement.style.setProperty('--fw-bar-h', phoneBarH + 'px');",
        "function open() { renderFavs(); place(); results.hidden = false;",
        "if (below >= 220 || below >= above) { s.top = (r.bottom + 2) + 'px'; s.bottom = 'auto';",
        "if (window.innerWidth <= 500 || window.innerHeight <= 500) enforceSingleWorld();",
        ".fwin.live-win { right: 18px; bottom: 18px; width: min(820px, calc(100vw - 36px)); height: min(76vh, 720px); z-index: 2000; }",
        "new MutationObserver(sync).observe(panel, { attributes: true, attributeFilter: ['hidden'] });",
        ".fw-maxbtn { display: none; }",
        "options: { position: 'topleft' },",
        ".fwin.fw-min { height: auto !important; min-height: 0; min-width: 0; }",
        ".live-chip .forecast-win:not(.fw-min):not(.fw-max) { bottom: 70px; }",
        "document.body.classList.toggle('live-chip', !!(d.open && d.mode === 'min'));",
        "onMode: function () { if (window.innerWidth <= 500 || window.innerHeight <= 500) enforceSingleWorld(); },",
        ".home-menu { bottom: 62px; }",
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
        assert body.count('<div class="fw-edge fw-edge-%s" data-edge="%s" aria-hidden="true"></div>' % (k, k)) == 2, k
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
