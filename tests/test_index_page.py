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
    for needle in ('id="topBar"', 'class="brand"', 'id="stationTrigger"', 'id="settingsBtn"', 'id="settingsPanel"',
                   'id="tz" name="tz"', 'id="unit" name="unit"', 'id="forecastWin" class="forecast-win fw-min"',
                   'id="fwHeader"', 'id="fwMin"', 'id="fwMax"', 'id="viewBar"', 'id="modelBar"', 'id="rangeBar"',
                   'id="fwBody"', 'id="fwError"', 'id="forecastMeta"', 'id="forecastTable"', 'id="forecastLoading"',
                   'id="graphs"', 'id="heightChart"', 'id="periodChart"', 'id="directionChart"', 'id="fwResize"',
                   '/ui/forecast.js?v=%s' % A.UI_ASSET_VERSION, "window.AllshoreForecast.init(", "document.documentElement.classList.add('js')"):
        assert needle in body, needle
    for gone in ('id="updateBtn"', 'id="model" name="model"', 'id="view" name="view"', "sticky-top-bar", "nav-hidden",
                 '<script src="https://cdn.jsdelivr.net/npm/chart.js', "allshore.focusStation", "controlForm').submit()"):
        assert gone not in body, gone
    # the settings live in the gear's panel, hidden until opened; the no-JS Go button only inside <noscript>
    assert re.search(r'id="settingsPanel"[^>]*\bhidden\b', body)
    assert re.search(r"<noscript><button type=\"submit\"", body)


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
    assert re.search(r"function handleMapMoveEnd\(\) \{[^}]*saveMapView\(map\)", body, re.S)
    assert "new CustomEvent('allshore:station', { detail: { sid: String(s.id), source: 'map' } })" in body
    assert "new CustomEvent('allshore:station', { detail: { sid: sid, source: 'picker' } })" in body
    assert "if (d.source === 'picker' && d.sid) focusStation(String(d.sid));" in body
    assert "addEventListener('submit', (e) => { e.preventDefault(); })" in body
    assert "select.value = String(d.sid); refreshCurrent();" in body


def test_phone_sheets_sit_above_the_top_bar_and_the_no_js_page_shows_its_controls(client):
    """G16-A P0-1 / P2-5 / P3-1 / P3-2 as CSS pins: the full-screen sheets outrank the top bar in phone mode; the
    no-JS layout shows the settings and the table whatever the [hidden] attributes say; the refresh is to the Table view."""
    body = client.get("/?station=51201&view=Graph").get_data(as_text=True)
    bar = re.search(r"\.top-bar \{[^}]*z-index: (\d+)", body).group(1)
    phone = body[body.index("@media (max-width: 500px), (max-height: 500px) {"):]
    sheet = re.search(r"\.forecast-win:not\(\.fw-min\) \{ z-index: (\d+); \}", phone).group(1)
    assert int(sheet) > int(bar) == 3000
    assert "html:not(.js) .settings-panel[hidden] { display: flex !important; }" in body
    assert "html:not(.js) #forecastTable[hidden] { display: block !important; }" in body
    assert "html:not(.js) .forecast-win { position: static !important; width: auto !important; max-width: none;" in body
    assert "view=Table&amp;render=full" in body and "view=Graph&amp;render=full" not in body
    live = re.search(r"@media \(max-width: 500px\) \{\s*#liveBuoyPanel \{\s*z-index: (\d+);", body).group(1)
    assert int(live) > int(bar), "the full-screen live panel outranks the bar (only where it is full screen)"
    assert "#forecastTable[hidden] { display: block !important; } #graphs { display: none !important; }" in body[body.index("@media print"):]
