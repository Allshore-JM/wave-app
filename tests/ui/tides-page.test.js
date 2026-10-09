'use strict';
// The page's own tide-station block (templates/index.html, plan section 38), run in Node with stubs: the zoom gate and
// its legend note, the station list asked once (and again after a failure), the markers per world copy with the
// active one highlighted, the credit while the layer is on, a click that opens the window (or goes to a map tool),
// and the close that clears the graph and the highlight. The legend-note painter shared with the live list.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..', '..');
const TPL = fs.readFileSync(path.join(ROOT, 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');

function cut(from, to, inclusive) {
  const a = TPL.indexOf(from); if (a < 0) throw new Error('not in the template: ' + from);
  const b = TPL.indexOf(to, a); if (b < 0) throw new Error('not in the template: ' + to);
  return TPL.slice(a, inclusive ? b + to.length : b);
}
const NOTE = cut("    // The live list's state beside its legend entry, as TEXT.", "    map.on('overlayadd', saveLayerVisibility);");
const STATE = cut('    const TIDE_MIN_ZOOM = 9;', '    let tideView = null;\n', true);
const BLOCK = cut('    // ---------------- Tide stations (plan section 38) ----------------', '    // Wire map movement after all wrapped-marker state');

const flush = async (n) => { for (let i = 0; i < (n || 10); i++) await new Promise((r) => setImmediate(r)); };
const LIST = { fields: ['id', 'name', 'lat', 'lon', 'type', 'tz', 'obs'], stations: [
  ['1612340', 'Honolulu', 21.3033, -157.8645, 'R', 'Pacific/Honolulu', true],
  ['1611401', 'Waimea Bay', 21.9567, -159.673, 'S', 'Pacific/Honolulu', false],
  ['9999999', 'Nowhere', 'x', 1, 'R', 'UTC', false]] };

function boot(o = {}) {
  const notes = { live: { textContent: '' }, tide: { textContent: '' } };
  const byId = {};
  ['tideTitle', 'tideSubtitle', 'tideWin', 'tz'].forEach((id) => { byId[id] = { textContent: '', hidden: true, value: '' }; });
  const events = [];
  const document = {
    querySelector: (sel) => (sel.indexOf('[data-live-note]') >= 0 ? notes.live : sel.indexOf('[data-tide-note]') >= 0 ? notes.tide : null),
    getElementById: (id) => byId[id] || null,
    dispatchEvent: (e) => { events.push(e.type); return true; },
  };
  let zoom = o.zoom === undefined ? 9 : o.zoom, on = o.on !== false;
  const tideLayer = { items: [], clearLayers() { this.items = []; } };
  // Leaflet's attribution control COUNTS: each addAttribution needs its removeAttribution before the text goes
  const counts = new Map();
  const credits = { count: (t) => counts.get(t) || 0, has: (t) => (counts.get(t) || 0) > 0,
    get size() { let c = 0; counts.forEach((v) => { if (v > 0) c++; }); return c; } };
  // a flat stand-in for the projection: at zoom 9 one degree is pxPerDeg px, doubling per zoom (container = the same, no pan)
  const pxPerDeg = o.pxPerDeg || 1000, scale = () => pxPerDeg * Math.pow(2, zoom - 9);
  const container = { focused: 0, focus() { this.focused++; } };
  const map = { hasLayer: (l) => l === tideLayer && on, getZoom: () => zoom,
    latLngToContainerPoint: (ll) => ({ x: (ll[1] + 180) * scale(), y: (90 - ll[0]) * scale() }),
    project: (ll, z) => ({ x: (ll[1] + 180) * pxPerDeg * Math.pow(2, z - 9), y: (90 - ll[0]) * pxPerDeg * Math.pow(2, z - 9) }),   // Leaflet's: x from -180
    getSize: () => o.size || { x: 1e9, y: 1e9 }, getContainer: () => container,
    attributionControl: { addAttribution: (t) => { counts.set(t, credits.count(t) + 1); },
                          removeAttribution: (t) => { if (credits.count(t)) counts.set(t, credits.count(t) - 1); } } };
  const L = { marker(ll, opts) {
    const el = { keys: {}, addEventListener(t, fn) { this.keys[t] = fn; } };
    return { ll, opts, el, handlers: {}, addTo(layer) { layer.items.push(this); return this; }, getElement() { return el; },
      getLatLng() { return { lat: ll[0], lng: ll[1] }; }, fire(t, d) { if (this.handlers[t]) this.handlers[t](d); return this; },
      bindTooltip(c, tipOpts) { this.tip = c; this.tipOpts = tipOpts; return this; }, on(t, fn) { this.handlers[t] = fn; return this; } }; },
    divIcon: (opts) => ({ divIcon: opts }) };
  const fetchCalls = [];
  const answers = (o.answers || [{ ok: true, status: 200, json: () => Promise.resolve(o.list || LIST) }]).slice();
  const fetch = (u, init) => { fetchCalls.push(u); const a = answers.length ? answers.shift() : answers.at(-1);
    if (a instanceof Error) return Promise.reject(a); return Promise.resolve(a); };
  const measures = [];
  const loads = [], clears = [];
  const view = { load: (st, opts) => loads.push([st.id, opts]), clear: () => clears.push(1), resize() {}, show() {}, setUnit() {}, setZone() {} };
  const AllshoreTides = { createTideView: (deps) => { view.deps = deps; return view; } };
  const toolClicks = [];
  const window = { AllshoreTides, AllshoreTools: { active: () => !!o.toolActive, click: (ll, ev) => toolClicks.push(ll) }, Chart: undefined, sessionStorage: { getItem: () => null, setItem() {} } };
  const tideWin = { opens: 0, el: byId.tideWin, isOpen: () => !byId.tideWin.hidden, window: { mode: 'normal' },
    open() { this.opens++; byId.tideWin.hidden = false; }, close() { byId.tideWin.hidden = true; api.tideWindowClosed(); } };
  const visibleCopies = (stations) => stations.flatMap((s) => (o.copies || [0]).map((off) => ({ s, lng: s.lon + off, o: off })));
  const renderSignature = (vis) => vis.map((c) => c.s.id + '@' + c.lng).join(',');
  const saves = [];
  const code = STATE + NOTE + BLOCK +
    '\nreturn { get tideNoteText() { return tideNoteText; }, get tideStationsData() { return tideStationsData; }, get activeTideId() { return activeTideId; }, ' +
    'rebuildTideMarkers, loadTideStations, openTideStation, closeTideWindow, tideWindowClosed, tideZone, paintTideNote, paintLiveNote, TIDE_CREDIT, ' +
    'get tideView() { return tideView; }, setLive(t) { liveNoteText = t; } };';
  const layersControl = { _update: function () { notes.live.textContent = ''; notes.tide.textContent = ''; return this; } };
  const api = new Function('window', 'document', 'fetch', 'map', 'L', 'tideLayer', 'tideIcon', 'tideIconActive', 'visibleCopies', 'renderSignature', 'textTip',
    'saveMapView', 'saveLayerVisibility', 'measureTopRight', 'getSelectedUnit', 'tzAbbr', 'loadChartJs', 'CustomEvent', 'tideWin', 'layersControl', 'MAX_MAP_ZOOM', code)(
    window, document, fetch, map, L, tideLayer, 'ICON', 'ICON-ACTIVE', visibleCopies, renderSignature, (t) => ({ text: String(t) }),
    () => saves.push('view'), () => saves.push('layers'), () => measures.push(1), () => (o.unit || 'US'), (iso, tz) => tz + '!', () => Promise.resolve(),
    class { constructor(type) { this.type = type; } }, tideWin, layersControl, 11);
  api.container = container;
  return { api, notes, byId, events, tideLayer, credits, fetchCalls, loads, clears, view, toolClicks, saves, measures, layersControl, tideWin,
    setZoom: (z) => { zoom = z; }, setOn: (v) => { on = v; } };
}

test('below the gate: no request, no markers, the note; at the gate the list is asked once and the markers drawn', async () => {
  const b = boot({ zoom: 8.9 });
  b.api.rebuildTideMarkers(true);
  assert.equal(b.fetchCalls.length, 0); assert.equal(b.tideLayer.items.length, 0);
  assert.equal(b.notes.tide.textContent, 'zoom in to see tide stations');
  assert.equal(b.credits.has(b.api.TIDE_CREDIT), true, 'the credit while the layer is on');
  b.setZoom(9.0); b.api.rebuildTideMarkers(false);
  assert.deepEqual(b.fetchCalls, ['/api/tides/stations']);
  assert.equal(b.notes.tide.textContent, 'loading…');
  await flush();
  assert.equal(b.api.tideStationsData.length, 2, 'a row without numbers is dropped');
  assert.equal(b.tideLayer.items.length, 2); assert.equal(b.notes.tide.textContent, '');
  assert.equal(b.tideLayer.items[0].opts.icon, 'ICON'); assert.equal(b.tideLayer.items[0].opts.zIndexOffset, 400);
  assert.deepEqual(b.tideLayer.items[0].tip, { text: 'Tide station Honolulu' }, 'the name as text, never HTML');
  b.api.rebuildTideMarkers(false); b.api.rebuildTideMarkers(false);
  assert.equal(b.fetchCalls.length, 1, 'asked once');
  assert.equal(b.credits.count(b.api.TIDE_CREDIT), 1, 'the credit added once, however often the markers are rebuilt');
  b.setZoom(8.5); b.api.rebuildTideMarkers(false);
  assert.equal(b.tideLayer.items.length, 0, 'zoomed out: the layer is cleared (the gate bit of the signature)');
  assert.equal(b.notes.tide.textContent, 'zoom in to see tide stations');
  b.setZoom(10); b.api.rebuildTideMarkers(false);
  assert.equal(b.tideLayer.items.length, 2); assert.equal(b.fetchCalls.length, 1);
  b.setOn(false); b.api.rebuildTideMarkers(true);
  assert.equal(b.tideLayer.items.length, 0); assert.equal(b.notes.tide.textContent, ''); assert.equal(b.credits.size, 0, 'unticked: no note, no credit');
  b.api.rebuildTideMarkers(false); b.api.rebuildTideMarkers(true);
  assert.equal(b.credits.count(b.api.TIDE_CREDIT), 0, 'removed once, never below');
  b.setOn(true); b.api.rebuildTideMarkers(true); b.api.rebuildTideMarkers(false);
  assert.equal(b.credits.count(b.api.TIDE_CREDIT), 1, 'ticked again: back, once');
});

test('world copies get their own markers; a failed list is said and asked again', async () => {
  const b = boot({ copies: [-360, 0, 360], answers: [new Error('down'), { ok: false, status: 503, json: () => Promise.resolve({}) }, { ok: true, status: 200, json: () => Promise.resolve(LIST) }] });
  const realNow = Date.now; let t = realNow();
  Date.now = () => t;
  try {
    b.api.rebuildTideMarkers(true); await flush();
    assert.equal(b.notes.tide.textContent, 'unavailable'); assert.equal(b.tideLayer.items.length, 0);
    b.api.rebuildTideMarkers(false); await flush();
    assert.equal(b.fetchCalls.length, 1, 'not asked again on the next pan (G27 A-F11)');
    t += 30000;
    b.api.rebuildTideMarkers(false); await flush();
    assert.equal(b.fetchCalls.length, 2); assert.equal(b.notes.tide.textContent, 'unavailable');
    t += 30000;
    b.api.rebuildTideMarkers(false); await flush();
    assert.equal(b.fetchCalls.length, 3); assert.equal(b.tideLayer.items.length, 6, 'two stations in three world copies');
  } finally { Date.now = realNow; }
  assert.deepEqual(b.tideLayer.items.map((m) => m.ll[1]).slice(0, 3), [-157.8645 - 360, -157.8645, -157.8645 + 360]);
});

test('a click opens the window on that station in the chosen zone (else its own), highlights it; closing clears both', async () => {
  const b = boot();
  b.api.rebuildTideMarkers(true); await flush();
  const hnl = b.tideLayer.items[0];
  hnl.handlers.click({ latlng: { lat: 21.3, lng: -157.9 }, originalEvent: {} });
  assert.equal(b.api.activeTideId, '1612340');
  assert.equal(b.tideLayer.items[0].opts.icon, 'ICON-ACTIVE'); assert.equal(b.tideLayer.items[1].opts.icon, 'ICON', 'rebuilt with the highlight');
  assert.deepEqual(b.saves, ['view', 'layers']);
  assert.equal(b.byId.tideTitle.textContent, 'Honolulu'); assert.equal(b.byId.tideSubtitle.textContent, 'Tide station · NOAA 1612340');
  assert.equal(b.tideWin.opens, 1); assert.deepEqual(b.events, ['allshore:tidepanel']);
  assert.deepEqual(b.loads, [['1612340', { unit: 'US', zone: 'Pacific/Honolulu' }]]);
  assert.equal(b.api.tideView, b.view, 'the view is made once');
  assert.equal(b.view.deps.visible(), true); assert.equal(b.view.deps.zoneAbbr(0, 'UTC'), 'UTC!');
  assert.equal(b.view.deps.loadChartJs, undefined, 'no Chart.js: the strip is SVG');
  b.byId.tz.value = 'UTC';
  b.tideLayer.items[1].handlers.click({ latlng: {}, originalEvent: {} });
  assert.deepEqual(b.loads[1], ['1611401', { unit: 'US', zone: 'UTC' }], 'the site zone when chosen');
  assert.equal(b.api.activeTideId, '1611401');
  b.api.closeTideWindow();
  assert.equal(b.byId.tideWin.hidden, true); assert.equal(b.clears.length, 1); assert.equal(b.api.activeTideId, null);
  assert.ok(b.tideLayer.items.every((m) => m.opts.icon === 'ICON'), 'no highlight left');
  assert.equal(b.view.deps.visible(), false);
  b.api.tideWindowClosed();
  assert.equal(b.clears.length, 2, 'the window\'s own close (the x, Escape) clears too');
});

test('with a map tool active the click goes to the tool', async () => {
  const b = boot({ toolActive: true });
  b.api.rebuildTideMarkers(true); await flush();
  b.tideLayer.items[0].handlers.click({ latlng: { lat: 1, lng: 2 }, originalEvent: {} });
  assert.deepEqual(b.toolClicks, [{ lat: 1, lng: 2 }]); assert.equal(b.tideWin.opens, 0); assert.equal(b.api.activeTideId, null);
});

test('the legend painter keeps both notes through Leaflet\'s rebuilds and re-measures the corner', () => {
  const b = boot({ zoom: 5 });
  b.api.rebuildTideMarkers(true);
  b.api.setLive('cached list'); b.api.paintLiveNote();
  assert.equal(b.notes.live.textContent, 'cached list'); assert.equal(b.notes.tide.textContent, 'zoom in to see tide stations');
  b.layersControl._update();
  assert.equal(b.notes.live.textContent, 'cached list'); assert.equal(b.notes.tide.textContent, 'zoom in to see tide stations');
  assert.ok(b.measures.length >= 2);
  const n = b.measures.length;
  b.api.paintTideNote(); assert.equal(b.measures.length, n, 'nothing to paint: no re-measure');
});

test('tideZone: the site zone, else the station\'s, else UTC', () => {
  const b = boot();
  assert.equal(b.api.tideZone({ tz: 'Pacific/Fiji' }), 'Pacific/Fiji');
  assert.equal(b.api.tideZone({}), 'UTC');
  b.byId.tz.value = 'America/New_York';
  assert.equal(b.api.tideZone({ tz: 'Pacific/Fiji' }), 'America/New_York');
});

test('busy coasts: below the last zoom an icon overlapping a more important one is not drawn; the opened one always; all at the last zoom', async () => {
  // 1 px = 1/1000 degree: A (gauge, harmonic) and B (harmonic) 10 px apart, C (subordinate) 15 px from A, D far away, E 30 px off
  const list = { fields: ['id', 'name', 'lat', 'lon', 'type', 'tz', 'obs'], stations: [
    ['C', 'Sub', 20.015, -157, 'S', 'UTC', false], ['B', 'Harm', 20.010, -157, 'R', 'UTC', false],
    ['A', 'Gauge', 20.000, -157, 'R', 'UTC', true], ['D', 'Far', 21, -157, 'S', 'UTC', false], ['E', 'Edge', 20, -156.970, 'S', 'UTC', false]] };
  const b = boot({ zoom: 9.5, list });
  b.api.rebuildTideMarkers(true); await flush();
  const ids = () => b.tideLayer.items.map((m) => m.opts.title.replace('Tide station ', '')).sort();
  assert.deepEqual(ids(), ['Edge', 'Far', 'Gauge'], 'the gauge station keeps its place; 24 px or more apart all show');
  // at the map's last zoom every station shows
  b.setZoom(11); b.api.rebuildTideMarkers(true);
  assert.deepEqual(ids(), ['Edge', 'Far', 'Gauge', 'Harm', 'Sub']);
  // the station opened there still shows after a zoom-out, and hides its neighbours instead
  b.tideLayer.items.find((m) => /Sub/.test(m.opts.title)).handlers.click({ latlng: null, originalEvent: {} });
  assert.equal(b.api.activeTideId, 'C');
  b.setZoom(9.5); b.api.rebuildTideMarkers(true);
  assert.deepEqual(ids(), ['Edge', 'Far', 'Sub']);
  // the same answer whatever order the list comes in (the most important wins, ties by id)
  const c = boot({ zoom: 9.5, list: { fields: list.fields, stations: list.stations.slice().reverse() } });
  c.api.rebuildTideMarkers(true); await flush();
  assert.deepEqual(c.tideLayer.items.map((m) => m.opts.title.replace('Tide station ', '')).sort(), ['Edge', 'Far', 'Gauge']);
});

test('busy coasts: the order of importance (gauge, then harmonic, then id) and neighbours across a grid cell line', async () => {
  const list = { fields: ['id', 'name', 'lat', 'lon', 'type', 'tz', 'obs'], stations: [
    ['Y1', 'Harmonic no gauge', 25.000, -150, 'R', 'UTC', false], ['Z1', 'Subordinate gauge', 25.010, -150, 'S', 'UTC', true],
    ['P1', 'Sub', 26.000, -150, 'S', 'UTC', false], ['P2', 'Harm', 26.010, -150, 'R', 'UTC', false],
    ['Q2', 'Second', 27.000, -150, 'S', 'UTC', false], ['Q1', 'First', 27.010, -150, 'S', 'UTC', false],
    ['K1', 'West of the line', 30, -156.002, 'S', 'UTC', false], ['K2', 'East of the line', 30, -155.997, 'S', 'UTC', false]] };
  const b = boot({ zoom: 10, list });
  b.api.rebuildTideMarkers(true); await flush();
  assert.deepEqual(b.tideLayer.items.map((m) => m.opts.title.replace('Tide station ', '')).sort(),
    ['First', 'Harm', 'Subordinate gauge', 'West of the line'], 'a gauge first, then a harmonic station, then the lower id; 5 px apart across a cell line: one');
});

test('G27 B-P1-1: an icon shown at one zoom stays shown at every closer zoom; never two within 24 px; the opened one first', async () => {
  // 60 random stations in a 0.2 x 0.2 degree box (200 px at zoom 9): a busy coast
  let seed = 7; const rnd = () => { seed = (seed * 16807) % 2147483647; return seed / 2147483647; };
  const rows = [];
  for (let i = 0; i < 60; i++) rows.push(['S' + String(i).padStart(3, '0'), 'St ' + i, 25 + rnd() * 0.2, -81 + rnd() * 0.2, rnd() < 0.3 ? 'R' : 'S', 'UTC', rnd() < 0.15]);
  const b = boot({ zoom: 9, list: { fields: ['id', 'name', 'lat', 'lon', 'type', 'tz', 'obs'], stations: rows } });
  b.api.rebuildTideMarkers(true); await flush();
  const shownAt = {};
  for (const z of [9, 9.25, 9.5, 9.75, 10, 10.25, 10.5, 10.75, 10.99]) {
    b.setZoom(z); b.api.rebuildTideMarkers(true);
    shownAt[z] = new Set(b.tideLayer.items.map((m) => m.opts.title));
    const pts = b.tideLayer.items.map((m) => [m.ll[1] * 1000 * Math.pow(2, z - 9), -m.ll[0] * 1000 * Math.pow(2, z - 9)]);
    for (let i = 0; i < pts.length; i++) for (let j = i + 1; j < pts.length; j++) {
      assert.ok(Math.hypot(pts[i][0] - pts[j][0], pts[i][1] - pts[j][1]) >= 24 - 1e-6, 'zoom ' + z + ': two icons overlap');
    }
  }
  const zs = Object.keys(shownAt).map(Number).sort((a, c) => a - c);
  for (let k = 1; k < zs.length; k++) for (const t of shownAt[zs[k - 1]]) assert.ok(shownAt[zs[k]].has(t), t + ' vanished at ' + zs[k]);
  assert.ok(shownAt[9].size < 60 && shownAt[10.99].size > shownAt[9].size, 'more come with zooming in');
  b.setZoom(11); b.api.rebuildTideMarkers(true);
  assert.equal(b.tideLayer.items.length, 60, 'all at the last zoom');
  const hidden = rows.find((r) => !shownAt[9].has('Tide station ' + r[1]));
  b.tideLayer.items.find((m) => m.opts.title === 'Tide station ' + hidden[1]).handlers.click({ latlng: null, originalEvent: {} });
  b.setZoom(9); b.api.rebuildTideMarkers(true);
  assert.ok(b.tideLayer.items.some((m) => m.opts.title === 'Tide station ' + hidden[1]), 'the opened station at zoom 9');
});

test('G27 A-F7: at the last zoom an icon on top of another moves 12 px aside; stations across the date line are neighbours', async () => {
  const list = { fields: ['id', 'name', 'lat', 'lon', 'type', 'tz', 'obs'], stations: [
    ['A1', 'First', 30, -150, 'R', 'UTC', true], ['A2', 'Same place', 30, -150, 'S', 'UTC', false],
    ['A3', 'Near', 30.0007, -150, 'S', 'UTC', false], ['A4', 'Far', 31, -150, 'S', 'UTC', false]] };
  const b = boot({ zoom: 11, list });
  b.api.rebuildTideMarkers(true); await flush();
  const icon = (n) => b.tideLayer.items.find((m) => m.opts.title === 'Tide station ' + n).opts.icon;
  assert.equal(icon('First'), 'ICON'); assert.equal(icon('Far'), 'ICON');
  assert.deepEqual(icon('Same place').divIcon.iconAnchor, [9 - 12, 9], 'moved 12 px east');
  assert.ok(icon('Near').divIcon, 'within 9 px of one placed (2.8 px): moved too');
  b.setZoom(10.5); b.api.rebuildTideMarkers(true);
  assert.ok(b.tideLayer.items.every((m) => m.opts.icon === 'ICON'), 'no nudge below the last zoom');
  // Leaflet's world at zoom 9 is 131,072 px wide: the stub's degree scaled to it, so x wraps as on the real map
  const d = boot({ zoom: 9, pxPerDeg: 131072 / 360, list: { fields: list.fields, stations: [['D1', 'East of 180', -17, 179.99, 'R', 'UTC', true], ['D2', 'West of 180', -17, -179.995, 'S', 'UTC', false]] } });
  d.api.rebuildTideMarkers(true); await flush();
  assert.equal(d.tideLayer.items.length, 1, 'the two sides of the date line are 5.5 px apart: one shows at zoom 9');
});

test('G27 B-P2-1 / B-P3-1 / B-P3-3: Enter opens a focused marker; the legend says when stations are hidden; closing hands the focus to the map', async () => {
  const list = { fields: ['id', 'name', 'lat', 'lon', 'type', 'tz', 'obs'], stations: [
    ['1612340', 'Honolulu', 21.3, -157.86, 'R', 'Pacific/Honolulu', true], ['1612341', 'Next door', 21.305, -157.86, 'S', 'Pacific/Honolulu', false]] };
  const b = boot({ zoom: 9, list, size: { x: 1e9, y: 1e9 } });
  b.api.rebuildTideMarkers(true); await flush();
  assert.equal(b.tideLayer.items.length, 1);
  assert.equal(b.notes.tide.textContent, 'zoom in for more stations');
  let prevented = 0;
  b.tideLayer.items[0].el.keys.keydown({ key: 'Tab', preventDefault() { prevented++; } });
  assert.equal(b.loads.length, 0, 'other keys do nothing');
  b.tideLayer.items[0].el.keys.keydown({ key: 'Enter', preventDefault() { prevented++; } });
  assert.equal(prevented, 1); assert.equal(b.loads.length, 1); assert.equal(b.loads[0][0], '1612340');
  b.setZoom(11); b.api.rebuildTideMarkers(true);
  assert.equal(b.notes.tide.textContent, '', 'all shown: no note');
  b.api.closeTideWindow();
  assert.equal(b.api.container.focused, 1, 'the map takes the focus');
  const c = boot({ zoom: 9, list, size: { x: 10, y: 10 } });                   // the hidden one lies outside the view
  c.api.rebuildTideMarkers(true); await flush();
  assert.equal(c.notes.tide.textContent, '');
});

test('G27 B-P1-1 pin: a station waits only for neighbours shown before the two are clear', async () => {
  // A (gauge) at 0; P (harmonic) 10 px east of A waits until 10.26; X (subordinate) 20 px east of P (30 px from A) is
  // clear of P from 9.26, after P would show: X is not held back by P and shows at 9
  const list = { fields: ['id', 'name', 'lat', 'lon', 'type', 'tz', 'obs'], stations: [
    ['A', 'Gauge', 30, -150, 'R', 'UTC', true], ['P', 'Harmonic', 30, -149.99, 'R', 'UTC', false], ['X', 'Sub', 30, -149.97, 'S', 'UTC', false]] };
  const b = boot({ zoom: 9, list });
  b.api.rebuildTideMarkers(true); await flush();
  assert.deepEqual(b.tideLayer.items.map((m) => m.opts.title.replace('Tide station ', '')).sort(), ['Gauge', 'Sub']);
});
