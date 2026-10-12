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
// the live markers' rebuild (it shares the key, Tab-stop and refocus helpers of the tide block)
const LIVE = cut('    function liveTabStops() {', '    // ---------------- Tide stations (plan section 38) ----------------');

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
    activeElement: null, body: { tag: 'body' },
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
  const liveLayer = { items: [], clearLayers() { this.items = []; } };
  const map = { hasLayer: (l) => (l === tideLayer && on) || (l === liveLayer && o.liveOn !== false), getZoom: () => zoom,
    latLngToContainerPoint: (ll) => ({ x: (ll[1] + 180) * scale(), y: (90 - ll[0]) * scale() }),
    project: (ll, z) => ({ x: (ll[1] + 180) * pxPerDeg * Math.pow(2, z - 9), y: (90 - ll[0]) * pxPerDeg * Math.pow(2, z - 9) }),   // Leaflet's: x from -180
    getSize: () => o.size || { x: 1e9, y: 1e9 }, getContainer: () => container,
    attributionControl: { addAttribution: (t) => { counts.set(t, credits.count(t) + 1); },
                          removeAttribution: (t) => { if (credits.count(t)) counts.set(t, credits.count(t) - 1); } } };
  // an element: its listeners, attributes, inline style and the focus (document.activeElement)
  const mkEl = () => ({ keys: {}, bound: {}, attrs: {}, style: { props: {}, setProperty(k, v) { this.props[k] = v; } },
    addEventListener(t, fn) { this.keys[t] = fn; this.bound[t] = (this.bound[t] || 0) + 1; }, setAttribute(k, v) { this.attrs[k] = String(v); },
    getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }, focus() { document.activeElement = this; } });
  byId.twHeader = Object.assign(mkEl(), { closest: () => byId.tideWin });
  byId.tideWin.contains = (x) => x === byId.twHeader;                       // the header is in the window
  const L = { marker(ll, opts) {
    const el = mkEl();
    return { ll, opts, el, handlers: {}, addTo(layer) { layer.items.push(this); return this; }, getElement() { return this.el; },
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
  const window = { AllshoreTides: o.noTides ? null : AllshoreTides, AllshoreTools: { active: () => !!o.toolActive, click: (ll, ev) => toolClicks.push(ll) }, Chart: undefined, sessionStorage: { getItem: () => null, setItem() {} } };
  const tideWin = { opens: 0, el: byId.tideWin, isOpen: () => !byId.tideWin.hidden, window: { mode: 'normal' },
    open() { this.opens++; byId.tideWin.hidden = false; }, close() { byId.tideWin.hidden = true; api.tideWindowClosed(); } };
  const visibleCopies = (stations) => stations.flatMap((s) => (o.copies || [0]).map((off) => ({ s, lng: s.lon + off, o: off })));
  const renderSignature = (vis) => vis.map((c) => c.s.id + '@' + c.lng).join(',');
  const saves = [];
  const liveOpened = [];
  const code = 'let liveStationsData = [], lastLiveSig = null, liveDrawn = [], activeLiveBuoyId = null;\n' + STATE + NOTE + LIVE + BLOCK +
    '\nreturn { get tideNoteText() { return tideNoteText; }, get tideStationsData() { return tideStationsData; }, get activeTideId() { return activeTideId; }, ' +
    'rebuildTideMarkers, loadTideStations, openTideStation, closeTideWindow, tideWindowClosed, tideZone, paintTideNote, paintLiveNote, TIDE_CREDIT, ' +
    'get tideView() { return tideView; }, setLive(t) { liveNoteText = t; }, rebuildLiveBuoyMarkers, set liveData(d) { liveStationsData = d; } };';
  const layersControl = { _update: function () { notes.live.textContent = ''; notes.tide.textContent = ''; return this; } };
  const windDrawn = o.windDrawn || [];                                  // the wind block's drawn flags ({s: {lat}, lng}): a tide icon on one keeps no pad
  const api = new Function('window', 'document', 'fetch', 'map', 'L', 'tideLayer', 'tideIcon', 'tideIconActive', 'visibleCopies', 'renderSignature', 'textTip',
    'saveMapView', 'saveLayerVisibility', 'measureTopRight', 'getSelectedUnit', 'tzAbbr', 'loadChartJs', 'CustomEvent', 'tideWin', 'layersControl', 'MAX_MAP_ZOOM',
    'liveBuoyLayer', 'liveBuoyIconActive', 'liveBuoyIconInactive', 'buoyDisplayLabel', 'loadLiveBuoyDetails', 'windDrawn', code)(
    window, document, fetch, map, L, tideLayer, 'ICON', 'ICON-ACTIVE', visibleCopies, renderSignature, (t) => ({ text: String(t) }),
    () => saves.push('view'), () => saves.push('layers'), () => measures.push(1), () => (o.unit || 'US'), (iso, tz) => tz + '!', () => Promise.resolve(),
    class { constructor(type) { this.type = type; } }, tideWin, layersControl, 11,
    liveLayer, 'LIVE-ACTIVE', 'LIVE', (st) => st.id, (st) => liveOpened.push(st.id), windDrawn);
  api.container = container;
  return { api, notes, byId, events, tideLayer, credits, fetchCalls, loads, clears, view, toolClicks, saves, measures, layersControl, tideWin,
    document, mkEl, liveLayer, liveOpened, setZoom: (z) => { zoom = z; }, setOn: (v) => { on = v; } };
}

test('below the gate: no request, no markers, no zoom hint (owner, 2026-10-10); at the gate the list is asked once and the markers drawn', async () => {
  const b = boot({ zoom: 8.9 });
  b.api.rebuildTideMarkers(true);
  assert.equal(b.fetchCalls.length, 0); assert.equal(b.tideLayer.items.length, 0);
  assert.equal(b.notes.tide.textContent, '');
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
  assert.equal(b.notes.tide.textContent, '', 'zoomed out: no zoom hint');
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
  const b = boot({ zoom: 9 });
  b.api.rebuildTideMarkers(true);                                             // the list asked, not answered yet: "loading…"
  b.api.setLive('cached list'); b.api.paintLiveNote();
  assert.equal(b.notes.live.textContent, 'cached list'); assert.equal(b.notes.tide.textContent, 'loading…');
  b.layersControl._update();
  assert.equal(b.notes.live.textContent, 'cached list'); assert.equal(b.notes.tide.textContent, 'loading…');
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
  // the station opened there still shows after a zoom-out, on top of the others, and nothing else changes (G27
  // re-check RC-15: no icon elsewhere comes or goes when a station is opened or closed)
  b.tideLayer.items.find((m) => /Sub/.test(m.opts.title)).handlers.click({ latlng: null, originalEvent: {} });
  assert.equal(b.api.activeTideId, 'C');
  b.setZoom(9.5); b.api.rebuildTideMarkers(true);
  assert.deepEqual(ids(), ['Edge', 'Far', 'Gauge', 'Sub']);
  assert.equal(b.tideLayer.items.find((m) => /Sub/.test(m.opts.title)).opts.zIndexOffset, 450, 'the opened one on top');
  assert.equal(b.tideLayer.items.find((m) => /Gauge/.test(m.opts.title)).opts.zIndexOffset, 400);
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

test('G27 B-P2-1 / B-P3-3: Enter opens a focused marker; no note for the stations the thinning holds back (owner, 2026-10-10); closing hands the focus to the map', async () => {
  const list = { fields: ['id', 'name', 'lat', 'lon', 'type', 'tz', 'obs'], stations: [
    ['1612340', 'Honolulu', 21.3, -157.86, 'R', 'Pacific/Honolulu', true], ['1612341', 'Next door', 21.305, -157.86, 'S', 'Pacific/Honolulu', false]] };
  const b = boot({ zoom: 9, list, size: { x: 1e9, y: 1e9 } });
  b.api.rebuildTideMarkers(true); await flush();
  assert.equal(b.tideLayer.items.length, 1);
  assert.equal(b.notes.tide.textContent, '', 'one station in view held back: no zoom hint (the G27 B-P3-1 note is gone)');
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

// ---------------------------------------------------------------------------------------------- G27 re-check (fix round 2)

const FIELDS = ['id', 'name', 'lat', 'lon', 'type', 'tz', 'obs'];
const byTitle = (b, n) => b.tideLayer.items.find((m) => m.opts.title === 'Tide station ' + n);

test('G27 re-check RC-1: a finger\'s target is cut to half the gap to the nearest icon (no target covers another icon\'s centre); updated at every zoom', async () => {
  // 1 px = 1/1000 degree at zoom 9 (the stub): a lone station, a pair 30 px apart (east), a diagonal pair 17 px x 17 px
  const list = { fields: FIELDS, stations: [
    ['L', 'Lone', 40, -150, 'R', 'UTC', true],
    ['P1', 'Pair west', 30, -150, 'R', 'UTC', true], ['P2', 'Pair east', 30, -149.970, 'R', 'UTC', true],
    ['D1', 'Diag one', 20, -150, 'R', 'UTC', true], ['D2', 'Diag two', 19.983, -149.983, 'R', 'UTC', true]] };
  const b = boot({ zoom: 9, list });
  b.api.rebuildTideMarkers(true); await flush();
  const pad = (n) => byTitle(b, n).el.style.props['--tide-tap'];
  assert.equal(pad('Lone'), '10px');
  assert.equal(pad('Pair west'), '6px'); assert.equal(pad('Pair east'), '6px');      // 30 px apart: 15 - 9
  assert.equal(pad('Diag one'), '0px', 'a diagonal neighbour 24 px away: 17 px in x and y, inside a 38-px target');
  for (const [a, c] of [['Pair west', 'Pair east'], ['Diag one', 'Diag two']]) {
    const A = byTitle(b, a), C = byTitle(b, c);
    const ha = 9 + parseInt(pad(a), 10), cx = (C.ll[1] - A.ll[1]) * 1000, cy = (A.ll[0] - C.ll[0]) * 1000;
    assert.ok(Math.max(Math.abs(cx), Math.abs(cy)) > ha, a + '\'s target stops short of ' + c + '\'s centre');
  }
  b.setZoom(10); b.api.rebuildTideMarkers(false);                          // the same icons, twice as far apart
  assert.equal(pad('Pair west'), '10px'); assert.equal(pad('Diag one'), '8px');
});

test('step 5 F3: a tide icon with a wind flag on the same spot keeps only its own 18 px on touch screens (pad 0); the others their pads', async () => {
  const list = { fields: FIELDS, stations: [['L', 'Lone', 40, -150, 'R', 'UTC', true], ['G', 'Gauge with wind', 30, -150, 'R', 'UTC', true]] };
  const b = boot({ zoom: 9, list, windDrawn: [{ s: { lat: 30 }, lng: -150.001 }, { s: { lat: 40 }, lng: -149.970 }] });   // 1 px east of the gauge: the same spot; 30 px east of Lone: not
  b.api.rebuildTideMarkers(true); await flush();
  const pad = (n) => byTitle(b, n).el.style.props['--tide-tap'];
  assert.equal(pad('Lone'), '10px'); assert.equal(pad('Gauge with wind'), '0px');
});

test('G27 re-check RC-4: the nudge tests the square boxes; beside the opened (larger) icon too; no icon lands on a third; by importance; tooltips follow', async () => {
  // at zoom 11 the stub's 1/1000 degree is 4 px: G and S 6.4 px apart in x and y (9 px: outside a 9-px circle, inside the box)
  const list = { fields: FIELDS, stations: [
    ['S', 'Sub', 30.0016, -150 + 0.0016, 'S', 'UTC', false], ['G', 'Gauge', 30, -150, 'R', 'UTC', true],
    ['T1', 'Three a', 25, -150, 'R', 'UTC', true], ['T2', 'Three b', 25, -150, 'S', 'UTC', false], ['T3', 'Three c', 25, -150, 'S', 'UTC', false],
    ['F1', 'Five a', 20, -150, 'R', 'UTC', true], ['F2', 'Five b', 20, -150, 'S', 'UTC', false], ['F3', 'Five c', 20, -150, 'S', 'UTC', false],
    ['F4', 'Five d', 20, -150, 'S', 'UTC', false], ['F5', 'Five e', 20, -150, 'S', 'UTC', false], ['F6', 'Five f', 20, -150, 'S', 'UTC', false],
    ['N1', 'Near open', 35, -150, 'S', 'UTC', false], ['N2', 'Opened', 35, -149.99725, 'S', 'UTC', false]] };
  const b = boot({ zoom: 11, list });
  b.api.rebuildTideMarkers(true); await flush();
  const icon = (n) => byTitle(b, n).opts.icon;
  assert.equal(icon('Gauge'), 'ICON', 'the more important one stays');
  assert.deepEqual(icon('Sub').divIcon.iconAnchor, [9 - 12, 9], 'its centre lies in the gauge\'s box: moved');
  assert.deepEqual(icon('Sub').divIcon.tooltipAnchor, [12, 0], 'the tooltip follows the icon');
  const spots = ['Three a', 'Three b', 'Three c'].map((n) => (icon(n) === 'ICON' ? '0,0' : icon(n).divIcon.tooltipAnchor.join(',')));
  assert.equal(new Set(spots).size, 3, 'three at one spot: three places (a nudged spot is taken)');
  const five = ['Five a', 'Five b', 'Five c', 'Five d', 'Five e', 'Five f'].map((n) => (icon(n) === 'ICON' ? '0,0' : icon(n).divIcon.tooltipAnchor.join(',')));
  assert.equal(new Set(five).size, 6, 'six at one spot'); assert.ok(five.includes('12,12') || five.includes('-12,12'), 'the corners when the sides are taken');
  // the reverse list order gives the same nudges (importance decides, not the list)
  const r = boot({ zoom: 11, list: { fields: FIELDS, stations: list.stations.slice().reverse() } });
  r.api.rebuildTideMarkers(true); await flush();
  assert.equal(byTitle(r, 'Gauge').opts.icon, 'ICON');
  // the opened icon is 22 px: a neighbour 11 px away lies inside its box, so the opened one (placed at its own rank)
  // finds room beside it; the neighbour stays (fresh check F-2)
  assert.equal(icon('Near open'), 'ICON'); assert.equal(icon('Opened'), 'ICON');
  byTitle(b, 'Opened').handlers.click({ latlng: null, originalEvent: {} });
  assert.equal(icon('Near open'), 'ICON', 'opening moves no other icon');
  assert.ok(icon('Opened').divIcon && /tide-icon-active/.test(icon('Opened').divIcon.className), 'the opened one moved aside');
});

test('G27 re-check RC-6 / T02: the reveal grid meets across the date line; stations at one spot wait for the last zoom', async () => {
  // Leaflet's world at zoom 9 is 131,072 px (the stub's degree scaled to it): x 131,061 and x 1.8 are 12.8 px apart
  const d = boot({ zoom: 9, pxPerDeg: 131072 / 360, list: { fields: FIELDS, stations: [
    ['D1', 'East of 180', -17, 179.97, 'R', 'UTC', true], ['D2', 'West of 180', -17, -179.995, 'S', 'UTC', false]] } });
  d.api.rebuildTideMarkers(true); await flush();
  assert.equal(d.tideLayer.items.length, 1, 'the cells at x 131,040 and x 0 are neighbours');
  const s = boot({ zoom: 10.99, list: { fields: FIELDS, stations: [['A', 'One', 30, -150, 'R', 'UTC', true], ['B', 'Two', 30, -150, 'S', 'UTC', false]] } });
  s.api.rebuildTideMarkers(true); await flush();
  assert.equal(s.tideLayer.items.length, 1, 'one spot: one icon below the last zoom');
  s.setZoom(11); s.api.rebuildTideMarkers(true);
  assert.equal(s.tideLayer.items.length, 2);
});

test('G27 re-check RC-3 / RC-5: Tab stops only in view; the focus survives a rebuild; a key open focuses the window; a re-added element gets the key', async () => {
  const size = { x: 300, y: 1e9 };                                          // the view: x 0 .. 300 (the stub: lon -180 at x 0)
  const list = { fields: FIELDS, stations: [['IN', 'Inside', 30, -179.85, 'R', 'UTC', true], ['OUT', 'Outside', 30, -179.5, 'R', 'UTC', true]] };
  const b = boot({ zoom: 9, list, size });
  b.api.rebuildTideMarkers(true); await flush();
  assert.equal(byTitle(b, 'Inside').el.getAttribute('tabindex'), '0');
  assert.equal(byTitle(b, 'Outside').el.getAttribute('tabindex'), '-1', 'a marker in the padding is no Tab stop');
  assert.equal(byTitle(b, 'Inside').opts.autoPanOnFocus, false, 'a focused marker never pans the map');
  size.x = 600;                                                              // the view moves: the same icons
  b.api.rebuildTideMarkers(false);
  assert.equal(byTitle(b, 'Outside').el.getAttribute('tabindex'), '0', 'Tab stops follow the view without a rebuild');
  byTitle(b, 'Inside').el.focus();
  const old = byTitle(b, 'Inside').el;
  b.api.rebuildTideMarkers(true);
  assert.notEqual(byTitle(b, 'Inside').el, old); assert.equal(b.document.activeElement, byTitle(b, 'Inside').el, 'the focus on the new element');
  // Enter opens the station and the window's header takes the focus; with a map tool active the focus stays
  byTitle(b, 'Inside').el.keys.keydown({ key: 'Enter', preventDefault() {} });
  assert.equal(b.loads.length, 1); assert.equal(b.document.activeElement, b.byId.twHeader);
  const t = boot({ zoom: 9, list, size, toolActive: true });
  t.api.rebuildTideMarkers(true); await flush();
  byTitle(t, 'Inside').el.focus();
  byTitle(t, 'Inside').el.keys.keydown({ key: ' ', preventDefault() {} });
  assert.equal(t.toolClicks.length, 1); assert.equal(t.document.activeElement, byTitle(t, 'Inside').el, 'a tool took the press');
  // Leaflet makes a new element when a marker is added again (its layer unticked and ticked): the key comes with it
  const m = byTitle(b, 'Outside'), fresh = b.mkEl();
  m.el = fresh; m.handlers.add();
  assert.equal(typeof fresh.keys.keydown, 'function'); assert.equal(fresh.getAttribute('data-mk'), 't:OUT#0');
});

test('G27 re-check RC-7 / T11 / T23: the list\'s note during its back-off; the map takes the focus only from the window; a drawn station is not "hidden"', async () => {
  const b = boot({ zoom: 9.5, answers: [new Error('down')] });
  const realNow = Date.now; let t = realNow(); Date.now = () => t;
  try {
    b.api.rebuildTideMarkers(true); await flush();
    assert.equal(b.notes.tide.textContent, 'unavailable');
    b.setZoom(8); b.api.rebuildTideMarkers(false);
    assert.equal(b.notes.tide.textContent, '', 'zoomed out: no note (no zoom hint, owner 2026-10-10)');
    t += 5000; b.setZoom(9.5); b.api.rebuildTideMarkers(false);
    assert.equal(b.notes.tide.textContent, 'unavailable', 'zoomed in again, still within the back-off: the list\'s state');
  } finally { Date.now = realNow; }
  const c = boot({ zoom: 9 });
  c.api.rebuildTideMarkers(true); await flush();
  c.tideLayer.items[0].handlers.click({ latlng: null, originalEvent: {} });
  const elsewhere = c.mkEl(); c.document.activeElement = elsewhere;        // the gear, a field: not in the window
  c.api.closeTideWindow();
  assert.equal(c.api.container.focused, 0, 'the focus stays where the visitor put it');
  // a station held back by the thinning inside the view, and below the gate: never a zoom hint (owner, 2026-10-10)
  const list = { fields: FIELDS, stations: [['A', 'In view', 30, -179.9, 'R', 'UTC', true], ['B', 'Beside it', 30, -179.895, 'S', 'UTC', false],
    ['C', 'Lone', 30, -179.5, 'R', 'UTC', true]] };
  const d = boot({ zoom: 9, list, size: { x: 600, y: 1e9 } });               // B (held back) at x 105, inside the view
  d.api.rebuildTideMarkers(true); await flush();
  assert.equal(d.tideLayer.items.length, 2);
  assert.equal(d.notes.tide.textContent, '');
  for (const z of [8.9, 5, 2, 9, 10.5, 11]) {
    d.setZoom(z); d.api.rebuildTideMarkers(false);
    assert.equal(d.notes.tide.textContent, '', 'no note at zoom ' + z);
  }
});

test('G27 re-check RC-4: when every spot 12 px away is taken, an icon moves 24 px (ten stations at one spot: ten places)', async () => {
  const stations = [['R0', 'Ring 0', 10, -150, 'R', 'UTC', true]];
  for (let i = 1; i < 10; i++) stations.push(['R' + i, 'Ring ' + i, 10, -150, 'S', 'UTC', false]);
  const b = boot({ zoom: 11, list: { fields: FIELDS, stations } });
  b.api.rebuildTideMarkers(true); await flush();
  const at = b.tideLayer.items.map((m) => (m.opts.icon === 'ICON' ? '0,0' : m.opts.icon.divIcon.tooltipAnchor.join(',')));
  assert.equal(new Set(at).size, 10, 'ten places');
  assert.ok(at.includes('24,0'), 'the tenth 24 px east');
});

test('fix round 2 pin (N26): a key press a map tool takes leaves the focus on the marker, also with a tide window open', async () => {
  const o = { zoom: 9, list: { fields: FIELDS, stations: [['A', 'One', 30, -179.9, 'R', 'UTC', true], ['B', 'Two', 30, -179.5, 'R', 'UTC', true]] }, size: { x: 1e9, y: 1e9 } };
  const b = boot(o);
  b.api.rebuildTideMarkers(true); await flush();
  byTitle(b, 'One').el.keys.keydown({ key: 'Enter', preventDefault() {} });
  assert.equal(b.document.activeElement, b.byId.twHeader, 'the window opened and took the focus');
  o.toolActive = true;                                                       // a map tool now: the press goes to it
  byTitle(b, 'Two').el.focus();
  byTitle(b, 'Two').el.keys.keydown({ key: ' ', preventDefault() {} });
  assert.equal(b.toolClicks.length, 1); assert.equal(b.document.activeElement, byTitle(b, 'Two').el);
});

// ---------------------------------------------------------------------------------------------- fresh check (fix round 3)

const offOf = (m) => (typeof m.opts.icon === 'string' ? null : m.opts.icon.divIcon.tooltipAnchor.join(','));

test('fresh check F-1: a zoom that keeps the drawn icons but changes a nudge redraws them', async () => {
  // at zoom 11 (4 px per 1/1000 degree) G and O are 12.4 px apart: room even for the opened 22-px icon; O is shown only at 11
  // (it waits for G until 9 + log2(24 / 3) = 12) or opened. Opened, it stays drawn at 10.5, where the two are 8.8 px apart
  const list = { fields: FIELDS, stations: [['G', 'Gauge', 30, -150, 'R', 'UTC', true], ['O', 'Opened', 30, -149.9969, 'S', 'UTC', false]] };
  const b = boot({ zoom: 11, list });
  b.api.rebuildTideMarkers(true); await flush();
  byTitle(b, 'Opened').handlers.click({ latlng: null, originalEvent: {} });
  assert.equal(offOf(byTitle(b, 'Opened')), null, '12 px apart at zoom 11: no nudge');
  b.setZoom(10.5); b.api.rebuildTideMarkers(false);                          // the same two icons (no rebuild before)
  assert.equal(b.tideLayer.items.length, 2);
  assert.ok(offOf(byTitle(b, 'Opened')), "the opened icon moved aside: the gauge's centre is not under it");
  assert.equal(offOf(byTitle(b, 'Gauge')), null, 'the gauge stays');
  b.setZoom(11); b.api.rebuildTideMarkers(false);
  assert.equal(offOf(byTitle(b, 'Opened')), null, 'back at 11: back on its spot');
});

test('fresh check F-2 / T04 / T05: nudges by importance (opening swaps nothing); the box test is square and needs one more pixel', async () => {
  const list = { fields: FIELDS, stations: [
    ['G', 'Gauge', 30, -150, 'R', 'UTC', true], ['S', 'Same spot', 30, -150, 'S', 'UTC', false],
    ['E1', 'Edge a', 20, -150, 'R', 'UTC', true], ['E2', 'Edge b', 20, -149.99775, 'S', 'UTC', false],              // 9 px east
    ['D1', 'Diag a', 10, -150, 'R', 'UTC', true], ['D2', 'Diag b', 9.998, -149.998, 'S', 'UTC', false]] };          // 8 px, 8 px
  const b = boot({ zoom: 11, list });
  b.api.rebuildTideMarkers(true); await flush();
  assert.equal(offOf(byTitle(b, 'Gauge')), null); assert.equal(offOf(byTitle(b, 'Same spot')), '12,0');
  assert.ok(offOf(byTitle(b, 'Edge b')), 'a centre ON the other box\'s edge (9 px) moves too (T04)');
  assert.ok(offOf(byTitle(b, 'Diag b')), '8 px in x and y (11.3 px apart) lies in the square box (T05)');
  byTitle(b, 'Same spot').handlers.click({ latlng: null, originalEvent: {} });
  assert.equal(offOf(byTitle(b, 'Gauge')), null, 'the neighbour does not move into the clicked spot');
  assert.equal(offOf(byTitle(b, 'Same spot')), '12,0', 'the opened icon stays under the pointer');
});

test('fresh check F-3 / T13 / T15 / T26 / T28: a hidden focused marker hands the focus to the map; padding Tab stops; a hidden window keeps no focus; one key handler', async () => {
  const list = { fields: FIELDS, stations: [['G', 'Gauge', 30, -150, 'R', 'UTC', true], ['O', 'Other', 30, -149.997, 'S', 'UTC', false]] };
  const b = boot({ zoom: 11, list });
  b.api.rebuildTideMarkers(true); await flush();
  byTitle(b, 'Other').el.focus();
  b.setZoom(10.5); b.api.rebuildTideMarkers(false);                          // Other is hidden below 11
  assert.equal(b.tideLayer.items.length, 1);
  assert.equal(b.api.container.focused, 1, 'its focus goes to the map, not the page\'s start');
  const live = b.mkEl(); live.attrs['data-mk'] = 'l:51201#0'; b.document.activeElement = live;   // another layer's marker
  b.setZoom(11); b.api.rebuildTideMarkers(false);
  assert.equal(b.document.activeElement, live, 'a live marker keeps the focus through a tide rebuild');
  assert.equal(b.api.container.focused, 1);
  // a marker 50 px past the view's edge is no Tab stop (T13: the padding is not the view)
  const p = boot({ zoom: 9, size: { x: 300, y: 1e9 }, list: { fields: FIELDS, stations: [['N', 'Near edge', 30, -179.65, 'R', 'UTC', true]] } });
  p.api.rebuildTideMarkers(true); await flush();
  assert.equal(byTitle(p, 'Near edge').el.getAttribute('tabindex'), '-1');
  // the focus on the page's body when the window closes: the map takes it (T15)
  const c = boot({ zoom: 11, list });
  c.api.rebuildTideMarkers(true); await flush();
  byTitle(c, 'Gauge').handlers.click({ latlng: null, originalEvent: {} });
  c.document.activeElement = c.document.body;
  c.api.closeTideWindow();
  assert.equal(c.api.container.focused, 1);
  // no tide module: the window stays hidden and its header takes no focus (T26); the key handler is bound once (T28)
  const n = boot({ zoom: 11, list, noTides: true });
  n.api.rebuildTideMarkers(true); await flush();
  const g = byTitle(n, 'Gauge');
  g.el.focus(); g.el.keys.keydown({ key: 'Enter', preventDefault() {} });
  assert.equal(n.byId.tideWin.hidden, true); assert.notEqual(n.document.activeElement, n.byId.twHeader);
  g.handlers.add(); g.handlers.add();
  assert.equal(g.el.bound.keydown, 1, 'one key handler per element, however often Leaflet adds it');
});

test('fresh check F-4: Retry\'s answer takes the focus only from the page\'s body or the tide window', async () => {
  const b = boot({ zoom: 9 });
  b.api.rebuildTideMarkers(true); await flush();
  b.tideLayer.items[0].handlers.click({ latlng: null, originalEvent: {} });
  const gear = b.mkEl(); b.document.activeElement = gear;                    // the visitor went to the settings
  b.view.deps.onRetried();
  assert.equal(b.document.activeElement, gear, 'the focus stays in the settings');
  b.document.activeElement = b.document.body;
  b.view.deps.onRetried();
  assert.equal(b.document.activeElement, b.byId.twHeader);
  const retryBtn = b.mkEl();                                                  // the Retry button, inside the window
  b.byId.tideWin.contains = (x) => x === b.byId.twHeader || x === retryBtn;
  b.document.activeElement = retryBtn;
  b.view.deps.onRetried();
  assert.equal(b.document.activeElement, b.byId.twHeader, 'from inside the window too');
});

test('fresh check T16 / T17 / T27: live markers: Tab stops follow the view without a rebuild, the focus survives a rebuild, no pan on focus, Enter opens', async () => {
  const size = { x: 300, y: 1e9 };
  const b = boot({ zoom: 9, size, list: { fields: FIELDS, stations: [] } });
  b.api.liveData = [{ id: 'IN', lat: 30, lon: -179.85, source: 'NDBC' }, { id: 'OUT', lat: 30, lon: -179.5, source: 'NDBC' }];
  b.api.rebuildLiveBuoyMarkers(true);
  const byId = (id) => b.liveLayer.items.find((m) => m.opts.title.indexOf('Live ' + id + ' ') === 0);
  assert.equal(byId('IN').opts.autoPanOnFocus, false, 'a focused live marker never pans the map (T27)');
  assert.equal(byId('IN').el.getAttribute('tabindex'), '0'); assert.equal(byId('OUT').el.getAttribute('tabindex'), '-1');
  size.x = 600; b.api.rebuildLiveBuoyMarkers(false);
  assert.equal(byId('OUT').el.getAttribute('tabindex'), '0', 'Tab stops follow the view (T16)');
  byId('IN').el.focus();
  const old = byId('IN').el;
  b.api.rebuildLiveBuoyMarkers(true);
  assert.notEqual(byId('IN').el, old); assert.equal(b.document.activeElement, byId('IN').el, 'the focus on the new element (T17)');
  byId('IN').el.keys.keydown({ key: 'Enter', preventDefault() {} });
  assert.deepEqual(b.liveOpened, ['IN']);
});
