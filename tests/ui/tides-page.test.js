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
  const map = { hasLayer: (l) => l === tideLayer && on, getZoom: () => zoom,
    attributionControl: { addAttribution: (t) => { counts.set(t, credits.count(t) + 1); },
                          removeAttribution: (t) => { if (credits.count(t)) counts.set(t, credits.count(t) - 1); } } };
  const L = { marker(ll, opts) { return { ll, opts, handlers: {}, addTo(layer) { layer.items.push(this); return this; },
    bindTooltip(c, tipOpts) { this.tip = c; this.tipOpts = tipOpts; return this; }, on(t, fn) { this.handlers[t] = fn; return this; } }; } };
  const fetchCalls = [];
  const answers = (o.answers || [{ ok: true, status: 200, json: () => Promise.resolve(LIST) }]).slice();
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
    'saveMapView', 'saveLayerVisibility', 'measureTopRight', 'getSelectedUnit', 'tzAbbr', 'loadChartJs', 'CustomEvent', 'tideWin', 'layersControl', code)(
    window, document, fetch, map, L, tideLayer, 'ICON', 'ICON-ACTIVE', visibleCopies, renderSignature, (t) => ({ text: String(t) }),
    () => saves.push('view'), () => saves.push('layers'), () => measures.push(1), () => (o.unit || 'US'), (iso, tz) => tz + '!', () => Promise.resolve(),
    class { constructor(type) { this.type = type; } }, tideWin, layersControl);
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
  b.api.rebuildTideMarkers(true); await flush();
  assert.equal(b.notes.tide.textContent, 'unavailable'); assert.equal(b.tideLayer.items.length, 0);
  b.api.rebuildTideMarkers(false); await flush();
  assert.equal(b.fetchCalls.length, 2); assert.equal(b.notes.tide.textContent, 'unavailable');
  b.api.rebuildTideMarkers(false); await flush();
  assert.equal(b.fetchCalls.length, 3); assert.equal(b.tideLayer.items.length, 6, 'two stations in three world copies');
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
