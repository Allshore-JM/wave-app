'use strict';
// The page's own wind-station block (templates/index.html, plan section 39), run in Node against the REAL
// static_ui/winds.js (flags, feed) with stubs for Leaflet, the fetches and the window: the zoom gate and its legend
// note, the station list asked once (and again after a failure), one flag per station and world copy with the opened
// one rimmed, the feed running only while the flags show, a feed answer repainting the drawn flags IN PLACE (the same
// markers), the thinning, the credit while the layer is on, a click that opens the window (or goes to a map tool), the
// unit change, and the close that clears the chart and the rim. The tide block's shared helpers come along as they are.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Document } = require('./fakedom');

const ROOT = path.join(__dirname, '..', '..');
const TPL = fs.readFileSync(path.join(ROOT, 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');
const SRC = fs.readFileSync(path.join(ROOT, 'static_ui', 'winds.js'), 'utf8');

function cut(from, to, inclusive) {
  const a = TPL.indexOf(from); if (a < 0) throw new Error('not in the template: ' + from);
  const b = TPL.indexOf(to, a); if (b < 0) throw new Error('not in the template: ' + to);
  return TPL.slice(a, inclusive ? b + to.length : b);
}
const NOTE = cut("    // The live list's state beside its legend entry, as TEXT.", "    map.on('overlayadd', saveLayerVisibility);");
const TIDE_STATE = cut('    const TIDE_MIN_ZOOM = 9;', '    let tideView = null;\n', true);
const WIND_STATE = cut("    // the wind layer's state (plan section 39", "    let windStaleS = 7200;", true) + ';\n';
const TIDE_BLOCK = cut('    // ---------------- Tide stations (plan section 38) ----------------', '    // Wire map movement after all wrapped-marker state');
const WIND_BLOCK = cut('    // ---------------- Wind stations (plan section 39) ----------------', '    // ---------------- (end of the wind stations block) ----------------');

const flush = async (n) => { for (let i = 0; i < (n || 10); i++) await new Promise((r) => setImmediate(r)); };
const NOW = 1791651180000;
const LIST = { fields: ['id', 'name', 'lat', 'lon', 'kind', 'src', 'tz', 'alias'], stations: [
  ['coops:1612340', 'Honolulu', 21.3033, -157.8645, 'gauge', 'coops', 'Pacific/Honolulu', 'OOUH1'],
  ['ndbc:51003', 'Western Hawaii', 19.151, -160.617, 'buoy', 'ndbc', 'Pacific/Honolulu', null],
  ['metar:PHNL', 'Honolulu Intl', 21.315, -157.924, 'airport', 'metar', 'Pacific/Honolulu', null],
  ['metar:BAD', 'Nowhere', 'x', 1, 'airport', 'metar', 'UTC', null]] };
const LATEST = { now: NOW / 1000, stale_s: 7200, fields: ['id', 't', 's', 'g', 'd'],
  rows: [['coops:1612340', NOW / 1000 - 600, 1.0, 2.8, 75], ['ndbc:51003', NOW / 1000 - 3600 * 3, 6.0, 8.0, 130]], missing: ['metar:PHNL'] };
function resp(status, body, headers) {
  return { ok: status >= 200 && status < 300, status, headers: { get: (k) => (headers || {})[k] || null }, json: () => Promise.resolve(JSON.parse(JSON.stringify(body))) };
}

function boot(o = {}) {
  const doc = new Document();
  const mk = (tag, id) => doc.register(doc.createElement(tag), id);
  const notes = { live: { textContent: '' }, tide: { textContent: '' }, wind: { textContent: '' } };
  doc.querySelector = (sel) => (sel.indexOf('[data-live-note]') >= 0 ? notes.live : sel.indexOf('[data-tide-note]') >= 0 ? notes.tide : sel.indexOf('[data-wind-note]') >= 0 ? notes.wind : null);
  ['windTitle', 'windSubtitle', 'tideTitle', 'tideSubtitle'].forEach((id) => mk('span', id));
  const windWinEl = mk('section', 'windWin'); windWinEl.hidden = true; mk('section', 'tideWin').hidden = true;
  const wwHeader = mk('div', 'wwHeader'); windWinEl.appendChild(wwHeader); mk('div', 'twHeader');
  ['windContent', 'windLoading', 'windError', 'windErrorText', 'windRetry', 'windCurrent', 'windChart', 'windArrows', 'windTable', 'windMeta'].forEach((id) => mk('div', id));
  const tz = mk('select', 'tz'); const opt = doc.createElement('option'); opt.value = 'UTC'; tz.appendChild(opt);
  mk('div', 'station').value = '51201';                                   // (the page's select; a plain element keeps any value)
  const events = []; doc.dispatchEvent = (e) => { events.push(e.type); return true; };
  let zoom = o.zoom === undefined ? 9 : o.zoom, on = o.on !== false, hidden = false;
  const docListeners = {};
  doc.addEventListener = (t, fn) => { (docListeners[t] = docListeners[t] || []).push(fn); };
  Object.defineProperty(doc, 'hidden', { get: () => hidden });
  const windLayer = { items: [], clearLayers() { this.items = []; } }, tideLayer = { items: [], clearLayers() { this.items = []; } };
  const counts = new Map();
  const credits = { count: (t) => counts.get(t) || 0, has: (t) => (counts.get(t) || 0) > 0 };
  const pxPerDeg = o.pxPerDeg || 1000, scale = () => pxPerDeg * Math.pow(2, zoom - 9);
  const container = { focused: 0, focus() { this.focused++; } };
  const map = { hasLayer: (l) => (l === windLayer && on) || (l === tideLayer && false), getZoom: () => zoom,
    latLngToContainerPoint: (ll) => ({ x: (ll[1] + 180) * scale(), y: (90 - ll[0]) * scale() }),
    mouseEventToContainerPoint: (ev) => ({ x: ev.clientX, y: ev.clientY }),
    project: (ll, z) => ({ x: (ll[1] + 180) * pxPerDeg * Math.pow(2, z - 9), y: (90 - ll[0]) * pxPerDeg * Math.pow(2, z - 9) }),
    getSize: () => o.size || { x: 1e9, y: 1e9 }, getContainer: () => container,
    attributionControl: { addAttribution: (t) => { counts.set(t, credits.count(t) + 1); }, removeAttribution: (t) => { if (credits.count(t)) counts.set(t, credits.count(t) - 1); } } };
  // Leaflet stubs: a marker's element holds the divIcon's html element (the flag)
  const L = { marker(ll, opts) {
    const el = doc.createElement('div'); if (opts.icon && opts.icon.divIcon && opts.icon.divIcon.html) el.appendChild(opts.icon.divIcon.html);
    el.focus = function () { doc.activeElement = this; };
    return { ll, opts, el, handlers: {}, addTo(layer) { layer.items.push(this); return this; }, getElement() { return this.el; },
      getLatLng() { return { lat: ll[0], lng: ll[1] }; }, fire(t, d) { if (this.handlers[t]) this.handlers[t](d); return this; },
      bindTooltip(c, tipOpts) { this.tip = c; this.tipOpts = tipOpts; return this; }, setTooltipContent(c) { this.tip = c; this.tipSets = (this.tipSets || 0) + 1; return this; },
      on(t, fn) { this.handlers[t] = fn; return this; } }; },
    divIcon: (opts) => ({ divIcon: opts }) };
  const fetchCalls = [];
  const answers = { '/api/wind/stations': (o.listAnswers || [resp(200, o.list || LIST)]).slice(), '/api/wind/latest': (o.latestAnswers || [resp(200, LATEST)]).slice() };
  const fetch = (u, init) => { fetchCalls.push(u); const q = answers[u] || []; const a = q.length > 1 ? q.shift() : q[0];
    if (!a) return new Promise(() => {}); if (a instanceof Error) return Promise.reject(a); return Promise.resolve(a); };
  const timerQ = [];
  const fakeSet = (fn, ms) => { timerQ.push({ fn, ms }); return timerQ.length; }, fakeClear = (id) => { if (timerQ[id - 1]) timerQ[id - 1].fn = null; };
  const w = {}; new Function('window', 'module', 'setTimeout', 'clearTimeout', SRC)(w, undefined, fakeSet, fakeClear);
  const loads = [], clears = [], units = [], zones = [];
  const view = { load: (st, opts) => loads.push([st.id, opts]), clear: () => clears.push(1), resize() {}, show() {}, setUnit: (u) => units.push(u), setZone: (z) => zones.push(z) };
  const realWinds = w.AllshoreWinds;
  const AllshoreWinds = Object.assign({}, realWinds, { createWindView: (deps) => { view.deps = deps; return view; } });
  const toolClicks = [];
  const window = { AllshoreWinds: o.noWinds ? null : AllshoreWinds, AllshoreTools: { active: () => !!o.toolActive, click: (ll, ev) => toolClicks.push(ll) } };
  const windWin = { opens: 0, el: windWinEl, isOpen: () => !windWinEl.hidden, window: { mode: 'normal' },
    open() { this.opens++; windWinEl.hidden = false; }, close() { windWinEl.hidden = true; api.windWindowClosed(); } };
  const visibleCopies = (stations) => stations.flatMap((s) => (o.copies || [0]).map((off) => ({ s, lng: s.lon + off, o: off })));
  const renderSignature = (vis) => vis.map((c) => c.s.id + '@' + c.lng).join(',');
  const saves = [];
  let unit = o.unit || 'US';
  const code = 'let liveNoteText = "";\n' + TIDE_STATE + WIND_STATE + NOTE.replace("    let liveNoteText = '';\n", '') + TIDE_BLOCK + WIND_BLOCK +
    '\nreturn { get windNoteText() { return windNoteText; }, get windStationsData() { return windStationsData; }, get activeWindId() { return activeWindId; }, ' +
    'get windDrawn() { return windDrawn; }, get windReadings() { return windReadings; }, get windFeed() { return windFeed; }, ' +
    'rebuildWindMarkers, loadWindStations, openWindStation, closeWindWindow, windWindowClosed, windZone, refreshWindFlags, syncWindNote, paintWindNote, WIND_CREDIT, windSubtitle, ' +
    'get windView() { return windView; } };';
  const layersControl = { _update: function () { notes.wind.textContent = ''; notes.tide.textContent = ''; notes.live.textContent = ''; return this; } };
  const forecastStationsData = o.forecast || [];                       // the yellow dots (a flag over one gets a hollow ring)
  const api = new Function('window', 'document', 'fetch', 'map', 'L', 'windLayer', 'tideLayer', 'tideIcon', 'tideIconActive', 'visibleCopies', 'renderSignature', 'textTip',
    'saveMapView', 'saveLayerVisibility', 'measureTopRight', 'getSelectedUnit', 'tzAbbr', 'CustomEvent', 'windWin', 'tideWin', 'layersControl', 'MAX_MAP_ZOOM', 'Date', 'forecastStationsData', code)(
    window, doc, fetch, map, L, windLayer, tideLayer, 'ICON', 'ICON-ACTIVE', visibleCopies, renderSignature, (t) => ({ text: String(t) }),
    () => saves.push('view'), () => saves.push('layers'), () => {}, () => unit, (iso, tz) => tz + '!',
    class { constructor(type) { this.type = type; } }, windWin, null, layersControl, 11, Object.assign(function () {}, { now: () => NOW }), forecastStationsData);
  api.container = container;
  return { api, notes, doc, events, windLayer, credits, fetchCalls, loads, clears, units, zones, view, toolClicks, saves, windWin, timerQ, docListeners,
    setZoom: (z) => { zoom = z; }, setOn: (v) => { on = v; }, setUnit: (u) => { unit = u; }, setHidden: (h) => { hidden = h; }, answers };
}
const flagOf = (m) => m.el.children[0];

test('below the gate: no request, no flags, no zoom hint (owner, 2026-10-10); at the gate the list is asked once, the flags drawn, the feed started', async () => {
  const b = boot({ zoom: 8.9 });
  b.api.rebuildWindMarkers(true);
  assert.equal(b.fetchCalls.length, 0); assert.equal(b.windLayer.items.length, 0);
  assert.equal(b.notes.wind.textContent, '');
  assert.equal(b.credits.has(b.api.WIND_CREDIT), true, 'the credit while the layer is on');
  assert.equal(b.api.windFeed, null, 'no feed below the gate');
  b.setZoom(9.0); b.api.rebuildWindMarkers(false);
  assert.deepEqual(b.fetchCalls, ['/api/wind/stations']);                  // the feed's request goes out on a microtask
  assert.equal(b.notes.wind.textContent, 'loading…');
  await flush();
  assert.deepEqual(b.fetchCalls, ['/api/wind/stations', '/api/wind/latest']);
  assert.equal(b.api.windStationsData.length, 3, 'a row without numbers is dropped');
  assert.equal(b.windLayer.items.length, 3);
  const m = b.windLayer.items[0];
  assert.equal(m.opts.icon.divIcon.className, 'wind-marker'); assert.deepEqual(m.opts.icon.divIcon.iconSize, [40, 40]); assert.equal(m.opts.zIndexOffset, 300);
  assert.ok(flagOf(m).classList.contains('wind-flag'), 'the divIcon holds the flag element');
  assert.equal(m.tip.text, 'Wind station Honolulu: 2 mph from ENE (75°), gusts 6 mph · 10 min ago');
  assert.equal(m.opts.title, 'Wind station Honolulu: No recent reading', 'built from the list before the feed answered ...');
  assert.equal(m.el.getAttribute('title'), m.tip.text, '... then the answer rewrote the element in place');
  assert.equal(m.tipSets, 1);
  assert.ok(flagOf(m).classList.contains('wind-light') && !flagOf(m).classList.contains('wind-stale'));
  const buoy = b.windLayer.items[1];
  assert.ok(flagOf(buoy).classList.contains('wind-moderate') && flagOf(buoy).classList.contains('wind-stale'), '3 h old: grey');
  const air = b.windLayer.items[2];
  assert.ok(flagOf(air).classList.contains('wind-none'), 'no reading: a small grey ring');
  assert.equal(air.tip.text, 'Wind station Honolulu Intl: No recent reading');
  assert.equal(b.notes.wind.textContent, '', 'a complete answer: no note');
  assert.equal(b.api.windFeed.state().running, true);
  b.api.rebuildWindMarkers(false); b.api.rebuildWindMarkers(false);
  assert.equal(b.fetchCalls.length, 2, 'asked once each');
  assert.equal(b.credits.count(b.api.WIND_CREDIT), 1, 'the credit added once');
  b.setZoom(8.5); b.api.rebuildWindMarkers(false);
  assert.equal(b.windLayer.items.length, 0); assert.equal(b.notes.wind.textContent, '', 'zoomed out: no zoom hint');
  assert.equal(b.api.windFeed.state().running, false, 'the feed stops below the gate');
  b.setZoom(9.5); b.api.rebuildWindMarkers(false); await flush();
  assert.equal(b.fetchCalls.length, 3, 'the feed asks again when it restarts (the list is kept)');
  assert.equal(b.fetchCalls[2], '/api/wind/latest');
  b.setOn(false); b.api.rebuildWindMarkers(true);
  assert.equal(b.windLayer.items.length, 0); assert.equal(b.notes.wind.textContent, ''); assert.equal(b.credits.has(b.api.WIND_CREDIT), false);
  assert.equal(b.api.windFeed.state().running, false);
});

test('a feed answer repaints the drawn flags in place: the same markers, new text, classes and tooltips; a unit change too', async () => {
  const b = boot({ latestAnswers: [resp(200, { now: NOW / 1000, stale_s: 7200, fields: ['id', 't', 's', 'g', 'd'], rows: [], missing: [] }, { 'X-Wind-Stations-Partial': 'COOPS' })] });
  b.api.rebuildWindMarkers(true); await flush();
  const before = b.windLayer.items.slice();
  assert.ok(before.every((m) => flagOf(m).classList.contains('wind-none')), 'nothing yet: grey rings');
  assert.equal(b.notes.wind.textContent, 'loading more…', 'a partial answer');
  b.answers['/api/wind/latest'] = [resp(200, LATEST)];
  const t = b.timerQ.find((x) => x.fn && x.ms === 5000); t.fn(); await flush();   // the partial re-ask
  assert.equal(b.windLayer.items.length, 3);
  assert.ok(b.windLayer.items.every((m, i) => m === before[i]), 'the same marker objects');
  const m = before[0], f = flagOf(m);
  assert.ok(f.classList.contains('wind-light') && f.classList.contains('wind-dir') && !f.classList.contains('wind-none'));
  assert.equal(f.querySelector('.wind-num').textContent, '2'); assert.equal(f.querySelector('.wind-arrow').style.transform, 'rotate(255deg)');
  assert.equal(m.tip.text, 'Wind station Honolulu: 2 mph from ENE (75°), gusts 6 mph · 10 min ago'); assert.equal(m.tipSets, 1);
  assert.equal(m.el.getAttribute('title'), m.tip.text); assert.equal(m.el.getAttribute('aria-label'), m.tip.text);
  assert.equal(b.notes.wind.textContent, '', 'complete: the note cleared');
  assert.equal(Object.keys(b.api.windReadings).length, 2);
  b.setUnit('Metric'); b.api.refreshWindFlags();
  assert.equal(f.querySelector('.wind-num').textContent, '4'); assert.match(m.tip.text, /4 km\/h from ENE/);
  assert.ok(b.windLayer.items.every((x, i) => x === before[i]));
  assert.equal(f._html, '', 'never innerHTML');
});

test('world copies: a flag per copy, keys per copy; the active station rimmed and kept whatever the thinning', async () => {
  const b = boot({ copies: [0, 360] });
  b.api.rebuildWindMarkers(true); await flush();
  assert.equal(b.windLayer.items.length, 6);
  const keys = b.windLayer.items.map((m) => m.el.getAttribute('data-mk'));
  assert.ok(keys.includes('w:coops:1612340#0') && keys.includes('w:coops:1612340#360'));
  b.windLayer.items[0].fire('click', { latlng: { lat: 21.3, lng: -157.86 } });
  assert.equal(b.api.activeWindId, 'coops:1612340'); assert.equal(b.windWin.opens, 1);
  assert.deepEqual(b.loads, [['coops:1612340', { unit: 'US', zone: 'Pacific/Honolulu', reading: { t: NOW / 1000 - 600, s: 1.0, g: 2.8, d: 75 } }]], "the flag's reading goes with the load (shown when the history is empty: F4)");
  assert.deepEqual(b.saves, ['view', 'layers']); assert.ok(b.events.includes('allshore:windpanel'));
  assert.equal(b.doc.getElementById('windTitle').textContent, 'Honolulu');
  assert.equal(b.doc.getElementById('windSubtitle').textContent, 'Wind · NOAA tide gauge 1612340 (NDBC OOUH1)');
  const act = b.windLayer.items.filter((m) => m.opts.icon.divIcon.className === 'wind-marker wind-marker-active');
  assert.equal(act.length, 2, 'both copies of the opened station rimmed'); assert.equal(act[0].opts.zIndexOffset, 350);
  b.api.closeWindWindow();
  assert.equal(b.api.activeWindId, null); assert.equal(b.clears.length, 1); assert.equal(b.doc.getElementById('windWin').hidden, true);
  assert.equal(b.windLayer.items.filter((m) => m.opts.icon.divIcon.className.indexOf('active') >= 0).length, 0);
  assert.equal(b.api.container.focused, 1, 'the map takes the focus');
  assert.equal(b.api.windSubtitle({ id: 'metar:PHNL', kind: 'airport' }), 'Wind · Airport (METAR) PHNL');
  assert.equal(b.api.windSubtitle({ id: 'ndbc:51003', kind: 'buoy' }), 'Wind · NDBC buoy 51003');
});

test('thinning: two stations 20 px apart at zoom 9 (the gauge first) show one, both at zoom 10; the opened one always; no note for it', async () => {
  const list = { fields: LIST.fields, stations: [
    ['metar:PHJR', 'Kalaeloa', 21.3, -158.07, 'airport', 'metar', 'Pacific/Honolulu', null],
    ['coops:1612401', 'Pearl Harbor', 21.3, -158.05, 'gauge', 'coops', 'Pacific/Honolulu', null]] };
  const b = boot({ list, latestAnswers: [resp(200, { now: NOW / 1000, stale_s: 7200, fields: ['id', 't', 's', 'g', 'd'], rows: [], missing: [] })] });
  b.api.rebuildWindMarkers(true); await flush();
  assert.deepEqual(b.windLayer.items.map((m) => m.opts.title.split(':')[0]), ['Wind station Pearl Harbor'], 'the gauge first, the airport waits');
  assert.equal(b.notes.wind.textContent, '', 'a flag held back by the thinning: no note (owner, 2026-10-10)');
  b.setZoom(10); b.api.rebuildWindMarkers(false);
  assert.equal(b.windLayer.items.length, 2, '40 px apart at zoom 10'); assert.equal(b.notes.wind.textContent, '');
  b.setZoom(9); b.api.rebuildWindMarkers(false);
  assert.equal(b.windLayer.items.length, 1);
  b.setZoom(10); b.api.rebuildWindMarkers(false);
  b.windLayer.items.find((m) => m.opts.title.indexOf('Kalaeloa') >= 0).fire('click', { latlng: {} });
  b.setZoom(9); b.api.rebuildWindMarkers(false);
  assert.equal(b.windLayer.items.length, 2, 'the opened airport drawn whatever its reveal zoom');
  b.setZoom(11); b.api.rebuildWindMarkers(false);
  assert.equal(b.windLayer.items.length, 2, 'every station at the last zoom');
});

test('a tool active: the click goes to the tool; a failed list says so and is asked again later; Enter opens', async () => {
  const b = boot({ toolActive: true, listAnswers: [resp(503, { error: 'x' }), resp(200, LIST)] });
  b.api.rebuildWindMarkers(true); await flush();
  assert.equal(b.notes.wind.textContent, 'unavailable'); assert.equal(b.windLayer.items.length, 0);
  b.api.rebuildWindMarkers(false); assert.equal(b.fetchCalls.filter((u) => u.indexOf('stations') >= 0).length, 1, 'not again on every pan');
  await flush();
  const Date_ = Date; const later = { now: () => NOW + 31000 };
  // the retry after WIND_LIST_RETRY_MS: the block reads Date.now() (injected as `Date` in this harness, fixed at NOW), so force it
  b.api.loadWindStations(); await flush();
  assert.equal(b.api.windStationsData.length, 3); assert.equal(b.notes.wind.textContent, '');
  b.windLayer.items[0].fire('click', { latlng: { lat: 1, lng: 2 }, originalEvent: {} });
  assert.deepEqual(b.toolClicks, [{ lat: 1, lng: 2 }]); assert.equal(b.windWin.opens, 0); assert.equal(b.api.activeWindId, null);
  const m = b.windLayer.items[1];
  m.el.listeners.keydown[0].fn.call(m.el, { key: 'Enter', preventDefault() {} });
  assert.deepEqual(b.toolClicks.length, 2, 'Enter fires the click (the tool takes it)');
  void Date_; void later;
});

test('a forecast point under a flag\'s ring: the ring is hollow (no clicks there, the dot keeps its click); in the box: a click within 8 px of the dot selects the forecast point, anywhere else the flag opens; whole again when the zoom parts them; part of the drawn signature (step 5b)', async () => {
  // 1 px = 1/1000 degree at zoom 9 (the stub): the dot 0.005 deg east of Honolulu's gauge = 5 px at zoom 9 (hollow), 20 px at zoom 11 (near), 40 px at zoom 12
  const b = boot({ zoom: 9, forecast: [{ id: 'HNL01', lat: 21.3033, lon: -157.8595 }, { id: 'far', lat: 30, lon: -150 }, { id: 'bad', lat: 'x', lon: 1 }] });
  b.api.rebuildWindMarkers(true); await flush();
  const marker = (id) => b.windLayer.items.find((m) => m.opts.title.startsWith('Wind station ' + id));
  const flag = (id) => flagOf(marker(id));
  assert.ok(flag('Honolulu').classList.contains('wind-hollow'), 'the dot within 7 px of the station');
  assert.ok(!flag('Western Hawaii').classList.contains('wind-hollow')); assert.ok(!flag('Honolulu Intl').classList.contains('wind-hollow'));
  const before = b.windLayer.items;
  b.api.rebuildWindMarkers(false); assert.equal(b.windLayer.items, before, 'the same zoom: no redraw');
  b.setZoom(11); b.api.rebuildWindMarkers(false);
  assert.notEqual(b.windLayer.items, before, 'the set changed: redrawn');
  assert.ok(!flag('Honolulu').classList.contains('wind-hollow'), '20 px apart at zoom 11: a whole ring');
  // the stub's container point of a lat/lng: ((lon + 180) * 4000, (90 - lat) * 4000) at zoom 11
  const pt = (lat, lon) => ({ x: (lon + 180) * 4000, y: (90 - lat) * 4000 });
  const dot = pt(21.3033, -157.8595), st = pt(21.3033, -157.8645);
  marker('Honolulu').fire('click', { latlng: { lat: 21.3, lng: -157.86 }, originalEvent: { clientX: dot.x + 6, clientY: dot.y, pointerType: 'mouse' } });
  assert.equal(b.doc.getElementById('station').value, 'HNL01', 'a click 6 px from the dot: the forecast point');
  assert.ok(b.events.includes('allshore:station')); assert.equal(b.windWin.opens, 0); assert.equal(b.api.activeWindId, null);
  marker('Honolulu').fire('click', { latlng: { lat: 21.3, lng: -157.86 }, originalEvent: { clientX: dot.x + 10, clientY: dot.y, pointerType: 'touch' } });
  assert.equal(b.windWin.opens, 0, 'a tap 10 px from the dot: still the forecast point (12 px on touch)');
  marker('Honolulu').fire('click', { latlng: { lat: 21.3, lng: -157.86 }, originalEvent: { clientX: st.x - 6, clientY: st.y, pointerType: 'mouse' } });
  assert.equal(b.windWin.opens, 1, 'a click on the far side of the ring (26 px from the dot): the wind window');
  assert.equal(b.api.activeWindId, 'coops:1612340');
  b.api.windWindowClosed();
  marker('Honolulu').fire('click', { latlng: { lat: 21.3, lng: -157.86 }, originalEvent: { key: 'Enter' } });
  assert.equal(b.windWin.opens, 2, 'a key press has no position: the window');
  b.api.windWindowClosed();
  b.api.refreshWindFlags();                                                // a feed repaint changes nothing here
  b.setZoom(12); b.api.rebuildWindMarkers(false);
  marker('Honolulu').fire('click', { latlng: { lat: 21.3, lng: -157.86 }, originalEvent: { clientX: (-157.8595 + 180) * 8000, clientY: (90 - 21.3033) * 8000, pointerType: 'mouse' } });
  assert.equal(b.windWin.opens, 3, '40 px apart at zoom 12: a plain flag (no forecast point beside it)');
  b.setZoom(9); b.api.rebuildWindMarkers(false); assert.ok(flag('Honolulu').classList.contains('wind-hollow'));
  const c = boot({ zoom: 9 }); c.api.rebuildWindMarkers(true); await flush();
  assert.ok(c.windLayer.items.every((m) => !flagOf(m).classList.contains('wind-hollow')), 'no forecast points: plain flags');
  // a world copy: the dot is looked for in the flag's own copy (360 degrees east here)
  const d = boot({ zoom: 11, copies: [0, 360], forecast: [{ id: 'HNL01', lat: 21.3033, lon: -157.8595 }] });
  d.api.rebuildWindMarkers(true); await flush();
  const east = d.windLayer.items.find((m) => m.opts.title.startsWith('Wind station Honolulu') && m.ll[1] > 0);
  east.fire('click', { latlng: { lat: 21.3, lng: 202.14 }, originalEvent: { clientX: (-157.8595 + 360 + 180) * 4000 + 6, clientY: (90 - 21.3033) * 4000, pointerType: 'mouse' } });
  assert.equal(d.doc.getElementById('station').value, 'HNL01', 'the copy 360 degrees east: its own dot');
  assert.equal(d.windWin.opens, 0);
});

test('the zone hooks, the note painter after a legend rebuild, never a zoom hint, no module', async () => {
  const b = boot();
  b.api.rebuildWindMarkers(true); await flush();
  assert.equal(b.api.windZone({ tz: 'Pacific/Honolulu' }), 'Pacific/Honolulu');
  b.doc.getElementById('tz').value = 'UTC';
  assert.equal(b.api.windZone({ tz: 'Pacific/Honolulu' }), 'UTC', 'the site zone when one is chosen');
  for (const z of [8, 5, 2, 9, 10.5, 11]) {
    b.setZoom(z); b.api.rebuildWindMarkers(false);
    assert.equal(b.notes.wind.textContent, '', 'no note at zoom ' + z);
  }
  // a state note (the list unavailable) survives Leaflet's legend rebuild: the painter puts it back
  const u = boot({ listAnswers: [resp(503, { error: 'x' })] });
  u.api.rebuildWindMarkers(true); await flush();
  assert.equal(u.notes.wind.textContent, 'unavailable');
  u.notes.wind.textContent = '';                                              // Leaflet rebuilt the legend
  u.api.paintWindNote(); assert.equal(u.notes.wind.textContent, 'unavailable');
  u.api.syncWindNote(); assert.equal(u.notes.wind.textContent, 'unavailable', 'nothing to change');
  u.setZoom(8); u.api.rebuildWindMarkers(false);
  assert.equal(u.notes.wind.textContent, '', 'below the gate even the list\'s state is not shown');
  const n = boot({ noWinds: true });
  n.api.rebuildWindMarkers(true); await flush();
  assert.equal(n.windLayer.items.length, 3, 'markers without the module: plain markers'); assert.equal(n.api.windFeed, null);
  n.windLayer.items[0].fire('click', { latlng: {} });
  assert.equal(n.windWin.opens, 0, 'no view without the module: nothing opens');
});
