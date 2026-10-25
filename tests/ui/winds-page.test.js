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
  ['windContent', 'windLoading', 'windError', 'windErrorText', 'windRetry', 'windCurrent', 'windConditions', 'windChart', 'windArrows', 'windTemp', 'windTable', 'windMeta'].forEach((id) => mk('div', id));
  const tz = mk('select', 'tz'); const opt = doc.createElement('option'); opt.value = 'UTC'; tz.appendChild(opt);
  mk('div', 'station').value = '51201';                                   // (the page's select; a plain element keeps any value)
  const events = []; doc.dispatchEvent = (e) => { events.push(e.type); return true; };
  let zoom = o.zoom === undefined ? 9 : o.zoom, on = o.on !== false, hidden = false;
  const docListeners = {};
  doc.addEventListener = (t, fn) => { (docListeners[t] = docListeners[t] || []).push(fn); };
  Object.defineProperty(doc, 'hidden', { get: () => hidden });
  const windLayer = { items: [], clearLayers() { this.items = []; }, removeLayer(m) { this.items = this.items.filter((x) => x !== m); } },
    tideLayer = { items: [], clearLayers() { this.items = []; } };
  const forecastLayer = { name: 'forecast' }, pointLayer = { name: 'points' };
  const counts = new Map();
  const credits = { count: (t) => counts.get(t) || 0, has: (t) => (counts.get(t) || 0) > 0 };
  const pxPerDeg = o.pxPerDeg || 1000, scale = () => pxPerDeg * Math.pow(2, zoom - 9);
  const container = { focused: 0, focus() { this.focused++; } };
  let forecastOn = !o.forecastOff, pointsOn = !o.pointsOff;
  const map = { hasLayer: (l) => (l === windLayer && on) || (l === tideLayer && false) || (l === forecastLayer && forecastOn) || (l === pointLayer && pointsOn), getZoom: () => zoom,
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
  // a request budget (G28 A-9 T10): a page that asks again in a loop gets no more answers after FETCH_BUDGET requests, so
  // the loop stops (the test then fails on its counts) instead of spinning the process forever
  const FETCH_BUDGET = 100;
  const fetch = (u, init) => { fetchCalls.push(u); if (fetchCalls.length > FETCH_BUDGET) return new Promise(() => {});
    const q = answers[u] || []; const a = q.length > 1 ? q.shift() : q[0];
    if (!a) return new Promise(() => {}); if (a instanceof Error) return Promise.reject(a); return Promise.resolve(a); };
  const timerQ = [];
  const fakeSet = (fn, ms) => { timerQ.push({ fn, ms }); return timerQ.length; }, fakeClear = (id) => { if (timerQ[id - 1]) timerQ[id - 1].fn = null; };
  const w = {}; new Function('window', 'module', 'setTimeout', 'clearTimeout', SRC)(w, undefined, fakeSet, fakeClear);
  const loads = [], clears = [], units = [], zones = [];
  const view = { load: (st, opts) => loads.push([st.id, opts]), clear: () => clears.push(1), resize() {}, show() {}, setUnit: (u) => units.push(u), setZone: (z) => zones.push(z),
    readings: [], setReading(r) { this.readings.push(r); } };
  const realWinds = w.AllshoreWinds;
  const AllshoreWinds = Object.assign({}, realWinds, { createWindView: (deps) => { view.deps = deps; return view; } });
  const toolClicks = [];
  const window = { AllshoreWinds: o.noWinds ? null : AllshoreWinds, AllshoreTools: { active: () => !!o.toolActive, click: (ll, ev) => toolClicks.push(ll) } };
  if (o.coarse !== undefined) window.matchMedia = (q) => ({ matches: q === '(any-pointer: coarse)' && !!o.coarse });   // any touch screen (G35 B-7)
  const windWin = { opens: 0, el: windWinEl, isOpen: () => !windWinEl.hidden, window: { mode: 'normal' },
    open() { this.opens++; windWinEl.hidden = false; }, close() { windWinEl.hidden = true; api.windWindowClosed(); } };
  const visibleCopies = (stations) => stations.flatMap((s) => (o.copies || [0]).map((off) => ({ s, lng: s.lon + off, o: off })));
  const renderSignature = (vis) => vis.map((c) => c.s.id + '@' + c.lng).join(',');
  const saves = [];
  let unit = o.unit || 'US';
  const code = 'let liveNoteText = "";\n' + TIDE_STATE + WIND_STATE + NOTE.replace("    let liveNoteText = '';\n", '') + TIDE_BLOCK + WIND_BLOCK +
    '\nreturn { get windNoteText() { return windNoteText; }, get windStationsData() { return windStationsData; }, get activeWindId() { return activeWindId; }, ' +
    'get windDrawn() { return windDrawn; }, get windReadings() { return windReadings; }, get windFeed() { return windFeed; }, ' +
    'rebuildWindMarkers, loadWindStations, openWindStation, closeWindWindow, windWindowClosed, windZone, refreshWindFlags, syncWindNote, paintWindNote, WIND_CREDIT, windSubtitle, windRank, WIND_ALL_ZOOM, ' +
    'get windView() { return windView; }, tideTargets, setTideDrawn(x) { tideDrawn = x; } };';
  const layersControl = { _update: function () { notes.wind.textContent = ''; notes.tide.textContent = ''; notes.live.textContent = ''; return this; } };
  const forecastStationsData = o.forecast || [];                       // the yellow dots (a flag over one gets a hollow ring)
  const savedPointsData = o.points || [];                              // My points (the same rule: G28 B-5)
  const api = new Function('window', 'document', 'fetch', 'map', 'L', 'windLayer', 'tideLayer', 'tideIcon', 'tideIconActive', 'visibleCopies', 'renderSignature', 'textTip',
    'saveMapView', 'saveLayerVisibility', 'measureTopRight', 'getSelectedUnit', 'tzAbbr', 'CustomEvent', 'windWin', 'tideWin', 'layersControl', 'MAX_MAP_ZOOM', 'Date', 'forecastStationsData',
    'forecastLayer', 'pointLayer', 'savedPointsData', code)(
    window, doc, fetch, map, L, windLayer, tideLayer, 'ICON', 'ICON-ACTIVE', visibleCopies, renderSignature, (t) => ({ text: String(t) }),
    () => saves.push('view'), () => saves.push('layers'), () => {}, () => unit, (iso, tz) => tz + '!',
    class { constructor(type) { this.type = type; } }, windWin, null, layersControl, 11, Object.assign(function () {}, { now: () => NOW }), forecastStationsData,
    forecastLayer, pointLayer, savedPointsData);
  api.container = container;
  return { api, notes, doc, events, windLayer, credits, fetchCalls, loads, clears, units, zones, view, toolClicks, saves, windWin, timerQ, docListeners,
    setZoom: (z) => { zoom = z; }, setOn: (v) => { on = v; }, setForecastOn: (v) => { forecastOn = v; }, setPointsOn: (v) => { pointsOn = v; }, setUnit: (u) => { unit = u; }, setHidden: (h) => { hidden = h; }, answers };
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
  assert.equal(b.api.windSubtitle({ id: 'nws:001HE', kind: 'utility' }), 'Wind · HECO / HELCO / MECO station 001HE');
  assert.equal(b.api.windSubtitle({ id: 'nws:029HI', kind: 'mesonet' }), 'Wind · Mesonet station 029HI');
  assert.equal(b.api.windSubtitle({ id: 'nws:AP834', kind: 'cwop' }), 'Wind · CWOP amateur station AP834');
  // who keeps the flag where flags crowd (owner, 2026-10-10): gauges / buoys, airports / NDBC stations, the NWS stations, CWOP last
  const rank = (k) => b.api.windRank({ kind: k });
  assert.deepEqual(['gauge', 'buoy', 'airport', 'cman', 'station', 'utility', 'mesonet', 'raws', 'hads', 'weather', 'cwop'].map(rank), [0, 0, 1, 1, 1, 2, 2, 2, 2, 2, 3]);
  assert.equal(b.api.WIND_ALL_ZOOM, 13, 'every station from zoom 13 (owner, 2026-10-10)');
  assert.ok(b.api.WIND_CREDIT.indexOf('https://api.weather.gov/') > 0, 'the NWS API in the credit (step 5c)');
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

test('the dot\'s tap (G28 B-1, B-5): a forecast point or one of My points within the tap distance (8 px mouse, 12 px touch) makes the ring hollow and keeps its click; a solid ring always opens the wind window; layers off: no dots; part of the drawn signature', async () => {
  // 1 px = 1/1000 degree at zoom 9 (the stub): the dot 0.005 deg east of Honolulu's gauge = 5 px at zoom 9, 10 px at zoom 10
  const HNL = { id: 'HNL01', lat: 21.3033, lon: -157.8595 };
  const b = boot({ zoom: 9, forecast: [HNL, { id: 'far', lat: 30, lon: -150 }, { id: 'bad', lat: 'x', lon: 1 }] });
  b.api.rebuildWindMarkers(true); await flush();
  const marker = (bb, id) => bb.windLayer.items.find((m) => m.opts.title === 'Wind station ' + id || m.opts.title.startsWith('Wind station ' + id + ':'));   // the exact name (not 'Honolulu Intl')
  const flag = (bb, id) => flagOf(marker(bb, id));
  assert.ok(flag(b, 'Honolulu').classList.contains('wind-hollow'), 'the dot 5 px away: hollow');
  assert.ok(!flag(b, 'Western Hawaii').classList.contains('wind-hollow')); assert.ok(!flag(b, 'Honolulu Intl').classList.contains('wind-hollow'));
  const kept = marker(b, 'Western Hawaii');
  b.api.rebuildWindMarkers(true); assert.equal(marker(b, 'Western Hawaii'), kept, 'a flag that stays keeps its marker (B-11)');
  // hollow at zoom 9: a click on the arrow / number within 8 px of the dot -> the forecast point; further -> the window
  const pt9 = (lat, lon) => ({ x: (lon + 180) * 1000, y: (90 - lat) * 1000 });
  const dot9 = pt9(HNL.lat, HNL.lon), st9 = pt9(21.3033, -157.8645);
  marker(b, 'Honolulu').fire('click', { latlng: {}, originalEvent: { clientX: dot9.x + 7, clientY: dot9.y } });
  assert.equal(b.doc.getElementById('station').value, 'HNL01', 'within 8 px of the dot: the forecast point');
  assert.equal(b.windWin.opens, 0);
  marker(b, 'Honolulu').fire('click', { latlng: {}, originalEvent: { clientX: st9.x - 10, clientY: st9.y } });
  assert.equal(b.windWin.opens, 1, '15 px from the dot: the wind window');
  b.api.windWindowClosed();
  // zoom 10: 10 px apart. A mouse: a SOLID ring, and a click on it (the station) opens the window, never the dot (B-1)
  b.setZoom(10); b.api.rebuildWindMarkers(false);
  assert.ok(!flag(b, 'Honolulu').classList.contains('wind-hollow'), '10 px with a mouse: solid');
  const st10 = { x: (-157.8645 + 180) * 2000, y: (90 - 21.3033) * 2000 };
  marker(b, 'Honolulu').fire('click', { latlng: {}, originalEvent: { clientX: st10.x + 3, clientY: st10.y } });
  assert.equal(b.windWin.opens, 2, 'the solid ring: the wind window (7 px from the dot)');
  b.api.windWindowClosed();
  // a touch screen: 12 px -> hollow at zoom 10
  const t = boot({ zoom: 10, coarse: true, forecast: [HNL] }); t.api.rebuildWindMarkers(true); await flush();
  assert.ok(flag(t, 'Honolulu').classList.contains('wind-hollow'), '10 px on a touch screen: hollow');
  marker(t, 'Honolulu').fire('click', { latlng: {}, originalEvent: { clientX: st10.x + 9, clientY: st10.y } });
  assert.equal(t.doc.getElementById('station').value, 'HNL01', 'a tap 1 px from the dot: the forecast point');
  marker(t, 'Honolulu').fire('click', { latlng: {}, originalEvent: { key: 'Enter' } });
  assert.equal(t.windWin.opens, 1, 'a key press has no position: the window');
  // My points (B-5): the same rule; a hit opens the point (its marker's own click)
  const m = boot({ zoom: 9, points: [{ id: 'pt_21303N_157859W', lat: 21.3033, lon: -157.8595, label: 'Mine' }] });
  m.api.rebuildWindMarkers(true); await flush();
  assert.ok(flag(m, 'Honolulu').classList.contains('wind-hollow'), 'one of My points 5 px away: hollow');
  marker(m, 'Honolulu').fire('click', { latlng: {}, originalEvent: { clientX: dot9.x + 2, clientY: dot9.y } });
  assert.ok(m.events.includes('allshore:station') && m.windWin.opens === 0, 'the point opens, not the wind window');
  assert.notEqual(m.doc.getElementById('station').value, 'pt_21303N_157859W', 'the select is the forecast points\' (the event carries the point)');
  // layers off: no dots
  const off = boot({ zoom: 9, forecastOff: true, pointsOff: true, forecast: [HNL], points: [{ id: 'p', lat: 21.3033, lon: -157.8595 }] });
  off.api.rebuildWindMarkers(true); await flush();
  assert.ok(off.windLayer.items.every((x) => !flagOf(x).classList.contains('wind-hollow')), 'their layers unticked: plain flags');
  const c = boot({ zoom: 9 }); c.api.rebuildWindMarkers(true); await flush();
  assert.ok(c.windLayer.items.every((x) => !flagOf(x).classList.contains('wind-hollow')), 'no dots: plain flags');
  // a world copy: the dot is looked for in the flag's own copy (360 degrees east here)
  const d = boot({ zoom: 9, copies: [0, 360], forecast: [HNL] });
  d.api.rebuildWindMarkers(true); await flush();
  const east = d.windLayer.items.find((x) => x.opts.title.startsWith('Wind station Honolulu:') && x.ll[1] > 0);
  east.fire('click', { latlng: {}, originalEvent: { clientX: (HNL.lon + 360 + 180) * 1000 + 6, clientY: (90 - HNL.lat) * 1000 } });
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

// ---- the wind window's measured box (G28 B-7 / B-13: syncWindWidth) ----
const SYNC = cut('      var WW_GAP = 60, WW_OUT = 30, WW_ABOVE = 30;', "      window.addEventListener('resize', syncWindWidth);");
function syncPage(o) {
  const props = {}, removed = [];
  const rect = (r) => ({ getBoundingClientRect: () => r });
  const win = (w) => w && { hidden: !!w.hidden, offsetWidth: w.offsetWidth === undefined ? w.width : w.offsetWidth,
    classList: { contains: (c) => (w.cls || []).indexOf(c) >= 0 }, getBoundingClientRect: () => ({ top: w.top, width: w.width }) };
  const els = { windWin: win(o.wind), liveBuoyPanel: win(o.live), tideWin: win(o.tide) };
  const box = { getBoundingClientRect: () => ({ left: 0 }), querySelectorAll: () => (o.controls || []).map(rect) };
  const map = { getContainer: () => box };
  const document = { getElementById: (id) => els[id] || null,
    documentElement: { style: { setProperty: (k, v) => { props[k] = v; }, removeProperty: (k) => { delete props[k]; removed.push(k); } } } };
  const sync = new Function('map', 'document', SYNC + '\nreturn syncWindWidth;')(map, document);
  sync();
  return { props, removed };
}

test('the wind window\'s box: right of the top-left controls; a 60-px gap at the right while the live / tide window is open; 30 px wider than one whose top it does not clear', () => {
  const controls = [{ right: 204, width: 194 }, { right: 360, width: 300 }, { right: 999, width: 0 }];   // a hidden control (width 0) does not count
  let p = syncPage({ controls, wind: { top: 188, width: 1030 } }).props;
  assert.deepEqual(p, { '--map-topleft-r': '360px', '--ww-gap': '0px' }, 'nothing else open: the full box');
  p = syncPage({ controls, wind: { top: 188, width: 900 }, live: { top: 260, width: 820 } }).props;
  assert.equal(p['--ww-gap'], '60px'); assert.equal(p['--ww-min-w'], undefined, 'its top clears the live window by 72 px: no stick-out');
  p = syncPage({ controls, wind: { top: 188, width: 900 }, live: { top: 217.9, width: 820 } }).props;
  assert.equal(p['--ww-min-w'], '850px', 'less than 30 px above it: 30 px wider than the live window');
  p = syncPage({ controls, wind: { top: 188, width: 900 }, live: { top: 218, width: 820 } }).props;
  assert.equal(p['--ww-min-w'], undefined, 'exactly 30 px: clear');
  p = syncPage({ controls, wind: { top: 188, width: 900 }, live: { top: 150, width: 820 }, tide: { top: 120, width: 880.4 } }).props;
  assert.equal(p['--ww-min-w'], '910px', 'the wider of the two it does not clear');
  for (const cls of [['fw-min'], ['fw-max']]) {
    const r = syncPage({ controls, wind: { top: 188, width: 900 }, live: { top: 150, width: 820, cls } });
    assert.equal(r.props['--ww-gap'], '0px', cls[0] + ': not an open window'); assert.ok(r.removed.indexOf('--ww-min-w') >= 0);
  }
  assert.equal(syncPage({ controls, wind: { top: 188, width: 900 }, live: { top: 150, width: 820, hidden: true } }).props['--ww-gap'], '0px');
  assert.equal(syncPage({ controls, wind: { top: 188, width: 900 }, live: { top: 150, width: 820, offsetWidth: 0 } }).props['--ww-gap'], '0px');
  p = syncPage({ controls, wind: { top: 188, width: 900, hidden: true }, live: { top: 150, width: 820 } }).props;
  assert.equal(p['--ww-gap'], '60px'); assert.equal(p['--ww-min-w'], undefined, 'the wind window closed: nothing to measure against');
  assert.deepEqual(syncPage({ controls: [] }).props, { '--map-topleft-r': '0px', '--ww-gap': '0px' }, 'no wind window, no controls');
});

test('a partial feed answer keeps the readings of the feeds it names; the open window gets the newer reading (G28 A-10, B-3)', async () => {
  const partial = { now: NOW / 1000, stale_s: 7200, fields: ['id', 't', 's', 'g', 'd'], rows: [['ndbc:51003', NOW / 1000 - 600, 6.0, 8.0, 130]], missing: [] };
  const later = { now: NOW / 1000, stale_s: 7200, fields: ['id', 't', 's', 'g', 'd'], rows: [['coops:1612340', NOW / 1000 - 60, 5.0, null, 90]], missing: [] };
  const b = boot({ latestAnswers: [resp(200, LATEST), resp(200, partial, { 'X-Wind-Stations-Partial': 'COOPS' }), resp(200, later)] });
  b.api.rebuildWindMarkers(true); await flush();
  const hnl = () => b.windLayer.items.find((m) => m.opts.title.startsWith('Wind station Honolulu:'));
  hnl().fire('click', { latlng: {} });
  assert.equal(b.api.activeWindId, 'coops:1612340');
  const turn = () => { const t = b.timerQ.find((x) => x.fn && x.ms >= 5000); const fn = t.fn; t.fn = null; fn(); };
  turn(); await flush();                                                       // the CO-OPS feed still loading: its readings stay
  assert.ok(flagOf(hnl()).classList.contains('wind-light'), 'the gauge keeps the reading it showed');
  assert.equal(b.api.windReadings['coops:1612340'].s, 1.0);
  assert.equal(b.api.windReadings['ndbc:51003'].t, NOW / 1000 - 600, 'the named-complete feed takes its new reading');
  turn(); await flush();
  const last = b.view.readings[b.view.readings.length - 1];
  assert.equal(last.t, NOW / 1000 - 60, 'the window hears the newer reading of its station'); assert.equal(last.s, 5.0);
});

test('a tide icon on a wind flag\'s gauge has no tap pad, matched by station id (the snapshots differ by pixels at high zoom: G28 A-10)', async () => {
  const b = boot({ zoom: 9 });
  b.api.rebuildWindMarkers(true); await flush();
  const el = () => ({ style: { setProperty(k, v) { this[k] = v; } }, setAttribute() {}, getAttribute() { return null; } });
  const a = { s: { id: '1612340', lat: 21.3033, lon: -157.8595 }, lng: -157.8595, o: 0, active: false, marker: { el: el(), getElement() { return this.el; } } };
  const far = { s: { id: '1612480', lat: 21.3433, lon: -157.8595 }, lng: -157.8595, o: 0, active: false, marker: { el: el(), getElement() { return this.el; } } };
  b.api.setTideDrawn([a, far]); b.api.tideTargets();
  assert.equal(a.marker.el.style['--tide-tap'], '0px', '5 px from its own gauge\'s flag: no pad');
  assert.equal(far.marker.el.style['--tide-tap'], '10px', 'another station: its pad');
});

// ---- G28 re-check fixes (fix round 7c) ----
const markerOf = (bb, id) => bb.windLayer.items.find((m) => m.opts.title === 'Wind station ' + id || m.opts.title.startsWith('Wind station ' + id + ':'));
const HNLDOT = (dlon) => ({ id: 'HNL01', lat: 21.3033, lon: -157.8645 + dlon });   // a dot east of Honolulu's gauge, 1000 px a degree at zoom 9

test('the rings follow the forecast / My points layers: untick -> solid at the next rebuild, tick -> hollow again (R1-N3 / R2-4)', async () => {
  const b = boot({ zoom: 9, forecast: [HNLDOT(0.005)] });
  b.api.rebuildWindMarkers(true); await flush();
  assert.ok(flagOf(markerOf(b, 'Honolulu')).classList.contains('wind-hollow'));
  b.setForecastOn(false); b.api.rebuildWindMarkers(false);                 // the page calls this on overlayremove (pinned)
  assert.ok(!flagOf(markerOf(b, 'Honolulu')).classList.contains('wind-hollow'), 'the dot is hidden: the ring is solid again');
  b.setForecastOn(true); b.api.rebuildWindMarkers(false);
  assert.ok(flagOf(markerOf(b, 'Honolulu')).classList.contains('wind-hollow'), 'shown again: hollow');
  const m = boot({ zoom: 9, points: [{ id: 'pt_x', lat: 21.3033, lon: -157.8595 }] });
  m.api.rebuildWindMarkers(true); await flush();
  assert.ok(flagOf(markerOf(m, 'Honolulu')).classList.contains('wind-hollow'));
  m.setPointsOn(false); m.api.rebuildWindMarkers(false);
  assert.ok(!flagOf(markerOf(m, 'Honolulu')).classList.contains('wind-hollow'), 'My points unticked: solid');
});

test('touch: a solid ring\'s finger disc ends before a visible dot 12-26 px away; nearer: hollow; farther: the full disc (R2-3)', async () => {
  const disc = (bb) => flagOf(markerOf(bb, 'Honolulu')).style['--wind-disc'] || '';                // (the fake DOM's style)
  const x = (lng) => Math.round((((lng + 540) % 360) - 180 + 180) * 1000);   // drawn x through the harness's projection
  for (const px of [13, 15, 20]) {
    const b = boot({ zoom: 9, coarse: true, forecast: [HNLDOT(px / 1000)] });
    b.api.rebuildWindMarkers(true); await flush();
    const f = flagOf(markerOf(b, 'Honolulu'));
    assert.ok(!f.classList.contains('wind-hollow'), px + ' px: solid');
    const d = x(-157.8645 + px / 1000) - x(-157.8645);
    assert.equal(disc(b), (d - 4 - 1 - 5) + 'px', px + ' px: the disc ends 1 px before the dot (4-px dot, 5-px ring)');
  }
  const near = boot({ zoom: 9, coarse: true, forecast: [HNLDOT(0.011)] });
  near.api.rebuildWindMarkers(true); await flush();
  assert.ok(flagOf(markerOf(near, 'Honolulu')).classList.contains('wind-hollow'), '11 px on touch: hollow');
  const far = boot({ zoom: 9, coarse: true, forecast: [HNLDOT(0.030)] });
  far.api.rebuildWindMarkers(true); await flush();
  assert.equal(disc(far), '', '30 px: the full disc (no variable)');
  const point = boot({ zoom: 9, coarse: true, points: [{ id: 'pt_y', lat: 21.3033, lon: -157.8645 + 0.020 }] });
  point.api.rebuildWindMarkers(true); await flush();
  assert.equal(disc(point), (x(-157.8645 + 0.020) - x(-157.8645) - 7 - 1 - 5) + 'px', 'one of My points is drawn 14 px wide: d - 7 - 1 - 5');
  const mouse = boot({ zoom: 9, forecast: [HNLDOT(0.015)] });
  mouse.api.rebuildWindMarkers(true); await flush();
  assert.equal(disc(mouse), '', 'a mouse has no disc');
  // the disc is part of the drawn state: a zoom that changes it rebuilds the flag
  const z = boot({ zoom: 9, coarse: true, forecast: [HNLDOT(0.015)] });
  z.api.rebuildWindMarkers(true); await flush();
  const before = markerOf(z, 'Honolulu');
  z.setZoom(9.2); z.api.rebuildWindMarkers(false);
  assert.notEqual(markerOf(z, 'Honolulu'), before); assert.notEqual(disc(z), '5px');
});

test('the hollow distance is measured between whole pixels, as Leaflet draws the markers (R2-6)', async () => {
  // find an offset whose raw distance is over 8 px but whose rounded distance is 8 (the harness projects 1000 px a degree)
  const y = (lat) => (90 - lat) * 1000;                                 // a dot due south: the harness's y
  const st = 21.3033;
  let off = null;
  for (let k = 0; k < 4000 && off === null; k++) {
    const d = (8.05 + k * 0.0004) / 1000, raw = y(st - d) - y(st);
    if (raw > 8 && Math.round(y(st - d)) - Math.round(y(st)) === 8) off = d;
  }
  assert.ok(off !== null);
  const b = boot({ zoom: 9, forecast: [{ id: 'HNL01', lat: st - off, lon: -157.8645 }] });
  b.api.rebuildWindMarkers(true); await flush();
  assert.ok(flagOf(markerOf(b, 'Honolulu')).classList.contains('wind-hollow'), (off * 1000).toFixed(3) + ' px raw, 8 px drawn: hollow');
});

test('a flag whose dot changes keeps no stale marker (R57) and the dot search wraps at the date line (R54)', async () => {
  const fc = [HNLDOT(0.005)];
  const b = boot({ zoom: 9, forecast: fc });
  b.api.rebuildWindMarkers(true); await flush();
  const first = markerOf(b, 'Honolulu');
  fc.splice(0, 1, { id: 'OTHER', lat: 21.3033, lon: -157.8645 + 0.005 });
  b.api.rebuildWindMarkers(false);
  assert.notEqual(markerOf(b, 'Honolulu'), first, 'another dot at the same place: the flag is rebuilt (its click goes to the new dot)');
  // a station a hair west of 180 E and a dot a hair east of 180 W: neighbours across the world's edge
  const wrap = boot({ zoom: 9, pxPerDeg: 256 * 512 / 360, list: { fields: ['id', 'name', 'lat', 'lon', 'kind', 'src', 'tz', 'alias'],
    stations: [['metar:EDGE', 'Edge', -16.5, 179.999, 'airport', 'metar', 'Pacific/Fiji', null]] },
  forecast: [{ id: 'X', lat: -16.5, lon: -179.999 }] });
  wrap.api.rebuildWindMarkers(true); await flush();
  assert.ok(flagOf(markerOf(wrap, 'Edge')).classList.contains('wind-hollow'), '2 px apart across 180: hollow');
});

// ---- plan section 52 D: a stalled feed in the legend (the NWS API stopped on 2026-10-22; MADIS is its backup)
const STALL = cut('    const WIND_STALL_S = 3 * 3600;', '    function windNoteWanted(on) {');
const stallNote = new Function(STALL + '\nreturn windStallNote;')();

test("the stall note (owner, G35 decision 2): most NWS flags grey (fewer than half of the NWS stations fresh, from any feed) and the NWS feed itself quiet for 3 h; the time in the stations' zone, named; after a restart without the time", () => {
  const now = 1792876500, t = now - 4 * 3600;
  const ST = ['001HE', '011HI', '016HE', '029HI'].map((id) => ({ id: 'nws:' + id, tz: 'Pacific/Honolulu' })).concat([{ id: 'metar:PHNL', tz: 'Pacific/Honolulu' }]);
  const row = (id, age) => ['nws:' + id, now - age, 5, null, 90];
  const base = { now, stale_s: 7200, newest: { nws: t }, rows: [row('001HE', 60)], fed_by: {} };
  const note = stallNote(base, [], ST);
  const when = new Date(t * 1000).toLocaleString('en-US', { timeZone: 'Pacific/Honolulu', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZoneName: 'short' });
  assert.equal(note, 'NWS feed stalled since ' + when, '1 of 4 fresh'); assert.ok(/HST$/.test(note), 'the zone named: ' + note);
  const two = Object.assign({}, base, { rows: [row('001HE', 60), row('011HI', 7200)] });
  assert.equal(stallNote(two, [], ST), '', 'half fresh (stale_s exactly counts): no note');
  const relay = Object.assign({}, base, { rows: [row('001HE', 60), row('011HI', 300)], fed_by: { 'nws:001HE': 'madis', 'nws:011HI': 'madis' } });
  assert.equal(stallNote(relay, [], ST), '', 'MADIS fills half of them: no note');
  const oneRelay = Object.assign({}, base, { rows: [row('001HE', 60), row('011HI', 7201), row('016HE', 9000)], fed_by: { 'nws:001HE': 'madis' } });
  assert.ok(stallNote(oneRelay, [], ST).length > 0, 'one fresh MADIS row does not hide the outage (B-10)');
  assert.equal(stallNote(Object.assign({}, base, { rows: [row('001HE', 60), ['metar:PHNL', now - 60, 1, null, 1]] }), [], ST).slice(0, 16), 'NWS feed stalled', 'other stations do not count');
  assert.equal(stallNote(Object.assign({}, base, { newest: { nws: now - 3 * 3600 } }), [], ST), '', 'the feed brought a reading 3 h ago: alive, catching up');
  assert.equal(stallNote(Object.assign({}, base, { newest: {} }), [], ST), 'NWS feed: no recent readings', 'after a restart during the outage (B-15)');
  assert.equal(stallNote(base, ['nws'], ST), '', 'the NWS feed still loading after a deploy: not stalled');
  assert.equal(stallNote(base, [], []), '', 'no NWS stations: nothing to tell');
  assert.equal(stallNote(base, [], null), ''); assert.equal(stallNote(null, [], ST), ''); assert.equal(stallNote({}, null, ST), '');
  const ny = stallNote(base, [], [{ id: 'nws:016HE', tz: 'America/New_York' }]);
  assert.ok(/ EDT$/.test(ny), "the stations' zone, not the computer's: " + ny);
  const badZone = stallNote(base, [], [{ id: 'nws:016HE', tz: 'Not/AZone' }]);
  assert.ok(badZone.startsWith('NWS feed stalled since '), 'an unknown zone: the computer\'s, named: ' + badZone);
});

test('the stall note in the page: after the flags are repainted, guarded, behind the feed\'s own note; the credit names MADIS', () => {
  assert.ok(TPL.includes("            refreshWindFlags();\n            try { windStallText = windStallNote(payload, missing, windStationsData); } catch (e) { windStallText = ''; }   // never the flags' repaint\n            syncWindNote();"));
  assert.ok(TPL.includes('      return windFeedNote || windStallText;'));
  assert.ok(TPL.includes(' · <a href="https://madis.ncep.noaa.gov/" target="_blank" rel="noopener">NOAA MADIS</a>\';'));
});
