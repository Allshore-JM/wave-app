'use strict';
// The page's OWN live-list blocks (templates/index.html: paintLiveNote + the layersControl._update wrapper +
// addLiveBuoyLayer, and the live window's loadGenericBuoyDetails) run in Node against static_ui/livelist.js with stubs.
// Written by the G25 reviewer B (plan section 36), adopted in the fix round. TEMPLATE / LIVELIST env vars pick other
// files (template mutants).
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..', '..');
const TPL = fs.readFileSync(process.env.TEMPLATE || path.join(ROOT, 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');
const SRC = fs.readFileSync(process.env.LIVELIST || path.join(ROOT, 'static_ui', 'livelist.js'), 'utf8');

function cut(from, to, inclusive) {
  const a = TPL.indexOf(from); if (a < 0) throw new Error('not in the template: ' + from);
  const b = TPL.indexOf(to, a); if (b < 0) throw new Error('not in the template: ' + to);
  return TPL.slice(a, inclusive ? b + to.length : b);
}
const NOTE = cut("    // The live list's state beside its legend entry, as TEXT.", "    map.on('overlayadd', saveLayerVisibility);");
const LOADER = cut('    function addLiveBuoyLayer() {', '    addLiveBuoyLayer();\n', true);

const flush = async (n) => { for (let i = 0; i < (n || 10); i++) await new Promise((r) => setImmediate(r)); };
function resp(body, opts) {
  opts = opts || {};
  const headers = new Map(Object.entries(opts.headers || {}).map(([k, v]) => [k.toLowerCase(), v]));
  return { ok: opts.status === undefined || (opts.status >= 200 && opts.status < 300), status: opts.status || 200,
    headers: { get: (k) => (headers.has(k.toLowerCase()) ? headers.get(k.toLowerCase()) : null) },
    json: () => Promise.resolve(JSON.parse(JSON.stringify(body))) };
}
function station(id, source, extra) {
  return Object.assign({ id, name: 'Buoy ' + id, lat: 21.5, lon: -158.1, source, source_name: source + ' net', source_url: 'https://x', license_label: 'Open',
    attribution_text: 'Source: ' + source, capabilities: { bulk: true, recent_history: true, directional: false, spectra: false, partitions: false },
    is_stale: false, dup_of: null, tz: 'Pacific/Honolulu' }, extra || {});
}
const FULL = [station('ndbc:51201', 'NDBC'), station('aodn:SYD', 'AODN', { lat: -33.8, lon: 151.4, tz: 'Australia/Sydney' })];

function boot(o) {
  o = o || {};
  const w = {};
  if (!o.noModule) new Function('window', SRC)(w);
  const stored = new Map(Object.entries(o.local || {}));
  if (o.storageThrows) Object.defineProperty(w, 'localStorage', { get() { throw new Error('SecurityError'); } });
  else w.localStorage = { getItem: (k) => (stored.has(k) ? stored.get(k) : null), setItem: (k, v) => stored.set(k, String(v)), removeItem: (k) => stored.delete(k) };
  w.__early = o.early === undefined ? { live: Promise.resolve(resp(FULL)), stations: null } : o.early;
  const noteEl = { textContent: '' };
  const docListeners = {};
  // the legend's key button (plan section 54): its label and its dot follow the notes
  const legendDot = { hidden: true };
  const legendAttrs = { 'data-tip': 'Legend', 'aria-label': 'Legend' };
  const legendBtn = { getAttribute: (k) => legendAttrs[k], setAttribute: (k, v) => { legendAttrs[k] = String(v); },
    querySelector: (sel) => (sel === '.legend-dot' ? legendDot : null) };
  const document = { visibilityState: o.visibility || 'visible',
    getElementById: (id) => (id === 'legendBtn' ? legendBtn : null),
    querySelector: (sel) => (sel.indexOf('[data-live-note]') >= 0 ? noteEl : null),
    addEventListener: (t, fn) => { (docListeners[t] = docListeners[t] || []).push(fn); } };
  const fetchCalls = [];
  const answers = (o.answers || [resp(FULL)]).slice();
  const fetch = (u, init) => { fetchCalls.push({ u, init }); const a = answers.length ? answers.shift() : 'hang';
    if (a === 'hang') return new Promise(() => {}); if (a instanceof Error) return Promise.reject(a); return Promise.resolve(a); };
  const marks = [];
  const performance = { mark: (n, d) => { if (o.markThrows) throw new Error('no mark'); marks.push([n, d && d.detail]); } };
  const warns = [];
  const console = { warn: (...a) => warns.push(a.map(String).join(' ')) };
  const earlyCalls = [];
  const earlyFetch = (name, url) => { earlyCalls.push([name, url]); const e = w.__early && w.__early[name]; if (w.__early) w.__early[name] = null; return e || fetch(url); };
  const rebuilds = [];
  const rebuildLiveBuoyMarkers = (force) => rebuilds.push(force);
  const measures = [];
  const measureTopRight = () => measures.push(1);
  const updates = [];
  const layersControl = { _update: function () { updates.push(1); noteEl.textContent = ''; return this; } };   // Leaflet empties the list's DOM
  const code = "let liveStationsData = [];\n" + NOTE + LOADER +
    "\nreturn { get liveStationsData() { return liveStationsData; }, get liveNoteText() { return liveNoteText; }, paintLiveNote, addLiveBuoyLayer, syncLegendNote," +
    " setTide(t) { tideNoteText = t; paintTideNote(); }, setWind(t) { windNoteText = t; paintWindNote(); } };";
  // the map's live layer (G37 F3: the live note reaches the key only while the layer is on)
  const liveBuoyLayer = {}; const layerState = { on: !o.liveOff };
  const map = { hasLayer: (l) => l === liveBuoyLayer && layerState.on };
  const api = new Function('window', 'document', 'fetch', 'performance', 'console', 'earlyFetch', 'rebuildLiveBuoyMarkers', 'measureTopRight', 'layersControl', 'AbortController', 'map', 'liveBuoyLayer', code)(
    w, document, fetch, performance, console, earlyFetch, rebuildLiveBuoyMarkers, measureTopRight, layersControl, AbortController, map, liveBuoyLayer);
  return { w, api, noteEl, document, docListeners, fetchCalls, marks, warns, earlyCalls, rebuilds, measures, updates, stored, layersControl, legendDot, legendAttrs, layerState };
}

test('the head request is taken once and nulled; no own request; markers rebuilt once; one performance mark', async () => {
  const b = boot();
  assert.equal(b.w.__early.live, null, 'taken: earlyFetch could not hand it out again');
  await flush();
  assert.equal(b.fetchCalls.length, 0);
  assert.equal(b.earlyCalls.length, 0);
  assert.deepEqual(b.api.liveStationsData.map((s) => s.id), FULL.map((s) => s.id));
  assert.deepEqual(b.rebuilds, [true]);
  assert.deepEqual(b.marks, [['allshore:live-first-markers', { cached: false, partial: false }]]);
  assert.equal(b.noteEl.textContent, '');
  assert.ok(JSON.parse(b.stored.get('allshore.liveList.v1')).s.length === 2, 'stored');
});

test('without the module the plain request runs through earlyFetch (and a failure is only warned)', async () => {
  const b = boot({ noModule: true });
  await flush();
  assert.deepEqual(b.earlyCalls, [['live', '/api/buoys/live-stations']]);
  assert.deepEqual(b.rebuilds, [true]);
  assert.equal(b.api.liveStationsData.length, 2);
  const c = boot({ noModule: true, early: { live: Promise.reject(new Error('down')) } });
  await flush();
  assert.equal(c.warns.length, 1);
  assert.equal(c.api.liveStationsData.length, 0);
});

test('a remembered list: drawn first (mark says cached), the same fresh list does not rebuild, the mark fires once', async () => {
  const I = boot().w.AllshoreLiveList._internals;
  const local = { 'allshore.liveList.v1': JSON.stringify(I.pack(FULL, Date.now() - 60e3)) };
  const b = boot({ local });
  assert.deepEqual(b.rebuilds, [true], 'synchronous first draw');
  assert.equal(b.noteEl.textContent, '', 'no "cached list" flash when the fresh list lands at once');
  await flush();
  assert.deepEqual(b.rebuilds, [true, false], 'the unchanged fresh list keeps its markers');
  assert.deepEqual(b.marks, [['allshore:live-first-markers', { cached: true, partial: false }]]);
  assert.equal(b.noteEl.textContent, '');
  const changed = FULL.map((s) => Object.assign({}, s)); changed[0].name = 'Renamed';
  const c = boot({ local, early: { live: Promise.resolve(resp(changed)) } });
  await flush();
  assert.deepEqual(c.rebuilds, [true, true], 'a changed name rebuilds');
});

test('an own request carries an abort signal; the deadline retries (real timers, 2 s)', async () => {
  const b = boot({ early: { live: Promise.reject(new Error('head failed')) }, answers: [resp(FULL)] });
  await flush();
  assert.equal(b.fetchCalls.length, 0, 'the head failure first schedules a retry');
  await new Promise((r) => setTimeout(r, 2300));
  assert.equal(b.fetchCalls.length, 1);
  assert.ok(b.fetchCalls[0].init && b.fetchCalls[0].init.signal, 'signal');
  assert.equal(b.noteEl.textContent, '');
});

test('a hidden document defers the retry until visibilitychange (real timers)', async () => {
  const b = boot({ early: { live: Promise.reject(new Error('head failed')) }, answers: [resp(FULL)], visibility: 'hidden' });
  await flush();
  assert.ok(b.docListeners.visibilitychange && b.docListeners.visibilitychange.length === 1, 'registered once');
  await new Promise((r) => setTimeout(r, 2300));
  assert.equal(b.fetchCalls.length, 0, 'nothing while hidden');
  assert.equal(b.noteEl.textContent, 'unavailable, retrying…', 'nothing drawn, the head request failed');
  b.document.visibilityState = 'visible';
  b.docListeners.visibilitychange.forEach((fn) => fn());
  await flush();
  assert.equal(b.fetchCalls.length, 1, 'asked when shown');
  assert.equal(b.api.liveStationsData.length, 2);
  assert.equal(b.noteEl.textContent, '');
});

test('the note is painted as text, re-measured only on change, and re-painted after Leaflet rebuilds the list', async () => {
  const b = boot({ early: { live: new Promise(() => {}) } });
  assert.equal(b.noteEl.textContent, '', 'nothing before NOTE_DELAY_MS');
  await new Promise((r) => setTimeout(r, 350));
  assert.equal(b.noteEl.textContent, 'loading…');
  assert.equal(b.measures.length, 1);
  b.api.paintLiveNote();
  assert.equal(b.measures.length, 1, 'no change: no re-measure');
  b.layersControl._update();
  assert.equal(b.updates.length, 1);
  assert.equal(b.noteEl.textContent, 'loading…', 're-painted after the rebuild');
  assert.equal(b.measures.length, 2);
});

test('a throwing localStorage getter and a throwing performance.mark are harmless', async () => {
  const b = boot({ storageThrows: true, markThrows: true });
  await flush();
  assert.equal(b.api.liveStationsData.length, 2);
  assert.deepEqual(b.rebuilds, [true]);
});

test('an empty complete answer sets no mark; a later non-empty one marks once', async () => {
  const b = boot({ early: { live: Promise.resolve(resp([])) } });
  await flush();
  assert.deepEqual(b.marks, []);
  assert.deepEqual(b.rebuilds, [true]);
});


// ------------------------------------------------------------------ the live window's quiet retry (G25 A-1)
const GENERIC = cut("    // ---- the live window's refresh helpers (plan section 56", "    const unitSelect = document.getElementById('unit');");

function window503(answers, o) {
  o = o || {};
  const calls = [], renders = [], errors = [], waits = [];
  const ctx = { seq: 1 };
  const note = { textContent: '' };
  const document = { getElementById: (id) => (id === 'liveBuoyLoadingNote' ? note : { textContent: '', style: {}, classList: { add() {}, remove() {} } }) };
  const fetch = (u) => { calls.push(u); const a = answers.length ? answers.shift() : (o.repeat ? o.repeat(calls.length) : undefined);
    if (o.onFetch) o.onFetch(calls.length, ctx); return Promise.resolve(a); };
  // a fake setTimeout: records the wait, runs the callback on the next turn (the real one would wait seconds)
  const setTimeout = o.realTimers ? globalThis.setTimeout : (fn, ms) => { waits.push(ms); Promise.resolve().then(fn); return 0; };
  const code = 'let lastGenericSpectra = null;\n' + GENERIC + '\nreturn loadGenericBuoyDetails;';
  const fn = new Function('fetch', 'showLiveBuoyPanelLoading', 'renderGenericBuoy', 'showLiveBuoyError', 'loadChartJs', 'renderGenericSpectra',
    'getSelectedUnit', 'console', 'ctx', 'document', 'setTimeout', code.replace(/liveDetailSeq/g, 'ctx.seq'))(
    fetch, () => {}, (p) => renders.push(p), (m) => errors.push(m), () => Promise.resolve(), () => {}, () => 'US', { warn() {} }, ctx,
    document, setTimeout);
  return { run: (st) => fn(st, 1), calls, renders, errors, ctx, waits, note };
}
function r503(after) { return resp({ id: 'aodn:SYD', error: 'The buoy list is still loading.', retry: true }, { status: 503, headers: { 'Retry-After': String(after) } }); }

test('the live window retries a "still loading" answer quietly, then shows the buoy', async () => {
  const w = window503([r503(1), resp({ id: 'aodn:SYD', latest: { hs_m: 1 }, recent: [] })]);
  await w.run(FULL[1]);
  assert.equal(w.calls.length, 2, 'asked again after Retry-After');
  assert.equal(w.renders.length, 1);
  assert.deepEqual(w.errors, [], 'no error shown in between');
});

test('a retry stops when the window moved on to another buoy (stale)', async () => {
  const w = window503([r503(1), resp({ latest: null, recent: [] })], { realTimers: true });
  const p = w.run(FULL[1]);
  await flush();
  w.ctx.seq = 2;                                                   // another buoy opened meanwhile
  await p;
  assert.equal(w.calls.length, 1);
  assert.deepEqual(w.renders, []);
  assert.deepEqual(w.errors, []);
});

test('an ordinary error (no retry flag) is shown at once', async () => {
  const w = window503([resp({ error: 'unknown source' }, { status: 404 })]);
  await w.run(FULL[1]);
  assert.equal(w.calls.length, 1);
  assert.deepEqual(w.errors, ['unknown source']);
});


test('the retry loop gives up after 25 requests with a clear message (no endless spinner)', async () => {
  // an endless loop would end at the 61st request with a render (and fail), never spin forever
  const w = window503([], { repeat: (n) => (n > 60 ? resp({ latest: { hs_m: 1 }, recent: [] }) : r503(5)) });
  await w.run(FULL[1]);
  assert.equal(w.calls.length, 25);
  assert.equal(w.waits.length, 24);
  assert.deepEqual(w.renders, []);
  assert.equal(w.errors.length, 1);
  assert.match(w.errors[0], /not available yet/);
  assert.match(w.note.textContent, /still loading/, 'the wait was explained');
});

test('Retry-After is honoured and clamped to 1-30 s (5 s by default)', async () => {
  const hdr = (v) => resp({ retry: true, error: 'x' }, { status: 503, headers: v === undefined ? {} : { 'Retry-After': v } });
  const w = window503([hdr('2'), hdr('100'), hdr(undefined), hdr('abc'), hdr('0.2'), resp({ latest: { hs_m: 1 }, recent: [] })]);
  await w.run(FULL[1]);
  assert.deepEqual(w.waits, [2000, 30000, 5000, 5000, 1000]);
  assert.equal(w.renders.length, 1);
});

test('a window moved on while a request was in flight neither renders nor asks again', async () => {
  const w = window503([r503(5), resp({ latest: { hs_m: 1 }, recent: [] })], { onFetch: (n, ctx) => { if (n === 1) ctx.seq = 2; } });
  await w.run(FULL[1]);
  assert.equal(w.calls.length, 1);
  assert.deepEqual(w.renders, []);
  assert.deepEqual(w.errors, []);
});

test('a non-JSON error page (a proxy timeout) is an ordinary failure, not a parse error', async () => {
  const html = { ok: false, status: 524, headers: { get: () => null }, json: () => Promise.reject(new SyntaxError("Unexpected token '<'")) };
  const w = window503([html]);
  await w.run(FULL[1]);
  assert.deepEqual(w.errors, ['Unable to load buoy observation.']);
});

test('opening a window forgets the previous buoy (a unit change must not re-render it under the new name)', () => {
  const open = cut('    function showLiveBuoyPanelLoading(station) {', '    // NDBC buoy with no observation inside the freshness window');
  for (const name of ['lastNoaaWaveSummaryData', 'lastLiveComponentData', 'lastNoRecentStation', 'lastGenericBuoy', 'lastGenericSpectra']) {
    assert.match(open, new RegExp(name + ' = null;'), name);
  }
  const err = cut('    function showLiveBuoyError(message) {', '    function componentTypeLabel(type) {');
  assert.match(err, /getElementById\('liveBuoyGeneric'\)\.classList\.add\('d-none'\)/, 'the error hides the generic panel');
});

test('an answer for a window that moved on while it was in flight is never rendered', async () => {
  const w = window503([resp({ latest: { hs_m: 1 }, recent: [] })], { onFetch: (n, ctx) => { ctx.seq = 2; } });
  await w.run(FULL[1]);
  assert.deepEqual(w.renders, []);
  assert.deepEqual(w.errors, []);
});

test('a 503 without the retry flag (another failure) is shown at once, not retried', async () => {
  const w = window503([resp({ error: 'Service unavailable' }, { status: 503, headers: { 'Retry-After': '5' } })]);
  await w.run(FULL[1]);
  assert.equal(w.calls.length, 1);
  assert.deepEqual(w.errors, ['Service unavailable']);
});

test('the legend key shows a dot and the note in its label while a note exists (plan section 54)', async () => {
  const b = boot({ early: { live: new Promise(() => {}) } });
  assert.equal(b.legendDot.hidden, true, 'no note yet: no dot');
  assert.equal(b.legendAttrs['data-tip'], 'Legend');
  await new Promise((r) => setTimeout(r, 350));
  assert.equal(b.noteEl.textContent, 'loading…');
  assert.equal(b.legendDot.hidden, false, 'a note: the dot');
  assert.equal(b.legendAttrs['data-tip'], 'Legend · Live buoys: loading…');
  assert.equal(b.legendAttrs['aria-label'], b.legendAttrs['data-tip'], 'the label read aloud = the hover label');
  b.layersControl._update();
  assert.equal(b.legendDot.hidden, false, 'kept through a rebuild of the list');
});

test('the dot goes when the note goes', async () => {
  const b = boot();
  await flush();
  assert.equal(b.noteEl.textContent, '');
  assert.equal(b.legendDot.hidden, true);
  assert.equal(b.legendAttrs['data-tip'], 'Legend');
  assert.equal(b.legendAttrs['aria-label'], 'Legend');
});

test('G37 F3/F6: the tide and wind notes reach the key; the live note only while the Live buoys layer is on', async () => {
  const b = boot({ early: { live: new Promise(() => {}) } });
  b.api.setTide('unavailable');
  assert.equal(b.legendDot.hidden, false, 'a tide note: the dot');
  assert.equal(b.legendAttrs['data-tip'], 'Legend · Tide stations: unavailable');
  b.api.setWind('NWS feed stalled');
  assert.equal(b.legendAttrs['data-tip'], 'Legend · Tide stations: unavailable; Wind stations: NWS feed stalled');
  b.api.setTide(''); b.api.setWind('');
  assert.equal(b.legendDot.hidden, true, 'both cleared: no dot');
  await new Promise((r) => setTimeout(r, 350));
  assert.equal(b.legendAttrs['data-tip'], 'Legend · Live buoys: loading…');
  b.layerState.on = false; b.api.syncLegendNote();                          // the page's overlayremove of the live layer
  assert.equal(b.legendDot.hidden, true, 'the live layer off: no dot for its note');
  assert.equal(b.legendAttrs['data-tip'], 'Legend');
  assert.equal(b.legendAttrs['aria-label'], 'Legend');
  assert.equal(b.noteEl.textContent, 'loading…', 'the note itself stays under its (unticked) row');
  b.layerState.on = true; b.api.syncLegendNote();
  assert.equal(b.legendDot.hidden, false, 'ticked again: the dot back');
});

test('G37 F3: a viewer who turned Live buoys off sees no dot from a failing live list', async () => {
  const b = boot({ liveOff: true, early: { live: new Promise(() => {}) } });
  await new Promise((r) => setTimeout(r, 350));
  assert.equal(b.noteEl.textContent, 'loading…');
  assert.equal(b.legendDot.hidden, true);
  assert.equal(b.legendAttrs['data-tip'], 'Legend');
});


// ------------------------------------------------------------------ the live window's refresh button (plan section 56)
// The page's own dispatcher, NDBC loader and generic loader (with their refresh helpers) against a small fake page.
const DISPATCH = cut('    function loadLiveBuoyDetails(station, opts) {', '    async function loadNdbcBuoyDetails(station, seq, refresh) {');
const NDBC = cut('    async function loadNdbcBuoyDetails(station, seq, refresh) {', '    // ---- Adaptive bulk panel (non-NDBC sources) ----');
function livePage(answers) {
  const ctx = { seq: 0 }, calls = [], log = [], marks = [];
  const els = {};
  const elem = (id) => {
    if (els[id]) return els[id];
    const cls = new Set(/^liveBuoy(Content|Generic|NoRecent|Error)$/.test(id) ? ['d-none'] : []);   // as the page's markup
    return (els[id] = { id, textContent: '', scrollTop: 0, style: { display: id === 'liveBuoyLoading' ? 'block' : '' },
      classList: { add(c) { cls.add(c); }, remove(c) { cls.delete(c); }, contains(c) { return cls.has(c); } } });
  };
  const document = { getElementById: elem, dispatchEvent: () => log.push('front') };
  const fetch = (u, opts) => {
    calls.push({ u, opts });
    const a = answers[u];
    return a instanceof Error ? Promise.reject(a) : Promise.resolve(typeof a === 'function' ? a() : a);
  };
  const liveWin = { markLoaded: () => marks.push(1) };
  const code = 'let lastGenericSpectra = null, lastNoaaWaveSummaryData = null, lastLiveComponentData = null, lastNoRecentStation = null;\n' +
    // the real renderGenericSpectra remembers what it drew (lastGenericSpectra): the stub does the same
    'function renderGenericSpectra(d, m, tz) { lastGenericSpectra = d; drawSpectra(d, m, tz); }\n' +
    // ... and so does the real renderLiveBuoyDetails (lastLiveComponentData)
    'function renderLiveBuoyDetails(d) { lastLiveComponentData = d; drawComponents(d); }\n' +
    DISPATCH + NDBC + GENERIC + '\nreturn loadLiveBuoyDetails;';
  const fn = new Function('fetch', 'showLiveBuoyPanelLoading', 'renderGenericBuoy', 'drawSpectra', 'renderNoaaWaveSummary',
    'drawComponents', 'showLiveBuoyNoRecent', 'showLiveBuoyError', 'loadChartJs', 'getSelectedUnit', 'console', 'ctx',
    'document', 'setTimeout', 'liveWin', 'CustomEvent', code.replace(/liveDetailSeq/g, 'ctx.seq'))(
    fetch,
    () => { log.push('spinner'); elem('liveBuoyLoading').style.display = 'block'; },
    (p) => { log.push('bulk:' + (p.latest && p.latest.hs_m)); elem('lwBody').scrollTop = 0; },   // the real one rewrites the body
    (d) => log.push('spectra:' + d.tag),
    (d) => { log.push('summary:' + d.tag); elem('lwBody').scrollTop = 0; },
    (d) => log.push('components:' + d.tag),
    () => { log.push('norecent'); elem('liveBuoyNoRecent').classList.remove('d-none'); elem('liveBuoyContent').classList.add('d-none'); },
    (m) => log.push('error:' + m),
    () => Promise.resolve(), () => 'US', { warn() {} }, ctx, document, (f) => Promise.resolve().then(f), liveWin,
    function (t) { this.type = t; });
  return { load: fn, ctx, calls, log, marks, els: elem, shown() { elem('liveBuoyLoading').style.display = 'none'; } };
}
const AODN = station('aodn:SYD', 'AODN', { capabilities: { bulk: true, spectra: true } });
const NDBC_ST = station('ndbc:51201', 'NDBC');
const REFRESH_OPTS = { cache: 'reload', headers: { 'X-Allshore-Refresh': '1' } };
const AL = '/api/buoys/aodn%3ASYD/latest', AC = '/api/buoys/aodn%3ASYD/components';
const NS = '/api/ndbc/station/51201/wave-summary', NC = '/api/ndbc/station/51201/components';

test('section 56: a generic buoy refresh asks with the header, gets the spectra first, draws both in place, no spinner, no window raise', async () => {
  const answers = { [AL]: resp({ latest: { hs_m: 1 }, recent: [] }), [AC]: resp({ tag: 'v1' }) };
  const pg = livePage(answers);
  assert.equal(await pg.load(AODN), true);
  assert.deepEqual(pg.log, ['front', 'spinner', 'bulk:1', 'spectra:v1'], 'the first load: the bulk view at once, then the spectra');
  assert.deepEqual(pg.calls.map((c) => c.opts), [undefined, undefined], 'no refresh header on a plain load');
  assert.equal(pg.marks.length, 1, 'the window refresh clock');
  pg.shown(); pg.els('lwBody').scrollTop = 250; pg.log.length = 0; pg.calls.length = 0;
  answers[AL] = resp({ latest: { hs_m: 2 }, recent: [] }); answers[AC] = resp({ tag: 'v2' });
  assert.equal(await pg.load(AODN, { refresh: true }), true);
  assert.deepEqual(pg.calls.map((c) => c.u), [AL, AC]);
  assert.deepEqual(pg.calls.map((c) => c.opts), [REFRESH_OPTS, REFRESH_OPTS]);
  assert.deepEqual(pg.log, ['bulk:2', 'spectra:v2'], 'no spinner, no raise; both drawn together');
  assert.equal(pg.els('lwBody').scrollTop, 250, 'the scroll kept');
  assert.equal(pg.marks.length, 2);
});

test('section 56: a generic refresh that fails keeps what is shown; failed spectra keep the whole view (G39 V3-1)', async () => {
  const answers = { [AL]: resp({ latest: { hs_m: 1 }, recent: [] }), [AC]: resp({ tag: 'v1' }) };
  const pg = livePage(answers); await pg.load(AODN); pg.shown(); pg.log.length = 0;
  answers[AC] = new Error('THREDDS down');
  answers[AL] = resp({ latest: { hs_m: 3 }, recent: [] });
  assert.equal(await pg.load(AODN, { refresh: true }), false, 'the spectra shown could not be refreshed: a failure');
  assert.deepEqual(pg.log, [], 'nothing replaced: the old bulk view and spectra stay');
  answers[AC] = resp({ error: 'no spectra for this buoy' }, { status: 404 });
  assert.equal(await pg.load(AODN, { refresh: true }), false);
  assert.deepEqual(pg.log, []);
  answers[AC] = resp({ tag: 'v3' });
  assert.equal(await pg.load(AODN, { refresh: true }), true);
  assert.deepEqual(pg.log, ['bulk:3', 'spectra:v3']);
  pg.log.length = 0;
  answers[AL] = resp({ error: 'down' }, { status: 503 });
  assert.equal(await pg.load(AODN, { refresh: true }), false);
  assert.deepEqual(pg.log, [], 'nothing replaced, no error box');
  answers[AL] = new Error('offline');
  assert.equal(await pg.load(AODN, { refresh: true }), false);
  assert.deepEqual(pg.log, []);
  assert.equal(pg.marks.length, 2, 'only good loads mark');
});

test('G39 V3-1: a buoy whose spectra never showed refreshes its bulk view when the spectra still fail', async () => {
  const answers = { [AL]: resp({ latest: { hs_m: 1 }, recent: [] }), [AC]: resp({ error: 'none yet' }, { status: 404 }) };
  const pg = livePage(answers); await pg.load(AODN); pg.shown(); pg.log.length = 0;
  answers[AL] = resp({ latest: { hs_m: 4 }, recent: [] });
  assert.equal(await pg.load(AODN, { refresh: true }), true);
  assert.deepEqual(pg.log, ['bulk:4']);
});

test('section 56: a refresh while the spinner shows loads as usual; a refresh superseded by another buoy has no verdict', async () => {
  const answers = { [AL]: resp({ error: 'down' }, { status: 500 }) };
  const pg = livePage(answers);
  assert.equal(await pg.load(AODN, { refresh: true }), false);
  assert.deepEqual(pg.log, ['spinner', 'error:down'], 'still loading: the spinner, then the error as on any load');
  assert.equal(pg.calls[0].opts.headers['X-Allshore-Refresh'], '1');
  let release;
  answers[AL] = () => new Promise((r) => { release = r; });
  pg.shown();
  const p = pg.load(AODN, { refresh: true });
  await flush(2);
  pg.ctx.seq++;                                                              // another buoy, or the window closed
  release(resp({ latest: { hs_m: 9 }, recent: [] }));
  assert.equal(await p, null);
  assert.ok(!pg.log.includes('bulk:9'));
});

test('section 56: an NDBC refresh asks both files with the header and draws the summary and the spectra together; failures keep the view', async () => {
  const answers = { [NS]: resp({ tag: 's1' }), [NC]: resp({ tag: 'c1' }) };
  const pg = livePage(answers);
  assert.equal(await pg.load(NDBC_ST), true);
  assert.deepEqual(pg.log, ['front', 'spinner', 'summary:s1', 'components:c1']);
  assert.deepEqual(pg.calls.map((c) => c.opts), [undefined, undefined]);
  pg.shown(); pg.els('lwBody').scrollTop = 120; pg.log.length = 0; pg.calls.length = 0;
  answers[NS] = resp({ tag: 's2' }); answers[NC] = resp({ tag: 'c2' });
  assert.equal(await pg.load(NDBC_ST, { refresh: true }), true);
  assert.deepEqual(pg.calls.map((c) => c.opts), [REFRESH_OPTS, REFRESH_OPTS]);
  assert.deepEqual(pg.log, ['summary:s2', 'components:c2']); assert.equal(pg.els('lwBody').scrollTop, 120);
  pg.log.length = 0;
  answers[NS] = resp({ tag: 's3' }); answers[NC] = resp({ error: 'NDBC file not available' }, { status: 404 });
  assert.equal(await pg.load(NDBC_ST, { refresh: true }), false, 'the spectra shown could not be refreshed: a failure (G39 V3-1)');
  assert.deepEqual(pg.log, [], 'nothing replaced: the old summary and spectra stay together');
  answers[NC] = new Error('offline');
  assert.equal(await pg.load(NDBC_ST, { refresh: true }), false);
  assert.deepEqual(pg.log, []);
  answers[NS] = resp({ tag: 's2' });
  pg.log.length = 0;
  answers[NS] = resp({ error: 'NDBC down' }, { status: 502 });
  assert.equal(await pg.load(NDBC_ST, { refresh: true }), false);
  assert.deepEqual(pg.log, [], 'nothing replaced, no error box');
  answers[NS] = resp({ no_recent_reports: true });
  assert.equal(await pg.load(NDBC_ST, { refresh: true }), true);
  assert.deepEqual(pg.log, ['norecent']);
});

test('G39 V3-1: an NDBC buoy whose spectra never showed refreshes its summary when the spectra still fail', async () => {
  const answers = { [NS]: resp({ tag: 's1' }), [NC]: resp({ error: 'no file' }, { status: 404 }) };
  const pg = livePage(answers);
  assert.equal(await pg.load(NDBC_ST), true);
  assert.deepEqual(pg.log, ['front', 'spinner', 'summary:s1']);
  pg.shown(); pg.log.length = 0;
  answers[NS] = resp({ tag: 's2' });
  assert.equal(await pg.load(NDBC_ST, { refresh: true }), true);
  assert.deepEqual(pg.log, ['summary:s2']);
});

test('G39-2: a refresh that brings a quiet NDBC buoy back hides its "No Recent Reports" box', async () => {
  const answers = { [NS]: resp({ no_recent_reports: true }), [NC]: resp({ tag: 'c1' }) };
  const pg = livePage(answers);
  assert.equal(await pg.load(NDBC_ST), true);
  const shown = (id) => !pg.els(id).classList.contains('d-none');
  assert.ok(shown('liveBuoyNoRecent') && !shown('liveBuoyContent'));
  pg.shown(); pg.log.length = 0;
  answers[NS] = resp({ tag: 's2' });
  assert.equal(await pg.load(NDBC_ST, { refresh: true }), true);
  assert.deepEqual(pg.log, ['summary:s2', 'components:c1']);
  assert.ok(!shown('liveBuoyNoRecent') && shown('liveBuoyContent'), 'only the readings show');
});

test('G39-4 H3: a new buoy (a plain load) does not take the previous buoy\'s scroll', async () => {
  const answers = { [AL]: resp({ latest: { hs_m: 1 }, recent: [] }), [AC]: resp({ tag: 'v1' }) };
  const pg = livePage(answers);
  assert.equal(await pg.load(AODN), true);
  pg.shown(); pg.els('lwBody').scrollTop = 300;
  assert.equal(await pg.load(AODN), true);                                   // a pick, not a refresh: the spinner path
  assert.equal(pg.els('lwBody').scrollTop, 0, 'starts at the top');
});
