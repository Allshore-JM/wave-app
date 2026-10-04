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
  const document = { visibilityState: o.visibility || 'visible',
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
    "\nreturn { get liveStationsData() { return liveStationsData; }, get liveNoteText() { return liveNoteText; }, paintLiveNote, addLiveBuoyLayer };";
  const api = new Function('window', 'document', 'fetch', 'performance', 'console', 'earlyFetch', 'rebuildLiveBuoyMarkers', 'measureTopRight', 'layersControl', 'AbortController', code)(
    w, document, fetch, performance, console, earlyFetch, rebuildLiveBuoyMarkers, measureTopRight, layersControl, AbortController);
  return { w, api, noteEl, document, docListeners, fetchCalls, marks, warns, earlyCalls, rebuilds, measures, updates, stored, layersControl };
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
const GENERIC = cut('    async function loadGenericBuoyDetails(station, seq) {', "    const unitSelect = document.getElementById('unit');");

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
