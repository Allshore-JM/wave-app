'use strict';
// The forecast window's behaviour (static_ui/forecast.js) against the fake DOM in tests/ui/fakedom.js:
// the loader's sequencing, cache and URL sync; re-entrant graphs; drag / resize / modes; the page wiring.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { fakeWindow, buildPage, fakeChart, memStorage } = require('./fakedom');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'forecast.js'), 'utf8');
function load(win) { new Function('window', 'URLSearchParams', SRC)(win, URLSearchParams); return win.AllshoreForecast; }
const tick = () => new Promise((r) => setImmediate(r));
const I_PHONE_QUERY = () => load(fakeWindow())._internals.PHONE_QUERY;
async function settle(n) { for (let i = 0; i < (n || 8); i++) await tick(); }

function payload(over) {
  const labels = [];
  for (let i = 0; i < 48; i++) labels.push(`Saturday, September 26, 2026 ${(i % 12) + 1}:00 ${i % 24 < 12 ? 'AM' : 'PM'}`);
  const g = (v) => ({ s1: labels.map(() => v), s2: labels.map(() => null), s3: [], s4: [], s5: [], s6: [], combined: labels.map(() => v + 1) });
  return Object.assign({
    station: '51201', error: null, table_html: '<table class="forecast-compact"><tr><td>1.0</td></tr></table>',
    graph_data: { labels, height: g(2), period: g(10), direction: g(300), units: 'ft' },
    graph_header: { cycle: '20260926 12 UTC', location: '51201 (21.67N 158.12W)', tz: 'Pacific/Honolulu' },
    model: 'GFS', swan_available: true
  }, over || {});
}
// a fetch stub whose responses are released by hand, in any order
function fetchStub() {
  const calls = [];
  const fetch = (url, o) => new Promise((res, rej) => {
    const c = { url, signal: o && o.signal, aborted: false, release: (d, status) => res({ ok: !status || status < 400, status: status || 200, json: () => Promise.resolve(d) }), fail: () => rej(new TypeError('Failed to fetch')) };
    if (c.signal) c.signal.addEventListener('abort', () => { c.aborted = true; rej(Object.assign(new Error('aborted'), { name: 'AbortError' })); });
    calls.push(c);
  });
  return { fetch, calls, last: () => calls[calls.length - 1], param: (c, k) => new URLSearchParams(c.url.split('?')[1]).get(k) };
}
function ui() { const r = { applied: [], busy: [], errors: [] }; r.ui = { apply: (d, s) => r.applied.push([d, { ...s }]), busy: (b) => r.busy.push(b), error: (m, retry) => r.errors.push([m, retry]) }; return r; }

// ---- the loader ----
test('loader: a slow first response never overwrites a faster later one; the superseded fetch is aborted; the URL follows the state', async () => {
  const F = load(fakeWindow()), I = F._internals, fs_ = fetchStub(), u = ui(), urls = [];
  const st = { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  const L = I.createLoader({ fetch: fs_.fetch, now: () => 1000, replaceState: (x) => urls.push(x), swanStations: ['51201'], ui: u.ui }, st);
  const p1 = L.load({});                        // 51201
  const p2 = L.load({ station: '46001' });      // 46001, before the first answers
  await settle();
  assert.equal(fs_.calls.length, 2); assert.equal(fs_.calls[0].aborted, true, 'the first fetch is aborted');
  assert.equal(fs_.param(fs_.calls[1], 'station'), '46001'); assert.equal(fs_.param(fs_.calls[1], 'compact'), '1');
  fs_.calls[1].release(payload({ station: '46001', swan_available: false })); await p2; await settle();
  assert.equal(u.applied.length, 1); assert.equal(u.applied[0][0].station, '46001');
  assert.deepEqual(u.busy, [true, true, false]);
  assert.equal(urls[urls.length - 1], '?station=46001');
  await p1; assert.equal(u.applied.length, 1, 'the aborted first request applied nothing');
});

test('loader: the second response of two in flight is dropped when an even newer request exists; the newest wins', async () => {
  const F = load(fakeWindow()), I = F._internals, fs_ = fetchStub(), u = ui();
  const st = { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  const L = I.createLoader({ fetch: fs_.fetch, now: () => 1000, replaceState: () => {}, swanStations: [], ui: u.ui }, st);
  L.load({ unit: 'US' }); L.load({ unit: 'Metric' }); const p3 = L.load({ tz: 'UTC' });
  await settle();
  fs_.calls[2].release(payload()); await p3; await settle();
  assert.equal(u.applied.length, 1); assert.equal(u.applied[0][1].tz, 'UTC');
  assert.deepEqual(fs_.calls.map((c) => c.aborted), [true, true, false]);
});

test('loader: a cached forecast is applied without a fetch until its TTL passes; the cache holds 16 entries', async () => {
  const F = load(fakeWindow()), I = F._internals, fs_ = fetchStub(), u = ui(); let now = 1000;
  const st = { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  const L = I.createLoader({ fetch: fs_.fetch, now: () => now, replaceState: () => {}, swanStations: [], ui: u.ui }, st);
  const p = L.load({}); await settle(); fs_.last().release(payload()); await p;
  await L.load({}); assert.equal(fs_.calls.length, 1, 'served from the cache');
  await L.load({ view: 'Graph' }); assert.equal(fs_.calls.length, 1, 'the view is not part of the key');
  now += I.CACHE_TTL_MS + 1;
  const p2 = L.load({}); await settle(); assert.equal(fs_.calls.length, 2, 'expired: fetched again'); fs_.last().release(payload()); await p2;
  for (let i = 0; i < 20; i++) { const q = L.load({ station: '4600' + i }); await settle(); fs_.last().release(payload({ station: '4600' + i })); await q; }
  assert.equal(L.cache.size, I.CACHE_MAX);
});

test('loader: a failed fetch reports an error whose Retry re-runs the same state; a bad status too; an abort is silent', async () => {
  const F = load(fakeWindow()), I = F._internals, fs_ = fetchStub(), u = ui();
  const st = { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  const L = I.createLoader({ fetch: fs_.fetch, now: () => 1, replaceState: () => {}, swanStations: [], ui: u.ui }, st);
  const p = L.load({}); await settle(); fs_.last().fail(); await p;
  const errs = u.errors.filter((e) => e[0]);
  assert.equal(errs.length, 1); assert.equal(typeof errs[0][1], 'function'); assert.equal(u.busy[u.busy.length - 1], false);
  const r = errs[0][1](); await settle(); assert.equal(fs_.calls.length, 2); assert.equal(fs_.param(fs_.last(), 'station'), '51201');
  fs_.last().release({}, 503); await r; assert.equal(u.errors.filter((e) => e[0]).length, 2, 'HTTP 503 is an error');
  assert.equal(u.applied.length, 0);
});

test('loader: SWAN is never requested off the SWAN stations; the server\'s model echo wins and the URL drops model= on the fallback', async () => {
  const F = load(fakeWindow()), I = F._internals, fs_ = fetchStub(), u = ui(), urls = [];
  const st = { station: '51201', tz: '', unit: 'US', model: 'SWAN', view: 'Table' };
  const L = I.createLoader({ fetch: fs_.fetch, now: () => 1, replaceState: (x) => urls.push(x), swanStations: ['51201'], ui: u.ui }, st);
  const p = L.load({ station: '46001' }); await settle();
  assert.equal(fs_.param(fs_.last(), 'model'), 'GFS', 'normalised before the fetch'); assert.equal(urls[urls.length - 1], '?station=46001');
  fs_.last().release(payload({ station: '46001', model: 'GFS', swan_available: false })); await p;
  assert.equal(st.model, 'GFS');
  const p2 = L.load({ station: '51201', model: 'SWAN' }); await settle();
  assert.equal(fs_.param(fs_.last(), 'model'), 'SWAN');
  fs_.last().release(payload({ model: 'gfs' })); await p2;                     // a server that resolved otherwise
  assert.equal(st.model, 'GFS'); assert.equal(urls[urls.length - 1], '?station=51201');
});

test('loader: seed() shows a page-rendered forecast without a fetch and caches it', async () => {
  const F = load(fakeWindow()), I = F._internals, fs_ = fetchStub(), u = ui();
  const st = { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  const L = I.createLoader({ fetch: fs_.fetch, now: () => 1, replaceState: () => {}, swanStations: [], ui: u.ui }, st);
  L.seed(payload()); assert.equal(u.applied.length, 1); assert.equal(fs_.calls.length, 0);
  await L.load({}); assert.equal(fs_.calls.length, 0);
});

// ---- the graphs ----
function graphDeps(win, page, Chart, over) {
  const canvases = ['heightChart', 'periodChart', 'directionChart'].map((id) => win.document.getElementById(id));
  return Object.assign({ host: page.graphs, boxes: canvases.map((c) => c.parentNode), canvases, rangeBar: page.rangeBar,
    loadChartJs: () => Promise.resolve(), getChart: () => Chart, storage: win.sessionStorage, bodyHeight: () => 400, visible: () => !page.graphs.hidden }, over || {});
}

test('graphs: rendered twice for different data -> three fresh charts each time, the old ones destroyed, no canvas listener growth', async () => {
  const win = fakeWindow(), page = buildPage(win), F = load(win), I = F._internals, Chart = fakeChart();
  page.graphs.hidden = false;
  const G = I.createForecastGraphs(graphDeps(win, page, Chart));
  await G.setData(payload().graph_data); await settle();
  assert.equal(Chart.made.length, 3); assert.equal(G.charts().length, 3);
  const cv = win.document.getElementById('heightChart');
  const n1 = cv.listenerCount('mousemove') + cv.listenerCount('touchstart');
  assert.ok(n1 > 0, 'sync listeners wired');
  await G.setData(payload({ graph_data: Object.assign(payload().graph_data, { units: 'm' }) }).graph_data); await settle();
  assert.equal(Chart.made.length, 6); assert.ok(Chart.made.slice(0, 3).every((c) => c.destroyed)); assert.ok(Chart.made.slice(3).every((c) => !c.destroyed));
  assert.equal(cv.listenerCount('mousemove') + cv.listenerCount('touchstart'), n1, 'listeners replaced, not stacked');
  assert.equal(Chart.made[3].options.scales.y.title.text, 'Height (m)');
  assert.equal(Chart.made[3].options.plugins.title.text, 'Swell Height');
  assert.deepEqual([page.graphs.children[0].style.height, page.graphs.children[1].style.height], ['300px', '300px'], 'boxes at least 300 px (a 400 px body scrolls)');
  G.destroy(); assert.ok(Chart.made.slice(3).every((c) => c.destroyed)); assert.equal(cv.listenerCount('mousemove'), 0);
});

test('graphs: not rendered while hidden (drawn on show); the range setting is applied and remembered; Chart.js failing reports an error', async () => {
  const win = fakeWindow({ session: { chartRange: '7' } }), page = buildPage(win), F = load(win), I = F._internals, Chart = fakeChart();
  const errs = [];
  const G = I.createForecastGraphs(graphDeps(win, page, Chart, { onError: (e) => errs.push(e) }));
  await G.setData(payload().graph_data); await settle();
  assert.equal(Chart.made.length, 0, 'hidden: nothing built');
  page.graphs.hidden = false; await G.show(); await settle();
  assert.equal(Chart.made.length, 3);
  assert.deepEqual([Chart.made[0].options.scales.x.min, Chart.made[0].options.scales.x.max], [0, 47], '7 days of a 48-h series');
  const pressed = page.rangeBar.querySelectorAll('[data-days]').map((b) => b.getAttribute('aria-pressed'));
  assert.deepEqual(pressed, ['false', 'true', 'false']);
  page.rangeBar.querySelectorAll('[data-days]')[2].dispatch('click');
  assert.deepEqual([Chart.made[0].options.scales.x.max, win.sessionStorage.getItem('chartRange')], [47, '3']);
  G.setRange('full'); assert.equal(win.sessionStorage.getItem('chartRange'), 'full');
  const G2 = I.createForecastGraphs(graphDeps(win, page, Chart, { loadChartJs: () => Promise.reject(new Error('cdn')), onError: (e) => errs.push(e) }));
  await G2.setData(payload().graph_data); await settle();
  assert.equal(errs.length, 1);
});

test('graphs: parseLabel and rangeWindow (the old page\'s rules)', () => {
  const I = load(fakeWindow())._internals;
  const d = I.parseLabel('8/30/25 6:00 PM'); assert.deepEqual([d.getFullYear(), d.getMonth(), d.getDate(), d.getHours()], [2025, 7, 30, 18]);
  assert.equal(I.parseLabel('9/1/2026 12:00 AM').getHours(), 0);
  assert.equal(I.parseLabel('Saturday, September 26, 2026 2:00 PM').getHours(), 14);
  assert.ok(!isNaN(I.parseLabel('garbage').getTime()), 'unparseable -> some date, never NaN');
  assert.deepEqual(I.rangeWindow(385, 7), { min: 0, max: 167 }); assert.deepEqual(I.rangeWindow(385, 0), { min: 0, max: 384 }); assert.deepEqual(I.rangeWindow(10, 3), { min: 0, max: 9 });
});

test('shortCycle for GFS and SWAN headers', () => {
  const I = load(fakeWindow())._internals;
  assert.equal(I.shortCycle('GFS', { cycle: '20260926 12 UTC' }), 'GFS · run 20260926 12 UTC');
  assert.equal(I.shortCycle('SWAN', { cycle: 'PacIOOS SWAN updated 20260925 23 UTC' }), 'SWAN · updated 20260925 23 UTC');
  assert.equal(I.shortCycle('GFS', null), 'GFS'); assert.equal(I.shortCycle('SWAN', { cycle: '' }), 'SWAN');
});

// ---- the window ----
function makeWindow(win, page, over) {
  const I = load(win)._internals, modes = [], resizes = [];
  const fw = new I.FloatingWindow(Object.assign({ el: page.w, header: page.header, handle: win.document.getElementById('fwResize'), storage: win.sessionStorage, win,
    onMode: (m) => modes.push(m), onResize: () => resizes.push(1) }, over || {}));
  return { fw, modes, resizes, I };
}
const ptr = (x, y, extra) => Object.assign({ clientX: x, clientY: y, pointerId: 1, button: 0 }, extra || {});

test('window: a new tab starts minimised; modes toggle classes and are saved; expand returns to the mode before minimising', () => {
  const win = fakeWindow(), page = buildPage(win), { fw, modes } = makeWindow(win, page);
  assert.equal(fw.mode, 'min'); assert.ok(page.w.classList.contains('fw-min'));
  fw.expand(); assert.equal(fw.mode, 'normal'); assert.ok(!page.w.classList.contains('fw-min'));
  fw.toggleMax(); assert.equal(fw.mode, 'max'); assert.ok(page.w.classList.contains('fw-max'));
  fw.minimise(); fw.expand(); assert.equal(fw.mode, 'max', 'back to maximised');
  assert.deepEqual(modes, ['min', 'normal', 'max', 'min', 'max']);
  assert.deepEqual(win.sessionStorage.read('allshore.forecastWin.v1').mode, 'max');
  const win2 = fakeWindow({ session: { 'allshore.forecastWin.v1': { mode: 'normal', prev: 'normal', x: 100, y: 100, w: 800, h: 400 } } }), page2 = buildPage(win2);
  const r2 = makeWindow(win2, page2);
  assert.equal(r2.fw.mode, 'normal', 'the same tab keeps its mode'); assert.equal(page2.w.style.left, '100px');
  const win3 = fakeWindow({ session: { 'allshore.forecastWin.v1': 'garbage' } }); assert.equal(makeWindow(win3, buildPage(win3)).fw.mode, 'min');
});

test('window: drag by the header moves it (not from a button, not when minimised or maximised); the corner handle resizes; both clamp and save', () => {
  const win = fakeWindow(), page = buildPage(win), { fw, resizes } = makeWindow(win, page);
  fw.expand();
  page.header.dispatch('pointerdown', ptr(500, 320)); page.header.dispatch('pointermove', ptr(400, 220)); page.header.dispatch('pointerup', ptr(400, 220));
  assert.deepEqual([page.w.style.left, page.w.style.top, page.w.style.width], ['8px', '200px', '1180px'], 'moved 100 px up/left, clamped to the left edge');
  assert.ok(page.header.log.includes('capture:1'));
  const saved = win.sessionStorage.read('allshore.forecastWin.v1'); assert.deepEqual([saved.x, saved.y, saved.w, saved.h], [8, 200, 1180, 480]);
  const btn = win.document.getElementById('fwMin');
  btn.dispatch('pointerdown', ptr(500, 320)); page.header.dispatch('pointermove', ptr(900, 700)); page.header.dispatch('pointerup', ptr(900, 700));
  assert.equal(page.w.style.left, '8px', 'a press on a button never drags');
  const handle = win.document.getElementById('fwResize'), before = resizes.length;                  // (expand() refits once by design)
  handle.dispatch('pointerdown', ptr(1000, 700)); handle.dispatch('pointermove', ptr(900, 600)); handle.dispatch('pointerup', ptr(900, 600));
  assert.deepEqual([page.w.style.width, page.w.style.height], ['1080px', '380px']); assert.equal(resizes.length, before + 1);
  handle.dispatch('pointerdown', ptr(900, 600)); handle.dispatch('pointermove', ptr(0, 0)); handle.dispatch('pointerup', ptr(0, 0));
  assert.deepEqual([page.w.style.width, page.w.style.height], ['360px', '220px'], 'never below the minimum');
  fw.setMode('normal'); page.header.dispatch('pointerdown', ptr(100, 300)); page.header.dispatch('pointermove', ptr(400, 400)); page.header.dispatch('pointerup', ptr(400, 400));
  const g0 = { ...fw.geom }; assert.ok(g0.x > 8 && g0.y > 64, 'moved off the edges first: ' + JSON.stringify(g0));
  fw.toggleMax();
  page.header.dispatch('pointerdown', ptr(500, 320)); page.header.dispatch('pointermove', ptr(700, 500)); page.header.dispatch('pointerup', ptr(700, 500));
  assert.deepEqual(fw.geom, g0, 'no drag while maximised');
  handle.dispatch('pointerdown', ptr(900, 600)); handle.dispatch('pointermove', ptr(1100, 700)); handle.dispatch('pointerup', ptr(1100, 700));
  assert.deepEqual(fw.geom, g0, 'no resize while maximised');
  win.innerWidth = 700; win.innerHeight = 500; fw.setMode('normal'); win.fire('resize');
  const g = fw.geom; assert.ok(g.x + g.w <= 692 && g.y + g.h <= 492 && g.y >= 64, JSON.stringify(g));
});

test('window: on a phone nothing drags or resizes and geometry is left to CSS; a double-click on the header toggles maximise on desktops', () => {
  const win = fakeWindow({ phone: true, width: 375, height: 812 }), page = buildPage(win), { fw } = makeWindow(win, page);
  assert.ok(/max-height: 500px/.test(I_PHONE_QUERY()), 'a short window counts as phone mode too');
  fw.expand(); fw._expandedAt = 0;
  page.header.dispatch('pointerdown', ptr(100, 100)); page.header.dispatch('pointermove', ptr(50, 50)); page.header.dispatch('pointerup', ptr(50, 50));
  assert.equal(page.w.style.left, undefined); assert.equal(fw.geom, null);
  page.header.dispatch('dblclick', {}); assert.equal(fw.mode, 'normal', 'no maximise by double-click on a phone');
  const win2 = fakeWindow(), page2 = buildPage(win2), r = makeWindow(win2, page2);
  r.fw.expand(); r.fw._expandedAt = Date.now() - 1000;                        // (a double-click right after expanding is ignored: G16-A P3-7)
  page2.header.dispatch('dblclick', {}); assert.equal(r.fw.mode, 'max'); page2.header.dispatch('dblclick', {}); assert.equal(r.fw.mode, 'normal');
});

// ---- init: the page wiring ----
function boot(opts) {
  const win = fakeWindow(opts), page = buildPage(win), F = load(win), fs_ = fetchStub(), Chart = fakeChart();
  win.Chart = Chart;
  const closes = [];
  const app = F.init({ window: win, initial: Object.assign({ station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table', swan_available: true, swan_stations: ['51201', '51202'] }, (opts && opts.initial) || {}),
    stationLabel: (sid) => { const o = page.sel.options.find((x) => x.value === sid); return o ? o.textContent : sid; },
    loadChartJs: () => Promise.resolve(), fetch: fs_.fetch, closeLivePanel: () => { closes.push(1); page.live.style.display = 'none'; }, liveOpen: () => page.live.style.display === 'block',
    early: opts && opts.early });
  return { win, page, F, fs_, Chart, app, closes, doc: win.document };
}

test('init: fetches the URL\'s station into the minimised window; the first forecast replaces the loading placeholder and fills title, cycle, meta and table', async () => {
  const b = boot({ search: '?station=51201&view=Graph' });
  assert.ok(b.app); assert.equal(b.app.window.mode, 'min'); assert.equal(b.app.state.view, 'Graph');
  await settle(); assert.equal(b.fs_.calls.length, 1); assert.equal(b.fs_.param(b.fs_.last(), 'compact'), '1');
  assert.equal(b.doc.getElementById('fwBusy').hidden, false);
  b.fs_.last().release(payload()); await settle();
  assert.equal(b.doc.getElementById('stationCurrent').textContent, '51201 — Waimea Bay, HI');
  assert.equal(b.doc.getElementById('fwCycle').textContent, 'GFS · run 20260926 12 UTC');
  assert.equal(b.doc.getElementById('forecastMeta').textContent, 'Cycle : 20260926 12 UTC  |  Location : 51201 (21.67N 158.12W)  |  Time Zone: Pacific/Honolulu');
  assert.ok(/Cycle\s*:\s*([^|\n]*)/.exec(b.doc.getElementById('forecastMeta').textContent)[1].trim() === '20260926 12 UTC', 'the overlay\'s pageCycle regex still finds the run');
  assert.equal(b.doc.getElementById('forecastLoading').parentNode, null, 'the loading placeholder is gone');
  assert.ok(b.page.table.innerHTML.includes('forecast-compact')); assert.equal(b.page.table.hidden, true, 'Graph view: the table is hidden');
  assert.equal(b.doc.getElementById('fwBusy').hidden, true); assert.equal(b.doc.getElementById('modelBar').hidden, false);
  assert.equal(b.Chart.made.length, 0, 'minimised: no charts yet');
  b.app.expand(); await settle(); assert.equal(b.Chart.made.length, 3, 'expanded in Graph view: charts built');
  assert.equal(b.win.history.urls[b.win.history.urls.length - 1], '?station=51201&view=Graph');
});

test('init: a map pick loads the station with Buoy Local and expands; a picker pick keeps the time zone; the selects follow the state', async () => {
  const b = boot({ search: '?station=51201&tz=UTC' });
  await settle(); b.fs_.last().release(payload()); await settle();
  assert.equal(b.page.tz.value, 'UTC');
  b.doc.fire('allshore:station', { detail: { sid: '46001', source: 'map' } }); await settle();
  assert.equal(b.app.window.mode, 'normal', 'expanded'); assert.equal(b.app.state.tz, ''); assert.equal(b.page.tz.value, ''); assert.equal(b.page.sel.value, '46001');
  assert.equal(b.fs_.param(b.fs_.last(), 'tz'), '');
  b.fs_.last().release(payload({ station: '46001', swan_available: false })); await settle();
  assert.equal(b.doc.getElementById('modelBar').hidden, true, 'no SWAN here');
  assert.equal(b.doc.getElementById('stationCurrent').textContent, '46001 — Gulf of Alaska');
  b.page.tz.value = 'Pacific/Honolulu'; b.page.tz.dispatch('change'); await settle();
  assert.equal(b.fs_.param(b.fs_.last(), 'tz'), 'Pacific/Honolulu'); b.fs_.last().release(payload({ station: '46001', swan_available: false })); await settle();
  b.doc.fire('allshore:station', { detail: { sid: '51201', source: 'picker' } }); await settle();
  assert.equal(b.app.state.tz, 'Pacific/Honolulu', 'a favourites pick keeps the time zone');
  assert.equal(b.win.localStorage.read('allshore.settings.v1').tz, 'Pacific/Honolulu');
});

test('init: units and model changes refetch and are remembered; the view switches without a fetch; Escape closes the live panel first, then minimises with focus returned', async () => {
  const b = boot({});
  await settle(); b.fs_.last().release(payload()); await settle();
  b.page.unit.value = 'Metric'; b.page.unit.dispatch('change'); await settle();
  assert.equal(b.fs_.param(b.fs_.last(), 'unit'), 'Metric'); assert.deepEqual(b.win.localStorage.read('allshore.settings.v1'), { tz: '', unit: 'Metric' });
  b.fs_.last().release(payload()); await settle();
  b.doc.getElementById('modelBar').querySelectorAll('[data-model]')[1].dispatch('click'); await settle();
  assert.equal(b.fs_.param(b.fs_.last(), 'model'), 'SWAN'); b.fs_.last().release(payload({ model: 'SWAN', graph_header: { cycle: 'PacIOOS SWAN updated 20260925 23 UTC', location: 'x', tz: 'y' } })); await settle();
  assert.equal(b.doc.getElementById('fwCycle').textContent, 'SWAN · updated 20260925 23 UTC');
  assert.equal(b.doc.getElementById('modelBar').querySelectorAll('[data-model]')[1].getAttribute('aria-pressed'), 'true');
  const n = b.fs_.calls.length;
  b.page.viewBar.querySelectorAll('[data-view]')[1].dispatch('click'); await settle();
  assert.equal(b.fs_.calls.length, n, 'no fetch for a view change'); assert.equal(b.page.graphs.hidden, false); assert.equal(b.page.table.hidden, true);
  assert.equal(b.win.history.urls[b.win.history.urls.length - 1], '?station=51201&model=SWAN&view=Graph', 'Metric is the saved setting now: not named');
  b.page.trigger.focus(); b.app.expand(); assert.equal(b.doc.activeElement, b.page.header);
  b.page.live.style.display = 'block';
  b.doc.fire('keydown', { key: 'Escape' }); assert.equal(b.closes.length, 1); assert.equal(b.app.window.mode, 'normal', 'the live panel closed first');
  b.doc.fire('keydown', { key: 'Escape' }); assert.equal(b.app.window.mode, 'min'); assert.equal(b.doc.activeElement, b.page.trigger, 'focus back on the opener');
  b.doc.fire('keydown', { key: 'Escape' }); assert.equal(b.app.window.mode, 'min');
  b.doc.activeElement = b.page.gear; b.app.expand(); b.doc.activeElement = b.page.gear;   // the gear is outside the window (the select is inside it now)
  b.doc.fire('keydown', { key: 'Escape' }); assert.equal(b.app.window.mode, 'normal', 'Escape outside the window leaves it alone');
});

test('init: a page-rendered forecast (render=full) is shown without a fetch; a server error with no table shows the message; the settings panel toggles', async () => {
  const b = boot({ initial: { inline: true, graph_data: payload().graph_data, graph_header: payload().graph_header, model: 'GFS', swan_available: true } });
  b.page.table.innerHTML = '<table class="x">server</table>';
  await settle(); assert.equal(b.fs_.calls.length, 0);
  const b2 = boot({}); await settle(); b2.fs_.last().release(payload({ table_html: null, graph_data: null, error: 'No SWAN forecast available for 51201' })); await settle();
  assert.equal(b2.page.table.textContent, 'No SWAN forecast available for 51201');
  assert.equal(b2.doc.getElementById('fwError').hidden, false, 'a table-less error is shown in the box too (G16-A P2-1)');
  b2.page.gear.dispatch('click'); assert.equal(b2.page.panel.hidden, false); assert.equal(b2.page.gear.getAttribute('aria-expanded'), 'true');
  b2.page.panel.dispatch('keydown', { key: 'Escape' }); assert.equal(b2.page.panel.hidden, true); assert.equal(b2.app.window.mode, 'min', 'Escape in the panel does not reach the window');
  b2.page.gear.dispatch('click'); b2.page.header.dispatch('pointerdown', ptr(1, 1)); assert.equal(b2.page.panel.hidden, true, 'outside press closes it');
});

test('init: saved settings apply on load and win over the server (but not over the URL); a saved tz missing from the list is added to the select', async () => {
  const b = boot({ local: { 'allshore.settings.v1': { tz: 'Australia/Lord_Howe', unit: 'Metric' } } });
  assert.deepEqual([b.app.state.tz, b.app.state.unit], ['Australia/Lord_Howe', 'Metric']);
  assert.equal(b.page.tz.value, 'Australia/Lord_Howe'); assert.equal(b.page.unit.value, 'Metric');
  const c = boot({ local: { 'allshore.settings.v1': { unit: 'Metric' } }, search: '?unit=US' });
  assert.equal(c.app.state.unit, 'US');
});

test('init: the minimised bar expands on a click (not on its buttons); the maximise button from minimised opens maximised; the public API reflects the app', async () => {
  const b = boot({});
  b.page.header.dispatch('click', {}); assert.equal(b.app.window.mode, 'normal');
  b.doc.getElementById('fwMin').dispatch('click'); assert.equal(b.app.window.mode, 'min');
  b.doc.getElementById('fwMax').dispatch('click'); assert.equal(b.app.window.mode, 'max');
  b.doc.getElementById('fwMax').dispatch('click'); assert.equal(b.app.window.mode, 'normal');
  assert.equal(b.F.getMode(), 'normal'); assert.equal(b.F.getState().station, '51201');
  b.F.minimise(); assert.equal(b.F.getMode(), 'min');
});

test('loader: a fetch that ignores its abort and answers late is still dropped by the sequence guard', async () => {
  const F = load(fakeWindow()), I = F._internals, u = ui(), pend = [];
  const fetch = (url) => new Promise((res) => { pend.push((d) => res({ ok: true, status: 200, json: () => Promise.resolve(d) })); });   // no signal handling at all
  const st = { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  const L = I.createLoader({ fetch, now: () => 1, replaceState: () => {}, swanStations: [], ui: u.ui }, st);
  const p1 = L.load({}); const p2 = L.load({ station: '46001' }); await settle();
  pend[1](payload({ station: '46001' })); await p2; await settle();
  pend[0](payload({ station: '51201' })); await p1; await settle();
  assert.equal(u.applied.length, 1); assert.equal(u.applied[0][0].station, '46001', 'the late first answer never lands');
  assert.equal(st.station, '46001');
});

test('graphs: hidden (or replaced) while Chart.js is still loading -> nothing is built for that request', async () => {
  const win = fakeWindow(), page = buildPage(win), F = load(win), I = F._internals, Chart = fakeChart();
  let release; const loadChartJs = () => new Promise((r) => { release = r; });
  page.graphs.hidden = false;
  const G = I.createForecastGraphs(graphDeps(win, page, Chart, { loadChartJs }));
  const p = G.setData(payload().graph_data); await settle();
  page.graphs.hidden = true; release(); await p; await settle();
  assert.equal(Chart.made.length, 0, 'hidden before Chart.js arrived: not built');
  page.graphs.hidden = false; const p2 = G.show(); await settle(); release(); await p2; await settle();   // show builds it (the data is still dirty)
  assert.equal(Chart.made.length, 3);
});

test('G16-B P2-4: the loader writes the address against the saved settings, so a reload comes back to the view on screen', async () => {
  const F = load(fakeWindow()), I = F._internals, fs_ = fetchStub(), u = ui(), urls = [];
  const st = { station: '46001', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  const L = I.createLoader({ fetch: fs_.fetch, now: () => 1, replaceState: (x) => urls.push(x), swanStations: [], ui: u.ui, saved: () => ({ tz: 'Pacific/Honolulu', unit: 'Metric' }) }, st);
  L.load({}); await settle();
  assert.equal(urls[urls.length - 1], '?station=46001&tz=&unit=US');
  const b = boot({ local: { 'allshore.settings.v1': { tz: 'Pacific/Honolulu', unit: 'Metric' } }, search: '?station=51201' });
  await settle(); b.fs_.last().release(payload()); await settle();
  assert.equal(b.win.history.urls[b.win.history.urls.length - 1], '?station=51201&unit=Metric&tz=Pacific%2FHonolulu'.replace('&unit=Metric&tz=Pacific%2FHonolulu', ''), 'stored settings on screen: nothing to name');
  b.doc.fire('allshore:station', { detail: { sid: '46001', source: 'map' } }); await settle();
  assert.equal(b.win.history.urls[b.win.history.urls.length - 1], '?station=46001&tz=', 'a map pick back to Buoy Local is named against the saved zone');
  assert.deepEqual(b.win.localStorage.read('allshore.settings.v1'), { tz: 'Pacific/Honolulu', unit: 'Metric' }, 'the saved settings are untouched by a pick');
});

test('G16-B P2-2: a hidden opener (a favourites button whose list closed) never takes the focus back; the trigger does', async () => {
  const b = boot({});
  await settle(); b.fs_.last().release(payload()); await settle();
  const fav = b.doc.createElement('button'); b.page.header.appendChild(fav); fav.offsetParent = null;   // hidden with its list
  b.doc.activeElement = fav;
  b.app.expand(); assert.equal(b.app.window.opener, null);
  b.doc.fire('keydown', { key: 'Escape' });
  assert.equal(b.app.window.mode, 'min'); assert.equal(b.doc.activeElement, b.page.trigger);
  b.page.trigger.focus(); b.app.expand(); assert.equal(b.app.window.opener, b.page.trigger);
  b.doc.fire('keydown', { key: 'Escape' }); assert.equal(b.doc.activeElement, b.page.trigger);
});

test('G16-B P2-1: every forecast that lands is announced with the table\'s zone, station and model (the overlay follows)', async () => {
  const b = boot({}); const seen = [];
  b.doc.addEventListener('allshore:forecast', (e) => seen.push(e.detail));
  await settle(); b.fs_.last().release(payload({ tz_label: 'Pacific/Honolulu' })); await settle();
  assert.deepEqual(seen, [{ station: '51201', tz: 'Pacific/Honolulu', model: 'GFS', view: 'Table' }]);
  b.page.tz.value = 'UTC'; b.page.tz.dispatch('change'); await settle(); b.fs_.last().release(payload({ tz_label: 'UTC', model: 'SWAN' })); await settle();
  assert.deepEqual(seen[1], { station: '51201', tz: 'UTC', model: 'SWAN', view: 'Table' });
  assert.equal(b.doc.getElementById('fwCycle').title, 'SWAN · updated 20260926 12 UTC'.replace('updated ', 'updated ').replace('SWAN · updated 20260926 12 UTC', b.doc.getElementById('fwCycle').textContent), 'titles carry the full text');
});

// ---- G16 reviewer A ----
test('G16-A M5: a forecast served from the cache supersedes a fetch still in flight; the late answer never lands', async () => {
  const F = load(fakeWindow()), I = F._internals, fs_ = fetchStub(), u = ui();
  const st = { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  const L = I.createLoader({ fetch: fs_.fetch, now: () => 1, replaceState: () => {}, swanStations: [], ui: u.ui }, st);
  const pB = L.load({ station: '46001' }); await settle(); fs_.last().release(payload({ station: '46001' })); await pB;   // B cached
  const pA = L.load({ station: '51201' }); await settle();                                                               // A in flight
  await L.load({ station: '46001' });                                                                                    // B from the cache
  assert.equal(u.applied[u.applied.length - 1][0].station, '46001'); assert.equal(fs_.calls[1].aborted, true, 'the pending fetch is dropped');
  fs_.calls[1].release(payload({ station: '51201' })); await pA; await settle();
  assert.equal(u.applied[u.applied.length - 1][0].station, '46001', 'the late answer for A never overwrote B');
  assert.equal(st.station, '46001');
});

test('G16-A P2-1 / P2-2: a table-less error is shown in the visible box with Retry in BOTH views, is never cached, and takes the loading placeholder with it', async () => {
  const b = boot({ search: '?station=51201&view=Graph' });
  await settle(); assert.ok(b.doc.getElementById('forecastLoading').parentNode, 'placeholder present while loading');
  b.fs_.last().release(payload({ table_html: null, graph_data: null, error: 'Forecast temporarily unavailable' })); await settle();
  const box = b.doc.getElementById('fwError');
  assert.equal(box.hidden, false); assert.ok(box.textContent.startsWith('Forecast temporarily unavailable'), box.textContent);
  assert.equal(box.querySelectorAll('button').length, 1, 'a Retry button');
  assert.equal(b.doc.getElementById('forecastLoading').parentNode, null, 'the placeholder is gone');
  assert.equal(b.page.table.hidden, true, 'Graph view: the table area stays hidden'); assert.equal(b.Chart.made.length, 0);
  const n = b.fs_.calls.length;
  box.querySelector('button').dispatch('click'); await settle();
  assert.equal(b.fs_.calls.length, n + 1, 'Retry fetches again (the error was not cached)');
  b.fs_.last().release(payload()); await settle();
  assert.equal(box.hidden, true, 'a good forecast clears the error'); assert.equal(b.app.loader.cache.size, 1);
  b.page.viewBar.querySelectorAll('[data-view]')[0].dispatch('click');
  const c = boot({}); await settle(); c.fs_.last().release(payload({ table_html: null, error: 'No SWAN forecast available for 51201' })); await settle();
  assert.equal(c.doc.getElementById('fwError').hidden, false, 'Table view too'); assert.equal(c.page.table.textContent, 'No SWAN forecast available for 51201');
  assert.equal(c.app.loader.cache.size, 0, 'not cached');
});

test('G16-A P2-3: data that lands while Chart.js is still loading is drawn (the latest), and data set to null while loading destroys the charts', async () => {
  const win = fakeWindow(), page = buildPage(win), F = load(win), I = F._internals, Chart = fakeChart();
  let gate = null; const loadChartJs = () => gate || Promise.resolve();      // Chart.js: pending while a gate is held, instant afterwards
  const hold = () => { let r; gate = new Promise((res) => { r = res; }); return () => { gate = null; r(); }; };
  page.graphs.hidden = false;
  const G = I.createForecastGraphs(graphDeps(win, page, Chart, { loadChartJs }));
  const gd1 = payload().graph_data, gd2 = payload({ graph_data: Object.assign(payload().graph_data, { units: 'm' }) }).graph_data;
  let release = hold();
  const p1 = G.setData(gd1); await settle();
  const p2 = G.setData(gd2); await settle();                                    // a newer forecast before Chart.js arrived
  release(); await p1; await p2; await settle();
  assert.equal(Chart.made.length, 3, 'built once, for the latest data'); assert.equal(Chart.made[0].options.scales.y.title.text, 'Height (m)');
  release = hold();
  const p3 = G.setData(gd1); await settle(); G.setData(null); release(); await p3; await settle();
  assert.equal(G.charts().length, 0, 'null data while loading: nothing left'); assert.ok(Chart.made.slice(0, 3).every((c) => c.destroyed));
});

test('G16-A M1 / M2: an error payload after a good one destroys the charts; the next load clears the error box', async () => {
  const b = boot({ search: '?view=Graph' });
  await settle(); b.fs_.last().release(payload()); await settle(); b.app.expand(); await settle();
  assert.equal(b.Chart.made.length, 3);
  b.doc.fire('allshore:station', { detail: { sid: '46001', source: 'map' } }); await settle();
  b.fs_.last().release(payload({ station: '46001', table_html: null, graph_data: null, error: 'down' })); await settle();
  assert.ok(b.Chart.made.every((c) => c.destroyed), 'stale charts never outlive their data'); assert.equal(b.doc.getElementById('fwError').hidden, false);
  b.doc.fire('allshore:station', { detail: { sid: '51202', source: 'map' } }); await settle();
  assert.equal(b.doc.getElementById('fwError').hidden, true, 'a new (fetching) load clears the previous error at once');
  assert.equal(b.doc.getElementById('fwBusy').hidden, false);
});

test('G16-A M3 / M11: a window resize refits the charts through init; expanding in Table view builds no chart and loads no Chart.js', async () => {
  const b = boot({}); b.app.graphs.destroy();
  b.app.expand(); await settle(); b.fs_.last().release(payload()); await settle();   // the forecast lands while expanded in Table view
  assert.equal(b.Chart.made.length, 0, 'Table view: no charts');
  b.page.viewBar.querySelectorAll('[data-view]')[1].dispatch('click'); await settle(); assert.equal(b.Chart.made.length, 3);
  const r0 = b.Chart.made[0].resizes; b.win.fire('resize'); assert.ok(b.Chart.made[0].resizes > r0, 'refitted on a window resize');
  const c = boot({}); c.win.Chart = undefined; c.app.graphs.destroy();
  const winC = c.win; const F2 = c.F; let chartLoads = 0;
  const app2 = F2.init({ window: winC, initial: { station: '51201', swan_stations: [] }, loadChartJs: () => { chartLoads++; return Promise.resolve(); }, fetch: c.fs_.fetch });
  await settle(); c.fs_.last().release(payload()); await settle(); app2.expand(); await settle();
  assert.equal(chartLoads, 0, 'no Chart.js download for the Table view');
});

test('G16-A M6: a saved geometry is clamped into a smaller viewport on restore', () => {
  const win = fakeWindow({ width: 1280, height: 800, session: { 'allshore.forecastWin.v1': { mode: 'normal', prev: 'normal', x: 2000, y: 1500, w: 3000, h: 2000 } } }), page = buildPage(win);
  const { fw } = makeWindow(win, page);
  assert.deepEqual(fw.geom, { x: 8, y: 8, w: 1264, h: 784 }); assert.equal(page.w.style.left, '8px');   // the map is the page: no top bar to clear
});

test('G16-A P3-5: throwing storage accessors do not stop the window; P3-6: the long label form is parsed exactly; P3-7: a double-click on the chip only expands, the top bar height follows a resize', async () => {
  const win = fakeWindow(), page = buildPage(win), F = load(win), fs_ = fetchStub();
  Object.defineProperty(win, 'sessionStorage', { get() { throw new Error('SecurityError'); } });
  Object.defineProperty(win, 'localStorage', { get() { throw new Error('SecurityError'); } });
  const app = F.init({ window: win, initial: { station: '51201', swan_stations: [] }, fetch: fs_.fetch, loadChartJs: () => Promise.resolve() });
  assert.ok(app, 'started'); await settle(); fs_.last().release(payload()); await settle();
  assert.equal(win.document.getElementById('stationCurrent').textContent, '51201');
  app.window.expand(); assert.equal(app.window.mode, 'normal');
  const I = F._internals, d = I.parseLabel('Saturday, September 26, 2026 2:00 PM');
  assert.deepEqual([d.getFullYear(), d.getMonth(), d.getDate(), d.getHours(), d.getMinutes()], [2026, 8, 26, 14, 0]);
  assert.equal(I.parseLabel('Wednesday, January 7, 2026 12:00 AM').getHours(), 0); assert.equal(I.parseLabel('Monday, December 1, 2025 12:30 PM').getHours(), 12);
  const b = boot({}); await settle(); b.fs_.last().release(payload()); await settle();
  b.page.header.dispatch('click', {}); b.page.header.dispatch('dblclick', {});
  assert.equal(b.app.window.mode, 'normal', 'a double-click on the chip expands, never maximises');
  b.app.window.setMode('normal'); b.app.window._place({ x: 100, y: 100, w: 600, h: 400 });
  b.win.innerWidth = 500; b.win.fire('resize'); assert.equal(b.app.window.geom.x, 500 - 8 - 484, 'a resize clamps the window into the viewport (no top bar to allow for)');
});

test('G16 re-review: a transport failure removes the loading placeholder and names the station of the address bar; a page-rendered error is not cached and gets Retry; settings are saved before the load (the address stays minimal)', async () => {
  const b = boot({ search: '?station=46001' });
  await settle(); assert.ok(b.doc.getElementById('forecastLoading').parentNode);
  b.fs_.last().fail(); await settle();
  assert.equal(b.doc.getElementById('forecastLoading').parentNode, null, 'placeholder gone on a transport failure');
  assert.equal(b.doc.getElementById('stationCurrent').textContent, '46001 — Gulf of Alaska', 'the header names the station the address bar shows');
  const c = boot({ initial: { inline: true, error: 'No .bull file found for 51201', model: 'GFS', swan_available: true } });
  c.page.table.innerHTML = '\n   \n';
  await settle();
  assert.equal(c.fs_.calls.length, 0); assert.equal(c.app.loader.cache.size, 0, 'a page-rendered error is not cached');
  assert.equal(c.doc.getElementById('fwError').hidden, false); assert.equal(c.doc.getElementById('fwError').querySelectorAll('button').length, 1, 'with Retry');
  const d = boot({}); await settle(); d.fs_.last().release(payload()); await settle();
  d.page.unit.value = 'Metric'; d.page.unit.dispatch('change');
  assert.equal(d.win.history.urls[d.win.history.urls.length - 1], '?station=51201', 'saved before the load: the address names nothing');
  assert.deepEqual(d.win.localStorage.read('allshore.settings.v1'), { tz: '', unit: 'Metric' });
});

test('PR C: the first load takes the <head>\'s early response when the query matches (no second request), only once', async () => {
  let release; const p = new Promise((res) => { release = res; });
  const q = 'station=51201&tz=&unit=US&model=GFS&compact=1';
  const b = boot({ search: '?station=51201', early: { forecast: { q, p } } });
  await settle();
  assert.equal(b.fs_.calls.length, 0, 'no fetch: the early request is used');
  release({ ok: true, status: 200, json: () => Promise.resolve(payload()) }); await settle();
  assert.equal(b.doc.getElementById('stationCurrent').textContent, '51201 — Waimea Bay, HI');
  b.app.loader.cache.clear(); b.app.loader.load({}); await settle();
  assert.equal(b.fs_.calls.length, 1, 'a later load fetches as usual (the early response is taken once)');
});

test('PR C: an early request for another query is ignored (the first load fetches its own); a newer pick supersedes a slow early answer', async () => {
  let release; const p = new Promise((res) => { release = res; });
  const b = boot({ search: '?station=51201&unit=Metric', early: { forecast: { q: 'station=51201&tz=&unit=US&model=GFS&compact=1', p } } });
  await settle();
  assert.equal(b.fs_.calls.length, 1); assert.equal(b.fs_.param(b.fs_.last(), 'unit'), 'Metric');
  let r2; const p2 = new Promise((res) => { r2 = res; });
  const c = boot({ search: '?station=51201', early: { forecast: { q: 'station=51201&tz=&unit=US&model=GFS&compact=1', p: p2 } } });
  await settle(); assert.equal(c.fs_.calls.length, 0);
  c.app.loader.load({ station: '46001', tz: '' }); await settle();
  c.fs_.last().release(payload({ station: '46001', swan_available: false })); await settle();
  r2({ ok: true, status: 200, json: () => Promise.resolve(payload()) }); await settle();
  assert.equal(c.app.state.station, '46001');
  assert.equal(c.doc.getElementById('stationCurrent').textContent, '46001 — Gulf of Alaska', 'the slow early answer never overwrites the newer pick');
  release({ ok: true, status: 200, json: () => Promise.resolve(payload()) });
});

test('PR C: the arrow keys on the focused resize handle resize the normal window (16 px, 64 with Shift), clamped, saved; not when maximised', async () => {
  const b = boot({}); await settle(); b.fs_.last().release(payload()); await settle();
  const fw = b.app.window; fw.setMode('normal'); fw._place({ x: 100, y: 100, w: 600, h: 400 });
  const h = b.doc.getElementById('fwResize');
  const key = (k, shift) => { let prevented = false; h.dispatch('keydown', { key: k, shiftKey: !!shift, preventDefault: () => { prevented = true; } }); return prevented; };
  assert.equal(key('ArrowRight'), true); assert.deepEqual([fw.geom.w, fw.geom.h], [616, 400]);
  key('ArrowDown', true); assert.deepEqual([fw.geom.w, fw.geom.h], [616, 464]);
  key('ArrowLeft'); key('ArrowUp'); assert.deepEqual([fw.geom.w, fw.geom.h], [600, 448]);
  for (let i = 0; i < 100; i++) key('ArrowLeft', true);
  assert.equal(fw.geom.w, 360, 'never below the minimum width');
  assert.equal(JSON.parse(b.win.sessionStorage.getItem('allshore.forecastWin.v1')).w, 360, 'saved');
  assert.equal(key('Enter'), false, 'other keys pass through');
  fw.setMode('max'); const w0 = fw.geom.w; key('ArrowRight'); assert.equal(fw.geom.w, w0, 'maximised: no resize');
});

test('G17: the keyboard resize resizes the charts too and does nothing minimised or in phone mode; an early HTTP error shows Retry, which fetches afresh', async () => {
  const b = boot({}); await settle(); b.fs_.last().release(payload()); await settle();
  const fw = b.app.window; fw.setMode('normal'); fw._place({ x: 100, y: 100, w: 600, h: 400 });
  let resized = 0; const orig = b.app.graphs.resize; b.app.graphs.resize = () => { resized++; return orig(); };
  b.app.setView('Graph'); await settle(); resized = 0;
  const h = b.doc.getElementById('fwResize'), key = (k) => h.dispatch('keydown', { key: k, preventDefault: () => {} });
  key('ArrowRight'); assert.equal(resized, 1, 'the charts follow a keyboard resize');
  fw.setMode('min'); const g = { ...fw.geom }; key('ArrowRight'); assert.deepEqual(fw.geom, g, 'minimised: nothing');
  fw.setMode('normal'); b.win.matchMedia = () => ({ matches: true }); key('ArrowRight'); assert.deepEqual(fw.geom, g, 'phone mode: nothing');
  const c = boot({ search: '?station=51201', early: { forecast: { q: 'station=51201&tz=&unit=US&model=GFS&compact=1', p: Promise.resolve({ ok: false, status: 503, json: () => Promise.resolve({}) }) } } });
  await settle(); assert.equal(c.fs_.calls.length, 0);
  const box = c.doc.getElementById('fwError'); assert.equal(box.hidden, false);
  box.querySelectorAll('button')[0].dispatch('click'); await settle();
  assert.equal(c.fs_.calls.length, 1, 'Retry fetches afresh (the early response is spent)');
});

test('plan section 26: the charts draw only the swells present (legend on top), boxes at least 300 px, the period axis from a data floor', async () => {
  const Chart = fakeChart(), page = buildPage(fakeWindow()), F = load(fakeWindow()), I = F._internals;
  const labels = ['9/26/26 1:00 AM', '9/26/26 2:00 AM', '9/26/26 3:00 AM'];
  const gd = { labels, units: 'ft', swells: ['s1', 's3'],
    height: { s1: [2, 3, 4], s2: [null, null, null], s3: [1, 1, 1], s4: [], s5: [], s6: [], combined: [3, 4, 5] },
    period: { s1: [11, 12, 13], s2: [], s3: [15, 16, 14], s4: [], s5: [], s6: [] },
    direction: { s1: [300, 310, 320], s2: [], s3: [180, 190, 200], s4: [], s5: [], s6: [] } };
  const G = I.createForecastGraphs({ host: page.graphs, boxes: page.graphs.children, canvases: page.graphs.children.map((b) => b.children[0]), rangeBar: null,
    loadChartJs: () => Promise.resolve(), getChart: () => Chart, storage: fakeWindow().sessionStorage, bodyHeight: () => 1300, visible: () => true });
  await G.setData(gd); await settle();
  const [h, p, d] = Chart.made;
  assert.deepEqual(h.data.datasets.map((x) => x.label), ['Swell 1', 'Swell 3', 'Combined']);
  assert.deepEqual(p.data.datasets.map((x) => x.label), ['Swell 1', 'Swell 3']);
  assert.equal(h.options.plugins.legend.position, 'top');
  assert.equal(page.graphs.children[0].style.height, '420px', 'three boxes share a tall body');
  assert.equal(p.options.scales.y.min, 5, 'one step (5 s for a 16.8 s top) under the shortest period (11 s), rounded down'); assert.equal(p.options.scales.y.max, 20);
  assert.equal(I.periodFloor(3, 1), 2); assert.equal(I.periodFloor(0.5, 1), 0); assert.equal(I.periodFloor(NaN, 2), 0);
  assert.deepEqual(I.swellKeys({}), ['s1', 's2', 's3', 's4', 's5', 's6'], 'an old payload: all six');
  assert.deepEqual(I.swellKeys({ swells: ['s2', 'bogus'] }), ['s2']); assert.deepEqual(I.swellKeys({ swells: [] }).length, 6);
});

test('plan section 26: in Table view the window is no wider than its table (drag, keys and maximise included); Graph view and the chip are not capped', async () => {
  const b = boot({}); await settle();
  const t = { scrollWidth: 900 };
  const orig = b.page.table.querySelector.bind(b.page.table);
  b.page.table.querySelector = (sel) => (sel === 'table' ? t : orig(sel));
  b.fs_.last().release(payload()); await settle();
  const fw = b.app.window;
  assert.equal(fw.maxW, 900 + 40); assert.equal(b.page.w.style.maxWidth, '', 'minimised: the chip keeps its CSS width');
  fw.setMode('normal'); await settle();
  assert.equal(b.page.w.style.maxWidth, '940px');
  fw._place({ x: 100, y: 100, w: 600, h: 400 });
  const h = b.doc.getElementById('fwResize');
  for (let i = 0; i < 20; i++) h.dispatch('keydown', { key: 'ArrowRight', shiftKey: true, preventDefault: () => {} });
  assert.equal(fw.geom.w, 940, 'the keyboard never grows it past the table');
  fw._place({ x: 100, y: 100, w: 1150, h: 400 });                           // the viewer's own width, wider than the table
  fw.setMaxWidth(900); assert.equal(fw.geom.w, 1150, 'a new cap never rewrites the saved width');
  fw.clamp(); assert.equal(fw.geom.w, 1150, 'nor does a clamp'); fw.setMaxWidth(940);
  fw.setMode('max'); assert.equal(b.page.w.style.maxWidth, '940px', 'maximised: full height, the table width');
  fw.setMode('min'); assert.equal(b.page.w.style.maxWidth, '', 'the chip keeps its own CSS width');
  fw.setMode('normal'); b.app.setView('Graph'); await settle();
  assert.equal(fw.maxW, 0); assert.equal(b.page.w.style.maxWidth, '', 'Graph view uses the full width');
  const cg = b.F._internals.clampGeometry({ x: 1400, y: 0, w: 1200, h: 300 }, 1500, 900, 0, null, 700);
  assert.equal(cg.w, 1200, 'the width stays the viewer\'s'); assert.equal(cg.x, 1500 - 8 - 700, 'x keeps the capped window on screen');
});

test('G18a: the date labels are thinned so they never overlap on a narrow chart, and follow the range in view', () => {
  const I = load(fakeWindow())._internals, parsed = [], mids = [];
  for (let i = 0; i < 385; i++) { const d = new Date(2026, 8, 26, 14 + i); parsed.push(d); if (d.getHours() === 0) mids.push(i); }
  const shown = (scale) => parsed.map((d, i) => I.dateTick(scale, parsed, mids, i)).filter(Boolean);
  assert.equal(shown({ width: 1100, min: 0, max: 384 }).length, 16, 'wide: every midnight');
  const narrow = shown({ width: 300, min: 0, max: 384 });
  assert.ok(narrow.length <= Math.floor(300 / 44) + 1 && narrow.length >= 4, String(narrow.length));
  assert.equal(shown({ width: 300, min: 0, max: 71 }).length, 3, 'the 3-day range: its three midnights fit');
  assert.equal(I.dateTick({ width: 1100, min: 0, max: 384 }, parsed, mids, 1), '', 'not midnight: no label');
});

test('G18a: a Graph-view width survives a trip through Table view; a SWAN-to-GFS switch widens the window again', async () => {
  const b = boot({}); await settle();
  let tw = 800; const orig = b.page.table.querySelector.bind(b.page.table);
  b.page.table.querySelector = (sel) => (sel === 'table' ? { scrollWidth: tw } : orig(sel));
  b.fs_.last().release(payload()); await settle();
  const fw = b.app.window; fw.setMode('normal'); fw._place({ x: 50, y: 100, w: 1150, h: 400 });
  b.app.setView('Table'); assert.equal(b.page.w.style.maxWidth, '840px'); assert.equal(fw.geom.w, 1150);
  b.app.setView('Graph'); assert.equal(b.page.w.style.maxWidth, ''); assert.equal(fw.geom.w, 1150, 'the viewer\'s width is back');
  b.app.setView('Table'); tw = 950; b.app.loader.load({ model: 'GFS', station: '46001' }); await settle();
  b.fs_.last().release(payload({ station: '46001', swan_available: false })); await settle();
  assert.equal(b.page.w.style.maxWidth, '990px', 'a wider table widens the window (the width was never cut)');
});

// ---- plan section 26 (D2): the picker heading, two windows, the live-buoy window ----
test('D2: the forecast title is the picker label (#stationCurrent); FloatingWindow takes its own key and default mode', async () => {
  const b = boot({ search: '?station=46001' }); await settle(); b.fs_.last().release(payload({ station: '46001', swan_available: false })); await settle();
  assert.equal(b.doc.getElementById('stationCurrent').textContent, '46001 — Gulf of Alaska');
  const win = fakeWindow(), page = buildPage(win), I = load(win)._internals;
  const a = new I.FloatingWindow({ el: page.w, header: page.header, storage: win.sessionStorage, win, key: 'k.a', defaultMode: 'normal' });
  const c = new I.FloatingWindow({ el: page.live, header: page.lwHeader, storage: win.sessionStorage, win, key: 'k.c' });
  assert.equal(a.mode, 'normal'); assert.equal(c.mode, 'min');
  a.setMode('max'); c.setMode('normal');
  assert.equal(JSON.parse(win.sessionStorage.getItem('k.a')).mode, 'max'); assert.equal(JSON.parse(win.sessionStorage.getItem('k.c')).mode, 'normal', 'separate keys');
  assert.equal(win.sessionStorage.getItem(I.WINDOW_KEY), null, 'neither touched the forecast key');
  const d = new I.FloatingWindow({ el: page.w, header: page.header, storage: win.sessionStorage, win, key: 'k.a', defaultMode: 'normal' });
  assert.equal(d.mode, 'max', 'a saved mode wins over the default');
  assert.equal(page.w.style['--topbar-h'], undefined, 'no top bar variable any more');
});

test('D2: the live-buoy window opens expanded on a pick, minimises to a chip, closes from any mode, and announces every change', async () => {
  const win = fakeWindow(), page = buildPage(win), F = load(win), closes = [], events = [];
  win.document.addEventListener('allshore:livewin', (e) => events.push([e.detail.open, e.detail.mode]));
  const lw = F.createLiveWindow({ window: win, onClose: () => closes.push(1) });
  assert.ok(lw && lw.window); assert.equal(lw.isOpen(), false); assert.equal(lw.window.mode, 'normal', 'a new tab: a window, never a chip at first');
  lw.open(); assert.equal(page.live.hidden, false); assert.equal(lw.isOpen(), true);
  assert.deepEqual(events[events.length - 1], [true, 'normal']);
  page.lwMin.dispatch('click'); assert.equal(lw.window.mode, 'min'); assert.ok(page.live.classList.contains('fw-min'));
  assert.equal(page.lwMin.getAttribute('aria-expanded'), 'false'); assert.equal(page.lwMin.getAttribute('aria-label'), 'Expand live buoy');
  assert.deepEqual(events[events.length - 1], [true, 'min']);
  page.lwHeader.dispatch('click', { target: page.lwHeader }); assert.equal(lw.window.mode, 'normal', 'a click on the chip expands it');
  page.lwMin.dispatch('click'); lw.open(); assert.equal(lw.window.mode, 'normal', 'a new pick expands a parked chip');
  page.lwMin.dispatch('click'); page.lwClose.dispatch('click');
  assert.equal(page.live.hidden, true); assert.equal(closes.length, 1); assert.deepEqual(events[events.length - 1], [false, 'min'], 'closed from the chip');
  lw.open(); assert.equal(lw.window.mode, 'normal');
  lw.window.setMode('max'); assert.equal(lw.window.mode, 'normal', 'no maximised state (owner: no gain)');
  lw.window._expandedAt = 0; page.lwHeader.dispatch('dblclick', { target: page.lwHeader }); assert.equal(lw.window.mode, 'normal', 'a double-click does not maximise it either');
  win.sessionStorage.setItem(F._internals.LIVE_WINDOW_KEY, JSON.stringify({ mode: 'max', prev: 'max' }));
  assert.equal(F.createLiveWindow({ window: win }).window.mode, 'normal', 'a saved max (an older build) opens as a window'); lw.window._save();   // (that probe instance never saves; put the live one's state back)
  lw.close(); assert.equal(closes.length, 2);
  assert.equal(JSON.parse(win.sessionStorage.getItem(F._internals.LIVE_WINDOW_KEY)).mode, 'normal');
  assert.equal(F.createLiveWindow({ window: fakeWindow() }), null, 'no live markup: nothing');
});

test('D2: on a phone the live window neither drags nor resizes; Escape in the page closes an OPEN live window first, leaves a parked chip alone', async () => {
  const win = fakeWindow({ phone: true }), page = buildPage(win), F = load(win);
  const lw = F.createLiveWindow({ window: win }); lw.open();
  page.lwHeader.dispatch('pointerdown', ptr(10, 10)); page.lwHeader.dispatch('pointermove', ptr(80, 60)); page.lwHeader.dispatch('pointerup', ptr(80, 60));
  assert.equal(lw.window.geom, null, 'phone: no drag');
  const b = boot({}); await settle(); b.fs_.last().release(payload()); await settle();
  const live = b.F.createLiveWindow({ window: b.win });
  const liveOpen = () => live.isOpen() && live.window.mode !== 'min';
  b.app.expand(); live.open();
  assert.equal(liveOpen(), true);
  b.page.lwMin.dispatch('click'); assert.equal(liveOpen(), false, 'a parked chip does not count as open');
});

test('G18b-B P3-3: a phone-width load keeps a saved desktop geometry as it is (not cut to 360 px, not rewritten); widening clamps it', () => {
  const saved = { 'allshore.forecastWin.v1': { mode: 'normal', prev: 'normal', x: 300, y: 200, w: 1150, h: 600 } };
  const win = fakeWindow({ width: 375, height: 812, phone: true, session: saved }), page = buildPage(win);
  const { fw } = makeWindow(win, page);
  assert.deepEqual(fw.geom, { x: 300, y: 200, w: 1150, h: 600 }, 'untouched in memory');
  assert.equal(page.w.style.left, undefined, 'and not applied (CSS owns the phone layout)');
  fw.setMode('min'); fw.setMode('normal');                                    // mode changes save: still the desktop box
  assert.equal(JSON.parse(win.sessionStorage.getItem('allshore.forecastWin.v1')).w, 1150);
  win.phone = false; win.innerWidth = 1024; win.innerHeight = 768; win.fire('resize');
  assert.deepEqual([fw.geom.w, fw.geom.x], [1008, 8], 'back on a desktop: clamped into the viewport, not 360 wide');
});

