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
  assert.deepEqual([page.graphs.children[0].style.height, page.graphs.children[1].style.height], ['180px', '180px'], 'boxes fitted to the body');
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
    topBarHeight: () => 56, onMode: (m) => modes.push(m), onResize: () => resizes.push(1) }, over || {}));
  return { fw, modes, resizes, I };
}
const ptr = (x, y, extra) => Object.assign({ clientX: x, clientY: y, pointerId: 1, button: 0 }, extra || {});

test('window: a new tab starts minimised; modes toggle classes and are saved; expand returns to the mode before minimising', () => {
  const win = fakeWindow(), page = buildPage(win), { fw, modes } = makeWindow(win, page);
  assert.equal(fw.mode, 'min'); assert.ok(page.w.classList.contains('fw-min')); assert.equal(page.w.style['--topbar-h'], '56px');
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
  fw.toggleMax(); page.header.dispatch('pointerdown', ptr(500, 320)); page.header.dispatch('pointermove', ptr(0, 0)); page.header.dispatch('pointerup', ptr(0, 0));
  assert.equal(page.w.style.left, '8px', 'no drag while maximised');
  win.innerWidth = 700; win.innerHeight = 500; fw.setMode('normal'); win.fire('resize');
  const g = fw.geom; assert.ok(g.x + g.w <= 692 && g.y + g.h <= 492 && g.y >= 64, JSON.stringify(g));
});

test('window: on a phone nothing drags or resizes and geometry is left to CSS; a double-click on the header toggles maximise on desktops', () => {
  const win = fakeWindow({ phone: true, width: 375, height: 812 }), page = buildPage(win), { fw } = makeWindow(win, page);
  fw.expand();
  page.header.dispatch('pointerdown', ptr(100, 100)); page.header.dispatch('pointermove', ptr(50, 50)); page.header.dispatch('pointerup', ptr(50, 50));
  assert.equal(page.w.style.left, undefined); assert.equal(fw.geom, null);
  page.header.dispatch('dblclick', {}); assert.equal(fw.mode, 'normal');
  const win2 = fakeWindow(), page2 = buildPage(win2), r = makeWindow(win2, page2);
  r.fw.expand(); page2.header.dispatch('dblclick', {}); assert.equal(r.fw.mode, 'max'); page2.header.dispatch('dblclick', {}); assert.equal(r.fw.mode, 'normal');
});

// ---- init: the page wiring ----
function boot(opts) {
  const win = fakeWindow(opts), page = buildPage(win), F = load(win), fs_ = fetchStub(), Chart = fakeChart();
  win.Chart = Chart;
  const closes = [];
  const app = F.init({ window: win, initial: Object.assign({ station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table', swan_available: true, swan_stations: ['51201', '51202'] }, (opts && opts.initial) || {}),
    stationLabel: (sid) => { const o = page.sel.options.find((x) => x.value === sid); return o ? o.textContent : sid; },
    loadChartJs: () => Promise.resolve(), fetch: fs_.fetch, closeLivePanel: () => { closes.push(1); page.live.style.display = 'none'; }, liveOpen: () => page.live.style.display !== 'none' });
  return { win, page, F, fs_, Chart, app, closes, doc: win.document };
}

test('init: fetches the URL\'s station into the minimised window; the first forecast replaces the loading placeholder and fills title, cycle, meta and table', async () => {
  const b = boot({ search: '?station=51201&view=Graph' });
  assert.ok(b.app); assert.equal(b.app.window.mode, 'min'); assert.equal(b.app.state.view, 'Graph');
  await settle(); assert.equal(b.fs_.calls.length, 1); assert.equal(b.fs_.param(b.fs_.last(), 'compact'), '1');
  assert.equal(b.doc.getElementById('fwBusy').hidden, false);
  b.fs_.last().release(payload()); await settle();
  assert.equal(b.doc.getElementById('fwTitle').textContent, '51201 — Waimea Bay, HI');
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
  assert.equal(b.doc.getElementById('fwTitle').textContent, '46001 — Gulf of Alaska');
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
  assert.equal(b.win.history.urls[b.win.history.urls.length - 1], '?station=51201&unit=Metric&model=SWAN&view=Graph');
  b.page.trigger.focus(); b.app.expand(); assert.equal(b.doc.activeElement, b.page.header);
  b.page.live.style.display = 'block';
  b.doc.fire('keydown', { key: 'Escape' }); assert.equal(b.closes.length, 1); assert.equal(b.app.window.mode, 'normal', 'the live panel closed first');
  b.doc.fire('keydown', { key: 'Escape' }); assert.equal(b.app.window.mode, 'min'); assert.equal(b.doc.activeElement, b.page.trigger, 'focus back on the opener');
  b.doc.fire('keydown', { key: 'Escape' }); assert.equal(b.app.window.mode, 'min');
  b.doc.activeElement = b.page.sel; b.app.expand(); b.doc.activeElement = b.page.sel;
  b.doc.fire('keydown', { key: 'Escape' }); assert.equal(b.app.window.mode, 'normal', 'Escape outside the window leaves it alone');
});

test('init: a page-rendered forecast (render=full) is shown without a fetch; a server error with no table shows the message; the settings panel toggles', async () => {
  const b = boot({ initial: { inline: true, graph_data: payload().graph_data, graph_header: payload().graph_header, model: 'GFS', swan_available: true } });
  b.page.table.innerHTML = '<table class="x">server</table>';
  await settle(); assert.equal(b.fs_.calls.length, 0);
  const b2 = boot({}); await settle(); b2.fs_.last().release(payload({ table_html: null, graph_data: null, error: 'No SWAN forecast available for 51201' })); await settle();
  assert.equal(b2.page.table.textContent, 'No SWAN forecast available for 51201');
  assert.equal(b2.doc.getElementById('fwError').hidden, true);
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
