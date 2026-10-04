'use strict';
// Plan section 35, the client: the real sky on the charts (the day / twilight / night bands), the compass direction axis,
// the 'Significant Wave Height' series, the Detailed | Summary table mode, the now-row reveal and the sticky Date / Time
// measure, and the older payloads that carry none of it. (The sky strip and the 'now' line were tried and removed:
// owner, 2026-10-03, "not very useful and only create clutter".)
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { fakeWindow, buildPage, fakeChart, fakeCtx } = require('./fakedom');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'forecast.js'), 'utf8');
function load(win) { new Function('window', 'URLSearchParams', SRC)(win, URLSearchParams); return win.AllshoreForecast; }
const tick = () => new Promise((r) => setImmediate(r));
async function settle(n) { for (let i = 0; i < (n || 8); i++) await tick(); }
const I = () => load(fakeWindow())._internals;

function labels(n, startHour) {
  const out = [];
  for (let i = 0; i < n; i++) { const h = ((startHour || 0) + i) % 24; out.push(`Saturday, September 26, 2026 ${(h % 12) || 12}:00 ${h < 12 ? 'AM' : 'PM'}`); }
  return out;
}
function payload(over) {
  const lb = labels(48);
  const g = (v) => ({ s1: lb.map(() => v), s2: lb.map(() => null), s3: [], s4: [], s5: [], s6: [], combined: lb.map(() => v + 1) });
  return Object.assign({
    station: '51201', error: null, table_html: '<table class="forecast-compact"><tr class="now-row"><td class="col-date">Sat</td></tr></table>',
    graph_data: { labels: lb, height: g(2), period: g(10), direction: g(300), units: 'ft', tz: 'Pacific/Honolulu' },
    graph_header: { cycle: '20260926 12 UTC', location: '51201 (21.67N 158.12W)', tz: 'Pacific/Honolulu' },
    model: 'GFS', swan_available: false
  }, over || {});
}
// a 48-slot sky: night to 5, twilight 6, day 7-17, twilight 18, night from 19 (both days)
function sky48() { return labels(48).map((_, i) => { const h = i % 24; return h < 6 || h > 18 ? 'night' : (h === 6 || h === 18) ? 'twilight' : 'day'; }); }
function skyPayload(over) {
  const p = payload(over);
  p.graph_data.sky = sky48();
  p.summary_html = '<table class="forecast-summary"><tr class="sum-day"><td class="col-date">Sat 9/26</td></tr></table>';
  return p;
}
function stubChart(over) {
  return Object.assign({ ctx: fakeCtx(), chartArea: { left: 0, right: 1000, top: 40, bottom: 300 }, scales: { x: { min: undefined, max: undefined, getPixelForValue: (i) => i * 10 } } }, over || {});
}
function fetchStub() {
  const calls = [];
  const fetch = (url, o) => new Promise((res, rej) => {
    const c = { url, release: (d, status) => res({ ok: !status || status < 400, status: status || 200, json: () => Promise.resolve(d) }), fail: () => rej(new TypeError('Failed to fetch')) };
    if (o && o.signal) o.signal.addEventListener('abort', () => rej(Object.assign(new Error('aborted'), { name: 'AbortError' })));
    calls.push(c);
  });
  return { fetch, calls, last: () => calls[calls.length - 1] };
}
function boot(opts) {
  opts = opts || {};
  const win = fakeWindow(opts), page = buildPage(win), F = load(win), fs_ = fetchStub(), Chart = fakeChart();
  win.Chart = Chart;
  const app = F.init({ window: win, initial: Object.assign({ station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table', swan_available: false, swan_stations: ['51201'] }, opts.initial || {}),
    stationLabel: (sid) => sid, loadChartJs: () => Promise.resolve(), fetch: fs_.fetch });
  return { win, page, F, fs_, Chart, app, doc: win.document };
}

// ---- the sky, pure ----
test('skyOf takes the server\'s per-slot sky when it covers every slot, else the 6 PM - 6 AM rule on the labels; skyBands merges runs inside the view', () => {
  const i = I(), parsed = labels(48).map(i.parseLabel);
  assert.deepEqual(i.skyOf({ sky: sky48() }, parsed).slice(4, 9), ['night', 'night', 'twilight', 'day', 'day']);
  assert.deepEqual(i.skyOf({ sky: ['day', 'bogus'] }, parsed.slice(0, 2)), ['day', null], 'an unknown word is no state');
  const rule = i.skyOf({ sky: ['day'] }, parsed);                              // the wrong length: ignored
  assert.deepEqual([rule[0], rule[5], rule[6], rule[17], rule[18]], ['night', 'night', 'day', 'day', 'night']);
  assert.deepEqual(i.skyOf({}, parsed)[23], 'night'); assert.equal(i.hasSky({ sky: sky48() }, 48), true); assert.equal(i.hasSky({ sky: sky48() }, 47), false);
  assert.deepEqual(i.skyBands(['day', 'day', 'twilight', 'night', 'night', 'day'], 0, 6), [{ from: 2, to: 3, kind: 'twilight' }, { from: 3, to: 5, kind: 'night' }]);
  assert.deepEqual(i.skyBands(['night', 'night', 'night'], 1, 3), [{ from: 1, to: 3, kind: 'night' }], 'clipped to the view');
  assert.deepEqual(i.skyBands([null, 'day'], 0, 2), []);
});

test('makeNightShade fills the twilight and night bands of the view (beforeDraw); older payloads keep the hour rule', () => {
  const i = I(), parsed = labels(48).map(i.parseLabel), kinds = i.skyOf({ sky: sky48() }, parsed);
  const ch = stubChart(); i.makeNightShade(parsed, kinds).beforeDraw(ch);
  const rects = ch.ctx.ops.filter((o) => o.op === 'rect');
  assert.deepEqual(rects.map((r) => [r.x, r.w]), [[0, 60], [60, 10], [180, 10], [190, 110], [300, 10], [420, 10], [430, 40]]);   // (the last slot has no right edge: unshaded, as before)
  assert.ok(rects[0].fill.indexOf('30,60,110') >= 0 && rects[1].fill.indexOf('255,170,0') >= 0, 'night blue, twilight amber');
  assert.ok(rects.every((r) => r.y === 40 && r.h === 260), 'the whole plot height');
  const win = stubChart({ scales: { x: { min: 10, max: 20, getPixelForValue: (k) => k * 10 } } }); i.makeNightShade(parsed, kinds).beforeDraw(win);
  assert.deepEqual(win.ctx.ops.filter((o) => o.op === 'rect').map((r) => [r.x, r.w]), [[180, 10], [190, 10]], 'only the bands in view');
  const old = stubChart(); i.makeNightShade(parsed, i.skyOf({}, parsed)).beforeDraw(old);
  assert.deepEqual(old.ctx.ops.filter((o) => o.op === 'rect').map((r) => [r.x, r.w]), [[0, 60], [180, 120], [420, 50]], '6 PM - 6 AM');
});

test('build: one plugin (the shade), the small padding, the compass direction axis, the Significant Wave Height series; no strip, no now line, no timer', async () => {
  const win = fakeWindow(), page = buildPage(win), F = load(win), Chart = fakeChart();
  page.graphs.hidden = false;
  const canvases = ['heightChart', 'periodChart', 'directionChart'].map((id) => win.document.getElementById(id));
  const G = F._internals.createForecastGraphs({ host: page.graphs, boxes: canvases.map((c) => c.parentNode), canvases, rangeBar: page.rangeBar, loadChartJs: () => Promise.resolve(),
    getChart: () => Chart, storage: win.sessionStorage, bodyHeight: () => 400, visible: () => true });
  await G.setData(skyPayload().graph_data); await settle();
  const c = Chart.made;
  assert.ok(c.every((x) => x.config.plugins.map((p) => p.id).join() === 'nightShade'));
  assert.ok(c.every((x) => x.options.layout.padding.top === 8));
  assert.deepEqual(c[0].data.datasets.map((d) => d.label), ['Swell 1', 'Swell 2', 'Swell 3', 'Swell 4', 'Swell 5', 'Swell 6', 'Significant Wave Height']);
  const y = c[2].options.scales.y;
  assert.deepEqual([y.min, y.max, y.ticks.stepSize, y.title.text], [0, 360, 45, 'Direction (from)']);
  assert.deepEqual([0, 45, 90, 180, 270, 360, 100].map((v) => y.ticks.callback(v)), ['N', 'NE', 'E', 'S', 'W', 'N', '']);
  for (const k of ['makeSkyStrip', 'makeNowLine', 'zoneWallClock', 'nowIndex', 'STRIP_H']) assert.ok(!(k in F._internals), k + ' is gone');
  G.destroy();
});

// ---- the Detailed | Summary mode ----
test('the mode buttons show with a summary in Table view only; Summary swaps the tables; the choice lives in the tab, not the address or the state', async () => {
  const b = boot({}); await settle();
  b.fs_.last().release(skyPayload()); await settle();
  assert.equal(b.page.modeBar.hidden, false); assert.equal(b.page.summary.hidden, true); assert.equal(b.page.table.hidden, false);
  assert.ok(b.page.summary.innerHTML.indexOf('forecast-summary') >= 0);
  b.page.modeBar.querySelectorAll('[data-mode]')[1].dispatch('click');
  assert.equal(b.page.summary.hidden, false); assert.equal(b.page.table.hidden, true); assert.equal(b.app.tableMode(), 'summary');
  assert.equal(b.win.sessionStorage.getItem('allshore.tableMode.v1'), 'summary');
  assert.deepEqual(b.page.modeBar.querySelectorAll('[data-mode]').map((x) => x.getAttribute('aria-pressed')), ['false', 'true']);
  assert.equal(b.win.history.urls[b.win.history.urls.length - 1], '?station=51201', 'not in the address bar');
  assert.ok(!('tableMode' in b.F.getState()), 'not in the state');
  b.app.setView('Graph'); assert.equal(b.page.modeBar.hidden, true); assert.equal(b.page.summary.hidden, true); assert.equal(b.page.graphs.hidden, false);
  b.app.setView('Table'); assert.equal(b.page.modeBar.hidden, false); assert.equal(b.page.summary.hidden, false, 'Summary again');
  b.page.modeBar.querySelectorAll('[data-mode]')[0].dispatch('click');
  assert.equal(b.page.table.hidden, false); assert.equal(b.page.summary.hidden, true); assert.equal(b.win.sessionStorage.getItem('allshore.tableMode.v1'), 'detailed');
});

test('a tab that chose Summary starts in it; a forecast without a summary shows the detailed table and hides the buttons, keeping the preference; a failed load empties the summary', async () => {
  const b = boot({ session: { 'allshore.tableMode.v1': 'summary' } }); await settle();
  assert.deepEqual(b.page.modeBar.querySelectorAll('[data-mode]').map((x) => x.getAttribute('aria-pressed')), ['false', 'true'], 'the buttons show the preference before any forecast');
  b.fs_.last().release(skyPayload()); await settle();
  assert.equal(b.page.summary.hidden, false); assert.equal(b.page.table.hidden, true);
  b.app.loader.load({ station: '46001' }); await settle(); b.fs_.last().release(payload({ station: '46001' })); await settle();
  assert.equal(b.page.modeBar.hidden, true); assert.equal(b.page.summary.hidden, true); assert.equal(b.page.table.hidden, false, 'no summary: the detailed table');
  assert.equal(b.app.tableMode(), 'summary', 'the preference survives'); assert.equal(b.page.summary.textContent, '');
  b.app.loader.load({ station: '51201' }); await settle(); b.fs_.last().release(skyPayload()); await settle();
  assert.equal(b.page.summary.hidden, false, 'back with a summary');
  b.app.loader.load({ station: '46001' }); await settle(); b.fs_.last().fail(); await settle();
  assert.equal(b.page.summary.textContent, ''); assert.equal(b.page.modeBar.hidden, true);
});

// ---- the now row and the sticky columns ----
function wireTable(b, over) {
  const row = Object.assign({ getBoundingClientRect: () => ({ top: 500 }), offsetHeight: 20 }, (over && over.row) || {});
  const dateCell = { offsetWidth: 72 };
  const tableEl = { scrollWidth: 900, style: { setProperty(k, v) { this[k] = v; } }, querySelector: (s) => (s === 'td.col-date' ? dateCell : null) };
  const orig = b.page.table.querySelector.bind(b.page.table);
  b.page.table.querySelector = (sel) => (sel === 'table' ? tableEl : sel === 'tr.now-row' ? row : sel === 'thead' ? { offsetHeight: 40 } : orig(sel));
  b.page.body.rect = { left: 0, top: 100, width: 1000, height: 500 }; b.page.body.scrollTop = 0;
  return { row, dateCell, tableEl };
}

test('the now row is scrolled under the frozen header once per station (not on a unit change), only with the window open, in Table view and Detailed mode', async () => {
  const b = boot({}); await settle(); const t = wireTable(b);
  b.fs_.last().release(skyPayload()); await settle();
  assert.equal(b.page.body.scrollTop, 0, 'minimised: deferred');
  b.app.window.setMode('normal');
  assert.equal(b.page.body.scrollTop, 500 - 100 - 40 - 20, 'one row of context under the header');
  b.page.body.scrollTop = 0;
  b.app.loader.load({ unit: 'Metric' }); await settle(); b.fs_.last().release(skyPayload()); await settle();
  assert.equal(b.page.body.scrollTop, 0, 'the same station again: left where the visitor scrolled');
  b.app.loader.load({ station: '46001' }); await settle(); b.fs_.last().release(skyPayload({ station: '46001' })); await settle();
  assert.equal(b.page.body.scrollTop, 340, 'another station: revealed');
  assert.equal(t.tableEl.style['--date-w'], '72px', 'the Time column\'s offset is the Date column\'s width');
  t.dateCell.offsetWidth = 80; b.win.fire('resize'); assert.equal(t.tableEl.style['--date-w'], '80px', 're-measured on a resize');
});

test('the reveal waits for Table view and Detailed mode', async () => {
  const g = boot({ search: '?view=Graph' }); await settle(); wireTable(g); g.app.window.setMode('normal');
  g.fs_.last().release(skyPayload()); await settle();
  assert.equal(g.page.body.scrollTop, 0, 'Graph view: deferred');
  g.app.setView('Table'); assert.equal(g.page.body.scrollTop, 340);
  const s = boot({ session: { 'allshore.tableMode.v1': 'summary' } }); await settle(); wireTable(s); s.app.window.setMode('normal');
  s.fs_.last().release(skyPayload()); await settle();
  assert.equal(s.page.body.scrollTop, 0, 'Summary mode: deferred');
  s.app.setTableMode('detailed'); assert.equal(s.page.body.scrollTop, 340);
  s.page.body.scrollTop = 7; s.app.setTableMode('summary'); s.app.setTableMode('detailed'); assert.equal(s.page.body.scrollTop, 7, 'once only');
});

test('an older payload (no sky, no summary) renders as before: the hour-rule shade, the small padding, no mode buttons', async () => {
  const b = boot({}); await settle();
  b.fs_.last().release(payload()); await settle();
  assert.equal(b.page.modeBar.hidden, true); assert.equal(b.page.summary.hidden, true); assert.equal(b.page.table.hidden, false);
  b.app.window.setMode('normal'); b.app.setView('Graph'); await settle();
  const c = b.Chart.made;
  assert.equal(c.length, 3); assert.deepEqual(c[0].config.plugins.map((p) => p.id), ['nightShade']); assert.equal(c[0].options.layout.padding.top, 8);
  assert.deepEqual([c[2].options.scales.y.min, c[2].options.scales.y.max], [0, 360]);
});
