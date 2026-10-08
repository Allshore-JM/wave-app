'use strict';
// static_ui/tides.js (plan section 38): clocks in a zone (23- and 25-hour days), the axis marks and labels, the chart's
// configuration and plugins, and the view: loading, retries, stale answers, unit / zone / range changes, hidden windows.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Document, fakeChart, fakeCtx, memStorage } = require('./fakedom');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'tides.js'), 'utf8');
const w = {}; new Function('window', SRC)(w);
const T = w.AllshoreTides;
const I = T._internals;
const H = 3600000;

// ---------------------------------------------------------------------------------------------- clocks and marks

test('local midnights in a zone, including 23-, 25- and 23.5-hour days', () => {
  const ny = I.zoneClock('America/New_York');
  const spring = I.midnights(Date.UTC(2026, 2, 7, 18), 2, ny);              // Sat 7 Mar 2026, 1 PM EST
  assert.equal(spring[0], Date.UTC(2026, 2, 7, 5));                          // 00:00 EST
  assert.equal((spring[2] - spring[1]) / H, 23);                             // Sun 8 Mar: clocks go forward
  const fall = I.midnights(Date.UTC(2026, 9, 31, 18), 2, ny);
  assert.equal((fall[2] - fall[1]) / H, 25);                                 // Sun 1 Nov: clocks go back
  const hi = I.midnights(Date.UTC(2026, 9, 8, 20, 17, 42, 500), 16, I.zoneClock('Pacific/Honolulu'));
  assert.equal(hi[0], Date.UTC(2026, 9, 8, 10));
  assert.ok(hi.every((m, k) => k === 0 || m - hi[k - 1] === 24 * H));
  const lh = I.midnights(Date.UTC(2026, 9, 3, 6), 2, I.zoneClock('Australia/Lord_Howe'));
  assert.equal((lh[2] - lh[1]) / H, 23.5);                                   // Sun 4 Oct: a 30-minute step
  const kol = I.zoneClock('Asia/Kolkata');
  assert.equal(I.localMidnightBefore(Date.UTC(2026, 9, 8, 0, 0), kol), Date.UTC(2026, 9, 7, 18, 30));
  const bad = I.zoneClock('Nowhere/Atlantis');
  assert.equal(bad.zone, 'UTC');
  assert.equal(I.localMidnightBefore(Date.UTC(2026, 9, 8, 7, 5), bad), Date.UTC(2026, 9, 8));
  assert.equal(I.zoneClock('constructor').zone, 'UTC');                      // never an Object member
});

test('hour marks follow the local clock across a clock change', () => {
  const ny = I.zoneClock('America/New_York');
  const m = I.midnights(Date.UTC(2026, 10, 1, 12), 1, ny);
  const marks = I.hourMarks(m[0], m[1], ny);
  assert.deepEqual(marks.map((k) => [k.h, k.x]), [[0, 0], [6, 7], [12, 13], [18, 19], [0, 25]]);
});

test('hour marks stay on the hour after a 30-minute clock change', () => {
  const lh = I.zoneClock('Australia/Lord_Howe');
  const m = I.midnights(Date.UTC(2026, 9, 3, 6), 2, lh);
  const marks = I.hourMarks(m[1], m[2], lh);
  assert.ok(marks.every((k) => k.p.mi === 0));
  assert.deepEqual(marks.map((k) => k.h), [0, 6, 12, 18, 0]);
});

test('a range ends at the local midnight, also across a clock change', () => {
  const view = { unit: 'US', zone: 'America/New_York', range: '3', now: () => Date.UTC(2026, 9, 30, 16) };   // Fri 30 Oct
  assert.equal(I.chartConfig(payloadAt(Date.UTC(2026, 9, 28, 12) / 1000), null, view).options.scales.x.max, 73);
});

test('tick plan: as many hour marks as fit, dates thinned and with the weekday when there is room', () => {
  assert.deepEqual(I.tickPlan(3, 700), { step: 6, weekday: true, every: 1 });
  assert.equal(I.tickPlan(7, 700).step, 12);
  assert.deepEqual(I.tickPlan(16, 700), { step: 24, weekday: false, every: 1 });
  assert.equal(I.tickPlan(16, 360).every, 2);                                // a phone: every other date
  const p = (h, mi = 0) => ({ wd: 'Thu', mo: 10, d: 8, h, mi });
  const plan = I.tickPlan(3, 700);
  assert.equal(I.tickLabel({ h: 0, p: p(0) }, plan, 0), 'Thu 10/8');
  assert.equal(I.tickLabel({ h: 12, p: p(12) }, plan, 0), 'Noon');
  assert.equal(I.tickLabel({ h: 6, p: p(6) }, plan, 0), '6 AM');
  assert.equal(I.tickLabel({ h: 18, p: p(18) }, plan, 0), '6 PM');
  assert.equal(I.tickLabel({ h: 6, p: p(6) }, I.tickPlan(7, 700), 0), '');
  assert.equal(I.tickLabel({ h: 0, p: p(0) }, I.tickPlan(16, 360), 1), '');
  assert.equal(I.clockText(p(15, 5), false), '3:05 PM');
  assert.equal(I.clockText(p(0, 0), false), '12:00 AM');
});

// ---------------------------------------------------------------------------------------------- a payload

const NOW = Date.UTC(2026, 9, 8, 20, 30);                                    // Thu 8 Oct, 10:30 AM HST
const BEGIN = Date.UTC(2026, 9, 6, 12) / 1000;                               // the curve: 12 h before 00:00 UTC yesterday
function payload(extra) {
  const v = [], hilo = [], night = [];
  for (let i = 0; i < 913; i++) v.push(i === 400 ? null : +(0.5 + 0.4 * Math.sin(2 * Math.PI * i / 25)).toFixed(3));
  for (let k = 0; k < 74; k++) hilo.push([BEGIN + Math.round((k * 12.5 + 3.125) * 3600 / 2), k % 2 ? 0.1 : 0.9, k % 2 ? 'L' : 'H']);
  for (let d = 0; d < 19; d++) { const s = Date.UTC(2026, 9, 6, 4, 30) / 1000 + d * 86400; night.push([s, s + 11.5 * 3600]); }
  return Object.assign({ id: '1612340', name: 'Honolulu', tz: 'Pacific/Honolulu', type: 'R', obs: true, datum: 'MLLW', units: 'm',
    begin: BEGIN, window: [BEGIN + 43200, BEGIN + 43200 + 18 * 86400], step: 1800, v, hilo, night, method: 'harmonic', ref: null }, extra || {});
}
function payloadAt(begin) { return Object.assign(payload(), { begin }); }
const VIEW = (o) => Object.assign({ unit: 'US', zone: 'Pacific/Honolulu', range: '3', now: () => NOW }, o || {});

test('points: hours since the local midnight, heights in the unit, gaps kept, extremes and observations', () => {
  const origin = Date.UTC(2026, 9, 8, 10);
  const d = payload();
  const pts = I.points(d, { t: [BEGIN + 86400, BEGIN + 86460], v: [0.5, null] }, origin, 'US');
  assert.equal(pts.curve.length, 913);
  assert.equal(pts.curve[0].x, (BEGIN * 1000 - origin) / H);
  assert.equal(pts.curve[1].x - pts.curve[0].x, 0.5);
  assert.equal(pts.curve[400].y, null);
  assert.ok(Math.abs(pts.curve[0].y - 0.5 * I.FT_PER_M) < 1e-9);
  assert.equal(pts.ext[0].k, 'H'); assert.equal(pts.ext[1].k, 'L');
  assert.equal(pts.seen.length, 1);
  assert.equal(I.points(d, null, origin, 'Metric').curve[0].y, 0.5);
  const holes = I.points({ begin: BEGIN, step: 1800, v: [null, undefined, 'x', 0], hilo: [[BEGIN, null, 'H'], [null, 1, 'L'], [BEGIN, 0, 'L']],
                           night: [[BEGIN, null], [BEGIN, BEGIN + 60]] }, null, origin, 'US');
  assert.deepEqual(holes.curve.map((p) => p.y), [null, null, null, 0]);       // a gap is never drawn as 0
  assert.equal(holes.ext.length, 1);
  assert.equal(I.nightSpans({ night: [[BEGIN, null], [BEGIN, BEGIN + 60]] }, origin).length, 1);
  assert.equal(I.heightText(1, 'US'), '3.3 ft');
  assert.equal(I.heightText(1, 'Metric'), '1.00 m');
});

test('chart configuration: the range ends at a local midnight, ticks only at the marks, tooltips in the zone', () => {
  const cfg = I.chartConfig(payload(), null, VIEW());
  const x = cfg.options.scales.x;
  assert.equal(x.type, 'linear');
  assert.equal(x.min, 0); assert.equal(x.max, 72);
  assert.equal(I.chartConfig(payload(), null, VIEW({ range: '7' })).options.scales.x.max, 168);
  assert.equal(I.chartConfig(payload(), null, VIEW({ range: '16' })).options.scales.x.max, 384);
  assert.equal(cfg._view.origin, Date.UTC(2026, 9, 8, 10));
  assert.deepEqual(cfg.data.datasets.map((d) => d.label), ['Predicted', 'High / low']);
  assert.equal(I.chartConfig(payload(), { t: [BEGIN + 86400], v: [0.4] }, VIEW()).data.datasets[2].label, 'Observed');
  const axis = { min: 0, max: 72 };
  x.afterBuildTicks(axis);
  assert.deepEqual(axis.ticks.map((t) => t.value), [0, 6, 12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72]);
  const scale = { min: 0, max: 72, width: 700 };
  assert.equal(x.ticks.callback.call(scale, 0), 'Thu 10/8');
  assert.equal(x.ticks.callback.call(scale, 12), 'Noon');
  assert.equal(x.ticks.callback.call(scale, 30), '6 AM');
  assert.equal(x.ticks.callback.call(scale, 7), '');
  assert.equal(x.grid.color({ tick: { value: 24 } }), 'rgba(0,0,0,0.25)');
  const tt = cfg.options.plugins.tooltip.callbacks;
  assert.equal(tt.title([{ parsed: { x: 15.5 } }]), 'Thu 10/8, 3:30 PM');
  assert.equal(tt.label({ raw: { k: 'H' }, parsed: { y: 2.345 }, dataset: { label: 'High / low' } }), 'High: 2.3 ft');
  assert.equal(tt.label({ raw: { x: 1, y: 1 }, parsed: { y: 0.7 }, dataset: { label: 'Predicted' } }), 'Predicted: 0.7 ft');
  assert.match(cfg.options.scales.y.title.text, /ft, above MLLW/);
  assert.equal(cfg.options.scales.y.ticks, undefined);                       // Chart.js's own height labels
  const sort = cfg.options.plugins.legend.labels.sort;
  assert.deepEqual([{ datasetIndex: 2 }, { datasetIndex: 0 }, { datasetIndex: 1 }].sort(sort).map((l) => l.datasetIndex), [0, 1, 2]);
  assert.match(I.chartConfig(payload(), null, VIEW({ unit: 'Metric' })).options.scales.y.title.text, /\(m, above MLLW/);
  const utc = I.chartConfig(payload(), null, VIEW({ zone: 'UTC' }));
  assert.equal(utc._view.origin, Date.UTC(2026, 9, 8));
});

test('plugins: nights shaded inside the plot, the now line at the current time', () => {
  const chart = { ctx: fakeCtx(), chartArea: { left: 50, right: 750, top: 20, bottom: 300 }, scales: { x: { getPixelForValue: (h) => 50 + h * 10 } } };
  const shade = I.makeNightShade([[-5, 6], [18, 30]]);
  const fills = [];
  const ctx2 = Object.assign(fakeCtx(), { fillRect: (x, y, w2, h2) => fills.push([x, y, w2, h2]) });
  shade.beforeDraw(Object.assign({}, chart, { ctx: ctx2 }));
  assert.deepEqual(fills, [[50, 20, 60, 280], [230, 20, 120, 280]]);         // the first clipped at the left edge
  const ctx3 = fakeCtx();
  const origin = Date.UTC(2026, 9, 8, 10);
  I.makeNowLine(origin, () => NOW).afterDatasetsDraw(Object.assign({}, chart, { ctx: ctx3 }));
  const mv = ctx3.ops.find((o) => o.op === 'move');
  assert.equal(mv.x, 50 + 10.5 * 10);
  assert.ok(ctx3.ops.some((o) => o.op === 'text' && o.t === 'now'));
  const ctx4 = fakeCtx();
  I.makeNowLine(origin, () => origin - 2 * H).afterDatasetsDraw(Object.assign({}, chart, { ctx: ctx4 }));
  assert.equal(ctx4.ops.length, 0);                                          // left of the plot: not drawn
});

// ---------------------------------------------------------------------------------------------- the view

function deferred() { let res, rej; const p = new Promise((a, b) => { res = a; rej = b; }); return { p, res, rej }; }
function response(status, body, headers) {
  return { status, headers: { get: (k) => (headers || {})[k] || null }, json: () => (body === undefined ? Promise.reject(new Error('x')) : Promise.resolve(body)) };
}
function fakeTimers() {
  let seq = 0; const q = new Map();
  return { set(fn, ms) { const id = ++seq; q.set(id, { fn, ms }); return id; }, clear(id) { q.delete(id); },
           pending() { return [...q.values()].map((x) => x.ms); },
           fire(ms) { for (const [id, x] of [...q]) if (ms === undefined || x.ms === ms) { q.delete(id); x.fn(); } } };
}
const flush = async () => { for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r)); };

function setup(opts = {}) {
  const doc = new Document();
  const mk = (tag, id) => doc.register(doc.createElement(tag), id);
  const els = { content: mk('div', 'tideContent'), loading: mk('div', 'tideLoading'), error: mk('div', 'tideError'),
    errorText: mk('span', 'tideErrorText'), retry: mk('button', 'tideRetry'), rangeBar: mk('div', 'tideRangeBar'),
    box: mk('div', 'tideBox'), canvas: mk('canvas', 'tideChart'), hilo: mk('div', 'tideHiLo'), meta: mk('div', 'tideMeta') };
  ['3', '7', '16'].forEach((d) => { const b = doc.createElement('button'); b.setAttribute('data-days', d); els.rangeBar.appendChild(b); });
  const calls = [], answers = [];
  const fetch = (url, o) => { calls.push({ url, signal: o.signal }); const d = deferred(); answers.push(d); return d.p; };
  const Chart = fakeChart(), timers = fakeTimers(), storage = memStorage(opts.storage);
  let visible = opts.visible !== false, body = opts.body || 600;
  const chartJs = opts.chartJs || (() => Promise.resolve());
  const view = T.createTideView({ els, document: doc, fetch, loadChartJs: chartJs, getChart: () => Chart,
    storage, timers, now: () => NOW, visible: () => visible, bodyHeight: () => body, unit: opts.unit || 'US',
    zoneAbbr: (ms, tz) => (tz === 'Pacific/Honolulu' ? 'HST' : tz) });
  return { doc, els, calls, answers, Chart, timers, storage, view, setVisible: (v) => { visible = v; }, setBody: (h) => { body = h; } };
}
const HNL = { id: '1612340', name: 'Honolulu', tz: 'Pacific/Honolulu', obs: true };
const WAI = { id: '1611401', name: 'Waimea Bay', tz: 'Pacific/Honolulu', obs: false };

test('a station loads: the chart, the highs and lows of the range, the notes, then the observations', async () => {
  const s = setup();
  s.view.load(HNL, { unit: 'US' });
  assert.equal(s.view.state().status, 'loading');
  assert.equal(s.calls[0].url, '/api/tides/1612340');
  assert.equal(s.els.loading.classList.contains('d-none'), false);
  s.answers[0].res(response(200, payload()));
  await flush();
  const st = s.view.state();
  assert.equal(st.status, 'ready');
  assert.equal(s.Chart.made.length, 1);
  assert.equal(s.Chart.made[0].config.options.scales.x.max, 72);
  assert.equal(s.els.content.classList.contains('d-none'), false);
  const rows = s.els.hilo.querySelectorAll('tr');
  const inRange = payload().hilo.filter((e) => e[0] * 1000 >= Date.UTC(2026, 9, 8, 10) && e[0] * 1000 < Date.UTC(2026, 9, 11, 10));
  assert.equal(rows.length - 1, inRange.length);
  assert.match(rows[1].textContent, /^Thu 10\/8/);
  assert.equal(rows[2].querySelectorAll('td')[0].textContent, '');            // the day once per day
  assert.match(s.els.meta.textContent, /times in HST/);
  assert.match(s.els.meta.textContent, /Curve: NOAA tide predictions/);
  assert.equal(s.els.box.style.height, Math.min(I.BOX_MAX, 600 - I.BOX_CHROME) + 'px');
  assert.equal(s.calls[1].url, '/api/tides/1612340/observed');               // a gauge: asked after the curve
  s.answers[1].res(response(200, { t: [BEGIN + 86400], v: [0.4] }));
  await flush();
  assert.equal(s.Chart.made.length, 2);
  assert.equal(s.Chart.made[0].destroyed, true);
  assert.deepEqual(s.Chart.made[1].config.data.datasets.map((d) => d.label), ['Predicted', 'High / low', 'Observed']);
  assert.ok(s.timers.pending().includes(I.NOW_REDRAW_MS));
  s.timers.fire(I.NOW_REDRAW_MS);
  assert.equal(s.Chart.made[1].draws, 1);                                    // the now line moves
  assert.ok(s.timers.pending().includes(I.NOW_REDRAW_MS));
});

test('a station without a gauge is not asked for observations; the method is named', async () => {
  const s = setup();
  s.view.load(WAI);
  s.answers[0].res(response(200, payload({ id: '1611401', method: 'reference', ref: '1611400', obs: false })));
  await flush();
  assert.equal(s.calls.length, 1);
  assert.match(s.els.meta.textContent, /NOAA station 1611400 shaped between/);
});

test('a newer station voids the older one\'s answers', async () => {
  const s = setup();
  s.view.load(HNL);
  s.view.load(WAI);
  assert.equal(s.calls[0].signal.aborted, true);
  s.answers[0].res(response(200, payload()));
  await flush();
  assert.equal(s.Chart.made.length, 0);
  assert.equal(s.view.state().station, '1611401');
  s.answers[1].res(response(200, payload({ id: '1611401', obs: false })));
  await flush();
  assert.equal(s.Chart.made.length, 1);
  s.view.clear();
  assert.equal(s.Chart.made[0].destroyed, true);
  assert.equal(s.view.state().nowTimer, false);
  assert.equal(s.view.state().status, 'idle');
});

test('busy: asked again after Retry-After, a limited number of times; final and unknown answers are not retried', async () => {
  const s = setup();
  s.view.load(HNL);
  s.answers[0].res(response(503, { error: 'busy', retry: true }, { 'Retry-After': '7' }));
  await flush();
  assert.deepEqual(s.timers.pending(), [7000]);
  assert.equal(s.view.state().status, 'loading');
  s.timers.fire(7000);
  assert.equal(s.calls.length, 2);
  for (let k = 1; k < I.RETRY_MAX; k++) {
    s.answers[k].res(response(503, { retry: true }, {}));
    await flush();
    if (k < I.RETRY_MAX - 1) { assert.deepEqual(s.timers.pending(), [5000]); s.timers.fire(5000); }
  }
  assert.equal(s.view.state().status, 'error');
  assert.equal(s.els.retry.hidden, false);
  assert.match(s.els.errorText.textContent, /not available yet/);
  s.els.retry.dispatch('click');
  assert.equal(s.view.state().status, 'loading');
  assert.equal(s.calls.length, I.RETRY_MAX + 1);

  const f = setup();
  f.view.load(WAI);
  f.answers[0].res(response(200, { id: '1611401', error: 'NOAA publishes no tide predictions for this station', final: true }));
  await flush();
  assert.equal(f.view.state().status, 'final');
  assert.equal(f.els.retry.hidden, true);
  assert.match(f.els.errorText.textContent, /NOAA publishes no tide predictions/);
  const u = setup();
  u.view.load({ id: '0000000', tz: 'UTC' });
  u.answers[0].res(response(404, { error: 'Unknown tide station' }));
  await flush();
  assert.equal(u.view.state().status, 'final');
  assert.match(u.els.errorText.textContent, /not known/);
});

test('a failed request offers Retry; an aborted one says nothing', async () => {
  const s = setup();
  s.view.load(HNL);
  s.answers[0].rej(new TypeError('Failed to fetch'));
  await flush();
  assert.equal(s.view.state().status, 'error');
  assert.equal(s.els.retry.hidden, false);
  const a = setup();
  a.view.load(HNL);
  const e = new Error('aborted'); e.name = 'AbortError';
  a.answers[0].rej(e);
  await flush();
  assert.equal(a.view.state().status, 'loading');
  const j = setup();
  j.view.load(HNL);
  j.answers[0].res(response(200, undefined));                                 // not JSON
  await flush();
  assert.equal(j.view.state().status, 'error');
});

test('unit and zone changes redraw from the data in hand; a range change moves the end', async () => {
  const s = setup();
  s.view.load(HNL);
  s.answers[0].res(response(200, payload({ obs: false })));
  await flush();
  const before = s.els.hilo.textContent;
  s.view.setUnit('Metric');
  await flush();
  assert.equal(s.Chart.made.length, 2);
  assert.match(s.Chart.made[1].config.options.scales.y.title.text, /\(m,/);
  assert.notEqual(s.els.hilo.textContent, before);
  assert.match(s.els.hilo.textContent, /0\.90 m/);
  s.view.setZone('UTC');
  await flush();
  assert.equal(s.Chart.made.length, 3);
  assert.equal(s.Chart.made[2].config._view.origin, Date.UTC(2026, 9, 8));
  assert.match(s.els.meta.textContent, /times in UTC/);
  s.view.setZone('');
  await flush();
  assert.equal(s.view.state().zone, 'Pacific/Honolulu');                     // back to the station's own zone
  const ch = s.Chart.made[3], n = s.els.hilo.querySelectorAll('tr').length;
  s.els.rangeBar.querySelectorAll('[data-days]')[1].dispatch('click');       // 7 d
  assert.equal(s.storage.getItem(I.RANGE_KEY), '7');
  assert.equal(ch.options.scales.x.max, 168);
  assert.equal(ch.updates, 1);
  assert.equal(s.Chart.made.length, 4);                                      // no rebuild
  assert.ok(s.els.hilo.querySelectorAll('tr').length > n);
  assert.equal(s.els.rangeBar.querySelectorAll('[data-days]')[1].getAttribute('aria-pressed'), 'true');
  s.view.setRange('nonsense');
  assert.equal(s.storage.getItem(I.RANGE_KEY), '3');
  assert.equal(s.calls.length, 1);                                           // never asked the server again
});

test('the remembered range is used; a hidden window builds its chart when shown', async () => {
  const s = setup({ storage: { [I.RANGE_KEY]: '16' }, visible: false });
  s.view.load(HNL);
  s.answers[0].res(response(200, payload({ obs: false })));
  await flush();
  assert.equal(s.Chart.made.length, 0);
  assert.equal(s.view.state().status, 'ready');
  s.setVisible(true);
  await s.view.show();
  assert.equal(s.Chart.made.length, 1);
  assert.equal(s.Chart.made[0].config.options.scales.x.max, 384);
  await s.view.show();
  assert.equal(s.Chart.made.length, 1);                                      // nothing new: only fitted
  s.setBody(2000); s.view.resize();
  assert.equal(s.els.box.style.height, I.BOX_MAX + 'px');
  s.setBody(100); s.view.resize();
  assert.equal(s.els.box.style.height, I.BOX_MIN + 'px');
  assert.ok(s.Chart.made[0].resizes >= 2);
});

test('text from the server goes into the page as text only', async () => {
  const s = setup();
  s.view.load(HNL);
  s.answers[0].res(response(200, { id: '1611401', error: '<img src=x onerror=alert(1)>', final: true }));
  await flush();
  assert.equal(s.els.errorText.textContent, '<img src=x onerror=alert(1)>');
  assert.equal(s.els.errorText._html, '');                                  // set as text, never parsed
  assert.ok(!/innerHTML/.test(SRC));
});

test('a station picked while Chart.js loads gets its chart; a window hidden meanwhile builds it when shown', async () => {
  let loaded; const once = new Promise((r) => { loaded = r; });
  const s = setup({ chartJs: () => once });
  s.view.load(HNL);
  s.answers[0].res(response(200, payload({ obs: false })));
  await flush();                                                             // Honolulu waits for Chart.js
  s.view.load(WAI);
  s.answers[1].res(response(200, payload({ id: '1611401', obs: false, method: 'cosine' })));
  await flush();
  loaded();
  await flush();
  assert.equal(s.Chart.made.length, 1);
  assert.match(s.els.meta.textContent, /drawn between NOAA/);                // Waimea's, the station on screen
  let go; const later = new Promise((r) => { go = r; });
  const h = setup({ chartJs: () => later });
  h.view.load(HNL);
  h.answers[0].res(response(200, payload({ obs: false })));
  await flush();
  h.setVisible(false);
  go();
  await flush();
  assert.equal(h.Chart.made.length, 0);
  h.setVisible(true);
  await h.view.show();
  assert.equal(h.Chart.made.length, 1);
});

test('an answer still being read when another station is picked is dropped', async () => {
  const s = setup();
  s.view.load(HNL);
  let body; const slow = new Promise((r) => { body = r; });
  s.answers[0].res({ status: 200, headers: { get: () => null }, json: () => slow });
  await flush();
  s.view.load(WAI);
  body(payload());
  await flush();
  assert.equal(s.view.state().hasData, false);
  assert.equal(s.Chart.made.length, 0);
});

test('a new station starts in its own zone unless one is chosen', async () => {
  const s = setup();
  s.view.load(HNL, { zone: 'UTC' });
  assert.equal(s.view.state().zone, 'UTC');
  s.view.load(WAI);
  assert.equal(s.view.state().zone, 'Pacific/Honolulu');
});

test('a window closed while Chart.js loads builds nothing', async () => {
  let loaded; const once = new Promise((r) => { loaded = r; });
  const s = setup({ chartJs: () => once });
  s.view.load(HNL);
  s.answers[0].res(response(200, payload({ obs: false })));
  await flush();
  s.view.clear();
  loaded();
  await flush();
  assert.equal(s.Chart.made.length, 0);
  assert.equal(s.view.state().status, 'idle');
});
