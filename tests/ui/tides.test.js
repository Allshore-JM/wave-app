'use strict';
// static_ui/tides.js (plan section 38): clocks in a zone (23- and 25-hour days), the day strip's geometry (columns,
// time <-> px, the vertical scale), the rows' pure helpers, and the view: loading, retries, stale answers, the strip's
// table and SVG, expanding days, unit / zone changes, the now line, the readout, hidden windows, text only.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Document } = require('./fakedom');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'tides.js'), 'utf8');
const w = {}; new Function('window', SRC)(w);
const T = w.AllshoreTides;
const I = T._internals;
const H = 3600000;

// ---------------------------------------------------------------------------------------------- clocks

test('local midnights in a zone, including 23-, 25- and 23.5-hour days', () => {
  const ny = I.zoneClock('America/New_York');
  const spring = I.midnights(Date.UTC(2026, 2, 7, 18), 2, ny);              // Sat 7 Mar 2026, 1 PM EST
  assert.equal(spring[0], Date.UTC(2026, 2, 7, 5));                          // 00:00 EST
  assert.equal((spring[2] - spring[1]) / H, 23);                             // Sun 8 Mar: clocks go forward
  const fall = I.midnights(Date.UTC(2026, 9, 31, 18), 2, ny);
  assert.equal((fall[2] - fall[1]) / H, 25);                                 // Sun 1 Nov: clocks go back
  const hi = I.midnights(Date.UTC(2026, 9, 8, 20, 17, 42, 500), 30, I.zoneClock('Pacific/Honolulu'));
  assert.equal(hi.length, 31); assert.equal(hi[0], Date.UTC(2026, 9, 8, 10));
  assert.ok(hi.every((m, k) => k === 0 || m - hi[k - 1] === 24 * H));
  const lh = I.midnights(Date.UTC(2026, 9, 3, 6), 2, I.zoneClock('Australia/Lord_Howe'));
  assert.equal((lh[2] - lh[1]) / H, 23.5);                                   // Sun 4 Oct: a 30-minute step
  assert.equal(I.localMidnightBefore(Date.UTC(2026, 9, 8, 0, 0), I.zoneClock('Asia/Kolkata')), Date.UTC(2026, 9, 7, 18, 30));
  const bad = I.zoneClock('Nowhere/Atlantis');
  assert.equal(bad.zone, 'UTC');
  assert.equal(I.localMidnightBefore(Date.UTC(2026, 9, 8, 7, 5), bad), Date.UTC(2026, 9, 8));
  assert.equal(I.zoneClock('constructor').zone, 'UTC');                      // never an Object member
  const p = { wd: 'Fri', mo: 10, d: 9, h: 15, mi: 5 };
  assert.equal(I.clockText(p), '3:05 PM'); assert.equal(I.clockText({ h: 0, mi: 0 }), '12:00 AM');
  assert.equal(I.dayShort(p), 'Fri 9'); assert.equal(I.dayLong(p), 'Friday, Oct 9'); assert.equal(I.stampText(p), 'Fri 10/9, 3:05 PM');
});

// ---------------------------------------------------------------------------------------------- geometry

const NOW = Date.UTC(2026, 9, 8, 20, 30);                                    // Thu 8 Oct, 10:30 AM HST
const HNL = I.zoneClock('Pacific/Honolulu');
const MIDS = I.midnights(NOW, I.DAYS, HNL);

test('layout: a column per day, open days wide; time <-> px linear within each day, also across a clock change', () => {
  const L = I.layout(MIDS, { 0: true, 3: true });
  assert.equal(L.widths[0], I.COL_OPEN); assert.equal(L.widths[1], I.COL_CLOSED); assert.equal(L.widths[3], I.COL_OPEN);
  assert.equal(L.total, 2 * I.COL_OPEN + 28 * I.COL_CLOSED);
  assert.equal(L.lefts[1], I.COL_OPEN); assert.equal(L.lefts[2], I.COL_OPEN + I.COL_CLOSED);
  assert.equal(I.dayOf(L, MIDS[0]), 0); assert.equal(I.dayOf(L, MIDS[1] - 1), 0); assert.equal(I.dayOf(L, MIDS[5] + 1), 5);
  assert.equal(I.dayOf(L, MIDS[0] - 1), -1); assert.equal(I.dayOf(L, MIDS[30]), -1);
  assert.equal(I.xOf(L, MIDS[0] + 12 * H), I.COL_OPEN / 2);                  // noon today: the middle of the open column
  assert.equal(I.xOf(L, MIDS[1] + 12 * H), I.COL_OPEN + I.COL_CLOSED / 2);
  assert.equal(I.xOf(L, MIDS[0] - 12 * H), -I.COL_OPEN / 2);                 // before the strip: the first day's slope
  assert.equal(I.tOf(L, I.COL_OPEN + I.COL_CLOSED / 2), MIDS[1] + 12 * H);
  assert.equal(I.tOf(L, -5), MIDS[0]); assert.equal(I.tOf(L, 1e6), MIDS[30]);
  for (let x = 0; x < L.total; x += 37) assert.ok(Math.abs(I.xOf(L, I.tOf(L, x)) - x) < 1e-6);   // a round trip
  const ny = I.zoneClock('America/New_York'), m = I.midnights(Date.UTC(2026, 9, 31, 18), 3, ny), Ln = I.layout(m, { 1: true });
  assert.equal((m[2] - m[1]) / H, 25);
  assert.equal(I.xOf(Ln, m[1] + 12.5 * H), I.COL_CLOSED + I.COL_OPEN / 2, 'the 25-hour day: its middle instant sits mid-column');
});

test('the vertical scale: a nice step with 3-7 ticks, room above the top, feet and metres', () => {
  const s = I.yScale([-0.2, 0.3, 1.9, 2.05], 'US');
  assert.equal(s.step, 0.5); assert.deepEqual(s.ticks, [-0.5, 0, 0.5, 1, 1.5, 2, 2.5]);
  assert.equal(s.y(s.hi), I.CHART_PAD.top); assert.equal(s.y(s.lo), I.CHART_H - I.CHART_PAD.bottom);
  assert.ok(s.y(1) > s.y(2));
  const m = I.yScale([0.05, 0.62], 'Metric');
  assert.equal(m.step, 0.1); assert.equal(m.ticks[0], 0); assert.equal(m.ticks[m.ticks.length - 1], 0.7);
  const big = I.yScale([-1, 31], 'US'); assert.equal(big.step, 5);
  const flat = I.yScale([null, undefined, 'x'], 'US'); assert.ok(flat.ticks.length >= 3);
  assert.equal(I.tickText(1.5, 'US'), '1.5'); assert.equal(I.tickText(2, 'US'), '2'); assert.equal(I.tickText(0.25, 'US'), '0.25'); assert.equal(I.tickText(0.5, 'Metric'), '0.5');
  assert.equal(I.heightText(1, 'US'), '3.3 ft'); assert.equal(I.heightText(1, 'Metric'), '1.00 m');
});

// ---------------------------------------------------------------------------------------------- a payload

const BEGIN = Date.UTC(2026, 9, 6, 12) / 1000;                               // the curve: 12 h before 00:00 UTC yesterday
function payload(extra) {
  const v = [], hilo = [], night = [], events = [], moon = [];
  for (let i = 0; i < 1585; i++) v.push(i === 400 ? null : +(0.5 + 0.4 * Math.sin(2 * Math.PI * i / 25)).toFixed(3));
  for (let k = 0; k < 128; k++) hilo.push([BEGIN + Math.round((k * 12.5 + 3.125) * 3600 / 2), k % 2 ? 0.1 : 0.9, k % 2 ? 'L' : 'H']);
  for (let d = 0; d < 33; d++) {
    const s = Date.UTC(2026, 9, 6, 4, 30) / 1000 + d * 86400;                // 6:30 PM HST
    night.push([s, s + 11.5 * 3600]);
    events.push([s + 11.75 * 3600, 'sunrise'], [s - 0.25 * 3600, 'sunset'], [s + 3 * 3600, 'moonrise'], [s + 15 * 3600, 'moonset']);
  }
  for (let i = 0; i <= 33 * 4; i++) moon.push([BEGIN + i * 21600, (0.2 + i / 400) % 1, 50, 'Waxing gibbous']);
  events.sort((a, b) => a[0] - b[0]);
  return Object.assign({ id: '1612340', name: 'Honolulu', lat: 21.3, lon: -157.86, tz: 'Pacific/Honolulu', type: 'R', obs: true, datum: 'MLLW', units: 'm',
    begin: BEGIN, window: [BEGIN + 43200, BEGIN + 43200 + 32 * 86400], step: 1800, v, hilo, night, events, moon, method: 'harmonic', ref: null }, extra || {});
}

test('the rows\' helpers: samples and the height at an instant, extremes and events per half day, the moon per day, the nights', () => {
  const d = payload(), L = I.layout(MIDS, { 0: true });
  const s = I.samples(d, MIDS[0], MIDS[1]);
  assert.ok(s[0].t <= MIDS[0] && s[s.length - 1].t >= MIDS[1]);
  assert.equal(I.heightAt(d, d.begin * 1000), 0.5);
  assert.ok(Math.abs(I.heightAt(d, d.begin * 1000 + 900000) - (0.5 + d.v[1]) / 2) < 1e-9, 'linear between samples');
  assert.equal(I.heightAt(d, d.begin * 1000 + 400 * 1800000 - 1), null, 'a gap');
  assert.equal(I.heightAt(d, d.begin * 1000 - 1), null);
  const am = I.extremesIn(d, L, 0, 0), pm = I.extremesIn(d, L, 0, 1), all = I.extremesIn(d, L, 0, -1);
  assert.equal(am.length + pm.length, all.length); assert.ok(all.length >= 3 && all.length <= 5);
  assert.ok(am.every((e) => e.t < MIDS[0] + 12 * H) && pm.every((e) => e.t >= MIDS[0] + 12 * H));
  assert.ok(all.every((e, i) => !i || e.t > all[i - 1].t));
  const sun = I.eventsIn(d, L, 0, -1, ['sunrise', 'sunset']);
  assert.deepEqual(sun.map((e) => e.kind), ['sunrise', 'sunset']);
  assert.deepEqual(I.eventsIn(d, L, 0, 0, ['sunrise', 'sunset']).map((e) => e.kind), ['sunrise']);
  assert.deepEqual(I.eventsIn(d, L, 0, 1, ['sunrise', 'sunset']).map((e) => e.kind), ['sunset']);
  const moon = I.moonOf(d, L, 2, 21.3);
  assert.equal(moon.name, 'Waxing gibbous'); assert.equal(moon.pct, 50); assert.ok(I.MOON_GLYPHS.indexOf(moon.glyph) >= 0);
  const north = I.moonOf({ moon: [[MIDS[0] / 1000 + 43200, 0.25, 50, 'First quarter']] }, L, 0, 21.3);
  const south = I.moonOf({ moon: [[MIDS[0] / 1000 + 43200, 0.25, 50, 'First quarter']] }, L, 0, -33.8);
  assert.equal(north.glyph, I.MOON_GLYPHS[2]); assert.equal(south.glyph, I.MOON_GLYPHS[6], 'the lit side mirrored south of the equator');
  assert.equal(I.moonOf({ moon: [] }, L, 0, 21.3), null);
  const nights = I.nightSpans(d, L);
  assert.ok(nights.length >= 30 && nights.every((n) => n[1] > n[0] && n[0] >= 0 && n[1] <= L.total));
  assert.ok(nights[0][0] === 0 && nights[0][1] < I.COL_OPEN / 2, 'the first night runs from the strip\'s start into the morning of today');
  const holes = I.extremesIn({ hilo: [[MIDS[0] / 1000 + 10, null, 'H'], [null, 1, 'L'], ['x', 1, 'L'], [MIDS[0] / 1000 + 20, 0.3, 'L']] }, L, 0, -1);
  assert.equal(holes.length, 1);
  const paths = I.curvePaths(d, L, I.yScale([0, 1], 'US'), 'US');
  assert.match(paths.line, /^M[\d.]+ [\d.]+L/); assert.ok((paths.line.match(/M/g) || []).length >= 2, 'a new M after the gap');
  assert.ok(/Z/.test(paths.area));
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
    errorText: mk('span', 'tideErrorText'), retry: mk('button', 'tideRetry'), strip: mk('div', 'tideStrip'), meta: mk('div', 'tideMeta') };
  const calls = [], answers = [];
  const fetch = (url, o) => { calls.push({ url, signal: o.signal }); const d = deferred(); answers.push(d); return d.p; };
  const timers = fakeTimers();
  let visible = opts.visible !== false, nowMs = opts.now || NOW;
  const view = T.createTideView({ els, document: doc, fetch, timers, now: () => nowMs, visible: () => visible, unit: opts.unit || 'US',
    zoneAbbr: (ms, tz) => (tz === 'Pacific/Honolulu' ? 'HST' : tz) });
  const q = (sel) => els.strip.querySelectorAll(sel);
  return { doc, els, calls, answers, timers, view, q, setVisible: (v) => { visible = v; }, setNow: (t) => { nowMs = t; } };
}
const HNL_ST = { id: '1612340', name: 'Honolulu', tz: 'Pacific/Honolulu', obs: true, lat: 21.3 };
const WAI_ST = { id: '1611401', name: 'Waimea Bay', tz: 'Pacific/Honolulu', obs: false, lat: 21.95 };

test('a station loads: the strip has 30 day columns with today open, the chart, the rows, the notes, then the observations', async () => {
  const s = setup();
  s.view.load(HNL_ST, { unit: 'US' });
  assert.equal(s.view.state().status, 'loading'); assert.equal(s.calls[0].url, '/api/tides/1612340');
  s.answers[0].res(response(200, payload()));
  await flush();
  const st = s.view.state();
  assert.equal(st.status, 'ready'); assert.equal(st.built, true); assert.deepEqual(st.open, [0]);
  const days = s.q('button[data-day]');
  assert.equal(days.length, I.DAYS);
  assert.equal(days[0].getAttribute('aria-expanded'), 'true'); assert.equal(days[0].textContent, 'Thursday, Oct 8');
  assert.equal(days[1].getAttribute('aria-expanded'), 'false'); assert.equal(days[1].textContent, 'Fri 9');
  assert.equal(days[0].getAttribute('type'), 'button');
  const halves = s.q('.tide-half'); assert.equal(halves.length, 2 + 29, 'AM | PM for the open day, one cell for each closed day');
  assert.equal(halves[0].textContent, 'AM'); assert.equal(halves[1].textContent, 'PM');
  const cols = s.q('col'); assert.equal(cols.length, 1 + 2 * I.DAYS); assert.equal(cols[0].style.width, I.LABEL_W + 'px'); assert.equal(cols[1].style.width, (I.COL_OPEN / 2) + 'px'); assert.equal(cols[3].style.width, (I.COL_CLOSED / 2) + 'px');
  const svg = s.q('svg')[0];
  assert.equal(svg.getAttribute('width'), String(st.layout.total));
  assert.ok(s.q('.tide-curve').length === 1 && s.q('.tide-area').length === 1);
  assert.ok(s.q('.tide-dot').length >= 100);
  assert.ok(s.q('.tide-callout').length >= 3 && s.q('.tide-callout').length <= 5, 'callouts on the open day only');
  assert.ok(s.q('.tide-night').length >= 30);
  assert.equal(s.q('.tide-observed').length, 0);
  const high = s.q('.tide-row-high')[0].querySelectorAll('td'), low = s.q('.tide-row-low')[0].querySelectorAll('td');
  assert.equal(high.length, 1 + 2 + 29); assert.equal(low.length, 1 + 2 + 29);
  assert.equal(high[0].textContent, 'HIGH(HST)');
  assert.match(s.q('.tide-row-high')[0].querySelectorAll('.tide-ex')[0].textContent, /^\d{1,2}:\d{2} [AP]M[\d.]+ ft$/);
  assert.equal(s.q('.tide-row-sun')[0].querySelectorAll('td')[0].textContent, 'Sun'); assert.equal(s.q('.tide-row-sun')[0].querySelectorAll('.tide-ev').length, 2 * I.DAYS);
  assert.equal(s.q('.tide-row-moon')[0].querySelectorAll('.tide-moon-glyph').length, I.DAYS);
  assert.match(s.q('.tide-row-moon')[0].querySelectorAll('.tide-moon-glyph')[0].getAttribute('title'), /Waxing gibbous, 50% lit/);
  assert.equal(s.q('.tide-ytick').length, st.scale.ticks.length);
  const nowG = s.q('.tide-now')[0];
  assert.equal(nowG.getAttribute('visibility'), 'visible');
  const nowX = +nowG.children[0].getAttribute('x1');
  assert.ok(Math.abs(nowX - I.xOf(st.layout, NOW)) < 0.1, 'the now line at 10:30 AM today');
  assert.match(s.q('.tide-ynow')[0].textContent, /ft$/);
  assert.match(s.els.meta.textContent, /times in HST/); assert.match(s.els.meta.textContent, /Curve: NOAA tide predictions/);
  assert.equal(s.calls[1].url, '/api/tides/1612340/observed');               // a gauge: asked after the strip
  s.answers[1].res(response(200, { t: [BEGIN + 2 * 86400], v: [0.4] }));
  await flush();
  assert.equal(s.q('.tide-observed').length, 1);
  assert.ok(s.timers.pending().includes(I.NOW_REDRAW_MS));
});

test('clicking a day header opens it (AM | PM, wider column, callouts), again closes it; several open; focus stays on it', async () => {
  const s = setup();
  s.view.load(HNL_ST);
  s.answers[0].res(response(200, payload({ obs: false })));
  await flush();
  const before = s.view.state().layout.total;
  s.q('button[data-day="3"]')[0].dispatch('click');
  let st = s.view.state();
  assert.deepEqual(st.open, [0, 3]); assert.equal(st.layout.total, before + I.COL_OPEN - I.COL_CLOSED);
  assert.equal(s.q('button[data-day="3"]')[0].getAttribute('aria-expanded'), 'true');
  assert.equal(s.q('button[data-day="3"]')[0].textContent, 'Sunday, Oct 11');
  assert.equal(s.doc.activeElement, s.q('button[data-day="3"]')[0], 'focus follows the rebuilt button');
  assert.ok(s.q('.tide-callout').length >= 6);
  s.q('button[data-day="0"]')[0].dispatch('click');
  st = s.view.state(); assert.deepEqual(st.open, [3]);
  assert.equal(s.q('button[data-day="0"]')[0].textContent, 'Thu 8');
  assert.equal(s.q('.tide-row-high')[0].querySelectorAll('td').length, 1 + 29 + 2);
  s.view.toggleDay(99); assert.deepEqual(s.view.state().open, [3], 'no such day');
  assert.equal(s.calls.length, 1, 'never asked the server again');
});

test('a station without a gauge is not asked for observations; the method is named; the southern moon is mirrored', async () => {
  const s = setup();
  s.view.load({ id: '9999', name: 'Sydney', tz: 'Australia/Sydney', obs: false, lat: -33.8 });
  s.answers[0].res(response(200, payload({ id: '9999', lat: -33.8, tz: 'Australia/Sydney', method: 'reference', ref: '1611400', obs: false,
    moon: [[MIDS[0] / 1000 + 43200, 0.25, 50, 'First quarter']] })));
  await flush();
  assert.equal(s.calls.length, 1);
  assert.match(s.els.meta.textContent, /NOAA station 1611400 shaped between/);
  assert.equal(s.view.state().zone, 'Australia/Sydney');
  assert.equal(s.q('.tide-moon-glyph')[0].textContent, I.MOON_GLYPHS[6]);
});

test('a newer station voids the older one\'s answers; clear empties the strip', async () => {
  const s = setup();
  s.view.load(HNL_ST);
  s.view.load(WAI_ST);
  assert.equal(s.calls[0].signal.aborted, true);
  s.answers[0].res(response(200, payload()));
  await flush();
  assert.equal(s.view.state().built, false); assert.equal(s.view.state().station, '1611401');
  s.answers[1].res(response(200, payload({ id: '1611401', obs: false })));
  await flush();
  assert.equal(s.view.state().built, true);
  s.view.clear();
  assert.equal(s.els.strip.children.length, 0); assert.equal(s.view.state().nowTimer, false); assert.equal(s.view.state().status, 'idle');
});

test('busy: asked again after Retry-After, a limited number of times; final and unknown answers are not retried', async () => {
  const s = setup();
  s.view.load(HNL_ST);
  s.answers[0].res(response(503, { error: 'busy', retry: true }, { 'Retry-After': '7' }));
  await flush();
  assert.deepEqual(s.timers.pending(), [7000]); assert.equal(s.view.state().status, 'loading');
  s.timers.fire(7000);
  assert.equal(s.calls.length, 2);
  for (let k = 1; k < I.RETRY_MAX; k++) {
    s.answers[k].res(response(503, { retry: true }, {}));
    await flush();
    if (k < I.RETRY_MAX - 1) { assert.deepEqual(s.timers.pending(), [5000]); s.timers.fire(5000); }
  }
  assert.equal(s.view.state().status, 'error'); assert.equal(s.els.retry.hidden, false);
  assert.match(s.els.errorText.textContent, /not available yet/);
  s.els.retry.dispatch('click');
  assert.equal(s.view.state().status, 'loading'); assert.equal(s.calls.length, I.RETRY_MAX + 1);
  const f = setup();
  f.view.load(WAI_ST);
  f.answers[0].res(response(200, { id: '1611401', error: 'NOAA publishes no tide predictions for this station', final: true }));
  await flush();
  assert.equal(f.view.state().status, 'final'); assert.equal(f.els.retry.hidden, true);
  const u = setup();
  u.view.load({ id: '0000000', tz: 'UTC' });
  u.answers[0].res(response(404, { error: 'Unknown tide station' }));
  await flush();
  assert.equal(u.view.state().status, 'final'); assert.match(u.els.errorText.textContent, /not known/);
});

test('a failed request offers Retry; an aborted one says nothing; a non-JSON answer is a failure', async () => {
  const s = setup();
  s.view.load(HNL_ST);
  s.answers[0].rej(new TypeError('Failed to fetch'));
  await flush();
  assert.equal(s.view.state().status, 'error'); assert.equal(s.els.retry.hidden, false);
  const a = setup();
  a.view.load(HNL_ST);
  const e = new Error('aborted'); e.name = 'AbortError';
  a.answers[0].rej(e);
  await flush();
  assert.equal(a.view.state().status, 'loading');
  const j = setup();
  j.view.load(HNL_ST);
  j.answers[0].res(response(200, undefined));
  await flush();
  assert.equal(j.view.state().status, 'error');
});

test('unit and zone changes rebuild from the data in hand; the open days are kept', async () => {
  const s = setup();
  s.view.load(HNL_ST);
  s.answers[0].res(response(200, payload({ obs: false })));
  await flush();
  s.q('button[data-day="2"]')[0].dispatch('click');
  s.view.setUnit('Metric');
  assert.match(s.q('.tide-row-high')[0].querySelectorAll('.tide-ex')[0].textContent, / m$/);
  assert.deepEqual(s.view.state().open, [0, 2]);
  assert.match(s.q('.tide-ynow')[0].textContent, / m$/);
  s.view.setZone('UTC');
  assert.equal(s.view.state().zone, 'UTC');
  assert.equal(s.view.state().layout.mids[0], Date.UTC(2026, 9, 8), 'the days start at UTC midnight');
  assert.match(s.els.meta.textContent, /times in UTC/);
  assert.equal(s.q('button[data-day="0"]')[0].textContent, 'Thursday, Oct 8');
  s.view.setZone('');
  assert.equal(s.view.state().zone, 'Pacific/Honolulu');
  assert.equal(s.calls.length, 1);
});

test('the now line moves every minute; a new day restarts the strip at today; a hidden window builds when shown', async () => {
  const s = setup();
  s.view.load(HNL_ST);
  s.answers[0].res(response(200, payload({ obs: false })));
  await flush();
  const x0 = +s.q('.tide-now')[0].children[0].getAttribute('x1');
  s.setNow(NOW + 2 * H);
  s.timers.fire(I.NOW_REDRAW_MS);
  const x1 = +s.q('.tide-now')[0].children[0].getAttribute('x1');
  assert.ok(Math.abs(x1 - x0 - 2 * I.COL_OPEN / 24) < 0.1, 'two hours on the open day');
  assert.ok(s.timers.pending().includes(I.NOW_REDRAW_MS));
  s.q('button[data-day="4"]')[0].dispatch('click');
  s.setNow(MIDS[1] + 5 * 60000);                                             // five minutes into tomorrow
  s.timers.fire(I.NOW_REDRAW_MS);
  assert.equal(s.view.state().layout.mids[0], MIDS[1], 'the strip starts at the new today');
  assert.deepEqual(s.view.state().open, [0]);
  const h = setup({ visible: false });
  h.view.load(HNL_ST);
  h.answers[0].res(response(200, payload({ obs: false })));
  await flush();
  assert.equal(h.view.state().built, false); assert.equal(h.view.state().status, 'ready');
  h.setVisible(true);
  await h.view.show();
  assert.equal(h.view.state().built, true);
  const n = h.q('svg').length;
  await h.view.show();
  assert.equal(h.q('svg').length, n, 'nothing new');
  h.view.resize();
});

test('the readout follows the pointer over the chart (time + height in the zone), hides off it; touch too', async () => {
  const s = setup();
  s.view.load(HNL_ST);
  s.answers[0].res(response(200, payload({ obs: false })));
  await flush();
  const svg = s.q('svg')[0];
  svg.rect = { left: 100, top: 50, width: s.view.state().layout.total, height: I.CHART_H };
  s.els.strip.dispatch('mousemove', { clientX: 100 + I.COL_OPEN / 2, clientY: 120 });   // noon today
  assert.equal(s.view.state().readout, 'Thu 10/8, 12:00 PM · ' + I.heightText(I.heightAt(payload(), MIDS[0] + 12 * H), 'US'));
  assert.equal(s.q('.tide-readout')[0].getAttribute('visibility'), 'visible');
  s.els.strip.dispatch('mousemove', { clientX: 100 + 10, clientY: 10 });     // above the chart
  assert.equal(s.view.state().readout, '');
  s.els.strip.dispatch('touchstart', { touches: [{ clientX: 100 + I.COL_OPEN + I.COL_CLOSED / 2, clientY: 120 }] });
  assert.match(s.view.state().readout, /^Fri 10\/9, 12:00 PM/);
  s.els.strip.dispatch('touchend', {});
  assert.equal(s.view.state().readout, '');
  s.els.strip.dispatch('mousemove', { clientX: 100 + I.COL_OPEN / 2, clientY: 120 });
  s.els.strip.dispatch('mouseleave', {});
  assert.equal(s.q('.tide-readout')[0].getAttribute('visibility'), 'hidden');
});

test('text from the server goes into the page as text only', async () => {
  const s = setup();
  s.view.load(HNL_ST);
  s.answers[0].res(response(200, { id: '1611401', error: '<img src=x onerror=alert(1)>', final: true }));
  await flush();
  assert.equal(s.els.errorText.textContent, '<img src=x onerror=alert(1)>');
  assert.equal(s.els.errorText._html, '');
  assert.ok(!/innerHTML/.test(SRC));
  const n = setup();
  n.view.load(HNL_ST);
  n.answers[0].res(response(200, payload({ obs: false, moon: [[MIDS[0] / 1000 + 43200, 0.5, 100, '<b>Full</b>']] })));
  await flush();
  assert.equal(n.q('.tide-moon-glyph')[0].getAttribute('title'), '<b>Full</b>, 100% lit');
});

test('the scale keeps a step of room above a value that sits on a tick; a slow answer is dropped once a newer station is picked; today reopens on a new station', async () => {
  const s = I.yScale([0, 2], 'US');
  assert.equal(s.hi, 2.5, 'a top value on a tick gets a full step above it (the callout needs the room)');
  const a = setup();
  a.view.load(HNL_ST);
  let body; const slow = new Promise((r) => { body = r; });
  a.answers[0].res({ status: 200, headers: { get: () => null }, json: () => slow });
  await flush();
  a.view.load(WAI_ST);
  body(payload());
  await flush();
  assert.equal(a.view.state().hasData, false); assert.equal(a.view.state().built, false);
  a.answers[1].res(response(200, payload({ id: '1611401', obs: false })));
  await flush();
  a.q('button[data-day="0"]')[0].dispatch('click'); a.q('button[data-day="5"]')[0].dispatch('click');
  assert.deepEqual(a.view.state().open, [5]);
  a.view.load(HNL_ST);
  a.answers[2].res(response(200, payload({ obs: false })));
  await flush();
  assert.deepEqual(a.view.state().open, [0], 'a new station: today open, nothing else');
});
