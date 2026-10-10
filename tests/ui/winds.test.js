'use strict';
// static_ui/winds.js (plan section 39): the colour bands, the words for a reading, the flag (built once, updated in
// place), the feed of latest readings (schedule, partial answers, failures, deadline, hidden tab, stop / restart),
// and the 24-hour view (chart, arrows, readout, table, current reading, notes, retries, stale answers, unit / zone /
// size changes, the minute tick and the quiet refresh).
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Document } = require('./fakedom');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'winds.js'), 'utf8');
const w = {}; new Function('window', SRC)(w);
const WD = w.AllshoreWinds;
const I = WD._internals;
const KT = I.KT_MS;
const kt = (n) => n * KT;                                                    // n knots in m/s

// ---------------------------------------------------------------------------------------------- bands and words

test('bands by whole knots: calm under 1, light 1-9, moderate 10-15, fresh 16-21, strong 22-30, gale 31+', () => {
  const at = (k) => WD.bandOf(kt(k)).name;
  assert.equal(at(0), 'calm'); assert.equal(at(0.49), 'calm'); assert.equal(at(0.5), 'light');   // 0.5 kt rounds to 1
  assert.equal(at(1), 'light'); assert.equal(at(9), 'light'); assert.equal(at(9.49), 'light');
  assert.equal(at(9.5), 'moderate'); assert.equal(at(10), 'moderate'); assert.equal(at(15), 'moderate');
  assert.equal(at(16), 'fresh'); assert.equal(at(21), 'fresh'); assert.equal(at(22), 'strong'); assert.equal(at(30), 'strong');
  assert.equal(at(31), 'gale'); assert.equal(at(120), 'gale');
  assert.equal(WD.bandOf(NaN).name, 'calm'); assert.equal(WD.bandOf(null).name, 'calm');
  assert.deepEqual(WD.BANDS.map((b) => b.name), ['calm', 'light', 'moderate', 'fresh', 'strong', 'gale']);
  assert.ok(WD.BANDS.every((b) => /^#[0-9a-f]{6}$/.test(b.color)));
});

test('speeds in the site unit and knots, directions, ages', () => {
  assert.equal(WD.speedText(4.0, 'US'), '9 mph'); assert.equal(WD.speedText(4.0, 'Metric'), '14 km/h');
  assert.equal(WD.speedText(0, 'US'), '0 mph'); assert.equal(WD.speedText(4.0, 'bogus'), '9 mph');   // anything else is US
  assert.equal(WD.knotsText(4.0), '8 kt'); assert.equal(WD.knotsText(kt(31)), '31 kt');
  assert.equal(WD.arrowDeg(0), 180); assert.equal(WD.arrowDeg(75), 255); assert.equal(WD.arrowDeg(270), 90);
  assert.equal(WD.arrowDeg(359), 179); assert.equal(WD.arrowDeg(-90), 90);
  assert.equal(WD.compass(0), 'N'); assert.equal(WD.compass(75), 'ENE'); assert.equal(WD.compass(348.75), 'N');
  assert.equal(WD.compass(348.74), 'NNW'); assert.equal(WD.compass(360), 'N'); assert.equal(I.dirText(75), 'ENE (75°)');
  assert.equal(I.ageText(30), 'just now'); assert.equal(I.ageText(-50), 'just now'); assert.equal(I.ageText(60), '1 min ago');
  assert.equal(I.ageText(59 * 60 + 59), '59 min ago'); assert.equal(I.ageText(3600), '1 h ago');
  assert.equal(I.ageText(3600 + 20 * 60), '1 h 20 min ago'); assert.equal(I.ageText(6 * 3600 + 20 * 60), '6 h ago');
});

test('the words for a reading: calm, variable, a direction, gusts only when higher, the age', () => {
  const now = 1791651180000 + 12 * 60000;
  const r = { t: 1791651180, s: 4.0, g: 6.2, d: 75 };
  assert.equal(WD.readingText(r, now, 'US'), '9 mph from ENE (75°), gusts 14 mph · 12 min ago');
  assert.equal(WD.readingText(r, now, 'Metric', false), '14 km/h from ENE (75°), gusts 22 km/h');
  assert.equal(WD.readingText({ t: r.t, s: 4.0, g: 4.1, d: 75 }, now, 'US', false), '9 mph from ENE (75°)');   // same whole knots: no gust
  assert.equal(WD.readingText({ t: r.t, s: 2.6, g: null, d: null }, now, 'US', false), 'Variable, 6 mph');
  assert.equal(WD.readingText({ t: r.t, s: 0.1, g: null, d: 200 }, now, 'US', false), 'Calm');
  assert.equal(WD.readingText(null, now, 'US'), 'No recent reading');
  assert.equal(WD.readingText({ t: r.t, s: 'x' }, now, 'US'), 'No recent reading');
});

// ---------------------------------------------------------------------------------------------- the flag

test('the number sits NUM_OFFSET px upwind of the station, opposite the arrow, for every direction', () => {
  const c = I.FLAG / 2;
  for (let d = 0; d < 360; d += 7.5) {
    const p = I.numberAt(d), dx = p.x - c, dy = p.y - c;
    assert.ok(Math.abs(Math.hypot(dx, dy) - I.NUM_OFFSET) < 0.1, 'distance at ' + d);
    const bearing = (Math.atan2(dx, -dy) * 180 / Math.PI + 360) % 360;      // screen: x east, y south
    assert.ok(Math.min(Math.abs(bearing - d), 360 - Math.abs(bearing - d)) < 0.5, 'toward the wind at ' + d);
    const away = Math.abs(((bearing - WD.arrowDeg(d)) % 360 + 360) % 360 - 180);
    assert.ok(away < 0.5, 'the arrow points the other way at ' + d);
  }
  assert.deepEqual(I.numberAt(0), { x: 20, y: 6 });                        // a north wind: the number north, the arrow south
  assert.deepEqual(I.numberAt(90), { x: 34, y: 20 });
});

const NOW = Date.UTC(2026, 9, 10, 17, 0);                                    // Sat 10 Oct, 7:00 AM HST
const T0 = NOW / 1000;

test('flagState: none, calm, variable, a direction; stale past stale_s (the server\'s, else 2 h)', () => {
  const none = WD.flagState(null, NOW, 'US');
  assert.deepEqual([none.kind, none.num, none.arrow, none.classes], ['none', '', null, ['wind-none']]);
  const calm = WD.flagState({ t: T0, s: 0.2, g: null, d: 90 }, NOW, 'US');
  assert.deepEqual([calm.kind, calm.num, calm.arrow, calm.x, calm.y], ['calm', '0', null, 20, 20]);
  assert.deepEqual(calm.classes, ['wind-calm', 'wind-calm']);
  const v = WD.flagState({ t: T0, s: 2.6, g: null, d: null }, NOW, 'Metric');
  assert.deepEqual([v.kind, v.num, v.arrow, v.x, v.y], ['var', '9', null, 20, 6]);
  const d = WD.flagState({ t: T0, s: kt(18), g: kt(25), d: 315 }, NOW, 'US');
  assert.deepEqual([d.kind, d.band, d.num, d.arrow], ['dir', 'fresh', '21', 135]);
  assert.deepEqual(d.classes, ['wind-dir', 'wind-fresh']);
  assert.match(d.title, /^21 mph from NW \(315°\), gusts 29 mph · just now$/);
  const r = { t: T0 - 7200, s: 4, g: null, d: 10 };
  assert.equal(WD.flagState(r, NOW, 'US').stale, false);                    // exactly 2 h: not yet
  assert.equal(WD.flagState({ ...r, t: T0 - 7201 }, NOW, 'US').stale, true);
  assert.ok(WD.flagState({ ...r, t: T0 - 7201 }, NOW, 'US').classes.includes('wind-stale'));
  assert.equal(WD.flagState({ ...r, t: T0 - 3601 }, NOW, 'US', 3600).stale, true);   // the server's own stale_s
});

function stripped(e) { return { tag: e.tagName, kids: e.children.map(stripped) }; }

test('buildFlag builds text-only DOM; updateFlag changes text, classes and styles in place, never the elements', () => {
  const doc = new Document();
  const f = WD.buildFlag(doc, { t: T0, s: kt(12), g: null, d: 45 }, NOW, 'US');
  assert.ok(f.classList.contains('wind-flag') && f.classList.contains('wind-moderate') && f.classList.contains('wind-dir'));
  assert.equal(f.getAttribute('aria-hidden'), 'true'); assert.equal(f.getAttribute('data-kind'), 'dir');
  const [arrow, ring, numEl] = f.children;
  assert.equal(arrow.tagName, 'SVG'); assert.equal(arrow.namespaceURI, 'http://www.w3.org/2000/svg');
  assert.ok(ring.classList.contains('wind-ring') && numEl.classList.contains('wind-num'));
  assert.equal(numEl.textContent, '14'); assert.equal(arrow.style.transform, 'rotate(225deg)'); assert.equal(arrow.style.display, '');
  const p = I.numberAt(45); assert.equal(numEl.style.left, p.x + 'px'); assert.equal(numEl.style.top, p.y + 'px');
  const shape = JSON.stringify(stripped(f));
  // a calm reading, then variable, then none, then a gale: same elements, one band class at a time
  const seen = [];
  for (const r of [{ t: T0, s: 0, g: null, d: null }, { t: T0, s: kt(5), g: null, d: null }, null, { t: T0 - 9000, s: kt(40), g: null, d: 200 }]) {
    const s = WD.updateFlag(f, r, NOW, 'Metric');
    seen.push(s.kind);
    assert.equal(f.children[0], arrow); assert.equal(f.children[2], numEl); assert.equal(JSON.stringify(stripped(f)), shape);
    const bands = WD.BANDS.map((b) => 'wind-' + b.name).filter((c) => f.classList.contains(c));
    assert.equal(bands.length, s.band ? 1 : 0, 'one band class at most');
    assert.equal(arrow.style.display, s.arrow === null ? 'none' : '');
    assert.equal(numEl.textContent, s.num);
    assert.equal(f._html, '');                                                // never innerHTML
  }
  assert.deepEqual(seen, ['calm', 'var', 'none', 'dir']);
  assert.ok(f.classList.contains('wind-gale') && f.classList.contains('wind-stale') && !f.classList.contains('wind-none'));
  assert.equal(numEl.textContent, '74'); assert.equal(arrow.style.transform, 'rotate(20deg)');
  // an element without windParts (a rebuilt marker): its parts are found by class
  const g = WD.buildFlag(doc, null, NOW, 'US'); delete g.windParts;
  WD.updateFlag(g, { t: T0, s: kt(3), g: null, d: 0 }, NOW, 'US');
  assert.equal(g.children[2].textContent, '3'); assert.equal(g.children[0].style.transform, 'rotate(180deg)');
});

// ---------------------------------------------------------------------------------------------- the feed

function deferred() { let res, rej; const p = new Promise((a, b) => { res = a; rej = b; }); return { p, res, rej }; }
function fakeTimers() {
  let seq = 0; const q = new Map();
  return { set(fn, ms) { const id = ++seq; q.set(id, { fn, ms }); return id; }, clear(id) { q.delete(id); },
           pending() { return [...q.values()].map((x) => x.ms).sort((a, b) => a - b); },
           fire(ms) { for (const [id, x] of [...q]) if (ms === undefined || x.ms === ms) { q.delete(id); x.fn(); } } };
}
const flush = async () => { for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r)); };
function resp(status, body, headers) {
  return { status, ok: status >= 200 && status < 300, headers: { get: (k) => (headers || {})[k] || null },
           json: () => (body === undefined ? Promise.reject(new Error('x')) : Promise.resolve(body)) };
}
class FakeAbort { constructor() { this.signal = { aborted: false }; } abort() { this.signal.aborted = true; } }
const LATEST = { now: T0, stale_s: 7200, fields: ['id', 't', 's', 'g', 'd'], units: {},
  rows: [['coops:1612340', T0 - 600, 1.0, 2.8, 75], ['metar:PHNL', T0 - 900, 2.6, null, null], ['bad'], [5, T0, 1, 1, 1],
         ['ndbc:51003', 'x', 1, 1, 1]], missing: ['coops:1611400'] };

test('readings(): the latest table by id; malformed rows and answers skipped; the field order is the server\'s', () => {
  assert.deepEqual(WD.readings(LATEST), {
    'coops:1612340': { t: T0 - 600, s: 1.0, g: 2.8, d: 75 }, 'metar:PHNL': { t: T0 - 900, s: 2.6, g: null, d: null } });
  assert.deepEqual(WD.readings({ fields: ['s', 'id', 't'], rows: [[3, 'a:B', 9]] }), { 'a:B': { t: 9, s: 3, g: null, d: null } });
  assert.deepEqual(WD.readings(null), {}); assert.deepEqual(WD.readings({ rows: [] }), {}); assert.deepEqual(WD.readings({ fields: ['id'], rows: [] }), {});
});

function feedSetup(opts = {}) {
  const calls = [], answers = [], timers = fakeTimers();
  let visible = opts.visible !== false; const vis = [];
  const feed = WD.createWindFeed({ fetch: (url, o) => { calls.push({ url, o }); const d = deferred(); answers.push(d); return d.p; },
    timers, AbortController: FakeAbort, visibility: { isVisible: () => visible, onChange: (fn) => vis.push(fn) } });
  const data = [], status = [];
  return { feed, calls, answers, timers, data, status, vis, setVisible(v) { visible = v; vis.forEach((f) => f()); },
           start() { feed.start({ onData: (b, info) => data.push({ b, info }), onStatus: (c, t) => status.push([c, t]) }); } };
}

test('the feed asks at once, then every 5 minutes; a partial answer every 5 s (24 times at most)', async () => {
  const s = feedSetup(); s.start(); await flush();
  assert.equal(s.calls.length, 1); assert.equal(s.calls[0].url, '/api/wind/latest'); assert.deepEqual(s.status, [['loading', 'loading…']]);
  assert.deepEqual(s.calls[0].o.headers, { Accept: 'application/json' }); assert.ok(s.calls[0].o.signal);
  s.answers[0].res(resp(200, LATEST, { 'X-Wind-Stations-Partial': 'COOPS, METAR' })); await flush();
  assert.equal(s.data.length, 1); assert.deepEqual(s.data[0].info.partial, ['COOPS', 'METAR']);
  assert.equal(Object.keys(s.data[0].info.readings).length, 2); assert.deepEqual(s.timers.pending(), [I.FEED_PARTIAL_MS]);
  assert.deepEqual(s.status.at(-1), ['partial', 'loading more…']);
  for (let k = 1; k <= I.FEED_PARTIAL_MAX; k++) {
    s.timers.fire(I.FEED_PARTIAL_MS); await flush();
    s.answers[k].res(resp(200, LATEST, { 'X-Wind-Stations-Partial': 'COOPS' })); await flush();
  }
  assert.deepEqual(s.timers.pending(), [I.FEED_INTERVAL_MS], 'after 24 partial answers: the plain interval');
  assert.equal(s.status.at(-1)[0], '');
  s.timers.fire(I.FEED_INTERVAL_MS); await flush();
  s.answers.at(-1).res(resp(200, LATEST)); await flush();
  assert.deepEqual(s.timers.pending(), [I.FEED_INTERVAL_MS]); assert.equal(s.feed.state().partials, 0);
  assert.deepEqual(s.data.at(-1).info.partial, []);
});

test('the feed retries after 5, 10, 30, 30 s: HTTP errors, bad answers, network errors and the 8-s deadline', async () => {
  const s = feedSetup(); s.start(); await flush();
  s.answers[0].res(resp(500, LATEST)); await flush();                         // a well-formed body under an error status
  assert.equal(s.data.length, 0);
  assert.deepEqual(s.status.at(-1), ['unavailable', 'unavailable']); assert.deepEqual(s.timers.pending(), [5000]);
  s.timers.fire(5000); await flush();
  s.answers[1].res(resp(200, { rows: 'no' })); await flush();
  assert.deepEqual(s.timers.pending(), [10000]);
  s.timers.fire(10000); await flush();
  s.answers[2].rej(new Error('offline')); await flush();
  assert.deepEqual(s.timers.pending(), [30000]);
  s.timers.fire(30000); await flush();
  s.answers[3].res(resp(200, undefined)); await flush();                      // not JSON
  assert.deepEqual(s.timers.pending(), [30000]);
  s.timers.fire(30000); await flush();
  s.answers[4].res(resp(200, LATEST)); await flush();
  assert.equal(s.data.length, 1); assert.equal(s.feed.state().failures, 0); assert.equal(s.status.at(-1)[0], '');
  // once something is drawn a failure says "retrying"; a request that never answers is cut at the deadline
  s.timers.fire(I.FEED_INTERVAL_MS); await flush();
  assert.deepEqual(s.timers.pending(), [I.FEED_DEADLINE_MS]);
  const sig = s.calls.at(-1).o.signal;
  s.timers.fire(I.FEED_DEADLINE_MS); await flush();
  assert.equal(sig.aborted, true); assert.deepEqual(s.status.at(-1), ['retrying', 'retrying…']); assert.deepEqual(s.timers.pending(), [5000]);
  s.answers.at(-1).res(resp(200, LATEST)); await flush();                    // a late answer to the cut request: ignored
  assert.equal(s.data.length, 1);
});

test('the feed asks nothing while the tab is hidden and takes its turn when it shows', async () => {
  const s = feedSetup({ visible: false }); s.start(); await flush();
  assert.equal(s.calls.length, 0);
  s.setVisible(true); await flush();
  assert.equal(s.calls.length, 1);
  s.answers[0].res(resp(200, LATEST)); await flush();
  s.setVisible(false);
  s.timers.fire(I.FEED_INTERVAL_MS); await flush();
  assert.equal(s.calls.length, 1); assert.equal(s.feed.state().waitingVisible, true);
  s.setVisible(true); await flush();
  assert.equal(s.calls.length, 2);
  s.setVisible(true); await flush();
  assert.equal(s.calls.length, 2, 'shown again without a missed turn: nothing extra');
});

test('stop() ends the schedule and voids an answer in flight; start() again asks at once; one visibility listener', async () => {
  const s = feedSetup(); s.start(); await flush();
  const sig = s.calls[0].o.signal;
  s.feed.stop();
  assert.equal(sig.aborted, true); assert.equal(s.feed.state().running, false);
  s.answers[0].res(resp(200, LATEST)); await flush();
  assert.equal(s.data.length, 0); assert.deepEqual(s.timers.pending(), []);
  s.start(); await flush();
  assert.equal(s.calls.length, 2); assert.equal(s.vis.length, 1);
  s.start(); await flush();
  assert.equal(s.calls.length, 2, 'start while running: nothing new');
  s.answers[1].res(resp(200, LATEST)); await flush();
  assert.equal(s.data.length, 1);
  s.feed.stop(); s.setVisible(true); await flush();
  assert.equal(s.calls.length, 2, 'a stopped feed ignores the tab showing');
});

// ---------------------------------------------------------------------------------------------- the view helpers

test('hour marks every 3 or 6 hours on the zone\'s clock (half-hour zones too); local midnights marked', () => {
  const hnl = I.zoneClock('Pacific/Honolulu');
  const m3 = I.hourMarks(NOW - 86400000, NOW, 3, hnl);
  assert.equal(m3.length, 8); assert.ok(m3.every((m) => m.p.h % 3 === 0 && m.p.mi === 0));
  assert.equal(m3.filter((m) => m.midnight).length, 1); assert.equal(m3.find((m) => m.midnight).t, Date.UTC(2026, 9, 10, 10));
  assert.equal(I.hourMarks(NOW - 86400000, NOW, 6, hnl).length, 4);
  const ind = I.hourMarks(NOW - 86400000, NOW, 3, I.zoneClock('Asia/Kolkata'));
  assert.equal(ind.length, 8); assert.ok(ind.every((m) => new Date(m.t).getUTCMinutes() === 30), 'IST hours are :30 UTC');
  assert.equal(I.zoneClock('Nowhere/Atlantis').zone, 'UTC'); assert.equal(I.zoneClock('constructor').zone, 'UTC');
  assert.equal(I.hourText({ h: 0 }), '12 AM'); assert.equal(I.hourText({ h: 15 }), '3 PM');
  assert.equal(I.dateText({ wd: 'Sat', mo: 10, d: 10 }), 'Sat 10/10'); assert.equal(I.stampText({ wd: 'Sat', mo: 10, d: 10, h: 15, mi: 5 }), 'Sat 10/10, 3:05 PM');
});

test('nice steps, rows from a payload, lines with gaps, one arrow per slot, the nearest reading', () => {
  assert.equal(I.niceStep(10, 5), 2); assert.equal(I.niceStep(21, 5), 5); assert.equal(I.niceStep(43, 5), 10); assert.equal(I.niceStep(0.7, 5), 0.2);
  assert.deepEqual(I.historyRows({ t: [3, 1, 'x', 2], s: [1, 2, 3, null], g: [null, 5, 1, 1], d: [10, null, 1, 1] }),
    [{ t: 1, s: 2, g: 5, d: null }, { t: 3, s: 1, g: null, d: 10 }]);
  assert.deepEqual(I.historyRows(null), []);
  const rows = [{ t: 0, s: 1, g: 2 }, { t: 600, s: 1, g: null }, { t: 1200, s: 1, g: 2 }, { t: 1200 + I.GAP_S + 1, s: 1, g: 2 }];
  const x = (t) => t / 100, y = (v) => v;
  assert.equal(I.linePath(rows, 's', x, y), 'M0.0 1.0L6.0 1.0L12.0 1.0M' + ((1201 + I.GAP_S) / 100).toFixed(1) + ' 1.0');
  assert.equal(I.linePath(rows, 'g', x, y), 'M0.0 2.0M12.0 2.0M' + ((1201 + I.GAP_S) / 100).toFixed(1) + ' 2.0', 'a missing gust breaks its line');
  const many = []; for (let t = 0; t <= 86400; t += 360) many.push({ t, s: 1, g: null, d: 90 });
  const slots = I.arrowSlots(many, 0, 86400, 560);
  assert.equal(slots.length, 20); assert.ok(slots.every((a) => Math.abs(a.r.t - a.mid) <= 180));
  assert.equal(I.arrowSlots([{ t: 100, s: 1 }], 0, 86400, 560).length, 1);
  assert.equal(I.arrowSlots([], 0, 86400, 560).length, 0);
  assert.equal(I.nearest(many, 1000, 300).t, 1080); assert.equal(I.nearest([{ t: 0 }], 4000, 3600), null);
  assert.equal(I.sourceLink('ndbc', 'ndbc:OOUH1'), 'https://www.ndbc.noaa.gov/station_page.php?station=oouh1');
  assert.equal(I.sourceLink('coops', 'coops:1612340'), 'https://tidesandcurrents.noaa.gov/stationhome.html?id=1612340');
  assert.equal(I.sourceLink('metar', 'metar:PHNL'), 'https://aviationweather.gov/data/metar/?ids=PHNL&hours=24');
  assert.equal(I.sourceLink('nws', 'nws:001HE'), 'https://api.weather.gov/stations/001HE/observations/latest');
  assert.equal(I.sourceLink('x', 'x:1'), null);
});

// ---------------------------------------------------------------------------------------------- the view

function history(extra) {
  const t = [], s = [], g = [], d = [];
  for (let k = 0; k < 240; k++) {                                           // 6-minute readings over 24 h ending now
    const tt = T0 - (239 - k) * 360;
    if (k >= 100 && k < 120) continue;                                       // a 2-hour hole: a gap in the lines
    t.push(tt); s.push(+(3 + 2 * Math.sin(k / 20)).toFixed(1)); g.push(k % 5 ? +(5 + 2 * Math.sin(k / 20)).toFixed(1) : null);
    d.push(k === 230 ? null : (60 + k) % 360);
  }
  return Object.assign({ id: 'coops:1612340', name: 'Honolulu', kind: 'gauge', src: 'coops', tz: 'Pacific/Honolulu', alias: 'OOUH1',
    hours: 24, units: { s: 'm/s' }, stale_s: 7200, now: T0, t, s, g, d, source: 'NOAA CO-OPS (tidesandcurrents.noaa.gov)', via: null, note: null }, extra || {});
}
const bodyRows = (s) => s.els.table.querySelector('tbody').querySelectorAll('tr');   // the fake DOM: no descendant selectors
const HNL = { id: 'coops:1612340', name: 'Honolulu', tz: 'Pacific/Honolulu', kind: 'gauge', alias: 'OOUH1' };

function viewSetup(opts = {}) {
  const doc = new Document();
  const mk = (tag, id) => doc.register(doc.createElement(tag), id);
  const els = { content: mk('div', 'windContent'), loading: mk('div', 'windLoading'), error: mk('div', 'windError'),
    errorText: mk('span', 'windErrorText'), retry: mk('button', 'windRetry'), current: mk('div', 'windCurrent'),
    conditions: mk('div', 'windConditions'), chart: mk('div', 'windChart'), arrows: mk('div', 'windArrows'),
    temp: mk('div', 'windTemp'), table: mk('div', 'windTable'), meta: mk('div', 'windMeta') };
  if (opts.oldEls) { delete els.conditions; delete els.temp; }                 // a page without the step-5d elements
  const calls = [], answers = [], timers = fakeTimers();
  let visible = opts.visible !== false, nowMs = opts.now || NOW, width = opts.width || 640;
  const retried = [];
  const view = WD.createWindView({ els, document: doc, timers, now: () => nowMs, visible: () => visible, unit: opts.unit || 'US',
    width: () => width, AbortController: FakeAbort, wallClock: opts.wallClock, onRetried: () => retried.push(1),
    zoneAbbr: (ms, tz) => (tz === 'Pacific/Honolulu' ? 'HST' : tz),
    fetch: (url, o) => { calls.push({ url, o }); const dd = deferred(); answers.push(dd); return dd.p; } });
  return { doc, els, view, calls, answers, timers, retried, q: (sel) => els.chart.querySelectorAll(sel),
           setVisible(v) { visible = v; }, setNow(t) { nowMs = t; }, setWidth(v) { width = v; } };
}

test('a station loads: the chart, the arrows, the current reading, the table, the notes', async () => {
  const s = viewSetup();
  s.view.load(HNL, { unit: 'US' });
  assert.equal(s.view.state().status, 'loading'); assert.equal(s.calls[0].url, '/api/wind/coops%3A1612340/history');
  assert.ok(!s.els.loading.classList.contains('d-none') && s.els.content.classList.contains('d-none'));
  s.answers[0].res(resp(200, history())); await flush();
  const st = s.view.state();
  assert.equal(st.status, 'ready'); assert.equal(st.built, true); assert.equal(st.rows, 220); assert.equal(st.width, 640);
  assert.ok(s.els.loading.classList.contains('d-none') && !s.els.content.classList.contains('d-none'));
  const svg = s.q('svg')[0];
  assert.equal(svg.getAttribute('width'), '640'); assert.equal(svg.getAttribute('height'), String(I.CHART_H));
  const speed = s.q('.wind-speed')[0].getAttribute('d'), gust = s.q('.wind-gust')[0].getAttribute('d');
  assert.equal((speed.match(/M/g) || []).length, 2, 'the 2-hour hole splits the speed line');
  assert.ok((gust.match(/M/g) || []).length > 2, 'a missing gust breaks the gust line');
  assert.equal(s.q('.wind-xtick').length, 8); assert.equal(s.q('.wind-xdate').length, 1);
  assert.equal(s.q('.wind-xdate')[0].textContent, 'Sat 10/10'); assert.equal(s.q('.wind-midnight').length, 1);
  const yt = s.q('.wind-ytick').map((e) => +e.textContent), kt2 = s.q('.wind-ktick').map((e) => +e.textContent);
  assert.equal(yt[0], 0); assert.ok(yt.at(-1) >= 15 && yt.length <= 7); assert.equal(kt2[0], 0); assert.ok(kt2.length >= 3);
  assert.equal(s.q('.wind-last').length, 1);
  const arrows = s.els.arrows.querySelectorAll('.wind-dir'), dots = s.els.arrows.querySelectorAll('.wind-dir-none');
  const plotW = 640 - I.MARGIN.l - I.MARGIN.r;
  assert.ok(arrows.length + dots.length <= Math.floor(plotW / I.ARROW_SLOT) && arrows.length >= 15);
  assert.match(arrows[0].getAttribute('transform'), /rotate\(\d+ 20 20\)$/);
  const cur = s.els.current.textContent;
  assert.match(cur, /^\d+ mph from [A-Z]+ \(\d+°\)(, gusts \d+ mph)? · \d+ kt · just now$/);
  const rows = bodyRows(s);
  assert.equal(rows.length, 48); assert.equal(I.TABLE_MAX, 48);
  assert.equal(rows[0].children[0].textContent, 'Sat 10/10, 7:00 AM', 'newest first');
  assert.match(rows[0].children[1].textContent, /^\d+ mph \(\d+ kt\)$/);
  assert.equal(s.els.table.querySelector('caption').textContent, 'Readings, newest first (times in HST)');
  const dirs = rows.map((r) => r.children[3].textContent);
  assert.ok(dirs.includes('Variable')); assert.ok(rows.map((r) => r.children[2].textContent).includes('–'));
  const meta = s.els.meta.children.map((e) => e.textContent);
  assert.deepEqual(meta.slice(0, -1), ['Times in HST.'], 'the zone, nothing more (owner, 2026-10-10); then the source');
  assert.ok(meta[meta.length - 1].startsWith('Source: '));
  const a = s.els.meta.querySelector('a');
  assert.equal(a.getAttribute('href'), 'https://tidesandcurrents.noaa.gov/stationhome.html?id=1612340');
  assert.equal(a.textContent, 'NOAA CO-OPS (tidesandcurrents.noaa.gov)'); assert.equal(a.getAttribute('rel'), 'noopener');
  assert.ok(s.timers.pending().includes(I.TICK_MS));
});

function weather(extra) {
  const h = history(extra);
  h.at = h.t.map((t, i) => (i % 7 === 3 ? null : +(24 + 3 * Math.sin(i / 30)).toFixed(1)));
  h.wt = h.t.map(() => 27.2); h.dp = h.t.map(() => 20.0); h.rh = h.t.map(() => 78); h.p = h.t.map((t, i) => +(1014 - i / 100).toFixed(1));
  h.conditions = { at: [T0, 25.6], wt: [T0, 27.2], rh: [T0, 78], dp: [T0, 20.0], p: [T0, 1011.8], trend: -1.2, vis: [T0, 16.1], wx: [T0, 'light rain'] };
  return h;
}

test('the weather in the site\'s unit: temperatures, pressure, distances, the trend words, the conditions line', () => {
  assert.equal(I.tempText(25.6, 'US'), '78°F'); assert.equal(I.tempText(25.6, 'Metric'), '25.6°C'); assert.equal(I.tempText(-0.04, 'US'), '32°F');
  assert.equal(I.pressText(1011.8, 'US'), '29.88 inHg'); assert.equal(I.pressText(1011.8, 'Metric'), '1012 hPa');
  assert.equal(I.distText(16.1, 'US'), '10 mi'); assert.equal(I.distText(0.8, 'US'), '0.5 mi'); assert.equal(I.distText(16.1, 'Metric'), '16 km'); assert.equal(I.distText(2.45, 'Metric'), '2.5 km');
  assert.deepEqual([I.trendText(1.2), I.trendText(-0.6), I.trendText(0.3), I.trendText(null)], ['rising', 'falling', 'steady', '']);
  const c = weather().conditions;
  assert.equal(I.conditionsText(c, 'US'), 'Air 78°F · Water 81°F · Humidity 78% · Dew point 68°F · Pressure 29.88 inHg, falling · Visibility 10 mi · Light rain');
  assert.equal(I.conditionsText(c, 'Metric'), 'Air 25.6°C · Water 27.2°C · Humidity 78% · Dew point 20°C · Pressure 1012 hPa, falling · Visibility 16 km · Light rain');
  assert.equal(I.conditionsText({ p: [1, 1020.0], trend: 0.1 }, 'US'), 'Pressure 30.12 inHg, steady');
  assert.equal(I.conditionsText({ at: [1, 'x'], wx: [1, ''] }, 'US'), ''); assert.equal(I.conditionsText(null, 'US'), ''); assert.equal(I.conditionsText({}, 'US'), '');
});

test('a station with weather: the conditions line, the temperature chart on the wind chart\'s axis, Air / Water columns, the readout; without weather nothing of it', async () => {
  const s = viewSetup();
  s.view.load(HNL, { unit: 'US' }); s.answers[0].res(resp(200, weather())); await flush();
  assert.equal(s.view.state().temp, true);
  assert.equal(s.els.conditions.textContent, 'Air 78°F · Water 81°F · Humidity 78% · Dew point 68°F · Pressure 29.88 inHg, falling · Visibility 10 mi · Light rain');
  assert.ok(!s.els.conditions.classList.contains('d-none'));
  const tsvg = s.els.temp.querySelectorAll('svg')[0];
  assert.equal(tsvg.getAttribute('height'), String(I.TEMP_H)); assert.equal(tsvg.getAttribute('width'), '640');
  const air = s.els.temp.querySelectorAll('.wind-air')[0].getAttribute('d'), water = s.els.temp.querySelectorAll('.wind-water')[0].getAttribute('d');
  assert.ok((air.match(/M/g) || []).length >= 30, 'every 7th air temperature is missing: the air line breaks there');
  assert.equal((water.match(/M/g) || []).length, 2, 'the 2-hour hole splits the water line');
  const ticks = s.els.temp.querySelectorAll('.wind-ttick').map((e) => +e.textContent);
  assert.ok(ticks.length >= 3 && ticks[0] <= 72 && ticks.at(-1) >= 82, 'the ticks span the temperatures in °F');
  assert.equal(s.els.temp.querySelectorAll('.wind-tunit')[0].textContent, '°F');
  assert.equal(s.els.temp.querySelectorAll('.wind-midnight').length, 1, 'the same marks as the wind chart');
  const firstX = (d) => +d.slice(1).split(' ')[0];
  assert.equal(firstX(water), firstX(s.q('.wind-speed')[0].getAttribute('d')), 'the same x axis');
  const rows = bodyRows(s), heads = s.els.table.querySelector('thead').querySelector('tr').children.map((e) => e.textContent);
  assert.deepEqual(heads, ['Time', 'Speed', 'Gust', 'Direction', 'Air', 'Water']);
  assert.equal(rows[0].children[5].textContent, '81°F'); assert.match(rows[0].children[4].textContent, /^(\d+°F|–)$/);
  assert.ok(rows.some((r) => r.children[4].textContent === '–'), 'a missing air temperature is a dash');
  // the readout names the air temperature
  const svg = s.q('svg')[0]; svg.rect = { left: 0, top: 0, width: 640, height: I.CHART_H };
  s.els.chart.dispatch('mousemove', { clientX: 640 - I.MARGIN.r, clientY: 50 });
  assert.match(s.view.state().readout, / · Air \d+°F$/);
  // Metric re-renders the line and the chart
  s.view.setUnit('Metric');
  assert.equal(s.els.conditions.textContent.slice(0, 29), 'Air 25.6°C · Water 27.2°C · H'); assert.equal(s.els.temp.querySelectorAll('.wind-tunit')[0].textContent, '°C');
  // a new load clears the previous station's weather while the answer is awaited
  s.view.load(HNL, { unit: 'US' });
  assert.equal(s.els.conditions.textContent, ''); assert.equal(s.els.temp.querySelectorAll('svg').length, 0);
  // a plain answer: no line, no chart, four columns
  s.answers[1].res(resp(200, history())); await flush();
  assert.equal(s.els.conditions.textContent, ''); assert.ok(s.els.conditions.classList.contains('d-none'));
  assert.equal(s.els.temp.querySelectorAll('svg').length, 0); assert.equal(s.view.state().temp, false);
  assert.deepEqual(s.els.table.querySelector('thead').querySelector('tr').children.map((e) => e.textContent), ['Time', 'Speed', 'Gust', 'Direction']);
  // a page without the new elements still works
  const o = viewSetup({ oldEls: true });
  o.view.load(HNL, { unit: 'US' }); o.answers[0].res(resp(200, weather())); await flush();
  assert.equal(o.view.state().status, 'ready'); assert.equal(o.view.state().temp, true);
});

test('narrow windows: hour ticks every 6 h and fewer arrows; resize rebuilds only past 8 px', async () => {
  const s = viewSetup({ width: 300 });
  s.view.load(HNL); s.answers[0].res(resp(200, history())); await flush();
  assert.equal(s.view.state().width, 300); assert.equal(s.q('.wind-xtick').length, 4);
  assert.ok(s.els.arrows.querySelectorAll('.wind-dir, .wind-dir-none').length <= Math.floor((300 - I.MARGIN.l - I.MARGIN.r) / I.ARROW_SLOT));
  const svg = s.q('svg')[0];
  s.setWidth(305); s.view.resize(); assert.equal(s.q('svg')[0], svg, 'within 8 px: the same chart');
  s.setWidth(200); s.view.resize(); assert.equal(s.view.state().width, I.MIN_WIDTH, 'never narrower than MIN_WIDTH');
  assert.notEqual(s.q('svg')[0], svg);
});

test('empty, calm and stale answers; the relay note; the source of an airport and a buoy', async () => {
  const s = viewSetup();
  s.view.load(HNL); s.answers[0].res(resp(200, history({ t: [], s: [], g: [], d: [], note: 'No wind readings in the last 24 hours' }))); await flush();
  assert.equal(s.view.state().status, 'ready'); assert.equal(s.q('.wind-empty').length, 1); assert.equal(s.q('.wind-speed').length, 0);
  assert.equal(s.q('.wind-empty')[0].textContent, I.MSG.empty, 'no reading at all: no readings in 24 hours');
  assert.equal(s.els.current.textContent, I.MSG.empty);
  assert.ok(s.els.meta.children.some((e) => e.textContent === 'No wind readings in the last 24 hours.'));
  s.view.load(HNL); s.answers[1].res(resp(200, history({ t: [T0 - 3 * 3600], s: [0.1], g: [null], d: [null], via: 'ndbc', source: 'NOAA National Data Buoy Center (ndbc.noaa.gov)' }))); await flush();
  assert.equal(s.els.current.textContent, 'Calm · 3 h ago');
  assert.ok(s.els.current.querySelector('.wind-stale'));
  const meta = s.els.meta.children.map((e) => e.textContent);
  assert.ok(meta.includes('Readings from NDBC’s copy of this gauge (NOAA CO-OPS did not answer).'));
  assert.ok(meta.includes('The latest reading is more than 2 hours old.'));
  assert.equal(bodyRows(s)[0].children[3].textContent, 'Calm');
  const air = { id: 'metar:PHNL', name: 'Honolulu Intl', tz: 'Pacific/Honolulu', kind: 'airport' };
  s.view.load(air); s.answers[2].res(resp(200, history({ id: 'metar:PHNL', kind: 'airport', src: 'metar', alias: null, source: 'NWS' }))); await flush();
  assert.equal(s.els.meta.children[0].textContent, 'Times in HST.');
  assert.equal(s.els.meta.querySelector('a').getAttribute('href'), 'https://aviationweather.gov/data/metar/?ids=PHNL&hours=24');
});

test('no history (NDBC has no 24-hour file, step 5 F4): the current reading is the flag\'s own, passed with the load; the note shows', async () => {
  const s = viewSetup();
  const reading = { t: T0 - 1200, s: 4.0, g: null, d: 40 };
  s.view.load(HNL, { reading }); s.answers[0].res(resp(200, history({ t: [], s: [], g: [], d: [], note: 'NDBC publishes no 24-hour history for this station; the flag shows its latest report' }))); await flush();
  assert.equal(s.view.state().status, 'ready'); assert.equal(s.q('.wind-empty').length, 1);
  assert.equal(s.els.current.textContent, '9 mph from NE (40°) · 8 kt · 20 min ago');
  assert.equal(s.q('.wind-empty')[0].textContent, I.MSG.noHistory, 'the chart never says "no readings" under the flag\'s reading');
  assert.equal(I.MSG.noHistory, 'No 24-hour history for this station.');
  assert.ok(s.els.meta.children.some((e) => e.textContent === 'NDBC publishes no 24-hour history for this station; the flag shows its latest report.'));
  s.view.load(HNL, { reading: { t: T0, s: 'x' } }); s.answers[1].res(resp(200, history({ t: [], s: [], g: [], d: [] }))); await flush();
  assert.equal(s.els.current.textContent, I.MSG.empty, 'a reading without a number is no reading');
  assert.equal(s.q('.wind-empty')[0].textContent, I.MSG.empty);
  s.view.load(HNL, { reading }); s.answers[2].res(resp(200, history())); await flush();
  assert.match(s.els.current.textContent, /^\d+ mph from [A-Z]+ \(\d+°\)(, gusts \d+ mph)? · \d+ kt · just now$/, 'with a history the history\'s latest reading');
});

test('a busy server is asked again after its Retry-After (capped); unknown and failed say so; Retry', async () => {
  const s = viewSetup();
  s.view.load(HNL);
  s.answers[0].res(resp(503, { retry: true }, { 'Retry-After': '7' })); await flush();
  assert.deepEqual(s.timers.pending(), [7000]); assert.equal(s.view.state().status, 'loading');
  s.timers.fire(7000); await flush();
  s.answers[1].res(resp(503, { retry: true }, { 'Retry-After': '600' })); await flush();
  assert.deepEqual(s.timers.pending(), [I.RETRY_MAX_S * 1000]);
  s.timers.fire(); await flush();
  s.answers[2].res(resp(503, { error: 'no retry flag' })); await flush();
  assert.equal(s.view.state().status, 'error'); assert.equal(s.els.errorText.textContent, I.MSG.busy); assert.equal(s.els.retry.hidden, false);
  s.els.retry.dispatch('click'); await flush();
  assert.equal(s.calls.length, 4);
  s.answers[3].res(resp(200, history())); await flush();
  assert.equal(s.view.state().status, 'ready'); assert.deepEqual(s.retried, [1]);
  s.view.load(HNL); s.answers[4].res(resp(404, { error: 'Unknown wind station' })); await flush();
  assert.equal(s.view.state().status, 'final'); assert.equal(s.els.errorText.textContent, I.MSG.unknown); assert.equal(s.els.retry.hidden, true);
  s.view.load(HNL); s.answers[5].rej(new Error('offline')); await flush();
  assert.equal(s.els.errorText.textContent, I.MSG.failed); assert.equal(s.els.retry.hidden, false);
  s.view.load(HNL); s.answers[6].rej(Object.assign(new Error('x'), { name: 'AbortError' })); await flush();
  assert.equal(s.view.state().status, 'loading', 'an abort says nothing');
});

test('the retries stop after RETRY_MAX tries', async () => {
  const s = viewSetup();
  s.view.load(HNL);
  for (let k = 0; k < I.RETRY_MAX; k++) {
    s.answers[k].res(resp(503, { retry: true })); await flush();
    if (k < I.RETRY_MAX - 1) { assert.deepEqual(s.timers.pending(), [5000]); s.timers.fire(5000); }
  }
  assert.equal(s.view.state().status, 'error'); assert.deepEqual(s.timers.pending(), []);
});

test('a newer load voids the older answer; clear() voids everything', async () => {
  const s = viewSetup();
  s.view.load(HNL);
  const sig = s.calls[0].o.signal;
  s.view.load({ id: 'ndbc:51003', name: 'Western Hawaii', tz: 'Pacific/Honolulu', kind: 'buoy' });
  assert.equal(sig.aborted, true);
  s.answers[1].res(resp(200, history({ id: 'ndbc:51003', kind: 'buoy', src: 'ndbc', alias: null }))); await flush();
  s.answers[0].res(resp(200, history())); await flush();
  assert.equal(s.view.state().station, 'ndbc:51003'); assert.equal(s.els.meta.children[0].textContent, 'Times in HST.');
  s.view.clear();
  assert.equal(s.view.state().status, 'idle'); assert.equal(s.els.chart.children.length, 0); assert.deepEqual(s.timers.pending(), []);
});

test('unit and zone changes re-render without asking again; a hidden window builds when shown', async () => {
  const s = viewSetup({ visible: false });
  s.view.load(HNL); s.answers[0].res(resp(200, history())); await flush();
  assert.equal(s.view.state().built, false); assert.equal(s.view.state().status, 'ready');
  s.setVisible(true); await s.view.show();
  assert.equal(s.view.state().built, true);
  s.view.setUnit('Metric');
  assert.match(s.els.current.textContent, / km\/h /); assert.equal(s.q('.wind-yunit')[0].textContent, 'km/h');
  assert.equal(s.els.meta.children[0].textContent, 'Times in HST.');
  s.view.setZone('UTC');
  assert.equal(s.view.state().zone, 'UTC'); assert.equal(bodyRows(s)[0].children[0].textContent, 'Sat 10/10, 5:00 PM');
  assert.equal(s.els.meta.children[0].textContent, 'Times in UTC.');
  assert.equal(s.q('.wind-xdate').length, 1); assert.equal(s.q('.wind-midnight').length, 1);
  assert.equal(s.calls.length, 1);
});

test('the readout: the nearest reading under the pointer, nothing far from any reading; taps end with the tap', async () => {
  let wall = 1e6;
  const s = viewSetup({ wallClock: () => wall });
  s.view.load(HNL); s.answers[0].res(resp(200, history())); await flush();
  const svg = s.q('svg')[0];
  svg.rect = { left: 100, top: 50, width: 640, height: I.CHART_H };
  const plotW = 640 - I.MARGIN.l - I.MARGIN.r;
  s.els.chart.dispatch('mousemove', { clientX: 100 + I.MARGIN.l + plotW, clientY: 100 });   // the right edge: now
  assert.match(s.view.state().readout, /^Sat 10\/10, 7:00 AM · \d+ mph .* · \d+ kt$/);
  s.els.chart.dispatch('mousemove', { clientX: 100 + I.MARGIN.l + plotW * (109.5 / 240), clientY: 100 });   // inside the 2-h hole
  assert.equal(s.view.state().readout, '', 'more than 45 min from any reading: nothing');
  s.els.chart.dispatch('mousemove', { clientX: 100 + 5, clientY: 100 });    // the left margin
  assert.equal(s.view.state().readout, '');
  s.els.chart.dispatch('mousemove', { clientX: 300, clientY: 400 });        // below the chart
  assert.equal(s.view.state().readout, '');
  s.els.chart.dispatch('touchstart', { touches: [{ clientX: 100 + I.MARGIN.l + plotW, clientY: 100 }] });
  assert.notEqual(s.view.state().readout, '');
  s.els.chart.dispatch('touchend', {});
  assert.equal(s.view.state().readout, '');
  s.els.chart.dispatch('mousemove', { clientX: 100 + I.MARGIN.l + plotW, clientY: 100 });   // the touch's own mouse event
  assert.equal(s.view.state().readout, '');
  wall += I.TOUCH_MOUSE_MS + 1;
  s.els.chart.dispatch('mousemove', { clientX: 100 + I.MARGIN.l + plotW, clientY: 100 });
  assert.notEqual(s.view.state().readout, '');
  svg.rect = { left: 100, top: 50, width: 320, height: I.CHART_H / 2 };     // drawn at half size: pointer scaled back
  s.els.chart.dispatch('mousemove', { clientX: 100 + (I.MARGIN.l + plotW) / 2, clientY: 60 });
  assert.match(s.view.state().readout, /7:00 AM/);
});

test('every minute the age moves; after 5 minutes the readings are asked again, quietly', async () => {
  const s = viewSetup();
  s.view.load(HNL); s.answers[0].res(resp(200, history())); await flush();
  for (let m = 1; m <= 4; m++) { s.setNow(NOW + m * 60000); s.timers.fire(I.TICK_MS); }
  assert.match(s.els.current.textContent, / · 4 min ago$/); assert.equal(s.calls.length, 1);
  s.setNow(NOW + 5 * 60000); s.timers.fire(I.TICK_MS); await flush();
  assert.equal(s.calls.length, 2); assert.equal(s.calls[1].o.cache, 'no-cache'); assert.equal(s.calls[1].url, s.calls[0].url);
  s.answers[1].res(resp(500, { error: 'x' })); await flush();
  assert.equal(s.view.state().status, 'ready'); assert.equal(s.view.state().rows, 220, 'a failed refresh keeps the chart');
  s.setNow(NOW + 10 * 60000); s.timers.fire(I.TICK_MS); await flush();
  assert.equal(s.calls.length, 3);
  const h = history({ t: [T0 + 540], s: [9], g: [null], d: [180] });
  s.answers[2].res(resp(200, h)); await flush();
  assert.equal(s.view.state().rows, 1); assert.match(s.els.current.textContent, /^20 mph from S \(180°\) · 17 kt · 1 min ago$/);
  assert.ok(s.timers.pending().includes(I.TICK_MS));
});
