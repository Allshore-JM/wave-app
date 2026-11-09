'use strict';
// Land weather from the visitor's browser (plan section 91c): the forecast loader (static_ui/forecast.js) asks
// /api/forecast, then Open-Meteo itself (om_url), then POSTs that answer to /api/forecast/weather. Held answers,
// failures, aborts, the point tool's prefetch.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { fakeWindow } = require('./fakedom');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'forecast.js'), 'utf8');
function load(win) { new Function('window', 'URLSearchParams', SRC)(win, URLSearchParams); return win.AllshoreForecast; }
const tick = () => new Promise((r) => setImmediate(r));
async function settle(n) { for (let i = 0; i < (n || 10); i++) await tick(); }

const POINT = 'pt_21450N_158000W';
const OM_URL = 'https://api.open-meteo.com/v1/forecast?latitude=21.45&longitude=-158.0&hourly=temperature_2m';
const OM_TEXT = '{"latitude": 21.5, "longitude": -158.0, "hourly": {"time": ["2026-11-09T00:00"]}}';

function getAnswer(over) {
  return Object.assign({ station: POINT, kind: 'weather', model: 'GFS', weather_models: ['GFS', 'ECMWF', 'ICON', 'AIFS', 'NWS'],
    om_url: OM_URL, om_key: 'k1', table_html: null, error: null, graph_data: null, graph_header: null }, over || {});
}
function weatherAnswer(over) {
  return Object.assign({ station: POINT, kind: 'weather', model: 'GFS', table_html: '<table class="wx-table"></table>', error: null,
    graph_data: { kind: 'weather', labels: [] }, graph_header: { cycle: 'Weather · GFS run 2026-11-09 00 UTC', location: 'x', tz: 'UTC' } }, over || {});
}

// a fetch whose answers are released by hand: c.json(d, status), c.text(t, status, contentLength), c.fail()
function stub() {
  const calls = [];
  const fetch = (url, o) => new Promise((res, rej) => {
    const c = { url, o: o || {}, signal: o && o.signal, aborted: false };
    const resp = (status, text, len) => ({ ok: !status || status < 400, status: status || 200,
      headers: { get: (k) => (k === 'content-length' && len != null ? String(len) : null) },
      text: () => Promise.resolve(text), json: () => Promise.resolve(JSON.parse(text)) });
    c.json = (d, status) => res(resp(status, JSON.stringify(d)));
    c.text = (t, status, len) => res(resp(status, t, len));
    c.fail = () => rej(new TypeError('Failed to fetch'));
    if (c.signal) {
      const abort = () => { c.aborted = true; rej(Object.assign(new Error('aborted'), { name: 'AbortError' })); };
      if (c.signal.aborted) abort(); else c.signal.addEventListener('abort', abort);
    }
    calls.push(c);
  });
  return { fetch, calls, last: () => calls[calls.length - 1],
    kinds: () => calls.map((c) => (c.url.startsWith('https://api.open-meteo.com/') ? 'om' : c.url.startsWith('/api/forecast/weather') ? 'post' : 'get')) };
}
function fakeTimers() {
  const all = [];
  return { all, set: (fn, ms) => { const t = { fn, ms, on: true }; all.push(t); return t; }, clear: (t) => { if (t) t.on = false; },
    fire: (ms) => { all.filter((t) => t.on && (ms == null || t.ms === ms)).forEach((t) => { t.on = false; t.fn(); }); } };
}
function ui() { const r = { applied: [], busy: [], errors: [] }; r.ui = { apply: (d, s) => r.applied.push([d, { ...s }]), busy: (b) => r.busy.push(b), error: (m, retry) => r.errors.push([m, retry]) }; return r; }

function setup(over) {
  const F = load(fakeWindow()), I = F._internals, f = stub(), u = ui(), timers = fakeTimers(), clock = { t: 1000 };
  const st = Object.assign({ station: POINT, tz: '', unit: 'US', model: 'GFS', view: 'Table' }, over || {});
  const L = I.createLoader({ fetch: f.fetch, now: () => clock.t, replaceState: () => {}, swanStations: [], ui: u.ui,
    landWeather: true, setTimer: timers.set, clearTimer: timers.clear }, st);
  return { I, f, u, timers, clock, L, st };
}
// one whole land load: the GET, Open-Meteo, the POST
async function landLoad(t, next, getOver, postOver) {
  const p = t.L.load(next || {}); await settle();
  t.f.last().json(getAnswer(getOver)); await settle();
  return { p };
}

test('a land point: the GET, then Open-Meteo from the browser, then the POST of its answer; the table shown and kept', async () => {
  const t = setup();
  const { p: p } = await landLoad(t);
  assert.deepEqual(t.f.kinds(), ['get', 'om']);
  const om = t.f.last();
  assert.equal(om.url, OM_URL); assert.equal(om.o.credentials, 'omit'); assert.ok(om.signal, 'the Open-Meteo fetch can be aborted');
  om.text(OM_TEXT); await settle();
  assert.deepEqual(t.f.kinds(), ['get', 'om', 'post']);
  const post = t.f.last();
  assert.equal(post.o.method, 'POST'); assert.equal(post.o.body, OM_TEXT);
  assert.equal(post.o.headers['Content-Type'], 'text/plain');
  assert.equal(post.url.split('?')[1], t.f.calls[0].url.split('?')[1], 'the POST carries the GET\'s own query');
  post.json(weatherAnswer());
  const d = await p;
  assert.equal(d.table_html, weatherAnswer().table_html);
  assert.equal(t.u.applied.length, 1); assert.equal(t.u.applied[0][0].table_html, weatherAnswer().table_html);
  assert.equal(t.L.cache.size, 1, 'the built table is kept like any forecast');
  assert.equal(t.timers.all.filter((x) => x.on).length, 0, 'the Open-Meteo deadline is cleared');
  t.L.load({}); await settle();
  assert.equal(t.f.calls.length, 3, 'the same view again: from the cache, nothing asked');
});

test('the NWS tab where the server falls back to GFS: the POST asks for the model the server answered with (G60 A-1)', async () => {
  const t = setup();
  const { p } = await landLoad(t, { model: 'NWS' }, { model: 'GFS' });
  assert.equal(new URLSearchParams(t.f.calls[0].url.split('?')[1]).get('model'), 'NWS');
  t.f.last().text(OM_TEXT); await settle();
  const post = new URLSearchParams(t.f.last().url.split('?')[1]);
  assert.equal(t.f.last().o.method, 'POST');
  assert.equal(post.get('model'), 'GFS', 'never the NWS the server refuses on the POST');
  assert.equal(post.get('station'), POINT);
  t.f.last().json(weatherAnswer()); const d = await p;
  assert.equal(d.table_html, weatherAnswer().table_html);
});

test('another tab, unit or Retry reuses the held Open-Meteo answer; a new model run asks Open-Meteo again', async () => {
  const t = setup();
  const { p: p } = await landLoad(t); t.f.last().text(OM_TEXT); await settle(); t.f.last().json(weatherAnswer()); await p;
  const { p: p2 } = await landLoad(t, { model: 'ECMWF' }, { model: 'ECMWF' });
  assert.deepEqual(t.f.kinds().slice(3), ['get', 'post'], 'no second Open-Meteo request for the same runs');
  assert.equal(t.f.last().o.body, OM_TEXT);
  assert.equal(new URLSearchParams(t.f.last().url.split('?')[1]).get('model'), 'ECMWF');
  t.f.last().json(weatherAnswer({ model: 'ECMWF' })); await p2;
  const { p: p3 } = await landLoad(t, { unit: 'Metric' }, { om_key: 'k2' });
  assert.deepEqual(t.f.kinds().slice(5), ['get', 'om'], 'a new run (om_key) asks Open-Meteo again');
  t.f.last().text(OM_TEXT + ' '); await settle(); t.f.last().json(weatherAnswer()); await p3;
  t.clock.t += I_held(t) + 1;
  const { p: p4 } = await landLoad(t, { model: 'AIFS' }, { om_key: 'k2', model: 'AIFS' });
  assert.equal(t.f.kinds().slice(-1)[0], 'om', 'held no longer than OM_HELD_MS');
  t.f.last().text(OM_TEXT); await settle(); t.f.last().json(weatherAnswer()); await p4;
});
function I_held(t) { return t.I.OM_HELD_MS; }

test('Open-Meteo refusing the visitor (429) says busy; no answer says unavailable; neither is posted or kept', async () => {
  for (const [how, want] of [['429', 'OM_BUSY'], ['500', 'OM_DOWN'], ['net', 'OM_DOWN'], ['empty', 'OM_DOWN']]) {
    const t = setup();
    const { p: p } = await landLoad(t);
    const om = t.f.last();
    if (how === 'net') om.fail(); else if (how === 'empty') om.text(''); else om.text('{"error":true}', Number(how));
    const d = await p;
    assert.equal(d.error, t.I[want], how); assert.ok(!d.om_url, 'the answer shown never asks again by itself');
    assert.deepEqual(t.f.kinds(), ['get', 'om'], `${how}: nothing posted`);
    assert.equal(t.L.cache.size, 0, `${how}: not kept`);
    assert.equal(t.u.applied[0][0].error, t.I[want]);
  }
});

test('Open-Meteo slower than the deadline: the request is aborted and the weather is unavailable', async () => {
  const t = setup();
  const { p: p } = await landLoad(t);
  const om = t.f.last();
  assert.equal(t.timers.all.filter((x) => x.on && x.ms === t.I.OM_TIMEOUT_MS).length, 1);
  t.timers.fire(t.I.OM_TIMEOUT_MS); await settle();
  assert.equal(om.aborted, true);
  assert.equal((await p).error, t.I.OM_DOWN);
  assert.deepEqual(t.f.kinds(), ['get', 'om']);
});

test('an answer over the size cap is never posted (by its header, or by its text)', async () => {
  for (const big of ['header', 'text']) {
    const t = setup();
    const { p: p } = await landLoad(t);
    if (big === 'header') t.f.last().text(OM_TEXT, 200, t.I.OM_BODY_MAX + 1);
    else t.f.last().text('x'.repeat(t.I.OM_BODY_MAX + 1));
    assert.equal((await p).error, t.I.OM_DOWN, big);
    assert.deepEqual(t.f.kinds(), ['get', 'om']);
  }
});

test('an om_url to another host is never fetched', async () => {
  const t = setup();
  const { p: p } = await landLoad(t, {}, { om_url: 'https://evil.example/collect?x=1' });
  assert.equal((await p).error, t.I.OM_DOWN);
  assert.deepEqual(t.f.kinds(), ['get']);
});

test('a newer load aborts the Open-Meteo request; a late POST of a superseded load writes nothing', async () => {
  const t = setup();
  const { p: p1 } = await landLoad(t);
  const om = t.f.last();
  const p2 = t.L.load({ station: '51201' }); await settle();
  assert.equal(om.aborted, true, 'the superseded Open-Meteo request is aborted');
  assert.equal(t.f.kinds().filter((k) => k === 'post').length, 0);
  t.f.last().json({ station: '51201', table_html: '<table></table>', model: 'GFS', error: null }); await p2; await p1;
  assert.equal(t.u.applied.length, 1); assert.equal(t.u.applied[0][0].station, '51201');
  // a POST still on its way when the visitor moves on
  const t2 = setup();
  const { p: q1 } = await landLoad(t2); t2.f.last().text(OM_TEXT); await settle();
  const post = t2.f.last();
  const q2 = t2.L.load({ station: '46001' }); await settle();
  assert.equal(post.aborted, true, 'the POST is aborted with its load');
  t2.f.last().json({ station: '46001', table_html: '<table></table>', model: 'GFS', error: null }); await q2; await q1;
  assert.deepEqual(t2.u.applied.map((a) => a[0].station), ['46001']);
});

test('a gateway error or a dropped connection on the POST is asked once more, quietly, with the same text (G60 B-2)', async () => {
  for (const how of [502, 503, 504, 520, 524, 'net']) {
    const t = setup();
    const { p } = await landLoad(t); t.f.last().text(OM_TEXT); await settle();
    const first = t.f.last();
    if (how === 'net') first.fail(); else first.text('<html>bad gateway</html>', how);
    await settle();
    assert.deepEqual(t.f.kinds(), ['get', 'om', 'post', 'post'], 'one more POST, no second Open-Meteo request: ' + how);
    assert.equal(t.f.last().o.body, OM_TEXT); assert.equal(t.f.last().url, first.url);
    t.f.last().json(weatherAnswer()); const d = await p;
    assert.equal(d.table_html, weatherAnswer().table_html);
    assert.equal(t.u.errors.filter((e) => e[0]).length, 0, 'nothing shown on the way');
  }
  // only once: a second failure shows the error; a plain 500 is not asked again
  for (const [a, b, posts] of [[502, 502, 2], ['net', 'net', 2], [500, null, 1]]) {
    const t = setup();
    const { p } = await landLoad(t); t.f.last().text(OM_TEXT); await settle();
    if (a === 'net') t.f.last().fail(); else t.f.last().text('x', a);
    await settle();
    if (b !== null) { if (b === 'net') t.f.last().fail(); else t.f.last().text('x', b); }
    const d = await p;
    assert.equal(t.f.kinds().filter((k) => k === 'post').length, posts);
    assert.equal(d.error, t.I.OM_DOWN);
  }
});

test('a refused POST (the server would not build it) is shown with Retry and the held answer is dropped', async () => {
  const t = setup();
  const { p: p } = await landLoad(t); t.f.last().text(OM_TEXT); await settle();
  t.f.last().json({ station: POINT, error: 'Weather temporarily unavailable', table_html: null }, 200);
  const d = await p;
  assert.equal(d.error, 'Weather temporarily unavailable'); assert.equal(t.L.cache.size, 0);
  const { p: p2 } = await landLoad(t);
  assert.equal(t.f.kinds().slice(-1)[0], 'om', 'asked again: the refused answer was not kept');
  t.f.last().fail(); await p2;
});

test('the POST answering busy waits quietly and asks again (the Open-Meteo answer is reused)', async () => {
  const F = load(fakeWindow()), I = F._internals, f = stub(), u = ui(), waits = [];
  const st = { station: POINT, tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  const L = I.createLoader({ fetch: f.fetch, now: () => 1, replaceState: () => {}, swanStations: [], ui: u.ui, landWeather: true,
    setTimer: () => null, clearTimer: () => {}, wait: (ms) => new Promise((res) => waits.push({ ms, res })) }, st);
  const p = L.load({}); await settle();
  f.last().json(getAnswer()); await settle(); f.last().text(OM_TEXT); await settle();
  f.last().json({ station: POINT, busy: true, error: 'Weather is busy right now; try again in a moment', table_html: null }); await settle();
  assert.equal(waits.length, 1); waits[0].res(); await settle();
  f.last().json(getAnswer()); await settle();
  assert.deepEqual(f.calls.map((c) => c.url.split('?')[0]).slice(-2), ['/api/forecast', '/api/forecast/weather']);
  f.last().json(weatherAnswer()); await p;
  assert.equal(u.applied.length, 1); assert.ok(u.applied[0][0].table_html);
});

test('the point tool\'s prefetch builds the weather the same way', async () => {
  const t = setup();
  const q = t.L.prefetch({ station: POINT }); await settle();
  t.f.last().json(getAnswer()); await settle();
  assert.equal(t.f.last().o.credentials, 'omit'); t.f.last().text(OM_TEXT); await settle();
  assert.equal(t.f.last().o.method, 'POST'); t.f.last().json(weatherAnswer());
  const d = await q;
  assert.ok(d.table_html); assert.equal(t.L.cache.size, 1);
  assert.equal(t.u.applied.length, 0, 'a prefetch shows nothing');
});

test('the held answers are capped at OM_HELD_MAX points', async () => {
  const t = setup();
  for (let i = 0; i <= t.I.OM_HELD_MAX; i++) {
    const { p: p } = await landLoad(t, { station: `pt_2${i}000N_158000W` }, { om_url: OM_URL + '&p=' + i });
    t.f.last().text(OM_TEXT); await settle(); t.f.last().json(weatherAnswer()); await p;
  }
  // the first point's held answer went (the oldest), the last one's stayed
  t.L.cache.clear();
  const { p: p0 } = await landLoad(t, { station: 'pt_20000N_158000W', unit: 'Metric' }, { om_url: OM_URL + '&p=0' });
  assert.equal(t.f.kinds().slice(-1)[0], 'om'); t.f.last().fail(); await p0;
  const { p: pn } = await landLoad(t, { station: `pt_2${t.I.OM_HELD_MAX}000N_158000W`, unit: 'US' }, { om_url: OM_URL + '&p=' + t.I.OM_HELD_MAX });
  assert.equal(t.f.kinds().slice(-1)[0], 'post'); t.f.last().json(weatherAnswer()); await pn;
});

test('needsOm: only an answer that hands over om_url with no table and no error', () => {
  const I = load(fakeWindow())._internals;
  assert.equal(I.needsOm(getAnswer()), true);
  assert.equal(I.needsOm(getAnswer({ table_html: '<table></table>' })), false);
  assert.equal(I.needsOm(getAnswer({ error: 'Weather needs JavaScript in this browser' })), false);
  assert.equal(I.needsOm({ station: '51201', table_html: null }), false);
  assert.equal(I.needsOm(null), false);
});
