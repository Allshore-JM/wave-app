'use strict';
// static_ui/livelist.js (plan section 36): the remembered list drawn first, the deadline and retries, hidden tabs,
// partial answers (union with the remembered markers of the missing sources, never stored), the complete answer stored.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'livelist.js'), 'utf8');
const w = {}; new Function('window', SRC)(w);
const L = w.AllshoreLiveList;
const I = L._internals;

const NOW = Date.UTC(2026, 9, 4, 8, 0, 0);

function station(id, source, extra) {
  return Object.assign({ id: id, name: 'Buoy ' + id, lat: 21.5, lon: -158.1, source: source, source_name: source + ' net',
    source_url: 'https://' + source.toLowerCase(), license_label: 'Open', attribution_text: 'Source: ' + source,
    capabilities: { bulk: true, recent_history: true, directional: false, spectra: source === 'AODN', partitions: false },
    is_stale: false, dup_of: null, tz: 'Pacific/Honolulu' }, extra || {});
}
const FULL = [station('ndbc:51201', 'NDBC'), station('cdip:106', 'CDIP', { lat: 32.9, lon: -117.3 }),
  station('aodn:SYD', 'AODN', { lat: -33.8, lon: 151.4, tz: 'Australia/Sydney', also_sources: ['AusWaves'] }),
  station('cmems:6200001', 'CMEMS', { lat: 45.2, lon: -5.0, tz: 'Europe/Paris', is_stale: true })];

function memStorage(init) {
  const m = new Map(Object.entries(init || {}));
  return { getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => { m.set(k, String(v)); }, removeItem: (k) => m.delete(k), _m: m };
}

function fakeTimers() {
  let t = 0, seq = 0;
  const q = new Map();
  return {
    set(fn, ms) { const id = ++seq; q.set(id, { at: t + ms, fn }); return id; },
    clear(id) { q.delete(id); },
    pending() { return [...q.values()].map((x) => x.at - t).sort((a, b) => a - b); },
    async advance(ms) {
      const end = t + ms;
      for (;;) {
        let next = null, nid = null;
        for (const [id, x] of q) if (x.at <= end && (next === null || x.at < next.at)) { next = x; nid = id; }
        if (!next) break;
        q.delete(nid); t = next.at; next.fn(); await flush();
      }
      t = end; await flush();
    },
  };
}
async function flush() { for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r)); }

function resp(body, opts) {
  opts = opts || {};
  const headers = new Map(Object.entries(opts.headers || {}).map(([k, v]) => [k.toLowerCase(), v]));
  return { ok: opts.status === undefined || (opts.status >= 200 && opts.status < 300), status: opts.status || 200,
    headers: { get: (k) => (headers.has(k.toLowerCase()) ? headers.get(k.toLowerCase()) : null) },
    json: () => (opts.badJson ? Promise.reject(new SyntaxError('x')) : Promise.resolve(JSON.parse(JSON.stringify(body)))) };
}

// A scripted fetch: each call takes the next answer; 'hang' never settles, an Error rejects.
function scriptedFetch(answers) {
  const calls = [];
  const fn = (url, init) => {
    calls.push({ url, init });
    const a = answers.length ? answers.shift() : 'hang';
    if (a === 'hang') return new Promise((res, rej) => { if (init && init.signal) init.signal.addEventListener('abort', () => rej(new Error('aborted'))); });
    if (a instanceof Error) return Promise.reject(a);
    return Promise.resolve(a);
  };
  fn.calls = calls;
  return fn;
}

function visibility(visible) {
  const v = { visible: visible !== false, fns: [] };
  return { isVisible: () => v.visible, onChange: (f) => v.fns.push(f), _set(x) { v.visible = x; v.fns.forEach((f) => f()); } };
}

function run(o) {
  const timers = o.timers || fakeTimers();
  const out = { stations: [], status: [] };
  const ll = L.create(Object.assign({ url: '/api/buoys/live-stations', storage: o.storage || memStorage(), now: () => NOW,
    timers, visibility: o.visibility || visibility(true), fetch: o.fetch || scriptedFetch([]), early: o.early || null }, o.extra || {}));
  ll.start({ onStations: (list, info) => out.stations.push({ ids: list.map((s) => s.id), list, info }),
    onStatus: (code, text) => out.status.push([code, text]) });
  return Object.assign(out, { ll, timers });
}

// ------------------------------------------------------------------ pack / unpack

test('pack and unpack keep what the page reads, compactly', () => {
  const p = I.pack(FULL, NOW);
  assert.equal(p.v, 1); assert.equal(p.t, NOW); assert.equal(p.s.length, 4);
  assert.deepEqual(Object.keys(p.m).sort(), ['AODN', 'CDIP', 'CMEMS', 'NDBC']);
  const back = I.unpack(JSON.parse(JSON.stringify(p)), NOW + 1000);
  assert.equal(back.length, 4);
  for (let i = 0; i < FULL.length; i++) {
    const a = FULL[i], b = back[i];
    for (const k of ['id', 'name', 'lat', 'lon', 'source', 'source_name', 'source_url', 'license_label', 'attribution_text', 'tz', 'is_stale'])
      assert.deepEqual(b[k], a[k], k);
    assert.deepEqual(b.capabilities, a.capabilities);
    assert.deepEqual(b.also_sources, a.also_sources);
  }
  assert.ok(JSON.stringify(p).length < JSON.stringify(FULL).length * 0.75, 'smaller than the API form');
});

test('unpack refuses old, future, foreign and broken lists; drops broken rows', () => {
  const p = I.pack(FULL, NOW);
  assert.equal(I.unpack(p, NOW + I.MAX_AGE_MS + 1), null);
  assert.ok(I.unpack(p, NOW + I.MAX_AGE_MS));
  assert.equal(I.unpack(p, NOW - I.FUTURE_SKEW_MS - 1), null);
  assert.equal(I.unpack(Object.assign({}, p, { v: 2 }), NOW), null);
  assert.equal(I.unpack({ v: 1, t: NOW, s: 'x' }, NOW), null);
  assert.equal(I.unpack(null, NOW), null);
  const bad = Object.assign({}, p, { s: [['', 'n', 1, 2, 'X', '', 0, 0, []], ['a', 'n', 'x', 2, 'X', '', 0, 0, []],
    ['b', 'n', 95, 2, 'X', '', 0, 0, []], ['c', 'n', 1, 2], 7, ['ok', 3, 1, 2, 5, 9, 'x', 0, 'no']] });
  const out = I.unpack(bad, NOW);
  assert.equal(out.length, 1);
  assert.deepEqual([out[0].id, out[0].name, out[0].source, out[0].tz, out[0].also_sources], ['ok', '', '', undefined, undefined]);
  assert.deepEqual(out[0].capabilities, I.capsOf(0));
  assert.equal(I.unpack(Object.assign({}, p, { s: [] }), NOW), null, 'an empty list is nothing to draw');
});

test('capsMask round-trips every capability', () => {
  for (let m = 0; m < 32; m++) assert.equal(I.capsMask(I.capsOf(m)), m);
  assert.equal(I.capsMask(null), 0);
});

test('storage errors never escape', () => {
  const throwing = { getItem() { throw new Error('denied'); }, setItem() { throw new Error('quota'); } };
  assert.equal(I.readStored(throwing, NOW), null);
  assert.equal(I.writeStored(throwing, FULL, NOW), false);
  assert.equal(I.readStored(memStorage({ [I.KEY]: '{not json' }), NOW), null);
  assert.equal(I.readStored(null, NOW), null);
});

test('unionPartial adds remembered markers only for the missing sources, never twice', () => {
  const server = [FULL[0], FULL[1]];
  const cached = FULL.concat([station('cdip:999', 'CDIP')]);
  const u = I.unionPartial(server, cached, ['AODN', 'CMEMS']);
  assert.deepEqual(u.map((s) => s.id), ['ndbc:51201', 'cdip:106', 'aodn:SYD', 'cmems:6200001']);
  assert.deepEqual(I.unionPartial(server, null, ['AODN']).map((s) => s.id), ['ndbc:51201', 'cdip:106']);
  assert.deepEqual(I.unionPartial(server, cached, []).map((s) => s.id), ['ndbc:51201', 'cdip:106']);
  assert.deepEqual(I.unionPartial([FULL[2]], cached, ['AODN']).map((s) => s.id), ['aodn:SYD']);
  assert.deepEqual(I.parseMissing(' AODN, CMEMS ,,'), ['AODN', 'CMEMS']);
  assert.deepEqual(I.parseMissing(null), []);
});

// ------------------------------------------------------------------ the flow

test('a remembered list is drawn at once, then the complete answer replaces it and is stored', async () => {
  const storage = memStorage({ [I.KEY]: JSON.stringify(I.pack(FULL.slice(0, 3), NOW - 3600e3)) });
  const f = scriptedFetch([resp(FULL)]);
  const r = run({ storage, fetch: f });
  assert.equal(r.stations.length, 1, 'drawn synchronously in start()');
  assert.deepEqual(r.stations[0].ids, FULL.slice(0, 3).map((s) => s.id));
  assert.equal(r.stations[0].info.cached, true);
  assert.deepEqual(r.status, [], 'no note yet: the fresh list usually lands within NOTE_DELAY_MS');
  await flush();
  assert.equal(r.stations.length, 2);
  assert.deepEqual(r.stations[1].ids, FULL.map((s) => s.id));
  assert.equal(r.stations[1].info.same, false);
  assert.deepEqual(r.status, [['', '']], 'the "cached list" note never showed');
  const stored = JSON.parse(storage.getItem(I.KEY));
  assert.equal(stored.t, NOW); assert.equal(stored.s.length, 4);
  assert.equal(f.calls.length, 1);
  assert.ok(f.calls[0].init && f.calls[0].init.signal, 'own requests carry an abort signal');
  assert.equal(r.ll.state().done, true);
  assert.deepEqual(r.timers.pending(), [], 'no timer left behind');
});

test('an unchanged list is reported as the same (the page skips the rebuild)', async () => {
  const storage = memStorage({ [I.KEY]: JSON.stringify(I.pack(FULL, NOW - 60e3)) });
  const r = run({ storage, fetch: scriptedFetch([resp(FULL)]) });
  await flush();
  assert.equal(r.stations.length, 2);
  assert.equal(r.stations[1].info.same, true);
  const changed = FULL.map((s) => Object.assign({}, s));
  changed[0].name = 'Renamed';
  const r2 = run({ storage: memStorage({ [I.KEY]: JSON.stringify(I.pack(FULL, NOW - 60e3)) }), fetch: scriptedFetch([resp(changed)]) });
  await flush();
  assert.equal(r2.stations[1].info.same, false, 'same ids, other content: rebuilt');
});

test('no remembered list: the answer, and no note when it lands quickly', async () => {
  const r = run({ fetch: scriptedFetch([resp(FULL)]) });
  assert.equal(r.stations.length, 0);
  await flush();
  assert.equal(r.stations.length, 1);
  assert.equal(r.stations[0].info.cached, false);
  assert.deepEqual(r.status, [['', '']]);
});

test('"loading" and "cached list" show only when the list is later than NOTE_DELAY_MS (no flash: G25 B-1)', async () => {
  assert.equal(I.NOTE_DELAY_MS, 300);
  const r = run({ fetch: scriptedFetch(['hang']) });
  await r.timers.advance(I.NOTE_DELAY_MS - 1);
  assert.deepEqual(r.status, []);
  await r.timers.advance(1);
  assert.deepEqual(r.status, [['loading', 'loading\u2026']]);
  const storage = memStorage({ [I.KEY]: JSON.stringify(I.pack(FULL, NOW - 60e3)) });
  const r2 = run({ storage, fetch: scriptedFetch(['hang']) });
  assert.equal(r2.stations.length, 1);
  await r2.timers.advance(I.NOTE_DELAY_MS);
  assert.deepEqual(r2.status, [['cached', 'cached list']]);
  const r3 = run({ fetch: scriptedFetch(['hang']) });
  r3.ll.stop();
  await r3.timers.advance(5000);
  assert.deepEqual(r3.status, [], 'stop() clears the pending note');
  assert.deepEqual(r3.timers.pending(), []);
});

test('a remembered list older than 48 h is not drawn', async () => {
  const storage = memStorage({ [I.KEY]: JSON.stringify(I.pack(FULL, NOW - I.MAX_AGE_MS - 1)) });
  const r = run({ storage, fetch: scriptedFetch(['hang']) });
  assert.equal(r.stations.length, 0);
  await r.timers.advance(I.NOTE_DELAY_MS);
  assert.deepEqual(r.status[0][0], 'loading');
  assert.equal(I.MAX_AGE_MS, 48 * 3600 * 1000, 'the owner\'s 48 hours');
});

test('the <head> request is used once; the deadline then retries with fresh requests', async () => {
  let resolveEarly;
  const early = new Promise((res) => { resolveEarly = res; });
  const f = scriptedFetch(['hang', resp(FULL)]);
  const r = run({ early, fetch: f });
  await flush();
  assert.equal(f.calls.length, 0, 'the head request first');
  await r.timers.advance(I.DEADLINE_MS);
  assert.deepEqual(r.status.at(-1)[0], 'unavailable');
  await r.timers.advance(I.RETRY_MS[0]);
  assert.equal(f.calls.length, 1);
  resolveEarly(resp([FULL[0]]));                                  // the late head answer is ignored
  await flush();
  assert.equal(r.stations.length, 0);
  await r.timers.advance(I.DEADLINE_MS);                           // the own request hung too: aborted at the deadline
  assert.equal(r.ll.state().failures, 2);
  await r.timers.advance(I.RETRY_MS[1]);
  assert.equal(f.calls.length, 2);
  await flush();
  assert.deepEqual(r.stations.at(-1).ids, FULL.map((s) => s.id));
  assert.equal(r.ll.state().done, true);
});

test('failures back off 2, 4, 8, 16, then every 30 s; HTTP errors and bad bodies count as failures', async () => {
  const f = scriptedFetch([new Error('net'), resp([], { status: 503 }), resp({ not: 'a list' }), resp(null, { badJson: true }),
    new Error('net'), new Error('net'), resp(FULL)]);
  const r = run({ fetch: f });
  const gaps = [];
  await flush();
  for (let i = 0; i < 6; i++) {
    const p = r.timers.pending();
    gaps.push(p[0]);
    await r.timers.advance(p[0]);
  }
  assert.deepEqual(gaps, [2000, 4000, 8000, 16000, 30000, 30000]);
  assert.equal(f.calls.length, 7);
  assert.equal(r.ll.state().done, true);
  assert.deepEqual(r.status.map((x) => x[0]), ['unavailable', ''], 'the failure came before the loading note');
});

test('with a remembered list a failure says "retrying" and keeps the markers', async () => {
  const storage = memStorage({ [I.KEY]: JSON.stringify(I.pack(FULL, NOW - 60e3)) });
  const r = run({ storage, fetch: scriptedFetch([new Error('down')]) });
  await flush();
  assert.equal(r.stations.length, 1);
  assert.deepEqual(r.status.map((x) => x[0]), ['retrying'], 'shown at once; the pending "cached list" note dropped');
  assert.equal(JSON.parse(storage.getItem(I.KEY)).t, NOW - 60e3, 'a failure never touches the stored list');
});

test('a hidden tab waits: the due retry runs when the tab is shown again', async () => {
  const vis = visibility(true);
  const f = scriptedFetch([new Error('down'), resp(FULL)]);
  const r = run({ fetch: f, visibility: vis });
  await flush();
  vis._set(false);
  await r.timers.advance(60000);
  assert.equal(f.calls.length, 1, 'nothing asked while hidden');
  assert.equal(r.ll.state().waitingVisible, true);
  vis._set(true);
  await flush();
  assert.equal(f.calls.length, 2);
  assert.equal(r.ll.state().done, true);
  vis._set(false); vis._set(true);
  await flush();
  assert.equal(f.calls.length, 2, 'nothing after the list is complete');
});

test('partial answers: union with the remembered markers of the missing sources, never stored, polled again', async () => {
  const storage = memStorage({ [I.KEY]: JSON.stringify(I.pack(FULL, NOW - 600e3)) });
  const P = { headers: { 'X-Live-Stations-Partial': 'AODN,CMEMS' } };
  const f = scriptedFetch([resp([FULL[0], FULL[1]], P), resp([FULL[0], FULL[1], FULL[2]], { headers: { 'X-Live-Stations-Partial': 'CMEMS' } }), resp(FULL)]);
  const r = run({ storage, fetch: f });
  await flush();
  assert.deepEqual(r.stations[1].ids, FULL.map((s) => s.id), 'the missing sources come from the remembered list');
  assert.equal(r.stations[1].info.partial, true);
  assert.deepEqual(r.stations[1].info.missing, ['AODN', 'CMEMS']);
  assert.deepEqual(r.status.at(-1), ['partial', 'loading more…']);
  assert.equal(JSON.parse(storage.getItem(I.KEY)).t, NOW - 600e3, 'a partial answer is never stored');
  assert.deepEqual(r.timers.pending(), [I.PARTIAL_MS[0]]);
  await r.timers.advance(I.PARTIAL_MS[0]);
  assert.equal(f.calls.length, 2);
  await r.timers.advance(I.PARTIAL_MS[1]);
  assert.equal(f.calls.length, 3);
  assert.equal(JSON.parse(storage.getItem(I.KEY)).t, NOW, 'the complete answer is stored');
  assert.deepEqual(r.status.at(-1)[0], '');
});

test('partial answers without a remembered list draw what the server has', async () => {
  const f = scriptedFetch([resp([FULL[0]], { headers: { 'X-Live-Stations-Partial': 'CDIP' } })]);
  const r = run({ fetch: f });
  await flush();
  assert.deepEqual(r.stations[0].ids, ['ndbc:51201']);
});

test('the partial poll schedule: fast, then one a minute for two hours; the note stays (G25 B-3)', async () => {
  const P = { headers: { 'X-Live-Stations-Partial': 'CMEMS' } };
  const total = I.PARTIAL_MAX + I.SLOW_MAX;
  const answers = [];
  for (let i = 0; i < total + 5; i++) answers.push(resp([FULL[0]], P));
  const f = scriptedFetch(answers);
  const r = run({ fetch: f });
  await flush();
  const gaps = [];
  for (let guard = 0; r.timers.pending().length && guard < total + 10; guard++) {
    const g = r.timers.pending()[0]; gaps.push(g); await r.timers.advance(g);
  }
  assert.deepEqual(gaps.slice(0, 9), [3000, 3000, 5000, 5000, 10000, 10000, 15000, 15000, 15000]);
  assert.equal(gaps[I.PARTIAL_MAX - 2], 15000);
  assert.equal(gaps[I.PARTIAL_MAX - 1], I.SLOW_MS);
  assert.equal(I.SLOW_MS, 60000);
  assert.ok(gaps.slice(I.PARTIAL_MAX - 1).every((g) => g === I.SLOW_MS));
  assert.equal(f.calls.length, total);
  assert.ok(gaps.reduce((a, b) => a + b, 0) >= 2 * 3600e3, 'about two hours in all');
  assert.equal(r.ll.state().done, true);
  assert.deepEqual(r.status.at(-1), ['partial', 'loading more\u2026'], 'still incomplete: the note stays');
});

test('"loading more" only when something is drawn; else "loading" (G25 B-2)', async () => {
  const P = { headers: { 'X-Live-Stations-Partial': 'NDBC,CDIP,AODN,CMEMS' } };
  const f = scriptedFetch([resp([], P), resp([FULL[0]], P)]);
  const r = run({ fetch: f });
  await flush();
  assert.equal(r.stations.at(-1).ids.length, 0);
  await r.timers.advance(I.NOTE_DELAY_MS);
  assert.deepEqual(r.status, [['loading', 'loading\u2026']], 'nothing on the map: still loading');
  await r.timers.advance(I.PARTIAL_MS[0] - I.NOTE_DELAY_MS);
  assert.deepEqual(r.status.at(-1), ['partial', 'loading more\u2026']);
});

test('the deadline grows after consecutive timeouts and resets on an answer (G25 B-5)', async () => {
  assert.deepEqual(I.DEADLINE_STEPS, [8000, 8000, 16000, 30000]);
  const f = scriptedFetch(['hang', 'hang', 'hang', 'hang', resp([], { status: 503 }), 'hang', resp(FULL)]);
  const r = run({ fetch: f });
  await flush();
  const deadlines = [];
  for (let i = 0; i < 4; i++) {                                   // a hung attempt: its deadline is the only timer
    if (i) await r.timers.advance(r.timers.pending()[0]);          // the retry
    const p = r.timers.pending().filter((x) => x > 300);
    deadlines.push(p[0]);
    await r.timers.advance(p[0]);
  }
  assert.deepEqual(deadlines, [8000, 8000, 16000, 30000]);
  assert.equal(r.ll.state().timeouts, 4);
  await r.timers.advance(r.timers.pending()[0]);                   // the retry: the 503 answers -> back to 8 s
  assert.equal(r.ll.state().timeouts, 0);
  await r.timers.advance(r.timers.pending()[0]);
  assert.deepEqual(r.timers.pending(), [8000]);
  const signal = f.calls.at(-1).init.signal;
  await r.timers.advance(8000);
  assert.equal(signal.aborted, true, 'the deadline aborts the hung request');
});

test('a successful (partial) answer resets the deadline too', async () => {
  const P = { headers: { 'X-Live-Stations-Partial': 'CMEMS' } };
  const f = scriptedFetch(['hang', 'hang', resp([FULL[0]], P), 'hang']);
  const r = run({ fetch: f });
  await flush();
  await r.timers.advance(8000);                                    // timeout 1
  await r.timers.advance(r.timers.pending()[0]);                   // retry
  await r.timers.advance(8000);                                    // timeout 2
  assert.equal(r.ll.state().timeouts, 2);
  await r.timers.advance(r.timers.pending()[0]);                   // retry: the partial answer
  assert.equal(r.ll.state().timeouts, 0);
  await r.timers.advance(I.PARTIAL_MS[0]);                         // the next poll hangs: 8 s again, not 16
  assert.deepEqual(r.timers.pending(), [8000]);
});

test('pins: a meta-only change is not "the same"; a second start() does nothing; empty names kept; lon bounds', async () => {
  const meta = FULL.map((s) => Object.assign({}, s));
  meta[0].attribution_text = 'Source: someone else';
  assert.notEqual(I.contentKey(meta), I.contentKey(FULL));
  const f = scriptedFetch([resp(FULL)]);
  const r = run({ fetch: f });
  r.ll.start({ onStations: () => { throw new Error('must not run'); } });
  await flush();
  assert.equal(f.calls.length, 1);
  assert.equal(r.stations.length, 1);
  const p = I.pack([Object.assign({}, FULL[0], { name: '' })], NOW);
  assert.equal(I.unpack(p, NOW)[0].name, '', 'a station without a name is kept');
  const lon = (x) => I.unpack(Object.assign({}, p, { s: [['a', 'n', 1, x, 'X', '', 0, 0, []]] }), NOW);
  assert.ok(lon(539) && lon(-539));
  assert.equal(lon(541), null);
});

test('an empty complete answer is drawn but not stored', async () => {
  const storage = memStorage({ [I.KEY]: JSON.stringify(I.pack(FULL, NOW - 60e3)) });
  const r = run({ storage, fetch: scriptedFetch([resp([])]) });
  await flush();
  assert.deepEqual(r.stations.at(-1).ids, []);
  assert.equal(JSON.parse(storage.getItem(I.KEY)).t, NOW - 60e3);
});

test('stop() ends everything: no request, no callback, the in-flight request aborted', async () => {
  const f = scriptedFetch(['hang', resp(FULL)]);
  const r = run({ fetch: f });
  await flush();
  const signal = f.calls[0].init.signal;
  r.ll.stop();
  assert.equal(signal.aborted, true);
  await r.timers.advance(120000);
  assert.equal(f.calls.length, 1);
  assert.equal(r.stations.length, 0);
});

test('a throwing fetch, a throwing handler and a second start() are harmless', async () => {
  let n = 0;
  const f = () => { n += 1; if (n === 1) throw new Error('sync'); return Promise.resolve(resp(FULL)); };
  const timers = fakeTimers();
  const ll = L.create({ fetch: f, timers, now: () => NOW, storage: memStorage() });
  ll.start({ onStations: () => { throw new Error('page bug'); }, onStatus: () => { throw new Error('page bug'); } });
  ll.start({});
  await flush();
  await timers.advance(I.RETRY_MS[0]);
  assert.equal(n, 2);
  assert.equal(ll.state().done, true);
});

test('without AbortController the deadline still moves on', async () => {
  const f = scriptedFetch(['hang', resp(FULL)]);
  const r = run({ fetch: f, extra: { AbortController: null } });
  await flush();
  assert.equal(f.calls[0].init, undefined);
  await r.timers.advance(I.DEADLINE_MS + I.RETRY_MS[0]);
  assert.equal(f.calls.length, 2);
  assert.equal(r.ll.state().done, true);
});

test('an answer resets the back-off: a failure after a partial answer waits 2 s again', async () => {
  const P = { headers: { 'X-Live-Stations-Partial': 'CMEMS' } };
  const f = scriptedFetch([new Error('a'), new Error('b'), resp([FULL[0]], P), new Error('c'), resp(FULL)]);
  const r = run({ fetch: f });
  await flush();
  await r.timers.advance(I.RETRY_MS[0]);
  await r.timers.advance(I.RETRY_MS[1]);                           // the partial answer
  assert.equal(r.ll.state().failures, 0);
  await r.timers.advance(I.PARTIAL_MS[0]);                          // fails again
  assert.deepEqual(r.timers.pending(), [I.RETRY_MS[0]]);
  await r.timers.advance(I.RETRY_MS[0]);
  assert.equal(r.ll.state().done, true);
});


test('a malformed (non-list) answer also resets the deadline', async () => {
  const f = scriptedFetch(['hang', resp({ not: 'a list' }), 'hang']);
  const r = run({ fetch: f });
  await flush();
  await r.timers.advance(8000);                                    // timeout 1
  assert.equal(r.ll.state().timeouts, 1);
  await r.timers.advance(r.timers.pending()[0]);                   // the retry: a malformed answer (the server answered)
  assert.equal(r.ll.state().timeouts, 0);
});
