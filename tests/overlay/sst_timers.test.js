'use strict';
// The SST store's stall timers (plan section 44, G31-3 / G31-4 and the re-check of their fix round) on a FAKE clock
// with the REAL decodeFrame / readBody / inflate path and a held network (sst_net.js): every path leaves no timer behind
// (but the store's own retry timer where a cooldown is set), never lands a dropped download and leaves no abort listener on
// the store's shared controller; readBody; the frames' decodeFrame path unchanged; browsers without DecompressionStream.
//   node --test --test-force-exit tests/overlay/sst_timers.test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const { loadOverlay, fakeClock, png, makeNet, countingAC, FILE } = require('./sst_net');
const GRID0 = { cols: 1440, rows: 721, lon0: -180, lat0: 90, dlon: 0.25, dlat: -0.25, registration: 'center', lon_periodic: true };
const FRAME = png(1440, 721, (x, y) => 100 + ((x + y) % 50));
const CELL2 = png(502, 502, (x, y) => 120 + ((x * 3 + y) % 40));
const CELL1 = png(402, 402, (x, y) => 110 + ((x + 2 * y) % 30));
function manifest() {
  return { format: 'sst-v1', day: '2026-10-15', run: '20261015', build: 'B', complete: true, published_utc: '2026-10-16T12:31:40Z', units: 'degC', range: [-2, 35],
    tier0: { key: 'sst/v1/20261015/B/t0.png', grid: GRID0 },
    tiers: [{ name: 't2', cell: 5, res: 0.01, px: 500, apron: 1, dir: 'sst/v1/20261015/B/t2', cells: { '20_-160': [24, 27, 1000], '20_-155': [24, 27, 1000], '15_-160': [24, 27, 1000], '15_-155': [24, 27, 1000], '25_-160': [24, 27, 1000] } },
      { name: 't1', cell: 20, res: 0.05, px: 400, apron: 1, dir: 'sst/v1/20261015/B/t1', cells: { '20_-160': [22, 28, 1000], '0_-160': [22, 28, 1000] } }] };
}
const REAL_NOW = Date.now;
test.afterEach(() => { Date.now = REAL_NOW; });
const PTR = { format: 'sst-v1', day: '2026-10-15', run: '20261015', manifest: 'sst/v1/20261015/manifest-x.json', complete: true, published_utc: '2026-10-16T12:31:40Z' };
function world() {
  const clock = fakeClock(), net = makeNet(), CAC = countingAC();
  const base = REAL_NOW(); Date.now = () => base + clock.now();      // cooldowns follow the fake clock
  const I = loadOverlay(FILE, { timers: clock, fetch: net.fetch, AbortController: CAC });
  const st = new I.SstStore('https://b');
  return { clock, net, CAC, I, st };
}
const flush = async (n) => { for (let i = 0; i < (n || 30); i++) await new Promise((r) => setImmediate(r)); };
// real time for the inflater (zlib runs on the thread pool): poll a condition up to 3 s
const until = async (f) => { const t0 = REAL_NOW(); while (!f() && REAL_NOW() - t0 < 3000) await new Promise((r) => setTimeout(r, 5)); return f(); };
// deliver a body in k chunks, advancing the clock gapMs before each
async function stream(w, r, buf, k, gapMs) {
  const size = Math.ceil(buf.length / k);
  for (let i = 0; i < k; i++) { await w.clock.advance(gapMs); w.net.push(r, buf.subarray(i * size, Math.min(buf.length, (i + 1) * size))); await flush(); }
  w.net.end(r); await flush();
}
async function loaded(w) {
  const p = w.st.load(true); await flush();
  w.net.answer(w.net.last(/latest\.json$/), { json: PTR }); await flush();
  w.net.answer(w.net.last(/manifest-x\.json$/), { json: manifest() }); await flush();
  const f = w.net.last(/t0\.png$/); w.net.answer(f, { headers: { 'content-length': FRAME.length } }); await flush();
  await stream(w, f, FRAME, 2, 0);
  const res = await p; assert.equal(res, w.st); return res;
}

test('stall timers R3: slow steps: pointer at +14 s, manifest at +14 s, frame headers at +14 s, then a chunk every 14 s for 70 s: loads; no timer left', async () => {
  const w = world();
  const p = w.st.load(true); await flush();
  await w.clock.advance(14000); w.net.answer(w.net.last(/latest\.json$/), { json: PTR }); await flush();
  await w.clock.advance(14000); w.net.answer(w.net.last(/manifest-x\.json$/), { json: manifest() }); await flush();
  const f = w.net.last(/t0\.png$/);
  await w.clock.advance(14000); w.net.answer(f, { headers: { 'content-length': FRAME.length } }); await flush();
  await stream(w, f, FRAME, 5, 14000);
  assert.equal(await p, w.st); assert.equal(w.st.status, 'ok');
  assert.equal(w.clock.pending(), 0, 'no timer left: ' + w.clock.sources().join(' | '));
});

test('stall timers R3: a silent step: the manifest answers after 15.1 s -> failed, the request aborted, no timer left; the late answer changes nothing', async () => {
  const w = world();
  const p = w.st.load(true); await flush();
  w.net.answer(w.net.last(/latest\.json$/), { json: PTR }); await flush();
  const mr = w.net.last(/manifest-x\.json$/);
  await w.clock.advance(15100); await flush();
  assert.equal(await p, null); assert.equal(w.st.status, 'failed');
  assert.equal(mr.sig.aborted, true);
  assert.equal(w.clock.pending(), 0, w.clock.sources().join(' | '));
  assert.equal(w.net.answer(mr, { json: manifest() }), false, 'the aborted request cannot be answered');
});

test('stall timers R3: the frame body stalls after 2 of 5 chunks: failed 15 s after the last chunk (not before), no timer left', async () => {
  const w = world();
  const p = w.st.load(true); await flush();
  w.net.answer(w.net.last(/latest\.json$/), { json: PTR }); await flush();
  w.net.answer(w.net.last(/manifest-x\.json$/), { json: manifest() }); await flush();
  const f = w.net.last(/t0\.png$/); w.net.answer(f, { headers: { 'content-length': FRAME.length } }); await flush();
  const size = Math.ceil(FRAME.length / 5);
  await w.clock.advance(10000); w.net.push(f, FRAME.subarray(0, size)); await flush();
  await w.clock.advance(10000); w.net.push(f, FRAME.subarray(size, 2 * size)); await flush();
  await w.clock.advance(14900); await flush();
  assert.equal(w.st.status, 'loading', 'still within 15 s of the last chunk');
  await w.clock.advance(200); await flush();
  assert.equal(await p, null); assert.equal(w.st.status, 'failed'); assert.equal(f.sig.aborted, true);
  assert.equal(w.clock.pending(), 0, w.clock.sources().join(' | '));
});

test('stall timers R3: Off (abortAll) in the middle of the frame body: idle, no timer left; a load right after is a fresh one that completes', async () => {
  const w = world();
  const p = w.st.load(true); await flush();
  w.net.answer(w.net.last(/latest\.json$/), { json: PTR }); await flush();
  w.net.answer(w.net.last(/manifest-x\.json$/), { json: manifest() }); await flush();
  const f = w.net.last(/t0\.png$/); w.net.answer(f, { headers: { 'content-length': FRAME.length } }); await flush();
  w.net.push(f, FRAME.subarray(0, 1000));
  w.st.abortAll();                                                    // same tick as a chunk: the chunk's kick may run after
  await flush();
  assert.equal(await p, null); assert.equal(w.st.status, 'idle');
  assert.equal(w.clock.pending(), 0, w.clock.sources().join(' | '));
  const p2 = w.st.load(true); await flush();
  assert.notEqual(p2, p);
  w.net.answer(w.net.last(/latest\.json$/), { json: PTR }); await flush();
  w.net.answer(w.net.last(/manifest-x\.json$/), { json: manifest() }); await flush();
  const f2 = w.net.last(/t0\.png$/); w.net.answer(f2, { headers: { 'content-length': FRAME.length } }); await flush();
  await stream(w, f2, FRAME, 3, 1000);
  assert.equal(await p2, w.st); assert.equal(w.clock.pending(), 0);
});

test('stall timers R3: supersession: Off during the body, a new load; the first download\'s late chunk (pushed before the abort) re-arms nothing that fires on the second', async () => {
  const w = world();
  const p = w.st.load(true); await flush();
  w.net.answer(w.net.last(/latest\.json$/), { json: PTR }); await flush();
  w.net.answer(w.net.last(/manifest-x\.json$/), { json: manifest() }); await flush();
  const f = w.net.last(/t0\.png$/); w.net.answer(f, { headers: {} }); await flush();
  w.st.abortAll(); const p2 = w.st.load(true);
  await flush();
  w.net.answer(w.net.last(/latest\.json$/), { json: PTR }); await flush();
  w.net.answer(w.net.last(/manifest-x\.json$/), { json: manifest() }); await flush();
  const f2 = w.net.last(/t0\.png$/); w.net.answer(f2, { headers: {} }); await flush();
  await stream(w, f2, FRAME, 4, 14000);
  assert.equal(await p, null); assert.equal(await p2, w.st); assert.equal(w.st.status, 'ok');
  assert.equal(w.clock.pending(), 0, w.clock.sources().join(' | '));
});

// ---- cells ----
const Z9 = { z: 9, x: 31, y: 224 };                                      // inside 20_-160 (5-deg) and 20_-160 (20-deg)
function cellReqs(w) { return w.net.find(/\/t[12]\/.*\.png$/).filter((r) => r.state === 'pending' || r.state === 'streaming'); }

test('stall timers R4: a cell silent for 15 s: slot freed at 15 s, a cooldown, the fetch aborted, no listener left on the shared controller; the retry after the cooldown', async () => {
  const w = world(); await loaded(w);
  w.st.retryMs = 60000;
  let changes = 0; w.st.onChange = () => { changes++; };
  w.st.tierFor(Z9); await flush();
  const reqs = cellReqs(w); assert.equal(reqs.length, 2, 'the 1-km cell and its 0.05-degree stand-in');
  const shared = w.st.abort;
  assert.equal(shared.signal.__ls.size, 2);
  await w.clock.advance(14999); await flush();
  assert.equal(Object.keys(w.st.inflight).length, 2);
  await w.clock.advance(2); await flush();
  assert.equal(Object.keys(w.st.inflight).length, 0, 'both slots freed');
  assert.ok(reqs.every((r) => r.sig.aborted), 'both downloads aborted');
  assert.equal(shared.signal.__ls.size, 0, 'no listener left');
  assert.equal(typeof w.st.failed['t2/20_-160'], 'number');
  assert.equal(changes, 2, 'one change per stalled cell');
  const pend = w.clock.sources(); assert.equal(w.clock.pending(), 1, 'only the retry timer: ' + pend.join(' | '));
  // a late answer of the dropped download (impossible for a real aborted fetch, but a decode already past the body is the same)
  assert.equal(w.net.answer(reqs[0], {}), false);
  await w.clock.advance(60100); await flush();
  assert.equal(changes, 3, 'the retry timer moved the revision');
  w.st.tierFor(Z9); await flush();
  assert.equal(cellReqs(w).length, 2, 'asked again after the cooldown');
});

test('stall timers R4: a cell that keeps sending (a chunk every 14 s for 70 s) is never dropped and lands; no timer, no listener left', async () => {
  const w = world(); await loaded(w);
  w.st.tierFor(Z9); await flush();
  const [r2, r1] = cellReqs(w);
  w.net.answer(r2, { headers: {} }); w.net.answer(r1, { headers: {} }); await flush();
  const shared = w.st.abort;
  // interleave both bodies
  const k = 5, s2 = Math.ceil(CELL2.length / k), s1 = Math.ceil(CELL1.length / k);
  for (let i = 0; i < k; i++) {
    await w.clock.advance(14000);
    w.net.push(r2, CELL2.subarray(i * s2, Math.min(CELL2.length, (i + 1) * s2))); w.net.push(r1, CELL1.subarray(i * s1, Math.min(CELL1.length, (i + 1) * s1))); await flush();
  }
  w.net.end(r2); w.net.end(r1); await flush(60);
  await until(() => w.st.tiers[1].loaded.has('20_-160') && w.st.tiers[0].loaded.has('20_-160'));
  assert.equal(w.st.tiers[1].loaded.has('20_-160'), true); assert.equal(w.st.tiers[0].loaded.has('20_-160'), true);
  assert.equal(Object.keys(w.st.inflight).length, 0);
  assert.equal(shared.signal.__ls.size, 0);
  assert.equal(w.clock.pending(), 0, w.clock.sources().join(' | '));
});

test('stall timers R4: Off / a field switch / a new day while cells stall: no timer, no failed entry, no change callback, no listener', async () => {
  for (const how of ['abortAll', 'idle', 'release', 'newday']) {
    const w = world(); await loaded(w);
    let changes = 0; w.st.onChange = () => { changes++; };
    w.st.tierFor(Z9); w.st.tierFor({ z: 9, x: 34, y: 224 }); w.st.tierFor({ z: 9, x: 31, y: 228 }); await flush();
    const shared = w.st.abort, n = Object.keys(w.st.inflight).length;
    assert.ok(n >= 3 && n <= 4, 'cells in flight: ' + n);
    await w.clock.advance(5000);
    if (how === 'newday') w.st._dropDay(); else w.st[how]();
    await flush();
    assert.equal(shared.signal.__ls.size, 0, how + ': listeners');
    await w.clock.advance(30000); await flush();
    assert.equal(changes, 0, how + ': no change callback after');
    assert.deepEqual(Object.keys(w.st.failed), [], how + ': no failure noted');
    assert.equal(w.clock.pending(), 0, how + ': ' + w.clock.sources().join(' | '));
  }
});

test('stall timers R4: a late ERROR of a stalled download (the body errors after the stall) is not a second failure: one cooldown, one change', async () => {
  const w = world(); await loaded(w);
  let changes = 0; w.st.onChange = () => { changes++; };
  w.st.retryMs = 60000;
  w.st.tierFor(Z9); await flush();
  const [r2] = cellReqs(w);
  w.net.answer(r2, { headers: {} }); await flush();
  w.net.push(r2, CELL2.subarray(0, 100)); await flush();
  await w.clock.advance(15001); await flush();
  const f0 = w.st.failed['t2/20_-160']; assert.equal(typeof f0, 'number');
  const c0 = changes, rev0 = w.st.rev;
  w.net.fail(r2, new TypeError('network error')); await flush();
  assert.equal(w.st.failed['t2/20_-160'], f0); assert.equal(changes, c0); assert.equal(w.st.rev, rev0);
});

test('stall timers R4: a decode that outlives the stall (body complete, the decode slower than 15 s) is dropped and never lands', async () => {
  const w = world(); await loaded(w);
  w.st.retryMs = 60000;
  const d0 = w.st._decode.bind(w.st);
  // the body arrives at once; the "decode" then waits 16 s on the fake clock before handing the frame back
  w.st._decode = (url, sig, expect, prog) => d0(url, sig, expect, prog).then((fr) => new Promise((res) => w.clock.setTimeout(() => res(fr), 16000)));
  w.st.tierFor(Z9); await flush();
  const [r2, r1] = cellReqs(w);
  for (const [r, b] of [[r2, CELL2], [r1, CELL1]]) { w.net.answer(r, { headers: {} }); w.net.push(r, b); w.net.end(r); }
  await flush(60);
  await w.clock.advance(17000); await flush(60);
  assert.equal(w.st.tiers[1].loaded.has('20_-160'), false, 'dropped although every byte had arrived');
  assert.equal(typeof w.st.failed['t2/20_-160'], 'number');
});

test('stall timers R4: re-entrancy: the stall\'s change callback asks for more cells (a tile redraw): the cap holds, nothing throws, each started download has a timer', async () => {
  const w = world(); await loaded(w);
  w.st.retryMs = 60000;
  const more = [{ z: 9, x: 34, y: 224 }, { z: 9, x: 31, y: 228 }, { z: 9, x: 34, y: 228 }, { z: 9, x: 31, y: 221 }];
  let calls = 0; w.st.onChange = () => { calls++; for (const c of more) w.st.tierFor(c); };
  w.st.onNeeded = () => { const n = {}; for (const c of [Z9].concat(more)) w.st.keysFor(c, n); return n; };
  w.st.tierFor(Z9); await flush();
  let maxIn = 0; const iv = w.clock.setInterval(() => { maxIn = Math.max(maxIn, Object.keys(w.st.inflight).length); }, 100);
  await w.clock.advance(15001); await flush();
  maxIn = Math.max(maxIn, Object.keys(w.st.inflight).length);
  w.clock.clearInterval(iv);
  assert.ok(maxIn <= 4, 'cap: ' + maxIn);
  const inflight = Object.keys(w.st.inflight).length;
  const timers = w.clock.sources().filter((s) => /cellStallMs|mine\(\)/.test(s)).length;
  assert.equal(timers, inflight, 'one stall timer per download in flight (' + inflight + ')');
});

test('stall timers R4: listener bookkeeping over 120 downloads through one shared controller (landings, 404s, stalls): none left', async () => {
  const w = world(); await loaded(w);
  w.st.retryMs = 1;
  const T2 = w.st.tiers[1], names = Object.keys(T2.cells);
  let shared = null;
  for (let round = 0; round < 40; round++) {
    for (const nm of names) { T2.loaded.delete(nm); delete w.st.failed['t2/' + nm]; }
    w.st.bytes = 0;
    w.st.request(T2, names); await flush();
    shared = shared || w.st.abort; assert.equal(w.st.abort, shared, 'one shared controller');
    const live = cellReqs(w);
    live.forEach((r, i) => {
      if (round % 3 === 0 && i === 0) return;                           // left silent: the stall timer ends it
      if (i === 1) { w.net.answer(r, { status: 404, headers: {} }); return; }
      w.net.answer(r, { headers: {} }); w.net.push(r, CELL2); w.net.end(r);
    });
    await flush(60);
    await w.clock.advance(15001); await flush(60);
  }
  assert.equal(shared.signal.__ls.size, 0, 'listeners left on the shared controller: ' + shared.signal.__ls.size);
});

// ---- readBody and decodeFrame's paths ----
const R = loadOverlay(FILE);
function reader(chunks, opts) {
  opts = opts || {}; let i = 0; const log = { reads: 0, cancels: 0 };
  return { log, read() { log.reads++; if (opts.errorAt === i) return Promise.reject(Object.assign(new Error(opts.err || 'network error'), { name: opts.errName || 'TypeError' }));
    if (i >= chunks.length) return Promise.resolve({ done: true, value: undefined }); return Promise.resolve({ done: false, value: chunks[i++] }); },
    cancel() { log.cancels++; return opts.cancelRejects ? Promise.reject(new Error('cancel failed')) : Promise.resolve(); } };
}
test('readBody: the chunks in order as one ArrayBuffer; onProgress once per chunk (not for the end)', async () => {
  const a = new Uint8Array([1, 2, 3]), b = new Uint8Array([]), c = new Uint8Array([4, 5]);
  let n = 0; const buf = await R.readBody(reader([a, b, c]), () => { n++; });
  assert.ok(buf instanceof ArrayBuffer); assert.deepEqual([...new Uint8Array(buf)], [1, 2, 3, 4, 5]); assert.equal(n, 3);
});
test('readBody: over the 2-MB cap -> "frame decode failed", the reader cancelled once, no more reads; exactly 2 MB passes', async () => {
  const big = new Uint8Array(1024 * 1024), r = reader([big, big, new Uint8Array(1), big]);
  await assert.rejects(R.readBody(r, () => {}), /frame decode failed/);
  assert.equal(r.log.cancels, 1); assert.equal(r.log.reads, 3);
  assert.equal((await R.readBody(reader([big, big]), () => {})).byteLength, 2 * 1024 * 1024);
});
test('readBody: a reader error mid-body propagates as is (AbortError stays one; a network error stays transient)', async () => {
  await assert.rejects(R.readBody(reader([new Uint8Array(5)], { errorAt: 1, errName: 'AbortError', err: 'aborted' }), () => {}), (e) => e.name === 'AbortError');
  await assert.rejects(R.readBody(reader([new Uint8Array(5)], { errorAt: 1 }), () => {}), (e) => e.name === 'TypeError' && R.failureKind(e) === 'transient');
});
test('readBody: a cancel() that rejects (an errored stream) leaves no unhandled rejection (re-check RC-6)', async () => {
  const seen = []; const h = (e) => seen.push(String(e && e.message)); process.on('unhandledRejection', h);
  const big = new Uint8Array(1024 * 1024 + 600 * 1024);
  await assert.rejects(R.readBody(reader([big, big], { cancelRejects: true }), () => {}), /frame decode failed/);
  await new Promise((r) => setTimeout(r, 50));
  process.off('unhandledRejection', h);
  assert.deepEqual(seen, []);
});
function fakeFetch(buf, log) {
  return async () => {
    log.push('fetch');
    const resp = new Response(buf, { headers: { 'content-length': String(buf.length) } });
    return { ok: true, status: 200, headers: resp.headers, get body() { log.push('body'); return resp.body; }, arrayBuffer: () => { log.push('arrayBuffer'); return resp.arrayBuffer(); }, blob: () => { log.push('blob'); return resp.blob(); } };
  };
}
test('decodeFrame: a download with a progress callback (every model frame since 2.22.4, the SST store) reads the stream; a call without one (the retry after a dead worker) reads arrayBuffer(); the same codes', async () => {
  const P = png(64, 32, (x, y) => (x + y) % 200 + 1);
  const run = async (withProgress) => {
    const log = []; const J = loadOverlay(FILE, { fetch: fakeFetch(P, log) });
    let prog = 0;
    const fr = await J.decodeFrame('https://x/f.png', null, { cols: 64, rows: 32 }, withProgress ? null : () => log.push('onBody'), false, withProgress ? () => { prog++; } : undefined);
    return { log, prog, q: Buffer.from(fr.q).toString('base64') };
  };
  const model = await run(false), sst = await run(true);
  assert.deepEqual(model.log, ['fetch', 'arrayBuffer', 'onBody'], 'without a progress callback: arrayBuffer()');
  assert.deepEqual(sst.log.slice(0, 2), ['fetch', 'body']); assert.ok(sst.prog >= 2); assert.equal(sst.q, model.q);
});
test('decodeFrame without DecompressionStream: a streamed frame body reaches the canvas decode as a Blob holding exactly its bytes, and decodes to the same codes (fix check F-3)', async () => {
  const P = png(64, 32, (x, y) => (x * 3 + y) % 200 + 1);
  const ref = await loadOverlay(FILE, { fetch: fakeFetch(P, []) }).decodeFrame('https://x/f.png', null, { cols: 64, rows: 32 }, null, false, () => {});
  const seen = [];
  const doc = { hidden: false, addEventListener() {}, removeEventListener() {},
    createElement() {
      let bmp = null;
      return { width: 0, height: 0, getContext() {
        return { clearRect() {}, drawImage(b) { bmp = b; },
          getImageData(x, y, w, h) { const d = new Uint8ClampedArray(w * h * 4); for (let i = 0; i < w * h; i++) d[i * 4] = bmp.q[i]; return { data: d }; } };
      } };
    } };
  const had = globalThis.createImageBitmap;
  globalThis.createImageBitmap = async (blob) => {
    seen.push(blob instanceof Blob);
    const bytes = Buffer.from(await blob.arrayBuffer());
    assert.ok(bytes.equals(Buffer.from(P)), 'the Blob holds the body as it arrived');
    return { width: ref.cols, height: ref.rows, q: ref.q, close() {} };
  };
  try {
    const log = [], J = loadOverlay(FILE, { fetch: fakeFetch(P, log), DecompressionStream: undefined, document: doc });
    let prog = 0;
    const fr = await J.decodeFrame('https://x/f.png', null, { cols: 64, rows: 32 }, () => log.push('onBody'), false, () => { prog++; });
    assert.deepEqual(seen, [true], 'createImageBitmap got a Blob');
    assert.ok(log.includes('body') && !log.includes('blob') && !log.includes('arrayBuffer'), JSON.stringify(log));
    assert.ok(prog >= 2);
    assert.equal(Buffer.from(fr.q).toString('base64'), Buffer.from(ref.q).toString('base64'));
  } finally { if (had === undefined) delete globalThis.createImageBitmap; else globalThis.createImageBitmap = had; }
});
test('decodeFrame without DecompressionStream (older Safari / Firefox): the SST path still reports progress chunk by chunk (re-check RC-3)', async () => {
  const P = png(64, 32, (x, y) => (x + y) % 200 + 1), log = [];
  const J = loadOverlay(FILE, { fetch: fakeFetch(P, log), DecompressionStream: undefined });
  let prog = 0;
  await J.decodeFrame('https://x/f.png', null, { cols: 64, rows: 32 }, null, false, () => { prog++; }).catch(() => {});   // the canvas decode cannot run in Node
  assert.ok(log.includes('body') && !log.includes('blob'), 'the stream, not blob(): ' + JSON.stringify(log));
  assert.ok(prog >= 2, 'progress during the body: ' + prog);
});

// ---- a cell whose body keeps arriving, with and without DecompressionStream (re-check RC-3) ----
{
  const GRID = { cols: 1440, rows: 721, lon0: -180, lat0: 90, dlon: 0.25, dlat: -0.25, registration: 'center', lon_periodic: true };
  const M = { format: 'sst-v1', day: '2026-10-15', run: '20261015', complete: true, units: 'degC', range: [-2, 35], tier0: { key: 'sst/v1/20261015/B/t0.png', grid: GRID },
    tiers: [{ name: 't2', cell: 5, res: 0.01, px: 500, apron: 1, dir: 'sst/v1/20261015/B/t2', cells: { '20_-160': [24, 27, 1000] } }] };
  const CELL = png(502, 502, (x, y) => 120 + ((x * 3 + y) % 40));
  for (const withDS of [true, false]) {
    test('stall timers: a cell body arriving over 20 s (a chunk every 2 s) is never dropped, ' + (withDS ? 'with' : 'without') + ' DecompressionStream', async () => {
      const clock = fakeClock(), net = makeNet(), base = REAL_NOW(); Date.now = () => base + clock.now();
      const opts = { timers: clock, fetch: net.fetch, AbortController: countingAC() };
      if (!withDS) opts.DecompressionStream = undefined;
      const J = loadOverlay(FILE, opts), st = new J.SstStore('https://b');
      st.status = 'ok'; st.manifest = M; st.grid = GRID; st.frame = { q: new Uint8Array(1440 * 721), cols: 1440, rows: 721 };
      st.tiers = M.tiers.map((t) => ({ name: t.name, cell: t.cell, res: t.res, px: t.px, n: t.px + 2, dir: t.dir, cells: t.cells, loaded: new Map() }));
      st.retryMs = 60000;
      st.tierFor({ z: 9, x: 31, y: 224 }); await flush();
      const r = net.last(/\/t2\//);
      net.answer(r, { headers: {} }); await flush();
      const k = 10, sz = Math.ceil(CELL.length / k);
      for (let i = 0; i < k; i++) { await clock.advance(2000); net.push(r, CELL.subarray(i * sz, Math.min(CELL.length, (i + 1) * sz))); await flush(); }
      assert.equal(r.sig.aborted, false, 'bytes kept arriving: alive');
      assert.deepEqual(st.failed, {});
    });
  }
}
