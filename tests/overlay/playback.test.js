'use strict';
// Playback state machine tests for static_overlay/overlay.js (no browser): fetch, createImageBitmap,
// document and Leaflet are stubbed; the decode stage is released by hand so races can be staged.
// Derived from the G3 reviewer B reproduction of the run-poisoning P0.  node --test tests/overlay/
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const GRID = { cols: 1440, rows: 721, lon0: -180, lat0: 90, dlon: 0.25, dlat: -0.25, registration: 'center', lon_periodic: true };
const HALF = { cols: 720, rows: 361, lon0: -180, lat0: 90, dlon: 0.5, dlat: -0.5, registration: 'center', lon_periodic: true };
const HS = { lo: 0, hi: 15, legend: [0, 12], units: 'm', interpolation: 'bilinear' };

function manifest(run, runUtc, tag) {
  const frames = [];
  for (let s = 0; s <= 240; s += 3) frames.push({ step: s, valid_utc: new Date(Date.parse(runUtc) + s * 3.6e6).toISOString().replace('.000Z', 'Z') });
  return { schema: 3, run, run_utc: runUtc, encoding: 'u8-linear-v2', complete: true, fields: { hs: HS }, grid: GRID, grid_half: HALF, frames,
    files: { template: `gfswave/0p25/v1/${run}/{res}{field}/f{step:03d}.png`, res: { full: '', half: 'half/' } }, model: {}, tag };
}
const ptr = (m) => ({ run: m.run, manifest: `gfswave/0p25/v1/${m.run}/manifest-x.json`, complete: true, published_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z') });
const tick = () => new Promise((r) => setImmediate(r));
async function settle(n) { for (let i = 0; i < (n || 6); i++) await tick(); }

// One world per test: stubs, a fresh module evaluation, and a controller with the DOM parts neutralised.
function world(opts) {
  const w = { pendingBitmaps: [], fetches: [], failNext: {}, pointer: null, manifests: {}, pendingCoast: [], coastAnswer: () => ({ ok: false, status: 500 }) };
  const reg = { doc: {}, win: {} };
  const on = (t) => (ev, fn) => { (reg[t][ev] = reg[t][ev] || []).push(fn); };
  const off = (t) => (ev, fn) => { reg[t][ev] = (reg[t][ev] || []).filter((f) => f !== fn); };
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  const g = {
    L: { GridLayer: { prototype: { initialize(o) { this.options = o; this._tiles = {}; } },
      extend(p) { function C(o) { p.initialize.call(this, o); } C.prototype = Object.assign({ setOpacity() {}, addTo() { return this; } }, p); return C; } },
      DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} } },
    document: { hidden: !!(opts && opts.hidden), addEventListener: on('doc'), removeEventListener: off('doc'),
      createElement() { return { width: 0, height: 0, getContext() { let bmp = null; return {
        clearRect() {}, drawImage(b) { bmp = b; }, getImageData(x, y, wd, h) { const d = new Uint8ClampedArray(wd * h * 4).fill(1); d[0] = bmp.tag; return { data: d }; } }; } }; } },
    sessionStorage: (opts && opts.storage) || { getItem() { return null; }, setItem() {} },
    createImageBitmap: (blob) => new Promise((res) => w.pendingBitmaps.push(() => res({ width: blob.w, height: blob.h, tag: blob.tag, close() {} }))),
    fetch: (url, o) => {
      w.fetches.push(url);
      if (o && o.signal && o.signal.aborted) return Promise.reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
      if (url.endsWith('latest.json')) return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(w.pointer) });
      const mm = /gfswave\/0p25\/v1\/(\d{10})\/manifest-x\.json$/.exec(url);
      if (mm) return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(w.manifests[mm[1]]) });
      const fail = w.failNext[url]; if (fail) { delete w.failNext[url]; if (fail === 'network') return Promise.reject(new TypeError('Failed to fetch')); return Promise.resolve({ ok: false, status: fail }); }
      if (url.indexOf('/static/coast/') >= 0) return new Promise((res, rej) => {                  // released by hand, like decodes; aborts reject like a real fetch
        w.pendingCoast.push(() => res(w.coastAnswer(url)));
        if (o && o.signal) o.signal.addEventListener('abort', () => rej(Object.assign(new Error('aborted'), { name: 'AbortError' })));
      });
      const run = /\/v1\/(\d{10})\//.exec(url)[1];
      return Promise.resolve({ ok: true, status: 200, headers: { get: () => null }, blob: () => Promise.resolve({ w: 1440, h: 721, tag: Number(run.slice(-2)) }) });
    },
    setTimeout, clearTimeout, setInterval, clearInterval, Date, Math, console, Promise, Map, Array, Object, Number, String, Error, TypeError, JSON, isNaN, parseInt, parseFloat, AbortController,
    Uint8Array, Uint8ClampedArray, Float32Array, Float64Array, Symbol, matchMedia: undefined,
  };
  g.window = g; g.addEventListener = on('win'); g.removeEventListener = off('win');
  w.fire = (t, ev) => (reg[t][ev] || []).slice().forEach((f) => f({}));
  w.listeners = (t, ev) => (reg[t][ev] || []).length;
  w.doc = g.document;
  if (opts && opts.reduced) g.matchMedia = (q) => ({ matches: /reduced-motion/.test(q) });
  // timers never keep the test process alive (the module keeps a 30-min run-check interval while mounted)
  const st = (f, ms) => { const t = setTimeout(f, ms); if (t.unref) t.unref(); return t; };
  const si = (f, ms) => { const t = setInterval(f, ms); if (t.unref) t.unref(); return t; };
  // DecompressionStream is shadowed so the module takes the canvas path, whose stages the harness can hold
  const fn = new Function('L', 'document', 'sessionStorage', 'createImageBitmap', 'fetch', 'window', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'AbortController', 'DecompressionStream', src);
  fn(g.L, g.document, g.sessionStorage, g.createImageBitmap, g.fetch, g.window, st, clearTimeout, si, clearInterval, AbortController, undefined);
  const I = g.AllshoreOverlay._internals, Overlay = I.Overlay;
  for (const k of ['render', '_syncUI', '_attribute', '_bindReadout', '_bindMap', '_bindDocument', '_unbindReadout', '_unattribute', '_removeSheet']) {
    if (k === '_bindDocument' && opts && opts.realDocument) continue;
    Overlay.prototype[k] = function (st) { if (k === 'render') { this.last = st; this.ui = null; } };
  }
  Overlay.prototype._dims = () => ({ w: 1200, h: 800 });
  const map = { getPane: () => ({}), getZoom: () => 7, getSize: () => ({ x: 1200, y: 800 }), on() {}, off() {}, removeLayer() {},
    getContainer: () => ({ clientWidth: 1200, clientHeight: 800, classList: { add() {}, remove() {} }, style: { setProperty() {}, removeProperty() {} } }) };
  w.I = I;
  w.create = (extra) => g.AllshoreOverlay.create(map, Object.assign({ base: 'https://x/gfswave/0p25/v1', panel: {}, tz: 'UTC', getUnit: () => 'US', fmtTime: () => '', tzAbbr: () => '', pageCycle: () => null }, extra || {}));
  w.release = async (n) => { for (let i = 0; i < (n === undefined ? 1 : n) && w.pendingBitmaps.length; i++) w.pendingBitmaps.shift()(); await settle(); };
  w.releaseAll = async () => { while (w.pendingBitmaps.length) { w.pendingBitmaps.shift()(); await settle(); } };
  return w;
}
const A = manifest('2026092212', '2026-09-22T12:00:00Z', 12), B = manifest('2026092218', '2026-09-22T18:00:00Z', 18);

async function mounted(w, m) {
  w.pointer = ptr(m); w.manifests[m.run] = m;
  const o = w.create();
  o.mount('hs'); await settle();
  await w.release(1);                                          // the first frame lands; ring prefetch starts
  return o;
}
function drawnRun(o) { return o.layer._frame.q[0] === 12 ? A.run : o.layer._frame.q[0] === 18 ? B.run : '?'; }

test('a decode that outlives Update never reaches the new run: no old-run frame under a new-run time', async () => {
  const w = world(); w.manifests[B.run] = B;
  const o = await mounted(w, A);
  const idxA = o.frameIndex;
  assert.equal(drawnRun(o), A.run);
  assert.equal(w.pendingBitmaps.length, 2, 'two ring frames decoding');
  w.pointer = ptr(B); o.newerRun = w.pointer;
  o.update(); await settle();                                  // manifest B adopted while the run-A decodes are pending
  assert.equal(o.manifest.run, B.run); assert.equal(o.cache.size(), 0);
  const late = w.pendingBitmaps.splice(0, 2); late.forEach((f) => f()); await settle();
  assert.equal(o.cache.size(), 0, 'late run-A decodes must not populate the cache');
  assert.equal(Object.keys(o.unavailable).length, 0);
  await w.release(1);                                          // run B's target frame
  assert.equal(drawnRun(o), B.run);
  for (let guard = 0; guard < 6 && o.frameIndex < idxA + 2; guard++) { o.step(1); await settle(); await w.release(1); }
  assert.equal(drawnRun(o), B.run, 'stepping across the formerly poisoned steps draws run B');
  assert.equal(o.layer.entry.valid_utc, B.frames[o.frameIndex].valid_utc);
  clearInterval(o.runTimer); o.unmount();
});

test('in-flight bookkeeping: a late failure of an aborted fetch never marks the new run unavailable or drops its record', async () => {
  const w = world(); w.manifests[B.run] = B;
  const o = await mounted(w, A);
  const keysBefore = Object.keys(o.inflight);
  assert.ok(keysBefore.length <= 2 && keysBefore.length >= 1);
  w.pointer = ptr(B); o.newerRun = w.pointer;
  o.update(); await settle();
  // every remaining in-flight record belongs to run B; releasing the old decodes changes nothing
  for (const k of Object.keys(o.inflight)) assert.ok(k.indexOf(B.run) === 0 || k.indexOf(B.run) > 0, 'key carries the run: ' + k);
  const late = w.pendingBitmaps.splice(0, 2); late.forEach((f) => f()); await settle();
  for (const k of Object.keys(o.inflight)) assert.ok(k.includes(B.run));
  await w.releaseAll();
  assert.equal(drawnRun(o), B.run);
  assert.equal(Object.keys(o.unavailable).length, 0);
  clearInterval(o.runTimer); o.unmount();
});

test('never more than MAX_INFLIGHT fetches during rapid seeks; unmount during pending loads leaves nothing behind', async () => {
  const w = world();
  const o = await mounted(w, A);
  for (let i = 0; i < 12; i++) { o.seek((i * 7) % 81); await tick(); assert.ok(Object.keys(o.inflight).length <= w.I.MAX_INFLIGHT, 'inflight ' + Object.keys(o.inflight).length); }
  assert.ok(o.cache.size() <= w.I.MAX_DECODED);
  o.unmount();
  assert.equal(o.layer, null); assert.equal(o.cache.size(), 0); assert.equal(Object.keys(o.inflight).length, 0);
  assert.equal(o.timer, null); assert.equal(o.runTimer, null); assert.equal(o.playing, false);
  await w.releaseAll();                                        // decodes that were pending resolve into the void
  assert.equal(o.cache.size(), 0); assert.equal(o.layer, null);
});

test('outage then Retry with the drawn frame still cached ends in the ready state, not "Loading" (G4 B1)', async () => {
  const w = world();
  const o = await mounted(w, A);
  await w.releaseAll();
  const i0 = o.frameIndex, key = (i) => `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i].step).padStart(3, '0')}.png`;
  for (const i of [i0 + 10, i0 + 11, i0 + 12]) w.failNext['https://x/' + key(i)] = 'network';   // beyond the ring: an outage
  o.seek(i0 + 10); await settle(); o.seek(i0 + 11); await settle(); o.seek(i0 + 12); await settle();
  assert.equal(o.transientFails, 3);
  assert.equal(o.last && o.last.state, 'error');
  // the panel's Retry button does exactly this
  o.unavailable = {}; o.transientFails = 0; o.mount('hs'); await settle();
  assert.equal(o.last && o.last.state, 'ready', 'Retry with the cached frame must render ready');
  assert.equal(o.frameIndex, i0); assert.equal(o.target, i0);
  assert.ok(Object.keys(o.inflight).length >= 1, 'prefetch resumed');
  o.unmount();
});

test('unavailable frames: 404 is permanent and skipped, a network error is a cooldown, and the drawn frame never advances early', async () => {
  const w = world();
  const o = await mounted(w, A);
  await w.releaseAll();
  const i0 = o.frameIndex, key = (i) => `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i].step).padStart(3, '0')}.png`;
  w.failNext['https://x/' + key(i0 + 3)] = 404;               // beyond the ring: not fetched yet
  o.seek(i0 + 3); await settle();
  assert.equal(o.frameIndex, i0, 'picture unchanged'); assert.equal(o.target, i0 + 3);
  assert.equal(o._isUnavailable(i0 + 3), true);
  assert.equal(o.unavailable[o._key(i0 + 3)], true, 'permanent');
  o.step(1); await settle(); await w.releaseAll();
  assert.equal(o.frameIndex, i0 + 4 > i0 + 3 ? o.frameIndex : -1);
  assert.notEqual(o.frameIndex, i0 + 3, 'the missing frame is skipped');
  w.failNext['https://x/' + key(i0 + 9)] = 'network';
  o.seek(i0 + 9); await settle();
  const u = o.unavailable[o._key(i0 + 9)];
  assert.equal(typeof u, 'number'); assert.ok(u > Date.now(), 'cooldown timestamp');
  o.unmount();
});

// ---- coastlines ----
// A tiny coast-v1 file: one 1x1-degree island at 10 N 10 E (see tests/overlay/coast.test.js for the encoder).
function coastBytes() {
  const leb = (v) => { const out = []; do { const b = v % 128; v = Math.floor(v / 128); out.push(v ? b | 128 : b); } while (v); return out; };
  const zz = (v) => (v < 0 ? -2 * v - 1 : 2 * v), ring = [100000, 100000, 110000, 100000, 110000, 110000, 100000, 110000];
  const body = [...leb(zz(100000)), ...leb(zz(100000)), ...leb(10000), ...leb(10000), ...leb(1), ...leb(4)];
  let px = 100000, py = 100000;
  for (let i = 0; i < 8; i += 2) { body.push(...leb(zz(ring[i] - px)), ...leb(zz(ring[i + 1] - py))); px = ring[i]; py = ring[i + 1]; }
  const buf = new ArrayBuffer(40 + body.length), dv = new DataView(buf), u8 = new Uint8Array(buf);
  u8.set([67, 83, 84, 49], 0); dv.setUint16(4, 30, true); dv.setUint32(8, 10000, true); dv.setUint32(12, 1, true); dv.setUint32(16, 1, true); dv.setUint32(20, 4, true);
  [100000, 100000, 110000, 110000].forEach((v, i) => dv.setInt32(24 + i * 4, v, true));
  u8.set(body, 40);
  return buf;
}
const COAST_INDEX = { format: 'coast-v1', q: 10000, tier0: { file: 'world-i.bin', cell: 30, max_zoom: 6 }, tier1: { dir: 'f', cell: 5, min_zoom: 7, cells: {} } };
function coastOk(url) {
  if (url.endsWith('index.json')) return { ok: true, status: 200, json: () => Promise.resolve(COAST_INDEX) };
  const b = coastBytes();
  return { ok: true, status: 200, headers: { get: () => String(b.byteLength) }, arrayBuffer: () => Promise.resolve(b) };
}

test('coast: the first draw waits for the coastlines, a failed coast load draws unclipped, Off aborts a pending load', async () => {
  const w = world();
  w.coastAnswer = coastOk;
  const A3 = { ...A, fields: { hs: HS, tp: { lo: 1, hi: 30, legend: [4, 22], units: 's', interpolation: 'nearest' }, wind: { lo: 0, hi: 41.2, legend: [0, 30.9], units: 'm/s', interpolation: 'bilinear' } } };
  w.pointer = ptr(A3); w.manifests[A3.run] = A3;
  const o = w.create({ coast: true });
  o.mount('hs'); await settle();
  assert.equal(o.coast.status, 'loading');
  assert.equal(w.fetches.filter((u) => u.indexOf('/static/coast/') >= 0).length, 2);        // index + tier 0, beside latest.json/manifest/frame
  await w.release(1);                                                                          // the frame is decoded...
  assert.equal(o.layer._frame, null);                                                          // ...but not drawn: the coastlines are still loading
  while (w.pendingCoast.length) w.pendingCoast.shift()();
  await settle();
  assert.equal(o.coast.status, 'ok');
  assert.equal(o.layer._coast, o.coast); assert.equal(o.layer._clip, false);                 // clipping is decided per drawn field
  assert.equal(o.layer._frame, null);                                                          // the frame fetch starts only now
  for (let i = 0; i < 4 && !w.pendingBitmaps.length; i++) await settle();
  await w.release(1);
  assert.ok(o.layer._frame && o.last.state === 'ready', JSON.stringify([!!o.layer._frame, o.last]));
  assert.equal(o.layer._clip, true);
  o.mount('wind'); await settle(); await w.releaseAll(); await settle();                     // wind: never clipped, no coast wait (ring prefetches are released too)
  assert.equal(o.layer._clip, false); assert.ok(o.layer._frame, JSON.stringify(o.last));
  o.mount('tp'); await settle(); await w.releaseAll(); await settle();                       // back to a clipped field: the store is already there
  assert.equal(o.layer._clip, true);
  assert.equal(w.fetches.filter((u) => u.indexOf('/static/coast/') >= 0).length, 2);        // never fetched again
  o.unmount();
  assert.equal(o.coast.status, 'ok');                                                          // the decoded coastlines survive Off
  // a fresh page whose coast data is unreachable: the frame still lands, unclipped
  const w2 = world();
  w2.coastAnswer = () => ({ ok: false, status: 503 });
  w2.pointer = ptr(A); w2.manifests[A.run] = A;
  const o2 = w2.create({ coast: true });
  o2.mount('hs'); await settle();
  while (w2.pendingCoast.length) w2.pendingCoast.shift()();
  await settle();
  for (let i = 0; i < 4 && !w2.pendingBitmaps.length; i++) await settle();
  await w2.release(1);
  assert.equal(o2.coast.status, 'failed'); assert.equal(o2.layer._coast, null); assert.equal(o2.layer._clip, false);
  assert.ok(o2.layer._frame && o2.last.state === 'ready');
  // Off while the coast load is pending: the load is abandoned and the next On starts it again
  const w3 = world();
  w3.coastAnswer = coastOk;
  w3.pointer = ptr(A); w3.manifests[A.run] = A;
  const o3 = w3.create({ coast: true });
  o3.mount('hs'); await settle();
  o3.unmount(); await settle();
  assert.equal(o3.coast.status, 'idle');
  while (w3.pendingCoast.length) w3.pendingCoast.shift()();
  await settle();
  assert.equal(o3.coast.status, 'idle');                                                       // the late answer is ignored
  o3.mount('hs'); await settle();
  assert.equal(o3.coast.status, 'loading');
  assert.equal(w3.fetches.filter((u) => u.indexOf('/static/coast/') >= 0).length, 4);
  o3.unmount();
});

test('coast: a download that hangs past the watchdog fails the coast load; the frame draws unclipped with the store failed', async () => {
  const w = world();
  w.coastAnswer = coastOk;
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create({ coast: true });
  o.coast.timeoutMs = 40;                                                                      // the harness never answers coast fetches by itself
  o.mount('hs'); await settle();
  assert.equal(o.coast.status, 'loading');
  await new Promise((r) => setTimeout(r, 120));
  await settle();
  assert.equal(o.coast.status, 'failed');
  for (let i = 0; i < 4 && !w.pendingBitmaps.length; i++) await settle();
  await w.release(1);
  assert.ok(o.layer._frame && o.last.state === 'ready' && o.layer._clip === false, JSON.stringify(o.last));
  while (w.pendingCoast.length) w.pendingCoast.shift()();                                      // the late answers change nothing
  await settle();
  assert.equal(o.coast.status, 'failed');
  o.unmount();
  o.coast.timeoutMs = 15000;
  o.mount('hs'); await settle();
  assert.equal(o.coast.status, 'failed');                                                      // within the cooldown: no second wait, still unclipped
  o.unmount(); o.coast.retryMs = 0;
  o.mount('hs'); await settle();
  assert.equal(o.coast.status, 'loading');                                                     // after the cooldown the next On retries
  while (w.pendingCoast.length) w.pendingCoast.shift()();
  await settle();
  assert.equal(o.coast.status, 'ok');
  o.unmount();
});

// ---- A1: the overlay's state survives a reload (another forecast point) ----
function memStore(init) {
  const m = new Map(init ? [['allshore.overlay.v1', JSON.stringify(init)]] : []);
  return { getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => { m.set(k, String(v)); }, read: () => JSON.parse(m.get('allshore.overlay.v1') || '{}') };
}
async function restored(opts, st) {
  const storage = memStore(st), w = world(Object.assign({ storage }, opts));
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create();
  o.mount('hs', true); await settle(); await w.release(1);
  return { w, o, storage };
}
function done(o) { o.pause(); clearInterval(o.runTimer); o.unmount(); }

test('restoreIndex: a recent save within reach of the run resumes there; stale, far or malformed saves do not', () => {
  const w = world(), R = w.I.restoreIndex, now = Date.parse('2026-09-23T00:00:00Z'), t5 = Date.parse(A.frames[5].valid_utc);
  assert.equal(R(A, { t: t5, at: now - 60000 }, now), 5);
  assert.equal(R(A, { t: t5 + 80 * 60000, at: now }, now), 5);                                   // nearest frame, 80 min away: still fine
  assert.equal(R(A, { t: t5, at: now - 31 * 60000 }, now), null);                               // older than 30 min
  assert.equal(R(A, { t: t5, at: now + 5 * 60000 }, now), null);                                // written in the future
  assert.equal(R(A, { t: Date.parse(A.frames[80].valid_utc) + 3 * 3.6e6, at: now }, now), null); // 3 h beyond the run's last frame
  for (const bad of [null, {}, { t: 'x', at: now }, { t: t5 }, { t: NaN, at: now }]) assert.equal(R(A, bad, now), null, JSON.stringify(bad));
});

test('a reload restores the valid time and keeps playing; the state is written on landing, play and pause', async () => {
  const t5 = Date.parse(A.frames[5].valid_utc);
  const { o, storage } = await restored({}, { field: 'hs', t: t5, playing: true, at: Date.now() });
  assert.equal(o.frameIndex, 5); assert.equal(o.playing, true);
  let st = storage.read();
  assert.equal(st.field, 'hs'); assert.equal(st.t, t5); assert.equal(st.playing, true); assert.ok(Date.now() - st.at < 5000);
  o.pause(); st = storage.read();
  assert.equal(st.playing, false); assert.equal(st.t, t5);
  // Off while playing: the page saves field '' and unmounts; the unmount's own pause must not save the layer as on again
  o.play();
  storage.setItem('allshore.overlay.v1', JSON.stringify(Object.assign(storage.read(), { field: '', playing: false })));
  clearInterval(o.runTimer); o.unmount();
  assert.equal(storage.read().field, ''); assert.equal(storage.read().playing, false);
});

test('no resume under reduced motion or in a hidden tab (it resumes when shown); stale saves give the usual first frame', async () => {
  const t5 = Date.parse(A.frames[5].valid_utc), fresh = { field: 'hs', t: t5, playing: true, at: Date.now() };
  let r = await restored({ reduced: true }, fresh);
  assert.equal(r.o.frameIndex, 5); assert.equal(r.o.playing, false); done(r.o);
  r = await restored({ hidden: true }, fresh);
  assert.equal(r.o.frameIndex, 5); assert.equal(r.o.playing, false); assert.equal(r.o.wasPlaying, true);
  assert.equal(r.storage.read().playing, true);                                                   // still counts as playing for the next reload
  done(r.o);
  r = await restored({}, Object.assign({}, fresh, { at: Date.now() - 31 * 60000 }));
  assert.equal(r.o.frameIndex, r.w.I.pickFrame(A)); assert.equal(r.o.playing, false); done(r.o);
  // a mount the user makes (not a reload) never reuses the saved time
  const storage = memStore(fresh), w = world({ storage });
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.mount('hs'); await settle(); await w.release(1);
  assert.equal(o.frameIndex, w.I.pickFrame(A)); assert.equal(o.playing, false); done(o);
});

// ---- G8 fix round ----
const TPB = { lo: 1, hi: 30, legend: [4, 22], units: 's', interpolation: 'bilinear' };
function withTp(run, runUtc, tag) { const m = manifest(run, runUtc, tag); m.fields.tp = TPB; return m; }

test('the first landed frame is saved at once: field, valid time, not playing (no play or pause needed)', async () => {
  const storage = memStore(), w = world({ storage });
  const o = await mounted(w, A);
  const st = storage.read();
  assert.equal(st.field, 'hs'); assert.equal(st.t, Date.parse(A.frames[o.frameIndex].valid_utc)); assert.equal(st.playing, false);
  assert.ok(Date.now() - st.at < 5000);
  done(o);
});

test('leaving the page refreshes the save, paused or not: 31 min paused on the page, then another point keeps the time (G8 A-P2-1)', async () => {
  const storage = memStore(), w = world({ storage, realDocument: true }), realNow = Date.now;
  let clock = realNow();
  Date.now = () => clock;
  try {
    const o = await mounted(w, A);
    assert.equal(w.listeners('win', 'pagehide'), 1); assert.equal(w.listeners('doc', 'visibilitychange'), 1);
    o.seek(16); await settle(); await w.releaseAll();
    assert.equal(o.frameIndex, 16);
    o.pause();
    const t16 = Date.parse(A.frames[16].valid_utc);
    assert.equal(storage.read().t, t16);
    clock += 31 * 60000;                                                     // half an hour reading the table, overlay paused
    w.doc.hidden = true; w.fire('doc', 'visibilitychange');                   // navigating away hides the page first
    assert.equal(storage.read().at, clock); assert.equal(storage.read().playing, false); assert.equal(storage.read().t, t16);
    clock += 60000; w.fire('win', 'pagehide');                               // and pagehide alone does the same
    assert.equal(storage.read().at, clock);
    const w2 = world({ storage }); w2.pointer = ptr(A); w2.manifests[A.run] = A;
    const o2 = w2.create(); o2.mount('hs', true); await settle(); await w2.release(1);
    assert.equal(o2.frameIndex, 16, 'the next forecast point restores +48 h');
    done(o2);
    o.unmount();
    assert.equal(w.listeners('win', 'pagehide'), 0); assert.equal(w.listeners('doc', 'visibilitychange'), 0);
    w.fire('win', 'pagehide'); assert.equal(storage.read().field, 'hs', 'nothing is written after Off by a stale listener');
  } finally { Date.now = realNow; }
});

test('the saved time survives a field switch before the first restored frame and a Retry after a failed restore (G8 A-P3-5)', async () => {
  const M = withTp('2026092306', '2026-09-23T06:00:00Z', 6), t5 = Date.parse(M.frames[5].valid_utc);
  const st = { field: 'hs', t: t5, playing: false, at: Date.now() };
  let storage = memStore(st), w = world({ storage });
  w.pointer = ptr(M); w.manifests[M.run] = M;
  let o = w.create();
  o.mount('hs', true); o.mount('tp', false); await settle(); await w.releaseAll();
  assert.equal(o.field, 'tp'); assert.equal(o.frameIndex, 5); assert.equal(o._pendingRestore, null, 'spent once a frame is on the map');
  o.unmount(); o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(o.frameIndex, w.I.pickFrame(M), 'after Off a pick starts at the usual first frame');
  done(o);
  storage = memStore(st); w = world({ storage }); w.manifests[M.run] = M;
  w.pointer = null;                                                          // the bucket answers nonsense: the restore fails
  o = w.create(); o.mount('hs', true); await settle();
  assert.equal(o.last.state, 'error');
  w.pointer = ptr(M);
  o.unavailable = {}; o.transientFails = 0; o.mount(o.field); await settle(); await w.releaseAll();   // what Retry does
  assert.equal(o.frameIndex, 5, 'Retry lands on the saved time');
  done(o);
});

test('contours: off by default, remembered for the tab, the interval follows the site unit, a new layer gets them on its first frame', async () => {
  let unit = 'US';
  const storage = memStore(), w = world({ storage });
  w.pointer = ptr(A); w.manifests[A.run] = A;
  let o = w.create({ getUnit: () => unit });
  assert.equal(o.contours, false);
  o.mount('hs'); await settle(); await w.release(1);
  assert.equal(o.layer._contour, null);
  o.setContours(true);
  assert.equal(storage.read().contours, true);
  assert.deepEqual(o.layer._contour, { step: 2, per: 3.28084 });
  unit = 'Metric'; o.refresh();
  assert.deepEqual(o.layer._contour, { step: 0.5, per: 1 });
  done(o);
  const w2 = world({ storage }); w2.pointer = ptr(A); w2.manifests[A.run] = A;
  o = w2.create({ getUnit: () => 'US' });
  assert.equal(o.contours, true, 'remembered for the tab');
  o.mount('hs'); await settle(); await w2.release(1);
  assert.deepEqual(o.layer._contour, { step: 2, per: 3.28084 }, 'the new layer draws its first frame with them');
  o.setContours(false);
  assert.equal(o.layer._contour, null); assert.equal(storage.read().contours, false);
  done(o);
});

test('a corrupted session value counts as empty and the next write replaces it (G8 A-P3-3)', () => {
  for (const raw of ['5', '"hs"', '[1,2]', 'null', 'garbage', '{"opacity":0.4}']) {
    const m = new Map([['allshore.overlay.v1', raw]]);
    const storage = { getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => { m.set(k, String(v)); } };
    const o = world({ storage }).create();
    assert.equal(o.opacity, raw === '{"opacity":0.4}' ? 0.4 : 0.65, raw);
    o.setOpacity(0.5);
    const back = JSON.parse(m.get('allshore.overlay.v1'));
    assert.ok(back && typeof back === 'object' && !Array.isArray(back) && back.opacity === 0.5, raw + ' -> ' + m.get('allshore.overlay.v1'));
  }
});
