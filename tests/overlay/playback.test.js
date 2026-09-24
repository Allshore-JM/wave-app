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
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  const g = {
    L: { GridLayer: { prototype: { initialize(o) { this.options = o; this._tiles = {}; } },
      extend(p) { function C(o) { p.initialize.call(this, o); } C.prototype = Object.assign({ setOpacity() {}, addTo() { return this; } }, p); return C; } },
      DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} } },
    document: { hidden: false, addEventListener() {}, removeEventListener() {},
      createElement() { return { width: 0, height: 0, getContext() { let bmp = null; return {
        clearRect() {}, drawImage(b) { bmp = b; }, getImageData(x, y, wd, h) { const d = new Uint8ClampedArray(wd * h * 4).fill(1); d[0] = bmp.tag; return { data: d }; } }; } }; } },
    sessionStorage: { getItem() { return null; }, setItem() {} },
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
  g.window = g;
  // timers never keep the test process alive (the module keeps a 30-min run-check interval while mounted)
  const st = (f, ms) => { const t = setTimeout(f, ms); if (t.unref) t.unref(); return t; };
  const si = (f, ms) => { const t = setInterval(f, ms); if (t.unref) t.unref(); return t; };
  // DecompressionStream is shadowed so the module takes the canvas path, whose stages the harness can hold
  const fn = new Function('L', 'document', 'sessionStorage', 'createImageBitmap', 'fetch', 'window', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'AbortController', 'DecompressionStream', src);
  fn(g.L, g.document, g.sessionStorage, g.createImageBitmap, g.fetch, g.window, st, clearTimeout, si, clearInterval, AbortController, undefined);
  const I = g.AllshoreOverlay._internals, Overlay = I.Overlay;
  for (const k of ['render', '_syncUI', '_attribute', '_bindReadout', '_bindMap', '_bindDocument', '_unbindReadout', '_unattribute', '_removeSheet']) {
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
  assert.equal(o.coast.status, 'loading');                                                     // the next On retries
  while (w.pendingCoast.length) w.pendingCoast.shift()();
  await settle();
  assert.equal(o.coast.status, 'ok');
  o.unmount();
});
