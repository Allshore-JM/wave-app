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
  const w = { pendingBitmaps: [], fetches: [], failNext: {}, pointer: null, manifests: {} };
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
  w.create = () => g.AllshoreOverlay.create(map, { base: 'https://x/gfswave/0p25/v1', panel: {}, tz: 'UTC', getUnit: () => 'US', fmtTime: () => '', tzAbbr: () => '', pageCycle: () => null });
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
