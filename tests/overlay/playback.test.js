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
const WIND = { lo: 0, hi: 41.15555555555556, legend: [0, 30.866666666666667], units: 'm/s', interpolation: 'bilinear' };
const PDIR = { lo: 0, hi: 360, legend: [0, 360], units: 'deg', interpolation: 'circular', resolutions: ['full', 'half'], circular: true, convention: 'from' };
const WDIR = Object.assign({}, PDIR, { resolutions: ['half'] });

// Every manifest carries the direction fields of phase B (the animation is off unless a test ticks it).
function manifest(run, runUtc, tag, nodirs) {
  const frames = [];
  for (let s = 0; s <= 240; s += 3) frames.push({ step: s, valid_utc: new Date(Date.parse(runUtc) + s * 3.6e6).toISOString().replace('.000Z', 'Z') });
  const fields = nodirs ? { hs: HS, wind: WIND } : { hs: HS, wind: WIND, pdir: PDIR, wdir: WDIR };
  return { schema: 3, run, run_utc: runUtc, encoding: 'u8-linear-v2', complete: true, fields, grid: GRID, grid_half: HALF, frames,
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
    createImageBitmap: (blob) => new Promise((res) => { const f = () => res({ width: blob.w, height: blob.h, tag: blob.tag, close() {} }); f.blob = blob; w.pendingBitmaps.push(f); }),
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
      const run = /\/v1\/(\d{10})\//.exec(url)[1], half = url.indexOf('/half/') >= 0;
      return Promise.resolve({ ok: true, status: 200, headers: { get: () => null }, blob: () => Promise.resolve({ w: half ? 720 : 1440, h: half ? 361 : 721, tag: Number(run.slice(-2)), url }) });
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
  // the animator's DOM and drawing are neutralised (flow.test.js exercises them); its data flow stays real
  for (const k of ['attach', 'detach', '_rebuild', '_clear', 'stop', '_start']) I.FlowAnimator.prototype[k] = function () { (this.calls = this.calls || []).push(k); };
  const map = { getPane: () => ({}), getZoom: () => 7, getSize: () => ({ x: 1200, y: 800 }), on() {}, off() {}, removeLayer() {},
    getContainer: () => ({ clientWidth: 1200, clientHeight: 800, classList: { add() {}, remove() {} }, style: { setProperty() {}, removeProperty() {} } }) };
  w.I = I; w.map = map;
  // release one pending decode by kind: the direction frames are the half-size ones in these tests
  w.releaseDir = async () => { const i = w.pendingBitmaps.findIndex((f) => f.blob.w === 720); assert.ok(i >= 0, 'a direction decode is pending'); w.pendingBitmaps.splice(i, 1)[0](); await settle(); };
  w.releaseField = async () => { const i = w.pendingBitmaps.findIndex((f) => f.blob.w === 1440); assert.ok(i >= 0, 'a field decode is pending'); w.pendingBitmaps.splice(i, 1)[0](); await settle(); };
  w.holdDir = () => { const i = w.pendingBitmaps.findIndex((f) => f.blob.w === 720); assert.ok(i >= 0); return w.pendingBitmaps.splice(i, 1)[0]; };
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

test('restore pending, then Off, then a pick: the usual first frame, not the saved time (the saved state ends at Off)', async () => {
  const w0 = world(), first = w0.I.pickFrame(A), k = (first + 40) % A.frames.length;
  const st = { field: 'hs', t: Date.parse(A.frames[k].valid_utc), playing: true, at: Date.now() };
  const storage = memStore(st), w = world({ storage });
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create();
  o.mount('hs', true); await settle();                                           // the restored frame is still decoding
  o.unmount();                                                                     // Off before it lands
  const s2 = storage.read(); s2.field = 'hs'; s2.playing = false; delete s2.t; delete s2.at;   // what the page's pick from Off writes
  storage.setItem('allshore.overlay.v1', JSON.stringify(s2));
  o.mount('hs', false); await settle(); await w.releaseAll();
  assert.equal(o.frameIndex, first); assert.notEqual(o.frameIndex, k); assert.equal(o.playing, false);
  done(o);
});

test('hiding the tab pauses playback and saves it as playing; showing it resumes', async () => {
  const storage = memStore(), w = world({ storage, realDocument: true });
  const o = await mounted(w, A);
  o.play(); assert.equal(o.playing, true);
  w.doc.hidden = true; w.fire('doc', 'visibilitychange');
  assert.equal(o.playing, false); assert.equal(o.wasPlaying, true); assert.equal(storage.read().playing, true);
  w.doc.hidden = false; w.fire('doc', 'visibilitychange');
  assert.equal(o.playing, true); assert.equal(o.wasPlaying, false);
  done(o);
});


// ---- direction frames and the animation (plan section 21 phase C) ----
const isDir = (k) => k.indexOf('/pdir/') >= 0 || k.indexOf('/wdir/') >= 0;

test('animation on: the direction frame rides beside the field frame, never more than MAX_INFLIGHT, keys and caches by kind, delivered only once the step is on the map', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true;                                       // the saved checkbox
  o.mount('hs'); await settle();
  assert.ok(o.flow, 'animator attached'); assert.equal(o.animAvailable(), true); assert.equal(o.dres, 'half', 'zoom 7: half-resolution direction');
  const keys = Object.keys(o.inflight);
  assert.equal(keys.length, 2, keys.join()); assert.ok(keys.some((k) => k.includes('/half/pdir/')) && keys.some((k) => k.includes('/full/hs/')));
  await w.releaseDir();                                                      // the direction lands first: cached, not shown (no frame on the map yet)
  assert.equal(o.flow.dir, null); assert.equal(o.dcache.size(), 1); assert.equal(o.cache.size(), 0);
  await w.releaseField();                                                    // the field lands: the cached direction is delivered with it
  assert.ok(o.flow.dir, 'direction delivered'); assert.equal(o.flow.dir, o.dcache.get(o._key(o.frameIndex, 'dir')));
  assert.ok(o.flow.calls.includes('_rebuild'));
  for (let i = 0; i < 12; i++) { o.seek((i * 7) % 81); await tick(); assert.ok(Object.keys(o.inflight).length <= w.I.MAX_INFLIGHT, 'inflight ' + Object.keys(o.inflight).length); }
  for (const k of o.cache.map.keys()) assert.ok(!isDir(k), 'field cache: ' + k);
  for (const k of o.dcache.map.keys()) assert.ok(k.includes('/half/pdir/'), 'direction cache: ' + k);
  assert.ok(o.dcache.size() <= w.I.MAX_DECODED);
  const flow = o.flow;
  o.unmount();
  assert.equal(o.flow, null); assert.equal(o.dcache.size(), 0); assert.equal(o.dres, null); assert.equal(Object.keys(o.inflight).length, 0);
  assert.ok(flow.calls.includes('detach'));
  await w.releaseAll(); assert.equal(o.dcache.size(), 0, 'late decodes resolve into the void');
});

test('a direction frame that lands after the map moved on is never shown under the new step (a newer target drops it)', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  const i0 = o.frameIndex; assert.ok(o.flow.dir);
  o.seek(i0 + 5); await settle();                                            // outside the ring: field + direction fetched
  await w.releaseField();                                                    // its field decoded: the step waits for its direction
  assert.equal(o.frameIndex, i0, 'waiting for the direction of i0 + 5'); assert.ok(o.inflight[o._key(i0 + 5, 'dir')]);
  const late = w.holdDir();                                                  // hold that direction decode
  o.seek(i0 + 9); await settle();                                            // a newer target: the old one's direction is dropped
  assert.equal(o.inflight[o._key(i0 + 5, 'dir')], undefined, 'aborted');
  late(); await settle();
  assert.equal(o.dcache.has(o._key(i0 + 5, 'dir')), false, 'a late decode of an aborted fetch is dropped');
  assert.equal(o.frameIndex, i0, 'the picture never moved to i0 + 5'); assert.equal(o.flow.entry, A.frames[i0], 'the direction on the map is still that of i0');
  await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 9); assert.equal(o.flow.entry, A.frames[i0 + 9], 'the new step shows its own direction'); assert.equal(o.flow.dir, o.dcache.get(o._key(i0 + 9, 'dir')));
  o.unmount();
});

test('a run without direction fields: the animation is unavailable and no direction frame is ever requested', async () => {
  const w = world(); const A0 = manifest(A.run, A.run_utc, 12, true); w.pointer = ptr(A0); w.manifests[A0.run] = A0;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(o.animAvailable(), false); assert.equal(o.flow, null); assert.equal(o.dres, null);
  o.step(1); await settle(); await w.releaseAll();
  assert.ok(!w.fetches.some(isDir), 'no direction fetch'); assert.equal(o.dcache.size(), 0);
  o.unmount();
});

test('unticking Animation drops the direction fetches and cache; ticking it again fetches the direction of the frame on the map', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseField();
  assert.ok(Object.keys(o.inflight).some(isDir));
  o.setAnim(false); await settle();
  assert.equal(o.flow, null); assert.ok(!Object.keys(o.inflight).some(isDir), 'direction fetches aborted'); assert.equal(o.dcache.size(), 0);
  const before = w.fetches.length;
  o.setAnim(true); await settle();
  assert.ok(o.flow); assert.ok(w.fetches.slice(before).some((u) => u.includes('/half/pdir/f' + String(A.frames[o.frameIndex].step).padStart(3, '0'))), 'the shown step direction is fetched first');
  assert.ok(Object.keys(o.inflight).length <= w.I.MAX_INFLIGHT);
  await w.releaseAll(); assert.ok(o.flow.dir);
  o.unmount();
});

test('wind takes the half-resolution wind direction at every zoom; pdir follows the zoom with hysteresis and a change refetches without clearing', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  w.map.getZoom = () => 9;
  const o = w.create(); o.anim = true; o.mount('wind'); await settle();
  assert.equal(o.dres, 'half'); assert.ok(Object.keys(o.inflight).some((k) => k.includes('/half/wdir/')));
  await w.releaseAll(); assert.ok(o.flow.dir);
  o.mount('hs'); await settle();                                             // a field change at zoom 9: full-resolution pdir
  assert.equal(o.dres, 'full'); assert.equal(o.flow.dir, null, 'the wind direction is not shown under wave height');
  const fetches = w.fetches.filter((u) => u.includes('/pdir/'));
  assert.ok(fetches.length && fetches.every((u) => !u.includes('/half/')), 'full-resolution pdir at zoom 9');
  w.pendingBitmaps.forEach((f) => { if (f.blob.w === 1440) f(); }); w.pendingBitmaps = w.pendingBitmaps.filter((f) => f.blob.w !== 1440); await settle();
  assert.ok(o.flow.dir, 'full pdir delivered'); const shown = o.flow.dir;
  w.map.getZoom = () => 6.5; o._checkRes(); await settle();                  // zoom out: half-resolution pdir, the shown one stays meanwhile
  assert.equal(o.dres, 'half'); assert.equal(o.flow.dir, shown, 'kept until the half frame lands');
  await w.releaseDir();
  assert.notEqual(o.flow.dir, shown); assert.equal(o.flow.dir.cols, 720);
  o.unmount();
});

test('Update: the direction cache goes with the run and a late old-run direction decode never reaches the animator', async () => {
  const w = world(); w.manifests[B.run] = B; w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseField();
  assert.ok(w.pendingBitmaps.some((f) => f.blob.w === 720), 'run A direction pending');
  w.pointer = ptr(B); o.newerRun = w.pointer; o.update(); await settle();
  assert.equal(o.manifest.run, B.run); assert.equal(o.dcache.size(), 0); assert.equal(o.flow.dir, null);
  const old = w.pendingBitmaps.filter((f) => f.blob.url.includes(A.run)); w.pendingBitmaps = w.pendingBitmaps.filter((f) => !f.blob.url.includes(A.run));
  old.forEach((f) => f()); await settle();
  assert.equal(o.dcache.size(), 0, 'no run-A direction in the cache'); assert.equal(o.flow.dir, null);
  await w.releaseAll();
  assert.ok(o.flow.dir && o.flow.dir.q[0] === 18, 'run B direction shown'); assert.equal(drawnRun(o), B.run);
  for (const k of o.dcache.map.keys()) assert.ok(k.startsWith(B.run + '/'));
  clearInterval(o.runTimer); o.unmount();
});

// ---- G10-A: a pending seek / step survives every new caller of _prefetch / _startDir; delivery follows the layer ----

test('G10-A S1: a far seek is loading, then the direction resolution changes (zoom 7 -> 7.6): the seek still lands', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  const i0 = o.frameIndex;
  assert.equal(o.res, 'full'); assert.equal(o.dres, 'half');
  o.seek(i0 + 30); await settle();                                            // far target: outside the ring of i0
  assert.equal(o.target, i0 + 30); assert.ok(o.inflight[o._key(i0 + 30)], 'the target field is in flight');
  w.map.getZoom = () => 7.6; o._checkRes(); await settle();                   // desktop hs: res stays full, dres half -> full
  assert.equal(o.res, 'full'); assert.equal(o.dres, 'full');
  assert.ok(o.inflight[o._key(i0 + 30)], 'the pending target fetch survives the direction resolution change');
  assert.ok(Object.keys(o.inflight).length <= w.I.MAX_INFLIGHT);
  await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 30, 'the seek landed'); assert.equal(o.target, i0 + 30);
  assert.ok(o.flow.dir && o.flow.dir.cols === 1440, 'the full-resolution direction of the landed step is shown');
  o.unmount();
});

test('G10-A S2 / S5: Animation ticked while a step is loading (outside the ring, and with _startDir eviction): the step still lands', async () => {
  for (const ahead of [3, 2]) {
    const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
    const o = w.create(); o.anim = false; o.mount('hs'); await settle();
    if (ahead === 3) await w.releaseAll(); else await w.release(1);            // S2: all cached, seek past the ring; S5: ring {i0+1, i0+2} in flight
    const i0 = o.frameIndex;
    o.seek(i0 + ahead); await settle();
    assert.equal(o.target, i0 + ahead); assert.ok(o.inflight[o._key(i0 + ahead)], 'target in flight');
    o.setAnim(true); await settle();
    assert.ok(o.inflight[o._key(i0 + ahead)], 'the pending target survives ticking Animation (ahead ' + ahead + ')');
    assert.ok(Object.keys(o.inflight).length <= w.I.MAX_INFLIGHT);
    await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, i0 + ahead, 'landed (ahead ' + ahead + ')');
    assert.ok(o.flow.dir, 'its direction followed');
    o.unmount();
  }
});

test('G10-A: unticking Animation while a step is loading keeps the step; the shown step gets no direction fetch while another target is pending', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  const i0 = o.frameIndex;
  o.seek(i0 + 30); await settle();
  o.setAnim(false); await settle();
  assert.ok(o.inflight[o._key(i0 + 30)], 'the target survives unticking'); assert.ok(!Object.keys(o.inflight).some(isDir));
  o.setAnim(true); await settle();
  assert.ok(!o.inflight[o._key(i0, 'dir')], 'no direction fetch for the shown frame while the seek is pending');
  assert.ok(o.inflight[o._key(i0 + 30)] && (o.inflight[o._key(i0 + 30, 'dir')] || Object.keys(o.inflight).length <= 2), 'the target and its direction come first');
  await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 30); assert.ok(o.flow.dir);
  o.unmount();
});

test('G10-A M26 / G10-C P2-1: a step shows its picture and its direction together; the old direction never sits under the new step', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  const shown = o.flow.dir, i0 = o.frameIndex; assert.ok(shown, 'a direction is on the map');
  const dirUrl = (i) => 'https://x/' + `gfswave/0p25/v1/${A.run}/half/pdir/f${String(A.frames[i].step).padStart(3, '0')}.png`;
  w.failNext[dirUrl(i0 + 4)] = 404; w.failNext[dirUrl(i0 + 5)] = 'network';   // beyond the ring: not fetched yet
  o.seek(i0 + 3); await settle(); await w.releaseField();                    // outside the ring: its field decoded, it waits for its direction
  assert.equal(o.frameIndex, i0, 'the picture waits for its direction'); assert.equal(o.flow.dir, shown, 'the old picture keeps its own direction meanwhile');
  await w.releaseDir();
  assert.equal(o.frameIndex, i0 + 3); assert.ok(o.flow.dir && o.flow.dir !== shown, 'both changed together'); assert.equal(o.flow.entry, A.frames[i0 + 3]);
  await w.releaseAll();                                                       // the ring: i0 + 4's direction 404s
  o.step(1); await settle(); await w.releaseAll();
  assert.equal(o.frameIndex, i0 + 4, 'a step whose direction is missing lands without it'); assert.equal(o.flow.dir, null); assert.equal(o._isUnavailable(i0 + 4, 'dir'), true);
  o.step(1); await settle(); await w.releaseAll();
  assert.equal(o.frameIndex, i0 + 5, 'a transient direction failure does not hold the step either'); assert.equal(o.flow.dir, null);
  o.unmount();
});

test('G10-A M11/M12: _trimInflight keeps no survivor beside a target whose direction is in flight, exactly one otherwise', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.release(1);
  // constructed sets: the record shape is what the scheduler looks at
  const rec = (k, kind) => ({ promise: Promise.resolve(), abort: new AbortController(), key: k, kind });
  const i = o.frameIndex, t = i + 1;
  o.inflight = {}; o.inflight[o._key(t)] = rec(o._key(t), 'field'); o.inflight[o._key(t, 'dir')] = rec(o._key(t, 'dir'), 'dir'); o.inflight[o._key(i + 2)] = rec(o._key(i + 2), 'field');
  o.dir = 1; o._trimInflight(t);
  assert.deepEqual(Object.keys(o.inflight).sort(), [o._key(t), o._key(t, 'dir')].sort(), 'direction target in flight: no other survivor');
  o.inflight = {}; o.inflight[o._key(t)] = rec(o._key(t), 'field'); o.inflight[o._key(i + 2)] = rec(o._key(i + 2), 'field'); o.inflight[o._key(i + 3)] = rec(o._key(i + 3), 'field');
  o._trimInflight(t);
  assert.equal(Object.keys(o.inflight).length, 2, 'exactly one survivor beside the target'); assert.ok(o.inflight[o._key(t)]);
  o.inflight = {}; o.unmount();
});

test('G10-A M60 / P3-5 / P3-4: a cached direction is not fetched again; untick before the first frame aborts the direction fetch; direction successes do not mask field failures', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle();
  assert.ok(Object.keys(o.inflight).some(isDir));
  o.setAnim(false); await settle();                                           // before the first frame landed
  assert.ok(!Object.keys(o.inflight).some(isDir), 'direction fetch aborted before the first landing'); assert.equal(o.dcache.size(), 0);
  o.setAnim(true); await settle(); await w.releaseAll();
  const before = w.fetches.length; const key = o._key(o.frameIndex, 'dir'); assert.ok(o.dcache.has(key));
  o._startDir(o.frameIndex); o._syncFlow(o.frameIndex); await settle();
  assert.equal(w.fetches.length, before, 'a cached direction is never downloaded again');
  // three field failures with direction successes in between still trip the outage
  const i0 = o.frameIndex, fkey = (i) => `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i].step).padStart(3, '0')}.png`;
  for (const i of [i0 + 10, i0 + 20, i0 + 30]) w.failNext['https://x/' + fkey(i)] = 'network';
  o.seek(i0 + 10); await settle(); await w.releaseAll(); o.seek(i0 + 20); await settle(); await w.releaseAll(); o.seek(i0 + 30); await settle(); await w.releaseAll();
  assert.equal(o.transientFails, 3, 'the counter follows the field frames only'); assert.equal(o.last && o.last.state, 'error');
  o.unmount();
});

test('G10-A D1 / M52: on a field switch a direction that lands before the field frame is not delivered; the animator is told the new field', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  assert.ok(o.flow.dir); assert.equal(o.flow.field, 'hs');
  o.mount('wind'); await settle();
  assert.equal(o.flow.field, 'wind'); assert.equal(o.flow.dir, null); assert.equal(o.layer.hasFrame(), false);
  await w.releaseDir();                                                       // the wind direction lands first (it is smaller)
  assert.equal(o.flow.dir, null, 'not delivered: the layer has no frame yet'); assert.ok(o.dcache.has(o._key(o.frameIndex, 'dir')), 'but cached');
  o.flow.resume(true);                                                        // a tab show in that window
  await w.releaseField();
  assert.ok(o.flow.dir && o.flow.dir.cols === 720, 'delivered with the field frame'); assert.equal(o.flow.entry, A.frames[o.frameIndex]);
  o.unmount();
});

test('G10-A P3-8: a phone crossing zoom 7.5 changes both resolutions; the same step direction stays until the new one lands', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  w.I.Overlay.prototype._dims = () => ({ w: 400, h: 700 }); w.map.getZoom = () => 6.5;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(o.res, 'half'); assert.equal(o.dres, 'half');
  const shown = o.flow.dir; assert.ok(shown);
  w.map.getZoom = () => 7.6; o._checkRes(); await settle();
  assert.equal(o.res, 'full'); assert.equal(o.dres, 'full');
  assert.equal(o.flow.dir, shown, 'the same step direction is kept across the resolution change');
  await w.releaseAll(); await settle();
  assert.ok(o.flow.dir !== shown && o.flow.dir.cols === 1440, 'then replaced by the full-resolution one');
  o.unmount();
});

test('G10-A P3-2: a manifest whose direction grid fails validation gives no animation for that run', async () => {
  const w = world(); const bad = manifest(A.run, A.run_utc, 12); bad.grid_half = Object.assign({}, HALF, { dlon: 1 });
  w.pointer = ptr(bad); w.manifests[bad.run] = bad;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(o.animAvailable(), false); assert.equal(o.flow, null); assert.ok(!w.fetches.some(isDir));
  o.unmount();
});

test('G11 P1-1: under reduced motion nothing animates and no direction frame is fetched; the checkbox reports it', async () => {
  const w = world({ reduced: true }); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(o.animAvailable(), true, 'the run has direction data'); assert.equal(o._wantDir(), false); assert.equal(o.flow, null);
  assert.ok(!w.fetches.some(isDir), 'no direction fetch'); assert.equal(o.dcache.size(), 0);
  o.step(1); await settle(); await w.releaseAll(); assert.ok(!w.fetches.some(isDir));
  o.unmount();
});
