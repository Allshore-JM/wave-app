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
const PRES = { lo: 920, hi: 1060, legend: [920, 1060], units: 'hPa', interpolation: 'bilinear', resolutions: ['half'] };
// The Great Lakes run (plan section 50): the published grid, hourly frames, the model's own ranges at full resolution only.
const LAKE_GRID = { cols: 831, rows: 401, lon0: -92.4, lat0: 49.2, dlon: 0.02, dlat: -0.02, registration: 'center', lon_periodic: false };
const LAKE_FIELDS = { hs: Object.assign({}, HS, { resolutions: ['full'] }), tp: { lo: 1, hi: 30, legend: [4, 22], units: 's', interpolation: 'bilinear', resolutions: ['full'] },
  pdir: Object.assign({}, PDIR, { resolutions: ['full'] }) };
function lakeManifest(run, runUtc, hours) {
  const frames = [];
  for (const h of hours || [...Array(150).keys()]) frames.push({ step: h, valid_utc: new Date(Date.parse(runUtc) + h * 3.6e6).toISOString().replace('.000Z', 'Z') });
  return { schema: 3, run, run_utc: runUtc, encoding: 'u8-linear-v2', complete: true, fields: LAKE_FIELDS, grid: LAKE_GRID, frames,
    files: { template: `glwu/v1/${run}/{res}{field}/f{step:03d}.png`, res: { full: '' } }, model: {} };
}
const lptr = (m) => ({ run: m.run, manifest: `glwu/v1/${m.run}/manifest-x.json`, complete: true, encoding: 'u8-linear-v2', frames: m.frames.length, published_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z') });

// Every manifest carries the direction fields of phase B (the animation is off unless a test ticks it).
// steps: the frame hours (default 0..240 every 3 h, the 81-frame runs published before plan section 22)
const FULL_HORIZON = [...Array(121).keys()].concat([...Array(88).keys()].map((i) => 123 + 3 * i));   // hourly to 120, 3-hourly to 384
function manifest(run, runUtc, tag, nodirs, steps) {
  const frames = [];
  for (const s of steps || [...Array(81).keys()].map((i) => 3 * i)) frames.push({ step: s, valid_utc: new Date(Date.parse(runUtc) + s * 3.6e6).toISOString().replace('.000Z', 'Z') });
  const fields = nodirs ? { hs: HS, wind: WIND } : { hs: HS, wind: WIND, pdir: PDIR, wdir: WDIR, pres: PRES };
  return { schema: 3, run, run_utc: runUtc, encoding: 'u8-linear-v2', complete: true, fields, grid: GRID, grid_half: HALF, frames,
    files: { template: `gfswave/0p25/v1/${run}/{res}{field}/f{step:03d}.png`, res: { full: '', half: 'half/' } }, model: {}, tag };
}
const ptr = (m) => ({ run: m.run, manifest: `gfswave/0p25/v1/${m.run}/manifest-x.json`, complete: true, published_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z') });
const tick = () => new Promise((r) => setImmediate(r));
async function settle(n) { for (let i = 0; i < (n || 6); i++) await tick(); }
// Tests that step or seek from "now" through run A (2026-09-22 12Z, 81 frames to +240 h) run on A's own day,
// with a clock that still advances (cooldowns and timers keep working).
async function onRunDay(fn) {
  const real = Date.now, t0 = real(), base = Date.parse('2026-09-23T00:00:00Z');
  Date.now = () => base + (real() - t0);
  try { await fn(); } finally { Date.now = real; }
}

// One world per test: stubs, a fresh module evaluation, and a controller with the DOM parts neutralised.
function world(opts) {
  const w = { pendingBitmaps: [], fetches: [], failNext: {}, hang: {}, pointer: null, manifests: {}, pendingCoast: [], coastAnswer: () => ({ ok: false, status: 500 }), pendingTerrain: [],
    lakePointer: null, lakeManifests: {} };
  const reg = { doc: {}, win: {}, map: {} };
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
      // the isobars' terrain asset (fix round 3): its own list, outside w.fetches (the frame-request pins); pending until
      // w.releaseTerrain() answers it (decoded by the canvas fake: code 1 everywhere, the tag at node 0)
      if (url.indexOf('/overlay/terrain_half.png') >= 0) {
        if (w.terrainAnswer) return Promise.resolve(w.terrainAnswer);                                   // a canned failure
        return new Promise((res) => { w.pendingTerrain.push(() => res({ ok: true, status: 200, headers: { get: () => null }, blob: () => Promise.resolve({ w: 720, h: 361, tag: 1, url }) })); });
      }
      w.fetches.push(url);
      if (o && o.signal && o.signal.aborted) return Promise.reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
      if (w.streams) { const sr = w.streams(url, o); if (sr) return Promise.resolve(sr); }   // a hand-made streamed answer (re-check R-1)
      if (/latest\.json$|manifest-x\.json$/.test(url)) (w.jsonSignals = w.jsonSignals || []).push({ url, signal: o && o.signal });   // re-check R-5
      if (/\/glwu\/v1\/latest\.json$/.test(url)) {                          // the lake run's pointer (plan section 50): none = no lake run
        if (!w.lakePointer) return Promise.resolve({ ok: false, status: 404 });
        const la = { ok: true, status: 200, headers: { get: () => null }, json: () => Promise.resolve(w.lakePointer) };
        if (w.holdLakePointer) return new Promise((res) => { (w.heldLakePointers = w.heldLakePointers || []).push(() => res(la)); });
        return Promise.resolve(la);
      }
      const lmm = /\/glwu\/v1\/(\d{10})\/manifest-x\.json$/.exec(url);
      if (lmm) return Promise.resolve({ ok: true, status: 200, headers: { get: () => null }, json: () => Promise.resolve(w.lakeManifests[lmm[1]]) });
      if (url.endsWith('latest.json')) {                                     // per model (w.pointers[prefix]), else the one pointer
        const pm = /(gfswave|ecmwf)\/0p25\/v1\/latest\.json$/.exec(url), pp = w.pointers && pm ? w.pointers[pm[1]] : undefined;
        const answer = { ok: true, status: 200, json: () => Promise.resolve(pp !== undefined ? pp : w.pointer) };
        if (w.holdPointer) return new Promise((res) => { (w.heldPointers = w.heldPointers || []).push(() => res(answer)); });   // released by hand
        return Promise.resolve(answer);
      }
      const mm = /(gfswave|ecmwf)\/0p25\/v1\/(\d{10})\/manifest-x\.json$/.exec(url);
      if (mm) {
        const answer = { ok: true, status: 200, json: () => Promise.resolve(w.manifests[mm[1] + '/' + mm[2]] || w.manifests[mm[2]]) };
        if (w.holdManifest) return new Promise((res) => { (w.heldManifests = w.heldManifests || []).push(() => res(answer)); });   // released by hand
        return Promise.resolve(answer);
      }
      if (w.hang[url] > 0) {                                                 // a download that never answers (until aborted)
        w.hang[url]--; (w.hung = w.hung || []).push({ url, signal: o && o.signal });
        return new Promise((res, rej) => { if (o && o.signal) o.signal.addEventListener('abort', () => rej(Object.assign(new Error('aborted'), { name: 'AbortError' }))); });
      }
      const fail = w.failNext[url]; if (fail) { delete w.failNext[url]; if (fail === 'network') return Promise.reject(new TypeError('Failed to fetch')); return Promise.resolve({ ok: false, status: fail }); }
      if (url.indexOf('/static/coast/') >= 0) return new Promise((res, rej) => {                  // released by hand, like decodes; aborts reject like a real fetch
        w.pendingCoast.push(() => res(w.coastAnswer(url)));
        if (o && o.signal) o.signal.addEventListener('abort', () => rej(Object.assign(new Error('aborted'), { name: 'AbortError' })));
      });
      const run = /\/v1\/(\d{10})\//.exec(url)[1], half = url.indexOf('/half/') >= 0, lake = url.indexOf('/glwu/v1/') >= 0;
      return Promise.resolve({ ok: true, status: 200, headers: { get: () => null }, blob: () => Promise.resolve({ w: lake ? LAKE_GRID.cols : half ? 720 : 1440, h: lake ? LAKE_GRID.rows : half ? 361 : 721, tag: Number(run.slice(-2)), url }) });
    },
    setTimeout, clearTimeout, setInterval, clearInterval, Date, Math, console, Promise, Map, Array, Object, Number, String, Error, TypeError, JSON, isNaN, parseInt, parseFloat, AbortController,
    Uint8Array, Uint8ClampedArray, Float32Array, Float64Array, Symbol, matchMedia: undefined,
  };
  g.window = g; g.addEventListener = on('win'); g.removeEventListener = off('win');
  w.g = g;                                                                   // the module's window (a page global such as AllshoreBasemap)
  w.fire = (t, ev) => (reg[t][ev] || []).slice().forEach((f) => f({}));
  w.listeners = (t, ev) => (reg[t][ev] || []).length;
  w.doc = g.document;
  if (opts && opts.reduced) g.matchMedia = (q) => ({ matches: /reduced-motion/.test(q) });
  // timers never keep the test process alive (the module keeps a 30-min run-check interval while mounted)
  let st = (f, ms) => { const t = setTimeout(f, ms); if (t.unref) t.unref(); return t; }, ct = clearTimeout;
  let si = (f, ms) => { const t = setInterval(f, ms); if (t.unref) t.unref(); return t; }, ci = clearInterval, raf, caf;
  if (opts && opts.fakeTimers) {                 // a test that must not depend on the machine's speed (G28 K-2)
    let now = 0, seq = 0; const q = new Map();
    w.now = () => now;                             // the fake clock (a test pins Date.now to it: section 42's cadence)
    st = (f, ms) => { const id = ++seq; q.set(id, { f, at: now + (ms || 0) }); return id; };
    ct = (id) => { q.delete(id); };
    if (opts.fakeIntervals) {                      // intervals in the same queue (the teardown pin counts what is left after Off)
      si = (f, ms) => { const id = ++seq; q.set(id, { f, at: now + (ms || 1), every: ms || 1 }); return id; };
      ci = ct;
    }
    w.pendingTimers = () => q.size; w.timerSources = () => [...q.values()].map((x) => (x.every ? 'every ' + x.every + ' ' : '') + String(x.f).replace(/\s+/g, ' ').slice(0, 120));
    w.fade = async (ms) => {                       // the fake clock moves on; due timers run in order
      now += ms === undefined ? 330 : ms;
      for (;;) {
        const due = [...q].filter(([, x]) => x.at <= now).sort((a, b) => a[1].at - b[1].at)[0];
        if (!due) break;
        if (due[1].every) due[1].at += due[1].every; else q.delete(due[0]);
        due[1].f();
        await Promise.resolve();
      }
    };
    if (opts.raf) {                                // a fake animation frame: w.paint() runs the callbacks queued since (the paint gate)
      w.rafQ = new Map();
      raf = (f) => { const id = ++seq; w.rafQ.set(id, f); return id; };
      caf = (id) => { w.rafQ.delete(id); };
      w.paint = async () => { const cbs = [...w.rafQ.values()]; w.rafQ.clear(); for (const f of cbs) f(now); await w.fade(0); };
    }
  }
  // DecompressionStream is shadowed so the module takes the canvas path, whose stages the harness can hold
  const fn = new Function('L', 'document', 'sessionStorage', 'createImageBitmap', 'fetch', 'window', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'AbortController', 'DecompressionStream', 'requestAnimationFrame', 'cancelAnimationFrame', src);
  fn(g.L, g.document, g.sessionStorage, g.createImageBitmap, g.fetch, g.window, st, ct, si, ci, AbortController, undefined, raf, caf);
  const I = g.AllshoreOverlay._internals, Overlay = I.Overlay;
  w.orig = {};                                                             // the stubbed methods' originals (a test may bind the real one)
  for (const k of ['render', '_syncUI', '_attribute', '_bindReadout', '_bindMap', '_bindDocument', '_unbindReadout', '_unattribute', '_removeSheet']) {
    if (k === '_bindDocument' && opts && opts.realDocument) continue;
    w.orig[k] = Overlay.prototype[k];
    Overlay.prototype[k] = function (st) { if (k === 'render') { this.last = st; this.ui = null; } };
  }
  Overlay.prototype._dims = () => ({ w: 1200, h: 800 });
  // the animator's DOM and drawing are neutralised (flow.test.js exercises them); its data flow stays real
  for (const k of ['attach', 'detach', '_rebuild', '_clear', 'stop', '_start']) I.FlowAnimator.prototype[k] = function () { (this.calls = this.calls || []).push(k); };
  // the isobar layer likewise: no canvas, no animation frame; setData / clearData keep their data flow (entry, frame)
  for (const k of ['attach', 'detach', '_schedule', '_clear', '_draw']) I.IsobarLayer.prototype[k] = function () { (this.calls = this.calls || []).push(k); };
  // the map's view: centred on the Great Lakes (the lake tests) unless a test sets w.view before the mount or moves it
  // (w.setView fires moveend: only an Overlay bound with the real _bindMap hears it)
  w.view = { lat: 44.8, lng: -84.2 };
  const map = { getPane: () => ({}), getZoom: () => 6, getSize: () => ({ x: 1200, y: 800 }), on: on('map'), off: off('map'), removeLayer() {},
    getPixelBounds: () => { const sc = Math.pow(2, map.getZoom()), p = I.worldXY(w.view.lng, w.view.lat), x = p[0] * sc - 600, y = p[1] * sc - 400; return { min: { x, y }, max: { x: x + 1200, y: y + 800 } }; },
    getContainer: () => ({ clientWidth: 1200, clientHeight: 800, classList: { add() {}, remove() {} }, style: { setProperty() {}, removeProperty() {} } }) };
  w.setView = (lat, lng) => { w.view = { lat, lng }; w.fire('map', 'moveend'); };
  w.I = I; w.map = map; w.L = g.L;
  // release one pending decode by kind (told apart by the frame's URL: the direction and the pressure frames are both half-size)
  const isLakeUrl = (u) => /\/glwu\//.test(u), isLakeDirUrl = (u) => isLakeUrl(u) && /\/pdir\//.test(u), isLakeFieldUrl = (u) => isLakeUrl(u) && !isLakeDirUrl(u);
  const isDirUrl = (u) => /\/(pdir|wdir)\//.test(u) && !isLakeUrl(u), isPresUrl = (u) => /\/pres\//.test(u), isFieldUrl = (u) => !isDirUrl(u) && !isPresUrl(u) && !isLakeUrl(u) && !/terrain_half/.test(u);
  const pick = (test, what) => { const i = w.pendingBitmaps.findIndex((f) => test(f.blob.url)); assert.ok(i >= 0, what + ' decode is pending'); return w.pendingBitmaps.splice(i, 1)[0]; };
  w.releaseDir = async () => { pick(isDirUrl, 'a direction')(); await settle(); };
  w.releaseField = async () => { pick(isFieldUrl, 'a field')(); await settle(); };
  w.releasePres = async () => { pick(isPresUrl, 'a pressure')(); await settle(); };
  w.holdDir = () => pick(isDirUrl, 'a direction');
  w.releaseTerrain = async () => { while (w.pendingTerrain.length) w.pendingTerrain.shift()(); await settle(); const i = w.pendingBitmaps.findIndex((f) => /terrain_half/.test(f.blob.url)); if (i >= 0) w.pendingBitmaps.splice(i, 1)[0](); await settle(); };
  w.holdPres = () => pick(isPresUrl, 'a pressure');
  w.releaseLake = async () => { pick(isLakeFieldUrl, 'a lake field')(); await settle(); };
  w.releaseLakeDir = async () => { pick(isLakeDirUrl, 'a lake direction')(); await settle(); };
  w.holdLake = () => pick(isLakeFieldUrl, 'a lake field');
  w.holdLakeDir = () => pick(isLakeDirUrl, 'a lake direction');
  w.lakePending = () => w.pendingBitmaps.filter((f) => isLakeUrl(f.blob.url)).length;
  w.releaseUrl = async (test) => { pick(test, 'the named')(); await settle(); };   // a decode by its URL: never an orphan of an aborted fetch
  // The scheduler tests keep their original baseline (animation and contours off unless a test turns them on); the
  // page's defaults (both on) are pinned by their own test.
  w.create = (extra) => { const o = g.AllshoreOverlay.create(map, Object.assign({ base: 'https://x/gfswave/0p25/v1', panel: {}, tz: 'UTC', getUnit: () => 'US', fmtTime: () => '', tzAbbr: () => '', pageCycle: () => null }, extra || {})); o.anim = false; o.contours = false; o.isobars = false; return o; };
  w.release = async (n) => { for (let i = 0; i < (n === undefined ? 1 : n) && w.pendingBitmaps.length; i++) w.pendingBitmaps.shift()(); await settle(); };
  w.releaseAll = async () => { while (w.pendingBitmaps.length) { w.pendingBitmaps.shift()(); await settle(); } };
  return w;
}
const A = manifest('2026092212', '2026-09-22T12:00:00Z', 12), B = manifest('2026092218', '2026-09-22T18:00:00Z', 18);

async function mounted(w, m, extra) {
  w.pointer = ptr(m); w.manifests[m.run] = m;
  const o = w.create(extra);
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

test('outage then Retry with the drawn frame still cached ends in the ready state, not "Loading" (G4 B1)', () => onRunDay(async () => {
  const w = world();
  const o = await mounted(w, A);
  await w.releaseAll();
  const i0 = o.frameIndex, key = (i) => `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i].step).padStart(3, '0')}.png`;
  for (const i of [i0 + 10, i0 + 11, i0 + 12]) w.failNext['https://x/' + key(i)] = 'network';   // beyond the ring: an outage
  o.seek(i0 + 10); await settle(); o.seek(i0 + 11); await settle();
  const nFetch = w.fetches.length; o.seek(i0 + 12); await settle();
  assert.equal(o.transientFails, 3);
  assert.equal(o.last && o.last.state, 'error');
  assert.equal(o.frameIndex, i0); assert.ok(o.layer.hasFrame(), 'a frame of the run is on the map: the outage keeps its picture (fix round 2 clears only a picture without its frame)');
  assert.equal(w.fetches.length, nFetch + 1, 'the outage pause starts nothing beyond the failed frame (section 42: a pause only shrinks the ring)');
  // the panel's Retry button does exactly this
  o.unavailable = {}; o.transientFails = 0; o.mount('hs'); await settle();
  assert.equal(o.last && o.last.state, 'ready', 'Retry with the cached frame must render ready');
  assert.equal(o.frameIndex, i0); assert.equal(o.target, i0);
  const ring = w.I.ringPlan(i0, A.frames.length, 1);                      // section 42: the ring refills on every landing, so it
  assert.ok(ring.every((i) => o.cache.has(o._key(i)) || o.inflight[o._key(i)]), 'the ring is complete or resumed');   // may be complete already
  o.unmount();
}));

test('unavailable frames: 404 is permanent and skipped, a network error is a cooldown, and the drawn frame never advances early', () => onRunDay(async () => {
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
}));

test('a frame whose download is done but whose decode waits behind a busy page is late, never "unavailable" (2026-10-12)', () => onRunDay(async () => {
  const w = world({ fakeTimers: true });
  const o = await mounted(w, A);
  await w.releaseAll();
  const i0 = o.frameIndex, url = 'https://x/' + `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i0 + 10].step).padStart(3, '0')}.png`;
  o.seek(i0 + 10); await settle();
  assert.equal(w.fetches.filter((u) => u === url).length, 1);
  await w.fade(20000); await settle();                         // the decode is held 20 s (the watchdog used to fire at 5 s)
  assert.equal(o._isUnavailable(i0 + 10), false, 'not marked');
  assert.equal(w.fetches.filter((u) => u === url).length, 1, 'not asked again');
  await w.releaseAll();
  assert.equal(o.frameIndex, i0 + 10, 'it lands');
  assert.equal(o.transientFails, 0);
  o.unmount();
}));

test('a download that stalls is asked again at once; only the third stall in a row makes it a cooldown; a superseded one is not retried', () => onRunDay(async () => {
  const w = world({ fakeTimers: true });
  const o = await mounted(w, A);
  await w.releaseAll();
  const i0 = o.frameIndex, u = (i) => 'https://x/' + `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i].step).padStart(3, '0')}.png`;
  const n = (i) => w.fetches.filter((x) => x === u(i)).length;
  w.hang[u(i0 + 10)] = 1;                                      // stalls once
  o.seek(i0 + 10); await settle();
  await w.fade(w.I.STALL_MS); await settle();
  assert.equal(n(i0 + 10), 2, 'asked again at the watchdog');
  assert.equal(o._isUnavailable(i0 + 10), false);
  await w.releaseAll();
  assert.equal(o.frameIndex, i0 + 10, 'the second answer lands');
  w.hang[u(i0 + 20)] = 3;                                      // stalls on every attempt
  o.seek(i0 + 20); await settle();
  for (let k = 0; k < w.I.LATE_TRIES; k++) { await w.fade(w.I.STALL_MS); await settle(); }
  assert.equal(n(i0 + 20), w.I.LATE_TRIES);
  assert.equal(typeof o.unavailable[o._key(i0 + 20)], 'number', 'a cooldown, not permanent');
  assert.equal(o.frameIndex, i0 + 10, 'the drawn frame stays');
  assert.equal(o.transientFails, 1, 'one failure, not three');
  w.hang[u(i0 + 30)] = 1;
  o.seek(i0 + 30); await settle();
  o.seek(i0 + 40); await settle();                             // the stalling download is dropped by the newer seek
  assert.equal(w.hung.filter((h) => h.url === u(i0 + 30))[0].signal.aborted, true, 'its request is cancelled at once');
  await w.fade(w.I.STALL_MS * 2); await settle();
  assert.equal(n(i0 + 30), 1, 'never asked again');
  assert.equal(o._isUnavailable(i0 + 30), false);
  o.unmount();
}));

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

test('lake outlines (plan section 51): under wind below zoom 4 nothing is asked; zooming in to 4 asks the set when the zoom ends; Off aborts its pending load', async () => {
  const w = world();
  w.coastAnswer = coastOk;
  const A3 = { ...A, fields: { hs: HS, wind: { lo: 0, hi: 41.2, legend: [0, 30.9], units: 'm/s', interpolation: 'bilinear' } } };
  w.pointer = ptr(A3); w.manifests[A3.run] = A3;
  w.map.getZoom = () => 3.2;
  const o = w.create({ coast: true });
  o.mount('wind'); w.orig._bindMap.call(o); await settle();
  const lakeAsks = () => w.fetches.filter((u) => u.indexOf('/static/coast/lakeoutlines-v1/') >= 0).length;
  assert.equal(lakeAsks(), 0, 'zoom 3: nothing asked');
  w.map.getZoom = () => 4.6; w.fire('map', 'zoomend'); await settle();
  assert.equal(lakeAsks(), 2, 'index.json and the overview, asked when the zoom ends');
  assert.equal(o.lakeOutCoast.status, 'loading');
  o.unmount();
  assert.equal(o.lakeOutCoast.status, 'idle', 'Off aborts the pending load');
  while (w.pendingCoast.length) w.pendingCoast.shift()();
  await settle();
});

test('lake outlines (V7-5): wind -> SST takes the set from the layer at once (its masks go, no zoom needed); back to wind gives it back', async () => {
  const w = world();
  const lakeIdx = { ...COAST_INDEX, tier0: { ...COAST_INDEX.tier0, file: 'lakeoutlines-i.bin' } };
  w.coastAnswer = (url) => (url.indexOf('/lakeoutlines-v1/') >= 0 && url.endsWith('index.json') ? { ok: true, status: 200, json: () => Promise.resolve(lakeIdx) } : coastOk(url));
  const A3 = { ...A, fields: { hs: HS, wind: { lo: 0, hi: 41.2, legend: [0, 30.9], units: 'm/s', interpolation: 'bilinear' } } };
  w.pointer = ptr(A3); w.manifests[A3.run] = A3;
  w.map.getZoom = () => 4.6;
  const o = w.create({ coast: true, sst: true });
  o.mount('wind'); w.orig._bindMap.call(o);
  for (let i = 0; i < 6; i++) { while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle(); }
  assert.ok(o.layer._lakeOut && o.layer._lakeOut === o.lakeOutCoast, 'wind at zoom 4.6: the set on the layer');
  sstFake(o); o.mount('sst'); await settle();
  assert.equal(o.layer._lakeOut, null, 'SST: the set taken from the layer at once');
  o.mount('wind');
  for (let i = 0; i < 4; i++) { while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle(); }
  assert.equal(o.layer._lakeOut, o.lakeOutCoast, 'back to wind: given back');
  o.unmount();
});

test('coast: the first draw waits for the coastlines, a failed coast load draws unclipped, Off aborts a pending load', async () => {
  const w = world();
  w.coastAnswer = coastOk;
  const A3 = { ...A, fields: { hs: HS, tp: { lo: 1, hi: 30, legend: [4, 22], units: 's', interpolation: 'nearest' }, wind: { lo: 0, hi: 41.2, legend: [0, 30.9], units: 'm/s', interpolation: 'bilinear' } } };
  w.pointer = ptr(A3); w.manifests[A3.run] = A3;
  const o = w.create({ coast: true });
  o.mount('hs'); await settle();
  assert.equal(o.coast.status, 'loading');
  assert.equal(w.fetches.filter((u) => u.indexOf('/static/coast/') >= 0).length, 2);        // index + tier 0, beside latest.json/manifest/frame
  assert.ok(w.fetches.some((u) => u.indexOf('/hs/') >= 0), 'the first frame is asked for beside the coastline (re-check R-1)');
  await w.release(1);                                                                          // the frame is decoded...
  assert.equal(o.layer._frame, null);                                                          // ...but not drawn: the coastlines are still loading
  while (w.pendingCoast.length) w.pendingCoast.shift()();
  for (let i = 0; i < 4 && !o.layer._frame; i++) await settle();
  assert.equal(o.coast.status, 'ok');
  assert.equal(o.layer._coast, o.coast);
  assert.ok(o.layer._frame && o.last.state === 'ready', JSON.stringify([!!o.layer._frame, o.last]));   // drawn at once: the frame came beside it
  assert.equal(o.layer._clip, true);
  o.mount('wind'); await settle(); await w.releaseAll(); await settle();                     // wind: never clipped, no coast wait (ring prefetches are released too)
  assert.equal(o.layer._clip, false); assert.ok(o.layer._frame, JSON.stringify(o.last));
  o.mount('tp'); await settle(); await w.releaseAll(); await settle();                       // back to a clipped field: the store is already there
  assert.equal(o.layer._clip, true);
  assert.equal(w.fetches.filter((u) => u.indexOf('/static/coast/v1/') >= 0).length, 2);     // never fetched again
  assert.deepEqual(w.fetches.filter((u) => u.indexOf('/static/coast/lakeoutlines-v1/') >= 0).map((u) => u.split('/').pop()).sort(),
    ['index.json', 'lakeoutlines-i.bin'], 'the lake-outline set: asked once, under wind (plan section 51)');
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

test('re-check R-1: the first picture waits for the coastline at most COAST_FIRST_MS, then is drawn unclipped and clipped the moment the coastline lands; settled() waits for both; the note', async () => {
  const w = world({ fakeTimers: true });
  w.coastAnswer = coastOk;
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create({ coast: true }), I = w.g.AllshoreOverlay._internals;
  o.coast.timeoutMs = 60000;                                   // a slow but moving coastline (the harness sends no byte: the no-progress limit stands aside)
  assert.equal(I.COAST_FIRST_MS, 15000);
  let settled = null; o.settled().then((s) => { settled = s; });
  o.mount('hs'); await settle();
  assert.equal(o.coast.status, 'loading');
  assert.ok(w.fetches.some((u) => u.indexOf('/hs/') >= 0), 'the frame is asked for at once, beside the coastline');
  await w.release(1);
  assert.equal(o.layer._frame, null, 'decoded but not drawn: the coastline is loading');
  await w.fade(I.COAST_FIRST_MS - 1); await settle();
  assert.equal(o.layer._frame, null, 'not before 15 s');
  await w.fade(1); for (let i = 0; i < 4 && !o.layer._frame; i++) await settle();
  assert.ok(o.layer._frame, 'drawn at 15 s'); assert.equal(o.layer._clip, false, 'unclipped'); assert.equal(o.last.state, 'ready');
  assert.equal(o.layer._coast, null);
  o._noteSettled('ready');                                     // (the harness stubs render: the call render('ready') makes)
  await settle(); assert.equal(settled, null, 'settled() also waits for the coastline (the names do not take its line)');
  assert.equal(I.coastNote(o.coast), 'Coastline still loading; the field is shown without coastline clipping until it arrives.');
  let renders = 0; const r0 = o.render.bind(o); o.render = (st) => { renders++; return r0(st); };
  while (w.pendingCoast.length) w.pendingCoast.shift()();
  for (let i = 0; i < 4 && !o.layer._clip; i++) await settle();
  assert.equal(o.coast.status, 'ok'); assert.equal(o.layer._coast, o.coast); assert.equal(o.layer._clip, true, 'clipped the moment it lands');
  for (let i = 0; i < 4 && settled === null; i++) await settle();
  assert.ok(renders >= 1, 'the panel follows (the note goes)'); assert.equal(settled, 'ready');
  // the coastline given up after the unclipped draw: the warning says so
  const w2 = world({ fakeTimers: true });
  w2.coastAnswer = () => ({ ok: false, status: 503 });
  w2.pointer = ptr(A); w2.manifests[A.run] = A;
  const o2 = w2.create({ coast: true }); let s2 = null; o2.settled().then((s) => { s2 = s; });
  o2.mount('hs'); await settle(); await w2.release(1);
  await w2.fade(I.COAST_FIRST_MS); for (let i = 0; i < 4 && !o2.layer._frame; i++) await settle();
  assert.ok(o2.layer._frame); assert.equal(o2.layer._clip, false);
  o2._noteSettled('ready');
  while (w2.pendingCoast.length) w2.pendingCoast.shift()();
  for (let i = 0; i < 4 && s2 === null; i++) await settle();
  assert.equal(o2.coast.status, 'failed'); assert.equal(o2.layer._clip, false); assert.equal(s2, 'ready');
  assert.equal(I.coastNote(o2.coast), 'Coastline data could not be loaded; the field is shown without coastline clipping.');
  // a coastline landing before 15 s: drawn clipped at once, no unclipped picture first
  const w3 = world({ fakeTimers: true });
  w3.coastAnswer = coastOk;
  w3.pointer = ptr(A); w3.manifests[A.run] = A;
  const o3 = w3.create({ coast: true }); o3.coast.timeoutMs = 60000;
  o3.mount('hs'); await settle(); await w3.release(1);
  await w3.fade(5000); await settle(); assert.equal(o3.layer._frame, null);
  while (w3.pendingCoast.length) w3.pendingCoast.shift()();
  for (let i = 0; i < 4 && !o3.layer._frame; i++) await settle();
  assert.ok(o3.layer._frame); assert.equal(o3.layer._clip, true, 'the first picture is the clipped one');
  // a field switch to wind during the wait: no wait, and the coastline joins as outlines when it lands
  const w4 = world({ fakeTimers: true });
  w4.coastAnswer = coastOk;
  w4.pointer = ptr(A); w4.manifests[A.run] = A;
  const o4 = w4.create({ coast: true }); o4.coast.timeoutMs = 60000;
  o4.mount('hs'); await settle();
  o4.mount('wind'); await settle(); await w4.releaseAll(); for (let i = 0; i < 4 && !o4.layer._frame; i++) await settle();
  assert.ok(o4.layer._frame, 'wind never waits'); assert.equal(o4.layer._clip, false);
  await w4.fade(I.COAST_FIRST_MS); await settle();
  assert.equal(o4.field, 'wind', 'the old wait drew nothing of hs');
  while (w4.pendingCoast.length) w4.pendingCoast.shift()();
  for (let i = 0; i < 4 && o4.layer._coast !== o4.coast; i++) await settle();
  assert.equal(o4.layer._coast, o4.coast, 'the coastline joins the wind (outlines)'); assert.equal(o4.layer._clip, false);
  o4.unmount(); o.unmount(); o2.unmount(); o3.unmount();
});

// a streamed answer read chunk by chunk at the test's command; like a real fetch, an abort errors the body
function stream(o, reads) {
  const aborted = (rej) => { if (o && o.signal) o.signal.addEventListener('abort', () => rej(Object.assign(new Error('aborted'), { name: 'AbortError' }))); };
  const never = () => new Promise((res, rej) => aborted(rej));
  return { ok: true, status: 200, headers: { get: () => null }, blob: never, arrayBuffer: never,
    body: { getReader: () => ({ read: () => new Promise((res, rej) => { reads.push(res); aborted(rej); }), cancel() {} }) } };
}
test('re-check R-1 / V2-1: a frame download (the first of a mount or a later one) gives up only after STALL_MS with no byte arriving; a slow but moving download goes on', async () => {
  const w = world({ fakeTimers: true });
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const reads = [];
  w.streams = (url, o) => (url.indexOf('/hs/') >= 0 ? stream(o, reads) : null);
  const o = w.create();
  o.mount('hs'); await settle();
  const first = w.fetches.filter((u) => u.indexOf('/hs/') >= 0);
  assert.equal(first.length, 1, 'one first frame asked');
  for (let t = 0; t < 4; t++) { await w.fade(4000); while (reads.length) reads.shift()({ done: false, value: new Uint8Array(10) }); await settle(); }
  assert.equal(w.fetches.filter((u) => u === first[0]).length, 1, '16 s of a slow but moving download: never asked again');
  await w.fade(5000); await settle();
  assert.equal(w.fetches.filter((u) => u === first[0]).length, 2, 'no byte for STALL_MS: asked again');
  o.unmount();
  // a later frame (a picture on the map): the whole download within the limit, as before (section 41's late frames)
  const w2 = world({ fakeTimers: true });
  w2.pointer = ptr(A); w2.manifests[A.run] = A;
  const o2 = w2.create();
  o2.mount('hs'); await settle(); await w2.release(1); await settle();
  assert.ok(o2.layer._frame, 'the first picture');
  const reads2 = [];
  w2.streams = (url, o) => (url.indexOf('/hs/') >= 0 ? stream(o, reads2) : null);
  const far = (o2.frameIndex + Math.floor(o2.n / 2)) % o2.n, before = w2.fetches.length;   // a frame outside the (looping) ring
  o2.seek(far); await settle();
  const u = w2.fetches.slice(before).filter((x) => x.indexOf('/hs/') >= 0)[0];
  assert.ok(u, 'the far frame asked for');
  const n0 = w2.fetches.filter((x) => x === u).length;
  for (let t = 0; t < 4; t++) { await w2.fade(4000); while (reads2.length) reads2.shift()({ done: false, value: new Uint8Array(10) }); await settle(); }
  assert.equal(w2.fetches.filter((x) => x === u).length, n0, '16 s of a slow but moving later download: never asked again (V2-1; section 41 cut it at 5 s in all)');
  await w2.fade(5000); await settle();
  assert.equal(w2.fetches.filter((x) => x === u).length, n0 + 1, 'no byte for STALL_MS: asked again (a hung download still is)');
  o2.unmount();
});

test('V2-1: with the coastline still loading after the unclipped first picture, ring frames sharing the slow link and still receiving bytes are asked once, never marked unavailable', async () => {
  const w = world({ fakeTimers: true });
  w.coastAnswer = coastOk;
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create({ coast: true });
  o.coast.timeoutMs = 600000;                                    // the fake coast sends nothing: its own stall must not fire
  const I = w.g.AllshoreOverlay._internals;
  o.mount('hs'); await settle();
  const first = w.fetches.filter((u) => u.indexOf('/hs/') >= 0);
  assert.equal(first.length, 1, 'the first frame asked beside the coastline');
  const reads = [];
  w.streams = (url, oo) => (url.indexOf('/hs/') >= 0 && url !== first[0] ? stream(oo, reads) : null);
  await w.release(1);
  await w.fade(I.COAST_FIRST_MS); for (let i = 0; i < 4 && !o.layer._frame; i++) await settle();
  assert.ok(o.layer._frame, 'the unclipped first picture'); assert.equal(o.coast.status, 'loading');
  const ring = [...new Set(w.fetches.filter((u) => u.indexOf('/hs/') >= 0 && u !== first[0]))];
  assert.ok(ring.length >= 1, 'the ring asked beside the loading coastline');
  for (let t = 0; t < 16; t++) { await w.fade(1000); while (reads.length) reads.shift()({ done: false, value: new Uint8Array(10) }); await settle(); }
  assert.deepEqual(ring.map((u) => w.fetches.filter((x) => x === u).length), ring.map(() => 1), 'each asked once (2.22.3: three times, then a cooldown)');
  assert.equal(Object.keys(o.unavailable).length, 0); assert.equal(o.transientFails, 0);
  o.unmount();
});

test("V2-1: the shown step's direction frame still arriving after 5 s is kept (its particles come), and a hung one is still asked again", async () => {
  const w = world({ fakeTimers: true }); w.pointer = ptr(A); w.manifests[A.run] = A;
  const reads = [];
  w.streams = (url, oo) => (/\/pdir\//.test(url) ? stream(oo, reads) : null);
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseField();
  const dirs = () => w.fetches.filter((u) => /\/pdir\//.test(u));
  const shownDir = dirs()[0];
  assert.ok(shownDir, 'the shown step direction asked');
  for (let t = 0; t < 12; t++) { await w.fade(1000); while (reads.length) reads.shift()({ done: false, value: new Uint8Array(10) }); await settle(); }
  assert.equal(w.fetches.filter((u) => u === shownDir).length, 1, '12 s of a moving direction download: asked once');
  assert.ok(!Object.keys(o.unavailable).length, 'nothing marked unavailable');
  o.unmount();
  // a direction download that sends nothing: asked again after STALL_MS, three tries, then the cooldown (unchanged)
  const w2 = world({ fakeTimers: true }); w2.pointer = ptr(A); w2.manifests[A.run] = A;
  w2.streams = (url, oo) => (/\/pdir\//.test(url) ? stream(oo, []) : null);
  const o2 = w2.create(); o2.anim = true; o2.mount('hs'); await settle(); await w2.releaseField();
  const d2 = w2.fetches.filter((u) => /\/pdir\//.test(u))[0];
  for (let t = 0; t < 4; t++) { await w2.fade(w2.I.STALL_MS); await settle(); }
  assert.equal(w2.fetches.filter((u) => u === d2).length, w2.I.LATE_TRIES, 'a hung download: LATE_TRIES attempts');
  assert.ok(Object.keys(o2.unavailable).some((k) => /pdir/.test(k)), 'then the cooldown');
  o2.unmount();
});

test('re-check R-4: a coastline load whose index fails stops its tier-0 download at once (Off could not stop it any more)', async () => {
  const w = world();
  const idxUrl = 'https://x/static/coast/v1/index.json', binUrl = 'https://x/static/coast/v1/world-i.bin';
  w.failNext[idxUrl] = 503; w.hang[binUrl] = 1;
  const o = w.create({ coast: true });
  const p = o.coast.load(); const st = await p;
  assert.equal(st, null); assert.equal(o.coast.status, 'failed');
  assert.equal(w.hung.length, 1); assert.equal(w.hung[0].url, binUrl);
  assert.equal(w.hung[0].signal.aborted, true, 'the tier-0 download is stopped with the failed load');
});

test('coast: a download that hangs past the watchdog fails the coast load; the frame draws unclipped with the store failed', async () => {
  const w = world();
  w.coastAnswer = coastOk;
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create({ coast: true });
  o.coast.timeoutMs = 150;                                                                     // the harness never answers coast fetches by itself (150 ms: the first check must run before it under load)
  o.mount('hs'); await settle();
  assert.equal(o.coast.status, 'loading');
  await new Promise((r) => setTimeout(r, 400));
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

test('contours and the animation are always on (no checkboxes; a tab\'s old "off" saves are ignored and nothing is saved); the contour interval follows the site unit on the first frame', async () => {
  let unit = 'US';
  const storage = memStore({ contours: false, anim: false, opacity: 0.3, opacityWind: 0.4 }), w = world({ storage });
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = new w.I.Overlay(w.map, { base: 'https://x/gfswave/0p25/v1', panel: {}, tz: 'UTC', getUnit: () => unit, fmtTime: () => '', tzAbbr: () => '', pageCycle: () => null });   // the page's defaults
  assert.equal(o.contours, true); assert.equal(o.anim, true);
  assert.deepEqual([o._opacityFor('hs'), o._opacityFor('tp'), o._opacityFor('wind')], [0.65, 0.65, 1], 'fixed opacity; old slider saves ignored');
  assert.equal(o.setOpacity, undefined, 'no opacity setter');
  o.mount('hs'); await settle(); await w.releaseAll();
  assert.deepEqual(o.layer._contour, { step: 2, per: 3.28084, off: 0 }, 'the first frame is drawn with them');
  assert.ok(o._wantDir(), 'the direction (particles) is wanted from the start');
  unit = 'Metric'; o.refresh();
  assert.deepEqual(o.layer._contour, { step: 0.5, per: 1, off: 0 });
  o.setContours(true); o.setAnim(true); o.setContours(false); o.setAnim(false); o.setContours(true); o.setAnim(true);
  const st = storage.read(); assert.equal(st.contours, false); assert.equal(st.anim, false, 'the setters write nothing (the old values stay as they were)');
  done(o);
});

test('a corrupted session value counts as empty and the next write replaces it (G8 A-P3-3)', () => {
  for (const raw of ['5', '"hs"', '[1,2]', 'null', 'garbage', '{"field":"hs"}']) {
    const m = new Map([['allshore.overlay.v1', raw]]);
    const storage = { getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => { m.set(k, String(v)); } };
    const o = world({ storage }).create();
    o.field = 'tp'; o.manifest = A; o.frameIndex = 3; o._persist();
    const back = JSON.parse(m.get('allshore.overlay.v1'));
    assert.ok(back && typeof back === 'object' && !Array.isArray(back) && back.field === 'tp' && back.t === Date.parse(A.frames[3].valid_utc), raw + ' -> ' + m.get('allshore.overlay.v1'));
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
  assert.ok(o.flow, 'animator attached'); assert.equal(o.animAvailable(), true); assert.equal(o.dres, 'half', 'the default zoom 6: half-resolution direction');
  const keys = Object.keys(o.inflight);
  assert.deepEqual(keys.length, 1, keys.join()); assert.ok(keys[0].includes('/full/hs/'), 'the first picture is fetched alone (G15 P2-1)');
  await w.releaseField();                                                    // drawn at once, without waiting for its direction
  assert.ok(o.layer.hasFrame()); assert.equal(o.flow.dir, null);
  assert.ok(Object.keys(o.inflight).some((k) => k.includes('/half/pdir/')), 'then its direction');
  await w.releaseDir();                                                      // the direction joins the frame on the map
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

test('a direction frame that lands after the map moved on is never shown under the new step (a newer target drops it)', () => onRunDay(async () => {
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
}));

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
  assert.equal(o.dres, 'half');
  await w.releaseAll(); assert.ok(o.flow.dir); assert.ok(w.fetches.some((u) => u.includes('/half/wdir/')));
  o.mount('hs'); await settle();                                             // a field change at zoom 9: full-resolution pdir
  assert.equal(o.dres, 'full'); assert.equal(o.flow.dir, null, 'the wind direction is not shown under wave height');
  await w.releaseAll();
  const fetches = w.fetches.filter((u) => u.includes('/pdir/'));
  assert.ok(fetches.length && fetches.every((u) => !u.includes('/half/')), 'full-resolution pdir at zoom 9');
  assert.ok(o.flow.dir && o.flow.dir.cols === 1440, 'full pdir delivered'); const shown = o.flow.dir;
  w.map.getZoom = () => 5.8; o._checkRes(); await settle();                  // zoom out below 6: half-resolution pdir, the shown one stays meanwhile
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
  for (const k of o.dcache.map.keys()) assert.ok(k.startsWith('gfswave/' + B.run + '/'), 'keys carry the model and the run');
  clearInterval(o.runTimer); o.unmount();
});

// ---- G10-A: a pending seek / step survives every new caller of _prefetch / _startDir; delivery follows the layer ----

test('G10-A S1: a far seek is loading, then the direction resolution changes (zoom 6 -> 7.6): the seek still lands', () => onRunDay(async () => {
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
}));

test('G10-A S2 / S5: Animation ticked while a step is loading (outside the ring, and with _startDir eviction): the step still lands', () => onRunDay(async () => {
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
}));

test('G10-A: unticking Animation while a step is loading keeps the step; the shown step gets no direction fetch while another target is pending', () => onRunDay(async () => {
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
}));

test('G10-A M26 / G10-C P2-1: a step shows its picture and its direction together; the old direction never sits under the new step', () => onRunDay(async () => {
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
}));

test('G10-A M11/M12: _trimInflight keeps no survivor beside a target whose direction is in flight, exactly one otherwise', () => onRunDay(async () => {
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
  assert.ok(o.inflight[o._key(i + 2)], 'the nearest survives, not the oldest (section 42)');
  o.inflight = {}; o.unmount();
}));

test('G10-A M60 / P3-5 / P3-4: a cached direction is not fetched again; untick before the first frame aborts the direction fetch; direction successes do not mask field failures', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle();
  assert.ok(!Object.keys(o.inflight).some(isDir), 'no direction before the first picture (G15 P2-1)');
  o.setAnim(false); await settle();                                           // before the first frame landed
  assert.ok(!Object.keys(o.inflight).some(isDir), 'direction fetch aborted before the first landing'); assert.equal(o.dcache.size(), 0);
  o.setAnim(true); await settle(); await w.releaseAll();
  const before = w.fetches.length; const key = o._key(o.frameIndex, 'dir'); assert.ok(o.dcache.has(key));
  o._startDir(o.frameIndex); o._syncFlow(o.frameIndex); await settle();
  assert.equal(w.fetches.length, before, 'a cached direction is never downloaded again');
  // three field failures with direction successes in between still trip the outage
  const i0 = o.frameIndex, fkey = (i) => `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i].step).padStart(3, '0')}.png`;
  // (section 42: a landed direction frame refills the ring around the target, and a ring frame that lands resets the
  // counter, as any consecutive success does; so, as in a real outage, the ring frames around each target fail too)
  for (const tg of [i0 + 10, i0 + 20, i0 + 30]) for (const i of w.I.ringPlan(tg, A.frames.length, 1)) w.failNext['https://x/' + fkey(i)] = 'network';
  const before2 = w.fetches.length;
  o.seek(i0 + 10); await settle(); await w.releaseAll(); o.seek(i0 + 20); await settle(); await w.releaseAll(); o.seek(i0 + 30); await settle(); await w.releaseAll();
  assert.ok(o.transientFails >= 3, 'the counter follows the field frames only (' + o.transientFails + ')'); assert.equal(o.last && o.last.state, 'error');
  assert.ok(w.fetches.length - before2 <= 40, 'no endless refetch around a target in a cooldown (' + (w.fetches.length - before2) + ' fetches)');
  o.unmount();
}));

test('G10-A D1 / M52: on a field switch a direction that lands before the field frame is not delivered; the animator is told the new field', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  w.map.getZoom = () => 7;                                                    // the wind field full (1440), its direction half (720): told apart by size
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  assert.ok(o.flow.dir); assert.equal(o.flow.field, 'hs');
  o.mount('wind'); await settle();
  assert.equal(o.flow.field, 'wind'); assert.equal(o.flow.dir, null); assert.equal(o.layer.hasFrame(), false);
  assert.ok(!Object.keys(o.inflight).some(isDir), 'the first picture of the new field is fetched alone (G15 P2-1)');
  o.flow.resume(true);                                                        // a tab show in that window
  await w.releaseField();
  assert.ok(o.layer.hasFrame()); assert.equal(o.flow.dir, null, 'no hs direction under the wind');
  await w.releaseDir();
  assert.ok(o.flow.dir && o.flow.dir.cols === 720, 'the wind direction joins'); assert.equal(o.flow.entry, A.frames[o.frameIndex]);
  o.unmount();
});

test('G10-A P3-8: a phone zooming from 5.8 to 7.6 changes both resolutions; the same step direction stays until the new one lands', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  w.I.Overlay.prototype._dims = () => ({ w: 400, h: 700 }); w.map.getZoom = () => 5.8;
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

test('G11 P1-1: under reduced motion nothing animates and no direction frame is fetched (the animation is otherwise always on)', async () => {
  const w = world({ reduced: true }); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(o.animAvailable(), true, 'the run has direction data'); assert.equal(o._wantDir(), false); assert.equal(o.flow, null);
  assert.ok(!w.fetches.some(isDir), 'no direction fetch'); assert.equal(o.dcache.size(), 0);
  o.step(1); await settle(); await w.releaseAll(); assert.ok(!w.fetches.some(isDir));
  o.unmount();
});

test('G12: a phone zoomed in to 6.6 keeps the half-resolution field and fetches the full-resolution direction', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  w.I.Overlay.prototype._dims = () => ({ w: 400, h: 700 }); w.map.getZoom = () => 6.6;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle();
  assert.equal(o.res, 'half'); assert.equal(o.dres, 'full');
  assert.ok(Object.keys(o.inflight).some((k) => k.includes('/half/hs/')));
  await w.releaseAll(); assert.ok(w.fetches.some((u) => /\/\d{10}\/pdir\//.test(u)), 'the full-resolution pdir (no res prefix in its URL)');
  assert.ok(o.flow.dir && o.flow.dir.cols === 1440, 'the full-resolution direction on the map'); assert.equal(o.layer._frame.cols, 720);
  o.unmount();
});


// ---- plan section 22: the full model horizon (hourly to +120 h, 3-hourly to +384 h, 209 frames) with the LIVE client ----

test('section 22: the live client plays a 209-frame run: every frame, seek to +384 h, a loop wraps, in-flight <= 2', async () => {
  assert.equal(FULL_HORIZON.length, 209); assert.equal(FULL_HORIZON[120], 120); assert.equal(FULL_HORIZON[121], 123); assert.equal(FULL_HORIZON[208], 384);
  const w = world(); const F = manifest('2026092606', '2026-09-26T06:00:00Z', 6, false, FULL_HORIZON);
  assert.doesNotThrow(() => w.I.validateManifest(F));
  w.pointer = ptr(F); w.manifests[F.run] = F;
  const o = w.create(); o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(o.n, 209); assert.equal(o.last.state, 'ready');
  o.seek(208); await settle(); await w.releaseAll();
  assert.equal(o.frameIndex, 208); assert.equal(o._hours(o.layer.entry), 384, 'the last frame is +384 h');
  o.seek(119); await settle(); await w.releaseAll(); o.step(1); await settle(); await w.releaseAll();
  assert.equal(o._hours(o.layer.entry), 120, 'hourly to +120 h'); o.step(1); await settle(); await w.releaseAll();
  assert.equal(o._hours(o.layer.entry), 123, 'then 3-hourly');
  o.seek(206); await settle(); await w.releaseAll();
  o.setSpeed(4); o.play();
  let wrapped = false;
  for (let k = 0; k < 12 && !wrapped; k++) {
    await new Promise((r) => setTimeout(r, 140)); await w.releaseAll(); await settle();
    assert.ok(Object.keys(o.inflight).length <= o._maxInflight(), 'in flight ' + Object.keys(o.inflight).length);   // 4 at shark speed (section 42)
    if (o.frameIndex !== null && o.frameIndex < 5) wrapped = true;
  }
  o.pause();
  assert.ok(wrapped, 'the loop wrapped from +384 h to the start');
  o.unmount();
});

test('section 22: Update between an 81-frame run and a 209-frame run keeps the valid time (and clamps to +240 h the other way)', async () => {
  const w = world();
  const OLD = manifest('2026092600', '2026-09-26T00:00:00Z', 0);                               // 81 frames to +240 h
  const NEW = manifest('2026092606', '2026-09-26T06:00:00Z', 6, false, FULL_HORIZON);          // 209 frames to +384 h
  w.manifests[OLD.run] = OLD; w.manifests[NEW.run] = NEW; w.pointer = ptr(OLD);
  const o = w.create(); o.mount('hs'); await settle(); await w.releaseAll();
  o.seek(40); await settle(); await w.releaseAll();                                              // +120 h of OLD = 2026-10-01T00Z
  const t = Date.parse(o.layer.entry.valid_utc);
  w.pointer = ptr(NEW); o.newerRun = w.pointer; o.update(); await settle(); await w.releaseAll();
  assert.equal(o.manifest.run, NEW.run); assert.equal(Date.parse(o.layer.entry.valid_utc), t, 'the same valid time on the hourly run');
  assert.equal(o._hours(o.layer.entry), 114);
  o.seek(o.n - 30); await settle(); await w.releaseAll();                                       // +297 h of NEW, beyond OLD's +240 h
  const later = manifest('2026092612', '2026-09-26T12:00:00Z', 12);                             // an 81-frame run again (a rollback)
  w.manifests[later.run] = later; w.pointer = ptr(later); o.newerRun = w.pointer; o.update(); await settle(); await w.releaseAll();
  assert.equal(o.manifest.run, later.run); assert.equal(o.frameIndex, 80, 'the nearest valid time is its last frame (+240 h)');
  clearInterval(o.runTimer); o.unmount();
});

test('section 22: a reload restores a saved time past +120 h on a 209-frame run (and an hourly one before it)', () => {
  const w = world(); const F = manifest('2026092606', '2026-09-26T06:00:00Z', 6, false, FULL_HORIZON), now = Date.parse('2026-09-26T12:00:00Z');
  const at = (h) => Date.parse(F.run_utc) + h * 3.6e6;
  assert.equal(w.I.restoreIndex(F, { t: at(300), playing: false, at: now - 60000 }, now), FULL_HORIZON.indexOf(300));
  assert.equal(w.I.restoreIndex(F, { t: at(301), playing: false, at: now - 60000 }, now), FULL_HORIZON.indexOf(300), 'within 90 min: the nearest frame');
  assert.equal(w.I.restoreIndex(F, { t: at(37), playing: false, at: now - 60000 }, now), 37, 'hourly frames before +120 h');
});

// ---- plan section 23: the basemap follows what is drawn; per-field opacity ----
function lookMap() {
  const on = new Set(), pane = { style: {} };
  return { layers: on, pane, hasLayer: (l) => on.has(l), removeLayer(l) { on.delete(l); }, getPane: (n) => (n === 'modelPane' ? pane : undefined) };
}
function fakeTile(url, options) {
  return { url, options: options || {}, ev: {}, ons: {},
    once(e, f) { (this.ev[e] = this.ev[e] || []).push(f); return this; }, on(e, f) { (this.ons[e] = this.ons[e] || []).push(f); return this; },
    addTo(m) { m.layers.add(this); return this; },
    fire(e) { const fs = this.ev[e] || []; this.ev[e] = []; (this.ons[e] || []).forEach((f) => f()); fs.forEach((f) => f()); } };
}
function drawn(field, frame) { return { field, op: null, options: {}, frame: frame === undefined ? !!field : frame, hasFrame() { return this.frame; }, setOpacity(v) { this.op = v; this.options.opacity = v; } }; }

test('section 39 step 5b: the relief is made by the page\'s basemap layer (static_ui/basemap.js) when it is there, so a relief tile Esri does not have is drawn from a coarser one; else by Leaflet', () => {
  const w = world(), map = lookMap(), img = fakeTile('World_Imagery', { maxZoom: 18, minZoom: 1, keepBuffer: 2 });
  const made = [];
  w.L.tileLayer = (url, o) => { made.push(['leaflet', url]); return fakeTile(url, o); };
  w.g.AllshoreBasemap = { tileLayer: (url, o) => { made.push(['basemap', url, o.maxNativeZoom, o.maxZoom]); return fakeTile(url, o); } };
  img.addTo(map);
  const o = new w.I.Overlay(map, { base: 'https://x/gfswave/0p25/v1', baseLayer: img });
  o.layer = drawn('wind'); o._syncLook();
  assert.deepEqual(made, [['basemap', w.I.WIND_LOOK.base, 16, 18]]);
  const w2 = world(), map2 = lookMap(), img2 = fakeTile('World_Imagery', { maxZoom: 18 }), made2 = [];
  w2.L.tileLayer = (url, o) => { made2.push(url); return fakeTile(url, o); };
  img2.addTo(map2);
  const o2 = new w2.I.Overlay(map2, { base: 'https://x/gfswave/0p25/v1', baseLayer: img2 });
  o2.layer = drawn('wind'); o2._syncLook();
  assert.deepEqual(made2, [w2.I.WIND_LOOK.base], 'no basemap module: Leaflet\'s own layer');
});

test('wind look: the relief goes on top and the imagery leaves once the relief has loaded AND faded in; any other field brings the imagery back; quick switches never leave the map without a basemap', async () => {
  const w = world({ fakeTimers: true }), map = lookMap(), img = fakeTile('World_Imagery', { maxZoom: 11, minZoom: 1, keepBuffer: 2 });
  w.L.tileLayer = (url, o) => fakeTile(url, o);
  img.addTo(map);
  const o = new w.I.Overlay(map, { base: 'https://x/gfswave/0p25/v1', baseLayer: img });
  const both = () => [map.hasLayer(img), !!o.relief && map.hasLayer(o.relief)];
  o.layer = drawn('hs'); o._syncLook();
  assert.deepEqual(both(), [true, false]); assert.equal(map.pane.style.mixBlendMode, ''); assert.equal(o.layer.op, 0.65);
  o.layer = drawn('wind', false); o._syncLook();                             // wind picked but no frame drawn yet: nothing changes
  assert.deepEqual(both(), [true, false]); assert.equal(o.relief, null);
  o.layer = drawn('wind'); o._syncLook();
  const relief = o.relief;
  assert.equal(relief.url, w.I.WIND_LOOK.base); assert.equal(relief.options.maxZoom, 11); assert.equal(relief.options.attribution, w.I.WIND_LOOK.attribution);
  assert.equal(relief.options.maxNativeZoom, 16, 'Esri\'s relief ends at zoom 16: scaled up past it (the map zooms to 18 since section 39 step 5b)');
  assert.ok(!('noWrap' in relief.options), 'options the page did not set are left to Leaflet');
  assert.deepEqual(both(), [true, true], 'the imagery stays until the relief is in');
  assert.equal(map.pane.style.mixBlendMode, '', 'no blend onto the imagery'); assert.equal(o.layer.op, 1);
  relief.fire('tileload'); relief.fire('load');
  assert.deepEqual(both(), [true, true], 'the relief tiles are still fading in: the imagery stays');
  await w.fade();
  assert.deepEqual(both(), [false, true]); assert.equal(map.pane.style.mixBlendMode, 'multiply');
  o.layer.op = null; o._syncLook(); assert.equal(o.layer.op, null, 'opacity set only on a change (every landing calls this)');
  o.layer = drawn('tp'); o._syncLook();                                      // back to a wave field: the imagery on top, loading
  assert.deepEqual(both(), [true, true]); assert.equal(map.pane.style.mixBlendMode, ''); assert.equal(o.layer.op, 0.65);
  o.layer = drawn('wind'); o._syncLook();                                    // wind again before the imagery loaded: the relief is still there
  assert.deepEqual(both(), [false, true]); assert.equal(map.pane.style.mixBlendMode, 'multiply');
  img.fire('load'); await w.fade();                                            // the stale landing changes nothing
  assert.deepEqual(both(), [false, true]);
  o.layer = drawn(null); o._syncLook();                                      // a field loading (nothing drawn): the imagery
  assert.deepEqual(both(), [true, true]); img.fire('load'); assert.deepEqual(both(), [true, true]); await w.fade();
  assert.deepEqual(both(), [true, false]);
  o.layer = drawn('wind'); o._syncLook(); assert.equal(o.relief, relief, 'one relief layer for the session'); relief.fire('tileload'); relief.fire('load');
  o.layer = drawn('hs'); o._syncLook();                                      // switched away inside the fade: the pending removal is dropped
  assert.deepEqual(both(), [true, false], 'the imagery was still there: the relief leaves at once'); await w.fade(); assert.deepEqual(both(), [true, false], 'the pending removal is dropped');
  o.layer = drawn('wind'); o._syncLook(); o.layer = drawn('hs'); o._syncLook();   // away before the relief loaded...
  relief.fire('load'); await w.fade();                                         // ...its late 'load' (no tile yet) is not a failure
  assert.equal(o._reliefFailed, 0); assert.deepEqual(both(), [true, false]);
  o.layer = drawn('wind'); o._syncLook(); relief.fire('tileload'); relief.fire('load'); await w.fade();
  o.layer = null; o._syncLook();                                             // Off / unmount
  assert.deepEqual(both(), [true, true]); assert.equal(map.pane.style.mixBlendMode, ''); img.fire('load'); await w.fade();
  assert.deepEqual(both(), [true, false]);
});

test('wind look: a relief that loads no tile is given up for a cooldown (imagery kept); no imagery handed over or not on the map -> the basemap is left alone', async () => {
  const w = world({ fakeTimers: true }); w.L.tileLayer = (url, o) => fakeTile(url, o);
  let map = lookMap(), img = fakeTile('World_Imagery'); img.addTo(map);
  let o = new w.I.Overlay(map, { base: 'https://x/gfswave/0p25/v1', baseLayer: img });
  o.layer = drawn('wind'); o._syncLook();
  o.relief.fire('tileerror'); o.relief.fire('load');
  assert.ok(o._reliefFailed > 0);
  assert.deepEqual([map.hasLayer(img), map.hasLayer(o.relief)], [true, false]); assert.equal(map.pane.style.mixBlendMode, '');
  o._syncLook(); assert.deepEqual([map.hasLayer(img), map.hasLayer(o.relief)], [true, false], 'not retried during the cooldown');
  o._reliefFailed = Date.now() - w.I.RELIEF_RETRY_MS - 1; o._base = 'imagery'; o._syncLook();
  assert.equal(map.hasLayer(o.relief), true, 'retried after the cooldown'); o.relief.fire('tileload'); o.relief.fire('load'); await w.fade();
  assert.deepEqual([map.hasLayer(img), map.hasLayer(o.relief)], [false, true]);
  map = lookMap();
  o = new w.I.Overlay(map, { base: 'https://x/gfswave/0p25/v1' });
  o.layer = drawn('wind'); o._syncLook();
  assert.equal(map.layers.size, 0); assert.equal(o.relief, null); assert.equal(map.pane.style.mixBlendMode, ''); assert.equal(o.layer.op, 1);
  map = lookMap(); img = fakeTile('World_Imagery');                           // handed over but not on the map
  o = new w.I.Overlay(map, { base: 'https://x/gfswave/0p25/v1', baseLayer: img });
  o.layer = drawn('wind'); o._syncLook();
  assert.equal(map.layers.size, 0); assert.equal(map.pane.style.mixBlendMode, '');
});

test('fixed opacity per field: the layer takes 0.65 for wave height / period and 1 for wind at each field change', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const seen = [];
  const o = w.create(); o.mount('hs'); await settle(); await w.release(1);
  assert.equal(o.layer.options.opacity, 0.65);
  o.layer.setOpacity = (v) => { seen.push(v); o.layer.options.opacity = v; };
  o.mount('wind'); await settle(); await w.releaseAll();
  o.mount('tp'); await settle(); await w.releaseAll();
  assert.deepEqual(seen, [1, 0.65], 'switched once per field change');
  done(o);
});

test('wind look through mount/unmount: the relief after the wind lands, the imagery back at a field change (before its frame) and at Off; the wind coastlines join without holding the frame and survive an Update', async () => {
  const w = world({ fakeTimers: true }); w.coastAnswer = coastOk; w.L.tileLayer = (url, o) => fakeTile(url, o);
  const map = w.map, on = new Set(), pane = { style: {} };
  Object.assign(map, { hasLayer: (l) => on.has(l), removeLayer(l) { on.delete(l); }, getPane: () => pane, layers: on });
  const img = fakeTile('World_Imagery'); img.addTo(map);
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create({ coast: true, baseLayer: img });
  o.mount('wind'); await settle();
  for (let i = 0; i < 4 && !w.pendingBitmaps.length; i++) await settle();
  await w.release(1);                                                         // the wind frame lands while the coast is still loading
  assert.ok(o.layer.hasFrame(), 'wind does not wait for the coastlines'); assert.equal(o.layer._coast, null);
  assert.ok(o.relief && on.has(o.relief)); o.relief.fire('tileload'); o.relief.fire('load'); await w.fade();
  assert.deepEqual([on.has(img), on.has(o.relief), pane.style.mixBlendMode], [false, true, 'multiply']);
  o.newerRun = null; o.abortAll();                                            // what an Update does to the mount's signal
  while (w.pendingCoast.length) w.pendingCoast.shift()();
  await settle();
  assert.equal(o.layer._coast, o.coast, 'the coastlines joined'); assert.equal(o.layer._masked(), true); assert.equal(o.layer._clip, false);
  assert.ok(/GSHHG/.test(o._attribution()), 'the GSHHG credit for wind');
  o.layer._tiles = { a: { el: {}, coords: { z: 8, x: 15, y: 112 } } };
  o.coast.index = o.coast.index || { tier1: { cell: 5, cells: {} } };
  const tz = o.coast.tier1Zoom; o.coast.tier1Zoom = (z) => z >= 7;
  assert.ok(Object.keys(o.layer._cellsNeeded()).length > 0, 'wind asks for the tier-1 cells of its tiles');
  o.coast.tier1Zoom = tz; o.layer._tiles = {};
  o.mount('hs'); await settle();                                              // a field change: nothing drawn -> the imagery at once
  assert.deepEqual([on.has(img), pane.style.mixBlendMode], [true, '']);
  await w.releaseAll();
  o.mount('wind'); await settle(); await w.releaseAll(); await w.fade();
  assert.deepEqual([on.has(img), on.has(o.relief), pane.style.mixBlendMode], [false, true, 'multiply'], 'the relief again (still loaded beneath)');
  o.unmount();
  assert.deepEqual([on.has(img), pane.style.mixBlendMode], [true, '']);
  img.fire('load'); await w.fade();
  assert.deepEqual([on.has(img), on.has(o.relief)], [true, false]);
  o.mount('wind'); await settle(); await w.releaseAll();                      // a new layer with the store already loaded: it keeps it
  assert.ok(o.layer.hasFrame()); assert.equal(o.layer._coast, o.coast);
  done(o);
});

test('plan section 37: a saved 0.5x (no longer offered) plays at 1x; setSpeed takes only snail / fish / shark speeds', async () => {
  const { w, o, storage } = await restored({}, { speed: 0.5 });
  assert.equal(o.speed, 1);
  o.setSpeed(0.5); assert.equal(o.speed, 1); assert.equal(storage.read().speed, 0.5, 'nothing written for a refused speed');
  o.setSpeed(2); assert.equal(o.speed, 2); assert.equal(storage.read().speed, 2); assert.equal(o._interval(), 250);
  o.setSpeed(4); assert.equal(o._interval(), 125); o.setSpeed(1); assert.equal(o._interval(), 500);
  done(o);
  const r = await restored({}, { speed: 4 }); assert.equal(r.o.speed, 4); done(r.o);
  void w;
});

// ---- plan section 37: the compass ribbon's scrub path (drag / tap / wheel / keys all go through _scrubTo) ----
test('section 37: a scrub across frames pauses, keeps at most MAX_INFLIGHT fetches and aborts the rest; a repeated index is free', () => onRunDay(async () => {
  const w = world();
  const o = await mounted(w, A);
  o.play(); assert.equal(o.playing, true);
  const before = w.fetches.length;
  for (const idx of [5, 6, 7, 8, 9, 10]) { o._scrubTo(idx); await tick(); assert.ok(Object.keys(o.inflight).length <= w.I.MAX_INFLIGHT, 'inflight ' + Object.keys(o.inflight).length); }
  assert.equal(o.playing, false, 'the first scrub paused playback'); assert.equal(o.target, 10);
  assert.ok(o.inflight[o._key(10)], 'the latest target is in flight');
  for (const idx of [5, 6, 7]) assert.ok(!o.inflight[o._key(idx)], 'frame ' + idx + ' was abandoned');
  const n = w.fetches.length; o._scrubTo(10); await tick(); assert.equal(w.fetches.length, n, 'the same index again: no new fetch');
  const scrubbed = w.fetches.slice(before);                                     // frames 8, 9, 10 (6 and 7 were in flight already)
  assert.ok(scrubbed.length >= 3, 'the scrub did fetch new frames');
  assert.equal(new Set(scrubbed).size, scrubbed.length, 'no frame fetched twice (section 42: the old rule dropped frame 6 in flight on the scrub onto the decoded frame 5, then fetched it again)');
  o.unmount(); await w.releaseAll();
}));

test('section 37: a frame that lands late never replaces a newer scrub target', () => onRunDay(async () => {
  const w = world();
  const o = await mounted(w, A);
  const drawn0 = o.layer._frame;
  o._scrubTo(4); await settle();                               // frame 4 fetched, its decode held
  o._scrubTo(9); await settle();                               // frame 9 fetched; frame 4's decode is still pending
  const i4 = w.pendingBitmaps.findIndex((f) => /f012\.png/.test(f.blob.url)), i9 = w.pendingBitmaps.findIndex((f) => /f027\.png/.test(f.blob.url));
  assert.ok(i9 >= 0, 'frame 9 is decoding');
  if (i4 >= 0) { w.pendingBitmaps.splice(i4, 1)[0](); await settle(); }
  assert.notEqual(o.frameIndex, 4, 'the late frame 4 did not land'); assert.equal(o.layer._frame, drawn0, 'the picture is unchanged');
  assert.equal(o.target, 9);
  const j9 = w.pendingBitmaps.findIndex((f) => /f027\.png/.test(f.blob.url)); w.pendingBitmaps.splice(j9, 1)[0](); await settle();
  assert.equal(o.frameIndex, 9, 'the newer target landed');
  o.unmount(); await w.releaseAll();
}));

test('section 37: _ribbonKey steps one available frame, jumps to the ends and pages a day; before a frame is on the map it does nothing', () => onRunDay(async () => {
  const w = world();
  const o = await mounted(w, A);
  const i0 = o.frameIndex;
  assert.equal(o._ribbonKey('ArrowRight'), true); assert.equal(o.target, i0 + 1); assert.equal(o.playing, false);
  await w.releaseAll();
  o.unavailable[o._key(o.frameIndex + 1)] = true;
  assert.equal(o._ribbonKey('ArrowRight'), true); assert.equal(o.target, o.frameIndex + 2, 'an unavailable neighbour is skipped');
  await w.releaseAll();
  assert.equal(o._ribbonKey('Home'), true); assert.equal(o.target, 0); await w.releaseAll();
  assert.equal(o._ribbonKey('End'), true); assert.equal(o.target, 80); await w.releaseAll();
  assert.equal(o._ribbonKey('PageDown'), true); assert.equal(o.target, 72, '-24 h on an 81-frame run = 8 frames'); await w.releaseAll();
  assert.equal(o._ribbonKey('PageUp'), true); assert.equal(o.target, 80); await w.releaseAll();
  assert.equal(o._ribbonKey('Tab'), false); assert.equal(o._ribbonKey('x'), false);
  o.unmount(); await w.releaseAll();
  // the hourly / 3-hourly run: a page from +120 h lands on +144 h (index 128), from +123 h back on +99 h
  const w2 = world(); const F = manifest('2026092606', '2026-09-26T06:00:00Z', 6, false, FULL_HORIZON);
  const o2 = await mounted(w2, F);
  o2.target = 120; o2.frameIndex = 120;
  assert.equal(o2._ribbonKey('PageUp'), true); assert.equal(o2.target, 128); await w2.releaseAll();
  o2.target = 121; o2.frameIndex = 121;
  assert.equal(o2._ribbonKey('PageDown'), true); assert.equal(o2.target, 99); await w2.releaseAll();
  o2.target = 119; o2.frameIndex = 119;                        // +143 h is between two 3-hourly frames: a page LATER lands on +144
  assert.equal(o2._ribbonKey('PageUp'), true); assert.equal(o2.target, 128); await w2.releaseAll();
  o2.unmount(); await w2.releaseAll();
  const w3 = world(); w3.pointer = ptr(A); w3.manifests[A.run] = A; const o3 = w3.create();
  assert.equal(o3._ribbonKey('ArrowRight'), false, 'nothing mounted');
}));

test('section 37 (step-4 F1): manual navigation stops at the first and last AVAILABLE frames; playback still wraps', () => onRunDay(async () => {
  const w = world();
  const o = await mounted(w, A); await w.releaseAll();
  o.seek(80); await settle(); await w.releaseAll(); assert.equal(o.frameIndex, 80);
  assert.equal(o._ribbonKey('ArrowRight'), true); await settle(); await w.releaseAll();
  assert.equal(o.frameIndex, 80, 'no wrap to the first frame'); assert.equal(o.target, 80);
  o.seek(0); await settle(); await w.releaseAll(); assert.equal(o.frameIndex, 0);
  assert.equal(o._ribbonKey('ArrowLeft'), true); await settle(); await w.releaseAll();
  assert.equal(o.frameIndex, 0, 'no wrap to the last frame'); assert.equal(o.target, 0);
  // the first and the last two frames unavailable: the keys stop at the first / last frame that exists
  for (const i of [0, 79, 80]) o.unavailable[o._key(i)] = true;
  o.seek(78); await settle(); await w.releaseAll();
  o._ribbonKey('ArrowRight'); await settle(); await w.releaseAll(); assert.equal(o.frameIndex, 78);
  o._ribbonKey('Home'); assert.equal(o.target, 1, 'Home: the first available frame'); await settle(); await w.releaseAll(); assert.equal(o.frameIndex, 1);
  o._ribbonKey('ArrowLeft'); await settle(); await w.releaseAll(); assert.equal(o.frameIndex, 1);
  o._ribbonKey('End'); assert.equal(o.target, 78, 'End: the last available frame'); await settle(); await w.releaseAll(); assert.equal(o.frameIndex, 78);
  o.seek(74); await settle(); await w.releaseAll();
  o._ribbonKey('PageUp'); assert.equal(o.target, 78, '+24 h is past the run, whose last frames are missing: the last available one');
  await settle(); await w.releaseAll();
  o.seek(8); await settle(); await w.releaseAll();
  o._ribbonKey('PageDown'); assert.equal(o.target, 1, '-24 h = the missing first frame: the nearest available one, never a wrap');
  await settle(); await w.releaseAll();
  // playback is unchanged: from the last frame it goes on to the first
  o.unavailable = {};
  o.seek(80); await settle(); await w.releaseAll();
  o.play(); assert.equal(o.target, 0, 'playback wraps from the last frame to the first'); o.pause();
  o.unmount(); await w.releaseAll();
}));

test('section 37 (step-4 F2): a field switch or an Update while a scrubbed frame is loading keeps the REQUESTED time', () => onRunDay(async () => {
  const M = withTp('2026092212', '2026-09-22T12:00:00Z', 12);
  const w = world(); w.manifests[B.run] = B;
  const o = await mounted(w, M); await w.releaseAll();
  const i0 = o.frameIndex;
  o._scrubTo(i0 + 9); await settle();                          // beyond the ring: fetched now, its decode held
  assert.equal(o.target, i0 + 9); assert.equal(o.frameIndex, i0, 'still loading');
  o.mount('tp'); await settle(); await w.releaseAll();
  assert.equal(o.field, 'tp'); assert.equal(o.frameIndex, i0 + 9, 'the requested time, not the drawn one');
  o._scrubTo(i0 + 12); await settle(); assert.equal(o.frameIndex, i0 + 9);
  o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(o.field, 'hs'); assert.equal(o.frameIndex, i0 + 12);
  o._scrubTo(i0 + 15); await settle(); assert.equal(o.frameIndex, i0 + 12);
  w.pointer = ptr(B); o.newerRun = w.pointer; o.update(); await settle(); await w.releaseAll();
  assert.equal(o.manifest.run, B.run);
  assert.equal(o.layer.entry.valid_utc, M.frames[i0 + 15].valid_utc.replace('.000Z', 'Z'), 'Update: the requested time on the new run');
  clearInterval(o.runTimer); o.unmount(); await w.releaseAll();
}));

test('section 37 (step-4 F2): a field switch before a reload\'s first frame has landed still restores the saved time and play state', async () => {
  const M = withTp('2026092212', '2026-09-22T12:00:00Z', 12), t5 = Date.parse(M.frames[5].valid_utc);
  const storage = memStore({ field: 'hs', t: t5, playing: true, at: Date.now() }), w = world({ storage });
  w.pointer = ptr(M); w.manifests[M.run] = M;
  const o = w.create();
  o.mount('hs', true); await settle();                          // the manifest is in; the restored frame is decoding
  assert.equal(o.target, 5); assert.equal(o.frameIndex, null);
  o.mount('tp'); await settle(); await w.releaseAll();
  assert.equal(o.field, 'tp'); assert.equal(o.playing, true, 'the saved play state survives the switch');
  assert.ok(o.frameIndex === 5 || o.frameIndex === 6, 'restored at the saved frame (playback starts by stepping on): ' + o.frameIndex);
  done(o); await w.releaseAll();
});

test('section 37: _ribbonLabel gives the requested time while loading and the drawn one after; unavailable keeps the picture', () => onRunDay(async () => {
  const w = world();
  const o = await mounted(w, A);
  await w.releaseAll();
  o.opts.tz = 'Pacific/Honolulu'; o.opts.tzAbbr = () => 'HST';
  const fmt = w.I.ribbonFormatter('Pacific/Honolulu'), at = (i) => Date.parse(A.frames[i].valid_utc), hrs = (i) => A.frames[i].step;
  const NAMES = { Sun: 'Sunday', Mon: 'Monday', Tue: 'Tuesday', Wed: 'Wednesday', Thu: 'Thursday', Fri: 'Friday', Sat: 'Saturday' };
  const wd = (i) => fmt.parts(at(i)).wd;
  const i0 = o.frameIndex, expect = (i, st) => ({ text: wd(i) + ', ' + fmt.day(at(i)) + ' · ' + fmt.clock(at(i)) + ' HST', state: st ? st + '…' : '',
    valuetext: NAMES[wd(i)] + ', ' + fmt.day(at(i)) + ', ' + fmt.clock(at(i)) + ' HST (+' + hrs(i) + ' h)' + (st ? ', ' + st : '') });
  assert.match(expect(i0).text, /^(Sun|Mon|Tue|Wed|Thu|Fri|Sat), Sep \d+ · /);
  assert.deepEqual(o._ribbonLabel(), expect(i0, ''));
  const key = (i) => `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i].step).padStart(3, '0')}.png`;
  o._scrubTo(i0 + 5); await settle();                          // beyond the ring: fetched now, its decode held
  assert.deepEqual(o._ribbonLabel(), expect(i0 + 5, 'loading'));
  assert.equal(o.frameIndex, i0, 'the picture is still the drawn frame');
  await w.releaseAll();
  assert.equal(o._ribbonLabel().state, ''); assert.equal(o.frameIndex, i0 + 5);
  w.failNext['https://x/' + key(i0 + 9)] = 404;
  o._scrubTo(i0 + 9); await settle(); await w.releaseAll(); await settle();
  assert.equal(o._isUnavailable(i0 + 9), true); assert.equal(o.frameIndex, i0 + 5, 'the drawn frame stays');
  const u = o._ribbonLabel(); assert.equal(u.text, expect(i0 + 9).text); assert.equal(u.state, 'unavailable'); assert.match(u.valuetext, /, unavailable$/);
  o.unmount(); await w.releaseAll();
}));

test('G26 A-P3-7: a minute timer refreshes the run line while mounted (an idle panel turns "about ..." into "expected shortly"); Off stops it', async () => {
  const w = world();
  const o = await mounted(w, A);
  assert.ok(o.lineTimer, 'set at mount');
  if (o.lineTimer._idleTimeout !== undefined) assert.equal(o.lineTimer._idleTimeout, 60000, 'once a minute (re-check RC-8 g)');
  const t0 = o.lineTimer; o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(o.lineTimer, t0, 'one timer however many mounts');
  clearInterval(o.runTimer); o.unmount();
  assert.equal(o.lineTimer, null, 'cleared at Off');
});


// ---- plan section 42: the fast speeds reach their rate ----
// The module's Date.now follows the harness's fake clock (w.now) for the cadence tests.
async function onFakeClock(w, fn) {
  const real = Date.now, base = Date.parse('2026-09-23T00:00:00Z');
  Date.now = () => base + w.now();
  try { await fn(); } finally { Date.now = real; }
}

test('section 42: the cadence is drift-free (each frame due one interval after the last was due); a late frame is followed by one back-to-back frame, never a skip', async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    const o = await mounted(w, A); await w.releaseAll();
    const i0 = o.frameIndex, seen = [];
    o.setSpeed(4); o.play(); await settle(); await w.releaseAll();
    assert.equal(o._interval(), 125);
    assert.equal(o.frameIndex, i0 + 1, 'play() shows the next frame at once');
    for (let k = 0; k < 16; k++) {                                          // every frame cached or landing at once: 125 ms apart exactly
      await w.fade(125); await settle(); await w.releaseAll(); await settle();
      seen.push([w.now(), o.frameIndex]);
    }
    for (let k = 0; k < seen.length; k++) assert.equal(seen[k][1], (i0 + 2 + k) % A.frames.length, 'frame ' + k + ' at ' + seen[k][0]);
    assert.equal(o._due - (Date.now() - w.now()), 17 * 125, 'the due time advanced by exactly one interval per frame');
    // a frame whose decode is held 300 ms (two intervals and a half): its successor comes at once, the next one 125 ms later
    let steps = 0;                                                          // no more releases: the playhead reaches the ring's pending decodes
    while (o.target === o.frameIndex && steps < 12) { await w.fade(125); await settle(); steps++; }
    const held = o.target;
    assert.equal(held, o.frameIndex + 1, 'the picture waits for the frame'); assert.ok(steps < 12);
    const heldUrl = 'https://x/gfswave/0p25/v1/' + A.run + '/hs/f' + String(A.frames[held].step).padStart(3, '0') + '.png';
    const pending = w.pendingBitmaps.find((f) => f.blob.url === heldUrl); assert.ok(pending, 'its decode is pending');
    await w.fade(300); await settle();
    assert.equal(o.frameIndex, held - 1, 'still waiting: no frame skipped');
    pending(); w.pendingBitmaps = w.pendingBitmaps.filter((f) => f !== pending); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, held, 'the late frame lands');
    await w.fade(0); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, held + 1, 'its successor follows at once (one back-to-back frame)');
    await w.fade(124); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, held + 1, 'the one after it waits a whole interval again');
    await w.fade(1); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, held + 2);
    o.pause(); o.unmount();
  });
});

test('section 42: the ring follows the speed: shark fetches 4 at once and keeps up to 9 frames, a pause shrinks both back and never drops the target\'s own fetch; a landing refills the ring', async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    const o = await mounted(w, A);
    // paused: a ring frame that lands starts the next one (the ring fills without another landing of the target)
    assert.equal(Object.keys(o.inflight).length, 2);
    await w.release(1);
    assert.equal(Object.keys(o.inflight).length, 2, 'a third ring fetch started when the first landed');
    await w.releaseAll();
    const i0 = o.frameIndex, ring1 = w.I.ringPlan(i0, A.frames.length, 1);
    assert.ok(ring1.every((i) => o.cache.has(o._key(i))), 'the paused ring is complete'); assert.equal(o.cache.size(), 5);
    assert.deepEqual(o._ring(), w.I.PLAY_RING[1]);
    o.setSpeed(4); o.play(); await settle();
    assert.deepEqual(o._ring(), w.I.PLAY_RING[4]); assert.equal(o._maxInflight(), 4);
    assert.equal(Object.keys(o.inflight).length, 4, 'four downloads at once at shark speed');
    await w.releaseAll();
    const target = o._anchorIndex(), ring4 = w.I.ringPlan(target, A.frames.length, 1, 6, 1);
    assert.ok(ring4.every((i) => o.cache.has(o._key(i))), 'the shark ring is complete'); assert.ok(o.cache.size() <= 9 && o.cache.size() >= 8, 'cache ' + o.cache.size());
    o.setSpeed(2); assert.equal(o._maxInflight(), 3); assert.equal(o._ring().decoded, 7);
    o.pause(); await settle();
    assert.equal(o.cache.size(), 5, 'back to the paused budget'); assert.ok(o.cache.has(o._key(o.frameIndex)), 'the drawn frame kept');
    const a0 = o._anchorIndex();                                                 // the shrink keeps the paused ring's near frames, not the LRU ones
    for (const j of [a0, a0 + 1, a0 + 2, a0 - 1]) assert.ok(o.cache.has(o._key(j)), 'kept after the shrink: ' + (j - a0));
    assert.ok(Object.keys(o.inflight).length <= 2);
    // a pending far target with its fetch in flight, then shark play and a pause: the cap shrinks around it
    o.seek(i0 + 40); await tick();
    assert.ok(o.inflight[o._key(i0 + 40)], 'the target is in flight');
    o.setSpeed(4); o.play(); await tick();
    assert.ok(Object.keys(o.inflight).length <= 4 && Object.keys(o.inflight).length >= 3, 'the ring around the pending target started');
    o.pause(); await tick();
    assert.ok(Object.keys(o.inflight).length <= 2, 'in flight ' + Object.keys(o.inflight).length);
    assert.ok(o.inflight[o._key(i0 + 40)], 'the target\'s own fetch (rank 0) survives the shrink');
    assert.ok(o.cache.has(o._key(o.frameIndex)) && o.cache.get(o._key(o.frameIndex)) === o.layer._frame, 'the frame on the map, outside the pending target\'s ring, survives the shrink (G29 P5)');
    o.unmount();
    assert.equal(Object.keys(o.inflight).length, 0); assert.equal(o.cache.size(), 0);
  });
});

test('section 42: with the animation on, shark keeps the field and direction frames of the ring and the pause keeps the target\'s direction fetch (rank 1)', async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    w.pointer = ptr(A); w.manifests[A.run] = A;
    const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
    const i0 = o.frameIndex;
    o.setSpeed(4); o.play(); await settle(); await w.releaseAll();
    assert.ok(o.cache.size() <= 9 && o.dcache.size() <= 9, 'caches ' + o.cache.size() + ' / ' + o.dcache.size());
    const ring4 = w.I.ringPlan(o._anchorIndex(), A.frames.length, 1, 6, 1);
    assert.ok(ring4.every((i) => o.cache.has(o._key(i)) && o.dcache.has(o._key(i, 'dir'))), 'field and direction of every ring step');
    o.pause(); await settle();
    assert.ok(o.cache.size() <= 5 && o.dcache.size() <= 5);
    o.seek(i0 + 40); await tick();                                            // field + direction of the target in flight (rank 0 / 1)
    assert.ok(o.inflight[o._key(i0 + 40)] && o.inflight[o._key(i0 + 40, 'dir')]);
    o.setSpeed(4); o.play(); await tick();
    assert.ok(Object.keys(o.inflight).length <= 4);
    o.pause(); await tick();
    assert.ok(Object.keys(o.inflight).length <= 2);
    assert.ok(o.inflight[o._key(i0 + 40)] && o.inflight[o._key(i0 + 40, 'dir')], 'both of the target\'s fetches survive');
    assert.ok(o.dcache.has(o._key(o.frameIndex, 'dir')) && o.dcache.get(o._key(o.frameIndex, 'dir')) === o.flow.dir, 'the animator\'s direction, outside the ring, survives the shrink (G29 P7)');
    o.unmount();
  });
});

test('section 42: a far target in a cooldown with the drawn frame outside its ring: the ring is filled up to the budget and nothing is fetched back and forth', () => onRunDay(async () => {
  const w = world();
  const o = await mounted(w, A); await w.releaseAll();
  const i0 = o.frameIndex, key = (i) => `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i].step).padStart(3, '0')}.png`;
  w.failNext['https://x/' + key(i0 + 40)] = 'network';
  const before = w.fetches.length;
  o.seek(i0 + 40); await settle(); await w.releaseAll(); await settle(); await w.releaseAll();
  assert.equal(o.transientFails, 1); assert.equal(o.frameIndex, i0); assert.equal(o.target, i0 + 40);
  assert.ok(w.fetches.length - before <= 5, 'fetches ' + (w.fetches.length - before));
  assert.equal(o.cache.size(), 5); assert.ok(o.cache.has(o._key(i0)), 'the drawn frame kept');
  assert.equal(Object.keys(o.inflight).length, 0, 'nothing left in flight: the ring is full up to the budget');
  o.unmount();
}));


test('section 42 (step 2 finding): a step whose frame is decoded keeps every ring fetch ahead of it; with too little room the farthest go, never the nearest', async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    w.pointer = ptr(A); w.manifests[A.run] = A;
    const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
    const rec = (k, kind) => ({ promise: Promise.resolve(), abort: new AbortController(), key: k, kind });
    o.setSpeed(4); o.play(); await settle();                                    // (play's own landing; the ring is in flight now)
    const i = o.frameIndex, t = i + 1;
    // the next step's frame decoded, four ring fetches ahead of it in flight: none is dropped
    o.cache.set(o._key(t), { q: new Uint8Array(1), cols: 1, rows: 1 }, o._key(i));
    o.inflight = {}; for (const k of [o._key(t + 1), o._key(t + 1, 'dir'), o._key(t + 2), o._key(t + 2, 'dir')]) o.inflight[k] = rec(k, /dir/.test(k) ? 'dir' : 'field');
    const before = Object.keys(o.inflight).sort();
    o._trimInflight(t);
    assert.deepEqual(Object.keys(o.inflight).sort(), before, 'a decoded target needs no slot: every ring fetch keeps its own');
    // the target not decoded (its slot is reserved) and four ring fetches, the OLDEST one the nearest: room for three; the
    // farthest goes (the old rule dropped the oldest, i.e. the next frame needed)
    const T = t + 3; o.cache.map.delete(o._key(T));
    o.inflight = {}; for (const j of [T + 1, T + 6, T + 4, T + 5]) o.inflight[o._key(j)] = rec(o._key(j), 'field');
    o._trimInflight(T);
    assert.deepEqual(Object.keys(o.inflight).sort(), [T + 1, T + 4, T + 5].map((j) => o._key(j)).sort(), 'kept the nearest three, dropped T+6');
    o.inflight = {}; o.pause(); o.unmount();
  });
});

test('section 42 (step 2 finding): the cache never evicts a frame the ring still needs: with downloads faster than the playback (a phone at shark speed), no frame is downloaded twice, at 4x and at 1x', async () => {
  for (const speed of [4, 1]) {
    const w = world({ fakeTimers: true });
    await onFakeClock(w, async () => {
      w.pointer = ptr(A); w.manifests[A.run] = A;
      const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
      o.seek(0); await settle(); await w.releaseAll();
      const from = w.fetches.length;
      o.setSpeed(speed); o.play(); await settle(); await w.releaseAll();
      for (let k = 0; k < 40; k++) { await w.fade(o._interval()); await settle(); await w.releaseAll(); await settle(); }
      o.pause();
      const seen = {}, twice = [];
      for (const u of w.fetches.slice(from).filter((x) => /\.png$/.test(x))) { if (seen[u]) twice.push(u.split('/').slice(-2).join('/')); seen[u] = true; }
      assert.deepEqual(twice, [], speed + 'x: frames downloaded twice');
      assert.ok(o.frameIndex >= 40, 'played on (' + o.frameIndex + ')');
      o.unmount();
    });
  }
});


// G29 re-check R-1: shark playback, the animation on, the playhead waiting for a held frame T; reduced motion switched
// on (read live, no event), then a pause + play (or a hide + show) while T decodes: the old play's _goto(T) waited for
// T's direction too, the new one does not, so the old landing comes second. It must not touch the new play's gate.
async function staleGateScenario(variant, o2) {
  const w = world({ fakeTimers: true, raf: true, realDocument: !!o2.vis });
  const out = {};
  await onFakeClock(w, async () => {
    w.pointer = ptr(A); w.manifests[A.run] = A;
    const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll(); await w.paint(); await settle();
    o.setSpeed(4); o.play(); await settle(); await w.releaseAll(); await settle();
    for (let k = 0; k < 3; k++) { await w.paint(); await settle(); await w.fade(125); await settle(); await w.releaseAll(); await settle(); }
    let guard = 0;                                                             // stop releasing: the playhead runs to a held frame
    while (o.target === o.frameIndex && guard++ < 30) { await w.paint(); await settle(); await w.fade(125); await settle(); }
    const T = o.target, stepT = String(A.frames[T].step).padStart(3, '0');
    if (!o2.noReduced) w.g.matchMedia = (q) => ({ matches: /reduced-motion/.test(q) });
    if (o2.vis) { w.doc.hidden = true; w.fire('doc', 'visibilitychange'); w.doc.hidden = false; w.fire('doc', 'visibilitychange'); }
    else { o.pause(); o.play(); }
    await settle();
    await w.fade(variant === 'late' ? 200 : 10); await settle();
    const i = w.pendingBitmaps.findIndex((f) => f.blob.url.endsWith('/hs/f' + stepT + '.png'));
    assert.ok(i >= 0, 'T\'s field decode is held'); w.pendingBitmaps.splice(i, 1)[0](); await settle();
    out.landed = o.frameIndex === T;
    await w.fade(variant === 'late' ? 0 : 120); await settle();               // the next frame is due (late: at once)
    const f0 = o.frameIndex;
    for (let k = 0; k < 60; k++) { await w.paint(); await settle(); await w.fade(16); await settle(); if (w.pendingBitmaps.length) await w.releaseAll(); }
    out.advanced = o.frameIndex - f0; out.playing = o.playing;
    out.pending = o.timer !== null || !!o._paintWaiting || w.rafQ.size > 0;
    o.unmount();
  });
  return out;
}

test('G29 re-check R-1: a frame of an older play landing after the new play\'s never strands playback (late landing, slow render, hide + show; reduced motion switched on in between)', async () => {
  for (const [variant, o2] of [['late', {}], ['slowRender', {}], ['late', { vis: true }], ['late', { noReduced: true }]]) {
    const r = await staleGateScenario(variant, o2);
    const name = variant + (o2.vis ? ' + hide/show' : '') + (o2.noReduced ? ' (control)' : '');
    if (!o2.noReduced) assert.ok(r.landed, name + ': the held frame landed (no direction to wait for)');
    assert.equal(r.playing, true, name);
    assert.ok(r.advanced >= 4, name + ': playback goes on (advanced ' + r.advanced + ' frames in ~1 s of renders)');
    assert.ok(r.pending, name + ': something is pending (a due timer, a waiting tick or an animation frame)');
  }
});

test('G29 re-check R-2 / G8: a frame that lands after a pause arms no gate (no animation frame, no fallback timer); play right after it lands its next frame from the play itself', async () => {
  const w = world({ fakeTimers: true, raf: true });
  await onFakeClock(w, async () => {
    const o = await mounted(w, A); await w.releaseAll();
    o.setSpeed(4); o.play(); await settle(); await w.releaseAll(); await settle();
    let guard = 0;                                                             // stop releasing: the playhead runs to a held frame
    while (o.target === o.frameIndex && guard++ < 30) { await w.paint(); await settle(); await w.fade(125); await settle(); }
    const T = o.target; assert.notEqual(T, o.frameIndex, 'a frame is loading');
    o.pause(); await settle();
    await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, T, 'the frame the playback waited for still lands');
    assert.equal(o._painting, false, 'paused: no gate'); assert.equal(w.rafQ.size, 0, 'no animation frame asked for');
    assert.equal(o._paintFallback, null, 'no fallback timer while paused');
    o.play(); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, T + 1, 'play lands the next frame at once (the first tick of a play is not gated)');
    for (let k = 0; k < 4; k++) { await w.paint(); await settle(); await w.fade(125); await settle(); await w.releaseAll(); await settle(); }
    assert.equal(o.frameIndex, T + 5, 'and plays on, one frame per interval');
    o.unmount();
  });
});

test('section 42 step 3: the particles\' rate follows the playback (1 paused, the speed while playing); Off disposes the decode worker; the readout is text only and runs only with ?ovdebug=1', async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    w.pointer = ptr(A); w.manifests[A.run] = A;
    const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
    assert.ok(o.flow); assert.equal(o.flow.rate, 1);
    o.setSpeed(4); assert.equal(o.flow.rate, 1, 'paused: 1 whatever the speed');
    o.play(); assert.equal(o.flow.rate, 4);
    o.setSpeed(2); assert.equal(o.flow.rate, 2);
    o.pause(); assert.equal(o.flow.rate, 1);
    o.play(); assert.equal(o.flow.rate, 2);
    const fake = { disposed: 0, dead: false, worker: null, dispose() { this.disposed++; } };
    w.I.setDecoderForTest(fake);
    assert.equal(w.I.OV_DEBUG, false, 'no ?ovdebug in the harness'); assert.equal(o.debugTimer, undefined);
    assert.match(o._debugText(), /playing: true\nspeed: 2\n/); assert.match(o._debugText(), /decoder: idle/); assert.match(o._debugText(), /flow\.rate: 2/);
    o.unmount();
    assert.equal(fake.disposed, 1, 'Off disposes the decoder'); assert.equal(o.flow, null);
    w.I.resetDecoder();
  });
});

// ---- G29 fix round ----
test('G29 B-P1-1: the paint gate: a playback landing is not followed by the next until an animation frame has run after it (the browser has rendered it); rendering that keeps up leaves the cadence alone, slow rendering paces the landings one per render without a burst; the fallback; pause and Off leave nothing', async () => {
  const w = world({ fakeTimers: true, raf: true });
  await onFakeClock(w, async () => {
    const o = await mounted(w, A); await w.releaseAll();
    assert.equal(w.I.PAINT_FALLBACK_MS, 250);
    o.setSpeed(4); o.play(); await settle(); await w.releaseAll(); await settle();
    const i1 = o.frameIndex;                                                 // play's first landing (not gated), its render pending
    assert.equal(o._painting, true); assert.equal(w.rafQ.size, 1, 'one animation frame asked for');
    await w.fade(125); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, i1, 'due, but the last landing is not on screen yet: no landing');
    assert.equal(o.timer, null, 'the due timer fired and the tick waits for the render, no second timer');
    { const cbs = [...w.rafQ.values()]; w.rafQ.clear(); for (const f of cbs) f(w.now()); await settle(); }
    assert.equal(o.frameIndex, i1, 'nothing lands inside the animation-frame callback itself (the rendering update comes after it)');
    await w.fade(0); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, i1 + 1, 'the render releases exactly one landing (from a task after the animation frame)');
    assert.equal(o._painting, true, 'and that landing waits for its own render');
    // rendering that keeps up: the cadence is the plain 125 ms
    const dueBefore = o._due;
    for (let k = 0; k < 8; k++) {
      await w.paint(); await settle();
      assert.equal(o.frameIndex, i1 + 1 + k, 'a render alone lands nothing before the frame is due');
      await w.fade(125); await settle(); await w.releaseAll(); await settle();
      assert.equal(o.frameIndex, i1 + 2 + k, 'landing ' + k + ' at its due time');
    }
    assert.equal(o._due - dueBefore, 8 * 125, 'the due time advanced one interval per landing');
    // rendering every 150 ms: one landing per render, never two (no catch-up burst), the cadence follows the renders
    let idx = o.frameIndex;
    for (let k = 0; k < 6; k++) {
      await w.fade(150); await settle(); await w.releaseAll(); await settle();
      assert.equal(o.frameIndex, idx, 'cycle ' + k + ': due, waiting for the render');
      await w.paint(); await settle(); await w.releaseAll(); await settle();
      assert.equal(o.frameIndex, idx + 1, 'cycle ' + k + ': exactly one landing per render'); idx++;
    }
    // no animation frame at all (a throttled page): the fallback lands the frame PAINT_FALLBACK_MS after the last landing
    await w.fade(125); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, idx, 'due, no render');
    await w.fade(125); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, idx + 1, 'the fallback landed it 250 ms after the last landing');
    // pause and Off clear the gate and its timers
    o.pause();
    assert.equal(o._painting, false); assert.equal(o._paintWaiting, null); assert.equal(w.rafQ.size, 0, 'the animation frame is cancelled');
    o.play(); await settle(); await w.releaseAll(); await settle();
    assert.equal(o._painting, true);
    o.unmount(); await settle();
    assert.equal(o._painting, false); assert.equal(w.rafQ.size, 0); assert.equal(w.pendingTimers(), 0, 'no timer of any kind left after Off');
  });
});

test('G29 B-P1-1: without requestAnimationFrame (no rendering to wait for) the gate is off and the cadence is as before', async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    const o = await mounted(w, A); await w.releaseAll();
    o.setSpeed(4); o.play(); await settle(); await w.releaseAll(); await settle();
    assert.equal(o._painting, false);
    const i1 = o.frameIndex;
    await w.fade(125); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, i1 + 1);
    o.unmount();
  });
});

test('G29 A-P3-1: after a resolution change the frame on the map and the animator\'s direction are kept by VALUE, not by their old key: a shrink or a landing over the cap never drops them, and a zoom back across the threshold draws them without a download', async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    // the field: wind at zoom 6 is half-resolution, zoom 8 full (a 1200-px map); shark fills 9 half frames
    w.pointer = ptr(A); w.manifests[A.run] = A;
    w.map.getZoom = () => 6;
    const o = w.create(); o.mount('wind'); await settle(); await w.releaseAll();
    assert.equal(o.res, 'half');
    o.setSpeed(4); o.play(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll();
    const drawn = o.layer._frame, drawnKey = o._key(o.frameIndex);
    assert.ok(o.cache.size() >= 8 && o.cache.get(drawnKey) === drawn);
    for (const k of [...o.cache.map.keys()]) if (k !== drawnKey) o.cache.get(k);                  // the drawn frame is now the least recently used
    w.map.getZoom = () => 8; o._checkRes(); await settle();                                       // full resolution: the new target's fetch is pending
    assert.equal(o.res, 'full'); assert.equal(o.layer._frame, drawn, 'the half frame stays on the map meanwhile');
    o.pause(); await settle();                                                                      // the shrink 9 -> 5 (the half frames have no rank now)
    assert.ok(o.cache.size() <= 6);
    assert.ok([...o.cache.map.values()].includes(drawn), 'the frame on the map survives the shrink under its old-resolution key');
    const url = 'https://x/gfswave/0p25/v1/' + A.run + '/half/wind/f' + String(A.frames[o.frameIndex].step).padStart(3, '0') + '.png';
    const before = w.fetches.filter((u) => u === url).length;
    w.map.getZoom = () => 6; o._checkRes(); await settle();                                        // back to half before the full frame landed
    assert.equal(o.res, 'half'); assert.equal(o.layer._frame, drawn);
    assert.equal(w.fetches.filter((u) => u === url).length, before, 'no download of the frame on the map: it was in the cache (its evicted ring neighbours are fetched, as before)');
    o.unmount();
  });
  const w2 = world({ fakeTimers: true });
  await onFakeClock(w2, async () => {
    // the direction: pdir half at zoom 6, full from 6.5; a full landing over the cap must not drop the animator's half direction
    w2.pointer = ptr(A); w2.manifests[A.run] = A;
    const o = w2.create(); o.anim = true; o.mount('hs'); await settle(); await w2.releaseAll();
    assert.equal(o.dres, 'half'); const dir = o.flow.dir, dirKey = o._key(o.frameIndex, 'dir');
    assert.ok(dir && o.dcache.get(dirKey) === dir && o.dcache.size() === 5);
    for (const k of [...o.dcache.map.keys()]) if (k !== dirKey) o.dcache.get(k);                 // the shown direction is the least recently used
    w2.map.getZoom = () => 6.5; o._checkRes(); await settle();
    assert.equal(o.dres, 'full'); assert.equal(o.flow.dir, dir, 'kept until the full one lands');
    { const i = w2.pendingBitmaps.findIndex((f) => f.blob.url.includes('/pdir/') && f.blob.w === 1440); assert.ok(i >= 0, 'a full direction decode is pending'); w2.pendingBitmaps.splice(i, 1)[0](); await settle(); }   // one full direction lands: 6 > 5
    assert.ok(o.dcache.size() <= 6);
    assert.ok([...o.dcache.map.values()].includes(dir), 'the animator\'s direction survives the landing over the cap');
    const url = 'https://x/gfswave/0p25/v1/' + A.run + '/half/pdir/f' + String(A.frames[o.frameIndex].step).padStart(3, '0') + '.png';
    const before = w2.fetches.filter((u) => u === url).length;
    w2.map.getZoom = () => 3.7; o._checkRes(); await settle();
    assert.equal(o.dres, 'half'); assert.equal(o.flow.dir, dir, 'the same direction, no download');
    assert.equal(w2.fetches.filter((u) => u === url).length, before, "the shown step's direction was in the cache");
    o.unmount();
  });
});

test('G29 G6: _startDir at shark speed honours the speed\'s download cap: with 3 ring downloads in flight the next step\'s direction joins them and nothing is aborted', async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    w.pointer = ptr(A); w.manifests[A.run] = A;
    const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
    o.setSpeed(4); o.play(); await settle(); await w.releaseAll(); await settle();
    const t = o.frameIndex + 1;
    o.dcache.map.delete(o._key(t, 'dir'));
    for (const k in o.inflight) o.inflight[k].abort.abort(); o.inflight = {};
    const fakes = [t + 2, t + 3, t + 4].map((j) => { const r = { promise: Promise.resolve(), abort: new AbortController(), key: o._key(j), kind: 'field', aborted: false }; r.abort.signal.addEventListener('abort', () => { r.aborted = true; }); o.inflight[r.key] = r; return r; });
    o._startDir(t); await tick();
    assert.ok(o.inflight[o._key(t, 'dir')], 'the direction download started');
    assert.equal(Object.keys(o.inflight).length, 4, 'four in flight at shark speed');
    assert.deepEqual(fakes.map((r) => r.aborted), [false, false, false], 'no ring download aborted');
    o.inflight = {}; o.unmount();
  });
});

test("G29 B-P2-2: an exception inside the animator (a refused drawing surface) never reaches the field's playback: the frame lands, the state stays ready, one warning", async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    w.pointer = ptr(A); w.manifests[A.run] = A;
    const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseAll();
    const had = console.warn, warned = []; console.warn = (...a) => warned.push(a.join(' '));
    try {
      o.flow.setData = () => { throw new TypeError('Cannot read properties of null'); };
      const i0 = o.frameIndex;
      o.seek(i0 + 1); await settle(); await w.releaseAll(); await settle();
      assert.equal(o.frameIndex, i0 + 1, 'the frame landed'); assert.equal(o.last.state, 'ready');
      o.seek(i0 + 2); await settle(); await w.releaseAll(); await settle();
      assert.equal(o.frameIndex, i0 + 2); assert.equal(warned.length, 1, 'warned once: ' + warned.join(' | '));
    } finally { console.warn = had; }
    o.unmount();
  });
});

test('G29 G5: an animator created while playing takes the playback rate at once', async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    const o = await mounted(w, A); await w.releaseAll();
    o.setSpeed(4); o.play(); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.flow, null);
    o.setAnim(true); await settle();
    assert.ok(o.flow); assert.equal(o.flow.rate, 4, 'a new animator runs at the speed');
    o.unmount();
  });
});

test('G29 L6/L7/G2/G4: Off leaves no timer, interval, idle pass or animation frame behind, also in debug mode; the readout\'s timer stops by itself when no readout element exists', async () => {
  const hadLoc = global.location;
  global.location = { search: '?ovdebug=1' };
  const w = world({ fakeTimers: true, fakeIntervals: true, raf: true });
  try {
    await onFakeClock(w, async () => {
      assert.equal(w.I.OV_DEBUG, true);
      const o = await mounted(w, A); await w.releaseAll();
      o.setSpeed(4); o.play(); await settle(); await w.releaseAll(); await settle();
      for (let k = 0; k < 3; k++) { await w.paint(); await w.fade(125); await settle(); await w.releaseAll(); await settle(); }
      o._debugOn(); assert.ok(o.debugTimer, 'the readout timer');
      o._debugOn(); o._debugOn();                                               // renders: never a second interval
      const n = w.pendingTimers();
      await w.fade(1000);                                                        // no readout element (render is stubbed): the timer stops itself
      assert.equal(o.debugTimer, null); assert.ok(w.pendingTimers() < n);
      o._debugOn(); assert.ok(o.debugTimer);
      o.unmount(); await settle();                                                // an aborted download clears its own watchdog a microtask later
      assert.equal(o.debugTimer, null); assert.equal(o.runTimer, null); assert.equal(o.timer, null);
      assert.deepEqual(w.timerSources(), [], 'nothing in the timer queue after Off'); assert.equal(w.rafQ.size, 0, 'no animation frame after Off');
      await w.fade(60000); assert.equal(w.pendingTimers(), 0, 'and nothing appears later');
    });
  } finally { if (hadLoc === undefined) delete global.location; else global.location = hadLoc; }
});

// ------------------------------- plan section 43: the model switch (GFS-Wave | ECMWF) -------------------------------
// An ECMWF run of the same days as A (2026-09-22 00Z, 85 frames: 3-hourly to 144 h, 6-hourly to 360 h) under its own
// prefix; its frames carry the tag 0. A's frame 10 (+30 h = 09-23 18Z) is E's frame 14 (+42 h).
const E_STEPS = [...Array(49).keys()].map((i) => 3 * i).concat([...Array(36).keys()].map((i) => 150 + 6 * i));
const E = (() => { const m = manifest('2026092200', '2026-09-22T00:00:00Z', 0, false, E_STEPS); m.files.template = `ecmwf/0p25/v1/${m.run}/{res}{field}/f{step:03d}.png`; m.model = { name: 'ECMWF IFS wave (ecWAM) + IFS' }; return m; })();
const eptr = (m) => ({ run: m.run, manifest: `ecmwf/0p25/v1/${m.run}/manifest-x.json`, complete: true, published_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z') });
const TWO = ['gfswave', 'ecmwf'];
const tagOf = (o) => o.layer._frame.q[0];                      // 12 = run A (GFS), 0 = run E (ECMWF)
async function twoModels(w, extra) {                           // mounted on A (GFS) with E (ECMWF) published, at A's frame 10
  w.manifests['ecmwf/' + E.run] = E; w.pointers = { ecmwf: eptr(E) };
  const o = await mounted(w, A, Object.assign({ models: TWO }, extra || {}));
  o.seek(10); await settle(); await w.releaseAll();
  assert.equal(o.frameIndex, 10); assert.equal(tagOf(o), 12);
  return o;
}
const releaseRun = async (w, run) => { const f = w.pendingBitmaps.filter((x) => x.blob.url.includes('/' + run + '/')); w.pendingBitmaps = w.pendingBitmaps.filter((x) => !x.blob.url.includes('/' + run + '/')); f.forEach((x) => x()); await settle(); };
const landed = async (w, run) => { for (let i = 0; i < 6 && !w.pendingBitmaps.some((x) => x.blob.url.includes('/' + run + '/')); i++) await settle(); await releaseRun(w, run); };
assert.equal(E.frames[14].valid_utc, A.frames[10].valid_utc);

test('section 43: a model switch drops the old run at once (aborted downloads, empty caches, no picture), loads the new model from ITS pointer, keeps the time and the play state, and a late old-model decode never lands', async () => {
  const store = memStore({}), w = world({ storage: store });
  const o = await twoModels(w);
  o.play(); await settle();
  const pendingA = w.pendingBitmaps.length;                                   // ring downloads of run A decoding
  assert.ok(pendingA >= 1);
  const anchor = o._anchorIndex(), want = w.I.nearestIndex(E, Date.parse(A.frames[anchor].valid_utc));   // playing: the NEXT frame is already requested
  assert.equal(anchor, 11);
  o.setModel('ecmwf');
  assert.equal(o.model, 'ecmwf'); assert.equal(o.manifest, null); assert.equal(o.cache.size(), 0); assert.equal(o.dcache.size(), 0);
  assert.deepEqual(Object.keys(o.inflight), []); assert.equal(o.playing, false); assert.equal(o.frameIndex, null); assert.equal(o.layer._frame, null);
  const sv = JSON.parse(store.getItem(w.I.SESSION_KEY));
  assert.equal(sv.model, 'gfswave', 'nothing of ECMWF has landed: the tab still names GFS (G30-3: a reload is never trapped)');
  assert.equal(sv.playing, true, 'and keeps the play state (G30-5)');
  await releaseRun(w, A.run);                                                 // the old decodes resolve into the void
  assert.equal(o.cache.size(), 0, 'no run-A frame in the cache'); assert.equal(o.layer._frame, null);
  await landed(w, E.run);
  assert.ok(w.fetches.includes('https://x/ecmwf/0p25/v1/latest.json'), 'the ECMWF pointer'); assert.ok(w.fetches.includes('https://x/' + eptr(E).manifest));
  assert.equal(o.manifest.run, E.run); assert.equal(o.frameIndex, want, 'the requested time, +33 h: E frame ' + want); assert.equal(tagOf(o), 0, 'an ECMWF frame on the map');
  assert.equal(o.playing, true, 'playback resumed'); assert.equal(o._carry, null, 'the carry is spent');
  assert.equal(JSON.parse(store.getItem(w.I.SESSION_KEY)).model, 'ecmwf', 'saved for the tab when its first frame landed');
  for (const k of o.cache.map.keys()) assert.ok(k.startsWith('ecmwf/' + E.run + '/'), k);
  assert.equal(o._def().cycleMs, 12 * 3.6e6); assert.equal(o._def().staleS, 15 * 3600);
  done(o);
});

test('section 43: a switch while a scrub target is still loading carries the REQUESTED time, not the drawn one', async () => {
  const w = world();
  const o = await twoModels(w);
  o.seek(20); await settle();                                                 // held: nothing released
  assert.equal(o.target, 20); assert.equal(o.frameIndex, 10);
  o.setModel('ecmwf');
  assert.deepEqual(o._carry, { t: Date.parse(A.frames[20].valid_utc), playing: false });
  await landed(w, E.run);
  assert.equal(o.frameIndex, 24, '+60 h in E'); assert.equal(E.frames[24].valid_utc, A.frames[20].valid_utc);
  done(o);
});

test('section 43: a second switch before the first lands goes back with the carried time; one landing in the end, nothing of the model in between', async () => {
  const w = world();
  const o = await twoModels(w);
  o.setModel('ecmwf');
  const nE = w.fetches.filter((u) => u.includes('/ecmwf/')).length;
  o.setModel('gfswave');
  assert.equal(o.model, 'gfswave'); assert.deepEqual(o._carry, { t: Date.parse(A.frames[10].valid_utc), playing: false }, 'kept: nothing was on the map');
  await landed(w, A.run);
  assert.equal(o.manifest.run, A.run); assert.equal(o.frameIndex, 10); assert.equal(tagOf(o), 12);
  assert.equal(w.fetches.filter((u) => u.includes('/ecmwf/')).length, nE, 'no ECMWF frame was asked for');
  assert.equal(o.cache.size() + Object.keys(o.inflight).length > 0, true);
  for (const k of [...o.cache.map.keys(), ...Object.keys(o.inflight)]) assert.ok(k.startsWith('gfswave/' + A.run + '/'), k);
  done(o);
});

test('section 43: a field switch during a pending model switch keeps the carried time (the one loading path)', async () => {
  const w = world();
  const o = await twoModels(w);
  o.setModel('ecmwf');
  o.mount('wind');
  await landed(w, E.run);
  assert.equal(o.layer.field, 'wind'); assert.equal(o.manifest.run, E.run); assert.equal(o.frameIndex, 14); assert.equal(o._carry, null);
  done(o);
});

test('section 43: a reload restore still pending when the model is switched: the saved time and play state go to the new model', async () => {
  const w = world({ storage: memStore({ field: 'hs', t: Date.parse(A.frames[10].valid_utc), at: Date.now(), playing: true }) });
  w.manifests['ecmwf/' + E.run] = E; w.pointers = { ecmwf: eptr(E) }; w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create({ models: TWO });
  o.mount('hs', true); await settle();                                        // the manifest is in, A's frame 10 is downloading
  assert.ok(o._pendingRestore && o.frameIndex === null);
  o.setModel('ecmwf');
  assert.equal(o._carry, null, 'nothing was on the map: no carry'); assert.ok(o._pendingRestore, 'the restore is still pending');
  await landed(w, E.run);
  assert.equal(o.manifest.run, E.run); assert.equal(o.frameIndex, 14); assert.equal(o.playing, true, 'resumed from the save');
  done(o);
});

test('section 43: the 30-minute run check answers for the model it started with: a newer GFS run arriving during a switch raises no banner on ECMWF', async () => {
  const w = world(); w.manifests[B.run] = B;
  const o = await twoModels(w);
  w.pointer = ptr(B);                                                         // a newer GFS run
  w.holdPointer = true; o._checkRun(); w.holdPointer = false;                 // its fetch is slow: it answers after the switch has landed
  o.setModel('ecmwf');
  await landed(w, E.run);
  assert.equal(o.manifest.run, E.run); assert.equal(w.heldPointers.length, 1);
  w.heldPointers.shift()(); await settle();
  assert.equal(o.newerRun, null, 'the old model\'s answer (a GFS run newer than the ECMWF one by its digits) is ignored');
  o._checkRun(); await settle();
  assert.equal(o.newerRun, null, 'E is current for ECMWF');
  w.pointers.ecmwf = Object.assign(eptr(E), { run: '2026092212', manifest: 'ecmwf/0p25/v1/2026092212/manifest-x.json' });
  o._checkRun(); await settle();
  assert.equal(o.newerRun && o.newerRun.run, '2026092212', 'a newer ECMWF run does');
  done(o);
});

test('section 43: a model whose pointer fails shows the error panel; switching back recovers at the carried time (Retry keeps it too)', async () => {
  const w = world();
  const o = await twoModels(w);
  w.pointers.ecmwf = { run: E.run, complete: false };                          // not a complete run
  o.setModel('ecmwf'); await settle();
  assert.equal(o.last.state, 'error'); assert.equal(o.manifest, null); assert.equal(o.layer._frame, null, 'no GFS picture under the ECMWF name');
  assert.deepEqual(o._carry, { t: Date.parse(A.frames[10].valid_utc), playing: false }, 'kept for Retry or a switch');
  o.setModel('gfswave');
  await landed(w, A.run);
  assert.equal(o.last.state, 'ready'); assert.equal(o.frameIndex, 10); assert.equal(tagOf(o), 12);
  done(o);
});

test('section 43: a switch while Off drops the kept run and is saved; the next On loads the new model; an unknown, unoffered or same key is a no-op', async () => {
  const store = memStore({}), w = world({ storage: store }); w.manifests['ecmwf/' + E.run] = E; w.pointers = { ecmwf: eptr(E) }; w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create({ models: TWO });
  o.mount('hs'); await settle(); await w.releaseAll();
  o.unmount(); assert.ok(o.manifest, 'Off keeps the run for the next On');
  o.setModel('ecmwf'); assert.equal(o.manifest, null); assert.equal(o.layer, null);
  assert.equal(store.read().model, 'ecmwf', 'saved for the tab although nothing is on the map');
  const n = w.fetches.length;
  o.setModel('ecmwf'); o.setModel('icon'); o.setModel(undefined);
  assert.equal(w.fetches.length, n, 'no-ops fetch nothing'); assert.equal(o.model, 'ecmwf');
  o.mount('hs'); await landed(w, E.run);
  assert.equal(o.manifest.run, E.run); assert.equal(tagOf(o), 0);
  o.play(); o.setModel('ecmwf'); assert.equal(o.playing, true, 'the same key does not even pause');
  const single = w.create({ models: ['gfswave'] });
  single.setModel('ecmwf'); assert.equal(single.model, 'gfswave', 'not offered: ignored');
  done(o);
});

test('section 43: the saved model is honoured only when offered; the list is known keys in order, deduplicated, never empty', () => {
  const w = world({ storage: memStore({ model: 'ecmwf' }) });
  assert.equal(w.create({ models: ['gfswave'] }).model, 'gfswave');
  assert.equal(w.create({ models: TWO }).model, 'ecmwf');
  assert.equal(w.create({ models: ['ecmwf', 'gfswave'] }).model, 'ecmwf');
  assert.deepEqual(w.create({ models: ['ecmwf', 'bogus', 'ecmwf', 'gfswave'] }).models, ['ecmwf', 'gfswave']);
  assert.deepEqual(w.create({}).models, ['gfswave']); assert.deepEqual(w.create({ models: ['x'] }).models, ['gfswave']);
  assert.equal(w.create({ models: ['ecmwf'] }).model, 'ecmwf', 'the first offered is the default');
  const v = world({ storage: memStore({ model: 'bogus' }) });
  assert.equal(v.create({ models: TWO }).model, 'gfswave');
});

test('section 43: the credit, the base and the keys follow the model; the GFS base stays the configured one byte for byte', async () => {
  const w = world();
  const o = await twoModels(w);
  assert.equal(o._modelBase(), 'https://x/gfswave/0p25/v1'); assert.match(o._attribution(), /NOAA GFS-Wave/);
  const un = []; o._unattribute = () => un.push(1);
  assert.equal(o._key(10), 'gfswave/' + A.run + '/full/hs/30');
  o.setModel('ecmwf'); await landed(w, E.run);
  assert.equal(o._modelBase(), 'https://x/ecmwf/0p25/v1'); o.coast = null;
  assert.equal(un.length, 1, 'the old credit went with the run');
  const a = o._attribution();
  assert.match(a, /ECMWF/); assert.match(a, /CC BY 4\.0/); assert.ok(!/NOAA/.test(a)); assert.ok(a.replace(/<[^>]+>/g, '').length <= 29, a);
  assert.equal(o._key(14), 'ecmwf/' + E.run + '/full/hs/42');
  const pub = o.pointer.published_utc, t = w.I.runTimes(Object.assign({}, E, { published_utc: pub }), Date.parse(pub) + 1, false, o._def().cycleMs);
  assert.equal(t.next, Math.ceil((Date.parse(pub) + 12 * 3.6e6) / 3e5) * 3e5, 'Next Update: one 12-h cycle after the publish');
  const u = w.create({ base: 'https://frames.example/other', models: TWO });
  assert.equal(u._modelBase(), 'https://frames.example/other', 'a base that is not the GFS prefix is used as given');
  u.model = 'ecmwf'; assert.equal(u._modelBase(), 'https://frames.example/other/ecmwf/0p25/v1');
  done(o);
});

test('section 43: Off during a pending model switch leaves nothing behind', async () => {
  const w = world({ fakeTimers: true, fakeIntervals: true });
  const o = await twoModels(w);
  o.setModel('ecmwf');
  o.unmount();
  assert.equal(o.layer, null); assert.equal(o._carry, null); assert.equal(o.manifest, null);
  await settle(); await w.releaseAll();
  assert.equal(o.cache.size(), 0); assert.equal(Object.keys(o.inflight).length, 0); assert.equal(w.pendingTimers(), 0, w.timerSources().join(' | '));
});

// ------------------------------- G30 fix round (asset 2.17.1) -------------------------------
test('G30-2: under reduced motion a model switch keeps the viewer\'s own playback (as Update and a field switch do); a reload restore still does not resume', async () => {
  const w = world({ reduced: true });
  const o = await twoModels(w);
  o.play(); await settle();
  assert.equal(o.playing, true);
  o.setModel('ecmwf');
  await landed(w, E.run);
  assert.equal(o.manifest.run, E.run); assert.equal(o.playing, true, 'still playing after the switch');
  done(o);
});

test('G30-3: a pointer or manifest request that answers nothing ends in the error panel (whose dropdown leads back); the tab still names the old model', async () => {
  for (const hold of ['holdPointer', 'holdManifest']) {
    const store = memStore({}), w = world({ storage: store });
    const o = await twoModels(w);
    o._loadStallMs = 40;
    w[hold] = true; o.setModel('ecmwf');
    await new Promise((r) => setTimeout(r, 90)); await settle(); w[hold] = false;
    assert.equal(o.last.state, 'error', hold); assert.equal(o.last.message, 'no answer from the frame host');
    assert.equal(store.read().model, 'gfswave', hold + ': a reload would not be trapped on ECMWF');
    o.setModel('gfswave');
    await landed(w, A.run);
    assert.equal(o.last.state, 'ready'); assert.equal(o.frameIndex, 10, hold + ': back at the carried time'); assert.equal(tagOf(o), 12);
    done(o);
  }
});

test('G30-3: the stall timer never outlives its request (a switch away, Off)', async () => {
  const w = world({ fakeTimers: true, fakeIntervals: true });
  const o = await twoModels(w);
  w.holdPointer = true; o.setModel('ecmwf'); w.holdPointer = false;
  assert.ok(w.timerSources().some((s) => /no answer from the frame host/.test(s)), 'the watchdog is armed');
  o.unmount();
  assert.ok(!w.timerSources().some((s) => /no answer from the frame host/.test(s)), 'Off cleared it');
  assert.equal(w.pendingTimers(), 0, w.timerSources().join(' | '));
});

test('G30-7: an ECMWF manifest answering after a switch back is never adopted', async () => {
  const w = world();
  const o = await twoModels(w);
  w.holdManifest = true; o.setModel('ecmwf'); await settle(); w.holdManifest = false;
  assert.equal(w.heldManifests.length, 1, 'the ECMWF manifest is in flight');
  o.setModel('gfswave');
  await landed(w, A.run);
  w.heldManifests.shift()(); await settle();
  assert.equal(o.manifest.run, A.run); assert.equal(o.model, 'gfswave'); assert.equal(tagOf(o), 12); assert.equal(o.frameIndex, 10);
  done(o);
});

test('G30-15: a round trip returns to the time the viewer chose (the 6-hourly part); once the viewer moves the time a switch starts from there', async () => {
  const w = world();
  const o = await twoModels(w);
  o.seek(51); await settle(); await w.releaseAll();                           // A +153 h = E +165 h: between ECMWF's 162 and 168
  assert.equal(o.frameIndex, 51);
  o.setModel('ecmwf'); await landed(w, E.run);
  assert.ok([162, 168].includes(E.frames[o.frameIndex].step), 'ECMWF lands on its nearest frame');
  o.setModel('gfswave'); await landed(w, A.run);
  assert.equal(o.frameIndex, 51, 'back at the chosen time, not at the ECMWF frame\'s time');
  o.seek(54); await settle(); await w.releaseAll();                           // the viewer moves the time: A +162 h = E +174 h
  o.setModel('ecmwf'); await landed(w, E.run);
  assert.equal(E.frames[o.frameIndex].step, 174);
  o.setModel('gfswave'); await landed(w, A.run);
  o.play(); await settle(); o.pause();                                        // playing moves the time too
  const at = o._anchorIndex();
  o.setModel('ecmwf');
  assert.equal(o._carry.t, Date.parse(A.frames[at].valid_utc));
  done(o);
});

test('G30-6: Update while a switch is pending lands at the carried time', async () => {
  const w = world();
  const o = await twoModels(w);
  w.pointers.ecmwf = { run: E.run, complete: false };
  o.setModel('ecmwf'); await settle();
  assert.equal(o.last.state, 'error'); assert.ok(o._carry);
  o.newerRun = eptr(E);
  o.update(); await landed(w, E.run);
  assert.equal(o.manifest.run, E.run); assert.equal(o.frameIndex, 14, 'the carried time, not "now"');
  done(o);
});

test('G30-8: a model switch on wind keeps the relief (no look change while nothing is drawn); on wave height the look follows as before', async () => {
  for (const [field, want] of [['wind', 0], ['hs', 1]]) {
    const w = world();
    const o = await twoModels(w);
    if (field === 'wind') { o.mount('wind'); await settle(); await w.releaseAll(); assert.equal(o.layer.field, 'wind'); }
    const empty = []; const s0 = o._syncLook;
    o._syncLook = function () { if (!this.layer || !this.layer.hasFrame()) empty.push(1); return s0.apply(this, arguments); };
    o.setModel('ecmwf'); await landed(w, E.run);
    assert.equal(empty.length >= want && (want === 0 ? empty.length === 0 : true), true, field + ': ' + empty.length);
    assert.equal(o.layer.field, field); assert.equal(tagOf(o), 0);
    done(o);
  }
});

test('G30-10: a switch drops the old run\'s banner, unavailable list and outage count; a hidden tab\'s playback is carried (wasPlaying)', async () => {
  const w = world(); w.manifests[B.run] = B;
  const o = await twoModels(w);
  o.newerRun = ptr(B); o.unavailable[o._key(12)] = true; o.transientFails = 2;
  o.playing = false; o.wasPlaying = true;                                     // paused by a hidden tab
  o.setModel('ecmwf');
  assert.equal(o.newerRun, null, 'no GFS banner under ECMWF'); assert.deepEqual(o.unavailable, {}); assert.equal(o.transientFails, 0);
  assert.equal(o.wasPlaying, false, 'the old run\'s visibility pause goes with it'); assert.equal(o._carry.playing, true, 'the hidden tab was playing');
  await landed(w, E.run);
  assert.equal(o.playing, true, 'a visible tab resumes the carried playback');
  assert.equal(o.newerRun, null);
  done(o);
});

test('G30-15: a step ends a run of switches too; Update starts a new one from its own landing', async () => {
  const w = world();
  const o = await twoModels(w);
  o.seek(51); await settle(); await w.releaseAll();
  o.setModel('ecmwf'); await landed(w, E.run);
  o.setModel('gfswave'); await landed(w, A.run);
  assert.equal(o.frameIndex, 51);
  o.step(1); await settle(); await w.releaseAll();                            // the viewer moves the time: A 52 (+156 h)
  assert.equal(o.frameIndex, 52);
  o.setModel('ecmwf');
  assert.equal(o._carry.t, Date.parse(A.frames[52].valid_utc), 'the stepped time, not the earlier chosen one');
  await landed(w, E.run);
  w.manifests['ecmwf/2026092212'] = Object.assign({}, E, { run: '2026092212', run_utc: '2026-09-22T12:00:00Z',
    files: Object.assign({}, E.files, { template: 'ecmwf/0p25/v1/2026092212/{res}{field}/f{step:03d}.png' }) });
  o.newerRun = Object.assign(eptr(E), { run: '2026092212', manifest: 'ecmwf/0p25/v1/2026092212/manifest-x.json' });
  const before = o._switchT;
  assert.ok(before !== null);
  o.update(); await landed(w, '2026092212');
  assert.equal(o._switchT, null, 'Update moved the time: a later switch starts from its landing');
  done(o);
});

test('G30-15: a switch back before the first lands keeps the carried playback (the live state is paused by the switch)', async () => {
  const w = world();
  const o = await twoModels(w);
  o.play(); await settle();
  o.setModel('ecmwf');
  assert.equal(o.playing, false); assert.equal(o._carry.playing, true);
  o.setModel('gfswave');
  assert.equal(o._carry.playing, true, 'still playing when it lands');
  await landed(w, A.run); await w.releaseAll();
  assert.equal(o.model, 'gfswave'); assert.equal(tagOf(o), 12); assert.equal(o.playing, true);
  done(o);
});

// ------------------------------- G30 fix round 2 (asset 2.17.3) -------------------------------
test('Update race (pre-existing, G30 re-check): a landing of the run being replaced, already decoded when Update starts, never lands over Update\'s loading panel', async () => {
  const w = world(); w.manifests[B.run] = B;
  const o = await twoModels(w);
  assert.ok(o.cache.has(o._key(11)), 'frame 11 is decoded in the ring');
  const step = o._goto(11).catch((e) => e);                                   // a step / playback landing, its frame in hand
  o.newerRun = ptr(B);
  w.holdManifest = true; o.update(); w.holdManifest = false;                  // the newer run's manifest is slow
  await settle();
  const err = await step;
  assert.equal(err && err.name, 'AbortError', 'the old run\'s landing is superseded');
  assert.equal(o.last.state, 'loading', 'Update\'s loading panel stays'); assert.equal(o.frameIndex, 10);
  w.heldManifests.shift()(); await settle(); await w.releaseAll();
  assert.equal(o.manifest.run, B.run); assert.equal(o.frameIndex, w.I.nearestIndex(B, Date.parse(A.frames[11].valid_utc)), 'the requested time');
  assert.equal(o.last.state, 'ready');
  done(o);
});

test('Update race (pre-existing): an Update whose frames never come leaves no picture of the old run (no picture without its frame and run)', async () => {
  const w = world(); w.manifests[B.run] = B;
  const o = await twoModels(w);
  for (const f of B.frames) for (const half of [false, true]) w.failNext['https://x/' + w.I.frameKey(B, f, 'hs', half)] = 404;
  o.newerRun = ptr(B);
  o.update(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.last.state, 'error'); assert.equal(o.frameIndex, null);
  assert.equal(o.layer.hasFrame(), false, 'the old run\'s picture went with the failure');
  done(o);
});

test('re-check R-2: a failed model switch on wind gives the page\'s imagery back (the basemap follows what is drawn)', async () => {
  const w = world();
  const o = await twoModels(w);
  o.mount('wind'); await settle(); await w.releaseAll();
  const looks = []; const s0 = o._syncLook;
  o._syncLook = function () { looks.push(this.layer && this.layer.hasFrame()); return s0.apply(this, arguments); };
  w.pointers.ecmwf = { run: E.run, complete: false };
  o.setModel('ecmwf'); await settle();
  assert.equal(o.last.state, 'error'); assert.equal(o.layer.hasFrame(), false);
  assert.ok(looks.length >= 1 && looks[looks.length - 1] === false, 'the look synced with nothing drawn: ' + JSON.stringify(looks));
  done(o);
});

test('re-check R-4: the chosen time is what a run of switches keeps (never a snapped frame\'s); Off ends it', async () => {
  const w = world();
  const o = await twoModels(w);
  o.seek(51); await settle(); await w.releaseAll();
  const T = Date.parse(A.frames[51].valid_utc);
  o.setModel('ecmwf'); assert.equal(o._switchT, T); await landed(w, E.run);
  o.setModel('gfswave'); assert.equal(o._switchT, T, 'still the chosen time, not the ECMWF frame\'s'); await landed(w, A.run);
  o.setModel('ecmwf'); assert.equal(o._switchT, T); for (let i = 0; i < 4 && o.frameIndex === null; i++) await landed(w, E.run);
  assert.notEqual(Date.parse(E.frames[o.frameIndex].valid_utc), T, 'ECMWF has no frame at the chosen time');
  o.unmount(); assert.equal(o._switchT, null, 'Off ends the run of switches');
  o.mount('hs'); await settle(); await w.releaseAll();
  const onMap = Date.parse(o.manifest.frames[o.frameIndex].valid_utc);
  o.setModel('gfswave');
  assert.equal(o._switchT, onMap, 'after Off/On a switch starts from the frame on the map');
  await landed(w, A.run);
  done(o);
});

test('re-check R-5: the watchdog and a switch away cancel the pointer request itself (the signal fetch was given is aborted)', async () => {
  const w = world();
  const o = await twoModels(w);
  o._loadStallMs = 40;
  w.holdPointer = true; o.setModel('ecmwf'); await settle();
  const a = w.jsonSignals.filter((x) => /ecmwf\/0p25\/v1\/latest\.json$/.test(x.url)).pop();
  assert.ok(a && a.signal && !a.signal.aborted);
  await new Promise((r) => setTimeout(r, 90)); await settle();
  assert.equal(o.last.state, 'error'); assert.equal(a.signal.aborted, true, 'the watchdog cancelled the request');
  w.holdPointer = false; o.setModel('gfswave'); await landed(w, A.run);
  w.holdPointer = true; o.setModel('ecmwf'); await settle();
  const b = w.jsonSignals.filter((x) => /ecmwf\/0p25\/v1\/latest\.json$/.test(x.url)).pop();
  assert.notEqual(b, a); assert.equal(b.signal.aborted, false);
  w.holdPointer = false; o.setModel('gfswave');
  assert.equal(b.signal.aborted, true, 'a switch away cancelled the request');
  await landed(w, A.run);
  done(o);
});

// ---- section 44: the observation (sea surface temperature) beside the frames ----
function sstFake(o, opts) {                              // the store's network replaced: pointer, manifest, frame, cells (held by hand)
  opts = opts || {};
  const st = o.sst, held = [];
  const m = { format: 'sst-v1', day: '2026-09-21', run: '20260921', complete: true, units: 'degC', range: [-2, 35],
    tier0: { key: 'sst/v1/20260921/B/t0.png', grid: GRID }, tiers: [] };
  st._json = (url) => new Promise((res, rej) => { const go = () => (opts.fail ? rej(new Error('sst 503')) : res(url.endsWith('latest.json') ? { format: 'sst-v1', complete: true, run: m.run, manifest: 'sst/v1/x/manifest.json' } : m)); if (opts.hold) held.push(go); else go(); });
  st._decode = () => Promise.resolve({ q: new Uint8Array(1440 * 721).fill(200), cols: 1440, rows: 721 });
  return { held, m };
}
test('section 44: hs -> sst -> hs: the player stops, the layer samples the store (no particles, the SST credit), the model\'s time and play state come back; nothing of the observation is saved as a time', async () => {
  const store = memStore({}), w = world({ storage: store });
  const o = await mounted(w, A, { sst: true });
  o.anim = true; o._ensureFlow();
  o.seek(10); await settle(); await w.releaseAll();
  o.play(); await settle();
  const t10 = Date.parse(A.frames[o._anchorIndex()].valid_utc);
  const fake = sstFake(o);
  o.mount('sst'); await settle();
  assert.equal(o.mode, 'obs'); assert.equal(o.field, 'sst'); assert.equal(o.playing, false); assert.equal(o.frameIndex, null);
  assert.ok(o.layer._src === o.sst && o.layer.field === 'sst' && o.layer.hasFrame());
  assert.equal(o.layer.fdef, w.I.SST_FDEF);
  assert.equal(o.flow, null, 'no particles over the observation');
  assert.deepEqual(o._carry, { t: t10, playing: true });
  assert.ok(o._attribution().indexOf('NASA MUR SST') >= 0 && o._attribution().indexOf('GFS-Wave') < 0);
  assert.equal(Object.keys(o.inflight).length, 0, 'the frames\' downloads stopped');
  const savedBefore = JSON.stringify(JSON.parse(store.getItem(w.I.SESSION_KEY)));
  o._persist(); o.pause();
  assert.equal(JSON.stringify(JSON.parse(store.getItem(w.I.SESSION_KEY))), savedBefore, 'the observation writes no time over the model\'s');
  let rechecks = 0; o.sst.recheck = () => { rechecks++; return Promise.resolve(false); };
  o._checkRun(); assert.equal(rechecks, 1, 'the 30-min check only notes a newer day');
  // back to the frames: at the carried time, playing again; the store stops (its cells stay), the layer leaves it
  const cellCtl = new AbortController(); o.sst.abort = cellCtl; o.sst.inflight['t2/20_-160'] = cellCtl; o.sst.queue.push([{}, 'x']);   // a cell still downloading
  const gotos = [], goto0 = o._goto; o._goto = function (idx, sig) { gotos.push(idx); return goto0.call(this, idx, sig); };
  o.mount('hs'); await settle();
  if (o.frameIndex === null) await w.releaseField();                         // the carried frame lands (from the kept cache, else decoded); then playback moves on
  assert.equal(o.mode, 'model'); assert.equal(o.layer._src, null); assert.equal(o.sst.onChange, null);
  assert.ok(cellCtl.signal.aborted && Object.keys(o.sst.inflight).length === 0 && o.sst.queue.length === 0, 'its downloads stop on leaving');
  assert.equal(Date.parse(A.frames[gotos[0]].valid_utc), t10, 'the first frame asked for is the carried time');
  assert.ok(o.frameIndex >= gotos[0]);
  assert.equal(o.playing, true);
  assert.equal(o.layer.field, 'hs');
  assert.equal(fake.m.run, o.sst.manifest.run, 'the day stays for the next switch');
  done(o);
});

test("section 44: a switch to the observation keeps the field the page saved (a reload comes back on it), with the model's time and play state beside it", async () => {
  const store = memStore({}), w = world({ storage: store });
  const o = await mounted(w, A, { sst: true });
  o.seek(12); await settle(); await w.releaseAll();
  o.play(); await settle();
  const before = JSON.parse(store.getItem(w.I.SESSION_KEY));
  assert.equal(before.field, 'hs'); assert.equal(before.playing, true);
  sstFake(o);
  // the page's change handler: remember('sst') runs before the mount (the module is loaded asynchronously)
  store.setItem(w.I.SESSION_KEY, JSON.stringify(Object.assign({}, before, { field: 'sst' })));
  o.mount('sst'); await settle();
  assert.equal(o.mode, 'obs');
  const after = JSON.parse(store.getItem(w.I.SESSION_KEY));
  assert.equal(after.field, 'sst', 'a reload brings the observation back');
  assert.equal(after.model, 'gfswave');
  assert.equal(typeof after.t, 'number', "the model's time stays beside it");
  done(o);
});

test('section 44: a switch away while the observation loads never lets it land; a failure takes the error panel and Retry loads it; Off from it leaves nothing', async () => {
  const w = world();
  const o = await mounted(w, A, { sst: true });
  const fake = sstFake(o, { hold: true });
  o.mount('sst'); await settle();
  assert.equal(o.last.state, 'loading');
  o.mount('hs'); await settle(); await w.releaseAll();
  for (let i = 0; i < 4; i++) { fake.held.splice(0).forEach((f) => f()); await settle(); }   // pointer, then manifest: each answer held
  assert.equal(o.mode, 'model'); assert.equal(o.layer.field, 'hs'); assert.equal(o.layer._src, null, 'the late observation did not land');
  // failure, then Retry
  o.sst.status = 'idle'; o.sst.manifest = null; o.sst.failedAt = 0;
  const bad = sstFake(o, { fail: true });
  o.mount('sst'); await settle();
  assert.equal(o.last.state, 'error'); assert.ok(/cannot be loaded/.test(o.last.message));
  assert.equal(o.layer.hasFrame(), false, 'no picture under the error');
  o.sst.status = 'idle'; o.sst.failedAt = 0; sstFake(o);
  o.mount('sst'); await settle();
  assert.equal(o.last.state, 'ready'); assert.ok(o.layer._src);
  // setModel while it is shown: the choice is kept, the model's run dropped, the picture stays
  o.models = ['gfswave', 'ecmwf'];
  o.setModel('ecmwf');
  assert.equal(o.model, 'ecmwf'); assert.equal(o.manifest, null); assert.ok(o.layer.hasFrame() && o.layer._src);
  // Off
  let aborted = 0; const ab = o.sst.abortAll.bind(o.sst); o.sst.abortAll = () => { aborted++; ab(); };
  o.unmount();
  assert.equal(o.mode, 'model'); assert.equal(o.layer, null); assert.equal(aborted, 1);
  assert.ok(o.sst.manifest, 'the day stays for the next On');
  void bad;
});

// ---- G31 fix round (plan section 44 step 7) ----
test("G31-7 / G31-16 / G31-2: a switch to SST saves the carried play state, stops the run-line timer and loads without the store's cooldown", async () => {
  const store = memStore({}), w = world({ storage: store });
  const o = await mounted(w, A, { sst: true });
  o.seek(12); await settle(); await w.releaseAll();
  o.play(); await settle();
  assert.ok(o.lineTimer, 'the model mount runs the run-line timer');
  sstFake(o);
  const loads = []; const l0 = o.sst.load.bind(o.sst); o.sst.load = (force) => { loads.push(force); return l0(force); };
  o.mount('sst'); await settle();
  const s = JSON.parse(store.getItem(w.I.SESSION_KEY));
  assert.equal(s.field, 'sst'); assert.equal(s.playing, true, 'a reload then a model field resumes playing');
  assert.equal(o.lineTimer, null);
  assert.deepEqual(loads, [true], 'a pick is a viewer action: no cooldown');
  done(o);
});

test('G31-8 / G31-9: a newer run announced by the banner stays an announcement through a visit to SST; leaving SST releases its cells', async () => {
  const w = world(); w.manifests[B.run] = B;
  const o = await mounted(w, A, { sst: true });
  w.pointer = ptr(B); o.newerRun = w.pointer;
  sstFake(o);
  o.mount('sst'); await settle();
  let released = 0; const r0 = o.sst.release.bind(o.sst); o.sst.release = () => { released++; r0(); };
  o.pointerAt = 0;                                                          // the pointer is re-read on the way back too
  o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(released, 1, 'the cells go when the observation does');
  assert.equal(o.manifest.run, A.run, 'the pinned run stays');
  assert.ok(o.newerRun && o.newerRun.run === B.run, 'and the newer one is still only announced');
  done(o);
});

test('G31-15: on a tab left open on SST the 30-min check re-renders when the day crosses the stale line (and only then)', async () => {
  const w = world();
  const o = await mounted(w, A, { sst: true });
  sstFake(o);
  o.mount('sst'); await settle();
  o.sst.recheck = () => Promise.resolve(false);
  let renders = 0; const r0 = o.render.bind(o); o.render = (st) => { renders++; return r0(st); };
  o._sstStale = false; o.sst.manifest.day = new Date(Date.now() - 2 * 86400e3).toISOString().slice(0, 10);
  o._checkRun(); assert.equal(renders, 0, 'still fresh: nothing');
  o.sst.manifest.day = new Date(Date.now() - 6 * 86400e3).toISOString().slice(0, 10);
  o._checkRun(); assert.equal(renders, 1, 'now stale: the panel is drawn again');
  done(o);
});

// ---- G31 re-check, fix round 2 ----
function reloadInto(storage) {                               // a new page in the same tab: a new world on the same sessionStorage
  const w = world({ storage });
  w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create({ sst: true });
  sstFake(o);
  return { w, o };
}
test('RC-1: play wave height -> SST -> TWO page loads on SST -> wave height: plays again at the carried time', async () => {
  const storage = memStore({}), w = world({ storage });
  const o = await mounted(w, A, { sst: true });
  o.seek(12); await settle(); await w.releaseAll(); o.play(); await settle();
  sstFake(o); o.mount('sst'); await settle();
  assert.equal(storage.read().playing, true);
  done(o);
  const r1 = reloadInto(storage); r1.o.mount('sst', true); await settle();
  assert.equal(storage.read().playing, true, 'a page loaded on SST keeps the saved play state');
  done(r1.o);
  const r2 = reloadInto(storage); r2.o.mount('sst', true); await settle();
  r2.o.mount('hs'); await settle(); await r2.w.releaseAll();
  assert.equal(r2.o.playing, true);
  assert.equal(r2.o.manifest.frames[r2.o._anchorIndex()] !== undefined, true);
  done(r2.o);
});

test('RC-2: a run pinned by the carried time and pruned while SST was up: the return fails; Retry takes the newer run at that time', async () => {
  const w = world(); w.manifests[B.run] = B;
  const o = await mounted(w, A, { sst: true });
  await w.releaseAll();
  o.seek(30); await settle();                                              // the decode of frame 30 is still pending
  const t30 = Date.parse(A.frames[30].valid_utc);
  w.pendingBitmaps.length = 0;
  sstFake(o); o.mount('sst'); await settle();
  assert.ok(o._carry);
  const key = (m, i) => `https://x/gfswave/0p25/v1/${m.run}/hs/f${String(m.frames[i].step).padStart(3, '0')}.png`;
  for (let i = 0; i < A.frames.length; i++) w.failNext[key(A, i)] = 404;   // run A pruned
  w.pointer = ptr(B); o.pointerAt = 0;
  o.mount('hs'); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.manifest.run, A.run, 'the carried time keeps its run (G31-8)');
  assert.equal(o.last.state, 'error');
  assert.equal(o.newerRun && o.newerRun.run, B.run);
  o._retry(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.manifest.run, B.run, 'Retry let the pruned run go');
  assert.notEqual(o.frameIndex, null);
  assert.equal(Math.abs(Date.parse(B.frames[o.frameIndex].valid_utc) - t30) <= 3 * 3600e3, true, 'at the carried time');
  done(o);
});


// ---- the isobars' pressure frames: the third frame kind (plan section 46) ----
const isPres = (k) => k.indexOf('/pres/') >= 0;
const presKey = (m, i) => `gfswave/0p25/v1/${m.run}/half/pres/f${String(m.frames[i].step).padStart(3, '0')}.png`;

test('isobars on under wind: the pressure frame rides beside the field and direction frames, keys and caches by kind, one more in flight, the first picture alone', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.isobars = true;
  o.mount('wind'); await settle();
  assert.ok(o.iso, 'the isobar layer exists under wind'); assert.ok(o.iso.calls.includes('attach'));
  assert.equal(o._maxInflight(), w.I.MAX_INFLIGHT + 1, 'one more download at once while the isobars ride along');
  assert.equal(o._own(), 3);
  assert.deepEqual(Object.keys(o.inflight).map((k) => k.split('/')[3]), ['wind'], 'the first picture is fetched alone');
  await w.releaseField();
  assert.ok(o.layer.hasFrame()); assert.equal(o.iso.frame, null);
  const kinds = Object.keys(o.inflight).map((k) => k.split('/')[3]).sort();
  assert.deepEqual(kinds.slice(0, 2), ['pres', 'wdir'], 'then its direction and its pressure: ' + kinds.join());
  assert.ok(Object.keys(o.inflight).length <= o._maxInflight());
  await w.releasePres();
  assert.equal(o.iso.frame, o.pcache.get(o._key(o.frameIndex, 'pres')), 'the pressure frame delivered to the layer');
  assert.equal(o.iso.entry, A.frames[o.frameIndex]); assert.equal(o.iso.fdef, A.fields.pres); assert.equal(o.iso.grid, A.grid_half);
  const plan = o._plan(o.frameIndex);
  assert.deepEqual(plan.slice(0, 3).map((p) => p[1]), ['field', 'dir', 'pres'], 'the target owns three ranks');
  for (let i = 0; i < 12; i++) { o.seek((i * 7) % 81); await tick(); assert.ok(Object.keys(o.inflight).length <= o._maxInflight(), 'inflight ' + Object.keys(o.inflight).length); }
  for (const k of o.cache.map.keys()) assert.ok(!isPres(k) && !isDir(k), 'field cache: ' + k);
  for (const k of o.pcache.map.keys()) assert.ok(k.includes('/half/pres/'), 'pressure cache: ' + k);
  assert.ok(o.pcache.size() <= w.I.MAX_DECODED);
  await w.releaseAll(); await settle();
  // the target's three fetches are the ones _trimInflight keeps room for
  o.seek(70); await tick();
  const keys = Object.keys(o.inflight);
  assert.ok(keys.includes(o._key(70)) && keys.includes(o._key(70, 'dir')) && keys.includes(o._key(70, 'pres')), 'the target field, direction and pressure in flight: ' + keys.join());
  assert.ok(keys.length <= o._maxInflight());
  const iso = o.iso;
  o.unmount();
  assert.equal(o.iso, null); assert.equal(o.pcache.size(), 0); assert.equal(Object.keys(o.inflight).length, 0); assert.ok(iso.calls.includes('detach'));
  await w.releaseAll(); assert.equal(o.pcache.size(), 0, 'late decodes resolve into the void');
});

test('a step waits for its pressure frame; a pressure frame that lands after the map moved on never reaches the layer (the newer target drops it)', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll();
  const i0 = o.frameIndex; assert.ok(o.iso.frame); assert.equal(o.iso.entry, A.frames[i0]);
  o.seek(i0 + 5); await settle();
  await w.releaseField();
  assert.equal(o.frameIndex, i0, 'waiting for the pressure of i0 + 5'); assert.ok(o.inflight[o._key(i0 + 5, 'pres')]);
  assert.equal(o.iso.entry, A.frames[i0], 'the old lines stay with the old picture meanwhile');
  const late = w.holdPres();
  o.seek(i0 + 9); await settle();
  assert.equal(o.inflight[o._key(i0 + 5, 'pres')], undefined, 'aborted');
  late(); await settle();
  assert.equal(o.pcache.has(o._key(i0 + 5, 'pres')), false, 'a late decode of an aborted fetch is dropped');
  assert.equal(o.frameIndex, i0); assert.equal(o.iso.entry, A.frames[i0]);
  await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 9); assert.equal(o.iso.entry, A.frames[i0 + 9], 'the new step shows its own isobars'); assert.equal(o.iso.frame, o.pcache.get(o._key(i0 + 9, 'pres')));
  o.unmount();
}));

test('a missing pressure frame never holds a step and is never an outage: the old lines are cleared, the wind plays on', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll();
  const i0 = o.frameIndex; assert.ok(o.iso.frame);
  w.failNext['https://x/' + presKey(A, i0 + 5)] = 404;
  for (const i of [i0 + 6, i0 + 7, i0 + 8]) w.failNext['https://x/' + presKey(A, i)] = 'network';
  o.seek(i0 + 5); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 5, 'the step landed without its pressure');
  assert.equal(o.iso.frame, null, "no lines: an older step's were cleared"); assert.ok(o.iso.calls.includes('_clear'));
  assert.equal(o.unavailable[o._key(i0 + 5, 'pres')], true, 'permanent for the session');
  for (const i of [6, 7, 8]) { o.seek(i0 + i); await settle(); await w.releaseAll(); await settle(); }
  assert.equal(o.frameIndex, i0 + 8); assert.equal(o.transientFails, 0, 'pressure failures never count toward the outage rule');
  assert.ok(!o.last || o.last.state !== 'error');
  o.unmount();
}));

test('wave height and period never fetch pressure frames; leaving wind detaches the layer and aborts its fetches; a run without the field has no isobars', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('hs'); await settle(); await w.releaseAll();
  assert.equal(o.iso, null); assert.ok(!w.fetches.some(isPres), 'no pressure fetch under wave height');
  o.mount('wind'); await settle(); await w.releaseField();
  assert.ok(o.iso && Object.keys(o.inflight).some(isPres));
  const n0 = w.fetches.length;
  o.mount('hs'); await settle();
  assert.equal(o.iso, null, 'detached'); assert.ok(!Object.keys(o.inflight).some(isPres), 'pressure fetches aborted');
  await w.releaseAll(); await settle();
  assert.ok(!w.fetches.slice(n0).some(isPres), 'no pressure fetch since the switch');
  o.unmount();
  const A0 = manifest(A.run, A.run_utc, 12, true); w.pointer = ptr(A0); w.manifests[A0.run] = A0;
  const n = w.fetches.length;
  const p = w.create(); p.isobars = true; p.mount('wind'); await settle(); await w.releaseAll();
  assert.equal(p.iso, null); assert.equal(p._wantPres(), false); assert.equal(p._own(), 1);
  assert.ok(!w.fetches.slice(n).some(isPres), 'a run without the field: nothing asked'); assert.equal(p._maxInflight(), w.I.MAX_INFLIGHT);
  p.unmount();
});

test("setIsobars(false) drops the pressure fetches and cache at once; setIsobars(true) fetches the shown step's pressure first", () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll();
  assert.ok(o.pcache.size() >= 2, 'the ring holds pressure frames'); o.seek(o.frameIndex + 10); await settle(); await w.releaseField();
  assert.ok(Object.keys(o.inflight).some(isPres), 'the target pressure in flight');
  o.setIsobars(false); await settle();
  assert.equal(o.iso, null); assert.ok(!Object.keys(o.inflight).some(isPres), 'pressure fetches aborted'); assert.equal(o.pcache.size(), 0, 'the cache emptied');
  assert.equal(o._maxInflight(), w.I.MAX_INFLIGHT, 'the cap is back to the field alone');
  await w.releaseAll();
  const before = w.fetches.length;
  o.setIsobars(true); await settle();
  assert.ok(o.iso);
  assert.equal(w.fetches.slice(before)[0], 'https://x/' + presKey(A, o.frameIndex), 'the shown step pressure first');
  await w.releaseAll(); assert.ok(o.iso.frame); assert.equal(o.iso.entry, A.frames[o.frameIndex]);
  o.unmount();
}));

test('Update clears the pressure cache and the lines; a late pressure decode of the old run never lands', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A; w.manifests[B.run] = B;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll();
  assert.ok(o.iso.frame); o.seek(o.frameIndex + 5); await settle(); await w.releaseField();
  const late = w.holdPres();
  w.pointer = ptr(B); o.newerRun = w.pointer; o.update(); await settle();
  assert.equal(o.manifest.run, B.run); assert.equal(o.pcache.size(), 0); assert.equal(o.iso.frame, null, 'cleared with the run');
  late(); await settle();
  assert.equal(o.pcache.size(), 0, "the old run's decode is dropped");
  await w.releaseAll(); await settle();
  assert.ok(o.iso.frame && o.iso.entry === B.frames[o.frameIndex], "the new run's lines");
  o.unmount();
}));

test("at shark speed the ring holds field, direction and pressure frames; the target's three are never evicted for a ring fetch", async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll();
  o.setSpeed(4); assert.equal(o._maxInflight(), w.I.MAX_INFLIGHT + 1, 'paused: the paused ring + the pressure');
  o.play(); assert.equal(o._maxInflight(), w.I.PLAY_RING[4].inflight + 1, 'shark: 4 + 1');
  for (let k = 0; k < 10; k++) {
    await new Promise((r) => setTimeout(r, 140)); await w.releaseAll(); await settle();
    assert.ok(Object.keys(o.inflight).length <= o._maxInflight(), 'in flight ' + Object.keys(o.inflight).length);
  }
  o.pause(); await settle();
  assert.ok(o.pcache.size() >= 2 && o.pcache.size() <= o._ring().decoded, 'pressure frames ride in the ring: ' + o.pcache.size());
  // a full in-flight set of the target's own three fetches: nothing is evicted for a ring fetch
  const i = o.frameIndex + 20; o.seek(i); await tick();
  const own = [o._key(i), o._key(i, 'dir'), o._key(i, 'pres')];
  for (const k of own) assert.ok(o.inflight[k], 'own fetch in flight: ' + k);
  o._startKind(i + 1, 'pres'); o._startKind(i + 1, 'dir');
  for (const k of own) assert.ok(o.inflight[k], 'own fetch kept: ' + k);
  assert.ok(Object.keys(o.inflight).length <= o._maxInflight());
  o.unmount();
});

test('pressure failures neither count toward the outage rule nor reset it (only field frames do)', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll();
  const i0 = o.frameIndex;
  // a pressure failure with the counter one short of the outage: still one short, no error panel
  o.transientFails = 2;
  w.failNext['https://x/' + presKey(A, i0 + 10)] = 'network';
  o.seek(i0 + 10); await settle();
  assert.equal(o.transientFails, 2, 'a pressure failure is not counted'); assert.ok(!o.last || o.last.state !== 'error', 'no outage from a pressure failure');
  await w.releaseField(); await settle();                                    // the field lands: that resets the counter, as any field landing does
  assert.equal(o.frameIndex, i0 + 10); assert.equal(o.transientFails, 0);
  // a pressure LANDING with the counter raised: left alone (the mutant that reset it would hide a real outage)
  o.transientFails = 2;
  o.seek(i0 + 20); await settle();
  assert.ok(o.inflight[o._key(i0 + 20, 'pres')] && o.inflight[o._key(i0 + 20)], 'field and pressure of the target in flight');
  // the TARGET's pressure decode by its URL: the first pending pressure decode can be an orphan of a ring fetch the seek aborted
  await w.releaseUrl((u) => u.endsWith(presKey(A, i0 + 20))); await settle();
  assert.ok(o.pcache.has(o._key(i0 + 20, 'pres')), 'the pressure frame landed');
  assert.equal(o.transientFails, 2, 'a pressure landing leaves the outage counter alone'); assert.equal(o.frameIndex, i0 + 10, 'the field is still held');
  o.unmount();
}));

test('the pressure frame is always the half-resolution one, keyed so: at a zoom where the wind field is full-res too', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  w.map.getZoom = () => 9;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll();
  assert.equal(o.res, 'full', 'wind at zoom 9: full-resolution field');
  const i = o.frameIndex;
  assert.ok(o._key(i).includes('/full/wind/') && o._key(i, 'pres').includes('/half/pres/'), o._key(i, 'pres'));
  const pres = w.fetches.filter(isPres);
  assert.ok(pres.length >= 1 && pres.every((u) => u.includes('/half/pres/')), 'every pressure download is the half frame: ' + pres.join());
  assert.ok(o.iso.frame && o.iso.frame.cols === 720, 'the layer holds a half frame');
  o.unmount();
});

test('a manifest whose pressure field is not published at half resolution (or is not a pressure) has no isobars', async () => {
  const w = world();
  for (const bad of [Object.assign({}, PRES, { resolutions: ['full'] }), Object.assign({}, PRES, { units: 'Pa' }), Object.assign({}, PRES, { interpolation: 'circular', circular: true })]) {
    const M = manifest(A.run, A.run_utc, 12); M.fields = Object.assign({}, M.fields, { pres: bad });
    w.pointer = ptr(M); w.manifests[M.run] = M;
    const n = w.fetches.length;
    const o = w.create(); o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll();
    assert.equal(o._presDef(), null); assert.equal(o.iso, null); assert.ok(!w.fetches.slice(n).some(isPres));
    o.unmount();
  }
});

test('_syncIsobars hands the layer only the lines of the step the LAYER shows, and clears another step\'s', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll();
  const i0 = o.frameIndex; assert.equal(o.iso.entry, A.frames[i0]);
  assert.ok(o.pcache.has(o._key(i0 + 1, 'pres')), 'the ring holds the next step\'s pressure');
  o._syncIsobars(i0 + 1);                                                    // asked for a step the layer does not show
  assert.equal(o.iso.frame, null, 'nothing of another step is delivered'); assert.equal(o.iso.entry, null, 'the shown step\'s lines were cleared');
  o._syncIsobars(i0);
  assert.equal(o.iso.entry, A.frames[i0], 'the shown step\'s lines are back');
  o.unmount();
}));

test('section 46: the particles slider: a strength fades the particles (the animator\'s opacity), 0 stops them as unticking Animation did, back above 0 fetches the shown step\'s direction; never saved', async () => {
  const storage = memStore({}), w = world({ storage }); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.mount('hs'); await settle(); await w.releaseField();
  assert.ok(o.flow && Object.keys(o.inflight).some(isDir), 'the direction of the shown step is on its way');
  o.setParticleOpacity(0.4); await settle();
  assert.equal(o.particleOpacity, 0.4); assert.equal(o.anim, true); assert.equal(o.flow.opacity, 0.4, 'the animator fades');
  assert.ok(Object.keys(o.inflight).some(isDir), 'a fade keeps the direction download');
  o.setParticleOpacity(0); await settle();
  assert.equal(o.anim, false); assert.equal(o.flow, null, 'no animator at 0');
  assert.ok(!Object.keys(o.inflight).some(isDir), 'direction fetches aborted'); assert.equal(o.dcache.size(), 0);
  const before = w.fetches.length;
  o.setParticleOpacity(0.6); await settle();
  assert.equal(o.anim, true); assert.ok(o.flow); assert.equal(o.flow.opacity, 0.6, 'a new animator takes the strength');
  assert.ok(w.fetches.slice(before).some((u) => u.includes('/half/pdir/f' + String(A.frames[o.frameIndex].step).padStart(3, '0'))), 'the shown step direction is fetched again');
  for (const bad of [NaN, -1, 7, '0.5', null, undefined]) { o.setParticleOpacity(bad); assert.ok(o.particleOpacity >= 0 && o.particleOpacity <= 1, String(bad)); }
  o.setParticleOpacity(-1); assert.equal(o.particleOpacity, 0); assert.equal(o.anim, false);
  o.setParticleOpacity(7); assert.equal(o.particleOpacity, 1); assert.equal(o.anim, true);
  o.setParticleOpacity('0.5'); assert.equal(o.particleOpacity, 1, 'only a number counts');
  assert.deepEqual(Object.keys(storage.read()).filter((k) => /partic|opacit|anim|isobar/i.test(k)), [], 'the module saves nothing of it (the page keeps it)');
  assert.equal(o.setOpacity, undefined, 'still no overlay opacity setter');
  await w.releaseAll(); o.unmount();
});

test('section 46: the page\'s settings reach the module: opts.particles 0 starts with no particles (nothing animates, no direction frame); opts.isobars false starts without isobars; the defaults are on and full strength', async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const mk = (extra) => new w.I.Overlay(w.map, Object.assign({ base: 'https://x/gfswave/0p25/v1', panel: {}, tz: 'UTC', getUnit: () => 'US', fmtTime: () => '', tzAbbr: () => '', pageCycle: () => null }, extra));
  const d = mk({}); assert.equal(d.particleOpacity, 1); assert.equal(d.anim, true); assert.equal(d.isobars, true);
  const half = mk({ particles: 0.35 }); assert.equal(half.particleOpacity, 0.35); assert.equal(half.anim, true);
  half.mount('hs'); await settle(); await w.releaseAll();
  assert.ok(half.flow, 'particles at 35 %'); assert.equal(half.flow.opacity, 0.35, 'an animator made by a mount takes the strength');
  half.mount('wind'); await settle(); await w.releaseAll();
  assert.equal(half.flow.opacity, 0.35, 'and after a field switch'); half.unmount();
  const off = mk({ particles: 0, isobars: false }); assert.equal(off.anim, false); assert.equal(off.isobars, false);
  const n0 = w.fetches.length;
  off.mount('hs'); await settle(); await w.releaseAll();
  assert.ok(off.layer.hasFrame(), 'the picture is there'); assert.equal(off.flow, null);
  assert.ok(!w.fetches.slice(n0).some(isDir), 'no direction frame was ever asked for');
  off.unmount();
});

// ---- G32 fix round ----
test('G32 (owner): a step waits at most PRES_WAIT_MS for its pressure frame, then lands without lines; the lines come when the frame does', () => onRunDay(async () => {
  const w = world({ fakeTimers: true }); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll(); await settle();
  const i0 = o.frameIndex; assert.ok(o.iso.frame); assert.equal(w.I.PRES_WAIT_MS, 1000);
  o.seek(i0 + 5); await settle();
  await w.releaseField(); await settle();
  assert.equal(o.frameIndex, i0, 'the field is in, the pressure held: waiting');
  await w.fade(999); await settle();
  assert.equal(o.frameIndex, i0, 'still waiting at 999 ms');
  await w.fade(1); await settle();
  assert.equal(o.frameIndex, i0 + 5, 'landed at 1 s without its pressure');
  assert.equal(o.iso.frame, null, "no other step's lines under the new picture");
  assert.ok(o.inflight[o._key(i0 + 5, 'pres')], 'its pressure still on its way');
  await w.releasePres(); await settle();
  assert.equal(o.iso.entry, A.frames[i0 + 5], 'the lines appear when the frame arrives');
  assert.equal(o.iso.frame, o.pcache.get(o._key(i0 + 5, 'pres')));
  // a pressure frame that comes in time: the step lands with it, the fake clock never moved (no wait for the cap)
  const t = w.now();
  o.seek(i0 + 7); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 7); assert.equal(o.iso.entry, A.frames[i0 + 7]); assert.equal(w.now(), t);
  o.unmount();
}));

test('fix round 3 (re-check S-1): the pressure wait is counted per step: a goto of the same step issued again (a resolution change) waits only the rest of the second; the record goes at the landing', () => onRunDay(async () => {
  const w = world({ fakeTimers: true });
  await onFakeClock(w, async () => {
    w.pointer = ptr(A); w.manifests[A.run] = A;
    const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll(); await settle();
    const i0 = o.frameIndex; assert.ok(o.iso.frame);
    o.seek(i0 + 5); await settle(); await w.releaseField(); await settle();
    assert.equal(o.frameIndex, i0, 'the field is in, the pressure held: waiting');
    assert.ok(o._presWait && o._presWait.key === o._key(i0 + 5, 'pres') && o._presWait.timer !== null, 'the wait record for the step');
    await w.fade(600); await settle();
    // as _checkRes does when the zoom crosses a resolution threshold: the res changes, so the first goto's own continuation
    // dies on its res check and the re-issued goto must carry the wait (2.19.5 started a new second here)
    o.res = 'full'; o._prefetch(); o._goto(i0 + 5).catch(() => {}); await settle();
    const step5 = 'f' + String(A.frames[i0 + 5].step).padStart(3, '0') + '.png';
    await w.releaseUrl((u) => /\/wind\//.test(u) && !/\/half\//.test(u) && u.endsWith(step5)); await settle();
    assert.equal(o.frameIndex, i0, 'the full-res field is in too, the pressure still held');
    await w.fade(399); await settle();
    assert.equal(o.frameIndex, i0, 'still waiting at 999 ms after the FIRST goto');
    await w.fade(1); await settle();
    assert.equal(o.frameIndex, i0 + 5, 'landed 1 s after the first goto (2.19.5: 1.6 s, the second goto started a new second)');
    assert.equal(o._presWait, null, 'the record is spent at the landing');
    await w.releasePres(); await settle();
    assert.equal(o.iso.entry, A.frames[i0 + 5], 'the lines come with the frame');
    // another step starts its own second (its own field frame released: the ring's frames are pending too)
    o.seek(i0 + 9); await settle();
    const step = 'f' + String(A.frames[i0 + 9].step).padStart(3, '0') + '.png';
    await w.releaseUrl((u) => /\/wind\//.test(u) && u.endsWith(step)); await settle();
    assert.equal(o._presWait.key, o._key(i0 + 9, 'pres'));
    await w.fade(999); await settle(); assert.equal(o.frameIndex, i0 + 5);
    await w.fade(1); await settle(); assert.equal(o.frameIndex, i0 + 9);
    o.unmount();
  });
}));

test('fix round 3 (R1 S3): Off while a step waits for its pressure leaves no cap timer behind', () => onRunDay(async () => {
  const w = world({ fakeTimers: true, fakeIntervals: true }); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll(); await settle();
  const i0 = o.frameIndex;
  o.seek(i0 + 5); await settle(); await w.releaseField(); await settle();
  assert.ok(o._presWait && o._presWait.timer !== null, 'waiting with a timer');
  o.unmount(); await settle();
  assert.equal(o._presWait, null);
  assert.equal(w.pendingTimers(), 0, 'no timer of any kind left after Off: ' + w.timerSources().join(' | '));
  await w.fade(5000); await settle();
  assert.equal(o.iso, null); assert.equal(o.layer, null);
}));

test('fix round 3 (re-check H-2): the readout says "<= 920" / ">= 1060" within half a code of the stored range\'s ends (a typhoon\'s clamped eye), else the rounded value; nothing from another step\'s frame', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll(); await settle();
  const half = (1060 - 920) / 254 / 2;
  o.iso.valueAt = () => 1003.6; assert.equal(o._hpaText(21, -158), '1004');
  o.iso.valueAt = () => 920; assert.equal(o._hpaText(21, -158), '\u2264 920');
  o.iso.valueAt = () => 920 + half - 0.01; assert.equal(o._hpaText(21, -158), '\u2264 920');
  o.iso.valueAt = () => 920 + half + 0.01; assert.equal(o._hpaText(21, -158), '920', 'past half a code: a value');
  o.iso.valueAt = () => 1060 - half + 0.01; assert.equal(o._hpaText(21, -158), '\u2265 1060');
  o.iso.valueAt = () => 1059.5; assert.equal(o._hpaText(21, -158), '1060', 'not quite at the top: the rounded value');
  o.iso.entry = A.frames[(o.frameIndex + 4) % A.frames.length]; assert.equal(o._hpaText(21, -158), null, "another step's frame: none");
  o.iso.entry = o.layer.entry; o.iso.valueAt = () => null; assert.equal(o._hpaText(21, -158), null);
  o.unmount();
}));

test('fix round 3 (re-check H-1): the isobar layer asks for the terrain asset once per page (never through the frame scheduler) and keeps it; a layer made later takes it from the module; a failed load leaves the marks without it and is asked again', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.iso); assert.equal(o.iso.terrain, null, 'no terrain yet');
  assert.equal(w.pendingTerrain.length, 1, 'one request for terrain_half.png');
  assert.ok(!w.fetches.some((u) => /terrain_half/.test(u)) && Object.keys(o.inflight).every((k) => !/terrain/.test(k)), 'outside the frame scheduler');
  await w.releaseTerrain(); await settle();
  assert.ok(o.iso.terrain instanceof Uint8Array && o.iso.terrain.length === 720 * 361, 'the half grid');
  const first = o.iso;
  o.mount('hs'); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.iso, null, 'no layer under wave height');
  o.mount('wind'); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.iso && o.iso !== first);
  assert.equal(w.pendingTerrain.length, 0, 'not asked again');
  assert.ok(o.iso.terrain && o.iso.terrain.length === 720 * 361, 'the module keeps it for every later layer');
  o.unmount();
  // a failed load: no terrain, the next layer asks again
  const w3 = world(); w3.pointer = ptr(A); w3.manifests[A.run] = A; w3.terrainAnswer = { ok: false, status: 503 };
  const o3 = w3.create(); o3.anim = false; o3.isobars = true;
  o3.mount('wind'); await settle(); await w3.releaseAll(); await settle();
  assert.equal(o3.iso.terrain, null, 'a 503: no terrain'); assert.equal(w3.pendingTerrain.length, 0);
  o3.mount('hs'); await settle(); await w3.releaseAll(); await settle();
  w3.terrainAnswer = null;
  o3.mount('wind'); await settle(); await w3.releaseAll(); await settle();
  assert.equal(w3.pendingTerrain.length, 1, 'asked again after a failure');
  await w3.releaseTerrain(); await settle();
  assert.ok(o3.iso.terrain && o3.iso.terrain.length === 720 * 361);
  o3.unmount();
}));

test('G32 (S2): a step whose field frame cannot be shown asks for none of its extras (pressure, direction) when it is the target or in the ring', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = true; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll(); await settle();
  const i0 = o.frameIndex, bad = i0 + 4, n0 = w.fetches.length;              // outside the first ring (i0 +- 2)
  assert.ok(!w.fetches.some((u) => u.indexOf(presKey(A, bad)) >= 0), 'not fetched yet');
  o.unavailable[o._key(bad)] = true;                                       // the state a 404 of its field frame leaves
  o.seek(i0 + 3); await settle(); await w.releaseAll(); await settle();     // the ring around i0 + 3 holds i0 + 4
  o.seek(bad); await settle(); await w.releaseAll(); await settle();
  const pk = presKey(A, bad), step = String(A.frames[bad].step).padStart(3, '0');
  assert.ok(!w.fetches.slice(n0).some((u) => u.indexOf(pk) >= 0), 'no pressure download for it');
  assert.ok(!w.fetches.slice(n0).some((u) => /\/wdir\//.test(u) && u.indexOf('/f' + step + '.png') >= 0), 'no direction download for it');
  assert.ok(w.fetches.slice(n0).some((u) => isPres(u)), 'the other steps still bring their pressure');
  o.unmount();
}));

test('G32 (S7): what a step waits for settles at the abort of its fetch, not at a decode that cannot be cancelled', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll(); await settle();
  const i = o.frameIndex + 3;
  o._startKind(i, 'pres'); await settle();
  const rec = o.inflight[o._key(i, 'pres')]; assert.ok(rec);
  let done = false; o._kindReady(i, 'pres').then(() => { done = true; });
  await settle(); assert.equal(done, false, 'waiting for the held decode');
  rec.abort.abort(); delete o.inflight[o._key(i, 'pres')];
  await settle(); assert.equal(done, true, 'settled at the abort');
  o.unmount(); await w.releaseAll();
}));

test('G32 (S17): the readout\'s pressure only from the pressure frame of the step on the map', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll(); await settle();
  o.iso.valueAt = () => 1003.6;
  assert.equal(o._hpaAt(21, -158), 1004, 'rounded to the hPa');
  o.iso.entry = A.frames[(o.frameIndex + 4) % A.frames.length];
  assert.equal(o._hpaAt(21, -158), null, "another step's frame: none");
  o.iso.entry = o.layer.entry; o.iso.valueAt = () => null;
  assert.equal(o._hpaAt(21, -158), null, 'outside the grid: none');
  o.iso.frame = null; assert.equal(o._hpaAt(21, -158), null);
  o.unmount();
}));

test('G32 (A-P1-1 / B-5): wind -> sea surface temperature leaves no isobar layer and no pressure frames; Update to a run with pressure draws the shown step\'s lines (A-P2-1)', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll(); await settle();
  const iso = o.iso; assert.ok(iso && iso.frame && o.pcache.size() > 0);
  o.sst = { load: () => new Promise(() => {}), idle() {}, abortAll() {} };  // an SST store that never answers: the switch itself is what counts
  o.mount('sst'); await settle();
  assert.equal(o.iso, null, 'the isobar layer detached'); assert.ok(iso.calls.includes('detach'));
  assert.equal(o.pcache.size(), 0, 'its frames dropped');
  o.unmount();
  // Update: a run without pressure -> a run with it
  const A0 = manifest(A.run, A.run_utc, 12, true); w.pointer = ptr(A0); w.manifests[A0.run] = A0;
  const p = w.create(); p.anim = false; p.isobars = true; p.mount('wind'); await settle(); await w.releaseAll(); await settle();
  assert.equal(p.iso, null, 'no pressure in the first run: no layer');
  w.pointer = ptr(B); w.manifests[B.run] = B; p.newerRun = w.pointer;
  p.update(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(p.manifest, B); assert.ok(p.iso, 'the new run has pressure: the layer is made');
  assert.equal(p.iso.entry, B.frames[p.frameIndex], "the shown step's lines");
  p.unmount();
}));

test('G32: a failed load with no frame of the run on the map clears the lines too', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A;
  const o = w.create(); o.anim = false; o.isobars = true; o.mount('wind'); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.iso.frame);
  o.frameIndex = null;                                                     // what an Update or a switch leaves before its first frame
  o.layer.clear(); assert.equal(o.layer.hasFrame(), false); assert.ok(o.iso.frame, 'the lines are still there');
  o._fail(null, new Error('frames cannot be loaded right now'));
  assert.equal(o.iso.frame, null); assert.ok(o.iso.calls.includes('_clear'));
  o.unmount();
}));

// ---- the Great Lakes frames in the scheduler (plan section 50, step 4) ----
// The lake run LA starts an hour after A (the 13Z cycle): A's frames +3 h .. +150 h have a lake frame, +0 h and the
// 3-hourly frames past +150 h (index 51 on) have none.
const LA = lakeManifest('2026092213', '2026-09-22T13:00:00Z'), LB = lakeManifest('2026092219', '2026-09-22T19:00:00Z');
const isLake = (k) => k.indexOf('glwu/') === 0;
// opts: extra (create options), field, anim (the animation on before the mount), manifest (the model run, default A)
async function lakeMounted(w, opts) {
  opts = opts || {};
  w.pointer = ptr(A); w.manifests[A.run] = opts.manifest || A; w.lakePointer = lptr(LA); w.lakeManifests[LA.run] = LA;
  const o = w.create(Object.assign({ glwu: true }, opts.extra || {}));
  if (opts.anim) o.anim = true;
  o.mount(opts.field || 'hs'); await settle();
  return o;
}
const TPH = { lo: 1, hi: 30, legend: [4, 22], units: 's', interpolation: 'bilinear' };

test('section 50 (step 6): the lakes credit comes with the lakes coverage: a lake frame drawn before the lakes set lands is not credited; the set landing credits it at once', () => onRunDay(async () => {
  const w = world();
  const LIDX = { format: 'coast-v1', q: 10000, kind: 'lakes', tier0: { file: 'lakes-i.bin', cell: 30, max_zoom: 6 }, tier1: { dir: 'f', cell: 5, min_zoom: 7, cells: {} } };
  w.coastAnswer = (url) => url.endsWith('/lakes-v1/index.json') ? { ok: true, status: 200, json: () => Promise.resolve(LIDX) } : coastOk(url);
  const o = await lakeMounted(w, {}), credits = [];
  o.layer.lakeWaterInView = () => true;                                      // lake water in view (that rule: the G33 re-check P3-5 test)
  o._attribute = function () { credits.push(this._attribution()); };      // the harness stubs the map control: record what each call credits
  await w.releaseField(); await w.releaseLake(); await settle();
  assert.ok(o.layer._lake, 'the lake frame is set'); assert.equal(o.layer._lakeMask, false, 'the lakes set has not landed');
  assert.ok(credits.length > 0 && credits.every((c) => !/GLWU/.test(c)), 'no lakes credit without their coverage: ' + JSON.stringify(credits));
  assert.ok(w.pendingCoast.length > 0, 'the lakes set is still on its way');
  while (w.pendingCoast.length) w.pendingCoast.shift()();
  await settle();
  assert.equal(o.layer._lakeMask, true, 'the lakes coverage is on the layer');
  assert.ok(/GLWU/.test(credits[credits.length - 1]), 'credited as soon as the set lands: ' + JSON.stringify(credits));
}));
const holdUrl = (w, tail) => { const i = w.pendingBitmaps.findIndex((f) => f.blob.url.endsWith(tail)); assert.ok(i >= 0, 'a decode of ' + tail + ' is pending'); return w.pendingBitmaps.splice(i, 1)[0]; };
const lakeFile = (m, i, field) => `glwu/v1/${LA.run}/${field || 'hs'}/f${String(m.frames[i].step - 1).padStart(3, '0')}.png`;

test('section 50: under GFS wave height the lake frames ride beside the field and direction frames: keys and caches by kind, one more in flight each, the first picture alone, the landing draws them', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true });
  assert.ok(o.lake && o.lakeCoast, 'the lake store and the lakes coast set exist with opts.glwu');
  assert.equal(o.lakeCoast.file, "lakes-i.bin", "the lakes set's own overview (step 6: world-i.bin was asked for)");
  const lakeCoastUrls = w.fetches.filter((u) => u.indexOf('lakes-v1') >= 0);
  assert.ok(lakeCoastUrls.some((u) => u.endsWith('/lakes-v1/lakes-i.bin')) && !lakeCoastUrls.some((u) => u.endsWith('world-i.bin')), lakeCoastUrls.join());
  assert.equal(o.lake.status, 'ok'); assert.equal(o.lake.manifest.run, LA.run);
  assert.equal(o._wantLake(), true); assert.equal(o._wantLakeDir(), true);
  assert.equal(o._maxInflight(), w.I.MAX_INFLIGHT + 2, 'one more download at once per lake kind');
  assert.deepEqual(Object.keys(o.inflight).map((k) => k.split('/')[3]), ['hs'], 'the first picture is fetched alone');
  await w.releaseField();
  const i0 = o.frameIndex; assert.ok(o.layer.hasFrame()); assert.ok(o._lakeIndex(i0) >= 0, 'the first frame has a lake frame');
  assert.equal(o.layer._lake, null, 'the first picture never waited for its lake');
  const keys = Object.keys(o.inflight);
  assert.ok(keys.includes(o._key(i0, 'dir')) && keys.includes(o._key(i0, 'lake')) && keys.includes(o._key(i0, 'lakedir')), 'then its direction and its lake frames: ' + keys.join());
  assert.ok(keys.length <= o._maxInflight());
  assert.equal(o._key(i0, 'lake'), 'glwu/' + LA.run + '/lake/hs/' + (A.frames[i0].step - 1)); assert.equal(o._key(i0, 'lakedir'), 'glwu/' + LA.run + '/lakedir/pdir/' + (A.frames[i0].step - 1));
  await w.releaseLake();
  assert.ok(o.layer._lake && o.layer._lake.frame === o.lcache.get(o._key(i0, 'lake')) && o.layer._lake.entry === A.frames[i0], 'the lake frame delivered to the layer');
  assert.equal(o.layer._lake.grid, LAKE_GRID); assert.equal(o.layer._lake.lo, HS.lo); assert.equal(o.layer._lake.hi, HS.hi);
  await w.releaseLakeDir(); await w.releaseDir(); await settle();
  assert.equal(o.flow.ldir, o.ldcache.get(o._key(i0, 'lakedir')), 'the lake direction delivered to the animator'); assert.equal(o.flow.lentry, A.frames[i0]); assert.equal(o.flow.lframe, o.layer._lake.frame);
  assert.equal(o._own(), 4);
  assert.deepEqual(o._plan(i0).slice(0, 4).map((p) => p[1]), ['field', 'dir', 'lake', 'lakedir'], 'the target owns four ranks');
  for (let i = 0; i < 12; i++) { o.seek(1 + (i * 7) % 49); await tick(); assert.ok(Object.keys(o.inflight).length <= o._maxInflight(), 'inflight ' + Object.keys(o.inflight).length); }
  for (const k of o.cache.map.keys()) assert.ok(!isLake(k) && !isDir(k), 'field cache: ' + k);
  for (const k of o.lcache.map.keys()) assert.ok(k.indexOf('glwu/' + LA.run + '/lake/hs/') === 0, 'lake cache: ' + k);
  for (const k of o.ldcache.map.keys()) assert.ok(k.indexOf('glwu/' + LA.run + '/lakedir/pdir/') === 0, 'lake direction cache: ' + k);
  assert.ok(o.lcache.size() <= w.I.MAX_DECODED && o.ldcache.size() <= w.I.MAX_DECODED);
  await w.releaseAll(); await settle();
  o.seek(40); await tick();
  const k40 = Object.keys(o.inflight);
  assert.ok(k40.includes(o._key(40)) && k40.includes(o._key(40, 'dir')) && k40.includes(o._key(40, 'lake')) && k40.includes(o._key(40, 'lakedir')), 'the target\'s four fetches in flight: ' + k40.join());
  assert.ok(k40.length <= o._maxInflight());
  await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, 40); assert.equal(o.layer._lake.entry, A.frames[40]); assert.equal(o.flow.lentry, A.frames[40]);
  o.unmount();
  assert.equal(o.lcache.size(), 0); assert.equal(o.ldcache.size(), 0); assert.equal(Object.keys(o.inflight).length, 0); assert.equal(o.flow, null);
  assert.equal(o.lake.status, 'ok', 'the lake run stays for the next On');
  await w.releaseAll(); assert.equal(o.lcache.size(), 0, 'late decodes resolve into the void');
}));

test('section 50: a step waits for its lake frame at most PRES_WAIT_MS, then lands bare; the lake joins when the frame arrives; a lake frame that lands after the map moved on never reaches the layer', () => onRunDay(async () => {
  const w = world({ fakeTimers: true }); const o = await lakeMounted(w); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const i0 = o.frameIndex; assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[i0]);
  o.seek(i0 + 5); await settle();
  await w.releaseField(); await settle();
  assert.equal(o.frameIndex, i0, 'the field is in, the lake held: waiting'); assert.ok(o.inflight[o._key(i0 + 5, 'lake')]);
  assert.equal(o.layer._lake.entry, A.frames[i0], 'the old lake stays with the old picture meanwhile');
  await w.fade(999); await settle();
  assert.equal(o.frameIndex, i0, 'still waiting at 999 ms');
  await w.fade(1); await settle();
  assert.equal(o.frameIndex, i0 + 5, 'landed at 1 s without its lake');
  assert.equal(o.layer._lake, null, 'no other step\'s lake under the new picture');
  assert.ok(o.inflight[o._key(i0 + 5, 'lake')], 'its lake frame still on its way');
  await w.releaseLake(); await settle();
  assert.equal(o.layer._lake.entry, A.frames[i0 + 5], 'the lake appears when the frame arrives'); assert.equal(o.layer._lake.frame, o.lcache.get(o._key(i0 + 5, 'lake')));
  // a lake frame that comes in time: the step lands with it, the clock never moved
  const t = w.now();
  o.seek(i0 + 7); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 7); assert.equal(o.layer._lake.entry, A.frames[i0 + 7]); assert.equal(w.now(), t);
  // a late lake frame of a step the map left (a step beyond everything the ring decoded; the next target four steps on: outside its ring) never lands
  o.seek(i0 + 20); await settle(); holdUrl(w, `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i0 + 20].step).padStart(3, '0')}.png`)(); await settle();
  assert.equal(o.frameIndex, i0 + 7, 'waiting for the lake of i0 + 20');
  const late = holdUrl(w, lakeFile(A, i0 + 20));
  o.seek(i0 + 24); await settle();
  assert.equal(o.inflight[o._key(i0 + 20, 'lake')], undefined, 'aborted');
  late(); await settle();
  assert.equal(o.lcache.has(o._key(i0 + 20, 'lake')), false, 'a late decode of an aborted fetch is dropped');
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 24); assert.equal(o.layer._lake.entry, A.frames[i0 + 24]);
  o.unmount();
}));

test('section 50: a missing lake frame never holds a step and is never an outage (the lake stays blank for it); frames without a lake time plan and fetch no lake kinds', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const i0 = o.frameIndex; assert.ok(o.layer._lake);
  const lakeKey = (i) => `glwu/v1/${LA.run}/hs/f${String(A.frames[i].step - 1).padStart(3, '0')}.png`;
  w.failNext['https://x/' + lakeKey(i0 + 5)] = 404;
  for (const i of [i0 + 6, i0 + 7, i0 + 8]) w.failNext['https://x/' + lakeKey(i)] = 'network';
  o.seek(i0 + 5); await settle(); await w.release(1); await settle();                // the step lands; the ring's first lake fetch (a network failure) fails before any other landing
  assert.equal(o.frameIndex, i0 + 5, 'the step landed without its lake'); assert.equal(o.layer._lake, null);
  assert.equal(o.unavailable[o._key(i0 + 5, 'lake')], true, 'permanent for the session');
  assert.equal(o.transientFails, 0, "the ring's failed lake fetch (network) never counts toward the outage rule");
  assert.equal(typeof o.unavailable[o._key(i0 + 6, 'lake')], 'number', 'a cooldown for the failed ring frame');
  await w.releaseAll(); await settle();
  for (const i of [6, 7, 8]) { o.seek(i0 + i); await settle(); await w.releaseAll(); await settle(); assert.equal(o.transientFails, 0, 'lake failures never count toward the outage rule (step ' + i + ')'); assert.equal(o.frameIndex, i0 + i); }
  assert.ok(!o.last || o.last.state !== 'error');
  // +180 h: past the lake run's horizon (nothing in its ring either); +0 h: before it (its neighbours have lake frames)
  for (const idx of [60, 0]) {
    const before = w.fetches.length;
    o.seek(idx); await settle(); await w.releaseAll(); await settle();
    assert.equal(o.frameIndex, idx); assert.equal(o._lakeIndex(idx), -1); assert.equal(o.layer._lake, null);
    assert.ok(o._plan(idx).every((p) => p[0] !== idx || (p[1] !== 'lake' && p[1] !== 'lakedir')), 'no lake kinds planned for ' + idx);
    assert.equal(o._key(idx, 'lake'), 'glwu/-'); assert.equal(o._own(idx), 1);
    if (idx === 60) assert.ok(w.fetches.slice(before).every((u) => u.indexOf('/glwu/') < 0), 'past the horizon: no lake frame asked for');
  }
  assert.ok(w.fetches.every((u) => !/\/glwu\/v1\/.*\/f1[5-9][0-9]\.png$/.test(u) && !/\/glwu\/v1\/.*\/f-/.test(u)), 'no lake frame past +149 h (or before +0 h) was ever asked for');
  o.unmount();
}));

test('section 50: wind never fetches lake frames; period does; ECMWF never (the mask alone); a model switch and Off drop the lake caches and the drawn lake; the lakes coast set is given to a new layer', () => onRunDay(async () => {
  const w = world(); w.pointers = { gfswave: ptr(A), ecmwf: ptr(B) }; w.manifests[B.run] = B;
  const AT = Object.assign({}, A, { fields: Object.assign({}, A.fields, { tp: TPH }) });                 // the runs with a period field
  w.manifests[B.run] = Object.assign({}, B, { fields: Object.assign({}, B.fields, { tp: TPH }) });
  const o = await lakeMounted(w, { extra: { models: ['gfswave', 'ecmwf'] }, manifest: AT }); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.lcache.size() > 0);
  const n0 = w.fetches.length;
  o.mount('wind'); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o._wantLake(), false); assert.equal(o.layer._lake, null, 'the field switch cleared the lake');
  assert.ok(w.fetches.slice(n0).every((u) => u.indexOf('/glwu/') < 0), 'wind: no lake frames');
  assert.equal(o._maxInflight(), w.I.MAX_INFLIGHT);
  o.mount('tp'); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o._wantLake(), true); assert.ok(o.layer._lake && o.layer._lake.lo === 1 && o.layer._lake.hi === 30, 'the period\'s lake frame in the period\'s code space');
  assert.ok(o.lcache.size() > 0 && [...o.lcache.map.keys()].every((k) => k.indexOf('/lake/tp/') > 0), 'the period\'s lake frames in the cache');
  o.setModel('ecmwf'); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.model, 'ecmwf'); assert.ok(o.frameIndex !== null && o.manifest.run === B.run, 'ECMWF\'s frame on the map');
  assert.ok(o._lakeIndex(o.frameIndex) >= 0, 'the lake run covers this time, yet'); assert.equal(o._wantLake(), false); assert.equal(o.layer._lake, null); assert.equal(o.lcache.size(), 0); assert.equal(o.ldcache.size(), 0);
  const n1 = w.fetches.length;
  o.seek(o.frameIndex + 3); await settle(); await w.releaseAll(); await settle();
  assert.ok(w.fetches.slice(n1).every((u) => u.indexOf('/glwu/') < 0), 'ECMWF: no lake frames');
  o.setModel('gfswave'); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o._wantLake(), true); assert.ok(o.layer._lake, 'back under GFS the lake is drawn again');
  const layer1 = o.layer, lakeCoast = o.lakeCoast;
  o.unmount();
  assert.equal(o.lcache.size(), 0); assert.equal(o.ldcache.size(), 0);
  o.mount('hs'); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.notEqual(o.layer, layer1); assert.ok(o.layer._lake, 'a new layer after Off draws the lake again');
  assert.equal(o.lakeCoast, lakeCoast);
  o.unmount();
}));

test('section 50: setAnim(false) aborts the lake direction fetches and empties their cache, the lake field frame stays; setAnim(true) fetches the shown step\'s lake direction', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true });
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const i0 = o.frameIndex; assert.ok(o.flow && o.flow.ldir && o.ldcache.size() > 0 && o.layer._lake);
  o.seek(i0 + 3); await tick();
  assert.ok(Object.keys(o.inflight).some((k) => k.indexOf('/lakedir/') > 0), 'a lake direction in flight');
  o.setAnim(false);
  assert.equal(o.flow, null); assert.equal(o.ldcache.size(), 0); assert.ok(Object.keys(o.inflight).every((k) => k.indexOf('/lakedir/') < 0 && !isDir(k)), 'lake direction fetches aborted');
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 3); assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[i0 + 3], 'the lake field frame still rides');
  assert.equal(o._wantLakeDir(), false); assert.equal(o._own(), 2);
  o.setAnim(true); await settle();
  assert.ok(Object.keys(o.inflight).includes(o._key(i0 + 3, 'lakedir')), 'the shown step\'s lake direction is fetched');
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.flow.lentry, A.frames[i0 + 3]);
  o.unmount();
}));

test('section 50: a lake run that loads after the first frame is on the map is fetched for the shown frame at once; without a lake run (404) nothing is asked and the lakes stay blank', () => onRunDay(async () => {
  const w = world(); w.holdLakePointer = true;
  const o = await lakeMounted(w); await w.releaseAll(); await settle();
  assert.ok(o.layer.hasFrame()); assert.equal(o.lake.status, 'loading'); assert.equal(o._wantLake(), false); assert.equal(o.layer._lake, null);
  assert.ok(Object.keys(o.inflight).every((k) => !isLake(k)));
  w.heldLakePointers.shift()(); await settle(); await settle();
  assert.equal(o.lake.status, 'ok'); assert.equal(o._wantLake(), true);
  assert.ok(o.inflight[o._key(o.frameIndex, 'lake')], 'the shown frame\'s lake frame asked for as soon as the run is known');
  await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[o.frameIndex]);
  o.unmount();
  const w2 = world(); w2.pointer = ptr(A); w2.manifests[A.run] = A;                  // no lake pointer: 404
  const o2 = w2.create({ glwu: true }); o2.mount('hs'); await settle(); await w2.releaseAll(); await settle(); await w2.releaseAll(); await settle();
  assert.equal(o2.lake.status, 'failed'); assert.equal(o2._wantLake(), false); assert.equal(o2.layer._lake, null);
  assert.ok(w2.fetches.every((u) => u.indexOf('/glwu/') < 0 || /latest\.json$/.test(u)), 'nothing but the pointer');
  o2.unmount();
  // opts.glwu off: no store, no requests, the budgets unchanged
  const w3 = world(); w3.pointer = ptr(A); w3.manifests[A.run] = A; w3.lakePointer = lptr(LA); w3.lakeManifests[LA.run] = LA;
  const o3 = w3.create(); o3.mount('hs'); await settle(); await w3.releaseAll(); await settle();
  assert.equal(o3.lake, null); assert.equal(o3.lakeCoast, null); assert.equal(o3._wantLake(), false); assert.equal(o3._maxInflight(), w3.I.MAX_INFLIGHT);
  assert.ok(w3.fetches.every((u) => u.indexOf('/glwu/') < 0 && u.indexOf('lakes-v1') < 0));
  o3.unmount();
}));

test('section 50: a newer lake run noted by the run check takes over at the next step (its old fetches aborted, caches afresh), never under the picture; the drawn lake stays until that step lands', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const i0 = o.frameIndex; const drawnA = o.layer._lake; assert.ok(drawnA);
  w.lakePointer = lptr(LB); w.lakeManifests[LB.run] = LB;
  o._checkRun(); await settle(); await settle();
  assert.ok(o.lake.next && o.lake.next.m.run === LB.run, 'the newer run is noted'); assert.equal(o.lake.manifest.run, LA.run, '... not adopted by the check');
  assert.equal(o.layer._lake, drawnA, 'the picture keeps its lake');
  o.seek(i0 + 6); await tick();
  assert.equal(o.lake.manifest.run, LB.run, 'adopted at the next step'); assert.equal(o.lake.next, null);
  assert.equal(o.lcache.size(), 0, 'the old run\'s cached lake frames went with it'); assert.equal(o.ldcache.size(), 0);
  assert.ok(Object.keys(o.inflight).every((k) => !isLake(k) || k.indexOf('glwu/' + LB.run + '/') === 0), 'only the new run\'s lake fetches in flight: ' + Object.keys(o.inflight).join());
  assert.ok(o.inflight[o._key(i0 + 6, 'lake')], 'the target\'s lake frame from the new run');
  assert.equal(o.layer._lake, drawnA, 'the old lake stays drawn until the step lands');
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 6); assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[i0 + 6] && o.layer._lake.frame === o.lcache.get('glwu/' + LB.run + '/lake/hs/' + (A.frames[i0 + 6].step - 7)));
  for (const k of o.lcache.map.keys()) assert.ok(k.indexOf('glwu/' + LB.run + '/') === 0, 'no old-run frame in the cache: ' + k);
  // A's first frames fall before LB's start: no lake there now
  o.seek(1); await settle(); await w.releaseAll(); await settle();
  assert.equal(o._lakeIndex(1), -1); assert.equal(o.layer._lake, null);
  o.unmount();
}));

test('section 50: a lake run taken by the store\'s own re-read (a mount\'s load) never lets the old run\'s decode in flight land; a lake run in another code space is never drawn', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const i0 = o.frameIndex;
  o.seek(i0 + 20); await settle(); holdUrl(w, `gfswave/0p25/v1/${A.run}/hs/f${String(A.frames[i0 + 20].step).padStart(3, '0')}.png`)(); await settle();
  const late = holdUrl(w, lakeFile(A, i0 + 20));
  w.lakePointer = lptr(LB); w.lakeManifests[LB.run] = LB; o.lake.loadedAt = 0;
  o._loadLakes(o.manifest); await settle(); await settle();                        // a mount's load: the re-read finds the newer run
  assert.equal(o.lake.manifest.run, LB.run, "the re-read's newer run is taken at once, through the adoption path (G33)"); assert.equal(o.lake.next, null);
  late(); await settle();
  assert.equal(o.lcache.has('glwu/' + LA.run + '/lake/hs/' + (A.frames[i0 + 20].step - 1)), false, 'the old run\'s decode is dropped');
  assert.equal(o.frameIndex, i0 + 20, 'the dropped decode let the step land bare'); assert.equal(o.layer._lake, null);
  assert.ok(o.inflight[o._key(i0 + 20, 'lake')] && o._key(i0 + 20, 'lake').indexOf('glwu/' + LB.run + '/') === 0, 'its lake frame asked for from the new run');
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[i0 + 20] && o.layer._lake.frame === o.lcache.get(o._key(i0 + 20, 'lake')), 'the new run\'s lake drawn for the step');
  o.unmount();
  // the lake run's wave height in another code space than the model's: never wanted, never fetched
  const w2 = world(); w2.pointer = ptr(A); w2.manifests[A.run] = A; w2.lakePointer = lptr(LA);
  w2.lakeManifests[LA.run] = Object.assign({}, LA, { fields: Object.assign({}, LA.fields, { hs: Object.assign({}, LA.fields.hs, { hi: 22.86 }) }) });
  const o2 = w2.create({ glwu: true }); o2.mount('hs'); await settle(); await w2.releaseAll(); await settle(); await w2.releaseAll(); await settle();
  assert.equal(o2.lake.status, 'ok'); assert.equal(o2._wantLake(), false); assert.equal(o2.layer._lake, null); assert.equal(o2._maxInflight(), w2.I.MAX_INFLIGHT);
  assert.ok(w2.fetches.every((u) => u.indexOf('/glwu/') < 0 || !/\.png$/.test(u)), 'no lake frame asked for');
  o2.unmount();
}));

test('section 50: the landing sets the step\'s lake before its particles: one animator rebuild per step with everything cached; Update clears the lake caches', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true });
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const i0 = o.frameIndex;
  for (const i of [i0 + 1, i0 + 2]) { o.seek(i); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); }
  const before = o.flow.calls.filter((c) => c === '_rebuild').length, changes = [], realSetLake = o.layer.setLake;
  o.layer.setLake = function (data, quiet) { const r = realSetLake.call(this, data, quiet); changes.push(r); return r; };
  o.seek(i0 + 1); await settle();                                                  // everything cached: no fetch, one landing
  assert.equal(o.frameIndex, i0 + 1);
  assert.equal(o.flow.calls.filter((c) => c === '_rebuild').length - before, 1, 'one rebuild for the step');
  assert.ok(changes.every((r) => r === false), 'the lake frame rode in setFrame: nothing for setLake to change after the landing (' + changes.join() + ')');
  o.layer.setLake = realSetLake;
  assert.equal(o.flow.entry, A.frames[i0 + 1]); assert.equal(o.flow.lentry, A.frames[i0 + 1]); assert.equal(o.flow.lframe, o.layer._lake.frame);
  // Update to a newer model run: the lake caches go with the model's; the new run's steps find their lake frames by time
  w.manifests[B.run] = B; o.newerRun = ptr(B);
  o.update(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.manifest.run, B.run); assert.ok(o.layer._lake && o.layer._lake.entry === B.frames[o.frameIndex], 'the new run\'s step draws its lake');
  for (const k of o.lcache.map.keys()) assert.ok(k.indexOf('glwu/' + LA.run + '/lake/hs/') === 0);
  o.unmount();
}));

const isLakeFrameUrl = (u) => /\/glwu\/v1\/\d{10}\/(hs|tp|pdir)\//.test(u);
test('section 50 (step 6b): the lake frames are fetched only while the lakes are in or near the view: nothing of them off the lakes; a pan onto them asks for the shown step\'s at once and the ring follows; a pan away drops the fetches and empties the lake caches; a world copy counts', () => onRunDay(async () => {
  const w = world(); w.view = { lat: 21.3, lng: -157.9 };                      // Hawaii
  const o = await lakeMounted(w, { anim: true }); w.orig._bindMap.call(o);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.lake.status, 'ok', 'the lake run is read (its pointer and manifest only)');
  assert.equal(o._wantLake(), false); assert.equal(o._wantLakeDir(), false); assert.equal(o._maxInflight(), w.I.MAX_INFLIGHT, 'no extra download slot');
  assert.ok(!w.fetches.some(isLakeFrameUrl), 'no lake frame asked for off the lakes: ' + w.fetches.filter(isLakeFrameUrl).join());
  assert.equal(o.layer._lake, null); assert.equal(o.flow.ldir, null);
  const i0 = o.frameIndex; assert.ok(o._lakeIndex(i0) >= 0, 'the step would have a lake frame');
  assert.deepEqual(o._plan(i0).slice(0, 2).map((p) => p[1]), ['field', 'dir'], 'the plan holds no lake kind');
  assert.equal(w.listeners('map', 'moveend'), 1); assert.equal(w.listeners('map', 'zoomend'), 1); assert.equal(w.listeners('map', 'viewreset'), 1);
  // onto the lakes: the shown step's lake frames start at once
  w.setView(44.8, -84.2); await settle();
  assert.equal(o._wantLake(), true); assert.equal(o._maxInflight(), w.I.MAX_INFLIGHT + 2);
  const keys = Object.keys(o.inflight);
  assert.ok(keys.includes(o._key(i0, 'lake')) && keys.includes(o._key(i0, 'lakedir')), 'the shown step\'s lake frames: ' + keys.join());
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[i0], 'the lake drawn under the shown step'); assert.equal(o.flow.lentry, A.frames[i0]);
  assert.ok(o.lcache.size() > 1 && o.ldcache.size() > 1, 'the ring follows: ' + o.lcache.size() + ' / ' + o.ldcache.size());
  // away again: every lake fetch aborted, the caches emptied, the picture's lake and the animator's gone
  o.seek(i0 + 1); await settle(); await w.releaseField(); await settle();
  assert.equal(o.frameIndex, i0 + 1); const before = w.fetches.filter(isLakeFrameUrl).length;
  assert.ok(Object.keys(o.inflight).some(isLake), 'a lake fetch of the ring in flight');
  w.setView(21.3, -157.9); await settle();
  assert.equal(o._wantLake(), false);
  assert.ok(!Object.keys(o.inflight).some(isLake), 'lake fetches aborted: ' + Object.keys(o.inflight).join());
  assert.equal(o.lcache.size(), 0); assert.equal(o.ldcache.size(), 0);
  assert.equal(o.layer._lake, null); assert.equal(o.flow.ldir, null); assert.equal(o.flow.lentry, null);
  o.seek(i0 + 3); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, i0 + 3); assert.equal(o.layer._lake, null);
  assert.equal(w.fetches.filter(isLakeFrameUrl).length, before, 'no lake frame for the next steps');
  // the same view again: nothing to do (no event-driven churn)
  const n1 = w.fetches.length; w.fire('map', 'moveend'); w.fire('map', 'zoomend'); await settle(); assert.equal(w.fetches.length, n1);
  // a world copy of the lakes counts
  w.setView(44.8, -84.2 - 360); await settle(); assert.equal(o._wantLake(), true);
  assert.ok(Object.keys(o.inflight).includes(o._key(i0 + 3, 'lake')), 'the shown step\'s lake frame from the copy');
  o.unmount(); assert.equal(o._lakeNear, null);
}));

test('section 50 (step 6b): a step waiting for its lake frames lands at once when the lakes leave the view (the wait settles on the aborted fetch, the clock never moves); back on the lakes the shown step\'s lake frames are asked for', () => onRunDay(async () => {
  const w = world({ fakeTimers: true }); const o = await lakeMounted(w); w.orig._bindMap.call(o);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const i0 = o.frameIndex; assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[i0]);
  o.seek(i0 + 5); await settle(); await w.releaseField(); await settle();
  assert.equal(o.frameIndex, i0, 'the field is in, the lake held: waiting'); assert.ok(o.inflight[o._key(i0 + 5, 'lake')]);
  const t = w.now();
  w.setView(21.3, -157.9); await settle();
  assert.equal(o.frameIndex, i0 + 5, 'landed at once without its lake'); assert.equal(w.now(), t);
  assert.equal(o.layer._lake, null); assert.equal(o.lcache.size(), 0); assert.ok(!o.inflight[o._key(i0 + 5, 'lake')]);
  assert.equal(o._lakeWait, null, 'no lake wait left over');
  w.setView(44.8, -84.2); await settle();
  assert.ok(o.inflight[o._key(i0 + 5, 'lake')], 'the shown step\'s lake frame asked for again');
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[i0 + 5], 'the lake joins the picture'); assert.equal(w.now(), t);
  o.unmount();
}));

// ---- G33 (plan section 50 step 8): the lake state machine's fixes and the reviewer's test gaps ----
const pad3 = (n) => String(n).padStart(3, '0');
const LAKES_IDX = { format: 'coast-v1', q: 10000, kind: 'lakes', tier0: { file: 'lakes-i.bin', cell: 30, max_zoom: 6 }, tier1: { dir: 'f', cell: 5, min_zoom: 7, cells: {} } };
const lakesCoastOk = (url) => url.endsWith('/lakes-v1/index.json') ? { ok: true, status: 200, json: () => Promise.resolve(LAKES_IDX) } : coastOk(url);
const lakeFetches = (o) => Object.keys(o.inflight).filter(isLake);

test('G33 F1: the view gate acts while a step is pending (an Update’s first landing): leaving the lakes aborts the target’s lake fetches and drops the drawn lake and the animator’s lake data at once; their late decodes are never cached', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true }); w.orig._bindMap.call(o);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.flow.ldir, 'the step drawn with its lake and lake direction');
  w.manifests[B.run] = B; o.newerRun = ptr(B); o.update(); await settle();
  assert.equal(o.frameIndex, null); assert.ok(lakeFetches(o).length > 0, 'the Update target’s lake fetches started');
  w.setView(21.3, -157.9); await settle();
  assert.deepEqual(lakeFetches(o), [], 'no lake fetch left out of reach');
  assert.ok(Object.keys(o.inflight).length <= o._maxInflight(), 'in flight within the cap');
  assert.equal(o.layer._lake, null, 'the old picture’s lake dropped'); assert.equal(o.flow.ldir, null, 'the animator’s lake data dropped');
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.lcache.size(), 0); assert.equal(o.ldcache.size(), 0, 'the late lake decodes were never cached');
  assert.equal(o.manifest, B); assert.ok(o.frameIndex !== null, 'the new run’s step landed');
  o.unmount();
}));

test('G33 F1: on the error panel of a failed Update, leaving the lakes aborts the pending target’s lake fetches (no wait left running, nothing cached out of reach)', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true }); w.orig._bindMap.call(o);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const t = Date.parse(A.frames[o.frameIndex].valid_utc); let best = 0;
  for (let i = 0; i < B.frames.length; i++) if (Math.abs(Date.parse(B.frames[i].valid_utc) - t) < Math.abs(Date.parse(B.frames[best].valid_utc) - t)) best = i;
  w.manifests[B.run] = B; o.newerRun = ptr(B);
  w.failNext[`https://x/gfswave/0p25/v1/${B.run}/hs/f${pad3(B.frames[best].step)}.png`] = 404;
  o.update(); await settle(); await settle();
  assert.equal(o.frameIndex, null); assert.ok(o.last && o.last.state === 'error', 'the error panel: ' + JSON.stringify(o.last));
  w.setView(21.3, -157.9); await settle();
  assert.deepEqual(lakeFetches(o), []); assert.ok(!o._lakeWait || o._lakeWait.timer === null, 'no lake wait timer left running');
  await w.releaseAll(); await settle();
  assert.equal(o.lcache.size(), 0); assert.equal(o.ldcache.size(), 0);
  o.unmount();
}));

test('G33 F1b: a zoom that changes the resolution AND takes the lakes out of reach while a newer lake run waits: the resolution change’s goto adopts the run, and the gate still drops the old lake at once', () => onRunDay(async () => {
  const w = world(); w.view = { lat: -40, lng: 150 }; w.map.getZoom = () => 3;
  const o = await lakeMounted(w, { anim: true }); w.orig._bindMap.call(o);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.res, 'half'); assert.ok(o._wantLake() && o.layer._lake, 'zoom 3: the lakes lie within one screen of the Pacific view');
  w.lakePointer = lptr(LB); w.lakeManifests[LB.run] = LB; o._checkRun(); await settle(); await settle();
  assert.ok(o.lake.next && o.lake.next.m.run === LB.run, 'a newer lake run noted');
  w.map.getZoom = () => 6; w.fire('map', 'zoomend'); w.fire('map', 'moveend'); await settle();
  assert.equal(o.res, 'full'); assert.equal(o.lake.manifest.run, LB.run, 'adopted by the resolution change’s goto');
  assert.equal(o._wantLake(), false);
  assert.equal(o.layer._lake, null, 'the old run’s lake dropped at once'); assert.equal(o.flow.ldir, null); assert.deepEqual(lakeFetches(o), []);
  o.unmount();
}));

test('G33 F2: after a lake-run adoption the new run’s lake direction of the shown step never reaches the animator beside the old run’s lake heights; it joins with the new run’s height frame', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true });
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const S = o.frameIndex, runTag = (f) => f.q[0];                                   // the harness tags every decoded frame with its run's hour
  assert.equal(runTag(o.layer._lake.frame), 13); assert.equal(runTag(o.flow.ldir), 13);
  w.lakePointer = lptr(LB); w.lakeManifests[LB.run] = LB; o._checkRun(); await settle(); await settle();
  o.seek(S + 1); await settle();
  assert.equal(o.lake.manifest.run, LB.run, 'adopted'); assert.equal(o.frameIndex, S); assert.equal(runTag(o.layer._lake.frame), 13, 'the old run’s lake stays drawn until the step lands');
  o._ensureLakeDir(S); await settle();                                             // the ring’s fetch of the shown step’s direction from the new run
  await w.releaseUrl((u) => u.indexOf(`/glwu/v1/${LB.run}/pdir/f${pad3(LB.frames[o._lakeIndex(S)].step)}`) >= 0);
  assert.ok(o.flow.ldir === null || runTag(o.flow.ldir) === runTag(o.layer._lake.frame), 'never two lake runs in the animator');
  assert.ok(o.ldcache.has(o._key(S, 'lakedir')), 'the direction is cached for the new run’s height frame');
  o._ensureLake(S); await settle();
  await w.releaseUrl((u) => u.indexOf(`/glwu/v1/${LB.run}/hs/f${pad3(LB.frames[o._lakeIndex(S)].step)}`) >= 0);
  assert.equal(runTag(o.layer._lake.frame), 19, 'the new run’s height frame drawn for the step'); assert.equal(runTag(o.flow.ldir), 19, '... and its direction with it');
  o.unmount();
}));

test('G33 F3: a mount 30 minutes on (Retry, the same field) keeps the drawn lake while the store re-reads its pointer; a newer run found by the re-read is taken through the adoption path', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const drawn = o.layer._lake; assert.ok(drawn);
  o.lake.loadedAt = 0; w.holdLakePointer = true;
  o.mount('hs'); await settle();
  assert.equal(o.lake.status, 'ok', 'the run in hand stays usable'); assert.equal(o._wantLake(), true);
  await w.releaseAll(); await settle();
  assert.ok(o.layer._lake, 'the same frame landed with its lake (never a blank lake on Retry)');
  w.lakePointer = lptr(LB); w.lakeManifests[LB.run] = LB;
  w.heldLakePointers.shift()(); w.holdLakePointer = false; await settle(); await settle();
  assert.equal(o.lake.manifest.run, LB.run, 'the re-read’s newer run adopted'); assert.equal(o.lake.next, null);
  assert.ok(lakeFetches(o).every((k) => k.indexOf('glwu/' + LB.run + '/') === 0), 'only the new run’s lake fetches: ' + lakeFetches(o).join());
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.layer._lake.run === LB.run, 'the new run’s lake drawn');
  o.unmount();
}));

test('G33 B-1: a lake run that failed at the mount is asked again at a landing once its cooldown has passed (never within it); the lakes then join', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A; w.lakePointer = null;          // the pointer 404s
  const o = w.create({ glwu: true }); o.mount('hs'); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.lake.status, 'failed'); assert.equal(o.layer._lake, null);
  w.lakePointer = lptr(LA); w.lakeManifests[LA.run] = LA;
  const asked = () => w.fetches.filter((u) => /\/glwu\/v1\/latest\.json$/.test(u)).length, n0 = asked();
  o.seek(o.frameIndex + 1); await settle(); await w.releaseAll(); await settle();
  assert.equal(asked(), n0, 'within the cooldown: not asked'); assert.equal(o.lake.status, 'failed');
  o.lake.failedAt = Date.now() - o.lake.retryMs - 1;
  o.seek(o.frameIndex + 1); await settle(); await w.releaseAll(); await settle();
  assert.equal(asked(), n0 + 1, 'asked again at the landing'); assert.equal(o.lake.status, 'ok');
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[o.frameIndex], 'the shown step’s lake drawn');
  o.unmount();
}));

test('G33 B-1: with the lakes coast set failed no lake frame is fetched (it could not be drawn); a landing after its cooldown asks for the set again, and its arrival starts the shown step’s lake frames', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w);
  while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle();                         // the harness answers 500
  assert.equal(o.lakeCoast.status, 'failed'); assert.equal(o._wantLake(), false);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.frameIndex !== null); assert.ok(!w.fetches.some(isLakeFrameUrl), 'no lake frame fetched while the set has failed');
  w.coastAnswer = lakesCoastOk; o.lakeCoast.failedAt = Date.now() - o.lakeCoast.retryMs - 1;
  o.seek(o.frameIndex + 1); await settle(); await w.releaseAll(); await settle();
  assert.ok(w.pendingCoast.length > 0, 'the set asked for again at the landing');
  while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle();
  assert.equal(o.lakeCoast.status, 'ok'); assert.equal(o.layer._lakeMask, true); assert.equal(o._wantLake(), true);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[o.frameIndex], 'the shown step’s lake drawn once the set is in');
  o.unmount();
}));

test('G33 B-2: under peak period the hover’s word fetches the shown step’s lake height frame once (never drawn), then says "calm" where the height is under 0.2 m', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { field: 'tp', manifest: Object.assign({}, A, { fields: Object.assign({}, A.fields, { tp: TPH }) }) });
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake); o.layer.lakeCodeAt = () => 1;                         // the sampled height: code 1 (0 m)
  const S = o.frameIndex, hsUrl = `/glwu/v1/${LA.run}/hs/f${pad3(LA.frames[o._lakeIndex(S)].step)}.png`;
  let ready = 0;
  assert.equal(o._lakeWord(45, -87, 7, () => ready++), '…');
  assert.equal(w.fetches.filter((u) => u.endsWith(hsUrl)).length, 1, 'the step’s lake height frame asked for');
  await settle(); await w.releaseUrl((u) => u.endsWith(hsUrl));
  assert.equal(ready, 1, 'the readout is told when it lands'); assert.equal(o._lakeWord(45, -87, 7, null), 'calm');
  o._lakeWord(45, -87, 7, null); assert.equal(w.fetches.filter((u) => u.endsWith(hsUrl)).length, 1, 'once per step');
  assert.ok(o.layer._lake.frame !== o._lakeHs.frame, 'never drawn'); assert.equal(o.lcache.has('glwu/' + LA.run + '/lake/hs/' + LA.frames[o._lakeIndex(S)].step), false, 'nor cached as a lake frame');
  o.unmount(); assert.equal(o._lakeHs, null);
}));

test('G33 B-5: the lakes credit shows only while the drawn lake frame’s water is on screen, not within the fetch margin alone; re-judged on every move', () => onRunDay(async () => {
  const w = world(); w.coastAnswer = lakesCoastOk;
  const o = await lakeMounted(w); w.orig._bindMap.call(o);
  while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle();
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.layer._lakeMask, true); assert.ok(o.layer._lake);
  plantLakeTile(w, o, 44.8, -84.2);                                                 // lake water at the view's centre
  const credits = []; o._attributed = 'x'; o._attribute = function () { credits.push(this._attribution()); };
  assert.ok(/GLWU/.test(o._attribution()), 'on the lakes: credited');
  w.setView(44.8, -54.2); await settle();                                          // the view's west edge ~67 W: the lakes off screen, within one screen
  assert.equal(o._wantLake(), true, 'still within the fetch margin');
  assert.ok(credits.length && !/GLWU/.test(credits[credits.length - 1]), 'off screen: no lake credit ' + JSON.stringify(credits));
  w.setView(44.8, -84.2); await settle();
  assert.ok(/GLWU/.test(credits[credits.length - 1]), 'back on the lakes: credited again');
  o.unmount();
}));

test('G33 test gaps G2: the lake wait is counted per step across a resolution change (a re-issued goto of the same step waits only the rest of the second)', () => {
  const w = world({ fakeTimers: true });
  return onFakeClock(w, async () => {
    const o = await lakeMounted(w); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
    const S = o.frameIndex, T = S + 5;
    o.seek(T); await settle();
    await w.releaseUrl((u) => u.endsWith(`gfswave/0p25/v1/${A.run}/hs/f${pad3(A.frames[T].step)}.png`));
    assert.equal(o.frameIndex, S); assert.equal(o.target, T); assert.ok(o._lakeWait, 'waiting for its held lake frames');
    await w.fade(600); await settle();
    o.res = 'half'; o._goto(T).catch(() => {}); await settle();                    // the resolution change re-issues the goto
    await w.releaseUrl((u) => u.endsWith(`gfswave/0p25/v1/${A.run}/half/hs/f${pad3(A.frames[T].step)}.png`));
    assert.equal(o.frameIndex, S, 'still waiting for the lake');
    await w.fade(401); await settle();
    assert.equal(o.frameIndex, T, 'landed at 1,001 ms, not 1,600');
    o.unmount();
  });
});

test('G33 test gaps G6: a lake landing never resets the outage counter (the field frames own it)', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const S = o.frameIndex, url = (i) => `https://x/gfswave/0p25/v1/${A.run}/hs/f${pad3(A.frames[i].step)}.png`;
  for (const i of [S + 20, S + 30]) { w.failNext[url(i)] = 'network'; o.seek(i); await settle(); await settle(); }
  assert.equal(o.transientFails, 2);
  o.seek(S + 1); await settle();
  let released = 0;
  for (let i = w.pendingBitmaps.length - 1; i >= 0; i--) if (/\/glwu\/v1\//.test(w.pendingBitmaps[i].blob.url)) { w.pendingBitmaps.splice(i, 1)[0](); released++; }
  await settle(); await settle();
  assert.ok(released > 0 && o.lcache.size() + o.ldcache.size() > 0, 'lake frames landed'); assert.equal(o.transientFails, 2, 'the counter untouched');
  o.unmount();
}));

test('G33 test gaps G18: at shark speed the lake caches follow the ring: no lake frame is downloaded twice', () => {
  const w = world({ fakeTimers: true, raf: true });
  return onFakeClock(w, async () => {
    const o = await lakeMounted(w); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
    o.setSpeed(4); o.play(); await settle();
    for (let k = 0; k < 60; k++) { await w.fade(125); await w.releaseAll(); await w.paint(); await settle(); }
    const lake = w.fetches.filter(isLakeFrameUrl), seen = {}; let dup = 0;
    for (const u of lake) { if (seen[u]) dup++; seen[u] = true; }
    assert.ok(o.frameIndex > 20 && lake.length > 40, lake.length + ' lake fetches, frame ' + o.frameIndex);
    assert.equal(dup, 0, 'no lake frame fetched twice');
    o.pause(); o.unmount();
  });
});

test('G33 test gaps G21 / P7: with a canvas on the animator a cached step lands with ONE rebuild (the lake rides in it; setLakeData never rebuilds for another step’s data)', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true });
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const S = o.frameIndex;
  for (const i of [S + 1, S + 2]) { o.seek(i); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); }
  assert.ok(o.lcache.size() >= 3 && o.ldcache.size() >= 3);
  o.flow.canvas = {};                                                               // the harness animator has none: setLakeData's rebuild condition made reachable
  const before = o.flow.calls.filter((c) => c === '_rebuild').length;
  o.seek(S + 1); await settle(); await settle();
  assert.equal(o.frameIndex, S + 1); assert.equal(o.flow.calls.filter((c) => c === '_rebuild').length - before, 1, 'one rebuild');
  o.unmount();
}));

test('G33 B-1: when the lakes coast set fails with the ring in flight, the lake share of the download cap goes with the lake fetches (in flight never above the new cap)', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true });
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  o.seek(50); await settle();                                                        // the last step with a lake frame: the ring ahead has none
  await w.releaseUrl((u) => u.endsWith(`gfswave/0p25/v1/${A.run}/hs/f${pad3(A.frames[50].step)}.png`));
  await w.releaseUrl((u) => u.indexOf(`/pdir/f${pad3(A.frames[50].step)}`) >= 0 && u.indexOf('/glwu/') < 0);
  for (let i = 0; i < 3 && o.frameIndex !== 50; i++) { const j = w.pendingBitmaps.findIndex((f) => /\/glwu\//.test(f.blob.url)); if (j < 0) break; w.pendingBitmaps.splice(j, 1)[0](); await settle(); }
  const capBefore = o._maxInflight();
  assert.ok(capBefore > w.I.MAX_INFLIGHT && Object.keys(o.inflight).length > w.I.MAX_INFLIGHT, 'the lake share in use: ' + Object.keys(o.inflight).join());
  while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle();            // the lakes coast set fails (the harness answers 500)
  assert.equal(o.lakeCoast.status, 'failed'); assert.equal(o._maxInflight(), w.I.MAX_INFLIGHT);
  assert.deepEqual(lakeFetches(o), []);
  assert.ok(Object.keys(o.inflight).length <= o._maxInflight(), 'trimmed to the new cap: ' + Object.keys(o.inflight).join());
  o.unmount();
}));

// ---- G33 re-check (plan section 50, fix round 2, overlay asset 2.21.6) ----
// a tile of the view at the layer's tile zoom with lake coverage (default: all water): the credit counts lake water in view
function plantLakeTile(w, o, lat, lng, mask, z) {
  z = z === undefined ? 6 : z;
  const p = w.I.worldXY(lng, lat), n = Math.pow(2, z), x = Math.floor(p[0] * n / 256), y = Math.floor(p[1] * n / 256);
  const el = { _ovLake: mask || new Uint8Array(65536).fill(255) };
  o.layer._tiles[x + ':' + y + ':' + z] = { el, coords: { x, y, z } };
  return { x, y, el };
}
const adoptionsOf = (o) => { const list = [], real = o._adoptLake; o._adoptLake = function () { const r = real.call(this); if (r) list.push(this.lake.manifest.run); return r; }; return list; };

test('G33 re-check P3-1: a lake run noted by the run check and taken by a mount\'s landing is adopted once; the mount\'s re-read never puts it back (its answer judged against the run in hand when it answers)', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true });
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const adopted = adoptionsOf(o);
  w.lakePointer = lptr(LB); w.lakeManifests[LB.run] = LB; o.lake.recheck(); await settle(); await settle();
  assert.ok(o.lake.next && o.lake.next.m.run === LB.run, 'the run check noted the newer run');
  w.holdLakePointer = true;                                                    // the mount's re-read: a slow pointer
  o.mount('hs'); await settle();
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.deepEqual(adopted, [LB.run], 'adopted by the landing');
  assert.ok(o.layer._lake && o.layer._lake.run === LB.run, 'the new run\'s lake drawn');
  const drawn = o.layer._lake.frame, cached = o.lcache.size(), lakeFrames = () => w.fetches.filter(isLakeFrameUrl).length, n0 = lakeFrames();
  const manifests = () => w.fetches.filter((u) => u.indexOf('/glwu/v1/' + LB.run + '/manifest') >= 0).length, m0 = manifests();
  w.heldLakePointers.shift()(); w.holdLakePointer = false; await settle(); await settle();   // the re-read answers the run now in hand
  assert.equal(manifests(), m0, 'its manifest not asked for again (judged at the pointer)');
  assert.deepEqual(adopted, [LB.run], 'never adopted a second time'); assert.equal(o.lake.next, null);
  assert.equal(o.layer._lake && o.layer._lake.frame, drawn, 'the drawn lake stays'); assert.equal(o.lcache.size(), cached, 'the lake cache kept');
  await w.releaseAll(); await settle();
  assert.equal(lakeFrames(), n0, 'no lake frame downloaded again');
  // the store refuses a `next` that is not newer than the run in hand (and forgets it); the scheduler then drops nothing
  o.lake.next = { ptr: lptr(LA), m: LA };
  assert.equal(o._adoptLake(), false); assert.equal(o.lake.next, null); assert.equal(o.lake.manifest.run, LB.run);
  assert.equal(o.lcache.size(), cached, 'nothing emptied for a refused adoption');
  o.unmount();
}));

test('G33 re-check P3-2: the lake callback re-judges the reach at once: a re-read answering during a pending Update leaves the gate set, and the next move out of reach drops the lake fetches, the drawn lake and the caches', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true }); w.orig._bindMap.call(o);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  o.lake.loadedAt = 0; w.holdLakePointer = true;
  o.mount('hs'); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.lake.status === 'ok' && w.heldLakePointers.length === 1, 'landed with its lake while the re-read is out');
  w.manifests[B.run] = B; o.newerRun = ptr(B); o.update(); await settle(); await settle();
  assert.equal(o.frameIndex, null); assert.ok(lakeFetches(o).length > 0, 'the Update target\'s lake fetches started');
  w.heldLakePointers.shift()(); w.holdLakePointer = false; await settle(); await settle();
  assert.equal(o._lakeGateNear, true, 'the reach judged at once by the callback (never left unset)');
  w.setView(21.3, -157.9); await settle();
  assert.deepEqual(lakeFetches(o), [], 'no lake fetch left out of reach');
  assert.ok(Object.keys(o.inflight).length <= o._maxInflight(), 'in flight within the cap: ' + Object.keys(o.inflight).join());
  assert.equal(o.layer._lake, null, 'the old picture\'s lake dropped');
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.lcache.size() + o.ldcache.size(), 0, 'nothing of the lakes cached out of reach');
  o.unmount();
}));

test('G33 re-check P3-3: leaving the lakes under wind or under sea surface temperature drops the lake frames cached under wave height (nothing kept out of reach, whatever the field or mode)', () => onRunDay(async () => {
  for (const field of ['wind', 'sst']) {
    const w = world(); const o = await lakeMounted(w, { anim: true, extra: field === 'sst' ? { sst: true } : {} }); w.orig._bindMap.call(o);
    if (field === 'sst') sstFake(o);
    await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
    assert.ok(o.lcache.size() > 0 && o.ldcache.size() > 0, field + ': lake frames cached under wave height');
    o.mount(field); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
    assert.ok(o.lcache.size() > 0, field + ': kept on the lakes (a switch back is instant)');
    w.setView(21.3, -157.9); await settle();
    assert.equal(o.lcache.size(), 0, field + ': lake cache emptied out of reach'); assert.equal(o.ldcache.size(), 0);
    assert.deepEqual(lakeFetches(o), []); assert.equal(o.layer && o.layer._lake, null);
    o.unmount();
  }
}));

test('G33 re-check R03: leaving the lakes with the ring in flight trims the downloads to the smaller cap (the lake share goes with the lake fetches)', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true }); w.orig._bindMap.call(o);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  o.seek(50); await settle();                                                        // the last step with a lake frame: the ring ahead has none
  await w.releaseUrl((u) => u.endsWith(`gfswave/0p25/v1/${A.run}/hs/f${pad3(A.frames[50].step)}.png`));
  await w.releaseUrl((u) => u.indexOf(`/pdir/f${pad3(A.frames[50].step)}`) >= 0 && u.indexOf('/glwu/') < 0);
  for (let i = 0; i < 3 && o.frameIndex !== 50; i++) { const j = w.pendingBitmaps.findIndex((f) => /\/glwu\//.test(f.blob.url)); if (j < 0) break; w.pendingBitmaps.splice(j, 1)[0](); await settle(); }
  assert.ok(o._maxInflight() > w.I.MAX_INFLIGHT && Object.keys(o.inflight).length > w.I.MAX_INFLIGHT, 'the lake share in use: ' + Object.keys(o.inflight).join());
  w.setView(21.3, -157.9); await settle();
  assert.equal(o._maxInflight(), w.I.MAX_INFLIGHT); assert.deepEqual(lakeFetches(o), []);
  assert.ok(Object.keys(o.inflight).length <= o._maxInflight(), 'trimmed to the new cap: ' + Object.keys(o.inflight).join());
  o.unmount();
}));

test('G33 re-check (fix round 2, found by the fuzz): leaving the lakes with nothing of them held still trims the downloads to the smaller cap', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { anim: true }); w.orig._bindMap.call(o);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  o.seek(50); await settle();
  await w.releaseUrl((u) => u.endsWith(`gfswave/0p25/v1/${A.run}/hs/f${pad3(A.frames[50].step)}.png`));
  await w.releaseUrl((u) => u.indexOf(`/pdir/f${pad3(A.frames[50].step)}`) >= 0 && u.indexOf('/glwu/') < 0);
  for (let i = 0; i < 3 && o.frameIndex !== 50; i++) { const j = w.pendingBitmaps.findIndex((f) => /\/glwu\//.test(f.blob.url)); if (j < 0) break; w.pendingBitmaps.splice(j, 1)[0](); await settle(); }
  const real = o._prefetch; o._prefetch = function () {}; o._dropLakes(); o._prefetch = real;   // nothing of the lakes held, the ring as it was
  assert.equal(o._lakeHeld(), false);
  assert.ok(Object.keys(o.inflight).length > w.I.MAX_INFLIGHT, 'the larger cap in use: ' + Object.keys(o.inflight).join());
  w.setView(21.3, -157.9); await settle();
  assert.ok(Object.keys(o.inflight).length <= o._maxInflight() && o._maxInflight() === w.I.MAX_INFLIGHT, 'trimmed: ' + Object.keys(o.inflight).join());
  o.unmount();
}));

test('G33 re-check R13: a move while the lake run\'s FIRST load is out (no grid yet) never keeps the lakes away: they come with the run', () => onRunDay(async () => {
  const w = world(); w.pointer = ptr(A); w.manifests[A.run] = A; w.lakePointer = lptr(LA); w.lakeManifests[LA.run] = LA; w.holdLakePointer = true;
  const o = w.create({ glwu: true }); o.mount('hs'); w.orig._bindMap.call(o); await settle();
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.frameIndex !== null && o.lake.status === 'loading');
  w.fire('map', 'moveend'); await settle();                                          // the view settling, or the viewer's first pan
  assert.equal(o._lakeNear, false, 'no grid yet: not near');
  w.heldLakePointers.shift()(); w.holdLakePointer = false; await settle(); await settle();
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o._wantLake(), true); assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[o.frameIndex], 'the lakes came with the run');
  o.unmount();
}));

test('G33 re-check R06 / R08 / R12: leaving the lakes aborts the hover\'s height frame and forgets it; a lakes coast set that fails drops the drawn lake and both caches', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w, { field: 'tp', manifest: Object.assign({}, A, { fields: Object.assign({}, A.fields, { tp: TPH }) }) });
  w.orig._bindMap.call(o);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake, 'the lake drawn (the coast set still loading)'); o.layer.lakeCodeAt = () => 1;
  assert.equal(o._lakeWord(45, -87, 7, null), '…'); const h = o._lakeHs; assert.ok(h && h.abort && !h.abort.signal.aborted);
  w.setView(21.3, -157.9); await settle();
  assert.equal(o._lakeHs, null, 'forgotten'); assert.equal(h.abort.signal.aborted, true, 'its fetch aborted');
  w.setView(44.8, -84.2); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.lcache.size() > 0, 'back: the lake and its ring');
  while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle();            // the lakes coast set fails (the harness answers 500)
  assert.equal(o.lakeCoast.status, 'failed');
  assert.equal(o.layer._lake, null, 'the drawn lake dropped'); assert.equal(o.lcache.size() + o.ldcache.size(), 0, 'both caches emptied');
  o.unmount();
}));

test('G33 re-check R14: a re-read that fails keeps the run and is not asked again at the next mount (its 30 minutes start afresh)', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const asked = () => w.fetches.filter((u) => /\/glwu\/v1\/latest\.json$/.test(u)).length;
  o.lake.loadedAt = 0; w.lakePointer = null;                                          // the re-read's pointer answers 404
  const n0 = asked(); o.mount('hs'); await settle(); await settle();
  assert.equal(asked(), n0 + 1); assert.equal(o.lake.status, 'ok'); assert.equal(o.lake.manifest.run, LA.run, 'the run kept');
  o.mount('hs'); await settle(); await settle();
  assert.equal(asked(), n0 + 1, 'not asked again at the next mount');
  o.unmount();
}));

test('G33 re-check P3-6: no lake frame while the lakes coast set loads again after a failure; its arrival starts the shown step\'s lake frames', () => onRunDay(async () => {
  const w = world(); const o = await lakeMounted(w);
  while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle();            // the first load fails (500)
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.lakeCoast.status, 'failed'); assert.ok(!w.fetches.some(isLakeFrameUrl));
  w.coastAnswer = lakesCoastOk; o.lakeCoast.failedAt = Date.now() - o.lakeCoast.retryMs - 1;
  o.seek(o.frameIndex + 1); await settle(); await w.releaseAll(); await settle();
  assert.ok(w.pendingCoast.length > 0, 'the set asked for again'); assert.equal(o.lakeCoast.status, 'loading');
  assert.equal(o._wantLake(), false, 'no lake frames while it loads again');
  for (let k = 0; k < 3; k++) { o.seek(o.frameIndex + 1); await settle(); await w.releaseAll(); await settle(); }
  assert.ok(!w.fetches.some(isLakeFrameUrl), 'none fetched during the retry: ' + w.fetches.filter(isLakeFrameUrl).join());
  while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle();
  assert.equal(o.lakeCoast.status, 'ok');
  assert.ok(o.inflight[o._key(o.frameIndex, 'lake')], 'the arrival starts the shown step\'s lake frame');
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.layer._lake.entry === A.frames[o.frameIndex], 'drawn');
  o.unmount();
}));

test('G33 re-check P3-5: the lakes credit needs lake water in the view, not the lake grid\'s box (Lansing at zoom 10 lies in the box, on land); a tile at the view\'s edge counts by its pixels in view', () => onRunDay(async () => {
  const w = world(); w.coastAnswer = lakesCoastOk;
  const o = await lakeMounted(w);
  while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle();
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.ok(o.layer._lake && o.layer._lakeMask);
  w.map.getZoom = () => 10; w.view = { lat: 42.73, lng: -84.55 };
  for (let k = 0; k < 400; k++) {                                                    // a view whose left edge cuts a tile well inside it
    const c0 = Math.floor(w.map.getPixelBounds().min.x) % 256;
    if (c0 >= 10 && c0 <= 240) break; w.view = { lat: 42.73, lng: w.view.lng + 0.003 };
  }
  assert.equal(w.I.lakeNearView(w.I.viewOf(w.map, o.layer), o.layer._lake.grid, 0), true, 'inside the grid\'s box');
  o.layer._tiles = {};
  plantLakeTile(w, o, w.view.lat, w.view.lng, null, 10).el._ovLake = null;          // the view's own tile: land
  assert.equal(o._lakeOnScreen(), false, 'no lake water in view: no credit'); assert.ok(!/GLWU/.test(o._attribution()));
  { const c = plantLakeTile(w, o, w.view.lat, w.view.lng, null, 10); delete o.layer._tiles[c.x + ':' + c.y + ':10'];   // a zoom-9 tile of water kept from the zoom
    o.layer._tiles[c.x + ':' + c.y + ':9'] = { el: { _ovLake: new Uint8Array(65536).fill(255) }, coords: { x: c.x, y: c.y, z: 9 } };
    plantLakeTile(w, o, w.view.lat, w.view.lng, null, 10).el._ovLake = null; }    // before (numbered like the view's own: never read as one)
  assert.equal(o._lakeOnScreen(), false, 'a tile of another zoom level does not count');
  const pb = w.map.getPixelBounds(), tx = Math.floor(pb.min.x / 256), ty = Math.floor((pb.min.y + pb.max.y) / 2 / 256), c0 = Math.floor(pb.min.x) - tx * 256;
  const mask = new Uint8Array(65536); for (let y = 0; y < 256; y++) mask[y * 256 + c0 - 5] = 255;   // water only left of the view's edge
  o.layer._tiles[tx + ':' + ty + ':10'] = { el: { _ovLake: mask }, coords: { x: tx, y: ty, z: 10 } };
  assert.equal(o._lakeOnScreen(), false, 'the edge tile\'s water lies outside the view');
  mask[100 * 256 + c0 + 5] = 128;
  assert.equal(o._lakeOnScreen(), true, 'one water pixel in view: credited'); assert.ok(/GLWU/.test(o._attribution()));
  o.unmount();
}));

// the readout bound for real (its DOM parts faked): the re-show after a height frame lands
function readoutOn(w, o) {
  const cont = { clientWidth: 1200, clientHeight: 800, classList: { add() {}, remove() {} }, style: { setProperty() {}, removeProperty() {} }, appendChild() {}, addEventListener() {}, removeEventListener() {} };
  w.map.getContainer = () => cont;
  const ce = w.doc.createElement; w.doc.createElement = (t) => { const e = ce(t); e.style = {}; e.remove = () => {}; return e; };
  w.orig._bindReadout.call(o);
  const fn = (ev) => o._listeners.filter((l) => l[0] === ev).map((l) => l[1]).pop();
  return { move: (lat, lng) => fn('mousemove')({ latlng: { lat, lng }, containerPoint: { x: 400, y: 300 }, originalEvent: { pointerType: 'mouse', target: null } }), out: () => fn('mouseout')({}) };
}
const tpLakes = (w, extra) => lakeMounted(w, Object.assign({ field: 'tp', manifest: Object.assign({}, A, { fields: Object.assign({}, A.fields, { tp: TPH }) }) }, extra || {}));
const hsFramesOf = (w) => w.fetches.filter((u) => /\/glwu\/v1\/\d{10}\/hs\//.test(u));

test('G33 re-check R27 / R28 / P3-4: the readout shows "calm" when the hover\'s height frame lands (no further move); a hidden readout stays hidden; a frame that lands after the step moved on starts no other step\'s frame', () => onRunDay(async () => {
  const w = world(); const o = await tpLakes(w);
  await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  const r = readoutOn(w, o), L = o.layer; L.readoutAt = () => null; L.lakeAt = () => true; L.lakeCodeAt = () => 1;
  const S = o.frameIndex, hsUrl = (i) => `/glwu/v1/${LA.run}/hs/f${pad3(LA.frames[o._lakeIndex(i)].step)}.png`;
  r.move(45, -87); assert.equal(o.readout.textContent, '…'); assert.equal(o.readout.hidden, false);
  await settle(); await w.releaseUrl((u) => u.endsWith(hsUrl(S)));
  assert.equal(o.readout.textContent, 'calm', 'shown when the frame lands, with no further move');
  // a hidden readout is not shown again by a landing
  o.seek(S + 1); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, S + 1);
  r.move(45, -87); assert.equal(o.readout.textContent, '…'); r.out(); assert.equal(o.readout.hidden, true);
  await settle(); await w.releaseUrl((u) => u.endsWith(hsUrl(S + 1)));
  assert.equal(o.readout.hidden, true, 'hidden stays hidden');
  // the step moves on while its height frame loads: its landing asks for no other step's frame
  const isHt = (u) => /\/glwu\/v1\/\d{10}\/hs\//.test(u);
  o.seek(S + 2); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  assert.equal(o.frameIndex, S + 2);
  r.move(45, -87); assert.equal(o.readout.textContent, '…'); assert.equal(o.readout.hidden, false);
  o.seek(S + 3); await settle();
  for (let k = 0; k < 10; k++) { const j = w.pendingBitmaps.findIndex((f) => !isHt(f.blob.url)); if (j < 0) break; w.pendingBitmaps.splice(j, 1)[0](); await settle(); }
  assert.equal(o.frameIndex, S + 3);
  const n = hsFramesOf(w).length;
  await w.releaseUrl((u) => u.endsWith(hsUrl(S + 2)));
  assert.equal(hsFramesOf(w).length, n, 'no height frame of another step asked for by the landing');
  assert.equal(o.readout.textContent, 'calm', 'the hovered step\'s word (2.21.6 left "…" until the next move)');
  // the same, but the readout shows another value meanwhile (the cursor moved onto water with a value): never overwritten
  o.seek(S + 4); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
  r.move(45, -87); assert.equal(o.readout.textContent, '…');
  L.lakeAt = () => false; L.readoutAt = () => 1.5; r.move(45.5, -86); const shown = o.readout.textContent;
  assert.ok(/ s$/.test(shown), 'a period value shown: ' + shown);
  o.seek(S + 5); await settle();
  for (let k = 0; k < 10; k++) { const j = w.pendingBitmaps.findIndex((f) => !isHt(f.blob.url)); if (j < 0) break; w.pendingBitmaps.splice(j, 1)[0](); await settle(); }
  await w.releaseUrl((u) => u.endsWith(hsUrl(S + 4)));
  assert.equal(o.readout.textContent, shown, 'the late word never replaces another value');
  o.unmount();
}));

test('G33 re-check N-3 / R22: a height frame that never comes reads "no data" after LOAD_STALL_MS (a hung download, or a decode that cannot be cancelled: its late landing is ignored); a 404 reads "no data" at once; a grid that fails validation reads "no data"', () => {
  const w = world({ fakeTimers: true });
  return onFakeClock(w, async () => {
    const o = await tpLakes(w); w.coastAnswer = lakesCoastOk; while (w.pendingCoast.length) w.pendingCoast.shift()(); await settle();   // the set in (its own 15-s watchdog would fail it)
    await w.releaseAll(); await settle(); await w.releaseAll(); await settle();
    const r = readoutOn(w, o), L = o.layer; L.readoutAt = () => null; L.lakeAt = () => true; L.lakeCodeAt = () => 1;
    const S = o.frameIndex, url = (i) => `https://x/glwu/v1/${LA.run}/hs/f${pad3(LA.frames[o._lakeIndex(i)].step)}.png`;
    const step = async (i) => { o.seek(i); await settle(); await w.releaseAll(); await settle(); await w.releaseAll(); await settle(); assert.equal(o.frameIndex, i); };
    // a hung download
    w.hang[url(S)] = 1;
    r.move(45, -87); assert.equal(o.readout.textContent, '…');
    await w.fade(14000); await settle(); assert.equal(o.readout.textContent, '…');
    await w.fade(1001); await settle(); await settle();
    assert.equal(o.readout.textContent, 'no data', 'the hung download gives up and the readout is told');
    // a decode that never ends (its download done: an abort cannot stop it), and its late landing
    await step(S + 1);
    r.move(45, -87); assert.equal(o.readout.textContent, '…');
    await settle(); const late = holdUrl(w, url(S + 1).slice('https://x/'.length));
    await w.fade(15001); await settle(); await settle();
    assert.equal(o.readout.textContent, 'no data', 'given up although the decode never settles');
    late(); await settle(); await settle();
    assert.equal(o._lakeWord(45, -87, 7, null), 'no data', 'a landing after the give-up is ignored'); assert.equal(o.readout.textContent, 'no data');
    // a 404: told at once
    await step(S + 2);
    w.failNext[url(S + 2)] = 404;
    r.move(45, -87); await settle(); await settle();
    assert.equal(o.readout.textContent, 'no data', 'a failed height frame is told at once');
    // a grid that fails validation (the frame's size matches)
    await step(S + 3);
    o.lake.manifest = Object.assign({}, o.lake.manifest, { grid: Object.assign({}, o.lake.manifest.grid, { lat0: 80 }) });
    assert.equal(o._lakeWord(45, -87, 7, null), '…');
    await settle(); await w.releaseAll(); await settle();
    assert.equal(o._lakeWord(45, -87, 7, null), 'no data', 'never "calm" from an invalid grid');
    o.unmount(); assert.equal(o._lakeHs, null);
  });
});
