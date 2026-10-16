'use strict';
// Sea surface temperature store and sampler tests for static_overlay/overlay.js (plan section 44, step 3; no
// browser): the manifest and pointer rules, the tier by zoom, the cell names with a pad, the affine unit, the
// store's state machine with held downloads (requests, in-flight cap, LRU, failures, abort, recheck), the
// sampler's exactness at the nodes and across cell seams, readout = drawn pixel through the source, the value
// contours with the deg F offset, and two real cells of the published day 2026-10-15 against ERDDAP's own values.
//   node --test tests/overlay/sst.test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

function load() {
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  global.L = {
    GridLayer: {
      prototype: { initialize(o) { this.options = o; this._tiles = {}; } },
      extend(p) {
        function C(o) { p.initialize.call(this, o); }
        C.prototype = Object.assign({ setOpacity() {}, addTo() { return this; } }, p);
        return C;
      }
    },
    DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} }
  };
  global.window = global;
  new Function(src)();
  return global.AllshoreOverlay._internals;
}
const I = load();
const settle = async () => { for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r)); };
const FIX = path.join(__dirname, '..', 'fixtures', 'sst');
const EXCERPT = JSON.parse(fs.readFileSync(path.join(FIX, 'manifest_excerpt.json'), 'utf8'));
const LO = I.SST_FDEF.lo, HI = I.SST_FDEF.hi;

// ---- a synthetic world: a smooth periodic field in deg C, encoded the way the job encodes it ----
const field = (lat, lon) => 15 + 0.1 * lat + 2 * Math.sin(lon * Math.PI / 180) + 0.5 * Math.cos(3 * lon * Math.PI / 180);
const toCode = (v, lo, hi) => 1 + Math.round((v - lo) / (hi - lo) * 254);
function tier0Frame(fn) {
  const cols = 1440, rows = 721, q = new Uint8Array(cols * rows);
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) { const v = fn(90 - r * 0.25, -180 + c * 0.25); q[r * cols + c] = Number.isNaN(v) ? 0 : toCode(v, LO, HI); }
  return { q, cols, rows };
}
const GRID0 = { cols: 1440, rows: 721, lon0: -180, lat0: 90, dlon: 0.25, dlat: -0.25, registration: 'center', lon_periodic: true };
// A cell PNG's codes: (px + 2)^2 nodes, row r / col c at lat0 + cell - r * res, lon0 + (c - 1) * res; lo / hi floor / ceil to 2 decimals.
function cellCodes(T, lat0, lon0, fn, fixed) {
  const N = T.px + 2, vals = new Float64Array(N * N);
  let mn = Infinity, mx = -Infinity;
  for (let r = 0; r < N; r++) for (let c = 0; c < N; c++) {
    const v = fn(lat0 + T.cell - r * T.res, lon0 + (c - 1) * T.res); vals[r * N + c] = v;
    if (!Number.isNaN(v)) { if (v < mn) mn = v; if (v > mx) mx = v; }
  }
  let lo = fixed ? fixed[0] : Math.floor(mn * 100) / 100, hi = fixed ? fixed[1] : Math.ceil(mx * 100) / 100;
  if (!(hi > lo)) hi = lo + 0.01;
  const q = new Uint8Array(N * N);
  for (let i = 0; i < N * N; i++) q[i] = Number.isNaN(vals[i]) ? 0 : toCode(vals[i], lo, hi);
  return { q, lo, hi, cols: N, rows: N, bytes: N * N };
}
const T1 = { name: 't1', cell: 20, res: 0.05, px: 400, apron: 1, dir: 'sst/v1/20261015/B/t1' };
const T2 = { name: 't2', cell: 5, res: 0.01, px: 500, apron: 1, dir: 'sst/v1/20261015/B/t2' };
// A manifest listing the given cells (names per tier) over the synthetic field; the bytes column is nominal.
function manifest(cellsT2, cellsT1, fn) {
  fn = fn || field;
  const list = (T, names) => { const o = {}; for (const n of names) { const [a, b] = n.split('_').map(Number); const c = cellCodes(T, a, b, fn); o[n] = [c.lo, c.hi, 1000]; } return o; };
  return { format: 'sst-v1', day: '2026-10-15', run: '20261015', build: 'B', complete: true, published_utc: '2026-10-16T12:31:40Z', units: 'degC', range: [LO, HI],
    source: { name: 'x' }, tier0: { key: 'sst/v1/20261015/B/t0.png', lo: LO, hi: HI, grid: GRID0 },
    tiers: [Object.assign({ cells: list(T2, cellsT2) }, T2), Object.assign({ cells: list(T1, cellsT1) }, T1)], stats: 'sst/v1/20261015/stats-x.json' };
}
const ptrOf = (m) => ({ format: 'sst-v1', day: m.day, run: m.run, manifest: 'sst/v1/' + m.run + '/manifest-x.json', complete: true, published_utc: m.published_utc });
// A store over a fake bucket: the pointer and manifest through a fetch stub, the frame and the cells through a decode
// stub that can hold answers (held: url -> release function list).
function fakeStore(m, opts) {
  opts = opts || {};
  const st = new I.SstStore('https://x'), held = [], log = [], fails = {};
  st.retryMs = 0;
  st._json = (url, sig, noCache) => {
    log.push(url);
    if (url.endsWith('/latest.json')) { assert.equal(noCache, true); const p = opts.pointer ? opts.pointer() : ptrOf(st.fake.m); return p instanceof Error ? Promise.reject(p) : Promise.resolve(p); }
    return Promise.resolve(opts.manifest ? opts.manifest(url) : st.fake.m);
  };
  st._decode = (url, sig, expect) => new Promise((resolve, reject) => {
    log.push(url);
    const go = () => {
      if (sig && sig.aborted) return reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
      if (fails[url]) { const f = fails[url]; delete fails[url]; return reject(new Error(f)); }
      if (url.endsWith('/t0.png')) return resolve(st.fake.frame);
      const mm = /\/(t\d)\/(-?\d+)_(-?\d+)\.png$/.exec(url), T = mm[1] === 't2' ? T2 : T1;
      const ent = st.fake.m.tiers.find((t) => t.name === mm[1]).cells[mm[2] + '_' + mm[3]];
      resolve(cellCodes(T, +mm[2], +mm[3], st.fake.fn, ent));
    };
    if (opts.hold && /\/t[12]\//.test(url)) st.fake.held.push({ url, go }); else go();   // st.fake.held: tests may replace the array
  });
  st.fake = { m, frame: tier0Frame(opts.fn || field), fn: opts.fn || field, held, log, fails };
  return st;
}
const release = (st, n) => { for (let i = 0; i < (n || 1) && st.fake.held.length; i++) st.fake.held.shift().go(); };
function fakeEl() { return { getContext: () => ({ clearRect() {}, createImageData: () => ({ data: new Uint8ClampedArray(65536 * 4) }), putImageData() {} }) }; }
function sstLayer(st) {
  const l = new I.ModelGridLayer({ opacity: 1 });
  l.setFrame(st.frame, st.grid, 'sst', I.SST_FDEF, I.buildLut('sst', I.SST_FDEF.legend), null);
  l.setSource(st);
  return l;
}
const halfStep = (lo, hi) => (hi - lo) / 254 / 2 + 1e-9;
const fixedStep = (HI - LO) / 254;

test('validSstManifest and validSstPointer: the published excerpt passes; the rules that keep bad data off the map', () => {
  const ok = JSON.parse(JSON.stringify(EXCERPT));
  const m = { format: 'sst-v1', day: '2026-10-15', run: ok.day, complete: true, units: 'degC', range: ok.range, tier0: ok.tier0, tiers: ok.tiers };
  assert.equal(I.validSstManifest(m), true);
  assert.equal(I.validSstPointer({ format: 'sst-v1', complete: true, run: '20261015', manifest: 'sst/v1/20261015/manifest-20261016T123140Z.json' }), true);
  const bad = (f) => { const c = JSON.parse(JSON.stringify(m)); f(c); return I.validSstManifest(c); };
  assert.equal(bad((c) => { c.format = 'sst-v2'; }), false);
  assert.equal(bad((c) => { c.complete = false; }), false);
  assert.equal(bad((c) => { c.range = [-2, 40]; }), false, 'another range than this file draws');
  assert.equal(bad((c) => { c.units = 'K'; }), false);
  assert.equal(bad((c) => { c.tier0.key = '../x.png'; }), false);
  assert.equal(bad((c) => { c.tiers[0].px = 499; }), false, 'px * res must be the cell');
  assert.equal(bad((c) => { c.tiers[0].apron = 0; }), false);
  assert.equal(bad((c) => { c.tiers[0].cell = 7; }), false, '7 does not tile the world');
  assert.equal(bad((c) => { c.tiers[0].dir = 'sst/../x'; }), false);
  assert.equal(bad((c) => { c.tiers[0].cells['20_-160'] = [28, 25, 10]; }), false, 'lo < hi');
  assert.equal(bad((c) => { c.tiers[0].cells['20_-160'] = [25, 28]; }), false);
  assert.equal(bad((c) => { c.tiers[0].cells['x_y'] = [25, 28, 10]; }), false);
  assert.equal(bad((c) => { c.tiers = [c.tiers[0], c.tiers[0], c.tiers[0], c.tiers[0], c.tiers[0]]; }), false);
  assert.equal(bad((c) => { c.tiers = []; }), true, 'a frame-only day (the 0.05-degree fallback source) is fine');
  assert.equal(I.validSstPointer({ format: 'sst-v1', complete: true, run: '2026101', manifest: 'a' }), false);
  assert.equal(I.validSstPointer({ format: 'sst-v1', complete: false, run: '20261015', manifest: 'a' }), false);
  assert.equal(I.validSstPointer({ format: 'sst-v1', complete: true, run: '20261015', manifest: '/etc/passwd' }), false);
  assert.equal(I.validSstPointer(null), false);
});

test('sstTierFor: the frame below zoom 5, the 0.05-degree cells to 7.99, the 0.01-degree cells from 8, never beyond what is published', () => {
  assert.deepEqual([4.99, 5, 6, 7.99, 8, 12].map((z) => I.sstTierFor(z, 2)), [0, 1, 1, 1, 2, 2]);
  assert.deepEqual([4, 6, 9].map((z) => I.sstTierFor(z, 1)), [0, 1, 1]);
  assert.deepEqual([4, 6, 9].map((z) => I.sstTierFor(z, 0)), [0, 0, 0]);
  assert.deepEqual(I.SST_TIER_ZOOMS, [5, 8]);
});

test('cellsForTile: pad 0 is the coast rule; a pad reaches the neighbour at a tile edge; 20-degree cells run -100..80', () => {
  for (const c of [{ z: 8, x: 15, y: 112 }, { z: 5, x: 1, y: 13 }, { z: 3, x: 7, y: 7 }, { z: 9, x: 511, y: 200 }, { z: 2, x: 5, y: 1 }])
    for (const d of [5, 20]) assert.deepEqual(I.cellsForTile(c, d, 0), I.coastCellsForTile(c, d));
  // a z8 tile whose west edge is exactly lon -135 (x 32): without a pad only cells east of it, with one the 20_-140 cell too
  const t = { z: 8, x: 32, y: 112 };
  assert.deepEqual(I.cellsForTile(t, 5, 0), ['20_-135']);
  assert.deepEqual(I.cellsForTile(t, 5, I.SST_PAD_PX).sort(), ['20_-135', '20_-140']);
  const all = new Set();
  for (let x = 0; x < 8; x++) for (let y = 0; y < 8; y++) for (const n of I.cellsForTile({ z: 3, x, y }, 20, 4)) all.add(n);
  const lats = new Set([...all].map((n) => +n.split('_')[0]));
  assert.deepEqual([...lats].sort((a, b) => a - b), [-100, -80, -60, -40, -20, 0, 20, 40, 60, 80]);
});

test('unitOf sst: deg F is affine (scale 1.8, offset 32, inverse), the ticks sit at the right positions, the top reads 95+ / 35+', () => {
  const f = I.unitOf('sst', 'US'), c = I.unitOf('sst', 'Metric');
  assert.equal(f.f(25), 77); assert.equal(f.inv(77), 25); assert.equal(f.scale, 1.8); assert.equal(f.offset, 32); assert.equal(f.label, '°F');
  assert.equal(c.f(25), 25); assert.equal(c.inv(25), 25); assert.equal(c.scale, 1); assert.equal(c.offset, 0); assert.equal(c.label, '°C');
  const us = I.legendTicks('sst', I.SST_FDEF, 'US'), si = I.legendTicks('sst', I.SST_FDEF, 'Metric');
  assert.deepEqual(us.map((t) => t.label), ['40', '50', '60', '70', '80', '95+ °F']);
  assert.ok(Math.abs(us[0].pos - I.legendPos('sst', I.SST_FDEF.legend, (40 - 32) / 1.8)) < 1e-12, 'a deg F tick is placed at its deg C value, not at v / 33.8');
  assert.deepEqual(si.map((t) => t.label), ['0', '5', '10', '15', '20', '25', '35+ °C']);
  assert.ok(Math.abs(si[2].pos - I.legendPos('sst', I.SST_FDEF.legend, 10)) < 1e-12 && Math.abs(si[2].pos - 0.22) < 1e-12);   // the knots: 10 C at 0.22
  assert.equal(I.CLIP_FIELDS.sst, true); assert.deepEqual(I.CONTOURS.sst, { US: 2, Metric: 1 }); assert.equal(I.FIXED_OPACITY.sst, 0.75);
  assert.ok(I.RAMPS.sst.length > 5 && I.buildLut('sst', [LO, HI]).length === 768);
  assert.equal(I.RAMPS.sst[0][1], '#2d004b'); assert.equal(I.RAMPS.sst[I.RAMPS.sst.length - 1][1], '#67001f');   // the owner's pick B (thermal)
  // the knots: 15-30 C takes 56 % of the scale; -2 and 35 C are its ends; the bar and the tiles agree at a knot
  assert.ok(Math.abs(I.legendPos('sst', [LO, HI], 30) - I.legendPos('sst', [LO, HI], 15) - 0.56) < 1e-12);
  assert.equal(I.legendPos('sst', [LO, HI], LO), 0); assert.equal(I.legendPos('sst', [LO, HI], HI), 1);
  assert.ok(Math.abs(I.legendInv('sst', [LO, HI], 0.48) - 20) < 1e-12);
  // hs is unchanged by the knots rule written for a legend that starts below 0
  assert.ok(Math.abs(I.legendPos('hs', [0, 12], 3) - 0.47) < 1e-12 && Math.abs(I.legendPos('hs', [0, 15], 3.75) - 0.47) < 1e-12);
});

test('SstStore.load: pointer -> manifest -> frame; a bad pointer fails with a cooldown; abort leaves idle; the frame is validated', async () => {
  const m = manifest(['20_-160'], ['20_-160']);
  let st = fakeStore(m, { pointer: () => new Error('sst 503') });
  st.retryMs = 1000;
  assert.equal(await st.load(), null); assert.equal(st.status, 'failed');
  assert.equal(await st.load(), null, 'within the cooldown: no second wait');
  assert.equal(st.fake.log.length, 1);
  st = fakeStore(m, { pointer: () => ({ format: 'sst-v1', complete: true, run: '20261015', manifest: '../x' }) });
  assert.equal(await st.load(), null);
  st = fakeStore(m, { manifest: () => Object.assign({}, m, { run: '20261014' }) });
  assert.equal(await st.load(), null, 'a manifest for another day than the pointer names');
  st = fakeStore(m);
  st.fake.frame = { q: new Uint8Array(10), cols: 5, rows: 2 };
  assert.equal(await st.load(), null, 'a frame that is not the manifest grid');
  st = fakeStore(m);
  const p = st.load(); st.abortAll(); assert.equal(await p, null); assert.equal(st.status, 'idle');
  st = fakeStore(m);
  assert.equal(await st.load(), st); assert.equal(st.status, 'ok');
  assert.equal(st.frame.cols, 1440); assert.equal(st.tiers.length, 2); assert.equal(st.tiers[0].name, 't1'); assert.equal(st.tiers[1].name, 't2');
  assert.equal(st.tiers[1].n, 502); assert.equal(st.available(), 2);
  const n = st.fake.log.length;
  assert.equal(await st.load(), st); assert.equal(st.fake.log.length, n, 'loaded and fresh: nothing fetched');
});

test('SstStore: a recheck only notes a newer day; the next load adopts it and drops the cells; a failed re-read keeps the day', async () => {
  const m = manifest(['20_-160'], ['20_-160']), m2 = Object.assign({}, m, { run: '20261016', day: '2026-10-16' });
  let now = m;
  const st = fakeStore(m, { pointer: () => (now instanceof Error ? now : ptrOf(now)), manifest: () => now });
  assert.equal(await st.load(), st);
  st.tierFor({ z: 8, x: 14, y: 112 }); await settle();
  assert.equal(st.tiers[1].loaded.size, 1);
  assert.equal(await st.recheck(), false); assert.equal(st.newer, null);
  now = m2;
  assert.equal(await st.recheck(), true); assert.equal(st.newer.run, '20261016');
  assert.equal(st.manifest.run, '20261015', 'noted, not adopted');
  assert.equal(await st.load(), st);
  assert.equal(st.manifest.run, '20261016'); assert.equal(st.newer, null); assert.equal(st.tiers[1].loaded.size, 0, 'the old day\'s cells are gone');
  st.loadedAt = 0; now = new Error('sst 503');
  assert.equal(await st.load(), st); assert.equal(st.status, 'ok'); assert.equal(st.manifest.run, '20261016', 'a failed re-read keeps the day on screen');
  st.loadedAt = 0; now = m2;
  const before = st.fake.log.length;
  assert.equal(await st.load(), st); assert.equal(st.fake.log.length, before + 1, 'the same run: the pointer only');
});

test('SstStore.tierFor: the finest tier whose cells are all in hand, missing ones requested with <= 4 in flight; landing moves the tier; pad cells never gate', async () => {
  const st = fakeStore(manifest(['20_-160', '20_-165', '15_-160', '15_-165', '25_-160'], ['20_-160', '0_-160', '20_-180']), { hold: true });
  assert.equal(await st.load(), st);
  const z8 = { z: 8, x: 14, y: 112 };                                        // inside 20_-160 (lon -160.3..-158.9 crosses -160: two cells)
  assert.deepEqual(I.cellsForTile(z8, 5, 0).sort(), ['20_-160', '20_-165']);
  assert.equal(st.tierFor(z8), 0, 'nothing in hand: the frame');
  assert.deepEqual(Object.keys(st.inflight).sort(), ['t1/20_-160', 't1/20_-180', 't2/20_-160', 't2/20_-165'], 'its t2 cells, and the t1 cells it falls back to');
  const releaseUrl = async (re) => { const h = st.fake.held.find((x) => re.test(x.url)); st.fake.held = st.fake.held.filter((x) => x !== h); h.go(); await settle(); };
  await releaseUrl(/t2\/20_-160/);
  assert.equal(st.tierFor(z8), 0, 'one t2 cell of two, no t1 cell yet: still the frame');
  await releaseUrl(/t1\/20_-160/); await releaseUrl(/t1\/20_-180/);
  assert.equal(st.tierFor(z8), 1, 'the t1 cells are in hand: that tier while t2 completes');
  await releaseUrl(/t2\/20_-165/);
  assert.equal(st.tierFor(z8), 2);
  assert.ok(st.tiers[0].loaded.has('20_-160') && st.tiers[1].loaded.has('20_-160') && st.tiers[1].loaded.has('20_-165'));
  // the in-flight cap: a z8 tile row at the 25 N edge wants 2 x 2 cells + pad cells; never more than 4 downloads at once
  const z8b = { z: 8, x: 14, y: 109 };                                       // straddles lat 25
  st.tierFor(z8b); st.tierFor({ z: 8, x: 13, y: 109 });
  assert.ok(Object.keys(st.inflight).length <= I.MAX_SST_INFLIGHT && Object.keys(st.inflight).length >= 1);
  while (st.fake.held.length) { release(st, 1); await settle(); }
  await settle();
  assert.equal(Object.keys(st.inflight).length, 0);
  // an unlisted cell is land: nothing to fetch, the tier is complete without it
  const z8land = { z: 8, x: 14, y: 100 };                                    // lat ~30: no cell listed there
  assert.equal(st.tierFor(z8land), 2); assert.equal(st.fake.held.length, 0);
  // z5: the 0.05-degree cells (0_-160 listed: lon -160..-140, lat 0..20)
  const z5 = { z: 5, x: 2, y: 15 };                                          // lon -157.5..-146.25, lat 0..11.2
  assert.deepEqual(I.cellsForTile(z5, 20, 0), ['0_-160']);
  assert.equal(st.tierFor(z5), 0); release(st, 1); await settle(); assert.equal(st.tierFor(z5), 1);
  // a z3 tile draws the frame whatever is loaded
  assert.equal(st.tierFor({ z: 3, x: 0, y: 3 }), 0);
  st.abortAll();
});

test('SstStore: a 404 is final (the tile keeps the coarser tier for good), a network error a cooldown with a retry timer; LRU never drops a needed cell', async () => {
  const st = fakeStore(manifest(['20_-160', '20_-165', '20_-170', '15_-160'], ['20_-160']), { hold: true });
  assert.equal(await st.load(), st);
  const z8 = { z: 8, x: 14, y: 112 };
  st.fake.fails['https://x/sst/v1/20261015/B/t2/20_-165.png'] = 'frame 404';
  st.fake.fails['https://x/sst/v1/20261015/B/t2/20_-160.png'] = 'Failed to fetch';
  st.retryMs = 40;
  let changes = 0; st.onChange = () => { changes++; };
  st.tierFor(z8); release(st, 3); await settle();
  assert.equal(st.failed['t2/20_-165'], true); assert.equal(typeof st.failed['t2/20_-160'], 'number'); assert.ok(st.retryTimer);
  assert.equal(st.tierFor(z8), 1, 'the t1 cell landed: that tier; t2 can never complete for this tile');
  assert.equal(st.fake.held.length, 0, 'nothing re-requested in the cooldown');
  await new Promise((r) => setTimeout(r, 120));
  st.tierFor(z8);
  assert.equal(st.fake.held.length, 1, 'the cooldown ended: the network failure is asked again, the 404 is not');
  release(st, 1); await settle();
  assert.equal(st.tierFor(z8), 1); assert.ok(changes >= 3);
  // LRU by decoded bytes, needed cells kept
  const l = sstLayer(st);
  l._tiles['14:112:8'] = { el: fakeEl(), coords: z8 };
  const need = st.onNeeded();
  assert.ok(need['t2/20_-160'] && need['t1/20_-160'] && need['t2/20_-165'] && !need['t2/20_-170']);
  st.tiers[1].loaded.set('20_-170', { q: new Uint8Array(1), lo: 0, hi: 1, bytes: 30e6, rev: 0 }); st.bytes += 30e6;
  st.tiers[1].loaded.set('15_-160', { q: new Uint8Array(1), lo: 0, hi: 1, bytes: 30e6, rev: 0 }); st.bytes += 30e6;
  st._evict();
  assert.ok(st.tiers[1].loaded.has('20_-160') && !st.tiers[1].loaded.has('20_-170'), 'the oldest unneeded cell went first');
  assert.ok(st.bytes <= I.MAX_SST_CHUNK_BYTES + 30e6);
  // idle keeps the cells, abortAll drops them and keeps the frame
  st.idle(); assert.equal(st.onChange, null); assert.ok(st.tiers[1].loaded.size > 0);
  st.abortAll(); assert.equal(st.tiers[1].loaded.size, 0); assert.equal(st.bytes, 0); assert.ok(st.frame && st.manifest); assert.equal(st.status, 'ok');
});

test('sampleGrid: exact at every node of a cell (within half its step), the frame below zoom 5, world copies identical, absent where no cell', async () => {
  const st = fakeStore(manifest(['20_-160', '20_-165'], ['20_-160']));
  assert.equal(await st.load(), st);
  const z8 = { z: 8, x: 14, y: 112 };
  assert.equal(st.tierFor(z8), 0); await settle(); assert.equal(st.tierFor(z8), 2);
  const out = new Float64Array(256 * 256), copy = new Float64Array(256 * 256);
  st.sampleGrid(z8, 0, 0, 1, 256, out, 2);
  st.sampleGrid({ z: 8, x: 14 + 256, y: 112 }, 0, 0, 1, 256, copy, 2);
  assert.deepEqual(copy, out, 'a world copy samples the one world');
  const rec = st.tiers[1].loaded.get('20_-160');
  let checked = 0;
  for (let py = 0; py < 256; py += 7) for (let px = 0; px < 256; px += 5) {
    const ll = I.tilePixelLatLng(z8, px, py), v = LO + (out[py * 256 + px] - 1) / 254 * (HI - LO);
    if (ll.lng < -160) continue;                                               // the other cell: its own range
    assert.ok(Math.abs(v - field(ll.lat, ll.lng)) <= halfStep(rec.lo, rec.hi) + 0.002, `${px},${py}: ${v} vs ${field(ll.lat, ll.lng)}`);   // bilinear within 0.01 deg of a smooth field
    checked++;
  }
  assert.ok(checked > 1000);
  // the lattice sampled at a node is that node exactly (the quantised node value, not the field)
  const N = 502, lat0 = 20, lon0 = -160;
  for (const [r, c] of [[0, 1], [1, 1], [250, 250], [501, 501], [0, 0], [501, 0], [100, 501]]) {
    const lat = lat0 + 5 - r * 0.01, lon = lon0 + (c - 1) * 0.01, p = I.pixelOf(lat, lon, 12), one = new Float64Array(1);
    if (!(lat < 25 && lat > 20 && lon >= -160 && lon < -155)) continue;   // apron nodes belong to the neighbours' interior
    st.sampleGrid(p, p.px, p.py, 1, 1, one, 2);
    const q = rec.q[r * N + c], want = rec.lo + (q - 1) / 254 * (rec.hi - rec.lo);
    const got = LO + (one[0] - 1) / 254 * (HI - LO);
    assert.ok(Math.abs(got - want) <= halfStep(rec.lo, rec.hi) + 1e-6, `node ${r},${c}: ${got} vs ${want}`);    // the pixel centre is within 0.005 deg of the node
  }
  // tier 0 = the frame sampler
  const z3 = { z: 3, x: 0, y: 3 }, a = new Float64Array(256 * 256), b = new Float64Array(256 * 256);
  st.sampleGrid(z3, 0, 0, 1, 256, a, 0);
  const l = new I.ModelGridLayer({ opacity: 1 });
  l.setFrame(st.frame, st.grid, 'sst', I.SST_FDEF, I.buildLut('sst', [LO, HI]), null);
  l.tileCodes(z3, b);
  assert.deepEqual(a, b);
  // a cell nobody listed (land): absent
  const z8far = { z: 8, x: 100, y: 112 }, far = new Float64Array(256 * 256);
  st.sampleGrid(z8far, 0, 0, 1, 256, far, 2);
  assert.ok(far.every((x) => x === 0));
});

test('seams: a point on the shared edge of two cells reads the same from either side within half a step, bit-identical when their ranges agree', async () => {
  const st = fakeStore(manifest(['20_-160', '20_-165', '15_-160'], []));
  assert.equal(await st.load(), st);
  const t = { z: 9, x: 28, y: 225 }, t2 = { z: 9, x: 28, y: 227 };            // around 20 N / -160, above and below lat 20
  st.tierFor(t); st.tierFor(t2); await settle(); assert.equal(st.tierFor(t), 2); assert.equal(st.tierFor(t2), 2);
  const A = st.tiers[1].loaded.get('20_-160'), B = st.tiers[1].loaded.get('20_-165'), C = st.tiers[1].loaded.get('15_-160');
  const sample = (lat, lon, z) => { const p = I.pixelOf(lat, lon, z), one = new Float64Array(1); st.sampleGrid(p, p.px, p.py, 1, 1, one, 2); return LO + (one[0] - 1) / 254 * (HI - LO); };
  // the shared node column (lon -160.00 = A's first interior column, B's east apron) and row (lat 20.00)
  let worst = 0;
  for (let i = 0; i < 40; i++) {
    const lat = 20.005 + i * 0.1237, lon = -160;
    const vA = A.lo + (A.q[Math.round((25 - lat) / 0.01) * 502 + 1] - 1) / 254 * (A.hi - A.lo);
    const vB = B.lo + (B.q[Math.round((25 - lat) / 0.01) * 502 + 501] - 1) / 254 * (B.hi - B.lo);
    assert.ok(Math.abs(vA - vB) <= halfStep(A.lo, A.hi) + halfStep(B.lo, B.hi), 'shared nodes agree within their steps');
    worst = Math.max(worst, Math.abs(vA - vB));
    const v = sample(lat, lon - 1e-9, 12), v2 = sample(lat, lon + 1e-9, 12);
    assert.ok(Math.abs(v - vA) <= halfStep(A.lo, A.hi) + halfStep(B.lo, B.hi) + 0.002 && Math.abs(v2 - v) <= halfStep(A.lo, A.hi) + halfStep(B.lo, B.hi) + 0.002);
  }
  assert.ok(worst <= 0.011, 'the job\'s 2-decimal ranges: a shared node differs by less than a hundredth');
  for (let i = 0; i < 40; i++) {
    const lon = -159.99 + i * 0.1239, lat = 20;
    const vA = A.lo + (A.q[500 * 502 + Math.round((lon + 160) / 0.01) + 1] - 1) / 254 * (A.hi - A.lo);
    const vC = C.lo + (C.q[0 * 502 + Math.round((lon + 160) / 0.01) + 1] - 1) / 254 * (C.hi - C.lo);
    assert.ok(Math.abs(vA - vC) <= halfStep(A.lo, A.hi) + halfStep(C.lo, C.hi));
  }
  // equal ranges: bit-identical across the seam
  const same = fakeStore(manifest(['20_-160', '20_-165'], []));
  same.fake.m.tiers[0].cells['20_-160'] = [20, 30, 1000]; same.fake.m.tiers[0].cells['20_-165'] = [20, 30, 1000];
  assert.equal(await same.load(), same); same.tierFor(t); await settle(); assert.equal(same.tierFor(t), 2);
  const sA = same.tiers[1].loaded.get('20_-160'), sB = same.tiers[1].loaded.get('20_-165');
  for (let r = 0; r < 502; r += 9) assert.equal(sA.q[r * 502 + 1], sB.q[r * 502 + 501]);
  const s2 = (lat, lon) => { const p = I.pixelOf(lat, lon, 11), one = new Float64Array(1); same.sampleGrid(p, p.px, p.py, 1, 1, one, 2); return one[0]; };
  for (let i = 0; i < 20; i++) { const lat = 21 + i * 0.17; assert.equal(s2(lat, -160 - 1e-9), s2(lat, -160 + 1e-9)); }
});

test('readoutAt = the drawn pixel through the source at tiers 1 and 2, across a cell boundary, wrapped and unwrapped; null over land and off the map', async () => {
  const st = fakeStore(manifest(['20_-160', '20_-165', '25_-160', '25_-165'], ['20_-160', '20_-180']));
  assert.equal(await st.load(), st);
  const l = sstLayer(st);
  for (const [coords, tier] of [[{ z: 8, x: 14, y: 109 }, 2], [{ z: 6, x: 3, y: 27 }, 1], [{ z: 3, x: 0, y: 3 }, 0]]) {
    const el = fakeEl(), key = coords.x + ':' + coords.y + ':' + coords.z;
    l._tiles[key] = { el, coords };
    l._draw(el, coords); await settle(); l._draw(el, coords);
    assert.equal(el._ovSrcTier, tier, key);
    const codes = l.tileCodes(coords, new Float64Array(65536), tier);
    let n = 0, present = 0;
    for (let py = 0; py < 256; py += 3) for (let px = 0; px < 256; px += 5) {
      const ll = I.tilePixelLatLng(coords, px, py), v = l.readoutAt(ll.lat + 1e-7, ll.lng - 1e-7, coords.z), c = codes[py * 256 + px];
      if (c) { assert.ok(v !== null && Math.abs(v - l._value(c)) < 1e-9, `${key} ${px},${py}`); present++; } else assert.equal(v, null);
      n++;
    }
    assert.ok(present > (tier ? 1000 : 3000), key + ': ' + present + ' of ' + n);
    // a world copy of the tile: the same values under the same pixels
    const wrapped = { z: coords.z, x: coords.x + Math.pow(2, coords.z), y: coords.y }, elw = fakeEl();
    l._tiles[wrapped.x + ':' + wrapped.y + ':' + wrapped.z] = { el: elw, coords: wrapped };
    l._draw(elw, wrapped);
    assert.equal(elw._ovSrcTier, tier);
    const ll = I.tilePixelLatLng(coords, 100, 100);
    assert.equal(l.readoutAt(ll.lat, ll.lng + 360, coords.z), l.readoutAt(ll.lat, ll.lng, coords.z));
  }
  // no tile on the map at that zoom: null; a land mask: null under it
  const ll = I.tilePixelLatLng({ z: 8, x: 14, y: 109 }, 10, 10);
  assert.equal(l.readoutAt(ll.lat, ll.lng, 9), null);
  const el8 = l._tiles['14:109:8'].el;
  const mask = new Uint8Array(65536).fill(0); mask[10 * 256 + 10] = 255;
  el8._ovLand = mask; el8._ovLandKey = '8/14/109'; el8._ovLandFinal = true;
  l._coast = { status: 'ok', rev: 0 }; l._clip = true;
  assert.equal(l.readoutAt(ll.lat, ll.lng, 8), null, 'over land');
  const ll2 = I.tilePixelLatLng({ z: 8, x: 14, y: 109 }, 11, 10);
  assert.ok(l.readoutAt(ll2.lat, ll2.lng, 8) !== null, 'beside it');
});

test('the source redraws a tile only when its tier changed or one of its cells landed; setSource(null) returns to the frame', async () => {
  const st = fakeStore(manifest(['20_-160', '20_-165', '20_-155'], ['20_-160']), { hold: true });
  assert.equal(await st.load(), st);
  const l = sstLayer(st), a = fakeEl(), b = fakeEl();
  const ca = { z: 8, x: 14, y: 112 }, cb = { z: 8, x: 17, y: 112 };          // a: 20_-160 + 20_-165; b: 20_-160 + 20_-155
  assert.deepEqual(I.cellsForTile(ca, 5, 0).sort(), ['20_-160', '20_-165']); assert.deepEqual(I.cellsForTile(cb, 5, 0).sort(), ['20_-155', '20_-160']);
  l._tiles['14:112:8'] = { el: a, coords: ca }; l._tiles['17:112:8'] = { el: b, coords: cb };
  let draws = 0; const orig = l._draw; l._draw = function (el, c) { draws++; return orig.call(this, el, c); };
  l._draw(a, ca); l._draw(b, cb); draws = 0;
  assert.equal(a._ovSrcTier, 0); assert.equal(b._ovSrcTier, 0);
  const order = st.fake.held.map((h) => h.url.replace(/.*\//, ''));
  assert.ok(order.indexOf('20_-160.png') >= 0);
  // the t1 cell lands: both tiles can move to tier 1
  st.fake.held.find((h) => /t1\//.test(h.url)).go(); st.fake.held = st.fake.held.filter((h) => !/t1\//.test(h.url)); await settle();
  assert.equal(draws, 2); assert.equal(a._ovSrcTier, 1); assert.equal(b._ovSrcTier, 1); draws = 0;
  // t2 20_-165 lands: tile a is still waiting for 20_-160; nothing changes tier, but a's own cell landed -> a redrawn? no: the tier did not change
  // and landedAfter looks at cells of the DRAWN tier (1), so neither tile is redrawn
  st.fake.held.find((h) => /20_-165/.test(h.url)).go(); st.fake.held = st.fake.held.filter((h) => !/20_-165/.test(h.url)); await settle();
  assert.equal(draws, 0);
  // t2 20_-160 lands: a has both -> tier 2; b still waits for 20_-155 -> stays at 1
  st.fake.held.find((h) => /20_-160/.test(h.url)).go(); st.fake.held = st.fake.held.filter((h) => !/20_-160/.test(h.url)); await settle();
  assert.equal(draws, 1); assert.equal(a._ovSrcTier, 2); assert.equal(b._ovSrcTier, 1); draws = 0;
  while (st.fake.held.length) { release(st, 1); await settle(); }
  assert.equal(b._ovSrcTier, 2);
  draws = 0;
  // a pad cell landing after the draw: the tile is redrawn (its contour lattice reads it)
  const rec = st.tiers[1].loaded.get('20_-165'); rec.rev = st.rev + 1; st.rev++;
  st.onChange(); assert.equal(draws, 1); draws = 0;
  st.onChange(); assert.equal(draws, 0, 'nothing new: nothing redrawn');
  l.setSource(null, true);
  assert.equal(st.onChange, null); assert.equal(l._src, null);
  const codes = l.tileCodes(ca, new Float64Array(65536));
  assert.ok(codes.some((c) => c > 0), 'the frame draws again');
  l.clear(); assert.equal(l._frame, null);
});

test('value contours: isotherms at whole deg C / even deg F (the offset), continuous across a tile edge, RGB only; hs settings carry off 0', async () => {
  // a field linear in latitude, 8 C per degree: a z10 tile (0.33 deg tall) holds 2-3 whole-degree isotherms (Metric), 2-3 even-deg-F ones (US)
  const lin = (lat, lon) => 20 + (lat - 20) * 8;
  const st = fakeStore(manifest(['20_-160', '20_-165'], ['20_-160'], lin), { fn: lin });
  assert.equal(await st.load(), st);
  const l = sstLayer(st);
  const c1 = { z: 10, x: 56, y: 450 }, c2 = { z: 10, x: 56, y: 451 };        // one above the other, inside 20_-160
  st.tierFor(c1); st.tierFor(c2); await settle();
  assert.equal(st.tierFor(c1), 2); assert.equal(st.tierFor(c2), 2);
  function inked(coords, cfg) {
    l.setContours(cfg, true);
    const codes = l.tileCodes(coords, new Float64Array(65536), 2), plain = new Uint8ClampedArray(65536 * 4);
    I.composeTile(codes, null, l._lut, LO, HI, LO, HI, plain);
    const d = plain.slice(); l._contours(coords, d, false, 2);
    const rows = [], want = [];
    for (let py = 0; py < 256; py++) { let n = 0; for (let px = 0; px < 256; px++) { const k = (py * 256 + px) * 4; if (d[k] !== plain[k] || d[k + 1] !== plain[k + 1] || d[k + 2] !== plain[k + 2]) n++; assert.equal(d[k + 3], plain[k + 3]); } if (n > 200) rows.push(py); }
    // where the lines belong: the rows where the DRAWN value (the quantised cell code under column 0) crosses a level
    const one = new Float64Array(1), codeAt = (py) => py < 256 ? codes[py * 256] : (st.sampleGrid(coords, 0, py, 1, 1, one, 2), one[0]);   // row 256: the pad row below
    const lvl = (py) => Math.floor((l._value(codeAt(py)) * cfg.per + cfg.off) / cfg.step);
    for (let py = 0; py < 257; py++) if (lvl(py) !== lvl(py + 1)) want.push(py);        // a crossing just under the tile inks its last row
    return { rows, want, d };
  }
  const near = (rows, want) => rows.length > 0 && rows.every((r) => want.some((w) => Math.abs(w - r) <= 2)) && want.every((w) => rows.some((r) => Math.abs(w - r) <= 2));
  const metric = inked(c1, { step: 1, per: 1, off: 0 }), us = inked(c1, { step: 2, per: 1.8, off: 32 });
  assert.ok(metric.want.length >= 2 && us.want.length >= 2, 'the tile holds isotherms: ' + metric.want + ' / ' + us.want);
  assert.ok(near(metric.rows, metric.want), 'Metric lines sit where the drawn value crosses a whole deg C: ' + metric.rows + ' vs ' + metric.want);
  assert.ok(near(us.rows, us.want), 'US lines sit where the drawn value crosses an even deg F: ' + us.rows + ' vs ' + us.want);
  assert.notDeepEqual(metric.want, us.want, 'the two units draw different sets of lines');
  // an offset that is not a whole number of levels (3 F lines: 32 / 3): the lines still sit where the DISPLAY value crosses a level
  const odd = inked(c1, { step: 3, per: 1.8, off: 32 });
  assert.ok(odd.want.length >= 1 && near(odd.rows, odd.want), '3 F lines: ' + odd.rows + ' vs ' + odd.want);
  assert.notDeepEqual(odd.want, us.want);
  // the ideal isotherms of the field itself: the drawn crossings sit within one quantisation terrace of them
  const ideal = (fn) => { const out = []; for (let py = 0; py < 255; py++) if (Math.floor(fn(I.tilePixelLatLng(c1, 0, py).lat)) !== Math.floor(fn(I.tilePixelLatLng(c1, 0, py + 1).lat))) out.push(py); return out; };
  const idealC = ideal((lat) => lin(lat, 0)), terracePx = 256 * 15 / 254 + 2;          // 254 codes over a 5-degree cell: a terrace is ~15 px of a z10 tile
  assert.ok(idealC.every((w) => metric.rows.some((r) => Math.abs(w - r) <= terracePx)), 'drawn ' + metric.rows + ' vs ideal ' + idealC);
  // continuity: the tile below draws its own crossings the same way (its lattice pad reads the shared edge)
  const below = inked(c2, { step: 1, per: 1, off: 0 });
  assert.ok(below.want.length >= 2 && near(below.rows, below.want), 'the tile below: ' + below.rows + ' vs ' + below.want);
  // hs: the Overlay's settings carry off 0 and the layer adds nothing
  const o = Object.create(I.Overlay.prototype); o.field = 'hs'; o.contours = true; o.opts = { getUnit: () => 'US' };
  assert.deepEqual(o._contourCfg(), { step: 2, per: 3.28084, off: 0 });
  o.field = 'sst'; assert.deepEqual(o._contourCfg(), { step: 2, per: 1.8, off: 32, coarseBelow: 7, zero: true });
  o.opts.getUnit = () => 'Metric'; assert.deepEqual(o._contourCfg(), { step: 1, per: 1, off: 0, coarseBelow: 7, zero: true });
  o.field = 'wind'; assert.equal(o._contourCfg(), null);
});

async function realCell(name) {
  const buf = fs.readFileSync(path.join(FIX, name));
  return I.decodePngGrey(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength));
}

test('real day 2026-10-15: two published cells and the frame, sampled through the client, equal ERDDAP\'s values within half a step; the seam holds', async () => {
  const [a, b, t0] = await Promise.all([realCell('t2_20_-160.png'), realCell('t2_20_-165.png'), realCell('t0.png')]);
  assert.ok(a && a.cols === 502 && b && b.cols === 502 && t0 && t0.cols === 1440 && t0.rows === 721, 'the fixtures decode');
  const ex = EXCERPT, m = { format: 'sst-v1', day: '2026-10-15', run: ex.day, complete: true, units: 'degC', range: ex.range, tier0: ex.tier0, tiers: ex.tiers };
  assert.equal(I.validSstManifest(m), true);
  const st = new I.SstStore('https://x');
  st._json = (url) => Promise.resolve(url.endsWith('latest.json') ? { format: 'sst-v1', complete: true, run: ex.day, manifest: 'sst/v1/x/manifest.json' } : m);
  st._decode = (url) => Promise.resolve(url.endsWith('/t0.png') ? t0 : /20_-160/.test(url) ? a : /20_-165/.test(url) ? b : null);
  assert.equal(await st.load(), st);
  const z8 = { z: 8, x: 14, y: 112 };
  st.tierFor(z8); await settle(); assert.equal(st.tierFor(z8), 2);
  const T = st.tiers[1], ra = T.loaded.get('20_-160'), rb = T.loaded.get('20_-165');
  assert.ok(ra && rb);
  const sample = (lat, lon, tier) => { const p = I.pixelOf(lat, lon, 14), one = new Float64Array(1); st.sampleGrid(p, p.px, p.py, 1, 1, one, tier); return one[0] ? LO + (one[0] - 1) / 254 * (HI - LO) : null; };
  let n = 0, worst = 0, worst0 = 0;
  for (const [lat, lon, v] of ex.griddap_2026_10_15) {
    const rec = lon >= -160 ? ra : rb;
    const got = sample(lat, lon, 2);
    assert.ok(got !== null, lat + ',' + lon);
    const tol = halfStep(rec.lo, rec.hi) + fixedStep / 254 + 0.004;           // the cell's step, the fixed-range re-coding, bilinear within 0.0005 deg of a node
    assert.ok(Math.abs(got - v) <= tol, `${lat},${lon}: client ${got} vs ERDDAP ${v} (tol ${tol})`);
    worst = Math.max(worst, Math.abs(got - v));
    const g0 = sample(lat, lon, 0);
    assert.ok(g0 !== null && Math.abs(g0 - v) < 0.5, `tier 0 at ${lat},${lon}: ${g0} vs ${v}`);   // a 25x25 block mean vs the point
    worst0 = Math.max(worst0, Math.abs(g0 - v));
    n++;
  }
  assert.ok(n >= 20, n + ' points');
  // the seam at lon -160: both cells, every interior row
  let seamWorst = 0;
  for (let r = 1; r <= 500; r++) {
    const qa = ra.q[r * 502 + 1], qb = rb.q[r * 502 + 501];
    if (!qa || !qb) continue;
    const va = ra.lo + (qa - 1) / 254 * (ra.hi - ra.lo), vb = rb.lo + (qb - 1) / 254 * (rb.hi - rb.lo);
    seamWorst = Math.max(seamWorst, Math.abs(va - vb));
  }
  assert.ok(seamWorst <= halfStep(ra.lo, ra.hi) + halfStep(rb.lo, rb.hi), 'seam ' + seamWorst);
  // the drawn tile and the readout agree on the real data too
  const l = sstLayer(st), el = fakeEl();
  l._tiles['14:112:8'] = { el, coords: z8 }; l._draw(el, z8); assert.equal(el._ovSrcTier, 2);
  const codes = l.tileCodes(z8, new Float64Array(65536), 2);
  for (let py = 0; py < 256; py += 11) for (let px = 0; px < 256; px += 7) {
    const ll = I.tilePixelLatLng(z8, px, py), v = l.readoutAt(ll.lat, ll.lng, 8), c = codes[py * 256 + px];
    if (c) assert.ok(v !== null && Math.abs(v - l._value(c)) < 1e-9); else assert.equal(v, null);
  }
});

test('sampler: the absent rule (a point mostly over absent nodes is absent, else the present ones carry it); the in-flight cap is exact; a pad-only cell is requested', async () => {
  // a cell with a hole: nodes with lon in [-159.5, -159) absent
  const holed = (lat, lon) => (lon >= -159.5 && lon < -159 ? NaN : field(lat, lon));
  const st = fakeStore(manifest(['20_-160', '20_-165', '20_-135', '20_-140', '20_-130', '20_-145'], [], holed), { fn: holed });
  assert.equal(await st.load(), st);
  const z8 = { z: 8, x: 14, y: 112 };
  st.tierFor(z8); await settle(); assert.equal(st.tierFor(z8), 2);
  const one = new Float64Array(1), at = (lat, lon) => { const p = I.pixelOf(lat, lon, 16); st.sampleGrid(p, p.px, p.py, 1, 1, one, 2); return one[0]; };
  assert.equal(at(21.3, -159.25), 0, 'inside the hole');
  assert.ok(at(21.3, -159.75) > 0, 'outside it');
  // the hole's west edge: nodes at -159.50 absent, -159.51 present. A point 0.1 of a node east of -159.51 has the two
  // present nodes at weight 0.9 (kept, their value); 0.8 of a node east, weight 0.2 < 0.25: absent
  assert.ok(at(21.305, -159.509) > 0);
  assert.equal(at(21.305, -159.502), 0, 'weight 0.2 of present nodes: absent (sampleRow\'s rule)');
  // the cap: six listed cells asked for at once -> four downloads, two queued
  const held = fakeStore(manifest(['20_-160', '20_-165', '20_-135', '20_-140', '20_-130', '20_-145', '20_-90', '20_-95'], ['20_-100']), { hold: true });
  assert.equal(await held.load(), held);
  held.request(held.tiers[1], ['20_-160', '20_-165', '20_-135', '20_-140', '20_-130', '20_-145']);
  assert.equal(Object.keys(held.inflight).length, I.MAX_SST_INFLIGHT); assert.equal(held.queue.length, 6 - I.MAX_SST_INFLIGHT); assert.equal(I.MAX_SST_INFLIGHT, 4);
  release(held, 1); await settle();
  assert.equal(Object.keys(held.inflight).length, 4); assert.equal(held.queue.length, 1);
  while (held.fake.held.length) { release(held, 1); await settle(); }
  assert.equal(held.queue.length, 0); assert.equal(Object.keys(held.inflight).length, 0);
  // a pad-only cell: the z8 tile at x 64 starts exactly at lon -90; its own cell is 20_-90, the pad reaches 20_-95
  const pad = { z: 8, x: 64, y: 112 };
  assert.deepEqual(I.cellsForTile(pad, 5, 0), ['20_-90']);
  assert.equal(held.tierFor(pad), 0);
  assert.deepEqual(Object.keys(held.inflight).sort(), ['t1/20_-100', 't2/20_-90'], 'the own cells first, no pad yet');
  const own = held.fake.held.find((h) => /20_-90/.test(h.url)); held.fake.held = held.fake.held.filter((h) => h !== own); own.go(); await settle();
  assert.equal(held.tierFor(pad), 2, 'its own cell in hand: the pad cell is never a condition');
  assert.ok(held.inflight['t2/20_-95'], 'and the pad cell is asked for now');
  while (held.fake.held.length) { release(held, 1); await settle(); }
  assert.ok(held.tiers[1].loaded.has('20_-90') && held.tiers[1].loaded.has('20_-95'));
  held.abortAll(); st.abortAll();
});

test('contour ink: the level\'s colour is judged at the level\'s VALUE, with the deg F offset taken off (dark lines on light colours, light on dark)', async () => {
  // a field around 5 C: dark blue, dark enough for LIGHT isotherms (ink 255); a level read without the offset is
  // judged 17.8 C warmer (32 F / 1.8), where the thermal ramp is light yellow-green, and would be inked dark
  const cool = (lat, lon) => 5 + (lat - 21.15) * 8;                          // the z10 tile spans 21.0-21.3 N: 3.8-6.2 C, the 40 and 42 F lines inside
  const st = fakeStore(manifest(['20_-160', '20_-165'], [], cool), { fn: cool });
  assert.equal(await st.load(), st);
  const l = sstLayer(st), c = { z: 10, x: 56, y: 450 };
  st.tierFor(c); await settle(); assert.equal(st.tierFor(c), 2);
  l.setContours({ step: 2, per: 1.8, off: 32 }, true);
  const codes = l.tileCodes(c, new Float64Array(65536), 2), plain = new Uint8ClampedArray(65536 * 4);
  I.composeTile(codes, null, l._lut, LO, HI, LO, HI, plain);
  const d = plain.slice(); l._contours(c, d, false, 2);
  let lighter = 0, darker = 0;
  for (let i = 0; i < d.length; i += 4) {
    const a = d[i] + d[i + 1] + d[i + 2], b = plain[i] + plain[i + 1] + plain[i + 2];
    if (a > b) lighter++; else if (a < b) darker++;
  }
  assert.ok(lighter > 300 && darker === 0, `light ink on a dark field: ${lighter} lighter, ${darker} darker`);
  const lum = (v) => { const t = Math.max(0, Math.min(255, Math.round((v - LO) / (HI - LO) * 255))), lut = l._lut; return 0.299 * lut[t * 3] + 0.587 * lut[t * 3 + 1] + 0.114 * lut[t * 3 + 2]; };   // t clamped like the ink's
  const levels = [], seen = {};
  for (let py = 0; py < 256; py++) { const lv = Math.floor((l._value(codes[py * 256]) * 1.8 + 32) / 2); if (!seen[lv]) { seen[lv] = 1; levels.push(lv); } }
  assert.ok(levels.length >= 2, 'levels in the tile: ' + levels);
  assert.ok(levels.every((lv) => lum((lv * 2 - 32) / 1.8) <= 170), "the premise: every level's own value is dark");
  assert.ok(levels.some((lv) => lum(lv * 2 / 1.8) > 170), 'the premise: read without the offset, some level is light');
});

// ---- G31 fix round (plan section 44 step 7) ----
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

test('G31-1: a padded tile at the date line lists the cell beyond +-180 (the coast rule, pad 0, does not); it is fetched, watched and kept', async () => {
  const west = { z: 8, x: 0, y: 111 }, east = { z: 8, x: 255, y: 111 };
  assert.deepEqual(I.cellsForTile(west, 5, 0), ['20_-180']);
  assert.deepEqual(I.cellsForTile(east, 5, 0), ['20_175']);
  assert.deepEqual(I.cellsForTile(west, 5, 4).sort(), ['20_-180', '20_175']);
  assert.deepEqual(I.cellsForTile(east, 5, 4).sort(), ['20_-180', '20_175']);
  assert.deepEqual(I.cellsForTile({ z: 8, x: 40, y: 111 }, 5, 4), I.cellsForTile({ z: 8, x: 40, y: 111 }, 5, 0), 'away from the date line nothing changes');
  const st = fakeStore(manifest(['20_-180', '20_175'], ['20_-180', '20_160']));
  assert.equal(await st.load(), st);
  st.tierFor(west); await settle(); assert.equal(st.tierFor(west), 2); await settle();
  assert.ok(st.fake.log.some((u) => u.endsWith('/t2/20_175.png')), 'the wrap cell is asked for');
  const need = st.keysFor(west, {});
  assert.ok(need['t2/20_175'], 'and kept while the tile is on the map');
  const rev = st.rev; st.tiers[1].loaded.delete('20_175'); st.bytes = 0;
  st.request(st.tiers[1], ['20_175']); await settle();
  assert.equal(st.landedAfter(west, 2, rev), true, 'its landing redraws the west tile');
});

test('G31-2: after a failed load the cooldown paces automatic re-loads, but load(true) (a pick, Retry, On) asks at once', async () => {
  let bad = true;
  const m = manifest(['20_-160'], ['20_-160']);
  const st = fakeStore(m, { pointer: () => (bad ? new Error('sst 503') : ptrOf(m)) });
  st.retryMs = 60000;
  assert.equal(await st.load(), null); assert.equal(st.status, 'failed');
  const n = st.fake.log.length; bad = false;
  assert.equal(await st.load(), null); assert.equal(st.fake.log.length, n, 'within the cooldown: no request');
  assert.equal(await st.load(true), st); assert.equal(st.status, 'ok');
});

test('G31-3: the load times each step by its silence: three slow steps pass, one silent step fails; bytes arriving re-arm the timer', async () => {
  const m = manifest(['20_-160'], ['20_-160']);
  const slowStore = (stepMs, progressEvery) => {
    const st = fakeStore(m); st.timeoutMs = 60;
    const j0 = st._json, d0 = st._decode;
    st._json = (...a) => j0(...a).then((v) => wait(stepMs).then(() => v));
    st._decode = (url, sig, expect, prog) => {
      if (!progressEvery) return wait(stepMs).then(() => d0(url, sig, expect));
      return new Promise((res) => { let k = 0; const iv = setInterval(() => { if (prog) prog(); if (++k === 5) { clearInterval(iv); res(d0(url, sig, expect)); } }, progressEvery); });
    };
    return st;
  };
  const a = slowStore(40); assert.equal(await a.load(), a, '3 x 40 ms under a 60-ms stall timer');
  const b = slowStore(90); assert.equal(await b.load(), null); assert.equal(b.status, 'failed', 'a 90-ms silent step');
  const c = slowStore(40, 30); assert.equal(await c.load(), c, 'a 150-ms frame download that keeps sending bytes');
});

test('G31-4: a silent cell download frees its slot when its stall timer fires: a cooldown, a retry, and the stale answer never lands', async () => {
  const st = fakeStore(manifest(['20_-160'], ['20_-160']), { hold: true });
  st.cellStallMs = 30; st.retryMs = 40;
  assert.equal(await st.load(), st);
  const c = { z: 9, x: 31, y: 224 };
  assert.equal(st.tierFor(c), 0); await settle();
  assert.equal(Object.keys(st.inflight).length, 2, 'the 1-km cell and its 0.05-degree stand-in');
  await wait(45);
  assert.equal(Object.keys(st.inflight).length, 0, 'both slots freed');
  assert.equal(typeof st.failed['t2/20_-160'], 'number', 'a cooldown, not a final failure');
  await wait(50);
  const before = st.fake.log.filter((u) => u.endsWith('/t2/20_-160.png')).length;
  st.tierFor(c); await settle();
  assert.equal(st.fake.log.filter((u) => u.endsWith('/t2/20_-160.png')).length, before + 1, 'asked again after the cooldown');
  const stale = st.fake.held.shift(); stale.go(); await settle();
  assert.equal(st.tiers[1].loaded.has('20_-160'), false, 'the dropped download did not land');
  while (st.fake.held.length) { release(st); await settle(); }
  assert.equal(st.tiers[1].loaded.has('20_-160'), true);
});

test('G31-5: level 0 is a line only where it is a value (SST 0 C), not for wave height / period', () => {
  const W = 32, S = W + 2, F = new Float64Array(S * S);
  for (let y = 0; y < S; y++) for (let x = 0; x < S; x++) F[y * S + x] = (x - 16) / 4;      // levels -4..4, level 0 at x = 16
  const run = (zero) => { const rgba = new Uint8ClampedArray(W * W * 4).fill(255); I.contourTile(F, W, rgba, 3, () => 0, 0, 0, W, W, undefined, undefined, zero);
    let n = 0; for (let y = 0; y < W; y++) for (let x = 14; x <= 16; x++) if (rgba[(y * W + x) * 4] !== 255) n++; return n; };
  assert.equal(run(false), 0, 'no line at level 0 (wave height)');
  assert.ok(run(true) > W, 'a line at 0 C');
});

test('G31-6: a tile entirely on land asks for no cells and needs none', async () => {
  const st = fakeStore(manifest(['20_-160'], ['20_-160']));
  assert.equal(await st.load(), st);
  const l = sstLayer(st), c = { z: 9, x: 31, y: 224 }, el = fakeEl();
  l._clip = true; l._masked = () => true; l._landFor = (e) => { e._ovLand = I.LAND_ALL; e._ovLandKey = 'k'; return I.LAND_ALL; };
  const n = st.fake.log.length;
  l._draw(el, c); await settle();
  assert.equal(st.fake.log.length, n, 'nothing fetched'); assert.equal(el._ovSrcTier, 0);
  l._tiles = { a: { el, coords: c } };
  assert.deepEqual(l._srcNeeded(), {}, 'and nothing kept for it');
});

test('G31-10: one least-recently-used order across both tiers (an older 1-km cell goes before a newer 0.05-degree one)', async () => {
  const st = fakeStore(manifest(['20_-160'], ['20_-160']));
  assert.equal(await st.load(), st);
  const big = 13 * 1024 * 1024;
  st.tiers[0].loaded.set('a', { bytes: big, used: 5 }); st.tiers[1].loaded.set('b', { bytes: big, used: 1 });
  st.bytes = 2 * big; st.onNeeded = () => ({});
  st._evict();
  assert.equal(st.tiers[1].loaded.has('b'), false, 'the older (1-km) cell went'); assert.equal(st.tiers[0].loaded.has('a'), true);
  st.release(); assert.equal(st.bytes, 0); assert.equal(st.tiers[0].loaded.size, 0); assert.ok(st.manifest && st.frame, 'release keeps the day');
});

test('G31-19: below tile zoom 7 the SST isotherms are drawn at twice the interval (2 C / 4 F); from zoom 7 at the interval', async () => {
  const lin = (lat, lon) => 20 + (lat - 20) * 2;
  const st = fakeStore(manifest(['20_-160'], ['20_-180', '20_-160'], lin), { fn: lin });
  assert.equal(await st.load(), st);
  const l = sstLayer(st);
  const draw = (coords, cfg) => {
    st.tierFor(coords);
    l.setContours(cfg, true);
    const codes = l.tileCodes(coords, new Float64Array(65536), 1), d = new Uint8ClampedArray(65536 * 4);
    I.composeTile(codes, null, l._lut, LO, HI, LO, HI, d); l._contours(coords, d, true, 1); return d;
  };
  const z6 = { z: 6, x: 3, y: 27 }, z7 = { z: 7, x: 7, y: 55 };
  st.tierFor(z6); st.tierFor(z7); await settle();
  assert.deepEqual(draw(z6, { step: 1, per: 1, off: 0, coarseBelow: 7, zero: true }), draw(z6, { step: 2, per: 1, off: 0, zero: true }), 'zoom 6: every 2 C');
  assert.deepEqual(draw(z7, { step: 1, per: 1, off: 0, coarseBelow: 7, zero: true }), draw(z7, { step: 1, per: 1, off: 0, zero: true }), 'zoom 7: every 1 C');
  assert.notDeepEqual(draw(z6, { step: 1, per: 1, off: 0, zero: true }), draw(z6, { step: 2, per: 1, off: 0, zero: true }));
});

test('G31-17: a cell that only the pad reads redraws its tile when it lands; _draw traces the isotherms at the tier it drew', async () => {
  const st = fakeStore(manifest(['20_-180', '20_175'], ['20_-180', '20_160']), { hold: true });
  assert.equal(await st.load(), st);
  const l = sstLayer(st), west = { z: 8, x: 0, y: 111 }, el = fakeEl();
  l._tiles = { a: { el, coords: west } };
  l.setContours({ step: 1, per: 1, off: 0, zero: true }, true);
  const tiers = [], c0 = l._contours.bind(l); l._contours = (c, d, all, t) => { tiers.push(t); return c0(c, d, all, t); };
  l._draw(el, west); assert.equal(el._ovSrcTier, 0);
  while (st.fake.held.length && !st.fake.held[0].url.endsWith('/t2/20_175.png')) { release(st); await settle(); }
  assert.equal(el._ovSrcTier, 2, 'its own cells landed: drawn at 1 km');
  assert.equal(tiers[tiers.length - 1], 2, 'the isotherms traced at the same tier');
  assert.ok(st.fake.held.some((h) => h.url.endsWith('/t2/20_175.png')), 'the pad cell beyond 180 is on its way');
  let draws = 0; const d0 = l._draw.bind(l); l._draw = (e, c) => { draws++; return d0(e, c); };
  while (st.fake.held.length) { release(st); await settle(); }
  assert.equal(draws, 1, 'the pad landing redrew the tile');
});
