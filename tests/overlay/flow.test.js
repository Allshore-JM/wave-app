'use strict';
// The animation of plan section 21 phase C (static_overlay/overlay.js, no browser): particles along the swell or the
// wind from a bilinear VECTOR flow field (asset 2.9.0: no arrows anywhere), the direction field rules, and the
// FlowAnimator's scheduling with a recording canvas.  node --test tests/overlay/
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// The particles are scattered and respawned with Math.random, so four assertions below failed about one run in 86
// (coast-build client-tests #714 / #719, 2026-11-19): 'moved up the screen', 'moved east', 'respawned instead of moving
// onto the wall' and 'the share that left'.  Those four now hold for any seed (see each), and each test runs on a seeded
// PRNG (mulberry32), reseeded before every test so a test sees the same numbers alone (--test-name-pattern) or in the
// file; the real Math.random is restored after the file.  The production code is untouched.  FLOW_TEST_SEED=<n> runs the
// file on another seed (to check that an assertion holds for most seeds, not only for this one).
const realRandom = Math.random;
const SEED = Number(process.env.FLOW_TEST_SEED || 20261119) >>> 0;
function seedRandom(seed) {
  let a = seed >>> 0;
  Math.random = function () {
    a = (a + 0x6D2B79F5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
test.beforeEach(() => seedRandom(SEED));
test.after(() => { Math.random = realRandom; });

function load() {
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  global.L = {
    GridLayer: {
      prototype: { initialize(o) { this.options = o; this._tiles = {}; } },
      extend(p) { function C(o) { p.initialize.call(this, o); } C.prototype = Object.assign({ setOpacity() {}, addTo() { return this; } }, p); return C; }
    },
    DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} }
  };
  global.window = global;
  new Function(src)();
  return global.AllshoreOverlay._internals;
}
const I = load();
const GRID = { cols: 1440, rows: 721, lon0: -180, lat0: 90, dlon: 0.25, dlat: -0.25, registration: 'center', lon_periodic: true };
const HALF = { cols: 720, rows: 361, lon0: -180, lat0: 90, dlon: 0.5, dlat: -0.5, registration: 'center', lon_periodic: true };
const HS = { lo: 0, hi: 15, legend: [0, 12], units: 'm', interpolation: 'bilinear' };
const TPDEF = { lo: 1, hi: 30, legend: [4, 22], units: 's', interpolation: 'bilinear' };
const WIND = { lo: 0, hi: 41.15555555555556, legend: [0, 30.866666666666667], units: 'm/s', interpolation: 'bilinear' };
const PDIR = { lo: 0, hi: 360, legend: [0, 360], units: 'deg', interpolation: 'circular', resolutions: ['full', 'half'], circular: true, convention: 'from' };
const WDIR = Object.assign({}, PDIR, { resolutions: ['half'] });
const code = (deg) => 1 + (Math.round(deg * 254 / 360) % 254);          // the job's circular coding
const codeOf = (v, f) => 1 + Math.round((v - f.lo) / (f.hi - f.lo) * 254);
function frame(cols, rows, fn) {
  const q = new Uint8Array(cols * rows);
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) q[r * cols + c] = fn(r, c);
  return { q, cols, rows };
}
function layer(fr, grid, field, fdef, opacity) {
  const l = new I.ModelGridLayer({ opacity: opacity === undefined ? 0.65 : opacity });
  l.setFrame(fr, grid, field, fdef, I.buildLut(field, fdef.legend), { step: 0, valid_utc: '2026-09-22T12:00:00Z' });
  return l;
}
function viewAt(lat, lng, z, w, h) { const p = I.forwardPixel(lat, lng, z); return { z, w, h, ox: p.x - w / 2, oy: p.y - h / 2, zt: Math.round(z) }; }
const at = (vf, x, y) => { const c = ((y / vf.s) | 0) * vf.cols + ((x / vf.s) | 0); return [vf.u[c], vf.v[c]]; };

test('sampleRow is the layer sampler; dirFieldOk and dirRes: only circular FROM fields, wind direction half only, pdir half at the default zoom 6, full from 6.5 with hysteresis', () => {
  const hs = frame(720, 361, (r, c) => 1 + ((r * 7 + c * 3) % 254)), l = layer(hs, HALF, 'hs', HS);
  const colPos = new Float64Array([10.3, 400.75, 719.5]), out = new Float64Array(3), viaLayer = new Float64Array(3);
  I.sampleRow(hs, false, 100.4, colPos, 3, out, 0); l._codeRow(100.4, colPos, 3, viaLayer, 0);
  assert.deepEqual(Array.from(out), Array.from(viaLayer));
  assert.equal(I.dirFieldOk(PDIR), true); assert.equal(I.dirFieldOk(WDIR), true);
  assert.equal(I.dirFieldOk(HS), false); assert.equal(I.dirFieldOk(Object.assign({}, PDIR, { convention: 'to' })), false);
  assert.equal(I.dirFieldOk(Object.assign({}, PDIR, { resolutions: [] })), false); assert.equal(I.dirFieldOk(undefined), false);
  for (const z of [3, 7, 9, 11]) assert.equal(I.dirRes(WDIR, z, null), 'half');
  assert.equal(I.dirRes(PDIR, 6, null), 'half', 'the default zoom: half'); assert.equal(I.dirRes(PDIR, 6.5, null), 'full', 'zoomed in: full'); assert.equal(I.dirRes(PDIR, 6.3, null), 'half');
  assert.equal(I.dirRes(PDIR, 6.3, 'full'), 'full', 'hysteresis'); assert.equal(I.dirRes(PDIR, 6, 'full'), 'half', 'zoom buttons: 6 -> 6.5 -> 6 returns to half');
  assert.equal(I.dirRes(PDIR, 6.2, 'full'), 'half'); assert.equal(I.dirRes(PDIR, 6.3, 'half'), 'half');
  assert.equal(I.dirRes(PDIR, 3, null), 'half'); assert.equal(I.dirRes(PDIR, 11, 'half'), 'full');
  assert.equal(I.DIR_FIELDS.hs, 'pdir'); assert.equal(I.DIR_FIELDS.tp, 'pdir'); assert.equal(I.DIR_FIELDS.wind, 'wdir');
});

test('vectorNodes: the field value under each direction node times its FROM direction (TOWARD vectors); the speed floors; code 255 and missing nodes absent', () => {
  const wind = frame(720, 361, () => codeOf(10, WIND)), wdir = frame(720, 361, (r, c) => (c === 5 ? 255 : c === 6 ? 0 : code(270)));
  const N = I.vectorNodes('wind', layer(wind, HALF, 'wind', WIND), wdir, HALF);
  assert.equal(N.cols, 720); assert.equal(N.rows, 361);
  const k = 100 * 720 + 10;
  assert.ok(Math.abs(N.U[k] - 30) < 0.5 && Math.abs(N.V[k]) < 0.5, 'from the west: 30 px/s east (10 m/s * 3)'); assert.equal(N.M[k], 1);
  assert.equal(N.M[100 * 720 + 5], 0, 'code 255 is absent'); assert.equal(N.M[100 * 720 + 6], 0, 'a missing direction is absent');
  // a full-resolution field under a half-resolution direction: the node reads the field at the same place
  const hsFull = frame(1440, 721, (r, c) => codeOf(r % 2 === 0 && c % 2 === 0 ? 3 : 0.05, HS)), pdir = frame(720, 361, () => code(180));
  const H = I.vectorNodes('hs', layer(hsFull, GRID, 'hs', HS), pdir, HALF);
  assert.ok(Math.abs(H.V[k] - 17) < 0.3 && Math.abs(H.U[k]) < 0.3, 'from the south: north at 8 + 3 * 3 m = 17 px/s'); assert.equal(H.M[k], 1);
  const flat = I.vectorNodes('hs', layer(frame(1440, 721, () => codeOf(0.05, HS)), GRID, 'hs', HS), pdir, HALF);
  assert.equal(flat.M[k], 0, 'no swell particles under 0.1 m');
  const T = I.vectorNodes('tp', layer(frame(1440, 721, () => codeOf(10, TPDEF)), GRID, 'tp', TPDEF), pdir, HALF);
  assert.ok(Math.abs(T.V[k] - 15) < 0.3, 'period: 1.5 px/s per second');
  assert.equal(I.vectorNodes('tp', layer(frame(1440, 721, () => codeOf(2, TPDEF)), GRID, 'tp', TPDEF), pdir, HALF).M[k], 0, 'no particles under the 3 s floor');
});

test('flowField: bilinear VECTORS (a cyclone turns smoothly and stops at its eye), the Mercator stretch capped at 3, nothing without data', () => {
  const wind = frame(720, 361, () => codeOf(10, WIND));
  const cx = 100, cy = 100;                                                                   // a vortex around node (100, 100): calm at the centre
  const vortex = frame(720, 361, (r, c) => { const dx = c - cx, dy = cy - r; if (!dx && !dy) return 0; const bearingTo = (Math.atan2(dx, dy) * 180 / Math.PI + 360) % 360; return code((bearingTo + 180 + 90) % 360); });
  const l = layer(wind, HALF, 'wind', WIND), N = I.vectorNodes('wind', l, vortex, HALF);
  const lat = 90 - cy * 0.5, lng = -180 + cx * 0.5, view = viewAt(lat, lng, 6, 400, 400);   // the eye at the view centre
  const vf = I.flowField(view, 4, N, HALF, l, false);
  const centre = at(vf, 200, 200), off = at(vf, 260, 200);                                    // ~a half cell (22 px) east of the eye
  assert.ok(Math.hypot(centre[0], centre[1]) < 8, 'near the eye the vectors cancel: ' + Math.hypot(centre[0], centre[1]).toFixed(1) + ' px/s');
  assert.ok(Math.hypot(off[0], off[1]) > 15, 'off the eye the flow runs: ' + Math.hypot(off[0], off[1]).toFixed(1));
  const half = at(vf, 211, 260);                                                              // between two nodes whose directions differ by ~90 deg
  assert.ok(Math.abs(half[0]) > 3 && Math.abs(half[1]) > 3, 'a blend of both neighbours, not a snap: ' + half.map((x) => x.toFixed(1)));
  const west = I.vectorNodes('wind', l, frame(720, 361, () => code(270)), HALF);
  const eq = I.flowField(viewAt(0, -158, 4, 200, 200), 4, west, HALF, l, false), k = at(eq, 100, 100);
  assert.ok(Math.abs(k[0] - 30) < 0.6 && Math.abs(k[1]) < 0.6, 'equator: 30 px/s east, v ~ 0 (within the coding error)');
  const polar = I.flowField(viewAt(84, -158, 4, 200, 200), 4, west, HALF, l, false), kp = at(polar, 100, 100);
  assert.ok(Math.abs(kp[0] - 90) < 1, 'capped at 3x: ' + kp[0]);
  const none = I.flowField(view, 4, I.vectorNodes('wind', l, frame(720, 361, () => 0), HALF), HALF, l, false);
  assert.equal(at(none, 200, 200)[0], 0, 'no direction: no vector');
  const south = I.flowField(viewAt(20, -158, 5, 200, 200), 4, I.vectorNodes('wind', l, frame(720, 361, () => code(180)), HALF), HALF, l, false);
  assert.ok(at(south, 100, 100)[1] < -20, 'from the south: screen v negative (up)');
});

test('flowField: swell particles run only on drawn water (the tile land masks, unwrapped tile keys west of the dateline)', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(300));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6; l._clip = true;
  const N = I.vectorNodes('hs', l, pdir, HALF), view = viewAt(21.5, -158, 6, 300, 300);
  assert.equal(I.flowField(view, 4, N, HALF, l, true).u.some((x) => x !== 0), false, 'clipped and no tiles: nothing');
  const tilesFor = (v, mask) => { for (let y = 0; y <= v.h + 256; y += 64) for (let x = 0; x <= v.w + 256; x += 64) { const tx = Math.floor((v.ox + x) / 256), ty = Math.floor((v.oy + y) / 256); l._tiles[tx + ':' + ty + ':' + v.zt] = { el: { _ovLand: mask }, coords: { x: tx, y: ty, z: v.zt } }; } };
  tilesFor(view, null);
  const sea = I.flowField(view, 4, N, HALF, l, true), s0 = at(sea, 150, 150);
  assert.ok(s0[0] > 5 && s0[1] > 5, 'ocean masks: swell from 300 runs to the south-east: ' + s0.map((x) => x.toFixed(1)));
  for (const k in l._tiles) l._tiles[k].el._ovLand = I.LAND_ALL;
  assert.equal(I.flowField(view, 4, N, HALF, l, true).u.some((x) => x !== 0), false, 'all-land tiles: nothing');
  const halfMask = new Uint8Array(256 * 256); for (let i = 0; i < 256 * 256; i++) halfMask[i] = (i % 256) < 128 ? 255 : 0;   // the left half of every tile is land
  for (const k in l._tiles) l._tiles[k].el._ovLand = halfMask;
  const mixed = I.flowField(view, 4, N, HALF, l, true);
  let land = 0, water = 0;
  for (let j = 0; j < mixed.rows; j++) for (let i = 0; i < mixed.cols; i++) { const X = view.ox + i * 4, px = Math.floor(X - Math.floor(X / 256) * 256); const on = mixed.u[j * mixed.cols + i] !== 0; if (px < 128) land += on; else water += !on; }
  assert.equal(land, 0, 'nothing over the land half'); assert.equal(water, 0, 'everything over the water half');
  const west = viewAt(21.5, -181, 6, 300, 300); tilesFor(west, null);                      // west of the dateline: negative tile x, as Leaflet keys them
  assert.ok(Object.keys(l._tiles).some((k) => k.startsWith('-')), 'negative tile x registered');
  assert.ok(I.flowField(west, 4, N, HALF, l, true).u.some((x) => x !== 0), 'particles west of the dateline');
});

// ---- the animator with a recording canvas ----
function fakeCtx() {
  const c = { ops: [], fills: [], globalCompositeOperation: 'source-over', fillStyle: '' };
  for (const m of ['clearRect', 'beginPath', 'moveTo', 'lineTo', 'stroke', 'fillRect', 'setTransform']) c[m] = function () { c.ops.push(m); if (m === 'fillRect') { const a = /,([0-9.]+)\)$/.exec(c.fillStyle); c.fills.push(a ? Number(a[1]) : 1); } };
  return c;
}
function animator(field, l, dframe, dgrid, zoom, size) {
  const ctx = fakeCtx(), events = {}, sz = size || [800, 600];
  const map = { on(ev, fn) { (events[ev] = events[ev] || []).push(fn); }, off(ev, fn) { events[ev] = (events[ev] || []).filter((f) => f !== fn); },
    getPane: () => null, createPane: () => ({ style: {}, appendChild() {} }), getSize: () => ({ x: sz[0], y: sz[1] + 300 }), getContainer: () => ({ clientWidth: sz[0], clientHeight: sz[1] }),
    getPixelBounds: () => ({ min: { x: 10000, y: 9000 } }), getZoom: () => zoom || 6, containerPointToLayerPoint: () => ({ x: -3, y: 7 }) };
  const canvas = { style: {}, width: 0, height: 0, getContext: () => ctx, remove() { canvas.removed = true; } };
  global.document = { createElement: () => canvas, hidden: false };
  const fa = new I.FlowAnimator(map, l);
  fa.scheduled = 0; fa.raf = (fn) => { fa.scheduled++; fa.pending = fn; return 1; }; fa.caf = () => { fa.pending = null; };
  fa.attach(); fa.setField(field); fa.setData(dframe, dgrid, { step: 0 });
  return { fa, ctx, canvas, events, map };
}
const strokes = (ctx) => ctx.ops.filter((o) => o === 'stroke').length;
// A live particle the next frames can neither carry out of the view nor age out (its index * 4): particle 0 itself was
// sometimes born within a pixel of the edge it moves to (or at the end of its life) and respawned, failing 'it moved'.
const steady = (fa, margin) => {
  const P = fa.particles;
  for (let i = 0; i < fa.count; i++) {
    const k = i * 4;
    if (P[k + 2] >= 0 && P[k + 3] - P[k + 2] > 500 && P[k] > margin && P[k] < fa.v.w - margin && P[k + 1] > margin && P[k + 1] < fa.v.h - margin) return k;
  }
  return -1;
};

test('swell particles on wave height: seeded from the container size, four strokes a frame (head and tail, halo + core) over a cleared canvas, moving along the swell, longer-lived than wind', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(180));   // from the south: up the screen
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa, ctx, canvas } = animator('hs', l, pdir, HALF);
  assert.equal(fa.mode, 'particles'); assert.ok(fa.vf && fa.nodes); assert.equal(fa.count, Math.round(800 * 600 / 900));
  assert.equal(canvas.width, 800); assert.equal(canvas.height, 600, 'the container size, not Leaflet stale getSize'); assert.equal(canvas.style.transform, 'translate3d(-3px,7px,0)');
  assert.equal(fa.scheduled, 1, 'one animation frame scheduled after the rebuild');
  const s = steady(fa, 50); assert.ok(s >= 0);
  const y0 = fa.particles[s + 1], x0 = fa.particles[s];
  fa.pending(100); fa.pending(150);
  assert.equal(strokes(ctx), 8, 'head + tail, halo + core, two frames'); assert.equal(ctx.ops.filter((o) => o === 'fillRect').length, 0, 'no compositing fade');
  assert.equal(ctx.ops.filter((o) => o === 'clearRect').length, 3, 'the canvas is cleared every frame (and once at the rebuild)');
  assert.ok(fa.particles[s + 1] < y0 && Math.abs(fa.particles[s] - x0) < 0.2, 'a particle moved up the screen (from the south)');
  assert.ok(fa.particles[3] >= I.PARTICLE_LIFE_MS.hs[0] && fa.particles[3] <= I.PARTICLE_LIFE_MS.hs[1], 'swell life 2-4.5 s');
  fa.stop(); assert.equal(fa.pending, null); assert.equal(fa.active, false);
  const flat = layer(frame(1440, 721, () => codeOf(0.05, HS)), GRID, 'hs', HS); flat._tileZoom = 6;
  assert.equal(animator('hs', flat, pdir, HALF).fa.vf.u.some((x) => x !== 0), false, 'a flat sea has no flow');
  const floor = layer(frame(1440, 721, () => codeOf(1.6, TPDEF)), GRID, 'tp', TPDEF); floor._tileZoom = 6;
  assert.equal(animator('tp', floor, pdir, HALF).fa.vf.u.some((x) => x !== 0), false, 'the model no-wave floor (Tp 1.6 s) has no flow on the period layer');
});

test('wind particles: adapt their count to the frame budget, keep the trails across a step, follow the view on a resize; the midpoint step keeps a particle on a circle', () => {
  const wind = frame(720, 361, () => codeOf(10, WIND)), wdir = frame(720, 361, () => code(270)), wdir2 = frame(720, 361, () => code(270));
  const l = layer(wind, HALF, 'wind', WIND); l._tileZoom = 6;
  const { fa, ctx, map } = animator('wind', l, wdir, HALF);
  assert.equal(fa.mode, 'particles'); assert.ok(fa.vf.s === 8 && fa.vf.cols === 101, 'a 22-px cell at zoom 6: an 8-px lattice');
  const s = steady(fa, 50); assert.ok(s >= 0);
  const x0 = fa.particles[s];
  fa.pending(100); fa.pending(150); assert.ok(fa.particles[s] > x0, 'moved east');
  fa.ema = 20; fa.adaptAt = 0; fa._adapt(20, 5000); assert.ok(fa.count < Math.round(800 * 600 / 900), 'over budget: fewer particles');
  fa.ema = 0.5; fa.adaptAt = 0; fa._adapt(0.5, 9000); assert.ok(fa.count > I.PARTICLE_MIN, 'room: back up');
  const clears = ctx.ops.filter((o) => o === 'clearRect').length, nodes0 = fa.nodes;
  fa.setData(wdir2, HALF, { step: 3 }); assert.equal(ctx.ops.filter((o) => o === 'clearRect').length, clears, 'a new step does not wipe the tails');
  assert.ok(fa.nodesFor === wdir2 && fa.nodes.U === nodes0.U, 'the node vectors are recomputed for the new step, in the same buffers (G11 m32 / m05)');
  const full = frame(1440, 721, () => code(270)); fa.setData(full, GRID, { step: 6 }); assert.equal(fa.nodes.cols, 1440, 'a resolution change reallocates the node buffers');
  fa.setData(wdir, HALF, { step: 9 });
  const vf0 = fa.vf; fa._rebuild(); assert.ok(fa.vf.u !== vf0.u && fa._vfSpare === vf0, 'the old lattice is kept intact (section 100)'); fa._rebuild(); assert.equal(fa.vf.u, vf0.u, 'the two lattice buffers alternate (no allocation)');
  map.getContainer = () => ({ clientWidth: 1600, clientHeight: 1200 }); fa._rebuild();
  assert.equal(fa.target, Math.round(1600 * 1200 / 900), 'the target follows the view'); assert.ok(fa.count <= fa.target);
  assert.equal(ctx.ops.filter((o) => o === 'clearRect').length, clears + 1, 'a moved view starts clean');
  // a synthetic solid-body rotation on the lattice: the midpoint step keeps the radius (an Euler step spirals out)
  const vf = fa.vf, cx = 800, cy = 600, omega = 1.5;                                    // rad/s
  for (let j = 0; j < vf.rows; j++) for (let i = 0; i < vf.cols; i++) { const x = i * vf.s - cx, y = j * vf.s - cy; vf.u[j * vf.cols + i] = -omega * y; vf.v[j * vf.cols + i] = omega * x; }
  const P = fa.particles; P[0] = cx + 200; P[1] = cy; P[2] = 0; P[3] = 1e9; fa.count = 1;
  for (let k = 0; k < 120; k++) fa._renderParticles(16);                                // ~1.9 s, a bit less than one turn
  const r = Math.hypot(P[0] - cx, P[1] - cy), euler = 200 * (Math.pow(Math.sqrt(1 + (omega * 0.016) ** 2), 120) - 1);
  assert.ok(Math.abs(r - 200) < 6, 'radius kept within 3 %: ' + r.toFixed(1) + ' (an Euler step would spiral out by ~' + euler.toFixed(0) + ' px)');
});

test('suspend / resume count nested moves and zooms with a dirty flag, a view reset rebuilds, a hidden document stops the loop, reduced motion animates nothing, detach leaves nothing', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(90));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa, events, canvas, map } = animator('hs', l, pdir, HALF);
  const vf0 = fa.vf;
  events.viewreset[0](); assert.ok(fa.vf !== vf0 && fa._vfSpare === vf0, 'a view reset rebuilds (the old lattice kept as the spare)');
  events.zoomstart[0](); events.movestart[0]();                              // a zoom fires both
  assert.equal(fa.suspended, 2); assert.equal(fa.active, false); assert.equal(canvas.style.visibility, 'hidden');
  fa.suspend();                                                              // the tab hidden meanwhile
  map.getZoom = () => 8; l._tileZoom = 8; map.getPixelBounds = () => ({ min: { x: 40000, y: 36000 } });
  events.zoomend[0](); events.moveend[0]();                                  // the zoom ends while hidden
  assert.equal(fa.suspended, 1); assert.equal(fa.v.z, 6, 'nothing rebuilt while hidden');
  fa.resume();                                                               // shown: the remembered rebuild runs
  assert.equal(fa.suspended, 0); assert.equal(fa.active, true); assert.equal(fa.v.z, 8); assert.equal(canvas.style.visibility, '');
  global.document.hidden = true; const sched = fa.scheduled; fa.pending(200); assert.equal(fa.active, false); assert.equal(fa.scheduled, sched, 'nothing scheduled while hidden'); assert.equal(fa.rafId, null);
  global.document.hidden = false; fa.resume(); assert.equal(fa.active, true);
  fa.clearData(); assert.equal(fa.active, false); assert.equal(fa.dir, null); assert.equal(fa.nodes, null);
  fa.setData(pdir, HALF, { step: 0 }); assert.equal(fa.active, true);
  assert.equal(typeof l.onRedraw, 'function', 'the animator follows the layer');
  const vf1 = fa.vf; l.onRedraw(); assert.ok(fa.vf !== vf1 && fa._vfSpare === vf1, 'a coast chunk landing (tiles redrawn) rebuilds the flow');
  fa.detach(); assert.equal(fa.active, false); assert.equal(fa.canvas, null); assert.equal(canvas.removed, true); assert.equal(Object.values(events).flat().length, 0, 'map listeners removed');
  assert.equal(l.onRedraw, null, 'detached from the layer');
  const l2 = layer(hs, GRID, 'hs', HS); l2._tileZoom = 6;                    // a direction delivered without a field frame cannot crash a frame
  const bare = animator('hs', l2, null, null); l2.clear(); bare.fa.setData(pdir, HALF, { step: 0 });
  assert.equal(bare.fa.vf, null); assert.doesNotThrow(() => { bare.fa.resume(); if (bare.fa.pending) bare.fa.pending(100); bare.fa._renderParticles(16); }); assert.equal(bare.fa.active, false);
  global.matchMedia = () => ({ matches: true });                              // prefers-reduced-motion: nothing at all
  const s = animator('hs', l, pdir, HALF);
  assert.equal(s.fa.mode, null); assert.equal(s.fa.active, false); assert.equal(s.fa.scheduled, 0); assert.equal(strokes(s.ctx), 0, 'nothing drawn');
  s.fa.suspend(); s.fa.resume(); assert.equal(s.fa.active, false); assert.equal(s.fa.scheduled, 0, 'no loop after hide / show either');
  delete global.matchMedia;
});

// ---- G11 ----

test('G11 P2-1 / P2-2: tails come from a per-particle history (cleared on respawn), a particle next to a cell without flow respawns before drifting onto it', () => {
  const wind = frame(720, 361, () => codeOf(10, WIND)), wdir = frame(720, 361, () => code(270));
  const l = layer(wind, HALF, 'wind', WIND); l._tileZoom = 6;
  const { fa, ctx } = animator('wind', l, wdir, HALF);
  const P = fa.particles; P[0] = 100; P[1] = 300; P[2] = 0; P[3] = 1e9; fa.count = 1; fa.histN[0] = 0;
  for (let k = 0; k < 30; k++) fa._renderParticles(33);                                   // ~1 s: history points every 66 ms
  assert.ok(fa.histN[0] >= I.TRAIL_POINTS, 'history filled: ' + fa.histN[0]);
  const tailOps = ctx.ops.slice(-6); assert.ok(tailOps.includes('lineTo') && tailOps.includes('stroke'), 'the tail is drawn');
  const before = fa.histN[0]; fa._respawn(0, fa.v); assert.equal(fa.histN[0], 0, 'a respawn clears the history'); assert.ok(before > 0);
  // a wall of no-flow cells east of x = 400: a particle at x = 396 (its right nodes without flow) respawns at once
  const vf = fa.vf; for (let j = 0; j < vf.rows; j++) for (let i = 0; i < vf.cols; i++) { const wall = i * vf.s >= 400; vf.u[j * vf.cols + i] = wall ? 0 : 30; vf.v[j * vf.cols + i] = 0; }
  // a respawn starts a new life (age 0, or a negative dormant wait) somewhere else; a step would have aged it to 16 ms.
  // (A spot test 'more than 8 px away' failed when the random new spot fell within 8 px, e.g. dormant at 399.8, 292.7.)
  P[0] = 396; P[1] = 300; P[2] = 0;
  assert.equal(fa._flowing(396, 300), false); fa._renderParticles(16);
  assert.ok(P[2] <= 0 && !(P[0] === 396 && P[1] === 300), 'respawned instead of moving onto the wall: ' + P[0] + ',' + P[1] + ' age ' + P[2]);
  P[0] = 380; P[1] = 300; P[2] = 0; assert.equal(fa._flowing(380, 300), true); fa._renderParticles(16); assert.ok(Math.abs(P[0] - 380.5) < 0.2, 'a particle with flow on all four nodes moves');
});

test('G11 P2-3: bilinear reads between lattice cells (m35), absent nodes excluded and the 0.25 rule (m36 / m07), the column wrap at the last column (m08 / m30), fractional-zoom tile keys (m13 / m14), ageing (m21 / m23)', () => {
  const wind = frame(720, 361, () => codeOf(10, WIND)), l = layer(wind, HALF, 'wind', WIND); l._tileZoom = 6;
  const { fa } = animator('wind', l, frame(720, 361, () => code(270)), HALF);
  const vf = fa.vf; vf.u.fill(0); vf.v.fill(0); vf.u[0] = 10; vf.u[1] = 30;                 // cells (0,0) and (1,0)
  const out = new Float64Array(2); fa._velocity(vf.s * 0.5, 0, out); assert.ok(Math.abs(out[0] - 20) < 1e-9, 'halfway: the mean, not a snap');
  const one = frame(720, 361, (r, c) => (r === 100 && c === 100 ? code(270) : 0)), N = I.vectorNodes('wind', l, one, HALF);
  const v = (lat, lng) => { const f = I.flowField(viewAt(lat, lng, 6, 40, 40), 4, N, HALF, l, false); return at(f, 20, 20); };
  const lat = 90 - 100 * 0.5, lon = -180 + 100 * 0.5, sec = 1 / Math.cos(lat * Math.PI / 180);
  assert.ok(Math.abs(v(lat, lon + 0.25)[0] - 30 * sec) < 1, 'one node at weight 0.5: its vector (not blended with absent ones), times the Mercator stretch'); assert.equal(v(lat, lon + 0.4)[0], 0, 'weight 0.2: nothing');
  const wrap = frame(720, 361, (r, c) => (c === 719 || c === 0 ? code(270) : 0)), NW = I.vectorNodes('wind', l, wrap, HALF);
  const fw = I.flowField(viewAt(lat, 179.9, 6, 40, 40), 4, NW, HALF, l, false); assert.ok(Math.abs(at(fw, 20, 20)[0] - 30 * sec) < 1, 'between column 719 and column 0 the flow is defined');
  const hs = frame(1440, 721, () => codeOf(3, HS)), lh = layer(hs, GRID, 'hs', HS); lh._tileZoom = 6; lh._clip = true;
  const NH = I.vectorNodes('hs', lh, frame(720, 361, () => code(300)), HALF), view = viewAt(21.5, -158, 6.4, 200, 200);
  const scale = Math.pow(2, 0.4); for (let y = -256; y <= 456; y += 64) for (let x = -256; x <= 456; x += 64) { const tx = Math.floor((view.ox + x) / scale / 256), ty = Math.floor((view.oy + y) / scale / 256); lh._tiles[tx + ':' + ty + ':6'] = { el: { _ovLand: null }, coords: { x: tx, y: ty, z: 6 } }; }
  assert.ok(I.flowField(view, 4, NH, HALF, lh, true).u.some((x) => x !== 0), 'tiles found at zoom 6.4 through the tile zoom 6 keys');
  for (const k in lh._tiles) delete lh._tiles[k]; assert.equal(I.flowField(view, 4, NH, HALF, lh, true).u.some((x) => x !== 0), false, 'no tiles: nothing');
  const P = fa.particles; vf.u.fill(30); vf.v.fill(0); P[0] = 100; P[1] = 100; P[2] = 0; P[3] = 100; fa.count = 1;
  fa._renderParticles(40); assert.equal(P[2], 40); fa._renderParticles(40); assert.equal(P[2], 80); fa._renderParticles(40); assert.equal(P[2], 120, 'still alive at the check'); fa._renderParticles(40); assert.ok(P[2] < 80 && P[3] >= I.PARTICLE_LIFE_MS.wind[0], 'aged out at 120 > 100 ms: respawned with a wind life');
});

test('G12: a half-resolution field under a full-resolution direction (a phone zoomed in to 6.5-7.5): each direction node reads the field between the half nodes', () => {
  const half = frame(720, 361, (r, c) => codeOf(c % 2 ? 4 : 2, HS));                     // 2 m and 4 m alternating half columns
  const l = layer(half, HALF, 'hs', HS), pdir = frame(1440, 721, () => code(180));
  const N = I.vectorNodes('hs', l, pdir, GRID), row = 200 * 1440;                          // full row 200 = half row 100
  const speed = (m) => 8 + 3 * m;
  assert.ok(Math.abs(N.V[row + 20] - speed(2)) < 0.2, 'on a half node (2 m): ' + N.V[row + 20]);
  assert.ok(Math.abs(N.V[row + 21] - speed(3)) < 0.2, 'between the half nodes: the mean height (3 m): ' + N.V[row + 21]);
  assert.ok(Math.abs(N.V[row + 22] - speed(4)) < 0.2, 'on the next half node (4 m)');
});


// ---- plan section 42 step 3: the look table, the playback rate, the watchdog, the canvas after a zoom, the readout ----
test('section 42: the one particle look (the owner\'s pick D, 2026-10-13): the density before section 42, 2-px cores over 4-px halos, brighter tails', () => {
  assert.equal(I.LOOK.px2, 900, 'the density: one particle per 30x30 px (a denser look cost the 1080p frame rate)');
  assert.deepEqual(I.LOOK.style, { wind: { width: 1.6, halo: 3.4 }, jet: { width: 1.6, halo: 3.4 }, hs: { width: 2.0, halo: 4.0 }, tp: { width: 2.0, halo: 4.0 } });   // jet: the wind's look (section 106)
  assert.equal(I.LOOK.head, 0.95); assert.equal(I.LOOK.headHalo, 0.4); assert.equal(I.LOOK.tail, 0.55); assert.equal(I.LOOK.tailHalo, 0.25);
  assert.equal(I.PARTICLE_PX2, I.LOOK.px2); assert.equal(I.FLOW_STYLE, I.LOOK.style);
  assert.equal(I.LOOKS, undefined, 'the candidate table is gone'); assert.equal(I.lookFor, undefined);
  // the strokes use the look: a frame draws the head core at 2 px / 0.95 and the tail at 0.55 for wave height
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(180));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa, ctx } = animator('hs', l, pdir, HALF);
  const seen = []; const c = fa.ctx; const st = c.stroke; c.stroke = function () { seen.push([c.lineWidth, c.strokeStyle]); return st.apply(this, arguments); };
  fa._renderParticles(16);
  assert.deepEqual(seen, [[4, 'rgba(0,0,0,0.4)'], [2, 'rgba(255,255,255,0.95)'], [4, 'rgba(0,0,0,0.25)'], [2, 'rgba(255,255,255,0.55)']]);
  void ctx;
});

test('section 42: the rate: particles move rate times as far per frame, their tails sample rate times as often, lifetimes stay in real time; the default rate is 1', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(270));   // from the west: particles run east (x grows)
  const run = (rate) => {
    const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
    const { fa } = animator('hs', l, pdir, HALF);
    fa.setRate(rate);
    const P = fa.particles; fa.count = 1; P[0] = 100; P[1] = 300; P[2] = 0; P[3] = 1e9;
    for (let i = 0; i < 1000; i++) fa.hist[i] = 0; fa.histN[0] = 0; fa.histT[0] = 0;
    const dx = []; let samples = 0;
    for (let k = 0; k < 10; k++) { const x0 = P[0]; fa._renderParticles(16); dx.push(P[0] - x0); samples = fa.histN[0]; }
    return { dx: dx.slice(2).reduce((a, b) => a + b, 0) / 8, age: P[2], samples };
  };
  const one = run(1), four = run(4);
  assert.ok(one.dx > 0, 'moves east'); assert.ok(Math.abs(four.dx / one.dx - 4) < 0.05, 'four times as far: ' + four.dx / one.dx);
  assert.equal(one.age, 160); assert.equal(four.age, 160, 'age in real time');
  assert.ok(four.samples > one.samples, 'the tail samples more often at the higher rate (' + one.samples + ' -> ' + four.samples + ')');
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6; const { fa } = animator('hs', l, pdir, HALF);
  assert.equal(fa.rate, 1); fa.setRate(0); assert.equal(fa.rate, 1, 'a bad rate is 1');
});

test('section 42: the watchdog restarts an animator that should run and does not (a stuck suspend, a dead loop), never while the map is busy, hidden or without data', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(180));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa, map } = animator('hs', l, pdir, HALF);
  assert.ok(fa.watch !== null, 'the watchdog timer exists');
  fa.pending(16);                                                              // one frame drawn: fresh
  assert.ok(fa.lastFrameAt > 0, 'a frame records its time (G29 F11)');
  assert.equal(fa.active, true); fa._check(); assert.equal(fa.healed, 0, 'healthy: nothing');
  fa.lastFrameAt = performance.now() - 3000; fa.stop(); fa._start(); fa._check(); assert.equal(fa.healed, 0, 'a loop just (re)started is not a dead one (G29 B-P3-2)');
  assert.ok(performance.now() - fa.lastFrameAt < 1000);
  // a suspend that never ended
  fa.suspend(); assert.equal(fa.suspended, 1); assert.equal(fa.active, false);
  fa._check(); assert.equal(fa.healed, 1); assert.equal(fa.suspended, 0); assert.equal(fa.active, true); assert.match(fa.log[fa.log.length - 1][0], /heal:suspended/);
  // the loop stopped drawing (no frame for WATCHDOG_MS) although active
  fa.lastFrameAt = performance.now() - 3000; fa._check(); assert.equal(fa.healed, 2); assert.match(fa.log[fa.log.length - 1][0], /heal:dead/);
  fa.pending(16);
  // not while something is in progress on the map; a map busy for BUSY_STUCK_MS in one go is a flag stuck true (G29 A-P3-3)
  fa.suspend(); map._animatingZoom = true; fa._check(); assert.equal(fa.healed, 2, 'a zoom animation'); assert.ok(fa.busySince > 0);
  fa.busySince = performance.now() - I.BUSY_STUCK_MS - 1; fa._check(); assert.equal(fa.healed, 3, 'busy for 10 s: healed anyway'); assert.equal(fa.busySince, 0); assert.equal(fa.suspended, 0);
  fa.suspend(); fa._check(); assert.equal(fa.healed, 3, 'still busy: the next 10 s run from now'); map._animatingZoom = false; fa.healed = 2;   // still suspended: the original sequence goes on
  map.dragging = { moving: () => true }; fa._check(); assert.equal(fa.healed, 2, 'a drag'); map.dragging = null;
  map.touchZoom = { _zooming: true }; fa._check(); assert.equal(fa.healed, 2, 'a pinch'); map.touchZoom = null;
  map._panAnim = { _inProgress: true }; fa._check(); assert.equal(fa.healed, 2, 'a pan animation'); map._panAnim = null;
  global.document.hidden = true; fa._check(); assert.equal(fa.healed, 2, 'hidden'); global.document.hidden = false;
  fa._check(); assert.equal(fa.healed, 3, 'then healed'); assert.equal(fa.suspended, 0);
  // no data: nothing to run
  fa.clearData(); fa.suspended = 1; fa._check(); assert.equal(fa.healed, 3);
  fa.detach(); assert.equal(fa.watch, null, 'the timer is cleared on detach');
});

test('G29 re-check R-4: every map event restarts the stuck clock: separate gestures, a gesture after a hidden period and a long moving gesture never heal; a busy flag with no event for 10 s still does', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(180));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa, map, events } = animator('hs', l, pdir, HALF);
  const fire = (ev) => (events[ev] || []).slice().forEach((f) => f());
  const longAgo = () => performance.now() - I.BUSY_STUCK_MS - 1;
  fa.pending(16);
  // separate drags, each spanning a check: the old rule added them up to "busy for 10 s"
  map.dragging = { moving: () => true };
  fire('movestart'); fa._check(); assert.ok(fa.busySince > 0);
  fa.busySince = longAgo();                                                    // 10 s of checks that all saw a drag ...
  fire('moveend'); fire('movestart'); fa._check();                             // ... but each was its own gesture
  assert.equal(fa.healed, 0, 'a new gesture restarts the clock: no heal mid-drag');
  // a check that returned early (hidden) left an old clock; the next drag starts afresh
  fa.busySince = longAgo(); global.document.hidden = true; fa._check(); global.document.hidden = false;
  fire('moveend'); fire('movestart'); fa._check(); assert.equal(fa.healed, 0, 'no heal at the first check of a new drag');
  // one long drag that keeps moving fires 'move' all along; a pinch fires 'zoom'
  fa.busySince = longAgo(); fire('move'); fa._check(); assert.equal(fa.healed, 0, 'a moving drag is alive');
  map.dragging = null; map.touchZoom = { _zooming: true };
  fa.busySince = longAgo(); fire('zoom'); fa._check(); assert.equal(fa.healed, 0, 'a moving pinch is alive');
  map.touchZoom = null; fire('moveend'); assert.equal(fa.suspended, 0);
  // a flag stuck true with no event for 10 s: healed, as before (G29 A-P3-3)
  fire('zoomstart'); map._animatingZoom = true; fa._check(); assert.equal(fa.healed, 0, 'a zoom in progress');
  fa.busySince = longAgo(); fa._check(); assert.equal(fa.healed, 1, 'busy for 10 s with no map event: healed');
  assert.equal(fa.suspended, 0); assert.equal(fa.canvas.style.visibility, '', 'the canvas the zoom hid is shown');
  map._animatingZoom = false; fa.detach();
});

test('section 42: after a zoom the canvas is a fresh element on WebKit (the old one released), the same element elsewhere; the map events are logged for the readout', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(180));
  const saveNav = Object.getOwnPropertyDescriptor(global, 'navigator');
  const setNav = (v) => Object.defineProperty(global, 'navigator', { value: v, configurable: true, writable: true });
  try {
    for (const [ua, fresh, touch] of [['Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1', true],
      ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36 Edg/155.0.0.0', false],
      ['Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/120.0 Mobile/15E148 Safari/604.1', true],   // Chrome on iOS is WebKit too (K-11)
      ['Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/120.0 Mobile/15E148 Safari/605.1.15', true],
      ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15', true, 5],            // iPadOS asking for desktop pages
      ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15', true, 0],            // Safari on a Mac
      ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36', true, 5],              // Chrome for iOS as a Mac
      ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36', false, 0],             // Chrome on a Mac
      ['Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Mobile Safari/537.36', false, 5],
      ['Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0', false]]) {
      setNav({ userAgent: ua, maxTouchPoints: touch });
      assert.equal(I.isWebKit(), fresh, ua.slice(0, 60) + ' touch ' + touch);
      const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
      const { fa, events, canvas } = animator('hs', l, pdir, HALF);
      const made = []; const pane = { style: {}, appendChild(c) { made.push(c); } };
      // the harness's document hands out one object; give the pane and a fresh canvas per creation here
      global.document.createElement = () => ({ style: {}, width: 0, height: 0, getContext: () => fakeCtx(), remove() { this.removed = true; } });
      fa.canvas.parentNode = pane;
      const before = fa.canvas;
      events.zoomstart.forEach((h) => h()); events.zoomend.forEach((h) => h());
      if (fresh) { assert.notEqual(fa.canvas, before, 'a fresh canvas'); assert.equal(before.width, 0); assert.equal(before.removed, true); assert.equal(made.length, 1); assert.ok(fa.canvas.width > 0, 'placed by the rebuild'); }
      else { assert.equal(fa.canvas, before, 'the same canvas'); assert.equal(made.length, 0); }
      assert.deepEqual(fa.log.map((e) => e[0]), ['zoomstart', 'zoomend']);
      assert.equal(fa.suspended, 0); assert.equal(fa.active, true);
      const st = fa.debugState();
      assert.equal(typeof st.log, 'string'); assert.match(st.log, /zoomstart@\d+ zoomend@\d+/); assert.equal(st.rate, 1); assert.equal(st.dir, true); assert.equal(st.healed, 0);
      void canvas;
    }
  } finally { if (saveNav) Object.defineProperty(global, 'navigator', saveNav); else delete global.navigator; }
});


test('G29 B-P2-1: a zoom whose zoomend never comes leaves the canvas hidden; the heal shows it again (on WebKit: a fresh canvas)', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(180));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa, events, canvas } = animator('hs', l, pdir, HALF);
  fa.pending(16);
  events.zoomstart.forEach((h) => h()); events.movestart.forEach((h) => h());
  assert.equal(canvas.style.visibility, 'hidden'); assert.equal(fa.suspended, 2); assert.equal(fa.active, false);
  fa._check();
  assert.equal(fa.healed, 1); assert.equal(fa.suspended, 0); assert.equal(fa.active, true);
  assert.equal(canvas.style.visibility, '', 'the heal shows the canvas again'); assert.equal(fa.canvas, canvas, 'the same canvas off WebKit');
  // a flyTo-like move: suspended with none of the busy flags, healed after the watchdog's period (A-P3-3 a, accepted)
  assert.equal(fa._busy(), false);
  // a zoomend after the pane is gone (map.remove() during a zoom): no exception into Leaflet's dispatch (A-P3-4)
  const saveNav = Object.getOwnPropertyDescriptor(global, 'navigator');
  Object.defineProperty(global, 'navigator', { value: { userAgent: 'Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1' }, configurable: true, writable: true });
  try { canvas.parentNode = null; events.zoomstart.forEach((h) => h()); events.zoomend.forEach((h) => h()); assert.equal(fa.canvas, canvas); }
  finally { if (saveNav) Object.defineProperty(global, 'navigator', saveNav); else delete global.navigator; }
});

test('G29 B-P2-2: a refused 2D context never throws into the map events or the playback: the old canvas is kept; without any context the animator draws nothing and the watchdog asks again until it gets one', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(180));
  const saveNav = Object.getOwnPropertyDescriptor(global, 'navigator');
  Object.defineProperty(global, 'navigator', { value: { userAgent: 'Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1' }, configurable: true, writable: true });
  try {
    const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
    const { fa, events, canvas, ctx } = animator('hs', l, pdir, HALF);
    const pane = { style: {}, appendChild(c) { pane.added = (pane.added || 0) + 1; c.parentNode = pane; } }; canvas.parentNode = pane;
    let refuse = true; const made = [];
    global.document.createElement = () => { const c = { style: {}, width: 0, height: 0, getContext: () => (refuse ? null : fakeCtx()), remove() { c.removed = true; } }; made.push(c); return c; };
    fa.pending(16);
    events.zoomstart.forEach((h) => h()); events.zoomend.forEach((h) => h());                  // WebKit: a fresh canvas is tried
    assert.equal(made.length, 1); assert.equal(fa.canvas, canvas, 'refused: the old canvas stays'); assert.equal(fa.ctx, ctx); assert.equal(canvas.removed, undefined);
    assert.equal(fa.active, true); assert.equal(fa.suspended, 0); assert.equal(fa.debugState().ctx, true);
    fa.setData(frame(720, 361, () => code(90)), HALF, { step: 1 });                               // the playback's path: no throw
    fa.detach();
    // no context from the start (attach), on a NON-WebKit browser too: nothing drawn, no exception anywhere, the watchdog
    // retries the canvas and heals once a context comes
    Object.defineProperty(global, 'navigator', { value: { userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36' }, configurable: true, writable: true });
    assert.equal(I.isWebKit(), false);
    const l2 = layer(hs, GRID, 'hs', HS); l2._tileZoom = 6;
    const a2 = animator('hs', l2, pdir, HALF);                                                  // its canvas is the harness's (with a context) ...
    global.document.createElement = () => { const c = { style: {}, width: 0, height: 0, getContext: () => (refuse ? null : fakeCtx()), remove() { c.removed = true; } }; made.push(c); return c; };   // (animator() re-made the document)
    const bare = { style: {}, width: 0, height: 0, getContext: () => null, remove() {} };
    a2.fa.stop(); a2.fa.canvas = bare; a2.fa.ctx = null; bare.parentNode = pane;
    a2.fa._rebuild();
    assert.equal(a2.fa.active, false, 'no context: the loop does not run'); assert.equal(a2.fa.debugState().ctx, false);
    a2.fa._renderParticles(16); a2.fa._clear();                                                   // nothing throws
    a2.fa._check(); assert.equal(a2.fa.healed, 1, 'dead: healed'); assert.equal(a2.fa.ctx, null, 'still refused'); assert.equal(a2.fa.active, false);
    refuse = false;
    a2.fa._check(); assert.equal(a2.fa.healed, 2); assert.ok(a2.fa.ctx, 'the retry got a context'); assert.equal(a2.fa.active, true, 'and the loop runs');
    a2.fa.detach();
  } finally { if (saveNav) Object.defineProperty(global, 'navigator', saveNav); else delete global.navigator; }
});

test('G29 B-P3-1 (owner: the same length at every speed): a tail never spans more than TRAIL_SPAN_MS of flow, so a slow particle loop at the fast rates draws tails of the same length as a fast one', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(270));   // from the west: particles run east
  assert.equal(I.TRAIL_SPAN_MS, (I.TRAIL_POINTS + 1) * I.TRAIL_EVERY_MS);
  const tailLength = (dt, rate) => {
    const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
    const { fa } = animator('hs', l, pdir, HALF);
    fa.setRate(rate);
    const P = fa.particles; fa.count = 1; P[0] = 100; P[1] = 300; P[2] = 0; P[3] = 1e9;
    fa.histN[0] = 0; fa.histT[0] = 0; fa.flowT[0] = 0;
    let pts = [], paths = 0; const c = fa.ctx;
    c.beginPath = () => { paths++; if (paths % 2 === 0) pts = []; }; c.moveTo = (x, y) => { if (paths % 2 === 0) pts.push([x, y]); }; c.lineTo = (x, y) => { if (paths % 2 === 0) pts.push([x, y]); };
    for (let k = 0; k < Math.round(1500 / dt); k++) fa._renderParticles(dt);                 // 1.5 s of real time: the tail is full at every rate
    let len = 0; for (let i = 1; i < pts.length; i++) len += Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]);
    return { len, points: pts.length };
  };
  const ref = tailLength(1000 / 60, 1);
  assert.ok(ref.points >= 9 && ref.len > 5, JSON.stringify(ref));
  for (const hz of [60, 30, 15]) for (const rate of [1, 2, 4]) {
    const t = tailLength(1000 / hz, rate), ratio = t.len / ref.len;
    assert.ok(ratio > 0.7 && ratio < 1.2, hz + ' Hz at x' + rate + ': ' + ratio.toFixed(2) + ' of the 60 Hz x1 tail (' + t.points + ' points)');
  }
  // the old rule: at 30 Hz and x4 every frame recorded a point and ten points spanned twice the flow
  const slow = tailLength(1000 / 30, 4);
  assert.ok(slow.points < I.TRAIL_POINTS + 1, 'old points left out: ' + slow.points);
  // every tail point too old (the flow clock far ahead): the path starts at the head with a moveTo, never a lineTo from another particle
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa } = animator('hs', l, pdir, HALF);
  fa.count = 2; const P = fa.particles; P[0] = 100; P[1] = 300; P[2] = 0; P[3] = 1e9; P[4] = 400; P[5] = 300; P[6] = 0; P[7] = 1e9;
  for (let k = 0; k < 20; k++) fa._renderParticles(16);
  fa.flowT[1] += 1e6; fa.histT[1] = 0;                                     // no new point this frame: nothing of the tail is young enough
  const ops = []; let paths = 0; const c = fa.ctx;
  c.beginPath = () => { paths++; }; c.moveTo = () => { if (paths === 2) ops.push('moveTo'); }; c.lineTo = () => { if (paths === 2) ops.push('lineTo'); };
  fa._renderParticles(16);
  const second = ops.lastIndexOf('moveTo');
  assert.ok(second > 0 && ops.slice(second).length === 1, "the second particle's tail is its head alone, started with a moveTo: " + ops.join(","));
});

test('section 42 C2: a pan beyond the node window recomputes the nodes (the lattice keeps its vectors), a small pan does not', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(270));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa, map } = animator('hs', l, pdir, HALF);
  const win0 = fa.nodes.win; assert.ok(win0 && win0.count < 720);
  const moving = (fa.vf.u.filter((x) => x !== 0).length);
  assert.ok(moving > 0.9 * fa.vf.u.length, 'a uniform field: nearly every lattice node moves');
  map.getPixelBounds = () => ({ min: { x: 10000 + 300, y: 9000 } }); fa._rebuild();
  assert.equal(fa.nodes.win, win0, 'a small pan: the same nodes');
  map.getPixelBounds = () => ({ min: { x: 10000 + 3 * 800, y: 9000 } }); fa._rebuild();
  assert.notEqual(fa.nodes.win, win0, 'three screens east: recomputed'); assert.notEqual(fa.nodes.win.c0, win0.c0);
  assert.ok(fa.vf.u.filter((x) => x !== 0).length > 0.9 * fa.vf.u.length, 'the lattice still has its vectors (stale nodes would be absent)');
});

test('section 46: the particles slider fades the whole pane by CSS opacity (the strokes\' colours untouched); full strength leaves no inline opacity; a pane made later takes the strength', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(90));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa } = animator('hs', l, pdir, HALF);
  let fb = null;
  try {
  assert.equal(fa.pane.style.opacity, '', 'full strength: the pane as before');
  fa.setOpacity(0.4);
  assert.equal(fa.pane.style.opacity, '0.4');
  fa.setOpacity(1); assert.equal(fa.pane.style.opacity, '', 'back to full: no inline opacity');
  for (const [v, want] of [[-3, '0'], [9, ''], [NaN, ''], ['0.3', ''], [0.25, '0.25']]) { fa.setOpacity(v); assert.equal(fa.pane.style.opacity, want, String(v)); }
  // an animator whose pane is made after the strength is set (attach after setOpacity)
  const map = { on() {}, off() {}, getPane: () => null, createPane: () => ({ style: {}, appendChild() {} }) };
  fb = new I.FlowAnimator(map, null); fb.setOpacity(0.5); assert.equal(fb.pane, null);
  global.document = { createElement: () => ({ style: {}, getContext: () => fakeCtx(), remove() {} }), hidden: false };
  fb.attach(); assert.equal(fb.pane.style.opacity, '0.5', 'applied at attach');
  assert.equal(I.particleOpacityOf(0.4), 0.4); assert.equal(I.particleOpacityOf(-1), 0); assert.equal(I.particleOpacityOf(2), 1);
  assert.equal(I.particleOpacityOf(undefined), 1); assert.equal(I.particleOpacityOf('0'), 1); assert.equal(I.particleOpacityOf(Infinity), 1);
  } finally { fa.detach(); if (fb) fb.detach(); }        // a failed assertion leaves no live animator behind
});

// ---- plan section 100: no dense blocks after a pan or zoom ----
// Wind with data only west of 44 E: at the test view (x 10000-10800 at zoom 6: 39.7-57.3 E) about the left quarter flows.
const QUARTER = () => frame(720, 361, (r, c) => (c < 448 ? codeOf(10, WIND) : 0));
const liveOf = (fa) => { let n = 0; for (let i = 0; i < fa.count; i++) if (fa.particles[i * 4 + 2] >= 0) n++; return n; };
const flowShare = (fa) => { let n = 0, m = 0; for (let y = 2; y < fa.v.h; y += 8) for (let x = 2; x < fa.v.w; x += 8) { m++; if (fa._flowing(x, y)) n++; } return n / m; };

test('section 100 D1: a particle born where nothing flows is dormant for a life (never crowds into the flowing part): the live share is the flowing share, dormant particles neither move nor draw', () => {
  const l = layer(QUARTER(), HALF, 'wind', WIND); l._tileZoom = 6;
  const { fa, ctx } = animator('wind', l, frame(720, 361, () => code(180)), HALF);
  const f = flowShare(fa); assert.ok(f > 0.18 && f < 0.3, 'about a quarter flows: ' + f);
  const P = fa.particles, n = fa.count;
  for (let i = 0; i < n; i++) assert.equal(P[i * 4 + 2] >= 0, fa._flowing(P[i * 4], P[i * 4 + 1]), 'the seed: live exactly where the flow runs');
  assert.ok(Math.abs(liveOf(fa) / n - f) < 0.1, 'seeded live share ' + liveOf(fa) / n + ' ~ ' + f);
  for (let t = 0; t < 300; t++) fa._renderParticles(33);                                 // ~10 s: several wind lives
  const share = liveOf(fa) / n;
  assert.ok(share > f - 0.12 && share < f + 0.1, 'after 10 s the live share ' + share + ' is still the flowing share ' + f + ' (production: every particle)');
  for (let i = 0; i < n; i++) if (P[i * 4 + 2] >= 0) assert.ok(P[i * 4] < 230, 'live particles only where it flows');
  // a dormant particle waits where it is and is not drawn; it counts its wait up in real time
  const k = (() => { for (let i = 0; i < n; i++) if (P[i * 4 + 2] < -500) return i * 4; return -1; })(); assert.ok(k >= 0);
  const x = P[k], y = P[k + 1], age = P[k + 2];
  fa.count = 1; P.copyWithin(0, k, k + 4); fa.histN[0] = 0; ctx.ops.length = 0;
  fa.rate = 4; fa._renderParticles(40);
  assert.equal(P[0], x); assert.equal(P[1], y); assert.equal(P[2], Math.fround(age + 40), 'the wait runs in real time (rate 4 changes nothing)');
  assert.equal(ctx.ops.filter((o) => o === 'moveTo' || o === 'lineTo').length, 0, 'nothing drawn for a dormant particle');
  P[2] = -10; fa._renderParticles(16); assert.equal(fa.histN[0], 0); assert.ok(P[2] === 0 || P[2] < -900, 'its wait over: born again (live, or dormant for a new life)');
});

test('section 100 D1: a dormant seed waits a random part of its life (no wave of returns); a live one starts at a random age', () => {
  const l = layer(QUARTER(), HALF, 'wind', WIND); l._tileZoom = 6;
  const { fa } = animator('wind', l, frame(720, 361, () => code(180)), HALF);
  const P = fa.particles, waits = [], ages = [];
  for (let i = 0; i < I.PARTICLE_MAX; i++) { if (P[i * 4 + 2] < 0) waits.push(-P[i * 4 + 2] / P[i * 4 + 3]); else ages.push(P[i * 4 + 2]); }
  assert.ok(waits.length > 1500 && ages.length > 300);
  const mean = waits.reduce((a, x) => a + x, 0) / waits.length;
  assert.ok(waits.every((w) => w >= 0 && w <= 1) && Math.abs(mean - 0.5) < 0.05, 'waits spread over the life: mean ' + mean);
  assert.ok(Math.max(...ages) <= I.PARTICLE_LIFE_MS.wind[0] && Math.max(...ages) > 0.8 * I.PARTICLE_LIFE_MS.wind[0]);
});

test('section 100 D1b: a rebuild wakes dormant particles only into area the old lattice did not flow on (a step that gains nothing wakes nothing; newly drawn water fills at once, evenly)', () => {
  const l = layer(QUARTER(), HALF, 'wind', WIND); l._tileZoom = 6;
  const wdir = frame(720, 361, () => code(180)), { fa } = animator('wind', l, wdir, HALF);
  for (let t = 0; t < 60; t++) fa._renderParticles(33);
  const live0 = liveOf(fa), pos0 = Array.from(fa.particles);
  fa._rebuild();                                                                         // a playback step over the same data
  assert.equal(liveOf(fa), live0, 'nothing gained: nobody woken');
  assert.deepEqual(Array.from(fa.particles), pos0, 'and nothing moved');
  // half the view flows now (a quarter gained of three quarters dead): a third of the dormant wake, not all of them
  l.setFrame(frame(720, 361, (r, c) => (c < 457 ? codeOf(10, WIND) : 0)), HALF, 'wind', WIND, I.buildLut('wind', WIND.legend), { step: 1, valid_utc: '2026-09-22T13:00:00Z' });
  fa._rebuild();
  const f2 = flowShare(fa); assert.ok(Math.abs(liveOf(fa) / fa.count - f2) < 0.08, 'live ' + liveOf(fa) / fa.count + ' ~ the flowing share ' + f2);
  // the rest of the view starts to flow (the new tiles drawn): the dormant fill it at the one density, at once
  l.setFrame(frame(720, 361, () => codeOf(10, WIND)), HALF, 'wind', WIND, I.buildLut('wind', WIND.legend), { step: 2, valid_utc: '2026-09-22T14:00:00Z' });
  fa._rebuild();
  const n = fa.count, live = liveOf(fa);
  assert.ok(live > 0.88 * n, 'filled at once: ' + live + ' of ' + n);
  let right = 0; for (let i = 0; i < n; i++) if (fa.particles[i * 4 + 2] >= 0 && fa.particles[i * 4] >= 230) right++;
  assert.ok(Math.abs(right / live - 0.72) < 0.08, 'spread over the gained area by its share: ' + right / live);
});

test('section 100 D3: a pan moves the particles and their tails with the map (no jump at moveend); those that left wake in the exposed strip only; a zoom scatters afresh', () => {
  const l = layer(frame(720, 361, () => codeOf(10, WIND)), HALF, 'wind', WIND); l._tileZoom = 6;
  const { fa, map } = animator('wind', l, frame(720, 361, () => code(180)), HALF);
  for (let t = 0; t < 20; t++) fa._renderParticles(33);
  const P = fa.particles, n = fa.count, before = Array.from(P), H0 = Array.from(fa.hist), N0 = Array.from(fa.histN);
  assert.equal(liveOf(fa), n, 'all live over a full field');
  map.getPixelBounds = () => ({ min: { x: 10000 + 100, y: 9000 + 60 } }); fa._rebuild();      // a pan 100 px east, 60 px south
  let kept = 0, left = 0, woken = 0;
  for (let i = 0; i < n; i++) {
    const k = i * 4, ox = before[k], oy = before[k + 1];
    if (ox >= 100 && oy >= 60) {
      kept++; assert.equal(P[k], Math.fround(ox - 100)); assert.equal(P[k + 1], Math.fround(oy - 60)); assert.equal(P[k + 2], before[k + 2], 'its life goes on');
      if (N0[i] > 0) { const q = i * I.TRAIL_POINTS * 2; assert.equal(fa.hist[q], Math.fround(H0[q] - 100)); assert.equal(fa.hist[q + 1], Math.fround(H0[q + 1] - 60)); assert.equal(fa.histN[i], N0[i], 'the tail kept'); }
    } else {
      left++;
      if (P[k + 2] >= 0) { woken++; assert.ok(P[k] >= 692 || P[k + 1] >= 532, 'woken in the exposed strip (to one 8-px wake cell): ' + P[k] + ',' + P[k + 1]); assert.equal(fa.histN[i], 0); }
    }
  }
  const strip = (800 * 600 - 700 * 540) / (800 * 600);
  // left / n is a binomial share (n = 533 particles, p = 0.2125): sigma ~ 0.018, so 0.05 (2.8 sigma) failed ~1 run in 120;
  // 0.075 is 4.2 sigma and still fails if none or all of the particles move out.
  assert.ok(kept > 0 && Math.abs(left / n - strip) < 0.075, 'the share that left is the exposed share: ' + (left / n).toFixed(3) + ' ~ ' + strip.toFixed(3));
  assert.ok(woken > 0.85 * left, 'most woken at once: ' + woken + ' of ' + left);
  // a small pan (review P2): the 20-px strip gets the one density at once
  let inStrip = 0, ox = 10100;
  for (let r = 0; r < 20; r++) {
    for (let t = 0; t < 5; t++) fa._renderParticles(33);
    ox += 20; const at = ox; map.getPixelBounds = () => ({ min: { x: at, y: 9060 } }); fa._rebuild();
    for (let i = 0; i < n; i++) if (P[i * 4 + 2] === 0 && P[i * 4] >= 780 - 8) inStrip++;
  }
  const want = 20 * n * 20 / 800;
  assert.ok(inStrip > 0.75 * want && inStrip < 1.3 * want, 'twenty 20-px strips hold ' + inStrip + ' (the one density: ' + want.toFixed(0) + ')');
  assert.ok(liveOf(fa) > 0.97 * n, 'still all live over a full field');
  // a fresh canvas (a WebKit zoom or a watchdog heal) at the same zoom keeps the particles where they are (review P3-4)
  const keep = Array.from(P.slice(0, 40)); fa._newCanvas({ appendChild() {} }); assert.equal(fa.v, null); fa._rebuild();
  for (let i = 0; i < 10; i++) if (keep[i * 4 + 2] >= 0) { assert.equal(P[i * 4], keep[i * 4]); assert.equal(P[i * 4 + 1], keep[i * 4 + 1]); }
  // a zoom: positions do not map; every particle scattered afresh, no tails
  for (let t = 0; t < 20; t++) fa._renderParticles(33);
  const before2 = Array.from(P); for (let i = 0; i < before.length; i++) before[i] = before2[i];
  map.getZoom = () => 6.5; fa._rebuild();                                         // the same pixel origin: a shift would be 0
  assert.equal(fa.histN.slice(0, n).every((m) => m === 0), true, 'no tail survives a zoom');
  assert.ok(fa.v.z === 6.5);
  let same = 0; for (let i = 0; i < n; i++) if (P[i * 4] === before[i * 4]) same++;
  assert.ok(same < 3, 'scattered');
});

test('section 100 D2: a redraw of the layer while the map moves does not rebuild (the moveend resume does); the readout counts the live particles', () => {
  const l = layer(QUARTER(), HALF, 'wind', WIND); l._tileZoom = 6;
  const { fa } = animator('wind', l, frame(720, 361, () => code(180)), HALF);
  let rebuilds = 0; const rb = fa._rebuild; fa._rebuild = function () { rebuilds++; return rb.apply(this, arguments); };
  fa.suspend(); l.onRedraw(); assert.equal(rebuilds, 0);
  fa.resume(true); assert.equal(rebuilds, 1);
  l.onRedraw(); assert.equal(rebuilds, 2);
  const d = fa.debugState(); assert.equal(d.live, liveOf(fa)); assert.ok(d.live < d.count);
  const rm = global.matchMedia; global.matchMedia = () => ({ matches: true });
  try { fa._rebuild(); assert.equal(fa.particles, null, 'reduced motion: the particles are dropped'); } finally { if (rm) global.matchMedia = rm; else delete global.matchMedia; }
  fa._rebuild(); assert.ok(fa.particles && liveOf(fa) > 0, 'reseeded when motion is back');
  fa.detach(); assert.equal(fa._vfSpare, null); assert.equal(fa._gained, null);
});
