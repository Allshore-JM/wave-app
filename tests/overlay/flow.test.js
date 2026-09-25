'use strict';
// The animation of plan section 21 phase C (static_overlay/overlay.js, no browser): the direction
// sampler, the arrow lattice, the wind field and the FlowAnimator's scheduling, with a recording canvas.
// node --test tests/overlay/
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

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
const angDiff = (a, b) => Math.abs(((a - b) % 360 + 540) % 360 - 180);

test('dirAt: 350 and 10 degrees average to north, across a regime edge the nearest node rules (no blend), one present node is its direction', () => {
  const f = frame(1440, 721, (r, c) => (c % 2 ? code(10) : code(350)));
  // exactly between two columns on a node row: 350 / 10 -> 0 (within the 0.71-degree coding error)
  const lat = 90 - 300 * 0.25, lonMid = -180 + 100.5 * 0.25;
  assert.ok(angDiff(I.dirAt(f, GRID, lat, lonMid), 0) < 0.75, String(I.dirAt(f, GRID, lat, lonMid)));
  assert.ok(angDiff(I.dirAt(f, GRID, lat - 0.125, lonMid), 0) < 0.75, 'four nodes 350/10/350/10');
  assert.ok(angDiff(I.dirAt(f, GRID, lat, -180 + 100 * 0.25), 350) < 0.75, 'on a node: that node');
  const opp = frame(1440, 721, (r, c) => (c % 2 ? code(180) : code(0)));
  assert.ok(angDiff(I.dirAt(opp, GRID, lat, lonMid - 0.01), 0) < 0.75 && angDiff(I.dirAt(opp, GRID, lat, lonMid + 0.01), 180) < 0.75, 'opposite neighbours: the nearest node, no blend');
  const edge = frame(1440, 721, (r, c) => (c % 2 ? code(300) : code(80)));                   // a NW swell beside an E wind sea (220 deg apart... 140 the short way)
  assert.ok(angDiff(I.dirAt(edge, GRID, lat, lonMid - 0.02), 80) < 0.75 && angDiff(I.dirAt(edge, GRID, lat, lonMid + 0.02), 300) < 0.75, 'regime edge: nearest');
  const near = frame(1440, 721, (r, c) => (c % 2 ? code(100) : code(50)));                   // 50 deg apart: one regime, blended
  assert.ok(angDiff(I.dirAt(near, GRID, lat, lonMid), 75) < 1.5, 'within 60 deg: the circular mean');
  const four = frame(1440, 721, (r, c) => ((r + c) % 4 === 0 ? code(300) : code(85)));        // reviewer B's 2x2: three ENE nodes and one NW
  assert.ok([85, 300].some((d) => angDiff(I.dirAt(four, GRID, lat - 0.125, lonMid), d) < 0.75), 'a 2x2 across regimes shows one of its nodes');
  const one = frame(1440, 721, (r, c) => (c === 100 ? code(270) : 0));
  assert.ok(angDiff(I.dirAt(one, GRID, lat, lonMid), 270) < 0.75, 'one present node beside an absent one (weight 0.5)');
  assert.equal(I.dirAt(one, GRID, lat, -180 + 100.8 * 0.25), null, 'mostly absent (weight 0.2): nothing');
  assert.equal(I.dirAt(one, GRID, 91, lonMid), null, 'outside the rows');
  assert.equal(I.dirAt(frame(1440, 721, () => 0), GRID, lat, lonMid), null, 'no data');
  const wrap = frame(1440, 721, (r, c) => (c === 1439 ? code(90) : c === 0 ? code(90) : 0));
  assert.ok(angDiff(I.dirAt(wrap, GRID, lat, 179.999), 90) < 0.75, 'the last and first columns are neighbours across the dateline');
});

test('sampleDirRow agrees with dirAt and yields unit TOWARD vectors; sampleRow is the layer sampler', () => {
  const f = frame(720, 361, (r, c) => code((c * 37) % 360));
  const colPos = new Float64Array([10.3, 400.75, 719.5]), e = new Float64Array(3), n = new Float64Array(3);
  I.sampleDirRow(f, 100.4, colPos, 3, e, n, 0);
  for (let i = 0; i < 3; i++) {
    const lat = 90 - 100.4 * 0.5, lon = -180 + colPos[i] * 0.5, from = I.dirAt(f, HALF, lat, lon);
    assert.ok(Math.abs(Math.hypot(e[i], n[i]) - 1) < 1e-9, 'unit vector');
    const toward = (from + 180) % 360;                                     // east = sin(toward), north = cos(toward)
    assert.ok(Math.abs(e[i] - Math.sin(toward * Math.PI / 180)) < 1e-9 && Math.abs(n[i] - Math.cos(toward * Math.PI / 180)) < 1e-9);
  }
  const hs = frame(720, 361, (r, c) => 1 + ((r * 7 + c * 3) % 254)), l = layer(hs, HALF, 'hs', HS), out = new Float64Array(3), viaLayer = new Float64Array(3);
  I.sampleRow(hs, false, 100.4, colPos, 3, out, 0); l._codeRow(100.4, colPos, 3, viaLayer, 0);
  assert.deepEqual(Array.from(out), Array.from(viaLayer));
});

test('screenVec: from the south moves up the screen, from the west moves right', () => {
  const near = (v, x, y) => Math.abs(v[0] - x) < 1e-12 && Math.abs(v[1] - y) < 1e-12;
  assert.ok(near(I.screenVec(180), 0, -1)); assert.ok(near(I.screenVec(270), 1, 0));
  assert.ok(near(I.screenVec(0), 0, 1)); assert.ok(near(I.screenVec(90), -1, 0));
});

test('dirFieldOk and dirRes: only circular FROM fields; wind direction half only; pdir half below 7, full from 7.5 with hysteresis', () => {
  assert.equal(I.dirFieldOk(PDIR), true); assert.equal(I.dirFieldOk(WDIR), true);
  assert.equal(I.dirFieldOk(HS), false); assert.equal(I.dirFieldOk(Object.assign({}, PDIR, { convention: 'to' })), false);
  assert.equal(I.dirFieldOk(Object.assign({}, PDIR, { resolutions: [] })), false); assert.equal(I.dirFieldOk(undefined), false);
  for (const z of [3, 7, 9, 11]) assert.equal(I.dirRes(WDIR, z, null), 'half');
  assert.equal(I.dirRes(PDIR, 6.9, null), 'half'); assert.equal(I.dirRes(PDIR, 7.5, null), 'full'); assert.equal(I.dirRes(PDIR, 7.2, null), 'half');
  assert.equal(I.dirRes(PDIR, 7.2, 'full'), 'full'); assert.equal(I.dirRes(PDIR, 6.9, 'full'), 'half'); assert.equal(I.dirRes(PDIR, 7.2, 'half'), 'half');
  assert.equal(I.DIR_FIELDS.hs, 'pdir'); assert.equal(I.DIR_FIELDS.tp, 'pdir'); assert.equal(I.DIR_FIELDS.wind, 'wdir');
});

test('arrowAnchors: tile-pixel centres every 64 px, the same world anchors after a pan and on a world copy, unwrapped longitude for the tile lookup', () => {
  const zt = 6, nt = 256 * 64, view = { z: 6.3, w: 800, h: 600, ox: 10000.3, oy: 6000.7 };
  const A = I.arrowAnchors(view, zt);
  assert.ok(A.length > 100);
  const key = (a) => a.i + ':' + a.j;
  for (const a of A) {                                                     // every anchor is the centre of tile pixel 32 + 64k, both ways
    const p = I.pixelOf(a.lat, a.lng, zt);
    assert.equal(p.px % 64, 32, 'px ' + p.px); assert.equal(p.py % 64, 32, 'py ' + p.py);
    assert.equal(p.x, Math.floor((a.i * 64 + 32.5) / 256), 'the unwrapped tile x Leaflet keys the tile by');
    assert.ok(a.sx > -200 && a.sx < view.w + 200 && a.sy > -200 && a.sy < view.h + 200);
  }
  const B = I.arrowAnchors({ z: 6.3, w: 800, h: 600, ox: 10000.3 + 137, oy: 6000.7 - 55 }, zt), mapB = new Map(B.map((b) => [key(b), b]));
  let shared = 0;
  for (const a of A) { const b = mapB.get(key(a)); if (!b) continue; shared++; assert.ok(Math.abs(b.sx - (a.sx - 137)) < 1e-6 && Math.abs(b.sy - (a.sy + 55)) < 1e-6); assert.equal(b.lat, a.lat); assert.equal(b.lng, a.lng); }
  assert.ok(shared > A.length / 2, 'most anchors are shared after a small pan');
  const scale = Math.pow(2, 6.3 - zt), C = I.arrowAnchors({ z: 6.3, w: 800, h: 600, ox: 10000.3 + nt * scale, oy: 6000.7 }, zt);
  const bottom = I.arrowAnchors({ z: 6.3, w: 800, h: 600, ox: 10000.3, oy: 20000.7 }, zt);                 // the world's bottom edge
  assert.ok(bottom.length > 0 && bottom.every((a) => a.lat > -85.1) && bottom.length < A.length, 'no anchors beyond the pole');
  assert.equal(C.length, A.length);
  for (let k = 0; k < A.length; k++) { assert.ok(Math.abs(C[k].lng - 360 - A[k].lng) < 1e-9 && Math.abs(C[k].sx - A[k].sx) < 1e-6, 'world copy anchors coincide, one world east'); }
  const D = I.arrowAnchors({ z: 6, w: 400, h: 300, ox: nt - 200, oy: 8000 }, zt);          // across the dateline
  assert.ok(D.some((a) => a.lng > 170 && a.lng < 180) && D.some((a) => a.lng >= 180), 'east of the dateline the longitude runs past 180 (Leaflet tile x >= 2^z)');
  assert.ok(D.filter((a) => a.sx >= 0 && a.sx < 400).length >= 4 * 4);
  // the samplers wrap: a direction frame read at lng 190 equals lng -170
  const pd = frame(720, 361, (r, c) => code((c * 37) % 360));
  assert.equal(I.dirAt(pd, HALF, 20, 190), I.dirAt(pd, HALF, 20, -170));
  // the readout gate finds the tile by its unwrapped coordinates (a tile west of the dateline has x < 0)
  const west = I.arrowAnchors({ z: 6, w: 300, h: 300, ox: -150, oy: 8000 }, zt).filter((a) => a.sx >= 0 && a.sx < 150);
  assert.ok(west.length > 0 && west.every((a) => a.lng < -180 && I.pixelOf(a.lat, a.lng, zt).x < 0));
});

test('windField: a 10 m/s wind from the west moves particles right at 30 px/s times the latitude stretch (capped at 3); a calm has no vector', () => {
  const wind = frame(720, 361, () => codeOf(10, WIND)), wdir = frame(720, 361, () => code(270));
  const l = layer(wind, HALF, 'wind', WIND);
  const view = { z: 5, w: 400, h: 4400, ox: 4096, oy: 0 };                  // a tall view from the pole down past the equator (n = 8192)
  const vf = I.windField(view, 4, l, wdir, HALF);
  assert.equal(vf.cols, 101); assert.equal(vf.rows, 1101);
  const eqRow = Math.round(4096 / 4), k = eqRow * vf.cols + 50;
  assert.ok(Math.abs(vf.u[k] - 30) < 0.5, 'equator: 3 px/s per m/s -> ' + vf.u[k]); assert.ok(Math.abs(vf.v[k]) < 0.5, 'north component within the 0.71-degree coding error');
  const lat60 = I.latOfWorldY(0, 8192);                                    // row 0 is the top of the Mercator square (85 N): sec capped at 3
  assert.ok(lat60 > 85); assert.ok(Math.abs(vf.u[50] - 90) < 1, 'cap: ' + vf.u[50]);
  const row45 = (() => { for (let j = 0; j < vf.rows; j++) if (I.latOfWorldY(j * 4, 8192) < 45) return j; })();
  assert.ok(Math.abs(vf.u[row45 * vf.cols + 50] - 30 * Math.SQRT2) < 1.5, '45 N: sec = sqrt 2');
  const calm = layer(frame(720, 361, () => codeOf(0, WIND)), HALF, 'wind', WIND), vfc = I.windField(view, 4, calm, wdir, HALF);
  assert.equal(vfc.u[k], 0, 'no velocity in a calm');
  const none = I.windField(view, 4, l, frame(720, 361, () => 0), HALF);
  assert.equal(none.u[k], 0, 'no direction: no vector');
});

// ---- the animator with a recording canvas ----
function fakeCtx() {
  const c = { ops: [], fills: [], globalCompositeOperation: 'source-over', fillStyle: '' };
  for (const m of ['clearRect', 'beginPath', 'moveTo', 'lineTo', 'stroke', 'fillRect', 'setTransform']) c[m] = function () { c.ops.push(m); if (m === 'fillRect') { const a = /,([0-9.]+)\)$/.exec(c.fillStyle); c.fills.push(a ? Number(a[1]) : 1); } };
  return c;
}
function animator(field, l, dframe, dgrid, zoom) {
  const ctx = fakeCtx(), events = {};
  const map = { on(ev, fn) { (events[ev] = events[ev] || []).push(fn); }, off(ev, fn) { events[ev] = (events[ev] || []).filter((f) => f !== fn); },
    getPane: () => null, createPane: () => ({ style: {}, appendChild() {} }), getSize: () => ({ x: 800, y: 600 }),
    getPixelBounds: () => ({ min: { x: 10000, y: 9000 } }), getZoom: () => zoom || 6, containerPointToLayerPoint: () => ({ x: -3, y: 7 }) };
  const canvas = { style: {}, width: 0, height: 0, getContext: () => ctx, remove() { canvas.removed = true; } };
  global.document = { createElement: () => canvas, hidden: false };
  const fa = new I.FlowAnimator(map, l);
  fa.scheduled = 0; fa.raf = (fn) => { fa.scheduled++; fa.pending = fn; return 1; }; fa.caf = () => { fa.pending = null; };
  fa.attach(); fa.setField(field); fa.setData(dframe, dgrid);
  return { fa, ctx, canvas, events, map };
}
const strokes = (ctx) => ctx.ops.filter((o) => o === 'stroke').length;

test('arrows: a wave-height frame with a direction draws tracks and gliding chevrons (four strokes a frame), then keeps scheduling', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(350));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa, ctx, canvas } = animator('hs', l, pdir, HALF);
  assert.equal(fa.mode, 'arrows'); assert.ok(fa.anchors.length > 100, 'anchors ' + fa.anchors.length);
  assert.equal(canvas.width, 800); assert.equal(canvas.style.transform, 'translate3d(-3px,7px,0)');
  for (const a of fa.anchors) { assert.ok(Math.abs(a.dx - I.screenVec(350)[0]) < 0.02 && Math.abs(a.dy - I.screenVec(350)[1]) < 0.02); assert.equal(a.rate, 1); }
  assert.equal(fa.scheduled, 1, 'one animation frame scheduled after the rebuild');
  const before = strokes(ctx); fa.pending(100); assert.equal(strokes(ctx) - before, 4); assert.equal(fa.scheduled, 2);
  const c0 = fa.clock; fa.pending(116); assert.ok(fa.clock > c0, 'the chevrons advance');
  fa.stop(); assert.equal(fa.pending, null); assert.equal(fa.active, false);
  const flat = layer(frame(1440, 721, () => codeOf(0.05, HS)), GRID, 'hs', HS); flat._tileZoom = 6;
  const q = animator('hs', flat, pdir, HALF); assert.equal(q.fa.anchors.length, 0, 'no arrow under a flat sea'); assert.equal(q.fa.scheduled, 1);
  const TPDEF = { lo: 1, hi: 30, legend: [4, 22], units: 's', interpolation: 'bilinear' };
  const tp = layer(frame(1440, 721, () => 1 + Math.round((16 - 1) / 29 * 254)), GRID, 'tp', TPDEF); tp._tileZoom = 6;
  const t = animator('tp', tp, pdir, HALF); assert.ok(t.fa.anchors.length > 100); assert.ok(Math.abs(t.fa.anchors[0].rate - 1.6) < 0.05, 'a 16 s swell glides at 1.6x');
  const floor = layer(frame(1440, 721, () => 1 + Math.round((1.6 - 1) / 29 * 254)), GRID, 'tp', TPDEF); floor._tileZoom = 6;
  assert.equal(animator('tp', floor, pdir, HALF).fa.anchors.length, 0, 'the model no-wave floor (Tp 1.6 s) gets no arrow on the period layer (G10-B P2-1)');
});

test('particles: the wind field seeds particles that move with the wind, fade the trails and adapt their count to the frame budget', () => {
  const wind = frame(720, 361, () => codeOf(10, WIND)), wdir = frame(720, 361, () => code(270));
  const l = layer(wind, HALF, 'wind', WIND); l._tileZoom = 6;
  const { fa, ctx } = animator('wind', l, wdir, HALF);
  assert.equal(fa.mode, 'particles'); assert.ok(fa.vf && fa.vf.s === 8 && fa.vf.cols === 101, 'a 22-px cell at zoom 6: an 8-px lattice'); assert.equal(fa.count, Math.round(800 * 600 / 900));
  const x0 = fa.particles[0], y0 = fa.particles[1];
  fa.pending(100); fa.pending(150);                                          // dt 16 then 50 ms
  assert.ok(ctx.ops.includes('fillRect'), 'the trails fade'); assert.equal(strokes(ctx), 4, 'halo + core per frame, two frames');
  assert.ok(fa.particles[2] >= 66 && fa.particles[3] >= I.PARTICLE_LIFE_MS[0], 'age and life in ms');
  assert.ok(fa.particles[0] > x0 && Math.abs(fa.particles[1] - y0) < 0.1, 'a particle moved east');
  fa._render(50); fa._render(50);
  fa.ema = 20; fa.adaptAt = 0; fa._adapt(20, 5000); assert.ok(fa.count < Math.round(800 * 600 / 900), 'over budget: fewer particles');
  fa.ema = 0.5; fa.adaptAt = 0; fa._adapt(0.5, 9000); assert.ok(fa.count > I.PARTICLE_MIN, 'room: back up');
  const c = ctx.ops.length; fa._render(16); assert.equal(ctx.ops.slice(c).filter((o) => o === 'clearRect').length, 0, 'no periodic hard clear (no blink): the fade is the only erase');
  assert.ok(Math.abs(Math.pow(I.TRAIL_KEEP, 33.4 / 16.7) - I.TRAIL_KEEP * I.TRAIL_KEEP) < 1e-12, 'the fade is frame-rate independent');
  const vf0 = fa.vf; fa._rebuild(); assert.equal(fa.vf.u, vf0.u, 'the wind field buffers are reused across rebuilds');
});

test('suspend / resume count nested moves and zooms; a hidden document stops the loop; static under reduced motion; detach leaves nothing', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(90));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa, events, canvas } = animator('hs', l, pdir, HALF);
  events.zoomstart[0](); events.movestart[0]();                              // a zoom fires both
  assert.equal(fa.suspended, 2); assert.equal(fa.active, false); assert.equal(canvas.style.visibility, 'hidden');
  events.zoomend[0](); assert.equal(fa.suspended, 1); assert.equal(fa.active, false, 'still moving');
  events.moveend[0](); assert.equal(fa.suspended, 0); assert.equal(fa.active, true); assert.equal(canvas.style.visibility, '');
  global.document.hidden = true; const sched = fa.scheduled; fa.pending(200); assert.equal(fa.active, false); assert.equal(fa.scheduled, sched, 'nothing scheduled while hidden'); assert.equal(fa.rafId, null);
  global.document.hidden = false; fa.resume(); assert.equal(fa.active, true);
  fa.clearData(); assert.equal(fa.active, false); assert.equal(fa.dir, null);
  fa.setData(pdir, HALF); assert.equal(fa.active, true);
  assert.equal(typeof l.onRedraw, 'function', 'the animator follows the layer');
  const a0 = fa.anchors; l.onRedraw(); assert.notEqual(fa.anchors, a0, 'a coast chunk landing (tiles redrawn) rebuilds the arrows'); assert.equal(fa.anchors.length, a0.length);
  fa.detach(); assert.equal(fa.active, false); assert.equal(fa.canvas, null); assert.equal(canvas.removed, true); assert.equal(Object.values(events).flat().length, 0, 'map listeners removed');
  assert.equal(l.onRedraw, null, 'detached from the layer');
  global.matchMedia = () => ({ matches: true });                              // prefers-reduced-motion
  const s = animator('hs', l, pdir, HALF);
  assert.equal(s.fa.mode, 'static'); assert.equal(s.fa.active, false); assert.equal(s.fa.scheduled, 0); assert.equal(strokes(s.ctx), 4, 'drawn once');
  const w = animator('wind', layer(frame(720, 361, () => codeOf(10, WIND)), HALF, 'wind', WIND), frame(720, 361, () => code(270)), HALF);
  assert.equal(w.fa.mode, 'static'); assert.ok(w.fa.anchors.length > 100, 'wind gets static arrows, no particles');
  delete global.matchMedia;
});

// ---- G10-A ----

test('G10-A A1: a zoom that ends while the tab is hidden still rebuilds when the tab is shown (nested suspends remember the request)', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(90));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const { fa, events, canvas, map } = animator('hs', l, pdir, HALF);
  const a0 = fa.anchors, v0 = fa.v;
  events.zoomstart[0](); events.movestart[0](); fa.suspend();                 // zoom + the tab hidden
  map.getZoom = () => 8; l._tileZoom = 8; map.getPixelBounds = () => ({ min: { x: 40000, y: 36000 } });
  events.zoomend[0](); events.moveend[0]();                                   // the zoom ends while hidden
  assert.equal(fa.suspended, 1); assert.equal(fa.anchors, a0, 'nothing rebuilt while hidden');
  fa.resume();                                                                // shown (the Overlay passes true, but even a bare resume must rebuild)
  assert.equal(fa.suspended, 0); assert.notEqual(fa.anchors, a0); assert.equal(fa.v.z, 8); assert.equal(canvas.style.visibility, '');
});

test('G10-A A2 / A3: reduced motion never starts the loop after hide / show; a direction delivered without a field frame cannot crash a frame', () => {
  global.matchMedia = () => ({ matches: true });
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(90));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6;
  const s = animator('hs', l, pdir, HALF);
  s.fa.suspend(); s.fa.resume(); assert.equal(s.fa.active, false); assert.equal(s.fa.scheduled, 0, 'static: no loop');
  delete global.matchMedia;
  const l2 = layer(hs, GRID, 'hs', HS); l2._tileZoom = 6;
  const { fa } = animator('hs', l2, null, null);
  l2.clear(); fa.setData(pdir, HALF);
  assert.equal(fa.dir, pdir); assert.equal(fa.anchors, null); assert.equal(fa.active, false);
  assert.doesNotThrow(() => { fa.resume(); if (fa.pending) fa.pending(100); fa._render(16); });
  assert.equal(fa.active, false, 'no loop without anchors');
});

test('G10-A M22 / M33 / P3-6 / P3-7: the fade alpha is below 1, a south wind moves particles up, a new step keeps the trails, the particle target follows the view', () => {
  const wind = frame(720, 361, () => codeOf(10, WIND)), wdirS = frame(720, 361, () => code(180)), wdirS2 = frame(720, 361, () => code(180));
  const l = layer(wind, HALF, 'wind', WIND); l._tileZoom = 6;
  const { fa, ctx, map } = animator('wind', l, wdirS, HALF);
  const k = Math.round(fa.vf.rows / 2) * fa.vf.cols + 10;
  assert.ok(fa.vf.v[k] < -10 && Math.abs(fa.vf.u[k]) < 0.5, 'from the south: screen v negative (up)');
  fa.pending(100); fa.pending(133);
  const alphas = ctx.fills; assert.ok(alphas.length >= 2 && alphas.every((a) => a > 0 && a < 1), 'fade alpha in (0, 1): ' + alphas.join());
  const clears = ctx.ops.filter((o) => o === 'clearRect').length;
  fa.setData(wdirS2, HALF, { step: 3 });                                       // the next playback step, same view
  assert.equal(ctx.ops.filter((o) => o === 'clearRect').length, clears, 'a new step does not wipe the trails');
  map.getSize = () => ({ x: 1600, y: 1200 }); fa._rebuild();
  assert.equal(fa.target, Math.round(1600 * 1200 / 900), 'the target follows the view'); assert.ok(fa.count <= fa.target);
  assert.equal(ctx.ops.filter((o) => o === 'clearRect').length, clears + 1, 'a moved view starts clean');
});

test('G10-A M59 / P3-3: no arrow over a tile that is all land or whose mask says land; code 255 is absent, not north', () => {
  const hs = frame(1440, 721, () => codeOf(3, HS)), pdir = frame(720, 361, () => code(90));
  const l = layer(hs, GRID, 'hs', HS); l._tileZoom = 6; l._clip = true;      // clipped like a real wave layer
  const { fa } = animator('hs', l, pdir, HALF);
  assert.equal(fa.anchors.length, 0, 'clipped and no tiles on the map: readoutAt gives null everywhere');
  const all = I.arrowAnchors(fa.v, 6);
  for (const a of all) { const p = I.pixelOf(a.lat, a.lng, 6); l._tiles[p.x + ':' + p.y + ':' + p.z] = { el: { _ovLand: null }, coords: { x: p.x, y: p.y, z: p.z } }; }
  fa._rebuild(); assert.ok(fa.anchors.length > 100, 'all-ocean masks: arrows');
  for (const kk in l._tiles) l._tiles[kk].el._ovLand = I.LAND_ALL;
  fa._rebuild(); assert.equal(fa.anchors.length, 0, 'all-land tiles: no arrows');
  const p255 = frame(720, 361, () => 255);
  assert.equal(I.dirAt(p255, HALF, 20, -158), null, 'code 255 is absent');
  const mixed = frame(720, 361, (r, c) => (c % 2 ? 255 : code(90)));
  assert.ok(angDiff(I.dirAt(mixed, HALF, 90 - 100 * 0.5, -180 + 100.5 * 0.5), 90) < 0.75, 'beside a 255 node the present node rules');
});
