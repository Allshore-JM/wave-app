'use strict';
// Isobars (plan section 46; the G32 fix round's Windy-like look): the sea-level pressure frame traced into smooth lines
// with at most one small pill each and H / L marks (static_overlay/overlay.js, no browser). Synthetic fields pin the
// geometry (closed loops, saddles, the date line, the interval by zoom and view width, the node spacing, the block means
// and the blur, the highs and lows); a REAL GFS PRMSL frame (tests/capture_pres_fixture.py: the job's own 920-1060 hPa half
// frame + the true values) pins how far the smoothed lines sit from the true isobars; a recording canvas pins the layer.
// node --test tests/overlay/
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

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
const HALF = { cols: 720, rows: 361, lon0: -180, lat0: 90, dlon: 0.5, dlat: -0.5, registration: 'center', lon_periodic: true };
const PRES = { lo: 920, hi: 1060, legend: [920, 1060], units: 'hPa', interpolation: 'bilinear', resolutions: ['half'] };
const codeOf = (v) => Math.max(1, Math.min(255, 1 + Math.round((v - PRES.lo) / (PRES.hi - PRES.lo) * 254)));
function frameOf(fn) {                                       // a half-grid frame of the job's codes from hPa(r, c)
  const q = new Uint8Array(720 * 361);
  for (let r = 0; r < 361; r++) for (let c = 0; c < 720; c++) q[r * 720 + c] = codeOf(fn(r, c));
  return { q, cols: 720, rows: 361 };
}
function grid(R, C, fn) { const V = new Float64Array(R * C); for (let i = 0; i < R; i++) for (let j = 0; j < C; j++) V[i * C + j] = fn(i, j); return V; }
const near = (a, b, tol) => Math.abs(a - b) <= tol;

test('the interval follows the zoom and the view width: 8 hPa at the widest views and whenever the whole world shows, 4 hPa to zoom 5.9, 2 hPa beyond (Windy)', () => {
  assert.deepEqual([1, 2, 2.9, 3, 4, 5, 5.9, 6, 9, 18].map((z) => I.isoInterval(z)), [8, 8, 8, 4, 4, 4, 4, 2, 2, 2]);
  assert.equal(I.isoInterval(4, I.ISO_WORLD_DEG), 8, 'the whole world in view: 8 hPa at any zoom');
  assert.equal(I.isoInterval(7, 360), 8);
  assert.equal(I.isoInterval(4, I.ISO_WORLD_DEG - 1), 4, 'just under: the zoom decides');
  assert.equal(I.isoInterval(2.907, 1920 / (256 * Math.pow(2, 2.907)) * 360), 8, 'the 1920-px world view (G32 B-13)');
});

test('the traced nodes are at least ISO_NODE_PX apart on screen: one in every `stride` grid nodes', () => {
  assert.deepEqual([1, 2, 3, 4, 4.5, 6, 9].map((z) => I.isoStride(z, 0.5)), [10, 5, 3, 2, 1, 1, 1]);
  for (const z of [1, 2, 2.5, 3, 3.3, 4, 4.4, 5]) {
    const px = 256 * Math.pow(2, z) * 0.5 / 360, st = I.isoStride(z, 0.5);
    assert.ok(st * px >= I.ISO_NODE_PX - 1e-9 && (st === 1 || (st - 1) * px < I.ISO_NODE_PX), `zoom ${z}: ${st} nodes of ${px.toFixed(2)} px`);
  }
});

test('a round low traces one closed loop per level at the right radius, its first point repeated at the end', () => {
  const cy = 30.3, cx = 40.7, V = grid(61, 81, (i, j) => 980 + 0.05 * ((i - cy) ** 2 + (j - cx) ** 2));
  const lines = I.traceIsolines(V, 61, 81, 2);
  const byLevel = {};
  for (const l of lines) (byLevel[l.level] = byLevel[l.level] || []).push(l);
  for (let L = 982; L <= 1004; L += 2) {
    assert.equal(byLevel[L].length, 1, `one line at ${L}`);
    const l = byLevel[L][0], p = l.pts, rad = Math.sqrt((L - 980) / 0.05);
    assert.equal(l.closed, true);
    assert.equal(p[0], p[p.length - 2]); assert.equal(p[1], p[p.length - 1]);
    let worst = 0;
    for (let m = 0; m < p.length; m += 2) worst = Math.max(worst, Math.abs(Math.hypot(p[m + 1] - cy, p[m] - cx) - rad));
    assert.ok(worst < 0.06, `radius within 0.06 node at ${L} (${worst.toFixed(3)})`);
  }
  assert.ok(!byLevel[980], 'no line at a level the field never reaches on a node');
  assert.ok(lines.every((l) => !(l.level % 2)), 'every 2 hPa');
});

test('a saddle gives two open lines through the col (never a false join), ends on the window border', () => {
  const V = grid(41, 41, (i, j) => 1000 + ((j - 20.3) ** 2 - (i - 20.6) ** 2) / 50);
  const at = I.traceIsolines(V, 41, 41, 2).filter((l) => l.level === 1000);
  assert.equal(at.length, 2);
  for (const l of at) {
    assert.equal(l.closed, false);
    const ends = [[l.pts[0], l.pts[1]], [l.pts[l.pts.length - 2], l.pts[l.pts.length - 1]]];
    for (const [x, y] of ends) assert.ok(x === 0 || y === 0 || x === 40 || y === 40, 'an end on the border');
  }
  // the four corners of the col cell have no common side: whichever pairing, the two lines share no point
  const pts = at.map((l) => new Set(Array.from({ length: l.pts.length / 2 }, (_, k) => l.pts[2 * k] + ',' + l.pts[2 * k + 1])));
  assert.equal([...pts[0]].filter((p) => pts[1].has(p)).length, 0);
});

test('a saddle cell takes the pairing its centre decides', () => {
  // a and c high at 1000 (case 10); centre mean 1000.125: a and c join, so the lines cut off b (top-right) and d (bottom-left)
  const up = I.traceIsolines(Float64Array.from([1001, 999, 999, 1001.5]), 2, 2, 2).filter((l) => l.level === 1000);
  const ends = (l) => [[l.pts[0], l.pts[1]], [l.pts[l.pts.length - 2], l.pts[l.pts.length - 1]]];
  const corner = (l) => { const e = ends(l); return e.every(([x, y]) => y === 0 || x === 1) ? 'b' : e.every(([x, y]) => x === 0 || y === 1) ? 'd' : '?'; };
  assert.deepEqual(up.map(corner).sort(), ['b', 'd']);
  // the same corners with a low centre (mean 999.875): the lines cut off a (top-left) and c (bottom-right)
  const dn = I.traceIsolines(Float64Array.from([1000.5, 999, 999, 1000]), 2, 2, 2).filter((l) => l.level === 1000);
  const corner2 = (l) => { const e = ends(l); return e.every(([x, y]) => y === 0 || x === 0) ? 'a' : e.every(([x, y]) => x === 1 || y === 1) ? 'c' : '?'; };
  assert.deepEqual(dn.map(corner2).sort(), ['a', 'c']);
});

test('a missing node ends a line there (no line through cells without data)', () => {
  const V = grid(21, 21, (i, j) => 1000 + (j - 10.2));
  V[10 * 21 + 10] = NaN;
  const at = I.traceIsolines(V, 21, 21, 2).filter((l) => l.level === 1000);
  assert.equal(at.length, 2, 'the line breaks at the hole');
  assert.ok(at.every((l) => !l.closed));
});

test('the date line: a low on it is one closed loop in a window that crosses it; projected without a jump of a world', () => {
  const lowC = 720, lowR = 140;                              // 180 E, 20 N
  const fr = frameOf((r, c) => { const dc = Math.min(Math.abs(c - lowC), 720 - Math.abs(c - lowC)); return 985 + 0.12 * ((r - lowR) ** 2 + dc ** 2); });
  const view = { z: 5, w: 1200, h: 800 };
  const pc = I.forwardPixel(20, 180, 5);
  view.ox = pc.x - 600; view.oy = pc.y - 400;
  const win = I.nodeWindow(view, HALF, fr, 0);
  assert.ok(win.count < 720 && win.c0 + win.count > 720, 'the window wraps past column 719');
  const g = I.isoValues(fr, PRES, win, 1, null);
  const lines = I.traceIsolines(g.V, g.R, g.C, 2);
  const ring = lines.filter((l) => l.level === 990);
  assert.equal(ring.length, 1); assert.equal(ring[0].closed, true);
  const paths = I.projectIsolines(ring, g, HALF, view);
  assert.equal(paths.length, 1, 'one world copy');
  const xy = paths[0].xy;
  let jump = 0, cx = 0, cy = 0;
  for (let m = 2; m < xy.length; m += 2) jump = Math.max(jump, Math.hypot(xy[m] - xy[m - 2], xy[m + 1] - xy[m - 1]));
  for (let m = 0; m < xy.length; m += 2) { cx += xy[m]; cy += xy[m + 1]; }
  cx /= xy.length / 2; cy /= xy.length / 2;
  assert.ok(jump < 30, `continuous on screen (${jump.toFixed(1)} px)`);
  assert.ok(near(cx, 600, 6) && near(cy, 400, 6), `centred on the low at the view's centre (${cx.toFixed(1)}, ${cy.toFixed(1)})`);
});

test('a whole-world view traces the cell across the date line and draws every world copy it shows', () => {
  const fr = frameOf((r, c) => 1000 + 10 * Math.sin(c / 720 * 2 * Math.PI) + (r - 180) * 0.05);
  const view = { z: 1.5, w: 2000, h: 700, ox: -300, oy: 0 };
  const win = I.nodeWindow(view, HALF, fr, 0);
  assert.equal(win.count, 720);
  const g = I.isoValues(fr, PRES, win, I.isoStride(view.z, 0.5), null);
  assert.equal(I.isoStride(view.z, 0.5), 7);
  assert.equal(g.wrap, true); assert.equal(g.stride, 8, 'a whole-world stride divides the 720 columns (7 -> 8)');
  assert.equal(g.C, 91, '90 traced columns + the seam column, the first again');
  for (let i = 0; i < g.R; i++) { const a = g.V[i * g.C], b = g.V[i * g.C + g.C - 1]; assert.ok(a === b || (a !== a && b !== b)); }
  const lines = I.traceIsolines(g.V, g.R, g.C, 8);
  assert.ok(lines.length > 0);
  const paths = I.projectIsolines(lines, g, HALF, view);
  const copies = paths.length / lines.length, n = 256 * Math.pow(2, 1.5);
  assert.ok(Number.isInteger(copies) && copies >= 3, `the view (2000 px, a world ${n.toFixed(0)} px) shows ${copies} copies`);
  for (let k = 0; k < copies; k++) {                       // each copy a world apart, and together they cover the view
    assert.ok(near(paths[k * lines.length].xy[0] - paths[0].xy[0], k * n, 1e-6));
  }
  const xs = paths.map((p) => p.xy[0]);
  assert.ok(Math.min(...xs) <= 0 && Math.max(...xs) + n >= view.w);
});

// A frame of exact codes from code(r, c) (no rounding): the means and the blur can be checked to the bit.
function codeFrame(fn) { const q = new Uint8Array(720 * 361); for (let r = 0; r < 361; r++) for (let c = 0; c < 720; c++) q[r * 720 + c] = fn(r, c); return { q, cols: 720, rows: 361 }; }
const valOf = (code) => PRES.lo + (code - 1) * (PRES.hi - PRES.lo) / 254;

test('isoValues: a constant field stays exact at every stride; block means are centred on their node (odd and even strides); a plane keeps its interior values', () => {
  const flat = codeFrame(() => 100);
  for (const st of [1, 2, 3, 4, 5]) {
    const g = I.isoValues(flat, PRES, { r0: 100, r1: 140, c0: 30, count: 41 }, st, null);
    for (let k = 0; k < g.R * g.C; k++) assert.ok(Math.abs(g.V[k] - valOf(100)) < 1e-9, `stride ${st}`);
  }
  const plane = codeFrame((r, c) => 20 + (c % 200) + Math.floor(r / 4));      // exact in columns; rows step every 4
  for (const st of [1, 2, 3, 4]) {
    const g = I.isoValues(codeFrame((r, c) => 20 + (c % 200)), PRES, { r0: 100, r1: 140, c0: 30, count: 101 }, st, null);
    for (let i = 0; i < g.R; i++) for (let j = 2; j < g.C - 2; j++) {           // the blur renormalises at the window's edges only
      const col = 30 + j * st;
      assert.ok(Math.abs(g.V[i * g.C + j] - valOf(20 + col)) < 1e-9, `stride ${st}: node ${j} (column ${col}) is its own column's value`);
    }
  }
  void plane;
});

test('isoValues: a missing node stays missing and is left out of its neighbours; the blur smooths a step', () => {
  const fr = codeFrame((r, c) => c < 50 ? 100 : 120);
  fr.q[100 * 720 + 40] = 0;
  const g = I.isoValues(fr, PRES, { r0: 90, r1: 110, c0: 30, count: 41 }, 1, null);
  assert.ok(Number.isNaN(g.V[10 * g.C + 10]), 'the hole');
  assert.ok(Math.abs(g.V[10 * g.C + 9] - valOf(100)) < 1e-9 && Math.abs(g.V[10 * g.C + 11] - valOf(100)) < 1e-9, 'its flat neighbours keep their value (no zero pulled in)');
  const row = Array.from({ length: g.C }, (_, j) => g.V[5 * g.C + j]);
  assert.ok(row[19] > valOf(100) && row[19] < valOf(120) && row[20] > valOf(100) && row[20] < valOf(120), 'the step is spread over the nodes beside it');
  for (let j = 1; j < g.C; j++) assert.ok(row[j] >= row[j - 1] - 1e-9, 'and stays monotone');
  assert.ok(Math.abs(row[0] - valOf(100)) < 1e-9 && Math.abs(row[g.C - 1] - valOf(120)) < 1e-9, 'far from it nothing moves');
});

test('isoBlur: a whole-world window blurs across the date line (the seam column is the first again)', () => {
  const C = 9, R = 3, V = new Float64Array(R * C), T = new Float64Array(R * C);
  for (let i = 0; i < R; i++) { for (let j = 0; j < C - 1; j++) V[i * C + j] = j === 0 ? 10 : 0; V[i * C + C - 1] = V[i * C]; }
  I.isoBlur(V, T, R, C, true);
  assert.ok(Math.abs(V[1 * C + 7] - 2.5) < 1e-9, 'the last column feels the first across the seam');
  assert.ok(Math.abs(V[1 * C + 1] - 2.5) < 1e-9);
  assert.ok(Math.abs(V[1 * C] - 5) < 1e-9, 'the first column averages its neighbour across the seam too: (2 x 10 + 0 + 0) / 4');
  assert.equal(V[1 * C + C - 1], V[1 * C], 'the seam column = the first');
  const W = new Float64Array(R * C); for (let i = 0; i < R; i++) W[i * C] = 10;
  I.isoBlur(W, T, R, C, false);
  assert.equal(W[1 * C + C - 2], 0, 'not a whole world: no wrap');
});

// ---- the real frame (G32a A-1 (b)) ----
function realFrames() {
  const png = fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'pres_half.png'));
  const raw = zlib.gunzipSync(fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'pres_half_true.bin.gz')));
  const truth = new Float64Array(720 * 361);
  for (let i = 0; i < truth.length; i++) truth[i] = 900 + raw.readUInt16LE(2 * i) / 20;
  return { png: png.buffer.slice(png.byteOffset, png.byteOffset + png.byteLength), truth };
}
// The share of the true isobars' points more than `tol` nodes from the traced line of the same level.
function offShare(trueLines, lines, tol) {
  const segs = new Map(), B = 1;
  for (const l of lines) {
    const p = l.pts;
    for (let m = 2; m < p.length; m += 2) {
      const key = l.level + ':' + Math.floor(p[m] / B) + ':' + Math.floor(p[m + 1] / B);
      (segs.get(key) || segs.set(key, []).get(key)).push([p[m - 2], p[m - 1], p[m], p[m + 1]]);
    }
  }
  function dist(x, y, s) {
    const dx = s[2] - s[0], dy = s[3] - s[1], L2 = dx * dx + dy * dy, t = L2 ? Math.max(0, Math.min(1, ((x - s[0]) * dx + (y - s[1]) * dy) / L2)) : 0;
    return Math.hypot(x - s[0] - t * dx, y - s[1] - t * dy);
  }
  let off = 0, all = 0;
  const W = Math.ceil(tol) + 2;                               // segments within tol of a point lie within W cells of it
  for (const l of trueLines) {
    const p = l.pts;
    for (let m = 0; m < p.length; m += 2) {
      let best = Infinity;
      for (let gx = Math.floor(p[m]) - W; gx <= Math.floor(p[m]) + W; gx++) for (let gy = Math.floor(p[m + 1]) - W; gy <= Math.floor(p[m + 1]) + W; gy++) {
        const list = segs.get(l.level + ':' + gx + ':' + gy);
        if (list) for (const s of list) best = Math.min(best, dist(p[m], p[m + 1], s));
      }
      all++; if (best > tol) off++;
    }
  }
  return off / all;
}

test('on a real GFS PRMSL frame the smoothed isobars follow the large systems closely and are far smoother than the coded field', async () => {
  const { png, truth } = realFrames();
  const fr = await I.decodePngGrey(png, { cols: 720, rows: 361 });
  const R = 241, C = 719;                                  // |lat| <= 60 (the reviewer's measure), the 719 cells of one world
  const T = new Float64Array(R * C), Q = new Float64Array(R * C);
  for (let i = 0; i < R; i++) for (let j = 0; j < C; j++) {
    T[i * C + j] = truth[(60 + i) * 720 + j];
    const q = fr.q[(60 + i) * 720 + j]; Q[i * C + j] = PRES.lo + (q - 1) * (PRES.hi - PRES.lo) / 254;
  }
  const length = (l) => { let s = 0; for (let m = 2; m < l.pts.length; m += 2) s += Math.hypot(l.pts[m] - l.pts[m - 2], l.pts[m + 1] - l.pts[m - 1]); return s; };
  const turn = (ls) => { let s = 0, n = 0; for (const l of ls) { const p = l.pts; for (let m = 4; m < p.length; m += 2) {
    let d = Math.abs(Math.atan2(p[m + 1] - p[m - 1], p[m] - p[m - 2]) - Math.atan2(p[m - 1] - p[m - 3], p[m - 2] - p[m - 4])); if (d > Math.PI) d = 2 * Math.PI - d; s += d; n++; } } return s / n; };
  const truthAll = I.traceIsolines(T, R, C, 2), truthBig = truthAll.filter((l) => length(l) >= 30);   // systems with >= 30 nodes (~1,650 km) of line
  const g = I.isoValues(fr, PRES, { r0: 60, r1: 300, c0: 0, count: 719 }, 1, null);
  const ours = I.traceIsolines(g.V, g.R, g.C, 2), raw = I.traceIsolines(Q, R, C, 2);
  const off1 = offShare(truthBig, ours, 1), off2 = offShare(truthBig, ours, 2);
  console.log(`real PRMSL, the large systems: points > 1 node off ${(off1 * 100).toFixed(2)} %, > 2 nodes ${(off2 * 100).toFixed(2)} %; ` +
    `lines ${ours.length} (truth ${truthAll.length}, raw codes ${raw.length}); mean turn per vertex ${turn(ours).toFixed(3)} (raw ${turn(raw).toFixed(3)})`);
  assert.ok(off1 < 0.07, `> 1 node (55 km): ${(off1 * 100).toFixed(2)} %`);
  assert.ok(off2 < 0.025, `> 2 nodes (110 km): ${(off2 * 100).toFixed(2)} %`);
  assert.ok(turn(ours) < 0.5 * turn(raw), 'far smoother than the coded field');
  assert.ok(ours.length < 0.5 * raw.length, 'the small rings of the coded field (mountain reductions, 8-bit terraces) are gone');
});

test('the whole-world redraw of a real frame stays cheap (1920x1080, the world in view; a soft ceiling, the time logged)', async () => {
  const { png } = realFrames();
  const fr = await I.decodePngGrey(png, { cols: 720, rows: 361 });
  const z = Math.log2(1920 / 256), n = 256 * Math.pow(2, z), view = { z, w: 1920, h: 1080, ox: 0, oy: n / 2 - 540 };
  const span = view.w / n * 360;
  let best = Infinity, lines = 0, scratch = null; const P = {};
  for (let k = 0; k < 5; k++) {
    const t0 = performance.now();
    const g = I.isoValues(fr, PRES, I.nodeWindow(view, HALF, fr, 0), I.isoStride(view.z, 0.5), scratch); scratch = g.scratch;
    const paths = I.projectIsolines(I.traceIsolines(g.V, g.R, g.C, I.isoInterval(view.z, span)), g, HALF, view);
    const rad = Math.round(I.ISO_HL_PX / (n * 0.5 / 360));
    I.projectCentres(I.findCentres(fr, PRES, I.nodeWindow(view, HALF, fr, 0), rad, { stride: I.isoStride(z, 0.5), margin: Math.max(2 * rad, 16), grid: HALF, scratch: P }), HALF, view);
    best = Math.min(best, performance.now() - t0); lines = paths.length;
  }
  console.log(`whole world 1920x1080 (zoom ${z.toFixed(3)}, 8 hPa): ${best.toFixed(1)} ms, ${lines} paths`);
  assert.ok(best < 100, `${best.toFixed(1)} ms`);
});

// ---- labels: at most ONE small pill per line (owner, G32) ----
const hline = (level, y, x0, x1) => ({ level, xy: Float64Array.from([x0, y, (x0 + x1) / 2, y, x1, y]), closed: false });
const aabb = (b) => { const ca = Math.abs(Math.cos(b.a)), sa = Math.abs(Math.sin(b.a)), hw = (b.w * ca + b.h * sa) / 2, hv = (b.w * sa + b.h * ca) / 2; return { x0: b.x - hw, x1: b.x + hw, y0: b.y - hv, y1: b.y + hv }; };

test('labels: one pill per line, at the middle of its length in view, rotated along it and upright; none on a line under ISO_LABEL_MIN_PX in view', () => {
  const view = { w: 1200, h: 800 };
  const one = I.placeLabels([hline(1004, 400, 50, 1150)], view, (t) => 6 * t.length);
  assert.equal(one.length, 1, 'one pill on a 1,100-px line');
  assert.equal(Math.round(one[0].x), 600); assert.equal(one[0].a, 0);
  assert.equal(one[0].text, '1004'); assert.equal(one[0].w, 24 + 2 * I.ISO_STYLE.padX); assert.equal(one[0].h, I.ISO_STYLE.h);
  const half = I.placeLabels([hline(1004, 400, -1000, 1000)], view, (t) => 6 * t.length);
  assert.equal(Math.round(half[0].x), 500, 'the middle of the part in view (0..1000), not of the whole line');
  const back = I.placeLabels([hline(996, 300, 900, 100)], view, () => 24);
  assert.equal(back[0].a, 0, 'drawn right to left: the text still upright');
  const up = I.placeLabels([{ level: 996, xy: Float64Array.from([600, 700, 600, 100]), closed: false }], view, () => 24);
  const down = I.placeLabels([{ level: 996, xy: Float64Array.from([600, 100, 600, 700]), closed: false }], view, () => 24);
  assert.ok(near(up[0].a, Math.PI / 2, 1e-12) && near(down[0].a, Math.PI / 2, 1e-12), 'vertical either way: the same reading direction');
  assert.deepEqual(I.placeLabels([hline(996, 300, 100, 100 + I.ISO_LABEL_MIN_PX - 1)], view, () => 24), [], 'too short in view');
  assert.equal(I.placeLabels([hline(996, 300, 100, 100 + I.ISO_LABEL_MIN_PX + 1)], view, () => 24).length, 1);
  assert.deepEqual(I.placeLabels([hline(996, 300, -2000, -1000)], view, () => 24), [], 'off the view: none');
  const loop = []; for (let k = 0; k <= 64; k++) loop.push(600 + 300 * Math.cos(k / 64 * 2 * Math.PI), 400 + 300 * Math.sin(k / 64 * 2 * Math.PI));
  assert.equal(I.placeLabels([{ level: 1000, xy: Float64Array.from(loop), closed: true }], view, () => 24).length, 1, 'a closed ring 1,900 px round: still one');
});

test('labels: a pill blocked at the middle moves along its line (0.35, 0.65 ...) and is left out only when no place is free', () => {
  const view = { w: 1200, h: 800 };
  const mark = { type: 'H', x: 600, y: 400, text: '1030' };
  const moved = I.placeLabels([hline(1028, 400, 100, 1100)], view, (t) => 6 * t.length, [mark]);
  assert.equal(moved.length, 1); assert.equal(Math.round(moved[0].x), 450, 'the 0.35 point (an H at the middle)');
  const marks = I.ISO_LABEL_AT.map((f) => ({ type: 'L', x: 100 + 1000 * f, y: 400, text: '990' }));
  assert.deepEqual(I.placeLabels([hline(992, 400, 100, 1100)], view, (t) => 6 * t.length, marks), [], 'every place taken: none');
});

test('labels never overlap each other or the H / L marks (every grid cell a pill covers holds it: G32 B-2) and keep their edge distance', () => {
  let seed = 11; const rnd = () => (seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648;
  const view = { w: 900, h: 640 }, E = I.ISO_LABEL_EDGE_PX, gap = I.ISO_LABEL_GAP_PX;
  let placed = 0;
  for (let run = 0; run < 40; run++) {
    const paths = [], marks = [];
    for (let k = 0; k < 120; k++) {
      const x0 = rnd() * 1000 - 50, y0 = rnd() * 740 - 50, ang = rnd() * Math.PI * 2, L = 130 + rnd() * 400, pts = [];
      for (let s = 0; s <= 8; s++) pts.push(x0 + Math.cos(ang) * L * s / 8, y0 + Math.sin(ang) * L * s / 8 + 20 * Math.sin(s));
      paths.push({ level: 980 + 2 * k, xy: Float64Array.from(pts), closed: false });
    }
    for (let k = 0; k < 6; k++) marks.push({ type: k % 2 ? 'H' : 'L', x: 40 + rnd() * 820, y: 40 + rnd() * 560, text: '1000' });
    const labels = I.placeLabels(paths, view, (t) => 5.5 * t.length, marks);
    placed += labels.length;
    const boxes = labels.map(aabb), mboxes = marks.map((m) => ({ x0: m.x - 16, x1: m.x + 16, y0: m.y - 15, y1: m.y + 16 }));
    for (let i = 0; i < boxes.length; i++) {
      const a = boxes[i];
      assert.ok(a.x0 >= E - 1e-9 && a.y0 >= E - 1e-9 && a.x1 <= view.w - E + 1e-9 && a.y1 <= view.h - E + 1e-9, 'edge distance');
      for (const b of boxes.slice(i + 1).concat(mboxes)) {
        assert.ok(!(a.x0 < b.x1 + gap && b.x0 < a.x1 + gap && a.y0 < b.y1 + gap && b.y0 < a.y1 + gap), `run ${run}: no overlap`);
      }
    }
    assert.equal(new Set(labels.map((l) => l.text)).size, labels.length, 'one pill per line');
  }
  assert.ok(placed > 400, `${placed} pills placed in 40 crowded views`);
});

// ---- highs and lows: from the frame's own codes (exact centres and pressures) ----
// An independent brute force of the rule: every node's rad-square extremes by sliding passes (columns wrap, rows end at the
// grid, missing nodes left out); ties only among the 8 neighbours (the earlier node in raster order stands); the depth
// against the square's border mean; then in raster order: a node of a patch already taken for its kind is skipped, one
// within rad of a kept centre of its kind is dropped, else its whole patch is gathered breadth first (no mark for a shelf,
// a patch over ISO_PATCH_MAX nodes or one spanning half the columns) and the mark sits at the patch's middle.
// The independent brute force of the H / L rule (fix round 3): the square test, the patch rules, the clamped ends and the
// terrain as the module states them, then the PROMINENCE of each candidate by a minimax flood (Dijkstra on the greatest code
// along the path) from its block over the window widened by `margin` nodes, on blocks of `stride` nodes (its own block
// reduction): the first block popped that is stronger (a lesser code, or an equal code whose index is below the
// candidate's own root block: the earliest block of its equal-code set) ends it, the prominence = that path's greatest
// code minus the candidate's; none -> the cap. Nothing shared with the module's merge tree.
function bruteCentres(fr, win, rad, opts) {
  opts = opts || {};
  const Q = fr.q, cols = fr.cols, rows = fr.rows, k = (PRES.hi - PRES.lo) / 254, wrapC = (c) => ((c % cols) + cols) % cols;
  const r0 = Math.max(0, win.r0), r1 = Math.min(rows - 1, win.r1), n = Math.min(win.count, cols);
  const R0 = Math.max(0, r0 - rad), R1 = Math.min(rows - 1, r1 + rad), rlo = [], rhi = [];
  for (let r = R0; r <= R1; r++) {                           // each row's extremes over +-rad columns, per window column
    const lo = new Float64Array(n), hi = new Float64Array(n);
    for (let jj = 0; jj < n; jj++) {
      let a = Infinity, b = -Infinity;
      for (let dc = -rad; dc <= rad; dc++) { const x = Q[r * cols + wrapC(win.c0 + jj + dc)]; if (x) { if (x < a) a = x; if (x > b) b = x; } }
      lo[jj] = a; hi[jj] = b;
    }
    rlo.push(lo); rhi.push(hi);
  }
  const cands = [];
  for (let r = r0; r <= r1; r++) for (let jj = 0; jj < n; jj++) {
    const c = wrapC(win.c0 + jj), v = Q[r * cols + c];
    if (!v) continue;
    let a = Infinity, b = -Infinity;
    for (let rr = Math.max(R0, r - rad); rr <= Math.min(R1, r + rad); rr++) { a = Math.min(a, rlo[rr - R0][jj]); b = Math.max(b, rhi[rr - R0][jj]); }
    if (a !== v && b !== v) continue;
    let tie = false;                                          // an equal neighbour earlier in raster order, inside the window
    const inWin = (rr, cc) => rr >= r0 && rr <= r1 && (n >= cols || wrapC(cc - win.c0) < n);
    for (let dr = -1; dr <= 1; dr++) for (let dc = -1; dc <= 1; dc++) {
      if ((dr < 0 || (!dr && dc < 0)) && r + dr >= 0 && Q[(r + dr) * cols + wrapC(c + dc)] === v && inWin(r + dr, wrapC(c + dc))) tie = true;
    }
    if (tie) continue;
    if (a === v && v !== 255) cands.push({ type: 'L', r, c, q: v });
    if (b === v && v !== 1) cands.push({ type: 'H', r, c, q: v });
  }
  cands.sort((x, y) => (x.type !== y.type ? (x.type < y.type ? -1 : 1) : x.r - y.r || x.c - y.c));
  // the prominence window: blocks of `stride`
  const mg = opts.margin || 0, st = opts.stride || 1, PR0 = Math.max(0, r0 - mg), PR1 = Math.min(rows - 1, r1 + mg);
  let PC0 = wrapC(win.c0 - mg), pcount = n + 2 * mg, pwrap = pcount >= cols, s2 = st;
  if (pwrap) { pcount = cols; while (cols % s2) s2--; }
  const bR = Math.ceil((PR1 - PR0 + 1) / s2), bC = Math.ceil(pcount / s2);
  const blockVal = (low) => {
    const val = new Int32Array(bR * bC).fill(0);
    for (let r = PR0; r <= PR1; r++) for (let j = 0; j < pcount; j++) {
      const x = Q[r * cols + wrapC(PC0 + j)]; if (!x) continue;
      const v = low ? x : 256 - x, bi = Math.floor((r - PR0) / s2) * bC + Math.floor(j / s2);
      if (!val[bi] || v < val[bi]) val[bi] = v;
    }
    return val;
  };
  const vals = { L: blockVal(true), H: blockVal(false) };
  const blockOf = (r, c) => { const jj = wrapC(c - PC0); return r < PR0 || r > PR1 || jj >= pcount ? -1 : Math.floor((r - PR0) / s2) * bC + Math.floor(jj / s2); };
  const neigh = (bi) => { const out = [], i = Math.floor(bi / bC), j = bi - i * bC; for (let di = -1; di <= 1; di++) for (let dj = -1; dj <= 1; dj++) { if (!di && !dj) continue; const ni = i + di; let nj = j + dj; if (ni < 0 || ni >= bR) continue; if (nj < 0 || nj >= bC) { if (!pwrap) continue; nj = (nj + bC) % bC; } out.push(ni * bC + nj); } return out; };
  const gkey = (bi) => { const i = Math.floor(bi / bC), j = bi - i * bC; return i * 100000 + wrapC(PC0 + j * s2); };   // the grid's raster order (global columns)
  const prominence = (cd) => {
    const low = cd.type === 'L', val = vals[cd.type], b0 = blockOf(cd.r, cd.c);
    if (b0 < 0 || !val[b0]) return -1;
    const q = val[b0];
    let root = b0; const seen = new Set([b0]), st2 = [b0];        // the equal-code set's earliest block (the grid's order)
    while (st2.length) { const x = st2.pop(); if (gkey(x) < gkey(root)) root = x; for (const y of neigh(x)) if (!seen.has(y) && val[y] === q) { seen.add(y); st2.push(y); } }
    const dist = new Map([[b0, q]]), done = new Set(), heap = [[q, b0]];    // Dijkstra-minimax on a binary heap (a lazy delete)
    const push = (e) => { heap.push(e); let i = heap.length - 1; while (i > 0) { const p = (i - 1) >> 1; if (heap[p][0] <= heap[i][0]) break; [heap[p], heap[i]] = [heap[i], heap[p]]; i = p; } };
    const pop = () => { const top = heap[0], last = heap.pop(); if (heap.length) { heap[0] = last; let i = 0; for (;;) { const l = 2 * i + 1, r = l + 1; let m = i; if (l < heap.length && heap[l][0] < heap[m][0]) m = l; if (r < heap.length && heap[r][0] < heap[m][0]) m = r; if (m === i) break; [heap[m], heap[i]] = [heap[i], heap[m]]; i = m; } } return top; };
    while (heap.length) {                                       // pop the least greatest-code-on-path
      const [bd, best] = pop();
      if (done.has(best) || dist.get(best) !== bd) continue;
      done.add(best);
      if (val[best] < q || (val[best] === q && gkey(best) < gkey(root))) return bd - q;
      for (const y of neigh(best)) { if (!val[y] || done.has(y)) continue; const nd = Math.max(bd, val[y]); if (!(dist.has(y)) || nd < dist.get(y)) { dist.set(y, nd); push([nd, y]); } }
    }
    return I.ISO_HL_PROM_CAP;
  };
  const terrain = opts.terrain && opts.terrain.length === rows * cols ? opts.terrain : null, tmin = opts.terrainMin || 1;
  const high = (r, c) => !!terrain && terrain[Math.round(r) * cols + wrapC(Math.round(c))] >= tmin;
  const taken = { H: new Set(), L: new Set() }, kept = [];
  for (const cd of cands) {
    if (taken[cd.type].has(cd.r * cols + cd.c)) continue;
    const low = cd.type === 'L', seen = new Map([[cd.r * cols + cd.c, 0]]), queue = [[cd.r, cd.c, 0]];
    let shelf = false;
    for (let h = 0; h < queue.length; h++) {
      const [r, c, off] = queue[h];
      for (let dr = -1; dr <= 1; dr++) for (let dc = -1; dc <= 1; dc++) {
        const rr = r + dr; if ((!dr && !dc) || rr < 0 || rr >= rows) continue;
        const key = rr * cols + wrapC(c + dc), y = Q[key];
        if (!y) continue;
        if (low ? y < cd.q : y > cd.q) shelf = true;
        else if (y === cd.q && !seen.has(key)) { seen.set(key, off + dc); queue.push([rr, wrapC(c + dc), off + dc]); }
      }
    }
    for (const key of seen.keys()) taken[cd.type].add(key);
    if (shelf || seen.size > I.ISO_PATCH_MAX) continue;
    const offs = [...seen.values()];
    if (Math.max(...offs) - Math.min(...offs) >= cols >> 1) continue;
    let fk = Infinity; for (const key of seen.keys()) if (key < fk) fk = key;   // the patch's first node in raster order
    const fr0 = Math.floor(fk / cols), fc0 = fk % cols;
    const clamp = cd.q === 1 ? -1 : cd.q === 255 ? 1 : 0;
    if (clamp && (seen.size > I.ISO_CLAMP_PATCH_MAX || Math.abs(90 - fr0 * 0.5) > I.ISO_CLAMP_LAT)) continue;
    let sr = 0, sc = 0;
    for (const [key, off] of seen) { sr += Math.floor(key / cols); sc += off; }
    const mr = sr / seen.size, mc = ((cd.c + sc / seen.size) % cols + cols) % cols;
    let prom = null;
    if (high(fr0, fc0) || high(mr, mc)) {                     // only a strong low over ground below the cap keeps its mark
      const lowG = (r, c) => opts.terrainCap > 0 && terrain[Math.round(r) * cols + wrapC(Math.round(c))] < opts.terrainCap;
      if (!(low && opts.strongLowHpa > 0 && lowG(fr0, fc0) && lowG(mr, mc) && (prom = prominence(cd)) * k >= opts.strongLowHpa - 1e-9)) continue;
    }
    if (prom === null) prom = prominence(cd);
    if (prom < I.ISO_HL_PROM_CODES) continue;
    kept.push({ type: cd.type, r: mr, c: mc, q: cd.q, prom, clamp, v: PRES.lo + (cd.q - 1) * k });
  }
  return kept.map(key).sort();
}
const key = (e) => e.type + e.r + ',' + e.c + ' q' + e.q + ' p' + e.prom + ' c' + e.clamp;
const OPTS = (rad, extra) => Object.assign({ margin: Math.max(2 * rad, 16), stride: 1, grid: HALF }, extra || {});

test('findCentres = an independent brute force of the rule (random smooth fields with noise, plateaus and holes; windows that wrap, cross the seam or touch the poles; radii 1-14; margins; the terrain)', () => {
  let seed = 3; const rnd = () => (seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648;
  let runs = 0, found = 0, proms = new Set();
  const terrain = new Uint8Array(720 * 361);
  for (let i = 0; i < terrain.length; i++) terrain[i] = (i * 7919) % 97 < 9 ? 30 : 0;    // high ground on ~9 % of the nodes
  for (let t = 0; t < 36; t++) {
    const cx = [], n = 3 + Math.floor(rnd() * 3);
    for (let i = 0; i < n; i++) cx.push([rnd() * 361, rnd() * 720, (rnd() - 0.5) * 50, 20 + rnd() * 200]);
    const plateau = t % 4 === 0, noise = rnd() * 3;
    const fr = codeFrame((r, c) => { let v = 1012; for (const [a, b, d, s] of cx) { const dc = Math.min(Math.abs(c - b), 720 - Math.abs(c - b)); v += d * Math.exp(-((r - a) ** 2 + dc ** 2) / (2 * s)); }
      v += noise * Math.sin(r * 1.7 + c * 2.3); const q = Math.max(1, Math.min(255, 1 + Math.round((v - PRES.lo) / (PRES.hi - PRES.lo) * 254)));
      return plateau ? q - (q % 2) || 1 : q; });
    if (t % 5 === 0) for (let h = 0; h < 40; h++) fr.q[Math.floor(rnd() * 361) * 720 + Math.floor(rnd() * 720)] = 0;   // holes
    const wins = [{ r0: Math.floor(rnd() * 250), r1: 0, c0: Math.floor(rnd() * 720), count: 40 + Math.floor(rnd() * 160) }, { r0: 330, r1: 360, c0: 700, count: 60 }];
    wins[0].r1 = Math.min(360, wins[0].r0 + 20 + Math.floor(rnd() * 80));
    if (t % 9 === 0) wins.push({ r0: 0, r1: 360, c0: 0, count: 720 });   // the whole world (small radii only: the brute force is slow)
    for (const win of wins) for (const rad of [1, 3, 7, 14]) {
      if (win.count === 720 && rad > 3) continue;
      const o = OPTS(rad, { margin: t % 3 === 0 ? 0 : Math.max(2 * rad, 16), terrain: t % 2 ? terrain : null, terrainMin: 25,
        strongLowHpa: [0, 2, 6][t % 3], terrainCap: [0, 35, 28, 31][t % 4] });   // the strong-low exception: none, some, every low; caps over / under the ground
      const got = I.findCentres(fr, PRES, win, rad, o).map(key).sort(), want = bruteCentres(fr, win, rad, o);
      assert.deepEqual(got, want, `t ${t}, window ${JSON.stringify(win)}, rad ${rad}`);
      runs++; found += got.length; for (const g of got) proms.add(g.split(' p')[1].split(' ')[0]);
    }
  }
  assert.ok(found > 100 && proms.size > 10, `${found} centres in ${runs} runs, ${proms.size} prominence values`);
});

test('prominenceMap on blocks (the lines\' stride at wide views) = the brute force on the same block reduction: the whole world at strides 2, 3, 5 (dividing 720; 7 -> 6), a window crossing the seam at 2', () => {
  let seed = 11; const rnd = () => (seed = (seed * 1103515245 + 12345) % 2147483648) / 2147483648;
  const cx = []; for (let i = 0; i < 6; i++) cx.push([rnd() * 361, rnd() * 720, (rnd() - 0.5) * 60, 40 + rnd() * 400]);
  const fr = codeFrame((r, c) => { let v = 1012; for (const [a, b, d, s] of cx) { const dc = Math.min(Math.abs(c - b), 720 - Math.abs(c - b)); v += d * Math.exp(-((r - a) ** 2 + dc ** 2) / (2 * s)); } return Math.max(1, Math.min(255, 1 + Math.round((v - PRES.lo) / (PRES.hi - PRES.lo) * 254))); });
  for (let h = 0; h < 60; h++) fr.q[Math.floor(rnd() * 361) * 720 + Math.floor(rnd() * 720)] = 0;
  const cases = [[{ r0: 0, r1: 360, c0: 0, count: 720 }, 2, 3], [{ r0: 0, r1: 360, c0: 0, count: 720 }, 3, 3], [{ r0: 0, r1: 360, c0: 0, count: 720 }, 5, 3], [{ r0: 0, r1: 360, c0: 0, count: 720 }, 7, 3],
    [{ r0: 60, r1: 200, c0: 650, count: 150 }, 2, 7], [{ r0: 100, r1: 160, c0: 700, count: 60 }, 3, 5]];
  let found = 0;
  for (const [win, stride, rad] of cases) {
    const o = { margin: 16, stride, grid: HALF };
    const got = I.findCentres(fr, PRES, win, rad, o).map(key).sort(), want = bruteCentres(fr, win, rad, o);
    assert.deepEqual(got, want, `stride ${stride} ${JSON.stringify(win)}`);
    found += got.length;
  }
  assert.ok(found > 20, found + ' centres');
  const pm = I.prominenceMap(fr, 0, 360, 0, 720, 7, true, {});
  assert.equal(pm.at(-1, 0), -1); assert.equal(pm.at(0, 5000), pm.at(0, 5000 % 720), 'columns wrap');
});

test('findCentres: the exact centre and pressure of a compact deep low the smoothed lines flatten (a typhoon); none on a slope; a one-code dimple beside a stronger low has no prominence (the only low of a region counts); a clamped end marks only when compact and away from the poles, with its clamp', () => {
  const k = (PRES.hi - PRES.lo) / 254;
  const ty = codeFrame((r, c) => Math.max(1, Math.round(1 + (1010 - 52 * Math.exp(-((r - 150) ** 2 + (c - 640) ** 2) / 3) - PRES.lo) / k)));   // ~1-node radius, 958 hPa
  const win = { r0: 100, r1: 200, c0: 560, count: 160 };
  const e = I.findCentres(ty, PRES, win, 10, OPTS(10));
  assert.deepEqual(e.map((x) => [x.type, x.r, x.c, x.clamp]), [['L', 150, 640, 0]]);
  assert.equal(e[0].prom, I.ISO_HL_PROM_CAP, 'the only low of its region');
  assert.deepEqual(I.findCentres(ty, PRES, win, 1000, OPTS(1000)).map((x) => [x.type, x.r, x.c]), [['L', 150, 640]], 'a radius wider than the world: half the world');
  assert.ok(near(e[0].v, 958, k), `the central pressure ${e[0].v.toFixed(1)}`);
  const g = I.isoValues(ty, PRES, win, 3, null), sm = Math.min(...Array.from(g.V.subarray(0, g.R * g.C)).filter((x) => x === x));
  assert.ok(sm > 990, `the smoothed field at that view reads ${sm.toFixed(1)}: the lines alone would hide it`);
  assert.deepEqual(I.findCentres(codeFrame((r, c) => 60 + Math.floor(c / 4) + Math.floor(r / 5)), PRES, { r0: 100, r1: 200, c0: 300, count: 160 }, 10, OPTS(10)), [], 'a slope');
  // a one-code dimple (0.55 hPa) 30 nodes from a deeper low: its basin reaches the low one code up -> prominence 1, no mark
  const dim = codeFrame((r, c) => 100 - (Math.abs(r - 150) <= 1 && Math.abs(c - 640) <= 1 ? 1 : 0) - (Math.abs(r - 150) <= 1 && Math.abs(c - 670) <= 1 ? 6 : 0));
  const d = I.findCentres(dim, PRES, win, 10, OPTS(10));
  assert.deepEqual(d.map((x) => [x.type, x.r, x.c, x.prom]), [['L', 150, 670, I.ISO_HL_PROM_CAP]], 'the deeper low only');
  const flat = codeFrame((r, c) => 100 - (Math.abs(r - 150) <= 1 && Math.abs(c - 640) <= 1 ? 1 : 0));
  assert.deepEqual(I.findCentres(flat, PRES, win, 10, OPTS(10)).map((x) => [x.type, x.r, x.c, x.prom]), [['L', 150, 640, I.ISO_HL_PROM_CAP]], 'alone in a flat field: the region\'s one low');
  const two = codeFrame((r, c) => 100 - (Math.abs(r - 150) <= 1 && Math.abs(c - 640) <= 1 ? 2 : 0) - (Math.abs(r - 150) <= 1 && Math.abs(c - 670) <= 1 ? 6 : 0));
  assert.deepEqual(I.findCentres(two, PRES, win, 10, OPTS(10)).map((x) => [x.type, x.c, x.prom]).sort((p, q) => p[1] - q[1]), [['L', 640, 2], ['L', 670, I.ISO_HL_PROM_CAP]], 'two codes deep: marked, with its prominence');
  // the clamped ends: a 3 x 3 patch of code 1 (below 920 hPa) at 15 N is a centre marked "<=920"; a 5 x 5 one is not compact; one at 80 S never
  const eye = (sz, r) => codeFrame((rr, c) => (Math.abs(rr - r) < sz && Math.abs(c - 640) < sz ? 1 : 60));
  const ce = I.findCentres(eye(2, 150), PRES, win, 10, OPTS(10));
  assert.deepEqual(ce.map((x) => [x.type, x.r, x.c, x.clamp, x.v]), [['L', 150, 640, -1, PRES.lo]], 'a compact clamped low');
  assert.deepEqual(I.findCentres(eye(3, 150), PRES, win, 10, OPTS(10)), [], '25 nodes: not compact');
  assert.deepEqual(I.findCentres(eye(2, 340), PRES, { r0: 320, r1: 360, c0: 560, count: 160 }, 10, OPTS(10)), [], 'at 80 S: the polar plateau');
  const top = codeFrame((r, c) => (Math.abs(r - 150) < 2 && Math.abs(c - 640) < 2 ? 255 : 200));
  assert.deepEqual(I.findCentres(top, PRES, win, 10, OPTS(10)).map((x) => [x.type, x.r, x.c, x.clamp, x.v]), [['H', 150, 640, 1, PRES.hi]], 'a compact clamped high');
  assert.deepEqual(I.findCentres(codeFrame((r, c) => (Math.abs(r - 150) < 2 && Math.abs(c - 640) < 2 ? 1 : 60)), PRES, win, 10, { margin: 16, stride: 1 }), [['L', 150, 640, -1].length && I.findCentres(eye(2, 150), PRES, win, 10, { margin: 16, stride: 1 })[0]].filter(Boolean), 'without a grid the latitude is not judged');
  assert.deepEqual(I.findCentres(codeFrame((r, c) => (Math.abs(r - 150) < 2 && Math.abs(c - 640) < 2 ? 2 : 60)), PRES, win, 10, OPTS(10)).map((x) => [x.clamp, Math.round(x.v)]), [[0, 921]], 'code 2 is an ordinary value');
  const plateau = codeFrame((r, c) => (Math.abs(r - 150) <= 2 && Math.abs(c - 640) <= 2 ? 80 : 100));
  const p = I.findCentres(plateau, PRES, win, 10, OPTS(10));
  assert.deepEqual(p.map((x) => [x.type, x.r, x.c]), [['L', 150, 640]], 'a flat-bottomed low: one L, at its middle');
});

test('findCentres: promNodesMax coarsens the prominence blocks for a large window (the world view) by the layer\'s rule, and nothing else changes; the layer draws exactly findCentres at its own inputs (radius, stride, margin, terrain, the cap)', async () => {
  const { png } = realFrames();
  const fr = await I.decodePngGrey(png, { cols: 720, rows: 361 });
  const z = Math.log2(1920 / 256), n = 256 * Math.pow(2, z), view = { z, w: 1920, h: 1080, ox: (-170 + 180) / 360 * n - 960, oy: n / 2 - 540 };
  const rad = Math.round(I.ISO_HL_PX / (n * 0.5 / 360)), win = I.nodeWindow(view, HALF, fr, 0), mg = Math.max(2 * rad, 16);
  let st = I.isoStride(z, 0.5); const pw = (Math.min(win.r1, 360) - Math.max(win.r0, 0) + 1 + 2 * mg) * Math.min(720, win.count + 2 * mg);
  while (pw / (st * st) > I.ISO_PROM_NODES_MAX) st++;
  assert.ok(st > I.isoStride(z, 0.5), 'coarsened at the world view: stride ' + st);
  const a = I.findCentres(fr, PRES, win, rad, { stride: I.isoStride(z, 0.5), margin: mg, grid: HALF, promNodesMax: I.ISO_PROM_NODES_MAX }).map(key).sort();
  const b = I.findCentres(fr, PRES, win, rad, { stride: st, margin: mg, grid: HALF }).map(key).sort();
  assert.deepEqual(a, b, 'the cap = the coarser stride');
  assert.notDeepEqual(a, I.findCentres(fr, PRES, win, rad, { stride: I.isoStride(z, 0.5), margin: mg, grid: HALF }).map(key).sort(), 'the stride matters (else the test proves nothing)');
  // the layer: a recording canvas at a zoom-4 view with a terrain under one real centre -> its marks = findCentres at the layer's inputs
  const v0 = { z: 4, w: 1280, h: 800, ox: I.forwardPixel(40, -170, 4).x - 640, oy: I.forwardPixel(40, -170, 4).y - 400 }, rad0 = Math.round(I.ISO_HL_PX / (256 * 16 * 0.5 / 360));
  const cs0 = I.findCentres(fr, PRES, I.nodeWindow(v0, HALF, fr, 0), rad0, { stride: I.isoStride(4, 0.5), margin: Math.max(2 * rad0, 16), grid: HALF, promNodesMax: I.ISO_PROM_NODES_MAX });
  const terrain = new Uint8Array(720 * 361); terrain[Math.round(cs0[0].r) * 720 + Math.round(cs0[0].c)] = 30;   // 600 m under the first centre's middle
  const { il, ctx } = isoLayer({ w: 1280, h: 800 }, 4, 40, -170);
  il.setTerrain(terrain); il.setData(fr, HALF, { step: 0 }, PRES); il.pendingFn();
  const v2 = { z: 4, w: 1280, h: 800, ox: I.forwardPixel(40, -170, 4).x - 640, oy: I.forwardPixel(40, -170, 4).y - 400 };
  const rad2 = Math.round(I.ISO_HL_PX / (256 * 16 * 0.5 / 360));
  const want = I.projectCentres(I.findCentres(fr, PRES, I.nodeWindow(v2, HALF, fr, 0), rad2, { stride: I.isoStride(4, 0.5), margin: Math.max(2 * rad2, 16), grid: HALF, terrain, terrainMin: I.ISO_TERRAIN_M / I.TERRAIN_M_PER_CODE, promNodesMax: I.ISO_PROM_NODES_MAX }), HALF, v2);
  const drawn = ctx.texts.filter((t, k) => ctx.fonts[k] === I.ISO_STYLE.hlFont);
  assert.ok(want.length >= 3 && il.drawn.hl === want.length, `${il.drawn.hl} marks drawn, ${want.length} expected`);
  assert.deepEqual(drawn.sort(), want.map((m) => m.type).sort());
  const withoutTerrain = I.projectCentres(I.findCentres(fr, PRES, I.nodeWindow(v2, HALF, fr, 0), rad2, { stride: I.isoStride(4, 0.5), margin: Math.max(2 * rad2, 16), grid: HALF, promNodesMax: I.ISO_PROM_NODES_MAX }), HALF, v2);
  assert.equal(withoutTerrain.length, want.length + 1, 'the terrain removed that one (the layer passes it)');
  const rawDepth = I.projectCentres(I.findCentres(fr, PRES, I.nodeWindow(v2, HALF, fr, 0), rad2 + 3, { stride: I.isoStride(4, 0.5), margin: Math.max(2 * rad2, 16), grid: HALF, terrain, terrainMin: I.ISO_TERRAIN_M / I.TERRAIN_M_PER_CODE, promNodesMax: I.ISO_PROM_NODES_MAX }), HALF, v2);
  assert.notEqual(rawDepth.length, want.length, 'the radius is the layer\'s own (R1 H26)');
  // the layer ROUNDS ISO_HL_PX / nodePx (zoom 4.5 at 20 N 150 W: 13.66 -> 14, where 13 draws another set of marks)
  const z3 = 4.5, x3 = I.ISO_HL_PX / (256 * Math.pow(2, z3) * 0.5 / 360), rad3 = Math.round(x3);
  assert.notEqual(rad3, Math.floor(x3));
  const L3 = isoLayer({ w: 1280, h: 800 }, z3, 20, -150);
  L3.il.setData(fr, HALF, { step: 0 }, PRES); L3.il.pendingFn();
  const p3 = I.forwardPixel(20, -150, z3), v3 = { z: z3, w: 1280, h: 800, ox: p3.x - 640, oy: p3.y - 400 };
  const cs = (rad) => I.projectCentres(I.findCentres(fr, PRES, I.nodeWindow(v3, HALF, fr, 0), rad, { stride: I.isoStride(z3, 0.5), margin: Math.max(2 * rad, 16), grid: HALF, promNodesMax: I.ISO_PROM_NODES_MAX }), HALF, v3);
  assert.notEqual(cs(rad3).length, cs(rad3 - 1).length, '(the floor would draw another number of marks here)');
  assert.equal(L3.il.drawn.hl, cs(rad3).length, 'the layer rounds its radius');
});

test('the world\'s prominence blocks meet across the seam: a stride that does not divide the 720 columns is lowered (7 -> 6), so stride 7 = stride 6 at the world view of the real frame', async () => {
  const { png } = realFrames();
  const fr = await I.decodePngGrey(png, { cols: 720, rows: 361 });
  const win = { r0: 0, r1: 360, c0: 0, count: 720 };
  for (const rad of [3, 7]) {
    const r = (s) => I.findCentres(fr, PRES, win, rad, { stride: s, margin: Math.max(2 * rad, 16), grid: HALF }).map(key).sort();
    const a = r(7), b = r(6);
    assert.ok(a.length > 100, 'centres found: ' + a.length);
    assert.deepEqual(a, b, 'rad ' + rad + ': stride 7 is computed as 6 (blocks of 7 would lose ' + a.filter((k) => !b.includes(k)).length + ' and add ' + b.filter((k) => !a.includes(k)).length + ')');
  }
});

test('a broad flat high whose patch runs out of a small window keeps its mark: an equal neighbour counts against a candidate only inside the window (zoom 8-9: the patch\'s first node lay outside; fix round 3 test-site verification V3-3)', () => {
  // a 10 x 10 plateau at 1030 hPa (rows 100-109, columns 300-309), falling 2 hPa a node outside it: one H at its middle
  const fr = frameOf((r, c) => { const d = Math.max(Math.max(100 - r, r - 109, 0), Math.max(300 - c, c - 309, 0)); return 1030 - 2 * d; });
  const at = (win) => I.findCentres(fr, PRES, win, 1, OPTS(1)).map((x) => x.type + x.r + ',' + x.c);
  const whole = at({ r0: 80, r1: 130, c0: 280, count: 50 });
  assert.deepEqual(whole, ['H104.5,304.5'], 'the whole patch in the window');
  for (const win of [{ r0: 103, r1: 130, c0: 280, count: 50 }, { r0: 105, r1: 108, c0: 302, count: 5 }, { r0: 80, r1: 130, c0: 304, count: 30 }, { r0: 101, r1: 101, c0: 305, count: 1 }]) {
    assert.deepEqual(at(win), whole, 'a window that cuts the patch: the same mark ' + JSON.stringify(win));
    assert.deepEqual(bruteCentres(fr, win, 1, { margin: 16, stride: 1 }), bruteCentres(fr, { r0: 80, r1: 130, c0: 280, count: 50 }, 1, { margin: 16, stride: 1 }), 'the oracle agrees ' + JSON.stringify(win));
  }
});

test('the terrain test uses the patch\'s first node in raster order and its middle, never the candidate that found the patch (which depends on the window)', () => {
  const fr = frameOf((r, c) => { const d = Math.max(Math.max(100 - r, r - 109, 0), Math.max(300 - c, c - 309, 0)); return 1030 - 2 * d; });
  const cut = { r0: 103, r1: 130, c0: 280, count: 50 }, whole = { r0: 80, r1: 130, c0: 280, count: 50 };
  const tA = new Uint8Array(720 * 361); tA[103 * 720 + 300] = 30;            // high ground under the cut window's own candidate only
  const at = (win, terrain) => I.findCentres(fr, PRES, win, 1, OPTS(1, { terrain, terrainMin: 25 })).map((x) => x.type + x.r + ',' + x.c);
  assert.deepEqual(at(whole, tA), ['H104.5,304.5']); assert.deepEqual(at(cut, tA), ['H104.5,304.5'], 'the cut window marks it too');
  const tB = new Uint8Array(720 * 361); tB[100 * 720 + 300] = 30;            // high ground under the patch's first node
  assert.deepEqual(at(whole, tB), []); assert.deepEqual(at(cut, tB), [], 'the cut window drops it too');
  // the clamp's latitude limit likewise: a compact code-1 patch (3 x 3 nodes, rows 39-41 = 70.5-69.5 N) is poleward of
  // ISO_CLAMP_LAT by its first node (70.5 N): no mark through either window (a cut window's own candidate sits at 69.5 N)
  const clampFr = frameOf((r, c) => (r >= 39 && r <= 41 && c >= 100 && c <= 102 ? 900 : 940 + 4 * Math.max(Math.abs(r - 40), Math.abs(c - 101))));
  const ends = (win) => I.findCentres(clampFr, PRES, win, 1, OPTS(1)).filter((x) => x.clamp).map((x) => x.type + x.r + ',' + x.c);
  assert.equal(I.ISO_CLAMP_LAT, 70);
  assert.deepEqual(ends({ r0: 30, r1: 60, c0: 90, count: 30 }), [], 'the whole patch in view: poleward of 70 N');
  assert.deepEqual(ends({ r0: 41, r1: 60, c0: 90, count: 30 }), [], 'a window from 69.5 N: still none');
});

test('findCentres: no mark on high ground (the terrain codes at or above terrainMin, at the candidate or at the patch\'s middle); without a terrain, or one of another size, the marks stay', () => {
  const win = { r0: 100, r1: 200, c0: 560, count: 160 };
  const bump = codeFrame((r, c) => 120 + (Math.abs(r - 150) <= 1 && Math.abs(c - 640) <= 1 ? 8 : 0) + (Math.abs(r - 150) <= 1 && Math.abs(c - 600) <= 1 ? 8 : 0));
  const terrain = new Uint8Array(720 * 361); terrain[150 * 720 + 640] = 30;              // 600 m under the first bump
  assert.deepEqual(I.findCentres(bump, PRES, win, 10, OPTS(10, { terrain, terrainMin: 25 })).map((x) => [x.type, x.c]), [['H', 600]], 'the mountain high goes');
  assert.deepEqual(I.findCentres(bump, PRES, win, 10, OPTS(10, { terrain, terrainMin: 31 })).map((x) => [x.type, x.c]).sort((p, q) => p[1] - q[1]), [['H', 600], ['H', 640]], 'below the threshold: kept');
  assert.equal(I.findCentres(bump, PRES, win, 10, OPTS(10)).length, 2, 'no terrain: both');
  assert.deepEqual(I.findCentres(bump, PRES, win, 10, OPTS(10, { terrain, terrainMin: 30 })).map((x) => [x.type, x.c]), [['H', 600]], 'at the threshold exactly: high ground (the rule is "at or above")');
  assert.equal(I.findCentres(bump, PRES, win, 10, OPTS(10, { terrain: new Uint8Array(10), terrainMin: 25 })).length, 2, 'a terrain of another size is ignored');
  assert.equal(I.findCentres(bump, PRES, win, 10, OPTS(10, { terrain: new Uint8Array(720 * 361 + 1).fill(255), terrainMin: 25 })).length, 2, 'also one that would call every node high');
  const wide = codeFrame((r, c) => 120 + (Math.abs(r - 150) <= 1 && Math.abs(c - 640) <= 6 ? 8 : 0));     // a 3 x 13 flat top: the candidate at its left, the middle at 640
  const t2 = new Uint8Array(720 * 361); t2[150 * 720 + 640] = 30;
  assert.deepEqual(I.findCentres(wide, PRES, win, 10, OPTS(10, { terrain: t2, terrainMin: 25 })), [], 'high at the middle: no mark');
});

test('fix round 4 (owner, V3-1): over high ground only a STRONG LOW keeps its mark: at least strongLowHpa prominent and the ground at both nodes below terrainCap; a high never; the layer passes 15 hPa and 2,000 m', () => {
  assert.equal(I.ISO_STRONG_LOW_HPA, 15); assert.equal(I.ISO_TERRAIN_CAP_M, 2000);
  const k = (PRES.hi - PRES.lo) / 254, win = { r0: 100, r1: 200, c0: 560, count: 160 };
  const cone = (depth, sign) => codeFrame((r, c) => Math.max(1, Math.min(255, Math.round(1 + (1010 + sign * depth * Math.exp(-((r - 150) ** 2 + (c - 640) ** 2) / 30) - PRES.lo) / k))));
  const ground = (code) => { const t = new Uint8Array(720 * 361); for (let dr = -2; dr <= 2; dr++) for (let dc = -2; dc <= 2; dc++) t[(150 + dr) * 720 + 640 + dc] = code; return t; };
  const rule = (extra) => OPTS(10, Object.assign({ terrainMin: I.ISO_TERRAIN_M / I.TERRAIN_M_PER_CODE, strongLowHpa: I.ISO_STRONG_LOW_HPA, terrainCap: I.ISO_TERRAIN_CAP_M / I.TERRAIN_M_PER_CODE }, extra));
  const marks = (fr, extra) => I.findCentres(fr, PRES, win, 10, rule(extra)).map((x) => x.type + x.c);
  const deep = cone(40, -1);                                                     // a 40-hPa low (the typhoon over Taiwan: 31-48)
  assert.deepEqual(marks(deep, { terrain: ground(40) }), ['L640'], '800 m: marked');
  assert.deepEqual(marks(deep, { terrain: ground(99) }), ['L640'], '1,980 m: marked');
  assert.deepEqual(marks(deep, { terrain: ground(100) }), [], '2,000 m: not (the cap is "below")');
  assert.deepEqual(marks(deep, { terrain: ground(40), strongLowHpa: 0 }), [], 'without the exception: the 400-m rule as before');
  assert.deepEqual(marks(deep, { terrain: ground(40), terrainCap: 0 }), [], 'without a cap: none either');
  assert.deepEqual(marks(cone(40, 1), { terrain: ground(40) }), [], 'a high as strong: never');
  // beside a deeper low (60 nodes west) a low's prominence is its own depth (alone in its region it is never absorbed)
  const deeper = (r, c) => 150 - Math.round(80 * Math.exp(-((r - 150) ** 2 + (c - 580) ** 2) / 30));
  const pit = (codes) => codeFrame((r, c) => deeper(r, c) - (Math.abs(r - 150) <= 1 && Math.abs(c - 640) <= 1 ? codes : 0));
  assert.deepEqual(marks(pit(18), { terrain: ground(40) }), ['L580'], 'a 10-hPa low over 800 m (the mountain bumps measured at most 11-14 hPa, all highs): not');
  assert.deepEqual(marks(pit(18), { terrain: null }).sort(), ['L580', 'L640'], 'the same low over the sea: marked');
  // the boundary: prominence * k against 15 hPa, both sides of it
  const at = (codes) => pit(codes);
  const need = Math.ceil(15 / k - 1e-9);
  assert.deepEqual(marks(at(need), { terrain: ground(40) }).sort(), ['L580', 'L640'], `${need} codes (${(need * k).toFixed(2)} hPa): marked`);
  assert.deepEqual(marks(at(need - 1), { terrain: ground(40) }), ['L580'], `${need - 1} codes (${((need - 1) * k).toFixed(2)} hPa): not`);
  // both nodes: a patch whose first node lies under the cap and middle over it (or the reverse) marks nothing
  const flat = codeFrame((r, c) => 150 - (Math.abs(r - 150) <= 1 && Math.abs(c - 640) <= 6 ? 40 : 0));   // a 3 x 13 floor: first node (149, 634), middle (150, 640)
  const g2 = new Uint8Array(720 * 361).fill(40); g2[150 * 720 + 640] = 120;
  assert.deepEqual(marks(flat, { terrain: g2 }), [], 'the middle over the cap');
  const g3 = new Uint8Array(720 * 361).fill(40); g3[149 * 720 + 634] = 120;
  assert.deepEqual(marks(flat, { terrain: g3 }), [], 'the first node over the cap');
  assert.deepEqual(marks(flat, { terrain: new Uint8Array(720 * 361).fill(40) }), ['L640'], 'both under: marked');
});

test('findCentres: a trough with lows B, A (equal to B, a row above it) and C (deeper), 8 deg apart each: C and B get an L, A (beside the deeper C) none (G32 verification V11-2); two equal lows within the radius both come out, projectCentres keeps the more prominent (the earlier root)', () => {
  const dips = [[100, 340, 120], [99, 324, 121], [100, 308, 121]];      // C, A, B (codes; each the tip of a cone 6 nodes wide)
  const fr = codeFrame((r, c) => { let q = 170; for (const [a, b, v] of dips) { const d = Math.max(Math.abs(r - a), Math.abs(c - b)); if (d <= 6) q = Math.min(q, v + 3 * d); } return q; });
  const e = I.findCentres(fr, PRES, { r0: 80, r1: 120, c0: 280, count: 90 }, 19, OPTS(19));
  assert.deepEqual(e.map((x) => [x.type, x.r, x.c]).sort(), [['L', 100, 308], ['L', 100, 340]]);
  assert.ok(near(e.find((x) => x.c === 308).v, PRES.lo + 120 * (PRES.hi - PRES.lo) / 254, 1e-9), 'B with its own pressure');
  assert.equal(e.find((x) => x.c === 308).prom, 170 - 121, 'B: up the cone to the plateau, then down to C');
  const pair = codeFrame((r, c) => (r === 150 && (c === 714 || c === 3) ? 80 : 100));        // two equal lows 9 columns apart across the date line
  const both = I.findCentres(pair, PRES, { r0: 140, r1: 160, c0: 700, count: 30 }, 10, OPTS(10));
  assert.deepEqual(both.map((x) => [x.type, x.r, x.c, x.prom]).sort((p, q) => p[2] - q[2]), [['L', 150, 3, I.ISO_HL_PROM_CAP], ['L', 150, 714, 20]], 'both; the earlier in the grid\'s raster order (column 3) is the stronger, though the window starts at 700');
  const other = I.findCentres(pair, PRES, { r0: 140, r1: 160, c0: 710, count: 30 }, 10, OPTS(10));
  assert.deepEqual(other.map((x) => [x.c, x.prom]).sort((p, q) => p[0] - q[0]), [[3, I.ISO_HL_PROM_CAP], [714, 20]], 'the same from a window starting at 710');
  const view = { z: 5, w: 1200, h: 800, ox: 0, oy: 0 }, n = 256 * 32;
  const pc = I.forwardPixel(15, -180, 5); view.ox = pc.x - 600; view.oy = pc.y - 400;
  const marks = I.projectCentres(both, HALF, view);
  assert.deepEqual(marks.map((m) => [m.type, m.text]), [['L', '964']], 'one mark: 9 nodes apart is under ISO_HL_PX at zoom 5');
  assert.ok(near(marks[0].x, 3 * 0.5 / 360 * n - view.ox, 1e-6), 'the more prominent one (column 3, 1.5 deg east of the date line)');
});

test('projectCentres: one mark per ISO_HL_PX among the DRAWN marks, the most prominent first, then the deeper; an H whose box overlaps a kept L goes; a twin beyond the view edge no longer hides the one in view (re-check H-7); "<=920" / ">=1060" texts', () => {
  const view = { z: 6, w: 1200, h: 800, ox: 0, oy: 0 }, pc = I.forwardPixel(30, -150, 6); view.ox = pc.x - 600; view.oy = pc.y - 400;
  const nodePx = 256 * 64 * 0.5 / 360;                                   // 22.8 px per half node
  const at = (lat, lng) => ({ r: (90 - lat) / 0.5, c: (lng + 180) / 0.5 });
  const L = (lat, lng, q, prom) => Object.assign({ type: 'L', v: PRES.lo + (q - 1) * (PRES.hi - PRES.lo) / 254, q, prom, clamp: 0 }, at(lat, lng));
  const H = (lat, lng, q, prom) => Object.assign({ type: 'H', v: PRES.lo + (q - 1) * (PRES.hi - PRES.lo) / 254, q, prom, clamp: 0 }, at(lat, lng));
  const dx = 100 / nodePx * 0.5;                                          // 100 px east in degrees
  let m = I.projectCentres([L(30, -150, 100, 5), L(30, -150 + dx, 100, 9)], HALF, view);
  assert.deepEqual(m.map((x) => [x.type, Math.round(x.x)]), [['L', Math.round(600 + 100)]], 'two equal lows 100 px apart: the more prominent');
  m = I.projectCentres([L(30, -150, 100, 5), L(30, -150 + dx, 98, 5)], HALF, view);
  assert.deepEqual(m.map((x) => [x.type, Math.round(x.x)]), [['L', 700]], 'equal prominence: the deeper');
  m = I.projectCentres([L(30, -150, 100, 5), L(30, -150 + dx * 1.3, 100, 9)], HALF, view);
  assert.equal(m.length, 2, '130 px apart: both');
  m = I.projectCentres([L(30, -150, 100, 5), H(30, -150 + 20 / nodePx * 0.5, 180, 9)], HALF, view);
  assert.deepEqual(m.map((x) => x.type), ['H'], 'an L and an H 20 px apart (overlapping boxes): the more prominent');
  m = I.projectCentres([L(30, -150, 100, 5), H(30, -150 + 40 / nodePx * 0.5, 180, 9)], HALF, view);
  assert.equal(m.length, 2, '40 px apart: both');
  // a twin 10 px beyond the view's edge (not drawn) and its equal within 110 px inside the view: the inside one is drawn
  const edgeLng = -150 + (600 - 10) / nodePx * 0.5, inLng = edgeLng - 100 / nodePx * 0.5;
  m = I.projectCentres([L(30, edgeLng, 100, 9), L(30, inLng, 100, 5)], HALF, view);
  assert.deepEqual(m.map((x) => [x.type, Math.round(x.x)]), [['L', Math.round(600 + (inLng + 150) / 0.5 * nodePx)]], 'the one in view');
  assert.deepEqual(I.projectCentres([Object.assign(L(30, -150, 1, 255), { clamp: -1, v: PRES.lo }), Object.assign(H(28, -150, 255, 255), { clamp: 1, v: PRES.hi })], HALF, view).map((x) => x.text).sort(), ['\u2264920', '\u22651060']);
  assert.deepEqual(I.projectCentres([{ type: 'H', r: 0, c: 0, v: 1030 }], HALF, view).length, 0, 'beyond the view: nothing');
  assert.deepEqual(I.projectCentres([{ type: 'H', r: at(30, -150).r, c: at(30, -150).c, v: 1030.4 }], HALF, view).map((x) => x.text), ['1030'], 'a centre without q / prom (older callers): drawn, rounded');
});

test('findCentres: a flat floor that runs down to a lower node is a shelf (no mark); a patch takes one mark however many of its nodes qualify; a patch over ISO_PATCH_MAX nodes or spanning half the world marks nothing', () => {
  const floor = codeFrame((r, c) => (r === 150 && c >= 600 && c <= 630 ? 100 : r === 150 && c === 631 ? 99 : 130));
  assert.deepEqual(I.findCentres(floor, PRES, { r0: 140, r1: 160, c0: 590, count: 50 }, 10, OPTS(10)).map((x) => [x.type, x.r, x.c]), [['L', 150, 631]], 'the low end only');
  const vee = codeFrame((r, c) => ((r === 150 && (c === 640 || c === 642)) || (r === 151 && c === 641) ? 80 : 100));   // two nodes qualify
  const one = I.findCentres(vee, PRES, { r0: 140, r1: 160, c0: 630, count: 20 }, 1, OPTS(1));
  assert.equal(one.length, 1, 'one mark for the patch');
  assert.ok(near(one[0].r, 451 / 3, 1e-12) && one[0].c === 641, 'at its middle: ' + JSON.stringify(one[0]));
  const square = (side) => codeFrame((r, c) => (r >= 100 && r < 100 + side && c >= 300 && c < 300 + side ? 80 : 100));
  const s60 = I.findCentres(square(60), PRES, { r0: 90, r1: 170, c0: 290, count: 80 }, 5, OPTS(5));
  assert.deepEqual(s60.map((x) => [x.type, x.r, x.c]), [['L', 129.5, 329.5]], '3,600 nodes: one L at the middle');
  assert.equal(64 * 64, I.ISO_PATCH_MAX);
  assert.deepEqual(I.findCentres(square(64), PRES, { r0: 90, r1: 175, c0: 290, count: 90 }, 5, OPTS(5)).map((x) => [x.type, x.r, x.c]), [['L', 131.5, 331.5]], 'ISO_PATCH_MAX nodes: still one');
  assert.deepEqual(I.findCentres(square(70), PRES, { r0: 90, r1: 180, c0: 290, count: 90 }, 5, OPTS(5)), [], '4,900 nodes: none');
  const band = (len) => codeFrame((r, c) => (r === 150 && c >= 100 && c < 100 + len ? 80 : 100));
  assert.deepEqual(I.findCentres(band(300), PRES, { r0: 140, r1: 160, c0: 90, count: 40 }, 5, OPTS(5)).map((x) => [x.type, x.r, x.c]), [['L', 150, 249.5]], 'a band 150 deg long');
  assert.deepEqual(I.findCentres(band(400), PRES, { r0: 140, r1: 160, c0: 90, count: 40 }, 5, OPTS(5)), [], 'a band 200 deg long');
  assert.equal(I.findCentres(band(360), PRES, { r0: 140, r1: 160, c0: 90, count: 40 }, 5, OPTS(5)).length, 1, '360 nodes: spans 359 columns');
  assert.deepEqual(I.findCentres(band(361), PRES, { r0: 140, r1: 160, c0: 90, count: 40 }, 5, OPTS(5)), [], '361 nodes: spans half the columns');
});

test('findCentres: a centre near the grid\'s end counts (the square is cut by the grid, not the window); one on the date line of a whole-world window is found once; projectCentres draws it in every world copy', () => {
  const south = codeFrame((r, c) => Math.round(1 + (1010 - 60 * Math.exp(-((r - 340) ** 2 + (c - 200) ** 2) / 80) - PRES.lo) / ((PRES.hi - PRES.lo) / 254)));
  const s = I.findCentres(south, PRES, { r0: 300, r1: 345, c0: 150, count: 100 }, 40, OPTS(40));
  assert.deepEqual(s.map((x) => [x.type, x.r, x.c]), [['L', 340, 200]], '80 S, 40 nodes from the grid\'s end: still a centre');
  const fr = codeFrame((r, c) => { const dc = Math.min(c, 720 - c); return 150 - Math.floor(40 * Math.exp(-((r - 120) ** 2 + dc ** 2) / 200)); });   // a unique lowest node at 30 N, 180
  const view = { z: 1.5, w: 2000, h: 700, ox: -300, oy: 0 }, n = 256 * Math.pow(2, 1.5);
  const e = I.findCentres(fr, PRES, I.nodeWindow(view, HALF, fr, 0), 40, OPTS(40, { stride: 3 }));
  assert.equal(e.length, 1); assert.equal(e[0].type, 'L'); assert.deepEqual([e[0].r, e[0].c], [120, 0]);
  const marks = I.projectCentres(e, HALF, view);
  assert.ok(marks.length >= 2, `${marks.length} copies in view`);
  for (let k = 1; k < marks.length; k++) assert.ok(near(marks[k].x - marks[k - 1].x, n, 1e-6));
  assert.ok(marks.every((m) => m.text === String(Math.round(e[0].v)) && m.y === marks[0].y && m.x >= I.ISO_HL_EDGE_PX && m.x <= view.w - I.ISO_HL_EDGE_PX));
  const pc = I.forwardPixel(30, -180, 1.5);
  assert.ok(near(marks[0].y, pc.y - view.oy, 1e-9), 'at 30 N'); assert.ok(near(((marks[0].x + view.ox - pc.x) % n + n) % n, 0, 1e-6), 'at 180');
});

test('on the real GFS frame the world view marks the deep centres with their own pressure (a 956-hPa typhoon, a 946-hPa Southern Ocean low) = the brute force at the layer\'s own inputs', async () => {
  const { png } = realFrames();
  const fr = await I.decodePngGrey(png, { cols: 720, rows: 361 });
  const z = Math.log2(1920 / 256), n = 256 * Math.pow(2, z), view = { z, w: 1920, h: 1080, ox: 0, oy: n / 2 - 540 };
  view.ox = (-170 + 180) / 360 * n - 960;
  const rad = Math.round(I.ISO_HL_PX / (n * 0.5 / 360)), o = { stride: I.isoStride(z, 0.5), margin: Math.max(2 * rad, 16), grid: HALF };
  assert.equal(o.stride, 3);
  const e = I.findCentres(fr, PRES, I.nodeWindow(view, HALF, fr, 0), rad, o);
  const at = (type, lat, lng) => e.find((x) => x.type === type && Math.abs(90 - x.r * 0.5 - lat) <= 1 && Math.abs(((-180 + x.c * 0.5 - lng + 540) % 360) - 180) <= 1);
  const ty = at('L', 13.5, 142), so = at('L', -54, 77);
  assert.ok(ty && Math.round(ty.v) === 956, 'the typhoon: ' + JSON.stringify(ty));
  assert.ok(so && Math.round(so.v) === 946, 'the Southern Ocean low: ' + JSON.stringify(so));
  assert.deepEqual(e.map(key).sort(), bruteCentres(fr, I.nodeWindow(view, HALF, fr, 0), rad, o), 'every mark = the brute force');
  assert.ok(e.every((x) => x.prom >= I.ISO_HL_PROM_CODES), 'every mark at least ISO_HL_PROM_CODES prominent');
});

// ---- the canvas layer ----
function fakeCtx() {
  const c = { ops: [], widths: [], cleared: [], texts: [], fonts: [], fills: [] };
  for (const m of ['beginPath', 'moveTo', 'lineTo', 'quadraticCurveTo', 'closePath', 'setTransform', 'translate', 'rotate']) c[m] = function () { c.ops.push(m); };
  const stack = [];                                           // save / restore keep the composite mode, as a real context does
  c.save = function () { c.ops.push('save'); stack.push(c.globalCompositeOperation); };
  c.restore = function () { c.ops.push('restore'); c.globalCompositeOperation = stack.pop(); };
  c.fill = function () { c.ops.push('fill'); c.fills.push(c.globalCompositeOperation || 'source-over'); };
  c.fillText = function (t) { c.ops.push('fillText'); c.texts.push(String(t)); c.fonts.push(c.font); };
  c.stroke = function () { c.ops.push('stroke'); c.widths.push(c.lineWidth); };
  c.clearRect = function (x, y, w, h) { c.ops.push('clearRect'); c.cleared.push([x, y, w, h]); };
  c.measureText = (t) => ({ width: 6 * t.length });
  return c;
}
function isoLayer(view, zoom, lat, lng) {
  const ctx = fakeCtx(), events = {}, v = view || { w: 1200, h: 800 }, z = zoom === undefined ? 5 : zoom;
  const pc = I.forwardPixel(lat === undefined ? 30 : lat, lng === undefined ? -150 : lng, z);
  const map = { on(ev, fn) { (events[ev] = events[ev] || []).push(fn); }, off(ev, fn) { events[ev] = (events[ev] || []).filter((f) => f !== fn); },
    getPane: () => null, createPane: () => ({ style: {}, appendChild() {} }), getSize: () => ({ x: v.w, y: v.h + 300 }), getContainer: () => ({ clientWidth: v.w, clientHeight: v.h }),
    getPixelBounds: () => ({ min: { x: pc.x - v.w / 2, y: pc.y - v.h / 2 } }), getZoom: () => z, containerPointToLayerPoint: () => ({ x: -3, y: 7 }) };
  const canvas = { style: {}, width: 0, height: 0, getContext: () => ctx, remove() { canvas.removed = true; } };
  global.document = { createElement: () => canvas, hidden: false };
  const il = new I.IsobarLayer(map);
  il.scheduled = 0; il.raf = (fn) => { il.scheduled++; il.pendingFn = fn; return 7; }; il.caf = () => { il.pendingFn = null; il.cancelled = (il.cancelled || 0) + 1; };
  il.attach();
  return { il, ctx, canvas, events, map };
}
const lowAt = (lat, lng, depth) => frameOf((r, c) => { const la = 90 - r * 0.5, lo = -180 + c * 0.5; return 1012 - depth * Math.exp(-((la - lat) ** 2 + (lo - lng) ** 2) / 40); });

test('the layer draws on the next animation frame: halos then cores in two strokes, one pill per line, an L at the low', () => {
  const { il, ctx, canvas } = isoLayer();
  assert.equal(il.scheduled, 0, 'nothing to draw before a frame');
  const fr = lowAt(30, -150, 20), entry = { step: 0 };
  il.setData(fr, HALF, entry, PRES);
  assert.equal(il.scheduled, 1); assert.equal(ctx.ops.length, 0, 'never drawn on the stack of setData');
  il.setData(fr, HALF, entry, PRES); il.setData(fr, HALF, entry, PRES);
  assert.equal(il.scheduled, 1, 'the same frame again: nothing new');
  il.setData(lowAt(30, -150, 12), HALF, { step: 1 }, PRES); il.setData(fr, HALF, { step: 2 }, PRES);
  assert.equal(il.scheduled, 1, 'new frames before the animation frame: still one draw');
  il.pendingFn();
  assert.equal(canvas.width, 1200); assert.equal(canvas.style.transform, 'translate3d(-3px,7px,0)');
  assert.deepEqual(ctx.widths, [I.ISO_STYLE.haloW, I.ISO_STYLE.coreW], 'every halo in one stroke, then every core in one');
  assert.ok(il.drawn.lines >= 3, `${il.drawn.lines} lines`);
  assert.equal(ctx.cleared.length, 1, 'the canvas cleared once (no cut under the pills)');
  assert.equal(il.drawn.hl, 1, 'one mark: the low');
  const pillTexts = ctx.texts.filter((t, k) => ctx.fonts[k] === I.ISO_STYLE.font);
  assert.equal(pillTexts.length, il.drawn.labels); assert.ok(pillTexts.length >= 1);
  assert.equal(new Set(pillTexts).size, pillTexts.length, 'one pill per isobar');
  assert.ok(pillTexts.every((t) => +t % 4 === 0), 'every 4 hPa at zoom 5');
  const markTexts = ctx.texts.filter((t, k) => ctx.fonts[k] !== I.ISO_STYLE.font);
  assert.equal(markTexts[0], 'L');
  assert.ok(/^99\d$/.test(markTexts[1]), `the low's pressure under its L (${markTexts[1]})`);
  assert.ok(il.lastMs >= 0);
});

test('the layer: no pills below zoom 4 or while the whole world is in view; 8 hPa there', () => {
  const z3 = isoLayer({ w: 1200, h: 800 }, 3.5);
  z3.il.setData(lowAt(30, -150, 30), HALF, { step: 0 }, PRES); z3.il.pendingFn();
  assert.ok(z3.il.drawn.lines >= 2); assert.equal(z3.il.drawn.labels, 0, 'zoom 3.5: no pills');
  const world = isoLayer({ w: 1920, h: 1080 }, Math.log2(1920 / 256), 20, -150);
  world.il.setData(lowAt(30, -150, 30), HALF, { step: 0 }, PRES); world.il.pendingFn();
  assert.equal(world.il.drawn.labels, 0, 'the whole world: no pills');
  assert.ok(world.il.drawn.hl >= 1, 'H / L still shown');
  const wide = isoLayer({ w: 3500, h: 900 }, 4);                         // zoom 4 but 308 deg across (a very wide screen): the whole world
  wide.il.setData(lowAt(30, -150, 30), HALF, { step: 0 }, PRES); wide.il.pendingFn();
  assert.equal(wide.il.drawn.labels, 0, 'the whole world at zoom 4: no pills'); assert.ok(wide.il.drawn.lines >= 2);
  const z45 = isoLayer({ w: 1200, h: 800 }, 4.5);
  z45.il.setData(lowAt(30, -150, 30), HALF, { step: 0 }, PRES); z45.il.pendingFn();
  assert.ok(z45.il.drawn.labels >= 1, 'zoom 4.5: pills');
});

test('clearData clears at once and drops a pending draw; zoomstart hides the canvas until zoomend; detach leaves nothing', () => {
  const { il, ctx, canvas, events } = isoLayer();
  il.setData(lowAt(30, -150, 20), HALF, { step: 0 }, PRES); il.pendingFn();
  const n = ctx.cleared.length;
  il.setData(lowAt(30, -150, 10), HALF, { step: 1 }, PRES);
  il.clearData();
  assert.equal(il.cancelled, 1, 'the pending draw is cancelled'); assert.equal(ctx.cleared.length, n + 1, 'cleared at once');
  assert.deepEqual(il.drawn, { lines: 0, labels: 0, hl: 0 });
  assert.equal(il.frame, null); assert.equal(il.valueAt(30, -150), null);
  events.zoomstart.forEach((f) => f()); assert.equal(canvas.style.visibility, 'hidden');
  events.zoomend.forEach((f) => f()); assert.equal(canvas.style.visibility, '');
  il.detach();
  assert.equal(canvas.removed, true); assert.equal(il.canvas, null);
  for (const ev of Object.keys(events)) assert.equal(events[ev].length, 0, `${ev} listener removed`);
});

test('valueAt reads the shown frame bilinearly (the readout\'s "· 1004 hPa"), null outside the grid', () => {
  const { il } = isoLayer();
  const fr = frameOf((r, c) => 960 + r * 0.6 + c * 0.01);           // more than a code a row: a row off would show
  il.setData(fr, HALF, { step: 0 }, PRES);
  const out = new Float64Array(1), cp = new Float64Array([(-150.25 + 180) / 0.5]);
  I.sampleRow(fr, false, (90 - 30.25) / 0.5, cp, 1, out, 0);
  assert.equal(il.valueAt(30.25, -150.25), PRES.lo + (out[0] - 1) / 254 * (PRES.hi - PRES.lo));
  assert.ok(near(il.valueAt(30.25, -150.25), 960 + 119.5 * 0.6 + 59.5 * 0.01, 0.3));
  assert.ok(near(il.valueAt(30.25, 209.75), il.valueAt(30.25, -150.25), 1e-9), 'longitude wraps');
  assert.equal(il.valueAt(91, 0), null);
});

test('FlowAnimator and the isobar layer share one placement (viewOf / placeCanvas)', () => {
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  assert.ok(/FlowAnimator\.prototype\.view = function \(\) \{ return viewOf\(this\.map, this\.layer\); \};/.test(src));
  assert.ok(/FlowAnimator\.prototype\._place = function \(v\) \{ this\.dpr = placeCanvas\(this\.canvas, this\.ctx, this\.map, v\); \};/.test(src));
  assert.ok(/pane\.style\.zIndex = 280;/.test(src), 'the isobar pane sits between the field (250) and the particles (300)');
});

// ---- G32 test gaps (reviewer A's surviving mutants T9, L1, L6, L7) ----
test('projectIsolines: a point between two node rows is placed between their projected rows (Mercator rows, not snapped to a node)', () => {
  const view = { z: 5, w: 1200, h: 800, ox: 0, oy: 0 };
  const g = { R: 3, C: 3, r0: 100, c0: 360, stride: 1 };
  const out = I.projectIsolines([{ level: 1000, pts: Float64Array.from([0, 0.5, 1, 0.25]), closed: false }], g, HALF, view);
  const Y = (r) => I.forwardPixel(90 - r * 0.5, 0, 5).y;
  assert.ok(near(out[0].xy[1], (Y(100) + Y(101)) / 2, 1e-9), 'halfway between rows 100 and 101');
  assert.ok(near(out[0].xy[3], Y(100) + (Y(101) - Y(100)) * 0.25, 1e-9));
  assert.ok(Math.abs(out[0].xy[1] - Y(100)) > 1, 'not snapped');
});

test('the layer: the same frame again after a draw schedules nothing; the canvas resolution is capped at 2 device pixels; valueAt is null below the grid', () => {
  const { il, canvas } = isoLayer();
  const fr = lowAt(30, -150, 20), entry = { step: 0 };
  il.setData(fr, HALF, entry, PRES); il.pendingFn(); const n = il.scheduled;
  il.setData(fr, HALF, entry, PRES);
  assert.equal(il.scheduled, n, 'no redraw for the frame already drawn');
  const dpr0 = global.devicePixelRatio;
  try {
    global.devicePixelRatio = 3; il.setData(fr, HALF, { step: 1 }, PRES); il.pendingFn();
    assert.equal(canvas.width, 2400, 'a 3x screen draws at 2x'); assert.equal(il.dpr, 2);
  } finally { global.devicePixelRatio = dpr0; }
  assert.equal(il.valueAt(-91, -150), null); assert.equal(il.valueAt(-90, -150) === null, false, 'the last row is in');
});

test('the layer (fix round 3): the terrain hides a mark on high ground; setTerrain with a frame on redraws; the lines are cleared under each mark before its text (destination-out)', () => {
  const { il, ctx } = isoLayer();
  il.setData(lowAt(30, -150, 20), HALF, { step: 0 }, PRES); il.pendingFn();
  assert.equal(il.drawn.hl, 1, 'the L');
  const di = ctx.ops.indexOf('fill', ctx.ops.lastIndexOf('stroke')), ti = ctx.ops.indexOf('fillText', ctx.ops.indexOf('fillText') + 0);
  assert.ok(ctx.fills.includes('destination-out'), 'a clearing fill');
  const clearAt = ctx.fills.indexOf('destination-out'), nFillsBefore = ctx.fills.length;
  assert.ok(clearAt >= 0 && ctx.texts.indexOf('L') >= 0, 'cleared, then the mark');
  assert.ok(di >= 0 && ti >= 0, 'fills after the strokes');
  // the terrain: high ground under the low's node -> no mark; the pills and lines stay
  const q = new Uint8Array(720 * 361), r = Math.round((90 - 30) / 0.5), c = Math.round((-150 + 180) / 0.5);
  for (let dr = -1; dr <= 1; dr++) for (let dc = -1; dc <= 1; dc++) q[(r + dr) * 720 + c + dc] = 40;    // 800 m
  il.setTerrain(q); il.pendingFn();
  assert.equal(il.drawn.hl, 1, 'a 20-hPa low over 800 m keeps its L (fix round 4: the layer passes the strong-low rule)');
  const q2 = new Uint8Array(720 * 361);
  for (let dr = -1; dr <= 1; dr++) for (let dc = -1; dc <= 1; dc++) q2[(r + dr) * 720 + c + dc] = 110;  // 2,200 m: over the cap
  const n0 = il.scheduled, nFills2 = ctx.fills.length;
  il.setTerrain(q2); assert.equal(il.scheduled, n0 + 1, 'a redraw scheduled'); il.pendingFn();
  assert.equal(il.drawn.hl, 0, 'no mark on high ground'); assert.ok(il.drawn.lines > 0);
  assert.ok(nFillsBefore > 0);
  assert.ok(!ctx.fills.slice(nFills2).includes('destination-out'), 'nothing cleared without a mark');
  il.setTerrain(q2); assert.equal(il.scheduled, n0 + 1, 'the same terrain again: no redraw');
  il.setTerrain(new Uint8Array(5)); assert.equal(il.terrain, null, 'another size: none');
  il.pendingFn(); assert.equal(il.drawn.hl, 1, 'the mark is back');
});

// An independent oracle for isoValues: the documented block means (an even stride's end rows / columns count half) and
// ISO_BLUR passes of [1, 2, 1] across then down, missing nodes left out (G32 re-check R1 test gaps L3, L5, L6, L12).
function refValues(fr, win, st) {
  const cols = fr.cols, rows = fr.rows, q = fr.q, wrap = win.count >= cols;
  let s = st; if (wrap) while (cols % s) s++;
  const c0 = wrap ? 0 : win.c0, count = wrap ? cols + 1 : win.count;
  const R = Math.floor((win.r1 - win.r0) / s) + 1, C = wrap ? cols / s + 1 : Math.floor((count - 1) / s) + 1;
  const h = Math.floor(s / 2), even = s % 2 === 0, k = (PRES.hi - PRES.lo) / 254;
  let V = [];
  for (let i = 0; i < R; i++) for (let j = 0; j < C; j++) {
    let acc = 0, n = 0; const rc = win.r0 + i * s, cc = c0 + j * s;
    for (let di = -h; di <= h; di++) for (let dj = -h; dj <= h; dj++) {
      const rr = rc + di; if (rr < 0 || rr >= rows) continue;
      const x = q[rr * cols + (((cc + dj) % cols) + cols) % cols]; if (!x) continue;
      const w = (even && Math.abs(di) === h ? 0.5 : 1) * (even && Math.abs(dj) === h ? 0.5 : 1);
      acc += x * w; n += w;
    }
    V.push(n ? PRES.lo + (acc / n - 1) * k : NaN);
  }
  const m = wrap ? C - 1 : C;
  for (let p = 0; p < I.ISO_BLUR; p++) {
    const T = V.slice();
    for (let i = 0; i < R; i++) for (let j = 0; j < m; j++) {
      const c = V[i * C + j]; if (Number.isNaN(c)) { T[i * C + j] = NaN; continue; }
      let acc = 2 * c, w = 2;
      for (const d of [-1, 1]) { let jj = j + d; if (wrap) jj = (jj + m) % m; if (jj < 0 || jj >= m) continue; const x = V[i * C + jj]; if (!Number.isNaN(x)) { acc += x; w++; } }
      T[i * C + j] = acc / w;
    }
    if (wrap) for (let i = 0; i < R; i++) T[i * C + C - 1] = T[i * C];
    const U = T.slice();
    for (let i = 0; i < R; i++) for (let j = 0; j < C; j++) {
      const v = T[i * C + j]; if (Number.isNaN(v)) { U[i * C + j] = NaN; continue; }
      let acc = 2 * v, w = 2;
      for (const d of [-1, 1]) { const ii = i + d; if (ii < 0 || ii >= R) continue; const y = T[ii * C + j]; if (!Number.isNaN(y)) { acc += y; w++; } }
      U[i * C + j] = acc / w;
    }
    V = U;
  }
  if (wrap) for (let i = 0; i < R; i++) V[i * C + C - 1] = V[i * C];
  return { V, R, C };
}

test('pins (G32 re-check R1): isoValues = the oracle on a curved field with holes (even strides\' half end weights, the blur\'s last row and both passes, the seam) also after a smaller window on the same scratch; marks keep ISO_HL_EDGE_PX from the top and bottom; the mark text rounds', () => {
  const fr = frameOf((r, c) => 1000 + 0.004 * (r - 150) * (r - 150) + 0.003 * (((c + 200) % 720) - 300) * (((c + 200) % 720) - 300) + 2 * Math.sin(r * 0.7) * Math.cos(c * 0.9));
  for (let hh = 0; hh < 300; hh++) fr.q[((hh * 7919) % 361) * 720 + ((hh * 104729) % 720)] = 0;    // holes
  const cases = [[{ r0: 120, r1: 180, c0: 280, count: 60 }, 1], [{ r0: 120, r1: 181, c0: 280, count: 61 }, 2], [{ r0: 100, r1: 200, c0: 700, count: 50 }, 2],
    [{ r0: 120, r1: 180, c0: 280, count: 60 }, 3], [{ r0: 0, r1: 360, c0: 0, count: 720 }, 3], [{ r0: 0, r1: 360, c0: 0, count: 720 }, 4], [{ r0: 0, r1: 360, c0: 0, count: 720 }, 7], [{ r0: 350, r1: 360, c0: 10, count: 30 }, 2]];
  let checked = 0;
  for (const [win, st] of cases) {
    const g = I.isoValues(fr, PRES, win, st, null), ref = refValues(fr, win, st);
    assert.equal(g.R, ref.R); assert.equal(g.C, ref.C);
    for (let i = 0; i < ref.V.length; i++) {
      const a = g.V[i], b = ref.V[i];
      if (Number.isNaN(b)) assert.ok(Number.isNaN(a), `${JSON.stringify(win)} st ${st} node ${i}: missing`);
      else { assert.ok(near(a, b, 1e-9), `${JSON.stringify(win)} st ${st} node ${i}: ${a} vs ${b}`); checked++; }
    }
  }
  assert.ok(checked > 50000, checked + ' nodes');
  // L12: a larger window after a smaller one on the same scratch
  const small = I.isoValues(fr, PRES, { r0: 150, r1: 160, c0: 300, count: 10 }, 1, null);
  const big = I.isoValues(fr, PRES, { r0: 100, r1: 200, c0: 280, count: 120 }, 1, small.scratch), fresh = I.isoValues(fr, PRES, { r0: 100, r1: 200, c0: 280, count: 120 }, 1, null);
  for (let i = 0; i < big.R * big.C; i++) assert.ok((Number.isNaN(big.V[i]) && Number.isNaN(fresh.V[i])) || near(big.V[i], fresh.V[i], 1e-12), 'scratch reuse node ' + i);
  // H22: the edge margin at the top and the bottom; H24: the text rounds half up
  const lng = -150, pc = I.forwardPixel(30, lng, 6), c = (lng + 180) / 0.5, r = (90 - 30) / 0.5;
  const mk = (oy, v) => I.projectCentres([{ type: 'L', r, c, v, q: 100, prom: 9, clamp: 0 }], HALF, { z: 6, w: 1200, h: 800, ox: pc.x - 600, oy });
  assert.equal(mk(pc.y - (I.ISO_HL_EDGE_PX - 1), 1004).length, 0, '13 px from the top: no mark');
  assert.equal(mk(pc.y - I.ISO_HL_EDGE_PX, 1004).length, 1, '14 px: drawn');
  assert.equal(mk(pc.y - 800 + I.ISO_HL_EDGE_PX - 1, 1004).length, 0, '13 px from the bottom: none');
  assert.equal(mk(pc.y - 800 + I.ISO_HL_EDGE_PX, 1004).length, 1, '14 px from the bottom: drawn');
  assert.deepEqual([mk(pc.y - 400, 1004.5)[0].text, mk(pc.y - 400, 1004.49)[0].text, mk(pc.y - 400, 1003.5)[0].text], ['1005', '1004', '1004'], 'rounded to the hPa');
});

test('the layer: a low 80 px inside the view\'s edge still gets its L (the traced window reaches past the view by the H / L radius)', () => {
  const pxPerDeg = 256 * 32 / 360, lng = -150 - (600 - 80) / pxPerDeg;
  const { il, ctx } = isoLayer();
  il.setData(lowAt(30, lng, 20), HALF, { step: 0 }, PRES); il.pendingFn();
  assert.equal(il.drawn.hl, 1, 'the L near the edge');
  assert.ok(ctx.texts.includes('L'));
});
