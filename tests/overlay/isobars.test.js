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
  const span = view.w / n * 360, mg = Math.min(0.5, (I.ISO_HL_PX + 8) / 1080);
  let best = Infinity, lines = 0, scratch = null;
  for (let k = 0; k < 5; k++) {
    const t0 = performance.now();
    const g = I.isoValues(fr, PRES, I.nodeWindow(view, HALF, fr, mg), I.isoStride(view.z, 0.5), scratch); scratch = g.scratch;
    const paths = I.projectIsolines(I.traceIsolines(g.V, g.R, g.C, I.isoInterval(view.z, span)), g, HALF, view);
    I.projectExtrema(I.findExtrema(g, Math.round(I.ISO_HL_PX / (n * 0.5 / 360 * g.stride)), PRES.lo, PRES.hi, 2), g, HALF, view);
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

// ---- highs and lows ----
test('findExtrema: one L at a low, one H at a high, nothing on a slope or in a dimple shallower than the depth; window edges and clamped values skipped', () => {
  const R = 41, C = 61, mk = (fn) => ({ V: grid(R, C, fn), R, C, wrap: false, r0: 0, c0: 0, stride: 1 });
  const lowHigh = mk((i, j) => 1012 - 18 * Math.exp(-((i - 20) ** 2 + (j - 15) ** 2) / 30) + 14 * Math.exp(-((i - 20) ** 2 + (j - 45) ** 2) / 40));
  const e = I.findExtrema(lowHigh, 8, PRES.lo, PRES.hi, 1);
  assert.deepEqual(e.map((x) => [x.type, x.i, x.j]), [['L', 20, 15], ['H', 20, 45]]);
  assert.ok(near(e[0].v, 994, 0.01) && near(e[1].v, 1026, 0.01));
  assert.deepEqual(I.findExtrema(mk((i, j) => 1000 + 0.3 * j + 0.1 * i), 8, PRES.lo, PRES.hi, 1), [], 'a slope');
  const dimple = (i, j) => 1012 - 0.5 * Math.exp(-((i - 20) ** 2 + (j - 30) ** 2) / 30);
  assert.deepEqual(I.findExtrema(mk(dimple), 8, PRES.lo, PRES.hi, 1), [], 'a 0.5-hPa dimple');
  assert.equal(I.findExtrema(mk(dimple), 8, PRES.lo, PRES.hi, 0.2).length, 1, 'found at a smaller depth');
  assert.deepEqual(I.findExtrema(mk((i, j) => 1012 - 18 * Math.exp(-((i - 3) ** 2 + (j - 30) ** 2) / 30)), 8, PRES.lo, PRES.hi, 1), [], 'within the radius of the window edge');
  assert.deepEqual(I.findExtrema(mk((i, j) => Math.min(1060, 1040 + 30 * Math.exp(-((i - 20) ** 2 + (j - 30) ** 2) / 30))), 8, PRES.lo, PRES.hi, 1), [], 'a clamped high');
  const flatBottom = mk((i, j) => 1012 - 18 * Math.exp(-((i - 20) ** 2 + (j - 30) ** 2) / 30));
  flatBottom.V[20 * C + 31] = flatBottom.V[20 * C + 30];
  assert.equal(I.findExtrema(flatBottom, 8, PRES.lo, PRES.hi, 1).length, 1, 'two equal lowest nodes: one L (the earlier)');
});

test('findExtrema: a low on the date line of a whole-world window is found once; projectExtrema draws it in every world copy, rounded to the hPa, away from the edges', () => {
  const fr = frameOf((r, c) => { const dc = Math.min(c, 720 - c); return 1013 - 25 * Math.exp(-((r - 120) ** 2 + dc ** 2) / 200); });
  const view = { z: 1.5, w: 2000, h: 700, ox: -300, oy: 0 }, n = 256 * Math.pow(2, 1.5);
  const g = I.isoValues(fr, PRES, I.nodeWindow(view, HALF, fr, 0.2), I.isoStride(view.z, 0.5), null);
  const e = I.findExtrema(g, 6, PRES.lo, PRES.hi, 1);
  assert.equal(e.length, 1); assert.equal(e[0].type, 'L'); assert.equal(e[0].j, 0, 'at the date line column');
  const marks = I.projectExtrema(e, g, HALF, view);
  assert.ok(marks.length >= 2, `${marks.length} copies in view`);
  for (let k = 1; k < marks.length; k++) assert.ok(near(marks[k].x - marks[k - 1].x, n, 1e-6));
  assert.ok(marks.every((m) => m.text === String(Math.round(e[0].v)) && m.y === marks[0].y && m.x >= I.ISO_HL_EDGE_PX && m.x <= view.w - I.ISO_HL_EDGE_PX));
  const pc = I.forwardPixel(30, 180, 1.5);
  assert.ok(near(marks[0].y, pc.y - view.oy, 1), 'at 30 N');
});

// ---- the canvas layer ----
function fakeCtx() {
  const c = { ops: [], widths: [], cleared: [], texts: [], fonts: [] };
  for (const m of ['beginPath', 'moveTo', 'lineTo', 'quadraticCurveTo', 'closePath', 'fill', 'setTransform', 'save', 'restore', 'translate', 'rotate']) c[m] = function () { c.ops.push(m); };
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

test('the layer: a low 80 px inside the view\'s edge still gets its L (the traced window reaches past the view by the H / L radius)', () => {
  const pxPerDeg = 256 * 32 / 360, lng = -150 - (600 - 80) / pxPerDeg;
  const { il, ctx } = isoLayer();
  il.setData(lowAt(30, lng, 20), HALF, { step: 0 }, PRES); il.pendingFn();
  assert.equal(il.drawn.hl, 1, 'the L near the edge');
  assert.ok(ctx.texts.includes('L'));
});
