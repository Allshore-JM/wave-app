'use strict';
// Isobars (plan section 46, step 3): the sea-level pressure frame traced into labelled lines (static_overlay/overlay.js,
// no browser). Synthetic fields pin the geometry (closed loops, saddles, the date line, the interval by zoom); a REAL GFS
// PRMSL frame (tests/capture_pres_fixture.py: the job's own 920-1060 hPa half frame + the true values) pins how far the
// traced lines sit from the true isobars (G32a A-1); a recording canvas pins the layer.  node --test tests/overlay/
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

test('the interval and the node stride follow the zoom: 8 hPa at the widest views, 4 hPa, every 2 hPa from zoom 5', () => {
  assert.deepEqual([1, 2, 2.9, 3, 4, 4.9, 5, 9, 18].map(I.isoInterval), [8, 8, 8, 4, 4, 4, 2, 2, 2]);
  assert.deepEqual([1, 2, 3, 3.45, 3.5, 4, 6].map((z) => I.isoStride(z, 0.5)), [2, 2, 2, 2, 1, 1, 1], 'every other node while a 0.5-deg cell is under 4 px (to zoom ~3.49)');
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
  assert.equal(g.stride, 2); assert.equal(g.C, 361, '720 columns + the seam column, every other one');
  const lines = I.traceIsolines(g.V, g.R, g.C, 8);
  const paths = I.projectIsolines(lines, g, HALF, view);
  const copies = paths.length / lines.length, n = 256 * Math.pow(2, 1.5);
  assert.ok(Number.isInteger(copies) && copies >= 3, `the view (2000 px, a world ${n.toFixed(0)} px) shows ${copies} copies`);
  for (let k = 0; k < copies; k++) {                       // each copy a world apart, and together they cover the view
    assert.ok(near(paths[k * lines.length].xy[0] - paths[0].xy[0], k * n, 1e-6));
  }
  const xs = paths.map((p) => p.xy[0]);
  assert.ok(Math.min(...xs) <= 0 && Math.max(...xs) + n >= view.w);
});

test('isoValues smooths a flat field within half a code of every node, leaves a steep one as coded, and skips missing nodes', () => {
  const fr = frameOf((r, c) => 1000 + c * 0.13);
  fr.q[100 * 720 + 50] = 0;
  const g = I.isoValues(fr, PRES, { r0: 90, r1: 110, c0: 40, count: 30 }, 1, null);
  const half = (PRES.hi - PRES.lo) / 254 / 2;
  for (let i = 0; i < g.R; i++) for (let j = 0; j < g.C; j++) {
    const q = fr.q[(90 + i) * 720 + 40 + j], v = g.V[i * g.C + j];
    if (!q) { assert.ok(Number.isNaN(v)); continue; }
    assert.ok(Math.abs(v - (PRES.lo + (q - 1) * 2 * half)) <= half + 1e-9);
  }
  assert.ok(Number.isNaN(g.V[10 * g.C + 10]));
  // a pit one code deep in a flat field: the kernel pulls it up 0.75 code, the bound stops it at half a code
  const pit = frameOf(() => 1000);
  const v0 = pit.q[100 * 720 + 50]; pit.q[100 * 720 + 50] = v0 - 1;
  const gp = I.isoValues(pit, PRES, { r0: 99, r1: 101, c0: 49, count: 3 }, 1, null);
  assert.ok(Math.abs(gp.V[1 * 3 + 1] - (PRES.lo + (v0 - 1 - 1 + 0.5) * 2 * half)) < 1e-6, 'held at half a code above its own code');
  const asCoded = (fr2, k, C2) => PRES.lo + (fr2.q[(90 + Math.floor(k / C2)) * 720 + 40 + k % C2] - 1) * 2 * half;
  assert.ok(Array.from(g.V).some((v, k) => v === v && Math.abs(v - asCoded(fr, k, g.C)) > 1e-6), 'a flat field is smoothed');
  const steep = frameOf((r, c) => 940 + c * 1.5);             // ~3 codes a node: no node is flat, every value stays its code
  const s2 = I.isoValues(steep, PRES, { r0: 90, r1: 110, c0: 40, count: 30 }, 1, null);
  for (let k = 0; k < s2.V.length; k++) assert.ok(Math.abs(s2.V[k] - asCoded(steep, k, s2.C)) < 1e-9);
});

// ---- the real frame (G32a A-1 (b)) ----
function realFrames() {
  const png = fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'pres_half.png'));
  const raw = zlib.gunzipSync(fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'pres_half_true.bin.gz')));
  const truth = new Float64Array(720 * 361);
  for (let i = 0; i < truth.length; i++) truth[i] = 900 + raw.readUInt16LE(2 * i) / 20;
  return { png: png.buffer.slice(png.byteOffset, png.byteOffset + png.byteLength), truth };
}
// The share of the true isobars' points (2 hPa, |lat| <= 60) more than `tol` nodes from the traced line of the same level.
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
  for (const l of trueLines) {
    const p = l.pts;
    for (let m = 0; m < p.length; m += 2) {
      let best = Infinity;
      for (let gx = Math.floor(p[m]) - 2; gx <= Math.floor(p[m]) + 2; gx++) for (let gy = Math.floor(p[m + 1]) - 2; gy <= Math.floor(p[m + 1]) + 2; gy++) {
        const list = segs.get(l.level + ':' + gx + ':' + gy);
        if (list) for (const s of list) best = Math.min(best, dist(p[m], p[m + 1], s));
      }
      all++; if (best > tol) off++;
    }
  }
  return off / all;
}

test("on a real GFS PRMSL frame the traced isobars sit on the true ones, closer than the raw codes or the contours' smoothing would put them", async () => {
  const { png, truth } = realFrames();
  const fr = await I.decodePngGrey(png, { cols: 720, rows: 361 });
  const win = { r0: 60, r1: 300, c0: 0, count: 720 };      // |lat| <= 60 (the reviewer's measure), every column
  const R = 241, C = 719;                                  // the 719 cells of one world (no seam column)
  const T = new Float64Array(R * C), Q = new Float64Array(R * C);
  for (let i = 0; i < R; i++) for (let j = 0; j < C; j++) {
    T[i * C + j] = truth[(60 + i) * 720 + j];
    const q = fr.q[(60 + i) * 720 + j]; Q[i * C + j] = PRES.lo + (q - 1) * (PRES.hi - PRES.lo) / 254;
  }
  const truthLines = I.traceIsolines(T, R, C, 2);
  const g = I.isoValues(fr, PRES, Object.assign({}, win, { count: 719 }), 1, null);   // the same 719 columns as T (no seam column)
  const smooth = offShare(truthLines, I.traceIsolines(g.V, g.R, g.C, 2), 1);
  const raw = offShare(truthLines, I.traceIsolines(Q, R, C, 2), 1);
  const tight = offShare(truthLines, I.traceIsolines(g.V, g.R, g.C, 2), 0.5);
  const S = new Float32Array(720 * 361);                      // the contours' rule (smoothBlock), for comparison
  I.smoothBlock(fr.q, 720, 361, S, null, 0, 60, 301, 0, 720);
  const B = new Float64Array(R * 719);
  for (let i = 0; i < R; i++) for (let j = 0; j < 719; j++) B[i * 719 + j] = PRES.lo + (S[(60 + i) * 720 + j] - 1) * (PRES.hi - PRES.lo) / 254;
  const block = offShare(truthLines, I.traceIsolines(B, R, 719, 2), 1);
  console.log(`real PRMSL: points > 1 node off: isoSmooth ${(smooth * 100).toFixed(2)} %, raw codes ${(raw * 100).toFixed(2)} %, smoothBlock ${(block * 100).toFixed(2)} %; > 0.5 node isoSmooth ${(tight * 100).toFixed(2)} %`);
  assert.ok(smooth < 0.025, `isoSmooth: ${(smooth * 100).toFixed(2)} % of the points more than a node (55 km) off`);
  assert.ok(smooth < raw && smooth < block, "better than the raw codes and than the contours' rule");
  assert.ok(tight < 0.05, `> 0.5 node: ${(tight * 100).toFixed(2)} %`);
});

test('the whole-world trace of a real frame is fast enough for every playback step (z3, 1080p)', async () => {
  const { png } = realFrames();
  const fr = await I.decodePngGrey(png, { cols: 720, rows: 361 });
  const view = { z: 3, w: 1920, h: 1080, ox: 0, oy: 256 * 8 / 2 - 540 };
  let best = Infinity, lines = 0;
  for (let k = 0; k < 5; k++) {
    const t0 = performance.now();
    const g = I.isoValues(fr, PRES, I.nodeWindow(view, HALF, fr, 0), I.isoStride(view.z, 0.5), null);
    const paths = I.projectIsolines(I.traceIsolines(g.V, g.R, g.C, I.isoInterval(view.z)), g, HALF, view);
    I.placeLabels(paths, view, (t) => 6 * t.length);
    best = Math.min(best, performance.now() - t0); lines = paths.length;
  }
  console.log(`whole world z3 1920x1080: ${best.toFixed(1)} ms, ${lines} paths`);
  assert.ok(best < 40, `${best.toFixed(1)} ms`);
});

// ---- labels ----
test('labels: one at 175 px and every 350 px along a long line, one in the middle of a shorter one, none on a short one', () => {
  const view = { w: 1200, h: 800 };
  const line = (level, y, x0, x1) => ({ level, xy: Float64Array.from([x0, y, (x0 + x1) / 2, y, x1, y]), closed: false });
  const long = I.placeLabels([line(1004, 400, 50, 1150)], view, (t) => 6 * t.length);
  assert.deepEqual(long.map((b) => Math.round(b.x)), [225, 575, 925]);
  assert.equal(long[0].text, '1004'); assert.equal(long[0].w, 24 + 2 * I.ISO_STYLE.padX); assert.equal(long[0].h, I.ISO_STYLE.h);
  assert.deepEqual(I.placeLabels([line(996, 300, 100, 400)], view, () => 24).map((b) => b.x), [250], 'a 300-px line: one in the middle');
  assert.deepEqual(I.placeLabels([line(996, 300, 100, 240)], view, () => 24), [], 'under 150 px in view: none');
  assert.deepEqual(I.placeLabels([line(996, 300, -2000, -1000)], view, () => 24), [], 'off the view: none');
});

test('labels never overlap each other and keep 8 px from the edges', () => {
  const view = { w: 800, h: 600 };
  const paths = [];
  for (let k = 0; k < 60; k++) paths.push({ level: 1000 + 2 * k, xy: Float64Array.from([0, 3 + k * 9, 400, 3 + k * 9, 800, 3 + k * 9]), closed: false });   // 9 px apart: pills (14 px) would collide
  const labels = I.placeLabels(paths, view, (t) => 6 * t.length);
  assert.ok(labels.length > 5);
  for (const a of labels) {
    assert.ok(a.x - a.w / 2 >= 8 && a.y - a.h / 2 >= 8 && a.x + a.w / 2 <= 792 && a.y + a.h / 2 <= 592);
    for (const b of labels) if (a !== b) assert.ok(!(Math.abs(a.x - b.x) < (a.w + b.w) / 2 && Math.abs(a.y - b.y) < (a.h + b.h) / 2), 'no overlap');
  }
});

// ---- the canvas layer ----
function fakeCtx() {
  const c = { ops: [], widths: [], cleared: [] };
  for (const m of ['beginPath', 'moveTo', 'lineTo', 'quadraticCurveTo', 'closePath', 'fill', 'fillText', 'setTransform']) c[m] = function () { c.ops.push(m); };
  c.stroke = function () { c.ops.push('stroke'); c.widths.push(c.lineWidth); };
  c.clearRect = function (x, y, w, h) { c.ops.push('clearRect'); c.cleared.push([x, y, w, h]); };
  c.measureText = (t) => ({ width: 6 * t.length });
  return c;
}
function isoLayer(view) {
  const ctx = fakeCtx(), events = {}, v = view || { w: 1200, h: 800 };
  const pc = I.forwardPixel(30, -150, 5);
  const map = { on(ev, fn) { (events[ev] = events[ev] || []).push(fn); }, off(ev, fn) { events[ev] = (events[ev] || []).filter((f) => f !== fn); },
    getPane: () => null, createPane: () => ({ style: {}, appendChild() {} }), getSize: () => ({ x: v.w, y: v.h + 300 }), getContainer: () => ({ clientWidth: v.w, clientHeight: v.h }),
    getPixelBounds: () => ({ min: { x: pc.x - v.w / 2, y: pc.y - v.h / 2 } }), getZoom: () => 5, containerPointToLayerPoint: () => ({ x: -3, y: 7 }) };
  const canvas = { style: {}, width: 0, height: 0, getContext: () => ctx, remove() { canvas.removed = true; } };
  global.document = { createElement: () => canvas, hidden: false };
  const il = new I.IsobarLayer(map);
  il.scheduled = 0; il.raf = (fn) => { il.scheduled++; il.pendingFn = fn; return 7; }; il.caf = () => { il.pendingFn = null; il.cancelled = (il.cancelled || 0) + 1; };
  il.attach();
  return { il, ctx, canvas, events, map };
}
const lowAt = (lat, lng, depth) => frameOf((r, c) => { const la = 90 - r * 0.5, lo = -180 + c * 0.5; return 1012 - depth * Math.exp(-((la - lat) ** 2 + (lo - lng) ** 2) / 40); });

test('the layer draws on the next animation frame: halos then cores in two strokes, the line cleared under each pill', () => {
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
  assert.deepEqual(ctx.widths.slice(0, 2), [I.ISO_STYLE.haloW, I.ISO_STYLE.coreW], 'halo first, then the core');
  assert.ok(il.drawn.lines >= 5, `${il.drawn.lines} lines`);
  const pills = ctx.ops.filter((o) => o === 'fillText').length;
  assert.equal(pills, il.drawn.labels); assert.ok(pills >= 1);
  assert.equal(ctx.cleared.length, 1 + pills, 'the whole canvas once, then under every pill');
  assert.ok(il.lastMs >= 0);
});

test('clearData clears at once and drops a pending draw; zoomstart hides the canvas until zoomend; detach leaves nothing', () => {
  const { il, ctx, canvas, events } = isoLayer();
  il.setData(lowAt(30, -150, 20), HALF, { step: 0 }, PRES); il.pendingFn();
  const n = ctx.cleared.length;
  il.setData(lowAt(30, -150, 10), HALF, { step: 1 }, PRES);
  il.clearData();
  assert.equal(il.cancelled, 1, 'the pending draw is cancelled'); assert.equal(ctx.cleared.length, n + 1, 'cleared at once');
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
