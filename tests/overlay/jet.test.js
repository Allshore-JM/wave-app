'use strict';
// The jet stream overlay (plan section 106): the 250 hPa speed in its ramp only in its cores (from 50 kt), a pale teal
// shade under that (the owner's pick S2), its particles only in the cores, the wind's relief / outline / resolution rules, and the 250 hPa height contours through the isobar layer
// (12 dam, 24 at the whole-world view, 6 from zoom 6; labels and the readout in dam; no H / L marks).
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
const KT = 1852 / 3600;
const HALF = { cols: 720, rows: 361, lon0: -180, lat0: 90, dlon: 0.5, dlat: -0.5, registration: 'center', lon_periodic: true };
// the job's published entries (tools/model_frames/encode.py FIELDS)
const JET = { lo: 0, hi: 130, legend: [50 * KT, 200 * KT], units: 'm/s', interpolation: 'bilinear', resolutions: ['full', 'half'] };
const JDIR = { lo: 0, hi: 360, legend: [0, 360], units: 'deg', interpolation: 'circular', resolutions: ['half'], circular: true, convention: 'from' };
const JHGT = { lo: 8750, hi: 11290, legend: [8750, 11290], units: 'm', interpolation: 'bilinear', resolutions: ['half'] };
const PRES = { lo: 920, hi: 1060, legend: [920, 1060], units: 'hPa', interpolation: 'bilinear', resolutions: ['half'] };
const codeOf = (v, f) => Math.max(1, Math.min(255, 1 + Math.round((v - f.lo) / (f.hi - f.lo) * 254)));
const dcode = (deg) => 1 + (Math.round(deg * 254 / 360) % 254);
function frame(cols, rows, fn) {
  const q = new Uint8Array(cols * rows);
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) q[r * cols + c] = fn(r, c);
  return { q, cols, rows };
}

test('GFS and ECMWF offer the jet stream, RRFS never; the field has a ramp, a fixed opacity of 1 and the wind\'s rules', () => {
  assert.ok(I.MODEL_DEFS.gfswave.fields.includes('jet') && I.MODEL_DEFS.ecmwf.fields.includes('jet'));
  assert.ok(!I.MODEL_DEFS.rrfs.fields.includes('jet'));
  assert.equal(I.modelOffers('gfswave', 'jet'), true); assert.equal(I.modelOffers('rrfs', 'jet'), false);
  assert.ok(Array.isArray(I.RAMPS.jet) && I.RAMPS.jet[0][0] === 0 && I.RAMPS.jet[I.RAMPS.jet.length - 1][0] === 1);
  assert.equal(I.FIXED_OPACITY.jet, 1);
  assert.equal(I.CLIP_FIELDS.jet, false, 'drawn over land like the wind'); assert.equal(I.OUTLINE_FIELDS.jet, true);
  assert.equal(I.RELIEF_FIELDS.jet, true); assert.equal(I.RELIEF_FIELDS.wind, true); assert.ok(!I.RELIEF_FIELDS.hs);
  assert.deepEqual(I.CORE_FLOOR.jet, { from: [0xe6, 0xf2, 0xf1], to: [0x9c, 0xcd, 0xcf], alpha: 0.8 }, 'S2 pale teal, 80 %');
  assert.ok(!I.CORE_FLOOR.wind && !I.CORE_FLOOR.hs);
  for (const w of [400, 1200]) for (const z of [2, 3.5, 5, 6.9, 7, 7.4, 7.5, 9]) {
    assert.equal(I.wantHalf(z, w, 'jet'), I.wantHalf(z, w, 'wind'), `half ${z} ${w}`);
    assert.equal(I.wantFull(z, w, 'jet'), I.wantFull(z, w, 'wind'), `full ${z} ${w}`);
  }
  assert.equal(I.DIR_FIELDS.jet, 'jdir');
  assert.deepEqual(I.PARTICLE_LIFE_MS.jet, [1500, 3500]);
});

test('composeTile: a cores field shades every value under the legend floor (from -> to at alpha; true: transparent); the colours above are the LUT\'s; other fields unchanged', () => {
  const lut = I.buildLut('jet', JET.legend), vals = [0, 20, 49 * KT, 50.5 * KT, 80, 120, 130];
  const codes = Uint8Array.from(vals.map((v) => codeOf(v, JET)));
  const d = new Uint8ClampedArray(codes.length * 4), U = I.CORE_FLOOR.jet;
  I.composeTile(codes, null, lut, JET.lo, JET.hi, JET.legend[0], JET.legend[1], d, null, U);
  const alpha = (i) => d[i * 4 + 3], rgb = (i) => [...d.slice(i * 4, i * 4 + 3)];
  for (const i of [0, 1, 2]) {
    const v = JET.lo + (codes[i] - 1) / 254 * (JET.hi - JET.lo), f = v / JET.legend[0];
    assert.equal(alpha(i), Math.round(255 * 0.8), `${vals[i]} m/s: shaded at 80 %`);
    for (let k = 0; k < 3; k++) assert.ok(Math.abs(rgb(i)[k] - (U.from[k] + f * (U.to[k] - U.from[k]))) <= 0.5, `${vals[i]} m/s channel ${k}`);
  }
  assert.deepEqual(rgb(0), U.from, 'calm: the lightest shade');
  assert.ok(rgb(2).every((c, k) => Math.abs(c - U.to[k]) <= 3), '49 kt: next to the darkest shade');
  for (const i of [3, 4, 5, 6]) assert.equal(alpha(i), 255, `value ${vals[i]} drawn`);
  const half = new Uint8ClampedArray(codes.length * 4);
  I.composeTile(codes, Uint8Array.from(vals.map(() => 128)), lut, JET.lo, JET.hi, JET.legend[0], JET.legend[1], half, null, U);
  assert.equal(half[3], Math.round(127 * 0.8), 'the shade takes the pixel\'s alpha times 80 %');
  const clear = new Uint8ClampedArray(codes.length * 4);
  I.composeTile(codes, null, lut, JET.lo, JET.hi, JET.legend[0], JET.legend[1], clear, null, true);
  for (const i of [0, 1, 2]) assert.equal(clear[i * 4 + 3], 0, 'core true: transparent under the floor');
  for (const i of [3, 4, 5, 6]) for (let k = 0; k < 4; k++) assert.equal(clear[i * 4 + k], d[i * 4 + k], 'same above the floor either way');
  const plain = new Uint8ClampedArray(codes.length * 4);
  I.composeTile(codes, null, lut, JET.lo, JET.hi, JET.legend[0], JET.legend[1], plain, null);
  for (let i = 0; i < vals.length; i++) assert.equal(plain[i * 4 + 3], 255, 'without the flag every value is drawn (wind, waves: unchanged)');
  for (const i of [3, 4, 5, 6]) for (let k = 0; k < 3; k++) assert.equal(d[i * 4 + k], plain[i * 4 + k], 'the same colour above the floor');
  assert.deepEqual([...plain.slice(0, 3)], [...lut.slice(0, 3)], 'under the floor the plain compose clamps to the first colour');
});

test('legend ticks in the site\'s units: 50-200 kt as 58-230 mph / 93-370 km/h, no "<=" (under the floor is a separate shade, not the first colour)', () => {
  const us = I.legendTicks('jet', JET, 'US'), metric = I.legendTicks('jet', JET, 'Metric');
  assert.deepEqual(us.map((t) => t.label), ['60', '100', '150', '230+ mph']);
  assert.deepEqual(metric.map((t) => t.label), ['100', '150', '200', '250', '300', '370+ km/h']);
  for (const t of us.concat(metric)) assert.ok(t.pos >= 0 && t.pos <= 1);
  assert.ok(us[0].pos > 0 && us[0].pos < 0.03, '60 mph sits just above the 57.5-mph floor');
  assert.ok(I.legendTicks('tp', { lo: 1, hi: 30, legend: [4, 22] }, 'US')[0].label.startsWith('≤'), 'peak period keeps its "<="');
});

test('particles: none under the legend floor (50 kt, from the manifest), 1.3 px/s per m/s above; vectorNodes marks the sub-core nodes absent', () => {
  assert.ok(Math.abs(I.FLOW_SPEED.jet(77) - 100.1) < 1e-9, '150 kt = 77 m/s: 100 px/s');
  assert.equal(I.JET_CORE_MS, undefined, 'no hard-coded floor (G74 A-3)');
  const jet = frame(720, 361, (r, c) => codeOf(c < 360 ? 20 : 70, JET)), jdir = frame(720, 361, () => dcode(270));
  const l = new I.ModelGridLayer({ opacity: 1 });
  l.setFrame(jet, HALF, 'jet', JET, I.buildLut('jet', JET.legend), { step: 0, valid_utc: '2026-11-22T00:00:00Z' });
  const N = I.vectorNodes('jet', l, jdir, HALF);
  const west = 100 * 720 + 100, east = 100 * 720 + 500;
  assert.equal(N.M[west], 0, '20 m/s (39 kt): no particle'); assert.equal(N.U[west], 0);
  assert.equal(N.M[east], 1); assert.ok(Math.abs(N.U[east] - 1.3 * 70) < 0.6 && Math.abs(N.V[east]) < 1.5, `from the west: east at 91 px/s (${N.U[east]}, ${N.V[east]})`);
  assert.equal(N.floor, JET.legend[0]);
  // the floor follows the manifest: a 60-kt legend moves it (the shade edge and the particles together)
  const l60 = new I.ModelGridLayer({ opacity: 1 }), J60 = Object.assign({}, JET, { legend: [60 * KT, 200 * KT] });
  l60.setFrame(frame(720, 361, () => codeOf(55 * KT, JET)), HALF, 'jet', J60, I.buildLut('jet', J60.legend), { step: 0, valid_utc: '2026-11-22T00:00:00Z' });
  assert.equal(I.vectorNodes('jet', l60, jdir, HALF).M[west], 0, '55 kt under a 60-kt floor');
  const wind = new I.ModelGridLayer({ opacity: 1 });
  wind.setFrame(jet, HALF, 'wind', JET, I.buildLut('wind', JET.legend), { step: 0, valid_utc: '2026-11-22T00:00:00Z' });
  const NW = I.vectorNodes('wind', wind, jdir, HALF);
  assert.equal(NW.floor, null); assert.equal(NW.M[west], 1, 'the wind has no floor');
});

test('particles at the cores\' rims (G74 A-2): a lattice point takes a vector only where the value interpolated there reaches the floor', () => {
  // nodes: calm west of 20 E, 70 m/s from 20 E; the value between 19.5 E and 20 E reaches 50 kt at 37 % of the cell
  const jet = frame(720, 361, (r, c) => codeOf(c < 400 ? 0 : 70, JET)), jdir = frame(720, 361, () => dcode(270));
  const l = new I.ModelGridLayer({ opacity: 1 });
  l.setFrame(jet, HALF, 'jet', JET, I.buildLut('jet', JET.legend), { step: 0, valid_utc: '2026-11-22T00:00:00Z' });
  const N = I.vectorNodes('jet', l, jdir, HALF), z = 9, s = 2, w = 600, h = 20;
  const p = I.forwardPixel(0, 19.75, z), view = { z, w, h, ox: p.x - w / 2, oy: p.y - h / 2, zt: z };
  const vf = I.flowField(view, s, N, HALF, l, false), n = 256 * Math.pow(2, z), edge = (25.722 - 0) / 70;
  let rim = 0, onFloor = 0, inCore = 0;
  for (let i = 0; i < vf.cols; i++) {
    const lon = (view.ox + i * s) / n * 360 - 180, f = (lon - 19.5) / 0.5, u = vf.u[5 * vf.cols + i];
    if (f < 0 || f > 1) continue;
    if (f < edge - 0.01) { assert.equal(u, 0, `lon ${lon.toFixed(3)}: ${(70 * f / KT).toFixed(1)} kt, under the floor`); if (f >= 0.25) rim++; }
    else if (f > edge + 0.01) { assert.ok(u > 0, `lon ${lon.toFixed(3)}: in the core`); inCore++; }
    else onFloor++;
  }
  assert.ok(rim >= 5 && inCore > 10, `the old node-weight rule alone would have moved ${rim} rim points; ${inCore} in the core`);
});

test('the contour spec per field: isobars under wind (unchanged), the 250 hPa height under the jet in dam, no H / L', () => {
  assert.deepEqual(I.PRES_UNDER, { wind: 'pres', jet: 'jhgt' });
  const P = I.contourSpecOf('pres'), H = I.contourSpecOf('jhgt');
  assert.equal(I.contourSpecOf(undefined), P); assert.equal(I.contourSpecOf('xyz'), P);
  assert.equal(P.hl, true); assert.equal(H.hl, false); assert.equal(H.unit, 'dam'); assert.equal(H.scale, 0.1);
  for (const z of [1, 2.9, 3, 5.9, 6, 9]) for (const span of [40, 200, 300, 400]) assert.equal(I.contourInterval(P, z, span), I.isoInterval(z, span), 'isobars: the old rule');
  assert.equal(I.contourInterval(H, 3, 120), 120); assert.equal(I.contourInterval(H, 5.9, 90), 120);
  assert.equal(I.contourInterval(H, 6, 40), 60); assert.equal(I.contourInterval(H, 9, 2), 60);
  assert.equal(I.contourInterval(H, 2, 360), 240, 'the whole world: 24 dam'); assert.equal(I.contourInterval(H, 6, 300), 240);
  assert.equal(I.presFieldOk(JHGT, 'jhgt'), true); assert.equal(I.presFieldOk(JHGT, 'pres'), false, 'metres are not hPa');
  assert.equal(I.presFieldOk(PRES), true); assert.equal(I.presFieldOk(PRES, 'jhgt'), false);
  assert.equal(I.presFieldOk(Object.assign({}, JHGT, { resolutions: ['full'] }), 'jhgt'), false);
});

test('a height contour\'s label is its level in dam; an isobar\'s stays the hPa level', () => {
  const view = { z: 5, w: 800, h: 600, ox: 0, oy: 0 };
  const line = (level) => ({ level, closed: false, xy: Float64Array.from([...Array(200).keys()].flatMap((i) => [100 + 3 * i, 300])) });
  const dam = I.placeLabels([line(10320)], view, (t) => t.length * 6, [], (lv) => String(Math.round(lv / 10)));
  assert.equal(dam.length, 1); assert.equal(dam[0].text, '1032');
  const hpa = I.placeLabels([line(1012)], view, (t) => t.length * 6, []);
  assert.equal(hpa[0].text, '1012');
});

test('the isobar layer takes the jet\'s spec: setSpec clears the lines at once, the same spec again is a no-op', () => {
  const map = { getSize() { return { x: 800, y: 600 }; }, getPane() { return null; }, on() {}, off() {} };
  const iso = new I.IsobarLayer(map);
  assert.equal(iso.spec, I.CONTOUR_SPECS.pres, 'isobars by default');
  iso.frame = { q: new Uint8Array(1), cols: 1, rows: 1 }; iso.entry = { step: 0 }; iso.fdef = PRES;
  iso.setSpec(I.CONTOUR_SPECS.pres); assert.ok(iso.frame, 'the same spec keeps the frame');
  iso.setSpec(I.CONTOUR_SPECS.jhgt);
  assert.equal(iso.spec, I.CONTOUR_SPECS.jhgt); assert.equal(iso.frame, null); assert.equal(iso.entry, null);
});

test('G74 A-10: a drawn jet tile carries the shade under 50 kt (the layer passes CORE_FLOOR), the ramp above', () => {
  let put = null;
  const ctx = { clearRect() {}, createImageData: () => ({ data: new Uint8ClampedArray(65536 * 4) }), putImageData(img) { put = img.data; } };
  global.document = { createElement: () => ({ getContext: () => ctx }) };
  try {
    const l = new I.ModelGridLayer({ opacity: 1 }), jet = frame(720, 361, (r, c) => codeOf(c < 360 ? 20 : 70, JET));
    l.setFrame(jet, HALF, 'jet', JET, I.buildLut('jet', JET.legend), { step: 0, valid_utc: '2026-11-22T00:00:00Z' });
    for (const [x, want] of [[1, 'shade'], [2, 'ramp']]) {
      const el = { getContext: () => ctx };
      put = null; l._drawTile(el, { z: 2, x, y: 1 });
      assert.ok(put, 'the tile was painted');
      const k = (128 * 256 + 128) * 4, px = [...put.slice(k, k + 4)];
      if (want === 'shade') {
        assert.equal(px[3], 204, 'the shade at 80 %, never opaque teal: ' + px);
        const U = I.CORE_FLOOR.jet, f = 20 / JET.legend[0];
        for (let i = 0; i < 3; i++) assert.ok(Math.abs(px[i] - (U.from[i] + f * (U.to[i] - U.from[i]))) <= 2, 'shade colour ' + px);
      } else assert.equal(px[3], 255, 'the ramp, opaque');
    }
  } finally { delete global.document; }
});

test('G74 A-10 / A-9: the readout\'s contour part carries the spec\'s unit; 24 dam to zoom 2.9', () => {
  const o = Object.create(I.Overlay.prototype), entry = { step: 0 };
  o.layer = { entry }; o.iso = { frame: {}, entry, fdef: JHGT, spec: I.CONTOUR_SPECS.jhgt, valueAt: () => 10324 };
  assert.equal(o._contourReadout(30, -150), '1032 dam');
  o.iso = { frame: {}, entry, fdef: PRES, spec: I.CONTOUR_SPECS.pres, valueAt: () => 1004.2 };
  assert.equal(o._contourReadout(30, -150), '1004 hPa');
  o.iso.entry = { step: 1 }; assert.equal(o._contourReadout(30, -150), null, 'another step\'s contours: nothing');
  const H = I.CONTOUR_SPECS.jhgt;
  assert.equal(I.contourInterval(H, 1.7, 162), 240, 'a portrait phone\'s widest view'); assert.equal(I.contourInterval(H, 2.9, 200), 240);
  assert.equal(I.contourInterval(H, 3, 200), 120);
});

test('G74 B-P1-1: at full resolution (zoom 9) no lattice point flows where the DRAWN value is under 50 kt, though its half-grid direction nodes would carry it; every point drawn in the core with a core node flows', () => {
  const GRID = { cols: 1440, rows: 721, lon0: -180, lat0: 90, dlon: 0.25, dlat: -0.25, registration: 'center', lon_periodic: true };
  // the speed on the full grid: a core with a one-cell dip to 15 m/s at 20.25 E; the half-grid direction nodes (20.0, 20.5 E)
  // both sit in the core, so their weights alone would carry the flow straight across the drawn shade
  const jet = frame(1440, 721, (r, c) => codeOf(c === 801 ? 15 : 60, JET)), jdir = frame(720, 361, () => dcode(270));
  const l = new I.ModelGridLayer({ opacity: 1 });
  l.setFrame(jet, GRID, 'jet', JET, I.buildLut('jet', JET.legend), { step: 0, valid_utc: '2026-11-22T00:00:00Z' });
  const N = I.vectorNodes('jet', l, jdir, HALF), z = 9, s = 4, w = 800, h = 40;
  const p = I.forwardPixel(0, 20.2, z), view = { z, w, h, ox: p.x - w / 2, oy: p.y - h / 2, zt: z };
  const vf = I.flowField(view, s, N, HALF, l, false), n = 256 * Math.pow(2, z);
  let under = 0, flowing = 0;
  for (let j = 0; j < vf.rows; j++) for (let i = 0; i < vf.cols; i++) {
    const lon = (view.ox + i * s) / n * 360 - 180, lat = I.latOfWorldY(view.oy + j * s, n), v = l.valueAt(lat, lon), u = vf.u[j * vf.cols + i];
    if (v < JET.legend[0]) { assert.equal(u, 0, `lon ${lon.toFixed(3)}: drawn at ${(v / KT).toFixed(1)} kt, no flow`); under++; }
    else { assert.ok(u > 0, `lon ${lon.toFixed(3)}: drawn in the core`); flowing++; }
  }
  assert.ok(under > 50 && flowing > 50, `${under} points under the floor, ${flowing} in the core`);
});
