'use strict';
// The jet stream overlay (plan section 106): the 250 hPa speed drawn only in its cores (from 50 kt), its particles only
// there, the wind's relief / outline / resolution rules, and the 250 hPa height contours through the isobar layer
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
  assert.equal(I.CORE_FLOOR.jet, true); assert.ok(!I.CORE_FLOOR.wind && !I.CORE_FLOOR.hs);
  for (const w of [400, 1200]) for (const z of [2, 3.5, 5, 6.9, 7, 7.4, 7.5, 9]) {
    assert.equal(I.wantHalf(z, w, 'jet'), I.wantHalf(z, w, 'wind'), `half ${z} ${w}`);
    assert.equal(I.wantFull(z, w, 'jet'), I.wantFull(z, w, 'wind'), `full ${z} ${w}`);
  }
  assert.equal(I.DIR_FIELDS.jet, 'jdir');
  assert.deepEqual(I.PARTICLE_LIFE_MS.jet, [1500, 3500]);
});

test('composeTile: a cores-only field leaves every value under the legend floor transparent; the colours above are the LUT\'s; other fields unchanged', () => {
  const lut = I.buildLut('jet', JET.legend), vals = [0, 20, 49 * KT, 50.5 * KT, 80, 120, 130];
  const codes = Uint8Array.from(vals.map((v) => codeOf(v, JET)));
  const d = new Uint8ClampedArray(codes.length * 4);
  I.composeTile(codes, null, lut, JET.lo, JET.hi, JET.legend[0], JET.legend[1], d, null, true);
  const alpha = (i) => d[i * 4 + 3];
  assert.equal(alpha(0), 0); assert.equal(alpha(1), 0); assert.equal(alpha(2), 0, '49 kt: under the core floor');
  for (const i of [3, 4, 5, 6]) assert.equal(alpha(i), 255, `value ${vals[i]} drawn`);
  const plain = new Uint8ClampedArray(codes.length * 4);
  I.composeTile(codes, null, lut, JET.lo, JET.hi, JET.legend[0], JET.legend[1], plain, null);
  for (let i = 0; i < vals.length; i++) assert.equal(plain[i * 4 + 3], 255, 'without the flag every value is drawn (wind, waves: unchanged)');
  for (const i of [3, 4, 5, 6]) for (let k = 0; k < 3; k++) assert.equal(d[i * 4 + k], plain[i * 4 + k], 'the same colour above the floor');
  assert.deepEqual([...plain.slice(0, 3)], [...lut.slice(0, 3)], 'under the floor the plain compose clamps to the first colour');
});

test('legend ticks in the site\'s units: 50-200 kt as 58-230 mph / 93-370 km/h, no "<=" (nothing is drawn under the floor)', () => {
  const us = I.legendTicks('jet', JET, 'US'), metric = I.legendTicks('jet', JET, 'Metric');
  assert.deepEqual(us.map((t) => t.label), ['60', '100', '150', '230+ mph']);
  assert.deepEqual(metric.map((t) => t.label), ['100', '150', '200', '250', '300', '370+ km/h']);
  for (const t of us.concat(metric)) assert.ok(t.pos >= 0 && t.pos <= 1);
  assert.ok(us[0].pos > 0 && us[0].pos < 0.03, '60 mph sits just above the 57.5-mph floor');
  assert.ok(I.legendTicks('tp', { lo: 1, hi: 30, legend: [4, 22] }, 'US')[0].label.startsWith('≤'), 'peak period keeps its "<="');
});

test('particles: none under 50 kt, 1.3 px/s per m/s above; vectorNodes marks the sub-core nodes absent', () => {
  assert.equal(I.FLOW_SPEED.jet(49.9 * KT), 0); assert.equal(I.FLOW_SPEED.jet(10), 0);
  assert.ok(Math.abs(I.FLOW_SPEED.jet(77) - 100.1) < 1e-9, '150 kt = 77 m/s: 100 px/s');
  assert.ok(Math.abs(I.JET_CORE_MS - 50 * KT) < 1e-12);
  const jet = frame(720, 361, (r, c) => codeOf(c < 360 ? 20 : 70, JET)), jdir = frame(720, 361, () => dcode(270));
  const l = new I.ModelGridLayer({ opacity: 1 });
  l.setFrame(jet, HALF, 'jet', JET, I.buildLut('jet', JET.legend), { step: 0, valid_utc: '2026-11-22T00:00:00Z' });
  const N = I.vectorNodes('jet', l, jdir, HALF);
  const west = 100 * 720 + 100, east = 100 * 720 + 500;
  assert.equal(N.M[west], 0, '20 m/s (39 kt): no particle'); assert.equal(N.U[west], 0);
  assert.equal(N.M[east], 1); assert.ok(Math.abs(N.U[east] - 1.3 * 70) < 0.6 && Math.abs(N.V[east]) < 1.5, `from the west: east at 91 px/s (${N.U[east]}, ${N.V[east]})`);
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
