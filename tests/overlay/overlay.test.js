'use strict';
// Unit tests for the pure parts of static_overlay/overlay.js (no browser):  node --test tests/overlay/
// The module is loaded in a vm context with a stub Leaflet; only _internals are exercised.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// The module is evaluated in the MAIN realm (not a vm context): cross-realm typed arrays are ~15x
// slower in V8 and have foreign prototypes, which would distort both the perf smoke and deepEqual.
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
  new Function(src)();                                             // the IIFE assigns window.AllshoreOverlay
  return global.AllshoreOverlay._internals;
}
const I = load();

const GRID = { cols: 1440, rows: 721, lon0: -180, lat0: 90, dlon: 0.25, dlat: -0.25, registration: 'center', lon_periodic: true };
const HALF = { cols: 720, rows: 361, lon0: -180, lat0: 90, dlon: 0.5, dlat: -0.5, registration: 'center', lon_periodic: true };
const HS = { lo: 0, hi: 15, legend: [0, 12], units: 'm', interpolation: 'bilinear' };
const TP = { lo: 1, hi: 30, legend: [4, 22], units: 's', interpolation: 'nearest' };
const WIND = { lo: 0, hi: 41.15555555555556, legend: [0, 30.866666666666667], units: 'm/s', interpolation: 'bilinear' };

function frame(cols, rows, fn) {
  const q = new Uint8Array(cols * rows);
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) q[r * cols + c] = fn(r, c);
  return { q, cols, rows };
}
function layer(fr, grid, field, fdef) {
  const l = new I.ModelGridLayer({ opacity: 1 });
  l.setFrame(fr, grid, field, fdef, I.buildRamp(I.RAMPS[field]), { step: 0, valid_utc: '2026-09-22T12:00:00Z' });
  return l;
}
// codes 1..254 everywhere except a missing block (0) — never a code that could be mistaken for "absent"
const PATTERN = (r, c) => (r >= 300 && r < 320 && c >= 100 && c < 130) ? 0 : 1 + ((r * 7 + c * 3) % 254);

test('frameKey: schema 2 per-frame keys and schema 3 template', () => {
  const fr2 = { step: 3, files: { hs: { full: 'a/hs/f003.png', half: 'a/half/hs/f003.png' } } };
  assert.equal(I.frameKey({ schema: 2 }, fr2, 'hs', false), 'a/hs/f003.png');
  assert.equal(I.frameKey({ schema: 2 }, fr2, 'hs', true), 'a/half/hs/f003.png');
  const m3 = { schema: 3, files: { template: 'gfswave/0p25/v1/2026092212/{res}{field}/f{step:03d}.png', res: { full: '', half: 'half/' } } };
  assert.equal(I.frameKey(m3, { step: 3 }, 'tp', false), 'gfswave/0p25/v1/2026092212/tp/f003.png');
  assert.equal(I.frameKey(m3, { step: 240 }, 'wind', true), 'gfswave/0p25/v1/2026092212/half/wind/f240.png');
  assert.equal(I.frameKey(m3, { step: 0 }, 'hs', false), 'gfswave/0p25/v1/2026092212/hs/f000.png');
  assert.throws(() => I.frameKey({ schema: 3 }, { step: 0 }, 'hs', false), /no frame files/);
});

test('pickFrame: first frame valid at or after now, else the last', () => {
  const m = { frames: [0, 3, 6, 9].map(h => ({ step: h, valid_utc: `2026-09-22T${String(12 + h).padStart(2, '0')}:00:00Z` })) };
  assert.equal(I.pickFrame(m, Date.parse('2026-09-22T11:00:00Z')), 0);
  assert.equal(I.pickFrame(m, Date.parse('2026-09-22T15:00:00Z')), 1);         // exactly valid
  assert.equal(I.pickFrame(m, Date.parse('2026-09-22T15:00:01Z')), 2);
  assert.equal(I.pickFrame(m, Date.parse('2026-09-23T00:00:00Z')), 3);         // all past -> last
});

test('validateManifest accepts schema 2 and 3 with the known encoding and file shapes only', () => {
  const files3 = { template: 'x/{res}{field}/f{step:03d}.png', res: { full: '', half: 'half/' } };
  const base = { run: '2026092212', run_utc: '2026-09-22T12:00:00Z', encoding: 'u8-linear-v2', complete: true,
    fields: { hs: HS }, grid: GRID, grid_half: HALF, frames: [{ step: 0, valid_utc: '2026-09-22T12:00:00Z' }] };
  const m3 = { ...base, schema: 3, files: files3 };
  const m2 = { ...base, schema: 2, frames: [{ step: 0, valid_utc: '2026-09-22T12:00:00Z', files: { hs: { full: 'a/hs/f000.png', half: 'a/half/hs/f000.png' } } }] };
  assert.equal(I.validateManifest(m3).run, '2026092212');
  assert.equal(I.validateManifest(m2).run, '2026092212');
  assert.throws(() => I.validateManifest({ ...m3, schema: 4 }), /schema/);
  assert.throws(() => I.validateManifest({ ...m3, encoding: 'u8-linear-v3' }), /encoding/);
  assert.throws(() => I.validateManifest({ ...m3, complete: false }), /complete/);
  assert.throws(() => I.validateManifest({ ...m3, frames: [] }), /incomplete/);
  assert.throws(() => I.validateManifest({ ...m3, frames: [{ step: '0', valid_utc: 'x' }] }), /bad frame/);
  assert.throws(() => I.validateManifest({ ...base, schema: 3 }), /incomplete/);                       // no file template
  assert.throws(() => I.validateManifest({ ...m3, files: { template: 'x', res: { full: '' } } }), /incomplete/);
  assert.throws(() => I.validateManifest({ ...base, schema: 2 }), /bad frame/);                        // no per-frame files
  assert.throws(() => I.validateManifest({ ...m2, frames: [{ step: 0, valid_utc: '2026-09-22T12:00:00Z', files: { hs: { full: 'a' } } }] }), /bad frame/);
});

test('the client accepts a manifest carrying the direction fields of plan section 21 phase B (wave and wind direction)', () => {
  const dir = (res) => ({ lo: 0, hi: 360, legend: [0, 360], units: 'deg', interpolation: 'circular', resolutions: res, circular: true, convention: 'from' });
  const m = { schema: 3, run: '2026092512', run_utc: '2026-09-25T12:00:00Z', encoding: 'u8-linear-v2', complete: true,
    files: { template: 'gfswave/0p25/v1/2026092512/{res}{field}/f{step:03d}.png', res: { full: '', half: 'half/' } },
    fields: { hs: Object.assign({ resolutions: ['full', 'half'] }, HS), tp: Object.assign({ resolutions: ['full', 'half'] }, TP),
      wind: Object.assign({ resolutions: ['full', 'half'] }, WIND), pdir: dir(['full', 'half']), wdir: dir(['half']) },
    grid: GRID, grid_half: HALF, frames: [{ step: 0, valid_utc: '2026-09-25T12:00:00Z' }, { step: 3, valid_utc: '2026-09-25T15:00:00Z' }],
    fill: { version: 2, fields: ['hs', 'pdir', 'tp'], cells: 4, methods: { hs: 'mean', pdir: 'nearest', tp: 'nearest' } } };
  assert.equal(I.validateManifest(m).run, '2026092512');
  assert.equal(I.frameKey(m, m.frames[1], 'hs', true), 'gfswave/0p25/v1/2026092512/half/hs/f003.png');
  assert.equal(I.frameKey(m, m.frames[1], 'wdir', true), 'gfswave/0p25/v1/2026092512/half/wdir/f003.png');
});

test('snapToPixel maps any point to the centre of the drawn pixel, so the readout reads what the tile shows', () => {
  const l = layer(frame(1440, 721, PATTERN), GRID, 'hs', HS);
  for (const coords of [{ z: 1, x: 0, y: 0 }, { z: 3, x: 7, y: 2 }, { z: 5, x: 2, y: 14 }, { z: 6, x: 33, y: 27 }]) {
    const codes = l.tileCodes(coords, new Float64Array(65536));
    let checked = 0;
    for (let py = 3; py < 256; py += 29) for (let px = 5; px < 256; px += 31) {
      const centre = I.tilePixelLatLng(coords, px, py);
      for (const [dx, dy] of [[0, 0], [0.49, 0.49], [-0.49, -0.49], [0.3, -0.45], [-0.2, 0.44]]) {
        const off = I.tilePixelLatLng(coords, px + dx, py + dy);                  // an arbitrary point inside the pixel
        const s = I.snapToPixel(off.lat, off.lng, coords.z);
        assert.ok(Math.abs(s.lat - centre.lat) < 1e-9 && Math.abs(s.lng - centre.lng) < 1e-9, `${JSON.stringify(coords)} ${px},${py} +${dx},${dy}`);
        const code = codes[py * 256 + px], v = l.valueAt(s.lat, s.lng);
        if (!code) assert.equal(v, null); else assert.ok(Math.abs(v - l._value(code)) < 1e-9);
        checked++;
      }
    }
    assert.ok(checked > 300);
  }
  const a = I.snapToPixel(20.3, 200.7, 4), b = I.snapToPixel(20.3, -159.3, 4);                // a world copy snaps the same
  assert.ok(Math.abs(a.lat - b.lat) < 1e-9 && Math.abs(a.lng - 360 - b.lng) < 1e-9);
  assert.ok(Math.abs(l.valueAt(a.lat, a.lng) - l.valueAt(b.lat, b.lng)) < 1e-9);
});

test('validateGrid rejects a frame or grid that does not match the contract', () => {
  const ok = frame(1440, 721, () => 5);
  assert.doesNotThrow(() => I.validateGrid(GRID, ok, HS));
  assert.doesNotThrow(() => I.validateGrid(HALF, frame(720, 361, () => 5), TP));
  const bad = [
    [{ ...GRID, cols: 1441 }, ok, HS], [GRID, frame(1440, 720, () => 5), HS], [{ ...GRID, dlat: 0.25 }, ok, HS],
    [{ ...GRID, registration: 'corner' }, ok, HS], [{ ...GRID, lon_periodic: false }, ok, HS], [{ ...GRID, dlon: 0.5 }, ok, HS],
    [{ ...GRID, lat0: 89.875 }, ok, HS], [{ ...GRID, lon0: 0 }, ok, HS], [GRID, ok, { ...HS, lo: 15, hi: 0 }],
    [GRID, ok, { ...HS, legend: [12, 0] }], [GRID, ok, { ...HS, legend: [0, 16] }], [GRID, { q: ok.q.subarray(0, 10), cols: 1440, rows: 721 }, HS],
  ];
  for (const [g, f, d] of bad) assert.throws(() => I.validateGrid(g, f, d), /does not match/);
});

test('_code: bilinear over present neighbours, periodic columns, absent rule; nearest for Tp', () => {
  const small = { cols: 4, rows: 3, q: Uint8Array.from([10, 20, 30, 40, 50, 60, 70, 80, 0, 0, 0, 0]) };
  const g = { cols: 4, rows: 3, lon0: -180, lat0: 90, dlon: 90, dlat: -90, registration: 'center', lon_periodic: true };
  const l = new I.ModelGridLayer({ opacity: 1 });
  l.setFrame(small, g, 'hs', HS, I.buildRamp(I.RAMPS.hs));
  assert.equal(l._code(0, 0), 10);
  assert.equal(l._code(0.5, 0.5), 35);                                    // (10+20+50+60)/4
  assert.equal(l._code(0, 3.5), 25);                                      // wraps: (40+10)/2
  assert.equal(l._code(1.5, 0.5), 55);                                    // bottom row absent: renormalised (50+60)/2
  assert.equal(l._code(1.9, 0.5), 0);                                     // weight of present cells 0.1 < 0.25 -> no value
  assert.equal(l._code(2, 1), 0);
  const t = new I.ModelGridLayer({ opacity: 1 });
  t.setFrame(small, g, 'tp', TP, I.buildRamp(I.RAMPS.tp));
  assert.equal(t._code(0.6, 3.4), 80);                                    // round(0.6)=1, round(3.4)=3
  assert.equal(t._code(0.4, 3.6), 10);                                    // round(3.6)=4 -> column 0
});

test('readout equals the drawn value: valueAt vs tileCodes, full and half grids, wrapped and pole tiles', () => {
  const cases = [
    [layer(frame(1440, 721, PATTERN), GRID, 'hs', HS), 'hs full'],
    [layer(frame(720, 361, PATTERN), HALF, 'hs', HS), 'hs half'],
    [layer(frame(1440, 721, PATTERN), GRID, 'tp', TP), 'tp nearest'],
    [layer(frame(1440, 721, PATTERN), GRID, 'wind', WIND), 'wind'],
  ];
  // z5/x2/y14 spans 11-22 N, 157.5-146.25 W: it contains part of PATTERN's missing block (10-15 N, 155-147.5 W)
  const tiles = [{ z: 1, x: 0, y: 0 }, { z: 3, x: 7, y: 2 }, { z: 5, x: 2, y: 14 }, { z: 2, x: 0, y: 3 }, { z: 6, x: 33, y: 27 }];
  for (const [l, name] of cases) {
    for (const coords of tiles) {
      const codes = l.tileCodes(coords, new Float64Array(256 * 256));
      let compared = 0, nulls = 0;
      for (let py = 0; py < 256; py += 13) for (let px = 0; px < 256; px += 11) {
        const ll = I.tilePixelLatLng(coords, px, py), code = codes[py * 256 + px], v = l.valueAt(ll.lat, ll.lng);
        if (!code) { assert.equal(v, null, `${name} ${JSON.stringify(coords)} px ${px},${py}`); nulls++; continue; }
        assert.ok(Math.abs(v - l._value(code)) < 1e-9, `${name} ${JSON.stringify(coords)} px ${px},${py}: ${v} vs ${l._value(code)}`);
        compared++;
      }
      assert.ok(compared > 100, `${name}: ${compared} compared`);
      if (coords.z === 5 && coords.x === 2 && l._grid.cols === 1440) assert.ok(nulls > 0, `${name}: missing block visible at ${JSON.stringify(coords)}`);   // (the half grid's block sits elsewhere)
    }
    // a world copy (unwrapped x beyond the world) samples identically to the wrapped tile
    const a = l.tileCodes({ z: 3, x: 7, y: 2 }, new Float64Array(65536)), b = l.tileCodes({ z: 3, x: 15, y: 2 }, new Float64Array(65536));
    const c = l.tileCodes({ z: 3, x: -1, y: 2 }, new Float64Array(65536));
    for (let i = 0; i < a.length; i += 97) { assert.equal(b[i], a[i], name + ' +1 world'); assert.equal(c[i], a[i], name + ' -1 world'); }
  }
});

test('dateline continuity: the last column and the first column are neighbours', () => {
  const l = layer(frame(1440, 721, (r, c) => (c === 1439 ? 100 : c === 0 ? 200 : 50)), GRID, 'hs', HS);
  const mid = l.valueAt(0, 179.875);                                        // exactly between column 1439 (179.75) and column 0 (-180 == 180)
  assert.ok(Math.abs(mid - l._value(150)) < 1e-9, String(mid));
  assert.ok(Math.abs(l.valueAt(0, -180) - l._value(200)) < 1e-9);
  assert.ok(Math.abs(l.valueAt(0, 180) - l._value(200)) < 1e-9);
  assert.ok(Math.abs(l.valueAt(0, 540) - l._value(200)) < 1e-9);
});

test('valueAt is null outside the grid rows and on absent cells', () => {
  const l = layer(frame(1440, 721, PATTERN), GRID, 'hs', HS);
  assert.equal(l.valueAt(90.5, 0), null);
  assert.equal(l.valueAt(-90.5, 0), null);
  assert.equal(l.valueAt(90 - 310 * 0.25, -180 + 115 * 0.25), null);      // inside the missing block
  assert.ok(l.valueAt(89.999, 0) !== null);
  assert.ok(l.valueAt(-89.999, 0) !== null);
});

test('resolution hysteresis: desktop waves 3.5/4, phones and wind 7/7.5 (data budgets)', () => {
  assert.equal(I.wantHalf(3, 1200, 'hs'), true); assert.equal(I.wantFull(3, 1200, 'hs'), false);
  assert.equal(I.wantHalf(3.7, 1200, 'hs'), false); assert.equal(I.wantFull(3.7, 1200, 'hs'), false);   // dead band keeps the current choice
  assert.equal(I.wantFull(4, 1200, 'tp'), true);
  assert.equal(I.wantHalf(5, 800, 'hs'), false);
  // narrow maps (phones): half until zoom 7 for every field (<= 5 MB per loop), full from 7.5
  assert.equal(I.wantHalf(6, 375, 'hs'), true); assert.equal(I.wantFull(6.9, 375, 'hs'), false);
  assert.equal(I.wantHalf(7.2, 375, 'hs'), false); assert.equal(I.wantFull(7.2, 375, 'hs'), false);
  assert.equal(I.wantFull(7.5, 375, 'tp'), true);
  // wind: half until zoom 7 everywhere (frames ~2.7x larger: 32 MB vs 10 MB per loop)
  assert.equal(I.wantHalf(6.9, 1200, 'wind'), true); assert.equal(I.wantFull(6.9, 1200, 'wind'), false);
  assert.equal(I.wantHalf(7.2, 1200, 'wind'), false); assert.equal(I.wantFull(7.2, 1200, 'wind'), false);
  assert.equal(I.wantFull(7.5, 1200, 'wind'), true);
  assert.equal(I.wantHalf(6.9, 1200, 'hs'), false);
});

test('legend ticks are nice numbers in the site unit with the legend top as N+', () => {
  const labels = (f, d, u) => Array.from(I.legendTicks(f, d, u), t => t.label);   // main-realm array (vm arrays differ by prototype)
  assert.deepEqual(labels('hs', HS, 'US'), ['0', '3', '6', '10', '15', '20', '39+ ft']);
  assert.deepEqual(labels('hs', HS, 'Metric'), ['0', '1', '2', '3', '4', '6', '12+ m']);
  assert.deepEqual(labels('tp', TP, 'US'), ['≤4', '8', '12', '16', '22+ s']);
  assert.deepEqual(labels('wind', WIND, 'US'), ['0', '20', '40', '69+ mph']);
  assert.deepEqual(labels('wind', WIND, 'Metric'), ['0', '25', '50', '75', '111+ km/h']);
  for (const [f, d, u] of [['hs', HS, 'US'], ['hs', HS, 'Metric'], ['tp', TP, 'US'], ['wind', WIND, 'US'], ['wind', WIND, 'Metric']]) {
    const pos = Array.from(I.legendTicks(f, d, u), t => t.pos);
    assert.equal(pos[0], 0); assert.equal(pos[pos.length - 1], 1);
    for (let i = 1; i < pos.length; i++) assert.ok(pos[i] > pos[i - 1] && pos[i] <= 1, `${f} ${u} ${pos}`);
    assert.ok(pos[pos.length - 2] <= 0.8, `${f} ${u}: last numeric tick ${pos[pos.length - 2]} would collide with the top label`);
  }
});

test('unitOf conversions', () => {
  assert.equal(I.unitOf('hs', 'US').f(1).toFixed(3), '3.281');
  assert.equal(I.unitOf('hs', 'Metric').label, 'm');
  assert.equal(I.unitOf('wind', 'US').f(30.866666666666667).toFixed(2), '69.05');
  assert.equal(I.unitOf('wind', 'Metric').f(10), 36);
  assert.equal(I.unitOf('tp', 'Metric').label, 's');
  assert.equal(I.pad3(7), '007'); assert.equal(I.pad3(42), '042'); assert.equal(I.pad3(240), '240');
});

test('buildRamp endpoints match the stops', () => {
  const rgb = (h) => [parseInt(h.slice(1, 3), 16), parseInt(h.slice(3, 5), 16), parseInt(h.slice(5, 7), 16)];
  for (const f of ['hs', 'tp', 'wind']) {
    const lut = I.buildRamp(I.RAMPS[f]), st = I.RAMPS[f];
    assert.deepEqual([lut[0], lut[1], lut[2]], rgb(st[0][1]), f);
    assert.deepEqual([lut[765], lut[766], lut[767]], rgb(st[st.length - 1][1]), f);
  }
});

test('wave-height knots: legend position and its inverse, the LUT follows them, linear fields are unchanged', () => {
  const L = HS.legend;
  for (const [v, p] of [[0, 0], [0.5, 0.08], [1, 0.17], [3, 0.47], [12, 1], [15, 1], [-1, 0]]) assert.ok(Math.abs(I.legendPos('hs', L, v) - p) < 1e-12, `${v}`);
  for (let v = 0; v <= 12; v += 0.37) assert.ok(Math.abs(I.legendInv('hs', L, I.legendPos('hs', L, v)) - v) < 1e-9, `${v}`);
  assert.ok(I.legendPos('hs', L, 3) > 0.45, '0-3 m fills about half of the legend');
  // the LUT is linear in value over the legend (what composeTile indexes); entry i gets the colour of its legend position
  const lut = I.buildLut('hs', L), bar = I.buildRamp(I.RAMPS.hs);
  for (const v of [0, 1, 2, 3, 6, 12]) {
    const i = Math.round(v / 12 * 255), j = Math.round(I.legendPos('hs', L, i / 255 * 12) * 255);
    for (let c = 0; c < 3; c++) assert.ok(Math.abs(lut[i * 3 + c] - bar[j * 3 + c]) <= 2, `hs ${v} m channel ${c}`);
  }
  // contrast at the low end: 0, 1, 2 and 3 m are clearly different colours (the old ramp was navy to blue)
  const col = (v) => { const i = Math.round(v / 12 * 255) * 3; return [lut[i], lut[i + 1], lut[i + 2]]; };
  const dist = (a, b) => Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]);
  for (const [a, b, min] of [[0, 1, 90], [1, 2, 90], [2, 3, 90], [3, 4, 60]]) assert.ok(dist(col(a), col(b)) > min, `${a} m vs ${b} m: ${dist(col(a), col(b)).toFixed(0)}`);
  // fields without knots: the LUT is exactly today's ramp, byte for byte
  assert.deepEqual(Array.from(I.buildLut('tp', TP.legend)), Array.from(I.buildRamp(I.RAMPS.tp)));
  assert.deepEqual(Array.from(I.buildLut('wind', WIND.legend)), Array.from(I.buildRamp(I.RAMPS.wind)));
  // tick positions sit where their values are drawn
  for (const t of I.legendTicks('hs', HS, 'Metric').slice(0, -1)) assert.ok(Math.abs(t.pos - I.legendPos('hs', L, Number(t.label))) < 1e-9, t.label);
  const us = I.legendTicks('hs', HS, 'US');
  assert.ok(Math.abs(us[3].pos - I.legendPos('hs', L, 10 / 3.28084)) < 1e-9);
});

test('wave-height knots stretch to any legend the manifest carries: the top colour is the legend top (G8 A-P3-7)', () => {
  const col = (lut, i) => [lut[i * 3], lut[i * 3 + 1], lut[i * 3 + 2]];
  const at = (legend, v) => col(I.buildLut('hs', legend), Math.max(0, Math.min(255, Math.round((v - legend[0]) / (legend[1] - legend[0]) * 255))));
  const top = I.legendBar('hs').slice(255 * 3, 256 * 3);
  // today's legend is unchanged (the colours the owner approved on the test site)
  assert.deepEqual(at([0, 12], 1), [15, 163, 233]); assert.deepEqual(at([0, 12], 3), [250, 203, 21]);
  assert.deepEqual(at([0, 12], 12), Array.from(top));
  for (const legend of [[0, 14], [0, 10], [0, 20]]) {
    assert.equal(I.legendPos('hs', legend, legend[1]), 1, `${legend}: top`);
    assert.deepEqual(at(legend, legend[1]), Array.from(top), `${legend}: the legend top draws the top colour`);
    assert.ok(I.legendPos('hs', legend, legend[1] * 0.9) < 1, `${legend}: below the top is below the top colour`);
    for (let v = 0; v <= legend[1]; v += legend[1] / 17) assert.ok(Math.abs(I.legendInv('hs', legend, I.legendPos('hs', legend, v)) - v) < 1e-9, `${legend}: ${v}`);
    for (const t of I.legendTicks('hs', { lo: 0, hi: 15, legend }, 'Metric').slice(0, -1)) assert.ok(Math.abs(t.pos - I.legendPos('hs', legend, Number(t.label))) < 1e-9, `${legend}: tick ${t.label}`);
  }
  assert.notDeepEqual(at([0, 14], 12), Array.from(top), '12 m is no longer the top on a 14 m legend');
});

test('the legend bar shows, under each tick, the colour the tiles draw for that value', () => {
  const bar = I.legendBar('hs'), lut = I.buildLut('hs', HS.legend);
  assert.notDeepEqual(Array.from(bar), Array.from(lut), 'the bar is laid out by legend position, the tiles by value');
  for (const v of [0.5, 1, 2, 3, 4, 6, 9]) {
    const b = Math.round(I.legendPos('hs', HS.legend, v) * 255), t = Math.round(v / 12 * 255);
    for (let c = 0; c < 3; c++) assert.ok(Math.abs(bar[b * 3 + c] - lut[t * 3 + c]) <= 8, `${v} m channel ${c}: bar ${bar[b * 3 + c]} tile ${lut[t * 3 + c]}`);
  }
  assert.deepEqual(Array.from(I.legendBar('tp')), Array.from(I.buildLut('tp', TP.legend)), 'linear fields: bar = tiles');
});

test('ringPlan: current, two ahead in the play direction, two behind, wrapping, no duplicates', () => {
  assert.deepEqual(Array.from(I.ringPlan(10, 81, 1)), [10, 11, 12, 9, 8]);
  assert.deepEqual(Array.from(I.ringPlan(80, 81, 1)), [80, 0, 1, 79, 78]);
  assert.deepEqual(Array.from(I.ringPlan(0, 81, -1)), [0, 80, 79, 1, 2]);
  assert.deepEqual(Array.from(I.ringPlan(0, 3, 1)), [0, 1, 2]);
  assert.deepEqual(Array.from(I.ringPlan(0, 1, 1)), [0]);
});

test('dotMonth: the month of the folded line gets a period (owner, 2026-10-07), never "May", never the shape of another locale', () => {
  assert.equal(I.dotMonth('Oct 8, 06:00 PM'), 'Oct. 8, 06:00 PM');
  for (const mo of ['Jan', 'Feb', 'Mar', 'Apr', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']) assert.equal(I.dotMonth(mo + ' 1, 12:00 AM'), mo + '. 1, 12:00 AM');
  assert.equal(I.dotMonth('May 8, 06:00 PM'), 'May 8, 06:00 PM', 'the full name');
  assert.equal(I.dotMonth('8. Okt., 18:00'), '8. Okt., 18:00', 'German (the browser locale decides the page clock)');
  assert.equal(I.dotMonth('8 Oct, 18:00'), '8 Oct, 18:00', 'en-GB: day first, left alone');
  assert.equal(I.dotMonth('—'), '—'); assert.equal(I.dotMonth('Oct. 8, 06:00 PM'), 'Oct. 8, 06:00 PM', 'never twice');
  assert.equal(I.dotMonth('x Oct 8'), 'x Oct 8', 'only at the start'); assert.equal(I.dotMonth('Oct x'), 'Oct x', 'only before the day number');
});

test('nextAvailable skips unavailable frames, wraps, and reports when nothing is left', () => {
  const un = set => j => set.has(j);
  assert.equal(I.nextAvailable(5, 1, 81, un(new Set())), 6);
  assert.equal(I.nextAvailable(80, 1, 81, un(new Set())), 0);
  assert.equal(I.nextAvailable(0, -1, 81, un(new Set())), 80);
  assert.equal(I.nextAvailable(5, 1, 81, un(new Set([6, 7]))), 8);
  assert.equal(I.nextAvailable(5, -1, 81, un(new Set([4]))), 3);
  assert.equal(I.nextAvailable(5, 1, 81, un(new Set(Array.from({ length: 81 }, (_, i) => i)))), null);
  assert.equal(I.nextAvailable(5, 1, 81, un(new Set(Array.from({ length: 81 }, (_, i) => i).filter(i => i !== 5)))), null);   // never i itself
  // manual navigation (plan section 37): stepAvailable never wraps; from -1 / n it finds the first / last available frame
  const S = I.stepAvailable, none = un(new Set());
  assert.equal(S(5, 1, 81, none), 6); assert.equal(S(5, -1, 81, none), 4);
  assert.equal(S(80, 1, 81, none), null, 'the last frame: no wrap to the first');
  assert.equal(S(0, -1, 81, none), null, 'the first frame: no wrap to the last');
  assert.equal(S(5, 1, 81, un(new Set([6, 7]))), 8); assert.equal(S(5, -1, 81, un(new Set([4]))), 3);
  assert.equal(S(78, 1, 81, un(new Set([79, 80]))), null, 'only unavailable frames ahead: nothing');
  assert.equal(S(-1, 1, 81, un(new Set([0, 1]))), 2, 'the first available frame');
  assert.equal(S(81, -1, 81, un(new Set([80]))), 79, 'the last available frame');
  assert.equal(S(-1, 1, 81, none), 0); assert.equal(S(81, -1, 81, none), 80);
  assert.equal(S(-1, 1, 3, () => true), null);
  const m = { frames: [0, 3, 6, 9].map(h => ({ step: h, valid_utc: `2026-09-22T${String(12 + h).padStart(2, '0')}:00:00Z` })) };
  assert.equal(I.nearestIndex(m, Date.parse('2026-09-22T16:20:00Z')), 1);
  assert.equal(I.nearestIndex(m, Date.parse('2026-09-22T16:40:00Z')), 2);
  assert.equal(I.nearestIndex(m, Date.parse('2026-09-30T00:00:00Z')), 3);
  assert.equal(I.nearestIndex(m, 0), 0);
});

test('FrameCache: LRU with a cap, never evicts the frame on the map', () => {
  const c = new I.FrameCache(3);
  c.set('a', 1); c.set('b', 2); c.set('c', 3);
  assert.equal(c.size(), 3);
  c.get('a');                                     // a is now most recently used
  c.set('d', 4, 'b');                             // over the cap: b is protected, so c (least recent) goes
  assert.deepEqual(['a', 'b', 'c', 'd'].map(k => c.has(k)), [true, true, false, true]);
  c.set('e', 5, 'b');                             // a is the least recent now
  assert.deepEqual(['a', 'b', 'd', 'e'].map(k => c.has(k)), [false, true, true, true]);
  assert.equal(c.get('zz'), null);
  c.clear(); assert.equal(c.size(), 0);
  assert.equal(I.MAX_DECODED, 5); assert.equal(I.MAX_INFLIGHT, 2);
  assert.deepEqual(Array.from(I.SPEEDS), [1, 2, 4]); assert.equal(I.BASE_FPS, 2);
});

test('failureKind: missing or undecodable frames are permanent, everything else is retried', () => {
  assert.equal(I.failureKind(new Error('frame 404')), 'permanent');
  assert.equal(I.failureKind(new Error('frame 410')), 'permanent');
  assert.equal(I.failureKind(new Error('frame 403')), 'transient');           // r2.dev bot checks answer 403, absent keys 404
  assert.equal(I.failureKind(new Error('frame decode failed')), 'permanent');
  assert.equal(I.failureKind(new Error('frame decoded to 0x0')), 'permanent');
  assert.equal(I.failureKind(new Error('frame 503')), 'transient');
  assert.equal(I.failureKind(new Error('frame 500')), 'transient');
  assert.equal(I.failureKind(new TypeError('Failed to fetch')), 'transient');
  assert.equal(I.failureKind(null), 'transient');
  assert.equal(I.failureKind(new Error('frame stalled')), 'transient');
});

test('decodePngGrey reproduces the reference decode of real frames byte for byte (PIL sha256)', async () => {
  const crypto = require('node:crypto');
  const cases = [
    ['frame_hs_half.png', 720, 361, 112653, '5b70e09d7174f3b5940e5f30f9f04f6ed505f302e656a152288658ef599d0f7b'],
    ['frame_tp_full.png', 1440, 721, 449759, '50fa030639ad712dcd69d577c17061f5cca1b10b4c687653047d4b701c9984d5'],
  ];
  for (const [name, cols, rows, zeros, sha] of cases) {
    const buf = fs.readFileSync(path.join(__dirname, '..', 'fixtures', name));
    const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    const t0 = process.hrtime.bigint();
    const f = await I.decodePngGrey(ab);
    const ms = Number(process.hrtime.bigint() - t0) / 1e6;
    assert.equal(f.cols, cols); assert.equal(f.rows, rows); assert.equal(f.q.length, cols * rows);
    let z = 0; for (let i = 0; i < f.q.length; i++) if (!f.q[i]) z++;
    assert.equal(z, zeros, name + ' land cells');
    assert.equal(crypto.createHash('sha256').update(f.q).digest('hex'), sha, name);
    console.log(`decodePngGrey ${name}: ${ms.toFixed(1)} ms`);
  }
  // a PNG that is not our format (RGB) is handed back as null for the canvas path; garbage is rejected
  const rgb = Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), Buffer.from([0, 0, 0, 13]), Buffer.from('IHDR'),
    Buffer.from([0, 0, 0, 2, 0, 0, 0, 2, 8, 2, 0, 0, 0]), Buffer.alloc(4), Buffer.from([0, 0, 0, 0]), Buffer.from('IEND'), Buffer.alloc(4)]);
  assert.equal(await I.decodePngGrey(rgb.buffer.slice(rgb.byteOffset, rgb.byteOffset + rgb.byteLength)), null);
  await assert.rejects(() => I.decodePngGrey(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9]).buffer), /decode failed/);
});

test('decodePngGrey refuses a picture of another size before inflating it (hostile bucket)', async () => {
  const buf = fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'frame_hs_half.png'));
  const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
  const ok = await I.decodePngGrey(ab, { cols: 720, rows: 361 });
  assert.equal(ok.cols, 720);
  await assert.rejects(() => I.decodePngGrey(ab, { cols: 1440, rows: 721 }), /decode failed/);
  assert.throws(() => I.parsePng(ab, { cols: 1440, rows: 721 }), /decode failed/);      // rejected at IHDR, no inflate
});

test('validateManifest rejects mistyped fields and absurd frame counts', () => {
  const files3 = { template: 'x/{res}{field}/f{step:03d}.png', res: { full: '', half: 'half/' } };
  const base = { schema: 3, run: '2026092212', run_utc: '2026-09-22T12:00:00Z', encoding: 'u8-linear-v2', complete: true,
    fields: { hs: HS }, grid: GRID, grid_half: HALF, frames: [{ step: 0, valid_utc: '2026-09-22T12:00:00Z' }], files: files3, model: {} };
  assert.equal(I.validateManifest(base).run, '2026092212');
  assert.throws(() => I.validateManifest({ ...base, complete: 'yes' }), /complete/);
  assert.throws(() => I.validateManifest({ ...base, run_utc: 12345 }), /incomplete/);
  assert.throws(() => I.validateManifest({ ...base, run_utc: 'not a date' }), /incomplete/);
  assert.throws(() => I.validateManifest({ ...base, model: 'string' }), /incomplete/);
  assert.throws(() => I.validateManifest({ ...base, run: 2026092212 }), /incomplete/);
  const many = Array.from({ length: 513 }, (_, i) => ({ step: i * 3, valid_utc: new Date(Date.parse('2026-09-22T12:00:00Z') + i * 3 * 3.6e6).toISOString().replace('.000Z', 'Z') }));
  assert.throws(() => I.validateManifest({ ...base, frames: many }), /incomplete/);
  assert.equal(I.validateManifest({ ...base, frames: many.slice(0, 512) }).frames.length, 512);
});

test('unfilter handles every PNG filter type on a synthetic image', () => {
  const w = 4, h = 5, img = new Uint8Array(w * h);
  for (let i = 0; i < img.length; i++) img[i] = (i * 37 + 11) & 255;
  // filter each row with a different type and check the round trip
  const raw = new Uint8Array((w + 1) * h);
  for (let y = 0; y < h; y++) {
    const f = y % 5; raw[y * (w + 1)] = f;
    for (let x = 0; x < w; x++) {
      const cur = img[y * w + x], a = x ? img[y * w + x - 1] : 0, b = y ? img[(y - 1) * w + x] : 0, c = (x && y) ? img[(y - 1) * w + x - 1] : 0;
      let pred = 0;
      if (f === 1) pred = a; else if (f === 2) pred = b; else if (f === 3) pred = (a + b) >> 1;
      else if (f === 4) { const p = a + b - c, pa = Math.abs(p - a), pb = Math.abs(p - b), pc = Math.abs(p - c); pred = pa <= pb && pa <= pc ? a : pb <= pc ? b : c; }
      raw[y * (w + 1) + 1 + x] = (cur - pred) & 255;
    }
  }
  assert.deepEqual(Array.from(I.unfilter(raw, w, h)), Array.from(img));
  raw[0] = 7; assert.throws(() => I.unfilter(raw, w, h), /decode failed/);
});

test('tileCodes performance smoke (full grid, bilinear)', () => {
  const l = layer(frame(1440, 721, (r, c) => 1 + ((r + c) % 254)), GRID, 'hs', HS);
  const out = new Float64Array(65536);
  l.tileCodes({ z: 5, x: 1, y: 10 }, out);                                  // warm-up
  const t0 = process.hrtime.bigint();
  for (let i = 0; i < 20; i++) l.tileCodes({ z: 5, x: (i * 3) % 32, y: 10 + (i % 5) }, out);
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  console.log(`tileCodes: ${(ms / 20).toFixed(2)} ms per 256x256 tile`);
  assert.ok(ms < 4000, `${ms} ms for 20 tiles`);
});

// ---- plan section 22: the timeline by time, the run's times in the computer's time zone ----

test('frameAtHour: the frame for a timeline hour snaps in the direction of travel (hourly to +120 h, then every 3 h)', () => {
  const hours = [...Array(121).keys()].concat([...Array(88).keys()].map((i) => 123 + 3 * i));
  const at = (h, later) => hours[I.frameAtHour(hours, h, later)];
  assert.equal(at(37, true), 37); assert.equal(at(37, false), 37);                 // hourly: exact
  assert.equal(at(121, true), 123); assert.equal(at(122, true), 123);              // later: the next frame
  assert.equal(at(122, false), 120); assert.equal(at(124, false), 123);            // earlier: the previous one
  assert.equal(at(124, true), 126); assert.equal(at(125, false), 123);             // an arrow key from 123 or 126 moves one frame
  assert.equal(at(384, true), 384); assert.equal(at(400, true), 384); assert.equal(at(-5, false), 0);
  const old = [...Array(81).keys()].map((i) => 3 * i);                             // an 81-frame run (0..240 every 3 h)
  assert.equal(old[I.frameAtHour(old, 7, true)], 9); assert.equal(old[I.frameAtHour(old, 7, false)], 6);
});

// ---- plan section 37: the compass ribbon's engine (real timestamps, local midnights, snapping) ----

const HOURS_209 = [...Array(121).keys()].concat([...Array(88).keys()].map((i) => 123 + 3 * i));
const HOURS_81 = [...Array(81).keys()].map((i) => 3 * i);
function framesAt(runUtc, hours) { const run = Date.parse(runUtc); return hours.map((h) => ({ step: h, valid_utc: new Date(run + h * 3.6e6).toISOString() })); }

test('ribbonFormatter: day / tick / clock texts in a zone; an unknown zone reads as UTC; midnight is "00" and "12 AM"', () => {
  const f = I.ribbonFormatter('Pacific/Honolulu'), t = Date.parse('2026-10-11T17:00:00Z');                 // 7 AM HST
  assert.equal(f.zone, 'Pacific/Honolulu');
  assert.equal(f.day(t), 'Oct 11'); assert.equal(f.clock(t), '7 AM'); assert.equal(f.tick(t), '07');
  assert.deepEqual(f.parts(t), { y: 2026, mo: 'Oct', d: 11, h: 7, mi: 0, wd: 'Sun' });
  assert.deepEqual(f.stamp(t), { date: 'Sun, Oct 11', clock: '7 AM', weekday: 'Sun', weekdayName: 'Sunday', day: 'Oct 11' });
  // the weekday turns over at the LOCAL midnight: 23:59 HST Saturday, 00:00 HST Sunday
  assert.equal(f.stamp(Date.parse('2026-10-11T09:59:00Z')).date, 'Sat, Oct 10'); assert.equal(f.stamp(Date.parse('2026-10-11T10:00:00Z')).date, 'Sun, Oct 11');
  assert.equal(I.ribbonFormatter('UTC').stamp(Date.parse('2026-10-11T09:59:00Z')).date, 'Sun, Oct 11', 'the same instant is already Sunday in UTC');
  assert.equal(f.stamp(Date.parse('2026-10-31T12:00:00Z')).date, 'Sat, Oct 31'); assert.equal(f.stamp(Date.parse('2026-11-01T12:00:00Z')).date, 'Sun, Nov 1');
  // without Intl (very old engines) the formatter falls back to UTC by hand, weekday included, and never throws
  const DTF = Intl.DateTimeFormat;
  try {
    Intl.DateTimeFormat = function () { throw new RangeError('no Intl'); };
    const u = I.ribbonFormatter('No/IntlZone');
    assert.equal(u.zone, 'UTC'); assert.deepEqual(u.parts(t), { y: 2026, mo: 'Oct', d: 11, h: 17, mi: 0, wd: 'Sun' });
    assert.equal(u.stamp(Date.parse('2026-10-13T01:00:00Z')).date, 'Tue, Oct 13'); assert.equal(u.clock(t), '5 PM');
  } finally { Intl.DateTimeFormat = DTF; }
  const mid = Date.parse('2026-10-11T10:00:00Z');                                                            // midnight HST
  assert.equal(f.tick(mid), '00'); assert.equal(f.clock(mid), '12 AM'); assert.equal(f.clock(mid + 12 * 3.6e6), '12 PM');
  assert.equal(f.clock(Date.parse('2026-10-11T05:30:00Z')), '7:30 PM', 'minutes only when not on the hour');
  const u = I.ribbonFormatter('Not/AZone');
  assert.equal(u.zone, 'UTC'); assert.equal(u.day(t), 'Oct 11'); assert.equal(u.clock(t), '5 PM');
  assert.equal(I.ribbonFormatter(undefined).zone, 'UTC');
  const k = I.ribbonFormatter('Asia/Kolkata');                                                               // a half-hour zone
  assert.deepEqual(k.parts(Date.parse('2026-10-11T18:30:00Z')), { y: 2026, mo: 'Oct', d: 12, h: 0, mi: 0, wd: 'Mon' });
});

test('localMidnightBefore: plain days, both clock changes of New York, a half-hour zone', () => {
  const H = I.ribbonFormatter('Pacific/Honolulu'), NY = I.ribbonFormatter('America/New_York'), K = I.ribbonFormatter('Asia/Kolkata');
  assert.equal(I.localMidnightBefore(Date.parse('2026-10-11T17:00:00Z'), H), Date.parse('2026-10-11T10:00:00Z'));
  assert.equal(I.localMidnightBefore(Date.parse('2026-10-11T10:00:00Z'), H), Date.parse('2026-10-11T10:00:00Z'), 'midnight itself');
  assert.equal(I.localMidnightBefore(Date.parse('2026-10-11T09:59:00Z'), H), Date.parse('2026-10-10T10:00:00Z'));
  // 2026-03-08: 2 AM EST -> 3 AM EDT (07:00Z). A frame at 5 AM EDT is 09:00Z; midnight was 05:00Z (EST): a 23-hour day.
  assert.equal(I.localMidnightBefore(Date.parse('2026-03-08T09:00:00Z'), NY), Date.parse('2026-03-08T05:00:00Z'));
  // 2026-11-01: 2 AM EDT -> 1 AM EST (06:00Z). A frame at 5 AM EST is 10:00Z; midnight was 04:00Z (EDT): a 25-hour day.
  assert.equal(I.localMidnightBefore(Date.parse('2026-11-01T10:00:00Z'), NY), Date.parse('2026-11-01T04:00:00Z'));
  assert.equal(I.localMidnightBefore(Date.parse('2026-11-01T06:30:00Z'), NY), Date.parse('2026-11-01T04:00:00Z'), 'inside the repeated hour');
  assert.equal(I.localMidnightBefore(Date.parse('2026-10-11T20:00:00Z'), K), Date.parse('2026-10-11T18:30:00Z'), '1:30 AM Oct 12 IST: midnight was 18:30Z');
  for (const [ms, f] of [[Date.parse('2026-03-08T09:00:00Z'), NY], [Date.parse('2026-11-01T10:00:00Z'), NY], [Date.parse('2026-10-11T20:00:00Z'), K]]) {
    const p = f.parts(I.localMidnightBefore(ms, f)); assert.equal(p.h, 0); assert.equal(p.mi, 0);
  }
});

test('ribbonLayout: positions from the frames\' own times (3-hourly frames three times as far apart), the span, the width', () => {
  const L = I.ribbonLayout(framesAt('2026-10-07T06:00:00Z', HOURS_209), '2026-10-07T06:00:00Z', 'Pacific/Honolulu', 4.5);
  assert.equal(L.xs.length, 209); assert.equal(L.xs[0], 0); assert.equal(L.xs[1], 4.5); assert.equal(L.xs[120], 540);
  assert.equal(L.xs[121] - L.xs[120], 13.5); assert.equal(L.width, 384 * 4.5); assert.equal(L.hours[208], 384); assert.equal(L.hours[0], 0);
  assert.equal(L.spanHours, 384); assert.equal(L.every, 6);
  const O = I.ribbonLayout(framesAt('2026-09-22T12:00:00Z', HOURS_81), '2026-09-22T12:00:00Z', 'UTC', 4.5);
  assert.equal(O.xs[1], 13.5); assert.equal(O.width, 240 * 4.5); assert.equal(O.spanHours, 240);
  assert.equal(I.forecastSpanText, undefined, 'the run-length line is gone (owner, 2026-10-07)');
  // frames that do not start at the run (a trimmed run): hours count from the run, xs from the first frame
  const Tr = I.ribbonLayout(framesAt('2026-10-07T06:00:00Z', [6, 9, 12]), '2026-10-07T06:00:00Z', 'UTC', 4);
  assert.deepEqual(Tr.hours, [6, 9, 12]); assert.deepEqual(Tr.xs, [0, 12, 24]); assert.equal(Tr.spanHours, 6);
});

test('ribbonLayout: day labels at the local midnights across a month boundary, the first day pinned to x 0, ticks only inside the run', () => {
  // run 2026-09-29 12Z = 2 AM HST Sep 29: the ribbon starts inside Sep 29; the next midnights are Sep 30, Oct 1, ...
  const L = I.ribbonLayout(framesAt('2026-09-29T12:00:00Z', HOURS_209), '2026-09-29T12:00:00Z', 'Pacific/Honolulu', 4.5);
  const texts = L.days.map((d) => d.text);
  assert.deepEqual(texts.slice(0, 4), ['Sep 29', 'Sep 30', 'Oct 1', 'Oct 2']);
  assert.deepEqual(L.days[0], { x: 0, text: 'Sep 29', ms: Date.parse('2026-09-29T10:00:00Z'), clamped: true });
  assert.equal(L.days[1].x, 22 * 4.5, 'Sep 30 midnight is 22 h after the 2 AM start'); assert.equal(L.days[1].clamped, false);
  for (let i = 2; i < L.days.length; i++) assert.equal(L.days[i].x - L.days[i - 1].x, 24 * 4.5, 'consecutive days a whole day apart (no clock change in Hawaii)');
  assert.equal(L.days[L.days.length - 1].text, 'Oct 15'); assert.equal(texts.length, 17);
  assert.ok(L.ticks.every((t) => t.x >= 0 && t.x <= L.width), 'no tick outside the run');
  assert.ok(L.ticks.every((t) => ['00', '06', '12', '18'].includes(t.text)));
  assert.ok(L.ticks.every((t) => t.major === (t.text === '00')));
  assert.equal(L.ticks[0].text, '06'); assert.equal(L.ticks[0].x, 4 * 4.5, 'the first tick after a 2 AM start is 6 AM, 4 h in');
  assert.equal(L.ticks.filter((t) => t.text === '00').length, 16, 'one midnight tick per full day boundary inside the run');
  // a run starting right at a local midnight: the first day label is not clamped and sits at x 0
  const M = I.ribbonLayout(framesAt('2026-10-07T10:00:00Z', HOURS_81), '2026-10-07T10:00:00Z', 'Pacific/Honolulu', 4);
  assert.deepEqual(M.days[0], { x: 0, text: 'Oct 7', ms: Date.parse('2026-10-07T10:00:00Z'), clamped: false });
  assert.equal(M.ticks[0].text, '00');
  // a run starting at 11 PM local: the pinned "Oct 6" would sit on top of "Oct 7" one hour later, so it is dropped
  const N = I.ribbonLayout(framesAt('2026-10-07T09:00:00Z', HOURS_81), '2026-10-07T09:00:00Z', 'Pacific/Honolulu', 4);
  assert.equal(N.days[0].text, 'Oct 7'); assert.equal(N.days[0].x, 4); assert.equal(N.days[0].clamped, false);
});

test('ribbonLayout: the pinned first-day label goes when the next midnight is under 60 px away (two-digit dates never touch)', () => {
  assert.equal(I.RIBBON_DAY_MIN_PX, 60);
  // UTC zone, a 12Z run at 3.86 px/h: midnight 46 px in; "Oct 12" pinned at x 0 (3 px in, ~33 px wide) would touch the
  // centred "Oct 13" (step-4 finding F3)
  const U = I.ribbonLayout(framesAt('2026-10-12T12:00:00Z', HOURS_81), '2026-10-12T12:00:00Z', 'UTC', 3.86);
  assert.deepEqual([U.days[0].text, U.days[0].clamped], ['Oct 13', false]);
  // 15 h before midnight = 57.9 px: dropped; 16 h = 61.8 px: kept
  const a = I.ribbonLayout(framesAt('2026-10-12T09:00:00Z', HOURS_81), '2026-10-12T09:00:00Z', 'UTC', 3.86);
  assert.equal(a.days[0].text, 'Oct 13');
  const b = I.ribbonLayout(framesAt('2026-10-12T08:00:00Z', HOURS_81), '2026-10-12T08:00:00Z', 'UTC', 3.86);
  assert.deepEqual([b.days[0].text, b.days[0].clamped, b.days[1].text], ['Oct 12', true, 'Oct 13']);
});

test('ribbonLayout: a clock change gives a 23-hour and a 25-hour day, with the ticks still on the local clock hours', () => {
  const NY = 'America/New_York';
  // spring forward 2026-03-08: run 2026-03-07 00Z (7 PM EST Mar 6)
  const S = I.ribbonLayout(framesAt('2026-03-07T00:00:00Z', HOURS_209), '2026-03-07T00:00:00Z', NY, 4);
  const sd = S.days.map((d) => d.text), i8 = sd.indexOf('Mar 8');
  assert.equal(S.days[i8 + 1].x - S.days[i8].x, 23 * 4, 'Mar 8 is 23 hours wide');
  assert.equal(S.days[i8 + 2].x - S.days[i8 + 1].x, 24 * 4);
  const fmt = I.ribbonFormatter(NY);
  for (const t of S.ticks) { const p = fmt.parts(t.ms); assert.equal(p.mi, 0); assert.equal(p.h % 6, 0); assert.equal(t.text, (p.h < 10 ? '0' : '') + p.h); }
  const mar8 = S.ticks.filter((t) => fmt.parts(t.ms).d === 8 && fmt.parts(t.ms).mo === 'Mar').map((t) => t.text);
  assert.deepEqual(mar8, ['00', '06', '12', '18']);
  // fall back 2026-11-01: run 2026-10-31 00Z
  const F = I.ribbonLayout(framesAt('2026-10-31T00:00:00Z', HOURS_209), '2026-10-31T00:00:00Z', NY, 4);
  const fd = F.days.map((d) => d.text), i1 = fd.indexOf('Nov 1');
  assert.equal(F.days[i1 + 1].x - F.days[i1].x, 25 * 4, 'Nov 1 is 25 hours wide');
  const nov1 = F.ticks.filter((t) => fmt.parts(t.ms).d === 1 && fmt.parts(t.ms).mo === 'Nov');
  assert.deepEqual(nov1.map((t) => t.text), ['00', '06', '12', '18']);
  assert.equal(nov1[1].x - nov1[0].x, 7 * 4, '6 AM EST is 7 elapsed hours after midnight EDT');
  // a half-hour zone: ticks on the local hours (which fall on :30 UTC), never between
  const K = I.ribbonLayout(framesAt('2026-10-07T06:00:00Z', HOURS_81), '2026-10-07T06:00:00Z', 'Asia/Kolkata', 4);
  const kf = I.ribbonFormatter('Asia/Kolkata');
  assert.ok(K.ticks.length > 30); assert.ok(K.ticks.every((t) => kf.parts(t.ms).mi === 0 && kf.parts(t.ms).h % 6 === 0));
  assert.equal(kf.parts(K.days[1].ms).h, 0);
  // a HALF-HOUR clock change (Lord Howe Island, 2026-10-04 2:00 -> 2:30): the walk gets back onto the local hours, so the
  // days and ticks after the change are still labelled, all on the clock hour
  const LH = 'Australia/Lord_Howe', lf = I.ribbonFormatter(LH);
  const H = I.ribbonLayout(framesAt('2026-10-03T00:00:00Z', HOURS_209), '2026-10-03T00:00:00Z', LH, 4);
  const hd = H.days.map((d) => d.text);
  assert.ok(hd.includes('Oct 4') && hd.includes('Oct 6') && hd.includes('Oct 12'), hd.join());
  assert.ok(H.days.every((d) => { const p = lf.parts(d.ms); return p.h === 0 && p.mi === 0; }));
  assert.ok(H.ticks.every((t) => { const p = lf.parts(t.ms); return p.mi === 0 && p.h % 6 === 0; }));
  const i4 = hd.indexOf('Oct 4'); assert.equal(H.days[i4 + 1].x - H.days[i4].x, 23.5 * 4, 'the change-over day is 23.5 hours wide');
  assert.ok(H.ticks.filter((t) => t.ms > H.days[i4 + 1].ms).length > 30, 'ticks go on after the change');
});

test('ribbonLayout / ribbonScale: tick density follows the scale; the scale follows the viewport within 3-5 px per hour', () => {
  const frames = framesAt('2026-10-07T06:00:00Z', HOURS_81);
  assert.equal(I.ribbonLayout(frames, '2026-10-07T06:00:00Z', 'UTC', 4.5).every, 6);
  const L3 = I.ribbonLayout(frames, '2026-10-07T06:00:00Z', 'UTC', 2.5);
  assert.equal(L3.every, 12); assert.ok(L3.ticks.every((t) => t.text === '00' || t.text === '12'));
  assert.equal(I.ribbonLayout(frames, '2026-10-07T06:00:00Z', 'UTC', 1.2).every, 24);
  assert.equal(I.ribbonLayout(frames, '2026-10-07T06:00:00Z', 'UTC', 2.5, 10).every, 6, 'a smaller gap keeps every tick');
  assert.equal(I.ribbonLayout(frames, '2026-10-07T06:00:00Z', 'UTC', 18 / 6).every, 6, 'exactly the gap still fits');
  assert.equal(I.ribbonLayout(frames, '2026-10-07T06:00:00Z', 'UTC', 18 / 6 - 0.01).every, 12);
  assert.equal(I.RIBBON_MIN_TICK_GAP, 18);
  assert.equal(I.ribbonScale(324), 4.5); assert.ok(Math.abs(I.ribbonScale(253) - 3.514) < 0.001);
  assert.equal(I.ribbonScale(10), 3); assert.equal(I.ribbonScale(1000), 5); assert.equal(I.ribbonScale(0), I.ribbonScale(300)); assert.equal(I.ribbonScale(undefined), I.ribbonScale(300));
});

test('RibbonState: nearest frame (ties later), clamping, a drag follows the pointer one to one, a tap picks under the finger, wheel', () => {
  const L = I.ribbonLayout(framesAt('2026-10-07T06:00:00Z', HOURS_209), '2026-10-07T06:00:00Z', 'UTC', 4.5);
  const rb = new I.RibbonState(L.xs);
  assert.equal(rb.width, 384 * 4.5); assert.equal(rb.offset, 0);
  assert.equal(rb.nearest(L.xs[120] + 6), 120); assert.equal(rb.nearest(L.xs[120] + 7), 121);
  assert.equal(rb.nearest(L.xs[120] + 6.75), 121, 'a tie goes to the later frame');
  assert.equal(rb.nearest(-50), 0); assert.equal(rb.nearest(1e9), 208);
  assert.equal(rb.clamp(-5), 0); assert.equal(rb.clamp(rb.width + 1), rb.width); assert.equal(rb.clamp(7), 7);
  rb.setFrame(10); assert.equal(rb.offset, 45); rb.setFrame(-3); assert.equal(rb.offset, 0); rb.setFrame(999); assert.equal(rb.offset, rb.width);
  // a drag: the ribbon moves with the pointer, so dragging LEFT shows later times
  rb.setFrame(10); rb.begin(100);
  assert.equal(rb.move(90), rb.nearest(55)); assert.equal(rb.offset, 55); assert.equal(rb.dragging, true);
  assert.equal(rb.move(300), 0); assert.equal(rb.offset, 0, 'clamped at the first frame');
  rb.move(-10000); assert.equal(rb.offset, rb.width, 'clamped at the last frame');
  rb.move(96); const idx = rb.end(); assert.equal(rb.dragging, false); assert.equal(rb.offset, L.xs[idx]); assert.equal(idx, rb.nearest(49));
  assert.equal(rb.move(5), null, 'no drag in progress');
  // a tap (under 4 px of travel) picks the frame under the finger: 27 px right of the pointer = 6 h later at 4.5 px/h
  rb.setFrame(10); rb.begin(100); rb.move(102); assert.equal(rb.end(27), 16); assert.equal(rb.offset, L.xs[16]);
  rb.setFrame(10); rb.begin(100); rb.move(102); assert.equal(rb.end(-27), 4, 'left of the pointer = earlier');
  rb.setFrame(0); rb.begin(100); assert.equal(rb.end(-27), 0, 'a tap before the first frame clamps');
  rb.setFrame(10); rb.begin(100); rb.move(130); assert.equal(rb.end(27), rb.nearest(45 - 30), 'a real drag ignores the tap position');
  assert.equal(I.RIBBON_TAP_PX, 4);
  // monotone: a slow 1-px forward drag through the 3-hourly part never steps back, backward never forward (G13b's property)
  rb.setFrame(118); rb.begin(0); let prev = -1;
  for (let px = 0; px >= -150; px--) { const i = rb.move(px); assert.ok(i >= prev, 'forward drag stepped back at ' + px); prev = i; }
  assert.equal(prev, rb.nearest(L.xs[118] + 150));
  rb.end(); rb.setFrame(141); rb.begin(0); prev = 1e9;
  for (let px = 0; px <= 150; px++) { const i = rb.move(px); assert.ok(i <= prev, 'backward drag jumped forward at ' + px); prev = i; }
  rb.end();
  // wheel: 13.5 px = one 3-hourly frame at 4.5 px/h, clamped at the ends
  rb.setFrame(130); assert.equal(rb.wheel(13.5), 131); assert.equal(rb.offset, L.xs[131]); assert.equal(rb.wheel(-1e6), 0); assert.equal(rb.offset, 0);
  const one = new I.RibbonState([0]); assert.equal(one.nearest(100), 0); assert.equal(one.width, 0); one.begin(0); assert.equal(one.end(5), 0);
});

test('runTimes / localClock: the Updated and Next Update parts in the computer time zone (weekday only when not today; rounded up to 5 min)', () => {
  const tz0 = process.env.TZ;
  const m = { run_utc: '2026-09-25T18:00:00Z', published_utc: '2026-09-25T23:07:18Z' };        // live 23:07Z; next ~05:10Z
  try {
    process.env.TZ = 'Pacific/Honolulu';                                                     // 1:07 PM HST on Sep 25
    let now = Date.parse('2026-09-26T00:00:00Z');                                           // 2 PM HST, same day
    let t = I.runTimes(m, now, false);
    assert.equal(t.status, 'about'); assert.equal(t.next, Date.parse('2026-09-26T05:10:00Z'));
    assert.match(t.updated, /^1:07\sPM HST$/); assert.match(t.nextText, /^about 7:10\sPM HST$/); assert.equal(t.text, undefined);
    process.env.TZ = 'America/New_York';                                                     // 7:07 PM EDT; next 1:10 AM EDT tomorrow
    t = I.runTimes(m, now, false);
    assert.match(t.updated, /^7:07\sPM EDT$/); assert.match(t.nextText, /^about Sat,? 1:10\sAM EDT$/);
    process.env.TZ = 'Asia/Kolkata';                                                         // a half-hour zone: 4:37 AM on the same local day as now (5:30 AM)
    t = I.runTimes(m, now, false);
    assert.match(t.updated, /^4:37\sAM (GMT\+5:30|IST)$/); assert.match(t.nextText, /^about 10:40\sAM (GMT\+5:30|IST)$/);
    process.env.TZ = 'UTC';
    t = I.runTimes(m, now, false);
    assert.match(t.updated, /^Fri,? 11:07\sPM UTC$/); assert.match(t.nextText, /^about 5:10\sAM UTC$/);
    now = Date.parse('2026-09-26T05:30:00Z');
    assert.equal(I.runTimes(m, now, false).status, 'shortly'); assert.equal(I.runTimes(m, now, false).nextText, 'expected shortly');
    assert.equal(I.runTimes(m, now, true).status, 'newer'); assert.equal(I.runTimes(m, now, true).nextText, 'a newer run is available');
    assert.equal(I.runTimes({ run_utc: m.run_utc }, now, false), null, 'no publish time: no line');
    assert.equal(I.runTimes({ run_utc: m.run_utc, published_utc: '2026-09-25T17:00:00Z' }, now, false), null, 'published before its cycle: no line');
  } finally { if (tz0 === undefined) delete process.env.TZ; else process.env.TZ = tz0; }
});

test('G13b P1-1: a slow drag in either direction never picks a frame against its direction; keys compare with the shown thumb', () => {
  const hours = [...Array(121).keys()].concat([...Array(88).keys()].map((i) => 123 + 3 * i));
  const tl = new I.TimelineState(); tl.shown = 118;
  tl.start();
  let prev = -1;
  for (let v = 118; v <= 140; v++) { const h = hours[tl.pick(hours, v)]; assert.ok(h >= prev, 'forward drag stepped back to +' + h + ' at ' + v); prev = h; }
  assert.equal(prev, 141, 'a forward drag ending at 140 lands on the next frame, +141');
  tl.end(); tl.shown = 141; tl.start(); prev = 1e9;
  for (let v = 141; v >= 110; v--) { const h = hours[tl.pick(hours, v)]; assert.ok(h <= prev, 'backward drag jumped forward to +' + h + ' at ' + v); prev = h; }
  assert.equal(prev, 110); tl.end();
  tl.shown = 123; assert.equal(hours[tl.pick(hours, 124)], 126, 'ArrowRight from +123'); assert.equal(hours[tl.pick(hours, 125)], 123, 'ArrowLeft from +126');
  tl.shown = 150; assert.equal(hours[tl.pick(hours, 151)], 153, 'keys go from the thumb as shown');
  const old = [...Array(81).keys()].map((i) => 3 * i); const t2 = new I.TimelineState(); t2.shown = 117; t2.start(); prev = -1;
  for (let v = 117; v <= 130; v++) { const h = old[t2.pick(old, v)]; assert.ok(h >= prev); prev = h; }
});

test('plan section 27: a run whose legend is 0-60 ft (18.288 m) gets the wider scale; any other legend keeps the 40-ft scale', () => {
  const L60 = [0, 18.288], L12 = [0, 12];
  const col = (lut, i) => [lut[i * 3], lut[i * 3 + 1], lut[i * 3 + 2]];
  const at = (legend, v) => col(I.buildLut('hs', legend), Math.max(0, Math.min(255, Math.round(v / legend[1] * 255))));
  assert.ok(I.scaleFor('hs', L60)); assert.equal(I.scaleFor('hs', L12), null); assert.equal(I.scaleFor('hs', [0, 20]), null); assert.equal(I.scaleFor('tp', L60), null);
  // the knots: 0-30 ft keep their colours, the top goes to 40 / 50 / 60 ft
  assert.equal(I.legendPos('hs', L60, 18.288), 1); assert.equal(I.legendPos('hs', L60, 12), 0.86); assert.equal(I.legendPos('hs', L60, 15.24), 0.93);
  assert.equal(I.legendPos('hs', L12, 12), 1, 'a 40-ft run is unchanged');
  for (const v of [1, 3, 6, 9]) {
    const a = at(L60, v), b = at(L12, v);
    for (let c = 0; c < 3; c++) assert.ok(Math.abs(a[c] - b[c]) <= 10, `${v} m keeps its colour on the wider scale (channel ${c}: ${a[c]} vs ${b[c]})`);
  }
  assert.deepEqual(at(L60, 18.288), [76, 29, 149], '60 ft: deep purple (C3)');
  const hex = (h) => [parseInt(h.slice(1, 3), 16), parseInt(h.slice(3, 5), 16), parseInt(h.slice(5, 7), 16)];
  const near = (a, b, tol) => a.every((x, i) => Math.abs(x - b[i]) <= tol);
  assert.ok(near(at(L60, 15.24), hex('#be185d'), 12), '50 ft: rose ' + at(L60, 15.24));
  assert.ok(near(at(L60, 12), hex('#f5d0fe'), 12), '40 ft: pale pink ' + at(L60, 12));
  const d = (a, b) => Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]);
  assert.ok(d(at(L60, 12), at(L60, 15.24)) > 120 && d(at(L60, 15.24), at(L60, 18.288)) > 80, '40, 50 and 60 ft are clearly different colours');
  // the bar and the ticks
  assert.deepEqual(I.legendBar('hs', L60).slice(255 * 3), new Uint8ClampedArray([76, 29, 149]));
  assert.deepEqual(Array.from(I.legendBar('hs', L12)), Array.from(I.legendBar('hs')), 'the 40-ft bar is today\'s');
  const fdef = { lo: 0, hi: 22.86, legend: L60 };
  const us = I.legendTicks('hs', fdef, 'US'), mt = I.legendTicks('hs', fdef, 'Metric');
  assert.deepEqual(us.map((t) => t.label), ['0', '3', '6', '10', '15', '20', '30', '60+ ft']);
  assert.deepEqual(mt.map((t) => t.label), ['0', '1', '2', '3', '4', '6', '9', '18+ m']);
  assert.ok(us[us.length - 2].pos <= 0.8 && mt[mt.length - 2].pos <= 0.8, 'no numeric tick under the top label');
  for (const v of [0, 5, 10, 15, 18.288]) assert.ok(Math.abs(I.legendInv('hs', L60, I.legendPos('hs', L60, v)) - v) < 1e-9);
});

test('the speed selector\'s table: snail 1x, fish 2x, shark 4x; an older or odd saved speed plays at 1x; asset URLs carry the version', () => {
  assert.deepEqual(I.SPEED_ANIMALS.map((a) => [a.speed, a.name, a.file, a.title]),
    [[1, 'Snail', 'snail.png', 'Snail: 1\u00d7 speed'], [2, 'Fish', 'fish.png', 'Fish: 2\u00d7 speed'], [4, 'Shark', 'shark.png', 'Shark: 4\u00d7 speed']]);
  assert.deepEqual(I.SPEED_ANIMALS.map((a) => a.speed), Array.from(I.SPEEDS));
  assert.equal(I.speedOf(0.5), 1); assert.equal(I.speedOf(2), 2); assert.equal(I.speedOf(4), 4); assert.equal(I.speedOf('4'), 1);
  assert.equal(I.speedOf(undefined), 1); assert.equal(I.speedOf(3), 1);
  assert.equal(I.animalOf(4).name, 'Shark'); assert.equal(I.animalOf(2).name, 'Fish'); assert.equal(I.animalOf(0.5).name, 'Snail');
  assert.equal(I.assetUrl({ version: '2.14.0' }, 'fish.png'), '/overlay/fish.png?v=2.14.0');
  assert.equal(I.assetUrl({ version: 'a b' }, 'fish.png'), '/overlay/fish.png?v=a%20b');
  assert.equal(I.assetUrl({}, 'snail.png'), '/overlay/snail.png', 'no version known (Node): a bare path');
  assert.equal(I.SHEET_OPEN_PX, 110);
});
