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
  assert.deepEqual(labels('hs', HS, 'US'), ['0', '10', '20', '30', '39+ ft']);
  assert.deepEqual(labels('hs', HS, 'Metric'), ['0', '3', '6', '9', '12+ m']);
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
  const lut = I.buildRamp(I.RAMPS.hs);
  assert.deepEqual([lut[0], lut[1], lut[2]], [0x0b, 0x2c, 0x6b]);
  assert.deepEqual([lut[255 * 3], lut[255 * 3 + 1], lut[255 * 3 + 2]], [0xa3, 0x12, 0x9e]);
});

test('ringPlan: current, two ahead in the play direction, two behind, wrapping, no duplicates', () => {
  assert.deepEqual(Array.from(I.ringPlan(10, 81, 1)), [10, 11, 12, 9, 8]);
  assert.deepEqual(Array.from(I.ringPlan(80, 81, 1)), [80, 0, 1, 79, 78]);
  assert.deepEqual(Array.from(I.ringPlan(0, 81, -1)), [0, 80, 79, 1, 2]);
  assert.deepEqual(Array.from(I.ringPlan(0, 3, 1)), [0, 1, 2]);
  assert.deepEqual(Array.from(I.ringPlan(0, 1, 1)), [0]);
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
  assert.deepEqual(Array.from(I.SPEEDS), [0.5, 1, 2, 4]); assert.equal(I.BASE_FPS, 2);
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
