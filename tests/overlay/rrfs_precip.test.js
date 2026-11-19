'use strict';
// RRFS precipitation (plan section 102 A) for static_overlay/overlay.js, no browser: the code table (exactly the job's,
// tools/model_frames/rrfs.py PRECIP_CODES), the manifest's precip field (optional, exact, wind kept when it is not),
// the typed regional sampler against an independent per-point reference (dominant type, never a blend of two types,
// dry vs missing, region priority, the date line), the colours (the radar's legend through Marshall-Palmer, the dry
// tint, the alpha ramp), the layer (tiles and readout = the drawn pixel), the readout's words in both units, the gating.
//   node --test tests/overlay/rrfs_precip.test.js
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
const FIX = path.join(__dirname, '..', 'fixtures', 'rrfs_frames');
const MAN = () => JSON.parse(fs.readFileSync(path.join(FIX, 'manifest.json'), 'utf8'));
const PREFIX = 'rrfs/v1/frames';
const TILE = 256;
const PDEF = () => ({ kind: 'typed', encoding: 'rrfs-precip-v1', unit: 'mm', lo: 0.1, hi: 75, per_type: 50, dry: 1,
  types: { rain: [2, 51], snow: [52, 101], mix: [102, 151], ice: [152, 201], thunder: [202, 251] }, period: 'the hour ending at the frame’s time', resolutions: ['full'] });
// A manifest as the job writes it once the sources carry precip: every region names the field, every step after f000 a key.
function withPrecip(m) {
  for (const r of Object.keys(m.regions)) m.regions[r].fields.precip = PDEF();
  for (const fr of m.frames) if (fr.step > 0) { fr.precip = {}; for (const r of Object.keys(fr.files)) fr.precip[r] = fr.files[r].wind.replace('/wind/', '/precip/'); }
  return m;
}

// ---------------------------------------------------------------------------------------- an independent reference

const TYPES = ['rain', 'snow', 'mix', 'ice', 'thunder'];
function lvlOf(c) { return c >= 2 && c <= 251 ? (c - 2) % 50 : -1; }   // the level of a wet code; -1 otherwise
function typeOf(c) { return Math.floor((c - 2) / 50); }
// The typed value at (lat, lon) of ONE region, written from the rule in the plan, not from the client's code.
function refOne(frame, g, lat, lon) {
  const lonR = g.lon0 + ((((lon - g.lon0) % 360) + 360) % 360);
  const fy = (g.lat0 - lat) / -g.dlat, fx = (lonR - g.lon0) / g.dlon;
  if (!(fy >= 0 && fy <= g.rows - 1 && fx >= 0 && fx <= g.cols - 1)) return 0;
  const r0 = Math.floor(fy), r1 = Math.min(r0 + 1, g.rows - 1), c0 = Math.floor(fx), c1 = Math.min(c0 + 1, g.cols - 1);
  const wy = fy - r0, wx = fx - c0;
  const nodes = [[r0, c0, (1 - wy) * (1 - wx)], [r0, c1, (1 - wy) * wx], [r1, c0, wy * (1 - wx)], [r1, c1, wy * wx]];
  let cov = 0, w = 0, acc = 0, best = -1, bt = 0;
  for (const [r, c, f] of nodes) {
    const q = frame.q[r * g.cols + c];
    if (q) cov += f;
    if (lvlOf(q) >= 0) { w += f; acc += f * lvlOf(q); if (f > best) { best = f; bt = typeOf(q); } }
  }
  if (w >= 0.25) return 2 + 50 * bt + acc / w;
  return cov >= 0.25 ? 1 : 0;
}
function refAll(parts, lat, lon) { for (const p of parts) { const v = refOne(p.frame, p.grid, lat, lon); if (v) return v; } return 0; }
function tilePoint(coords, px, py) {
  const n = TILE * Math.pow(2, coords.z);
  return [I.latOfWorldY(coords.y * TILE + py + 0.5, n), I.lngOfWorldX(coords.x * TILE + px + 0.5, n)];
}

// A synthetic storm on a 0.05-degree grid (the CONUS spacing): rain everywhere wet, a snow band, a thunder core, an ice
// patch, dry ground around and a hole of missing nodes; levels vary along the row.
function storm(lat0, lon0, rows, cols) {
  const g = { cols, rows, lat0, lon0, dlat: -0.05, dlon: 0.05, registration: 'center', lon_periodic: false };
  const q = new Uint8Array(rows * cols);
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) {
    const i = r * cols + c, d = Math.hypot(r - rows / 2, c - cols / 2);
    if (r >= 10 && r < 16 && c >= 40 && c < 46) { q[i] = 0; continue; }       // a hole of missing nodes inside the rain (in the tested tile)
    if (d > rows / 2.5) { q[i] = 1; continue; }                                 // dry
    const lvl = Math.max(0, Math.min(49, Math.round(49 - d * 2)));
    let t = 0;
    if (r < rows / 3) t = 1;                                                   // snow to the north
    if (d < 3) t = 4;                                                          // thunder core
    if (r > rows * 0.6 && c > cols * 0.6) t = 3;                               // ice patch
    if (r === Math.floor(rows / 3)) t = 2;                                      // a mix line
    q[i] = 2 + 50 * t + lvl;
  }
  return { grid: g, frame: { q, cols, rows } };
}

// ------------------------------------------------------------------------------------------------- the code table

test('the code table is the job’s (rrfs.py PRECIP_CODES) and the levels are the log scale 0.1-75 mm', () => {
  assert.deepEqual(I.PRECIP_CODES, { rain: [2, 51], snow: [52, 101], mix: [102, 151], ice: [152, 201], thunder: [202, 251] });
  assert.deepEqual(I.PRECIP_TYPES, TYPES);
  assert.ok(Math.abs(I.precipMm(0) - 0.1) < 1e-12 && Math.abs(I.precipMm(49) - 75) < 1e-9);
  assert.ok(Math.abs(I.precipMm(1) / I.precipMm(0) - Math.pow(750, 1 / 49)) < 1e-12);
  for (let c = 0; c < 256; c++) {
    const wet = c >= 2 && c <= 251;
    assert.equal(I.PRECIP_K[c], wet ? (c - 2) % 50 + 1 : 0, 'level of ' + c);
    if (wet) assert.equal(I.PRECIP_T[c], Math.floor((c - 2) / 50));
  }
  assert.equal(I.precipOf(0), null); assert.equal(I.precipOf(0.5), null);
  assert.deepEqual(I.precipOf(1), { mm: 0, type: null });
  const p = I.precipOf(2 + 50 * 3 + 24.5);
  assert.equal(p.type, 'ice'); assert.ok(Math.abs(p.mm - I.precipMm(24.5)) < 1e-12);
  assert.equal(I.precipOf(251).type, 'thunder'); assert.ok(Math.abs(I.precipOf(251).mm - 75) < 1e-9);
});

test('the manifest: precip is optional and exact; unreadable precip keeps the wind; keys of precip are accepted', () => {
  const plain = I.validateRegionalManifest(MAN(), PREFIX);
  assert.equal(plain.fields.precip, undefined); assert.equal(plain.precipBad, false);
  const m = I.validateRegionalManifest(withPrecip(MAN()), PREFIX);
  assert.deepEqual(m.fields.precip.legend, [0.1, 75]); assert.equal(m.fields.precip.kind, 'typed');
  assert.equal(I.regionFrameKey(m.frames[1], 'hi', 'precip'), 'rrfs/v1/frames/2026111718/hi/precip/f001.png');
  assert.equal(I.regionFrameKey(m.frames[0], 'hi', 'precip'), null, 'a run’s f000 has none');
  const odd = [
    (d) => { d.encoding = 'rrfs-precip-v2'; }, (d) => { d.types.ice = [152, 200]; }, (d) => { d.per_type = 49; },
    (d) => { d.hi = 100; }, (d) => { d.dry = 0; }, (d) => { delete d.types.thunder; }, (d) => { d.types.hail = [252, 255]; },
    (d) => { d.kind = 'linear'; }, (d) => { d.resolutions = ['half']; }, (d) => { d.lo = 0.2; }, (d) => { d.types.rain = [2, 51, 0]; }
  ];
  for (const edit of odd) {
    const mm = withPrecip(MAN());
    edit(mm.regions.ak.fields.precip);
    const v = I.validateRegionalManifest(mm, PREFIX);                       // never refused: the wind is still good
    assert.equal(v.fields.precip, undefined); assert.equal(v.precipBad, true); assert.ok(v.fields.wind);
  }
  const some = MAN();                                                        // only some regions with the field: offered (the others draw nothing)
  some.regions.hi.fields.precip = PDEF();
  assert.ok(I.validateRegionalManifest(some, PREFIX).fields.precip);
  for (const edit of [(fr) => { fr.precip.hi = 'gfswave/0p25/v1/x.png'; }, (fr) => { fr.precip.xx = fr.precip.hi; }, (fr) => { fr.precip = ['x']; },
    (fr) => { fr.precip = null; }, (fr) => { fr.precip.hi = 'rrfs/v1/frames/../../x.png'; }]) {
    const bad = withPrecip(MAN());                                          // a malformed map never costs the wind (G68 P3)
    edit(bad.frames[3]);
    const v = I.validateRegionalManifest(bad, PREFIX);
    assert.equal(v.precipBad, true); assert.equal(v.fields.precip, undefined); assert.ok(v.fields.wind);
  }
  for (const f of ['rain', 'precip']) {                                       // `files` stays wind + direction only (G68a P0-1)
    const unk = MAN();
    unk.frames[3].files.hi[f] = unk.frames[3].files.hi.wind;
    assert.throws(() => I.validateRegionalManifest(unk, PREFIX), /bad frame entry/);
  }
  assert.equal(I.precipDefOk(PDEF()), true);
});

// ------------------------------------------------------------------------------------------------------ the sampler

test('the typed sampler = the independent reference at EVERY pixel of a tile (storm, dry ground, missing nodes)', () => {
  const s = storm(41.0, -125.0, 60, 80);
  const parts = [{ name: 'conus', grid: s.grid, frame: s.frame }];
  const coords = { z: 7, x: 20, y: 48 }, out = new Float64Array(TILE * TILE);   // 38.8-41.0 N, 120.9-123.75 W
  I.sampleRegionsTyped(parts, coords, 0, 0, 1, TILE, out);
  const seen = new Set();
  for (let py = 0; py < TILE; py++) for (let px = 0; px < TILE; px++) {
    const [lat, lon] = tilePoint(coords, px, py), want = refAll(parts, lat, lon), got = out[py * TILE + px];
    assert.ok(Math.abs(got - want) < 1e-9, `${px},${py}: ${got} vs ${want}`);
    seen.add(got === 0 ? 'none' : got === 1 ? 'dry' : TYPES[Math.floor((got - 2) / 50)]);
  }
  for (const k of ['none', 'dry', 'rain', 'snow', 'mix', 'ice', 'thunder']) assert.ok(seen.has(k), 'the tile shows ' + k);
});

test('never a blend of two types: a wet pixel takes the dominant node’s type and a level within its range', () => {
  const g = { cols: 2, rows: 2, lat0: 1, lon0: 0, dlat: -1, dlon: 1, registration: 'center', lon_periodic: false };
  const f = { q: Uint8Array.from([2 + 49, 52 + 0, 202 + 10, 1]), cols: 2, rows: 2 };   // heavy rain, light snow, thunder, dry
  const parts = [{ name: 'x', grid: g, frame: f }];
  const out = new Float64Array(1), at = (lat, lon) => {
    const p = I.pixelOf(lat, lon, 20); out[0] = 0; I.sampleRegionsTyped(parts, p, p.px, p.py, 1, 1, out); return out[0];
  };
  const nearRain = at(0.9, 0.1), nearSnow = at(0.9, 0.9), nearThunder = at(0.1, 0.1), nearDry = at(0.05, 0.95);
  assert.equal(Math.floor((nearRain - 2) / 50), 0); assert.equal(Math.floor((nearSnow - 2) / 50), 1); assert.equal(Math.floor((nearThunder - 2) / 50), 4);
  for (const v of [nearRain, nearSnow, nearThunder]) {
    const t = Math.floor((v - 2) / 50), lvl = v - 2 - 50 * t;
    assert.ok(lvl >= 0 && lvl <= 49, 'a level inside its own type’s range');
  }
  assert.equal(nearDry, 1, 'the dry node dominates its corner: wet weight under a quarter');
});

test('regions: the first with data (dry counts) wins; Alaska across the date line samples one column', () => {
  const a = storm(41.0, -125.0, 20, 20), b = storm(42.0, -126.0, 60, 60);
  a.frame.q.fill(1);                                                       // region a: all dry
  const parts = [{ name: 'a', grid: a.grid, frame: a.frame }, { name: 'b', grid: b.grid, frame: b.frame }];
  const coords = { z: 6, x: 9, y: 23 }, out = new Float64Array(TILE * TILE);
  I.sampleRegionsTyped(parts, coords, 0, 0, 1, TILE, out);
  let dryA = 0, fromB = 0;
  for (let py = 0; py < TILE; py++) for (let px = 0; px < TILE; px++) {
    const [lat, lon] = tilePoint(coords, px, py), v = out[py * TILE + px];
    assert.ok(Math.abs(v - refAll(parts, lat, lon)) < 1e-9);
    if (refOne(a.frame, a.grid, lat, lon)) { assert.equal(v, 1); dryA++; } else if (v) fromB++;
  }
  assert.ok(dryA > 0 && fromB > 0);
  const ak = { cols: 200, rows: 20, lat0: 60, lon0: 170, dlat: -0.07, dlon: 0.07, registration: 'center', lon_periodic: false };
  const akq = new Uint8Array(4000);
  for (let r = 0; r < 20; r++) for (let c = 0; c < 200; c++) akq[r * 200 + c] = 2 + 50 * (c % 5) + (c % 50);
  const ap = [{ name: 'ak', grid: ak, frame: { q: akq, cols: 200, rows: 20 } }];
  const one = (lon) => { const p = I.pixelOf(59.5, lon, 12); const o = new Float64Array(1); I.sampleRegionsTyped(ap, p, p.px, p.py, 1, 1, o); return [o[0], tilePoint(p, p.px, p.py)]; };
  for (const lon of [-178.5, -179.9, 179.9, 175]) {
    const [v, [la, lo]] = one(lon);
    assert.ok(Math.abs(v - refOne(ap[0].frame, ak, la, lo)) < 1e-9 && v > 1, 'Alaska at ' + lon);
  }
  const one0 = (lon) => one(lon)[0];
  assert.equal(one0(160), 0, 'west of the box');
});

// ------------------------------------------------------------------------------------------------------- the colours

test('colours: the radar’s legend through Marshall-Palmer; the dry tint; the alpha ramp; each type its ramp', () => {
  assert.ok(Math.abs(I.precipDbz(1) - 23.0103) < 1e-3 && Math.abs(I.precipDbz(10) - 39.0103) < 1e-3);
  assert.equal(I.precipPos(1e-6), 0); assert.equal(I.precipPos(1000), 1);
  const lut = I.precipLut(), rgb = [0, 0, 0];
  assert.deepEqual(Array.from(lut.slice(8, 12)), I.RADAR_TINT, 'value 1 (dry) = the radar’s coverage tint');
  assert.equal(lut[3], 0); assert.equal(lut[7], 0);
  const ramps = ['radar', 'radarSnow', 'radarMix', 'precipIce', 'radar'];   // thunder drawn as rain (owner, 2026-11-19)
  for (let t = 0; t < 5; t++) for (const lvl of [0, 7, 24, 49]) {
    const v = 2 + 50 * t + lvl, e = Math.round(v * 2) * 4, mm = I.precipMm(lvl);
    I.rampAt(I.RAMPS[ramps[t]], I.precipPos(mm), rgb, 0);
    assert.deepEqual(Array.from(lut.slice(e, e + 3)), rgb.map((x) => Math.max(0, Math.min(255, Math.round(x)))), `${ramps[t]} level ${lvl}`);
    const a = lut[e + 3] / 255, dbz = I.precipDbz(mm);
    const want = Math.max(I.RADAR_ALPHA[0], Math.min(I.RADAR_ALPHA[1], I.RADAR_ALPHA[0] + (dbz - 5) / 20 * (I.RADAR_ALPHA[1] - I.RADAR_ALPHA[0])));
    assert.ok(Math.abs(a - want) < 1 / 255 + 1e-9);
  }
  assert.ok(lut[(Math.round((2 + 49) * 2)) * 4 + 3] === 255 && lut[4 * 4 + 3] < 160, 'drizzle see-through, heavy rain solid');
  const d = new Uint8ClampedArray(5 * 4);
  I.composeTyped(Float64Array.from([0, 1, 2.2, 251, 2.3]), lut, d);
  assert.equal(d[3], 0); assert.deepEqual(Array.from(d.slice(4, 8)), I.RADAR_TINT);
  assert.deepEqual(Array.from(d.slice(8, 12)), Array.from(lut.slice(4 * 4, 4 * 4 + 4)));
  assert.deepEqual(Array.from(d.slice(16, 20)), Array.from(lut.slice(5 * 4, 5 * 4 + 4)), 'a value * 2 ROUNDED (2.3 -> entry 5)');
  // thunder has no colour of its own: a thunder level is exactly the rain level's colour (owner: the rate matters more)
  for (const lvl of [0, 13, 30, 49]) { const a = Math.round((2 + lvl) * 2) * 4, b = Math.round((202 + lvl) * 2) * 4; assert.deepEqual(Array.from(lut.slice(b, b + 4)), Array.from(lut.slice(a, a + 4))); }
  assert.equal(I.RAMPS.precipThunder, undefined); assert.equal(I.RAMPS.precipIce[0][1], '#d9fbf4', 'ice teal');
});

// --------------------------------------------------------------------------------------------- the layer, readout

test('the layer draws a typed bundle: tiles go through the typed sampler; readoutAt = the drawn pixel', () => {
  const s = storm(41.0, -125.0, 60, 80), m = I.validateRegionalManifest(withPrecip(MAN()), PREFIX), fd = m.fields.precip;
  const l = new I.ModelGridLayer({ opacity: 0.75 });
  const bundle = { regional: true, parts: [{ name: 'conus', grid: s.grid, frame: s.frame }], names: ['conus'], cols: 0, rows: 0, q: new Uint8Array(0) };
  l.setFrame(bundle, undefined, 'precip', fd, I.precipLut(), m.frames[3], null);
  assert.equal(l._typed, true);
  const z = 9, checks = [[40.0, -123.8], [39.8, -123.6], [40.4, -124.6], [39.3, -124.9], [40.9, -121.1]];
  for (const [lat, lon] of checks) {
    const p = I.pixelOf(lat, lon, z), out = new Float64Array(TILE * TILE);
    l.tileCodes({ z, x: p.x, y: p.y }, out);
    l._tiles[p.x + ':' + p.y + ':' + z] = { el: {}, coords: { z, x: p.x, y: p.y } };
    const drawn = out[p.py * TILE + p.px], v = l.readoutAt(lat, lon, z);
    assert.equal(v, drawn || null, `${lat},${lon}`);
  }
  { const lat = 40.4, lon = -122.88, p = I.pixelOf(lat, lon, z), out = new Float64Array(TILE * TILE);   // inside the hole of missing nodes
    l.tileCodes({ z, x: p.x, y: p.y }, out); l._tiles[p.x + ':' + p.y + ':' + z] = { el: {}, coords: { z, x: p.x, y: p.y } };
    assert.equal(out[p.py * TILE + p.px], 0); assert.equal(l.readoutAt(lat, lon, z), null, 'nothing drawn: no readout'); }
  // a whole tile through _drawTile: the colours are composeTyped's with the precipitation LUT (G68 M23)
  { const p = I.pixelOf(40.0, -123.8, z), img = { data: new Uint8ClampedArray(TILE * TILE * 4) }, ctx = { createImageData: () => img, putImageData() {}, clearRect() {} };
    const el = { getContext: () => ctx }, out = new Float64Array(TILE * TILE), lut = I.precipLut();
    l._drawTile(el, { z, x: p.x, y: p.y });
    l.tileCodes({ z, x: p.x, y: p.y }, out);
    let wet = 0;
    for (let i = 0; i < TILE * TILE; i++) {
      const e = out[i] ? Math.round(out[i] * 2) * 4 : -1;
      if (e < 0) { assert.equal(img.data[i * 4 + 3], 0); continue; }
      if (out[i] >= 2) wet++;
      for (let c = 0; c < 4; c++) assert.equal(img.data[i * 4 + c], lut[e + c], 'pixel ' + i);
    }
    assert.ok(wet > 1000, 'a wet tile'); }
  l._tiles = {}; l.clear(); assert.equal(l._typed, false, 'clear() drops the typed state (G68 M18)');
  l.setFrame(bundle, undefined, 'precip', fd, I.precipLut(), m.frames[3], null); assert.equal(l._typed, true);
  l._tiles = {};
  l.setFrame({ regional: true, parts: [{ name: 'hi', grid: s.grid, frame: s.frame }], names: ['hi'], cols: 0, rows: 0, q: new Uint8Array(0) }, undefined, 'wind', m.fields.wind, I.buildLut('wind', m.fields.wind.legend), m.frames[3], null);
  assert.equal(l._typed, false, 'wind is not typed');
  l._tiles = {}; l.clear(); assert.equal(l._typed, false);
});

test('the readout’s words in both units: amounts, types, dry, the top of the scale', () => {
  const v = (t, lvl) => 2 + 50 * t + lvl, lvl25 = Math.log(2.5 / 0.1) / Math.log(750) * 49;
  assert.equal(I.precipReadout(v(0, lvl25), 'Metric', 75), '2.5 mm/h rain');
  assert.equal(I.precipReadout(v(0, lvl25), 'US', 75), '0.10 in/h rain');
  assert.equal(I.precipReadout(v(1, 0), 'US', 75), '0.004 in/h snow (water)');
  assert.equal(I.precipReadout(v(1, 0), 'Metric', 75), '0.1 mm/h snow (water)');
  assert.equal(I.precipReadout(v(2, 30), 'Metric', 75).endsWith('rain and snow (water)'), true);
  assert.equal(I.precipReadout(v(3, 10), 'Metric', 75).endsWith('freezing rain / ice pellets'), true);
  assert.equal(I.precipReadout(v(4, 49), 'Metric', 75), '≥ 70 mm/h rain, thunder');   // the top level's lower edge (G68 P3)
  assert.equal(I.precipReadout(v(4, 49), 'US', 75), '≥ 2.8 in/h rain, thunder');
  assert.equal(I.precipReadout(1, 'US', 75), 'no precipitation');
  assert.equal(I.precipReadout(0, 'US', 75), null);
  assert.equal(I.precipAmountText(12.3, 'Metric', '/h'), '12 mm/h');
});

test('gating: Precipitation is RRFS’s alone; GFS and ECMWF keep waves and wind', () => {
  assert.equal(I.modelOffers('rrfs', 'precip'), true);
  assert.equal(I.modelOffers('gfswave', 'precip'), false); assert.equal(I.modelOffers('ecmwf', 'precip'), false);
  assert.equal(I.modelOffers('rrfs', 'hs'), false); assert.equal(I.modelOffers('gfswave', 'hs'), true); assert.equal(I.modelOffers('ecmwf', 'tp'), true);
  assert.equal(I.FIXED_OPACITY ? I.FIXED_OPACITY.precip : 0.75, 0.75);
  assert.ok(I.RAMPS.precip, 'the field has a ramp (mount checks RAMPS[field])');
});
