'use strict';
// Great Lakes client tests for static_overlay/overlay.js (plan section 50, step 4; no browser): the lake pointer /
// manifest / grid rules, the lake sampler against a brute-force bilinear (nodes, the absent rule, no wrap, world copies,
// the coverage gate), the composite tile and the one alpha rule, readout = the drawn pixel across the shore and the
// "no data" case, contours under the lake, the LakeStore's state machine with held answers, the lake flow (window,
// nodes, the lattice's gate), the layer's coverage from a lakes coast set, and the published run 2026102119's frames
// against an independent decode.   node --test tests/overlay/lake.test.js
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
const settle = async () => { for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r)); };
const FIX = path.join(__dirname, '..', 'fixtures', 'glwu');
const PTR = JSON.parse(fs.readFileSync(path.join(FIX, 'latest.json'), 'utf8'));
const MAN = JSON.parse(fs.readFileSync(path.join(FIX, 'manifest.json'), 'utf8'));
const SAMPLES = JSON.parse(fs.readFileSync(path.join(FIX, 'f000_samples.json'), 'utf8'));
const LG = MAN.grid;                                                            // 831 x 401, 49.2..41.2 N, -92.4..-75.8 E
const HS = MAN.fields.hs, TP = MAN.fields.tp;                                   // the model's own ranges (hs 0..22.86 m)
const GRID = { cols: 1440, rows: 721, lon0: -180, lat0: 90, dlon: 0.25, dlat: -0.25, registration: 'center', lon_periodic: true };
const PDIR = { lo: 0, hi: 360, legend: [0, 360], units: 'deg', interpolation: 'circular', resolutions: ['full', 'half'], circular: true, convention: 'from' };
const TILE = 256;
const toCode = (v, f) => 1 + Math.round((v - f.lo) / (f.hi - f.lo) * 254);
// a lake field: 0.5 m at the grid's south-west corner, rising north and east (one quantum per few nodes)
const lakeFn = (lat, lon) => 0.5 + 0.2 * (lat - 41.2) + 0.1 * (lon + 92.4);
function lakeFrame(fn, f, absent) {
  const q = new Uint8Array(LG.cols * LG.rows);
  for (let r = 0; r < LG.rows; r++) for (let c = 0; c < LG.cols; c++) {
    const v = fn(LG.lat0 + r * LG.dlat, LG.lon0 + c * LG.dlon);
    q[r * LG.cols + c] = Number.isNaN(v) || (absent && absent(r, c)) ? 0 : toCode(v, f || HS);
  }
  return { q, cols: LG.cols, rows: LG.rows };
}
// the GFS frame: nothing over the Great Lakes (the fill reaches no lake: pinned on the live frames 2026-10-21, 0 of 2,485 nodes), a pattern elsewhere
const GFS_LAKE = (r, c) => { const lat = 90 - r * 0.25, lon = -180 + c * 0.25; return lat >= 41 && lat <= 49.5 && lon >= -93 && lon <= -75.5; };
function globalFrame(fn) {
  const q = new Uint8Array(GRID.cols * GRID.rows);
  for (let r = 0; r < GRID.rows; r++) for (let c = 0; c < GRID.cols; c++) q[r * GRID.cols + c] = fn(r, c);
  return { q, cols: GRID.cols, rows: GRID.rows };
}
const PATTERN = (r, c) => GFS_LAKE(r, c) ? 0 : 1 + ((r * 7 + c * 3) % 254);
function layer(fr, field, fdef, lake) {
  const l = new I.ModelGridLayer({ opacity: 1 });
  l.setFrame(fr, GRID, field, fdef, I.buildLut(field, fdef.legend), { step: 12, valid_utc: '2026-10-22T07:00:00Z' }, lake || null);
  return l;
}
const tileEl = () => ({ getContext: () => ({ clearRect() {}, createImageData: () => ({ data: new Uint8ClampedArray(TILE * TILE * 4) }), putImageData() {} }) });
const SUPERIOR = { z: 7, x: 32, y: 44 };                                        // 90..87.2 W, 47.0..48.9 N: Lake Superior
const EAST = { z: 7, x: 37, y: 46 };                                            // 75.9..73.1 W: the grid's last column crosses it
const key = (c) => c.x + ':' + c.y + ':' + c.z;
// coverage: a lake over the tile's left part with an anti-aliased shore at column `edge`
function coverage(edge, width) {
  const cov = new Uint8Array(TILE * TILE);
  for (let py = 0; py < TILE; py++) for (let px = 0; px < TILE; px++) cov[py * TILE + px] = px < edge ? 255 : px < edge + (width || 4) ? Math.round(255 * (1 - (px - edge + 0.5) / (width || 4))) : 0;
  return cov;
}
// the reference sampler: bilinear over the lake nodes (present ones; weight < 0.25 absent), no wrap, nothing beyond the grid
function refSample(lf, lat, lon) {
  lon = ((lon + 180) % 360 + 360) % 360 - 180;
  const fx = (lon - LG.lon0) / LG.dlon, fy = (LG.lat0 - lat) / -LG.dlat;
  if (!(fx >= 0 && fx <= LG.cols - 1 && fy >= 0 && fy <= LG.rows - 1)) return 0;
  const c0 = Math.floor(fx), r0 = Math.floor(fy), c1 = Math.min(c0 + 1, LG.cols - 1), r1 = Math.min(r0 + 1, LG.rows - 1), fc = fx - c0, fr = fy - r0;
  const nodes = [[r0, c0, (1 - fr) * (1 - fc)], [r0, c1, (1 - fr) * fc], [r1, c0, fr * (1 - fc)], [r1, c1, fr * fc]];
  let w = 0, acc = 0;
  for (const [r, c, wt] of nodes) { const q = lf.q[r * LG.cols + c]; if (q) { acc += q * wt; w += wt; } }
  return w < 0.25 ? 0 : acc / w;
}

test('the published run 2026102119: its pointer and manifest pass; the rules that keep bad data off the map', () => {
  assert.equal(I.validLakePointer(PTR), true);
  assert.equal(I.validLakeManifest(MAN), true);
  assert.equal(I.validLakeGridDef(LG), true);
  assert.equal(MAN.frames.length, 150);
  for (const bad of [null, {}, Object.assign({}, PTR, { encoding: 'x' }), Object.assign({}, PTR, { complete: false }), Object.assign({}, PTR, { run: '202610211' }),
    Object.assign({}, PTR, { manifest: 'gfswave/0p25/v1/x.json' }), Object.assign({}, PTR, { manifest: 'glwu/v1/../x.json' })]) assert.equal(I.validLakePointer(bad), false, JSON.stringify(bad));
  const mod = (fn) => { const m = JSON.parse(JSON.stringify(MAN)); fn(m); return m; };
  for (const [what, m] of [
    ['schema', mod((m) => { m.schema = 2; })], ['encoding', mod((m) => { m.encoding = 'u8'; })], ['incomplete', mod((m) => { m.complete = false; })],
    ['run', mod((m) => { m.run = '20261021'; })], ['no tp', mod((m) => { delete m.fields.tp; })], ['no pdir', mod((m) => { delete m.fields.pdir; })],
    ['pdir half only', mod((m) => { m.fields.pdir.resolutions = ['half']; })], ['hs half only', mod((m) => { m.fields.hs.resolutions = ['half']; })],
    ['periodic grid', mod((m) => { m.grid.lon_periodic = true; })], ['grid off the lakes', mod((m) => { m.grid.lat0 = 60; })], ['grid too big', mod((m) => { m.grid.cols = 2000; m.grid.rows = 1000; })],
    ['template elsewhere', mod((m) => { m.files.template = 'gfswave/0p25/v1/x/{res}{field}/f{step:03d}.png'; })], ['no res', mod((m) => { delete m.files.res; })],
    ['frames out of order', mod((m) => { m.frames[5].step = 3; })], ['times out of order', mod((m) => { m.frames[5].valid_utc = m.frames[2].valid_utc; })],
    ['no frames', mod((m) => { m.frames = []; })], ['too many frames', mod((m) => { for (let i = 0; i < 400; i++) m.frames.push({ step: 200 + i, valid_utc: new Date(Date.parse(m.run_utc) + (200 + i) * 3.6e6).toISOString() }); })]
  ]) assert.equal(I.validLakeManifest(m), false, what);
  // the grid of a frame must be the manifest's; the lake and the model field must share lo / hi
  I.validateLakeGrid(LG, { cols: 831, rows: 401, q: new Uint8Array(831 * 401) });
  assert.throws(() => I.validateLakeGrid(LG, { cols: 830, rows: 401, q: new Uint8Array(830 * 401) }), /lake frame/);
  assert.throws(() => I.validateLakeGrid(LG, { cols: 831, rows: 401, q: new Uint8Array(10) }), /lake frame/);
  assert.throws(() => I.validateLakeGrid(Object.assign({}, LG, { lon_periodic: true }), { cols: 831, rows: 401, q: new Uint8Array(831 * 401) }), /lake frame/);
  assert.equal(I.lakeFieldMatches(HS, { lo: 0, hi: 22.86 }), true);
  assert.equal(I.lakeFieldMatches(HS, { lo: 0, hi: 15 }), false);
  assert.equal(I.lakeFieldMatches(undefined, HS), false);
  assert.equal(I.lakeFieldOk(HS), true); assert.equal(I.lakeFieldOk({ lo: 0, hi: 1, legend: [0, 1], resolutions: ['half'] }), false);
  assert.deepEqual(I.LAKE_UNDER, { hs: true, tp: true }); assert.equal(I.LAKE_DIR_FIELD, 'pdir'); assert.equal(I.LAKE_PREFIX, 'glwu/v1');
});

test('sampleLake: exact at the nodes, bilinear between them with the absent rule, no wrap at the last column, 0 beyond the grid, world copies identical, written only under lake coverage', () => {
  const lf = lakeFrame(lakeFn, HS, (r, c) => r >= 60 && r < 70 && c >= 100 && c < 140);   // a hole of absent nodes inside Superior
  const full = I.lakeFull(), out = new Float64Array(TILE * TILE);
  for (const coords of [SUPERIOR, EAST]) {
    I.sampleLake(lf, LG, coords, 0, 0, 1, TILE, out.fill(-1), full);
    let nonzero = 0, zero = 0;
    for (let py = 0; py < TILE; py += 3) for (let px = 0; px < TILE; px += 5) {
      const ll = I.tilePixelLatLng(coords, px, py), ref = refSample(lf, ll.lat, ll.lng), got = out[py * TILE + px];
      assert.ok(Math.abs(got - ref) < 1e-9, key(coords) + ' ' + px + ',' + py + ': ' + got + ' vs ' + ref);
      if (got) nonzero++; else zero++;
    }
    assert.ok(nonzero > (coords === EAST ? 100 : 2000), key(coords) + ' samples values: ' + nonzero);
    if (coords === SUPERIOR) assert.ok(zero > 20, 'the hole of absent nodes reads 0: ' + zero);
    if (coords === EAST) assert.ok(zero > 1000, 'east of the last column nothing: ' + zero);
    // a world copy samples the one world
    const copy = new Float64Array(TILE * TILE);
    I.sampleLake(lf, LG, { z: coords.z, x: coords.x + 128, y: coords.y }, 0, 0, 1, TILE, copy.fill(-1), full);
    assert.deepEqual(copy, out);
  }
  // exact at a node: the pixel whose centre is a node reads that node's code (a z13 tile: pixels 0.0023 deg apart)
  const node = { r: 85, c: 230 }, lat = LG.lat0 + node.r * LG.dlat, lon = LG.lon0 + node.c * LG.dlon, p = I.pixelOf(lat, lon, 13);
  const ll = I.tilePixelLatLng(p, p.px, p.py), one = new Float64Array(1);
  I.sampleLake(lf, LG, p, p.px, p.py, 1, 1, one, full);
  assert.ok(Math.abs(one[0] - refSample(lf, ll.lat, ll.lng)) < 1e-9);
  assert.ok(Math.abs(one[0] - lf.q[node.r * LG.cols + node.c]) < 0.6, 'within a code of the node at the pixel nearest it: ' + one[0] + ' vs ' + lf.q[node.r * LG.cols + node.c]);
  // the last column: a point between the last node and the one beyond is NOT blended with column 0 (no wrap)
  const lastLon = LG.lon0 + (LG.cols - 1) * LG.dlon, q = lf.q, rr = 100;
  q[rr * LG.cols] = 254;                                                       // column 0 bright: a wrap would show it
  const pp = I.pixelOf(LG.lat0 + rr * LG.dlat, lastLon - 0.004, 14);          // just west of the last node
  I.sampleLake(lf, LG, pp, pp.px, pp.py, 1, 1, one, full);
  const expect = refSample(lf, I.tilePixelLatLng(pp, pp.px, pp.py).lat, I.tilePixelLatLng(pp, pp.px, pp.py).lng);
  assert.ok(Math.abs(one[0] - expect) < 1e-9 && Math.abs(one[0] - q[rr * LG.cols + LG.cols - 1]) < 2, 'the last column stands alone: ' + one[0]);
  const beyond = I.pixelOf(LG.lat0 + rr * LG.dlat, lastLon + 0.004, 14);
  I.sampleLake(lf, LG, beyond, beyond.px, beyond.py, 1, 1, one.fill(-1), full);
  assert.equal(one[0], 0, 'beyond the last node: nothing');
  // the coverage gate: only pixels under lake coverage are written; the rest keep what they held
  const cov = coverage(100, 4), gated = new Float64Array(TILE * TILE).fill(-1), ref = new Float64Array(TILE * TILE).fill(-1);
  I.sampleLake(lf, LG, SUPERIOR, 0, 0, 1, TILE, gated, cov); I.sampleLake(lf, LG, SUPERIOR, 0, 0, 1, TILE, ref, full);
  for (let py = 0; py < TILE; py += 7) for (let px = 0; px < TILE; px += 3) {
    const i = py * TILE + px;
    if (cov[i]) assert.ok(gated[i] >= 0 && Math.abs(gated[i] - ref[i]) < 1e-9, 'written under coverage ' + px);
    else assert.equal(gated[i], -1, 'untouched without coverage ' + px);
  }
  // the contour lattice's pad: samples one step outside the tile take the edge pixel's coverage
  const m = TILE / 4 + 2, lat2 = new Float64Array(m * m).fill(-1);
  I.sampleLake(lf, LG, SUPERIOR, -4, -4, 4, m, lat2, cov);
  assert.ok(lat2[0] >= 0, 'the corner sample outside the tile is written (the edge pixel is lake)');
  assert.equal(lat2[m - 1], -1, 'the far corner is not (the edge pixel there is land)');
});

test('the composite tile: the model\'s codes outside the lake coverage, the lake frame\'s under it, 0 where the lake frame has no node; composeTile\'s one alpha rule', () => {
  const lf = lakeFrame(lakeFn, HS, (r, c) => c >= 120 && c < 128), gf = globalFrame(PATTERN);
  const lake = { frame: lf, grid: LG, entry: { step: 12, valid_utc: '2026-10-22T07:00:00Z' }, lo: HS.lo, hi: HS.hi };
  const l = layer(gf, 'hs', HS, lake);
  assert.ok(l.hasLake());
  const cov = coverage(100, 4), plain = l.tileCodes(SUPERIOR, new Float64Array(TILE * TILE), 0, null), comp = l.tileCodes(SUPERIOR, new Float64Array(TILE * TILE), 0, cov);
  const full = new Float64Array(TILE * TILE).fill(-1); I.sampleLake(lf, LG, SUPERIOR, 0, 0, 1, TILE, full, I.lakeFull());
  let underLake = 0, blank = 0;
  for (let i = 0; i < TILE * TILE; i++) {
    if (cov[i]) { assert.equal(comp[i], full[i], 'lake code at ' + i); if (comp[i]) underLake++; else blank++; }
    else assert.equal(comp[i], plain[i], 'model code at ' + i);
    assert.equal(plain[i], 0, 'the GFS frame holds 0 over the lake');
  }
  assert.ok(underLake > 10000 && blank > 500, underLake + ' lake pixels, ' + blank + ' blank (absent nodes)');
  // the alpha rule: land takes it, lake coverage gives it back; without a lake argument the call is today's byte for byte
  const codes = new Float64Array(TILE * TILE).fill(100), land = new Uint8Array(TILE * TILE).fill(255), lut = I.buildLut('hs', HS.legend);
  const a = I.composeTile(codes, land, lut, HS.lo, HS.hi, HS.legend[0], HS.legend[1], new Uint8ClampedArray(TILE * TILE * 4));
  const b = I.composeTile(codes, land, lut, HS.lo, HS.hi, HS.legend[0], HS.legend[1], new Uint8ClampedArray(TILE * TILE * 4), null);
  assert.deepEqual(a, b); assert.equal(a[3], 0, 'all land: transparent');
  const c = I.composeTile(codes, land, lut, HS.lo, HS.hi, HS.legend[0], HS.legend[1], new Uint8ClampedArray(TILE * TILE * 4), cov);
  for (let px = 0; px < TILE; px += 2) assert.equal(c[px * 4 + 3], cov[px], 'alpha = the lake coverage over land at ' + px);
  const halfLand = new Uint8Array(TILE * TILE).fill(55), d = I.composeTile(codes, halfLand, lut, HS.lo, HS.hi, HS.legend[0], HS.legend[1], new Uint8ClampedArray(TILE * TILE * 4), cov);
  for (let px = 0; px < TILE; px += 2) assert.equal(d[px * 4 + 3], Math.max(200, cov[px]), 'the larger of the two at ' + px);
  codes[5] = 0; const e = I.composeTile(codes, null, lut, HS.lo, HS.hi, HS.legend[0], HS.legend[1], new Uint8ClampedArray(TILE * TILE * 4), cov);
  assert.equal(e[5 * 4 + 3], 0, 'no code: transparent whatever the coverage');
  assert.equal(c.slice(0, 3).join(), d.slice(0, 3).join(), 'the colour is the code\'s, coverage or not');
});

// a layer whose tile has the lakes' land mask (the lakes are land in the coast set) and a planted lake coverage
function shoreLayer(gf, field, fdef, lake, cov) {
  const l = layer(gf, field, fdef, lake), el = tileEl(), k = SUPERIOR.z + '/' + SUPERIOR.x + '/' + SUPERIOR.y;
  l._coast = { status: 'ok', rev: 0 }; l._clip = true;
  el._ovLand = new Uint8Array(TILE * TILE).fill(255); el._ovLandKey = k; el._ovLandFinal = true;
  el._ovLake = cov; el._ovLakeKey = k; el._ovLakeFinal = true;
  l._lakeCoast = { status: 'ok', rev: 0 }; l._lakeMask = true; l._lakeBox = { x0: 0, y0: 0, x1: 256, y1: 256 };
  l._tiles[key(SUPERIOR)] = { el, coords: SUPERIOR };
  return { l, el };
}

test('readoutAt = the drawn pixel across the shore: the lake frame\'s value on lake water, null on land, null + lakeAt on blank lake water ("no data"); the clip\'s rule unchanged elsewhere', () => {
  const lf = lakeFrame(lakeFn, HS, (r, c) => r >= 60 && r < 70 && c >= 100 && c < 140), gf = globalFrame(PATTERN);
  const lake = { frame: lf, grid: LG, entry: { step: 12, valid_utc: '2026-10-22T07:00:00Z' }, lo: HS.lo, hi: HS.hi }, cov = coverage(100, 6);
  for (const [field, fdef] of [['hs', HS], ['tp', TP]]) {
    const { l, el } = shoreLayer(gf, field, fdef, Object.assign({}, lake, { lo: fdef.lo, hi: fdef.hi, frame: field === 'tp' ? lakeFrame(lakeFn, TP, (r, c) => r >= 60 && r < 70 && c >= 100 && c < 140) : lf }), cov);
    l._draw(el, SUPERIOR);
    const d = el._ovImg.data, codes = l.tileCodes(SUPERIOR, new Float64Array(TILE * TILE), 0, cov);
    let water = 0, land = 0, blank = 0;
    for (let py = 0; py < TILE; py += 3) for (let px = 0; px < TILE; px += 2) {
      const i = py * TILE + px, ll = I.tilePixelLatLng(SUPERIOR, px, py), v = l.readoutAt(ll.lat + 1e-7, ll.lng - 1e-7, 7), a = d[i * 4 + 3];
      if (a >= I.LAND_READOUT) { assert.ok(v !== null && Math.abs(v - l._value(codes[i])) < 1e-9, field + ' ' + px + ',' + py); water++; }
      else { assert.equal(v, null, field + ' ' + px + ',' + py + ' alpha ' + a); if (cov[i] >= I.LAND_READOUT) blank++; else land++; }
      assert.equal(l.lakeAt(ll.lat + 1e-7, ll.lng - 1e-7, 7), cov[i] >= I.LAND_READOUT, 'lakeAt ' + px);
    }
    assert.ok(water > 1500 && land > 2000 && blank > 30, field + ': ' + water + ' water, ' + land + ' land, ' + blank + ' blank lake');
  }
  // no lake frame (past the lake run, no lake run): every lake pixel is blank, the readout null and lakeAt true (the "no data" text)
  const { l: l2, el: el2 } = shoreLayer(gf, 'hs', HS, null, cov);
  l2._draw(el2, SUPERIOR);
  const ll = I.tilePixelLatLng(SUPERIOR, 50, 128);
  assert.equal(el2._ovImg.data[(128 * TILE + 50) * 4 + 3], 0); assert.equal(l2.readoutAt(ll.lat, ll.lng, 7), null); assert.equal(l2.lakeAt(ll.lat, ll.lng, 7), true);
  const lland = I.tilePixelLatLng(SUPERIOR, 200, 128);
  assert.equal(l2.lakeAt(lland.lat, lland.lng, 7), false, 'land is not lake water');
  assert.equal(l2.lakeAt(ll.lat, ll.lng, 8), false, 'a tile not on the map: unknown');
  // without the lake mask (wind, SST, no lakes coast set) nothing changes: the land rule alone
  const { l: l3, el: el3 } = shoreLayer(gf, 'hs', HS, lake, cov);
  l3._lakeMask = false; l3._draw(el3, SUPERIOR);
  assert.equal(el3._ovImg.data[(128 * TILE + 50) * 4 + 3], 0); assert.equal(l3.readoutAt(ll.lat, ll.lng, 7), null); assert.equal(l3.lakeAt(ll.lat, ll.lng, 7), false);
  // a world copy's tile reads the same
  const wl = { l: l3 }; wl.l._lakeMask = true; wl.l._tiles['160:44:7'] = { el: el3, coords: { z: 7, x: 160, y: 44 } };
  assert.equal(wl.l.readoutAt(ll.lat, ll.lng + 360, 7), wl.l.readoutAt(ll.lat, ll.lng, 7));
});

test('contours under the lake: lines follow the lake frame\'s values inside the coverage (RGB only), none on a blank lake; the model\'s contours outside unchanged', () => {
  const steep = (lat, lon) => 0.5 + 2.5 * (lat - 47) + 0.02 * (lon + 90);        // 2.5 m per degree: a z7 tile (1.9 deg) holds 2-3 one-metre lines
  const lf = lakeFrame(steep, HS), gf = globalFrame(PATTERN), lake = { frame: lf, grid: LG, entry: { step: 1 }, lo: HS.lo, hi: HS.hi }, cov = coverage(128, 1);
  function inked(withLake) {
    const { l, el } = shoreLayer(gf, 'hs', HS, withLake ? lake : null, cov);
    l.setContours({ step: 1, per: 1, off: 0 }, true);
    l._draw(el, SUPERIOR);
    const d = el._ovImg.data, codes = l.tileCodes(SUPERIOR, new Float64Array(TILE * TILE), 0, cov), plain = new Uint8ClampedArray(TILE * TILE * 4);
    I.composeTile(codes, el._ovLand, l._lut, HS.lo, HS.hi, HS.legend[0], HS.legend[1], plain, cov);
    const rows = {}; let outside = 0;
    for (let py = 0; py < TILE; py++) for (let px = 0; px < TILE; px++) {
      const k = (py * TILE + px) * 4, changed = d[k] !== plain[k] || d[k + 1] !== plain[k + 1] || d[k + 2] !== plain[k + 2];
      assert.equal(d[k + 3], plain[k + 3], 'alpha untouched');
      if (changed) { if (px < 128) rows[py] = (rows[py] || 0) + 1; else outside++; }
    }
    return { rows: Object.keys(rows).map(Number).filter((r) => rows[r] > 40), outside, l, codes };
  }
  const withL = inked(true), without = inked(false);
  assert.equal(without.rows.length, 0, 'a blank lake draws no lines'); assert.equal(without.outside, 0);
  assert.equal(withL.outside, 0, 'nothing outside the coverage (the model holds 0 there)');
  // the lines sit where the drawn lake value crosses a whole metre, by column 0
  const want = [];
  for (let py = 0; py < TILE - 1; py++) if (Math.floor(withL.l._value(withL.codes[py * TILE])) !== Math.floor(withL.l._value(withL.codes[(py + 1) * TILE]))) want.push(py);
  assert.ok(want.length >= 2 && withL.rows.length >= 2, 'lines expected ' + want + ' drawn ' + withL.rows);
  // the lines sit where the lattice (every 4 px at this zoom) interpolates the level: within a lattice step of the pixel crossing
  assert.ok(withL.rows.every((r) => want.some((w) => Math.abs(w - r) <= 4)) && want.every((w) => withL.rows.some((r) => Math.abs(w - r) <= 4)), 'drawn ' + withL.rows + ' vs ' + want);
});

// a LakeStore whose answers are promises the test resolves by hand
function heldStore() {
  const st = new I.LakeStore('https://x'), calls = [];
  st._json = (url, sig) => {
    const h = { url }; h.p = new Promise((res, rej) => { h.res = res; h.rej = rej; }); calls.push(h);
    if (sig) sig.addEventListener('abort', () => h.rej(Object.assign(new Error('aborted'), { name: 'AbortError' })));   // as fetch does
    return h.p;
  };
  return { st, calls, last: () => calls[calls.length - 1] };
}

test('LakeStore: pointer -> manifest; byValid; a bad pointer or manifest fails with a cooldown, load(true) asks again; a failed re-read keeps the run; abortAll', async () => {
  const { st, calls, last } = heldStore();
  assert.equal(st.status, 'idle');
  const p = st.load();
  assert.equal(st.status, 'loading'); assert.ok(/glwu\/v1\/latest\.json$/.test(last().url));
  last().res(PTR); await settle();
  assert.equal(calls.length, 2); assert.equal(last().url, 'https://x/' + PTR.manifest);
  last().res(MAN); await settle();
  assert.equal(await p, st); assert.equal(st.status, 'ok'); assert.equal(st.manifest.run, '2026102119'); assert.equal(st.grid, MAN.grid);
  assert.equal(st.indexOf(Date.parse('2026-10-21T19:00:00Z')), 0); assert.equal(st.indexOf(Date.parse('2026-10-28T00:00:00Z')), 149);
  assert.equal(st.indexOf(Date.parse('2026-10-28T01:00:00Z')), -1); assert.equal(st.indexOf(Date.parse('2026-10-21T19:30:00Z')), -1); assert.equal(st.indexOf(Date.parse('2026-10-21T18:00:00Z')), -1);
  assert.equal(await st.load(), st); assert.equal(calls.length, 2, 'a fresh run: no re-read');
  // a re-read (older than recheckMs) that fails keeps the run
  st.loadedAt = 0;
  const p2 = st.load(); assert.equal(calls.length, 3); last().rej(new Error('x')); await settle();
  assert.equal(await p2, st); assert.equal(st.status, 'ok'); assert.equal(st.manifest.run, '2026102119');
  // the same run again: nothing changes
  st.loadedAt = 0; const p3 = st.load(); last().res(PTR); await settle(); assert.equal(await p3, st); assert.equal(calls.length, 4);
  // a bad pointer: failed + cooldown; load() answers null at once; load(true) asks again
  const b = heldStore();
  const pb = b.st.load(); b.last().res(Object.assign({}, PTR, { encoding: 'x' })); await settle();
  assert.equal(await pb, null); assert.equal(b.st.status, 'failed'); assert.ok(b.st.failedAt > 0);
  assert.equal(await b.st.load(), null); assert.equal(b.calls.length, 1, 'in the cooldown: no request');
  const pf = b.st.load(true); assert.equal(b.calls.length, 2); b.last().res(PTR); await settle(); b.last().res(Object.assign({}, MAN, { run: '2026102113' })); await settle();
  assert.equal(await pf, null, 'a manifest of another run than the pointer\'s is refused'); assert.equal(b.st.status, 'failed');
  // abortAll while loading: idle, the late answer never lands
  const c = heldStore();
  const pc = c.st.load(); c.st.abortAll(); assert.equal(c.st.status, 'idle'); assert.equal(c.st.loading, null);
  c.last().res(PTR); await settle(); assert.equal(await pc, null); assert.equal(c.st.status, 'idle'); assert.equal(c.st.manifest, null);
  // the watchdog: a silent pointer fails the load
  const d = heldStore(); d.st.timeoutMs = 20;
  const pd = d.st.load(); await new Promise((r) => setTimeout(r, 60));
  assert.equal(await pd, null); assert.equal(d.st.status, 'failed');
});

test('LakeStore.recheck notes a newer run into next (its manifest fetched once); adoptNewer takes it; an older or the same run is ignored', async () => {
  const { st, calls, last } = heldStore();
  const p = st.load(); last().res(PTR); await settle(); last().res(MAN); await settle(); await p;
  const NEWER = Object.assign({}, PTR, { run: '2026102201', manifest: 'glwu/v1/2026102201/manifest-x.json' });
  const M2 = Object.assign({}, MAN, { run: '2026102201', run_utc: '2026-10-22T01:00:00Z', frames: MAN.frames.map((f) => ({ step: f.step, valid_utc: new Date(Date.parse('2026-10-22T01:00:00Z') + f.step * 3.6e6).toISOString().replace('.000Z', 'Z') })) });
  const r1 = st.recheck(); last().res(NEWER); await settle(); assert.equal(last().url, 'https://x/' + NEWER.manifest); last().res(M2); await settle();
  assert.equal(await r1, true); assert.ok(st.next && st.next.m.run === '2026102201'); assert.equal(st.manifest.run, '2026102119', 'not swapped in by the recheck');
  const n = calls.length, r2 = st.recheck(); last().res(NEWER); await settle();
  assert.equal(await r2, true); assert.equal(calls.length, n + 1, 'the same newer run: its manifest is not fetched again');
  assert.equal(st.adoptNewer(), true); assert.equal(st.manifest.run, '2026102201'); assert.equal(st.next, null);
  assert.equal(st.indexOf(Date.parse('2026-10-22T01:00:00Z')), 0); assert.equal(st.indexOf(Date.parse('2026-10-21T19:00:00Z')), -1);
  assert.equal(st.adoptNewer(), false);
  const r3 = st.recheck(); last().res(PTR); await settle(); assert.equal(await r3, false); assert.equal(st.next, null, 'an older run is ignored');
  const r4 = st.recheck(); last().rej(new Error('x')); await settle(); assert.equal(await r4, false);
  // a load after a recheck noted a newer run takes `next` without fetching its manifest again
  const r5 = st.recheck(); const NEWEST = Object.assign({}, NEWER, { run: '2026102207', manifest: 'glwu/v1/2026102207/manifest-x.json' });
  last().res(NEWEST); await settle(); last().res(Object.assign({}, M2, { run: '2026102207', run_utc: '2026-10-22T07:00:00Z' })); await settle(); assert.equal(await r5, true);
  const m = calls.length, p2 = st.load(); assert.equal(calls.length, m + 1); last().res(NEWEST); await settle();
  assert.equal(await p2, st); assert.equal(calls.length, m + 1); assert.equal(st.manifest.run, '2026102207'); assert.equal(st.next, null);
});

test('lakeWindow: the lake grid\'s rows and columns under the view (plus the margin), null off the lakes, a world copy the same, a view wider than the world every column; windowCovers', () => {
  const view = { z: 7, w: 512, h: 256, ox: SUPERIOR.x * TILE, oy: SUPERIOR.y * TILE, zt: 7 };
  const w0 = I.lakeWindow(view, LG, 0), w1 = I.lakeWindow(view, LG, 1);
  assert.ok(w0 && w1);
  assert.ok(w0.r0 >= 0 && w0.r1 <= LG.rows - 1 && w0.c0 >= 0 && w0.c0 + w0.count <= LG.cols);
  // the view's corners lie inside the window (with the node of margin)
  for (const [px, py] of [[0, 0], [511, 0], [0, 255], [511, 255]]) {
    const ll = I.tilePixelLatLng({ z: 7, x: SUPERIOR.x, y: SUPERIOR.y }, px, py), r = (LG.lat0 - ll.lat) / -LG.dlat, c = (ll.lng - LG.lon0) / LG.dlon;
    assert.ok(r >= w0.r0 - 1e-9 && r <= w0.r1 + 1e-9 && c >= w0.c0 - 1e-9 && c <= w0.c0 + w0.count - 1 + 1e-9, 'corner ' + px + ',' + py + ' in ' + JSON.stringify(w0));
  }
  assert.ok(I.windowCovers(w1, w0, LG.cols) && !I.windowCovers(w0, w1, LG.cols), 'the margin window covers the view\'s');
  assert.ok(w1.r0 <= w0.r0 && w1.r1 >= w0.r1 && w1.c0 <= w0.c0 && w1.c0 + w1.count >= w0.c0 + w0.count);
  assert.deepEqual(I.lakeWindow(Object.assign({}, view, { ox: view.ox + 128 * TILE }), LG, 0), w0, 'a world copy');
  assert.deepEqual(I.lakeWindow(Object.assign({}, view, { ox: view.ox - 128 * TILE }), LG, 0), w0);
  assert.equal(I.lakeWindow({ z: 7, w: 512, h: 256, ox: 2000, oy: 14000, zt: 7 }, LG, 1), null, 'Hawaii: no lake node');
  assert.equal(I.lakeWindow({ z: 7, w: 512, h: 256, ox: SUPERIOR.x * TILE, oy: 60 * TILE, zt: 7 }, LG, 0), null, 'south of the grid');
  assert.equal(I.lakeWindow({ z: 7, w: 512, h: 256, ox: 50 * TILE, oy: SUPERIOR.y * TILE, zt: 7 }, LG, 1), null, 'the Atlantic at the lakes\' latitude: rows but no column');
  const wide = I.lakeWindow({ z: 2, w: 2000, h: 300, ox: 0, oy: 300, zt: 2 }, LG, 0);
  assert.ok(wide && wide.c0 === 0 && wide.count === LG.cols, 'a view wider than the world: every column');
});

test('lakeVectorNodes: FROM -> TOWARD vectors of the field\'s speed on the lake nodes, absent without a direction or a value, only inside the window', () => {
  const lf = lakeFrame(lakeFn, HS, (r, c) => c === 300), dirCode = (deg) => 1 + Math.round(deg * 254 / 360);
  const ld = { q: new Uint8Array(LG.cols * LG.rows), cols: LG.cols, rows: LG.rows };
  for (let r = 0; r < LG.rows; r++) for (let c = 0; c < LG.cols; c++) ld.q[r * LG.cols + c] = c === 301 ? 0 : c === 302 ? 255 : dirCode((r + c) % 360);
  const win = { r0: 100, r1: 110, c0: 290, count: 20 }, nodes = I.lakeVectorNodes('hs', lf, HS.lo, HS.hi, ld, LG, null, win);
  assert.equal(nodes.cols, LG.cols); assert.equal(nodes.rows, LG.rows); assert.deepEqual(nodes.win, win);
  let present = 0;
  for (let r = 95; r <= 115; r++) for (let c = 285; c <= 315; c++) {
    const i = r * LG.cols + c, inWin = r >= 100 && r <= 110 && c >= 290 && c < 310;
    if (!inWin || c === 300 || c === 301 || c === 302) { assert.equal(nodes.M[i], 0, 'absent at ' + r + ',' + c); continue; }
    const v = HS.lo + (lf.q[i] - 1) / 254 * (HS.hi - HS.lo), sp = I.FLOW_SPEED.hs(v), ang = (ld.q[i] - 1) / 254 * 2 * Math.PI;
    assert.equal(nodes.M[i], 1); assert.ok(Math.abs(nodes.U[i] + sp * Math.sin(ang)) < 1e-4 && Math.abs(nodes.V[i] + sp * Math.cos(ang)) < 1e-4, 'vector at ' + r + ',' + c);
    present++;
  }
  assert.ok(present > 150);
  const again = I.lakeVectorNodes('hs', lf, HS.lo, HS.hi, ld, LG, nodes, { r0: 200, r1: 201, c0: 10, count: 2 });
  assert.equal(again.U, nodes.U, 'the arrays are reused'); assert.equal(again.M[105 * LG.cols + 295], 0, 'the old window is cleared');
  assert.equal(again.M[200 * LG.cols + 10], 1);
});

test('flowField: a lattice point on lake water reads the lake vectors (nothing where they are absent); on lake water without lake vectors the model\'s nodes (the land gate lifted); the pixels\' alpha rule', () => {
  const gf = globalFrame(() => 100), gd = { q: new Uint8Array(GRID.cols * GRID.rows).fill(1 + Math.round(90 * 254 / 360)), cols: GRID.cols, rows: GRID.rows };   // the model: from the east everywhere
  const lf = lakeFrame(() => 2, HS), ld = { q: new Uint8Array(LG.cols * LG.rows).fill(1), cols: LG.cols, rows: LG.rows };   // the lake: from the north
  for (let r = 0; r < LG.rows; r++) for (let c = 140; c < 150; c++) ld.q[r * LG.cols + c] = 0;                               // a band without a lake direction (inside the tile's lake)
  const cov = coverage(100, 6), landCov = new Uint8Array(TILE * TILE).fill(255);
  const el = { _ovLand: landCov, _ovLake: cov }, el2 = { _ovLand: landCov, _ovLake: null };
  const lay = { _tiles: { [key(SUPERIOR)]: { el, coords: SUPERIOR }, '33:44:7': { el: el2, coords: { z: 7, x: 33, y: 44 } } }, _lakeMask: true, _frame: gf, _grid: GRID, _nearest: false, _lo: HS.lo, _hi: HS.hi, _clip: true };
  const view = { z: 7, w: 512, h: 256, ox: SUPERIOR.x * TILE, oy: SUPERIOR.y * TILE, zt: 7 };
  const nodes = I.vectorNodes('hs', lay, gd, GRID, null, null), lnodes = I.lakeVectorNodes('hs', lf, HS.lo, HS.hi, ld, LG, null, I.lakeWindow(view, LG, 1));
  const withLake = I.flowField(view, 4, nodes, GRID, lay, true, null, { nodes: lnodes, grid: LG });
  const noLake = I.flowField(view, 4, nodes, GRID, lay, true, null, null);
  const at = (f, px, py) => { const i = Math.floor(py / 4) * f.cols + Math.floor(px / 4); return [f.u[i], f.v[i]]; };
  // on lake water: the lake vector (from the north: toward the south = +v), not the model's (from the east: -u)
  let lakePts = 0, landPts = 0, bandPts = 0;
  for (let px = 0; px < 96; px += 4) for (let py = 0; py < 256; py += 4) {
    const [u, v] = at(withLake, px, py), ll = I.tilePixelLatLng(SUPERIOR, px, py), c = (ll.lng - LG.lon0) / LG.dlon;
    if (c > 140.75 && c < 149.25) { assert.ok(u === 0 && v === 0, 'no lake direction: nothing (never the model\'s)'); bandPts++; continue; }
    if (c >= 139 && c <= 151) continue;                                                                    // the band's fringe: a present neighbour may carry it
    assert.ok(Math.abs(u) < 1e-6 && v > 0, 'lake vector at ' + px + ',' + py + ': ' + u + ',' + v); lakePts++;
    const [u2, v2] = at(noLake, px, py);
    assert.ok(u2 < 0 && Math.abs(v2) < 0.05 * Math.abs(u2), 'without lake vectors the model serves on lake water (ECMWF): ' + u2 + ',' + v2);   // (code 65 is 90.7 deg: a trace of v)
  }
  // on land (the lake's shore is past column 106): nothing, with or without lake vectors
  for (let px = 112; px < 256; px += 8) for (let py = 0; py < 256; py += 8) {
    assert.deepEqual(at(withLake, px, py), [0, 0], 'land ' + px); assert.deepEqual(at(noLake, px, py), [0, 0]); landPts++;
  }
  // the second tile (no lake coverage, all land): nothing
  for (let px = 260; px < 512; px += 16) assert.deepEqual(at(withLake, px, 100), [0, 0]);
  assert.ok(lakePts > 500 && landPts > 300 && bandPts > 0, lakePts + ' ' + landPts + ' ' + bandPts);
  // the shore's partial coverage follows the pixels' rule: coverage >= 128 is lake water, below it nothing (land took the alpha)
  const [us, vs] = at(withLake, 100, 128), [uo, vo] = at(withLake, 104, 128);
  assert.ok(vs > 0 && cov[128 * TILE + 100] >= I.LAND_READOUT, 'coverage ' + cov[128 * TILE + 100]);
  assert.ok(uo === 0 && vo === 0 && cov[128 * TILE + 104] < I.LAND_READOUT, 'coverage ' + cov[128 * TILE + 104]);
  // without the lake mask on the layer the lake coverage is ignored (the land rule alone: all land here)
  assert.ok(at(withLake, 8, 128)[1] > 0, 'a lake point with a vector');
  lay._lakeMask = false;
  const off = I.flowField(view, 4, nodes, GRID, lay, true, null, { nodes: lnodes, grid: LG });
  assert.deepEqual(at(off, 8, 128), [0, 0], 'the lake coverage is ignored without the layer\'s lake mask: all land here');
});

test('FlowAnimator: the lake direction rides only while it is the step on the map\'s (lentry === entry); set before the step\'s own direction it joins its one rebuild, set after it rebuilds at once; clearLakeData', () => {
  const gf = globalFrame(() => 100), gd = { q: new Uint8Array(GRID.cols * GRID.rows).fill(1 + Math.round(90 * 254 / 360)), cols: GRID.cols, rows: GRID.rows };
  const lf = lakeFrame(() => 2, HS), ld = { q: new Uint8Array(LG.cols * LG.rows).fill(1), cols: LG.cols, rows: LG.rows };
  const entryA = { step: 12 }, entryB = { step: 13 }, cov = coverage(100, 6), { l, el } = shoreLayer(gf, 'hs', HS, { frame: lf, grid: LG, entry: entryA, lo: HS.lo, hi: HS.hi }, cov);
  const ctx = { ops: [], globalCompositeOperation: '', fillStyle: '' };
  for (const m of ['clearRect', 'beginPath', 'moveTo', 'lineTo', 'stroke', 'fillRect', 'setTransform']) ctx[m] = () => ctx.ops.push(m);
  const map = { on() {}, off() {}, getPane: () => null, createPane: () => ({ style: {}, appendChild() {} }), getSize: () => ({ x: 512, y: 256 }), getContainer: () => ({ clientWidth: 512, clientHeight: 256 }),
    getPixelBounds: () => ({ min: { x: SUPERIOR.x * TILE, y: SUPERIOR.y * TILE } }), getZoom: () => 7, containerPointToLayerPoint: () => ({ x: 0, y: 0 }) };
  const canvas = { style: {}, width: 0, height: 0, getContext: () => ctx, remove() {} };
  const hadDoc = global.document; global.document = { createElement: () => canvas, hidden: false };
  try {
    const fa = new I.FlowAnimator(map, l);
    let rebuilds = 0; const real = fa._rebuild; fa._rebuild = function () { rebuilds++; return real.apply(this, arguments); };
    fa.raf = () => 1; fa.caf = () => {};
    fa.attach(); fa.setField('hs');
    const at = (x, y) => { const vf = fa.vf; const c = ((y / vf.s) | 0) * vf.cols + ((x / vf.s) | 0); return [vf.u[c], vf.v[c]]; };
    fa.setLakeData(ld, lf, LG, entryA);                                           // before the step's own direction: stored, no rebuild
    assert.equal(rebuilds, 0); assert.equal(fa.lentry, entryA);
    fa.setData(gd, GRID, entryA);                                                 // the step's direction: one rebuild, the lake rides in it
    assert.equal(rebuilds, 1); assert.ok(fa.lnodes, 'lake nodes computed');
    assert.ok(Math.abs(at(8, 128)[0]) < 1e-6 && at(8, 128)[1] > 0, 'a lake point moves with the lake direction (from the north): ' + at(8, 128));
    fa.setData({ q: gd.q.slice(), cols: gd.cols, rows: gd.rows }, GRID, entryB);  // the next step's own direction (a new frame object, as every decode is): step A's lake is not drawn
    assert.ok(at(8, 128)[0] < 0 && Math.abs(at(8, 128)[1]) < 0.05 * Math.abs(at(8, 128)[0]), 'another step\'s lake is not used: the model\'s nodes serve: ' + at(8, 128));
    const n = rebuilds;
    const ld2 = { q: ld.q.slice(), cols: ld.cols, rows: ld.rows };
    fa.setLakeData(ld2, lf, LG, entryB);                                         // delivered after the step landed (a new decode): rebuilds at once
    assert.equal(rebuilds, n + 1);
    l._lake = { frame: lf, grid: LG, entry: entryB, lo: HS.lo, hi: HS.hi }; fa._rebuild();
    assert.ok(at(8, 128)[1] > 0, 'with the layer drawing that step\'s lake frame the lake vectors return');
    fa.setLakeData(ld2, lf, LG, entryB); assert.equal(rebuilds, n + 2, 'the same data again: nothing');
    const m = rebuilds; fa.clearLakeData();
    assert.equal(rebuilds, m + 1); assert.equal(fa.ldir, null); assert.equal(fa.lnodes, null); assert.ok(at(8, 128)[0] < 0, 'back to the model\'s nodes');
    fa.clearLakeData(); assert.equal(rebuilds, m + 1, 'nothing to clear: no rebuild');
    fa.setLakeData(ld, lf, LG, entryB); fa.clearData(); assert.equal(fa.ldir, ld, 'a step change leaves the lake data in place (the landing sets the lake first; unused until its step is on)'); assert.equal(fa.entry, null);
    fa.detach();
  } finally { global.document = hadDoc; }
});

// the coast-v1 encoder of coast.test.js (a mirror of tools/coast/build_coast.py) for a lakes set fixture
function leb(v) { const out = []; do { const b = v % 128; v = Math.floor(v / 128); out.push(v ? b | 128 : b); } while (v); return out; }
const zz = (v) => (v < 0 ? -2 * v - 1 : 2 * v);
function encodeCoast(pieces, cell) {
  const body = []; let nr = 0, nv = 0, bx0 = Infinity, by0 = Infinity, bx1 = -Infinity, by1 = -Infinity;
  for (const rings of pieces) {
    let minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
    for (const r of rings) for (let i = 0; i < r.length; i += 2) { minx = Math.min(minx, r[i]); maxx = Math.max(maxx, r[i]); miny = Math.min(miny, r[i + 1]); maxy = Math.max(maxy, r[i + 1]); }
    bx0 = Math.min(bx0, minx); by0 = Math.min(by0, miny); bx1 = Math.max(bx1, maxx); by1 = Math.max(by1, maxy);
    body.push(...leb(zz(minx)), ...leb(zz(miny)), ...leb(maxx - minx), ...leb(maxy - miny), ...leb(rings.length));
    for (const r of rings) {
      body.push(...leb(r.length / 2)); nr++; nv += r.length / 2;
      let px = minx, py = miny;
      for (let i = 0; i < r.length; i += 2) { body.push(...leb(zz(r[i] - px)), ...leb(zz(r[i + 1] - py))); px = r[i]; py = r[i + 1]; }
    }
  }
  const buf = new ArrayBuffer(40 + body.length), dv = new DataView(buf), u8 = new Uint8Array(buf);
  u8.set([67, 83, 84, 49], 0); dv.setUint16(4, cell, true); dv.setUint32(8, 10000, true); dv.setUint32(12, pieces.length, true);
  dv.setUint32(16, nr, true); dv.setUint32(20, nv, true);
  [bx0, by0, bx1, by1].forEach((v, i) => dv.setInt32(24 + i * 4, pieces.length ? v : 0, true));
  u8.set(body, 40);
  return buf;
}
const D = (deg) => Math.round(deg * 10000);
const sq = (x0, y0, x1, y1) => [D(x0), D(y0), D(x1), D(y0), D(x1), D(y1), D(x0), D(y1)];
const rev = (ring) => { const out = []; for (let i = ring.length - 2; i >= 0; i -= 2) out.push(ring[i], ring[i + 1]); return out; };

test('the layer\'s lake coverage: from the lakes coast set (an island cuts its lake out), only inside the lakes\' box, cached per tile; setLakeCoast / setLake / setFrame / clear; _redrawLake', () => {
  const lakeRing = sq(-89.5, 47.3, -87.5, 48.7), island = rev(sq(-88.5, 47.8, -88.3, 48));
  const coast = I.decodeCoast(encodeCoast([[lakeRing, island]], 30));
  let asked = 0;
  const store = { status: 'ok', rev: 0, onChange: null, onNeeded: null, tier0: coast, index: { tier0: { max_zoom: 6 }, tier1: { cell: 5, cells: {} } }, tier1Zoom: () => false, setsFor: () => { asked++; return { sets: [coast], complete: true }; } };
  const bb = I.lakeBoxOf(store);
  assert.ok(bb && bb.x0 < bb.x1 && bb.y0 < bb.y1);
  assert.equal(I.lakeBoxHits(SUPERIOR, bb), true); assert.equal(I.lakeBoxHits({ z: 7, x: SUPERIOR.x + 128, y: SUPERIOR.y }, bb), true, 'a world copy');
  assert.equal(I.lakeBoxHits({ z: 7, x: 10, y: 50 }, bb), false); assert.equal(I.lakeBoxHits(SUPERIOR, null), false);
  assert.equal(I.lakeBoxOf({ tier0: null }), null);
  const l = layer(globalFrame(PATTERN), 'hs', HS, null);
  assert.equal(l._lakeMask, false);
  l.setLakeCoast(store);
  assert.equal(l._lakeCoast, store); assert.equal(l._lakeMask, true); assert.deepEqual(l._lakeBox, bb); assert.equal(typeof store.onChange, 'function');
  const el = tileEl(), cov = l._lakeFor(el, SUPERIOR);
  assert.ok(cov && cov.length === TILE * TILE && asked === 1 && el._ovLakeKey === '7/32/44' && el._ovLakeFinal === true);
  assert.equal(l._lakeFor(el, SUPERIOR), cov, 'cached on the element'); assert.equal(asked, 1);
  const at = (lat, lon) => { const p = I.pixelOf(lat, lon, 7); return cov[p.py * TILE + p.px]; };
  assert.equal(at(48.5, -89), 255, 'lake water'); assert.equal(at(47.9, -88.4), 0, 'the island'); assert.equal(at(47.5, -87.3), 0, 'east of the lake'); assert.equal(at(48.85, -89), 0, 'north of it');
  const far = tileEl();
  assert.equal(l._lakeFor(far, { z: 7, x: 10, y: 50 }), null, 'outside the box: no coverage'); assert.equal(asked, 1, '... and the store not asked');
  // a tile of nothing but lake water: the shared full array
  const inner = I.pixelOf(48.4, -88.7, 11), fullEl = tileEl();
  assert.equal(l._lakeFor(fullEl, { z: 11, x: inner.x, y: inner.y }), I.lakeFull());
  // setLake: the field's code space only; redraws the tiles with coverage (and tells the animator); clear drops it
  l._tiles[key(SUPERIOR)] = { el, coords: SUPERIOR }; l._tiles['10:50:7'] = { el: far, coords: { z: 7, x: 10, y: 50 } };
  let redraws = 0; l.onRedraw = () => redraws++;
  const lf = lakeFrame(lakeFn, HS), data = { frame: lf, grid: LG, entry: { step: 1 }, lo: HS.lo, hi: HS.hi };
  const drawn = []; l._draw = (e, c) => drawn.push(key(c));
  assert.equal(l.setLake(data), true); assert.equal(l._lake, data); assert.deepEqual(drawn, [key(SUPERIOR)], 'only the tile with lake coverage'); assert.equal(redraws, 1);
  assert.equal(l.setLake(data), false, 'the same again: nothing'); assert.equal(redraws, 1);
  assert.throws(() => l.setLake({ frame: lf, grid: LG, entry: {}, lo: 0, hi: 15 }), /code space/);
  assert.throws(() => l.setLake({ frame: { cols: 10, rows: 10, q: new Uint8Array(100) }, grid: LG, entry: {}, lo: HS.lo, hi: HS.hi }), /lake frame/);
  assert.equal(l.setLake(null), true); assert.equal(l._lake, null); assert.equal(drawn.length, 2); assert.equal(l.setLake(null), false);
  l.setLake(data, true); assert.equal(drawn.length, 2, 'quiet: no redraw');
  l._redrawLake(true); assert.equal(drawn.length, 3, 'every tile in the box');
  l.clear(); assert.equal(l._lake, null); assert.equal(l._lakeMask, false);
  // setFrame takes the lake into the one redraw; another field (another code space) without it throws; the mask follows the field
  l.setFrame(globalFrame(PATTERN), GRID, 'hs', HS, I.buildLut('hs', HS.legend), { step: 1 }, data);
  assert.equal(l._lake, data); assert.equal(l._lakeMask, true);
  assert.throws(() => l.setFrame(globalFrame(PATTERN), GRID, 'tp', TP, I.buildLut('tp', TP.legend), { step: 1 }, data), /code space/);
  l.setFrame(globalFrame(PATTERN), GRID, 'wind', { lo: 0, hi: 60, legend: [0, 30], units: 'm/s', interpolation: 'bilinear' }, I.buildLut('wind', [0, 30]), { step: 1 });
  assert.equal(l._lake, null); assert.equal(l._lakeMask, false, 'wind: no lake mask');
  l.setLakeCoast(null); assert.equal(l._lakeCoast, null); assert.equal(store.onChange, null);
});

async function realFrame(name) {
  const buf = fs.readFileSync(path.join(FIX, name));
  return I.decodePngGrey(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength));
}

test('the published run 2026102119: the three f000 frames decode to the lake grid and equal an independent decode at 60 nodes; the sampler is exact at the nodes and matches the reference over a tile', async () => {
  const frames = {};
  for (const name of ['hs', 'tp', 'pdir']) {
    const fr = await realFrame(name + '_f000.png');
    I.validateLakeGrid(LG, fr);
    for (const [r, c, code] of SAMPLES[name]) assert.equal(fr.q[r * LG.cols + c], code, name + ' node ' + r + ',' + c);
    frames[name] = fr;
  }
  assert.equal(SAMPLES.run, MAN.run); assert.deepEqual(SAMPLES.grid, LG);
  assert.equal(I.dirFieldOk(MAN.fields.pdir), true);
  const out = new Float64Array(TILE * TILE), full = I.lakeFull();
  let values = 0;
  for (const coords of [SUPERIOR, { z: 8, x: 66, y: 94 }, { z: 6, x: 17, y: 23 }]) {
    I.sampleLake(frames.hs, LG, coords, 0, 0, 1, TILE, out.fill(-1), full);
    for (let py = 0; py < TILE; py += 5) for (let px = 0; px < TILE; px += 3) {
      const ll = I.tilePixelLatLng(coords, px, py), ref = refSample(frames.hs, ll.lat, ll.lng);
      assert.ok(Math.abs(out[py * TILE + px] - ref) < 1e-9, key(coords) + ' ' + px + ',' + py);
      if (ref) values++;
    }
  }
  assert.ok(values > 2000, values + ' lake values sampled');
  // the lake direction frame has a value exactly where the period has one (the job's calm rule)
  let same = 0, diff = 0;
  for (let i = 0; i < frames.tp.q.length; i += 7) { if (!!frames.tp.q[i] === !!frames.pdir.q[i]) same++; else diff++; }
  assert.equal(diff, 0, 'period and direction share their cells'); assert.ok(same > 40000);
});

test('the credit names the lakes while a lake frame is drawn under the model (never under SST); the readout says "no data" on lake water that shows nothing', () => {
  const D = I.MODEL_DEFS.gfswave, credit = (mode, hasLake) => I.Overlay.prototype._attribution.call({ mode, layer: { hasLake: () => hasLake, _masked: () => false }, coast: null, _def: () => D });
  assert.equal(credit('model', true), D.attribution + I.LAKE_ATTRIBUTION);
  assert.equal(credit('model', false), D.attribution);
  assert.ok(credit('obs', true).indexOf('GLWU') < 0, 'SST: no lake credit');
  assert.equal(I.LAKE_ATTRIBUTION.replace(/<[^>]+>/g, ''), ' · Lakes: NOAA GLWU');
  assert.match(I.LAKE_ATTRIBUTION, /NDFD winds/); assert.match(I.LAKE_ATTRIBUTION, /Not an official NWS product/);
  // the readout: the real _bindReadout on a fake map; the layer answers per point
  const made = [], handlers = {};
  const mkEl = () => { const e = { hidden: false, style: {}, textContent: '', className: '', remove() {} }; made.push(e); return e; };
  const hadDoc = global.document; global.document = { createElement: mkEl };
  try {
    const container = { appendChild() {}, addEventListener() {}, removeEventListener() {} };
    const ctx = { map: { getContainer: () => container, getZoom: () => 7 }, opts: { getUnit: () => 'US' }, _listeners: [],
      _on(ev, fn) { handlers[ev] = fn; }, _hpaText: () => null,
      layer: { fdef: HS, field: 'hs', _tileZoom: 7, readoutAt: (lat) => (lat > 45 ? 1.5 : null), lakeAt: (lat) => lat > 40 } };
    I.Overlay.prototype._bindReadout.call(ctx);
    const el = ctx.readout, move = (lat) => handlers.mousemove({ latlng: { lat, lng: -87 }, containerPoint: { x: 10, y: 20 }, originalEvent: { pointerType: 'mouse', target: null } });
    move(46); assert.equal(el.hidden, false); assert.equal(el.textContent, '4.9 ft', 'a value');
    move(42); assert.equal(el.hidden, false); assert.equal(el.textContent, 'no data', 'blank lake water');
    move(30); assert.equal(el.hidden, true, 'land / nothing: no label');
    ctx.layer.lakeAt = () => false; move(42); assert.equal(el.hidden, true);
  } finally { global.document = hadDoc; }
});
