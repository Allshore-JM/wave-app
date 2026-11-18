'use strict';
// RRFS as its own Wind-speed model (plan section 99 D2) for static_overlay/overlay.js, no browser: the regional manifest
// (the real dry-run manifest of run 2026111718 and its rejections), the region sampler against an independent bilinear
// (a real Hawaii frame, tests/fixtures/rrfs_frames), the date line, the bundle's priority and blank elsewhere, the layer
// (tiles, readout = the drawn pixel), the regions near a view (Oahu, the West Coast, Alaska, the open Atlantic), the
// particles (vectors only inside the regions, born inside their boxes) and the field gating.
//   node --test tests/overlay/rrfs.test.js
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
const SAMP = JSON.parse(fs.readFileSync(path.join(FIX, 'hi_f012_samples.json'), 'utf8'));
const HI = SAMP.grid;
const HIQ = { q: new Uint8Array(fs.readFileSync(path.join(FIX, 'hi_wind_f012.bin'))), cols: HI.cols, rows: HI.rows };
const HIH = { cols: Math.ceil(HI.cols / 2), rows: Math.ceil(HI.rows / 2), lat0: HI.lat0, lon0: HI.lon0, dlat: 2 * HI.dlat, dlon: 2 * HI.dlon, registration: 'center', lon_periodic: false };
const HID = { q: new Uint8Array(fs.readFileSync(path.join(FIX, 'hi_wdir_f012.bin'))), cols: HIH.cols, rows: HIH.rows };   // the direction: every second node
const PREFIX = 'rrfs/v1/frames';
const TILE = 256;

// ------------------------------------------------------------------------------------------------- the manifest

test('the real dry-run manifest validates and is normalised (regions in order, one wind scale, the direction field)', () => {
  const m = I.validateRegionalManifest(MAN(), PREFIX);
  assert.equal(m.regional, true);
  assert.deepEqual(m.regionList.map((r) => r.name), ['hi', 'pr', 'conus', 'ak']);
  assert.equal(m.frames.length, 85);
  assert.equal(m.fields.wind.lo, 0); assert.ok(Math.abs(m.fields.wind.hi - 120 * 1852 / 3600) < 1e-9, 'the GFS frames’ wind range');
  assert.equal(m.fields.wdir.circular, true);
  assert.equal(I.regionFrameKey(m.frames[0], 'conus', 'wind'), 'rrfs/v1/frames/2026111718/conus/wind/f000.png');
  assert.equal(I.regionFrameKey(m.frames[0], 'nope', 'wind'), null);
  for (const r of m.regionList) assert.ok(r.grid.cols * r.grid.rows <= 1 << 20, r.name);
  const ak = m.regionList[3].grid;
  assert.ok(ak.lon0 + (ak.cols - 1) * ak.dlon > 180, 'Alaska crosses the date line');
});

test('the manifest rejections', () => {
  const bad = [
    ['schema', (m) => { m.schema = 3; }],
    ['encoding', (m) => { m.encoding = 'u16'; }],
    ['incomplete', (m) => { m.complete = false; }],
    ['run', (m) => { m.run = '2026111718x'; }],
    ['no frames', (m) => { m.frames = []; }],
    ['too many frames', (m) => { m.frames = Array(513).fill(m.frames[0]); }],
    ['grid too big', (m) => { m.regions.conus.grid.cols = 2000; }],
    ['grid periodic', (m) => { m.regions.hi.grid.lon_periodic = true; }],
    ['lon0 past 180', (m) => { m.regions.ak.grid.lon0 = 190; }],
    ['row 0 south', (m) => { m.regions.pr.grid.dlat = 0.025; }],
    ['wind scales disagree', (m) => { m.regions.ak.fields.wind.hi = 50; }],
    ['legends disagree', (m) => { m.regions.ak.fields.wind.legend = [0, 20]; }],
    ['no direction', (m) => { delete m.regions.hi.fields.wdir; }],
    ['steps not increasing', (m) => { m.frames[3].step = m.frames[2].step; }],
    ['time not increasing', (m) => { m.frames[3].valid_utc = m.frames[2].valid_utc; }],
    ['unknown region in files', (m) => { m.frames[1].files.xx = { wind: m.frames[1].files.hi.wind }; }],
    ['unknown field in files', (m) => { m.frames[1].files.hi.pres = m.frames[1].files.hi.wind; }],
    ['key outside the prefix', (m) => { m.frames[1].files.hi.wind = 'gfswave/0p25/v1/x.png'; }],
    ['key climbing out', (m) => { m.frames[1].files.hi.wind = 'rrfs/v1/frames/../../x.png'; }],
    ['bad region name', (m) => { m.regions['Bad Name'] = m.regions.hi; }],
  ];
  for (const [what, edit] of bad) {
    const m = MAN(); edit(m);
    assert.throws(() => I.validateRegionalManifest(m, PREFIX), Error, what);
  }
  const m = MAN(); delete m.frames[4].files.ak;                       // a frame without one region is fine (drawn blank there)
  assert.doesNotThrow(() => I.validateRegionalManifest(m, PREFIX));
});

// ---------------------------------------------------------------------------------------------- the region sampler

test('regionCode = an independent bilinear over the real Hawaii frame (409 points incl. outside and the 0-360 longitude)', () => {
  let nz = 0;
  for (const [lat, lon, want] of SAMP.samples) {
    const got = I.regionCode(HIQ, HI, lat, lon);
    assert.ok(Math.abs(got - want) < 1e-9, `${lat},${lon}: ${got} vs ${want}`);
    if (want) nz++;
  }
  assert.ok(nz > 250);
  assert.equal(I.regionCode(HIQ, HI, 21.3, -157.85), I.regionCode(HIQ, HI, 21.3, 202.15), 'world copies sample alike');
  assert.ok(I.regionCode(HIQ, HI, 21.3, -157.85) > 1, 'Honolulu has wind');
});

const AKG = { cols: 1661, rows: 501, lat0: 75.46, lon0: 150.15, dlat: -0.07, dlon: 0.07, registration: 'center', lon_periodic: false };
function akFrame() {                                        // code = 2 + column / 10 (east of 180 included), nothing in row 0
  const q = new Uint8Array(AKG.cols * AKG.rows);
  for (let r = 1; r < AKG.rows; r++) for (let c = 0; c < AKG.cols; c++) q[r * AKG.cols + c] = 2 + Math.floor(c / 10) % 250;
  return { q, cols: AKG.cols, rows: AKG.rows };
}

test('Alaska across the date line: -178 and 182 are the same column; east of the box is outside', () => {
  const f = akFrame();
  const c180 = Math.round((180 - AKG.lon0) / AKG.dlon);       // the node at 180
  assert.equal(I.regionCode(f, AKG, 52.0, 180), 2 + Math.floor(c180 / 10) % 250);
  assert.equal(I.regionCode(f, AKG, 52.0, -178.0), I.regionCode(f, AKG, 52.0, 182.0));
  assert.ok(I.regionCode(f, AKG, 52.0, -178.0) > I.regionCode(f, AKG, 52.0, 178.0), 'east of 180 is further along the row');
  assert.equal(I.regionCode(f, AKG, 52.0, -90.0), 0, 'east of the box');
  assert.equal(I.regionCode(f, AKG, 52.0, 140.0), 0, 'west of the box');
  assert.equal(I.regionCode(f, AKG, 30.0, -150.0), 0, 'south of the box');
  assert.equal(I.inRegionBox(AKG, 60, -150), true); assert.equal(I.inRegionBox(AKG, 60, 140), false);
  assert.ok(Math.abs(I.regionLon(AKG, -178) - 182) < 1e-9);
});

test('the bundle: the first region with data wins, blank outside every region; tiles = per-pixel regionsCode', () => {
  const g1 = { cols: 11, rows: 11, lat0: 30, lon0: -130, dlat: -1, dlon: 1, registration: 'center', lon_periodic: false };
  const g2 = { cols: 21, rows: 21, lat0: 35, lon0: -135, dlat: -1, dlon: 1, registration: 'center', lon_periodic: false };
  const f1 = { q: new Uint8Array(121).fill(50), cols: 11, rows: 11 }, f2 = { q: new Uint8Array(441).fill(200), cols: 21, rows: 21 };
  for (let c = 0; c < 11; c++) f1.q[5 * 11 + c] = 0;             // a row of g1 left empty: g2 shows through there
  const parts = [{ name: 'a', grid: g1, frame: f1 }, { name: 'b', grid: g2, frame: f2 }];
  assert.equal(I.regionsCode(parts, 25, -125), 200, 'g1 row 5 is empty: g2');
  assert.equal(I.regionsCode(parts, 27, -125), 50, 'g1 has data: g1');
  assert.equal(I.regionsCode(parts, 33, -125), 200, 'only g2');
  assert.equal(I.regionsCode(parts, 40, -125), 0, 'neither');
  const coords = { z: 4, x: 2, y: 6 }, out = new Float64Array(TILE * TILE);
  I.sampleRegions(parts, coords, 0, 0, 1, TILE, out);
  const n = TILE * Math.pow(2, coords.z);
  for (const [px, py] of [[0, 0], [100, 37], [255, 255], [17, 200], [128, 128]]) {
    const lon = I.lngOfWorldX(coords.x * TILE + px + 0.5, n), lat = I.latOfWorldY(coords.y * TILE + py + 0.5, n);
    assert.equal(out[py * TILE + px], I.regionsCode(parts, lat, lon), `${px},${py}`);
  }
});

test('the layer draws a bundle: tiles, valueAt and readoutAt agree with the sampler; nothing outside', () => {
  const m = I.validateRegionalManifest(MAN(), PREFIX), fd = m.fields.wind;
  const l = new I.ModelGridLayer({ opacity: 1 });
  const bundle = { regional: true, parts: [{ name: 'hi', grid: HI, frame: HIQ }], cols: 0, rows: 0, q: new Uint8Array(0) };
  l.setFrame(bundle, undefined, 'wind', fd, I.buildLut('wind', fd.legend), m.frames[12], null);
  assert.equal(l.hasFrame(), true); assert.equal(l._regions.length, 1);
  const lat = 21.3, lon = -157.85, code = I.regionCode(HIQ, HI, lat, lon);
  assert.ok(Math.abs(l.valueAt(lat, lon) - (fd.lo + (code - 1) / 254 * (fd.hi - fd.lo))) < 1e-9);
  assert.equal(l.valueAt(30, -140), null, 'outside: no value');
  const z = 8, p = I.pixelOf(lat, lon, z), out = new Float64Array(TILE * TILE);
  l.tileCodes({ z, x: p.x, y: p.y }, out);
  l._tiles[p.x + ':' + p.y + ':' + z] = { el: {}, coords: { z, x: p.x, y: p.y } };
  const drawn = out[p.py * TILE + p.px], v = l.readoutAt(lat, lon, z);
  assert.ok(drawn > 0 && Math.abs(v - (fd.lo + (drawn - 1) / 254 * (fd.hi - fd.lo))) < 1e-9, 'readout = the drawn pixel');
  assert.throws(() => l.setFrame({ regional: true, parts: [{ name: 'hi', grid: HI, frame: { q: new Uint8Array(5), cols: 5, rows: 1 } }] }, undefined, 'wind', fd, null, null, null), /region frame/);
  assert.throws(() => l.setFrame(bundle, undefined, 'wind', fd, null, null, { frame: HIQ, grid: HI }), /no lake/);
  l._tiles = {}; l.clear(); assert.equal(l._regions, null);
});

// ------------------------------------------------------------------------------------------- the regions near a view

function viewAt(lat, lng, z, w, h) {
  const sc = Math.pow(2, z), p = I.worldXY(lng, lat);
  return { z, w: w || 1920, h: h || 1080, ox: p[0] * sc - (w || 1920) / 2, oy: p[1] * sc - (h || 1080) / 2, zt: Math.round(z) };
}

test('the regions near a view (their DATA within a tenth of a screen, not their boxes): Oahu, the West Coast, Alaska, the open Atlantic', () => {
  const list = I.validateRegionalManifest(MAN(), PREFIX).regionList;
  assert.equal(I.REGION_NEAR_SCREENS, 0.1);
  assert.deepEqual(I.regionsNear(list, viewAt(21.4, -157.9, 9)), ['hi']);
  assert.deepEqual(I.regionsNear(list, viewAt(21.4, -157.9, 9, 375, 812)), ['hi'], 'a phone');
  assert.deepEqual(I.regionsNear(list, viewAt(37.5, -122.5, 9)), ['conus'], 'San Francisco');
  // a 1080p California view at zoom 7: Alaska's own data off Oregon (west of CONUS's edge) is more than a tenth of a
  // screen away (at half a screen both were decoded: +73 MB heap peaks at shark); Seattle has both on screen
  assert.deepEqual(I.regionsNear(list, viewAt(37.5, -122.5, 7)), ['conus']);
  assert.deepEqual(I.regionsNear(list, viewAt(47.6, -122.3, 7)), ['conus', 'ak'], 'Seattle');
  assert.deepEqual(I.regionsNear(list, viewAt(61.0, -150.0, 6)), ['ak'], 'Alaska at zoom 6');
  const ak = list.find((r) => r.name === 'ak');
  assert.ok(I.lakeNearView(viewAt(41.5, -122.4, 8), ak.grid, 0.1), 'Redding, CA is inside Alaska\u2019s box ...');
  assert.deepEqual(I.regionsNear(list, viewAt(41.5, -122.4, 8)), ['conus'], '... but not near its data (the cover mask)');
  assert.deepEqual(I.regionsNear(list, viewAt(61.2, -150, 8)), ['ak'], 'Anchorage');
  assert.deepEqual(I.regionsNear(list, viewAt(52, 179.5, 7)), ['ak'], 'the Aleutians at the date line');
  assert.deepEqual(I.regionsNear(list, viewAt(18.4, -66.1, 9)), ['pr'], 'San Juan');
  assert.deepEqual(I.regionsNear(list, viewAt(30, -40, 7)), [], 'nothing of RRFS near');
  assert.deepEqual(I.regionsNear(list, viewAt(35, -150, 3)), ['hi', 'pr', 'conus', 'ak'], 'a wide view: all four');
});

test('the coverage mask decodes the job\u2019s hex (north-west first, 4 cells a digit) and rejects a malformed one', () => {
  const m = MAN();
  const hi = I.validateRegionalManifest(m, PREFIX).regionList[0].cover;
  assert.equal(hi.rows * hi.cols, 54); assert.ok(hi.bits.every((b) => b === 1));
  for (const bad of [(c) => { c.hex = c.hex.slice(1); }, (c) => { c.hex = 'G' + c.hex.slice(1); }, (c) => { c.res = 0; }, (c) => { c.rows = 0; }, (c) => { delete c.hex; }]) {
    const mm = MAN(); bad(mm.regions.pr.cover);
    assert.throws(() => I.validateRegionalManifest(mm, PREFIX), /manifest incomplete/);
  }
  const mm = MAN(); delete mm.signature;
  assert.throws(() => I.validateRegionalManifest(mm, PREFIX), /manifest incomplete/, 'the signature is the manifest\u2019s identity (G67a P1-1)');
});

// ---------------------------------------------------------------------------------------------------- the particles

test('the particles: vectors only inside the regions, from the region’s own speed and direction; born inside the boxes', () => {
  const m = I.validateRegionalManifest(MAN(), PREFIX), fd = m.fields.wind;
  const v = viewAt(20.5, -157.5, 6, 1200, 800);
  assert.equal(HID.q.length, HIH.cols * HIH.rows);
  assert.ok(I.halfOf(HI, I.validateRegionalManifest(MAN(), PREFIX).regionList[0].gridHalf));
  const vf = I.regionFlowField(v, 8, [{ lf: HIQ, lg: HI, ld: HID, dg: HIH, lo: fd.lo, hi: fd.hi }], 'wind', null);
  const n = 256 * Math.pow(2, v.z);
  let inside = 0, moving = 0;
  for (let j = 0; j < vf.rows; j++) for (let i = 0; i < vf.cols; i++) {
    const lat = I.latOfWorldY(v.oy + j * 8, n), lon = I.lngOfWorldX(v.ox + i * 8, n), k = j * vf.cols + i;
    const has = vf.u[k] !== 0 || vf.v[k] !== 0;
    if (!I.inRegionBox(HI, lat, lon)) assert.equal(has, false, `${lat},${lon} is outside: no vector`);
    else { inside++; if (has) moving++; }
  }
  assert.ok(inside > 500 && moving > 0.9 * inside, `${moving} of ${inside}`);
  // one node's vector = lakeVec of its own codes (wind FROM -> TOWARD, Mercator stretch)
  const lat = 21.3, lon = -157.85, fx = (I.regionLon(HI, lon) - HI.lon0) / HI.dlon, fy = (HI.lat0 - lat) / -HI.dlat;
  assert.ok(fx > 0 && fy > 0);
  const rects = I.regionRects(v, [HI]);
  assert.equal(rects.length, 1);
  const [x0, y0, x1, y1] = rects[0];
  assert.ok(x0 >= 0 && y0 >= 0 && x1 <= v.w && y1 <= v.h && x1 > x0 && y1 > y0);
  const a = I.worldXY(HI.lon0, HI.lat0), sc = Math.pow(2, v.z);
  assert.ok(Math.abs(Math.max(0, a[0] * sc - v.ox) - x0) < 1e-6 && Math.abs(Math.max(0, a[1] * sc - v.oy) - y0) < 1e-6);
  const akv = viewAt(52, 179.5, 5, 1200, 800);
  assert.ok(I.regionRects(akv, [AKG]).length >= 1, 'the date line: the box is found on the view’s world copy');
  assert.equal(I.regionRects(viewAt(30, -40, 7), [HI]).length, 0);
});

test('the animator rebuilds a regional step from the bundle pair (no global frame) and births particles in the boxes', () => {
  const m = I.validateRegionalManifest(MAN(), PREFIX), fd = m.fields.wind;
  const v = viewAt(20.5, -157.5, 6, 1200, 800);
  const layer = new I.ModelGridLayer({ opacity: 1 });
  const bundle = { regional: true, parts: [{ name: 'hi', grid: HI, frame: HIQ }], cols: 0, rows: 0, q: new Uint8Array(0) };
  layer.setFrame(bundle, undefined, 'wind', fd, null, m.frames[12], null);
  const fa = Object.create(I.FlowAnimator.prototype);
  Object.assign(fa, { canvas: {}, ctx: null, layer, field: 'wind', dir: { regional: true, parts: [{ name: 'hi', grid: HIH, frame: HID }] }, v: null, particles: null, suspended: 0 });
  fa.view = () => v; fa._place = () => {}; fa._clear = () => {}; fa._start = () => { fa.started = true; }; fa.stop = () => {};
  fa._rebuild();
  assert.equal(fa.mode, 'particles'); assert.ok(fa.started); assert.ok(fa.vf && fa.vf.u.some((x) => x !== 0));
  assert.equal(fa.spawn.length, 1);
  const [x0, y0, x1, y1] = fa.spawn[0];
  for (let k = 0; k < 200; k++) {
    const p = fa.particles[k * 4], q = fa.particles[k * 4 + 1];
    assert.ok(p >= x0 && p <= x1 && q >= y0 && q <= y1, 'born inside the Hawaii box');
  }
  for (let k = 0; k < fa.count; k++) assert.equal(fa.particles[k * 4 + 2] >= 0, fa._flowing(fa.particles[k * 4], fa.particles[k * 4 + 1]), 'section 100: live only where the region flows, else dormant');
  fa.dir = { regional: true, parts: [{ name: 'pr', grid: HIH, frame: HID }] };  // a direction of another region: no pair, no vectors
  fa._rebuild();
  assert.ok(!fa.vf.u.some((x) => x !== 0));
  for (let k = 0; k < 50; k++) { fa._respawn(k * 4, v); assert.ok(fa.particles[k * 4 + 2] < 0, 'section 100: born in a box without vectors: dormant'); }
});

test('the tile sampler equals regionsCode at EVERY pixel (real Hawaii frame; Alaska across the date line; two regions overlapping)', () => {
  const ak = akFrame();
  const g2 = { cols: 21, rows: 21, lat0: 60, lon0: 170, dlat: -1, dlon: 1, registration: 'center', lon_periodic: false };
  const f2 = { q: new Uint8Array(441).map((_, i) => (i % 7 ? 100 + (i % 50) : 0)), cols: 21, rows: 21 };
  const cases = [
    [[{ name: 'hi', grid: HI, frame: HIQ }], { z: 7, x: 7, y: 56 }],                // Oahu
    [[{ name: 'hi', grid: HI, frame: HIQ }], { z: 5, x: 1, y: 13 }],                // Hawaii's box edge
    [[{ name: 'ak', grid: AKG, frame: ak }], { z: 4, x: 15, y: 4 }],                // west of 180
    [[{ name: 'ak', grid: AKG, frame: ak }], { z: 4, x: 0, y: 4 }],                 // east of 180 (the world's first column)
    [[{ name: 'x', grid: g2, frame: f2 }, { name: 'ak', grid: AKG, frame: ak }], { z: 4, x: 15, y: 4 }],
  ];
  for (const [parts, coords] of cases) {
    const out = new Float64Array(TILE * TILE), n = TILE * Math.pow(2, coords.z);
    I.sampleRegions(parts, coords, 0, 0, 1, TILE, out);
    let nz = 0;
    for (let py = 0; py < TILE; py++) for (let px = 0; px < TILE; px++) {
      const lon = I.lngOfWorldX(coords.x * TILE + px + 0.5, n), lat = I.latOfWorldY(coords.y * TILE + py + 0.5, n);
      const want = I.regionsCode(parts, lat, lon), got = out[py * TILE + px];
      if (got !== want) assert.fail(`${JSON.stringify(coords)} ${px},${py}: ${got} vs ${want}`);
      if (got) nz++;
    }
    assert.ok(nz > 0, JSON.stringify(coords) + ' has data');
  }
});


// ------------------------------------------------------------------------------------------------- G67 fixes, pinned

test('the direction is the half grid (every second node from the first); a manifest without it, or with another, is refused', () => {
  const m = MAN();
  assert.equal(m.regions.hi.fields.wdir.resolutions[0], 'half');
  for (const bad of [(r) => { delete r.grid_half; }, (r) => { r.grid_half.cols += 1; }, (r) => { r.grid_half.dlat = r.grid.dlat; }, (r) => { r.grid_half.lon0 += 0.01; },
    (r) => { r.fields.wdir.resolutions = ['full']; }]) {
    const mm = MAN(); bad(mm.regions.conus);
    assert.throws(() => I.validateRegionalManifest(mm, PREFIX), /manifest incomplete/);
  }
});

function uniform(lat0, rows, cols, d, code) {                   // a region grid of 1-deg cells, every node `code`
  const g = { cols, rows, lat0, lon0: -170, dlat: -d, dlon: d, registration: 'center', lon_periodic: false };
  const h = { cols: Math.ceil(cols / 2), rows: Math.ceil(rows / 2), lat0, lon0: -170, dlat: -2 * d, dlon: 2 * d, registration: 'center', lon_periodic: false };
  return { g, h, f: { q: new Uint8Array(cols * rows).fill(code), cols, rows } };
}

test('the particles: speed x the Mercator stretch (G67 M22) and the FIRST region with data wins (M23)', () => {
  const fd = { lo: 0, hi: 61.7, legend: [0, 30.9] };
  const A = uniform(70, 71, 41, 1, 100);                         // 70 N .. 0, 170 W .. 130 W, speed code 100
  const dirE = { q: new Uint8Array(A.h.cols * A.h.rows).fill(1 + Math.round(270 * 254 / 360) % 254), cols: A.h.cols, rows: A.h.rows };   // from the west
  const dirN = { q: new Uint8Array(A.h.cols * A.h.rows).fill(1), cols: A.h.cols, rows: A.h.rows };                                         // from the north
  const view = { z: 2, w: 1024, h: 1024, ox: 0, oy: 0 };                // the whole zoom-2 world
  const sc = 4, p0 = I.worldXY(-150, 60), p1 = I.worldXY(-150, 1);
  const vf = I.regionFlowField(view, 4, [{ lf: A.f, lg: A.g, ld: dirE, dg: A.h, lo: fd.lo, hi: fd.hi }], 'wind', null);
  const at = (p) => { const i = Math.round(p[0] * sc / 4), j = Math.round(p[1] * sc / 4); return [vf.u[j * vf.cols + i], vf.v[j * vf.cols + i]]; };
  const a = at(p0), b = at(p1);
  assert.ok(a[0] > 0 && Math.abs(a[1]) < 0.02 * a[0] && b[0] > 0, 'a westerly blows east (code 192 = 270.7 deg)');
  const lat60 = I.latOfWorldY(Math.round(p0[1] * sc / 4) * 4, 256 * sc), lat1 = I.latOfWorldY(Math.round(p1[1] * sc / 4) * 4, 256 * sc);
  const k = (lat) => Math.min(3, 1 / Math.cos(lat * Math.PI / 180));
  assert.ok(Math.abs(a[0] / b[0] - k(lat60) / k(lat1)) < 0.02, 'the Mercator stretch: ' + (a[0] / b[0]) + ' vs ' + k(lat60) / k(lat1));
  const vf2 = I.regionFlowField(view, 4, [{ lf: A.f, lg: A.g, ld: dirN, dg: A.h, lo: fd.lo, hi: fd.hi }, { lf: A.f, lg: A.g, ld: dirE, dg: A.h, lo: fd.lo, hi: fd.hi }], 'wind', null);
  const j0 = Math.round(p0[1] * sc / 4), i0 = Math.round(p0[0] * sc / 4);
  assert.ok(vf2.v[j0 * vf2.cols + i0] > 0 && Math.abs(vf2.u[j0 * vf2.cols + i0]) < 1e-6, 'the first region (a northerly: south, screen down) wins');
});

test('coverNear sees the southernmost cell row (G67 M26), coverHas reads one cell, coverRects find the date line on either world copy (M6)', () => {
  const cv = { res: 1, lat0: 50, lon0: 170, rows: 10, cols: 30, bits: new Uint8Array(300) };
  for (let c = 5; c < 20; c++) cv.bits[9 * 30 + c] = 1;           // only the southern row: 40-41 N, 175 E .. 170 W
  const v = viewAt(40.5, 180, 8, 400, 120);
  assert.equal(I.coverNear(cv, v, 0), true, 'the south row is near');
  assert.equal(I.coverHas(cv, 40.5, -178), true); assert.equal(I.coverHas(cv, 40.5, 182), true); assert.equal(I.coverHas(cv, 41.5, -178), false);
  assert.equal(I.coverHas(cv, 40.5, 172), false, 'a cell without data');
  const r1 = I.coverRects(v, cv, 4000), sc = Math.pow(2, 8), n = 256 * sc;
  const v2 = Object.assign({}, v, { ox: v.ox + n });               // the same place on the next world copy
  const r2 = I.coverRects(v2, cv, 4000);
  assert.ok(r1.length > 0 && r1.length === r2.length, 'both copies: ' + r1.length + ' / ' + r2.length);
  for (const [x0, y0, x1, y1] of r1) assert.ok(x0 >= 0 && y0 >= 0 && x1 <= v.w && y1 <= v.h && x1 > x0 && y1 > y0);
  assert.equal(I.coverRects(viewAt(35, -150, 1, 1920, 1080), { res: 1, lat0: 80, lon0: -180, rows: 160, cols: 360, bits: new Uint8Array(57600).fill(1) }, 4000), null, 'past the cap: null (the boxes then)');
});
