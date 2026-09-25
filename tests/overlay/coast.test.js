'use strict';
// Coastline clip tests for static_overlay/overlay.js (no browser): the coast-v1 decoder, tile
// geometry, the scanline reference rasteriser, compose, the clipped readout and the CoastStore.
// The canvas rasteriser has no canvas here, so `rasterise` falls back to the scanline version;
// the browser path is verified on the test site.  node --test tests/overlay/
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
const PATTERN = (r, c) => (r >= 300 && r < 320 && c >= 100 && c < 130) ? 0 : 1 + ((r * 7 + c * 3) % 254);

// A JS encoder for the coast-v1 format (mirror of tools/coast/build_coast.py) to build fixtures.
function leb(v) { const out = []; do { const b = v % 128; v = Math.floor(v / 128); out.push(v ? b | 128 : b); } while (v); return out; }
const zz = (v) => (v < 0 ? -2 * v - 1 : 2 * v);
function encodeCoast(pieces, cell) {                   // pieces: [[ring, ...]], ring = flat [x, y, ...] in 1e-4 deg
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
const sq = (x0, y0, x1, y1) => [D(x0), D(y0), D(x1), D(y0), D(x1), D(y1), D(x0), D(y1)];          // CCW in (lon, lat)
const ISLAND = sq(10, 10, 11, 11), ANTARCTICA = [D(-180), D(-90), D(180), D(-90), D(180), D(-63), D(-180), D(-63)];
const OAHU = sq(-158.28, 21.26, -157.65, 21.71);
function cellBody(url) {                                          // a valid coast-v1 body for the cell named in the URL
  const m = /\/f\/(-?\d+)_(-?\d+)\.bin$/.exec(url); if (!m) return null;
  const lat0 = +m[1], lon0 = +m[2];
  return (lat0 === 20 && lon0 === -160) ? encodeCoast([[OAHU]], 5) : encodeCoast([[sq(lon0 + 1, lat0 + 1, lon0 + 2, lat0 + 2)]], 5);
}
function fakeStore(sets, complete) { return { status: 'ok', rev: 0, onChange: null, setsFor: () => ({ sets, complete: complete !== false }) }; }
const settle = async () => { for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r)); };

test('decodeCoast: varint/zigzag round trip, Mercator projection with the polar clamp, bboxes, rejects bad input', () => {
  const c = I.decodeCoast(encodeCoast([[ISLAND], [ANTARCTICA], [OAHU]], 30));
  assert.equal(c.cell, 30); assert.equal(c.n, 3); assert.equal(c.ringStart[3], 3); assert.equal(c.vertStart[3], 12);
  const w = I.worldXY(10, 10);
  assert.ok(Math.abs(c.xy[0] - w[0]) < 1e-4 && Math.abs(c.xy[1] - w[1]) < 1e-4);
  assert.ok(Math.abs(c.box[0] - w[0]) < 1e-4 && Math.abs(c.box[2] - I.worldXY(11, 10)[0]) < 1e-4);
  assert.ok(Math.abs(c.box[4] - 0) < 1e-6 && Math.abs(c.box[6] - 256) < 1e-6 && Math.abs(c.box[7] - 256) < 1e-6);   // Antarctica spans the world, clamped to y = 256
  assert.ok(c.box[5] > 0 && c.box[5] < 256);
  assert.equal(c.bytes, 12 * 8);
  const good = encodeCoast([[ISLAND]], 5);
  assert.throws(() => I.decodeCoast(good.slice(0, 40)), /decode failed/);                           // truncated stream
  const bad = good.slice(0); new Uint8Array(bad)[0] = 88;
  assert.throws(() => I.decodeCoast(bad), /decode failed/);
  const extra = new Uint8Array(good.byteLength + 1); extra.set(new Uint8Array(good)); extra[good.byteLength] = 5;
  assert.throws(() => I.decodeCoast(extra.buffer), /decode failed/);                                // trailing bytes
  assert.throws(() => I.decodeCoast(new ArrayBuffer(9 * 1024 * 1024)), /decode failed/);
});

test('coastCellsForTile: 5-degree cells a tile touches, world copies, the dateline and the poles', () => {
  const cells = I.coastCellsForTile({ z: 7, x: 7, y: 56 }, 5);                                  // 19.3-21.9 N, 160.3-157.5 W
  assert.deepEqual(cells, ['15_-165', '15_-160', '20_-165', '20_-160']);
  assert.deepEqual(I.coastCellsForTile({ z: 7, x: 7 + 128, y: 56 }, 5), cells);
  assert.deepEqual(I.coastCellsForTile({ z: 7, x: 7 - 128, y: 56 }, 5), cells);
  assert.equal(I.coastCellsForTile({ z: 3, x: 0, y: 3 }, 5).filter((n) => n.endsWith('_-180')).length, 9);   // -180..-135, 0..40.9 N -> 9 lat cells
  assert.equal(I.coastCellsForTile({ z: 3, x: 7, y: 3 }, 5).filter((n) => n.endsWith('_175')).length, 9);
  const polar = I.coastCellsForTile({ z: 2, x: 0, y: 3 }, 5);                                  // -66.5..-85.05 -> cells -90..-70
  assert.ok(polar.indexOf('-90_-180') >= 0 && polar.indexOf('-70_-95') >= 0 && polar.indexOf('-65_-180') < 0 && polar.length === 5 * 18);
  assert.deepEqual(I.coastCellsForTile({ z: 7, x: 0, y: 63 }, 5), ['0_-180']);                 // 2.8..0 N: the equator edge belongs to the cell above
  assert.deepEqual(I.coastCellsForTile({ z: 7, x: 0, y: 64 }, 5), ['-5_-180']);                // 0..-2.8: the one below
});

test('landPathsForTile: bbox selection, sub-pixel pieces skipped, tile pixel space, world copies identical', () => {
  const c = I.decodeCoast(encodeCoast([[ISLAND], [OAHU], [sq(-158.0, 21.5, -157.9999, 21.5001)]], 5));
  const oahuTile = { z: 8, x: 15, y: 112 };                                                   // 20.6-21.9 N, 158.9-157.5 W (z8: 1.4 deg)
  const paths = I.landPathsForTile(oahuTile, [c]);
  assert.equal(paths.length, 1);                                                               // Oahu only: the islet is sub-pixel, the island far away
  const p = paths[0];
  assert.equal(p.length, 8);
  for (let i = 0; i < 8; i += 2) assert.ok(p[i] > -300 && p[i] < 556 && p[i + 1] > -300 && p[i + 1] < 556, 'near the tile');
  const [x0, y0] = [p[0], p[1]], exp = I.forwardPixel(21.26, -158.28, 8);
  assert.ok(Math.abs(x0 - (exp.x - 15 * 256)) < 0.05 && Math.abs(y0 - (exp.y - 112 * 256)) < 0.05);
  assert.deepEqual(Array.from(I.landPathsForTile({ z: 8, x: 15 + 256, y: 112 }, [c])[0]), Array.from(p));
  assert.deepEqual(Array.from(I.landPathsForTile({ z: 8, x: 15 - 256, y: 112 }, [c])[0]), Array.from(p));
  assert.equal(I.landPathsForTile({ z: 8, x: 100, y: 112 }, [c]).length, 0);
  assert.equal(I.landPathsForTile({ z: 2, x: 0, y: 1 }, [c]).length, 1);                       // z2: Oahu is 1.8 px wide, the islet is sub-pixel
  assert.equal(I.landPathsForTile({ z: 1, x: 0, y: 0 }, [c]).length, 1);                       // z1, western half: Oahu (0.9 px wide: one direction >= 0.5 px keeps it)
  assert.equal(I.landPathsForTile({ z: 1, x: 1, y: 0 }, [c]).length, 1);                       // z1, eastern half: the island
  assert.equal(I.landPathsForTile({ z: 0, x: 0, y: 0 }, [c]).length, 1);                       // z0: only the 1-degree island (0.7 px); Oahu is 0.45 x 0.3 px
  assert.equal(I.landPathsForTile({ z: 2, x: 2, y: 1 }, [c]).length, 1);                       // the 1-degree island (~2.8 px)
  assert.equal(I.landPathsForTile(oahuTile, []).length, 0);
});

test('rasteriseScanline: exact coverage for pixel-aligned shapes, half-pixel edges, holes and unions', () => {
  const S = 16, square = Float32Array.from([2, 2, 10, 2, 10, 10, 2, 10]);
  const m = I.rasteriseScanline([square], S);
  assert.equal(m[5 * S + 5], 255); assert.equal(m[2 * S + 2], 255); assert.equal(m[9 * S + 9], 255);
  assert.equal(m[1 * S + 5], 0); assert.equal(m[10 * S + 10], 0); assert.equal(m[5 * S + 1], 0); assert.equal(m[5 * S + 10], 0);
  const half = I.rasteriseScanline([Float32Array.from([2.5, 2, 10, 2, 10, 10, 2.5, 10])], S);
  assert.equal(half[5 * S + 2], 128); assert.equal(half[5 * S + 3], 255);
  const hole = Float32Array.from([4, 4, 4, 8, 8, 8, 8, 4]);                                    // opposite winding inside the square
  const h = I.rasteriseScanline([square, hole], S);
  assert.equal(h[5 * S + 5], 0); assert.equal(h[3 * S + 3], 255); assert.equal(h[7 * S + 7], 0); assert.equal(h[8 * S + 8], 255); assert.equal(h[9 * S + 9], 255);
  const left = Float32Array.from([2, 2, 6, 2, 6, 10, 2, 10]), right = Float32Array.from([6, 2, 10, 2, 10, 10, 6, 10]);
  const u = I.rasteriseScanline([left, right], S);
  for (let x = 2; x < 10; x++) assert.equal(u[5 * S + x], 255, 'union column ' + x);          // no seam on the shared edge
  const same = I.rasteriseScanline([left, left], S);                                            // nonzero: overlapping pieces never double
  assert.equal(same[5 * S + 3], 255);
  assert.equal(I.rasteriseScanline([], S).every((v) => v === 0), true);
  assert.equal(I.rasterise([square], S)[5 * S + 5], 255);                                       // no canvas here: the fallback is the scanline
});

test('maskState and composeTile: all-ocean / all-land shortcuts and the alpha algebra', () => {
  assert.equal(I.maskState(new Uint8Array(16)), null);
  assert.equal(I.maskState(new Uint8Array(16).fill(255)), I.LAND_ALL);
  const mixed = new Uint8Array(16); mixed[3] = 7;
  assert.equal(I.maskState(mixed), mixed);
  const lut = I.buildRamp(I.RAMPS.hs), codes = new Float64Array([0, 1, 128, 255, 200, 200]), land = new Uint8Array([0, 0, 0, 255, 100, 0]);
  const d = I.composeTile(codes, land, lut, 0, 15, 0, 12, new Uint8ClampedArray(24));
  assert.deepEqual([d[3], d[7], d[11], d[15], d[19], d[23]], [0, 255, 255, 0, 155, 255]);
  assert.deepEqual([d[16], d[17], d[18]], [d[20], d[21], d[22]]);                                 // straight (non-premultiplied) colour under partial alpha
  const ref = new Uint8ClampedArray(24);                                                        // the previous drawing loop
  for (let i = 0, k = 0; i < codes.length; i++, k += 4) {
    const code = codes[i]; if (!code) { ref[k + 3] = 0; continue; }
    let t = Math.round((0 + (code - 1) / 254 * 15 - 0) * 255 / 12); t = t < 0 ? 0 : t > 255 ? 255 : t;
    ref[k] = lut[t * 3]; ref[k + 1] = lut[t * 3 + 1]; ref[k + 2] = lut[t * 3 + 2]; ref[k + 3] = 255;
  }
  assert.deepEqual(Array.from(I.composeTile(codes, null, lut, 0, 15, 0, 12, new Uint8ClampedArray(24))), Array.from(ref));
});

test('readoutAt equals the drawn pixel under the clip: null over land, over missing tiles, never for wind', () => {
  const coast = I.decodeCoast(encodeCoast([[OAHU], [sq(-158.1, 21.0, -157.8, 21.2)]], 5));
  const coords = { z: 8, x: 15, y: 112 }, key = '15:112:8';
  for (const [field, fdef, name] of [['hs', HS, 'hs'], ['tp', TP, 'tp']]) {
    const l = layer(frame(1440, 721, PATTERN), GRID, field, fdef);
    assert.equal(l._clip, false);
    l.setCoast(fakeStore([coast]));
    assert.equal(l._clip, true);
    const el = { getContext: () => ({ clearRect() {}, createImageData: () => ({ data: new Uint8ClampedArray(65536 * 4) }), putImageData() {} }) };
    const mask = l._landFor(el, coords);
    assert.ok(mask && mask !== I.LAND_ALL && el._ovLandFinal === true && el._ovLandKey === '8/15/112');
    assert.equal(l._landFor(el, coords), mask);                                                   // cached on the element
    l._tiles[key] = { el, coords };
    const codes = l.tileCodes(coords, new Float64Array(65536)), d = I.composeTile(codes, mask, l._lut, fdef.lo, fdef.hi, fdef.legend[0], fdef.legend[1], new Uint8ClampedArray(65536 * 4));
    let land = 0, water = 0;
    for (let py = 0; py < 256; py += 3) for (let px = 0; px < 256; px += 5) {
      const ll = I.tilePixelLatLng(coords, px, py), v = l.readoutAt(ll.lat + 1e-7, ll.lng - 1e-7, 8), a = d[(py * 256 + px) * 4 + 3];
      if (a >= I.LAND_READOUT) { assert.ok(v !== null && Math.abs(v - l._value(codes[py * 256 + px])) < 1e-9, name + ' ' + px + ',' + py); water++; }
      else { assert.equal(v, null, name + ' ' + px + ',' + py + ' alpha ' + a); land++; }
    }
    assert.ok(land > 200 && water > 1500, name + ': ' + land + ' land, ' + water + ' water');
    const inside = I.tilePixelLatLng(coords, 128, 120);                                           // central Oahu
    assert.equal(l.readoutAt(inside.lat, inside.lng, 8), null);
    assert.equal(l.readoutAt(inside.lat, inside.lng, 9), null);                                   // that tile is not on the map
    assert.equal(l.readoutAt(inside.lat, inside.lng + 360, 8), null);                             // a world copy: its tile is not on the map either
    l._tiles['271:112:8'] = { el, coords: { z: 8, x: 271, y: 112 } };
    assert.equal(l.readoutAt(inside.lat, inside.lng + 360, 8), null);
    l.setCoast(null);
    assert.equal(l._clip, false);
    assert.ok(l.readoutAt(inside.lat, inside.lng, 8) !== null);
  }
  const w = layer(frame(1440, 721, PATTERN), GRID, 'wind', WIND);
  w.setCoast(fakeStore([coast]));
  assert.equal(w._clip, false);
  const inside = I.tilePixelLatLng(coords, 128, 120);
  assert.ok(w.readoutAt(inside.lat, inside.lng, 8) !== null);
});

test('tiles entirely inside Antarctica are LAND_ALL; the Arctic is ocean; an incomplete tile is redrawn when its chunk lands', () => {
  const coast = I.decodeCoast(encodeCoast([[ANTARCTICA]], 30));
  const l = layer(frame(1440, 721, () => 7), GRID, 'hs', HS);
  l.setCoast(fakeStore([coast]));
  assert.equal(l._landFor({}, { z: 3, x: 5, y: 7 }), I.LAND_ALL);                              // bottom row: -66.5..-85.05
  assert.equal(l._landFor({}, { z: 3, x: 5, y: 0 }), null);
  assert.equal(l._landFor({}, { z: 3, x: 5, y: 6 }), I.LAND_ALL);                              // -66.5..-79.2: still inside
  const mixed = l._landFor({}, { z: 3, x: 5, y: 5 });                                          // -41..-66.5: the -63 edge crosses it
  assert.ok(mixed && mixed !== I.LAND_ALL);
  // a tile drawn from the stand-in is recomputed once the store's revision moves, a final one is not
  let calls = 0; const st2 = { status: 'ok', rev: 0, onChange: null, setsFor: () => { calls++; return { sets: [coast], complete: calls > 1 }; } };
  l.setCoast(st2);
  const el = {}, c = { z: 3, x: 5, y: 5 };
  l._landFor(el, c); assert.equal(el._ovLandFinal, false); l._landFor(el, c); assert.equal(calls, 1);
  st2.rev = 1; l._landFor(el, c); assert.equal(calls, 2); assert.equal(el._ovLandFinal, true);
  st2.rev = 2; l._landFor(el, c); assert.equal(calls, 2);
  let drawn = 0; l._draw = () => { drawn++; };
  l._tiles = { a: { el, coords: c }, b: { el: { _ovLandFinal: false }, coords: c }, c: { el: { _ovLandFinal: true }, coords: c } };
  st2.onChange();
  assert.equal(drawn, 1);
});

test('validateManifest accepts an optional fill object and rejects a mistyped one', () => {
  const base = { schema: 3, run: '2026092212', run_utc: '2026-09-22T12:00:00Z', encoding: 'u8-linear-v2', complete: true, fields: { hs: HS }, grid: GRID, grid_half: HALF,
    frames: [{ step: 0, valid_utc: '2026-09-22T12:00:00Z' }], files: { template: 'a/{res}{field}/f{step:03d}.png', res: { full: '', half: 'half/' } } };
  assert.doesNotThrow(() => I.validateManifest({ ...base }));
  assert.doesNotThrow(() => I.validateManifest({ ...base, fill: { fields: ['hs', 'tp'], cells: 4 } }));
  for (const fill of [null, 4, 'x', [1]]) assert.throws(() => I.validateManifest({ ...base, fill }), /incomplete/);
});

test('CoastStore: tier 0 up front, tier-1 cells on demand with <= 2 in flight, failures, LRU, abort', async () => {
  const idx = { format: 'coast-v1', q: 10000, tier0: { file: 'world-i.bin', cell: 30, max_zoom: 6 }, tier1: { dir: 'f', cell: 5, min_zoom: 7, cells: { '20_-160': [1, 1], '20_-165': [1, 1], '60_20': [1, 1] } } };
  const world = encodeCoast([[ISLAND], [ANTARCTICA]], 30), cellBuf = encodeCoast([[OAHU]], 5);
  const pending = [], log = [];
  const fails = {};
  global.fetch = (url, o) => {
    log.push(url);
    return new Promise((resolve, reject) => {
      const go = () => {
        if (o && o.signal && o.signal.aborted) return reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
        if (fails[url]) { const f = fails[url]; delete fails[url]; return f === 'network' ? reject(new TypeError('Failed to fetch')) : resolve({ ok: false, status: f }); }
        if (url.endsWith('index.json')) return resolve({ ok: true, status: 200, json: () => Promise.resolve(idx) });
        const body = url.endsWith('world-i.bin') ? world : cellBody(url);
        resolve({ ok: true, status: 200, headers: { get: () => String(body.byteLength) }, arrayBuffer: () => Promise.resolve(body.slice(0)) });
      };
      if (url.indexOf('/f/') >= 0) pending.push(go); else go();
    });
  };
  try {
    const s = new I.CoastStore('https://x/static/coast/v1');
    s.retryMs = 0;                                                                                // no cooldowns in this test
    fails['https://x/static/coast/v1/world-i.bin'] = 503;
    assert.equal(await s.load(), null); assert.equal(s.status, 'failed');
    assert.equal(await s.load(), s); assert.equal(s.status, 'ok');                               // a failed load is retried
    assert.equal(await s.load(), s);
    assert.equal(log.filter((u) => u.endsWith('index.json')).length, 2);
    assert.deepEqual(s.setsFor({ z: 6, x: 7, y: 28 }), { sets: [s.tier0], complete: true });
    const t = { z: 7, x: 7, y: 56 };                                                              // touches 20_-165 and 20_-160
    let got = s.setsFor(t);
    assert.equal(got.complete, false); assert.deepEqual(got.sets, [s.tier0]);
    s.request(['60_20', '20_-160', 'nope']);
    assert.equal(Object.keys(s.inflight).length, 2); assert.deepEqual(s.queue, ['60_20']);
    let changes = 0; s.onChange = () => { changes++; };
    pending.shift()(); await settle();
    assert.equal(changes, 1); assert.equal(s.rev, 1); assert.equal(Object.keys(s.inflight).length, 2);   // the queue refilled
    pending.shift()(); pending.shift()(); await settle();
    assert.equal(s.chunks.size, 3); assert.equal(changes, 3);
    got = s.setsFor(t);
    assert.equal(got.complete, true); assert.equal(got.sets.length, 2);
    assert.ok(got.sets.indexOf(s.chunks.get('20_-160')) >= 0);
    // an indexed cell the bucket cannot serve (404): tier 0 stands in for that tile, final, no re-request (A1);
    // a network error: cooldown, the tile stays on the stand-in and is asked again when the cooldown ends (A2)
    s.chunks.clear(); s.bytes = 0;
    fails['https://x/static/coast/v1/f/20_-165.bin'] = 404; fails['https://x/static/coast/v1/f/20_-160.bin'] = 'network';
    s.retryMs = 40;
    assert.equal(s.setsFor(t).complete, false);
    pending.shift()(); pending.shift()(); await settle();
    assert.equal(s.failed['20_-165'], true); assert.equal(typeof s.failed['20_-160'], 'number');
    got = s.setsFor(t);
    assert.equal(got.complete, false); assert.equal(Object.keys(s.inflight).length, 0);         // in cooldown: nothing re-requested yet
    assert.ok(s.retryTimer, 'a retry timer is armed for the cooldown');
    const revBefore = s.rev, changesBefore = changes;
    await new Promise((r) => setTimeout(r, 120));
    assert.equal(s.rev, revBefore + 1); assert.equal(changes, changesBefore + 1);               // the cooldown ended: revision moved, listeners told
    assert.equal(s.setsFor(t).complete, false); assert.equal(Object.keys(s.inflight).length, 1);   // asked again
    pending.shift()(); await settle();
    got = s.setsFor(t);
    assert.equal(got.complete, true); assert.deepEqual(got.sets, [s.tier0]);                    // 20_-165 is still the 404: the tile keeps the 1-km stand-in, for good
    assert.equal(Object.keys(s.inflight).length, 0);
    // LRU by decoded bytes
    s.chunks.clear(); s.bytes = 0;
    s.chunks.set('a', { bytes: 20e6 }); s.chunks.set('b', { bytes: 20e6 }); s.bytes = 40e6; s._evict();
    assert.deepEqual(Array.from(s.chunks.keys()), ['b']); assert.equal(s.bytes, 20e6);
    // the queue is trimmed to what the tiles need (A10), but never the cells asked for right now (R1: Leaflet
    // registers a tile only after createTile returns, so the tile being drawn is not in _tiles yet)
    s.onNeeded = () => ({ '60_20': true });
    s.chunks.clear(); s.bytes = 0; delete s.failed['20_-165'];
    s.request(['20_-165', '60_20', '20_-160']);
    assert.deepEqual(Object.keys(s.inflight).sort(), ['20_-165', '60_20']); assert.deepEqual(s.queue, ['20_-160']);
    pending.shift()(); await settle();                                                          // a handler's pump: the stale entry no tile needs is dropped
    assert.deepEqual(s.queue, []); assert.equal(Object.keys(s.inflight).length, 1);
    pending.shift()(); await settle();
    assert.equal(Object.keys(s.inflight).length, 0);
    // abort: pending requests are dropped, their late answers ignored, chunks and failures forgotten (A5, A9)
    s.chunks.clear(); s.bytes = 0; s.onNeeded = null;
    s.request(['60_20']); assert.equal(Object.keys(s.inflight).length, 1);
    s.abortAll(); assert.equal(Object.keys(s.inflight).length, 0); assert.equal(s.onChange, null); assert.equal(s.chunks.size, 0);
    s.request(['60_20']); assert.equal(Object.keys(s.inflight).length, 1);                     // a new request for the same cell right after Off
    pending.shift()(); await settle();                                                          // the OLD fetch answers (aborted): the new record survives
    assert.equal(Object.keys(s.inflight).length, 1); assert.equal(s.chunks.has('60_20'), false);
    pending.shift()(); await settle();
    assert.equal(s.chunks.has('60_20'), true); assert.equal(s.bytes, s.chunks.get('60_20').bytes);
    s.abortAll();
  } finally {
    delete global.fetch;
  }
});

test('CoastStore: a failed load is not retried within retryMs (no second wait on the first frame); index validation', async () => {
  const idx = { format: 'coast-v1', q: 10000, tier0: { file: 'world-i.bin', cell: 30, max_zoom: 6 }, tier1: { dir: 'f', cell: 5, min_zoom: 7, cells: { '20_-160': [1, 1] } } };
  let indexAnswer = idx, worldStatus = 200, fetches = 0;
  global.fetch = (url) => {
    fetches++;
    if (url.endsWith('index.json')) return Promise.resolve({ ok: true, status: 200, json: () => Promise.resolve(indexAnswer) });
    const body = encodeCoast([[ISLAND]], 30);
    return Promise.resolve(worldStatus === 200 ? { ok: true, status: 200, headers: { get: () => String(body.byteLength) }, arrayBuffer: () => Promise.resolve(body) } : { ok: false, status: worldStatus });
  };
  try {
    const s = new I.CoastStore('https://x/static/coast/v1');
    worldStatus = 503;
    assert.equal(await s.load(), null); assert.equal(s.status, 'failed'); assert.equal(fetches, 2);
    worldStatus = 200;
    assert.equal(await s.load(), null); assert.equal(fetches, 2);                               // within the cooldown: answered at once, no fetch
    s.failedAt = 0;
    assert.equal(await s.load(), s); assert.equal(fetches, 4);
    for (const bad of [{ ...idx, tier1: { ...idx.tier1, cell: 0 } }, { ...idx, tier1: { ...idx.tier1, cell: 7 } }, { ...idx, tier1: { ...idx.tier1, cell: -5 } },
      { ...idx, tier1: { ...idx.tier1, cell: 0.5 } }, { ...idx, tier1: { ...idx.tier1, cell: Math.pow(2, -10) } },
      { ...idx, tier0: { ...idx.tier0, max_zoom: -1 } }, { ...idx, tier0: { ...idx.tier0, max_zoom: 6.5 } },
      { ...idx, tier1: { ...idx.tier1, dir: undefined } }, { ...idx, tier1: { ...idx.tier1, dir: '../x' } }, { ...idx, tier1: { ...idx.tier1, cells: [] } }, { ...idx, format: 'coast-v2' }]) {
      const b = new I.CoastStore('https://y/static/coast/v1');
      indexAnswer = bad;
      assert.equal(await b.load(), null, JSON.stringify(bad.tier1)); assert.equal(b.status, 'failed');
    }
    indexAnswer = { ...idx, tier1: { ...idx.tier1, cells: {} } };                               // no tier-1 cells at all: tier 0 everywhere, no requests
    const c = new I.CoastStore('https://z/static/coast/v1');
    assert.equal(await c.load(), c);
    assert.equal(c.tier1Zoom(9), false);
    assert.deepEqual(c.setsFor({ z: 9, x: 60, y: 450 }), { sets: [c.tier0], complete: true });
    assert.ok(I.validCoastIndex(idx) && !I.validCoastIndex({ ...idx, tier1: { ...idx.tier1, cells: { __proto__: null } } }) === false);
    indexAnswer = { ...idx, tier0: { ...idx.tier0, max_zoom: 0 }, tier1: { ...idx.tier1, cell: 1 } };   // a valid but hostile index: 1-degree cells from zoom 1
    const d = new I.CoastStore('https://w/static/coast/v1');
    assert.equal(await d.load(), d);
    assert.deepEqual(d.setsFor({ z: 1, x: 0, y: 0 }), { sets: [d.tier0], complete: true });            // > MAX_CELLS_PER_TILE names: tier 0 for that tile, no requests
    assert.deepEqual(Object.keys(d.inflight), []); assert.deepEqual(d.queue, []);
    indexAnswer = idx;
    const e = new I.CoastStore('https://v/static/coast/v1');
    global.fetch = ((orig) => (url) => url.endsWith('index.json') ? Promise.resolve({ ok: true, status: 200, headers: { get: () => String(2 * 1024 * 1024) }, json: () => Promise.resolve(idx) }) : orig(url))(global.fetch);
    assert.equal(await e.load(), null); assert.equal(e.status, 'failed');                                // an index over MAX_INDEX_BYTES is not even parsed
  } finally {
    delete global.fetch;
  }
});

test('decodeCoast rejects coordinates outside the world; landPathsForTile keeps the clip points on a piece\'s bbox edges', () => {
  const far = [D(-100000), D(10), D(-99999), D(10), D(-99999), D(11)];
  assert.throws(() => I.decodeCoast(encodeCoast([[far]], 30)), /decode failed/);
  assert.throws(() => I.decodeCoast(encodeCoast([[[D(10), D(91), D(11), D(91), D(11), D(92)]]], 30)), /decode failed/);
  assert.doesNotThrow(() => I.decodeCoast(encodeCoast([[[D(-180), D(-90), D(180), D(-90), D(180), D(90)]]], 30)));
  // two pieces of one coast split by the builder at lon 20: the shared boundary vertices lie within 0.5 px of
  // coastline vertices at zoom 7; they must survive decimation so the nonzero union has no seam on the line
  const west = [D(19.5), D(60.0), D(20.0), D(60.0), D(20.0), D(60.5), D(19.9999), D(60.5001), D(19.5), D(60.5)];
  const east = [D(20.0), D(60.0), D(20.5), D(60.0), D(20.5), D(60.5), D(20.0), D(60.5)];
  const c = I.decodeCoast(encodeCoast([[west], [east]], 5));
  const coords = { z: 7, x: 71, y: 36 };                                                       // 19.7-22.5 E, 60-61.9 N
  const paths = I.landPathsForTile(coords, [c]);
  assert.equal(paths.length, 2);
  const bx = I.forwardPixel(60.25, 20.0, 7).x - 71 * 256;                                       // the cell line in tile pixels
  for (const p of paths) {
    let onLine = 0;
    for (let i = 0; i < p.length; i += 2) if (Math.abs(p[i] - bx) < 1e-3) onLine++;
    assert.equal(onLine, 2, 'both clip points kept on the line');
  }
  const m = I.rasteriseScanline(paths, 256);
  const row = Math.floor(I.forwardPixel(60.25, 20.0, 7).y - 36 * 256), col = Math.floor(bx);
  assert.equal(m[row * 256 + col], 255, 'no seam on the cell line');
  assert.equal(m[row * 256 + col - 1], 255); assert.equal(m[row * 256 + col + 1], 255);
});

test('layer: a stand-in that is still a stand-in is not re-rasterised or redrawn; setCoast(null) detaches the store', () => {
  const coast = I.decodeCoast(encodeCoast([[ANTARCTICA]], 30));
  const l = layer(frame(1440, 721, () => 7), GRID, 'hs', HS);
  let sets = { sets: [coast], complete: false }, calls = 0;
  const st = { status: 'ok', rev: 0, onChange: null, onNeeded: null, tier1Zoom: () => false, setsFor: () => { calls++; return sets; } };
  l.setCoast(st);
  assert.equal(typeof st.onChange, 'function'); assert.equal(typeof st.onNeeded, 'function');
  const el = { getContext: () => ({ clearRect() {}, createImageData: () => ({ data: new Uint8ClampedArray(65536 * 4) }), putImageData() {} }) }, c = { z: 3, x: 5, y: 5 };
  l._tiles['5:5:3'] = { el, coords: c };
  const m1 = l._landFor(el, c);
  assert.equal(calls, 1); assert.equal(el._ovLandFinal, false);
  let draws = 0; const origDraw = l._draw; l._draw = function (e, cc) { draws++; return origDraw.call(this, e, cc); };
  st.rev = 1; st.onChange();                                                                    // an unrelated chunk landed
  assert.equal(calls, 2); assert.equal(l._landFor(el, c), m1); assert.equal(draws, 0);        // asked again, same stand-in object, no redraw
  sets = { sets: [coast], complete: true }; st.rev = 2; st.onChange();
  assert.equal(calls, 3); assert.ok(el._ovLandFinal); assert.equal(draws, 1);                   // final now: one redraw
  st.rev = 3; st.onChange();
  assert.equal(calls, 3); assert.equal(draws, 1);                                               // final tiles never recompute
  assert.deepEqual(Object.keys(l._cellsNeeded()), []);                                          // the fake store has no index at z3 -> tier1Zoom false
  l.setCoast(null);
  assert.equal(st.onChange, null); assert.equal(st.onNeeded, null); assert.equal(l._clip, false);
});

test('landPathsForTile collapses vertex runs beyond one side of the tile without changing the mask', () => {
  const n = 20000, ring = new Array(n * 2);
  for (let i = 0; i < n; i++) { const a = i / n * 2 * Math.PI, r = 30 + 6 * Math.sin(23 * a) + Math.sin(301 * a); ring[i * 2] = D(r * Math.cos(a)); ring[i * 2 + 1] = D(r * Math.sin(a) * 0.7); }
  const c = I.decodeCoast(encodeCoast([[ring]], 30));
  for (const coords of [{ z: 5, x: 18, y: 14 }, { z: 4, x: 8, y: 7 }, { z: 6, x: 33, y: 31 }, { z: 3, x: 4, y: 3 }]) {
    const fast = I.landPathsForTile(coords, [c]);
    // reference: the same ring projected with the same-pixel decimation only (no run collapsing)
    const b = I.tileBox(coords), scale = b.n, ox = b.xw * 256, oy = coords.y * 256, ref = [];
    const [bx0, by0, bx1, by1] = c.box;
    let lx = NaN, ly = NaN;
    for (let v = 0; v < n; v++) {
      const wx = c.xy[v * 2], wy = c.xy[v * 2 + 1], x = wx * scale - ox, y = wy * scale - oy;
      if (ref.length && Math.abs(x - lx) < 0.5 && Math.abs(y - ly) < 0.5 && wx !== bx0 && wx !== bx1 && wy !== by0 && wy !== by1) continue;
      ref.push(x, y); lx = x; ly = y;
    }
    const mFast = I.rasteriseScanline(fast, 256), mRef = I.rasteriseScanline([Float32Array.from(ref)], 256);
    assert.deepEqual(Array.from(mFast), Array.from(mRef), JSON.stringify(coords));
    const kept = fast.reduce((a, p) => a + p.length / 2, 0);
    assert.ok(kept < ref.length / 2, JSON.stringify(coords) + ': ' + kept + ' vs ' + ref.length / 2);
  }
});

test('CoastStore: a chunk whose body is not coast-v1 is failed for good, fetched once, and the queue keeps moving', async () => {
  const idx = { format: 'coast-v1', q: 10000, tier0: { file: 'world-i.bin', cell: 30, max_zoom: 6 }, tier1: { dir: 'f', cell: 5, min_zoom: 7, cells: { '20_-160': [1, 1], '20_-165': [1, 1], '60_20': [1, 1] } } };
  const world = encodeCoast([[ISLAND], [ANTARCTICA]], 30), good = encodeCoast([[OAHU]], 5), bad = new ArrayBuffer(64);
  const pending = []; let fetches = 0;
  global.fetch = (url) => { fetches++; return new Promise((resolve) => {
    const go = () => resolve(url.endsWith('index.json') ? { ok: true, status: 200, json: () => Promise.resolve(idx) }
      : { ok: true, status: 200, headers: { get: () => '64' }, arrayBuffer: () => Promise.resolve((url.indexOf('20_-160') >= 0 ? bad : url.endsWith('world-i.bin') ? world : cellBody(url)).slice(0)) });
    if (url.indexOf('/f/') >= 0) pending.push(go); else go();
  }); };
  try {
    const s = new I.CoastStore('https://x/static/coast/v1');
    s.retryMs = 0;
    assert.equal(await s.load(), s);
    let changes = 0; s.onChange = () => { changes++; };
    s.request(['20_-160', '20_-165', '60_20']);
    assert.deepEqual(Object.keys(s.inflight), ['20_-160', '20_-165']); assert.deepEqual(s.queue, ['60_20']);
    const before = fetches;
    pending.shift()(); await settle();                                                          // the 64 zero bytes land for 20_-160
    assert.equal(s.failed['20_-160'], true); assert.equal(s.rev, 1); assert.equal(changes, 1);
    assert.deepEqual(Object.keys(s.inflight).sort(), ['20_-165', '60_20']);                     // the queue was pumped
    assert.equal(s.chunks.has('20_-160'), false); assert.equal(fetches, before + 1);
    s.request(['20_-160']);
    assert.equal(fetches, before + 1); assert.deepEqual(s.queue, []);                           // never fetched again
    pending.shift()(); pending.shift()(); await settle();
    assert.equal(s.chunks.size, 2);
    assert.deepEqual(s.setsFor({ z: 7, x: 7, y: 56 }), { sets: [s.tier0], complete: true });   // the tile touching 20_-160 keeps the 1-km stand-in for good
    s.abortAll();
  } finally {
    delete global.fetch;
  }
});

test('layer: a tile asks for its cells while Leaflet has not registered it yet (createTile runs before _tiles is set)', async () => {
  const idx = { format: 'coast-v1', q: 10000, tier0: { file: 'world-i.bin', cell: 30, max_zoom: 6 }, tier1: { dir: 'f', cell: 5, min_zoom: 7, cells: { '20_-160': [1, 1], '20_-165': [1, 1] } } };
  const world = encodeCoast([[ISLAND], [ANTARCTICA]], 30), cellBuf = encodeCoast([[OAHU]], 5);
  const pending = [];
  global.fetch = (url) => new Promise((resolve) => {
    const go = () => resolve(url.endsWith('index.json') ? { ok: true, status: 200, json: () => Promise.resolve(idx) }
      : { ok: true, status: 200, headers: { get: () => '1' }, arrayBuffer: () => Promise.resolve((url.endsWith('world-i.bin') ? world : cellBody(url)).slice(0)) });
    if (url.indexOf('/f/') >= 0) pending.push(go); else go();
  });
  const ctx = { clearRect() {}, createImageData: () => ({ data: new Uint8ClampedArray(65536 * 4) }), putImageData() {} };
  global.document = { createElement: () => ({ getContext: () => ctx }) };
  try {
    const s = new I.CoastStore('https://x/static/coast/v1');
    assert.equal(await s.load(), s);
    const l = layer(frame(1440, 721, PATTERN), GRID, 'hs', HS);
    l.setCoast(s);
    assert.deepEqual(l._tiles, {});
    const coords = { z: 9, x: 28, y: 224 };                                                      // Niihau: crosses 160 W, needs 20_-165 and 20_-160
    const el = l.createTile(coords, () => {});                                                   // Leaflet registers the tile only after this returns
    assert.equal(el._ovLandFinal, false);
    assert.deepEqual(Object.keys(s.inflight).sort(), ['20_-160', '20_-165']);                   // both cells requested although no tile is registered yet
    l._tiles['28:224:9'] = { el, coords };
    pending.shift()(); pending.shift()(); await settle();
    assert.equal(el._ovLandFinal, true); assert.equal(s.chunks.size, 2);
    s.abortAll();
  } finally {
    delete global.fetch; delete global.document;
  }
});

test('CoastStore: a valid body for another cell, or a tier-0 file, under a cell name is refused and the tile keeps the stand-in', async () => {
  const idx = { format: 'coast-v1', q: 10000, tier0: { file: 'world-i.bin', cell: 30, max_zoom: 6 }, tier1: { dir: 'f', cell: 5, min_zoom: 7, cells: { '20_-160': [1, 1], '20_-165': [1, 1] } } };
  const world = encodeCoast([[ISLAND], [ANTARCTICA]], 30);
  const answers = { '20_-160': encodeCoast([[sq(21, 61, 22, 62)]], 5), '20_-165': encodeCoast([[sq(-164, 21, -163, 22)]], 30) };   // 60_20's land under 20_-160; a tier-0 header under 20_-165
  global.fetch = (url) => Promise.resolve(url.endsWith('index.json') ? { ok: true, status: 200, json: () => Promise.resolve(idx) }
    : { ok: true, status: 200, headers: { get: () => '1' }, arrayBuffer: () => Promise.resolve((url.endsWith('world-i.bin') ? world : answers[/\/f\/(.+)\.bin$/.exec(url)[1]]).slice(0)) });
  try {
    const s = new I.CoastStore('https://x/static/coast/v1');
    assert.equal(await s.load(), s);
    const t = { z: 7, x: 7, y: 56 };                                                              // touches 20_-165 and 20_-160
    assert.equal(s.setsFor(t).complete, false); await settle();
    assert.equal(s.failed['20_-160'], true); assert.equal(s.failed['20_-165'], true); assert.equal(s.chunks.size, 0);
    assert.deepEqual(s.setsFor(t), { sets: [s.tier0], complete: true });                       // never "no land here"
    const oahu = I.decodeCoast(encodeCoast([[OAHU]], 5));
    assert.ok(I.withinCell(oahu, '20_-160') && !I.withinCell(oahu, '20_-165') && !I.withinCell(oahu, '25_-160'));
    assert.ok(I.withinCell(I.decodeCoast(encodeCoast([[sq(-180, -90, -175, -85)]], 5)), '-90_-180'));   // the pole cell: Float32 slack at y = 256
    assert.ok(I.withinCell(I.decodeCoast(encodeCoast([[sq(175, 85, 180, 90)]], 5)), '85_175'));         // and at y = 0
    assert.ok(!I.withinCell(oahu, 'x') && !I.withinCell(oahu, '20_-160_1'));
    s.abortAll();
  } finally {
    delete global.fetch;
  }
});

test('coast performance smoke: a 300k-vertex coastline rasterised at z1 and z6', () => {
  const n = 300000, ring = new Array(n * 2);
  for (let i = 0; i < n; i++) { const a = i / n * 2 * Math.PI, r = 40 + 8 * Math.sin(37 * a) + 2 * Math.sin(211 * a); ring[i * 2] = D(r * Math.cos(a)); ring[i * 2 + 1] = D(r * Math.sin(a) * 0.8); }
  const c = I.decodeCoast(encodeCoast([[ring]], 30));
  const t0 = process.hrtime.bigint();
  const p1 = I.landPathsForTile({ z: 1, x: 1, y: 0 }, [c]), m1 = I.maskState(I.rasteriseScanline(p1, 256));
  const p6 = I.landPathsForTile({ z: 6, x: 39, y: 27 }, [c]), m6 = I.maskState(I.rasteriseScanline(p6, 256));
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  console.log('coast: z1 ' + (p1[0].length / 2) + ' vertices, z6 ' + (p6.length ? p6[0].length / 2 : 0) + ' vertices, ' + ms.toFixed(0) + ' ms for both tiles (scanline)');
  assert.ok(m1 && m1 !== I.LAND_ALL && m6 !== undefined);
  assert.ok(ms < 8000, ms + ' ms');
});

// ---- A3: contour lines (G8 fix round: smoothed field, one-sided slopes, node-space jumps, block skipping) ----
function rampF(W, fn) {                                     // (W+2)^2 level coordinates from fn(x, y), x/y from -1 to W
  const S = W + 2, F = new Float64Array(S * S);
  for (let y = -1; y <= W; y++) for (let x = -1; x <= W; x++) F[(y + 1) * S + x + 1] = fn(x, y);
  return F;
}
function solid(W, rgb) { const d = new Uint8ClampedArray(W * W * 4); for (let i = 0; i < W * W; i++) { d[i * 4] = rgb[0]; d[i * 4 + 1] = rgb[1]; d[i * 4 + 2] = rgb[2]; d[i * 4 + 3] = 200; } return d; }
const grabEl = () => { const el = { out: null, getContext: () => ({ clearRect() { el.out = null; }, createImageData: () => ({ data: new Uint8ClampedArray(65536 * 4) }), putImageData(img) { el.out = img.data.slice(); } }) }; return el; };
const drawTile = (l, c) => { const el = grabEl(); l._draw(el, c); return el.out; };
const composed = (l, c) => { const codes = l.tileCodes(c, new Float64Array(65536)), d = new Uint8ClampedArray(65536 * 4); I.composeTile(codes, null, l._lut, l._lo, l._hi, l._legend[0], l._legend[1], d); return d; };
const inkOf = (a, b) => { let n = 0; for (let i = 0; i < a.length; i += 4) if (a[i] !== b[i] || a[i + 1] !== b[i + 1] || a[i + 2] !== b[i + 2]) n++; return n; };

test('contourTile: ~2 px anti-aliased lines at whole levels at every angle, alpha untouched, level 0 / dense / missing skipped, ink per level', () => {
  const W = 32, px = (d, x, y) => Array.from(d.slice((y * W + x) * 4, (y * W + x) * 4 + 4));
  let d = solid(W, [30, 60, 160]);
  const n = I.contourTile(rampF(W, (x) => x / 10), W, d, 3);                  // levels at x = 10, 20, 30; level 0 at x = 0 skipped
  assert.ok(n > 0);
  for (const x of [10, 20, 30]) assert.notDeepEqual(px(d, x, 5).slice(0, 3), [30, 60, 160], `line at x=${x}`);
  for (const x of [0, 5, 15, 25]) assert.deepEqual(px(d, x, 5), [30, 60, 160, 200], `no line at x=${x}`);
  assert.ok(px(d, 10, 5)[0] > 150, 'light ink on a dark colour');
  for (let i = 3; i < d.length; i += 4) assert.equal(d[i], 200, 'alpha never changes');
  d = solid(W, [250, 203, 21]); I.contourTile(rampF(W, (x) => x / 10), W, d, 3);
  assert.ok(px(d, 10, 5)[0] < 250 && px(d, 10, 5)[1] < 203, 'dark ink on a light (yellow) colour');
  d = solid(W, [30, 60, 160]); I.contourTile(rampF(W, (x) => x / 10), W, d, 3, (lv) => (lv === 1 ? 30 : 255));
  assert.ok(px(d, 10, 5)[1] < 60 && px(d, 20, 5)[0] > 150, 'ink(level) decides per level, not per pixel');
  d = solid(W, [30, 60, 160]); assert.equal(I.contourTile(rampF(W, (x) => x / 2), W, d, 3), 0, 'levels 2 px apart would merge: none drawn');
  d = solid(W, [30, 60, 160]);
  I.contourTile(rampF(W, (x) => (x >= 8 && x <= 12 ? NaN : x / 10)), W, d, 3);
  assert.deepEqual(px(d, 13, 5), [30, 60, 160, 200], 'nothing beside missing data');
  d = solid(W, [30, 60, 160]); d[(5 * W + 10) * 4 + 3] = 0;
  I.contourTile(rampF(W, (x) => x / 10), W, d, 3);
  assert.deepEqual(px(d, 10, 5), [30, 60, 160, 0], 'transparent pixels stay untouched');
  d = solid(W, [30, 60, 160]); I.contourTile(rampF(W, (x) => x / 10), W, d, 3, null, 0, 0, 16, 16);
  assert.notDeepEqual(px(d, 10, 5).slice(0, 3), [30, 60, 160]); assert.deepEqual(px(d, 20, 5), [30, 60, 160, 200], 'only the given rect is drawn');
  assert.deepEqual(px(d, 10, 20), [30, 60, 160, 200]);
  // integrated width of single straight lines over 24 angles x 5 sub-pixel offsets (the docs say ~2 px)
  const Wb = 96, Sb = Wb + 2, widths = [];
  for (let a = 0; a < 24; a++) for (let o = 0; o < 5; o++) {
    const th = a * Math.PI / 24 + 0.013, cs = Math.cos(th), sn = Math.sin(th), F = new Float64Array(Sb * Sb);
    for (let y = -1; y <= Wb; y++) for (let x = -1; x <= Wb; x++) F[(y + 1) * Sb + x + 1] = 3.5 + ((x - 48) * cs + (y - 48) * sn + o / 5) / 20;
    const b = new Uint8ClampedArray(Wb * Wb * 4); for (let i = 3; i < b.length; i += 4) b[i] = 200;
    I.contourTile(F, Wb, b, 3, () => 255);
    let area = 0, len = 0;
    for (let y = 24; y < 72; y++) for (let x = 24; x < 72; x++) area += b[(y * Wb + x) * 4] / (255 * 0.55);
    for (let lv = 1; lv <= 7; lv++) {
      const c = (lv - 3.5) * 20 - o / 5, pts = [];
      for (const [x0, y0, x1, y1] of [[24, 24, 72, 24], [72, 24, 72, 72], [72, 72, 24, 72], [24, 72, 24, 24]]) {
        const f0 = (x0 - 48) * cs + (y0 - 48) * sn - c, f1 = (x1 - 48) * cs + (y1 - 48) * sn - c;
        if ((f0 <= 0 && f1 > 0) || (f0 > 0 && f1 <= 0)) { const t = f0 / (f0 - f1); pts.push([x0 + (x1 - x0) * t, y0 + (y1 - y0) * t]); }
      }
      if (pts.length >= 2) len += Math.hypot(pts[0][0] - pts[1][0], pts[0][1] - pts[1][1]);
    }
    if (len > 20) widths.push(area / len);
  }
  assert.ok(widths.length > 100 && Math.min(...widths) > 2.0 && Math.max(...widths) < 2.5, `widths ${Math.min(...widths).toFixed(2)}..${Math.max(...widths).toFixed(2)}`);
});

test('contourTile: never along the foot or the top of a steep ramp, where the level is not reached (G8 B-P2-2)', () => {
  const W = 32, blank = () => { const d = new Uint8ClampedArray(W * W * 4); for (let i = 3; i < d.length; i += 4) d[i] = 200; return d; };
  const onPlateau = (d, x0, x1) => { let k = 0; for (let y = 0; y < W; y++) for (let x = x0; x <= x1; x++) if (d[(y * W + x) * 4]) k++; return k; };
  // flat at 10.10 (level 10 is never reached) up to x = 10, then a ramp of 0.3 levels per px
  let d = blank(); I.contourTile(rampF(W, (x) => (x <= 10 ? 10.10 : 10.10 + (x - 10) * 0.3)), W, d, 3, () => 255);
  assert.equal(onPlateau(d, 0, 10), 0, 'nothing on the flat side of the foot');
  assert.ok(onPlateau(d, 12, 31) > 0, 'the ramp itself still carries its lines');
  // the mirror: a ramp that tops out at 9.90 (level 10 above it, never reached)
  d = blank(); I.contourTile(rampF(W, (x) => (x >= 21 ? 9.90 : 9.90 - (21 - x) * 0.3)), W, d, 3, () => 255);
  assert.equal(onPlateau(d, 21, 31), 0, 'nothing on the flat top');
});

test('smoothBlock: masked [1,2,1] mean, missing stays missing, periodic columns, rows not wrapped; jumps kept apart, marked alike both ways', () => {
  const cols = 6, rows = 5, q = new Uint8Array(cols * rows), at = (r, c) => r * cols + c;
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) q[at(r, c)] = 100 + (r === 4 ? 1 : 0) + (c === 5 ? 1 : 0);
  q[at(2, 3)] = 0;                                                            // one missing node
  q[at(0, 2)] = 150;                                                          // a big step: the mean would move its neighbours by several codes
  const S = new Float32Array(cols * rows);
  I.smoothBlock(q, cols, rows, S, null, 0, 0, rows, 0, cols);
  assert.equal(S[at(2, 3)], 0, 'missing stays missing');
  const mean = (r, c) => {                                                    // the reference: masked weighted mean, cols periodic, rows clipped,
    let acc = 0, w = 0;                                                       // held within half a code of the node's own value
    for (let dr = -1; dr <= 1; dr++) for (let dc = -1; dc <= 1; dc++) {
      const rr = r + dr, cc = (c + dc + cols) % cols; if (rr < 0 || rr >= rows) continue;
      const v = q[at(rr, cc)]; if (!v) continue; const wt = (2 - Math.abs(dr)) * (2 - Math.abs(dc)); acc += v * wt; w += wt;
    }
    const v0 = q[at(r, c)]; return Math.max(v0 - 0.5, Math.min(v0 + 0.5, acc / w));
  };
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) if (q[at(r, c)]) assert.ok(Math.abs(S[at(r, c)] - mean(r, c)) < 1e-4, `${r},${c}`);
  assert.equal(S[at(0, 1)], q[at(0, 1)] + 0.5, 'next to the big step: half a code, no more');
  assert.equal(S[at(0, 2)], q[at(0, 2)] - 0.5, 'the step itself: half a code, no more');
  assert.ok(S[at(2, 0)] > S[at(2, 2)] + 0.2, 'column 0 averages with column 5 (periodic), column 2 does not reach it');
  assert.ok(Math.abs(S[at(0, 4)] - mean(0, 4)) < 1e-4 && S[at(0, 4)] < 100.4, 'row 0 never averages with row 4 (rows do not wrap; wrapping gives 100.5)');
  // a peak-period jump (2 s = 17.5 codes on 1..30 s) between columns 2|3 and between rows 1|2: never blended, cells marked
  const jump = 2 * 254 / 29, qt = new Uint8Array(cols * rows);
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) qt[at(r, c)] = 80 + (c >= 3 ? 25 : 0) + (r >= 2 && c < 3 ? 25 : 0) + c;
  const St = new Float32Array(cols * rows), J = new Uint8Array(cols * rows);
  I.smoothBlock(qt, cols, rows, St, J, jump, 0, rows, 0, cols);
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) {
    let lo = Infinity, hi = -Infinity;                                        // the smoothed value stays inside its own regime's range
    for (let dr = -1; dr <= 1; dr++) for (let dc = -1; dc <= 1; dc++) {
      const rr = r + dr, cc = (c + dc + cols) % cols; if (rr < 0 || rr >= rows) continue;
      const v = qt[at(rr, cc)]; if (Math.abs(v - qt[at(r, c)]) > jump) continue; lo = Math.min(lo, v); hi = Math.max(hi, v);
    }
    assert.ok(St[at(r, c)] >= lo - 1e-4 && St[at(r, c)] <= hi + 1e-4, `regime kept at ${r},${c}`);
  }
  assert.equal(J[at(0, 2)], 1, 'east-west jump: the cell between columns 2 and 3 is marked');
  assert.equal(J[at(1, 1)], 1, 'north-south jump: the cell between rows 1 and 2 is marked');
  assert.equal(J[at(0, 0)], 0); assert.equal(J[at(3, 0)], 0); assert.equal(J[at(3, 4)], 0);
  assert.equal(J[at(0, 5)], 1, 'the periodic cell between columns 5 and 0 jumps too');
  assert.equal(I.jumpAt(J, cols, rows, 2, 0, false), false); assert.equal(I.jumpAt(J, cols, rows, 2, 0, true), true, 'wide: the jump cell above counts');
  assert.equal(I.jumpAt(J, cols, rows, 3, 3, true), false, 'wide: nothing within one cell');
});

const SMOOTH = (r, c) => 1 + Math.round(120 + 90 * Math.sin(c / 9) * Math.cos(r / 7));

test('contours on the layer: RGB only, never on nearest runs or for wind; skipping blocks changes nothing; world copies identical', () => {
  const c = { z: 6, x: 3, y: 27 }, cfg = { step: 2, per: 3.28084 };
  const hs = layer(frame(1440, 721, SMOOTH), GRID, 'hs', HS);
  const plain = drawTile(hs, c);
  hs.setContours(cfg, true); const lined = drawTile(hs, c);
  for (let i = 3; i < plain.length; i += 4) assert.equal(lined[i], plain[i]);
  const changed = inkOf(plain, lined);
  assert.ok(changed > 200 && changed < 20000, `${changed} line pixels`);
  const tp = layer(frame(1440, 721, SMOOTH), GRID, 'tp', TP);                     // TP here is drawn nearest (an old run's hint)
  const tpPlain = drawTile(tp, c); tp.setContours(cfg, true); assert.deepEqual(Array.from(drawTile(tp, c)), Array.from(tpPlain));
  const wind = layer(frame(1440, 721, SMOOTH), GRID, 'wind', WIND);
  const wPlain = drawTile(wind, c); wind.setContours(cfg, true); assert.deepEqual(Array.from(drawTile(wind, c)), Array.from(wPlain), 'wind: no contours');
  // the skipped blocks never hold a line pixel: drawing every block gives the same tile, at several zooms and resolutions
  const tpb = layer(frame(1440, 721, SMOOTH), GRID, 'tp', Object.assign({}, TP, { interpolation: 'bilinear' }));
  tpb.setContours({ step: 2, per: 1 }, true);
  const half = layer(frame(720, 361, SMOOTH), HALF, 'hs', HS); half.setContours(cfg, true);
  for (const [l, cs] of [[hs, [c, { z: 8, x: 15, y: 111 }, { z: 4, x: 1, y: 6 }]], [tpb, [c, { z: 5, x: 31, y: 12 }]], [half, [{ z: 2, x: 1, y: 1 }, { z: 1, x: 0, y: 0 }, { z: 3, x: 7, y: 3 }]]]) {
    for (const cc of cs) {
      const a = composed(l, cc), b = a.slice();
      l._contours(cc, a); l._contours(cc, b, true);
      assert.deepEqual(Array.from(a), Array.from(b), `blocks z${cc.z}`);
      assert.ok(inkOf(composed(l, cc), a) > 0, `lines at z${cc.z}`);
      const N = Math.pow(2, cc.z), base = drawTile(l, cc);
      for (const xx of [cc.x + N, cc.x - N]) assert.deepEqual(Array.from(drawTile(l, { z: cc.z, x: xx, y: cc.y })), Array.from(base), `world copy z${cc.z}`);
    }
  }
  hs.setContours(null, true); assert.equal(hs._sm, null, 'switching contours off drops the smoothed copy');
});

test('contours: neighbouring tiles agree on every shared value, and four tiles equal one 512-px computation', () => {
  const cfg = { step: 2, per: 3.28084 };
  for (const [fr, grid, z, x, y] of [[frame(1440, 721, SMOOTH), GRID, 6, 3, 27], [frame(1440, 721, SMOOTH), GRID, 5, 31, 12], [frame(720, 361, SMOOTH), HALF, 3, 0, 2]]) {
    const l = layer(fr, grid, 'hs', HS); l.setContours(cfg, true);
    const W = 512, S = W + 2, big = new Float64Array(S * S).fill(NaN), seen = new Uint8Array(S * S), plain = new Uint8ClampedArray(W * W * 4), lined = {};
    for (const [dx, dy] of [[0, 0], [1, 0], [0, 1], [1, 1]]) {
      const c = { z, x: x + dx, y: y + dy }, a = composed(l, c);
      for (let py = 0; py < 256; py++) plain.set(a.subarray(py * 1024, py * 1024 + 1024), ((dy * 256 + py) * W + dx * 256) * 4);
      const b = a.slice(); l._contours(c, b, true); lined[dx + ',' + dy] = b;
      for (let py = -1; py <= 256; py++) for (let px = -1; px <= 256; px++) {
        const gx = dx * 256 + px, gy = dy * 256 + py; if (gx > W || gy > W) continue;
        const v = l._F[(py + 1) * 258 + px + 1], j = (gy + 1) * S + gx + 1;
        if (seen[j]) assert.ok((v !== v && big[j] !== big[j]) || v === big[j], `z${z} shared value at ${gx},${gy}`);
        big[j] = v; seen[j] = 1;
      }
    }
    const ref = plain.slice(), k = cfg.per / (cfg.step * (z < 4 ? 2 : 1)), lut = l._lut, sc = 255 / (l._legend[1] - l._legend[0]);
    I.contourTile(big, W, ref, 3, (lv) => { let t = Math.round((lv / k - l._legend[0]) * sc); t = Math.max(0, Math.min(255, t)); return (0.299 * lut[t * 3] + 0.587 * lut[t * 3 + 1] + 0.114 * lut[t * 3 + 2]) > 170 ? 30 : 255; });
    for (const [dx, dy] of [[0, 0], [1, 0], [0, 1], [1, 1]]) {
      const q = lined[dx + ',' + dy];
      for (let py = 0; py < 256; py++) for (let px = 0; px < 256; px++) {
        const a = (py * 256 + px) * 4, b = ((dy * 256 + py) * W + dx * 256 + px) * 4;
        for (let ch = 0; ch < 4; ch++) if (q[a + ch] !== ref[b + ch]) assert.fail(`z${z} tile ${dx},${dy} pixel ${px},${py}`);
      }
    }
  }
});

test('contours: no peak-period line in a cell that jumps more than 2 s, north-south as east-west, at 60 N as at the equator (G8 A-P2-2)', () => {
  const TPB = Object.assign({}, TP, { interpolation: 'bilinear' }), code = (s) => Math.round(1 + (s - 1) / 29 * 254);
  // 10 s rising gently (0.12 s per cell) with a step of `jump` seconds across one row or one column; the 12 s level sits inside the step
  function run(jump, axis, lat) {
    const r0 = Math.round((90 - lat) / 0.25), c0 = Math.round((-150 + 180) / 0.25);
    const fr = frame(1440, 721, (r, c) => code(10.6 + 0.12 * (axis === 'ew' ? c - c0 : r0 - r) + ((axis === 'ew' ? c > c0 : r < r0) ? jump : 0)));
    const l = layer(fr, GRID, 'tp', TPB); l.setContours({ step: 2, per: 1 }, true);
    const z = 7, n = 256 * 128, X = (-150 + 180) / 360 * n, s = Math.sin(lat * Math.PI / 180), Y = (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * n;
    let inJump = 0, lines = 0;
    for (const [dx, dy] of [[-1, -1], [0, -1], [-1, 0], [0, 0]]) {
      const c = { z, x: Math.floor(X / 256) + dx, y: Math.floor(Y / 256) + dy }, a = composed(l, c), b = a.slice();
      l._contours(c, b);
      for (let py = 0; py < 256; py++) for (let px = 0; px < 256; px++) {
        const i = (py * 256 + px) * 4; if (a[i] === b[i] && a[i + 1] === b[i + 1] && a[i + 2] === b[i + 2]) continue;
        lines++;
        const ll = I.tilePixelLatLng(c, px, py), rr = (90 - ll.lat) / 0.25, cc = (ll.lng + 180) / 0.25;
        if (axis === 'ew' ? Math.abs(Math.floor(cc) - c0) <= 1 : Math.abs(Math.floor(rr) - (r0 - 1)) <= 1) inJump++;   // the step's cell or its neighbours
      }
    }
    return { lines, inJump };
  }
  for (const lat of [0, 60]) for (const axis of ['ew', 'ns']) {
    const big = run(2.5, axis, lat), small = run(1.5, axis, lat);
    assert.equal(big.inJump, 0, `${axis} at ${lat}: a 2.5 s step carries no line, in its cell or beside it`);
    assert.ok(small.inJump > 0, `${axis} at ${lat}: a 1.5 s step is an ordinary slope and keeps its line (${small.lines})`);
  }
});

test('contours: the interval doubles below tile zoom 4 (2 ft lines at z4, none at z3 on a 1-3.5 ft sea)', () => {
  const ft = (c) => 1.0 + 2.5 * c / 1439;                                     // 1.0 ft in the west to 3.5 ft in the east
  const fr = frame(1440, 721, (r, c) => Math.round(1 + ft(c) / 3.28084 / 15 * 254));
  const l = layer(fr, GRID, 'hs', HS); l.setContours({ step: 2, per: 3.28084 }, true);
  let z4 = 0, z3 = 0;
  for (let x = 0; x < 16; x++) { const c = { z: 4, x, y: 6 }, a = composed(l, c), b = a.slice(); l._contours(c, b); z4 += inkOf(a, b); }
  for (let x = 0; x < 8; x++) { const c = { z: 3, x, y: 3 }, a = composed(l, c), b = a.slice(); l._contours(c, b); z3 += inkOf(a, b); }
  assert.ok(z4 > 100, `z4: the 2 ft line (${z4} px)`);
  assert.equal(z3, 0, 'z3: every 4 ft, and 4 ft is never reached');
});

test('readout equals the drawn pixel with contours on (clipped hs and tp), and the lines never change alpha', () => {
  const coast = I.decodeCoast(encodeCoast([[OAHU], [sq(-158.1, 21.0, -157.8, 21.2)]], 5));
  const coords = { z: 8, x: 15, y: 112 }, key = '15:112:8';
  for (const [field, fdef, cfg] of [['hs', HS, { step: 2, per: 3.28084 }], ['tp', Object.assign({}, TP, { interpolation: 'bilinear' }), { step: 2, per: 1 }]]) {
    const l = layer(frame(1440, 721, SMOOTH), GRID, field, fdef);
    l.setCoast(fakeStore([coast]));
    const el = grabEl(); l._tiles[key] = { el, coords };
    l._draw(el, coords); const off = el.out;
    l.setContours(cfg, true); l._draw(el, coords); const on = el.out;
    assert.ok(inkOf(off, on) > 100, field + ': lines drawn');
    for (let i = 3; i < on.length; i += 4) assert.equal(on[i], off[i], field + ': alpha');
    const codes = l.tileCodes(coords, new Float64Array(65536));
    for (let py = 0; py < 256; py += 3) for (let px = 0; px < 256; px += 5) {
      const ll = I.tilePixelLatLng(coords, px, py), v = l.readoutAt(ll.lat + 1e-7, ll.lng - 1e-7, 8), a = on[(py * 256 + px) * 4 + 3];
      if (a >= I.LAND_READOUT) assert.ok(v !== null && Math.abs(v - l._value(codes[py * 256 + px])) < 1e-9, field + ' ' + px + ',' + py);
      else assert.equal(v, null, field + ' ' + px + ',' + py);
    }
  }
});

test('contourTile: the distance is measured along the slope that leads to the level, not the central difference', () => {
  // one pixel: the level 5 lies 0.1 below it; the way down (left) falls 0.1 per px, the other side rises 0.4
  const F = new Float64Array(9); F.fill(NaN);
  F[1] = 5.1; F[3] = 5.0; F[4] = 5.1; F[5] = 5.5; F[7] = 5.1;                // u, l, f, r, d (W = 1, S = 3)
  const d = new Uint8ClampedArray([30, 60, 160, 200]);
  assert.equal(I.contourTile(F, 1, d, 3, () => 255), 1);
  const a = ((1.5 - 1.0) / 0.75) * 0.55;                                      // 1.0 px away (0.1 / 0.1): partial coverage
  for (const [ch, base] of [[0, 30], [1, 60], [2, 160]]) assert.ok(Math.abs(d[ch] - (base + (255 - base) * a)) <= 1, `channel ${ch}: ${d[ch]}`);
  assert.equal(d[3], 200);
});

async function realFrame(name) {
  const buf = fs.readFileSync(path.join(__dirname, '..', 'fixtures', name));
  return I.decodePngGrey(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength));
}
const tilesOf = (z, x0, x1, y0, y1) => { const t = []; for (let y = y0; y <= y1; y++) for (let x = x0; x <= x1; x++) t.push({ z, x, y }); return t; };

test('real frames: skipping blocks draws exactly what drawing every block draws, at zooms 1 to 8', async () => {
  const hsHalf = await realFrame('frame_hs_half.png'), tpFull = await realFrame('frame_tp_full.png');
  const TPB = Object.assign({}, TP, { interpolation: 'bilinear' });
  const cases = [
    [hsHalf, HALF, 'hs', HS, { step: 2, per: 3.28084 }, [...tilesOf(1, 0, 1, 0, 1), ...tilesOf(2, 0, 3, 1, 2), ...tilesOf(3, 0, 7, 2, 4)]],
    [hsHalf, HALF, 'hs', HS, { step: 0.5, per: 1 }, tilesOf(3, 0, 7, 2, 4)],
    [tpFull, GRID, 'tp', TPB, { step: 2, per: 1 }, [...tilesOf(4, 0, 15, 5, 8), ...tilesOf(6, 26, 33, 19, 21), ...tilesOf(8, 10, 18, 108, 112)]]];
  let tiles = 0, lines = 0;
  for (const [fr, grid, field, fdef, cfg, list] of cases) {
    const l = layer(fr, grid, field, fdef); l.setContours(cfg, true);
    for (const c of list) {
      const a = composed(l, c), b = a.slice(), plain = a.slice();
      l._contours(c, a); l._contours(c, b, true);
      for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) assert.fail(`${field} z${c.z} ${c.x},${c.y}: byte ${i} ${a[i]} vs ${b[i]}`);
      tiles++; lines += inkOf(plain, a);
    }
  }
  assert.equal(tiles, 193); assert.ok(lines > 20000, `${lines} line px`);
});

test('real frames: no peak-period line inside a cell that jumps more than 2 s, also where cells are narrower than the sampling (z1-z2)', async () => {
  const full = await realFrame('frame_tp_full.png'), half = { cols: 720, rows: 361, q: new Uint8Array(720 * 361) };
  for (let r = 0; r < 361; r++) for (let c = 0; c < 720; c++) half.q[r * 720 + c] = full.q[(2 * r) * 1440 + 2 * c];   // the job's half = full[::2, ::2]
  const TPB = Object.assign({}, TP, { interpolation: 'bilinear' }), jump = 2 * 254 / (TP.hi - TP.lo);
  for (const [fr, grid, list] of [[half, HALF, [...tilesOf(1, 0, 1, 0, 1), ...tilesOf(2, 0, 3, 0, 3)]], [full, GRID, [...tilesOf(4, 0, 15, 5, 8), ...tilesOf(8, 10, 18, 108, 112)]]]) {
    const l = layer(fr, grid, 'tp', TPB); l.setContours({ step: 2, per: 1 }, true);
    const cols = fr.cols, rows = fr.rows, q = fr.q;
    const jumps = (r0, c0) => {                                                // a > 2 s edge in the cell (nodes r0..r0+1, c0..c0+1)
      const r1 = Math.min(rows - 1, r0 + 1), c1 = (c0 + 1) % cols;
      const A = q[r0 * cols + c0], B = q[r0 * cols + c1], C = q[r1 * cols + c0], D = q[r1 * cols + c1];
      const j = (x, y) => x && y && Math.abs(x - y) > jump;
      return j(A, B) || j(C, D) || j(A, C) || j(B, D);
    };
    let lines = 0, near = 0;
    for (const c of list) {
      const a = composed(l, c), b = a.slice(); l._contours(c, b);
      for (let py = 0; py < 256; py++) for (let px = 0; px < 256; px++) {
        const i = (py * 256 + px) * 4; if (a[i] === b[i] && a[i + 1] === b[i + 1] && a[i + 2] === b[i + 2]) continue;
        lines++;
        const ll = I.tilePixelLatLng(c, px, py), rr = (grid.lat0 - ll.lat) / -grid.dlat, cc = (((ll.lng - grid.lon0) / grid.dlon) % cols + cols) % cols;
        if (rr >= 0 && rr <= rows - 1 && jumps(Math.floor(rr), Math.floor(cc))) near++;
      }
    }
    assert.ok(lines > 1000, `${cols} cols: ${lines} line px`);
    assert.equal(near, 0, `${cols} cols: ${near} of ${lines} line px inside a jump cell`);
  }
});

// ---- G8 re-review (R1): the smoothing stays within one code, follows the frame, fills lazily without gaps ----
const around = (lat, lng, z, w, h) => {
  const n = 256 * Math.pow(2, z), s = Math.sin(lat * Math.PI / 180), cx = Math.floor((lng + 180) / 360 * n / 256);
  const cy = Math.floor((0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * n / 256), out = [];
  for (let dy = 0; dy < h; dy++) for (let dx = 0; dx < w; dx++) out.push({ z, x: cx - (w >> 1) + dx, y: cy - (h >> 1) + dy });
  return out;
};

test('a patch one code above its surroundings keeps its closed contour when the level lies within that code (G8 R2 P2-1)', () => {
  // hs nodes of code 100 everywhere and one of 101 (0.059 m higher); the only level sits 0.3 code above 100. The model
  // value at the bump may be anywhere in its code's bin, so smoothing may lower it by half a code, never by one.
  const span = (HS.hi - HS.lo) / 254, level = HS.lo + (99 + 0.3) * span;
  const fr = frame(1440, 721, (r, c) => (r === 300 && c === 700 ? 101 : 100));
  const l = layer(fr, GRID, 'hs', HS); l.setContours({ step: level, per: 1 }, true);
  const c = around(15, -5, 8, 1, 1)[0], a = composed(l, c), b = a.slice();
  l._contours(c, b);
  assert.ok(inkOf(a, b) > 50, `the bump's closed contour (${inkOf(a, b)} px)`);
});

test('real frames: every line core sits within 0.3 interval of the value drawn at its pixel around the islands (G8 R1 P2-1)', async () => {
  const hsHalf = await realFrame('frame_hs_half.png'), tpFull = await realFrame('frame_tp_full.png');
  const TPB = Object.assign({}, TP, { interpolation: 'bilinear' });
  let cores = 0;
  for (const [fr, grid, field, fdef, cfg] of [[hsHalf, HALF, 'hs', HS, { step: 2, per: 3.28084 }], [hsHalf, HALF, 'hs', HS, { step: 0.5, per: 1 }],
    [tpFull, GRID, 'tp', TPB, { step: 2, per: 1 }]]) {
    const l = layer(fr, grid, field, fdef); l.setContours(cfg, true);
    for (const z of [6, 7, 8, 9]) {
      for (const c of [...around(20.8, -156.9, z, 3, 3), ...around(21.5, -158.2, z, 3, 3), ...around(22.0, -159.6, z, 2, 2)]) {
        const codes = l.tileCodes(c, new Float64Array(65536)), a = composed(l, c), b = a.slice();
        l._contours(c, b, true);
        const F = l._F, S = 258;
        for (let py = 0; py < 256; py++) for (let px = 0; px < 256; px++) {
          const k = (py * 256 + px) * 4;
          if (!a[k + 3] || (a[k] === b[k] && a[k + 1] === b[k + 1] && a[k + 2] === b[k + 2])) continue;
          const i = (py + 1) * S + px + 1, fv = F[i], lv = Math.round(fv), df = fv - lv;
          const sx = Math.max(0, df > 0 ? Math.max(fv - F[i - 1], fv - F[i + 1]) : Math.max(F[i - 1] - fv, F[i + 1] - fv));
          const sy = Math.max(0, df > 0 ? Math.max(fv - F[i - S], fv - F[i + S]) : Math.max(F[i - S] - fv, F[i + S] - fv));
          if (!(Math.abs(df) / Math.hypot(sx, sy) <= 0.75)) continue;                 // the line's core (full coverage)
          const step = cfg.step * (z < 4 ? 2 : 1), drawn = l._value(codes[py * 256 + px]) * cfg.per;
          const off = Math.abs(drawn - lv * step) / step;
          if (off > 0.3) assert.fail(`${field} ${cfg.step} z${z} ${c.x},${c.y} px ${px},${py}: line at ${lv * step}, drawn value ${drawn.toFixed(2)} (${off.toFixed(2)} interval)`);
          cores++;
        }
      }
    }
  }
  assert.ok(cores > 50000, `${cores} line cores`);
});

test('one layer through frames, resolutions and fields draws exactly what fresh layers draw (the smoothed copy follows the frame)', () => {
  const TPB = Object.assign({}, TP, { interpolation: 'bilinear' });
  const JUMPY = (r, c) => 1 + Math.round(90 + 60 * Math.sin(c / 7) * Math.cos(r / 5)) + ((Math.floor(c / 40) % 2) ? 25 : 0);
  const SHIFT = (r, c) => SMOOTH(r + 3, c + 5);
  const F = { a: frame(1440, 721, SMOOTH), b: frame(1440, 721, SHIFT), h: frame(720, 361, SMOOTH), t: frame(1440, 721, JUMPY), th: frame(720, 361, JUMPY) };
  const cfg = { hs: { step: 2, per: 3.28084 }, tp: { step: 2, per: 1 } };
  const seq = [['a', GRID, 'hs', HS], ['b', GRID, 'hs', HS], ['h', HALF, 'hs', HS], ['a', GRID, 'hs', HS], ['t', GRID, 'tp', TPB], ['th', HALF, 'tp', TPB], ['b', GRID, 'hs', HS]];
  const tiles = [{ z: 6, x: 3, y: 27 }, { z: 6, x: 4, y: 27 }, { z: 4, x: 1, y: 6 }, { z: 5, x: 31, y: 12 }, { z: 5, x: 0, y: 12 }];
  const one = new I.ModelGridLayer({ opacity: 1 });
  let draws = 0;
  for (const [f, grid, field, fdef] of seq) {
    if (one.field !== field) one.clear();                                         // what mount() does on a field switch
    one.setContours(cfg[field], true);
    one.setFrame(F[f], grid, field, fdef, I.buildRamp(I.RAMPS[field]), { step: 0, valid_utc: '2026-09-22T12:00:00Z' });
    const fresh = layer(F[f], grid, field, fdef); fresh.setContours(cfg[field], true);
    for (const c of tiles) {
      const a = composed(one, c), b = composed(fresh, c);
      one._contours(c, a); fresh._contours(c, b);
      assert.deepEqual(Array.from(a), Array.from(b), `${f} ${field} z${c.z} ${c.x},${c.y}`);
      draws++;
    }
  }
  assert.equal(draws, seq.length * tiles.length);
});

test('smoothing filled lazily, block by block, equals the whole frame smoothed up front, across node-block seams', () => {
  const TPB = Object.assign({}, TP, { interpolation: 'bilinear' });
  const JUMPY = (r, c) => 1 + Math.round(90 + 60 * Math.sin(c / 7) * Math.cos(r / 5)) + ((Math.floor(c / 40) % 2) ? 25 : 0);
  for (const [fr, grid, field, fdef, cfg, tiles] of [
    [frame(1440, 721, SMOOTH), GRID, 'hs', HS, { step: 0.5, per: 1 }, [...around(30, -120, 6, 4, 4), ...around(0, 179.5, 5, 2, 2)]],
    [frame(1440, 721, JUMPY), GRID, 'tp', TPB, { step: 2, per: 1 }, [...around(30, -120, 6, 4, 4), ...around(-40, 10, 4, 3, 3)]],
    [frame(720, 361, JUMPY), HALF, 'tp', TPB, { step: 2, per: 1 }, [...around(20, -160, 2, 3, 3), ...around(0, 0, 1, 2, 2)]]]) {
    const lazy = layer(fr, grid, field, fdef), up = layer(fr, grid, field, fdef);
    lazy.setContours(cfg, true); up.setContours(cfg, true);
    up._contours({ z: 3, x: 0, y: 0 }, new Uint8ClampedArray(65536 * 4));            // allocate its smoothed copy, then fill all of it
    const sm = up._sm;
    I.smoothBlock(fr.q, fr.cols, fr.rows, sm.S, sm.jump ? sm.J : null, sm.jump, 0, fr.rows, 0, fr.cols);
    sm.done.fill(1);
    for (const c of tiles) {
      const a = composed(lazy, c), b = composed(up, c);
      lazy._contours(c, a); up._contours(c, b);
      assert.deepEqual(Array.from(a), Array.from(b), `${field} ${fr.cols} z${c.z} ${c.x},${c.y}`);
    }
  }
});

test('the layer treats exactly 2 s as the peak-period jump: a 17-code step (1.94 s) is smoothed, an 18-code step (2.06 s) is a jump', () => {
  const TPB = Object.assign({}, TP, { interpolation: 'bilinear' });
  for (const [step, isJump] of [[17, false], [18, true]]) {
    const fr = frame(1440, 721, (r, c) => (c < 720 ? 100 : 100 + step));          // one step, between columns 719 and 720
    const l = layer(fr, GRID, 'tp', TPB); l.setContours({ step: 2, per: 1 }, true);
    const c = { z: 6, x: 32, y: 31 };                                               // a tile on the step (0 E, near the equator)
    l._contours(c, composed(l, c));
    const r = 360 * 1440;                                                           // row 360 (the equator)
    assert.equal(l._sm.J[r + 719], isJump ? 1 : 0, `${step} codes: the cell across the step`);
    assert.equal(l._sm.J[r + 700], 0, `${step} codes: a cell away from it`);
    assert.equal(l._sm.S[r + 719] > 100, !isJump, `${step} codes: blended across the step or not`);
  }
});

test('every node a tile\'s samples read has been smoothed for this frame, also when they end at a node-block seam', () => {
  const TPB = Object.assign({}, TP, { interpolation: 'bilinear' });
  const JUMPY = (r, c) => 1 + Math.round(90 + 60 * Math.sin(c / 7) * Math.cos(r / 5)) + ((Math.floor(c / 40) % 2) ? 25 : 0);
  const fr = frame(1440, 721, JUMPY), cols = 1440, rows = 721;
  const z = 10, n = 256 * Math.pow(2, z), latOf = (Y) => Math.atan(Math.sinh(Math.PI - 2 * Math.PI * Y / n)) * 180 / Math.PI;
  // tiles whose last sample column / row is the last node of a smoothing block (64 columns, 32 rows), so the next node
  // (read with the bilinear weight) lies in a block no sample of the tile falls in
  let colTile = null, rowTile = null;
  for (let x = 0; x < 1024 && !colTile; x++) { const c = ((x * 256 + 256.5) / n * 360) / 0.25; if (Math.floor(c) % 64 === 63 && c - Math.floor(c) > 0.1) colTile = { z, x, y: 400 }; }
  for (let y = 40; y < 900 && !rowTile; y++) { const r = (90 - latOf(y * 256 + 256.5)) / 0.25; if (Math.floor(r) % 32 === 31 && r - Math.floor(r) > 0.1) rowTile = { z, x: 30, y }; }
  assert.ok(colTile && rowTile, 'seam tiles found');
  for (const c of [colTile, rowTile, { z: 2, x: 1, y: 1 }, { z: 6, x: 11, y: 20 }]) {
    const lazy = layer(fr, GRID, 'tp', TPB), up = layer(fr, GRID, 'tp', TPB);
    lazy.setContours({ step: 2, per: 1 }, true); up.setContours({ step: 2, per: 1 }, true);
    up._contours(c, composed(up, c));
    I.smoothBlock(fr.q, cols, rows, up._sm.S, up._sm.J, up._sm.jump, 0, rows, 0, cols); up._sm.done.fill(1);
    lazy._contours(c, composed(lazy, c));                                          // a fresh layer: only this tile's blocks are filled
    const cellPx = 256 * Math.pow(2, c.z) * 0.25 / 360, sp = cellPx >= 6 ? 4 : 2, m = 256 / sp + 2, wide = cellPx < sp;
    let read = 0;
    for (let j = 0; j < m; j++) {
      const rr = lazy._sr[j]; if (!(rr >= 0 && rr <= rows - 1)) continue;
      const r0 = Math.floor(rr), r1 = Math.min(rows - 1, r0 + 1);
      for (let i = 0; i < m; i++) {
        const c0 = Math.floor(lazy._sc[i]), c1 = (c0 + 1) % cols;
        for (const k of [r0 * cols + c0, r0 * cols + c1, r1 * cols + c0, r1 * cols + c1]) {
          assert.equal(lazy._sm.S[k], up._sm.S[k], `z${c.z} ${c.x},${c.y}: S at node ${k}`); read++;
        }
        for (let dr = wide ? -1 : 0; dr <= (wide ? 1 : 0); dr++) for (let dc = wide ? -1 : 0; dc <= (wide ? 1 : 0); dc++) {
          const rj = r0 + dr; if (rj < 0 || rj >= rows) continue;
          const k = rj * cols + (c0 + dc + cols) % cols;
          assert.equal(lazy._sm.J[k], up._sm.J[k], `z${c.z} ${c.x},${c.y}: J at node ${k}`);
        }
      }
    }
    assert.ok(read > 1000, `${read} node reads`);
  }
});
