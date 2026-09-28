'use strict';
// static_ui/tools.js (plan section 29): geodesy, units, the coast-v1 decoder, the ray test, the exposure wedges,
// snapping to water and the fan. Run by tests/test_ui_module.py and CI.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'tools.js'), 'utf8');
function load(win) { new Function('window', SRC)(win); return win.AllshoreTools; }
const T = load({})._internals;

// A JS encoder for coast-v1 (mirror of tools/coast/build_coast.py encode_file), for fixtures.
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
const sq = (x0, y0, x1, y1) => [D(x0), D(y0), D(x1), D(y0), D(x1), D(y1), D(x0), D(y1)];
const set = (pieces, cell) => T.decodeCoastLL(encodeCoast(pieces, cell || 5));
function exposure(origin, nearPieces, farPieces) {
  return T.computeExposure(origin, nearPieces ? [set(nearPieces, 5)] : [], farPieces ? set(farPieces, 30) : null);
}

test('geodesy: known distances, bearings, destinations and the dateline', () => {
  const hnl = { lat: 21.3187, lng: -157.9225 }, lax = { lat: 33.9416, lng: -118.4085 };
  assert.ok(Math.abs(T.distanceKm(hnl, lax) - 4111) < 10, String(T.distanceKm(hnl, lax)));
  assert.ok(Math.abs(T.distanceKm({ lat: 0, lng: 0 }, { lat: 0, lng: 1 }) - 111.195) < 0.01);
  assert.ok(Math.abs(T.bearingDeg({ lat: 0, lng: 0 }, { lat: 1, lng: 0 })) < 1e-9);
  assert.ok(Math.abs(T.bearingDeg({ lat: 0, lng: 0 }, { lat: 0, lng: 1 }) - 90) < 1e-9);
  const d = T.destination(hnl, 57, 1234);
  assert.ok(Math.abs(T.distanceKm(hnl, d) - 1234) < 1e-6 && Math.abs(T.bearingDeg(hnl, d) - 57) < 1e-6);
  const across = T.distanceKm({ lat: 10, lng: 179.5 }, { lat: 10, lng: -179.5 });
  assert.ok(Math.abs(across - 109.5) < 0.2, 'the short way across the dateline: ' + across);
  const line = T.densify({ lat: 10, lng: 179.5 }, { lat: 10, lng: -179.5 }, 10);
  assert.ok(line[line.length - 1].lng > 180 && line.every((p, i) => !i || Math.abs(p.lng - line[i - 1].lng) < 1), 'continuous longitudes');
});

test('spherical area: a 1-degree cell at the equator, a triangle, both windings, across the dateline', () => {
  const cell = [{ lat: 0, lng: 0 }, { lat: 0, lng: 1 }, { lat: 1, lng: 1 }, { lat: 1, lng: 0 }];
  const a = T.sphericalAreaKm2(cell);
  assert.ok(Math.abs(a - 12363.7) / 12363.7 < 0.005, String(a));
  assert.ok(Math.abs(T.sphericalAreaKm2(cell.slice().reverse()) - a) < 1e-6, 'either winding');
  const moved = cell.map((p) => ({ lat: p.lat, lng: p.lng + 179.5 > 180 ? p.lng + 179.5 - 360 : p.lng + 179.5 }));
  assert.ok(Math.abs(T.sphericalAreaKm2(moved) - a) < 1, 'the same cell straddling the dateline');
  const oct = T.sphericalAreaKm2([{ lat: 0, lng: 0 }, { lat: 0, lng: 90 }, { lat: 90, lng: 0 }]);
  assert.ok(Math.abs(oct - 4 * Math.PI * 6371.0088 ** 2 / 8) / oct < 0.005, 'an octant of the sphere: ' + oct);
  assert.equal(T.sphericalAreaKm2(cell.slice(0, 2)), 0);
});

test('units: lengths in the site unit plus nautical miles, areas in acres / sq mi or ha / km2', () => {
  assert.equal(T.fmtLength(0.3, 'US'), '984 ft · 0.16 nm');
  assert.equal(T.fmtLength(4111, 'US'), '2,554 mi · 2,220 nm');
  assert.equal(T.fmtLength(12.34, 'Metric'), '12.3 km · 6.66 nm');
  assert.equal(T.fmtLength(0.5, 'Metric'), '500 m · 0.27 nm');
  assert.equal(T.fmtArea(2, 'US'), '494 acres');
  assert.equal(T.fmtArea(12363.7, 'US'), '4,774 sq mi');
  assert.equal(T.fmtArea(0.5, 'Metric'), '50.0 ha');
  assert.equal(T.fmtArea(12363.7, 'Metric'), '12,364 km²');
  assert.equal(T.compass(0), 'N'); assert.equal(T.compass(292.5), 'WNW'); assert.equal(T.compass(359), 'N');
});

test('coast-v1 decoder: round trip in degrees, corrupted input refused', () => {
  const s = set([[sq(10, 10, 11, 11)], [sq(-158.28, 21.26, -157.65, 21.71), sq(-158, 21.4, -157.9, 21.5)]], 5);
  assert.equal(s.n, 2); assert.equal(s.cell, 5);
  assert.deepEqual(Array.from(s.ll.slice(0, 8)), [10, 10, 11, 10, 11, 11, 10, 11]);
  assert.deepEqual(Array.from(s.box.slice(4, 8)).map((v) => +v.toFixed(4)), [-158.28, 21.26, -157.65, 21.71]);
  assert.equal(s.ringStart[2] - s.ringStart[1], 2, 'a piece with two rings');
  const buf = encodeCoast([[sq(10, 10, 11, 11)]], 5);
  new Uint8Array(buf)[0] = 0;
  assert.throws(() => T.decodeCoastLL(buf));
  assert.throws(() => T.decodeCoastLL(encodeCoast([[sq(10, 10, 11, 11)]], 5).slice(0, 45)));
});

test('the ray test: segment crossings (an island narrower than a step is not missed), the first crossing wins', () => {
  const origin = { lat: 0, lng: 0 };
  const thin = set([[sq(0.9, -0.5, 0.9005, 0.5)]], 5);                         // 55 m wide, 11 km tall, 100 km east
  const ix = T.buildIndexes(origin, [], thin);
  const f = T.rayFetch(origin, 90, null, ix.far);
  assert.ok(Math.abs(f - 100.08) < 0.1, 'stops at the thin island: ' + f);
  assert.equal(T.rayFetch(origin, 270, null, ix.far), T.CAP_KM, 'nothing the other way');
  const two = set([[sq(0.9, -0.5, 1, 0.5)], [sq(0.5, -0.5, 0.6, 0.5)]], 5);
  const ix2 = T.buildIndexes(origin, [], two);
  assert.ok(Math.abs(T.rayFetch(origin, 90, null, ix2.far) - 55.6) < 0.1, 'the nearer island');
});

test('cell-line edges from the builder\'s clipping are not coastlines (a bridge along 30 N)', () => {
  // a concave coast clipped at a cell line leaves an edge along it; here a strip along lat 30 from lon -88 to -86
  const bridge = [D(-88), D(30), D(-86), D(30), D(-86), D(30.2), D(-86.1), D(30.2), D(-86.1), D(30.0001), D(-87.9), D(30.0001), D(-87.9), D(30.2), D(-88), D(30.2)];
  const origin = { lat: 29.5, lng: -87 };
  const ix = T.buildIndexes(origin, [set([[bridge]], 5)], null);
  const f = T.rayFetch(origin, 0.25, ix.near, null);
  assert.ok(f > 60, 'the ray passes the cell line (only the real coast at 30.0001 would stop it): ' + f);
  assert.equal(T.onCellLine(-88, 30, -86, 30, 5), true); assert.equal(T.onCellLine(-88, 30.0001, -86, 30.0001, 5), false);
  assert.equal(T.onCellLine(-85, 20, -85, 25, 30), false, '-85 is not a 30-degree cell line');
  assert.equal(T.onCellLine(-85, 20, -85, 25, 5), true);
  assert.equal(T.onCellLine(-90, 20, -90, 25, 30), true);
});

test('exposure: the spot\'s own coast is dark, a distant island light, open ocean open; wedges are swell FROM', () => {
  // a point 2 km north of a long east-west coast (south blocked close by), an island 150 km to the west-north-west
  const origin = { lat: 21.018, lng: -158 };
  const coast = [sq(-159.5, 20.5, -156.5, 21)];
  const island = [sq(-159.55, 21.3, -159.3, 21.75)];
  const res = exposure(origin, [coast], [coast, island]);
  assert.equal(res.sectors.length, 72);
  const at = (b) => res.sectors[Math.floor(b / 5)];
  assert.equal(at(180).level, 'dark'); assert.ok(at(180).minLandKm < 3);
  assert.equal(at(0).level, 'open'); assert.equal(at(0).minLandKm, null);
  const wnw = res.sectors.filter((s) => s.from >= 285 && s.to <= 300);
  assert.ok(wnw.every((s) => s.level === 'light'), JSON.stringify(wnw.map((s) => [s.from, s.level, +s.s.toFixed(2)])));
  assert.equal(res.fRef, T.CAP_KM);
  assert.ok(T.windowsText(res.openWindows).startsWith('Open: '));
  assert.ok(res.openWindows.some((w) => w[0] <= 0 || w[1] >= 360 || (w[0] > w[1])) || res.openWindows.some((w) => w[0] < 30), 'north is open');
  assert.match(T.sectorText(at(180), 'US'), /^S 180–185°: shadowed \(\d+%\), land at \d+ mi$/);
  assert.match(T.sectorText(at(0), 'Metric'), /^N 000–005°: open, 3,000 km\+ of open water$/);
});

test('exposure adapts to an enclosed sea: every direction reaching the far shore reads open', () => {
  const origin = { lat: 0, lng: 0 };
  // a closed ring of land ~500 km away all round (a square "sea" 9 x 9 degrees)
  const walls = [[sq(-5, -5, 5, -4.5)], [sq(-5, 4.5, 5, 5)], [sq(-5, -4.5, -4.5, 4.5)], [sq(4.5, -4.5, 5, 4.5)]];
  const res = exposure(origin, null, walls);
  assert.ok(res.fRef > 490 && res.fRef < 720, String(res.fRef));
  assert.ok(res.sectors.every((s) => s.level === 'open'), 'no wedge shaded: ' + res.sectors.filter((s) => s.level !== 'open').length);
  assert.equal(T.windowsText(res.openWindows), 'Open to swell from every direction');
});

test('open windows merge through north and wedge levels follow the thresholds', () => {
  const mk = (levels) => levels.map((l, k) => ({ from: k * 5, to: k * 5 + 5, level: l }));
  const lv = Array(72).fill('dark'); for (let k = 58; k < 72; k++) lv[k] = 'open'; lv[0] = lv[1] = 'open'; lv[30] = 'open';
  assert.deepEqual(T.openWindows(mk(lv)), [[150, 155], [290, 10]]);
  assert.equal(T.windowsText([[290, 10]]), 'Open: 290°–010°');
  assert.equal(T.windowsText([]), 'No open swell window');
  assert.equal(T.levelOf(0.1), 'open'); assert.equal(T.levelOf(0.5), 'light'); assert.equal(T.levelOf(0.9), 'dark');
  assert.equal(T.rayShadow(5, 3000), 1); assert.equal(T.rayShadow(3000, 3000), 0);
  const kauai = T.rayShadow(150, 3000);
  assert.ok(kauai > 0.4 && kauai < 0.7, 'an island 150 km off counts about half: ' + kauai);
});

test('snap to water: a click just inside the land moves to the nearest water; deep inland is refused; water stays', () => {
  const land = set([[sq(-158.3, 21.25, -157.6, 21.7)]], 5);
  const water = T.snapToWater({ lat: 21.8, lng: -158 }, [land], 2);
  assert.deepEqual([water.snapped, water.lat, water.lng], [false, 21.8, -158]);
  const s = T.snapToWater({ lat: 21.695, lng: -158 }, [land], 2);
  assert.ok(s && s.snapped && s.lat > 21.7 && s.lat < 21.702, JSON.stringify(s));
  assert.equal(T.inLand([land], s.lng, s.lat), false);
  assert.equal(T.snapToWater({ lat: 21.45, lng: -158 }, [land], 2), null, 'deep inland');
});

test('the fan: 72 wedge paths, the selected one outlined; sectorAt maps screen offsets to wedges', () => {
  const sectors = Array.from({ length: 72 }, (_, k) => ({ from: k * 5, to: k * 5 + 5, level: k < 36 ? 'open' : k < 54 ? 'light' : 'dark' }));
  const f = T.fanSvg({ sectors }, 120, 3);
  assert.equal((f.svg.match(/<path /g) || []).length, 72);
  assert.equal((f.svg.match(/stroke="#fde047"/g) || []).length, 1);
  assert.equal(f.size, 272); assert.equal(f.center, 136);
  assert.equal(T.sectorAt(0, -50, 120), 0, 'straight up = north');
  assert.equal(T.sectorAt(50, 0, 120), 18, 'right = east (090-095)');
  assert.equal(T.sectorAt(0, 50, 120), 36);
  assert.equal(T.sectorAt(-50, -0.1, 120), 54, 'just north of west = 270-275');
  assert.equal(T.sectorAt(-50, 0.1, 120), 53, 'just south of west = 265-270');
  assert.equal(T.sectorAt(0, -200, 120), -1); assert.equal(T.sectorAt(1, 1, 120), -1);
});

test('the coast source: index + tier 0 loaded once; tier-1 cells around a point; a failed chunk gives null', async () => {
  const calls = [];
  const idx = { format: 'coast-v1', tier0: { max_zoom: 6 }, tier1: { cell: 5, dir: 'f', cells: { '20_-160': [1, 1], '20_-155': [1, 1] } } };
  const body = (b) => ({ ok: true, json: async () => b, arrayBuffer: async () => b });
  const fetchFn = async (url) => {
    calls.push(url.replace('https://c', ''));
    if (url.endsWith('/index.json')) return body(idx);
    if (url.endsWith('/world-i.bin')) return body(encodeCoast([[sq(10, 10, 11, 11)]], 30));
    if (url.endsWith('20_-160.bin')) return body(encodeCoast([[sq(-158.3, 21.25, -157.6, 21.7)]], 5));
    return { ok: false, status: 503 };
  };
  const cs = new T.CoastSource('https://c', fetchFn);
  await cs.load(); await cs.load();
  assert.deepEqual(calls, ['/index.json', '/world-i.bin']);
  const near = await cs.near({ lat: 21.7, lng: -158 });
  assert.equal(near.length, 1, 'only the listed cell in the window');
  assert.deepEqual(calls.slice(2), ['/f/20_-160.bin']);
  await cs.near({ lat: 21.7, lng: -158 });
  assert.equal(calls.length, 3, 'cached');
  assert.equal(await cs.near({ lat: 21.7, lng: -155.2 }), null, 'a failed chunk = null (tier 0 stands in)');
});

test('computeExposure is time-sliced and can be stopped', async () => {
  let yields = 0, stopAfter = 3;
  const out = await T.computeExposure({ lat: 0, lng: 0 }, [], null, { batch: 100, yieldFn: () => { yields++; return Promise.resolve(); }, shouldStop: () => yields >= stopAfter });
  assert.equal(out, null);
  yields = 0; stopAfter = 1e9;
  const full = await T.computeExposure({ lat: 0, lng: 0 }, [], null, { batch: 100, yieldFn: () => { yields++; return Promise.resolve(); } });
  assert.equal(yields, 7); assert.equal(full.sectors.length, 72);
  assert.ok(full.sectors.every((s) => s.level === 'open'));
});

test('active() and click() before init are harmless', () => {
  const w = {}; const A = load(w);
  assert.equal(A.active(), false); A.click({ lat: 0, lng: 0 });
  assert.equal(A.init({}), null);
});
