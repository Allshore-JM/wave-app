'use strict';
// static_ui/tools.js (plan section 29): geodesy, units, the coast-v1 decoder, the coast edges (cell-line
// cancellation), the ray test, the exposure wedges on synthetic and real (Hawaii crop) coasts, where a click is
// evaluated, the fan and its placement, and the coast source. Run by tests/test_ui_module.py and CI.
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
const sq = (x0, y0, x1, y1) => [D(x0), D(y0), D(x1), D(y0), D(x1), D(y1), D(x0), D(y1)];     // CCW in (lon, lat)
const set = (pieces, cell) => T.decodeCoastLL(encodeCoast(pieces, cell || 5));
function exposure(origin, nearPieces, farPieces) {
  return T.computeExposure(origin, nearPieces ? [set(nearPieces, 5)] : [], farPieces ? set(farPieces, 30) : null);
}
const levels = (res) => res.sectors.map((s) => (s.level === 'open' ? '.' : s.level === 'light' ? '+' : '#')).join('');
function fixture(name) { const b = fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'coast', name)); return T.decodeCoastLL(b.buffer.slice(b.byteOffset, b.byteOffset + b.length)); }

test('geodesy: known distances, bearings, destinations, the dateline and antipodes', () => {
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
  const anti = T.densify({ lat: 0, lng: 0 }, { lat: 0, lng: 180 }, 50);
  assert.equal(anti.length, 2, 'antipodes are joined directly (no zigzag)');
  assert.ok(anti.every((p) => Number.isFinite(p.lat) && Number.isFinite(p.lng)));
});

test('spherical area: a 1-degree cell, both windings, the dateline, a pole, a crossing outline', () => {
  const cell = [{ lat: 0, lng: 0 }, { lat: 0, lng: 1 }, { lat: 1, lng: 1 }, { lat: 1, lng: 0 }];
  const a = T.sphericalAreaKm2(cell);
  assert.ok(Math.abs(a - 12363.7) / 12363.7 < 0.005, String(a));
  assert.ok(Math.abs(T.sphericalAreaKm2(cell.slice().reverse()) - a) < 1e-6, 'either winding');
  const moved = cell.map((p) => ({ lat: p.lat, lng: p.lng + 179.5 > 180 ? p.lng + 179.5 - 360 : p.lng + 179.5 }));
  assert.ok(Math.abs(T.sphericalAreaKm2(moved) - a) < 1, 'the same cell straddling the dateline');
  const oct = T.sphericalAreaKm2([{ lat: 0, lng: 0 }, { lat: 0, lng: 90 }, { lat: 90, lng: 0 }]);
  assert.ok(Math.abs(oct - 4 * Math.PI * 6371.0088 ** 2 / 8) / oct < 0.005, 'an octant of the sphere: ' + oct);
  const cap = T.sphericalAreaKm2([{ lat: 80, lng: 0 }, { lat: 80, lng: 120 }, { lat: 80, lng: -120 }]);
  assert.ok(cap > 1.4e6 && cap < 1.8e6, 'a polygon around the pole: the cap, not the rest of the globe: ' + cap);
  assert.equal(T.sphericalAreaKm2(cell.slice(0, 2)), 0);
  assert.equal(T.selfIntersects(cell), false);
  assert.equal(T.selfIntersects([cell[0], cell[2], cell[1], cell[3]]), true, 'a bow tie');
  assert.equal(T.selfIntersects(cell.slice(0, 3)), false);
});

test('units: rounded before the unit and precision are chosen (no "10.00", "640 acres", "100.0 ha")', () => {
  assert.equal(T.fmtLength(0.3, 'US'), '984 ft · 0.16 nm');
  assert.equal(T.fmtLength(4111, 'US'), '2,554 mi · 2,220 nm');
  assert.equal(T.fmtLength(12.34, 'Metric'), '12.3 km · 6.66 nm');
  assert.equal(T.fmtLength(0.5, 'Metric'), '500 m · 0.27 nm');
  assert.equal(T.fmtLength(0.9996, 'Metric'), '1.00 km · 0.54 nm');
  assert.equal(T.fmtLength(9.998 * 1.609344, 'US'), '10.0 mi · 8.69 nm');
  assert.equal(T.fmtLength(99.98 * 1.609344, 'US').split(' ·')[0], '100 mi');
  assert.equal(T.fmtArea(2, 'US'), '494 acres');
  assert.equal(T.fmtArea(0.9996 * 2.589988110336, 'US'), '1.00 sq mi');
  assert.equal(T.fmtArea(12363.7, 'US'), '4,774 sq mi');
  assert.equal(T.fmtArea(0.5, 'Metric'), '50.0 ha');
  assert.equal(T.fmtArea(0.99999, 'Metric'), '1.00 km²');
  assert.equal(T.fmtArea(12363.7, 'Metric'), '12,364 km²');
  assert.equal(T.fmtDist(0.1, 'US'), 'under 0.1 mi'); assert.equal(T.fmtDist(0.2, 'US'), '0.1 mi');
  assert.equal(T.fmtDist(1.6, 'US'), '1.0 mi'); assert.equal(T.fmtDist(9.985 * 1.609344, 'US'), '10 mi'); assert.equal(T.fmtDist(150, 'Metric'), '150 km');
  assert.equal(T.fmtReach('US'), '1,800+ mi'); assert.equal(T.fmtReach('Metric'), '3,000+ km');
  assert.equal(T.compass(0), 'N'); assert.equal(T.compass(292.5), 'WNW'); assert.equal(T.compass(359), 'N');
});

test('coast-v1 decoder: round trip in degrees; corrupted or impossible files refused before allocating', () => {
  const s = set([[sq(10, 10, 11, 11)], [sq(-158.28, 21.26, -157.65, 21.71), sq(-158, 21.4, -157.9, 21.5)]], 5);
  assert.equal(s.n, 2); assert.equal(s.cell, 5);
  assert.deepEqual(Array.from(s.ll.slice(0, 8)), [10, 10, 11, 10, 11, 11, 10, 11]);
  assert.deepEqual(Array.from(s.box.slice(4, 8)).map((v) => +v.toFixed(4)), [-158.28, 21.26, -157.65, 21.71]);
  assert.equal(s.ringStart[2] - s.ringStart[1], 2, 'a piece with two rings');
  const buf = encodeCoast([[sq(10, 10, 11, 11)]], 5);
  new Uint8Array(buf)[0] = 0;
  assert.throws(() => T.decodeCoastLL(buf));
  assert.throws(() => T.decodeCoastLL(encodeCoast([[sq(10, 10, 11, 11)]], 5).slice(0, 45)));
  const huge = encodeCoast([[sq(10, 10, 11, 11)]], 5); new DataView(huge).setUint32(20, 3000000, true); new DataView(huge).setUint32(16, 1000000, true);
  assert.throws(() => T.decodeCoastLL(huge), 'counts the stream cannot hold');
  const extra = new Uint8Array(encodeCoast([[sq(10, 10, 11, 11)]], 5).byteLength + 1); extra.set(new Uint8Array(encodeCoast([[sq(10, 10, 11, 11)]], 5)));
  assert.throws(() => T.decodeCoastLL(extra.buffer), 'trailing bytes');
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
  // an edge the ray's LINE would cross, but beside the ray (u outside [0, 1])
  const beside = set([[sq(0.3, 0.2, 0.31, 0.3)]], 5);
  assert.equal(T.rayFetch(origin, 90, T.buildIndexes(origin, [beside], null).near, null), T.CAP_KM, 'an island north of an eastward ray is not hit');
});

test('cell lines: clip borders and zero-width bridges cancel (the Pensacola false coast), real coast along a line stays', () => {
  const origin = { lat: 29.9, lng: -87.15 };                                   // 11 km south of the 30 N cell line
  // a zero-width bridge out and back along 30 N (as the builder's clip leaves it) between two legs, and the real
  // coast at 30.3 N (44 km, inside the 50 km near phase)
  const bridge = [D(-88), D(30.3), D(-88), D(30), D(-86), D(30), D(-86), D(30.3), D(-86.1), D(30.3), D(-86.1), D(30), D(-87.9), D(30), D(-87.9), D(30.3)];
  const near = [set([[bridge], [sq(-88, 30.3, -86, 30.6)]], 5)];
  const f = T.rayFetch(origin, 0.25, T.buildIndexes(origin, near, null).near, null);
  assert.ok(f > 44 && f < 45, 'the ray passes the bridge at 11 km and stops on the real coast at 44.5 km: ' + f);
  // the same land where the line is REAL coast: one piece whose south edge lies on 30 N (once, one direction)
  const real = [set([[[D(-88), D(30), D(-86), D(30), D(-86), D(30.3), D(-88), D(30.3)]]], 5)];
  const g = T.rayFetch(origin, 0.25, T.buildIndexes(origin, real, null).near, null);
  assert.ok(Math.abs(g - 11.12) < 0.1, 'a real coastline lying on the cell line still stops the ray: ' + g);
  // two cells' pieces meeting along 30 N: their borders run in opposite directions and cancel
  const south = [D(-88), D(29.9), D(-86), D(29.9), D(-86), D(30), D(-88), D(30)];
  const north = [D(-88), D(30), D(-86), D(30), D(-86), D(30.1), D(-88), D(30.1)];
  const e = T.collectEdges([set([[south], [north]], 5)], { lat: 30, lng: -87 }, 2, 1);
  const onLine = []; for (let i = 0; i < e.length; i += 4) if (Math.abs(e[i + 1] - 30) < 1e-9 && Math.abs(e[i + 3] - 30) < 1e-9) onLine.push(e.slice(i, i + 4));
  assert.equal(onLine.length, 0, 'the shared border cancels');
  assert.equal(T.onCellLine(-85, 20, -85, 25, 30), false); assert.equal(T.onCellLine(-85, 20, -85, 25, 5), true); assert.equal(T.onCellLine(-90, 20, -90, 25, 30), true);
});

test('the reference: open ocean through a wedge uses the full reach; an enclosed sea its own rays; a harbour the floor', () => {
  const rays = (fn) => Array.from({ length: 720 }, (_, i) => fn(i));
  assert.equal(T.referenceKm(rays((i) => (i < 10 ? 3000 : 5))), T.CAP_KM, 'one wedge of open ocean (10 rays)');
  assert.equal(T.referenceKm(rays((i) => (i < 10 ? 3000 : i < 210 ? 100 : 5))), T.CAP_KM, 'open ocean wins over many leaving rays that stop at 100 km');
  assert.equal(T.referenceKm(rays((i) => (i < 9 ? 3000 : i < 210 ? 100 : 5))), 100, 'one ray short of a wedge: the percentile');
  assert.equal(T.referenceKm(rays((i) => (i < 9 ? 3000 : i < 300 ? 60 : 5))), 60, 'most leaving rays stop at 60 km');
  assert.equal(T.referenceKm(rays((i) => (i < 9 ? 3000 : i < 300 ? 40 : 5))), T.REF_MIN_KM, 'never below the floor');
  assert.equal(T.referenceKm(rays(() => 5)), T.REF_MIN_KM, 'a harbour: nothing leaves');
  assert.equal(T.referenceKm(rays((i) => 500 + i)), 1147, 'an enclosed sea: its own 90th percentile');
  // stability near the shore (G20 B P1-3): the result no longer flips around 72 capped rays
  assert.equal(T.referenceKm(rays((i) => (i < 72 ? 3000 : i < 400 ? 3 : 150))), T.CAP_KM);
  assert.equal(T.referenceKm(rays((i) => (i < 74 ? 3000 : i < 400 ? 3 : 150))), T.CAP_KM);
});

test('the shadow of one ray: full within 15 km, log fall-off, nothing beyond 1,000 km (owner) or the reference', () => {
  assert.equal(T.rayShadow(5, 3000), 1); assert.equal(T.rayShadow(15, 3000), 1);
  assert.equal(T.rayShadow(1000, 3000), 0); assert.equal(T.rayShadow(1200, 3000), 0);
  assert.equal(T.rayShadow(300, 300), 0);
  const kauai = T.rayShadow(150, 3000);
  assert.ok(kauai > 0.4 && kauai < 0.5, 'Kauai from the North Shore (~150 km): light grey: ' + kauai);
  assert.ok(T.rayShadow(45, 3000) >= 0.7, 'the Channel Islands from Rincon (~45 km): dark');
  assert.ok(Math.abs(T.rayShadow(100, 300) - (1 - Math.log(100 / 15) / Math.log(300 / 15))) < 1e-12);
  assert.equal(T.levelOf(0.19), 'open'); assert.equal(T.levelOf(0.2), 'light'); assert.equal(T.levelOf(0.69), 'light'); assert.equal(T.levelOf(0.7), 'dark');
});

test('exposure on synthetic coasts: own coast dark, a distant island light, open ocean open; wedges are swell FROM', () => {
  const origin = { lat: 21.018, lng: -158 };                                   // 2 km north of a long east-west coast
  const coast = [sq(-159.5, 20.5, -156.5, 21)];
  const island = [sq(-159.55, 21.3, -159.3, 21.75)];                          // ~150 km WNW
  const res = exposure(origin, [coast], [coast, island]);
  assert.equal(res.sectors.length, 72);
  const at = (b) => res.sectors[Math.floor(b / 5)];
  assert.equal(at(180).level, 'dark'); assert.ok(at(180).minLandKm < 3);
  assert.equal(at(0).level, 'open'); assert.equal(at(0).minLandKm, null);
  const wnw = res.sectors.filter((s) => s.from >= 285 && s.to <= 300);
  assert.ok(wnw.every((s) => s.level === 'light'), JSON.stringify(wnw.map((s) => [s.from, s.level, +s.s.toFixed(2)])));
  assert.equal(res.fRef, T.CAP_KM);
  assert.match(T.sectorText(at(180), 'US'), /^S 180–185°: shadowed \(\d+%\), land at [\d.]+ mi$/);
  assert.equal(T.sectorText(at(0), 'Metric'), 'N 000–005°: open ocean for 3,000+ km');
});

test('exposure adapts to an enclosed sea (every direction reaching the far shore reads open), and to a small one', () => {
  const walls = (r) => [sq(-r - 0.5, -r - 0.5, r + 0.5, -r), sq(-r - 0.5, r, r + 0.5, r + 0.5), sq(-r - 0.5, -r, -r, r), sq(r, -r, r + 0.5, r)];
  const big = exposure({ lat: 0, lng: 0 }, null, walls(4.5).map((w) => [w]));
  assert.ok(big.fRef > 490 && big.fRef < 720, String(big.fRef));
  assert.equal(T.windowsText(big.openWindows), 'Open to swell from every direction');
  const small = exposure({ lat: 0, lng: 0 }, walls(0.4).map((w) => [w]), walls(0.4).map((w) => [w]));
  assert.ok(small.fRef < 70 && small.fRef >= T.REF_MIN_KM, 'a 90 km sea keeps its own scale: ' + small.fRef);
  assert.ok(small.sectors.filter((s) => s.level === 'open').length > 36, 'a small sea is mostly open (G20 B P2-8)');
});

test('real coast (Hawaii crop of the published data): the owner\'s examples, pinned', () => {
  const t1 = fixture('oahu-t1.bin'), t0 = fixture('hawaii-t0.bin');
  function at(lat, lng) { const o = T.placeOrigin({ lat, lng }, [t1]); return { o, r: T.computeExposure({ lat: o.lat, lng: o.lng }, [t1], t0) }; }
  const pipe = at(21.6655, -158.054);
  assert.ok(pipe.o.moved > 0.15 && pipe.o.moved < 0.5, 'a beach click is evaluated off the shore: ' + pipe.o.moved);
  assert.equal(levels(pipe.r), '.........########################################+.....++++.............');
  assert.equal(T.windowsText(pipe.r.openWindows), 'Open: W (250°–275°), N (295°–045°)');
  assert.equal(levels(at(21.269, -157.829).r), '############################+..........................+################', 'Waikiki: south open, north dark');
  assert.equal(levels(at(21.5975, -158.109).r), '####################################################+..+++++...........#', 'Haleiwa: Kauai light');
});

test('where a click is evaluated: 150 m off the shore; land within 2 km snaps; deep inland refused; a point ON an edge works', () => {
  const land = set([[sq(-158.3, 21.25, -157.6, 21.7)]], 5);
  const open = T.placeOrigin({ lat: 21.8, lng: -158 }, [land]);
  assert.deepEqual([open.moved, open.lat, open.lng], [0, 21.8, -158], 'open water stays');
  const beach = T.placeOrigin({ lat: 21.695, lng: -158 }, [land]);
  assert.ok(beach && beach.lat > 21.7013 && beach.lat < 21.7022, 'a land click lands ~150 m off the shore: ' + JSON.stringify(beach));
  assert.equal(T.inLand([land], beach.lng, beach.lat), false);
  const surf = T.placeOrigin({ lat: 21.7003, lng: -158 }, [land]);
  assert.ok(surf.lat > 21.7013 && surf.moved < 0.15, 'a water click 33 m out moves to 150 m: ' + JSON.stringify(surf));
  const edge = T.placeOrigin({ lat: 21.7, lng: -158 }, [land]);
  assert.ok(edge && edge.lat > 21.7013, 'exactly on the coast edge: stepped along its normal (G20 B P3-9)');
  assert.equal(T.placeOrigin({ lat: 21.45, lng: -158 }, [land]), null, 'deep inland');
  // a narrow channel: the best clearance within reach, not a walk across to the far shore
  const banks = set([[sq(-158.3, 21.5, -157.6, 21.6)], [sq(-158.3, 21.6015, -157.6, 21.7)]], 5);  // 167 m wide
  const mid = T.placeOrigin({ lat: 21.6003, lng: -158 }, [banks]);
  assert.ok(mid.lat > 21.6005 && mid.lat < 21.6011 && !T.inLand([banks], mid.lng, mid.lat), 'the middle of the channel: ' + JSON.stringify(mid));
  // a click near the head of a narrow inlet stays near it (at most twice the stand-off), not out at the mouth
  const inlet = set([[sq(-158.05, 21.5, -158.00078, 21.6)], [sq(-157.99922, 21.5, -157.95, 21.6)], [sq(-158.00078, 21.5136, -157.99922, 21.6)]], 5);
  const head = T.placeOrigin({ lat: 21.5132, lng: -158.0 }, [inlet]);
  assert.ok(head && head.moved <= 0.31 && !T.inLand([inlet], head.lng, head.lat), 'stays in the inlet: ' + JSON.stringify(head));
  // the clip border along a 5-degree line is not "the nearest coast" (G20 A P2-4)
  const cellEdge = set([[[D(-120.2), D(34.3), D(-120), D(34.3), D(-120), D(34.6), D(-120.2), D(34.6)]], [[D(-120), D(34.3), D(-119.9), D(34.3), D(-119.9), D(34.6), D(-120), D(34.6)]]], 5);
  assert.equal(T.placeOrigin({ lat: 34.46, lng: -120.0 }, [cellEdge]), null, 'inside land 15 km from any real shore: refused (not snapped onto the cell border)');
  const g2 = T.placeOrigin({ lat: 34.3015, lng: -120.0 }, [cellEdge]);
  assert.ok(g2 && g2.lat < 34.3 - 0.0012, 'near the real south shore, on the cell line: snaps south: ' + JSON.stringify(g2));
});

test('open windows: merged through north, compass names, the widest four listed', () => {
  const mk = (lv) => lv.map((l, k) => ({ from: k * 5, to: k * 5 + 5, level: l }));
  const lv = Array(72).fill('dark'); for (let k = 58; k < 72; k++) lv[k] = 'open'; lv[0] = lv[1] = 'open'; lv[30] = 'open';
  assert.deepEqual(T.openWindows(mk(lv)), [[150, 155], [290, 10]]);
  assert.equal(T.windowsText([[290, 10]]), 'Open: NNW (290°–010°)');
  assert.equal(T.windowsText([]), 'No open swell window');
  assert.equal(T.windowsText([[0, 360]]), 'Open to swell from every direction');
  assert.equal(T.windowsText([[10, 15], [30, 60], [90, 100], [120, 170], [200, 205], [250, 300]]), 'Open: NE (030°–060°), E (090°–100°), SE (120°–170°), W (250°–300°) +2 more');
});

test('the fan: 72 wedges, a clear centre ring, rims on open windows, separators only where the shading changes', () => {
  const sectors = Array.from({ length: 72 }, (_, k) => ({ from: k * 5, to: k * 5 + 5, level: k < 36 ? 'open' : k < 54 ? 'light' : 'dark' }));
  const f = T.fanSvg({ sectors, openWindows: [[0, 180]] }, 120, 3);
  assert.equal((f.svg.match(/<path data-k=/g) || []).length, 72);
  assert.equal((f.svg.match(/stroke="#fde047"/g) || []).length, 1, 'the selected wedge');
  assert.equal((f.svg.match(/class="tools-rim"/g) || []).length, 1, 'one rim arc for the one open window');
  assert.equal((f.svg.match(/stroke="rgba\(255,255,255,0\.7\)" stroke-width="1"\/>/g) || []).length, 3 + 1, 'three level changes (0, 180, 270) + the inner ring');
  assert.equal(f.size, 272); assert.equal(f.center, 136); assert.equal(T.innerRadius(120), 29);
  const all = T.fanSvg({ sectors: sectors.map((s) => Object.assign({}, s, { level: 'open' })), openWindows: [[0, 360]] }, 90, -1);
  assert.match(all.svg, /<circle class="tools-rim"/);
  assert.equal(T.sectorAt(0, -50, 120), 0, 'straight up = north');
  assert.equal(T.sectorAt(50, 0, 120), 18, 'right = east (090-095)');
  assert.equal(T.sectorAt(0, 50, 120), 36);
  assert.equal(T.sectorAt(-50, -0.1, 120), 54, 'just north of west = 270-275');
  assert.equal(T.sectorAt(-50, 0.1, 120), 53, 'just south of west = 265-270');
  assert.equal(T.sectorAt(0, -200, 120), -1); assert.equal(T.sectorAt(0, -20, 120), -1, 'inside the clear ring');
});

test('placing the fan: stays when clear, moves beside the tool bar, shrinks on a short map, gives up when nothing fits', () => {
  const bar = { l: 320, t: 120, r: 600, b: 330 };
  assert.deepEqual(T.placeFan(1280, 800, 400, 500, 120, [bar]), { x: 400, y: 500, r: 120 }, 'already clear');
  const moved = T.placeFan(610, 364, 450, 200, 120, [bar]);
  assert.ok(moved && moved.x + moved.r + 16 <= bar.l && moved.r <= 120, 'beside the bar on a short map: ' + JSON.stringify(moved));
  const tiny = T.placeFan(300, 200, 150, 100, 120, []);
  assert.ok(tiny && tiny.r < 120 && tiny.r >= 50, 'a small map gets a smaller fan: ' + JSON.stringify(tiny));
  assert.equal(T.placeFan(100, 100, 50, 50, 120, []), null, 'nothing fits');
  const inside = T.placeFan(1280, 800, 1270, 790, 120, []);
  assert.ok(inside.x <= 1280 - 136 - 8 && inside.y <= 800 - 136 - 8, 'pulled back inside the map');
});

test('the far window reaches as far as the rays: the whole circle when a ray can pass a pole', () => {
  assert.equal(T.farWindow(74.5).wx, 180);
  const w60 = T.farWindow(60);
  assert.ok(w60.wx > 60, 'at 60 N a 3,000 km ray reaches beyond 55 degrees of longitude: ' + w60.wx);
  assert.ok(T.farWindow(0).wx > 27 && T.farWindow(0).wx < 28.5);
});

test('the coast source: loaded once, retried after a failure; tier-1 cells deduplicated in flight; an LRU cache; a failed chunk gives null', async () => {
  const calls = [];
  const idx = { format: 'coast-v1', tier0: { max_zoom: 6 }, tier1: { cell: 5, dir: 'f', cells: { '20_-160': [1, 1], '20_-155': [1, 1] } } };
  const body = (b) => ({ ok: true, json: async () => b, arrayBuffer: async () => b });
  let failIndex = true;
  const fetchFn = async (url) => {
    calls.push(url.replace('https://c', ''));
    if (url.endsWith('/index.json')) { if (failIndex) { failIndex = false; return { ok: false, status: 503 }; } return body(idx); }
    if (url.endsWith('/world-i.bin')) return body(encodeCoast([[sq(10, 10, 11, 11)]], 30));
    if (url.endsWith('20_-160.bin')) return body(encodeCoast([[sq(-158.3, 21.25, -157.6, 21.7)]], 5));
    return { ok: false, status: 503 };
  };
  const cs = new T.CoastSource('https://c', fetchFn);
  await assert.rejects(cs.load());
  await cs.load(); await cs.load();
  assert.equal(calls.filter((c) => c === '/index.json').length, 2, 'retried once after the failure, then cached');
  calls.length = 0;
  const [a, b] = await Promise.all([cs.near({ lat: 21.7, lng: -158 }), cs.near({ lat: 21.7, lng: -158 })]);
  assert.equal(a.length, 1); assert.equal(a[0], b[0]);
  assert.deepEqual(calls, ['/f/20_-160.bin'], 'one request for two quick clicks');
  await cs.near({ lat: 21.7, lng: -158 });
  assert.equal(calls.length, 1, 'cached');
  assert.equal(await cs.near({ lat: 21.7, lng: -155.2 }), null, 'a failed chunk = null (tier 0 stands in)');
  cs.chunks.set('other', {});
  await cs.chunk('20_-160');
  assert.equal([...cs.chunks.keys()].pop(), '20_-160', 'a hit moves to the most recent end (LRU)');
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
