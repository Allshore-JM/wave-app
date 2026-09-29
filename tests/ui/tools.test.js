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

const { encodeCoast, D, sq } = require('./coastenc.js');
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
  assert.equal(T.fmtLength(0.4996 * 1.609344, 'US').split(' ·')[0], '0.50 mi', 'rounded to 0.50 mi, so miles (not 2,638 ft)');
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
  // the same with the land SOUTH of the line: its on-line edge runs the other way (net -1) and stays too
  const southLand = [set([[[D(-88), D(29.7), D(-86), D(29.7), D(-86), D(30), D(-88), D(30)]]], 5)];
  const o2 = { lat: 30.1, lng: -87.15 }, h = T.rayFetch(o2, 180.25, T.buildIndexes(o2, southLand, null).near, null);
  assert.ok(Math.abs(h - 11.12) < 0.1, 'either direction along the line: ' + h);
  // two cells' pieces meeting along 30 N: their borders run in opposite directions and cancel
  const south = [D(-88), D(29.9), D(-86), D(29.9), D(-86), D(30), D(-88), D(30)];
  const north = [D(-88), D(30), D(-86), D(30), D(-86), D(30.1), D(-88), D(30.1)];
  const e = T.collectEdges([set([[south], [north]], 5)], { lat: 30, lng: -87 }, 2, 1);
  const onLine = []; for (let i = 0; i < e.length; i += 4) if (Math.abs(e[i + 1] - 30) < 1e-9 && Math.abs(e[i + 3] - 30) < 1e-9) onLine.push(e.slice(i, i + 4));
  assert.equal(onLine.length, 0, 'the shared border cancels');
  assert.equal(T.onCellLine(-85, 20, -85, 25, 30), false); assert.equal(T.onCellLine(-85, 20, -85, 25, 5), true); assert.equal(T.onCellLine(-90, 20, -90, 25, 30), true);
});

test('the reference: open ocean through 15 rays uses the full reach, blended from 5; an enclosed sea its own rays; the floor', () => {
  const rays = (fn) => Array.from({ length: 720 }, (_, i) => fn(i));
  assert.equal(T.REF_MIN_KM, 100, 'a small bay or sound is not "open" (G20 re-review)');
  assert.equal(T.referenceKm(rays((i) => (i < 15 ? 3000 : 5))), T.CAP_KM, '15 rays of open ocean');
  assert.equal(T.referenceKm(rays((i) => (i < 15 ? 3000 : i < 215 ? 100 : 5))), T.CAP_KM, 'open ocean wins over many leaving rays that stop at 100 km');
  assert.equal(T.referenceKm(rays((i) => (i < 10 ? 3000 : 5))), T.CAP_KM, 'only open ocean leaves: its percentile is the full reach anyway');
  assert.equal(T.referenceKm(rays((i) => (i < 5 ? 3000 : i < 205 ? 150 : 5))), 150, '5 rays: the spot\'s own percentile');
  // no flip at one ray more or less (G20 re-review): each extra ray multiplies the reference by (3000 / own)^(1/10)
  const blend = (n) => T.referenceKm(rays((i) => (i < n ? 3000 : i < n + 200 ? 150 : 5)));
  assert.ok(Math.abs(blend(10) - Math.sqrt(150 * 3000)) < 1e-6, 'half way: the geometric mean: ' + blend(10));
  for (let n = 5; n < 15; n++) assert.ok(Math.abs(blend(n + 1) / blend(n) - Math.pow(20, 0.1)) < 1e-9, 'smooth at ' + n);
  assert.equal(T.referenceKm(rays((i) => (i < 5 ? 3000 : i < 300 ? 60 : 5))), T.REF_MIN_KM, 'never below the floor');
  assert.equal(T.referenceKm(rays(() => 5)), T.REF_MIN_KM, 'a harbour: nothing leaves');
  assert.equal(T.referenceKm(rays((i) => 500 + i)), 1147, 'an enclosed sea: its own 90th percentile');
  // the percentile is over the rays that LEAVE the spot's coast (> 15 km), not over all rays (G20 re-review mutants)
  assert.ok(Math.abs(T.referenceKm(rays((i) => (i < 600 ? 5 : 100 + (i - 600) * 900 / 119))) - (100 + 107 * 900 / 119)) < 1e-9, 'over the leaving rays');
  assert.equal(T.referenceKm(rays((i) => (i < 700 ? 10 : 500))), 500, 'rays stopping within 15 km do not dilute it');
  // stability near the shore (G20 B P1-3): the result no longer flips around 72 capped rays
  assert.equal(T.referenceKm(rays((i) => (i < 72 ? 3000 : i < 400 ? 3 : 150))), T.CAP_KM);
  assert.equal(T.referenceKm(rays((i) => (i < 74 ? 3000 : i < 400 ? 3 : 150))), T.CAP_KM);
});

test('the shadow of one ray: full within 15 km, log fall-off to the reference, far land fading out from 600 to 1,000 km', () => {
  assert.equal(T.rayShadow(5, 3000), 1); assert.equal(T.rayShadow(15, 3000), 1);
  assert.equal(T.rayShadow(1000, 3000), 0); assert.equal(T.rayShadow(1200, 3000), 0);
  assert.equal(T.rayShadow(300, 300), 0);
  const base = (f, ref) => 1 - Math.log(f / 15) / Math.log(ref / 15);
  const kauai = T.rayShadow(150, 3000);
  assert.ok(kauai > 0.5 && kauai < 0.6, 'Kauai from the North Shore (~150 km): light grey: ' + kauai);
  assert.ok(T.rayShadow(45, 3000) >= 0.7 && T.rayShadow(60, 3000) >= 0.7, 'the Channel Islands from Rincon (~45 km) and land at 60 km: dark');
  assert.ok(Math.abs(T.rayShadow(432, 3000) - base(432, 3000)) < 1e-12 && T.rayShadow(432, 3000) >= 0.2, 'land at 432 km still light (not the compressed curve)');
  assert.ok(Math.abs(T.rayShadow(100, 300) - base(100, 300)) < 1e-12);
  assert.equal(T.FAR_FADE_KM, 600);
  assert.ok(Math.abs(T.rayShadow(600, 3000) - base(600, 3000)) < 1e-12, 'the fade starts at 600 km');
  assert.ok(Math.abs(T.rayShadow(800, 3000) - base(800, 3000) / 2) < 1e-12, 'half way through the fade');
  assert.ok(T.rayShadow(999, 3000) < 0.001);
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
  const mid = exposure({ lat: 0, lng: 0 }, walls(1).map((w) => [w]), walls(1).map((w) => [w]));
  assert.ok(mid.fRef > 140 && mid.fRef < 150, 'a 220 km sea (Marmara-sized) keeps its own scale: ' + mid.fRef);
  assert.equal(T.windowsText(mid.openWindows), 'Open to swell from every direction', 'and reads open (G20 B P2-8)');
  const small = exposure({ lat: 0, lng: 0 }, walls(0.4).map((w) => [w]), walls(0.4).map((w) => [w]));
  assert.equal(small.fRef, T.REF_MIN_KM, 'a 90 km bay is held at the floor');
  assert.equal(T.windowsText(small.openWindows), 'No open swell window', 'a small bay or sound is sheltered, not "open" (G20 re-review)');
});

test('real coast (Hawaii crop of the published data): the owner\'s examples, pinned', () => {
  const t1 = fixture('oahu-t1.bin'), t0 = fixture('hawaii-t0.bin');
  function at(lat, lng) { const o = T.placeOrigin({ lat, lng }, [t1]); return { o, r: T.computeExposure({ lat: o.lat, lng: o.lng }, [t1], t0) }; }
  const pipe = at(21.6655, -158.054);
  assert.ok(pipe.o.moved > 0.15 && pipe.o.moved < 0.5, 'a beach click is evaluated off the shore: ' + pipe.o.moved);
  assert.equal(levels(pipe.r), '.........########################################+.....++++.............');
  assert.equal(T.windowsText(pipe.r.openWindows), 'Open: W (250°–275°), WNW–NE (295°–045°)');
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

test('where a click is evaluated (G20 re-review): never on the coastline, never through a spit, 2 km reach, local scale', () => {
  const lat0 = 21.5, kx = 111.32 * Math.cos(lat0 * Math.PI / 180), ky = 110.57;
  // two narrow V coves cut into a north coast (apexes at 21.5 N): the old walk evaluated hundreds of clicks around
  // them ON the coastline (every ray 0 km, "No open swell window")
  const ring = [D(-0.1), D(21.4), D(0.1), D(21.4), D(0.1), D(21.51), D(0.0031), D(21.51), D(0.003), D(21.5003), D(0.0029), D(21.51),
    D(0.0006), D(21.51), D(0), D(21.5), D(-0.0006), D(21.51), D(-0.1), D(21.51)];
  const cove = set([[ring]]);
  const clearance = (o) => {
    const e = T.collectEdges([cove], o, 0.02, 0.02); let md = Infinity;
    for (let i = 0; i < e.length; i += 4) {
      const x0 = e[i] * kx, y0 = (e[i + 1] - o.lat) * ky, ex = e[i + 2] * kx - x0, ey = (e[i + 3] - o.lat) * ky - y0, L2 = ex * ex + ey * ey;
      const t = Math.max(0, Math.min(1, -(x0 * ex + y0 * ey) / L2)); md = Math.min(md, Math.hypot(x0 + t * ex, y0 + t * ey));
    }
    return md;
  };
  let placed = 0, onCoast = 0;
  for (let dy = -0.0006; dy <= 0.00001; dy += 0.00004) for (let dx = -0.0008; dx <= 0.0038; dx += 0.00004) {
    const o = T.placeOrigin({ lat: lat0 + dy, lng: dx }, [cove]);
    if (!o) continue;
    placed++; if (clearance(o) < 0.005) onCoast++;
  }
  assert.ok(placed > 500, 'most clicks are placed: ' + placed);
  assert.equal(onCoast, 0, 'none within 5 m of a coast');
  // a water click in a 25 m channel beside a 100 m spit stays in its channel (it walked 165 m beyond the spit)
  const m = (v) => v / 1000 / 111.32;
  const spit = set([[sq(-0.1, -0.1, -m(10), 0.1)], [sq(m(15), -0.1, m(115), 0.1)], [sq(m(400), -0.1, 0.1, 0.1)]]);
  const ch = T.placeOrigin({ lat: 0, lng: 0 }, [spit]);
  assert.ok(ch && ch.lng < m(15), 'not across the spit: ' + JSON.stringify(ch));
  // a land click beside a channel too narrow for the stand-off stays in it, not across the next land (60 m channel,
  // 100 m of land, then open water)
  const strait = set([[sq(-0.1, -0.1, 0, 0.1)], [sq(m(60), -0.1, m(160), 0.1)]]);
  const narrow = T.placeOrigin({ lat: 0, lng: -m(20) }, [strait]);
  assert.ok(narrow && narrow.lng > 0 && narrow.lng < m(60), 'in the channel: ' + JSON.stringify(narrow));
  // the local scale follows the latitude (Thurso, 58.6 N): 150 m off an east-facing coast in true metres
  const kxT = 111.32 * Math.cos(58.6 * Math.PI / 180);
  const th = T.placeOrigin({ lat: 58.6, lng: -3.0 - 0.1 / kxT }, [set([[sq(-3.6, 58.3, -3.0, 58.9)]])]);
  const out = (th.lng + 3.0) * kxT * 1000;
  assert.ok(out > 140 && out < 180, 'metres off the coast: ' + out);
  // land clicks snap within 2 km of water, not beyond
  const blk = set([[sq(-0.5, -0.5, 0, 0.5)]]);
  assert.ok(T.placeOrigin({ lat: 0, lng: -1.5 / 111.32 }, [blk]), '1.5 km inland');
  assert.ok(T.placeOrigin({ lat: 0, lng: -1.95 / 111.32 }, [blk]), '1.95 km inland');
  assert.equal(T.placeOrigin({ lat: 0, lng: -2.5 / 111.32 }, [blk]), null, '2.5 km inland');
  // coast beyond the 2 km reach still counts for the clearance: a channel from 1.8 to 2.05 km north of a land click
  const isl = set([[sq(-0.5, -0.5, 0.5, 1.8 / ky)], [sq(-0.5, 2.05 / ky, 0.5, 0.5)]]);
  const mid = T.placeOrigin({ lat: 0, lng: 0 }, [isl]);
  assert.ok(mid && mid.lat * ky > 1.9 && mid.lat * ky < 1.94, 'the middle of the channel, not 50 m off the far shore: ' + (mid && mid.lat * ky));
});

test('open windows: merged through north, compass names (both ends when wide), the widest four listed, "open except"', () => {
  const mk = (lv) => lv.map((l, k) => ({ from: k * 5, to: k * 5 + 5, level: l }));
  const lv = Array(72).fill('dark'); for (let k = 58; k < 72; k++) lv[k] = 'open'; lv[0] = lv[1] = 'open'; lv[30] = 'open';
  assert.deepEqual(T.openWindows(mk(lv)), [[150, 155], [290, 10]]);
  assert.equal(T.windowsText([[250, 275]]), 'Open: W (250°–275°)', 'a narrow window by its centre');
  assert.equal(T.windowsText([[290, 10]]), 'Open: WNW–N (290°–010°)', 'a window of 45 degrees or more by its ends');
  assert.equal(T.windowsText([[15, 230]]), 'Open: NNE–SW (015°–230°)', 'Cape Hatteras (not "ESE")');
  assert.equal(T.windowsText([[350, 260]]), 'Open: N–W (350°–260°)', 'east of the Big Island (not "SE")');
  assert.equal(T.windowsText([[185, 180]]), 'Open except S (180°–185°)', 'north of Oahu (not "N (185°–180°)")');
  assert.equal(T.windowsText([[0, 170], [180, 355]]), 'Open except S (170°–180°), N (355°–000°)');
  assert.equal(T.windowsText([[195, 200], [205, 215]]), 'Open: SSW (195°–200°, 205°–215°)', 'neighbours with one name share it');
  assert.equal(T.windowsText([]), 'No open swell window');
  assert.equal(T.windowsText([[0, 360]]), 'Open to swell from every direction');
  assert.equal(T.windowsText([[10, 15], [30, 60], [90, 100], [120, 170], [200, 205], [250, 300]]), 'Open: NE (030°–060°), E (090°–100°), ESE–S (120°–170°), WSW–WNW (250°–300°) +2 more');
});

test('the fan: 72 wedges, a clear centre ring, rims on open windows, separators only where the shading changes', () => {
  const sectors = Array.from({ length: 72 }, (_, k) => ({ from: k * 5, to: k * 5 + 5, level: k < 36 ? 'open' : k < 54 ? 'light' : 'dark' }));
  const f = T.fanSvg({ sectors, openWindows: [[0, 180]] }, 120, 3);
  assert.equal((f.svg.match(/<path data-k=/g) || []).length, 72);
  assert.equal((f.svg.match(/<path data-k="\d+" [^>]*fill="none"/g) || []).length, 36, 'open wedges are clear (owner, G20)');
  assert.equal((f.svg.match(/stroke="#fde047"/g) || []).length, 1, 'the selected wedge');
  assert.equal((f.svg.match(/class="tools-rim"/g) || []).length, 1, 'one rim arc for the one open window');
  assert.equal((f.svg.match(/stroke="#0b2536" stroke-width="7"/g) || []).length, 1, 'on a dark underlay (G20 re-review: the overlay\'s cyan)');
  const wide = T.fanSvg({ sectors, openWindows: [[15, 230]] }, 120, -1).svg;
  assert.match(wide, /class="tools-rim" d="M[\d.,]+A118,118 0 1 1 /, 'a rim over 180 degrees takes the large arc');
  assert.match(T.fanSvg({ sectors, openWindows: [[15, 100]] }, 120, -1).svg, /class="tools-rim" d="M[\d.,]+A118,118 0 0 1 /);
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

test('placing the fan: stays when clear, the nearest clear spot, shrinks on a short map, short pans, least overlap', () => {
  const bar = { l: 320, t: 120, r: 600, b: 330 };
  assert.deepEqual(T.placeFan(1280, 800, 400, 500, 120, [bar]), { x: 400, y: 500, r: 120 }, 'already clear');
  const moved = T.placeFan(610, 364, 450, 200, 120, [bar]);
  assert.deepEqual(moved, { x: 184, y: 200, r: 120 }, 'the NEAREST clear spot beside the bar on a short map');
  const tiny = T.placeFan(300, 200, 150, 100, 120, []);
  assert.deepEqual(tiny, { x: 150, y: 100, r: 70 }, 'a small map gets a smaller fan');
  const inside = T.placeFan(1280, 800, 1270, 790, 120, []);
  assert.ok(inside.x <= 1280 - 136 - 8 && inside.y <= 800 - 136 - 8, 'pulled back inside the map');
  // a mid-size window (683 x 657) with the bar and the overlay panel: a smaller fan nearby, not a long pan (G20 re-review)
  const obs = [{ l: 383, t: 60, r: 673, b: 400, hard: true }, { l: 10, t: 10, r: 300, b: 240 }];
  const far = T.placeFan(683, 657, 360, 100, 120, obs), near = T.placeFan(683, 657, 360, 100, 120, obs, { maxPan: 683 / 3 });
  assert.deepEqual(far, { x: 240, y: 376, r: 120 });
  assert.ok(near.r < 120 && Math.hypot(near.x - 360, near.y - 100) <= 683 / 3, 'within a third of the map: ' + JSON.stringify(near));
  assert.deepEqual(T.placeFan(683, 657, 360, 400, 120, obs, { maxPan: 228 }), T.placeFan(683, 657, 360, 400, 120, obs), 'the same when the full fan is near');
  // nothing clear: the least overlap, never centred under the tool bar (G20 re-review, small desktop windows)
  assert.deepEqual(T.placeFan(100, 100, 50, 50, 120, []), { x: 50, y: 50, r: 50, overlap: true }, 'a tiny map: centred');
  const hardBar = { l: 120, t: 50, r: 410, b: 290, hard: true };
  const fall = T.placeFan(420, 300, 250, 150, 90, [hardBar]);
  assert.ok(fall.overlap && fall.r === 50 && fall.x + 50 + 16 <= hardBar.l + 16, 'beside the bar: ' + JSON.stringify(fall));
  assert.deepEqual(T.placeFan(420, 300, 250, 150, 90, [Object.assign({}, hardBar, { hard: false })]).overlap, true);
  assert.equal(T.leastOverlap(0, 300, 10, 10, 50, [], 8), null, 'a map with no size');
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
  // a chunk that arrives but does not decode (a captive portal's HTML) is fetched again next time (G20 re-review)
  let portal = true; calls.length = 0;
  const cs2 = new T.CoastSource('https://c', async (url) => {
    calls.push(url);
    if (url.endsWith('/index.json')) return body(idx);
    if (url.endsWith('/world-i.bin')) return body(encodeCoast([[sq(10, 10, 11, 11)]], 30));
    if (portal) { portal = false; return body(new TextEncoder().encode('<html>sign in</html>').buffer); }
    return body(encodeCoast([[sq(-158.3, 21.25, -157.6, 21.7)]], 5));
  });
  await cs2.load();
  assert.equal(await cs2.near({ lat: 21.7, lng: -158 }), null, 'the bad body: tier 0 stands in');
  assert.equal(cs2.inflight.size, 0, 'nothing left in flight');
  const again = await cs2.near({ lat: 21.7, lng: -158 });
  assert.equal(again && again.length, 1, 'fetched again and decoded');
  assert.equal(calls.filter((c) => c.endsWith('20_-160.bin')).length, 2);
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
