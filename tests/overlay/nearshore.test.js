'use strict';
// Nearshore analysis (plan section 109): one picture per region and step as tools/model_frames/nearshore.py writes it (a
// header row, one 256x256 height plane per z14 tile, ten bytes per break-line vertex), the readout's words, the note
// beside the box and the layer's states (only the regions in view are asked for; a time the run does not hold clears
// the picture).
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
// two regions of the shore: the first holds tile (997, 7183), the second lies north-east of it
const R1 = { id: 'laniakea', box: [-158.12, 21.6, -158.06, 21.65], tiles: [[997, 7183]] };
const R2 = { id: 'sunset', box: [-158.05, 21.64, -158.01, 21.7], tiles: [[999, 7181]] };
const DOC = { format: 'nearshore-v2', run: '2026112612', set: 'ns-1', steps: [0, 1, 2, 3, 6], z: 14, regions: [R1, R2],
  box: [-158.12, 21.6, -158.01, 21.7], height_step_m: 0.05, vertex_bytes: 10 };
const RUN_MS = Date.UTC(2026, 10, 26, 12);
const HOME = [21.62, -158.088], BOTH = [21.645, -158.06];   // a view of the first region alone (z15); of both (z13)
// tile (997, 7183) at z14: its west edge and north edge in degrees
const TILE_W = 997 / 16384 * 360 - 180, TILE_N = Math.atan(Math.sinh(Math.PI * (1 - 2 * 7183 / 16384))) * 180 / Math.PI;
const PX = 360 / 16384 / 256;                               // one z14 pixel of longitude

// the picture nearshore.py writes: vertices = [{lon, lat, peel, fwd, right, mixed, breaker, band, first, ride_m, hb_m, score, share}]
function picture(verts, plane, o, region) {
  o = o || {}; region = region || R1;
  const rows = 1 + 256 + Math.ceil(verts.length * 10 / 256), q = new Uint8Array(rows * 256), b = region.box;
  q.set([78, 83, 72, 49, 1, 1, verts.length & 255, verts.length >> 8]);
  const tide = Math.round((o.tide === undefined ? -0.21 : o.tide) * 100) & 0xffff, hs = Math.round((o.hs || 4.02) * 100), tp = Math.round((o.tp || 16.3) * 10), dir = o.dir === undefined ? 335 : o.dir;
  q.set([tide & 255, tide >> 8, hs & 255, hs >> 8, tp & 255, tp >> 8, dir & 255, dir >> 8, 1, 3], 8);
  if (plane) q.set(plane, 256);
  verts.forEach((v, i) => {
    const at = 256 * 257 + i * 10, x = Math.round((v.lon - b[0]) / (b[2] - b[0]) * 65535), y = Math.round((v.lat - b[1]) / (b[3] - b[1]) * 65535);
    q.set([x & 255, x >> 8, y & 255, y >> 8, v.peel | (v.fwd ? 128 : 0),
      (v.right ? 1 : 0) | (v.mixed ? 2 : 0) | (v.breaker || 0) << 2 | (v.band || 0) << 4 | (v.first ? 128 : 0),
      Math.round((v.ride_m || 0) / 5), Math.round(v.hb_m / 0.05), v.score || 0, v.share === undefined ? 100 : v.share], at);
  });
  return { q, cols: 256, rows };
}
const V1 = { lon: -158.088, lat: 21.62, peel: 38, fwd: true, right: true, breaker: 1, band: 2, first: true, ride_m: 140, hb_m: 3.65, score: 71 };
const V2 = { lon: -158.0875, lat: 21.6204, peel: 9, fwd: true, right: false, breaker: 0, band: 0, ride_m: 0, hb_m: 3.2, share: 55, mixed: true };
const V3 = { lon: -158.04, lat: 21.66, peel: 55, fwd: false, right: false, breaker: 0, band: 3, first: true, ride_m: 60, hb_m: 2.0 };
const frameOf = (verts, plane, region) => { const p = picture(verts, plane, null, region); return I.parseNearFrame(p.q, p.cols, p.rows, region || R1); };

function fakeMap(z, c) {
  const world = 256 * Math.pow(2, z), s = Math.sin(c[0] * Math.PI / 180);
  const cx = (c[1] + 180) / 360 * world, cy = (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * world;
  const dLng = 700 / world * 360, dLat = 450 / world * 360 * Math.cos(c[0] * Math.PI / 180);
  return { getZoom: () => z, getSize: () => ({ x: 1400, y: 900 }), getPixelBounds: () => ({ min: { x: cx - 700, y: cy - 450 } }),
    getBounds: () => ({ getWest: () => c[1] - dLng, getEast: () => c[1] + dLng, getSouth: () => c[0] - dLat, getNorth: () => c[0] + dLat }),
    on() {}, off() {}, getPane() { return null; } };
}
function layerAt(z, c, unit) {
  const ly = new I.NearshoreLayer(fakeMap(z, c), 'https://frames.example', { getUnit: () => unit || 'US' });
  ly.doc = DOC; ly.runMs = RUN_MS; ly.state = 'ready';
  return ly;
}

test('the picture parses to its header, its plane and its vertices; a picture of another shape is refused', () => {
  const plane = new Uint8Array(65536); plane[10 * 256 + 20] = 73;
  const p = picture([V1, V2], plane), f = I.parseNearFrame(p.q, p.cols, p.rows, R1);
  assert.equal(f.n, 2); assert.equal(f.planes.length, 65536); assert.equal(f.planes[10 * 256 + 20], 73); assert.equal(f.region, R1);
  assert.ok(Math.abs(f.tide - -0.21) < 1e-9); assert.deepEqual(f.buoy, { hs: 4.02, tp: 16.3, dir: 335 });
  assert.ok(Math.abs(f.wx[0] - (V1.lon + 180) / 360) < 2e-9, 'world x of the first vertex');
  assert.ok(f.wy[0] > f.wy[1], 'the more northerly vertex is higher on the map');
  assert.equal(f.v[5] & 128, 128); assert.equal(f.v[15] & 128, 0);
  const g = frameOf([V3], null, R2);
  assert.ok(Math.abs(g.wx[0] - (V3.lon + 180) / 360) < 2e-9, 'a vertex is a share of its own region\'s box');
  assert.throws(() => I.parseNearFrame(p.q, 255, p.rows, R1), /not a nearshore frame/);
  const bad = picture([V1], plane); bad.q[0] = 0;
  assert.throws(() => I.parseNearFrame(bad.q, 256, bad.rows, R1), /not a nearshore frame/);
  assert.throws(() => I.parseNearFrame(p.q, 256, p.rows, Object.assign({}, R1, { tiles: [[1, 1], [2, 2]] })), /bad nearshore frame/);
  assert.throws(() => I.parseNearFrame(p.q.subarray(0, 256 * 257), 256, 257, R1), /bad nearshore frame/);
});

test('the readout names the nearest break-line vertex: height, sets, peel, band, side, ride, breaker; a closeout has no side or ride', () => {
  const ly = layerAt(15, HOME);
  ly.shown = [frameOf([V1, V2])];
  assert.equal(ly.readoutAt(V1.lat, V1.lon), 'Breaking face 12 ft (sets 16 ft) · Peel 38° ideal, right · Ride 140 m · plunging');
  assert.equal(ly.readoutAt(V2.lat, V2.lon), 'Breaking face 10 ft (sets 14 ft) · Peel 9° closeout · spilling · reform (broke further out) · mixed swell');
  assert.equal(ly.readoutAt(21.63, -158.07), null, 'nothing within reach and no zone under the cursor');
  assert.equal(ly.readoutAt(V1.lat, V1.lon).indexOf('\n'), -1, 'one line: no best-swell text (owner, 2026-11-26)');
  const m = layerAt(15, HOME, 'Metric');
  m.shown = [frameOf([V1])];
  assert.match(m.readoutAt(V1.lat, V1.lon), /^Breaking face 3\.7 m \(sets 4\.8 m\)/);
  const two = layerAt(13, BOTH);
  two.shown = [frameOf([V1, V2]), frameOf([V3], null, R2)];
  assert.equal(two.readoutAt(V3.lat, V3.lon), 'Breaking face 6.6 ft (sets 8.6 ft) · Peel 55° easy, left · Ride 60 m · spilling', 'a vertex of the second region');
  assert.match(two.readoutAt(V1.lat, V1.lon), /Peel (38° ideal|9° closeout)/, 'and of the first, under the same view');
});

test('off the line the readout is the breaking zone under the cursor; the top code reads as "or more"', () => {
  const plane = new Uint8Array(65536); plane[100 * 256 + 50] = 40; plane[100 * 256 + 51] = 255;
  const lat = (py) => { const n = Math.PI * (1 - 2 * (7183 + py / 256) / 16384); return Math.atan(Math.sinh(n)) * 180 / Math.PI; };
  const ly = layerAt(15, [lat(100.5), TILE_W + 50.5 * PX]);
  ly.shown = [frameOf([], plane)];
  assert.equal(ly.readoutAt(lat(100.5), TILE_W + 50.5 * PX), 'Breaking zone: face 6.6 ft (sets 8.6 ft)');
  assert.equal(ly.readoutAt(lat(100.5), TILE_W + 51.5 * PX), 'Breaking zone: face 42 ft+ (sets 55 ft)');
  assert.equal(ly.readoutAt(lat(101.5), TILE_W + 50.5 * PX), null);
  assert.ok(Math.abs(lat(0) - TILE_N) < 1e-9);
  // two regions may hold the same tile, each only the water it owns: the one with a height under the cursor answers
  const SHARE = { id: 'haleiwa', box: R1.box, tiles: [[996, 7183], [997, 7183]] }, mine = new Uint8Array(2 * 65536);
  mine[65536 + 101 * 256 + 50] = 60;
  const q2 = new Uint8Array(256 * (1 + 2 * 256)); q2.set([78, 83, 72, 49, 1, 2, 0, 0]); q2.set(mine, 256);
  ly.shown = [frameOf([], plane), I.parseNearFrame(q2, 256, 1 + 2 * 256, SHARE)];
  assert.equal(ly.readoutAt(lat(101.5), TILE_W + 50.5 * PX), 'Breaking zone: face 9.8 ft (sets 13 ft)', 'the neighbour owns this pixel');
  assert.equal(ly.readoutAt(lat(100.5), TILE_W + 50.5 * PX), 'Breaking zone: face 6.6 ft (sets 8.6 ft)');
});

test('the note says the buoy swell and the tide of the step shown, or why nothing is drawn', () => {
  const ly = layerAt(15, HOME);
  ly.shown = [frameOf([V1])]; ly.state = 'shown';
  assert.equal(ly.noteText(), 'Buoy swell 13.2 ft @ 16 s from 335° · tide −0.7 ft');
  ly.opts.getUnit = () => 'Metric';
  assert.equal(ly.noteText(), 'Buoy swell 4.0 m @ 16 s from 335° · tide −0.2 m');
  ly.state = 'error'; assert.equal(ly.noteText(), 'Nearshore data is unavailable');
  ly.state = 'fetching'; assert.equal(ly.noteText(), 'Loading …');
  const far = layerAt(7, HOME); far.state = 'ready';
  assert.equal(far.noteText(), 'Zoom in on Oahu’s North Shore');
  const away = layerAt(15, [34.0, -119.0]); assert.equal(away.inView(), false);
});

test('out of view nothing is asked for; a time the run does not hold clears the picture; a held step is shown at once', () => {
  let changes = 0; const asked = [];
  const ly = layerAt(15, HOME);
  ly._download = (step, k) => { asked.push(step + ':' + k); return new Promise(() => {}); }; ly.onChange = () => { changes++; };
  ly.setTime(RUN_MS + 2 * 3.6e6); assert.deepEqual(asked, ['2:0'], 'a step of the run, one region in view: one request');
  assert.equal(ly.state, 'fetching');
  ly.setTime(RUN_MS + 2 * 3.6e6); assert.deepEqual(asked, ['2:0'], 'the same step again: the request on its way is the one');
  ly.frames.set(ly._key(6, 0), frameOf([V1]));
  ly.setTime(RUN_MS + 6 * 3.6e6); assert.deepEqual(asked, ['2:0']); assert.equal(ly.shownStep, 6); assert.equal(ly.state, 'shown');
  ly.setTime(RUN_MS + 4 * 3.6e6); assert.equal(ly.state, 'nostep'); assert.equal(ly.shown, null, 'hour 4 is not a step of this run');
  ly.setTime(RUN_MS + 2.5 * 3.6e6); assert.equal(ly.state, 'nostep');
  ly.setTime(RUN_MS - 6 * 3.6e6); assert.equal(ly.state, 'nostep');
  const far = layerAt(8, HOME); far._download = (step, k) => { asked.push(step + ':' + k); return new Promise(() => {}); };
  far.setTime(RUN_MS + 2 * 3.6e6); assert.equal(far.state, 'away'); assert.deepEqual(asked, ['2:0'], 'zoomed out: no request');
  assert.ok(changes >= 3);
  assert.equal(I.NEAR_PREFIX, 'gfswave/nearshore/v1');
  assert.deepEqual(I.NEAR_BANDS.map((b) => b[0]), ['closeout', 'fast', 'ideal', 'easy', 'soft'], 'the five words of the key (owner, 2026-11-26)');
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  assert.ok(!/Hawaiian/.test(src) && !/Best here/.test(src) && !/atlas\.json/.test(src), 'no Hawaiian box, no best-swell line');
  assert.ok(/'Breaking zone: set-wave face height, on the scale above\.'/.test(src) && !/how the wave peels/.test(src), 'the key: one sentence, then the coloured words');
});

// the whole North Shore in regions (2026-11-27): only the regions in view are asked for and drawn
const tick = () => new Promise((r) => setImmediate(r));
function wired(ly) {
  const p = { 0: picture([V1]), 1: picture([V3], null, null, R2) }, open = new Map(), aborted = [], asked = [];
  ly._download = (step, k, signal) => new Promise((res, rej) => {
    const key = step + ':' + k; asked.push(key); open.set(key, () => res({ q: p[k].q, cols: 256, rows: p[k].rows }));
    if (signal) signal.addEventListener('abort', () => { aborted.push(key); open.delete(key); const e = new Error('aborted'); e.name = 'AbortError'; rej(e); });
  });
  return { asked, aborted, open, land: async (key) => { open.get(key)(); open.delete(key); await tick(); } };
}
test('regions: the ones in view are asked for, what is in hand is drawn while the rest loads, a pan brings the neighbour', async () => {
  const ly = layerAt(13, BOTH), w = wired(ly);
  assert.deepEqual(ly.regionsInView(), [0, 1]);
  ly.setTime(RUN_MS + 2 * 3.6e6); assert.deepEqual(w.asked, ['2:0', '2:1'], 'one picture per region in view');
  await w.land('2:1');
  assert.equal(ly.state, 'fetching'); assert.equal(ly.shown.length, 1); assert.equal(ly.shown[0].region, R2, 'the region in hand is on the map');
  assert.equal(ly.noteText(), 'Loading …');
  await w.land('2:0');
  assert.equal(ly.state, 'shown'); assert.deepEqual(ly.shown.map((f) => f.region.id), ['laniakea', 'sunset']);
  // zoom in on the first region: the second leaves the map and gives up its drawn tiles, nothing new is asked for
  const second = ly.shown[1]; second.tiles = [{}];
  ly.map = fakeMap(15, HOME); assert.deepEqual(ly.regionsInView(), [0]);
  ly._moved();
  assert.deepEqual(ly.shown.map((f) => f.region.id), ['laniakea']); assert.equal(second.tiles, null); assert.equal(ly.state, 'shown');
  assert.deepEqual(w.asked, ['2:0', '2:1']);
  // and back: both are in hand
  ly.map = fakeMap(13, BOTH); ly._moved();
  assert.equal(ly.shown.length, 2); assert.deepEqual(w.asked, ['2:0', '2:1']);
  // a view of the second region alone at another step asks for that one picture
  ly.map = fakeMap(15, [21.68, -158.02]); assert.deepEqual(ly.regionsInView(), [1]);
  ly.setTime(RUN_MS + 6 * 3.6e6); assert.deepEqual(w.asked, ['2:0', '2:1', '6:1']); assert.equal(ly.shown, null);
  await w.land('6:1'); assert.equal(ly.shownStep, 6); assert.deepEqual(ly.shown.map((f) => f.region.id), ['sunset']);
  assert.equal(ly.map.getZoom() >= 12, true);
  const low = layerAt(11.9, BOTH); assert.deepEqual(low.regionsInView(), [], 'under zoom 12 nothing is in view');
});

test('the pictures kept are bounded, and the ones on the map are never the ones dropped', () => {
  const ly = layerAt(15, HOME), first = frameOf([V1]);
  ly._keep(ly._key(0, 0), first); ly.shown = [first]; ly.shownStep = 0;
  for (let s = 1; s <= 60; s++) ly._keep(ly._key(s, 0), frameOf([V1]));
  assert.equal(ly.frames.size, 36); assert.ok(ly.frames.has(ly._key(0, 0)), 'the picture on the map stays');
  assert.ok(ly.frames.has(ly._key(60, 0)) && !ly.frames.has(ly._key(1, 0)), 'the oldest other one went');
});

// review round 1, F2: playback
test('playback: the step on its way is never dropped for the next one, the steps ahead are asked for', async () => {
  const DOC8 = Object.assign({}, DOC, { steps: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] });
  const ly = layerAt(15, HOME); ly.doc = DOC8;
  const w = wired(ly);
  ly.setTime(RUN_MS); assert.deepEqual(w.asked, ['0:0'], 'the first step alone: nothing says the map is playing');
  ly.setTime(RUN_MS + 3.6e6); assert.deepEqual(w.asked, ['0:0', '1:0', '2:0', '3:0'], 'one step on: its picture and the next two');
  assert.deepEqual(w.aborted, [], 'the step before stays on its way');
  await w.land('1:0'); assert.equal(ly.shownStep, 1); assert.equal(ly.state, 'shown');
  ly.setTime(RUN_MS + 2 * 3.6e6); assert.equal(ly.shown, null, 'step 2 is not in hand yet: nothing of step 1 under its time');
  assert.deepEqual(w.asked, ['0:0', '1:0', '2:0', '3:0', '4:0'], 'steps 2 and 3 are on their way already; step 4 is asked for');
  await w.land('2:0'); assert.equal(ly.shownStep, 2, 'it lands and is shown');
  await w.land('3:0'); assert.equal(ly.shownStep, 2); assert.ok(ly.frames.has(ly._key(3, 0)), 'a picture ahead of the map is kept, not shown');
  ly.setTime(RUN_MS + 3 * 3.6e6); assert.equal(ly.shownStep, 3, 'in hand when its turn comes: shown at once');
  ly.detach(); assert.equal(ly.flights.size, 0, 'detached: every request is dropped');
});

test('twelve requests at most: a jump with every place taken puts the step on the map in place of the oldest others', async () => {
  const DOC8 = Object.assign({}, DOC, { steps: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] });
  const ly = layerAt(13, BOTH); ly.doc = DOC8;
  const w = wired(ly);
  for (let s = 0; s <= 4; s++) ly.setTime(RUN_MS + s * 3.6e6);   // two regions, each step and the two ahead: steps 0-5 on their way
  assert.equal(ly.flights.size, 12); assert.equal(w.asked.length, 12); assert.ok(!w.asked.includes('6:0'), 'a step ahead waits for a free place');
  ly.setTime(RUN_MS + 9 * 3.6e6);
  assert.equal(ly.flights.size, 12); assert.ok(ly.flights.has(ly._key(9, 0)) && ly.flights.has(ly._key(9, 1)));
  assert.deepEqual(w.aborted, ['0:0', '0:1'], 'the two oldest requests for another step gave way');
  await w.land('9:0'); await w.land('9:1'); assert.equal(ly.shownStep, 9); assert.equal(ly.state, 'shown');
  ly.detach();
});

test('a step that cannot be had: the note says so; ahead of the map it is simply asked again at its turn', async () => {
  const ly = layerAt(15, HOME); let loads = 0;
  ly.load = () => { loads++; }; ly.loadedAt = Date.now();
  const fail = new Map(); ly._download = (step) => new Promise((res, rej) => fail.set(step, rej));
  ly.setTime(RUN_MS); ly.setTime(RUN_MS + 3.6e6);                   // step 1 wanted, 2 and 3 ahead
  fail.get(2)(new Error('frame 404')); await tick(); assert.equal(ly.state, 'fetching', 'a miss ahead of the map says nothing');
  fail.get(1)(new Error('frame 404')); await tick(); assert.equal(ly.state, 'error'); assert.equal(loads, 0, 'the pointer was read under a minute ago');
  ly.loadedAt = Date.now() - 61000; ly.setTime(RUN_MS + 2 * 3.6e6);
  fail.get(2)(new Error('frame 404')); await tick(); assert.equal(loads, 1, 'a minute on, the pointer is read again: the run may have been replaced');
});

// review round 1, F3: the map's copies of the world
test('on another copy of the world (the view came round from the west) the stretch is in view, read and placed', () => {
  const ly = layerAt(15, HOME);
  const m = fakeMap(15, [21.62, -158.088 + 360]); m.getCenter = () => ({ lat: 21.62, lng: -158.088 + 360 });
  ly.map = m; ly.shown = [frameOf([V1])];
  assert.equal(ly._wrapDeg(), 360); assert.equal(ly.inView(), true); assert.deepEqual(ly.regionsInView(), [0]);
  assert.match(ly.readoutAt(V1.lat, V1.lon + 360), /^Breaking face 12 ft/);
  const home = layerAt(15, HOME); home.map.getCenter = () => ({ lat: 21.62, lng: -158.088 });
  assert.equal(home._wrapDeg(), 0);
  const west = layerAt(15, HOME); west.map = fakeMap(15, [21.62, -158.088 - 720]); west.map.getCenter = () => ({ lat: 21.62, lng: -158.088 - 720 });
  assert.equal(west._wrapDeg(), -720); assert.equal(west.inView(), true);
});

// review round 1, F6: the index is checked field by field
test('an index the layer cannot use is refused whole', () => {
  assert.equal(I.validNearDoc(DOC), true);
  const bad = (patch) => I.validNearDoc(Object.assign({}, DOC, patch));
  const reg = (patch) => bad({ regions: [Object.assign({}, R1, patch)] });
  assert.equal(reg({}), true, 'one region is a whole index');
  assert.equal(reg({ tiles: [null] }), false); assert.equal(reg({ tiles: [[997]] }), false); assert.equal(reg({ tiles: [[997, 7183.5]] }), false);
  assert.equal(reg({ tiles: [] }), false); assert.equal(reg({ tiles: new Array(64).fill([1, 1]) }), false);
  assert.equal(reg({ box: [1, 2, 3] }), false); assert.equal(reg({ box: [-158.06, 21.6, -158.12, 21.65] }), false); assert.equal(reg({ box: [NaN, 21.6, -158.06, 21.65] }), false);
  for (const id of ['', 'A', '../x', 'a/b', 'a.b', 'x', 'a'.repeat(22), 7]) assert.equal(reg({ id }), false, 'region id ' + id);
  assert.equal(bad({ regions: [] }), false); assert.equal(bad({ regions: [R1, null] }), false); assert.equal(bad({ regions: new Array(17).fill(R1) }), false);
  assert.equal(bad({ regions: undefined }), false);
  assert.equal(bad({ steps: [0, '1'] }), false); assert.equal(bad({ steps: [] }), false); assert.equal(bad({ steps: [-1] }), false);
  assert.equal(bad({ run: '20261126' }), false); assert.equal(bad({ z: 15 }), false); assert.equal(bad({ vertex_bytes: 8 }), false);
  assert.equal(bad({ height_step_m: 0 }), false); assert.equal(bad({ format: 'nearshore-v1' }), false, 'the single-stretch index of the pilot');
  assert.equal(I.validNearDoc(null), false); assert.equal(I.validNearDoc('x'), false);
});

// back-end review round 1, B1: the pointer names <run>/<table set>/index.json; each region's frames sit in its folder beside it
test('the layer follows the pointer: the frames are read from the region folders beside the index; a pointer of another shape is refused', async () => {
  const calls = [], real = global.fetch, RAW = Object.assign({}, DOC); delete RAW.box;
  const answer = (body) => Promise.resolve({ ok: true, json: () => Promise.resolve(JSON.parse(JSON.stringify(body))) });
  try {
    global.fetch = (url) => { calls.push(url); return /latest\.json$/.test(url) ? answer({ run: DOC.run, set: 'ns-1', index: 'gfswave/nearshore/v1/2026112612/ns-1/index.json' }) : answer(RAW); };
    const ly = new I.NearshoreLayer(fakeMap(15, HOME), 'https://frames.example', { getUnit: () => 'US' });
    await ly.load();
    assert.deepEqual(calls, ['https://frames.example/gfswave/nearshore/v1/latest.json', 'https://frames.example/gfswave/nearshore/v1/2026112612/ns-1/index.json']);
    assert.equal(ly.base, 'https://frames.example/gfswave/nearshore/v1/2026112612/ns-1/'); assert.equal(ly.state, 'ready');
    assert.deepEqual(ly.doc.box, [-158.12, 21.6, -158.01, 21.7], 'the whole product\'s box is the regions\' boxes together');
    const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
    assert.ok(src.includes("decodeFrame(this.base + this.doc.regions[k].id + '/f' + pad3(step) + '.png'"), 'a frame is read from its region\'s folder beside the index');
    for (const index of ['gfswave/nearshore/v1/2026112612/index.json', 'gfswave/nearshore/v1/2026112612/../../x/index.json', 'https://evil.example/index.json', 'gfswave/nearshore/v1/20261126/ns-1/index.json']) {
      const bad = new I.NearshoreLayer(fakeMap(15, HOME), 'https://frames.example', {});
      global.fetch = (url) => (/latest\.json$/.test(url) ? answer({ run: DOC.run, index }) : answer(RAW));
      const warn = console.warn; console.warn = () => {};
      try { await bad.load(); } finally { console.warn = warn; }
      assert.equal(bad.state, 'error', index); assert.equal(bad.doc, null);
    }
    // the pilot's single-stretch index (before the regions): unavailable, never half-read
    const old = new I.NearshoreLayer(fakeMap(15, HOME), 'https://frames.example', {});
    global.fetch = (url) => (/latest\.json$/.test(url) ? answer({ run: DOC.run, index: 'gfswave/nearshore/v1/2026112612/pilot-1/index.json' })
      : answer({ format: 'nearshore-v1', run: DOC.run, steps: [0], z: 14, tiles: R1.tiles, box: R1.box, height_step_m: 0.05, vertex_bytes: 10 }));
    const warn = console.warn; console.warn = () => {};
    try { await old.load(); } finally { console.warn = warn; }
    assert.equal(old.state, 'error'); assert.equal(old.noteText(), 'Nearshore data is unavailable');
    // the same run from another table set: the old pictures go with the old folder
    ly.frames.set(ly._key(2, 0), {}); ly.shownStep = 2;
    global.fetch = (url) => (/latest\.json$/.test(url) ? answer({ run: DOC.run, index: 'gfswave/nearshore/v1/2026112612/ns-2/index.json' }) : answer(RAW));
    await ly.load();
    assert.equal(ly.base, 'https://frames.example/gfswave/nearshore/v1/2026112612/ns-2/'); assert.equal(ly.frames.size, 0); assert.equal(ly.shownStep, null);
  } finally { global.fetch = real; }
});
