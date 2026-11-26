'use strict';
// Nearshore analysis (plan section 109): the step picture's layout as tools/model_frames/nearshore.py writes it (a header
// row, one 256x256 height plane per z14 tile, ten bytes per break-line vertex), the readout's words, the note beside the
// box and the layer's states (out of view nothing is asked for; a time the run does not hold clears the picture).
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
const DOC = { format: 'nearshore-v1', run: '2026112612', steps: [0, 1, 2, 3, 6], z: 14, tiles: [[997, 7183]], box: [-158.12, 21.6, -158.06, 21.65],
  height_step_m: 0.05, vertex_bytes: 10, atlas: 'static/nearshore/t1/atlas.json' };
const RUN_MS = Date.UTC(2026, 10, 26, 12);
// tile (997, 7183) at z14: its west edge and north edge in degrees
const TILE_W = 997 / 16384 * 360 - 180, TILE_N = Math.atan(Math.sinh(Math.PI * (1 - 2 * 7183 / 16384))) * 180 / Math.PI;
const PX = 360 / 16384 / 256;                               // one z14 pixel of longitude

// the picture nearshore.py writes: vertices = [{lon, lat, peel, fwd, right, mixed, breaker, band, first, ride_m, hb_m, score, share}]
function picture(verts, plane, o) {
  o = o || {};
  const rows = 1 + 256 + Math.ceil(verts.length * 10 / 256), q = new Uint8Array(rows * 256);
  q.set([78, 83, 72, 49, 1, 1, verts.length & 255, verts.length >> 8]);
  const tide = Math.round((o.tide === undefined ? -0.21 : o.tide) * 100) & 0xffff, hs = Math.round((o.hs || 4.02) * 100), tp = Math.round((o.tp || 16.3) * 10), dir = o.dir === undefined ? 335 : o.dir;
  q.set([tide & 255, tide >> 8, hs & 255, hs >> 8, tp & 255, tp >> 8, dir & 255, dir >> 8, 1, 3], 8);
  if (plane) q.set(plane, 256);
  verts.forEach((v, i) => {
    const at = 256 * 257 + i * 10, x = Math.round((v.lon - DOC.box[0]) / (DOC.box[2] - DOC.box[0]) * 65535), y = Math.round((v.lat - DOC.box[1]) / (DOC.box[3] - DOC.box[1]) * 65535);
    q.set([x & 255, x >> 8, y & 255, y >> 8, v.peel | (v.fwd ? 128 : 0),
      (v.right ? 1 : 0) | (v.mixed ? 2 : 0) | (v.breaker || 0) << 2 | (v.band || 0) << 4 | (v.first ? 128 : 0),
      Math.round((v.ride_m || 0) / 5), Math.round(v.hb_m / 0.05), v.score || 0, v.share === undefined ? 100 : v.share], at);
  });
  return { q, cols: 256, rows };
}
const V1 = { lon: -158.088, lat: 21.62, peel: 38, fwd: true, right: true, breaker: 1, band: 2, first: true, ride_m: 140, hb_m: 3.65, score: 71 };
const V2 = { lon: -158.0875, lat: 21.6204, peel: 9, fwd: true, right: false, breaker: 0, band: 0, ride_m: 0, hb_m: 3.2, share: 55, mixed: true };

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
  const p = picture([V1, V2], plane), f = I.parseNearFrame(p.q, p.cols, p.rows, DOC);
  assert.equal(f.n, 2); assert.equal(f.planes.length, 65536); assert.equal(f.planes[10 * 256 + 20], 73);
  assert.ok(Math.abs(f.tide - -0.21) < 1e-9); assert.deepEqual(f.buoy, { hs: 4.02, tp: 16.3, dir: 335 });
  assert.ok(Math.abs(f.wx[0] - (V1.lon + 180) / 360) < 2e-9, 'world x of the first vertex');
  assert.ok(f.wy[0] > f.wy[1], 'the more northerly vertex is higher on the map');
  assert.equal(f.v[5] & 128, 128); assert.equal(f.v[15] & 128, 0);
  assert.throws(() => I.parseNearFrame(p.q, 255, p.rows, DOC), /not a nearshore frame/);
  const bad = picture([V1], plane); bad.q[0] = 0;
  assert.throws(() => I.parseNearFrame(bad.q, 256, bad.rows, DOC), /not a nearshore frame/);
  assert.throws(() => I.parseNearFrame(p.q, 256, p.rows, Object.assign({}, DOC, { tiles: [[1, 1], [2, 2]] })), /bad nearshore frame/);
  assert.throws(() => I.parseNearFrame(p.q.subarray(0, 256 * 257), 256, 257, DOC), /bad nearshore frame/);
});

test('the readout names the nearest break-line vertex: height, sets, peel, band, side, ride, breaker; a closeout has no side or ride', () => {
  const ly = layerAt(15, [21.62, -158.088]), p = picture([V1, V2]);
  ly.shown = I.parseNearFrame(p.q, p.cols, p.rows, DOC);
  assert.equal(ly.readoutAt(V1.lat, V1.lon), 'Breaking face 12 ft (sets 16 ft) · Peel 38° ideal, right · Ride 140 m · plunging');
  assert.equal(ly.readoutAt(V2.lat, V2.lon), 'Breaking face 10 ft (sets 14 ft) · Peel 9° closeout · spilling · reform (broke further out) · mixed swell');
  assert.equal(ly.readoutAt(21.63, -158.07), null, 'nothing within reach and no zone under the cursor');
  assert.equal(ly.readoutAt(V1.lat, V1.lon).indexOf('\n'), -1, 'one line: no best-swell text (owner, 2026-11-26)');
  const m = layerAt(15, [21.62, -158.088], 'Metric'), pm = picture([V1]);
  m.shown = I.parseNearFrame(pm.q, pm.cols, pm.rows, DOC);
  assert.match(m.readoutAt(V1.lat, V1.lon), /^Breaking face 3\.7 m \(sets 4\.8 m\)/);
});

test('off the line the readout is the breaking zone under the cursor; the top code reads as "or more"', () => {
  const plane = new Uint8Array(65536); plane[100 * 256 + 50] = 40; plane[100 * 256 + 51] = 255;
  const p = picture([], plane), lat = (py) => { const n = Math.PI * (1 - 2 * (7183 + py / 256) / 16384); return Math.atan(Math.sinh(n)) * 180 / Math.PI; };
  const ly = layerAt(15, [lat(100.5), TILE_W + 50.5 * PX]);
  ly.shown = I.parseNearFrame(p.q, p.cols, p.rows, DOC);
  assert.equal(ly.readoutAt(lat(100.5), TILE_W + 50.5 * PX), 'Breaking zone: face 6.6 ft (sets 8.6 ft)');
  assert.equal(ly.readoutAt(lat(100.5), TILE_W + 51.5 * PX), 'Breaking zone: face 42 ft+ (sets 55 ft)');
  assert.equal(ly.readoutAt(lat(101.5), TILE_W + 50.5 * PX), null);
  assert.ok(Math.abs(lat(0) - TILE_N) < 1e-9);
});

test('the note says the buoy swell and the tide of the step shown, or why nothing is drawn', () => {
  const ly = layerAt(15, [21.62, -158.088]), p = picture([V1]);
  ly.shown = I.parseNearFrame(p.q, p.cols, p.rows, DOC); ly.state = 'shown';
  assert.equal(ly.noteText(), 'Buoy swell 13.2 ft @ 16 s from 335° · tide −0.7 ft');
  ly.opts.getUnit = () => 'Metric';
  assert.equal(ly.noteText(), 'Buoy swell 4.0 m @ 16 s from 335° · tide −0.2 m');
  ly.state = 'error'; assert.equal(ly.noteText(), 'Nearshore data is unavailable');
  ly.state = 'fetching'; assert.equal(ly.noteText(), 'Loading …');
  const far = layerAt(7, [21.62, -158.088]); far.state = 'ready';
  assert.equal(far.noteText(), 'Zoom in on Oahu’s North Shore (Laniakea)');
  const away = layerAt(15, [34.0, -119.0]); assert.equal(away.inView(), false);
});

test('out of view nothing is asked for; a time the run does not hold clears the picture; a held step is shown at once', () => {
  let changes = 0; const asked = [];
  const ly = layerAt(15, [21.62, -158.088]), p = picture([V1]);
  ly._download = (step) => { asked.push(step); return new Promise(() => {}); }; ly.onChange = () => { changes++; };
  ly.setTime(RUN_MS + 2 * 3.6e6); assert.deepEqual(asked, [2], 'a step of the run, in view: one request');
  assert.equal(ly.state, 'fetching');
  ly.setTime(RUN_MS + 2 * 3.6e6); assert.deepEqual(asked, [2], 'the same step again: the request on its way is the one');
  ly.frames.set(6, I.parseNearFrame(p.q, p.cols, p.rows, DOC));
  ly.setTime(RUN_MS + 6 * 3.6e6); assert.deepEqual(asked, [2]); assert.equal(ly.shownStep, 6); assert.equal(ly.state, 'shown');
  ly.setTime(RUN_MS + 4 * 3.6e6); assert.equal(ly.state, 'nostep'); assert.equal(ly.shown, null, 'hour 4 is not a step of this run');
  ly.setTime(RUN_MS + 2.5 * 3.6e6); assert.equal(ly.state, 'nostep');
  ly.setTime(RUN_MS - 6 * 3.6e6); assert.equal(ly.state, 'nostep');
  const far = layerAt(8, [21.62, -158.088]); far._download = (step) => { asked.push(step); return new Promise(() => {}); };
  far.setTime(RUN_MS + 2 * 3.6e6); assert.equal(far.state, 'away'); assert.deepEqual(asked, [2], 'zoomed out: no request');
  assert.ok(changes >= 3);
  assert.equal(I.NEAR_PREFIX, 'gfswave/nearshore/v1');
  assert.deepEqual(I.NEAR_BANDS.map((b) => b[0]), ['closeout', 'fast', 'ideal', 'easy', 'soft'], 'the five words of the key (owner, 2026-11-26)');
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  assert.ok(!/Hawaiian/.test(src) && !/Best here/.test(src) && !/atlas\.json/.test(src), 'no Hawaiian box, no best-swell line');
  assert.ok(/'Breaking zone: set-wave face height, on the scale above\.'/.test(src) && !/how the wave peels/.test(src), 'the key: one sentence, then the coloured words');
});

// review round 1, F2: playback
const tick = () => new Promise((r) => setImmediate(r));
test('playback: the step on its way is never dropped for the next one, the steps ahead are asked for, three requests at most', async () => {
  const DOC8 = Object.assign({}, DOC, { steps: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] });
  const ly = layerAt(15, [21.62, -158.088]); ly.doc = DOC8;
  const p = picture([V1]), open = new Map(), aborted = [], asked = [];
  ly._download = (step, signal) => new Promise((res, rej) => {
    asked.push(step); open.set(step, res);
    if (signal) signal.addEventListener('abort', () => { aborted.push(step); open.delete(step); const e = new Error('aborted'); e.name = 'AbortError'; rej(e); });
  });
  const land = async (step) => { open.get(step)({ q: p.q, cols: p.cols, rows: p.rows }); open.delete(step); await tick(); };
  ly.setTime(RUN_MS); assert.deepEqual(asked, [0], 'the first step alone: nothing says the map is playing');
  ly.setTime(RUN_MS + 3.6e6); assert.deepEqual(asked, [0, 1, 2], 'one step on: its picture and the next ones, three requests at most');
  assert.deepEqual(aborted, [], 'the step before stays on its way');
  await land(1); assert.equal(ly.shownStep, 1); assert.equal(ly.state, 'shown');
  ly.setTime(RUN_MS + 2 * 3.6e6); assert.equal(ly.shown, null, 'step 2 is not in hand yet: nothing of step 1 under its time');
  assert.deepEqual(asked, [0, 1, 2, 3], 'step 2 is on its way already; step 3 takes the free place');
  await land(2); assert.equal(ly.shownStep, 2, 'it lands and is shown');
  await land(3); assert.equal(ly.shownStep, 2); assert.ok(ly.frames.has(3), 'a picture ahead of the map is kept, not shown');
  ly.setTime(RUN_MS + 3 * 3.6e6); assert.equal(ly.shownStep, 3, 'in hand when its turn comes: shown at once');
  ly.setTime(RUN_MS + 9 * 3.6e6);                                   // a jump with every place taken: the oldest other request gives way
  assert.equal(ly.flights.size, 3); assert.ok(ly.flights.has(9)); assert.equal(aborted.length, 1);
  await land(9); assert.equal(ly.shownStep, 9);
  ly.detach(); assert.equal(ly.flights.size, 0, 'detached: every request is dropped');
});

test('a step that cannot be had: the note says so; ahead of the map it is simply asked again at its turn', async () => {
  const ly = layerAt(15, [21.62, -158.088]); let loads = 0;
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
  const ly = layerAt(15, [21.62, -158.088]), p = picture([V1]);
  const m = fakeMap(15, [21.62, -158.088 + 360]); m.getCenter = () => ({ lat: 21.62, lng: -158.088 + 360 });
  ly.map = m; ly.shown = I.parseNearFrame(p.q, p.cols, p.rows, DOC);
  assert.equal(ly._wrapDeg(), 360); assert.equal(ly.inView(), true);
  assert.match(ly.readoutAt(V1.lat, V1.lon + 360), /^Breaking face 12 ft/);
  const home = layerAt(15, [21.62, -158.088]); home.map.getCenter = () => ({ lat: 21.62, lng: -158.088 });
  assert.equal(home._wrapDeg(), 0);
  const west = layerAt(15, [21.62, -158.088]); west.map = fakeMap(15, [21.62, -158.088 - 720]); west.map.getCenter = () => ({ lat: 21.62, lng: -158.088 - 720 });
  assert.equal(west._wrapDeg(), -720); assert.equal(west.inView(), true);
});

// review round 1, F6: the index is checked field by field
test('an index the layer cannot use is refused whole', () => {
  assert.equal(I.validNearDoc(DOC), true);
  const bad = (patch) => I.validNearDoc(Object.assign({}, DOC, patch));
  assert.equal(bad({ tiles: [null] }), false); assert.equal(bad({ tiles: [[997]] }), false); assert.equal(bad({ tiles: [[997, 7183.5]] }), false);
  assert.equal(bad({ tiles: [] }), false); assert.equal(bad({ tiles: new Array(64).fill([1, 1]) }), false);
  assert.equal(bad({ steps: [0, '1'] }), false); assert.equal(bad({ steps: [] }), false); assert.equal(bad({ steps: [-1] }), false);
  assert.equal(bad({ box: [1, 2, 3] }), false); assert.equal(bad({ box: [-158.06, 21.6, -158.12, 21.65] }), false); assert.equal(bad({ box: [NaN, 21.6, -158.06, 21.65] }), false);
  assert.equal(bad({ run: '20261126' }), false); assert.equal(bad({ z: 15 }), false); assert.equal(bad({ vertex_bytes: 8 }), false);
  assert.equal(bad({ height_step_m: 0 }), false); assert.equal(bad({ format: 'nearshore-v2' }), false);
  assert.equal(I.validNearDoc(null), false); assert.equal(I.validNearDoc('x'), false);
});
