'use strict';
// The radar layer (plan section 85): the format, the store, the sampler and its one invariant (the readout is the drawn
// pixel), the loop player, and the overlay's obs path. Real frames: tests/fixtures/radar_client (tests/capture_radar_client_fixtures.py).
const test = require('node:test');
const assert = require('node:assert/strict');
const { world, fixtureBytes, fixtureJson, settle, until, pointerDoc, slotName, frame, BOXES } = require('./radar_world');

const POINTER = fixtureJson('pointer.json'), EXPECT = fixtureJson('expect.json'), TABLES = fixtureJson('cell_tables.json');
const clone = (o) => JSON.parse(JSON.stringify(o));
const T = world(), I = T.I;
const ROOT = 'https://x';

// An independent reference of the job's geometry: a tile pixel's centre, then the bilinear rule over a picture.
function pixelCentre(x, y, z, px, py) {
  const n = 256 * Math.pow(2, z), wx = ((x % Math.pow(2, z)) + Math.pow(2, z)) % Math.pow(2, z);
  return { lat: Math.atan(Math.sinh(Math.PI - 2 * Math.PI * (y * 256 + py + 0.5) / n)) * 180 / Math.PI, lon: (wx * 256 + px + 0.5) / n * 360 - 180 };
}
function bilinear(pic, fyFile, fxFile) {
  // The arguments are file positions (the job's geometry); a cropped picture starts at its r0, c0.
  const fy = fyFile - (pic.r0 || 0), fx = fxFile - (pic.c0 || 0);
  const r0 = Math.floor(fy), c0 = Math.floor(fx), wy = fy - r0, wx = fx - c0;
  let w = 0, acc = 0;
  for (const [r, c, f] of [[r0, c0, (1 - wx) * (1 - wy)], [r0, c0 + 1, wx * (1 - wy)], [r0 + 1, c0, (1 - wx) * wy], [r0 + 1, c0 + 1, wx * wy]]) {
    if (r < 0 || c < 0 || r >= pic.rows || c >= pic.cols) continue;
    const v = pic.q[r * pic.cols + c];
    if (v >= 2) { w += f; acc += f * v; }
  }
  return w < 0.25 ? 0 : acc / w;
}
// One store with one pointer, and a helper to land pictures without the network.
function storeWith(w, doc) {
  const st = new w.I.RadarStore(ROOT); st._take(doc);
  st.landed = (slot, region, tier, name, fr) => st._land({ key: st.key(slot, region, tier, name), slot, region, tier }, w.I.radarPic(fr, tier !== 't0'));
  return st;
}
function layerWith(w, st, idx, playing) {
  const layer = new w.I.RadarLayer(); layer.addTo(w.map); layer.store = st; layer.idx = idx; layer.playing = !!playing;
  return layer;
}
function tile(layer, x, y, z) {
  const el = layer.createTile({ x, y, z }, () => {});
  layer._tiles[x + ':' + y + ':' + z] = { el, coords: { x, y, z } };
  return el;
}
function tileAt(layer, lat, lon, z) {
  const n = Math.pow(2, z), s = Math.sin(lat * Math.PI / 180);
  const x = Math.floor((lon + 180) / 360 * n), y = Math.floor((0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * n);
  return layer._tiles[x + ':' + y + ':' + z] ? layer._tiles[x + ':' + y + ':' + z].el : tile(layer, x, y, z);
}
const t0Of = (region, value) => { const b = BOXES[region]; return frame((b[3] - b[1]) * 10, (b[2] - b[0]) * 10, value); };

// ------------------------------------------------------------------ the format
test('validRadarPointer takes the real pointer and refuses what the client cannot read', () => {
  assert.equal(I.validRadarPointer(POINTER), true);
  const bad = {
    format: (p) => { p.format = 'radar-v2'; }, floor: (p) => { p.floor_dbz = 0; }, step: (p) => { p.step_dbz = 1; }, code: (p) => { p.max_code = 255; },
    t1ppd: (p) => { p.tiers.t1.ppd = 50; }, t2cell: (p) => { p.tiers.t2.cell = 10; }, noRegions: (p) => { p.regions = {}; }, regionsList: (p) => { p.regions = []; },
    upper: (p) => { p.regions.HAWAII = p.regions.hawaii; }, proto: (p) => { Object.defineProperty(p.regions, '__proto__', { value: p.regions.hawaii, enumerable: true }); },
    floatBox: (p) => { p.regions.hawaii.box = [15.5, -164, 26, -151]; }, inverted: (p) => { p.regions.hawaii.box = [26, -164, 15, -151]; }, world: (p) => { p.regions.hawaii.box = [15, -200, 26, -151]; },
    noName: (p) => { delete p.regions.hawaii.name; }, slotText: (p) => { p.slots[0].t = 'yesterday'; }, slotOrder: (p) => { p.slots.reverse(); }, slotDup: (p) => { p.slots[1].t = p.slots[0].t; },
    unknownRegion: (p) => { p.slots[0].r.mars = { o: 0 }; }, offset: (p) => { p.slots[0].r.hawaii.o = 60; }, offsetFloat: (p) => { p.slots[0].r.hawaii.o = 1.5; }, hex: (p) => { p.slots[0].r.hawaii.t1 = 'xyz'; }, hexUpper: (p) => { p.slots[0].r.hawaii.t2 = 'AB'; }
  };
  for (const k of Object.keys(bad)) { const p = clone(POINTER); bad[k](p); assert.equal(I.validRadarPointer(p), false, k); }
  assert.equal(I.validRadarPointer(null), false); assert.equal(I.validRadarPointer([]), false);
});
test('the cell tables and bitsets are the job\'s', () => {
  for (const region of Object.keys(TABLES)) {
    const b = POINTER.regions[region].box;
    assert.deepEqual(I.radarCellTable(b, 20).cells.map((c) => c.name), TABLES[region].t1, region + ' t1');
    assert.deepEqual(I.radarCellTable(b, 5).cells.map((c) => c.name), TABLES[region].t2, region + ' t2');
  }
  const rec = POINTER.slots.find((s) => s.t === EXPECT.slot), st = new I.RadarStore(ROOT); st._take(POINTER);
  for (const tier of ['t1', 't2']) {
    const got = TABLES.hawaii[tier].filter((nm) => st.listed(rec, 'hawaii', tier, nm));
    assert.deepEqual(got, EXPECT[tier].listed, tier);
  }
  assert.equal(I.radarBit('a', 0), false); assert.equal(I.radarBit('a', 1), true); assert.equal(I.radarBit('a', 3), true); assert.equal(I.radarBit('41', 4), true); assert.equal(I.radarBit('41', 2), true); assert.equal(I.radarBit('41', 40), false, 'past the end = false');
});
test('radarLoopFrames: the owner\'s four loops by the clock, plus the newest frame', () => {
  const slots = (endIso) => pointerDoc(endIso, 145).slots;
  const spacing = (fr) => fr.slice(1).map((s, i) => (I.radarSlotMs(s.t) - I.radarSlotMs(fr[i].t)) / 60000);
  const a = I.radarLoopFrames(slots('2026-11-06T12:00:00Z'), 2);
  assert.equal(a.length, 13); assert.ok(spacing(a).every((m) => m === 10));
  const b = I.radarLoopFrames(slots('2026-11-06T12:00:00Z'), 6);
  assert.equal(b.length, 19); assert.ok(spacing(b).every((m) => m === 20));
  const c = I.radarLoopFrames(slots('2026-11-06T12:00:00Z'), 12);
  assert.equal(c.length, 25); assert.ok(spacing(c).every((m) => m === 30));
  const d = I.radarLoopFrames(slots('2026-11-06T12:00:00Z'), 24);
  assert.equal(d.length, 25); assert.ok(spacing(d).every((m) => m === 60));
  // the newest frame whatever its minute; the marks stay on the clock, so the files already held still serve the next loop
  const e = I.radarLoopFrames(slots('2026-11-06T12:50:00Z'), 6);
  assert.equal(e[e.length - 1].t, '202611061250'); assert.ok(e.slice(0, -1).every((s) => Number(s.t.slice(10)) % 20 === 0));
  assert.equal(spacing(e)[spacing(e).length - 1], 10);
  assert.deepEqual(I.radarLoopFrames(slots('2026-11-06T12:00:00Z'), 99).length, 13, 'an unknown length is the default');
  assert.deepEqual(I.radarLoopFrames([], 2), []);
  // upTo: a slot that has not joined yet is not part of the loop
  const f = I.radarLoopFrames(slots('2026-11-06T12:50:00Z'), 2, '202611061240');
  assert.equal(f[f.length - 1].t, '202611061240');
});
test('a slot with no region is no frame; the priority order holds', () => {
  const p = clone(POINTER); p.slots.push({ t: '209901010000', r: {} });
  const st = new I.RadarStore(ROOT); st._take(p);
  assert.ok(st.frames.every((s) => s.t !== '209901010000'));
  assert.deepEqual(st.order, ['hawaii', 'guam', 'conus', 'alaska', 'carib', 'europe']);
});
test('radarViewOf: longitudes to one copy with a small margin, the whole world past 360; radarHits across the date line', () => {
  const map = (w, e, s, n) => ({ getZoom: () => 6, getBounds: () => ({ getWest: () => w, getEast: () => e, getSouth: () => s, getNorth: () => n }) });
  const v = I.radarViewOf(map(-162, -154, 18, 24), null);
  assert.ok(Math.abs(v.w - (-162.4)) < 1e-9 && Math.abs(v.e - (-153.6)) < 1e-9 && Math.abs(v.s - 17.7) < 1e-9 && Math.abs(v.n - 24.3) < 1e-9);
  const copy = I.radarViewOf(map(-162 - 360, -154 - 360, 18, 24), null);
  assert.ok(Math.abs(copy.w - v.w) < 1e-9, 'a world copy is the same view');
  assert.equal(I.radarViewOf(map(-300, 300, -60, 60), null).whole, true);
  const dl = I.radarViewOf(map(170, 190, 0, 20), null);
  assert.equal(I.radarHits(dl, 5, -179, 10, -170), true, 'a cell past 180 from the view that spans it');
  assert.equal(I.radarHits(dl, 5, 140, 10, 150), false);
  assert.equal(I.radarHits(dl, 30, 175, 40, 178), false, 'above the view');
  assert.deepEqual([3.99, 4, 6.99, 7, 12].map(I.radarWantTier), [0, 1, 1, 2, 2], 'about one screen pixel per picture node');
});
test('the colour table: nothing below the floor, thin drizzle, solid rain; the words', () => {
  const lut = I.radarLut();
  assert.equal(lut[1 * 2 * 4 + 3], 0, 'code 1 = no echo');
  const floor = lut[2 * 2 * 4 + 3], at25 = lut[(2 + 40) * 2 * 4 + 3], top = lut[142 * 2 * 4 + 3];
  assert.ok(floor > 100 && floor < 140, 'alpha at 5 dBZ ' + floor); assert.ok(at25 > 200 && at25 === top, 'alpha from 25 dBZ');
  assert.equal(I.radarDbz(2), 5); assert.equal(I.radarDbz(142), 75);
  assert.deepEqual([15, 25, 35, 45, 55, 65].map(I.radarRainWord), ['very light rain', 'light rain', 'moderate rain', 'heavy rain', 'very heavy rain', 'intense rain, possible hail']);
  const text = I.RADAR_ATTRIBUTION.replace(/<[^>]+>/g, '');
  assert.ok(text.length <= 29, text); assert.ok(/\/notice\/radar/.test(I.RADAR_ATTRIBUTION));
});

// ------------------------------------------------------------------ the store
test('the store reads real frames: the client\'s decoder = a plain PNG reader, every sampled code', async () => {
  const w = world(), st = new w.I.RadarStore(ROOT); st._take(POINTER);
  const rec = POINTER.slots.find((s) => s.t === EXPECT.slot), loop = [rec], view = { whole: false, s: 18, n: 24, w: -162, e: -154, z: 12 };
  st.plan(view, loop, 0, null);
  const pre = 'radar/v1/' + EXPECT.slot + '/hawaii/';
  assert.ok(w.pending().some((u) => u.endsWith(pre + 't0.png')), 'the t0 first');
  w.answer(new RegExp(pre + 't0\\.png$'), fixtureBytes('hawaii_t0.png')); await until(() => st.pics.has(st.key(EXPECT.slot, 'hawaii', 't0')));
  const t0 = st.pics.get(st.key(EXPECT.slot, 'hawaii', 't0'));
  assert.deepEqual([t0.cols, t0.rows], [130, 110]);
  for (const [r, c, v] of EXPECT.t0.hawaii.samples) assert.equal(t0.q[r * t0.cols + c], v, 't0 ' + r + ',' + c);
  for (const [r, c, v] of EXPECT.t0.hawaii.echo) { assert.equal(t0.q[r * t0.cols + c], v); assert.equal(t0.re[r], 1, 'the row holds echo'); }
  for (const tier of ['t1', 't2']) {
    const nm = EXPECT[tier].name, u = pre + tier + '/' + nm + '.png';
    await until(() => w.answer(new RegExp(u.replace(/[-/]/g, '\\$&') + '$'), fixtureBytes('hawaii_' + tier + '_' + nm + '.png')));
    await until(() => st.pics.has(st.key(EXPECT.slot, 'hawaii', tier, nm)));
    const pic = st.pics.get(st.key(EXPECT.slot, 'hawaii', tier, nm));
    // A cell keeps the box round its rain: every sampled echo pixel lies inside it, and the box is inside the file's 502 x 502.
    assert.ok(pic.r0 >= 0 && pic.c0 >= 0 && pic.r0 + pic.rows <= 502 && pic.c0 + pic.cols <= 502, tier + ' box inside the file');
    assert.equal(pic.q.length, pic.cols * pic.rows);
    assert.ok(pic.bytes < 502 * 502, tier + ' really cropped: ' + pic.cols + ' x ' + pic.rows);
    for (const [r, c, v] of EXPECT[tier].echo) assert.equal(pic.q[(r - pic.r0) * pic.cols + (c - pic.c0)], v, tier + ' ' + r + ',' + c);
  }
  st.stop();
});
test('load: never rejects; a failure is null with a cooldown a force overrides; the held pointer survives a failed re-read', async () => {
  const w = world(), st = new w.I.RadarStore(ROOT);
  const p1 = st.load(true); w.answer(/latest\.json/, {}, 503);
  assert.equal(await p1, null); assert.equal(st.status, 'failed');
  assert.equal(await st.load(false), null, 'inside the cooldown no request'); assert.equal(w.pending().length, 0);
  const p2 = st.load(true); w.answer(/latest\.json/, clone(POINTER));
  assert.equal(await p2, st); assert.equal(st.status, 'ok');
  st.readAt = 0;
  const p3 = st.load(true); w.fail(/latest\.json/);
  assert.equal(await p3, st, 'a failed re-read keeps the map'); assert.ok(st.pointer);
  const p4 = st.load(true); w.answer(/latest\.json/, { format: 'nope' });
  assert.equal(await p4, st, 'an unreadable pointer changes nothing'); assert.equal(st.pointer.format, 'radar-v1');
});
test('plan: at most four downloads, the coverage first, then the shown frame, the loop forward; a downloading file out of the plan is dropped', async () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 13, ['hawaii', 'guam']), st = new w.I.RadarStore(ROOT); st._take(doc);
  st.active = true;
  const frames = st.frames, view = { whole: false, s: 18, n: 24, w: -162, e: -154, z: 6 };
  st.plan(view, frames, 12, null);
  assert.equal(w.I.RADAR_INFLIGHT, 4);
  assert.equal(w.pending().length, 4);
  assert.ok(w.pending()[0].endsWith(frames[12].t + '/hawaii/t0.png'), 'the newest frame first');
  assert.ok(w.pending().every((u) => /hawaii/.test(u)), 'nothing of a region out of view');
  assert.ok(w.pending()[1].endsWith(frames[0].t + '/hawaii/t0.png'), 'then forward from the shown frame, wrapping');
  // the view moves to Guam: Hawaii's downloads stop and Guam's start
  st.plan({ whole: false, s: 10, n: 16, w: 142, e: 148, z: 6 }, frames, 12, null);
  await settle(4);
  assert.ok(w.pending().every((u) => /guam/.test(u)), w.pending().join(' '));
  st.stop();
  assert.equal(w.pending().length, 0, 'stop drops every download');
});
test('a file 404: tried again after the cooldown, the pointer re-read; a bad file is final; a stalled download is given up', async () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 2), st = new w.I.RadarStore(ROOT); st._take(doc);
  st.active = true; st.retryMs = 40; st.readAt = 0;
  const frames = st.frames, view = { whole: false, s: 18, n: 24, w: -162, e: -154, z: 6 };
  st.plan(view, frames, 1, null);
  const k = st.key(frames[1].t, 'hawaii', 't0'), kOld = st.key(frames[0].t, 'hawaii', 't0');
  w.answer(new RegExp(frames[1].t + '/hawaii/t0\\.png'), null, 404); await until(() => st.fails[k]);
  assert.equal(st.fails[k].final, false, 'a retired slot may come back');
  assert.ok(w.pending(/latest\.json/).length === 1, 'the pointer is read again');
  w.answer(/latest\.json/, doc);
  await until(() => w.pending(new RegExp(frames[1].t + '/hawaii/t0\\.png')).length === 1);
  assert.equal(st.done(k), false, 'the cooldown over, it is not done');
  // garbage: final
  w.answer(new RegExp(frames[0].t + '/hawaii/t0\\.png'), new Uint8Array([1, 2, 3]).buffer); await until(() => st.fails[kOld]);
  assert.equal(st.fails[kOld].final, true); assert.equal(st.done(kOld), true);
  st.stop();
  // a stall: no byte for stallMs
  const w2 = world(), st2 = new w2.I.RadarStore(ROOT); st2._take(doc); st2.active = true; st2.stallMs = 15; st2.retryMs = 100000;
  st2.plan(view, st2.frames.slice(1), 0, null);
  const k2 = st2.key(st2.frames[1].t, 'hawaii', 't0');
  await until(() => st2.fails[k2]);
  assert.equal(st2.fails[k2].final, false); assert.equal(Object.keys(st2.inflight).length, 0, 'the slot is free');
  st2.stop();
});
test('the budget: a stack that does not fit is not started; the cap never evicts what the plan names; release leaves nothing', () => {
  const w = world();
  const doc = pointerDoc('2026-11-06T12:00:00Z', 13, ['hawaii'], () => ({ t1: 'f', t2: 'ffffffff' })), st = new w.I.RadarStore(ROOT); st._take(doc);
  st.active = true;
  const view = { whole: false, s: 20.5, n: 21.5, w: -158.5, e: -157.5, z: 10 };
  st.plan(view, st.frames, 12, null);
  assert.equal(st.stack.t1, true); assert.equal(st.stack.t2, true, 'a Hawaii view fits');
  st.maxBytes = 2 * 1024 * 1024;
  st.plan(view, st.frames, 12, null);
  assert.equal(st.stack.t2, false, 'over the cap the t2 stack is not started');
  assert.ok(st.queue.some((q) => q.tier === 't2' && q.slot === st.frames[12].t), 'the shown frame still has its t2');
  assert.ok(!st.queue.some((q) => q.tier === 't2' && q.slot === st.frames[3].t), 'no other frame');
  st.stop();
  // the LRU
  const s2 = storeWith(w, doc); s2.maxBytes = 30000;
  const big = () => t0Of('hawaii', () => 1);
  s2.loop = s2.frames; s2.planSet = Object.create(null); s2.planSet[s2.key(s2.frames[0].t, 'hawaii', 't0')] = true;
  s2.landed(s2.frames[0].t, 'hawaii', 't0', null, big());
  for (let i = 1; i < 5; i++) s2.landed(s2.frames[i].t, 'hawaii', 't0', null, big());
  assert.ok(s2.bytes <= 30000 + 14300, 'bounded: ' + s2.bytes);
  assert.ok(s2.pics.has(s2.key(s2.frames[0].t, 'hawaii', 't0')), 'the plan\'s file stays');
  s2.release();
  assert.equal(s2.pics.size, 0); assert.equal(s2.bytes, 0); assert.equal(Object.keys(s2.cov).length, 0); assert.ok(s2.pointer, 'the pointer stays');
});

// ------------------------------------------------------------------ the sampler and the readout
test('a cell keeps only the box round its rain: the same pixels, a fraction of the memory', () => {
  const side = 502, rain = new Set();
  for (let r = 120; r < 180; r++) for (let c = 300; c < 390; c++) if ((r + c) % 3) rain.add(r * side + c);
  const q = new Uint8Array(side * side); rain.forEach((i) => { q[i] = 40 + (i % 50); });
  q[121 * side + 301] = 1;                                   // a code 1 beside the rain is no echo, and is not what makes the box
  const fr = { q, cols: side, rows: side };
  const full = I.radarPic(fr), box = I.radarPic(fr, true);
  assert.deepEqual([full.r0, full.c0, full.cols, full.rows, full.bytes], [0, 0, side, side, side * side], 'not cropped unless asked: t0 holds the coverage');
  assert.deepEqual([box.r0, box.c0, box.cols, box.rows, box.bytes], [120, 300, 90, 60, 90 * 60]);
  assert.equal(box.q[0 * 90 + 1], q[120 * side + 301], 'the first row'); assert.equal(box.q[59 * 90 + 89], q[179 * side + 389], 'the last corner');
  assert.equal(box.re.length, 60); assert.ok(box.re.every((v) => v === 1));
  const dry = I.radarPic({ q: new Uint8Array(side * side), cols: side, rows: side }, true);
  assert.deepEqual([dry.cols, dry.rows, dry.bytes], [0, 0, 0], 'a cell with nothing in it holds nothing');
  // Every pixel of a tile over the cell, through the one sampler, full against cropped, at the box's edges too.
  const tile = 256, cases = [{ z: 7, x: 8, y: 54 }, { z: 9, x: 34, y: 217 }, { z: 11, x: 137, y: 871 }];
  for (const c of cases) {
    const lats = new Float64Array(tile), lons = new Float64Array(tile), N = tile * 2 ** c.z;
    for (let i = 0; i < tile; i++) {
      lats[i] = Math.atan(Math.sinh(Math.PI - 2 * Math.PI * (c.y * tile + i + 0.5) / N)) * 180 / Math.PI;
      lons[i] = (c.x * tile + i + 0.5) / N * 360 - 180;
    }
    const own = new Uint8Array(tile * tile).fill(1), a = new Float64Array(tile * tile), b = new Float64Array(tile * tile);
    const lat0 = Math.floor(lats[128] / 5) * 5, lon0 = Math.floor(lons[128] / 5) * 5;
    const piece = (pic) => ({ pic, own: 1, ppd: 100, x0: 0, x1: tile, y0: 0, y1: tile, top: lat0 + 5, ay: 0.5, left: lon0, ax: 0.5 });
    const na = I.radarSample([piece(full)], { _rLat: lats, _rLon: lons }, own, 0, tile, 0, tile, a);
    const nb = I.radarSample([piece(box)], { _rLat: lats, _rLon: lons }, own, 0, tile, 0, tile, b);
    assert.equal(nb, na, 'z' + c.z + ' pixels with echo');
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) assert.fail('z' + c.z + ' pixel ' + i + ': ' + a[i] + ' vs ' + b[i]);
  }
});
test('the budget counts what cells really hold; heavier rain than the estimate gives way, never the cap', () => {
  const w = world();
  const doc = pointerDoc('2026-11-06T12:00:00Z', 13, ['hawaii'], () => ({ t1: 'f', t2: 'ffffffff' })), st = storeWith(w, doc);
  st.active = true; st._pump = () => {};
  const view = { whole: false, s: 18, n: 23, w: -161, e: -155, z: 10 };
  st.plan(view, st.frames, 12, null);
  const side = 502, before = st._cellBytes(st.key(st.frames[0].t, 'hawaii', 't2', '20_-160'), 't2');
  assert.equal(before, side * side * 0.5, 'nothing landed: half a cell');
  const fill = (n) => ({ q: new Uint8Array(side * side).map((_, i) => (i % side < n ? 40 : 0)), cols: side, rows: side });
  st.landed(st.frames[0].t, 'hawaii', 't2', '20_-160', fill(10));
  assert.equal(st._cellBytes(st.key(st.frames[0].t, 'hawaii', 't2', '20_-160'), 't2'), 10 * side, 'landed: its real size');
  assert.equal(st._cellBytes(st.key(st.frames[1].t, 'hawaii', 't2', '20_-160'), 't2'), 10 * side, 'not landed: the mean of what has');
  // A small cap and full cells: the stack was on, a landing puts the store over, the stack goes off and what it held is let go.
  const s2 = storeWith(w, doc); s2.active = true; s2._pump = () => {}; s2.maxBytes = 1.2 * 1024 * 1024;
  s2.tierSeen.t2 = { n: 1, bytes: 1000 }; s2.tierSeen.t1 = { n: 1, bytes: 1000 };
  s2.plan(view, s2.frames, 12, null);
  assert.equal(s2.stack.t2, true, 'small cells fit');
  const names = s2._cellsIn('hawaii', 't2', view).map((c) => c.name);
  for (let f = 0; f < 13 && s2.stack.t2; f++) for (const n of names) s2.landed(s2.frames[f].t, 'hawaii', 't2', n, fill(side));
  assert.equal(s2.stack.t2, false, 'full cells: the t2 stack gives way');
  assert.ok(s2.queue.every((q) => q.tier !== 't2' || q.slot === s2.frames[12].t), 'only the shown frame still asks for t2');
  const shown = s2.frames[12].t;
  let extra = 0; s2.pics.forEach((p, k) => { if (k.includes('/t2/') && !k.startsWith(shown)) extra += p.bytes; });
  assert.ok(s2.bytes <= s2.maxBytes + names.length * side * side, 'bounded: ' + s2.bytes + ', other frames hold ' + extra);
  s2.release(); assert.equal(s2.lastPlan, null);
});
test('tile = readout = the job\'s geometry, on real frames at every tier', async () => {
  const w = world(), st = new w.I.RadarStore(ROOT); st._take(POINTER);
  const rec = st.index[EXPECT.slot];
  st.loop = [rec];
  const land = async (tier, nm, file) => {
    const key = st.key(rec.t, 'hawaii', tier, nm);
    st._start({ key, slot: rec.t, region: 'hawaii', tier, expect: tier === 't0' ? { cols: 130, rows: 110 } : { cols: 502, rows: 502 } });
    w.answer(new RegExp(rec.t + '/hawaii/' + tier + (nm ? '/' + nm : '') + '\\.png$'), fixtureBytes(file));
    await until(() => st.pics.has(key));
    return st.pics.get(key);
  };
  const t0 = await land('t0', null, 'hawaii_t0.png');
  const t2 = await land('t2', EXPECT.t2.name, 'hawaii_t2_' + EXPECT.t2.name + '.png');
  const t1 = await land('t1', EXPECT.t1.name, 'hawaii_t1_' + EXPECT.t1.name + '.png');
  const layer = layerWith(w, st, 0, false);
  const box = POINTER.regions.hawaii.box;
  // t0 at tile zoom 3: an echo node's pixel, against the reference
  const check = (z, lat, lon, pic, fyFx) => {
    const size = 256 * Math.pow(2, z), wx = (lon + 180) / 360 * size, sinL = Math.sin(lat * Math.PI / 180), wy = (0.5 - Math.log((1 + sinL) / (1 - sinL)) / (4 * Math.PI)) * size;
    const x = Math.floor(wx / 256), y = Math.floor(wy / 256), px = Math.floor(wx) - x * 256, py = Math.floor(wy) - y * 256;
    const key = x + ':' + y + ':' + z, el = layer._tiles[key] ? layer._tiles[key].el : tile(layer, x, y, z);
    layer._draw(el, { x, y, z });
    const c = pixelCentre(x, y, z, px, py), [fy, fx] = fyFx(c.lat, c.lon), want = bilinear(pic, fy, fx);
    const got = layer.readoutAt(lat, lon, z);
    if (want === 0) { assert.ok(got.kind !== 'echo', 'no echo at ' + lat + ',' + lon + ' got ' + JSON.stringify(got)); return null; }
    assert.equal(got.kind, 'echo', 'echo expected at ' + lat + ',' + lon);
    assert.ok(Math.abs(got.code - want) < 1e-3, 'code ' + got.code + ' vs ' + want);
    assert.equal(got.dbz, I.radarDbz(Math.round(want * 2) / 2));
    return { el, i: py * 256 + px, want };
  };
  let checked = 0;
  for (const [r, c] of EXPECT.t0.hawaii.echo) {
    const lat = box[2] - (r + 0.5) / 10, lon = box[1] + (c + 0.5) / 10;
    if (check(3, lat, lon, t0, (la, lo) => [(box[2] - la) * 10 - 0.5, (lo - box[1]) * 10 - 0.5])) checked++;
  }
  assert.ok(checked >= 20, 't0 pixels checked ' + checked);
  // t2 at tile zoom 12
  const nm2 = EXPECT.t2.name.split('_').map(Number); checked = 0;
  for (const [r, c] of EXPECT.t2.echo) {
    const lat = nm2[0] + 5 + 0.5 / 100 - r / 100, lon = nm2[1] - 0.5 / 100 + c / 100;
    if (lat < nm2[0] || lat >= nm2[0] + 5 || lon < nm2[1] || lon >= nm2[1] + 5) continue;
    const res = check(12, lat, lon, t2, (la, lo) => [(nm2[0] + 5 - la) * 100 + 0.5, (lo - nm2[1]) * 100 + 0.5]);
    if (res) {
      checked++;
      const lut = layer._lut, j = Math.round(res.want * 2) * 4, img = res.el.lastImg;
      assert.deepEqual([img[res.i * 4], img[res.i * 4 + 1], img[res.i * 4 + 2], img[res.i * 4 + 3]], [lut[j], lut[j + 1], lut[j + 2], lut[j + 3]], 'the drawn pixel = the readout');
    }
  }
  assert.ok(checked >= 20, 't2 pixels checked ' + checked);
  // t1 at tile zoom 6
  const nm1 = EXPECT.t1.name.split('_').map(Number); checked = 0;
  for (const [r, c] of EXPECT.t1.echo) {
    const lat = nm1[0] + 20 + 0.5 / 25 - r / 25, lon = nm1[1] - 0.5 / 25 + c / 25;
    if (lat < box[0] || lat >= box[2] || lon < nm1[1] || lon >= nm1[1] + 20) continue;
    if (check(6, lat, lon, t1, (la, lo) => [(nm1[0] + 20 - la) * 25 + 0.5, (lo - nm1[1]) * 25 + 0.5])) checked++;
  }
  assert.ok(checked >= 10, 't1 pixels checked ' + checked);
  st.stop();
});
test('a pixel belongs to the first region covering it, never by box: the US strip does not hide the Caribbean', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 1, ['conus', 'carib']), st = storeWith(w, doc), rec = st.frames[0];
  // conus covers only north of 24 N; carib covers everything, with echo at 22.5 N and at 24.5 N
  st.landed(rec.t, 'conus', 't0', null, t0Of('conus', (r) => ((55 - (r + 0.5) / 10) >= 24 ? 1 : 0)));
  const echoAt = (la) => (r) => Math.abs((25 - (r + 0.5) / 10) - la) < 0.25;
  st.landed(rec.t, 'carib', 't0', null, t0Of('carib', (r, c) => (echoAt(22.5)(r) || echoAt(24.5)(r) ? 42 : 1)));
  st.loop = [rec];
  const layer = layerWith(w, st, 0, false);
  const z = 5, lon = -80, tileOf = (lat) => {
    const size = 256 * 32, s = Math.sin(lat * Math.PI / 180);
    return [Math.floor(((lon + 180) / 360 * size) / 256), Math.floor(((0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * size) / 256)];
  };
  for (const lat of [22.5, 24.5]) { const [x, y] = tileOf(lat); if (!layer._tiles[x + ':' + y + ':' + z]) tile(layer, x, y, z); }
  const south = layer.readoutAt(22.5, lon, z), north = layer.readoutAt(24.5, lon, z);
  assert.equal(south.region, 'carib'); assert.equal(south.kind, 'echo', 'the Caribbean\'s echo shows under the US box\'s no-coverage strip');
  assert.equal(north.region, 'conus', 'where both cover, the first in the order'); assert.equal(north.kind, 'dry', 'and only its own data shows');
  const out = layer.readoutAt(5, lon, z);
  assert.equal(out, null, 'no tile, no answer');
});
test('the tint: over the covered area only, the same on every step; echo over it; nothing outside', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 2), st = storeWith(w, doc);
  st.landed(st.frames[1].t, 'hawaii', 't0', null, t0Of('hawaii', (r, c) => (c < 65 ? 1 : 0)));   // the west half covered
  st.landed(st.frames[0].t, 'hawaii', 't0', null, t0Of('hawaii', (r, c) => (c < 65 ? (r === 55 && c === 30 ? 60 : 1) : 0)));
  st.loop = st.frames;
  const layer = layerWith(w, st, 1, false), [x, y, z] = [0, 7, 4], el = tile(layer, x, y, z);
  const tint = w.I.RADAR_TINT, img = el.lastImg;
  assert.equal(tint[3], 41, "the owner's pick: 16 % white");
  assert.equal(tint[0] + tint[1] + tint[2], 765);
  assert.ok(img, 'covered: drawn');
  let tinted = 0, clear = 0;
  for (let py = 0; py < 256; py++) for (let px = 0; px < 256; px++) {
    const k = (py * 256 + px) * 4, c = pixelCentre(x, y, z, px, py);
    const inBox = c.lat >= 15 && c.lat < 26 && c.lon >= -164 && c.lon < -157.5;
    if (inBox) { assert.equal(img[k + 3], tint[3], 'tinted ' + px + ',' + py); tinted++; } else { assert.equal(img[k + 3], 0); clear++; }
  }
  assert.ok(tinted > 100 && clear > 100);
  // a dry tile staying dry with the same coverage is not painted again
  const puts = el.puts;
  layer.setStep(1, false, false); assert.equal(el.puts, puts);
  // step 0 has echo: painted; the tint elsewhere unchanged
  layer.setStep(0, false, false); assert.equal(el.puts, puts + 1);
  const k = (0 * 256 + 0) * 4;
  assert.equal(el.lastImg[k + 3], img[k + 3]);
});
test('tiers while playing: a cell draws t2 only once its whole stack is in; paused, whatever has landed', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 2, ['hawaii'], () => ({ t2: 'ffffffff' })), st = storeWith(w, doc);
  const [a, b] = st.frames, nm = '20_-160';
  st.stack = { t1: true, t2: true };
  st.loop = st.frames; st.rev++;
  for (const rec of st.frames) st.landed(rec.t, 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  st.landed(b.t, 'hawaii', 't2', nm, frame(502, 502, () => 40));
  const layer = layerWith(w, st, 1, false);
  assert.ok(layer._usable(b, 'hawaii', 't2', nm), 'paused: landed = drawn');
  layer.playing = true;
  assert.equal(layer._usable(b, 'hawaii', 't2', nm), null, 'playing: the other step is missing');
  st.landed(a.t, 'hawaii', 't2', nm, frame(502, 502, () => 40));
  assert.ok(layer._usable(b, 'hawaii', 't2', nm), 'the whole stack in');
  st.stack.t2 = false; st.rev++;
  assert.equal(layer._usable(b, 'hawaii', 't2', nm), null, 'over the budget: never in playback');
});
test('a region missing a step holds its last frame for two steps, then shows none', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 5), st = storeWith(w, doc), fr = st.frames;
  st.loop = fr;
  st.landed(fr[0].t, 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  assert.equal(st.resolve('hawaii', 0), fr[0]);
  assert.equal(st.resolve('hawaii', 1), fr[0]); assert.equal(st.resolve('hawaii', 2), fr[0]);
  assert.equal(st.resolve('hawaii', 3), null, 'never an old frame for long');
  // a slot that does not list the region is passed over the same way
  delete fr[4].r.hawaii;
  st.landed(fr[3].t, 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  assert.equal(st.resolve('hawaii', 4), fr[3]);
});
test('world copies draw the same pixels; the date line wraps longitudes', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 1), st = storeWith(w, doc), rec = st.frames[0];
  st.landed(rec.t, 'hawaii', 't0', null, t0Of('hawaii', (r, c) => (r % 7 === 0 && c % 5 === 0 ? 50 : 1)));
  st.loop = [rec];
  const layer = layerWith(w, st, 0, false), a = tile(layer, 0, 7, 4), b = tile(layer, 16, 7, 4), c = tile(layer, -16, 7, 4);
  assert.deepEqual(Array.from(a.lastImg), Array.from(b.lastImg)); assert.deepEqual(Array.from(a.lastImg), Array.from(c.lastImg));
  assert.ok(Array.from(a.lastImg).some((v, i) => i % 4 === 3 && v > 30));
  // the readout on a copy reads through its unwrapped key
  assert.deepEqual(layer.readoutAt(21, -158, 4), layer.readoutAt(21, -158 - 360, 4));
});
test('the readout says echo, no echo, or no coverage', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 1), st = storeWith(w, doc), rec = st.frames[0];
  st.landed(rec.t, 'hawaii', 't0', null, t0Of('hawaii', (r, c) => (r >= 20 && r < 40 && c >= 20 && c < 40 ? 62 : c < 80 ? 1 : 0)));
  st.loop = [rec];
  const layer = layerWith(w, st, 0, false), box = BOXES.hawaii;
  for (const [la, lo] of [[box[2] - 3, box[1] + 3], [box[2] - 8, box[1] + 3], [box[2] - 3, box[1] + 11]]) tileAt(layer, la, lo, 4);
  const hit = layer.readoutAt(box[2] - 3, box[1] + 3, 4);
  assert.equal(hit.kind, 'echo'); assert.equal(hit.dbz, 35); assert.equal(hit.rec, rec); assert.equal(hit.region, 'hawaii');
  assert.equal(layer.readoutAt(box[2] - 8, box[1] + 3, 4).kind, 'dry');
  assert.equal(layer.readoutAt(box[2] - 3, box[1] + 11, 4).kind, 'none');
});

// ------------------------------------------------------------------ the player
function playerWith(w, doc, loopH) {
  if (loopH) w.saved = { radarLoop: loopH };
  const st = storeWith(w, doc), layer = layerWith(w, st, -1, false);
  const ov = { map: w.map, speed: 1, readout: null, uis: 0, _radarUI() { this.uis++; } };
  const p = new w.I.RadarPlayer(ov, st, layer);
  return { p, st, layer, ov };
}
const landAll = (st, frames) => { for (const rec of frames) if (!st.pics.has(st.key(rec.t, 'hawaii', 't0'))) st.landed(rec.t, 'hawaii', 't0', null, t0Of('hawaii', () => 1)); };
test('the player opens paused on the newest frame and never plays by itself', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st, layer, ov } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 145));
  p.start();
  assert.equal(p.loopH, 2); assert.equal(p.frames.length, 13); assert.equal(p.idx, 12); assert.equal(p.playing, false);
  assert.equal(layer.idx, 12);
  landAll(st, p.frames);
  t.mock.timers.tick(30000); await settle(2);
  assert.equal(p.idx, 12, 'still on the newest');
  assert.equal('playing' in ov, false, 'the model\'s play state is not touched');
  p.stop();
});
test('Play from the newest starts the loop; every frame in turn on a drift-free clock; a held last frame; pause stops', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st, layer, ov } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, p.frames);
  p.play();
  assert.equal(p.idx, 0, 'back to the start'); assert.equal(layer.idx, 0); assert.equal(layer.playing, true);
  const seen = [];
  for (let i = 0; i < 12; i++) { t.mock.timers.tick(500); await settle(1); seen.push(p.idx); }
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], 'no frame skipped at 2 frames/s');
  t.mock.timers.tick(500); await settle(1);
  assert.equal(p.idx, 12, 'the last frame is held longer');
  t.mock.timers.tick(1000); await settle(1);
  assert.equal(p.idx, 0, 'then the loop wraps');
  ov.speed = 4; p.speedChanged();
  t.mock.timers.tick(500); await settle(1); t.mock.timers.tick(125); await settle(1);
  const a = p.idx; t.mock.timers.tick(125); await settle(1);
  assert.equal(p.idx, a + 1, 'shark: 8 frames/s');
  p.pause();
  const held = p.idx; t.mock.timers.tick(5000); await settle(1);
  assert.equal(p.idx, held); assert.equal(p.playing, false);
  p.stop();
});
test('a step whose pictures are missing waits five seconds at most, then moves on', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start();
  for (const rec of [p.frames[12], p.frames[0]]) st.landed(rec.t, 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  p.play();
  t.mock.timers.tick(500); await settle(1);
  assert.equal(p.idx, 0, 'frame 1 not here: wait');
  for (let i = 0; i < 70; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.equal(p.idx, 0, 'still waiting after four seconds: no empty map');
  for (let i = 0; i < 25; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.ok(p.idx >= 1, 'moved on past the wait');
  p.stop();
});
test('the slider stops the loop on that frame; a loop change keeps the slot and is remembered for the tab', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st, layer } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 145));
  p.start(); landAll(st, p.frames); p.play();
  t.mock.timers.tick(500); await settle(1);
  p.scrub(7);
  assert.equal(p.playing, false); assert.equal(p.idx, 7); assert.equal(layer.idx, 7); assert.equal(layer.playing, false);
  const slot = p.frames[7].t;
  p.setLoop(6);
  assert.equal(p.frames.length, 19); assert.equal(w.saved.radarLoop, 6);
  assert.ok(p.frames[p.idx].t >= slot, 'the nearest frame at or after the shown one');
  assert.ok((I.radarSlotMs(p.frames[p.idx].t) - I.radarSlotMs(slot)) <= 20 * 60000);
  p.stop();
  const again = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 145));
  assert.equal(again.p.loopH, 6, 'read back for the tab');
  again.p.start(); assert.equal(again.p.idx, again.p.frames.length - 1, 'opens paused on the newest'); again.p.stop();
});
test('a new slot joins once its pictures land: paused on the newest moves to it, playing keeps its place', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, p.frames);
  const newer = pointerDoc('2026-11-06T12:10:00Z', 14);
  st._take(newer);
  assert.equal(p.frames[p.idx].t, '202611061200', 'not before its files are here');
  assert.equal(p.pending.t, '202611061210');
  st.landed('202611061210', 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  assert.equal(p.frames[p.idx].t, '202611061210', 'paused on the newest: moved'); assert.equal(p.pending, null);
  // playing: the shown slot stays shown
  p.play(); t.mock.timers.tick(500); await settle(1); t.mock.timers.tick(500); await settle(1);
  const shown = p.frames[p.idx].t;
  st._take(pointerDoc('2026-11-06T12:20:00Z', 15));
  st.landed('202611061220', 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  assert.equal(p.frames[p.idx].t, shown); assert.equal(p.frames[p.frames.length - 1].t, '202611061220');
  p.stop();
});
test('a hidden tab pauses and resumes on the same frame; stop leaves nothing running', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, p.frames); p.play();
  t.mock.timers.tick(500); await settle(1);
  const held = p.idx;
  p.visibility(true);
  assert.equal(p.playing, false); assert.equal(st.timer, null);
  t.mock.timers.tick(5000); await settle(1); assert.equal(p.idx, held);
  p.visibility(false);
  assert.equal(p.playing, true); assert.equal(p.idx, held, 'the same frame');
  p.stop();
  assert.equal(p.playing, false); assert.equal(p.uiTimer, null); assert.equal(st.timer, null); assert.equal(st.active, false);
  const at = p.idx; t.mock.timers.tick(5000); await settle(1); assert.equal(p.idx, at);
});

test('the clock: a late step is repaid by one quick step, never a skipped frame or a late loop', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, p.frames); p.play();
  assert.equal(p.idx, 0);
  clearTimeout(p.timer); p.timer = null;
  p.due = Date.now() - 100;                  // this step started 100 ms late
  p._next(p.gen);
  t.mock.timers.tick(399); await settle(1);
  assert.equal(p.idx, 0, 'not early');
  t.mock.timers.tick(1); await settle(1);
  assert.equal(p.idx, 1, 'due 400 ms from now, not 500');
  clearTimeout(p.timer); p.timer = null;
  p.due = Date.now() - 5000;                 // a long stall: one step at once, not five
  p._next(p.gen);
  t.mock.timers.tick(0); await settle(1);
  assert.equal(p.idx, 2);
  t.mock.timers.tick(100); await settle(1);
  assert.equal(p.idx, 2, 'then back on the beat');
  p.stop();
});
test('one frame or none: nothing to play, nothing breaks', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), one = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 1));
  one.p.start(); landAll(one.st, one.p.frames);
  assert.equal(one.p.frames.length, 1); assert.equal(one.p.idx, 0);
  one.p.play(); assert.equal(one.p.playing, false, 'one frame does not play');
  t.mock.timers.tick(5000); await settle(1); assert.equal(one.p.idx, 0);
  one.p.stop();
  const w2 = world(), none = playerWith(w2, pointerDoc('2026-11-06T12:00:00Z', 0));
  none.p.start();
  assert.equal(none.p.frames.length, 0); assert.equal(none.layer.idx, -1);
  none.p.play(); assert.equal(none.p.playing, false);
  none.p.scrub(3); assert.equal(none.layer.idx, -1);
  none.p.stop();
});

test('a tick of an earlier play does nothing; a pointer past the joined slot leaves a loop to show', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, p.frames); p.play();
  const at = p.idx, gen = p.gen;
  p._tick(gen - 1);
  assert.equal(p.idx, at, 'an old generation');
  p.pause();
  p._tick(p.gen);
  assert.equal(p.idx, at, 'paused');
  // two days on, every slot the player knew has left the pointer
  st._take(pointerDoc('2026-11-08T12:00:00Z', 13));
  assert.ok(p.frames.length >= 2, 'a loop, not nothing');
  assert.ok(st.index[p.frames[p.idx].t], 'a slot the pointer still lists');
  p.stop();
});
test('an older frame landing late never takes the coverage from a newer one', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 2), st = storeWith(w, doc), [older, newer] = st.frames;
  st.landed(newer.t, 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  const rev = st.covRev;
  st.landed(older.t, 'hawaii', 't0', null, t0Of('hawaii', () => 0));
  assert.equal(st.cov.hawaii.slot, newer.t);
  assert.equal(st.covRev, rev, 'the tile owners stand');
});

// ------------------------------------------------------------------ the overlay
function ctl(w, opts) {
  const ov = w.AO.create(w.map, Object.assign({ base: 'https://x/gfswave/0p25/v1', panel: w.g.document.createElement('div'), tz: 'Pacific/Honolulu', getUnit: () => 'US',
    fmtTime: (iso) => 'T' + iso.slice(11, 16), tzAbbr: () => 'HST', pageCycle: () => null, version: '9.9.9', radar: true }, opts || {}));
  ov._syncLook = function () {}; ov._dims = () => ({ w: 1200, h: 800 }); ov._coastFirst = (p) => p; ov.coast = null;
  return ov;
}
const findCls = (el, cls) => { if ((el.className || '').split(' ').indexOf(cls) >= 0) return el; for (const c of el.children || []) { const f = findCls(c, cls); if (f) return f; } return null; };
const allText = (el) => (el.textContent || '') + ' ' + (el.children || []).map(allText).join(' ');
test('the entry: only the radar\'s files, its pane and credit, the panel with play, slider, speed, loop and scale, no particles', async () => {
  const w = world(), ov = ctl(w);
  assert.equal(I.hasObs('radar'), true);
  ov.mount('radar');
  assert.equal(ov.mode, 'obs'); assert.equal(ov.field, 'radar'); assert.equal(w.saved.field, 'radar');
  assert.deepEqual(w.pending(), ['https://x/radar/v1/latest.json'], 'the pointer, nothing else');
  assert.equal(ov.last.state, 'loading');
  w.answer(/latest\.json$/, pointerDoc('2026-11-06T12:00:00Z', 13)); await settle();
  assert.equal(ov.last.state, 'ready');
  assert.ok(ov.radarLayer && w.layers.indexOf(ov.radarLayer) >= 0); assert.equal(w.panes.obsPane.style.zIndex, 260);
  assert.ok(ov.radarPlayer); assert.equal(ov.radarPlayer.playing, false); assert.equal(ov.radarPlayer.idx, 12);
  assert.ok(w.pending().length > 0 && w.pending().every((u) => /\/radar\/v1\//.test(u)), 'no model or SST request: ' + w.pending().join(' '));
  assert.equal(ov.flow, null, 'no particles');
  assert.ok(w.attrs.length === 1 && /MRMS/.test(w.attrs[0]) && !/GSHHG/.test(w.attrs[0]));
  const host = ov.ui.host;
  assert.ok(findCls(host, 'ov-play')); assert.ok(findCls(host, 'ov-timeline')); assert.ok(findCls(host, 'ov-speed')); assert.ok(findCls(host, 'ov-radar-loop')); assert.ok(findCls(host, 'ov-radar-words'));
  assert.equal(findCls(host, 'ov-timeline').max, '12');
  assert.equal(findCls(host, 'ov-play')['aria-label'], 'Play');
  assert.ok(/Radar: NOAA MRMS, EUMETNET OPERA/.test(allText(host)));
  assert.ok(/No/.test('No') && !/GFS-Wave/.test(allText(host)));
  ov.unmount();
});
test('Play and the panel follow in place; Off stops everything and asks nothing more', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('radar');
  w.answer(/latest\.json$/, pointerDoc('2026-11-06T12:00:00Z', 13)); await settle();
  const play = findCls(ov.ui.host, 'ov-play'), st = ov.radar;
  play._ls.click();
  assert.equal(ov.radarPlayer.playing, true);
  assert.equal(findCls(ov.ui.host, 'ov-play')['aria-label'], 'Pause', 'the same panel');
  assert.equal(play, findCls(ov.ui.host, 'ov-play'));
  const slider = findCls(ov.ui.host, 'ov-timeline'); slider.value = '3'; slider._ls.input();
  assert.equal(ov.radarPlayer.playing, false, 'the slider pauses'); assert.equal(ov.radarPlayer.idx, 3);
  const sel = findCls(ov.ui.host, 'ov-radar-loop'); sel.value = '6'; sel._ls.change();
  assert.equal(ov.radarPlayer.loopH, 6);
  // setSpeed under an observation: saved, no model prefetch
  const before = w.fetches.length; ov.setSpeed(4);
  assert.equal(ov.speed, 4); assert.equal(w.saved.speed, 4);
  assert.ok(w.fetches.slice(before).every((u) => /radar/.test(u)), 'no model frames');
  ov.unmount();
  assert.equal(ov.radarLayer, null); assert.equal(ov.radarPlayer, null); assert.equal(st.active, false); assert.equal(st.pics.size, 0);
  await settle(4);
  assert.equal(w.pending(/radar/).length, 0, 'every download dropped');
  const n = w.fetches.length; await settle(4); assert.equal(w.fetches.length, n);
});
test('a refused pointer: the error panel with Retry, then the layer', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('radar');
  w.answer(/latest\.json$/, {}, 503); await settle();
  assert.equal(ov.last.state, 'error'); assert.equal(ov.radarLayer, null);
  ov.mount('radar');
  w.answer(/latest\.json$/, pointerDoc('2026-11-06T12:00:00Z', 2)); await settle();
  assert.equal(ov.last.state, 'ready'); assert.ok(ov.radarPlayer);
  ov.unmount();
});
test('without the setting there is no radar; leaving for a model field drops the layer; a pick of another observation too', async () => {
  const w = world(), off = ctl(w, { radar: false });
  off.mount('radar'); await settle();
  assert.equal(off.last.state, 'error'); assert.deepEqual(w.pending(/radar/), []);
  const w2 = world(), ov = ctl(w2);
  ov.mount('radar');
  w2.answer(/latest\.json$/, pointerDoc('2026-11-06T12:00:00Z', 2)); await settle();
  ov.mount('hs');
  assert.equal(ov.mode, 'model'); assert.equal(ov.radarLayer, null); assert.equal(ov.radarPlayer, null);
  assert.ok(w2.pending().some((u) => /gfswave\/0p25\/v1\/latest\.json/.test(u)));
  assert.deepEqual(w2.pending(/radar/), []);
  ov.unmount();
});
test('a region whose pictures do not answer says so; the note goes when they land', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('radar');
  w.answer(/latest\.json$/, pointerDoc('2026-11-06T12:00:00Z', 2)); await settle();
  const st = ov.radar, nw = st.newestOf('hawaii'), key = st.key(nw.t, 'hawaii', 't0');
  assert.ok(!/not answering/.test(ov._radarNote()), 'nothing failed yet');
  st.fails[key] = { at: Date.now(), final: false };
  assert.ok(/hawaii.*not answering\. Trying again\./.test(ov._radarNote()), ov._radarNote());
  st.fails[key] = { at: Date.now(), final: true };
  assert.ok(/hawaii.*cannot be read\./.test(ov._radarNote()), ov._radarNote());
  st.fails[key] = { at: Date.now(), final: false };
  st.cov.hawaii = { slot: nw.t, pic: { q: new Uint8Array(1), cols: 1, rows: 1 } };   // an older picture is in hand: the newest still does not answer
  assert.ok(/hawaii.*not answering/.test(ov._radarNote()), ov._radarNote());
  st.pics.set(key, { q: new Uint8Array(1), cols: 1, rows: 1, bytes: 1 });             // it landed
  assert.ok(!/not answering|cannot be read/.test(ov._radarNote()), ov._radarNote());
  ov.unmount();
});
test('the panel and the hover say what is known: loading, no picture for the time shown', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('radar');
  w.answer(/latest\.json$/, pointerDoc(new Date(Date.now() - Date.now() % 600000 - 600000).toISOString().replace('.000Z', 'Z'), 8)); await settle();
  const st = ov.radar, layer = ov.radarLayer, p = ov.radarPlayer, box = BOXES.hawaii;
  assert.ok(/^Loading the radar pictures\.$/.test(ov._radarNote()), ov._radarNote());
  const z = 4, pre = tileAt(layer, box[2] - 3, box[1] + 3, z);
  assert.ok(pre);
  assert.equal(layer.readoutAt(box[2] - 3, box[1] + 3, z).kind, 'unknown', 'nothing is known yet here');
  assert.equal(ov._radarReadout({ kind: 'unknown' }), 'radar pictures not in yet');
  assert.equal(st.coverageDue(box[2] - 3, box[1] + 3), true);
  assert.equal(st.coverageDue(box[2] - 3, box[1] + 3 + 360), true, 'a world copy');
  assert.equal(st.coverageDue(0, 0), false, 'outside every box');
  const fr = st.frames, older = fr[0];
  st._land({ key: st.key(older.t, 'hawaii', 't0'), slot: older.t, region: 'hawaii', tier: 't0' }, w.I.radarPic(t0Of('hawaii', () => 1)));   // only the oldest frame's picture: the newest has none within the hold
  assert.ok(st.cov.hawaii, 'coverage in');
  p.scrub(p.frames.length - 1);
  assert.ok(/no picture for/.test(ov._radarNote()), ov._radarNote());
  { const f = p.frames[p.idx]; f.r.hawaii.o = 4;                                       // the file's own time, as the label says it
    assert.ok(ov._radarNote().indexOf('no picture for ' + ov._radarClock(w.I.radarSlotMs(f.t) + 240000).clock) >= 0, ov._radarNote()); f.r.hawaii.o = 0; }
  for (const k of Object.keys(layer._tiles)) delete layer._tiles[k];                  // the tile drawn afresh, as the landing's redraw does
  tileAt(layer, box[2] - 3, box[1] + 3, z);
  assert.equal(layer.readoutAt(box[2] - 3, box[1] + 3, z).kind, 'nopic', 'covered, but no picture for this time: not "no echo"');
  assert.equal(ov._radarReadout({ kind: 'nopic', region: 'hawaii', rec: null }), 'no radar picture for this time');
  ov.unmount();
});
test('the readout stays on the map: past the right edge it goes to the left of the cursor', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('radar');
  w.answer(/latest\.json$/, pointerDoc('2026-11-06T12:00:00Z', 2)); await settle();
  const mv = w.reg.map.mousemove[w.reg.map.mousemove.length - 1], el = ov.readout, box = BOXES.hawaii;
  tileAt(ov.radarLayer, 21, -158, Math.round(w.map.getZoom()));
  el.offsetWidth = 280;
  mv({ latlng: { lat: 21, lng: -158 }, containerPoint: { x: 100, y: 100 }, originalEvent: {} });
  assert.equal(el.classList.contains('ov-readout-left'), false);
  mv({ latlng: { lat: 21, lng: -158 }, containerPoint: { x: 1000, y: 100 }, originalEvent: {} });
  assert.equal(el.classList.contains('ov-readout-left'), true, '1000 + 12 + 280 is past 1200');
  mv({ latlng: { lat: 21, lng: -158 }, containerPoint: { x: 900, y: 100 }, originalEvent: {} });
  assert.equal(el.classList.contains('ov-readout-left'), false, '900 + 12 + 280 fits');
  // a phone: neither side has room, so as far over as it fits; the top edge holds it on the map
  w.map.getContainer().clientWidth = 360; el.offsetWidth = 265;
  mv({ latlng: { lat: 21, lng: -158 }, containerPoint: { x: 190, y: 100 }, originalEvent: {} });
  assert.equal(el.classList.contains('ov-readout-left'), false);
  assert.equal(el.style.left, '79px', 'box 91..356 inside the 360-px map (the CSS adds 12)');
  assert.equal(el.style.top, '100px');
  mv({ latlng: { lat: 21, lng: -158 }, containerPoint: { x: 340, y: 5 }, originalEvent: {} });
  assert.equal(el.classList.contains('ov-readout-left'), true, 'room on the left');
  assert.equal(el.style.left, '340px');
  assert.equal(el.style.top, '32px', 'the CSS lifts it 28 px: kept 4 px inside');
  el.offsetWidth = 400;
  mv({ latlng: { lat: 21, lng: -158 }, containerPoint: { x: 200, y: 100 }, originalEvent: {} });
  assert.equal(el.style.left, '-8px', 'wider than the map: its start stays on the map');
  ov.unmount();
});
test('Play keeps the picture\'s tier while the stacks come in, for 3 s at most; a pause ends it, a resume does not hold', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st, layer } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, p.frames);
  let ready = false; st.stacksReady = () => ready;
  p.scrub(5); p.play();
  assert.ok(p.holdFrom > 0, 'holding'); assert.equal(p.playing, true); assert.equal(layer.playing, false, 'drawn by the paused rule: what has landed');
  for (let i = 0; i < 25; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.equal(p.idx, 5, 'no step while the stacks come in');
  ready = true;
  for (let i = 0; i < 12; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.ok(p.idx > 5, 'the stacks are in: it plays'); assert.equal(p.holdFrom, 0); assert.equal(layer.playing, true);
  p.pause(); assert.equal(p.holdFrom, 0);
  ready = false; p.scrub(5); p.play();
  for (let i = 0; i < 45; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.equal(p.idx, 5, 'still holding at 2.7 s');
  for (let i = 0; i < 20; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.ok(p.idx > 5, 'past 3 s it plays anyway'); assert.equal(p.holdFrom, 0); assert.equal(layer.playing, true);
  p.pause(); p.scrub(5); p.play(); assert.ok(p.holdFrom > 0); p.pause();
  assert.equal(p.holdFrom, 0, 'a pause ends the hold');
  p.play(true); assert.equal(p.holdFrom, 0, 'a tab shown again plays on');
  p.stop();
});
test('Play from the newest frame: the newest stays on the map through the hold, then the loop starts at its first frame', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st, layer } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, p.frames);
  const last = p.frames.length - 1;
  let ready = false; st.stacksReady = () => ready;
  assert.equal(p.idx, last);
  p.play();
  assert.ok(p.holdFrom > 0); assert.equal(p.idx, last, 'not to frame 0 yet: its fine files are not in');
  assert.equal(layer.idx, last, 'the map keeps the frame it shows'); assert.equal(layer.playing, false);
  for (let i = 0; i < 25; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.equal(p.idx, last, 'still the newest while the stacks come in');
  ready = true;
  t.mock.timers.tick(60); await settle(1);
  assert.equal(p.idx, 0, 'the hold is over: the loop starts at its first frame'); assert.equal(p.fromStart, false);
  assert.equal(layer.idx, 0); assert.equal(layer.playing, true);
  for (let i = 0; i < 25; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.ok(p.idx >= 1, 'and it plays on from there');
  p.pause();
  // a pause during the hold: back on the frame it was showing, and the next Play starts over from there
  p.scrub(last); ready = false; p.play(); assert.equal(p.fromStart, true); p.pause();
  assert.equal(p.fromStart, false); assert.equal(p.idx, last);
  // past the 3 s limit it starts at frame 0 all the same
  p.play();
  for (let i = 0; i < 55; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.ok(p.idx < last, 'on with the loop after 3 s'); assert.equal(p.fromStart, false);
  p.pause();
  // nothing to wait for: straight to frame 0, as ever
  ready = true; p.scrub(last); p.play(); assert.equal(p.idx, 0); assert.equal(p.holdFrom, 0); assert.equal(p.fromStart, false);
  p.stop();
});
test('stacksReady: every wanted stack of the view has landed or been given up; a stack not wanted does not count', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 3, ['hawaii'], () => ({ t1: 'f' })), st = storeWith(w, doc);
  st.active = true; st._pump = () => {};
  const view = { whole: false, s: 16, n: 25, w: -163, e: -152, z: 5 };
  st.plan(view, st.frames, 2, null);
  assert.equal(st.stack.t1, true);
  assert.equal(st.stacksReady(view), false, 'nothing landed');
  const cells = st._cellsIn('hawaii', 't1', view);
  assert.ok(cells.length > 1);
  const lastCell = cells[cells.length - 1];
  for (const rec of st.frames) for (const c of cells.slice(0, -1)) st.landed(rec.t, 'hawaii', 't1', c.name, frame(502, 502, () => 60));
  assert.equal(st.stacksReady(view), false, 'one cell short');
  for (const rec of st.frames) st.fails[st.key(rec.t, 'hawaii', 't1', lastCell.name)] = { at: Date.now(), final: false };
  st.rev++;
  assert.equal(st.stacksReady(view), true, 'landed or given up for now');
  for (const rec of st.frames) delete st.fails[st.key(rec.t, 'hawaii', 't1', lastCell.name)];
  st.rev++;
  assert.equal(st.stacksReady(view), false);
  st.stack.t1 = false; st.rev++;
  assert.equal(st.stacksReady(view), true, 'too big to want');
});
test('a refused pointer: the error panel asks again each minute by itself and recovers; hidden it waits; Off stops it', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), ov = ctl(w), doc = pointerDoc('2026-11-06T12:00:00Z', 2);
  ov.mount('radar');
  w.answer(/latest\.json$/, null, 503); await settle(3);
  assert.equal(ov.radarPlayer, null);
  const asked = () => w.fetches.filter((u) => /latest\.json$/.test(u)).length, one = asked();
  t.mock.timers.tick(59000); await settle(2);
  assert.equal(asked(), one, 'not before the minute');
  w.g.document.hidden = true;
  t.mock.timers.tick(1000); await settle(2);
  assert.equal(asked(), one, 'a hidden tab does not ask');
  w.g.document.hidden = false;
  t.mock.timers.tick(60000); await settle(2);
  assert.equal(asked(), one + 1, 'asked again');
  w.answer(/latest\.json$/, null, 503); await settle(3);
  assert.equal(ov.radarPlayer, null, 'refused again: still the error');
  t.mock.timers.tick(60000); await settle(2);
  assert.equal(asked(), one + 2, 'and again a minute later');
  w.answer(/latest\.json$/, doc); await settle(3);
  w.answer(/latest\.json$/, doc); await settle(3);
  assert.ok(ov.radarPlayer, 'recovered without a click');
  assert.equal(ov.radarRecoverTimer, null);
  ov.unmount();
  const w2 = world(), ov2 = ctl(w2), asked2 = () => w2.fetches.filter((u) => /latest\.json$/.test(u)).length;   // (one store per root: a new world)
  ov2.mount('radar');
  w2.answer(/latest\.json$/, null, 503); await settle(3);
  const two = asked2();
  assert.ok(ov2.radarRecoverTimer, 'armed while the error shows');
  ov2.unmount();
  assert.equal(ov2.radarRecoverTimer, null, 'Off clears it');
  t.mock.timers.tick(120000); await settle(2);
  assert.equal(asked2(), two, 'Off: nothing more');
});
test('the label says the time of the newest file in view; ages in hours past an hour; the rain word follows the number shown', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('radar');
  const real = Date.now, t0 = Date.parse('2026-11-06T12:04:00Z');
  w.answer(/latest\.json$/, pointerDoc('2026-11-06T12:00:00Z', 2)); await settle();
  const p = ov.radarPlayer, fr = p.frames[p.idx], reg = ov.radar.regionsIn(p.view());
  assert.ok(reg.indexOf('hawaii') >= 0, 'the fixture view holds Hawaii');
  for (const k of reg) if (fr.r[k]) fr.r[k].o = 0;
  fr.r.hawaii.o = 4;
  const c = ov._radarClock(Date.parse('2026-11-06T12:04:00Z')), c0 = ov._radarClock(Date.parse('2026-11-06T12:00:00Z'));
  try {
    Date.now = () => t0 + 10 * 60000;
    assert.ok(ov._radarLabel().indexOf(c.clock) >= 0, ov._radarLabel());
    assert.ok(/ 10 min ago$/.test(ov._radarLabel()), 'the age from that file: ' + ov._radarLabel());
    const regionsIn = ov.radar.regionsIn;
    fr.r.guam = { o: 1 }; ov.radar.regionsIn = () => ['hawaii', 'guam'];
    assert.ok(ov._radarLabel().indexOf(c.clock) >= 0, 'two regions in view: the newer file, ' + ov._radarLabel());
    ov.radar.regionsIn = regionsIn; delete fr.r.guam;
    fr.r.hawaii.o = 0;
    assert.ok(ov._radarLabel().indexOf(c0.clock) >= 0, ov._radarLabel());
    Date.now = () => Date.parse('2026-11-06T13:30:00Z');
    assert.ok(/ 1 h 30 min ago$/.test(ov._radarLabel()), ov._radarLabel());
    for (const k of reg) ov.radar.cov[k] = { slot: fr.t, pic: {} };
    assert.ok(/the newest frame is 1 h 30 min old\./.test(ov._radarNote()), ov._radarNote());
    fr.r.hawaii.o = 4;                                       // the note counts from the file's own time, as the label does
    assert.ok(/ 1 h 26 min ago$/.test(ov._radarLabel()), ov._radarLabel());
    assert.ok(/the newest frame is 1 h 26 min old\./.test(ov._radarNote()), ov._radarNote());
    fr.r.hawaii.o = 0;
    Date.now = real;
    p.holdFrom = Date.now();
    assert.ok(/^Loading the loop\./.test(ov._radarNote()), ov._radarNote());
    p.holdFrom = 0;
    assert.ok(!/Loading the loop/.test(ov._radarNote()), ov._radarNote());
  } finally { Date.now = real; }
  assert.equal(w.I.radarAge(59), '59 min'); assert.equal(w.I.radarAge(60), '1 h'); assert.equal(w.I.radarAge(1307), '21 h 47 min');
  const text = ov._radarReadout({ kind: 'echo', region: 'hawaii', rec: fr, dbz: 29.6 });
  assert.ok(text.startsWith('30 dBZ · moderate rain · '), text);
  ov.unmount();
});
test('a sheet with no room for the whole scale folds, without a toggle that opens nothing', async () => {
  for (const [sh, cut] of [[5000, true], [10, false]]) {
    const w = world(), ce = w.g.document.createElement;
    w.g.document.createElement = (tag) => { const e = ce(tag); e.scrollHeight = sh; return e; };
    const ov = ctl(w);
    ov._dims = () => ({ w: 400, h: 500 });
    ov.mount('radar');
    w.answer(/latest\.json$/, pointerDoc('2026-11-06T12:00:00Z', 2)); await settle();
    assert.equal(ov.ui.compact, true);
    assert.equal(!!ov.ui.collapsed, cut, 'scrollHeight ' + sh);
    assert.equal(!!findCls(ov.ui.host, 'ov-details'), !cut);
    assert.equal(!!findCls(ov.ui.host, 'ov-toggle'), !cut);
    ov.unmount();
  }
});
test('a slot joining while paused mid-loop: the map draws the frame the label names; a pointer that moves nothing keeps the hover', async (t) => {
  const w = world(), { p, st, layer, ov } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, st.frames); await settle(2);
  p.scrub(5);
  const shown = p.frames[5].t;
  assert.equal(layer.idx, 5);
  st._take(pointerDoc('2026-11-06T12:10:00Z', 14));
  st.landed('202611061210', 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  assert.equal(p.pending, null, 'joined');
  assert.equal(p.frames[p.idx].t, shown, 'the viewer keeps the frame');
  assert.equal(layer.idx, p.idx, 'the layer follows the index');
  assert.equal(st.loop[layer.idx].t, shown, 'and draws that frame');
  // the job publishes again with nothing new for this view: the step did not move, the hover stays, the tiles are not redrawn
  const el = tile(layer, 3, 28, 6); layer._tiles['3:28:6'] = { el, coords: { x: 3, y: 28, z: 6 } };
  ov.readout = { hidden: false };
  const steps = []; const orig = layer.setStep; layer.setStep = function () { steps.push(arguments); return orig.apply(this, arguments); };
  const again = pointerDoc('2026-11-06T12:10:00Z', 14); again.published_utc = '2026-11-06T12:19:30Z';
  st._take(again);
  assert.equal(steps.length, 0, 'no redraw');
  assert.equal(ov.readout.hidden, false, 'the hover stays');
  p.stop();
});
test('a paused viewer: a pointer that moves the shown frame and joins a slot at once keeps the label and the map on one frame', async () => {
  const w = world();
  const doc = pointerDoc('2026-11-06T12:10:00Z', 146, ['hawaii']);
  doc.slots = doc.slots.filter((s) => s.t !== '202611060800');                         // a hole on the 60-min grid
  const { p, st, layer } = playerWith(w, doc, 24);
  p.start();
  p.scrub(p.frames.findIndex((f) => f.t === '202611061000'));
  const next = pointerDoc('2026-11-06T12:20:00Z', 147, ['hawaii']);                    // the hole filled; the new slot has no Hawaii yet: it joins at once
  delete next.slots[next.slots.length - 1].r.hawaii; next.slots[next.slots.length - 1].r.conus = { o: 4 };
  st._take(next);
  await settle(4);
  assert.equal(p.frames[p.idx].t, '202611061000', 'the viewer keeps the frame');
  assert.equal(layer.idx, p.idx, 'the map draws the frame the label names');
  p.stop();
});
test('Play looks at the plan again first; a viewer\'s pause plans for the frame on the map; a hidden tab\'s pause does not', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st, layer } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, p.frames);
  let planned = 0, idxAt = null; const plan = st.plan;
  st.plan = function (view, frames, idx) { planned++; idxAt = idx; return plan.apply(this, arguments); };
  st.stacksReady = () => planned > 0;                                                  // ready only once the plan was looked at again
  p.scrub(5); planned = 0;
  p.play();
  assert.equal(p.holdFrom, 0, 'the fresh plan decides the hold'); assert.ok(planned > 0);
  t.mock.timers.tick(2000); await settle(1);
  const on = p.idx; assert.ok(on > 5, 'playing');
  planned = 0;
  p.toggle();
  assert.equal(p.playing, false); assert.equal(planned, 1, 'the pause plans'); assert.equal(idxAt, on, 'for the frame on the map');
  assert.equal(layer.playing, false);
  p.play(); planned = 0;
  p.visibility(true);
  assert.equal(p.playing, false); assert.equal(planned, 0, 'a hidden tab plans nothing');
  planned = 0; p.visibility(false);
  assert.equal(p.playing, true); assert.equal(planned, 0, 'a resume keeps the plan it had');
  p.stop();
});
test('another loop length while playing holds like Play; paused, or with the files in, it does not', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st, layer } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 145));
  p.start(); landAll(st, st.frames);
  let ready = true; st.stacksReady = () => ready;
  p.scrub(5); p.play(); t.mock.timers.tick(600); await settle(1);
  ready = false; const at = p.idx;
  p.setLoop(6);
  assert.ok(p.holdFrom > 0, 'holding'); assert.equal(layer.playing, false, 'drawn by what has landed');
  const shown = p.idx;
  for (let i = 0; i < 25; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.equal(p.idx, shown, 'no step during the hold');
  ready = true; for (let i = 0; i < 25; i++) { t.mock.timers.tick(60); await settle(1); }
  assert.equal(p.holdFrom, 0); assert.ok(p.idx !== shown, 'on with the loop'); assert.equal(layer.playing, true);
  assert.ok(at >= 0);
  p.setLoop(12); assert.equal(p.holdFrom, 0, 'the files are in: no hold');
  p.pause(); ready = false; p.setLoop(24); assert.equal(p.holdFrom, 0, 'paused: no hold');
  p.stop();
});
test('a read stopped and asked again starts afresh: Retry during the recovery\'s read does not get the aborted one', async () => {
  const w = world(), st = new w.I.RadarStore('https://x');
  const first = st.load(true);
  assert.equal(w.pending(/latest\.json$/).length, 1);
  st.stop();
  const second = st.load(true);
  assert.notEqual(second, first, 'its own read'); assert.equal(w.pending(/latest\.json$/).length, 1, 'the first was aborted, the second asks');
  const gotFirst = await first;
  assert.equal(gotFirst, null, 'the stopped read found nothing');
  assert.notEqual(st.status, 'failed', 'and does not decide for the new one'); assert.ok(st.loading, 'nor clear it');
  w.answer(/latest\.json$/, pointerDoc('2026-11-06T12:00:00Z', 2));
  assert.equal(await second, st); assert.equal(st.status, 'ok'); assert.equal(st.loading, null);
  st.stop();
});
test('a hidden tab asks for no pictures: no retry, no new file; shown again it goes on', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); await settle(1);
  while (w.fail(/\.png$/)) await settle(1);                                            // every file refused
  assert.ok(st.retryTimer, 'a retry is due');
  const asked = w.fetches.length;
  st.hide();
  assert.equal(st.retryTimer, null, 'the retry is cancelled');
  t.mock.timers.tick(180000); await settle(2);
  st._pump();
  assert.equal(w.fetches.filter((u) => /\.png$/.test(u)).length, w.fetches.slice(0, asked).filter((u) => /\.png$/.test(u)).length, 'nothing asked while hidden');
  st.show(); await settle(1);
  assert.ok(w.pending(/\.png$/).length > 0, 'shown again, it asks');
  p.stop();
});
test('a pending slot the pointer withdraws is forgotten', async () => {
  const w = world(), { p, st } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, st.frames);
  st._take(pointerDoc('2026-11-06T12:10:00Z', 14));
  assert.equal(p.pending.t, '202611061210');
  const back = pointerDoc('2026-11-06T12:00:00Z', 13); back.published_utc = '2026-11-06T12:20:00Z';
  st._take(back);
  assert.equal(p.pending, null);
  assert.equal(st.queue.filter((q) => q.slot === '202611061210').length, 0);
  p.stop();
});
test('the pointer: its stamp must be a time, a far-future stamp never spins, a new newest slot counts whatever the stamp, Off stops a read', async () => {
  const w = world(), I = w.I, good = pointerDoc('2026-11-06T12:00:00Z', 2);
  assert.equal(I.validRadarPointer(good), true);
  for (const bad of [undefined, 5, 'soon']) { const d = clone(good); d.published_utc = bad; assert.equal(I.validRadarPointer(d), false, String(bad)); }
  const big = clone(good); big.regions.hawaii.box = [-60, -170, 60, -100]; assert.equal(I.validRadarPointer(big), false, 'one region of 8,400 sq deg');
  const many = clone(good); for (const nm of ['aa', 'bb', 'cc', 'dd', 'ee']) many.regions[nm] = { name: nm, src: 'mrms', box: [0, 0, 50, 90] }; assert.equal(I.validRadarPointer(many), false, 'the sum');
  // far future: the wait stays at two publishes; no timer the browser would run at once
  const delays = []; const realST = w.g.setTimeout;
  const st = new I.RadarStore('https://x'); st._take(pointerDoc('2026-11-06T12:00:00Z', 2));
  st.pointer.published_utc = new Date(Date.now() + 30 * 86400000).toISOString();
  const nodeST = global.setTimeout; global.setTimeout = (f, ms) => { delays.push(ms); return nodeST(f, 0x7ffffff); };
  try { st.active = true; st._arm(); } finally { global.setTimeout = nodeST; }
  assert.ok(delays.length && delays[delays.length - 1] <= 20 * 60000, 'waited ' + delays[delays.length - 1]);
  st.stop();
  // a new newest slot with the same stamp and length
  const s2 = new I.RadarStore('https://x'); let fired = 0; s2.onPointer = () => { fired++; };
  const a = pointerDoc('2026-11-06T12:00:00Z', 5), b = pointerDoc('2026-11-06T12:10:00Z', 5); b.published_utc = a.published_utc;
  s2._take(a); const f1 = fired; s2._take(b);
  assert.equal(fired, f1 + 1, 'announced');
  // Off during a read
  const s3 = new I.RadarStore('https://x'); const rd = s3._read();
  assert.equal(w.pending(/latest/).length, 1);
  s3.stop();
  assert.equal(w.held.filter((h) => /latest/.test(h.url)).every((h) => h.done), true, 'aborted at once, not by the 15-s stall timer');
  assert.equal(await rd, false);
});
test('a picture that lands repaints the tiles in view', async () => {
  const w = world(), { p, st, layer } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 3));
  p.start();
  const x = 8, y = 54, z = 7, el = tile(layer, x, y, z);          // Hawaii's own tile: nothing landed, nothing drawn
  assert.equal(el.lastImg === undefined || el.lastImg === null, true, 'nothing yet');
  const nw = p.frames[p.idx];
  st._land({ key: st.key(nw.t, 'hawaii', 't0'), slot: nw.t, region: 'hawaii', tier: 't0' }, w.I.radarPic(t0Of('hawaii', () => 62)));
  await new Promise((r) => setTimeout(r, 40));
  assert.ok(el.lastImg, 'painted without a step or a move');
  assert.ok(el.lastImg.some((v, i) => i % 4 === 3 && v > 100), 'with its echo');
  p.stop();
});
test('the coverage mask: a pixel takes the coarse node it falls in, not the nearest one', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 1), st = storeWith(w, doc), rec = st.frames[0], box = BOXES.hawaii;
  st.landed(rec.t, 'hawaii', 't0', null, t0Of('hawaii', (r) => (r >= 50 ? 1 : 0)));    // covered from row 50 down: south of 21.0 N
  st.loop = [rec];
  const layer = layerWith(w, st, 0, false), z = 8;
  tileAt(layer, 21.03, -158, z); tileAt(layer, 20.97, -158, z);
  assert.equal(layer.readoutAt(21.03, -158, z).kind, 'none', 'row 49, 0.03 deg north of the edge');
  assert.equal(layer.readoutAt(20.97, -158, z).kind, 'dry', 'row 50, 0.03 deg south of it');
  assert.equal(box[2] - 5, 21, 'the fixture: row 50 starts at 21 N');
});
test('a cell without its t2 draws the 20-degree t1 cell that holds it, not a t1 named by its own corner', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 1, ['hawaii'], () => ({ t1: 'f' })), st = storeWith(w, doc), rec = st.frames[0];
  st.landed(rec.t, 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  st.landed(rec.t, 'hawaii', 't1', '0_-160', frame(502, 502, (r, c) => (r >= 100 && r < 400 && c >= 100 && c < 400 ? 60 : 0)));
  st.loop = [rec];
  const layer = layerWith(w, st, 0, false), el = tileAt(layer, 17, -153, 10);           // t2 is wanted at this zoom; cell 15_-155 has none
  assert.ok(el._rPieces && el._rPieces.length, 'drawn');
  assert.ok(el._rPieces.every((q) => q.ppd === 25), 'the t1 picture, not t0: ' + el._rPieces.map((q) => q.ppd));
});
test('a failed file counts as done only for its cooldown; a damaged one for five minutes', () => {
  const w = world(), st = new w.I.RadarStore(ROOT); st._take(pointerDoc('2026-11-06T12:00:00Z', 1));
  st.fails.a = { at: Date.now(), final: false }; st.fails.b = { at: Date.now() - st.retryMs - 1, final: false }; st.fails.c = { at: Date.now() - 299000, final: true }; st.fails.e = { at: Date.now() - 301000, final: true };
  assert.equal(st.done('a'), true, 'cooling down');
  assert.equal(st.done('b'), false, 'to be asked again');
  assert.equal(st.done('c'), true, 'damaged, within five minutes');
  assert.equal(st.done('e'), false, 'damaged, past five minutes: asked again');
  assert.equal(st.done('d'), false, 'never asked');
});
test('the t1 stack is refused like the t2 stack when it does not fit', () => {
  const w = world(), doc = pointerDoc('2026-11-06T12:00:00Z', 13, ['hawaii'], () => ({ t1: 'f' })), st = new w.I.RadarStore(ROOT); st._take(doc);
  st.active = true; st._pump = () => {};
  const view = { whole: false, s: 16, n: 25, w: -163, e: -152, z: 5 };
  st.plan(view, st.frames, 12, null);
  assert.equal(st.stack.t1, true, 'fits the cap');
  st.maxBytes = 2 * 1024 * 1024;
  st.plan(view, st.frames, 12, null);
  assert.equal(st.stack.t1, false, 'over a small cap');
});
test('the pointer is not read more often than its floor, even long overdue', () => {
  const w = world(), st = new w.I.RadarStore(ROOT); st._take(pointerDoc('2020-01-01T00:00:00Z', 1));
  const delays = [], nodeST = global.setTimeout;
  global.setTimeout = (f, ms) => { delays.push(ms); return nodeST(f, 0x7ffffff); };
  try { st.active = true; st._arm(); } finally { global.setTimeout = nodeST; }
  assert.equal(delays[delays.length - 1], st.pointerMs);
  st.stop();
});
test('a hidden tab playing the newest frame comes back on the newest frame, not the loop\'s start', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start(); landAll(st, p.frames); p.play();
  p.idx = p.frames.length - 1;
  p.visibility(true); p.visibility(false);
  assert.equal(p.playing, true); assert.equal(p.idx, p.frames.length - 1);
  p.stop();
});
test('a region\'s age is said past 30 minutes, not before', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('radar');
  const slot = (m) => new Date((Date.now() - m * 60000) - (Date.now() - m * 60000) % 600000).toISOString().replace('.000Z', 'Z');
  w.answer(/latest\.json$/, pointerDoc(slot(50), 2)); await settle();
  const st = ov.radar, nw = st.newestOf('hawaii'), pic = () => w.I.radarPic(t0Of('hawaii', () => 1));
  st._land({ key: st.key(nw.t, 'hawaii', 't0'), slot: nw.t, region: 'hawaii', tier: 't0' }, pic());
  assert.ok(/newest frame is \d+ min old/.test(ov._radarNote()), ov._radarNote());
  const real = Date.now, t = w.I.radarSlotMs(nw.t) + nw.r.hawaii.o * 60000;   // from the file's own time
  Date.now = () => t + 29 * 60000;
  try { assert.ok(!/min old/.test(ov._radarNote()), ov._radarNote()); } finally { Date.now = real; }
  Date.now = () => t + 31 * 60000;
  try { assert.ok(/min old/.test(ov._radarNote()), ov._radarNote()); } finally { Date.now = real; }
  ov.unmount();
});
test('the store\'s cap follows the screen: 48 MB, 32 MB under 768 px', async () => {
  for (const [width, cap] of [[1200, 48], [768, 48], [767, 32], [375, 32]]) {
    const w = world(), ov = ctl(w);
    w.map.getContainer().clientWidth = width;
    ov.mount('radar');
    w.answer(/latest\.json$/, pointerDoc('2026-11-06T12:00:00Z', 2)); await settle();
    assert.equal(ov.radar.maxBytes, cap * 1024 * 1024, width + ' px');
    ov.unmount();
  }
});
test('a region whose box moves starts over: tables, coverage and pictures', () => {
  const w = world(), a = pointerDoc('2026-11-06T12:00:00Z', 2, ['hawaii', 'guam']), st = storeWith(w, a), rec = st.frames[1];
  st.landed(rec.t, 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  st.landed(rec.t, 'guam', 't0', null, t0Of('guam', () => 1));
  st._table('hawaii', 't2'); st._table('guam', 't2');
  const before = st.bytes, rev = st.covRev;
  const b = clone(a); b.published_utc = '2026-11-06T12:09:30Z'; b.regions.hawaii.box = [15, -164, 26, -150];
  st._take(b);
  assert.equal(st.pics.has(st.key(rec.t, 'hawaii', 't0')), false);
  assert.equal(st.pics.has(st.key(rec.t, 'guam', 't0')), true, 'the other region stays');
  assert.equal(st.cov.hawaii, undefined); assert.ok(st.cov.guam);
  assert.ok(st.bytes < before); assert.ok(st.covRev > rev);
  assert.equal(st.tables['hawaii|t2'], undefined); assert.ok(st.tables['guam|t2']);
});
test('the readout and the label', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('radar');
  const end = Date.now() - 10 * 60000, iso = new Date(end - end % 600000).toISOString().replace('.000Z', 'Z');
  const doc = pointerDoc(iso, 2, ['hawaii', 'europe']);
  w.answer(/latest\.json$/, doc); await settle();
  const rec = ov.radar.frames[1];
  assert.equal(ov._radarReadout(null), null);
  assert.equal(ov._radarReadout({ kind: 'none' }), 'no radar coverage');
  assert.equal(ov._radarReadout({ kind: 'dry', region: 'hawaii', rec }), 'no echo');
  const a = ov._radarReadout({ kind: 'echo', dbz: 32, region: 'hawaii', rec });
  assert.ok(/^32 dBZ · moderate rain · \d/.test(a), a); assert.ok(!/max/.test(a));
  const b = ov._radarReadout({ kind: 'echo', dbz: 32, region: 'europe', rec });
  assert.ok(/10-min max to/.test(b), b);
  assert.ok(/HST · \d+ min ago$/.test(ov._radarLabel()), ov._radarLabel());
  ov.unmount();
});
test('the scale in dBZ is the same for both unit systems; the slot names read back', () => {
  const us = I.legendTicks('radar', I.RADAR_FDEF, 'US'), metric = I.legendTicks('radar', I.RADAR_FDEF, 'Metric');
  assert.deepEqual(us, metric);
  assert.ok(us.length >= 3);
  assert.equal(I.unitOf('radar', 'US').label, 'dBZ');
  assert.equal(slotName(Date.parse('2026-11-06T12:00:00Z')), '202611061200');
  assert.equal(I.radarSlotMs('202611061200'), Date.parse('2026-11-06T12:00:00Z'));
});
