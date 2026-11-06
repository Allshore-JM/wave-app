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
function bilinear(pic, fy, fx) {
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
  st.landed = (slot, region, tier, name, fr) => st._land({ key: st.key(slot, region, tier, name), slot, region, tier }, w.I.radarPic(fr));
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
  assert.deepEqual([5.99, 6, 8.99, 9, 12].map(I.radarWantTier), [0, 1, 1, 2, 2]);
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
    assert.deepEqual([pic.cols, pic.rows], [502, 502]);
    for (const [r, c, v] of EXPECT[tier].echo) assert.equal(pic.q[r * 502 + c], v, tier + ' ' + r + ',' + c);
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
  // t0 at tile zoom 4: an echo node's pixel, against the reference
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
    if (check(4, lat, lon, t0, (la, lo) => [(box[2] - la) * 10 - 0.5, (lo - box[1]) * 10 - 0.5])) checked++;
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
  // t1 at tile zoom 7
  const nm1 = EXPECT.t1.name.split('_').map(Number); checked = 0;
  for (const [r, c] of EXPECT.t1.echo) {
    const lat = nm1[0] + 20 + 0.5 / 25 - r / 25, lon = nm1[1] - 0.5 / 25 + c / 25;
    if (lat < box[0] || lat >= box[2] || lon < nm1[1] || lon >= nm1[1] + 20) continue;
    if (check(7, lat, lon, t1, (la, lo) => [(nm1[0] + 20 - la) * 25 + 0.5, (lo - nm1[1]) * 25 + 0.5])) checked++;
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
test('a step whose pictures are missing waits a second at most, then moves on', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'], now: Date.parse('2026-11-06T12:08:00Z') });
  const w = world(), { p, st } = playerWith(w, pointerDoc('2026-11-06T12:00:00Z', 13));
  p.start();
  for (const rec of [p.frames[12], p.frames[0]]) st.landed(rec.t, 'hawaii', 't0', null, t0Of('hawaii', () => 1));
  p.play();
  t.mock.timers.tick(500); await settle(1);
  assert.equal(p.idx, 0, 'frame 1 not here: wait');
  for (let i = 0; i < 18; i++) { t.mock.timers.tick(60); await settle(1); }
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
