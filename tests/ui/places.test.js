'use strict';
// static_ui/places.js (plan section 51, steps 4 and 5): the lines-v1 decoder (a JS mirror of the builder's
// encoder, refusals, two real published cells against the Python decoder), the cells of a tile, the store (tier 0 /
// cells / stand-in, at most two downloads, LRU, final vs cooldown failures, idle), the per-kind and per-line zoom gates,
// the tile's paths (world copies, margins, the dateline guard), the drawing order, and create(): the pane, the settings,
// nothing fetched before the page is ready or while every kind is off.   node --test tests/ui/places.test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { encodeLines, encodePlaces, leb, zz } = require('./placesenc.js');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'places.js'), 'utf8');
function load(win) { new Function('window', SRC)(win); return win.AllshorePlaces; }
const P = load({});
const I = P._internals;
const FIX = path.join(__dirname, '..', 'fixtures', 'places');
const settle = async () => { for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r)); };
const D = (deg) => Math.round(deg * 10000);
const ab = (b) => b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength);
// the tile holding (lat, lon) at zoom z
const tileOf = (lat, lon, z) => ({ z, x: Math.floor(I.lonToX(lon) * 2 ** z / 256), y: Math.floor(I.latToY(lat) * 2 ** z / 256) });

test('the decoder: a round trip of the builder\'s format (kinds, zooms, relative first points, zoom-0 pixels)', () => {
  const buf = encodeLines([
    [0, [[[D(-120), D(-119.5)], [D(42), D(42)], 20], [[D(-119.5), D(-119)], [D(42), D(41.5)], 86]]],
    [1, [[[D(-115), D(-114), D(-113)], [D(35), D(36), D(35.5)], 30]]],
    [2, [[[D(179), D(179.9999)], [D(60), D(61)], 0]]]
  ]);
  const d = I.decodeLines(buf);
  assert.equal(d.n, 3); assert.equal(d.cell, 10); assert.equal(d.q, 10000); assert.equal(d.kinds, 7);
  assert.deepEqual(Array.from(d.kind), [0, 1, 2]);
  assert.deepEqual(Array.from(d.lineStart), [0, 2, 3, 4]); assert.deepEqual(Array.from(d.vertStart), [0, 2, 4, 7, 9]);
  assert.deepEqual(Array.from(d.mz), [20, 86, 30, 0]);
  const at = (v) => [d.xy[2 * v], d.xy[2 * v + 1]];
  assert.ok(Math.abs(at(0)[0] - I.lonToX(-120)) < 1e-9 && Math.abs(at(0)[1] - I.latToY(42)) < 1e-9);
  assert.ok(Math.abs(at(3)[0] - I.lonToX(-119)) < 1e-9 && Math.abs(at(3)[1] - I.latToY(41.5)) < 1e-9, 'the second line starts from the first line\'s end');
  assert.ok(Math.abs(at(6)[0] - I.lonToX(-113)) < 1e-9);
  assert.ok(Math.abs(d.box[0] - I.lonToX(-120)) < 1e-9 && Math.abs(d.box[1] - I.latToY(42)) < 1e-9 && Math.abs(d.box[3] - I.latToY(41.5)) < 1e-9, 'box: west, north (y0), east, south');
  assert.equal(I.unzz(0), 0); assert.equal(I.unzz(1), -1); assert.equal(I.unzz(2), 1); assert.equal(I.unzz(4294967295), -2147483648);
  const big = new Float64Array(2); assert.equal(I.readVarints(new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x0f, 0x05]), 0, 6, big), 2); assert.equal(big[0], 4294967295);
});

test('the decoder refuses damaged or foreign files', () => {
  const good = encodeLines([[0, [[[D(1), D(2)], [D(1), D(2)], 0]]]]);
  const bad = (mut) => { const b = good.slice(0); mut(new DataView(b), new Uint8Array(b)); return b; };
  assert.throws(() => I.decodeLines(new ArrayBuffer(10)), /too short/);
  assert.throws(() => I.decodeLines(bad((dv, u8) => { u8[0] = 88; })), /not a lines file/);
  assert.throws(() => I.decodeLines(bad((dv) => dv.setUint32(8, 77, true))), /bad header/);
  assert.throws(() => I.decodeLines(bad((dv) => dv.setUint32(16, 5, true))), /bad header|counts|more lines/);
  assert.throws(() => I.decodeLines(good.slice(0, good.byteLength - 1)), /stream|counts/);
  const cut = new Uint8Array(good.slice(0)); cut[cut.length - 1] |= 0x80;
  assert.throws(() => I.decodeLines(cut.buffer), /inside a varint/);
  assert.throws(() => I.decodeLines(encodeLines([[3, [[[D(1), D(2)], [D(1), D(2)], 0]]]])), /unknown kind/);
  assert.throws(() => I.decodeLines(encodeLines([[0, [[[D(1)], [D(1)], 0]]]])), /bad line/);
  assert.throws(() => I.decodeLines(encodeLines([[0, [[[D(1), D(2)], [D(1), D(2)], 300]]]])), /bad line/);
  assert.throws(() => I.decodeLines(encodeLines([[0, [[[D(179), D(181)], [D(1), D(2)], 0]]]])), /out of range/);
  assert.throws(() => I.readVarints(new Uint8Array([0x80, 0x80, 0x80, 0x80, 0x80, 0x01]), 0, 6, new Float64Array(2)), /longer than 5 bytes/);
  const tail = new Uint8Array(good.byteLength + 1); tail.set(new Uint8Array(good)); tail[good.byteLength] = 5;
  assert.throws(() => I.decodeLines(tail.buffer), /counts/);
});

test('two published cells (static/places/v1/c/40_-120 and 30_70) decode to what the builder\'s own decoder reads', () => {
  const expect = JSON.parse(fs.readFileSync(path.join(FIX, 'lines_expect.json'), 'utf8'));
  for (const name of ['40_-120', '30_70']) {
    const e = expect[name], d = I.decodeLines(ab(fs.readFileSync(path.join(FIX, name + '.l.bin'))));
    assert.equal(d.cell, e.cell); assert.equal(d.q, e.q); assert.equal(d.kinds, e.kinds); assert.equal(d.n, e.pieces.length);
    e.pieces.forEach((pc, p) => {
      assert.equal(d.kind[p], pc.kind, name + ' piece ' + p);
      assert.ok(Math.abs(d.box[4 * p] - I.lonToX(pc.b[0] / e.q)) < 1e-9 && Math.abs(d.box[4 * p + 3] - I.latToY(pc.b[1] / e.q)) < 1e-9);
      pc.lines.forEach((ln, j) => {
        const l = d.lineStart[p] + j, v0 = d.vertStart[l], v1 = d.vertStart[l + 1];
        assert.equal(v1 - v0, ln[0]); assert.equal(d.mz[l], ln[1]);
        assert.ok(Math.abs(d.xy[2 * v0] - I.lonToX(ln[2][0] / e.q)) < 1e-9 && Math.abs(d.xy[2 * v0 + 1] - I.latToY(ln[2][1] / e.q)) < 1e-9, name + ' first point');
        assert.ok(Math.abs(d.xy[2 * v1 - 2] - I.lonToX(ln[3][0] / e.q)) < 1e-9 && Math.abs(d.xy[2 * v1 - 1] - I.latToY(ln[3][1] / e.q)) < 1e-9, name + ' last point');
      });
    });
  }
  assert.ok(I.decodeLines(ab(fs.readFileSync(path.join(FIX, '30_70.l.bin')))).kind.includes(I.KIND_DISPUTED), 'Kashmir\'s line of control is kind 2');
});

test('the cells of a tile: 10-degree south-west corners, world copies wrapped, the poles left out', () => {
  assert.deepEqual(I.cellsForTile({ z: 3, x: 1, y: 3 }, 10).sort(), I.cellsForTile({ z: 3, x: 9, y: 3 }, 10).sort(), 'a world copy');
  assert.deepEqual(I.cellsForTile(tileOf(21.5, -158, 8), 10), ['20_-160'], 'Oahu at zoom 8');
  const corner = I.cellsForTile(tileOf(40.01, -119.99, 6), 10).sort();
  assert.ok(corner.includes('40_-120') && corner.includes('30_-120'), 'a tile across 40 N lists both rows: ' + corner);
  const z0 = I.cellsForTile({ z: 0, x: 0, y: 0 }, 10);
  assert.equal(z0.length, 18 * 36, 'the whole world between 85 S and 85 N: rows -90 (Antarctica) to 80, 36 columns');
  assert.ok(z0.includes('-90_0') && !z0.some((n) => n.startsWith('90_')));
  const south = I.cellsForTile({ z: 3, x: 4, y: 4 }, 10);                     // its north edge is the equator
  assert.ok(south.length && south.every((n) => n.startsWith('-')), 'a tile whose north edge lies on a cell line takes no cell north of it: ' + south);
  const edge = I.cellsForTile({ z: 5, x: 0, y: 12 }, 10);                     // starts at -180
  assert.ok(edge.some((n) => n.endsWith('_-180')) && edge.every((n) => n.endsWith('_-180') || n.endsWith('_-170')), 'the first tile (-180..-168.75): ' + edge);
});

test('the zoom gates: countries from tile zoom 3, states from 5, each line from its own zoom (tile zoom z serves map zooms below z + 0.5)', () => {
  const both = { borders: true, states: true };
  assert.deepEqual(I.kindsAt(2, both), {}); assert.deepEqual(I.kindsAt(3, both), { 0: true, 2: true }); assert.deepEqual(I.kindsAt(4, both), { 0: true, 2: true });
  assert.deepEqual(I.kindsAt(5, both), { 0: true, 1: true, 2: true });
  assert.deepEqual(I.kindsAt(6, { borders: false, states: true }), { 1: true }); assert.deepEqual(I.kindsAt(6, { borders: true, states: false }), { 0: true, 2: true });
  assert.equal(I.lineShown(74, 7), true); assert.equal(I.lineShown(75, 7), false); assert.equal(I.lineShown(75, 8), true);
  assert.equal(I.lineShown(0, 0), true); assert.equal(I.lineShown(34, 3), true); assert.equal(I.lineShown(35, 3), false, 'zoom 3.5 rounds to tile zoom 4');
});

test('a tile\'s paths: the pieces it meets, in tile pixels, per kind; world copies identical; a margin reaches a line just beyond the edge; per-line zooms; the dateline guard', () => {
  const set = I.decodeLines(encodeLines([
    [0, [[[D(-120), D(-110)], [D(40), D(40)], 0]]],                           // a border along 40 N
    [1, [[[D(-115), D(-115)], [D(35), D(45)], 0]]],                           // a state line along 115 W
    [1, [[[D(-114), D(-114)], [D(35), D(45)], 60]]],                          // a state line Natural Earth shows from zoom 6
    [0, [[[D(10), D(20)], [D(10), D(10)], 0]]]                                // far away
  ]));
  const z = 4, n = 16, all = { 0: true, 1: true, 2: true };
  const t = tileOf(40, -115, z);                                               // the border runs 14.6 px below this tile's top
  assert.deepEqual(t, { z, x: 2, y: 6 });
  const p = I.tilePaths(t, [set], all, 0);
  assert.equal(p[0].length, 1, 'the border'); assert.equal(p[1].length, 1, 'the zoom-0 state line only (the zoom-6 one waits)');
  const yb = I.latToY(40) * n - t.y * 256, xs = I.lonToX(-115) * n - t.x * 256;
  assert.ok(Math.abs(p[0][0][1] - yb) < 1e-9 && Math.abs(yb - 14.6) < 0.1);
  assert.ok(Math.abs(p[1][0][0] - xs) < 1e-9 && p[1][0].length === 4);
  const above = { z, x: 2, y: 5 };                                             // the border lies 14.6 px beyond its bottom edge
  assert.equal(I.tilePaths(above, [set], all, 3)[0], undefined, 'a 3-px margin does not reach 14.6 px');
  assert.equal(I.tilePaths(above, [set], all, 16)[0].length, 1, 'a 16-px margin does');
  assert.deepEqual(I.tilePaths({ z, x: t.x - n, y: t.y }, [set], all, 0), p, 'one world west');
  assert.deepEqual(I.tilePaths({ z, x: t.x + 2 * n, y: t.y }, [set], all, 0), p, 'two worlds east');
  assert.deepEqual(Object.keys(I.tilePaths(t, [set], { 0: true }, 0)), ['0'], 'states not wanted');
  assert.equal(I.tilePaths(tileOf(40, -114, 6), [set], all, 0)[1].length, 2, 'at tile zoom 6 the zoom-6 state line shows too (one tile holds both)');
  assert.equal(I.tilePaths(tileOf(40, -114, 5), [set], all, 0)[1].length, 1, 'at tile zoom 5 only the zoom-0 one');
  // a line drawn across the world (never written by the builder): split there, nothing drawn across
  const wide = I.decodeLines(encodeLines([[0, [[[D(-179), D(179), D(178)], [D(0), D(0.5), D(1)], 0]]]], 10));
  const w0 = I.tilePaths({ z: 0, x: 0, y: 0 }, [wide], all, 0);
  assert.ok(w0[0].length >= 1 && w0[0].every((pts) => { for (let i = 2; i < pts.length; i += 2) if (Math.abs(pts[i] - pts[i - 2]) > 128) return false; return true; }));
  // sub-pixel steps dropped at a low zoom, every step kept at a high one
  const dense = I.decodeLines(encodeLines([[0, [[Array.from({ length: 50 }, (_, i) => D(10 + i * 0.001)), Array.from({ length: 50 }, () => D(10)), 0]]]]));
  assert.ok(I.tilePaths(tileOf(10, 10.02, 3), [dense], all, 0)[0][0].length < 10);
  assert.equal(I.tilePaths(tileOf(10, 10.0245, 18), [dense], all, 300)[0][0].length, 100);
});

test('the drawing: halos of every kind first, then the cores; countries over states; disputed and state lines dashed', () => {
  const calls = [];
  const ctx = { set lineWidth(v) { calls.push(['w', v]); }, set strokeStyle(v) { calls.push(['s', v]); }, set lineJoin(v) {}, set lineCap(v) {},
    setLineDash(d) { calls.push(['d', d.join(',')]); }, beginPath() {}, moveTo() {}, lineTo() {}, stroke() { calls.push(['stroke']); } };
  I.drawTile(ctx, { 0: [[0, 0, 10, 10]], 1: [[0, 0, 5, 5]], 2: [[1, 1, 2, 2]] });
  const strokes = []; let w = null, s = null, d = null;
  calls.forEach((c) => { if (c[0] === 'w') w = c[1]; else if (c[0] === 's') s = c[1]; else if (c[0] === 'd') d = c[1]; else strokes.push({ w, s, d }); });
  assert.equal(strokes.length, 6);
  assert.deepEqual(strokes.map((x) => x.w), [2, 2.5, 2.5, 1, 1, 1], 'halos (states 2 px, borders 2.5 px) then cores');
  assert.deepEqual(strokes.map((x) => x.d), ['4,3', '6,4', '', '4,3', '6,4', ''], 'states, disputed, settled: the settled border last (on top)');
  assert.ok(strokes.slice(0, 3).every((x) => /^rgba\(0,0,0/.test(x.s)) && strokes.slice(3).every((x) => /^rgba\(255,255,255/.test(x.s)));
  assert.equal(calls[calls.length - 1][0], 'd', 'the dash is reset at the end');
});

// ---- the store ----
const LINES0 = encodeLines([[0, [[[D(-120), D(-110)], [D(40), D(40)], 0]]]]);
const CELL_A = encodeLines([[0, [[[D(-120), D(-115)], [D(40), D(40)], 0]]]]);
const PLACES0 = encodePlaces([{ lat: 35.687, lon: 139.75, rank: 0, mz10: 17, pop: 35676000, flags: 1, name: 'Tokyo' },
  { lat: 21.303, lon: -157.858, rank: 1, mz10: 40, pop: 786000, flags: 0, name: 'Honolulu' }, { lat: 34.049, lon: -118.232, rank: 2, mz10: 20, pop: 12500000, flags: 0, name: 'Los Angeles' },
  { lat: 21.503, lon: -158.024, rank: 3, mz10: 67, pop: 174766, flags: 0, name: 'Wahiawa' }, { lat: 19.706, lon: -155.082, rank: 4, mz10: 51, pop: 52391, flags: 0, name: 'Hilo' }]);
const CELL_P = encodePlaces([{ lat: 21.4, lon: -157.74, rank: 4, mz10: 80, pop: 38635, flags: 2, name: 'Kailua' }, { lat: 21.59, lon: -158.1, rank: 5, mz10: 100, pop: 3970, flags: 2, name: 'Hale\u2018iwa' }]);
const INDEX = { format: 'places-v1', tier0: { lines: { file: 'world-lines.bin' }, places: { file: 'world-places.bin' } },
  tier1: { dir: 'c', cell: 10, min_zoom: 8, cells: { '40_-120': [CELL_A.byteLength, 0], '30_-120': [0, 50], '20_-160': [0, CELL_P.byteLength] } } };
const TIER0 = { 'index.json': { json: INDEX }, 'world-lines.bin': LINES0, 'world-places.bin': PLACES0 };
function fakeFetch(answers) {
  const asked = [], held = [];
  const f = (url, init) => {
    asked.push(url);
    const name = url.split('/static/places/v1/')[1];
    const a = answers[name];
    const p = new Promise((res, rej) => {
      const go = () => {
        if (init && init.signal && init.signal.aborted) return rej(new Error('aborted'));
        if (!a) return res({ ok: false, status: 404 });
        if (a === 'down') return res({ ok: false, status: 503 });
        if (a.json) return res({ ok: true, status: 200, headers: { get: () => '100' }, json: () => Promise.resolve(a.json) });
        return res({ ok: true, status: 200, headers: { get: () => String(a.byteLength) }, arrayBuffer: () => Promise.resolve(a) });
      };
      if (answers.hold) held.push(go); else go();
    });
    return p;
  };
  f.asked = asked; f.held = held;
  return f;
}
const BASE = 'https://m/static/places/v1';

test('the store: index + tier 0 at load; tier 0 up to zoom 7; from zoom 8 the tile\'s cells (none for a cell without lines), tier 0 standing in while they load', async () => {
  const f = fakeFetch({ ...TIER0, 'c/40_-120.l.bin': CELL_A });
  const st = new I.PlacesStore(BASE, f);
  assert.deepEqual(st.linesFor({ z: 3, x: 1, y: 3 }), { sets: [], complete: false }, 'not loaded: nothing');
  assert.equal(await st.load(), st);
  assert.deepEqual(f.asked, [BASE + '/index.json', BASE + '/world-lines.bin', BASE + '/world-places.bin']);
  assert.equal(st.status, 'ok'); assert.equal(st.lines0.n, 1); assert.equal(st.places0.n, 5);
  const t7 = { z: 7, x: 21, y: 48 };
  assert.deepEqual(st.linesFor(t7), { sets: [st.lines0], complete: true });
  let changes = 0; st.onChange = () => changes++;
  const t8 = tileOf(42, -119, 8);
  assert.deepEqual(I.cellsForTile(t8, 10), ['40_-120']);
  assert.deepEqual(st.linesFor(t8), { sets: [st.lines0], complete: false }, 'tier 0 stands in while the cell loads');
  await settle();
  const second = st.linesFor(t8);
  assert.equal(second.complete, true); assert.equal(second.sets.length, 1);
  assert.ok(second.sets[0] !== st.lines0 && second.sets[0].n === 1, 'the cell, not tier 0');
  assert.equal(changes, 1, 'one change: the cell landed');
  assert.deepEqual(st.linesFor(tileOf(35.5, -119.5, 8)), { sets: [], complete: true }, 'a cell the index lists without lines: nothing to draw, nothing asked');
  assert.ok(!f.asked.some((u) => /30_-120\.l\.bin/.test(u)), 'a cell without lines is never asked');
});

test('the store: at most two cell downloads at once; a 404 is final (tier 0 stays), a 503 is asked again after the cooldown; idle stops the queue', async () => {
  const cells = {}; ['40_-120', '40_-110', '40_-100', '40_-90'].forEach((n) => { cells[n] = [10, 0]; });
  const idx = { ...INDEX, tier1: { ...INDEX.tier1, cells } };
  const f = fakeFetch({ ...TIER0, hold: true, 'index.json': { json: idx }, 'c/40_-120.l.bin': CELL_A, 'c/40_-110.l.bin': 'down' });
  const st = new I.PlacesStore(BASE, f);
  const loading = st.load(); while (f.held.length) f.held.shift()(); await settle(); while (f.held.length) f.held.shift()(); await loading;
  st.request(['40_-120.l', '40_-110.l', '40_-100.l', '40_-90.l']);
  assert.equal(Object.keys(st.inflight).length, I.MAX_INFLIGHT); assert.equal(st.queue.length, 2);
  while (f.held.length) f.held.shift()(); await settle();
  assert.ok(st.cells.has('40_-120.l')); assert.equal(typeof st.failed['40_-110.l'], 'number', '503: a cooldown');
  while (f.held.length) f.held.shift()(); await settle();
  assert.equal(st.failed['40_-100.l'], true, '404: final'); assert.equal(st.failed['40_-90.l'], true);
  st.request(['40_-100.l', '40_-110.l']); assert.equal(st.queue.length + Object.keys(st.inflight).length, 0, 'neither asked again now');
  // a tile over a final cell keeps tier 0, complete
  const t = tileOf(45, -95, 8);
  assert.deepEqual(I.cellsForTile(t, 10), ['40_-100']);
  assert.deepEqual(st.linesFor(t), { sets: [st.lines0], complete: true }, 'over a cell that cannot be served: tier 0 for good');
  assert.deepEqual(I.cellsForTile(tileOf(45, -105, 8), 10), ['40_-110']);
  assert.deepEqual(st.linesFor(tileOf(45, -105, 8)), { sets: [st.lines0], complete: false }, 'over a cell in its cooldown: tier 0, not final');
  st.request(['40_-120.l']); assert.equal(st.queue.length, 0, 'already decoded');
  // a cell file of another cell size is refused for good (tier 0 there)
  const other = encodeLines([[0, [[[D(-79), D(-78)], [D(41), D(41)], 0]]]], 5);
  const g = fakeFetch({ ...TIER0, 'index.json': { json: { ...INDEX, tier1: { ...INDEX.tier1, cells: { '40_-80': [10, 0] } } } }, 'c/40_-80.l.bin': other });
  const so = new I.PlacesStore(BASE, g); await so.load(); so.request(['40_-80.l']); await settle();
  assert.equal(so.failed['40_-80.l'], true); assert.equal(so.cells.size, 0);
  st.queue.push('40_-90x'); st.idle(); assert.equal(st.queue.length, 0); assert.equal(st.retryTimer, null);
});

test('the store: a failed load answers null and is retried only after the cooldown; an index of another kind is refused; the LRU keeps 6 MB', async () => {
  const f = fakeFetch({ 'index.json': 'down' });
  const st = new I.PlacesStore(BASE, f);
  assert.equal(await st.load(), null); assert.equal(st.status, 'failed');
  assert.equal(await st.load(), null); assert.equal(f.asked.length, 1, 'no second request inside the cooldown');
  st.failedAt -= I.RETRY_MS + 1; await st.load(); assert.equal(f.asked.length, 2, 'retried after it');
  const g = fakeFetch({ ...TIER0, 'index.json': { json: { ...INDEX, format: 'coast-v1' } } });
  assert.equal(await new I.PlacesStore(BASE, g).load(), null);
  assert.equal(I.validIndex({ ...INDEX, tier1: { ...INDEX.tier1, cell: 5 } }), false); assert.equal(I.validIndex({ ...INDEX, tier1: { ...INDEX.tier1, dir: '../x' } }), false);
  assert.equal(I.validIndex({ ...INDEX, tier0: { ...INDEX.tier0, lines: { file: 'a/b.bin' } } }), false); assert.equal(I.validIndex({ ...INDEX, tier0: { lines: INDEX.tier0.lines } }), false); assert.equal(I.validIndex(INDEX), true);
  const lru = new I.PlacesStore(BASE, f);
  for (let i = 0; i < 10; i++) { lru.cells.set('c' + i, { bytes: 1 << 20 }); lru.bytes += 1 << 20; lru._trim(); }
  assert.ok(lru.bytes <= I.MAX_CELL_BYTES_KEPT); assert.ok(lru.cells.has('c9') && !lru.cells.has('c0'));
});

// ---- create() ----
// an element: style, class, text, children; never innerHTML
function el(tag) {
  const e = { tag, style: {}, className: '', textContent: '', hidden: false, children: [], attrs: {},
    setAttribute(k, v) { this.attrs[k] = v; }, appendChild(c) { this.children.push(c); return c; }, getContext: () => null };
  return e;
}
const fakeDoc = (mk) => ({ createElement: (tag) => (mk && tag === 'canvas' ? mk() : el(tag)) });
// a map at zoom z centred on (lat, lon), size x by y; the panes sit 100 x 50 px off the container (layer != container points)
function fakeMap(z, lat, lon, sx, sy) {
  const panes = {}, layers = new Set(), ev = {}, size = { x: sx || 800, y: sy || 600 }, controls = [], credits = [];
  const m = { zoom: z, center: { lat: lat === undefined ? 0 : lat, lon: lon === undefined ? 0 : lon }, size, controls, credits,
    getZoom() { return this.zoom; }, getSize: () => size,
    getPane: (n) => panes[n], createPane: (n) => (panes[n] = { style: {}, children: [], appendChild(c) { this.children.push(c); return c; } }), panes,
    hasLayer: (l) => layers.has(l), removeLayer: (l) => { layers.delete(l); }, _add: (l) => layers.add(l), layers,
    on: (e, f) => { e.split(' ').forEach((n) => { (ev[n] = ev[n] || []).push(f); }); }, fire: (e) => (ev[e] || []).forEach((f) => f()),
    _wpx() { const n = 2 ** this.zoom; return { x: I.lonToX(this.center.lon) * n, y: I.latToY(this.center.lat) * n, n }; },
    latLngToContainerPoint(ll) { const c = this._wpx(); return { x: I.lonToX(ll[1]) * c.n - c.x + size.x / 2, y: I.latToY(ll[0]) * c.n - c.y + size.y / 2 }; },
    latLngToLayerPoint(ll) { const p = this.latLngToContainerPoint(ll); return { x: p.x - 100, y: p.y - 50 }; },
    getBounds() {
      const c = this._wpx(), x0 = (c.x - size.x / 2) / c.n, x1 = (c.x + size.x / 2) / c.n, y0 = (c.y - size.y / 2) / c.n, y1 = (c.y + size.y / 2) / c.n;
      return { getWest: () => x0 / 256 * 360 - 180, getEast: () => x1 / 256 * 360 - 180, getNorth: () => I.yToLat(y0), getSouth: () => I.yToLat(y1) };
    },
    getContainer: () => ({ getBoundingClientRect: () => ({ left: 10, top: 20 }), querySelectorAll: () => controls }),
    attributionControl: { addAttribution: (t) => credits.push('+' + t.slice(0, 7)), removeAttribution: (t) => credits.push('-' + t.slice(0, 7)) } };
  return m;
}
function fakeL(map) {
  return { GridLayer: { extend(p) { function C(o) { this.options = o; this._tiles = {}; } C.prototype = Object.assign({ addTo(m) { m._add(this); return this; } }, p); return C; } } };
}
function storage(init) { const m = new Map(Object.entries(init || {})); return { m, getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)) }; }

test('create(): its own pane (z 340, no pointer events); both kinds on by default; nothing fetched before the page is ready, below zoom 3, or with both kinds off; the settings stored', async () => {
  const map = fakeMap(6), f = fakeFetch(TIER0);
  let start = null;
  const s = storage();
  const pl = P.create(map, { base: BASE, L: fakeL(map), document: fakeDoc(), storage: s, fetch: f, whenReady: (go) => { start = go; } });
  assert.equal(map.panes.placesLinePane.style.zIndex, 340); assert.equal(map.panes.placesLinePane.style.pointerEvents, 'none');
  assert.equal(map.panes.placesLabelPane.style.zIndex, 360); assert.equal(map.panes.placesLabelPane.style.pointerEvents, 'none');
  assert.equal(map.panes.placesLabelPane.children[0], pl.labels); assert.equal(pl.labels.attrs['aria-hidden'], 'true');
  assert.equal(pl.on('borders'), true); assert.equal(pl.on('states'), true); assert.equal(pl.on('places'), true);
  assert.ok(map.hasLayer(pl.layer));
  assert.equal(f.asked.length, 0, 'nothing before the page is ready');
  assert.deepEqual(map.credits, ['+Borders', '+Places:'], 'the credits at once');
  start(); await settle();
  assert.deepEqual(f.asked, [BASE + '/index.json', BASE + '/world-lines.bin', BASE + '/world-places.bin']);
  pl.set('borders', false); pl.set('states', false);
  assert.equal(s.m.get('allshore.borders.v1'), '0'); assert.equal(s.m.get('allshore.states.v1'), '0');
  assert.ok(map.hasLayer(pl.layer), 'places still on: the layer stays'); assert.deepEqual(map.credits.slice(2), ['-Borders'], 'the borders credit goes once');
  pl.set('places', false);
  assert.ok(!map.hasLayer(pl.layer), 'every kind off: the layer goes'); assert.equal(s.m.get('allshore.places.v1'), '0'); assert.deepEqual(map.credits.slice(3), ['-Places:']);
  pl.set('states', true); assert.ok(map.hasLayer(pl.layer)); assert.equal(s.m.get('allshore.states.v1'), '1'); assert.deepEqual(map.credits.slice(4), ['+Borders']);
  pl.set('nope', true); assert.equal(s.m.has('nope'), false);
  // a fresh page at zoom 2 with the saved settings: nothing until zoom 3 (borders) / 5 (states only) / 2 (places)
  const map2 = fakeMap(1), f2 = fakeFetch(TIER0);
  const pl2 = P.create(map2, { base: BASE, L: fakeL(map2), document: fakeDoc(), storage: storage({ 'allshore.borders.v1': '0', 'allshore.places.v1': '0' }), fetch: f2, whenReady: (go) => go() });
  assert.equal(pl2.on('borders'), false); assert.equal(pl2.on('states'), true); assert.equal(pl2.on('places'), false);
  await settle(); assert.equal(f2.asked.length, 0);
  map2.zoom = 4; map2.fire('zoomend'); await settle(); assert.equal(f2.asked.length, 0, 'states only: not before zoom 5');
  map2.zoom = 5; map2.fire('zoomend'); await settle(); assert.equal(f2.asked.length, 3);
  const map3 = fakeMap(1), f3 = fakeFetch(TIER0);
  P.create(map3, { base: BASE, L: fakeL(map3), document: fakeDoc(), storage: storage({ 'allshore.borders.v1': '0', 'allshore.states.v1': '0' }), fetch: f3, whenReady: (go) => go() });
  await settle(); assert.equal(f3.asked.length, 0, 'places only: not at zoom 1');
  map3.zoom = 2; map3.fire('zoomend'); await settle(); assert.equal(f3.asked.length, 3, 'places from zoom 2');
  // a storage that throws: on, and changes never throw
  const pl3 = P.create(fakeMap(6), { base: BASE, L: fakeL(fakeMap(6)), document: fakeDoc(), storage: { getItem() { throw new Error('x'); }, setItem() { throw new Error('x'); } }, whenReady: () => {} });
  assert.equal(pl3.on('borders'), true); pl3.set('borders', false); assert.equal(pl3.on('borders'), false);
  // without a base the rows still work and nothing loads
  const pl4 = P.create(fakeMap(6), { L: fakeL(fakeMap(6)), document: fakeDoc(), whenReady: (go) => go() });
  assert.equal(pl4.store, null); assert.equal(pl4.wanted(), false);
});

test('create(): tiles painted from the store: halo and core strokes for a tile with a border, a blank tile while loading, redrawn when the data lands', async () => {
  const map = fakeMap(4), f = fakeFetch({ ...TIER0, hold: true });
  const strokes = [];
  const mkCanvas = () => { const c = { width: 0, height: 0, style: {} }; c.getContext = () => ({ setTransform() {}, clearRect() { strokes.push('clear'); }, setLineDash() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() { strokes.push('stroke'); }, measureText: (t) => ({ width: t.length * 6 }) }); return c; };
  const pl = P.create(map, { base: BASE, L: fakeL(map), document: fakeDoc(mkCanvas), storage: storage({ 'allshore.places.v1': '0' }), fetch: f, whenReady: (go) => go() });
  const t = tileOf(40, -115, 4), empty = tileOf(-40, 60, 4);                    // the tile holding the 40 N border; one in the Indian Ocean
  const el = pl.layer.createTile.call(pl.layer, t), el2 = pl.layer.createTile.call(pl.layer, empty);
  pl.layer._tiles.a = { el, coords: t }; pl.layer._tiles.b = { el: el2, coords: empty };
  assert.equal(el._plComplete, false, 'blank while loading'); assert.ok(!strokes.includes('stroke'));
  while (f.held.length) f.held.shift()(); await settle(); while (f.held.length) f.held.shift()(); await settle();
  assert.equal(pl.store.status, 'ok');
  assert.equal(strokes.filter((x) => x === 'stroke').length, 2, 'redrawn once the data landed: the halo and the core of the one border tile');
  assert.equal(el._plComplete, true); assert.equal(el2._plComplete, true);
  strokes.length = 0; map.zoom = 2; pl.set('borders', true);
  assert.equal(strokes.filter((x) => x === 'stroke').length, 2, 'a setting change repaints (the tiles keep their own zoom)');
});

// ---- the places (step 5) ----
test('the places decoder: a round trip of the builder\'s format (degrees, ranks, zooms, populations, flags, UTF-8 names)', () => {
  const ps = [{ lat: 35.687, lon: 139.75, rank: 0, mz10: 17, pop: 35676000, flags: 1, name: 'Tokyo' }, { lat: 21.5928, lon: -158.1034, rank: 5, mz10: 100, pop: 3970, flags: 2, name: 'Hale\u2018iwa' },
    { lat: 21.5028, lon: -158.0236, rank: 4, mz10: 90, pop: 17821, flags: 2, name: 'Wahiaw\u0101' }, { lat: 66.5, lon: 180, rank: 5, mz10: 100, pop: 1000, flags: 2, name: 'Edge' }];
  const d = I.decodePlaces(encodePlaces(ps));
  assert.equal(d.n, 4); assert.equal(d.cell, 10); assert.equal(d.q, 10000);
  ps.forEach((p, k) => {
    assert.ok(Math.abs(d.lat[k] - p.lat) < 1e-9 && Math.abs(d.lon[k] - (p.lon >= 180 ? -180 : p.lon)) < 1e-9, p.name);
    assert.equal(d.rank[k], p.rank); assert.equal(d.mz[k], p.mz10); assert.equal(d.pop[k], p.pop); assert.equal(d.flags[k], p.flags); assert.equal(d.name[k], p.name);
  });
  assert.equal(I.decodePlaces(encodePlaces([])).n, 0);
});

test('the places decoder refuses damaged or foreign files', () => {
  const good = encodePlaces([{ lat: 1, lon: 2, rank: 5, mz10: 100, pop: 1000, flags: 2, name: 'A' }, { lat: 3, lon: 4, rank: 5, mz10: 100, pop: 1000, flags: 2, name: 'B' }]);
  const bad = (mut) => { const b = good.slice(0); mut(new DataView(b), new Uint8Array(b)); return b; };
  assert.throws(() => I.decodePlaces(new ArrayBuffer(10)), /too short/);
  assert.throws(() => I.decodePlaces(bad((dv, u8) => { u8[0] = 88; })), /not a places file/);
  assert.throws(() => I.decodePlaces(bad((dv) => dv.setUint32(8, 77, true))), /bad header/);
  assert.throws(() => I.decodePlaces(bad((dv) => dv.setUint32(16, 9999, true))), /bad header/);
  assert.throws(() => I.decodePlaces(bad((dv) => dv.setUint32(12, 3, true))), /counts/);
  assert.throws(() => I.decodePlaces(bad((dv, u8) => { u8[u8.length - 2] = 0x20; })), /names/);       // "A B": one name for two places
  assert.throws(() => I.decodePlaces(bad((dv, u8) => { u8[u8.length - 1] = 0xff; })), /./);           // a bad UTF-8 byte: the strict decoder throws
  assert.throws(() => I.decodePlaces(encodePlaces([{ lat: 1, lon: 2, rank: 5, mz10: 100, pop: 1, flags: 2, name: '' }])), /bad place|names/);
  assert.throws(() => I.decodePlaces(encodePlaces([{ lat: 95, lon: 2, rank: 5, mz10: 100, pop: 1, flags: 2, name: 'X' }])), /bad place/);
  assert.throws(() => I.decodePlaces(encodePlaces([{ lat: 1, lon: 2, rank: 300, mz10: 100, pop: 1, flags: 2, name: 'X' }])), /bad place/);
  assert.throws(() => I.decodeLines(good), /not a lines file/); assert.throws(() => I.decodePlaces(encodeLines([[0, [[[D(1), D(2)], [D(1), D(2)], 0]]]])), /not a places file/);
});

test('the published world places (a Pacific crop) and the Oahu cell decode to what the builder\'s own decoder reads', () => {
  const expect = JSON.parse(fs.readFileSync(path.join(FIX, 'places_expect.json'), 'utf8'));
  for (const [file, key] of [['world-places-crop.bin', 'crop'], ['20_-160.p.bin', '20_-160']]) {
    const e = expect[key], d = I.decodePlaces(ab(fs.readFileSync(path.join(FIX, file))));
    assert.equal(d.n, e.count); assert.equal(d.q, e.q); assert.equal(d.cell, e.cell);
    e.places.forEach((p, k) => {
      assert.ok(Math.abs(d.lat[k] - p[0] / e.q) < 1e-9 && Math.abs(d.lon[k] - p[1] / e.q) < 1e-9, file + ' ' + p[6]);
      assert.deepEqual([d.rank[k], d.mz[k], d.pop[k], d.flags[k], d.name[k]], [p[2], p[3], p[4], p[5], p[6]], file + ' ' + p[6]);
    });
  }
  const crop = I.decodePlaces(ab(fs.readFileSync(path.join(FIX, 'world-places-crop.bin'))));
  const tokyo = crop.name.indexOf('Tokyo'), hnl = crop.name.indexOf('Honolulu');
  assert.ok(tokyo >= 0 && crop.rank[tokyo] === 0 && (crop.flags[tokyo] & I.FLAG_CAPITAL), 'Tokyo: a capital');
  assert.ok(hnl >= 0 && crop.rank[hnl] === 1 && crop.mz[hnl] === 40);
  const cell = I.decodePlaces(ab(fs.readFileSync(path.join(FIX, '20_-160.p.bin'))));
  assert.ok(cell.name.includes('Kailua') && cell.name.includes('Hale\u2018iwa') && cell.flags.every((f) => f & I.FLAG_GEONAMES), 'the Oahu cell: GeoNames towns');
  assert.ok(cell.mz.every((z) => z >= 80) && cell.name.indexOf('Honolulu') < 0, 'from zoom 8; Honolulu is in tier 0');
});

test('cellsForBox: a box across the date line names both edge columns; the rows by the latitudes', () => {
  assert.deepEqual(I.cellsForBox(170, 50, 190, 60, 10).sort(), ['50_-180', '50_170']);
  assert.deepEqual(I.cellsForBox(-158.5, 21.2, -157.5, 21.8, 10), ['20_-160']);
  assert.deepEqual(I.cellsForBox(-158.5, 19.5, -157.5, 21.8, 10).sort(), ['10_-160', '20_-160']);
  assert.deepEqual(I.cellsForBox(-200, 0, -190, 5, 10), ['0_160'], 'a west longitude beyond -180 wraps');
});

test('the candidates: a place shows from its own zoom, once per world copy inside the view and its margin, the largest first', () => {
  const crop = I.decodePlaces(ab(fs.readFileSync(path.join(FIX, 'world-places-crop.bin'))));
  assert.equal(I.placeShown(40, 39), false); assert.equal(I.placeShown(40, 40), true);
  const names = (c) => c.map((x) => x.name);
  const hawaii = I.candidatesFor([crop], 40, -162, 18, -154, 23, 0);
  assert.deepEqual(names(hawaii), ['Honolulu'], 'at zoom 4.0 only Honolulu (Hilo 5.1, Wahiawa 6.7)');
  assert.deepEqual(names(I.candidatesFor([crop], 67, -162, 18, -154, 23, 0)), ['Honolulu', 'Wahiawa', 'Hilo', 'Wailuku', 'Lihue', 'Kailua-Kona'], 'rank, then population');
  assert.deepEqual(names(I.candidatesFor([crop], 50, -162, 18, -154, 23, 0)), ['Honolulu'], 'Hilo at 5.1 not yet at 5.0');
  const pacific = I.candidatesFor([crop], 40, 130, 18, 210, 40, 0);                 // Japan to Hawaii, across the date line (unwrapped)
  const hnl = pacific.filter((x) => x.name === 'Honolulu'), tky = pacific.filter((x) => x.name === 'Tokyo');
  assert.equal(hnl.length, 1); assert.ok(Math.abs(hnl[0].lon - 202.142) < 1e-9, 'Honolulu in the eastern copy');
  assert.equal(tky.length, 1); assert.equal(pacific[0].name, 'Tokyo', 'the capital first');
  const twice = I.candidatesFor([crop], 40, -170, 18, 210, 23, 0);
  assert.deepEqual(twice.filter((x) => x.name === 'Honolulu').map((x) => x.lon).sort((a, b) => a - b), [-157.858, 202.142], 'a view wider than the world: both copies');
  assert.equal(I.candidatesFor([crop], 40, -157, 18, -154, 23, 0).length, 0, 'just east of Honolulu: none');
  assert.deepEqual(names(I.candidatesFor([crop], 40, -162, 18, -157.858, 23, 0)), ['Honolulu'], 'a place on the east edge counts');
  assert.equal(I.candidatesFor([crop], 40, -157, 18, -154, 23, 1).length, 1, '... until the margin reaches it');
  const two = I.candidatesFor([crop, crop], 40, -162, 18, -154, 23, 0);
  assert.equal(two.length, 2, 'every set counts (the same place twice from two sets would thin to one)');
});

test('the thinning: no two kept labels overlap, the larger place wins, blocked rects are kept clear, the cap holds, and the result is the same every time', () => {
  const it = (x, y, w, name) => ({ x, y, w, name });
  const items = [it(100, 100, 50, 'big'), it(110, 104, 30, 'under big'), it(300, 100, 20, 'free'), it(100, 130, 50, 'below big'), it(100, 118, 50, 'touching big')];
  const kept = I.thin(items, [], 10).map((i) => i.name);
  assert.deepEqual(kept, ['big', 'free', 'below big'], 'under and touching overlap (the gap counts)');
  const box = (i) => ({ left: i.x - I.DOT_R - I.LABEL_GAP, top: i.y - I.LABEL_H / 2 - I.LABEL_GAP, right: i.x + I.LABEL_DX + i.w + I.LABEL_GAP, bottom: i.y + I.LABEL_H / 2 + I.LABEL_GAP });
  // a crowd: brute-force check that nothing kept overlaps
  const crowd = []; let seed = 7;
  const rnd = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
  for (let i = 0; i < 600; i++) crowd.push(it(Math.floor(rnd() * 800), Math.floor(rnd() * 600), 20 + Math.floor(rnd() * 60), 'p' + i));
  const k = I.thin(crowd, [{ left: 0, top: 0, right: 200, bottom: 60 }], 120);
  assert.ok(k.length > 30 && k.length <= 120, String(k.length));
  for (let a = 0; a < k.length; a++) {
    const ra = box(k[a]);
    assert.ok(!(ra.left < 200 && ra.right > 0 && ra.top < 60 && ra.bottom > 0), 'clear of the blocked rect: ' + k[a].name);
    for (let b = a + 1; b < k.length; b++) { const rb = box(k[b]); assert.ok(!(ra.left < rb.right && ra.right > rb.left && ra.top < rb.bottom && ra.bottom > rb.top), k[a].name + ' vs ' + k[b].name); }
  }
  assert.deepEqual(I.thin(crowd, [{ left: 0, top: 0, right: 200, bottom: 60 }], 120).map((i) => i.name), k.map((i) => i.name), 'deterministic');
  assert.equal(I.thin(crowd, [], 10).length, 10, 'the cap');
  // every candidate that survives keeps its order: the first (largest) is always kept
  assert.equal(I.thin(crowd, [], 120)[0], crowd[0]);
});

test('the store: tier-0 places at every zoom; from tile zoom 8 the view\'s .p cells (a cell with no places file is never asked); labels rebuilt when a cell lands', async () => {
  const f = fakeFetch({ ...TIER0, 'c/20_-160.p.bin': CELL_P });
  const st = new I.PlacesStore(BASE, f); await st.load();
  assert.deepEqual(st.placesFor(-160, 20, -150, 25, 6), { sets: [st.places0], complete: true });
  const first = st.placesFor(-158.5, 21, -157.5, 22, 8);
  assert.deepEqual(first, { sets: [st.places0], complete: false }, 'the cell is on its way');
  await settle();
  const second = st.placesFor(-158.5, 21, -157.5, 22, 8);
  assert.equal(second.complete, true); assert.equal(second.sets.length, 2); assert.equal(second.sets[1].n, 2);
  assert.ok(f.asked.some((u) => /c\/20_-160\.p\.bin$/.test(u)) && !f.asked.some((u) => /20_-160\.l\.bin/.test(u)), 'the places file, not the (absent) lines file');
  assert.deepEqual(st.placesFor(-121, 41, -119, 42, 8), { sets: [st.places0], complete: true }, '40_-120 has lines but no places file: nothing asked');
  assert.ok(!f.asked.some((u) => /40_-120\.p\.bin/.test(u)));
});

// the window the module is loaded into: a frame queue stands in for requestAnimationFrame
function loadWithFrames() { const q = []; const win = { requestAnimationFrame: (fn) => { q.push(fn); return q.length; } }; const M = load(win); return { M, flush: () => { while (q.length) q.shift()(); } }; }
const oahuDoc = () => fakeDoc(() => { const c = el('canvas'); c.getContext = () => ({ setTransform() {}, clearRect() {}, setLineDash() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() {}, measureText: (t) => ({ width: t.length * 6 }) }); return c; });

test('create(): the labels: text spans at the layer points, the largest first, the capital in bold, hidden while zooming and rebuilt after a move; off = none; the pool is reused', async () => {
  const { M, flush } = loadWithFrames();
  const crop = fs.readFileSync(path.join(FIX, 'world-places-crop.bin'));
  const f = fakeFetch({ ...TIER0, 'world-places.bin': ab(crop) });
  const map = fakeMap(6, 21.3, -157.9), pl = M.create(map, { base: BASE, L: fakeL(map), document: oahuDoc(), storage: storage(), fetch: f, whenReady: (go) => go() });
  await settle(); flush();
  const shown = () => pl.labels.children.filter((s) => !s.hidden).map((s) => s.textContent);
  assert.deepEqual(shown(), ['Honolulu', 'Hilo'], 'zoom 6.0: Honolulu (4.0) and Hilo (5.1); Wahiawa waits for 6.7');
  const hnl = pl.labels.children[0], lp = map.latLngToLayerPoint([21.303, -157.858]);
  assert.equal(hnl.className, 'place-label'); assert.equal(hnl.style.left, Math.round(lp.x) + 'px'); assert.equal(hnl.style.top, Math.round(lp.y) + 'px');
  assert.equal(hnl.innerHTML, undefined, 'text only');
  map.zoom = 6.7; map.fire('zoomstart'); assert.equal(pl.labels.hidden, true);
  map.fire('zoomend'); flush(); assert.equal(pl.labels.hidden, false);
  assert.deepEqual(shown(), ['Honolulu', 'Hilo', 'Wailuku', 'Lihue'], 'zoom 6.7: Wahiawa and Kailua-Kona are shown by their zoom but their labels sit under Honolulu and Hilo (thinned)');
  map.zoom = 6.69; map.fire('zoomend'); flush();
  assert.ok(!shown().includes('Lihue'), 'zoom 6.69 is zoom 66 (the floor): Lihue waits for 67');
  map.zoom = 6.7; map.fire('zoomend'); flush();
  assert.ok(shown().includes('Lihue'), '... and shows at 6.7');
  map.zoom = 7.4; map.fire('zoomend'); flush();
  const s74 = shown();
  assert.deepEqual(s74.slice(0, 3), ['Honolulu', 'Wahiawa', 'Hilo'], 'zoom 7.4: clear of each other; then Wailuku, Lihue, Kailua-Kona');
  assert.equal(pl.labels.children.length, s74.length, 'one span per shown label in the pool');
  map.center = { lat: 35.7, lon: 139.8 }; map.zoom = 6; map.fire('moveend'); flush();
  const sj = shown();
  assert.equal(sj[0], 'Tokyo', 'the capital first, then Kyoto, Nagano ...'); assert.equal(pl.labels.children[0].className, 'place-label place-capital');
  assert.ok(sj.length > 3 && sj.indexOf('Kyoto') > 0);
  assert.equal(pl.labels.children.length, Math.max(s74.length, sj.length), 'the pool: spans reused, grown only when a view needs more');
  map.center = { lat: 30, lon: -170 }; map.fire('moveend'); flush();
  assert.deepEqual(shown(), [], 'open ocean: every span of the pool hidden');
  assert.equal(pl.labels.children.length, Math.max(s74.length, sj.length), 'and kept');
  map.center = { lat: 35.7, lon: 139.8 }; map.fire('moveend'); flush();
  pl.set('places', false); assert.equal(pl.labels.hidden, true);
  pl.set('places', true); assert.equal(pl.labels.hidden, false); assert.deepEqual(shown(), sj);
  // a narrow map keeps at most 60, a wide one 120
  assert.equal(pl.labels.children.filter((s) => !s.hidden).length <= 120, true);
});

test('create(): a label never sits under a control or a marker; a crowd thins to the larger places; a cell landing rebuilds the labels once', async () => {
  const { M, flush } = loadWithFrames();
  const crop = fs.readFileSync(path.join(FIX, 'world-places-crop.bin')), cell = fs.readFileSync(path.join(FIX, '20_-160.p.bin'));
  const f = fakeFetch({ ...TIER0, hold: true, 'world-places.bin': ab(crop), 'c/20_-160.p.bin': ab(cell) });
  const map = fakeMap(10, 21.33, -157.86), pl = M.create(map, { base: BASE, L: fakeL(map), document: oahuDoc(), storage: storage(), fetch: f, whenReady: (go) => go() });
  while (f.held.length) f.held.shift()(); await settle(); while (f.held.length) f.held.shift()(); await settle(); flush();
  const shown = () => pl.labels.children.filter((s) => !s.hidden).map((s) => s.textContent);
  assert.deepEqual(shown(), ['Honolulu', 'Wahiawa'], 'tier 0 alone until the cell lands');
  assert.ok(f.asked.some((u) => /20_-160\.p\.bin$/.test(u)), 'the cell asked for at zoom 10');
  while (f.held.length) f.held.shift()(); await settle(); flush();
  const after = shown();
  assert.ok(after.length > 5 && after[0] === 'Honolulu', 'the towns joined, Honolulu first: ' + after.length);
  assert.ok(after.includes('Pearl City') || after.includes('Kailua') || after.includes('Kaneohe'));
  // a control over Honolulu's label (client rects: the container sits at 10, 20): Honolulu goes, the rest stays
  const p = map.latLngToContainerPoint([21.303, -157.858]);
  const ctl = { hidden: false, getBoundingClientRect: () => ({ left: p.x - 40 + 10, top: p.y - 6 + 20, right: p.x + 80 + 10, bottom: p.y + 6 + 20 }) };
  map.controls.push(ctl);
  pl.update();
  assert.ok(!shown().includes('Honolulu') && shown().length > 3, 'blocked by the control');
  ctl.hidden = true; pl.update(); assert.ok(shown().includes('Honolulu'), 'a hidden control blocks nothing');
  map.controls.length = 0; pl.update(); assert.ok(shown().includes('Honolulu'));
});

test('the places decoder refuses a longitude of exactly 180 (the builder stores it as -180)', () => {
  const vals = [zz(10000), zz(1800000), 5, 100, 1000, 2], body = [];
  vals.forEach((v) => body.push(...leb(v)));
  const names = Buffer.from('Edge', 'utf8');
  const buf = new ArrayBuffer(36 + body.length + names.length), dv = new DataView(buf), u8 = new Uint8Array(buf);
  u8.set([80, 76, 67, 49], 0); dv.setUint16(4, 10, true); dv.setUint32(8, 10000, true); dv.setUint32(12, 1, true); dv.setUint32(16, body.length, true);
  u8.set(body, 36); u8.set(names, 36 + body.length);
  assert.throws(() => I.decodePlaces(buf), /bad place/);
  const ok = buf.slice(0); const u = new Uint8Array(ok); const lo = leb(zz(-1800000)); u.set(lo, 36 + leb(zz(10000)).length);   // -180: fine (same varint length)
  assert.equal(I.decodePlaces(ok).lon[0], -180);
});

test('create(): a narrow map keeps at most 60 labels, a wide one 120', async () => {
  const many = [];
  for (let r = 0; r < 14; r++) for (let c = 0; c < 14; c++) many.push({ lat: 20 + r * 0.6, lon: -160 + c * 1.5, rank: 5, mz10: 0, pop: 1000 + r * 14 + c, flags: 0, name: 'p' + r + '_' + c });
  const grid = encodePlaces(many);
  for (const [sx, sy, cap] of [[500, 400, 60], [800, 600, 120]]) {
    const { M, flush } = loadWithFrames();
    const f = fakeFetch({ ...TIER0, 'world-places.bin': grid });
    const map = fakeMap(6, 24, -150, sx, sy), pl = M.create(map, { base: BASE, L: fakeL(map), document: oahuDoc(), storage: storage(), fetch: f, whenReady: (go) => go() });
    await settle(); flush();
    const n = pl.labels.children.filter((s) => !s.hidden).length;
    assert.ok(n === cap, sx + ' px wide: ' + n + ' labels (cap ' + cap + ')');
  }
});
