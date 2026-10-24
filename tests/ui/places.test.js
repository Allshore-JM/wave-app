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

test('the zoom gates: one Borders setting: countries from tile zoom 3, states from 5, each line from its own zoom (tile zoom z serves map zooms below z + 0.5)', () => {
  const on = { borders: true };
  assert.deepEqual(I.kindsAt(2, on), {}); assert.deepEqual(I.kindsAt(3, on), { 0: true, 2: true }); assert.deepEqual(I.kindsAt(4, on), { 0: true, 2: true });
  assert.deepEqual(I.kindsAt(5, on), { 0: true, 1: true, 2: true });
  assert.deepEqual(I.kindsAt(6, { borders: false }), {}, 'off: no line of any kind');
  assert.deepEqual(I.kindsAt(6, { borders: false, states: true }), {}, 'a stray states flag draws nothing');
  assert.deepEqual(Object.keys(I.KEYS).sort(), ['borders', 'places'], 'two settings: Borders and City names');
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
  const cells = {}; ['40_-120', '40_-110', '40_-100', '40_-90'].forEach((n) => { cells[n] = [CELL_A.byteLength, 0]; });
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
  const g = fakeFetch({ ...TIER0, 'index.json': { json: { ...INDEX, tier1: { ...INDEX.tier1, cells: { '40_-80': [other.byteLength, 0] } } } }, 'c/40_-80.l.bin': other });
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
    sheets: [],
    getContainer() { const mm = this; return { getBoundingClientRect: () => ({ left: 10, top: 20 }), querySelectorAll: (sel) => { if (sel === '.ov-sheet') return mm.sheets; mm.lastSel = sel; return controls; } }; },
    attributionControl: { addAttribution: (t) => credits.push('+' + t.slice(0, 7)), removeAttribution: (t) => credits.push('-' + t.slice(0, 7)) } };
  return m;
}
function fakeL(map) {
  return { GridLayer: { extend(p) { function C(o) { this.options = o; this._tiles = {}; } C.prototype = Object.assign({ addTo(m) { m._add(this); return this; } }, p); return C; } } };
}
function storage(init) { const m = new Map(Object.entries(init || {})); return { m, getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)) }; }

test('create(): its own panes (lines z 340, names z 620 above the station icons, no pointer events); both settings on by default; nothing fetched before the page is ready, below zoom 3, or with both off; the settings stored; credits only once the data has loaded', async () => {
  const map = fakeMap(6), f = fakeFetch(TIER0);
  let start = null;
  const s = storage();
  const pl = P.create(map, { base: BASE, L: fakeL(map), document: fakeDoc(), storage: s, fetch: f, whenReady: (go) => { start = go; } });
  assert.equal(map.panes.placesLinePane.style.zIndex, 340); assert.equal(map.panes.placesLinePane.style.pointerEvents, 'none');
  assert.equal(map.panes.placesLabelPane.style.zIndex, 620); assert.equal(map.panes.placesLabelPane.style.pointerEvents, 'none');
  assert.ok(I.LABEL_PANE_Z > 600 && I.LABEL_PANE_Z < 650, 'above the marker pane (600), below the tooltips (650)');
  assert.equal(map.panes.placesLabelPane.children[0], pl.labels); assert.equal(pl.labels.attrs['aria-hidden'], 'true');
  assert.equal(pl.on('borders'), true); assert.equal(pl.on('places'), true); assert.equal(pl.on('states'), false, 'no states setting of its own');
  assert.ok(map.hasLayer(pl.layer));
  assert.equal(f.asked.length, 0, 'nothing before the page is ready');
  assert.deepEqual(map.credits, [], 'no credit before the data has loaded (V7-6)');
  start(); await settle();
  assert.deepEqual(f.asked, [BASE + '/index.json', BASE + '/world-lines.bin', BASE + '/world-places.bin']);
  assert.deepEqual(map.credits, ['+Borders', '+Places:'], 'credited once loaded');
  pl.set('borders', false);
  assert.equal(s.m.get('allshore.borders.v1'), '0'); assert.equal(s.m.has('allshore.states.v1'), false);
  assert.ok(!map.hasLayer(pl.layer), 'Borders off: the line layer goes, City names alone draws no tile (G34 B-7)'); assert.deepEqual(map.credits.slice(2), ['-Borders'], 'the borders credit goes once');
  assert.equal(pl.labels.hidden, false, 'the names stay');
  pl.set('places', false);
  assert.ok(!map.hasLayer(pl.layer), 'both off: no layer'); assert.equal(s.m.get('allshore.places.v1'), '0'); assert.deepEqual(map.credits.slice(3), ['-Places:']);
  pl.set('borders', true); assert.ok(map.hasLayer(pl.layer)); assert.equal(s.m.get('allshore.borders.v1'), '1'); assert.deepEqual(map.credits.slice(4), ['+Borders'], 'loaded: credited at once');
  pl.set('states', true); assert.equal(s.m.has('allshore.states.v1'), false, 'not a setting any more');
  pl.set('nope', true); assert.equal(s.m.has('nope'), false);
  // a fresh page at zoom 1 with the saved settings: nothing with both off; borders alone from zoom 3; places from 2
  const map2 = fakeMap(1), f2 = fakeFetch(TIER0);
  const pl2 = P.create(map2, { base: BASE, L: fakeL(map2), document: fakeDoc(), storage: storage({ 'allshore.borders.v1': '0', 'allshore.places.v1': '0', 'allshore.states.v1': '1' }), fetch: f2, whenReady: (go) => go() });
  assert.equal(pl2.on('borders'), false); assert.equal(pl2.on('places'), false);
  await settle(); map2.zoom = 6; map2.fire('zoomend'); await settle(); assert.equal(f2.asked.length, 0, 'both off: nothing at any zoom (an old states key changes nothing)');
  assert.equal(map2.hasLayer(pl2.layer), false);
  const map2b = fakeMap(1), f2b = fakeFetch(TIER0);
  P.create(map2b, { base: BASE, L: fakeL(map2b), document: fakeDoc(), storage: storage({ 'allshore.places.v1': '0' }), fetch: f2b, whenReady: (go) => go() });
  map2b.zoom = 2; map2b.fire('zoomend'); await settle(); assert.equal(f2b.asked.length, 0, 'borders alone: not before zoom 3');
  map2b.zoom = 3; map2b.fire('zoomend'); await settle(); assert.deepEqual(f2b.asked, [BASE + '/index.json', BASE + '/world-lines.bin'], 'Borders alone: no places file (re-check R-2)');
  const map3 = fakeMap(1), f3 = fakeFetch(TIER0);
  P.create(map3, { base: BASE, L: fakeL(map3), document: fakeDoc(), storage: storage({ 'allshore.borders.v1': '0' }), fetch: f3, whenReady: (go) => go() });
  await settle(); assert.equal(f3.asked.length, 0, 'places only: not at zoom 1');
  map3.zoom = 2; map3.fire('zoomend'); await settle(); assert.deepEqual(f3.asked, [BASE + '/index.json', BASE + '/world-places.bin'], 'places from zoom 2, no lines file (re-check R-2)');
  // a storage that throws: on, and changes never throw
  const pl3 = P.create(fakeMap(6), { base: BASE, L: fakeL(fakeMap(6)), document: fakeDoc(), storage: { getItem() { throw new Error('x'); }, setItem() { throw new Error('x'); } }, whenReady: () => {} });
  assert.equal(pl3.on('borders'), true); pl3.set('borders', false); assert.equal(pl3.on('borders'), false);
  // without a base the rows still work, nothing loads, and nothing is credited (V7-6)
  const map4 = fakeMap(6), pl4 = P.create(map4, { L: fakeL(map4), document: fakeDoc(), whenReady: (go) => go() });
  assert.equal(pl4.store, null); assert.equal(pl4.wanted(), false); await settle(); assert.deepEqual(map4.credits, []);
  // a load that fails: nothing credited
  const map5 = fakeMap(6), f5 = fakeFetch({ 'index.json': 'down' });
  P.create(map5, { base: BASE, L: fakeL(map5), document: fakeDoc(), storage: storage(), fetch: f5, whenReady: (go) => go() });
  await settle(); assert.ok(f5.asked.length >= 1); assert.deepEqual(map5.credits, [], 'no data: no credit');
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

test('the thinning with the map size: a name is kept only with its dot and whole text on the map; past the right edge it goes left of its dot; the left box is kept clear like the right one (V7-4)', () => {
  const it = (x, y, w, name) => ({ x, y, w, name }), size = { x: 800, y: 600 };
  const k = I.thin([it(100, 100, 40, 'inside'), it(770, 200, 40, 'right edge'), it(-2, 300, 40, 'dot off the left'), it(30, 400, 900, 'too long'),
    it(400, 5, 40, 'top edge'), it(400, 597, 40, 'bottom edge'), it(805, 250, 20, 'dot off the right'), it(3, 500, 30, 'dot at the left edge')], [], 120, size);
  assert.deepEqual(k.map((i) => i.name + ':' + i.side), ['inside:R', 'right edge:L', 'dot at the left edge:R']);
  const exact = I.thin([it(800 - I.LABEL_DX - 40, 300, 40, 'flush right')], [], 120, size);
  assert.deepEqual(exact.map((i) => i.side), ['R'], 'the text ending exactly on the edge stays right');
  assert.deepEqual(I.thin([it(800 - I.LABEL_DX - 40 + 1, 300, 40, 'one over')], [], 120, size).map((i) => i.side), ['L']);
  assert.deepEqual(I.thin([it(I.LABEL_DX + 40, 300, 40, 'flush left')], [], 120, size).map((i) => i.side), ['R'], 'right first');
  // the left box blocks a later name: a name whose right box meets the first one's left box is thinned
  const two = I.thin([it(780, 300, 60, 'big'), it(700, 300, 20, 'small')], [], 120, size);
  assert.deepEqual(two.map((i) => i.name), ['big'], 'big went left (720..780); small at 700..726 meets it');
  // without a size (the old callers) no fit rule applies
  assert.deepEqual(I.thin([it(-50, -50, 10, 'off')], [], 120).map((i) => i.name), ['off']);
  // a crowd: every kept box on the map and clear of the others (the side's own box)
  const crowd = []; let seed = 11;
  const rnd = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
  for (let i = 0; i < 600; i++) crowd.push(it(Math.floor(rnd() * 840) - 20, Math.floor(rnd() * 640) - 20, 20 + Math.floor(rnd() * 80), 'p' + i));
  const kept = I.thin(crowd, [], 120, size);
  const box = (i) => i.side === 'L' ? { left: i.x - I.LABEL_DX - i.w, right: i.x + I.DOT_R, top: i.y - I.LABEL_H / 2, bottom: i.y + I.LABEL_H / 2 }
                                    : { left: i.x - I.DOT_R, right: i.x + I.LABEL_DX + i.w, top: i.y - I.LABEL_H / 2, bottom: i.y + I.LABEL_H / 2 };
  assert.ok(kept.some((i) => i.side === 'L') && kept.length > 30);
  for (let a = 0; a < kept.length; a++) {
    const ra = box(kept[a]);
    assert.ok(ra.left >= 0 && ra.top >= 0 && ra.right <= 800 && ra.bottom <= 600, 'on the map: ' + kept[a].name + JSON.stringify(ra));
    for (let b = a + 1; b < kept.length; b++) {
      const rb = box(kept[b]), g = I.LABEL_GAP * 2;
      assert.ok(!(ra.left - g < rb.right && ra.right + g > rb.left && ra.top - g < rb.bottom && ra.bottom + g > rb.top), kept[a].name + ' vs ' + kept[b].name);
    }
  }
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

test('create(): a label never sits under a control (the station icons are no obstacle); a crowd thins to the larger places; a cell landing rebuilds the labels once', async () => {
  const { M, flush } = loadWithFrames();
  const crop = fs.readFileSync(path.join(FIX, 'world-places-crop.bin')), cell = fs.readFileSync(path.join(FIX, '20_-160.p.bin'));
  const idx = { ...INDEX, tier1: { ...INDEX.tier1, cells: { ...INDEX.tier1.cells, '20_-160': [0, cell.length] } } };
  const f = fakeFetch({ ...TIER0, hold: true, 'index.json': { json: idx }, 'world-places.bin': ab(crop), 'c/20_-160.p.bin': ab(cell) });
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
  assert.equal(map.lastSel, I.BLOCKED_SELECTOR, 'the rects asked for: controls and their panels, the sheet, the gridline labels, the fan and labels of the map tools');
  ['.leaflet-control', '.settings-panel', '.ov-sheet', '.graticule-label', '.tools-fan', '.tools-label', '.tools-ring-label'].forEach((c) => assert.ok(I.BLOCKED_SELECTOR.split(', ').includes(c), c));
  assert.equal(I.FLOAT_SELECTOR, '.fwin', 'and the floating windows (G34 B-1)');
  assert.ok(!/marker/.test(I.BLOCKED_SELECTOR), 'never the station icons: the names are drawn above them');
});

test('create(): near the right edge a name is drawn to the left of its dot (class place-left), never cut; a control corner or the overlay sheet changing size lays the names out again (V7-1, V7-4)', async () => {
  const ros = [], mos = [], q = [];
  const win = { requestAnimationFrame: (fn) => { q.push(fn); return q.length; },
    ResizeObserver: class { constructor(cb) { this.cb = cb; this.els = []; ros.push(this); } observe(e) { this.els.push(e); } unobserve(e) { this.els = this.els.filter((x) => x !== e); } },
    MutationObserver: class { constructor(cb) { this.cb = cb; mos.push(this); } observe(t, o) { this.target = t; this.opts = o; } } };
  const M = load(win), flush = () => { while (q.length) q.shift()(); };
  const crop = fs.readFileSync(path.join(FIX, 'world-places-crop.bin'));
  const f = fakeFetch({ ...TIER0, 'world-places.bin': ab(crop) });
  // Honolulu 20 px from the right edge at zoom 6 (45.5 px a degree): its 48-px name cannot run right
  const map = fakeMap(6, 21.3, -157.858 - 380 / (256 * 64 / 360));
  const corners = { topleft: { c: 'tl' }, topright: { c: 'tr' }, bottomleft: { c: 'bl' }, bottomright: { c: 'br' } };
  map._controlCorners = corners;
  const pl = M.create(map, { base: BASE, L: fakeL(map), document: oahuDoc(), storage: storage(), fetch: f, whenReady: (go) => go() });
  await settle(); flush();
  const hnl = pl.labels.children.find((sp) => sp.textContent === 'Honolulu' && !sp.hidden);
  assert.ok(hnl, 'Honolulu shown'); assert.equal(hnl.className, 'place-label place-left');
  const lp = map.latLngToLayerPoint([21.303, -157.858]);
  assert.equal(hnl.style.left, Math.round(lp.x) + 'px', 'the span sits at the dot; CSS moves its text before it');
  // the observers: the four corners watched; a sheet added to the container is watched too; a size change re-lays
  assert.equal(ros.length, 1); assert.deepEqual(ros[0].els.map((e) => e.c), ['tl', 'tr', 'bl', 'br']);
  assert.equal(mos.length, 1, 'no floating window on this page: one observer'); assert.deepEqual(mos[0].opts, { childList: true, subtree: true });
  const sheet = { className: 'ov-sheet' }; map.sheets.push(sheet);
  mos[0].cb([{ addedNodes: [sheet], removedNodes: [] }]); assert.ok(ros[0].els.includes(sheet), 'the overlay sheet watched once it is there');
  mos[0].cb([{ addedNodes: [sheet], removedNodes: [] }]); assert.equal(ros[0].els.filter((e) => e === sheet).length, 1, 'once');
  flush();
  mos[0].cb([{ addedNodes: [{ className: 'leaflet-tile' }, { className: 'place-label' }], removedNodes: [] }]);
  assert.equal(q.length, 0, 'a tile or a name of its own coming lays nothing out');
  mos[0].cb([{ addedNodes: [], removedNodes: [sheet] }]);
  assert.ok(!ros[0].els.includes(sheet), 'a sheet that goes is let go (G34 F-A4)'); assert.equal(q.length, 1, 'and the names laid out again');
  flush();
  mos[0].cb([{ addedNodes: [{ className: 'leaflet-marker-icon tools-fan leaflet-zoom-animated' }], removedNodes: [] }]);
  assert.equal(q.length, 1, 'the fan of a map tool coming lays the names out again (G34 B-2)'); flush();
  mos[0].cb([{ addedNodes: [], removedNodes: [{ className: 'leaflet-tooltip tools-label' }] }]); assert.equal(q.length, 1, 'a tool label going too'); flush();
  const p = map.latLngToContainerPoint([21.303, -157.858]);
  map.controls.push({ hidden: false, getBoundingClientRect: () => ({ left: p.x - 80 + 10, top: p.y - 8 + 20, right: p.x + 10 + 10, bottom: p.y + 8 + 20 }) });
  assert.ok(!hnl.hidden, 'nothing re-laid yet');
  ros[0].cb([]); flush();
  assert.ok(!pl.labels.children.some((sp) => sp.textContent === 'Honolulu' && !sp.hidden), 'a panel grew over it: laid out again, Honolulu goes');
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

// ---- G34 fix round ----
test('G34 F-A1: a header that claims more than the stream holds is refused before anything is sized by it', () => {
  const buf = new ArrayBuffer(60), dv = new DataView(buf), u8 = new Uint8Array(buf);
  u8.set([76, 78, 83, 49], 0); dv.setUint16(4, 10, true); dv.setUint16(6, 1, true); dv.setUint32(8, 10000, true);
  dv.setUint32(12, 5e6, true); dv.setUint32(16, 5e6, true); dv.setUint32(20, 5e6, true);
  assert.throws(() => I.decodeLines(buf), /counts do not match the stream/, 'the count check comes first (not "stream shorter than its counts" after sizing 277 MB)');
});

test('G34 B-5: the tier-0 lines and places are two units: one failing keeps the other; the failed one is asked again after its cooldown, not the index', async () => {
  const answers = { ...TIER0, 'world-places.bin': 'down' }, f = fakeFetch(answers);
  const st = new I.PlacesStore(BASE, f);
  assert.equal(await st.load(), st, 'the lines usable');
  assert.equal(st.ok('l'), true); assert.equal(st.ok('p'), false); assert.equal(st.unit.p.status, 'failed');
  assert.deepEqual(st.placesFor(-160, 20, -150, 25, 6), { sets: [], complete: false });
  assert.deepEqual(st.linesFor({ z: 4, x: 2, y: 5 }), { sets: [st.lines0], complete: true });
  const n = f.asked.length;
  assert.equal(await st.load(), st); assert.equal(f.asked.length, n, 'inside the cooldown: nothing asked');
  answers['world-places.bin'] = PLACES0; st.unit.p.failedAt -= I.RETRY_MS + 1;
  assert.equal(await st.load(), st); assert.equal(st.ok('p'), true);
  assert.deepEqual(f.asked.slice(n), [BASE + '/world-places.bin'], 'only the failed file again, not the index');
  const g = fakeFetch({ ...TIER0, 'world-lines.bin': 'down' }), s2 = new I.PlacesStore(BASE, g);
  assert.equal(await s2.load(), s2); assert.equal(s2.ok('l'), false); assert.equal(s2.ok('p'), true, 'the reverse');
  const h = fakeFetch({ 'index.json': { json: INDEX } }), s3 = new I.PlacesStore(BASE, h);
  assert.equal(await s3.load(), null, 'neither kind: null'); assert.equal(s3.status, 'ok', 'the index kept');
  // on the page: the borders credited, no names and no places credit
  const map = fakeMap(6), pl = P.create(map, { base: BASE, L: fakeL(map), document: fakeDoc(), storage: storage(), fetch: fakeFetch({ ...TIER0, 'world-places.bin': 'down' }), whenReady: (go) => go() });
  await settle(); pl.update();
  assert.deepEqual(map.credits, ['+Borders'], 'only the kind that loaded is credited'); assert.equal(pl.labels.hidden, true);
  const map2 = fakeMap(6);
  P.create(map2, { base: BASE, L: fakeL(map2), document: fakeDoc(), storage: storage(), fetch: fakeFetch({ ...TIER0, 'world-lines.bin': 'down' }), whenReady: (go) => go() });
  await settle(); assert.deepEqual(map2.credits, ['+Places:'], 'and the reverse');
  // a move begins a new view (the cells it uses are kept while it lasts)
  const v = pl.store.view; map.fire('movestart'); assert.equal(pl.store.view, v + 1); map.fire('zoomstart'); assert.equal(pl.store.view, v + 2);
});

test('G34 F-A2: a file of another length than the index gives is refused (a tier-0 file: its unit fails; a cell: for good)', async () => {
  const idx = { ...INDEX, tier0: { lines: { file: 'world-lines.bin', bytes: LINES0.byteLength + 1 }, places: { file: 'world-places.bin', bytes: PLACES0.byteLength } },
    tier1: { ...INDEX.tier1, cells: { '40_-120': [CELL_A.byteLength - 1, 0] } } };
  const f = fakeFetch({ ...TIER0, 'index.json': { json: idx }, 'c/40_-120.l.bin': CELL_A });
  const st = new I.PlacesStore(BASE, f);
  assert.equal(await st.load(), st); assert.equal(st.ok('l'), false, 'the lines cut or grown: refused'); assert.equal(st.ok('p'), true, 'the places of the right length');
  const idx2 = { ...idx, tier0: { ...idx.tier0, lines: { file: 'world-lines.bin', bytes: LINES0.byteLength } } };
  const s2 = new I.PlacesStore(BASE, fakeFetch({ ...TIER0, 'index.json': { json: idx2 }, 'c/40_-120.l.bin': CELL_A }));
  await s2.load(); s2.request(['40_-120.l']); await settle();
  assert.equal(s2.failed['40_-120.l'], true, 'a cell of another length: final'); assert.equal(s2.cells.size, 0);
  assert.equal(I.expectOf(10), 10); assert.equal(I.expectOf(0), 0); assert.equal(I.expectOf(1.5), 0); assert.equal(I.expectOf('10'), 0); assert.equal(I.expectOf(undefined), 0);
});

test('G34 B-6: afterWait starts once the overlay promise settles, at once without one, and after WAIT_MAX_MS at the latest; create() waits for it after the page load', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let n = 0;
  I.afterWait(null, () => n++); I.afterWait(() => null, () => n++); I.afterWait(() => { throw new Error('x'); }, () => n++);
  assert.equal(n, 3, 'nothing to wait for: at once');
  let res; const p = new Promise((r) => { res = r; });
  I.afterWait(() => p, () => n++); assert.equal(n, 3); res(); await settle(); assert.equal(n, 4, 'when it settles');
  t.mock.timers.tick(I.WAIT_MAX_MS); assert.equal(n, 4, 'once');
  I.afterWait(() => new Promise(() => {}), () => n++); t.mock.timers.tick(I.WAIT_MAX_MS - 1); assert.equal(n, 4); t.mock.timers.tick(1); assert.equal(n, 5, 'the cap');
  I.afterWait(() => Promise.reject(new Error('no')), () => n++); await settle(); assert.equal(n, 6, 'a failure starts it too');
  assert.equal(I.WAIT_MAX_MS, 120000);
  const map = fakeMap(6), f = fakeFetch(TIER0); let go; const wait = new Promise((r) => { go = r; });
  P.create(map, { base: BASE, L: fakeL(map), document: { ...fakeDoc(), readyState: 'complete' }, storage: storage(), fetch: f, waitFor: () => wait });
  t.mock.timers.tick(1500); await settle();
  assert.equal(f.asked.length, 0, 'the load + 1.5 s passed: still waiting for the overlay');
  go(); await settle(); assert.deepEqual(f.asked.slice(0, 1), [BASE + '/index.json'], 'then the downloads');
});

test('G34 B-6 / F-A6: the tier-0 stall timer is re-armed by every chunk (a slow but moving link loads), a download with no byte for STALL_MS is given up; every request asks for a low priority', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const inits = [], pending = [];
  function streamFetch() {
    return (url, init) => {
      inits.push(init);
      const name = url.split('/static/places/v1/')[1], a = TIER0[name];
      if (a.json) return Promise.resolve({ ok: true, status: 200, headers: { get: () => '100' }, json: () => Promise.resolve(a.json) });
      const u8 = new Uint8Array(a), half = u8.length >> 1, parts = [u8.subarray(0, half), u8.subarray(half)];
      let i = 0;
      const reader = { read: () => new Promise((res, rej) => {
        pending.push(() => res(i < parts.length ? { done: false, value: parts[i++] } : { done: true }));
        init.signal.addEventListener('abort', () => rej(new Error('aborted')), { once: true });
      }), cancel() {} };
      return Promise.resolve({ ok: true, status: 200, headers: { get: () => String(u8.length) }, body: { getReader: () => reader } });
    };
  }
  const step = async () => { const now = pending.splice(0); now.forEach((fn) => fn()); await settle(); };
  const st = new I.PlacesStore(BASE, streamFetch()), loading = st.load();
  await settle();
  for (let k = 0; k < 3; k++) { t.mock.timers.tick(I.STALL_MS - 1000); await step(); }     // 42 s in all, a byte every 14 s
  assert.equal(await loading, st, 'loaded'); assert.equal(st.ok('l') && st.ok('p'), true);
  assert.ok(inits.every((o) => o.priority === 'low' && o.mode === 'cors'), 'low priority');
  pending.length = 0;
  const s2 = new I.PlacesStore(BASE, streamFetch()), l2 = s2.load();
  await settle(); await step();                                  // the first chunks, then nothing
  t.mock.timers.tick(I.STALL_MS); await settle();
  assert.equal(await l2, null, 'no byte for 15 s: given up'); assert.equal(s2.unit.l.status, 'failed'); assert.equal(s2.unit.p.status, 'failed');
});

test('G34 B-4: the cells are kept by their decoded size; a cell the current view uses is never evicted; the measured widths are bounded', async () => {
  const st = new I.PlacesStore(BASE, fakeFetch(TIER0));
  st.view = 3;
  for (let i = 0; i < 10; i++) { st.cells.set('c' + i, { bytes: 1 << 20, used: i < 8 ? 2 : 3 }); st.bytes += 1 << 20; }
  st._trim();
  assert.ok(st.bytes <= I.MAX_CELL_BYTES_KEPT); assert.ok(!st.cells.has('c0') && st.cells.has('c4') && st.cells.has('c9'), 'the oldest first');
  st.view = 4; for (const [, c] of st.cells) c.used = 4;
  for (let i = 10; i < 14; i++) { st.cells.set('c' + i, { bytes: 1 << 20, used: 4 }); st.bytes += 1 << 20; }
  st._trim();
  assert.equal(st.cells.size, 10, 'all in the current view: none evicted (over the cap for that view)');
  st.newView(); assert.equal(st.view, 5);
  const lines = I.decodeLines(CELL_A), places = I.decodePlaces(PLACES0);
  assert.equal(I.decodedBytes(lines), lines.box.byteLength + lines.xy.byteLength + lines.lineStart.byteLength + lines.vertStart.byteLength + lines.mz.byteLength + lines.kind.byteLength);
  assert.ok(I.decodedBytes(places) > places.name.reduce((a, s) => a + 2 * s.length, 0), 'names counted');
  // a landing cell carries its decoded size and the view it came for; a touch moves it to the view in use
  const idx = { ...INDEX, tier1: { ...INDEX.tier1, cells: { '40_-120': [CELL_A.byteLength, 0] } } };
  const s2 = new I.PlacesStore(BASE, fakeFetch({ ...TIER0, 'index.json': { json: idx }, 'c/40_-120.l.bin': CELL_A }));
  await s2.load(); s2.view = 7; s2.request(['40_-120.l']); await settle();
  const c = s2.cells.get('40_-120.l');
  assert.equal(c.bytes, I.decodedBytes(c.data)); assert.equal(c.used, 7);
  s2.view = 9; s2.linesFor(tileOf(42, -119, 8)); assert.equal(c.used, 9);
  assert.ok(I.WIDTH_CACHE_MAX >= 1000);
  const map = fakeMap(6), pl = P.create(map, { base: BASE, L: fakeL(map), document: oahuDoc(), storage: storage(), fetch: fakeFetch(TIER0), whenReady: () => {} });
  for (let i = 0; i < I.WIDTH_CACHE_MAX + 10; i++) pl.textWidth('n' + i, false);
  assert.equal(pl.textWidth('abc', false), 18, 'still measured after the cache started again');
});

test('G34 B-7 / F-A5 / M25 / M40: a tile without a line keeps a 1x1 canvas, one with a line is grown; a line within 3 px of the tile is drawn (its halo bleeds in); both kinds off stops the downloads', async () => {
  // a second line 2 px above the top edge of tile row 24 at zoom 6 (beside the 40 N line)
  const yNear = I.yToLat((24 * 256 - 2) / 64);
  const lines = encodeLines([[0, [[[D(-120), D(-110)], [D(40), D(40)], 0]]], [0, [[[D(-120), D(-110)], [D(yNear), D(yNear)], 0]]]]);
  const map = fakeMap(4), f = fakeFetch({ ...TIER0, 'world-lines.bin': lines }), strokes = [];
  const mkCanvas = () => { const c = { width: 0, height: 0, style: {} }; c.getContext = () => ({ setTransform() {}, clearRect() {}, setLineDash() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() { strokes.push(c); }, measureText: (s) => ({ width: s.length * 6 }) }); return c; };
  const pl = P.create(map, { base: BASE, L: fakeL(map), document: fakeDoc(mkCanvas), storage: storage({ 'allshore.places.v1': '0' }), fetch: f, whenReady: (go) => go() });
  await settle();
  const border = pl.layer.createTile.call(pl.layer, tileOf(40, -115, 4)), sea = pl.layer.createTile.call(pl.layer, tileOf(-40, 60, 4));
  assert.equal(border.width, 256, 'a tile with a line: full size'); assert.equal(sea.width, 1, 'a tile with none: 1x1');
  assert.equal(border.style.width, '256px'); assert.equal(sea.style.width, '256px');
  // the tile just south of that line at zoom 6: the line lies under 3 px above its top edge
  const below = { z: 6, x: tileOf(yNear, -115, 6).x, y: 24 };
  const yLine = I.latToY(yNear) * 64, top = below.y * 256;
  assert.ok(top - yLine > 0 && top - yLine < 3, 'the line is ' + (top - yLine).toFixed(2) + ' px outside the tile');
  map.zoom = 6; strokes.length = 0;
  const edge = pl.layer.createTile.call(pl.layer, below);
  assert.ok(strokes.includes(edge), 'drawn: the 3-px margin at the call site'); assert.equal(edge.width, 256);
  // both off: queued and running cell downloads stop (M25)
  pl.store.queue.push('x.l'); let aborted = 0; pl.store.inflight['y.l'] = { abort: () => aborted++ };
  pl.set('borders', false);
  assert.equal(pl.store.queue.length, 0); assert.equal(aborted, 1); assert.deepEqual(pl.store.inflight, {}, 'City names was off already: both off now');
});

test('G34 B-8: the names are measured in the font they are drawn in (a hidden probe of each kind)', async () => {
  const fonts = [];
  const doc = {
    defaultView: { getComputedStyle: (e) => ({ fontSize: '14px', fontFamily: 'system-ui, sans-serif', fontWeight: /capital/.test(e.className) ? '700' : '600', fontStyle: 'normal', lineHeight: '14px' }) },
    createElement: (tag) => {
      const e = el(tag);
      e.removeChild = function (c) { this.children = this.children.filter((x) => x !== c); };
      if (tag === 'canvas') e.getContext = () => ({ set font(v) { this._f = v; }, get font() { return this._f; }, measureText(s) { fonts.push(this._f); return { width: s.length * 8 }; } });
      return e;
    }
  };
  const map = fakeMap(6, 21.3, -157.9), pl = P.create(map, { base: BASE, L: fakeL(map), document: doc, storage: storage(), fetch: fakeFetch(TIER0), whenReady: (go) => go() });
  await settle(); pl.update();
  assert.equal(pl.labels.children.filter((c) => c.style.visibility === 'hidden').length, 0, 'the probes removed');
  fonts.length = 0; pl.textWidth('Zzzq', false); pl.textWidth('Zzzr', true);
  assert.deepEqual(fonts, ['600 14px system-ui, sans-serif', '700 14px system-ui, sans-serif'], 'the drawn font, not the 11-px constant');
  // without getComputedStyle: the constants
  const map2 = fakeMap(6), fonts2 = [];
  const doc2 = { createElement: (tag) => { const e = el(tag); if (tag === 'canvas') e.getContext = () => ({ set font(v) { this._f = v; }, measureText(s) { fonts2.push(this._f); return { width: 1 }; } }); return e; } };
  const pl2 = P.create(map2, { base: BASE, L: fakeL(map2), document: doc2, storage: storage(), fetch: fakeFetch(TIER0), whenReady: (go) => go() });
  await settle(); pl2.update(); pl2.textWidth('Hilo', false);
  assert.deepEqual(fonts2, [I.LABEL_FONT]);
  // the label height follows the font: two names 24 px apart fit at the 14-px default, not at a 20-px line
  const items = () => [{ x: 100, y: 100, w: 30 }, { x: 100, y: 124, w: 30 }];
  assert.equal(I.thin(items(), [], 10, { x: 800, y: 600 }).length, 2);
  assert.equal(I.thin(items(), [], 10, { x: 800, y: 600 }, 20).length, 1);
});

test('G34 B-1: the floating windows (outside the map container) are obstacles while shown; a window opening, closing or moving lays the names out again', async () => {
  const ros = [], mos = [], q = [];
  const win = { requestAnimationFrame: (fn) => { q.push(fn); return q.length; },
    ResizeObserver: class { constructor(cb) { this.cb = cb; this.els = []; ros.push(this); } observe(e) { this.els.push(e); } unobserve() {} },
    MutationObserver: class { constructor(cb) { this.cb = cb; this.t = []; mos.push(this); } observe(tg, o) { this.t.push([tg, o]); } } };
  const M = load(win), flush = () => { while (q.length) q.shift()(); };
  const crop = fs.readFileSync(path.join(FIX, 'world-places-crop.bin'));
  const map = fakeMap(6, 21.3, -157.9);
  const p = map.latLngToContainerPoint([21.303, -157.858]);
  const fwin = { className: 'fwin forecast-win fw-min', hidden: false, getBoundingClientRect: () => ({ left: p.x - 20 + 10, top: p.y - 10 + 20, right: p.x + 120 + 10, bottom: p.y + 10 + 20 }) };
  const shut = { className: 'fwin live-win', hidden: true, getBoundingClientRect: () => ({ left: 0, top: 0, right: 2000, bottom: 2000 }) };
  const doc = { ...oahuDoc(), querySelectorAll: (sel) => (sel === '.fwin' ? [fwin, shut] : []) };
  const pl = M.create(map, { base: BASE, L: fakeL(map), document: doc, storage: storage(), fetch: fakeFetch({ ...TIER0, 'world-places.bin': ab(crop) }), whenReady: (go) => go() });
  await settle(); flush();
  const shown = () => pl.labels.children.filter((s) => !s.hidden).map((s) => s.textContent);
  assert.ok(!shown().includes('Honolulu') && shown().includes('Hilo'), 'under the forecast bar: Honolulu goes (a hidden window blocks nothing)');
  assert.ok(ros[0].els.includes(fwin) && ros[0].els.includes(shut), 'the windows watched for size');
  const attrMo = mos.find((m) => m.t.some(([tg]) => tg === fwin));
  assert.ok(attrMo, 'and for their class / style / hidden'); assert.deepEqual(attrMo.t[0][1], { attributes: true, attributeFilter: ['class', 'style', 'hidden'] });
  fwin.hidden = true; attrMo.cb([]); assert.equal(q.length, 1, 'a window closing lays the names out again'); flush();
  assert.ok(shown().includes('Honolulu'));
});

// ---- G34 re-check (fix round 2) ----
test('re-check R-2: only the files of the kinds that are on are loaded; each kind is drawn and credited as its file lands, in either order; a kind turned on later loads its file', async () => {
  const crop = fs.readFileSync(path.join(FIX, 'world-places-crop.bin'));
  const answers = { ...TIER0, 'world-places.bin': ab(crop) };
  // City names alone: the lines file is never asked for (re-check R-2: it held the names on a slow link)
  const map = fakeMap(6, 21.3, -157.9), f = fakeFetch(answers);
  const pl = P.create(map, { base: BASE, L: fakeL(map), document: oahuDoc(), storage: storage({ 'allshore.borders.v1': '0' }), fetch: f, whenReady: (go) => go() });
  await settle();
  assert.deepEqual(f.asked, [BASE + '/index.json', BASE + '/world-places.bin'], 'no lines with Borders off');
  assert.deepEqual(map.credits, ['+Places:']); assert.equal(pl.labels.hidden, false);
  pl.set('borders', true); await settle();
  assert.deepEqual(f.asked.slice(2), [BASE + '/world-lines.bin'], 'Borders turned on: its file, not the index again');
  assert.deepEqual(map.credits.slice(1), ['+Borders']);
  // both on, the places file first: the names and their credit at once, the lines still loading
  const strokes = [];
  const mk = () => { const c = { width: 0, height: 0, style: {} }; c.getContext = () => ({ setTransform() {}, clearRect() {}, setLineDash() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() { strokes.push(1); }, measureText: (t) => ({ width: t.length * 6 }) }); return c; };
  const order = async (first) => {
    const m = fakeMap(4, 30, -130), g = fakeFetch({ ...answers, hold: true });
    const p = P.create(m, { base: BASE, L: fakeL(m), document: fakeDoc(mk), storage: storage(), fetch: g, whenReady: (go) => go() });
    const t = tileOf(40, -115, 4), tile = p.layer.createTile.call(p.layer, t); p.layer._tiles.a = { el: tile, coords: t };
    g.held.shift()(); await settle();                                   // the index
    assert.deepEqual(g.asked.slice(1), [BASE + '/world-lines.bin', BASE + '/world-places.bin']);
    strokes.length = 0;
    const lines = g.held[0], places = g.held[1];
    return { m, p, tile, lines, places };
  };
  let o = await order();
  o.places(); await settle();
  assert.equal(o.p.store.ok('p'), true); assert.equal(o.p.store.ok('l'), false);
  assert.deepEqual(o.m.credits, ['+Places:'], 'credited as it lands'); assert.equal(o.p.labels.hidden, false, 'the names shown although the lines are still on their way');
  assert.equal(strokes.length, 0, 'no line yet');
  o.lines(); await settle();
  assert.deepEqual(o.m.credits, ['+Places:', '+Borders']); assert.equal(strokes.length, 2, 'the border tile drawn once (halo + core)');
  // the lines file first: the borders and their credit at once, no names yet; then the names
  o = await order();
  o.p.update();                                                        // a layout while the places are not usable yet
  o.lines(); await settle();
  assert.deepEqual(o.m.credits, ['+Borders']); assert.equal(strokes.length, 2); assert.equal(o.p.labels.hidden, true);
  o.places(); await settle();
  assert.deepEqual(o.m.credits, ['+Borders', '+Places:']); assert.equal(o.p.labels.hidden, false, 'laid out when its file lands (the earlier layout found nothing to show)');
});

test('re-check R-33 / R-2: a failed tier-0 file is asked again on a later view change once its cooldown has passed (the other kind loaded); nothing inside the cooldown', async () => {
  const answers = { ...TIER0, 'world-places.bin': 'down' }, map = fakeMap(6), f = fakeFetch(answers);
  const pl = P.create(map, { base: BASE, L: fakeL(map), document: oahuDoc(), storage: storage(), fetch: f, whenReady: (go) => go() });
  await settle();
  assert.equal(pl.store.ok('l'), true); assert.equal(pl.store.ok('p'), false);
  const n = f.asked.length;
  map.fire('zoomend'); await settle(); assert.equal(f.asked.length, n, 'inside the cooldown: nothing');
  answers['world-places.bin'] = PLACES0; pl.store.unit.p.failedAt -= I.RETRY_MS + 1;
  map.fire('zoomend'); await settle();
  assert.deepEqual(f.asked.slice(n), [BASE + '/world-places.bin'], 'the failed file again, alone');
  assert.equal(pl.store.ok('p'), true); assert.deepEqual(map.credits, ['+Borders', '+Places:']);
});

test('re-check R-5: a file whose names are not UTF-8 is refused as damaged (a cell: final, never asked again)', async () => {
  const bad = new Uint8Array(CELL_P.slice(0)); bad[bad.length - 2] = 0xff;
  assert.throws(() => I.decodePlaces(bad.buffer), /places-v1: names are not UTF-8/);
  const idx = { ...INDEX, tier1: { ...INDEX.tier1, cells: { '20_-160': [0, bad.byteLength] } } };
  const f = fakeFetch({ ...TIER0, 'index.json': { json: idx }, 'c/20_-160.p.bin': bad.buffer });
  const st = new I.PlacesStore(BASE, f);
  await st.load(); st.request(['20_-160.p']); await settle();
  assert.equal(st.failed['20_-160.p'], true, 'final');
  st.request(['20_-160.p']); await settle();
  assert.equal(f.asked.filter((u) => u.endsWith('20_-160.p.bin')).length, 1, 'never asked again');
});

test('re-check R-19 / R-20 / R-21: the font is read again after a map resize; the label height follows the probe\'s line height; a new font drops the measured widths', async () => {
  let size = '14px', lh = '20px';
  const fonts = [];
  const doc = {
    defaultView: { getComputedStyle: () => ({ fontSize: size, fontFamily: 'system-ui', fontWeight: '600', fontStyle: 'normal', lineHeight: lh }) },
    createElement: (tag) => {
      const e = el(tag);
      e.removeChild = function (c) { this.children = this.children.filter((x) => x !== c); };
      if (tag === 'canvas') e.getContext = () => ({ set font(v) { this._f = v; }, get font() { return this._f; }, measureText(s) { fonts.push(this._f); return { width: s.length * 8 }; } });
      return e;
    }
  };
  const map = fakeMap(6, 21.3, -157.9), pl = P.create(map, { base: BASE, L: fakeL(map), document: doc, storage: storage(), fetch: fakeFetch(TIER0), whenReady: (go) => go() });
  await settle(); pl.update();
  assert.equal(pl.fontUsed().h, 20, 'the probe\'s line height (R-20)');
  pl.textWidth('Hilo', false); fonts.length = 0; pl.textWidth('Hilo', false); assert.deepEqual(fonts, [], 'measured once');
  size = '18px'; lh = 'normal';
  pl.update(); pl.textWidth('Hilo', false); assert.deepEqual(fonts, [], 'no new probe without a resize');
  map.fire('resize'); pl.update();
  assert.equal(pl.fontUsed().n, '600 18px system-ui', 'read again after a resize (R-19)'); assert.equal(pl.fontUsed().h, 22, 'no line height given: 1.2 x the size');
  pl.textWidth('Hilo', false);
  assert.ok(fonts.length >= 1 && fonts.every((x) => x === '600 18px system-ui'), 'the old widths dropped: the names measured again, in the new font (R-21): ' + JSON.stringify(fonts));
});

test('re-check R-27: the gear and tools panels inside the map are watched (their opening and closing lay the names out again)', async () => {
  const ros = [], mos = [];
  const win = { requestAnimationFrame: (fn) => { fn(); return 1; },
    ResizeObserver: class { constructor(cb) { this.cb = cb; this.els = []; ros.push(this); } observe(e) { this.els.push(e); } unobserve() {} },
    MutationObserver: class { constructor(cb) { this.cb = cb; this.t = []; mos.push(this); } observe(tg, o) { this.t.push([tg, o]); } } };
  const M = load(win), map = fakeMap(6, 21.3, -157.9);
  const panel = { className: 'settings-panel', hidden: true, getBoundingClientRect: () => ({ left: 0, top: 0, right: 0, bottom: 0 }) };
  map.controls.push(panel);
  M.create(map, { base: BASE, L: fakeL(map), document: { ...oahuDoc(), querySelectorAll: () => [] }, storage: storage(), fetch: fakeFetch(TIER0), whenReady: () => {} });
  assert.ok(ros[0].els.includes(panel), 'watched for size');
  assert.ok(mos.some((m) => m.t.some(([tg, o]) => tg === panel && o.attributes)), 'and for class / style / hidden');
});

test('re-check R-12: a cell download that keeps receiving bytes is never cut by the stall timer; one with no byte for STALL_MS is', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const idx = { ...INDEX, tier1: { ...INDEX.tier1, cells: { '20_-160': [0, CELL_P.byteLength] } } };
  const pending = [];
  const cellFetch = (url, init) => {
    const name = url.split('/static/places/v1/')[1];
    if (name === 'index.json') return Promise.resolve({ ok: true, status: 200, headers: { get: () => '100' }, json: () => Promise.resolve(idx) });
    if (name !== 'c/20_-160.p.bin') { const a = TIER0[name]; return Promise.resolve({ ok: true, status: 200, headers: { get: () => String(a.byteLength) }, arrayBuffer: () => Promise.resolve(a) }); }
    const u8 = new Uint8Array(CELL_P), parts = [u8.subarray(0, 10), u8.subarray(10, 20), u8.subarray(20)];
    let i = 0;
    const reader = { read: () => new Promise((res, rej) => {
      pending.push(() => res(i < parts.length ? { done: false, value: parts[i++] } : { done: true }));
      init.signal.addEventListener('abort', () => rej(new Error('aborted')), { once: true });
    }), cancel() {} };
    return Promise.resolve({ ok: true, status: 200, headers: { get: () => String(u8.length) }, body: { getReader: () => reader } });
  };
  const st = new I.PlacesStore(BASE, cellFetch);
  await st.load(); st.request(['20_-160.p']); await settle();
  for (let k = 0; k < 4; k++) { t.mock.timers.tick(I.STALL_MS - 1000); pending.splice(0).forEach((fn) => fn()); await settle(); }   // 56 s in all
  assert.ok(st.cells.has('20_-160.p'), 'landed: each chunk re-arms the timer');
  const s2 = new I.PlacesStore(BASE, cellFetch);
  await s2.load(); s2.request(['20_-160.p']); await settle();
  pending.splice(0).forEach((fn) => fn()); await settle();
  t.mock.timers.tick(I.STALL_MS); await settle();
  assert.equal(s2.cells.has('20_-160.p'), false); assert.ok(s2.failed['20_-160.p'], 'given up after STALL_MS with no byte');
});

test('re-check R-7 / R-8 / R-43: a streamed file over its cap is refused and its reader cancelled; the stall limit is 15 s', async () => {
  let cancelled = 0, i = 0;
  const reader = { read: () => Promise.resolve(i++ < 3 ? { done: false, value: new Uint8Array(6) } : { done: true }), cancel: () => { cancelled++; return Promise.resolve(); } };
  await assert.rejects(I.readAll(reader, 10, () => {}), /places: file too large/);
  assert.equal(cancelled, 1, 'the reader cancelled');
  i = 0; const ok = { read: () => Promise.resolve(i++ < 2 ? { done: false, value: new Uint8Array(5) } : { done: true }), cancel() {} };
  assert.equal((await I.readAll(ok, 10, () => {})).byteLength, 10, 'exactly the cap: kept');
  assert.equal(I.STALL_MS, 15000); assert.equal(I.WAIT_MAX_MS, 120000, 'the overlay is waited for at most 120 s (owner, re-check R-1)');
});

test('re-check R-2 (idle): with nothing to wait for, the downloads still start only 1.5 s after the page load', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const map = fakeMap(6), f = fakeFetch(TIER0);
  P.create(map, { base: BASE, L: fakeL(map), document: { ...fakeDoc(), readyState: 'complete' }, storage: storage(), fetch: f, waitFor: () => Promise.resolve() });
  await settle(); assert.equal(f.asked.length, 0, 'not at once');
  t.mock.timers.tick(1499); await settle(); assert.equal(f.asked.length, 0);
  t.mock.timers.tick(1); await settle(); assert.equal(f.asked[0], BASE + '/index.json', 'at load + 1.5 s');
});

test('re-check R-16 / R-18: the decoded size counts each name exactly; the width cache starts again past its bound (an old name is measured anew)', async () => {
  const places = I.decodePlaces(PLACES0);
  const typed = places.lat.byteLength + places.lon.byteLength + places.rank.byteLength + places.mz.byteLength + places.pop.byteLength + places.flags.byteLength;
  assert.equal(I.decodedBytes(places), typed + places.name.reduce((a, s) => a + 2 * s.length + 32, 0));
  let calls = 0;
  const doc = { createElement: (tag) => { const e = el(tag); if (tag === 'canvas') e.getContext = () => ({ font: '', measureText: (s) => { calls++; return { width: s.length * 6 }; } }); return e; } };
  const map = fakeMap(6), pl = P.create(map, { base: BASE, L: fakeL(map), document: doc, storage: storage(), fetch: fakeFetch(TIER0), whenReady: () => {} });
  pl.textWidth('first', false); const c0 = calls;
  pl.textWidth('first', false); assert.equal(calls, c0, 'cached');
  for (let i = 0; i < I.WIDTH_CACHE_MAX + 5; i++) pl.textWidth('n' + i, false);
  const c1 = calls; pl.textWidth('first', false);
  assert.equal(calls, c1 + 1, 'the cache started again: measured anew');
});

test('re-check R-2: Borders turned on while the names file is still loading: its file is fetched once that load ends', async () => {
  const map = fakeMap(6), f = fakeFetch({ ...TIER0, hold: true });
  const pl = P.create(map, { base: BASE, L: fakeL(map), document: oahuDoc(), storage: storage({ 'allshore.borders.v1': '0' }), fetch: f, whenReady: (go) => go() });
  f.held.shift()(); await settle();                                     // the index; the places file now on its way
  assert.deepEqual(f.asked, [BASE + '/index.json', BASE + '/world-places.bin']);
  pl.set('borders', true); await settle();
  assert.equal(f.asked.length, 2, 'a load is running: nothing new yet');
  f.held.shift()(); await settle();                                     // the places file lands, the load ends
  assert.deepEqual(f.asked.slice(2), [BASE + '/world-lines.bin'], 'then the lines file');
  f.held.shift()(); await settle();
  assert.equal(pl.store.ok('l'), true); assert.deepEqual(map.credits, ['+Places:', '+Borders']);
});
