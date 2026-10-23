'use strict';
// static_ui/places.js, the border lines (plan section 51, step 4): the lines-v1 decoder (a JS mirror of the builder's
// encoder, refusals, two real published cells against the Python decoder), the cells of a tile, the store (tier 0 /
// cells / stand-in, at most two downloads, LRU, final vs cooldown failures, idle), the per-kind and per-line zoom gates,
// the tile's paths (world copies, margins, the dateline guard), the drawing order, and create(): the pane, the settings,
// nothing fetched before the page is ready or while every kind is off.   node --test tests/ui/places.test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { encodeLines } = require('./placesenc.js');

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
  const big = new Float64Array(2); assert.equal(I.readVarints(new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x0f, 0x05]), 0, big), 2); assert.equal(big[0], 4294967295);
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
  assert.throws(() => I.readVarints(new Uint8Array([0x80, 0x80, 0x80, 0x80, 0x80, 0x01]), 0, new Float64Array(2)), /longer than 5 bytes/);
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
const INDEX = { format: 'places-v1', tier0: { lines: { file: 'world-lines.bin' }, places: { file: 'world-places.bin' } },
  tier1: { dir: 'c', cell: 10, min_zoom: 8, cells: { '40_-120': [CELL_A.byteLength, 100], '30_-120': [0, 50], '20_-160': [500, 10] } } };
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
  const f = fakeFetch({ 'index.json': { json: INDEX }, 'world-lines.bin': LINES0, 'c/40_-120.l.bin': CELL_A });
  const st = new I.PlacesStore(BASE, f);
  assert.deepEqual(st.linesFor({ z: 3, x: 1, y: 3 }), { sets: [], complete: false }, 'not loaded: nothing');
  assert.equal(await st.load(), st);
  assert.deepEqual(f.asked, [BASE + '/index.json', BASE + '/world-lines.bin']);
  assert.equal(st.status, 'ok'); assert.equal(st.lines0.n, 1);
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
  const f = fakeFetch({ hold: true, 'index.json': { json: idx }, 'world-lines.bin': LINES0, 'c/40_-120.l.bin': CELL_A, 'c/40_-110.l.bin': 'down' });
  const st = new I.PlacesStore(BASE, f);
  const loading = st.load(); while (f.held.length) f.held.shift()(); await settle(); while (f.held.length) f.held.shift()(); await loading;
  st.request(['40_-120', '40_-110', '40_-100', '40_-90']);
  assert.equal(Object.keys(st.inflight).length, I.MAX_INFLIGHT); assert.equal(st.queue.length, 2);
  while (f.held.length) f.held.shift()(); await settle();
  assert.ok(st.cells.has('40_-120')); assert.equal(typeof st.failed['40_-110'], 'number', '503: a cooldown');
  while (f.held.length) f.held.shift()(); await settle();
  assert.equal(st.failed['40_-100'], true, '404: final'); assert.equal(st.failed['40_-90'], true);
  st.request(['40_-100', '40_-110']); assert.equal(st.queue.length + Object.keys(st.inflight).length, 0, 'neither asked again now');
  // a tile over a final cell keeps tier 0, complete
  const t = tileOf(45, -95, 8);
  assert.deepEqual(I.cellsForTile(t, 10), ['40_-100']);
  assert.deepEqual(st.linesFor(t), { sets: [st.lines0], complete: true }, 'over a cell that cannot be served: tier 0 for good');
  assert.deepEqual(I.cellsForTile(tileOf(45, -105, 8), 10), ['40_-110']);
  assert.deepEqual(st.linesFor(tileOf(45, -105, 8)), { sets: [st.lines0], complete: false }, 'over a cell in its cooldown: tier 0, not final');
  st.request(['40_-120']); assert.equal(st.queue.length, 0, 'already decoded');
  // a cell file of another cell size is refused for good (tier 0 there)
  const other = encodeLines([[0, [[[D(-79), D(-78)], [D(41), D(41)], 0]]]], 5);
  const g = fakeFetch({ 'index.json': { json: { ...INDEX, tier1: { ...INDEX.tier1, cells: { '40_-80': [10, 0] } } } }, 'world-lines.bin': LINES0, 'c/40_-80.l.bin': other });
  const so = new I.PlacesStore(BASE, g); await so.load(); so.request(['40_-80']); await settle();
  assert.equal(so.failed['40_-80'], true); assert.equal(so.cells.size, 0);
  st.queue.push('40_-90x'); st.idle(); assert.equal(st.queue.length, 0); assert.equal(st.retryTimer, null);
});

test('the store: a failed load answers null and is retried only after the cooldown; an index of another kind is refused; the LRU keeps 6 MB', async () => {
  const f = fakeFetch({ 'index.json': 'down' });
  const st = new I.PlacesStore(BASE, f);
  assert.equal(await st.load(), null); assert.equal(st.status, 'failed');
  assert.equal(await st.load(), null); assert.equal(f.asked.length, 1, 'no second request inside the cooldown');
  st.failedAt -= I.RETRY_MS + 1; await st.load(); assert.equal(f.asked.length, 2, 'retried after it');
  const g = fakeFetch({ 'index.json': { json: { ...INDEX, format: 'coast-v1' } }, 'world-lines.bin': LINES0 });
  assert.equal(await new I.PlacesStore(BASE, g).load(), null);
  assert.equal(I.validIndex({ ...INDEX, tier1: { ...INDEX.tier1, cell: 5 } }), false); assert.equal(I.validIndex({ ...INDEX, tier1: { ...INDEX.tier1, dir: '../x' } }), false);
  assert.equal(I.validIndex({ ...INDEX, tier0: { lines: { file: 'a/b.bin' } } }), false); assert.equal(I.validIndex(INDEX), true);
  const lru = new I.PlacesStore(BASE, f);
  for (let i = 0; i < 10; i++) { lru.cells.set('c' + i, { bytes: 1 << 20 }); lru.bytes += 1 << 20; lru._trim(); }
  assert.ok(lru.bytes <= I.MAX_CELL_BYTES_KEPT); assert.ok(lru.cells.has('c9') && !lru.cells.has('c0'));
});

// ---- create() ----
function fakeMap(z) {
  const panes = {}, layers = new Set(), ev = {};
  return { zoom: z, getZoom() { return this.zoom; }, getPane: (n) => panes[n], createPane: (n) => (panes[n] = { style: {} }), panes,
    hasLayer: (l) => layers.has(l), removeLayer: (l) => { layers.delete(l); }, _add: (l) => layers.add(l), layers,
    on: (e, f) => { (ev[e] = ev[e] || []).push(f); }, fire: (e) => (ev[e] || []).forEach((f) => f()) };
}
function fakeL(map) {
  return { GridLayer: { extend(p) { function C(o) { this.options = o; this._tiles = {}; } C.prototype = Object.assign({ addTo(m) { m._add(this); return this; } }, p); return C; } } };
}
function storage(init) { const m = new Map(Object.entries(init || {})); return { m, getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)) }; }

test('create(): its own pane (z 340, no pointer events); both kinds on by default; nothing fetched before the page is ready, below zoom 3, or with both kinds off; the settings stored', async () => {
  const map = fakeMap(6), f = fakeFetch({ 'index.json': { json: INDEX }, 'world-lines.bin': LINES0 });
  let start = null;
  const s = storage();
  const pl = P.create(map, { base: BASE, L: fakeL(map), document: { createElement: () => ({ style: {}, getContext: () => null }) }, storage: s, fetch: f, whenReady: (go) => { start = go; } });
  assert.equal(map.panes.placesLinePane.style.zIndex, 340); assert.equal(map.panes.placesLinePane.style.pointerEvents, 'none');
  assert.equal(pl.on('borders'), true); assert.equal(pl.on('states'), true);
  assert.ok(map.hasLayer(pl.layer));
  assert.equal(f.asked.length, 0, 'nothing before the page is ready');
  start(); await settle();
  assert.deepEqual(f.asked, [BASE + '/index.json', BASE + '/world-lines.bin']);
  pl.set('borders', false); pl.set('states', false);
  assert.equal(s.m.get('allshore.borders.v1'), '0'); assert.equal(s.m.get('allshore.states.v1'), '0');
  assert.ok(!map.hasLayer(pl.layer), 'both off: the layer goes');
  pl.set('states', true); assert.ok(map.hasLayer(pl.layer)); assert.equal(s.m.get('allshore.states.v1'), '1');
  pl.set('nope', true); assert.equal(s.m.has('nope'), false);
  // a fresh page at zoom 2 with the saved settings: nothing until zoom 3 (borders) / 5 (states only)
  const map2 = fakeMap(2), f2 = fakeFetch({ 'index.json': { json: INDEX }, 'world-lines.bin': LINES0 });
  const pl2 = P.create(map2, { base: BASE, L: fakeL(map2), document: { createElement: () => ({}) }, storage: storage({ 'allshore.borders.v1': '0' }), fetch: f2, whenReady: (go) => go() });
  assert.equal(pl2.on('borders'), false); assert.equal(pl2.on('states'), true);
  await settle(); assert.equal(f2.asked.length, 0);
  map2.zoom = 4; map2.fire('zoomend'); await settle(); assert.equal(f2.asked.length, 0, 'states only: not before zoom 5');
  map2.zoom = 5; map2.fire('zoomend'); await settle(); assert.equal(f2.asked.length, 2);
  // a storage that throws: on, and changes never throw
  const pl3 = P.create(fakeMap(6), { base: BASE, L: fakeL(fakeMap(6)), document: { createElement: () => ({}) }, storage: { getItem() { throw new Error('x'); }, setItem() { throw new Error('x'); } }, whenReady: () => {} });
  assert.equal(pl3.on('borders'), true); pl3.set('borders', false); assert.equal(pl3.on('borders'), false);
  // without a base the rows still work and nothing loads
  const pl4 = P.create(fakeMap(6), { L: fakeL(fakeMap(6)), document: { createElement: () => ({}) }, whenReady: (go) => go() });
  assert.equal(pl4.store, null); assert.equal(pl4.wanted(), false);
});

test('create(): tiles painted from the store: halo and core strokes for a tile with a border, a blank tile while loading, redrawn when the data lands', async () => {
  const map = fakeMap(4), f = fakeFetch({ hold: true, 'index.json': { json: INDEX }, 'world-lines.bin': LINES0 });
  const strokes = [];
  const mkCanvas = () => { const c = { width: 0, height: 0, style: {} }; c.getContext = () => ({ setTransform() {}, clearRect() { strokes.push('clear'); }, setLineDash() {}, beginPath() {}, moveTo() {}, lineTo() {}, stroke() { strokes.push('stroke'); } }); return c; };
  const pl = P.create(map, { base: BASE, L: fakeL(map), document: { createElement: mkCanvas }, storage: storage(), fetch: f, whenReady: (go) => go() });
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
