'use strict';
// Lake outlines under the wind overlay (plan section 51, step 3; no browser): the lake-outline coast set's store (its own
// overview file), the per-tile lake mask (zoom gate, no rasterising without a lake piece, a tile inside a lake draws no
// line), the line drawn by the coastline's own edge pass and only under wind, the redraws when the set arrives / goes /
// a chunk lands, the Overlay's gate (wind, tile zoom 4+, the setting), and the published set's overview against real
// lakes (Okeechobee, Superior with Isle Royale, Baikal).   node --test tests/overlay/lakeoutlines.test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

function load() {
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  global.L = {
    GridLayer: {
      prototype: { initialize(o) { this.options = o; this._tiles = {}; } },
      extend(p) {
        function C(o) { p.initialize.call(this, o); }
        C.prototype = Object.assign({ setOpacity() {}, addTo() { return this; } }, p);
        return C;
      }
    },
    DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} }
  };
  global.window = global;
  new Function(src)();
  return global.AllshoreOverlay._internals;
}
const I = load();
const TILE = 256;
const FIX = path.join(__dirname, '..', 'fixtures', 'coast');
const GRID = { cols: 1440, rows: 721, lon0: -180, lat0: 90, dlon: 0.25, dlat: -0.25, registration: 'center', lon_periodic: true };
const WIND = { lo: 0, hi: 61.73, legend: [0, 30.87], units: 'm/s', interpolation: 'bilinear' };
const HS = { lo: 0, hi: 22.86, legend: [0, 18.288], units: 'm', interpolation: 'bilinear' };
const frame = () => { const q = new Uint8Array(GRID.cols * GRID.rows); for (let i = 0; i < q.length; i++) q[i] = 1 + (i * 7) % 254; return { q, cols: GRID.cols, rows: GRID.rows }; };
const FR = frame();
function layer(field) {
  const l = new I.ModelGridLayer({ opacity: 1 }), f = field === 'hs' ? HS : WIND;
  l.setFrame(FR, GRID, field, f, I.buildLut(field, f.legend), { step: 3 });
  return l;
}
function canvas() {
  const el = { puts: 0, img: null };
  el.getContext = () => ({ clearRect() {}, createImageData: () => ({ data: new Uint8ClampedArray(TILE * TILE * 4) }), putImageData(img) { el.puts++; el.img = new Uint8ClampedArray(img.data); } });
  return el;
}
const key = (c) => c.x + ':' + c.y + ':' + c.z;
const px = (lat, lon, z) => { const p = I.pixelOf(lat, lon, z); return p.py * TILE + p.px; };
const tileOf = (lat, lon, z) => { const p = I.pixelOf(lat, lon, z); return { z, x: p.x, y: p.y }; };

// the coast-v1 encoder of coast.test.js (a mirror of tools/coast/build_coast.py)
function leb(v) { const out = []; do { const b = v % 128; v = Math.floor(v / 128); out.push(v ? b | 128 : b); } while (v); return out; }
const zz = (v) => (v < 0 ? -2 * v - 1 : 2 * v);
function encodeCoast(pieces, cell) {
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
const rev = (ring) => { const out = []; for (let i = ring.length - 2; i >= 0; i -= 2) out.push(ring[i], ring[i + 1]); return out; };
// a square lake (2 x 1.6 degrees) with a square island cut out (a clockwise ring)
const SET = I.decodeCoast(encodeCoast([[sq(10, 10, 12, 11.6), rev(sq(10.8, 10.6, 11.2, 11))]], 30));
function store(set, opts) {
  const st = Object.assign({ status: 'ok', rev: 0, onChange: null, onNeeded: null, tier0: set, asked: 0,
    index: { tier0: { max_zoom: 6 }, tier1: { cell: 5, cells: {} } }, tier1Zoom: () => false }, opts || {});
  if (!st.setsFor) st.setsFor = () => { st.asked++; return { sets: [set], complete: true }; };
  return st;
}
const LAKE_T = tileOf(10.8, 11, 6);                                             // the tile holding the whole lake at zoom 6

test('the constants: the set\'s prefix and overview, the zoom gate; the store asks for lakeoutlines-i.bin', () => {
  assert.equal(I.LAKEOUT_TIER0, 'lakeoutlines-i.bin'); assert.equal(I.LAKEOUT_PREFIX, 'static/coast/lakeoutlines-v1');
  assert.equal(I.LAKE_OUTLINE_MIN_ZOOM, 4);
  assert.equal(I.coastStore('https://u/static/coast/lakeoutlines-v1', I.LAKEOUT_TIER0).file, 'lakeoutlines-i.bin');
});

test('the tile\'s lake mask: none below zoom 4 or without a lake piece (nothing rasterised), the lake and its island at zoom 4+, LAND_ALL inside a lake; cached per tile', () => {
  const l = layer('wind'), st = store(SET);
  l.setLakeOutlineCoast(st);
  assert.equal(l._lakeOut, st); assert.equal(typeof st.onChange, 'function'); assert.equal(typeof st.onNeeded, 'function');
  const low = canvas();
  assert.equal(l._lakeOutFor(low, tileOf(10.8, 11, 3)), null, 'zoom 3: no mask'); assert.equal(st.asked, 0, 'and the set not asked');
  const el = canvas(), m = l._lakeOutFor(el, LAKE_T);
  assert.ok(m && m.length === TILE * TILE); assert.equal(st.asked, 1);
  assert.equal(m[px(10.3, 10.3, 6)], 255, 'lake water'); assert.equal(m[px(10.8, 11, 6)], 0, 'the island'); assert.equal(m[px(9.5, 9.5, 6)], 0, 'land');
  assert.equal(l._lakeOutFor(el, LAKE_T), m, 'cached'); assert.equal(st.asked, 1);
  assert.equal(l._lakeOutFor(canvas(), tileOf(10.8, 11, 4)) !== null, true, 'zoom 4: a mask');
  // a tile no lake piece touches: null, no rasterising (rasterise would be asked for an empty path list otherwise)
  const far = canvas();
  assert.equal(l._lakeOutFor(far, tileOf(40, 40, 6)), null);
  // a tile inside the lake: LAND_ALL (every pixel lake water)
  assert.equal(l._lakeOutFor(canvas(), tileOf(10.3, 10.2, 12)), I.LAND_ALL);
  l.setLakeOutlineCoast(null); assert.equal(l._lakeOut, null); assert.equal(st.onChange, null); assert.equal(st.onNeeded, null);
  assert.equal(l._lakeOutFor(el, LAKE_T), null, 'no set: no mask');
});

test('the line: the coastline\'s own edge pass over the lake mask, under wind only; elsewhere the tile is byte-identical; a tile inside a lake gets no line', () => {
  const plain = layer('wind'), withL = layer('wind'), st = store(SET);
  withL.setLakeOutlineCoast(st);
  const a = canvas(), b = canvas();
  plain._draw(a, LAKE_T); withL._draw(b, LAKE_T);
  const mask = withL._lakeOutFor(canvas(), LAKE_T), edges = I.coastEdges(mask, I.WIND_LOOK.gain);
  assert.ok(edges.n > 100, 'a shore around the lake and the island');
  const expect = I.applyEdges(edges, new Uint8ClampedArray(a.img), I.WIND_LOOK.ink);
  assert.deepEqual(Array.from(b.img), Array.from(expect), 'the wind tile = the plain tile with the edges applied');
  const onEdge = new Set(Array.from(edges.idx));
  let changed = 0, offEdge = 0;
  for (let i = 0; i < TILE * TILE; i++) {
    const d = a.img[i * 4] !== b.img[i * 4] || a.img[i * 4 + 3] !== b.img[i * 4 + 3];
    if (d) { changed++; if (!onEdge.has(i)) offEdge++; }
  }
  assert.ok(changed > 100); assert.equal(offEdge, 0, 'only shore pixels change');
  const isl = px(10.6, 11, 6), shore = px(10, 11, 6);                           // the island's south shore, the lake's
  assert.ok(onEdge.has(isl) || onEdge.has(isl - TILE) || onEdge.has(isl + TILE), 'the island shore is drawn');
  assert.ok(onEdge.has(shore) || onEdge.has(shore - TILE) || onEdge.has(shore + TILE), 'the lake shore is drawn');
  // under wave height: no lake pass at all
  const h1 = layer('hs'), h2 = layer('hs'), c1 = canvas(), c2 = canvas();
  h2.setLakeOutlineCoast(store(SET));
  h1._draw(c1, LAKE_T); h2._draw(c2, LAKE_T);
  assert.deepEqual(Array.from(c2.img), Array.from(c1.img), 'wave height: byte-identical');
  assert.equal(c2._ovLo, undefined, 'and no mask computed');
  // a tile inside the lake: nothing drawn on top
  const inner = tileOf(10.3, 10.2, 12), p = canvas(), q = canvas();
  plain._draw(p, inner); withL._draw(q, inner);
  assert.deepEqual(Array.from(q.img), Array.from(p.img));
  // below zoom 4: identical
  const z3 = tileOf(10.8, 11, 3), r = canvas(), s = canvas();
  plain._draw(r, z3); withL._draw(s, z3);
  assert.deepEqual(Array.from(s.img), Array.from(r.img));
});

test('redraws: the set arriving redraws the outlined tiles from zoom 4, its removal redraws only the tiles that had a mask; a chunk landing redraws a stand-in tile only when its mask changed', () => {
  const l = layer('wind'), st = store(SET);
  const tiles = { lake: LAKE_T, far: tileOf(40, 40, 6), low: tileOf(10.8, 11, 3) };
  for (const n in tiles) l._tiles[key(tiles[n])] = { el: canvas(), coords: tiles[n] };
  const drawn = []; const draw0 = l._draw; l._draw = function (el, c) { drawn.push(key(c)); return draw0.call(this, el, c); };
  l.setLakeOutlineCoast(st);
  assert.deepEqual(drawn.sort(), [key(tiles.far), key(tiles.lake)].sort(), 'zoom 4+ tiles redrawn; zoom 3 not');
  drawn.length = 0;
  l.setLakeOutlineCoast(null);
  assert.deepEqual(drawn, [key(tiles.lake)], 'only the tile that had a mask');
  assert.equal(l._tiles[key(tiles.lake)].el._ovLo, null);
  // a stand-in (complete: false) tile: re-evaluated when the store moves on
  let complete = false;
  const st2 = store(SET, { setsFor: () => ({ sets: [SET], complete }) });
  l.setLakeOutlineCoast(st2);
  const el = l._tiles[key(tiles.lake)].el;
  assert.equal(el._ovLoFinal, false);
  drawn.length = 0;
  st2.rev++; st2.onChange();
  assert.deepEqual(drawn, [], 'the same stand-in mask: no redraw');
  complete = true; st2.setsFor = () => ({ sets: [I.decodeCoast(encodeCoast([[sq(10, 10, 12, 11.6)]], 5))], complete: true });
  st2.rev++; st2.onChange();
  assert.deepEqual(drawn, [key(tiles.lake)], 'the full-resolution mask differs: redrawn'); assert.equal(el._ovLoFinal, true);
  // the cells the tiles need: only outlined tiles at a tier-1 zoom
  const st3 = store(SET, { tier1Zoom: (z) => z >= 7 });
  l.setLakeOutlineCoast(st3);
  assert.deepEqual(st3.onNeeded(), {}, 'no tile at a tier-1 zoom');
  const t7 = tileOf(10.8, 11, 7); l._tiles[key(t7)] = { el: canvas(), coords: t7 };
  assert.deepEqual(Object.keys(st3.onNeeded()), I.coastCellsForTile(t7, 5));
  l.setFrame(FR, GRID, 'hs', HS, I.buildLut('hs', HS.legend), { step: 3 });
  assert.deepEqual(st3.onNeeded(), {}, 'wave height: none');
});

test('V7-5: the set taken away under another field (wave height, SST): every tile lets its mask go, none is redrawn; under wind the masked ones are redrawn', () => {
  const l = layer('wind'), st = store(SET);
  l._tiles[key(LAKE_T)] = { el: canvas(), coords: LAKE_T };
  l.setLakeOutlineCoast(st);
  const el = l._tiles[key(LAKE_T)].el;
  assert.ok(el._ovLo, 'a mask under wind');
  l.setFrame(FR, GRID, 'hs', HS, I.buildLut('hs', HS.legend), { step: 3 });
  assert.ok(el._ovLo, 'the switch to wave height alone keeps it until the set is taken away');
  assert.ok(el._ovLoEdgesOf === el._ovLo && el._ovLoEdges, 'the edge cache holds the mask');
  const drawn = []; const draw0 = l._draw; l._draw = function (e, c) { drawn.push(key(c)); return draw0.call(this, e, c); };
  l.setLakeOutlineCoast(null);
  assert.equal(el._ovLo, null, 'the mask let go'); assert.equal(el._ovLoKey, null);
  assert.equal(el._ovLoEdges, null, 'the edge list too'); assert.equal(el._ovLoEdgesOf, null, 'and the edge cache’s mask (G34 F-A3)');
  assert.deepEqual(drawn, [], 'wave height draws no lake line: nothing to redraw');
});

// the Overlay's gate: wind, a tile zoom of 4 or more, a model field (not SST); always on (owner, 2026-10-23)
function overlay(o) {
  const ov = Object.create(I.Overlay.prototype);
  let loads = 0, zoom = o.zoom === undefined ? 6 : o.zoom;
  const st = { status: 'idle', file: I.LAKEOUT_TIER0, aborted: 0, dropped: 0, load() { loads++; this.status = 'ok'; return Promise.resolve(o.fail ? null : this); }, abortAll() { this.aborted++; }, dropQueue() { this.dropped++; } };
  const given = [];
  Object.assign(ov, { lakeOutCoast: st, mode: o.mode || 'model', field: o.field || 'wind',
    map: { getZoom: () => zoom },
    layer: { _lakeOut: null, setLakeOutlineCoast(s) { this._lakeOut = s; given.push(s); } } });
  return { ov, st, given, loads: () => loads, setZoom: (z) => { zoom = z; } };
}
const settle = async () => { for (let i = 0; i < 4; i++) await new Promise((r) => setImmediate(r)); };

test('the Overlay: the set loads and reaches the layer only under wind at tile zoom 4+ (no setting: always on); not wanted any more, it is taken from the layer and its queue dropped (V7-5)', async () => {
  let t = overlay({});
  t.ov._syncLakeOutlines(); await settle();
  assert.equal(t.loads(), 1); assert.deepEqual(t.given, [t.st]);
  t.ov._syncLakeOutlines(); await settle();
  assert.equal(t.loads(), 1, 'already on the layer: nothing more');
  for (const o of [{ field: 'hs' }, { field: 'tp' }, { field: 'sst', mode: 'obs' }, { zoom: 3.4 }, { mode: 'obs' }, { on: false, field: 'wind' }]) {
    t = overlay(o); t.ov._syncLakeOutlines(); await settle();
    assert.equal(t.loads(), o.on === false ? 1 : 0, JSON.stringify(o)); assert.deepEqual(t.given, o.on === false ? [t.st] : [], 'an old "off" option is no setting any more');
  }
  t = overlay({ zoom: 3.5 }); t.ov._syncLakeOutlines(); await settle();
  assert.equal(t.loads(), 1, 'zoom 3.5: tile zoom 4');
  t = overlay({ zoom: 2 }); t.ov._syncLakeOutlines(); await settle(); assert.equal(t.loads(), 0);
  t.setZoom(5); t.ov._syncLakeOutlines(); await settle();
  assert.equal(t.loads(), 1, 'zooming in loads it');
  t = overlay({ fail: true }); t.ov._syncLakeOutlines(); await settle();
  assert.deepEqual(t.given, [], 'a failed load gives the layer nothing');
  assert.equal(I.Overlay.prototype.setLakeOutlines, undefined, 'no setting');
  // not wanted any more (another field, SST, a lower zoom): taken from the layer, the queue dropped; given back after
  for (const leave of [(x) => { x.ov.field = 'hs'; }, (x) => { x.ov.mode = 'obs'; x.ov.field = 'sst'; }, (x) => { x.setZoom(3); }]) {
    t = overlay({}); t.ov._syncLakeOutlines(); await settle();
    leave(t); t.ov._syncLakeOutlines();
    assert.deepEqual(t.given, [t.st, null]); assert.equal(t.st.dropped, 1); assert.equal(t.st.aborted, 0, 'decoded data stays (cheap to give back)');
    t.ov._syncLakeOutlines(); assert.deepEqual(t.given, [t.st, null], 'already off the layer: given nothing'); assert.equal(t.st.dropped, 2);
    t.ov.field = 'wind'; t.ov.mode = 'model'; t.setZoom(6); t.ov._syncLakeOutlines(); await settle();
    assert.deepEqual(t.given, [t.st, null, t.st], 'wanted again: given back');
  }
  // a late answer after it stopped being wanted is not given
  t = overlay({}); t.ov._syncLakeOutlines(); t.ov.field = 'hs'; await settle();
  assert.deepEqual(t.given, []);
  // without a layer or a store: nothing
  t = overlay({}); t.ov.layer = null; t.ov._syncLakeOutlines(); await settle(); assert.equal(t.loads(), 0);
});

// The published set (static/coast/lakeoutlines-v1, built 2026-10-23): index.json + lakeoutlines-i.bin
test('the published lake-outline set: the store loads index.json and lakeoutlines-i.bin; real lakes are water, their islands and the land are not; the wind tile over Lake Okeechobee draws its shore', async () => {
  const idx = JSON.parse(fs.readFileSync(path.join(FIX, 'lakeoutlines_index.json'), 'utf8')), bin = fs.readFileSync(path.join(FIX, 'lakeoutlines-i.bin'));
  const asked = [];
  global.fetch = (url) => {
    asked.push(url);
    if (url.endsWith('/index.json')) return Promise.resolve({ ok: true, status: 200, headers: { get: () => '9184' }, json: () => Promise.resolve(idx) });
    if (url.endsWith('/lakeoutlines-i.bin')) return Promise.resolve({ ok: true, status: 200, headers: { get: () => String(bin.length) }, arrayBuffer: () => Promise.resolve(bin.buffer.slice(bin.byteOffset, bin.byteOffset + bin.length)) });
    return Promise.resolve({ ok: false, status: 404 });
  };
  try {
    const base = 'https://p/' + I.LAKEOUT_PREFIX, st = I.coastStore(base, I.LAKEOUT_TIER0);
    assert.equal(await st.load(), st);
    assert.deepEqual(asked.slice().sort(), [base + '/index.json', base + '/lakeoutlines-i.bin']);
    assert.equal(st.index.kind, 'lakeoutlines'); assert.equal(st.tier1Zoom(6), false); assert.equal(st.tier1Zoom(7), true);
    const l = layer('wind');
    l.setLakeOutlineCoast(st);
    const at = (lat, lon) => { const t = tileOf(lat, lon, 6), m = l._lakeOutFor(canvas(), t); return m === I.LAND_ALL ? 255 : m ? m[px(lat, lon, 6)] : 0; };
    for (const [name, lat, lon] of [['Okeechobee', 26.95, -80.83], ['Superior', 47.7, -87.5], ['Baikal', 53.5, 108.2], ['Victoria', -1.0, 33.0], ['Titicaca', -15.8, -69.4], ['Caspian', 42.0, 50.5]]) {
      assert.equal(at(lat, lon), 255, name);
    }
    for (const [name, lat, lon] of [['Isle Royale', 48.0, -88.9], ['Olkhon', 53.15, 107.35], ['Orlando', 28.54, -81.38], ['Paris', 48.86, 2.35]]) {
      assert.equal(at(lat, lon), 0, name);
    }
    const t = tileOf(26.95, -80.83, 6), plain = layer('wind'), a = canvas(), b = canvas();
    plain._draw(a, t); l._draw(b, t);
    let diff = 0; for (let i = 0; i < TILE * TILE; i++) if (a.img[i * 4] !== b.img[i * 4]) diff++;
    assert.ok(diff > 40, 'Okeechobee\'s shore drawn (' + diff + ' px)');
  } finally { delete global.fetch; }
});
