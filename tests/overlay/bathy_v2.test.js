'use strict';
// The bathymetry layer on data v2 (plan section 93b, G61 fix round): real tiles of the builder's land-marked Oahu build
// (tests/fixtures/bathy/v2: regions.json, the region's index (land_id 30), one presence file, three z15 tiles at Haleiwa),
// read here by an independent PNG decoder; plus the store's own rules (eviction, retry, stall, Off) the G61 reviewers'
// mutants showed untested.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const { world, settle, until } = require('./radar_world.js');

const FIX = path.join(__dirname, '..', 'fixtures', 'bathy');
const raw = (n) => fs.readFileSync(path.join(FIX, n));
const bytes = (n) => { const b = raw(n); return b.buffer.slice(b.byteOffset, b.byteOffset + b.length); };
const json = (n) => JSON.parse(raw(n).toString('utf8'));
const I = world().I;
const LAND = 30, P = 258 * 258, A = 3, W = 256 + 2 * A, ENTRY = 258 * 774 + P, MAX = 32 * 1024 * 1024;
const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// ---- an independent reader of the builder's tiles ----
function readPng(buf) {
  let off = 8, w = 0, h = 0; const idat = [];
  while (off < buf.length) {
    const len = buf.readUInt32BE(off), type = buf.toString('ascii', off + 4, off + 8), data = buf.subarray(off + 8, off + 8 + len);
    if (type === 'IHDR') { w = data.readUInt32BE(0); h = data.readUInt32BE(4); }
    else if (type === 'IDAT') idat.push(data);
    off += 12 + len;
  }
  const z = zlib.inflateSync(Buffer.concat(idat)), out = new Uint8Array(w * h);
  for (let y = 0; y < h; y++) {
    const ft = z[y * (w + 1)], line = z.subarray(y * (w + 1) + 1, (y + 1) * (w + 1));
    for (let x = 0; x < w; x++) {
      const a = x ? out[y * w + x - 1] : 0, b = y ? out[(y - 1) * w + x] : 0, c = x && y ? out[(y - 1) * w + x - 1] : 0;
      let p;
      if (ft === 0) p = 0; else if (ft === 1) p = a; else if (ft === 2) p = b; else if (ft === 3) p = (a + b) >> 1;
      else { const q = a + b - c, pa = Math.abs(q - a), pb = Math.abs(q - b), pc = Math.abs(q - c); p = pa <= pb && pa <= pc ? a : pb <= pc ? b : c; }
      out[y * w + x] = (line[x] + p) & 255;
    }
  }
  assert.equal(w, 258); assert.equal(h, 774);
  return out;
}
function depthOf(c) {
  if (c === 0 || c > 65401) return NaN;
  if (c <= 41001) return (c - 1) / 100 - 10;
  if (c <= 62001) return 400 + (c - 41001) / 10;
  return 2500 + (c - 62001) * 2.5;
}
// node (r, c): {code, depth, src, flags}
function nodes(x) {
  const q = readPng(raw('v2/h15_' + x + '_14403.png'));
  return (r, c) => { const i = r * 258 + c, code = (q[i] << 8) | q[i + P]; return { code, depth: depthOf(code), src: q[i + 2 * P] & 0x1F, sf: q[i + 2 * P] }; };
}
function centre(x, y, z, px, py) {
  const n = 256 * Math.pow(2, z), X = x * 256 + px + 0.5, Y = y * 256 + py + 0.5;
  return { lat: Math.atan(Math.sinh(Math.PI * (1 - 2 * Y / n))) * 180 / Math.PI, lng: X / n * 360 - 180 };
}
// A GEBCO tile made up here: the same depth everywhere (a stand-in that would show wherever nothing above it stops it).
function gebcoAll(d) {
  const q = new Uint8Array(258 * 774), c = I.bathyCode(d);
  for (let i = 0; i < P; i++) { q[i] = c >> 8; q[i + P] = c & 255; q[i + 2 * P] = 0x80; }
  return { q, shade: null, dz: 7, tx: 7, ty: 56, reg: 'gebco' };
}
async function v2store(w) {
  const st = w.I.bathyStore('https://x'); const p = st.load(); w.answer(/regions\.json$/, json('v2/regions.json')); await p;
  assert.equal(st.status, 'ok');
  return st;
}
function layerOn(w, st) {
  const layer = new w.I.BathyLayer(); layer._map = w.map; layer._queueStale = function () {}; layer._cancelStale = function () {};
  layer.setStore(st);
  return layer;
}
function addTile(w, layer, c) {
  const el = w.g.document.createElement('canvas'); el.width = 256; el.height = 256;
  layer._tiles[c.x + ':' + c.y + ':' + c.z] = { el, coords: c };
  return el;
}
// Answers every bathymetry request from the v2 fixtures (404 for the rest); `hold` keeps the matching ones unanswered.
function serve(w, hold) {
  let any = false;
  for (const u of w.pending()) {
    const m = /\/static\/bathy\/(.*)$/.exec(u); if (!m) continue;
    if (hold && hold.test(u)) continue;
    const f = m[1]; let name = null;
    if (f === 'hawaii/v2/index.json') name = 'v2/hawaii_index.json';
    else if (f === 'gebco/v1/index.json') name = 'gebco_index.json';
    else if (f === 'hawaii/v2/idx/63_450.bin') name = 'v2/idx/63_450.bin';
    else { const t = /^hawaii\/v2\/t\/15\/(\d+)\/14403\.png$/.exec(f); if (t) name = 'v2/h15_' + t[1] + '_14403.png'; }
    if (name && fs.existsSync(path.join(FIX, name))) w.answer(new RegExp(esc(u) + '$'), bytes(name));
    else w.answer(new RegExp(esc(u) + '$'), null, 404);
    any = true;
  }
  return any;
}
async function drawAll(w, st, layer, hold) {
  for (let round = 0; round < 12; round++) {
    for (const k in layer._tiles) layer._draw(layer._tiles[k].el, layer._tiles[k].coords);
    const answered = serve(w, hold);
    if (!answered && !Object.keys(st.inflight).length) break;
    await until(() => !Object.keys(st.inflight).length || (hold && w.pending().every((u) => hold.test(u))));
  }
  for (const k in layer._tiles) layer._draw(layer._tiles[k].el, layer._tiles[k].coords);
}

test('v2 fixtures: the index carries land_id 30 and two sources; the presence file lists the three Haleiwa tiles', async () => {
  const w = world(), st = await v2store(w);
  const reg = st.regions.find((r) => r.name === 'hawaii');
  assert.equal(reg.v, 2);
  const ix = w.I.validBathyIndex(json('v2/hawaii_index.json'), reg);
  assert.ok(ix, 'read');
  assert.equal(ix.landId, LAND);
  assert.deepEqual(Object.keys(ix.sources).map(Number).sort((a, b) => a - b), [10, 20]);
  assert.equal(w.I.validBathyIndex(Object.assign(json('v2/hawaii_index.json'), { land_id: 99 }), reg), null, 'a land id past the source ids is refused');
  st.index['i:hawaii@2'] = ix;
  st.idx.set('f:hawaii@2/63_450', w.I.parseBathyIdx(bytes('v2/idx/63_450.bin'), ix.idx));
  for (const x of [2019, 2020, 2021]) assert.equal(st.present(reg, ix, 15, x, 14403, []), true, String(x));
});

test('land marker: land nodes draw nothing and read "none" (GEBCO under them never shows); water reads its own node, 1/3" water labelled as such', async () => {
  const w = world(), st = await v2store(w), layer = layerOn(w, st), c = { x: 2020, y: 14403, z: 15 }, el = addTile(w, layer, c);
  st.tiles.set('t:gebco@1/7/7/56', gebcoAll(100));
  await drawAll(w, st, layer);
  assert.ok(el._bShown);
  assert.deepEqual(el._bPieces.map((p) => [p.reg.name, p.dz, p.gebco, p.fall]), [['hawaii', 15, false, false], ['gebco', 7, true, false]]);
  const node = nodes(2020), img = el.lastImg;
  let land = 0, water10 = 0, water20 = 0, outside = 0;
  for (let py = 0; py < 256; py += 3) {
    for (let px = 0; px < 256; px += 3) {
      const n = node(py + 1, px + 1), ll = centre(c.x, c.y, 15, px, py), r = layer.readoutAt(ll.lat, ll.lng, 15), alpha = img[(py * 256 + px) * 4 + 3];
      if (n.code === 0 && n.src === LAND) { assert.equal(r.kind, 'none', 'land at ' + px + ',' + py); assert.equal(alpha, 0); land++; }
      else if (n.code === 0) { assert.equal(r.region, 'gebco', 'outside the survey: GEBCO stands in'); outside++; }
      else {
        assert.equal(r.kind, 'depth'); assert.equal(r.region, 'hawaii');
        assert.ok(Math.abs(r.depth - n.depth) < 1e-4, px + ',' + py + ': ' + r.depth + ' vs ' + n.depth);
        assert.equal(r.srcId, n.src); assert.equal(alpha, 255);
        if (n.src === 20) { water20++; assert.ok(/1\/3/.test(r.src.name), r.src.name); } else water10++;
      }
    }
  }
  assert.ok(land > 1000 && water10 > 2000 && water20 > 500, land + ' / ' + water10 + ' / ' + water20 + ' / ' + outside);
  const words = w.I.Overlay.prototype._bathyReadout.call({ opts: { getUnit: () => 'Metric' } }, { kind: 'none' });
  assert.equal(words, null, 'land: no depth words');
});

test('GEBCO\'s hold: kept out of the shallows while the region\'s index, presence file or tile is on the way, never once a v2 tile is drawn; a v1 tile keeps it', async () => {
  const w = world(), st = await v2store(w), layer = layerOn(w, st), c = { x: 2020, y: 14403, z: 15 }, el = addTile(w, layer, c);
  st.tiles.set('t:gebco@1/7/7/56', gebcoAll(10));
  const fall = () => { const p = w.I.bathyPieces(st, c, [], [], []).find((q) => q.gebco); return p && p.fall; };
  layer._draw(el, c);
  w.answer(/gebco\/v1\/index\.json$/, bytes('gebco_index.json')); await until(() => !!st.index['i:gebco@1']);
  assert.equal(fall(), true, 'the index on the way');
  w.answer(/hawaii\/v2\/index\.json$/, bytes('v2/hawaii_index.json')); await until(() => !!st.index['i:hawaii@2']);
  layer._draw(el, c);
  assert.equal(fall(), true, 'the presence file on the way');
  w.answer(/idx\/63_450\.bin$/, bytes('v2/idx/63_450.bin')); await until(() => st.idx.has('f:hawaii@2/63_450'));
  layer._draw(el, c);
  assert.ok(w.pending(/t\/15\/2020\/14403/).length, 'the tile asked for');
  assert.equal(fall(), true, 'the tile on the way');
  w.answer(/t\/15\/2020\/14403/, bytes('v2/h15_2020_14403.png')); await until(() => st.tiles.has('t:hawaii@2/15/2020/14403'));
  assert.equal(fall(), false, 'drawn from a v2 tile: the land marker decides');
  // a failure cooling down holds again; a final one does not
  st.tiles.delete('t:hawaii@2/15/2020/14403'); st.fails['t:hawaii@2/15/2020/14403'] = { at: Date.now(), final: false };
  assert.equal(fall(), true, 'cooling down');
  st.fails['t:hawaii@2/15/2020/14403'].final = true;
  assert.equal(fall(), false, 'gone for good');
  // a v1 index (no land marker): code 0 is land and "no data" alike, so GEBCO stays out of the shallows
  const reg = st.regions.find((r) => r.name === 'hawaii'), ix = st.index['i:hawaii@2'];
  delete st.fails['t:hawaii@2/15/2020/14403'];
  st.tiles.set('t:hawaii@2/15/2020/14403', { q: readPng(raw('v2/h15_2020_14403.png')), shade: null, dz: 15, tx: 2020, ty: 14403, reg: 'hawaii' });
  st.index['i:hawaii@2'] = Object.assign({}, ix, { landId: 0 });
  assert.equal(fall(), true, 'v1');
  assert.ok(reg);
});

test('the apron: the nodes past the tile come from the held neighbour; without it the own edge stands in, its key is noted and its landing redraws the tile', async () => {
  const w = world(), st = await v2store(w), layer = layerOn(w, st);
  const a = { x: 2020, y: 14403, z: 15 }, elA = addTile(w, layer, a);
  st.tiles.set('t:gebco@1/7/7/56', gebcoAll(100));
  await drawAll(w, st, layer);
  const kb = 't:hawaii@2/15/2021/14403', kl = 't:hawaii@2/15/2019/14403';
  assert.ok(elA._bNb.indexOf(kb) >= 0 && elA._bNb.indexOf(kl) >= 0, 'both side neighbours noted: ' + elA._bNb.join(' '));
  assert.ok(w.fetches.every((u) => !/2021\/14403|2019\/14403/.test(u)), 'a neighbour is never asked for by this tile');
  const na = nodes(2020), nb = nodes(2021);
  // pixel p of a z15 tile sits on node p + 1: pixels 257 and 258 (apron) read node 257 (own edge) before, 2021's 2 and 3 after
  layer._draw(elA, a);
  let own = 0;
  for (let q = 0; q < 256; q += 4) {
    for (const p of [257, 258]) {
      const want = na(q + 1, 257).depth, got = layer._D[(q + A) * W + p + A];
      if (Number.isNaN(want)) continue;
      assert.ok(Math.abs(got - want) < 1e-4, 'own edge ' + p + ',' + q); own++;
    }
  }
  assert.ok(own > 50, String(own));
  // the neighbouring map tile asks for its file; its landing marks the first tile for a redraw
  const b = { x: 2021, y: 14403, z: 15 }, elB = addTile(w, layer, b);
  layer._draw(elB, b);
  elA._ovGen = layer._gen;
  serve(w); await until(() => !Object.keys(st.inflight).length);
  assert.equal(elA._ovGen, -1, 'drawn again when the neighbour landed');
  layer._draw(elA, a);
  assert.ok(elA._bUse.indexOf(kb) >= 0, 'the neighbour is in use');
  let same = 0, differ = 0;
  for (let q = 0; q < 256; q++) {
    for (const [p, col] of [[256, 1], [257, 2], [258, 3]]) {
      const want = nb(q + 1, col).depth, got = layer._D[(q + A) * W + p + A];
      if (Number.isNaN(want)) continue;
      assert.ok(Math.abs(got - want) < 1e-4, p + ',' + q + ': ' + got + ' vs ' + want);
      same++;
      if (p > 256 && Math.abs(want - na(q + 1, 257).depth) > 0.05) differ++;
    }
  }
  assert.ok(same > 300 && differ > 20, same + ' / ' + differ);
});

test('a new tile is ready after its first full draw (never before its files are in, never twice); with nothing to wait for, at once', async () => {
  const w = world(), st = await v2store(w), layer = layerOn(w, st), c = { x: 2020, y: 14403, z: 15 };
  const calls = [];
  const el = layer.createTile(c, (err, t) => calls.push([err, t]));
  assert.ok(el._bDoneTimer, 'the fallback timer is set');
  layer._tiles['2020:14403:15'] = { el, coords: c };
  st.tiles.set('t:gebco@1/7/7/56', gebcoAll(100));
  layer._draw(el, c);
  assert.equal(calls.length, 0, 'files still on the way');
  await drawAll(w, st, layer);
  assert.equal(calls.length, 1); assert.equal(calls[0][0], null); assert.equal(calls[0][1], el);
  assert.equal(el._bDoneTimer, null, 'the timer cleared');
  layer._draw(el, c);
  assert.equal(calls.length, 1, 'never twice');
  // a tile with no data anywhere: GEBCO's file missing for good counts as answered
  const w2 = world(), st2 = await v2store(w2), l2 = layerOn(w2, st2), c2 = { x: 9000, y: 14000, z: 15 }, calls2 = [];
  const el2 = l2.createTile(c2, () => calls2.push(1)); l2._tiles['9000:14000:15'] = { el: el2, coords: c2 };
  await drawAll(w2, st2, l2);
  assert.equal(calls2.length, 1, 'ready once its last request was answered (404)');
  // without a store the tile is ready at once
  const l3 = new w.I.BathyLayer(), calls3 = [];
  const el3 = l3.createTile(c, () => calls3.push(1)); l3._draw(el3, c);
  assert.equal(calls3.length, 1);
});

test('contours where survey pixels meet GEBCO\'s: no line on the seam; each side keeps its own levels', () => {
  const layer = new I.BathyLayer(), N = W * W;
  for (let s = 0; s < N; s++) {
    const cx = s % W - A;
    layer._D[s] = cx < 130 ? 5 + 0.02 * cx : 150 + 0.4 * (cx - 130); layer._PI[s] = cx < 130 ? 0 : 1; layer._SF[s] = 0;
  }
  const rgba = new Uint8ClampedArray(256 * 256 * 4);
  layer._contours({ x: 0, y: 0, z: 15 }, rgba, [{ coarse: false }, { coarse: true }]);
  const M = 258, G = layer._G;
  for (let gy = 2; gy < 256; gy += 17) {
    // grid cell gx holds lattice pixel gx - 1: pixels 129 and 130 touch the other kind
    assert.ok(Number.isNaN(G[gy * M + 130]) && Number.isNaN(G[gy * M + 131]), 'the seam, row ' + gy);
    assert.ok(Number.isFinite(G[gy * M + 60]) && Number.isFinite(G[gy * M + 200]), 'both sides, row ' + gy);
  }
  // the coarse side reads GEBCO's sparse levels, the fine side the close-in ones
  const L = I.bathyLevels(15, 'US'), LC = I.bathyLevels(0, 'US');
  assert.ok(Math.abs(G[100 * M + 60] - I.bathyLevel(L, layer._F[(99 + A) * W + 59 + A] * 3.28084)) < 1e-5, 'fine side');
  assert.ok(Math.abs(G[100 * M + 200] - I.bathyLevel(LC, layer._F[(99 + A) * W + 199 + A] * 3.28084)) < 1e-5, 'coarse side');
});

test('the words: thousands separators, whole numbers from 100 up, "1 source", the sources line once per printed text, loading and cooling notes', () => {
  assert.equal(I.bathyNum(1234.5), '1,235'); assert.equal(I.bathyNum(150.4), '150'); assert.equal(I.bathyNum(100), '100'); assert.equal(I.bathyNum(99.94), '99.9'); assert.equal(I.bathyNum(12000, true), '12,000');
  assert.equal(I.bathyNum(5.4, true), '5'); assert.equal(I.bathyNum(-0.3), '-0.3'); assert.equal(I.bathyNum(1234567.8), '1,234,568');
  assert.equal(I.unitOf('bathy', 'Metric').top(), '5,000+ m');
  const one = I.bathyCredit(['National Oceanic and Atmospheric Administration']);
  assert.ok(/1 source\b/.test(one) && !/sources/.test(one), one);
  const text = I.Overlay.prototype._bathySourcesText;
  assert.equal(text.call({ bathyLayer: null, _bathySources: () => [] }), 'Sources here: …');
  const layer = (busy, cool) => ({ store: { busy: () => busy }, coolingInView: () => cool });
  assert.equal(text.call({ bathyLayer: layer(true, false), _bathySources: () => [] }), 'Sources here: loading…');
  assert.equal(text.call({ bathyLayer: layer(false, false), _bathySources: () => [] }), 'Sources here: none in view');
  const s = { name: 'NOAA NCEI CUDEM 1/9 arc-second topobathy', year: '2022', res_m: 3 }, g = { name: 'GEBCO 2026 Grid', year: '2026', res_m: 460 };
  const two = text.call({ bathyLayer: layer(false, true), _bathySources: () => [s, Object.assign({}, s, { region: 'prvi' }), g] });
  assert.equal(two, 'Sources here: ' + I.bathyLabel(s) + ' (3 m grid); ' + I.bathyLabel(g) + ' (460 m grid). Some depth files could not be loaded; trying again shortly.');
  const r = I.Overlay.prototype._bathyReadout.call({ opts: { getUnit: () => 'Metric' } }, { kind: 'depth', depth: 1234.4, coarse: true, src: g });
  assert.ok(/^~1,234 m /.test(r) && /regional estimate/.test(r), r);
});

test('presence bits are read low bit first (hawaii 11/124/897 is present in the v1 files); a z10 cell the index does not list asks for nothing', async () => {
  const w = world();
  const st = w.I.bathyStore('https://x'); const p = st.load(); w.answer(/regions\.json$/, json('regions.json')); await p;
  const reg = st.regions.find((r) => r.name === 'hawaii'), ix = w.I.validBathyIndex(json('hawaii_index.json'), reg);
  st.index['i:hawaii@1'] = ix;
  st.idx.set('f:hawaii@1/62_448', w.I.parseBathyIdx(bytes('idx/62_448.bin'), ix.idx));
  assert.equal(st.present(reg, ix, 11, 124, 897, []), true);
  // cell 60_448 lies inside Hawaii's box but is not in the index's list: no presence file is asked for
  const need = [];
  assert.equal(st.present(reg, ix, 12, 241, 1793, need), false);
  assert.deepEqual(need, []);
  assert.deepEqual(w.pending().filter((u) => /idx\//.test(u)), []);
});

test('the box test reads longitude too: an Atlantic tile at Hawaii\'s latitude asks nothing of Hawaii', async () => {
  const w = world(), st = await v2store(w), need = [];
  // z10 tile at about 21.5 N, 40 W
  w.I.bathyPieces(st, { x: 398, y: 449, z: 10 }, need, [], []);
  assert.ok(need.every((k) => !/hawaii/.test(k)), need.join(' '));
  assert.ok(w.pending().every((u) => !/hawaii/.test(u)));
  const need2 = [];
  w.I.bathyPieces(st, { x: 63, y: 450, z: 10 }, need2, [], []);
  assert.ok(need2.some((k) => /hawaii/.test(k)), 'over Oahu it does');
});

test('GEBCO\'s land mask: 128 and up is land (not only 255); under 128 is water', () => {
  const e = { q: gebcoAll(50).q, shade: null, dz: 7, tx: 0, ty: 0, reg: 'gebco' };
  const pc = { reg: { name: 'gebco' }, e, dz: 7, ax: I.bathyAxes(0, 1, 0), ay: I.bathyAxes(0, 1, 0), gebco: true, fade: 1, fall: false, coarse: true, landId: 0, nb: null };
  const land = new Uint8Array(256 * 256); land[10 * 256 + 10] = 128; land[10 * 256 + 11] = 200; land[10 * 256 + 12] = 127;
  const D = new Float32Array(W * W), SF = new Uint8Array(W * W), SH = new Float32Array(W * W), PI = new Int8Array(W * W);
  I.bathySample([pc], land, 0, 256, 0, 256, A, W, D, SF, SH, PI);
  const at = (x, y) => D[(y + A) * W + x + A];
  assert.ok(Number.isNaN(at(10, 10)) && Number.isNaN(at(11, 10)), '128 and 200: land');
  assert.ok(Math.abs(at(12, 10) - 50) < 1e-3, '127: water');
});

test('sources and the credit count only the tiles in view (not the ring kept around it)', async () => {
  const w = world(), st = await v2store(w), layer = layerOn(w, st), reg = st.regions.find((r) => r.name === 'hawaii');
  st.index['i:hawaii@2'] = w.I.validBathyIndex(json('v2/hawaii_index.json'), reg);
  layer._tiles['1:1:6'] = { el: { _bSrc: [{ reg, mask: 1 << 10 }] }, coords: { x: 1, y: 1, z: 6 } };
  layer._tiles['40:30:6'] = { el: { _bSrc: [{ reg, mask: 1 << 20 }] }, coords: { x: 40, y: 30, z: 6 } };
  assert.deepEqual(layer.sourcesInView().map((s) => s.year), ['2021'], 'only the tile in view');
});

test('the store: at most 4 requests at once; over the cap the oldest unused tiles go (never one in use, wanted or just landed); release empties it', async () => {
  const w = world(), st = await v2store(w), reg = st.regions.find((r) => r.name === 'hawaii');
  // 126 decoded tiles fill the cap; the oldest is in use, the second wanted
  for (let i = 0; i < 126; i++) { st.tiles.set('t:old' + i, {}); st.bytes += ENTRY; }
  assert.ok(st.bytes <= MAX && st.bytes + ENTRY > MAX);
  st.inUse = () => new Set(['t:old0']); st.wanted = () => new Set(['t:old1', 'k0', 'k1', 'k2', 'k3', 'k4', 'k5']);
  for (let i = 0; i < 6; i++) st._ask('k' + i, { kind: 'tile', reg, z: 15, x: 2019 + (i % 3), y: 14403 });
  assert.equal(Object.keys(st.inflight).length, 4, 'four at once'); assert.equal(st.queue.length, 2);
  for (let round = 0; round < 8 && (Object.keys(st.inflight).length || st.queue.length); round++) {
    for (const u of w.pending(/\/t\/15\//)) { const x = /\/(\d+)\/14403/.exec(u)[1]; w.answer(new RegExp(esc(u) + '$'), bytes('v2/h15_' + x + '_14403.png')); }
    await until(() => w.pending(/\/t\/15\//).length > 0 || !Object.keys(st.inflight).length);
  }
  for (let i = 0; i < 6; i++) assert.ok(st.tiles.has('k' + i), 'landed ' + i);
  assert.ok(st.tiles.has('t:old0') && st.tiles.has('t:old1'), 'in use / wanted kept');
  for (let i = 2; i < 8; i++) assert.ok(!st.tiles.has('t:old' + i), 'the oldest unused went: ' + i);
  assert.ok(st.tiles.has('t:old8'));
  assert.ok(st.bytes <= MAX, String(st.bytes));
  st.release();
  assert.equal(st.tiles.size, 0); assert.equal(st.bytes, 0);
});

test('the store: a tile that lands when every older tile is in use is kept (over the cap for now), not evicted as it lands', async () => {
  const w = world(), st = await v2store(w), reg = st.regions.find((r) => r.name === 'hawaii'), old = new Set();
  for (let i = 0; i < 126; i++) { st.tiles.set('t:old' + i, {}); st.bytes += ENTRY; old.add('t:old' + i); }
  st.inUse = () => old; st.wanted = () => new Set(['t:new']);
  st._ask('t:new', { kind: 'tile', reg, z: 15, x: 2019, y: 14403 });
  st.wanted = () => new Set();                               // the tile that asked is gone by the time it lands
  w.answer(/t\/15\/2019\/14403/, bytes('v2/h15_2019_14403.png'));
  await until(() => !Object.keys(st.inflight).length);
  assert.ok(st.tiles.has('t:new'), 'just landed: kept');
  assert.equal(st.tiles.size, 127); assert.ok(st.bytes > MAX, 'over the cap until a tile can go');
});

test('a failed or stalled request is remembered, retried after its cooldown and redraws the tiles; an answer after Off is dropped', async () => {
  const w = world(), st = await v2store(w), reg = st.regions.find((r) => r.name === 'hawaii'), seen = [];
  st.onChange = (k) => seen.push(k); st.retryMs = 300; st.stallMs = 150;
  st._ask('t:a', { kind: 'tile', reg, z: 15, x: 2019, y: 14403 });
  w.answer(/t\/15\/2019\/14403/, null, 503);
  await until(() => !Object.keys(st.inflight).length);
  assert.equal(st._failed('t:a'), true, 'cooling'); assert.ok(seen.indexOf('t:a') >= 0, 'redraw at the failure');
  await until(() => seen.indexOf('*') >= 0);
  assert.equal(st._failed('t:a'), false, 'the cooldown ended'); assert.equal(st.cooling('t:a'), false);
  st._ask('t:a', { kind: 'tile', reg, z: 15, x: 2019, y: 14403 });
  assert.equal(w.pending(/t\/15\/2019\/14403/).length, 1, 'asked again');
  w.answer(/t\/15\/2019\/14403/, bytes('v2/h15_2019_14403.png')); await until(() => st.tiles.has('t:a'));
  // a stall: no byte for stallMs -> a failure record, a retry, a redraw
  seen.length = 0;
  st._ask('t:b', { kind: 'tile', reg, z: 15, x: 2020, y: 14403 });
  await until(() => !Object.keys(st.inflight).length);
  assert.ok(st.fails['t:b'] && !st.fails['t:b'].final, 'remembered');
  assert.ok(seen.indexOf('t:b') >= 0, 'redraw');
  assert.ok(st.retryTimer, 'a retry armed');
  // Off while an index answer is on its way: it is not stored
  st.retryMs = 60000;
  st._ask('i:x', { kind: 'index', reg });
  w.answer(/hawaii\/v2\/index\.json$/, bytes('v2/hawaii_index.json'));
  st.abortAll();
  await settle(); await settle();
  assert.equal(st.index['i:x'], undefined);
  assert.equal(st.retryTimer, null);
});

test('leaving the entry stops every bathymetry request at once (none new after); a unit change reaches the layer', async () => {
  let unit = 'US';
  const w = world();
  const ov = w.AO.create(w.map, { base: 'https://x/gfswave/0p25/v1', panel: w.g.document.createElement('div'), tz: 'Pacific/Honolulu', getUnit: () => unit,
    fmtTime: (iso) => 'T' + iso.slice(11, 16), tzAbbr: () => 'HST', pageCycle: () => null, version: '9.9.9', bathy: true });
  ov._syncLook = function () {}; ov._dims = () => ({ w: 1200, h: 800 }); ov._coastFirst = (q) => q; ov.coast = null;
  ov.mount('bathy');
  w.answer(/regions\.json$/, json('v2/regions.json')); await settle();
  assert.equal(ov.last.state, 'ready');
  unit = 'Metric'; ov.refresh();
  assert.equal(ov.bathyLayer.unit, 'Metric');
  const st = ov.bathy, reg = st.regions.find((r) => r.name === 'hawaii');
  st.wanted = null;
  for (let i = 0; i < 6; i++) st._ask('t:z' + i, { kind: 'tile', reg, z: 15, x: 2000 + i, y: 14403 });
  const held = w.held.filter((h) => /static\/bathy\/hawaii\/v2\/t\//.test(h.url));
  assert.equal(held.length, 4);
  ov.mount('hs'); await settle();
  assert.equal(held.filter((h) => h.aborted).length, 4, 'all four aborted');
  assert.equal(st.queue.length, 0);
  const n = w.fetches.filter((u) => /static\/bathy/.test(u)).length;
  await settle(); await settle();
  assert.equal(w.fetches.filter((u) => /static\/bathy/.test(u)).length, n, 'nothing new');
});
