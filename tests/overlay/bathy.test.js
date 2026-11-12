'use strict';
// The bathymetry layer (plan section 93b) on real files the builder published (tests/fixtures/bathy: regions.json, two
// indexes, one presence file, three Hawaii tiles and one GEBCO tile), checked against an independent PNG decode here.
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

// ---- an independent reader of the builder's tiles (not the module's decoder) ----
function readPng(buf) {
  assert.deepEqual([...buf.subarray(0, 8)], [137, 80, 78, 71, 13, 10, 26, 10]);
  let off = 8, w = 0, h = 0; const idat = [];
  while (off < buf.length) {
    const len = buf.readUInt32BE(off), type = buf.toString('ascii', off + 4, off + 8), data = buf.subarray(off + 8, off + 8 + len);
    if (type === 'IHDR') { w = data.readUInt32BE(0); h = data.readUInt32BE(4); assert.equal(data[8], 8); assert.equal(data[9], 0); assert.equal(data[12], 0); }
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
  return { w, h, q: out };
}
// The builder's code -> metres (tools/bathy/bathy_tiles.py's table, written out again).
function depthOf(c) {
  if (c === 0 || c > 65401) return NaN;
  if (c <= 41001) return (c - 1) / 100 - 10;
  if (c <= 62001) return 400 + (c - 41001) / 10;
  return 2500 + (c - 62001) * 2.5;
}
function nodes(name) {
  const t = readPng(raw(name)), P = 258 * 258;
  assert.equal(t.w, 258); assert.equal(t.h, 774);
  return { depth: (r, c) => depthOf((t.q[r * 258 + c] << 8) | t.q[r * 258 + c + P]), sf: (r, c) => t.q[r * 258 + c + 2 * P] };
}
// The centre of pixel (px, py) of tile (x, y, z): its latitude and longitude.
function centre(x, y, z, px, py) {
  const n = 256 * Math.pow(2, z), X = x * 256 + px + 0.5, Y = y * 256 + py + 0.5;
  return { lat: Math.atan(Math.sinh(Math.PI * (1 - 2 * Y / n))) * 180 / Math.PI, lng: X / n * 360 - 180 };
}
function canvas(w) { const el = w.g.document.createElement('canvas'); el.width = 256; el.height = 256; return el; }
// A store over the fixtures and a layer with the given map tiles; every file the layer asks for is answered from the
// fixtures (404 for anything else), then the tiles are drawn.
async function drawn(tiles, opts) {
  opts = opts || {};
  const w = world();
  const st = w.I.bathyStore('https://x');
  const p = st.load(); w.answer(/regions\.json$/, json('regions.json')); await p;
  assert.equal(st.status, 'ok');
  const layer = new w.I.BathyLayer(); layer._map = w.map; if (opts.unit) layer.unit = opts.unit;
  layer._queueStale = function () {}; layer._cancelStale = function () {};
  const els = {};
  for (const c of tiles) { const el = canvas(w); layer._tiles[c.x + ':' + c.y + ':' + c.z] = { el, coords: c }; els[c.x + ':' + c.y + ':' + c.z] = el; }
  layer.setStore(st);
  const serve = () => {
    let any = false;
    for (const u of w.pending()) {
      const m = /\/static\/bathy\/(.*)$/.exec(u); if (!m) continue;
      const f = m[1];
      let name = null;
      if (f === 'hawaii/v1/index.json') name = 'hawaii_index.json';
      else if (f === 'gebco/v1/index.json') name = 'gebco_index.json';
      else if (f === 'hawaii/v1/idx/62_448.bin') name = 'idx/62_448.bin';
      else { const t = /^(hawaii|gebco)\/v1\/t\/(\d+)\/(\d+)\/(\d+)\.png$/.exec(f); if (t) name = { hawaii: 'h', gebco: 'g' }[t[1]] + t[2] + '_' + t[3] + '_' + t[4] + '.png'; }
      // index files are read as bytes (the store decodes them itself), like every file but regions.json
      if (name && fs.existsSync(path.join(FIX, name))) w.answer(new RegExp(u.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '$'), bytes(name));
      else w.answer(new RegExp(u.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '$'), null, opts.missing || 404);
      any = true;
    }
    return any;
  };
  for (let round = 0; round < 12; round++) {
    for (const k in layer._tiles) layer._draw(layer._tiles[k].el, layer._tiles[k].coords);
    if (!serve() && !Object.keys(st.inflight).length) break;
    await until(() => !Object.keys(st.inflight).length);
  }
  for (const k in layer._tiles) layer._draw(layer._tiles[k].el, layer._tiles[k].coords);
  return { w, st, layer, els };
}

test('the depth code: the table equals the builder\'s formula, the encoder round-trips within half a step', () => {
  for (const c of [0, 1, 2, 1000, 1001, 41000, 41001, 41002, 50000, 62001, 62002, 65000, 65401, 65402, 65535]) {
    const want = depthOf(c), got = I.BATHY_DEPTH[c];
    if (Number.isNaN(want)) assert.ok(Number.isNaN(got), String(c)); else assert.ok(Math.abs(got - want) < 1e-3, c + ': ' + got + ' vs ' + want);
  }
  for (const d of [-10, -0.4, 0, 0.004, 0.006, 18.4, 399.99, 400, 400.04, 1234.5, 2500, 2501.2, 6000, 11000]) {
    const back = I.BATHY_DEPTH[I.bathyCode(d)], step = d < 400 ? 0.01 : d < 2500 ? 0.1 : 2.5;
    assert.ok(Math.abs(back - d) <= step / 2 + 1e-4, d + ' -> ' + back);
  }
  assert.equal(I.bathyCode(NaN), 0); assert.equal(I.bathyCode(-50), 1); assert.equal(I.bathyCode(20000), 65401);
});

test('regions.json: the four regions by priority then name, GEBCO last; anything the client cannot read is refused', () => {
  const regs = I.validBathyRegions(json('regions.json'));
  assert.deepEqual(regs.map((r) => r.name), ['hawaii', 'marianas', 'prvi', 'samoa', 'gebco']);
  assert.deepEqual(regs[0], { name: 'hawaii', box: [-160.25, 18.75, -154.75, 22.25], prio: 10, v: 1, zmin: 8, zmax: 15 });
  const bad = (f) => { const d = json('regions.json'); f(d); return I.validBathyRegions(d); };
  assert.equal(bad((d) => { d.format = 'x'; }), null);
  assert.equal(bad((d) => { d.regions.hawaii.box = [-154, 18, -160, 22]; }), null, 'west of east');
  assert.equal(bad((d) => { d.regions.hawaii.v = 0; }), null);
  assert.equal(bad((d) => { d.regions.hawaii.zmin = 16; }), null, 'zmin above zmax');
  assert.equal(bad((d) => { d.regions['Bad Name'] = d.regions.hawaii; }), null);
  assert.equal(I.validBathyRegions(null), null); assert.equal(I.validBathyRegions({ format: 'bathy-regions-v1', regions: [] }), null);
});

test('the region indexes: levels, presence files, sources; another region\'s or version\'s index is refused; the presence file\'s length', () => {
  const regs = I.validBathyRegions(json('regions.json')), hawaii = regs[0], gebco = regs[4];
  const ix = I.validBathyIndex(json('hawaii_index.json'), hawaii);
  assert.ok(ix && ix.idx && ix.idx.z0 === 11 && ix.idx.z1 === 15 && ix.t10['62_448'] && !ix.t10['0_0']);
  assert.equal(ix.idx.bytes, raw('idx/62_448.bin').length, 'the presence file is z11..z15 bitsets');
  assert.ok(ix.levels[10] && ix.levels[8] && !ix.levels[11]);
  assert.equal(ix.sources[10].name, 'NOAA NCEI CUDEM 1/9 arc-second topobathy');
  assert.equal(I.validBathyIndex(json('hawaii_index.json'), Object.assign({}, hawaii, { v: 2 })), null, 'version');
  assert.equal(I.validBathyIndex(json('hawaii_index.json'), gebco), null, 'region');
  const gx = I.validBathyIndex(json('gebco_index.json'), gebco);
  assert.ok(gx && gx.idx === null && gx.levels[0] && gx.levels[7]);
  const bits = I.parseBathyIdx(bytes('idx/62_448.bin'), ix.idx);
  assert.equal(bits.length, 5);
  assert.equal(I.parseBathyIdx(new ArrayBuffer(ix.idx.bytes - 1), ix.idx), null);
});

test('a z15 map tile is drawn 1:1 from its z15 file: every pixel\'s readout = the node under it, decoded independently; drawn = has data', async () => {
  const c = { x: 1997, y: 14363, z: 15 }, { w, layer, els } = await drawn([c]), el = els['1997:14363:15'];
  assert.ok(el._bShown, 'drawn');
  assert.deepEqual(el._bPieces.map((p) => [p.reg.name, p.dz]), [['hawaii', 15]]);
  const n = nodes('h15_1997_14363.png'), img = el.getContext('2d') && el.lastImg;
  assert.ok(img, 'putImageData');
  let checked = 0, wet = 0;
  for (let py = 0; py < 256; py += 5) {
    for (let px = 0; px < 256; px += 7) {
      const ll = centre(c.x, c.y, c.z, px, py), r = layer.readoutAt(ll.lat, ll.lng, 15), want = n.depth(py + 1, px + 1), alpha = img[(py * 256 + px) * 4 + 3];
      if (Number.isNaN(want)) { assert.equal(r.kind, 'none', px + ',' + py); assert.equal(alpha, 0); }
      else {
        assert.equal(r.kind, 'depth'); assert.ok(Math.abs(r.depth - want) < 1e-4, px + ',' + py + ': ' + r.depth + ' vs ' + want);
        const sfb = n.sf(py + 1, px + 1);
        assert.equal(r.srcId, sfb & 0x1F); assert.equal(r.region, 'hawaii');
        assert.equal(r.filled, (sfb & 0x20) !== 0); assert.equal(r.blended, (sfb & 0x40) !== 0); assert.equal(r.coarse, (sfb & 0x80) !== 0);
        assert.ok(r.src && r.src.name, 'the source entry');
        assert.equal(alpha, 255); wet++;
      }
      checked++;
    }
  }
  assert.ok(checked > 1500 && wet > 300, checked + ' / ' + wet);
  assert.equal(w.pending().length, 0);
});

test('overzoom: a z16 map tile reads the z15 file bilinearly at its pixel centres (no tile is fetched at z16 or above)', async () => {
  const c = { x: 3994, y: 28727, z: 16 }, { w, layer, els } = await drawn([c]), el = els['3994:28727:16'];
  assert.ok(el._bShown);
  assert.deepEqual(el._bPieces.map((p) => p.dz), [15]);
  assert.ok(w.fetches.every((u) => !/\/t\/1[6-9]\//.test(u)), 'nothing above the region\'s zmax');
  const n = nodes('h15_1997_14363.png');
  let checked = 0;
  for (let py = 3; py < 256; py += 17) {
    for (let px = 5; px < 256; px += 19) {
      // pixel (px, py) of the z16 tile: its centre in z15 tile pixels, then in nodes (+1 apron, -0.5 node centre)
      const u = (c.x * 256 + px + 0.5) / 2 - 1997 * 256 + 0.5, v = (c.y * 256 + py + 0.5) / 2 - 14363 * 256 + 0.5;
      const c0 = Math.floor(u), r0 = Math.floor(v), fx = u - c0, fy = v - r0;
      let acc = 0, wt = 0;
      for (const [dr, dc, f] of [[0, 0, (1 - fx) * (1 - fy)], [0, 1, fx * (1 - fy)], [1, 0, (1 - fx) * fy], [1, 1, fx * fy]]) {
        const d = n.depth(r0 + dr, c0 + dc); if (!Number.isNaN(d) && f > 0) { acc += f * d; wt += f; }
      }
      const ll = centre(c.x, c.y, c.z, px, py), r = layer.readoutAt(ll.lat, ll.lng, 16);
      if (wt < 0.25) { assert.equal(r.kind, 'none'); continue; }
      assert.equal(r.kind, 'depth'); assert.ok(Math.abs(r.depth - acc / wt) < 1e-3, px + ',' + py + ': ' + r.depth + ' vs ' + acc / wt);
      checked++;
    }
  }
  assert.ok(checked > 50, String(checked));
});

test('a lone tile\'s first draw asks for its files at once (the store keeps the requests the drawing tile wants)', async () => {
  const w = world();
  const st = w.I.bathyStore('https://x'); const p = st.load(); w.answer(/regions\.json$/, json('regions.json')); await p;
  const layer = new w.I.BathyLayer(); layer._map = w.map; layer._queueStale = function () {}; layer._cancelStale = function () {};
  const el = canvas(w), c = { x: 1997, y: 14363, z: 15 };
  layer._tiles['1997:14363:15'] = { el, coords: c };
  layer.setStore(st);
  layer._draw(el, c);
  // the region's index and GEBCO's (GEBCO fills under every zoom where no finer grid reaches)
  assert.deepEqual(w.pending(), ['https://x/static/bathy/hawaii/v1/index.json', 'https://x/static/bathy/gebco/v1/index.json'], 'asked by the first draw');
  w.answer(/hawaii\/v1\/index\.json$/, bytes('hawaii_index.json')); w.answer(/gebco\/v1\/index\.json$/, bytes('gebco_index.json'));
  await until(() => !Object.keys(st.inflight).length);
  layer._draw(el, c);
  assert.deepEqual(w.pending(), ['https://x/static/bathy/hawaii/v1/idx/62_448.bin', 'https://x/static/bathy/gebco/v1/t/7/7/56.png']);
  // a tile gone from the map: its queued request is dropped, never started
  delete layer._tiles['1997:14363:15'];
  st._ask('t:hawaii@1/15/5/5', { kind: 'tile', reg: st.regions[0], z: 15, x: 5, y: 5 });
  assert.ok(!w.pending().some((u) => /\/t\/15\/5\/5\.png$/.test(u)), 'nobody wants it');
});

test('a tile whose finer presence file is on the way fetches no coarser tile meanwhile; held coarser tiles stand in', async () => {
  const w = world();
  const st = w.I.bathyStore('https://x'); const p = st.load(); w.answer(/regions\.json$/, json('regions.json')); await p;
  st.index['i:hawaii@1'] = w.I.validBathyIndex(json('hawaii_index.json'), st.regions[0]);
  const fake = (z, x, y) => ({ q: new Uint8Array(258 * 774), shade: null, dz: z, tx: x, ty: y, reg: 'hawaii' });
  const need = [], use = [];
  const pieces = w.I.bathyPieces(st, { x: 1997, y: 14363, z: 15 }, need, use);
  assert.equal(pieces.length, 0);
  assert.ok(need.indexOf('f:hawaii@1/62_448') >= 0);
  assert.deepEqual(w.pending().filter((u) => /\/t\//.test(u)), [], 'no tile asked while the presence file loads: ' + w.pending().join(' '));
  // still pending: a held z9 tile (coarser than the z10 the index lists) stands in, nothing is asked
  st.tiles.set('t:hawaii@1/9/31/224', fake(9, 31, 224));
  const p1 = w.I.bathyPieces(st, { x: 1997, y: 14363, z: 15 }, [], []);
  assert.deepEqual(p1.map((q) => q.dz), [9], 'the held z9 tile');
  assert.deepEqual(w.pending().filter((u) => /\/t\//.test(u)), []);
  // the presence file in: the z15 tile is asked for and a held z12 tile stands in meanwhile
  st.idx.set('f:hawaii@1/62_448', w.I.parseBathyIdx(bytes('idx/62_448.bin'), st.index['i:hawaii@1'].idx));
  st.tiles.set('t:hawaii@1/12/249/1795', fake(12, 249, 1795));
  const need2 = [], p2 = w.I.bathyPieces(st, { x: 1997, y: 14363, z: 15 }, need2, []);
  assert.deepEqual(p2.map((q) => q.dz), [12], 'the held z12 tile');
  assert.ok(need2.indexOf('t:hawaii@1/15/1997/14363') >= 0);
  assert.ok(w.pending().some((u) => /\/t\/15\/1997\/14363\.png$/.test(u)));
});

test('the readout at a tile edge reads the tile under the pointer; a pixel with no file reads "none"; Metric contours change the tile', async () => {
  const a = { x: 1997, y: 14363, z: 15 }, b = { x: 1998, y: 14363, z: 15 };
  const { layer, els } = await drawn([a, b]);
  const na = nodes('h15_1997_14363.png'), nb = nodes('h15_1998_14363.png');
  for (const py of [0, 64, 128, 255]) {
    const la = centre(a.x, a.y, 15, 255, py), lb = centre(b.x, b.y, 15, 0, py);
    const ra = layer.readoutAt(la.lat, la.lng, 15), rb = layer.readoutAt(lb.lat, lb.lng, 15);
    const wa = na.depth(py + 1, 256), wb = nb.depth(py + 1, 1);
    if (Number.isNaN(wa)) assert.equal(ra.kind, 'none'); else assert.ok(Math.abs(ra.depth - wa) < 1e-4);
    if (Number.isNaN(wb)) assert.ok(rb === null || rb.kind === 'none'); else assert.ok(Math.abs(rb.depth - wb) < 1e-4);
  }
  // the apron agrees with the neighbour's own pixels (the builder's one-node apron): a's last apron column = b's first pixel
  let same = 0, both = 0;
  for (let r = 1; r < 257; r++) { const x = na.depth(r, 257), y = nb.depth(r, 1); if (!Number.isNaN(x) && !Number.isNaN(y)) { both++; if (Math.abs(x - y) < 1e-6) same++; } }
  assert.equal(same, both, 'apron = neighbour');
  const before = Array.from(els['1997:14363:15'].lastImg);
  layer.setUnit('Metric'); layer._draw(els['1997:14363:15'], a);
  const after = els['1997:14363:15'].lastImg;
  let diff = 0; for (let i = 0; i < before.length; i += 4) if (before[i] !== after[i] || before[i + 3] !== after[i + 3]) diff++;
  assert.ok(diff > 0, 'other contour levels in metres');
  for (let i = 3; i < before.length; i += 4) assert.equal(before[i], after[i], 'contours never change the alpha');
});

test('GEBCO at z5: drawn coarse (never the shallow colours), its readout flagged coarse; a missing file is final (no retry)', async () => {
  const c = { x: 1, y: 14, z: 5 }, { w, st, layer, els } = await drawn([c]), el = els['1:14:5'];
  assert.ok(el._bShown);
  assert.deepEqual(el._bPieces.map((p) => [p.reg.name, p.dz, p.gebco]), [['gebco', 5, true]]);
  const n = nodes('g5_1_14.png');
  let wet = 0;
  for (let py = 0; py < 256; py += 9) for (let px = 0; px < 256; px += 9) {
    const ll = centre(c.x, c.y, 5, px, py), r = layer.readoutAt(ll.lat, ll.lng, 5), want = n.depth(py + 1, px + 1);
    if (Number.isNaN(want)) continue;
    assert.equal(r.kind, 'depth'); assert.equal(r.coarse, true); assert.ok(Math.abs(r.depth - want) < 1e-3); wet++;
  }
  assert.ok(wet > 100, String(wet));
  assert.equal(I.bathyPos(5, true), I.bathyPos(30, false), 'coarse never shallow');
  assert.equal(I.bathyPos(-1, false), 0); assert.ok(I.bathyPos(30, false) > I.bathyPos(10, false));
  // a tile the index lists but the bucket does not have: 404 = final (the layer detached, so no want list filters the asks)
  layer.setStore(null);
  assert.equal(st.wanted, null);
  const k = 't:hawaii@1/15/1/1';
  st._ask(k, { kind: 'tile', reg: st.regions[0], z: 15, x: 1, y: 1 });
  w.answer(/\/t\/15\/1\/1\.png$/, null, 404); await settle();
  assert.equal(st._failed(k), 'final');
  st._ask('t:hawaii@1/15/2/2', { kind: 'tile', reg: st.regions[0], z: 15, x: 2, y: 2 });
  w.answer(/\/t\/15\/2\/2\.png$/, null, 503); await settle();
  assert.equal(st._failed('t:hawaii@1/15/2/2'), true, 'a 503 cools down');
  assert.ok(st.retryTimer, 'and comes back');
  st.abortAll(); assert.equal(st.retryTimer, null);
});

// A data tile entry whose every node holds depthFn(r, c) metres (NaN = none) with flag byte flagFn(r, c).
function entry(depthFn, flagFn) {
  const P = 258 * 258, q = new Uint8Array(258 * 774);
  for (let r = 0; r < 258; r++) for (let c = 0; c < 258; c++) {
    const d = depthFn(r, c), code = Number.isNaN(d) ? 0 : I.bathyCode(d), i = r * 258 + c;
    q[i] = code >> 8; q[i + P] = code & 255; q[i + 2 * P] = flagFn ? flagFn(r, c) : 0;
  }
  return { q, shade: null, dz: 15, tx: 0, ty: 0, reg: 'x' };
}
const piece = (e, extra) => Object.assign({ reg: { name: 'x', v: 1 }, e, dz: 15, ax: I.bathyAxes(0, 1, 0), ay: I.bathyAxes(0, 1, 0), gebco: false, fade: 1, fall: false, coarse: false }, extra || {});
function sample(pieces, land) {
  const N = 256 * 256, D = new Float32Array(N), SF = new Uint8Array(N), SH = new Float32Array(N), PI = new Int8Array(N);
  I.bathySample(pieces, land, 0, 256, 0, 256, 0, 256, D, SF, SH, PI);
  return { D, SF, PI };
}

test('the sampler: a region first; GEBCO beside a region never in the shallows (deep water stands in); GEBCO masked off the land', () => {
  const regional = piece(entry((r, c) => (c < 129 ? 8 : NaN)));
  const gebcoShallow = piece(entry(() => 10, () => 0x80), { gebco: true, fall: true, coarse: true });
  const gebcoDeep = piece(entry(() => 100, () => 0x80), { gebco: true, fall: true, coarse: true });
  let s = sample([regional, gebcoShallow], null);
  assert.ok(Math.abs(s.D[10 * 256 + 10] - 8) < 1e-3 && s.PI[10 * 256 + 10] === 0, 'the region where it has data');
  assert.ok(Number.isNaN(s.D[10 * 256 + 200]) && s.PI[10 * 256 + 200] === -1, 'GEBCO 10 m beside a region: nothing');
  s = sample([regional, gebcoDeep], null);
  assert.ok(Math.abs(s.D[10 * 256 + 200] - 100) < 1e-3 && s.PI[10 * 256 + 200] === 1, 'GEBCO 100 m stands in');
  assert.equal(s.SF[10 * 256 + 200] & 0x80, 0x80, 'its flag byte');
  // no region near: GEBCO's shallows are drawn
  s = sample([piece(entry(() => 10), { gebco: true, fall: false, coarse: true })], null);
  assert.ok(Math.abs(s.D[10 * 256 + 200] - 10) < 1e-3);
  // the land mask: GEBCO pixels over land (>= 128) are left out, the region's never
  const land = new Uint8Array(256 * 256); land[5 * 256 + 5] = 255; land[5 * 256 + 6] = 127;
  const g = piece(entry(() => 50), { gebco: true, coarse: true });
  s = sample([g], land);
  assert.ok(Number.isNaN(s.D[5 * 256 + 5]) && Math.abs(s.D[5 * 256 + 6] - 50) < 1e-3);
  s = sample([g], I.LAND_ALL || new Uint8Array(0));
  assert.ok(Number.isNaN(s.D[100 * 256 + 100]), 'a tile all land: no GEBCO');
  s = sample([piece(entry(() => 50))], land);
  assert.ok(Math.abs(s.D[5 * 256 + 5] - 50) < 1e-3, 'a region is never masked');
  // a quarter of the weight present is enough, less is none (a node row at the edge of the data)
  s = sample([piece(entry((r, c) => (c <= 1 ? 4 : NaN)))], null);
  assert.ok(Math.abs(s.D[10 * 256 + 0] - 4) < 1e-3, 'pixel 0 sits on node 1');
  assert.ok(Number.isNaN(s.D[10 * 256 + 2]), 'pixel 2 has no present node');
});

test('GEBCO pieces: overzoomed at every zoom at full strength (it fills wherever no finer grid reaches), kept out of the shallows while a region\'s index loads', async () => {
  const w = world();
  const st = w.I.bathyStore('https://x'); const p = st.load(); w.answer(/regions\.json$/, json('regions.json')); await p;
  const gr = st.regions.find((r) => r.name === 'gebco');
  st.index['i:gebco@1'] = w.I.validBathyIndex(json('gebco_index.json'), gr);
  // a GEBCO z7 tile the index lists, held: the open ocean north-east of Hawaii (outside every region's box)
  let px = -1, py = -1;
  for (let y = 48; y < 56 && px < 0; y++) for (let x = 10; x < 20; x++) if (st.present(gr, st.index['i:gebco@1'], 7, x, y)) { px = x; py = y; break; }
  assert.ok(px >= 0, 'a present z7 GEBCO tile');
  st.tiles.set('t:gebco@1/7/' + px + '/' + py, Object.assign(entry(() => 4000), { dz: 7, tx: px, ty: py, reg: 'gebco' }));
  const at = (z) => w.I.bathyPieces(st, { x: px << (z - 7), y: py << (z - 7), z }, [], []);
  for (const z of [8, 10, 11, 12, 15, 17]) assert.deepEqual(at(z).map((q) => [q.dz, q.fade, q.gebco, q.fall]), [[7, 1, true, false]], 'z' + z);
  // inside Hawaii's box while its index is on the way: GEBCO is drawn only deep there
  // a z8 tile wholly inside Hawaii's box (18.75-22.25 N, 160.25-154.75 W) whose GEBCO z7 parent is listed
  const tileLat = (y, z) => Math.atan(Math.sinh(Math.PI * (1 - 2 * y / Math.pow(2, z)))) * 180 / Math.PI;
  const tileLon = (x, z) => x / Math.pow(2, z) * 360 - 180;
  let h8 = null;
  for (let y = 108; y <= 116 && !h8; y++) for (let x = 13; x <= 18; x++) {
    const inBox = tileLon(x, 8) >= -160.25 && tileLon(x + 1, 8) <= -154.75 && tileLat(y, 8) <= 22.25 && tileLat(y + 1, 8) >= 18.75;
    if (inBox && st.present(gr, st.index['i:gebco@1'], 7, x >> 1, y >> 1)) { h8 = { x, y, z: 8 }; break; }
  }
  assert.ok(h8, 'a z8 tile inside Hawaii\'s box with a listed GEBCO parent');
  st.tiles.set('t:gebco@1/7/' + (h8.x >> 1) + '/' + (h8.y >> 1), Object.assign(entry(() => 10), { dz: 7, tx: h8.x >> 1, ty: h8.y >> 1, reg: 'gebco' }));
  const pc = w.I.bathyPieces(st, h8, [], []);
  assert.deepEqual(pc.map((q) => [q.gebco, q.fall]), [[true, true]], 'fall while the regional index loads');
  assert.ok(w.pending().some((u) => /hawaii\/v1\/index\.json$/.test(u)));
  // a region whose own zooms start past 10 (only its presence files tell where it has data), over the open-ocean tile:
  // while the presence file is on the way GEBCO keeps out of the shallows too; once it says "nothing here", GEBCO is free
  const c12 = { x: px << 5, y: py << 5, z: 12 }, lon0 = tileLon(c12.x, 12), lat0 = tileLat(c12.y + 1, 12);
  const lid = { name: 'lidar', box: [lon0 - 0.5, lat0 - 0.5, lon0 + 0.5, lat0 + 0.5], prio: 20, v: 1, zmin: 11, zmax: 15 };
  const cell = (c12.x >> 2) + '_' + (c12.y >> 2);
  st.regions.unshift(lid); st.index['i:lidar@1'] = { levels: [], idx: 'idx', t10: { [cell]: 1 } };
  assert.deepEqual(w.I.bathyPieces(st, c12, [], []).map((q) => [q.gebco, q.fall]), [[true, true]], 'fall while its presence file loads');
  st.idx.set('f:lidar@1/' + cell, [new Uint8Array(1), new Uint8Array(1), new Uint8Array(1), new Uint8Array(1), new Uint8Array(1)]);
  assert.deepEqual(w.I.bathyPieces(st, c12, [], []).map((q) => [q.gebco, q.fall]), [[true, false]], 'nothing there: GEBCO free');
  st.regions.shift();
});

test('contour levels: 0 first, increasing, coarser bands thinner; level coordinates monotone; nothing at 0 (the shoreline)', () => {
  for (const unit of ['US', 'Metric']) {
    let prev = Infinity;
    for (const z of [17, 15, 13, 11, 8, 3]) {
      const L = I.bathyLevels(z, unit);
      assert.equal(L[0], 0);
      for (let i = 1; i < L.length; i++) assert.ok(L[i] > L[i - 1], unit + ' z' + z);
      const first = L[1]; assert.ok(first <= prev || first >= prev, 'defined'); prev = first;
    }
    const L = I.bathyLevels(15, unit);
    let last = -Infinity;
    for (let v = -3; v < 2000; v += 0.37) { const k = I.bathyLevel(L, v); assert.ok(k >= last - 1e-9, v); last = k; }
    for (let i = 1; i < L.length; i++) assert.ok(Math.abs(I.bathyLevel(L, L[i]) - i) < 1e-9);
    assert.ok(I.bathyLevel(L, -100) > -0.5 && I.bathyLevel(L, 0) === 0);
  }
  assert.ok(I.bathyLevels(15, 'Metric').length > I.bathyLevels(11, 'Metric').length);
});

// A coast-v1 body (the format of tools/coast/build_coast.py, as tests/overlay/coast.test.js writes it).
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
const DEG = (d) => Math.round(d * 10000);
const SQ = (x0, y0, x1, y1) => [DEG(x0), DEG(y0), DEG(x1), DEG(y0), DEG(x1), DEG(y1), DEG(x0), DEG(y1)];

test('GEBCO is masked by the detailed coastline close in: tier 0 while a chunk loads, the chunk once it lands (only those tiles drawn again); the store\'s hooks borrowed and handed back', async () => {
  const c = { x: 1, y: 14, z: 5 }, { w, layer, els } = await drawn([c]), el = els['1:14:5'];
  // a second tile on the map that has never drawn GEBCO (no mask): it asks for no chunk and is never redrawn
  const other = canvas(w), oc = { x: 0, y: 14, z: 5 }; layer._tiles['0:14:5'] = { el: other, coords: oc };
  const t0 = I.decodeCoast(encodeCoast([[SQ(-164, 15, -162, 17)]], 30)), chunk = I.decodeCoast(encodeCoast([[SQ(-166, 13, -165, 14)]], 5));
  let got = { sets: [t0], complete: false }, asked = 0;
  const waveOn = () => 'wave', waveNeed = () => ({});
  const store = { status: 'ok', rev: 0, onChange: waveOn, onNeeded: waveNeed, index: { tier1: { cell: 5 } }, tier1Zoom: () => true,
    setsFor: () => { asked++; return got; } };
  let queued = 0; layer._queueStale = () => { queued++; };
  layer.setCoast(store);
  assert.notEqual(store.onChange, waveOn, 'borrowed'); assert.notEqual(store.onNeeded, waveNeed);
  layer._draw(el, c);
  const at = (lat, lng) => layer.readoutAt(lat, lng, 5).kind;
  assert.equal(el._bLandFinal, false); assert.equal(at(16, -163), 'none', 'tier 0 land'); assert.equal(at(13.5, -165.5), 'depth');
  assert.equal(at(19, -160), 'depth', 'open water');
  // the cells of the tiles that mask GEBCO (the other tile drew none)
  assert.deepEqual(Object.keys(store.onNeeded()).sort(), I.coastCellsForTile(c, 5).sort());
  // the chunk lands: the tile is queued for a redraw, then drawn from the chunk
  got = { sets: [chunk], complete: true }; store.rev = 1; queued = 0;
  store.onChange();
  assert.equal(el._ovGen, -1, 'queued'); assert.equal(queued, 1); assert.notEqual(other._ovGen, -1);
  layer._draw(el, c);
  assert.equal(el._bLandFinal, true); assert.equal(at(16, -163), 'depth'); assert.equal(at(13.5, -165.5), 'none', 'the chunk\'s land');
  // final: another landing asks the store nothing for it and draws nothing
  const before = asked, fin = el._bLand; store.rev = 2; queued = 0; store.onChange();
  assert.equal(asked, before); assert.equal(queued, 0); assert.notEqual(el._ovGen, -1);
  // a final mask is never asked for again, also when the tile itself asks after the store moved on
  assert.ok(fin && fin.length, 'a real mask'); assert.ok(layer._landFor(el, c) === fin); assert.equal(asked, before, 'not re-asked');
  // a stand-in that is still a stand-in after the store moved on is kept (no re-rasterising, no redraw):
  // a world copy of the same tile, so the stand-in holds real land (an array, not null)
  const el2 = canvas(w), c2 = { x: 33, y: 14, z: 5 }; layer._tiles['33:14:5'] = { el: el2, coords: c2 };
  got = { sets: [t0], complete: false };
  const m = layer._landFor(el2, c2); assert.ok(m && m.length, 'a real stand-in mask');
  const asked2 = asked; store.rev = 3; queued = 0; store.onChange();
  assert.equal(asked, asked2 + 1, 'asked once'); assert.ok(layer._landFor(el2, c2) === m, 'the same mask kept'); assert.equal(queued, 0);
  assert.equal(asked, asked2 + 1, 'the rev it saw remembered');
  // a store that stops being ready while held masks nothing (and is not asked)
  store.status = 'idle'; const el3 = canvas(w);
  assert.equal(layer._landFor(el3, c), null); assert.equal(asked, asked2 + 1); store.status = 'ok';
  // let go: the wave layer's hooks are back
  layer.setCoast(null);
  assert.equal(store.onChange, waveOn); assert.equal(store.onNeeded, waveNeed);
  // a store that is not loaded masks nothing
  layer.setCoast({ status: 'loading' }); layer._draw(el, c);
  assert.equal(el._bLandUsed, null); assert.equal(at(13.5, -165.5), 'depth');
});

test('credit, labels and the slider\'s value', () => {
  const sets = [['NOAA'], ['GEBCO'], ['NOAA', 'GEBCO'], ['NOAA', 'USACE'], ['NOAA', 'USACE', 'GEBCO'], ['NOAA', 'USACE', 'NGS', 'GEBCO'], []];
  for (const s of sets) {
    const html = I.bathyCredit(s), text = html.replace(/<[^>]+>/g, '');
    assert.ok(text.length <= 29, text); assert.ok(/href="\/notice\/bathy"/.test(html)); assert.ok(!/[<>]/.test(text));
  }
  assert.equal(I.bathyCredit(['NOAA', 'GEBCO']).replace(/<[^>]+>/g, ''), 'Bathymetry: NOAA · GEBCO');
  assert.equal(I.bathyCredit(['NOAA', 'USACE', 'NGS', 'GEBCO']).replace(/<[^>]+>/g, ''), 'Bathymetry: 4 sources');
  const hs = json('hawaii_index.json').sources['10'], gs = json('gebco_index.json').sources['31'];
  assert.equal(I.bathyLabel(hs), 'NOAA CUDEM 2021'); assert.equal(I.bathyShort(hs), 'NOAA');
  assert.equal(I.bathyLabel(gs), 'GEBCO 2026 Grid'); assert.equal(I.bathyShort(gs), 'GEBCO');
  for (const [v, want] of [[85, 85], ['50', 50], [19, 85], [101, 85], [55.5, 85], ['x', 85], [null, 85], [20, 20], [100, 100]]) assert.equal(I.bathyOpacityOf(v), want, String(v));
});

// ---- the controller ----
function ctl(w, opts) {
  const ov = w.AO.create(w.map, Object.assign({ base: 'https://x/gfswave/0p25/v1', panel: w.g.document.createElement('div'), tz: 'Pacific/Honolulu', getUnit: () => 'US',
    fmtTime: (iso) => 'T' + iso.slice(11, 16), tzAbbr: () => 'HST', pageCycle: () => null, version: '9.9.9', bathy: true }, opts || {}));
  ov._syncLook = function () {}; ov._dims = () => ({ w: 1200, h: 800 }); ov._coastFirst = (p) => p; ov.coast = null;
  return ov;
}
const findCls = (el, cls) => { if ((el.className || '').split(' ').indexOf(cls) >= 0) return el; for (const c of el.children || []) { const f = findCls(c, cls); if (f) return f; } return null; };
const allText = (el) => (el.textContent || '') + ' ' + (el.children || []).map(allText).join(' ');

test('the entry: only the bathymetry\'s files, its layer and credit, the panel with scale, sources, note and slider; no particles, no player', async () => {
  const w = world(), ov = ctl(w);
  assert.equal(I.hasObs('bathy'), true);
  assert.deepEqual(I.OBS_LOAD.bathy, ['bathy', '_loadBathy']);
  ov.mount('bathy');
  assert.equal(ov.mode, 'obs'); assert.equal(ov.field, 'bathy');
  assert.deepEqual(w.pending(), ['https://x/static/bathy/regions.json'], 'regions.json, nothing else');
  w.answer(/regions\.json$/, json('regions.json')); await settle();
  assert.equal(ov.last.state, 'ready');
  assert.ok(ov.bathyLayer && w.layers.indexOf(ov.bathyLayer) >= 0); assert.equal(ov.bathyLayer.opacity, 0.85);
  assert.equal(ov.flow, null, 'no particles'); assert.ok(!ov.radarPlayer);
  assert.ok(w.attrs.length === 1 && /Bathymetry/.test(w.attrs[0]) && /notice\/bathy/.test(w.attrs[0]));
  const host = ov.ui.host;
  assert.ok(findCls(host, 'ov-bathy-sources')); assert.ok(findCls(host, 'ov-bathy-note')); assert.ok(findCls(host, 'ov-bathy-opacity-range'));
  assert.ok(!findCls(host, 'ov-play') && !findCls(host, 'ov-timeline') && !findCls(host, 'ov-model-sel'));
  assert.ok(/Bathymetry: water depth/.test(allText(host)) && /not for navigation/.test(allText(host)));
  // the slider: 15 % transparent by default; 40 % -> layer 0.6, kept on the device
  const range = findCls(host, 'ov-bathy-opacity-range');
  assert.equal(range.value, '15');
  range.value = '40'; range._ls.input();
  assert.equal(ov.bathyLayer.opacity, 0.6); assert.equal(w.ls['allshore.bathyOpacity.v1'], '60');
  // Off: the layer goes, nothing more is asked
  ov.unmount(); await settle();
  assert.equal(ov.bathyLayer, null); assert.equal(w.layers.filter((l) => l instanceof w.I.BathyLayer).length, 0);
  const n = w.fetches.length; await settle(); assert.equal(w.fetches.length, n);
});

test('back to a model field leaves the bathymetry: its layer and requests go; a failed regions.json shows the error and Retry asks again at once', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('bathy');
  w.answer(/regions\.json$/, null, 503); await settle();
  assert.equal(ov.last.state, 'error');
  ov.mount('bathy');
  assert.ok(w.pending().some((u) => /regions\.json$/.test(u)), 'Retry within the cooldown asks again');
  w.answer(/regions\.json$/, json('regions.json')); await settle();
  assert.equal(ov.last.state, 'ready');
  ov.mount('hs');
  assert.equal(ov.bathyLayer, null);
  assert.ok(w.pending().every((u) => !/static\/bathy/.test(u)), 'no bathymetry request under wave height');
});

test('the coastline\'s hooks: the bathymetry layer borrows them once the coastline is in, and hands them back when it leaves', async () => {
  const w = world(), ov = ctl(w);
  const waveOn = () => 0, waveNeed = () => ({});
  const store = { status: 'ok', rev: 0, onChange: waveOn, onNeeded: waveNeed, index: { tier1: { cell: 5 } }, tier1Zoom: () => false,
    setsFor: () => ({ sets: [], complete: true }) };
  ov.coast = { load: () => Promise.resolve(store), abortAll() {} };
  ov.mount('bathy');
  w.answer(/regions\.json$/, json('regions.json')); await settle();
  assert.equal(ov.last.state, 'ready');
  assert.equal(ov.bathyLayer._coast, store); assert.notEqual(store.onChange, waveOn); assert.notEqual(store.onNeeded, waveNeed);
  ov.unmount(); await settle();
  assert.equal(store.onChange, waveOn); assert.equal(store.onNeeded, waveNeed);
});

test('the readout\'s words: depth in feet / metres, the source and year, "estimated" on filled pixels, GEBCO as a regional estimate, dry ground', () => {
  const w = world(), ov = ctl(w);
  const cudem = json('hawaii_index.json').sources['10'], gebco = json('gebco_index.json').sources['31'];
  assert.equal(ov._bathyReadout({ kind: 'depth', depth: 5.6, src: cudem, filled: false, coarse: false }), '18.4 ft · NOAA CUDEM 2021 · approximate');
  assert.equal(ov._bathyReadout({ kind: 'depth', depth: 5.6, src: cudem, filled: true, coarse: false }), '18.4 ft · NOAA CUDEM 2021 · estimated');
  assert.equal(ov._bathyReadout({ kind: 'depth', depth: 42.7, src: gebco, filled: false, coarse: true }), '~140 ft (GEBCO, regional estimate)');
  assert.equal(ov._bathyReadout({ kind: 'depth', depth: -0.3, src: cudem, filled: false, coarse: false }), 'dries at mean sea level · NOAA CUDEM 2021');
  assert.equal(ov._bathyReadout({ kind: 'none' }), null); assert.equal(ov._bathyReadout(null), null);
  const m = ctl(w, { getUnit: () => 'Metric' });
  assert.equal(m._bathyReadout({ kind: 'depth', depth: 5.63, src: cudem, filled: false, coarse: false }), '5.6 m · NOAA CUDEM 2021 · approximate');
  assert.equal(m._bathyReadout({ kind: 'depth', depth: 812.4, src: gebco, filled: false, coarse: true }), '~812 m (GEBCO, regional estimate)');
});

// ---- pins found by the mutation pass ----

// Overzoomed by 2 (s = 2): pixel 0 sits between nodes 0 and 1 at 0.75 of the way, pixel 1 at 0.25 past node 1.
const piece2 = (e, extra) => Object.assign(piece(e, extra), { ax: I.bathyAxes(0, 2, 0), ay: I.bathyAxes(0, 2, 0) });
function sampleAt(pc, p, q) {
  const D = new Float32Array(1), SF = new Uint8Array(1), SH = new Float32Array(1), PI = new Int8Array(1);
  I.bathySample([pc], null, p, p + 1, q, q + 1, -p, 0, D, SF, SH, PI);
  return { d: D[0], sf: SF[0], sh: SH[0], pi: PI[0] };
}

test('the sampler: under a quarter of the weight present is no depth; the flag of the heaviest node; the shade interpolated', () => {
  // pixel (0, 0): node (0, 0) weighs 0.0625, node (1, 1) 0.5625
  assert.ok(Number.isNaN(sampleAt(piece2(entry((r, c) => (r === 0 && c === 0 ? 7 : NaN))), 0, 0).d), 'one light node only: none');
  assert.ok(Math.abs(sampleAt(piece2(entry((r, c) => (r === 1 && c === 1 ? 7 : NaN))), 0, 0).d - 7) < 1e-3, 'the heavy node alone: its depth');
  // pixel (1, 1): node (1, 1) weighs 0.5625 and comes first; node (2, 2) weighs 0.0625 and comes last
  const flags = (r, c) => (r === 1 && c === 1 ? 3 : r === 2 && c === 2 ? 9 : r === 1 && c === 2 ? 5 : r === 2 && c === 1 ? 6 : 0);
  assert.equal(sampleAt(piece2(entry(() => 12, flags)), 1, 1).sf, 3, 'the heaviest node\'s flag byte, not the last one\'s');
  const e = entry(() => 12), P = 258 * 258;
  e.shade = new Uint8Array(P);
  e.shade[1 * 258 + 1] = 200; e.shade[1 * 258 + 2] = 100; e.shade[2 * 258 + 1] = 100; e.shade[2 * 258 + 2] = 40;
  const want = (0.5625 * 200 + 0.1875 * 100 + 0.1875 * 100 + 0.0625 * 40) / 127.5;
  assert.ok(Math.abs(sampleAt(piece2(e), 1, 1).sh - want) < 1e-5, 'the shade bilinear over the same nodes');
});

test('the shade: lit from the west and north, darker facing away', () => {
  const at = (depthFn) => {
    const e = entry(depthFn); e.ty = 16384;                  // a zoom-15 tile on the equator
    return I.bathyShade(e)[129 * 258 + 129];
  };
  // facing west = rising to the east (depth falls eastward); facing north = rising to the south
  const westFacing = at((r, c) => 200 - c), eastFacing = at((r, c) => 100 + c);
  const northFacing = at((r, c) => 200 - r), southFacing = at((r, c) => 100 + r);
  assert.ok(westFacing > 135 && eastFacing < 120, westFacing + ' / ' + eastFacing);
  assert.ok(northFacing > 135 && southFacing < 120, northFacing + ' / ' + southFacing);
  assert.equal(at(() => 50), 128, 'flat ground: the middle value');
});

test('the store: a broken index or presence file is final (no retry); the eviction never lets go of a tile in view', async () => {
  const w = world();
  const st = w.I.bathyStore('https://x'); const p = st.load(); w.answer(/regions\.json$/, json('regions.json')); await p;
  const hawaii = st.regions.find((r) => r.name === 'hawaii');
  assert.equal(st.indexOf(hawaii, []), undefined);
  w.answer(/hawaii\/v1\/index\.json$/, new TextEncoder().encode('not an index').buffer);
  await until(() => !Object.keys(st.inflight).length);
  assert.equal(st._failed('i:hawaii@1'), 'final');
  assert.equal(st.retryTimer, null, 'nothing to retry');
  assert.equal(st.indexOf(hawaii, []), null);
  const e = I.bathyStore('https://x'), MAX = 32 * 1024 * 1024;
  e.tiles.set('t:a', {}); e.tiles.set('t:b', {}); e.tiles.set('t:c', {});
  e.inUse = () => new Set(['t:a']);
  e.bytes = MAX + 1;
  e._evict();
  assert.ok(e.tiles.has('t:a') && !e.tiles.has('t:b') && e.tiles.has('t:c'), [...e.tiles.keys()].join());
});

test('a tile whose file failed: a held coarser tile of the same region stands in', async () => {
  const w = world();
  const st = w.I.bathyStore('https://x'); const p = st.load(); w.answer(/regions\.json$/, json('regions.json')); await p;
  const hawaii = st.regions.find((r) => r.name === 'hawaii');
  st.index['i:hawaii@1'] = w.I.validBathyIndex(json('hawaii_index.json'), hawaii);
  st.idx.set('f:hawaii@1/62_448', w.I.parseBathyIdx(bytes('idx/62_448.bin'), st.index['i:hawaii@1'].idx));
  st.tiles.set('t:hawaii@1/12/249/1795', { q: readPng(raw('h12_249_1795.png')).q, shade: null, dz: 12, tx: 249, ty: 1795, reg: 'hawaii' });
  for (const final of [true, false]) {
    st.fails['t:hawaii@1/15/1997/14363'] = { at: Date.now(), final };
    const pieces = w.I.bathyPieces(st, { x: 1997, y: 14363, z: 15 }, [], []);
    assert.equal(pieces.length, 1, 'final ' + final);
    assert.equal(pieces[0].dz, 12, 'final ' + final);
  }
});

test('filled pixels are drawn a little darker (x 0.88), nothing else changes', async () => {
  const w = world();
  const st = w.I.bathyStore('https://x'); const p = st.load(); w.answer(/regions\.json$/, json('regions.json')); await p;
  const hawaii = st.regions.find((r) => r.name === 'hawaii');
  st.index['i:hawaii@1'] = w.I.validBathyIndex(json('hawaii_index.json'), hawaii);
  st.idx.set('f:hawaii@1/62_448', w.I.parseBathyIdx(bytes('idx/62_448.bin'), st.index['i:hawaii@1'].idx));
  const plain = readPng(raw('h15_1997_14363.png')).q, P = 258 * 258;
  const draw = (q) => {
    st.tiles.set('t:hawaii@1/15/1997/14363', { q, shade: null, dz: 15, tx: 1997, ty: 14363, reg: 'hawaii' });
    const layer = new w.I.BathyLayer(); layer._map = w.map; layer._queueStale = function () {}; layer.contours = false; layer.store = st;
    const el = canvas(w); layer._draw(el, { x: 1997, y: 14363, z: 15 });
    return el.lastImg;
  };
  const a = draw(plain), filled = new Uint8Array(plain); for (let i = 2 * P; i < 3 * P; i++) filled[i] |= 0x20;
  const b = draw(filled);
  let checked = 0;
  for (let i = 0; i < a.length; i += 4) {
    assert.equal(b[i + 3], a[i + 3]);
    if (!a[i + 3]) continue;
    for (let c = 0; c < 3; c++) if (a[i + c] < 250) { assert.ok(Math.abs(b[i + c] - a[i + c] * 0.88) <= 1.5, i + ':' + c + ' ' + a[i + c] + ' ' + b[i + c]); checked++; }
  }
  assert.ok(checked > 10000, 'water pixels compared: ' + checked);
});

test('contour bands by zoom and unit; dry ground just under level 0; the lines are traced on the twice-smoothed depths in the site\'s unit', () => {
  for (const [z, m, us] of [[17, 1, 5], [15, 1, 5], [14, 2, 10], [13, 2, 10], [12, 5, 15], [11, 5, 15], [10, 10, 30], [9.9, 100, 300], [9, 100, 300], [3, 100, 300]]) {
    assert.equal(I.bathyLevels(z, 'Metric')[1], m, 'Metric z' + z);
    assert.equal(I.bathyLevels(z, 'US')[1], us, 'US z' + z);
  }
  // below zoom 10 only the owner's four depths (2026-11-11): 100 / 500 / 1,000 / 2,000 m or 300 / 1,500 / 3,000 / 6,000 ft
  assert.deepEqual(I.bathyLevels(8, 'Metric').slice(0, 5), [0, 100, 500, 1000, 2000]);
  assert.deepEqual(I.bathyLevels(8, 'US').slice(0, 5), [0, 300, 1500, 3000, 6000]);
  for (const unit of ['US', 'Metric']) {   // nothing past the last line (its sentinel lies below the deepest trench)
    const L = I.bathyLevels(5, unit), per = unit === 'Metric' ? 1 : 3.28084;
    assert.ok(I.bathyLevel(L, 10900 * per) < L.length - 1, unit + ': no line past ' + L[L.length - 2]);
  }
  assert.equal(I.bathyLevel([0, 1, 2], -0.2), -0.2);
  assert.equal(I.bathyLevel([0, 2, 4], -1), -0.49);
  assert.equal(I.bathyLevel([0, 1, 2], -100), -0.49);
  const W = 258, c = 129 * W + 129, far = 10 * W + 10;
  for (const unit of ['US', 'Metric']) {
    const layer = new I.BathyLayer(); layer.unit = unit;
    layer._D.fill(10); layer._D[c] = 30;
    layer._contours({ x: 0, y: 0, z: 15 }, new Uint8ClampedArray(256 * 256 * 4));
    const per = unit === 'Metric' ? 1 : 3.28084, L = I.bathyLevels(15, unit);
    assert.ok(Math.abs(layer._F[far] - I.bathyLevel(L, 10 * per)) < 1e-4, unit + ': flat water at its level');
    assert.ok(layer._F[c] < I.bathyLevel(L, 30 * per) - 0.5, unit + ': the single deep node is smoothed down');
  }
});

test('the credit carries no markup from a source name', () => {
  const html = I.bathyCredit(['<i>N</i>', 'A&B']);
  const text = html.replace(/^.*?">/, '').replace(/<\/a>$/, '');
  assert.ok(!/[<>&]/.test(text), text);
  const title = /title="([^"]*)"/.exec(html)[1];
  assert.ok(!/[<>&]/.test(title), title);
  assert.ok(/title="AB, modified by Allshore Surf"/.test(I.bathyCredit(['A"B'])), 'a quote never closes the title early');
});
