'use strict';
// A lines-v1 encoder for tests: a mirror of tools/places/build_places.py encode_lines (40-byte little-endian header,
// then one LEB128 stream of zigzag values). pieces: [[kind, [[xs, ys, mz10], ...]], ...], xs / ys integers in 1/q degree.
function leb(v) { const out = []; do { const b = v % 128; v = Math.floor(v / 128); out.push(v ? b | 128 : b); } while (v); return out; }
const zz = (v) => (v < 0 ? -2 * v - 1 : 2 * v);
function encodeLines(pieces, cell, q) {
  cell = cell || 10; q = q || 10000;
  const body = []; let nl = 0, nv = 0, kinds = 0, bx0 = Infinity, by0 = Infinity, bx1 = -Infinity, by1 = -Infinity;
  for (const [kind, lines] of pieces) {
    const xs = lines.flatMap((l) => l[0]), ys = lines.flatMap((l) => l[1]);
    const minx = Math.min(...xs), miny = Math.min(...ys), maxx = Math.max(...xs), maxy = Math.max(...ys);
    bx0 = Math.min(bx0, minx); by0 = Math.min(by0, miny); bx1 = Math.max(bx1, maxx); by1 = Math.max(by1, maxy);
    kinds |= 1 << kind;
    body.push(...leb(zz(minx)), ...leb(zz(miny)), ...leb(maxx - minx), ...leb(maxy - miny), ...leb(kind), ...leb(lines.length));
    let px = minx, py = miny;
    for (const [lx, ly, mz] of lines) {
      body.push(...leb(lx.length), ...leb(mz));
      for (let i = 0; i < lx.length; i++) { body.push(...leb(zz(lx[i] - px)), ...leb(zz(ly[i] - py))); px = lx[i]; py = ly[i]; }
      nl++; nv += lx.length;
    }
  }
  if (!pieces.length) bx0 = by0 = bx1 = by1 = 0;
  const buf = new ArrayBuffer(40 + body.length), dv = new DataView(buf), u8 = new Uint8Array(buf);
  u8.set([76, 78, 83, 49], 0); dv.setUint16(4, cell, true); dv.setUint16(6, kinds, true); dv.setUint32(8, q, true);
  dv.setUint32(12, pieces.length, true); dv.setUint32(16, nl, true); dv.setUint32(20, nv, true);
  [bx0, by0, bx1, by1].forEach((v, i) => dv.setInt32(24 + i * 4, v, true));
  u8.set(body, 40);
  return buf;
}
// A places-v1 encoder (the builder's encode_places): places [{lat, lon (degrees), rank, mz10, pop, flags, name}] in order.
function encodePlaces(places, cell, q) {
  cell = cell || 10; q = q || 10000;
  const body = [];
  let bx0 = Infinity, by0 = Infinity, bx1 = -Infinity, by1 = -Infinity;
  for (const p of places) {
    const la = Math.round(p.lat * q), lo = Math.round((p.lon >= 180 ? -180 : p.lon) * q);
    bx0 = Math.min(bx0, lo); by0 = Math.min(by0, la); bx1 = Math.max(bx1, lo); by1 = Math.max(by1, la);
    body.push(...leb(zz(la)), ...leb(zz(lo)), ...leb(p.rank), ...leb(p.mz10), ...leb(p.pop), ...leb(p.flags));
  }
  if (!places.length) bx0 = by0 = bx1 = by1 = 0;
  const names = Buffer.from(places.map((p) => p.name).join('\n'), 'utf8');
  const buf = new ArrayBuffer(36 + body.length + names.length), dv = new DataView(buf), u8 = new Uint8Array(buf);
  u8.set([80, 76, 67, 49], 0); dv.setUint16(4, cell, true); dv.setUint16(6, 0, true); dv.setUint32(8, q, true);
  dv.setUint32(12, places.length, true); dv.setUint32(16, body.length, true);
  [bx0, by0, bx1, by1].forEach((v, i) => dv.setInt32(20 + i * 4, v, true));
  u8.set(body, 36); u8.set(names, 36 + body.length);
  return buf;
}
module.exports = { encodeLines, encodePlaces, leb, zz };
