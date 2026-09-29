'use strict';
// A JS encoder for coast-v1 (mirror of tools/coast/build_coast.py encode_file), for the map tools' test fixtures.
function leb(v) { const out = []; do { const b = v % 128; v = Math.floor(v / 128); out.push(v ? b | 128 : b); } while (v); return out; }
const zz = (v) => (v < 0 ? -2 * v - 1 : 2 * v);
function encodeCoast(pieces, cell) {                   // pieces: [[ring, ...]], ring = flat [x, y, ...] in 1e-4 deg
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
const sq = (x0, y0, x1, y1) => [D(x0), D(y0), D(x1), D(y0), D(x1), D(y1), D(x0), D(y1)];     // CCW in (lon, lat)

module.exports = { encodeCoast, D, sq };
