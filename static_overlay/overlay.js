/* Allshore Surf model overlay (Phase 3: animated frames per field). Loaded on demand; never on page load.
 *
 * Data contract (manifest schema 2 or 3 from tools/model_frames): 8-bit greyscale PNG frames,
 * q = 0 missing, value = lo + (q - 1) / 254 * (hi - lo), q = 1 means "<= lo", q = 255 ">= hi";
 * grid centre-registered from (+90 N, -180 E), longitude periodic; grid_half = full[::2, ::2].
 * Frames are sampled per canvas tile (inverse Web-Mercator) in a dedicated 'modelPane'
 * (z 250, pointer-events none), so markers and gestures are untouched. ONE sampler
 * (ModelGridLayer._code) feeds both the drawn pixels and the hover/long-press readout.
 */
(function () {
  'use strict';

  var SESSION_KEY = 'allshore.overlay.v1';
  var STALE_AFTER_S = 9 * 3600;               // pointer older than this -> "stale" banner
  var POINTER_RECHECK_MS = 30 * 60 * 1000;    // re-read latest.json on mount when the cached pointer is older
  var ENCODING = 'u8-linear-v2';
  var TILE = 256;
  // <= 29 visible characters incl. the separator so it never wraps over the zoom control; the full
  // sentence (manifest attribution) is in the panel note.
  var ATTRIBUTION = 'Overlay: <a href="https://polar.ncep.noaa.gov/waves/" target="_blank" rel="noopener">NOAA GFS-Wave</a>/GFS';
  var RAMPS = {
    hs:   [[0,'#0b2c6b'],[0.15,'#1f6fd6'],[0.3,'#19c3e6'],[0.45,'#3fd96b'],[0.6,'#f2e33a'],[0.75,'#f5901f'],[0.9,'#e02020'],[1,'#a3129e']],
    tp:   [[0,'#2a1f7a'],[0.25,'#2e7ed8'],[0.5,'#38c9a8'],[0.7,'#c8e63c'],[0.85,'#f7a52b'],[1,'#e8321f']],
    wind: [[0,'#e8f1ff'],[0.2,'#8cc4ff'],[0.4,'#3aa35a'],[0.6,'#f0d433'],[0.8,'#f0731f'],[1,'#b00f3a']]
  };
  // Legend tick values in DISPLAY units per field and site unit; the legend top is added as "N+".
  // No numeric tick above ~80 % of the bar: it would collide with the right-aligned top label.
  var TICKS = {
    'hs|US': [0, 10, 20, 30], 'hs|Metric': [0, 3, 6, 9],
    'tp|US': [4, 8, 12, 16], 'tp|Metric': [4, 8, 12, 16],
    'wind|US': [0, 20, 40], 'wind|Metric': [0, 25, 50, 75]
  };

  function saved() { try { return JSON.parse(sessionStorage.getItem(SESSION_KEY) || '{}'); } catch (e) { return {}; } }
  function save(patch) { try { sessionStorage.setItem(SESSION_KEY, JSON.stringify(Object.assign(saved(), patch))); } catch (e) {} }
  function hexToRgb(h) { var n = parseInt(h.slice(1), 16); return [n >> 16 & 255, n >> 8 & 255, n & 255]; }
  function buildRamp(stops) {
    // 256-entry RGB lookup over t in [0,1]
    var out = new Uint8ClampedArray(256 * 3);
    for (var i = 0; i < 256; i++) {
      var t = i / 255, k = 0;
      while (k < stops.length - 2 && t > stops[k + 1][0]) k++;
      var a = stops[k], b = stops[k + 1], f = (t - a[0]) / Math.max(1e-9, b[0] - a[0]);
      f = Math.max(0, Math.min(1, f));
      var ca = hexToRgb(a[1]), cb = hexToRgb(b[1]);
      out[i * 3] = ca[0] + (cb[0] - ca[0]) * f; out[i * 3 + 1] = ca[1] + (cb[1] - ca[1]) * f; out[i * 3 + 2] = ca[2] + (cb[2] - ca[2]) * f;
    }
    return out;
  }
  function pad3(n) { return (n < 10 ? '00' : n < 100 ? '0' : '') + n; }
  function clear(el) { if (el) while (el.firstChild) el.removeChild(el.firstChild); }
  function mk(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text !== undefined) e.textContent = text; return e; }

  // ---- units (site preference: 'US' | 'Metric') ----
  function unitOf(field, unit) {
    if (field === 'hs') return unit === 'Metric' ? { label: 'm', f: function (v) { return v; }, d: 1 } : { label: 'ft', f: function (v) { return v * 3.28084; }, d: 1 };
    if (field === 'tp') return { label: 's', f: function (v) { return v; }, d: 1 };
    return unit === 'Metric' ? { label: 'km/h', f: function (v) { return v * 3.6; }, d: 0 } : { label: 'mph', f: function (v) { return v * 2.23694; }, d: 0 };
  }
  function legendTicks(field, fdef, unit) {
    var u = unitOf(field, unit), lo = u.f(fdef.legend[0]), hi = u.f(fdef.legend[1]);
    var vals = TICKS[field + '|' + (unit === 'Metric' ? 'Metric' : 'US')] || [fdef.legend[0]];
    var out = vals.map(function (v, i) {
      var below = i === 0 && fdef.lo < fdef.legend[0] - 1e-9;          // values under the legend floor exist (Tp)
      return { pos: Math.max(0, Math.min(1, (v - lo) / (hi - lo))), label: (below ? '≤' : '') + Math.round(v) };
    });
    out.push({ pos: 1, label: Math.round(hi) + '+ ' + u.label });
    return out;
  }

  // ---- manifest helpers (schema 2: per-frame file keys; schema 3: one template) ----
  function frameKey(m, fr, field, half) {
    if (fr.files && fr.files[field]) return fr.files[field][half ? 'half' : 'full'];
    var t = m.files && m.files.template, res = m.files && m.files.res;
    if (!t || !res) throw new Error('manifest has no frame files');
    return t.replace('{res}', half ? res.half : res.full).replace('{field}', field).replace('{step:03d}', pad3(fr.step));
  }
  function validateManifest(m) {
    if (!m || (m.schema !== 2 && m.schema !== 3)) throw new Error('unsupported manifest schema');
    if (m.encoding !== ENCODING) throw new Error('unsupported frame encoding ' + m.encoding);
    if (m.complete !== true) throw new Error('published run is not complete');
    if (m.fill !== undefined && (typeof m.fill !== 'object' || m.fill === null || Array.isArray(m.fill))) throw new Error('manifest incomplete');
    if (typeof m.run !== 'string' || typeof m.run_utc !== 'string' || isNaN(Date.parse(m.run_utc)) || !m.fields || typeof m.fields !== 'object' ||
      !m.grid || typeof m.grid !== 'object' || !m.grid_half || typeof m.grid_half !== 'object' || (m.model !== undefined && (typeof m.model !== 'object' || m.model === null)) ||
      !Array.isArray(m.frames) || !m.frames.length || m.frames.length > 512) throw new Error('manifest incomplete');
    var tpl = m.schema === 3 && m.files && m.files.res, t = m.schema === 3 && m.files && m.files.template;
    if (m.schema === 3 && !(tpl && typeof t === 'string' && t.indexOf('{res}') >= 0 && t.indexOf('{field}') >= 0 && t.indexOf('{step:03d}') >= 0 &&
      typeof tpl.full === 'string' && typeof tpl.half === 'string')) throw new Error('manifest incomplete');
    for (var i = 0; i < m.frames.length; i++) {
      var fr = m.frames[i], prev = i ? m.frames[i - 1] : null;
      if (!fr || typeof fr.step !== 'number' || !fr.valid_utc || isNaN(Date.parse(fr.valid_utc))) throw new Error('bad frame entry ' + i);
      if (prev && !(fr.step > prev.step && Date.parse(fr.valid_utc) > Date.parse(prev.valid_utc))) throw new Error('bad frame entry ' + i);   // one picture per time
      if (m.schema === 2) {
        for (var name in m.fields) {
          var f = fr.files && fr.files[name];
          if (!f || typeof f.full !== 'string' || typeof f.half !== 'string') throw new Error('bad frame entry ' + i);
        }
      }
    }
    return m;
  }
  // The decoded PNG must be the grid the manifest describes; anything else is drawn nowhere.
  function validateGrid(grid, frame, fdef) {
    var ok = grid && frame && fdef && frame.cols === grid.cols && frame.rows === grid.rows && frame.q && frame.q.length === grid.cols * grid.rows &&
      grid.registration === 'center' && grid.lon_periodic === true && grid.dlat < 0 && grid.dlon > 0 &&
      Math.abs(grid.cols * grid.dlon - 360) < 1e-6 && Math.abs(grid.lat0 - 90) < 1e-9 &&
      Math.abs(grid.lat0 + (grid.rows - 1) * grid.dlat + 90) < 1e-6 && Math.abs(grid.lon0 + 180) < 1e-9 &&
      typeof fdef.lo === 'number' && typeof fdef.hi === 'number' && fdef.lo < fdef.hi && fdef.legend && fdef.legend[0] < fdef.legend[1] &&
      fdef.legend[0] >= fdef.lo - 1e-9 && fdef.legend[1] <= fdef.hi + 1e-9;
    if (!ok) throw new Error('frame does not match the manifest grid');
  }
  function pickFrame(m, now) {
    now = now === undefined ? Date.now() : now;
    for (var i = 0; i < m.frames.length; i++) if (Date.parse(m.frames[i].valid_utc) >= now) return i;
    return m.frames.length - 1;
  }
  // Half-resolution (0.5 deg) frames where a 0.25 deg cell is only a few pixels anyway, with hysteresis
  // so a zoom hovering around the threshold does not re-fetch on every step. Data budgets (plan section 6:
  // a full 81-frame loop <= 16 MB on desktops, <= 5 MB on phones): narrow maps (phones) stay on the half
  // frames until zoom 7 (a 0.25 deg cell is 22 px there); wind frames are ~2.7x larger (land included), so
  // wind stays at half resolution until zoom 7 everywhere (full loop 32 MB vs 10 MB).
  function wantHalf(zoom, width, field) {
    if (field === 'wind' || width < 700) return zoom < 7;
    return zoom < 3.5;
  }
  function wantFull(zoom, width, field) {
    if (field === 'wind' || width < 700) return zoom >= 7.5;
    return zoom >= 4;
  }
  // Pixel centre of a Web-Mercator tile pixel (the same expressions tileCodes uses).
  function tilePixelLatLng(coords, px, py) {
    var n = TILE * Math.pow(2, coords.z);
    var lon = (coords.x * TILE + px + 0.5) / n * 360 - 180;
    var lat = Math.atan(Math.sinh(Math.PI - 2 * Math.PI * (coords.y * TILE + py + 0.5) / n)) * 180 / Math.PI;
    return { lat: lat, lng: lon };
  }
  // World pixel coordinates of a point at integer zoom z (the inverse of tilePixelLatLng's convention).
  function forwardPixel(lat, lng, z) {
    var n = TILE * Math.pow(2, z), s = Math.sin(Math.max(-89.9, Math.min(89.9, lat)) * Math.PI / 180);
    return { x: (lng + 180) / 360 * n, y: (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * n };
  }
  // The centre of the drawn pixel that contains (lat, lng) at tile zoom z: the readout samples THIS
  // point, so it reports exactly the value the tile shows under the cursor (same sampler, same sample).
  function pixelOf(lat, lng, z) {
    var p = forwardPixel(lat, lng, z), px = Math.floor(p.x), py = Math.floor(p.y);
    var cx = Math.floor(px / TILE), cy = Math.floor(py / TILE);
    return { z: z, x: cx, y: cy, px: px - cx * TILE, py: py - cy * TILE };   // unwrapped tile coords, like Leaflet's _tiles keys
  }
  function snapToPixel(lat, lng, z) {
    var p = pixelOf(lat, lng, z);
    return tilePixelLatLng(p, p.px, p.py);
  }

  // ---- coastlines (the land clip) ----
  // Wave height and peak period are clipped to the ocean: every tile's land alpha is rasterised once
  // per tile per zoom from GSHHG polygons (tools/coast, format coast-v1) and multiplied into the pixel
  // alpha on every frame; the readout consults the same mask, so it still reports the drawn pixel.
  // Tier 0 (~1 km, one file) serves tile zooms up to index.tier0.max_zoom; tier 1 (full resolution,
  // 5-degree cells) is fetched per cell in view for higher zooms, tier 0 standing in until it lands.
  // Wind is never clipped (owner decision: the full field over land).
  var CLIP_FIELDS = { hs: true, tp: true, wind: false };
  var LAND_ALL = new Uint8Array(0);                        // sentinel: the tile is entirely land
  var LAND_READOUT = 128;                                  // composed alpha below this = "over land" for the readout
  var MAX_LAT = 85.0511287798;                             // Web-Mercator limit; the Antarctic ring closes through the pole
  var MAX_COAST_BYTES = 8 * 1024 * 1024;                  // one coast file is <= 1.4 MB; nothing bigger is decoded
  var MAX_CHUNK_BYTES = 32 * 1024 * 1024;                 // decoded tier-1 chunks kept (LRU), in vertex bytes
  var MAX_COAST_INFLIGHT = 2;
  var MIN_PIECE_PX = 0.5;                                  // a piece smaller than this in both directions is not drawn
  var COAST_RETRY_MS = 60 * 1000;
  // World pixel coordinates at zoom 0 (0..256), lat clamped to the Mercator limit.
  function worldXY(lon, lat) {
    var s = Math.sin(Math.max(-MAX_LAT, Math.min(MAX_LAT, lat)) * Math.PI / 180);
    return [(lon + 180) / 360 * TILE, (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * TILE];
  }
  // coast-v1: 40-byte header (magic "CST1", u16 cell, u16 0, u32 q, u32 pieces, u32 rings, u32 vertices,
  // i32 bbox[4]) then one LEB128 varint stream; per piece zz(minx) zz(miny) w h nrings, per ring n then
  // n zigzag delta pairs (first relative to the piece corner). Decoded once into zoom-0 world pixels.
  function decodeCoast(buf) {
    if (!(buf instanceof ArrayBuffer) || buf.byteLength < 40 || buf.byteLength > MAX_COAST_BYTES) throw new Error('coast decode failed');
    var u8 = new Uint8Array(buf), dv = new DataView(buf);
    if (u8[0] !== 67 || u8[1] !== 83 || u8[2] !== 84 || u8[3] !== 49) throw new Error('coast decode failed');
    var cell = dv.getUint16(4, true), q = dv.getUint32(8, true), nP = dv.getUint32(12, true), nR = dv.getUint32(16, true), nV = dv.getUint32(20, true);
    if (!q || nP > 200000 || nR > 400000 || nV > 4000000 || nR < nP || nV < 3 * nR) throw new Error('coast decode failed');
    var pos = 40, end = u8.length;
    function varint() {                                    // up to 35 bits; the fifth byte via a multiply (no 32-bit overflow)
      var v = 0, shift = 1, b;
      do {
        if (pos >= end) throw new Error('coast decode failed');
        b = u8[pos++]; v += (b & 127) * shift; shift *= 128;
        if (shift > 34359738368) throw new Error('coast decode failed');
      } while (b & 128);
      return v;
    }
    function zz() { var v = varint(); return v % 2 ? -(v + 1) / 2 : v / 2; }
    var box = new Float32Array(nP * 4), ringStart = new Int32Array(nP + 1), vertStart = new Int32Array(nR + 1), xy = new Float32Array(nV * 2);
    var r = 0, v = 0, inv = 1 / q;
    for (var p = 0; p < nP; p++) {
      var minx = zz(), miny = zz(); varint(); varint();                     // the integer bbox is recomputed from the projected vertices
      var nr = varint();
      if (r + nr > nR) throw new Error('coast decode failed');
      var bx0 = Infinity, by0 = Infinity, bx1 = -Infinity, by1 = -Infinity;
      ringStart[p] = r;
      for (var k = 0; k < nr; k++) {
        var n = varint();
        if (n < 3 || v + n > nV) throw new Error('coast decode failed');
        vertStart[r++] = v;
        var x = minx, y = miny;
        for (var i = 0; i < n; i++) {
          x += zz(); y += zz();
          var w = worldXY(x * inv, y * inv);
          xy[v * 2] = w[0]; xy[v * 2 + 1] = w[1]; v++;
          if (w[0] < bx0) bx0 = w[0]; if (w[0] > bx1) bx1 = w[0]; if (w[1] < by0) by0 = w[1]; if (w[1] > by1) by1 = w[1];
        }
      }
      box[p * 4] = bx0; box[p * 4 + 1] = by0; box[p * 4 + 2] = bx1; box[p * 4 + 3] = by1;
    }
    ringStart[nP] = r; vertStart[nR] = v;
    if (r !== nR || v !== nV || pos !== end) throw new Error('coast decode failed');
    return { cell: cell, n: nP, box: box, ringStart: ringStart, vertStart: vertStart, xy: xy, bytes: xy.byteLength };
  }
  // Tile coords wrapped into the single world (x may be a world copy); the tile's box in zoom-0 units.
  function tileBox(coords) {
    var n = Math.pow(2, coords.z), xw = ((coords.x % n) + n) % n, s = TILE / n;
    return { z: coords.z, xw: xw, y: coords.y, n: n, x0: xw * s, y0: coords.y * s, x1: (xw + 1) * s, y1: (coords.y + 1) * s };
  }
  // The 5-degree (cellDeg) tier-1 cells a tile touches, as "lat0_lon0" names (south-west corners).
  function coastCellsForTile(coords, cellDeg) {
    var b = tileBox(coords), lonW = b.x0 / TILE * 360 - 180, lonE = b.x1 / TILE * 360 - 180;
    function lat(y) { return Math.atan(Math.sinh(Math.PI - 2 * Math.PI * y / TILE)) * 180 / Math.PI; }
    var latN = lat(b.y0), latS = lat(b.y1), eps = 1e-9, out = [];
    var i0 = Math.max(-Math.round(90 / cellDeg), Math.floor(latS / cellDeg)), i1 = Math.min(Math.round(90 / cellDeg) - 1, Math.floor((latN - eps) / cellDeg));
    var j0 = Math.max(-Math.round(180 / cellDeg), Math.floor(lonW / cellDeg)), j1 = Math.min(Math.round(180 / cellDeg) - 1, Math.floor((lonE - eps) / cellDeg));
    for (var i = i0; i <= i1; i++) for (var j = j0; j <= j1; j++) out.push((i * cellDeg) + '_' + (j * cellDeg));
    return out;
  }
  // Rings of every piece that reaches the tile, in tile pixel space (Float32Array x,y pairs), from the
  // given decoded sets; sub-pixel pieces and same-pixel vertices are dropped.
  function landPathsForTile(coords, sets) {
    var b = tileBox(coords), scale = b.n, ox = b.xw * TILE, oy = coords.y * TILE, out = [];
    for (var s = 0; s < sets.length; s++) {
      var c = sets[s], box = c.box;
      for (var p = 0; p < c.n; p++) {
        var k = p * 4;
        if (box[k + 2] < b.x0 || box[k] > b.x1 || box[k + 3] < b.y0 || box[k + 1] > b.y1) continue;
        if ((box[k + 2] - box[k]) * scale < MIN_PIECE_PX && (box[k + 3] - box[k + 1]) * scale < MIN_PIECE_PX) continue;
        for (var r = c.ringStart[p]; r < c.ringStart[p + 1]; r++) {
          var v0 = c.vertStart[r], v1 = c.vertStart[r + 1], ring = new Float32Array((v1 - v0) * 2), m = 0, lx = NaN, ly = NaN;
          for (var v = v0; v < v1; v++) {
            var x = c.xy[v * 2] * scale - ox, y = c.xy[v * 2 + 1] * scale - oy;
            if (m && Math.abs(x - lx) < 0.5 && Math.abs(y - ly) < 0.5) continue;
            ring[m * 2] = x; ring[m * 2 + 1] = y; m++; lx = x; ly = y;
          }
          if (m >= 3) out.push(m * 2 === ring.length ? ring : ring.subarray(0, m * 2));
        }
      }
    }
    return out;
  }
  // Pure nonzero-winding scanline rasteriser (4 sub-rows, exact horizontal coverage): the reference for
  // tests and the fallback where a canvas is not available. -> Uint8Array(size*size) coverage 0..255.
  function rasteriseScanline(paths, size) {
    var SUB = 4, rows = size * SUB, buckets = new Array(rows), out = new Uint8Array(size * size), i, j;
    for (i = 0; i < rows; i++) buckets[i] = null;
    for (i = 0; i < paths.length; i++) {
      var ring = paths[i], n = ring.length / 2;
      for (j = 0; j < n; j++) {
        var x0 = ring[j * 2], y0 = ring[j * 2 + 1], x1 = ring[((j + 1) % n) * 2], y1 = ring[((j + 1) % n) * 2 + 1];
        if (y0 === y1) continue;
        var dir = y1 > y0 ? 1 : -1, ya = Math.min(y0, y1), yb = Math.max(y0, y1);
        var r0 = Math.max(0, Math.ceil(ya * SUB - 0.5)), r1 = Math.min(rows - 1, Math.ceil(yb * SUB - 0.5) - 1);
        for (var rr = r0; rr <= r1; rr++) {
          var sy = (rr + 0.5) / SUB, x = x0 + (sy - y0) * (x1 - x0) / (y1 - y0);
          (buckets[rr] || (buckets[rr] = [])).push(x, dir);
        }
      }
    }
    var cov = new Float32Array(size);
    for (var row = 0; row < size; row++) {
      cov.fill(0);
      var any = false;
      for (var sub = 0; sub < SUB; sub++) {
        var xs = buckets[row * SUB + sub];
        if (!xs) continue;
        var pairs = [];
        for (i = 0; i < xs.length; i += 2) pairs.push([xs[i], xs[i + 1]]);
        pairs.sort(function (a, b) { return a[0] - b[0]; });
        var wnd = 0;
        for (i = 0; i < pairs.length - 1; i++) {
          wnd += pairs[i][1];
          if (!wnd) continue;
          var xa = Math.max(0, pairs[i][0]), xb = Math.min(size, pairs[i + 1][0]);
          if (xb <= xa) continue;
          any = true;
          var pa = Math.floor(xa), pb = Math.min(size - 1, Math.ceil(xb) - 1);
          for (var px = pa; px <= pb; px++) cov[px] += Math.min(xb, px + 1) - Math.max(xa, px);
        }
      }
      if (!any) continue;
      for (i = 0; i < size; i++) if (cov[i] > 0) out[row * size + i] = Math.min(255, Math.round(cov[i] * 255 / SUB));
    }
    return out;
  }
  var maskCanvas = null;
  // Canvas rasteriser (anti-aliased, one nonzero fill for all pieces so shared cell edges never seam);
  // falls back to the scanline version where a 2D context is not available.
  function rasterise(paths, size) {
    try {
      var c = maskCanvas || (maskCanvas = document.createElement('canvas'));
      if (c.width !== size || c.height !== size) { c.width = size; c.height = size; }
      var ctx = c.getContext('2d', { willReadFrequently: true });
      ctx.clearRect(0, 0, size, size);
      ctx.beginPath();
      for (var i = 0; i < paths.length; i++) {
        var ring = paths[i];
        ctx.moveTo(ring[0], ring[1]);
        for (var j = 2; j < ring.length; j += 2) ctx.lineTo(ring[j], ring[j + 1]);
        ctx.closePath();
      }
      ctx.fillStyle = '#000';
      ctx.fill('nonzero');
      var d = ctx.getImageData(0, 0, size, size).data, out = new Uint8Array(size * size);
      for (var k = 0, a = 3; k < out.length; k++, a += 4) out[k] = d[a];
      return out;
    } catch (e) {
      return rasteriseScanline(paths, size);
    }
  }
  // null = no land in the tile, LAND_ALL = nothing but land, else the mask itself.
  function maskState(mask) {
    var lo = 255, hi = 0;
    for (var i = 0; i < mask.length && (lo || hi < 255); i++) { var m = mask[i]; if (m < lo) lo = m; if (m > hi) hi = m; }
    if (hi === 0) return null;
    if (lo === 255) return LAND_ALL;
    return mask;
  }
  // Codes -> RGBA for one tile: colour from the ramp over the legend range, alpha 255 (no data: 0)
  // times the ocean fraction (255 - land).
  function composeTile(codes, land, lut, lo, hi, L0, L1, d) {
    var scale = 255 / (L1 - L0);
    for (var i = 0, k = 0; i < codes.length; i++, k += 4) {
      var code = codes[i], a = land ? 255 - land[i] : 255;
      if (!code || !a) { d[k + 3] = 0; continue; }
      var t = Math.round((lo + (code - 1) / 254 * (hi - lo) - L0) * scale);
      t = t < 0 ? 0 : t > 255 ? 255 : t;
      d[k] = lut[t * 3]; d[k + 1] = lut[t * 3 + 1]; d[k + 2] = lut[t * 3 + 2]; d[k + 3] = a;
    }
    return d;
  }
  // Coast data for one base URL (index.json + tier 0 up front, tier-1 chunks on demand). One per URL
  // per page: the decoded tier 0 (~3 MB) and the chunk LRU survive Off/On.
  var COAST_STORES = {};
  function coastStore(url) { return COAST_STORES[url] || (COAST_STORES[url] = new CoastStore(url)); }
  function CoastStore(url) {
    this.url = url; this.status = 'idle'; this.index = null; this.tier0 = null; this.loading = null;
    this.chunks = new Map(); this.bytes = 0; this.inflight = {}; this.queue = []; this.failed = {};
    this.rev = 0; this.onChange = null; this.abort = null;
  }
  // Resolves to the store when tier 0 is usable, to null otherwise (never rejects); a failed load is
  // retried on the next call. The load has its own abort (Off), so a field change while it runs
  // simply keeps waiting for it.
  CoastStore.prototype.load = function () {
    var self = this;
    if (this.status === 'ok') return Promise.resolve(this);
    if (this.loading) return this.loading;
    this.status = 'loading';
    var base = this.url, ctrl = this.loadAbort = new AbortController(), sig = ctrl.signal;
    this.loading = Promise.all([
      fetch(base + '/index.json', { signal: sig, mode: 'cors' }).then(function (r) { if (!r.ok) throw new Error('coast ' + r.status); return r.json(); }),
      fetch(base + '/world-i.bin', { signal: sig, mode: 'cors' }).then(function (r) {
        if (!r.ok) throw new Error('coast ' + r.status);
        if (Number(r.headers.get('content-length') || 0) > MAX_COAST_BYTES) throw new Error('coast decode failed');
        return r.arrayBuffer();
      })
    ]).then(function (res) {
      if (self.loadAbort !== ctrl || sig.aborted) throw abortError();     // aborted or superseded: this load owns nothing any more
      var idx = res[0];
      if (!idx || idx.format !== 'coast-v1' || !idx.tier0 || !idx.tier1 || typeof idx.tier1.cell !== 'number' || typeof idx.tier0.max_zoom !== 'number' ||
        !idx.tier1.cells || typeof idx.tier1.cells !== 'object') throw new Error('unsupported coast index');
      self.tier0 = decodeCoast(res[1]); self.index = idx; self.status = 'ok'; self.loading = null; self.loadAbort = null;
      return self;
    }).catch(function () {
      if (self.loadAbort === ctrl) { self.loading = null; self.loadAbort = null; self.status = sig.aborted ? 'idle' : 'failed'; }
      return null;
    });
    return this.loading;
  };
  CoastStore.prototype.tier1Zoom = function (z) { return this.status === 'ok' && z > this.index.tier0.max_zoom; };
  // The decoded sets a tile should be rasterised from, and whether they are the final ones. Missing
  // tier-1 chunks are requested; until they land the tile uses tier 0 (complete: false).
  CoastStore.prototype.setsFor = function (coords) {
    if (this.status !== 'ok') return { sets: [], complete: true };
    if (!this.tier1Zoom(coords.z)) return { sets: [this.tier0], complete: true };
    var names = coastCellsForTile(coords, this.index.tier1.cell), sets = [], missing = [];
    for (var i = 0; i < names.length; i++) {
      var nm = names[i];
      if (!this.index.tier1.cells[nm]) continue;                          // no land in that cell
      var c = this.chunks.get(nm);
      if (c) { this.chunks.delete(nm); this.chunks.set(nm, c); sets.push(c); }   // LRU touch
      else if (this.failed[nm] === true) continue;                         // permanently absent: treated as no land
      else missing.push(nm);
    }
    if (!missing.length) return { sets: sets, complete: true };
    this.request(missing);
    return { sets: [this.tier0], complete: false };
  };
  CoastStore.prototype.request = function (names) {
    for (var i = 0; i < names.length; i++) {
      var nm = names[i], f = this.failed[nm];
      if (!this.index.tier1.cells[nm] || this.inflight[nm] || this.queue.indexOf(nm) >= 0 || this.chunks.has(nm) || f === true || (typeof f === 'number' && f > Date.now())) continue;
      this.queue.push(nm);
    }
    this._pump();
  };
  CoastStore.prototype._pump = function () {
    var self = this;
    while (this.queue.length && Object.keys(this.inflight).length < MAX_COAST_INFLIGHT) {
      var nm = this.queue.shift();
      if (!this.abort) this.abort = new AbortController();
      (function (name, ctrl) {
        self.inflight[name] = true;
        fetch(self.url + '/' + self.index.tier1.dir + '/' + name + '.bin', { signal: ctrl.signal, mode: 'cors' }).then(function (r) {
          if (!r.ok) throw new Error('coast ' + r.status);
          if (Number(r.headers.get('content-length') || 0) > MAX_COAST_BYTES) throw new Error('coast decode failed');
          return r.arrayBuffer();
        }).then(function (buf) {
          delete self.inflight[name];
          if (ctrl.signal.aborted) return;
          var c = decodeCoast(buf);
          self.chunks.set(name, c); self.bytes += c.bytes; delete self.failed[name];
          self._evict();
          self.rev++;
          if (self.onChange) self.onChange();
          self._pump();
        }).catch(function (err) {
          delete self.inflight[name];
          if (ctrl.signal.aborted || (err && err.name === 'AbortError')) return;
          var m = /^coast (\d{3})$/.exec(String(err && err.message || ''));
          self.failed[name] = (m && (m[1] === '404' || m[1] === '410')) || /decode/.test(String(err && err.message)) ? true : Date.now() + COAST_RETRY_MS;
          self.rev++;
          if (self.onChange) self.onChange();                                // a tile waiting on this cell falls back for good / for now
          self._pump();
        });
      })(nm, this.abort);
    }
  };
  CoastStore.prototype._evict = function () {
    var it = this.chunks.keys();
    while (this.bytes > MAX_CHUNK_BYTES && this.chunks.size > 1) {
      var k = it.next().value, c = this.chunks.get(k);
      this.chunks.delete(k); this.bytes -= c.bytes;
    }
  };
  CoastStore.prototype.abortAll = function () {
    if (this.abort) { this.abort.abort(); this.abort = null; }
    if (this.loadAbort) { this.loadAbort.abort(); this.loadAbort = null; }
    if (this.status === 'loading') { this.status = 'idle'; this.loading = null; }
    this.inflight = {}; this.queue = []; this.onChange = null;
  };

  // ---- frame decoding ----
  // Frames are 8-bit greyscale, non-interlaced PNGs. The direct path parses the chunks, lets the
  // browser inflate the zlib stream (DecompressionStream) and unfilters the rows into the 1 MB code
  // array: no bitmap, no 4 MB RGBA copy per frame (the canvas path is kept for older browsers).
  var MAX_FRAME_BYTES = 2 * 1024 * 1024;                   // live frames are <= 0.5 MB; nothing bigger is decoded
  function parsePng(buf, expect) {
    var u8 = new Uint8Array(buf), dv = new DataView(buf);
    if (u8.length < 8 || u8[0] !== 137 || u8[1] !== 80 || u8[2] !== 78 || u8[3] !== 71) throw new Error('frame decode failed');
    var pos = 8, w = 0, h = 0, depth = 0, ctype = 0, interlace = 0, idat = [], total = 0;
    while (pos + 8 <= u8.length) {
      var len = dv.getUint32(pos), type = String.fromCharCode(u8[pos + 4], u8[pos + 5], u8[pos + 6], u8[pos + 7]), start = pos + 8;
      if (start + len > u8.length) throw new Error('frame decode failed');
      if (type === 'IHDR') {
        w = dv.getUint32(start); h = dv.getUint32(start + 4); depth = u8[start + 8]; ctype = u8[start + 9]; interlace = u8[start + 12];
        // the manifest says how big a frame is: anything else is refused BEFORE it is inflated
        if (expect && (w !== expect.cols || h !== expect.rows)) throw new Error('frame decode failed');
      }
      else if (type === 'IDAT') { idat.push(u8.subarray(start, start + len)); total += len; }
      else if (type === 'IEND') break;
      pos = start + len + 4;
    }
    if (!w || !h || depth !== 8 || ctype !== 0 || interlace !== 0) return null;   // not our format: the canvas path handles it
    var z = new Uint8Array(total), o = 0;
    for (var i = 0; i < idat.length; i++) { z.set(idat[i], o); o += idat[i].length; }
    return { w: w, h: h, z: z };
  }
  function inflate(z) {
    var ds = new DecompressionStream('deflate'), writer = ds.writable.getWriter();
    writer.write(z).catch(function () {}); writer.close().catch(function () {});
    return new Response(ds.readable).arrayBuffer().then(function (b) { return new Uint8Array(b); });
  }
  // PNG scanline filters 0..4 for one byte per pixel.
  function unfilter(raw, w, h) {
    var out = new Uint8Array(w * h), stride = w + 1, x, a, b, c, p, pa, pb, pc;
    for (var y = 0; y < h; y++) {
      var f = raw[y * stride], src = y * stride + 1, dst = y * w, up = dst - w;
      if (f === 0) out.set(raw.subarray(src, src + w), dst);
      else if (f === 1) for (x = 0; x < w; x++) out[dst + x] = (raw[src + x] + (x ? out[dst + x - 1] : 0)) & 255;
      else if (f === 2) for (x = 0; x < w; x++) out[dst + x] = (raw[src + x] + (y ? out[up + x] : 0)) & 255;
      else if (f === 3) for (x = 0; x < w; x++) out[dst + x] = (raw[src + x] + (((x ? out[dst + x - 1] : 0) + (y ? out[up + x] : 0)) >> 1)) & 255;
      else if (f === 4) for (x = 0; x < w; x++) {
        a = x ? out[dst + x - 1] : 0; b = y ? out[up + x] : 0; c = (x && y) ? out[up + x - 1] : 0;
        p = a + b - c; pa = Math.abs(p - a); pb = Math.abs(p - b); pc = Math.abs(p - c);
        out[dst + x] = (raw[src + x] + (pa <= pb && pa <= pc ? a : pb <= pc ? b : c)) & 255;
      }
      else throw new Error('frame decode failed');
    }
    return out;
  }
  function decodePngGrey(buf, expect) {
    var p;
    try { p = parsePng(buf, expect); } catch (e) { return Promise.reject(e); }
    if (!p) return Promise.resolve(null);
    return inflate(p.z).then(function (raw) {
      if (raw.length !== (p.w + 1) * p.h) throw new Error('frame decode failed');
      return { q: unfilter(raw, p.w, p.h), cols: p.w, rows: p.h };
    });
  }
  var decodeCanvas = null;
  // PNG -> Uint8Array of codes. Direct inflate where the browser has DecompressionStream, else the
  // canvas path (needs CORS on the bucket for getImageData). `expect` = the manifest grid the frame
  // must have: a body that is too large or a picture of another size is refused before decoding.
  function decodeFrame(url, signal, expect) {
    var direct = typeof DecompressionStream === 'function' && typeof Response === 'function';
    return fetch(url, { signal: signal, mode: 'cors' }).then(function (r) {
      if (!r.ok) throw new Error('frame ' + r.status);
      var len = Number(r.headers.get('content-length') || 0);
      if (len > MAX_FRAME_BYTES) throw new Error('frame decode failed');
      return direct ? r.arrayBuffer() : r.blob();
    }).then(function (body) {
      var size = direct ? body.byteLength : body.size;
      if (size > MAX_FRAME_BYTES) throw new Error('frame decode failed');
      if (!direct) return decodeCanvas_(body, expect);
      return decodePngGrey(body, expect).then(function (frame) { return frame || decodeCanvas_(new Blob([body]), expect); });
    });
  }
  function decodeCanvas_(blob, expect) {
    return Promise.resolve(blob).then(function (blob) {
      if (typeof createImageBitmap === 'function') return createImageBitmap(blob, { premultiplyAlpha: 'none', colorSpaceConversion: 'none' });
      return new Promise(function (resolve, reject) {                  // older Safari: <img> from a same-origin blob URL
        var img = new Image(), u = URL.createObjectURL(blob);
        img.onload = function () { URL.revokeObjectURL(u); resolve(img); };
        img.onerror = function () { URL.revokeObjectURL(u); reject(new Error('frame decode failed')); };
        img.src = u;
      });
    }).then(function (bmp) {
      var w = bmp.width, h = bmp.height;                               // read BEFORE close(): a closed bitmap reports 0x0
      if (!w || !h) throw new Error('frame decoded to 0x0');
      if (expect && (w !== expect.cols || h !== expect.rows)) { if (bmp.close) bmp.close(); throw new Error('frame decode failed'); }
      var c = decodeCanvas || (decodeCanvas = document.createElement('canvas'));   // one scratch canvas, not one per frame
      if (c.width !== w || c.height !== h) { c.width = w; c.height = h; }
      var ctx = c.getContext('2d', { willReadFrequently: true });
      ctx.clearRect(0, 0, w, h);
      ctx.drawImage(bmp, 0, 0);
      var d = ctx.getImageData(0, 0, w, h).data;
      var q = new Uint8Array(w * h);
      for (var i = 0, j = 0; i < q.length; i++, j += 4) q[i] = d[j];
      if (bmp.close) bmp.close();
      return { q: q, cols: w, rows: h };
    });
  }

  // ---- the layer ----
  var ModelGridLayer = L.GridLayer.extend({
    initialize: function (opts) {
      L.GridLayer.prototype.initialize.call(this, { pane: 'modelPane', tileSize: TILE, updateWhenZooming: false,
        updateWhenIdle: true, keepBuffer: 1, opacity: opts.opacity, className: 'ov-tiles' });
      this._frame = null; this._grid = null; this._lut = null; this._nearest = false; this._lo = 0; this._hi = 1;
      this._legend = [0, 1]; this.field = null; this.fdef = null; this.entry = null;
      this._codes = new Float64Array(TILE * TILE); this._colPos = new Float64Array(TILE);
      this._one = new Float64Array(1); this._oneOut = new Float64Array(1);
      this._coast = null; this._clip = false;
    },
    // frame {q, cols, rows}; grid = the manifest grid for that resolution; fdef = the manifest field
    setFrame: function (frame, grid, fieldName, fdef, lut, entry) {
      validateGrid(grid, frame, fdef);
      this._frame = frame; this._grid = grid; this._lut = lut; this.field = fieldName; this.fdef = fdef; this.entry = entry || null;
      this._nearest = fdef.interpolation === 'nearest'; this._lo = fdef.lo; this._hi = fdef.hi; this._legend = fdef.legend;
      this._clip = !!(this._coast && CLIP_FIELDS[fieldName]);
      this._redraw();
    },
    clear: function () { this._frame = null; this.field = null; this.fdef = null; this.entry = null; this._clip = false; this._redraw(); },
    hasFrame: function () { return !!this._frame; },
    // A loaded CoastStore (or null): from now on hs/tp tiles are clipped to its polygons.
    setCoast: function (store) {
      var self = this;
      this._coast = store && store.status === 'ok' ? store : null;
      this._clip = !!(this._coast && CLIP_FIELDS[this.field]);
      if (this._coast) this._coast.onChange = function () { self._redrawIncomplete(); };
      this._redraw();
    },
    _redraw: function () { for (var k in this._tiles) { var t = this._tiles[k]; if (t.el && t.coords) this._draw(t.el, t.coords); } },
    // A tier-1 chunk landed (or failed): only the tiles drawn from the tier-0 stand-in are redrawn.
    _redrawIncomplete: function () {
      for (var k in this._tiles) { var t = this._tiles[k]; if (t.el && t.coords && t.el._ovLandFinal === false) this._draw(t.el, t.coords); }
    },
    // The tile's land alpha, computed once per tile element (per zoom) and kept on it like _ovImg.
    _landFor: function (el, coords) {
      var st = this._coast, key = coords.z + '/' + tileBox(coords).xw + '/' + coords.y;
      if (el._ovLandKey === key && (el._ovLandFinal || el._ovLandRev === st.rev)) return el._ovLand;
      var got = st.setsFor(coords);
      el._ovLand = got.sets.length ? maskState(rasterise(landPathsForTile(coords, got.sets), TILE)) : null;
      el._ovLandKey = key; el._ovLandFinal = got.complete; el._ovLandRev = st.rev;
      return el._ovLand;
    },
    // The readout's value for the drawn pixel that contains (lat, lng) at tile zoom z: null where the
    // tile shows nothing (no data, land, or a tile that is not on the map yet).
    readoutAt: function (lat, lng, z) {
      var p = pixelOf(lat, lng, z), ll = tilePixelLatLng(p, p.px, p.py), v = this.valueAt(ll.lat, ll.lng);
      if (v === null || !this._clip) return v;
      var t = this._tiles[p.x + ':' + p.y + ':' + p.z], land = t && t.el ? t.el._ovLand : undefined;
      if (land === undefined || land === LAND_ALL) return null;
      return land === null || 255 - land[p.py * TILE + p.px] >= LAND_READOUT ? v : null;
    },
    createTile: function (coords, done) {
      var el = document.createElement('canvas'); el.width = TILE; el.height = TILE;
      this._draw(el, coords);
      setTimeout(function () { done(null, el); }, 0);
      return el;
    },
    _value: function (code) { return this._lo + (code - 1) / 254 * (this._hi - this._lo); },
    // THE sampler: codes along one fractional grid row r (0..rows-1, caller-checked) at the periodic
    // column positions colPos[0..n) (each 0 <= cpos < cols), written to out[off..off+n). Bilinear over
    // the present neighbours (q 0 = absent; mostly absent -> 0), nearest for discontinuous fields (Tp).
    // 0 means "no value". Both the tile drawing and the readout go through here.
    _codeRow: function (r, colPos, n, out, off) {
      var f = this._frame, cols = f.cols, q = f.q, i;
      if (this._nearest) {
        var base = Math.round(r) * cols;
        for (i = 0; i < n; i++) out[off + i] = q[base + (Math.round(colPos[i]) % cols)];
        return;
      }
      var r0 = Math.floor(r), r1 = r0 + 1 < f.rows ? r0 + 1 : r0, fr = r - r0, w0 = 1 - fr, b0 = r0 * cols, b1 = r1 * cols;
      for (i = 0; i < n; i++) {
        var cpos = colPos[i], c0 = Math.floor(cpos), c1 = c0 + 1 === cols ? 0 : c0 + 1, fc = cpos - c0;
        var a = q[b0 + c0], b = q[b0 + c1], c = q[b1 + c0], d = q[b1 + c1];
        var w = 0, acc = 0, wt;
        if (a) { wt = w0 * (1 - fc); acc += a * wt; w += wt; }
        if (b) { wt = w0 * fc; acc += b * wt; w += wt; }
        if (c) { wt = fr * (1 - fc); acc += c * wt; w += wt; }
        if (d) { wt = fr * fc; acc += d * wt; w += wt; }
        out[off + i] = w < 0.25 ? 0 : acc / w;
      }
    },
    _code: function (r, cpos) { this._one[0] = cpos; this._codeRow(r, this._one, 1, this._oneOut, 0); return this._oneOut[0]; },
    valueAt: function (lat, lon) {
      if (!this._frame) return null;
      var g = this._grid, f = this._frame, r = (g.lat0 - lat) / -g.dlat;
      if (!(r >= 0 && r <= f.rows - 1)) return null;
      var c = (lon - g.lon0) / g.dlon, cpos = ((c % f.cols) + f.cols) % f.cols;
      var code = this._code(r, cpos);
      return code ? this._value(code) : null;
    },
    // Codes for one Web-Mercator tile (pixel centres) into out[TILE*TILE]; 0 = transparent. Column
    // positions are periodic, so wrapped and unwrapped tile x (world copies) sample identically.
    tileCodes: function (coords, out) {
      var f = this._frame, g = this._grid, cols = f.cols, rows = f.rows;
      var n = TILE * Math.pow(2, coords.z), x0 = coords.x * TILE, y0 = coords.y * TILE, colPos = this._colPos;
      for (var px = 0; px < TILE; px++) {
        var c = ((x0 + px + 0.5) / n * 360 - 180 - g.lon0) / g.dlon;
        colPos[px] = ((c % cols) + cols) % cols;
      }
      var k = 0;
      for (var py = 0; py < TILE; py++) {
        var lat = Math.atan(Math.sinh(Math.PI - 2 * Math.PI * (y0 + py + 0.5) / n)) * 180 / Math.PI;
        var r = (g.lat0 - lat) / -g.dlat;
        if (r >= 0 && r <= rows - 1) this._codeRow(r, colPos, TILE, out, k); else out.fill(0, k, k + TILE);
        k += TILE;
      }
      return out;
    },
    _draw: function (el, coords) {
      var ctx = el.getContext('2d');
      if (!this._frame) { ctx.clearRect(0, 0, TILE, TILE); return; }
      var land = this._clip ? this._landFor(el, coords) : null;
      if (land === LAND_ALL) { ctx.clearRect(0, 0, TILE, TILE); return; }         // nothing but land: no sampling at all
      var codes = this.tileCodes(coords, this._codes);
      var img = el._ovImg || (el._ovImg = ctx.createImageData(TILE, TILE));     // reused per tile: no 256 KB per frame
      composeTile(codes, land, this._lut, this._lo, this._hi, this._legend[0], this._legend[1], img.data);
      ctx.putImageData(img, 0, 0);
    }
  });

  // ---- playback helpers (pure; tested in Node) ----
  var SPEEDS = [0.5, 1, 2, 4], BASE_FPS = 2;                 // 1x = 2 frames/s (owner default)
  var RING_AHEAD = 2, RING_BEHIND = 2, MAX_DECODED = 5, MAX_INFLIGHT = 2;
  var RUN_CHECK_MS = 30 * 60 * 1000;                         // newer run -> banner only, never an automatic switch
  var RETRY_AFTER_MS = 60 * 1000;                            // a transient fetch failure keeps a frame out for this long
  var MAX_TRANSIENT = 3;                                     // consecutive transient failures -> "unavailable" + Retry
  var STALL_MS = 5000;                                       // a frame download slower than this (or 4 intervals) is skipped
  // A frame that does not exist (or cannot be decoded) is gone for the session; anything else (network
  // error, 403 from a bot check, 5xx, browser out of resources) is retried after a cooldown.
  function failureKind(err) {
    var msg = String(err && err.message || ''), m = /^frame (\d{3})$/.exec(msg);
    if (m) return (m[1] === '404' || m[1] === '410') ? 'permanent' : 'transient';
    return /decode/.test(msg) ? 'permanent' : 'transient';
  }
  function abortError() { var e = new Error('aborted'); e.name = 'AbortError'; return e; }
  // Indices worth having decoded around i: i itself, then ahead in the play direction, then behind (wrapping).
  function ringPlan(i, n, dir) {
    var out = [i], seen = {}, uniq = [], k;
    for (k = 1; k <= RING_AHEAD; k++) out.push(((i + k * dir) % n + n) % n);
    for (k = 1; k <= RING_BEHIND; k++) out.push(((i - k * dir) % n + n) % n);
    for (k = 0; k < out.length; k++) if (!seen[out[k]]) { seen[out[k]] = true; uniq.push(out[k]); }
    return uniq;
  }
  // Next index from i in direction dir that is not unavailable (wrapping, never i itself); null when
  // nothing else is left.
  function nextAvailable(i, dir, n, unavailable) {
    for (var k = 1; k < n; k++) { var j = ((i + k * dir) % n + n) % n; if (!unavailable(j)) return j; }
    return null;
  }
  // The frame whose valid time is nearest to t (ms since the epoch).
  function nearestIndex(m, t) {
    var idx = 0, best = Infinity;
    for (var i = 0; i < m.frames.length; i++) { var d = Math.abs(Date.parse(m.frames[i].valid_utc) - t); if (d < best) { best = d; idx = i; } }
    return idx;
  }
  // Decoded frames, least recently used first out; the frame on the map is never evicted.
  function FrameCache(max) { this.max = max; this.map = new Map(); }
  FrameCache.prototype.get = function (key) { var v = this.map.get(key); if (v) { this.map.delete(key); this.map.set(key, v); } return v || null; };
  FrameCache.prototype.has = function (key) { return this.map.has(key); };
  FrameCache.prototype.clear = function () { this.map.clear(); };
  FrameCache.prototype.size = function () { return this.map.size; };
  FrameCache.prototype.set = function (key, frame, keep) {
    this.map.delete(key); this.map.set(key, frame);
    if (this.map.size <= this.max) return;
    var keys = Array.from(this.map.keys());
    for (var i = 0; i < keys.length && this.map.size > this.max; i++) if (keys[i] !== key && keys[i] !== keep) this.map.delete(keys[i]);
  };

  // ---- controller ----
  function Overlay(map, opts) {
    this.map = map; this.opts = opts;
    // Keys inside latest.json / the manifest are bucket-absolute (gfswave/0p25/v1/...); the
    // configured base is the v1 prefix URL, so resolve keys against the bucket root.
    this.base = String(opts.base).replace(/\/+$/, '');
    this.root = this.base.replace(/\/gfswave\/0p25\/v1$/, '');
    this.field = null; this.layer = null; this.manifest = null; this.pointer = null; this.pointerAt = 0; this.newerRun = null;
    this.abort = null; this.readout = null; this.frameIndex = null; this.res = null; this.sheet = null; this.last = null;
    this._listeners = []; this._attributed = false; this.collapsed = undefined; this._touch = null; this._onVis = null;
    this.cache = new FrameCache(MAX_DECODED); this.inflight = {}; this.unavailable = {}; this.target = null; this.dir = 1; this.n = 0;
    this.playing = false; this.wasPlaying = false; this.timer = null; this.runTimer = null; this.ui = null; this._lutFor = null;
    this.playGen = 0; this.transientFails = 0; this._staleShown = false;
    // opts.coast: clip wave height / peak period to the coastlines published beside the frames
    this.coast = opts.coast ? coastStore(this.root + '/static/coast/v1') : null;
    var s = saved();
    this.opacity = typeof s.opacity === 'number' && s.opacity >= 0.2 && s.opacity <= 1 ? s.opacity : 0.65;
    this.speed = SPEEDS.indexOf(s.speed) >= 0 ? s.speed : 1;
  }
  Overlay.prototype.mount = function (fieldName) {
    var self = this;
    if (!this.map.getPane('modelPane')) {
      var pane = this.map.createPane('modelPane'); pane.style.zIndex = 250; pane.style.pointerEvents = 'none';
    }
    this.field = fieldName;
    this.abortAll();
    this.abort = new AbortController();
    var sig = this.abort.signal;
    if (!this.layer) {
      this.layer = new ModelGridLayer({ opacity: this.opacity }).addTo(this.map);
      this._bindReadout();
      this._bindMap();
      this._bindDocument();
    } else if (this.layer.field !== fieldName) {
      this.layer.clear();                                  // never one field's picture under another's label
    }
    this.render({ state: 'loading' });
    // The time position survives a field change and even a run change (nearest valid time), like Update.
    var prevValid = this.manifest && this.frameIndex !== null ? Date.parse(this.manifest.frames[this.frameIndex].valid_utc) : null;
    var prevRun = this.manifest ? this.manifest.run : null;
    // The coastlines load beside the pointer/manifest/frame; the FIRST DRAW waits for them (a filled
    // field spilling onto land and then snapping back is the wrong picture), later draws never do.
    var coastP = this.coast && CLIP_FIELDS[fieldName] ? this.coast.load() : Promise.resolve(null);
    this._loadManifest(sig).then(function (m) {
      if (!m.fields[fieldName] || !RAMPS[fieldName]) throw new Error('layer "' + fieldName + '" is not in this run');
      return coastP.then(function (store) { return [m, store]; });
    }).then(function (ms) {
      var m = ms[0], store = ms[1];
      if (sig.aborted || !self.layer) throw abortError();
      if (self.layer._coast !== (store || null)) self.layer.setCoast(store);
      self.n = m.frames.length;
      var idx = prevValid === null ? pickFrame(m) : m.run === prevRun ? Math.min(self.frameIndex, self.n - 1) : nearestIndex(m, prevValid);
      self.res = wantHalf(self.map.getZoom(), self._dims().w, fieldName) ? 'half' : 'full';
      if (!self.runTimer) self.runTimer = setInterval(function () { self._checkRun(); }, RUN_CHECK_MS);
      return self._goto(idx, sig).catch(function (err) {
        // the chosen first frame is missing: show the next one that exists rather than nothing
        if (!(err && err.unavailable) || (sig && sig.aborted)) throw err;
        var next = nextAvailable(idx, 1, self.n, function (j) { return self._isUnavailable(j); });
        if (next === null) throw err;
        return self._goto(next, sig);
      });
    }).catch(function (err) { self._fail(sig, err); });
  };
  // Cache / in-flight / unavailable keys carry the run: a decode that outlives an Update can never be
  // taken for a frame of the new run.
  Overlay.prototype._key = function (idx) { return this.manifest.run + '/' + this.res + '/' + this.field + '/' + this.manifest.frames[idx].step; };
  // unavailable[key] is true (permanent) or a timestamp until which the frame is left alone (transient).
  Overlay.prototype._isUnavailable = function (idx) {
    var u = this.unavailable[this._key(idx)];
    return u === true || (typeof u === 'number' && u > Date.now());
  };
  Overlay.prototype._lut = function () {
    if (this._lutFor !== this.field) { this._lutCache = buildRamp(RAMPS[this.field]); this._lutFor = this.field; }
    return this._lutCache;
  };
  // The decoded frame for index idx: cache, then an in-flight fetch, then a new one. A 404 or a decode
  // failure marks the frame unavailable for this session (err.unavailable); an abort does not.
  Overlay.prototype._ensure = function (idx) {
    var self = this, key = this._key(idx), hit = this.cache.get(key);
    if (hit) return Promise.resolve(hit);
    if (this.inflight[key]) return this.inflight[key].promise;
    if (this._isUnavailable(idx)) { var e = new Error('frame unavailable'); e.unavailable = true; return Promise.reject(e); }
    delete this.unavailable[key];                                            // an expired cooldown: try again
    var m = this.manifest, ctrl = new AbortController(), rec, half = this.res === 'half', stalled = false;
    var url = this.root + '/' + frameKey(m, m.frames[idx], this.field, half);
    function mine() { return self.inflight[key] === rec; }
    // a download that stalls (one frame in a loop took 6.5 s at G4) is dropped like a transient failure
    var watchdog = setTimeout(function () { stalled = true; ctrl.abort(); }, Math.max(STALL_MS, 4 * self._interval()));
    var p = decodeFrame(url, ctrl.signal, half ? m.grid_half : m.grid).then(function (frame) {
      clearTimeout(watchdog);
      if (mine()) delete self.inflight[key];
      // A decode cannot be cancelled: one that outlives its abort (Update, Off, field or resolution
      // change) must neither be cached nor delivered.
      if ((ctrl.signal.aborted && !stalled) || self.manifest !== m) throw abortError();
      self.transientFails = 0;
      self.cache.set(key, frame, self.frameIndex !== null ? self._key(self.frameIndex) : null);
      return frame;
    }, function (err) {
      clearTimeout(watchdog);
      if (mine()) delete self.inflight[key];
      if ((ctrl.signal.aborted && !stalled) || self.manifest !== m) throw abortError();
      if (stalled) err = new Error('frame stalled');
      var kind = failureKind(err);
      self.unavailable[key] = kind === 'permanent' ? true : Date.now() + RETRY_AFTER_MS;
      err.unavailable = true;
      if (kind === 'transient' && ++self.transientFails >= MAX_TRANSIENT) err.outage = true;   // the bucket, not one frame
      throw err;
    });
    rec = this.inflight[key] = { promise: p, abort: ctrl, key: key };
    return p;
  };
  // Keep the in-flight set to what the target needs: everything outside the new ring is dropped, and
  // at most MAX_INFLIGHT - 1 older fetches survive beside the target (timeline drags fire many seeks).
  Overlay.prototype._trimInflight = function (idx) {
    var keep = {}, plan = ringPlan(idx, this.n, this.dir), i, k;
    for (i = 0; i < plan.length; i++) keep[this._key(plan[i])] = true;
    var target = this._key(idx), survivors = [];
    for (k in this.inflight) if (!keep[k]) { this.inflight[k].abort.abort(); delete this.inflight[k]; } else if (k !== target) survivors.push(k);
    while (survivors.length > MAX_INFLIGHT - 1) { k = survivors.shift(); this.inflight[k].abort.abort(); delete this.inflight[k]; }
  };
  // Show frame idx: the label/timeline move to the target at once, the picture and the valid time only
  // when the frame has landed (never an old picture under a new time). Rejects for an unavailable frame
  // (err.unavailable) or an aborted load (AbortError).
  Overlay.prototype._goto = function (idx, sig) {
    var self = this, m = this.manifest, field = this.field, res = this.res;
    this.target = idx; this._syncUI();
    this._trimInflight(idx);
    return this._ensure(idx).then(function (frame) {
      if ((sig && sig.aborted) || self.target !== idx || !self.layer || self.manifest !== m || self.field !== field || self.res !== res) throw abortError();
      // the frame already on the map (Retry after an outage, a repeated seek): no redraw, but the same
      // state transition as a fresh landing, or the panel would stay on "Loading"
      if (self.layer._frame !== frame) {
        var half = res === 'half';
        self.layer.setFrame(frame, half ? m.grid_half : m.grid, field, m.fields[field], self._lut(), m.frames[idx]);
      }
      self.frameIndex = idx;
      self._attribute();
      if (!self.last || self.last.state !== 'ready') self.render({ state: 'ready' }); else self._syncUI();
      self._prefetch();
    });
  };
  // Keep the ring (current, two ahead, two behind) decoded with at most MAX_INFLIGHT fetches; drop
  // fetches the ring no longer wants (direction, field or resolution changed).
  Overlay.prototype._prefetch = function () {
    if (this.frameIndex === null || !this.manifest) return;
    var plan = ringPlan(this.frameIndex, this.n, this.dir), want = {}, i, k;
    for (i = 0; i < plan.length; i++) want[this._key(plan[i])] = true;
    for (k in this.inflight) if (!want[k]) { this.inflight[k].abort.abort(); delete this.inflight[k]; }
    for (i = 1; i < plan.length && Object.keys(this.inflight).length < MAX_INFLIGHT; i++) {
      var key = this._key(plan[i]);
      if (!this.cache.has(key) && !this.inflight[key] && !this._isUnavailable(plan[i])) this._ensure(plan[i]).catch(function () {});
    }
  };
  Overlay.prototype._fail = function (sig, err) {
    if (sig && sig.aborted) return;
    if (err && err.name === 'AbortError') return;
    this.pause();
    var msg = err && err.outage ? 'frames cannot be loaded right now' : err && err.message ? err.message : String(err);
    try { this.render({ state: 'error', message: msg }); } catch (e) { /* host gone */ }
  };
  // latest.json -> manifest. A pointer older than POINTER_RECHECK_MS is re-read; while a frame is on
  // the map the session stays pinned to its run (a newer one is only announced), otherwise it adopts it.
  Overlay.prototype._loadManifest = function (sig) {
    var self = this;
    if (this.manifest && Date.now() - this.pointerAt < POINTER_RECHECK_MS) return Promise.resolve(this.manifest);
    var cached = this.manifest;
    return fetch(this.base + '/latest.json', { signal: sig, cache: 'no-cache' }).then(function (r) {
      if (!r.ok) throw new Error('latest ' + r.status);
      return r.json();
    }).catch(function (err) {
      // A failed re-read must not take away a validated run the session already holds: keep it and back off.
      if (cached && !(sig && sig.aborted)) { self.pointerAt = Date.now(); return null; }
      throw err;
    }).then(function (ptr) {
      if (ptr === null) return cached;
      if (!ptr || !ptr.complete || !ptr.run || !ptr.manifest || isNaN(Date.parse(ptr.published_utc))) throw new Error('published run is not complete');
      self.pointerAt = Date.now();
      if (self.manifest && self.manifest.run === ptr.run) { self.pointer = ptr; return self.manifest; }
      if (self.manifest && self.frameIndex !== null) { self.newerRun = ptr; return self.manifest; }   // a time position is pinned: banner only
      return self._fetchManifest(ptr, sig).then(function (m) { self._adopt(m, ptr); return m; });
    });
  };
  Overlay.prototype._loadManifest = (function (inner) {
    // A run announced by the banner is adopted on the next mount after Off (no time position is pinned
    // any more); a field change while On keeps the pinned run and the banner.
    return function (sig) {
      var self = this;
      if (this.newerRun && this.manifest && this.frameIndex === null) {
        var ptr = this.newerRun;
        return this._fetchManifest(ptr, sig).then(function (m) { self._adopt(m, ptr); self.pointerAt = Date.now(); return m; });
      }
      return inner.call(this, sig);
    };
  })(Overlay.prototype._loadManifest);
  Overlay.prototype._fetchManifest = function (ptr, sig) {
    return fetch(this.root + '/' + ptr.manifest, { signal: sig }).then(function (r) {
      if (!r.ok) throw new Error('manifest ' + r.status);
      return r.json();
    }).then(function (m) {
      validateManifest(m);
      if (m.run !== ptr.run) throw new Error('manifest/pointer run mismatch');
      return m;
    });
  };
  Overlay.prototype._adopt = function (m, ptr) {
    this.manifest = m; this.pointer = ptr; this.newerRun = null; this.n = m.frames.length;
    this.cache.clear(); this.unavailable = {}; this.frameIndex = null; this.target = null;
  };
  // Every RUN_CHECK_MS while mounted: a newer complete run only raises the "Update" banner.
  Overlay.prototype._checkRun = function () {
    var self = this;
    if (!this.manifest) return;
    fetch(this.base + '/latest.json', { cache: 'no-cache' }).then(function (r) { return r.ok ? r.json() : null; }).then(function (ptr) {
      if (!ptr || !ptr.complete || !ptr.run || !ptr.manifest || isNaN(Date.parse(ptr.published_utc)) || !self.manifest) return;
      self.pointerAt = Date.now();
      var rerender = false;
      if (ptr.run > self.manifest.run) { self.newerRun = ptr; rerender = true; }
      else if (ptr.run === self.manifest.run) self.pointer = ptr;
      if (self._isStale() !== self._staleShown) rerender = true;              // the 9 h banner appears without a user action
      if (rerender && self.last && self.last.state === 'ready') self.render(self.last);
    }).catch(function () {});
  };
  Overlay.prototype._isStale = function () { return !!this.pointer && (Date.now() - Date.parse(this.pointer.published_utc)) / 1000 > STALE_AFTER_S; };
  // "Update" on the banner: switch to the announced run at the nearest valid time, keep the play state.
  Overlay.prototype.update = function () {
    var self = this, ptr = this.newerRun;
    if (!ptr || !this.field) return;
    var wasPlaying = this.playing; this.pause();
    var prevValid = this.frameIndex !== null ? Date.parse(this.manifest.frames[this.frameIndex].valid_utc) : Date.now();
    this.abortAll(); this.abort = new AbortController();
    var sig = this.abort.signal;
    this.render({ state: 'loading' });
    this._fetchManifest(ptr, sig).then(function (m) {
      if (!m.fields[self.field]) throw new Error('layer "' + self.field + '" is not in run ' + m.run);
      self._adopt(m, ptr); self.pointerAt = Date.now();
      return self._goto(nearestIndex(m, prevValid), sig).then(function () { if (wasPlaying) self.play(); });
    }).catch(function (err) { self._fail(sig, err); });
  };

  // ---- playback ----
  Overlay.prototype._interval = function () { return 1000 / (BASE_FPS * this.speed); };
  // One tick chain at a time: play() starts a generation; a continuation from an older generation
  // (a frame that was loading when the user paused and played again) never schedules anything.
  Overlay.prototype.play = function () {
    if (this.playing || !this.manifest || this.frameIndex === null) return;
    this.playing = true; this.dir = 1; this.playGen++; this._syncUI(); this._tick(this.playGen);
  };
  Overlay.prototype.pause = function () {
    this.playing = false; this.playGen++;
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    this._syncUI();
  };
  Overlay.prototype._tick = function (gen) {
    var self = this;
    if (!this.playing || gen !== this.playGen) return;
    var un = function (j) { return self._isUnavailable(j); };
    var pending = this.target !== null && this.target !== this.frameIndex;
    var next = pending && !un(this.target) ? this.target                      // a seek or a slow frame: wait for it
      : nextAvailable(pending ? this.target : this.frameIndex, 1, this.n, un);
    if (next === null) { this.pause(); return; }
    var t0 = Date.now();
    this._goto(next).then(function () { self._after(t0, gen); }, function (err) {
      if (err && err.outage) self._fail(null, err);
      else if (err && (err.unavailable || err.name === 'AbortError')) self._after(t0, gen);   // skipped or superseded: move on
      else self._fail(null, err);
    });
  };
  Overlay.prototype._after = function (t0, gen) {
    var self = this;
    if (!this.playing || gen !== this.playGen) return;
    if (this.timer) clearTimeout(this.timer);
    var wait = Math.max(0, this._interval() - (Date.now() - t0));
    this.timer = setTimeout(function () { self.timer = null; self._tick(gen); }, wait);
  };
  Overlay.prototype.step = function (dir) {
    var self = this;
    this.pause();
    if (!this.manifest || this.frameIndex === null) return;
    var base = this.target !== null ? this.target : this.frameIndex;
    var next = nextAvailable(base, dir, this.n, function (j) { return self._isUnavailable(j); });
    if (next !== null) { this.dir = dir; this._goto(next).catch(function (err) { self._afterMiss(err); }); }
  };
  Overlay.prototype.seek = function (idx) {
    var self = this;
    if (!this.manifest || this.frameIndex === null) return;
    idx = Math.max(0, Math.min(this.n - 1, idx | 0));
    this._goto(idx).catch(function (err) { self._afterMiss(err); });        // unavailable: the drawn frame stays, the hint says so
  };
  // A seek or step that did not land: an outage (3 transient failures in a row) shows the Retry state
  // like playback does; a single missing frame just leaves the hint in the valid-time line.
  Overlay.prototype._afterMiss = function (err) { if (err && err.outage) this._fail(null, err); else this._syncUI(); };
  Overlay.prototype.setSpeed = function (v) { if (SPEEDS.indexOf(v) < 0) return; this.speed = v; save({ speed: v }); this._syncUI(); };
  Overlay.prototype._bindDocument = function () {
    var self = this;
    this._onVis = function () {
      if (document.hidden) { self.wasPlaying = self.playing; if (self.playing) self.pause(); }
      else if (self.wasPlaying) { self.wasPlaying = false; self.play(); }
    };
    document.addEventListener('visibilitychange', this._onVis);
  };

  // ---- attribution, map events, resolution ----
  // The container's real size: the site resizes #map by script and Leaflet's cached getSize() can lag.
  Overlay.prototype._dims = function () {
    var c = this.map.getContainer(), s = this.map.getSize();
    return { w: c.clientWidth || s.x, h: c.clientHeight || s.y };
  };
  Overlay.prototype._attribute = function () {
    if (this._attributed) return;
    this._attributed = true;
    if (this.map.attributionControl) this.map.attributionControl.addAttribution(ATTRIBUTION);
    this.map.getContainer().classList.add('ov-on');
    this._sizeAttribution();
  };
  // While On the attribution may wrap, but only inside its own box: never over the zoom control.
  Overlay.prototype._sizeAttribution = function () {
    if (this._attributed) this.map.getContainer().style.setProperty('--ov-attr-max', Math.max(120, this._dims().w - 60) + 'px');
  };
  Overlay.prototype._unattribute = function () {
    if (!this._attributed) return;
    this._attributed = false;
    if (this.map.attributionControl) this.map.attributionControl.removeAttribution(ATTRIBUTION);
    var c = this.map.getContainer(); c.classList.remove('ov-on'); c.style.removeProperty('--ov-attr-max');
  };
  Overlay.prototype._on = function (ev, fn) { this.map.on(ev, fn); this._listeners.push([ev, fn]); };
  Overlay.prototype._bindMap = function () {
    var self = this;
    this._on('zoomend', function () { self._checkRes(); });
    this._on('resize', function () {
      self._sizeAttribution(); self._checkRes();
      if (!self.last) return;
      // the panel is rebuilt only when it has to move between the control and the sheet
      var compactNow = self.isCompact(), wasCompact = !!self.sheet;
      if (compactNow !== wasCompact) self.render(self.last); else { self._syncUI(); self._layoutSheet(); }
    });
  };
  Overlay.prototype._checkRes = function () {
    if (!this.layer || !this.layer.hasFrame() || !this.manifest || this.frameIndex === null || !this.field) return;
    var z = this.map.getZoom(), w = this._dims().w, want = this.res;
    if (this.res === 'full' && wantHalf(z, w, this.field)) want = 'half';
    else if (this.res === 'half' && wantFull(z, w, this.field)) want = 'full';
    if (want === this.res) return;
    this.res = want;
    var self = this, idx = this.target !== null ? this.target : this.frameIndex;
    this._prefetch();                                                        // drops the other resolution's fetches
    this._goto(idx).catch(function () { self._syncUI(); });
  };
  Overlay.prototype.abortAll = function () {
    if (this.abort) { this.abort.abort(); this.abort = null; }
    for (var k in this.inflight) this.inflight[k].abort.abort();
    this.inflight = {};
  };
  Overlay.prototype.unmount = function () {
    var self = this;
    this.pause();
    this.abortAll();
    if (this.runTimer) { clearInterval(this.runTimer); this.runTimer = null; }
    if (this._onVis) { document.removeEventListener('visibilitychange', this._onVis); this._onVis = null; }
    if (this.layer) { this.map.removeLayer(this.layer); this.layer = null; }
    if (this.coast) this.coast.abortAll();                 // the decoded coastlines stay for the next On
    this._unattribute();
    this._unbindReadout();
    this._listeners.forEach(function (l) { self.map.off(l[0], l[1]); }); this._listeners = [];
    this._removeSheet();
    this.cache.clear(); this.unavailable = {}; this.target = null; this.wasPlaying = false; this.ui = null;
    this.field = null; this.frameIndex = null; this.res = null; this.last = null; this.collapsed = undefined;
    clear(this.opts.panel);
  };
  Overlay.prototype.setOpacity = function (v) {
    v = Math.max(0.2, Math.min(1, v));
    this.opacity = v; save({ opacity: v });
    if (this.layer) this.layer.setOpacity(v);
  };

  // Readout: hover only on hover-capable pointers, long-press (450 ms) on touch; a plain tap shows
  // nothing. Marker events are unaffected (the pane has no pointer events); controls are ignored.
  Overlay.prototype._bindReadout = function () {
    var self = this, map = this.map, el = mk('div', 'ov-readout'); el.hidden = true;
    map.getContainer().appendChild(el); this.readout = el;
    var pressTimer = null, lastTouch = 0;
    function overControl(ev) { var t = ev && ev.target; return !!(t && t.closest && t.closest('.leaflet-control, .ov-sheet')); }
    function show(latlng, pt) {
      var layer = self.layer, v = null;
      if (layer && layer.fdef) {
        var z = typeof layer._tileZoom === 'number' ? layer._tileZoom : Math.round(map.getZoom());
        v = layer.readoutAt(latlng.lat, latlng.lng, z);            // the drawn pixel (its centre, its land mask), not the raw cursor point
      }
      if (v === null || v === undefined) { el.hidden = true; return; }
      var u = unitOf(layer.field, self.opts.getUnit()), f = layer.fdef;
      var txt = u.f(v).toFixed(u.d) + ' ' + u.label;
      if (v <= f.lo + 1e-9 && f.lo > 0) txt = '≤ ' + txt; else if (v >= f.hi - 1e-9) txt = '≥ ' + txt;   // "<=" only where values below lo exist (Tp)
      el.textContent = txt; el.style.left = pt.x + 'px'; el.style.top = pt.y + 'px'; el.hidden = false;
    }
    var hoverable = !window.matchMedia || window.matchMedia('(hover: hover) and (pointer: fine)').matches;
    if (hoverable) {
      this._on('mousemove', function (e) {
        var oe = e.originalEvent;
        if (!oe || oe.pointerType === 'touch' || Date.now() - lastTouch < 1000 || overControl(oe)) { el.hidden = true; return; }
        show(e.latlng, e.containerPoint);
      });
      this._on('mouseout', function () { el.hidden = true; });
    }
    this._on('movestart', function () { el.hidden = true; if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; } });
    var c = map.getContainer();
    function ts(e) {
      lastTouch = Date.now();
      if (e.touches.length !== 1 || overControl(e)) return;
      var t = e.touches[0];
      pressTimer = setTimeout(function () { pressTimer = null; var pt = map.mouseEventToContainerPoint(t); show(map.containerPointToLatLng(pt), pt); }, 450);
    }
    function te() { lastTouch = Date.now(); if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; } setTimeout(function () { el.hidden = true; }, 1500); }
    c.addEventListener('touchstart', ts, { passive: true }); c.addEventListener('touchend', te, { passive: true }); c.addEventListener('touchmove', te, { passive: true });
    this._touch = [c, ts, te, function () { if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; } }];
  };
  Overlay.prototype._unbindReadout = function () {
    if (this._touch) {
      var c = this._touch[0];
      c.removeEventListener('touchstart', this._touch[1]); c.removeEventListener('touchend', this._touch[2]); c.removeEventListener('touchmove', this._touch[2]);
      this._touch[3](); this._touch = null;
    }
    if (this.readout) { this.readout.remove(); this.readout = null; }
  };

  // ---- panel: inside the top-left control on desktops; a sheet on the map's bottom edge on phones ----
  // Compact = the site's mobile breakpoint, or a short map (desktop windows are often short too: the
  // site caps the map at 48 % of the viewport height).
  Overlay.prototype.isCompact = function () {
    var d = this._dims(), coarse = !!(window.matchMedia && window.matchMedia('(pointer: coarse)').matches);
    return d.w < 576 || d.h < 330 || (d.h < 400 && coarse);   // < 330 px: the top-left panel's details would be a slit above the zoom stack
  };
  Overlay.prototype._host = function () {
    if (this.isCompact()) {
      if (!this.sheet) {
        var sh = mk('div', 'ov-sheet'); sh.setAttribute('role', 'region'); sh.setAttribute('aria-label', 'Model overlay');
        L.DomEvent.disableClickPropagation(sh); L.DomEvent.disableScrollPropagation(sh);
        this.map.getContainer().appendChild(sh); this.sheet = sh;
      }
      clear(this.opts.panel);
      return this.sheet;
    }
    this._removeSheet();
    return this.opts.panel;
  };
  Overlay.prototype._removeSheet = function () {
    if (this.sheet) { this.sheet.remove(); this.sheet = null; }
    var c = this.map.getContainer(); c.classList.remove('ov-sheet-open');
    c.style.removeProperty('--ov-sheet-h'); c.style.removeProperty('--ov-sheet-left');
  };
  // The sheet sits on the bottom edge to the RIGHT of the zoom/Home column (that column is never
  // covered or moved); CSS lifts only the attribution corner above it by --ov-sheet-h.
  Overlay.prototype._layoutSheet = function () {
    if (!this.sheet) return;
    var c = this.map.getContainer(); c.classList.add('ov-sheet-open');
    c.style.setProperty('--ov-sheet-left', (this._stackWidth() + 6) + 'px');
    c.style.setProperty('--ov-sheet-h', this.sheet.offsetHeight + 'px');
  };
  Overlay.prototype._stackWidth = function () {
    var corners = this.map._controlCorners, el = corners && corners.bottomleft;
    return el && el.offsetWidth ? el.offsetWidth : 45;
  };
  Overlay.prototype._stackHeight = function () {
    var corners = this.map._controlCorners, el = corners && corners.bottomleft;
    return el && el.offsetHeight ? el.offsetHeight : 120;
  };
  Overlay.prototype.refresh = function () { if (this.readout) this.readout.hidden = true; if (this.last && this.layer) this.render(this.last); };
  Overlay.prototype._hours = function (entry) { return Math.round((Date.parse(entry.valid_utc) - Date.parse(this.manifest.run_utc)) / 3.6e6); };
  Overlay.prototype._validLocal = function (entry) { return this.opts.fmtTime(entry.valid_utc, this.opts.tz) + ' ' + this.opts.tzAbbr(entry.valid_utc, this.opts.tz); };
  function button(cls, text, label, onClick) {
    var b = mk('button', 'ov-btn ' + cls, text); b.type = 'button'; b.setAttribute('aria-label', label);
    b.addEventListener('click', onClick); return b;
  }
  // Builds the panel for a state; frame-by-frame changes only touch the live parts through _syncUI().
  Overlay.prototype.render = function (st) {
    this.last = st; this.ui = null;
    var self = this, host = this._host(), compact = host === this.sheet, m = this.manifest, unit = this.opts.getUnit();
    var mapH = this._dims().h;
    clear(host);
    host.setAttribute('aria-live', 'polite');
    if (st.state === 'loading') { host.appendChild(mk('div', 'ov-meta', 'Loading model frame…')); this._layoutSheet(); return; }
    if (st.state === 'error') {
      var e = mk('div', 'ov-err', 'Overlay unavailable: ' + st.message + ' ');
      e.appendChild(button('ov-retry', 'Retry', 'Retry loading the overlay', function () { if (self.field) { self.unavailable = {}; self.transientFails = 0; self.mount(self.field); } }));
      host.appendChild(e); this._layoutSheet(); return;
    }
    var field = this.layer.field, f = m.fields[field], fdesc = (m.model && m.model.fields && m.model.fields[field]) || {};
    var label = fdesc.label || field, modelName = String(m.model && m.model.name || 'NOAA GFS-Wave').split(' + ')[0];
    var cap = Math.floor(mapH * 0.4);
    if (this.collapsed === undefined) this.collapsed = compact;
    var collapsed = !!this.collapsed;
    var head = mk('div', 'ov-row ov-head');
    var btn = mk('button', 'ov-toggle', collapsed ? '▸' : '▾'); btn.type = 'button';
    btn.setAttribute('aria-label', collapsed ? 'Show overlay details' : 'Hide overlay details');
    btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true'); btn.setAttribute('aria-controls', 'ovDetails');
    btn.addEventListener('click', function () { self.collapsed = !self.collapsed; self.render(st); });
    head.appendChild(btn);
    var playHead = button('ov-play', '▶', 'Play', function () { if (self.playing) self.pause(); else self.play(); });
    head.appendChild(playHead);
    var title = mk('span', 'ov-title');
    head.appendChild(title);
    host.appendChild(head);
    var ui = this.ui = { title: title, play: [playHead], slider: null, valid: null, unavail: null, label: label, modelName: modelName, collapsed: collapsed, compact: compact };
    if (compact && cap - host.offsetHeight - 8 < 40) { head.removeChild(btn); collapsed = ui.collapsed = true; }
    if (collapsed) { this._syncUI(); this._layoutSheet(); return; }
    var body = mk('div', 'ov-details'); body.id = 'ovDetails'; body.style.maxHeight = cap + 'px'; host.appendChild(body);
    // transport + timeline
    var tr = mk('div', 'ov-row ov-transport');
    tr.appendChild(button('', '⏮', 'First frame', function () { self.pause(); self.seek(0); }));
    tr.appendChild(button('', '◀', 'Previous frame', function () { self.step(-1); }));
    var playMain = button('ov-play', '▶', 'Play', function () { if (self.playing) self.pause(); else self.play(); });
    ui.play.push(playMain); tr.appendChild(playMain);
    tr.appendChild(button('', '▶▶', 'Next frame', function () { self.step(1); }));
    tr.appendChild(button('', '⏭', 'Last frame', function () { self.pause(); self.seek(self.n - 1); }));
    var speed = mk('select', 'ov-speed'); speed.setAttribute('aria-label', 'Playback speed');
    SPEEDS.forEach(function (v) { var o = mk('option', null, v + '×'); o.value = String(v); speed.appendChild(o); });
    speed.value = String(this.speed);
    speed.addEventListener('change', function () { self.setSpeed(parseFloat(speed.value)); });
    tr.appendChild(speed);
    body.appendChild(tr);
    var slider = mk('input', 'ov-timeline'); slider.type = 'range'; slider.min = '0'; slider.max = String(this.n - 1); slider.step = '1';
    slider.setAttribute('aria-label', 'Forecast hour');
    slider.addEventListener('input', function () { self.seek(parseInt(slider.value, 10)); });
    body.appendChild(slider); ui.slider = slider;
    var valid = mk('div', 'ov-meta'); body.appendChild(valid); ui.valid = valid;
    var runLine = mk('div', 'ov-meta'); runLine.appendChild(mk('b', null, 'Run: '));
    var runLabel = m.run_utc.replace('T', ' ').replace(':00:00Z', 'Z'), pc = typeof this.opts.pageCycle === 'function' ? this.opts.pageCycle() : this.opts.pageCycle;
    runLine.appendChild(document.createTextNode(runLabel + ' (UTC), ' + this.n + ' frames to +' + this._hours(m.frames[this.n - 1]) + ' h' +
      (pc && pc.model === 'SWAN' ? ' — the forecast table is a PacIOOS SWAN run' : pc && pc.run && pc.run !== m.run ? ' — the forecast table is on run ' + pc.run : '')));
    body.appendChild(runLine);
    var age = (Date.now() - Date.parse(this.pointer.published_utc)) / 1000;
    this._staleShown = age > STALE_AFTER_S;
    if (this._staleShown) body.appendChild(mk('div', 'ov-warn', 'Model overlay data is stale (published ' + Math.round(age / 3600) + ' h ago).'));
    if (this.newerRun) {
      var banner = mk('div', 'ov-warn', 'A newer run (' + String(this.newerRun.run).replace(/^(\d{8})(\d{2})$/, '$1 $2Z') + ') is available. ');
      banner.appendChild(button('ov-update', 'Update', 'Switch to the newer run', function () { self.update(); }));
      body.appendChild(banner);
    }
    var unavail = mk('div', 'ov-warn'); unavail.hidden = true; body.appendChild(unavail); ui.unavail = unavail;
    var clipped = !!this.layer._clip;
    if (CLIP_FIELDS[field] && this.coast && !clipped) body.appendChild(mk('div', 'ov-warn', 'Coastline data could not be loaded; the field is shown without coastline clipping.'));
    // legend over the LEGEND range in the site's units (the encoding range is wider; extremes clamp)
    var leg = mk('div', 'ov-legend'), cv = mk('canvas'); cv.width = 256; cv.height = 1; leg.appendChild(cv);
    var lut = this._lut(), ctx = cv.getContext('2d'), im = ctx.createImageData(256, 1);
    for (var i = 0; i < 256; i++) { im.data[i * 4] = lut[i * 3]; im.data[i * 4 + 1] = lut[i * 3 + 1]; im.data[i * 4 + 2] = lut[i * 3 + 2]; im.data[i * 4 + 3] = 255; }
    ctx.putImageData(im, 0, 0);
    var ticks = mk('div', 'ov-ticks'), tk = legendTicks(field, f, unit);
    tk.forEach(function (t, k) {
      var s = mk('span', k === 0 ? 'ov-first' : k === tk.length - 1 ? 'ov-last' : '', t.label);
      s.style.left = (t.pos * 100).toFixed(2) + '%'; ticks.appendChild(s);
    });
    leg.appendChild(ticks); body.appendChild(leg);
    var row = mk('div', 'ov-row'), lab = mk('label', null, 'Opacity ');
    var rng = mk('input'); rng.type = 'range'; rng.min = '0.2'; rng.max = '1'; rng.step = '0.05'; rng.value = String(this.opacity);
    rng.setAttribute('aria-label', 'Overlay opacity');
    rng.addEventListener('input', function () { self.setOpacity(parseFloat(rng.value)); });
    lab.appendChild(rng); row.appendChild(lab); body.appendChild(row);
    body.appendChild(mk('div', 'ov-note',
      (field === 'tp' ? 'Peak period Tp (GRIB PERPW = 1/fp), nearest grid cell (no smoothing). ' : field === 'wind' ? 'GFS wind at 10 m over land and sea; legend top 60 kt; 0.5° frames below zoom 6. ' : '') +
      'GFS-Wave 0.25° (~28 km) grid — display smoothing is not extra detail. ' +
      (clipped ? (m.fill ? 'Nearshore values are extrapolated from the nearest model cells; ' : '') + 'coastlines from GSHHG (Wessel & Smith). Hover or long-press the map for values (none over land). '
        : 'Hover or long-press the map for values. ') +
      String(m.model && m.model.attribution || '')));
    this._syncUI();
    // Clamp from the real layout: on phones the WHOLE sheet <= cap; on desktops the details <= cap AND
    // the top-left control must end above the zoom/Home stack (short windows: the site caps the map at
    // 48 % of the viewport). Not enough room for a scroll box -> back to the one-line summary.
    var room;
    if (compact) {
      room = cap - (host.offsetHeight - body.offsetHeight) - 2;
    } else {
      var ctl = (host.closest && host.closest('.ov-ctl')) || host, chrome = ctl.offsetHeight - body.offsetHeight;
      room = Math.min(cap, mapH - this._stackHeight() - 10 - 10 - chrome - 8);
    }
    if (room < 40) {
      host.removeChild(body); head.removeChild(btn); this.collapsed = true; ui.collapsed = true;
      ui.slider = null; ui.valid = null; ui.unavail = null; ui.play = [playHead];
      this._syncUI();
    } else {
      body.style.maxHeight = room + 'px';
    }
    this._layoutSheet();
  };
  // The live parts: play/pause glyphs, the title, the valid-time line (from the DRAWN frame; a pending
  // target is announced as loading), the timeline thumb (at the target) and the unavailable-frame note.
  Overlay.prototype._syncUI = function () {
    var ui = this.ui, self = this;
    if (!ui || !this.manifest || !this.layer) return;
    var drawn = this.layer.entry, pending = this.target !== null && this.target !== this.frameIndex;
    var validLocal = drawn ? this._validLocal(drawn) : '…', hours = drawn ? this._hours(drawn) : null;
    var glyph = this.playing ? '❚❚' : '▶', name = this.playing ? 'Pause' : 'Play';
    ui.play.forEach(function (b) { b.textContent = glyph; b.setAttribute('aria-label', name); });
    ui.title.textContent = ui.collapsed ? ui.label + ' · ' + validLocal + (hours === null ? '' : ' (+' + hours + ' h)')
      : ui.compact ? ui.label : ui.label + ' — ' + ui.modelName;
    if (ui.valid) {
      clear(ui.valid);
      ui.valid.appendChild(mk('b', null, 'Valid: '));
      ui.valid.appendChild(document.createTextNode(validLocal + (hours === null ? '' : ' (+' + hours + ' h)')));
      if (pending) {
        var t = this.manifest.frames[this.target], hint = this._isUnavailable(this.target) ? ' — frame +' + this._hours(t) + ' h is unavailable' : ' — loading +' + this._hours(t) + ' h…';
        ui.valid.appendChild(mk('span', 'ov-hint', hint));
      }
    }
    if (ui.slider && document.activeElement !== ui.slider) ui.slider.value = String(this.target !== null ? this.target : this.frameIndex);
    if (ui.unavail) {
      var missing = [];
      for (var i = 0; i < this.n; i++) if (this._isUnavailable(i)) missing.push('+' + this._hours(this.manifest.frames[i]) + ' h');
      ui.unavail.hidden = missing.length === 0;
      ui.unavail.textContent = missing.length ? 'Unavailable frames are skipped: ' + missing.slice(0, 8).join(', ') +
        (missing.length > 8 ? ' and ' + (missing.length - 8) + ' more' : '') : '';
    }
  };

  window.AllshoreOverlay = {
    create: function (map, opts) { var o = new Overlay(map, opts); window.AllshoreOverlay._last = o; return o; },   // _last: debugging handle
    _internals: { buildRamp: buildRamp, unitOf: unitOf, ModelGridLayer: ModelGridLayer, Overlay: Overlay, RAMPS: RAMPS,
      frameKey: frameKey, pickFrame: pickFrame, validateManifest: validateManifest, validateGrid: validateGrid,
      wantHalf: wantHalf, wantFull: wantFull, legendTicks: legendTicks, tilePixelLatLng: tilePixelLatLng,
      parsePng: parsePng, unfilter: unfilter, decodePngGrey: decodePngGrey,
      forwardPixel: forwardPixel, snapToPixel: snapToPixel, pixelOf: pixelOf, pad3: pad3,
      ringPlan: ringPlan, nextAvailable: nextAvailable, nearestIndex: nearestIndex, FrameCache: FrameCache, failureKind: failureKind, SPEEDS: SPEEDS, BASE_FPS: BASE_FPS,
      MAX_DECODED: MAX_DECODED, MAX_INFLIGHT: MAX_INFLIGHT,
      worldXY: worldXY, decodeCoast: decodeCoast, tileBox: tileBox, coastCellsForTile: coastCellsForTile, landPathsForTile: landPathsForTile,
      rasteriseScanline: rasteriseScanline, rasterise: rasterise, maskState: maskState, composeTile: composeTile, CoastStore: CoastStore, coastStore: coastStore,
      LAND_ALL: LAND_ALL, CLIP_FIELDS: CLIP_FIELDS, LAND_READOUT: LAND_READOUT, MAX_CHUNK_BYTES: MAX_CHUNK_BYTES, MAX_COAST_INFLIGHT: MAX_COAST_INFLIGHT }
  };
})();
