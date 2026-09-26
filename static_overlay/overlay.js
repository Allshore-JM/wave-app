/* Allshore Surf model overlay (animated frames per field; swell and wind particle animation). Loaded on demand; never on page load.
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
  // A reload (e.g. another forecast point) restores the valid time and play state only from a recent
  // save, and only when the run still has a frame close to that time; the field itself is restored by
  // the page's bootstrap from the same session key.
  var RESTORE_MAX_AGE_MS = 30 * 60 * 1000;
  var RESTORE_MAX_SHIFT_MS = 90 * 60 * 1000;
  var POINTER_RECHECK_MS = 30 * 60 * 1000;    // re-read latest.json on mount when the cached pointer is older
  var ENCODING = 'u8-linear-v2';
  var TILE = 256;
  // <= 29 visible characters incl. the separator so it never wraps over the zoom control. While a
  // coastline-clipped field is shown, " · GSHHG" follows, linking the LGPL notice published beside the
  // coast data (the only credit on the page: the panel carries no caption).
  var ATTRIBUTION = 'Overlay: <a href="https://polar.ncep.noaa.gov/waves/" target="_blank" rel="noopener">NOAA GFS-Wave</a>/GFS';
  // Colour stops over the LEGEND POSITION (0..1). For wave height the position is not linear in the
  // value (KNOTS below): 0-3 m takes about half of the scale, with a hue change every 0.5-1 m.
  var RAMPS = {
    hs:   [[0,'#1d3a8a'],[0.08,'#2563eb'],[0.17,'#0ea5e9'],[0.25,'#22d3ee'],[0.33,'#34d399'],[0.40,'#a3e635'],[0.47,'#facc15'],
           [0.58,'#fb923c'],[0.72,'#ef4444'],[0.87,'#c026d3'],[1,'#f5d0fe']],
    tp:   [[0,'#2a1f7a'],[0.25,'#2e7ed8'],[0.5,'#38c9a8'],[0.7,'#c8e63c'],[0.85,'#f7a52b'],[1,'#e8321f']],
    wind: [[0,'#e8f1ff'],[0.2,'#8cc4ff'],[0.4,'#3aa35a'],[0.6,'#f0d433'],[0.8,'#f0731f'],[1,'#b00f3a']]
  };
  // Value (SI units) -> legend position knots, piecewise linear, written for a legend from 0 to the last
  // knot (12 m; legendPos stretches them to any other legend); fields without knots are linear over the
  // legend range. The tiles stay linear in value (composeTile indexes a 256-entry LUT over the legend
  // range); only the colour each value gets, and where it sits on the legend, follow the knots.
  var KNOTS = {
    hs: [[0, 0], [0.5, 0.08], [1, 0.17], [2, 0.33], [3, 0.47], [4, 0.58], [6, 0.72], [9, 0.87], [12, 1]]
  };
  // Legend tick values in DISPLAY units per field and site unit; the legend top is added as "N+".
  // No numeric tick above ~80 % of the bar: it would collide with the right-aligned top label.
  var TICKS = {
    'hs|US': [0, 3, 6, 10, 15, 20], 'hs|Metric': [0, 1, 2, 3, 4, 6],
    'tp|US': [4, 8, 12, 16], 'tp|Metric': [4, 8, 12, 16],
    'wind|US': [0, 20, 40], 'wind|Metric': [0, 25, 50, 75]
  };

  // The tab's saved state; anything but a plain object (a corrupted or foreign value) counts as empty,
  // so the next write replaces it instead of failing on it for the rest of the tab's life.
  function saved() {
    try { var o = JSON.parse(sessionStorage.getItem(SESSION_KEY) || '{}'); return o && typeof o === 'object' && !Array.isArray(o) ? o : {}; } catch (e) { return {}; }
  }
  function save(patch) { try { sessionStorage.setItem(SESSION_KEY, JSON.stringify(Object.assign(saved(), patch))); } catch (e) {} }
  function hexToRgb(h) { var n = parseInt(h.slice(1), 16); return [n >> 16 & 255, n >> 8 & 255, n & 255]; }
  function rampAt(stops, t, out, o) {
    var k = 0;
    while (k < stops.length - 2 && t > stops[k + 1][0]) k++;
    var a = stops[k], b = stops[k + 1], f = (t - a[0]) / Math.max(1e-9, b[0] - a[0]);
    f = Math.max(0, Math.min(1, f));
    var ca = hexToRgb(a[1]), cb = hexToRgb(b[1]);
    out[o] = ca[0] + (cb[0] - ca[0]) * f; out[o + 1] = ca[1] + (cb[1] - ca[1]) * f; out[o + 2] = ca[2] + (cb[2] - ca[2]) * f;
  }
  function buildRamp(stops) {
    // 256-entry RGB lookup over the legend position t in [0,1] (the legend bar)
    var out = new Uint8ClampedArray(256 * 3);
    for (var i = 0; i < 256; i++) rampAt(stops, i / 255, out, i * 3);
    return out;
  }
  // Piecewise-linear map through knots [[x, y], ...] (x ascending), clamped at both ends.
  function through(knots, x, from, to) {
    if (x <= knots[0][from]) return knots[0][to];
    for (var i = 1; i < knots.length; i++) {
      var a = knots[i - 1], b = knots[i];
      if (x <= b[from]) return a[to] + (b[to] - a[to]) * (x - a[from]) / Math.max(1e-12, b[from] - a[from]);
    }
    return knots[knots.length - 1][to];
  }
  // Legend position (0..1) of a value in SI units, and back. A manifest whose legend is not 0..last knot
  // stretches the knots to it, so the bar, its ticks and the tiles agree and the legend top is the top colour.
  function knotsFit(k, legend) { return legend[0] === 0 && legend[1] === k[k.length - 1][0]; }
  function legendPos(field, legend, v) {
    var k = KNOTS[field];
    if (k) return through(k, knotsFit(k, legend) ? v : (v - legend[0]) / (legend[1] - legend[0]) * k[k.length - 1][0], 0, 1);
    return Math.max(0, Math.min(1, (v - legend[0]) / (legend[1] - legend[0])));
  }
  function legendInv(field, legend, p) {
    var k = KNOTS[field];
    if (k) { var x = through(k, p, 1, 0); return knotsFit(k, legend) ? x : legend[0] + x / k[k.length - 1][0] * (legend[1] - legend[0]); }
    return legend[0] + Math.max(0, Math.min(1, p)) * (legend[1] - legend[0]);
  }
  // The tiles' lookup: 256 entries LINEAR IN VALUE over the legend range (what composeTile indexes),
  // each coloured at that value's legend position. Linear fields give exactly buildRamp(stops).
  function buildLut(field, legend) {
    var stops = RAMPS[field], out = new Uint8ClampedArray(256 * 3);
    for (var i = 0; i < 256; i++) rampAt(stops, KNOTS[field] ? legendPos(field, legend, legend[0] + i / 255 * (legend[1] - legend[0])) : i / 255, out, i * 3);
    return out;
  }
  // The legend bar: 256 colours by LEGEND POSITION (buildLut is by value; for knotted fields they differ).
  function legendBar(field) { return buildRamp(RAMPS[field]); }
  function pad3(n) { return (n < 10 ? '00' : n < 100 ? '0' : '') + n; }
  function clear(el) { if (el) while (el.firstChild) el.removeChild(el.firstChild); }
  function mk(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text !== undefined) e.textContent = text; return e; }

  // ---- units (site preference: 'US' | 'Metric') ----
  function unitOf(field, unit) {
    if (field === 'hs') return unit === 'Metric' ? { label: 'm', f: function (v) { return v; }, d: 1 } : { label: 'ft', f: function (v) { return v * 3.28084; }, d: 1 };
    if (field === 'tp') return { label: 's', f: function (v) { return v; }, d: 1 };
    return unit === 'Metric' ? { label: 'km/h', f: function (v) { return v * 3.6; }, d: 0 } : { label: 'mph', f: function (v) { return v * 2.23694; }, d: 0 };
  }
  function fmtTick(v) { var r = Math.round(v * 10) / 10; return String(Math.abs(r - Math.round(r)) < 1e-9 ? Math.round(r) : r); }
  function legendTicks(field, fdef, unit) {
    var u = unitOf(field, unit), hi = u.f(fdef.legend[1]), per = u.f(1);          // display = SI * per
    var vals = TICKS[field + '|' + (unit === 'Metric' ? 'Metric' : 'US')] || [fdef.legend[0]];
    var out = vals.map(function (v, i) {
      var below = i === 0 && fdef.lo < fdef.legend[0] - 1e-9;          // values under the legend floor exist (Tp)
      return { pos: legendPos(field, fdef.legend, v / per), label: (below ? '≤' : '') + fmtTick(v) };
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
  // a full loop <= 16 MB on desktops, <= 5 MB on phones, for the 81 frames of the time): narrow maps (phones) stay on the half
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
  var MAX_INDEX_BYTES = 1024 * 1024;                      // index.json is 29 KB; every 5-degree cell listed would be ~60 KB
  var MAX_CELLS_PER_TILE = 16;                            // a tile touches <= 4 cells at tier-1 zooms; more = tier 0 for that tile
  var MAX_CHUNK_BYTES = 32 * 1024 * 1024;                 // decoded tier-1 chunks kept (LRU), in vertex bytes
  var MAX_COAST_INFLIGHT = 2;
  var MIN_PIECE_PX = 0.5;                                  // a piece smaller than this in both directions is not drawn
  var COAST_RETRY_MS = 60 * 1000;
  // World pixel coordinates at zoom 0 (0..256), lat clamped to the Mercator limit.
  function worldXY(lon, lat) {
    var s = Math.sin(Math.max(-MAX_LAT, Math.min(MAX_LAT, lat)) * Math.PI / 180);
    return [(lon + 180) / 360 * TILE, (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * TILE];
  }
  // A decoded tier-1 file may only hold pieces inside the cell it was fetched as: another cell's bytes
  // under this name must not become "no land here". The slack covers Float32 rounding of the boxes
  // (<= 2e-5 world px) and stays below the height of the polar rows (0.43 px), so even the two rows
  // nearest a pole cannot be swapped.
  var CELL_SLACK_PX = 0.01;
  function withinCell(c, name) {
    var p = name.split('_'), lat0 = +p[0], lon0 = +p[1], cell = c.cell, e = CELL_SLACK_PX;
    if (p.length !== 2 || !isFinite(lat0) || !isFinite(lon0) || !(cell > 0)) return false;
    var a = worldXY(lon0, lat0 + cell), b = worldXY(lon0 + cell, lat0), x0 = a[0] - e, y0 = a[1] - e, x1 = b[0] + e, y1 = b[1] + e;
    for (var i = 0; i < c.n; i++) {
      var k = i * 4;
      if (c.box[k] < x0 || c.box[k + 1] < y0 || c.box[k + 2] > x1 || c.box[k + 3] > y1) return false;
    }
    return true;
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
          if (x < -180 * q - 1 || x > 180 * q + 1 || y < -90 * q - 1 || y > 90 * q + 1) throw new Error('coast decode failed');
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
  // given decoded sets; sub-pixel pieces and same-pixel vertices are dropped, and a run of vertices
  // that all lie beyond the same side of the tile is collapsed to its ends (a segment between two
  // points in one half-plane never enters the tile, so the coverage inside it is unchanged).
  // Which sides of the tile (0..TILE) a point lies beyond, as bits.
  function outside(x, y) { return (x < 0 ? 1 : x > TILE ? 2 : 0) | (y < 0 ? 4 : y > TILE ? 8 : 0); }
  function landPathsForTile(coords, sets) {
    var b = tileBox(coords), scale = b.n, ox = b.xw * TILE, oy = coords.y * TILE, out = [];
    for (var s = 0; s < sets.length; s++) {
      var c = sets[s], box = c.box;
      for (var p = 0; p < c.n; p++) {
        var k = p * 4;
        if (box[k + 2] < b.x0 || box[k] > b.x1 || box[k + 3] < b.y0 || box[k + 1] > b.y1) continue;
        if ((box[k + 2] - box[k]) * scale < MIN_PIECE_PX && (box[k + 3] - box[k + 1]) * scale < MIN_PIECE_PX) continue;
        var bx0 = box[k], by0 = box[k + 1], bx1 = box[k + 2], by1 = box[k + 3];
        for (var r = c.ringStart[p]; r < c.ringStart[p + 1]; r++) {
          var v0 = c.vertStart[r], v1 = c.vertStart[r + 1], ring = new Float32Array((v1 - v0) * 2), m = 0, lx = NaN, ly = NaN;
          var run = 0, px = 0, py = 0, pending = false;             // the outside run being collapsed: its side bits and its last point
          for (var v = v0; v < v1; v++) {
            var wx = c.xy[v * 2], wy = c.xy[v * 2 + 1], x = wx * scale - ox, y = wy * scale - oy;
            // same-pixel vertices are dropped, judged against the last vertex that survived this test (so
            // the run collapsing below never changes which ones go), except on the piece's own bbox edges:
            // those are the clip points shared with the neighbouring cell's piece, and the nonzero union needs them exact
            if (v > v0 && Math.abs(x - lx) < 0.5 && Math.abs(y - ly) < 0.5 && wx !== bx0 && wx !== bx1 && wy !== by0 && wy !== by1) continue;
            lx = x; ly = y;
            var side = outside(x, y);
            if (m && (run & side)) { run &= side; px = x; py = y; pending = true; continue; }   // still beyond the same side: keep only the run's end
            if (pending) { ring[m * 2] = px; ring[m * 2 + 1] = py; m++; pending = false; }
            ring[m * 2] = x; ring[m * 2 + 1] = y; m++; run = side;
          }
          if (pending) { ring[m * 2] = px; ring[m * 2 + 1] = py; m++; }
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
  // ---- contour lines (pure; tested in Node) ----
  // Fields that can carry contours, and the interval in DISPLAY units per site unit (owner, 2026-09-25):
  // wave height every 2 ft / 0.5 m, peak period every 2 s; doubled below tile zoom 4.
  var CONTOURS = { hs: { US: 2, Metric: 0.5 }, tp: { US: 2, Metric: 2 } };
  var CONTOUR_INK = 0.55;                                  // line opacity over the field colour ("light" lines)
  var CONTOUR_JUMP = 2;                                    // peak period: no lines where it jumps more than 2 s between neighbouring nodes
  var CONTOUR_BLOCK = 16;                                  // tiles are drawn in 16-px blocks; a block no level crosses is skipped
  // Line profile by tile zoom, [up to zoom, core, edge]: full coverage up to `core` px from the level, fading to
  // none at `edge` px, so a line is core + edge px wide. Thinner at wide zooms, where the lines are densest
  // (owner, 2026-09-25: "slightly thinner, at wider zooms in particular").
  var CONTOUR_PROFILE = [[3, 0.2, 0.85], [5, 0.3, 0.95], [7, 0.4, 1.1], [99, 0.55, 1.25]];
  function contourProfile(z) {
    for (var i = 0; i < CONTOUR_PROFILE.length - 1; i++) if (z <= CONTOUR_PROFILE[i][0]) return CONTOUR_PROFILE[i];
    return CONTOUR_PROFILE[CONTOUR_PROFILE.length - 1];
  }
  var SMOOTH_ROWS = 32, SMOOTH_COLS = 64;                  // the smoothed copy of a frame is filled in blocks of nodes this size
  // Anti-aliased isolines drawn into an already composed tile, over the pixels x0..x1-1, y0..y1-1 (the
  // whole tile by default). F: (W+2)^2 level coordinates (display value / interval) with a 1-px apron,
  // NaN = no line there (no data, or a peak-period jump); only the pixels in the rect and their four
  // neighbours are read. rgba: the tile (W*W*4). A line sits where F crosses a whole level; a pixel's
  // coverage comes from its distance to the nearest level measured along the slope that LEADS to that
  // level (one-sided differences: a flat side never counts, so the foot of a steep ramp is not outlined
  // at a level the field does not reach there), giving anti-aliased lines core + edge px wide at any angle
  // (core, edge: the zoom's CONTOUR_PROFILE entry; the closest zooms' by default).
  // Only RGB changes, never alpha (the readout's drawn-pixel rule and the coast clip stay exactly as
  // composed). No line beside NaN, at level 0, or where levels would be under minGapPx apart (they would
  // merge into a band). ink(level): 30 (dark) or 255 (light) for that level; else by the pixel's colour.
  function contourTile(F, W, rgba, minGapPx, ink, x0, y0, x1, y1, core, edge) {
    var S = W + 2, n = 0;
    if (x0 === undefined) { x0 = 0; y0 = 0; x1 = W; y1 = W; }
    if (core === undefined) { var last = CONTOUR_PROFILE[CONTOUR_PROFILE.length - 1]; core = last[1]; edge = last[2]; }
    var ramp = edge - core;
    for (var y = y0; y < y1; y++) {
      for (var x = x0; x < x1; x++) {
        var i = (y + 1) * S + x + 1, k = (y * W + x) * 4;
        if (!rgba[k + 3]) continue;
        var f = F[i], l = F[i - 1], r = F[i + 1], u = F[i - S], d = F[i + S];
        if (f !== f || l !== l || r !== r || u !== u || d !== d) continue;       // NaN anywhere: no line
        var lv = Math.round(f), df = f - lv;
        if (lv === 0) continue;
        var sx = df > 0 ? Math.max(f - l, f - r) : Math.max(l - f, r - f);          // slope toward the level, per axis
        var sy = df > 0 ? Math.max(f - u, f - d) : Math.max(u - f, d - f);
        if (sx < 0) sx = 0;
        if (sy < 0) sy = 0;
        if (Math.abs(df) >= edge * (sx + sy)) continue;                             // edge px or more from the level (or no way to it)
        var gx = (r - l) / 2, gy = (d - u) / 2, g = Math.sqrt(gx * gx + gy * gy);
        if (g < 1e-9 || 1 / g < minGapPx) continue;
        var dpx = Math.abs(df) / Math.sqrt(sx * sx + sy * sy);                      // distance to the level in pixels
        var cov = dpx <= core ? 1 : dpx >= edge ? 0 : (edge - dpx) / ramp;
        if (!cov) continue;
        var R = rgba[k], G = rgba[k + 1], B = rgba[k + 2];
        var c = ink ? ink(lv) : (0.299 * R + 0.587 * G + 0.114 * B) > 170 ? 30 : 255;   // dark ink on light colours, light ink elsewhere
        var a = cov * CONTOUR_INK;
        rgba[k] = R + (c - R) * a; rgba[k + 1] = G + (c - G) * a; rgba[k + 2] = B + (c - B) * a;
        n++;
      }
    }
    return n;
  }
  // The smoothed copy of a frame the contours are traced on (colours, readout and clip keep the raw
  // codes): the 8-bit codes step in terraces that a flat sea crosses only every cell or two, so lines
  // on the raw codes follow the model grid in staircases. S = a masked [1,2,1]x[1,2,1] mean over the
  // present nodes (columns periodic, rows not wrapped), held within HALF a code of the node's own value
  // (its quantisation bin: the model value the code stands for lies within it): enough to dissolve the
  // terraces, never enough to move a line past what the colours and the readout show at the nodes (next
  // to islands the field changes by many codes per node). Missing nodes stay 0.
  // jump > 0 (peak period, in codes): a neighbour across a larger jump is left out, so swell regimes are
  // never blended, and J marks the cells (nodes r..r+1, c..c+1) with such a jump on an edge: no line is
  // drawn in them. The jump is counted between model nodes, so it is the same north-south and east-west
  // at every latitude. Fills nodes r0..r1-1, c0..c1-1 of S (and of J when given).
  function smoothBlock(q, cols, rows, S, J, jump, r0, r1, c0, c1) {
    var acc = 0, w = 0, v = 0;
    function add(x, wt) { if (x && !(jump && (x - v > jump || v - x > jump))) { acc += x * wt; w += wt; } }
    for (var r = r0; r < r1; r++) {
      var up = r > 0 ? (r - 1) * cols : -1, mid = r * cols, dn = r + 1 < rows ? (r + 1) * cols : -1;
      for (var c = c0; c < c1; c++) {
        var cl = c ? c - 1 : cols - 1, cr = c + 1 < cols ? c + 1 : 0;
        v = q[mid + c];
        if (J) {
          var b = q[mid + cr], e = dn >= 0 ? q[dn + c] : 0, h = dn >= 0 ? q[dn + cr] : 0;
          J[mid + c] = (v && b && Math.abs(v - b) > jump) || (e && h && Math.abs(e - h) > jump) ||
            (v && e && Math.abs(v - e) > jump) || (b && h && Math.abs(b - h) > jump) ? 1 : 0;
        }
        if (!v) { S[mid + c] = 0; continue; }
        acc = 4 * v; w = 4;
        add(q[mid + cl], 2); add(q[mid + cr], 2);
        if (up >= 0) { add(q[up + c], 2); add(q[up + cl], 1); add(q[up + cr], 1); }
        if (dn >= 0) { add(q[dn + c], 2); add(q[dn + cl], 1); add(q[dn + cr], 1); }
        var sv = acc / w;
        S[mid + c] = sv < v - 0.5 ? v - 0.5 : sv > v + 0.5 ? v + 0.5 : sv;     // (v >= 1: S stays >= 0.5, never 0 = missing)
      }
    }
  }
  // A peak-period jump in the cell (r, c), or with wide in any of the 3x3 cells around it (cells
  // narrower than the contour sample spacing, where a jump cell can fall between two samples).
  function jumpAt(J, cols, rows, r, c, wide) {
    if (J[r * cols + c]) return true;
    if (!wide) return false;
    for (var dr = -1; dr <= 1; dr++) {
      var rr = r + dr;
      if (rr < 0 || rr >= rows) continue;
      for (var dc = -1; dc <= 1; dc++) if (J[rr * cols + (c + dc + cols) % cols]) return true;
    }
    return false;
  }
  // Coast data for one base URL (index.json + tier 0 up front, tier-1 chunks on demand). One per URL
  // per page: the decoded tier 0 (~3 MB) and the chunk LRU survive Off/On.
  var COAST_STORES = {};
  function coastStore(url) { return COAST_STORES[url] || (COAST_STORES[url] = new CoastStore(url)); }
  function CoastStore(url) {
    this.url = url; this.status = 'idle'; this.index = null; this.tier0 = null; this.loading = null; this.failedAt = 0;
    this.chunks = new Map(); this.bytes = 0; this.inflight = {}; this.queue = []; this.queued = {}; this.failed = {};
    this.rev = 0; this.onChange = null; this.onNeeded = null; this.abort = null; this.retryTimer = null;
  }
  // The index the client can act on: a cell size that tiles the world, a directory name, a cells map.
  function validCoastIndex(idx) {
    return !!(idx && idx.format === 'coast-v1' && idx.tier0 && Number.isInteger(idx.tier0.max_zoom) && idx.tier0.max_zoom >= 0 && idx.tier1 &&
      Number.isInteger(idx.tier1.cell) && idx.tier1.cell >= 1 && idx.tier1.cell <= 90 && 180 % idx.tier1.cell === 0 &&
      typeof idx.tier1.dir === 'string' && /^[A-Za-z0-9_-]+$/.test(idx.tier1.dir) &&
      idx.tier1.cells && typeof idx.tier1.cells === 'object' && !Array.isArray(idx.tier1.cells));
  }
  // Resolves to the store when tier 0 is usable, to null otherwise (never rejects). The load has its
  // own abort (Off), so a field change while it runs simply keeps waiting for it; after a failure the
  // next loads answer null at once for retryMs (no second 15 s wait on the first frame), then retry.
  CoastStore.prototype.timeoutMs = 15000;                  // a coast download slower than this fails (unclipped + warning), like a stalled frame
  CoastStore.prototype.retryMs = COAST_RETRY_MS;
  CoastStore.prototype.load = function () {
    var self = this;
    if (this.status === 'ok') return Promise.resolve(this);
    if (this.loading) return this.loading;
    if (this.status === 'failed' && Date.now() - this.failedAt < this.retryMs) return Promise.resolve(null);
    this.status = 'loading';
    var base = this.url, ctrl = this.loadAbort = new AbortController(), sig = ctrl.signal, timedOut = false;
    var watchdog = setTimeout(function () { timedOut = true; ctrl.abort(); }, this.timeoutMs);
    this.loading = Promise.all([
      fetch(base + '/index.json', { signal: sig, mode: 'cors' }).then(function (r) {
        if (!r.ok) throw new Error('coast ' + r.status);
        if (r.headers && r.headers.get && Number(r.headers.get('content-length') || 0) > MAX_INDEX_BYTES) throw new Error('unsupported coast index');
        return r.json();
      }),
      fetch(base + '/world-i.bin', { signal: sig, mode: 'cors' }).then(function (r) {
        if (!r.ok) throw new Error('coast ' + r.status);
        if (Number(r.headers.get('content-length') || 0) > MAX_COAST_BYTES) throw new Error('coast decode failed');
        return r.arrayBuffer();
      })
    ]).then(function (res) {
      clearTimeout(watchdog);
      if (self.loadAbort !== ctrl || sig.aborted) throw abortError();     // aborted or superseded: this load owns nothing any more
      var idx = res[0];
      if (!validCoastIndex(idx)) throw new Error('unsupported coast index');
      self.tier0 = decodeCoast(res[1]); self.index = idx; self.hasTier1 = Object.keys(idx.tier1.cells).length > 0;
      self.status = 'ok'; self.loading = null; self.loadAbort = null;
      return self;
    }).catch(function () {
      clearTimeout(watchdog);
      if (self.loadAbort === ctrl) {
        self.loading = null; self.loadAbort = null;
        self.status = sig.aborted && !timedOut ? 'idle' : 'failed';
        if (self.status === 'failed') self.failedAt = Date.now();
      }
      return null;
    });
    return this.loading;
  };
  CoastStore.prototype.tier1Zoom = function (z) { return this.status === 'ok' && this.hasTier1 && z > this.index.tier0.max_zoom; };
  // The decoded sets a tile should be rasterised from, and whether they are the final ones. Missing
  // tier-1 chunks are requested; until they land the tile uses tier 0 (complete: false). A cell the
  // index lists but the bucket cannot serve (404, corrupt) keeps tier 0 for good: the same land at
  // ~1 km, never an open sea where the index says there is land.
  CoastStore.prototype.setsFor = function (coords) {
    if (this.status !== 'ok') return { sets: [], complete: true };
    if (!this.tier1Zoom(coords.z)) return { sets: [this.tier0], complete: true };
    var names = coastCellsForTile(coords, this.index.tier1.cell), sets = [], missing = [], standIn = false;
    if (names.length > MAX_CELLS_PER_TILE) return { sets: [this.tier0], complete: true };   // only a hostile index gets here
    for (var i = 0; i < names.length; i++) {
      var nm = names[i];
      if (!Object.prototype.hasOwnProperty.call(this.index.tier1.cells, nm)) continue;   // no land in that cell
      var c = this.chunks.get(nm);
      if (c) { this.chunks.delete(nm); this.chunks.set(nm, c); sets.push(c); }   // LRU touch
      else if (this.failed[nm] === true) standIn = true;
      else missing.push(nm);
    }
    if (missing.length) { this.request(missing); return { sets: [this.tier0], complete: false }; }
    return { sets: standIn ? [this.tier0] : sets, complete: true };
  };
  // The cells asked for by this call are fetched even when no registered tile needs them: Leaflet
  // registers a tile in _tiles only after createTile returns, so the tile being drawn is invisible to
  // onNeeded (the trim below is for entries left in the queue by tiles that have since been pruned).
  CoastStore.prototype.request = function (names) {
    var fresh = {};
    for (var i = 0; i < names.length; i++) {
      var nm = names[i], f = this.failed[nm];
      if (!Object.prototype.hasOwnProperty.call(this.index.tier1.cells, nm) || this.inflight[nm] || this.chunks.has(nm) || f === true) continue;
      if (typeof f === 'number' && f > Date.now()) { this._armRetry(f); continue; }         // in cooldown: asked again when it ends
      if (!this.queued[nm]) { this.queue.push(nm); this.queued[nm] = true; }
      fresh[nm] = true;
    }
    this._pump(fresh);
  };
  // One timer per store: when a cooldown ends the revision moves, so tiles still on the stand-in ask again.
  CoastStore.prototype._armRetry = function (until) {
    var self = this;
    if (this.retryTimer) return;
    this.retryTimer = setTimeout(function () { self.retryTimer = null; self.rev++; if (self.onChange) self.onChange(); }, Math.max(0, until - Date.now()) + 50);
  };
  CoastStore.prototype._pump = function (fresh) {
    var self = this, need = this.onNeeded ? this.onNeeded() : null;
    while (this.queue.length && Object.keys(this.inflight).length < MAX_COAST_INFLIGHT) {
      var nm = this.queue.shift();
      delete this.queued[nm];
      if (need && !need[nm] && !(fresh && fresh[nm])) continue;              // the tiles that wanted it are gone
      if (!this.abort) this.abort = new AbortController();
      (function (name, ctrl) {
        self.inflight[name] = ctrl;
        function mine() { return self.inflight[name] === ctrl; }
        fetch(self.url + '/' + self.index.tier1.dir + '/' + name + '.bin', { signal: ctrl.signal, mode: 'cors' }).then(function (r) {
          if (!r.ok) throw new Error('coast ' + r.status);
          if (Number(r.headers.get('content-length') || 0) > MAX_COAST_BYTES) throw new Error('coast decode failed');
          return r.arrayBuffer();
        }).then(function (buf) {
          if (ctrl.signal.aborted || !mine()) return;                       // Off, or a newer request owns this cell
          var c;
          try { c = decodeCoast(buf); } catch (e) { throw new Error('coast decode failed'); }   // the record is still ours: the catch below marks the cell failed for good
          if (c.cell !== self.index.tier1.cell || !withinCell(c, name)) throw new Error('coast decode failed');   // another cell's bytes under this name
          delete self.inflight[name];
          self.chunks.set(name, c); self.bytes += c.bytes; delete self.failed[name];
          self._evict();
          self.rev++;
          if (self.onChange) self.onChange();
          self._pump();
        }).catch(function (err) {
          if (ctrl.signal.aborted || (err && err.name === 'AbortError') || !mine()) return;
          delete self.inflight[name];
          var m = /^coast (\d{3})$/.exec(String(err && err.message || ''));
          if ((m && (m[1] === '404' || m[1] === '410')) || /decode/.test(String(err && err.message))) self.failed[name] = true;
          else { self.failed[name] = Date.now() + self.retryMs; self._armRetry(self.failed[name]); }
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
  // Off: stop everything and drop the chunks (cheap to re-fetch from the edge cache); tier 0 stays.
  CoastStore.prototype.abortAll = function () {
    if (this.abort) { this.abort.abort(); this.abort = null; }
    if (this.loadAbort) { this.loadAbort.abort(); this.loadAbort = null; }
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    if (this.status === 'loading') { this.status = 'idle'; this.loading = null; }
    this.inflight = {}; this.queue = []; this.queued = {}; this.failed = {}; this.onChange = null; this.onNeeded = null;
    this.chunks.clear(); this.bytes = 0;
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
      this._coast = null; this._clip = false; this.onRedraw = null;
      this._contour = null; this._F = null; this._sm = null; this._sv = null;
      this._sc = new Float64Array(TILE / 2 + 2); this._sr = new Float64Array(TILE / 2 + 2); this._srow = new Float64Array(TILE / 2 + 2);
      this._pi = new Int32Array(TILE + 2); this._pt = new Float64Array(TILE + 2);
    },
    // cfg {step: interval in display units, per: SI -> display factor} or null (no contours; the smoothed
    // copy of the frame is dropped). quiet: only set it, for the draw that follows.
    setContours: function (cfg, quiet) { this._contour = cfg; if (!cfg) this._sm = null; if (!quiet) this._redraw(); },
    // frame {q, cols, rows}; grid = the manifest grid for that resolution; fdef = the manifest field
    setFrame: function (frame, grid, fieldName, fdef, lut, entry) {
      validateGrid(grid, frame, fdef);
      this._frame = frame; this._grid = grid; this._lut = lut; this.field = fieldName; this.fdef = fdef; this.entry = entry || null;
      this._nearest = fdef.interpolation === 'nearest'; this._lo = fdef.lo; this._hi = fdef.hi; this._legend = fdef.legend;
      this._clip = !!(this._coast && CLIP_FIELDS[fieldName]);
      this._redraw();
    },
    clear: function () { this._frame = null; this.field = null; this.fdef = null; this.entry = null; this._clip = false; this._sm = null; this._redraw(); },
    hasFrame: function () { return !!this._frame; },
    // A loaded CoastStore (or null): from now on hs/tp tiles are clipped to its polygons.
    setCoast: function (store) {
      var self = this, prev = this._coast;
      if (prev && prev.onChange === this._onCoast) { prev.onChange = null; prev.onNeeded = null; }
      this._coast = store && store.status === 'ok' ? store : null;
      this._clip = !!(this._coast && CLIP_FIELDS[this.field]);
      if (this._coast) {
        this._onCoast = function () { self._redrawIncomplete(); };
        this._coast.onChange = this._onCoast;
        this._coast.onNeeded = function () { return self._cellsNeeded(); };
      }
      this._redraw();
      if (this.onRedraw) this.onRedraw();
    },
    _redraw: function () { for (var k in this._tiles) { var t = this._tiles[k]; if (t.el && t.coords) this._draw(t.el, t.coords); } },
    // The tier-1 cells the tiles on the map need right now (the store drops queued downloads for others).
    _cellsNeeded: function () {
      var st = this._coast, need = {};
      if (!st || !this._clip) return need;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.coords || !st.tier1Zoom(t.coords.z)) continue;
        var names = coastCellsForTile(t.coords, st.index.tier1.cell);
        if (names.length > MAX_CELLS_PER_TILE) continue;
        for (var i = 0; i < names.length; i++) need[names[i]] = true;
      }
      return need;
    },
    // A tier-1 chunk landed, failed, or a cooldown ended: the tiles drawn from the tier-0 stand-in are
    // re-evaluated and redrawn only when their mask actually changed.
    _redrawIncomplete: function () {
      if (!this._coast || !this._clip) return;
      var changed = false;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords || t.el._ovLandFinal !== false) continue;
        var before = t.el._ovLand, after = this._landFor(t.el, t.coords);
        if (after !== before) { this._draw(t.el, t.coords); changed = true; }
      }
      if (changed && this.onRedraw) this.onRedraw();                        // the animation's particles are gated by the masks
    },
    // The tile's land alpha, computed once per tile element (per zoom) and kept on it like _ovImg. A
    // stand-in that is still a stand-in after the store moved on is reused, not re-rasterised.
    _landFor: function (el, coords) {
      var st = this._coast, key = coords.z + '/' + tileBox(coords).xw + '/' + coords.y;
      if (el._ovLandKey === key && (el._ovLandFinal || el._ovLandRev === st.rev)) return el._ovLand;
      var got = st.setsFor(coords);
      if (!got.complete && el._ovLandKey === key && el._ovLandFinal === false) { el._ovLandRev = st.rev; return el._ovLand; }
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
    // THE sampler (sampleRow): both the tile drawing and the readout go through here, and so do the
    // particle flow's node values.
    _codeRow: function (r, colPos, n, out, off) { sampleRow(this._frame, this._nearest, r, colPos, n, out, off); },
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
      if (this._contour && !this._nearest && CONTOURS[this.field]) this._contours(coords, img.data);
      ctx.putImageData(img, 0, 0);
    },
    // Contours on the smoothed copy of the frame (smoothBlock), sampled at world pixels every s px (the
    // same pixels for every tile, so neighbouring tiles agree on their shared edges and world copies
    // match) and interpolated in between; a 16-px block is drawn only if its samples span a whole level.
    // all: draw every block (the tests compare it with the skipping).
    _contours: function (coords, rgba, all) {
      var cfg = this._contour, fr = this._frame, g = this._grid, cols = fr.cols, rows = fr.rows, i, j;
      var step = cfg.step * (coords.z < 4 ? 2 : 1), k = cfg.per / step, lo = this._lo, span = (this._hi - lo) / 254;
      var n = TILE * Math.pow(2, coords.z), cellPx = n * g.dlon / 360, s = cellPx >= 6 ? 4 : 2, m = TILE / s + 2;
      var prof = contourProfile(coords.z), core = prof[1], edge = prof[2];
      var x0 = coords.x * TILE, y0 = coords.y * TILE, cp = this._sc, rp = this._sr, row = this._srow, pi = this._pi, pt = this._pt;
      var V = this._sv || (this._sv = new Float64Array((TILE / 2 + 2) * (TILE / 2 + 2)));
      for (i = 0; i < m; i++) {
        var c = ((x0 + (i - 1) * s + 0.5) / n * 360 - 180 - g.lon0) / g.dlon;
        cp[i] = ((c % cols) + cols) % cols;
      }
      for (j = 0; j < m; j++) {
        var lat = Math.atan(Math.sinh(Math.PI - 2 * Math.PI * (y0 + (j - 1) * s + 0.5) / n)) * 180 / Math.PI;
        rp[j] = (g.lat0 - lat) / -g.dlat;
      }
      var sm = this._smooth(cp, rp, m), J = sm.jump ? sm.J : null, wide = cellPx < s;
      for (j = 0; j < m; j++) {
        var r = rp[j], o = j * m;
        if (!(r >= 0 && r <= rows - 1)) { for (i = 0; i < m; i++) V[o + i] = NaN; continue; }
        this._smoothRow(r, cp, m, row, sm.S);
        var r0 = Math.floor(r);
        for (i = 0; i < m; i++) V[o + i] = row[i] && !(J && jumpAt(J, cols, rows, r0, Math.floor(cp[i]), wide)) ? (lo + (row[i] - 1) * span) * k : NaN;
      }
      var W2 = TILE + 2, F = this._F || (this._F = new Float64Array(W2 * W2)), B = CONTOUR_BLOCK;
      for (i = 0; i < W2; i++) { var fi = (i - 1) / s + 1; pi[i] = Math.floor(fi); pt[i] = fi - pi[i]; }   // pixel column i - 1 between samples pi, pi + 1
      var lut = this._lut, L0 = this._legend[0], sc = 255 / (this._legend[1] - L0);
      var ink = function (lv) {                                  // one ink per level, from the colour of the level's own value
        var t = Math.round((lv / k - L0) * sc);
        t = t < 0 ? 0 : t > 255 ? 255 : t;
        return (0.299 * lut[t * 3] + 0.587 * lut[t * 3 + 1] + 0.114 * lut[t * 3 + 2]) > 170 ? 30 : 255;
      };
      for (var by = 0; by < TILE; by += B) {
        for (var bx = 0; bx < TILE; bx += B) {
          if (!all) {
            // F over the block and its ring lies within [mn, mx] of the samples it is interpolated from, and
            // changes by at most gxm / s (gym / s) per pixel across (down); a pixel is inked only within
            // `edge` px of a level along those slopes: no whole level within reach of [mn, mx], no line pixel
            var i0 = Math.floor((bx - 1) / s) + 1, i1 = Math.min(m - 1, Math.floor((bx + B) / s) + 2);
            var j0 = Math.floor((by - 1) / s) + 1, j1 = Math.min(m - 1, Math.floor((by + B) / s) + 2);
            var mn = Infinity, mx = -Infinity, gxm = 0, gym = 0;
            for (j = j0; j <= j1; j++) {
              for (i = i0; i <= i1; i++) {
                var v = V[j * m + i], dv;
                if (v < mn) mn = v;
                if (v > mx) mx = v;
                if (i < i1 && (dv = Math.abs(V[j * m + i + 1] - v)) > gxm) gxm = dv;
                if (j < j1 && (dv = Math.abs(V[(j + 1) * m + i] - v)) > gym) gym = dv;
              }
            }
            var reach = edge * (gxm + gym) / s;
            if (!(Math.floor(mx + reach) >= Math.ceil(mn - reach))) continue;
          }
          for (var py = by - 1; py <= by + B; py++) {             // F over the block and its 1-px ring, between the samples
            var jj = pi[py + 1], u = pt[py + 1], orow = (py + 1) * W2, ra = jj * m;
            for (var px = bx - 1; px <= bx + B; px++) {
              var t = pt[px + 1], a = ra + pi[px + 1];
              var val = t ? V[a] + (V[a + 1] - V[a]) * t : V[a];
              if (u) { var e = a + m, bot = t ? V[e] + (V[e + 1] - V[e]) * t : V[e]; val += (bot - val) * u; }
              F[orow + px + 1] = val;
            }
          }
          contourTile(F, TILE, rgba, 3, ink, bx, by, bx + B, by + B, core, edge);
        }
      }
    },
    // The smoothed copy of the frame on the map, filled in blocks of nodes as the tiles need them: once
    // per frame, and only while contours are on (setContours(null) drops it; ~5 MB at full resolution).
    _smooth: function (cp, rp, m) {
      var fr = this._frame, cols = fr.cols, rows = fr.rows, sm = this._sm, i, j, b;
      var jump = this.field === 'tp' ? CONTOUR_JUMP * 254 / (this._hi - this._lo) : 0;
      if (!sm || sm.cols !== cols || sm.rows !== rows) {
        var nbr = Math.ceil(rows / SMOOTH_ROWS), nbc = Math.ceil(cols / SMOOTH_COLS);
        sm = this._sm = { frame: null, jump: -1, cols: cols, rows: rows, S: new Float32Array(cols * rows), J: null,
          nbr: nbr, nbc: nbc, done: new Uint8Array(nbr * nbc), wr: new Uint8Array(nbr), wc: new Uint8Array(nbc) };
      }
      if (sm.frame !== fr || sm.jump !== jump) {
        sm.frame = fr; sm.jump = jump; sm.done.fill(0);
        if (jump && !sm.J) sm.J = new Uint8Array(cols * rows);
      }
      var wr = sm.wr, wc = sm.wc;
      wr.fill(0); wc.fill(0);
      for (j = 0; j < m; j++) {                                   // the rows the samples read, and one around them (jumpAt)
        if (!(rp[j] >= 0 && rp[j] <= rows - 1)) continue;
        var r0 = Math.floor(rp[j]);
        for (b = Math.max(0, r0 - 1); b <= Math.min(rows - 1, r0 + 1); b++) wr[(b / SMOOTH_ROWS) | 0] = 1;
      }
      for (i = 0; i < m; i++) {
        var c0 = Math.floor(cp[i]);
        for (b = -1; b <= 1; b++) wc[(((c0 + b + cols) % cols) / SMOOTH_COLS) | 0] = 1;
      }
      for (var br = 0; br < sm.nbr; br++) {
        if (!wr[br]) continue;
        for (var bc = 0; bc < sm.nbc; bc++) {
          if (!wc[bc] || sm.done[br * sm.nbc + bc]) continue;
          smoothBlock(fr.q, cols, rows, sm.S, jump ? sm.J : null, jump, br * SMOOTH_ROWS, Math.min(rows, (br + 1) * SMOOTH_ROWS),
            bc * SMOOTH_COLS, Math.min(cols, (bc + 1) * SMOOTH_COLS));
          sm.done[br * sm.nbc + bc] = 1;
        }
      }
      return sm;
    },
    // The contours' sampler: _codeRow's bilinear rule (same cells, same absent rule) over the smoothed
    // copy; a separate function keeps the tiles' sampler monomorphic on its Uint8Array.
    _smoothRow: function (r, colPos, n, out, S) {
      var cols = this._frame.cols, rows = this._frame.rows;
      var r0 = Math.floor(r), r1 = r0 + 1 < rows ? r0 + 1 : r0, fr = r - r0, w0 = 1 - fr, b0 = r0 * cols, b1 = r1 * cols;
      for (var i = 0; i < n; i++) {
        var cpos = colPos[i], c0 = Math.floor(cpos), c1 = c0 + 1 === cols ? 0 : c0 + 1, fc = cpos - c0;
        var a = S[b0 + c0], b = S[b0 + c1], c = S[b1 + c0], d = S[b1 + c1];
        var w = 0, acc = 0, wt;
        if (a) { wt = w0 * (1 - fc); acc += a * wt; w += wt; }
        if (b) { wt = w0 * fc; acc += b * wt; w += wt; }
        if (c) { wt = fr * (1 - fc); acc += c * wt; w += wt; }
        if (d) { wt = fr * fc; acc += d * wt; w += wt; }
        out[i] = w < 0.25 ? 0 : acc / w;
      }
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
  // The frame to resume on after a reload, or null (then the usual first frame): the save must be
  // recent and the run must have a frame within RESTORE_MAX_SHIFT_MS of the saved valid time.
  function restoreIndex(m, st, now) {
    if (!st || typeof st.t !== 'number' || typeof st.at !== 'number' || !isFinite(st.t) || !isFinite(st.at)) return null;
    if (now - st.at > RESTORE_MAX_AGE_MS || st.at - now > 60000) return null;
    var idx = nearestIndex(m, st.t);
    return Math.abs(Date.parse(m.frames[idx].valid_utc) - st.t) <= RESTORE_MAX_SHIFT_MS ? idx : null;
  }
  function reducedMotion() {
    try { return !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches); } catch (e) { return false; }
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

  // ---- direction fields and the animation (plan section 21 phase C) ----
  // Beside the field frame, the animation reads a second frame per step: pdir (the peak / dominant wave
  // direction) under wave height and peak period, wdir (the 10 m wind direction) under wind speed; both
  // are degrees true the waves / wind come FROM, coded circularly by the job (code 1 = 0 = 360 deg, code
  // 255 unused; the manifest marks them circular, convention "from"). They are never drawn as colour:
  // combined with the field into vectors they steer the particles drawn on one canvas in the ovAnimPane
  // (z 300, under the forecast points, no pointer events). A direction is shown only for the step on
  // the map, never an older one under a newer time: a step change clears the canvas until that step's
  // direction frame has landed (the frames load through the same scheduler, target field first, then
  // the target direction, then the ring interleaved, never more than MAX_INFLIGHT fetches in all).
  var DIR_FIELDS = { hs: 'pdir', tp: 'pdir', wind: 'wdir' };
  var ANIM_BUDGET_MS = { desktop: 4, phone: 8 };           // animation time per animation frame (plan section 21)
  var PARTICLE_PX2 = 900, PARTICLE_MIN = 150, PARTICLE_MAX = 3000;
  var PARTICLE_PX_PER_S = 3;                               // screen px/s per m/s of wind, before the latitude stretch
  // Particle speed in screen px/s (before the Mercator stretch) from the field value under it: wind by
  // its speed; swell lines by the wave height (nothing under the 0.1 m floor) or by the peak period (the
  // deep-water group speed grows with the period; nothing under the model's 3 s no-wave floor).
  var FLOW_SPEED = {
    wind: function (v) { return PARTICLE_PX_PER_S * v; },
    hs: function (v) { return v < 0.1 ? 0 : 8 + 3 * v; },
    tp: function (v) { return v < 3 ? 0 : 1.5 * v; }
  };
  var FLOW_STYLE = { wind: { width: 1.2, halo: 2.6 }, hs: { width: 1.5, halo: 3.2 }, tp: { width: 1.5, halo: 3.2 } };
  var TRAIL_POINTS = 10, TRAIL_EVERY_MS = 66;              // a particle's tail: its last 10 positions, one every 66 ms (~0.6 s), drawn fresh each frame
  var PARTICLE_LIFE_MS = { wind: [1000, 2500], hs: [2000, 4500], tp: [2000, 4500] };   // a particle lives this long (uniform), then respawns
  var CODE_SIN = new Float64Array(256), CODE_COS = new Float64Array(256);
  (function () { for (var q = 1; q < 255; q++) { var a = (q - 1) / 254 * 2 * Math.PI; CODE_SIN[q] = Math.sin(a); CODE_COS[q] = Math.cos(a); } })();
  // A manifest field usable as a direction: circular over 0-360, the FROM convention, published resolutions.
  function dirFieldOk(f) {
    return !!(f && f.circular === true && f.convention === 'from' && f.lo === 0 && f.hi === 360 && Array.isArray(f.resolutions) &&
      (f.resolutions.indexOf('half') >= 0 || f.resolutions.indexOf('full') >= 0));
  }
  // The grids a direction field is sampled on must pass the same checks as the field's (validateGrid runs
  // in setFrame for the field's resolution only; a desktop wave layer at zoom 6 reads its direction from
  // grid_half, which nothing else validates).
  function dirGridsOk(m, f) {
    var fd = { lo: 0, hi: 360, legend: [0, 360] };
    try {
      if (f.resolutions.indexOf('full') >= 0) validateGrid(m.grid, { cols: m.grid.cols, rows: m.grid.rows, q: { length: m.grid.cols * m.grid.rows } }, fd);
      if (f.resolutions.indexOf('half') >= 0) validateGrid(m.grid_half, { cols: m.grid_half.cols, rows: m.grid_half.rows, q: { length: m.grid_half.cols * m.grid_half.rows } }, fd);
      return true;
    } catch (e) { return false; }
  }
  // Resolution of the direction frame: the 0.5-degree frames at the site's default zoom (6) and wider,
  // the 0.25-degree ones as soon as the map is zoomed in (from 6.5; back below 6.25: hysteresis), among the
  // resolutions the field publishes (wind direction: half only). Near coasts a 0.5-degree node can sit
  // across land from the water it steers (G10-B P3-1), which the finer grid avoids once zoomed in.
  var DIR_FULL_AT = 6.5, DIR_HALF_BELOW = 6.25;             // the zoom buttons step 0.5: 6 -> 6.5 -> 6 returns to half
  function dirRes(f, zoom, current) {
    var full = f.resolutions.indexOf('full') >= 0, half = f.resolutions.indexOf('half') >= 0;
    if (!full) return 'half';
    if (!half) return 'full';
    if (current === 'full') return zoom < DIR_HALF_BELOW ? 'half' : 'full';
    return zoom >= DIR_FULL_AT ? 'full' : 'half';
  }
  // THE scalar sampler (ModelGridLayer._codeRow delegates here): codes along one fractional grid row r
  // (0..rows-1, caller-checked) at the periodic column positions colPos[0..n) (each 0 <= cpos < cols),
  // written to out[off..off+n). Bilinear over the present neighbours (q 0 = absent; mostly absent -> 0),
  // nearest for discontinuous fields. 0 means "no value".
  function sampleRow(f, nearest, r, colPos, n, out, off) {
    var cols = f.cols, q = f.q, i;
    if (nearest) {
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
  }
  // Latitude / longitude of a world pixel (n = world width in px at that zoom). lngOfWorldX is UNWRAPPED
  // (a world copy west of the dateline gives longitudes below -180): the samplers wrap columns themselves,
  // and Leaflet keys its tiles by unwrapped coordinates, which the readout gate looks up.
  function latOfWorldY(wy, n) { return Math.atan(Math.sinh(Math.PI - 2 * Math.PI * wy / n)) * 180 / Math.PI; }
  function lngOfWorldX(wx, n) { return wx / n * 360 - 180; }
  // The flow as VECTORS on the direction grid's nodes: U (east) / V (north) in screen px/s from the
  // field value under the node (the drawn frame, sampled at the node: exact where the field grid is the
  // direction grid or its parent) and the node's FROM direction; M marks the nodes that have both. The
  // lattice below interpolates these vectors, so a cyclone turns smoothly and slows to nothing at its eye
  // (blending directions alone snaps between neighbours there).
  function vectorNodes(field, layer, dframe, dgrid, prev) {
    var cols = dframe.cols, rows = dframe.rows, n = cols * rows, reuse = prev && prev.cols === cols && prev.rows === rows;
    var U = reuse ? prev.U : new Float32Array(n), V = reuse ? prev.V : new Float32Array(n), M = reuse ? prev.M : new Uint8Array(n);
    var f = layer._frame, g = layer._grid, spd = FLOW_SPEED[field] || FLOW_SPEED.wind, lo = layer._lo, hi = layer._hi, q = dframe.q;
    var colPos = new Float64Array(cols), codes = new Float64Array(cols), i, j;
    for (i = 0; i < cols; i++) { var lng = dgrid.lon0 + i * dgrid.dlon; colPos[i] = ((((lng - g.lon0) / g.dlon) % f.cols) + f.cols) % f.cols; }
    for (j = 0; j < rows; j++) {
      var lat = dgrid.lat0 + j * dgrid.dlat, r = (g.lat0 - lat) / -g.dlat, base = j * cols;
      if (r >= 0 && r <= f.rows - 1) sampleRow(f, layer._nearest, r, colPos, cols, codes, 0); else codes.fill(0);
      for (i = 0; i < cols; i++) {
        var d = q[base + i], c = codes[i];
        if (!d || d === 255 || !c) { M[base + i] = 0; U[base + i] = 0; V[base + i] = 0; continue; }
        var sp = spd(lo + (c - 1) / 254 * (hi - lo));
        M[base + i] = sp > 0 ? 1 : 0; U[base + i] = -sp * CODE_SIN[d]; V[base + i] = -sp * CODE_COS[d];   // FROM -> TOWARD
      }
    }
    return { cols: cols, rows: rows, U: U, V: V, M: M };
  }
  // The particles' screen lattice, every s px over the view: the node vectors interpolated bilinearly over
  // the present nodes (weight < 0.25: nothing), as screen velocities (x right, y down) times the Mercator
  // stretch min(sec lat, 3); nothing where the field is not drawn (clipped fields: the layer's tile land
  // masks, the readout's own rule, so swell particles never run over land).
  function flowField(view, s, nodes, dgrid, layer, clip, prev) {
    var cols = Math.ceil(view.w / s) + 1, rows = Math.ceil(view.h / s) + 1, n = TILE * Math.pow(2, view.z), reuse = prev && prev.cols === cols && prev.rows === rows;
    var u = reuse ? prev.u : new Float32Array(cols * rows), v = reuse ? prev.v : new Float32Array(cols * rows);
    if (reuse) { u.fill(0); v.fill(0); }
    var nc = nodes.cols, nr = nodes.rows, U = nodes.U, V = nodes.V, M = nodes.M, cp = new Float64Array(cols), i, j;
    for (i = 0; i < cols; i++) cp[i] = ((((lngOfWorldX(view.ox + i * s, n) - dgrid.lon0) / dgrid.dlon) % nc) + nc) % nc;
    var scale = Math.pow(2, view.z - view.zt), tiles = clip ? layer._tiles : null, lastKey = null, lastMask;
    for (j = 0; j < rows; j++) {
      var lat = latOfWorldY(view.oy + j * s, n), r = (dgrid.lat0 - lat) / -dgrid.dlat, base = j * cols;
      if (!(r >= 0 && r <= nr - 1)) continue;
      var k = Math.min(3, 1 / Math.max(1e-6, Math.cos(lat * Math.PI / 180)));
      var r0 = Math.floor(r), r1 = r0 + 1 < nr ? r0 + 1 : r0, fr = r - r0, w0 = 1 - fr, b0 = r0 * nc, b1 = r1 * nc;
      var Y = (view.oy + j * s) / scale, ty = Math.floor(Y / TILE), py = Math.floor(Y - ty * TILE);
      for (i = 0; i < cols; i++) {
        if (tiles) {                                                            // drawn water only (alpha >= LAND_READOUT)
          var X = (view.ox + i * s) / scale, tx = Math.floor(X / TILE), px = Math.floor(X - tx * TILE), key = tx + ':' + ty + ':' + view.zt;
          if (key !== lastKey) { var t = tiles[key]; lastKey = key; lastMask = t && t.el ? t.el._ovLand : undefined; }
          if (lastMask === undefined || lastMask === LAND_ALL) continue;
          if (lastMask !== null && 255 - lastMask[py * TILE + px] < LAND_READOUT) continue;
        }
        var cpos = cp[i], c0 = Math.floor(cpos), c1 = c0 + 1 === nc ? 0 : c0 + 1, fc = cpos - c0;
        var a = b0 + c0, b = b0 + c1, c = b1 + c0, d = b1 + c1, w = 0, x = 0, y = 0, wt;
        if (M[a]) { wt = w0 * (1 - fc); x += U[a] * wt; y += V[a] * wt; w += wt; }
        if (M[b]) { wt = w0 * fc; x += U[b] * wt; y += V[b] * wt; w += wt; }
        if (M[c]) { wt = fr * (1 - fc); x += U[c] * wt; y += V[c] * wt; w += wt; }
        if (M[d]) { wt = fr * fc; x += U[d] * wt; y += V[d] * wt; w += wt; }
        if (w < 0.25) continue;
        u[base + i] = k * x / w; v[base + i] = -k * y / w;
      }
    }
    return { s: s, cols: cols, rows: rows, u: u, v: v };
  }
  function nowMs() { return typeof performance !== 'undefined' && performance.now ? performance.now() : Date.now(); }

  // The animation: particles with fading trails along the swell (wave height, peak period) or the wind
  // (wind speed) on one devicePixelRatio canvas in the ovAnimPane (owner: particles only). A plain
  // object (no L.Layer): the controller attaches it while the Animation box is ticked and a direction
  // frame is on the map. It stops on Off, when unticked, in a hidden tab and while the map moves or
  // zooms (the canvas is hidden during a zoom and rebuilt on zoomend / moveend); under reduced motion
  // nothing animates at all (the checkbox says so).
  function FlowAnimator(map, layer) {
    this.map = map; this.layer = null; this._onLayerRedraw = null;
    this.canvas = null; this.ctx = null; this.dpr = 1; this.v = null;
    this.field = null; this.dir = null; this.dgrid = null; this.entry = null; this.fframe = null; this.mode = null;   // 'particles' while animating
    this.nodes = null; this.nodesFor = null; this.vf = null; this.particles = null; this.count = 0; this.target = 0;
    this.vel = new Float64Array(2); this.vel2 = new Float64Array(2); this.hist = null; this.histN = null; this.histT = null;
    this.active = false; this.suspended = 0; this.dirty = false; this.rafId = null; this.lastT = 0;
    this.ema = 0; this.adaptAt = 0; this._listeners = [];
    this.setLayer(layer);
  }
  // The field layer the particles are gated by; when its tiles are redrawn for a coastline chunk that landed
  // (the land mask changed under the particles) the flow is rebuilt.
  FlowAnimator.prototype.setLayer = function (layer) {
    var self = this;
    if (this.layer === layer) return;
    if (this.layer && this.layer.onRedraw === this._onLayerRedraw) this.layer.onRedraw = null;
    this.layer = layer;
    if (layer) { this._onLayerRedraw = function () { if (self.dir && !self.suspended) self._rebuild(); }; layer.onRedraw = this._onLayerRedraw; }
  };
  // The scheduler (rAF in browsers; a timer where there is none, e.g. the Node tests).
  FlowAnimator.prototype.raf = function (fn) { return typeof requestAnimationFrame === 'function' ? requestAnimationFrame(fn) : setTimeout(function () { fn(nowMs()); }, 16); };
  FlowAnimator.prototype.caf = function (id) { if (typeof cancelAnimationFrame === 'function') cancelAnimationFrame(id); else clearTimeout(id); };
  FlowAnimator.prototype._on = function (ev, fn) { this.map.on(ev, fn); this._listeners.push([ev, fn]); };
  FlowAnimator.prototype.attach = function () {
    if (this.canvas) return;
    var self = this, map = this.map, pane = map.getPane('ovAnimPane');
    if (!pane) { pane = map.createPane('ovAnimPane'); pane.style.zIndex = 300; pane.style.pointerEvents = 'none'; }
    var cv = document.createElement('canvas'); cv.className = 'ov-anim leaflet-zoom-hide';
    pane.appendChild(cv); this.canvas = cv; this.ctx = cv.getContext('2d');
    // a zoom fires zoomstart + movestart, then zoomend + moveend: the counter rebuilds once, at the end
    this._on('zoomstart', function () { self.suspend(); if (self.canvas) self.canvas.style.visibility = 'hidden'; });
    this._on('zoomend', function () { if (self.canvas) self.canvas.style.visibility = ''; self.resume(true); });
    this._on('movestart', function () { self.suspend(); });
    this._on('moveend', function () { self.resume(true); });
    // a view reset (setView without animation, a jump of a screen or more, the site's single-world
    // clamp on a resize) recreates the tiles AFTER moveend: the flow is rebuilt once they exist
    this._on('viewreset', function () { if (!self.suspended) self._rebuild(); });
    this._on('resize', function () { self._rebuild(); });
  };
  FlowAnimator.prototype.detach = function () {
    var self = this;
    this.stop();
    this._listeners.forEach(function (l) { self.map.off(l[0], l[1]); }); this._listeners = [];
    if (this.canvas) { if (this.canvas.remove) this.canvas.remove(); this.canvas = null; this.ctx = null; }
    this.setLayer(null);
    this.dir = null; this.dgrid = null; this.fframe = null; this.vf = null; this.nodes = null; this.nodesFor = null; this.particles = null; this.v = null;
  };
  FlowAnimator.prototype.setField = function (field) { if (this.field !== field) { this.field = field; this.particles = null; this.clearData(); } };
  // The direction frame of the step on the map (the caller checks it is that step's; entry = the
  // manifest frame entry). A new frame keeps the particles, so playback flows on.
  FlowAnimator.prototype.setData = function (frame, grid, entry) {
    if (this.dir === frame && this.fframe === (this.layer && this.layer._frame)) { this.entry = entry || null; return; }
    this.dir = frame; this.dgrid = grid; this.entry = entry || null; this._rebuild();
  };
  FlowAnimator.prototype.clearData = function () {
    this.dir = null; this.dgrid = null; this.entry = null; this.fframe = null; this.vf = null; this.nodes = null; this.nodesFor = null; this.stop(); this._clear();
  };
  // Nested suspends (a zoom fires zoomstart + movestart; a hidden tab adds one): the last resume runs the
  // rebuild any of them asked for, so a zoom that ended while the tab was hidden is not drawn stale.
  FlowAnimator.prototype.suspend = function () { this.suspended++; this.stop(); };
  FlowAnimator.prototype.resume = function (rebuild) {
    this.dirty = this.dirty || !!rebuild;
    if (this.suspended > 0) this.suspended--;
    if (this.suspended) return;
    if (this.dirty) { this.dirty = false; this._rebuild(); } else this._start();
  };
  // The layer changed under the animation (opacity, a coast store): rebuild.
  FlowAnimator.prototype.refresh = function () { if (this.dir && !this.suspended && this.canvas) this._rebuild(); };
  // The map's geometry: the world pixel of the container's top-left from Leaflet's pixel bounds (exact
  // whatever Leaflet's cached size says) and the container's REAL size (the site sets the map height by
  // script after Leaflet measured it; Leaflet's getSize() lags until the first resize).
  FlowAnimator.prototype.view = function () {
    var map = this.map, size = map.getSize(), c = map.getContainer && map.getContainer(), pb = map.getPixelBounds(), z = map.getZoom();
    var w = (c && c.clientWidth) || size.x, h = (c && c.clientHeight) || size.y;
    return { z: z, w: w, h: h, ox: pb.min.x, oy: pb.min.y, zt: typeof this.layer._tileZoom === 'number' ? this.layer._tileZoom : Math.round(z) };
  };
  FlowAnimator.prototype._place = function (v) {
    var cv = this.canvas, dpr = Math.min(2, (window.devicePixelRatio || 1)), W = Math.round(v.w * dpr), H = Math.round(v.h * dpr);
    this.dpr = dpr;
    if (cv.width !== W || cv.height !== H) { cv.width = W; cv.height = H; cv.style.width = v.w + 'px'; cv.style.height = v.h + 'px'; }
    var p = this.map.containerPointToLayerPoint([0, 0]);
    cv.style.transform = 'translate3d(' + p.x + 'px,' + p.y + 'px,0)';
    this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  };
  FlowAnimator.prototype._clear = function () { if (this.ctx && this.v) { this.ctx.globalCompositeOperation = 'source-over'; this.ctx.clearRect(0, 0, this.v.w, this.v.h); } };
  // Everything that depends on the view or the data: the canvas placement, the node vectors (per step)
  // and the screen lattice (per view), then the loop.
  FlowAnimator.prototype._rebuild = function () {
    if (!this.canvas || !this.dir || !this.layer || !this.layer.hasFrame()) { this.stop(); this.vf = null; this._clear(); return; }
    var v = this.view(), prev = this.v, same = !!(prev && prev.w === v.w && prev.h === v.h && prev.ox === v.ox && prev.oy === v.oy && prev.z === v.z);
    this.v = v; this._place(v); this.fframe = this.layer._frame;
    if (reducedMotion()) { this.mode = null; this.stop(); this.vf = null; this._clear(); return; }   // no animation at all
    this.mode = 'particles';
    var cellPx = TILE * Math.pow(2, v.z) * this.dgrid.dlon / 360;
    if (!this.nodes || this.nodesFor !== this.dir || this.nodesFrame !== this.fframe) {       // once per step, not per pan
      this.nodes = vectorNodes(this.field, this.layer, this.dir, this.dgrid, this.nodes); this.nodesFor = this.dir; this.nodesFrame = this.fframe;
    }
    this.vf = flowField(v, cellPx >= 16 ? 8 : 4, this.nodes, this.dgrid, this.layer, !!this.layer._clip, this.vf);
    this.target = Math.max(PARTICLE_MIN, Math.min(PARTICLE_MAX, Math.round(v.w * v.h / PARTICLE_PX2)));   // follows the view
    if (!this.particles) this._seed(v); else if (this.count > this.target) this.count = this.target;
    if (!same) this._clear();                                                    // a moved view starts clean (a new step keeps the tails)
    this._start();
  };
  FlowAnimator.prototype._life = function () { return PARTICLE_LIFE_MS[this.field] || PARTICLE_LIFE_MS.wind; };
  FlowAnimator.prototype._seed = function (v) {
    this.count = this.target;
    this.particles = new Float32Array(PARTICLE_MAX * 4);                     // x, y, age (ms), life (ms) per particle
    this.hist = new Float32Array(PARTICLE_MAX * TRAIL_POINTS * 2); this.histN = new Uint8Array(PARTICLE_MAX); this.histT = new Float32Array(PARTICLE_MAX);
    for (var i = 0; i < PARTICLE_MAX; i++) { this._respawn(i * 4, v); this.particles[i * 4 + 2] = Math.random() * this._life()[0]; }
  };
  FlowAnimator.prototype._respawn = function (k, v) {
    var P = this.particles, life = this._life(); P[k] = Math.random() * v.w; P[k + 1] = Math.random() * v.h; P[k + 2] = 0;
    P[k + 3] = life[0] + Math.random() * (life[1] - life[0]);
    this.histN[k >> 2] = 0; this.histT[k >> 2] = 0;                             // a fresh particle has no tail
  };
  // Does the flow run at every lattice node around (x, y)? (A particle at a coast or a data edge respawns
  // before it can drift onto the undrawn side.)
  FlowAnimator.prototype._flowing = function (x, y) {
    var vf = this.vf, s = vf.s, cols = vf.cols, rows = vf.rows, i0 = (x / s) | 0, j0 = (y / s) | 0, i1 = i0 + 1 < cols ? i0 + 1 : i0, j1 = j0 + 1 < rows ? j0 + 1 : j0;
    var u = vf.u, v = vf.v, a = j0 * cols + i0, b = j0 * cols + i1, c = j1 * cols + i0, d = j1 * cols + i1;
    return (u[a] !== 0 || v[a] !== 0) && (u[b] !== 0 || v[b] !== 0) && (u[c] !== 0 || v[c] !== 0) && (u[d] !== 0 || v[d] !== 0);
  };
  // The lattice velocity at a screen point, bilinear over the four cells around it (out[0], out[1] px/s).
  FlowAnimator.prototype._velocity = function (x, y, out) {
    var vf = this.vf, s = vf.s, cols = vf.cols, rows = vf.rows, gx = x / s, gy = y / s, i0 = gx | 0, j0 = gy | 0, fx = gx - i0, fy = gy - j0;
    var i1 = i0 + 1 < cols ? i0 + 1 : i0, j1 = j0 + 1 < rows ? j0 + 1 : j0, a = j0 * cols + i0, b = j0 * cols + i1, c = j1 * cols + i0, d = j1 * cols + i1, u = vf.u, v = vf.v;
    out[0] = (u[a] * (1 - fx) + u[b] * fx) * (1 - fy) + (u[c] * (1 - fx) + u[d] * fx) * fy;
    out[1] = (v[a] * (1 - fx) + v[b] * fx) * (1 - fy) + (v[c] * (1 - fx) + v[d] * fx) * fy;
  };
  FlowAnimator.prototype._start = function () {
    if (this.rafId !== null || !this.dir || !this.canvas || !this.vf) return;
    var self = this; this.active = true; this.lastT = 0;
    this.rafId = this.raf(function (t) { self._frame(t); });
  };
  FlowAnimator.prototype.stop = function () { this.active = false; if (this.rafId !== null) { this.caf(this.rafId); this.rafId = null; } };
  FlowAnimator.prototype._frame = function (t) {
    this.rafId = null;
    if (!this.active || this.suspended || !this.dir || !this.canvas || (typeof document !== 'undefined' && document.hidden)) { this.active = false; return; }
    var dt = this.lastT ? Math.min(50, Math.max(0, t - this.lastT)) : 16; this.lastT = t;
    var t0 = nowMs(); this._renderParticles(dt); this._adapt(nowMs() - t0, t);
    var self = this; this.rafId = this.raf(function (tt) { self._frame(tt); });
  };
  // Frame-time adaptation (particles): fewer when a frame runs over the budget, back up when there is room.
  FlowAnimator.prototype._adapt = function (ms, t) {
    this.ema = this.ema ? this.ema * 0.9 + ms * 0.1 : ms;
    if (t - this.adaptAt < 1000) return;
    this.adaptAt = t;
    if (this.mode !== 'particles' || !this.v) return;
    var budget = this.v.w < 700 ? ANIM_BUDGET_MS.phone : ANIM_BUDGET_MS.desktop;
    if (this.ema > budget) this.count = Math.max(PARTICLE_MIN, Math.round(this.count * 0.85));
    else if (this.ema < budget * 0.6 && this.count < this.target) this.count = Math.min(this.target, Math.round(this.count * 1.1) + 1);
  };
  // Particles over a CLEARED canvas each frame (no compositing fade: an 8-bit fade never reaches zero and
  // leaves a veil): every live particle moves by the flow (a midpoint step, smooth around a turning
  // flow), keeps a short history of positions (one every TRAIL_EVERY_MS) and is drawn as a dim tail
  // through them plus a bright head segment, each a dark halo under a light core (four strokes a frame).
  // One that ages out, leaves the map, or reaches a lattice cell where the flow stops (a coast, a data
  // edge, a flat sea) is respawned somewhere in the view.
  FlowAnimator.prototype._renderParticles = function (dt) {
    var ctx = this.ctx, v = this.v, vf = this.vf, P = this.particles, H = this.hist, HN = this.histN, HT = this.histT;
    if (!ctx || !v || !vf || !P) return;
    var w = v.w, h = v.h, n = this.count, dts = dt / 1000, half = dts / 2, V1 = this.vel, V2 = this.vel2, i, k, q, m;
    var style = FLOW_STYLE[this.field] || FLOW_STYLE.wind;
    ctx.globalCompositeOperation = 'source-over'; ctx.clearRect(0, 0, w, h);
    ctx.lineCap = 'round'; ctx.lineJoin = 'round';
    // move, then the head segments (moveTo the old position, lineTo the new one) in one path
    ctx.beginPath();
    for (i = 0, k = 0; i < n; i++, k += 4) {
      var x = P[k], y = P[k + 1], age = P[k + 2];
      if (age >= P[k + 3] || !this._flowing(x, y)) { this._respawn(k, v); continue; }
      this._velocity(x, y, V1);
      var xm = x + V1[0] * half, ym = y + V1[1] * half;
      if (xm >= 0 && ym >= 0 && xm < w && ym < h) this._velocity(xm, ym, V2); else { V2[0] = V1[0]; V2[1] = V1[1]; }
      var nx = x + V2[0] * dts, ny = y + V2[1] * dts;
      if (!(nx >= 0 && ny >= 0 && nx < w && ny < h)) { this._respawn(k, v); continue; }
      HT[i] += dt;
      if (HT[i] >= TRAIL_EVERY_MS || HN[i] === 0) {                             // record the position the head leaves behind
        HT[i] = 0; m = HN[i]; q = (i * TRAIL_POINTS + (m % TRAIL_POINTS)) * 2; H[q] = x; H[q + 1] = y; HN[i] = m + 1 > 255 ? 255 - TRAIL_POINTS + ((m + 1) % TRAIL_POINTS) : m + 1;
      }
      ctx.moveTo(x, y); ctx.lineTo(nx, ny);
      P[k] = nx; P[k + 1] = ny; P[k + 2] = age + dt;
    }
    ctx.lineWidth = style.halo; ctx.strokeStyle = 'rgba(0,0,0,0.35)'; ctx.stroke();
    ctx.lineWidth = style.width; ctx.strokeStyle = 'rgba(255,255,255,0.9)'; ctx.stroke();
    // the tails: the recorded positions, oldest first, up to the current position
    ctx.beginPath();
    for (i = 0, k = 0; i < n; i++, k += 4) {
      m = HN[i]; if (m < 2 && !(m === 1)) continue;
      var cnt = m < TRAIL_POINTS ? m : TRAIL_POINTS, start = m - cnt, first = true;
      for (q = start; q < m; q++) { var idx = (i * TRAIL_POINTS + (q % TRAIL_POINTS)) * 2; if (first) { ctx.moveTo(H[idx], H[idx + 1]); first = false; } else ctx.lineTo(H[idx], H[idx + 1]); }
      ctx.lineTo(P[k], P[k + 1]);
    }
    ctx.lineWidth = style.halo; ctx.strokeStyle = 'rgba(0,0,0,0.18)'; ctx.stroke();
    ctx.lineWidth = style.width; ctx.strokeStyle = 'rgba(255,255,255,0.4)'; ctx.stroke();
  };

  // ---- the timeline by time and the run's times (plan section 22) ----
  // The frame for a timeline value of `hour` (hours after the run; `hours` = every frame's hour, ascending): moving
  // later, the first frame at or after it; moving earlier, the last frame at or before it. Frames are hourly to +120 h
  // and 3-hourly after, so an arrow key never sticks between two frames.
  function frameAtHour(hours, hour, later) {
    var i;
    if (later) { for (i = 0; i < hours.length; i++) if (hours[i] >= hour) return i; return hours.length - 1; }
    for (i = hours.length - 1; i >= 0; i--) if (hours[i] <= hour) return i;
    return 0;
  }
  // The timeline's direction of travel. During a pointer drag each input is compared with the pointer's PREVIOUS raw value
  // (comparing with the frame just snapped to made a slow forward drag read as backward: 120 -> 123 -> 120 ..., G13b P1-1);
  // otherwise (keys, a click) with the value the thumb showed. pick() returns the frame index and records what is shown.
  function TimelineState() { this.dragging = false; this.lastRaw = null; this.shown = null; }
  TimelineState.prototype.pick = function (hours, v) {
    var ref = this.dragging && this.lastRaw !== null ? this.lastRaw : this.shown;
    var idx = frameAtHour(hours, v, ref === null || v >= ref);
    if (this.dragging) this.lastRaw = v;
    this.shown = hours[idx];
    return idx;
  };
  TimelineState.prototype.start = function () { this.dragging = true; this.lastRaw = null; };
  TimelineState.prototype.end = function () { this.dragging = false; this.lastRaw = null; };
  // "1:07 PM HST" in the computer's own time zone (not the forecast table's), with the weekday when it is not today there.
  function localClock(ms, now) {
    var d = new Date(ms), opts = { hour: 'numeric', minute: '2-digit', timeZoneName: 'short' };
    if (new Date(now).toDateString() !== d.toDateString()) opts.weekday = 'short';
    try { return new Intl.DateTimeFormat(undefined, opts).format(d); } catch (e) { return d.toISOString().slice(11, 16) + ' UTC'; }
  }
  var CYCLE_MS = 6 * 3.6e6;                                // a new model cycle every 6 h
  // When this run went live, and when the next one is expected: one cycle later with this run's own publish lag
  // (published_utc + 6 h), rounded up to 5 minutes; null without a usable publish time.
  function runTimes(m, now, newer) {
    var run = Date.parse(m.run_utc), pub = Date.parse(m.published_utc);
    if (!isFinite(run) || !isFinite(pub) || pub < run) return null;
    var next = Math.ceil((pub + CYCLE_MS) / 3e5) * 3e5, status = newer ? 'newer' : next <= now ? 'shortly' : 'about';
    return { status: status, next: next, text: 'live since ' + localClock(pub, now) + ' · ' +
      (status === 'newer' ? 'a newer run is available' : status === 'shortly' ? 'next update expected shortly' : 'next update about ' + localClock(next, now)) };
  }

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
    this.playGen = 0; this.transientFails = 0; this._staleShown = false; this._pendingRestore = null; this._onHide = null;
    // opts.coast: clip wave height / peak period to the coastlines published beside the frames
    this.coast = opts.coast ? coastStore(this.root + '/static/coast/v1') : null;
    var s = saved();
    this.opacity = typeof s.opacity === 'number' && s.opacity >= 0.2 && s.opacity <= 1 ? s.opacity : 0.65;
    this.speed = SPEEDS.indexOf(s.speed) >= 0 ? s.speed : 1;
    this.contours = s.contours === true;                   // off until ticked, then remembered for the tab
    this.anim = s.anim === true;                           // the Animation box, likewise
    this.dcache = new FrameCache(MAX_DECODED); this.flow = null; this.dres = null;   // direction frames and their animator
  }
  // restore: true on the mount the page makes after a reload (the field was saved in this tab); the
  // saved valid time and play state are then reused when they are recent (restoreIndex), by this mount
  // or, if it never lands a frame, by the next one (another field picked meanwhile, Retry) until Off.
  Overlay.prototype.mount = function (fieldName, restore) {
    if (restore) this._pendingRestore = saved();
    var self = this, st = this._pendingRestore;
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
      if (this.flow) this.flow.clearData();
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
      var rIdx = prevValid === null && st ? restoreIndex(m, st, Date.now()) : null;
      var idx = prevValid === null ? (rIdx !== null ? rIdx : pickFrame(m)) : m.run === prevRun ? Math.min(self.frameIndex, self.n - 1) : nearestIndex(m, prevValid);
      var resume = rIdx !== null && st.playing === true;
      self.res = wantHalf(self.map.getZoom(), self._dims().w, fieldName) ? 'half' : 'full';
      self._ensureFlow();
      if (!self.runTimer) self.runTimer = setInterval(function () { self._checkRun(); }, RUN_CHECK_MS);
      return self._goto(idx, sig).catch(function (err) {
        // the chosen first frame is missing: show the next one that exists rather than nothing
        if (!(err && err.unavailable) || (sig && sig.aborted)) throw err;
        var next = nextAvailable(idx, 1, self.n, function (j) { return self._isUnavailable(j); });
        if (next === null) throw err;
        return self._goto(next, sig);
      }).then(function () {
        // it was playing before the reload: keep playing, unless the viewer asks for reduced motion;
        // in a hidden tab it resumes when the tab is shown (the same path as the visibility pause)
        if (!resume || sig.aborted || reducedMotion()) return;
        if (document.hidden) { self.wasPlaying = true; self._persist(); } else self.play();
      });
    }).catch(function (err) { self._fail(sig, err); });
  };
  // Cache / in-flight / unavailable keys carry the run: a decode that outlives an Update can never be
  // taken for a frame of the new run.
  // kind 'dir': the direction frame (pdir / wdir at its own resolution) of the same step.
  Overlay.prototype._key = function (idx, kind) {
    var dir = kind === 'dir';
    return this.manifest.run + '/' + (dir ? this.dres : this.res) + '/' + (dir ? DIR_FIELDS[this.field] : this.field) + '/' + this.manifest.frames[idx].step;
  };
  // unavailable[key] is true (permanent) or a timestamp until which the frame is left alone (transient).
  Overlay.prototype._isUnavailable = function (idx, kind) {
    var u = this.unavailable[this._key(idx, kind)];
    return u === true || (typeof u === 'number' && u > Date.now());
  };
  Overlay.prototype._lut = function () {
    var f = this.manifest && this.manifest.fields[this.field], legend = f ? f.legend : [0, 1], key = this.field + '|' + legend.join(',');
    if (this._lutFor !== key) { this._lutCache = buildLut(this.field, legend); this._lutFor = key; }
    return this._lutCache;
  };
  // The decoded frame for index idx (kind 'dir': its direction frame): cache, then an in-flight fetch,
  // then a new one. A 404 or a decode failure marks the frame unavailable for this session
  // (err.unavailable); an abort does not.
  Overlay.prototype._ensure = function (idx, kind) {
    var dir = kind === 'dir', cache = dir ? this.dcache : this.cache;
    var self = this, key = this._key(idx, kind), hit = cache.get(key);
    if (hit) return Promise.resolve(hit);
    if (this.inflight[key]) return this.inflight[key].promise;
    if (this._isUnavailable(idx, kind)) { var e = new Error('frame unavailable'); e.unavailable = true; return Promise.reject(e); }
    delete this.unavailable[key];                                            // an expired cooldown: try again
    var m = this.manifest, ctrl = new AbortController(), rec, half = (dir ? this.dres : this.res) === 'half', stalled = false, kindName = dir ? 'dir' : 'field';
    var url = this.root + '/' + frameKey(m, m.frames[idx], dir ? DIR_FIELDS[this.field] : this.field, half);
    function mine() { return self.inflight[key] === rec; }
    // a download that stalls (one frame in a loop took 6.5 s at G4) is dropped like a transient failure
    var watchdog = setTimeout(function () { stalled = true; ctrl.abort(); }, Math.max(STALL_MS, 4 * self._interval()));
    var p = decodeFrame(url, ctrl.signal, half ? m.grid_half : m.grid).then(function (frame) {
      clearTimeout(watchdog);
      if (mine()) delete self.inflight[key];
      // A decode cannot be cancelled: one that outlives its abort (Update, Off, field or resolution
      // change) must neither be cached nor delivered.
      if ((ctrl.signal.aborted && !stalled) || self.manifest !== m) throw abortError();
      if (!dir) self.transientFails = 0;                                      // the outage counter follows the field frames only
      cache.set(key, frame, self.frameIndex !== null ? self._key(self.frameIndex, kind) : null);
      return frame;
    }, function (err) {
      clearTimeout(watchdog);
      if (mine()) delete self.inflight[key];
      if ((ctrl.signal.aborted && !stalled) || self.manifest !== m) throw abortError();
      if (stalled) err = new Error('frame stalled');
      var kind_ = failureKind(err);
      self.unavailable[key] = kind_ === 'permanent' ? true : Date.now() + RETRY_AFTER_MS;
      err.unavailable = true;
      if (kind_ === 'transient' && !dir && ++self.transientFails >= MAX_TRANSIENT) err.outage = true;   // the bucket, not one frame
      throw err;
    });
    rec = this.inflight[key] = { promise: p, abort: ctrl, key: key, kind: kindName };
    return p;
  };
  // What the ring wants, in priority order: the target's field frame, its direction frame (Animation
  // on), then the ring ahead and behind, field and direction interleaved.
  Overlay.prototype._plan = function (idx) {
    var plan = ringPlan(idx, this.n, this.dir), want = this._wantDir(), out = [];
    for (var i = 0; i < plan.length; i++) { out.push([plan[i], 'field']); if (want) out.push([plan[i], 'dir']); }
    return out;
  };
  // Keep the in-flight set to what the target needs: everything outside the new ring is dropped, and
  // at most MAX_INFLIGHT - 1 older fetches survive beside the target (timeline drags fire many seeks),
  // the target's direction frame first among them.
  Overlay.prototype._trimInflight = function (idx) {
    var keep = {}, plan = this._plan(idx), i, k;
    for (i = 0; i < plan.length; i++) keep[this._key(plan[i][0], plan[i][1])] = true;
    var target = this._key(idx), dtarget = this._wantDir() ? this._key(idx, 'dir') : null, survivors = [], room = MAX_INFLIGHT - 1;
    for (k in this.inflight) if (!keep[k]) { this.inflight[k].abort.abort(); delete this.inflight[k]; } else if (k === dtarget) room--; else if (k !== target) survivors.push(k);
    while (survivors.length > Math.max(0, room)) { k = survivors.shift(); this.inflight[k].abort.abort(); delete this.inflight[k]; }
  };
  // Show frame idx: the label/timeline move to the target at once, the picture and the valid time only
  // when the frame has landed (never an old picture under a new time). Rejects for an unavailable frame
  // (err.unavailable) or an aborted load (AbortError).
  Overlay.prototype._goto = function (idx, sig) {
    var self = this, m = this.manifest, field = this.field, res = this.res;
    this.target = idx; this._syncUI();
    this._trimInflight(idx);
    var p = this._ensure(idx);
    // With Animation on, the picture and its particles change together: the step waits for its direction
    // frame as well (fetched beside the field frame); a missing or failed direction never holds it.
    var pd = this._wantDir() && !this._isUnavailable(idx) ? this._dirReady(idx) : null;
    return (pd ? Promise.all([p, pd]).then(function (both) { return both[0]; }) : p).then(function (frame) {
      if ((sig && sig.aborted) || self.target !== idx || !self.layer || self.manifest !== m || self.field !== field || self.res !== res) throw abortError();
      // the frame already on the map (Retry after an outage, a repeated seek): no redraw, but the same
      // state transition as a fresh landing, or the panel would stay on "Loading"
      if (self.layer._frame !== frame) {
        var half = res === 'half';
        self.layer.setContours(self._contourCfg(), true);                        // (setFrame redraws with it)
        self.layer.setFrame(frame, half ? m.grid_half : m.grid, field, m.fields[field], self._lut(), m.frames[idx]);
      }
      self.frameIndex = idx; self._pendingRestore = null;                     // a frame is on the map: the saved state is spent
      self._persist();
      self._attribute();
      if (!self.last || self.last.state !== 'ready') self.render({ state: 'ready' }); else self._syncUI();
      self._syncFlow(idx);
      self._prefetch();
    });
  };
  // The step the ring is planned around: a pending seek / step target, else the frame on the map.
  Overlay.prototype._anchorIndex = function () { return this.target !== null ? this.target : this.frameIndex; };
  // Keep the ring (current, two ahead, two behind; with the direction frames interleaved while the
  // Animation is on) decoded with at most MAX_INFLIGHT fetches; drop fetches the ring no longer wants
  // (direction, field, resolution or Animation changed). Planned around a PENDING target, so a seek that
  // is still loading is never aborted by a change of Animation or of the direction's resolution.
  Overlay.prototype._prefetch = function () {
    if (this.frameIndex === null || !this.manifest) return;
    var plan = this._plan(this._anchorIndex()), want = {}, i, k;
    for (i = 0; i < plan.length; i++) want[this._key(plan[i][0], plan[i][1])] = true;
    for (k in this.inflight) if (!want[k]) { this.inflight[k].abort.abort(); delete this.inflight[k]; }
    for (i = 0; i < plan.length && Object.keys(this.inflight).length < MAX_INFLIGHT; i++) {
      var idx = plan[i][0], kind = plan[i][1], key = this._key(idx, kind);
      if (this.inflight[key] || this._isUnavailable(idx, kind) || (kind === 'dir' ? this.dcache : this.cache).has(key)) continue;
      if (kind === 'dir') this._ensureDir(idx); else this._ensure(idx).catch(function () {});
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
    this.cache.clear(); this.dcache.clear(); this.unavailable = {}; this.frameIndex = null; this.target = null;
    if (this.flow) this.flow.clearData();
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
      else self._refreshRunLine();                                             // "expected shortly", the weekday after midnight: in place
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
      self._ensureFlow();                                  // the new run may or may not carry direction data
      return self._goto(nearestIndex(m, prevValid), sig).then(function () { if (wasPlaying) self.play(); });
    }).catch(function (err) { self._fail(sig, err); });
  };

  // ---- playback ----
  Overlay.prototype._interval = function () { return 1000 / (BASE_FPS * this.speed); };
  // One tick chain at a time: play() starts a generation; a continuation from an older generation
  // (a frame that was loading when the user paused and played again) never schedules anything.
  Overlay.prototype.play = function () {
    if (this.playing || !this.manifest || this.frameIndex === null) return;
    this.playing = true; this.dir = 1; this.playGen++; this._syncUI(); this._tick(this.playGen); this._persist();
  };
  Overlay.prototype.pause = function () {
    this.playing = false; this.playGen++;
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    this._syncUI(); this._persist();
  };
  // The state a reload restores (with the field the page saved): the valid time on the map, whether it
  // is playing (a visibility pause still counts as playing), and when this was written.
  Overlay.prototype._persist = function () {
    if (!this.field || !this.manifest || this.frameIndex === null || !this.manifest.frames[this.frameIndex]) return;
    save({ field: this.field, t: Date.parse(this.manifest.frames[this.frameIndex].valid_utc),
      playing: !!(this.playing || this.wasPlaying), at: Date.now() });
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
  // Leaving the tab or the page refreshes the save, paused or not: a reload restores what was on the map
  // when the page was left (the 30-min rule counts from then, not from the last frame change).
  Overlay.prototype._bindDocument = function () {
    var self = this;
    this._onVis = function () {
      if (document.hidden) { self.wasPlaying = self.playing; if (self.playing) self.pause(); else self._persist(); }
      else if (self.wasPlaying) { self.wasPlaying = false; self.play(); }
      if (self.flow) { if (document.hidden) self.flow.suspend(); else self.flow.resume(true); }   // shown: rebuilt (the map may have moved)
    };
    this._onHide = function () { self._persist(); };
    document.addEventListener('visibilitychange', this._onVis);
    if (window.addEventListener) window.addEventListener('pagehide', this._onHide);
  };

  // ---- attribution, map events, resolution ----
  // The container's real size: the site resizes #map by script and Leaflet's cached getSize() can lag.
  Overlay.prototype._dims = function () {
    var c = this.map.getContainer(), s = this.map.getSize();
    return { w: c.clientWidth || s.x, h: c.clientHeight || s.y };
  };
  Overlay.prototype._attribution = function () {
    if (!(this.layer && this.layer._clip && this.coast)) return ATTRIBUTION;
    var url = String(this.coast.url + '/LICENSE.txt').replace(/["<>]/g, encodeURIComponent);
    return ATTRIBUTION + ' · <a href="' + url + '" target="_blank" rel="noopener license">GSHHG</a>';
  };
  Overlay.prototype._attribute = function () {
    var text = this._attribution();
    if (this._attributed === text) return;
    var ctl = this.map.attributionControl;
    if (ctl && this._attributed) ctl.removeAttribution(this._attributed);
    this._attributed = text;
    if (ctl) ctl.addAttribution(text);
    this.map.getContainer().classList.add('ov-on');
    this._sizeAttribution();
  };
  // While On the attribution may wrap, but only inside its own box: never over the zoom control.
  Overlay.prototype._sizeAttribution = function () {
    if (this._attributed) this.map.getContainer().style.setProperty('--ov-attr-max', Math.max(120, this._dims().w - 60) + 'px');
  };
  Overlay.prototype._unattribute = function () {
    if (!this._attributed) return;
    if (this.map.attributionControl) this.map.attributionControl.removeAttribution(this._attributed);
    this._attributed = false;
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
      if (compactNow !== wasCompact || self.sheet) self.render(self.last); else { self._syncUI(); self._layoutSheet(); }   // the sheet's cap follows the map height
    });
  };
  Overlay.prototype._checkRes = function () {
    if (!this.layer || !this.layer.hasFrame() || !this.manifest || this.frameIndex === null || !this.field) return;
    var z = this.map.getZoom(), w = this._dims().w, want = this.res, d = this._dirDef(), dwant = d && this.flow ? dirRes(d, z, this.dres) : this.dres;
    if (this.res === 'full' && wantHalf(z, w, this.field)) want = 'half';
    else if (this.res === 'half' && wantFull(z, w, this.field)) want = 'full';
    if (want === this.res && dwant === this.dres) return;
    var self = this, idx = this.target !== null ? this.target : this.frameIndex;
    this.dres = dwant;
    if (want === this.res) {                                                 // only the direction's resolution changed
      this._prefetch();                                                      // drops the other resolution's direction fetches
      this._syncFlow(this.frameIndex);                                       // the same step's direction stays until the new one lands
      return;
    }
    this.res = want;
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
    this.field = null;                                     // first: the pause below must not save the layer as still on
    this.pause();
    this.abortAll();
    if (this.runTimer) { clearInterval(this.runTimer); this.runTimer = null; }
    if (this._onVis) { document.removeEventListener('visibilitychange', this._onVis); this._onVis = null; }
    if (this._onHide) { if (window.removeEventListener) window.removeEventListener('pagehide', this._onHide); this._onHide = null; }
    this._pendingRestore = null;
    if (this.layer) { this.map.removeLayer(this.layer); this.layer = null; }
    if (this.coast) this.coast.abortAll();                 // the decoded coastlines stay for the next On
    if (this.flow) { this.flow.detach(); this.flow = null; }
    this._unattribute();
    this._unbindReadout();
    this._listeners.forEach(function (l) { self.map.off(l[0], l[1]); }); this._listeners = [];
    this._removeSheet();
    this.cache.clear(); this.dcache.clear(); this.unavailable = {}; this.target = null; this.wasPlaying = false; this.ui = null;
    this.field = null; this.frameIndex = null; this.res = null; this.dres = null; this.last = null; this.collapsed = undefined;
    clear(this.opts.panel);
  };
  Overlay.prototype.setOpacity = function (v) {
    v = Math.max(0.2, Math.min(1, v));
    this.opacity = v; save({ opacity: v });
    if (this.layer) this.layer.setOpacity(v);
    if (this.flow) this.flow.refresh();                    // the particle contrast reads the layer's opacity
  };
  // The contour settings for the current field and site unit, or null.
  Overlay.prototype._contourCfg = function () {
    var c = CONTOURS[this.field];
    if (!this.contours || !c) return null;
    var unit = this.opts.getUnit() === 'Metric' ? 'Metric' : 'US';
    return { step: c[unit], per: unitOf(this.field, unit).f(1) };
  };
  // The manifest field carrying this layer's direction (pdir under the wave fields, wdir under wind)
  // when the run publishes it as a circular FROM field; null otherwise (the Animation box is disabled).
  Overlay.prototype._dirDef = function () {
    var name = this.field && DIR_FIELDS[this.field], m = this.manifest, f = m && name ? m.fields[name] : null;
    if (!dirFieldOk(f)) return null;
    if (this._dirGridsFor !== m || this._dirGridsField !== f) { this._dirGridsFor = m; this._dirGridsField = f; this._dirGridsOk = dirGridsOk(m, f); }
    return this._dirGridsOk ? f : null;
  };
  Overlay.prototype.animAvailable = function () { return !!this._dirDef(); };
  Overlay.prototype._wantDir = function () { return !!(this.anim && this.layer && this._dirDef() && !reducedMotion()); };   // reduced motion: nothing animates, nothing is fetched
  // The animator exists exactly while the box is ticked and the run has this field's direction; it
  // follows the layer and the field, and its direction resolution follows the zoom.
  Overlay.prototype._ensureFlow = function () {
    if (!this._wantDir()) { if (this.flow) { this.flow.detach(); this.flow = null; } return; }
    if (!this.flow) { this.flow = new FlowAnimator(this.map, this.layer); this.flow.attach(); }
    this.flow.setLayer(this.layer);
    this.flow.setField(this.field);
    this.dres = dirRes(this._dirDef(), this.map.getZoom(), this.dres);
  };
  // The direction frame for idx, delivered to the animator only if the LAYER shows that step of that run
  // (its frame entry; a field switch keeps frameIndex while the layer is empty), at this field and
  // resolution; a failure is swallowed: the field plays on without its particles.
  Overlay.prototype._ensureDir = function (idx) {
    var self = this, m = this.manifest, field = this.field, dres = this.dres;
    return this._ensure(idx, 'dir').then(function (frame) {
      if (self.flow && self.layer && self.layer.hasFrame() && self.layer.entry === m.frames[idx] && self.manifest === m && self.field === field && self.dres === dres && self._wantDir()) {
        self.flow.setData(frame, dres === 'half' ? m.grid_half : m.grid, m.frames[idx]);
      }
    }).catch(function (err) {                                                // a missing direction frame: the field plays on without arrows
      if (err && !err.unavailable && err.name !== 'AbortError' && typeof console !== 'undefined' && console.warn) console.warn('overlay animation', err);
    });
  };
  // Settles when the direction frame of idx is decoded, unavailable, or its fetch has failed or been
  // aborted (at the abort itself: a decode that cannot be cancelled must not hold the step); never
  // rejects. What a step waits for beside its field frame.
  Overlay.prototype._dirReady = function (idx) {
    var key = this._key(idx, 'dir');
    if (this.dcache.has(key) || this._isUnavailable(idx, 'dir')) return Promise.resolve();
    if (!this.inflight[key]) this._startDir(idx);
    var rec = this.inflight[key];
    if (!rec) return Promise.resolve();
    return new Promise(function (resolve) {
      var done = function () { resolve(); };
      rec.promise.then(done, done);
      if (rec.abort.signal.aborted) done(); else rec.abort.signal.addEventListener('abort', done);
    });
  };
  // Start the direction frame of idx unless it is cached, in flight or unavailable. Ranked by the plan
  // around the PENDING target (else the frame on the map): a full in-flight set gives up its
  // lowest-ranked ring fetch, never the target's own field or direction fetch.
  Overlay.prototype._startDir = function (idx) {
    var key = this._key(idx, 'dir');
    if (this.dcache.has(key) || this.inflight[key] || this._isUnavailable(idx, 'dir')) return;
    if (Object.keys(this.inflight).length >= MAX_INFLIGHT) {
      var plan = this._plan(this._anchorIndex()), rank = {}, i, k, worst = null, worstRank = -1;
      for (i = 0; i < plan.length; i++) rank[this._key(plan[i][0], plan[i][1])] = i;
      for (k in this.inflight) { var r = rank[k] === undefined ? 1e9 : rank[k]; if (r > worstRank) { worstRank = r; worst = k; } }
      if (worst === null || worstRank < 2) return;                          // only the target's own fetches are in flight (never evicted)
      this.inflight[worst].abort.abort(); delete this.inflight[worst];
    }
    this._ensureDir(idx);
  };
  // The frame idx is on the map: the animator shows its direction if decoded; a direction of ANOTHER step
  // is cleared (never an old direction under a new time), the same step's (another resolution) stays until
  // the new one lands. The fetch is started only when no other seek / step target is pending: that
  // target's own frames come first, and it will bring its direction when it lands.
  Overlay.prototype._syncFlow = function (idx) {
    if (!this.flow) return;
    var m = this.manifest, entry = m.frames[idx];
    if (!this._wantDir() || !this.layer.hasFrame() || this.layer.entry !== entry) { if (this.flow.entry !== entry) this.flow.clearData(); return; }
    var hit = this.dcache.get(this._key(idx, 'dir'));
    if (hit) { this.flow.setData(hit, this.dres === 'half' ? m.grid_half : m.grid, entry); return; }
    if (this.flow.entry !== entry) this.flow.clearData();
    if (this.target === null || this.target === idx) this._startDir(idx);
  };
  Overlay.prototype.setAnim = function (on) {
    this.anim = !!on; save({ anim: this.anim });
    if (!this.manifest || !this.field || !this.layer) return;
    this._ensureFlow();
    if (!this.anim) {                                      // the direction downloads stop at once, before or after the first frame
      for (var k in this.inflight) if (this.inflight[k].kind === 'dir') { this.inflight[k].abort.abort(); delete this.inflight[k]; }
      this.dcache.clear();
    }
    if (this.flow && this.frameIndex !== null) this._syncFlow(this.frameIndex);
    this._prefetch();                                      // starts or drops the direction fetches (planned around a pending target)
  };
  Overlay.prototype.setContours = function (on) {
    this.contours = !!on; save({ contours: this.contours });
    if (this.layer) this.layer.setContours(this._contourCfg());
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
  Overlay.prototype.refresh = function () {
    if (this.readout) this.readout.hidden = true;
    if (this.last && this.layer) this.render(this.last);
    if (this.layer && this.contours) this.layer.setContours(this._contourCfg());   // the interval follows the site unit
  };
  // The run line's text, in place (no panel rebuild: focus and the sheet's scroll stay).
  Overlay.prototype._refreshRunLine = function () {
    var ui = this.ui;
    this._runLineAt = Date.now();
    if (!ui || !ui.runText || !this.manifest) return;
    var t = runTimes(this.manifest, Date.now(), !!this.newerRun);
    this._runStatus = t && t.status;
    ui.runText.nodeValue = ui.runLabel + ' (UTC)' + (t ? ' · ' + t.text : '') + ui.runTail;
  };
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
    var rt0 = runTimes(m, Date.now(), !!this.newerRun); this._runStatus = rt0 && rt0.status;
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
    // The timeline runs in hours (hourly frames to +120 h, then 3-hourly), so its thumb sits where the hour is; a value
    // between two frames snaps in the direction of travel (frameAtHour).
    var hrs = ui.hours = m.frames.map(function (fr) { return self._hours(fr); });
    var slider = mk('input', 'ov-timeline'); slider.type = 'range'; slider.min = String(hrs[0]); slider.max = String(hrs[hrs.length - 1]); slider.step = '1';
    slider.setAttribute('aria-label', 'Forecast hour');
    var tl = ui.timeline = new TimelineState(), shownIdx = this.target !== null ? this.target : this.frameIndex;
    tl.shown = shownIdx !== null ? hrs[shownIdx] : null;
    slider.addEventListener('pointerdown', function () { tl.start(); });
    ['pointerup', 'pointercancel', 'lostpointercapture', 'change', 'keydown', 'blur'].forEach(function (ev) { slider.addEventListener(ev, function () { tl.end(); }); });
    slider.addEventListener('input', function () {
      var idx = tl.pick(hrs, parseInt(slider.value, 10));
      slider.value = String(hrs[idx]); slider.setAttribute('aria-valuetext', '+' + hrs[idx] + ' h');
      self.seek(idx);
    });
    body.appendChild(slider); ui.slider = slider;
    var valid = mk('div', 'ov-meta'); body.appendChild(valid); ui.valid = valid;
    var runLine = mk('div', 'ov-meta'); runLine.appendChild(mk('b', null, 'Run: '));
    var runLabel = m.run_utc.replace('T', ' ').replace(':00:00Z', 'Z'), pc = typeof this.opts.pageCycle === 'function' ? this.opts.pageCycle() : this.opts.pageCycle;
    ui.runTail = pc && pc.model === 'SWAN' ? ' — the forecast table is a PacIOOS SWAN run' : pc && pc.run && pc.run !== m.run ? ' — the forecast table is on run ' + pc.run : '';
    ui.runLabel = runLabel;
    ui.runText = runLine.appendChild(document.createTextNode(''));
    this._refreshRunLine();
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
    var lut = legendBar(field), ctx = cv.getContext('2d'), im = ctx.createImageData(256, 1);        // the bar is laid out by legend position
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
    lab.appendChild(rng); row.appendChild(lab);
    if (CONTOURS[field]) {
      var cl = mk('label', 'ov-check'), cb = mk('input'); cb.type = 'checkbox'; cb.checked = this.contours;
      var every = CONTOURS[field][unit === 'Metric' ? 'Metric' : 'US'], ul = unitOf(field, unit).label;
      cb.setAttribute('aria-label', 'Contours, every ' + fmtTick(every) + ' ' + ul + ' (' + fmtTick(2 * every) + ' ' + ul + ' below zoom 4)');
      cb.addEventListener('change', function () { self.setContours(cb.checked); });
      cl.appendChild(cb); cl.appendChild(document.createTextNode(' Contours')); row.appendChild(cl);
    }
    // Animation: swell arrows (wave height, period) or wind particles; disabled on a run without direction data
    var avail = this.animAvailable(), reduced = reducedMotion(), al = mk('label', 'ov-check'), ab = mk('input'); ab.type = 'checkbox';
    ab.checked = this.anim && avail && !reduced; ab.disabled = !avail || reduced;
    ab.setAttribute('aria-label', 'Animation: ' + (field === 'wind' ? 'wind particles' : 'swell particles'));
    if (!avail) al.title = 'This run has no direction data'; else if (reduced) al.title = 'Off under your reduced-motion setting';
    ab.addEventListener('change', function () { self.setAnim(ab.checked); });
    al.appendChild(ab); al.appendChild(document.createTextNode(' Animation')); row.appendChild(al);
    body.appendChild(row);
    // On phones the settings row sits right under the timeline, inside the short details box (it was below
    // the fold under the run line and the legend); the valid time is in the header line there.
    if (compact) body.insertBefore(row, ui.valid);
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
    // The sheet (phones): its header line carries the forecast hour FIRST (a narrow screen clips the end, never
    // the hour), then the valid time, and a pending seek's hint ("loading", "unavailable"), which the short
    // details box cannot show; the field's name is in the select above the map.
    if (ui.compact) {
      var th = pending ? this._hours(this.manifest.frames[this.target]) : null;
      ui.title.textContent = hours === null ? '…' : pending
        ? '+' + hours + ' h → +' + th + ' h ' + (this._isUnavailable(this.target) ? 'unavailable' : 'loading…')
        : '+' + hours + ' h · ' + validLocal;
    } else {
      ui.title.textContent = ui.collapsed ? ui.label + ' · ' + validLocal + (hours === null ? '' : ' (+' + hours + ' h)')
        : ui.label + ' — ' + ui.modelName;
    }
    if (ui.valid) {
      clear(ui.valid);
      ui.valid.appendChild(mk('b', null, 'Valid: '));
      ui.valid.appendChild(document.createTextNode(validLocal + (hours === null ? '' : ' (+' + hours + ' h)')));
      if (pending) {
        var t = this.manifest.frames[this.target], hint = this._isUnavailable(this.target) ? ' — frame +' + this._hours(t) + ' h is unavailable' : ' — loading +' + this._hours(t) + ' h…';
        ui.valid.appendChild(mk('span', 'ov-hint', hint));
      }
    }
    var si = this.target !== null ? this.target : this.frameIndex;
    if (ui.slider && ui.hours && si !== null && !(ui.timeline && ui.timeline.dragging)) {
      ui.slider.value = String(ui.hours[si]); ui.slider.setAttribute('aria-valuetext', '+' + ui.hours[si] + ' h');
      if (ui.timeline) ui.timeline.shown = ui.hours[si];
    }
    if (ui.runText && Date.now() - (this._runLineAt || 0) > 60000) this._refreshRunLine();
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
    _internals: { contourTile: contourTile, smoothBlock: smoothBlock, jumpAt: jumpAt, CONTOURS: CONTOURS, CONTOUR_JUMP: CONTOUR_JUMP,
      CONTOUR_BLOCK: CONTOUR_BLOCK, CONTOUR_PROFILE: CONTOUR_PROFILE, contourProfile: contourProfile, saved: saved, legendBar: legendBar, buildRamp: buildRamp, buildLut: buildLut, legendPos: legendPos, legendInv: legendInv, KNOTS: KNOTS, TICKS: TICKS, unitOf: unitOf, ModelGridLayer: ModelGridLayer, Overlay: Overlay, RAMPS: RAMPS,
      frameKey: frameKey, pickFrame: pickFrame, validateManifest: validateManifest, validateGrid: validateGrid,
      wantHalf: wantHalf, wantFull: wantFull, legendTicks: legendTicks, tilePixelLatLng: tilePixelLatLng,
      parsePng: parsePng, unfilter: unfilter, decodePngGrey: decodePngGrey,
      forwardPixel: forwardPixel, snapToPixel: snapToPixel, pixelOf: pixelOf, pad3: pad3,
      ringPlan: ringPlan, nextAvailable: nextAvailable, nearestIndex: nearestIndex, restoreIndex: restoreIndex, SESSION_KEY: SESSION_KEY, FrameCache: FrameCache, failureKind: failureKind, SPEEDS: SPEEDS, BASE_FPS: BASE_FPS,
      MAX_DECODED: MAX_DECODED, MAX_INFLIGHT: MAX_INFLIGHT,
      worldXY: worldXY, decodeCoast: decodeCoast, tileBox: tileBox, coastCellsForTile: coastCellsForTile, withinCell: withinCell, landPathsForTile: landPathsForTile,
      rasteriseScanline: rasteriseScanline, rasterise: rasterise, maskState: maskState, composeTile: composeTile, CoastStore: CoastStore, coastStore: coastStore,
      LAND_ALL: LAND_ALL, CLIP_FIELDS: CLIP_FIELDS, LAND_READOUT: LAND_READOUT, MAX_CHUNK_BYTES: MAX_CHUNK_BYTES, MAX_COAST_INFLIGHT: MAX_COAST_INFLIGHT,
      validCoastIndex: validCoastIndex,
      FlowAnimator: FlowAnimator, sampleRow: sampleRow,
      vectorNodes: vectorNodes, flowField: flowField, FLOW_SPEED: FLOW_SPEED, dirFieldOk: dirFieldOk, dirRes: dirRes, DIR_FIELDS: DIR_FIELDS, PARTICLE_LIFE_MS: PARTICLE_LIFE_MS,
      latOfWorldY: latOfWorldY, lngOfWorldX: lngOfWorldX, PARTICLE_PX_PER_S: PARTICLE_PX_PER_S, PARTICLE_MIN: PARTICLE_MIN, PARTICLE_MAX: PARTICLE_MAX,
      ANIM_BUDGET_MS: ANIM_BUDGET_MS, TRAIL_POINTS: TRAIL_POINTS, TRAIL_EVERY_MS: TRAIL_EVERY_MS, dirGridsOk: dirGridsOk,
      frameAtHour: frameAtHour, localClock: localClock, runTimes: runTimes, TimelineState: TimelineState }
  };
})();
