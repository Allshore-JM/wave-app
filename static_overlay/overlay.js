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
  var STALE_AFTER_S = 9 * 3600;               // pointer older than this -> "stale" banner (the GFS value; per model below)
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
  // The models the panel can show (plan section 43). Each publishes the same frame layout under its own bucket prefix
  // (the GFS job and the ECMWF job share the pipeline); the page says which are offered (opts.models, from the
  // MODEL_OVERLAY_MODELS setting), in order, the first being the default. cycleMs: a new run every so often ("Next
  // Update"); staleS: the stale banner (ECMWF's 00Z / 12Z cycles land 12 h apart); table: how the "forecast table is on
  // run X" note reads (the table is always GFS-Wave / SWAN); attribution: <= 29 visible characters (the GFS rule above).
  // The ECMWF credit is the short form; the title carries the notice; the manifest's model block has the full text.
  var MODEL_DEFS = {
    gfswave: { prefix: 'gfswave/0p25/v1', label: 'GFS-Wave (WAVEWATCH III)', short: 'GFS', cycleMs: 6 * 3.6e6, staleS: 9 * 3600, table: 'cycle', attribution: ATTRIBUTION },
    // the credit's "ECMWF" opens the site's notice (/notice/ecmwf: copyright, source, licence, ECMWF's disclaimer, what
    // Allshore Surf changed; owner, G30-4: a notice, linked from the credit)
    ecmwf: { prefix: 'ecmwf/0p25/v1', label: 'ECMWF IFS wave (ecWAM)', short: 'ECMWF', cycleMs: 12 * 3.6e6, staleS: 15 * 3600, table: 'model',
      attribution: 'Overlay: <a href="/notice/ecmwf" target="_blank" rel="noopener" ' +
        'title="Based on data and products of ECMWF (CC BY 4.0), modified by Allshore Surf: quantised, calm water and sea ice left empty, coast-filled and rendered. ECMWF does not endorse this product.">ECMWF</a> ' +
        '<a href="https://creativecommons.org/licenses/by/4.0/" target="_blank" rel="noopener license">CC BY 4.0</a>' }
  };
  var DEFAULT_MODEL = 'gfswave';
  // A model this file knows (own keys only: never 'constructor' or 'toString'; G30-9).
  function hasModel(key) { return typeof key === 'string' && Object.prototype.hasOwnProperty.call(MODEL_DEFS, key); }
  // The models offered, in the page's order: known keys only, deduplicated, never empty (the default alone).
  function modelList(list) {
    var out = [], i;
    if (Array.isArray(list)) for (i = 0; i < list.length; i++) if (hasModel(list[i]) && out.indexOf(list[i]) < 0) out.push(list[i]);
    return out.length ? out : [DEFAULT_MODEL];
  }
  // The model a saved key names, when it is offered; else the first offered.
  function modelOf(key, list) { return list.indexOf(key) >= 0 ? key : list[0]; }
  // Colour stops over the LEGEND POSITION (0..1). For wave height the position is not linear in the
  // value (KNOTS below): 0-3 m takes about half of the scale, with a hue change every 0.5-1 m.
  var RAMPS = {
    // sea surface temperature over -2..35 C (plan section 44; provisional until the owner's pick from the preview sheet)
    sst:  [[0,'#312e81'],[0.14,'#2563eb'],[0.3,'#06b6d4'],[0.42,'#14b8a6'],[0.52,'#22c55e'],[0.62,'#eab308'],[0.72,'#f97316'],
           [0.82,'#dc2626'],[0.92,'#c026d3'],[1,'#fbcfe8']],
    hs:   [[0,'#1d3a8a'],[0.08,'#2563eb'],[0.17,'#0ea5e9'],[0.25,'#22d3ee'],[0.33,'#34d399'],[0.40,'#a3e635'],[0.47,'#facc15'],
           [0.58,'#fb923c'],[0.72,'#ef4444'],[0.87,'#c026d3'],[1,'#f5d0fe']],
    tp:   [[0,'#2a1f7a'],[0.25,'#2e7ed8'],[0.5,'#38c9a8'],[0.7,'#c8e63c'],[0.85,'#f7a52b'],[1,'#e8321f']],
    // wind: blue at the calm end (never white: it covers land), a hue change every ~2 m/s up to ~20 m/s
    // (0 2 4 6 8 10 12 14 16 18 21 25 m/s over the 0-30.9 m/s legend), purple then pale mauve at the top
    wind: [[0,'#6271b7'],[0.0648,'#3961a0'],[0.1296,'#4a94a9'],[0.1944,'#4d8d7b'],[0.2592,'#53a553'],[0.324,'#359f35'],
           [0.3888,'#a79d51'],[0.4536,'#9f7f3a'],[0.5184,'#a16c5c'],[0.5832,'#813a4e'],[0.6804,'#af5088'],[0.81,'#754a93'],[1,'#d9c2d9']]
  };
  // Under the wind overlay the site's satellite imagery (earth tones) gives way to a neutral relief map
  // with a flat light sea; the field is multiplied into it (the sea shows the colour itself, land the
  // colour shaded by the terrain) and the coastlines are drawn into the wind tiles. The imagery stays for
  // no overlay, wave height and peak period (owner, 2026-09-26).
  var WIND_LOOK = {
    base: 'https://server.arcgisonline.com/ArcGIS/rest/services/Elevation/World_Hillshade/MapServer/tile/{z}/{y}/{x}',
    attribution: '&copy; Esri & contributors',
    maxNativeZoom: 16,                                       // Esri's relief ends at zoom 16 (a flat placeholder past it): closer zooms scale its tiles up
    blend: 'multiply',                                       // the model pane over the relief (the particle pane is not blended)
    ink: [34, 38, 46], gain: 1.4                             // the coastline's colour and strength (coastEdges)
  };
  // Fixed opacity per field (owner, 2026-09-26: no slider): the waves over the imagery, the wind multiplied into the relief.
  var FIXED_OPACITY = { hs: 0.65, tp: 0.65, wind: 1, sst: 0.75 };
  // Leaflet fires a tile layer's 'load' when its last tile STARTS its 200-ms fade-in (G14 P1-1: removing the old
  // basemap then showed the bare map for ~200 ms): the old one leaves this long after.
  var BASE_FADE_MS = 300;
  var RELIEF_RETRY_MS = 10 * 60 * 1000;                     // a relief that loaded no tile is tried again after this
  // Value (SI units) -> legend position knots, piecewise linear, written for a legend from 0 to the last
  // knot (12 m; legendPos stretches them to any other legend); fields without knots are linear over the
  // legend range. The tiles stay linear in value (composeTile indexes a 256-entry LUT over the legend
  // range); only the colour each value gets, and where it sits on the legend, follow the knots.
  var KNOTS = {
    hs: [[0, 0], [0.5, 0.08], [1, 0.17], [2, 0.33], [3, 0.47], [4, 0.58], [6, 0.72], [9, 0.87], [12, 1]]
  };
  // Wider scales chosen by the manifest's legend (plan section 27): a run whose legend is exactly [0, top] uses
  // these knots, colours and ticks; any other legend uses KNOTS / RAMPS / TICKS (stretched to it). The wave-height
  // scale to 60 ft (18.288 m) keeps the 0-30 ft colours at the same values as the 40-ft scale and spends the top
  // on 40 / 50 / 60 ft; the frames themselves store up to 75 ft (the job's encode range).
  var SCALES = {
    hs: { top: 18.288,
          knots: [[0, 0], [0.5, 0.075], [1, 0.155], [2, 0.30], [3, 0.43], [4, 0.53], [6, 0.655], [9, 0.77], [12, 0.86], [15.24, 0.93], [18.288, 1]],
          ramp: [[0,'#1d3a8a'],[0.075,'#2563eb'],[0.155,'#0ea5e9'],[0.2275,'#22d3ee'],[0.30,'#34d399'],[0.365,'#a3e635'],[0.43,'#facc15'],
                 [0.53,'#fb923c'],[0.655,'#ef4444'],[0.77,'#c026d3'],[0.86,'#f5d0fe'],[0.93,'#be185d'],[1,'#4c1d95']],   // owner's pick C3: 40 ft pale pink, 50 ft rose, 60 ft deep purple
          ticks: { US: [0, 3, 6, 10, 15, 20, 30], Metric: [0, 1, 2, 3, 4, 6, 9] } }
  };
  function scaleFor(field, legend) {
    var s = SCALES[field];
    return s && legend && legend[0] === 0 && legend[1] === s.top ? s : null;
  }
  function knotsOf(field, legend) { var s = scaleFor(field, legend); return s ? s.knots : KNOTS[field]; }
  function rampOf(field, legend) { var s = scaleFor(field, legend); return s ? s.ramp : RAMPS[field]; }
  // Legend tick values in DISPLAY units per field and site unit; the legend top is added as "N+".
  // No numeric tick above ~80 % of the bar: it would collide with the right-aligned top label.
  var TICKS = {
    'hs|US': [0, 3, 6, 10, 15, 20], 'hs|Metric': [0, 1, 2, 3, 4, 6],
    'tp|US': [4, 8, 12, 16], 'tp|Metric': [4, 8, 12, 16],
    'wind|US': [0, 20, 40], 'wind|Metric': [0, 25, 50, 75],
    'sst|US': [40, 50, 60, 70, 80], 'sst|Metric': [0, 5, 10, 15, 20, 25]
  };
  // ---- sea surface temperature (plan section 44) ----
  // An OBSERVATION beside the model fields: the newest NASA MUR day, published by tools/model_frames/sst.py as a tiered
  // store (sst/v1/latest.json -> manifest -> one 0.25-degree frame on the model grid + 0.05- and 0.01-degree cells with
  // a one-node apron and a per-cell code range). The layer samples it through a pluggable SOURCE (SstStore) that writes
  // fractional codes over this one FIXED range into the layer's code buffer, so the tile composer, the land clip, the
  // readout and the contours work unchanged. deg C throughout (the site unit only at the legend and the readout).
  var SST_FDEF = { lo: -2, hi: 35, legend: [-2, 35], units: 'degC', interpolation: 'bilinear' };
  var SST_PREFIX = 'sst/v1';
  // The chunk tier drawn by tile zoom: below 5 the 0.25-degree frame (a 1-km node is a fraction of a pixel there), 5-7
  // the 0.05-degree cells, 8 and closer the 0.01-degree cells (Leaflet rounds 7.5 up). A tile whose cells are not all
  // in hand yet draws from the next coarser tier (the frame always is) and is redrawn when they land.
  var SST_TIER_ZOOMS = [5, 8];
  function sstTierFor(z, available) {
    var t = z >= SST_TIER_ZOOMS[1] ? 2 : z >= SST_TIER_ZOOMS[0] ? 1 : 0;
    return t < available ? t : available;
  }

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
    var k = knotsOf(field, legend);
    if (k) return through(k, knotsFit(k, legend) ? v : (v - legend[0]) / (legend[1] - legend[0]) * k[k.length - 1][0], 0, 1);
    return Math.max(0, Math.min(1, (v - legend[0]) / (legend[1] - legend[0])));
  }
  function legendInv(field, legend, p) {
    var k = knotsOf(field, legend);
    if (k) { var x = through(k, p, 1, 0); return knotsFit(k, legend) ? x : legend[0] + x / k[k.length - 1][0] * (legend[1] - legend[0]); }
    return legend[0] + Math.max(0, Math.min(1, p)) * (legend[1] - legend[0]);
  }
  // The tiles' lookup: 256 entries LINEAR IN VALUE over the legend range (what composeTile indexes),
  // each coloured at that value's legend position. Linear fields give exactly buildRamp(stops).
  function buildLut(field, legend) {
    var stops = rampOf(field, legend), out = new Uint8ClampedArray(256 * 3);
    for (var i = 0; i < 256; i++) rampAt(stops, knotsOf(field, legend) ? legendPos(field, legend, legend[0] + i / 255 * (legend[1] - legend[0])) : i / 255, out, i * 3);
    return out;
  }
  // The legend bar: 256 colours by LEGEND POSITION (buildLut is by value; for knotted fields they differ).
  function legendBar(field, legend) { return buildRamp(rampOf(field, legend)); }
  function pad3(n) { return (n < 10 ? '00' : n < 100 ? '0' : '') + n; }
  function clear(el) { if (el) while (el.firstChild) el.removeChild(el.firstChild); }
  function mk(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text !== undefined) e.textContent = text; return e; }

  // ---- units (site preference: 'US' | 'Metric') ----
  function unitOf(field, unit) {
    if (field === 'hs') return unit === 'Metric' ? { label: 'm', f: function (v) { return v; }, d: 1 } : { label: 'ft', f: function (v) { return v * 3.28084; }, d: 1 };
    if (field === 'tp') return { label: 's', f: function (v) { return v; }, d: 1 };
    if (field === 'sst') {                                 // an affine unit: scale / offset / inv beside f (ticks, contours)
      if (unit === 'Metric') return { label: '°C', f: function (v) { return v; }, inv: function (v) { return v; }, scale: 1, offset: 0, d: 1 };
      return { label: '°F', f: function (v) { return v * 1.8 + 32; }, inv: function (v) { return (v - 32) / 1.8; }, scale: 1.8, offset: 32, d: 1 };
    }
    return unit === 'Metric' ? { label: 'km/h', f: function (v) { return v * 3.6; }, d: 0 } : { label: 'mph', f: function (v) { return v * 2.23694; }, d: 0 };
  }
  function fmtTick(v) { var r = Math.round(v * 10) / 10; return String(Math.abs(r - Math.round(r)) < 1e-9 ? Math.round(r) : r); }
  function legendTicks(field, fdef, unit) {
    var u = unitOf(field, unit), hi = u.f(fdef.legend[1]), per = u.f(1);          // display = SI * per
    var sc = scaleFor(field, fdef.legend), um = unit === 'Metric' ? 'Metric' : 'US';
    var vals = (sc && sc.ticks && sc.ticks[um]) || TICKS[field + '|' + um] || [fdef.legend[0]];
    var out = vals.map(function (v, i) {
      var below = i === 0 && fdef.lo < fdef.legend[0] - 1e-9;          // values under the legend floor exist (Tp)
      return { pos: legendPos(field, fdef.legend, u.inv ? u.inv(v) : v / per), label: (below ? '≤' : '') + fmtTick(v) };
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
  var CLIP_FIELDS = { hs: true, tp: true, wind: false, sst: true };
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
  // The cellDeg-degree cells a tile touches, as "lat0_lon0" names (south-west corners on multiples of cellDeg from 0:
  // the coast tier-1 cells, and the SST cells, which the job names by the same rule). padPx: the tile box grown by so
  // many tile pixels on every side (the contour lattice samples one step outside the tile).
  function coastCellsForTile(coords, cellDeg) { return cellsForTile(coords, cellDeg, 0); }
  function cellsForTile(coords, cellDeg, padPx) {
    var b = tileBox(coords), pad = (padPx || 0) / b.n, lonW = (b.x0 - pad) / TILE * 360 - 180, lonE = (b.x1 + pad) / TILE * 360 - 180;
    function lat(y) { return Math.atan(Math.sinh(Math.PI - 2 * Math.PI * y / TILE)) * 180 / Math.PI; }
    var latN = lat(Math.max(0, b.y0 - pad)), latS = lat(Math.min(TILE, b.y1 + pad)), eps = 1e-9, out = [];
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
  // Fields drawn over land whose tiles carry the coastline as a thin line (no clipping).
  var OUTLINE_FIELDS = { wind: true };
  // The coastline of a tile's land mask (anti-aliased, 0..255): the mask's gradient (central differences,
  // one-sided on the tile's border) is large only where the mask crosses from sea to land, so its magnitude
  // times gain / 255 is the line's ink (about 1.5 px wide in all). Computed once per mask; a sparse list.
  function coastEdges(mask, gain) {
    var S = TILE, n = 0, idx = new Int32Array(S * S), a = new Float32Array(S * S), k = gain / 255;
    for (var y = 0; y < S; y++) {
      var ym = y > 0 ? y - 1 : y, yp = y < S - 1 ? y + 1 : y, o = y * S, om = ym * S, op = yp * S, dy = yp - ym;
      for (var x = 0; x < S; x++) {
        var xm = x > 0 ? x - 1 : x, xp = x < S - 1 ? x + 1 : x;
        var gx = (mask[o + xp] - mask[o + xm]) / (xp - xm), gy = (mask[op + x] - mask[om + x]) / dy;
        if (!gx && !gy) continue;
        var e = Math.sqrt(gx * gx + gy * gy) * k;
        if (e < 0.03) continue;
        idx[n] = o + x; a[n] = e > 1 ? 1 : e; n++;
      }
    }
    return { n: n, idx: idx.slice(0, n), a: a.slice(0, n) };
  }
  // The coastline into a composed tile: ink over the field's colour (a pixel without data takes the ink).
  function applyEdges(edges, d, ink) {
    for (var j = 0; j < edges.n; j++) {
      var k = edges.idx[j] * 4, e = edges.a[j], al = e * 255;
      if (!d[k + 3]) { d[k] = ink[0]; d[k + 1] = ink[1]; d[k + 2] = ink[2]; d[k + 3] = al; continue; }
      var f = 1 - e;
      d[k] = d[k] * f + ink[0] * e; d[k + 1] = d[k + 1] * f + ink[1] * e; d[k + 2] = d[k + 2] * f + ink[2] * e;
      if (d[k + 3] < al) d[k + 3] = al;
    }
    return d;
  }
  // ---- contour lines (pure; tested in Node) ----
  // Fields that can carry contours, and the interval in DISPLAY units per site unit (owner, 2026-09-25):
  // wave height every 2 ft / 0.5 m, peak period every 2 s; doubled below tile zoom 4.
  var CONTOURS = { hs: { US: 2, Metric: 0.5 }, tp: { US: 2, Metric: 2 }, sst: { US: 2, Metric: 1 } };   // isotherms every 2 F / 1 C (owner, 2026-10-16)
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

  // ---- the sea surface temperature store (plan section 44) ----
  // One per bucket root per page (like the coast stores): the decoded 0.25-degree frame, the manifest and the cell
  // LRU survive a field switch (idle) and Off (abortAll keeps the frame and the manifest, drops the cells).
  var SST_STORES = {};
  function sstStore(root) { return SST_STORES[root] || (SST_STORES[root] = new SstStore(root)); }
  var MAX_SST_INFLIGHT = 4;                                // cell downloads at once (the frames keep their own budget)
  var MAX_SST_CHUNK_BYTES = 24 * 1024 * 1024;              // decoded cells kept (LRU; the cells the tiles on the map need are never evicted)
  var MAX_SST_MANIFEST_BYTES = 2 * 1024 * 1024;            // a day's manifest is ~100 KB (one line per published cell)
  var SST_PAD_PX = 4;                                      // cells asked for around a tile: the contour lattice's step (2 or 4 px)
  var SST_MAX_CELL_PX = 2048;
  function sstKey(k) { return typeof k === 'string' && k.length > 0 && k.length <= 200 && /^[A-Za-z0-9][A-Za-z0-9_./-]*$/.test(k) && k.indexOf('..') < 0; }
  // The pointer the client can act on.
  function validSstPointer(p) { return !!(p && p.format === 'sst-v1' && p.complete === true && typeof p.run === 'string' && /^\d{8}$/.test(p.run) && sstKey(p.manifest)); }
  // The manifest the client can act on: the fixed range this file draws, a tier-0 frame on a grid validateGrid accepts
  // (checked against the decoded frame), up to four chunk tiers whose geometry closes (px nodes = cell / res) with a
  // one-node apron, every listed cell a [lo, hi, bytes] triple with lo < hi.
  function validSstManifest(m) {
    if (!m || m.format !== 'sst-v1' || m.complete !== true || typeof m.run !== 'string' || !/^\d{8}$/.test(m.run) || typeof m.day !== 'string' ||
      !Array.isArray(m.range) || m.range.length !== 2 || m.range[0] !== SST_FDEF.lo || m.range[1] !== SST_FDEF.hi || m.units !== 'degC' ||
      !m.tier0 || typeof m.tier0 !== 'object' || !sstKey(m.tier0.key) || !m.tier0.grid || typeof m.tier0.grid !== 'object' ||
      !Array.isArray(m.tiers) || m.tiers.length > 4) return false;
    for (var i = 0; i < m.tiers.length; i++) {
      var t = m.tiers[i];
      if (!t || typeof t !== 'object' || typeof t.name !== 'string' || !/^[a-z0-9]{1,8}$/.test(t.name) || !Number.isInteger(t.cell) || t.cell < 1 || t.cell > 90 ||
        180 % t.cell !== 0 || 360 % t.cell !== 0 || typeof t.res !== 'number' || !(t.res > 0) || !Number.isInteger(t.px) || t.px < 2 || t.px > SST_MAX_CELL_PX ||
        Math.abs(t.px * t.res - t.cell) > 1e-9 || t.apron !== 1 || !sstKey(t.dir) || !t.cells || typeof t.cells !== 'object' || Array.isArray(t.cells)) return false;
      for (var name in t.cells) {
        var c = t.cells[name];
        if (!/^-?\d{1,3}_-?\d{1,3}$/.test(name) || !Array.isArray(c) || c.length !== 3 || typeof c[0] !== 'number' || typeof c[1] !== 'number' || !(c[0] < c[1]) || !Number.isInteger(c[2]) || c[2] < 1) return false;
      }
    }
    return true;
  }
  function SstStore(root) {
    this.root = root; this.status = 'idle'; this.pointer = null; this.manifest = null; this.frame = null; this.grid = null; this.tiers = [];
    this.loading = null; this.loadAbort = null; this.failedAt = 0; this.loadedAt = 0; this.newer = null;
    this.inflight = {}; this.queue = []; this.queued = {}; this.failed = {}; this.bytes = 0;
    this.rev = 0; this.onChange = null; this.onNeeded = null; this.abort = null; this.retryTimer = null;
    this._lon0 = new Float64Array(TILE + 2); this._fx = new Float64Array(TILE + 2); this._colPos = new Float64Array(TILE + 2);
  }
  SstStore.prototype.timeoutMs = 15000;                    // pointer + manifest + the frame slower than this: failed (Retry), like a stalled load
  SstStore.prototype.retryMs = COAST_RETRY_MS;
  SstStore.prototype.recheckMs = POINTER_RECHECK_MS;
  SstStore.prototype._decode = function (url, sig, expect) { return decodeFrame(url, sig, expect); };   // the frames' decoder (tests replace it)
  SstStore.prototype._json = function (url, sig, noCache, maxBytes) {
    return fetch(url, noCache ? { signal: sig, mode: 'cors', cache: 'no-cache' } : { signal: sig, mode: 'cors' }).then(function (r) {
      if (!r.ok) throw new Error('sst ' + r.status);
      if (r.headers && r.headers.get && Number(r.headers.get('content-length') || 0) > maxBytes) throw new Error('unsupported sst manifest');
      return r.json();
    });
  };
  SstStore.prototype._pointer = function (sig) { return this._json(this.root + '/' + SST_PREFIX + '/latest.json', sig, true, 64 * 1024); };
  // Resolves to the store when the frame is usable, to null otherwise (never rejects). A store already loaded re-reads
  // the pointer when it is older than recheckMs or a recheck noted a newer day: the same run keeps everything, a newer
  // run drops the day and loads afresh (the next mount shows the new day; no banner: plan section 44).
  SstStore.prototype.load = function () {
    var self = this;
    if (this.loading) return this.loading;
    if (this.status === 'ok' && !this.newer && Date.now() - this.loadedAt < this.recheckMs) return Promise.resolve(this);
    if (this.status === 'failed' && Date.now() - this.failedAt < this.retryMs) return Promise.resolve(null);
    var was = this.status === 'ok' ? this.manifest : null;
    this.status = 'loading';
    var ctrl = this.loadAbort = new AbortController(), sig = ctrl.signal, timedOut = false;
    var watchdog = setTimeout(function () { timedOut = true; ctrl.abort(); }, this.timeoutMs);
    this.loading = this._pointer(sig).then(function (ptr) {
      if (!validSstPointer(ptr)) throw new Error('unsupported sst pointer');
      if (was && was.run === ptr.run) return null;                         // the day on screen is still the newest
      return self._json(self.root + '/' + ptr.manifest, sig, false, MAX_SST_MANIFEST_BYTES).then(function (m) {
        if (!validSstManifest(m) || m.run !== ptr.run) throw new Error('unsupported sst manifest');
        return self._decode(self.root + '/' + m.tier0.key, sig, { cols: m.tier0.grid.cols, rows: m.tier0.grid.rows }).then(function (frame) {
          validateGrid(m.tier0.grid, frame, SST_FDEF);
          return { ptr: ptr, m: m, frame: frame };
        });
      });
    }).then(function (res) {
      clearTimeout(watchdog);
      if (self.loadAbort !== ctrl || sig.aborted) throw abortError();     // aborted or superseded: this load owns nothing any more
      if (res) {
        if (was) self._dropDay();
        self.pointer = res.ptr; self.manifest = res.m; self.frame = res.frame; self.grid = res.m.tier0.grid;
        self.tiers = res.m.tiers.slice().sort(function (a, b) { return b.res - a.res; }).map(function (t) {   // coarse first: tiers[0] = tier 1
          return { name: t.name, cell: t.cell, res: t.res, px: t.px, n: t.px + 2, dir: t.dir, cells: t.cells, loaded: new Map() };
        });
      }
      self.newer = null; self.loadedAt = Date.now();
      self.status = 'ok'; self.loading = null; self.loadAbort = null;
      return self;
    }).catch(function () {
      clearTimeout(watchdog);
      if (self.loadAbort === ctrl) {
        self.loading = null; self.loadAbort = null;
        if (was) { self.status = 'ok'; self.loadedAt = Date.now(); }       // a failed re-read keeps the day on screen
        else { self.status = sig.aborted && !timedOut ? 'idle' : 'failed'; if (self.status === 'failed') self.failedAt = Date.now(); }
      }
      return was ? self : null;
    });
    return this.loading;
  };
  // Every 30 minutes while shown: a newer day is only NOTED (adopted by the next load). Resolves true when there is one.
  SstStore.prototype.recheck = function () {
    var self = this;
    if (this.status !== 'ok') return Promise.resolve(false);
    return this._pointer(null).then(function (ptr) {
      if (self.status === 'ok' && validSstPointer(ptr) && ptr.run > self.manifest.run) { self.newer = ptr; return true; }
      return false;
    }, function () { return false; });
  };
  SstStore.prototype._dropDay = function () {
    this._stop();
    this.pointer = null; this.manifest = null; this.frame = null; this.grid = null; this.tiers = []; this.bytes = 0; this.failed = {};
  };
  SstStore.prototype.available = function () { return this.status === 'ok' ? this.tiers.length : 0; };
  SstStore.prototype._tier = function (t) { return t > 0 && t <= this.tiers.length ? this.tiers[t - 1] : null; };
  // The tier a tile draws from now: the finest wanted at its zoom whose cells are all in hand (an unlisted cell is land,
  // ice or a lake: nothing to fetch, nothing drawn), else the next coarser; 0 = the frame. Missing cells are requested,
  // and so are the pad cells around the tile (never a condition for drawing).
  SstStore.prototype.tierFor = function (coords) {
    if (this.status !== 'ok') return 0;
    for (var t = sstTierFor(coords.z, this.tiers.length); t > 0; t--) {
      var T = this.tiers[t - 1], names = cellsForTile(coords, T.cell, 0), missing = [], all = true, i;
      if (names.length > MAX_CELLS_PER_TILE) continue;
      for (i = 0; i < names.length; i++) {
        var nm = names[i];
        if (!Object.prototype.hasOwnProperty.call(T.cells, nm)) continue;
        var rec = T.loaded.get(nm);
        if (rec) { T.loaded.delete(nm); T.loaded.set(nm, rec); continue; }          // LRU touch
        all = false;
        if (this.failed[T.name + '/' + nm] !== true) missing.push(nm);
      }
      if (missing.length) this.request(T, missing);
      if (!all) continue;
      var padded = cellsForTile(coords, T.cell, SST_PAD_PX), extra = [];
      for (i = 0; i < padded.length; i++) if (Object.prototype.hasOwnProperty.call(T.cells, padded[i]) && !T.loaded.has(padded[i]) && this.failed[T.name + '/' + padded[i]] !== true) extra.push(padded[i]);
      if (extra.length) this.request(T, extra);
      return t;
    }
    return 0;
  };
  // Did a cell a tile reads (its own or a pad cell) land after the revision the tile was drawn at?
  SstStore.prototype.landedAfter = function (coords, tier, rev) {
    var T = this._tier(tier);
    if (!T) return false;
    var names = cellsForTile(coords, T.cell, SST_PAD_PX);
    for (var i = 0; i < names.length; i++) { var rec = T.loaded.get(names[i]); if (rec && rec.rev > rev) return true; }
    return false;
  };
  // The keys ("tier/name") of every cell the tiles on the map could read at their zoom (the eviction and the queue keep them).
  SstStore.prototype.keysFor = function (coords, into) {
    for (var t = sstTierFor(coords.z, this.tiers.length); t > 0; t--) {
      var T = this.tiers[t - 1], names = cellsForTile(coords, T.cell, SST_PAD_PX);
      for (var i = 0; i < names.length; i++) into[T.name + '/' + names[i]] = true;
    }
    return into;
  };
  SstStore.prototype.request = function (T, names) {
    var fresh = {};
    for (var i = 0; i < names.length; i++) {
      var nm = names[i], key = T.name + '/' + nm, f = this.failed[key];
      if (!Object.prototype.hasOwnProperty.call(T.cells, nm) || this.inflight[key] || T.loaded.has(nm) || f === true) continue;
      if (typeof f === 'number' && f > Date.now()) { this._armRetry(f); continue; }
      if (!this.queued[key]) { this.queue.push([T, nm]); this.queued[key] = true; }
      fresh[key] = true;
    }
    this._pump(fresh);
  };
  SstStore.prototype._armRetry = CoastStore.prototype._armRetry;
  SstStore.prototype._pump = function (fresh) {
    var self = this, need = this.onNeeded ? this.onNeeded() : null;
    while (this.queue.length && Object.keys(this.inflight).length < MAX_SST_INFLIGHT) {
      var item = this.queue.shift(), T = item[0], nm = item[1], key = T.name + '/' + nm;
      delete this.queued[key];
      if (need && !need[key] && !(fresh && fresh[key])) continue;
      if (!this.abort) this.abort = new AbortController();
      (function (T, name, key, ctrl) {
        self.inflight[key] = ctrl;
        function mine() { return self.inflight[key] === ctrl; }
        self._decode(self.root + '/' + T.dir + '/' + name + '.png', ctrl.signal, { cols: T.n, rows: T.n }).then(function (fr) {
          if (ctrl.signal.aborted || !mine()) return;
          if (!fr || fr.cols !== T.n || fr.rows !== T.n || !fr.q || fr.q.length !== T.n * T.n) throw new Error('frame decode failed');
          delete self.inflight[key];
          var ent = T.cells[name];
          self.rev++;
          T.loaded.set(name, { q: fr.q, lo: ent[0], hi: ent[1], bytes: fr.q.length, rev: self.rev });
          self.bytes += fr.q.length; delete self.failed[key];
          self._evict();
          if (self.onChange) self.onChange();
          self._pump();
        }).catch(function (err) {
          if (ctrl.signal.aborted || (err && err.name === 'AbortError') || !mine()) return;
          delete self.inflight[key];
          var kind = failureKind(err);
          self.failed[key] = kind === 'permanent' ? true : Date.now() + self.retryMs;
          if (self.failed[key] !== true) self._armRetry(self.failed[key]);
          self.rev++;
          if (self.onChange) self.onChange();
          self._pump();
        });
      })(T, nm, key, this.abort);
    }
  };
  // Over the cap: the least recently used cells go first, never one a tile on the map could read now.
  SstStore.prototype._evict = function () {
    if (this.bytes <= MAX_SST_CHUNK_BYTES) return;
    var need = this.onNeeded ? this.onNeeded() : {};
    for (var t = 0; t < this.tiers.length && this.bytes > MAX_SST_CHUNK_BYTES; t++) {
      var T = this.tiers[t], it = T.loaded.keys(), k;
      while (this.bytes > MAX_SST_CHUNK_BYTES && !(k = it.next()).done) {
        if (need[T.name + '/' + k.value]) continue;
        this.bytes -= T.loaded.get(k.value).bytes; T.loaded.delete(k.value);
      }
    }
  };
  SstStore.prototype._stop = function () {
    if (this.abort) { this.abort.abort(); this.abort = null; }
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    this.inflight = {}; this.queue = []; this.queued = {};
  };
  // A field switch: downloads stop, the cells stay for the switch back.
  SstStore.prototype.idle = function () { this._stop(); this.onChange = null; this.onNeeded = null; };
  // Off: also the cells (the frame and the manifest stay: the next On is instant, the day re-checked by load()).
  SstStore.prototype.abortAll = function () {
    this.idle();
    if (this.loadAbort) { this.loadAbort.abort(); this.loadAbort = null; }
    if (this.status === 'loading') { this.status = this.manifest ? 'ok' : 'idle'; this.loading = null; }
    for (var t = 0; t < this.tiers.length; t++) this.tiers[t].loaded.clear();
    this.bytes = 0; this.failed = {};
  };
  // THE sampler of the store: fractional codes over SST_FDEF's range (0 = none) for an n x n lattice of pixel centres
  // at world pixels (x0 + ox + i * step + 0.5, y0 + oy + j * step + 0.5), x0 / y0 the tile's origin at its zoom, into
  // out[0..n*n). The tile (ox 0, oy 0, step 1, n TILE), the contour lattice (ox -s, oy -s, step s) and the readout
  // (ox px, oy py, n 1) all read here, so they agree exactly. tier 0: the frame on the model grid (the frames' own
  // sampler); a chunk tier: bilinear over the four nodes around the point within ITS cell (the apron keeps them inside;
  // absent nodes, q 0, drop out as in sampleRow), in that cell's code space, then the fixed range. A cell not in hand
  // (unlisted, or not loaded: a pad cell) is absent.
  SstStore.prototype.sampleGrid = function (coords, ox, oy, step, n, out, tier) {
    var nW = TILE * Math.pow(2, coords.z), x0 = coords.x * TILE + ox, y0 = coords.y * TILE + oy, i, j, k = 0, lat, c;
    var T = this._tier(tier);
    if (!T) {
      var f = this.frame, g = this.grid, cols = f.cols, rows = f.rows, colPos = this._colPos;
      for (i = 0; i < n; i++) { c = ((x0 + i * step + 0.5) / nW * 360 - 180 - g.lon0) / g.dlon; colPos[i] = ((c % cols) + cols) % cols; }
      for (j = 0; j < n; j++, k += n) {
        lat = latOfWorldY(y0 + j * step + 0.5, nW);
        var r = (g.lat0 - lat) / -g.dlat;
        if (r >= 0 && r <= rows - 1) sampleRow(f, false, r, colPos, n, out, k); else out.fill(0, k, k + n);
      }
      return out;
    }
    var cell = T.cell, res = T.res, N = T.n, lon0s = this._lon0, fxs = this._fx, LO = SST_FDEF.lo, SC = 254 / (SST_FDEF.hi - SST_FDEF.lo);
    for (i = 0; i < n; i++) {
      var lon = lngOfWorldX(x0 + i * step + 0.5, nW);
      lon = ((lon + 180) % 360 + 360) % 360 - 180;                               // world copies sample the one world
      var lon0 = Math.floor(lon / cell) * cell;
      lon0s[i] = lon0; fxs[i] = (lon - lon0) / res + 1;
    }
    for (j = 0; j < n; j++, k += n) {
      lat = latOfWorldY(y0 + j * step + 0.5, nW);
      if (!(lat > -90 && lat < 90)) { out.fill(0, k, k + n); continue; }
      var lat0 = Math.floor(lat / cell) * cell, fy = (lat0 + cell - lat) / res;
      var r0 = Math.floor(fy), r1 = r0 + 1 < N ? r0 + 1 : r0, fr = fy - r0, w0 = 1 - fr, b0 = r0 * N, b1 = r1 * N;
      for (var i0 = 0; i0 < n;) {
        var l0 = lon0s[i0], i1 = i0 + 1;
        while (i1 < n && lon0s[i1] === l0) i1++;                                   // one run per cell column (at most a few per row)
        var rec = T.loaded.get(lat0 + '_' + l0);
        if (!rec) { out.fill(0, k + i0, k + i1); i0 = i1; continue; }
        var q = rec.q, lo = rec.lo, span = (rec.hi - rec.lo) / 254;
        for (i = i0; i < i1; i++) {
          var fx = fxs[i], c0 = Math.floor(fx), c1 = c0 + 1 < N ? c0 + 1 : c0, fc = fx - c0;
          var a = q[b0 + c0], b = q[b0 + c1], cc = q[b1 + c0], d = q[b1 + c1], w = 0, acc = 0, wt;
          if (a) { wt = w0 * (1 - fc); acc += a * wt; w += wt; }
          if (b) { wt = w0 * fc; acc += b * wt; w += wt; }
          if (cc) { wt = fr * (1 - fc); acc += cc * wt; w += wt; }
          if (d) { wt = fr * fc; acc += d * wt; w += wt; }
          if (w < 0.25) { out[k + i] = 0; continue; }
          var code = 1 + (lo + (acc / w - 1) * span - LO) * SC;
          out[k + i] = code < 1 ? 1 : code > 255 ? 255 : code;
        }
        i0 = i1;
      }
    }
    return out;
  };

  // ---- frame decoding ----
  // Frames are 8-bit greyscale, non-interlaced PNGs. The direct path parses the chunks, lets the
  // browser inflate the zlib stream (DecompressionStream) and unfilters the rows into the 1 MB code
  // array: no bitmap, no 4 MB RGBA copy per frame (the canvas path is kept for older browsers).
  var MAX_FRAME_BYTES = 2 * 1024 * 1024;                   // live frames are <= 0.5 MB; nothing bigger is decoded
  function parsePng(buf, expect) {
    var u8 = new Uint8Array(buf), dv = new DataView(buf);
    if (u8.length < 8 || u8[0] !== 137 || u8[1] !== 80 || u8[2] !== 78 || u8[3] !== 71) throw new Error('frame decode failed');
    var pos = 8, w = 0, h = 0, depth = 0, ctype = 0, interlace = 0, idat = [];
    while (pos + 8 <= u8.length) {
      var len = dv.getUint32(pos), type = String.fromCharCode(u8[pos + 4], u8[pos + 5], u8[pos + 6], u8[pos + 7]), start = pos + 8;
      if (start + len > u8.length) throw new Error('frame decode failed');
      if (type === 'IHDR') {
        w = dv.getUint32(start); h = dv.getUint32(start + 4); depth = u8[start + 8]; ctype = u8[start + 9]; interlace = u8[start + 12];
        // the manifest says how big a frame is: anything else is refused BEFORE it is inflated
        if (expect && (w !== expect.cols || h !== expect.rows)) throw new Error('frame decode failed');
      }
      else if (type === 'IDAT') idat.push(u8.subarray(start, start + len));
      else if (type === 'IEND') break;
      pos = start + len + 4;
    }
    if (!w || !h || depth !== 8 || ctype !== 0 || interlace !== 0) return null;   // not our format: the canvas path handles it
    return { w: w, h: h, idat: idat };
  }
  // The zlib stream's IDAT pieces go to the browser's inflater as they are (no concatenated copy) and its output is read
  // into ONE buffer of the expected size, (w + 1) * h bytes: more or fewer = a broken frame. The inflater's own chunks
  // are collected in `spent` (when given) so the worker can hand them to the main thread (G29 B-P1-2: the decode's
  // garbage in the worker's own heap was only collected at V8's external-memory trigger, ~65 MB).
  function inflate(parts, size, spent) {
    var ds = new DecompressionStream('deflate'), writer = ds.writable.getWriter(), reader = ds.readable.getReader();
    var out = new Uint8Array(size), got = 0, i;
    for (i = 0; i < parts.length; i++) writer.write(parts[i]).catch(function () {});
    writer.close().catch(function () {});
    function pump() {
      return reader.read().then(function (r) {
        if (r.done) { if (got !== size) throw new Error('frame decode failed'); return out; }
        var c = r.value, n = c.length;
        if (got + n > size) { try { reader.cancel().catch(function () {}); } catch (e) { /* closed */ } throw new Error('frame decode failed'); }
        out.set(c, got); got += n;
        if (spent && c.buffer && spent.indexOf(c.buffer) < 0) spent.push(c.buffer);
        return pump();
      });
    }
    return pump().catch(function (err) { throw /decode failed/.test(String(err && err.message)) ? err : new Error('frame decode failed'); });
  }
  // PNG scanline filters 0..4 for one byte per pixel, IN PLACE: row y's pixels land at y * w, below the row's own bytes
  // at y * (w + 1) + 1, so nothing unread is ever overwritten; the result is a view of the first w * h bytes (no copy).
  function unfilter(raw, w, h) {
    var out = raw, stride = w + 1, x, a, b, c, p, pa, pb, pc;
    for (var y = 0; y < h; y++) {
      var f = raw[y * stride], src = y * stride + 1, dst = y * w, up = dst - w;
      if (f === 0) out.copyWithin(dst, src, src + w);
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
    return out.subarray(0, w * h);
  }
  // spent (optional): every buffer the decode made beyond the code array goes in it (the worker hands them back).
  function decodePngGrey(buf, expect, spent) {
    var p;
    try { p = parsePng(buf, expect); } catch (e) { return Promise.reject(e); }
    if (!p) return Promise.resolve(null);
    return inflate(p.idat, (p.w + 1) * p.h, spent).then(function (raw) { return { q: unfilter(raw, p.w, p.h), cols: p.w, rows: p.h }; });
  }
  var decodeCanvas = null;
  // ---- decoding off the main thread: a diagnostic only (plan section 42) ----
  // The frames are decoded on the MAIN THREAD by default (owner, 2026-10-14, step 5b: the worker's thread and isolate cost a
  // fixed ~17-20 MB of the tab's memory, paused or not, and bought no frame rate at 1920x1080 once the fix round's leaner
  // decoder was in: 7.9-8.0 frames/s at shark either way, idle and under load; it only freed 6-15 ms of main-thread time per
  // frame). With ?ovdecode=worker in the page's address:
  // a worker built from this module's own decode functions (their source text, so there is one asset and one version).
  // The main thread still fetches (the download watchdog, LATE_TRIES and onBody are unchanged), hands the body over
  // (transferred, not copied) and gets the code array back (transferred): no inflate pumping and no unfilter loop compete
  // with the tile drawing, and the raw body and the inflated bytes never enter the page's heap. A PNG that is not our
  // format comes back raw for the canvas path. A worker that dies ends its pending decodes with err.retry (decodeFrame
  // then fetches each once more, from the edge cache, for the main-thread path) and stays dead for the session, with
  // one console warning. Without Worker / Blob / createObjectURL / DecompressionStream the main-thread path runs.
  function workerMain(self) {
    self.onmessage = function (e) {
      var d = e.data, spent = [];
      decodePngGrey(d.buf, d.expect, spent).then(function (frame) {
        if (!frame) { self.postMessage({ id: d.id, raw: d.buf }, [d.buf]); return; }
        // The code array goes over transferred, and so does everything else the decode held (the body, the inflater's
        // chunks): the worker keeps nothing, and that garbage is collected on the main thread, where it always was
        // before the worker (G29 B-P1-2). A buffer the browser will not transfer: the code array goes alone.
        var list = [frame.q.buffer], extra = [], i, b;
        if (d.buf && d.buf.byteLength) { list.push(d.buf); extra.push(d.buf); }
        for (i = 0; i < spent.length; i++) { b = spent[i]; if (b && b.byteLength && list.indexOf(b) < 0) { list.push(b); extra.push(b); } }
        try { self.postMessage({ id: d.id, q: frame.q, cols: frame.cols, rows: frame.rows, spent: extra }, list); }
        catch (err) { self.postMessage({ id: d.id, q: frame.q, cols: frame.cols, rows: frame.rows }, [frame.q.buffer]); }
      }, function (err) { self.postMessage({ id: d.id, error: String(err && err.message || err) }); });
    };
  }
  function workerSource() {
    return [parsePng, inflate, unfilter, decodePngGrey].map(function (f) { return f.toString(); }).join('\n') + '\n(' + workerMain.toString() + ')(self);\n';
  }
  // The page's query as typed: the forecast loader rewrites the address (replaceState with the station query only) before
  // this module is loaded, so the live search is joined with the navigation entry's original URL (its query survives the
  // rewrite). The diagnostics ?ovdecode=worker and ?ovdebug=1 read it.
  function pageQuery() {
    var q = '';
    try { q = typeof location !== 'undefined' && location ? String(location.search || '') : ''; } catch (e) { q = ''; }
    try {
      var nav = typeof performance !== 'undefined' && performance.getEntriesByType ? performance.getEntriesByType('navigation')[0] : null;
      var u = nav && nav.name ? String(nav.name).replace(/#[\s\S]*$/, '') : '', i = u.indexOf('?');   // a '?' in the fragment is no query
      if (i >= 0) q += (q ? '&' : '?') + u.slice(i + 1);
    } catch (e) { /* no navigation entry */ }
    return q;
  }
  function FrameDecoder() { this.worker = null; this.url = null; this.pending = {}; this.seq = 0; this.dead = false; }
  FrameDecoder.available = function () {
    try {
      if (!/[?&]ovdecode=worker(?:&|$)/.test(pageQuery())) return false;      // the main thread by default (owner, 2026-10-14)
      return typeof Worker === 'function' && typeof Blob === 'function' && typeof URL !== 'undefined' && typeof URL.createObjectURL === 'function' && typeof DecompressionStream === 'function';
    } catch (e) { return false; }
  };
  FrameDecoder.prototype._boot = function () {
    var self = this;
    this.url = URL.createObjectURL(new Blob([workerSource()], { type: 'text/javascript' }));
    this.worker = new Worker(this.url);
    this.worker.onmessage = function (e) {
      var d = e.data, p = d && self.pending[d.id];
      if (!p) return;
      delete self.pending[d.id];
      if (d.error) p.reject(new Error(d.error)); else p.resolve(d);
    };
    this.worker.onerror = function (ev) { self._die(String(ev && ev.message || 'worker error')); };
  };
  function retryError() { var e = new Error('decoder died'); e.retry = true; return e; }
  FrameDecoder.prototype._die = function (why) {
    this.dead = true; decoderDead = true;
    this._drop(retryError);
    this._release();
    if (typeof console !== 'undefined' && console.warn) console.warn('overlay: frame decoding falls back to the main thread (' + why + ')');
  };
  FrameDecoder.prototype._drop = function (mkErr) { var p = this.pending; this.pending = {}; for (var k in p) p[k].reject(mkErr()); };
  FrameDecoder.prototype._release = function () {
    if (this.worker) { try { this.worker.terminate(); } catch (e) { /* gone */ } this.worker = null; }
    if (this.url) { try { URL.revokeObjectURL(this.url); } catch (e) { /* gone */ } this.url = null; }
  };
  // buf: an ArrayBuffer (transferred: unusable afterwards). Resolves {q, cols, rows}, or {raw} (not our format).
  FrameDecoder.prototype.decode = function (buf, expect) {
    var self = this;
    if (this.dead) return Promise.reject(retryError());
    return new Promise(function (resolve, reject) {
      try {
        if (!self.worker) self._boot();
        var id = ++self.seq; self.pending[id] = { resolve: resolve, reject: reject };
        self.worker.postMessage({ id: id, buf: buf, expect: expect }, [buf]);
      } catch (err) { self._die(String(err && err.message || err)); reject(retryError()); }
    });
  };
  // Off: nothing left behind (the G3 teardown rule); pending decodes end as aborted.
  FrameDecoder.prototype.dispose = function () { this._drop(abortError); this._release(); };
  var frameDecoder = null, decoderDead = false;
  function getDecoder() {
    if (decoderDead || !FrameDecoder.available()) return null;
    if (!frameDecoder) frameDecoder = new FrameDecoder();
    return frameDecoder;
  }
  function disposeDecoder() { if (frameDecoder) { frameDecoder.dispose(); frameDecoder = null; } }
  function resetDecoder() { disposeDecoder(); decoderDead = false; }   // tests only
  function setDecoderForTest(d) { frameDecoder = d; decoderDead = false; }
  // PNG -> Uint8Array of codes. Direct inflate where the browser has DecompressionStream (in the worker where it can
  // run, else on the main thread), else the canvas path (needs CORS on the bucket for getImageData). `expect` = the
  // manifest grid the frame must have: a body that is too large or a picture of another size is refused before decoding.
  // onBody(): called once the whole body has arrived, before the decode (the download watchdog stops there).
  // mainOnly: the retry after a dead worker (one more download: the body went with the worker).
  function decodeFrame(url, signal, expect, onBody, mainOnly) {
    var direct = typeof DecompressionStream === 'function' && typeof Response === 'function';
    return fetch(url, { signal: signal, mode: 'cors' }).then(function (r) {
      if (!r.ok) throw new Error('frame ' + r.status);
      var len = Number(r.headers.get('content-length') || 0);
      if (len > MAX_FRAME_BYTES) throw new Error('frame decode failed');
      return direct ? r.arrayBuffer() : r.blob();
    }).then(function (body) {
      if (onBody) onBody();
      var size = direct ? body.byteLength : body.size;
      if (size > MAX_FRAME_BYTES) throw new Error('frame decode failed');
      if (!direct) return decodeCanvas_(body, expect);
      if (signal && signal.aborted) throw abortError();                   // Off while the body arrived: no decoder is booted for it
      var dec = mainOnly ? null : getDecoder();
      if (!dec) return decodePngGrey(body, expect).then(function (frame) { return frame || decodeCanvas_(new Blob([body]), expect); });
      return dec.decode(body, expect).then(function (res) {
        if (res.raw) return decodeCanvas_(new Blob([res.raw]), expect);
        return { q: res.q, cols: res.cols, rows: res.rows };
      }, function (err) {
        if (err && err.retry && !(signal && signal.aborted)) return decodeFrame(url, signal, expect, null, true);
        throw err;
      });
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
  var STALE_TILE_MS = 3;                                     // idle time one buffer tile needs (about 2 ms measured)
  var STALE_MAX_MS = 500;                                    // a buffer tile is never further behind than this while the page is busy
  function tileInRange(c, r) { return c.z === r.z && c.x >= r.x0 && c.x <= r.x1 && c.y >= r.y0 && c.y <= r.y1; }
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
      this._src = null; this._sq = null;                   // plan section 44: the SST store the tiles sample instead of the frame
      this._sc = new Float64Array(TILE / 2 + 2); this._sr = new Float64Array(TILE / 2 + 2); this._srow = new Float64Array(TILE / 2 + 2);
      this._pi = new Int32Array(TILE + 2); this._pt = new Float64Array(TILE + 2);
      this._gen = 0; this._staleQ = null;                   // plan section 42: which frame a tile shows (el._ovGen)
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
      this._gen++;
      this._redraw(true);
    },
    clear: function () { this._frame = null; this.field = null; this.fdef = null; this.entry = null; this._clip = false; this._sm = null; this.setSource(null, true); this._redraw(); },
    // The SST store the tiles sample (plan section 44), or null (the frame again). The caller sets the store's tier-0
    // frame with setFrame(SST_FDEF ...) beside it; a loaded store answers every tile from the finest tier in hand.
    setSource: function (store, quiet) {
      var self = this, prev = this._src;
      if (prev && prev.onChange === this._onSrc) { prev.onChange = null; prev.onNeeded = null; }
      this._src = store && store.status === 'ok' ? store : null;
      if (this._src) {
        this._onSrc = function () { self._redrawSrc(); };
        this._src.onChange = this._onSrc;
        this._src.onNeeded = function () { return self._srcNeeded(); };
      }
      if (!quiet) this._redraw();
    },
    // A cell landed or failed: the tiles whose tier changed, or whose cells (own or pad) landed since they were drawn.
    _redrawSrc: function () {
      var st = this._src;
      if (!st || !this._frame) return;
      for (var k in this._tiles) {
        var t = this._tiles[k], el = t.el;
        if (!el || !t.coords) continue;
        if (st.tierFor(t.coords) !== el._ovSrcTier || st.landedAfter(t.coords, el._ovSrcTier, el._ovSrcRev)) this._draw(el, t.coords);
      }
    },
    _srcNeeded: function () {
      var st = this._src, need = {};
      if (!st) return need;
      for (var k in this._tiles) { var t = this._tiles[k]; if (t.coords) st.keysFor(t.coords, need); }
      return need;
    },
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
    // Every tile, or (split: a new frame while playing, plan section 42) first the tiles inside the map's pixel bounds,
    // synchronously, and the buffer tiles Leaflet keeps around the view (keepBuffer 1: up to about as many again after a
    // pan) in idle time afterwards (_drawStale), so a step's main-thread cost is the visible tiles' alone. A tile a
    // frame behind is never read: the readout samples the frame itself and the particles read only the land masks.
    _redraw: function (split) {
      var range = split ? this._viewRange() : null, later = false;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords) continue;
        if (range && !tileInRange(t.coords, range)) { later = true; continue; }
        this._draw(t.el, t.coords);
      }
      if (later) this._queueStale();
    },
    onRemove: function (map) {
      this._cancelStale();
      L.GridLayer.prototype.onRemove.call(this, map);
    },
    // The tiles the viewer sees: Leaflet's pixel bounds at the map zoom, scaled to the tile zoom (a fractional zoom keeps
    // the tiles of the nearest level, CSS-scaled); null off a map (every tile is drawn at once then: the Node stubs).
    _viewRange: function () {
      var map = this._map;
      if (!map || typeof map.getPixelBounds !== 'function') return null;
      var z = map.getZoom(), tz = typeof this._tileZoom === 'number' ? this._tileZoom : Math.round(z);
      var scale = typeof map.getZoomScale === 'function' ? map.getZoomScale(z, tz) : Math.pow(2, z - tz), pb = map.getPixelBounds();
      return { z: tz, x0: Math.floor(pb.min.x / scale / TILE), x1: Math.floor((pb.max.x / scale - 1) / TILE),
        y0: Math.floor(pb.min.y / scale / TILE), y1: Math.floor((pb.max.y / scale - 1) / TILE) };
    },
    // Draw the tiles that still show an older frame: those in view at once (a drag that brings a buffer tile into
    // view: the page calls this on 'move'), the rest while the browser has idle time (requestIdleCallback where it
    // exists, else one task), coming back for the remainder.
    _drawStale: function (inView, deadline) {
      if (!this._frame) return;
      var range = this._viewRange(), left = false;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords || t.el._ovGen === this._gen) continue;
        if (inView) { if (range && !tileInRange(t.coords, range)) continue; }
        else if (deadline && typeof deadline.timeRemaining === 'function' && !deadline.didTimeout && deadline.timeRemaining() < STALE_TILE_MS) { left = true; break; }
        this._draw(t.el, t.coords);
      }
      if (left) this._queueStale();
    },
    _queueStale: function () {
      var self = this;
      if (this._staleQ) return;
      var run = function (dl) { self._staleQ = null; self._drawStale(false, dl); };
      this._staleQ = typeof requestIdleCallback === 'function' ? { idle: requestIdleCallback(run, { timeout: STALE_MAX_MS }) } : { timer: setTimeout(run, 0) };
    },
    _cancelStale: function () {
      var q = this._staleQ;
      if (!q) return;
      this._staleQ = null;
      if (q.timer !== undefined) clearTimeout(q.timer);
      else if (typeof cancelIdleCallback === 'function') cancelIdleCallback(q.idle);
    },
    // The tiles need land masks: clipped fields (hs, tp) and outlined ones (wind) once a coast store is set.
    _masked: function () { return !!(this._coast && (CLIP_FIELDS[this.field] || OUTLINE_FIELDS[this.field])); },
    // The tier-1 cells the tiles on the map need right now (the store drops queued downloads for others).
    _cellsNeeded: function () {
      var st = this._coast, need = {};
      if (!st || !this._masked()) return need;
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
      if (!this._coast || !this._masked()) return;
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
      var p = pixelOf(lat, lng, z), t = this._tiles[p.x + ':' + p.y + ':' + p.z], v;
      if (this._src) {                                       // the tile's own tier, the pixel's own sample
        if (!this._frame || !t || !t.el || t.el._ovSrcTier === undefined) return null;
        this._src.sampleGrid(p, p.px, p.py, 1, 1, this._oneOut, t.el._ovSrcTier);
        v = this._oneOut[0] ? this._value(this._oneOut[0]) : null;
      } else {
        var ll = tilePixelLatLng(p, p.px, p.py);
        v = this.valueAt(ll.lat, ll.lng);
      }
      if (v === null || !this._clip) return v;
      var land = t && t.el ? t.el._ovLand : undefined;
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
    tileCodes: function (coords, out, tier) {
      if (this._src) return this._src.sampleGrid(coords, 0, 0, 1, TILE, out, tier || 0);
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
      el._ovGen = this._gen;
      if (!this._frame) { ctx.clearRect(0, 0, TILE, TILE); return; }
      var land = this._masked() ? this._landFor(el, coords) : null;
      var tier = 0;
      if (this._src) { tier = this._src.tierFor(coords); el._ovSrcTier = tier; el._ovSrcRev = this._src.rev; }
      if (this._clip && land === LAND_ALL) { ctx.clearRect(0, 0, TILE, TILE); return; }   // nothing but land: no sampling at all
      var codes = this.tileCodes(coords, this._codes, tier);
      var img = el._ovImg || (el._ovImg = ctx.createImageData(TILE, TILE));     // reused per tile: no 256 KB per frame
      composeTile(codes, this._clip ? land : null, this._lut, this._lo, this._hi, this._legend[0], this._legend[1], img.data);
      if (this._contour && !this._nearest && CONTOURS[this.field]) this._contours(coords, img.data, false, tier);
      if (!this._clip && land && land !== LAND_ALL) {                          // the coastline under an unclipped field
        if (el._ovEdgesOf !== land) { el._ovEdges = coastEdges(land, WIND_LOOK.gain); el._ovEdgesOf = land; }
        applyEdges(el._ovEdges, img.data, WIND_LOOK.ink);
      }
      ctx.putImageData(img, 0, 0);
    },
    // Contours on the smoothed copy of the frame (smoothBlock), sampled at world pixels every s px (the
    // same pixels for every tile, so neighbouring tiles agree on their shared edges and world copies
    // match) and interpolated in between; a 16-px block is drawn only if its samples span a whole level.
    // all: draw every block (the tests compare it with the skipping).
    _contours: function (coords, rgba, all, tier) {
      var cfg = this._contour, fr = this._frame, g = this._grid, cols = fr.cols, rows = fr.rows, i, j;
      var step = cfg.step * (coords.z < 4 ? 2 : 1), k = cfg.per / step, koff = cfg.off ? cfg.off / step : 0, lo = this._lo, span = (this._hi - lo) / 254;
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
      if (this._src) {                                       // the store's values on the lattice, unsmoothed (steps <= 0.1 C)
        var Q = this._sq || (this._sq = new Float64Array((TILE / 2 + 2) * (TILE / 2 + 2)));
        this._src.sampleGrid(coords, -s, -s, s, m, Q, tier || 0);
        for (j = 0; j < m * m; j++) V[j] = Q[j] ? (lo + (Q[j] - 1) * span) * k + koff : NaN;
      } else {
        var sm = this._smooth(cp, rp, m), J = sm.jump ? sm.J : null, wide = cellPx < s;
        for (j = 0; j < m; j++) {
          var r = rp[j], o = j * m;
          if (!(r >= 0 && r <= rows - 1)) { for (i = 0; i < m; i++) V[o + i] = NaN; continue; }
          this._smoothRow(r, cp, m, row, sm.S);
          var r0 = Math.floor(r);
          for (i = 0; i < m; i++) V[o + i] = row[i] && !(J && jumpAt(J, cols, rows, r0, Math.floor(cp[i]), wide)) ? (lo + (row[i] - 1) * span) * k + koff : NaN;
        }
      }
      var W2 = TILE + 2, F = this._F || (this._F = new Float64Array(W2 * W2)), B = CONTOUR_BLOCK;
      for (i = 0; i < W2; i++) { var fi = (i - 1) / s + 1; pi[i] = Math.floor(fi); pt[i] = fi - pi[i]; }   // pixel column i - 1 between samples pi, pi + 1
      var lut = this._lut, L0 = this._legend[0], sc = 255 / (this._legend[1] - L0);
      var ink = function (lv) {                                  // one ink per level, from the colour of the level's own value
        var t = Math.round(((lv - koff) / k - L0) * sc);
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
  var SPEEDS = [1, 2, 4], BASE_FPS = 2;                      // 1x = 2 frames/s (owner default); 0.5x dropped (owner, 2026-10-07)
  // The speed selector shows animals, never numbers (plan section 37): the speeds live in the tooltips and accessible names.
  var SPEED_ANIMALS = [
    { speed: 1, name: 'Snail', file: 'snail.png', title: 'Snail: 1\u00d7 speed' },
    { speed: 2, name: 'Fish', file: 'fish.png', title: 'Fish: 2\u00d7 speed' },
    { speed: 4, name: 'Shark', file: 'shark.png', title: 'Shark: 4\u00d7 speed' }
  ];
  function animalOf(speed) { for (var i = 0; i < SPEED_ANIMALS.length; i++) if (SPEED_ANIMALS[i].speed === speed) return SPEED_ANIMALS[i]; return SPEED_ANIMALS[0]; }
  // A saved speed that is not offered any more (0.5 from an older save, anything odd) plays at 1x.
  function speedOf(v) { return SPEEDS.indexOf(v) >= 0 ? v : 1; }
  // The module's own version (the page passes it; the script URL's ?v= is the fallback): the illustrations are served
  // immutable beside the module and only at the exact version.
  var SCRIPT_VERSION = (function () {
    try { var src = typeof document !== 'undefined' && document.currentScript && document.currentScript.src, m = src && /[?&]v=([^&#]+)/.exec(src); return m ? decodeURIComponent(m[1]) : ''; }
    catch (e) { return ''; }
  })();
  function assetUrl(opts, file) { var v = (opts && opts.version) || SCRIPT_VERSION; return '/overlay/' + file + (v ? '?v=' + encodeURIComponent(v) : ''); }
  var RING_AHEAD = 2, RING_BEHIND = 2, MAX_DECODED = 5, MAX_INFLIGHT = 2;
  // Playing fast, the ring runs further ahead with more downloads at once, so a step never waits for its own frame
  // (plan section 42): decoded = the ring's size + 1, so a ring entry is never evicted before it is drawn (at 4x the
  // 5-frame cache fetched every frame about 1.8 times). Paused, and at 1x, the budgets of G3/G4 (5 / 2 / 5) stand.
  var PLAY_RING = {
    1: { ahead: RING_AHEAD, behind: RING_BEHIND, inflight: MAX_INFLIGHT, decoded: MAX_DECODED },
    2: { ahead: 4, behind: 1, inflight: 3, decoded: 7 },
    4: { ahead: 6, behind: 1, inflight: 4, decoded: 9 }
  };
  function ringFor(speed, playing) { return (playing && PLAY_RING[speed]) || PLAY_RING[1]; }
  var RUN_CHECK_MS = 30 * 60 * 1000;                         // newer run -> banner only, never an automatic switch
  var RETRY_AFTER_MS = 60 * 1000;                            // a transient fetch failure keeps a frame out for this long
  var MAX_TRANSIENT = 3;                                     // consecutive transient failures -> "unavailable" + Retry
  var LOAD_STALL_MS = 15000;                                 // a pointer / manifest request silent this long -> the error panel (G30-3)
  var STALL_MS = 5000;                                       // a frame DOWNLOAD slower than this (or 4 intervals) is asked again
  var PAINT_FALLBACK_MS = 250;                               // a playback landing waits for the browser to render it, at most this long
  var LATE_TRIES = 3;                                        // download attempts before a stalling frame counts as a failure
  // A frame that does not exist (or cannot be decoded) is gone for the session; anything else (network
  // error, 403 from a bot check, 5xx, browser out of resources) is retried after a cooldown.
  function failureKind(err) {
    var msg = String(err && err.message || ''), m = /^frame (\d{3})$/.exec(msg);
    if (m) return (m[1] === '404' || m[1] === '410') ? 'permanent' : 'transient';
    return /decode/.test(msg) ? 'permanent' : 'transient';
  }
  function abortError() { var e = new Error('aborted'); e.name = 'AbortError'; return e; }
  // Indices worth having decoded around i: i itself, then ahead in the play direction, then behind (wrapping).
  function ringPlan(i, n, dir, ahead, behind) {
    var out = [i], seen = {}, uniq = [], k;
    if (ahead === undefined) ahead = RING_AHEAD;
    if (behind === undefined) behind = RING_BEHIND;
    for (k = 1; k <= ahead; k++) out.push(((i + k * dir) % n + n) % n);
    for (k = 1; k <= behind; k++) out.push(((i - k * dir) % n + n) % n);
    for (k = 0; k < out.length; k++) if (!seen[out[k]]) { seen[out[k]] = true; uniq.push(out[k]); }
    return uniq;
  }
  // Next index from i in direction dir that is not unavailable (wrapping, never i itself); null when
  // nothing else is left.
  function nextAvailable(i, dir, n, unavailable) {
    for (var k = 1; k < n; k++) { var j = ((i + k * dir) % n + n) % n; if (!unavailable(j)) return j; }
    return null;
  }
  // Manual navigation stops at the ends (playback keeps wrapping through nextAvailable): the next index from i in
  // direction dir that is not unavailable, without wrapping; null past the first / last frame. From -1 (dir 1) it
  // gives the first available frame, from n (dir -1) the last.
  function stepAvailable(i, dir, n, unavailable) {
    for (var j = i + dir; j >= 0 && j < n; j += dir) if (!unavailable(j)) return j;
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
  // Decoded frames, least recently used first out (with the ring's ranks: what the ring does not need first, then the
  // ring's farthest); the frame on the map is never evicted.
  function FrameCache(max) { this.max = max; this.map = new Map(); }
  FrameCache.prototype.get = function (key) { var v = this.map.get(key); if (v) { this.map.delete(key); this.map.set(key, v); } return v || null; };
  FrameCache.prototype.has = function (key) { return this.map.has(key); };
  FrameCache.prototype.clear = function () { this.map.clear(); };
  FrameCache.prototype.size = function () { return this.map.size; };
  FrameCache.prototype.set = function (key, frame, keep, rank, keepValue) {
    this.map.delete(key); this.map.set(key, frame);
    this._trim(key, keep, rank, keepValue);
  };
  // A new cap (the ring grows with the speed and shrinks back on pause): over it, entries go as _trim says, never keep.
  FrameCache.prototype.limit = function (max, keep, rank, keepValue) { this.max = max; this._trim(null, keep, rank, keepValue); };
  // Over the cap. rank (key -> the entry's place in the ring's plan; undefined = the ring does not need it): first the
  // entries the ring does not need, least recently used first (a frame that just landed outside the ring included), then
  // the ring's farthest. A plain LRU dropped the frames fetched EARLY for the ring, the next ones, before the frames
  // behind the playhead (plan section 42: a phone at shark speed downloaded 3 frames in 4 twice). Without rank: LRU,
  // never the frame that just landed. Never keep (the key of the frame on the map) nor keepValue (the frame object
  // itself: after a resolution change the drawn frame's key is the OTHER resolution's, G29 A-P3-1).
  FrameCache.prototype._trim = function (key, keep, rank, keepValue) {
    if (this.map.size <= this.max) return;
    var self = this, order;
    var all = Array.from(this.map.keys()).filter(function (k) { return k !== keep && (keepValue === undefined || keepValue === null || self.map.get(k) !== keepValue); });
    if (rank) {
      order = all.filter(function (k) { return rank[k] === undefined; })
        .concat(all.filter(function (k) { return k !== key && rank[k] !== undefined; }).sort(function (a, b) { return rank[b] - rank[a]; }));
    } else order = all.filter(function (k) { return k !== key; });
    for (var i = 0; i < order.length && this.map.size > this.max; i++) this.map.delete(order[i]);
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
  var PARTICLE_MIN = 150, PARTICLE_MAX = 3000;
  // The particles' look (plan section 42, step 3; the owner's pick of 2026-10-13 from four candidates measured on the test
  // site): px2 = screen pixels per particle (the density before section 42: its count is what the main thread pays for;
  // +25 % cost the 1080p frame rate), the line widths (a light core over a dark halo) per field, heavier than before, and
  // the head's and the tail's opacities, brighter than before.
  var LOOK = { px2: 900, style: { wind: { width: 1.6, halo: 3.4 }, hs: { width: 2.0, halo: 4.0 }, tp: { width: 2.0, halo: 4.0 } }, head: 0.95, headHalo: 0.4, tail: 0.55, tailHalo: 0.25 };
  var PARTICLE_PX2 = LOOK.px2;
  var OV_DEBUG = (function () { try { return /[?&]ovdebug=1(?:&|$)/.test(pageQuery()); } catch (e) { return false; } })();
  var WATCHDOG_MS = 2000;                                  // the animator restarts itself when it should run and does not (iPad)
  var BUSY_STUCK_MS = 10000;                               // a map "busy" this long in one go is a flag stuck true, not a gesture
  var PARTICLE_PX_PER_S = 3;                               // screen px/s per m/s of wind, before the latitude stretch
  // Particle speed in screen px/s (before the Mercator stretch) from the field value under it: wind by
  // its speed; swell lines by the wave height (nothing under the 0.1 m floor) or by the peak period (the
  // deep-water group speed grows with the period; nothing under the model's 3 s no-wave floor).
  var FLOW_SPEED = {
    wind: function (v) { return PARTICLE_PX_PER_S * v; },
    hs: function (v) { return v < 0.1 ? 0 : 8 + 3 * v; },
    tp: function (v) { return v < 3 ? 0 : 1.5 * v; }
  };
  var FLOW_STYLE = LOOK.style;
  var TRAIL_POINTS = 10, TRAIL_EVERY_MS = 66;              // a particle's tail: its last 10 positions, one every 66 ms (~0.6 s), drawn fresh each frame
  var TRAIL_SPAN_MS = (TRAIL_POINTS + 1) * TRAIL_EVERY_MS; // ... and never older than this in FLOW time (G29 B-P3-1; owner: the same length at every speed)
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
  // win (plan section 42): only the nodes in a window {r0, r1, c0, count} (rows inclusive, columns from c0 wrapping) are
  // computed, the rest marked absent; null = every node. The animator asks for the window around its view plus a screen
  // on every side (nodeWindow), so a step costs a few percent of the grid; a pan beyond it recomputes.
  function vectorNodes(field, layer, dframe, dgrid, prev, win) {
    var cols = dframe.cols, rows = dframe.rows, n = cols * rows, reuse = prev && prev.cols === cols && prev.rows === rows;
    var U = reuse ? prev.U : new Float32Array(n), V = reuse ? prev.V : new Float32Array(n), M = reuse ? prev.M : new Uint8Array(n);
    var f = layer._frame, g = layer._grid, spd = FLOW_SPEED[field] || FLOW_SPEED.wind, lo = layer._lo, hi = layer._hi, q = dframe.q;
    var colPos = new Float64Array(cols), codes = new Float64Array(cols), i, j, ii;
    var r0 = win ? Math.max(0, win.r0) : 0, r1 = win ? Math.min(rows - 1, win.r1) : rows - 1, c0 = win ? ((win.c0 % cols) + cols) % cols : 0, count = win ? Math.min(cols, win.count) : cols;
    if (win && reuse) M.fill(0);
    for (i = 0; i < cols; i++) { var lng = dgrid.lon0 + i * dgrid.dlon; colPos[i] = ((((lng - g.lon0) / g.dlon) % f.cols) + f.cols) % f.cols; }
    for (j = r0; j <= r1; j++) {
      var lat = dgrid.lat0 + j * dgrid.dlat, r = (g.lat0 - lat) / -g.dlat, base = j * cols;
      if (r >= 0 && r <= f.rows - 1) sampleRow(f, layer._nearest, r, colPos, cols, codes, 0); else codes.fill(0);
      for (ii = 0; ii < count; ii++) {
        i = c0 + ii; if (i >= cols) i -= cols;
        var d = q[base + i], c = codes[i];
        if (!d || d === 255 || !c) { M[base + i] = 0; U[base + i] = 0; V[base + i] = 0; continue; }
        var sp = spd(lo + (c - 1) / 254 * (hi - lo));
        M[base + i] = sp > 0 ? 1 : 0; U[base + i] = -sp * CODE_SIN[d]; V[base + i] = -sp * CODE_COS[d];   // FROM -> TOWARD
      }
    }
    return { cols: cols, rows: rows, U: U, V: V, M: M, win: win ? { r0: r0, r1: r1, c0: c0, count: count } : null };
  }
  // The node window for a view plus `margin` screens on every side (default 1: what the animator computes; 0: what a
  // view needs, the coverage test), in direction-grid rows and columns (wrapping), with one node of margin for the
  // lattice's bilinear neighbours. The rows are clamped to the world (85.05 N / S): lattice points beyond it have no
  // nodes, where the whole grid would still find some; the page never shows that band (84 N / 79 S, G29 A-P3-2).
  function nodeWindow(view, dgrid, dframe, margin) {
    var n = TILE * Math.pow(2, view.z), cols = dframe.cols, rows = dframe.rows, mg = margin === undefined ? 1 : margin;
    var y0 = Math.max(0, view.oy - mg * view.h), y1 = Math.min(n, view.oy + (1 + mg) * view.h), x0 = view.ox - mg * view.w, x1 = view.ox + (1 + mg) * view.w;
    var latTop = latOfWorldY(y0, n), latBot = latOfWorldY(y1, n);
    var r0 = Math.floor((dgrid.lat0 - latTop) / -dgrid.dlat) - 1, r1 = Math.ceil((dgrid.lat0 - latBot) / -dgrid.dlat) + 1;
    r0 = Math.max(0, Math.min(rows - 1, r0)); r1 = Math.max(0, Math.min(rows - 1, r1));
    if (x1 - x0 >= n) return { r0: r0, r1: r1, c0: 0, count: cols };
    var c0 = Math.floor(((((lngOfWorldX(x0, n) - dgrid.lon0) / dgrid.dlon) % cols) + cols) % cols) - 1;
    var count = Math.min(cols, Math.ceil((x1 - x0) / n * 360 / Math.abs(dgrid.dlon)) + 4);
    return { r0: r0, r1: r1, c0: ((c0 % cols) + cols) % cols, count: count };
  }
  // Does a computed window hold everything a wanted one needs?
  function windowCovers(have, want, cols) {
    if (!have) return false;
    if (have.r0 > want.r0 || have.r1 < want.r1) return false;
    if (have.count >= cols) return true;
    if (want.count > have.count) return false;
    var off = ((want.c0 - have.c0) % cols + cols) % cols;
    return off + want.count <= have.count;
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
  // Safari, and every browser on iOS / iPadOS (all of them WebKit): WebKit with an iOS mark (iPad / iPhone / iPod, or
  // Chrome / Edge / Firefox for iOS: CriOS / EdgiOS / FxiOS), a Macintosh user agent on a touch screen (iPadOS asks for
  // desktop pages as a Mac), or no Chromium / Firefox mark at all (K-11: the first rule left iOS Chrome / Edge / Firefox out).
  function isWebKit() {
    try {
      var nav = typeof navigator !== 'undefined' && navigator ? navigator : null, ua = nav ? String(nav.userAgent || '') : '';
      if (!/AppleWebKit/.test(ua)) return false;
      if (/iPad|iPhone|iPod|CriOS|EdgiOS|FxiOS/.test(ua)) return true;
      if (/Macintosh/.test(ua) && typeof nav.maxTouchPoints === 'number' && nav.maxTouchPoints > 1) return true;
      return !/Chrome|Chromium|Edg\/|OPR|Firefox/.test(ua);
    } catch (e) { return false; }
  }

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
    this.vel = new Float64Array(2); this.vel2 = new Float64Array(2); this.hist = null; this.histN = null; this.histT = null; this.histF = null; this.flowT = null;
    this.active = false; this.suspended = 0; this.dirty = false; this.rafId = null; this.lastT = 0;
    this.ema = 0; this.adaptAt = 0; this._listeners = [];
    this.rate = 1;                                           // the playback speed: particles move x1 (paused, snail), x2, x4
    this.lastFrameAt = 0; this.healed = 0; this.log = []; this.watch = null; this.busySince = 0;   // the watchdog (plan section 42) and the debug readout
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
    this._newCanvas(pane);
    // a zoom fires zoomstart + movestart, then zoomend + moveend: the counter rebuilds once, at the end
    this._on('zoomstart', function () { self._note('zoomstart'); self.suspend(); if (self.canvas) self.canvas.style.visibility = 'hidden'; });
    // WebKit (the owner's iPad): the particles stayed off after a pinch zoom until a reload, which Chromium never
    // reproduces; a canvas whose backing store Safari dropped during the zoom draws nothing, so the canvas is a fresh
    // element after every zoom there (the old one released first).
    this._on('zoomend', function () { self._note('zoomend'); if (self.canvas && self.canvas.parentNode && isWebKit()) self._newCanvas(self.canvas.parentNode); if (self.canvas) self.canvas.style.visibility = ''; self.resume(true); });
    this._on('movestart', function () { self._note('movestart'); self.suspend(); });
    this._on('moveend', function () { self._note('moveend'); self.resume(true); });
    // a view reset (setView without animation, a jump of a screen or more, the site's single-world
    // clamp on a resize) recreates the tiles AFTER moveend: the flow is rebuilt once they exist
    this._on('viewreset', function () { self._note('viewreset'); if (!self.suspended) self._rebuild(); });
    this._on('resize', function () { self._note('resize'); self._rebuild(); });
    // a moving gesture or animation fires these continuously: the map is alive, not stuck (see _check)
    var alive = function () { self.busySince = 0; };
    this._on('move', alive); this._on('zoom', alive);
    // the watchdog: every 2 s, an animator that should be running and is not is restarted (see _check)
    this.watch = setInterval(function () { self._check(); }, WATCHDOG_MS);
    if (this.watch && this.watch.unref) this.watch.unref();
  };
  // The new canvas's drawing context is taken FIRST: a browser that refuses one (Safari with its canvas memory used
  // up, G29 B-P2-2) leaves the old canvas in place; without any context the animator draws nothing and the watchdog
  // asks again (never an exception into the map's events or the playback).
  FlowAnimator.prototype._newCanvas = function (pane) {
    var old = this.canvas, cv = document.createElement('canvas'), ctx = null;
    cv.className = 'ov-anim leaflet-zoom-hide';
    try { ctx = cv.getContext('2d') || null; } catch (e) { ctx = null; }
    if (!ctx && old && this.ctx) return;
    if (old) { try { old.width = 0; old.height = 0; } catch (e) { /* gone */ } if (old.remove) old.remove(); }
    pane.appendChild(cv); this.canvas = cv; this.ctx = ctx; this.v = null;   // placed afresh by the next rebuild
  };
  // A map event (a gesture or an animation starting or ending, a view reset, a resize) restarts the stuck clock (G29
  // re-check R-4: separate gestures each spanning a 2-s check added up to "busy for 10 s").
  FlowAnimator.prototype._note = function (what) { this.busySince = 0; this.log.push([what, Math.round(nowMs())]); if (this.log.length > 8) this.log.shift(); };
  // The playback speed (1 paused or at 1x, 2 at 2x, 4 at 4x): the particles move that much faster, so at the fast
  // speeds they keep pace with the time-lapse instead of drifting behind the storms; their tails keep their length on
  // screen (the sampling interval shrinks with the rate) and their lifetimes stay in real time.
  FlowAnimator.prototype.setRate = function (r) { this.rate = r > 0 ? r : 1; };
  // Is anything that legitimately holds the animator in progress (a zoom or pan animation, a drag, a pinch)? (A flyTo
  // sets none of these flags and would be healed mid-flight; the page never uses one.)
  FlowAnimator.prototype._busy = function () {
    var map = this.map;
    try {
      return !!(map._animatingZoom || (map._panAnim && map._panAnim._inProgress) || (map.dragging && map.dragging.moving && map.dragging.moving()) ||
        (map.touchZoom && map.touchZoom._zooming) || (map.boxZoom && map.boxZoom._moved));
    } catch (e) { return false; }
  };
  // The watchdog (plan section 42; the owner's iPad lost its particles after a pinch zoom until a reload): when the
  // animator has its data and canvas, the page is visible and nothing is in progress on the map, yet the loop is not
  // running, has not drawn a frame for WATCHDOG_MS, or sits behind a suspend that never ended, it is reset and rebuilt.
  FlowAnimator.prototype._check = function () {
    if (!this.canvas || !this.dir || !this.layer || !this.layer.hasFrame() || this.mode !== 'particles') return;
    if ((typeof document !== 'undefined' && document.hidden) || reducedMotion()) return;
    // a gesture or an animation in progress holds the animator legitimately; "busy" for BUSY_STUCK_MS with no map event
    // in between (every event restarts busySince) is a Leaflet flag stuck true (G29 A-P3-3), which would otherwise keep a
    // stuck iPad stuck for good
    if (this._busy()) { if (!this.busySince) this.busySince = nowMs(); if (nowMs() - this.busySince < BUSY_STUCK_MS) return; } else this.busySince = 0;
    var stuck = this.suspended > 0, dead = !this.active || this.rafId === null || nowMs() - this.lastFrameAt > WATCHDOG_MS;
    if (!stuck && !dead) return;
    this.healed++; this._note(stuck ? 'heal:suspended' : 'heal:dead'); this.busySince = 0;
    // the pause that never ended may be a zoom whose zoomend never came: its canvas is still hidden (G29 B-P2-1); on
    // WebKit, and wherever the canvas got no drawing context, the canvas is a fresh element, as after a zoom
    if (this.canvas && this.canvas.parentNode && (!this.ctx || isWebKit())) this._newCanvas(this.canvas.parentNode);
    if (this.canvas && this.canvas.style) this.canvas.style.visibility = '';
    this.suspended = 0; this.dirty = false; this.stop(); this._rebuild();
  };
  // The ?ovdebug=1 readout's line (text only).
  FlowAnimator.prototype.debugState = function () {
    var cv = this.canvas;
    return { active: this.active, suspended: this.suspended, dirty: this.dirty, raf: this.rafId !== null, frameAgeMs: this.lastFrameAt ? Math.round(nowMs() - this.lastFrameAt) : null,
      ctx: !!this.ctx, ctxLost: !!(this.ctx && typeof this.ctx.isContextLost === 'function' && this.ctx.isContextLost()),
      canvas: cv ? cv.width + 'x' + cv.height + (cv.style && cv.style.visibility ? ' ' + cv.style.visibility : '') : 'none', dir: !!this.dir, entry: this.entry ? this.entry.step : null,
      mode: this.mode, count: this.count, target: this.target, rate: this.rate, ema: Math.round(this.ema * 10) / 10, healed: this.healed, busy: this._busy(), log: this.log.map(function (e) { return e[0] + '@' + e[1]; }).join(' ') };
  };
  FlowAnimator.prototype.detach = function () {
    var self = this;
    this.stop();
    if (this.watch) { clearInterval(this.watch); this.watch = null; }
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
  // The layer changed under the animation (a coast store): rebuild.
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
    if (this.ctx) this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
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
    if (!this.nodes || this.nodesFor !== this.dir || this.nodesFrame !== this.fframe || !windowCovers(this.nodes.win, nodeWindow(v, this.dgrid, this.dir, 0), this.dir.cols)) {   // once per step, or past the window
      this.nodes = vectorNodes(this.field, this.layer, this.dir, this.dgrid, this.nodes, nodeWindow(v, this.dgrid, this.dir, 1)); this.nodesFor = this.dir; this.nodesFrame = this.fframe;
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
    this.histF = new Float32Array(PARTICLE_MAX * TRAIL_POINTS); this.flowT = new Float32Array(PARTICLE_MAX);   // each point's flow time and the particle's flow clock
    for (var i = 0; i < PARTICLE_MAX; i++) { this._respawn(i * 4, v); this.particles[i * 4 + 2] = Math.random() * this._life()[0]; }
  };
  FlowAnimator.prototype._respawn = function (k, v) {
    var P = this.particles, life = this._life(); P[k] = Math.random() * v.w; P[k + 1] = Math.random() * v.h; P[k + 2] = 0;
    P[k + 3] = life[0] + Math.random() * (life[1] - life[0]);
    this.histN[k >> 2] = 0; this.histT[k >> 2] = 0; this.flowT[k >> 2] = 0;     // a fresh particle has no tail
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
    if (this.rafId !== null || !this.dir || !this.canvas || !this.ctx || !this.vf) return;
    var self = this; this.active = true; this.lastT = 0; this.lastFrameAt = nowMs();   // a loop just started is not a dead one (G29 B-P3-2)
    this.rafId = this.raf(function (t) { self._frame(t); });
  };
  FlowAnimator.prototype.stop = function () { this.active = false; if (this.rafId !== null) { this.caf(this.rafId); this.rafId = null; } };
  FlowAnimator.prototype._frame = function (t) {
    this.rafId = null;
    if (!this.active || this.suspended || !this.dir || !this.canvas || (typeof document !== 'undefined' && document.hidden)) { this.active = false; return; }
    var dt = this.lastT ? Math.min(50, Math.max(0, t - this.lastT)) : 16; this.lastT = t;
    var t0 = nowMs(); this.lastFrameAt = t0; this._renderParticles(dt); this._adapt(nowMs() - t0, t);
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
    var ctx = this.ctx, v = this.v, vf = this.vf, P = this.particles, H = this.hist, HN = this.histN, HT = this.histT, HF = this.histF, FT = this.flowT;
    if (!ctx || !v || !vf || !P) return;
    var w = v.w, h = v.h, n = this.count, dtf = dt * this.rate, dts = dtf / 1000, half = dts / 2, V1 = this.vel, V2 = this.vel2, i, k, q, m;
    var style = LOOK.style[this.field] || LOOK.style.wind, every = TRAIL_EVERY_MS / this.rate;
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
      if (HT[i] >= every || HN[i] === 0) {                                      // record the position the head leaves behind, with its flow time
        HT[i] = 0; m = HN[i]; q = (i * TRAIL_POINTS + (m % TRAIL_POINTS)) * 2; H[q] = x; H[q + 1] = y; HF[q >> 1] = FT[i]; HN[i] = m + 1 > 255 ? 255 - TRAIL_POINTS + ((m + 1) % TRAIL_POINTS) : m + 1;
      }
      ctx.moveTo(x, y); ctx.lineTo(nx, ny);
      P[k] = nx; P[k + 1] = ny; P[k + 2] = age + dt; FT[i] += dtf;
    }
    ctx.lineWidth = style.halo; ctx.strokeStyle = 'rgba(0,0,0,' + LOOK.headHalo + ')'; ctx.stroke();
    ctx.lineWidth = style.width; ctx.strokeStyle = 'rgba(255,255,255,' + LOOK.head + ')'; ctx.stroke();
    // the tails: the recorded positions, oldest first, up to the current position; a point older than TRAIL_SPAN_MS of
    // flow is left out, so a slow particle loop (one point per frame at the fast speeds) draws no longer a tail
    ctx.beginPath();
    for (i = 0, k = 0; i < n; i++, k += 4) {
      m = HN[i]; if (m < 2 && !(m === 1)) continue;
      var cnt = m < TRAIL_POINTS ? m : TRAIL_POINTS, start = m - cnt, first = true, ft = FT[i];
      for (q = start; q < m; q++) {
        var idx = (i * TRAIL_POINTS + (q % TRAIL_POINTS)) * 2;
        if (ft - HF[idx >> 1] > TRAIL_SPAN_MS) continue;
        if (first) { ctx.moveTo(H[idx], H[idx + 1]); first = false; } else ctx.lineTo(H[idx], H[idx + 1]);
      }
      if (first) ctx.moveTo(P[k], P[k + 1]); else ctx.lineTo(P[k], P[k + 1]);
    }
    ctx.lineWidth = style.halo; ctx.strokeStyle = 'rgba(0,0,0,' + LOOK.tailHalo + ')'; ctx.stroke();
    ctx.lineWidth = style.width; ctx.strokeStyle = 'rgba(255,255,255,' + LOOK.tail + ')'; ctx.stroke();
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
  // ---- the compass ribbon (plan section 37): the timeline laid out by real time ----
  // A fixed pointer with the dates and hours of the forecast scrolling under it. Every position comes from a frame's own
  // valid time (hourly frames to +120 h, then 3-hourly: three times as far apart); day labels sit at the real local
  // midnights of the forecast's zone and hour ticks at local 00/06/12/18, so a 23-hour or 25-hour day of a clock change
  // is drawn at its true width with its ticks on the clock hours. Nothing is invented between frames.
  var MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  var WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  var WEEKDAY_NAMES = { Sun: 'Sunday', Mon: 'Monday', Tue: 'Tuesday', Wed: 'Wednesday', Thu: 'Thursday', Fri: 'Friday', Sat: 'Saturday' };
  var RIBBON_WINDOW_H = 72;                                  // the viewport shows about three days
  var RIBBON_MIN_PX_H = 3, RIBBON_MAX_PX_H = 5;              // pixels per hour, bounded
  var RIBBON_MIN_TICK_GAP = 18;                              // hour ticks closer than this are thinned (06/18 first)
  // A pinned first-day label (left-aligned at x 0, 3 px in) needs this much room before the next one (centred on its
  // midnight): 3 + its width (a two-digit date is ~33-35 px at 11 px) + a gap + half the next label's width.
  var RIBBON_DAY_MIN_PX = 60;
  var RIBBON_TAP_PX = 4;                                     // a press that moves less than this is a tap, not a drag
  var RIBBON_AXIS_PX = 10;                                   // the travel at which a gesture's direction is decided (sideways = a scrub)
  // Local calendar parts in a zone, through one cached Intl formatter (en-US: fixed short month names, "7 AM" clocks).
  // An empty or unknown zone reads as the COMPUTER's zone, as the page's own clock (fmtTimeInTz) does, so the weekday and
  // the clock of a line can never come from two zones (G26 A-P3-2); without Intl at all, the computer's clock by hand.
  // Never throws: the cache keys are prefixed, so a zone string such as "constructor" can never hit an Object member
  // (G26 re-check RC-9).
  var FMT_CACHE = {};
  function ribbonFormatter(tz) {
    var key = 'z:' + (tz || '');
    if (FMT_CACHE[key]) return FMT_CACHE[key];
    var out = buildFormatter(tz); FMT_CACHE[key] = out; return out;
  }
  function buildFormatter(tz) {
    var f = null, zone = 'local';
    var o = { hourCycle: 'h23', weekday: 'short', year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' };
    try { f = new Intl.DateTimeFormat('en-US', tz ? Object.assign({ timeZone: tz }, o) : o); }
    catch (e) { try { f = new Intl.DateTimeFormat('en-US', o); } catch (e2) { f = null; } }
    try { if (f) zone = f.resolvedOptions().timeZone || zone; } catch (e3) { /* the name only */ }
    function parts(ms) {
      var d = new Date(ms);
      if (!f) return { y: d.getFullYear(), mo: MONTHS[d.getMonth()], d: d.getDate(), h: d.getHours(), mi: d.getMinutes(), wd: WEEKDAYS[d.getDay()] };
      var got = {}, p = f.formatToParts(d);
      for (var i = 0; i < p.length; i++) got[p[i].type] = p[i].value;
      return { y: +got.year, mo: got.month, d: +got.day, h: (+got.hour) % 24, mi: +got.minute, wd: got.weekday };   // some engines print "24" at midnight
    }
    function clockOf(p) { var h12 = p.h % 12 || 12, mi = p.mi ? ':' + (p.mi < 10 ? '0' : '') + p.mi : ''; return h12 + mi + ' ' + (p.h < 12 ? 'AM' : 'PM'); }
    return {
      zone: zone, parts: parts,
      day: function (ms) { var p = parts(ms); return p.mo + ' ' + p.d; },                               // "Oct 10"
      tick: function (ms) { var h = parts(ms).h; return (h < 10 ? '0' : '') + h; },                     // "06"
      clock: function (ms) { return clockOf(parts(ms)); },                                               // "7 AM", "7:30 PM"
      // the label above the pointer: "Sun, Oct 11" + "7 AM" (one formatting pass), and the weekday's full name
      stamp: function (ms) {
        var p = parts(ms);
        return { date: (p.wd ? p.wd + ', ' : '') + p.mo + ' ' + p.d, clock: clockOf(p), weekday: p.wd, weekdayName: WEEKDAY_NAMES[p.wd] || p.wd || '', day: p.mo + ' ' + p.d };
      }
    };
  }
  // The local midnight at or before ms. The first guess subtracts the clock time; on a clock-change day the elapsed time
  // since midnight differs from the clock time by the shift, which one correction from the guess's own clock reading fixes.
  function localMidnightBefore(ms, fmt) {
    var p = fmt.parts(ms), cand = ms - (p.h * 60 + p.mi) * 60000;
    p = fmt.parts(cand);
    if (p.h || p.mi) cand += p.h >= 12 ? (1440 - p.h * 60 - p.mi) * 60000 : -(p.h * 60 + p.mi) * 60000;
    return cand;
  }
  // Pixels per hour for a viewport: about three days visible (324 px -> 4.5; a 253-px phone sheet -> 3.5), within bounds.
  function ribbonScale(viewportW) {
    return Math.max(RIBBON_MIN_PX_H, Math.min(RIBBON_MAX_PX_H, (viewportW || 300) / RIBBON_WINDOW_H));
  }
  // The ribbon's geometry for a run: xs[i] = px of frame i from frame 0 (its hours since frame 0 x pxPerHour), hours[i] =
  // hours since the run, width, the day labels (one for every local date the run covers, where that date begins; the day frame 0 starts in is
  // pinned to x 0 and marked clamped, dropped when the next label would crowd it), the hour ticks (local 00/06/12/18;
  // 00/12 when 6 h is narrower than minGap px, then 00 only) and the span of the run in hours.
  function ribbonLayout(frames, runUtc, tz, pxPerHour, minGap) {
    var fmt = ribbonFormatter(tz), n = frames.length, t0 = Date.parse(frames[0].valid_utc), run = Date.parse(runUtc);
    var xs = new Array(n), hours = new Array(n), i, t;
    for (i = 0; i < n; i++) { t = Date.parse(frames[i].valid_utc); xs[i] = (t - t0) / 3.6e6 * pxPerHour; hours[i] = Math.round((t - run) / 3.6e6); }
    var last = Date.parse(frames[n - 1].valid_utc), width = xs[n - 1];
    var gap = minGap === undefined ? RIBBON_MIN_TICK_GAP : minGap;
    var every = 6 * pxPerHour >= gap ? 6 : 12 * pxPerHour >= gap ? 12 : 24;
    // A day label where the local DATE changes, not at "00:00": a clock change AT midnight gives a day without 00:00
    // (its label goes at the day's first hour) or a 00:00 twice (one label, one tick; G26 A-P2-1: Azores, Havana,
    // Santiago, Beirut, Cairo).
    var days = [], ticks = [], prevDay = null, prevTick = null;
    for (t = localMidnightBefore(t0, fmt); t <= last; t += 3.6e6) {
      var p = fmt.parts(t), x = (t - t0) / 3.6e6 * pxPerHour;
      if (p.mi) { t += (60 - p.mi) * 60000 - 3.6e6; continue; }   // a half-hour clock change (Lord Howe): back onto the local hours
      var day = p.mo + ' ' + p.d;
      if (day !== prevDay) { days.push({ x: Math.max(0, x), text: day, ms: t, clamped: x < 0 }); prevDay = day; }
      if (x >= 0 && p.h % every === 0 && day + ' ' + p.h !== prevTick) { ticks.push({ x: x, text: fmt.tick(t), major: p.h === 0, ms: t }); prevTick = day + ' ' + p.h; }
    }
    if (days.length > 1 && days[0].clamped && days[1].x < RIBBON_DAY_MIN_PX) days.shift();
    return { xs: xs, hours: hours, width: width, days: days, ticks: ticks, pxPerHour: pxPerHour, every: every,
      spanHours: (last - t0) / 3.6e6, zone: fmt.zone, fmt: fmt };
  }
  // The ribbon's position and the bookkeeping of a drag. offset = the px (0..width) of the time under the fixed pointer;
  // the ribbon follows the pointer one to one (dragging right shows earlier times); nearest() is monotone in the offset,
  // so a slow drag in either direction never picks a frame against its direction.
  function RibbonState(xs) {
    this.xs = xs; this.width = xs[xs.length - 1]; this.offset = 0;
    this.dragging = false; this.x0 = 0; this.o0 = 0; this.moved = 0;
  }
  RibbonState.prototype.clamp = function (o) { return Math.max(0, Math.min(this.width, o)); };
  RibbonState.prototype.nearest = function (o) {               // the frame whose px is nearest o (ties go later)
    var xs = this.xs, lo = 0, hi = xs.length - 1;
    if (hi <= 0) return 0;
    while (hi - lo > 1) { var mid = (lo + hi) >> 1; if (xs[mid] <= o) lo = mid; else hi = mid; }
    return o - xs[lo] < xs[hi] - o ? lo : hi;
  };
  RibbonState.prototype.setFrame = function (idx) { this.offset = this.xs[Math.max(0, Math.min(this.xs.length - 1, idx))]; };
  RibbonState.prototype.begin = function (px) { this.dragging = true; this.x0 = px; this.o0 = this.offset; this.moved = 0; };
  RibbonState.prototype.move = function (px) {                 // -> the frame now under the pointer, or null when not dragging
    if (!this.dragging) return null;
    this.moved = Math.max(this.moved, Math.abs(px - this.x0));
    this.offset = this.clamp(this.o0 - (px - this.x0));
    return this.nearest(this.offset);
  };
  // The release: a tap picks the frame under the finger where it TOUCHED DOWN (pxFromCentre = the touch-down point right
  // of the pointer = later); a drag snaps to the nearest frame. Returns the frame index; the offset then sits on it.
  RibbonState.prototype.end = function (pxFromCentre) {
    this.dragging = false;
    // under 4 px of sideways travel: a tap reads the touch-down point against the ribbon as it was at the press (the
    // offset at the press + the point), so a roll before lifting never counts, also at the run's ends where the ribbon
    // could not follow it (G26 re-check RC-5, fresh check F1). Without a finger position (a cancelled gesture) it goes back
    // exactly where it started, never to a neighbour.
    if (this.moved < RIBBON_TAP_PX) this.offset = typeof pxFromCentre === 'number' ? this.clamp(this.o0 + pxFromCentre) : this.o0;
    var idx = this.nearest(this.offset); this.offset = this.xs[idx]; return idx;
  };
  // A gesture that is no pick (a swipe up or down, a second finger): back where it started, whatever the travel.
  RibbonState.prototype.cancel = function () { this.dragging = false; this.offset = this.o0; return this.nearest(this.o0); };
  RibbonState.prototype.wheel = function (dx) { this.offset = this.clamp(this.offset + dx); return this.nearest(this.offset); };
  // "1:07 PM HST" in the computer's own time zone (not the forecast table's), with the weekday when it is not today there.
  function localClock(ms, now) {
    var d = new Date(ms), opts = { hour: 'numeric', minute: '2-digit', timeZoneName: 'short' };
    if (new Date(now).toDateString() !== d.toDateString()) opts.weekday = 'short';
    try { return new Intl.DateTimeFormat(undefined, opts).format(d); } catch (e) { return d.toISOString().slice(11, 16) + ' UTC'; }
  }
  var CYCLE_MS = 6 * 3.6e6;                                // a new model cycle every 6 h
  // When this run went live ("Updated:"), and when the next one is expected ("Next Update:"): one cycle later with this
  // run's own publish lag (published_utc + 6 h), rounded up to 5 minutes; null without a usable publish time.
  function runTimes(m, now, newer, cycleMs) {
    var run = Date.parse(m.run_utc), pub = Date.parse(m.published_utc);
    if (!isFinite(run) || !isFinite(pub) || pub < run) return null;
    var next = Math.ceil((pub + (cycleMs || CYCLE_MS)) / 3e5) * 3e5, status = newer ? 'newer' : next <= now ? 'shortly' : 'about';
    return { status: status, next: next, updated: localClock(pub, now),
      nextText: status === 'newer' ? 'a newer run is available' : status === 'shortly' ? 'expected shortly' : 'about ' + localClock(next, now) };
  }

  // ---- controller ----
  function Overlay(map, opts) {
    this.map = map; this.opts = opts;
    // Keys inside latest.json / the manifest are bucket-absolute (gfswave/0p25/v1/...); the
    // configured base is the v1 prefix URL, so resolve keys against the bucket root.
    this.base = String(opts.base).replace(/\/+$/, '');
    this.root = this.base.replace(/\/gfswave\/0p25\/v1$/, '');
    // The models offered and the one in use (the tab's save, when offered); every other model's base is its prefix under
    // the same bucket root (the GFS base stays the configured one, byte for byte).
    this.models = modelList(opts.models);
    this.field = null; this.layer = null; this.manifest = null; this.pointer = null; this.pointerAt = 0; this.newerRun = null;
    this.abort = null; this.readout = null; this.frameIndex = null; this.res = null; this.sheet = null; this.last = null;
    this._listeners = []; this._attributed = false; this.collapsed = undefined; this._touch = null; this._onVis = null;
    this.cache = new FrameCache(MAX_DECODED); this.inflight = {}; this.unavailable = {}; this.target = null; this.dir = 1; this.n = 0;
    this.playing = false; this.wasPlaying = false; this.timer = null; this.runTimer = null; this.ui = null; this._lutFor = null;
    this._due = 0;                                          // when the frame being shown was due (the drift-free cadence)
    this._painting = false; this._paintWaiting = null; this._paintRaf = null; this._paintTask = null; this._paintFallback = null;   // the paint gate
    this.playGen = 0; this.transientFails = 0; this._staleShown = false; this._pendingRestore = null; this._onHide = null;
    // opts.coast: clip wave height / peak period to the coastlines published beside the frames
    this.coast = opts.coast ? coastStore(this.root + '/static/coast/v1') : null;
    var s = saved();
    this.relief = null; this._base = 'imagery'; this._reliefFailed = 0;   // opts.baseLayer: the page's imagery (the wind swaps it)
    this.speed = speedOf(s.speed);
    this.model = modelOf(s.model, this.models);
    this._carry = null;                                      // the time and play state carried across a model switch
    this._switchT = null;                                    // the time the viewer chose, kept across consecutive switches (G30-15)
    // Contours (wave height, period) and the particle animation (every field) are always on (owner, 2026-09-26: no
    // checkboxes); the animation stays off under a reduced-motion setting and on runs without direction data.
    this.contours = true;
    this.anim = true;
    this.dcache = new FrameCache(MAX_DECODED); this.flow = null; this.dres = null;   // direction frames and their animator
  }
  Overlay.prototype._def = function () { return hasModel(this.model) ? MODEL_DEFS[this.model] : MODEL_DEFS[DEFAULT_MODEL]; };
  // The wait for a pointer or manifest answer before the error panel (which has the dropdown) shows (G30-3).
  Overlay.prototype._loadStallMs = LOAD_STALL_MS;
  // The pointer / manifest base of the model in use: the default model's is the configured base as given (as before
  // the dropdown), another model's its prefix under the bucket root.
  Overlay.prototype._modelBase = function () { return this.model === DEFAULT_MODEL ? this.base : this.root + '/' + this._def().prefix; };
  // Everything of the run in hand goes: manifest, pointer, banner, caches, the frame on the map and its particles, the
  // credit. Never one model's picture (or its time, its warnings) under another's name.
  Overlay.prototype._dropRun = function () {
    this.manifest = null; this.pointer = null; this.pointerAt = 0; this.newerRun = null; this.n = 0;
    this.cache.clear(); this.dcache.clear(); this.unavailable = {}; this.frameIndex = null; this.target = null;
    this.transientFails = 0; this._staleShown = false; this.wasPlaying = false;
    if (this.layer) this.layer.clear();
    if (this.flow) this.flow.clearData();
    if (this.readout) this.readout.hidden = true;
    this._unattribute();
    // Under wind the relief stays while the next model's wind loads (its landing calls _syncLook; Off restores the
    // imagery): a switch never flashes the satellite map (G30-8).
    if (this.field !== 'wind') this._syncLook();
  };
  // The model dropdown (plan section 43): the choice is saved for the tab. Off: the next mount loads the new model.
  // On: the requested time and the play state are carried (this._carry) into the ONE loading path, mount(): the old
  // run goes first (abort, drop), so a decode of the old model can never land, be cached or be drawn; the carry is
  // spent when a frame of the new model lands (a failed load keeps it for Retry, a field switch meanwhile, or a switch
  // back); a switch while nothing is on the map keeps an earlier carry.
  // The time is the one the viewer chose: kept across consecutive switches until the viewer moves the time (seek, step,
  // play, Update; G30-15), else the requested frame (a pending seek's target, else the frame on the map). The choice is
  // saved for the tab when the new model's first frame lands (_persist), so a load that never ends cannot trap a reload on
  // it (G30-3, G30-5); the old model's save keeps the play state meanwhile.
  Overlay.prototype.setModel = function (key) {
    if (!hasModel(key) || this.models.indexOf(key) < 0 || key === this.model) return;
    if (!this.field) { this.model = key; save({ model: key }); this._dropRun(); return; }
    var keep = this.frameIndex !== null ? this._anchorIndex() : null;
    var shown = keep !== null && this.manifest && this.manifest.frames[keep] ? Date.parse(this.manifest.frames[keep].valid_utc) : null;
    var t = this._switchT !== null ? this._switchT : shown;
    var playing = this.frameIndex === null && this._carry ? this._carry.playing : !!(this.playing || this.wasPlaying);
    this.pause();                                            // playGen++: the old tick chain and its paint gate die (saves the OLD model)
    if (playing && shown !== null) save({ playing: true });  // a reload before the new model lands resumes as it was
    this.model = key;
    this.abortAll();
    this._dropRun();
    if (t !== null) { this._carry = { t: t, playing: playing }; this._switchT = t; }
    this.mount(this.field);
  };
  // restore: true on the mount the page makes after a reload (the field was saved in this tab); the
  // saved valid time and play state are then reused when they are recent (restoreIndex), by this mount
  // or, if it never lands a frame, by the next one (another field picked meanwhile, Retry) until Off.
  Overlay.prototype.mount = function (fieldName, restore) {
    if (restore) this._pendingRestore = saved();
    // The time position survives a field change and even a run change (nearest valid time), like Update. A field switch
    // keeps the REQUESTED time (a scrub or step still loading) when there is one; Retry (the same field again) goes back
    // to the frame on the map, which is cached (G4 B1). Taken before anything aborts. Before any frame is on the map the
    // saved state decides, as before (a reload's restore, its play state included).
    var keep = this.manifest && this.frameIndex !== null ? (fieldName !== this.field ? this._anchorIndex() : this.frameIndex) : null;
    var self = this, st = this._pendingRestore, carry = this._carry;
    if (!this.map.getPane('modelPane')) {
      var pane = this.map.createPane('modelPane'); pane.style.zIndex = 250; pane.style.pointerEvents = 'none';
    }
    this.field = fieldName;
    this.abortAll();
    this.abort = new AbortController();
    var sig = this.abort.signal;
    if (!this.layer) {
      this.layer = new ModelGridLayer({ opacity: this._opacityFor(fieldName) }).addTo(this.map);
      this._bindReadout();
      this._bindMap();
      this._bindDocument();
    } else if (this.layer.field !== fieldName) {
      this.layer.clear();                                  // never one field's picture under another's label
      if (this.flow) this.flow.clearData();
      // nothing drawn: the imagery, the waves' opacity; wind -> wind (a model switch) keeps the relief while the next
      // wind frame loads (G30-8: never a satellite flash under the wind; its landing calls _syncLook)
      if (fieldName !== 'wind') this._syncLook();
    }
    this.render({ state: 'loading' });
    var prevValid = keep !== null && this.manifest.frames[keep] ? Date.parse(this.manifest.frames[keep].valid_utc) : null;
    var prevRun = this.manifest ? this.manifest.run : null;
    // The coastlines load beside the pointer/manifest/frame; the FIRST DRAW waits for them (a filled
    // field spilling onto land and then snapping back is the wrong picture), later draws never do.
    // Wind draws the coastlines as lines only: it never waits for them, they join when loaded.
    var waitCoast = !!CLIP_FIELDS[fieldName], coastP = this.coast && (waitCoast || OUTLINE_FIELDS[fieldName]) ? this.coast.load() : Promise.resolve(null);
    if (!waitCoast) coastP.then(function (store) {                           // (not tied to this mount's signal: an Update meanwhile keeps them)
      if (!self.layer || !store || self.layer._coast === store) return;
      self.layer.setCoast(store);
      if (self.layer.hasFrame()) self._attribute();
    }, function () {});
    this._loadManifest(sig).then(function (m) {
      if (!m.fields[fieldName] || !RAMPS[fieldName]) throw new Error('layer "' + fieldName + '" is not in this run');
      return (waitCoast ? coastP : Promise.resolve(undefined)).then(function (store) { return [m, store]; });
    }).then(function (ms) {
      var m = ms[0], store = ms[1];
      if (sig.aborted || !self.layer) throw abortError();
      if (store !== undefined && self.layer._coast !== (store || null)) self.layer.setCoast(store);
      self.n = m.frames.length;
      var rIdx = prevValid === null && st ? restoreIndex(m, st, Date.now()) : null;
      var idx = prevValid === null ? (carry ? nearestIndex(m, carry.t) : rIdx !== null ? rIdx : pickFrame(m)) : m.run === prevRun ? Math.min(keep, self.n - 1) : nearestIndex(m, prevValid);
      var resume = carry ? carry.playing : rIdx !== null && st.playing === true;
      self.res = wantHalf(self.map.getZoom(), self._dims().w, fieldName) ? 'half' : 'full';
      self._ensureFlow();
      if (!self.runTimer) self.runTimer = setInterval(function () { self._checkRun(); }, RUN_CHECK_MS);
      // "Next Update: about …" turns into "expected shortly" on an idle panel too (G26 A-P3-7): once a minute, in place
      if (!self.lineTimer) self.lineTimer = setInterval(function () { if (self.ui && self.ui.runText) self._refreshRunLine(); }, 60000);
      return self._goto(idx, sig).catch(function (err) {
        // the chosen first frame is missing: show the next one that exists rather than nothing
        if (!(err && err.unavailable) || (sig && sig.aborted)) throw err;
        var next = nextAvailable(idx, 1, self.n, function (j) { return self._isUnavailable(j); });
        if (next === null) throw err;
        return self._goto(next, sig);
      }).then(function () {
        // it was playing before the reload: keep playing, unless the viewer asks for reduced motion (a model switch
        // carries the viewer's own playback: it keeps playing, as Update and a field switch do; G30-2);
        // in a hidden tab it resumes when the tab is shown (the same path as the visibility pause)
        if (!resume || sig.aborted || (!carry && reducedMotion())) return;
        if (document.hidden) { self.wasPlaying = true; self._persist(); } else self.play();
      });
    }).catch(function (err) { self._fail(sig, err); });
  };
  // Cache / in-flight / unavailable keys carry the model and the run: a decode that outlives an Update or a model
  // switch can never be taken for a frame of the new run (both models name their runs YYYYMMDDHH).
  // kind 'dir': the direction frame (pdir / wdir at its own resolution) of the same step.
  Overlay.prototype._key = function (idx, kind) {
    var dir = kind === 'dir';
    return this.model + '/' + this.manifest.run + '/' + (dir ? this.dres : this.res) + '/' + (dir ? DIR_FIELDS[this.field] : this.field) + '/' + this.manifest.frames[idx].step;
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
    var m = this.manifest, ctrl = new AbortController(), rec, half = (dir ? this.dres : this.res) === 'half', kindName = dir ? 'dir' : 'field', url;
    try { url = this.root + '/' + frameKey(m, m.frames[idx], dir ? DIR_FIELDS[this.field] : this.field, half); }
    catch (e) { return Promise.reject(e); }                                  // never a throw: _prefetch runs from UI handlers (setSpeed, pause)
    function mine() { return self.inflight[key] === rec; }
    // The watchdog times the DOWNLOAD only (until the body has arrived): a decode that waits behind a busy page
    // (fast playback on a large screen) is late, never missing. A download that stalls (one took 6.5 s at G4) is
    // asked again at once, LATE_TRIES times in all; only then does it count as a failure ("frame stalled").
    function attempt(n) {
      var inner = new AbortController(), stalled = false, onOuter = function () { inner.abort(); };
      ctrl.signal.addEventListener('abort', onOuter);
      var watchdog = setTimeout(function () { stalled = true; inner.abort(); }, Math.max(STALL_MS, 4 * self._interval()));
      function done() { clearTimeout(watchdog); ctrl.signal.removeEventListener('abort', onOuter); }
      return decodeFrame(url, inner.signal, half ? m.grid_half : m.grid, function () { clearTimeout(watchdog); }).then(function (frame) {
        done(); return frame;
      }, function (err) {
        done();
        if (stalled && !ctrl.signal.aborted && self.manifest === m) {
          if (n + 1 < LATE_TRIES) return attempt(n + 1);                    // late: asked again, not marked
          err = new Error('frame stalled');
        }
        throw err;
      });
    }
    var p = attempt(0).then(function (frame) {
      if (mine()) delete self.inflight[key];
      // A decode cannot be cancelled: one that outlives its abort (Update, Off, field or resolution
      // change) must neither be cached nor delivered.
      if (ctrl.signal.aborted || self.manifest !== m) throw abortError();
      if (!dir) self.transientFails = 0;                                      // the outage counter follows the field frames only
      cache.set(key, frame, self.frameIndex !== null ? self._key(self.frameIndex, kind) : null, self._rankMap(), dir ? (self.flow ? self.flow.dir : null) : (self.layer ? self.layer._frame : null));
      self._prefetch();                                                        // the ring refills on every landing, not only the target's (plan section 42)
      return frame;
    }, function (err) {
      if (mine()) delete self.inflight[key];
      if (ctrl.signal.aborted || self.manifest !== m) throw abortError();
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
  // The ring budget for the moment: by speed while playing, the paused one otherwise.
  Overlay.prototype._ring = function () { return ringFor(this.speed, this.playing); };
  Overlay.prototype._maxInflight = function () { return this._ring().inflight; };
  Overlay.prototype._plan = function (idx) {
    var r = this._ring(), plan = ringPlan(idx, this.n, this.dir, r.ahead, r.behind), want = this._wantDir(), out = [];
    for (var i = 0; i < plan.length; i++) { out.push([plan[i], 'field']); if (want) out.push([plan[i], 'dir']); }
    return out;
  };
  // Keep the in-flight set to what the target needs: everything outside the new ring is dropped, and beside the
  // target's own fetches (its field frame, unless it is decoded already, and its direction frame) only as many ring
  // fetches survive as the in-flight cap leaves room for (timeline drags fire many seeks), the NEAREST ones by the
  // ring's rank. A target already decoded (the next step while playing) needs no slot: the ring fetches ahead of it
  // keep theirs (plan section 42: reserving one anyway and dropping the OLDEST survivor, usually the next frame
  // needed, made 1 in 4 frames at 4x be fetched twice and the next step wait for it).
  Overlay.prototype._trimInflight = function (idx) {
    var rank = {}, plan = this._plan(idx), i, k;
    for (i = 0; i < plan.length; i++) rank[this._key(plan[i][0], plan[i][1])] = i;
    var target = this._key(idx), dtarget = this._wantDir() ? this._key(idx, 'dir') : null, survivors = [];
    var room = this._maxInflight() - (this.cache.has(target) ? 0 : 1);
    for (k in this.inflight) if (rank[k] === undefined) { this.inflight[k].abort.abort(); delete this.inflight[k]; } else if (k === dtarget) room--; else if (k !== target) survivors.push(k);
    survivors.sort(function (a, b) { return rank[a] - rank[b]; });
    while (survivors.length > Math.max(0, room)) { k = survivors.pop(); this.inflight[k].abort.abort(); delete this.inflight[k]; }
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
    // The FIRST picture of a field (after a mount or a field switch) never waits for it: the particles join when
    // their frame lands (G15 P2-1: with the animation always on, waiting made every first wind frame ~2x slower).
    var pd = this._wantDir() && this.layer && this.layer.hasFrame() && !this._isUnavailable(idx) ? this._dirReady(idx) : null;
    return (pd ? Promise.all([p, pd]).then(function (both) { return both[0]; }) : p).then(function (frame) {
      if ((sig && sig.aborted) || self.target !== idx || !self.layer || self.manifest !== m || self.field !== field || self.res !== res) throw abortError();
      // the frame already on the map (Retry after an outage, a repeated seek): no redraw, but the same
      // state transition as a fresh landing, or the panel would stay on "Loading"
      if (self.layer._frame !== frame) {
        var half = res === 'half';
        self.layer.setContours(self._contourCfg(), true);                        // (setFrame redraws with it)
        self.layer.setFrame(frame, half ? m.grid_half : m.grid, field, m.fields[field], self._lut(), m.frames[idx]);
      }
      self.frameIndex = idx; self._pendingRestore = null; self._carry = null;   // a frame is on the map: the saved state is spent
      self._persist();
      self._syncLook();
      self._attribute();
      if (!self.last || self.last.state !== 'ready') self.render({ state: 'ready' }); else self._syncUI();
      self._syncFlow(idx);
      self._prefetch();
    });
  };
  // The step the ring is planned around: a pending seek / step target, else the frame on the map.
  Overlay.prototype._anchorIndex = function () { return this.target !== null ? this.target : this.frameIndex; };
  // ?ovdebug=1: the animator's and the scheduler's state in the panel, once a second (the owner's iPad; text only).
  Overlay.prototype._debugText = function () {
    var f = this.flow ? this.flow.debugState() : null, dec = frameDecoder ? (frameDecoder.dead ? 'dead' : (frameDecoder.worker ? 'worker' : 'idle')) : (decoderDead ? 'dead' : 'none');
    var o = { model: this.model, playing: this.playing, speed: this.speed, frame: this.frameIndex, target: this.target, res: this.res + '/' + this.dres, inflight: Object.keys(this.inflight).length,
      cache: this.cache.size() + '/' + this.dcache.size(), decoder: dec, hidden: typeof document !== 'undefined' ? !!document.hidden : null, dpr: typeof window !== 'undefined' ? window.devicePixelRatio : null };
    var lines = [], k;
    for (k in o) lines.push(k + ': ' + o[k]);
    if (f) for (k in f) lines.push('flow.' + k + ': ' + f[k]); else lines.push('flow: none');
    return lines.join('\n');
  };
  Overlay.prototype._debugOn = function () {
    var self = this;
    if (this.debugTimer) clearInterval(this.debugTimer);
    var tick = function () {                                   // no readout element (a sheet without room): the timer stops; the next render brings it back
      var d = self.ui && self.ui.debug;
      if (d && d.isConnected !== false) d.textContent = self._debugText();
      else if (self.debugTimer) { clearInterval(self.debugTimer); self.debugTimer = null; }
    };
    tick();
    this.debugTimer = setInterval(tick, 1000);
    if (this.debugTimer && this.debugTimer.unref) this.debugTimer.unref();
  };
  // The ring's plan as ranks (key -> place: the target's field, its direction, then the ring interleaved): what the
  // decoded-frame caches keep when they are over their cap. null before any target.
  Overlay.prototype._rankMap = function () {
    var a = this._anchorIndex(), rank = {}, plan, i;
    if (a === null || !this.manifest) return null;
    plan = this._plan(a);
    for (i = 0; i < plan.length; i++) rank[this._key(plan[i][0], plan[i][1])] = i;
    return rank;
  };
  // Keep the ring (current, ahead, behind: the counts by speed, with the direction frames interleaved while the
  // Animation is on) decoded with at most the cap's fetches; drop fetches the ring no longer wants (direction,
  // field, resolution, Animation or the ring's size changed), and above the cap (a pause, a slower speed) the
  // lowest-ranked ring fetches, never the target's own field or direction fetch (rank 0 / 1). Planned around a
  // PENDING target, so a seek that is still loading is never aborted by a change of Animation or of the direction's
  // resolution. Called on every landing (_ensure) as well as on play / pause / speed changes; shrinkOnly: trim the
  // caches and the in-flight set to the budget without starting anything (a pause).
  Overlay.prototype._prefetch = function (shrinkOnly) {
    if (this.frameIndex === null || !this.manifest || !this.field) return;
    var r = this._ring(), cap = r.inflight, plan = this._plan(this._anchorIndex()), rank = {}, i, k, keys;
    var shown = this.frameIndex, keepF = this._key(shown), keepD = this._wantDir() ? this._key(shown, 'dir') : null;
    for (i = 0; i < plan.length; i++) rank[this._key(plan[i][0], plan[i][1])] = i;
    var drawnF = this.layer ? this.layer._frame : null, drawnD = this.flow ? this.flow.dir : null;
    this.cache.limit(r.decoded, keepF, rank, drawnF); this.dcache.limit(r.decoded, keepD, rank, drawnD);
    for (k in this.inflight) if (rank[k] === undefined) { this.inflight[k].abort.abort(); delete this.inflight[k]; }
    keys = Object.keys(this.inflight);
    if (keys.length > cap) {
      keys.sort(function (a, b) { return rank[b] - rank[a]; });
      for (i = 0; i < keys.length && Object.keys(this.inflight).length > cap && rank[keys[i]] >= 2; i++) { this.inflight[keys[i]].abort.abort(); delete this.inflight[keys[i]]; }
    }
    if (shrinkOnly) return;
    // Only what the caches can hold beside the frame on the map is started: with a pending target far from it (a seek
    // into a cooldown, a failed far seek) the ring and the drawn frame together exceed the budget by one, and a ring
    // frame fetched into a full cache would evict another ring frame, whose landing would fetch it back, without end.
    // The same reserve while the frame on the map (or the animator's direction) is the OTHER resolution's, kept by value
    // beside the ring until the new one lands.
    var ring = ringPlan(this._anchorIndex(), this.n, this.dir, r.ahead, r.behind);
    var drawnInRing = ring.indexOf(shown) >= 0 && (!drawnF || this.cache.map.get(keepF) === drawnF) && (!keepD || !drawnD || this.dcache.map.get(keepD) === drawnD);
    var room = r.decoded - (drawnInRing ? 0 : 1);
    for (i = 0; i < plan.length && Object.keys(this.inflight).length < cap; i++) {
      var idx = plan[i][0], kind = plan[i][1], key = this._key(idx, kind);
      if (ring.indexOf(idx) >= room) continue;
      if (this.inflight[key] || this._isUnavailable(idx, kind) || (kind === 'dir' ? this.dcache : this.cache).has(key)) continue;
      if (kind === 'dir') this._ensureDir(idx); else this._ensure(idx).catch(function () {});
    }
  };
  Overlay.prototype._fail = function (sig, err) {
    if (sig && sig.aborted) return;
    if (err && err.name === 'AbortError') return;
    // A load that failed with no frame of the run in hand (an Update whose frames never came): its predecessor's picture
    // goes, never a picture without its frame and run; and the basemap follows what is drawn (a failed wind switch kept the
    // bare relief: re-check R-2).
    if (this.frameIndex === null && this.layer && this.layer.hasFrame()) {
      this.layer.clear();
      if (this.flow) this.flow.clearData();
      if (this.readout) this.readout.hidden = true;
      this._unattribute();
    }
    if (this.layer && !this.layer.hasFrame()) this._syncLook();
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
    return this._fetchJson(this._modelBase() + '/latest.json', { cache: 'no-cache' }, sig, 'latest').catch(function (err) {
      // A failed re-read must not take away a validated run the session already holds: keep it and back off.
      if (cached && !(sig && sig.aborted)) { self.pointerAt = Date.now(); return null; }
      throw err;
    }).then(function (ptr) {
      if (sig && sig.aborted) throw abortError();              // a model switch meanwhile: this pointer is the old model's
      if (ptr === null) return cached;
      if (!ptr || !ptr.complete || !ptr.run || !ptr.manifest || isNaN(Date.parse(ptr.published_utc))) throw new Error('published run is not complete');
      self.pointerAt = Date.now();
      if (self.manifest && self.manifest.run === ptr.run) { self.pointer = ptr; return self.manifest; }
      if (self.manifest && self.frameIndex !== null) { self.newerRun = ptr; return self.manifest; }   // a time position is pinned: banner only
      return self._fetchManifest(ptr, sig).then(function (m) {
        if (sig && sig.aborted) throw abortError();            // a switch meanwhile: never adopt the old load's run (G30-7)
        self._adopt(m, ptr); return m;
      });
    });
  };
  Overlay.prototype._loadManifest = (function (inner) {
    // A run announced by the banner is adopted on the next mount after Off (no time position is pinned
    // any more); a field change while On keeps the pinned run and the banner.
    return function (sig) {
      var self = this;
      if (this.newerRun && this.manifest && this.frameIndex === null) {
        var ptr = this.newerRun;
        return this._fetchManifest(ptr, sig).then(function (m) {
          if (sig && sig.aborted) throw abortError();
          self._adopt(m, ptr); self.pointerAt = Date.now(); return m;
        });
      }
      return inner.call(this, sig);
    };
  })(Overlay.prototype._loadManifest);
  // A JSON answer (pointer, manifest) within _loadStallMs: a request that answers nothing by then is given up as an error
  // (the error panel has the dropdown and Retry; G30-3). The signal's abort ends it at once; the timer never outlives it.
  Overlay.prototype._fetchJson = function (url, init, sig, label) {
    var ms = this._loadStallMs;
    return new Promise(function (resolve, reject) {
      var over = false, ctrl = typeof AbortController === 'function' ? new AbortController() : null, timer = null, opts = {}, k;
      var finish = function (fn, v) { if (over) return; over = true; clearTimeout(timer); if (sig && sig.removeEventListener) sig.removeEventListener('abort', onAbort); fn(v); };
      var onAbort = function () { if (ctrl) ctrl.abort(); finish(reject, abortError()); };
      if (sig && sig.aborted) { reject(abortError()); return; }
      if (sig && sig.addEventListener) sig.addEventListener('abort', onAbort);
      for (k in init) opts[k] = init[k];
      if (ctrl) opts.signal = ctrl.signal; else if (sig) opts.signal = sig;
      timer = setTimeout(function () { if (ctrl) ctrl.abort(); finish(reject, new Error('no answer from the frame host')); }, ms);
      fetch(url, opts).then(function (r) {
        if (!r.ok) throw new Error(label + ' ' + r.status);
        return r.json();
      }).then(function (v) { finish(resolve, v); }, function (err) { finish(reject, err); });
    });
  };
  Overlay.prototype._fetchManifest = function (ptr, sig) {
    return this._fetchJson(this.root + '/' + ptr.manifest, {}, sig, 'manifest').then(function (m) {
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
    var self = this, model = this.model, m = this.manifest;
    if (!m) return;
    fetch(this._modelBase() + '/latest.json', { cache: 'no-cache' }).then(function (r) { return r.ok ? r.json() : null; }).then(function (ptr) {
      if (self.model !== model || self.manifest !== m) return;                // a model switch (or an Update) meanwhile: the answer is the old run's
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
  Overlay.prototype._isStale = function () { return !!this.pointer && (Date.now() - Date.parse(this.pointer.published_utc)) / 1000 > this._def().staleS; };
  // "Update" on the banner: switch to the announced run at the nearest valid time, keep the play state.
  Overlay.prototype.update = function () {
    var self = this, ptr = this.newerRun;
    if (!ptr || !this.field) return;
    var wasPlaying = this.playing, keep = this.frameIndex !== null ? this._anchorIndex() : null; this.pause();   // the requested time while one is loading
    var prevValid = keep !== null && this.manifest.frames[keep] ? Date.parse(this.manifest.frames[keep].valid_utc) : this._carry ? this._carry.t : Date.now();
    this._switchT = null;
    // A landing of the run being replaced (a playback step or a seek decoded before the Update) must not pass _goto's checks
    // and put the old run's ready panel over the Update's loading one (fresh check of G30: the old picture with no frame).
    this.target = null;
    this.abortAll(); this.abort = new AbortController();
    var sig = this.abort.signal;
    this.render({ state: 'loading' });
    this._fetchManifest(ptr, sig).then(function (m) {
      if (sig.aborted) throw abortError();
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
    this._switchT = null;                                    // the time moves: a later switch starts from where it is
    this.playing = true; this.dir = 1; this.playGen++; this._due = Date.now(); this._paintClear(); this._syncUI();
    this._prefetch();                                        // the ring grows to the speed's budget at once
    this._syncRate();
    this._tick(this.playGen); this._persist();
  };
  Overlay.prototype.pause = function () {
    this.playing = false; this.playGen++;
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    this._paintClear();
    this._prefetch(true);                                    // back to the paused budget (nothing new is started)
    this._syncRate();
    this._syncUI(); this._persist();
  };
  // The particles' pace follows the playback: x1 paused or at 1x, x2 at 2x, x4 at 4x (plan section 42).
  Overlay.prototype._flowRate = function () { return this.playing ? this.speed : 1; };
  Overlay.prototype._syncRate = function () { if (this.flow) this.flow.setRate(this._flowRate()); };
  // The state a reload restores (with the field the page saved): the valid time on the map, whether it
  // is playing (a visibility pause still counts as playing), and when this was written.
  Overlay.prototype._persist = function () {
    if (!this.field || !this.manifest || this.frameIndex === null || !this.manifest.frames[this.frameIndex]) return;
    save({ field: this.field, model: this.model, t: Date.parse(this.manifest.frames[this.frameIndex].valid_utc),
      playing: !!(this.playing || this.wasPlaying), at: Date.now() });
  };
  Overlay.prototype._tick = function (gen) {
    var self = this;
    if (!this.playing || gen !== this.playGen) return;
    if (this._painting) { this._paintWaiting = function () { self._tick(gen); }; return; }   // the last landing is not on screen yet
    var un = function (j) { return self._isUnavailable(j); };
    var pending = this.target !== null && this.target !== this.frameIndex;
    var next = pending && !un(this.target) ? this.target                      // a seek or a slow frame: wait for it
      : nextAvailable(pending ? this.target : this.frameIndex, 1, this.n, un);
    if (next === null) { this.pause(); return; }
    this._goto(next).then(function () { self._paintWait(gen); self._after(gen); }, function (err) {
      if (err && err.outage) self._fail(null, err);
      else if (err && (err.unavailable || err.name === 'AbortError')) self._after(gen);   // skipped or superseded: move on
      else self._fail(null, err);
    });
  };
  // The next frame is due one interval after the last one was DUE, not after it landed: timer latency and the
  // particles' frames no longer add up (the snail showed 1.92 of 2 frames/s with everything cached). A frame that
  // lands late (past its successor's due time) is followed by that successor as soon as it has been rendered (the
  // paint gate below), one back-to-back frame, and the cadence restarts from there (the due time never lies in the
  // past); a frame is never skipped.
  // The paint gate (G29 B-P1-1): a landing is put on screen by the browser's next rendering update, which follows the
  // animation-frame callbacks; a successor that was already decoded and due at once landed in the very next task,
  // before that update, and replaced the late frame unseen (3-12 % of the frames at 1080p with particles, where the
  // GPU renders only ~8 times a second). So after a landing the next tick runs from a task scheduled by the first
  // animation frame after it (the update has completed by then), never earlier; without an animation frame for
  // PAINT_FALLBACK_MS (a hidden or throttled page) it runs anyway, so playback never stalls where frames stop. Seeks,
  // steps and scrubs are not gated; play, pause and Off clear it. Only the CURRENT play arms it: a frame that lands for an
  // older play (a pause + play, or a hide + show, while it was loading) or after a pause arms nothing, since clearing
  // the gate would drop the current play's waiting tick and leave playback stranded (G29 re-check R-1 / R-2). The gate
  // runs the waiter it holds; the waiter (a tick) checks its own generation.
  Overlay.prototype._paintWait = function (gen) {
    var self = this;
    if (!this.playing || gen !== this.playGen) return;
    this._paintClear();
    if (typeof requestAnimationFrame !== 'function') return;
    var done = function () { if (!self._painting) return; var w = self._paintWaiting; self._paintClear(); if (w) w(); };
    this._painting = true;
    this._paintRaf = requestAnimationFrame(function () { self._paintRaf = null; self._paintTask = setTimeout(done, 0); });
    this._paintFallback = setTimeout(done, PAINT_FALLBACK_MS);
  };
  Overlay.prototype._paintClear = function () {
    if (this._paintRaf !== null && typeof cancelAnimationFrame === 'function') cancelAnimationFrame(this._paintRaf);
    if (this._paintTask) clearTimeout(this._paintTask);
    if (this._paintFallback) clearTimeout(this._paintFallback);
    this._paintRaf = this._paintTask = this._paintFallback = null; this._painting = false; this._paintWaiting = null;
  };
  Overlay.prototype._after = function (gen) {
    var self = this;
    if (!this.playing || gen !== this.playGen) return;
    if (this.timer) clearTimeout(this.timer);
    var now = Date.now(), iv = this._interval();
    this._due = Math.max(this._due + iv, now);
    this.timer = setTimeout(function () { self.timer = null; self._tick(gen); }, Math.max(0, this._due - now));
  };
  // One available frame back or forward (the ribbon's arrow keys); it stops at the first and last available frame.
  Overlay.prototype.step = function (dir) {
    var self = this;
    this.pause();
    if (!this.manifest || this.frameIndex === null) return;
    this._switchT = null;
    var base = this.target !== null ? this.target : this.frameIndex;
    var next = stepAvailable(base, dir, this.n, function (j) { return self._isUnavailable(j); });
    if (next !== null) { this.dir = dir; this._goto(next).catch(function (err) { self._afterMiss(err); }); }
  };
  Overlay.prototype.seek = function (idx) {
    var self = this;
    if (!this.manifest || this.frameIndex === null) return;
    this._switchT = null;
    idx = Math.max(0, Math.min(this.n - 1, idx | 0));
    this._goto(idx).catch(function (err) { self._afterMiss(err); });        // unavailable: the drawn frame stays, the hint says so
  };
  // A seek or step that did not land: an outage (3 transient failures in a row) shows the Retry state
  // like playback does; a single missing frame just leaves the hint in the valid-time line.
  Overlay.prototype._afterMiss = function (err) { if (err && err.outage) this._fail(null, err); else this._syncUI(); };
  Overlay.prototype.setSpeed = function (v) {
    if (SPEEDS.indexOf(v) < 0) return;
    this.speed = v; save({ speed: v });
    this._prefetch();                                        // the ring follows the speed at once (while playing)
    this._syncRate();
    this._syncUI();
  };
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
    var text = this._def().attribution;
    if (!(this.layer && this.layer._masked() && this.coast)) return text;
    var url = String(this.coast.url + '/LICENSE.txt').replace(/["<>]/g, encodeURIComponent);
    return text + ' · <a href="' + url + '" target="_blank" rel="noopener license">GSHHG</a>';
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
    this._on('move', function () { if (self.layer) self.layer._drawStale(true); });   // a buffer tile a frame behind comes into view
    this._on('resize', function () { self._onSize(); });
    // The page sizes #map by script and has Leaflet re-read the size before invalidateSize(), so the map's own 'resize'
    // event does not fire on a window resize or a rotation: the window and the container are watched as well (as the
    // map tools do), and each new size is handled once.
    this._sizeKey = this._sizeOf(); this._sizeH = this._dims().h;
    this._onWinSize = function () {
      if (self._sizeTimer) clearTimeout(self._sizeTimer);
      self._sizeTimer = setTimeout(function () { self._sizeTimer = null; self._onSize(); }, 100);
    };
    if (window.addEventListener) { window.addEventListener('resize', this._onWinSize); window.addEventListener('orientationchange', this._onWinSize); }
    if (typeof window.ResizeObserver === 'function') { this._ro = new window.ResizeObserver(this._onWinSize); this._ro.observe(this.map.getContainer()); }
  };
  Overlay.prototype._sizeOf = function () { var d = this._dims(); return d.w + 'x' + d.h; };
  // A new map size: the attribution's width, the frame resolution, and the panel (rebuilt when it has to move between the
  // control and the sheet, when it is the sheet (its cap follows the map height) and on a desktop height change (the room
  // for the details follows it: G26 B-1); a width-only change on a desktop re-measures the ribbon).
  Overlay.prototype._onSize = function () {
    var key = this._sizeOf(), h = this._dims().h;
    if (key === this._sizeKey) return;                       // the map's event, the window and the observer report one change
    var hChanged = this._sizeH !== undefined && h !== this._sizeH;
    this._sizeKey = key; this._sizeH = h;
    this._sizeAttribution(); this._checkRes();
    if (!this.last) return;
    var compactNow = this.isCompact(), wasCompact = !!this.sheet;
    // a desktop height change re-renders too: the room for the details (and the fold) follows the map's height (G26 B-1)
    if (compactNow !== wasCompact || this.sheet || hChanged) this.render(this.last); else { this._ribbonMeasure(); this._syncUI(); this._layoutSheet(); }
  };
  Overlay.prototype._unbindSize = function () {
    if (this._onWinSize && window.removeEventListener) { window.removeEventListener('resize', this._onWinSize); window.removeEventListener('orientationchange', this._onWinSize); }
    if (this._ro) { this._ro.disconnect(); this._ro = null; }
    if (this._sizeTimer) { clearTimeout(this._sizeTimer); this._sizeTimer = null; }
    this._onWinSize = null;
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
    this._closeMenus();
    this.field = null;                                     // first: the pause below must not save the layer as still on
    this.pause();
    this.abortAll();
    disposeDecoder();                                      // the decode worker goes with the layer (nothing runs after Off)
    if (this.debugTimer) { clearInterval(this.debugTimer); this.debugTimer = null; }
    if (this.runTimer) { clearInterval(this.runTimer); this.runTimer = null; }
    if (this.lineTimer) { clearInterval(this.lineTimer); this.lineTimer = null; }
    if (this._onVis) { document.removeEventListener('visibilitychange', this._onVis); this._onVis = null; }
    if (this._onHide) { if (window.removeEventListener) window.removeEventListener('pagehide', this._onHide); this._onHide = null; }
    this._pendingRestore = null; this._pendingFocus = null; this._carry = null; this._switchT = null;
    if (this.layer) { this.map.removeLayer(this.layer); this.layer = null; }
    this._syncLook();                                      // the page's imagery back, no blend
    if (this.coast) this.coast.abortAll();                 // the decoded coastlines stay for the next On
    if (this.flow) { this.flow.detach(); this.flow = null; }
    this._unattribute();
    this._unbindReadout();
    this._listeners.forEach(function (l) { self.map.off(l[0], l[1]); }); this._listeners = [];
    this._unbindSize();
    this._removeSheet();
    this.cache.clear(); this.dcache.clear(); this.unavailable = {}; this.target = null; this.wasPlaying = false; this.ui = null;
    this.field = null; this.frameIndex = null; this.res = null; this.dres = null; this.last = null; this.collapsed = undefined;
    clear(this.opts.panel);
    if (this.opts.panel && this.opts.panel.classList) this.opts.panel.classList.remove('ov-playing');
    this._say('');                                             // Off leaves nothing to be read out
  };
  Overlay.prototype._opacityFor = function (field) { return FIXED_OPACITY[field] || FIXED_OPACITY.hs; };
  // The basemap and the model pane's blend follow what is DRAWN: a wind frame on the map -> the relief
  // (WIND_LOOK), multiplied; anything else (another field, a field still loading, Off) -> the page's
  // imagery, no blend. The new basemap goes on top and the old one leaves once the new one's tiles have
  // faded in (never a map without a basemap); the blend is on only while the relief is the sole basemap
  // (never the wind multiplied into the dark imagery). A relief that loads no tile is retried after a cooldown.
  Overlay.prototype._syncLook = function () {
    var map = this.map, img = this.opts.baseLayer, layer = this.layer, self = this;
    if (layer) {
      var op = this._opacityFor(layer.field || this.field);
      if (!layer.options || layer.options.opacity !== op) layer.setOpacity(op);   // (called on every landing: only on a change)
    }
    var wind = !!(layer && layer.hasFrame() && layer.field === 'wind');
    var failed = this._reliefFailed && Date.now() - this._reliefFailed < RELIEF_RETRY_MS;
    var want = wind && img && WIND_LOOK.base && !failed ? 'relief' : 'imagery';
    if (want === 'relief' && this._base !== 'relief' && !map.hasLayer(img)) want = 'imagery';   // the page's imagery is not on the map: leave the basemap alone
    if (want !== this._base && img) {
      this._base = want;
      var relief = this._reliefLayer(img), show = want === 'relief' ? relief : img, hide = want === 'relief' ? img : relief;
      if (map.hasLayer(show)) { if (map.hasLayer(hide)) map.removeLayer(hide); }  // still loaded beneath the other (a quick switch back)
      else {
        if (show === relief) relief._ovTiles = 0;
        show.once('load', function () {
          if (self._base !== want) return;                                    // switched again meanwhile
          if (show === relief && !relief._ovTiles) { self._reliefFailed = Date.now(); self._syncLook(); return; }
          setTimeout(function () {
            if (self._base !== want || !map.hasLayer(show)) return;
            if (map.hasLayer(hide)) map.removeLayer(hide);
            self._blend();
          }, BASE_FADE_MS);
        });
        show.addTo(map);
      }
    }
    this._blend();
  };
  Overlay.prototype._blend = function () {
    var map = this.map, pane = map.getPane && map.getPane('modelPane'), r = this.relief, img = this.opts.baseLayer;
    var on = this._base === 'relief' && !!r && map.hasLayer(r) && !(img && map.hasLayer(img));
    if (pane && pane.style) pane.style.mixBlendMode = on ? WIND_LOOK.blend : '';
  };
  Overlay.prototype._reliefLayer = function (img) {
    if (this.relief) return this.relief;
    var o = img.options || {}, ro = { attribution: WIND_LOOK.attribution, className: 'ov-relief' };
    ['maxZoom', 'minZoom', 'noWrap', 'keepBuffer'].forEach(function (k) { if (o[k] !== undefined) ro[k] = o[k]; });
    if (WIND_LOOK.maxNativeZoom) ro.maxNativeZoom = WIND_LOOK.maxNativeZoom;
    // the page's basemap layer (static_ui/basemap.js) draws a relief tile Esri does not have from a coarser one
    var make = (typeof window !== 'undefined' && window.AllshoreBasemap && window.AllshoreBasemap.tileLayer) || L.tileLayer;
    var r = this.relief = make(WIND_LOOK.base, ro);
    r._ovTiles = 0;
    r.on('tileload', function () { r._ovTiles++; });
    return r;
  };
  // The contour settings for the current field and site unit, or null.
  Overlay.prototype._contourCfg = function () {
    var c = CONTOURS[this.field];
    if (!this.contours || !c) return null;
    var unit = this.opts.getUnit() === 'Metric' ? 'Metric' : 'US';
    var u = unitOf(this.field, unit);
    return { step: c[unit], per: u.scale !== undefined ? u.scale : u.f(1), off: u.offset || 0 };
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
    this.flow.setRate(this._flowRate());
    this.dres = dirRes(this._dirDef(), this.map.getZoom(), this.dres);
  };
  // The direction frame for idx, delivered to the animator only if the LAYER shows that step of that run
  // (its frame entry; a field switch keeps frameIndex while the layer is empty), at this field and
  // resolution; a failure is swallowed: the field plays on without its particles.
  Overlay.prototype._ensureDir = function (idx) {
    var self = this, m = this.manifest, field = this.field, dres = this.dres;
    return this._ensure(idx, 'dir').then(function (frame) {
      if (self.flow && self.layer && self.layer.hasFrame() && self.layer.entry === m.frames[idx] && self.manifest === m && self.field === field && self.dres === dres && self._wantDir()) {
        self._flowData(frame, dres === 'half' ? m.grid_half : m.grid, m.frames[idx]);
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
    if (Object.keys(this.inflight).length >= this._maxInflight()) {
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
    if (hit) { this._flowData(hit, this.dres === 'half' ? m.grid_half : m.grid, entry); return; }
    if (this.flow.entry !== entry) this.flow.clearData();
    if (this.target === null || this.target === idx) this._startDir(idx);
  };
  // The animator takes a direction; an exception in it (a refused canvas context, G29 B-P2-2) is logged once and never
  // reaches the field's playback: the picture plays on without its particles.
  Overlay.prototype._flowData = function (frame, grid, entry) {
    try { this.flow.setData(frame, grid, entry); }
    catch (e) { if (!this._flowWarned && typeof console !== 'undefined' && console.warn) { this._flowWarned = true; console.warn('overlay animation', e); } }
  };
  // Animation / contours on or off (no checkbox any more: both are on; kept for the scheduler's own transitions and tests).
  Overlay.prototype.setAnim = function (on) {
    this.anim = !!on;
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
    this.contours = !!on;
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
  // The control's top within the map (the brand control sits above it in the same corner); 10 px when there is no layout.
  Overlay.prototype._ctlTop = function (ctl) {
    try {
      var a = ctl.getBoundingClientRect(), b = this.map.getContainer().getBoundingClientRect();
      if (a && b && a.height > 0 && isFinite(a.top - b.top)) return Math.max(0, a.top - b.top);
    } catch (e) { /* no layout */ }
    return 10;
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
  // The run, Updated and Next Update lines, in place (no panel rebuild: focus and the sheet's scroll stay).
  Overlay.prototype._refreshRunLine = function () {
    var ui = this.ui;
    this._runLineAt = Date.now();
    if (!ui || !ui.runText || !this.manifest) return;
    var t = runTimes(this.manifest, Date.now(), !!this.newerRun, this._def().cycleMs);
    this._runStatus = t && t.status;
    ui.runText.nodeValue = ui.runLabel + ' (UTC)' + ui.runTail;
    if (ui.updLine) ui.updLine.hidden = !t;
    if (ui.nextLine) ui.nextLine.hidden = !t;
    if (t && ui.updText) { ui.updText.nodeValue = t.updated; ui.nextText.nodeValue = t.nextText; }
  };
  Overlay.prototype._hours = function (entry) { return Math.round((Date.parse(entry.valid_utc) - Date.parse(this.manifest.run_utc)) / 3.6e6); };
  Overlay.prototype._validLocal = function (entry) { return this.opts.fmtTime(entry.valid_utc, this.opts.tz) + ' ' + this.opts.tzAbbr(entry.valid_utc, this.opts.tz); };
  // "Oct 8, 06:00 PM" -> "Oct. 8, 06:00 PM": the period after an abbreviated month in the folded line (owner,
  // 2026-10-07). Only the English three-letter abbreviations get one ("May" is the full name); a string in another
  // locale's shape (the page formats the clock in the browser's locale: "8. Okt., 18:00") is left as it is.
  function dotMonth(s) { return String(s).replace(/^(Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec) (?=\d)/, '$1. '); }
  // The folded panel's one line: "Thu, Oct. 8, 06:00 PM HST" (weekday, date, time, zone; no field name, no hour count:
  // owner, 2026-10-07) in the forecast table's own clock format, for the REQUESTED time with its state while a seek is
  // pending (the ribbon label's rule; a folded sheet has no ribbon to show it).
  Overlay.prototype._summaryLine = function () {
    var m = this.manifest, pending = this.target !== null && this.target !== this.frameIndex, idx = pending ? this.target : this.frameIndex;
    if (!m || idx === null || !m.frames[idx]) return '…';
    var fr = m.frames[idx], wd = ribbonFormatter(this.opts.tz).stamp(Date.parse(fr.valid_utc)).weekday;
    var state = !pending ? '' : this._isUnavailable(idx) ? ' · unavailable' : ' · loading…';
    var model = this.models.length > 1 ? ' · ' + this._def().short : '';    // which model, when there is a choice (owner, G30-14)
    return (wd ? wd + ', ' : '') + dotMonth(this._validLocal(fr)) + model + state;
  };
  function button(cls, text, label, onClick) {
    var b = mk('button', 'ov-btn ' + cls, text); b.type = 'button'; b.setAttribute('aria-label', label);
    b.addEventListener('click', onClick); return b;
  }
  // ---- the speed selector (plan section 37) ----
  // A button showing the chosen animal and a small chevron; a listbox menu of the three animals with a check on the
  // chosen one. Keyboard: Arrow keys on the button open it; in the menu the arrows wrap, Home/End, Enter/Space choose,
  // Escape closes (focus back on the button), Tab closes and moves on. A press anywhere outside closes it (a capture
  // listener: the map controls stop their events from bubbling). On the phone sheet it opens upward; on a desktop it
  // flips upward when it would leave the window.
  function animalImg(el, px) {
    el.alt = ''; el.width = px; el.height = px;                              // decorative: the button / option carries the name
    el.setAttribute('draggable', 'false'); el.setAttribute('decoding', 'async');
  }
  function speedSelector(ov, compact) {
    var root = mk('div', 'ov-speed'), btn = mk('button', 'ov-speed-btn'), img = mk('img', 'ov-animal'), menu = mk('div', 'ov-speed-menu');
    var options = [], docDown = null;
    btn.type = 'button'; btn.setAttribute('aria-haspopup', 'listbox'); btn.setAttribute('aria-expanded', 'false');
    animalImg(img, 28); btn.appendChild(img);
    var chev = mk('span', 'ov-chev', '\u25be'); chev.setAttribute('aria-hidden', 'true'); btn.appendChild(chev);
    menu.setAttribute('role', 'listbox'); menu.setAttribute('aria-label', 'Playback speed'); menu.hidden = true; menu.tabIndex = -1;
    menu.id = 'ovSpeedMenu'; btn.setAttribute('aria-controls', 'ovSpeedMenu');   // one panel at a time (G26 A-P3-3)
    SPEED_ANIMALS.forEach(function (a) {
      var o = mk('div', 'ov-speed-opt'); o.setAttribute('role', 'option'); o.tabIndex = -1;
      o.setAttribute('data-speed', String(a.speed)); o.setAttribute('aria-label', a.title); o.title = a.title;
      var oi = mk('img', 'ov-animal'); animalImg(oi, 36); oi.src = assetUrl(ov.opts, a.file); o.appendChild(oi);
      var ck = mk('span', 'ov-check', '\u2713'); ck.setAttribute('aria-hidden', 'true'); o.appendChild(ck);
      o.addEventListener('click', function () { choose(a.speed); });
      menu.appendChild(o); options.push(o);
    });
    function selectedIndex() { for (var i = 0; i < SPEED_ANIMALS.length; i++) if (SPEED_ANIMALS[i].speed === ov.speed) return i; return 0; }
    function sync() {
      var a = animalOf(ov.speed), src = assetUrl(ov.opts, a.file);
      if (img.src !== src) img.src = src;
      btn.setAttribute('data-animal', a.name.toLowerCase());
      btn.setAttribute('aria-label', 'Speed: ' + a.name + ', ' + a.speed + '\u00d7 speed'); btn.title = a.title;
      options.forEach(function (o, i) { o.setAttribute('aria-selected', SPEED_ANIMALS[i].speed === a.speed ? 'true' : 'false'); });
    }
    function open(focusIdx) {
      if (!menu.hidden) return;
      menu.hidden = false; btn.setAttribute('aria-expanded', 'true');
      root.classList.remove('ov-up');
      if (compact) root.classList.add('ov-up');
      else {
        try {
          var r = menu.getBoundingClientRect(), vh = (typeof window !== 'undefined' && window.innerHeight) || 0;
          if (vh && r.bottom > vh - 4) root.classList.add('ov-up');
        } catch (e) { /* no layout: stays below */ }
      }
      var o = options[focusIdx === undefined ? selectedIndex() : focusIdx];
      if (o && o.focus) o.focus();
      docDown = function (e) { if (!root.contains(e.target)) close(false); };
      document.addEventListener('pointerdown', docDown, true);
    }
    function close(refocus) {
      if (menu.hidden) return;
      menu.hidden = true; btn.setAttribute('aria-expanded', 'false');
      if (docDown) { document.removeEventListener('pointerdown', docDown, true); docDown = null; }
      if (refocus && btn.focus) btn.focus();
    }
    function choose(v) { ov.setSpeed(v); sync(); close(true); }
    btn.addEventListener('click', function () { if (menu.hidden) open(); else close(true); });
    btn.addEventListener('keydown', function (e) {
      if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;
      e.preventDefault(); e.stopPropagation(); open();
    });
    menu.addEventListener('keydown', function (e) {
      var i = options.indexOf(document.activeElement), n = options.length, k = e.key;
      if (k === 'ArrowDown') options[(i + 1) % n].focus();
      else if (k === 'ArrowUp') options[i < 0 ? n - 1 : (i - 1 + n) % n].focus();
      else if (k === 'Home') options[0].focus();
      else if (k === 'End') options[n - 1].focus();
      else if (k === 'Enter' || k === ' ') { if (i >= 0) choose(SPEED_ANIMALS[i].speed); }
      else if (k === 'Escape') close(true);
      else if (k === 'Tab') { close(false); return; }                      // focus moves on as usual
      else return;
      e.preventDefault(); e.stopPropagation();                               // the page's Escape and the map's arrow keys stay out
    });
    root.appendChild(btn); root.appendChild(menu); sync();
    return { root: root, btn: btn, menu: menu, options: options, open: open, close: close, sync: sync, isOpen: function () { return !menu.hidden; } };
  }
  Overlay.prototype._closeMenus = function () { if (this.ui && this.ui.speedSel) this.ui.speedSel.close(false); };
  // The timeline beside the play button (step 3 of plan section 37 replaces this range slider with the compass ribbon).
  // It runs in hours (hourly frames to +120 h, then 3-hourly), so its thumb sits where the hour is; a value between two
  // frames snaps in the direction of travel (frameAtHour).
  Overlay.prototype._buildTimeline = function (ui, m) {
    var self = this;
    var hrs = ui.hours = m.frames.map(function (fr) { return self._hours(fr); });
    var slider = mk('input', 'ov-timeline'); slider.type = 'range'; slider.min = String(hrs[0]); slider.max = String(hrs[hrs.length - 1]); slider.step = '1';
    slider.setAttribute('aria-label', 'Forecast overview: the whole run');
    var tl = ui.timeline = new TimelineState(), shownIdx = this.target !== null ? this.target : this.frameIndex;
    tl.shown = shownIdx !== null ? hrs[shownIdx] : null;
    slider.addEventListener('pointerdown', function () { tl.start(); });
    ['pointerup', 'pointercancel', 'lostpointercapture', 'change', 'keydown', 'blur'].forEach(function (ev) { slider.addEventListener(ev, function () { tl.end(); }); });
    slider.addEventListener('input', function () {
      var idx = tl.pick(hrs, parseInt(slider.value, 10));
      slider.value = String(hrs[idx]); slider.setAttribute('aria-valuetext', '+' + hrs[idx] + ' h');
      self._scrubTo(idx);
    });
    ui.slider = slider;
    return slider;
  };
  // ---- the compass ribbon (plan section 37): the main timeline ----
  // The dates and hours of the run scroll under a fixed pointer; the selected time (the REQUESTED one while a frame is
  // loading, with a state beside it) sits above the pointer. Dragging, a tap, the wheel and the keys all go through
  // _scrubTo: playback pauses and the frame becomes the target by the ordinary seek, so a scrub cancels the fetches it
  // made obsolete and a late answer never replaces a newer choice (the target check in _goto). Only the track's
  // transform changes from frame to frame; the spans are built once per render.
  var raf = (typeof window !== 'undefined' && window.requestAnimationFrame) ? function (f) { return window.requestAnimationFrame(f); } : function (f) { return setTimeout(f, 16); };
  var RIBBON_FALLBACK_W = 226;                                 // the viewport's width when it cannot be measured (tests)
  Overlay.prototype._buildRibbon = function (ui, m, body, compact) {
    var self = this;
    var wrap = mk('div', 'ov-ribbon-wrap');
    var now = mk('div', 'ov-ribbon-now'); ui.nowText = now.appendChild(document.createTextNode(''));
    var state = mk('span', 'ov-ribbon-state'); state.hidden = true; now.appendChild(state); ui.stateEl = state;
    wrap.appendChild(now);
    var ribbon = mk('div', 'ov-ribbon'); ribbon.setAttribute('role', 'slider'); ribbon.tabIndex = 0;
    ribbon.setAttribute('aria-label', 'Forecast time'); ribbon.setAttribute('aria-valuemin', '0'); ribbon.setAttribute('aria-valuemax', String(m.frames.length - 1));
    var track = mk('div', 'ov-ribbon-track'); ribbon.appendChild(track);
    ribbon.appendChild(mk('div', 'ov-ribbon-line')); ribbon.appendChild(mk('div', 'ov-ribbon-pointer'));
    wrap.appendChild(ribbon);
    ui.ribbon = ribbon; ui.track = track; ui.wrap = wrap;
    body.appendChild(wrap);                                    // measured in place: the scale follows the real width
    ui.ribbonW = ribbon.clientWidth || RIBBON_FALLBACK_W;
    var layout = ui.layout = ribbonLayout(m.frames, m.run_utc, this.opts.tz, ribbonScale(ui.ribbonW));
    ui.hours = layout.hours;
    track.style.width = layout.width + 'px';
    layout.days.forEach(function (d) { var sp = mk('span', 'ov-rb-day' + (d.clamped ? ' ov-rb-day-first' : ''), d.text); sp.style.left = d.x + 'px'; track.appendChild(sp); });
    layout.ticks.forEach(function (t) { var sp = mk('span', 'ov-rb-tick' + (t.major ? ' ov-rb-major' : ''), t.text); sp.style.left = t.x + 'px'; track.appendChild(sp); });
    var rb = ui.rb = new RibbonState(layout.xs);
    var lastIdx = null, pendingIdx = null, rafId = null, g = null;
    function queueScrub(idx) {
      pendingIdx = idx;
      if (rafId !== null) return;
      rafId = raf(function () { rafId = null; if (pendingIdx !== null && self.ui === ui) self._scrubTo(pendingIdx); pendingIdx = null; });
    }
    // The gesture (G26 re-check RC-1, RC-2, RC-4): ONE pointer, the one that pressed. Its direction is decided once it has
    // travelled RIBBON_AXIS_PX, by the larger of the two: sideways is a scrub (playback pauses then, and frames are asked
    // for from then on); up or down is a swipe, never a pick (the ribbon goes back, nothing is loaded, playback carries
    // on). Before that the ribbon follows the finger but nothing is loaded; a release then is a tap (< 4 px sideways: the
    // frame under the finger) or a short drag. A second pointer (a pinch, a palm) ends the gesture as no pick: back to
    // the frame it started on, and nothing more until the pointers lift. A gesture the browser cancels before it was a
    // scrub is no pick either; a cancelled scrub keeps the frame it reached.
    function back() {                                         // a swipe or a second pointer: no pick
      pendingIdx = null; rb.cancel();
      track.style.transition = 'transform 120ms ease-out'; self._placeRibbon();
      self._syncUI();                                          // while playing, the ribbon follows playback again at once
    }
    // Where the ribbon is SEEN: during its 150 ms step glide the visible position lags the offset, and a tap must read
    // what was under the finger (fresh check F6). null without a computed transform (no layout).
    function visibleOffset() {
      try {
        var cs = typeof window !== 'undefined' && window.getComputedStyle ? window.getComputedStyle(track) : null;
        var m = cs && /^matrix\(([^)]+)\)$/.exec(cs.transform || ''), tx = m ? parseFloat(m[1].split(',')[4]) : NaN;
        return isFinite(tx) ? ui.ribbonW / 2 - tx : null;
      } catch (x) { return null; }
    }
    ribbon.addEventListener('pointerdown', function (e) {
      if (e.button) return;
      e.preventDefault(); e.stopPropagation();
      if (g && e.pointerId === g.id) g = null;                 // a press whose release never reached us: start over
      // a gesture whose pointer no longer holds the capture ended without telling us: a new press is a new gesture, not a
      // second finger (else every later touch would be ignored until the next render)
      if (g && g.captured && ribbon.hasPointerCapture && !ribbon.hasPointerCapture(g.id)) g = null;
      if (g) {
        if (g.axis !== 'void') { var was = g.axis; g.axis = 'void'; pendingIdx = null; if (was === 'x') self._scrubTo(g.start); back(); }
        return;
      }
      var captured = false;
      if (ribbon.setPointerCapture) { try { ribbon.setPointerCapture(e.pointerId); captured = true; } catch (x) { /* no capture: the moves still arrive while over the ribbon */ } }
      var vis = visibleOffset();
      if (vis !== null) rb.offset = rb.clamp(vis);             // stop the glide where it is seen
      track.style.transition = 'none'; self._placeRibbon();
      rb.begin(e.clientX); lastIdx = null;
      g = { id: e.pointerId, x0: e.clientX, y0: typeof e.clientY === 'number' ? e.clientY : 0, axis: null, start: rb.nearest(rb.offset), captured: captured };
      if (ribbon.focus) { try { ribbon.focus({ preventScroll: true }); } catch (x) { ribbon.focus(); } }
    });
    ribbon.addEventListener('pointermove', function (e) {
      if (!g || e.pointerId !== g.id || g.axis === 'y' || g.axis === 'void' || typeof e.clientX !== 'number') return;
      if (g.axis === null) {
        var dx = Math.abs(e.clientX - g.x0), dy = typeof e.clientY === 'number' ? Math.abs(e.clientY - g.y0) : 0;
        if (Math.max(dx, dy) >= RIBBON_AXIS_PX) {
          if (dy >= dx) { g.axis = 'y'; back(); return; }
          g.axis = 'x'; if (self.playing) self.pause();
        }
      }
      var idx = rb.move(e.clientX); if (idx === null) return;
      self._placeRibbon();
      if (g.axis === 'x' && idx !== lastIdx) { lastIdx = idx; queueScrub(idx); }
    });
    // Lift-off. A scrub still queued for the next frame is dropped first, or it would land after the release with the
    // pre-release frame and undo a tap (G26 A-P2-2). Only the pressing pointer's release counts, once: the
    // lostpointercapture that follows every pointerup finds no gesture left (G26 re-check RC-8 a).
    function release(e) {
      if (!g || e.pointerId !== g.id) return;
      var axis = g.axis, x0 = g.x0, y0 = g.y0;
      g = null; pendingIdx = null;
      if (axis === 'y' || axis === 'void') return;            // already back where it started
      if (axis === null) {
        if (e.type !== 'pointerup') { back(); return; }        // a cancel or a lost capture before it was a scrub: no pick
        // the lift-off point decides like a move (a browser may send no move to where the button is released; fresh
        // check F3); short of the decision: a tap (< 4 px sideways) or a short drag, by rb.end
        if (typeof e.clientX === 'number') {
          var dx = Math.abs(e.clientX - x0), dy = typeof e.clientY === 'number' ? Math.abs(e.clientY - y0) : 0;
          if (Math.max(dx, dy) >= RIBBON_AXIS_PX) { if (dy >= dx) { back(); return; } axis = 'x'; }
          rb.move(e.clientX);
        }
      }
      var r = axis === null && ribbon.getBoundingClientRect ? ribbon.getBoundingClientRect() : null;
      var idx = rb.end(r ? x0 - (r.left + r.width / 2) : undefined);
      track.style.transition = 'transform 120ms ease-out'; self._placeRibbon(); lastIdx = idx; self._scrubTo(idx);
    }
    ['pointerup', 'pointercancel', 'lostpointercapture'].forEach(function (ev) { ribbon.addEventListener(ev, release); });
    var wheelSnap = null;
    ribbon.addEventListener('wheel', function (e) {
      var dx = e.deltaX || (e.shiftKey ? e.deltaY : 0);
      if (!dx || (!e.shiftKey && Math.abs(e.deltaX) < Math.abs(e.deltaY))) return;   // a vertical wheel stays with the details box
      e.preventDefault(); e.stopPropagation();
      if (e.deltaMode === 1) dx *= 16;
      track.style.transition = 'none';
      var idx = rb.wheel(dx); self._placeRibbon(); queueScrub(idx);
      if (wheelSnap) clearTimeout(wheelSnap);
      wheelSnap = setTimeout(function () { wheelSnap = null; if (self.ui !== ui) return; rb.setFrame(rb.nearest(rb.offset)); track.style.transition = 'transform 120ms ease-out'; self._placeRibbon(); }, 150);
    }, { passive: false });
    ribbon.addEventListener('keydown', function (e) { if (self._ribbonKey(e.key)) { e.preventDefault(); e.stopPropagation(); } });
    return wrap;
  };
  Overlay.prototype._placeRibbon = function () {
    var ui = this.ui;
    if (ui && ui.track && ui.rb) ui.track.style.transform = 'translateX(' + (ui.ribbonW / 2 - ui.rb.offset) + 'px)';
  };
  // After a resize: the viewport's width decides the scale, so a real change rebuilds; a small one just re-places.
  Overlay.prototype._ribbonMeasure = function () {
    var ui = this.ui;
    if (!ui || !ui.ribbon) return;
    var w = ui.ribbon.clientWidth || ui.ribbonW;
    if (Math.abs(w - ui.ribbonW) > 8 && this.last) { this.render(this.last); return; }
    ui.ribbonW = w; this._placeRibbon();
  };
  // A scrub (drag, tap, wheel, slider, key): playback pauses and the frame becomes the target; a repeated index is free.
  Overlay.prototype._scrubTo = function (idx) {
    if (this.playing) this.pause();
    if (this.manifest && this.frameIndex !== null && idx !== this.target) this.seek(idx);
  };
  Overlay.prototype._ribbonHours = function () {
    var self = this;
    return (this.ui && this.ui.hours) || this.manifest.frames.map(function (f) { return self._hours(f); });
  };
  // Keys on the ribbon: the arrows step one AVAILABLE frame (step pauses, skips unavailable ones and stops at the ends),
  // Home / End the first / last available frame, PageUp / PageDown a day (frameAtHour, snapping in the key's direction;
  // an unavailable frame there gives way to the next available one that way, else the nearest one back). Manual
  // navigation never wraps around. True when handled.
  Overlay.prototype._ribbonKey = function (key) {
    if (!this.manifest || this.frameIndex === null) return false;
    var self = this, un = function (j) { return self._isUnavailable(j); };
    var cur = this.target !== null ? this.target : this.frameIndex, hrs, idx, dir = 0;
    switch (key) {
      case 'ArrowRight': case 'ArrowUp': this.step(1); return true;
      case 'ArrowLeft': case 'ArrowDown': this.step(-1); return true;
      case 'Home': idx = stepAvailable(-1, 1, this.n, un); break;
      case 'End': idx = stepAvailable(this.n, -1, this.n, un); break;
      case 'PageUp': hrs = this._ribbonHours(); idx = frameAtHour(hrs, hrs[cur] + 24, true); dir = 1; break;
      case 'PageDown': hrs = this._ribbonHours(); idx = frameAtHour(hrs, hrs[cur] - 24, false); dir = -1; break;
      default: return false;
    }
    if (dir && un(idx)) { var on = stepAvailable(idx, dir, this.n, un); idx = on !== null ? on : stepAvailable(idx, -dir, this.n, un); }
    if (idx === null) { if (this.playing) this.pause(); return true; }        // nothing available at all
    this._scrubTo(idx); return true;
  };
  // The label above the pointer: the REQUESTED time with its weekday (the target while a seek is pending, else the drawn
  // frame) and a state while the picture is not that frame yet ("loading…", or "unavailable": the previous picture stays).
  Overlay.prototype._ribbonLabel = function () {
    var m = this.manifest, pending = this.target !== null && this.target !== this.frameIndex, idx = pending ? this.target : this.frameIndex;
    if (!m || idx === null || !m.frames[idx]) return { text: '…', state: '', valuetext: 'loading' };
    var fr = m.frames[idx], ms = Date.parse(fr.valid_utc), fmt = (this.ui && this.ui.layout) ? this.ui.layout.fmt : ribbonFormatter(this.opts.tz);
    var abbr = this.opts.tzAbbr(fr.valid_utc, this.opts.tz), zone = abbr ? ' ' + abbr : '', st = fmt.stamp(ms);
    var state = !pending ? '' : this._isUnavailable(idx) ? 'unavailable' : 'loading…';
    return { text: st.date + ' · ' + st.clock + zone, state: state,
      valuetext: (st.weekdayName ? st.weekdayName + ', ' : '') + st.day + ', ' + st.clock + zone + ' (+' + this._hours(fr) + ' h)' + (state ? ', ' + state.replace('…', '') : '') };
  };
  // Builds the panel for a state; frame-by-frame changes only touch the live parts through _syncUI().
  // Head: the details toggle and the model's name (collapsed: the field and the time on the map). Then #ovDetails: the
  // transport row (the ONE play/pause button, the timeline, the speed selector; outside the scroll box, so the speed
  // menu is never clipped) and the scroll box (legend, Run / Updated / Next Update, warnings).
  Overlay.prototype.render = function (st) {
    var focusCls = this._focusedPart();                       // before the menu closes (a hidden option loses the focus)
    // a part that had the focus before a loading panel (nothing to focus there) gets it back now, unless the focus went
    // somewhere else meanwhile (fresh check F2: Retry, Update)
    if (!focusCls && this._pendingFocus && this._focusLost()) focusCls = this._pendingFocus;
    this._pendingFocus = null;
    this._closeMenus();
    this.last = st; this.ui = null;
    var self = this, host = this._host(), compact = host === this.sheet, m = this.manifest, unit = this.opts.getUnit();
    var mapH = this._dims().h;
    clear(host);
    this._layoutSheet();                                       // a new sheet takes its place beside the zoom column BEFORE the ribbon is measured
    if (host.classList) host.classList.remove('ov-playing');
    // Nothing in the panel is a live region (its time changes on every frame while playing); the loading, error and
    // unavailable-frame messages are said once through the overlay's own status node (_say; G26 A-P3-5, re-check RC-7).
    // The focused ribbon's aria-valuetext has the time.
    if (st.state === 'loading') {
      // with a choice of models the dropdown stays while one loads: a slow or stuck model can be left (G30-3)
      if (this.models.length > 1) { var lh = mk('div', 'ov-row ov-head'); lh.appendChild(this._modelSelect()); host.appendChild(lh); }
      host.appendChild(mk('div', 'ov-meta', 'Loading model frame…')); this._say('Loading model frame…'); this._layoutSheet();
      if (focusCls === 'ov-model-sel' && this.models.length > 1) this._refocus(focusCls, host); else this._pendingFocus = focusCls;
      return;
    }
    if (st.state === 'error') {
      this._say('Overlay unavailable: ' + st.message);
      var e = mk('div', 'ov-err', 'Overlay unavailable: ' + st.message + ' ');
      if (this.models.length > 1) e.appendChild(this._modelSelect());         // a failing model must be escapable
      e.appendChild(button('ov-retry', 'Retry', 'Retry loading the overlay', function () { if (self.field) { self.unavailable = {}; self.transientFails = 0; self.mount(self.field); } }));
      host.appendChild(e);
      if (OV_DEBUG) { this.ui = { debug: mk('pre', 'ov-debug') }; host.appendChild(this.ui.debug); this._debugOn(); }   // the readout stays with the error (the owner's iPad)
      this._layoutSheet(); this._refocus(focusCls, host); return;
    }
    var field = this.layer.field, f = m.fields[field], fdesc = (m.model && m.model.fields && m.model.fields[field]) || {};
    // The model's name as the head shows it: the manifest's up to " + " (the wind's own model), without the agency
    // prefix ("NOAA/NCEP GFS-Wave (WAVEWATCH III)" -> "GFS-Wave (WAVEWATCH III)"; owner, 2026-10-07). NOAA stays in
    // the map's attribution.
    var label = fdesc.label || field, modelName = String(m.model && m.model.name || 'GFS-Wave').split(' + ')[0].replace(/^NOAA(\/NCEP)?\s+/i, '');
    var cap = Math.floor(mapH * 0.4);
    // Open or folded: the viewer's choice once the toggle was used; until then the size decides at every render: as much
    // as fits (G26 re-check RC-3: measured, not a fixed height), so a phone turned upright opens up again.
    var collapsed = this.collapsed !== undefined ? !!this.collapsed : false;
    var head = mk('div', 'ov-row ov-head');
    var btn = mk('button', 'ov-toggle', collapsed ? '▸' : '▾'); btn.type = 'button';
    btn.setAttribute('aria-label', collapsed ? 'Show overlay details' : 'Hide overlay details');
    btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true'); btn.setAttribute('aria-controls', 'ovDetails');
    btn.addEventListener('click', function () { self.collapsed = !collapsed; self.render(st); });   // the focus follows (_refocus)
    head.appendChild(btn);
    // Open, with more than one model offered: the head line is the model dropdown (its value is the model in use; its
    // options the page's list); otherwise a text span (the model's name, or the folded summary line).
    var modelSel = !collapsed && this.models.length > 1 ? this._modelSelect() : null;
    var title = modelSel || mk('span', 'ov-title' + (!collapsed ? ' ov-model' : ''));
    head.appendChild(title);
    host.appendChild(head);
    var ui = this.ui = { host: host, title: title, modelSel: modelSel, play: null, speedSel: null, slider: null, unavail: null,
      ribbon: null, track: null, rb: null, layout: null, nowText: null, stateEl: null, ribbonW: 0,
      runText: null, updText: null, nextText: null, updLine: null, nextLine: null,
      label: label, modelName: modelName, collapsed: false, compact: compact };
    // The one play / pause button sits beside the ribbon; when the transport row is not shown (collapsed, or no room for
    // it) it sits in the head instead, so playback is never out of reach.
    var playPause = function () { if (self.playing) self.pause(); else self.play(); };
    var headPlay = function () { ui.play = button('ov-play ov-play-head', '▶', 'Play', playPause); head.insertBefore(ui.play, title); };
    // A one-line head (folded by size) shows the summary line (the time and, with a choice, the model), not the dropdown:
    // the model is changed in the open panel (G30-1; the viewer's own fold never builds the dropdown).
    var oneLine = function () {
      if (!ui.modelSel) return;
      var span = mk('span', 'ov-title'); head.insertBefore(span, ui.modelSel); head.removeChild(ui.modelSel);
      title = ui.title = span; ui.modelSel = null;
    };
    var rt0 = runTimes(m, Date.now(), !!this.newerRun, this._def().cycleMs); this._runStatus = rt0 && rt0.status;
    if (compact && cap - host.offsetHeight - 8 < 40) {           // a sheet too short for anything but its head line
      head.removeChild(btn); ui.collapsed = true; oneLine(); headPlay(); this._syncUI(); this._layoutSheet(); this._refocus(focusCls); return;
    }
    var wrap = mk('div', 'ov-body'); wrap.id = 'ovDetails'; host.appendChild(wrap);
    var tr = mk('div', 'ov-row ov-transport');
    ui.play = button('ov-play', '▶', 'Play', playPause);
    tr.appendChild(ui.play);
    ui.speedSel = speedSelector(this, compact);
    wrap.appendChild(tr);
    this._buildRibbon(ui, m, tr, compact);                     // measured inside the row, between the play button and the speed
    tr.appendChild(ui.speedSel.root);
    ui.ribbonW = ui.ribbon.clientWidth || ui.ribbonW;
    var over = mk('div', 'ov-overview');                       // the owner's small slider: the whole run at a glance
    over.appendChild(this._buildTimeline(ui, m));
    wrap.appendChild(over);
    var body = mk('div', 'ov-details'); body.style.maxHeight = cap + 'px'; wrap.appendChild(body);
    // legend over the LEGEND range in the site's units (the encoding range is wider; extremes clamp)
    var leg = mk('div', 'ov-legend'), cv = mk('canvas'); cv.width = 256; cv.height = 1; leg.appendChild(cv);
    var lut = legendBar(field, f.legend), ctx = cv.getContext('2d'), im = ctx.createImageData(256, 1);        // the bar is laid out by legend position
    for (var i = 0; i < 256; i++) { im.data[i * 4] = lut[i * 3]; im.data[i * 4 + 1] = lut[i * 3 + 1]; im.data[i * 4 + 2] = lut[i * 3 + 2]; im.data[i * 4 + 3] = 255; }
    ctx.putImageData(im, 0, 0);
    var ticks = mk('div', 'ov-ticks'), tk = legendTicks(field, f, unit);
    tk.forEach(function (t, k) {
      var s = mk('span', k === 0 ? 'ov-first' : k === tk.length - 1 ? 'ov-last' : '', t.label);
      s.style.left = (t.pos * 100).toFixed(2) + '%'; ticks.appendChild(s);
    });
    leg.appendChild(ticks); body.appendChild(leg);
    // Run (UTC, with the forecast table's run when it differs), Updated and Next Update (the computer's own zone)
    var line = function (cls, name) { var d = mk('div', 'ov-meta ' + cls); d.appendChild(mk('b', null, name)); body.appendChild(d); return d; };
    var runLine = line('ov-run', 'Run: '); ui.runText = runLine.appendChild(document.createTextNode(''));
    ui.updLine = line('ov-upd', 'Updated: '); ui.updText = ui.updLine.appendChild(document.createTextNode(''));
    ui.nextLine = line('ov-next', 'Next Update: '); ui.nextText = ui.nextLine.appendChild(document.createTextNode(''));
    var pc = typeof this.opts.pageCycle === 'function' ? this.opts.pageCycle() : this.opts.pageCycle;
    // the forecast table is always GFS-Wave (or SWAN): under another model its run is named with its model
    ui.runTail = pc && pc.model === 'SWAN' ? ' — the forecast table is a PacIOOS SWAN run'
      : pc && pc.run && this._def().table === 'model' ? ' — the forecast table is on GFS-Wave run ' + pc.run
      : pc && pc.run && pc.run !== m.run ? ' — the forecast table is on run ' + pc.run : '';
    ui.runLabel = m.run_utc.replace('T', ' ').replace(':00:00Z', 'Z');
    this._refreshRunLine();
    var age = (Date.now() - Date.parse(this.pointer.published_utc)) / 1000;
    this._staleShown = age > this._def().staleS;
    if (this._staleShown) body.appendChild(mk('div', 'ov-warn', 'Model overlay data is stale (published ' + Math.round(age / 3600) + ' h ago).'));
    if (this.newerRun) {
      var banner = mk('div', 'ov-warn', 'A newer run (' + String(this.newerRun.run).replace(/^(\d{8})(\d{2})$/, '$1 $2Z') + ') is available. ');
      banner.appendChild(button('ov-update', 'Update', 'Switch to the newer run', function () { self.update(); }));
      body.appendChild(banner);
    }
    var unavail = mk('div', 'ov-warn'); unavail.hidden = true; body.appendChild(unavail); ui.unavail = unavail;
    var clipped = !!this.layer._clip;
    if (CLIP_FIELDS[field] && this.coast && !clipped) body.appendChild(mk('div', 'ov-warn', 'Coastline data could not be loaded; the field is shown without coastline clipping.'));
    if (OV_DEBUG) { ui.debug = mk('pre', 'ov-debug'); body.appendChild(ui.debug); this._debugOn(); }
    // (no settings row: fixed opacity, contours and the animation always on; owner, 2026-09-26)
    this._syncUI();
    // Clamp from the real layout: on phones the WHOLE sheet <= cap; on desktops the scroll box <= cap AND the top-left
    // control must end above the zoom/Home stack (short windows: the site caps the map at 48 % of the viewport). No room
    // for a scroll box -> the details go and the transport row stays (a phone turned sideways keeps play, ribbon and
    // speed); no room for that either -> the one-line summary with the play button in the head. A fold belongs to the
    // size: it is never saved as a collapse, so a bigger map gets everything back.
    // On desktops the room is measured from where the control really starts in the map (below the brand control in the
    // same corner), not from an assumed 10 px (G26 B-1: a 330-410 px map put the meta lines under the (i)/Home column).
    var ctl = compact ? host : (host.closest && host.closest('.ov-ctl')) || host, ctlTop = compact ? 0 : this._ctlTop(ctl);
    var spare = function () { return compact ? cap - host.offsetHeight - 2 : mapH - self._stackHeight() - 10 - ctlTop - ctl.offsetHeight - 8; };
    var room = compact ? cap - (host.offsetHeight - body.offsetHeight) - 2 : Math.min(cap, spare() + body.offsetHeight);
    // The open panel is built (and measured) even when it will be folded: the toggle is offered only when opening shows
    // more than the one-line head (G26 re-check RC-3: a toggle that could open nothing disappeared when tapped).
    var opens = true;
    if (room < 40) {
      wrap.removeChild(body);
      ui.unavail = null; ui.runText = ui.updText = ui.nextText = ui.updLine = ui.nextLine = null;
      if (spare() < 0) opens = false;
    } else {
      body.style.maxHeight = room + 'px';
    }
    if (!opens || collapsed) {                                  // one line: the viewer's fold (toggle kept) or no room (none)
      if (ui.speedSel) ui.speedSel.close(false);
      host.removeChild(wrap); if (!opens) head.removeChild(btn);
      ui.collapsed = true; ui.speedSel = null; ui.slider = null;
      ui.ribbon = ui.track = ui.rb = ui.nowText = ui.stateEl = null;
      ui.unavail = null; ui.runText = ui.updText = ui.nextText = ui.updLine = ui.nextLine = null;
      oneLine();
      if (title.classList) title.classList.remove('ov-model');
      headPlay();
    }
    this._syncUI();
    this._layoutSheet();
    // The ribbon's width once everything is in place: a small change re-places the track, a bigger one (another scale)
    // rebuilds once.
    if (ui.ribbon && !this._remeasuring) {
      var w = ui.ribbon.clientWidth;
      if (w && Math.abs(w - ui.ribbonW) > 8) { this._remeasuring = true; try { this.render(st); } finally { this._remeasuring = false; } }
      else if (w && w !== ui.ribbonW) { ui.ribbonW = w; this._placeRibbon(); }
    }
    this._refocus(focusCls);                                   // after the last (re)build: on the control that had it
  };
  // Keyboard focus through a render (G26 B-2, re-check RC-6): the part of the panel that had it, by its class; its
  // successor in the new panel gets it back (else the play button, else the toggle). Never taken from outside the panel.
  // The model dropdown: a native select (its options are the offered models' labels, in order; the value is the model in
  // use). A change switches the model (setModel); _syncUI only ever sets its value, never its text.
  Overlay.prototype._modelSelect = function () {
    var self = this, sel = mk('select', 'ov-title ov-model ov-model-sel');
    sel.setAttribute('aria-label', 'Overlay model'); sel.setAttribute('autocomplete', 'off');
    for (var i = 0; i < this.models.length; i++) {
      var opt = mk('option', null, MODEL_DEFS[this.models[i]].label); opt.value = this.models[i];
      if (this.models[i] === this.model) opt.selected = true;
      sel.appendChild(opt);
    }
    sel.value = this.model;
    sel.addEventListener('change', function () { var v = sel.value; if (v !== self.model) self.setModel(v); else sel.value = self.model; });
    return sel;
  };
  var FOCUS_PARTS = ['ov-toggle', 'ov-model-sel', 'ov-ribbon', 'ov-speed-btn', 'ov-speed-opt', 'ov-speed-menu', 'ov-timeline', 'ov-play', 'ov-retry', 'ov-update', 'ov-details'];
  Overlay.prototype._focusedPart = function () {
    var a = typeof document !== 'undefined' ? document.activeElement : null, hosts = [this.opts.panel, this.sheet];
    if (!a || !a.classList) return null;
    var inside = hosts.some(function (h) { return h && h !== a && h.contains && h.contains(a); });
    if (!inside) return null;
    for (var i = 0; i < FOCUS_PARTS.length; i++) {
      if (a.classList.contains(FOCUS_PARTS[i])) return /^ov-speed-(opt|menu)$/.test(FOCUS_PARTS[i]) ? 'ov-speed-btn' : FOCUS_PARTS[i];
    }
    return 'ov-play';
  };
  // No element has the focus (the body), or the one that had it is gone from the page.
  Overlay.prototype._focusLost = function () {
    if (typeof document === 'undefined') return false;
    var a = document.activeElement, b = document.body;
    return !a || a === b || !!(b && b.contains && !b.contains(a));
  };
  Overlay.prototype._refocus = function (cls, host) {
    var h = cls && (host || (this.ui && this.ui.host));
    if (!h || !h.querySelector) return;
    var el = h.querySelector('.' + cls) || h.querySelector('.ov-play') || h.querySelector('.ov-toggle') || h.querySelector('.ov-retry');
    if (el && el.focus) { try { el.focus({ preventScroll: true }); } catch (e) { el.focus(); } }
  };
  // The overlay's one live region: a visually hidden role=status node, created once, written only when its text
  // changes (a rewrite with the same text could be read out again on every frame; G26 re-check RC-7).
  // A live region that appears together with its text is often not read out (fresh check F5): the node goes in empty and
  // its first text follows a moment later (the latest text by then).
  Overlay.prototype._say = function (text) {
    var self = this;
    this._sayText = text = text || '';
    if (!this._status) {
      if (!text || typeof document === 'undefined' || !document.body) return;
      this._status = mk('div', 'ov-sr'); this._status.setAttribute('role', 'status'); document.body.appendChild(this._status);
      this._sayTimer = setTimeout(function () { self._sayTimer = null; if (self._status.textContent !== self._sayText) self._status.textContent = self._sayText; }, SAY_FIRST_MS);
      return;
    }
    if (this._sayTimer) return;                                // the first text is on its way and takes the latest
    if (this._status.textContent !== text) this._status.textContent = text;
  };
  var SAY_FIRST_MS = 100;
  // The live parts: the play/pause glyph and the playing look (the chosen animal moves), the speed button, the title,
  // the ribbon (its label = the requested time with a loading state, its position = the target), the overview slider's
  // thumb (at the target) and the unavailable-frame note.
  Overlay.prototype._syncUI = function () {
    var ui = this.ui;
    if (!ui || !ui.title || !this.manifest || !this.layer) return;        // no panel (the error panel's ?ovdebug=1 readout alone: re-check R-5)
    if (ui.play) { ui.play.textContent = this.playing ? '❚❚' : '▶'; ui.play.setAttribute('aria-label', this.playing ? 'Pause' : 'Play'); }
    if (ui.host && ui.host.classList) ui.host.classList.toggle('ov-playing', !!this.playing);
    if (ui.speedSel) ui.speedSel.sync();
    // The head line, desktop panel and phone sheet alike: open, the model's name (the time is in the ribbon's label);
    // folded, the one-line summary.
    if (ui.modelSel) { if (ui.modelSel.value !== this.model) ui.modelSel.value = this.model; }
    else ui.title.textContent = ui.collapsed ? this._summaryLine() : ui.modelName;
    var si = this.target !== null ? this.target : this.frameIndex;
    if (ui.ribbon && ui.rb && si !== null) {
      var lb = this._ribbonLabel();
      ui.nowText.nodeValue = lb.text; ui.stateEl.textContent = lb.state ? ' ' + lb.state : ''; ui.stateEl.hidden = !lb.state;
      ui.ribbon.setAttribute('aria-valuenow', String(si)); ui.ribbon.setAttribute('aria-valuetext', lb.valuetext);
      if (!ui.rb.dragging) {
        var prev = ui.rb.offset; ui.rb.setFrame(si);
        var jump = Math.abs(ui.rb.offset - prev) > ui.ribbonW, ms = (jump || reducedMotion() || (this.playing && this.speed >= 4)) ? 0 : 150;
        ui.track.style.transition = ms ? 'transform ' + ms + 'ms ease-out' : 'none';
        this._placeRibbon();
      }
    }
    if (ui.slider && ui.hours && si !== null && !(ui.timeline && ui.timeline.dragging)) {
      ui.slider.value = String(ui.hours[si]); ui.slider.setAttribute('aria-valuetext', '+' + ui.hours[si] + ' h');
      if (ui.timeline) ui.timeline.shown = ui.hours[si];
    }
    if (ui.runText && Date.now() - (this._runLineAt || 0) > 60000) this._refreshRunLine();
    var missing = [];
    for (var i = 0; i < this.n; i++) if (this._isUnavailable(i)) missing.push('+' + this._hours(this.manifest.frames[i]) + ' h');
    var note = missing.length ? 'Unavailable frames are skipped: ' + missing.slice(0, 8).join(', ') +
      (missing.length > 8 ? ' and ' + (missing.length - 8) + ' more' : '') : '';
    if (ui.unavail) {                                          // written only when it changes
      if (ui.unavail.hidden !== !note) ui.unavail.hidden = !note;
      if (ui.unavail.textContent !== note) ui.unavail.textContent = note;
    }
    this._say(note);
  };

  window.AllshoreOverlay = {
    create: function (map, opts) { var o = new Overlay(map, opts); window.AllshoreOverlay._last = o; return o; },   // _last: debugging handle
    _internals: { contourTile: contourTile, smoothBlock: smoothBlock, jumpAt: jumpAt, CONTOURS: CONTOURS, CONTOUR_JUMP: CONTOUR_JUMP,
      CONTOUR_BLOCK: CONTOUR_BLOCK, CONTOUR_PROFILE: CONTOUR_PROFILE, contourProfile: contourProfile, saved: saved, legendBar: legendBar, buildRamp: buildRamp, buildLut: buildLut, legendPos: legendPos, legendInv: legendInv, KNOTS: KNOTS, SCALES: SCALES, scaleFor: scaleFor, TICKS: TICKS, unitOf: unitOf, ModelGridLayer: ModelGridLayer, Overlay: Overlay, RAMPS: RAMPS,
      frameKey: frameKey, pickFrame: pickFrame, validateManifest: validateManifest, validateGrid: validateGrid,
      wantHalf: wantHalf, wantFull: wantFull, legendTicks: legendTicks, tilePixelLatLng: tilePixelLatLng,
      parsePng: parsePng, unfilter: unfilter, decodePngGrey: decodePngGrey,
      forwardPixel: forwardPixel, snapToPixel: snapToPixel, pixelOf: pixelOf, pad3: pad3,
      ringPlan: ringPlan, nextAvailable: nextAvailable, stepAvailable: stepAvailable, nearestIndex: nearestIndex, dotMonth: dotMonth, restoreIndex: restoreIndex, SESSION_KEY: SESSION_KEY, FrameCache: FrameCache, failureKind: failureKind, SPEEDS: SPEEDS, BASE_FPS: BASE_FPS,
      MAX_DECODED: MAX_DECODED, MAX_INFLIGHT: MAX_INFLIGHT, STALL_MS: STALL_MS, LATE_TRIES: LATE_TRIES,
      PLAY_RING: PLAY_RING, ringFor: ringFor, tileInRange: tileInRange, STALE_TILE_MS: STALE_TILE_MS, STALE_MAX_MS: STALE_MAX_MS,
      workerSource: workerSource, workerMain: workerMain, FrameDecoder: FrameDecoder, getDecoder: getDecoder, disposeDecoder: disposeDecoder, resetDecoder: resetDecoder, setDecoderForTest: setDecoderForTest,
      decodeFrame: decodeFrame, nodeWindow: nodeWindow, windowCovers: windowCovers, LOOK: LOOK, WATCHDOG_MS: WATCHDOG_MS, isWebKit: isWebKit, OV_DEBUG: OV_DEBUG, PARTICLE_PX2: PARTICLE_PX2, FLOW_STYLE: FLOW_STYLE, pageQuery: pageQuery,
      PAINT_FALLBACK_MS: PAINT_FALLBACK_MS, BUSY_STUCK_MS: BUSY_STUCK_MS, TRAIL_SPAN_MS: TRAIL_SPAN_MS, TRAIL_POINTS: TRAIL_POINTS, TRAIL_EVERY_MS: TRAIL_EVERY_MS,
      worldXY: worldXY, decodeCoast: decodeCoast, tileBox: tileBox, coastCellsForTile: coastCellsForTile, withinCell: withinCell, landPathsForTile: landPathsForTile,
      rasteriseScanline: rasteriseScanline, rasterise: rasterise, maskState: maskState, composeTile: composeTile, CoastStore: CoastStore, coastStore: coastStore,
      LAND_ALL: LAND_ALL, CLIP_FIELDS: CLIP_FIELDS, OUTLINE_FIELDS: OUTLINE_FIELDS, WIND_LOOK: WIND_LOOK, FIXED_OPACITY: FIXED_OPACITY, BASE_FADE_MS: BASE_FADE_MS, RELIEF_RETRY_MS: RELIEF_RETRY_MS, coastEdges: coastEdges, applyEdges: applyEdges, LAND_READOUT: LAND_READOUT, MAX_CHUNK_BYTES: MAX_CHUNK_BYTES, MAX_COAST_INFLIGHT: MAX_COAST_INFLIGHT,
      validCoastIndex: validCoastIndex,
      SstStore: SstStore, sstStore: sstStore, validSstManifest: validSstManifest, validSstPointer: validSstPointer, sstTierFor: sstTierFor, SST_FDEF: SST_FDEF,
      SST_TIER_ZOOMS: SST_TIER_ZOOMS, SST_PREFIX: SST_PREFIX, SST_PAD_PX: SST_PAD_PX, MAX_SST_INFLIGHT: MAX_SST_INFLIGHT, MAX_SST_CHUNK_BYTES: MAX_SST_CHUNK_BYTES, cellsForTile: cellsForTile,
      FlowAnimator: FlowAnimator, sampleRow: sampleRow,
      vectorNodes: vectorNodes, flowField: flowField, FLOW_SPEED: FLOW_SPEED, dirFieldOk: dirFieldOk, dirRes: dirRes, DIR_FIELDS: DIR_FIELDS, PARTICLE_LIFE_MS: PARTICLE_LIFE_MS,
      latOfWorldY: latOfWorldY, lngOfWorldX: lngOfWorldX, PARTICLE_PX_PER_S: PARTICLE_PX_PER_S, PARTICLE_MIN: PARTICLE_MIN, PARTICLE_MAX: PARTICLE_MAX,
      ANIM_BUDGET_MS: ANIM_BUDGET_MS, TRAIL_POINTS: TRAIL_POINTS, TRAIL_EVERY_MS: TRAIL_EVERY_MS, dirGridsOk: dirGridsOk,
      frameAtHour: frameAtHour, localClock: localClock, runTimes: runTimes, TimelineState: TimelineState,
      MODEL_DEFS: MODEL_DEFS, DEFAULT_MODEL: DEFAULT_MODEL, modelList: modelList, modelOf: modelOf, hasModel: hasModel, STALE_AFTER_S: STALE_AFTER_S, CYCLE_MS: CYCLE_MS,
      LOAD_STALL_MS: LOAD_STALL_MS,
      SPEED_ANIMALS: SPEED_ANIMALS, animalOf: animalOf, speedOf: speedOf, assetUrl: assetUrl, speedSelector: speedSelector,
      ribbonFormatter: ribbonFormatter, localMidnightBefore: localMidnightBefore, ribbonScale: ribbonScale, ribbonLayout: ribbonLayout,
      RibbonState: RibbonState, RIBBON_MIN_TICK_GAP: RIBBON_MIN_TICK_GAP, RIBBON_TAP_PX: RIBBON_TAP_PX, RIBBON_AXIS_PX: RIBBON_AXIS_PX,
      RIBBON_DAY_MIN_PX: RIBBON_DAY_MIN_PX, RIBBON_FALLBACK_W: RIBBON_FALLBACK_W }
  };
})();
