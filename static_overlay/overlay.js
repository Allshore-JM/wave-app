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
    // lakes: the Great Lakes wave frames (NOAA GLWU, plan section 50) are drawn under this model's wave height and period
    gfswave: { prefix: 'gfswave/0p25/v1', label: 'GFS-Wave (WAVEWATCH III)', short: 'GFS', cycleMs: 6 * 3.6e6, staleS: 9 * 3600, table: 'cycle', attribution: ATTRIBUTION, lakes: true },
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
    // sea surface temperature over the legend position of -2..35 C (KNOTS.sst; plan section 44): the owner's pick B, a
    // thermal scale (2026-10-16): deep purple, blue, teal, green, yellow, orange, red, dark red
    sst:  [[0,'#2d004b'],[0.14,'#3f2a9b'],[0.26,'#2c6fbb'],[0.36,'#1fa1a8'],[0.46,'#3db56b'],[0.56,'#a6cf3a'],[0.66,'#f2d33a'],
           [0.76,'#f39c2c'],[0.86,'#e0502a'],[0.94,'#b2182b'],[1,'#67001f']],
    hs:   [[0,'#1d3a8a'],[0.08,'#2563eb'],[0.17,'#0ea5e9'],[0.25,'#22d3ee'],[0.33,'#34d399'],[0.40,'#a3e635'],[0.47,'#facc15'],
           [0.58,'#fb923c'],[0.72,'#ef4444'],[0.87,'#c026d3'],[1,'#f5d0fe']],
    tp:   [[0,'#2a1f7a'],[0.25,'#2e7ed8'],[0.5,'#38c9a8'],[0.7,'#c8e63c'],[0.85,'#f7a52b'],[1,'#e8321f']],
    // wind: blue at the calm end (never white: it covers land), a hue change every ~2 m/s up to ~20 m/s
    // (0 2 4 6 8 10 12 14 16 18 21 25 m/s over the 0-30.9 m/s legend), purple then pale mauve at the top
    wind: [[0,'#6271b7'],[0.0648,'#3961a0'],[0.1296,'#4a94a9'],[0.1944,'#4d8d7b'],[0.2592,'#53a553'],[0.324,'#359f35'],
           [0.3888,'#a79d51'],[0.4536,'#9f7f3a'],[0.5184,'#a16c5c'],[0.5832,'#813a4e'],[0.6804,'#af5088'],[0.81,'#754a93'],[1,'#d9c2d9']]
  };
  // The radar's colours over the legend's 5-65 dBZ (the familiar radar scale) and the tint over the area radar sees, 16 % white
  // (the owner's pick on real frames, plan section 85).
  RAMPS.radar = [[0, '#04e9e7'], [0.083, '#019ff4'], [0.167, '#0300f4'], [0.25, '#02fd02'], [0.333, '#01c501'], [0.417, '#008e00'], [0.5, '#fdf802'],
                 [0.583, '#e5bc00'], [0.667, '#fd9500'], [0.75, '#fd0000'], [0.833, '#d40000'], [0.917, '#bc0000'], [1, '#f800fd']];
  // Snow and the estimated mix over the same 5-65 dBZ legend (plan section 89; snow seldom passes 35 dBZ, so the colours change most
  // in the bar's lower half). Snow lavender to purple, mix pink: the owner's pick of 2026-11-08 from two candidates on real frames.
  RAMPS.radarSnow = [[0, '#f1ebff'], [0.1, '#dacdff'], [0.2, '#bea8ff'], [0.3, '#9f80f2'], [0.42, '#7f57d9'], [0.55, '#5f33b3'], [1, '#2c0f63']];
  RAMPS.radarMix = [[0, '#ffe3f0'], [0.1, '#ffc6e0'], [0.2, '#ff9fca'], [0.3, '#f774b0'], [0.42, '#e04b95'], [0.55, '#b82a76'], [1, '#6b0f42']];
  var RADAR_TINT = [255, 255, 255, 41], RADAR_ALPHA = [0.57, 1];   // alpha at the 5-dBZ floor and from 25 dBZ up (the layer's own opacity on top)
  // The radar's opacity (plan section 89): one CSS opacity on the layer, set by the panel's transparency slider; kept on the device.
  var RADAR_OPACITY_KEY = 'allshore.radarOpacity.v1', RADAR_OPACITY_MIN = 20, RADAR_OPACITY_MAX = 100, RADAR_OPACITY_DEFAULT = 75;
  function radarOpacityOf(v) {
    var n = typeof v === 'string' && /^[0-9]{1,3}$/.test(v) ? +v : typeof v === 'number' && isFinite(v) ? Math.round(v) : NaN;
    return n >= RADAR_OPACITY_MIN && n <= RADAR_OPACITY_MAX ? n : RADAR_OPACITY_DEFAULT;
  }
  function readRadarOpacity() { try { return radarOpacityOf(window.localStorage.getItem(RADAR_OPACITY_KEY)); } catch (e) { return RADAR_OPACITY_DEFAULT; } }
  function writeRadarOpacity(n) { try { window.localStorage.setItem(RADAR_OPACITY_KEY, String(n)); } catch (e) {} }
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
    hs: [[0, 0], [0.5, 0.08], [1, 0.17], [2, 0.33], [3, 0.47], [4, 0.58], [6, 0.72], [9, 0.87], [12, 1]],
    // sea surface temperature: 15-30 C (the surf's water) takes 56 % of the scale, the cold end and the hottest seas the rest
    sst: [[-2, 0], [5, 0.14], [10, 0.22], [15, 0.3], [20, 0.48], [25, 0.68], [30, 0.86], [35, 1]]
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
    'sst|US': [40, 50, 60, 70, 80], 'sst|Metric': [0, 5, 10, 15, 20, 25],
    'radar|US': [10, 20, 30, 40, 50], 'radar|Metric': [10, 20, 30, 40, 50]
  };
  // ---- sea surface temperature (plan section 44) ----
  // An OBSERVATION beside the model fields: the newest NASA MUR day, published by tools/model_frames/sst.py as a tiered
  // store (sst/v1/latest.json -> manifest -> one 0.25-degree frame on the model grid + 0.05- and 0.01-degree cells with
  // a one-node apron and a per-cell code range). The layer samples it through a pluggable SOURCE (SstStore) that writes
  // fractional codes over this one FIXED range into the layer's code buffer, so the tile composer, the land clip, the
  // readout and the contours work unchanged. deg C throughout (the site unit only at the legend and the readout).
  var SST_FDEF = { lo: -2, hi: 35, legend: [-2, 35], units: 'degC', interpolation: 'bilinear' };
  var SST_PREFIX = 'sst/v1';
  // <= 29 visible characters (the GFS rule): "Overlay: NASA MUR SST" (+ " · GSHHG" from the clip); the title has the source.
  var SST_ATTRIBUTION = 'Overlay: <a href="https://podaac.jpl.nasa.gov/dataset/MUR-JPL-L4-GLOB-v4.1" target="_blank" rel="noopener" ' +
    'title="NASA JPL MUR SST v4.1 (GHRSST L4, doi:10.5067/GHGMR-4FJ04) via NOAA CoastWatch ERDDAP; tiled and rendered by Allshore Surf. A daily night-time (foundation) temperature: the afternoon surf zone can be ' +
    'warmer; near the coast it can be about 1 C (2 F) off either way.">NASA MUR SST</a>';
  var SST_HEAD = 'NASA MUR (1 km, daily)';                 // the open panel's head line (the source; the dropdown says what it is)
  var SST_COARSE_BELOW = 7;                                // SST isotherms every 2 C / 4 F below this tile zoom (owner, G31)
  var SST_STALE_DAYS = 4;                                  // the job's own stale alarm (tools/model_frames/sst.py STALE_DAYS)
  // The analysis day "YYYY-MM-DD" (a calendar day: no time zone applies) as "Thu, Oct. 15"; anything else as given.
  function sstDayText(day) {
    var m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(day || ''));
    if (!m) return String(day || '');
    var d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], 12));
    try {
      var wd = new Intl.DateTimeFormat('en-US', { timeZone: 'UTC', weekday: 'short' }).format(d);
      var md = new Intl.DateTimeFormat('en-US', { timeZone: 'UTC', month: 'short', day: 'numeric' }).format(d);
      return wd + ', ' + dotMonth(md);
    } catch (e) { return String(day); }
  }
  // How old the day is in days (its nominal time is 09:00 UTC), or null.
  function sstAgeDays(day, now) {
    var t = Date.parse(String(day || '') + 'T09:00:00Z');
    return isNaN(t) ? null : ((now === undefined ? Date.now() : now) - t) / 86400000;
  }
  // The chunk tier drawn by tile zoom: below 5 the 0.25-degree frame (a 1-km node is a fraction of a pixel there), 5-7
  // the 0.05-degree cells, 8 and closer the 0.01-degree cells (Leaflet rounds 7.5 up). A tile whose cells are not all
  // in hand yet draws from the next coarser tier (the frame always is) and is redrawn when they land.
  var SST_TIER_ZOOMS = [5, 8];
  function sstTierFor(z, available) {
    var t = z >= SST_TIER_ZOOMS[1] ? 2 : z >= SST_TIER_ZOOMS[0] ? 1 : 0;
    return t < available ? t : available;
  }
  // ---- satellite waves (plan sections 45 and 81) ----
  // Two OBSERVATION products published by tools/model_frames/alt.py (altimeter wave heights, alt/v1) and sarswell.py
  // (Sentinel-1 swell partitions, sarswell/v1): a pointer naming one chunk per 3-hour window, each chunk a header and ONE
  // zlib stream of columns (altfmt.py). The layer's own entry ("Satellite waves") draws the last 24 h faded by age, as
  // height dots in the wave-height colours or as swell arrows (the Heights | Swell switch); the "Satellite readings"
  // box under Wave height draws the height dots within 3 h of the frame on the map, in the model's own colours.
  var SAT_PREFIX = 'alt/v1', SWELL_PREFIX = 'sarswell/v1';
  var SAT_WINDOW_MS = 24 * 3.6e6;                          // the layer's own entry: the last 24 h of readings
  var SAT_RECHECK_MS = 12 * 60 * 1000;                     // the pointer re-read while shown (the job publishes every ~10 min)
  var SAT_STALE_H = 12, SWELL_STALE_H = 48;                // the jobs' own stale alarms (alt.py STALE_H, sarswell.py STALE_H)
  var SAT_FDEF = { lo: 0, hi: 22.86, legend: [0, 18.288], units: 'm', interpolation: 'bilinear' };   // = SCALES.hs: a dot is coloured as the field would draw that value
  var SAT_HEAD = 'Satellite waves (CMEMS, last 24 h)';
  var SAT_CREDIT = 'Generated using E.U. Copernicus Marine Service Information: wave heights WAVE_GLO_PHY_SWH_L3_NRT_014_001 (doi:10.48670/moi-00179), Sentinel-1 swell WAVE_GLO_PHY_SPC-FWK_L3_NRT_014_002 (doi:10.48670/moi-00178), gridded wave height WAVE_GLO_PHY_SWH_L4_NRT_014_003 (doi:10.48670/moi-00180); rendered by Allshore Surf';
  // <= 29 visible characters (the GFS rule): "Overlay: Copernicus Marine"; the title carries the full credit and the DOIs
  var SAT_ATTRIBUTION = 'Overlay: <a href="https://doi.org/10.48670/moi-00179" target="_blank" rel="noopener" title="' + SAT_CREDIT + '">Copernicus Marine</a>';
  var SAT_FADE = [1, 0.35];                                // a dot's alpha: the newest reading .. the window's far end
  var MAX_SAT_CHUNKS = 20, MAX_SAT_CHUNK_BYTES = 2 * 1024 * 1024, MAX_SAT_SAMPLES = 600000, MAX_SAT_INFLIGHT = 3, SAT_BUCKET_DEG = 0.5;
  var SAT_MAGIC = { alt: 'ALT1', swell: 'SWL1' }, SAT_FORMAT = { alt: 'alt-v1', swell: 'swell-v1' };
  // The columns exactly as altfmt.py defines them (name, dtype, scale, none): a chunk or pointer with others is refused.
  var SAT_COLUMNS = {
    alt: [['t', 'u4', 1, null], ['lat', 'i4', 1e-6, null], ['lon', 'i4', 1e-6, null], ['swh', 'u2', 0.001, 65535], ['model', 'u2', 0.001, 65535],
          ['mission', 'u1', 1, null], ['run', 'u1', 1, 255], ['flags', 'u1', 1, null]],
    swell: [['t', 'u4', 1, null], ['lat', 'i4', 1e-6, null], ['lon', 'i4', 1e-6, null], ['n', 'u1', 1, null], ['mission', 'u1', 1, null], ['flags', 'u1', 1, null],
            ['hs1', 'u2', 0.001, 65535], ['hs2', 'u2', 0.001, 65535], ['hs3', 'u2', 0.001, 65535], ['tp1', 'u2', 0.01, 65535], ['tp2', 'u2', 0.01, 65535], ['tp3', 'u2', 0.01, 65535],
            ['dir1', 'u2', 0.01, 65535], ['dir2', 'u2', 0.01, 65535], ['dir3', 'u2', 0.01, 65535]]
  };
  var SAT_FLAG_NEAR_LAND = 1, SAT_FLAG_MODEL_EDGE = 2;
  var SAT_EPOCH_MS = Date.UTC(2000, 0, 1);
  var SAT_DTYPE_BYTES = { u1: 1, u2: 2, u4: 4, i4: 4 };
  var SAT_KEY = 'Hollow dots: within a grid cell of land (less reliable).';
  var SAT_TAP_MS = 4000;                                   // a tapped satellite mark's readout stays this long (G52 B-4)
  // ---- the gridded satellite wave height (plan section 82) ----
  // tools/model_frames/altgrid.py publishes the newest day of Copernicus Marine's level-4 gridded significant wave height
  // (0.5 deg, 6-hourly; doi 10.48670/moi-00180) as PNG frames on the model's half grid under altgrid/v1/; the "Gridded
  // map" box of the Satellite waves entry draws the NEWEST step under the dots / arrows, in the same colours (SAT_FDEF).
  var GRID_PREFIX = 'altgrid/v1', GRID_FORMAT = 'altgrid-v1', GRID_DOI = '10.48670/moi-00180';
  var GRID_WARN_H = 48, GRID_HIDE_H = 72;                  // the newest step older than this: a warning; older still: hidden (owner, section 82)
  var GRID_EXPECT = { cols: 720, rows: 361 }, MAX_GRID_POINTER_BYTES = 64 * 1024, MAX_GRID_MANIFEST_BYTES = 64 * 1024;
  var GRID_HALF = { cols: 720, rows: 361, lon0: -180, lat0: 90, dlon: 0.5, dlat: -0.5, registration: 'center', lon_periodic: true };   // = the model's half grid
  // ---- the radar layer's constants (plan section 85): the job's format (tools/model_frames/radar.py) and the owner's loops ----
  var RADAR_PREFIX = 'radar/v1', RADAR_FORMAT = 'radar-v1', RADAR_FLOOR = 5, RADAR_STEP = 0.5, RADAR_MAX_CODE = 142;
  var RADAR_PPD = { t0: 10, t1: 25, t2: 100 }, RADAR_CELL = { t1: 20, t2: 5 };
  var RADAR_LOOPS = { 2: 10, 6: 20, 12: 30, 24: 60 }, RADAR_LOOP_HOURS = [2, 6, 12, 24], RADAR_DEFAULT_LOOP = 2;   // hours -> minutes between frames (owner, 2026-11-06)
  var RADAR_ORDER = ['hawaii', 'guam', 'conus', 'alaska', 'carib', 'europe', 'japan', 'taiwan'];   // who draws a pixel two regions cover
  var RADAR_T1_ZOOM = 4, RADAR_T2_ZOOM = 7;                 // tile zoom from which t1 / t2 are wanted: about one screen pixel per picture node
  var RADAR_FILL = 0.5;                                     // a t1 / t2 cell's share of its full size before any has landed (kept: the box round its rain)
  var RADAR_HOLD_STEPS = 2;                                 // a region missing a step holds its previous frame this many steps, then shows none
  var RADAR_MAX_BYTES = 48 * 1024 * 1024, RADAR_MAX_BYTES_SMALL = 32 * 1024 * 1024, RADAR_SMALL_PX = 768, RADAR_INFLIGHT = 4, RADAR_POINTER_BYTES = 256 * 1024;
  var READOUT_DX = 12, READOUT_DY = 28, READOUT_PAD = 4, READOUT_NEAR_EDGE = 360;   // px: the readout's offset from the cursor (the CSS's), and how near the right edge it is measured
  var RADAR_REGION_AREA = 6000, RADAR_TOTAL_AREA = 20000;    // square degrees (today's largest region: Europe 4,312; all six 8,545)
  var RADAR_STALL_MS = 15000, RADAR_RETRY_MS = 60000, RADAR_PUBLISH_MS = 10 * 60 * 1000, RADAR_UI_MS = 30000;
  var RADAR_SETTLE_MS = 120000;                              // a publish this young may still be writing its later regions: read again just after this
  var RADAR_STEP_WAIT_MS = 5000, RADAR_END_HOLD_MS = 1000, RADAR_WARN_MIN = 30;
  var RADAR_PLAY_HOLD_MS = 3000, RADAR_RECOVER_MS = 60000, RADAR_DAMAGED_RETRY_MS = 5 * 60000;   // Play's wait for the view's stacks; a refused pointer asked again; a damaged file asked again
  var RADAR_FDEF = { lo: 5, hi: 75, legend: [5, 65] };
  // Rain 2-142 (0.5 dB), snow 143-213 (1 dB), mix 214-249 (2 dB), each from 5 dBZ; 250-255 never written (radar.py TYPE_CODES). A
  // sampled pixel is stored as type * RADAR_TYPE_BASE + its value on the rain codes' scale: one colour table, one readout.
  var RADAR_TYPE_CODES = { snow: [143, 213, 1], mix: [214, 249, 2] }, RADAR_TYPE_BASE = 143, RADAR_TYPES = ['rain', 'snow', 'mix'];
  var RADAR_RC = new Float64Array(256), RADAR_T = new Uint8Array(256);
  (function () {
    for (var v = 2; v <= RADAR_MAX_CODE; v++) RADAR_RC[v] = v;
    RADAR_TYPES.forEach(function (name, t) {
      var b = RADAR_TYPE_CODES[name];
      if (!b) return;
      for (var c = b[0]; c <= b[1]; c++) { RADAR_RC[c] = (RADAR_FLOOR + (c - b[0]) * b[2] - RADAR_FLOOR) / RADAR_STEP + 2; RADAR_T[c] = t; }
    });
  })();
  // A pointer's type_codes must say exactly what this client reads; else its codes past the rain ones are never drawn.
  function radarTypeCodesOk(tc) {
    if (!tc || typeof tc !== 'object' || Array.isArray(tc)) return false;
    var names = Object.keys(tc);
    if (names.length !== 2) return false;
    for (var i = 0; i < names.length; i++) {
      var want = RADAR_TYPE_CODES[names[i]], got = tc[names[i]];
      if (!want || !Array.isArray(got) || got.length !== 3 || got[0] !== want[0] || got[1] !== want[1] || got[2] !== want[2]) return false;
    }
    return true;
  }
  var RADAR_WORDS = [[10, 'very light'], [25, 'light'], [35, 'moderate'], [45, 'heavy'], [58, 'very heavy']];
  // Japan's files hold JMA's eight rain-rate classes as the dBZ codes of the same rain (tools/model_frames/jma.py; a Python test compares
  // the rows): [code, from mm/h, to mm/h, the word]. A pixel of such a region is one of these codes, never between two.
  var RADAR_JMA8 = [[28, 0, 1, 'very light rain'], [49, 1, 5, 'light rain'], [65, 5, 10, 'moderate rain'], [75, 10, 20, 'heavy rain'], [82, 20, 30, 'heavy rain'], [89, 30, 50, 'heavy rain'], [96, 50, 80, 'very heavy rain'], [102, 80, null, 'very heavy rain']];
  var RADAR_JMA8_SNAP = (function () {                       // code -> the nearest class's code; a tie goes to the lower class
    var out = new Uint8Array(RADAR_MAX_CODE + 2);
    for (var c = 0; c < out.length; c++) {
      var best = RADAR_JMA8[0][0];
      for (var i = 1; i < RADAR_JMA8.length; i++) if (Math.abs(RADAR_JMA8[i][0] - c) < Math.abs(best - c)) best = RADAR_JMA8[i][0];
      out[c] = best;
    }
    return out;
  })();
  // Who the frames in view come from: the corner's short credit, the panel's head, the link's tooltip.
  var RADAR_SOURCES = { mrms: { credit: 'MRMS', head: 'NOAA MRMS', full: 'NOAA MRMS' }, opera: { credit: 'OPERA CC BY 4.0', head: 'EUMETNET OPERA', full: 'EUMETNET OPERA' },
    jma: { credit: 'JMA', head: 'JMA', full: 'Japan Meteorological Agency' }, cwa: { credit: 'CWA', head: 'CWA', full: 'Taiwan Central Weather Administration' } };
  var RADAR_SOURCE_ORDER = ['mrms', 'opera', 'jma', 'cwa'];
  function radarKnown(srcs) { return RADAR_SOURCE_ORDER.filter(function (k) { return srcs.indexOf(k) >= 0; }); }
  function radarHead(srcs) {
    var known = radarKnown(srcs);
    return known.length ? 'Radar: ' + known.map(function (k) { return RADAR_SOURCES[k].head; }).join(', ') : 'Radar';
  }
  // The corner's credit for the sources in view: at most 29 visible characters whichever they are (the map's rule); the licences and
  // the "modified" are in the notice it opens. Only this module's own words go into the HTML, never a name from the pointer.
  function radarCredit(srcs) {
    var known = radarKnown(srcs), names = known.map(function (k) { return RADAR_SOURCES[k].full; });
    var text = !srcs.length ? 'sources' : srcs.length > 2 || known.length !== srcs.length ? srcs.length + (srcs.length === 1 ? ' source' : ' sources') : known.map(function (k) { return RADAR_SOURCES[k].credit; }).join(' · ');
    var who = names.length ? names.slice(0, -1).join(', ') + (names.length > 1 ? ' and ' : '') + names[names.length - 1] : 'The radar sources';
    return 'Radar: <a href="/notice/radar" target="_blank" rel="noopener" title="' + who + ', modified by Allshore Surf">' + text + '</a>';
  }
  // The credit's visible text is 29 characters (the map's rule): the licences and the "modified" are in the notice it opens.
  var RADAR_ATTRIBUTION = radarCredit(['mrms', 'opera']);
  // ---- bathymetry (plan section 93b): what the obs kind 'bathy' needs at load (its store and layer come later) ----
  var BATHY_PREFIX = 'static/bathy';
  // Metres below mean sea level (negative = dry at MSL); the legend runs to 5,000 m ("5000+"), the tiles to 11,000.
  var BATHY_FDEF = { lo: 0, hi: 11000, legend: [0, 5000] };
  // 0-30 m (the reef, the bar, the channel) takes 60 % of the scale; deeper water the muted rest (owner, plan section 93).
  KNOTS.bathy = [[0, 0], [5, 0.2], [10, 0.33], [20, 0.5], [30, 0.6], [100, 0.72], [1000, 0.86], [5000, 1]];
  // Turquoise to blue, muted navy past 30 m (the owner's pick of two candidates on real tiles, 2026-11-11).
  RAMPS.bathy = [[0, '#d6f5ee'], [0.08, '#9fe8de'], [0.2, '#52cfd0'], [0.33, '#2aa8cc'], [0.5, '#1f7fbf'], [0.6, '#1f5ea8'], [0.72, '#2b4f80'], [0.86, '#273f63'], [1, '#1e2f48']];
  TICKS['bathy|Metric'] = [0, 5, 10, 20, 30, 100];
  TICKS['bathy|US'] = [0, 15, 30, 60, 100, 300];
  // The transparency slider under the scale (the radar's pattern): 20-100 % opacity kept on the device, default 85 %.
  var BATHY_OPACITY_KEY = 'allshore.bathyOpacity.v1', BATHY_OPACITY_MIN = 20, BATHY_OPACITY_MAX = 100, BATHY_OPACITY_DEFAULT = 85;
  function bathyOpacityOf(v) {
    var n = typeof v === 'number' ? v : (typeof v === 'string' && /^\d{1,3}$/.test(v) ? Number(v) : NaN);
    return n === n && Math.floor(n) === n && n >= BATHY_OPACITY_MIN && n <= BATHY_OPACITY_MAX ? n : BATHY_OPACITY_DEFAULT;
  }
  function readBathyOpacity() { try { return bathyOpacityOf(window.localStorage.getItem(BATHY_OPACITY_KEY)); } catch (e) { return BATHY_OPACITY_DEFAULT; } }
  function writeBathyOpacity(n) { try { window.localStorage.setItem(BATHY_OPACITY_KEY, String(n)); } catch (e) {} }
  // The credit: "Bathymetry: " + the sources' short names (NOAA, GEBCO ...) while they fit the map's 29 characters, else
  // "Bathymetry: N sources"; the citations, licences and "modified" are in the notice it opens.
  function bathyCredit(names) {
    var list = [], seen = {};
    (names || []).forEach(function (n) { n = String(n || '').trim(); if (n && !seen[n]) { seen[n] = true; list.push(n); } });
    if (!list.length) list = ['NOAA', 'GEBCO'];          // a view with no depth drawn (land only) credits the usual pair
    var text = list.join(' · ');
    if (('Bathymetry: ' + text).length > 29) text = list.length + (list.length === 1 ? ' source' : ' sources');
    var who = list.join(', ');
    return 'Bathymetry: <a href="/notice/bathy" target="_blank" rel="noopener" title="' + who.replace(/["<>&]/g, '') + ', modified by Allshore Surf">' + text.replace(/[<>&]/g, '') + '</a>';
  }
  var BATHY_ATTRIBUTION = bathyCredit(['NOAA', 'GEBCO']);

  // The observations the panel can show (mode 'obs'): own keys only (G30-9).
  var OBS_DEFS = { sst: { label: 'sea surface temperature', attribution: SST_ATTRIBUTION }, sat: { label: 'satellite waves', attribution: SAT_ATTRIBUTION }, radar: { label: 'radar', attribution: RADAR_ATTRIBUTION }, bathy: { label: 'bathymetry', attribution: BATHY_ATTRIBUTION } };
  function hasObs(key) { return typeof key === 'string' && Object.prototype.hasOwnProperty.call(OBS_DEFS, key); }

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
  // (the knots span k0..kl; wave height's start at 0, the temperature's at -2 C)
  function knotsFit(k, legend) { return legend[0] === k[0][0] && legend[1] === k[k.length - 1][0]; }
  function legendPos(field, legend, v) {
    var k = knotsOf(field, legend);
    if (k) { var k0 = k[0][0], kl = k[k.length - 1][0]; return through(k, knotsFit(k, legend) ? v : k0 + (v - legend[0]) / (legend[1] - legend[0]) * (kl - k0), 0, 1); }
    return Math.max(0, Math.min(1, (v - legend[0]) / (legend[1] - legend[0])));
  }
  function legendInv(field, legend, p) {
    var k = knotsOf(field, legend);
    if (k) { var x = through(k, p, 1, 0), k0 = k[0][0], kl = k[k.length - 1][0]; return knotsFit(k, legend) ? x : legend[0] + (x - k0) / (kl - k0) * (legend[1] - legend[0]); }
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
    if (field === 'radar') return { label: 'dBZ', f: function (v) { return v; }, inv: function (v) { return v; }, d: 0 };
    if (field === 'bathy') {                               // depth below mean sea level; the legend's top is 5000 m
      if (unit === 'Metric') return { label: 'm', f: function (v) { return v; }, inv: function (v) { return v; }, d: 1, top: function () { return '5,000+ m'; } };
      return { label: 'ft', f: function (v) { return v * 3.28084; }, inv: function (v) { return v / 3.28084; }, d: 1, top: function () { return '16,400+ ft'; } };
    }
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
    out.push({ pos: 1, label: u.top ? u.top(hi) : Math.round(hi) + '+ ' + u.label });
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
  var CLIP_FIELDS = { hs: true, tp: true, wind: false, sst: true, sat: true };   // sat: the gridded map of the Satellite waves entry (plan section 82)
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
  var COAST_FIRST_MS = 15000;                           // the first picture waits for the coastline at most this long; then it is drawn
                                                        // unclipped and clipped when the coastline lands (owner, G34 re-check R-1)
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
    // a padded box at the date line reaches past +-180: those cells are the other side's (the sampler wraps there too)
    var J = Math.round(360 / cellDeg), jMin = -Math.round(180 / cellDeg), j0 = Math.floor(lonW / cellDeg), j1 = Math.floor((lonE - eps) / cellDeg), seen = {};
    for (var i = i0; i <= i1; i++) for (var j = j0; j <= j1; j++) {
      var nm = (i * cellDeg) + '_' + ((((j - jMin) % J) + J) % J + jMin) * cellDeg;
      if (!seen[nm]) { seen[nm] = true; out.push(nm); }
    }
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
  // times the ocean fraction (255 - land). lake (plan section 50): the tile's Great Lakes water coverage (0..255 per
  // pixel, or null): the lakes are land in the coastline set, so a lake pixel's alpha is its lake coverage instead
  // (the larger of the two: one alpha rule, shared with the readout and the particles).
  function composeTile(codes, land, lut, lo, hi, L0, L1, d, lake) {
    var scale = 255 / (L1 - L0);
    for (var i = 0, k = 0; i < codes.length; i++, k += 4) {
      var code = codes[i], a = land === LAND_ALL ? 0 : land ? 255 - land[i] : 255;   // LAND_ALL: a tile of nothing but land (its lakes' water comes from lake)
      if (lake && lake[i] > a) a = lake[i];
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
  // zero: level 0 is a line too (SST's 0 C); wave height and period have no line at 0.
  function contourTile(F, W, rgba, minGapPx, ink, x0, y0, x1, y1, core, edge, zero) {
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
        if (lv === 0 && !zero) continue;
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
  var COAST_TIER0 = 'world-i.bin', LAKES_TIER0 = 'lakes-i.bin';   // the tier-0 overview files the two published sets carry
  // The lake-outline set (plan section 51): every lake of 100 km2 or more, its shore drawn as a line under the wind overlay
  // like the coastline, from this tile zoom on.
  var LAKEOUT_TIER0 = 'lakeoutlines-i.bin', LAKEOUT_PREFIX = 'static/coast/lakeoutlines-v1', LAKE_OUTLINE_MIN_ZOOM = 4;
  // file: the set's tier-0 overview (the land set's world-i.bin; the Great Lakes set's lakes-i.bin): fetched beside
  // index.json, which must name the same file (a set is never mixed with another set's overview).
  function coastStore(url, file) { return COAST_STORES[url] || (COAST_STORES[url] = new CoastStore(url, file)); }
  function CoastStore(url, file) {
    this.url = url; this.file = file || COAST_TIER0; this.status = 'idle'; this.index = null; this.tier0 = null; this.loading = null; this.failedAt = 0;
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
  CoastStore.prototype.timeoutMs = 15000;                  // a coast download that receives no byte for this long fails (unclipped + warning); a slow
                                                           // but moving one goes on (G34 B-6: 15 s in total failed on a 0.7-Mbit/s phone link)
  CoastStore.prototype.retryMs = COAST_RETRY_MS;
  CoastStore.prototype.load = function () {
    var self = this;
    if (this.status === 'ok') return Promise.resolve(this);
    if (this.loading) return this.loading;
    if (this.status === 'failed' && Date.now() - this.failedAt < this.retryMs) return Promise.resolve(null);
    this.status = 'loading';
    var base = this.url, ctrl = this.loadAbort = new AbortController(), sig = ctrl.signal, timedOut = false, watchdog = null, ms = this.timeoutMs, over = false;
    var arm = function () { if (over) return; clearTimeout(watchdog); watchdog = setTimeout(function () { timedOut = true; ctrl.abort(); }, ms); };
    arm();
    this.loading = Promise.all([
      fetch(base + '/index.json', { signal: sig, mode: 'cors' }).then(function (r) {
        if (!r.ok) throw new Error('coast ' + r.status);
        if (r.headers && r.headers.get && Number(r.headers.get('content-length') || 0) > MAX_INDEX_BYTES) throw new Error('unsupported coast index');
        arm();
        return r.json();
      }),
      fetch(base + '/' + self.file, { signal: sig, mode: 'cors' }).then(function (r) {
        if (!r.ok) throw new Error('coast ' + r.status);
        if (Number(r.headers.get('content-length') || 0) > MAX_COAST_BYTES) throw new Error('coast decode failed');
        arm();
        if (r.body && r.body.getReader) return readBody(r.body.getReader(), arm, MAX_COAST_BYTES, 'coast decode failed');   // every chunk re-arms the timer
        return r.arrayBuffer();
      })
    ]).then(function (res) {
      over = true; clearTimeout(watchdog);
      if (self.loadAbort !== ctrl || sig.aborted) throw abortError();     // aborted or superseded: this load owns nothing any more
      var idx = res[0];
      if (!validCoastIndex(idx) || (typeof idx.tier0.file === 'string' && idx.tier0.file !== self.file)) throw new Error('unsupported coast index');
      self.tier0 = decodeCoast(res[1]); self.index = idx; self.hasTier1 = Object.keys(idx.tier1.cells).length > 0;
      self.status = 'ok'; self.loading = null; self.loadAbort = null;
      return self;
    }).catch(function () {
      over = true; clearTimeout(watchdog);
      if (self.loadAbort === ctrl) {
        self.loading = null; self.loadAbort = null;
        self.status = sig.aborted && !timedOut ? 'idle' : 'failed';
        if (self.status === 'failed') self.failedAt = Date.now();
      }
      ctrl.abort();                                        // the other file stops too (its index failed: re-check R-4)
      return null;
    });
    return this.loading;
  };
  // The panel's note for a clipped field drawn without its coastline: still loading (re-check R-1), or given up.
  function coastNote(store) {
    return store && store.status === 'loading' ? 'Coastline still loading; the field is shown without coastline clipping until it arrives.'
      : 'Coastline data could not be loaded; the field is shown without coastline clipping.';
  }
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
  // Nothing wants the cells any more (the lake outlines not shown): the queue goes; a download under way finishes.
  CoastStore.prototype.dropQueue = function () { this.queue = []; this.queued = {}; };
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
    this.clock = 0;
    this.root = root; this.status = 'idle'; this.pointer = null; this.manifest = null; this.frame = null; this.grid = null; this.tiers = [];
    this.loading = null; this.loadAbort = null; this.failedAt = 0; this.loadedAt = 0; this.newer = null;
    this.inflight = {}; this.queue = []; this.queued = {}; this.failed = {}; this.bytes = 0;
    this.rev = 0; this.onChange = null; this.onNeeded = null; this.abort = null; this.retryTimer = null;
    this._lon0 = new Float64Array(TILE + 2); this._fx = new Float64Array(TILE + 2); this._colPos = new Float64Array(TILE + 2);
  }
  SstStore.prototype.timeoutMs = 15000;                    // a load step silent this long (no answer, no bytes): failed (Retry)
  SstStore.prototype.cellStallMs = 15000;                  // a cell download silent this long: given up, a cooldown and a retry
  SstStore.prototype.retryMs = COAST_RETRY_MS;
  SstStore.prototype.recheckMs = POINTER_RECHECK_MS;
  // The frames' decoder (tests replace it); onProgress is called as the body's bytes arrive (the stall timers).
  SstStore.prototype._decode = function (url, sig, expect, onProgress) { return decodeFrame(url, sig, expect, null, false, onProgress); };
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
  // force: a viewer's action (a pick, Retry, On) tries again at once; the cooldown only paces automatic re-loads (none
  // today: every caller is a mount; kept so a future automatic caller cannot hammer the bucket).
  SstStore.prototype.load = function (force) {
    var self = this;
    if (this.loading) return this.loading;
    if (this.status === 'ok' && !this.newer && Date.now() - this.loadedAt < this.recheckMs) return Promise.resolve(this);
    if (!force && this.status === 'failed' && Date.now() - this.failedAt < this.retryMs) return Promise.resolve(null);
    var was = this.status === 'ok' ? this.manifest : null;
    this.status = 'loading';
    var ctrl = this.loadAbort = new AbortController(), sig = ctrl.signal, timedOut = false, watchdog = null;
    // A slow link is not a dead one: each step (the pointer, the manifest, the frame's bytes) re-arms the timer (G31-3).
    var kick = function () { clearTimeout(watchdog); watchdog = setTimeout(function () { timedOut = true; ctrl.abort(); }, self.timeoutMs); };
    kick();
    this.loading = this._pointer(sig).then(function (ptr) {
      kick();
      if (!validSstPointer(ptr)) throw new Error('unsupported sst pointer');
      if (was && was.run === ptr.run) return null;                         // the day on screen is still the newest
      return self._json(self.root + '/' + ptr.manifest, sig, false, MAX_SST_MANIFEST_BYTES).then(function (m) {
        kick();
        if (!validSstManifest(m) || m.run !== ptr.run) throw new Error('unsupported sst manifest');
        return self._decode(self.root + '/' + m.tier0.key, sig, { cols: m.tier0.grid.cols, rows: m.tier0.grid.rows }, kick).then(function (frame) {
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
        if (rec) { rec.used = ++this.clock; continue; }                              // LRU touch (both tiers share the clock)
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
      (function (T, name, key, all) {
        // one controller per cell, aborted with the store's (idle / Off / a new day) or by its own stall timer (G31-4)
        var ctrl = new AbortController(), timer = null;
        var stop = function () { ctrl.abort(); };
        var done = function () { clearTimeout(timer); if (all.signal.removeEventListener) all.signal.removeEventListener('abort', stop); };
        function mine() { return self.inflight[key] === ctrl; }
        // a failure: 404 / undecodable final, anything else (a stall too) a cooldown with a retry timer
        var fail = function (kind) {
          delete self.inflight[key];
          self.failed[key] = kind === 'permanent' ? true : Date.now() + self.retryMs;
          if (self.failed[key] !== true) self._armRetry(self.failed[key]);
          self.rev++;
          if (self.onChange) self.onChange();
          self._pump();
        };
        // silent for cellStallMs (no answer, no bytes): the slot is freed at once and the download dropped
        var kick = function () { clearTimeout(timer); timer = setTimeout(function () { if (!mine()) return; done(); ctrl.abort(); fail('transient'); }, self.cellStallMs); };
        if (all.signal.addEventListener) all.signal.addEventListener('abort', stop);
        self.inflight[key] = ctrl;
        kick();
        self._decode(self.root + '/' + T.dir + '/' + name + '.png', ctrl.signal, { cols: T.n, rows: T.n }, kick).then(function (fr) {
          done();
          if (ctrl.signal.aborted || !mine()) return;
          if (!fr || fr.cols !== T.n || fr.rows !== T.n || !fr.q || fr.q.length !== T.n * T.n) throw new Error('frame decode failed');
          delete self.inflight[key];
          var ent = T.cells[name];
          self.rev++;
          T.loaded.set(name, { q: fr.q, lo: ent[0], hi: ent[1], bytes: fr.q.length, rev: self.rev, used: ++self.clock });
          self.bytes += fr.q.length; delete self.failed[key];
          self._evict();
          if (self.onChange) self.onChange();
          self._pump();
        }).catch(function (err) {
          done();
          if (!mine() || ctrl.signal.aborted || (err && err.name === 'AbortError')) return;
          fail(failureKind(err));
        });
      })(T, nm, key, this.abort);
    }
  };
  // Over the cap: the least recently used cells go first, of either tier (G31-10), never one a tile on the map could read now.
  SstStore.prototype._evict = function () {
    if (this.bytes <= MAX_SST_CHUNK_BYTES) return;
    var need = this.onNeeded ? this.onNeeded() : {}, all = [], t, i;
    for (t = 0; t < this.tiers.length; t++) this.tiers[t].loaded.forEach(function (rec, nm) { all.push([rec.used || 0, t, nm]); });
    all.sort(function (a, b) { return a[0] - b[0]; });
    for (i = 0; i < all.length && this.bytes > MAX_SST_CHUNK_BYTES; i++) {
      var T = this.tiers[all[i][1]], nm = all[i][2];
      if (need[T.name + '/' + nm]) continue;
      this.bytes -= T.loaded.get(nm).bytes; T.loaded.delete(nm);
    }
  };
  SstStore.prototype._stop = function () {
    if (this.abort) { this.abort.abort(); this.abort = null; }
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    this.inflight = {}; this.queue = []; this.queued = {};
  };
  // A field switch: downloads stop, the cells stay for the switch back.
  SstStore.prototype.idle = function () { this._stop(); this.onChange = null; this.onNeeded = null; };
  // A switch to a model field (owner, G31-9): downloads stop and the decoded cells go (they come back from the browser's
  // cache in ~0.1-0.2 s); the day's frame and manifest stay.
  SstStore.prototype.release = function () {
    this.idle();
    for (var t = 0; t < this.tiers.length; t++) this.tiers[t].loaded.clear();
    this.bytes = 0; this.failed = {};
  };
  // Off: the same, and a load in progress stops (the next On is instant, the day re-checked by load()).
  SstStore.prototype.abortAll = function () {
    this.release();
    if (this.loadAbort) { this.loadAbort.abort(); this.loadAbort = null; }
    if (this.status === 'loading') { this.status = this.manifest ? 'ok' : 'idle'; this.loading = null; }
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

  // ---- the satellite stores (plan sections 45 and 81) ----
  // One zlib stream as ONE ArrayBuffer of exactly `size` bytes through the browser's inflater (the frames' inflate takes
  // the stream in parts; this is the same pump over one part), capped by the expected size.
  function inflateBytes(bytes, size) { return inflate([bytes], size); }
  function satColumnsEqual(a, b) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    for (var i = 0; i < a.length; i++) {
      var x = a[i], y = b[i];
      if (!Array.isArray(x) || x.length !== 4 || x[0] !== y[0] || x[1] !== y[1] || x[2] !== y[2] || x[3] !== y[3]) return false;
    }
    return true;
  }
  function satStride(cols) { var n = 0; for (var i = 0; i < cols.length; i++) n += SAT_DTYPE_BYTES[cols[i][1]]; return n; }
  // Segments [[mission, start, count], ...] cover 0..n in order, each time-sorted (t: the u32 column).
  function satSegmentsOk(segs, n, t, missions) {
    if (!Array.isArray(segs)) return false;
    var pos = 0;
    for (var i = 0; i < segs.length; i++) {
      var s = segs[i];
      if (!Array.isArray(s) || s.length !== 3 || !Number.isInteger(s[0]) || !Number.isInteger(s[1]) || !Number.isInteger(s[2]) || s[0] < 0 || s[0] >= missions || s[1] !== pos || s[2] < 1) return false;
      for (var j = s[1] + 1; j < s[1] + s[2]; j++) if (t[j] < t[j - 1]) return false;
      pos += s[2];
    }
    return pos === n;
  }
  var LITTLE_ENDIAN = (function () { try { return new Uint8Array(new Uint16Array([1]).buffer)[0] === 1; } catch (e) { return true; } })();
  // The columns of an inflated stream as typed arrays (little-endian; copied into aligned buffers: an odd n leaves the u16
  // and i32 columns unaligned in the stream).
  function satSplitColumns(raw, cols, n) {
    var out = {}, pos = 0, dv = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
    for (var i = 0; i < cols.length; i++) {
      var name = cols[i][0], code = cols[i][1], size = SAT_DTYPE_BYTES[code], arr, k;
      if (code === 'u1') arr = raw.slice(pos, pos + n);
      else if (LITTLE_ENDIAN) arr = new (code === 'u2' ? Uint16Array : code === 'u4' ? Uint32Array : Int32Array)(raw.buffer.slice(raw.byteOffset + pos, raw.byteOffset + pos + n * size));
      else {
        arr = new (code === 'u2' ? Uint16Array : code === 'u4' ? Uint32Array : Int32Array)(n);
        for (k = 0; k < n; k++) arr[k] = code === 'u2' ? dv.getUint16(pos + k * 2, true) : code === 'u4' ? dv.getUint32(pos + k * 4, true) : dv.getInt32(pos + k * 4, true);
      }
      out[name] = arr;
      pos += n * size;
    }
    return out;
  }
  // A chunk (ArrayBuffer) -> Promise of {header, cols} after every check: the magic of `kind`, the header length, the
  // format and the columns pinned above, n within the cap, the inflated length n x stride, the segments contiguous and
  // time-sorted, every mission index listed, lat / lon in range. Anything else rejects "sat decode failed".
  function decodeSatChunk(buf, kind) {
    var bad = function () { return Promise.reject(new Error('sat decode failed')); };
    if (!(buf instanceof ArrayBuffer) || buf.byteLength < 8 || buf.byteLength > MAX_SAT_CHUNK_BYTES) return bad();
    var u8 = new Uint8Array(buf), dv = new DataView(buf), magic = String.fromCharCode(u8[0], u8[1], u8[2], u8[3]);
    if (magic !== SAT_MAGIC[kind]) return bad();
    var hlen = dv.getUint32(4, true), header;
    if (hlen > 1048576 || 8 + hlen > u8.length) return bad();
    try { header = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(u8.subarray(8, 8 + hlen))); } catch (e) { return bad(); }
    var cols = SAT_COLUMNS[kind];
    if (!header || typeof header !== 'object' || header.format !== SAT_FORMAT[kind] || !satColumnsEqual(header.columns, cols) || !Number.isInteger(header.n) || header.n < 0 || header.n > MAX_SAT_SAMPLES ||
      typeof header.window !== 'string' || !Array.isArray(header.missions) || !header.missions.length || header.missions.length > 255 ||
      !header.missions.every(function (m) { return m && typeof m === 'object' && typeof m.id === 'string'; }) || typeof header.credit !== 'string') return bad();
    var n = header.n, stride = satStride(cols);
    return inflateBytes(u8.subarray(8 + hlen), n * stride).then(function (raw) {
      var c = satSplitColumns(raw, cols, n);
      if (!satSegmentsOk(header.segments, n, c.t, header.missions.length)) throw new Error('sat decode failed');
      for (var i = 0; i < n; i++) if (c.lat[i] < -90000000 || c.lat[i] > 90000000 || c.lon[i] < -180000000 || c.lon[i] > 180000000 || c.mission[i] >= header.missions.length) throw new Error('sat decode failed');
      return { header: header, cols: c };
    }, function () { throw new Error('sat decode failed'); });
  }
  // The pointer the client can act on: the format and magic of `kind`, the columns pinned, at most MAX_SAT_CHUNKS chunks
  // each with a key, a window, parseable t0 < t1, a reading count and a size within the caps, missions as the chunks'.
  function validSatPointer(p, kind) {
    if (!p || typeof p !== 'object' || p.format !== SAT_FORMAT[kind] || p.magic !== SAT_MAGIC[kind] || p.encoding !== 'deflate' || !satColumnsEqual(p.columns, SAT_COLUMNS[kind]) ||
      typeof p.checked_utc !== 'string' || isNaN(Date.parse(p.checked_utc)) || !Array.isArray(p.chunks) || p.chunks.length > MAX_SAT_CHUNKS || !Array.isArray(p.missions) || p.missions.length > 255 ||
      !p.missions.every(function (m) { return m && typeof m === 'object' && typeof m.id === 'string' && typeof m.short === 'string'; })) return false;
    var prev = -Infinity, total = 0;
    for (var i = 0; i < p.chunks.length; i++) {
      var c = p.chunks[i];
      if (!c || typeof c !== 'object' || !sstKey(c.key) || c.key.indexOf((kind === 'alt' ? SAT_PREFIX : SWELL_PREFIX) + '/') !== 0 || typeof c.window !== 'string' || !/^\d{8}T\d{2}$/.test(c.window) ||
        typeof c.t0 !== 'string' || typeof c.t1 !== 'string' || isNaN(Date.parse(c.t0)) || isNaN(Date.parse(c.t1)) || Date.parse(c.t0) >= Date.parse(c.t1) || Date.parse(c.t0) < prev ||
        !Number.isInteger(c.n) || c.n < 0 || c.n > MAX_SAT_SAMPLES || !Number.isInteger(c.bytes) || c.bytes < 0 || c.bytes > MAX_SAT_CHUNK_BYTES || typeof c.build !== 'string') return false;
      prev = Date.parse(c.t1); total += c.n;
    }
    return total <= MAX_SAT_SAMPLES;
  }
  // The pointer's newest reading time in ms (newest_utc, else the last chunk's newest, else its t1), or null.
  function satNewestMs(p) {
    var t = typeof p.newest_utc === 'string' ? Date.parse(p.newest_utc) : NaN;
    if (!isNaN(t)) return t;
    for (var i = p.chunks.length - 1; i >= 0; i--) { var c = p.chunks[i], u = Date.parse(c.newest_utc || c.t1); if (!isNaN(u)) return u; }
    return null;
  }
  // ---- the gridded map's store (plan section 82): the pointer (no-cache), its manifest, the NEWEST step's frame through
  // the frames' decoder. A failed re-read keeps the frame in hand; a first failure waits retryMs unless forced (a viewer's
  // action); recheck() reads the pointer only and notes a newer manifest (load() then takes it in place).
  function validGridPointer(p) {
    return !!(p && p.format === GRID_FORMAT && p.complete === true && typeof p.run === 'string' && /^\d{8}$/.test(p.run) && sstKey(p.manifest) &&
      p.manifest.indexOf(GRID_PREFIX + '/') === 0 && typeof p.newest_utc === 'string' && !isNaN(Date.parse(p.newest_utc)));
  }
  function validGridManifest(m, ptr) {
    if (!m || m.format !== GRID_FORMAT || m.run !== ptr.run || m.complete !== true || m.encoding !== 'u8-linear-v2' || !m.grid || !m.fields || !m.fields.hs) return false;
    for (var k in GRID_HALF) if (m.grid[k] !== GRID_HALF[k]) return false;
    if (m.fields.hs.lo !== SAT_FDEF.lo || m.fields.hs.hi !== SAT_FDEF.hi) return false;   // another code space: refused, never mis-coloured
    if (!Array.isArray(m.frames) || m.frames.length < 1 || m.frames.length > 4) return false;
    var prev = -Infinity;
    for (var i = 0; i < m.frames.length; i++) {
      var fr = m.frames[i], t = fr ? Date.parse(fr.valid_utc) : NaN;
      if (!fr || typeof fr.step !== 'number' || isNaN(t) || t <= prev || !sstKey(fr.file) || fr.file.indexOf(GRID_PREFIX + '/') !== 0) return false;
      prev = t;
    }
    return true;
  }
  function GridStore(root) {
    this.root = root; this.status = 'idle'; this.pointer = null; this.manifest = null; this.entry = null; this.frame = null; this.grid = null;
    this.loading = null; this.loadAbort = null; this.failedAt = 0; this.loadedAt = 0; this.newer = null;
  }
  GridStore.prototype.timeoutMs = 15000;                   // a load step silent this long: failed
  GridStore.prototype.retryMs = COAST_RETRY_MS;
  GridStore.prototype.recheckMs = SAT_RECHECK_MS;
  GridStore.prototype._decode = function (url, sig, expect, onProgress) { return decodeFrame(url, sig, expect, null, false, onProgress); };
  GridStore.prototype._json = function (url, sig, noCache, maxBytes) {
    return fetch(url, noCache ? { signal: sig, mode: 'cors', cache: 'no-cache' } : { signal: sig, mode: 'cors' }).then(function (r) {
      if (!r.ok) throw new Error('gridded map ' + r.status);
      if (r.headers && r.headers.get && Number(r.headers.get('content-length') || 0) > maxBytes) throw new Error('gridded map file too large');
      return r.json();
    });
  };
  GridStore.prototype._pointer = function (sig) { return this._json(this.root + '/' + GRID_PREFIX + '/latest.json', sig, true, MAX_GRID_POINTER_BYTES); };
  // Resolves to the store when a frame is in hand, else null (never rejects).
  GridStore.prototype.load = function (force) {
    var self = this;
    if (this.loading) return this.loading;
    if (this.status === 'ok' && !this.newer && Date.now() - this.loadedAt < this.recheckMs) return Promise.resolve(this);
    if (!force && this.status === 'failed' && Date.now() - this.failedAt < this.retryMs) return Promise.resolve(null);
    var was = this.status === 'ok' && this.frame ? this.pointer : null;
    this.status = 'loading';
    var ctrl = this.loadAbort = new AbortController(), sig = ctrl.signal, timedOut = false, watchdog = null;
    var kick = function () { clearTimeout(watchdog); watchdog = setTimeout(function () { timedOut = true; ctrl.abort(); }, self.timeoutMs); };
    kick();
    var noted = this.newer;                                  // recheck() just read it: not read twice (G54-7)
    this.loading = (noted ? Promise.resolve(noted) : this._pointer(sig)).then(function (ptr) {
      kick();
      if (!validGridPointer(ptr)) throw new Error('unsupported gridded map pointer');
      if (was && was.manifest === ptr.manifest) return null;               // the step on the map is still the newest
      return self._json(self.root + '/' + ptr.manifest, sig, false, MAX_GRID_MANIFEST_BYTES).then(function (m) {
        kick();
        if (!validGridManifest(m, ptr)) throw new Error('unsupported gridded map manifest');
        var entry = m.frames[m.frames.length - 1];
        if (was && self.entry && self.frame && entry.file === self.entry.file) return { ptr: ptr, m: m, entry: self.entry, frame: self.frame };   // the same frame under a new manifest: kept, not fetched (G54-6)
        return self._decode(self.root + '/' + entry.file, sig, GRID_EXPECT, kick).then(function (frame) {
          validateGrid(m.grid, frame, SAT_FDEF);
          return { ptr: ptr, m: m, entry: entry, frame: frame };
        });
      });
    }).then(function (res) {
      clearTimeout(watchdog);
      if (self.loadAbort !== ctrl || sig.aborted) throw abortError();     // aborted or superseded: this load owns nothing any more
      if (res) { self.pointer = res.ptr; self.manifest = res.m; self.entry = res.entry; self.frame = res.frame; self.grid = res.m.grid; }
      self.newer = null; self.loadedAt = Date.now();
      self.status = 'ok'; self.loading = null; self.loadAbort = null;
      return self;
    }).catch(function () {
      clearTimeout(watchdog);
      if (self.loadAbort === ctrl) {
        self.loading = null; self.loadAbort = null;
        if (was) { self.status = 'ok'; self.loadedAt = Date.now(); }       // a failed re-read keeps the step on the map
        else { self.status = sig.aborted && !timedOut ? 'idle' : 'failed'; if (self.status === 'failed') self.failedAt = Date.now(); }
      }
      return was ? self : null;
    });
    return this.loading;
  };
  GridStore.prototype.recheck = function () {
    var self = this;
    if (this.status !== 'ok' || !this.pointer) return Promise.resolve(false);
    return this._pointer(null).then(function (ptr) {
      if (self.status === 'ok' && self.pointer && validGridPointer(ptr) && ptr.manifest !== self.pointer.manifest && ptr.run >= self.pointer.run) { self.newer = ptr; return true; }
      return false;
    }, function () { return false; });
  };
  GridStore.prototype.newestMs = function () { return this.entry ? Date.parse(this.entry.valid_utc) : null; };
  GridStore.prototype.ageH = function (now) { var t = this.newestMs(); return t === null ? null : ((now === undefined ? Date.now() : now) - t) / 3.6e6; };
  // Off / unmount: a load in progress stops; the frame in hand stays (the next On is instant, re-checked by load()).
  GridStore.prototype.abortAll = function () {
    if (this.loadAbort) { this.loadAbort.abort(); this.loadAbort = null; }
    if (this.status === 'loading') { this.status = this.frame ? 'ok' : 'idle'; this.loading = null; }
  };
  var GRID_STORES = {};
  function gridStore(root) { return GRID_STORES[root] || (GRID_STORES[root] = new GridStore(root)); }
  var SAT_LUT = null;
  function satLut() { return SAT_LUT || (SAT_LUT = buildLut('hs', SAT_FDEF.legend)); }
  var SAT_STORES = {};
  function satStore(root, kind) { var k = root + '|' + kind; return SAT_STORES[k] || (SAT_STORES[k] = new SatStore(root, kind)); }
  // The readings of one kind: the pointer (re-read every SAT_RECHECK_MS while attached: a newer one becomes `next` and is
  // adopted only once the chunks the current window needs from it have landed, so the picture never empties between
  // pointers), the chunks a window needs (nearest to the reference time first, <= MAX_SAT_INFLIGHT at once, per-chunk
  // stall timers, 404 / undecodable final, else a cooldown and a retry), and the FLAT store of every loaded chunk
  // (time-sorted, with a bucket index) the layer draws from. idle() stops the downloads and keeps the samples; abortAll()
  // (Off) stops the pointer load too and keeps them (an On is instant).
  function SatStore(root, kind) {
    this.root = root; this.kind = kind; this.prefix = kind === 'alt' ? SAT_PREFIX : SWELL_PREFIX;
    this.status = 'idle'; this.pointer = null; this.next = null; this.loading = null; this.loadAbort = null; this.failedAt = 0; this.loadedAt = 0; this.checking = null; this.checkAbort = null;
    this.chunks = new Map(); this.inflight = {}; this.queue = []; this.queued = {}; this.failed = {}; this.abort = null; this.retryTimer = null;
    this.window = null; this.flat = null; this.rev = 0; this.onChange = null; this._rebuildQ = null; this.bytes = 0;
  }
  SatStore.prototype.timeoutMs = 15000;
  SatStore.prototype.stallMs = 15000;
  SatStore.prototype.retryMs = COAST_RETRY_MS;
  SatStore.prototype.recheckMs = SAT_RECHECK_MS;
  SatStore.prototype._json = SstStore.prototype._json;
  SatStore.prototype._pointer = function (sig) { return this._json(this.root + '/' + this.prefix + '/latest.json', sig, true, 256 * 1024); };
  // The chunk's bytes as an ArrayBuffer (tests replace it); onProgress as the bytes arrive.
  SatStore.prototype._bytes = function (url, sig, onProgress) {
    return fetch(url, { signal: sig, mode: 'cors' }).then(function (r) {
      if (!r.ok) throw new Error('sat ' + r.status);
      if (Number(r.headers.get('content-length') || 0) > MAX_SAT_CHUNK_BYTES) throw new Error('sat decode failed');
      if (onProgress) onProgress();
      if (r.body && r.body.getReader) return readBody(r.body.getReader(), onProgress || function () {}, MAX_SAT_CHUNK_BYTES, 'sat decode failed');
      return r.arrayBuffer();
    });
  };
  SatStore.prototype.supported = function () { return typeof DecompressionStream === 'function' && typeof TextDecoder === 'function'; };
  // Resolves to the store when a pointer is in hand, to null otherwise (never rejects). A loaded store re-reads the
  // pointer when older than recheckMs; force (a viewer's pick, Retry, On) ignores the failure cooldown.
  SatStore.prototype.load = function (force) {
    var self = this;
    if (!this.supported()) { this.status = 'unsupported'; return Promise.resolve(null); }
    if (this.loading) return this.loading;
    if (this.status === 'ok' && Date.now() - this.loadedAt < this.recheckMs) return Promise.resolve(this);
    if (!force && this.status === 'failed' && Date.now() - this.failedAt < this.retryMs) return Promise.resolve(null);
    var was = this.pointer;
    if (!was) this.status = 'loading';
    var ctrl = this.loadAbort = new AbortController(), sig = ctrl.signal, timedOut = false, watchdog = null;
    var kick = function () { clearTimeout(watchdog); watchdog = setTimeout(function () { timedOut = true; ctrl.abort(); }, self.timeoutMs); };
    kick();
    this.loading = this._pointer(sig).then(function (ptr) {
      clearTimeout(watchdog);
      if (self.loadAbort !== ctrl || sig.aborted) throw abortError();
      if (!validSatPointer(ptr, self.kind)) throw new Error('unsupported satellite pointer');
      self._offer(ptr);
      self.loadedAt = Date.now(); self.status = 'ok'; self.loading = null; self.loadAbort = null;
      return self;
    }).catch(function () {
      clearTimeout(watchdog);
      if (self.loadAbort === ctrl) {
        self.loading = null; self.loadAbort = null;
        if (was) { self.status = 'ok'; self.loadedAt = Date.now(); }
        else { self.status = sig.aborted && !timedOut ? 'idle' : 'failed'; if (self.status === 'failed') self.failedAt = Date.now(); }
      }
      return was ? self : null;
    });
    return this.loading;
  };
  // A pointer just read: the first one is taken at once; a later one that names other chunks waits in `next` until the
  // chunks the current window needs from it are in hand (_tryAdopt); the same chunks = the same picture (taken quietly).
  SatStore.prototype._offer = function (ptr) {
    if (!this.pointer) { this.pointer = ptr; this._dropUnlisted(); if (this.window) this._queueFor(ptr, this.window); return; }   // a window set before the pointer: its chunks now
    if (satChunkKeys(ptr) === satChunkKeys(this.pointer)) { this.pointer = ptr; this.next = null; return; }
    this.next = ptr;
    this._tryAdopt();
    if (this.next && this.window) this._queueFor(this.next, this.window);
  };
  function satChunkKeys(p) { return p.chunks.map(function (c) { return c.key; }).join('\n'); }
  // Every 12 minutes while attached: a newer pointer is offered (resolves true when one arrived).
  SatStore.prototype.recheck = function () {
    var self = this;
    if (this.status !== 'ok' || this.loading) return Promise.resolve(false);
    if (this.checking) return this.checking;
    var ctrl = this.checkAbort = new AbortController(), watchdog = setTimeout(function () { ctrl.abort(); }, this.timeoutMs);   // a hung re-read never latches the 12-min check
    var p = this.checking = this._pointer(ctrl.signal).then(function (ptr) {
      if (self.checkAbort !== ctrl || self.status !== 'ok' || !validSatPointer(ptr, self.kind)) return false;
      var changed = satChunkKeys(ptr) !== satChunkKeys(self.pointer);
      self._offer(ptr); self.loadedAt = Date.now();
      return changed;
    }).catch(function () { return false; }).then(function (r) { clearTimeout(watchdog); if (self.checking === p) { self.checking = null; self.checkAbort = null; } return r; });
    return p;
  };
  // The next pointer takes over once every chunk it names for the window (not failed for good) is loaded.
  SatStore.prototype._tryAdopt = function () {
    var nx = this.next;
    if (!nx) return false;
    var need = this._chunksFor(nx, this.window);
    for (var i = 0; i < need.length; i++) if (!this.chunks.has(need[i].key) && this.failed[need[i].key] !== true) return false;
    this.pointer = nx; this.next = null;
    this._dropUnlisted();
    this._scheduleRebuild();
    return true;
  };
  // Chunks the pointer no longer names go (their keys were pruned or superseded); a download of one is dropped.
  SatStore.prototype._dropUnlisted = function () {
    var keep = {}, k;
    for (var i = 0; i < this.pointer.chunks.length; i++) keep[this.pointer.chunks[i].key] = true;
    var self = this, dropped = false;
    this.chunks.forEach(function (v, key) { if (!keep[key]) { self.chunks.delete(key); self.bytes -= v.bytes; dropped = true; } });
    for (k in this.inflight) if (!keep[k]) { this.inflight[k].abort(); delete this.inflight[k]; }
    this.queue = this.queue.filter(function (key) { return keep[key]; });
    this.queued = {}; for (i = 0; i < this.queue.length; i++) this.queued[this.queue[i]] = true;
    for (k in this.failed) if (!keep[k]) delete this.failed[k];
    if (dropped) this._scheduleRebuild();
  };
  // The pointer's chunks that overlap [t0, t1] (ms), nearest to ref first.
  SatStore.prototype._chunksFor = function (ptr, win) {
    if (!ptr || !win) return [];
    var out = [];
    for (var i = 0; i < ptr.chunks.length; i++) {
      var c = ptr.chunks[i], a = Date.parse(c.t0), b = Date.parse(c.t1);
      if (b > win.t0 && a < win.t1 && c.n > 0) out.push(c);
    }
    var ref = win.ref === undefined ? win.t1 : win.ref;
    out.sort(function (x, y) { return Math.abs((Date.parse(x.t0) + Date.parse(x.t1)) / 2 - ref) - Math.abs((Date.parse(y.t0) + Date.parse(y.t1)) / 2 - ref); });
    return out;
  };
  // The window the layer shows ({t0, t1, ref} in ms): the chunks it needs are queued (from the pointer in hand and from a
  // next one waiting), nearest the reference time first. Resolves nothing; the landings call onChange.
  SatStore.prototype.ensure = function (t0, t1, ref) {
    this.window = { t0: t0, t1: t1, ref: ref === undefined ? t1 : ref };
    if (this.status !== 'ok' || !this.pointer) return;
    this._queueFor(this.pointer, this.window);
    if (this.next) this._queueFor(this.next, this.window);
    this._tryAdopt();
  };
  SatStore.prototype._queueFor = function (ptr, win) {
    var need = this._chunksFor(ptr, win), fresh = {};
    for (var i = 0; i < need.length; i++) {
      var c = need[i], f = this.failed[c.key];
      if (this.chunks.has(c.key) || this.inflight[c.key] || f === true) continue;
      if (typeof f === 'number' && f > Date.now()) { this._armRetry(f); continue; }
      if (!this.queued[c.key]) { this.queue.push(c.key); this.queued[c.key] = true; }
      fresh[c.key] = c;
    }
    this._pump();
  };
  // One timer per store: when a cooldown ends the window's chunks are asked for again (the layer redraws on the landing).
  SatStore.prototype._armRetry = function (until) {
    var self = this;
    if (this.retryTimer && this.retryAt <= until) return;
    if (this.retryTimer) clearTimeout(this.retryTimer);                 // an earlier cooldown than the one armed: re-armed for it
    this.retryAt = until;
    this.retryTimer = setTimeout(function () {
      self.retryTimer = null;
      if (self.window && self.pointer) { self._queueFor(self.pointer, self.window); if (self.next) self._queueFor(self.next, self.window); }
    }, Math.max(0, until - Date.now()) + 50);
  };
  SatStore.prototype._record = function (key) {
    var i, p = this.pointer, nx = this.next;
    for (i = 0; p && i < p.chunks.length; i++) if (p.chunks[i].key === key) return p.chunks[i];
    for (i = 0; nx && i < nx.chunks.length; i++) if (nx.chunks[i].key === key) return nx.chunks[i];
    return null;
  };
  SatStore.prototype._pump = function () {
    var self = this;
    while (this.queue.length && Object.keys(this.inflight).length < MAX_SAT_INFLIGHT) {
      var key = this.queue.shift();
      delete this.queued[key];
      var rec = this._record(key);
      if (!rec || this.chunks.has(key) || this.inflight[key]) continue;
      (function (key, rec) {
        var ctrl = new AbortController(), timer = null;
        function mine() { return self.inflight[key] === ctrl; }
        var fail = function (kind) {
          delete self.inflight[key];
          self.failed[key] = kind === 'permanent' ? true : Date.now() + self.retryMs;
          if (self.failed[key] !== true) self._armRetry(self.failed[key]);
          self.rev++;
          self._tryAdopt();
          if (self.onChange) self.onChange();
          self._pump();
        };
        var kick = function () { clearTimeout(timer); timer = setTimeout(function () { if (!mine()) return; ctrl.abort(); fail('transient'); }, self.stallMs); };
        self.inflight[key] = ctrl;
        ctrl.signal.addEventListener('abort', function () { clearTimeout(timer); });   // idle / Off / a dropped chunk: no stall timer left behind
        kick();
        self._bytes(self.root + '/' + key, ctrl.signal, kick).then(function (buf) {
          clearTimeout(timer);
          if (ctrl.signal.aborted || !mine()) return null;
          if (rec.bytes && buf.byteLength !== rec.bytes) throw new Error('sat decode failed');          // not the object the pointer named
          return decodeSatChunk(buf, self.kind);
        }).then(function (res) {
          if (!res) return;
          clearTimeout(timer);
          if (ctrl.signal.aborted || !mine()) return;
          if (res.header.window !== rec.window || res.header.n !== rec.n) throw new Error('sat decode failed');
          delete self.inflight[key];
          self.chunks.set(key, { header: res.header, cols: res.cols, bytes: res.header.n * satStride(SAT_COLUMNS[self.kind]), rec: rec });
          self.bytes += self.chunks.get(key).bytes;
          delete self.failed[key];
          self.rev++;
          if (!self._tryAdopt()) self._scheduleRebuild();
          self._pump();
        }).catch(function (err) {
          clearTimeout(timer);
          if (!mine() || ctrl.signal.aborted || (err && err.name === 'AbortError')) return;
          var m = /^sat (\d{3})$/.exec(String(err && err.message || ''));
          fail((m && (m[1] === '404' || m[1] === '410')) || /decode/.test(String(err && err.message)) ? 'permanent' : 'transient');
        });
      })(key, rec);
    }
  };
  // One rebuild per burst of landings (a task), then onChange once.
  SatStore.prototype._scheduleRebuild = function () {
    var self = this;
    if (this._rebuildQ) return;
    this._rebuildQ = setTimeout(function () { self._rebuildQ = null; self._rebuild(); if (self.onChange) self.onChange(); }, 0);
  };
  // The flat store: every loaded chunk the pointer names, concatenated and sorted by time; the mission index mapped onto
  // the store's own mission list (the pointer's, plus any a chunk adds); `ord` = the reading's place in its chunk segment
  // (the stride thinning: deterministic, independent of the view); the bucket index over SAT_BUCKET_DEG cells.
  SatStore.prototype._rebuild = function () {
    var self = this, p = this.pointer, parts = [], total = 0, i;
    if (!p) { this.flat = null; this.rev++; return; }
    var missions = p.missions.map(function (m) { return { id: m.id, short: m.short, name: m.name || m.short }; }), mid = {};
    for (i = 0; i < missions.length; i++) mid[missions[i].id] = i;
    for (i = 0; i < p.chunks.length; i++) {
      var ch = this.chunks.get(p.chunks[i].key);
      if (ch && ch.header.n) { parts.push(ch); total += ch.header.n; }
    }
    if (total > MAX_SAT_SAMPLES) total = MAX_SAT_SAMPLES;
    var swell = this.kind === 'swell', n = total;
    var t = new Float64Array(n), lat = new Float32Array(n), lon = new Float32Array(n), mission = new Uint8Array(n), flags = new Uint8Array(n), ord = new Uint32Array(n);
    var swh = swell ? null : new Float32Array(n), model = swell ? null : new Float32Array(n), np = swell ? new Uint8Array(n) : null;
    var hs = swell ? [new Float32Array(n), new Float32Array(n), new Float32Array(n)] : null, tp = swell ? [new Float32Array(n), new Float32Array(n), new Float32Array(n)] : null, dir = swell ? [new Float32Array(n), new Float32Array(n), new Float32Array(n)] : null;
    var pos = 0;
    for (i = 0; i < parts.length && pos < n; i++) {
      var h = parts[i].header, c = parts[i].cols, map = [], k, j;
      for (k = 0; k < h.missions.length; k++) {
        var id = h.missions[k].id;
        if (mid[id] === undefined) { mid[id] = missions.length; missions.push({ id: id, short: h.missions[k].short || id, name: h.missions[k].name || id }); }
        map.push(mid[id]);
      }
      var segStart = {};
      for (k = 0; k < h.segments.length; k++) segStart[h.segments[k][1]] = h.segments[k][1];
      var start = 0;
      for (j = 0; j < h.n && pos < n; j++, pos++) {
        if (segStart[j] !== undefined) start = j;
        t[pos] = SAT_EPOCH_MS + c.t[j] * 1000; lat[pos] = c.lat[j] * 1e-6; lon[pos] = c.lon[j] * 1e-6; mission[pos] = map[c.mission[j]]; flags[pos] = c.flags[j]; ord[pos] = j - start;
        if (swell) {
          np[pos] = c.n[j];
          for (k = 0; k < 3; k++) {
            var hv = c['hs' + (k + 1)][j], pv = c['tp' + (k + 1)][j], dv = c['dir' + (k + 1)][j];
            hs[k][pos] = hv === 65535 ? NaN : hv * 0.001; tp[k][pos] = pv === 65535 ? NaN : pv * 0.01; dir[k][pos] = dv === 65535 ? NaN : dv * 0.01;
          }
        } else {
          swh[pos] = c.swh[j] === 65535 ? NaN : c.swh[j] * 0.001; model[pos] = c.model[j] === 65535 ? NaN : c.model[j] * 0.001;
        }
      }
    }
    // time order (stable) so a time slice is a contiguous index range and a bucket runs time-ascending: a typed sort on
    // packed keys (seconds from the oldest reading x 2^20 + the index, exact in a double: n <= 600k, a span < 2^33 s)
    var tmin = Infinity;
    for (i = 0; i < n; i++) if (t[i] < tmin) tmin = t[i];
    var keys = new Float64Array(n), M = 1048576;
    for (i = 0; i < n; i++) keys[i] = Math.round((t[i] - tmin) / 1000) * M + i;
    keys.sort();
    var ta = new Uint32Array(n);
    for (i = 0; i < n; i++) ta[i] = keys[i] % M;
    var pick = function (src) { var out = new src.constructor(n); for (var q = 0; q < n; q++) out[q] = src[ta[q]]; return out; };
    var flat = { n: n, t: pick(t), lat: pick(lat), lon: pick(lon), mission: pick(mission), flags: pick(flags), ord: pick(ord), missions: missions, kind: this.kind };
    if (swell) { flat.np = pick(np); flat.hs = hs.map(pick); flat.tp = tp.map(pick); flat.dir = dir.map(pick); }
    else { flat.swh = pick(swh); flat.model = pick(model); }
    flat.index = new SatIndex(flat.lat, flat.lon, n);
    this.flat = flat; this.rev++;
    self.loadedParts = parts.length;
  };
  // The flat store's index range [i0, i1) of readings with t0 <= t < t1 (binary search on the sorted times).
  SatStore.prototype.slice = function (t0, t1) {
    var f = this.flat;
    if (!f || !f.n) return [0, 0];
    return [satLowerBound(f.t, t0), satLowerBound(f.t, t1)];
  };
  function satLowerBound(arr, v) { var lo = 0, hi = arr.length; while (lo < hi) { var m = (lo + hi) >> 1; if (arr[m] < v) lo = m + 1; else hi = m; } return lo; }
  SatStore.prototype.count = function (t0, t1) { var s = this.slice(t0, t1); return s[1] - s[0]; };
  // Every chunk the pointer names for the window is in hand or failed for good (nothing more will come without a new pointer).
  SatStore.prototype.complete = function (t0, t1) {
    if (this.status !== 'ok' || !this.pointer) return false;
    var need = this._chunksFor(this.pointer, { t0: t0, t1: t1 });
    for (var i = 0; i < need.length; i++) if (!this.chunks.has(need[i].key) && this.failed[need[i].key] !== true) return false;
    return true;
  };
  SatStore.prototype.pending = function (t0, t1) {
    if (this.status !== 'ok' || !this.pointer) return 0;
    var need = this._chunksFor(this.pointer, { t0: t0, t1: t1 }), n = 0;
    for (var i = 0; i < need.length; i++) if (!this.chunks.has(need[i].key) && this.failed[need[i].key] !== true) n++;
    return n;
  };
  SatStore.prototype.newestMs = function () { return this.pointer ? satNewestMs(this.pointer) : null; };
  SatStore.prototype._stop = function () {
    for (var k in this.inflight) this.inflight[k].abort();
    this.inflight = {}; this.queue = []; this.queued = {};
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    if (this._rebuildQ) { clearTimeout(this._rebuildQ); this._rebuildQ = null; this._rebuild(); }
  };
  // Detached (another field, the box unticked): the downloads stop, the samples stay.
  SatStore.prototype.idle = function () { this._stop(); this.onChange = null; this.window = null; };
  // Off: the same, and a pointer load in progress stops (the next On re-reads it through load()).
  SatStore.prototype.abortAll = function () {
    this.idle();
    if (this.loadAbort) { this.loadAbort.abort(); this.loadAbort = null; this.loading = null; }   // a re-read too (its catch sees another owner): never a latched promise
    if (this.checkAbort) { this.checkAbort.abort(); this.checkAbort = null; this.checking = null; }
    if (this.status === 'loading') { this.status = this.pointer ? 'ok' : 'idle'; this.loading = null; }
  };
  // ---- the bucket index (CSR over SAT_BUCKET_DEG cells; each bucket time-ascending as the flat store is) ----
  function SatIndex(lat, lon, n) {
    var d = SAT_BUCKET_DEG, rows = Math.round(180 / d), cols = Math.round(360 / d), N = rows * cols;
    var cnt = new Int32Array(N + 1), bucket = new Int32Array(n), i, b;
    for (i = 0; i < n; i++) {
      var r = Math.min(rows - 1, Math.max(0, Math.floor((90 - lat[i]) / d))), c = ((Math.floor((lon[i] + 180) / d) % cols) + cols) % cols;
      b = r * cols + c; bucket[i] = b; cnt[b + 1]++;
    }
    for (b = 0; b < N; b++) cnt[b + 1] += cnt[b];
    var start = cnt, fill = new Int32Array(N), order = new Int32Array(n);
    for (i = 0; i < n; i++) { b = bucket[i]; order[start[b] + fill[b]] = i; fill[b]++; }
    this.rows = rows; this.cols = cols; this.d = d; this.start = start; this.order = order; this.n = n; this.lat = lat; this.lon = lon;
  }
  // The flat indices in the box (latS..latN, lonW..lonE; lonW may exceed lonE across the date line) within the time slice
  // [i0, i1) and the stride (ord % stride === 0), ascending (= time order): walked by buckets, or by the slice when that is
  // smaller, then sorted.
  SatIndex.prototype.collect = function (box, i0, i1, stride, ord) {
    var out = [], i;
    if (i1 <= i0) return out;
    var d = this.d, rows = this.rows, cols = this.cols;
    var r0 = Math.max(0, Math.floor((90 - box.latN) / d)), r1 = Math.min(rows - 1, Math.floor((90 - box.latS) / d));
    var c0 = Math.floor((box.lonW + 180) / d), c1 = Math.floor((box.lonE + 180) / d);
    if (box.lonE < box.lonW) c1 += cols;
    var nb = Math.max(0, r1 - r0 + 1) * Math.max(0, c1 - c0 + 1);
    if (nb * 2 < i1 - i0 && nb < 20000) {
      var lat = this.lat, lon = this.lon, wrap = box.lonE < box.lonW;
      for (var r = r0; r <= r1; r++) {
        for (var cc = c0; cc <= c1; cc++) {
          var b = r * cols + (((cc % cols) + cols) % cols), s = this.start[b], e = this.start[b + 1];
          for (i = s; i < e; i++) {
            var j = this.order[i];
            if (j < i0 || j >= i1 || (stride > 1 && ord[j] % stride !== 0)) continue;
            var la = lat[j], lo = lon[j];
            if (la < box.latS || la > box.latN) continue;
            if (wrap ? (lo < box.lonW && lo > box.lonE) : (lo < box.lonW || lo > box.lonE)) continue;
            out.push(j);
          }
        }
      }
      out.sort(function (a, b) { return a - b; });
      return out;
    }
    return null;                                             // the caller scans the slice itself (the usual case at low zoom)
  };
  // The reading under (lat, lon) at zoom z among those drawn (slice, stride): the TOP-MOST drawn one (the highest index:
  // the newest, painted last) whose mark holds the cursor (`inside(j, dx, dy)`: a dot's disc, an arrow's shaft), else the
  // nearest mark centre within rPx (the hover's slack), the newest on a tie; -1 for none.
  SatIndex.prototype.nearest = function (flat, lat, lon, z, rPx, i0, i1, stride, inside) {
    var n = TILE * Math.pow(2, z), p = forwardPixel(lat, lon, z), degPx = 360 / n, dLat = rPx * degPx * 1.2 + 1e-9, dLon = dLat / Math.max(0.05, Math.cos(lat * Math.PI / 180));
    var box = { latS: Math.max(-90, lat - dLat), latN: Math.min(90, lat + dLat), lonW: lon - dLon, lonE: lon + dLon };
    if (box.lonW < -180) box.lonW += 360; if (box.lonE > 180) box.lonE -= 360;
    var cand = this.collect(box, i0, i1, stride, flat.ord), best = -1, bd = Infinity, top = -1, i, j;
    var test = function (j) {
      var q = forwardPixel(flat.lat[j], flat.lon[j], z), dx = p.x - q.x, dy = p.y - q.y;
      if (dx > n / 2) dx -= n; else if (dx < -n / 2) dx += n;
      var dd = dx * dx + dy * dy;
      if (inside && j > top && inside(j, dx, dy)) top = j;
      if (dd <= rPx * rPx && (dd < bd || (dd === bd && j > best))) { bd = dd; best = j; }
    };
    if (cand) for (i = 0; i < cand.length; i++) test(cand[i]);
    else for (j = i0; j < i1; j++) if (stride <= 1 || flat.ord[j] % stride === 0) test(j);
    return top >= 0 ? top : best;
  };

  // ---- the Great Lakes (plan section 50) ----
  // NOAA's GLWU model gives the Great Lakes waves GFS-Wave masks: hourly frames to +149 h on their own 0.02-degree
  // grid (not periodic, one resolution), published under glwu/v1 with the SAME encoding and ranges as the model's
  // hs / tp / pdir, so one colour table, one legend and one code space serve both. Under the GFS wave height and
  // period, the lake frame of the SAME VALID TIME rides beside the step's own frames (two more frame kinds in the
  // scheduler, 'lake' and 'lakedir'); the tiles sample it where the lakes coast set (static/coast/lakes-v1, GSHHG
  // level 2 water with its islands cut out) says lake water, the global frame everywhere else. Past the lake run's
  // horizon, or without a lake run, the lakes are blank (the GFS frames hold 0 there) and the readout says so. Under
  // ECMWF only the mask applies: its own 0.25-degree lake cells show through (owner, 2026-10-21).
  var LAKE_PREFIX = 'glwu/v1';
  // the credit while a lake frame is drawn (after the model's own); the title names the winds that drive it
  var LAKE_ATTRIBUTION = ' · Lakes: <a href="https://polar.ncep.noaa.gov/waves/" target="_blank" rel="noopener" ' +
    'title="Great Lakes waves: NOAA/NCEP Great Lakes Wave Unstructured (GLWU, WAVEWATCH III) driven by the NWS forecasters\u2019 NDFD winds, rendered by Allshore Surf. Not an official NWS product.">NOAA GLWU</a>';
  var LAKE_UNDER = { hs: true, tp: true };                 // the fields the lake mask and frames apply to (never wind, SST)
  var LAKE_DIR_FIELD = 'pdir';
  var MAX_LAKE_MANIFEST_BYTES = 256 * 1024;                // a run's manifest is ~13 KB
  var MAX_LAKE_NODES = 1 << 20;                            // the published grid is 831 x 401; a frame is decoded against it
  var LAKE_BOX = { latS: 40, latN: 50, lonW: -95, lonE: -75 };   // the Great Lakes; a grid outside it is not this product
  var LAKE_FULL = null;                                    // a whole tile of lake water (one shared 255 array)
  function lakeFull() { if (!LAKE_FULL) { LAKE_FULL = new Uint8Array(TILE * TILE); LAKE_FULL.fill(255); } return LAKE_FULL; }
  // The lake grid the client can draw: centre-registered, not periodic, rows south-down, within the Great Lakes box,
  // a bounded size (the frames are decoded against it).
  function validLakeGridDef(g) {
    if (!g || typeof g !== 'object' || g.registration !== 'center' || g.lon_periodic !== false) return false;
    if (!(Number.isInteger(g.cols) && Number.isInteger(g.rows) && g.cols >= 2 && g.rows >= 2 && g.cols * g.rows <= MAX_LAKE_NODES)) return false;
    if (!(typeof g.lat0 === 'number' && typeof g.lon0 === 'number' && typeof g.dlat === 'number' && typeof g.dlon === 'number' && g.dlat < 0 && g.dlon > 0)) return false;
    var latS = g.lat0 + (g.rows - 1) * g.dlat, lonE = g.lon0 + (g.cols - 1) * g.dlon;
    return isFinite(latS) && isFinite(lonE) && g.lat0 <= LAKE_BOX.latN && latS >= LAKE_BOX.latS && g.lon0 >= LAKE_BOX.lonW && lonE <= LAKE_BOX.lonE;
  }
  // A decoded lake frame must be that grid (validateGrid's rule for the global grid).
  function validateLakeGrid(grid, frame) {
    var ok = validLakeGridDef(grid) && frame && frame.cols === grid.cols && frame.rows === grid.rows && frame.q && frame.q.length === grid.cols * grid.rows;
    if (!ok) throw new Error('lake frame does not match the lake grid');
  }
  function validLakePointer(p) {
    return !!(p && p.complete === true && p.encoding === ENCODING && typeof p.run === 'string' && /^\d{10}$/.test(p.run) && sstKey(p.manifest) && p.manifest.indexOf(LAKE_PREFIX + '/') === 0);
  }
  // A lake field the client can draw beside the model's: the same lo / hi (one code space; the manifest promises it) and
  // the full resolution; the direction field as a model's direction field.
  function lakeFieldOk(f) {
    return !!(f && typeof f.lo === 'number' && typeof f.hi === 'number' && f.lo < f.hi && Array.isArray(f.resolutions) && f.resolutions.indexOf('full') >= 0 &&
      f.legend && f.legend[0] < f.legend[1]);
  }
  function validLakeManifest(m) {
    if (!m || m.schema !== 3 || m.encoding !== ENCODING || m.complete !== true) return false;
    if (typeof m.run !== 'string' || !/^\d{10}$/.test(m.run) || typeof m.run_utc !== 'string' || isNaN(Date.parse(m.run_utc))) return false;
    if (!m.fields || typeof m.fields !== 'object' || !lakeFieldOk(m.fields.hs) || !lakeFieldOk(m.fields.tp) || !dirFieldOk(m.fields[LAKE_DIR_FIELD]) ||
      m.fields[LAKE_DIR_FIELD].resolutions.indexOf('full') < 0) return false;
    if (!validLakeGridDef(m.grid)) return false;
    var t = m.files && m.files.template, res = m.files && m.files.res;
    if (!(typeof t === 'string' && t.indexOf('{res}') >= 0 && t.indexOf('{field}') >= 0 && t.indexOf('{step:03d}') >= 0 && res && typeof res.full === 'string' &&
      sstKey(t.replace(/\{[^}]*\}/g, 'x')) && t.indexOf(LAKE_PREFIX + '/') === 0)) return false;
    if (!Array.isArray(m.frames) || !m.frames.length || m.frames.length > 512) return false;
    for (var i = 0; i < m.frames.length; i++) {
      var fr = m.frames[i], prev = i ? m.frames[i - 1] : null;
      if (!fr || !Number.isInteger(fr.step) || fr.step < 0 || typeof fr.valid_utc !== 'string' || isNaN(Date.parse(fr.valid_utc))) return false;
      if (prev && !(fr.step > prev.step && Date.parse(fr.valid_utc) > Date.parse(prev.valid_utc))) return false;
    }
    return true;
  }
  // Do the model's field and the lake's share one code space (the tiles write lake codes beside model codes)?
  function lakeFieldMatches(lf, mf) { return !!(lf && mf && lf.lo === mf.lo && lf.hi === mf.hi); }
  // The lakes' box in zoom-0 world pixels: the union of the coast set's tier-0 pieces (nothing outside it is lake water).
  function lakeBoxOf(store) {
    var t0 = store && store.tier0, box = t0 && t0.box;
    if (!box || !t0.n) return null;
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (var p = 0; p < t0.n; p++) {
      var k = p * 4;
      if (box[k] < x0) x0 = box[k]; if (box[k + 1] < y0) y0 = box[k + 1]; if (box[k + 2] > x1) x1 = box[k + 2]; if (box[k + 3] > y1) y1 = box[k + 3];
    }
    return x0 < x1 && y0 < y1 ? { x0: x0, y0: y0, x1: x1, y1: y1 } : null;
  }
  // Does a tile (any world copy) touch the box?
  function lakeBoxHits(coords, bb) {
    if (!bb) return false;
    var b = tileBox(coords);
    return !(b.x1 < bb.x0 || b.x0 > bb.x1 || b.y1 < bb.y0 || b.y0 > bb.y1);
  }
  // Are the lakes in or near the view (plan section 50, step 6b)? The lake grid's box (its node centres: the frames hold
  // nothing beyond them) against the view widened by `screens` of its own width and height on every side, in world
  // pixels at the view's zoom, any world copy. The lake frames are fetched only while this holds: off the lakes nothing
  // of them is downloaded, decoded or kept (the owner's call: a pan onto the lakes during playback then waits for its
  // first lake frames, up to the step cap).
  var LAKE_NEAR_SCREENS = 1;
  function lakeNearView(view, g, screens) {
    if (!view || !g || !(view.w > 0) || !(view.h > 0)) return false;
    var sc = Math.pow(2, view.z), n = TILE * sc, mg = screens === undefined ? LAKE_NEAR_SCREENS : screens;
    var a = worldXY(g.lon0, g.lat0), b = worldXY(g.lon0 + (g.cols - 1) * g.dlon, g.lat0 + (g.rows - 1) * g.dlat);
    var bx0 = Math.min(a[0], b[0]) * sc, bx1 = Math.max(a[0], b[0]) * sc, by0 = Math.min(a[1], b[1]) * sc, by1 = Math.max(a[1], b[1]) * sc;
    var x0 = view.ox - mg * view.w, x1 = view.ox + (1 + mg) * view.w, y0 = view.oy - mg * view.h, y1 = view.oy + (1 + mg) * view.h;
    if (by1 < y0 || by0 > y1) return false;
    for (var k = Math.floor((x0 - bx1) / n); k <= Math.ceil((x1 - bx0) / n); k++) if (bx0 + k * n <= x1 && bx1 + k * n >= x0) return true;
    return false;
  }
  // THE lake sampler: codes of the lake frame lf (grid lg) for an n x n lattice of pixel centres at world pixels
  // (x0 + ox + i * step + 0.5, y0 + oy + j * step + 0.5) of the tile coords, written to out[j * n + i] ONLY where the
  // tile's lake coverage (lake[py * TILE + px], the pixel clamped into the tile: the contour lattice reaches one step
  // outside it) is above 0; everything else is left as it is. Bilinear over the present nodes with sampleRow's absent
  // rule (w < 0.25: 0), NOT periodic (c1 = min(c0 + 1, cols - 1)); a point beyond the grid's nodes is 0. Longitudes are
  // wrapped into the one world, so world copies sample identically. The tile (0, 0, 1, TILE), the contour lattice
  // (-s, -s, s, m) and the readout (px, py, 1, 1) all read here, so the readout is the drawn pixel.
  function sampleLake(lf, lg, coords, ox, oy, step, n, out, lake) {
    var nW = TILE * Math.pow(2, coords.z), x0 = coords.x * TILE + ox, y0 = coords.y * TILE + oy, cols = lf.cols, rows = lf.rows, q = lf.q, i, j, k = 0;
    var fx = LAKE_FX.length >= n ? LAKE_FX : (LAKE_FX = new Float64Array(n)), px = LAKE_PX.length >= n ? LAKE_PX : (LAKE_PX = new Int32Array(n));
    for (i = 0; i < n; i++) {
      var lon = lngOfWorldX(x0 + i * step + 0.5, nW);
      lon = ((lon + 180) % 360 + 360) % 360 - 180;
      fx[i] = (lon - lg.lon0) / lg.dlon;
      var p = ox + i * step; px[i] = p < 0 ? 0 : p >= TILE ? TILE - 1 : p;
    }
    for (j = 0; j < n; j++, k += n) {
      var p2 = oy + j * step, py = (p2 < 0 ? 0 : p2 >= TILE ? TILE - 1 : p2) * TILE;
      var fy = (lg.lat0 - latOfWorldY(y0 + j * step + 0.5, nW)) / -lg.dlat, inRow = fy >= 0 && fy <= rows - 1;
      var r0 = Math.floor(fy), r1 = r0 + 1 < rows ? r0 + 1 : r0, fr = fy - r0, w0 = 1 - fr, b0 = r0 * cols, b1 = r1 * cols;
      for (i = 0; i < n; i++) {
        if (!lake[py + px[i]]) continue;
        var cpos = fx[i];
        if (!inRow || !(cpos >= 0 && cpos <= cols - 1)) { out[k + i] = 0; continue; }
        var c0 = Math.floor(cpos), c1 = c0 + 1 < cols ? c0 + 1 : c0, fc = cpos - c0;
        var a = q[b0 + c0], b = q[b0 + c1], c = q[b1 + c0], d = q[b1 + c1], w = 0, acc = 0, wt;
        if (a) { wt = w0 * (1 - fc); acc += a * wt; w += wt; }
        if (b) { wt = w0 * fc; acc += b * wt; w += wt; }
        if (c) { wt = fr * (1 - fc); acc += c * wt; w += wt; }
        if (d) { wt = fr * fc; acc += d * wt; w += wt; }
        out[k + i] = w < 0.25 ? 0 : acc / w;
      }
    }
    return out;
  }
  var LAKE_FX = new Float64Array(TILE), LAKE_PX = new Int32Array(TILE);
  // The lake run's pointer and manifest (never a frame: the scheduler decodes those like the model's). One per bucket
  // root per page; the manifest survives a field switch and Off (abortAll only stops a load in progress). A newer run is
  // NOTED by recheck() and fetched into `next`; the scheduler adopts it between landings (adoptNewer), never under a
  // frame in flight, so no step mixes two lake runs.
  var LAKE_STORES = {};
  function lakeStore(root) { return LAKE_STORES[root] || (LAKE_STORES[root] = new LakeStore(root)); }
  function LakeStore(root) {
    this.root = root; this.status = 'idle'; this.pointer = null; this.manifest = null; this.grid = null; this.byValid = new Map();
    this.loading = null; this.loadAbort = null; this.failedAt = 0; this.loadedAt = 0; this.next = null; this.checking = null;
  }
  LakeStore.prototype.timeoutMs = 15000;                   // a pointer / manifest step silent this long fails the load (= LOAD_STALL_MS, defined later)
  LakeStore.prototype.retryMs = COAST_RETRY_MS;
  LakeStore.prototype.recheckMs = POINTER_RECHECK_MS;
  LakeStore.prototype._json = SstStore.prototype._json;
  LakeStore.prototype._pointer = function (sig) { return this._json(this.root + '/' + LAKE_PREFIX + '/latest.json', sig, true, 64 * 1024); };
  LakeStore.prototype._manifest = function (ptr, sig) {
    var self = this;
    return this._json(this.root + '/' + ptr.manifest, sig, false, MAX_LAKE_MANIFEST_BYTES).then(function (m) {
      if (!validLakeManifest(m) || m.run !== ptr.run) throw new Error('unsupported lake manifest');
      return m;
    });
  };
  LakeStore.prototype._take = function (ptr, m) {
    this.pointer = ptr; this.manifest = m; this.grid = m.grid; this.next = null;
    var by = this.byValid = new Map();
    for (var i = 0; i < m.frames.length; i++) by.set(Date.parse(m.frames[i].valid_utc), i);
  };
  // Resolves to the store when a run is in hand, to null otherwise (never rejects). A store already loaded re-reads the
  // pointer when it is older than recheckMs (the same run keeps everything; a newer run is taken at once when nothing
  // is drawn from the old one: the caller mounts afresh); force: a viewer's action tries again at once.
  LakeStore.prototype.load = function (force) {
    var self = this;
    if (this.loading) return this.loading;
    if (this.status === 'ok' && !this.next && Date.now() - this.loadedAt < this.recheckMs) return Promise.resolve(this);
    if (!force && this.status === 'failed' && Date.now() - this.failedAt < this.retryMs) return Promise.resolve(null);
    var was = this.status === 'ok' ? this.manifest : null;
    if (!was) this.status = 'loading';                     // a re-read keeps the run in hand usable (status 'ok') until it answers (G33 F3)
    var ctrl = this.loadAbort = new AbortController(), sig = ctrl.signal, timedOut = false, watchdog = null;
    var kick = function () { clearTimeout(watchdog); watchdog = setTimeout(function () { timedOut = true; ctrl.abort(); }, self.timeoutMs); };
    kick();
    this.loading = this._pointer(sig).then(function (ptr) {
      kick();
      if (!validLakePointer(ptr)) throw new Error('unsupported lake pointer');
      if (was && !(ptr.run > self.manifest.run)) return null;   // the same run, an older one (G33 N1), or the run adopted while this read was out: keep the run in hand (G33 re-check P3-1)
      if (self.next && self.next.m.run === ptr.run) return self.next;
      return self._manifest(ptr, sig).then(function (m) { return { ptr: ptr, m: m }; });
    }).then(function (res) {
      clearTimeout(watchdog);
      if (self.loadAbort !== ctrl || sig.aborted) throw abortError();
      if (res && was && !(res.m.run > self.manifest.run)) res = null;           // adopted meanwhile (the run check's `next`, taken by a landing): never in `next` again (P3-1)
      if (res) { if (was) self.next = res; else self._take(res.ptr, res.m); }   // a re-read's newer run waits in `next` for the scheduler (_adoptLake: its one path, G33)
      self.loadedAt = Date.now();
      self.status = 'ok'; self.loading = null; self.loadAbort = null;
      return self;
    }).catch(function () {
      clearTimeout(watchdog);
      if (self.loadAbort === ctrl) {
        self.loading = null; self.loadAbort = null;
        if (was) { self.status = 'ok'; self.loadedAt = Date.now(); }
        else { self.status = sig.aborted && !timedOut ? 'idle' : 'failed'; if (self.status === 'failed') self.failedAt = Date.now(); }
      }
      return was ? self : null;
    });
    return this.loading;
  };
  // While a run is in hand: a newer run's manifest is fetched into `next` (resolves true), never swapped in here.
  LakeStore.prototype.recheck = function () {
    var self = this;
    if (this.status !== 'ok' || this.loading) return Promise.resolve(false);
    if (this.checking) return this.checking;
    this.checking = this._pointer(null).then(function (ptr) {
      if (self.status !== 'ok' || !validLakePointer(ptr) || !(ptr.run > self.manifest.run)) return false;
      if (self.next && self.next.m.run === ptr.run) return true;
      return self._manifest(ptr, null).then(function (m) {
        if (self.status === 'ok' && self.manifest && m.run > self.manifest.run) { self.next = { ptr: ptr, m: m }; return true; }
        return false;
      });
    }).catch(function () { return false; }).then(function (r) { self.checking = null; return r; });
    return this.checking;
  };
  // The newer run takes over (the scheduler calls this with no lake frame in flight and clears its lake caches).
  LakeStore.prototype.adoptNewer = function () {
    if (!this.next) return false;
    if (this.manifest && !(this.next.m.run > this.manifest.run)) { this.next = null; return false; }   // never the run in hand again (G33 re-check P3-1)
    this._take(this.next.ptr, this.next.m);
    this.loadedAt = Date.now();
    return true;
  };
  // The lake frame index of a valid time (ms), or -1 (the hourly lake run covers every GFS time to +120 h and the
  // 3-hourly ones to +147 h exactly; nothing past +149 h).
  LakeStore.prototype.indexOf = function (validMs) { var i = this.byValid.get(validMs); return i === undefined ? -1 : i; };
  // Off: a load in progress stops; the run in hand stays for the next On.
  LakeStore.prototype.abortAll = function () {
    if (this.loadAbort) { this.loadAbort.abort(); this.loadAbort = null; }
    this.loading = null;                                   // (a re-read with a run in hand keeps status 'ok' while it runs)
    if (this.status === 'loading') this.status = this.manifest ? 'ok' : 'idle';
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
  // onProgress (every frame download of _ensure, the SST store): called when the answer starts and as its bytes arrive.
  function decodeFrame(url, signal, expect, onBody, mainOnly, onProgress) {
    var direct = typeof DecompressionStream === 'function' && typeof Response === 'function';
    return fetch(url, { signal: signal, mode: 'cors' }).then(function (r) {
      if (!r.ok) throw new Error('frame ' + r.status);
      var len = Number(r.headers.get('content-length') || 0);
      if (len > MAX_FRAME_BYTES) throw new Error('frame decode failed');
      if (onProgress) onProgress();
      if (onProgress && r.body && r.body.getReader) {
        return readBody(r.body.getReader(), onProgress).then(function (buf) { return direct ? buf : new Blob([buf]); });   // (RC-3)
      }
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
  // A response body read chunk by chunk (each chunk reports progress), as one ArrayBuffer; capped like a whole body (a
  // frame's cap and message unless given).
  function readBody(reader, onProgress, limit, msg) {
    var parts = [], size = 0, cap = limit || MAX_FRAME_BYTES;
    function next() {
      return reader.read().then(function (c) {
        if (c.done) {
          var out = new Uint8Array(size), at = 0;
          for (var i = 0; i < parts.length; i++) { out.set(parts[i], at); at += parts[i].length; }
          return out.buffer;
        }
        size += c.value.length;
        if (size > cap) {
          try { var c = reader.cancel(); if (c && c.catch) c.catch(function () {}); } catch (e) { /* gone */ }   // (RC-6)
          throw new Error(msg || 'frame decode failed');
        }
        parts.push(c.value); onProgress();
        return next();
      });
    }
    return next();
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
      this._lake = null; this._lakeCoast = null; this._lakeMask = false; this._lakeBox = null; this._lq = null;   // plan section 50: the Great Lakes
      this._lakeOut = null;                                // plan section 51: the lake-outline set (lines under OUTLINE_FIELDS)
      this._sc = new Float64Array(TILE / 2 + 2); this._sr = new Float64Array(TILE / 2 + 2); this._srow = new Float64Array(TILE / 2 + 2);
      this._pi = new Int32Array(TILE + 2); this._pt = new Float64Array(TILE + 2);
      this._gen = 0; this._staleQ = null;                   // plan section 42: which frame a tile shows (el._ovGen)
    },
    // cfg {step: interval in display units, per: SI -> display factor} or null (no contours; the smoothed
    // copy of the frame is dropped). quiet: only set it, for the draw that follows.
    setContours: function (cfg, quiet) { this._contour = cfg; if (!cfg) this._sm = null; if (!quiet) this._redraw(); },
    // frame {q, cols, rows}; grid = the manifest grid for that resolution; fdef = the manifest field
    // lake (plan section 50): the step's lake frame {frame, grid, entry, lo, hi}, drawn in the same redraw, or none.
    setFrame: function (frame, grid, fieldName, fdef, lut, entry, lake) {
      validateGrid(grid, frame, fdef);
      if (lake) { validateLakeGrid(lake.grid, lake.frame); if (lake.lo !== fdef.lo || lake.hi !== fdef.hi) throw new Error('lake frame is not in the field\u2019s code space'); }
      this._frame = frame; this._grid = grid; this._lut = lut; this.field = fieldName; this.fdef = fdef; this.entry = entry || null;
      this._nearest = fdef.interpolation === 'nearest'; this._lo = fdef.lo; this._hi = fdef.hi; this._legend = fdef.legend;
      this._clip = !!(this._coast && CLIP_FIELDS[fieldName]);
      this._lakeMask = !!(this._lakeCoast && LAKE_UNDER[fieldName]);
      this._lake = lake || null;
      this._gen++;
      this._redraw(true);
    },
    clear: function () { this._frame = null; this.field = null; this.fdef = null; this.entry = null; this._clip = false; this._lakeMask = false; this._lake = null; this._sm = null; this.setSource(null, true); this._redraw(); },
    // The lake frame drawn under the frame on the map (plan section 50): data {frame, grid, entry, lo, hi} or null. It
    // must share the field's code space (lo / hi; the scheduler checks the manifests, this is the last line). quiet: only
    // set it, for the setFrame that follows (one redraw); else the tiles with lake water are redrawn and the particles told.
    // Returns whether anything changed.
    setLake: function (data, quiet) {
      var cur = this._lake;
      if (data) {
        validateLakeGrid(data.grid, data.frame);
        if (this.fdef && (data.lo !== this.fdef.lo || data.hi !== this.fdef.hi)) throw new Error('lake frame is not in the field\u2019s code space');
        if (cur && cur.frame === data.frame && cur.entry === data.entry) return false;
      } else if (!cur) return false;
      this._lake = data || null;
      if (!quiet) this._redrawLake(false);
      return true;
    },
    hasLake: function () { return !!(this._lake && this._frame); },
    // The lakes coast set (or null): from now on the tiles of the lake box know their lake water; LAKE_UNDER fields show
    // the lake frame (or the model's own lake cells) there.
    setLakeCoast: function (store) {
      var self = this, prev = this._lakeCoast;
      if (prev && prev.onChange === this._onLakeCoast) { prev.onChange = null; prev.onNeeded = null; }
      this._lakeCoast = store && store.status === 'ok' ? store : null;
      this._lakeBox = this._lakeCoast ? lakeBoxOf(this._lakeCoast) : null;
      this._lakeMask = !!(this._lakeCoast && LAKE_UNDER[this.field]);
      if (this._lakeCoast) {
        this._onLakeCoast = function () { self._redrawLakeIncomplete(); };
        this._lakeCoast.onChange = this._onLakeCoast;
        this._lakeCoast.onNeeded = function () { return self._lakeCellsNeeded(); };
      }
      this._redrawLake(true);
    },
    // The tile's lake water coverage (0..255 per pixel; null = none; a whole tile of water = the shared 255 array), computed
    // once per tile element like the land mask, from the lakes coast set; null outside the lakes' box without asking it.
    _lakeFor: function (el, coords) {
      var st = this._lakeCoast;
      if (!st || !lakeBoxHits(coords, this._lakeBox)) { el._ovLake = null; el._ovLakeKey = null; return null; }
      var key = coords.z + '/' + tileBox(coords).xw + '/' + coords.y;
      if (el._ovLakeKey === key && (el._ovLakeFinal || el._ovLakeRev === st.rev)) return el._ovLake;
      var got = st.setsFor(coords);
      if (!got.complete && el._ovLakeKey === key && el._ovLakeFinal === false) { el._ovLakeRev = st.rev; return el._ovLake; }
      var m = got.sets.length ? maskState(rasterise(landPathsForTile(coords, got.sets), TILE)) : null;
      el._ovLake = m === LAND_ALL ? lakeFull() : m;
      el._ovLakeKey = key; el._ovLakeFinal = got.complete; el._ovLakeRev = st.rev;
      return el._ovLake;
    },
    // Lake water (coverage > 0) in the viewer's view (the credit, G33 re-check P3-5): the tiles at the tile zoom, a tile
    // wholly in view counting by its mask (a mask is never all zero) and a tile at the view's edge by its pixels in view.
    // Off a map (the Node stubs' pixel bounds missing) every tile with coverage counts.
    lakeWaterInView: function (onMap) {
      var map = onMap || this._map, pb = null, scale = 1, tz = this._tileZoom;
      if (map && typeof map.getPixelBounds === 'function' && typeof map.getZoom === 'function') {
        var z = map.getZoom();
        if (typeof tz !== 'number') tz = Math.round(z);
        scale = typeof map.getZoomScale === 'function' ? map.getZoomScale(z, tz) : Math.pow(2, z - tz); pb = map.getPixelBounds();
      }
      for (var k in this._tiles) {
        var t = this._tiles[k], lk = t && t.el ? t.el._ovLake : null;
        if (!lk || !t.coords || (typeof tz === 'number' && t.coords.z !== tz)) continue;
        if (!pb) return true;
        var ox = t.coords.x * TILE, oy = t.coords.y * TILE;
        var x0 = Math.max(0, Math.floor(pb.min.x / scale) - ox), x1 = Math.min(TILE, Math.ceil(pb.max.x / scale) - ox);
        var y0 = Math.max(0, Math.floor(pb.min.y / scale) - oy), y1 = Math.min(TILE, Math.ceil(pb.max.y / scale) - oy);
        if (x0 >= x1 || y0 >= y1) continue;
        if (x0 === 0 && y0 === 0 && x1 === TILE && y1 === TILE) return true;
        for (var y = y0; y < y1; y++) { var row = y * TILE; for (var x = x0; x < x1; x++) if (lk[row + x]) return true; }
      }
      return false;
    },
    // Redraw the tiles that show lake water (all: every tile in the lakes' box, for a coast set that just arrived).
    _redrawLake: function (all) {
      if (!this._frame) return;
      var drawn = false;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords) continue;
        if (all ? !lakeBoxHits(t.coords, this._lakeBox) : !t.el._ovLake) continue;
        this._draw(t.el, t.coords); drawn = true;
      }
      if (drawn && this.onRedraw) this.onRedraw();
    },
    _redrawLakeIncomplete: function () {
      if (!this._lakeCoast || !this._lakeMask || !this._frame) return;
      var changed = false;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords || t.el._ovLakeFinal !== false) continue;
        var before = t.el._ovLake, after = this._lakeFor(t.el, t.coords);
        if (after !== before) { this._draw(t.el, t.coords); changed = true; }
      }
      if (changed && this.onRedraw) this.onRedraw();
    },
    _lakeCellsNeeded: function () {
      var st = this._lakeCoast, need = {};
      if (!st || !this._lakeMask) return need;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.coords || !lakeBoxHits(t.coords, this._lakeBox) || !st.tier1Zoom(t.coords.z)) continue;
        var names = coastCellsForTile(t.coords, st.index.tier1.cell);
        if (names.length > MAX_CELLS_PER_TILE) continue;
        for (var i = 0; i < names.length; i++) need[names[i]] = true;
      }
      return need;
    },
    // Lake water under the drawn pixel that contains (lat, lng) at tile zoom z (the readout: "no data" where readoutAt
    // is null there); false off the lakes, off the map, or under a field without the lake mask.
    lakeAt: function (lat, lng, z) {
      if (!this._lakeMask || !this._frame) return false;
      var p = pixelOf(lat, lng, z), t = this._tiles[p.x + ':' + p.y + ':' + p.z], lk = t && t.el ? t.el._ovLake : null;
      return !!(lk && lk[p.py * TILE + p.px] >= LAND_READOUT);
    },
    // Another lake frame's sample (a code, 0 = none) at the drawn pixel that contains (lat, lng), sampled exactly as the
    // tile samples its lake frame (the hover's lake height under peak period, G33 B-2); 0 off the lake water.
    lakeCodeAt: function (frame, grid, lat, lng, z) {
      if (!this._lakeMask || !this._frame) return 0;
      var p = pixelOf(lat, lng, z), t = this._tiles[p.x + ':' + p.y + ':' + p.z], lake = t && t.el ? t.el._ovLake : null;
      if (!lake || !lake[p.py * TILE + p.px]) return 0;
      this._oneOut[0] = 0;
      sampleLake(frame, grid, p, p.px, p.py, 1, 1, this._oneOut, lake);
      return this._oneOut[0];
    },
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
        if (!el || !t.coords || this._allLand(el)) continue;
        if (st.tierFor(t.coords) !== el._ovSrcTier || st.landedAfter(t.coords, el._ovSrcTier, el._ovSrcRev)) this._draw(el, t.coords);
      }
    },
    _srcNeeded: function () {
      var st = this._src, need = {};
      if (!st) return need;
      for (var k in this._tiles) { var t = this._tiles[k]; if (t.coords && !(t.el && this._allLand(t.el))) st.keysFor(t.coords, need); }
      return need;
    },
    // A tile the coastline mask shows as land only (as last drawn): the source has nothing to give it.
    _allLand: function (el) { return !!(this._clip && el._ovLand === LAND_ALL && el._ovLandKey); },
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
    // The lake-outline set (plan section 51), or null: from now on the tiles of an outlined field (wind) carry the lakes'
    // shores as lines, drawn like the coastline. Without it the tiles that had lines are redrawn without them.
    setLakeOutlineCoast: function (store) {
      var self = this, prev = this._lakeOut;
      if (prev && prev.onChange === this._onLakeOut) { prev.onChange = null; prev.onNeeded = null; }
      this._lakeOut = store && store.status === 'ok' ? store : null;
      if (this._lakeOut) {
        this._onLakeOut = function () { self._redrawLakeOutIncomplete(); };
        this._lakeOut.onChange = this._onLakeOut;
        this._lakeOut.onNeeded = function () { return self._lakeOutCellsNeeded(); };
      }
      var drawn = !!(this._frame && OUTLINE_FIELDS[this.field]);
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords) continue;
        if (!this._lakeOut) {                                  // detached: every tile lets its mask go (V7-5), whatever field is shown
          var had = !!t.el._ovLo;
          t.el._ovLo = null; t.el._ovLoKey = null; t.el._ovLoEdges = null; t.el._ovLoEdgesOf = null;   // the edge cache holds the mask too (G34 F-A3)
          if (had && drawn) this._draw(t.el, t.coords);
          continue;
        }
        if (!drawn || t.coords.z < LAKE_OUTLINE_MIN_ZOOM) continue;
        t.el._ovLoKey = null;
        this._draw(t.el, t.coords);
      }
    },
    // The tile's lake water (0..255 per pixel) from the lake-outline set: null below LAKE_OUTLINE_MIN_ZOOM and where no lake
    // piece touches the tile (nothing rasterised), LAND_ALL inside a lake (no shore to draw); computed once per tile element.
    _lakeOutFor: function (el, coords) {
      var st = this._lakeOut;
      if (!st || coords.z < LAKE_OUTLINE_MIN_ZOOM) { el._ovLo = null; el._ovLoKey = null; return null; }
      var key = coords.z + '/' + tileBox(coords).xw + '/' + coords.y;
      if (el._ovLoKey === key && (el._ovLoFinal || el._ovLoRev === st.rev)) return el._ovLo;
      var got = st.setsFor(coords);
      if (!got.complete && el._ovLoKey === key && el._ovLoFinal === false) { el._ovLoRev = st.rev; return el._ovLo; }
      var paths = got.sets.length ? landPathsForTile(coords, got.sets) : [];
      el._ovLo = paths.length ? maskState(rasterise(paths, TILE)) : null;
      el._ovLoKey = key; el._ovLoFinal = got.complete; el._ovLoRev = st.rev;
      return el._ovLo;
    },
    // A tier-1 lake chunk landed (or failed): the tiles drawn from the tier-0 stand-in are redrawn when their mask changed.
    _redrawLakeOutIncomplete: function () {
      if (!this._lakeOut || !this._frame || !OUTLINE_FIELDS[this.field]) return;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords || t.el._ovLoFinal !== false) continue;
        var before = t.el._ovLo, after = this._lakeOutFor(t.el, t.coords);
        if (after !== before) this._draw(t.el, t.coords);
      }
    },
    _lakeOutCellsNeeded: function () {
      var st = this._lakeOut, need = {};
      if (!st || !OUTLINE_FIELDS[this.field]) return need;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.coords || t.coords.z < LAKE_OUTLINE_MIN_ZOOM || !st.tier1Zoom(t.coords.z)) continue;
        var names = coastCellsForTile(t.coords, st.index.tier1.cell);
        if (names.length > MAX_CELLS_PER_TILE) continue;
        for (var i = 0; i < names.length; i++) need[names[i]] = true;
      }
      return need;
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
    _viewRange: function () { return viewRangeOf(this); },
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
      var lake = this._lakeMask && t && t.el ? t.el._ovLake : null, lk = lake ? lake[p.py * TILE + p.px] : 0, i = p.py * TILE + p.px;
      if (this._src) {                                       // the tile's own tier, the pixel's own sample
        if (!this._frame || !t || !t.el || t.el._ovSrcTier === undefined) return null;
        this._src.sampleGrid(p, p.px, p.py, 1, 1, this._oneOut, t.el._ovSrcTier);
        v = this._oneOut[0] ? this._value(this._oneOut[0]) : null;
      } else if (this._lake && lk) {                         // the lake frame's sample, as the tile drew it (plan section 50)
        this._oneOut[0] = 0;
        sampleLake(this._lake.frame, this._lake.grid, p, p.px, p.py, 1, 1, this._oneOut, lake);
        v = this._oneOut[0] ? this._value(this._oneOut[0]) : null;
      } else {
        var ll = tilePixelLatLng(p, p.px, p.py);
        v = this.valueAt(ll.lat, ll.lng);
      }
      if (v === null || !this._clip) return v;
      var land = t && t.el ? t.el._ovLand : undefined;
      if (land === undefined) return null;
      var a = land === LAND_ALL ? 0 : land === null ? 255 : 255 - land[i];
      if (lk > a) a = lk;
      return a >= LAND_READOUT ? v : null;
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
    // lake (plan section 50): the tile's lake water coverage; the lake frame's codes replace the model's there.
    tileCodes: function (coords, out, tier, lake) {
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
      if (lake && this._lake) sampleLake(this._lake.frame, this._lake.grid, coords, 0, 0, 1, TILE, out, lake);
      return out;
    },
    _draw: function (el, coords) {
      var ctx = el.getContext('2d');
      el._ovGen = this._gen;
      if (!this._frame) { ctx.clearRect(0, 0, TILE, TILE); return; }
      var land = this._masked() ? this._landFor(el, coords) : null;
      var lake = this._lakeMask ? this._lakeFor(el, coords) : null;     // first: the land set counts the Great Lakes as land (step 6)
      var tier = 0;
      if (this._clip && land === LAND_ALL && !lake) {        // nothing but land and no lake water: no sampling at all, no cells asked for (G31-6)
        if (this._src) { el._ovSrcTier = 0; el._ovSrcRev = this._src.rev; }
        ctx.clearRect(0, 0, TILE, TILE); return;
      }
      if (this._src) { tier = this._src.tierFor(coords); el._ovSrcTier = tier; el._ovSrcRev = this._src.rev; }
      var codes = this.tileCodes(coords, this._codes, tier, lake);
      var img = el._ovImg || (el._ovImg = ctx.createImageData(TILE, TILE));     // reused per tile: no 256 KB per frame
      composeTile(codes, this._clip ? land : null, this._lut, this._lo, this._hi, this._legend[0], this._legend[1], img.data, lake);
      if (this._contour && !this._nearest && CONTOURS[this.field]) this._contours(coords, img.data, false, tier, lake);
      if (!this._clip && land && land !== LAND_ALL) {                          // the coastline under an unclipped field
        if (el._ovEdgesOf !== land) { el._ovEdges = coastEdges(land, WIND_LOOK.gain); el._ovEdgesOf = land; }
        applyEdges(el._ovEdges, img.data, WIND_LOOK.ink);
      }
      if (this._lakeOut && OUTLINE_FIELDS[this.field]) {                    // the lakes' shores, the same line (plan section 51)
        var lo = this._lakeOutFor(el, coords);
        if (lo && lo !== LAND_ALL) {
          if (el._ovLoEdgesOf !== lo) { el._ovLoEdges = coastEdges(lo, WIND_LOOK.gain); el._ovLoEdgesOf = lo; }
          applyEdges(el._ovLoEdges, img.data, WIND_LOOK.ink);
        }
      }
      ctx.putImageData(img, 0, 0);
    },
    // Contours on the smoothed copy of the frame (smoothBlock), sampled at world pixels every s px (the
    // same pixels for every tile, so neighbouring tiles agree on their shared edges and world copies
    // match) and interpolated in between; a 16-px block is drawn only if its samples span a whole level.
    // all: draw every block (the tests compare it with the skipping).
    _contours: function (coords, rgba, all, tier, lake) {
      var cfg = this._contour, fr = this._frame, g = this._grid, cols = fr.cols, rows = fr.rows, i, j;
      var step = cfg.step * (coords.z < (cfg.coarseBelow || 4) ? 2 : 1), k = cfg.per / step, koff = cfg.off ? cfg.off / step : 0, lo = this._lo, span = (this._hi - lo) / 254;
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
      if (lake && this._lake) {                              // the lake frame's values on the lattice where the tile shows lake water, unsmoothed
        var LQ = this._lq || (this._lq = new Float64Array((TILE / 2 + 2) * (TILE / 2 + 2)));
        LQ.fill(-1, 0, m * m);
        sampleLake(this._lake.frame, this._lake.grid, coords, -s, -s, s, m, LQ, lake);
        for (j = 0; j < m * m; j++) if (LQ[j] >= 0) V[j] = LQ[j] ? (lo + (LQ[j] - 1) * span) * k + koff : NaN;
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
          contourTile(F, TILE, rgba, 3, ink, bx, by, bx + B, by + B, core, edge, !!cfg.zero);
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

  // The tiles a grid layer's viewer sees (ModelGridLayer and SatLayer share it): Leaflet's pixel bounds at the map zoom,
  // scaled to the tile zoom; null off a map (every tile is drawn at once then: the Node stubs).
  function viewRangeOf(layer) {
    var map = layer._map;
    if (!map || typeof map.getPixelBounds !== 'function') return null;
    var z = map.getZoom(), tz = typeof layer._tileZoom === 'number' ? layer._tileZoom : Math.round(z);
    var scale = typeof map.getZoomScale === 'function' ? map.getZoomScale(z, tz) : Math.pow(2, z - tz), pb = map.getPixelBounds();
    return { z: tz, x0: Math.floor(pb.min.x / scale / TILE), x1: Math.floor((pb.max.x / scale - 1) / TILE),
      y0: Math.floor(pb.min.y / scale / TILE), y1: Math.floor((pb.max.y / scale - 1) / TILE) };
  }

  // ---- the satellite layer (plan sections 45 and 81): height dots and swell arrows per tile ----
  // Thinning by tile zoom: every 2^(7 - z)-th reading of a chunk segment (1 Hz readings are ~7 km apart: at zoom 2 a
  // 32nd is ~220 km, about 3 px), everything from zoom 7.
  function satStrideFor(z) { var zz = Math.max(0, Math.floor(z)); return zz >= 7 ? 1 : Math.pow(2, 7 - zz); }
  // A dot's radius in px by tile zoom: 1.5 at the world view to 3 close in.
  function satRadius(z) { return z <= 2 ? 1.5 : z >= 8 ? 3 : 1.5 + (z - 2) * 0.25; }
  // A dot's alpha by its age fraction (0 = the newest, 1 = the window's far end): SAT_FADE[0] -> SAT_FADE[1].
  function satAlpha(frac) { var f = frac < 0 ? 0 : frac > 1 ? 1 : frac; return SAT_FADE[0] + (SAT_FADE[1] - SAT_FADE[0]) * f; }
  // The LUT index of a value over the legend range: composeTile's expression, so a dot is coloured exactly as the field
  // would draw that value (the comparison's premise: the same colour = agreement).
  function satColourIndex(v, L0, L1) { var t = Math.round((v - L0) * 255 / (L1 - L0)); return t < 0 ? 0 : t > 255 ? 255 : t; }
  // A swell arrow's length in px from its peak period: 8 s -> 13, 14 s -> 19, 20 s -> 26 (capped 6..26).
  function satArrowLength(tp) { var l = 4 + tp * 1.1; return l < 6 ? 6 : l > 26 ? 26 : l; }
  // The arrow's head and tail half-widths.
  var SAT_ARROW_HEAD = 4, SAT_ARROW_W = 1.6;
  // The readings' positions in tile pixels for a tile (coords, any world copy): x at the copy nearest the tile.
  function satTileXY(flat, j, coords, n, ox, oy) {
    var p = forwardPixel(flat.lat[j], flat.lon[j], coords.z), x = p.x - ox;
    if (x < -n / 2) x += n; else if (x > n / 2) x -= n;
    return { x: x, y: p.y - oy };
  }
  // The readings to draw on a tile: the time slice [i0, i1) of the window, thinned by stride, inside the tile's box widened
  // by `padPx` (a dot's radius, an arrow's length), in index order (= time order: the newest drawn last, on top).
  function satTileReadings(flat, win, coords, stride, padPx) {
    var n = TILE * Math.pow(2, coords.z), ox = coords.x * TILE, oy = coords.y * TILE;
    var latN = Math.min(90, latOfWorldY(Math.max(0, oy - padPx), n)), latS = Math.max(-90, latOfWorldY(Math.min(n, oy + TILE + padPx), n));
    var lonW = ((lngOfWorldX(ox - padPx, n) + 180) % 360 + 360) % 360 - 180, lonE = ((lngOfWorldX(ox + TILE + padPx, n) + 180) % 360 + 360) % 360 - 180;
    if (TILE + 2 * padPx >= n) { lonW = -180; lonE = 180; }
    var box = { latS: latS, latN: latN, lonW: lonW, lonE: lonE }, out = flat.index.collect(box, win.i0, win.i1, stride, flat.ord);
    if (out) return out;
    out = [];
    var wrap = lonE < lonW;
    for (var j = win.i0; j < win.i1; j++) {
      if (stride > 1 && flat.ord[j] % stride !== 0) continue;
      var la = flat.lat[j], lo = flat.lon[j];
      if (la < latS || la > latN) continue;
      if (wrap ? (lo < lonW && lo > lonE) : (lo < lonW || lo > lonE)) continue;
      out.push(j);
    }
    return out;
  }
  // Draws the readings `ids` of the flat store onto a tile context: height dots (mode 'dots': the colour of the reading's
  // wave height on the hs LUT over the legend, alpha by age, hollow when near land) or swell arrows (mode 'arrows': the
  // dominant partition's height for the colour, its peak period for the length, pointing where the swell GOES: FROM + 180).
  // -> the number drawn. Pure canvas calls (tests count them through a fake context).
  function composeDots(ctx, flat, ids, coords, win, lut, legend, mode, z) {
    var n = TILE * Math.pow(2, coords.z), ox = coords.x * TILE, oy = coords.y * TILE, L0 = legend[0], L1 = legend[1], r = satRadius(z), drawn = 0;
    for (var k = 0; k < ids.length; k++) {
      var j = ids[k], v = mode === 'arrows' ? flat.hs[0][j] : flat.swh[j];
      if (!(v === v)) continue;
      var p = satTileXY(flat, j, coords, n, ox, oy);
      var frac = (win.ref - flat.t[j]) / win.span, a = satAlpha(frac);
      var ci = satColourIndex(v, L0, L1), col = 'rgb(' + lut[ci * 3] + ',' + lut[ci * 3 + 1] + ',' + lut[ci * 3 + 2] + ')';
      ctx.globalAlpha = a;
      if (mode === 'arrows') {
        var tp = flat.tp[0][j], d = flat.dir[0][j];
        if (!(tp === tp) || !(d === d)) continue;
        var len = satArrowLength(tp), ang = (d + 180) * Math.PI / 180, dx = Math.sin(ang), dy = -Math.cos(ang);
        var x0 = p.x - dx * len / 2, y0 = p.y - dy * len / 2, x1 = p.x + dx * len / 2, y1 = p.y + dy * len / 2;
        ctx.lineCap = 'round'; ctx.lineJoin = 'round';
        ctx.strokeStyle = 'rgba(0,0,0,0.65)'; ctx.lineWidth = SAT_ARROW_W + 2;
        ctx.beginPath(); ctx.moveTo(x0, y0); ctx.lineTo(x1, y1);
        ctx.moveTo(x1 - dx * SAT_ARROW_HEAD - dy * SAT_ARROW_HEAD * 0.6, y1 - dy * SAT_ARROW_HEAD + dx * SAT_ARROW_HEAD * 0.6); ctx.lineTo(x1, y1);
        ctx.lineTo(x1 - dx * SAT_ARROW_HEAD + dy * SAT_ARROW_HEAD * 0.6, y1 - dy * SAT_ARROW_HEAD - dx * SAT_ARROW_HEAD * 0.6);
        ctx.stroke();
        ctx.strokeStyle = col; ctx.lineWidth = SAT_ARROW_W; ctx.stroke();
      } else {
        var hollow = (flat.flags[j] & SAT_FLAG_NEAR_LAND) !== 0;
        ctx.beginPath(); ctx.arc(p.x, p.y, r, 0, 2 * Math.PI);
        if (hollow) { ctx.strokeStyle = col; ctx.lineWidth = 1.2; ctx.stroke(); }
        else { ctx.fillStyle = col; ctx.fill(); ctx.strokeStyle = 'rgba(0,0,0,0.55)'; ctx.lineWidth = 0.8; ctx.stroke(); }
      }
      drawn++;
    }
    ctx.globalAlpha = 1;
    return drawn;
  }
  var SatLayer = L.GridLayer.extend({
    initialize: function () {
      L.GridLayer.prototype.initialize.call(this, { pane: 'obsPane', tileSize: TILE, updateWhenZooming: false, updateWhenIdle: true, keepBuffer: 1, className: 'ov-sat-tiles' });
      this.store = null; this.mode = 'dots'; this.win = null; this.entry = null; this._lut = buildLut('hs', SAT_FDEF.legend); this._legend = SAT_FDEF.legend;
      this._gen = 0; this._staleQ = null; this._onStore = null; this.drawn = 0;
    },
    // The store the tiles draw from (null detaches): its landings redraw the tiles in view first, the buffer in idle time.
    setStore: function (store, mode) {
      var self = this, prev = this.store;
      if (prev && prev.onChange === this._onStore) prev.onChange = null;
      this.store = store || null; this.mode = mode || 'dots';
      if (this.store) { this._onStore = function () { self._redraw(true); if (self.onRedraw) self.onRedraw(); }; this.store.onChange = this._onStore; }
      this._redraw();
    },
    // The time window drawn: {t0, t1, ref, span, frame (true over the model: alpha by |t - ref| / span), entry (the frame
    // entry it belongs to, for the landing hook's guard)}; null clears the dots. The store is asked for the window's chunks.
    setWindow: function (w) {
      this.win = w ? { t0: w.t0, t1: w.t1, ref: w.ref === undefined ? w.t1 : w.ref, span: w.span || (w.t1 - w.t0), frame: !!w.frame } : null;
      this.entry = w && w.entry ? w.entry : null;
      if (this.store && this.win) this.store.ensure(this.win.t0, this.win.t1, this.win.ref);
      this._redraw(true);
    },
    hasWindow: function () { return !!this.win; },
    count: function () { return this.store && this.win ? this.store.count(this.win.t0, this.win.t1) : 0; },
    complete: function () { return !!(this.store && this.win && this.store.complete(this.win.t0, this.win.t1)); },
    _slice: function () {
      var st = this.store, w = this.win;
      if (!st || !w || !st.flat || !st.flat.n) return null;
      var s = st.slice(w.t0, w.t1);
      return s[1] > s[0] ? { i0: s[0], i1: s[1], ref: w.ref, span: w.span, frame: w.frame } : null;
    },
    _redraw: function (split) {
      var range = split ? viewRangeOf(this) : null, later = false;
      this._gen++;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords) continue;
        if (range && !tileInRange(t.coords, range)) { later = true; continue; }
        this._draw(t.el, t.coords);
      }
      if (later) this._queueStale();
    },
    // The buffer tiles a generation behind (ModelGridLayer's rule, without its frame check).
    _drawStale: function (inView, deadline) {
      if (!this.store) return;
      var range = viewRangeOf(this), left = false;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords || t.el._ovGen === this._gen) continue;
        if (inView) { if (range && !tileInRange(t.coords, range)) continue; }
        else if (deadline && typeof deadline.timeRemaining === 'function' && !deadline.didTimeout && deadline.timeRemaining() < STALE_TILE_MS) { left = true; break; }
        this._draw(t.el, t.coords);
      }
      if (left) this._queueStale();
    },
    _queueStale: ModelGridLayer.prototype._queueStale,
    _cancelStale: ModelGridLayer.prototype._cancelStale,
    onRemove: function (map) { this._cancelStale(); L.GridLayer.prototype.onRemove.call(this, map); },
    createTile: function (coords, done) {
      var el = document.createElement('canvas'); el.width = TILE; el.height = TILE; el._ovSatN = 0;
      this._draw(el, coords);
      setTimeout(function () { done(null, el); }, 0);
      return el;
    },
    _padPx: function (z) { return this.mode === 'arrows' ? 26 + SAT_ARROW_HEAD : satRadius(z) + 2; },
    _draw: function (el, coords) {
      el._ovGen = this._gen;
      var sl = this._slice(), ctx;
      if (!sl) { if (el._ovSatN) { ctx = el.getContext('2d'); ctx.clearRect(0, 0, TILE, TILE); el._ovSatN = 0; } return; }   // empty -> empty touches nothing
      var flat = this.store.flat, stride = satStrideFor(coords.z), ids = satTileReadings(flat, sl, coords, stride, this._padPx(coords.z));
      if (!ids.length && !el._ovSatN) return;
      ctx = el.getContext('2d');
      ctx.clearRect(0, 0, TILE, TILE);
      el._ovSatN = ids.length ? composeDots(ctx, flat, ids, coords, sl, this._lut, this._legend, this.mode, coords.z) : 0;
    },
    // The top-most DRAWN reading within satRadius + 3 px (an arrow: within its half length) of (lat, lng) at tile zoom z, as
    // a flat index, or -1: the same window, stride and order as the tiles, so the hover is what is drawn.
    hitAt: function (lat, lng, z) {
      var sl = this._slice();
      if (!sl) return -1;
      var flat = this.store.flat, arrows = this.mode === 'arrows', r = satRadius(z), rPx = arrows ? satArrowLength(99) / 2 + SAT_ARROW_HEAD + 3 : r + 3;
      // the mark that holds the cursor: a dot's disc (its rim included), an arrow's shaft within SAT_ARROW_W + 1 px (the head's
      // width at the tip): dx / dy = the cursor from the mark's centre in tile pixels
      var inside = arrows ? function (j, dx, dy) {
        var tp = flat.tp[0][j], d = flat.dir[0][j];
        if (!(tp === tp) || !(d === d)) return false;
        var len = satArrowLength(tp), ang = (d + 180) * Math.PI / 180, ux = Math.sin(ang), uy = -Math.cos(ang), s = dx * ux + dy * uy;
        if (s < -len / 2 - 1 || s > len / 2 + SAT_ARROW_HEAD) return false;
        var off = Math.abs(dx * uy - dy * ux);
        return off <= (s > len / 2 - SAT_ARROW_HEAD ? SAT_ARROW_HEAD * 0.6 + 1 : SAT_ARROW_W + 1);
      } : function (j, dx, dy) { return dx * dx + dy * dy <= (r + 0.5) * (r + 0.5); };
      return flat.index.nearest(flat, lat, lng, z, rPx, sl.i0, sl.i1, satStrideFor(z), inside);
    }
  });

  // ---- the radar layer (plan section 85) ----
  // tools/model_frames/radar.py publishes NOAA MRMS and EUMETNET OPERA under radar/v1/ every 10 minutes: per slot and region
  // one whole-region picture (t0, 10 pixels a degree, codes 0 = no coverage, 1 = covered no echo, >= 2 = echo) and only the
  // cells holding echo at 25 (t1) and 100 (t2) pixels a degree, listed as hex bitsets in the pointer. It is its own data path
  // beside the model's: a RadarStore for the files, a RadarLayer for the tiles, a RadarPlayer for the loop. The model
  // scheduler, its caches and its play state are not used and not touched.
  function radarSlotMs(t) { return Date.UTC(+t.slice(0, 4), +t.slice(4, 6) - 1, +t.slice(6, 8), +t.slice(8, 10), +t.slice(10, 12)); }
  function radarDbz(code) { return RADAR_FLOOR + (code - 2) * RADAR_STEP; }
  function radarAge(mins) { return mins < 60 ? mins + ' min' : Math.floor(mins / 60) + ' h' + (mins % 60 ? ' ' + (mins % 60) + ' min' : ''); }
  function radarRainWord(dbz) {
    return dbz < 20 ? 'very light rain' : dbz < 30 ? 'light rain' : dbz < 40 ? 'moderate rain' : dbz < 50 ? 'heavy rain' : dbz < 60 ? 'very heavy rain' : 'intense rain, possible hail';
  }
  function radarSnowWord(dbz) { return dbz < 15 ? 'very light snow' : dbz < 25 ? 'light snow' : dbz < 32 ? 'moderate snow' : 'heavy snow'; }
  function radarMixWord(dbz) { return dbz < 25 ? 'light mix (estimated)' : dbz < 40 ? 'mix (estimated)' : 'heavy mix (estimated)'; }
  function radarLoopOf(v) { return RADAR_LOOP_HOURS.indexOf(v) >= 0 ? v : RADAR_DEFAULT_LOOP; }
  // The class row of a drawn code in a region on JMA's scale.
  function radarClassOf(code) {
    var c = RADAR_JMA8_SNAP[Math.max(0, Math.min(RADAR_MAX_CODE, Math.round(code)))];
    for (var i = 0; i < RADAR_JMA8.length; i++) if (RADAR_JMA8[i][0] === c) return RADAR_JMA8[i];
    return RADAR_JMA8[0];
  }
  function radarClassText(row) { return row[1] === 0 ? '<' + row[2] + ' mm/h' : row[2] === null ? row[1] + '+ mm/h' : row[1] + '-' + row[2] + ' mm/h'; }
  // The job's cell_table: south to north, then west to east; a cell is named by its south-west corner.
  function radarCellTable(box, cell) {
    var out = [], index = Object.create(null);
    for (var lat = Math.floor(box[0] / cell) * cell; lat < box[2]; lat += cell) {
      for (var lon = Math.floor(box[1] / cell) * cell; lon < box[3]; lon += cell) {
        var name = lat + '_' + lon;
        index[name] = out.length; out.push({ name: name, lat0: lat, lon0: lon });
      }
    }
    return { cells: out, index: index };
  }
  // Bit i of the job's bitset: hex, four bits a character, least significant first, trailing zeros off.
  function radarBit(hex, i) {
    var ch = hex.charCodeAt(i >> 2);
    if (ch !== ch) return false;
    var v = ch <= 57 ? ch - 48 : ch - 87;
    return (v >> (i & 3) & 1) === 1;
  }
  function validRadarPointer(p) {
    if (!p || typeof p !== 'object' || Array.isArray(p) || p.format !== RADAR_FORMAT) return false;
    if (p.floor_dbz !== RADAR_FLOOR || p.step_dbz !== RADAR_STEP || p.max_code !== RADAR_MAX_CODE) return false;
    var t = p.tiers;
    if (!t || !t.t0 || t.t0.ppd !== RADAR_PPD.t0 || !t.t1 || t.t1.ppd !== RADAR_PPD.t1 || t.t1.cell !== RADAR_CELL.t1 || !t.t2 || t.t2.ppd !== RADAR_PPD.t2 || t.t2.cell !== RADAR_CELL.t2) return false;
    if (!p.regions || typeof p.regions !== 'object' || Array.isArray(p.regions) || !Array.isArray(p.slots) || p.slots.length > 400) return false;
    if (typeof p.published_utc !== 'string' || Date.parse(p.published_utc) !== Date.parse(p.published_utc)) return false;
    var names = Object.keys(p.regions), area = 0;
    if (!names.length || names.length > 32) return false;
    for (var i = 0; i < names.length; i++) {
      var r = p.regions[names[i]], b = r && r.box;
      if (!/^[a-z]{2,12}$/.test(names[i]) || !Array.isArray(b) || b.length !== 4) return false;
      for (var k = 0; k < 4; k++) if (b[k] !== Math.round(b[k])) return false;
      if (!(b[0] < b[2] && b[1] < b[3] && b[0] >= -90 && b[2] <= 90 && b[1] >= -180 && b[3] <= 180)) return false;
      if (typeof r.name !== 'string') return false;
      area += (b[2] - b[0]) * (b[3] - b[1]);
      if ((b[2] - b[0]) * (b[3] - b[1]) > RADAR_REGION_AREA || area > RADAR_TOTAL_AREA) return false;   // every t0 of the loop stays in memory
    }
    var last = '';
    for (var s = 0; s < p.slots.length; s++) {
      var rec = p.slots[s];
      if (!rec || typeof rec.t !== 'string' || !/^\d{12}$/.test(rec.t) || rec.t <= last || !rec.r || typeof rec.r !== 'object' || Array.isArray(rec.r)) return false;
      last = rec.t;
      for (var nm in rec.r) {
        if (!Object.prototype.hasOwnProperty.call(p.regions, nm)) continue;   // a rolled-back job's leftovers: _take drops them
        var e = rec.r[nm];
        if (!e || typeof e !== 'object' || e.o !== Math.round(e.o) || e.o < 0 || e.o > 59) return false;
        if ((e.t1 !== undefined && !/^[0-9a-f]{0,2048}$/.test(e.t1)) || (e.t2 !== undefined && !/^[0-9a-f]{0,2048}$/.test(e.t2))) return false;
      }
    }
    return true;
  }
  // The loop's frames, oldest first: slots of the last `hours` on the loop's clock marks (:00 / :20 / :40; :00 / :30; :00), plus the
  // newest frame whatever its minute. A slot holding no frame yet (`r` empty) is no frame. upTo: the newest slot joined so far.
  function radarLoopFrames(slots, hours, upTo) {
    var every = RADAR_LOOPS[radarLoopOf(hours)], list = [], i;
    for (i = 0; i < slots.length; i++) if (!upTo || slots[i].t <= upTo) list.push(slots[i]);
    if (!list.length) return [];
    var newest = list[list.length - 1], min = radarSlotMs(newest.t) - radarLoopOf(hours) * 3600000, out = [];
    for (i = 0; i < list.length; i++) {
      var ms = radarSlotMs(list[i].t);
      if (ms >= min && (list[i] === newest || Math.round(ms / 60000) % every === 0)) out.push(list[i]);
    }
    return out;
  }
  // What the map shows, for planning: unwrapped longitudes brought to one world copy.
  function radarViewOf(map, layer) {
    var z = layer && typeof layer._tileZoom === 'number' ? layer._tileZoom : (map && map.getZoom ? Math.round(map.getZoom()) : 0);
    if (!map || typeof map.getBounds !== 'function') return { whole: true, s: -90, n: 90, w: -180, e: 180, z: z };
    var b = map.getBounds(), w = b.getWest(), e = b.getEast(), wide = e - w, s = b.getSouth(), n = b.getNorth(), mx = wide * 0.05, my = (n - s) * 0.05;
    if (wide + 2 * mx >= 360) return { whole: true, s: s - my, n: n + my, w: -180, e: 180, z: z };
    var w0 = ((w - mx + 180) % 360 + 360) % 360 - 180;
    return { whole: false, s: s - my, n: n + my, w: w0, e: w0 + wide + 2 * mx, z: z };
  }
  function radarHits(v, s, w, n, e) {
    if (s >= v.n || n <= v.s) return false;
    if (v.whole) return true;
    return (w < v.e && e > v.w) || (w < v.e - 360 && e > v.w - 360) || (w < v.e + 360 && e > v.w + 360);
  }
  function radarWantTier(z) { return z >= RADAR_T2_ZOOM ? 2 : z >= RADAR_T1_ZOOM ? 1 : 0; }
  // A landed picture. A t1 / t2 cell (crop) keeps only the box round its rain: rain fills about 2 % of a t1 cell and sits in about a quarter of
  // it, and everything outside the box reads as no echo anyway, so the sampler draws the same pixels (r0, c0 place the box).
  function radarPic(frame, crop) {
    var q = frame.q, cols = frame.cols, rows = frame.rows, r0 = 0, c0 = 0, r, c, i;
    if (crop) {
      var top = rows, bot = -1, lft = cols, rgt = -1;
      for (r = 0, i = 0; r < rows; r++) {
        for (c = 0; c < cols; c++, i++) {
          if (q[i] < 2) continue;
          if (r < top) top = r;
          bot = r;
          if (c < lft) lft = c;
          if (c > rgt) rgt = c;
        }
      }
      var cw = bot < 0 ? 0 : rgt - lft + 1, ch = bot < 0 ? 0 : bot - top + 1, out = new Uint8Array(cw * ch);
      for (r = 0; r < ch; r++) { var from = (top + r) * cols + lft; out.set(q.subarray(from, from + cw), r * cw); }
      q = out; cols = cw; rows = ch; r0 = bot < 0 ? 0 : top; c0 = bot < 0 ? 0 : lft;
    }
    var re = new Uint8Array(rows);
    for (r = 0, i = 0; r < rows; r++) {
      for (var end = i + cols; i < end; i++) if (q[i] >= 2) { re[r] = 1; i = end; break; }
    }
    return { q: q, cols: cols, rows: rows, r0: r0, c0: c0, re: re, bytes: q.length };
  }
  var RADAR_STORES = {};
  function radarStore(root) { return RADAR_STORES[root] || (RADAR_STORES[root] = new RadarStore(root)); }
  // The pointer and the files. Files are keyed by slot and never change, so everything survives a new slot; a 404 means the
  // pointer is older than the bucket (a slot retired): the pointer is read again, the file is tried again later, never given up
  // for good. At most RADAR_INFLIGHT downloads; over the cap the oldest landed picture goes first, never what the current plan names.
  function RadarStore(root) {
    this.root = root; this.url = root + '/' + RADAR_PREFIX;
    this.pointer = null; this.status = 'idle'; this.loading = null; this.readAt = 0; this.failedAt = 0; this.reading = null;
    this.frames = []; this.order = []; this.tables = Object.create(null); this.index = Object.create(null);
    this.pics = new Map(); this.bytes = 0; this.inflight = Object.create(null); this.fails = Object.create(null);
    this.tierSeen = { t1: { n: 0, bytes: 0 }, t2: { n: 0, bytes: 0 } }; this.lastPlan = null;
    this.queue = []; this.planSet = Object.create(null); this.loop = []; this.held = []; this.slotSig = null; this.stack = { t1: false, t2: false };
    this.cov = Object.create(null); this.covRev = 0; this.rev = 0; this._memo = null; this._memoRev = -1;
    this.active = false; this.hidden = false; this.timer = null; this.retryTimer = null;
    this.onLanding = null; this.onPointer = null;
  }
  RadarStore.prototype.stallMs = RADAR_STALL_MS;
  RadarStore.prototype.retryMs = RADAR_RETRY_MS;
  RadarStore.prototype.pointerMs = 60000;
  RadarStore.prototype.maxBytes = RADAR_MAX_BYTES;
  RadarStore.prototype._json = SstStore.prototype._json;
  // One read of the pointer, under a watchdog; never rejects: true when a valid pointer came.
  RadarStore.prototype._read = function () {
    var self = this;
    if (this.reading) return this.reading;
    var ctrl = this.readCtrl = new AbortController(), dog = setTimeout(function () { ctrl.abort(); }, this.stallMs);
    var p = this.reading = this._json(this.url + '/latest.json', ctrl.signal, true, RADAR_POINTER_BYTES).then(function (ptr) {
      if (!validRadarPointer(ptr)) throw new Error('unsupported radar pointer');
      self._take(ptr);
      return true;
    }).catch(function () { return false; }).then(function (ok) { clearTimeout(dog); if (self.reading === p) self.reading = null; return ok; });
    return p;
  };
  RadarStore.prototype._take = function (ptr) {
    var held = this.pointer, oldNewest = this.frames.length ? this.frames[this.frames.length - 1].t : null, s, nm;
    for (s = 0; s < ptr.slots.length; s++) for (nm in ptr.slots[s].r) if (!Object.prototype.hasOwnProperty.call(ptr.regions, nm)) delete ptr.slots[s].r[nm];
    // The job writes a tick's regions one after another under one stamp, so a later region shows only in the slots.
    var sig = JSON.stringify([ptr.regions, ptr.slots]), changed = !held || ptr.published_utc !== held.published_utc || sig !== this.slotSig;
    this.slotSig = sig;
    if (held) this._boxesMoved(held, ptr);
    this.pointer = ptr; this.readAt = Date.now(); this.typed = radarTypeCodesOk(ptr.type_codes);
    var names = Object.keys(ptr.regions), order = [], i;
    for (i = 0; i < RADAR_ORDER.length; i++) if (names.indexOf(RADAR_ORDER[i]) >= 0) order.push(RADAR_ORDER[i]);
    for (i = 0; i < names.length; i++) if (order.indexOf(names[i]) < 0) order.push(names[i]);
    if (held && order.join() !== this.order.join()) { this.covRev++; this.rev++; this.regionsMoved = true; }   // the tiles' owner numbers count in this order
    this.order = order;
    this.frames = ptr.slots.filter(function (s) { for (var k in s.r) return true; return false; });   // `r: {}` is no frame (G56a P3-7)
    this.index = Object.create(null);
    for (i = 0; i < this.frames.length; i++) this.index[this.frames[i].t] = this.frames[i];
    if ((this.frames.length ? this.frames[this.frames.length - 1].t : null) !== oldNewest) changed = true;
    if (changed && this.onPointer) this.onPointer();
  };
  // The pictures and tables of a region are cut for its box: when a pointer moves the box they start over.
  RadarStore.prototype._boxesMoved = function (held, ptr) {
    for (var name in held.regions) {
      var was = held.regions[name].box, now = ptr.regions[name] && ptr.regions[name].box;
      if (now && was[0] === now[0] && was[1] === now[1] && was[2] === now[2] && was[3] === now[3]) continue;
      delete this.tables[name + '|t1']; delete this.tables[name + '|t2'];
      if (this.cov[name]) { delete this.cov[name]; this.covRev++; }
      var self = this, mark = '/' + name + '/', gone = [];
      this.pics.forEach(function (pic, key) { if (key.indexOf(mark) >= 0) gone.push([key, pic]); });
      gone.forEach(function (g) { self.pics.delete(g[0]); self.bytes -= g[1].bytes; });
      this.rev++; this.regionsMoved = true;
    }
  };
  // Resolves to the store with a pointer, or null when none can be read (never rejects). force: a viewer's pick or Retry.
  RadarStore.prototype.load = function (force) {
    var self = this;
    if (this.loading) return this.loading;
    if (this.pointer && Date.now() - this.readAt < this.pointerMs) { this.status = 'ok'; return Promise.resolve(this); }
    if (!force && this.status === 'failed' && Date.now() - this.failedAt < this.retryMs) return Promise.resolve(null);
    if (!this.pointer) this.status = 'loading';
    var had = !!this.pointer;
    var p = this.loading = this._read().then(function (ok) {
      if (self.loading !== p) return ok || had ? self : null;   // stopped and asked again meanwhile: that read decides
      self.loading = null;
      if (ok || had) { self.status = 'ok'; return self; }
      self.status = 'failed'; self.failedAt = Date.now();
      return null;
    });
    return this.loading;
  };
  // While shown: the pointer is read again just after the next publish is due (the last publish + 10 min), every minute once overdue.
  RadarStore.prototype.start = function () { this.active = true; this.hidden = false; this._arm(); };
  RadarStore.prototype._arm = function () {
    var self = this;
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    if (!this.active || this.hidden) return;
    var pub = this.pointer ? Date.parse(this.pointer.published_utc) : NaN, now = Date.now();
    var wait = pub === pub ? Math.min(pub + RADAR_PUBLISH_MS + 20000 - now, RADAR_PUBLISH_MS * 2) : 0;
    if (pub === pub && now - pub < RADAR_SETTLE_MS) wait = Math.min(pub + RADAR_SETTLE_MS + 10000 - now, wait);   // a job still writing its later regions: they are seen minutes sooner
    this.timer = setTimeout(function () {
      self.timer = null;
      self._read().then(function () { self._arm(); });
    }, Math.max(wait, this.pointerMs));
  };
  RadarStore.prototype.hide = function () {
    this.hidden = true;
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
  };
  RadarStore.prototype.show = function () {
    var self = this;
    this.hidden = false;
    if (!this.active) return;
    if (Date.now() - this.readAt >= this.pointerMs) this._read().then(function () { self._arm(); }); else this._arm();
    this._pump();
  };
  RadarStore.prototype._table = function (region, tier) {
    var k = region + '|' + tier;
    return this.tables[k] || (this.tables[k] = radarCellTable(this.pointer.regions[region].box, RADAR_CELL[tier]));
  };
  RadarStore.prototype.listed = function (rec, region, tier, name) {
    var e = rec.r[region], hex = e && e[tier];
    if (!hex) return false;
    var i = this._table(region, tier).index[name];
    return i !== undefined && radarBit(hex, i);
  };
  RadarStore.prototype.key = function (slot, region, tier, name) { return slot + '/' + region + '/' + tier + (name ? '/' + name : ''); };
  RadarStore.prototype._failWait = function (f) { return f.final ? RADAR_DAMAGED_RETRY_MS : this.retryMs; };
  RadarStore.prototype.done = function (key) {
    if (this.pics.has(key)) return true;
    var f = this.fails[key];
    return !!(f && Date.now() - f.at < this._failWait(f));
  };
  RadarStore.prototype.newestOf = function (region) {
    for (var i = this.frames.length - 1; i >= 0; i--) if (this.frames[i].r[region]) return this.frames[i];
    return null;
  };
  RadarStore.prototype.regionsIn = function (view) {
    var out = [], regions = this.pointer ? this.pointer.regions : {};
    for (var i = 0; i < this.order.length; i++) {
      var b = regions[this.order[i]].box;
      if (radarHits(view, b[0], b[1], b[2], b[3])) out.push(this.order[i]);
    }
    return out;
  };
  // The region's frame for step i of the loop: its own, else the one before it, at most RADAR_HOLD_STEPS back; else none.
  // A region's box holds this point and its coverage has not landed: nothing can be said of the point yet.
  RadarStore.prototype.coverageDue = function (lat, lng) {
    var lon = ((lng + 180) % 360 + 360) % 360 - 180;
    for (var i = 0; i < this.order.length; i++) {
      var name = this.order[i], b = this.pointer.regions[name].box;
      if (!this.cov[name] && this.newestOf(name) && lat >= b[0] && lat < b[2] && lon >= b[1] && lon < b[3]) return true;
    }
    return false;
  };
  RadarStore.prototype.resolve = function (region, i) {
    var step = this.loop[i], rec;
    if (step && step.r[region] && this.pics.has(this.key(step.t, region, 't0'))) return step;
    if (step && i === this.loop.length - 1 && (rec = this.heldAt(region, step.t, true))) return rec;
    for (var j = i - 1; j >= 0 && j >= i - RADAR_HOLD_STEPS; j--) {
      rec = this.loop[j];
      if (rec && rec.r[region] && this.pics.has(this.key(rec.t, region, 't0'))) return rec;
    }
    return null;
  };
  // The region's own newest frame at or before the slot, at most RADAR_HOLD_STEPS slots back, on the loop's clock marks or not: at the
  // loop's newest step a region a slot behind (Taiwan's file lands a tick later) shows its own last frame, not the last mark's.
  RadarStore.prototype.heldAt = function (region, slot, landed) {
    var limit = radarSlotMs(slot) - RADAR_HOLD_STEPS * RADAR_PUBLISH_MS;
    for (var i = this.frames.length - 1; i >= 0; i--) {
      var rec = this.frames[i];
      if (rec.t > slot) continue;
      if (radarSlotMs(rec.t) < limit) return null;
      if (rec.r[region] && (!landed || this.pics.has(this.key(rec.t, region, 't0')))) return rec;
    }
    return null;
  };
  // A tier of a cell is drawn in playback only once every step of the loop that lists it has landed (or given up for now), so no
  // tile flips between tiers from frame to frame.
  RadarStore.prototype.stackReady = function (region, tier, name) {
    if (!this.stack[tier]) return false;
    if (this._memoRev !== this.rev) { this._memo = Object.create(null); this._memoRev = this.rev; }
    var k = region + '|' + tier + '|' + name, got = this._memo[k];
    if (got !== undefined) return got;
    var ok = true;
    for (var i = 0; i < this.loop.length; i++) {
      if (this.listed(this.loop[i], region, tier, name) && !this.done(this.key(this.loop[i].t, region, tier, name))) { ok = false; break; }
    }
    for (i = 0; ok && i < this.held.length; i++) {
      if (this.held[i][1] === region && this.listed(this.held[i][0], region, tier, name) && !this.done(this.key(this.held[i][0].t, region, tier, name))) ok = false;
    }
    this._memo[k] = ok;
    return ok;
  };
  // Every stack the view wants is in (or given up for now): Play can start without the picture dropping to a coarser tier.
  RadarStore.prototype.stacksReady = function (view) {
    var regs = this.regionsIn(view), tiers = ['t1', 't2'];
    for (var i = 0; i < regs.length; i++) for (var t = 0; t < 2; t++) {
      if (!this.stack[tiers[t]]) continue;
      var cells = this._cellsIn(regs[i], tiers[t], view);
      for (var c = 0; c < cells.length; c++) if (!this.stackReady(regs[i], tiers[t], cells[c].name)) return false;
    }
    return true;
  };
  // Everything the view needs of this frame has landed or been given up for now.
  RadarStore.prototype.frameReady = function (rec, view, deep) {
    var regs = this.regionsIn(view), want = radarWantTier(view.z), self = this;
    for (var i = 0; i < regs.length; i++) {
      var region = regs[i];
      if (!rec.r[region]) continue;
      if (!this.done(this.key(rec.t, region, 't0'))) return false;
      if (!deep) continue;
      for (var t = 1; t <= want; t++) {
        var tier = 't' + t, cells = this._cellsIn(region, tier, view);
        for (var c = 0; c < cells.length; c++) {
          if (self.listed(rec, region, tier, cells[c].name) && !self.done(self.key(rec.t, region, tier, cells[c].name))) return false;
        }
      }
    }
    return true;
  };
  RadarStore.prototype._cellsIn = function (region, tier, view) {
    var cells = this._table(region, tier).cells, size = RADAR_CELL[tier], out = [];
    for (var i = 0; i < cells.length; i++) if (radarHits(view, cells[i].lat0, cells[i].lon0, cells[i].lat0 + size, cells[i].lon0 + size)) out.push(cells[i]);
    return out;
  };
  // The downloads the view asks for, most urgent first: the coverage (newest t0), the shown frame, a slot about to join, the loop's
  // t0, then its t1 and t2 while the whole stack fits the budget (a stack that does not fit is never started: only the shown frame
  // gets that tier). Downloads outside the plan are dropped.
  RadarStore.prototype.plan = function (view, loop, shown, extra) {
    if (!this.pointer) return;
    var self = this, regs = this.regionsIn(view), want = radarWantTier(view.z), items = [], seen = Object.create(null), i, r;
    this.loop = loop; this.rev++; this.lastPlan = [view, loop, shown, extra];
    function add(rec, region, tier, cell) {
      var key = self.key(rec.t, region, tier, cell ? cell.name : null);
      if (seen[key]) return;
      seen[key] = true;
      var b = self.pointer.regions[region].box;
      var expect = tier === 't0' ? { cols: (b[3] - b[1]) * RADAR_PPD.t0, rows: (b[2] - b[0]) * RADAR_PPD.t0 } : { cols: RADAR_CELL[tier] * RADAR_PPD[tier] + 2, rows: RADAR_CELL[tier] * RADAR_PPD[tier] + 2 };
      items.push({ key: key, slot: rec.t, region: region, tier: tier, expect: expect, size: tier === 't0' ? expect.cols * expect.rows : self._cellBytes(key, tier) });
    }
    function addCells(rec, region, tier) {
      var cells = self._cellsIn(region, tier, view);
      for (var c = 0; c < cells.length; c++) if (self.listed(rec, region, tier, cells[c].name)) add(rec, region, tier, cells[c]);
    }
    function front(rec) {
      if (!rec) return;
      for (var q = 0; q < regs.length; q++) {
        if (!rec.r[regs[q]]) continue;
        add(rec, regs[q], 't0');
        if (want >= 1) addCells(rec, regs[q], 't1');
        if (want >= 2) addCells(rec, regs[q], 't2');
      }
    }
    for (r = 0; r < regs.length; r++) { var nw = this.newestOf(regs[r]); if (nw) add(nw, regs[r], 't0'); }
    var n = loop.length, last = loop[n - 1], held = this.held = [];   // the regions a slot behind at the newest step (resolve)
    if (last) for (r = 0; r < regs.length; r++) {
      if (last.r[regs[r]]) continue;
      var own = this.heldAt(regs[r], last.t);
      if (own) held.push([own, regs[r]]);
    }
    front(loop[shown]);
    if (shown === n - 1) for (i = 0; i < held.length; i++) {
      add(held[i][0], held[i][1], 't0');
      if (want >= 1) addCells(held[i][0], held[i][1], 't1');
      if (want >= 2) addCells(held[i][0], held[i][1], 't2');
    }
    front(extra);
    for (i = 0; i < n; i++) {
      var rec = loop[(shown + i) % n];
      for (r = 0; r < regs.length; r++) if (rec.r[regs[r]]) add(rec, regs[r], 't0');
    }
    for (i = 0; i < held.length; i++) add(held[i][0], held[i][1], 't0');
    // what the stacks would hold: each tier wanted only while the running total fits the cap
    var sum = 0; for (i = 0; i < items.length; i++) sum += items[i].size;
    // each cell once: the cells already planned (the shown frame's, the held frames') are in `sum`; a held frame can be a loop frame
    function stackSize(tier) {
      var total = 0, counted = Object.create(null);
      function cellsOf(rec, region) {
        var cells = self._cellsIn(region, tier, view);
        for (var c = 0; c < cells.length; c++) {
          if (!self.listed(rec, region, tier, cells[c].name)) continue;
          var key = self.key(rec.t, region, tier, cells[c].name);
          if (seen[key] || counted[key]) continue;
          counted[key] = true; total += self._cellBytes(key, tier);
        }
      }
      for (var f = 0; f < n; f++) for (var q = 0; q < regs.length; q++) if (loop[f].r[regs[q]]) cellsOf(loop[f], regs[q]);
      for (var h = 0; h < held.length; h++) cellsOf(held[h][0], held[h][1]);
      return total;
    }
    var s1 = want >= 1 ? stackSize('t1') : 0, s2 = want >= 2 ? stackSize('t2') : 0;
    this.stack = { t1: want >= 1 && sum + s1 <= this.maxBytes, t2: want >= 2 && sum + s1 + s2 <= this.maxBytes };
    if (this.stack.t1) for (i = 0; i < n; i++) { var a = loop[(shown + i) % n]; for (r = 0; r < regs.length; r++) if (a.r[regs[r]]) addCells(a, regs[r], 't1'); }
    if (this.stack.t2) for (i = 0; i < n; i++) { var b2 = loop[(shown + i) % n]; for (r = 0; r < regs.length; r++) if (b2.r[regs[r]]) addCells(b2, regs[r], 't2'); }
    for (i = 0; i < held.length; i++) {
      if (this.stack.t1) addCells(held[i][0], held[i][1], 't1');
      if (this.stack.t2) addCells(held[i][0], held[i][1], 't2');
    }
    this.queue = items; this.planSet = seen;
    for (var k in this.inflight) if (!seen[k]) this._drop(k);
    this._evict();
    this._pump();
  };
  // What a t1 / t2 cell holds or will hold: its real size once landed; before that, the mean of what has landed of that tier.
  RadarStore.prototype._cellBytes = function (key, tier) {
    var pic = this.pics.get(key), seen = this.tierSeen[tier];
    if (pic) return pic.bytes;
    if (seen.n) return seen.bytes / seen.n;
    var side = RADAR_CELL[tier] * RADAR_PPD[tier] + 2;
    return side * side * RADAR_FILL;
  };
  RadarStore.prototype._drop = function (key) {
    var rec = this.inflight[key];
    if (!rec) return;
    delete this.inflight[key];
    clearTimeout(rec.dog); rec.ctrl.abort();
  };
  RadarStore.prototype._pump = function () {
    if (this.hidden) return;                                  // show() starts it again
    var self = this, n = 0, now = Date.now(), soonest = null, k;
    for (k in this.inflight) n++;
    for (var i = 0; i < this.queue.length; i++) {
      var it = this.queue[i];
      if (this.pics.has(it.key) || this.inflight[it.key]) continue;
      var f = this.fails[it.key];
      if (f && now - f.at < this._failWait(f)) { var due = f.at + this._failWait(f); if (soonest === null || due < soonest) soonest = due; continue; }
      if (n >= RADAR_INFLIGHT) break;
      this._start(it); n++;
    }
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    if (soonest !== null && this.active) this.retryTimer = setTimeout(function () { self.retryTimer = null; self._pump(); }, Math.max(100, soonest - now));
  };
  RadarStore.prototype._start = function (it) {
    var self = this, ctrl = new AbortController(), rec = { ctrl: ctrl, dog: null };
    var kick = function () { clearTimeout(rec.dog); rec.dog = setTimeout(function () { ctrl.abort(); }, self.stallMs); };   // no byte for this long: given up for now
    this.inflight[it.key] = rec;
    kick();
    decodeFrame(this.url + '/' + it.key + '.png', ctrl.signal, it.expect, null, true, kick).then(function (frame) {
      clearTimeout(rec.dog);
      if (self.inflight[it.key] !== rec) return;
      delete self.inflight[it.key];
      self._land(it, radarPic(frame, it.tier !== 't0'));
    }, function (err) {
      clearTimeout(rec.dog);
      if (self.inflight[it.key] !== rec) return;
      delete self.inflight[it.key];
      var msg = String(err && err.message || err);
      self.fails[it.key] = { at: Date.now(), final: /decode/.test(msg) };
      if (/ 40[34]$/.test(msg) && Date.now() - self.readAt >= self.pointerMs) self._read().then(function () { if (self.active) self._arm(); });
      self.rev++;
      if (self.onLanding) self.onLanding(it, false);
      self._pump();
    });
  };
  RadarStore.prototype._land = function (it, pic) {
    this.pics.set(it.key, pic); this.bytes += pic.bytes; delete this.fails[it.key]; this.rev++;
    if (it.tier !== 't0') { this.tierSeen[it.tier].n++; this.tierSeen[it.tier].bytes += pic.bytes; }
    if (it.tier === 't0') {
      var had = this.cov[it.region];
      if (!had || it.slot >= had.slot) { this.cov[it.region] = { slot: it.slot, pic: pic }; this.covRev++; }
    }
    if (this.bytes > this.maxBytes && (this.stack.t1 || this.stack.t2) && this.lastPlan) this.plan.apply(this, this.lastPlan);   // heavier rain than the estimate: the stack gives way, never the cap
    this._evict();
    if (this.onLanding) this.onLanding(it, true);
    this._pump();
  };
  RadarStore.prototype._evict = function () {
    if (this.bytes <= this.maxBytes) return;
    var self = this, gone = [];
    this.pics.forEach(function (pic, key) {
      if (self.bytes <= self.maxBytes || self.planSet[key]) return;
      self.bytes -= pic.bytes; gone.push(key);
    });
    gone.forEach(function (key) { self.pics.delete(key); });
  };
  // Stops everything; the pointer stays, the pictures stay until release().
  RadarStore.prototype.stop = function () {
    this.active = false;
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    if (this.reading && this.readCtrl) this.readCtrl.abort();
    this.reading = null; this.loading = null;                 // the next load reads afresh, not this aborted read
    for (var k in this.inflight) this._drop(k);
    this.queue = []; this.planSet = Object.create(null); this.onLanding = null; this.onPointer = null;
  };
  // Leaving the layer: nothing of the pictures stays in memory (the browser's cache has the files).
  RadarStore.prototype.release = function () {
    this.stop();
    this.pics.clear(); this.bytes = 0; this.fails = Object.create(null); this.cov = Object.create(null); this.covRev++; this.loop = []; this.held = []; this.rev++; this.lastPlan = null;
  };
  // The tile's latitudes and longitudes at pixel centres, kept with the tile.
  function radarTileGeom(el, coords) {
    if (el._rLat) return el;
    var b = tileBox(coords), n = TILE * b.n, lat = new Float64Array(TILE), lon = new Float64Array(TILE);
    for (var i = 0; i < TILE; i++) {
      lat[i] = Math.atan(Math.sinh(Math.PI - 2 * Math.PI * (coords.y * TILE + i + 0.5) / n)) * 180 / Math.PI;
      lon[i] = (b.xw * TILE + i + 0.5) / n * 360 - 180;
    }
    el._rLat = lat; el._rLon = lon;
    return el;
  }
  // The rectangle of tile pixels whose centres lie in the cell (lat0 <= lat < lat0 + cell, lon0 <= lon < lon0 + cell), or null.
  function radarCellRect(el, lat0, lon0, cell) {
    var y0 = -1, y1 = -1, x0 = -1, x1 = -1, i;
    for (i = 0; i < TILE; i++) if (el._rLat[i] >= lat0 && el._rLat[i] < lat0 + cell) { if (y0 < 0) y0 = i; y1 = i + 1; }
    for (i = 0; i < TILE; i++) if (el._rLon[i] >= lon0 && el._rLon[i] < lon0 + cell) { if (x0 < 0) x0 = i; x1 = i + 1; }
    return y0 < 0 || x0 < 0 ? null : { y0: y0, y1: y1, x0: x0, x1: x1 };
  }
  // THE sampler (tiles and readout alike): fractional codes of the pieces into out[y * 256 + x], bilinear over the echo nodes
  // present (fewer than a quarter of the weight present = none); only pixels the piece's region owns. A row whose two node rows
  // hold no echo is skipped at once, so a dry frame costs next to nothing. -> how many pixels hold echo.
  // A typed region's piece: bilinear on the value (the rain codes' scale), the type of the node with the most weight (the first of
  // equals), so a colour is never a blend of two types; codes past the rain ones only where the pointer's types can be read.
  function radarTypedDef(st, def) { return !!(st.typed && def && typeof def.types === 'string' && /[sm]/.test(def.types) && def.scale !== 'jma8'); }
  function radarSample(pieces, el, own, x0, x1, y0, y1, out) {
    var count = 0, lats = el._rLat, lons = el._rLon, RC = RADAR_RC, T = RADAR_T, MAXC = RADAR_MAX_CODE, BASE = RADAR_TYPE_BASE;
    for (var p = 0; p < pieces.length; p++) {
      var pc = pieces[p], pic = pc.pic, q = pic.q, cols = pic.cols, rows = pic.rows, re = pic.re, who = pc.own, ppd = pc.ppd, snap = pc.snap, typed = pc.typed;
      var ys = Math.max(pc.y0, y0), ye = Math.min(pc.y1, y1), xs = Math.max(pc.x0, x0), xe = Math.min(pc.x1, x1);
      for (var y = ys; y < ye; y++) {
        var fy = (pc.top - lats[y]) * ppd + pc.ay - pic.r0, r0 = Math.floor(fy), wy = fy - r0, r1 = r0 + 1;
        var has0 = r0 >= 0 && r0 < rows && re[r0] === 1, has1 = r1 >= 0 && r1 < rows && re[r1] === 1;
        if (!has0 && !has1) continue;
        var b0 = r0 * cols, b1 = r1 * cols;
        for (var x = xs; x < xe; x++) {
          var i = y * TILE + x;
          if (own[i] !== who) continue;
          var fx = (lons[x] - pc.left) * ppd + pc.ax - pic.c0, c0 = Math.floor(fx), wx = fx - c0, c1 = c0 + 1, w = 0, acc = 0, v, f;
          var ok0 = c0 >= 0 && c0 < cols, ok1 = c1 >= 0 && c1 < cols;
          if (typed) {
            var best = -1, bt = 0, r;
            if (has0) {
              if (ok0 && (r = RC[v = q[b0 + c0]]) > 0) { f = (1 - wx) * (1 - wy); w += f; acc += f * r; if (f > best) { best = f; bt = T[v]; } }
              if (ok1 && (r = RC[v = q[b0 + c1]]) > 0) { f = wx * (1 - wy); w += f; acc += f * r; if (f > best) { best = f; bt = T[v]; } }
            }
            if (has1) {
              if (ok0 && (r = RC[v = q[b1 + c0]]) > 0) { f = (1 - wx) * wy; w += f; acc += f * r; if (f > best) { best = f; bt = T[v]; } }
              if (ok1 && (r = RC[v = q[b1 + c1]]) > 0) { f = wx * wy; w += f; acc += f * r; if (f > best) { best = f; bt = T[v]; } }
            }
            if (w < 0.25) continue;
            out[i] = bt * BASE + acc / w; count++;
            continue;
          }
          if (has0) {
            if (ok0 && (v = q[b0 + c0]) >= 2 && v <= MAXC) { f = (1 - wx) * (1 - wy); w += f; acc += f * v; }
            if (ok1 && (v = q[b0 + c1]) >= 2 && v <= MAXC) { f = wx * (1 - wy); w += f; acc += f * v; }
          }
          if (has1) {
            if (ok0 && (v = q[b1 + c0]) >= 2 && v <= MAXC) { f = (1 - wx) * wy; w += f; acc += f * v; }
            if (ok1 && (v = q[b1 + c1]) >= 2 && v <= MAXC) { f = wx * wy; w += f; acc += f * v; }
          }
          if (w < 0.25) continue;
          out[i] = snap ? snap[Math.round(acc / w)] : acc / w; count++;   // a region on classes: never a value between two
        }
      }
    }
    return count;
  }
  // RGBA per half code: colour by the legend's position, alpha rising from thin drizzle to solid rain; rain, snow and mix each a
  // section of RADAR_TYPE_BASE * 2 entries (a pixel's stored value * 2 is its index).
  function radarLut() {
    var n = RADAR_MAX_CODE * 2 + 2, sec = RADAR_TYPE_BASE * 2, out = new Uint8ClampedArray(sec * 3 * 4), rgb = [0, 0, 0], leg = RADAR_FDEF.legend;
    var ramps = [RAMPS.radar, RAMPS.radarSnow, RAMPS.radarMix];
    for (var t = 0; t < 3; t++) {
      for (var i = 4; i < n; i++) {
        var dbz = radarDbz(i / 2), pos = Math.max(0, Math.min(1, (dbz - leg[0]) / (leg[1] - leg[0]))), k = (t * sec + i) * 4;
        rampAt(ramps[t], pos, rgb, 0);
        out[k] = rgb[0]; out[k + 1] = rgb[1]; out[k + 2] = rgb[2];
        out[k + 3] = 255 * Math.min(RADAR_ALPHA[1], RADAR_ALPHA[0] + (dbz - RADAR_FLOOR) / 20 * (RADAR_ALPHA[1] - RADAR_ALPHA[0]));
      }
    }
    return out;
  }
  var RadarLayer = L.GridLayer.extend({
    initialize: function () {
      L.GridLayer.prototype.initialize.call(this, { pane: 'obsPane', tileSize: TILE, updateWhenZooming: false, updateWhenIdle: true, keepBuffer: 1, className: 'ov-radar-tiles' });
      this.store = null; this.idx = -1; this.playing = false; this._gen = 0; this._staleQ = null;
      this._lut = radarLut(); this._code = new Float32Array(TILE * TILE); this._img = null;
    },
    setStore: function (store) { this.store = store || null; this.idx = -1; this._redraw(); },
    // The loop's step on the map; split: the tiles in view now, the buffer in idle time.
    setStep: function (i, playing, split) { this.idx = i; this.playing = !!playing; this._redraw(!!split); },
    _redraw: SatLayer.prototype._redraw,
    _drawStale: SatLayer.prototype._drawStale,
    _queueStale: ModelGridLayer.prototype._queueStale,
    _cancelStale: ModelGridLayer.prototype._cancelStale,
    onRemove: function (map) { this._cancelStale(); L.GridLayer.prototype.onRemove.call(this, map); },
    createTile: function (coords, done) {
      var el = document.createElement('canvas'); el.width = TILE; el.height = TILE;
      this._draw(el, coords);
      setTimeout(function () { done(null, el); }, 0);
      return el;
    },
    // Which region owns each pixel: the first in the priority order whose newest t0 covers it (never by box: the US box's no-coverage
    // strip must not hide the Caribbean's data). Kept with the tile until the coverage changes.
    _owners: function (el) {
      var st = this.store;
      if (el._rOwn && el._rOwnRev === st.covRev) return el._rOwn;
      var own = el._rOwn || (el._rOwn = new Uint8Array(TILE * TILE)), any = false, lats = el._rLat, lons = el._rLon, has = {};
      own.fill(0);
      for (var ri = 0; ri < st.order.length; ri++) {
        var name = st.order[ri], cov = st.cov[name];
        if (!cov) continue;
        var b = st.pointer.regions[name].box, q = cov.pic.q, cols = cov.pic.cols, rows = cov.pic.rows, s = b[0], w = b[1], n = b[2], e = b[3], ppd = RADAR_PPD.t0;
        for (var y = 0; y < TILE; y++) {
          var lat = lats[y];
          if (lat < s || lat >= n) continue;
          var r = Math.floor((n - lat) * ppd);
          if (r < 0 || r >= rows) continue;
          for (var x = 0; x < TILE; x++) {
            var i = y * TILE + x;
            if (own[i]) continue;
            var lon = lons[x];
            if (lon < w || lon >= e) continue;
            var c = Math.floor((lon - w) * ppd);
            if (c >= 0 && c < cols && q[r * cols + c] !== 0) { own[i] = ri + 1; any = true; has[ri + 1] = true; }
          }
        }
      }
      el._rOwnRev = st.covRev; el._rAny = any; el._rHas = has;
      return own;
    },
    // A tier's picture of a cell at this frame, when it may be drawn now (paused: landed; playing: its whole stack is in).
    _usable: function (rec, region, tier, cell) {
      var st = this.store;
      if (!st.listed(rec, region, tier, cell)) return null;
      if (this.playing && !st.stackReady(region, tier, cell)) return null;
      return st.pics.get(st.key(rec.t, region, tier, cell)) || null;
    },
    // The pieces of this tile at this step: rectangles of tile pixels, each with the finest picture that may be drawn there.
    _pieces: function (el, coords, own) {
      var st = this.store, out = [], want = radarWantTier(coords.z), rects = el._rRects || (el._rRects = Object.create(null));
      function rectOf(lat0, lon0, cell) {
        var k = lat0 + '_' + lon0 + '_' + cell;
        if (rects[k] === undefined) rects[k] = radarCellRect(el, lat0, lon0, cell);
        return rects[k];
      }
      function piece(rec, who, region, tier, pic, rect, lat0, lon0) {
        var def = st.pointer.regions[region], b = def.box, t0 = tier === 't0', ppd = RADAR_PPD[tier];
        out.push({ rec: rec, region: region, own: who, pic: pic, ppd: ppd, snap: def.scale === 'jma8' ? RADAR_JMA8_SNAP : null, typed: radarTypedDef(st, def), x0: rect.x0, x1: rect.x1, y0: rect.y0, y1: rect.y1,
          top: t0 ? b[2] : lat0 + RADAR_CELL[tier], ay: t0 ? -0.5 : 0.5, left: t0 ? b[1] : lon0, ax: t0 ? -0.5 : 0.5 });
      }
      for (var ri = 0; ri < st.order.length; ri++) {
        var region = st.order[ri], who = ri + 1;
        if (!el._rHas[who]) continue;
        var rec = st.resolve(region, this.idx);
        if (!rec) continue;
        var t0 = st.pics.get(st.key(rec.t, region, 't0')), whole = { x0: 0, x1: TILE, y0: 0, y1: TILE };
        if (!t0) continue;
        if (want === 0) { piece(rec, who, region, 't0', t0, whole); continue; }
        var cell = want === 2 ? RADAR_CELL.t2 : RADAR_CELL.t1, names = el._rCells || (el._rCells = cellsForTile(coords, cell, 0));
        for (var n = 0; n < names.length; n++) {
          var cut = names[n].split('_'), lat0 = +cut[0], lon0 = +cut[1], rect = rectOf(lat0, lon0, cell);
          if (!rect) continue;
          var pic = null, tier = 't0', plat = lat0, plon = lon0;
          if (want === 2) pic = this._usable(rec, region, 't2', names[n]);
          if (pic) tier = 't2';
          else {
            plat = Math.floor(lat0 / RADAR_CELL.t1) * RADAR_CELL.t1; plon = Math.floor(lon0 / RADAR_CELL.t1) * RADAR_CELL.t1;
            pic = this._usable(rec, region, 't1', plat + '_' + plon);
            if (pic) tier = 't1';
          }
          piece(rec, who, region, tier, pic || t0, rect, plat, plon);
        }
      }
      return out;
    },
    _draw: function (el, coords) {
      el._ovGen = this._gen;
      var st = this.store, ctx;
      if (!st || !st.pointer || this.idx < 0) {
        if (el._rShown) { ctx = el.getContext('2d'); ctx.clearRect(0, 0, TILE, TILE); el._rShown = false; }
        el._rPieces = null;
        return;
      }
      radarTileGeom(el, coords);
      var own = this._owners(el);
      if (!el._rAny) {                                       // nothing here is covered: nothing is drawn
        if (el._rShown) { ctx = el.getContext('2d'); ctx.clearRect(0, 0, TILE, TILE); el._rShown = false; }
        el._rPieces = null; el._rEcho = false; el._rCov = st.covRev;
        return;
      }
      var pieces = this._pieces(el, coords, own), code = this._code;
      code.fill(0);
      var echo = radarSample(pieces, el, own, 0, TILE, 0, TILE, code);
      el._rPieces = pieces;
      if (!echo && !el._rEcho && el._rCov === st.covRev && el._rShown) return;   // dry stays dry: the tint on the canvas is right
      ctx = el.getContext('2d');
      var img = this._img || (this._img = ctx.createImageData(TILE, TILE)), d = img.data, lut = this._lut, tr = RADAR_TINT[0], tg = RADAR_TINT[1], tb = RADAR_TINT[2], ta = RADAR_TINT[3];
      for (var i = 0, k = 0; i < TILE * TILE; i++, k += 4) {
        var c = code[i];
        if (c > 0) { var j = Math.round(c * 2) * 4; d[k] = lut[j]; d[k + 1] = lut[j + 1]; d[k + 2] = lut[j + 2]; d[k + 3] = lut[j + 3]; }
        else if (own[i]) { d[k] = tr; d[k + 1] = tg; d[k + 2] = tb; d[k + 3] = ta; }
        else d[k + 3] = 0;
      }
      ctx.putImageData(img, 0, 0);
      el._rEcho = echo > 0; el._rCov = st.covRev; el._rShown = true;
    },
    // What the drawn pixel under (lat, lng) shows: the same pieces and sampler as the tile. {kind: 'none'} no coverage,
    // {kind: 'dry'} covered no echo, {kind: 'echo', code, dbz}; with the region and its frame; null without a drawn tile;
    // {kind: 'unknown'} a region's coverage is not in yet, {kind: 'nopic'} covered, but no picture for the shown time.
    readoutAt: function (lat, lng, z) {
      var p = pixelOf(lat, lng, z), t = this._tiles[p.x + ':' + p.y + ':' + p.z], el = t && t.el;
      if (!el || !el._rOwn || el._rOwnRev !== this.store.covRev) return null;   // the tile's owners are an older pointer's: its redraw is on the way
      var i = p.py * TILE + p.px, who = el._rOwn[i];
      if (!who) return { kind: this.store.coverageDue(lat, lng) ? 'unknown' : 'none' };
      var region = this.store.order[who - 1], rec = null, pcs = el._rPieces || [];
      for (var n = 0; n < pcs.length; n++) if (pcs[n].own === who && p.px >= pcs[n].x0 && p.px < pcs[n].x1 && p.py >= pcs[n].y0 && p.py < pcs[n].y1) { rec = pcs[n].rec; break; }
      if (!rec) return { kind: 'nopic', region: region, rec: null };
      this._code[i] = 0;
      if (!radarSample(pcs, el, el._rOwn, p.px, p.px + 1, p.py, p.py + 1, this._code)) return { kind: 'dry', region: region, rec: rec };
      var c = this._code[i], ty = Math.floor(c / RADAR_TYPE_BASE), rc = c - ty * RADAR_TYPE_BASE;
      return { kind: 'echo', code: rc, type: ty, dbz: radarDbz(Math.round(rc * 2) / 2), region: region, rec: rec };
    }
  });
  // The loop: opens paused on the newest frame, never plays by itself; its own play state, cadence and paint gate, none of them the
  // model's. A new slot joins once what the view needs of it has landed (the picture never goes blank for it).
  function RadarPlayer(ov, store, layer) {
    this.ov = ov; this.store = store; this.layer = layer;
    this.loopH = radarLoopOf(saved().radarLoop);
    this.frames = []; this.idx = 0; this.playing = false; this.wasPlaying = false; this.timer = null; this.gen = 0; this.due = 0;
    this.joined = null; this.pending = null; this.waitFrom = 0; this.uiTimer = null; this.drawQ = null; this._paint = null; this.holdFrom = 0; this.fromStart = false;
  }
  RadarPlayer.prototype.view = function () { return radarViewOf(this.ov.map, this.layer); };
  RadarPlayer.prototype.start = function () {
    var self = this;
    this.store.onPointer = function () { self._onPointer(); };
    this.store.onLanding = function (it) { self._onLanding(it); };
    var all = this.store.frames;
    this.joined = all.length ? all[all.length - 1].t : null;
    this._frames(true);
    this._plan();
    this.layer.setStep(this.frames.length ? this.idx : -1, false, false);
    this.store.start();
    this.uiTimer = setInterval(function () { self.ov._radarUI(); }, RADAR_UI_MS);
  };
  RadarPlayer.prototype.stop = function () {
    this.pause();
    if (this.uiTimer) { clearInterval(this.uiTimer); this.uiTimer = null; }
    if (this.drawQ !== null) { this._cancelFrame(this.drawQ); this.drawQ = null; }
    this.store.stop();
  };
  RadarPlayer.prototype._cancelFrame = function (h) { if (typeof cancelAnimationFrame === 'function') cancelAnimationFrame(h); clearTimeout(h); };
  // The frames of the loop up to the joined slot; the shown slot stays shown (a retired one: the next), the newest stays newest.
  RadarPlayer.prototype._frames = function (toNewest) {
    var cur = this.frames[this.idx], wasNewest = this.idx === this.frames.length - 1;
    this.frames = radarLoopFrames(this.store.frames, this.loopH, this.joined);
    var n = this.frames.length, i = n - 1;
    if (!toNewest && cur && !(wasNewest && !this.playing)) {
      for (i = 0; i < n && this.frames[i].t < cur.t; i++) { /* the first at or after the shown slot */ }
      if (i >= n) i = n - 1;
    }
    this.idx = Math.max(0, i);
  };
  RadarPlayer.prototype._plan = function () { this.store.plan(this.view(), this.frames, this.idx, this.pending); };
  RadarPlayer.prototype.viewChanged = function () { this._plan(); this._tryJoin(); this.ov._radarUI(); };
  RadarPlayer.prototype._stepKey = function () { var f = this.frames[this.idx]; return this.idx + '|' + (f ? f.t : ''); };
  RadarPlayer.prototype._onPointer = function () {
    var all = this.store.frames, newest = all.length ? all[all.length - 1] : null;
    if (this.pending && !this.store.index[this.pending.t]) this.pending = null;   // the slot was withdrawn
    if (newest && (!this.joined || newest.t > this.joined)) this.pending = newest;
    else if (!newest) this.pending = null;
    if (this.joined && !this.store.index[this.joined]) this.joined = newest ? newest.t : null;   // the joined slot left the pointer
    var before = this._stepKey();
    this._frames(false);
    this._plan();
    var moved = this.store.regionsMoved; this.store.regionsMoved = false;
    if ((!this._tryJoin(before) && before !== this._stepKey()) || moved) this._show();
    this.ov._radarUI();
  };
  RadarPlayer.prototype._tryJoin = function (prevKey) {
    var p = this.pending;
    if (!p || !this.store.frameReady(p, this.view(), true)) return false;
    this.joined = p.t; this.pending = null;
    var before = prevKey || this._stepKey();
    this._frames(false);
    this._plan();
    if (before !== this._stepKey()) this._show();
    this.ov._radarUI();
    return true;
  };
  RadarPlayer.prototype._show = function () { this.layer.setStep(this.frames.length ? this.idx : -1, this.playing && !this.holdFrom, true); if (this.ov.readout) this.ov.readout.hidden = true; };
  RadarPlayer.prototype._onLanding = function (it) {
    var self = this;
    this._tryJoin();
    var cur = this.frames[this.idx];
    if (!cur) return;
    var near = false;
    for (var j = this.idx; j >= 0 && j >= this.idx - RADAR_HOLD_STEPS; j--) if (this.frames[j] && this.frames[j].t === it.slot) near = true;
    var drawn = this.store.resolve(it.region, this.idx);
    if (drawn && drawn.t === it.slot) near = true;
    if (!near && this.store.cov[it.region] && this.store.cov[it.region].slot !== it.slot) return;
    if (!near && !this.store.cov[it.region]) return;
    if (this.drawQ !== null) return;
    var run = function () { self.drawQ = null; if (self.layer.store) self.layer._redraw(true); self.ov._radarUI(); };
    this.drawQ = typeof requestAnimationFrame === 'function' ? requestAnimationFrame(run) : setTimeout(run, 16);
  };
  RadarPlayer.prototype.play = function (resume) {
    if (this.playing || this.frames.length < 2) return;
    this.playing = true; this.gen++; this.due = Date.now(); this.waitFrom = 0;
    if (!resume) this._plan();                                // what fits is known better now than at the first plan
    this.holdFrom = resume || this.store.stacksReady(this.view()) ? 0 : Date.now();   // the frame on the map stays, at its tier, while the stacks come in
    if (!resume && this.idx === this.frames.length - 1) {                 // Play from the newest frame starts the loop at its start
      if (this.holdFrom) this.fromStart = true; else this.idx = 0;        // after the hold: frame 0's fine files may not be in yet
    }
    this._show();
    this._next(this.gen);
    this.ov._radarUI();
  };
  RadarPlayer.prototype.pause = function () {
    this.playing = false; this.gen++; this.holdFrom = 0; this.fromStart = false;
    if (this.timer) { clearTimeout(this.timer); this.timer = null; }
    if (this._paint) { this._paint(); this._paint = null; }
    if (this.layer && this.layer.store) { this.layer.playing = false; }
  };
  RadarPlayer.prototype.toggle = function () { if (this.playing) { this.pause(); this._plan(); this.layer._redraw(true); } else this.play(); this.ov._radarUI(); };
  RadarPlayer.prototype.speedChanged = function () { this.due = Date.now(); };
  RadarPlayer.prototype._interval = function () {
    var iv = 1000 / (BASE_FPS * speedOf(this.ov.speed));
    return this.idx === this.frames.length - 1 ? iv + RADAR_END_HOLD_MS : iv;
  };
  // The next step after the browser has rendered this one (a paint gate of its own: at most PAINT_FALLBACK_MS), on a drift-free clock.
  RadarPlayer.prototype._next = function (gen) {
    var self = this;
    this.due = Math.max(this.due + this._interval(), Date.now());
    var go = function () {
      if (gen !== self.gen) return;
      self.timer = setTimeout(function () { self.timer = null; self._tick(gen); }, Math.max(0, self.due - Date.now()));
    };
    if (typeof requestAnimationFrame !== 'function') { go(); return; }
    var fired = false, once = function () {
      if (fired) return;
      fired = true; self._paint = null;
      clearTimeout(fallback); if (raf !== null && typeof cancelAnimationFrame === 'function') cancelAnimationFrame(raf);
      setTimeout(go, 0);
    };
    var fallback = setTimeout(once, PAINT_FALLBACK_MS), raf = requestAnimationFrame(once);
    this._paint = function () { fired = true; clearTimeout(fallback); if (typeof cancelAnimationFrame === 'function') cancelAnimationFrame(raf); };
  };
  RadarPlayer.prototype._tick = function (gen) {
    var self = this;
    if (gen !== this.gen || !this.playing) return;
    var n = this.frames.length;
    if (n < 2) { this.pause(); this.ov._radarUI(); return; }
    if (this.holdFrom) {
      if (Date.now() - this.holdFrom < RADAR_PLAY_HOLD_MS && !this.store.stacksReady(this.view())) { this.timer = setTimeout(function () { self.timer = null; self._tick(gen); }, 60); return; }
      this.holdFrom = 0;
      if (this.fromStart) {                                               // the hold is over: now to the loop's start
        this.fromStart = false; this.idx = 0;
        this._show(); this.ov._radarUI(); this._next(gen);
        return;
      }
    }
    var next = (this.idx + 1) % n;
    if (!this.store.frameReady(this.frames[next], this.view(), false)) {   // its pictures are on the way: a short wait, then on with what there is
      if (!this.waitFrom) this.waitFrom = Date.now();
      if (Date.now() - this.waitFrom < RADAR_STEP_WAIT_MS) { this.timer = setTimeout(function () { self.timer = null; self._tick(gen); }, 60); return; }
    }
    this.waitFrom = 0; this.idx = next;
    this._show();
    this.ov._radarUI();
    this._next(gen);
  };
  // The slider: a viewer's choice stops the loop and shows that frame, with the view's files for it first.
  RadarPlayer.prototype.scrub = function (i) {
    if (!this.frames[i]) return;
    var was = this.playing;
    if (was) this.pause();
    this.idx = i; this.layer.playing = false;
    this._show();
    this._plan();
    this.ov._radarUI();
  };
  RadarPlayer.prototype.setLoop = function (h) {
    h = radarLoopOf(h);
    if (h === this.loopH) return;
    this.loopH = h; save({ radarLoop: h });
    this._frames(false);
    this._plan();
    if (this.playing && !this.store.stacksReady(this.view())) this.holdFrom = Date.now();
    this._show();
    this.ov._radarUI();
  };
  RadarPlayer.prototype.visibility = function (hidden) {
    if (hidden) { this.wasPlaying = this.playing; if (this.playing) this.pause(); this.store.hide(); return; }
    this.store.show();
    if (this.wasPlaying) { this.wasPlaying = false; this.play(true); }
  };

  // ---- bathymetry (plan section 93b): depth tiles, the store, the sampler, shading, contours, the layer ----
  // A data tile (tools/bathy/bathy_tiles.py) is one 8-bit grey PNG of 258 x 774: the depth code's high byte (rows 0-257),
  // its low byte (258-515) and a source/flag byte (516-773), each 256 px + a 1-px apron. Node j is centred at tile pixel
  // j - 0.5. Code 0 = no water; 1..41001 = -10..400 m in 1 cm, ..62001 = 400..2500 m in 10 cm, ..65401 = 2500..11000 m in 2.5 m.
  var BATHY_SIDE = 258, BATHY_ROWS = 774, BATHY_PLANE = BATHY_SIDE * BATHY_SIDE;
  var BATHY_C1 = 41001, BATHY_C2 = 62001, BATHY_C3 = 65401;
  var BATHY_SRC = 0x1F, BATHY_FILLED = 0x20, BATHY_BLENDED = 0x40, BATHY_COARSE = 0x80;
  var BATHY_SHALLOW = 30;                                  // GEBCO keeps out of shallower water while a region's data there is still on the way
  var BATHY_GEBCO_EX = 3;                                  // GEBCO's hillshade exaggeration at most this: its 450-m slopes stay calm beside finer grids (G61 K1)
  var BATHY_SHADE_FINE = 0.6, BATHY_SHADE_COARSE = 0.35;   // how much of the hillshade the colour takes: survey grids / GEBCO and other coarse pixels
  var BATHY_DEEP = [200, 1000];                            // survey water deeper than 200 m eases to GEBCO's exaggeration cap and weight, fully past 1,000 m (G61 re-check R-1)
  var BATHY_APRON = 3;                                     // lattice pixels sampled past the map tile on each side (two smoothing passes + the contour's neighbours: G61 A4)
  var BATHY_DONE_MS = 2000;                                // a tile tells the map it is ready after its first full draw, or after this at most (no blank on zoom: G61 B-8)
  var MAX_BATHY_BYTES = 32 * 1024 * 1024;                  // decoded tiles kept (LRU); the ones in view never go
  var MAX_BATHY_INFLIGHT = 4;
  var MAX_BATHY_POINTER = 64 * 1024, MAX_BATHY_INDEX = 1024 * 1024, MAX_BATHY_IDX = 64 * 1024;
  var BATHY_RETRY_MS = 60 * 1000, BATHY_STALL_MS = 15000;
  var BATHY_ENTRY_BYTES = BATHY_SIDE * BATHY_ROWS + BATHY_PLANE;   // the decoded planes + the shade made from them
  var BATHY_NAME = /^[a-z0-9_-]{1,32}$/;
  var BATHY_VIEW_MS = 300;                                  // the sources line and the credit follow the view this long after a redraw
  var BATHY_HEAD = 'Bathymetry: water depth';
  var BATHY_NOTE = 'Approximate; not for navigation. Depth below mean sea level.';
  // A source's short name for the credit ("NOAA", "GEBCO": the credit's first word unless the index names one) and its
  // label for the readout ("NOAA CUDEM 2021").
  function bathyShort(s) { return String((s && (s.short || String(s.credit || s.name || '').split(/[\s,]+/)[0])) || 'source'); }
  // How far a survey pixel at depth d (m) is eased toward GEBCO's look: 0 to 200 m, 1 past 1,000 m, by log depth between, so
  // the survey's footprint never stands out as a rugged box in deep water beside calm GEBCO (G61 re-check R-1).
  function bathyDeep(d) {
    if (!(d > BATHY_DEEP[0])) return 0;
    return d >= BATHY_DEEP[1] ? 1 : Math.log(d / BATHY_DEEP[0]) / Math.log(BATHY_DEEP[1] / BATHY_DEEP[0]);
  }
  function bathyNum(v, whole) {
    var s = whole || Math.round(v * 10) / 10 >= 100 ? String(Math.round(v)) : v.toFixed(1), i = s.indexOf('.'), a = i < 0 ? s : s.slice(0, i);
    return a.replace(/\B(?=(\d{3})+$)/g, ',') + (i < 0 ? '' : s.slice(i));
  }
  function bathyLabel(s) {
    var n = String(s.label || s.name || '').replace(/\bNCEI\s+/, '').replace(/\s+(1\/\d.*|topobathy.*)$/i, '').replace(/_/g, ' ').trim();
    return (n + (s.year && n.indexOf(String(s.year)) < 0 ? ' ' + s.year : '')).trim();
  }
  function bathyDepths() {
    var t = new Float32Array(65536);
    for (var c = 0; c < 65536; c++) {
      t[c] = c === 0 || c > BATHY_C3 ? NaN : c <= BATHY_C1 ? (c - 1) / 100 - 10 : c <= BATHY_C2 ? 400 + (c - BATHY_C1) / 10 : 2500 + (c - BATHY_C2) * 2.5;
    }
    return t;
  }
  var BATHY_DEPTH = bathyDepths();
  // The job's encoder, for tests and the readout's step (round trip within half a step).
  function bathyCode(d) {
    if (!(d === d)) return 0;
    var v = Math.max(-10, Math.min(11000, d)), c = v <= 400 ? 1 + Math.round((v + 10) * 100) : v <= 2500 ? BATHY_C1 + Math.round((v - 400) * 10) : BATHY_C2 + Math.round((v - 2500) / 2.5);
    return Math.max(1, Math.min(BATHY_C3, c));
  }
  function b64Bytes(s) {
    var bin = atob(s), out = new Uint8Array(bin.length);
    for (var i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  }
  function bitAt(b, i) { return (b[i >> 3] >> (i & 7)) & 1; }
  function bathyInt(v, lo, hi) { return Number.isInteger(v) && v >= lo && v <= hi; }
  function bathyBox(b) {
    return Array.isArray(b) && b.length === 4 && b.every(function (v) { return typeof v === 'number' && isFinite(v); }) &&
      b[0] >= -180 && b[2] <= 180 && b[0] < b[2] && b[1] >= -90 && b[3] <= 90 && b[1] < b[3];
  }
  // regions.json -> [{name, box [W, S, E, N], prio, v, zmin, zmax}] (the regional ones by priority, highest first, then by
  // name; gebco last), or null when the file is not one the client can act on.
  function validBathyRegions(doc) {
    if (!doc || doc.format !== 'bathy-regions-v1' || !doc.regions || typeof doc.regions !== 'object' || Array.isArray(doc.regions)) return null;
    var out = [];
    for (var name in doc.regions) {
      if (!Object.prototype.hasOwnProperty.call(doc.regions, name)) continue;
      var r = doc.regions[name];
      if (!BATHY_NAME.test(name) || !r || typeof r !== 'object' || !bathyBox(r.box) || typeof r.prio !== 'number' || !isFinite(r.prio) ||
        !bathyInt(r.v, 1, 9999) || !bathyInt(r.zmin, 0, 20) || !bathyInt(r.zmax, 0, 20) || r.zmin > r.zmax) return null;
      out.push({ name: name, box: r.box.slice(), prio: r.prio, v: r.v, zmin: r.zmin, zmax: r.zmax });
    }
    if (out.length > 64) return null;
    out.sort(function (a, b) { return (a.name === 'gebco') - (b.name === 'gebco') || b.prio - a.prio || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0); });
    return out;
  }
  // A region's index.json -> {levels {z: {x0, y0, w, h, bits}}, idx {dir, z0, z1, bytes} | null, t10 {"x_y": true},
  // sources {id: {...}}, landId}; null when it does not belong to the region named in regions.json or cannot be read.
  // landId (data v2): the source id that marks the survey's land (depth code 0 with it = land; code 0 with id 0 = outside
  // the survey); 0 in a v1 index (code 0 is both, so GEBCO keeps out of the shallows wherever the region has tiles).
  function validBathyIndex(doc, reg) {
    if (!doc || doc.format !== 'bathy-v1' || doc.region !== reg.name || doc.version !== reg.v || doc.zmin !== reg.zmin || doc.zmax !== reg.zmax) return null;
    if (!doc.levels || typeof doc.levels !== 'object' || Array.isArray(doc.levels) || !doc.sources || typeof doc.sources !== 'object' || Array.isArray(doc.sources)) return null;
    var levels = {}, top = Math.min(reg.zmax, 10);
    for (var z = reg.zmin; z <= top; z++) {
      var lv = doc.levels[String(z)];
      if (lv === undefined) continue;                      // no tile at this zoom
      if (!lv || !bathyInt(lv.x0, 0, 1 << 20) || !bathyInt(lv.y0, 0, 1 << 20) || !bathyInt(lv.w, 1, 1 << 20) || !bathyInt(lv.h, 1, 1 << 20) ||
        typeof lv.bits !== 'string' || lv.w * lv.h > 1 << 22) return null;
      var bits;
      try { bits = b64Bytes(lv.bits); } catch (e) { return null; }
      if (bits.length < Math.ceil(lv.w * lv.h / 8)) return null;
      levels[z] = { x0: lv.x0, y0: lv.y0, w: lv.w, h: lv.h, bits: bits };
    }
    var idx = null, t10 = {};
    if (reg.zmax > 10) {
      var ix = doc.idx;
      if (!ix || typeof ix.dir !== 'string' || !/^[A-Za-z0-9_-]{1,32}$/.test(ix.dir) || !Array.isArray(ix.zooms) || ix.zooms.length !== 2 ||
        ix.zooms[0] !== 11 || ix.zooms[1] !== reg.zmax || !Array.isArray(doc.t10) || doc.t10.length > 1 << 16) return null;
      var bytes = 0;
      for (var zz = 11; zz <= reg.zmax; zz++) { var k = 1 << (zz - 10); bytes += Math.ceil(k * k / 8); }
      idx = { dir: ix.dir, z0: 11, z1: reg.zmax, bytes: bytes };
      for (var i = 0; i < doc.t10.length; i++) {
        if (typeof doc.t10[i] !== 'string' || !/^\d{1,7}_\d{1,7}$/.test(doc.t10[i])) return null;
        t10[doc.t10[i]] = true;
      }
    }
    var sources = {};
    for (var id in doc.sources) {
      var s = doc.sources[id];
      if (!/^\d{1,2}$/.test(id) || +id > BATHY_SRC || !s || typeof s !== 'object') return null;
      sources[+id] = { name: String(s.name || ''), year: String(s.year || ''), res_m: typeof s.res_m === 'number' ? s.res_m : null,
        credit: String(s.credit || ''), datum: String(s.datum || ''), licence: String(s.licence || '') };
    }
    var landId = 0;
    if (doc.land_id !== undefined) {
      if (!bathyInt(doc.land_id, 1, BATHY_SRC)) return null;
      landId = doc.land_id;
    }
    return { levels: levels, idx: idx, t10: t10, sources: sources, landId: landId };
  }
  // The bitsets of one z10 cell's idx file: [z11, z12, ... zmax], each k^2 bits row-major (k = 2^(z - 10)); null if it is not one.
  function parseBathyIdx(buf, ix) {
    var b = new Uint8Array(buf), out = [], at = 0;
    if (b.length !== ix.bytes) return null;
    for (var z = ix.z0; z <= ix.z1; z++) { var k = 1 << (z - 10), n = Math.ceil(k * k / 8); out.push(b.subarray(at, at + n)); at += n; }
    return out;
  }

  function bathyStore(root) { return new BathyStore(root + '/' + BATHY_PREFIX); }
  // The bathymetry tiles: regions.json first, then each region's index the first time a tile in its box is drawn, then the
  // presence files and tiles the tiles in view ask for. Keys carry the region's version, so a rebuilt region never mixes.
  // Never rejects; a failed request waits BATHY_RETRY_MS (404 / a bad file: for good) and the layer asks again when it is due.
  function BathyStore(base) {
    this.base = base; this.status = 'idle'; this.regions = null; this.loading = null; this.failedAt = 0; this.loadAbort = null;
    this.index = {}; this.idx = new Map(); this.tiles = new Map(); this.bytes = 0;
    this.inflight = {}; this.queue = []; this.queued = {}; this.fails = {}; this.retryTimer = null; this.retryAt = 0;
    this.onChange = null; this.wanted = null; this.inUse = null;
  }
  BathyStore.prototype.retryMs = BATHY_RETRY_MS;
  BathyStore.prototype.stallMs = BATHY_STALL_MS;
  BathyStore.prototype.load = function (force) {
    var self = this;
    if (this.status === 'ok') return Promise.resolve(this);
    if (this.loading) return this.loading;
    if (this.status === 'failed' && !force && Date.now() - this.failedAt < this.retryMs) return Promise.resolve(null);
    this.status = 'loading';
    var ctrl = this.loadAbort = new AbortController(), timer = setTimeout(function () { ctrl.abort(); }, this.stallMs);
    this.loading = fetch(this.base + '/regions.json', { signal: ctrl.signal, mode: 'cors', cache: 'no-cache' }).then(function (r) {
      if (!r.ok) throw new Error('bathy ' + r.status);
      if (Number(r.headers.get('content-length') || 0) > MAX_BATHY_POINTER) throw new Error('unsupported bathy regions');
      return r.json();
    }).then(function (doc) {
      clearTimeout(timer);
      if (self.loadAbort !== ctrl) throw abortError();
      var regs = validBathyRegions(doc);
      if (!regs) throw new Error('unsupported bathy regions');
      self.regions = regs; self.status = 'ok'; self.loading = null; self.loadAbort = null;
      return self;
    }).catch(function () {
      clearTimeout(timer);
      if (self.loadAbort === ctrl) { self.loading = null; self.loadAbort = null; self.status = 'failed'; self.failedAt = Date.now(); }
      return null;
    });
    return this.loading;
  };
  BathyStore.prototype.regionKey = function (reg) { return reg.name + '@' + reg.v; };
  BathyStore.prototype._url = function (reg, path) { return this.base + '/' + reg.name + '/v' + reg.v + '/' + path; };
  // A failure that is still cooling down (true), for good ('final'), or none (false).
  BathyStore.prototype._failed = function (key) {
    var f = this.fails[key];
    if (!f) return false;
    if (f.final) return 'final';
    if (Date.now() - f.at < this.retryMs) return true;
    delete this.fails[key];
    return false;
  };
  // The region's index: the parsed index, null (none for good / cooling down), or undefined (asked for; not in yet).
  BathyStore.prototype.indexOf = function (reg, need) {
    var key = 'i:' + this.regionKey(reg), have = this.index[key];
    if (have) return have;
    if (need) need.push(key);
    var f = this._failed(key);
    if (f) return null;
    this._ask(key, { kind: 'index', reg: reg });
    return undefined;
  };
  // Does the region hold tile z/x/y (x wrapped)? true / false, or undefined while its presence file is on the way.
  BathyStore.prototype.present = function (reg, ix, z, x, y, need) {
    if (z < reg.zmin || z > reg.zmax) return false;
    var n = 1 << z, lv;
    if (z <= 10) {
      lv = ix.levels[z];
      if (!lv) return false;
      var dx = ((x - lv.x0) % n + n) % n, dy = y - lv.y0;
      if (dx >= lv.w || dy < 0 || dy >= lv.h) return false;
      return bitAt(lv.bits, dy * lv.w + dx) === 1;
    }
    if (!ix.idx) return false;
    var s = 1 << (z - 10), x10 = Math.floor(x / s), y10 = Math.floor(y / s), cell = x10 + '_' + y10;
    if (!ix.t10[cell]) return false;
    var key = 'f:' + this.regionKey(reg) + '/' + cell, have = this.idx.get(key);
    if (have === undefined) {
      if (need) need.push(key);
      var f = this._failed(key);
      if (f) return false;                                  // no presence file: only the zooms the index itself lists
      this._ask(key, { kind: 'idx', reg: reg, ix: ix.idx, cell: cell });
      return undefined;
    }
    var bits = have[z - 11];
    return bitAt(bits, (y - y10 * s) * s + (x - x10 * s)) === 1;
  };
  BathyStore.prototype.tileKey = function (reg, z, x, y) { return 't:' + this.regionKey(reg) + '/' + z + '/' + x + '/' + y; };
  // A tile's decoded planes: the entry, null (none for good / cooling down), or undefined (asked for). peek: never asks.
  BathyStore.prototype.tile = function (reg, z, x, y, need, peek) {
    var key = this.tileKey(reg, z, x, y), e = this.tiles.get(key);
    if (e) { this.tiles.delete(key); this.tiles.set(key, e); return e; }   // LRU touch
    if (peek) return undefined;
    if (need) need.push(key);
    if (this._failed(key)) return null;
    this._ask(key, { kind: 'tile', reg: reg, z: z, x: x, y: y });
    return undefined;
  };
  BathyStore.prototype.held = function (key) { return this.tiles.get(key) || null; };
  // Is any of these keys still on the way (asked, in flight or queued)? A map tile tells the map it is ready only when none is.
  BathyStore.prototype.waiting = function (keys) {
    if (!keys) return false;
    for (var i = 0; i < keys.length; i++) if (this.inflight[keys[i]] || this.queued[keys[i]]) return true;
    return false;
  };
  // Any request on the way at all (the panel's "loading…").
  BathyStore.prototype.busy = function () { return this.queue.length > 0 || Object.keys(this.inflight).length > 0; };
  // Did this key's last request fail, and is it still cooling down (asked again when the cooldown ends)?
  BathyStore.prototype.cooling = function (key) { return this._failed(key) === true; };
  BathyStore.prototype._ask = function (key, job) {
    if (this.inflight[key] || this.queued[key]) return;
    job.key = key; this.queued[key] = job; this.queue.push(job);
    this._pump();
  };
  BathyStore.prototype._pump = function () {
    var self = this, live = Object.keys(this.inflight).length;
    if (this.wanted && this.queue.length) {              // only what a tile still on the map asks for
      var want = this.wanted(), keep = [];
      for (var i = 0; i < this.queue.length; i++) { var j = this.queue[i]; if (want.has(j.key)) keep.push(j); else delete this.queued[j.key]; }
      this.queue = keep;
    }
    while (live < MAX_BATHY_INFLIGHT && this.queue.length) {
      var job = this.queue.shift();
      delete this.queued[job.key];
      this._start(job); live++;
    }
  };
  BathyStore.prototype._start = function (job) {
    var self = this, ctrl = new AbortController(), rec = { ctrl: ctrl, stalled: false, dog: null }, key = job.key;
    var kick = function () { clearTimeout(rec.dog); rec.dog = setTimeout(function () { rec.stalled = true; ctrl.abort(); }, self.stallMs); };
    this.inflight[key] = rec;
    kick();
    var p;
    if (job.kind === 'tile') {
      p = decodeFrame(this._url(job.reg, 't/' + job.z + '/' + job.x + '/' + job.y + '.png'), ctrl.signal, { cols: BATHY_SIDE, rows: BATHY_ROWS }, null, true, kick).then(function (fr) {
        if (!fr || fr.cols !== BATHY_SIDE || fr.rows !== BATHY_ROWS || !fr.q || fr.q.length !== BATHY_SIDE * BATHY_ROWS) throw new Error('bathy decode failed');
        return { q: fr.q, shade: null, dz: job.z, tx: job.x, ty: job.y, reg: job.reg.name };
      });
    } else {
      var path = job.kind === 'index' ? 'index.json' : job.ix.dir + '/' + job.cell + '.bin', cap = job.kind === 'index' ? MAX_BATHY_INDEX : MAX_BATHY_IDX;
      p = fetch(this._url(job.reg, path), { signal: ctrl.signal, mode: 'cors' }).then(function (r) {
        if (!r.ok) throw new Error('bathy ' + r.status);
        if (Number(r.headers.get('content-length') || 0) > cap) throw new Error('bathy decode failed');
        kick();
        return r.body && r.body.getReader ? readBody(r.body.getReader(), kick, cap, 'bathy decode failed') : r.arrayBuffer();
      }).then(function (buf) {
        if (buf.byteLength > cap) throw new Error('bathy decode failed');
        if (job.kind === 'idx') {
          var bits = parseBathyIdx(buf, job.ix);
          if (!bits) throw new Error('bathy decode failed');
          return bits;
        }
        var doc;
        try { doc = JSON.parse(new TextDecoder().decode(buf)); } catch (e) { throw new Error('bathy decode failed'); }
        var ix = validBathyIndex(doc, job.reg);
        if (!ix) throw new Error('bathy decode failed');
        return ix;
      });
    }
    p.then(function (val) {
      clearTimeout(rec.dog);
      if (self.inflight[key] !== rec) return;              // dropped meanwhile (Off): this answer belongs to nobody
      delete self.inflight[key];
      delete self.fails[key];
      if (job.kind === 'tile') { self.tiles.set(key, val); self.bytes += BATHY_ENTRY_BYTES; self._evict(key); }
      else if (job.kind === 'idx') self.idx.set(key, val);
      else self.index[key] = val;
      self._pump();
      if (self.onChange) self.onChange(key);
    }, function (err) {
      clearTimeout(rec.dog);
      if (self.inflight[key] !== rec) return;
      delete self.inflight[key];
      if (!rec.stalled && ctrl.signal.aborted) { self._pump(); return; }   // our own abort: nothing to remember
      var msg = (err && err.message) || '', final = /\b(404|410)\b/.test(msg) || /decode|unsupported/.test(msg);
      self.fails[key] = { at: Date.now(), final: final };
      if (!final) self._armRetry();
      self._pump();
      if (self.onChange) self.onChange(key);               // the tiles that asked fall back to what they can draw
    });
  };
  // One timer for every failure still cooling down: it fires when the earliest one ends (+50 ms), asks the layer to draw
  // again (the tiles that wanted those files ask once more) and arms itself for the next one (G61 A3).
  BathyStore.prototype._armRetry = function () {
    var self = this, now = Date.now(), next = Infinity;
    for (var k in this.fails) {
      var f = this.fails[k];
      if (f.final) continue;
      var t = f.at + this.retryMs;
      if (t > now && t < next) next = t;
    }
    if (next === Infinity) return;
    if (this.retryTimer && this.retryAt <= next) return;
    if (this.retryTimer) clearTimeout(this.retryTimer);
    this.retryAt = next;
    this.retryTimer = setTimeout(function () {
      self.retryTimer = null; self.retryAt = 0;
      if (self.onChange) self.onChange('*');
      self._armRetry();
    }, next - now + 50);
  };
  // Over the cap the least recently used tiles go, never one a map tile draws, still wants, or that has just landed (else a
  // view needing more than the cap would evict each tile as it lands and ask for it again without end: G61 A1).
  BathyStore.prototype._evict = function (justLanded) {
    if (this.bytes <= MAX_BATHY_BYTES) return;
    var use = this.inUse ? this.inUse() : null, want = this.wanted ? this.wanted() : null;
    for (var it = this.tiles.keys(), k = it.next(); !k.done && this.bytes > MAX_BATHY_BYTES; k = it.next()) {
      if (k.value === justLanded || (use && use.has(k.value)) || (want && want.has(k.value))) continue;
      this.tiles.delete(k.value); this.bytes -= BATHY_ENTRY_BYTES;
    }
  };
  // Off / another field: every request stops; release() also lets the decoded tiles go (the browser cache keeps the files).
  BathyStore.prototype.abortAll = function () {
    for (var k in this.inflight) { var rec = this.inflight[k]; clearTimeout(rec.dog); rec.ctrl.abort(); }
    this.inflight = {}; this.queue = []; this.queued = {};
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
    this.retryAt = 0;
    if (this.loadAbort) { this.loadAbort.abort(); this.loadAbort = null; this.loading = null; if (this.status === 'loading') this.status = 'idle'; }
  };
  BathyStore.prototype.release = function () { this.tiles.clear(); this.bytes = 0; };

  // ---- the sampler and its pieces ----
  // Node coordinate (and weight) of map-tile pixel p (-BATHY_APRON .. TILE-1+BATHY_APRON) along one axis: the pixel's centre
  // at the data zoom, in the data tile's nodes (node j at pixel j - 0.5). Not clamped: a node past the tile's own 0..257 is
  // read from the neighbouring data tile (the sampler), else the own edge stands in. world0: the map tile's first pixel at
  // its own zoom. Entry (p + BATHY_APRON) * 2.
  function bathyAxis(world0, p, s, t, out, o) {
    var u = (world0 + p + 0.5) / s - t * TILE + 0.5, c = Math.floor(u);
    out[o] = c; out[o + 1] = u - c;
  }
  function bathyAxes(world0, s, t) {
    var A = BATHY_APRON, a = new Float64Array((TILE + 2 * A) * 2);
    for (var p = -A; p < TILE + A; p++) bathyAxis(world0, p, s, t, a, (p + A) * 2);
    return a;
  }
  // The held data tiles around (tx, ty) at zoom dz that the axes reach into (index (dy + 1) * 3 + dx + 1; never asked for:
  // they are the neighbouring map tiles' own files), or null when the axes stay inside the tile. A tile not held is left
  // out (the own edge stands in) and its key goes to miss: the map tile is drawn again when it lands.
  function bathyNeighbours(store, reg, dz, tx, ty, ax, ay, use, miss) {
    var A = BATHY_APRON, last = (TILE + 2 * A - 1) * 2, M = BATHY_SIDE - 1;
    var lo = ax[0] < 0, hi = ax[last] + 1 > M, up = ay[0] < 0, dn = ay[last] + 1 > M;
    if (!lo && !hi && !up && !dn) return null;
    var nb = [null, null, null, null, null, null, null, null, null], N = 1 << dz;
    for (var dy = -1; dy <= 1; dy++) {
      if ((dy < 0 && !up) || (dy > 0 && !dn)) continue;
      var yy = ty + dy;
      if (yy < 0 || yy >= N) continue;
      for (var dx = -1; dx <= 1; dx++) {
        if ((!dx && !dy) || (dx < 0 && !lo) || (dx > 0 && !hi)) continue;
        var xx = ((tx + dx) % N + N) % N, e = store.tile(reg, dz, xx, yy, null, true), key = store.tileKey(reg, dz, xx, yy);
        if (e) { nb[(dy + 1) * 3 + dx + 1] = e; use.push(key); }
        else if (miss) miss.push(key);
      }
    }
    return nb;
  }
  // The shade of a data tile (lazy, kept with it): a multiplier per node (byte / 127.5; flat = 1, lit slopes brighter,
  // shadowed darker) from the elevation (-depth) with light from the north-west 45 degrees up; missing nodes 128.
  function bathyShade(e) {
    if (e.shade) return e.shade;
    var q = e.q, S = BATHY_SIDE, out = new Uint8Array(BATHY_PLANE), el = new Float32Array(BATHY_PLANE), D = BATHY_DEPTH;
    for (var i = 0; i < BATHY_PLANE; i++) { var c = (q[i] << 8) | q[i + BATHY_PLANE]; el[i] = -D[c]; }
    var n = TILE * Math.pow(2, e.dz), ex = Math.max(1, Math.min(e.reg === 'gebco' ? BATHY_GEBCO_EX : 20, Math.pow(2, (15 - e.dz) / 2))), lx = -0.5, ly = 0.5, lz = 0.70710678;
    var exC = Math.min(ex, BATHY_GEBCO_EX);                // a survey's deep water eases to GEBCO's cap (bathyDeep)
    for (var r = 0; r < S; r++) {
      var Y = e.ty * TILE + r - 0.5, lat = Math.atan(Math.sinh(Math.PI * (1 - 2 * Y / n))), m = 40075016.686 * Math.cos(lat) / n, row = r * S;
      for (var cc = 0; cc < S; cc++) {
        var i0 = row + cc, z0 = el[i0];
        if (!(z0 === z0)) { out[i0] = 128; continue; }
        var W = cc > 0 ? el[i0 - 1] : NaN, E = cc < S - 1 ? el[i0 + 1] : NaN, N = r > 0 ? el[i0 - S] : NaN, Sd = r < S - 1 ? el[i0 + S] : NaN;
        var gx = E === E && W === W ? (E - W) / (2 * m) : E === E ? (E - z0) / m : W === W ? (z0 - W) / m : 0;
        var gy = N === N && Sd === Sd ? (N - Sd) / (2 * m) : N === N ? (N - z0) / m : Sd === Sd ? (z0 - Sd) / m : 0;
        var exn = ex > exC ? ex - (ex - exC) * bathyDeep(-z0) : ex;
        var nx = -exn * gx, ny = -exn * gy, len = Math.sqrt(nx * nx + ny * ny + 1);
        var mult = (nx * lx + ny * ly + lz) / len / 0.70710678;
        mult = mult < 0 ? 0 : mult > 2 ? 2 : mult;
        out[i0] = Math.round(mult * 127.5);
      }
    }
    e.shade = out;
    return out;
  }
  // Colour position (0..1) of a depth: the knots spend most of the bar on 0-30 m; GEBCO and other coarse pixels never use
  // the shallow colours (a 450-m cell has no reef in it); dry at mean sea level = the lightest colour.
  // PREVIEW ONLY (removed with the owner's pick, G61 re-check R-2): ?ovgebco=depth colours GEBCO's shallows by their own depth.
  var BATHY_GEBCO_DEPTH = (function () { try { return /[?&]ovgebco=depth(?:&|$)/.test(pageQuery()); } catch (e) { return false; } })();
  function bathyPos(d, coarse) {
    if (coarse && d < BATHY_SHALLOW && !BATHY_GEBCO_DEPTH) d = BATHY_SHALLOW;
    if (d <= 0) return 0;
    return through(KNOTS.bathy, d, 0, 1);
  }
  // How much of the hillshade a pixel's colour takes: GEBCO and other coarse pixels 0.35; a survey's 0.6, easing to 0.35 in
  // deep water (bathyDeep).
  function bathyWeight(d, coarse) {
    return coarse ? BATHY_SHADE_COARSE : BATHY_SHADE_FINE - (BATHY_SHADE_FINE - BATHY_SHADE_COARSE) * bathyDeep(d);
  }
  function bathyLut() {
    var out = new Uint8ClampedArray(1024 * 3);
    for (var i = 0; i < 1024; i++) rampAt(RAMPS.bathy, i / 1023, out, i * 3);
    return out;
  }
  // The pieces of map tile coords, best first: per regional box (priority order) the finest zoom it holds there (a held
  // coarser tile of the same region stands in while the finer one loads), then GEBCO at every zoom, so it fills wherever
  // no finer grid reaches (no hard edge where a region's grid ends in deep water; owner, 2026-11-11). A piece:
  // {reg, e, dz, ax, ay, gebco, fade, fall, coarse, landId, nb}. need: every key asked for (the layer's want list); use:
  // the tile keys drawn (neighbours too); miss: neighbours not held (the tile is drawn again when one lands).
  // GEBCO's fall (no water shallower than BATHY_SHALLOW) holds only while a region's data here is still on the way (its
  // index, a presence file, the tile itself, or a failure still cooling down) or a region drawn here has no land marker
  // (a v1 index); otherwise the regions' own land and "outside the survey" codes decide where GEBCO shows (G61 A2, B-1).
  function bathyPieces(store, coords, need, use, miss) {
    var out = [], b = tileBox(coords), z = coords.z, xw = b.xw, y = coords.y, hold = false, regs = store.regions || [];
    var lonW = b.x0 / TILE * 360 - 180, lonE = b.x1 / TILE * 360 - 180;
    var latN = Math.atan(Math.sinh(Math.PI - 2 * Math.PI * b.y0 / TILE)) * 180 / Math.PI, latS = Math.atan(Math.sinh(Math.PI - 2 * Math.PI * b.y1 / TILE)) * 180 / Math.PI;
    function piece(reg, e, dz, gebco, ix) {
      var s = Math.pow(2, z - dz), tx = Math.floor(xw / s), ty = Math.floor(y / s);
      if (e.tx !== tx || e.ty !== ty) return false;           // not this tile's ancestor (cannot happen; a guard)
      use.push(store.tileKey(reg, dz, tx, ty));
      var ax = bathyAxes(xw * TILE, s, tx), ay = bathyAxes(y * TILE, s, ty);
      out.push({ reg: reg, e: e, dz: dz, ax: ax, ay: ay, gebco: gebco, fade: 1, fall: false, coarse: gebco,
        landId: (ix && ix.landId) || 0, nb: bathyNeighbours(store, reg, dz, tx, ty, ax, ay, use, miss) });
      return true;
    }
    for (var r = 0; r < regs.length; r++) {
      var reg = regs[r], bx = reg.box;
      if (reg.name === 'gebco') continue;
      if (z < reg.zmin || bx[3] <= latS || bx[1] >= latN || bx[2] <= lonW || bx[0] >= lonE) continue;
      var ix = store.indexOf(reg, need);
      if (ix === undefined) { hold = true; continue; }        // its index is on the way: GEBCO keeps out of the shallows meanwhile
      if (!ix) { if (store.cooling('i:' + store.regionKey(reg))) hold = true; continue; }
      var top = Math.min(z, reg.zmax), found = -1, pending = false;
      for (var dz = top; dz >= reg.zmin; dz--) {
        var sh = z - dz, pr = store.present(reg, ix, dz, xw >> sh, y >> sh, need);
        if (pr === undefined) { pending = true; continue; }
        if (pr) { found = dz; break; }
      }
      if (found < 0) { if (pending) hold = true; continue; }
      // A finer zoom's presence file still on the way: no coarser tile is fetched for the moment (one held stands in).
      var fx = xw >> (z - found), fy = y >> (z - found);
      var e = store.tile(reg, found, fx, fy, pending ? null : need, pending), drew = false;
      if (pending || (!e && store._failed(store.tileKey(reg, found, fx, fy)) !== 'final')) hold = true;
      if (e) drew = piece(reg, e, found, false, ix);
      else {
        // Still on the way, or its file failed (cooling down, or gone): a held coarser tile of the same region stands in.
        for (var dz2 = found - 1; dz2 >= reg.zmin; dz2--) {
          var sh2 = z - dz2, e2 = store.tile(reg, dz2, xw >> sh2, y >> sh2, null, true);
          if (e2) { drew = piece(reg, e2, dz2, false, ix); break; }
        }
      }
      if (drew && !ix.landId) hold = true;                    // a v1 tile: code 0 is land and "no data" alike
    }
    for (var g = 0; g < regs.length; g++) {
      if (regs[g].name !== 'gebco') continue;
      var gr = regs[g], gx = store.indexOf(gr, need);
      if (!gx) break;
      for (var gz = Math.min(z, gr.zmax); gz >= gr.zmin; gz--) {
        var gs = z - gz;
        if (!store.present(gr, gx, gz, xw >> gs, y >> gs, need)) continue;
        var ge = store.tile(gr, gz, xw >> gs, y >> gs, need);
        if (!ge) {
          for (var gz2 = gz - 1; gz2 >= gr.zmin && !ge; gz2--) {
            var gs2 = z - gz2, ge2 = store.tile(gr, gz2, xw >> gs2, y >> gs2, null, true);
            if (ge2) { piece(gr, ge2, gz2, true, gx); ge = ge2; }
          }
          break;
        }
        piece(gr, ge, gz, true, gx);
        break;
      }
      break;
    }
    for (var k = 0; k < out.length; k++) if (out[k].gebco) out[k].fall = hold;
    return out;
  }
  // THE sampler (tile, contour lattice and readout alike): for map-tile pixels p in [x0, x1), q in [y0, y1)
  // (-BATHY_APRON .. TILE-1+BATHY_APRON), the first piece holding water there: depth (bilinear over the present nodes; under a
  // quarter of the weight present = none), the source/flag byte of the heaviest node, the shade multiplier (bilinear over
  // the same nodes; 1 past the tile, where no colour is drawn) and the piece. Written to index (q + off) * stride + (p + off);
  // depth NaN and piece -1 where none. A piece with a land marker whose nodes are land (half the weight or more with the
  // water's) ends the search: no piece below it is drawn there (GEBCO never lies over a survey's land: G61 B-1). Nodes past
  // the data tile come from its held neighbours (else the own edge). land: GEBCO's mask (>= 128 = land), indexed by the pixel
  // clamped into the tile. Pixels 0..TILE-1 (the colours and the readout) never need a neighbour.
  function bathySample(pieces, land, x0, x1, y0, y1, off, stride, D, SF, SH, PI) {
    var Q = BATHY_SIDE, P = BATHY_PLANE, DEP = BATHY_DEPTH, A = BATHY_APRON, M = Q - 1, SRC = BATHY_SRC;
    for (var qq = y0; qq < y1; qq++) {
      for (var pp = x0; pp < x1; pp++) {
        var o = (qq + off) * stride + (pp + off), hit = -1;
        for (var k = 0; k < pieces.length; k++) {
          var pc = pieces[k];
          if (pc.gebco && land) {
            var lx = pp < 0 ? 0 : pp > TILE - 1 ? TILE - 1 : pp, ly = qq < 0 ? 0 : qq > TILE - 1 ? TILE - 1 : qq;
            if (land === LAND_ALL || land[ly * TILE + lx] >= LAND_READOUT) continue;
          }
          var ax = pc.ax, ay = pc.ay, ia = (pp + A) * 2, ib = (qq + A) * 2;
          var c0 = ax[ia], fx = ax[ia + 1], r0 = ay[ib], fy = ay[ib + 1], fast = c0 >= 0 && c0 < M && r0 >= 0 && r0 < M;
          var w = 0, wl = 0, acc = 0, best = -1, sf = 0, sh = 0, landId = pc.landId || 0, shade = pc.e.shade;
          for (var t = 0; t < 4; t++) {
            var f = (t & 1 ? fx : 1 - fx) * (t >> 1 ? fy : 1 - fy);
            if (f <= 0) continue;
            var cc = c0 + (t & 1), rr = r0 + (t >> 1), e = pc.e;
            if (!fast) {
              var nx = cc < 0 ? -1 : cc > M ? 1 : 0, ny = rr < 0 ? -1 : rr > M ? 1 : 0;
              if (nx || ny) {
                var ne = pc.nb ? pc.nb[(ny + 1) * 3 + nx + 1] : null;
                if (ne) { e = ne; cc -= nx * TILE; rr -= ny * TILE; }     // node -1 of a tile = node 255 of the one before it
                else { cc = cc < 0 ? 0 : cc > M ? M : cc; rr = rr < 0 ? 0 : rr > M ? M : rr; }
              }
            }
            var q = e.q, i = rr * Q + cc, code = (q[i] << 8) | q[i + P];
            if (!code) { if (landId && (q[i + 2 * P] & SRC) === landId) wl += f; continue; }
            var d = DEP[code];
            if (!(d === d)) continue;
            w += f; acc += f * d;
            if (fast && shade) sh += f * shade[i];
            if (f > best) { best = f; sf = q[i + 2 * P]; }
          }
          if (w >= 0.25) {
            var dep = acc / w;
            if (pc.gebco && pc.fall && dep < BATHY_SHALLOW) continue;
            D[o] = dep; SF[o] = sf; SH[o] = fast && shade ? sh / w / 127.5 : 1; PI[o] = k; hit = k;
            break;
          }
          if (landId && w + wl >= 0.5) break;                 // the survey's land: nothing here, nothing below it
        }
        if (hit < 0) { D[o] = NaN; SF[o] = 0; SH[o] = 1; PI[o] = -1; }
      }
    }
  }

  // ---- contours: a value -> level map (depth lists by zoom band and unit; levels need not be evenly spaced) ----
  function bathyRange(from, to, step) { var out = []; for (var v = from; v <= to + 1e-9; v += step) out.push(Math.round(v * 100) / 100); return out; }
  var BATHY_LEVELS = {
    'Metric|15': [].concat(bathyRange(1, 10, 1), bathyRange(12, 30, 2), bathyRange(40, 100, 10), bathyRange(150, 500, 50), bathyRange(600, 1000, 100), bathyRange(1500, 11000, 500)),
    'Metric|13': [].concat(bathyRange(2, 10, 2), bathyRange(15, 30, 5), bathyRange(40, 100, 20), [150, 200, 300, 400, 500], bathyRange(1000, 11000, 500)),
    'Metric|11': [5, 10, 15, 20, 30, 50, 75, 100, 200, 300, 500].concat(bathyRange(1000, 11000, 1000)),
    'Metric|10': [10, 20, 50, 100, 200, 500].concat(bathyRange(1000, 11000, 1000)),
    // below zoom 10 only 100 / 500 / 1,000 / 2,000 m (owner, 2026-11-11); the last level lies past any ocean, so no line deeper
    'Metric|0': [100, 500, 1000, 2000, 12000],
    'US|15': [].concat(bathyRange(5, 30, 5), bathyRange(40, 100, 10), bathyRange(150, 1000, 50), bathyRange(1500, 36000, 500)),
    'US|13': [10, 20, 30, 40, 50, 60, 80, 100, 150, 200, 300, 400, 500].concat(bathyRange(1000, 36000, 1000)),
    'US|11': [15, 30, 60, 100, 200, 300, 500].concat(bathyRange(1000, 36000, 1000)),
    'US|10': [30, 100, 300, 600, 1000].concat(bathyRange(2000, 36000, 2000)),
    'US|0': [300, 1500, 3000, 6000, 40000]
  };
  // The levels (display units, with 0 first: no line is drawn at 0, the shoreline) for a tile zoom and unit.
  function bathyLevels(z, unit) {
    var band = z >= 15 ? 15 : z >= 13 ? 13 : z >= 11 ? 11 : z >= 10 ? 10 : 0, key = (unit === 'Metric' ? 'Metric' : 'US') + '|' + band;
    return [0].concat(BATHY_LEVELS[key]);
  }
  // Level coordinate of a display value: k at levels[k], linear between, the last spacing beyond the end; just under 0 for
  // dry ground (no line is drawn at level 0, so the shoreline never carries one).
  function bathyLevel(L, v) {
    if (v <= 0) return Math.max(-0.49, v / (L[1] - L[0]));
    var lo = 0, hi = L.length - 1;
    if (v >= L[hi]) return hi + (v - L[hi]) / (L[hi] - L[hi - 1]);
    while (hi - lo > 1) { var mid = (lo + hi) >> 1; if (L[mid] <= v) lo = mid; else hi = mid; }
    return lo + (v - L[lo]) / (L[lo + 1] - L[lo]);
  }
  // One masked [1,2,1] x [1,2,1] pass over a W x W lattice (NaN = no data stays NaN; a node's mean over its present
  // neighbours) from A into B.
  function bathySmooth(A, B, W) {
    for (var y = 0; y < W; y++) {
      for (var x = 0; x < W; x++) {
        var i = y * W + x, v = A[i];
        if (!(v === v)) { B[i] = NaN; continue; }
        var acc = 0, w = 0;
        for (var dy = -1; dy <= 1; dy++) {
          var yy = y + dy;
          if (yy < 0 || yy >= W) continue;
          for (var dx = -1; dx <= 1; dx++) {
            var xx = x + dx;
            if (xx < 0 || xx >= W) continue;
            var a = A[yy * W + xx];
            if (!(a === a)) continue;
            var wt = (dx ? 1 : 2) * (dy ? 1 : 2);
            acc += a * wt; w += wt;
          }
        }
        B[i] = acc / w;
      }
    }
  }

  // ---- the layer ----
  // A tile Leaflet removes forgets its ready call: a tile made again under the same key must not be marked ready by the
  // old one's timer (G61 re-check R-3).
  function bathyForget(el) {
    if (!el) return;
    clearTimeout(el._bDoneTimer); el._bDoneTimer = null; el._bDone = null;
  }
  function bathyReady(el) {
    var done = el._bDone;
    if (!done) return;
    el._bDone = null;
    clearTimeout(el._bDoneTimer); el._bDoneTimer = null;
    done(null, el);
  }
  var BathyLayer = L.GridLayer.extend({
    initialize: function () {
      L.GridLayer.prototype.initialize.call(this, { pane: 'obsPane', tileSize: TILE, updateWhenZooming: false, updateWhenIdle: true, keepBuffer: 1, className: 'ov-bathy-tiles' });
      this.store = null; this.unit = 'US'; this.contours = true; this._coast = null; this._gen = 0; this._staleQ = null; this._lut = bathyLut();
      var W = TILE + 2 * BATHY_APRON, N = W * W, G = (TILE + 2) * (TILE + 2);
      this._D = new Float32Array(N); this._SF = new Uint8Array(N); this._SH = new Float32Array(N); this._PI = new Int8Array(N);
      this._T = new Float32Array(N); this._F = new Float32Array(N); this._K = new Int8Array(N); this._G = new Float32Array(G);
      this._img = null; this.onRedraw = null;
    },
    // The store the tiles draw from (null detaches). Its landings redraw only the tiles that asked for that file.
    setStore: function (store) {
      var self = this, prev = this.store;
      if (prev) { if (prev.onChange === this._onStore) prev.onChange = null; if (prev.wanted === this._wantFn) prev.wanted = null; if (prev.inUse === this._useFn) prev.inUse = null; }
      this.store = store || null;
      if (store) {
        this._onStore = function (key) { self._landed(key); };
        this._wantFn = function () { return self._keys('_bNeed'); };
        this._useFn = function () { return self._keys('_bUse'); };
        store.onChange = this._onStore; store.wanted = this._wantFn; store.inUse = this._useFn;
      }
      this._redraw();
    },
    // The coastline store that masks GEBCO's pixels off the land (null: unmasked): tier 0 to its own zoom, the 5-degree
    // chunks closer in (GEBCO is drawn at every zoom, so its edge must follow the shore). A tile whose chunk is still on the
    // way uses tier 0 and is drawn again when it lands. The store's hooks are borrowed while this layer holds it (the wave
    // layer draws nothing meanwhile) and handed back when it lets go.
    setCoast: function (coast) {
      var self = this, prev = this._coast, back = this._coastPrev;
      if (prev && prev.onChange === this._onCoast) { prev.onChange = back ? back[0] : null; prev.onNeeded = back ? back[1] : null; }
      this._coastPrev = null;
      this._coast = coast && coast.status === 'ok' ? coast : null;
      if (this._coast) {
        this._coastPrev = [this._coast.onChange, this._coast.onNeeded];
        this._onCoast = function () { self._coastLanded(); };
        this._coast.onChange = this._onCoast;
        this._coast.onNeeded = function () { return self._coastNeeded(); };
      }
      this._redraw();
    },
    // The tier-1 cells the tiles on the map that mask GEBCO need now (the store drops queued downloads for others).
    _coastNeeded: function () {
      var st = this._coast, need = {};
      if (!st) return need;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords || !t.el._bLandKey || !st.tier1Zoom(t.coords.z)) continue;
        var names = coastCellsForTile(t.coords, st.index.tier1.cell);
        if (names.length > MAX_CELLS_PER_TILE) continue;
        for (var i = 0; i < names.length; i++) need[names[i]] = true;
      }
      return need;
    },
    // A chunk landed, failed, or a cooldown ended: the tiles drawn from the tier-0 stand-in whose mask changed are drawn
    // again in idle time (the rest keep their picture).
    _coastLanded: function () {
      if (!this._coast) return;
      var any = false;
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (!t.el || !t.coords || t.el._bLandFinal !== false) continue;
        var before = t.el._bLand;
        if (this._landFor(t.el, t.coords) !== before) { t.el._ovGen = -1; any = true; }
      }
      if (any) this._queueStale();
    },
    setUnit: function (unit) { var u = unit === 'Metric' ? 'Metric' : 'US'; if (u === this.unit) return; this.unit = u; this._redraw(); },
    setContours: function (on) { on = !!on; if (on === this.contours) return; this.contours = on; this._redraw(); },
    _keys: function (prop) {
      var out = new Set();
      for (var k in this._tiles) { var el = this._tiles[k].el, a = el && el[prop]; if (a) for (var i = 0; i < a.length; i++) out.add(a[i]); }
      return out;
    },
    // A file landed (or '*': a retry is due): the tiles that asked for it, or that drew their edge without it (a neighbouring
    // data tile), are drawn again in idle time.
    _landed: function (key) {
      if (key === '*') { this._redraw(); return; }
      var any = false;
      for (var k in this._tiles) {
        var t = this._tiles[k], el = t.el, need = el && el._bNeed, nb = el && el._bNb;
        if ((need && need.indexOf(key) >= 0) || (nb && nb.indexOf(key) >= 0)) { el._ovGen = -1; any = true; }
      }
      if (any) this._queueStale();
    },
    // Every tile drawn again, in idle-time batches (a bathymetry tile costs ~10-20 ms: never a burst on one task), the
    // tiles in view first. The old picture stays until each tile is redrawn.
    _redraw: function () { this._gen++; this._queueStale(); },
    _drawStale: function (inView, deadline) {
      var range = viewRangeOf(this), left = false, drew = false;
      for (var pass = 0; pass < 2 && !left; pass++) {
        for (var k in this._tiles) {
          var t = this._tiles[k];
          if (!t.el || !t.coords || t.el._ovGen === this._gen) continue;
          var seen = !range || tileInRange(t.coords, range);
          if (pass === 0 ? !seen : seen) continue;
          if (deadline && typeof deadline.timeRemaining === 'function' && !deadline.didTimeout && deadline.timeRemaining() < STALE_TILE_MS) { left = true; break; }
          this._draw(t.el, t.coords); drew = true;
        }
      }
      if (left) this._queueStale();
      if (drew && this.onRedraw) this.onRedraw();
    },
    _queueStale: ModelGridLayer.prototype._queueStale,
    _cancelStale: ModelGridLayer.prototype._cancelStale,
    onRemove: function (map) { this._cancelStale(); L.GridLayer.prototype.onRemove.call(this, map); },
    _removeTile: function (key) {
      var t = this._tiles[key];
      if (t) bathyForget(t.el);
      var base = L.GridLayer.prototype._removeTile;
      if (base) base.call(this, key); else delete this._tiles[key];
    },
    // A new tile tells the map it is ready only after its first full draw (or after BATHY_DONE_MS at most): Leaflet keeps the
    // old zoom's tiles until then, so a zoom never shows an empty map while the new tiles load (G61 B-8).
    createTile: function (coords, done) {
      var el = document.createElement('canvas'); el.width = TILE; el.height = TILE; el._ovGen = -1;
      el._bDone = done;
      el._bDoneTimer = setTimeout(function () { bathyReady(el); }, BATHY_DONE_MS);
      this._queueStale();
      return el;
    },
    // After a draw: the tile is ready when none of the files it asked for is still on the way (failed ones count as answered).
    _ready: function (el, need) {
      if (!el._bDone) return;
      if (need && need.length && this.store && this.store.waiting(need)) return;
      bathyReady(el);
    },
    // The tile's land mask (null: none), kept on the tile like the wave layer's: a final one is reused, a stand-in is
    // reused until the store moves on and still a stand-in after it.
    _landFor: function (el, coords) {
      var st = this._coast;
      if (!st || st.status !== 'ok') return null;
      var key = coords.z + '/' + tileBox(coords).xw + '/' + coords.y, same = el._bLandKey === key && el._bLandOf === st;
      if (same && (el._bLandFinal || el._bLandRev === st.rev)) return el._bLand;
      var got = st.setsFor(coords);
      if (!got.complete && same && el._bLandFinal === false) { el._bLandRev = st.rev; return el._bLand; }
      el._bLand = got.sets.length ? maskState(rasterise(landPathsForTile(coords, got.sets), TILE)) : null;
      el._bLandKey = key; el._bLandOf = st; el._bLandFinal = got.complete; el._bLandRev = st.rev;
      return el._bLand;
    },
    _clearTile: function (el) {
      if (el._bShown) { el.getContext('2d').clearRect(0, 0, TILE, TILE); el._bShown = false; }
      el._bPieces = null; el._bSrc = null;
    },
    _draw: function (el, coords) {
      el._ovGen = this._gen;
      var st = this.store;
      if (!st || st.status !== 'ok' || !st.regions) { el._bNeed = null; el._bUse = null; el._bNb = null; this._clearTile(el); bathyReady(el); return; }
      // The want list is on the tile BEFORE the pieces ask for files: the store keeps only the requests some tile wants, and
      // this draw's own requests are among them.
      var need = [], use = [], miss = [];
      el._bNeed = need; el._bUse = use; el._bNb = miss;
      var pieces = bathyPieces(st, coords, need, use, miss);
      if (!pieces.length) { this._clearTile(el); this._ready(el, need); return; }
      for (var k = 0; k < pieces.length; k++) bathyShade(pieces[k].e);
      var hasGebco = false;
      for (k = 0; k < pieces.length; k++) if (pieces[k].gebco) hasGebco = true;
      var land = hasGebco ? this._landFor(el, coords) : null;
      var A = BATHY_APRON, W = TILE + 2 * A, D = this._D, SF = this._SF, SH = this._SH, PI = this._PI;
      bathySample(pieces, land, -A, TILE + A, -A, TILE + A, A, W, D, SF, SH, PI);
      var ctx = el.getContext('2d'), img = this._img || (this._img = ctx.createImageData(TILE, TILE)), d = img.data, lut = this._lut;
      var src = new Uint32Array(pieces.length), any = false;
      for (var y = 0; y < TILE; y++) {
        for (var x = 0; x < TILE; x++) {
          var o = (y + A) * W + x + A, i = (y * TILE + x) * 4, pi = PI[o];
          if (pi < 0) { d[i + 3] = 0; continue; }
          var pc = pieces[pi], sf = SF[o], coarse = pc.coarse || (sf & BATHY_COARSE) !== 0;
          var j = Math.round(bathyPos(D[o], coarse) * 1023) * 3, fac = 1 + bathyWeight(D[o], coarse) * (SH[o] - 1);
          if (sf & BATHY_FILLED) fac *= 0.88;
          d[i] = lut[j] * fac; d[i + 1] = lut[j + 1] * fac; d[i + 2] = lut[j + 2] * fac; d[i + 3] = 255 * pc.fade;
          src[pi] |= 1 << (sf & BATHY_SRC); any = true;
        }
      }
      if (!any) { this._clearTile(el); this._ready(el, need); return; }
      if (this.contours) this._contours(coords, d, pieces);
      ctx.putImageData(img, 0, 0);
      el._bPieces = pieces; el._bLandUsed = land; el._bShown = true;
      el._bSrc = pieces.map(function (p, n) { return { reg: p.reg, mask: src[n] }; });
      this._ready(el, need);
    },
    // Depth lines on the lattice the tile was just sampled on (the map tile plus BATHY_APRON pixels each side, smoothed twice,
    // so they do not follow single noisy nodes and join their neighbours' lines at the tile edge: G61 A4). GEBCO and other
    // coarse pixels take the sparse zoom-0 levels at every zoom (a 450-m grid has no 1-m line in it: G61 B-7); where the two
    // kinds meet no line is drawn (their level numbers differ).
    _contours: function (coords, rgba, pieces) {
      var A = BATHY_APRON, W = TILE + 2 * A, D = this._D, T = this._T, F = this._F, G = this._G, K = this._K, PI = this._PI, SF = this._SF;
      var per = this.unit === 'Metric' ? 1 : 3.28084, L = bathyLevels(coords.z, this.unit), LC = bathyLevels(0, this.unit), mixed = L.length !== LC.length;
      bathySmooth(D, T, W); bathySmooth(T, F, W);
      if (mixed) {
        for (var n = 0; n < W * W; n++) { var pk = PI[n]; K[n] = pk < 0 ? -1 : pieces[pk].coarse || (SF[n] & BATHY_COARSE) ? 1 : 0; }
      }
      var M = TILE + 2;
      for (var gy = 0; gy < M; gy++) {
        for (var gx = 0; gx < M; gx++) {
          var s = (gy + A - 1) * W + (gx + A - 1), v = F[s], out = NaN;
          if (v === v) {
            if (!mixed) out = bathyLevel(L, v * per);
            else {
              var kd = K[s];
              if (kd >= 0 && !((K[s - 1] >= 0 && K[s - 1] !== kd) || (K[s + 1] >= 0 && K[s + 1] !== kd) ||
                (K[s - W] >= 0 && K[s - W] !== kd) || (K[s + W] >= 0 && K[s + W] !== kd))) out = bathyLevel(kd ? LC : L, v * per);
            }
          }
          G[gy * M + gx] = out;
        }
      }
      var prof = contourProfile(coords.z);
      contourTile(G, TILE, rgba, 3, null, 0, 0, TILE, TILE, prof[1], prof[2], false);
    },
    // What the drawn pixel under (lat, lng) shows: the same pieces and sampler as the tile. null without a drawn tile;
    // {kind: 'none'} no data there; {kind: 'depth', depth (m below mean sea level; negative = dry), src (the source entry),
    // srcId, region, filled, blended, coarse}.
    readoutAt: function (lat, lng, z) {
      var p = pixelOf(lat, lng, z), t = this._tiles[p.x + ':' + p.y + ':' + p.z], el = t && t.el;
      if (!el || !el._bShown || !el._bPieces) return el && el._bNeed ? { kind: 'none' } : null;
      var D = new Float32Array(1), SF = new Uint8Array(1), SH = new Float32Array(1), PI = new Int8Array(1);
      bathySample(el._bPieces, el._bLandUsed, p.px, p.px + 1, p.py, p.py + 1, -p.px, 0, D, SF, SH, PI);   // written at index 0
      if (PI[0] < 0) return { kind: 'none' };
      var pc = el._bPieces[PI[0]], sf = SF[0], ix = this.store && this.store.index['i:' + this.store.regionKey(pc.reg)], id = sf & BATHY_SRC;
      return { kind: 'depth', depth: D[0], srcId: id, src: ix && ix.sources[id] || null, region: pc.reg.name,
        filled: (sf & BATHY_FILLED) !== 0, blended: (sf & BATHY_BLENDED) !== 0, coarse: pc.coarse || (sf & BATHY_COARSE) !== 0 };
    },
    // A tile in view that has not had its first draw yet (the sources line says "loading" then, not "none in view").
    pendingInView: function () {
      var range = viewRangeOf(this);
      for (var k in this._tiles) {
        var t = this._tiles[k];
        if (t.el && t.el._ovGen === -1 && (!range || tileInRange(t.coords, range))) return true;
      }
      return false;
    },
    // The sources drawn in the tiles in view, finest first: [{name, year, res_m, credit, region}].
    coolingInView: function () {
      var st = this.store, range = viewRangeOf(this);
      if (!st) return false;
      for (var k in this._tiles) {
        var t = this._tiles[k], need = t.el && t.el._bNeed;
        if (!need || (range && !tileInRange(t.coords, range))) continue;
        for (var i = 0; i < need.length; i++) if (st.cooling(need[i])) return true;
      }
      return false;
    },
    sourcesInView: function () {
      var range = viewRangeOf(this), seen = {}, out = [], st = this.store;
      if (!st) return out;
      for (var k in this._tiles) {
        var t = this._tiles[k], el = t.el;
        if (!el || !el._bSrc || (range && !tileInRange(t.coords, range))) continue;
        for (var i = 0; i < el._bSrc.length; i++) {
          var e = el._bSrc[i], ix = st.index['i:' + st.regionKey(e.reg)];
          if (!ix || !e.mask) continue;
          for (var id = 0; id <= BATHY_SRC; id++) {
            if (!(e.mask & (1 << id))) continue;
            var key = e.reg.name + ':' + id, s = ix.sources[id];
            if (seen[key] || !s) continue;
            seen[key] = true;
            out.push({ name: s.name, year: s.year, res_m: s.res_m, credit: s.credit, licence: s.licence, region: e.reg.name });
          }
        }
      }
      out.sort(function (a, b) { return (a.res_m === null ? 1e9 : a.res_m) - (b.res_m === null ? 1e9 : b.res_m) || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0); });
      return out;
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
  var STALL_MS = 5000;                                       // a frame DOWNLOAD with no byte for this long (or 4 intervals) is asked again
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
  // The isobars (plan section 46): a THIRD frame kind, 'pres' = the run's sea-level pressure (half resolution only), loaded
  // beside the wind frame and its direction, drawn by the IsobarLayer for the step on the map, never an older step's lines
  // under a newer time (the same entry rule as the particles). Wind only (the owner); a run without the field has none.
  var PRES_WAIT_MS = 1000;                                  // the longest a step waits for its pressure frame (G32)
  var PRES_FIELD = 'pres', PRES_UNDER = { wind: true };
  function presFieldOk(f) {
    return !!(f && f.interpolation === 'bilinear' && f.units === 'hPa' && typeof f.lo === 'number' && typeof f.hi === 'number' && f.lo < f.hi &&
      Array.isArray(f.resolutions) && f.resolutions.indexOf('half') >= 0 && !f.circular);
  }
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
  // One lake node's vector for the particles (plan section 50, step 6b: read per lattice point straight from the two
  // lake frames, which share one grid, so a node's value is its own code; nothing is kept per node): FROM -> TOWARD at
  // the field's speed, rounded to single precision like the model's node vectors (vectorNodes keeps Float32); false
  // without a direction, without a value, or at no speed.
  var LV = new Float64Array(2);
  function lakeVec(LF, LD, spd, lo, hi, idx, out) {
    var d = LD[idx], c = LF[idx];
    if (!d || d === 255 || !c) return false;
    var sp = spd(lo + (c - 1) / 254 * (hi - lo));
    if (!(sp > 0)) return false;
    out[0] = Math.fround(-sp * CODE_SIN[d]); out[1] = Math.fround(-sp * CODE_COS[d]);
    return true;
  }
  // The particles' screen lattice, every s px over the view: the node vectors interpolated bilinearly over
  // the present nodes (weight < 0.25: nothing), as screen velocities (x right, y down) times the Mercator
  // stretch min(sec lat, 3); nothing where the field is not drawn (clipped fields: the layer's tile land
  // masks, the readout's own rule, so swell particles never run over land). lake (plan section 50): {field, lf, ld,
  // grid, lo, hi}, the step's lake field and direction frames: a lattice point on lake water (the tiles' lake coverage,
  // the same alpha rule as the pixels) reads their nodes instead (lakeVec per node, nothing where they are absent: a
  // blank lake gets no particles); on lake water with no lake frames (the model's own lake cells, ECMWF) the model's
  // nodes serve, the land gate lifted there.
  function flowField(view, s, nodes, dgrid, layer, clip, prev, lake) {
    var cols = Math.ceil(view.w / s) + 1, rows = Math.ceil(view.h / s) + 1, n = TILE * Math.pow(2, view.z), reuse = prev && prev.cols === cols && prev.rows === rows;
    var u = reuse ? prev.u : new Float32Array(cols * rows), v = reuse ? prev.v : new Float32Array(cols * rows);
    if (reuse) { u.fill(0); v.fill(0); }
    var nc = nodes.cols, nr = nodes.rows, U = nodes.U, V = nodes.V, M = nodes.M, cp = new Float64Array(cols), i, j;
    for (i = 0; i < cols; i++) cp[i] = ((((lngOfWorldX(view.ox + i * s, n) - dgrid.lon0) / dgrid.dlon) % nc) + nc) % nc;
    var scale = Math.pow(2, view.z - view.zt), tiles = clip ? layer._tiles : null, lakes = !!(tiles && layer._lakeMask), lastKey = null, lastMask, lastLake = null;
    var LF = lake ? lake.lf.q : null, LD = lake ? lake.ld.q : null, lg = lake ? lake.grid : null, lcols = lg ? lg.cols : 0, lrows = lg ? lg.rows : 0;
    var lspd = lake ? (FLOW_SPEED[lake.field] || FLOW_SPEED.wind) : null, llo = lake ? lake.lo : 0, lhi = lake ? lake.hi : 0;
    var lfx = lake ? new Float64Array(cols) : null;
    if (lake) for (i = 0; i < cols; i++) { var ln = lngOfWorldX(view.ox + i * s, n); lfx[i] = ((((ln + 180) % 360) + 360) % 360 - 180 - lg.lon0) / lg.dlon; }
    for (j = 0; j < rows; j++) {
      var lat = latOfWorldY(view.oy + j * s, n), r = (dgrid.lat0 - lat) / -dgrid.dlat, base = j * cols;
      if (!(r >= 0 && r <= nr - 1)) continue;
      var k = Math.min(3, 1 / Math.max(1e-6, Math.cos(lat * Math.PI / 180)));
      var r0 = Math.floor(r), r1 = r0 + 1 < nr ? r0 + 1 : r0, fr = r - r0, w0 = 1 - fr, b0 = r0 * nc, b1 = r1 * nc;
      var Y = (view.oy + j * s) / scale, ty = Math.floor(Y / TILE), py = Math.floor(Y - ty * TILE);
      var lfy = lake ? (lg.lat0 - lat) / -lg.dlat : -1, lr0 = Math.floor(lfy), lr1 = lr0 + 1 < lrows ? lr0 + 1 : lr0, lfr = lfy - lr0, lw0 = 1 - lfr, lb0 = lr0 * lcols, lb1 = lr1 * lcols;
      for (i = 0; i < cols; i++) {
        var onLake = false;
        if (tiles) {                                                            // drawn water only (alpha >= LAND_READOUT)
          var X = (view.ox + i * s) / scale, tx = Math.floor(X / TILE), px = Math.floor(X - tx * TILE), key = tx + ':' + ty + ':' + view.zt;
          if (key !== lastKey) { var t = tiles[key]; lastKey = key; lastMask = t && t.el ? t.el._ovLand : undefined; lastLake = lakes && t && t.el ? t.el._ovLake : null; }
          if (lastMask === undefined) continue;
          var pa = lastMask === LAND_ALL ? 0 : lastMask === null ? 255 : 255 - lastMask[py * TILE + px], lk = lastLake ? lastLake[py * TILE + px] : 0;
          if (lk > pa) pa = lk;
          if (pa < LAND_READOUT) continue;
          onLake = lk >= LAND_READOUT;
        }
        if (onLake && LF) {                                                     // the lake frames' own vectors, or nothing
          var lx = lfx[i];
          if (!(lfy >= 0 && lfy <= lrows - 1 && lx >= 0 && lx <= lcols - 1)) continue;
          var lc0 = Math.floor(lx), lc1 = lc0 + 1 < lcols ? lc0 + 1 : lc0, lfc = lx - lc0;
          var la = lb0 + lc0, lb = lb0 + lc1, lc = lb1 + lc0, ld = lb1 + lc1, lwt = 0, lX = 0, lY = 0, lw;
          if (lakeVec(LF, LD, lspd, llo, lhi, la, LV)) { lw = lw0 * (1 - lfc); lX += LV[0] * lw; lY += LV[1] * lw; lwt += lw; }
          if (lakeVec(LF, LD, lspd, llo, lhi, lb, LV)) { lw = lw0 * lfc; lX += LV[0] * lw; lY += LV[1] * lw; lwt += lw; }
          if (lakeVec(LF, LD, lspd, llo, lhi, lc, LV)) { lw = lfr * (1 - lfc); lX += LV[0] * lw; lY += LV[1] * lw; lwt += lw; }
          if (lakeVec(LF, LD, lspd, llo, lhi, ld, LV)) { lw = lfr * lfc; lX += LV[0] * lw; lY += LV[1] * lw; lwt += lw; }
          if (lwt < 0.25) continue;
          u[base + i] = k * lX / lwt; v[base + i] = -k * lY / lwt;
          continue;
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

  // The map's geometry for a view-sized canvas: the world pixel of the container's top-left from Leaflet's pixel bounds
  // (exact whatever Leaflet's cached size says) and the container's REAL size (the site sets the map height by script
  // after Leaflet measured it; Leaflet's getSize() lags until the first resize); zt = the field layer's tile zoom.
  function viewOf(map, layer) {
    var size = map.getSize(), c = map.getContainer && map.getContainer(), pb = map.getPixelBounds(), z = map.getZoom();
    var w = (c && c.clientWidth) || size.x, h = (c && c.clientHeight) || size.y;
    return { z: z, w: w, h: h, ox: pb.min.x, oy: pb.min.y, zt: layer && typeof layer._tileZoom === 'number' ? layer._tileZoom : Math.round(z) };
  }
  // A view-sized devicePixelRatio canvas over the view (in the map pane's layer coordinates); -> the ratio used.
  function placeCanvas(cv, ctx, map, v) {
    var dpr = Math.min(2, (window.devicePixelRatio || 1)), W = Math.round(v.w * dpr), H = Math.round(v.h * dpr);
    if (cv.width !== W || cv.height !== H) { cv.width = W; cv.height = H; cv.style.width = v.w + 'px'; cv.style.height = v.h + 'px'; }
    var p = map.containerPointToLayerPoint([0, 0]);
    cv.style.transform = 'translate3d(' + p.x + 'px,' + p.y + 'px,0)';
    if (ctx) ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    return dpr;
  }

  // The animation: particles with fading trails along the swell (wave height, peak period) or the wind
  // (wind speed) on one devicePixelRatio canvas in the ovAnimPane (owner: particles only). A plain
  // object (no L.Layer): the controller attaches it while the Animation box is ticked and a direction
  // frame is on the map. It stops on Off, when unticked, in a hidden tab and while the map moves or
  // zooms (the canvas is hidden during a zoom and rebuilt on zoomend / moveend); under reduced motion
  // nothing animates at all (the checkbox says so).
  // The particles' strength from the page's settings slider (plan section 46): 0..1, anything else is today's look (1).
  function particleOpacityOf(v) { return typeof v === 'number' && isFinite(v) ? Math.max(0, Math.min(1, v)) : 1; }
  function FlowAnimator(map, layer) {
    this.map = map; this.layer = null; this._onLayerRedraw = null;
    this.canvas = null; this.ctx = null; this.dpr = 1; this.v = null;
    this.field = null; this.dir = null; this.dgrid = null; this.entry = null; this.fframe = null; this.mode = null;   // 'particles' while animating
    this.nodes = null; this.nodesFor = null; this.vf = null; this.particles = null; this.count = 0; this.target = 0;
    this.ldir = null; this.lframe = null; this.lgrid = null; this.lentry = null;   // the lake frames (plan section 50)
    this.vel = new Float64Array(2); this.vel2 = new Float64Array(2); this.hist = null; this.histN = null; this.histT = null; this.histF = null; this.flowT = null;
    this.active = false; this.suspended = 0; this.dirty = false; this.rafId = null; this.lastT = 0;
    this.ema = 0; this.adaptAt = 0; this._listeners = [];
    this.rate = 1;                                           // the playback speed: particles move x1 (paused, snail), x2, x4
    this.opacity = 1; this.pane = null;                      // the settings slider's strength, applied to the whole pane
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
    this.pane = pane; this._paneOpacity();
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
  // The settings slider fades the particles: one CSS opacity on their pane (one compositing layer, no per-frame cost, the
  // strokes' own colours untouched); 1 leaves the pane exactly as before (no inline opacity).
  FlowAnimator.prototype.setOpacity = function (k) { this.opacity = particleOpacityOf(k); this._paneOpacity(); };
  FlowAnimator.prototype._paneOpacity = function () { if (this.pane && this.pane.style) this.pane.style.opacity = this.opacity < 1 ? String(this.opacity) : ''; };
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
      mode: this.mode, count: this.count, target: this.target, rate: this.rate, ema: Math.round(this.ema * 10) / 10, healed: this.healed, busy: this._busy(),
      lake: this.ldir ? (this.lentry ? this.lentry.step : '?') : 'none', log: this.log.map(function (e) { return e[0] + '@' + e[1]; }).join(' ') };
  };
  FlowAnimator.prototype.detach = function () {
    var self = this;
    this.stop();
    if (this.watch) { clearInterval(this.watch); this.watch = null; }
    this._listeners.forEach(function (l) { self.map.off(l[0], l[1]); }); this._listeners = [];
    if (this.canvas) { if (this.canvas.remove) this.canvas.remove(); this.canvas = null; this.ctx = null; }
    this.setLayer(null);
    this.dir = null; this.dgrid = null; this.fframe = null; this.vf = null; this.nodes = null; this.nodesFor = null; this.particles = null; this.v = null;
    this._dropLake();
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
  // The lake frames of a step (plan section 50): its direction frame, the lake field frame the layer draws (the
  // vectors' magnitude) and their grid; used only while they are the step on the map's (lentry === entry), so a
  // step change (clearData, setData) never carries them over. Set before the step's own direction (the landing sets
  // the lake first: one rebuild), they ride along with it; delivered later, they rebuild at once.
  FlowAnimator.prototype.setLakeData = function (dir, lframe, grid, entry) {
    if (this.ldir === dir && this.lframe === lframe) { this.lentry = entry || null; return; }
    this.ldir = dir; this.lframe = lframe; this.lgrid = grid; this.lentry = entry || null;
    if (this.dir && this.entry === this.lentry && !this.suspended && this.canvas) this._rebuild();
  };
  FlowAnimator.prototype.clearLakeData = function () {
    if (!this.ldir) return;
    var shown = this.dir && this.entry === this.lentry;
    this._dropLake();
    if (shown && !this.suspended && this.canvas) this._rebuild();
  };
  FlowAnimator.prototype._dropLake = function () { this.ldir = null; this.lframe = null; this.lgrid = null; this.lentry = null; };
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
  FlowAnimator.prototype.view = function () { return viewOf(this.map, this.layer); };
  FlowAnimator.prototype._place = function (v) { this.dpr = placeCanvas(this.canvas, this.ctx, this.map, v); };
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
    // the lake frames of THIS step, while the layer draws that lake frame (plan section 50): the lattice reads their
    // nodes per point (step 6b: nothing is kept per lake node)
    var lake = null, ll = this.layer._lake;
    if (this.ldir && this.lframe && this.lentry === this.entry && ll && ll.frame === this.lframe) lake = { field: this.field, lf: this.lframe, ld: this.ldir, grid: this.lgrid, lo: this.layer._lo, hi: this.layer._hi };
    this.vf = flowField(v, cellPx >= 16 ? 8 : 4, this.nodes, this.dgrid, this.layer, !!this.layer._clip, this.vf, lake);
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

  // ---- isobars (plan section 46): the sea-level pressure frame as lines over the wind overlay, in Windy's manner ----
  // The frame is the job's half-resolution `pres` field (hPa, 0.5 deg, 8-bit codes over 920-1060). Per view (owner G32:
  // Windy as the model): the nodes under the view are averaged into blocks about ISO_NODE_PX apart on screen (the widest
  // views trace a coarse grid), smoothed by ISO_BLUR passes of a [1, 2, 1] kernel (smooth curves, a little less exact
  // around deep lows: the readout keeps the exact value), traced by marching squares into ONE polyline per isobar,
  // projected to the screen and stroked thin and faint; at most one small pill per line, rotated along it, from zoom
  // ISO_LABEL_MIN_Z (none at the whole-world view); H and L at the highs' and lows' centres with their pressure, found on
  // the frame's own codes (the exact centre and central pressure, also of a compact typhoon the smoothing flattens). Pure
  // functions below; IsobarLayer draws them on one view-sized canvas in the ovIsoPane (above the field's colours, under the
  // particles, outside the model pane's multiply blend).
  // The interval (Windy's spacing): every 8 hPa while the view shows the whole world (any screen width) or at zoom <= 2.9,
  // every 4 hPa to zoom 5.9, every 2 hPa beyond.
  var ISO_INTERVAL = [[2.9, 8], [5.9, 4], [99, 2]];
  var ISO_WORLD_DEG = 300;                                  // a view this many degrees of longitude wide shows the whole world
  var ISO_NODE_PX = 7;                                      // the traced grid's spacing on screen, at least
  var ISO_BLUR = 2;                                         // [1, 2, 1] passes over the traced grid
  var ISO_LABEL_MIN_Z = 4, ISO_LABEL_MIN_PX = 120, ISO_LABEL_EDGE_PX = 8, ISO_LABEL_GRID_PX = 32, ISO_LABEL_GAP_PX = 4;
  var ISO_LABEL_AT = [0.5, 0.35, 0.65, 0.2, 0.8];          // where along a line's visible length its one pill may sit, in turn
  var ISO_HL_PX = 110, ISO_HL_EDGE_PX = 14;                 // H / L: the neighbourhood radius on screen, the edge margin
  var ISO_HL_PROM_CODES = 2, ISO_HL_PROM_CAP = 255;         // a mark's least prominence (codes: 1.1 hPa); "never absorbed"
  var ISO_CLAMP_PATCH_MAX = 16, ISO_CLAMP_LAT = 70;         // a clamped-end centre: a compact patch, away from the poles
  var ISO_TERRAIN_M = 400, TERRAIN_M_PER_CODE = 20;         // no H / L on ground this high (terrain_half.png: 20 m per code)
  var ISO_STRONG_LOW_HPA = 15, ISO_TERRAIN_CAP_M = 2000;    // ...except a low this prominent over ground lower than this (owner, V3-1)
  var ISO_STRONG_NODES_MAX = 20000;                         // a strength search needing more nodes than this counts weak
  var ISO_PROM_NODES_MAX = 16000;                           // the prominence pass runs on blocks coarse enough for this many
  var ISO_HL_CLEAR_PX = 2;                                  // the lines are cleared this far around a mark's box
  var ISO_FONT = 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif';
  var ISO_STYLE = { halo: 'rgba(0,0,0,0.14)', haloW: 2.4, core: 'rgba(255,255,255,0.72)', coreW: 1.2,
    font: '600 9px ' + ISO_FONT, pill: 'rgba(255,255,255,0.82)', text: '#1d2b4f', padX: 4, h: 11, r: 5.5,
    hlFont: '600 15px ' + ISO_FONT, hlValueFont: '500 10px ' + ISO_FONT, hlText: 'rgba(255,255,255,0.96)', hlShadow: 'rgba(0,0,0,0.55)', hlBlur: 3 };
  var ISO_CASES = [                                         // marching squares: corner bits a=8 (top left) b=4 (top right) c=2 (bottom right) d=1 (bottom left)
    [], [['L', 'B']], [['B', 'R']], [['L', 'R']], [['T', 'R']], null, [['T', 'B']], [['T', 'L']],
    [['T', 'L']], [['T', 'B']], null, [['T', 'R']], [['L', 'R']], [['B', 'R']], [['L', 'B']], []];
  // spanDeg (optional): the view's width in degrees of longitude; the whole world gets the widest interval.
  function isoInterval(z, spanDeg) {
    if (spanDeg >= ISO_WORLD_DEG) return ISO_INTERVAL[0][1];
    for (var i = 0; i < ISO_INTERVAL.length; i++) if (z <= ISO_INTERVAL[i][0]) return ISO_INTERVAL[i][1];
    return ISO_INTERVAL[ISO_INTERVAL.length - 1][1];
  }
  // One traced node per `stride` grid nodes, so that traced nodes are at least ISO_NODE_PX apart on screen.
  function isoStride(z, dlon) {
    var px = TILE * Math.pow(2, z) * Math.abs(dlon) / 360;
    return Math.max(1, Math.ceil(ISO_NODE_PX / px - 1e-6));
  }
  // The window's pressures (hPa) on the traced grid: node (i, j) = the mean of the present codes in the stride x stride
  // block centred on grid node (r0 + i * stride, c0 + j * stride) (columns wrap; NaN where the block has none), then
  // ISO_BLUR passes of [1, 2, 1] across and down (weights renormalised around missing nodes). A whole-world window gets
  // a stride that divides the grid's columns plus one more column, the first again (the date line traced across).
  // scratch: buffers kept between redraws (returned). -> {V, R, C, r0, c0, stride, wrap, scratch}
  function isoValues(frame, fdef, win, stride, scratch) {
    var cols = frame.cols, rows = frame.rows, q = frame.q, st = stride || 1, r0 = win.r0, r1 = win.r1, c0 = win.c0;
    var wrap = win.count >= cols;
    if (wrap) { while (cols % st) st++; c0 = 0; }
    var count = wrap ? cols + 1 : win.count;
    var R = Math.floor((r1 - r0) / st) + 1, C = wrap ? cols / st + 1 : Math.floor((count - 1) / st) + 1, N = R * C;
    var sc = scratch && scratch.V && scratch.V.length >= N ? scratch : { V: new Float64Array(N * 2), T: new Float64Array(N * 2) };
    var V = sc.V, lo = fdef.lo, k = (fdef.hi - fdef.lo) / 254, h = Math.floor(st / 2), half = st % 2 === 0, i, j;
    for (i = 0; i < R; i++) {
      var rc = r0 + i * st;
      for (j = 0; j < C; j++) {
        var cc = c0 + j * st, acc = 0, n = 0;
        for (var di = -h; di <= h; di++) {                   // an even stride: the block's end rows / columns count half
          var rr = rc + di; if (rr < 0 || rr >= rows) continue;
          var base = rr * cols, wi = half && (di === h || di === -h) ? 0.5 : 1;
          for (var dj = -h; dj <= h; dj++) {
            var x = q[base + (((cc + dj) % cols) + cols) % cols];
            if (x) { var wt = half && (dj === h || dj === -h) ? wi * 0.5 : wi; acc += x * wt; n += wt; }
          }
        }
        V[i * C + j] = n ? lo + (acc / n - 1) * k : NaN;
      }
    }
    for (var p = 0; p < ISO_BLUR; p++) isoBlur(V, sc.T, R, C, wrap);
    if (wrap) for (i = 0; i < R; i++) V[i * C + C - 1] = V[i * C];
    return { V: V, R: R, C: C, r0: r0, c0: c0, stride: st, wrap: wrap, scratch: sc };
  }
  // One [1, 2, 1] pass across then down, missing nodes (NaN) left out and stay missing; wrap: the last column is the first
  // again (whole-world windows), so neighbours continue across the date line.
  function isoBlur(V, T, R, C, wrap) {
    var i, j, m = wrap ? C - 1 : C;
    for (i = 0; i < R; i++) {
      for (j = 0; j < m; j++) {
        var c = V[i * C + j];
        if (c !== c) { T[i * C + j] = NaN; continue; }
        var jl = j - 1, jr = j + 1, acc = 2 * c, w = 2, x;
        if (wrap) { if (jl < 0) jl = m - 1; if (jr >= m) jr = 0; }
        if (jl >= 0) { x = V[i * C + jl]; if (x === x) { acc += x; w++; } }
        if (jr < m) { x = V[i * C + jr]; if (x === x) { acc += x; w++; } }
        T[i * C + j] = acc / w;
      }
      if (wrap) T[i * C + C - 1] = T[i * C];
    }
    for (i = 0; i < R; i++) {
      for (j = 0; j < C; j++) {
        var v = T[i * C + j];
        if (v !== v) { V[i * C + j] = NaN; continue; }
        var a2 = 2 * v, w2 = 2, y;
        if (i > 0) { y = T[(i - 1) * C + j]; if (y === y) { a2 += y; w2++; } }
        if (i + 1 < R) { y = T[(i + 1) * C + j]; if (y === y) { a2 += y; w2++; } }
        V[i * C + j] = a2 / w2;
      }
    }
  }
  // Marching squares over V (R x C, row-major) at every multiple of `interval`: -> [{level, pts: [x0, y0, x1, y1, ...]
  // in node units (x = column, y = row of V), closed}]. A node counts as "high" when >= the level; a saddle cell is
  // resolved by the mean of its corners. Every segment ends on a grid edge that is shared with the next cell, so the
  // segments are joined through their edges into one polyline per isobar: open lines start at an edge only one segment
  // uses (the window's border or a missing node), the rest are closed loops.
  function traceIsolines(V, R, C, interval) {
    var out = [], i, j, NN = R * C;
    // Crossing points, one per (edge, level): index n -> X[n], Y[n], its two neighbours A0[n] / A1[n] (an edge is
    // shared by at most two cells, so a point never has more), and its level index K[n]. Typed storage, grown by doubling.
    var cap = 1024, X = new Float64Array(cap), Y = new Float64Array(cap), A0 = new Int32Array(cap), A1 = new Int32Array(cap), K = new Int32Array(cap), cnt = 0;
    var ids = new Map();
    function grow() {
      cap *= 2;
      var x = new Float64Array(cap); x.set(X); X = x; x = new Float64Array(cap); x.set(Y); Y = x;
      var y = new Int32Array(cap); y.set(A0); A0 = y; y = new Int32Array(cap); y.set(A1); A1 = y; y = new Int32Array(cap); y.set(K); K = y;
    }
    function eid(side, i, j) {                               // the edge of cell (i, j) on that side: 2 * node + (0 horizontal, 1 vertical)
      if (side === 'T') return (i * C + j) * 2;
      if (side === 'B') return ((i + 1) * C + j) * 2;
      if (side === 'L') return (i * C + j) * 2 + 1;
      return (i * C + j + 1) * 2 + 1;                         // 'R'
    }
    function point(key, lk, side, i, j, L, a, b, c, d) {      // the crossing on that edge, linear between its two nodes
      var n = ids.get(key);
      if (n !== undefined) return n;
      if (cnt === cap) grow();
      n = cnt++; ids.set(key, n); A0[n] = -1; A1[n] = -1; K[n] = lk;
      var t;
      if (side === 'T') { t = (L - a) / (b - a); X[n] = j + t; Y[n] = i; }
      else if (side === 'B') { t = (L - d) / (c - d); X[n] = j + t; Y[n] = i + 1; }
      else if (side === 'L') { t = (L - a) / (d - a); X[n] = j; Y[n] = i + t; }
      else { t = (L - b) / (c - b); X[n] = j + 1; Y[n] = i + t; }
      return n;
    }
    function link(n1, n2) {
      if (A0[n1] < 0) A0[n1] = n2; else A1[n1] = n2;
      if (A0[n2] < 0) A0[n2] = n1; else A1[n2] = n1;
    }
    var kmin = Infinity, kmax = -Infinity;
    for (i = 0; i < NN; i++) if (V[i] === V[i]) { var kk = V[i] / interval; if (kk < kmin) kmin = kk; if (kk > kmax) kmax = kk; }
    if (!(kmax >= kmin)) return out;
    kmin = Math.floor(kmin); kmax = Math.ceil(kmax);
    var span = kmax - kmin + 1;                               // key = edge * span + level index
    for (i = 0; i + 1 < R; i++) {
      for (j = 0; j + 1 < C; j++) {
        var a = V[i * C + j], b = V[i * C + j + 1], c = V[(i + 1) * C + j + 1], d = V[(i + 1) * C + j];
        if (a !== a || b !== b || c !== c || d !== d) continue;
        var mn = Math.min(a, b, c, d), mx = Math.max(a, b, c, d);
        for (var k = Math.ceil(mn / interval); k * interval <= mx; k++) {
          var L = k * interval, cs = (a >= L ? 8 : 0) | (b >= L ? 4 : 0) | (c >= L ? 2 : 0) | (d >= L ? 1 : 0), segs = ISO_CASES[cs], lk = k - kmin;
          if (segs === null) {                                // saddles: 5 = b and d high, 10 = a and c high
            var high = (a + b + c + d) / 4 >= L;
            segs = (cs === 5) === high ? [['T', 'L'], ['B', 'R']] : [['T', 'R'], ['L', 'B']];
          }
          for (var s = 0; s < segs.length; s++) {
            var n1 = point(eid(segs[s][0], i, j) * span + lk, lk, segs[s][0], i, j, L, a, b, c, d);
            var n2 = point(eid(segs[s][1], i, j) * span + lk, lk, segs[s][1], i, j, L, a, b, c, d);
            link(n1, n2);
          }
        }
      }
    }
    var seen = new Uint8Array(cnt), buf = new Float64Array(64);
    function walk(start, closedLoop) {
      var m = 0, prev = -1, cur = start;
      while (cur >= 0 && !seen[cur]) {
        seen[cur] = 1;
        if (m + 4 > buf.length) { var nb = new Float64Array(buf.length * 2); nb.set(buf); buf = nb; }
        buf[m++] = X[cur]; buf[m++] = Y[cur];
        var next = -1, o = A0[cur];
        if (o >= 0 && o !== prev && !seen[o]) next = o;
        else { o = A1[cur]; if (o >= 0 && o !== prev && !seen[o]) next = o; }
        prev = cur; cur = next;
      }
      if (closedLoop && m >= 4) { buf[m++] = buf[0]; buf[m++] = buf[1]; }
      out.push({ level: (K[start] + kmin) * interval, pts: buf.slice(0, m), closed: !!closedLoop });
    }
    for (i = 0; i < cnt; i++) if (!seen[i] && A1[i] < 0) walk(i, false);   // open lines start where one segment ends
    for (i = 0; i < cnt; i++) if (!seen[i]) walk(i, true);
    return out;
  }
  // The prominence of every local extreme of the frame over a window (rows R0..R1, columns C0 .. C0 + count - 1, wrapping;
  // the whole world when count covers every column), read on blocks of `s` x `s` nodes (a block = its extreme code; lows:
  // the least, highs: the greatest, stored flipped so that "lower" always means "stronger"): the blocks are added in code
  // order into 8-connected components (union-find); a component born at a block is a centre; when a block joins two
  // components the weaker (the shallower minimum; between equals the later in the GRID's raster order, whatever the
  // window's start: the same answer from every view) is absorbed, and its prominence is the joining code minus its own.
  // Equal-code blocks joined at their own code are one set with one representative (the earliest), whose prominence is
  // the set's. -> { at(r, c): the prominence in codes of the block holding the frame's node (r, c) (ISO_HL_PROM_CAP for a
  // centre never absorbed: the strongest of its region; 0 for a block beside a stronger one), -1 outside the window or
  // where no node is present }. `scratch` (optional) holds the typed arrays between calls.
  // The window's blocks of s x s nodes: each block's least and greatest present code (0 = none), one scan for both kinds.
  // -> {R0, R1, C0, count, s, bR, bC, min, max} (count = cols and s a divisor of cols when the window is the whole world).
  function blockExtremes(frame, R0, R1, C0, count, s, scratch) {
    var Q = frame.q, cols = frame.cols, rows = frame.rows;
    R0 = Math.max(0, R0); R1 = Math.min(rows - 1, R1);
    if (R1 < R0 || !(s >= 1) || !(count >= 1)) return null;
    if (count >= cols) { count = cols; while (cols % s) s--; }           // the world's blocks must meet across the seam
    var bR = Math.ceil((R1 - R0 + 1) / s), bC = Math.ceil(count / s), N = bR * bC, sc = scratch || {};
    if (!sc.bmin || sc.bmin.length < N) { sc.bmin = new Uint8Array(N); sc.bmax = new Uint8Array(N); }
    var bmin = sc.bmin, bmax = sc.bmax;
    bmin.fill(0, 0, N); bmax.fill(0, 0, N);
    for (var r = R0; r <= R1; r++) {
      var bi0 = Math.floor((r - R0) / s) * bC, base = r * cols;
      for (var j = 0; j < count; j++) {
        var v = Q[base + (C0 + j) % cols];
        if (!v) continue;
        var bk = bi0 + Math.floor(j / s);
        if (!bmin[bk] || v < bmin[bk]) bmin[bk] = v;
        if (v > bmax[bk]) bmax[bk] = v;
      }
    }
    return { R0: R0, R1: R1, C0: C0, count: count, s: s, bR: bR, bC: bC, min: bmin, max: bmax };
  }
  function prominenceMap(frame, R0, R1, C0, count, s, low, scratch, blocks) {
    var cols = frame.cols, none = { at: function () { return -1; } };
    var bx = blocks || blockExtremes(frame, R0, R1, C0, count, s, scratch);
    if (!bx) return none;
    R0 = bx.R0; R1 = bx.R1; C0 = bx.C0; count = bx.count; s = bx.s;
    var wrap = count >= cols, bR = bx.bR, bC = bx.bC, N = bR * bC, i, j;
    var sc = scratch || {};
    if (!sc.val || sc.val.length < N) {
      sc.val = new Uint8Array(N); sc.order = new Int32Array(N); sc.parent = new Int32Array(N); sc.born = new Int32Array(N); sc.prom = new Int16Array(N); sc.alias = new Int32Array(N);
    }
    var val = sc.val, order = sc.order, parent = sc.parent, born = sc.born, prom = sc.prom, alias = sc.alias, cnt = new Int32Array(257);
    parent.fill(-1, 0, N); born.fill(-1, 0, N); prom.fill(-1, 0, N); alias.fill(-1, 0, N);
    var src = low ? bx.min : bx.max;                                       // highs flipped: "lower" always means "stronger"
    for (i = 0; i < N; i++) val[i] = src[i] ? (low ? src[i] : 256 - src[i]) : 0;
    // a's block is earlier than b's in the grid's raster order (rows, then the GLOBAL column: a window may start anywhere)
    function earlier(a, b) {
      var ra = Math.floor(a / bC), rb = Math.floor(b / bC);
      if (ra !== rb) return ra < rb;
      return (C0 + (a - ra * bC) * s) % cols < (C0 + (b - rb * bC) * s) % cols;
    }
    var n = 0;                                                             // counting sort by code, stable in raster order
    for (i = 0; i < N; i++) if (val[i]) cnt[val[i] + 1]++;
    for (i = 1; i < 257; i++) cnt[i] += cnt[i - 1];
    for (i = 0; i < N; i++) if (val[i]) { order[cnt[val[i]]++] = i; n++; }
    function find(x) { while (parent[x] !== x) { parent[x] = parent[parent[x]]; x = parent[x]; } return x; }
    for (var k = 0; k < n; k++) {
      i = order[k]; parent[i] = i;
      var t = val[i], bi = Math.floor(i / bC), bj = i - bi * bC;
      for (var di = -1; di <= 1; di++) {
        var ni = bi + di;
        if (ni < 0 || ni >= bR) continue;
        for (var dj = -1; dj <= 1; dj++) {
          if (!di && !dj) continue;
          var nj = bj + dj;
          if (nj < 0 || nj >= bC) { if (!wrap) continue; nj = (nj + bC) % bC; }
          j = ni * bC + nj;
          if (parent[j] < 0) continue;                                     // not yet added: a weaker code, or absent
          var ra = find(i), rb = find(j);
          if (ra === rb) continue;
          var st = val[ra] < val[rb] || (val[ra] === val[rb] && earlier(ra, rb)) ? ra : rb, wk = st === ra ? rb : ra;
          prom[wk] = t - val[wk]; parent[wk] = st;                          // the roots are their components' minima
          if (val[wk] === t && val[st] === t) alias[wk] = st;               // two equal-code sets met at their own code: one set
        }
      }
      born[i] = find(i);
    }
    return { at: function (rr, cc) {
      if (rr < R0 || rr > R1) return -1;
      var jj = ((cc - C0) % cols + cols) % cols;
      if (jj >= count) return -1;
      var b = Math.floor((rr - R0) / s) * bC + Math.floor(jj / s), o = born[b];
      if (o < 0) return -1;
      if (val[o] !== val[b]) return 0;                                      // it joined a stronger block's component at once
      while (alias[o] >= 0) o = alias[o];                                   // its equal-code set's representative
      return prom[o] < 0 ? ISO_HL_PROM_CAP : prom[o];
    } };
  }
  // The highs' and lows' centres from the frame's own codes (G32 fix-round verification: on the smoothed grid a compact
  // typhoon read "L 1000" for 958 hPa and two tropical cyclones had no mark at the world view), chosen by PROMINENCE (the
  // G32 re-check: a depth against a ring 110 px out marked one-code dimples and lost broad highs when zoomed in; owner's
  // call, 2026-10-18):
  // 1. candidates: a node of the view's window (`win`, nodeWindow) that no node within `rad` grid nodes undercuts (L) /
  //    overtops (H) (rows beyond the grid's ends absent, columns wrapping); an equal code counts against it only as one of
  //    its 8 neighbours inside the window, where the earlier node in raster order stands (a patch that runs out of the
  //    window keeps a candidate inside it: a broad flat high at zoom 9, G32 fix-round-3 verification);
  // 2. a candidate's patch of equal codes (8-connected) is taken for its kind (one mark per patch) and marks nothing when
  //    it touches a lower (L) / higher (H) node (a shelf), holds more than ISO_PATCH_MAX nodes or spans half the world's
  //    columns (it may wrap); at the field's clamped ends (code 1: a typhoon below 920 hPa; code 255: a high above 1060)
  //    only a compact patch (ISO_CLAMP_PATCH_MAX nodes) within ISO_CLAMP_LAT degrees of the equator marks, as "<=920" /
  //    ">=1060" (the polar plateaus' clamped patches never); the centre sits at the patch's middle;
  // 3. no mark on high ground: opts.terrain (the half grid's terrain codes, terrain_half.png) at or above opts.terrainMin
  //    at the patch's first node in raster order or at its middle (never at the candidate that found the patch: which one
  //    does depends on the view) (the sea-level pressure both models reduce from the surface leaves a fixed bump
  //    on every mountain node: an "H" on the Big Island at every step, H / L pairs on mountain slopes); EXCEPT a low
  //    at least opts.strongLowHpa prominent whose ground at both nodes is below opts.terrainCap (a typhoon crossing
  //    Taiwan: 31-48 hPa; the mountain bumps measured at most 11-14 hPa, all highs) (owner, fix round 4), its strength
  //    judged over the WHOLE grid by strongLowSearch, never by the view's window (fix round 5: a low alone in a zoomed-in
  //    window has the window's cap prominence), searching at most opts.strongNodesMax nodes (else weak); a strong low's
  //    prominence for rule 4 and the declutter is the greater of the window's and the search's;
  // 4. prominence (prominenceMap over the window widened by opts.margin nodes, on blocks of opts.stride nodes, coarsened
  //    until the window holds at most opts.promNodesMax blocks when that is given: how far the field must rise (L) /
  //    fall (H) from the centre before a stronger centre is reached): at least ISO_HL_PROM_CODES; the strongest centre of
  //    a region always counts.
  // One mark per ISO_HL_PX on screen, the most prominent first, is decided on the DRAWN marks (projectCentres), so a
  // twin kept off screen never hides one in view.
  // Blocks of B x B nodes (2B - 1 <= rad) sift the candidates: a candidate is its block's extreme and its block is no worse
  // than the 8 around it (they lie inside its square), so only such blocks' extreme-valued nodes are tested (the same answer
  // as testing every node). -> [{type: 'H' | 'L', r, c, v, q, prom, clamp}] (r, c = grid row and column, fractional for a
  // patch's middle; q the code; prom in codes; clamp -1 / 1 at the field's ends, else 0)
  // The strength of a low over high ground (fix round 5, K-1): a minimax search from its patch over the WHOLE grid (8
  // neighbours, columns wrap, rows end at the poles), level by level: a queue per code, and since the greatest code along
  // the way only rises each node is queued once, at its final level. It ends at the first node taken that is lower than the
  // patch, or of its code and earlier than the patch's first node in raster order (of two equal patches the one with the
  // earlier first node counts as the lower: the merge tree's tie) -> prominence = that level minus the patch's code, strong
  // when that reaches need. It is strong once the level being TAKEN reaches the patch's code + need (a node merely queued
  // that high proves nothing: a lower node can still come at a lesser level) and weak once it has taken `cap` nodes without
  // an answer (no real strong low needs 20,000). Independent of the view. first = the patch's first node (rows x cols +
  // column); ss = reusable arrays (seen, next, head, gen). -> { prom: codes (the prominence when weak; need when strong:
  // all it proves; the cap when nothing lower can be reached at all), strong }
  function strongLowSearch(frame, first, need, cap, ss) {
    var Q = frame.q, cols = frame.cols, rows = frame.rows, N = cols * rows, v0 = Q[first];
    if (!ss.seen || ss.seen.length !== N) { ss.seen = new Uint32Array(N); ss.next = new Int32Array(N); ss.gen = 0; }
    if (!ss.head) ss.head = new Int32Array(256);
    if (ss.gen >= 0xfffffff0) { ss.seen.fill(0); ss.gen = 0; }
    var seen = ss.seen, next = ss.next, head = ss.head, gen = ++ss.gen, taken = 0;
    head.fill(-1);
    seen[first] = gen; next[first] = -1; head[v0] = first;
    for (var d = v0; d < 256; d++) {
      if (head[d] < 0) continue;
      if (d - v0 >= need) return { prom: need, strong: true };   // all it proves
      while (head[d] >= 0) {
        var x = head[d], cx = Q[x];
        head[d] = next[x];
        if (cx < v0 || (cx === v0 && x < first)) return { prom: d - v0, strong: false };
        if (taken >= cap) return { prom: d - v0, strong: false };
        taken++;
        var r = (x / cols) | 0, c = x - r * cols;
        for (var dr = -1; dr <= 1; dr++) {
          var rr = r + dr;
          if (rr < 0 || rr >= rows) continue;
          for (var dc = -1; dc <= 1; dc++) {
            if (!dr && !dc) continue;
            var cc = c + dc, y = rr * cols + (cc < 0 ? cc + cols : cc >= cols ? cc - cols : cc), cy = Q[y];
            if (!cy || seen[y] === gen) continue;
            seen[y] = gen;
            var lv = cy > d ? cy : d;
            next[y] = head[lv]; head[lv] = y;
          }
        }
      }
    }
    return { prom: ISO_HL_PROM_CAP, strong: true };           // nothing lower anywhere it could reach
  }
  var ISO_PATCH_MAX = 4096;                                 // the largest patch that marks a centre
  function findCentres(frame, fdef, win, rad, opts) {
    opts = opts || {};
    var Q = frame.q, cols = frame.cols, rows = frame.rows, k = (fdef.hi - fdef.lo) / 254, cands = [], out = [];
    if (!(rad >= 1)) rad = 1;
    if (rad > cols >> 1) rad = cols >> 1;                   // a square wider than the world would meet itself
    var r0 = Math.max(win.r0, 0), r1 = Math.min(win.r1, rows - 1), n = Math.min(win.count, cols), wrap = n >= cols;
    var B = Math.max(1, Math.min(8, Math.floor((rad + 1) / 2))), nbR = Math.ceil((r1 - r0 + 1) / B), nbC = Math.ceil(n / B);
    if (nbR < 1 || nbC < 1) return out;
    var bmin = new Uint8Array(nbR * nbC), bmax = new Uint8Array(nbR * nbC), r, jj, c, v, b;
    for (r = r0; r <= r1; r++) {
      var bi0 = Math.floor((r - r0) / B) * nbC;
      for (jj = 0; jj < n; jj++) {
        v = Q[r * cols + (win.c0 + jj) % cols];
        if (!v) continue;
        b = bi0 + Math.floor(jj / B);
        if (!bmin[b] || v < bmin[b]) bmin[b] = v;
        if (v > bmax[b]) bmax[b] = v;
      }
    }
    function test(r, c, v, low) {
      if (low ? v === 255 : v === 1) return;                 // the far end of the range is no centre of this kind
      var dr, dc, rr, cc, x;
      for (dr = -1; dr <= 1; dr++) {                          // its 8 neighbours: an equal one earlier in raster order
        rr = r + dr;                                          // and inside the window stands
        if (rr < 0 || rr >= rows) continue;
        for (dc = -1; dc <= 1; dc++) {
          if (!dr && !dc) continue;
          cc = c + dc; cc = cc < 0 ? cc + cols : cc >= cols ? cc - cols : cc;
          x = Q[rr * cols + cc];
          if (!x) continue;
          if (low ? x < v : x > v) return;
          if (x === v && (dr < 0 || (!dr && dc < 0)) && rr >= r0 && rr <= r1 && (wrap || (cc - win.c0 + cols) % cols < n)) return;
        }
      }
      for (var d = 2; d <= rad; d++) {                        // the rest of the square, ring by ring outward
        for (dr = -d; dr <= d; dr++) {
          rr = r + dr;
          if (rr < 0 || rr >= rows) continue;
          var base = rr * cols, step = dr === -d || dr === d ? 1 : 2 * d;
          for (dc = -d; dc <= d; dc += step) {
            cc = c + dc;
            x = Q[base + (cc < 0 ? cc + cols : cc >= cols ? cc - cols : cc)];
            if (x && (low ? x < v : x > v)) return;
          }
        }
      }
      cands.push({ type: low ? 'L' : 'H', r: r, c: c, q: v });
    }
    for (var bi = 0; bi < nbR; bi++) {
      for (var bj = 0; bj < nbC; bj++) {
        b = bi * nbC + bj;
        if (!bmin[b]) continue;
        var lo = true, hi = true;
        for (var di = -1; di <= 1; di++) {
          var ni = bi + di;
          if (ni < 0 || ni >= nbR) continue;
          for (var dj = -1; dj <= 1; dj++) {
            if (!di && !dj) continue;
            var nj = bj + dj;
            if (nj < 0 || nj >= nbC) { if (!wrap) continue; nj = (nj + nbC) % nbC; }
            var o = ni * nbC + nj;
            if (!bmin[o]) continue;
            if (bmin[o] < bmin[b]) lo = false;
            if (bmax[o] > bmax[b]) hi = false;
          }
        }
        if (!lo && !hi) continue;
        for (r = r0 + bi * B; r < Math.min(r0 + bi * B + B, r1 + 1); r++) {
          for (jj = bj * B; jj < Math.min(bj * B + B, n); jj++) {
            c = (win.c0 + jj) % cols; v = Q[r * cols + c];
            if (lo && v === bmin[b]) test(r, c, v, true);
            if (hi && v === bmax[b]) test(r, c, v, false);
          }
        }
      }
    }
    if (!cands.length) return out;
    var marks = null;                                         // per node: 1 = in a patch taken for a low, 2 = for a high
    // a candidate's whole patch, followed node by node (column offsets unwrapped from the candidate's column) and taken for
    // its kind, so no patch is followed twice; null for a shelf, a patch over ISO_PATCH_MAX nodes or one spanning half the
    // columns, a clamped patch that is not compact or lies poleward of ISO_CLAMP_LAT; else its middle
    function patch(cd) {
      if (!marks) marks = new Uint8Array(rows * cols);
      var low = cd.type === 'L', bit = low ? 1 : 2, stack = [cd.r, 0], sr = 0, sc = 0, m = 0, cmin = 0, cmax = 0, shelf = false;
      var first = cd.r * cols + cd.c;                         // the patch's first node in raster order (rows, global columns)
      marks[first] |= bit;
      while (stack.length) {
        var pc = stack.pop(), pr = stack.pop();
        sr += pr; sc += pc; m++;
        if (pc < cmin) cmin = pc; else if (pc > cmax) cmax = pc;
        for (var i = -1; i <= 1; i++) {
          var qr = pr + i;
          if (qr < 0 || qr >= rows) continue;
          for (var j = -1; j <= 1; j++) {
            if (!i && !j) continue;
            var qc = pc + j, key = qr * cols + (((cd.c + qc) % cols) + cols) % cols, y = Q[key];
            if (!y) continue;
            if (low ? y < cd.q : y > cd.q) shelf = true;
            else if (y === cd.q && !(marks[key] & bit)) { marks[key] |= bit; stack.push(qr, qc); if (key < first) first = key; }
          }
        }
      }
      if (shelf || m > ISO_PATCH_MAX || cmax - cmin >= cols >> 1) return null;
      var fr = Math.floor(first / cols), fc = first - fr * cols;
      if (cd.q === (low ? 1 : 255)) {
        var lat = opts.grid ? opts.grid.lat0 + fr * opts.grid.dlat : 0;
        if (m > ISO_CLAMP_PATCH_MAX || Math.abs(lat) > ISO_CLAMP_LAT) return null;
      }
      return { r: sr / m, c: ((cd.c + sc / m) % cols + cols) % cols, fr: fr, fc: fc };
    }
    var terrain = opts.terrain && opts.terrain.length === rows * cols ? opts.terrain : null, tmin = opts.terrainMin > 0 ? opts.terrainMin : 1;
    function high(rr, cc) { return !!terrain && terrain[Math.round(rr) * cols + (((Math.round(cc) % cols) + cols) % cols)] >= tmin; }
    var strongHpa = opts.strongLowHpa > 0 ? opts.strongLowHpa : 0, tcap = opts.terrainCap > 0 ? opts.terrainCap : 0;
    function lowGround(rr, cc) { return tcap > 0 && terrain[Math.round(rr) * cols + (((Math.round(cc) % cols) + cols) % cols)] < tcap; }
    var mg = Math.max(0, Math.floor(opts.margin || 0)), stride = Math.max(1, Math.floor(opts.stride || 1)), sc0 = opts.scratch || {};
    var need = Math.ceil(strongHpa / k - 1e-9), scap = opts.strongNodesMax > 0 ? Math.floor(opts.strongNodesMax) : ISO_STRONG_NODES_MAX;
    function strongOf(pt) {                                   // once per frame and patch (pans and zooms then cost nothing)
      var S = sc0.S || (sc0.S = {});
      if (S.q !== Q || S.need !== need || S.cap !== scap) { S.q = Q; S.need = need; S.cap = scap; S.m = new Map(); }
      var key = pt.fr * cols + pt.fc, got = S.m.get(key);
      if (!got) { got = strongLowSearch(frame, key, need, scap, S); S.m.set(key, got); }
      return got;
    }
    if (opts.promNodesMax > 0) {                              // coarser blocks for a large window (the world view): the pass stays cheap
      var pw = (r1 - r0 + 1 + 2 * mg) * Math.min(cols, n + 2 * mg);
      while (pw / (stride * stride) > opts.promNodesMax) stride++;
    }
    var pm = { L: null, H: null }, blocks = null;
    function promOf(cd) {
      var low = cd.type === 'L';
      if (!blocks) blocks = blockExtremes(frame, r0 - mg, r1 + mg, (((win.c0 - mg) % cols) + cols) % cols, n + 2 * mg, stride, sc0.B || (sc0.B = {}));
      if (!pm[cd.type]) pm[cd.type] = prominenceMap(frame, 0, 0, 0, 0, stride, low, sc0[cd.type] || (sc0[cd.type] = {}), blocks);
      return pm[cd.type].at(cd.r, cd.c);
    }
    cands.sort(function (a, b) { return a.type !== b.type ? (a.type < b.type ? -1 : 1) : a.r - b.r || a.c - b.c; });
    var trace = opts.trace || null;                           // (tests, debugging: why a candidate marks nothing)
    for (var i = 0; i < cands.length; i++) {
      var cd = cands[i];
      if (marks && marks[cd.r * cols + cd.c] & (cd.type === 'L' ? 1 : 2)) { if (trace) trace(cd, 'taken'); continue; }
      var pt = patch(cd);
      if (!pt) { if (trace) trace(cd, 'patch'); continue; }
      var sres = null;                                        // this candidate's strength search (none unless over high ground)
      if (high(pt.fr, pt.fc) || high(pt.r, pt.c)) {             // high ground: only a strong low below the cap marks
        if (!(cd.type === 'L' && strongHpa > 0 && lowGround(pt.fr, pt.fc) && lowGround(pt.r, pt.c) && (sres = strongOf(pt)).strong)) {
          if (trace) trace(cd, 'terrain'); continue;
        }
      }
      var prom = promOf(cd);
      if (sres) prom = Math.max(prom, Math.min(ISO_HL_PROM_CAP, sres.prom));   // the window's coarse blocks can read a strong low shallow
      if (prom < ISO_HL_PROM_CODES) { if (trace) trace(cd, 'prominence ' + prom); continue; }
      var clamp = cd.q === 1 ? -1 : cd.q === 255 ? 1 : 0;
      out.push({ type: cd.type, r: pt.r, c: pt.c, v: fdef.lo + (cd.q - 1) * k, q: cd.q, prom: prom, clamp: clamp });
    }
    return out;
  }
  // The projection of an isoValues window: node row -> container y (exact at the nodes, linear between), node column ->
  // x per world copy the view shows (longitudes unwrapped from the window's first column: the date line falls out).
  function isoProjector(g, grid, view) {
    var n = TILE * Math.pow(2, view.z), dlat = grid.dlat * g.stride;
    var lonA = grid.lon0 + g.c0 * grid.dlon, xA = (lonA + 180) / 360 * n, dx = grid.dlon * g.stride / 360 * n;
    var Y = new Float64Array(g.R);
    for (var i = 0; i < g.R; i++) Y[i] = forwardPixel(grid.lat0 + g.r0 * grid.dlat + i * dlat, 0, view.z).y - view.oy;
    var spanX = (g.C - 1) * dx, offs = [];
    if (spanX >= n - 1e-6) {                                  // the whole world: every copy the view touches
      for (var k = Math.floor((view.ox - xA - spanX) / n); k * n + xA <= view.ox + view.w; k++) offs.push(xA + k * n - view.ox);
    } else offs.push(xA + Math.round((view.ox + view.w / 2 - (xA + spanX / 2)) / n) * n - view.ox);
    return { Y: Y, dx: dx, offs: offs, y: function (yy) { var r = Math.floor(yy), fr = yy - r; return fr > 0 && r + 1 < g.R ? Y[r] + (Y[r + 1] - Y[r]) * fr : Y[Math.min(Math.max(r, 0), g.R - 1)]; } };
  }
  // Node units of an isoValues window -> container pixels of the view, once per world copy the view shows: [{level,
  // xy: Float64Array, closed}].
  function projectIsolines(lines, g, grid, view) {
    var pr = isoProjector(g, grid, view), out = [];
    for (var q = 0; q < pr.offs.length; q++) {
      var off = pr.offs[q];
      for (var l = 0; l < lines.length; l++) {
        var p = lines[l].pts, xy = new Float64Array(p.length);
        for (var m = 0; m < p.length; m += 2) { xy[m] = off + p[m] * pr.dx; xy[m + 1] = pr.y(p[m + 1]); }
        out.push({ level: lines[l].level, xy: xy, closed: lines[l].closed });
      }
    }
    return out;
  }
  // The H / L centres in container pixels, every world copy the view shows, kept ISO_HL_EDGE_PX inside the view, then one
  // mark per ISO_HL_PX among the drawn ones: the most prominent first (then the deeper, then the first found), a mark of
  // the same kind within ISO_HL_PX of a kept one or of the other kind whose box overlaps a kept one's goes.
  // -> [{type, x, y, text}] (text = the pressure rounded to the hPa; "<=920" / ">=1060" at the field's clamped ends).
  function projectCentres(cs, grid, view) {
    var n = TILE * Math.pow(2, view.z), all = [], e = ISO_HL_EDGE_PX;
    for (var i = 0; i < cs.length; i++) {
      var ce = cs[i], lng = grid.lon0 + ce.c * grid.dlon, x0 = (lng + 180) / 360 * n - view.ox;
      var y = forwardPixel(grid.lat0 + ce.r * grid.dlat, 0, view.z).y - view.oy;
      if (y < e || y > view.h - e) continue;
      var text = ce.clamp < 0 ? '\u2264' + Math.round(ce.v) : ce.clamp > 0 ? '\u2265' + Math.round(ce.v) : String(Math.round(ce.v));
      var prom = ce.prom === undefined ? ISO_HL_PROM_CAP : ce.prom, depth = ce.q === undefined ? 0 : (ce.type === 'L' ? 256 - ce.q : ce.q);
      for (var x = x0 - Math.floor((x0 - e) / n) * n; x <= view.w - e; x += n) all.push({ type: ce.type, x: x, y: y, text: text, prom: prom, depth: depth, i: i });
    }
    all.sort(function (p, q) { return q.prom - p.prom || q.depth - p.depth || p.i - q.i || p.x - q.x; });
    var kept = [], out = [];
    for (var j = 0; j < all.length; j++) {
      var m = all[j], taken = false, mb = markBox(m);
      for (var h = 0; h < kept.length && !taken; h++) {
        var o = kept[h];
        if (o.type === m.type) { if (Math.hypot(o.x - m.x, o.y - m.y) <= ISO_HL_PX) taken = true; }
        else { var ob = markBox(o); if (mb.x0 < ob.x1 + ISO_LABEL_GAP_PX && ob.x0 < mb.x1 + ISO_LABEL_GAP_PX && mb.y0 < ob.y1 + ISO_LABEL_GAP_PX && ob.y0 < mb.y1 + ISO_LABEL_GAP_PX) taken = true; }
      }
      if (taken) continue;
      kept.push(m); out.push({ type: m.type, x: m.x, y: m.y, text: m.text });
    }
    return out;
  }
  // At most ONE pill per line (owner G32): a line drawn over at least ISO_LABEL_MIN_PX of the view gets its pill at the
  // first of ISO_LABEL_AT (fractions of its length inside the view) where the pill fits: inside the view by
  // ISO_LABEL_EDGE_PX, clear of the H / L marks and of every pill already placed (ISO_LABEL_GAP_PX apart; a grid of
  // ISO_LABEL_GRID_PX buckets, each box filed in every bucket it covers). The pill is rotated along the line, its text
  // kept upright. measure(text) -> width in px. -> [{x, y, w, h, a, text}] (x, y = the pill's centre, a = its angle).
  function placeLabels(paths, view, measure, marks) {
    var out = [], grid = new Map(), G = ISO_LABEL_GRID_PX, P = ISO_LABEL_GAP_PX, cache = {}, hb = [];
    function width(t) { return cache[t] === undefined ? (cache[t] = measure(t)) : cache[t]; }
    function key(gx, gy) { return (gx + 4096) * 8192 + (gy + 4096); }
    function hits(b) {
      for (var gx = Math.floor((b.x0 - P) / G); gx <= Math.floor((b.x1 + P) / G); gx++) {
        for (var gy = Math.floor((b.y0 - P) / G); gy <= Math.floor((b.y1 + P) / G); gy++) {
          var cell = grid.get(key(gx, gy));
          if (!cell) continue;
          for (var i = 0; i < cell.length; i++) {
            var o = cell[i];
            if (b.x0 < o.x1 + P && o.x0 < b.x1 + P && b.y0 < o.y1 + P && o.y0 < b.y1 + P) return true;
          }
        }
      }
      return false;
    }
    function keep(b) {
      for (var gx = Math.floor(b.x0 / G); gx <= Math.floor(b.x1 / G); gx++) {
        for (var gy = Math.floor(b.y0 / G); gy <= Math.floor(b.y1 / G); gy++) {
          var kk = key(gx, gy), cell = grid.get(kk);
          if (cell) cell.push(b); else grid.set(kk, [b]);
        }
      }
    }
    for (var h = 0; marks && h < marks.length; h++) keep(markBox(marks[h]));
    for (var l = 0; l < paths.length; l++) {
      var xy = paths[l].xy, inView = 0, m;
      for (m = 2; m < xy.length; m += 2) if (isoSegIn(xy, m, view)) inView += Math.hypot(xy[m] - xy[m - 2], xy[m + 1] - xy[m - 1]);
      if (inView < ISO_LABEL_MIN_PX) continue;
      var text = String(paths[l].level), w = width(text) + 2 * ISO_STYLE.padX, hh = ISO_STYLE.h;
      for (var a = 0; a < ISO_LABEL_AT.length; a++) {
        var target = inView * ISO_LABEL_AT[a], walked = 0, x = null, y = null, ang = 0;
        for (m = 2; m < xy.length; m += 2) {
          if (!isoSegIn(xy, m, view)) continue;
          var dx = xy[m] - xy[m - 2], dy = xy[m + 1] - xy[m - 1], d = Math.hypot(dx, dy);
          if (walked + d >= target) {
            var t = d ? (target - walked) / d : 0;
            x = xy[m - 2] + dx * t; y = xy[m - 1] + dy * t; ang = Math.atan2(dy, dx);
            break;
          }
          walked += d;
        }
        if (x === null) continue;
        if (ang > Math.PI / 2) ang -= Math.PI; else if (ang <= -Math.PI / 2) ang += Math.PI;   // upright text
        var ca = Math.abs(Math.cos(ang)), sa = Math.abs(Math.sin(ang)), hw = (w * ca + hh * sa) / 2, hv = (w * sa + hh * ca) / 2;
        var b = { x0: x - hw, x1: x + hw, y0: y - hv, y1: y + hv };
        if (b.x0 < ISO_LABEL_EDGE_PX || b.y0 < ISO_LABEL_EDGE_PX || b.x1 > view.w - ISO_LABEL_EDGE_PX || b.y1 > view.h - ISO_LABEL_EDGE_PX) continue;
        if (hits(b)) continue;
        keep(b);
        out.push({ x: x, y: y, w: w, h: hh, a: ang, text: text });
        break;
      }
    }
    return out;
  }
  // A segment ending at xy[m] lies inside the view (both ends).
  function isoSegIn(xy, m, view) {
    return xy[m] >= 0 && xy[m] <= view.w && xy[m + 1] >= 0 && xy[m + 1] <= view.h && xy[m - 2] >= 0 && xy[m - 2] <= view.w && xy[m - 1] >= 0 && xy[m - 1] <= view.h;
  }
  // The box an H / L mark covers (the letter above, the pressure below its centre).
  function markBox(k) { return { x0: k.x - 16, x1: k.x + 16, y0: k.y - 15, y1: k.y + 16 }; }
  // A rounded rectangle's path (a pill, a mark's clearing).
  function roundRectPath(ctx, x0, y0, w, h, r) {
    ctx.moveTo(x0 + r, y0); ctx.lineTo(x0 + w - r, y0); ctx.quadraticCurveTo(x0 + w, y0, x0 + w, y0 + r);
    ctx.lineTo(x0 + w, y0 + h - r); ctx.quadraticCurveTo(x0 + w, y0 + h, x0 + w - r, y0 + h);
    ctx.lineTo(x0 + r, y0 + h); ctx.quadraticCurveTo(x0, y0 + h, x0, y0 + h - r);
    ctx.lineTo(x0, y0 + r); ctx.quadraticCurveTo(x0, y0, x0 + r, y0); ctx.closePath();
  }
  // One canvas pass: every line halo first, then every core (two strokes), then the pills (rotated along their lines,
  // drawn over them), then the H / L marks, the lines cleared under each mark's box (ISO_HL_CLEAR_PX around it) so a mark
  // stays legible inside a deep cyclone's packed rings (G32 re-check H-8). A line is drawn as quadratic curves through the
  // midpoints of its segments, with its points as control points (a smooth curve that keeps an open line's ends).
  function drawIsobars(ctx, paths, labels, st, marks) {
    st = st || ISO_STYLE;
    ctx.beginPath();
    for (var l = 0; l < paths.length; l++) {
      var xy = paths[l].xy, n = xy.length;
      if (n < 4) continue;
      if (n === 4) { ctx.moveTo(xy[0], xy[1]); ctx.lineTo(xy[2], xy[3]); continue; }
      if (paths[l].closed) {                                   // closed: the last point repeats the first
        ctx.moveTo((xy[0] + xy[2]) / 2, (xy[1] + xy[3]) / 2);
        for (var m = 2; m < n - 2; m += 2) ctx.quadraticCurveTo(xy[m], xy[m + 1], (xy[m] + xy[m + 2]) / 2, (xy[m + 1] + xy[m + 3]) / 2);
        ctx.quadraticCurveTo(xy[0], xy[1], (xy[0] + xy[2]) / 2, (xy[1] + xy[3]) / 2);
      } else {
        ctx.moveTo(xy[0], xy[1]);
        for (var k = 2; k < n - 2; k += 2) ctx.quadraticCurveTo(xy[k], xy[k + 1], (xy[k] + xy[k + 2]) / 2, (xy[k + 1] + xy[k + 3]) / 2);
        ctx.lineTo(xy[n - 2], xy[n - 1]);
      }
    }
    ctx.lineJoin = 'round'; ctx.lineCap = 'round';
    ctx.strokeStyle = st.halo; ctx.lineWidth = st.haloW; ctx.stroke();
    ctx.strokeStyle = st.core; ctx.lineWidth = st.coreW; ctx.stroke();
    if (labels && labels.length) {
      ctx.font = st.font; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
      for (var i = 0; i < labels.length; i++) {
        var b = labels[i], x0 = -b.w / 2, y0 = -b.h / 2, r = Math.min(st.r, b.h / 2);
        ctx.save(); ctx.translate(b.x, b.y); if (b.a) ctx.rotate(b.a);
        ctx.beginPath(); roundRectPath(ctx, x0, y0, b.w, b.h, r);
        ctx.fillStyle = st.pill; ctx.fill();
        ctx.fillStyle = st.text; ctx.fillText(b.text, 0, 0.5);
        ctx.restore();
      }
    }
    if (marks && marks.length) {
      ctx.save();
      ctx.globalCompositeOperation = 'destination-out'; ctx.fillStyle = '#000'; ctx.beginPath();
      for (var g = 0; g < marks.length; g++) { var mb = markBox(marks[g]), e = ISO_HL_CLEAR_PX; roundRectPath(ctx, mb.x0 - e, mb.y0 - e, mb.x1 - mb.x0 + 2 * e, mb.y1 - mb.y0 + 2 * e, 7); }
      ctx.fill();
      ctx.restore();
      ctx.save();
      ctx.textAlign = 'center'; ctx.textBaseline = 'middle'; ctx.fillStyle = st.hlText;
      ctx.shadowColor = st.hlShadow; ctx.shadowBlur = st.hlBlur;
      for (var h = 0; h < marks.length; h++) {
        ctx.font = st.hlFont; ctx.fillText(marks[h].type, marks[h].x, marks[h].y - 5);
        ctx.font = st.hlValueFont; ctx.fillText(marks[h].text, marks[h].x, marks[h].y + 9);
      }
      ctx.restore();
    }
  }

  // The isobar canvas. A plain object like FlowAnimator: the controller (plan section 46, step 4) attaches it under the
  // wind overlay and hands it the pressure frame of the step on the map (setData) or clears it (clearData: at once, so no
  // old lines stay under a new time). It redraws on a new frame and on every view change, in one animation frame after
  // the event (never on the stack of a frame landing); it is hidden while the map zooms.
  function IsobarLayer(map) {
    this.map = map; this.canvas = null; this.ctx = null; this.dpr = 1; this.v = null;
    this.frame = null; this.grid = null; this.entry = null; this.fdef = null; this.S = null; this.P = {}; this.terrain = null;
    this.pending = null; this._listeners = []; this.lastMs = 0; this.drawn = { lines: 0, labels: 0, hl: 0 };
  }
  IsobarLayer.prototype.raf = FlowAnimator.prototype.raf;
  IsobarLayer.prototype.caf = FlowAnimator.prototype.caf;
  IsobarLayer.prototype._on = FlowAnimator.prototype._on;
  IsobarLayer.prototype.attach = function () {
    if (this.canvas) return;
    var self = this, map = this.map, pane = map.getPane('ovIsoPane');
    if (!pane) { pane = map.createPane('ovIsoPane'); pane.style.zIndex = 280; pane.style.pointerEvents = 'none'; }
    this._newCanvas(pane);
    this._on('zoomstart', function () { if (self.canvas) self.canvas.style.visibility = 'hidden'; });
    this._on('zoomend', function () {
      if (self.canvas && self.canvas.parentNode && isWebKit()) self._newCanvas(self.canvas.parentNode);   // as FlowAnimator (Safari's dropped backing store)
      if (self.canvas) self.canvas.style.visibility = '';
      self._schedule();
    });
    this._on('moveend', function () { self._schedule(); });
    this._on('viewreset', function () { self._schedule(); });
    this._on('resize', function () { self._schedule(); });
  };
  IsobarLayer.prototype._newCanvas = function (pane) {
    var old = this.canvas, cv = document.createElement('canvas'), ctx = null;
    cv.className = 'ov-iso leaflet-zoom-hide';
    try { ctx = cv.getContext('2d') || null; } catch (e) { ctx = null; }
    if (!ctx && old && this.ctx) return;
    if (old) { try { old.width = 0; old.height = 0; } catch (e) { /* gone */ } if (old.remove) old.remove(); }
    pane.appendChild(cv); this.canvas = cv; this.ctx = ctx; this.v = null;
  };
  IsobarLayer.prototype.detach = function () {
    var self = this;
    if (this.pending !== null) { this.caf(this.pending); this.pending = null; }
    this._listeners.forEach(function (l) { self.map.off(l[0], l[1]); }); this._listeners = [];
    if (this.canvas) { if (this.canvas.remove) this.canvas.remove(); this.canvas = null; this.ctx = null; }
    this.frame = null; this.grid = null; this.entry = null; this.fdef = null; this.S = null; this.P = {}; this.v = null;
  };
  // The half grid's terrain codes (terrain_half.png; null = none: marks over high ground as well): a redraw if a frame is on.
  IsobarLayer.prototype.setTerrain = function (q) {
    var t = q && q.length === 720 * 361 ? q : null;
    if (t === this.terrain) return;
    this.terrain = t;
    if (this.frame) this._schedule();
  };
  // The pressure frame of the step on the map (half grid, the manifest's `pres` field definition, its manifest entry).
  IsobarLayer.prototype.setData = function (frame, grid, entry, fdef) {
    if (this.frame === frame && this.entry === entry) return;
    this.frame = frame; this.grid = grid; this.entry = entry || null; this.fdef = fdef;
    this._schedule();
  };
  IsobarLayer.prototype.clearData = function () {
    this.frame = null; this.grid = null; this.entry = null; this.fdef = null;
    if (this.pending !== null) { this.caf(this.pending); this.pending = null; }
    this._clear();
  };
  IsobarLayer.prototype._clear = function () { if (this.ctx && this.v) this.ctx.clearRect(0, 0, this.v.w, this.v.h); this.drawn = { lines: 0, labels: 0, hl: 0 }; };
  IsobarLayer.prototype._schedule = function () {
    var self = this;
    if (this.pending !== null || !this.canvas) return;
    this.pending = this.raf(function () { self.pending = null; self._draw(); });
  };
  // The pressure (hPa) at a point from the frame on the map (the readout's "· 1004 hPa"), or null.
  IsobarLayer.prototype.valueAt = function (lat, lng) {
    var f = this.frame, g = this.grid;
    if (!f || !g || !this.fdef) return null;
    var r = (lat - g.lat0) / g.dlat;
    if (!(r >= 0 && r <= f.rows - 1)) return null;
    var cp = new Float64Array([((((lng - g.lon0) / g.dlon) % f.cols) + f.cols) % f.cols]), out = new Float64Array(1);
    sampleRow(f, false, r, cp, 1, out, 0);
    return out[0] ? this.fdef.lo + (out[0] - 1) / 254 * (this.fdef.hi - this.fdef.lo) : null;
  };
  // One redraw: the nodes under the view, the interval for the zoom and the view's width, the lines (smoothed), the H / L
  // marks (from the frame's own codes: exact centres and pressures; by prominence, on the lines' stride, none on high
  // ground), the pills (only from ISO_LABEL_MIN_Z and never while the whole world is in view).
  IsobarLayer.prototype._draw = function () {
    if (!this.canvas || !this.ctx) return;
    var v = viewOf(this.map, null);
    this.v = v; this.dpr = placeCanvas(this.canvas, this.ctx, this.map, v);
    this.ctx.clearRect(0, 0, v.w, v.h);
    if (!this.frame || !this.grid || !this.fdef) { this.drawn = { lines: 0, labels: 0, hl: 0 }; return; }
    var t0 = nowMs(), ctx = this.ctx, grid = this.grid, worldPx = TILE * Math.pow(2, v.z);
    var span = v.w / worldPx * 360, interval = isoInterval(v.z, span);
    var win = nodeWindow(v, grid, this.frame, 0), nodePx = worldPx * Math.abs(grid.dlon) / 360;
    var stride = isoStride(v.z, grid.dlon), g = isoValues(this.frame, this.fdef, win, stride, this.S);
    this.S = g.scratch;
    var paths = projectIsolines(traceIsolines(g.V, g.R, g.C, interval), g, grid, v);
    var rad = Math.max(1, Math.round(ISO_HL_PX / nodePx));
    var marks = projectCentres(findCentres(this.frame, this.fdef, win, rad, { stride: stride, margin: Math.max(2 * rad, 16), grid: grid,
      terrain: this.terrain, terrainMin: ISO_TERRAIN_M / TERRAIN_M_PER_CODE, strongLowHpa: ISO_STRONG_LOW_HPA, terrainCap: ISO_TERRAIN_CAP_M / TERRAIN_M_PER_CODE,
      promNodesMax: ISO_PROM_NODES_MAX, scratch: this.P }), grid, v);
    var labels = [];
    if (v.z >= ISO_LABEL_MIN_Z && span < ISO_WORLD_DEG) {
      ctx.font = ISO_STYLE.font;
      labels = placeLabels(paths, v, function (t) { return ctx.measureText(t).width; }, marks);
    }
    drawIsobars(ctx, paths, labels, ISO_STYLE, marks);
    this.drawn = { lines: paths.length, labels: labels.length, hl: marks.length };
    this.lastMs = Math.round((nowMs() - t0) * 10) / 10;
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
    // opts.sst: the sea surface temperature field is offered (plan section 44; the page's SST_OVERLAY setting). It is an
    // OBSERVATION: mode 'obs' while it is shown (no run, no frames, no player, no model choice), 'model' otherwise.
    this.sst = opts.sst ? sstStore(this.root) : null;
    this.mode = 'model';
    var s = saved();
    // opts.sat / opts.swell (plan section 81; the page's ALT_OVERLAY / SARSWELL settings): the satellite readings. The
    // layer's own entry ("Satellite waves", mode 'obs', field 'sat') shows the height dots or, with the swell data offered,
    // the swell arrows (this.satKind, remembered per tab). (The "Satellite readings" box under Wave height was removed by
    // the owner on 2026-11-05: a 3-h window of tracks covered too few frames to help.)
    this.sat = opts.sat ? satStore(this.root, 'alt') : null;
    this.swell = opts.sat && opts.swell ? satStore(this.root, 'swell') : null;
    this.satLayer = null; this.satTimer = null;
    this.satKind = this.swell && s.satKind === 'swell' ? 'swell' : 'heights';
    // opts.grid (plan section 82; the page's ALT_GRID setting): the "Gridded map" box of the Satellite waves entry
    // (this.satGrid, per tab, off by default): the newest step of Copernicus Marine's level-4 gridded wave height drawn
    // in the entry's field layer, under the dots / arrows
    this.grid = opts.sat && opts.grid ? gridStore(this.root) : null;
    this.satGrid = !!(this.grid && s.satGrid === true);
    // opts.radar (plan section 85; the page's RADAR_OVERLAY setting): its own store, layer and player while the Radar entry is shown
    this.radar = opts.radar ? radarStore(this.root) : null; this.radarLayer = null; this.radarPlayer = null;
    // opts.bathy (plan section 93b; the page's BATHY_OVERLAY setting): its store and its own layer while the Bathymetry entry is shown
    this.bathy = opts.bathy ? bathyStore(this.root) : null; this.bathyLayer = null; this._bathyTimer = null;
    this.bathyOpacity = readBathyOpacity();                   // its own opacity, kept on the device
    this.relief = null; this._base = 'imagery'; this._reliefFailed = 0;   // opts.baseLayer: the page's imagery (the wind swaps it)
    this.speed = speedOf(s.speed);
    this.model = modelOf(s.model, this.models);
    this._carry = null;                                      // the time and play state carried across a model switch
    this._switchT = null;                                    // the time the viewer chose, kept across consecutive switches (G30-15)
    // Contours (wave height, period) and the particle animation (every field) are always on (owner, 2026-09-26: no
    // checkboxes); the animation stays off under a reduced-motion setting and on runs without direction data.
    this.contours = true;
    // the particles' strength (the page's settings slider, plan section 46): 0 = no particles (nothing animates, no
    // direction frames are fetched), 1 = today's look; never saved by the module (the page keeps it)
    this.particleOpacity = particleOpacityOf(opts.particles);
    this.radarOpacity = readRadarOpacity();                    // the radar's own opacity, kept on the device (section 89)
    this.anim = this.particleOpacity > 0;
    this.dcache = new FrameCache(MAX_DECODED); this.flow = null; this.dres = null;   // direction frames and their animator
    // The isobars (plan section 46): on unless the page says otherwise (its settings gear); the pressure frames' cache
    // and their layer (made under wind when the run has the field).
    this.isobars = opts.isobars !== false;
    this.pcache = new FrameCache(MAX_DECODED); this.iso = null;
    // The Great Lakes (plan section 50; the page's GLWU_OVERLAY setting): the lake run's store, the lakes coast set, and
    // the two lake frame kinds' caches; _lakeWait = the step's wait record for its lake frames (capped like the pressure's).
    this.lake = opts.glwu ? lakeStore(this.root) : null;
    this.lakeCoast = opts.glwu ? coastStore(this.root + '/static/coast/lakes-v1', LAKES_TIER0) : null;
    this.lcache = new FrameCache(MAX_DECODED); this.ldcache = new FrameCache(MAX_DECODED); this._lakeWait = null; this._lakeNear = null;
    // The lakes' shores under wind (plan section 51; always on: owner, 2026-10-23): its own coast set, loaded only when
    // first wanted (wind, tile zoom 4 or more).
    this.lakeOutCoast = opts.coast ? coastStore(this.root + '/' + LAKEOUT_PREFIX, LAKEOUT_TIER0) : null;
    this._lakeGateNear = null; this._lakeHs = null;        // the reach the view gate last acted on (G33 F1b); the hover's lake height frame (B-2)
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
    this.cache.clear(); this.dcache.clear(); this.pcache.clear(); this.lcache.clear(); this.ldcache.clear(); this.unavailable = {}; this.frameIndex = null; this.target = null;
    this.transientFails = 0; this._staleShown = false; this.wasPlaying = false;
    if (this.layer) this.layer.clear();
    if (this.flow) { this.flow.clearLakeData(); this.flow.clearData(); }
    if (this.iso) this.iso.clearData();
    this._dropLakeHs();
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
    if (this.mode === 'obs') {                               // (no dropdown on the observation's panel: only a call does this)
      this.model = key; save({ model: key });
      this.manifest = null; this.pointer = null; this.pointerAt = 0; this.newerRun = null; this.n = 0;
      this.cache.clear(); this.dcache.clear(); this.pcache.clear(); this.lcache.clear(); this.ldcache.clear(); this.unavailable = {};
      return;
    }
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
    if (hasObs(fieldName)) return this._mountObs(fieldName, restore);
    if (restore) this._pendingRestore = saved();
    if (this.mode === 'obs') this._leaveObs();
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
    if (this.satLayer && fieldName !== 'hs') this._detachSat();     // the readings never stay alone under "Loading model frame…" (G51 P2)
    if (!this.layer) {
      this.layer = new ModelGridLayer({ opacity: this._opacityFor(fieldName) }).addTo(this.map);
      this._bindReadout();
      this._bindMap();
      this._bindDocument();
    } else if (this.layer.field !== fieldName) {
      this.layer.clear();                                  // never one field's picture under another's label
      if (this.flow) { this.flow.clearLakeData(); this.flow.clearData(); }
      if (this.iso) this.iso.clearData();
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
    this._syncLakeOutlines();
    if (!waitCoast) coastP.then(function (store) {                           // (not tied to this mount's signal: an Update meanwhile keeps them)
      if (!self.layer || !store || self.layer._coast === store) return;
      self.layer.setCoast(store);
      if (self.layer.hasFrame()) self._attribute();
    }, function () {});
    var idx = null, resume = false;
    this._loadManifest(sig).then(function (m) {
      if (!m.fields[fieldName] || !RAMPS[fieldName]) throw new Error('layer "' + fieldName + '" is not in this run');
      if (sig.aborted || !self.layer) throw abortError();
      self.n = m.frames.length;
      var rIdx = prevValid === null && st ? restoreIndex(m, st, Date.now()) : null;
      idx = prevValid === null ? (carry ? nearestIndex(m, carry.t) : rIdx !== null ? rIdx : pickFrame(m)) : m.run === prevRun ? Math.min(keep, self.n - 1) : nearestIndex(m, prevValid);
      resume = carry ? carry.playing : rIdx !== null && st.playing === true;
      self.res = wantHalf(self.map.getZoom(), self._dims().w, fieldName) ? 'half' : 'full';
      if (!waitCoast) return [m, undefined];
      // the first frame downloads beside the coastline; the first draw waits for the coastline at most COAST_FIRST_MS
      // (owner, G34 re-check R-1: on a slow link the waves come unclipped, then clipped when the coastline lands)
      self._ensure(idx).catch(function () {});
      return self._coastFirst(coastP).then(function (store) { return [m, store]; });
    }).then(function (ms) {
      var m = ms[0], store = ms[1];
      if (sig.aborted || !self.layer) throw abortError();
      if (store !== undefined && self.layer._coast !== (store || null)) self.layer.setCoast(store);
      self._ensureFlow();
      self._ensureIso();
      self._loadLakes(m);
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
  // ---- the observations (plan sections 44 and 81): sea surface temperature, the satellite readings ----
  // Shown beside the model, never as a frame of a run: the model's manifest, its caches and its time stay (a switch back
  // is instant and lands at the time the viewer had: the carry, as a model switch does), the player stops, the particles
  // go, the panel shows the observation's own lines. Errors take the frames' error panel (Retry mounts it again). The
  // prologue is one for every kind (OBS_DEFS); the load is the kind's own (_loadSst: the layer's SOURCE; _loadSat: the
  // SatLayer in its own pane).
  Overlay.prototype._mountObs = function (kind, restore) {
    var self = this;
    if (restore) this._pendingRestore = saved();
    if (this.mode !== 'obs' && this.manifest && this.frameIndex !== null && this.field) {
      var keep = this._anchorIndex(), fr = this.manifest.frames[keep];
      var t = this._switchT !== null ? this._switchT : fr ? Date.parse(fr.valid_utc) : null;
      if (t !== null) { this._carry = { t: t, playing: !!(this.playing || this.wasPlaying) }; this._switchT = t; }
    }
    this.pause();                                            // the model's play state is saved first (its field still set)
    // ... then the field the page saved before this mount ran, and the play state the switch carries (a reload here and
    // then a model field resumes as the in-tab switch back does; G31-7)
    if (this._carry) save({ field: kind, playing: !!this._carry.playing }); else save({ field: kind });   // (RC-1)
    if (this.lineTimer) { clearInterval(this.lineTimer); this.lineTimer = null; }   // the run line is not shown (G31-16)
    if (!this.map.getPane('modelPane')) {
      var pane = this.map.createPane('modelPane'); pane.style.zIndex = 250; pane.style.pointerEvents = 'none';
    }
    this.abortAll();
    var was = this.field;
    this.field = kind; this.mode = 'obs'; this.frameIndex = null; this.target = null; this.wasPlaying = false;
    this.abort = new AbortController();
    var sig = this.abort.signal;
    if (!this.layer) {
      this.layer = new ModelGridLayer({ opacity: this._opacityFor(kind) }).addTo(this.map);
      this._bindReadout();
      this._bindMap();
      this._bindDocument();
    } else this.layer.clear();                               // never another field's picture under this label
    if (was === 'sst' && kind !== 'sst' && this.sst) this.sst.release();   // leaving SST for another observation releases its cells (G31-9)
    this._detachRadar();                                     // its loop, layer and pictures go (the Radar entry makes them anew)
    this._detachBathy();                                     // ... and the depths' (the Bathymetry entry makes them anew)
    if (kind !== 'sat') this._detachSat();                   // the satellite layer serves its own entry alone (the other observation entry drops it)
    else if (this.satLayer) this.satLayer.setWindow(null);  // ... and its window goes with the picture (its own follows the load)
    this._ensureFlow();                                      // no direction field: the particles go
    this._ensureIso(); this.pcache.clear();                  // no pressure under an observation: the isobars and their frames go (G32 A-P1-1)
    this._syncLakeOutlines();                                // no lake outlines under an observation: the set and its masks go (V7-5)
    if (this.readout) this.readout.hidden = true;
    this._unattribute();
    this._syncLook();                                        // the imagery (the wind's relief goes)
    var def = OBS_DEFS[kind];
    var own = OBS_LOAD[kind];                                 // each kind's own store and loader: never another kind's (G30-9)
    if (!own || !this[own[0]]) { this._fail(sig, new Error(def.label + ' is not offered')); return; }
    this.render({ state: 'loading' });
    this[own[1]](sig).catch(function (err) { self._fail(sig, err); });
  };
  Overlay.prototype._loadSst = function (sig) {
    var self = this, coastP = this.coast ? this.coast.load() : Promise.resolve(null);
    return Promise.all([this.sst.load(true), this._coastFirst(coastP)]).then(function (res) {      // a viewer's pick or Retry: no cooldown (G31-2); the
                                                                         // coastline waited for at most COAST_FIRST_MS (re-check R-1)
      var st = res[0], coast = res[1];
      if (sig.aborted || self.field !== 'sst' || self.mode !== 'obs' || !self.layer) throw abortError();
      if (!st) throw new Error('sea surface temperature cannot be loaded right now');
      if (coast !== undefined && self.layer._coast !== (coast || null)) self.layer.setCoast(coast);
      self.layer.setSource(st, true);
      self.layer.setContours(self.contours ? self._contourCfg() : null, true);
      self.layer.setFrame(st.frame, st.grid, 'sst', SST_FDEF, self._sstLut(), null);
      if (!self.runTimer) self.runTimer = setInterval(function () { self._checkRun(); }, RUN_CHECK_MS);
      self._attribute();
      self._syncLook();
      self.render({ state: 'ready' });
    });
  };
  // The satellite layer's own entry: the store of the kind chosen (heights, or swell when offered), its pointer, then the
  // SatLayer over the last 24 h of readings (newest at the far end of the fade); the chunks land one by one (the panel's
  // readings line follows them, in place). No coastline wait: dots are drawn over the imagery, nothing is clipped.
  Overlay.prototype._loadSat = function (sig) {
    var self = this, store = this._satStoreFor();
    return store.load(true).then(function (st) {
      if (sig.aborted || self.field !== 'sat' || self.mode !== 'obs' || !self.layer) throw abortError();
      if (!st) throw new Error(store.status === 'unsupported' ? 'satellite readings are not supported in this browser' : 'satellite readings cannot be loaded right now');
      self._attachSat(st, self.satKind === 'swell' ? 'arrows' : 'dots');
      self._syncSatOwnWindow();
      self._syncGrid(true);                                  // the gridded map under the readings when its box is on (a viewer's pick: no cooldown)
      if (!self.runTimer) self.runTimer = setInterval(function () { self._checkRun(); }, RUN_CHECK_MS);
      self._attribute();
      self._syncLook();
      self.render({ state: 'ready' });
    });
  };
  // The observation kinds' stores and loaders, by kind: [the Overlay's store property, its loader].
  var OBS_LOAD = { sst: ['sst', '_loadSst'], sat: ['sat', '_loadSat'], radar: ['radar', '_loadRadar'], bathy: ['bathy', '_loadBathy'] };
  // ---- the bathymetry layer (plan section 93b) ----
  // regions.json (a viewer's pick or Retry: no cooldown), then the BathyLayer; the coastline (GEBCO's land mask) follows when it lands.
  Overlay.prototype._loadBathy = function (sig) {
    var self = this, coastP = this.coast ? this.coast.load() : Promise.resolve(null);
    return this.bathy.load(true).then(function (st) {
      if (sig.aborted || self.field !== 'bathy' || self.mode !== 'obs' || !self.layer) throw abortError();
      if (!st) throw new Error('bathymetry cannot be loaded right now');
      self._attachBathy(st);
      coastP.then(function (c) { if (c && self.bathyLayer && self.field === 'bathy' && self.mode === 'obs') self.bathyLayer.setCoast(c); });
      self._attribute();
      self._syncLook();
      self.render({ state: 'ready' });
    });
  };
  Overlay.prototype._attachBathy = function (store) {
    var self = this;
    if (!this.map.getPane('obsPane')) { var pane = this.map.createPane('obsPane'); pane.style.zIndex = 260; pane.style.pointerEvents = 'none'; }
    this._detachBathy(true);
    this.bathyLayer = new BathyLayer();
    this.bathyLayer.unit = this.opts.getUnit() === 'Metric' ? 'Metric' : 'US';
    this.bathyLayer.onRedraw = function () { self._bathyViewSoon(); };
    this.bathyLayer.addTo(this.map);
    this.bathyLayer.setOpacity(this.bathyOpacity / 100);
    this.bathyLayer.setStore(store);
  };
  // keep: the store keeps its decoded tiles (a re-attach); else they go (leaving the entry, Off, a failure).
  Overlay.prototype._detachBathy = function (keep) {
    clearTimeout(this._bathyTimer); this._bathyTimer = null;
    if (this.bathyLayer) {
      var bl = this.bathyLayer; this.bathyLayer = null;
      bl.onRedraw = null; bl.setCoast(null); bl.setStore(null); this.map.removeLayer(bl);   // the coastline's hooks go back to the wave layer
    }
    if (this.bathy) { this.bathy.abortAll(); if (!keep) this.bathy.release(); }
  };
  // The panel's "Sources here" line and the credit follow the view, a moment after the tiles are drawn.
  Overlay.prototype._bathyViewSoon = function () {
    var self = this;
    if (this._bathyTimer || !this.bathyLayer) return;
    this._bathyTimer = setTimeout(function () { self._bathyTimer = null; self._bathyView(); }, BATHY_VIEW_MS);
  };
  Overlay.prototype._bathyView = function () {
    if (this.field !== 'bathy' || this.mode !== 'obs' || !this.bathyLayer) return;
    this._attribute();
    if (this.ui && this.ui.bathySources) this.ui.bathySources.textContent = this._bathySourcesText();
  };
  Overlay.prototype._bathySources = function () { return this.bathyLayer ? this.bathyLayer.sourcesInView() : []; };
  // "Sources here: NOAA CUDEM 2021 (3 m grid); GEBCO 2026 Grid (460 m grid)": one entry per printed text (Puerto Rico's
  // and the Virgin Islands' grids read the same); "loading…" while files are on the way; a note while a file a tile in
  // view asked for is waiting out its retry after a failure (G61 B-9).
  Overlay.prototype._bathySourcesText = function () {
    var bl = this.bathyLayer, st = bl && bl.store, list = this._bathySources(), seen = {}, out = [];
    list.forEach(function (s) {
      var t = bathyLabel(s) || 'source';
      if (s.res_m) t += ' (' + bathyNum(s.res_m, true) + ' m grid)';
      if (!seen[t]) { seen[t] = true; out.push(t); }
    });
    var txt = 'Sources here: ' + (out.length ? out.join('; ') : !st ? '…' : st.busy() || bl.pendingInView() ? 'loading…' : 'none in view');
    if (bl && bl.coolingInView()) txt += '. Some depth files could not be loaded; trying again shortly.';
    return txt;
  };
  // "18.4 ft · NOAA CUDEM 2021 · approximate"; "· estimated" on filled pixels; "~140 ft (GEBCO, regional estimate)";
  // "dries at mean sea level"; nothing where no depth is drawn.
  Overlay.prototype._bathyReadout = function (r) {
    if (!r || r.kind !== 'depth') return null;
    var unit = this.opts.getUnit(), s = r.src;
    if (!(r.depth > 0)) return 'dries at mean sea level' + (s ? ' · ' + bathyLabel(s) : '');
    var v = unit === 'Metric' ? r.depth : r.depth * 3.28084, lab = unit === 'Metric' ? 'm' : 'ft';
    if (r.coarse) return '~' + bathyNum(v, true) + ' ' + lab + ' (' + (s ? bathyShort(s) : 'GEBCO') + ', regional estimate)';
    var txt = bathyNum(v) + ' ' + lab;
    return txt + (s ? ' · ' + bathyLabel(s) : '') + (r.filled ? ' · estimated' : ' · approximate');
  };
  Overlay.prototype._renderBathy = function (st, host, compact, focusCls, mapH) {
    var self = this, unit = this.opts.getUnit();
    this._obsShell(st, host, compact, focusCls, mapH, BATHY_HEAD, 'Bathymetry', function (body) {
      var leg = buildLegend('bathy', BATHY_FDEF, unit); leg.className += ' ov-bathy-legend'; body.appendChild(leg);
      var src = mk('div', 'ov-meta ov-bathy-sources', self._bathySourcesText()); body.appendChild(src); self.ui.bathySources = src;
      body.appendChild(mk('div', 'ov-meta ov-bathy-note', BATHY_NOTE));
      body.appendChild(self._bathyOpacityRow());
    });
  };
  // The transparency slider under the scale (the radar's pattern): 0 % (fully opaque) to 80 % (faint).
  Overlay.prototype._bathyOpacityRow = function () {
    var self = this, row = mk('label', 'ov-row ov-radar-opacity ov-bathy-opacity'), name = mk('span', 'ov-radar-opacity-name', 'Transparency');
    var range = mk('input', 'ov-radar-opacity-range ov-bathy-opacity-range'), out = mk('span', 'ov-radar-opacity-value');
    range.type = 'range'; range.min = '0'; range.max = String(100 - BATHY_OPACITY_MIN); range.step = '5'; range.setAttribute('autocomplete', 'off');
    function show() {
      var t = 100 - self.bathyOpacity;
      range.value = String(t); out.textContent = t + '%'; range.setAttribute('aria-valuetext', t + '% transparent');
    }
    show();
    range.addEventListener('input', function () { self.setBathyOpacity(100 - (+range.value)); show(); });
    row.appendChild(name); row.appendChild(range); row.appendChild(out);
    this.ui.bathyOpacity = range;
    return row;
  };
  // The layer's opacity, 20-100 %: kept on the device; the open layer follows at once (one CSS opacity: no redraw).
  Overlay.prototype.setBathyOpacity = function (pct) {
    var n = bathyOpacityOf(pct);
    this.bathyOpacity = n; writeBathyOpacity(n);
    if (this.bathyLayer) this.bathyLayer.setOpacity(n / 100);
  };
  // Back to a model field: the layer samples frames again; the stores stop their downloads (their samples stay).
  Overlay.prototype._leaveObs = function () {
    this.mode = 'model';
    if (this.layer) this.layer.clear();                      // (clear() also detaches the source)
    if (this.sst) this.sst.release();                        // the cells go; the day stays (owner, G31-9)
    this._detachSat();                                       // the own entry's layer goes
    this._detachRadar();
    this._detachBathy();
    this._unattribute();
  };
  // ---- the radar layer (plan section 85) ----
  Overlay.prototype._loadRadar = function (sig) {
    var self = this;
    return this.radar.load(true).then(function (st) {
      if (sig.aborted || self.field !== 'radar' || self.mode !== 'obs' || !self.layer) throw abortError();
      if (!st) throw new Error('radar cannot be loaded right now');
      self._attachRadar(st);
      self._attribute();
      self._syncLook();
      self.render({ state: 'ready' });
    });
  };
  // The error panel asks again by itself while it shows, once a minute, quietly: a tab left open recovers.
  Overlay.prototype._radarRecover = function () {
    var self = this;
    clearTimeout(this.radarRecoverTimer);
    this.radarRecoverTimer = setTimeout(function () {
      self.radarRecoverTimer = null;
      if (self.field !== 'radar' || self.mode !== 'obs' || self.radarPlayer) return;
      if (typeof document !== 'undefined' && document.hidden) { self._radarRecover(); return; }
      self.radar.load(true).then(function (st) {
        if (self.field !== 'radar' || self.mode !== 'obs' || self.radarPlayer) return;
        if (st) self._retry(); else self._radarRecover();
      });
    }, RADAR_RECOVER_MS);
  };
  Overlay.prototype._attachRadar = function (store) {
    clearTimeout(this.radarRecoverTimer); this.radarRecoverTimer = null;
    if (!this.map.getPane('obsPane')) { var pane = this.map.createPane('obsPane'); pane.style.zIndex = 260; pane.style.pointerEvents = 'none'; }
    this._detachRadar();
    store.maxBytes = this.map.getContainer().clientWidth < RADAR_SMALL_PX ? RADAR_MAX_BYTES_SMALL : RADAR_MAX_BYTES;   // a small screen needs fewer cells
    this.radarLayer = new RadarLayer().addTo(this.map);
    this.radarLayer.setOpacity(this.radarOpacity / 100);
    this.radarLayer.setStore(store);
    this.radarPlayer = new RadarPlayer(this, store, this.radarLayer);
    this.radarPlayer.start();
  };
  Overlay.prototype._detachRadar = function () {
    clearTimeout(this.radarRecoverTimer); this.radarRecoverTimer = null;
    if (this.radarPlayer) { this.radarPlayer.stop(); this.radarPlayer = null; }
    if (this.radar) this.radar.release();
    if (this.radarLayer) { this.radarLayer.store = null; this.map.removeLayer(this.radarLayer); this.radarLayer = null; }
  };
  Overlay.prototype._radarView = function () { if (this.radarPlayer) this.radarPlayer.viewChanged(); };
  // The sources of the regions in view that have frames: what the head and the credit name.
  Overlay.prototype._radarSources = function () {
    var p = this.radarPlayer, st = this.radar, out = [];
    if (!p || !st || !st.pointer) return out;
    var regs = st.regionsIn(p.view());
    for (var i = 0; i < regs.length; i++) {
      var s = st.pointer.regions[regs[i]].src;
      if (st.newestOf(regs[i]) && typeof s === 'string' && out.indexOf(s) < 0) out.push(s);
    }
    return out;
  };
  // A region whose types can be read is in view with a frame: the snow and mix bars mean something here.
  Overlay.prototype._radarTyped = function () {
    var p = this.radarPlayer, st = this.radar;
    if (!p || !st || !st.pointer || !st.typed) return false;
    var regs = st.regionsIn(p.view());
    for (var i = 0; i < regs.length; i++) if (radarTypedDef(st, st.pointer.regions[regs[i]]) && st.newestOf(regs[i])) return true;
    return false;
  };
  // A thin bar on the legend's dBZ axis, its name on it.
  function radarTypeBar(ramp, label) {
    var row = mk('div', 'ov-radar-type'), cv = mk('canvas'); cv.width = 256; cv.height = 1; row.appendChild(cv);
    var lut = legendBar(ramp, RADAR_FDEF.legend), ctx = cv.getContext('2d'), im = ctx.createImageData(256, 1);
    for (var i = 0; i < 256; i++) { im.data[i * 4] = lut[i * 3]; im.data[i * 4 + 1] = lut[i * 3 + 1]; im.data[i * 4 + 2] = lut[i * 3 + 2]; im.data[i * 4 + 3] = 255; }
    ctx.putImageData(im, 0, 0);
    row.appendChild(mk('span', null, label));
    return row;
  }
  Overlay.prototype._radarClock = function (ms) {
    var iso = new Date(ms).toISOString().replace(/\.\d+Z$/, 'Z'), abbr = this.opts.tzAbbr(iso, this.opts.tz), st = ribbonFormatter(this.opts.tz).stamp(ms);
    return { day: st.weekday, clock: st.clock, zone: abbr ? ' ' + abbr : '' };
  };
  // "Thu 7:40 PM HST · 10 min ago": the shown frame's time and age.
  Overlay.prototype._radarLabel = function () {
    var p = this.radarPlayer, fr = p && p.frames[p.idx];
    if (!fr) return 'no radar frames yet';
    var ms = radarSlotMs(fr.t), st = this.radar, regs = st && st.pointer ? st.regionsIn(p.view()) : [], real = null, held = null, i;
    for (i = 0; i < regs.length; i++) { var e = fr.r[regs[i]]; if (e && (real === null || e.o > real)) real = e.o; }
    // The newest file on the map, as the hover says it: a region one step behind (Taiwan's file lands a tick later) shows its own frame.
    for (i = 0; i < regs.length; i++) {
      var rec = st.resolve(regs[i], p.idx), own = rec && rec.r[regs[i]];
      if (own && (held === null || radarSlotMs(rec.t) + own.o * 60000 > held)) held = radarSlotMs(rec.t) + own.o * 60000;
    }
    if (held !== null) ms = held;
    else if (real !== null) ms += real * 60000;              // nothing on the map yet: the frame's own files
    var c = this._radarClock(ms), mins = Math.round((Date.now() - ms) / 60000);
    return (c.day ? c.day + ' ' : '') + c.clock + c.zone + ' · ' + (mins < 1 ? 'now' : radarAge(mins) + ' ago');
  };
  // The notes under the scale, for the regions in view: a region's newest frame past RADAR_WARN_MIN; a region showing an earlier frame.
  Overlay.prototype._radarNote = function () {
    var p = this.radarPlayer, st = this.radar;
    if (!p || !st || !st.pointer) return '';
    var out = [], regs = st.regionsIn(p.view()), fr = p.frames[p.idx], now = Date.now(), loading = false;
    for (var i = 0; i < regs.length; i++) {
      var nw = st.newestOf(regs[i]), name = st.pointer.regions[regs[i]].name, nk = nw ? st.key(nw.t, regs[i], 't0') : null, bad = nk && !st.pics.has(nk) ? st.fails[nk] : null;
      if (bad) { out.push(name + (bad.final ? ': the radar pictures cannot be read.' : ': the radar pictures are not answering. Trying again.')); continue; }
      if (nw && !st.cov[regs[i]]) { loading = true; continue; }
      var real = function (r, reg) { return radarSlotMs(r.t) + ((r.r[reg] && r.r[reg].o) || 0) * 60000; };   // the file's own time, as the label and the hover say it
      var old = nw && (now - real(nw, regs[i])) / 60000 > RADAR_WARN_MIN;
      if (old) out.push(name + ': the newest frame is ' + radarAge(Math.round((now - real(nw, regs[i])) / 60000)) + ' old.');
      if (fr) {
        var rec = st.resolve(regs[i], p.idx);
        if (rec && rec !== fr) { if (radarSlotMs(fr.t) - radarSlotMs(rec.t) > RADAR_PUBLISH_MS) out.push(name + ' shows ' + this._radarClock(real(rec, regs[i])).clock + '.'); }   // one slot behind is how a late file lands, not news
        else if (!rec && !old) out.push(name + ': no picture for ' + this._radarClock(real(fr, regs[i])).clock + '.');
      }
    }
    if (loading) out.unshift('Loading the radar pictures.');
    else if (p.holdFrom) out.unshift('Loading the loop.');
    return out.join(' ');
  };
  // "32 dBZ · moderate rain · 7:44 PM"; Europe's frame is a 10-minute maximum ending at the slot.
  Overlay.prototype._radarReadout = function (r) {
    if (!r) return null;
    if (r.kind === 'none') return 'no radar coverage';
    if (r.kind === 'unknown') return 'radar pictures not in yet';
    if (r.kind === 'nopic') return 'no radar picture for this time';
    if (r.kind === 'dry' || !r.rec) return 'no echo';
    var st = this.radar, def = st.pointer && st.pointer.regions[r.region];
    if (!def) return null;                                     // the region left the pointer between the sample and this line
    var e = r.rec.r[r.region], ms = radarSlotMs(r.rec.t) + (e ? e.o : 0) * 60000, c = this._radarClock(ms).clock;
    if (def.scale === 'jma8') { var row = radarClassOf(r.code); return radarClassText(row) + ' · ' + row[3] + ' · ' + c; }
    var opera = def.src === 'opera', d = Math.round(r.dbz), word = r.type === 1 ? radarSnowWord(d) : r.type === 2 ? radarMixWord(d) : radarRainWord(d);
    return d + ' dBZ · ' + word + ' · ' + (opera ? '10-min max to ' : '') + c;
  };
  // The player's parts of the panel, in place (a new frame must not rebuild the panel: menus and focus stay).
  Overlay.prototype._radarUI = function () {
    var ui = this.ui, p = this.radarPlayer;
    if (p) this._attribute();                                // the corner names what is in view
    if (!ui || !ui.radar || !p) return;
    var playing = p.playing, label = this._radarLabel(), n = p.frames.length;
    if (ui.play) {
      ui.play.textContent = playing ? '❚❚' : '▶'; ui.play.setAttribute('aria-label', playing ? 'Pause' : 'Play');
      ui.play.disabled = n < 2;
    }
    if (ui.host && ui.host.classList) ui.host.classList.toggle('ov-playing', !!playing);
    if (ui.slider) {
      ui.slider.max = String(Math.max(0, n - 1)); ui.slider.value = String(p.idx); ui.slider.disabled = n < 2;
      ui.slider.setAttribute('aria-valuetext', label);
    }
    if (ui.speedSel) ui.speedSel.sync();
    if (ui.radarLine) ui.radarLine.textContent = label;
    if (ui.title) { var said = ui.collapsed ? 'Radar · ' + label : radarHead(this._radarSources()); if (ui.title.textContent !== said) ui.title.textContent = said; }
    if (ui.radarLoop) ui.radarLoop.value = String(p.loopH);
    if (ui.radarTypes) { var typed = this._radarTyped(); if (ui.radarTypes.hidden === typed) ui.radarTypes.hidden = !typed; }
    if (ui.radarNote) { var note = this._radarNote(); ui.radarNote.textContent = note; ui.radarNote.hidden = !note; }
  };
  // The radar's panel: head = the sources; the transport (play, slider, speed) stays on the smallest sheets; details = the frame's
  // time, the loop length, the scale with its rain words, the notes; folded: "Radar · Thu 7:40 PM HST · 10 min ago".
  Overlay.prototype._renderRadar = function (st, host, compact, focusCls, mapH) {
    var self = this, p = this.radarPlayer, unit = this.opts.getUnit();
    this._obsShell(st, host, compact, focusCls, mapH, radarHead(this._radarSources()), 'Radar · ' + this._radarLabel(), function (body) {
      var row = mk('div', 'ov-row ov-radar-line'), line = mk('span', 'ov-radar-label', self._radarLabel());
      var sel = mk('select', 'ov-radar-loop'); sel.setAttribute('aria-label', 'Loop length'); sel.setAttribute('autocomplete', 'off');
      RADAR_LOOP_HOURS.forEach(function (h) { var o = mk('option', null, h + ' h'); o.value = String(h); sel.appendChild(o); });
      sel.value = String(p ? p.loopH : RADAR_DEFAULT_LOOP);
      sel.addEventListener('change', function () { if (self.radarPlayer) self.radarPlayer.setLoop(+sel.value); });
      row.appendChild(line); row.appendChild(sel); body.appendChild(row);
      self.ui.radarLine = line; self.ui.radarLoop = sel;
      body.appendChild(buildLegend('radar', RADAR_FDEF, unit));
      var words = mk('div', 'ov-radar-words');
      RADAR_WORDS.forEach(function (w) {
        var s = mk('span', null, w[1]); s.style.left = (legendPos('radar', RADAR_FDEF.legend, w[0]) * 100).toFixed(2) + '%'; words.appendChild(s);
      });
      body.appendChild(words);
      var types = mk('div', 'ov-radar-types');
      types.appendChild(radarTypeBar('radarSnow', 'Snow'));
      types.appendChild(radarTypeBar('radarMix', 'Mix (est.)'));
      types.hidden = !self._radarTyped();
      body.appendChild(types); self.ui.radarTypes = types;
      body.appendChild(self._radarOpacityRow());
      var note = mk('div', 'ov-warn'); note.hidden = true; body.appendChild(note); self.ui.radarNote = note;
    }, function (h, ui) {
      var row = mk('div', 'ov-row ov-transport ov-radar-row');
      var play = button('ov-play', '▶', 'Play', function () { if (self.radarPlayer) self.radarPlayer.toggle(); });
      var slider = mk('input', 'ov-timeline'); slider.type = 'range'; slider.min = '0'; slider.step = '1'; slider.setAttribute('aria-label', 'Radar frame');
      slider.addEventListener('input', function () { if (self.radarPlayer) self.radarPlayer.scrub(+slider.value); });
      var speed = speedSelector(self, compact);
      row.appendChild(play); row.appendChild(slider); row.appendChild(speed.root); h.appendChild(row);
      ui.radar = true; ui.play = play; ui.slider = slider; ui.speedSel = speed;
    });
    this._radarUI();
  };
  // The transparency slider under the scale: 0 % (fully opaque) to 80 % (faint); the layer's opacity is 100 minus it.
  Overlay.prototype._radarOpacityRow = function () {
    var self = this, row = mk('label', 'ov-row ov-radar-opacity'), name = mk('span', 'ov-radar-opacity-name', 'Transparency');
    var range = mk('input', 'ov-radar-opacity-range'), out = mk('span', 'ov-radar-opacity-value');
    range.type = 'range'; range.min = '0'; range.max = String(100 - RADAR_OPACITY_MIN); range.step = '5'; range.setAttribute('autocomplete', 'off');
    function show() {
      var t = 100 - self.radarOpacity;
      range.value = String(t); out.textContent = t + '%'; range.setAttribute('aria-valuetext', t + '% transparent');
    }
    show();
    range.addEventListener('input', function () { self.setRadarOpacity(100 - (+range.value)); show(); });
    row.appendChild(name); row.appendChild(range); row.appendChild(out);
    this.ui.radarOpacity = range;
    return row;
  };
  // The radar layer's opacity, 20-100 %: kept on the device; the open layer follows at once (one CSS opacity: no redraw).
  Overlay.prototype.setRadarOpacity = function (pct) {
    var n = radarOpacityOf(pct);
    this.radarOpacity = n; writeRadarOpacity(n);
    if (this.radarLayer) this.radarLayer.setOpacity(n / 100);
  };
  // ---- the satellite readings (plan section 81) ----
  // The store the own entry shows: the swell's when chosen and offered, else the heights'.
  Overlay.prototype._satStoreFor = function () { return this.satKind === 'swell' && this.swell ? this.swell : this.sat; };
  // The SatLayer in its own pane (obsPane, z 260: above the field tiles, below the particles), drawing from `store` in `mode`
  // ('dots' / 'arrows'); one layer serves the own entry and the frame mode in turn. The store's pointer is re-read every
  // SAT_RECHECK_MS while attached.
  Overlay.prototype._attachSat = function (store, mode) {
    var self = this;
    if (!this.map.getPane('obsPane')) { var pane = this.map.createPane('obsPane'); pane.style.zIndex = 260; pane.style.pointerEvents = 'none'; }
    if (!this.satLayer) {
      this.satLayer = new SatLayer().addTo(this.map);
      this.satLayer.onRedraw = function () { self._onSatChange(); };
    }
    if (this.satLayer.store !== store || this.satLayer.mode !== mode) {
      if (this.satLayer.store && this.satLayer.store !== store) this.satLayer.store.idle();
      this.satLayer.setStore(store, mode);
    }
    if (!this.satTimer) this.satTimer = setInterval(function () { self._satRecheck(); }, SAT_RECHECK_MS);
  };
  Overlay.prototype._detachSat = function () {
    if (this.satTimer) { clearInterval(this.satTimer); this.satTimer = null; }
    if (!this.satLayer) return;
    var st = this.satLayer.store;
    this.satLayer.setWindow(null);
    this.satLayer.setStore(null);
    if (st) st.idle();
    this.map.removeLayer(this.satLayer);
    this.satLayer = null;
  };
  // A chunk landed or failed: the credit (readings on screen) and the readings line follow, in place.
  Overlay.prototype._onSatChange = function () {
    if (!this.satLayer) return;
    if (this.mode === 'obs' && this.field === 'sat') {
      var st = this.satLayer.store, w = this.satLayer.win;
      if (st && w && st.newestMs() !== null && w.ref !== st.newestMs()) this._syncSatOwnWindow();   // a pointer adopted once its chunks landed: the window follows its newest
      this._satLineSync();
    }
    if (this._attributed) this._attribute();
  };
  // The pointer re-read (every 12 min while attached): a newer one is adopted by the store once its chunks are in; the
  // own entry's window then follows the newest reading; the stale line follows the newest reading's age.
  Overlay.prototype._satRecheck = function () {
    var self = this, st = this.satLayer ? this.satLayer.store : null;
    if (typeof document !== 'undefined' && document.hidden) return;
    this._gridRecheck();                                     // the gridded map's pointer too, on the same timer (plan section 82)
    if (!st) return;
    if (st.status !== 'ok') { this._satReload(st); return; }       // a failed read: tried again (load() paces it by its cooldown)
    st.recheck().then(function (changed) {
      if (!self.satLayer || self.satLayer.store !== st) return;
      if (self.mode === 'obs' && self.field === 'sat') { if (changed) self._syncSatOwnWindow(); self._satLineSync(); }
    });
  };
  // The own entry's window: the last 24 h up to the newest reading the pointer names (nothing newer exists), its
  // reference the newest reading (alpha 1 there, SAT_FADE[1] at the far end).
  Overlay.prototype._syncSatOwnWindow = function () {
    var st = this.satLayer ? this.satLayer.store : null, newest = st ? st.newestMs() : null;
    if (!this.satLayer || newest === null) return;
    this.satLayer.setWindow({ t0: newest - SAT_WINDOW_MS, t1: newest + 1, ref: newest, span: SAT_WINDOW_MS, frame: false, entry: null });
  };
  // The Heights | Swell switch of the own entry (remembered per tab): the layer takes the other store at once.
  Overlay.prototype.setSatKind = function (kind) {
    kind = kind === 'swell' && this.swell ? 'swell' : 'heights';
    if (kind === this.satKind) return;
    this.satKind = kind; save({ satKind: kind });
    if (this.mode !== 'obs' || this.field !== 'sat' || !this.layer) return;
    var self = this, store = this._satStoreFor(), sig = this.abort ? this.abort.signal : null;
    this.render({ state: 'loading' });
    store.load(true).then(function (st) {
      if ((sig && sig.aborted) || self.field !== 'sat' || self.mode !== 'obs' || self.satKind !== kind || !self.layer) return;
      if (!st) { self._fail(sig, new Error(store.status === 'unsupported' ? 'satellite readings are not supported in this browser' : 'satellite readings cannot be loaded right now')); return; }
      self._attachSat(st, kind === 'swell' ? 'arrows' : 'dots');
      self._syncSatOwnWindow();
      self._attribute();
      self.render({ state: 'ready' });
    });
  };
  // A store whose pointer read failed (or never ran) is asked again, the cooldown permitting; a success re-syncs the own
  // window and the readings line.
  Overlay.prototype._satReload = function (st) {
    var self = this;
    if (st.status === 'ok' || st.loading) return;
    st.load(false).then(function (ok) {
      if (!ok || !self.satLayer || self.satLayer.store !== st) return;
      if (self.mode === 'obs' && self.field === 'sat') { self._syncSatOwnWindow(); self._satLineSync(); }
    });
  };
  // ---- the gridded map (plan section 82) ----
  // Wanted: the box on, on the Satellite waves entry. The map is drawn in the entry's ModelGridLayer (modelPane, under the
  // dots' obsPane) as field 'sat' (the coast clip; no lake mask, no contours), the newest step only.
  Overlay.prototype._wantGrid = function () { return !!(this.grid && this.satGrid && this.mode === 'obs' && this.field === 'sat' && this.layer && this.satLayer); };   // satLayer: never under the error panel (G54-1)
  Overlay.prototype._gridShown = function () { return !!(this.layer && this.layer.field === 'sat' && this.layer.hasFrame()); };
  Overlay.prototype._gridState = function () {                // '' fresh / 'warn' past GRID_WARN_H / 'hide' past GRID_HIDE_H (owner)
    var st = this.grid, h = st && this.satGrid ? st.ageH() : null;
    return h === null ? '' : h > GRID_HIDE_H ? 'hide' : h > GRID_WARN_H ? 'warn' : '';
  };
  Overlay.prototype.setSatGrid = function (on) {
    on = !!(on && this.grid);
    if (on === this.satGrid) return;
    this.satGrid = on; save({ satGrid: on });
    if (this.ui && this.ui.gridBox && this.ui.gridBox.checked !== on) this.ui.gridBox.checked = on;
    this._syncGrid(true, true);                              // quiet: the render below is the one (G54-5)
    if (this.last && this.last.state === 'ready' && this.mode === 'obs' && this.field === 'sat') this.render(this.last);   // the credit line, the warning
  };
  Overlay.prototype._syncGrid = function (force, quiet) {
    var self = this, st = this.grid;
    if (!this._wantGrid()) { this._clearGrid(); this._gridLineSync(quiet); return; }
    if (st.frame) this._showGrid(st);                        // the frame in hand at once, even while a re-read runs (it stays valid until a newer one lands)
    this._gridLineSync(quiet);
    st.load(force).then(function (ok) {
      if (ok && self._wantGrid()) self._showGrid(ok);
      self._gridLineSync();
    });
  };
  // Only the gridded map is ever cleared here (never a model field's picture: the layer's field says whose it is).
  Overlay.prototype._clearGrid = function () {
    if (!this._gridShown()) return;
    this.layer.clear();
    if (this.readout) this.readout.hidden = true;            // never "grid …" over a map that is gone (G54-4)
    if (this._attributed) this._attribute();
  };
  Overlay.prototype._showGrid = function (st) {
    var self = this;
    if (!this._wantGrid() || !st.frame) return;
    if (this._gridState() === 'hide') { this._clearGrid(); return; }
    if (this._gridShown() && this.layer.entry === st.entry) return;
    var entry = st.entry, coastP = this.coast ? this.coast.load() : Promise.resolve(null);
    this._coastFirst(coastP).then(function (coast) {                 // the clip waits for the coastline at most COAST_FIRST_MS (as SST)
      if (!self._wantGrid() || self.grid.entry !== entry || self._gridState() === 'hide') return;
      if (coast !== undefined && self.layer._coast !== (coast || null)) self.layer.setCoast(coast);
      if (self.layer._src) self.layer.setSource(null, true);        // never SST's cells under this label
      self.layer.setContours(null, true);
      self.layer.setFrame(st.frame, st.grid, 'sat', SAT_FDEF, satLut(), entry);
      self._syncLook();                                              // the field opacity (FIXED_OPACITY's default)
      self._gridLineSync();
      if (self._attributed) self._attribute();
    });
  };
  // Every SAT_RECHECK_MS with the box on: a failed store is asked again (its cooldown paces it); a newer manifest is
  // taken in place (the step on the map stays until the new one is decoded).
  Overlay.prototype._gridRecheck = function () {
    var self = this, st = this.grid;
    if (!this._wantGrid()) return;
    var adopt = function (ok) { if (ok && self._wantGrid()) self._showGrid(ok); self._gridLineSync(); };
    if (st.status !== 'ok') { st.load(false).then(adopt); return; }
    st.recheck().then(function (changed) { if (changed) st.load(false).then(adopt); else self._gridLineSync(); });
  };
  // The note beside the box, in place: "CMEMS L4 · Nov 3, 1:00 PM EST (31 h ago)" / "loading…" / "unavailable" /
  // "hidden: 75 h old"; a change of state (fresh / warn / hide) re-renders the panel (the warning line) and a map past
  // GRID_HIDE_H comes off.
  Overlay.prototype._gridLineText = function () {
    var st = this.grid;
    if (!st || !this.satGrid) return '';
    if (st.entry) {
      var h = st.ageH(), ago = Math.max(0, Math.round(h)), when = st.entry.valid_utc;
      if (h > GRID_HIDE_H) return 'hidden: ' + ago + ' h old';
      return 'CMEMS L4 · ' + this.opts.fmtTime(when, this.opts.tz) + ' ' + this.opts.tzAbbr(when, this.opts.tz) + ' (' + (ago < 1 ? 'under an hour' : ago + ' h') + ' ago)';
    }
    return st.status === 'failed' ? 'unavailable' : 'loading…';
  };
  Overlay.prototype._gridLineSync = function (quiet) {
    var ui = this.ui;
    if (ui && ui.gridNote) { var t = this._gridLineText(); if (ui.gridNote.textContent !== t) ui.gridNote.textContent = t; }
    if (ui && ui.gridWarn && this._gridWarnShown) { var w = this._gridWarnText(); if (ui.gridWarn.textContent !== w) ui.gridWarn.textContent = w; }
    var state = this._gridState();
    if (state === 'hide') this._clearGrid();
    if (!quiet && this.mode === 'obs' && this.field === 'sat' && state !== (this._gridWarnShown || '') && this.last && this.last.state === 'ready') this.render(this.last);
  };
  // The panel's warning line: "The gridded map is 52 h old." / "The gridded map is 75 h old: hidden."
  Overlay.prototype._gridWarnText = function () {
    return 'The gridded map is ' + Math.round(this.grid.ageH()) + ' h old' + (this._gridWarnShown === 'hide' ? ': hidden.' : '.');
  };
  Overlay.prototype._gridValueText = function (v) {
    var u = unitOf('hs', this.opts.getUnit());
    return (v >= SAT_FDEF.hi - 1e-9 ? '≥ ' : '') + u.f(v).toFixed(u.d) + ' ' + u.label;
  };
  // The hover over water with no reading under the cursor: the gridded map's value and its step's time.
  Overlay.prototype._gridReadout = function (v) {
    var when = (this._gridShown() && this.layer.entry ? this.layer.entry : this.grid.entry).valid_utc;   // the drawn step's time
    return 'grid ' + this._gridValueText(v) + ' · ' + this.opts.fmtTime(when, this.opts.tz) + ' ' + this.opts.tzAbbr(when, this.opts.tz);
  };
  // The own entry's readings line, in place: "Readings: last 24 h, newest 11:40 AM HST (2 h ago) · 7 satellites" ("loading
  // 3 of 8…" while chunks are on their way); the stale warning past the job's own limit re-renders the panel.
  Overlay.prototype._satLineText = function () {
    var st = this.satLayer ? this.satLayer.store : this._satStoreFor(), newest = st ? st.newestMs() : null;
    if (!st || newest === null) return 'Readings: none';
    var now = Date.now(), ago = Math.max(0, Math.round((now - newest) / 3.6e6)), win = this.satLayer && this.satLayer.win;
    var pend = win ? st.pending(win.t0, win.t1) : 0, needed = win ? st._chunksFor(st.pointer, { t0: win.t0, t1: win.t1 }) : [], need = needed.length;
    var retrying = 0, missing = 0, k;
    for (k = 0; k < need; k++) { var fk = st.failed[needed[k].key]; if (fk === true) missing++; else if (fk && !st.chunks.has(needed[k].key)) retrying++; }
    // the spacecraft, not the mission ids: Jason-3 and its OGDR feed are one satellite (G52 B-3)
    var sats = {}, f = st.flat, sl = win ? st.slice(win.t0, win.t1) : [0, 0], nsat = 0;
    if (f) for (var i = sl[0]; i < sl[1]; i++) { var m = f.missions[f.mission[i]], name = (m && m.short ? m.short : String(f.mission[i])).replace(/ OGDR$/, ''); if (!sats[name]) { sats[name] = true; nsat++; } }
    var when = new Date(newest).toISOString().replace(/\.\d+Z$/, 'Z'), clock = this.opts.fmtTime(when, this.opts.tz) + ' ' + this.opts.tzAbbr(when, this.opts.tz);
    var agoText = '(' + (ago < 1 ? 'under an hour' : ago + ' h') + ' ago)';
    // the swell's daily batch runs up to 31 h behind the clock: its line names the window's end (owner, section 82)
    var txt = st.kind === 'swell' ? 'Readings: 24 h to ' + clock + ' ' + agoText : 'Readings: last 24 h, newest ' + clock + ' ' + agoText;
    if (pend) txt += ' · loading ' + (need - pend) + ' of ' + need + '…' + (retrying ? ' (' + retrying + ' retrying)' : '');   // G52 B-6
    else if (nsat) txt += ' · ' + nsat + ' satellite' + (nsat === 1 ? '' : 's');
    if (!pend && missing) txt += ' · ' + (need - missing) + ' of ' + need + ' windows';                                          // G52 B-7: a chunk the bucket lacks
    return txt;
  };
  Overlay.prototype._satLineSync = function () {
    var ui = this.ui;
    if (!ui || !ui.satLine) return;
    var txt = this._satLineText();
    if (ui.satLine.textContent !== txt) ui.satLine.textContent = txt;
    var st = this.satLayer ? this.satLayer.store : null, stale = this._satStale(st);
    if (stale !== !!this._satStaleShown && this.last && this.last.state === 'ready') this.render(this.last);
  };
  Overlay.prototype._satStale = function (st) {
    var newest = st ? st.newestMs() : null;
    return newest !== null && (Date.now() - newest) / 3.6e6 > (st.kind === 'swell' ? SWELL_STALE_H : SAT_STALE_H);
  };
  // The hover on a reading (flat index j): a height dot gives the reading, its satellite, its time and the collocated model
  // value with the difference (frame mode: the model value from the drawn field under the cursor); a swell point every
  // partition. Units follow the site unit.
  // gv: the gridded map's value under the cursor (plan section 82), appended while the map is drawn.
  Overlay.prototype._satReadout = function (j, latlng, z, gv) {
    var txt = this._satReadoutCore(j, latlng, z);
    if (gv === null || gv === undefined || !this._wantGrid() || !this._gridShown()) return txt;
    return txt + ' · grid ' + this._gridValueText(gv);
  };
  Overlay.prototype._satReadoutCore = function (j, latlng, z) {
    var st = this.satLayer.store, f = st.flat, u = unitOf('hs', this.opts.getUnit()), i;
    var when = new Date(f.t[j]).toISOString().replace(/\.\d+Z$/, 'Z'), clock = this.opts.fmtTime(when, this.opts.tz) + ' ' + this.opts.tzAbbr(when, this.opts.tz);
    var sat = f.missions[f.mission[j]] ? f.missions[f.mission[j]].short : '?';
    var fmt = function (v) { return u.f(v).toFixed(u.d) + ' ' + u.label; };
    // the job's flags (G52 B-1, B-2): near land = a reading within a grid cell of land (drawn hollow); model edge = the
    // model sampled beside an absent node (the ice edge, a coast): its value is shown, the comparison is not
    var near = (f.flags[j] & SAT_FLAG_NEAR_LAND) !== 0, edge = (f.flags[j] & SAT_FLAG_MODEL_EDGE) !== 0;
    if (st.kind === 'swell') {
      var parts = [], ago = Math.max(0, Math.round((Date.now() - f.t[j]) / 3.6e6));
      for (i = 0; i < 3; i++) {
        var h = f.hs[i][j], tp = f.tp[i][j], d = f.dir[i][j];
        if (!(h === h)) continue;
        parts.push('Swell ' + (i + 1) + ': ' + fmt(h) + ' · ' + tp.toFixed(0) + ' s · from ' + Math.round(d) + '°');
      }
      return parts.join(' | ') + ' (' + sat + (near ? ', near land' : '') + ', ' + (ago < 1 ? 'under an hour' : ago + ' h') + ' ago)';
    }
    var txt = fmt(f.swh[j]) + ' · ' + sat + ' · ' + clock + (near ? ' · near land' : ''), mv = null, label = 'model';
    if (f.model[j] === f.model[j]) mv = f.model[j];
    if (mv === null) return txt + ' · ' + label + ' —';
    if (edge) return txt + ' · ' + label + ' ' + fmt(mv) + ' (edge)';
    var diff = +u.f(f.swh[j]).toFixed(u.d) - +u.f(mv).toFixed(u.d);   // of the two numbers printed (G52 B-5)
    return txt + ' · ' + label + ' ' + fmt(mv) + ' (' + (diff >= 0 ? '+' : '') + diff.toFixed(u.d) + ')';
  };
  // The error panel's Retry. A run pinned by a carried time that cannot be shown (e.g. pruned while SST was up) is let go:
  // with nothing on the map and a newer run known, the mount takes the newer run at the carried time (re-check RC-2).
  Overlay.prototype._retry = function () {
    if (!this.field) return;
    this.unavailable = {}; this.transientFails = 0;
    if (this._carry && this.newerRun && this.frameIndex === null) this._carry.free = true;
    this.mount(this.field);
  };
  Overlay.prototype._sstLut = function () {
    var key = 'sst|' + SST_FDEF.legend.join(',');
    if (this._lutFor !== key) { this._lutCache = buildLut('sst', SST_FDEF.legend); this._lutFor = key; }
    return this._lutCache;
  };
  // Cache / in-flight / unavailable keys carry the model and the run: a decode that outlives an Update or a model
  // switch can never be taken for a frame of the new run (both models name their runs YYYYMMDDHH).
  // kind 'dir': the direction frame (pdir / wdir at its own resolution) of the same step; 'pres': its sea-level pressure
  // frame (half resolution only); else the field frame.
  // kinds 'lake' / 'lakedir' (plan section 50): the lake run's field / direction frame of the SAME VALID TIME, keyed by
  // the lake run (never a model key; 'glwu/-' names no frame: a step without a lake frame).
  Overlay.prototype._key = function (idx, kind) {
    if (kind === 'lake' || kind === 'lakedir') {
      var li = this._lakeIndex(idx), lm = this.lake ? this.lake.manifest : null;
      return li < 0 || !lm ? 'glwu/-' : 'glwu/' + lm.run + '/' + kind + '/' + (kind === 'lake' ? this.field : LAKE_DIR_FIELD) + '/' + lm.frames[li].step;
    }
    var res = kind === 'dir' ? this.dres : kind === 'pres' ? 'half' : this.res, name = kind === 'dir' ? DIR_FIELDS[this.field] : kind === 'pres' ? PRES_FIELD : this.field;
    return this.model + '/' + this.manifest.run + '/' + res + '/' + name + '/' + this.manifest.frames[idx].step;
  };
  Overlay.prototype._cacheOf = function (kind) { return kind === 'dir' ? this.dcache : kind === 'pres' ? this.pcache : kind === 'lake' ? this.lcache : kind === 'lakedir' ? this.ldcache : this.cache; };
  // The frames the target itself needs (its own fetches, ranks 0..own-1 of the plan: never evicted by a ring fetch);
  // idx: the target (the plan's anchor by default), whose lake frames count only when it has them.
  Overlay.prototype._own = function (idx) {
    var a = idx === undefined ? this._anchorIndex() : idx, lk = a !== null && this._lakeIndex(a) >= 0;
    return 1 + (this._wantDir() ? 1 : 0) + (this._wantPres() ? 1 : 0) + (lk && this._wantLake() ? 1 : 0) + (lk && this._wantLakeDir() ? 1 : 0);
  };
  // ---- the Great Lakes (plan section 50) ----
  // The lake frame index of the model frame idx (the same valid time), -1 without one (no lake run, past its horizon).
  Overlay.prototype._lakeIndex = function (idx) {
    var lk = this.lake, fr = lk && lk.status === 'ok' && this.manifest ? this.manifest.frames[idx] : null;
    return fr ? lk.indexOf(Date.parse(fr.valid_utc)) : -1;
  };
  // The lake frames ride under the model that carries them (GFS), under wave height and period, when the lake run's
  // field shares the model's code space (one lo / hi: the tiles write lake codes beside model codes), and only while
  // the lakes are in or near the view (step 6b; _lakeNearNow is cached until the view changes).
  // Not while the lakes coast set has failed, nor while it loads again after a failure: its frames could not be drawn
  // (G33 B-1, re-check P3-6; the set's arrival starts the shown step's lake frames, _loadLakes). Its first load never
  // holds them (they load side by side).
  Overlay.prototype._wantLake = function () {
    return !!(this.lake && this.lake.status === 'ok' && this.mode === 'model' && this._def().lakes && this.layer && this.field && LAKE_UNDER[this.field] &&
      !(this.lakeCoast && (this.lakeCoast.status === 'failed' || (this.lakeCoast.status === 'loading' && this.lakeCoast.failedAt > 0))) &&
      this.manifest && lakeFieldMatches(this.lake.manifest.fields[this.field], this.manifest.fields[this.field]) && this._lakeNearNow());
  };
  Overlay.prototype._lakeNearNow = function () {
    if (this._lakeNear === null) {
      var g = this.lake ? (this.lake.grid || (this.lake.manifest && this.lake.manifest.grid)) : null;
      this._lakeNear = !!(g && this.map && lakeNearView(viewOf(this.map, this.layer), g, LAKE_NEAR_SCREENS));
      if (this._lakeGateNear === null) this._lakeGateNear = this._lakeNear;   // what the plan assumed first: the gate's starting point
    }
    return this._lakeNear;
  };
  // The view moved (moveend / zoomend / viewreset / a resize): when the lakes come within reach the shown step's lake
  // frames are asked for at once and the ring follows; when they leave it, every lake fetch is dropped and the lake
  // caches emptied (nothing of the lakes is on screen), the drawn lake frame and the animator's lake data with them.
  // The gate compares the reach with the one it last ACTED on (_lakeGateNear), never with the geometry cache, which a
  // _goto in the same event (_checkRes, an adoption) may have refreshed already (G33 F1b); and it acts whatever
  // frameIndex is (an Update's pending first landing, the error panel: G33 F1). The credit follows the view too (B-5).
  Overlay.prototype._syncLakeGate = function () {
    if (!this.lake) return;
    var was = this._lakeGateNear;
    this._lakeNear = null;
    var now = this._lakeNearNow();
    this._lakeGateNear = now;
    if (this._attributed && this.layer && this.layer.hasFrame()) this._attribute();
    if (!now) {                                            // out of reach: the fetches, the caches, the drawn lake and the animator's lake go, under
      var held = this._lakeHeld();                         // any field or mode and whatever the gate saw before (G33 re-check P3-2 / P3-3)
      if (held) this._dropLakes();
      if ((held || was !== false) && this.manifest && this.mode === 'model') this._prefetch();   // the cap shrinks with the lake share: the ring is trimmed to it
      return;
    }
    if (was !== false || !this.layer || !this.manifest || this.mode !== 'model') return;
    if (this.frameIndex !== null) this._syncLake(this.frameIndex);     // back in reach: the shown step's lake frames start
    this._prefetch();                                      // the ring follows (a pending target's lake frames with it)
  };
  // Anything of the lakes held on the client: a lake fetch, either lake cache, the drawn lake, the animator's lake data,
  // the hover's height frame.
  Overlay.prototype._lakeHeld = function () {
    for (var k in this.inflight) { var kd = this.inflight[k].kind; if (kd === 'lake' || kd === 'lakedir') return true; }
    return !!(this.lcache.size() || this.ldcache.size() || (this.layer && this.layer._lake) || (this.flow && this.flow.ldir) || this._lakeHs);
  };
  // Everything of the lakes on the client goes (the run and the coverage stay): every lake fetch in flight (a step
  // waiting for its lake frames lands at once: its wait settles on the aborted fetch), both lake caches, the lake frame
  // drawn, the animator's lake data and the hover's height frame.
  Overlay.prototype._dropLakes = function () {
    for (var k in this.inflight) { var kd = this.inflight[k].kind; if (kd === 'lake' || kd === 'lakedir') { this.inflight[k].abort.abort(); delete this.inflight[k]; } }
    this.lcache.clear(); this.ldcache.clear();
    if (this.layer && this.layer._lake) { this.layer.setLake(null); this._attribute(); }
    if (this.flow) this.flow.clearLakeData();
    this._dropLakeHs();
  };
  Overlay.prototype._dropLakeHs = function () {
    var h = this._lakeHs;
    if (h) { if (h.abort) h.abort.abort(); if (h.timer) clearTimeout(h.timer); }
    this._lakeHs = null;
  };
  // A lake run or a lakes coast set that failed is tried again once its cooldown has passed (G33 B-1: a failure at page
  // load used to leave the lakes blank for the visit); called at every landing, cheap otherwise.
  Overlay.prototype._retryLakes = function () {
    if (!this.lake || !this.manifest || this.mode !== 'model' || !this._def().lakes) return;
    var now = Date.now(), lk = this.lake, lc = this.lakeCoast;
    if ((lk.status === 'failed' && now - lk.failedAt >= lk.retryMs) || (lc && lc.status === 'failed' && now - lc.failedAt >= lc.retryMs)) this._loadLakes(this.manifest);
  };
  Overlay.prototype._wantLakeDir = function () { return this._wantLake() && this._wantDir(); };
  // A newer lake run (noted by the run check): the old run's lake fetches in flight go (ring fetches, or a target's own,
  // which the step asks for again from the new run), its caches too, and the new run serves from the next step on. The
  // lake frame drawn stays until that step lands (never a blank in between; never two runs under one step).
  Overlay.prototype._adoptLake = function () {
    if (!this.lake || !this.lake.next || !this.lake.adoptNewer()) return false;   // only a run newer than the one in hand (G33 re-check P3-1)
    for (var k in this.inflight) { var kd = this.inflight[k].kind; if (kd === 'lake' || kd === 'lakedir') { this.inflight[k].abort.abort(); delete this.inflight[k]; } }
    this._lakeNear = null;                                 // the grid may have changed with the run (the gate keeps what it acted on)
    this.lcache.clear(); this.ldcache.clear(); this._dropLakeHs();
    return true;
  };
  // The lake run and the lakes coast set load beside the model's run (never holding the first draw): a lake run that
  // arrives after the first frame is on the map is fetched for it at once; the coast set is given to the layer.
  Overlay.prototype._loadLakes = function (m) {
    var self = this;
    if (this.lakeCoast) this.lakeCoast.load().then(function (store) {
      if (!self.layer) return;
      if (!store) {                                          // nothing of the lakes can be drawn (B-1): their frames and their share of the cap go
        if (self.lakeCoast.status === 'failed') { self._dropLakes(); if (self.manifest) self._prefetch(); }
        return;
      }
      if (self.layer._lakeCoast === store) return;
      self.layer.setLakeCoast(store);
      self._attribute();                                     // the lakes' credit appears with their coverage
      if (self.manifest === m && self.frameIndex !== null && self.mode === 'model') { self._syncLake(self.frameIndex); self._prefetch(); }
    }, function () {});
    if (!this.lake || !this._def().lakes) return;
    this.lake.load().then(function (st) {
      self._lakeNear = null; self._lakeGateNear = null;   // the reach is judged afresh with the run's grid, at once (never left unset: an early
      if (st && self.layer && self.map) self._lakeNearNow();   // return below would leave the next move out of reach unacted on, G33 re-check P3-2)
      if (!st || self.manifest !== m || !self.layer || self.frameIndex === null || self.mode !== 'model') return;
      if (st.next) self._adoptLake();                      // a newer run found by the re-read: taken here, through the adoption path
      self._syncLake(self.frameIndex);
      self._prefetch();
    }, function () {});
  };
  // The lake frame drawn for the step idx, if decoded (for setFrame: one redraw with the step's own frame).
  Overlay.prototype._lakeHit = function (idx) {
    if (!this._wantLake() || this._lakeIndex(idx) < 0) return null;
    var f = this.lcache.get(this._key(idx, 'lake')), m = this.manifest, lm = this.lake.manifest;
    return f ? { frame: f, grid: lm.grid, entry: m.frames[idx], lo: m.fields[this.field].lo, hi: m.fields[this.field].hi, run: lm.run } : null;
  };
  // The frame idx is on the map: the layer draws its lake frame if decoded (the shore's tiles only, when it changed) and
  // the animator takes its lake direction; another step's lake frame is cleared at once (never an old lake under a new
  // time); the fetches start only when no other target is pending (that target's own frames come first).
  Overlay.prototype._syncLake = function (idx) {
    if (!this.layer) return;
    var m = this.manifest, entry = m.frames[idx], want = this._wantLake() && this._lakeIndex(idx) >= 0;
    if (!want || !this.layer.hasFrame() || this.layer.entry !== entry) {
      if (this.layer._lake) { this.layer.setLake(null); this._attribute(); }
      if (this.flow) this.flow.clearLakeData();
      return;
    }
    var hit = this.lcache.get(this._key(idx, 'lake'));
    if (hit) this._lakeData(hit, entry);
    else {
      if (this.layer._lake) { this.layer.setLake(null); this._attribute(); }
      if (this.flow) this.flow.clearLakeData();
      if (this.target === null || this.target === idx) this._startKind(idx, 'lake');
    }
    if (!this._wantLakeDir()) { if (this.flow) this.flow.clearLakeData(); return; }
    var dh = this.ldcache.get(this._key(idx, 'lakedir'));
    if (dh) this._flowLakeData(dh, entry);
    else {
      if (this.flow && this.flow.lentry !== entry) this.flow.clearLakeData();
      if (this.target === null || this.target === idx) this._startKind(idx, 'lakedir');
    }
  };
  // The layer takes a lake frame for the step on the map (its own entry), and the animator the step's lake direction
  // when it is decoded already; an exception (a refused canvas context) is logged once and never reaches the playback.
  Overlay.prototype._lakeData = function (frame, entry) {
    var m = this.manifest, lm = this.lake.manifest, f = m.fields[this.field];
    try {
      if (this.layer.setLake({ frame: frame, grid: lm.grid, entry: entry, lo: f.lo, hi: f.hi, run: lm.run })) this._attribute();
    } catch (e) { if (!this._lakeWarned && typeof console !== 'undefined' && console.warn) { this._lakeWarned = true; console.warn('overlay lakes', e); } return; }
    if (!this._wantLakeDir()) return;
    var dh = this.ldcache.get(this._key(this.frameIndex !== null && m.frames[this.frameIndex] === entry ? this.frameIndex : this.n, 'lakedir'));
    if (dh) this._flowLakeData(dh, entry);
  };
  // The direction frames delivered here are always the store's current run (its cache keys, _ensureLakeDir's guard): the
  // animator takes one only beside the lake height frame of that run (after an adoption the old run's frame stays drawn
  // until the step lands; its new direction waits for the new run's height frame: never two runs in one rebuild, G33 F2).
  Overlay.prototype._flowLakeData = function (dir, entry) {
    var ll = this.layer ? this.layer._lake : null, lm = this.lake ? this.lake.manifest : null;
    if (!this.flow || !ll || ll.entry !== entry || !lm || ll.run !== lm.run) return;
    try { this.flow.setLakeData(dir, ll.frame, ll.grid, entry); }
    catch (e) { if (!this._flowWarned && typeof console !== 'undefined' && console.warn) { this._flowWarned = true; console.warn('overlay animation', e); } }
  };
  // The lake field / direction frame for idx, delivered only if the LAYER still shows that step of that run at this field
  // and the lake run is the one it was asked for; a failure is swallowed: the lakes stay blank for the step.
  Overlay.prototype._ensureLake = function (idx) {
    var self = this, m = this.manifest, lm = this.lake ? this.lake.manifest : null, field = this.field;
    return this._ensure(idx, 'lake').then(function (frame) {
      if (self.layer && self.layer.hasFrame() && self.layer.entry === m.frames[idx] && self.manifest === m && self.lake && self.lake.manifest === lm && self.field === field && self._wantLake()) {
        self._lakeData(frame, m.frames[idx]);
      }
    }).catch(function (err) {
      if (err && !err.unavailable && err.name !== 'AbortError' && typeof console !== 'undefined' && console.warn) console.warn('overlay lakes', err);
    });
  };
  Overlay.prototype._ensureLakeDir = function (idx) {
    var self = this, m = this.manifest, lm = this.lake ? this.lake.manifest : null, field = this.field;
    return this._ensure(idx, 'lakedir').then(function (frame) {
      if (self.layer && self.layer.hasFrame() && self.layer.entry === m.frames[idx] && self.manifest === m && self.lake && self.lake.manifest === lm && self.field === field && self._wantLakeDir()) {
        self._flowLakeData(frame, m.frames[idx]);
      }
    }).catch(function (err) {
      if (err && !err.unavailable && err.name !== 'AbortError' && typeof console !== 'undefined' && console.warn) console.warn('overlay lakes', err);
    });
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
    kind = kind === 'dir' || kind === 'pres' || kind === 'lake' || kind === 'lakedir' ? kind : 'field';
    var dir = kind === 'dir', pres = kind === 'pres', lake = kind === 'lake' || kind === 'lakedir', cache = this._cacheOf(kind);
    var self = this, key = this._key(idx, kind), hit = cache.get(key);
    if (hit) return Promise.resolve(hit);
    if (this.inflight[key]) return this.inflight[key].promise;
    if (this._isUnavailable(idx, kind)) { var e = new Error('frame unavailable'); e.unavailable = true; return Promise.reject(e); }
    delete this.unavailable[key];                                            // an expired cooldown: try again
    var m = this.manifest, lm = lake && this.lake ? this.lake.manifest : null, ctrl = new AbortController(), rec, half = pres || (dir ? this.dres : this.res) === 'half', kindName = kind, url, grid;
    try {
      if (lake) {                                                            // the lake run's frame of the same valid time, on the lake grid
        var li = this._lakeIndex(idx);
        if (li < 0 || !lm) throw new Error('no lake frame for this time');
        url = this.root + '/' + frameKey(lm, lm.frames[li], kind === 'lake' ? this.field : LAKE_DIR_FIELD, false); grid = lm.grid;
      } else { url = this.root + '/' + frameKey(m, m.frames[idx], dir ? DIR_FIELDS[this.field] : pres ? PRES_FIELD : this.field, half); grid = half ? m.grid_half : m.grid; }
    } catch (e) { return Promise.reject(e); }                                // never a throw: _prefetch runs from UI handlers (setSpeed, pause)
    function mine() { return self.inflight[key] === rec; }
    // The watchdog times the DOWNLOAD only (until the body has arrived): a decode that waits behind a busy page
    // (fast playback on a large screen) is late, never missing. A download that stalls (one took 6.5 s at G4) is
    // asked again at once, LATE_TRIES times in all; only then does it count as a failure ("frame stalled").
    // It counts only the time with no byte arriving (every chunk re-arms it): on a slow link a download that is still
    // moving goes on, whatever its kind and whether a picture is on the map (the first picture beside the coastline,
    // re-check R-1; every frame, the owner's call after fix round 2's verification, V2-1: the 5 s in all had thrown away
    // direction and full-resolution frames still arriving at 30-45 KB/s).
    function attempt(n) {
      var inner = new AbortController(), stalled = false, onOuter = function () { inner.abort(); }, limit = Math.max(STALL_MS, 4 * self._interval());
      ctrl.signal.addEventListener('abort', onOuter);
      var stall = function () { stalled = true; inner.abort(); }, watchdog = setTimeout(stall, limit);
      function done() { clearTimeout(watchdog); ctrl.signal.removeEventListener('abort', onOuter); }
      var progress = function () { clearTimeout(watchdog); watchdog = setTimeout(stall, limit); };
      return decodeFrame(url, inner.signal, grid, function () { clearTimeout(watchdog); }, false, progress).then(function (frame) {
        done();
        if (lake) { try { validateLakeGrid(grid, frame); } catch (e) { throw new Error('frame decode failed: ' + e.message); } }   // (a 'decode' failure: permanent)
        return frame;
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
      if (ctrl.signal.aborted || self.manifest !== m || (lake && (!self.lake || self.lake.manifest !== lm))) throw abortError();
      if (!dir && !pres && !lake) self.transientFails = 0;                     // the outage counter follows the field frames only
      var drawn = dir ? (self.flow ? self.flow.dir : null) : pres ? (self.iso ? self.iso.frame : null) : kind === 'lake' ? (self.layer && self.layer._lake ? self.layer._lake.frame : null) :
        kind === 'lakedir' ? (self.flow ? self.flow.ldir : null) : (self.layer ? self.layer._frame : null);
      cache.set(key, frame, self.frameIndex !== null ? self._key(self.frameIndex, kind) : null, self._rankMap(), drawn);
      self._prefetch();                                                        // the ring refills on every landing, not only the target's (plan section 42)
      return frame;
    }, function (err) {
      if (mine()) delete self.inflight[key];
      if (ctrl.signal.aborted || self.manifest !== m || (lake && (!self.lake || self.lake.manifest !== lm))) throw abortError();
      var kind_ = failureKind(err);
      self.unavailable[key] = kind_ === 'permanent' ? true : Date.now() + RETRY_AFTER_MS;
      err.unavailable = true;
      if (kind_ === 'transient' && !dir && !pres && !lake && ++self.transientFails >= MAX_TRANSIENT) err.outage = true;   // the bucket, not one frame
      throw err;
    });
    rec = this.inflight[key] = { promise: p, abort: ctrl, key: key, kind: kindName };
    return p;
  };
  // What the ring wants, in priority order: the target's field frame, its direction frame (Animation
  // on), then the ring ahead and behind, field and direction interleaved.
  // The ring budget for the moment: by speed while playing, the paused one otherwise.
  Overlay.prototype._ring = function () { return ringFor(this.speed, this.playing); };
  // One more download at once while the isobars ride along (plan section 46): with three own frames a step and the paused
  // cap of two, the target's pressure could start only after a landing (a second round trip per step at 1x, or a blink).
  // ... and one more per lake frame kind while the lakes ride along (plan section 50), for the same reason.
  Overlay.prototype._maxInflight = function () { return this._ring().inflight + (this._wantPres() ? 1 : 0) + (this._wantLake() ? 1 : 0) + (this._wantLakeDir() ? 1 : 0); };
  Overlay.prototype._plan = function (idx) {
    var r = this._ring(), plan = ringPlan(idx, this.n, this.dir, r.ahead, r.behind), want = this._wantDir(), wantP = this._wantPres(), wantL = this._wantLake(), wantLD = this._wantLakeDir(), out = [];
    for (var i = 0; i < plan.length; i++) {
      out.push([plan[i], 'field']); if (want) out.push([plan[i], 'dir']); if (wantP) out.push([plan[i], 'pres']);
      if (wantL && this._lakeIndex(plan[i]) >= 0) { out.push([plan[i], 'lake']); if (wantLD) out.push([plan[i], 'lakedir']); }
    }
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
    var target = this._key(idx), dtarget = this._wantDir() ? this._key(idx, 'dir') : null, ptarget = this._wantPres() ? this._key(idx, 'pres') : null, survivors = [];
    var lk = this._lakeIndex(idx) >= 0, ltarget = lk && this._wantLake() ? this._key(idx, 'lake') : null, ldtarget = lk && this._wantLakeDir() ? this._key(idx, 'lakedir') : null;
    var room = this._maxInflight() - (this.cache.has(target) ? 0 : 1);
    for (k in this.inflight) if (rank[k] === undefined) { this.inflight[k].abort.abort(); delete this.inflight[k]; } else if (k === dtarget || k === ptarget || k === ltarget || k === ldtarget) room--; else if (k !== target) survivors.push(k);
    survivors.sort(function (a, b) { return rank[a] - rank[b]; });
    while (survivors.length > Math.max(0, room)) { k = survivors.pop(); this.inflight[k].abort.abort(); delete this.inflight[k]; }
  };
  // Show frame idx: the label/timeline move to the target at once, the picture and the valid time only
  // when the frame has landed (never an old picture under a new time). Rejects for an unavailable frame
  // (err.unavailable) or an aborted load (AbortError).
  Overlay.prototype._goto = function (idx, sig) {
    var self = this, m = this.manifest, field = this.field, res = this.res;
    this.target = idx; this._syncUI();
    this._adoptLake();                                     // a newer lake run takes over from this step on (plan section 50)
    this._trimInflight(idx);
    var p = this._ensure(idx);
    // With Animation on, the picture and its particles change together: the step waits for its direction
    // frame as well (fetched beside the field frame); a missing or failed direction never holds it.
    // The FIRST picture of a field (after a mount or a field switch) never waits for it: the particles join when
    // their frame lands (G15 P2-1: with the animation always on, waiting made every first wind frame ~2x slower).
    var pd = this._wantDir() && this.layer && this.layer.hasFrame() && !this._isUnavailable(idx) ? this._kindReady(idx, 'dir') : null;
    // The isobars likewise: a step waits for its pressure frame (the lines change with the picture, never a blink or an old
    // set under a new time); the first picture does not; a missing or failed pressure frame never holds a step.
    // A slow pressure frame holds the step at most PRES_WAIT_MS (owner, G32): the step then lands without lines and they
    // appear when the frame arrives (_ensurePres delivers it to the step on the map).
    // The wait is counted per step: a goto of the same step issued again (a zoom across the half / full threshold, a
    // re-issued tick) waits only the rest of it (G32 re-check S-1); the record goes when the step lands or the target changes.
    var pp = null;
    if (this._wantPres() && this.layer && this.layer.hasFrame() && !this._isUnavailable(idx)) {
      var pkey = this._key(idx, 'pres'), now = Date.now();
      if (!this._presWait || this._presWait.key !== pkey) this._presWait = { key: pkey, since: now, timer: null };
      pp = capWait(this._kindReady(idx, 'pres'), Math.max(0, PRES_WAIT_MS - (now - this._presWait.since)), this._presWait);
    } else this._presWait = null;
    // The lake frames likewise (plan section 50): a landing paints with its own lake frame (never an old lake under a new
    // time, never a lake blinking in after the step); the first picture does not wait; a missing, failed or slow lake
    // frame (the same cap, counted per step) never holds a step: it lands bare, the lake joins when the frame arrives.
    var pl = null;
    if (this._wantLake() && this._lakeIndex(idx) >= 0 && this.layer && this.layer.hasFrame() && !this._isUnavailable(idx)) {
      var lkey = this._key(idx, 'lake'), lnow = Date.now(), lwaits = [this._kindReady(idx, 'lake')];
      if (this._wantLakeDir()) lwaits.push(this._kindReady(idx, 'lakedir'));
      if (!this._lakeWait || this._lakeWait.key !== lkey) this._lakeWait = { key: lkey, since: lnow, timer: null };
      pl = capWait(Promise.all(lwaits), Math.max(0, PRES_WAIT_MS - (lnow - this._lakeWait.since)), this._lakeWait);
    } else this._lakeWait = null;
    var waits = [p]; if (pd) waits.push(pd); if (pp) waits.push(pp); if (pl) waits.push(pl);
    return (waits.length > 1 ? Promise.all(waits).then(function (all) { return all[0]; }) : p).then(function (frame) {
      if ((sig && sig.aborted) || self.target !== idx || !self.layer || self.manifest !== m || self.field !== field || self.res !== res) throw abortError();
      // the frame already on the map (Retry after an outage, a repeated seek): no redraw, but the same
      // state transition as a fresh landing, or the panel would stay on "Loading"
      if (self.layer._frame !== frame) {
        var half = res === 'half';
        self.layer.setContours(self._contourCfg(), true);                        // (setFrame redraws with it)
        self.layer.setFrame(frame, half ? m.grid_half : m.grid, field, m.fields[field], self._lut(), m.frames[idx], self._lakeHit(idx));   // the step's lake frame rides in the one redraw
      }
      self.frameIndex = idx; self._pendingRestore = null; self._carry = null;   // a frame is on the map: the saved state is spent
      self._presWait = null; self._lakeWait = null;
      self._persist();
      self._syncLook();
      self._attribute();
      if (!self.last || self.last.state !== 'ready') self.render({ state: 'ready' }); else self._syncUI();
      self._syncLake(idx);                                                       // before the particles: the lake direction rides in the step's one rebuild
      self._syncFlow(idx);
      self._syncIsobars(idx);
      self._prefetch();
      self._retryLakes();
    });
  };
  // `p` or `ms`, whichever settles first (never rejects: _kindReady never does); the timer is cleared with p, and kept in
  // `rec.timer` (the step's wait record) so that Off can clear it too.
  function capWait(p, ms, rec) {
    var tm = null;
    function done() { if (tm !== null) { clearTimeout(tm); tm = null; } if (rec && rec.timer !== null) rec.timer = null; }
    return Promise.race([p.then(done), new Promise(function (r) { tm = setTimeout(function () { tm = null; if (rec) rec.timer = null; r(); }, ms); if (rec) rec.timer = tm; })]);
  }
  // The step the ring is planned around: a pending seek / step target, else the frame on the map.
  Overlay.prototype._anchorIndex = function () { return this.target !== null ? this.target : this.frameIndex; };
  // ?ovdebug=1: the animator's and the scheduler's state in the panel, once a second (the owner's iPad; text only).
  Overlay.prototype._debugText = function () {
    var f = this.flow ? this.flow.debugState() : null, dec = frameDecoder ? (frameDecoder.dead ? 'dead' : (frameDecoder.worker ? 'worker' : 'idle')) : (decoderDead ? 'dead' : 'none');
    var o = { mode: this.mode, model: this.model, playing: this.playing, speed: this.speed, frame: this.frameIndex, target: this.target, res: this.res + '/' + this.dres, inflight: Object.keys(this.inflight).length,
      cache: this.cache.size() + '/' + this.dcache.size() + '/' + this.pcache.size(), decoder: dec, isobars: this.iso ? (this.iso.entry ? this.iso.entry.step : '-') + ' ' + this.iso.drawn.lines + 'l/' + this.iso.drawn.labels + 'p/' + this.iso.drawn.hl + 'm ' + this.iso.lastMs + 'ms' + (this.iso.terrain ? ' terrain' : ' no-terrain') : (this.isobars ? 'none' : 'off'), hidden: typeof document !== 'undefined' ? !!document.hidden : null, dpr: typeof window !== 'undefined' ? window.devicePixelRatio : null };
    var lines = [], k;
    for (k in o) lines.push(k + ': ' + o[k]);
    if (this.lake) lines.push('lake: ' + this.lake.status + ' ' + (this.lake.manifest ? this.lake.manifest.run : '-') + (this.lake.next ? ' next ' + this.lake.next.m.run : '') + (this._lakeNear === false ? ' (off the view)' : '') + ', drawn ' + (this.layer && this.layer._lake ? this.layer._lake.entry.step : '-') + ', cache ' + this.lcache.size() + '/' + this.ldcache.size());
    if (this.radarPlayer) { var rs = this.radar; lines.push('radar: step ' + this.radarPlayer.idx + '/' + this.radarPlayer.frames.length + (this.radarPlayer.playing ? ' playing' : '') + ', loop ' + this.radarPlayer.loopH + ' h, ' + (rs.bytes / 1048576).toFixed(1) + ' MB, stack ' + (rs.stack.t1 ? 't1' : '-') + (rs.stack.t2 ? '+t2' : '')); }
    if (this.sst) lines.push('sst: ' + this.sst.status + ' ' + (this.sst.manifest ? this.sst.manifest.run : '-') + ', cells ' + (this.sst.bytes / 1048576).toFixed(1) + ' MB, in flight ' + Object.keys(this.sst.inflight).length);
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
    var shown = this.frameIndex, keepF = this._key(shown), keepD = this._wantDir() ? this._key(shown, 'dir') : null, keepP = this._wantPres() ? this._key(shown, 'pres') : null;
    var shownL = this._lakeIndex(shown) >= 0, keepL = shownL && this._wantLake() ? this._key(shown, 'lake') : null, keepLD = shownL && this._wantLakeDir() ? this._key(shown, 'lakedir') : null;
    for (i = 0; i < plan.length; i++) rank[this._key(plan[i][0], plan[i][1])] = i;
    var drawnF = this.layer ? this.layer._frame : null, drawnD = this.flow ? this.flow.dir : null, drawnP = this.iso ? this.iso.frame : null, own = this._own();
    var drawnL = this.layer && this.layer._lake ? this.layer._lake.frame : null, drawnLD = this.flow ? this.flow.ldir : null;
    this.cache.limit(r.decoded, keepF, rank, drawnF); this.dcache.limit(r.decoded, keepD, rank, drawnD); this.pcache.limit(r.decoded, keepP, rank, drawnP);
    this.lcache.limit(r.decoded, keepL, rank, drawnL); this.ldcache.limit(r.decoded, keepLD, rank, drawnLD);
    cap = this._maxInflight();
    for (k in this.inflight) if (rank[k] === undefined) { this.inflight[k].abort.abort(); delete this.inflight[k]; }
    keys = Object.keys(this.inflight);
    if (keys.length > cap) {
      keys.sort(function (a, b) { return rank[b] - rank[a]; });
      for (i = 0; i < keys.length && Object.keys(this.inflight).length > cap && rank[keys[i]] >= own; i++) { this.inflight[keys[i]].abort.abort(); delete this.inflight[keys[i]]; }
    }
    if (shrinkOnly) return;
    // Only what the caches can hold beside the frame on the map is started: with a pending target far from it (a seek
    // into a cooldown, a failed far seek) the ring and the drawn frame together exceed the budget by one, and a ring
    // frame fetched into a full cache would evict another ring frame, whose landing would fetch it back, without end.
    // The same reserve while the frame on the map (or the animator's direction) is the OTHER resolution's, kept by value
    // beside the ring until the new one lands.
    var ring = ringPlan(this._anchorIndex(), this.n, this.dir, r.ahead, r.behind);
    var drawnInRing = ring.indexOf(shown) >= 0 && (!drawnF || this.cache.map.get(keepF) === drawnF) && (!keepD || !drawnD || this.dcache.map.get(keepD) === drawnD) && (!keepP || !drawnP || this.pcache.map.get(keepP) === drawnP) &&
      (!keepL || !drawnL || this.lcache.map.get(keepL) === drawnL) && (!keepLD || !drawnLD || this.ldcache.map.get(keepLD) === drawnLD);
    var room = r.decoded - (drawnInRing ? 0 : 1);
    for (i = 0; i < plan.length && Object.keys(this.inflight).length < cap; i++) {
      var idx = plan[i][0], kind = plan[i][1], key = this._key(idx, kind);
      if (ring.indexOf(idx) >= room) continue;
      if (this.inflight[key] || this._isUnavailable(idx, kind) || this._cacheOf(kind).has(key)) continue;
      if (kind !== 'field' && this._isUnavailable(idx)) continue;   // never the extras of a step that cannot be shown
      if (kind === 'dir') this._ensureDir(idx); else if (kind === 'pres') this._ensurePres(idx); else if (kind === 'lake') this._ensureLake(idx); else if (kind === 'lakedir') this._ensureLakeDir(idx); else this._ensure(idx).catch(function () {});
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
      if (this.flow) { this.flow.clearLakeData(); this.flow.clearData(); }
      if (this.iso) this.iso.clearData();
      if (this.readout) this.readout.hidden = true;
      this._unattribute();
    }
    if (this.mode === 'obs' && this.field === 'sat') this._detachSat();   // the own entry failed: no dots under the error panel
    if (this.mode === 'obs' && this.field === 'radar') { this._detachRadar(); this._radarRecover(); }
    if (this.mode === 'obs' && this.field === 'bathy') this._detachBathy();   // no depths under the error panel
    if (this.iso && (this.frameIndex === null || !this.layer || !this.layer.hasFrame())) this.iso.clearData();   // no picture, no lines (G32)
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
      // a time position is pinned (on the map, or carried through SST: G31-8): banner only
      if (self.manifest && (self.frameIndex !== null || (self._carry && !self._carry.free))) { self.newerRun = ptr; return self.manifest; }
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
      if (this.newerRun && this.manifest && this.frameIndex === null && !(this._carry && !this._carry.free)) {
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
    this.cache.clear(); this.dcache.clear(); this.pcache.clear(); this.lcache.clear(); this.ldcache.clear(); this.unavailable = {}; this.frameIndex = null; this.target = null;
    if (this.flow) { this.flow.clearLakeData(); this.flow.clearData(); }
    if (this.iso) this.iso.clearData();
  };
  // Every RUN_CHECK_MS while mounted: a newer complete run only raises the "Update" banner.
  Overlay.prototype._checkRun = function () {
    var self = this, model = this.model, m = this.manifest;
    if (this.mode === 'obs') {
      if (this.field === 'radar' || this.field === 'bathy') return;   // (radar: its pointer has its own timer; bathymetry: static)
      if (this.field === 'sat') { this._satLineSync(); this._gridLineSync(); return; }     // (the pointers' own re-reads run on satTimer)
      if (this.sst) this.sst.recheck();
      var m = this.sst && this.sst.manifest, age = m ? sstAgeDays(m.day) : null;
      if (this.last && this.last.state === 'ready' && (age !== null && age > SST_STALE_DAYS) !== !!this._sstStale) this.render(this.last);   // G31-15
      return;
    }
    if (!m) return;
    if (this.lake && this._def().lakes) this.lake.recheck();                    // a newer lake run is taken between landings (_goto)
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
      self._ensureIso();                                   // ... or pressure (G32 A-P2-1)
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
    if (this.mode === 'obs') return;
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
    if (this.mode === 'obs') { if (this.radarPlayer) this.radarPlayer.speedChanged(); return; }   // never the model's ring under an observation
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
      if (self.radarPlayer) self.radarPlayer.visibility(!!document.hidden);
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
    var text = this.mode === 'obs' ? (this.field === 'radar' ? radarCredit(this._radarSources()) : this.field === 'bathy' ? bathyCredit(this._bathySources().map(bathyShort)) : OBS_DEFS[hasObs(this.field) ? this.field : 'sst'].attribution) : this._def().attribution;
    if (this.mode !== 'obs' && this.layer && this.layer.hasLake() && this.layer._lakeMask && this._lakeOnScreen()) text += LAKE_ATTRIBUTION;   // a lake frame drawn, the lakes' coverage, and the lakes in the view (plan section 50; G33 B-5)
    if (!(this.layer && this.layer._masked() && this.coast)) return text;
    var url = String(this.coast.url + '/LICENSE.txt').replace(/["<>]/g, encodeURIComponent);
    return text + ' · <a href="' + url + '" target="_blank" rel="noopener license">GSHHG</a>';
  };
  // Lake water of the drawn lake frame on screen: its grid meets the view itself (no margin) and a tile in view has lake
  // coverage in its visible part; the credit names what is on screen (G33 B-5; the grid's box alone holds most of
  // Michigan's Lower Peninsula: re-check P3-5).
  Overlay.prototype._lakeOnScreen = function () {
    var ll = this.layer ? this.layer._lake : null;
    return !!(ll && this.map && lakeNearView(viewOf(this.map, this.layer), ll.grid, 0) && this.layer.lakeWaterInView(this.map));
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
    this._on('zoomend', function () { self._radarView(); self._checkRes(); self._syncLakeGate(); self._syncLakeOutlines(); });
    this._on('moveend', function () { self._syncLakeGate(); self._radarView(); self._bathyViewSoon(); });
    this._on('viewreset', function () { self._syncLakeGate(); });
    this._on('move', function () { if (self.layer) self.layer._drawStale(true); if (self.satLayer) self.satLayer._drawStale(true); if (self.radarLayer) self.radarLayer._drawStale(true); });   // a buffer tile a frame behind comes into view
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
    this._sizeAttribution(); this._checkRes(); this._syncLakeGate();
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
    if (this._presWait) { if (this._presWait.timer !== null) clearTimeout(this._presWait.timer); this._presWait = null; }
    if (this._lakeWait) { if (this._lakeWait.timer !== null) clearTimeout(this._lakeWait.timer); this._lakeWait = null; }
  };
  Overlay.prototype.unmount = function () {
    var self = this;
    this._noteSettled('off');                              // Off before a first picture: nothing to wait for any more
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
    if (this.layer) { if (this.layer._src) this.layer.setSource(null, true); this.map.removeLayer(this.layer); this.layer = null; }
    this._detachSat();                                     // the satellite layer goes; the stores keep their samples (an On is instant)
    this._detachRadar();                                   // the radar's pictures go with it (no radar request after Off)
    this._detachBathy();                                   // the depths' too (no bathymetry request after Off)
    if (this.sat) this.sat.abortAll();
    if (this.swell) this.swell.abortAll();
    if (this.sst) this.sst.abortAll();                     // the day's frame and manifest stay for the next On
    if (this.grid) this.grid.abortAll();                   // the gridded map's frame stays too (plan section 82)
    if (this.lake) this.lake.abortAll();                   // the lake run stays for the next On
    if (this.lakeCoast) this.lakeCoast.abortAll();
    if (this.lakeOutCoast) this.lakeOutCoast.abortAll();
    this.mode = 'model';
    this._syncLook();                                      // the page's imagery back, no blend
    if (this.coast) this.coast.abortAll();                 // the decoded coastlines stay for the next On
    if (this.flow) { this.flow.detach(); this.flow = null; }
    if (this.iso) { this.iso.detach(); this.iso = null; }
    this._unattribute();
    this._unbindReadout();
    this._listeners.forEach(function (l) { self.map.off(l[0], l[1]); }); this._listeners = [];
    this._unbindSize();
    this._removeSheet();
    this.cache.clear(); this.dcache.clear(); this.pcache.clear(); this.lcache.clear(); this.ldcache.clear(); this.unavailable = {}; this.target = null; this.wasPlaying = false; this.ui = null; this._lakeNear = null; this._lakeGateNear = null; this._dropLakeHs();
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
    var cfg = { step: c[unit], per: u.scale !== undefined ? u.scale : u.f(1), off: u.offset || 0 };
    if (this.field === 'sst') { cfg.coarseBelow = SST_COARSE_BELOW; cfg.zero = true; }   // 0 C is a line; 0 m / 0 s is not
    return cfg;
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
    this.flow.setOpacity(this.particleOpacity);
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
  // Settles when the frame of kind 'dir' / 'pres' of idx is decoded, unavailable, or its fetch has failed or been
  // aborted (at the abort itself: a decode that cannot be cancelled must not hold the step); never rejects. What a
  // step waits for beside its field frame.
  Overlay.prototype._kindReady = function (idx, kind) {
    var key = this._key(idx, kind);
    if (this._cacheOf(kind).has(key) || this._isUnavailable(idx, kind)) return Promise.resolve();
    if (!this.inflight[key]) this._startKind(idx, kind);
    var rec = this.inflight[key];
    if (!rec) return Promise.resolve();
    return new Promise(function (resolve) {
      var done = function () { resolve(); };
      rec.promise.then(done, done);
      if (rec.abort.signal.aborted) done(); else rec.abort.signal.addEventListener('abort', done);
    });
  };
  Overlay.prototype._dirReady = function (idx) { return this._kindReady(idx, 'dir'); };
  // Start the 'dir' / 'pres' frame of idx unless it is cached, in flight or unavailable. Ranked by the plan around the
  // PENDING target (else the frame on the map): a full in-flight set gives up its lowest-ranked ring fetch, never one of
  // the target's own fetches (its field, direction and pressure frames: ranks below _own()).
  Overlay.prototype._startKind = function (idx, kind) {
    var key = this._key(idx, kind);
    if (this._cacheOf(kind).has(key) || this.inflight[key] || this._isUnavailable(idx, kind)) return;
    if (Object.keys(this.inflight).length >= this._maxInflight()) {
      var plan = this._plan(this._anchorIndex()), rank = {}, i, k, worst = null, worstRank = -1;
      for (i = 0; i < plan.length; i++) rank[this._key(plan[i][0], plan[i][1])] = i;
      for (k in this.inflight) { var r = rank[k] === undefined ? 1e9 : rank[k]; if (r > worstRank) { worstRank = r; worst = k; } }
      if (worst === null || worstRank < this._own()) return;                // only the target's own fetches are in flight (never evicted)
      this.inflight[worst].abort.abort(); delete this.inflight[worst];
    }
    if (kind === 'dir') this._ensureDir(idx); else if (kind === 'pres') this._ensurePres(idx); else if (kind === 'lake') this._ensureLake(idx); else this._ensureLakeDir(idx);
  };
  Overlay.prototype._startDir = function (idx) { this._startKind(idx, 'dir'); };
  // ---- the isobars (plan section 46) ----
  // The run's pressure field, when it can be drawn: in the manifest, usable, with a valid half grid (memoised per
  // manifest like the direction's). A run published before the field existed has none: no lines, no message.
  Overlay.prototype._presDef = function () {
    var m = this.manifest, f = m ? m.fields[PRES_FIELD] : null;
    if (!presFieldOk(f)) return null;
    if (this._presGridFor !== m || this._presGridField !== f) {
      this._presGridFor = m; this._presGridField = f;
      try { validateGrid(m.grid_half, { cols: m.grid_half.cols, rows: m.grid_half.rows, q: { length: m.grid_half.cols * m.grid_half.rows } }, f); this._presGridOk = true; }
      catch (e) { this._presGridOk = false; }
    }
    return this._presGridOk ? f : null;
  };
  Overlay.prototype._wantPres = function () { return !!(this.isobars && this.layer && PRES_UNDER[this.field] && this._presDef()); };
  // The isobar layer exists exactly while the isobars are wanted (the setting on, wind shown, the run has the field).
  Overlay.prototype._ensureIso = function () {
    if (!this._wantPres()) { if (this.iso) { this.iso.detach(); this.iso = null; } return; }
    if (!this.iso) {
      var self = this, iso = this.iso = new IsobarLayer(this.map);
      iso.attach();
      loadTerrain(this.opts).then(function (q) { if (self.iso === iso) iso.setTerrain(q); });
    }
  };
  // The half grid's terrain (the asset terrain_half.png, one fetch per page: the marks of every later layer share it); null
  // when it cannot be had (the marks are then drawn over high ground too; asked again by the next layer).
  var terrainPromise = null;
  function loadTerrain(opts) {
    if (!terrainPromise) {
      terrainPromise = new Promise(function (res) { res(decodeFrame(assetUrl(opts, 'terrain_half.png'), null, { cols: 720, rows: 361 }, null, true)); })
        .then(function (f) { return f && f.q && f.q.length === 720 * 361 ? f.q : null; })
        .catch(function () { terrainPromise = null; return null; });
    }
    return terrainPromise;
  }
  // The pressure frame for idx, delivered to the layer only if the LAYER shows that step of that run at this field (its
  // frame entry); a failure is swallowed: the wind plays on without its isobars.
  Overlay.prototype._ensurePres = function (idx) {
    var self = this, m = this.manifest, field = this.field;
    return this._ensure(idx, 'pres').then(function (frame) {
      if (self.iso && self.layer && self.layer.hasFrame() && self.layer.entry === m.frames[idx] && self.manifest === m && self.field === field && self._wantPres()) {
        self._isoData(frame, m.grid_half, m.frames[idx], m.fields[PRES_FIELD]);
      }
    }).catch(function (err) {
      if (err && !err.unavailable && err.name !== 'AbortError' && typeof console !== 'undefined' && console.warn) console.warn('overlay isobars', err);
    });
  };
  // The frame idx is on the map: the layer draws its pressure if decoded; another step's lines are cleared at once
  // (never old lines under a new time); the fetch starts only when no other target is pending (that target's own
  // frames come first and bring their pressure when they land).
  Overlay.prototype._syncIsobars = function (idx) {
    if (!this.iso) return;
    var m = this.manifest, entry = m.frames[idx];
    if (!this._wantPres() || !this.layer.hasFrame() || this.layer.entry !== entry) { if (this.iso.entry !== entry) this.iso.clearData(); return; }
    var hit = this.pcache.get(this._key(idx, 'pres'));
    if (hit) { this._isoData(hit, m.grid_half, entry, m.fields[PRES_FIELD]); return; }
    if (this.iso.entry !== entry) this.iso.clearData();
    if (this.target === null || this.target === idx) this._startKind(idx, 'pres');
  };
  // The layer takes a pressure frame; an exception in it (a refused canvas context) is logged once and never reaches the
  // playback: the wind plays on without its isobars.
  Overlay.prototype._isoData = function (frame, grid, entry, fdef) {
    try { this.iso.setData(frame, grid, entry, fdef); }
    catch (e) { if (!this._isoWarned && typeof console !== 'undefined' && console.warn) { this._isoWarned = true; console.warn('overlay isobars', e); } }
  };
  // The lakes' shores (plan section 51) are wanted under an outlined field (wind) at tile zoom 4 or more; the set loads
  // then (never holding a draw) and is given to the layer. Not wanted (another field, SST, a lower zoom): the layer
  // drops it, the tiles their masks, the set its queued cells (V7-5); it is given back when wanted again.
  Overlay.prototype._wantLakeOutline = function () {
    return !!(this.lakeOutCoast && this.layer && this.mode !== 'obs' && OUTLINE_FIELDS[this.field] &&
      Math.round(this.map.getZoom()) >= LAKE_OUTLINE_MIN_ZOOM);
  };
  Overlay.prototype._syncLakeOutlines = function () {
    var self = this, st = this.lakeOutCoast;
    if (!this.layer || !st) return;
    if (!this._wantLakeOutline()) {
      if (this.layer._lakeOut) this.layer.setLakeOutlineCoast(null);
      st.dropQueue();
      return;
    }
    if (this.layer._lakeOut === st) return;
    st.load().then(function (store) {
      if (!store || !self.layer || self.layer._lakeOut === store || !self._wantLakeOutline()) return;
      self.layer.setLakeOutlineCoast(store);
    }, function () {});
  };
  // The page's setting (the gear): off drops the pressure downloads and frames at once; on fetches the shown step's.
  Overlay.prototype.setIsobars = function (on) {
    this.isobars = !!on;
    if (!this.manifest || !this.field || !this.layer) return;
    this._ensureIso();
    if (!this.isobars) {
      for (var k in this.inflight) if (this.inflight[k].kind === 'pres') { this.inflight[k].abort.abort(); delete this.inflight[k]; }
      this.pcache.clear();
    }
    if (this.iso && this.frameIndex !== null) this._syncIsobars(this.frameIndex);
    this._prefetch();                                      // starts or drops the pressure fetches (planned around a pending target)
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
      for (var k in this.inflight) if (this.inflight[k].kind === 'dir' || this.inflight[k].kind === 'lakedir') { this.inflight[k].abort.abort(); delete this.inflight[k]; }
      this.dcache.clear(); this.ldcache.clear();
    }
    if (this.flow && this.frameIndex !== null) this._syncFlow(this.frameIndex);
    this._prefetch();                                      // starts or drops the direction fetches (planned around a pending target)
  };
  // The page's particles slider: fades the particles; at 0 they stop altogether (the loop, the direction downloads and
  // their cache, as setAnim(false)); back above 0 the shown step's direction is fetched again.
  Overlay.prototype.setParticleOpacity = function (k) {
    this.particleOpacity = particleOpacityOf(k);
    var on = this.particleOpacity > 0;
    if (on !== this.anim) this.setAnim(on);
    if (this.flow) this.flow.setOpacity(this.particleOpacity);
  };
  Overlay.prototype.setContours = function (on) {
    this.contours = !!on;
    if (this.layer) this.layer.setContours(this._contourCfg());
  };

  // Readout: hover only on hover-capable pointers, long-press (450 ms) on touch; a plain tap shows
  // nothing (but on a satellite mark: its readout, plan section 81). Marker events are unaffected (the pane has no pointer events); controls are ignored.
  // The isobars' pressure at a point, rounded to the hPa (the readout's "· 1004 hPa"): only from the pressure frame of the
  // step on the map (never another step's lines' frame), else null.
  Overlay.prototype._hpaAt = function (lat, lng) {
    if (!this.iso || !this.iso.frame || !this.layer || this.iso.entry !== this.layer.entry) return null;
    var v = this.iso.valueAt(lat, lng);
    return v === null ? null : Math.round(v);
  };
  // The readout's text for it: "<= 920" / ">= 1060" within half a code of the field's clamped ends (the stored range;
  // a typhoon's eye below 920 hPa reads "<= 920", not "920"), else the rounded value.
  Overlay.prototype._hpaText = function (lat, lng) {
    var h = this._hpaAt(lat, lng);
    if (h === null) return null;
    var f = this.iso.fdef, v = this.iso.valueAt(lat, lng), half = (f.hi - f.lo) / 254 / 2;
    if (v <= f.lo + half) return '≤ ' + Math.round(f.lo);
    if (v >= f.hi - half) return '≥ ' + Math.round(f.hi);
    return String(h);
  };
  // The readout's word on lake water without a value (plan section 50; G33 B-2, owner): under peak period, water whose
  // lake height at the pixel is present and under LAKE_CALM_M is calm water ("calm": the job leaves a calm cell's period
  // blank); else "no data" (past the lake run's horizon, no lake run, a channel GLWU does not model, ice). The shown
  // step's lake height frame is fetched on demand, one per hovered step and never drawn; until it is in, "…" (onReady
  // shows the word when it lands).
  var LAKE_CALM_M = 0.2;
  Overlay.prototype._lakeWord = function (lat, lng, z, onReady) {
    var L = this.layer, ll = L ? L._lake : null, lm = this.lake ? this.lake.manifest : null;
    if (this.field !== 'tp' || !ll || !lm || ll.run !== lm.run || this.frameIndex === null || L.entry !== ll.entry || !lm.fields.hs) return 'no data';
    var key = this._lakeHsKey();
    if (key === null) return 'no data';
    var li = this._lakeIndex(this.frameIndex), h = this._lakeHs, self = this, fh = lm.fields.hs;
    var word = function (frame) {                          // calm: the hovered point's lake height present and under LAKE_CALM_M
      var c = frame ? L.lakeCodeAt(frame, lm.grid, lat, lng, z) : 0;
      return c && fh.lo + (c - 1) / 254 * (fh.hi - fh.lo) < LAKE_CALM_M ? 'calm' : 'no data';
    };
    if (!h || h.key !== key) {
      this._dropLakeHs();
      var ctrl = new AbortController(), rec = this._lakeHs = { key: key, frame: null, failed: false, abort: ctrl, timer: null };
      // the readout re-shows while the frame is still the shown step's; for a step played on since it is handed the word of
      // the hovered point and step instead (a re-show would ask for the new step's frame, and so on: G33 re-check P3-4);
      // a frame silent for LOAD_STALL_MS reads "no data" (N-3)
      var tell = function () {
        if (!onReady || self._lakeHs !== rec) return;
        if (self._lakeHsKey() === rec.key) onReady(); else onReady(word(rec.frame));
      };
      rec.timer = setTimeout(function () {             // given up whatever the decode does next (one past its download cannot be cancelled)
        rec.timer = null; ctrl.abort();
        if (self._lakeHs === rec && !rec.frame && !rec.failed) { rec.failed = true; rec.abort = null; tell(); }
      }, LOAD_STALL_MS);
      decodeFrame(this.root + '/' + frameKey(lm, lm.frames[li], 'hs', false), ctrl.signal, lm.grid).then(function (f) {
        validateLakeGrid(lm.grid, f);
        if (self._lakeHs !== rec || rec.failed) return;
        clearTimeout(rec.timer); rec.timer = null; rec.frame = f; rec.abort = null;
        tell();
      }).catch(function () {
        if (self._lakeHs !== rec || rec.failed) return;
        clearTimeout(rec.timer); rec.timer = null; rec.failed = true; rec.abort = null;
        tell();
      });
      return '…';
    }
    if (!h.frame) return h.failed ? 'no data' : '…';
    return word(h.frame);
  };
  // The shown step's lake height frame key (run/step), or null without a lake frame for it.
  Overlay.prototype._lakeHsKey = function () {
    var lm = this.lake ? this.lake.manifest : null, li = lm && this.frameIndex !== null ? this._lakeIndex(this.frameIndex) : -1;
    return li >= 0 ? lm.run + '/' + lm.frames[li].step : null;
  };
  Overlay.prototype._bindReadout = function () {
    var self = this, map = this.map, el = mk('div', 'ov-readout'); el.hidden = true;
    map.getContainer().appendChild(el); this.readout = el;
    var pressTimer = null, hideTimer = null, lastTouch = 0, last = null, satShown = false;   // satShown: the readout shows a satellite mark (G52 B-4)
    // one hide timer at a time: an earlier lift's timer must not hide a later press's readout (G57: a press within 1.5 s of a lift)
    function hideIn(ms) { if (hideTimer) clearTimeout(hideTimer); hideTimer = setTimeout(function () { hideTimer = null; el.hidden = true; }, ms); }
    function overControl(ev) { var t = ev && ev.target; return !!(t && t.closest && t.closest('.leaflet-control, .ov-sheet')); }
    function place(pt) {
      el.hidden = false;
      var W = map.getContainer().clientWidth, left = false, x = pt.x;
      if (pt.x > W - READOUT_NEAR_EDGE) {                    // measured only near the edge: a hover costs no layout elsewhere
        var w = el.offsetWidth;
        if (pt.x + READOUT_DX + w > W - READOUT_PAD) {
          if (pt.x - READOUT_DX - w >= READOUT_PAD) left = true;   // the other side of the pointer
          else x = Math.max(READOUT_PAD - READOUT_DX, W - READOUT_PAD - w - READOUT_DX);   // neither side has room: as far over as it fits
        }
      }
      el.style.left = x + 'px'; el.style.top = Math.max(pt.y, READOUT_DY + READOUT_PAD) + 'px';
      el.classList.toggle('ov-readout-left', left);
    }
    function show(latlng, pt) {
      var layer = self.layer, v = null, z = 0;
      if (layer && layer.fdef) {
        z = typeof layer._tileZoom === 'number' ? layer._tileZoom : Math.round(map.getZoom());
        v = layer.readoutAt(latlng.lat, latlng.lng, z);            // the drawn pixel (its centre, its land mask), not the raw cursor point
      }
      last = { latlng: latlng, pt: pt };
      if (self.mode === 'obs' && self.field === 'bathy') {   // the depth drawn under the cursor (plan section 93b)
        var bl = self.bathyLayer, bz = bl ? (typeof bl._tileZoom === 'number' ? bl._tileZoom : Math.round(map.getZoom())) : 0, bt = bl ? self._bathyReadout(bl.readoutAt(latlng.lat, latlng.lng, bz)) : null;
        if (!bt) { el.hidden = true; return; }
        el.textContent = bt; place(pt); return;
      }
      if (self.mode === 'obs' && self.field === 'radar') {   // the pixel drawn under the cursor (plan section 85)
        var rl = self.radarLayer, rz = rl ? (typeof rl._tileZoom === 'number' ? rl._tileZoom : Math.round(map.getZoom())) : 0, rt = rl ? self._radarReadout(rl.readoutAt(latlng.lat, latlng.lng, rz)) : null;
        if (!rt) { el.hidden = true; return; }
        el.textContent = rt; place(pt); return;
      }
      // a satellite reading under the cursor comes first (plan section 81): the top-most DRAWN one within its radius; on the
      // layer's own entry nothing else is shown
      var sat = self.satLayer;
      if (sat && sat.hasWindow()) {
        var sz = typeof sat._tileZoom === 'number' ? sat._tileZoom : Math.round(map.getZoom()), hit = sat.hitAt(latlng.lat, latlng.lng, sz);
        if (hit >= 0) { el.textContent = self._satReadout(hit, latlng, z || sz, v); satShown = true; place(pt); return; }
      }
      satShown = false;
      if (self.mode === 'obs' && self.field === 'sat') {     // the own entry: the gridded map's value over water (plan section 82), else nothing
        if (v === null || v === undefined || !self._wantGrid() || !self._gridShown()) { el.hidden = true; return; }
        el.textContent = self._gridReadout(v); place(pt); return;
      }
      if ((v === null || v === undefined) && layer && layer.fdef && layer.lakeAt(latlng.lat, latlng.lng, z)) {   // plan section 50 (owner): "no data" / "calm" (G33 B-2)
        el.textContent = self._lakeWord(latlng.lat, latlng.lng, z, function (w) {
          if (el.hidden || !last) return;
          if (w === undefined) show(last.latlng, last.pt);            // the shown step's frame landed: shown afresh
          else if (el.textContent === '…') el.textContent = w;        // the hovered step's word (playback moved on); never over other text
        });
        place(pt); return;
      }
      if (v === null || v === undefined) { el.hidden = true; return; }
      var u = unitOf(layer.field, self.opts.getUnit()), f = layer.fdef;
      var txt = u.f(v).toFixed(u.d) + ' ' + u.label;
      if (v <= f.lo + 1e-9 && f.lo > 0) txt = '≤ ' + txt; else if (v >= f.hi - 1e-9) txt = '≥ ' + txt;   // "<=" only where values below lo exist (Tp)
      var hpa = self._hpaText(latlng.lat, latlng.lng);
      if (hpa !== null) txt += ' · ' + hpa + ' hPa';
      el.textContent = txt; place(pt);
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
      if (hideTimer) { clearTimeout(hideTimer); hideTimer = null; }
      if (e.touches.length !== 1 || overControl(e)) return;
      var t = e.touches[0];
      pressTimer = setTimeout(function () { pressTimer = null; var pt = map.mouseEventToContainerPoint(t); show(map.containerPointToLatLng(pt), pt); }, 450);
    }
    // a plain tap on a satellite mark shows its readout for SAT_TAP_MS (plan section 81; G52 B-4): the field keeps the long press
    function te(e) {
      lastTouch = Date.now();
      var quick = !!pressTimer;
      if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; }
      var sat = self.satLayer;
      if (quick && e && e.type === 'touchend' && e.changedTouches && e.changedTouches.length === 1 && sat && sat.hasWindow() && !overControl(e)) {
        var pt = map.mouseEventToContainerPoint(e.changedTouches[0]), ll = map.containerPointToLatLng(pt);
        var sz = typeof sat._tileZoom === 'number' ? sat._tileZoom : Math.round(map.getZoom());
        if (sat.hitAt(ll.lat, ll.lng, sz) >= 0) { show(ll, pt); hideIn(SAT_TAP_MS); return; }
      }
      hideIn(!el.hidden && satShown ? SAT_TAP_MS : 1500);
    }
    c.addEventListener('touchstart', ts, { passive: true }); c.addEventListener('touchend', te, { passive: true }); c.addEventListener('touchmove', te, { passive: true }); c.addEventListener('touchcancel', te, { passive: true });
    this._touch = [c, ts, te, function () { if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; } if (hideTimer) { clearTimeout(hideTimer); hideTimer = null; } }];
  };
  Overlay.prototype._unbindReadout = function () {
    if (this._touch) {
      var c = this._touch[0];
      c.removeEventListener('touchstart', this._touch[1]); c.removeEventListener('touchend', this._touch[2]); c.removeEventListener('touchmove', this._touch[2]); c.removeEventListener('touchcancel', this._touch[2]);
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
        var sh = mk('div', 'ov-sheet'); sh.setAttribute('role', 'region'); sh.setAttribute('aria-label', 'Map overlay');
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
    if (this.bathyLayer) this.bathyLayer.setUnit(this.opts.getUnit());              // the depth lines in the site unit
  };
  // The forecast table's run beside the overlay's: the forecast table is always GFS-Wave (or SWAN): under another
  // model its run is named with its model.
  Overlay.prototype._runTailText = function (m) {
    var pc = typeof this.opts.pageCycle === 'function' ? this.opts.pageCycle() : this.opts.pageCycle;
    return pc && pc.model === 'SWAN' ? ' — the forecast table is a PacIOOS SWAN run'
      : pc && pc.model === 'GLWU' ? ' — the forecast table is a NOAA GLWU (Great Lakes) run'
      : pc && pc.model === 'NWPS' ? ' — the forecast table is a NOAA NWPS (nearshore) run'
      : pc && pc.run && this._def().table === 'model' ? ' — the forecast table is on GFS-Wave run ' + pc.run
      : pc && pc.run && pc.run !== m.run ? ' — the forecast table is on run ' + pc.run : '';
  };
  // The forecast table moved to a new run by itself (section 77d): only the run line follows, in place (no panel
  // rebuild: menus, the readout and focus stay; 77d review P3-4).
  Overlay.prototype.refreshRunTail = function () {
    var ui = this.ui;
    if (!ui || !ui.runText || !this.manifest) return;
    ui.runTail = this._runTailText(this.manifest);
    this._refreshRunLine();
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
    var shownSrc = null;
    function sync() {
      var a = animalOf(ov.speed), src = assetUrl(ov.opts, a.file);
      if (shownSrc !== src) { shownSrc = src; img.src = src; }     // (img.src reads back absolute: comparing it would ask again every step)
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
  // A promise of the first picture: it resolves once the first mount has drawn a picture or failed (or the overlay went
  // Off) and the coastline's tier 0 has landed or been given up. The page's location aids start their downloads after it,
  // so on a slow link the coastline is not starved (G34 B-6).
  Overlay.prototype.settled = function () {
    var self = this;
    return this._settledP || (this._settledP = new Promise(function (res) { self._settleFn = res; }));
  };
  // The first draw of a clipped field waits for the coastline at most COAST_FIRST_MS (owner, G34 re-check R-1). Resolves
  // to the store, to null (given up), or to undefined: still loading, the field is drawn unclipped with a note and
  // _coastLate clips it the moment the coastline lands (the download goes on: only 15 s with no byte gives it up).
  Overlay.prototype._coastFirst = function (coastP) {
    var self = this;
    if (!this.coast || this.coast.status !== 'loading') return coastP;
    return new Promise(function (res) {
      var timer = setTimeout(function () { timer = null; res(undefined); self._coastLate(coastP); }, COAST_FIRST_MS);
      coastP.then(function (store) { if (timer === null) return; clearTimeout(timer); timer = null; res(store); });
    });
  };
  // A coastline that lands after the first picture: the clipped or outlined field on the map takes it, and the panel's
  // note follows (gone, or "could not be loaded"). Not tied to one mount: whatever is shown when it lands takes it.
  Overlay.prototype._coastLate = function (coastP) {
    var self = this;
    coastP.then(function (store) {
      if (!self.layer || !self.field) return;
      if (store && self.layer._coast !== store && (CLIP_FIELDS[self.field] || OUTLINE_FIELDS[self.field])) {
        self.layer.setCoast(store);
        if (self.layer.hasFrame()) self._attribute();
      }
      if (self.last && self.last.state === 'ready') self.render(self.last);
    });
  };
  Overlay.prototype._noteSettled = function (state) {
    if (this._settledNoted) return;
    this._settledNoted = true;
    this.settled();
    var done = this._settleFn, cp = this.coast && this.coast.loading;
    Promise.resolve(cp).then(function () { done(state); }, function () { done(state); });
  };
  Overlay.prototype.render = function (st) {
    if (st && (st.state === 'ready' || st.state === 'error')) this._noteSettled(st.state);
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
      var obs = this.mode === 'obs', lt = obs ? 'Loading ' + OBS_DEFS[hasObs(this.field) ? this.field : 'sst'].label + '…' : 'Loading model frame…';
      if (this.models.length > 1 && !obs) { var lh = mk('div', 'ov-row ov-head'); lh.appendChild(this._modelSelect()); host.appendChild(lh); }
      host.appendChild(mk('div', 'ov-meta', lt)); this._say(lt); this._layoutSheet();
      if (focusCls === 'ov-model-sel' && this.models.length > 1 && !obs) this._refocus(focusCls, host); else this._pendingFocus = focusCls;
      return;
    }
    if (st.state === 'error') {
      this._say('Overlay unavailable: ' + st.message);
      var e = mk('div', 'ov-err', 'Overlay unavailable: ' + st.message + ' ');
      if (this.models.length > 1 && this.mode !== 'obs') e.appendChild(this._modelSelect());   // a failing model must be escapable
      e.appendChild(button('ov-retry', 'Retry', 'Retry loading the overlay', function () { self._retry(); }));
      host.appendChild(e);
      if (OV_DEBUG) { this.ui = { debug: mk('pre', 'ov-debug') }; host.appendChild(this.ui.debug); this._debugOn(); }   // the readout stays with the error (the owner's iPad)
      this._layoutSheet(); this._refocus(focusCls, host); return;
    }
    if (this.mode === 'obs') { this._renderObs(st, host, compact, focusCls, mapH); return; }
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
    body.appendChild(buildLegend(field, f, unit));
    // Run (UTC, with the forecast table's run when it differs), Updated and Next Update (the computer's own zone)
    var line = function (cls, name) { var d = mk('div', 'ov-meta ' + cls); d.appendChild(mk('b', null, name)); body.appendChild(d); return d; };
    var runLine = line('ov-run', 'Run: '); ui.runText = runLine.appendChild(document.createTextNode(''));
    ui.updLine = line('ov-upd', 'Updated: '); ui.updText = ui.updLine.appendChild(document.createTextNode(''));
    ui.nextLine = line('ov-next', 'Next Update: '); ui.nextText = ui.nextLine.appendChild(document.createTextNode(''));
    ui.runTail = this._runTailText(m);
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
    if (CLIP_FIELDS[field] && this.coast && !clipped) body.appendChild(mk('div', 'ov-warn', coastNote(this.coast)));
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
  // The legend over the LEGEND range in the site's units (the encoding range is wider; extremes clamp): the bar laid out by
  // legend position, the ticks at their values' positions.
  function buildLegend(field, f, unit) {
    var leg = mk('div', 'ov-legend'), cv = mk('canvas'); cv.width = 256; cv.height = 1; leg.appendChild(cv);
    var lut = legendBar(field, f.legend), ctx = cv.getContext('2d'), im = ctx.createImageData(256, 1);
    for (var i = 0; i < 256; i++) { im.data[i * 4] = lut[i * 3]; im.data[i * 4 + 1] = lut[i * 3 + 1]; im.data[i * 4 + 2] = lut[i * 3 + 2]; im.data[i * 4 + 3] = 255; }
    ctx.putImageData(im, 0, 0);
    var ticks = mk('div', 'ov-ticks'), tk = legendTicks(field, f, unit);
    tk.forEach(function (t, k) {
      var s = mk('span', k === 0 && t.pos < 0.03 ? 'ov-first' : k === tk.length - 1 ? 'ov-last' : '', t.label);   // centred unless at the edge (G31-13)
      s.style.left = (t.pos * 100).toFixed(2) + '%'; ticks.appendChild(s);
    });
    leg.appendChild(ticks);
    return leg;
  }
  // The observation's panel (plan section 44; owner, 2026-10-16: one date line in place of the run lines; no model
  // dropdown, no player, no timeline): head = the toggle and the source (folded: "SST · Thu, Oct. 15"); details = the
  // scale in the site's unit and the analysis day, with a note when the day is old or the coastline is missing. The same
  // fold rules as the frames' panel: open when the room allows, the viewer's fold kept, one line without a toggle when
  // nothing more would fit.
  Overlay.prototype._renderObs = function (st, host, compact, focusCls, mapH) {
    if (this.field === 'radar') return this._renderRadar(st, host, compact, focusCls, mapH);
    if (this.field === 'bathy') return this._renderBathy(st, host, compact, focusCls, mapH);
    if (this.field === 'sat') return this._renderSat(st, host, compact, focusCls, mapH);
    var self = this, m = this.sst && this.sst.manifest, unit = this.opts.getUnit();
    var dayText = m ? sstDayText(m.day) : '…', summary = 'SST · ' + dayText;
    var age = m ? sstAgeDays(m.day) : null;
    this._sstStale = age !== null && age > SST_STALE_DAYS;   // also for the one-line sheet (O-2)
    this._obsShell(st, host, compact, focusCls, mapH, SST_HEAD, summary, function (body) {
      body.appendChild(buildLegend('sst', SST_FDEF, unit));
      var dl = mk('div', 'ov-meta ov-day'); dl.appendChild(mk('b', null, 'Day: ')); dl.appendChild(document.createTextNode(dayText + ' (daily analysis)'));
      body.appendChild(dl);
      if (self._sstStale) body.appendChild(mk('div', 'ov-warn', 'Sea surface temperature data is ' + Math.floor(age) + ' days old.'));
      if (self.coast && !self.layer._clip) body.appendChild(mk('div', 'ov-warn', coastNote(self.coast)));
    });
  };
  // The observations' panel shell (sea surface temperature, the satellite readings): head = the toggle and the source's
  // line (folded: the summary); details = whatever `fill(body)` appends; the same fold rules as the frames' panel: open
  // when the room allows, the viewer's fold kept, one line without a toggle when nothing more would fit.
  Overlay.prototype._obsShell = function (st, host, compact, focusCls, mapH, headText, summary, fill, transport) {
    var self = this, cap = Math.floor(mapH * 0.4);
    var collapsed = this.collapsed !== undefined ? !!this.collapsed : false;
    var head = mk('div', 'ov-row ov-head');
    var btn = mk('button', 'ov-toggle', collapsed ? '▸' : '▾'); btn.type = 'button';
    btn.setAttribute('aria-label', collapsed ? 'Show overlay details' : 'Hide overlay details');
    btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true'); btn.setAttribute('aria-controls', 'ovDetails');
    btn.addEventListener('click', function () { self.collapsed = !collapsed; self.render(st); });
    head.appendChild(btn);
    var title = mk('span', 'ov-title' + (collapsed ? '' : ' ov-model'), collapsed ? summary : headText);
    head.appendChild(title);
    host.appendChild(head);
    this.ui = { host: host, title: title, obs: true, collapsed: collapsed, compact: compact };
    if (transport) transport(host, this.ui);                 // the radar's play, slider and speed: never folded away (plan section 85)
    var fold = function (keepToggle) {
      if (!keepToggle && btn.parentNode) head.removeChild(btn);
      title.textContent = summary; if (title.classList) title.classList.remove('ov-model');
      self.ui.collapsed = true; self.ui.radarLine = null; self.ui.radarLoop = null; self.ui.radarNote = null; self.ui.radarOpacity = null; self.ui.bathySources = null; self.ui.bathyOpacity = null; self.ui.satLine = null; self.ui.satSeg = null; self.ui.gridBox = null; self.ui.gridNote = null;
    };
    this._say('');                                           // shown: the "Loading..." status goes (G31-11)
    if (compact && cap - host.offsetHeight - 8 < 40) { fold(false); this._layoutSheet(); this._refocus(focusCls); return; }
    var wrap = mk('div', 'ov-body'); wrap.id = 'ovDetails'; host.appendChild(wrap);
    var body = mk('div', 'ov-details'); body.style.maxHeight = cap + 'px'; wrap.appendChild(body);
    fill(body);
    if (OV_DEBUG) { this.ui.debug = mk('pre', 'ov-debug'); body.appendChild(this.ui.debug); this._debugOn(); }
    var ctl = compact ? host : (host.closest && host.closest('.ov-ctl')) || host, ctlTop = compact ? 0 : this._ctlTop(ctl);
    var spare = function () { return compact ? cap - host.offsetHeight - 2 : mapH - self._stackHeight() - 10 - ctlTop - ctl.offsetHeight - 8; };
    var room = compact ? cap - (host.offsetHeight - body.offsetHeight) - 2 : Math.min(cap, spare() + body.offsetHeight);
    var cut = compact && this.field === 'radar' && body.scrollHeight > room;   // the radar's scale whole or not at all, and then no toggle that opens nothing
    if (room < 40 || collapsed || cut) { host.removeChild(wrap); fold(room >= 40 && !cut); }   // the toggle only when opening would show the details
    else body.style.maxHeight = room + 'px';
    this._layoutSheet();
    this._refocus(focusCls);
  };
  // The satellite layer's own panel (plan section 81): head "Satellite waves (CMEMS, last 24 h)"; details = the Heights |
  // Swell switch (only with the swell data offered), the wave-height scale (both kinds colour by height), the readings
  // line (newest reading, its age, the satellites; "loading n of m…" while chunks land), the credit line the licence
  // requires, a warning past the job's own stale limit; folded: "Satellite · newest 11:40 AM".
  Overlay.prototype._renderSat = function (st, host, compact, focusCls, mapH) {
    var self = this, unit = this.opts.getUnit(), store = this.satLayer ? this.satLayer.store : this._satStoreFor(), newest = store ? store.newestMs() : null;
    var when = newest !== null ? new Date(newest).toISOString().replace(/\.\d+Z$/, 'Z') : null;
    var summary = 'Satellite · ' + (when ? 'newest ' + this.opts.fmtTime(when, this.opts.tz) : '…');
    this._satStaleShown = this._satStale(store);
    this._gridWarnShown = this._gridState();
    this._obsShell(st, host, compact, focusCls, mapH, SAT_HEAD, summary, function (body) {
      if (self.swell) {
        var seg = mk('div', 'ov-row ov-seg'); seg.setAttribute('role', 'group'); seg.setAttribute('aria-label', 'Satellite readings shown');
        [['heights', 'Heights'], ['swell', 'Swell']].forEach(function (k) {
          var b = mk('button', 'ov-btn ov-seg-btn' + (self.satKind === k[0] ? ' ov-seg-on' : ''), k[1]); b.type = 'button';
          b.setAttribute('aria-pressed', self.satKind === k[0] ? 'true' : 'false'); b.setAttribute('data-kind', k[0]);
          b.addEventListener('click', function () { self.setSatKind(k[0]); });
          seg.appendChild(b);
        });
        body.appendChild(seg); self.ui.satSeg = seg;
      }
      body.appendChild(buildLegend('hs', SAT_FDEF, unit));
      // the "Gridded map" box (plan section 82): the note beside it says the step and its age, or the state
      if (self.grid) {
        var grow = mk('div', 'ov-row ov-satrow'), glab = mk('label', 'ov-sat-label'), gbox = mk('input', 'ov-sat-box'); gbox.type = 'checkbox'; gbox.checked = !!self.satGrid;
        glab.appendChild(gbox); glab.appendChild(mk('span', null, 'Gridded map')); grow.appendChild(glab);
        var gnote = mk('span', 'ov-sat-note', self._gridLineText()); gnote.id = 'ovGridNote'; grow.appendChild(gnote);
        gbox.setAttribute('aria-describedby', 'ovGridNote');
        gbox.addEventListener('change', function () { self.setSatGrid(!!gbox.checked); });
        body.appendChild(grow); self.ui.gridBox = gbox; self.ui.gridNote = gnote;
        // the age warning right under its box (G55 B-3: below the credit it fell out of a 360-px phone's details box);
        // its hour count kept current in place by _gridLineSync (G55 B-1)
        self.ui.gridWarn = null;
        if (self._gridWarnShown) { var gw = mk('div', 'ov-warn ov-grid-warn', self._gridWarnText()); body.appendChild(gw); self.ui.gridWarn = gw; }
      }
      var line = mk('div', 'ov-meta ov-sat-line', self._satLineText()); body.appendChild(line); self.ui.satLine = line;
      body.appendChild(mk('div', 'ov-meta ov-sat-credit', 'Generated using E.U. Copernicus Marine Service Information (doi:10.48670/moi-00179' + (self.satKind === 'swell' ? ', doi:10.48670/moi-00178' : '') + (self.satGrid && self.grid ? ', doi:' + GRID_DOI : '') + ').'));
      // the key last: on the smallest phones the details box cuts its bottom line, and the licence credit above must stay
      // in view (G55 fix check: with the grid warning under the box the credit fell 10 px below a 360 x 640 sheet)
      if (self.satKind !== 'swell') body.appendChild(mk('div', 'ov-meta ov-sat-key', SAT_KEY));   // the hollow dots explained (G52 B-2)
      if (self._satStaleShown) {
        var h = Math.round((Date.now() - newest) / 3.6e6);
        body.appendChild(mk('div', 'ov-warn', 'The newest satellite reading is ' + h + ' h old.'));
      }
    });
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
  var FOCUS_PARTS = ['ov-toggle', 'ov-model-sel', 'ov-radar-loop', 'ov-ribbon', 'ov-speed-btn', 'ov-speed-opt', 'ov-speed-menu', 'ov-timeline', 'ov-play', 'ov-retry', 'ov-update', 'ov-seg-btn', 'ov-sat-box', 'ov-details'];
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
    var el = (cls === 'ov-seg-btn' && h.querySelector('.ov-seg-btn.ov-seg-on')) || h.querySelector('.' + cls) || h.querySelector('.ov-play') || h.querySelector('.ov-toggle') || h.querySelector('.ov-retry');
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
    if (!ui || !ui.title || !this.manifest || !this.layer || ui.obs) return;   // no panel (the error panel's ?ovdebug=1 readout alone: re-check R-5); the observation's has no live parts
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
    _internals: { coastNote: coastNote, COAST_FIRST_MS: COAST_FIRST_MS, particleOpacityOf: particleOpacityOf, contourTile: contourTile, smoothBlock: smoothBlock, jumpAt: jumpAt, CONTOURS: CONTOURS, CONTOUR_JUMP: CONTOUR_JUMP,
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
      SST_TIER_ZOOMS: SST_TIER_ZOOMS, SST_ATTRIBUTION: SST_ATTRIBUTION, SST_HEAD: SST_HEAD, SST_STALE_DAYS: SST_STALE_DAYS, sstDayText: sstDayText, sstAgeDays: sstAgeDays, buildLegend: buildLegend, SST_PREFIX: SST_PREFIX, SST_PAD_PX: SST_PAD_PX, MAX_SST_INFLIGHT: MAX_SST_INFLIGHT, MAX_SST_CHUNK_BYTES: MAX_SST_CHUNK_BYTES, cellsForTile: cellsForTile,
      FlowAnimator: FlowAnimator, sampleRow: sampleRow, viewOf: viewOf, placeCanvas: placeCanvas,
      LakeStore: LakeStore, lakeStore: lakeStore, validLakeManifest: validLakeManifest, validLakePointer: validLakePointer, validLakeGridDef: validLakeGridDef, validateLakeGrid: validateLakeGrid,
      lakeFieldOk: lakeFieldOk, lakeFieldMatches: lakeFieldMatches, sampleLake: sampleLake, lakeBoxOf: lakeBoxOf, lakeBoxHits: lakeBoxHits, lakeNearView: lakeNearView, LAKE_NEAR_SCREENS: LAKE_NEAR_SCREENS, lakeVec: lakeVec,
      COAST_TIER0: COAST_TIER0, LAKES_TIER0: LAKES_TIER0, LAKEOUT_TIER0: LAKEOUT_TIER0, LAKEOUT_PREFIX: LAKEOUT_PREFIX, LAKE_OUTLINE_MIN_ZOOM: LAKE_OUTLINE_MIN_ZOOM, LAKE_PREFIX: LAKE_PREFIX, LAKE_ATTRIBUTION: LAKE_ATTRIBUTION, LAKE_UNDER: LAKE_UNDER, LAKE_DIR_FIELD: LAKE_DIR_FIELD, LAKE_CALM_M: LAKE_CALM_M, LAKE_BOX: LAKE_BOX, lakeFull: lakeFull,
      IsobarLayer: IsobarLayer, isoBlur: isoBlur, PRES_WAIT_MS: PRES_WAIT_MS, capWait: capWait, PRES_FIELD: PRES_FIELD, PRES_UNDER: PRES_UNDER, presFieldOk: presFieldOk, isoInterval: isoInterval, isoStride: isoStride, isoValues: isoValues, traceIsolines: traceIsolines,
      projectIsolines: projectIsolines, placeLabels: placeLabels, drawIsobars: drawIsobars, findCentres: findCentres, projectCentres: projectCentres, ISO_PATCH_MAX: ISO_PATCH_MAX, ISO_INTERVAL: ISO_INTERVAL, ISO_STYLE: ISO_STYLE,
      ISO_WORLD_DEG: ISO_WORLD_DEG, ISO_NODE_PX: ISO_NODE_PX, ISO_BLUR: ISO_BLUR, ISO_HL_PX: ISO_HL_PX, ISO_HL_EDGE_PX: ISO_HL_EDGE_PX, prominenceMap: prominenceMap, blockExtremes: blockExtremes, ISO_HL_PROM_CODES: ISO_HL_PROM_CODES, ISO_HL_PROM_CAP: ISO_HL_PROM_CAP, ISO_CLAMP_PATCH_MAX: ISO_CLAMP_PATCH_MAX, ISO_CLAMP_LAT: ISO_CLAMP_LAT, ISO_TERRAIN_M: ISO_TERRAIN_M, TERRAIN_M_PER_CODE: TERRAIN_M_PER_CODE, ISO_STRONG_LOW_HPA: ISO_STRONG_LOW_HPA, ISO_TERRAIN_CAP_M: ISO_TERRAIN_CAP_M, ISO_STRONG_NODES_MAX: ISO_STRONG_NODES_MAX, strongLowSearch: strongLowSearch, ISO_PROM_NODES_MAX: ISO_PROM_NODES_MAX, ISO_HL_CLEAR_PX: ISO_HL_CLEAR_PX, loadTerrain: loadTerrain, markBox: markBox, ISO_LABEL_GAP_PX: ISO_LABEL_GAP_PX, ISO_LABEL_MIN_Z: ISO_LABEL_MIN_Z,
      ISO_LABEL_MIN_PX: ISO_LABEL_MIN_PX, ISO_LABEL_AT: ISO_LABEL_AT, ISO_LABEL_EDGE_PX: ISO_LABEL_EDGE_PX,
      vectorNodes: vectorNodes, flowField: flowField, FLOW_SPEED: FLOW_SPEED, dirFieldOk: dirFieldOk, dirRes: dirRes, DIR_FIELDS: DIR_FIELDS, PARTICLE_LIFE_MS: PARTICLE_LIFE_MS,
      latOfWorldY: latOfWorldY, lngOfWorldX: lngOfWorldX, PARTICLE_PX_PER_S: PARTICLE_PX_PER_S, PARTICLE_MIN: PARTICLE_MIN, PARTICLE_MAX: PARTICLE_MAX,
      ANIM_BUDGET_MS: ANIM_BUDGET_MS, TRAIL_POINTS: TRAIL_POINTS, TRAIL_EVERY_MS: TRAIL_EVERY_MS, dirGridsOk: dirGridsOk,
      frameAtHour: frameAtHour, localClock: localClock, runTimes: runTimes, TimelineState: TimelineState,
      MODEL_DEFS: MODEL_DEFS, DEFAULT_MODEL: DEFAULT_MODEL, modelList: modelList, modelOf: modelOf, hasModel: hasModel, STALE_AFTER_S: STALE_AFTER_S, CYCLE_MS: CYCLE_MS,
      LOAD_STALL_MS: LOAD_STALL_MS, SST_COARSE_BELOW: SST_COARSE_BELOW, readBody: readBody,
      SatStore: SatStore, satStore: satStore, SatIndex: SatIndex, SatLayer: SatLayer, decodeSatChunk: decodeSatChunk, validSatPointer: validSatPointer, satNewestMs: satNewestMs,
      inflateBytes: inflateBytes, satSplitColumns: satSplitColumns, satColumnsEqual: satColumnsEqual, satStride: satStride, satSegmentsOk: satSegmentsOk, satLowerBound: satLowerBound,
      composeDots: composeDots, satTileReadings: satTileReadings, satStrideFor: satStrideFor, satRadius: satRadius, satAlpha: satAlpha, satColourIndex: satColourIndex, satArrowLength: satArrowLength,
      bathyDepths: bathyDepths, BATHY_DEPTH: BATHY_DEPTH, bathyCode: bathyCode, validBathyRegions: validBathyRegions, validBathyIndex: validBathyIndex,
      parseBathyIdx: parseBathyIdx, BathyStore: BathyStore, bathyStore: bathyStore, BathyLayer: BathyLayer, bathyAxes: bathyAxes, bathyShade: bathyShade,
      bathyPos: bathyPos, bathyPieces: bathyPieces, bathySample: bathySample, bathyLevels: bathyLevels, bathyLevel: bathyLevel, bathySmooth: bathySmooth,
      bathyCredit: bathyCredit, bathyShort: bathyShort, bathyLabel: bathyLabel, bathyNum: bathyNum, bathyOpacityOf: bathyOpacityOf, BATHY_FDEF: BATHY_FDEF, OBS_LOAD: OBS_LOAD,
      BATHY_APRON: BATHY_APRON, bathyNeighbours: bathyNeighbours, BATHY_GEBCO_EX: BATHY_GEBCO_EX, BATHY_SHADE_FINE: BATHY_SHADE_FINE, BATHY_SHADE_COARSE: BATHY_SHADE_COARSE, BATHY_DONE_MS: BATHY_DONE_MS, bathyReady: bathyReady, bathyForget: bathyForget, bathyDeep: bathyDeep, bathyWeight: bathyWeight, BATHY_DEEP: BATHY_DEEP,
      radarPic: radarPic, RadarStore: RadarStore, radarStore: radarStore, RadarLayer: RadarLayer, RadarPlayer: RadarPlayer, validRadarPointer: validRadarPointer, radarLoopFrames: radarLoopFrames, radarCellTable: radarCellTable,
      radarBit: radarBit, radarDbz: radarDbz, radarRainWord: radarRainWord, radarViewOf: radarViewOf, radarHits: radarHits, radarWantTier: radarWantTier, radarSample: radarSample, radarLut: radarLut, radarTileGeom: radarTileGeom, radarCellRect: radarCellRect,
      radarSlotMs: radarSlotMs, radarAge: radarAge, RADAR_ATTRIBUTION: RADAR_ATTRIBUTION, RADAR_FDEF: RADAR_FDEF, RADAR_TINT: RADAR_TINT, radarHead: radarHead, radarCredit: radarCredit, radarOpacityOf: radarOpacityOf, RADAR_OPACITY_KEY: RADAR_OPACITY_KEY, RADAR_ALPHA: RADAR_ALPHA, RADAR_JMA8: RADAR_JMA8, RADAR_JMA8_SNAP: RADAR_JMA8_SNAP, radarClassOf: radarClassOf, radarClassText: radarClassText, RADAR_TYPE_CODES: RADAR_TYPE_CODES, rampAt: rampAt, RADAR_TYPE_BASE: RADAR_TYPE_BASE, RADAR_RC: RADAR_RC, RADAR_T: RADAR_T, radarTypeCodesOk: radarTypeCodesOk, radarTypedDef: radarTypedDef, radarSnowWord: radarSnowWord, radarMixWord: radarMixWord, radarTypeBar: radarTypeBar, RADAR_SETTLE_MS: RADAR_SETTLE_MS, RADAR_HOLD_STEPS: RADAR_HOLD_STEPS, RADAR_MAX_BYTES: RADAR_MAX_BYTES, RADAR_MAX_BYTES_SMALL: RADAR_MAX_BYTES_SMALL, RADAR_INFLIGHT: RADAR_INFLIGHT, RADAR_ORDER: RADAR_ORDER, RADAR_PPD: RADAR_PPD, RADAR_CELL: RADAR_CELL,
      viewRangeOf: viewRangeOf, OBS_DEFS: OBS_DEFS, hasObs: hasObs, SAT_PREFIX: SAT_PREFIX, SWELL_PREFIX: SWELL_PREFIX, SAT_WINDOW_MS: SAT_WINDOW_MS, 
      SAT_RECHECK_MS: SAT_RECHECK_MS, SAT_STALE_H: SAT_STALE_H, SWELL_STALE_H: SWELL_STALE_H, SAT_FDEF: SAT_FDEF, SAT_HEAD: SAT_HEAD, SAT_ATTRIBUTION: SAT_ATTRIBUTION, 
      SAT_FADE: SAT_FADE, MAX_SAT_CHUNKS: MAX_SAT_CHUNKS, MAX_SAT_CHUNK_BYTES: MAX_SAT_CHUNK_BYTES, MAX_SAT_SAMPLES: MAX_SAT_SAMPLES, MAX_SAT_INFLIGHT: MAX_SAT_INFLIGHT, SAT_BUCKET_DEG: SAT_BUCKET_DEG,
      SAT_COLUMNS: SAT_COLUMNS, SAT_MAGIC: SAT_MAGIC, SAT_FORMAT: SAT_FORMAT, SAT_KEY: SAT_KEY, SAT_TAP_MS: SAT_TAP_MS, FOCUS_PARTS: FOCUS_PARTS,
      GridStore: GridStore, gridStore: gridStore, validGridPointer: validGridPointer, validGridManifest: validGridManifest, GRID_PREFIX: GRID_PREFIX, GRID_FORMAT: GRID_FORMAT,
      GRID_DOI: GRID_DOI, GRID_WARN_H: GRID_WARN_H, GRID_HIDE_H: GRID_HIDE_H, GRID_EXPECT: GRID_EXPECT, GRID_HALF: GRID_HALF, satLut: satLut, SAT_FLAG_NEAR_LAND: SAT_FLAG_NEAR_LAND, SAT_FLAG_MODEL_EDGE: SAT_FLAG_MODEL_EDGE, SAT_EPOCH_MS: SAT_EPOCH_MS,
      SPEED_ANIMALS: SPEED_ANIMALS, animalOf: animalOf, speedOf: speedOf, assetUrl: assetUrl, speedSelector: speedSelector,
      ribbonFormatter: ribbonFormatter, localMidnightBefore: localMidnightBefore, ribbonScale: ribbonScale, ribbonLayout: ribbonLayout,
      RibbonState: RibbonState, RIBBON_MIN_TICK_GAP: RIBBON_MIN_TICK_GAP, RIBBON_TAP_PX: RIBBON_TAP_PX, RIBBON_AXIS_PX: RIBBON_AXIS_PX,
      RIBBON_DAY_MIN_PX: RIBBON_DAY_MIN_PX, RIBBON_FALLBACK_W: RIBBON_FALLBACK_W }
  };
})();
