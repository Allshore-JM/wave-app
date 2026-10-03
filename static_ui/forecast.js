/* Allshore Surf forecast window (plan section 25). Loaded on every page; touches no DOM until init()
 * is called, so tests/ui/forecast.test.js can evaluate it in Node and drive each piece with fakes.
 *
 * Pieces: the pure state helpers; createLoader (ONE entry point for every change: URL sync, a small
 * client cache, request sequencing with abort, stale-while-loading); createForecastGraphs (the three
 * Chart.js charts, rebuilt per forecast: instances destroyed, canvas listeners aborted, the range and
 * night shading reapplied); FloatingWindow (drag, corner resize, minimise / maximise, geometry kept in
 * the tab); the settings panel; init() wires them to the page's ids.
 *
 * Page contract (templates/index.html): #forecastWin #fwHeader #fwTitle #fwCycle #fwBusy #fwMin #fwMax
 * #viewBar[data-view] #modelBar[data-model] #rangeBar[data-days] #fwBody #fwError #forecastMeta
 * #forecastTable (#forecastLoading inside it until the first forecast lands) #graphs (.chart-box > canvas
 * #heightChart #periodChart #directionChart) #fwResize; #settingsBtn #settingsPanel #tz #unit #station
 * #stationTrigger #stationCurrent (the favourites picker is the window's heading; #fwTitle is its field);
 * #liveBuoyPanel #lwHeader #lwMin #lwClose #lwResize (createLiveWindow). The page dispatches 'allshore:station' {sid, source} on a
 * marker click ('map') or a favourites pick ('picker'). After each forecast this module dispatches 'allshore:forecast'
 * {station, tz, model, view, ok, point}. Forecast points (plan section 31): pointId / parsePointId (the server's id rule)
 * and createPointStore (the visitor's own points, localStorage 'allshore.points.v1').
 */
(function () {
  'use strict';

  var SETTINGS_KEY = 'allshore.settings.v1';   // localStorage {tz, unit}
  var WINDOW_KEY = 'allshore.forecastWin.v1';  // sessionStorage {x, y, w, h, mode, prev}
  var LIVE_WINDOW_KEY = 'allshore.liveWin.v1'; // the live-buoy window's (the same shape)
  var RANGE_KEY = 'chartRange';                // sessionStorage 'full' | '7' | '3' (unchanged from the old page)
  var POINTS_KEY = 'allshore.points.v1';       // localStorage [{id, lat, lon, name}]: the visitor's forecast points
  var POINTS_MAX = 50, POINT_NAME_MAX = 40;
  var STALE_H = 13;                            // a point's run older than this: a NOAA cycle was missed (runs go live ~5.5 h after
                                               // the cycle and are replaced 6 h later, so a live run is 5.5-11.5 h old)
  function isUnit(u) { return u === 'US' || u === 'Metric'; }   // (an object lookup matched 'constructor' and the like)
  var CACHE_MAX = 16, CACHE_TTL_MS = 10 * 60 * 1000;
  var GAP_TTL_MS = 60 * 1000;                     // a forecast with wind gaps (a transient NOAA miss): kept a minute only
  var MIN_SIZE = { w: 360, h: 220 };
  var PHONE_QUERY = '(max-width: 500px), (max-height: 500px)';   // "phone mode": a bar + full screen, no drag / resize
  var API = '/api/forecast';
  var TABLE_CHROME = 20 + 2 + 10;                                  // #fwBody padding + the window border + its 5 px side margins (+ the scrollbar, measured)
  var DEFAULT_W = 1180;                                             // .forecast-win's CSS width: min(1180px, 100vw - 32px)
  var LABEL_PX = 44;                                                // room for one flat date label on the charts

  // ---- pure state helpers ----
  // The state a page starts from: the URL wins, then the viewer's saved settings (tz, unit only),
  // then what the server rendered. tz '' means Buoy Local; an explicit empty tz in the URL counts.
  function resolveInitialState(search, stored, server) {
    var p = new URLSearchParams(search || ''), st = stored || {}, sv = server || {};
    var unit = p.has('unit') ? p.get('unit') : (typeof st.unit === 'string' ? st.unit : sv.unit);
    return {
      station: (p.get('station') || '').trim() || sv.station || '51201',   // a link with white space after the id (G22 A-17)
      tz: p.has('tz') ? p.get('tz') : (typeof st.tz === 'string' ? st.tz : (sv.tz || '')),
      unit: isUnit(unit) ? unit : 'US',
      model: (p.get('model') || sv.model || 'GFS').toUpperCase() === 'SWAN' ? 'SWAN' : 'GFS',
      view: (p.get('view') || sv.view) === 'Graph' ? 'Graph' : 'Table'
    };
  }
  // The /api/forecast query for a state (view is client-only; the window takes the compact table).
  function queryFor(s) {
    return new URLSearchParams({ station: s.station, tz: s.tz || '', unit: s.unit, model: s.model, compact: '1' }).toString();
  }
  // The address bar for a state: the station always; tz and unit whenever they differ from what a reload
  // would assume (the viewer's saved settings, else Buoy Local / US: resolveInitialState's rule, so the
  // address always reloads to the view on screen); model and view when not the default.
  function urlFor(s, saved) {
    var st = saved || {}, tz0 = typeof st.tz === 'string' ? st.tz : '', unit0 = isUnit(st.unit) ? st.unit : 'US';
    var p = new URLSearchParams({ station: s.station });
    if ((s.tz || '') !== tz0) p.set('tz', s.tz || '');
    if (s.unit !== unit0) p.set('unit', s.unit);
    if (s.model !== 'GFS') p.set('model', s.model);
    if (s.view !== 'Table') p.set('view', s.view);
    return '?' + p.toString();
  }
  // The client cache key: one forecast per station, time zone, unit and model.
  function keyOf(s) { return [s.station, s.tz || '', s.unit, s.model].join('|'); }
  // A window geometry kept inside a viewport of vw x vh below a top edge: the size shrinks to fit (never
  // below min), the position is pulled back so the whole window (and so its header) stays on screen.
  // maxW (the table's width in Table view) is applied by CSS max-width only: the viewer's own width is kept, and x is
  // clamped with the width actually on screen.
  function clampGeometry(g, vw, vh, top, min, maxW) {
    var m = min || MIN_SIZE, t = top || 0, pad = 8;
    var w = Math.max(Math.min(m.w, vw - 2 * pad), Math.min(g.w, vw - 2 * pad));
    var shown = maxW > 0 ? Math.max(Math.min(w, maxW), Math.min(m.w, vw - 2 * pad)) : w;
    var h = Math.max(Math.min(m.h, vh - t - 2 * pad), Math.min(g.h, vh - t - 2 * pad));
    var x = Math.min(Math.max(g.x, pad), Math.max(pad, vw - pad - shown));
    var y = Math.min(Math.max(g.y, t + pad), Math.max(t + pad, vh - pad - h));
    return { x: x, y: y, w: w, h: h };
  }
  // A resize from one edge or corner ('n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw') of the window box r by (dx, dy):
  // the opposite edges stay where they are; the moving edge stops at the minimum size, at the viewport (8 px pad,
  // below the top edge) and, horizontally, at the table width maxW (Table view). A horizontal resize starts from the
  // width on screen (min(w, maxW)); a vertical one keeps the viewer's own width. (plan section 27)
  function resizeGeometry(r, edge, dx, dy, vw, vh, top, min, maxW) {
    var m = min || MIN_SIZE, t = top || 0, pad = 8;
    var minW = Math.min(m.w, vw - 2 * pad), minH = Math.min(m.h, vh - t - 2 * pad);
    var capW = maxW > 0 ? Math.max(maxW, minW) : Infinity;
    var shown = Math.min(r.w, capW), x = r.x, y = r.y, w = r.w, h = r.h;
    if (edge.indexOf('e') >= 0) { w = Math.max(minW, Math.min(shown + dx, capW, vw - pad - r.x)); }
    if (edge.indexOf('w') >= 0) {
      var right = r.x + shown;
      x = Math.max(pad, right - capW, Math.min(r.x + dx, right - minW)); w = right - x;
    }
    if (edge.indexOf('s') >= 0) { h = Math.max(minH, Math.min(r.h + dy, vh - pad - r.y)); }
    if (edge.indexOf('n') >= 0) {
      var bottom = r.y + r.h;
      y = Math.max(t + pad, Math.min(r.y + dy, bottom - minH)); h = bottom - y;
    }
    return { x: x, y: y, w: w, h: h };
  }
  function readJson(storage, key) {
    try { var o = JSON.parse(storage.getItem(key) || '{}'); return o && typeof o === 'object' && !Array.isArray(o) ? o : {}; } catch (e) { return {}; }
  }
  function writeJson(storage, key, value) {
    try { storage.setItem(key, JSON.stringify(value)); return true; } catch (e) { return false; }
  }
  // ---- forecast points (plan section 31) ----
  // The id of a point: latitude and longitude in thousandths of a degree, one spelling per point (point_forecast.py's
  // point_id, which the server checks: no leading zeros, zero is N / E, the antimeridian is 180 W). null off the map.
  function pointId(lat, lon) {
    lat = +lat; lon = +lon;
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
    var latM = Math.floor(Math.abs(lat) * 1000 + 0.5);
    if (latM > 90000) return null;
    if (!(lon >= -180 && lon < 180)) lon = (((lon + 180) % 360) + 360) % 360 - 180;
    var lonM = Math.floor(Math.abs(lon) * 1000 + 0.5);
    var ns = lat < 0 && latM ? 'S' : 'N', ew = (lon < 0 && lonM) || lonM === 180000 ? 'W' : 'E';
    return 'pt_' + latM + ns + '_' + lonM + ew;
  }
  var POINT_RE = /^pt_(\d{1,5})([NS])_(\d{1,6})([EW])$/;
  function isPointId(s) { return typeof s === 'string' && s.indexOf('pt_') === 0; }
  // {lat, lon} of an id, or null for anything pointId would not have written.
  function parsePointId(s) {
    var m = POINT_RE.exec(typeof s === 'string' ? s : '');
    if (!m) return null;
    var latM = +m[1], lonM = +m[3];
    if (latM > 90000 || lonM > 180000) return null;
    var lat = latM / 1000 * (m[2] === 'S' ? -1 : 1), lon = lonM / 1000 * (m[4] === 'W' ? -1 : 1);
    return pointId(lat, lon) === s ? { lat: lat, lon: lon } : null;
  }
  function fmtPoint(lat, lon) {
    return Math.abs(lat).toFixed(3) + (lat >= 0 ? 'N' : 'S') + ' ' + Math.abs(lon).toFixed(3) + (lon >= 0 ? 'E' : 'W');
  }
  // A name as the visitor typed it, made safe to keep: no control characters (bidi controls included: a name could
  // turn the coordinates round, G22 A-14), single spaces, at most 40 characters counted as the visitor sees them: a
  // family emoji, a flag or a skin tone is one character and is never cut apart (G22 B-11, R-A12).
  function graphemes(t) {
    if (typeof Intl !== 'undefined' && typeof Intl.Segmenter === 'function') {
      try { return Array.from(new Intl.Segmenter(undefined, { granularity: 'grapheme' }).segment(t), function (g) { return g.segment; }); } catch (e) {}
    }
    return Array.from(t);
  }
  function cleanName(s) {
    if (typeof s !== 'string' && typeof s !== 'number') s = '';                     // a stored object cannot stop the page (G22 R-A13)
    var t = String(s).replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029\u200e\u200f\u202a-\u202e\u2066-\u2069]/g, ' ').replace(/\s+/g, ' ').trim();
    var g = graphemes(t);
    if (g.length <= POINT_NAME_MAX) return t;
    return g.slice(0, POINT_NAME_MAX).join('').replace(/[\u200d\ufe0f]+$/, '').trim();   // without a Segmenter: no dangling joiner
  }
  // The text a point goes by: its coordinates always (owner: "the location should show the gps coordinates"), its
  // name in front when it has one ("Pipeline — 21.667N 158.054W", like "51201 — Waimea Bay, HI"). The name is
  // isolated (U+2068 ... U+2069): a right-to-left name cannot pull the coordinates into it (G22 B-10).
  function pointLabel(p) { var c = fmtPoint(p.lat, p.lon); return p.name ? '\u2068' + p.name + '\u2069 — ' + c : c; }
  // A label into an element. A point's coordinates are never what gets cut when room runs out (G22 B-4): they go
  // in a span that does not shrink, the name in front of them in one that does (CSS .lbl-split).
  var LABEL_TAIL = /^([\s\S]*) — (\d{1,2}\.\d{3}[NS] \d{1,3}\.\d{3}[EW])$/, COORDS_ONLY = /^\d{1,2}\.\d{3}[NS] \d{1,3}\.\d{3}[EW]$/;
  function writeLabel(el, label) {
    if (!el) return;
    label = String(label == null ? '' : label);
    if (el.firstChild && typeof el.removeChild === 'function') while (el.firstChild) el.removeChild(el.firstChild);
    var doc = el.ownerDocument, m = LABEL_TAIL.exec(label), only = COORDS_ONLY.test(label);
    if (!doc || typeof doc.createElement !== 'function') { el.textContent = label; return; }
    if (el.classList) el.classList.toggle('lbl-split', !!(m || only));
    if (!m && !only) { el.appendChild(doc.createTextNode(label)); return; }
    if (m) {
      var name = doc.createElement('span'); name.className = 'lbl-name'; name.setAttribute('dir', 'auto');
      name.textContent = m[1].replace(/^\u2068|\u2069$/g, ''); el.appendChild(name);
    }
    var tail = doc.createElement('span'); tail.className = 'lbl-coord'; tail.textContent = m ? '\u00a0— ' + m[2] : label; el.appendChild(tail);
  }
  // A time zone as a visitor reads it: the nautical "Etc/GMT+11" means UTC-11 (its sign is POSIX's, backwards;
  // G22 B-2), so it is shown as "UTC−11".
  function zoneLabel(name) {
    var z = String(name == null ? '' : name), m = /^Etc\/GMT([+-])(\d{1,2})$/.exec(z);
    if (m) return +m[2] ? 'UTC' + (m[1] === '+' ? '\u2212' : '+') + (+m[2]) : 'UTC';
    return /^Etc\/(GMT|UTC|UCT|Universal|Zulu|Greenwich)(0|[+-]0)?$/.test(z) ? 'UTC' : z;
  }
  // The visitor's points, in this browser only (accounts may come later: the record is versioned and self-contained).
  // Read back strictly: an entry whose id is not a point id is dropped; the coordinates come from the id.
  function readPoints(storage) {
    var raw;
    try { raw = JSON.parse(storage.getItem(POINTS_KEY) || '[]'); } catch (e) { return []; }
    if (!Array.isArray(raw)) return [];
    var seen = {}, out = [];
    raw.forEach(function (r) {
      var c = r && typeof r === 'object' ? parsePointId(r.id) : null;
      if (!c || seen[r.id]) return;
      seen[r.id] = true;
      out.push({ id: r.id, lat: c.lat, lon: c.lon, name: typeof r.name === 'string' ? cleanName(r.name) : '' });
    });
    return out.slice(0, POINTS_MAX);
  }
  function createPointStore(storage) {
    function write(list) { try { storage.setItem(POINTS_KEY, JSON.stringify(list)); return true; } catch (e) { return false; } }
    function list() { return readPoints(storage); }
    function get(id) { var l = list(); for (var i = 0; i < l.length; i++) if (l[i].id === id) return l[i]; return null; }
    return {
      list: list, get: get,
      has: function (id) { return !!get(id); },
      // 'added' (newest first), 'exists', 'full' (POINTS_MAX), 'invalid', or 'unsaved' (the storage refused it)
      add: function (id, name) {
        var c = parsePointId(id); if (!c) return 'invalid';
        var l = list();
        if (l.some(function (p) { return p.id === id; })) return 'exists';
        if (l.length >= POINTS_MAX) return 'full';
        l.unshift({ id: id, lat: c.lat, lon: c.lon, name: cleanName(name) });
        return write(l) ? 'added' : 'unsaved';
      },
      remove: function (id) { var l = list(), n = l.length; l = l.filter(function (p) { return p.id !== id; }); return l.length !== n && write(l); },
      rename: function (id, name) {
        var l = list(), hit = false;
        l.forEach(function (p) { if (p.id === id) { p.name = cleanName(name); hit = true; } });
        return hit && write(l);
      },
      label: function (id) { var p = get(id) || parsePointId(id); return p ? pointLabel(p) : null; }
    };
  }

  // "GFS · run 20260926 12 UTC" / "SWAN · updated 20260925 23 UTC" for the window's header.
  function shortCycle(model, header) {
    var c = header && header.cycle ? String(header.cycle) : '';
    if (!c) return model || '';
    if (model === 'SWAN') { var m = /updated\s+(.*)$/i.exec(c); return 'SWAN · updated ' + (m ? m[1] : c); }
    return (model || 'GFS') + ' · run ' + c;
  }
  // The graph's label strings -> Date (the old page's rule: "8/30/25 6:00 AM", else the browser's parser).
  var MONTHS = { january: 0, february: 1, march: 2, april: 3, may: 4, june: 5, july: 6, august: 7, september: 8, october: 9, november: 10, december: 11 };
  function hm(h, mi, ap) { h = +h; if (ap === 'pm' && h < 12) h += 12; if (ap === 'am' && h === 12) h = 0; return [h, +mi]; }
  function parseLabel(lbl) {
    var s = lbl || '', m = /^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2})\s*([APap][Mm])$/.exec(s), t;
    if (m) { t = hm(m[4], m[5], m[6].toLowerCase()); return new Date(m[3].length === 2 ? 2000 + +m[3] : +m[3], +m[1] - 1, +m[2], t[0], t[1], 0); }
    // the server's own form, "Saturday, September 26, 2026 2:00 PM" (the browser's Date parser is not trusted with it)
    m = /^[A-Za-z]+,\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})\s+(\d{1,2}):(\d{2})\s*([APap][Mm])$/.exec(s);
    if (m && MONTHS.hasOwnProperty(m[1].toLowerCase())) { t = hm(m[4], m[5], m[6].toLowerCase()); return new Date(+m[3], MONTHS[m[1].toLowerCase()], +m[2], t[0], t[1], 0); }
    var d = new Date(s);
    return isNaN(d) ? new Date() : d;
  }
  // The x window for a range of days from the START of the series. With the rows' times (parsed labels), the last
  // row less than `days` x 24 h after the first: a forecast point's rows are hourly to +120 h, then 3-hourly (plan
  // section 31). Without them, one row an hour (the old page's rule).
  function rangeWindow(n, days, times) {
    var end = n - 1;
    if (Number.isFinite(days) && days > 0) {
      var t0 = times && times.length === n && times[0] ? +times[0] : NaN;
      if (Number.isFinite(t0)) {
        var lim = t0 + days * 24 * 3600 * 1000;
        end = 0;
        for (var i = 1; i < n; i++) { if (+times[i] < lim) end = i; else break; }
      } else end = Math.min(n - 1, Math.round(days * 24) - 1);
    }
    return { min: 0, max: Math.max(0, end) };
  }

  // ---- the loader ----
  // deps: fetch(url, {signal}) -> Response-like {ok, status, json()}; now(); replaceState(url);
  //       swanStations (array); ui.apply(d, state), ui.busy(bool), ui.error(message | null, retry | null).
  function ttlOf(d) { return d && d.wind_complete === false ? GAP_TTL_MS : CACHE_TTL_MS; }
  function createLoader(deps, state) {
    var cache = new Map(), seq = 0, ctrl = null;
    function trim() { while (cache.size > CACHE_MAX) { var k = cache.keys().next().value; cache.delete(k); } }
    function normalise(s) {                                                    // never a SWAN request off the SWAN stations
      if (s.model === 'SWAN' && deps.swanStations && deps.swanStations.indexOf(s.station) < 0) s.model = 'GFS';
      return s;
    }
    function sync() { if (deps.replaceState) deps.replaceState(urlFor(state, deps.saved ? deps.saved() : null)); }
    function apply(d) {
      if (d && typeof d.model === 'string') state.model = d.model.toUpperCase() === 'SWAN' ? 'SWAN' : 'GFS';   // the server's echo is authoritative
      sync();
      deps.ui.apply(d, state, function () { return load({}); });
    }
    function load(next) {
      Object.assign(state, next || {});
      normalise(state);
      sync();
      var key = keyOf(state), hit = cache.get(key), now = deps.now();
      if (hit && now - hit.ts < (hit.ttl || CACHE_TTL_MS)) { if (ctrl) { ctrl.abort(); ctrl = null; } seq++; deps.ui.busy(false); deps.ui.error(null, null); apply(hit.d); return Promise.resolve(hit.d); }
      var my = ++seq;
      if (ctrl) ctrl.abort();
      var c = ctrl = new AbortController();
      deps.ui.busy(true); deps.ui.error(null, null);
      var q = queryFor(state), early = deps.takeEarly ? deps.takeEarly(q) : null;   // the page's <head> may have started this very request
      return (early || deps.fetch(API + '?' + q, { signal: c.signal })).then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      }).then(function (d) {
        if (my !== seq) return null;                                           // superseded: no DOM writes
        if (!d || typeof d !== 'object') throw new Error('bad forecast');
        if (!(d.error && !d.table_html)) { cache.set(key, { d: d, ts: deps.now(), ttl: ttlOf(d) }); trim(); }   // a failed build is not kept (the server does not keep it either)
        deps.ui.busy(false);
        apply(d);
        return d;
      }, function (err) {
        if (my !== seq || (err && err.name === 'AbortError')) return null;
        deps.ui.busy(false);
        if (deps.ui.clear) deps.ui.clear();                                    // never the last station's table under this title (G22 B-7)
        deps.ui.error('Could not load the forecast.', function () { return load({}); });
        return null;
      });
    }
    // A forecast fetched and kept for a station WITHOUT showing it (the map tool asks first, opens after: G22).
    // -> the payload, or a rejection for a network failure. A refusal is returned, not kept.
    function prefetch(next) {
      var s = normalise(Object.assign({}, state, next || {})), key = keyOf(s), hit = cache.get(key), now = deps.now();
      if (hit && now - hit.ts < (hit.ttl || CACHE_TTL_MS)) return Promise.resolve(hit.d);
      return deps.fetch(API + '?' + queryFor(s), {}).then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      }).then(function (d) {
        if (!d || typeof d !== 'object') throw new Error('bad forecast');
        if (!(d.error && !d.table_html)) { cache.set(key, { d: d, ts: deps.now(), ttl: ttlOf(d) }); trim(); }
        return d;
      });
    }
    // A forecast the page already rendered (render=full): cached and shown, no fetch.
    function seed(d) { if (!(d.error && !d.table_html)) { cache.set(keyOf(state), { d: d, ts: deps.now(), ttl: ttlOf(d) }); trim(); } apply(d); }
    return { load: load, seed: seed, prefetch: prefetch, state: state, cache: cache, sync: sync };
  }

  // ---- the graphs ----
  // deps: host (#graphs), boxes (3 elements holding the canvases), canvases (3), rangeBar (element with
  // [data-days] buttons) or null, loadChartJs() -> Promise, getChart() -> the Chart constructor,
  // storage (sessionStorage-like), bodyHeight() -> px, visible() -> bool.
  var COLORS = { s1: '#C00000', s2: '#ED7D31', s3: '#FFC000', s4: '#00B050', s5: '#00B0F0', s6: '#92D050', combined: '#7030A0' };
  function finiteVals(arr) { return (arr || []).filter(function (v) { return v !== null && v !== undefined && Number.isFinite(v); }); }
  function maxAcross(arrays) { return arrays.reduce(function (m, a) { return Math.max.apply(null, [m].concat(finiteVals(a), [-Infinity])); }, -Infinity); }
  function minAcross(arrays) { return arrays.reduce(function (m, a) { return Math.min.apply(null, [m].concat(finiteVals(a), [Infinity])); }, Infinity); }
  function padMax(v, p) { return Number.isFinite(v) ? v * (1 + (p || 0.1)) : v; }
  function niceCeil(v, step) { return Math.ceil(v / step) * step; }
  function heightStep(m) { if (m <= 1) return 0.1; if (m <= 2) return 0.2; if (m <= 4) return 0.5; if (m <= 8) return 1; return 2; }
  function periodStep(m) { if (m <= 8) return 1; if (m <= 16) return 2; return 5; }
  // the period axis starts one step under the shortest period (never below 0): 8-16 s swells fill the plot
  function periodFloor(min, step) { return Number.isFinite(min) ? Math.max(0, Math.floor((min - step) / step) * step) : 0; }
  // the swell series worth drawing: graph_data.swells from the server (plan section 26), else all six
  var ALL_SWELLS = ['s1', 's2', 's3', 's4', 's5', 's6'];
  function swellKeys(gd) {
    var s = gd && Array.isArray(gd.swells) ? gd.swells.filter(function (k) { return ALL_SWELLS.indexOf(k) >= 0; }) : null;
    return s && s.length ? s : ALL_SWELLS.slice();
  }
  var BOX_MIN = 300;                                        // px per chart (was 180-360: the plots were 60-80 px tall)
  // The rows that start a day: each row whose date differs from the row before (the first row only when it is
  // midnight: a day the series starts in the middle of gets no label). Hourly rows: the midnights. 3-hourly rows
  // (a forecast point after +120 h, plan section 31): whatever hour comes first that day.
  function dayStarts(parsed) {
    var out = [];
    for (var i = 0; i < parsed.length; i++) {
      var d = parsed[i], p = parsed[i - 1];
      if (!d || isNaN(d)) continue;
      if (i === 0 ? d.getHours() === 0 : (!p || isNaN(p) || d.getDate() !== p.getDate() || d.getMonth() !== p.getMonth() || d.getFullYear() !== p.getFullYear())) out.push(i);
    }
    return out;
  }
  // The rows that start an afternoon: the first row of a day at or after noon (the noon rows when hourly).
  function noonStarts(parsed) {
    var out = [];
    for (var i = 0; i < parsed.length; i++) {
      var d = parsed[i], p = parsed[i - 1];
      if (!d || isNaN(d) || d.getHours() < 12) continue;
      if (i === 0 ? d.getHours() === 12 : (!p || isNaN(p) || p.getHours() < 12 || p.getDate() !== d.getDate())) out.push(i);
    }
    return out;
  }
  // A flat date label at every n-th day start in view, n chosen so labels are at least LABEL_PX apart on this scale.
  function dateTick(scale, parsed, mids, i) {
    var d = parsed[i]; if (!d || isNaN(d) || mids.indexOf(i) < 0) return '';
    var lo = scale && Number.isFinite(scale.min) ? scale.min : 0, hi = scale && Number.isFinite(scale.max) ? scale.max : parsed.length - 1;
    var inView = mids.filter(function (k) { return k >= lo && k <= hi; }), w = scale && scale.width > 0 ? scale.width : 1000;
    var every = Math.max(1, Math.ceil(inView.length * LABEL_PX / w)), ord = inView.indexOf(i);
    return ord >= 0 && ord % every === 0 ? formatMD(d) : '';
  }
  function formatMD(d) { return !isNaN(d) ? (d.getMonth() + 1) + '/' + d.getDate() : ''; }
  function formatMDHour(d) {
    var M = d.getMonth() + 1, D = d.getDate(), h = d.getHours();
    return h === 0 ? M + '/' + D + ' 12am' : h === 12 ? M + '/' + D + ' 12pm' : '';
  }
  // A forecast point's graphs have one slot an hour and rows every 3 hours after +120 h: the slots between rows are
  // empty in every series. Chart.js's 'index' / 'nearest' modes look only at the two slots either side of the pointer
  // and skip empty ones, so the tooltip and the sync went blank over a third of the later days (G22 R-A4). This mode
  // takes the nearest slot that holds a row (any series with a value).
  var ROW_MODE = 'allshoreRow';
  function hasRow(datasets, j) {
    for (var d = 0; d < datasets.length; d++) { var v = datasets[d].data ? datasets[d].data[j] : null; if (v !== null && v !== undefined && Number.isFinite(+v)) return true; }
    return false;
  }
  // the slot of the row nearest to a slot value (fractional); -1 when none lies within `reach` slots
  function rowAt(datasets, n, value, reach) {
    if (!Number.isFinite(value)) return -1;
    var best = -1, bestD = Infinity, lo = Math.max(0, Math.floor(value) - reach), hi = Math.min(n - 1, Math.ceil(value) + reach);
    for (var j = lo; j <= hi; j++) { var dd = Math.abs(j - value); if (dd < bestD && hasRow(datasets, j)) { best = j; bestD = dd; } }
    return best;
  }
  function slotted(gd) {                                                       // some slot holds no row in any series
    var keys = ALL_SWELLS.concat(['combined']), n = gd && gd.labels ? gd.labels.length : 0;
    var sets = keys.map(function (k) { return { data: (gd.height || {})[k] || [] }; });
    for (var j = 0; j < n; j++) if (!hasRow(sets, j)) return true;
    return false;
  }
  function rowMode(chart, e) {
    var xs = chart.scales && chart.scales.x, area = chart.chartArea, labels = chart.data && chart.data.labels;
    if (!xs || !labels) return [];
    var x = e && typeof e.x === 'number' && ('native' in e || !('clientX' in e)) ? e.x : null;
    if (x === null && e && chart.canvas && chart.canvas.getBoundingClientRect) {
      var r = chart.canvas.getBoundingClientRect(), p = e.touches && e.touches[0] ? e.touches[0] : e;
      x = p.clientX - r.left;
    }
    if (x === null || !Number.isFinite(x) || (area && (x < area.left || x > area.right))) return [];
    var shown = chart.data.datasets.filter(function (ds, di) { return chart.isDatasetVisible(di); });   // a row is a slot with a SHOWN value (G22 R2-7)
    var j = rowAt(shown, labels.length, xs.getValueForPixel(x), 3);
    if (j < 0) return [];
    var out = [];
    chart.data.datasets.forEach(function (ds, di) {
      var v = ds.data ? ds.data[j] : null, meta = chart.getDatasetMeta(di);
      if (v === null || v === undefined || !chart.isDatasetVisible(di) || !meta || !meta.data[j]) return;
      out.push({ element: meta.data[j], datasetIndex: di, index: j });
    });
    return out;
  }
  function registerRowMode(Chart) {
    var modes = Chart && Chart.Interaction && Chart.Interaction.modes;
    if (!modes) return false;
    if (!modes[ROW_MODE]) modes[ROW_MODE] = rowMode;
    return true;
  }
  function readRange(storage) { try { var v = storage.getItem(RANGE_KEY); return v === '7' || v === '3' ? v : 'full'; } catch (e) { return 'full'; } }

  function createForecastGraphs(deps) {
    var charts = [], data = null, dirty = true, ac = null, rendering = null;
    function makeNightShade(parsed) {
      return { id: 'nightShade', beforeDraw: function (chart, args, opts) {
        var ctx = chart.ctx, area = chart.chartArea, x = chart.scales && chart.scales.x;
        if (!area || !x) return;
        var startH = 18, endH = 6, fill = (opts && opts.fill) || 'rgba(0,0,0,0.06)';
        var N = parsed.length, minIdx = Math.max(0, Math.floor(x.min == null ? 0 : x.min)), maxIdx = Math.min(N - 1, Math.ceil(x.max == null ? N - 1 : x.max));
        ctx.save(); ctx.fillStyle = fill;
        for (var i = minIdx; i < maxIdx; i++) {
          var h = parsed[i] ? parsed[i].getHours() : NaN;
          if (!Number.isFinite(h) || !(h >= startH || h < endH)) continue;
          var x0 = x.getPixelForValue(i), x1 = x.getPixelForValue(i + 1), w = x1 - x0;
          if (Number.isFinite(w) && w > 0) ctx.fillRect(x0, area.top, w, area.bottom - area.top);
        }
        ctx.restore();
      } };
    }
    function makeXAxis(parsed) {
      var mids = dayStarts(parsed), major = new Set(mids), minor = new Set(noonStarts(parsed));
      var idx = function (c) { return c.tick && typeof c.tick.index === 'number' ? c.tick.index : c.index; };
      return { type: 'category',
        grid: { color: function (c) { var i = idx(c); return major.has(i) ? 'rgba(0,0,0,0.25)' : minor.has(i) ? 'rgba(0,0,0,0.15)' : 'rgba(0,0,0,0.08)'; },
                lineWidth: function (c) { var i = idx(c); return major.has(i) ? 1.4 : minor.has(i) ? 1.0 : 0.5; } },
        ticks: { autoSkip: false, maxRotation: 0, minRotation: 0, font: { size: 10 },
                 callback: function (v, i) { return dateTick(this, parsed, mids, i); } } };   // flat date labels at each day's start, thinned to fit
    }
    function dots(label, key, src, color) { return { label: label, data: src[key], borderColor: color, backgroundColor: color, showLine: false, spanGaps: false }; }
    function series(src, combined, keys) {
      var ds = keys.map(function (k) { return dots('Swell ' + k.slice(1), k, src, COLORS[k]); });
      if (combined) ds.push({ label: 'Combined', data: src.combined, borderColor: COLORS.combined, backgroundColor: COLORS.combined, showLine: false, spanGaps: false, pointRadius: 1.6 });
      return ds;
    }
    function build(Chart, gd, parsed) {
      var shade = makeNightShade(parsed), xAxis = makeXAxis(parsed);
      var mode = slotted(gd) && registerRowMode(Chart) ? ROW_MODE : 'index';
      var common = { responsive: true, maintainAspectRatio: false, interaction: { mode: mode, intersect: false, axis: 'x' },
        layout: { padding: { top: 8, right: 8, bottom: 0, left: 8 } }, elements: { point: { radius: 1.6 }, line: { tension: 0.25, borderWidth: 0 } },
        // one line of legend above the plot (it used to wrap under it and eat the plot height)
        plugins: { legend: { position: 'top', align: 'start', labels: { usePointStyle: true, boxWidth: 8, boxHeight: 6, padding: 8, font: { size: 11 } } }, tooltip: { mode: mode === ROW_MODE ? ROW_MODE : 'nearest', intersect: false }, decimation: { enabled: false } },
        animation: false };
      var keys = swellKeys(gd);
      var hArr = keys.concat(['combined']).map(function (k) { return gd.height[k]; });
      var hMax = padMax(maxAcross(hArr), 0.05); if (!Number.isFinite(hMax)) hMax = 1; var hStep = heightStep(hMax); hMax = niceCeil(hMax, hStep);
      var pArr = keys.map(function (k) { return gd.period[k]; });
      var pMax = padMax(maxAcross(pArr), 0.05); if (!Number.isFinite(pMax)) pMax = 10; var pStep = periodStep(pMax); pMax = niceCeil(pMax, pStep);
      var pMin = periodFloor(minAcross(pArr), pStep);
      var dArr = keys.map(function (k) { return gd.direction[k]; });
      var dMin = minAcross(dArr), dMax = maxAcross(dArr);
      if (!Number.isFinite(dMin) || !Number.isFinite(dMax) || dMin === dMax) { dMin = 0; dMax = 360; }
      function opts(title, y) {
        return Object.assign({}, common, { plugins: Object.assign({}, common.plugins, { title: { display: false, text: title }, nightShade: { nightStart: 18, nightEnd: 6, fill: 'rgba(0,0,0,0.06)' } }),
          scales: { x: xAxis, y: Object.assign({ grid: { display: true, color: 'rgba(0,0,0,0.08)' }, border: { color: 'rgba(0,0,0,0.2)' } }, y) } });
      }
      var specs = [
        ['Swell Height', series(gd.height, true, keys), { beginAtZero: true, min: 0, max: hMax, ticks: { stepSize: hStep }, title: { display: true, text: 'Height (' + gd.units + ')' } }],
        ['Swell Period', series(gd.period, false, keys), { min: pMin, max: pMax, ticks: { stepSize: pStep }, title: { display: true, text: 'Period (s)' } }],
        ['Swell Direction', series(gd.direction, false, keys), { min: dMin, max: dMax, ticks: { stepSize: 45 }, title: { display: true, text: 'Direction (°)' } }]
      ];
      return specs.map(function (sp, i) {
        return new Chart(deps.canvases[i].getContext('2d'), { type: 'line', data: { labels: gd.labels, datasets: sp[1] }, plugins: [shade], options: opts(sp[0], sp[2]) });
      });
    }
    // hover / touch on one chart shows the same index on the other two (listeners die with the signal)
    function wireSync(list, signal) {
      function setActive(ch, idx) {
        if (!ch || !ch.data || !ch.data.labels) return;
        var i = Math.max(0, Math.min(idx, ch.data.labels.length - 1));
        var active = [];                                       // only the swells there: a missing one is no "Swell 4: 0" (G22 R2-4)
        ch.data.datasets.forEach(function (ds, di) {
          var v = ds.data ? ds.data[i] : null;
          if (v !== null && v !== undefined && Number.isFinite(+v) && (!ch.isDatasetVisible || ch.isDatasetVisible(di))) active.push({ datasetIndex: di, index: i });
        });
        ch.setActiveElements(active);
        var xs = ch.scales && ch.scales.x, x = xs ? xs.getPixelForValue(i) : undefined;
        if (ch.tooltip) ch.tooltip.setActiveElements(active, { x: x });
        ch.update('none');
      }
      function clear(ch) { if (!ch) return; ch.setActiveElements([]); if (ch.tooltip) ch.tooltip.setActiveElements([], {}); ch.update('none'); }
      function broadcast(src, evt) {
        var m = src.options && src.options.interaction && src.options.interaction.mode === ROW_MODE ? ROW_MODE : 'index';
        var pts = src.getElementsAtEventForMode(evt, m, { intersect: false, axis: 'x' }, true);
        if (!pts || !pts.length) return;
        list.forEach(function (ch) { if (ch !== src) setActive(ch, pts[0].index); });
      }
      list.forEach(function (ch) {
        var el = ch.canvas; if (!el) return;
        el.addEventListener('mousemove', function (e) { broadcast(ch, e); }, { signal: signal });
        el.addEventListener('mouseleave', function () { list.forEach(clear); }, { signal: signal });
        el.addEventListener('touchstart', function (e) { broadcast(ch, e); }, { passive: true, signal: signal });
        el.addEventListener('touchmove', function (e) { broadcast(ch, e); }, { passive: true, signal: signal });
        el.addEventListener('touchend', function () { list.forEach(clear); }, { signal: signal });
        el.addEventListener('touchcancel', function () { list.forEach(clear); }, { signal: signal });
      });
    }
    function destroy() {
      charts.forEach(function (c) { try { c.destroy(); } catch (e) {} });
      charts = [];
      if (ac) { ac.abort(); ac = null; }
    }
    function applyRange(v) {
      if (!data) return;
      var w = rangeWindow(data.labels.length, v === '7' ? 7 : v === '3' ? 3 : 0);   // one slot an hour (a point's too: the server fills the 3-hourly part)
      charts.forEach(function (ch) { ch.options.scales.x.min = w.min; ch.options.scales.x.max = w.max; ch.update('none'); });
      if (deps.rangeBar) Array.prototype.forEach.call(deps.rangeBar.querySelectorAll('[data-days]'), function (b) {
        var on = (b.getAttribute('data-days') === (v === '7' ? '7' : v === '3' ? '3' : '0'));
        b.classList.toggle('active', on); b.setAttribute('aria-pressed', on ? 'true' : 'false');
      });
    }
    function fit() {
      var h = Math.max(BOX_MIN, Math.floor((deps.bodyHeight() - 40) / 3));
      deps.boxes.forEach(function (b) { b.style.height = h + 'px'; });
      charts.forEach(function (c) { try { c.resize(); } catch (e) {} });
    }
    function render() {
      if (rendering) return rendering;
      if (!data) { destroy(); return Promise.resolve(); }
      var gd = data;
      rendering = deps.loadChartJs().then(function () {
        rendering = null;
        if (data !== gd) { if (!data) destroy(); else if (deps.visible()) return render(); return; }   // newer data landed meanwhile: draw that
        if (!deps.visible()) return;                                            // hidden while Chart.js loaded: show() draws it later
        destroy();
        var parsed = gd.labels.map(parseLabel);
        charts = build(deps.getChart(), gd, parsed);
        ac = new AbortController();
        wireSync(charts, ac.signal);
        fit();
        applyRange(readRange(deps.storage));
        dirty = false;
      }, function (err) {
        rendering = null;
        if (deps.onError) deps.onError(err);
      });
      return rendering;
    }
    function setData(gd) { data = gd && gd.labels ? gd : null; dirty = true; if (!data) destroy(); else if (deps.visible()) return render(); return Promise.resolve(); }
    function show() { if (dirty) return render(); fit(); return Promise.resolve(); }
    function setRange(v) { try { deps.storage.setItem(RANGE_KEY, v === '7' || v === '3' ? v : 'full'); } catch (e) {} applyRange(v); }
    if (deps.rangeBar) deps.rangeBar.addEventListener('click', function (e) {
      var b = e.target && e.target.closest ? e.target.closest('[data-days]') : null;
      if (!b) return;
      var d = b.getAttribute('data-days'); setRange(d === '7' ? '7' : d === '3' ? '3' : 'full');
    });
    return { setData: setData, show: show, resize: fit, destroy: destroy, setRange: setRange, charts: function () { return charts; }, hasData: function () { return !!data; } };
  }

  // ---- the floating window ----
  // deps: el (the window), header, handle, storage (sessionStorage-like), win (window-like: innerWidth,
  //       innerHeight, matchMedia, addEventListener), key (the storage key; the forecast window's by default),
  //       defaultMode ('min' by default: a new tab starts minimised), topBarHeight() -> px (optional, 0 now
  //       that the map is the page), onMode(mode), onResize().
  function FloatingWindow(deps) {
    this.d = deps; this.el = deps.el; this.key = deps.key || WINDOW_KEY;
    var saved = readJson(deps.storage, this.key);
    this.geom = typeof saved.w === 'number' && typeof saved.h === 'number' && typeof saved.x === 'number' && typeof saved.y === 'number' ? { x: saved.x, y: saved.y, w: saved.w, h: saved.h } : null;
    var dflt = deps.defaultMode === 'normal' || deps.defaultMode === 'max' ? deps.defaultMode : 'min';
    this.mode = saved.mode === 'normal' || saved.mode === 'max' || saved.mode === 'min' ? saved.mode : dflt;
    this.prev = saved.prev === 'max' ? 'max' : 'normal';
    if (deps.canMax === false) { if (this.mode === 'max') this.mode = 'normal'; this.prev = 'normal'; }
    this.opener = null; this.maxW = 0;
    this._applyMode();
    // phone mode leaves the geometry to CSS: a saved desktop box is neither applied nor cut down to the phone (G18b-B P3-3)
    if (this.geom && !this.isPhone()) this._place(clampGeometry(this.geom, deps.win.innerWidth, deps.win.innerHeight, this._top(), null, this.maxW));
    this._bind();
  }
  FloatingWindow.prototype._top = function () { return this.d.topBarHeight ? this.d.topBarHeight() : 0; };
  FloatingWindow.prototype.isPhone = function () { var mm = this.d.win.matchMedia; return !!(mm && mm.call(this.d.win, PHONE_QUERY).matches); };
  FloatingWindow.prototype._save = function () {
    var g = this.geom || {};
    writeJson(this.d.storage, this.key, { x: g.x, y: g.y, w: g.w, h: g.h, mode: this.mode, prev: this.prev });
  };
  FloatingWindow.prototype._place = function (g) {
    this.geom = g;
    var s = this.el.style;
    s.left = g.x + 'px'; s.top = g.y + 'px'; s.width = g.w + 'px'; s.height = g.h + 'px'; s.right = 'auto'; s.bottom = 'auto';
  };
  FloatingWindow.prototype._applyMode = function () {
    var cl = this.el.classList;
    cl.toggle('fw-min', this.mode === 'min'); cl.toggle('fw-max', this.mode === 'max');
    if (this.el.style) this.el.style.maxWidth = this.maxW && this.mode !== 'min' ? this.maxW + 'px' : '';
    if (this.d.onMode) this.d.onMode(this.mode);
  };
  FloatingWindow.prototype.setMode = function (mode) {
    if (mode !== 'min' && mode !== 'max' && mode !== 'normal') return;
    if (mode === 'max' && this.d.canMax === false) mode = 'normal';           // a window without a maximised state
    if (mode === 'min' && this.mode !== 'min') this.prev = this.mode;
    if (mode !== 'min' && this.mode === 'min') this._expandedAt = Date.now();
    this.mode = mode; this._applyMode(); this._save();
    if (mode !== 'min' && this.d.onResize) this.d.onResize();
  };
  FloatingWindow.prototype.minimise = function () { this.setMode('min'); };
  FloatingWindow.prototype.expand = function () { if (this.mode === 'min') this.setMode(this.prev); };
  FloatingWindow.prototype.toggleMax = function () { this.setMode(this.mode === 'max' ? 'normal' : 'max'); };
  // the window's current box (from CSS until the first drag or resize)
  FloatingWindow.prototype._rect = function () {
    if (this.geom) return this.geom;
    var r = this.el.getBoundingClientRect(), w = r.width;
    if (this.maxW && w >= this.maxW - 1) w = Math.min(DEFAULT_W, this.d.win.innerWidth - 32);   // capped on screen: the CSS default width
    return { x: r.left, y: r.top, w: w, h: r.height };
  };
  // The widest the window may be (0 = the viewport): the table's own width in Table view, so neither a drag,
  // a key nor maximising shows white space beside it. Not applied to the minimised chip (its own CSS width).
  FloatingWindow.prototype.setMaxWidth = function (w) {
    this.maxW = w > 0 ? Math.round(w) : 0;
    this.el.style.maxWidth = this.maxW && this.mode !== 'min' ? this.maxW + 'px' : '';   // the saved width stays the viewer's
  };
  // a resize starts from the width on screen and, in Table view, stops at the table
  FloatingWindow.prototype._shown = function (w) { return this.maxW ? Math.min(w, this.maxW) : w; };
  FloatingWindow.prototype._resized = function (w) { return this.maxW ? Math.min(w, this.maxW) : w; };
  FloatingWindow.prototype.resizeBy = function (dw, dh) {
    var r = this._rect(), d = this.d;
    this._place(resizeGeometry(r, 'se', dw, dh, d.win.innerWidth, d.win.innerHeight, this._top(), null, this.maxW));   // like the pointer grip (G19-A P3-2)
    this._save();
    if (d.onResize) d.onResize();
  };
  FloatingWindow.prototype.clamp = function () {
    if (!this.geom || this.isPhone()) return;
    this._place(clampGeometry(this.geom, this.d.win.innerWidth, this.d.win.innerHeight, this._top(), null, this.maxW));
    this._save();
  };
  FloatingWindow.prototype._bind = function () {
    var self = this, d = this.d;
    function drag(startEv, kind, edge) {
      if (self.isPhone() || self.mode === 'max') return;
      var r = self._rect(), sx = startEv.clientX, sy = startEv.clientY, moved = false;
      var target = startEv.currentTarget;
      if (target.setPointerCapture) try { target.setPointerCapture(startEv.pointerId); } catch (e) {}
      function onMove(ev) {
        var dx = ev.clientX - sx, dy = ev.clientY - sy;
        if (!moved && Math.abs(dx) + Math.abs(dy) < 2) return;
        moved = true;
        if (kind === 'move') { self._place(clampGeometry({ x: r.x + dx, y: r.y + dy, w: r.w, h: r.h }, d.win.innerWidth, d.win.innerHeight, self._top(), null, self.maxW)); return; }
        self._place(resizeGeometry(r, edge || 'se', dx, dy, d.win.innerWidth, d.win.innerHeight, self._top(), null, self.maxW));
      }
      function onUp() {
        target.removeEventListener('pointermove', onMove); target.removeEventListener('pointerup', onUp); target.removeEventListener('pointercancel', onUp);
        if (moved) { self._save(); if (kind === 'resize' && d.onResize) d.onResize(); }
      }
      target.addEventListener('pointermove', onMove); target.addEventListener('pointerup', onUp); target.addEventListener('pointercancel', onUp);
      if (startEv.preventDefault) startEv.preventDefault();
    }
    d.header.addEventListener('pointerdown', function (e) {
      if (e.button !== undefined && e.button !== 0) return;
      if (e.target && e.target.closest && e.target.closest('button, a, input, select, .station-results')) return;
      if (self.mode === 'min') return;                                         // the minimised bar is not dragged (a click expands it)
      drag(e, 'move');
    });
    d.header.addEventListener('dblclick', function (e) {
      if (e.target && e.target.closest && e.target.closest('button, a, input, select, .station-results')) return;
      if (self.isPhone()) return;
      if (self.mode === 'min') { self.expand(); return; }
      if (Date.now() - (self._expandedAt || 0) < 600) return;                   // the first click of this double-click expanded the chip
      if (d.canMax !== false) self.toggleMax();
    });
    if (d.handle) d.handle.addEventListener('pointerdown', function (e) { if (self.mode === 'normal') drag(e, 'resize', 'se'); });
    // the edges and the other corners (plan section 27): each element carries data-edge
    var edges = this.el.querySelectorAll ? this.el.querySelectorAll('.fw-edge') : [];
    Array.prototype.forEach.call(edges, function (h) {
      var edge = h.getAttribute && h.getAttribute('data-edge');
      if (!/^(n|s|e|w|ne|nw|se|sw)$/.test(edge || '')) return;
      h.addEventListener('pointerdown', function (e) {
        if (e.button !== undefined && e.button !== 0) return;
        if (self.mode === 'normal') drag(e, 'resize', edge);
      });
    });
    // the arrow keys on the focused handle: 16 px a press, 64 with Shift (right/down grow, left/up shrink)
    if (d.handle) d.handle.addEventListener('keydown', function (e) {
      var k = { ArrowRight: [1, 0], ArrowLeft: [-1, 0], ArrowDown: [0, 1], ArrowUp: [0, -1] }[e.key];
      if (!k || self.mode !== 'normal' || self.isPhone()) return;
      if (e.preventDefault) e.preventDefault();
      self.resizeBy(k[0] * (e.shiftKey ? 64 : 16), k[1] * (e.shiftKey ? 64 : 16));
    });
    d.win.addEventListener('resize', function () {
      self.clamp();
      if (self.mode !== 'min' && d.onResize) d.onResize();
    });
  };

  // ---- the settings panel (Time Zone, Units under the gear) ----
  function createSettings(deps) {
    var btn = deps.button, panel = deps.panel, doc = deps.document, open = false;
    if (!btn || !panel) return { open: function () {}, close: function () {}, isOpen: function () { return false; } };
    function set(on) {
      open = on; panel.hidden = !on; btn.setAttribute('aria-expanded', on ? 'true' : 'false');
      if (on && deps.focusFirst) deps.focusFirst();
    }
    btn.addEventListener('click', function () { set(!open); });
    doc.addEventListener('pointerdown', function (e) { if (open && !panel.contains(e.target) && !btn.contains(e.target)) set(false); });
    panel.addEventListener('keydown', function (e) { if (e.key === 'Escape') { e.stopPropagation(); set(false); btn.focus(); } });
    return { open: function () { set(true); }, close: function () { set(false); }, isOpen: function () { return open; } };
  }

  // ---- the live-buoy window (plan section 26) ----
  // A FloatingWindow over the page's live-buoy markup (#liveBuoyPanel with #lwHeader, #lwMin, #lwClose, #lwResize),
  // opened and closed by the map script; no maximised state (owner: no gain); a new tab opens it as a window;
  // minimised it is a chip at the bottom-right (a bar above the forecast bar on phones) with the close button still
  // there. Dispatches 'allshore:livewin' {open, mode} on every change (the page sizes the map around the phone bars).
  // opts: window, document, storage, onClose() (the map script's own close work, e.g. abandoning a fetch).
  function createLiveWindow(opts) {
    opts = opts || {};
    var win = opts.window || window, doc = opts.document || win.document;
    var $ = function (id) { return doc.getElementById(id); };
    var el = $('liveBuoyPanel'), header = $('lwHeader'), minBtn = $('lwMin'), closeBtn = $('lwClose');
    if (!el || !header) return null;
    var storage = opts.storage || (function () { try { var st = win.sessionStorage; if (st && typeof st.getItem === 'function') return st; } catch (e) {} return { getItem: function () { return null; }, setItem: function () {} }; })();
    var fw = new FloatingWindow({ el: el, header: header, handle: $('lwResize'), storage: storage, win: win, key: LIVE_WINDOW_KEY, defaultMode: 'normal', canMax: false,
      onMode: function (m) {
        if (minBtn) { minBtn.setAttribute('aria-expanded', m === 'min' ? 'false' : 'true'); minBtn.setAttribute('aria-label', m === 'min' ? 'Expand live buoy' : 'Minimise live buoy'); minBtn.textContent = m === 'min' ? '\u25B4' : '\u2013'; }
        notify();
      } });
    function notify() {
      if (!fw) return;                                                       // (onMode runs once inside the constructor)
      try { doc.dispatchEvent(new CustomEvent('allshore:livewin', { detail: { open: !el.hidden, mode: fw.mode } })); } catch (e) {}
    }
    function isOpen() { return !el.hidden; }
    function open() {                                                       // a buoy pick: shown, expanded, on top
      el.hidden = false;
      if (fw.mode === 'min') fw.expand();
      notify();
    }
    function close() { el.hidden = true; if (opts.onClose) opts.onClose(); notify(); }
    if (minBtn) minBtn.addEventListener('click', function () { if (fw.mode === 'min') { fw.expand(); if (header.focus) header.focus({ preventScroll: true }); } else fw.minimise(); });
    if (closeBtn) closeBtn.addEventListener('click', function () { close(); });
    header.addEventListener('click', function (e) {
      if (fw.mode === 'min' && !(e.target && e.target.closest && e.target.closest('button'))) { fw.expand(); }
    });
    return { window: fw, open: open, close: close, isOpen: isOpen, el: el };
  }

  // ---- init: wire everything to the page ----
  // opts: initial (window.__initial), stationLabel(sid) -> text, loadChartJs() -> Promise, closeLivePanel(),
  //       liveOpen() -> bool, fetch, document, window, storage (session), settings (local), onMode(mode).
  var app = null;
  function init(opts) {
    opts = opts || {};
    var win = opts.window || window, doc = opts.document || win.document;
    if (!doc || !doc.getElementById) return null;
    var $ = function (id) { return doc.getElementById(id); };
    // the title is the picker's own label (#stationCurrent: the favourites picker is the window's heading)
    var els = { win: $('forecastWin'), header: $('fwHeader'), title: $('stationCurrent') || $('fwTitle'), cycle: $('fwCycle'), busy: $('fwBusy'), min: $('fwMin'), max: $('fwMax'),
      viewBar: $('viewBar'), modelBar: $('modelBar'), rangeBar: $('rangeBar'), body: $('fwBody'), error: $('fwError'), meta: $('forecastMeta'),
      table: $('forecastTable'), graphs: $('graphs'), note: $('fwNote'), handle: $('fwResize'), tz: $('tz'), unit: $('unit'), station: $('station'), trigger: $('stationTrigger') };
    if (!els.win || !els.body || !els.table) return null;
    function safeStorage(name) {
      try { var st = win[name]; if (st && typeof st.getItem === 'function' && typeof st.setItem === 'function') return st; } catch (e) {}
      return { getItem: function () { return null; }, setItem: function () {} };   // nothing remembered, nothing broken
    }
    var session = opts.storage || safeStorage('sessionStorage'), local = opts.settings || safeStorage('localStorage');
    var initial = opts.initial || {}, swanStations = initial.swan_stations || [];
    var state = resolveInitialState((win.location && win.location.search) || '', readJson(local, SETTINGS_KEY), initial);
    var fw = new FloatingWindow({ el: els.win, header: els.header, handle: els.handle, storage: session, win: win,
      onMode: function (m) {
        if (els.min) { els.min.setAttribute('aria-expanded', m === 'min' ? 'false' : 'true'); els.min.setAttribute('aria-label', m === 'min' ? 'Expand forecast' : 'Minimise forecast'); els.min.textContent = m === 'min' ? '▴' : '–'; }
        if (els.max) els.max.setAttribute('aria-pressed', m === 'max' ? 'true' : 'false');
        if (opts.onMode) opts.onMode(m);
        if (m !== 'min') fitWidth();
      },
      onResize: function () { if (state.view === 'Graph') graphs.resize(); else fitWidth(); } });
    var canvases = ['heightChart', 'periodChart', 'directionChart'].map($);
    var graphs = createForecastGraphs({ host: els.graphs, boxes: canvases.map(function (c) { return c.parentNode; }), canvases: canvases, rangeBar: els.rangeBar,
      loadChartJs: opts.loadChartJs || function () { return win.Chart ? Promise.resolve() : Promise.reject(new Error('Chart.js unavailable')); },
      getChart: function () { return win.Chart; }, storage: session,
      bodyHeight: function () { return els.body.clientHeight; },
      visible: function () { return state.view === 'Graph' && fw.mode !== 'min' && !els.graphs.hidden; },
      onError: function () { showError('Charts are unavailable right now.', function () { return setView('Graph'); }); } });
    function pressed(bar, attr, value) {
      if (!bar) return;
      Array.prototype.forEach.call(bar.querySelectorAll('[' + attr + ']'), function (b) {
        var on = b.getAttribute(attr) === value; b.classList.toggle('active', on); b.setAttribute('aria-pressed', on ? 'true' : 'false');
      });
    }
    function text(el, s) { if (el) el.textContent = s; }
    function clearNode(el) { el.textContent = ''; }                                // children and any innerHTML text alike
    function showError(msg, retry) {
      var box = els.error; if (!box) return;
      clearNode(box);
      if (!msg) { box.hidden = true; return; }
      var ld = $('forecastLoading'); if (ld && ld.parentNode) ld.parentNode.removeChild(ld);   // no spinner beside an error
      var lbl = opts.stationLabel ? opts.stationLabel(state.station) : state.station;        // the header names the station the address bar shows
      writeLabel(els.title, lbl); if (els.title) els.title.title = lbl;
      box.appendChild(doc.createTextNode(msg + ' '));
      if (retry) {
        var b = doc.createElement('button'); b.type = 'button'; b.className = 'btn btn-sm btn-outline-secondary'; b.textContent = 'Retry';
        b.addEventListener('click', function () { if (els.body && els.body.focus) els.body.focus(); retry(); });   // the button goes: focus stays in the window (G22 R-B8)
        box.appendChild(b);
      }
      box.hidden = false;
    }
    function setMeta(h) {
      var m = els.meta; if (!m) return;
      clearNode(m);
      if (!h) return;
      [['Cycle : ', h.cycle], ['Location : ', h.location], ['Time Zone: ', zoneLabel(h.tz)]].forEach(function (pair, i) {
        if (i) m.appendChild(doc.createTextNode('  |  '));
        var b = doc.createElement('strong'); b.textContent = pair[0]; m.appendChild(b);
        m.appendChild(doc.createTextNode(String(pair[1] == null ? '' : pair[1])));
      });
    }
    // A line of news for the visitor in the window (a point that could not be kept, G22 B-3): shown until another
    // station or point is on screen.
    var noteFor = null;
    function note(msg, sid) {
      if (!els.note) return;
      noteFor = msg ? (sid || state.station) : null;
      els.note.textContent = msg || '';
      els.note.hidden = !msg;
    }
    // the <head>'s early request (window.__early.forecast = {q, p}): taken once, by the first load, and only
    // for exactly the same query; any other first query fetches as usual (the early response is dropped)
    var earlyBox = opts.early || null;
    function takeEarly(q) {
      var e = earlyBox && earlyBox.forecast; if (earlyBox) earlyBox.forecast = null; earlyBox = null;
      return e && e.q === q && e.p && typeof e.p.then === 'function' ? e.p : null;
    }
    var loader = createLoader({ fetch: opts.fetch || function (u, o) { return win.fetch(u, o); }, takeEarly: takeEarly, now: function () { return Date.now(); },
      replaceState: function (url) { try { win.history.replaceState(null, '', url); } catch (e) {} }, swanStations: swanStations,
      ui: { busy: function (on) { if (els.busy) els.busy.hidden = !on; els.body.setAttribute('aria-busy', on ? 'true' : 'false'); },
            error: showError,
            clear: function () {                                              // a failed load leaves nothing of the last forecast (G22 B-7, R-A9, R-A10)
              clearNode(els.table); setMeta(null); graphs.setData(null);
              text(els.cycle, ''); if (els.cycle) els.cycle.title = '';
              if (els.modelBar) els.modelBar.hidden = true;
              note(null);
            },
            apply: function (d, st, retry) {
              var avail = typeof d.swan_available === 'boolean' ? d.swan_available : swanStations.indexOf(st.station) >= 0;
              if (els.modelBar) { els.modelBar.hidden = !avail; pressed(els.modelBar, 'data-model', st.model); }
              var label = opts.stationLabel ? opts.stationLabel(st.station) : st.station, cyc = shortCycle(st.model, d.graph_header);
              var age = d.point && Number.isFinite(d.point.age_hours) ? d.point.age_hours : null;
              if (age !== null && age >= STALE_H) cyc += ' · ' + Math.round(age) + ' h old';   // a point's run NOAA stopped feeding (plan section 31)
              writeLabel(els.title, label); text(els.cycle, cyc);
              if (noteFor !== null && noteFor !== st.station) note(null);
              if (els.title) els.title.title = label; if (els.cycle) els.cycle.title = cyc;
              setMeta(d.graph_header);
              if (d.table_html) els.table.innerHTML = d.table_html;           // the server's own table (build_html_table)
              else { clearNode(els.table); if (!d.error) { var p = doc.createElement('div'); p.className = 'text-muted'; p.textContent = 'No forecast available.'; els.table.appendChild(p); } }   // an error is said once, in its box (G22 K-5)
              if (d.error && !d.table_html) showError(String(d.error), d.final ? null : retry || null);   // visible in both views, with Retry (a failed build is not cached; a land point's answer is final)
              else if (d.error) showError(String(d.error), null);
              else showError(null, null);
              graphs.setData(d.graph_data);
              fitWidth();                                                     // the window follows the new table's width
              syncSelects();
              // the page's other parts (the model overlay's valid-time zone and run line) follow the forecast on screen
              // ok: a forecast is on screen (a refused point - land, busy - is not; the page saves a new point only then)
              // final: the answer will not change (a point on land, sheltered water, no model data: the star cannot keep it)
              try { doc.dispatchEvent(new CustomEvent('allshore:forecast', { detail: { station: st.station, tz: d.tz_label || '', model: st.model, view: state.view,
                ok: !!d.table_html, point: d.point || null, final: !!d.final, reason: d.reason || null } })); } catch (e) {}
            } }, saved: function () { return readJson(local, SETTINGS_KEY); } }, state);
    function syncSelects() {
      if (els.station && els.station.value !== state.station) els.station.value = state.station;
      if (els.tz) {
        els.tz.value = state.tz;
        if (els.tz.value !== state.tz && state.tz) { var o = doc.createElement('option'); o.value = state.tz; o.textContent = state.tz; els.tz.appendChild(o); els.tz.value = state.tz; }
      }
      if (els.unit && els.unit.value !== state.unit) els.unit.value = state.unit;
    }
    // Table view: the window is no wider than its table (+ the body's padding, border and a scrollbar); Graph view
    // and phones: the full width. Measured only while the table is on screen (a minimised body measures 0).
    function fitWidth() {
      if (!fw) return;
      if (state.view !== 'Table' || fw.isPhone()) { fw.setMaxWidth(0); return; }
      var t = els.table.querySelector ? els.table.querySelector('table') : null, w = t ? t.scrollWidth : 0;
      var bar = els.body.offsetWidth > 0 ? Math.max(0, els.body.offsetWidth - els.body.clientWidth) : 18;   // 0 with overlay scrollbars
      if (w > 0) fw.setMaxWidth(w + TABLE_CHROME + bar);
    }
    function setView(v) {
      state.view = v === 'Graph' ? 'Graph' : 'Table';
      pressed(els.viewBar, 'data-view', state.view);
      var g = state.view === 'Graph';
      els.table.hidden = g; if (els.graphs) els.graphs.hidden = !g; if (els.rangeBar) els.rangeBar.hidden = !g;
      fitWidth();
      loader.sync();
      return g ? graphs.show() : Promise.resolve();
    }
    // the element to give the focus back to: on screen, or none (a favourites button is hidden with its list)
    function visible(el) { return !!el && el.isConnected !== false && el.offsetParent !== null && el !== doc.body; }
    function expand() {
      if (fw.mode === 'min') { fw.opener = visible(doc.activeElement) ? doc.activeElement : null; fw.expand(); }
      if (state.view === 'Graph') graphs.show();
      if (els.header && els.header.focus) els.header.focus({ preventScroll: true });
    }
    function minimise() {
      var back = visible(fw.opener) ? fw.opener : els.trigger;
      fw.minimise();
      if (back && back.focus) try { back.focus({ preventScroll: true }); } catch (e) {}
      fw.opener = null;
    }
    // controls
    if (els.min) els.min.addEventListener('click', function () { if (fw.mode === 'min') expand(); else minimise(); });
    if (els.max) els.max.addEventListener('click', function () { if (fw.mode === 'min') { fw.prev = 'max'; expand(); } else { fw.toggleMax(); if (state.view === 'Graph') graphs.resize(); } });
    if (els.header) els.header.addEventListener('click', function (e) {
      if (fw.mode === 'min' && !(e.target && e.target.closest && e.target.closest('button, .station-results'))) expand();
    });
    if (els.viewBar) els.viewBar.addEventListener('click', function (e) { var b = e.target && e.target.closest ? e.target.closest('[data-view]') : null; if (b) setView(b.getAttribute('data-view')); });
    if (els.modelBar) els.modelBar.addEventListener('click', function (e) { var b = e.target && e.target.closest ? e.target.closest('[data-model]') : null; if (b) loader.load({ model: b.getAttribute('data-model') === 'SWAN' ? 'SWAN' : 'GFS' }); });
    function saveSettings() { writeJson(local, SETTINGS_KEY, { tz: state.tz, unit: state.unit }); }
    // the settings are saved BEFORE the load, so the address bar names nothing a reload would not assume anyway
    if (els.tz) els.tz.addEventListener('change', function () { state.tz = els.tz.value || ''; saveSettings(); loader.load({}); });
    if (els.unit) els.unit.addEventListener('change', function () { state.unit = isUnit(els.unit.value) ? els.unit.value : 'US'; saveSettings(); loader.load({}); });
    doc.addEventListener('allshore:station', function (e) {
      var det = e.detail || {}, sid = String(det.sid || ''); if (!sid) return;
      var next = { station: sid }; if (det.source === 'map') next.tz = '';    // a map pick goes back to Buoy Local, as before
      loader.load(next);
      syncSelects();                                                            // the selects show the new state at once, not after the fetch
      expand();
    });
    doc.addEventListener('keydown', function (e) {
      if (e.key !== 'Escape') return;
      if (opts.liveOpen && opts.liveOpen()) { if (opts.closeLivePanel) opts.closeLivePanel(); return; }
      if (fw.mode !== 'min' && els.win.contains(doc.activeElement)) minimise();
    });
    var settings = createSettings({ button: $('settingsBtn'), panel: $('settingsPanel'), document: doc, focusFirst: function () { if (els.tz && els.tz.focus) els.tz.focus(); } });
    // start
    syncSelects();
    setView(state.view);
    if (initial.inline) {
      loader.seed({ table_html: initial.error ? null : (els.table.innerHTML.trim() || null), graph_data: initial.graph_data || null, graph_header: initial.graph_header || null,
        error: initial.error || null, model: initial.model, swan_available: initial.swan_available, wind_complete: initial.wind_complete,
        point: initial.point || null, final: !!initial.final, reason: initial.reason || null });
    } else loader.load({});
    app = { state: state, loader: loader, window: fw, graphs: graphs, settings: settings, setView: setView, expand: expand, minimise: minimise, note: note, els: els };
    return app;
  }

  window.AllshoreForecast = {
    init: init, createLiveWindow: createLiveWindow,
    pointId: pointId, parsePointId: parsePointId, isPointId: isPointId, pointLabel: pointLabel, fmtPoint: fmtPoint, cleanName: cleanName,
    createPointStore: createPointStore, POINTS_MAX: POINTS_MAX, writeLabel: writeLabel, zoneLabel: zoneLabel,
    load: function (next) { return app ? app.loader.load(next) : Promise.resolve(null); },
    // a station's forecast fetched and kept, not shown (a map pick goes back to Buoy Local, as allshore:station does)
    prefetch: function (sid) { return app ? app.loader.prefetch({ station: String(sid), tz: '' }) : Promise.reject(new Error('no forecast window')); },
    note: function (msg, sid) { if (app) app.note(msg, sid); },
    expand: function () { if (app) app.expand(); },
    minimise: function () { if (app) app.minimise(); },
    getMode: function () { return app ? app.window.mode : null; },
    getState: function () { return app ? Object.assign({}, app.state) : null; },
    _internals: {
      SETTINGS_KEY: SETTINGS_KEY, WINDOW_KEY: WINDOW_KEY, RANGE_KEY: RANGE_KEY, CACHE_MAX: CACHE_MAX, CACHE_TTL_MS: CACHE_TTL_MS, MIN_SIZE: MIN_SIZE, PHONE_QUERY: PHONE_QUERY,
      resolveInitialState: resolveInitialState, queryFor: queryFor, urlFor: urlFor, keyOf: keyOf,
      clampGeometry: clampGeometry, resizeGeometry: resizeGeometry, dateTick: dateTick, rowAt: rowAt, rowMode: rowMode, slotted: slotted, periodFloor: periodFloor, swellKeys: swellKeys, readJson: readJson, writeJson: writeJson, shortCycle: shortCycle, parseLabel: parseLabel, rangeWindow: rangeWindow,
      createLoader: createLoader, ttlOf: ttlOf, createForecastGraphs: createForecastGraphs, FloatingWindow: FloatingWindow, createSettings: createSettings,
      createLiveWindow: createLiveWindow, LIVE_WINDOW_KEY: LIVE_WINDOW_KEY,
      POINTS_KEY: POINTS_KEY, POINT_NAME_MAX: POINT_NAME_MAX, STALE_H: STALE_H, readPoints: readPoints, dayStarts: dayStarts, noonStarts: noonStarts,
      app: function () { return app; }
    }
  };
})();
