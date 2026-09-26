/* Allshore Surf forecast window (plan section 25). Loaded on every page; touches no DOM until init()
 * is called, so tests/ui/forecast.test.js can evaluate it in Node. The window itself (loader, drag and
 * resize, graphs, settings) lands with the page restructure; this first cut carries the pure helpers
 * the rest is built on and an init() that does nothing yet.
 */
(function () {
  'use strict';

  var SETTINGS_KEY = 'allshore.settings.v1';   // localStorage {tz, unit}
  var WINDOW_KEY = 'allshore.forecastWin.v1';  // sessionStorage {x, y, w, h, mode, prev}
  var UNITS = { US: 1, Metric: 1 };

  // The state a page starts from: the URL wins, then the viewer's saved settings (tz, unit only),
  // then what the server rendered. tz '' means Buoy Local; an explicit empty tz in the URL counts.
  function resolveInitialState(search, stored, server) {
    var p = new URLSearchParams(search || ''), st = stored || {}, sv = server || {};
    var unit = p.has('unit') ? p.get('unit') : (typeof st.unit === 'string' ? st.unit : sv.unit);
    return {
      station: p.get('station') || sv.station || '51201',
      tz: p.has('tz') ? p.get('tz') : (typeof st.tz === 'string' ? st.tz : (sv.tz || '')),
      unit: UNITS[unit] ? unit : 'US',
      model: (p.get('model') || sv.model || 'GFS').toUpperCase() === 'SWAN' ? 'SWAN' : 'GFS',
      view: (p.get('view') || sv.view) === 'Graph' ? 'Graph' : 'Table'
    };
  }
  // The /api/forecast query for a state (view is client-only).
  function queryFor(s) {
    return new URLSearchParams({ station: s.station, tz: s.tz || '', unit: s.unit, model: s.model }).toString();
  }
  // The address bar for a state: the station always, the rest only when not the default.
  function urlFor(s) {
    var p = new URLSearchParams({ station: s.station });
    if (s.tz) p.set('tz', s.tz);
    if (s.unit !== 'US') p.set('unit', s.unit);
    if (s.model !== 'GFS') p.set('model', s.model);
    if (s.view !== 'Table') p.set('view', s.view);
    return '?' + p.toString();
  }
  // The client cache key: one forecast per station, time zone, unit and model.
  function keyOf(s) { return [s.station, s.tz || '', s.unit, s.model].join('|'); }
  // A window geometry kept inside a viewport of vw x vh below a top edge: the size shrinks to fit (never
  // below min), the position is pulled back so the whole window (and so its header) stays on screen.
  function clampGeometry(g, vw, vh, top, min) {
    var m = min || { w: 360, h: 220 }, t = top || 0, pad = 8;
    var w = Math.max(Math.min(m.w, vw - 2 * pad), Math.min(g.w, vw - 2 * pad));
    var h = Math.max(Math.min(m.h, vh - t - 2 * pad), Math.min(g.h, vh - t - 2 * pad));
    var x = Math.min(Math.max(g.x, pad), Math.max(pad, vw - pad - w));
    var y = Math.min(Math.max(g.y, t + pad), Math.max(t + pad, vh - pad - h));
    return { x: x, y: y, w: w, h: h };
  }
  function readJson(storage, key) {
    try { var o = JSON.parse(storage.getItem(key) || '{}'); return o && typeof o === 'object' && !Array.isArray(o) ? o : {}; } catch (e) { return {}; }
  }
  function writeJson(storage, key, value) {
    try { storage.setItem(key, JSON.stringify(value)); return true; } catch (e) { return false; }
  }

  window.AllshoreForecast = {
    init: function () {},                        // the window is wired by the page restructure (plan section 25, PR B)
    _internals: {
      SETTINGS_KEY: SETTINGS_KEY, WINDOW_KEY: WINDOW_KEY,
      resolveInitialState: resolveInitialState, queryFor: queryFor, urlFor: urlFor, keyOf: keyOf,
      clampGeometry: clampGeometry, readJson: readJson, writeJson: writeJson
    }
  };
})();
