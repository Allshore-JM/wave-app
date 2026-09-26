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
 * #heightChart #periodChart #directionChart) #fwResize; #topBar #settingsBtn #settingsPanel #tz #unit
 * #station #stationTrigger #liveBuoyPanel. The page dispatches 'allshore:station' {sid, source} on a
 * marker click ('map') or a favourites pick ('picker').
 */
(function () {
  'use strict';

  var SETTINGS_KEY = 'allshore.settings.v1';   // localStorage {tz, unit}
  var WINDOW_KEY = 'allshore.forecastWin.v1';  // sessionStorage {x, y, w, h, mode, prev}
  var RANGE_KEY = 'chartRange';                // sessionStorage 'full' | '7' | '3' (unchanged from the old page)
  var UNITS = { US: 1, Metric: 1 };
  var CACHE_MAX = 16, CACHE_TTL_MS = 10 * 60 * 1000;
  var MIN_SIZE = { w: 360, h: 220 };
  var PHONE_QUERY = '(max-width: 500px)';
  var API = '/api/forecast';

  // ---- pure state helpers ----
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
  // The /api/forecast query for a state (view is client-only; the window takes the compact table).
  function queryFor(s) {
    return new URLSearchParams({ station: s.station, tz: s.tz || '', unit: s.unit, model: s.model, compact: '1' }).toString();
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
    var m = min || MIN_SIZE, t = top || 0, pad = 8;
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
  // "GFS · run 20260926 12 UTC" / "SWAN · updated 20260925 23 UTC" for the window's header.
  function shortCycle(model, header) {
    var c = header && header.cycle ? String(header.cycle) : '';
    if (!c) return model || '';
    if (model === 'SWAN') { var m = /updated\s+(.*)$/i.exec(c); return 'SWAN · updated ' + (m ? m[1] : c); }
    return (model || 'GFS') + ' · run ' + c;
  }
  // The graph's label strings -> Date (the old page's rule: "8/30/25 6:00 AM", else the browser's parser).
  function parseLabel(lbl) {
    var m = /^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(\d{1,2}):(\d{2})\s*([APap][Mm])$/.exec(lbl || '');
    if (m) {
      var M = +m[1], D = +m[2], Y = m[3].length === 2 ? 2000 + +m[3] : +m[3], h = +m[4], mi = +m[5], ap = m[6].toLowerCase();
      if (ap === 'pm' && h < 12) h += 12;
      if (ap === 'am' && h === 12) h = 0;
      return new Date(Y, M - 1, D, h, mi, 0);
    }
    var d = new Date(lbl);
    return isNaN(d) ? new Date() : d;
  }
  // The x window for a range of days from the START of the series (the old page's rule).
  function rangeWindow(n, days) {
    var end = n - 1;
    if (Number.isFinite(days) && days > 0) end = Math.min(n - 1, Math.round(days * 24) - 1);
    return { min: 0, max: Math.max(0, end) };
  }

  // ---- the loader ----
  // deps: fetch(url, {signal}) -> Response-like {ok, status, json()}; now(); replaceState(url);
  //       swanStations (array); ui.apply(d, state), ui.busy(bool), ui.error(message | null, retry | null).
  function createLoader(deps, state) {
    var cache = new Map(), seq = 0, ctrl = null;
    function trim() { while (cache.size > CACHE_MAX) { var k = cache.keys().next().value; cache.delete(k); } }
    function normalise(s) {                                                    // never a SWAN request off the SWAN stations
      if (s.model === 'SWAN' && deps.swanStations && deps.swanStations.indexOf(s.station) < 0) s.model = 'GFS';
      return s;
    }
    function sync() { if (deps.replaceState) deps.replaceState(urlFor(state)); }
    function apply(d) {
      if (d && typeof d.model === 'string') state.model = d.model.toUpperCase() === 'SWAN' ? 'SWAN' : 'GFS';   // the server's echo is authoritative
      sync();
      deps.ui.apply(d, state);
    }
    function load(next) {
      Object.assign(state, next || {});
      normalise(state);
      sync();
      var key = keyOf(state), hit = cache.get(key), now = deps.now();
      if (hit && now - hit.ts < CACHE_TTL_MS) { if (ctrl) { ctrl.abort(); ctrl = null; } seq++; deps.ui.busy(false); deps.ui.error(null, null); apply(hit.d); return Promise.resolve(hit.d); }
      var my = ++seq;
      if (ctrl) ctrl.abort();
      var c = ctrl = new AbortController();
      deps.ui.busy(true); deps.ui.error(null, null);
      return deps.fetch(API + '?' + queryFor(state), { signal: c.signal }).then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      }).then(function (d) {
        if (my !== seq) return null;                                           // superseded: no DOM writes
        if (!d || typeof d !== 'object') throw new Error('bad forecast');
        cache.set(key, { d: d, ts: deps.now() }); trim();
        deps.ui.busy(false);
        apply(d);
        return d;
      }, function (err) {
        if (my !== seq || (err && err.name === 'AbortError')) return null;
        deps.ui.busy(false);
        deps.ui.error('Could not load the forecast.', function () { return load({}); });
        return null;
      });
    }
    // A forecast the page already rendered (render=full): cached and shown, no fetch.
    function seed(d) { cache.set(keyOf(state), { d: d, ts: deps.now() }); trim(); apply(d); }
    return { load: load, seed: seed, state: state, cache: cache, sync: sync };
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
  function formatMDHour(d) {
    var M = d.getMonth() + 1, D = d.getDate(), h = d.getHours();
    return h === 0 ? M + '/' + D + ' 12am' : h === 12 ? M + '/' + D + ' 12pm' : '';
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
      var major = new Set(), minor = new Set();
      parsed.forEach(function (d, i) { var h = d.getHours(); if (h === 0) major.add(i); else if (h === 12) minor.add(i); });
      var idx = function (c) { return c.tick && typeof c.tick.index === 'number' ? c.tick.index : c.index; };
      return { type: 'category',
        grid: { color: function (c) { var i = idx(c); return major.has(i) ? 'rgba(0,0,0,0.25)' : minor.has(i) ? 'rgba(0,0,0,0.15)' : 'rgba(0,0,0,0.08)'; },
                lineWidth: function (c) { var i = idx(c); return major.has(i) ? 1.4 : minor.has(i) ? 1.0 : 0.5; } },
        ticks: { autoSkip: false, maxRotation: 90, minRotation: 90, callback: function (v, i) { return formatMDHour(parsed[i] || new Date(NaN)); }, font: { size: 10 } } };
    }
    function dots(label, key, src, color) { return { label: label, data: src[key], borderColor: color, backgroundColor: color, showLine: false, spanGaps: false }; }
    function series(src, combined) {
      var ds = ['s1', 's2', 's3', 's4', 's5', 's6'].map(function (k, i) { return dots('Swell ' + (i + 1), k, src, COLORS[k]); });
      if (combined) ds.push({ label: 'Combined', data: src.combined, borderColor: COLORS.combined, backgroundColor: COLORS.combined, showLine: false, spanGaps: false, pointRadius: 1.6 });
      return ds;
    }
    function build(Chart, gd, parsed) {
      var shade = makeNightShade(parsed), xAxis = makeXAxis(parsed);
      var common = { responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false, axis: 'x' },
        layout: { padding: { top: 8, right: 8, bottom: 0, left: 8 } }, elements: { point: { radius: 1.6 }, line: { tension: 0.25, borderWidth: 0 } },
        plugins: { legend: { position: 'bottom', labels: { usePointStyle: true, boxWidth: 10, boxHeight: 6 } }, tooltip: { mode: 'nearest', intersect: false }, decimation: { enabled: false } },
        animation: false };
      var hArr = ['s1', 's2', 's3', 's4', 's5', 's6', 'combined'].map(function (k) { return gd.height[k]; });
      var hMax = padMax(maxAcross(hArr)); if (!Number.isFinite(hMax)) hMax = 1; var hStep = heightStep(hMax); hMax = niceCeil(hMax, hStep);
      var pArr = ['s1', 's2', 's3', 's4', 's5', 's6'].map(function (k) { return gd.period[k]; });
      var pMax = padMax(maxAcross(pArr)); if (!Number.isFinite(pMax)) pMax = 10; var pStep = periodStep(pMax); pMax = niceCeil(pMax, pStep);
      var dArr = ['s1', 's2', 's3', 's4', 's5', 's6'].map(function (k) { return gd.direction[k]; });
      var dMin = minAcross(dArr), dMax = maxAcross(dArr);
      if (!Number.isFinite(dMin) || !Number.isFinite(dMax) || dMin === dMax) { dMin = 0; dMax = 360; }
      function opts(title, y) {
        return Object.assign({}, common, { plugins: Object.assign({}, common.plugins, { title: { display: true, text: title }, nightShade: { nightStart: 18, nightEnd: 6, fill: 'rgba(0,0,0,0.06)' } }),
          scales: { x: xAxis, y: Object.assign({ grid: { display: true, color: 'rgba(0,0,0,0.08)' }, border: { color: 'rgba(0,0,0,0.2)' } }, y) } });
      }
      var specs = [
        ['Swell Height', series(gd.height, true), { beginAtZero: true, min: 0, max: hMax, ticks: { stepSize: hStep }, title: { display: true, text: 'Height (' + gd.units + ')' } }],
        ['Swell Period', series(gd.period, false), { beginAtZero: true, min: 0, max: pMax, ticks: { stepSize: pStep }, title: { display: true, text: 'Period (s)' } }],
        ['Swell Direction', series(gd.direction, false), { min: dMin, max: dMax, ticks: { stepSize: 45 }, title: { display: true, text: 'Direction (°)' } }]
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
        var active = ch.data.datasets.map(function (_, di) { return { datasetIndex: di, index: i }; });
        ch.setActiveElements(active);
        var xs = ch.scales && ch.scales.x, x = xs ? xs.getPixelForValue(i) : undefined;
        if (ch.tooltip) ch.tooltip.setActiveElements(active, { x: x });
        ch.update('none');
      }
      function clear(ch) { if (!ch) return; ch.setActiveElements([]); if (ch.tooltip) ch.tooltip.setActiveElements([], {}); ch.update('none'); }
      function broadcast(src, evt) {
        var pts = src.getElementsAtEventForMode(evt, 'index', { intersect: false, axis: 'x' }, true);
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
      var w = rangeWindow(data.labels.length, v === '7' ? 7 : v === '3' ? 3 : 0);
      charts.forEach(function (ch) { ch.options.scales.x.min = w.min; ch.options.scales.x.max = w.max; ch.update('none'); });
      if (deps.rangeBar) Array.prototype.forEach.call(deps.rangeBar.querySelectorAll('[data-days]'), function (b) {
        var on = (b.getAttribute('data-days') === (v === '7' ? '7' : v === '3' ? '3' : '0'));
        b.classList.toggle('active', on); b.setAttribute('aria-pressed', on ? 'true' : 'false');
      });
    }
    function fit() {
      var h = Math.max(180, Math.min(360, Math.floor((deps.bodyHeight() - 60) / 3)));
      deps.boxes.forEach(function (b) { b.style.height = h + 'px'; });
      charts.forEach(function (c) { try { c.resize(); } catch (e) {} });
    }
    function render() {
      if (rendering) return rendering;
      if (!data) { destroy(); return Promise.resolve(); }
      var gd = data;
      rendering = deps.loadChartJs().then(function () {
        rendering = null;
        if (data !== gd || !deps.visible()) return;                            // moved on, or hidden while Chart.js loaded
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
  // deps: el (#forecastWin), header, handle, storage (sessionStorage-like), win (window-like: innerWidth,
  //       innerHeight, matchMedia, addEventListener), topBarHeight() -> px, onMode(mode), onResize().
  function FloatingWindow(deps) {
    this.d = deps; this.el = deps.el;
    var saved = readJson(deps.storage, WINDOW_KEY);
    this.geom = typeof saved.w === 'number' && typeof saved.h === 'number' && typeof saved.x === 'number' && typeof saved.y === 'number' ? { x: saved.x, y: saved.y, w: saved.w, h: saved.h } : null;
    this.mode = saved.mode === 'normal' || saved.mode === 'max' ? saved.mode : 'min';   // a new tab starts minimised
    this.prev = saved.prev === 'max' ? 'max' : 'normal';
    this.opener = null;
    this._applyMode();
    if (this.geom) this._place(clampGeometry(this.geom, deps.win.innerWidth, deps.win.innerHeight, deps.topBarHeight()));
    this._bind();
  }
  FloatingWindow.prototype.isPhone = function () { var mm = this.d.win.matchMedia; return !!(mm && mm.call(this.d.win, PHONE_QUERY).matches); };
  FloatingWindow.prototype._save = function () {
    var g = this.geom || {};
    writeJson(this.d.storage, WINDOW_KEY, { x: g.x, y: g.y, w: g.w, h: g.h, mode: this.mode, prev: this.prev });
  };
  FloatingWindow.prototype._place = function (g) {
    this.geom = g;
    var s = this.el.style;
    s.left = g.x + 'px'; s.top = g.y + 'px'; s.width = g.w + 'px'; s.height = g.h + 'px'; s.right = 'auto'; s.bottom = 'auto';
  };
  FloatingWindow.prototype._applyMode = function () {
    var cl = this.el.classList;
    cl.toggle('fw-min', this.mode === 'min'); cl.toggle('fw-max', this.mode === 'max');
    this.el.style.setProperty('--topbar-h', this.d.topBarHeight() + 'px');
    if (this.d.onMode) this.d.onMode(this.mode);
  };
  FloatingWindow.prototype.setMode = function (mode) {
    if (mode !== 'min' && mode !== 'max' && mode !== 'normal') return;
    if (mode === 'min' && this.mode !== 'min') this.prev = this.mode;
    this.mode = mode; this._applyMode(); this._save();
    if (mode !== 'min' && this.d.onResize) this.d.onResize();
  };
  FloatingWindow.prototype.minimise = function () { this.setMode('min'); };
  FloatingWindow.prototype.expand = function () { if (this.mode === 'min') this.setMode(this.prev); };
  FloatingWindow.prototype.toggleMax = function () { this.setMode(this.mode === 'max' ? 'normal' : 'max'); };
  // the window's current box (from CSS until the first drag or resize)
  FloatingWindow.prototype._rect = function () {
    if (this.geom) return this.geom;
    var r = this.el.getBoundingClientRect();
    return { x: r.left, y: r.top, w: r.width, h: r.height };
  };
  FloatingWindow.prototype.clamp = function () {
    if (!this.geom || this.isPhone()) return;
    this._place(clampGeometry(this.geom, this.d.win.innerWidth, this.d.win.innerHeight, this.d.topBarHeight()));
    this._save();
  };
  FloatingWindow.prototype._bind = function () {
    var self = this, d = this.d;
    function drag(startEv, kind) {
      if (self.isPhone() || self.mode === 'max') return;
      var r = self._rect(), sx = startEv.clientX, sy = startEv.clientY, moved = false;
      var target = startEv.currentTarget;
      if (target.setPointerCapture) try { target.setPointerCapture(startEv.pointerId); } catch (e) {}
      function onMove(ev) {
        var dx = ev.clientX - sx, dy = ev.clientY - sy;
        if (!moved && Math.abs(dx) + Math.abs(dy) < 2) return;
        moved = true;
        var g = kind === 'move' ? { x: r.x + dx, y: r.y + dy, w: r.w, h: r.h } : { x: r.x, y: r.y, w: r.w + dx, h: r.h + dy };
        self._place(clampGeometry(g, d.win.innerWidth, d.win.innerHeight, d.topBarHeight()));
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
      if (e.target && e.target.closest && e.target.closest('button, a, input, select')) return;
      if (self.mode === 'min') return;                                         // the minimised bar is not dragged (a click expands it)
      drag(e, 'move');
    });
    d.header.addEventListener('dblclick', function (e) {
      if (e.target && e.target.closest && e.target.closest('button, a, input, select')) return;
      if (self.isPhone()) return;
      if (self.mode === 'min') self.expand(); else self.toggleMax();
    });
    if (d.handle) d.handle.addEventListener('pointerdown', function (e) { if (self.mode === 'normal') drag(e, 'resize'); });
    d.win.addEventListener('resize', function () { self.clamp(); if (self.mode !== 'min' && d.onResize) d.onResize(); });
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

  // ---- init: wire everything to the page ----
  // opts: initial (window.__initial), stationLabel(sid) -> text, loadChartJs() -> Promise, closeLivePanel(),
  //       liveOpen() -> bool, fetch, document, window, storage (session), settings (local), onMode(mode).
  var app = null;
  function init(opts) {
    opts = opts || {};
    var win = opts.window || window, doc = opts.document || win.document;
    if (!doc || !doc.getElementById) return null;
    var $ = function (id) { return doc.getElementById(id); };
    var els = { win: $('forecastWin'), header: $('fwHeader'), title: $('fwTitle'), cycle: $('fwCycle'), busy: $('fwBusy'), min: $('fwMin'), max: $('fwMax'),
      viewBar: $('viewBar'), modelBar: $('modelBar'), rangeBar: $('rangeBar'), body: $('fwBody'), error: $('fwError'), meta: $('forecastMeta'),
      table: $('forecastTable'), graphs: $('graphs'), handle: $('fwResize'), topBar: $('topBar'), tz: $('tz'), unit: $('unit'), station: $('station'), trigger: $('stationTrigger') };
    if (!els.win || !els.body || !els.table) return null;
    var session = opts.storage || win.sessionStorage, local = opts.settings || win.localStorage;
    var initial = opts.initial || {}, swanStations = initial.swan_stations || [];
    var state = resolveInitialState((win.location && win.location.search) || '', readJson(local, SETTINGS_KEY), initial);
    var fw = new FloatingWindow({ el: els.win, header: els.header, handle: els.handle, storage: session, win: win,
      topBarHeight: function () { return els.topBar ? els.topBar.offsetHeight : 0; },
      onMode: function (m) {
        if (els.min) { els.min.setAttribute('aria-expanded', m === 'min' ? 'false' : 'true'); els.min.setAttribute('aria-label', m === 'min' ? 'Expand forecast' : 'Minimise forecast'); els.min.textContent = m === 'min' ? '▴' : '–'; }
        if (els.max) els.max.setAttribute('aria-pressed', m === 'max' ? 'true' : 'false');
        if (opts.onMode) opts.onMode(m);
      },
      onResize: function () { if (state.view === 'Graph') graphs.resize(); } });
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
    function clearNode(el) { while (el.firstChild) el.removeChild(el.firstChild); }
    function showError(msg, retry) {
      var box = els.error; if (!box) return;
      clearNode(box);
      if (!msg) { box.hidden = true; return; }
      box.appendChild(doc.createTextNode(msg + ' '));
      if (retry) { var b = doc.createElement('button'); b.type = 'button'; b.className = 'btn btn-sm btn-outline-secondary'; b.textContent = 'Retry'; b.addEventListener('click', function () { retry(); }); box.appendChild(b); }
      box.hidden = false;
    }
    function setMeta(h) {
      var m = els.meta; if (!m) return;
      clearNode(m);
      if (!h) return;
      [['Cycle : ', h.cycle], ['Location : ', h.location], ['Time Zone: ', h.tz]].forEach(function (pair, i) {
        if (i) m.appendChild(doc.createTextNode('  |  '));
        var b = doc.createElement('strong'); b.textContent = pair[0]; m.appendChild(b);
        m.appendChild(doc.createTextNode(String(pair[1] == null ? '' : pair[1])));
      });
    }
    var loader = createLoader({ fetch: opts.fetch || function (u, o) { return win.fetch(u, o); }, now: function () { return Date.now(); },
      replaceState: function (url) { try { win.history.replaceState(null, '', url); } catch (e) {} }, swanStations: swanStations,
      ui: { busy: function (on) { if (els.busy) els.busy.hidden = !on; els.body.setAttribute('aria-busy', on ? 'true' : 'false'); },
            error: showError,
            apply: function (d, st) {
              var avail = typeof d.swan_available === 'boolean' ? d.swan_available : swanStations.indexOf(st.station) >= 0;
              if (els.modelBar) { els.modelBar.hidden = !avail; pressed(els.modelBar, 'data-model', st.model); }
              text(els.title, opts.stationLabel ? opts.stationLabel(st.station) : st.station);
              text(els.cycle, shortCycle(st.model, d.graph_header));
              setMeta(d.graph_header);
              if (d.table_html) els.table.innerHTML = d.table_html;           // the server's own table (build_html_table)
              else { clearNode(els.table); var p = doc.createElement('div'); p.className = 'text-muted'; p.textContent = d.error || 'No forecast available.'; els.table.appendChild(p); }
              if (d.error && d.table_html) showError(String(d.error), null); else if (d.error) showError(null, null);
              graphs.setData(d.graph_data);
              syncSelects();
            } } }, state);
    function syncSelects() {
      if (els.station && els.station.value !== state.station) els.station.value = state.station;
      if (els.tz) {
        els.tz.value = state.tz;
        if (els.tz.value !== state.tz && state.tz) { var o = doc.createElement('option'); o.value = state.tz; o.textContent = state.tz; els.tz.appendChild(o); els.tz.value = state.tz; }
      }
      if (els.unit && els.unit.value !== state.unit) els.unit.value = state.unit;
    }
    function setView(v) {
      state.view = v === 'Graph' ? 'Graph' : 'Table';
      pressed(els.viewBar, 'data-view', state.view);
      var g = state.view === 'Graph';
      els.table.hidden = g; if (els.graphs) els.graphs.hidden = !g; if (els.rangeBar) els.rangeBar.hidden = !g;
      loader.sync();
      return g ? graphs.show() : Promise.resolve();
    }
    function expand() {
      if (fw.mode === 'min') { fw.opener = doc.activeElement; fw.expand(); }
      if (state.view === 'Graph') graphs.show();
      if (els.header && els.header.focus) els.header.focus({ preventScroll: true });
    }
    function minimise() {
      var back = fw.opener && fw.opener.isConnected ? fw.opener : els.trigger;
      fw.minimise();
      if (back && back.focus) try { back.focus({ preventScroll: true }); } catch (e) {}
      fw.opener = null;
    }
    // controls
    if (els.min) els.min.addEventListener('click', function () { if (fw.mode === 'min') expand(); else minimise(); });
    if (els.max) els.max.addEventListener('click', function () { if (fw.mode === 'min') { fw.prev = 'max'; expand(); } else { fw.toggleMax(); if (state.view === 'Graph') graphs.resize(); } });
    if (els.header) els.header.addEventListener('click', function (e) {
      if (fw.mode === 'min' && !(e.target && e.target.closest && e.target.closest('button'))) expand();
    });
    if (els.viewBar) els.viewBar.addEventListener('click', function (e) { var b = e.target && e.target.closest ? e.target.closest('[data-view]') : null; if (b) setView(b.getAttribute('data-view')); });
    if (els.modelBar) els.modelBar.addEventListener('click', function (e) { var b = e.target && e.target.closest ? e.target.closest('[data-model]') : null; if (b) loader.load({ model: b.getAttribute('data-model') === 'SWAN' ? 'SWAN' : 'GFS' }); });
    function saveSettings() { writeJson(local, SETTINGS_KEY, { tz: state.tz, unit: state.unit }); }
    if (els.tz) els.tz.addEventListener('change', function () { loader.load({ tz: els.tz.value || '' }); saveSettings(); });
    if (els.unit) els.unit.addEventListener('change', function () { loader.load({ unit: UNITS[els.unit.value] ? els.unit.value : 'US' }); saveSettings(); });
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
      loader.seed({ table_html: els.table.innerHTML, graph_data: initial.graph_data || null, graph_header: initial.graph_header || null,
        error: initial.error || null, model: initial.model, swan_available: initial.swan_available });
    } else loader.load({});
    app = { state: state, loader: loader, window: fw, graphs: graphs, settings: settings, setView: setView, expand: expand, minimise: minimise, els: els };
    return app;
  }

  window.AllshoreForecast = {
    init: init,
    load: function (next) { return app ? app.loader.load(next) : Promise.resolve(null); },
    expand: function () { if (app) app.expand(); },
    minimise: function () { if (app) app.minimise(); },
    getMode: function () { return app ? app.window.mode : null; },
    getState: function () { return app ? Object.assign({}, app.state) : null; },
    _internals: {
      SETTINGS_KEY: SETTINGS_KEY, WINDOW_KEY: WINDOW_KEY, RANGE_KEY: RANGE_KEY, CACHE_MAX: CACHE_MAX, CACHE_TTL_MS: CACHE_TTL_MS, MIN_SIZE: MIN_SIZE, PHONE_QUERY: PHONE_QUERY,
      resolveInitialState: resolveInitialState, queryFor: queryFor, urlFor: urlFor, keyOf: keyOf,
      clampGeometry: clampGeometry, readJson: readJson, writeJson: writeJson, shortCycle: shortCycle, parseLabel: parseLabel, rangeWindow: rangeWindow,
      createLoader: createLoader, createForecastGraphs: createForecastGraphs, FloatingWindow: FloatingWindow, createSettings: createSettings,
      app: function () { return app; }
    }
  };
})();
