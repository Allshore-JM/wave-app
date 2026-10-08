/* Allshore Surf tide station view (plan section 38). Loaded on every page; touches no DOM until createTideView(...)
 * is called with the tide window's elements, so tests/ui/tides.test.js evaluates it in Node with fakes.
 *
 * One station at a time: load(station) asks the server (/api/tides/<id>: NOAA's predictions, the curve every 30 min,
 * the predicted highs and lows, the nights), draws one chart and lists the highs and lows of the range shown.
 *  - The time axis is LINEAR in hours since today's local midnight in the display zone (the site's #tz, else the
 *    station's own zone), so 3-hourly gaps, observations and the now line all sit at their true times; its ticks are
 *    local clock hours (midnights with the date, then noon / 6 AM / 6 PM as room allows), found by reading the clock in
 *    the zone, so 23- and 25-hour days come out right.
 *  - Tabs 3 d / 7 d / 16 d (remembered for the tab in sessionStorage) end at the 3rd / 7th / 16th local midnight.
 *  - Heights above MLLW in the site's unit (US: ft, one decimal; Metric: m, two). Nights shaded (last light to first
 *    light, the forecast graphs' rule), a dashed line at the current time (redrawn every minute), and for a station
 *    with a gauge the observed water level of the last 48 h (asked after the curve is drawn; a failure is only a note).
 *  - A busy or unreachable server (503 + retry) is asked again after its Retry-After; a station NOAA has no
 *    predictions for says so (no retry); any other failure offers Retry.
 *  - A newer load() makes every answer of an older one void (sequence + AbortController), and a chart is only built
 *    while the window shows its body (show() builds a deferred one).
 */
(function () {
  'use strict';

  var RANGE_KEY = 'allshore.tideRange.v1';
  var RANGES = ['3', '7', '16'];
  var DEFAULT_RANGE = '3';
  var FT_PER_M = 3.28084;
  var HOUR_MS = 3600000;
  var BOX_MIN = 220, BOX_MAX = 420, BOX_CHROME = 120;       // the chart box: the body minus the tabs, list header, margins
  var MIN_TICK_PX = 40;                                     // hour ticks no closer than this
  var LABEL_PX = { full: 64, short: 38 };                   // "Thu 10/8" / "10/8"
  var RETRY_MAX = 24, RETRY_DEFAULT_S = 5, RETRY_MAX_S = 60;
  var NOW_REDRAW_MS = 60000;
  var COLORS = { predicted: 'rgba(29,111,214,1)', fill: 'rgba(29,111,214,0.10)', extreme: 'rgba(11,61,145,1)',
                 observed: 'rgba(232,89,12,0.95)', night: 'rgba(30,60,110,0.10)', now: 'rgba(214,51,108,0.9)' };
  var WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  var METHOD_TEXT = {
    harmonic: 'Curve: NOAA tide predictions',
    reference: 'Curve: the predictions of NOAA station %REF% shaped between this station’s predicted highs and lows (NOAA subordinate station)',
    cosine: 'Curve: drawn between NOAA’s predicted highs and lows (NOAA subordinate station)'
  };
  var MSG = {
    unknown: 'This tide station is not known.',
    failed: 'The tide predictions could not be loaded.',
    busy: 'The tide predictions are not available yet; try again in a moment.'
  };

  // ---- clocks in a zone ------------------------------------------------------------------------------------------
  // Cache keys are prefixed: a zone string such as "constructor" never hits an Object member.
  var FMT_CACHE = {};
  function zoneClock(tz) {
    var key = 'z:' + (tz || '');
    if (FMT_CACHE[key]) return FMT_CACHE[key];
    var out = buildClock(tz); FMT_CACHE[key] = out; return out;
  }
  function buildClock(tz) {
    var f = null, zone = 'UTC';
    var o = { hourCycle: 'h23', weekday: 'short', year: 'numeric', month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' };
    try { f = new Intl.DateTimeFormat('en-US', Object.assign({ timeZone: tz || 'UTC' }, o)); zone = tz || 'UTC'; }
    catch (e) { try { f = new Intl.DateTimeFormat('en-US', Object.assign({ timeZone: 'UTC' }, o)); } catch (e2) { f = null; } }
    function parts(ms) {
      var d = new Date(ms);
      if (!f) return { y: d.getUTCFullYear(), mo: d.getUTCMonth() + 1, d: d.getUTCDate(), h: d.getUTCHours(), mi: d.getUTCMinutes(), wd: WEEKDAYS[d.getUTCDay()] };
      var got = {}, p = f.formatToParts(d);
      for (var i = 0; i < p.length; i++) got[p[i].type] = p[i].value;
      return { y: +got.year, mo: +got.month, d: +got.day, h: (+got.hour) % 24, mi: +got.minute, wd: got.weekday };   // some engines print "24" at midnight
    }
    return { zone: zone, parts: parts };
  }
  // The local midnight at or before ms: the first guess subtracts the clock time; on a clock-change day the time
  // elapsed since midnight differs from the clock reading by the shift, which one correction fixes.
  function localMidnightBefore(ms, clk) {
    var p = clk.parts(ms), cand = ms - (p.h * 60 + p.mi) * 60000 - (new Date(ms).getUTCSeconds() * 1000 + new Date(ms).getUTCMilliseconds());
    p = clk.parts(cand);
    if (p.h || p.mi) cand += p.h >= 12 ? (1440 - p.h * 60 - p.mi) * 60000 : -(p.h * 60 + p.mi) * 60000;
    return cand;
  }
  // count + 1 local midnights from the one at or before ms (index k = the start of day k)
  function midnights(ms, count, clk) {
    var out = [localMidnightBefore(ms, clk)];
    for (var k = 0; k < count; k++) out.push(localMidnightBefore(out[k] + 36 * HOUR_MS, clk));
    return out;
  }
  function clockText(p, compact) {
    var h12 = p.h % 12 || 12, mi = p.mi ? ':' + (p.mi < 10 ? '0' : '') + p.mi : '';
    if (compact && !p.mi && p.h === 12) return 'Noon';
    return h12 + (compact ? mi : ':' + (p.mi < 10 ? '0' : '') + p.mi) + ' ' + (p.h < 12 ? 'AM' : 'PM');
  }
  function dateText(p, weekday) { return (weekday && p.wd ? p.wd + ' ' : '') + p.mo + '/' + p.d; }

  // The axis marks between two local midnights: every local clock hour 0 / 6 / 12 / 18 (read from the clock, so a
  // 23- or 25-hour day keeps its marks at the right instants). -> [{ms, x (hours since origin), h, p}]
  function hourMarks(origin, end, clk) {
    var out = [];
    for (var ms = origin; ms <= end + 1; ) {
      var p = clk.parts(ms);
      if (p.mi !== 0) { ms += (60 - p.mi) * 60000; continue; }             // after a 30-minute clock change: the next :00
      if (p.h % 6 === 0) out.push({ ms: ms, x: (ms - origin) / HOUR_MS, h: p.h, p: p });
      ms += HOUR_MS;
    }
    return out;
  }
  // The finest step (6, 12 or 24 h) whose marks are at least MIN_TICK_PX apart over this width, and the date labels:
  // every n-th midnight so labels do not collide (with the weekday when there is room).
  function tickPlan(days, widthPx) {
    var perDay = (widthPx > 0 ? widthPx : 600) / Math.max(1, days);
    var step = perDay / 4 >= MIN_TICK_PX ? 6 : perDay / 2 >= MIN_TICK_PX ? 12 : 24;
    var weekday = perDay >= LABEL_PX.full;
    var every = Math.max(1, Math.ceil((weekday ? LABEL_PX.full : LABEL_PX.short) / perDay));
    return { step: step, weekday: weekday, every: every };
  }
  function tickLabel(mark, plan, dayIndex) {
    if (mark.h === 0) return dayIndex % plan.every === 0 ? dateText(mark.p, plan.weekday) : '';
    return plan.step < 24 && mark.h % plan.step === 0 ? clockText(mark.p, true) : '';
  }

  // ---- the data ----------------------------------------------------------------------------------------------------
  function num(v) { return typeof v === 'number' && isFinite(v); }        // isFinite(null) is true: a gap is no 0
  function rangeOf(v) { v = String(v); return RANGES.indexOf(v) >= 0 ? v : DEFAULT_RANGE; }
  function readRange(storage) { try { return rangeOf(storage.getItem(RANGE_KEY)); } catch (e) { return DEFAULT_RANGE; } }
  function unitOf(u) { return u === 'Metric' ? 'Metric' : 'US'; }
  function height(m, unit) { return unit === 'Metric' ? m : m * FT_PER_M; }
  function heightText(m, unit) { return unit === 'Metric' ? height(m, unit).toFixed(2) + ' m' : height(m, unit).toFixed(1) + ' ft'; }

  // Chart points of the predicted curve, the extremes and the observations: x in hours since origin, y in the unit.
  function points(d, obs, origin, unit) {
    var curve = [], ext = [], seen = [];
    var b = d.begin * 1000, step = d.step * 1000;
    for (var i = 0; i < (d.v || []).length; i++) {
      var v = d.v[i];
      curve.push({ x: (b + i * step - origin) / HOUR_MS, y: num(v) ? height(v, unit) : null });
    }
    (d.hilo || []).forEach(function (e) {
      if (!e || !num(e[0]) || !num(e[1])) return;
      ext.push({ x: (e[0] * 1000 - origin) / HOUR_MS, y: height(+e[1], unit), k: e[2] === 'H' ? 'H' : 'L', m: +e[1], t: e[0] * 1000 });
    });
    if (obs && obs.t && obs.v) for (var j = 0; j < obs.t.length; j++) {
      if (num(obs.t[j]) && num(obs.v[j])) seen.push({ x: (obs.t[j] * 1000 - origin) / HOUR_MS, y: height(+obs.v[j], unit) });
    }
    return { curve: curve, ext: ext, seen: seen };
  }
  // The nights as [x0, x1] in hours since origin
  function nightSpans(d, origin) {
    return (d.night || []).filter(function (n) { return n && num(n[0]) && num(n[1]) && n[1] > n[0]; })
      .map(function (n) { return [(n[0] * 1000 - origin) / HOUR_MS, (n[1] * 1000 - origin) / HOUR_MS]; });
  }
  // The highs and lows between two instants: [{ms, kind, m}] in time order
  function extremesBetween(d, from, to) {
    return (d.hilo || []).filter(function (e) { return e && num(e[0]) && num(e[1]) && e[0] * 1000 >= from && e[0] * 1000 < to; })
      .map(function (e) { return { ms: e[0] * 1000, kind: e[2] === 'H' ? 'High' : 'Low', m: +e[1] }; });
  }

  // ---- plugins -------------------------------------------------------------------------------------------------
  function makeNightShade(spans) {
    return { id: 'tideNights', beforeDraw: function (chart) {
      var ctx = chart.ctx, area = chart.chartArea, x = chart.scales && chart.scales.x;
      if (!ctx || !area || !x) return;
      ctx.save();
      ctx.fillStyle = COLORS.night;
      spans.forEach(function (s) {
        var x0 = Math.max(area.left, x.getPixelForValue(s[0])), x1 = Math.min(area.right, x.getPixelForValue(s[1]));
        if (isFinite(x0) && isFinite(x1) && x1 > x0) ctx.fillRect(x0, area.top, x1 - x0, area.bottom - area.top);
      });
      ctx.restore();
    } };
  }
  function makeNowLine(origin, now) {
    return { id: 'tideNow', afterDatasetsDraw: function (chart) {
      var ctx = chart.ctx, area = chart.chartArea, x = chart.scales && chart.scales.x;
      if (!ctx || !area || !x) return;
      var px = x.getPixelForValue((now() - origin) / HOUR_MS);
      if (!isFinite(px) || px < area.left || px > area.right) return;
      ctx.save();
      ctx.strokeStyle = COLORS.now; ctx.lineWidth = 1; if (ctx.setLineDash) ctx.setLineDash([4, 3]);
      ctx.beginPath(); ctx.moveTo(px, area.top); ctx.lineTo(px, area.bottom); ctx.stroke();
      if (ctx.setLineDash) ctx.setLineDash([]);
      ctx.font = '10px system-ui, -apple-system, "Segoe UI", sans-serif'; ctx.fillStyle = COLORS.now; ctx.textAlign = 'left'; ctx.textBaseline = 'top';
      ctx.fillText('now', px + 3, area.top + 2);
      ctx.restore();
    } };
  }

  // ---- the chart's configuration (pure: tests read it) --------------------------------------------------------------
  function chartConfig(d, obs, view) {
    var unit = unitOf(view.unit), clk = zoneClock(view.zone), days = +view.range;
    var mids = midnights(view.now(), 16, clk), origin = mids[0];
    var pts = points(d, obs, origin, unit), marks = hourMarks(origin, mids[16], clk);
    var dayOf = function (ms) { for (var k = mids.length - 1; k >= 0; k--) if (ms >= mids[k]) return k; return 0; };
    var markAt = {};
    marks.forEach(function (m) { markAt[m.x.toFixed(4)] = m; });
    var unitText = unit === 'Metric' ? 'm' : 'ft';
    var datasets = [
      { label: 'Predicted', data: pts.curve, parsing: false, borderColor: COLORS.predicted, backgroundColor: COLORS.fill, fill: 'start',
        borderWidth: 1.8, pointRadius: 0, pointHitRadius: 4, tension: 0.3, spanGaps: false, order: 2 },
      { label: 'High / low', data: pts.ext, parsing: false, showLine: false, borderColor: COLORS.extreme, backgroundColor: COLORS.extreme,
        pointRadius: 3.5, pointHoverRadius: 5, order: 1 }
    ];
    if (pts.seen.length) datasets.push({ label: 'Observed', data: pts.seen, parsing: false, borderColor: COLORS.observed,
      backgroundColor: COLORS.observed, borderWidth: 1.5, borderDash: [5, 3], pointRadius: 0, pointHitRadius: 3, spanGaps: false, order: 0 });
    var x = {
      type: 'linear', min: 0, max: (mids[days] - origin) / HOUR_MS,
      afterBuildTicks: function (axis) {
        var lo = axis.min, hi = axis.max;
        axis.ticks = marks.filter(function (m) { return m.x >= lo - 1e-6 && m.x <= hi + 1e-6; }).map(function (m) { return { value: m.x }; });
      },
      ticks: { autoSkip: false, maxRotation: 0, minRotation: 0, includeBounds: false, font: { size: 10 },
        callback: function (value) {
          var m = markAt[(+value).toFixed(4)];
          if (!m) return '';
          var span = this && isFinite(this.max) && isFinite(this.min) ? (this.max - this.min) / 24 : days;
          var plan = tickPlan(span, this && this.width > 0 ? this.width : 600);
          return tickLabel(m, plan, dayOf(m.ms));
        } },
      grid: {
        color: function (c) { var m = c.tick && markAt[(+c.tick.value).toFixed(4)]; return !m ? 'rgba(0,0,0,0)' : m.h === 0 ? 'rgba(0,0,0,0.25)' : m.h === 12 ? 'rgba(0,0,0,0.12)' : 'rgba(0,0,0,0.06)'; },
        lineWidth: function (c) { var m = c.tick && markAt[(+c.tick.value).toFixed(4)]; return m && m.h === 0 ? 1.3 : 0.8; } }
    };
    var y = { grace: '8%', grid: { color: 'rgba(0,0,0,0.08)' }, border: { color: 'rgba(0,0,0,0.2)' },
      title: { display: true, text: 'Height (' + unitText + ', above MLLW)' } };   // Chart.js's own labels (0.5 steps stay 0.5)
    return {
      type: 'line',
      data: { datasets: datasets },
      plugins: [makeNightShade(nightSpans(d, origin)), makeNowLine(origin, view.now)],
      options: {
        responsive: true, maintainAspectRatio: false, animation: false, normalized: true,
        interaction: { mode: 'nearest', axis: 'x', intersect: false },
        layout: { padding: { top: 4, right: 8, bottom: 0, left: 4 } },
        scales: { x: x, y: y },
        plugins: {
          legend: { position: 'top', align: 'start', labels: { usePointStyle: true, boxWidth: 8, boxHeight: 6, padding: 8, font: { size: 11 },
            sort: function (a, b) { return a.datasetIndex - b.datasetIndex; } } },   // Predicted, High / low, Observed (not the drawing order)
          decimation: { enabled: false },
          tooltip: { callbacks: {
            title: function (items) {
              if (!items || !items.length) return '';
              var p = clk.parts(origin + items[0].parsed.x * HOUR_MS);
              return p.wd + ' ' + p.mo + '/' + p.d + ', ' + clockText(p, false);
            },
            label: function (item) {
              var raw = item.raw || {}, v = item.parsed.y;
              var name = raw.k ? (raw.k === 'H' ? 'High' : 'Low') : item.dataset.label;
              return name + ': ' + (unit === 'Metric' ? (+v).toFixed(2) + ' m' : (+v).toFixed(1) + ' ft');
            } } }
        }
      },
      _view: { origin: origin, mids: mids, marks: marks }
    };
  }

  // ---- the view ----------------------------------------------------------------------------------------------------
  function createTideView(deps) {
    var els = deps.els, doc = deps.document;
    var now = deps.now || function () { return Date.now(); };
    var timers = deps.timers || { set: function (f, ms) { return setTimeout(f, ms); }, clear: function (id) { clearTimeout(id); } };
    var st = { seq: 0, station: null, data: null, obs: null, unit: 'US', zone: null, chart: null, dirty: false,
               rendering: null, ac: null, retryTimer: null, nowTimer: null, tries: 0, status: 'idle' };
    if (deps.unit) st.unit = unitOf(deps.unit);

    function range() { return readRange(deps.storage); }
    function zone() { return st.zone || (st.station && st.station.tz) || (st.data && st.data.tz) || 'UTC'; }
    function show(el, on) { if (el) el.classList.toggle('d-none', !on); }
    function setStatus(s, text, retry) {
      st.status = s;
      show(els.loading, s === 'loading');
      show(els.error, s === 'error' || s === 'final');
      show(els.content, s === 'ready');
      if (els.errorText) els.errorText.textContent = text || '';
      if (els.retry) els.retry.hidden = !retry;
    }
    function stopTimers() {
      if (st.retryTimer !== null) { timers.clear(st.retryTimer); st.retryTimer = null; }
      if (st.nowTimer !== null) { timers.clear(st.nowTimer); st.nowTimer = null; }
    }
    function destroyChart() {
      if (st.chart) { try { st.chart.destroy(); } catch (e) {} st.chart = null; }
      if (st.nowTimer !== null) { timers.clear(st.nowTimer); st.nowTimer = null; }
    }
    function pressRange(v) {
      if (!els.rangeBar) return;
      Array.prototype.forEach.call(els.rangeBar.querySelectorAll('[data-days]'), function (b) {
        var on = b.getAttribute('data-days') === v;
        b.classList.toggle('active', on); b.setAttribute('aria-pressed', on ? 'true' : 'false');
      });
    }
    function fit() {
      var h = Math.max(BOX_MIN, Math.min(BOX_MAX, Math.floor((deps.bodyHeight ? deps.bodyHeight() : 0) - BOX_CHROME)));
      if (els.box) els.box.style.height = h + 'px';
      if (st.chart) { try { st.chart.resize(); } catch (e) {} }
    }
    function zoneText(ms) {
      try { if (deps.zoneAbbr) return deps.zoneAbbr(ms, zone()) || zone(); } catch (e) {}
      return zone();
    }
    function writeMeta() {
      var d = st.data;
      if (!els.meta || !d) return;
      els.meta.textContent = '';
      var lines = ['Heights above mean lower low water (MLLW) · times in ' + zoneText(now()),
                   (METHOD_TEXT[d.method] || METHOD_TEXT.harmonic).replace('%REF%', d.ref || ''),
                   'Predictions do not include storm surge or wind effects.'];
      if (st.obs && st.obs.note) lines.push(st.obs.note + '.');
      lines.forEach(function (t) { var p = doc.createElement('div'); p.textContent = t; els.meta.appendChild(p); });
      var src = doc.createElement('div');
      var a = doc.createElement('a');
      a.setAttribute('href', 'https://tidesandcurrents.noaa.gov/noaatidepredictions.html?id=' + encodeURIComponent(d.id));
      a.setAttribute('target', '_blank'); a.setAttribute('rel', 'noopener');
      a.textContent = 'NOAA CO-OPS station ' + d.id;
      src.appendChild(doc.createTextNode('Source: ')); src.appendChild(a);
      els.meta.appendChild(src);
    }
    function writeList() {
      var d = st.data;
      if (!els.hilo || !d) return;
      els.hilo.textContent = '';
      var clk = zoneClock(zone()), days = +range(), mids = midnights(now(), days, clk);
      var rows = extremesBetween(d, mids[0], mids[days]);
      var table = doc.createElement('table');
      table.className = 'table table-sm tide-hilo';
      var head = doc.createElement('tr');
      ['Day', 'Time', 'Tide', 'Height'].forEach(function (h) { var th = doc.createElement('th'); th.textContent = h; head.appendChild(th); });
      var thead = doc.createElement('thead'); thead.appendChild(head); table.appendChild(thead);
      var body = doc.createElement('tbody'), lastDay = '';
      rows.forEach(function (r) {
        var p = clk.parts(r.ms), day = dateText(p, true), tr = doc.createElement('tr');
        tr.className = r.kind === 'High' ? 'tide-high' : 'tide-low';
        [day === lastDay ? '' : day, clockText(p, false), r.kind, heightText(r.m, unitOf(st.unit))].forEach(function (t) {
          var td = doc.createElement('td'); td.textContent = t; tr.appendChild(td);
        });
        lastDay = day;
        body.appendChild(tr);
      });
      table.appendChild(body);
      els.hilo.appendChild(table);
    }
    function scheduleNow() {
      if (st.nowTimer !== null) timers.clear(st.nowTimer);
      st.nowTimer = timers.set(function tick() {
        st.nowTimer = null;
        if (!st.chart) return;
        try { st.chart.draw(); } catch (e) {}
        st.nowTimer = timers.set(tick, NOW_REDRAW_MS);
      }, NOW_REDRAW_MS);
    }
    // One build at a time; the build reads the station's CURRENT data, observations, unit, zone and range when it runs
    // (after Chart.js has loaded), so a change while it waits is drawn by it.
    function render() {
      if (st.rendering) return st.rendering;
      if (!st.data) { destroyChart(); return Promise.resolve(); }
      st.rendering = Promise.resolve(deps.loadChartJs()).then(function () {
        st.rendering = null;
        // whatever station is current NOW: a station picked while Chart.js loaded asked for its chart through this
        // same promise, so giving up here would leave it without one
        if (!st.data) return;
        if (!deps.visible()) { st.dirty = true; return; }                  // hidden: show() draws it
        destroyChart();
        var Chart = deps.getChart();
        st.chart = new Chart(els.canvas.getContext('2d'), chartConfig(st.data, st.obs, { unit: st.unit, zone: zone(), range: range(), now: now }));
        st.dirty = false;
        fit();
        scheduleNow();
      }, function (err) {
        st.rendering = null;
        if (st.data) setStatus('error', MSG.failed, true);                 // Chart.js could not be loaded
        if (deps.onError) deps.onError(err);
      });
      return st.rendering;
    }
    function redraw() {
      writeList(); writeMeta(); pressRange(range());
      st.dirty = true;
      return deps.visible() ? render() : Promise.resolve();
    }
    function ask(path, seq, signal) {
      return deps.fetch(path, { signal: signal, headers: { Accept: 'application/json' } }).then(function (r) {
        if (seq !== st.seq) return { stale: true };
        var retryAfter = r.headers && r.headers.get ? r.headers.get('Retry-After') : null;
        return r.json().then(function (body) { return { status: r.status, body: body, retryAfter: retryAfter }; },
                             function () { return { status: r.status, body: null, retryAfter: retryAfter }; });
      });
    }
    function fetchObserved(seq) {
      var s = st.station;
      if (!s || !st.data || !st.data.obs) return;                          // the server's word: the station has a gauge
      ask('/api/tides/' + encodeURIComponent(s.id) + '/observed', seq, st.ac && st.ac.signal).then(function (res) {
        if (res.stale || seq !== st.seq) return;
        if (res.status === 200 && res.body && Array.isArray(res.body.t)) { st.obs = res.body; redraw(); }
      }, function () { /* the predictions stand on their own */ });
    }
    function attempt(seq) {
      var s = st.station;
      st.tries++;
      ask('/api/tides/' + encodeURIComponent(s.id), seq, st.ac.signal).then(function (res) {
        if (res.stale || seq !== st.seq) return;
        var b = res.body || {};
        if (res.status === 200 && Array.isArray(b.v) && !b.final) {
          st.data = b; st.tries = 0;
          setStatus('ready');
          redraw();
          fetchObserved(seq);
          return;
        }
        if (res.status === 200 && b.final) { setStatus('final', b.error || MSG.failed, false); return; }
        if (res.status === 404) { setStatus('final', MSG.unknown, false); return; }
        if (res.status === 503 && b.retry && st.tries < RETRY_MAX) {
          var wait = parseInt(res.retryAfter, 10);
          wait = isFinite(wait) && wait > 0 ? Math.min(wait, RETRY_MAX_S) : RETRY_DEFAULT_S;
          st.retryTimer = timers.set(function () { st.retryTimer = null; if (seq === st.seq) attempt(seq); }, wait * 1000);
          return;
        }
        setStatus('error', res.status === 503 ? MSG.busy : MSG.failed, true);
      }, function (err) {
        if (seq !== st.seq || (err && err.name === 'AbortError')) return;
        setStatus('error', MSG.failed, true);
      });
    }
    function load(station, opts) {
      opts = opts || {};
      st.seq++;
      if (st.ac) { try { st.ac.abort(); } catch (e) {} }
      stopTimers(); destroyChart();
      st.ac = deps.AbortController ? new deps.AbortController() : new AbortController();
      st.station = station; st.data = null; st.obs = null; st.tries = 0; st.dirty = false;
      if (opts.unit) st.unit = unitOf(opts.unit);
      st.zone = opts.zone || null;
      if (els.hilo) els.hilo.textContent = '';
      if (els.meta) els.meta.textContent = '';
      pressRange(range());
      setStatus('loading');
      attempt(st.seq);
      return st.seq;
    }
    function retry() { if (st.station) load(st.station, { unit: st.unit, zone: st.zone }); }
    function clear() {
      st.seq++;
      if (st.ac) { try { st.ac.abort(); } catch (e) {} st.ac = null; }
      stopTimers(); destroyChart();
      st.station = null; st.data = null; st.obs = null;
      setStatus('idle');
    }
    function setRange(v) {
      v = rangeOf(v);
      try { deps.storage.setItem(RANGE_KEY, v); } catch (e) {}
      pressRange(v);
      if (!st.data) return;
      writeList();
      if (st.chart && !st.rendering) {                                     // same data, another end: no rebuild
        var cfg = chartConfig(st.data, st.obs, { unit: st.unit, zone: zone(), range: v, now: now });
        st.chart.options.scales.x.max = cfg.options.scales.x.max;
        st.chart.update('none');
      } else redraw();
    }
    if (els.rangeBar) els.rangeBar.addEventListener('click', function (e) {
      var b = e.target && e.target.closest ? e.target.closest('[data-days]') : null;
      if (b) setRange(b.getAttribute('data-days'));
    });
    if (els.retry) els.retry.addEventListener('click', retry);
    return {
      load: load, clear: clear, retry: retry, setRange: setRange,
      setUnit: function (u) { st.unit = unitOf(u); if (st.data) redraw(); },
      setZone: function (z) { st.zone = z || null; if (st.data) redraw(); },
      show: function () { if (st.data && (st.dirty || !st.chart)) return render(); fit(); return Promise.resolve(); },
      resize: fit,
      state: function () {
        return { seq: st.seq, status: st.status, station: st.station && st.station.id, hasData: !!st.data, hasObs: !!st.obs,
                 chart: st.chart, unit: st.unit, zone: zone(), range: range(), tries: st.tries,
                 retryTimer: st.retryTimer !== null, nowTimer: st.nowTimer !== null };
      }
    };
  }

  var api = {
    createTideView: createTideView,
    _internals: {
      RANGE_KEY: RANGE_KEY, RANGES: RANGES, DEFAULT_RANGE: DEFAULT_RANGE, FT_PER_M: FT_PER_M, MIN_TICK_PX: MIN_TICK_PX,
      RETRY_MAX: RETRY_MAX, NOW_REDRAW_MS: NOW_REDRAW_MS, BOX_MIN: BOX_MIN, BOX_MAX: BOX_MAX, BOX_CHROME: BOX_CHROME,
      zoneClock: zoneClock, localMidnightBefore: localMidnightBefore, midnights: midnights, hourMarks: hourMarks,
      tickPlan: tickPlan, tickLabel: tickLabel, clockText: clockText, dateText: dateText, readRange: readRange,
      points: points, nightSpans: nightSpans, extremesBetween: extremesBetween, heightText: heightText,
      makeNightShade: makeNightShade, makeNowLine: makeNowLine, chartConfig: chartConfig
    }
  };
  if (typeof window !== 'undefined') window.AllshoreTides = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})();
