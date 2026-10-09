/* Allshore Surf tide station view (plan section 38). Loaded on every page; touches no DOM until createTideView(...)
 * is called with the tide window's elements, so tests/ui/tides.test.js evaluates it in Node with fakes.
 *
 * One station at a time: load(station) asks the server (/api/tides/<id>: NOAA's predictions every 30 min for 30 days,
 * the predicted highs and lows, the nights, the sun and moon) and draws a DAY STRIP (after tide-forecast.com's layout,
 * owner 2026-10-08): one table whose first column (the labels) stays put while the rest scrolls sideways, a column per
 * local day for 30 days from today's midnight in the display zone (the site's #tz, else the station's own zone), each
 * day closed (narrow: the date) or OPEN (wide: the full date over AM | PM halves; today opens by itself; a click on a
 * day's header toggles it, several may be open). Over the columns one SVG chart: the predicted curve with its fill,
 * the nights shaded, a dot at every high and low with its time and height written out on open days, the observed
 * water level dashed (stations with a gauge), a red dashed line at the current time with a dot at the current height
 * and a dashed level line to the axis (the current height in the label column), redrawn every minute. Under the chart,
 * aligned to the columns: HIGH and LOW (time + height), Sun (rise / set), Moon (phase, set / rise). Hovering (or
 * touching) the chart reads the time and height under the pointer. Heights above MLLW in the site's unit (US: ft, one
 * decimal; Metric: m, two). Within a day the time runs linearly from midnight to the zone's own noon over the left half
 * of its column and from noon to midnight over the right half, so 23- and 25-hour days keep noon at the AM | PM line.
 *  - A busy or unreachable server (503 + retry) is asked again after its Retry-After; a station NOAA has no
 *    predictions for says so (no retry); any other failure offers Retry.
 *  - A newer load() makes every answer of an older one void (sequence + AbortController); a strip is only built while
 *    the window shows its body (show() builds a deferred one). Text only (every string from the server is a text node).
 */
(function () {
  'use strict';

  var FT_PER_M = 3.28084;
  var HOUR_MS = 3600000, MIN_MS = 60000;
  var DAYS = 30;
  var COL_CLOSED = 64, COL_OPEN = 240, LABEL_W = 60;          // px: a closed day, an open day (two halves), the label column
  var CHART_H = 230, CHART_PAD = { top: 26, bottom: 10 };     // the chart's height; room for callouts above the highest high
  var RETRY_MAX = 24, RETRY_DEFAULT_S = 5, RETRY_MAX_S = 60;
  var TOUCH_MOUSE_MS = 700;               // mouse events this soon after a touch come from the touch
  var NOW_REDRAW_MS = 60000;
  var REFRESH_RETRY_MS = 10 * 60000;     // a midnight refresh that failed is tried again this often until it answers
  var CALLOUT_CHAR_W = 6;                // a callout's text width per character at 10.5 px (an estimate, a little wide)
  var SVG_NS = 'http://www.w3.org/2000/svg';
  var COLORS = { curve: '#1d6fd6', fill: 'rgba(29,111,214,0.16)', extreme: '#0b3d91', observed: '#e8590c',
                 night: 'rgba(30,60,110,0.11)', now: '#d6336c', grid: 'rgba(0,0,0,0.12)', text: '#1d2b4f' };
  var WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  var WEEKDAY_NAMES = { Sun: 'Sunday', Mon: 'Monday', Tue: 'Tuesday', Wed: 'Wednesday', Thu: 'Thursday', Fri: 'Friday', Sat: 'Saturday' };
  var MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  var CALLOUT_EDGE = 26;                // px: a callout this close to an end of the strip is anchored at the dot, not centred
  // the moon as seen from the northern hemisphere, one glyph per eighth of the cycle (the forecast table's); mirrored south
  var MOON_GLYPHS = ['🌑', '🌒', '🌓', '🌔', '🌕', '🌖', '🌗', '🌘'];
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
    var p = clk.parts(ms), cand = ms - (p.h * 60 + p.mi) * MIN_MS - (new Date(ms).getUTCSeconds() * 1000 + new Date(ms).getUTCMilliseconds());
    p = clk.parts(cand);
    if (p.h || p.mi) cand += p.h >= 12 ? (1440 - p.h * 60 - p.mi) * MIN_MS : -(p.h * 60 + p.mi) * MIN_MS;
    // a clock that goes back AT midnight (Cuba, the first Sunday of November) shows 00:00-01:00 twice: the day starts
    // at the first 00:00 (G27 A-F8)
    p = clk.parts(cand);
    for (var back = 60; back >= 30; back -= 30) {
      var q = clk.parts(cand - back * MIN_MS);
      if (q.h === 0 && q.d === p.d && q.mo === p.mo) { cand -= back * MIN_MS; break; }
    }
    return cand;
  }
  // count + 1 local midnights from the one at or before ms (index k = the start of day k)
  function midnights(ms, count, clk) {
    var out = [localMidnightBefore(ms, clk)];
    for (var k = 0; k < count; k++) out.push(localMidnightBefore(out[k] + 36 * HOUR_MS, clk));
    return out;
  }
  function clockText(p) {
    var h12 = p.h % 12 || 12;
    return h12 + ':' + (p.mi < 10 ? '0' : '') + p.mi + ' ' + (p.h < 12 ? 'AM' : 'PM');
  }
  function dayShort(p) { return (p.wd || '') + ' ' + p.d; }                                       // "Fri 9"
  function dayLong(p) { return (WEEKDAY_NAMES[p.wd] || p.wd || '') + ', ' + MONTHS[p.mo - 1] + ' ' + p.d; }   // "Friday, Oct 9"
  function dayMedium(p) { return (p.wd || '') + ', ' + MONTHS[p.mo - 1] + ' ' + p.d; }                       // "Sun, Nov 1"
  function stampText(p) { return (p.wd || '') + ' ' + p.mo + '/' + p.d + ', ' + clockText(p); }   // "Fri 10/9, 3:05 PM"

  // ---- the data ----------------------------------------------------------------------------------------------------
  function num(v) { return typeof v === 'number' && isFinite(v); }        // isFinite(null) is true: a gap is no 0
  function unitOf(u) { return u === 'Metric' ? 'Metric' : 'US'; }
  function height(m, unit) { return unit === 'Metric' ? m : m * FT_PER_M; }
  function fixed(v, n) { var s = v.toFixed(n); return /^-0(\.0+)?$/.test(s) ? s.slice(1) : s; }            // never "-0.0"
  function heightText(m, unit) { return unit === 'Metric' ? fixed(height(m, unit), 2) + ' m' : fixed(height(m, unit), 1) + ' ft'; }

  // The local noon of each day: the instant the zone's clock reads 12:00 (on a clock-change day not the day's middle:
  // 25 h from midnight to midnight puts the middle at 11:30 AM, 23 h at 12:30 PM).
  function noons(mids, clk) {
    var out = [];
    for (var k = 0; k < mids.length - 1; k++) {
      var c = mids[k] + 12 * HOUR_MS, p = clk.parts(c);
      c -= ((p.h - 12) * 60 + p.mi) * MIN_MS;
      out.push(Math.max(mids[k] + 1, Math.min(mids[k + 1] - 1, c)));
    }
    return out;
  }
  // The columns: day k runs [mids[k], mids[k+1]) and is COL_OPEN wide when open, else COL_CLOSED; its local noon sits
  // in the middle of its column (noonList; without it the day's middle), so an open day's AM | PM halves are the
  // clock's. -> { mids, noons, lefts (px of each day's start), widths, total, open (booleans) }
  function layout(mids, openSet, noonList) {
    var lefts = [], widths = [], open = [], nn = [], x = 0;
    for (var k = 0; k < mids.length - 1; k++) {
      var o = !!(openSet && openSet[k]);
      open.push(o); lefts.push(x); widths.push(o ? COL_OPEN : COL_CLOSED); x += widths[k];
      nn.push(noonList && noonList[k] > mids[k] && noonList[k] < mids[k + 1] ? noonList[k] : (mids[k] + mids[k + 1]) / 2);
    }
    return { mids: mids, noons: nn, lefts: lefts, widths: widths, total: x, open: open };
  }
  function dayOf(L, ms) {                                                  // the day index of an instant, or -1 outside the strip
    var m = L.mids;
    if (ms < m[0] || ms >= m[m.length - 1]) return -1;
    var lo = 0, hi = m.length - 2;
    while (lo < hi) { var mid = (lo + hi + 1) >> 1; if (m[mid] <= ms) lo = mid; else hi = mid - 1; }
    return lo;
  }
  // an instant -> px from the strip's left edge: linear from midnight to noon over the left half of its day's column
  // and from noon to midnight over the right half (outside the strip: the edge halves' slopes)
  function xOf(L, ms) {
    var k = dayOf(L, ms);
    if (k < 0) k = ms < L.mids[0] ? 0 : L.mids.length - 2;
    var a = L.mids[k], n = L.noons[k], b = L.mids[k + 1], half = L.widths[k] / 2;
    return ms < n ? L.lefts[k] + (ms - a) / (n - a) * half : L.lefts[k] + half + (ms - n) / (b - n) * half;
  }
  function tOf(L, x) {                                                     // px -> the instant (inside the strip)
    if (x <= 0) return L.mids[0];
    for (var k = 0; k < L.widths.length; k++) {
      if (x < L.lefts[k] + L.widths[k] || k === L.widths.length - 1) {
        var half = L.widths[k] / 2, f = Math.max(0, Math.min(L.widths[k], x - L.lefts[k]));
        return f < half ? L.mids[k] + f / half * (L.noons[k] - L.mids[k]) : L.noons[k] + (f - half) / half * (L.mids[k + 1] - L.noons[k]);
      }
    }
    return L.mids[L.mids.length - 1];
  }

  // The chart's vertical scale over the curve, the extremes and the observations in the unit: a nice tick step giving
  // 3-7 ticks; -> { lo, hi, step, ticks, y(value) }
  function yScale(values, unit) {
    var lo = Infinity, hi = -Infinity;
    for (var i = 0; i < values.length; i++) { var v = values[i]; if (num(v)) { if (v < lo) lo = v; if (v > hi) hi = v; } }
    if (!isFinite(lo)) { lo = 0; hi = 1; }
    if (hi - lo < 0.2) { hi = lo + 0.2; }
    var steps = unit === 'Metric' ? [0.1, 0.2, 0.25, 0.5, 1, 2, 5] : [0.25, 0.5, 1, 2, 5, 10], step = steps[steps.length - 1];
    for (var s = 0; s < steps.length; s++) { if ((hi - lo) / steps[s] <= 7) { step = steps[s]; break; } }
    var a = Math.floor(lo / step) * step, b = Math.ceil(hi / step) * step;
    if (b - hi < step * 0.1) b += step;                                   // room above the top value
    var ticks = [];
    for (var t = a; t <= b + 1e-9; t += step) ticks.push(Math.round(t * 1000) / 1000);
    var inner = CHART_H - CHART_PAD.top - CHART_PAD.bottom;
    return { lo: a, hi: b, step: step, ticks: ticks,
             y: function (v) { return CHART_PAD.top + (b - v) / (b - a) * inner; } };
  }
  function tickText(v) { return Math.abs(v - Math.round(v)) < 1e-9 ? String(Math.round(v) + 0) : fixed(v, 2).replace(/0$/, ''); }   // 0.25 / 0.5 / 1 (G27 A-F1)

  // The curve as drawn: NOAA's 30-minute samples with the exact highs and lows put in between (NOAA's extremes fall
  // between samples: without them a big tide's peak is cut short by up to 0.1 m and a dot floats off the line; G27
  // A-F10). An extreme next to a gap stays out (the gap stays). Kept on the payload: [{t, m}] in time order.
  function series(d) {
    if (d.__series) return d.__series;
    var b = d.begin * 1000, step = d.step * 1000, v = d.v || [], out = [], ex = [], j = 0;
    (d.hilo || []).forEach(function (e) { if (e && num(e[0]) && num(e[1])) ex.push([e[0] * 1000, e[1]]); });
    ex.sort(function (p, q) { return p[0] - q[0]; });
    for (var i = 0; i < v.length; i++) {
      var t = b + i * step, m = num(v[i]) ? v[i] : null;
      while (j < ex.length && ex[j][0] < t) {
        var prev = out.length ? out[out.length - 1] : null;
        if (prev && prev.m !== null && m !== null && ex[j][0] > prev.t) out.push({ t: ex[j][0], m: ex[j][1] });
        j++;
      }
      if (j < ex.length && ex[j][0] === t) { if (m !== null) m = ex[j][1]; j++; }
      out.push({ t: t, m: m });
    }
    Object.defineProperty(d, '__series', { value: out, enumerable: false, configurable: true });
    return out;
  }
  function firstAtOrAfter(s, ms) { var lo = 0, hi = s.length; while (lo < hi) { var mid = (lo + hi) >> 1; if (s[mid].t < ms) lo = mid + 1; else hi = mid; } return lo; }
  // the curve's points inside [from, to) plus one on each side: [{t, m}] (m null in a gap)
  function samples(d, from, to) {
    var s = series(d), i0 = Math.max(0, firstAtOrAfter(s, from) - 1), i1 = Math.min(s.length - 1, firstAtOrAfter(s, to));
    return s.slice(i0, i1 + 1);
  }
  // the predicted height at an instant (linear between the curve's points), or null
  function heightAt(d, ms) {
    var s = series(d), i = firstAtOrAfter(s, ms);
    if (i < s.length && s[i].t === ms) return s[i].m;
    if (i === 0 || i >= s.length || s[i].m === null || s[i - 1].m === null) return null;
    var a = s[i - 1], c = s[i];
    return a.m + (c.m - a.m) * (ms - a.t) / (c.t - a.t);
  }
  // the extremes of a day's half: [{t, m, k}] (half: 0 AM, 1 PM, -1 the whole day), in time order
  function extremesIn(d, L, k, half) {
    var a = L.mids[k], b = L.mids[k + 1], noon = L.noons[k];
    var from = half === 1 ? noon : a, to = half === 0 ? noon : b, out = [];
    (d.hilo || []).forEach(function (e) {
      if (!e || !num(e[0]) || !num(e[1])) return;
      var t = e[0] * 1000;
      if (t >= from && t < to) out.push({ t: t, m: e[1], k: e[2] === 'H' ? 'H' : 'L' });
    });
    return out;
  }
  function eventsIn(d, L, k, half, kinds) {                                // sun / moon events of a day's half: [{t, kind}]
    var a = L.mids[k], b = L.mids[k + 1], noon = L.noons[k];
    var from = half === 1 ? noon : a, to = half === 0 ? noon : b, out = [];
    (d.events || []).forEach(function (e) {
      if (!e || !num(e[0])) return;
      var t = e[0] * 1000;
      if (t >= from && t < to && kinds.indexOf(e[1]) >= 0) out.push({ t: t, kind: e[1] });
    });
    return out;
  }
  // the moon on a day: the sample nearest the day's local noon -> {phase, pct, name, glyph} or null
  function moonOf(d, L, k, lat) {
    var mid = L.noons[k], best = null, bd = Infinity;
    (d.moon || []).forEach(function (m) {
      if (!m || !num(m[0])) return;
      var dd = Math.abs(m[0] * 1000 - mid);
      if (dd < bd) { bd = dd; best = m; }
    });
    if (!best) return null;
    var i = Math.round(best[1] * 8) % 8, g = lat < 0 ? (8 - i) % 8 : i;
    return { phase: best[1], pct: best[2], name: best[3], glyph: MOON_GLYPHS[g] };
  }
  // the nights as [x0, x1] px inside the strip
  function nightSpans(d, L) {
    var end = L.total, out = [];
    (d.night || []).forEach(function (n) {
      if (!n || !num(n[0]) || !num(n[1]) || n[1] <= n[0]) return;
      var x0 = Math.max(0, xOf(L, n[0] * 1000)), x1 = Math.min(end, xOf(L, n[1] * 1000));
      if (x1 > x0) out.push([x0, x1]);
    });
    return out;
  }
  // the SVG path of the curve ("M x y L x y ...", a new M after a gap), and the closed area under it
  function curvePaths(d, L, ys, unit) {
    var pts = samples(d, L.mids[0], L.mids[L.mids.length - 1]), line = '', area = '', run = [];
    function flush() {
      if (run.length < 2) { run = []; return; }
      var seg = run.map(function (p, i) { return (i ? 'L' : 'M') + p[0].toFixed(1) + ' ' + p[1].toFixed(1); }).join('');
      line += seg;
      area += seg + 'L' + run[run.length - 1][0].toFixed(1) + ' ' + CHART_H + 'L' + run[0][0].toFixed(1) + ' ' + CHART_H + 'Z';
      run = [];
    }
    var prev = null;
    pts.forEach(function (p) {
      if (p.m === null) { flush(); prev = null; return; }
      var q = [xOf(L, p.t), ys.y(height(p.m, unit))];
      if (q[0] < 0) { prev = q; return; }                                  // before the strip: kept to cut at x = 0
      if (prev && prev[0] < 0 && q[0] > 0) run.push([0, prev[1] + (q[1] - prev[1]) * (0 - prev[0]) / (q[0] - prev[0])]);
      if (q[0] > L.total) {                                                // past the strip: cut at its end, then stop
        if (prev && prev[0] < L.total) run.push([L.total, prev[1] + (q[1] - prev[1]) * (L.total - prev[0]) / (q[0] - prev[0])]);
        prev = null; return;
      }
      run.push(q); prev = q;
    });
    flush();
    return { line: line, area: area };
  }
  // whether the curve has a gap inside the strip (NOAA's list of highs and lows incomplete there: G27 A-F9)
  function hasGap(d, L) {
    var a = L.mids[0], b = L.mids[L.mids.length - 1];
    return samples(d, a, b).some(function (p) { return p.m === null && p.t >= a && p.t < b; });
  }

  // ---- the view ----------------------------------------------------------------------------------------------------
  function createTideView(deps) {
    var els = deps.els, doc = deps.document;
    var now = deps.now || function () { return Date.now(); };
    var timers = deps.timers || { set: function (f, ms) { return setTimeout(f, ms); }, clear: function (id) { clearTimeout(id); } };
    var st = { seq: 0, station: null, data: null, obs: null, unit: 'US', zone: null, dirty: false, built: null,
               ac: null, retryTimer: null, nowTimer: null, tries: 0, status: 'idle', open: {}, L: null, ys: null,
               retried: false, refreshAt: 0 };
    if (deps.unit) st.unit = unitOf(deps.unit);

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
    function addClass(e, cls) { if (cls) cls.split(' ').forEach(function (c) { if (c) e.classList.add(c); }); return e; }   // classList: also on SVG elements
    function el(tag, cls, text) { var e = addClass(doc.createElement(tag), cls); if (text !== undefined) e.textContent = text; return e; }
    function svgEl(tag, attrs) {
      var e = doc.createElementNS(SVG_NS, tag);
      for (var k in attrs) { if (attrs[k] === undefined || attrs[k] === null) continue; if (k === 'class') addClass(e, attrs[k]); else e.setAttribute(k, attrs[k]); }
      return e;
    }
    function kidsOf(e) { return e.childNodes && e.childNodes.length ? e.childNodes : e.children; }
    function zoneAt(ms) {
      try { if (deps.zoneAbbr) return deps.zoneAbbr(ms, zone()) || zone(); } catch (e) {}
      return zone();
    }
    // The zone's names over the strip, in order: [{k, text}], k = the day a name starts on. One entry unless the clocks
    // change inside the 30 days (New York in late October: EDT, then EST from Sun, Nov 1).
    function zoneSpans(L) {
      if (!L || !L.mids || L.mids.length < 2) return [{ k: 0, text: zoneAt(now()) }];
      var out = [{ k: 0, text: zoneAt(L.mids[0]) }];
      for (var k = 0; k + 1 < L.mids.length; k++) {
        var t = zoneAt(L.mids[k + 1] - 1);                                 // the name as day k ends
        if (t !== out[out.length - 1].text) out.push({ k: k, text: t });
      }
      return out;
    }
    function spans() { return st.zs || zoneSpans(st.L); }
    function zoneText() { var z = spans(); return z.length > 1 ? 'local' : z[0].text; }   // the rows' label: one name, else "local"
    function zoneMeta() {                                                  // "EDT (EST from Sun, Nov 1)"
      var z = spans(), clk = zoneClock(zone()), L = st.L;
      if (z.length < 2 || !L || !L.noons) return z[0].text;
      return z[0].text + ' (' + z.slice(1).map(function (s) { return s.text + ' from ' + dayMedium(clk.parts(L.noons[s.k])); }).join('; ') + ')';
    }

    function writeMeta() {
      var d = st.data;
      if (!els.meta || !d) return;
      els.meta.textContent = '';
      var ref = d.ref_name ? d.ref_name + ' (' + d.ref + ')' : (d.ref || '');
      var lines = ['Heights above mean lower low water (MLLW) · times in ' + zoneMeta() + ' · click a day to open or close it',
                   (METHOD_TEXT[d.method] || METHOD_TEXT.harmonic).replace('%REF%', ref),
                   'Predictions do not include storm surge or wind effects.'];
      if (st.L && hasGap(d, st.L)) lines.push('The curve has gaps where NOAA’s list of highs and lows is incomplete.');
      if (st.obs && st.obs.note) lines.push(st.obs.note + '.');
      lines.forEach(function (t) { els.meta.appendChild(el('div', '', t)); });
      var src = el('div', ''), a = el('a', '', 'NOAA CO-OPS station ' + d.id);
      a.setAttribute('href', 'https://tidesandcurrents.noaa.gov/noaatidepredictions.html?id=' + encodeURIComponent(d.id));
      a.setAttribute('target', '_blank'); a.setAttribute('rel', 'noopener');
      src.appendChild(doc.createTextNode('Source: ')); src.appendChild(a);
      els.meta.appendChild(src);
    }

    // ---- the strip
    function cell(tag, cls, span) { var c = el(tag, cls); if (span > 1) c.setAttribute('colspan', String(span)); return c; }
    function rowHead() { var c = el('th', 'tide-lab'); c.setAttribute('scope', 'row'); return c; }   // a row's label (B-P3-4)
    function twoLine(c, a, b) { c.appendChild(el('div', 'tide-l1', a)); c.appendChild(el('div', 'tide-l2', b)); return c; }
    // a sun / moon event: its glyph over its time, read by a screen reader as "Sunrise 7:18 AM" (B-P3-4)
    function evBox(word, glyph, time) {
      var box = el('div', 'tide-ev'), g = el('div', 'tide-l1', glyph);
      g.setAttribute('aria-hidden', 'true');
      box.appendChild(el('span', 'visually-hidden', word + ' ')); box.appendChild(g); box.appendChild(el('div', 'tide-l2', time));
      box.setAttribute('title', word + ' ' + time);
      return box;
    }
    function extremeCells(tr, d, L, k, kind, clk) {                        // HIGH / LOW cells of day k (one, or AM + PM)
      var halves = L.open[k] ? [0, 1] : [-1];
      halves.forEach(function (half) {
        var c = cell('td', 'tide-cell tide-' + (kind === 'H' ? 'high' : 'low'), half < 0 ? 2 : 1);
        extremesIn(d, L, k, half).filter(function (e) { return e.k === kind; }).forEach(function (e) {
          c.appendChild(twoLine(el('div', 'tide-ex'), clockText(clk.parts(e.t)), heightText(e.m, st.unit)));
        });
        tr.appendChild(c);
      });
    }
    function sunCells(tr, d, L, k, clk) {
      var halves = L.open[k] ? [0, 1] : [-1];
      halves.forEach(function (half) {
        var c = cell('td', 'tide-cell tide-sun', half < 0 ? 2 : 1);
        eventsIn(d, L, k, half, ['sunrise', 'sunset']).forEach(function (e) {
          var box = evBox(e.kind === 'sunrise' ? 'Sunrise' : 'Sunset', e.kind === 'sunrise' ? '☀️↑' : '☀️↓', clockText(clk.parts(e.t)));
          c.appendChild(box);
        });
        tr.appendChild(c);
      });
    }
    function moonCells(tr, d, L, k, clk, lat) {
      var halves = L.open[k] ? [0, 1] : [-1], m = moonOf(d, L, k, lat);
      halves.forEach(function (half, i) {
        var c = cell('td', 'tide-cell tide-moon', half < 0 ? 2 : 1);
        if (i === 0 && m) {
          var g = el('div', 'tide-moon-glyph', m.glyph); g.setAttribute('title', m.name + ', ' + m.pct + '% lit'); g.setAttribute('aria-hidden', 'true');
          c.appendChild(g); c.appendChild(el('span', 'visually-hidden', m.name + ', ' + m.pct + '% lit. '));
        }
        eventsIn(d, L, k, half, ['moonrise', 'moonset']).forEach(function (e) {
          var box = evBox(e.kind === 'moonrise' ? 'Moonrise' : 'Moonset', e.kind === 'moonrise' ? '☽↑' : '☽↓', clockText(clk.parts(e.t)));
          c.appendChild(box);
        });
        tr.appendChild(c);
      });
    }
    function buildChart(d, L, ys, clk) {
      var svg = svgEl('svg', { 'class': 'tide-svg', width: L.total, height: CHART_H, viewBox: '0 0 ' + L.total + ' ' + CHART_H, role: 'img', 'aria-label': 'Tide height graph' });
      nightSpans(d, L).forEach(function (s) { svg.appendChild(svgEl('rect', { 'class': 'tide-night', x: s[0].toFixed(1), y: 0, width: (s[1] - s[0]).toFixed(1), height: CHART_H, fill: COLORS.night })); });
      ys.ticks.forEach(function (v) { var y = ys.y(v).toFixed(1); svg.appendChild(svgEl('line', { x1: 0, x2: L.total, y1: y, y2: y, stroke: COLORS.grid, 'stroke-width': 0.7 })); });
      for (var k = 0; k < L.widths.length; k++) {
        svg.appendChild(svgEl('line', { x1: L.lefts[k], x2: L.lefts[k], y1: 0, y2: CHART_H, stroke: 'rgba(0,0,0,0.28)', 'stroke-width': 1 }));
        if (L.open[k]) svg.appendChild(svgEl('line', { x1: L.lefts[k] + L.widths[k] / 2, x2: L.lefts[k] + L.widths[k] / 2, y1: 0, y2: CHART_H, stroke: 'rgba(0,0,0,0.12)', 'stroke-width': 1, 'stroke-dasharray': '3 3' }));
      }
      var paths = curvePaths(d, L, ys, st.unit);
      svg.appendChild(svgEl('path', { 'class': 'tide-area', d: paths.area, fill: COLORS.fill, stroke: 'none' }));
      svg.appendChild(svgEl('path', { 'class': 'tide-curve', d: paths.line, fill: 'none', stroke: COLORS.curve, 'stroke-width': 1.8, 'stroke-linejoin': 'round' }));
      if (st.obs && st.obs.t && st.obs.v) {
        var o = '', pen = false;
        for (var i = 0; i < st.obs.t.length; i++) {
          if (!num(st.obs.t[i]) || !num(st.obs.v[i])) { pen = false; continue; }
          var ox = xOf(L, st.obs.t[i] * 1000);
          if (ox < 0 || ox > L.total) { pen = false; continue; }
          o += (pen ? 'L' : 'M') + ox.toFixed(1) + ' ' + ys.y(height(st.obs.v[i], st.unit)).toFixed(1); pen = true;
        }
        if (o) svg.appendChild(svgEl('path', { 'class': 'tide-observed', d: o, fill: 'none', stroke: COLORS.observed, 'stroke-width': 1.5, 'stroke-dasharray': '5 3' }));
      }
      var boxes = [];                                                      // the callouts drawn so far: none overlaps another
      function boxAt(tx, anchor, above, y, w) {
        var x0 = anchor === 'start' ? tx : anchor === 'end' ? tx - w : tx - w / 2;
        return { x0: x0, x1: x0 + w, y0: above ? y - 29 : y + 5, y1: above ? y - 6 : y + 28 };
      }
      function clashes(b) { return boxes.some(function (o) { return b.x0 < o.x1 && o.x0 < b.x1 && b.y0 < o.y1 && o.y0 < b.y1; }); }
      for (var k2 = 0; k2 < L.widths.length; k2++) {
        extremesIn(d, L, k2, -1).forEach(function (e) {
          var x = xOf(L, e.t), y = ys.y(height(e.m, st.unit));
          svg.appendChild(svgEl('circle', { 'class': 'tide-dot', cx: x.toFixed(1), cy: y.toFixed(1), r: 3.2, fill: COLORS.extreme }));
          if (L.open[k2]) {                                                // the callout on an open day: time over height
            // a high's callout above its dot, a low's below; flipped when that would leave the chart (G27 B-P3-7)
            var up = e.k === 'H', above = up ? y - 31 >= 0 : y + 28 > CHART_H;
            // near either end of the strip the text starts (or ends) at the dot, so the chart's edge does not cut it
            var anchor = x < CALLOUT_EDGE ? 'start' : x > L.total - CALLOUT_EDGE ? 'end' : 'middle';
            var txn = anchor === 'start' ? Math.max(2, x - 3) : anchor === 'end' ? Math.min(L.total - 2, x + 3) : x;
            var s1 = clockText(clk.parts(e.t)), s2 = (up ? '↑' : '↓') + heightText(e.m, st.unit);
            var w = Math.max(s1.length, s2.length) * CALLOUT_CHAR_W + 2, box = boxAt(txn, anchor, above, y, w);
            if (clashes(box)) {                                            // over another callout (a double high): the
              var other = !above, fits = other ? y - 31 >= 0 : y + 28 <= CHART_H;      // other side of the dot, else none
              box = boxAt(txn, anchor, other, y, w);
              if (!fits || clashes(box)) return;                           // the dot and the rows still carry the values
              above = other;
            }
            boxes.push(box);
            var y1 = above ? y - 20 : y + 14, y2 = above ? y - 9 : y + 25, tx = txn.toFixed(1);
            var g = svgEl('g', { 'class': 'tide-callout tide-callout-' + (up ? 'high' : 'low') });
            var t1 = svgEl('text', { x: tx, y: y1.toFixed(1), 'text-anchor': anchor, 'font-size': 10.5, fill: COLORS.text }); t1.textContent = s1; g.appendChild(t1);
            var t2 = svgEl('text', { x: tx, y: y2.toFixed(1), 'text-anchor': anchor, 'font-size': 10.5, 'font-weight': 600, fill: COLORS.text }); t2.textContent = s2; g.appendChild(t2);
            svg.appendChild(g);
          }
        });
      }
      // the current time: a dashed line, a dot at the current height, a level line to the axis
      var nowG = svgEl('g', { 'class': 'tide-now' });
      nowG.appendChild(svgEl('line', { 'class': 'tide-now-line', x1: 0, x2: 0, y1: 0, y2: CHART_H, stroke: COLORS.now, 'stroke-width': 1.2, 'stroke-dasharray': '4 3' }));
      nowG.appendChild(svgEl('line', { 'class': 'tide-now-level', x1: 0, x2: 0, y1: 0, y2: 0, stroke: COLORS.now, 'stroke-width': 1, 'stroke-dasharray': '2 3' }));
      nowG.appendChild(svgEl('circle', { 'class': 'tide-now-dot', cx: 0, cy: 0, r: 4, fill: COLORS.now, stroke: '#fff', 'stroke-width': 1.5 }));
      svg.appendChild(nowG);
      // the pointer's readout: a guide line and a label (hidden until the pointer is over the chart)
      var rd = svgEl('g', { 'class': 'tide-readout', visibility: 'hidden' });
      rd.appendChild(svgEl('line', { x1: 0, x2: 0, y1: 0, y2: CHART_H, stroke: 'rgba(0,0,0,0.45)', 'stroke-width': 1 }));
      rd.appendChild(svgEl('circle', { cx: 0, cy: 0, r: 3, fill: '#fff', stroke: COLORS.curve, 'stroke-width': 1.5 }));
      rd.appendChild(svgEl('rect', { x: 0, y: 2, width: 150, height: 16, rx: 3, fill: 'rgba(255,255,255,0.92)', stroke: 'rgba(0,0,0,0.25)' }));
      var rt = svgEl('text', { x: 0, y: 14, 'font-size': 11, fill: COLORS.text }); rt.textContent = ''; rd.appendChild(rt);
      svg.appendChild(rd);
      return { svg: svg, nowG: nowG, readout: rd };
    }
    function placeNow(ms) {                                                // the now group and the level label: at ms
      var b = st.built; if (!b || !st.data) return;
      var x = xOf(st.L, ms), m = heightAt(st.data, ms), inside = ms >= st.L.mids[0] && ms < st.L.mids[st.L.mids.length - 1];
      var kids = kidsOf(b.nowG), line = kids[0], level = kids[1], dot = kids[2];
      if (!inside) { b.nowG.setAttribute('visibility', 'hidden'); b.nowLabel.textContent = ''; return; }
      b.nowG.setAttribute('visibility', 'visible');
      line.setAttribute('x1', x.toFixed(1)); line.setAttribute('x2', x.toFixed(1));
      if (m === null) { dot.setAttribute('visibility', 'hidden'); level.setAttribute('visibility', 'hidden'); b.nowLabel.textContent = ''; return; }
      var y = st.ys.y(height(m, st.unit));
      dot.setAttribute('visibility', 'visible'); level.setAttribute('visibility', 'visible');
      dot.setAttribute('cx', x.toFixed(1)); dot.setAttribute('cy', y.toFixed(1));
      level.setAttribute('x1', 0); level.setAttribute('x2', x.toFixed(1)); level.setAttribute('y1', y.toFixed(1)); level.setAttribute('y2', y.toFixed(1));
      b.nowLabel.textContent = heightText(m, st.unit); b.nowLabel.style.top = (y - 8) + 'px';
    }
    function readoutAt(px) {                                               // the pointer at px from the strip's left edge
      var b = st.built; if (!b || !st.data) return;
      var kids = kidsOf(b.readout);
      if (px === null || px < 0 || px > st.L.total) { b.readout.setAttribute('visibility', 'hidden'); b.readoutText = ''; return; }
      var t = tOf(st.L, px), m = heightAt(st.data, t), clk = zoneClock(zone());
      var text = stampText(clk.parts(t)) + (spans().length > 1 ? ' ' + zoneAt(t) : '') + (m === null ? '' : ' · ' + heightText(m, st.unit));
      b.readout.setAttribute('visibility', 'visible');
      kids[0].setAttribute('x1', px.toFixed(1)); kids[0].setAttribute('x2', px.toFixed(1));
      if (m === null) kids[1].setAttribute('visibility', 'hidden');
      else { kids[1].setAttribute('visibility', 'visible'); kids[1].setAttribute('cx', px.toFixed(1)); kids[1].setAttribute('cy', st.ys.y(height(m, st.unit)).toFixed(1)); }
      var w = Math.min(190, 7 * text.length + 12), lx = Math.max(0, Math.min(st.L.total - w, px + 8));
      kids[2].setAttribute('x', lx.toFixed(1)); kids[2].setAttribute('width', w);
      kids[3].setAttribute('x', (lx + 6).toFixed(1)); kids[3].textContent = text;
      b.readoutText = text;
    }
    function build() {
      var d = st.data; if (!d || !els.strip) return;
      var clk = zoneClock(zone()), mids = midnights(now(), DAYS, clk), L = layout(mids, st.open, noons(mids, clk));
      var vals = [];
      samples(d, mids[0], mids[DAYS]).forEach(function (p) { if (p.m !== null) vals.push(height(p.m, st.unit)); });
      if (st.obs && st.obs.v) st.obs.v.forEach(function (v) { if (num(v)) vals.push(height(v, st.unit)); });
      var ys = yScale(vals, st.unit);
      st.L = L; st.ys = ys; st.zs = zoneSpans(L);
      var lat = num(d.lat) ? d.lat : (st.station && num(st.station.lat) ? st.station.lat : 0);
      var scroller = el('div', 'tide-scroll'), table = el('table', 'tide-table');
      var first = clk.parts(mids[0] + 12 * HOUR_MS);
      table.appendChild(el('caption', 'visually-hidden', 'Tide predictions for ' + (st.station && st.station.name || d.name || d.id) +
                                                         ', 30 days from ' + dayLong(first) + ', times in ' + zoneMeta()));
      var colgroup = el('colgroup'), c0 = el('col'); c0.style.width = LABEL_W + 'px'; colgroup.appendChild(c0);
      for (var k = 0; k < DAYS; k++) { for (var hh = 0; hh < 2; hh++) { var c = el('col'); c.style.width = (L.widths[k] / 2) + 'px'; colgroup.appendChild(c); } }
      table.appendChild(colgroup);
      // the day headers
      var thead = el('thead'), hr = el('tr', 'tide-days'); hr.appendChild(cell('th', 'tide-lab tide-lab-head', 1));
      for (k = 0; k < DAYS; k++) {
        var th = cell('th', 'tide-day' + (L.open[k] ? ' tide-day-open' : '') + (k === 0 ? ' tide-day-today' : ''), 2);
        th.setAttribute('data-day', String(k));
        var p = clk.parts(mids[k] + 12 * HOUR_MS), btn = el('button', 'tide-daybtn', L.open[k] ? dayLong(p) : dayShort(p));
        btn.setAttribute('type', 'button'); btn.setAttribute('data-day', String(k)); btn.setAttribute('aria-expanded', L.open[k] ? 'true' : 'false');
        btn.setAttribute('title', (L.open[k] ? 'Collapse ' : 'Expand ') + dayLong(p));
        th.appendChild(btn); hr.appendChild(th);
      }
      thead.appendChild(hr);
      var ar = el('tr', 'tide-ampm'); ar.appendChild(cell('th', 'tide-lab', 1));
      for (k = 0; k < DAYS; k++) {
        if (L.open[k]) { ar.appendChild(cell('th', 'tide-half', 1)).textContent = 'AM'; ar.appendChild(cell('th', 'tide-half', 1)).textContent = 'PM'; }
        else ar.appendChild(cell('th', 'tide-half tide-half-closed', 2));
      }
      thead.appendChild(ar); table.appendChild(thead);
      var tbody = el('tbody');
      // the chart row: the y labels in the sticky cell, the SVG across every day
      var cr = el('tr', 'tide-chart-row'), cl = cell('td', 'tide-lab tide-lab-chart', 1), lab = el('div', 'tide-ylabels'); lab.style.height = CHART_H + 'px';
      ys.ticks.forEach(function (v) { var s = el('span', 'tide-ytick', tickText(v, st.unit)); s.style.top = (ys.y(v) - 7) + 'px'; lab.appendChild(s); });
      var nowLabel = el('span', 'tide-ynow', ''); lab.appendChild(nowLabel);
      cl.appendChild(lab); cr.appendChild(cl);
      var cc = cell('td', 'tide-chart-cell', DAYS * 2), chart = buildChart(d, L, ys, clk); cc.appendChild(chart.svg); cr.appendChild(cc); tbody.appendChild(cr);
      [['tide-row-high', 'HIGH', 'H'], ['tide-row-low', 'LOW', 'L']].forEach(function (r) {
        var tr = el('tr', r[0]); tr.appendChild(twoLine(rowHead(), r[1], '(' + zoneText() + ')'));
        for (var kk = 0; kk < DAYS; kk++) extremeCells(tr, d, L, kk, r[2], clk);
        tbody.appendChild(tr);
      });
      var sr = el('tr', 'tide-row-sun'); sr.appendChild(rowHead()).textContent = 'Sun';
      for (k = 0; k < DAYS; k++) sunCells(sr, d, L, k, clk);
      tbody.appendChild(sr);
      var mr = el('tr', 'tide-row-moon'); mr.appendChild(rowHead()).textContent = 'Moon';
      for (k = 0; k < DAYS; k++) moonCells(mr, d, L, k, clk, lat);
      tbody.appendChild(mr);
      table.appendChild(tbody); scroller.appendChild(table);
      var keepScroll = st.built && st.built.scroller ? st.built.scroller.scrollLeft : 0;
      els.strip.textContent = '';
      els.strip.appendChild(scroller);
      st.built = { scroller: scroller, table: table, svg: chart.svg, nowG: chart.nowG, readout: chart.readout, nowLabel: nowLabel, readoutText: '' };
      if (keepScroll) scroller.scrollLeft = keepScroll;
      placeNow(now());
      st.dirty = false;
    }
    function toggleDay(k) {
      if (!st.data || !(k >= 0 && k < DAYS)) return;
      st.open[k] = !st.open[k];
      build();
      var btn = st.built && st.built.table.querySelector('button[data-day="' + k + '"]');
      if (btn && btn.focus) try { btn.focus({ preventScroll: true }); } catch (e) {}
    }
    function scheduleNow() {
      if (st.nowTimer !== null) timers.clear(st.nowTimer);
      st.nowTimer = timers.set(function tick() {
        st.nowTimer = null;
        if (!st.built || !st.data) return;
        var t = now();
        if (t >= st.L.mids[1]) {                                           // a new day: the strip starts today again, its
          st.open = { 0: true }; build(); writeMeta(); refresh();          // notes follow and a fresh answer is asked for
        } else {
          placeNow(t);
          if (st.refreshAt && t >= st.refreshAt) refresh();                // that answer failed: asked again (G27 re-check)
        }
        st.nowTimer = timers.set(tick, NOW_REDRAW_MS);
      }, NOW_REDRAW_MS);
    }
    function render() {
      if (!st.data) return;
      if (!deps.visible()) { st.dirty = true; return; }
      build();
      writeMeta();
      scheduleNow();
    }
    function ask(path, seq, signal) {
      return deps.fetch(path, { signal: signal, headers: { Accept: 'application/json' } }).then(function (r) {
        if (seq !== st.seq) return { stale: true };
        var retryAfter = r.headers && r.headers.get ? r.headers.get('Retry-After') : null;
        return r.json().then(function (body) { return { status: r.status, body: body, retryAfter: retryAfter }; },
                             function () { return { status: r.status, body: null, retryAfter: retryAfter }; });
      });
    }
    // The answer again, revalidated (a tab open across midnight: the old answer's curve ends 12 h after its 32 days; G27
    // A-F5). Quiet: a failure keeps the strip as it is, and the station is asked again every REFRESH_RETRY_MS.
    function refresh() {
      var s = st.station, seq = st.seq;
      if (!s) return;
      st.refreshAt = 0;
      var failed = function () { if (seq === st.seq) st.refreshAt = now() + REFRESH_RETRY_MS; };
      deps.fetch('/api/tides/' + encodeURIComponent(s.id), { cache: 'no-cache', headers: { Accept: 'application/json' } }).then(function (r) {
        return seq === st.seq && r.status === 200 ? r.json() : null;
      }).then(function (b) {
        if (seq !== st.seq) return;
        if (!b || !Array.isArray(b.v) || b.final) { failed(); return; }
        st.data = b; build(); writeMeta(); fetchObserved(seq);
      }, failed);
    }
    function fetchObserved(seq) {
      var s = st.station;
      if (!s || !st.data || !st.data.obs) return;                          // the server's word: the station has a gauge
      ask('/api/tides/' + encodeURIComponent(s.id) + '/observed', seq, st.ac && st.ac.signal).then(function (res) {
        if (res.stale || seq !== st.seq) return;
        if (res.status === 200 && res.body && Array.isArray(res.body.t)) { st.obs = res.body; render(); }
      }, function () { /* the predictions stand on their own */ });
    }
    function attempt(seq) {
      var s = st.station;
      st.tries++;
      ask('/api/tides/' + encodeURIComponent(s.id), seq, st.ac.signal).then(function (res) {
        if (res.stale || seq !== st.seq) return;
        var b = res.body || {};
        if (res.status === 200 && Array.isArray(b.v) && !b.final) {
          st.data = b; st.tries = 0; st.open = { 0: true };
          setStatus('ready');
          render();
          fetchObserved(seq);
          if (st.retried) { st.retried = false; if (deps.onRetried) try { deps.onRetried(); } catch (e) {} }   // focus back in the window
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
      stopTimers();
      st.ac = deps.AbortController ? new deps.AbortController() : new AbortController();
      st.station = station; st.data = null; st.obs = null; st.tries = 0; st.dirty = false; st.built = null; st.open = { 0: true };
      st.retried = false; st.refreshAt = 0;                                // a Retry sets its flag after this (G27 re-check RC-8)
      if (opts.unit) st.unit = unitOf(opts.unit);
      st.zone = opts.zone || null;
      if (els.strip) els.strip.textContent = '';
      if (els.meta) els.meta.textContent = '';
      setStatus('loading');
      attempt(st.seq);
      return st.seq;
    }
    function retry() { if (st.station) { load(st.station, { unit: st.unit, zone: st.zone }); st.retried = true; } }
    function clear() {
      st.seq++;
      if (st.ac) { try { st.ac.abort(); } catch (e) {} st.ac = null; }
      stopTimers();
      st.station = null; st.data = null; st.obs = null; st.built = null; st.retried = false; st.refreshAt = 0;
      if (els.strip) els.strip.textContent = '';
      setStatus('idle');
    }
    // the day headers and the readout: the strip's own listeners, which survive every rebuild
    function pointerX(e) {
      var svg = st.built && st.built.svg; if (!svg || !svg.getBoundingClientRect) return null;
      var p = e.touches && e.touches[0] ? e.touches[0] : e, r = svg.getBoundingClientRect();
      if (typeof p.clientX !== 'number') return null;
      if (p.clientY < r.top || p.clientY > r.bottom) return null;
      return p.clientX - r.left;
    }
    if (els.strip) {
      els.strip.addEventListener('click', function (e) {
        var b = e.target && e.target.closest ? e.target.closest('button[data-day]') : null;
        if (b) toggleDay(+b.getAttribute('data-day'));
      });
      // a tap's readout ends with the tap: the mouse events a browser sends after a touch are ignored (G27 B-P3-6)
      var touchedAt = -Infinity, wall = deps.wallClock || function () { return Date.now(); };
      els.strip.addEventListener('mousemove', function (e) { if (wall() - touchedAt > TOUCH_MOUSE_MS) readoutAt(pointerX(e)); });
      els.strip.addEventListener('mouseleave', function () { if (wall() - touchedAt > TOUCH_MOUSE_MS) readoutAt(null); });
      els.strip.addEventListener('touchstart', function (e) { touchedAt = wall(); readoutAt(pointerX(e)); }, { passive: true });
      els.strip.addEventListener('touchmove', function (e) { touchedAt = wall(); readoutAt(pointerX(e)); }, { passive: true });
      els.strip.addEventListener('touchend', function () { touchedAt = wall(); readoutAt(null); });
      els.strip.addEventListener('touchcancel', function () { touchedAt = wall(); readoutAt(null); });   // a pinch, a long press
    }
    if (els.retry) els.retry.addEventListener('click', retry);
    return {
      load: load, clear: clear, retry: retry, toggleDay: toggleDay,
      setUnit: function (u) { st.unit = unitOf(u); if (st.data) render(); },
      setZone: function (z) { st.zone = z || null; if (st.data) render(); },
      show: function () { if (st.data && (st.dirty || !st.built)) render(); return Promise.resolve(); },
      resize: function () { /* the strip is content-sized and scrolls: nothing to do */ },
      state: function () {
        return { seq: st.seq, status: st.status, station: st.station && st.station.id, hasData: !!st.data, hasObs: !!st.obs,
                 built: !!st.built, unit: st.unit, zone: zone(), tries: st.tries, open: Object.keys(st.open).filter(function (k) { return st.open[k]; }).map(Number),
                 retryTimer: st.retryTimer !== null, nowTimer: st.nowTimer !== null, readout: st.built ? st.built.readoutText : '',
                 layout: st.L, scale: st.ys };
      }
    };
  }

  var api = {
    createTideView: createTideView,
    _internals: {
      FT_PER_M: FT_PER_M, DAYS: DAYS, COL_CLOSED: COL_CLOSED, COL_OPEN: COL_OPEN, LABEL_W: LABEL_W, CHART_H: CHART_H, CHART_PAD: CHART_PAD,
      RETRY_MAX: RETRY_MAX, NOW_REDRAW_MS: NOW_REDRAW_MS, MOON_GLYPHS: MOON_GLYPHS,
      zoneClock: zoneClock, localMidnightBefore: localMidnightBefore, midnights: midnights, noons: noons, clockText: clockText, dayShort: dayShort,
      dayLong: dayLong, stampText: stampText, layout: layout, dayOf: dayOf, xOf: xOf, tOf: tOf, yScale: yScale, tickText: tickText,
      samples: samples, heightAt: heightAt, extremesIn: extremesIn, eventsIn: eventsIn, moonOf: moonOf, nightSpans: nightSpans,
      curvePaths: curvePaths, heightText: heightText, CALLOUT_EDGE: CALLOUT_EDGE, series: series, hasGap: hasGap,
      RETRY_MAX_S: RETRY_MAX_S, TOUCH_MOUSE_MS: TOUCH_MOUSE_MS, REFRESH_RETRY_MS: REFRESH_RETRY_MS, CALLOUT_CHAR_W: CALLOUT_CHAR_W
    }
  };
  if (typeof window !== 'undefined') window.AllshoreTides = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})();
