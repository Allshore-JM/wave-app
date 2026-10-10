/* Allshore Surf wind stations (plan section 39). Loaded on every page; touches no DOM until a function below is called
 * with the page's elements, so tests/ui/winds.test.js evaluates it in Node with fakes.
 *
 *  - BANDS: the flags' colours by speed, iKitesurf-style (owner 2026-10-09), in whole knots: calm under 1, light 1-9,
 *    moderate 10-15, fresh 16-21, strong 22-30, gale 31 and up.
 *  - buildFlag / updateFlag: a station's flag (a 40-px box for L.divIcon): a ring at the station, an arrow from it
 *    pointing the way the wind BLOWS (downwind, as iKitesurf), and the speed in the site's unit (US: mph, Metric: km/h)
 *    14 px UPWIND of the ring, so the number never sits under the arrow. Calm: a ring with "0". A speed without a
 *    direction (METAR "VRB"): the number above the ring, no arrow. No reading: a small grey ring. A reading older than
 *    stale_s (the server's: 2 h) is drawn grey. updateFlag rewrites text, classes and styles IN PLACE (the element, its
 *    listeners and its focus survive a refresh). Text only: every string is a text node.
 *  - createWindFeed: /api/wind/latest every 5 minutes while the layer shows (nothing while the tab is hidden; a missed
 *    turn is taken when it shows again), 8-s deadline, retries after 5 / 10 / 30 s, a partial answer (header
 *    X-Wind-Stations-Partial: feeds the server is still loading) asked again every 5 s (at most 24 times).
 *  - createWindView: the wind window's last 24 hours of one station (/api/wind/<id>/history): an SVG chart (speed solid,
 *    gusts dashed, the site's unit on the left, knots on the right, hour ticks every 3 h (6 h when narrow), local
 *    midnights marked, gaps where readings are more than 90 minutes apart), a row of direction arrows (one per 28 px at
 *    most), a hover / touch readout, a newest-first table (48 rows), the current reading, the source. A busy server
 *    (503 + retry) is asked again after its Retry-After; an unknown station says so; any other failure offers Retry. A
 *    newer load() voids every answer of an older one (sequence + AbortController). The chart is built only while the
 *    window shows its body. The age of the reading is updated every minute; the readings are asked again every 5 min.
 */
(function () {
  'use strict';

  var KT_MS = 0.514444, MPH_PER_MS = 2.236936, KMH_PER_MS = 3.6;
  var MIN_MS = 60000, HOUR_MS = 3600000;
  var STALE_S = 7200;                     // the server's stale_s, when an answer lacks it
  var FLAG = 40, NUM_OFFSET = 14;         // px: the flag's box; the number's distance upwind of the station
  var BANDS = [
    { name: 'calm', below: 1, color: '#8a96a3', label: 'Calm (under 1 kt)' },
    { name: 'light', below: 10, color: '#339af0', label: 'Light (1-9 kt)' },
    { name: 'moderate', below: 16, color: '#2f9e44', label: 'Moderate (10-15 kt)' },
    { name: 'fresh', below: 22, color: '#f59f00', label: 'Fresh (16-21 kt)' },
    { name: 'strong', below: 31, color: '#e03131', label: 'Strong (22-30 kt)' },
    { name: 'gale', below: Infinity, color: '#9c36b5', label: 'Gale (31 kt and up)' }
  ];
  var FLAG_CLASSES = ['wind-none', 'wind-calm', 'wind-var', 'wind-dir', 'wind-stale'].concat(BANDS.map(function (b) { return 'wind-' + b.name; }));
  var COMPASS = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];
  var SVG_NS = 'http://www.w3.org/2000/svg';
  // the arrow, pointing up (north) from the ring's edge to its tip; rotated to the downwind bearing about the box's centre
  var ARROW_PATH = 'M20 1 L25.5 10 L21.6 9 L21.6 15 L18.4 15 L18.4 9 L14.5 10 Z';
  // the chart's direction arrows: 24 px long, centred on (20, 20), pointing up before their rotation
  var SLOT_ARROW_PATH = 'M20 8 L24.5 16 L21.3 15 L21.3 32 L18.7 32 L18.7 15 L15.5 16 Z';

  // ---- the feed's schedule
  var FEED_INTERVAL_MS = 5 * MIN_MS, FEED_DEADLINE_MS = 8000, FEED_PARTIAL_MS = 5000, FEED_PARTIAL_MAX = 24;
  var FEED_RETRY_MS = [5000, 10000, 30000];
  var FEED_STATUS = { loading: 'loading…', partial: 'loading more…', retrying: 'retrying…', unavailable: 'unavailable' };

  // ---- the view's geometry and schedule
  var CHART_H = 190, ARROWS_H = 34, MARGIN = { l: 42, r: 36, t: 10, b: 24 };
  var ARROW_SLOT = 28;                    // px: at most one direction arrow per this much width
  var GAP_S = 90 * 60;                    // readings further apart than this: a gap in the lines
  var NARROW = 480;                       // px: below this width the hour ticks are 6 h apart (else 3 h)
  var TABLE_MAX = 48, READOUT_NEAR_S = 45 * 60, MIN_WIDTH = 280, DEFAULT_WIDTH = 640, RESIZE_PX = 8;
  var REFRESH_MS = 5 * MIN_MS, TICK_MS = MIN_MS;
  var RETRY_MAX = 24, RETRY_DEFAULT_S = 5, RETRY_MAX_S = 60, TOUCH_MOUSE_MS = 700;
  var COLORS = { speed: '#1d6fd6', gust: '#e8590c', grid: 'rgba(0,0,0,0.12)', midnight: 'rgba(0,0,0,0.38)', text: '#1d2b4f',
                 muted: '#5b6577' };
  var WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  var MSG = {
    unknown: 'This wind station is not known.',
    failed: 'The wind readings could not be loaded.',
    busy: 'The wind readings are not available yet; try again in a moment.',
    empty: 'No wind readings in the last 24 hours.',
    noHistory: 'No 24-hour history for this station.'          // the window shows the flag's own reading (F4)
  };
  // ---- speeds, bands, directions ---------------------------------------------------------------------------------
  function num(v) { return typeof v === 'number' && isFinite(v); }
  function unitOf(u) { return u === 'Metric' ? 'Metric' : 'US'; }
  function unitName(unit) { return unitOf(unit) === 'Metric' ? 'km/h' : 'mph'; }
  function inUnit(ms, unit) { return ms * (unitOf(unit) === 'Metric' ? KMH_PER_MS : MPH_PER_MS); }
  function speedValue(ms, unit) { return Math.round(inUnit(ms, unit)) + 0; }                 // never "-0"
  function speedText(ms, unit) { return speedValue(ms, unit) + ' ' + unitName(unit); }       // "9 mph", "15 km/h"
  function knots(ms) { return Math.round(ms / KT_MS) + 0; }
  function knotsText(ms) { return knots(ms) + ' kt'; }
  function bandOf(ms) {                   // by the speed in WHOLE knots (METAR reports whole knots)
    var kt = num(ms) ? knots(ms) : 0;
    for (var i = 0; i < BANDS.length; i++) if (kt < BANDS[i].below) return BANDS[i];
    return BANDS[BANDS.length - 1];
  }
  function arrowDeg(from) { return ((from % 360) + 360 + 180) % 360; }          // the way the wind blows (downwind)
  function compass(d) { return COMPASS[Math.round(((d % 360) + 360) % 360 / 22.5) % 16]; }
  function dirText(d) { return compass(d) + ' (' + Math.round(d) % 360 + '°)'; }               // "ENE (75°)"
  // the number's centre in the flag's box: NUM_OFFSET px toward where the wind comes FROM
  function numberAt(from) {
    var r = from * Math.PI / 180;
    return { x: +(FLAG / 2 + NUM_OFFSET * Math.sin(r)).toFixed(1), y: +(FLAG / 2 - NUM_OFFSET * Math.cos(r)).toFixed(1) };
  }
  function ageText(seconds) {
    if (!(seconds >= 60)) return 'just now';
    var m = Math.floor(seconds / 60);
    if (m < 60) return m + ' min ago';
    var h = Math.floor(m / 60), r = m % 60;
    return h < 6 && r ? h + ' h ' + r + ' min ago' : h + ' h ago';
  }
  // a reading {t (epoch s), s (m/s), g, d} -> the words for it ("9 mph from ENE (75°), gusts 14 mph · 12 min ago")
  function readingText(r, nowMs, unit, withAge) {
    if (!r || !num(r.s)) return 'No recent reading';
    var band = bandOf(r.s), out;
    if (band.name === 'calm') out = 'Calm';
    else if (!num(r.d)) out = 'Variable, ' + speedText(r.s, unit);
    else out = speedText(r.s, unit) + ' from ' + dirText(r.d);
    if (num(r.g) && r.g > r.s && knots(r.g) > knots(r.s)) out += ', gusts ' + speedText(r.g, unit);
    if (withAge !== false && num(r.t)) out += ' · ' + ageText(nowMs / 1000 - r.t);
    return out;
  }

  // ---- the flag ----------------------------------------------------------------------------------------------------
  // What a flag shows for a reading (pure): its kind, classes, number and where it sits, the arrow's rotation.
  function flagState(r, nowMs, unit, staleS) {
    var mid = FLAG / 2;
    if (!r || !num(r.s)) return { kind: 'none', band: null, classes: ['wind-none'], num: '', x: mid, y: mid, arrow: null, title: readingText(null) };
    var band = bandOf(r.s), stale = num(r.t) && nowMs / 1000 - r.t > (num(staleS) ? staleS : STALE_S);
    var kind = band.name === 'calm' ? 'calm' : num(r.d) ? 'dir' : 'var';
    var at = kind === 'dir' ? numberAt(r.d) : kind === 'var' ? { x: mid, y: mid - NUM_OFFSET } : { x: mid, y: mid };
    var classes = ['wind-' + kind, 'wind-' + band.name];
    if (stale) classes.push('wind-stale');
    return { kind: kind, band: band.name, classes: classes, num: kind === 'calm' ? '0' : String(speedValue(r.s, unit)),
             x: at.x, y: at.y, arrow: kind === 'dir' ? arrowDeg(r.d) : null, stale: stale, title: readingText(r, nowMs, unit) };
  }
  function addClasses(e, cls) { cls.split(' ').forEach(function (c) { if (c) e.classList.add(c); }); return e; }
  // The flag's element (the divIcon's html): div.wind-flag > svg.wind-arrow, span.wind-ring, span.wind-num.
  function buildFlag(doc, r, nowMs, unit, staleS) {
    var root = addClasses(doc.createElement('div'), 'wind-flag');
    root.setAttribute('aria-hidden', 'true');                               // the marker carries the words (its label)
    var svg = doc.createElementNS(SVG_NS, 'svg');
    svg.classList.add('wind-arrow');
    svg.setAttribute('width', String(FLAG)); svg.setAttribute('height', String(FLAG)); svg.setAttribute('viewBox', '0 0 ' + FLAG + ' ' + FLAG);
    var path = doc.createElementNS(SVG_NS, 'path');
    path.setAttribute('d', ARROW_PATH);
    svg.appendChild(path);
    var ring = addClasses(doc.createElement('span'), 'wind-ring');
    var n = addClasses(doc.createElement('span'), 'wind-num');
    root.appendChild(svg); root.appendChild(ring); root.appendChild(n);
    root.windParts = { arrow: svg, ring: ring, num: n };
    updateFlag(root, r, nowMs, unit, staleS);
    return root;
  }
  // The flag rewritten in place for a new reading / unit / time: classes, the number's text and place, the arrow's
  // rotation. Returns the state drawn.
  function updateFlag(root, r, nowMs, unit, staleS) {
    var s = flagState(r, nowMs, unit, staleS);
    var p = root.windParts || { arrow: root.querySelector('.wind-arrow'), ring: root.querySelector('.wind-ring'), num: root.querySelector('.wind-num') };
    FLAG_CLASSES.forEach(function (c) { root.classList.remove(c); });
    s.classes.forEach(function (c) { root.classList.add(c); });
    if (p.num) {
      if (p.num.textContent !== s.num) p.num.textContent = s.num;
      p.num.style.left = s.x + 'px'; p.num.style.top = s.y + 'px';
    }
    if (p.arrow) {
      if (s.arrow === null) p.arrow.style.display = 'none';
      else { p.arrow.style.display = ''; p.arrow.style.transform = 'rotate(' + s.arrow + 'deg)'; }
    }
    root.setAttribute('data-kind', s.kind);
    return s;
  }

  // ---- the feed of latest readings ---------------------------------------------------------------------------------
  // /api/wind/latest -> {now, stale_s, fields: [id, t, s, g, d], rows, missing}. readings(payload) -> {id: reading}.
  function readings(payload) {
    var out = {};
    if (!payload || !Array.isArray(payload.rows) || !Array.isArray(payload.fields)) return out;
    var f = payload.fields, it = f.indexOf('id'), tt = f.indexOf('t'), ts = f.indexOf('s'), tg = f.indexOf('g'), td = f.indexOf('d');
    if (it < 0 || tt < 0 || ts < 0) return out;
    payload.rows.forEach(function (row) {
      if (!Array.isArray(row) || typeof row[it] !== 'string' || !num(row[tt]) || !num(row[ts])) return;
      out[row[it]] = { t: row[tt], s: row[ts], g: tg >= 0 && num(row[tg]) ? row[tg] : null, d: td >= 0 && num(row[td]) ? row[td] : null };
    });
    return out;
  }
  function validLatest(b) { return !!b && typeof b === 'object' && Array.isArray(b.rows) && Array.isArray(b.fields); }
  function splitList(h) { return h ? String(h).split(',').map(function (x) { return x.trim(); }).filter(Boolean) : []; }

  function createWindFeed(opts) {
    opts = opts || {};
    var url = opts.url || '/api/wind/latest';
    var timers = opts.timers || { set: function (f, ms) { return setTimeout(f, ms); }, clear: function (t) { clearTimeout(t); } };
    var visibility = opts.visibility || { isVisible: function () { return true; }, onChange: function () {} };
    var intervalMs = opts.intervalMs || FEED_INTERVAL_MS, deadlineMs = opts.deadlineMs || FEED_DEADLINE_MS;
    var AbortCtl = opts.AbortController !== undefined ? opts.AbortController
      : (typeof AbortController !== 'undefined' ? AbortController : null);
    var st = { running: false, gen: 0, timer: null, ctl: null, failures: 0, partials: 0, waitingVisible: false, last: null,
               status: null, attempts: 0, listening: false };
    var onData = function () {}, onStatus = function () {};

    function setStatus(code) {
      if (st.status === code) return;
      st.status = code;
      try { onStatus(code, FEED_STATUS[code] || ''); } catch (e) {}
    }
    function schedule(ms) {
      if (!st.running) return;
      if (st.timer !== null) timers.clear(st.timer);
      st.timer = timers.set(function () {
        st.timer = null;
        if (!visibility.isVisible()) { st.waitingVisible = true; return; }   // asked when the tab shows again
        attempt();
      }, ms);
    }
    function failed() {
      st.failures += 1;
      setStatus(st.last ? 'retrying' : 'unavailable');
      schedule(FEED_RETRY_MS[Math.min(st.failures - 1, FEED_RETRY_MS.length - 1)]);
    }
    function attempt() {
      if (!st.running) return;
      var gen = ++st.gen;
      if (st.ctl) { try { st.ctl.abort(); } catch (e) {} }
      st.ctl = AbortCtl ? new AbortCtl() : null;
      st.attempts += 1;
      if (!st.last) setStatus('loading');
      var deadline = null;
      var timeout = new Promise(function (_, reject) {
        deadline = timers.set(function () { deadline = null; if (st.ctl && gen === st.gen) { try { st.ctl.abort(); } catch (e) {} } reject(new Error('deadline')); }, deadlineMs);
      });
      var req = Promise.resolve().then(function () {
        return opts.fetch(url, { signal: st.ctl ? st.ctl.signal : undefined, headers: { Accept: 'application/json' } });
      }).then(function (r) {
        if (!r || !r.ok) throw new Error('HTTP ' + (r && r.status));
        var partial = splitList(r.headers && r.headers.get ? r.headers.get('X-Wind-Stations-Partial') : null);
        return r.json().then(function (b) {
          if (!validLatest(b)) throw new Error('bad answer');
          return { body: b, partial: partial };
        });
      });
      Promise.race([req, timeout]).then(function (res) {
        if (deadline !== null) { timers.clear(deadline); deadline = null; }
        if (gen !== st.gen || !st.running) return;
        st.failures = 0;
        st.last = res.body;
        try { onData(res.body, { partial: res.partial, readings: readings(res.body) }); } catch (e) {}
        if (res.partial.length && st.partials < FEED_PARTIAL_MAX) {
          st.partials += 1;
          setStatus('partial');
          schedule(FEED_PARTIAL_MS);
        } else {
          st.partials = 0;
          setStatus('');
          schedule(intervalMs);
        }
      }, function () {
        if (deadline !== null) { timers.clear(deadline); deadline = null; }
        if (gen !== st.gen || !st.running) return;
        failed();
      });
    }
    function onVisibilityChange() {
      if (!st.running || !st.waitingVisible || !visibility.isVisible()) return;
      st.waitingVisible = false;
      attempt();
    }
    return {
      // start (or restart after stop): asks at once; handlers {onData(payload, info), onStatus(code, text)}
      start: function (handlers) {
        handlers = handlers || {};
        if (typeof handlers.onData === 'function') onData = handlers.onData;
        if (typeof handlers.onStatus === 'function') onStatus = handlers.onStatus;
        if (!st.listening) { st.listening = true; visibility.onChange(onVisibilityChange); }
        if (st.running) return;
        st.running = true; st.failures = 0; st.partials = 0; st.waitingVisible = false;
        if (!visibility.isVisible()) { st.waitingVisible = true; return; }
        attempt();
      },
      stop: function () {
        st.running = false; st.gen += 1; st.waitingVisible = false;
        if (st.timer !== null) { timers.clear(st.timer); st.timer = null; }
        if (st.ctl) { try { st.ctl.abort(); } catch (e) {} st.ctl = null; }
        setStatus('');
      },
      last: function () { return st.last; },
      state: function () {
        return { running: st.running, failures: st.failures, partials: st.partials, attempts: st.attempts, status: st.status,
                 timer: st.timer !== null, waitingVisible: st.waitingVisible };
      }
    };
  }

  // ---- clocks in a zone --------------------------------------------------------------------------------------------
  var FMT_CACHE = {};
  function zoneClock(tz) {
    var key = 'z:' + (tz || '');                                          // never an Object member ("constructor")
    if (FMT_CACHE[key]) return FMT_CACHE[key];
    var f = null, zone = 'UTC';
    var o = { hourCycle: 'h23', weekday: 'short', month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' };
    try { f = new Intl.DateTimeFormat('en-US', Object.assign({ timeZone: tz || 'UTC' }, o)); zone = tz || 'UTC'; }
    catch (e) { try { f = new Intl.DateTimeFormat('en-US', Object.assign({ timeZone: 'UTC' }, o)); } catch (e2) { f = null; } }
    function parts(ms) {
      var d = new Date(ms);
      if (!f) return { mo: d.getUTCMonth() + 1, d: d.getUTCDate(), h: d.getUTCHours(), mi: d.getUTCMinutes(), wd: WEEKDAYS[d.getUTCDay()] };
      var got = {}, p = f.formatToParts(d);
      for (var i = 0; i < p.length; i++) got[p[i].type] = p[i].value;
      return { mo: +got.month, d: +got.day, h: (+got.hour) % 24, mi: +got.minute, wd: got.weekday };
    }
    FMT_CACHE[key] = { zone: zone, parts: parts };
    return FMT_CACHE[key];
  }
  function clockText(p) { var h = p.h % 12 || 12; return h + ':' + (p.mi < 10 ? '0' : '') + p.mi + ' ' + (p.h < 12 ? 'AM' : 'PM'); }
  function hourText(p) { return (p.h % 12 || 12) + ' ' + (p.h < 12 ? 'AM' : 'PM'); }            // "3 PM"
  function dateText(p) { return p.wd + ' ' + p.mo + '/' + p.d; }                                  // "Sat 10/10"
  function stampText(p) { return dateText(p) + ', ' + clockText(p); }                             // "Sat 10/10, 3:05 PM"
  // The instants in [t0, t1] (ms) where the zone's clock reads a whole hour divisible by `step` (half-hour zones
  // included: the clock is read every 15 minutes). [{t, p, midnight}]
  function hourMarks(t0, t1, step, clk) {
    var out = [], q = 15 * MIN_MS;
    for (var t = Math.ceil(t0 / q) * q; t <= t1; t += q) {
      var p = clk.parts(t);
      if (p.mi === 0 && p.h % step === 0) out.push({ t: t, p: p, midnight: p.h === 0 });
    }
    return out;
  }
  // A nice step for a scale from 0 to `max` with at most `n` steps (1, 2, 5 x 10^k).
  function niceStep(max, n) {
    var raw = Math.max(max, 1e-9) / (n || 5), mag = Math.pow(10, Math.floor(Math.log(raw) / Math.LN10));
    var steps = [1, 2, 5, 10];
    for (var i = 0; i < steps.length; i++) if (steps[i] * mag >= raw - 1e-12) return steps[i] * mag;
    return 10 * mag;
  }
  // The history payload -> readings in time order [{t, s, g, d}] (the server's columns; anything malformed skipped).
  function historyRows(b) {
    var out = [];
    if (!b || !Array.isArray(b.t)) return out;
    for (var i = 0; i < b.t.length; i++) {
      var t = b.t[i], s = b.s && b.s[i];
      if (!num(t) || !num(s)) continue;
      out.push({ t: t, s: s, g: b.g && num(b.g[i]) ? b.g[i] : null, d: b.d && num(b.d[i]) ? b.d[i] : null });
    }
    out.sort(function (a, c) { return a.t - c.t; });
    return out;
  }
  // Polyline path pieces over the rows: a new piece after a gap of more than GAP_S or a missing value.
  function linePath(rows, key, x, y) {
    var d = '', prev = null;
    rows.forEach(function (r) {
      var v = r[key];
      if (!num(v)) { prev = null; return; }
      var cmd = prev !== null && r.t - prev <= GAP_S ? 'L' : 'M';
      d += cmd + x(r.t).toFixed(1) + ' ' + y(v).toFixed(1);
      prev = r.t;
    });
    return d;
  }
  // One direction arrow per slot at most: the reading nearest each slot's middle, within half a slot.
  function arrowSlots(rows, t0, t1, plotW) {
    var n = Math.max(1, Math.floor(plotW / ARROW_SLOT)), span = (t1 - t0) / n, out = [], j = 0;
    for (var k = 0; k < n; k++) {
      var mid = t0 + (k + 0.5) * span, best = null;
      while (j < rows.length && rows[j].t < mid - span / 2) j++;
      for (var i = j; i < rows.length && rows[i].t <= mid + span / 2; i++) {
        if (best === null || Math.abs(rows[i].t - mid) < Math.abs(best.t - mid)) best = rows[i];
      }
      if (best) out.push({ slot: k, mid: mid, r: best });
    }
    return out;
  }
  function nearest(rows, t, within) {
    var best = null;
    rows.forEach(function (r) { if (best === null || Math.abs(r.t - t) < Math.abs(best.t - t)) best = r; });
    return best && Math.abs(best.t - t) <= within ? best : null;
  }
  function sourceLink(src, id) {
    var local = String(id || '').split(':')[1] || '';
    if (src === 'ndbc') return 'https://www.ndbc.noaa.gov/station_page.php?station=' + encodeURIComponent(local.toLowerCase());
    if (src === 'coops') return 'https://tidesandcurrents.noaa.gov/stationhome.html?id=' + encodeURIComponent(local);
    if (src === 'metar') return 'https://aviationweather.gov/data/metar/?ids=' + encodeURIComponent(local) + '&hours=24';
    return null;
  }

  // ---- the view ----------------------------------------------------------------------------------------------------
  function createWindView(deps) {
    var els = deps.els, doc = deps.document;
    var now = deps.now || function () { return Date.now(); };
    var timers = deps.timers || { set: function (f, ms) { return setTimeout(f, ms); }, clear: function (id) { clearTimeout(id); } };
    var st = { seq: 0, station: null, data: null, rows: [], unit: unitOf(deps.unit), zone: null, dirty: false, built: null,
               ac: null, retryTimer: null, tickTimer: null, tries: 0, status: 'idle', retried: false, width: 0, fetchedAt: 0 };

    function zone() { return st.zone || (st.station && st.station.tz) || (st.data && st.data.tz) || 'UTC'; }
    function zoneAt(ms) { try { if (deps.zoneAbbr) return deps.zoneAbbr(ms, zone()) || zone(); } catch (e) {} return zone(); }
    function show(e, on) { if (e) e.classList.toggle('d-none', !on); }
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
      if (st.tickTimer !== null) { timers.clear(st.tickTimer); st.tickTimer = null; }
    }
    function el(tag, cls, text) { var e = doc.createElement(tag); if (cls) addClasses(e, cls); if (text !== undefined) e.textContent = text; return e; }
    function svgEl(tag, attrs) {
      var e = doc.createElementNS(SVG_NS, tag);
      for (var k in attrs) { if (attrs[k] === undefined || attrs[k] === null) continue; if (k === 'class') addClasses(e, attrs[k]); else e.setAttribute(k, attrs[k]); }
      return e;
    }
    function kidsOf(e) { return e.childNodes && e.childNodes.length ? e.childNodes : e.children; }
    function width() {
      var w = 0;
      try { w = deps.width ? deps.width() : (els.chart && els.chart.clientWidth) || 0; } catch (e) { w = 0; }
      return Math.max(MIN_WIDTH, Math.round(w || DEFAULT_WIDTH));
    }
    function staleS() { return st.data && num(st.data.stale_s) ? st.data.stale_s : STALE_S; }
    function latest() { return st.rows.length ? st.rows[st.rows.length - 1] : null; }

    function writeCurrent() {
      if (!els.current) return;
      var r = latest() || st.fallback, t = now();                      // no history: the flag's own latest reading (F4)
      els.current.textContent = '';
      if (!r) { els.current.appendChild(el('span', 'wind-cur-text', MSG.empty)); return; }
      var s = flagState(r, t, st.unit, staleS());
      var dot = el('span', 'wind-cur-dot wind-' + (s.band || 'none') + (s.stale ? ' wind-stale' : ''));
      dot.setAttribute('aria-hidden', 'true');
      els.current.appendChild(dot);
      var words = readingText(r, t, st.unit, false) + (s.kind === 'calm' ? '' : ' · ' + knotsText(r.s));
      els.current.appendChild(el('span', 'wind-cur-text', words));
      els.current.appendChild(el('span', 'wind-cur-age' + (s.stale ? ' wind-cur-stale' : ''), ' · ' + ageText(t / 1000 - r.t)));
    }
    function writeMeta() {
      var d = st.data, s = st.station || {};
      if (!els.meta || !d) return;
      els.meta.textContent = '';
      var src = d.src || String(d.id || '').split(':')[0];
      // the zone the readings are shown in, and nothing more (owner, 2026-10-10: the window's subtitle names the
      // station; the units and the arrows' meaning need no sentence); then the notes, then the source
      var lines = ['Times in ' + zoneAt(now()) + '.'];
      if (d.via === 'ndbc') lines.push('Readings from NDBC’s copy of this gauge (NOAA CO-OPS did not answer).');
      if (d.note) lines.push(d.note + (/[.!?]$/.test(d.note) ? '' : '.'));
      if (latest() && flagState(latest(), now(), st.unit, staleS()).stale) lines.push('The latest reading is more than ' + Math.round(staleS() / 3600) + ' hours old.');
      lines.forEach(function (t) { els.meta.appendChild(el('div', '', t)); });
      var line = el('div', ''), href = sourceLink(src, d.id);
      line.appendChild(doc.createTextNode('Source: '));
      if (href) {
        var a = el('a', '', d.source || src);
        a.setAttribute('href', href); a.setAttribute('target', '_blank'); a.setAttribute('rel', 'noopener');
        line.appendChild(a);
      } else line.appendChild(doc.createTextNode(d.source || src));
      els.meta.appendChild(line);
    }
    function writeTable() {
      if (!els.table) return;
      els.table.textContent = '';
      var clk = zoneClock(zone()), table = el('table', 'table table-sm wind-table');
      table.appendChild(el('caption', '', 'Readings, newest first (times in ' + zoneAt(now()) + ')'));
      var thead = el('thead'), hr = el('tr');
      ['Time', 'Speed', 'Gust', 'Direction'].forEach(function (h) { var th = el('th', '', h); th.setAttribute('scope', 'col'); hr.appendChild(th); });
      thead.appendChild(hr); table.appendChild(thead);
      var tbody = el('tbody'), rows = st.rows.slice(-TABLE_MAX).reverse();
      rows.forEach(function (r) {
        var tr = el('tr', 'wind-' + bandOf(r.s).name);
        var th = el('th', '', stampText(clk.parts(r.t * 1000))); th.setAttribute('scope', 'row'); tr.appendChild(th);
        tr.appendChild(el('td', '', speedText(r.s, st.unit) + ' (' + knotsText(r.s) + ')'));
        tr.appendChild(el('td', '', num(r.g) ? speedText(r.g, st.unit) : '–'));
        tr.appendChild(el('td', '', bandOf(r.s).name === 'calm' ? 'Calm' : num(r.d) ? dirText(r.d) : 'Variable'));
        tbody.appendChild(tr);
      });
      table.appendChild(tbody);
      els.table.appendChild(table);
    }
    function buildCharts() {
      var W = width(), plotW = W - MARGIN.l - MARGIN.r, plotH = CHART_H - MARGIN.t - MARGIN.b;
      var t1 = now() / 1000, t0 = t1 - 86400, rows = st.rows.filter(function (r) { return r.t >= t0 - 60 && r.t <= t1 + 600; });
      var clk = zoneClock(zone()), unit = st.unit;
      var top = 0;
      rows.forEach(function (r) { top = Math.max(top, inUnit(r.s, unit), num(r.g) ? inUnit(r.g, unit) : 0); });
      var minTop = unitOf(unit) === 'Metric' ? 20 : 10;                    // a calm day still has a readable scale
      var step = niceStep(Math.max(top, minTop) * 1.05, 5), hi = Math.ceil(Math.max(top, minTop) * 1.05 / step) * step;
      function x(t) { return MARGIN.l + (t - t0) / (t1 - t0) * plotW; }
      function y(ms) { return MARGIN.t + plotH - inUnit(ms, unit) / hi * plotH; }
      function yv(v) { return MARGIN.t + plotH - v / hi * plotH; }
      var svg = svgEl('svg', { 'class': 'wind-svg', width: W, height: CHART_H, viewBox: '0 0 ' + W + ' ' + CHART_H, role: 'img',
                               'aria-label': 'Wind speed and gusts over the last 24 hours' });
      for (var v = 0; v <= hi + 1e-9; v += step) {                          // the site's unit on the left
        var gy = yv(v).toFixed(1);
        svg.appendChild(svgEl('line', { 'class': 'wind-grid', x1: MARGIN.l, x2: W - MARGIN.r, y1: gy, y2: gy, stroke: COLORS.grid, 'stroke-width': 0.7 }));
        var lt = svgEl('text', { 'class': 'wind-ytick', x: MARGIN.l - 5, y: (+gy + 3.5).toFixed(1), 'text-anchor': 'end', 'font-size': 10.5, fill: COLORS.text });
        lt.textContent = String(Math.round(v * 100) / 100); svg.appendChild(lt);
      }
      var ul = svgEl('text', { 'class': 'wind-yunit', x: 4, y: MARGIN.t + 8, 'font-size': 10, fill: COLORS.muted }); ul.textContent = unitName(unit); svg.appendChild(ul);
      var hiKt = hi / inUnit(KT_MS, unit), kstep = niceStep(hiKt, 5);       // knots on the right
      for (var k = 0; k <= hiKt + 1e-9; k += kstep) {
        var ky = y(k * KT_MS).toFixed(1);
        var kt = svgEl('text', { 'class': 'wind-ktick', x: W - MARGIN.r + 5, y: (+ky + 3.5).toFixed(1), 'font-size': 10.5, fill: COLORS.muted });
        kt.textContent = String(Math.round(k * 100) / 100); svg.appendChild(kt);
      }
      var kl = svgEl('text', { 'class': 'wind-kunit', x: W - 4, y: MARGIN.t + 8, 'text-anchor': 'end', 'font-size': 10, fill: COLORS.muted }); kl.textContent = 'kt'; svg.appendChild(kl);
      var marks = hourMarks(t0 * 1000, t1 * 1000, W < NARROW ? 6 : 3, clk);
      marks.forEach(function (m) {
        var mx = x(m.t / 1000).toFixed(1);
        svg.appendChild(svgEl('line', { 'class': m.midnight ? 'wind-midnight' : 'wind-hour', x1: mx, x2: mx, y1: MARGIN.t, y2: MARGIN.t + plotH,
                                       stroke: m.midnight ? COLORS.midnight : COLORS.grid, 'stroke-width': m.midnight ? 1.2 : 0.7 }));
        var ht = svgEl('text', { 'class': 'wind-xtick' + (m.midnight ? ' wind-xdate' : ''), x: mx, y: CHART_H - 7, 'text-anchor': 'middle',
                                 'font-size': 10.5, 'font-weight': m.midnight ? 600 : 400, fill: COLORS.text });
        ht.textContent = m.midnight ? dateText(m.p) : hourText(m.p); svg.appendChild(ht);
      });
      var gust = linePath(rows, 'g', x, y), speed = linePath(rows, 's', x, y);
      if (gust) svg.appendChild(svgEl('path', { 'class': 'wind-gust', d: gust, fill: 'none', stroke: COLORS.gust, 'stroke-width': 1.3, 'stroke-dasharray': '4 3' }));
      if (speed) svg.appendChild(svgEl('path', { 'class': 'wind-speed', d: speed, fill: 'none', stroke: COLORS.speed, 'stroke-width': 2, 'stroke-linejoin': 'round' }));
      var last = rows.length ? rows[rows.length - 1] : null;
      if (last) svg.appendChild(svgEl('circle', { 'class': 'wind-last', cx: x(last.t).toFixed(1), cy: y(last.s).toFixed(1), r: 3.5, fill: COLORS.speed, stroke: '#fff', 'stroke-width': 1.5 }));
      if (!rows.length) {
        var e = svgEl('text', { 'class': 'wind-empty', x: (MARGIN.l + plotW / 2).toFixed(1), y: (MARGIN.t + plotH / 2).toFixed(1), 'text-anchor': 'middle', 'font-size': 12, fill: COLORS.muted });
        e.textContent = st.fallback ? MSG.noHistory : MSG.empty; svg.appendChild(e);
      }
      var rd = svgEl('g', { 'class': 'wind-readout', visibility: 'hidden' });
      rd.appendChild(svgEl('line', { x1: 0, x2: 0, y1: MARGIN.t, y2: MARGIN.t + plotH, stroke: 'rgba(0,0,0,0.45)', 'stroke-width': 1 }));
      rd.appendChild(svgEl('circle', { cx: 0, cy: 0, r: 3, fill: '#fff', stroke: COLORS.speed, 'stroke-width': 1.5 }));
      rd.appendChild(svgEl('rect', { x: 0, y: 1, width: 150, height: 16, rx: 3, fill: 'rgba(255,255,255,0.94)', stroke: 'rgba(0,0,0,0.25)' }));
      var rt = svgEl('text', { x: 0, y: 13, 'font-size': 11, fill: COLORS.text }); rt.textContent = ''; rd.appendChild(rt);
      svg.appendChild(rd);
      // the direction arrows under the chart, on the same time axis
      var asvg = svgEl('svg', { 'class': 'wind-arrows-svg', width: W, height: ARROWS_H, viewBox: '0 0 ' + W + ' ' + ARROWS_H, role: 'img',
                                'aria-label': 'Wind direction over the last 24 hours' });
      arrowSlots(rows, t0, t1, plotW).forEach(function (a) {
        var ax = x(a.r.t), band = bandOf(a.r.s).name;
        if (!num(a.r.d) || band === 'calm') {
          asvg.appendChild(svgEl('circle', { 'class': 'wind-dir-none wind-' + band, cx: ax.toFixed(1), cy: ARROWS_H / 2, r: 2.5, fill: COLORS.muted }));
          return;
        }
        var g = svgEl('g', { 'class': 'wind-dir wind-' + band, transform: 'translate(' + (ax - FLAG / 2).toFixed(1) + ' ' + (ARROWS_H / 2 - FLAG / 2) + ') rotate(' + arrowDeg(a.r.d) + ' 20 20)' });
        g.appendChild(svgEl('path', { d: SLOT_ARROW_PATH, fill: bandOf(a.r.s).color }));   // 24 px: never into the next slot
        asvg.appendChild(g);
      });
      return { svg: svg, arrows: asvg, readout: rd, x: x, y: y, t0: t0, t1: t1, rows: rows, W: W };
    }
    function build() {
      if (!st.data) return;
      var c = buildCharts();
      if (els.chart) { els.chart.textContent = ''; els.chart.appendChild(c.svg); }
      if (els.arrows) { els.arrows.textContent = ''; els.arrows.appendChild(c.arrows); }
      st.built = { svg: c.svg, readout: c.readout, x: c.x, y: c.y, t0: c.t0, t1: c.t1, rows: c.rows, readoutText: '' };
      st.width = c.W;
      writeCurrent(); writeTable(); writeMeta();
      st.dirty = false;
    }
    function readoutAt(px) {
      var b = st.built; if (!b) return;
      var kids = kidsOf(b.readout), t = b.t0 + (px - MARGIN.l) / (st.width - MARGIN.l - MARGIN.r) * (b.t1 - b.t0);
      var r = px === null || px < MARGIN.l || px > st.width - MARGIN.r ? null : nearest(b.rows, t, READOUT_NEAR_S);
      if (!r) { b.readout.setAttribute('visibility', 'hidden'); b.readoutText = ''; return; }
      var clk = zoneClock(zone()), rx = b.x(r.t);
      var text = stampText(clk.parts(r.t * 1000)) + ' · ' + readingText(r, now(), st.unit, false) + ' · ' + knotsText(r.s);
      b.readout.setAttribute('visibility', 'visible');
      kids[0].setAttribute('x1', rx.toFixed(1)); kids[0].setAttribute('x2', rx.toFixed(1));
      kids[1].setAttribute('cx', rx.toFixed(1)); kids[1].setAttribute('cy', b.y(r.s).toFixed(1));
      var w = Math.min(st.width - 4, 6.2 * text.length + 12), lx = Math.max(2, Math.min(st.width - w - 2, rx + 8));
      kids[2].setAttribute('x', lx.toFixed(1)); kids[2].setAttribute('width', w.toFixed(1));
      kids[3].setAttribute('x', (lx + 6).toFixed(1)); kids[3].textContent = text;
      b.readoutText = text;
    }
    function render() {
      if (!st.data) return;
      if (!deps.visible()) { st.dirty = true; return; }
      build();
    }
    function scheduleTick() {
      if (st.tickTimer !== null) timers.clear(st.tickTimer);
      st.tickTimer = timers.set(function tick() {
        st.tickTimer = null;
        if (!st.data) return;
        if (now() - st.fetchedAt >= REFRESH_MS) refresh();                // new readings (quiet: a failure keeps these)
        else if (deps.visible()) { writeCurrent(); } else st.dirty = true;  // the reading's age
        st.tickTimer = timers.set(tick, TICK_MS);
      }, TICK_MS);
    }
    function ask(path, seq, signal, extra) {
      var o = { signal: signal, headers: { Accept: 'application/json' } };
      if (extra) for (var k in extra) o[k] = extra[k];
      return deps.fetch(path, o).then(function (r) {
        if (seq !== st.seq) return { stale: true };
        var retryAfter = r.headers && r.headers.get ? r.headers.get('Retry-After') : null;
        return r.json().then(function (body) { return { status: r.status, body: body, retryAfter: retryAfter }; },
                             function () { return { status: r.status, body: null, retryAfter: retryAfter }; });
      });
    }
    function path() { return '/api/wind/' + encodeURIComponent(st.station.id) + '/history'; }
    function accept(b) {
      st.data = b; st.rows = historyRows(b); st.fetchedAt = now();
    }
    function refresh() {
      var seq = st.seq;
      if (!st.station) return;
      st.fetchedAt = now();                                               // the next try in REFRESH_MS, whatever happens
      ask(path(), seq, st.ac && st.ac.signal, { cache: 'no-cache' }).then(function (res) {
        if (res.stale || seq !== st.seq) return;
        if (res.status === 200 && res.body && Array.isArray(res.body.t)) { accept(res.body); render(); }
      }, function () { /* the chart stands */ });
    }
    function attempt(seq) {
      st.tries++;
      ask(path(), seq, st.ac.signal).then(function (res) {
        if (res.stale || seq !== st.seq) return;
        var b = res.body || {};
        if (res.status === 200 && Array.isArray(b.t)) {
          accept(b); st.tries = 0;
          setStatus('ready');
          render();
          scheduleTick();
          if (st.retried) { st.retried = false; if (deps.onRetried) try { deps.onRetried(); } catch (e) {} }
          return;
        }
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
      st.station = station; st.data = null; st.rows = []; st.tries = 0; st.dirty = false; st.built = null;
      st.retried = false; st.fetchedAt = 0;
      st.fallback = opts.reading && num(opts.reading.s) ? opts.reading : null;   // shown as the current reading when the history is empty
      if (opts.unit) st.unit = unitOf(opts.unit);
      st.zone = opts.zone || null;
      [els.chart, els.arrows, els.table, els.meta, els.current].forEach(function (e) { if (e) e.textContent = ''; });
      setStatus('loading');
      attempt(st.seq);
      return st.seq;
    }
    function retry() { if (st.station) { load(st.station, { unit: st.unit, zone: st.zone }); st.retried = true; } }
    function clear() {
      st.seq++;
      if (st.ac) { try { st.ac.abort(); } catch (e) {} st.ac = null; }
      stopTimers();
      st.station = null; st.data = null; st.rows = []; st.built = null; st.retried = false;
      [els.chart, els.arrows, els.table, els.meta, els.current].forEach(function (e) { if (e) e.textContent = ''; });
      setStatus('idle');
    }
    // the readout: the chart container's own listeners, which survive every rebuild
    function pointerX(e) {
      var svg = st.built && st.built.svg; if (!svg || !svg.getBoundingClientRect) return null;
      var p = e.touches && e.touches[0] ? e.touches[0] : e, r = svg.getBoundingClientRect();
      if (typeof p.clientX !== 'number' || p.clientY < r.top || p.clientY > r.bottom) return null;
      return (p.clientX - r.left) * (r.width ? st.width / r.width : 1);  // in the SVG's own units (it may be scaled)
    }
    if (els.chart) {
      var touchedAt = -Infinity, wall = deps.wallClock || function () { return Date.now(); };
      els.chart.addEventListener('mousemove', function (e) { if (wall() - touchedAt > TOUCH_MOUSE_MS) readoutAt(pointerX(e)); });
      els.chart.addEventListener('mouseleave', function () { if (wall() - touchedAt > TOUCH_MOUSE_MS) readoutAt(null); });
      els.chart.addEventListener('touchstart', function (e) { touchedAt = wall(); readoutAt(pointerX(e)); }, { passive: true });
      els.chart.addEventListener('touchmove', function (e) { touchedAt = wall(); readoutAt(pointerX(e)); }, { passive: true });
      els.chart.addEventListener('touchend', function () { touchedAt = wall(); readoutAt(null); });
      els.chart.addEventListener('touchcancel', function () { touchedAt = wall(); readoutAt(null); });
    }
    if (els.retry) els.retry.addEventListener('click', retry);
    return {
      load: load, clear: clear, retry: retry,
      setUnit: function (u) { st.unit = unitOf(u); if (st.data) render(); },
      setZone: function (z) { st.zone = z || null; if (st.data) render(); },
      show: function () { if (st.data && (st.dirty || !st.built)) render(); return Promise.resolve(); },
      resize: function () { if (st.data && st.built && Math.abs(width() - st.width) > RESIZE_PX) render(); },
      state: function () {
        return { seq: st.seq, status: st.status, station: st.station && st.station.id, hasData: !!st.data, rows: st.rows.length,
                 built: !!st.built, unit: st.unit, zone: zone(), tries: st.tries, retryTimer: st.retryTimer !== null,
                 tickTimer: st.tickTimer !== null, readout: st.built ? st.built.readoutText : '', width: st.width };
      }
    };
  }

  var api = {
    BANDS: BANDS, STALE_S: STALE_S,
    bandOf: bandOf, speedText: speedText, knotsText: knotsText, arrowDeg: arrowDeg, compass: compass, readingText: readingText,
    flagState: flagState, buildFlag: buildFlag, updateFlag: updateFlag, readings: readings,
    createWindFeed: createWindFeed, createWindView: createWindView,
    _internals: {
      KT_MS: KT_MS, FLAG: FLAG, NUM_OFFSET: NUM_OFFSET, FLAG_CLASSES: FLAG_CLASSES, numberAt: numberAt, ageText: ageText,
      speedValue: speedValue, unitName: unitName, dirText: dirText, validLatest: validLatest,
      FEED_INTERVAL_MS: FEED_INTERVAL_MS, FEED_DEADLINE_MS: FEED_DEADLINE_MS, FEED_PARTIAL_MS: FEED_PARTIAL_MS,
      FEED_PARTIAL_MAX: FEED_PARTIAL_MAX, FEED_RETRY_MS: FEED_RETRY_MS, FEED_STATUS: FEED_STATUS,
      CHART_H: CHART_H, ARROWS_H: ARROWS_H, MARGIN: MARGIN, ARROW_SLOT: ARROW_SLOT, GAP_S: GAP_S, NARROW: NARROW,
      TABLE_MAX: TABLE_MAX, READOUT_NEAR_S: READOUT_NEAR_S, MIN_WIDTH: MIN_WIDTH, REFRESH_MS: REFRESH_MS, TICK_MS: TICK_MS,
      RETRY_MAX: RETRY_MAX, RETRY_MAX_S: RETRY_MAX_S, TOUCH_MOUSE_MS: TOUCH_MOUSE_MS, MSG: MSG,
      zoneClock: zoneClock, hourMarks: hourMarks, niceStep: niceStep, historyRows: historyRows, linePath: linePath,
      arrowSlots: arrowSlots, nearest: nearest, sourceLink: sourceLink, clockText: clockText, hourText: hourText,
      dateText: dateText, stampText: stampText
    }
  };
  if (typeof window !== 'undefined') window.AllshoreWinds = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})();
