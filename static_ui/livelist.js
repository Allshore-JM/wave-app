/* Allshore Surf live-buoy list loader (plan section 36). Loaded on every page; touches no DOM and no global
 * state until create(...).start(...) is called, so tests/ui/livelist.test.js evaluates it in Node with fakes.
 *
 * What it does for the page:
 *  - draws the LAST list this browser saw at once (localStorage 'allshore.liveList.v1', positions and names
 *    only, at most 48 h old), marked as a cached list, while the fresh one loads;
 *  - takes the request the page started in <head> (once), then asks again itself: every attempt has a
 *    deadline (8 s, growing to 16 and 30 s after consecutive timeouts so a steadily slow server is not
 *    starved), a failed or late attempt is retried after 2, 4, 8, 16, then every 30 s, and only while the tab
 *    is visible (a hidden tab waits until it is shown again);
 *  - a PARTIAL answer (header X-Live-Stations-Partial: the sources the server is still loading after a
 *    restart) is drawn together with the cached markers of those sources, never stored, and asked again
 *    after 3, 3, 5, 5, 10, 10, 15 s ... (PARTIAL_MAX times), then every minute for up to two hours;
 *  - a complete answer replaces everything and is stored for the next visit.
 * The page draws what onStations hands it and shows the onStatus text under the "Live buoys" legend entry.
 * "loading…" / "cached list" appear only if the list has not landed within NOTE_DELAY_MS (no flash of the
 * legend on a normal load); "retrying…", "unavailable" and "loading more…" appear at once.
 */
(function () {
  'use strict';

  var KEY = 'allshore.liveList.v1';
  var VERSION = 1;
  var MAX_AGE_MS = 48 * 3600 * 1000;          // an older remembered list is not drawn
  var FUTURE_SKEW_MS = 5 * 60 * 1000;         // a list "saved in the future" (clock moved back) beyond this: ignored
  var DEADLINE_MS = 8000;
  var DEADLINE_STEPS = [8000, 8000, 16000, 30000];            // by consecutive timeouts; the last repeats
  var RETRY_MS = [2000, 4000, 8000, 16000, 30000];            // after a failure; the last repeats
  var PARTIAL_MS = [3000, 3000, 5000, 5000, 10000, 10000, 15000];   // after a partial answer; the last repeats
  var PARTIAL_MAX = 40;                       // ~9 min of partial answers on that schedule ...
  var SLOW_MS = 60000;                        // ... then one a minute
  var SLOW_MAX = 120;                         // ... for two hours; the page then keeps what it has
  var NOTE_DELAY_MS = 300;                    // "loading…" / "cached list" only when the list is this late
  var CAPS = ['bulk', 'recent_history', 'directional', 'spectra', 'partitions'];
  var META = ['source_name', 'source_url', 'license_label', 'attribution_text'];
  var STATUS_TEXT = {
    loading: 'loading…',
    cached: 'cached list',
    partial: 'loading more…',
    retrying: 'retrying…',
    unavailable: 'unavailable, retrying…',
    '': ''
  };

  function isStr(v) { return typeof v === 'string'; }
  function num(v) { return typeof v === 'number' && isFinite(v); }

  function capsMask(c) {
    var m = 0;
    if (c && typeof c === 'object') {
      for (var i = 0; i < CAPS.length; i++) if (c[CAPS[i]]) m |= (1 << i);
    }
    return m;
  }

  function capsOf(m) {
    var c = {};
    for (var i = 0; i < CAPS.length; i++) c[CAPS[i]] = !!(m & (1 << i));
    return c;
  }

  // The marker list in the compact form kept in storage: one row per station and the providers' fixed
  // attribution strings once per source. Rows without an id or a usable position are left out.
  function packRows(stations) {
    var rows = [], meta = {};
    for (var i = 0; i < stations.length; i++) {
      var s = stations[i];
      if (!s || !isStr(s.id) || !num(s.lat) || !num(s.lon)) continue;
      var src = isStr(s.source) ? s.source : '';
      if (src && !meta[src]) {
        var m = {};
        for (var k = 0; k < META.length; k++) if (isStr(s[META[k]])) m[META[k]] = s[META[k]];
        meta[src] = m;
      }
      var also = Array.isArray(s.also_sources) ? s.also_sources.filter(isStr) : [];
      rows.push([s.id, isStr(s.name) ? s.name : '', s.lat, s.lon, src, isStr(s.tz) ? s.tz : '',
                 capsMask(s.capabilities), s.is_stale ? 1 : 0, also]);
    }
    return { rows: rows, meta: meta };
  }

  function pack(stations, now) {
    var p = packRows(stations);
    return { v: VERSION, t: now, m: p.meta, s: p.rows };
  }

  // Stations back from storage (the shape the page's marker and panel code reads), or null when there is
  // nothing usable: wrong version, too old, from the future, or not the expected shape.
  function unpack(obj, now) {
    if (!obj || typeof obj !== 'object' || obj.v !== VERSION || !num(obj.t) || !Array.isArray(obj.s)) return null;
    if (now - obj.t > MAX_AGE_MS || obj.t - now > FUTURE_SKEW_MS) return null;
    var meta = obj.m && typeof obj.m === 'object' ? obj.m : {};
    var out = [];
    for (var i = 0; i < obj.s.length; i++) {
      var r = obj.s[i];
      if (!Array.isArray(r) || r.length < 9 || !isStr(r[0]) || !r[0] || !num(r[2]) || !num(r[3])) continue;
      if (Math.abs(r[2]) > 90 || Math.abs(r[3]) > 540) continue;
      var src = isStr(r[4]) ? r[4] : '';
      var st = { id: r[0], name: isStr(r[1]) ? r[1] : '', lat: r[2], lon: r[3], source: src,
                 capabilities: capsOf(num(r[6]) ? r[6] : 0), is_stale: r[7] === 1, dup_of: null };
      if (isStr(r[5]) && r[5]) st.tz = r[5];
      var m = meta[src];
      if (m && typeof m === 'object') {
        for (var k = 0; k < META.length; k++) if (isStr(m[META[k]])) st[META[k]] = m[META[k]];
      }
      if (Array.isArray(r[8]) && r[8].length) st.also_sources = r[8].filter(isStr);
      out.push(st);
    }
    return out.length ? out : null;
  }

  function readStored(storage, now) {
    try {
      if (!storage) return null;
      var raw = storage.getItem(KEY);
      if (!raw) return null;
      return unpack(JSON.parse(raw), now);
    } catch (e) {
      return null;
    }
  }

  function writeStored(storage, stations, now) {
    try {
      if (!storage) return false;
      storage.setItem(KEY, JSON.stringify(pack(stations, now)));
      return true;
    } catch (e) {                       // quota, a blocked storage, private mode: the page works without it
      return false;
    }
  }

  // A key that changes whenever anything the page draws or reads from a station changes.
  function contentKey(stations) {
    var p = packRows(stations);
    return JSON.stringify([p.rows, p.meta]);
  }

  // The server's partial answer plus the remembered markers of the sources it is still loading.
  function unionPartial(server, cached, missing) {
    if (!cached || !cached.length || !missing.length) return server.slice();
    var miss = {}, have = {};
    for (var i = 0; i < missing.length; i++) miss[missing[i]] = true;
    for (var j = 0; j < server.length; j++) if (server[j] && isStr(server[j].id)) have[server[j].id] = true;
    var out = server.slice();
    for (var k = 0; k < cached.length; k++) {
      var c = cached[k];
      if (miss[c.source] && !have[c.id]) out.push(c);
    }
    return out;
  }

  function parseMissing(h) {
    if (!h) return [];
    return String(h).split(',').map(function (x) { return x.trim(); }).filter(Boolean);
  }

  function create(opts) {
    opts = opts || {};
    var url = opts.url || '/api/buoys/live-stations';
    var doFetch = opts.fetch;
    var storage = opts.storage || null;
    var now = opts.now || function () { return Date.now(); };
    var timers = opts.timers || { set: function (f, ms) { return setTimeout(f, ms); }, clear: function (t) { clearTimeout(t); } };
    var visibility = opts.visibility || { isVisible: function () { return true; }, onChange: function () {} };
    var deadlineSteps = opts.deadlineSteps || DEADLINE_STEPS;
    var AbortCtl = opts.AbortController !== undefined ? opts.AbortController
      : (typeof AbortController !== 'undefined' ? AbortController : null);

    var early = opts.early || null;           // the <head> request: taken once
    var st = {
      started: false, stopped: false, done: false, gen: 0, timer: null, ctl: null,
      failures: 0, partials: 0, cached: null, shownKey: null, shown: false, status: null, waitingVisible: false,
      attempts: 0, timeouts: 0, noteTimer: null
    };
    var onStations = function () {}, onStatus = function () {};

    function applyStatus(code) {
      if (st.status === code) return;
      st.status = code;
      try { onStatus(code, STATUS_TEXT[code] || ''); } catch (e) {}
    }

    // "loading…" and "cached list" wait NOTE_DELAY_MS: on a normal load the list lands first and the legend never
    // grows a line only to lose it a moment later (G25 B-1). Every other status shows at once.
    function setStatus(code) {
      if (code === 'loading' || code === 'cached') {
        if (st.noteTimer !== null || st.status === code) return;
        st.noteTimer = timers.set(function () { st.noteTimer = null; applyStatus(code); }, NOTE_DELAY_MS);
        return;
      }
      if (st.noteTimer !== null) { timers.clear(st.noteTimer); st.noteTimer = null; }
      applyStatus(code);
    }

    function deliver(list, info) {
      var key = contentKey(list);
      info.same = key === st.shownKey;
      st.shownKey = key;
      st.shown = st.shown || list.length > 0;
      try { onStations(list, info); } catch (e) {}
    }

    function schedule(ms) {
      if (st.stopped || st.done) return;
      if (st.timer !== null) timers.clear(st.timer);
      st.timer = timers.set(function () {
        st.timer = null;
        if (!visibility.isVisible()) { st.waitingVisible = true; return; }   // asked again when shown
        attempt();
      }, ms);
    }

    function failed() {
      st.failures += 1;
      setStatus(st.shown ? 'retrying' : 'unavailable');
      schedule(RETRY_MS[Math.min(st.failures - 1, RETRY_MS.length - 1)]);
    }

    function withDeadline(promise, gen) {
      var ms = deadlineSteps[Math.min(st.timeouts, deadlineSteps.length - 1)];
      return new Promise(function (resolve, reject) {
        var t = timers.set(function () {
          if (st.ctl && gen === st.gen) { try { st.ctl.abort(); } catch (e) {} }
          reject(new Error('deadline'));
        }, ms);
        promise.then(function (v) { timers.clear(t); resolve(v); },
                     function (e) { timers.clear(t); reject(e); });
      });
    }

    function attempt() {
      if (st.stopped || st.done) return;
      var gen = ++st.gen;
      st.attempts += 1;
      var req;
      if (early) {
        req = early;
        early = null;
        st.ctl = null;                        // the head request cannot be aborted: only raced
      } else {
        st.ctl = AbortCtl ? new AbortCtl() : null;
        try {
          req = doFetch(url, st.ctl ? { signal: st.ctl.signal } : undefined);
        } catch (e) {
          req = Promise.reject(e);
        }
      }
      var body = Promise.resolve(req).then(function (r) {
        if (!r || !r.ok) throw new Error('HTTP ' + (r && r.status));
        var missing = parseMissing(r.headers && typeof r.headers.get === 'function' ? r.headers.get('X-Live-Stations-Partial') : null);
        return r.json().then(function (data) { return { data: data, missing: missing }; });
      });
      withDeadline(body, gen).then(function (res) {
        if (gen !== st.gen || st.stopped || st.done) return;
        st.timeouts = 0;                      // the server answered
        if (!Array.isArray(res.data)) { failed(); return; }
        st.failures = 0;
        if (res.missing.length) {
          st.partials += 1;
          deliver(unionPartial(res.data, st.cached, res.missing), { cached: false, partial: true, missing: res.missing });
          // "loading more…" only when something is on the map; else it is still loading (G25 B-2)
          setStatus(st.shown ? 'partial' : 'loading');
          if (st.partials >= PARTIAL_MAX + SLOW_MAX) {   // still partial after ~2 h: keep what is drawn (and the note)
            st.done = true;
            return;
          }
          schedule(st.partials < PARTIAL_MAX ? PARTIAL_MS[Math.min(st.partials - 1, PARTIAL_MS.length - 1)] : SLOW_MS);
          return;
        }
        st.done = true;
        if (res.data.length) writeStored(storage, res.data, now());
        deliver(res.data, { cached: false, partial: false, missing: [] });
        setStatus('');
      }, function (err) {
        if (gen !== st.gen || st.stopped || st.done) return;
        if (err && err.message === 'deadline') st.timeouts += 1;   // the next attempt gets longer (G25 B-5)
        else st.timeouts = 0;
        failed();
      });
    }

    function onVisibilityChange() {
      if (st.stopped || st.done || !st.waitingVisible || !visibility.isVisible()) return;
      st.waitingVisible = false;
      attempt();
    }

    return {
      start: function (handlers) {
        if (st.started) return;               // once per instance
        st.started = true;
        handlers = handlers || {};
        if (typeof handlers.onStations === 'function') onStations = handlers.onStations;
        if (typeof handlers.onStatus === 'function') onStatus = handlers.onStatus;
        visibility.onChange(onVisibilityChange);
        st.cached = readStored(storage, now());
        if (st.cached) {
          deliver(st.cached, { cached: true, partial: false, missing: [] });
          setStatus('cached');
        } else {
          setStatus('loading');
        }
        attempt();
      },
      stop: function () {
        st.stopped = true;
        if (st.timer !== null) { timers.clear(st.timer); st.timer = null; }
        if (st.noteTimer !== null) { timers.clear(st.noteTimer); st.noteTimer = null; }
        if (st.ctl) { try { st.ctl.abort(); } catch (e) {} }
      },
      state: function () {
        return { done: st.done, stopped: st.stopped, failures: st.failures, partials: st.partials, attempts: st.attempts,
                 status: st.status, cached: !!st.cached, waitingVisible: st.waitingVisible, timer: st.timer !== null,
                 timeouts: st.timeouts, notePending: st.noteTimer !== null };
      }
    };
  }

  var api = {
    create: create,
    _internals: {
      KEY: KEY, VERSION: VERSION, MAX_AGE_MS: MAX_AGE_MS, FUTURE_SKEW_MS: FUTURE_SKEW_MS, DEADLINE_MS: DEADLINE_MS,
      DEADLINE_STEPS: DEADLINE_STEPS, RETRY_MS: RETRY_MS, PARTIAL_MS: PARTIAL_MS, PARTIAL_MAX: PARTIAL_MAX,
      SLOW_MS: SLOW_MS, SLOW_MAX: SLOW_MAX, NOTE_DELAY_MS: NOTE_DELAY_MS, STATUS_TEXT: STATUS_TEXT,
      capsMask: capsMask, capsOf: capsOf, pack: pack, unpack: unpack, readStored: readStored, writeStored: writeStored,
      contentKey: contentKey, unionPartial: unionPartial, parseMissing: parseMissing
    }
  };
  if (typeof window !== 'undefined') window.AllshoreLiveList = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})();
