/* NWS coastal hazards on the coastline (plan section 90).
 *
 * Data: /api/alerts/coastal (alert_sources.py): the NWS coastal products in force or pending (High Surf, Rip Current,
 * Beach Hazards, Coastal / Lakeshore Flood, Storm Surge) and, for each zone they name, that zone's coastline as Google
 * encoded polylines (alert_coast.json, built from NWS zones and the GSHHG shoreline). Asked once the page has loaded and
 * the overlay's first picture has settled (at most 30 s), then every 2 minutes while the layer is on, the map is at
 * zoom 5 or closer and the tab is visible.
 * Drawing: each zone's coastline on the map's own canvas (the renderer the forecast dots use: one hit test, so a dot on
 * the shore keeps its tap; the lines are kept under the dots), a dark halo under the colour, in every world copy in view.
 * Colours by the strongest product in force on the zone: warning red; advisory, Rip Current and Beach Hazards
 * Statements orange; Coastal / Lakeshore Flood Statements blue; a watch, or anything issued but not yet in effect (its
 * onset ahead), yellow dashed (owner, 2026-11-08). The status is judged again every minute, so a pending product turns
 * to its colour at its onset and an ended one goes, without asking the server.
 * A click or tap on a line opens a popup with every product on that zone (text only: never the provider's words as
 * HTML). A map tool that is running takes the click instead.
 *
 * window.AllshoreAlerts = { create(map, opts), _internals }. Nothing touches the DOM at load (Node-testable).
 */
(function (root) {
  'use strict';

  var URL = '/api/alerts/coastal';
  var FORMAT = 'coastal-alerts-v1';
  var MIN_ZOOM = 5;                 // one region in view (owner: not at world zooms); no zoom hint is shown
  var REFRESH_MS = 120000, LOADING_RETRY_MS = 20000, FAIL_RETRY_MS = 60000, TICK_MS = 60000;
  var START_DELAY_MS = 1000, WAIT_MAX_MS = 30000;
  var VIEW_PAD = 0.25;              // world copies are judged on the view padded like the station markers' copies
  var COLORS = { warning: '#e03131', advisory: '#f76707', beach: '#f76707', flood_statement: '#1c7ed6', watch: '#fab005' };
  var OTHER_COLOR = '#868e96';
  var RANK = { warning: 4, advisory: 3, beach: 3, flood_statement: 2, watch: 1 };
  var HALO = 'rgba(0,0,0,0.55)', HALO_EXTRA = 3;
  var WEIGHT = 4, WEIGHT_WARNING = 5;
  var DASH = '8 6';
  var HIT_PX = 7;                   // a line takes a click this far beyond its own half width (plus a touch screen's reach)
  var CREDIT = 'Alerts: <a href="https://www.weather.gov/" target="_blank" rel="noopener">NWS</a>';
  var ZONE_RE = /^[A-Z]{2}Z\d{3}$/; // a forecast zone (the NWS page link below takes these)
  var MAX_LINE_CHARS = 2000000;     // an encoded line beyond this is not decoded (a damaged answer)

  // ---- pure helpers ----
  // Google's encoded polyline (precision 5) -> [[lat, lng], ...], or null for a damaged string.
  function decodePolyline(s) {
    if (typeof s !== 'string' || s.length > MAX_LINE_CHARS) return null;
    var out = [], i = 0, n = s.length, lat = 0, lng = 0;
    while (i < n) {
      var v = [0, 0];
      for (var k = 0; k < 2; k++) {
        var r = 0, sh = 0, b;
        do {
          if (i >= n || sh > 30) return null;
          b = s.charCodeAt(i++) - 63;
          if (b < 0 || b > 63) return null;
          r |= (b & 31) << sh;
          sh += 5;
        } while (b >= 32);
        v[k] = (r & 1) ? ~(r >> 1) : (r >> 1);
      }
      lat += v[0]; lng += v[1];
      var la = lat / 1e5, lo = lng / 1e5;
      if (!(la >= -90 && la <= 90 && lo >= -180 && lo <= 180)) return null;
      out.push([la, lo]);
    }
    return out.length >= 2 ? out : null;
  }

  function endOf(a) { return Number(a.ends || a.expires) || 0; }
  function onsetOf(a) { return Number(a.onset || a.sent) || 0; }
  function inForce(a, nowS) { return onsetOf(a) <= nowS && a.cls !== 'watch'; }
  function live(a, nowS) { return endOf(a) > nowS; }

  // The look of a zone's coastline at `nowS` (epoch seconds), from the products naming it: the strongest in force,
  // else yellow dashed while any is pending (or a watch), else null (nothing to draw).
  function zoneStyle(list, nowS) {
    var best = null, any = false;
    for (var i = 0; i < list.length; i++) {
      var a = list[i];
      if (!live(a, nowS) || !RANK[a.cls]) continue;
      any = true;
      if (inForce(a, nowS) && (!best || RANK[a.cls] > RANK[best.cls])) best = a;
    }
    if (best) return { cls: best.cls, color: COLORS[best.cls], dashed: false, weight: best.cls === 'warning' ? WEIGHT_WARNING : WEIGHT };
    if (any) return { cls: 'pending', color: COLORS.watch, dashed: true, weight: WEIGHT };
    return null;
  }

  // The products on a zone in the popup's order: in force by strength, then the pending ones by onset.
  function popupOrder(list, nowS) {
    return list.filter(function (a) { return live(a, nowS) && RANK[a.cls]; }).sort(function (x, y) {
      var fx = inForce(x, nowS) ? 1 : 0, fy = inForce(y, nowS) ? 1 : 0;
      if (fx !== fy) return fy - fx;
      if (fx && RANK[x.cls] !== RANK[y.cls]) return RANK[y.cls] - RANK[x.cls];
      return onsetOf(x) - onsetOf(y);
    });
  }

  // Multiples of 360 whose world copy meets [west, east] (degrees; the map's longitudes run on past +-180).
  function worldOffsets(west, east) {
    // copy k covers (-180 + 360k, 180 + 360k); a copy that only touches the view's edge is left out
    var out = [], k0 = Math.floor((west - 180) / 360) + 1, k1 = Math.ceil((east + 180) / 360) - 1;
    for (var k = k0; k <= k1 && out.length < 4; k++) out.push(k * 360 + 0);   // never -0
    return out;
  }

  var FMT = {};
  function formatter(tz) {
    if (Object.prototype.hasOwnProperty.call(FMT, tz)) return FMT[tz];
    var f = null;
    try {
      f = new Intl.DateTimeFormat('en-US', { timeZone: tz, weekday: 'short', hour: 'numeric', minute: '2-digit', timeZoneName: 'short' });
    } catch (e) { f = null; }
    FMT[tz] = f;
    return f;
  }
  // "Sat 6:00 PM HST" in the zone's own time (UTC when the browser does not know the zone's name).
  function fmtTime(sec, tz) {
    var f = formatter(tz || 'UTC') || formatter('UTC');
    var d = new Date(sec * 1000);
    if (!f) return d.toUTCString();
    return f.format(d).replace(',', '');
  }
  // The product's time line: "In effect until Sat 6:00 PM HST" or "From Mon 10:00 AM PST until Mon 12:00 PM PST".
  // A product with no end of its own (only the message's expiry, which NWS renews) says "until at least".
  function whenText(a, nowS, tz) {
    var until = a.ends ? 'until ' : 'until at least ';
    if (onsetOf(a) > nowS) return 'From ' + fmtTime(onsetOf(a), tz) + ' ' + until + fmtTime(endOf(a), tz);
    return (a.cls === 'watch' ? 'Watch in effect ' : 'In effect ') + until + fmtTime(endOf(a), tz);
  }

  function el(doc, tag, cls, text) {
    var e = doc.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = String(text);
    return e;
  }
  // The popup's content for one zone: every product on it, as text.
  function buildPopup(doc, zoneId, zone, list, nowS) {
    var tz = (zone && zone.tz) || 'UTC';
    var box = el(doc, 'div', 'alert-pop');
    box.appendChild(el(doc, 'div', 'alert-pop-zone', (zone && zone.name) || zoneId));
    popupOrder(list, nowS).forEach(function (a) {
      var force = inForce(a, nowS);
      var it = el(doc, 'div', 'alert-item');
      it.style.borderLeftColor = force ? (COLORS[a.cls] || OTHER_COLOR) : COLORS.watch;
      it.appendChild(el(doc, 'div', 'alert-ev', a.event + (onsetOf(a) > nowS ? ' (not yet in effect)' : '')));
      it.appendChild(el(doc, 'div', 'alert-when', whenText(a, nowS, tz)));
      if (a.headline) it.appendChild(el(doc, 'div', 'alert-head', a.headline));
      if (a.description) it.appendChild(el(doc, 'div', 'alert-desc', a.description));
      if (a.instruction) it.appendChild(el(doc, 'div', 'alert-instr', a.instruction));
      var src = (a.sent ? 'Issued ' + fmtTime(a.sent, tz) : 'Issued') + (a.office ? ' by ' + a.office : ' by the NWS');
      it.appendChild(el(doc, 'div', 'alert-src', src));
      box.appendChild(it);
    });
    if (ZONE_RE.test(zoneId)) {
      var link = el(doc, 'a', 'alert-link', 'NWS forecast for this zone');
      link.href = 'https://forecast.weather.gov/MapClick.php?zoneid=' + zoneId;
      link.target = '_blank';
      link.rel = 'noopener';
      box.appendChild(link);
    }
    return box;
  }

  // A checked answer -> {state, alerts, byZone: {id: [alert]}, zones: {id: {name, tz, key, lines}}}, or null.
  // Zones whose encoded lines did not change since the last answer keep their decoded points.
  function takeAnswer(j, prevZones) {
    if (!j || j.format !== FORMAT || typeof j.state !== 'string' || !Array.isArray(j.alerts) ||
        !j.zones || typeof j.zones !== 'object' || Array.isArray(j.zones)) return null;
    var zones = {}, byZone = {}, alerts = [];
    Object.keys(j.zones).forEach(function (id) {
      var z = j.zones[id];
      if (!z || !Array.isArray(z.lines)) return;
      var key = z.lines.join('|');
      var tz = typeof z.tz === 'string' ? z.tz : 'UTC', name = String(z.name || '');
      var prev = prevZones && Object.prototype.hasOwnProperty.call(prevZones, id) ? prevZones[id] : null;
      if (prev && prev.key === key) { zones[id] = { name: name, tz: tz, key: key, lines: prev.lines }; return; }
      var lines = [];
      z.lines.forEach(function (s) { var p = decodePolyline(s); if (p) lines.push(p); });
      if (lines.length) zones[id] = { name: name, tz: tz, key: key, lines: lines };
    });
    j.alerts.forEach(function (a) {
      if (!a || typeof a.event !== 'string' || !Object.prototype.hasOwnProperty.call(RANK, a.cls) || !Array.isArray(a.zones)) return;
      alerts.push(a);
      a.zones.forEach(function (z) {
        if (!Object.prototype.hasOwnProperty.call(zones, z)) return;
        (byZone[z] = byZone[z] || []).push(a);
      });
    });
    return { state: j.state, alerts: alerts, byZone: byZone, zones: zones };
  }

  // Calls start once the promise `waitFor()` returns has settled (the overlay's first picture), at once without one,
  // and WAIT_MAX_MS after the call at the latest (the places module's rule, G34 B-6).
  function afterWait(waitFor, start, setT) {
    var p = null;
    try { p = waitFor ? waitFor() : null; } catch (e) { p = null; }
    if (!p || typeof p.then !== 'function') { start(); return; }
    var done = false, fin = function () { if (!done) { done = true; start(); } };
    p.then(fin, fin); setT(fin, WAIT_MAX_MS);
  }

  // ---- the layer ----
  // opts: layer (an L.layerGroup in the legend), renderer (the map's canvas renderer), L, document, fetch, now (ms),
  // timers {set, clear}, visible (fn), coarse (fn: a touch screen), tip (fn(text) -> a tooltip element), onNote(text),
  // tool (fn(e) -> true when a map tool took the click), whenReady (fn(start)), waitFor (fn -> promise or null).
  function create(map, opts) {
    opts = opts || {};
    var Lf = opts.L || root.L, doc = opts.document || root.document, layer = opts.layer, renderer = opts.renderer;
    var fetchFn = opts.fetch || function (u, o) { return root.fetch(u, o); };
    var nowMs = opts.now || function () { return Date.now(); };
    var T = opts.timers || { set: function (f, ms) { return setTimeout(f, ms); }, clear: function (t) { clearTimeout(t); } };
    var visible = opts.visible || function () { return !doc.hidden; };
    var coarse = opts.coarse || function () { return false; };
    var onNote = opts.onNote || function () {};
    var tool = opts.tool || function () { return false; };
    var tip = opts.tip || null;
    var AlertLine = Lf.Polyline.extend({
      _clickTolerance: function () { return Lf.Polyline.prototype._clickTolerance.call(this) + HIT_PX; }
    });
    var data = null, started = false, fetching = false, lastFetch = -Infinity, dueAt = 0;
    var fetchTimer = null, tickTimer = null, note = '', drawnSig = '', creditOn = false, popup = null, popupZone = null;
    var drawn = [];                                   // {zone, line, halo}

    function nowS() { return Math.floor(nowMs() / 1000); }
    function layerOn() { return !!layer && map.hasLayer(layer); }
    function wanted() { return layerOn() && map.getZoom() >= MIN_ZOOM; }
    function setNote(t) { if (t !== note) { note = t; onNote(t); } }
    function credit(on) {
      if (on === creditOn || !map.attributionControl) return;
      creditOn = on;
      if (on) map.attributionControl.addAttribution(CREDIT); else map.attributionControl.removeAttribution(CREDIT);
    }
    function closePopup() {
      if (popup && map.closePopup) map.closePopup(popup);
      popup = null; popupZone = null;
    }
    function clearLines() {
      if (drawn.length) layer.clearLayers();
      drawn = [];
      drawnSig = '';
      credit(false);
      closePopup();
    }

    function openPopup(zone, latlng) {
      if (!data || !data.byZone[zone]) return;
      var size = map.getSize ? map.getSize() : { x: 400, y: 400 };
      var content = buildPopup(doc, zone, data.zones[zone], data.byZone[zone], nowS());
      popup = Lf.popup({ className: 'alert-popup', maxWidth: Math.max(200, Math.min(340, size.x - 40)),
                         maxHeight: Math.max(160, Math.min(340, size.y - 90)), autoPanPadding: [16, 16] })
        .setLatLng(latlng).setContent(content).openOn(map);
      popupZone = zone;
    }

    function draw(force) {
      if (!wanted() || !data) { if (drawn.length || creditOn) clearLines(); return; }
      var t = nowS(), styles = {}, keys = [];
      Object.keys(data.byZone).forEach(function (z) {
        var s = zoneStyle(data.byZone[z], t);
        if (s) { styles[z] = s; keys.push(z + ':' + s.color + ':' + (s.dashed ? 1 : 0) + ':' + s.weight); }
      });
      var b = map.getBounds().pad(VIEW_PAD), offs = worldOffsets(b.getWest(), b.getEast());
      var sig = keys.sort().join(',') + '|' + offs.join(',');
      if (!force && sig === drawnSig) return;
      if (popupZone && !styles[popupZone]) closePopup();
      layer.clearLayers();
      drawn = [];
      drawnSig = sig;
      var lines = [], halos = [];
      Object.keys(styles).sort().forEach(function (z) {
        var s = styles[z], zone = data.zones[z], name = zone.name || z;
        var label = data.byZone[z].filter(function (a) { return live(a, t); }).map(function (a) { return a.event; })
          .filter(function (v, i, arr) { return arr.indexOf(v) === i; }).join(', ') + ' · ' + name;
        offs.forEach(function (o) {
          zone.lines.forEach(function (pts) {
            var ll = o ? pts.map(function (p) { return [p[0], p[1] + o]; }) : pts;
            var halo = Lf.polyline(ll, { renderer: renderer, color: HALO, weight: s.weight + HALO_EXTRA, opacity: 1,
                                         lineCap: 'round', lineJoin: 'round', interactive: false });
            var line = new AlertLine(ll, { renderer: renderer, color: s.color, weight: s.weight, opacity: 1,
                                           dashArray: s.dashed ? DASH : null, lineCap: s.dashed ? 'butt' : 'round',
                                           lineJoin: 'round', interactive: true, bubblingMouseEvents: false });
            line.on('click', function (e) {
              if (tool(e)) return;                     // a running map tool takes the click (the markers' rule)
              openPopup(z, e.latlng);
            });
            if (tip && !coarse()) line.bindTooltip(tip(label), { sticky: true, direction: 'top', offset: [0, -6] });
            halo.addTo(layer); line.addTo(layer);
            halos.push(halo); lines.push(line);
            drawn.push({ zone: z, line: line, halo: halo });
          });
        });
      });
      // under the forecast dots on the shared canvas: the colours, then the halos, to the back (halos lowest)
      for (var i = lines.length - 1; i >= 0; i--) if (lines[i].bringToBack) lines[i].bringToBack();
      for (var j = halos.length - 1; j >= 0; j--) if (halos[j].bringToBack) halos[j].bringToBack();
      credit(drawn.length > 0);
    }

    function schedule(ms) {
      if (fetchTimer) T.clear(fetchTimer);
      dueAt = nowMs() + ms;
      fetchTimer = T.set(function () { fetchTimer = null; sync(); }, ms);
    }
    function noteIfOn(t) { return layerOn() ? t : ''; }
    function failed() {
      // the held products stay until their own end (judged every minute); the note says the list could not be read
      setNote(noteIfOn(data && data.state === 'ok' ? '' : 'unavailable'));
      schedule(FAIL_RETRY_MS);
      draw(false);
    }
    function load() {
      if (fetching) return;
      fetching = true;
      lastFetch = nowMs();
      var p;
      try { p = fetchFn(URL, { cache: 'no-cache', credentials: 'same-origin' }); } catch (e) { p = null; }
      if (!p || typeof p.then !== 'function') { fetching = false; failed(); return; }
      p.then(function (r) {
        if (!r || !r.ok) throw new Error('HTTP ' + (r && r.status));
        return r.json();
      }).then(function (j) {
        fetching = false;
        var d = takeAnswer(j, data && data.zones);
        if (!d) { failed(); return; }
        data = d;
        if (d.state === 'loading') { setNote(noteIfOn('loading…')); schedule(LOADING_RETRY_MS); }
        else if (d.state === 'ok') { setNote(''); schedule(REFRESH_MS); }
        else { setNote(noteIfOn('unavailable')); schedule(FAIL_RETRY_MS); }
        draw(true);
      }, function () { fetching = false; failed(); });
    }
    function tick() {
      tickTimer = null;
      if (!wanted()) return;
      draw(false);
      tickTimer = T.set(tick, TICK_MS);
    }

    function sync() {
      if (!started) return;
      if (!layerOn()) {
        clearLines(); setNote('');
        if (fetchTimer) { T.clear(fetchTimer); fetchTimer = null; }
        if (tickTimer) { T.clear(tickTimer); tickTimer = null; }
        return;
      }
      if (!wanted()) {
        clearLines();
        if (tickTimer) { T.clear(tickTimer); tickTimer = null; }
        return;
      }
      if (!tickTimer) tickTimer = T.set(tick, TICK_MS);
      draw(false);
      if (!visible() || fetching) return;              // a hidden tab asks nothing; the visibility change asks again
      if (!data || nowMs() >= dueAt) {
        if (!data && !note) setNote('loading…');
        load();
      }
    }

    map.on('moveend zoomend', sync);
    map.on('overlayadd overlayremove', function (e) { if (e && e.layer === layer) sync(); });
    if (doc && doc.addEventListener) doc.addEventListener('visibilitychange', sync);
    var whenReady = opts.whenReady || function (start) {
      var go = function () { T.set(function () { afterWait(opts.waitFor, start, T.set); }, START_DELAY_MS); };
      if (doc.readyState === 'complete') go(); else if (root.addEventListener) root.addEventListener('load', go, { once: true });
    };
    whenReady(function () { started = true; sync(); });

    return {
      sync: sync, draw: draw, openPopup: openPopup,
      state: function () {
        return { started: started, fetching: fetching, note: note, drawn: drawn.length, credit: creditOn, data: data,
                 lastFetch: lastFetch, dueAt: dueAt, popupZone: popupZone, fetchTimer: !!fetchTimer, tickTimer: !!tickTimer };
      }
    };
  }

  root.AllshoreAlerts = {
    create: create,
    _internals: { URL: URL, FORMAT: FORMAT, MIN_ZOOM: MIN_ZOOM, REFRESH_MS: REFRESH_MS, LOADING_RETRY_MS: LOADING_RETRY_MS,
                  FAIL_RETRY_MS: FAIL_RETRY_MS, TICK_MS: TICK_MS, WAIT_MAX_MS: WAIT_MAX_MS, COLORS: COLORS, RANK: RANK,
                  HIT_PX: HIT_PX, DASH: DASH, CREDIT: CREDIT, WEIGHT: WEIGHT, WEIGHT_WARNING: WEIGHT_WARNING,
                  decodePolyline: decodePolyline, zoneStyle: zoneStyle, popupOrder: popupOrder, worldOffsets: worldOffsets,
                  fmtTime: fmtTime, whenText: whenText, buildPopup: buildPopup, takeAnswer: takeAnswer, afterWait: afterWait }
  };
})(typeof window !== 'undefined' ? window : this);
