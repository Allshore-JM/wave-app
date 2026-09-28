/* Map tools (plan section 29): measure distance, measure area, and swell exposure.
 *
 * Distance and area use great-circle geometry on a sphere (R = 6371.0088 km), shown in the site's units plus
 * nautical miles. Swell exposure: from a clicked water point, 720 great-circle rays (every 0.5 degrees, ten per
 * 5-degree wedge) walk outward until they cross a coastline or reach 3,000 km. The coastline is the GSHHG data the
 * overlay already publishes (coast-v1: tier 1 at full resolution for the first 50 km, tier 0 beyond). Each ray's
 * open-water distance F is compared with the spot's own reference F_ref (the 90th percentile of its rays, clamped
 * to 200..3,000 km, so enclosed seas adapt: owner decision), openness o = min(1, F / F_ref), and a wedge's shadow
 * is 1 - mean(o) over its ten rays: open below 0.2, light grey below 0.7, dark grey above. A wedge at bearing b is
 * swell arriving FROM b. Geometric exposure only: real swell bends around islands.
 *
 * window.AllshoreTools = { init(opts), active(), click(latlng), _internals }. Nothing touches the DOM at load.
 */
(function (root) {
  'use strict';

  var R_KM = 6371.0088;
  var D2R = Math.PI / 180, R2D = 180 / Math.PI;
  var KM_PER_MI = 1.609344, KM_PER_NM = 1.852, FT_PER_KM = 3280.839895;

  // ---- geodesy (sphere) ----
  function toRad(p) { return { f: p.lat * D2R, l: p.lng * D2R }; }
  function wrapLng(x) { return ((x + 180) % 360 + 360) % 360 - 180; }
  function centralAngle(a, b) {                                             // haversine, radians
    var A = toRad(a), B = toRad(b), df = B.f - A.f, dl = B.l - A.l;
    var h = Math.sin(df / 2) * Math.sin(df / 2) + Math.cos(A.f) * Math.cos(B.f) * Math.sin(dl / 2) * Math.sin(dl / 2);
    return 2 * Math.atan2(Math.sqrt(h), Math.sqrt(Math.max(0, 1 - h)));
  }
  function distanceKm(a, b) { return centralAngle(a, b) * R_KM; }
  function bearingDeg(a, b) {
    var A = toRad(a), B = toRad(b), dl = B.l - A.l;
    var y = Math.sin(dl) * Math.cos(B.f), x = Math.cos(A.f) * Math.sin(B.f) - Math.sin(A.f) * Math.cos(B.f) * Math.cos(dl);
    return (Math.atan2(y, x) * R2D + 360) % 360;
  }
  function destination(p, brg, km) {
    var A = toRad(p), d = km / R_KM, t = brg * D2R;
    var sf = Math.sin(A.f) * Math.cos(d) + Math.cos(A.f) * Math.sin(d) * Math.cos(t);
    var f = Math.asin(Math.max(-1, Math.min(1, sf)));
    var l = A.l + Math.atan2(Math.sin(t) * Math.sin(d) * Math.cos(A.f), Math.cos(d) - Math.sin(A.f) * sf);
    return { lat: f * R2D, lng: wrapLng(l * R2D) };
  }
  // Points along the great circle from a to b (both included), no gap wider than maxKm. Longitudes are kept
  // continuous from a (the line is drawn the short way, in a's world copy).
  function densify(a, b, maxKm) {
    var d = centralAngle(a, b), n = Math.max(1, Math.ceil(d * R_KM / (maxKm || 50))), out = [];
    var A = toRad(a), B = toRad(b), prev = a.lng;
    for (var i = 0; i <= n; i++) {
      var p;
      if (d < 1e-12) p = { lat: a.lat, lng: a.lng };
      else {
        var f = i / n, s = Math.sin(d), ka = Math.sin((1 - f) * d) / s, kb = Math.sin(f * d) / s;
        var x = ka * Math.cos(A.f) * Math.cos(A.l) + kb * Math.cos(B.f) * Math.cos(B.l);
        var y = ka * Math.cos(A.f) * Math.sin(A.l) + kb * Math.cos(B.f) * Math.sin(B.l);
        var z = ka * Math.sin(A.f) + kb * Math.sin(B.f);
        p = { lat: Math.atan2(z, Math.sqrt(x * x + y * y)) * R2D, lng: Math.atan2(y, x) * R2D };
      }
      var lng = p.lng; while (lng - prev > 180) lng -= 360; while (lng - prev < -180) lng += 360;   // continuous
      out.push({ lat: p.lat, lng: lng }); prev = lng;
    }
    return out;
  }
  function pathKm(pts) { var s = 0; for (var i = 1; i < pts.length; i++) s += distanceKm(pts[i - 1], pts[i]); return s; }
  // Area of a simple polygon on the sphere (km2), edges taken as great circles: the ring is densified, then the
  // spherical-excess sum over the (continuous-longitude) ring, as in Chamberlain & Duquette (2007).
  function sphericalAreaKm2(pts) {
    if (!pts || pts.length < 3) return 0;
    var ring = [];
    for (var i = 0; i < pts.length; i++) {
      var seg = densify(pts[i], pts[(i + 1) % pts.length], 5);
      var base = ring.length ? ring[ring.length - 1].lng : seg[0].lng, sh = 0;
      while (seg[0].lng + sh - base > 180) sh -= 360; while (seg[0].lng + sh - base < -180) sh += 360;
      for (var k = ring.length ? 1 : 0; k < seg.length; k++) ring.push({ lat: seg[k].lat, lng: seg[k].lng + sh });
    }
    var s = 0, n = ring.length;
    for (var j = 0; j < n - 1; j++) {
      var p1 = ring[j], p2 = ring[j + 1];
      s += (p2.lng - p1.lng) * D2R * (2 + Math.sin(p1.lat * D2R) + Math.sin(p2.lat * D2R));
    }
    return Math.abs(s * R_KM * R_KM / 2);
  }

  // ---- formatting (site units: 'US' or 'Metric') ----
  function fmtNum(v, dp) { return v.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp }); }
  function sig(v) { return v < 10 ? fmtNum(v, 2) : v < 100 ? fmtNum(v, 1) : fmtNum(v, 0); }
  function fmtLength(km, unit) {
    var nm = ' · ' + sig(km / KM_PER_NM) + ' nm';
    if (unit === 'Metric') return (km < 1 ? fmtNum(km * 1000, 0) + ' m' : sig(km) + ' km') + nm;
    var mi = km / KM_PER_MI;
    return (mi < 0.5 ? fmtNum(km * FT_PER_KM, 0) + ' ft' : sig(mi) + ' mi') + nm;
  }
  function fmtArea(km2, unit) {
    if (unit === 'Metric') return km2 < 1 ? sig(km2 * 100) + ' ha' : sig(km2) + ' km²';
    var mi2 = km2 / (KM_PER_MI * KM_PER_MI);
    return mi2 < 1 ? sig(mi2 * 640) + ' acres' : sig(mi2) + ' sq mi';
  }
  // A distance in the site unit: one decimal below 10, whole numbers above, "under 0.1" for the spot's own shore.
  function fmtDist(km, unit) {
    var v = unit === 'Metric' ? km : km / KM_PER_MI, u = unit === 'Metric' ? ' km' : ' mi';
    if (v < 0.1) return 'under 0.1' + u;
    return (v < 10 ? fmtNum(v, 1) : fmtNum(Math.round(v), 0)) + u;
  }
  var COMPASS = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];
  function compass(b) { return COMPASS[Math.round(((b % 360) + 360) % 360 / 22.5) % 16]; }
  function pad3(d) { d = ((d % 360) + 360) % 360; return (d < 10 ? '00' : d < 100 ? '0' : '') + d; }

  // ---- coast-v1 decoding into lon/lat degrees (same format as static_overlay/overlay.js decodeCoast) ----
  var MAX_COAST_BYTES = 8 * 1024 * 1024;
  function decodeCoastLL(buf) {
    if (!(buf instanceof ArrayBuffer) || buf.byteLength < 40 || buf.byteLength > MAX_COAST_BYTES) throw new Error('coast decode failed');
    var u8 = new Uint8Array(buf), dv = new DataView(buf);
    if (u8[0] !== 67 || u8[1] !== 83 || u8[2] !== 84 || u8[3] !== 49) throw new Error('coast decode failed');
    var cell = dv.getUint16(4, true), q = dv.getUint32(8, true), nP = dv.getUint32(12, true), nR = dv.getUint32(16, true), nV = dv.getUint32(20, true);
    if (!q || nP > 200000 || nR > 400000 || nV > 4000000 || nR < nP || nV < 3 * nR) throw new Error('coast decode failed');
    var pos = 40, end = u8.length;
    function varint() {
      var v = 0, shift = 1, b;
      do {
        if (pos >= end) throw new Error('coast decode failed');
        b = u8[pos++]; v += (b & 127) * shift; shift *= 128;
        if (shift > 34359738368) throw new Error('coast decode failed');
      } while (b & 128);
      return v;
    }
    function zz() { var v = varint(); return v % 2 ? -(v + 1) / 2 : v / 2; }
    var box = new Float64Array(nP * 4), ringStart = new Int32Array(nP + 1), vertStart = new Int32Array(nR + 1), ll = new Float64Array(nV * 2);
    var r = 0, v = 0, inv = 1 / q;
    for (var p = 0; p < nP; p++) {
      var minx = zz(), miny = zz(), w = varint(), h = varint(), nr = varint();
      if (r + nr > nR) throw new Error('coast decode failed');
      ringStart[p] = r;
      box[p * 4] = minx * inv; box[p * 4 + 1] = miny * inv; box[p * 4 + 2] = (minx + w) * inv; box[p * 4 + 3] = (miny + h) * inv;
      for (var k = 0; k < nr; k++) {
        var n = varint();
        if (n < 3 || v + n > nV) throw new Error('coast decode failed');
        vertStart[r++] = v;
        var x = minx, y = miny;
        for (var i = 0; i < n; i++) {
          x += zz(); y += zz();
          if (x < -180 * q - 1 || x > 180 * q + 1 || y < -90 * q - 1 || y > 90 * q + 1) throw new Error('coast decode failed');
          ll[v * 2] = x * inv; ll[v * 2 + 1] = y * inv; v++;
        }
      }
    }
    ringStart[nP] = r; vertStart[nR] = v;
    if (r !== nR || v !== nV || pos !== end) throw new Error('coast decode failed');
    return { cell: cell, n: nP, box: box, ringStart: ringStart, vertStart: vertStart, ll: ll };
  }

  // ---- the edge index around a point ----
  // Coordinates are local: x = longitude relative to the origin, wrapped into [-180, 180), y = latitude (degrees).
  // Rays and coast edges are short (<= 10 km, ~1 km) so both are straight lines in this plane at the latitudes a
  // surf spot has (the tool refuses |lat| > 75).
  function EdgeIndex(bucketDeg) { this.b = bucketDeg; this.cells = new Map(); this.xy = []; this.n = 0; }
  EdgeIndex.prototype._key = function (i, j) { return i * 100003 + j; };
  EdgeIndex.prototype.add = function (x0, y0, x1, y1) {
    var id = this.n++, b = this.b;
    this.xy.push(x0, y0, x1, y1);
    var i0 = Math.floor(Math.min(x0, x1) / b), i1 = Math.floor(Math.max(x0, x1) / b);
    var j0 = Math.floor(Math.min(y0, y1) / b), j1 = Math.floor(Math.max(y0, y1) / b);
    for (var i = i0; i <= i1; i++) for (var j = j0; j <= j1; j++) {
      var k = this._key(i, j), a = this.cells.get(k);
      if (a) a.push(id); else this.cells.set(k, [id]);
    }
  };
  EdgeIndex.prototype.finish = function () { this.xy = new Float64Array(this.xy); return this; };
  // Parameter t in [0, 1] along a->b of the first crossing with any edge, or -1.
  EdgeIndex.prototype.firstHit = function (ax, ay, bx, by) {
    var b = this.b, best = 2, xy = this.xy, seen = null;
    var i0 = Math.floor(Math.min(ax, bx) / b), i1 = Math.floor(Math.max(ax, bx) / b);
    var j0 = Math.floor(Math.min(ay, by) / b), j1 = Math.floor(Math.max(ay, by) / b);
    var multi = (i1 > i0 || j1 > j0);
    var dx = bx - ax, dy = by - ay;
    for (var i = i0; i <= i1; i++) for (var j = j0; j <= j1; j++) {
      var list = this.cells.get(this._key(i, j));
      if (!list) continue;
      for (var n = 0; n < list.length; n++) {
        var e = list[n];
        if (multi) { if (!seen) seen = new Set(); if (seen.has(e)) continue; seen.add(e); }
        var cx = xy[e * 4], cy = xy[e * 4 + 1], ex = xy[e * 4 + 2] - cx, ey = xy[e * 4 + 3] - cy;
        var den = dx * ey - dy * ex;
        if (den === 0) continue;                                              // parallel (a touching collinear edge is not a crossing)
        var t = ((cx - ax) * ey - (cy - ay) * ex) / den, u = ((cx - ax) * dy - (cy - ay) * dx) / den;
        if (t >= 0 && t <= 1 && u >= 0 && u <= 1 && t < best) best = t;
      }
    }
    return best <= 1 ? best : -1;
  };
  // The builder clips every polygon to its tier's cell grid (30 degrees for tier 0, 5 for tier 1); clipping leaves
  // edges ALONG the cell lines (the cell border itself, and zero-width bridges between the parts of a concave
  // coast). They are harmless for a filled mask but would be false coastlines for a ray, so an edge lying exactly on
  // a cell line is skipped (a real, 1e-4-degree-quantised coastline never runs exactly along one).
  function onLine(v, cell) { var r = v / cell; return Math.abs(r - Math.round(r)) * cell < 1e-7; }
  function onCellLine(x0, y0, x1, y1, cell) {
    if (!(cell > 0)) return false;
    return (Math.abs(y0 - y1) < 1e-9 && onLine(y0, cell)) || (Math.abs(x0 - x1) < 1e-9 && onLine(x0, cell));
  }
  // Every edge of the pieces whose box meets the local window [-wx, wx] x [lat - wy, lat + wy].
  function addPieces(idx, set, origin, wx, wy) {
    if (!set) return 0;
    var added = 0, lat0 = origin.lat - wy, lat1 = origin.lat + wy;
    for (var p = 0; p < set.n; p++) {
      var bx0 = set.box[p * 4], by0 = set.box[p * 4 + 1], bx1 = set.box[p * 4 + 2], by1 = set.box[p * 4 + 3];
      if (by1 < lat0 || by0 > lat1) continue;
      if (wx < 180 && bx1 - bx0 < 360) {
        var c0 = wrapLng(bx0 - origin.lng), c1 = c0 + (bx1 - bx0);            // the box relative to the origin (may run past +180)
        if (!((c1 >= -wx && c0 <= wx) || c1 - 360 >= -wx)) continue;
      }
      for (var r = set.ringStart[p]; r < set.ringStart[p + 1]; r++) {
        var s = set.vertStart[r], e = set.vertStart[r + 1];
        for (var k = s; k < e; k++) {
          var k2 = k + 1 < e ? k + 1 : s;
          var x0 = wrapLng(set.ll[k * 2] - origin.lng), y0 = set.ll[k * 2 + 1];
          var x1 = wrapLng(set.ll[k2 * 2] - origin.lng), y1 = set.ll[k2 * 2 + 1];
          if (Math.abs(x1 - x0) > 180) continue;                             // crosses the origin's antimeridian: far away
          if (onCellLine(set.ll[k * 2], set.ll[k * 2 + 1], set.ll[k2 * 2], set.ll[k2 * 2 + 1], set.cell)) continue;
          if (Math.max(y0, y1) < lat0 || Math.min(y0, y1) > lat1) continue;
          if (Math.max(x0, x1) < -wx || Math.min(x0, x1) > wx) continue;
          idx.add(x0, y0, x1, y1); added++;
        }
      }
    }
    return added;
  }
  // Even-odd point-in-land over the pieces of the given sets (a point on a cell-clip seam may count twice; the
  // snap below tolerates it by testing a few candidates).
  function inLand(sets, lng, lat) {
    var inside = false;
    for (var si = 0; si < sets.length; si++) {
      var set = sets[si]; if (!set) continue;
      for (var p = 0; p < set.n; p++) {
        var bx0 = set.box[p * 4], by0 = set.box[p * 4 + 1], bx1 = set.box[p * 4 + 2], by1 = set.box[p * 4 + 3];
        if (lat < by0 || lat > by1) continue;
        var x = lng; if (x < bx0) x += 360; else if (x > bx1) x -= 360;
        if (x < bx0 || x > bx1) continue;
        var pin = false;
        for (var r = set.ringStart[p]; r < set.ringStart[p + 1]; r++) {
          var s = set.vertStart[r], e = set.vertStart[r + 1];
          for (var k = s, j = e - 1; k < e; j = k++) {
            var xi = set.ll[k * 2], yi = set.ll[k * 2 + 1], xj = set.ll[j * 2], yj = set.ll[j * 2 + 1];
            if ((yi > lat) !== (yj > lat) && x < (xj - xi) * (lat - yi) / (yj - yi) + xi) pin = !pin;
          }
        }
        if (pin) inside = !inside;
      }
    }
    return inside;
  }

  // ---- exposure ----
  var RAYS = 720, SECTORS = 72, PER_SECTOR = RAYS / SECTORS;
  var CAP_KM = 3000, NEAR_KM = 50, REF_MIN_KM = 200, REF_PCT = 0.9;
  var OPEN_BELOW = 0.2, DARK_FROM = 0.7;
  var MAX_ABS_LAT = 75;
  function stepKm(d) { return d < 20 ? 0.25 : d < 200 ? 2 : 10; }
  function rayBearing(i) { return (i + 0.5) * (360 / RAYS); }             // ray i lies in wedge floor(i / 10): [5k, 5k + 5)
  // Open-water distance along one ray (km, capped). near/far: EdgeIndex (either may be null = no land there).
  function rayFetch(origin, brg, near, far) {
    var d = 0, ax = 0, ay = origin.lat;
    while (d < CAP_KM) {
      var st = Math.min(stepKm(d), CAP_KM - d), q = destination(origin, brg, d + st);
      var bx = wrapLng(q.lng - origin.lng), by = q.lat;
      var idx = d < NEAR_KM ? near : far;
      if (idx && Math.abs(bx - ax) <= 180) {                                  // a step across the origin's antimeridian (near a pole) is not tested
        var t = idx.firstHit(ax, ay, bx, by);
        if (t >= 0) return d + st * t;
      }
      d += st; ax = bx; ay = by;
    }
    return CAP_KM;
  }
  function percentile(arr, p) {
    var s = Array.prototype.slice.call(arr).sort(function (a, b) { return a - b; });
    var i = Math.min(s.length - 1, Math.max(0, Math.ceil(p * s.length) - 1));
    return s[i];
  }
  function levelOf(s) { return s < OPEN_BELOW ? 'open' : s < DARK_FROM ? 'light' : 'dark'; }
  // How much one ray's land blocks swell: fully within SHADOW_FULL_KM (the spot's own coast), falling off with
  // the logarithm of the distance to nothing at the spot's reference fetch (owner: nearby land darker; a distant
  // island such as Kauai seen from the North Shore, ~150 km, counts about half: light grey).
  var SHADOW_FULL_KM = 15;
  function rayShadow(f, fRef) {
    if (f >= fRef) return 0;
    if (f <= SHADOW_FULL_KM) return 1;
    return Math.max(0, Math.min(1, 1 - Math.log(f / SHADOW_FULL_KM) / Math.log(fRef / SHADOW_FULL_KM)));
  }
  // From the per-ray fetches to the wedges.
  function summarise(fetch) {
    var fRef = Math.max(REF_MIN_KM, Math.min(CAP_KM, percentile(fetch, REF_PCT)));
    var sectors = [];
    for (var k = 0; k < SECTORS; k++) {
      var sum = 0, minLand = Infinity, blocked = 0;
      for (var j = 0; j < PER_SECTOR; j++) {
        var f = fetch[k * PER_SECTOR + j];
        sum += rayShadow(f, fRef);
        if (f < CAP_KM) { if (f < minLand) minLand = f; if (f < fRef) blocked++; }
      }
      var s = sum / PER_SECTOR;
      sectors.push({ from: k * 5, to: k * 5 + 5, s: s, level: levelOf(s), minLandKm: isFinite(minLand) ? minLand : null, blockedRays: blocked });
    }
    return { sectors: sectors, fRef: fRef, openWindows: openWindows(sectors) };
  }
  // Runs of open wedges, as [fromDeg, toDeg] going clockwise; a run through north is one window (e.g. 280-015).
  function openWindows(sectors) {
    var n = sectors.length, open = sectors.map(function (s) { return s.level === 'open'; });
    if (open.every(Boolean)) return [[0, 360]];
    var start = open.indexOf(false), out = [], run = null;
    for (var i = 1; i <= n; i++) {
      var k = (start + i) % n;
      if (open[k]) { if (!run) run = [sectors[k].from, sectors[k].to]; else run[1] = sectors[k].to; }
      else if (run) { out.push(run); run = null; }
    }
    if (run) out.push(run);
    return out;
  }
  function windowsText(ws) {
    if (!ws.length) return 'No open swell window';
    if (ws.length === 1 && ws[0][0] === 0 && ws[0][1] === 360) return 'Open to swell from every direction';
    return 'Open: ' + ws.map(function (w) { return pad3(w[0]) + '°–' + pad3(w[1] % 360) + '°'; }).join(', ');
  }
  function sectorText(sec, unit) {
    var head = compass(sec.from + 2.5) + ' ' + pad3(sec.from) + '–' + pad3(sec.to % 360) + '°: ';
    var pct = Math.round(sec.s * 100);
    if (sec.level === 'open') return head + (sec.minLandKm == null ? 'open, no land within ' + fmtDist(CAP_KM, unit) : 'open (nearest land ' + fmtDist(sec.minLandKm, unit) + ')');
    return head + (sec.level === 'light' ? 'partly shadowed' : 'shadowed') + ' (' + pct + '%), land at ' + fmtDist(sec.minLandKm, unit);
  }
  // Build both indexes for an origin from decoded sets: near = tier-1 sets (or tier 0 standing in), far = tier 0.
  function buildIndexes(origin, nearSets, farSet) {
    var coslat = Math.max(0.2, Math.cos(origin.lat * D2R));
    var nearWy = 0.5, nearWx = Math.min(180, 0.5 / coslat);
    var farWy = CAP_KM / 111.2 + 0.5, farWx = Math.min(180, farWy / coslat);
    var near = new EdgeIndex(0.02), far = new EdgeIndex(0.5), nn = 0;
    for (var i = 0; i < nearSets.length; i++) nn += addPieces(near, nearSets[i], origin, nearWx, nearWy);
    var nf = addPieces(far, farSet, origin, farWx, farWy);
    return { near: nn ? near.finish() : null, far: nf ? far.finish() : null };
  }
  // Nearest water to a point on land, within maxKm (null if none): walk outward from the point towards the
  // nearest coast edge and a little past it.
  function snapToWater(origin, nearSets, maxKm) {
    var sets = nearSets.filter(Boolean);
    if (!inLand(sets, origin.lng, origin.lat)) return { lat: origin.lat, lng: origin.lng, snapped: false };
    var coslat = Math.cos(origin.lat * D2R), kx = 111.32 * coslat, ky = 110.57, best = null;
    sets.forEach(function (set) {
      for (var p = 0; p < set.n; p++) {
        for (var r = set.ringStart[p]; r < set.ringStart[p + 1]; r++) {
          var s = set.vertStart[r], e = set.vertStart[r + 1];
          for (var k = s; k < e; k++) {
            var k2 = k + 1 < e ? k + 1 : s;
            var x0 = wrapLng(set.ll[k * 2] - origin.lng) * kx, y0 = (set.ll[k * 2 + 1] - origin.lat) * ky;
            var x1 = wrapLng(set.ll[k2 * 2] - origin.lng) * kx, y1 = (set.ll[k2 * 2 + 1] - origin.lat) * ky;
            var lim = maxKm + 1;
            if (Math.min(x0, x1) > lim || Math.max(x0, x1) < -lim || Math.min(y0, y1) > lim || Math.max(y0, y1) < -lim) continue;
            var ex = x1 - x0, ey = y1 - y0, L2 = ex * ex + ey * ey, t = L2 ? Math.max(0, Math.min(1, -(x0 * ex + y0 * ey) / L2)) : 0;
            var cx = x0 + t * ex, cy = y0 + t * ey, d = Math.sqrt(cx * cx + cy * cy);
            if (d <= maxKm && (!best || d < best.d)) best = { d: d, x: cx, y: cy };
          }
        }
      }
    });
    if (!best || best.d === 0) return null;
    var ux = best.x / best.d, uy = best.y / best.d;
    for (var extra = 0.02; best.d + extra <= maxKm + 0.2; extra *= 1.6) {
      var px = ux * (best.d + extra), py = uy * (best.d + extra);
      var cand = { lat: origin.lat + py / ky, lng: wrapLng(origin.lng + px / kx) };
      if (!inLand(sets, cand.lng, cand.lat)) return { lat: cand.lat, lng: cand.lng, snapped: true, km: best.d + extra };
    }
    return null;
  }
  // The whole computation, time-sliced: yieldFn() between batches (a Promise), shouldStop() aborts.
  function computeExposure(origin, nearSets, farSet, opts) {
    opts = opts || {};
    var ix = buildIndexes(origin, nearSets, farSet), fetch = new Float64Array(RAYS), i = 0;
    var batch = opts.batch || RAYS, yieldFn = opts.yieldFn, stop = opts.shouldStop || function () { return false; };
    function run() {
      var end = Math.min(RAYS, i + batch);
      for (; i < end; i++) fetch[i] = rayFetch(origin, rayBearing(i), ix.near, ix.far);
      if (stop()) return null;
      if (i < RAYS) return yieldFn ? yieldFn().then(run) : run();
      var out = summarise(fetch); out.fetch = fetch; out.origin = origin; return out;
    }
    return yieldFn ? Promise.resolve().then(run) : run();
  }

  // ---- the fan (an SVG string; the page wraps it in a marker so it keeps its size on screen) ----
  var FILL = { open: 'rgba(56,189,248,0.16)', light: 'rgba(120,125,130,0.55)', dark: 'rgba(38,40,44,0.82)' };
  function pt(c, r, b) { return (c + r * Math.sin(b * D2R)).toFixed(2) + ',' + (c - r * Math.cos(b * D2R)).toFixed(2); }
  function fanSvg(result, radius, selected) {
    var pad = 16, c = radius + pad, size = 2 * c, inner = 7, parts = [];
    parts.push('<svg xmlns="http://www.w3.org/2000/svg" width="' + size + '" height="' + size + '" viewBox="0 0 ' + size + ' ' + size + '">');
    result.sectors.forEach(function (s, k) {
      var a0 = s.from, a1 = s.to;
      var d = 'M' + pt(c, inner, a0) + 'L' + pt(c, radius, a0) + 'A' + radius + ',' + radius + ' 0 0 1 ' + pt(c, radius, a1) +
        'L' + pt(c, inner, a1) + 'A' + inner + ',' + inner + ' 0 0 0 ' + pt(c, inner, a0) + 'Z';
      var sel = k === selected;
      parts.push('<path data-k="' + k + '" d="' + d + '" fill="' + FILL[s.level] + '" stroke="' + (sel ? '#fde047' : 'rgba(255,255,255,0.55)') +
        '" stroke-width="' + (sel ? 2 : 0.6) + '"/>');
    });
    parts.push('<circle cx="' + c + '" cy="' + c + '" r="' + radius + '" fill="none" stroke="rgba(255,255,255,0.8)" stroke-width="1"/>');
    for (var b = 0; b < 360; b += 45) {
      parts.push('<line x1="' + pt(c, radius - 6, b).split(',')[0] + '" y1="' + pt(c, radius - 6, b).split(',')[1] + '" x2="' + pt(c, radius + 3, b).split(',')[0] +
        '" y2="' + pt(c, radius + 3, b).split(',')[1] + '" stroke="#fff" stroke-width="1.4"/>');
    }
    [['N', 0], ['E', 90], ['S', 180], ['W', 270]].forEach(function (l) {
      var xy = pt(c, radius + 10, l[1]).split(',');
      parts.push('<text x="' + xy[0] + '" y="' + xy[1] + '" text-anchor="middle" dominant-baseline="central" class="tools-fan-label">' + l[0] + '</text>');
    });
    parts.push('<circle cx="' + c + '" cy="' + c + '" r="4" fill="#fde047" stroke="#0b2536" stroke-width="1.5"/>');
    parts.push('</svg>');
    return { svg: parts.join(''), size: size, center: c };
  }
  // The wedge under a screen offset (dx, dy from the fan's centre), or -1.
  function sectorAt(dx, dy, radius) {
    var r = Math.sqrt(dx * dx + dy * dy);
    if (r < 7 || r > radius) return -1;
    var b = (Math.atan2(dx, -dy) * R2D + 360) % 360;
    return Math.min(SECTORS - 1, Math.floor(b / 5));
  }

  // ---- coast data (fetched on first use of the exposure tool) ----
  function CoastSource(base, fetchFn) { this.base = base; this.fetch = fetchFn; this.index = null; this.tier0 = null; this.chunks = new Map(); this._p = null; }
  CoastSource.prototype.load = function () {
    var self = this;
    if (this._p) return this._p;
    function get(path, kind) {
      return self.fetch(self.base + path, { mode: 'cors' }).then(function (r) {
        if (!r.ok) throw new Error('coast ' + r.status);
        return kind === 'json' ? r.json() : r.arrayBuffer();
      });
    }
    this._p = Promise.all([get('/index.json', 'json'), get('/world-i.bin', 'bin')]).then(function (res) {
      var idx = res[0];
      if (!idx || idx.format !== 'coast-v1' || !idx.tier1 || idx.tier1.cell !== 5 || !/^[A-Za-z0-9_-]+$/.test(idx.tier1.dir || '') || !idx.tier1.cells) throw new Error('coast index');
      self.index = idx; self.tier0 = decodeCoastLL(res[1]); return self;
    });
    this._p.catch(function () { self._p = null; });                          // a later use retries
    return this._p;
  };
  // Tier-1 cells around a point (the near window); resolves to decoded sets, or null when any listed cell failed.
  CoastSource.prototype.near = function (origin) {
    var self = this, cell = 5, names = {}, coslat = Math.max(0.2, Math.cos(origin.lat * D2R)), wy = 0.5, wx = Math.min(180, 0.5 / coslat);
    for (var y = origin.lat - wy; y <= origin.lat + wy + 1e-9; y += Math.min(wy, cell)) {
      for (var x = -wx; x <= wx + 1e-9; x += Math.min(wx, cell)) {
        var la = Math.floor(Math.max(-90, Math.min(89.999, y)) / cell) * cell, lo = Math.floor(wrapLng(origin.lng + x) / cell) * cell;
        names[la + '_' + lo] = true;
      }
    }
    var list = Object.keys(names).filter(function (n) { return Object.prototype.hasOwnProperty.call(self.index.tier1.cells, n); });
    return Promise.all(list.map(function (n) {
      if (self.chunks.has(n)) return Promise.resolve(self.chunks.get(n));
      return self.fetch(self.base + '/' + self.index.tier1.dir + '/' + n + '.bin', { mode: 'cors' }).then(function (r) {
        if (!r.ok) throw new Error('chunk ' + r.status);
        return r.arrayBuffer();
      }).then(function (b) {
        var set = decodeCoastLL(b);
        if (self.chunks.size >= 8) self.chunks.delete(self.chunks.keys().next().value);
        self.chunks.set(n, set); return set;
      });
    })).catch(function () { return null; });
  };

  // ---- the page: menu, tool bar, map interactions ----
  var TOOLS = { distance: 'Measure distance', area: 'Measure area', exposure: 'Swell exposure' };
  var state = null;

  function init(opts) {
    var L = root.L, map = opts.map, doc = root.document;
    if (!L || !map || !doc) return null;
    var btn = doc.getElementById('toolsBtn'), menu = doc.getElementById('toolsMenu');
    if (!btn || !menu) return null;
    var getUnit = opts.getUnit || function () { return 'US'; };
    var coast = opts.coastBase ? new CoastSource(opts.coastBase, opts.fetch || root.fetch.bind(root)) : null;
    var expoBtn = menu.querySelector('[data-tool="exposure"]');
    if (expoBtn && !coast) expoBtn.hidden = true;
    var s = state = { tool: null, pts: [], closed: false, group: L.featureGroup(), fan: null, result: null, selected: -1, gen: 0, busy: false, msg: '', dblWas: null };
    s.group.addTo(map);
    if (!map.getPane('toolsPane')) { var pane = map.createPane('toolsPane'); pane.style.zIndex = 590; pane.style.pointerEvents = 'none'; }

    // the menu (same behaviour as the gear's panel)
    var open = false;
    function setMenu(on) { open = on; menu.hidden = !on; btn.setAttribute('aria-expanded', on ? 'true' : 'false'); if (opts.onLayout) opts.onLayout(); }
    btn.addEventListener('click', function () { setMenu(!open); });
    doc.addEventListener('pointerdown', function (e) { if (open && !menu.contains(e.target) && !btn.contains(e.target)) setMenu(false); });
    menu.addEventListener('keydown', function (e) { if (e.key === 'Escape') { e.stopPropagation(); setMenu(false); btn.focus(); } });
    Array.prototype.forEach.call(menu.querySelectorAll('[data-tool]'), function (b) {
      b.addEventListener('click', function () { setMenu(false); start(b.getAttribute('data-tool')); });
    });

    // the tool bar (a control under the gear)
    var Bar = L.Control.extend({
      options: { position: 'topright' },
      onAdd: function () {
        var c = L.DomUtil.create('div', 'leaflet-control-toolsbar tools-bar');
        c.hidden = true; c.setAttribute('role', 'group');
        c.innerHTML = '<div class="tools-bar-head"><strong class="tools-bar-title"></strong><button type="button" class="tools-x" aria-label="Close tool">✕</button></div>' +
          '<div class="tools-bar-body" aria-live="polite"></div>' +
          '<div class="tools-bar-actions"><button type="button" data-act="undo">Undo</button><button type="button" data-act="finish">Finish</button><button type="button" data-act="clear">Clear</button></div>';
        L.DomEvent.disableClickPropagation(c); L.DomEvent.disableScrollPropagation(c);
        return c;
      }
    });
    var bar = new Bar(); map.addControl(bar);
    var barEl = bar.getContainer(), titleEl = barEl.querySelector('.tools-bar-title'), bodyEl = barEl.querySelector('.tools-bar-body');
    barEl.querySelector('.tools-x').addEventListener('click', function () { stop(); });
    barEl.querySelector('[data-act="undo"]').addEventListener('click', function () { undo(); });
    barEl.querySelector('[data-act="finish"]').addEventListener('click', function () { finish(); });
    barEl.querySelector('[data-act="clear"]').addEventListener('click', function () { clear(); });

    function unit() { return getUnit() === 'Metric' ? 'Metric' : 'US'; }
    function esc(t) { return String(t).replace(/[&<>"]/g, function (ch) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[ch]; }); }
    function setBody(lines) { bodyEl.innerHTML = lines.map(function (l) { return '<div class="' + (l.cls || '') + '">' + esc(l.t) + '</div>'; }).join(''); }
    function setActions() {
      var measuring = s.tool === 'distance' || s.tool === 'area';
      barEl.querySelector('[data-act="undo"]').hidden = !measuring || !s.pts.length || s.closed;
      barEl.querySelector('[data-act="finish"]').hidden = !measuring || s.closed || s.pts.length < (s.tool === 'area' ? 3 : 2);
      barEl.querySelector('[data-act="clear"]').hidden = !(s.pts.length || s.result || s.busy);
    }

    function start(tool) {
      if (!TOOLS[tool]) return;
      clear(true);
      s.tool = tool; barEl.hidden = false; titleEl.textContent = TOOLS[tool];
      Array.prototype.forEach.call(menu.querySelectorAll('[data-tool]'), function (b) { b.setAttribute('aria-pressed', b.getAttribute('data-tool') === tool ? 'true' : 'false'); });
      map.getContainer().classList.add('tools-active');
      if (s.dblWas === null) { s.dblWas = map.doubleClickZoom.enabled(); map.doubleClickZoom.disable(); }
      render();
      if (opts.onLayout) opts.onLayout();
    }
    function stop() {
      clear(true); s.tool = null; barEl.hidden = true;
      Array.prototype.forEach.call(menu.querySelectorAll('[data-tool]'), function (b) { b.setAttribute('aria-pressed', 'false'); });
      map.getContainer().classList.remove('tools-active');
      if (s.dblWas) map.doubleClickZoom.enable(); s.dblWas = null;
      if (opts.onLayout) opts.onLayout();
    }
    function clear(silent) {
      s.gen++; s.pts = []; s.closed = false; s.result = null; s.selected = -1; s.busy = false; s.msg = '';
      s.group.clearLayers(); if (s.fan) { map.removeLayer(s.fan); s.fan = null; }
      if (!silent) render();
    }
    function undo() { if (s.pts.length && !s.closed) { s.pts.pop(); render(); } }
    function finish() { if ((s.tool === 'distance' && s.pts.length >= 2) || (s.tool === 'area' && s.pts.length >= 3)) { s.closed = true; render(); } }

    // A touch tap (no hover to show a wedge's details): Chrome's click is a PointerEvent with pointerType; other
    // browsers fall back to the device's hover capability.
    function isTouch(ev) {
      if (ev && ev.pointerType) return ev.pointerType === 'touch' || ev.pointerType === 'pen';
      try { return !!(root.matchMedia && root.matchMedia('(hover: none)').matches); } catch (e) { return false; }
    }
    function click(latlng, ev) {
      if (!s.tool) return;
      var p = { lat: latlng.lat, lng: latlng.lng };
      if (s.tool === 'exposure') {
        if (s.result && s.fan && isTouch(ev)) {                         // a tap inside the fan picks a wedge; a mouse click places a new point
          var c = map.latLngToContainerPoint([s.result.origin.lat, s.fanLng]), q = map.latLngToContainerPoint(latlng);
          var k = sectorAt(q.x - c.x, q.y - c.y, s.radius);
          if (k >= 0) { select(k); return; }
        }
        return exposureAt(p);
      }
      if (s.closed) { s.pts = []; s.closed = false; }
      if (s.pts.length) {
        var last = s.pts[s.pts.length - 1], a = map.latLngToContainerPoint(last), b = map.latLngToContainerPoint(latlng);
        if (Math.abs(a.x - b.x) + Math.abs(a.y - b.y) < 4) return;       // the second click of a double-click
        if (s.tool === 'area' && s.pts.length >= 3) {
          var f = map.latLngToContainerPoint(s.pts[0]);
          if (Math.abs(f.x - b.x) + Math.abs(f.y - b.y) < 10) { s.closed = true; render(); return; }   // back on the first point
        }
        var lng = p.lng; while (lng - last.lng > 180) lng -= 360; while (lng - last.lng < -180) lng += 360; p.lng = lng;
      }
      s.pts.push(p); render();
    }

    function drawMeasure() {
      s.group.clearLayers();
      var pts = s.pts, u = unit(), line = [];
      for (var i = 1; i < pts.length; i++) { var seg = densify(pts[i - 1], pts[i], 25); line = line.concat(i > 1 ? seg.slice(1) : seg); }
      if (s.tool === 'area' && pts.length >= 3) {
        var ring = line.slice(); var back = densify(pts[pts.length - 1], pts[0], 25); ring = ring.concat(back.slice(1));
        L.polygon(ring.map(function (q) { return [q.lat, q.lng]; }), { color: '#fde047', weight: 2, fillColor: '#fde047', fillOpacity: s.closed ? 0.18 : 0.08, dashArray: s.closed ? null : '6 4', interactive: false }).addTo(s.group);
      } else if (line.length) {
        L.polyline(line.map(function (q) { return [q.lat, q.lng]; }), { color: '#fde047', weight: 3, interactive: false }).addTo(s.group);
      }
      var run = 0;
      pts.forEach(function (q, i) {
        if (i) run += distanceKm(pts[i - 1], q);
        var m = L.circleMarker([q.lat, q.lng], { radius: 4, color: '#0b2536', weight: 1.5, fillColor: '#fde047', fillOpacity: 1, interactive: false }).addTo(s.group);
        if (s.tool === 'distance' && i) m.bindTooltip(fmtLength(run, u), { permanent: true, direction: 'right', className: 'tools-label', offset: [6, 0] });
      });
      var lines = [];
      if (s.tool === 'distance') {
        lines.push({ t: pts.length < 2 ? 'Click the map to add points.' : 'Total ' + fmtLength(pathKm(pts), u), cls: 'tools-big' });
        if (pts.length >= 2 && !s.closed) lines.push({ t: 'Double-click or Finish to end.', cls: 'tools-hint' });
      } else {
        if (pts.length < 3) lines.push({ t: 'Click the map to outline an area (3+ points).', cls: 'tools-big' });
        else {
          lines.push({ t: 'Area ' + fmtArea(sphericalAreaKm2(pts), u), cls: 'tools-big' });
          lines.push({ t: 'Perimeter ' + fmtLength(pathKm(pts.concat([pts[0]])), u), cls: '' });
          if (!s.closed) lines.push({ t: 'Click the first point, double-click or Finish to close.', cls: 'tools-hint' });
        }
      }
      setBody(lines);
    }
    function drawExposure() {
      var u = unit(), lines = [];
      if (s.busy) lines.push({ t: 'Computing…', cls: 'tools-big' });
      else if (s.result) {
        lines.push({ t: windowsText(s.result.openWindows), cls: 'tools-big' });
        lines.push({ t: s.selected >= 0 ? sectorText(s.result.sectors[s.selected], u) : 'Point at (or tap) a wedge for details.', cls: 'tools-sector' });
        if (s.result.snapped) lines.push({ t: 'Moved to the nearest water.', cls: 'tools-hint' });
        if (s.result.coarse) lines.push({ t: 'Nearby coastline at lower detail.', cls: 'tools-hint' });
        lines.push({ t: 'Grey: shadowed by land (darker = more). Geometric exposure only: real swell bends around islands.', cls: 'tools-hint' });
      } else lines.push({ t: s.msg || 'Click the water to see which swell directions reach it.', cls: 'tools-big' });
      setBody(lines);
    }
    function render() {
      if (!s.tool) return;
      if (s.tool === 'exposure') drawExposure(); else drawMeasure();
      setActions();
      if (opts.onLayout) opts.onLayout();
    }
    function radius() { var w = map.getSize().x; return w < 576 ? 90 : 120; }
    function drawFan() {
      if (s.fan) { map.removeLayer(s.fan); s.fan = null; }
      if (!s.result) return;
      s.radius = radius();
      var f = fanSvg(s.result, s.radius, s.selected);
      var icon = L.divIcon({ className: 'tools-fan', html: f.svg, iconSize: [f.size, f.size], iconAnchor: [f.center, f.center] });
      s.fan = L.marker([s.result.origin.lat, s.fanLng], { icon: icon, interactive: false, keyboard: false, pane: 'toolsPane' }).addTo(map);
    }
    function select(k) { if (s.selected === k) return; s.selected = k; drawFan(); render(); }
    function exposureAt(p) {
      if (Math.abs(p.lat) > MAX_ABS_LAT) { clear(true); s.msg = 'Swell exposure works between 75°S and 75°N.'; render(); return; }
      var gen = ++s.gen, clickLng = p.lng, origin = { lat: p.lat, lng: wrapLng(p.lng) };
      s.group.clearLayers(); if (s.fan) { map.removeLayer(s.fan); s.fan = null; }
      s.result = null; s.selected = -1; s.busy = true; render();
      coast.load().then(function () { return coast.near(origin); }).then(function (nearSets) {
        if (gen !== s.gen) return null;
        var coarse = nearSets === null, sets = coarse ? [coast.tier0] : nearSets;
        var o = snapToWater(origin, coarse ? [coast.tier0] : (sets.length ? sets : []), 2);
        if (!o) { s.busy = false; s.msg = 'Click on the water to see swell exposure.'; render(); return null; }
        var start = { lat: o.lat, lng: o.lng };
        return computeExposure(start, sets, coast.tier0, {
          batch: 60, yieldFn: function () { return new Promise(function (r) { setTimeout(r, 0); }); },
          shouldStop: function () { return gen !== s.gen; }
        }).then(function (res) {
          if (!res || gen !== s.gen) return;
          res.snapped = o.snapped; res.coarse = coarse;
          s.result = res; s.busy = false; s.fanLng = clickLng + (o.lng - origin.lng);
          drawFan(); render();
        });
      }).catch(function () { if (gen !== s.gen) return; s.busy = false; s.msg = 'Coastline data unavailable. Try again later.'; render(); });
    }

    map.on('click', function (e) { if (s.tool) click(e.latlng, e.originalEvent); });
    map.on('dblclick', function () { if (s.tool === 'distance' || s.tool === 'area') finish(); });
    map.on('mousemove', function (e) {
      if (s.tool !== 'exposure' || !s.result || !s.fan) return;
      var c = map.latLngToContainerPoint([s.result.origin.lat, s.fanLng]);
      var k = sectorAt(e.containerPoint.x - c.x, e.containerPoint.y - c.y, s.radius);
      if (k >= 0) select(k);
    });
    map.on('resize', function () { if (s.result && s.radius !== radius()) drawFan(); });
    doc.addEventListener('keydown', function (e) {
      if (!s.tool || open) return;
      if (e.key === 'Escape') { e.stopPropagation(); if (s.pts.length || s.result) clear(); else stop(); }
      else if (e.key === 'Backspace' && (s.tool === 'distance' || s.tool === 'area') && !/INPUT|SELECT|TEXTAREA/.test((e.target && e.target.tagName) || '')) { e.preventDefault(); undo(); }
    }, true);
    if (opts.unitSelect) opts.unitSelect.addEventListener('change', function () { render(); });

    return { start: start, stop: stop, clear: clear, click: click };
  }

  var api = null;
  root.AllshoreTools = {
    init: function (opts) { api = init(opts); return api; },
    active: function () { return !!(state && state.tool); },
    click: function (latlng, ev) { if (api) api.click(latlng, ev); },
    _internals: {
      distanceKm: distanceKm, bearingDeg: bearingDeg, destination: destination, densify: densify, pathKm: pathKm,
      sphericalAreaKm2: sphericalAreaKm2, fmtLength: fmtLength, fmtArea: fmtArea, fmtDist: fmtDist, compass: compass,
      decodeCoastLL: decodeCoastLL, EdgeIndex: EdgeIndex, addPieces: addPieces, inLand: inLand, buildIndexes: buildIndexes,
      rayFetch: rayFetch, rayBearing: rayBearing, summarise: summarise, openWindows: openWindows, windowsText: windowsText,
      sectorText: sectorText, snapToWater: snapToWater, computeExposure: computeExposure, fanSvg: fanSvg, sectorAt: sectorAt,
      levelOf: levelOf, rayShadow: rayShadow, onCellLine: onCellLine, CoastSource: CoastSource, wrapLng: wrapLng,
      RAYS: RAYS, SECTORS: SECTORS, CAP_KM: CAP_KM, NEAR_KM: NEAR_KM, REF_MIN_KM: REF_MIN_KM, SHADOW_FULL_KM: SHADOW_FULL_KM, OPEN_BELOW: OPEN_BELOW, DARK_FROM: DARK_FROM
    }
  };
})(typeof window !== 'undefined' ? window : this);
