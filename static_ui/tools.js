/* Map tools (plan section 29): measure distance, measure area, and swell exposure.
 *
 * Distance and area use great-circle geometry on a sphere (R = 6371.0088 km), shown in the site's units plus
 * nautical miles.
 *
 * Swell exposure: from a water point, 720 great-circle rays (every 0.5 degrees, ten per 5-degree wedge) walk
 * outward until they cross a coastline or reach 3,000 km. The coastline is the GSHHG data the overlay already
 * publishes (coast-v1): tier 1 at full resolution for the first 50 km, tier 0 beyond. The builder clips polygons to
 * its cell grid; edges along a cell line cancel by net direction, so only real coast along a line remains.
 * - A click on land, or within 150 m of the shore, is evaluated 150 m off the shore (owner, G20: the shoreline data
 *   is good to ~50-100 m).
 * - The spot's reference distance F_ref: the 90th percentile of the rays that leave the spot's own coast (enclosed
 *   seas adapt: owner decision), 100..3,000 km; 3,000 km when 15 or more rays reach open ocean, blended in between
 *   from 5 rays (no flip at one ray more or less).
 * - A ray's shadow is 1 when land is within 15 km, falling with the log of the distance to 0 at F_ref; land beyond
 *   600 km reads open by itself and fades to nothing at 1,000 km (owner, G20 re-check: Cape Hatteras's NNE opens). A
 *   wedge's shadow is the mean over its ten rays: open below 0.2, light grey below 0.7, dark grey above.
 * A wedge at bearing b is swell arriving FROM b. Geometry only: real swell wraps headlands and islands, and reefs are
 * not in the data.
 *
 * window.AllshoreTools = { init(opts), active(), click(latlng, ev), _internals }. Nothing touches the DOM at load.
 */
(function (root) {
  'use strict';

  var R_KM = 6371.0088;
  var D2R = Math.PI / 180, R2D = 180 / Math.PI;
  var KM_PER_MI = 1.609344, KM_PER_NM = 1.852, FT_PER_KM = 3280.839895;
  var SPHERE_KM2 = 4 * Math.PI * R_KM * R_KM;

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
  // continuous from a (the line is drawn the short way, in a's world copy). Antipodal points have no single great
  // circle: they are joined directly.
  function densify(a, b, maxKm) {
    var d = centralAngle(a, b), out = [];
    if (d > Math.PI - 1e-9) return [{ lat: a.lat, lng: a.lng }, { lat: b.lat, lng: b.lng }];
    var n = Math.max(1, Math.ceil(d * R_KM / (maxKm || 50)));
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
  // spherical-excess sum over the (continuous-longitude) ring (Chamberlain & Duquette 2007). A ring around a pole
  // gives the complement, so the smaller of the two regions is returned.
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
    var a = Math.abs(s * R_KM * R_KM / 2) % SPHERE_KM2;
    return Math.min(a, SPHERE_KM2 - a);
  }
  // Whether two non-adjacent edges of the outline cross (longitudes made continuous first).
  function selfIntersects(pts) {
    var n = pts.length; if (n < 4) return false;
    var q = [{ x: pts[0].lng, y: pts[0].lat }];
    for (var i = 1; i < n; i++) { var x = pts[i].lng; while (x - q[i - 1].x > 180) x -= 360; while (x - q[i - 1].x < -180) x += 360; q.push({ x: x, y: pts[i].lat }); }
    function cross(a, b, c, d) {
      var d1 = (d.x - c.x) * (a.y - c.y) - (d.y - c.y) * (a.x - c.x), d2 = (d.x - c.x) * (b.y - c.y) - (d.y - c.y) * (b.x - c.x);
      var d3 = (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x), d4 = (b.x - a.x) * (d.y - a.y) - (b.y - a.y) * (d.x - a.x);
      return ((d1 > 0) !== (d2 > 0)) && ((d3 > 0) !== (d4 > 0)) && d1 && d2 && d3 && d4;
    }
    for (var e = 0; e < n; e++) for (var f = e + 2; f < n; f++) {
      if (e === 0 && f === n - 1) continue;                                   // they share the first point
      if (cross(q[e], q[(e + 1) % n], q[f], q[(f + 1) % n])) return true;
    }
    return false;
  }

  // ---- formatting (site units: 'US' or 'Metric'); the value is rounded before its precision and unit are chosen ----
  function fmtNum(v, dp) { return v.toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp }); }
  function roundTo(v, dp) { var k = Math.pow(10, dp); return Math.round(v * k) / k; }
  function sigRound(v) { var r2 = roundTo(v, 2); if (r2 < 10) return { v: r2, dp: 2 }; var r1 = roundTo(v, 1); if (r1 < 100) return { v: r1, dp: 1 }; return { v: Math.round(v), dp: 0 }; }
  function sig(v) { var r = sigRound(v); return fmtNum(r.v, r.dp); }
  function fmtLength(km, unit) {
    var nm = ' · ' + sig(km / KM_PER_NM) + ' nm';
    if (unit === 'Metric') return (Math.round(km * 1000) < 1000 ? fmtNum(Math.round(km * 1000), 0) + ' m' : sig(km) + ' km') + nm;
    var mi = km / KM_PER_MI;
    return (roundTo(mi, 2) < 0.5 ? fmtNum(Math.round(km * FT_PER_KM), 0) + ' ft' : sig(mi) + ' mi') + nm;
  }
  function fmtArea(km2, unit) {
    if (unit === 'Metric') return sigRound(km2 * 100).v < 100 ? sig(km2 * 100) + ' ha' : sig(km2) + ' km²';
    var mi2 = km2 / (KM_PER_MI * KM_PER_MI);
    return sigRound(mi2 * 640).v < 640 ? sig(mi2 * 640) + ' acres' : sig(mi2) + ' sq mi';
  }
  // A distance in the site unit: one decimal below 10, whole numbers above, "under 0.1" for the spot's own shore.
  function fmtDist(km, unit) {
    var v = unit === 'Metric' ? km : km / KM_PER_MI, u = unit === 'Metric' ? ' km' : ' mi';
    if (v < 0.1) return 'under 0.1' + u;
    var r1 = roundTo(v, 1);
    return (r1 < 10 ? fmtNum(r1, 1) : fmtNum(Math.round(v), 0)) + u;
  }
  // The rays' reach, as a round figure: "3,000+ km", "1,800+ mi".
  function fmtReach(unit) {
    var v = unit === 'Metric' ? CAP_KM : CAP_KM / KM_PER_MI;
    return fmtNum(Math.floor(v / 100) * 100, 0) + '+ ' + (unit === 'Metric' ? 'km' : 'mi');
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
    // every piece takes at least 5 bytes, every ring 1, every vertex 2: check before allocating (G20 A P3-12)
    if (5 * nP + nR + 2 * nV > buf.byteLength - 40) throw new Error('coast decode failed');
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

  // ---- coast edges around a point ----
  // Coordinates are local: x = longitude relative to the origin, wrapped into [-180, 180), y = latitude (degrees).
  // Rays and coast edges are short (<= 10 km, ~1 km), so both are straight lines in this plane at the latitudes a
  // surf spot has (the tool refuses |lat| > 75).
  function onLine(v, cell) { var r = v / cell; return Math.abs(r - Math.round(r)) * cell < 1e-7; }
  function onCellLine(x0, y0, x1, y1, cell) {
    if (!(cell > 0)) return false;
    return (Math.abs(y0 - y1) < 1e-9 && onLine(y0, cell)) || (Math.abs(x0 - x1) < 1e-9 && onLine(x0, cell));
  }
  // The edges of every piece whose box meets the window [-wx, wx] x [lat - wy, lat + wy], as a flat array
  // [x0, y0, x1, y1, ...] in local coordinates. The builder clips polygons to its cell grid (30 degrees for tier 0,
  // 5 for tier 1): that leaves edges ALONG the cell lines, the cell border between two cells' pieces (both directions)
  // and zero-width bridges (out and back). Along each cell line the edges are summed by direction and only the parts
  // with a non-zero net survive: borders, bridges and the +-180 split cancel, real coast running along a line stays
  // (G20 A P3-1).
  function collectEdges(sets, origin, wx, wy) {
    var out = [], lines = {}, lat0 = origin.lat - wy, lat1 = origin.lat + wy;
    for (var si = 0; si < sets.length; si++) {
      var set = sets[si]; if (!set) continue;
      for (var p = 0; p < set.n; p++) {
        var bx0 = set.box[p * 4], by0 = set.box[p * 4 + 1], bx1 = set.box[p * 4 + 2], by1 = set.box[p * 4 + 3];
        if (by1 < lat0 || by0 > lat1) continue;
        if (wx < 180 && bx1 - bx0 < 360) {
          var c0 = wrapLng(bx0 - origin.lng), c1 = c0 + (bx1 - bx0);          // the box relative to the origin (may run past +180)
          if (!((c1 >= -wx && c0 <= wx) || c1 - 360 >= -wx)) continue;
        }
        for (var r = set.ringStart[p]; r < set.ringStart[p + 1]; r++) {
          var s = set.vertStart[r], e = set.vertStart[r + 1];
          for (var k = s; k < e; k++) {
            var k2 = k + 1 < e ? k + 1 : s;
            var ax = set.ll[k * 2], ay = set.ll[k * 2 + 1], bx = set.ll[k2 * 2], by = set.ll[k2 * 2 + 1];
            var x0 = wrapLng(ax - origin.lng), x1 = wrapLng(bx - origin.lng);
            if (Math.abs(x1 - x0) > 180) continue;                           // crosses the origin's antimeridian: far away
            if (Math.max(ay, by) < lat0 || Math.min(ay, by) > lat1) continue;
            if (Math.max(x0, x1) < -wx || Math.min(x0, x1) > wx) continue;
            if (onCellLine(ax, ay, bx, by, set.cell)) {
              var horiz = Math.abs(ay - by) < 1e-9, key = horiz ? 'h' + Math.round(ay * 1e4) : 'v' + Math.round(x0 * 1e4);
              var L = lines[key] || (lines[key] = { horiz: horiz, at: horiz ? ay : x0, segs: [] });
              var a = horiz ? x0 : ay, b = horiz ? x1 : by;
              L.segs.push(a < b ? [a, b, 1] : [b, a, -1]);
              continue;
            }
            out.push(x0, ay, x1, by);
          }
        }
      }
    }
    Object.keys(lines).forEach(function (key) {
      var L = lines[key], ends = [];
      L.segs.forEach(function (sg) { ends.push(Math.round(sg[0] * 1e7) / 1e7, Math.round(sg[1] * 1e7) / 1e7); });
      ends.sort(function (a, b) { return a - b; });
      var u = ends.filter(function (v, i) { return i === 0 || v !== ends[i - 1]; });
      for (var i = 0; i + 1 < u.length; i++) {
        var mid = (u[i] + u[i + 1]) / 2, net = 0;
        L.segs.forEach(function (sg) { if (sg[0] <= mid && sg[1] >= mid) net += sg[2]; });
        if (net !== 0) { if (L.horiz) out.push(u[i], L.at, u[i + 1], L.at); else out.push(L.at, u[i], L.at, u[i + 1]); }
      }
    });
    return out;
  }
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
  function indexOf(edges, bucketDeg) {
    if (!edges.length) return null;
    var idx = new EdgeIndex(bucketDeg);
    for (var i = 0; i < edges.length; i += 4) idx.add(edges[i], edges[i + 1], edges[i + 2], edges[i + 3]);
    return idx.finish();
  }
  // Even-odd point-in-land over the pieces of the given sets. A point exactly on a clip seam is counted in one piece
  // only (the half-open crossing rule), so seams and bridges give consistent answers.
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
  var CAP_KM = 3000, NEAR_KM = 50, REF_MIN_KM = 100, REF_PCT = 0.9, BLEND_FROM = 5, BLEND_TO = 15;
  var SHADOW_FULL_KM = 15, FAR_FADE_KM = 600, FAR_LAND_KM = 1000, FAR_OPEN_MAX = 0.18;
  var OPEN_BELOW = 0.2, DARK_FROM = 0.7;
  var STANDOFF_KM = 0.15, SNAP_MAX_KM = 2, ON_COAST_KM = 0.005, OPEN_PROBE_KM = 2, OPEN_MIN_DIRS = 3;
  var PLACE_CELL_KM = 0.1, OPEN_WELL_DIRS = 6, OPEN_GOOD = 8, FALLBACK_SHARE = 0.8;
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
  // The spot's reference distance (G20: stable near the shore): the 90th percentile of the rays that leave its own
  // coast (floor 100 km: a small bay or sound is not "open"). A spot that sees open ocean through BLEND_TO rays or
  // more uses the full reach; between BLEND_FROM and BLEND_TO the two blend geometrically (G20 re-review: no flip
  // at one ray more or less).
  function referenceKm(fetch) {
    var capped = 0, leaving = [];
    for (var i = 0; i < fetch.length; i++) { if (fetch[i] >= CAP_KM) capped++; if (fetch[i] > SHADOW_FULL_KM) leaving.push(fetch[i]); }
    var own = leaving.length ? Math.max(REF_MIN_KM, Math.min(CAP_KM, percentile(leaving, REF_PCT))) : REF_MIN_KM;
    if (capped <= BLEND_FROM) return own;
    if (capped >= BLEND_TO) return CAP_KM;
    var w = (capped - BLEND_FROM) / (BLEND_TO - BLEND_FROM);
    return own * Math.pow(CAP_KM / own, w);
  }
  function levelOf(s) { return s < OPEN_BELOW ? 'open' : s < DARK_FROM ? 'light' : 'dark'; }
  // How much one ray's land blocks swell: fully within SHADOW_FULL_KM (the spot's own coast), falling off with the
  // log of the distance to nothing at F_ref (owner: nearby land darker; Kauai seen from the North Shore, ~150 km, is
  // light grey). Land beyond FAR_FADE_KM never makes a ray more than FAR_OPEN_MAX (below the open line) and fades to
  // nothing at FAR_LAND_KM (owner, G20 re-check: the shading nearer than 600 km stays as it was, and New England no
  // longer greys Cape Hatteras's NNE).
  function rayShadow(f, fRef) {
    if (f >= fRef || f >= FAR_LAND_KM) return 0;
    if (f <= SHADOW_FULL_KM) return 1;
    var s = 1 - Math.log(f / SHADOW_FULL_KM) / Math.log(fRef / SHADOW_FULL_KM);
    if (f > FAR_FADE_KM) s = Math.min(s, FAR_OPEN_MAX) * (FAR_LAND_KM - f) / (FAR_LAND_KM - FAR_FADE_KM);
    return Math.max(0, Math.min(1, s));
  }
  // From the per-ray fetches to the wedges.
  function summarise(fetch) {
    var fRef = referenceKm(fetch), sectors = [];
    for (var k = 0; k < SECTORS; k++) {
      var sum = 0, minLand = Infinity;
      for (var j = 0; j < PER_SECTOR; j++) {
        var f = fetch[k * PER_SECTOR + j];
        sum += rayShadow(f, fRef);
        if (f < CAP_KM && f < minLand) minLand = f;
      }
      var s = sum / PER_SECTOR;
      sectors.push({ from: k * 5, to: k * 5 + 5, s: s, level: levelOf(s), minLandKm: isFinite(minLand) ? minLand : null });
    }
    return { sectors: sectors, fRef: fRef, openWindows: openWindows(sectors) };
  }
  // Runs of open wedges, as [fromDeg, toDeg] going clockwise; a run through north is one window (e.g. [290, 15]).
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
  function spanOf(w) { return ((w[1] - w[0]) % 360 + 360) % 360 || 360; }
  var MAX_WINDOWS_LISTED = 4, WIDE_DEG = 45, EXCEPT_DEG = 300;
  // A window's name: the compass point of its centre, or of both ends when it is 45 degrees or wider (G20 re-review:
  // "SE" for a 270-degree window reads as "only SE swell").
  function nameOf(w) { var span = spanOf(w); return span >= WIDE_DEG ? compass(w[0]) + '–' + compass(w[1]) : compass(w[0] + span / 2); }
  function rangeOf(w) { return pad3(w[0]) + '°–' + pad3(w[1] % 360) + '°'; }
  // The widest four (in bearing order; the rest counted, G20 B P3-3); neighbours with the same name share it.
  function listText(ws) {
    var shown = ws;
    if (ws.length > MAX_WINDOWS_LISTED) {
      shown = ws.slice().sort(function (a, b) { return spanOf(b) - spanOf(a); }).slice(0, MAX_WINDOWS_LISTED);
      shown.sort(function (a, b) { return ws.indexOf(a) - ws.indexOf(b); });
    }
    var groups = [];
    shown.forEach(function (w) {
      var n = nameOf(w), g = groups[groups.length - 1];
      if (g && g.name === n) g.ranges.push(rangeOf(w)); else groups.push({ name: n, ranges: [rangeOf(w)] });
    });
    if (groups.length > 1 && groups[0].name === groups[groups.length - 1].name) {   // neighbours across north too
      var last = groups.pop(); groups[0].ranges = last.ranges.concat(groups[0].ranges);
    }
    return groups.map(function (g) { return g.name + ' (' + g.ranges.join(', ') + ')'; }).join(', ') +
      (ws.length > shown.length ? ' +' + (ws.length - shown.length) + ' more' : '');
  }
  // "Open: W (250°–275°), WNW–NE (295°–045°)"; open all round but a few wedges: "Open except S (180°–185°)".
  function windowsText(ws) {
    if (!ws.length) return 'No open swell window';
    if (ws.length === 1 && spanOf(ws[0]) === 360) return 'Open to swell from every direction';
    var total = 0; ws.forEach(function (w) { total += spanOf(w); });
    if (total >= EXCEPT_DEG) {                                                // name the gaps between the (clockwise) windows
      return 'Open except ' + listText(ws.map(function (w, i) { return [w[1] % 360, ws[(i + 1) % ws.length][0]]; })
        .sort(function (a, b) { return a[0] - b[0]; }));
    }
    return 'Open: ' + listText(ws);
  }
  function sectorText(sec, unit) {
    var head = compass(sec.from + 2.5) + ' ' + pad3(sec.from) + '–' + pad3(sec.to % 360) + '°: ';
    var pct = Math.round(sec.s * 100);
    if (sec.level === 'open') return head + (sec.minLandKm == null ? 'open ocean for ' + fmtReach(unit) : 'open (nearest land ' + fmtDist(sec.minLandKm, unit) + ')');
    return head + (sec.level === 'light' ? 'partly shadowed' : 'shadowed') + ' (' + pct + '%), land at ' + fmtDist(sec.minLandKm, unit);
  }
  // Both indexes for an origin: near = tier-1 sets (or tier 0 standing in) within 0.5 degrees, far = tier 0 within
  // the rays' reach (the exact longitude reach at the origin's latitude; the whole circle when a ray can pass a pole).
  function nearWindow(lat) { var coslat = Math.max(0.2, Math.cos(lat * D2R)); return { wx: Math.min(180, 0.5 / coslat), wy: 0.5 }; }
  function farWindow(lat) {
    var dl = CAP_KM / R_KM + 0.01, phi = Math.abs(lat) * D2R;
    var wx = dl >= Math.PI / 2 - phi ? 180 : Math.min(180, Math.asin(Math.min(1, Math.sin(dl) / Math.cos(phi))) * R2D + 0.5);
    return { wx: wx, wy: dl * R2D + 0.5 };
  }
  function buildIndexes(origin, nearSets, farSet) {
    var nw = nearWindow(origin.lat), fw = farWindow(origin.lat);
    return { near: indexOf(collectEdges(nearSets, origin, nw.wx, nw.wy), 0.02), far: indexOf(collectEdges([farSet], origin, fw.wx, fw.wy), 0.5) };
  }
  // Where to evaluate a click (G20, owner: 150 m off the shore). Returns {lat, lng, moved (km)} or null.
  // - Open water at least STANDOFF_KM from any coast: the click itself.
  // - A land click walks towards the nearest coast point and, once through the coast, turns along that edge's normal,
  //   straight out to sea; if it has not reached water two steps past that point (it only grazed the tip of a cove),
  //   it stops there. A water click within the stand-off walks away from the nearest coast.
  // - Unless that walk reached a point STANDOFF_KM out that looks well out (OPEN_GOOD of 16 directions run
  //   OPEN_PROBE_KM without meeting land), 24 directions are also searched (up to 2 km on land, twice the stand-off on
  //   water). The nearest point that reaches the stand-off and looks out well (OPEN_WELL_DIRS) wins, else the nearest
  //   that looks out at all (OPEN_MIN_DIRS), else the nearest that reaches the stand-off; with none, of the water found
  //   the nearest that looks out, else the nearest at least FALLBACK_SHARE as clear as the clearest. Measured on
  //   48,000 clicks against 1.11.1 (G20 re-check): a bay beats a pond or an inner sound.
  // - A walk stops at the far shore of its own water; the coast data says which side a crossing leads to. A point on
  //   the coastline itself is never used, a land click never settles within ON_COAST_KM of a coast (every ray would stop
  //   at once), and with no such water within 2 km it is refused.
  function placeOrigin(origin, sets, opts) {
    opts = opts || {};
    var maxKm = opts.maxKm == null ? SNAP_MAX_KM : opts.maxKm, stand = opts.standKm == null ? STANDOFF_KM : opts.standKm;
    sets = sets.filter(Boolean);
    var kx = 111.32 * Math.max(0.05, Math.cos(origin.lat * D2R)), ky = 110.57, lim = maxKm + Math.max(stand, OPEN_PROBE_KM) + 1;   // the probe's reach too
    var deg = collectEdges(sets, origin, Math.min(180, lim / kx), lim / ky), E = [];
    for (var i = 0; i < deg.length; i += 4) E.push(deg[i] * kx, (deg[i + 1] - origin.lat) * ky, deg[i + 2] * kx, (deg[i + 3] - origin.lat) * ky);
    // The edges in PLACE_CELL_KM buckets, so a step, a probe or a nearest-edge search tests only the buckets it touches
    // (G20 re-check R4: estuaries have 5,000+ edges in the window). An edge goes only into the buckets it crosses, and
    // only inside the window (every query stays inside it): a tier-0 edge can be 100 km long (G20 re-check R5).
    var cells = new Map(), stamp = new Int32Array(E.length / 4), mark = 0, W = lim + PLACE_CELL_KM, EPS = 1e-9;
    var tally = opts.stats || {}; tally.tests = 0; tally.entries = 0;
    function cell(v) { return Math.floor(v / PLACE_CELL_KM); }
    function key(ix, iy) { return ix * 1048576 + iy; }
    function put(ix, iy, e) { var kk = key(ix, iy), l = cells.get(kk); if (l) l.push(e); else cells.set(kk, [e]); tally.entries++; }
    for (var e = 0; e < E.length / 4; e++) {                                  // row by row: the x span of the edge in each row
      var ax = E[e * 4], ay = E[e * 4 + 1], bx = E[e * 4 + 2], by = E[e * 4 + 3];
      var ylo = Math.max(Math.min(ay, by), -W), yhi = Math.min(Math.max(ay, by), W);
      if (ylo > yhi || Math.max(ax, bx) < -W || Math.min(ax, bx) > W) continue;
      for (var jj = cell(ylo - EPS), j1 = cell(yhi + EPS); jj <= j1; jj++) {
        var xa = ax, xb = bx;
        if (by !== ay) {
          var ta = Math.max(0, Math.min(1, (Math.max(jj * PLACE_CELL_KM, ylo) - EPS - ay) / (by - ay)));
          var tb = Math.max(0, Math.min(1, (Math.min((jj + 1) * PLACE_CELL_KM, yhi) + EPS - ay) / (by - ay)));
          xa = ax + (bx - ax) * ta; xb = ax + (bx - ax) * tb;
        }
        var xl = Math.max(Math.min(xa, xb), -W), xh = Math.min(Math.max(xa, xb), W);
        for (var ii = cell(xl - EPS), i1 = cell(xh + EPS); ii <= i1; ii++) put(ii, jj, e);
      }
    }
    function each(x0, y0, x1, y1, fn) {                                       // every edge in the buckets over a box, once
      mark++;
      if (opts.brute) { for (var b = 0; b < E.length / 4; b++) { tally.tests++; fn(b * 4); } return; }   // (tests: the index is exact)
      for (var ix = cell(Math.min(x0, x1)), ix1 = cell(Math.max(x0, x1)); ix <= ix1; ix++) {
        for (var iy = cell(Math.min(y0, y1)), iy1 = cell(Math.max(y0, y1)); iy <= iy1; iy++) {
          var l = cells.get(key(ix, iy)); if (!l) continue;
          for (var n = 0; n < l.length; n++) { if (stamp[l[n]] !== mark) { stamp[l[n]] = mark; tally.tests++; fn(l[n] * 4); } }
        }
      }
    }
    function nearest(px, py) {                                                // growing boxes until the best is inside
      var best = null;
      function test(j) {
        var x0 = E[j] - px, y0 = E[j + 1] - py, ex = E[j + 2] - E[j], ey = E[j + 3] - E[j + 1], L2 = ex * ex + ey * ey;
        var t = L2 ? Math.max(0, Math.min(1, -(x0 * ex + y0 * ey) / L2)) : 0, cx = x0 + t * ex, cy = y0 + t * ey, d = Math.sqrt(cx * cx + cy * cy);
        // ties (here, and a walk through a vertex in cuts) go to the lower edge: no answer depends on the bucket order
        if (!best || d < best.d || (d === best.d && j < best.j)) best = { d: d, cx: cx, cy: cy, ex: ex, ey: ey, j: j };
      }
      for (var r = PLACE_CELL_KM; ; r *= 2) {
        each(px - r, py - r, px + r, py + r, test);
        if ((best && best.d <= r) || r > 2 * lim) return best;
      }
    }
    // The coast crossings of the step a->b: how many (edges met at one shared vertex count once) and the first one's
    // parameter and edge. A step owns [a, b): a crossing exactly at b belongs to the next step, so a walk whose samples
    // land on a coast still counts it once (G20 re-check); `atStart` drops one exactly at a (a walk starting on a coast).
    function cuts(ax, ay, bx, by, atStart) {
      var dx = bx - ax, dy = by - ay, ts = [], first = null;
      each(ax, ay, bx, by, function (j) {
        var cx = E[j], cy = E[j + 1], ex = E[j + 2] - cx, ey = E[j + 3] - cy, den = dx * ey - dy * ex;
        if (!den) return;
        var t = ((cx - ax) * ey - (cy - ay) * ex) / den, u = ((cx - ax) * dy - (cy - ay) * dx) / den;
        if (!(t >= (atStart ? 1e-9 : -1e-9) && t < 1 - 1e-9 && u >= 0 && u <= 1)) return;
        if (ts.every(function (v) { return Math.abs(v - t) > 1e-7; })) ts.push(t);
        if (!first || t < first.t || (t === first.t && j < first.j)) first = { t: t, ex: ex, ey: ey, j: j };
      });
      return { n: ts.length, first: first };
    }
    function water(px, py) { return !inLand(sets, wrapLng(origin.lng + px / kx), origin.lat + py / ky); }
    var land = !water(0, 0), nb = nearest(0, 0), step = 0.02;
    // exactly on the coastline (a data vertex or edge): the water side, stepping off along the edge's normal (which
    // side the point test calls it can flip with rounding, and a walk counting crossings must not start on a coast)
    var onCoast = !!nb && nb.d <= 1e-6;
    if (onCoast) land = false;
    if (!land && (!nb || nb.d >= stand)) return { lat: origin.lat, lng: origin.lng, moved: 0 };
    if (!nb) return land ? null : { lat: origin.lat, lng: origin.lng, moved: 0 };
    var cands = [];                                                           // {px, py, clr, d (km walked)}
    if (!land && !onCoast) cands.push({ px: 0, py: 0, clr: nb.d, d: 0 });   // (a point on the coastline is never kept)
    // One walk from (x0, y0) along (ux, uy): `inWater` says where it starts; after a crossing the coast data says
    // which side it is on (a grazed cove tip counts no crossing, or one; water narrower than one step, crossed within
    // it, leaves the walk on land). It stops at the far shore of its water, at the first sample reaching the stand-off,
    // at `len`, or (`giveUp`) when still on land that far along. `turn` (land clicks): once in the water, continue
    // along the crossed edge's normal instead.
    function walk(x0, y0, ux, uy, len, inWater, turn, base, giveUp) {
      var qx = x0, qy = y0, t = 0, dist = base;
      while (t + step <= len + 1e-9) {
        t += step;
        var px = qx + ux * step, py = qy + uy * step, c = cuts(qx, qy, px, py, t === step);
        if (c.n) {
          if (inWater) return;                                                // the far shore, or a thin spit
          // which side: the coast data, unless the step ends on the coastline itself (then the crossing count)
          var on = nearest(px, py);
          inWater = on && on.d <= 1e-6 ? c.n % 2 === 1 : water(px, py);
          if (inWater && turn && c.first) {                                  // through the coast: straight out to sea
            var cxp = qx + (px - qx) * c.first.t, cyp = qy + (py - qy) * c.first.t, el = Math.sqrt(c.first.ex * c.first.ex + c.first.ey * c.first.ey) || 1;
            var nx = -c.first.ey / el, ny = c.first.ex / el;
            if (nx * ux + ny * uy < 0) { nx = -nx; ny = -ny; }                  // the side the walk crossed into
            return walk(cxp, cyp, nx, ny, Math.min(len - t + step, 2 * stand), true, false, dist + step * c.first.t, null);
          }
        }
        dist += step; qx = px; qy = py;
        if (!inWater) { if (giveUp != null && t > giveUp) return; continue; }
        var n2 = nearest(px, py), clr = n2 ? n2.d : Infinity;
        cands.push({ px: px, py: py, clr: clr, d: dist });
        if (clr >= stand) return;
      }
    }
    // a candidate really is water (a turn can pick the land side of a feature narrower than a step): checked once
    function valid(c) { if (c.ok === undefined) c.ok = c.px === 0 && c.py === 0 ? !land : water(c.px, c.py); return c.ok; }
    // how many of 16 directions run OPEN_PROBE_KM from a point without meeting a coast (probed in bucket-sized steps)
    function openness(c) {
      if (c.open === undefined) {
        c.open = 0;
        var parts = Math.ceil(OPEN_PROBE_KM / PLACE_CELL_KM), sl = OPEN_PROBE_KM / parts;
        for (var k = 0; k < 16; k++) {
          var sx = Math.sin(k * 22.5 * D2R) * sl, sy = Math.cos(k * 22.5 * D2R) * sl, free = true;
          for (var m = 0; m < parts && free; m++) if (cuts(c.px + sx * m, c.py + sy * m, c.px + sx * (m + 1), c.py + sy * (m + 1), m === 0).n) free = false;
          if (free) c.open++;
        }
      }
      return c.open;
    }
    function byDist(p, q) { return p.d - q.d; }
    // the nearest candidate reaching the stand-off that looks out well (OPEN_WELL_DIRS: a bay beats a pond or an inner
    // sound, G20 re-checks), else the nearest that looks out at all
    function reachedOpen() {
      var list = cands.filter(function (c) { return c.clr >= stand && valid(c) && openness(c) >= OPEN_MIN_DIRS; }).sort(byDist);
      return list.filter(function (c) { return c.open >= OPEN_WELL_DIRS; })[0] || list[0] || null;
    }
    function reachedAny() { return cands.filter(function (c) { return c.clr >= stand && valid(c); }).sort(byDist)[0] || null; }
    // 1. towards (land) or away from (water) the nearest coast
    var ux, uy;
    if (nb.d > 1e-6) { ux = nb.cx / nb.d; uy = nb.cy / nb.d; if (!land) { ux = -ux; uy = -uy; } }
    else {                                                                    // exactly on an edge: its normal, towards the water
      var el = Math.sqrt(nb.ex * nb.ex + nb.ey * nb.ey) || 1, nx = -nb.ey / el, ny = nb.ex / el;
      if (water(nx * 0.01, ny * 0.01)) { ux = nx; uy = ny; } else { ux = -nx; uy = -ny; }
    }
    var reach = land ? maxKm : Math.min(maxKm, 2 * stand);
    walk(0, 0, ux, uy, reach, !land, land, 0, land ? nb.d + 2 * step : null);
    // 2. unless that found open water well out, search around the click
    var first = reachedOpen();
    if (!first || first.open < OPEN_GOOD) for (var a = 0; a < 360; a += 15) walk(0, 0, Math.sin(a * D2R), Math.cos(a * D2R), reach, !land, false, 0, null);
    var pick = reachedOpen() || reachedAny();
    if (!pick) {                                                              // no point reaches the stand-off
      var minClr = land || onCoast ? ON_COAST_KM : 0, pool = cands.filter(function (c) { return c.clr >= minClr && valid(c); }).sort(byDist);
      pick = pool.filter(function (c) { return c.clr >= ON_COAST_KM && openness(c) >= OPEN_MIN_DIRS; })[0];
      if (!pick && pool.length) {
        var top = 0; pool.forEach(function (c) { top = Math.max(top, c.clr); });
        pick = pool.filter(function (c) { return c.clr >= FALLBACK_SHARE * top; })[0];
      }
    }
    if (!pick) return land || onCoast ? null : { lat: origin.lat, lng: origin.lng, moved: 0 };
    if (pick.px === 0 && pick.py === 0) return { lat: origin.lat, lng: origin.lng, moved: 0 };
    return { lat: origin.lat + pick.py / ky, lng: wrapLng(origin.lng + pick.px / kx), moved: Math.sqrt(pick.px * pick.px + pick.py * pick.py) };
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
  // Owner (G20): open directions stay clear with a bright rim; shadow greys light enough to read over the dark ocean;
  // lines only where the shading changes; a clear centre ring so the break stays visible.
  var FILL = { open: 'none', light: 'rgba(214,219,224,0.58)', dark: 'rgba(84,90,98,0.80)' };
  var RIM = '#22d3ee', RIM_UNDER = '#0b2536', SELECT = '#fde047', LABEL_PAD = 16;
  function innerRadius(radius) { return Math.round(radius * 0.24); }
  function pt(c, r, b) { return (c + r * Math.sin(b * D2R)).toFixed(2) + ',' + (c - r * Math.cos(b * D2R)).toFixed(2); }
  function wedgePath(c, inner, radius, a0, a1) {
    return 'M' + pt(c, inner, a0) + 'L' + pt(c, radius, a0) + 'A' + radius + ',' + radius + ' 0 0 1 ' + pt(c, radius, a1) +
      'L' + pt(c, inner, a1) + 'A' + inner + ',' + inner + ' 0 0 0 ' + pt(c, inner, a0) + 'Z';
  }
  function fanSvg(result, radius, selected) {
    var c = radius + LABEL_PAD, size = 2 * c, inner = innerRadius(radius), parts = [], secs = result.sectors;
    parts.push('<svg xmlns="http://www.w3.org/2000/svg" width="' + size + '" height="' + size + '" viewBox="0 0 ' + size + ' ' + size + '">');
    secs.forEach(function (s, k) {
      var sel = k === selected;
      parts.push('<path data-k="' + k + '" d="' + wedgePath(c, inner, radius, s.from, s.to) + '" fill="' + FILL[s.level] + '" stroke="' + (sel ? SELECT : 'none') + '" stroke-width="2"/>');
    });
    secs.forEach(function (s, k) {                                             // separators where the shading changes
      if (s.level === secs[(k + secs.length - 1) % secs.length].level) return;
      var a = pt(c, inner, s.from).split(','), b = pt(c, radius, s.from).split(',');
      parts.push('<line x1="' + a[0] + '" y1="' + a[1] + '" x2="' + b[0] + '" y2="' + b[1] + '" stroke="rgba(255,255,255,0.7)" stroke-width="1"/>');
    });
    parts.push('<circle cx="' + c + '" cy="' + c + '" r="' + radius + '" fill="none" stroke="rgba(255,255,255,0.8)" stroke-width="1"/>');
    parts.push('<circle cx="' + c + '" cy="' + c + '" r="' + inner + '" fill="none" stroke="rgba(255,255,255,0.7)" stroke-width="1"/>');
    (result.openWindows || []).forEach(function (w) {                        // the open windows' rim, on a dark underlay
      var span = spanOf(w), rr = radius - 2;                                  // (it reads over the overlay's cyan: G20 re-review)
      [[RIM_UNDER, 7, ''], [RIM, 4, ' class="tools-rim"']].forEach(function (st) {
        if (span >= 360) parts.push('<circle' + st[2] + ' cx="' + c + '" cy="' + c + '" r="' + rr + '" fill="none" stroke="' + st[0] + '" stroke-width="' + st[1] + '"/>');
        else parts.push('<path' + st[2] + ' d="M' + pt(c, rr, w[0]) + 'A' + rr + ',' + rr + ' 0 ' + (span > 180 ? 1 : 0) + ' 1 ' + pt(c, rr, w[0] + span) +
          '" fill="none" stroke="' + st[0] + '" stroke-width="' + st[1] + '" stroke-linecap="butt"/>');
      });
    });
    for (var b = 0; b < 360; b += 45) {
      var t0 = pt(c, radius - 6, b).split(','), t1 = pt(c, radius + 3, b).split(',');
      parts.push('<line x1="' + t0[0] + '" y1="' + t0[1] + '" x2="' + t1[0] + '" y2="' + t1[1] + '" stroke="#fff" stroke-width="1.4"/>');
    }
    [['N', 0], ['E', 90], ['S', 180], ['W', 270]].forEach(function (l) {
      var xy = pt(c, radius + 10, l[1]).split(',');
      parts.push('<text x="' + xy[0] + '" y="' + xy[1] + '" text-anchor="middle" dominant-baseline="central" class="tools-fan-label">' + l[0] + '</text>');
    });
    parts.push('<circle cx="' + c + '" cy="' + c + '" r="3" fill="' + SELECT + '" stroke="#0b2536" stroke-width="1.2"/>');
    parts.push('</svg>');
    return { svg: parts.join(''), size: size, center: c };
  }
  // The wedge under a screen offset (dx, dy from the fan's centre), or -1 (inside the clear ring or outside the fan).
  function sectorAt(dx, dy, radius) {
    var r = Math.sqrt(dx * dx + dy * dy);
    if (r < innerRadius(radius) || r > radius) return -1;
    var b = (Math.atan2(dx, -dy) * R2D + 360) % 360;
    return Math.min(SECTORS - 1, Math.floor(b / 5));
  }
  // Where the fan should sit (container px): the nearest position to (cx, cy) whose disc (the fan plus its labels)
  // lies inside the map and clear of every obstacle rect {l, t, r, b}; the radius shrinks (to minR) when nothing fits
  // (G20: short maps, the tool bar, the map's other controls and the windows). A spot within opts.maxPan is preferred
  // to a larger fan farther away (G20 re-review: long pans). When nothing is clear at minR: the spot with the least
  // overlap, never with its centre on a `hard` obstacle (the tool bar), flagged `overlap`. Returns {x, y, r} or null.
  function placeFan(w, h, cx, cy, r0, obstacles, opts) {
    opts = opts || {};
    var pad = opts.pad == null ? 8 : opts.pad, minR = opts.minR || 50, step = opts.step || 8;
    var maxPan = opts.maxPan == null ? Infinity : opts.maxPan;
    function clear(x, y, e) {
      if (x - e < pad || y - e < pad || x + e > w - pad || y + e > h - pad) return false;
      for (var i = 0; i < obstacles.length; i++) {
        var o = obstacles[i], nx = Math.max(o.l, Math.min(x, o.r)), ny = Math.max(o.t, Math.min(y, o.b));
        if ((x - nx) * (x - nx) + (y - ny) * (y - ny) < e * e) return false;
      }
      return true;
    }
    function search(lim2) {
      for (var r = r0; r >= minR; r -= 10) {
        var e = r + LABEL_PAD;
        if (clear(cx, cy, e)) return { x: cx, y: cy, r: r };
        var best = null, bd = Infinity;
        for (var y = e + pad; y <= h - e - pad; y += step) {
          for (var x = e + pad; x <= w - e - pad; x += step) {
            var d = (x - cx) * (x - cx) + (y - cy) * (y - cy);
            if (d < bd && d <= lim2 && clear(x, y, e)) { bd = d; best = { x: x, y: y, r: r }; }
          }
        }
        if (best) return best;
      }
      return null;
    }
    return search(maxPan * maxPan) || (maxPan < Infinity ? search(Infinity) : null) || leastOverlap(w, h, cx, cy, minR, obstacles, step);
  }
  // Nothing clear: score sample points of the disc (outside the map 1, on an obstacle 1, on a hard obstacle 10).
  function leastOverlap(w, h, cx, cy, r, obstacles, step) {
    if (!(w > 0 && h > 0)) return null;
    step = Math.max(step, Math.ceil(Math.sqrt(w * h / 5000)));                // about 5,000 positions at most (a 4K map)
    var e = r + LABEL_PAD, pts = [[0, 0]], best = null;
    [0.35, 0.7, 1].forEach(function (f) { for (var a = 0; a < 360; a += 30) pts.push([Math.sin(a * D2R) * e * f, -Math.cos(a * D2R) * e * f]); });
    function inRect(o, x, y) { return x >= o.l && x <= o.r && y >= o.t && y <= o.b; }
    function axis(len) { var out = []; if (len <= 2 * e) out.push(len / 2); else for (var v = e; v <= len - e; v += step) out.push(v); return out; }
    var xs = axis(w), ys = axis(h);
    for (var yi = 0; yi < ys.length; yi++) for (var xi = 0; xi < xs.length; xi++) {
      var x = xs[xi], y = ys[yi], score = 0, bad = false;
      for (var i = 0; i < obstacles.length && !bad; i++) if (obstacles[i].hard && inRect(obstacles[i], x, y)) bad = true;
      if (bad) continue;
      for (var k = 0; k < pts.length; k++) {
        var px = x + pts[k][0], py = y + pts[k][1];
        if (px < 0 || py < 0 || px > w || py > h) { score += 1; continue; }
        for (var j = 0; j < obstacles.length; j++) if (inRect(obstacles[j], px, py)) score += obstacles[j].hard ? 10 : 1;
      }
      var d = (x - cx) * (x - cx) + (y - cy) * (y - cy);
      if (!best || score < best.score || (score === best.score && d < best.d)) best = { x: x, y: y, r: r, overlap: true, score: score, d: d };
    }
    return best && { x: best.x, y: best.y, r: r, overlap: true };
  }

  // ---- coast data (fetched on first use of the exposure tool) ----
  var CHUNK_CACHE = 8;
  function CoastSource(base, fetchFn) { this.base = base; this.fetch = fetchFn; this.index = null; this.tier0 = null; this.chunks = new Map(); this.inflight = new Map(); this._p = null; }
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
  // One tier-1 cell: a cached set (refreshed in the LRU), the request already in flight, or a new fetch.
  CoastSource.prototype.chunk = function (n) {
    var self = this;
    if (this.chunks.has(n)) { var hit = this.chunks.get(n); this.chunks.delete(n); this.chunks.set(n, hit); return Promise.resolve(hit); }
    if (this.inflight.has(n)) return this.inflight.get(n);
    var p = this.fetch(this.base + '/' + this.index.tier1.dir + '/' + n + '.bin', { mode: 'cors' }).then(function (r) {
      if (!r.ok) throw new Error('chunk ' + r.status);
      return r.arrayBuffer();
    }).then(function (b) {
      var set = decodeCoastLL(b);
      self.inflight.delete(n);
      if (self.chunks.size >= CHUNK_CACHE) self.chunks.delete(self.chunks.keys().next().value);
      self.chunks.set(n, set); return set;
    }).catch(function (err) { self.inflight.delete(n); throw err; });   // a failed fetch OR decode is retried next time
    this.inflight.set(n, p);
    return p;
  };
  // Tier-1 cells around a point (the near window); resolves to decoded sets, or null when any listed cell failed.
  CoastSource.prototype.near = function (origin) {
    var self = this, cell = 5, names = {}, nw = nearWindow(origin.lat);
    for (var y = origin.lat - nw.wy; y <= origin.lat + nw.wy + 1e-9; y += Math.min(nw.wy, cell)) {
      for (var x = -nw.wx; x <= nw.wx + 1e-9; x += Math.min(nw.wx, cell)) {
        var la = Math.floor(Math.max(-90, Math.min(89.999, y)) / cell) * cell, lo = Math.floor(wrapLng(origin.lng + x) / cell) * cell;
        names[la + '_' + lo] = true;
      }
    }
    var list = Object.keys(names).filter(function (n) { return Object.prototype.hasOwnProperty.call(self.index.tier1.cells, n); });
    return Promise.all(list.map(function (n) { return self.chunk(n); })).catch(function () { return null; });
  };

  // ---- the page: menu, tool bar, map interactions ----
  var TOOLS = { distance: 'Measure distance', area: 'Measure area', exposure: 'Swell exposure' };
  var TWIN_MOUSE_PX = 4, TWIN_TOUCH_PX = 16;
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
    var s = state = { tool: null, pts: [], closed: false, closedAt: null, group: L.featureGroup(), fan: null, result: null, selected: -1, gen: 0, busy: false, msg: '', dblWas: null, radius: 0, barH: -1, barMax: 0, size: null };
    s.group.addTo(map);
    if (!map.getPane('toolsPane')) { var pane = map.createPane('toolsPane'); pane.style.zIndex = 590; pane.style.pointerEvents = 'none'; }

    function touchUI() { try { return !!(root.matchMedia && root.matchMedia('(hover: none)').matches); } catch (e) { return false; } }
    // A touch tap (no hover to show a wedge's details): Chrome's click is a PointerEvent with pointerType; other
    // browsers fall back to the device's hover capability.
    function isTouch(ev) { if (ev && ev.pointerType) return ev.pointerType === 'touch' || ev.pointerType === 'pen'; return touchUI(); }

    // the menu (same behaviour as the gear's panel)
    var open = false;
    function setMenu(on) {
      open = on; menu.hidden = !on; btn.setAttribute('aria-expanded', on ? 'true' : 'false');
      if (on) { var first = Array.prototype.filter.call(menu.querySelectorAll('[data-tool]'), function (x) { return !x.hidden; })[0]; if (first && first.focus) try { first.focus(); } catch (e) { /* no focus */ } }   // Escape reaches the menu (G20 re-review)
    }
    btn.addEventListener('click', function () { setMenu(!open); });
    doc.addEventListener('pointerdown', function (e) { if (open && !menu.contains(e.target) && !btn.contains(e.target)) setMenu(false); });
    menu.addEventListener('keydown', function (e) { if (e.key === 'Escape') { e.stopPropagation(); setMenu(false); btn.focus(); } });
    Array.prototype.forEach.call(menu.querySelectorAll('[data-tool]'), function (b) {
      b.addEventListener('click', function () { setMenu(false); start(b.getAttribute('data-tool')); });
    });

    // the tool bar (a control under the gear; the settings control stacks above it, so its menus stay on top)
    var Bar = L.Control.extend({
      options: { position: 'topright' },
      onAdd: function () {
        var c = L.DomUtil.create('div', 'leaflet-control-toolsbar tools-bar');
        c.hidden = true; c.setAttribute('role', 'group'); c.setAttribute('aria-labelledby', 'toolsBarTitle');
        // the actions sit under the title and the body scrolls, so Clear stays reachable on a short map (G20 re-review)
        c.innerHTML = '<div class="tools-bar-head"><strong class="tools-bar-title" id="toolsBarTitle"></strong><button type="button" class="tools-x" aria-label="Close tool">✕</button></div>' +
          '<div class="tools-bar-actions"><button type="button" data-act="undo">Undo</button><button type="button" data-act="finish">Finish</button><button type="button" data-act="clear">Clear</button></div>' +
          '<div class="tools-bar-body" aria-live="polite"></div>';
        L.DomEvent.disableClickPropagation(c); L.DomEvent.disableScrollPropagation(c);
        return c;
      }
    });
    var bar = new Bar(); map.addControl(bar);
    var barEl = bar.getContainer(), titleEl = barEl.querySelector('.tools-bar-title'), bodyEl = barEl.querySelector('.tools-bar-body'), closeBtn = barEl.querySelector('.tools-x');
    var actsEl = barEl.querySelector('.tools-bar-actions');
    closeBtn.addEventListener('click', function () { stop(); });
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
      if (actsEl) actsEl.hidden = !Array.prototype.some.call(actsEl.querySelectorAll('button'), function (x) { return !x.hidden; });
    }
    // The bar never runs past the map's bottom edge (its body scrolls): G20 re-review, landscape phones.
    function fitBar() {
      if (barEl.hidden) return;
      var m = map.getContainer().getBoundingClientRect(), b = barEl.getBoundingClientRect();
      if (!m.height) return;
      barEl.style.maxHeight = Math.max(60, Math.floor(m.bottom - b.top - 8)) + 'px';
    }
    // The page's corner height, only when the bar's height changed; `grew` (the bar taller than it has been since the
    // tool started: not the dip to "Computing…" and back) lets the page minimise a window the bar now overlaps.
    function layout() {
      fitBar();
      var h = barEl.hidden ? 0 : (barEl.offsetHeight || 0);
      if (h === s.barH) return;
      var grew = h > s.barMax;
      if (grew) s.barMax = h;
      s.barH = h; if (opts.onLayout) opts.onLayout(barEl, grew);
    }

    function start(tool) {
      if (!TOOLS[tool]) return;
      clear(true);
      s.tool = tool; s.barMax = 0; s.barH = -1; barEl.hidden = false; titleEl.textContent = TOOLS[tool];   // a switch always reports
      Array.prototype.forEach.call(menu.querySelectorAll('[data-tool]'), function (b) { b.setAttribute('aria-pressed', b.getAttribute('data-tool') === tool ? 'true' : 'false'); });
      map.getContainer().classList.add('tools-active');
      if (s.dblWas === null) { s.dblWas = map.doubleClickZoom.enabled(); map.doubleClickZoom.disable(); }
      render();
      if (opts.onStart) opts.onStart(barEl, tool);                            // the page makes room (e.g. the overlay's details on a short map)
      try { closeBtn.focus(); } catch (e) { /* no focus */ }                   // keyboard focus stays in the tool (G20 a11y)
    }
    function stop() {
      clear(true); s.tool = null; barEl.hidden = true;
      Array.prototype.forEach.call(menu.querySelectorAll('[data-tool]'), function (b) { b.setAttribute('aria-pressed', 'false'); });
      map.getContainer().classList.remove('tools-active');
      if (s.dblWas) map.doubleClickZoom.enable(); s.dblWas = null;
      layout();
    }
    function clear(silent) {
      s.gen++; s.pts = []; s.closed = false; s.closedAt = null; s.result = null; s.selected = -1; s.busy = false; s.msg = '';
      s.group.clearLayers(); if (s.fan) { map.removeLayer(s.fan); s.fan = null; }
      if (!silent) render();
    }
    function undo() { if (s.pts.length && !s.closed) { s.pts.pop(); render(); } }
    function finish() { if ((s.tool === 'distance' && s.pts.length >= 2) || (s.tool === 'area' && s.pts.length >= 3)) { s.closed = true; render(); } }

    function click(latlng, ev) {
      if (!s.tool) return;
      var p = { lat: latlng.lat, lng: latlng.lng }, b = map.latLngToContainerPoint(latlng);
      if (s.tool === 'exposure') {
        if (s.result && s.fan && isTouch(ev)) {                         // a tap inside the fan picks a wedge; a mouse click places a new point
          var c = map.latLngToContainerPoint([s.result.origin.lat, s.fanLng]);
          var k = sectorAt(b.x - c.x, b.y - c.y, s.radius);
          if (k >= 0) { select(k); return; }
        }
        return exposureAt(p);
      }
      var twin = isTouch(ev) ? TWIN_TOUCH_PX : TWIN_MOUSE_PX;                 // a finger's double-tap lands 5-15 px apart (G20 re-review)
      if (s.closed) {
        if (s.closedAt && Math.abs(s.closedAt.x - b.x) + Math.abs(s.closedAt.y - b.y) < twin) return;   // the closing double-click's second click
        s.pts = []; s.closed = false; s.closedAt = null;
      }
      if (s.pts.length) {
        var last = s.pts[s.pts.length - 1], a = map.latLngToContainerPoint(last);
        if (Math.abs(a.x - b.x) + Math.abs(a.y - b.y) < twin) return;    // the second click of a double-click
        if (s.tool === 'area' && s.pts.length >= 3) {
          var f = map.latLngToContainerPoint(s.pts[0]);
          if (Math.abs(f.x - b.x) + Math.abs(f.y - b.y) < (isTouch(ev) ? 22 : 10)) { s.closed = true; s.closedAt = { x: b.x, y: b.y }; render(); return; }   // back on the first point
        }
        var lng = p.lng; while (lng - last.lng > 180) lng -= 360; while (lng - last.lng < -180) lng += 360; p.lng = lng;
      }
      s.pts.push(p); render();
    }

    function drawMeasure() {
      s.group.clearLayers();
      var pts = s.pts, u = unit(), line = [], verb = touchUI() ? 'Tap' : 'Click';
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
        lines.push({ t: pts.length < 2 ? verb + ' the map to add points.' : 'Total ' + fmtLength(pathKm(pts), u), cls: 'tools-big' });
        if (pts.length >= 2 && !s.closed) lines.push({ t: 'Double-' + verb.toLowerCase() + ' or Finish to end.', cls: 'tools-hint' });
      } else {
        if (pts.length < 3) lines.push({ t: verb + ' the map to outline an area (3+ points).', cls: 'tools-big' });
        else {
          lines.push({ t: 'Area ' + fmtArea(sphericalAreaKm2(pts), u), cls: 'tools-big' });
          lines.push({ t: 'Perimeter ' + fmtLength(pathKm(pts.concat([pts[0]])), u), cls: '' });
          if (selfIntersects(pts)) lines.push({ t: 'The outline crosses itself, so the area is not meaningful.', cls: 'tools-hint' });
          if (!s.closed) lines.push({ t: verb + ' the first point, double-' + verb.toLowerCase() + ' or Finish to close.', cls: 'tools-hint' });
        }
      }
      setBody(lines);
    }
    function drawExposure() {
      var u = unit(), lines = [], verb = touchUI() ? 'Tap' : 'Click';
      if (s.busy) lines.push({ t: 'Computing…', cls: 'tools-big' });
      else if (s.result) {
        lines.push({ t: windowsText(s.result.openWindows), cls: 'tools-big' });
        lines.push({ t: s.selected >= 0 ? sectorText(s.result.sectors[s.selected], u) : (touchUI() ? 'Tap a wedge for details; tap outside the fan to move the point.' : 'Point at a wedge for details.'), cls: 'tools-sector' });
        if (s.result.moved > 0.02) lines.push({ t: 'Moved ' + fmtDist(s.result.moved, u) + ' off the shore to open water.', cls: 'tools-hint' });
        if (s.result.coarse) lines.push({ t: 'Nearby coastline at lower detail.', cls: 'tools-hint' });
        lines.push({ t: 'Clear = swell reaches the spot from that direction; grey = land shadow (darker = more).', cls: 'tools-hint' });
        lines.push({ t: 'Coast geometry only: swell wraps headlands and islands, and reefs are not included.', cls: 'tools-hint' });
      } else lines.push({ t: s.msg || verb + ' the water to see which swell directions reach it.', cls: 'tools-big' });
      setBody(lines);
    }
    function render() {
      if (!s.tool) return;
      if (s.tool === 'exposure') drawExposure(); else drawMeasure();
      setActions();
      layout();
    }
    function defaultRadius() { var z = map.getSize(); return Math.min(z.x, z.y) < 576 ? 90 : 120; }
    function drawFan() {
      if (s.fan) { map.removeLayer(s.fan); s.fan = null; }
      if (!s.result) return;
      var f = fanSvg(s.result, s.radius, s.selected);
      var icon = L.divIcon({ className: 'tools-fan', html: f.svg, iconSize: [f.size, f.size], iconAnchor: [f.center, f.center] });
      s.fan = L.marker([s.result.origin.lat, s.fanLng], { icon: icon, interactive: false, keyboard: false, pane: 'toolsPane' }).addTo(map);
    }
    // Hover: restyle the one wedge (no rebuild), then the details line.
    function select(k) {
      if (s.selected === k) return;
      var el = s.fan && s.fan.getElement && s.fan.getElement();
      if (el && el.querySelector) {
        var prev = el.querySelector('path[data-k="' + s.selected + '"]'), next = el.querySelector('path[data-k="' + k + '"]');
        if (prev) prev.setAttribute('stroke', 'none');
        if (next) next.setAttribute('stroke', SELECT);
      }
      s.selected = k; render();
    }
    // The obstacles the fan should not sit under: the map's controls (the tool bar included, `hard`: the fan's centre
    // never goes under it) and whatever the page adds (the floating windows), as rects relative to the map.
    function obstacles() {
      var box = map.getContainer(), m = box.getBoundingClientRect(), out = [];
      function add(el) {
        if (!el || el.hidden) return;
        var r = el.getBoundingClientRect(); if (!r.width || !r.height) return;
        out.push({ l: r.left - m.left, t: r.top - m.top, r: r.right - m.left, b: r.bottom - m.top, hard: el === barEl });
      }
      Array.prototype.forEach.call(box.querySelectorAll('.leaflet-control, .ov-sheet'), add);   // the overlay's phone sheet too
      (opts.obstacles ? opts.obstacles() : []).forEach(add);
      return out;
    }
    // Put the fan where it is whole and unobstructed: pan the map to the nearest such spot (shrinking the fan when
    // nothing fits at full size, preferring a pan of at most a third of the map); run after a new result and after
    // the map changes size, but a fan the user has panned away from is only redrawn at the new size (G20 re-review).
    function placeCurrentFan() {
      if (!s.result || !s.fan) return;
      var size = map.getSize(), c = map.latLngToContainerPoint([s.result.origin.lat, s.fanLng]);
      s.size = size;
      var spot = placeFan(size.x, size.y, c.x, c.y, defaultRadius(), obstacles(), { maxPan: Math.max(size.x, size.y) / 3 });
      var r = spot ? spot.r : 50;
      if (r !== s.radius) { s.radius = r; drawFan(); }
      if (spot && (Math.abs(spot.x - c.x) > 0.5 || Math.abs(spot.y - c.y) > 0.5)) map.panBy([Math.round(c.x - spot.x), Math.round(c.y - spot.y)], { animate: true, duration: 0.3 });
    }
    function exposureAt(p) {
      var verb = touchUI() ? 'Tap' : 'Click';
      if (Math.abs(p.lat) > MAX_ABS_LAT) { clear(true); s.msg = 'Swell exposure works between 75°S and 75°N.'; render(); return; }
      var gen = ++s.gen, clickLng = p.lng, origin = { lat: p.lat, lng: wrapLng(p.lng) };
      s.group.clearLayers(); if (s.fan) { map.removeLayer(s.fan); s.fan = null; }
      s.result = null; s.selected = -1; s.busy = true; render();
      coast.load().then(function () { return coast.near(origin); }).then(function (nearSets) {
        // a frame first, so "Computing…" is painted before the placement's work (G20 re-checks R4, R5): a timer after the
        // next animation frame; a hidden tab runs no frames, so a 100 ms timer as well
        return new Promise(function (r) {
          var done = false; function go() { if (!done) { done = true; r(nearSets); } }
          if (typeof root.requestAnimationFrame === 'function') { root.requestAnimationFrame(function () { setTimeout(go, 0); }); setTimeout(go, 100); }
          else setTimeout(go, 0);
        });
      }).then(function (nearSets) {
        if (gen !== s.gen) return null;
        var coarse = nearSets === null, sets = coarse ? [coast.tier0] : nearSets;
        var o = placeOrigin(origin, sets);
        if (!o) { s.busy = false; s.msg = verb + ' the ocean to see swell exposure (lakes are not covered).'; render(); return null; }
        var startPt = { lat: o.lat, lng: o.lng };
        return computeExposure(startPt, sets, coast.tier0, {
          batch: 60, yieldFn: function () { return new Promise(function (r) { setTimeout(r, 0); }); },
          shouldStop: function () { return gen !== s.gen; }
        }).then(function (res) {
          if (!res || gen !== s.gen) return;
          res.moved = o.moved; res.coarse = coarse;
          s.result = res; s.busy = false; s.fanLng = clickLng + wrapLng(o.lng - origin.lng);
          s.radius = defaultRadius(); drawFan(); render(); placeCurrentFan();
        });
      }).catch(function () { if (gen !== s.gen) return; s.busy = false; s.msg = 'Coastline data unavailable. Try again later.'; render(); });
    }

    // A click that began on one of the map's controls or a floating window is not a map click, even when the control
    // re-rendered under it (G20 B P1-2: the overlay panel's details toggle), or when the press began there and was
    // released over the map (the browser then clicks their common parent, the map: G20 re-review).
    function onUi(ev) {
      if (!ev) return false;
      if (ev.target && ev.target.isConnected === false) return true;
      var path = ev.composedPath ? ev.composedPath() : [];
      for (var i = 0; i < path.length; i++) {
        var el = path[i];
        if (el && el.classList && (el.classList.contains('leaflet-control') || el.classList.contains('fwin') || el.classList.contains('ov-sheet'))) return true;
      }
      return false;
    }
    var pressUi = false;
    doc.addEventListener('pointerdown', function (e) { pressUi = onUi(e); }, true);
    map.on('click', function (e) { if (s.tool && !pressUi && !onUi(e.originalEvent)) click(e.latlng, e.originalEvent); });
    map.on('dblclick', function () { if (s.tool === 'distance' || s.tool === 'area') finish(); });
    // Hover straight from the map's element (the canvas renderer throttles Leaflet's mousemove).
    map.getContainer().addEventListener('mousemove', function (e) {
      if (s.tool !== 'exposure' || !s.result || !s.fan) return;
      var q = map.mouseEventToContainerPoint(e), c = map.latLngToContainerPoint([s.result.origin.lat, s.fanLng]);
      var k = sectorAt(q.x - c.x, q.y - c.y, s.radius);
      if (k >= 0) select(k);
    });
    // Size changes (rotation, window resize): Leaflet's own resize event does not always fire here.
    var rz = null;
    function onResize() {
      clearTimeout(rz);
      rz = setTimeout(function () {
        if (s.result && s.fan) {
          // Was the fan on the map BEFORE the resize? Leaflet keeps the map's centre, so the old point is the new one
          // shifted back by half the size change (G20 re-check: a rotation pushes a visible fan off the new map).
          var z = map.getSize(), o = s.size || z, c = map.latLngToContainerPoint([s.result.origin.lat, s.fanLng]);
          var ox = c.x - (z.x - o.x) / 2, oy = c.y - (z.y - o.y) / 2;
          if (ox >= 0 && oy >= 0 && ox <= o.x && oy <= o.y) placeCurrentFan();
          else if (s.radius !== defaultRadius()) { s.radius = defaultRadius(); drawFan(); }
          s.size = z;
        }
        layout();
      }, 150);
    }
    root.addEventListener('resize', onResize);
    root.addEventListener('orientationchange', onResize);
    if (typeof root.ResizeObserver === 'function') new root.ResizeObserver(onResize).observe(map.getContainer());
    // Escape and Backspace belong to the tool only while nothing else has focus: the body, the map and what is on it
    // outside the controls (a marker), the tool bar, the tools button and the gear with its panel closed (G20
    // re-review). The settings panel, the windows and the station search keep their own Escape (G20).
    function ownsKeys() {
      var a = doc.activeElement, box = map.getContainer();
      if (!a || a === doc.body || a === doc.documentElement || a === box || barEl.contains(a) || a === btn) return true;
      var gear = doc.getElementById('settingsBtn'), panel = doc.getElementById('settingsPanel');
      if (gear && a === gear) return !panel || panel.hidden;
      return box.contains(a) && !(a.closest && a.closest('.leaflet-control, .ov-sheet'));
    }
    doc.addEventListener('keydown', function (e) {
      if (!s.tool || open || !ownsKeys()) return;
      if (e.key === 'Escape') { e.stopPropagation(); if (s.pts.length || s.result) clear(); else stop(); }
      else if (e.key === 'Backspace' && (s.tool === 'distance' || s.tool === 'area') && !/INPUT|SELECT|TEXTAREA/.test((e.target && e.target.tagName) || '')) { e.preventDefault(); undo(); }
    }, true);
    if (opts.unitSelect) opts.unitSelect.addEventListener('change', function () { render(); });

    return { start: start, stop: stop, clear: clear, click: click, state: s };
  }

  var api = null;
  root.AllshoreTools = {
    init: function (opts) { api = init(opts); return api; },
    active: function () { return !!(state && state.tool); },
    click: function (latlng, ev) { if (api) api.click(latlng, ev); },
    _internals: {
      distanceKm: distanceKm, bearingDeg: bearingDeg, destination: destination, densify: densify, pathKm: pathKm,
      sphericalAreaKm2: sphericalAreaKm2, selfIntersects: selfIntersects, fmtLength: fmtLength, fmtArea: fmtArea, fmtDist: fmtDist,
      fmtReach: fmtReach, compass: compass, decodeCoastLL: decodeCoastLL, EdgeIndex: EdgeIndex, collectEdges: collectEdges,
      inLand: inLand, buildIndexes: buildIndexes, farWindow: farWindow, rayFetch: rayFetch, rayBearing: rayBearing,
      referenceKm: referenceKm, summarise: summarise, openWindows: openWindows, windowsText: windowsText, sectorText: sectorText,
      placeOrigin: placeOrigin, computeExposure: computeExposure, fanSvg: fanSvg, sectorAt: sectorAt, placeFan: placeFan, leastOverlap: leastOverlap,
      innerRadius: innerRadius, levelOf: levelOf, rayShadow: rayShadow, onCellLine: onCellLine, CoastSource: CoastSource, wrapLng: wrapLng,
      RAYS: RAYS, SECTORS: SECTORS, CAP_KM: CAP_KM, NEAR_KM: NEAR_KM, REF_MIN_KM: REF_MIN_KM, SHADOW_FULL_KM: SHADOW_FULL_KM,
      FAR_LAND_KM: FAR_LAND_KM, FAR_FADE_KM: FAR_FADE_KM, FAR_OPEN_MAX: FAR_OPEN_MAX, STANDOFF_KM: STANDOFF_KM, OPEN_BELOW: OPEN_BELOW, DARK_FROM: DARK_FROM
    }
  };
})(typeof window !== 'undefined' ? window : this);
