/* Latitude / longitude gridlines for the map (plan section 26).
 *
 * Lines: an L.GridLayer drawing meridians and parallels on 256-px canvas tiles in its own pane (z 350: above the
 * model overlay and its particles, below the forecast markers), so the world copies and the dateline come for
 * free and nothing is rebuilt while panning. Labels: a DOM strip along the map's top edge (longitudes) and right
 * edge (latitudes), repositioned on every move, hidden where they would sit under a map control.
 * The interval follows the tile zoom: the smallest of STEPS whose on-screen spacing is at least MIN_PX.
 * The setting lives in localStorage 'allshore.gridlines.v1' ('0' = off; anything else, or nothing, = on).
 *
 * window.AllshoreGraticule = { create(map, opts), _internals }. Nothing touches the DOM at load (Node-testable).
 */
(function (root) {
  'use strict';

  var KEY = 'allshore.gridlines.v1';
  var STEPS = [60, 30, 15, 10, 5, 2, 1, 0.5, 0.25];   // every one divides 360 (meridians repeat exactly per world)
  var MIN_PX = 70;                                   // the closest two lines may be on screen at the tile zoom
  var TILE = 256;
  var LAT_MAX = 85.05112878;                         // Web Mercator's edge

  // ---- pure helpers ----
  function worldSize(z) { return TILE * Math.pow(2, z); }
  function stepFor(z) {
    var size = worldSize(z);
    for (var i = STEPS.length - 1; i >= 0; i--) if (STEPS[i] / 360 * size >= MIN_PX) return STEPS[i];   // the finest that is not crowded
    return STEPS[0];
  }
  // latitude -> world pixel y at zoom z (spherical Mercator, as Leaflet's EPSG:3857)
  function latToY(lat, z) {
    var s = Math.sin(Math.max(-LAT_MAX, Math.min(LAT_MAX, lat)) * Math.PI / 180);
    return worldSize(z) * (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI));
  }
  function yToLat(y, z) { var n = Math.PI - 2 * Math.PI * y / worldSize(z); return 180 / Math.PI * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n))); }
  function lonToX(lon, z) { return (lon + 180) / 360 * worldSize(z); }
  function xToLon(x, z) { return x / worldSize(z) * 360 - 180; }
  // round away the floating noise of k * step (0.25 steps)
  function tidy(v) { return Math.round(v * 1e6) / 1e6; }
  // The lines crossing a tile (coords in Leaflet's UNWRAPPED tile grid), as tile-local pixel offsets. A margin
  // brings in the neighbours' lines too, so their halos run across the seam and adjacent tiles join exactly.
  function tileLines(coords, step, margin) {
    var z = coords.z, m = margin || 0, x0 = coords.x * TILE, y0 = coords.y * TILE, out = { xs: [], ys: [] };
    var lonW = xToLon(x0 - m, z), lonE = xToLon(x0 + TILE + m, z);
    for (var k = Math.ceil(lonW / step); k * step <= lonE; k++) {
      var lon = tidy(k * step);
      out.xs.push({ px: lonToX(lon, z) - x0, lon: lon, major: ((lon % 360) + 360) % 360 === 0 });
    }
    var latN = yToLat(Math.max(0, y0 - m), z), latS = yToLat(Math.min(worldSize(z), y0 + TILE + m), z);
    for (var j = Math.ceil(latS / step); j * step <= latN; j++) {
      var lat = tidy(j * step);
      if (Math.abs(lat) >= LAT_MAX) continue;
      out.ys.push({ px: latToY(lat, z) - y0, lat: lat, major: lat === 0 });
    }
    return out;
  }
  function fmt(v) { return String(+Math.abs(v).toFixed(2)); }
  function lonLabel(lon) {
    var l = ((tidy(lon) + 540) % 360 + 360) % 360 - 180;       // -> [-180, 180)
    l = tidy(l);
    if (l === 0) return '0°';
    if (l === -180) return '180°';
    return fmt(l) + '°' + (l < 0 ? 'W' : 'E');
  }
  function latLabel(lat) { lat = tidy(lat); return lat === 0 ? '0°' : fmt(lat) + '°' + (lat < 0 ? 'S' : 'N'); }
  function readOn(storage) { try { return storage.getItem(KEY) !== '0'; } catch (e) { return true; } }
  function writeOn(storage, on) { try { storage.setItem(KEY, on ? '1' : '0'); } catch (e) {} }
  function overlaps(a, b) { return a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top; }
  // The labels for a view: map-like {getZoom, getSize, getBounds, latLngToContainerPoint}; step from the TILE zoom.
  // Longitudes along the top edge (unwrapped, so every world copy on screen is labelled), latitudes along the right.
  function labelsFor(map, step) {
    var b = map.getBounds(), size = map.getSize(), out = [];
    var west = b.getWest(), east = b.getEast(), north = Math.min(b.getNorth(), LAT_MAX), south = Math.max(b.getSouth(), -LAT_MAX);
    var cLat = (north + south) / 2, cLng = (west + east) / 2;
    for (var k = Math.ceil(west / step); k * step <= east; k++) {
      var lon = tidy(k * step), px = map.latLngToContainerPoint([cLat, lon]).x;
      if (px >= 0 && px <= size.x) out.push({ kind: 'lon', text: lonLabel(lon), x: px, y: 0 });
    }
    for (var j = Math.ceil(south / step); j * step <= north; j++) {
      var lat = tidy(j * step), py = map.latLngToContainerPoint([lat, cLng]).y;
      if (py >= 0 && py <= size.y) out.push({ kind: 'lat', text: latLabel(lat), x: size.x, y: py });
    }
    return out;
  }

  // ---- the layer and the labels ----
  // opts: storage (localStorage-like), L (defaults to window.L), document; returns { on(), set(on), layer, update() }
  function create(map, opts) {
    opts = opts || {};
    var L = opts.L || root.L, doc = opts.document || root.document, storage = opts.storage || { getItem: function () { return null; }, setItem: function () {} };
    if (!L || !map || !doc) return null;
    var pane = map.getPane('graticulePane');
    if (!pane) { pane = map.createPane('graticulePane'); pane.style.zIndex = 350; pane.style.pointerEvents = 'none'; }
    var Layer = L.GridLayer.extend({
      createTile: function (coords) {
        var c = doc.createElement('canvas'), dpr = Math.min(2, root.devicePixelRatio || 1);
        c.width = TILE * dpr; c.height = TILE * dpr; c.style.width = TILE + 'px'; c.style.height = TILE + 'px';
        draw(c.getContext('2d'), coords, dpr);
        return c;
      }
    });
    function draw(ctx, coords, dpr) {
      if (!ctx) return;
      var lines = tileLines(coords, stepFor(coords.z), 3);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      function path(list, vertical, pick) {
        ctx.beginPath();
        list.forEach(function (l) {
          if (!pick(l)) return;
          var p = Math.floor(l.px) + 0.5;                       // the pixel the line falls in (crisp 1-px core)
          if (vertical) { ctx.moveTo(p, 0); ctx.lineTo(p, TILE); } else { ctx.moveTo(0, p); ctx.lineTo(TILE, p); }
        });
        ctx.stroke();
      }
      function all(minor) { return function (l) { return minor ? !l.major : l.major; }; }
      // a dark halo under a light core: legible on the imagery, the relief and the colour fields alike
      ctx.lineWidth = 3; ctx.strokeStyle = 'rgba(0,0,0,0.22)';
      path(lines.xs, true, function () { return true; }); path(lines.ys, false, function () { return true; });
      ctx.lineWidth = 1; ctx.strokeStyle = 'rgba(255,255,255,0.45)';
      path(lines.xs, true, all(true)); path(lines.ys, false, all(true));
      ctx.strokeStyle = 'rgba(255,255,255,0.8)';                  // the equator and the prime meridian
      path(lines.xs, true, all(false)); path(lines.ys, false, all(false));
    }
    var layer = new Layer({ pane: 'graticulePane', tileSize: TILE, updateWhenZooming: false, keepBuffer: 1, noWrap: false });

    // labels: one absolutely positioned strip over the map (below the controls, above the panes)
    var box = doc.createElement('div');
    box.className = 'graticule-labels';
    box.setAttribute('aria-hidden', 'true');
    map.getContainer().appendChild(box);
    var pool = [];
    function controlRects() {
      var c = map.getContainer(), base = c.getBoundingClientRect();
      return Array.prototype.map.call(c.querySelectorAll('.leaflet-control'), function (el) {
        var r = el.getBoundingClientRect();
        return { left: r.left - base.left - 4, right: r.right - base.left + 4, top: r.top - base.top - 4, bottom: r.bottom - base.top + 4 };
      }).filter(function (r) { return r.right > r.left && r.bottom > r.top; });
    }
    function update() {
      if (!on) { box.hidden = true; return; }
      box.hidden = false;
      var z = Math.round(map.getZoom()), list = labelsFor(map, stepFor(z)), rects = controlRects(), used = 0;
      list.forEach(function (lb) {
        var el = pool[used] || (pool[used] = box.appendChild(doc.createElement('span')));
        el.className = 'graticule-label graticule-' + lb.kind;
        el.textContent = lb.text;
        // top-edge longitudes centred on their line; right-edge latitudes centred on theirs
        var w = el.offsetWidth || lb.text.length * 7, h = el.offsetHeight || 14;
        var left = lb.kind === 'lon' ? lb.x - w / 2 : lb.x - w - 4, top = lb.kind === 'lon' ? 4 : lb.y - h / 2;
        var r = { left: left, right: left + w, top: top, bottom: top + h };
        var hidden = rects.some(function (c) { return overlaps(r, c); }) || (lb.kind === 'lat' && top < 22);   // under a control / the top row
        el.style.left = Math.round(left) + 'px'; el.style.top = Math.round(top) + 'px';
        el.hidden = hidden;
        used++;
      });
      for (var i = used; i < pool.length; i++) pool[i].hidden = true;
    }
    var on = readOn(storage), frame = null;
    function schedule() { if (frame !== null) return; frame = (root.requestAnimationFrame || function (f) { return setTimeout(f, 16); })(function () { frame = null; update(); }); }
    map.on('move resize viewreset zoomend', schedule);
    map.on('zoomstart', function () { box.hidden = true; });
    function set(v) {
      on = !!v; writeOn(storage, on);
      if (on && !map.hasLayer(layer)) layer.addTo(map);
      if (!on && map.hasLayer(layer)) map.removeLayer(layer);
      update();
    }
    if (on) layer.addTo(map);
    schedule();
    return { on: function () { return on; }, set: set, layer: layer, update: update, labels: box };
  }

  root.AllshoreGraticule = {
    create: create,
    _internals: { KEY: KEY, STEPS: STEPS, MIN_PX: MIN_PX, stepFor: stepFor, latToY: latToY, yToLat: yToLat, lonToX: lonToX, xToLon: xToLon,
                  tileLines: tileLines, lonLabel: lonLabel, latLabel: latLabel, labelsFor: labelsFor, readOn: readOn, writeOn: writeOn, overlaps: overlaps }
  };
})(typeof window !== 'undefined' ? window : this);
