/* The site's Esri basemaps without "Map data not yet available" (plan section 39, step 5b).
 *
 * Esri's tile services answer a tile they do not have with a placeholder image ("Map data not yet available": one
 * 2,521-byte JPEG, the same everywhere): the imagery over the open ocean from about zoom 14 and at a few remote coasts
 * from zoom 17-18, the relief map (World_Hillshade) from zoom 15-17. The map zooms to 18 since section 39 step 5b
 * (owner, 2026-10-10: as far as the imagery is clear), so from FROM_ZOOM a tile is first looked up in the service's
 * tilemap (which tiles exist, WIN x WIN tiles per request, kept for the session): a tile Esri has loads as usual; a
 * missing one is drawn from the closest coarser tile Esri has, its part scaled up (open water stays open water). A
 * tilemap that cannot be read counts as "every tile exists" (the old behaviour: at worst the placeholder).
 *
 * window.AllshoreBasemap = { tileLayer(url, options), _internals }. Nothing touches the DOM at load (Node-testable).
 */
(function (root) {
  'use strict';

  var FROM_ZOOM = 13;        // tiles from this zoom are looked up (below it Esri has every tile, as far as measured)
  var WIN = 32;              // tiles per tilemap request side (Esri's tilemap answers up to 32 x 32 at once)
  var MAX_UP = 12;           // the furthest a missing tile looks for a coarser tile Esri has
  var WINDOWS_MAX = 512, IMAGES_MAX = 64;
  var JPEG_QUALITY = 0.92;

  // ---- pure helpers ----
  // the service's MapServer address for an Esri tile template ('.../MapServer/tile/{z}/{y}/{x}'), else null
  function tilemapBase(url) {
    var m = /^(https:\/\/[^?#\s]+\/MapServer)\/tile\/\{z\}\/\{y\}\/\{x\}$/.exec(url || '');
    return m ? m[1] : null;
  }
  function windowKey(z, x, y) { return z + '/' + (y - y % WIN) + '/' + (x - x % WIN); }
  function windowUrl(base, z, x, y) { return base + '/tilemap/' + windowKey(z, x, y) + '/' + WIN + '/' + WIN + '?f=json'; }
  // 1 or 0 for a tile inside the window the server answered (it may answer a smaller window), null outside it or for
  // an answer that is not a tilemap
  function bitAt(win, x, y) {
    var loc = win && win.location, data = win && win.data;
    if (!loc || !Array.isArray(data) || !(loc.width > 0) || !(loc.height > 0)) return null;
    if (x < loc.left || x >= loc.left + loc.width || y < loc.top || y >= loc.top + loc.height) return null;
    var v = data[(y - loc.top) * loc.width + (x - loc.left)];
    return v === undefined ? null : (v ? 1 : 0);
  }
  // the tile `up` levels coarser that holds (z, x, y), and the part of it that is this tile, in its own pixels
  function cropOf(z, x, y, up, size) {
    size = size || 256;
    var n = Math.pow(2, up), ax = Math.floor(x / n), ay = Math.floor(y / n), part = size / n;
    return { z: z - up, x: ax, y: ay, sx: (x - ax * n) * part, sy: (y - ay * n) * part, sw: part };
  }
  function lru(max) {
    var m = new Map();
    return {
      get: function (k) { if (!m.has(k)) return undefined; var v = m.get(k); m.delete(k); m.set(k, v); return v; },
      set: function (k, v) { m.set(k, v); while (m.size > max) m.delete(m.keys().next().value); },
      size: function () { return m.size; }
    };
  }

  // ---- which tiles a service has ----
  // has(z, x, y) -> Promise<boolean> (true below FROM_ZOOM and whenever the tilemap cannot tell); resolve(z, x, y) ->
  // Promise<up>: 0 when Esri has the tile (or none of its coarser tiles up to MAX_UP), else how many levels up the
  // closest tile it has is. One request per window, shared by every tile in it.
  function createAvailability(base, fetchJson) {
    var windows = lru(WINDOWS_MAX);
    function win(z, x, y) {
      var k = windowKey(z, x, y), p = windows.get(k);
      if (p === undefined) {
        p = Promise.resolve().then(function () { return fetchJson(windowUrl(base, z, x, y)); })
          .then(function (j) { return j && typeof j === 'object' && j.valid !== false ? j : null; }, function () { return null; });
        windows.set(k, p);
      }
      return p;
    }
    function has(z, x, y) {
      if (z < FROM_ZOOM) return Promise.resolve(true);
      return win(z, x, y).then(function (w) { var b = bitAt(w, x, y); return b === null ? true : b === 1; });
    }
    function resolve(z, x, y) {
      function step(up) {
        if (up > MAX_UP || z - up < 0) return Promise.resolve(0);           // none found: the tile itself (the placeholder)
        var c = cropOf(z, x, y, up);
        return has(c.z, c.x, c.y).then(function (ok) { return ok ? up : step(up + 1); });
      }
      return step(0);
    }
    return { has: has, resolve: resolve, windows: windows };
  }

  // ---- drawing a coarser tile's part as this tile ----
  function drawCrop(doc, img, c, size) {
    var canvas = doc.createElement('canvas');
    canvas.width = size.x; canvas.height = size.y;
    var ctx = canvas.getContext('2d');
    ctx.imageSmoothingEnabled = true;
    if ('imageSmoothingQuality' in ctx) ctx.imageSmoothingQuality = 'high';
    ctx.drawImage(img, c.sx, c.sy, c.sw, c.sw, 0, 0, size.x, size.y);
    return canvas.toDataURL('image/jpeg', JPEG_QUALITY);
  }
  function defaultFetchJson(url) {
    return root.fetch(url, { credentials: 'omit' }).then(function (r) { return r.ok ? r.json() : null; });
  }
  function defaultLoadImage(url) {
    return new Promise(function (res, rej) {
      var img = new root.Image();
      img.crossOrigin = 'anonymous';                                        // Esri sends Access-Control-Allow-Origin: *
      img.onload = function () { res(img); };
      img.onerror = function () { rej(new Error('tile did not load')); };
      img.src = url;
    });
  }

  // ---- the tile layer ----
  // deps (tests): fetchJson(url) -> Promise<json>, loadImage(url) -> Promise<img>, document
  function layerClass(L, deps) {
    deps = deps || {};
    var Base = L.TileLayer;
    return Base.extend({
      initialize: function (url, options) {
        Base.prototype.initialize.call(this, url, options);
        var base = tilemapBase(url);
        this._avail = base && (deps.fetchJson || root.fetch) ? createAvailability(base, deps.fetchJson || defaultFetchJson) : null;
        this._images = lru(IMAGES_MAX);
      },
      _urlFor: function (z, x, y) {
        return L.Util.template(this._url, L.Util.extend({ r: '', s: '' }, this.options, { x: x, y: y, z: z }));
      },
      _image: function (url) {
        var p = this._images.get(url);
        if (p === undefined) {
          var self = this;
          p = (deps.loadImage || defaultLoadImage)(url);
          p.catch(function () { if (self._images.get(url) === p) self._images.set(url, undefined); });   // asked again next time
          this._images.set(url, p);
        }
        return p;
      },
      createTile: function (coords, done) {
        if (!this._avail || coords.z < FROM_ZOOM) return Base.prototype.createTile.call(this, coords, done);
        var doc = deps.document || root.document, tile = doc.createElement('img'), self = this;
        L.DomEvent.on(tile, 'load', L.Util.bind(this._tileOnLoad, this, done, tile));
        L.DomEvent.on(tile, 'error', L.Util.bind(this._tileOnError, this, done, tile));
        tile.alt = '';
        tile.setAttribute('role', 'presentation');
        var live = function () { return !!(self._map && tile.parentNode); };     // not removed meanwhile (a zoom, a pan, Off)
        var own = function () { if (live()) tile.src = self.getTileUrl(coords); };
        this._avail.resolve(coords.z, coords.x, coords.y).then(function (up) {
          if (!live()) return;
          if (!up) { own(); return; }
          var size = self.getTileSize(), c = cropOf(coords.z, coords.x, coords.y, up, size.x);
          return self._image(self._urlFor(c.z, c.x, c.y)).then(function (img) {
            if (live()) tile.src = drawCrop(doc, img, c, size);
          }, own);                                                              // the coarser tile failed: the tile itself
        }).catch(own);
        return tile;
      }
    });
  }
  var made = null;
  function tileLayer(url, options) {
    if (!made) made = layerClass(root.L);
    return new made(url, options);
  }

  root.AllshoreBasemap = {
    tileLayer: tileLayer,
    _internals: { FROM_ZOOM: FROM_ZOOM, WIN: WIN, MAX_UP: MAX_UP, WINDOWS_MAX: WINDOWS_MAX, IMAGES_MAX: IMAGES_MAX, tilemapBase: tilemapBase,
                  windowKey: windowKey, windowUrl: windowUrl, bitAt: bitAt, cropOf: cropOf, lru: lru, createAvailability: createAvailability,
                  drawCrop: drawCrop, layerClass: layerClass }
  };
})(typeof window !== 'undefined' ? window : this);
