/* Country and state/province borders and city names on the map (plan section 51).
 *
 * Data: static/places/v2 on the frames bucket (tools/places/build_places.py): index.json, tier 0 (world-lines.bin and
 * world-places.bin, for tile zoom 7 and below) and tier 1 (10-degree cells c/<lat>_<lon>.l.bin / .p.bin, from tile zoom 8).
 * Lines: an L.GridLayer of 256-px canvas tiles in its own pane (z 340: above the model overlay and its particles, below
 * the gridlines and the forecast markers), a dark halo under a light core, so they read on the imagery and the wind's
 * relief alike; world copies come with the grid layer. Kinds: 0 settled country border, 2 disputed / indefinite / line of
 * control (dashed), 1 state or province line (thinner, dashed). A country line shows from tile zoom 3, a state line from
 * zoom 5, and each line from its own Natural Earth zoom too.
 * Names: text spans in a second pane (z 620: above the station icons of the marker pane, below the tooltips; a name
 * takes no tap, so an icon under it stays clickable; owner, 2026-10-23), placed at the map's layer points (they ride
 * along with a pan) and rebuilt after every move or zoom and when a map control or the overlay's sheet changes size:
 * every place whose own zoom is reached (Natural Earth places at Natural Earth's min_zoom; GeoNames towns from zoom 8, 9
 * or 10 by size, set by the builder), the largest first, each label kept only where its dot and its whole text lie on
 * the map (to the left of the dot when the right runs off) and it overlaps neither another label nor a map control, the
 * overlay's sheet or a gridline label; at most 120 labels (60 on a narrow map).
 * Nothing is fetched before the page has loaded and the overlay's first picture has settled (at most 30 s: on a slow
 * link the overlay's coastline comes first, G34 B-6), nor while every kind is off or the map is below the first wanted
 * zoom; every download asks for a low priority. A download is given up only after 15 s in which no byte arrived. The
 * lines and the places of tier 0 load as two units: one failing keeps the other (G34 B-5).
 * Names keep clear of the map's controls and their open panels, the overlay's sheet, the gridline labels, the map tools'
 * fan and labels, and the floating windows (the forecast, live-buoy, tide and wind windows, their bars and chips), and
 * are laid out again when one of these opens, closes, moves or changes size (G34 B-1, B-2). Widths are measured in the
 * font the names are drawn in (a browser's minimum font size included: G34 B-8).
 * Settings (localStorage, '0' = off; anything else, or nothing, = on): 'allshore.borders.v1' (country and state lines
 * together, one "Borders" setting: owner, 2026-10-23), 'allshore.places.v1'. Credits join the map's attribution while a
 * kind is on and its data has loaded.
 *
 * window.AllshorePlaces = { create(map, opts), _internals }. Nothing touches the DOM at load (Node-testable).
 */
(function (root) {
  'use strict';

  var TILE = 256;
  var LAT_MAX = 85.05112878;                         // Web Mercator's edge
  var KEYS = { borders: 'allshore.borders.v1', places: 'allshore.places.v1' };
  var KIND_ADMIN0 = 0, KIND_ADMIN1 = 1, KIND_DISPUTED = 2;
  var BORDER_ZOOM = 3, STATE_ZOOM = 5, PLACES_ZOOM = 2;   // tile zoom from which each kind is drawn (owner, 2026-10-23)
  var TIER0_MAX_ZOOM = 7;                            // tile zoom up to which the world files serve (tier 1 from 8)
  var LINES_MAGIC = 'LNS1', LINES_HEADER = 40, PLACES_MAGIC = 'PLC1', PLACES_HEADER = 36;
  var FLAG_CAPITAL = 1, FLAG_GEONAMES = 2;
  var MAX_INDEX_BYTES = 1 << 20, MAX_TIER0_BYTES = 4 << 20, MAX_CELL_BYTES = 2 << 20;
  var MAX_CELL_BYTES_KEPT = 6 << 20;                 // decoded cells kept, by their decoded size (a cell the view uses stays)
  var MAX_INFLIGHT = 2;
  var STALL_MS = 15000, RETRY_MS = 60000, START_DELAY_MS = 1500;   // STALL_MS: no byte for this long gives a download up
  var WAIT_MAX_MS = 120000;                          // the longest the first download waits for the overlay's first picture and its
                                                     // coastline (G34 B-6; 120 s: owner, re-check R-1)
  var WIDTH_CACHE_MAX = 8000;                        // measured name widths kept (the cache starts again beyond)
  // the lines' look: [halo width, halo colour, core width, core colour, dash] per kind (CSS px)
  var STYLE = {};
  STYLE[KIND_ADMIN0] = { halo: 2.5, haloInk: 'rgba(0,0,0,0.35)', core: 1, coreInk: 'rgba(255,255,255,0.55)', dash: null };
  STYLE[KIND_DISPUTED] = { halo: 2.5, haloInk: 'rgba(0,0,0,0.35)', core: 1, coreInk: 'rgba(255,255,255,0.55)', dash: [6, 4] };
  STYLE[KIND_ADMIN1] = { halo: 2, haloInk: 'rgba(0,0,0,0.3)', core: 1, coreInk: 'rgba(255,255,255,0.45)', dash: [4, 3] };
  var DRAW_ORDER = [KIND_ADMIN1, KIND_DISPUTED, KIND_ADMIN0];   // countries over states
  // the labels' look (the page's .place-label CSS must match LABEL_FONT: the widths are measured with it)
  var LABEL_FONT = '600 11px system-ui, -apple-system, "Segoe UI", sans-serif', CAPITAL_FONT = '700 11px system-ui, -apple-system, "Segoe UI", sans-serif';
  var LABEL_H = 14, LABEL_GAP = 4, DOT_R = 3, LABEL_DX = 6;   // text height; gap between boxes; the dot's reach; dot -> text
  var LABEL_MAX = 120, LABEL_MAX_NARROW = 60, NARROW_PX = 600, VIEW_MARGIN_PX = 40, BUCKET = 32;
  var LABEL_PANE_Z = 620;                            // above the marker pane (600), below the tooltips (650)
  var BLOCKED_SELECTOR = '.leaflet-control, .settings-panel, .ov-sheet, .graticule-label, .tools-fan, .tools-label, .tools-ring-label';
  var FLOAT_SELECTOR = '.fwin';                      // the floating windows (outside the map's container; the hidden ones skipped)
  var RELAY_CLASSES = ['ov-sheet', 'tools-fan', 'tools-label', 'tools-ring-label'];   // their coming and going lays the names out again
  var CHAR_PX = 6.2;                                 // the width estimate without a canvas
  var CREDIT_BORDERS = 'Borders: <a href="https://www.naturalearthdata.com/" target="_blank" rel="noopener" title="Made with Natural Earth (public domain)">Natural Earth</a>';
  var CREDIT_PLACES = 'Places: <a href="https://www.geonames.org/" target="_blank" rel="noopener" title="Town names from GeoNames (and Natural Earth), modified: filtered and ranked">GeoNames</a> <a href="https://creativecommons.org/licenses/by/4.0/" target="_blank" rel="noopener">CC BY 4.0</a>';

  // ---- projection (zoom-0 world pixels, as Leaflet's EPSG:3857 at zoom 0) ----
  function lonToX(lon) { return (lon + 180) / 360 * TILE; }
  function latToY(lat) {
    var s = Math.sin(Math.max(-LAT_MAX, Math.min(LAT_MAX, lat)) * Math.PI / 180);
    return TILE * (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI));
  }
  function yToLat(y) { var n = Math.PI - 2 * Math.PI * y / TILE; return 180 / Math.PI * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n))); }

  // ---- lines-v1 (tools/places/build_places.py; little-endian header, then one LEB128 stream) ----
  // -> {cell, kinds, q, n (pieces), kind Uint8Array(n), box Float64Array(4n) [x0, y0, x1, y1] (zoom-0 px, y0 north),
  //     lineStart Int32Array(n + 1), vertStart Int32Array(lines + 1), mz Uint8Array(lines), xy Float64Array(2 vertices)}.
  // Throws on anything that does not add up (a damaged or foreign file never draws).
  function readVarints(u8, start, end, out) {
    var n = 0, v = 0, mul = 1, len = 0;
    for (var i = start; i < end; i++) {
      var b = u8[i];
      v += (b & 0x7f) * mul; mul *= 128; len++;
      if (len > 5) throw new Error('places: varint longer than 5 bytes');
      if (b < 0x80) { out[n++] = v; v = 0; mul = 1; len = 0; }
    }
    if (len) throw new Error('places: stream ends inside a varint');
    return n;
  }
  function unzz(v) { return v % 2 ? -(v + 1) / 2 : v / 2; }
  function magicOf(u8) { return String.fromCharCode(u8[0], u8[1], u8[2], u8[3]); }
  function decodeLines(buf) {
    if (!buf || buf.byteLength < LINES_HEADER) throw new Error('lines-v1: file too short');
    var dv = new DataView(buf), u8 = new Uint8Array(buf);
    if (magicOf(u8) !== LINES_MAGIC) throw new Error('lines-v1: not a lines file');
    var cell = dv.getUint16(4, true), kinds = dv.getUint16(6, true), q = dv.getUint32(8, true);
    var np = dv.getUint32(12, true), nl = dv.getUint32(16, true), nv = dv.getUint32(20, true);
    if (!(q === 1000 || q === 10000) || !(cell >= 1 && cell <= 90) || nv > 5e6 || nl > nv || np > nl) throw new Error('lines-v1: bad header');
    var vals = new Float64Array(buf.byteLength - LINES_HEADER), count = readVarints(u8, LINES_HEADER, u8.length, vals), i = 0;
    // the stream must hold exactly what the header says (6 values a piece, 2 a line, 2 a vertex) before anything is sized
    // by those counts: a hostile header never makes the decoder allocate (G34 F-A1)
    if (count !== 6 * np + 2 * nl + 2 * nv) throw new Error('lines-v1: counts do not match the stream');
    var kind = new Uint8Array(np), box = new Float64Array(4 * np), lineStart = new Int32Array(np + 1), vertStart = new Int32Array(nl + 1);
    var mz = new Uint8Array(nl), xy = new Float64Array(2 * nv), li = 0, vi = 0;
    function take() { if (i >= count) throw new Error('lines-v1: stream shorter than its counts'); return vals[i++]; }
    for (var p = 0; p < np; p++) {
      var minx = unzz(take()), miny = unzz(take()), w = take(), h = take(), k = take(), lines = take();
      if (k !== KIND_ADMIN0 && k !== KIND_ADMIN1 && k !== KIND_DISPUTED) throw new Error('lines-v1: unknown kind ' + k);
      if (minx < -180 * q || minx + w > 180 * q || miny < -90 * q || miny + h > 90 * q) throw new Error('lines-v1: out of range');
      kind[p] = k; lineStart[p] = li;
      box[4 * p] = lonToX(minx / q); box[4 * p + 2] = lonToX((minx + w) / q);
      box[4 * p + 1] = latToY((miny + h) / q); box[4 * p + 3] = latToY(miny / q);
      var px = minx, py = miny;
      for (var j = 0; j < lines; j++) {
        if (li >= nl) throw new Error('lines-v1: more lines than the header says');
        var n = take(), z = take();
        if (n < 2 || vi + n > nv || z > 255) throw new Error('lines-v1: bad line');
        vertStart[li] = vi; mz[li] = z;
        for (var t = 0; t < n; t++) {
          px += unzz(take()); py += unzz(take());
          xy[2 * vi] = lonToX(px / q); xy[2 * vi + 1] = latToY(py / q); vi++;
        }
        li++;
      }
    }
    lineStart[np] = li; vertStart[li] = vi;
    if (i !== count || li !== nl || vi !== nv) throw new Error('lines-v1: counts do not match the stream');
    return { cell: cell, kinds: kinds, q: q, n: np, kind: kind, box: box, lineStart: lineStart, vertStart: vertStart, mz: mz, xy: xy };
  }
  // ---- places-v1: header 36 B (magic, u16 cell, u16 0, u32 q, u32 count, u32 stream bytes, i32 bbox[4]), a LEB128 stream
  // of count x (zz(lat) zz(lon) rank mz10 pop flags), then the names, UTF-8, joined by "\n" (in record order).
  // -> {cell, q, n, lat Float64Array (degrees), lon Float64Array (degrees, -180..180), rank Uint8Array, mz Uint8Array,
  //     pop Float64Array, flags Uint8Array, name [string]}. Throws on anything that does not add up.
  function decodePlaces(buf) {
    if (!buf || buf.byteLength < PLACES_HEADER) throw new Error('places-v1: file too short');
    var dv = new DataView(buf), u8 = new Uint8Array(buf);
    if (magicOf(u8) !== PLACES_MAGIC) throw new Error('places-v1: not a places file');
    var cell = dv.getUint16(4, true), q = dv.getUint32(8, true), count = dv.getUint32(12, true), nstream = dv.getUint32(16, true);
    if (!(q === 1000 || q === 10000) || !(cell >= 1 && cell <= 90) || count > 1e6 || PLACES_HEADER + nstream > buf.byteLength) throw new Error('places-v1: bad header');
    var vals = new Float64Array(nstream), n = readVarints(u8, PLACES_HEADER, PLACES_HEADER + nstream, vals);
    if (n !== 6 * count) throw new Error('places-v1: counts do not match the stream');
    var text;
    try { text = new TextDecoder('utf-8', { fatal: true }).decode(u8.subarray(PLACES_HEADER + nstream)); }
    catch (e) { throw new Error('places-v1: names are not UTF-8'); }    // a damaged file is final, never asked again (re-check R-5)
    var names = count ? text.split('\n') : (text === '' ? [] : null);
    if (!names || names.length !== count) throw new Error('places-v1: names do not match the count');
    var lat = new Float64Array(count), lon = new Float64Array(count), rank = new Uint8Array(count), mz = new Uint8Array(count), pop = new Float64Array(count), flags = new Uint8Array(count);
    for (var k = 0; k < count; k++) {
      var la = unzz(vals[6 * k]) / q, lo = unzz(vals[6 * k + 1]) / q;
      if (!(la >= -90 && la <= 90 && lo >= -180 && lo < 180) || vals[6 * k + 2] > 255 || vals[6 * k + 3] > 255 || vals[6 * k + 5] > 255 || !names[k]) throw new Error('places-v1: bad place');
      lat[k] = la; lon[k] = lo; rank[k] = vals[6 * k + 2]; mz[k] = vals[6 * k + 3]; pop[k] = vals[6 * k + 4]; flags[k] = vals[6 * k + 5];
    }
    return { cell: cell, q: q, n: count, lat: lat, lon: lon, rank: rank, mz: mz, pop: pop, flags: flags, name: names };
  }

  // ---- the index ----
  function validIndex(idx) {
    if (!idx || idx.format !== 'places-v1' || !idx.tier0 || !idx.tier1) return false;
    var t0 = idx.tier0, t1 = idx.tier1, fileOk = function (f) { return typeof f === 'string' && /^[A-Za-z0-9_.-]+\.bin$/.test(f); };
    return !!(t0.lines && fileOk(t0.lines.file) && t0.places && fileOk(t0.places.file) &&
      t1.cell === 10 && typeof t1.dir === 'string' && /^[A-Za-z0-9_-]+$/.test(t1.dir) &&
      t1.cells && typeof t1.cells === 'object' && !Array.isArray(t1.cells));
  }
  function cellName(lat0, lon0) { return lat0 + '_' + lon0; }
  // The cells meeting a box of degrees (west..east unwrapped, south..north): south-west corners, longitudes wrapped.
  function cellsForBox(west, south, east, north, cell) {
    var out = {};
    for (var la = Math.floor(south / cell) * cell; la < north; la += cell) {
      if (la < -90 || la >= 90) continue;
      for (var lo = Math.floor(west / cell) * cell; lo < east; lo += cell) out[cellName(la, ((lo + 180) % 360 + 360) % 360 - 180)] = true;
    }
    return Object.keys(out);
  }
  // The 10-degree cells a tile touches (coords in Leaflet's unwrapped tile grid: x wrapped onto the world).
  function cellsForTile(coords, cell) {
    var n = Math.pow(2, coords.z), xw = ((coords.x % n) + n) % n, size = TILE * n;
    var lonW = xw / n * 360 - 180, lonE = (xw + 1) / n * 360 - 180;
    var latN = yToLat(coords.y * TILE * TILE / size), latS = yToLat((coords.y + 1) * TILE * TILE / size);
    return cellsForBox(lonW, latS, lonE, latN, cell);
  }

  // A response body read chunk by chunk (each chunk reports progress), as one ArrayBuffer, capped at `limit`.
  function readAll(reader, limit, onProgress) {
    var parts = [], size = 0;
    function next() {
      return reader.read().then(function (c) {
        if (c.done) {
          var out = new Uint8Array(size), at = 0;
          for (var i = 0; i < parts.length; i++) { out.set(parts[i], at); at += parts[i].length; }
          return out.buffer;
        }
        size += c.value.length;
        if (size > limit) {
          try { var p = reader.cancel(); if (p && p.catch) p.catch(function () {}); } catch (e) { /* gone */ }
          throw new Error('places: file too large');
        }
        parts.push(c.value); onProgress();
        return next();
      });
    }
    return next();
  }
  // The byte count the index gives a file (a positive whole number), else 0 (not compared).
  function expectOf(n) { return typeof n === 'number' && n > 0 && Math.floor(n) === n ? n : 0; }
  // A decoded set's size in memory: its typed arrays, and a place's name as UTF-16 plus a string's overhead.
  function decodedBytes(d) {
    var n = 0;
    for (var k in d) if (d[k] && typeof d[k].byteLength === 'number') n += d[k].byteLength;
    if (d.name) for (var i = 0; i < d.name.length; i++) n += 2 * d.name[i].length + 32;
    return n;
  }

  // ---- the store: the index, the two tier-0 files (two units: lines 'l', places 'p'), the cells on demand (keys
  // "<name>.l" / "<name>.p"). status: the index ('idle', 'loading', 'ok', 'failed'); unit[k].status: a tier-0 file. ----
  function PlacesStore(base, fetchFn) {
    this.base = base; this.fetch = fetchFn || (root.fetch && root.fetch.bind(root));
    this.status = 'idle'; this.index = null; this.lines0 = null; this.places0 = null; this.loading = null; this.failedAt = 0;
    this.unit = { l: { status: 'idle', failedAt: 0 }, p: { status: 'idle', failedAt: 0 } };
    this.cells = new Map(); this.bytes = 0; this.inflight = {}; this.failed = {}; this.queue = []; this.view = 0;
    this.rev = 0; this.onChange = null; this.onUnit = null; this.abort = null; this.retryTimer = null;
  }
  // A kind's tier 0 is usable.
  PlacesStore.prototype.ok = function (kind) { return this.status === 'ok' && !!this.unit[kind] && this.unit[kind].status === 'ok'; };
  // A new view begins (the map moves): cells used from now on are the view's, never evicted while it lasts.
  PlacesStore.prototype.newView = function () { this.view++; };
  // url, 'json' or 'bin', a size cap, the abort signal, the byte count the index gives it (0: none), onProgress (called
  // when the answer starts and as its bytes arrive: a stall timer's re-arm). Every request asks for a low priority: the
  // page's own downloads (the forecast, the overlay's coastline and frames) come first (G34 B-6).
  PlacesStore.prototype._get = function (url, kind, limit, signal, expect, onProgress) {
    return this.fetch(url, { mode: 'cors', signal: signal, priority: 'low' }).then(function (r) {
      if (!r.ok) { var e = new Error('places ' + r.status); e.status = r.status; throw e; }
      if (r.headers && r.headers.get && Number(r.headers.get('content-length') || 0) > limit) throw new Error('places: file too large');
      if (onProgress) onProgress();
      if (kind === 'json') return r.json();
      if (onProgress && r.body && r.body.getReader) return readAll(r.body.getReader(), limit, onProgress);
      return r.arrayBuffer();
    }).then(function (body) {
      if (kind !== 'json' && body.byteLength > limit) throw new Error('places: file too large');
      // a file cut short or grown: the index gives every file's length (G34 F-A2)
      if (kind !== 'json' && expect && body.byteLength !== expect) throw new Error('places: a file of another length than the index says');
      return body;
    });
  };
  // Resolves to the store once the index is in and each tier-0 file wanted has landed or failed (null when neither kind
  // is usable); never rejects. One stall timer for the load: it fires only after STALL_MS with no byte arriving (a slow
  // but moving link is not given up: G34 B-6 / F-A6). After a failure the index, or a unit, answers null for RETRY_MS;
  // a load after that asks for it again. A kind asked for while a load runs (a row turned on meanwhile) starts its file
  // at once beside it, under the same abort and stall timer, and the load in progress then waits for it too (V2-2). A
  // file joining restarts the stall timer: it gets a full STALL_MS for its first byte, also when the other file has been
  // silent almost that long (fix check F-1).
  PlacesStore.prototype.load = function (kinds) {
    var self = this, now = Date.now(), want = kinds || ['l', 'p'];
    var need = ['l', 'p'].filter(function (k) { var u = self.unit[k]; return want.indexOf(k) >= 0 && u.status !== 'ok' && u.status !== 'loading' && !(u.status === 'failed' && now - u.failedAt < RETRY_MS); });
    if (this.loading) {
      if (need.length && this._arm) this._arm();
      need.forEach(function (k) { self._unitLoad(k); });
      return this.loading;
    }
    if (this.status === 'failed' && now - this.failedAt < RETRY_MS) return Promise.resolve(null);
    if (this.status === 'ok' && !need.length) return Promise.resolve(this.ok('l') || this.ok('p') ? this : null);
    if (this.status !== 'ok') this.status = 'loading';
    var ctrl = typeof AbortController === 'function' ? new AbortController() : null, sig = ctrl ? ctrl.signal : undefined, timer = null;
    var arm = function () { clearTimeout(timer); timer = setTimeout(function () { if (ctrl) ctrl.abort(); }, STALL_MS); };
    this.abort = ctrl; arm();
    this._sig = sig; this._arm = arm; this._units = []; this._kinds = [];
    this._idxP = this.status === 'ok' ? Promise.resolve(this.index) : this._get(this.base + '/index.json', 'json', MAX_INDEX_BYTES, sig, 0, arm).then(function (idx) {
      if (!validIndex(idx)) throw new Error('places: unsupported index');
      self.index = idx; self.status = 'ok';
      return idx;
    });
    need.forEach(function (k) { self._unitLoad(k); });
    // every unit started in this load, also one added while it ran
    var all = function () { var n = self._units.length; return Promise.all(self._units).then(function () { return self._units.length > n ? all() : null; }); };
    this.loading = all().then(function () {
      clearTimeout(timer); self.loading = null; self.abort = null; self._units = []; self._kinds = [];
      return self.ok('l') || self.ok('p') ? self : null;
    }, function () {
      clearTimeout(timer); self.loading = null; self.abort = null;
      self.status = 'failed'; self.failedAt = Date.now();
      self._kinds.forEach(function (k) { if (self.unit[k].status === 'loading') self.unit[k].status = 'idle'; });
      self._units = []; self._kinds = [];
      return null;
    });
    return this.loading;
  };
  // One kind's tier-0 file within the load in progress: after the index, under the load's abort and stall timer. A failed
  // file marks only its unit; an index that fails rejects (the load's failure).
  PlacesStore.prototype._unitLoad = function (k) {
    var self = this, u = this.unit[k], sig = this._sig, arm = this._arm;
    u.status = 'loading'; this._kinds.push(k);
    var p = this._idxP.then(function (idx) {
      var meta = k === 'l' ? idx.tier0.lines : idx.tier0.places;
      return self._get(self.base + '/' + meta.file, 'bin', MAX_TIER0_BYTES, sig, expectOf(meta.bytes), arm).then(function (b) {
        var data = k === 'l' ? decodeLines(b) : decodePlaces(b);
        if (k === 'l') self.lines0 = data; else self.places0 = data;
        u.status = 'ok'; self.rev++;
        if (self.onUnit) self.onUnit(k);                  // drawn and credited as it lands (re-check R-2)
      }).catch(function () { u.status = 'failed'; u.failedAt = Date.now(); });
    });
    p.catch(function () {});                                // a unit added after the index failed: the load's own handler reports it
    this._units.push(p);
    return p;
  };
  // The cells (of one kind, 'l' lines or 'p' places) among names that exist: decoded ones returned (LRU-touched),
  // missing ones requested; -> {sets: [decoded], missing: n, standIn: a cell the bucket cannot serve}.
  PlacesStore.prototype.cellsOf = function (names, kind) {
    var sets = [], missing = [], standIn = false, cells = this.index.tier1.cells;
    for (var i = 0; i < names.length; i++) {
      var nm = names[i], entry = cells[nm], key = nm + '.' + kind;
      if (!Object.prototype.hasOwnProperty.call(cells, nm) || !entry || !entry[kind === 'l' ? 0 : 1]) continue;
      var c = this.cells.get(key);
      if (c) { this.cells.delete(key); this.cells.set(key, c); c.used = this.view; sets.push(c.data); }
      else if (this.failed[key] === true) standIn = true;
      else missing.push(key);
    }
    if (missing.length) this.request(missing);
    return { sets: sets, missing: missing.length, standIn: standIn };
  };
  // The decoded line sets a tile draws, and whether they are final: tier 0 up to TIER0_MAX_ZOOM; from tile zoom 8 the
  // tile's cells (a cell the index does not list has no lines; one still loading or unservable keeps tier 0).
  PlacesStore.prototype.linesFor = function (coords) {
    if (!this.ok('l')) return { sets: [], complete: false };
    if (coords.z <= TIER0_MAX_ZOOM) return { sets: [this.lines0], complete: true };
    var got = this.cellsOf(cellsForTile(coords, this.index.tier1.cell), 'l');
    if (got.missing) return { sets: [this.lines0], complete: false };
    return { sets: got.standIn ? [this.lines0] : got.sets, complete: true };
  };
  // The place sets for a view (west..east unwrapped, south..north) at tile zoom z: tier 0 always; from zoom 8 the
  // loaded cells of the box too (missing ones requested; the labels are rebuilt when they land).
  PlacesStore.prototype.placesFor = function (west, south, east, north, z) {
    if (!this.ok('p')) return { sets: [], complete: false };
    if (z <= TIER0_MAX_ZOOM) return { sets: [this.places0], complete: true };
    var got = this.cellsOf(cellsForBox(west, south, east, north, this.index.tier1.cell), 'p');
    return { sets: [this.places0].concat(got.sets), complete: !got.missing };
  };
  PlacesStore.prototype.request = function (keys) {
    var now = Date.now();
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i], f = this.failed[key];
      if (this.inflight[key] || this.cells.has(key) || f === true || this.queue.indexOf(key) >= 0) continue;
      if (typeof f === 'number' && f > now) { this._armRetry(f); continue; }
      this.queue.push(key);
    }
    this._pump();
  };
  PlacesStore.prototype._armRetry = function (until) {
    var self = this;
    if (this.retryTimer) return;
    this.retryTimer = setTimeout(function () { self.retryTimer = null; self.rev++; if (self.onChange) self.onChange(); }, Math.max(0, until - Date.now()) + 50);
  };
  PlacesStore.prototype._pump = function () {
    var self = this;
    while (this.queue.length && Object.keys(this.inflight).length < MAX_INFLIGHT) {
      (function (key) {
        var ctrl = typeof AbortController === 'function' ? new AbortController() : null, done = false, kind = key.slice(-1), timer = null;
        var arm = function () { clearTimeout(timer); timer = setTimeout(function () { if (!done && ctrl) ctrl.abort(); }, STALL_MS); };
        var entry = self.index.tier1.cells[key.slice(0, -2)], expect = entry ? expectOf(entry[kind === 'l' ? 0 : 1]) : 0;
        self.inflight[key] = ctrl || true; arm();
        self._get(self.base + '/' + self.index.tier1.dir + '/' + key + '.bin', 'bin', MAX_CELL_BYTES, ctrl ? ctrl.signal : undefined, expect, arm).then(function (b) {
          var data = kind === 'l' ? decodeLines(b) : decodePlaces(b);
          if (data.cell !== self.index.tier1.cell) throw new Error('places: a cell of another size');
          return { data: data, bytes: decodedBytes(data), used: self.view };
        }).then(function (c) {
          done = true; clearTimeout(timer);
          if (self.inflight[key] !== (ctrl || true)) return;
          delete self.inflight[key];
          self.cells.set(key, c); self.bytes += c.bytes; self._trim();
          self.rev++; if (self.onChange) self.onChange(); self._pump();
        }, function (err) {
          done = true; clearTimeout(timer);
          if (self.inflight[key] !== (ctrl || true)) return;
          delete self.inflight[key];
          // a missing or undecodable cell is final (tier 0 stays there); anything else is asked again after a cooldown
          self.failed[key] = err && (err.status === 404 || err.status === 403 || /lines-v1|places-v1|another size|another length|too large/.test(err.message || '')) ? true : Date.now() + RETRY_MS;
          if (typeof self.failed[key] === 'number') self._armRetry(self.failed[key]);
          self.rev++; if (self.onChange) self.onChange(); self._pump();
        });
      })(this.queue.shift());
    }
  };
  // Least recently used first, by decoded size; a cell the current view uses is never evicted (the view may hold more
  // than the cap for a while: no cell is ever fetched again for the view that needs it; G34 B-4).
  PlacesStore.prototype._trim = function () {
    var keys = Array.from(this.cells.keys());
    for (var i = 0; i < keys.length && this.bytes > MAX_CELL_BYTES_KEPT; i++) {
      var c = this.cells.get(keys[i]);
      if (c.used === this.view) continue;
      this.cells.delete(keys[i]); this.bytes -= c.bytes;
    }
  };
  // Stop downloading (every kind turned off): queued and running cell fetches go; what is decoded stays.
  PlacesStore.prototype.idle = function () {
    for (var k in this.inflight) { var a = this.inflight[k]; if (a && a.abort) a.abort(); }
    this.inflight = {}; this.queue = [];
    if (this.retryTimer) { clearTimeout(this.retryTimer); this.retryTimer = null; }
  };

  // ---- drawing the lines ----
  // Which kinds a tile at zoom z draws, from the settings.
  function kindsAt(z, on) {
    var k = {};
    if (on.borders && z >= BORDER_ZOOM) { k[KIND_ADMIN0] = true; k[KIND_DISPUTED] = true; }
    if (on.borders && z >= STATE_ZOOM) k[KIND_ADMIN1] = true;
    return k;
  }
  // A line shows from its own Natural Earth zoom: tile zoom z serves map zooms below z + 0.5.
  function lineShown(mz10, z) { return mz10 < z * 10 + 5; }
  // The polylines of a tile as tile-local CSS px, per kind: pieces whose box meets the tile (grown by `margin` px) at
  // the tile's wrapped world x, lines of the kinds wanted and shown at this zoom; a segment wider than half the world
  // (none after the builder's cut at +-180) is never drawn.
  function tilePaths(coords, sets, kinds, margin) {
    var n = Math.pow(2, coords.z), scale = n, xw = ((coords.x % n) + n) % n, ox = xw * TILE, oy = coords.y * TILE;
    var m = (margin || 0) / n, bx0 = xw * TILE / n - m, bx1 = (xw + 1) * TILE / n + m, by0 = coords.y * TILE / n - m, by1 = (coords.y + 1) * TILE / n + m;
    var out = {}, half = TILE / 2;
    for (var s = 0; s < sets.length; s++) {
      var c = sets[s];
      for (var p = 0; p < c.n; p++) {
        var k = c.kind[p];
        if (!kinds[k]) continue;
        var b = 4 * p;
        if (c.box[b + 2] < bx0 || c.box[b] > bx1 || c.box[b + 3] < by0 || c.box[b + 1] > by1) continue;
        for (var l = c.lineStart[p]; l < c.lineStart[p + 1]; l++) {
          if (!lineShown(c.mz[l], coords.z)) continue;
          var v0 = c.vertStart[l], v1 = c.vertStart[l + 1], pts = [], lx = NaN, ly = NaN;
          for (var v = v0; v < v1; v++) {
            var wx = c.xy[2 * v], wy = c.xy[2 * v + 1];
            if (v > v0 && Math.abs(wx - c.xy[2 * v - 2]) > half) { if (pts.length >= 4) (out[k] = out[k] || []).push(pts); pts = []; lx = NaN; }
            var x = wx * scale - ox, y = wy * scale - oy;
            if (Math.abs(x - lx) < 0.25 && Math.abs(y - ly) < 0.25) continue;      // sub-pixel steps add nothing
            pts.push(x, y); lx = x; ly = y;
          }
          if (pts.length >= 4) (out[k] = out[k] || []).push(pts);
        }
      }
    }
    return out;
  }
  function strokePaths(ctx, list) {
    ctx.beginPath();
    for (var i = 0; i < list.length; i++) {
      var pts = list[i];
      ctx.moveTo(pts[0], pts[1]);
      for (var j = 2; j < pts.length; j += 2) ctx.lineTo(pts[j], pts[j + 1]);
    }
    ctx.stroke();
  }
  // halos first (every kind), then the cores, so a core is never covered by another line's halo
  function drawTile(ctx, paths) {
    ctx.lineJoin = 'round'; ctx.lineCap = 'round';
    for (var pass = 0; pass < 2; pass++) {
      for (var i = 0; i < DRAW_ORDER.length; i++) {
        var k = DRAW_ORDER[i], list = paths[k];
        if (!list || !list.length) continue;
        var st = STYLE[k];
        ctx.setLineDash(st.dash || []);
        ctx.lineWidth = pass ? st.core : st.halo; ctx.strokeStyle = pass ? st.coreInk : st.haloInk;
        strokePaths(ctx, list);
      }
    }
    ctx.setLineDash([]);
  }

  // ---- the labels ----
  // A place shows when the map zoom (x 10, whole) reaches its own: Natural Earth's min_zoom, GeoNames 8 / 9 / 10 by size.
  function placeShown(mz10, zoom10) { return mz10 <= zoom10; }
  // The candidates of a view: every place of the sets whose zoom is reached and whose position (in any world copy) lies
  // inside the view box (unwrapped degrees) grown by `marginDeg`; each with the copy's longitude. Sorted largest first:
  // rank, then population, then name, then longitude (a stable order, so the same view always thins the same way).
  function candidatesFor(sets, zoom10, west, south, east, north, marginDeg) {
    var out = [], w = west - marginDeg, e = east + marginDeg, s = south - marginDeg, n = north + marginDeg;
    for (var i = 0; i < sets.length; i++) {
      var c = sets[i];
      for (var k = 0; k < c.n; k++) {
        if (!placeShown(c.mz[k], zoom10) || c.lat[k] < s || c.lat[k] > n) continue;
        var lon = c.lon[k];
        for (var copy = Math.ceil((w - lon) / 360); lon + copy * 360 <= e; copy++) {
          out.push({ lat: c.lat[k], lon: lon + copy * 360, rank: c.rank[k], pop: c.pop[k], flags: c.flags[k], name: c.name[k] });
        }
      }
    }
    out.sort(function (a, b) { return a.rank - b.rank || b.pop - a.pop || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0) || a.lon - b.lon; });
    return out;
  }
  // The labels kept: in order, each whose box (the dot and the text to its right, grown by the gap) overlaps neither a
  // kept one nor a blocked rect, up to `max`. items: [{x, y, w, ...}] in container px; blocked: [{left, top, right, bottom}].
  // With the map's size {x, y} a label is kept only where its dot and its whole text lie on the map; when the text runs
  // off the right it goes to the left of the dot (item.side 'L'), else it stays right ('R').
  // h: the labels' height (the drawn font's line; LABEL_H by default).
  function thin(items, blocked, max, size, h) {
    var kept = [], grid = {}, i, j, hh = (h || LABEL_H) / 2;
    function boxOf(it, left) {
      return left ? { left: it.x - LABEL_DX - it.w - LABEL_GAP, top: it.y - hh - LABEL_GAP, right: it.x + DOT_R + LABEL_GAP, bottom: it.y + hh + LABEL_GAP }
                  : { left: it.x - DOT_R - LABEL_GAP, top: it.y - hh - LABEL_GAP, right: it.x + LABEL_DX + it.w + LABEL_GAP, bottom: it.y + hh + LABEL_GAP };
    }
    function fits(r) { return !size || (r.left + LABEL_GAP >= 0 && r.top + LABEL_GAP >= 0 && r.right - LABEL_GAP <= size.x && r.bottom - LABEL_GAP <= size.y); }
    function file(r) {
      for (var gy = Math.floor(r.top / BUCKET); gy <= Math.floor(r.bottom / BUCKET); gy++) {
        for (var gx = Math.floor(r.left / BUCKET); gx <= Math.floor(r.right / BUCKET); gx++) (grid[gx + ',' + gy] = grid[gx + ',' + gy] || []).push(r);
      }
    }
    function free(r) {
      for (var gy = Math.floor(r.top / BUCKET); gy <= Math.floor(r.bottom / BUCKET); gy++) {
        for (var gx = Math.floor(r.left / BUCKET); gx <= Math.floor(r.right / BUCKET); gx++) {
          var list = grid[gx + ',' + gy];
          if (!list) continue;
          for (j = 0; j < list.length; j++) { var o = list[j]; if (r.left < o.right && r.right > o.left && r.top < o.bottom && r.bottom > o.top) return false; }
        }
      }
      return true;
    }
    for (i = 0; i < blocked.length; i++) file(blocked[i]);
    for (i = 0; i < items.length && kept.length < max; i++) {
      var r = boxOf(items[i], false), side = 'R';
      if (!fits(r)) { r = boxOf(items[i], true); side = 'L'; if (!fits(r)) continue; }
      if (!free(r)) continue;
      file(r); items[i].side = side; kept.push(items[i]);
    }
    return kept;
  }
  // Calls start once the promise `waitFor()` returns has settled (the overlay's first picture: G34 B-6), at once without
  // one, and WAIT_MAX_MS after the call at the latest.
  function afterWait(waitFor, start) {
    var p = null;
    try { p = waitFor ? waitFor() : null; } catch (e) { p = null; }
    if (!p || typeof p.then !== 'function') { start(); return; }
    var done = false, fin = function () { if (!done) { done = true; start(); } };
    p.then(fin, fin); setTimeout(fin, WAIT_MAX_MS);
  }
  function hasClass(el, name) {
    var c = el && el.className;
    if (c && typeof c === 'object' && typeof c.baseVal === 'string') c = c.baseVal;
    return typeof c === 'string' && (' ' + c + ' ').indexOf(' ' + name + ' ') >= 0;
  }
  function readOn(storage, key) { try { return storage.getItem(key) !== '0'; } catch (e) { return true; } }
  function writeOn(storage, key, on) { try { storage.setItem(key, on ? '1' : '0'); } catch (e) {} }

  // opts: base (the static/places/v2 URL), storage (localStorage-like), L, document, fetch, whenReady (fn(start): calls
  // start once the page may download; default: the window's load event + START_DELAY_MS, then waitFor), waitFor (fn ->
  // a promise of the overlay's first picture, or null: the downloads wait for it, WAIT_MAX_MS at most; G34 B-6).
  // Returns {on(kind), set(kind, on), layer, labels, store, redraw, update, wanted}.
  function create(map, opts) {
    opts = opts || {};
    var L = opts.L || root.L, doc = opts.document || root.document;
    var storage = opts.storage || { getItem: function () { return null; }, setItem: function () {} };
    if (!L || !map || !doc) return null;
    var on = { borders: readOn(storage, KEYS.borders), places: readOn(storage, KEYS.places) };
    var store = opts.base ? new PlacesStore(opts.base, opts.fetch) : null, started = false;
    if (!map.getPane('placesLinePane')) { var pane = map.createPane('placesLinePane'); pane.style.zIndex = 340; pane.style.pointerEvents = 'none'; }
    var lpane = map.getPane('placesLabelPane');
    if (!lpane) { lpane = map.createPane('placesLabelPane'); lpane.style.zIndex = LABEL_PANE_Z; lpane.style.pointerEvents = 'none'; }
    var Layer = L.GridLayer.extend({
      createTile: function (coords) {
        var c = doc.createElement('canvas'), dpr = Math.min(2, root.devicePixelRatio || 1);
        c.width = 1; c.height = 1; c.style.width = TILE + 'px'; c.style.height = TILE + 'px';   // grown when it has a line (G34 B-7)
        c._plDpr = dpr;
        paint(c, coords);
        return c;
      }
    });
    var layer = new Layer({ pane: 'placesLinePane', tileSize: TILE, updateWhenZooming: false, keepBuffer: 1, noWrap: false });
    // the data is wanted once the page may download, with a kind on, from the zoom its first kind shows
    function firstZoom() { return on.places ? PLACES_ZOOM : BORDER_ZOOM; }
    function wanted() { return !!(started && store && (on.borders || on.places) && Math.round(map.getZoom()) >= firstZoom()); }
    // A tile with no line keeps a 1x1 backing store (a full one is 1 MB at dpr 2: most of a surf map has no border; G34
    // B-7); it is grown when a line comes into it.
    function paint(c, coords) {
      var dpr = c._plDpr || 1, kinds = kindsAt(coords.z, on), paths = {}, any = false;
      c._plComplete = store ? store.ok('l') : true;
      if (store && store.ok('l') && (kinds[0] || kinds[1] || kinds[2])) {
        var got = store.linesFor(coords);
        c._plComplete = got.complete;
        paths = tilePaths(coords, got.sets, kinds, 3);
      }
      for (var k in paths) if (paths[k].length) any = true;
      var w = any ? Math.round(TILE * dpr) : 1;
      if (c.width !== w) { c.width = w; c.height = w; }                     // (setting the size clears the canvas)
      var ctx = c.getContext && c.getContext('2d');
      if (!ctx || !any) { if (ctx) { ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, c.width, c.height); } return; }
      ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, c.width, c.height);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      drawTile(ctx, paths);
    }
    function redraw(onlyIncomplete) {
      for (var k in layer._tiles) {
        var t = layer._tiles[k];
        if (!t.el || !t.coords) continue;
        if (onlyIncomplete && t.el._plComplete) continue;
        paint(t.el, t.coords);
      }
    }
    // the labels: a box in the label pane, spans reused from a pool
    var box = doc.createElement('div');
    box.className = 'place-labels'; box.setAttribute('aria-hidden', 'true');
    lpane.appendChild(box);
    var pool = [], widths = {}, nwidths = 0, measure = null, labelsComplete = true, font = { n: LABEL_FONT, b: CAPITAL_FONT, h: LABEL_H }, probed = false;
    // The font the names are drawn in, read from a hidden probe of each kind: a browser's minimum font size (or the page's
    // CSS) can make it larger than LABEL_FONT, and the widths and the line must be the drawn ones (G34 B-8).
    function probeFont() {
      probed = true;
      var view = doc.defaultView || root, gcs = view && view.getComputedStyle;
      if (typeof gcs !== 'function') return;
      try {
        var next = {}, kinds = [['n', 'place-label'], ['b', 'place-label place-capital']];
        for (var i = 0; i < kinds.length; i++) {
          var sp = doc.createElement('span');
          sp.className = kinds[i][1]; sp.textContent = 'M'; sp.style.visibility = 'hidden';
          box.appendChild(sp);
          var cs = gcs.call(view, sp), px = parseFloat(cs.fontSize);
          if (px > 0 && cs.fontFamily) next[kinds[i][0]] = (cs.fontStyle && cs.fontStyle !== 'normal' ? cs.fontStyle + ' ' : '') + (cs.fontWeight || '') + ' ' + px + 'px ' + cs.fontFamily;
          if (kinds[i][0] === 'n') { var lh = parseFloat(cs.lineHeight); next.h = Math.max(LABEL_H, lh > 0 ? lh : 0, px > 0 ? Math.ceil(px * 1.2) : 0); }
          box.removeChild(sp);
        }
        if (next.n && next.b && (next.n !== font.n || next.b !== font.b || next.h !== font.h)) { font = next; widths = {}; nwidths = 0; }
      } catch (e) { /* the defaults stay */ }
    }
    function textWidth(name, bold) {
      var key = (bold ? 'b:' : 'n:') + name;
      if (widths[key] !== undefined) return widths[key];
      if (measure === null) {
        try { var cv = doc.createElement('canvas'); measure = (cv.getContext && cv.getContext('2d')) || false; } catch (e) { measure = false; }
      }
      var w = name.length * CHAR_PX;
      if (measure) { measure.font = bold ? font.b : font.n; var m = measure.measureText(name); if (m && m.width > 0) w = m.width; }
      if (nwidths >= WIDTH_CACHE_MAX) { widths = {}; nwidths = 0; }      // bounded: a long session never grows it (G34 B-4)
      nwidths++;
      return (widths[key] = w);
    }
    // the rects (container px) labels keep clear of: the map's controls and their open panels, the overlay's sheet, the
    // gridline labels, the map tools' fan and labels, and the floating windows over the map (outside its container; the
    // hidden ones and those off the map skipped). The station icons are no obstacle: the names are drawn above them.
    function blockedRects() {
      var c = map.getContainer(), base = c.getBoundingClientRect ? c.getBoundingClientRect() : { left: 0, top: 0 }, size = map.getSize(), out = [];
      function add(el) {
        if (el.hidden || !el.getBoundingClientRect) return;
        var r = el.getBoundingClientRect();
        if (r.right <= r.left || r.bottom <= r.top) return;
        var b = { left: r.left - base.left, top: r.top - base.top, right: r.right - base.left, bottom: r.bottom - base.top };
        if (b.right < 0 || b.bottom < 0 || b.left > size.x || b.top > size.y) return;
        out.push(b);
      }
      if (c.querySelectorAll) Array.prototype.forEach.call(c.querySelectorAll(BLOCKED_SELECTOR), add);
      if (doc.querySelectorAll) Array.prototype.forEach.call(doc.querySelectorAll(FLOAT_SELECTOR), add);
      return out;
    }
    function update() {
      if (!on.places || !store || !store.ok('p')) { box.hidden = true; labelsComplete = !(on.places && store && store.unit.p.status === 'loading'); return; }
      if (!probed) probeFont();
      box.hidden = false;
      var zoom = map.getZoom(), z = Math.round(zoom), zoom10 = Math.floor(zoom * 10 + 1e-6), size = map.getSize(), b = map.getBounds();
      var west = b.getWest(), east = b.getEast(), south = Math.max(b.getSouth(), -LAT_MAX), north = Math.min(b.getNorth(), LAT_MAX);
      var degPx = 360 / (TILE * Math.pow(2, zoom)), margin = VIEW_MARGIN_PX * degPx;
      var got = store.placesFor(west - margin, south - margin, east + margin, north + margin, z);
      labelsComplete = got.complete;
      var cands = candidatesFor(got.sets, zoom10, west, south, east, north, margin), items = [];
      for (var i = 0; i < cands.length; i++) {
        var cd = cands[i], p = map.latLngToContainerPoint([cd.lat, cd.lon]);
        items.push({ x: p.x, y: p.y, w: textWidth(cd.name, !!(cd.flags & FLAG_CAPITAL)), cand: cd });
      }
      var kept = thin(items, blockedRects(), size.x < NARROW_PX ? LABEL_MAX_NARROW : LABEL_MAX, size, font.h), used = 0;
      for (i = 0; i < kept.length; i++) {
        var it = kept[i], el = pool[used] || (pool[used] = box.appendChild(doc.createElement('span'))), lp = map.latLngToLayerPoint([it.cand.lat, it.cand.lon]);
        el.className = 'place-label' + (it.cand.flags & FLAG_CAPITAL ? ' place-capital' : '') + (it.side === 'L' ? ' place-left' : '');
        el.textContent = it.cand.name;
        el.style.left = Math.round(lp.x) + 'px'; el.style.top = Math.round(lp.y) + 'px';
        el.hidden = false;
        used++;
      }
      for (i = used; i < pool.length; i++) pool[i].hidden = true;
    }
    var frame = null;
    function schedule() { if (frame !== null) return; frame = (root.requestAnimationFrame || function (f) { return setTimeout(f, 16); })(function () { frame = null; update(); }); }
    if (store) store.onChange = function () { redraw(true); if (!labelsComplete) schedule(); };
    // a tier-0 file that lands is drawn and credited at once, without waiting for the other one (re-check R-2); the names
    // not during a zoom (their box is hidden: the zoom's end lays them out)
    if (store) store.onUnit = function (kind) {
      credits();
      if (kind === 'l') redraw(false); else if (!map._animatingZoom) update();
    };
    // What moves the space the names may use lays them out again (V7-1, G34 B-1 / B-2): a map control or the overlay's
    // sheet changing size, the sheet or a map tool's fan or labels coming or going, a floating window or a control's
    // panel opening, closing, moving or changing size. A sheet that goes is let go by the observer (G34 F-A4).
    var RO = root.ResizeObserver, MO = root.MutationObserver, cont = map.getContainer();
    var ro = RO ? new RO(function () { schedule(); }) : null, watched = [];
    function watch(el) { if (ro && el && watched.indexOf(el) < 0) { watched.push(el); ro.observe(el); } }
    function unwatch(el) { var i = watched.indexOf(el); if (ro && i >= 0) { watched.splice(i, 1); ro.unobserve(el); } }
    function relayNode(n) { for (var i = 0; i < RELAY_CLASSES.length; i++) if (hasClass(n, RELAY_CLASSES[i])) return true; return false; }
    var corners = map._controlCorners || {};
    for (var ck in corners) if (corners[ck]) watch(corners[ck]);
    Array.prototype.forEach.call(cont.querySelectorAll ? cont.querySelectorAll('.ov-sheet') : [], watch);
    var floats = [].concat(Array.prototype.slice.call(doc.querySelectorAll ? doc.querySelectorAll(FLOAT_SELECTOR) : []),
                           Array.prototype.slice.call(cont.querySelectorAll ? cont.querySelectorAll('.settings-panel') : []));
    floats.forEach(watch);
    if (MO) {
      new MO(function (records) {
        var go = false;
        for (var i = 0; i < records.length; i++) {
          var rec = records[i], j;
          for (j = 0; j < rec.addedNodes.length; j++) { var a = rec.addedNodes[j]; if (relayNode(a)) { go = true; if (hasClass(a, 'ov-sheet')) watch(a); } }
          for (j = 0; j < rec.removedNodes.length; j++) { var d = rec.removedNodes[j]; if (relayNode(d)) { go = true; if (hasClass(d, 'ov-sheet')) unwatch(d); } }
        }
        if (go) schedule();
      }).observe(cont, { childList: true, subtree: true });
      if (floats.length) {
        var fmo = new MO(function () { schedule(); });
        floats.forEach(function (el) { fmo.observe(el, { attributes: true, attributeFilter: ['class', 'style', 'hidden'] }); });
      }
    }
    // the credits, changed only on a change (Leaflet counts add / remove calls)
    var credited = { borders: false, places: false };
    function credit(kind, text, want) {
      if (!map.attributionControl || want === credited[kind]) return;
      credited[kind] = want;
      if (want) map.attributionControl.addAttribution(text); else map.attributionControl.removeAttribution(text);
    }
    // a kind is credited while it is on and its data has loaded (nothing is credited that cannot be drawn: V7-6)
    function credits() {
      credit('borders', CREDIT_BORDERS, !!(store && store.ok('l')) && on.borders); credit('places', CREDIT_PLACES, !!(store && store.ok('p')) && on.places);
    }
    // the line layer is on the map only while Borders is on (City names alone draws no tile: G34 F-A5 / B-7)
    function sync() {
      var any = on.borders || on.places;
      if (on.borders && !map.hasLayer(layer)) layer.addTo(map);
      if (!on.borders && map.hasLayer(layer)) map.removeLayer(layer);
      if (!any && store) store.idle();
      credits();
      // only the files of the kinds that are on (Borders off downloads no lines: re-check R-2); each draws as it lands
      // a kind turned on during a load starts its file at once beside it (V2-2); nothing new asked: no second call
      var miss = wanted() ? units().filter(function (k) { return !store.ok(k) && !(store.loading && store.unit[k].status === 'loading'); }) : [];
      if (miss.length) store.load(units()).then(function () {
        credits();
        if (store.status === 'ok' && units().some(function (k) { return store.unit[k].status === 'idle'; })) sync();   // a kind turned on meanwhile
      });
    }
    function units() { var u = []; if (on.borders) u.push('l'); if (on.places) u.push('p'); return u; }
    function set(kind, v) {
      if (!Object.prototype.hasOwnProperty.call(KEYS, kind)) return;
      on[kind] = !!v; writeOn(storage, KEYS[kind], on[kind]);
      sync(); redraw(false); update();
    }
    map.on('zoomend', sync);
    map.on('moveend zoomend resize viewreset', schedule);
    map.on('resize', function () { probed = false; });                        // the font is read again (a zoomed page)
    map.on('movestart zoomstart viewreset', function () { if (store) store.newView(); });
    map.on('zoomstart', function () { box.hidden = true; });
    var whenReady = opts.whenReady || function (start) {
      var go = function () { setTimeout(function () { afterWait(opts.waitFor, start); }, START_DELAY_MS); };
      if (doc.readyState === 'complete') go(); else if (root.addEventListener) root.addEventListener('load', go, { once: true });
    };
    whenReady(function () { started = true; sync(); });
    sync();
    return { on: function (kind) { return !!on[kind]; }, set: set, layer: layer, labels: box, store: store, redraw: redraw, update: update, wanted: wanted, textWidth: textWidth,
             fontUsed: function () { return font; } };
  }

  root.AllshorePlaces = {
    create: create,
    _internals: { KEYS: KEYS, WAIT_MAX_MS: WAIT_MAX_MS, WIDTH_CACHE_MAX: WIDTH_CACHE_MAX, STALL_MS: STALL_MS, FLOAT_SELECTOR: FLOAT_SELECTOR, RELAY_CLASSES: RELAY_CLASSES,
                  afterWait: afterWait, hasClass: hasClass, readAll: readAll, expectOf: expectOf, decodedBytes: decodedBytes, KIND_ADMIN0: KIND_ADMIN0, KIND_ADMIN1: KIND_ADMIN1, KIND_DISPUTED: KIND_DISPUTED, BORDER_ZOOM: BORDER_ZOOM, STATE_ZOOM: STATE_ZOOM, PLACES_ZOOM: PLACES_ZOOM,
                  TIER0_MAX_ZOOM: TIER0_MAX_ZOOM, STYLE: STYLE, DRAW_ORDER: DRAW_ORDER, MAX_INFLIGHT: MAX_INFLIGHT, MAX_CELL_BYTES_KEPT: MAX_CELL_BYTES_KEPT, RETRY_MS: RETRY_MS,
                  FLAG_CAPITAL: FLAG_CAPITAL, FLAG_GEONAMES: FLAG_GEONAMES, LABEL_FONT: LABEL_FONT, CAPITAL_FONT: CAPITAL_FONT, LABEL_H: LABEL_H, LABEL_GAP: LABEL_GAP, DOT_R: DOT_R, LABEL_DX: LABEL_DX,
                  LABEL_MAX: LABEL_MAX, LABEL_MAX_NARROW: LABEL_MAX_NARROW, NARROW_PX: NARROW_PX, VIEW_MARGIN_PX: VIEW_MARGIN_PX, BUCKET: BUCKET, CHAR_PX: CHAR_PX,
                  LABEL_PANE_Z: LABEL_PANE_Z, BLOCKED_SELECTOR: BLOCKED_SELECTOR,
                  CREDIT_BORDERS: CREDIT_BORDERS, CREDIT_PLACES: CREDIT_PLACES,
                  lonToX: lonToX, latToY: latToY, yToLat: yToLat, decodeLines: decodeLines, decodePlaces: decodePlaces, readVarints: readVarints, unzz: unzz, validIndex: validIndex,
                  cellName: cellName, cellsForBox: cellsForBox, cellsForTile: cellsForTile, PlacesStore: PlacesStore, kindsAt: kindsAt, lineShown: lineShown, tilePaths: tilePaths,
                  drawTile: drawTile, placeShown: placeShown, candidatesFor: candidatesFor, thin: thin, readOn: readOn, writeOn: writeOn }
  };
})(typeof window !== 'undefined' ? window : this);
