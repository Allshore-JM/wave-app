/* Country and state/province borders and city names on the map (plan section 51).
 *
 * Data: static/places/v1 on the frames bucket (tools/places/build_places.py): index.json, tier 0 (world-lines.bin and
 * world-places.bin, for tile zoom 7 and below) and tier 1 (10-degree cells c/<lat>_<lon>.l.bin / .p.bin, from tile zoom 8).
 * Lines: an L.GridLayer of 256-px canvas tiles in its own pane (z 340: above the model overlay and its particles, below
 * the gridlines and the forecast markers), a dark halo under a light core, so they read on the imagery and the wind's
 * relief alike; world copies come with the grid layer. Kinds: 0 settled country border, 2 disputed / indefinite / line of
 * control (dashed), 1 state or province line (thinner, dashed). A country line shows from tile zoom 3, a state line from
 * zoom 5, and each line from its own Natural Earth zoom too.
 * Names: text spans in a second pane (z 360: above the gridlines, below the forecast circles and the markers), placed at
 * the map's layer points (they ride along with a pan) and rebuilt after every move or zoom: every place whose own zoom
 * is reached (Natural Earth places at Natural Earth's min_zoom; GeoNames towns from zoom 8, 9 or 10 by size, set by the
 * builder), the largest first, each label kept only where it overlaps neither another label nor a map control, a marker
 * or a gridline label; at most 120 labels (60 on a narrow map).
 * Nothing is fetched before the page has loaded, nor while every kind is off or the map is below the first wanted zoom.
 * Settings (localStorage, '0' = off; anything else, or nothing, = on): 'allshore.borders.v1', 'allshore.states.v1',
 * 'allshore.places.v1'. Credits join the map's attribution while a kind is on.
 *
 * window.AllshorePlaces = { create(map, opts), _internals }. Nothing touches the DOM at load (Node-testable).
 */
(function (root) {
  'use strict';

  var TILE = 256;
  var LAT_MAX = 85.05112878;                         // Web Mercator's edge
  var KEYS = { borders: 'allshore.borders.v1', states: 'allshore.states.v1', places: 'allshore.places.v1' };
  var KIND_ADMIN0 = 0, KIND_ADMIN1 = 1, KIND_DISPUTED = 2;
  var BORDER_ZOOM = 3, STATE_ZOOM = 5, PLACES_ZOOM = 2;   // tile zoom from which each kind is drawn (owner, 2026-10-23)
  var TIER0_MAX_ZOOM = 7;                            // tile zoom up to which the world files serve (tier 1 from 8)
  var LINES_MAGIC = 'LNS1', LINES_HEADER = 40, PLACES_MAGIC = 'PLC1', PLACES_HEADER = 36;
  var FLAG_CAPITAL = 1, FLAG_GEONAMES = 2;
  var MAX_INDEX_BYTES = 1 << 20, MAX_TIER0_BYTES = 4 << 20, MAX_CELL_BYTES = 2 << 20;
  var MAX_CELL_BYTES_KEPT = 6 << 20;                 // decoded cells kept (LRU by file bytes)
  var MAX_INFLIGHT = 2;
  var STALL_MS = 15000, RETRY_MS = 60000, START_DELAY_MS = 1500;
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
    var text = new TextDecoder('utf-8', { fatal: true }).decode(u8.subarray(PLACES_HEADER + nstream));
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

  // ---- the store: the index and both tier-0 files at once, the cells on demand (keys "<name>.l" / "<name>.p") ----
  function PlacesStore(base, fetchFn) {
    this.base = base; this.fetch = fetchFn || (root.fetch && root.fetch.bind(root));
    this.status = 'idle'; this.index = null; this.lines0 = null; this.places0 = null; this.loading = null; this.failedAt = 0;
    this.cells = new Map(); this.bytes = 0; this.inflight = {}; this.failed = {}; this.queue = [];
    this.rev = 0; this.onChange = null; this.abort = null; this.retryTimer = null;
  }
  PlacesStore.prototype._get = function (url, kind, limit, signal) {
    return this.fetch(url, { mode: 'cors', signal: signal }).then(function (r) {
      if (!r.ok) { var e = new Error('places ' + r.status); e.status = r.status; throw e; }
      if (r.headers && r.headers.get && Number(r.headers.get('content-length') || 0) > limit) throw new Error('places: file too large');
      return kind === 'json' ? r.json() : r.arrayBuffer();
    }).then(function (body) {
      if (kind !== 'json' && body.byteLength > limit) throw new Error('places: file too large');
      return body;
    });
  };
  // Resolves to the store (tier 0 usable) or null (never rejects); after a failure null for RETRY_MS, then a retry.
  PlacesStore.prototype.load = function () {
    var self = this;
    if (this.status === 'ok') return Promise.resolve(this);
    if (this.loading) return this.loading;
    if (this.status === 'failed' && Date.now() - this.failedAt < RETRY_MS) return Promise.resolve(null);
    this.status = 'loading';
    var ctrl = typeof AbortController === 'function' ? new AbortController() : null, sig = ctrl ? ctrl.signal : undefined;
    this.abort = ctrl;
    var watchdog = setTimeout(function () { if (ctrl) ctrl.abort(); }, STALL_MS);
    this.loading = this._get(this.base + '/index.json', 'json', MAX_INDEX_BYTES, sig).then(function (idx) {
      if (!validIndex(idx)) throw new Error('places: unsupported index');
      return Promise.all([idx, self._get(self.base + '/' + idx.tier0.lines.file, 'bin', MAX_TIER0_BYTES, sig),
                          self._get(self.base + '/' + idx.tier0.places.file, 'bin', MAX_TIER0_BYTES, sig)]);
    }).then(function (res) {
      clearTimeout(watchdog);
      if (self.abort !== ctrl) throw new Error('superseded');
      var lines0 = decodeLines(res[1]), places0 = decodePlaces(res[2]);
      self.lines0 = lines0; self.places0 = places0; self.index = res[0];
      self.status = 'ok'; self.loading = null; self.abort = null; self.rev++;
      return self;
    }).catch(function () {
      clearTimeout(watchdog);
      if (self.abort === ctrl) { self.loading = null; self.abort = null; self.status = 'failed'; self.failedAt = Date.now(); }
      return null;
    });
    return this.loading;
  };
  // The cells (of one kind, 'l' lines or 'p' places) among names that exist: decoded ones returned (LRU-touched),
  // missing ones requested; -> {sets: [decoded], missing: n, standIn: a cell the bucket cannot serve}.
  PlacesStore.prototype.cellsOf = function (names, kind) {
    var sets = [], missing = [], standIn = false, cells = this.index.tier1.cells;
    for (var i = 0; i < names.length; i++) {
      var nm = names[i], entry = cells[nm], key = nm + '.' + kind;
      if (!Object.prototype.hasOwnProperty.call(cells, nm) || !entry || !entry[kind === 'l' ? 0 : 1]) continue;
      var c = this.cells.get(key);
      if (c) { this.cells.delete(key); this.cells.set(key, c); sets.push(c.data); }
      else if (this.failed[key] === true) standIn = true;
      else missing.push(key);
    }
    if (missing.length) this.request(missing);
    return { sets: sets, missing: missing.length, standIn: standIn };
  };
  // The decoded line sets a tile draws, and whether they are final: tier 0 up to TIER0_MAX_ZOOM; from tile zoom 8 the
  // tile's cells (a cell the index does not list has no lines; one still loading or unservable keeps tier 0).
  PlacesStore.prototype.linesFor = function (coords) {
    if (this.status !== 'ok') return { sets: [], complete: false };
    if (coords.z <= TIER0_MAX_ZOOM) return { sets: [this.lines0], complete: true };
    var got = this.cellsOf(cellsForTile(coords, this.index.tier1.cell), 'l');
    if (got.missing) return { sets: [this.lines0], complete: false };
    return { sets: got.standIn ? [this.lines0] : got.sets, complete: true };
  };
  // The place sets for a view (west..east unwrapped, south..north) at tile zoom z: tier 0 always; from zoom 8 the
  // loaded cells of the box too (missing ones requested; the labels are rebuilt when they land).
  PlacesStore.prototype.placesFor = function (west, south, east, north, z) {
    if (this.status !== 'ok') return { sets: [], complete: false };
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
        var ctrl = typeof AbortController === 'function' ? new AbortController() : null, done = false, kind = key.slice(-1);
        var timer = setTimeout(function () { if (!done && ctrl) ctrl.abort(); }, STALL_MS);
        self.inflight[key] = ctrl || true;
        self._get(self.base + '/' + self.index.tier1.dir + '/' + key + '.bin', 'bin', MAX_CELL_BYTES, ctrl ? ctrl.signal : undefined).then(function (b) {
          var data = kind === 'l' ? decodeLines(b) : decodePlaces(b);
          if (data.cell !== self.index.tier1.cell) throw new Error('places: a cell of another size');
          return { data: data, bytes: b.byteLength };
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
          self.failed[key] = err && (err.status === 404 || err.status === 403 || /lines-v1|places-v1|another size|too large/.test(err.message || '')) ? true : Date.now() + RETRY_MS;
          if (typeof self.failed[key] === 'number') self._armRetry(self.failed[key]);
          self.rev++; if (self.onChange) self.onChange(); self._pump();
        });
      })(this.queue.shift());
    }
  };
  PlacesStore.prototype._trim = function () {
    var it = this.cells.keys();
    while (this.bytes > MAX_CELL_BYTES_KEPT && this.cells.size > 1) {
      var k = it.next().value, c = this.cells.get(k);
      this.cells.delete(k); this.bytes -= c.bytes;
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
    if (on.states && z >= STATE_ZOOM) k[KIND_ADMIN1] = true;
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
  function thin(items, blocked, max) {
    var kept = [], grid = {}, i, j;
    function boxOf(it) { return { left: it.x - DOT_R - LABEL_GAP, top: it.y - LABEL_H / 2 - LABEL_GAP, right: it.x + LABEL_DX + it.w + LABEL_GAP, bottom: it.y + LABEL_H / 2 + LABEL_GAP }; }
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
      var r = boxOf(items[i]);
      if (!free(r)) continue;
      file(r); kept.push(items[i]);
    }
    return kept;
  }
  function readOn(storage, key) { try { return storage.getItem(key) !== '0'; } catch (e) { return true; } }
  function writeOn(storage, key, on) { try { storage.setItem(key, on ? '1' : '0'); } catch (e) {} }

  // opts: base (the static/places/v1 URL), storage (localStorage-like), L, document, fetch, whenReady (fn(start): calls
  // start once the page may download; default: the window's load event + START_DELAY_MS).
  // Returns {on(kind), set(kind, on), layer, labels, store, redraw, update, wanted}.
  function create(map, opts) {
    opts = opts || {};
    var L = opts.L || root.L, doc = opts.document || root.document;
    var storage = opts.storage || { getItem: function () { return null; }, setItem: function () {} };
    if (!L || !map || !doc) return null;
    var on = { borders: readOn(storage, KEYS.borders), states: readOn(storage, KEYS.states), places: readOn(storage, KEYS.places) };
    var store = opts.base ? new PlacesStore(opts.base, opts.fetch) : null, started = false;
    if (!map.getPane('placesLinePane')) { var pane = map.createPane('placesLinePane'); pane.style.zIndex = 340; pane.style.pointerEvents = 'none'; }
    var lpane = map.getPane('placesLabelPane');
    if (!lpane) { lpane = map.createPane('placesLabelPane'); lpane.style.zIndex = 360; lpane.style.pointerEvents = 'none'; }
    var Layer = L.GridLayer.extend({
      createTile: function (coords) {
        var c = doc.createElement('canvas'), dpr = Math.min(2, root.devicePixelRatio || 1);
        c.width = TILE * dpr; c.height = TILE * dpr; c.style.width = TILE + 'px'; c.style.height = TILE + 'px';
        c._plDpr = dpr;
        paint(c, coords);
        return c;
      }
    });
    var layer = new Layer({ pane: 'placesLinePane', tileSize: TILE, updateWhenZooming: false, keepBuffer: 1, noWrap: false });
    // the data is wanted once the page may download, with a kind on, from the zoom its first kind shows
    function firstZoom() { return on.places ? PLACES_ZOOM : on.borders ? BORDER_ZOOM : STATE_ZOOM; }
    function wanted() { return !!(started && store && (on.borders || on.states || on.places) && Math.round(map.getZoom()) >= firstZoom()); }
    function paint(c, coords) {
      var ctx = c.getContext && c.getContext('2d');
      c._plComplete = true;
      if (!ctx) return;
      var dpr = c._plDpr || 1;
      ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, c.width, c.height);
      var kinds = kindsAt(coords.z, on);
      if (!store || store.status !== 'ok' || (!kinds[0] && !kinds[1] && !kinds[2])) { c._plComplete = store ? store.status === 'ok' : true; return; }
      var got = store.linesFor(coords);
      c._plComplete = got.complete;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      drawTile(ctx, tilePaths(coords, got.sets, kinds, 3));
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
    var pool = [], widths = {}, measure = null, labelsComplete = true;
    function textWidth(name, bold) {
      var key = (bold ? 'b:' : 'n:') + name;
      if (widths[key] !== undefined) return widths[key];
      if (measure === null) {
        try { var cv = doc.createElement('canvas'); measure = (cv.getContext && cv.getContext('2d')) || false; } catch (e) { measure = false; }
      }
      var w = name.length * CHAR_PX;
      if (measure) { measure.font = bold ? CAPITAL_FONT : LABEL_FONT; var m = measure.measureText(name); if (m && m.width > 0) w = m.width; }
      return (widths[key] = w);
    }
    // the rects (container px) labels keep clear of: the map's controls, the overlay's sheet, the gridline labels, the markers
    function blockedRects() {
      var c = map.getContainer(), base = c.getBoundingClientRect ? c.getBoundingClientRect() : { left: 0, top: 0 }, size = map.getSize(), out = [];
      if (!c.querySelectorAll) return out;
      Array.prototype.forEach.call(c.querySelectorAll('.leaflet-control, .ov-sheet, .graticule-label, .leaflet-marker-icon'), function (el) {
        if (el.hidden || !el.getBoundingClientRect) return;
        var r = el.getBoundingClientRect();
        if (r.right <= r.left || r.bottom <= r.top) return;
        var b = { left: r.left - base.left, top: r.top - base.top, right: r.right - base.left, bottom: r.bottom - base.top };
        if (b.right < 0 || b.bottom < 0 || b.left > size.x || b.top > size.y) return;
        out.push(b);
      });
      return out;
    }
    function update() {
      if (!on.places || !store || store.status !== 'ok' || !map.hasLayer(layer)) { box.hidden = true; labelsComplete = true; return; }
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
      var kept = thin(items, blockedRects(), size.x < NARROW_PX ? LABEL_MAX_NARROW : LABEL_MAX), used = 0;
      for (i = 0; i < kept.length; i++) {
        var it = kept[i], el = pool[used] || (pool[used] = box.appendChild(doc.createElement('span'))), lp = map.latLngToLayerPoint([it.cand.lat, it.cand.lon]);
        el.className = 'place-label' + (it.cand.flags & FLAG_CAPITAL ? ' place-capital' : '');
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
    // the credits, changed only on a change (Leaflet counts add / remove calls)
    var credited = { borders: false, places: false };
    function credit(kind, text, want) {
      if (!map.attributionControl || want === credited[kind]) return;
      credited[kind] = want;
      if (want) map.attributionControl.addAttribution(text); else map.attributionControl.removeAttribution(text);
    }
    function sync() {
      var any = on.borders || on.states || on.places;
      if (any && !map.hasLayer(layer)) layer.addTo(map);
      if (!any && map.hasLayer(layer)) map.removeLayer(layer);
      if (!any && store) store.idle();
      credit('borders', CREDIT_BORDERS, !!(on.borders || on.states)); credit('places', CREDIT_PLACES, !!on.places);
      if (wanted() && store.status !== 'ok' && !store.loading) store.load().then(function (s) { if (s) { redraw(false); update(); } });   // one redraw per load
    }
    function set(kind, v) {
      if (!Object.prototype.hasOwnProperty.call(KEYS, kind)) return;
      on[kind] = !!v; writeOn(storage, KEYS[kind], on[kind]);
      sync(); redraw(false); update();
    }
    map.on('zoomend', sync);
    map.on('moveend zoomend resize viewreset', schedule);
    map.on('zoomstart', function () { box.hidden = true; });
    var whenReady = opts.whenReady || function (start) {
      var go = function () { setTimeout(start, START_DELAY_MS); };
      if (doc.readyState === 'complete') go(); else if (root.addEventListener) root.addEventListener('load', go, { once: true });
    };
    whenReady(function () { started = true; sync(); });
    sync();
    return { on: function (kind) { return !!on[kind]; }, set: set, layer: layer, labels: box, store: store, redraw: redraw, update: update, wanted: wanted, textWidth: textWidth };
  }

  root.AllshorePlaces = {
    create: create,
    _internals: { KEYS: KEYS, KIND_ADMIN0: KIND_ADMIN0, KIND_ADMIN1: KIND_ADMIN1, KIND_DISPUTED: KIND_DISPUTED, BORDER_ZOOM: BORDER_ZOOM, STATE_ZOOM: STATE_ZOOM, PLACES_ZOOM: PLACES_ZOOM,
                  TIER0_MAX_ZOOM: TIER0_MAX_ZOOM, STYLE: STYLE, DRAW_ORDER: DRAW_ORDER, MAX_INFLIGHT: MAX_INFLIGHT, MAX_CELL_BYTES_KEPT: MAX_CELL_BYTES_KEPT, RETRY_MS: RETRY_MS,
                  FLAG_CAPITAL: FLAG_CAPITAL, FLAG_GEONAMES: FLAG_GEONAMES, LABEL_FONT: LABEL_FONT, CAPITAL_FONT: CAPITAL_FONT, LABEL_H: LABEL_H, LABEL_GAP: LABEL_GAP, DOT_R: DOT_R, LABEL_DX: LABEL_DX,
                  LABEL_MAX: LABEL_MAX, LABEL_MAX_NARROW: LABEL_MAX_NARROW, NARROW_PX: NARROW_PX, VIEW_MARGIN_PX: VIEW_MARGIN_PX, BUCKET: BUCKET, CHAR_PX: CHAR_PX,
                  CREDIT_BORDERS: CREDIT_BORDERS, CREDIT_PLACES: CREDIT_PLACES,
                  lonToX: lonToX, latToY: latToY, yToLat: yToLat, decodeLines: decodeLines, decodePlaces: decodePlaces, readVarints: readVarints, unzz: unzz, validIndex: validIndex,
                  cellName: cellName, cellsForBox: cellsForBox, cellsForTile: cellsForTile, PlacesStore: PlacesStore, kindsAt: kindsAt, lineShown: lineShown, tilePaths: tilePaths,
                  drawTile: drawTile, placeShown: placeShown, candidatesFor: candidatesFor, thin: thin, readOn: readOn, writeOn: writeOn }
  };
})(typeof window !== 'undefined' ? window : this);
