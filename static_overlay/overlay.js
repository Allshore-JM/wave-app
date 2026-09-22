/* Allshore Surf model overlay (Phase 2: one static frame per field). Loaded on demand; never on page load.
 *
 * Data contract (manifest schema 2 or 3 from tools/model_frames): 8-bit greyscale PNG frames,
 * q = 0 missing, value = lo + (q - 1) / 254 * (hi - lo), q = 1 means "<= lo", q = 255 ">= hi";
 * grid centre-registered from (+90 N, -180 E), longitude periodic; grid_half = full[::2, ::2].
 * Frames are sampled per canvas tile (inverse Web-Mercator) in a dedicated 'modelPane'
 * (z 250, pointer-events none), so markers and gestures are untouched. ONE sampler
 * (ModelGridLayer._code) feeds both the drawn pixels and the hover/long-press readout.
 */
(function () {
  'use strict';

  var SESSION_KEY = 'allshore.overlay.v1';
  var STALE_AFTER_S = 9 * 3600;               // pointer older than this -> "stale" banner
  var POINTER_RECHECK_MS = 30 * 60 * 1000;    // re-read latest.json on mount when the cached pointer is older
  var ENCODING = 'u8-linear-v2';
  var TILE = 256;
  // <= 29 visible characters incl. the separator so it never wraps over the zoom control; the full
  // sentence (manifest attribution) is in the panel note.
  var ATTRIBUTION = 'Overlay: <a href="https://polar.ncep.noaa.gov/waves/" target="_blank" rel="noopener">NOAA GFS-Wave</a>/GFS';
  var RAMPS = {
    hs:   [[0,'#0b2c6b'],[0.15,'#1f6fd6'],[0.3,'#19c3e6'],[0.45,'#3fd96b'],[0.6,'#f2e33a'],[0.75,'#f5901f'],[0.9,'#e02020'],[1,'#a3129e']],
    tp:   [[0,'#2a1f7a'],[0.25,'#2e7ed8'],[0.5,'#38c9a8'],[0.7,'#c8e63c'],[0.85,'#f7a52b'],[1,'#e8321f']],
    wind: [[0,'#e8f1ff'],[0.2,'#8cc4ff'],[0.4,'#3aa35a'],[0.6,'#f0d433'],[0.8,'#f0731f'],[1,'#b00f3a']]
  };
  // Legend tick values in DISPLAY units per field and site unit; the legend top is added as "N+".
  var TICKS = {
    'hs|US': [0, 10, 20, 30], 'hs|Metric': [0, 3, 6, 9],
    'tp|US': [4, 8, 12, 16, 20], 'tp|Metric': [4, 8, 12, 16, 20],
    'wind|US': [0, 20, 40, 60], 'wind|Metric': [0, 25, 50, 75, 100]
  };

  function saved() { try { return JSON.parse(sessionStorage.getItem(SESSION_KEY) || '{}'); } catch (e) { return {}; } }
  function save(patch) { try { sessionStorage.setItem(SESSION_KEY, JSON.stringify(Object.assign(saved(), patch))); } catch (e) {} }
  function hexToRgb(h) { var n = parseInt(h.slice(1), 16); return [n >> 16 & 255, n >> 8 & 255, n & 255]; }
  function buildRamp(stops) {
    // 256-entry RGB lookup over t in [0,1]
    var out = new Uint8ClampedArray(256 * 3);
    for (var i = 0; i < 256; i++) {
      var t = i / 255, k = 0;
      while (k < stops.length - 2 && t > stops[k + 1][0]) k++;
      var a = stops[k], b = stops[k + 1], f = (t - a[0]) / Math.max(1e-9, b[0] - a[0]);
      f = Math.max(0, Math.min(1, f));
      var ca = hexToRgb(a[1]), cb = hexToRgb(b[1]);
      out[i * 3] = ca[0] + (cb[0] - ca[0]) * f; out[i * 3 + 1] = ca[1] + (cb[1] - ca[1]) * f; out[i * 3 + 2] = ca[2] + (cb[2] - ca[2]) * f;
    }
    return out;
  }
  function pad3(n) { return (n < 10 ? '00' : n < 100 ? '0' : '') + n; }
  function clear(el) { if (el) while (el.firstChild) el.removeChild(el.firstChild); }
  function mk(tag, cls, text) { var e = document.createElement(tag); if (cls) e.className = cls; if (text !== undefined) e.textContent = text; return e; }

  // ---- units (site preference: 'US' | 'Metric') ----
  function unitOf(field, unit) {
    if (field === 'hs') return unit === 'Metric' ? { label: 'm', f: function (v) { return v; }, d: 1 } : { label: 'ft', f: function (v) { return v * 3.28084; }, d: 1 };
    if (field === 'tp') return { label: 's', f: function (v) { return v; }, d: 1 };
    return unit === 'Metric' ? { label: 'km/h', f: function (v) { return v * 3.6; }, d: 0 } : { label: 'mph', f: function (v) { return v * 2.23694; }, d: 0 };
  }
  function legendTicks(field, fdef, unit) {
    var u = unitOf(field, unit), lo = u.f(fdef.legend[0]), hi = u.f(fdef.legend[1]);
    var vals = TICKS[field + '|' + (unit === 'Metric' ? 'Metric' : 'US')] || [fdef.legend[0]];
    var out = vals.map(function (v, i) {
      var below = i === 0 && fdef.lo < fdef.legend[0] - 1e-9;          // values under the legend floor exist (Tp)
      return { pos: Math.max(0, Math.min(1, (v - lo) / (hi - lo))), label: (below ? '≤' : '') + Math.round(v) };
    });
    out.push({ pos: 1, label: Math.round(hi) + '+ ' + u.label });
    return out;
  }

  // ---- manifest helpers (schema 2: per-frame file keys; schema 3: one template) ----
  function frameKey(m, fr, field, half) {
    if (fr.files && fr.files[field]) return fr.files[field][half ? 'half' : 'full'];
    var t = m.files && m.files.template, res = m.files && m.files.res;
    if (!t || !res) throw new Error('manifest has no frame files');
    return t.replace('{res}', half ? res.half : res.full).replace('{field}', field).replace('{step:03d}', pad3(fr.step));
  }
  function validateManifest(m) {
    if (!m || (m.schema !== 2 && m.schema !== 3)) throw new Error('unsupported manifest schema');
    if (m.encoding !== ENCODING) throw new Error('unsupported frame encoding ' + m.encoding);
    if (!m.complete) throw new Error('published run is not complete');
    if (!m.run || !m.run_utc || !m.fields || !m.grid || !m.grid_half || !Array.isArray(m.frames) || !m.frames.length) throw new Error('manifest incomplete');
    for (var i = 0; i < m.frames.length; i++) {
      var fr = m.frames[i];
      if (!fr || typeof fr.step !== 'number' || !fr.valid_utc || isNaN(Date.parse(fr.valid_utc))) throw new Error('bad frame entry ' + i);
    }
    return m;
  }
  // The decoded PNG must be the grid the manifest describes; anything else is drawn nowhere.
  function validateGrid(grid, frame, fdef) {
    var ok = grid && frame && fdef && frame.cols === grid.cols && frame.rows === grid.rows && frame.q && frame.q.length === grid.cols * grid.rows &&
      grid.registration === 'center' && grid.lon_periodic === true && grid.dlat < 0 && grid.dlon > 0 &&
      Math.abs(grid.cols * grid.dlon - 360) < 1e-6 && Math.abs(grid.lat0 - 90) < 1e-9 &&
      Math.abs(grid.lat0 + (grid.rows - 1) * grid.dlat + 90) < 1e-6 && Math.abs(grid.lon0 + 180) < 1e-9 &&
      typeof fdef.lo === 'number' && typeof fdef.hi === 'number' && fdef.lo < fdef.hi && fdef.legend && fdef.legend[0] < fdef.legend[1] &&
      fdef.legend[0] >= fdef.lo - 1e-9 && fdef.legend[1] <= fdef.hi + 1e-9;
    if (!ok) throw new Error('frame does not match the manifest grid');
  }
  function pickFrame(m, now) {
    now = now === undefined ? Date.now() : now;
    for (var i = 0; i < m.frames.length; i++) if (Date.parse(m.frames[i].valid_utc) >= now) return i;
    return m.frames.length - 1;
  }
  // Half-resolution (0.5 deg) frames where a 0.25 deg cell is only a few pixels anyway; hysteresis so a
  // zoom hovering around the threshold does not re-fetch on every step.
  function wantHalf(zoom, width) { return zoom < 3.5 || (width < 700 && zoom < 5.5); }
  function wantFull(zoom, width) { return zoom >= 4 && (width >= 700 || zoom >= 6); }
  // Pixel centre of a Web-Mercator tile pixel (the same expressions tileCodes uses).
  function tilePixelLatLng(coords, px, py) {
    var n = TILE * Math.pow(2, coords.z);
    var lon = (coords.x * TILE + px + 0.5) / n * 360 - 180;
    var lat = Math.atan(Math.sinh(Math.PI - 2 * Math.PI * (coords.y * TILE + py + 0.5) / n)) * 180 / Math.PI;
    return { lat: lat, lng: lon };
  }

  // ---- frame decoding ----
  function decodeFrame(url, signal) {
    // PNG -> Uint8Array of codes (R channel). Needs CORS (bucket policy) for getImageData.
    return fetch(url, { signal: signal, mode: 'cors' }).then(function (r) {
      if (!r.ok) throw new Error('frame ' + r.status);
      return r.blob();
    }).then(function (blob) {
      if (typeof createImageBitmap === 'function') return createImageBitmap(blob, { premultiplyAlpha: 'none', colorSpaceConversion: 'none' });
      return new Promise(function (resolve, reject) {                  // older Safari: <img> from a same-origin blob URL
        var img = new Image(), u = URL.createObjectURL(blob);
        img.onload = function () { URL.revokeObjectURL(u); resolve(img); };
        img.onerror = function () { URL.revokeObjectURL(u); reject(new Error('frame decode failed')); };
        img.src = u;
      });
    }).then(function (bmp) {
      var w = bmp.width, h = bmp.height;                               // read BEFORE close(): a closed bitmap reports 0x0
      if (!w || !h) throw new Error('frame decoded to 0x0');
      var c = document.createElement('canvas'); c.width = w; c.height = h;
      var ctx = c.getContext('2d', { willReadFrequently: true });
      ctx.drawImage(bmp, 0, 0);
      var d = ctx.getImageData(0, 0, w, h).data;
      var q = new Uint8Array(w * h);
      for (var i = 0, j = 0; i < q.length; i++, j += 4) q[i] = d[j];
      if (bmp.close) bmp.close();
      return { q: q, cols: w, rows: h };
    });
  }

  // ---- the layer ----
  var ModelGridLayer = L.GridLayer.extend({
    initialize: function (opts) {
      L.GridLayer.prototype.initialize.call(this, { pane: 'modelPane', tileSize: TILE, updateWhenZooming: false,
        updateWhenIdle: true, keepBuffer: 1, opacity: opts.opacity, className: 'ov-tiles' });
      this._frame = null; this._grid = null; this._lut = null; this._nearest = false; this._lo = 0; this._hi = 1;
      this._legend = [0, 1]; this.field = null; this.fdef = null; this.entry = null;
      this._codes = new Float64Array(TILE * TILE); this._colPos = new Float64Array(TILE);
      this._one = new Float64Array(1); this._oneOut = new Float64Array(1);
    },
    // frame {q, cols, rows}; grid = the manifest grid for that resolution; fdef = the manifest field
    setFrame: function (frame, grid, fieldName, fdef, lut, entry) {
      validateGrid(grid, frame, fdef);
      this._frame = frame; this._grid = grid; this._lut = lut; this.field = fieldName; this.fdef = fdef; this.entry = entry || null;
      this._nearest = fdef.interpolation === 'nearest'; this._lo = fdef.lo; this._hi = fdef.hi; this._legend = fdef.legend;
      this._redraw();
    },
    clear: function () { this._frame = null; this.field = null; this.fdef = null; this.entry = null; this._redraw(); },
    hasFrame: function () { return !!this._frame; },
    _redraw: function () { for (var k in this._tiles) { var t = this._tiles[k]; if (t.el && t.coords) this._draw(t.el, t.coords); } },
    createTile: function (coords, done) {
      var el = document.createElement('canvas'); el.width = TILE; el.height = TILE;
      this._draw(el, coords);
      setTimeout(function () { done(null, el); }, 0);
      return el;
    },
    _value: function (code) { return this._lo + (code - 1) / 254 * (this._hi - this._lo); },
    // THE sampler: codes along one fractional grid row r (0..rows-1, caller-checked) at the periodic
    // column positions colPos[0..n) (each 0 <= cpos < cols), written to out[off..off+n). Bilinear over
    // the present neighbours (q 0 = absent; mostly absent -> 0), nearest for discontinuous fields (Tp).
    // 0 means "no value". Both the tile drawing and the readout go through here.
    _codeRow: function (r, colPos, n, out, off) {
      var f = this._frame, cols = f.cols, q = f.q, i;
      if (this._nearest) {
        var base = Math.round(r) * cols;
        for (i = 0; i < n; i++) out[off + i] = q[base + (Math.round(colPos[i]) % cols)];
        return;
      }
      var r0 = Math.floor(r), r1 = r0 + 1 < f.rows ? r0 + 1 : r0, fr = r - r0, w0 = 1 - fr, b0 = r0 * cols, b1 = r1 * cols;
      for (i = 0; i < n; i++) {
        var cpos = colPos[i], c0 = Math.floor(cpos), c1 = c0 + 1 === cols ? 0 : c0 + 1, fc = cpos - c0;
        var a = q[b0 + c0], b = q[b0 + c1], c = q[b1 + c0], d = q[b1 + c1];
        var w = 0, acc = 0, wt;
        if (a) { wt = w0 * (1 - fc); acc += a * wt; w += wt; }
        if (b) { wt = w0 * fc; acc += b * wt; w += wt; }
        if (c) { wt = fr * (1 - fc); acc += c * wt; w += wt; }
        if (d) { wt = fr * fc; acc += d * wt; w += wt; }
        out[off + i] = w < 0.25 ? 0 : acc / w;
      }
    },
    _code: function (r, cpos) { this._one[0] = cpos; this._codeRow(r, this._one, 1, this._oneOut, 0); return this._oneOut[0]; },
    valueAt: function (lat, lon) {
      if (!this._frame) return null;
      var g = this._grid, f = this._frame, r = (g.lat0 - lat) / -g.dlat;
      if (!(r >= 0 && r <= f.rows - 1)) return null;
      var c = (lon - g.lon0) / g.dlon, cpos = ((c % f.cols) + f.cols) % f.cols;
      var code = this._code(r, cpos);
      return code ? this._value(code) : null;
    },
    // Codes for one Web-Mercator tile (pixel centres) into out[TILE*TILE]; 0 = transparent. Column
    // positions are periodic, so wrapped and unwrapped tile x (world copies) sample identically.
    tileCodes: function (coords, out) {
      var f = this._frame, g = this._grid, cols = f.cols, rows = f.rows;
      var n = TILE * Math.pow(2, coords.z), x0 = coords.x * TILE, y0 = coords.y * TILE, colPos = this._colPos;
      for (var px = 0; px < TILE; px++) {
        var c = ((x0 + px + 0.5) / n * 360 - 180 - g.lon0) / g.dlon;
        colPos[px] = ((c % cols) + cols) % cols;
      }
      var k = 0;
      for (var py = 0; py < TILE; py++) {
        var lat = Math.atan(Math.sinh(Math.PI - 2 * Math.PI * (y0 + py + 0.5) / n)) * 180 / Math.PI;
        var r = (g.lat0 - lat) / -g.dlat;
        if (r >= 0 && r <= rows - 1) this._codeRow(r, colPos, TILE, out, k); else out.fill(0, k, k + TILE);
        k += TILE;
      }
      return out;
    },
    _draw: function (el, coords) {
      var ctx = el.getContext('2d');
      if (!this._frame) { ctx.clearRect(0, 0, TILE, TILE); return; }
      var codes = this.tileCodes(coords, this._codes), lut = this._lut;
      var lo = this._lo, hi = this._hi, L0 = this._legend[0], scale = 255 / (this._legend[1] - L0);
      var img = ctx.createImageData(TILE, TILE), d = img.data;
      for (var i = 0, k = 0; i < codes.length; i++, k += 4) {
        var code = codes[i];
        if (!code) continue;
        var t = Math.round((lo + (code - 1) / 254 * (hi - lo) - L0) * scale);
        t = t < 0 ? 0 : t > 255 ? 255 : t;
        d[k] = lut[t * 3]; d[k + 1] = lut[t * 3 + 1]; d[k + 2] = lut[t * 3 + 2]; d[k + 3] = 255;
      }
      ctx.putImageData(img, 0, 0);
    }
  });

  // ---- controller ----
  function Overlay(map, opts) {
    this.map = map; this.opts = opts;
    // Keys inside latest.json / the manifest are bucket-absolute (gfswave/0p25/v1/...); the
    // configured base is the v1 prefix URL, so resolve keys against the bucket root.
    this.base = String(opts.base).replace(/\/+$/, '');
    this.root = this.base.replace(/\/gfswave\/0p25\/v1$/, '');
    this.field = null; this.layer = null; this.manifest = null; this.pointer = null; this.pointerAt = 0; this.newerRun = null;
    this.abort = null; this.readout = null; this.frameIndex = null; this.res = null; this.sheet = null; this.last = null;
    this._listeners = []; this._attributed = false; this.collapsed = undefined; this._touch = null;
    var s = saved();
    this.opacity = typeof s.opacity === 'number' && s.opacity >= 0.2 && s.opacity <= 1 ? s.opacity : 0.65;
  }
  Overlay.prototype.mount = function (fieldName) {
    var self = this;
    if (!this.map.getPane('modelPane')) {
      var pane = this.map.createPane('modelPane'); pane.style.zIndex = 250; pane.style.pointerEvents = 'none';
    }
    this.field = fieldName;
    this.abortAll();
    this.abort = new AbortController();
    var sig = this.abort.signal;
    if (!this.layer) {
      this.layer = new ModelGridLayer({ opacity: this.opacity }).addTo(this.map);
      this._bindReadout();
      this._bindMap();
    } else if (this.layer.field !== fieldName) {
      this.layer.clear();                                  // never one field's picture under another's label
    }
    this.render({ state: 'loading' });
    this._loadManifest(sig).then(function (m) {
      if (!m.fields[fieldName] || !RAMPS[fieldName]) throw new Error('layer "' + fieldName + '" is not in this run');
      self.frameIndex = pickFrame(m);
      self.res = wantHalf(self.map.getZoom(), self._dims().w) ? 'half' : 'full';
      return self._loadCurrent(sig);
    }).catch(function (err) { self._fail(sig, err); });
  };
  // Fetch + draw the current (field, frame, resolution). Whatever is drawn stays until the new frame lands.
  Overlay.prototype._loadCurrent = function (sig) {
    var self = this, m = this.manifest, field = this.field, fr = m.frames[this.frameIndex], half = this.res === 'half';
    var url = this.root + '/' + frameKey(m, fr, field, half);
    return decodeFrame(url, sig).then(function (frame) {
      if (sig.aborted) return;
      self.layer.setFrame(frame, half ? m.grid_half : m.grid, field, m.fields[field], buildRamp(RAMPS[field]), fr);
      self._attribute();
      self.render({ state: 'ready', frame: fr });
    });
  };
  Overlay.prototype._fail = function (sig, err) {
    if (sig && sig.aborted) return;
    try { this.render({ state: 'error', message: err && err.message ? err.message : String(err) }); } catch (e) { /* host gone */ }
  };
  // latest.json -> manifest. A pointer older than POINTER_RECHECK_MS is re-read; while a frame is on
  // the map the session stays pinned to its run (a newer one is only announced), otherwise it adopts it.
  Overlay.prototype._loadManifest = function (sig) {
    var self = this;
    if (this.manifest && Date.now() - this.pointerAt < POINTER_RECHECK_MS) return Promise.resolve(this.manifest);
    return fetch(this.base + '/latest.json', { signal: sig, cache: 'no-cache' }).then(function (r) {
      if (!r.ok) throw new Error('latest ' + r.status);
      return r.json();
    }).then(function (ptr) {
      if (!ptr || !ptr.complete || !ptr.run || !ptr.manifest) throw new Error('published run is not complete');
      self.pointerAt = Date.now();
      if (self.manifest && self.manifest.run === ptr.run) { self.pointer = ptr; return self.manifest; }
      if (self.manifest && self.layer && self.layer.hasFrame()) { self.newerRun = ptr.run; return self.manifest; }
      return fetch(self.root + '/' + ptr.manifest, { signal: sig }).then(function (r) {
        if (!r.ok) throw new Error('manifest ' + r.status);
        return r.json();
      }).then(function (m) {
        validateManifest(m);
        if (m.run !== ptr.run) throw new Error('manifest/pointer run mismatch');
        self.manifest = m; self.pointer = ptr; self.newerRun = null;
        return m;
      });
    });
  };
  // The container's real size: the site resizes #map by script and Leaflet's cached getSize() can lag.
  Overlay.prototype._dims = function () {
    var c = this.map.getContainer(), s = this.map.getSize();
    return { w: c.clientWidth || s.x, h: c.clientHeight || s.y };
  };
  Overlay.prototype._attribute = function () {
    if (this._attributed) return;
    this._attributed = true;
    if (this.map.attributionControl) this.map.attributionControl.addAttribution(ATTRIBUTION);
    this.map.getContainer().classList.add('ov-on');
    this._sizeAttribution();
  };
  // While On the attribution may wrap, but only inside its own box: never over the zoom control.
  Overlay.prototype._sizeAttribution = function () {
    if (this._attributed) this.map.getContainer().style.setProperty('--ov-attr-max', Math.max(120, this._dims().w - 60) + 'px');
  };
  Overlay.prototype._unattribute = function () {
    if (!this._attributed) return;
    this._attributed = false;
    if (this.map.attributionControl) this.map.attributionControl.removeAttribution(ATTRIBUTION);
    var c = this.map.getContainer(); c.classList.remove('ov-on'); c.style.removeProperty('--ov-attr-max');
  };
  Overlay.prototype._on = function (ev, fn) { this.map.on(ev, fn); this._listeners.push([ev, fn]); };
  Overlay.prototype._bindMap = function () {
    var self = this;
    this._on('zoomend', function () { self._checkRes(); });
    this._on('resize', function () { self._sizeAttribution(); self._checkRes(); if (self.last) self.render(self.last); });
  };
  Overlay.prototype._checkRes = function () {
    if (!this.layer || !this.layer.hasFrame() || !this.manifest || this.frameIndex === null || !this.field) return;
    var z = this.map.getZoom(), w = this._dims().w, want = this.res;
    if (this.res === 'full' && wantHalf(z, w)) want = 'half';
    else if (this.res === 'half' && wantFull(z, w)) want = 'full';
    if (want === this.res) return;
    this.res = want;
    this.abortAll(); this.abort = new AbortController();
    var sig = this.abort.signal, self = this;
    this._loadCurrent(sig).catch(function (err) { self._fail(sig, err); });
  };
  Overlay.prototype.abortAll = function () { if (this.abort) { this.abort.abort(); this.abort = null; } };
  Overlay.prototype.unmount = function () {
    var self = this;
    this.abortAll();
    if (this.layer) { this.map.removeLayer(this.layer); this.layer = null; }
    this._unattribute();
    this._unbindReadout();
    this._listeners.forEach(function (l) { self.map.off(l[0], l[1]); }); this._listeners = [];
    this._removeSheet();
    this.field = null; this.frameIndex = null; this.res = null; this.last = null; this.collapsed = undefined;
    clear(this.opts.panel);
  };
  Overlay.prototype.setOpacity = function (v) {
    v = Math.max(0.2, Math.min(1, v));
    this.opacity = v; save({ opacity: v });
    if (this.layer) this.layer.setOpacity(v);
  };

  // Readout: hover only on hover-capable pointers, long-press (450 ms) on touch; a plain tap shows
  // nothing. Marker events are unaffected (the pane has no pointer events); controls are ignored.
  Overlay.prototype._bindReadout = function () {
    var self = this, map = this.map, el = mk('div', 'ov-readout'); el.hidden = true;
    map.getContainer().appendChild(el); this.readout = el;
    var pressTimer = null, lastTouch = 0;
    function overControl(ev) { var t = ev && ev.target; return !!(t && t.closest && t.closest('.leaflet-control, .ov-sheet')); }
    function show(latlng, pt) {
      var layer = self.layer, v = layer && layer.fdef ? layer.valueAt(latlng.lat, latlng.lng) : null;
      if (v === null || v === undefined) { el.hidden = true; return; }
      var u = unitOf(layer.field, self.opts.getUnit()), f = layer.fdef;
      var txt = u.f(v).toFixed(u.d) + ' ' + u.label;
      if (v <= f.lo + 1e-9) txt = '≤ ' + txt; else if (v >= f.hi - 1e-9) txt = '≥ ' + txt;
      el.textContent = txt; el.style.left = pt.x + 'px'; el.style.top = pt.y + 'px'; el.hidden = false;
    }
    var hoverable = !window.matchMedia || window.matchMedia('(hover: hover) and (pointer: fine)').matches;
    if (hoverable) {
      this._on('mousemove', function (e) {
        var oe = e.originalEvent;
        if (!oe || oe.pointerType === 'touch' || Date.now() - lastTouch < 1000 || overControl(oe)) { el.hidden = true; return; }
        show(e.latlng, e.containerPoint);
      });
      this._on('mouseout', function () { el.hidden = true; });
    }
    this._on('movestart', function () { el.hidden = true; if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; } });
    var c = map.getContainer();
    function ts(e) {
      lastTouch = Date.now();
      if (e.touches.length !== 1 || overControl(e)) return;
      var t = e.touches[0];
      pressTimer = setTimeout(function () { pressTimer = null; var pt = map.mouseEventToContainerPoint(t); show(map.containerPointToLatLng(pt), pt); }, 450);
    }
    function te() { lastTouch = Date.now(); if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; } setTimeout(function () { el.hidden = true; }, 1500); }
    c.addEventListener('touchstart', ts, { passive: true }); c.addEventListener('touchend', te, { passive: true }); c.addEventListener('touchmove', te, { passive: true });
    this._touch = [c, ts, te, function () { if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; } }];
  };
  Overlay.prototype._unbindReadout = function () {
    if (this._touch) {
      var c = this._touch[0];
      c.removeEventListener('touchstart', this._touch[1]); c.removeEventListener('touchend', this._touch[2]); c.removeEventListener('touchmove', this._touch[2]);
      this._touch[3](); this._touch = null;
    }
    if (this.readout) { this.readout.remove(); this.readout = null; }
  };

  // ---- panel: inside the top-left control on desktops; a sheet on the map's bottom edge on phones ----
  // Compact = the site's mobile breakpoint, or a short map on a touch device (desktop windows are
  // often short too: the site caps the map at 48 % of the viewport height).
  Overlay.prototype.isCompact = function () {
    var d = this._dims(), coarse = !!(window.matchMedia && window.matchMedia('(pointer: coarse)').matches);
    return d.w < 576 || (d.h < 400 && coarse);
  };
  Overlay.prototype._host = function () {
    if (this.isCompact()) {
      if (!this.sheet) {
        var sh = mk('div', 'ov-sheet'); sh.setAttribute('role', 'region'); sh.setAttribute('aria-label', 'Model overlay');
        L.DomEvent.disableClickPropagation(sh); L.DomEvent.disableScrollPropagation(sh);
        this.map.getContainer().appendChild(sh); this.sheet = sh;
      }
      clear(this.opts.panel);
      return this.sheet;
    }
    this._removeSheet();
    return this.opts.panel;
  };
  Overlay.prototype._removeSheet = function () {
    if (this.sheet) { this.sheet.remove(); this.sheet = null; }
    var c = this.map.getContainer(); c.classList.remove('ov-sheet-open');
    c.style.removeProperty('--ov-sheet-h'); c.style.removeProperty('--ov-sheet-left');
  };
  // The sheet sits on the bottom edge to the RIGHT of the zoom/Home column (that column is never
  // covered or moved); CSS lifts only the attribution corner above it by --ov-sheet-h.
  Overlay.prototype._layoutSheet = function () {
    if (!this.sheet) return;
    var c = this.map.getContainer(); c.classList.add('ov-sheet-open');
    c.style.setProperty('--ov-sheet-left', (this._stackWidth() + 6) + 'px');
    c.style.setProperty('--ov-sheet-h', this.sheet.offsetHeight + 'px');
  };
  Overlay.prototype._stackWidth = function () {
    var corners = this.map._controlCorners, el = corners && corners.bottomleft;
    return el && el.offsetWidth ? el.offsetWidth : 45;
  };
  Overlay.prototype.refresh = function () { if (this.last && this.layer) this.render(this.last); };
  Overlay.prototype.render = function (st) {
    this.last = st;
    var self = this, host = this._host(), compact = host === this.sheet, m = this.manifest, unit = this.opts.getUnit();
    var mapH = this._dims().h;
    clear(host);
    host.setAttribute('aria-live', 'polite');
    if (st.state === 'loading') { host.appendChild(mk('div', 'ov-meta', 'Loading model frame…')); this._layoutSheet(); return; }
    if (st.state === 'error') {
      var e = mk('div', 'ov-err', 'Overlay unavailable: ' + st.message + ' ');
      var rb = mk('button', 'ov-retry', 'Retry'); rb.type = 'button';
      rb.addEventListener('click', function () { if (self.field) self.mount(self.field); });
      e.appendChild(rb); host.appendChild(e); this._layoutSheet(); return;
    }
    var field = this.layer.field, f = m.fields[field], fdesc = (m.model && m.model.fields && m.model.fields[field]) || {};
    var label = fdesc.label || field;
    var runLabel = m.run_utc.replace('T', ' ').replace(':00:00Z', 'Z');
    var hours = Math.round((Date.parse(st.frame.valid_utc) - Date.parse(m.run_utc)) / 3.6e6);
    var validLocal = this.opts.fmtTime(st.frame.valid_utc, this.opts.tz) + ' ' + this.opts.tzAbbr(st.frame.valid_utc, this.opts.tz);
    // Expanded: the whole sheet <= 40 % of the map (the desktop panel's details likewise); the details
    // scroll inside that. Too little room (short landscape maps) -> the one-line summary only.
    var cap = Math.floor(mapH * 0.4);
    if (this.collapsed === undefined) this.collapsed = compact;
    var collapsed = !!this.collapsed;
    var head = mk('div', 'ov-row ov-head');
    var btn = mk('button', 'ov-toggle', collapsed ? '▸' : '▾'); btn.type = 'button';
    btn.setAttribute('aria-label', collapsed ? 'Show overlay details' : 'Hide overlay details');
    btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true'); btn.setAttribute('aria-controls', 'ovDetails');
    btn.addEventListener('click', function () { self.collapsed = !self.collapsed; self.render(st); });
    head.appendChild(btn);
    var title = mk('span', 'ov-title', label + ' · ' + validLocal + ' (+' + hours + ' h)');
    head.appendChild(title);
    host.appendChild(head);
    // Room for the details = the cap minus everything else in the host (head, paddings, margins),
    // measured from the real layout once the body is in place (see the clamp at the end).
    if (compact && cap - host.offsetHeight - 8 < 40) { head.removeChild(btn); collapsed = true; }
    if (collapsed) { this._layoutSheet(); return; }
    title.textContent = label + ' — ' + String(m.model && m.model.name || 'NOAA GFS-Wave').split(' + ')[0];
    var body = mk('div', 'ov-details'); body.id = 'ovDetails'; body.style.maxHeight = cap + 'px'; host.appendChild(body);
    var meta = mk('div', 'ov-meta');
    meta.appendChild(mk('b', null, 'Valid: ')); meta.appendChild(document.createTextNode(validLocal + ' (+' + hours + ' h)'));
    meta.appendChild(mk('br')); meta.appendChild(mk('b', null, 'Run: ')); meta.appendChild(document.createTextNode(runLabel + ' (UTC)'));
    var pc = typeof this.opts.pageCycle === 'function' ? this.opts.pageCycle() : this.opts.pageCycle;
    if (pc && pc.model === 'SWAN') meta.appendChild(document.createTextNode(' — the forecast table is a PacIOOS SWAN run'));
    else if (pc && pc.run && pc.run !== m.run) meta.appendChild(document.createTextNode(' — the forecast table is on run ' + pc.run));
    body.appendChild(meta);
    var age = (Date.now() - Date.parse(this.pointer.published_utc)) / 1000;
    if (age > STALE_AFTER_S) body.appendChild(mk('div', 'ov-warn', 'Model overlay data is stale (published ' + Math.round(age / 3600) + ' h ago).'));
    if (this.newerRun) body.appendChild(mk('div', 'ov-warn', 'A newer run (' + this.newerRun + ') is available: switch the overlay Off and back On to load it.'));
    // legend over the LEGEND range in the site's units (the encoding range is wider; extremes clamp)
    var leg = mk('div', 'ov-legend'), cv = mk('canvas'); cv.width = 256; cv.height = 1; leg.appendChild(cv);
    var lut = buildRamp(RAMPS[field]), ctx = cv.getContext('2d'), im = ctx.createImageData(256, 1);
    for (var i = 0; i < 256; i++) { im.data[i * 4] = lut[i * 3]; im.data[i * 4 + 1] = lut[i * 3 + 1]; im.data[i * 4 + 2] = lut[i * 3 + 2]; im.data[i * 4 + 3] = 255; }
    ctx.putImageData(im, 0, 0);
    var ticks = mk('div', 'ov-ticks'), tk = legendTicks(field, f, unit);
    tk.forEach(function (t, k) {
      var s = mk('span', k === 0 ? 'ov-first' : k === tk.length - 1 ? 'ov-last' : '', t.label);
      s.style.left = (t.pos * 100).toFixed(2) + '%'; ticks.appendChild(s);
    });
    leg.appendChild(ticks); body.appendChild(leg);
    var row = mk('div', 'ov-row'), lab = mk('label', null, 'Opacity ');
    var rng = mk('input'); rng.type = 'range'; rng.min = '0.2'; rng.max = '1'; rng.step = '0.05'; rng.value = String(this.opacity);
    rng.setAttribute('aria-label', 'Overlay opacity');
    rng.addEventListener('input', function () { self.setOpacity(parseFloat(rng.value)); });
    lab.appendChild(rng); row.appendChild(lab); body.appendChild(row);
    body.appendChild(mk('div', 'ov-note',
      (field === 'tp' ? 'Peak period Tp (GRIB PERPW = 1/fp). ' : field === 'wind' ? 'GFS wind at 10 m; legend top 60 kt. ' : '') +
      'GFS-Wave 0.25° (~28 km) grid — display smoothing is not extra detail. Hover or long-press the map for values. ' +
      String(m.model && m.model.attribution || '')));
    if (compact) {                                       // the WHOLE sheet <= cap: clamp the details to what is left
      var chrome = host.offsetHeight - body.offsetHeight;
      body.style.maxHeight = Math.max(40, cap - chrome - 2) + 'px';
    }
    this._layoutSheet();
  };

  window.AllshoreOverlay = {
    create: function (map, opts) { return new Overlay(map, opts); },
    _internals: { buildRamp: buildRamp, unitOf: unitOf, ModelGridLayer: ModelGridLayer, Overlay: Overlay, RAMPS: RAMPS,
      frameKey: frameKey, pickFrame: pickFrame, validateManifest: validateManifest, validateGrid: validateGrid,
      wantHalf: wantHalf, wantFull: wantFull, legendTicks: legendTicks, tilePixelLatLng: tilePixelLatLng, pad3: pad3 }
  };
})();
