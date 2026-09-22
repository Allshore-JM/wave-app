/* Allshore Surf model overlay (Phase 2: one static frame). Loaded on demand; never on page load.
 *
 * Data contract (manifest schema 2 from tools/model_frames): 8-bit greyscale PNG frames,
 * q = 0 missing, value = lo + (q - 1) / 254 * (hi - lo), q = 1 means "<= lo", q = 255 ">= hi";
 * grid centre-registered from (+90 N, -180 E), longitude periodic; grid_half = full[::2, ::2].
 * Frames are sampled per canvas tile (inverse Web-Mercator) in a dedicated 'modelPane'
 * (z 250, pointer-events none), so markers and gestures are untouched.
 */
(function () {
  'use strict';

  var KT = 1852 / 3600;
  var SESSION_KEY = 'allshore.overlay.v1';
  var STALE_AFTER_S = 9 * 3600;
  var RAMPS = {
    hs:   [[0,'#0b2c6b'],[0.15,'#1f6fd6'],[0.3,'#19c3e6'],[0.45,'#3fd96b'],[0.6,'#f2e33a'],[0.75,'#f5901f'],[0.9,'#e02020'],[1,'#a3129e']],
    tp:   [[0,'#2a1f7a'],[0.25,'#2e7ed8'],[0.5,'#38c9a8'],[0.7,'#c8e63c'],[0.85,'#f7a52b'],[1,'#e8321f']],
    wind: [[0,'#e8f1ff'],[0.2,'#8cc4ff'],[0.4,'#3aa35a'],[0.6,'#f0d433'],[0.8,'#f0731f'],[1,'#b00f3a']]
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

  // ---- units (site preference: 'US' | 'Metric') ----
  function unitOf(field, unit) {
    if (field === 'hs') return unit === 'Metric' ? { label: 'm', f: function (v) { return v; }, d: 1 } : { label: 'ft', f: function (v) { return v * 3.28084; }, d: 1 };
    if (field === 'tp') return { label: 's', f: function (v) { return v; }, d: 1 };
    return unit === 'Metric' ? { label: 'km/h', f: function (v) { return v * 3.6; }, d: 0 } : { label: 'mph', f: function (v) { return v * 2.23694; }, d: 0 };
  }

  // ---- frame decoding ----
  function decodeFrame(url, signal) {
    // PNG -> Uint8Array of codes (R channel). Needs CORS (bucket policy) for getImageData.
    return fetch(url, { signal: signal, mode: 'cors' }).then(function (r) {
      if (!r.ok) throw new Error('frame ' + r.status);
      return r.blob();
    }).then(function (blob) {
      return createImageBitmap(blob);
    }).then(function (bmp) {
      var c = document.createElement('canvas'); c.width = bmp.width; c.height = bmp.height;
      var ctx = c.getContext('2d', { willReadFrequently: true });
      ctx.drawImage(bmp, 0, 0);
      var w = bmp.width, h = bmp.height;                     // read BEFORE close(): a closed bitmap reports 0x0
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
      L.GridLayer.prototype.initialize.call(this, { pane: 'modelPane', tileSize: 256, updateWhenZooming: false,
        updateWhenIdle: true, keepBuffer: 1, opacity: opts.opacity, className: 'ov-tiles' });
      this._frame = null; this._grid = null; this._lut = null; this._nearest = false; this._lo = 0; this._hi = 1;
      this._legend = [0, 1];
    },
    setFrame: function (frame, grid, field, lut) {
      this._frame = frame; this._grid = grid; this._lut = lut;
      this._nearest = field.interpolation === 'nearest'; this._lo = field.lo; this._hi = field.hi; this._legend = field.legend;
      for (var k in this._tiles) if (this._tiles[k].el && this._tiles[k].coords) this._draw(this._tiles[k].el, this._tiles[k].coords);
    },
    createTile: function (coords, done) {
      var el = document.createElement('canvas'); el.width = 256; el.height = 256;
      this._draw(el, coords);
      setTimeout(function () { done(null, el); }, 0);
      return el;
    },
    valueAt: function (lat, lon) {
      if (!this._frame) return null;
      var g = this._grid, f = this._frame;
      var r = (g.lat0 - lat) / -g.dlat, c = (lon - g.lon0) / g.dlon;
      var ri = Math.round(r), ci = Math.round(c);
      if (ri < 0 || ri >= f.rows) return null;
      ci = ((ci % f.cols) + f.cols) % f.cols;
      var q = f.q[ri * f.cols + ci];
      return q ? this._value(q) : null;
    },
    _value: function (q) { return this._lo + (q - 1) / 254 * (this._hi - this._lo); },
    _draw: function (el, coords) {
      var ctx = el.getContext('2d');
      if (!this._frame) { ctx.clearRect(0, 0, 256, 256); return; }
      var f = this._frame, g = this._grid, lut = this._lut, nearest = this._nearest;
      var z = coords.z, n = 256 * Math.pow(2, z), x0 = coords.x * 256, y0 = coords.y * 256;
      var img = ctx.createImageData(256, 256), d = img.data;
      var rowPos = new Float64Array(256), colPos = new Float64Array(256);
      for (var py = 0; py < 256; py++) {
        var m = Math.PI - 2 * Math.PI * (y0 + py + 0.5) / n;
        var lat = Math.atan(Math.sinh(m)) * 180 / Math.PI;
        rowPos[py] = (g.lat0 - lat) / -g.dlat;                  // fractional grid row
      }
      for (var px = 0; px < 256; px++) {
        var lon = (x0 + px + 0.5) / n * 360 - 180;
        var c = (lon - g.lon0) / g.dlon;
        colPos[px] = ((c % f.cols) + f.cols) % f.cols;           // periodic longitude
      }
      var cols = f.cols, rows = f.rows, q = f.q, lo = this._lo, hi = this._hi, L0 = this._legend[0], L1 = this._legend[1];
      var scale = 255 / (L1 - L0);
      var k = 0;
      for (py = 0; py < 256; py++) {
        var r = rowPos[py];
        if (r < 0 || r > rows - 1) { k += 256 * 4; continue; }
        var r0 = Math.floor(r), r1 = Math.min(rows - 1, r0 + 1), fr = r - r0, rr = Math.round(r);
        for (px = 0; px < 256; px++, k += 4) {
          var cpos = colPos[px], code;
          if (nearest) {
            code = q[rr * cols + (Math.round(cpos) % cols)];
            if (!code) continue;
            var val = lo + (code - 1) / 254 * (hi - lo);
          } else {
            var c0 = Math.floor(cpos), c1 = (c0 + 1) % cols, fc = cpos - c0;
            var a = q[r0 * cols + c0], b = q[r0 * cols + c1], cc = q[r1 * cols + c0], dd = q[r1 * cols + c1];
            var w = 0, acc = 0, wt;
            if (a) { wt = (1 - fr) * (1 - fc); acc += a * wt; w += wt; }
            if (b) { wt = (1 - fr) * fc; acc += b * wt; w += wt; }
            if (cc) { wt = fr * (1 - fc); acc += cc * wt; w += wt; }
            if (dd) { wt = fr * fc; acc += dd * wt; w += wt; }
            if (w < 0.25) continue;                               // mostly missing -> transparent
            code = acc / w;
            val = lo + (code - 1) / 254 * (hi - lo);
          }
          var t = Math.max(0, Math.min(255, Math.round((val - L0) * scale)));
          d[k] = lut[t * 3]; d[k + 1] = lut[t * 3 + 1]; d[k + 2] = lut[t * 3 + 2]; d[k + 3] = 255;
        }
      }
      ctx.putImageData(img, 0, 0);
    }
  });

  // ---- controller ----
  function Overlay(map, opts) {
    this.map = map; this.opts = opts;
    // Keys inside latest.json / the manifest are bucket-absolute (gfswave/0p25/v1/...); the
    // configured base is the v1 prefix URL, so resolve keys against the bucket root.
    this.root = String(opts.base).replace(/\/+$/, '').replace(/\/gfswave\/0p25\/v1$/, ''); this.field = null; this.layer = null; this.manifest = null; this.pointer = null;
    this.abort = null; this.readout = null; this.frameIndex = null; this._listeners = [];
    var s = saved();
    this.opacity = typeof s.opacity === 'number' ? s.opacity : 0.65;
  }
  Overlay.prototype.mount = function (fieldName) {
    var self = this;
    if (!this.map.getPane('modelPane')) {
      var pane = this.map.createPane('modelPane'); pane.style.zIndex = 250; pane.style.pointerEvents = 'none';
    }
    this.field = fieldName;
    this.abortAll();
    this.abort = new AbortController();
    if (!this.layer) {
      this.layer = new ModelGridLayer({ opacity: this.opacity }).addTo(this.map);
      this._bindReadout();
    }
    this.render({ state: 'loading' });
    var base = this.opts.base, sig = this.abort.signal;
    var p = this.manifest ? Promise.resolve(this.manifest)
      : fetch(base + '/latest.json', { signal: sig, cache: 'no-cache' }).then(function (r) { if (!r.ok) throw new Error('latest ' + r.status); return r.json(); })
        .then(function (ptr) {
          if (!ptr.complete) throw new Error('published run is not complete');
          self.pointer = ptr;
          return fetch(self.root + '/' + ptr.manifest, { signal: sig }).then(function (r) { if (!r.ok) throw new Error('manifest ' + r.status); return r.json(); });
        }).then(function (m) { self.manifest = m; return m; });
    p.then(function (m) {
      var idx = self.pickFrame(m);
      self.frameIndex = idx;
      var fr = m.frames[idx], half = self.useHalf();
      var url = self.root + '/' + fr.files[fieldName][half ? 'half' : 'full'];
      return decodeFrame(url, sig).then(function (frame) { return { m: m, fr: fr, frame: frame, half: half }; });
    }).then(function (r) {
      if (sig.aborted) return;
      var fdef = r.m.fields[fieldName];
      self.layer.setFrame(r.frame, r.half ? r.m.grid_half : r.m.grid, fdef, buildRamp(RAMPS[fieldName]));
      self.map.attributionControl.addAttribution(self.attribution());
      self.render({ state: 'ready', frame: r.fr });
    }).catch(function (err) {
      if (sig.aborted) return;
      self.render({ state: 'error', message: err && err.message ? err.message : String(err) });
    });
  };
  Overlay.prototype.pickFrame = function (m) {
    var now = Date.now();
    for (var i = 0; i < m.frames.length; i++) if (Date.parse(m.frames[i].valid_utc) >= now) return i;
    return m.frames.length - 1;
  };
  Overlay.prototype.useHalf = function () {
    return this.map.getSize().x < 700 || this.map.getZoom() < 3.5;
  };
  Overlay.prototype.attribution = function () {
    return 'Overlay: <a href="https://polar.ncep.noaa.gov/waves/" target="_blank" rel="noopener">NOAA/NCEP GFS-Wave</a> &amp; GFS via NOAA Open Data (not an official NWS product)';
  };
  Overlay.prototype.abortAll = function () { if (this.abort) { this.abort.abort(); this.abort = null; } };
  Overlay.prototype.unmount = function () {
    this.abortAll();
    if (this.layer) { this.map.removeLayer(this.layer); this.layer = null; this.map.attributionControl.removeAttribution(this.attribution()); }
    this._unbindReadout();
    this.field = null; this.frameIndex = null;
    this.opts.panel.innerHTML = '';
  };
  Overlay.prototype.setOpacity = function (v) { this.opacity = v; save({ opacity: v }); if (this.layer) this.layer.setOpacity(v); };

  // readout: hover on desktop, long-press on touch; marker events are unaffected (pane has no pointer events)
  Overlay.prototype._bindReadout = function () {
    var self = this, map = this.map, el = document.createElement('div'); el.className = 'ov-readout'; el.hidden = true;
    map.getContainer().appendChild(el); this.readout = el;
    var pressTimer = null;
    function show(latlng, pt) {
      var v = self.layer && self.layer.valueAt(latlng.lat, latlng.lng);
      if (v === null || v === undefined) { el.hidden = true; return; }
      var u = unitOf(self.field, self.opts.getUnit()), f = self.manifest.fields[self.field];
      var txt = u.f(v).toFixed(u.d) + ' ' + u.label;
      if (v <= f.lo + 1e-9) txt = '≤ ' + txt; else if (v >= f.hi - 1e-9) txt = '≥ ' + txt;
      el.textContent = txt; el.style.left = pt.x + 'px'; el.style.top = pt.y + 'px'; el.hidden = false;
    }
    function on(ev, fn) { map.on(ev, fn); self._listeners.push([ev, fn]); }
    on('mousemove', function (e) { if (!e.originalEvent || e.originalEvent.pointerType === 'touch') return; show(e.latlng, e.containerPoint); });
    on('mouseout', function () { el.hidden = true; });
    on('movestart', function () { el.hidden = true; if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; } });
    var c = map.getContainer();
    function ts(e) { if (e.touches.length !== 1) return; var t = e.touches[0]; pressTimer = setTimeout(function () {
      var pt = map.mouseEventToContainerPoint(t); show(map.containerPointToLatLng(pt), pt); }, 450); }
    function te() { if (pressTimer) { clearTimeout(pressTimer); pressTimer = null; } setTimeout(function () { el.hidden = true; }, 1500); }
    c.addEventListener('touchstart', ts, { passive: true }); c.addEventListener('touchend', te, { passive: true }); c.addEventListener('touchmove', te, { passive: true });
    this._touch = [c, ts, te];
  };
  Overlay.prototype._unbindReadout = function () {
    var self = this;
    this._listeners.forEach(function (l) { self.map.off(l[0], l[1]); }); this._listeners = [];
    if (this._touch) { var c = this._touch[0]; c.removeEventListener('touchstart', this._touch[1]); c.removeEventListener('touchend', this._touch[2]); c.removeEventListener('touchmove', this._touch[2]); this._touch = null; }
    if (this.readout) { this.readout.remove(); this.readout = null; }
  };

  // ---- panel ----
  Overlay.prototype.refresh = function () { if (this.last && this.last.state === 'ready' && this.layer) this.render(this.last); };
  Overlay.prototype.render = function (st) {
    this.last = st;
    var p = this.opts.panel, self = this, m = this.manifest, unit = this.opts.getUnit();
    while (p.firstChild) p.removeChild(p.firstChild);
    function div(cls, text) { var d = document.createElement('div'); d.className = cls; if (text !== undefined) d.textContent = text; p.appendChild(d); return d; }
    if (st.state === 'loading') { div('ov-meta', 'Loading model frame…'); return; }
    if (st.state === 'error') { div('ov-err', 'Overlay unavailable: ' + st.message); return; }
    var f = m.fields[this.field], fdesc = m.model.fields[this.field];
    var runLabel = m.run_utc.replace('T', ' ').replace(':00:00Z', 'Z');
    var hours = Math.round((Date.parse(st.frame.valid_utc) - Date.parse(m.run_utc)) / 3.6e6);
    var validLocal = this.opts.fmtTime(st.frame.valid_utc, this.opts.tz) + ' ' + this.opts.tzAbbr(st.frame.valid_utc, this.opts.tz);
    // Short maps (phones: ~260 px) get a one-line summary with a toggle; the expanded panel is
    // capped at 40 % of the map height so zoom/Home and the buoy panel stay reachable.
    var mapH = this.map.getSize().y, compact = mapH < 400 || this.map.getSize().x < 576;   // same breakpoint as the site's mobile layout
    if (this.collapsed === undefined) this.collapsed = compact;
    var head = div('ov-row');
    var btn = document.createElement('button'); btn.type = 'button'; btn.className = 'ov-toggle';
    btn.textContent = this.collapsed ? '▸' : '▾'; btn.setAttribute('aria-label', this.collapsed ? 'Show overlay details' : 'Hide overlay details');
    btn.addEventListener('click', function () { self.collapsed = !self.collapsed; self.render(st); });
    head.appendChild(btn);
    var t = document.createElement('span'); t.className = 'ov-title';
    t.textContent = this.collapsed ? fdesc.label + ' · ' + validLocal + ' (+' + hours + ' h)' : fdesc.label + ' — ' + m.model.name.split(' + ')[0];
    head.appendChild(t);
    if (this.collapsed) { p.style.maxHeight = ''; p.style.overflowY = ''; return; }
    p.style.maxHeight = Math.max(90, Math.floor(mapH * 0.35)) + 'px'; p.style.overflowY = 'auto';
    var meta = div('ov-meta');
    meta.innerHTML = '<b>Valid:</b> ' + escapeHtml(validLocal) + ' (+' + hours + ' h)<br><b>Run:</b> ' + escapeHtml(runLabel) + ' (UTC)' +
      (this.opts.pageCycle && this.opts.pageCycle !== m.run ? ' — forecast table is on ' + escapeHtml(this.opts.pageCycle) : '');
    var age = (Date.now() - Date.parse(this.pointer.published_utc)) / 1000;
    if (age > STALE_AFTER_S) div('ov-warn', 'Model overlay data is stale (published ' + Math.round(age / 3600) + ' h ago).');
    // legend in the site's units over the LEGEND range (encoding range is wider; extremes clamp)
    var leg = div('ov-legend'), cv = document.createElement('canvas'); cv.width = 256; cv.height = 1; leg.appendChild(cv);
    var lut = buildRamp(RAMPS[this.field]), ctx = cv.getContext('2d'), im = ctx.createImageData(256, 1);
    for (var i = 0; i < 256; i++) { im.data[i * 4] = lut[i * 3]; im.data[i * 4 + 1] = lut[i * 3 + 1]; im.data[i * 4 + 2] = lut[i * 3 + 2]; im.data[i * 4 + 3] = 255; }
    ctx.putImageData(im, 0, 0);
    var u = unitOf(this.field, unit), ticks = document.createElement('div'); ticks.className = 'ov-ticks';
    [0, 0.25, 0.5, 0.75, 1].forEach(function (t, k) { var v = f.legend[0] + t * (f.legend[1] - f.legend[0]); var s = document.createElement('span');
      s.textContent = u.f(v).toFixed(u.d) + (k === 4 ? '+ ' + u.label : ''); ticks.appendChild(s); });
    leg.appendChild(ticks);
    var row = div('ov-row'); var lab = document.createElement('span'); lab.textContent = 'Opacity'; row.appendChild(lab);
    var rng = document.createElement('input'); rng.type = 'range'; rng.min = '0.2'; rng.max = '1'; rng.step = '0.05'; rng.value = String(this.opacity);
    rng.addEventListener('input', function () { self.setOpacity(parseFloat(rng.value)); }); row.appendChild(rng);
    div('ov-note', (this.field === 'tp' ? 'Peak period Tp (GRIB PERPW = 1/fp). ' : this.field === 'wind' ? 'GFS wind at 10 m. ' : '') +
      'GFS-Wave 0.25° (~28 km) grid — display smoothing is not extra detail. Hover or long-press for values.');
  };
  function escapeHtml(s) { return String(s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }

  window.AllshoreOverlay = {
    create: function (map, opts) { return new Overlay(map, opts); },
    _internals: { buildRamp: buildRamp, unitOf: unitOf, ModelGridLayer: ModelGridLayer }
  };
})();
