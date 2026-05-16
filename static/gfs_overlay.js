/**
 * gfs_overlay.js  v4.2
 *
 * Animated GFS wave-height / swell-period / wind overlays.
 *
 * v4.2 fixes:
 *  - Land masking: pixels only coloured when NEAREST grid cell is valid ocean
 *  - image-rendering: pixelated prevents browser bilinear bleed at coastlines
 *  - Wind always renders colour fill; velocity particles added on top if available
 *  - 4x canvas upscale for smooth ocean gradients with crisp coast boundaries
 */
(function initGfsOverlays() {
  'use strict';

  const STEPS = [];
  for (let h = 0; h <= 120; h += 6) STEPS.push(h);

  const RESOLUTION       = 1.0;
  const UPSCALE          = 4;
  const PARALLEL_FETCHES = 3;
  const AUTOPLAY_MIN_READY = 2;
  const DEFAULT_SPEED_MS = 500;

  // == Colour palettes ======================================================
  const WAVE_PAL = [
    [ 0.00, [  0,  22,  92]],
    [ 0.50, [  0,  68, 172]],
    [ 1.00, [  0, 132, 218]],
    [ 1.50, [  0, 192, 176]],
    [ 2.00, [ 10, 198, 112]],
    [ 3.00, [130, 208,   0]],
    [ 4.00, [255, 228,   0]],
    [ 5.00, [255, 148,   0]],
    [ 6.00, [238,  68,   0]],
    [ 8.00, [215,   0,   0]],
    [10.00, [148,   0, 112]],
  ];

  const PERIOD_PAL = [
    [ 0,  [ 50,  50, 158]],
    [ 5,  [  0,  88, 215]],
    [ 8,  [  0, 168, 168]],
    [11,  [  0, 208,  88]],
    [14,  [168, 218,   0]],
    [17,  [255, 208,   0]],
    [21,  [255, 108,   0]],
    [25,  [208,   0,  58]],
  ];

  const WIND_PAL = [
    [ 0,  [  0,  40, 100]],
    [ 2,  [  0,  80, 160]],
    [ 5,  [  0, 160, 192]],
    [ 8,  [  0, 200, 160]],
    [11,  [160, 224,   0]],
    [14,  [255, 255,   0]],
    [18,  [255, 128,   0]],
    [22,  [255,   0,   0]],
    [28,  [180,   0,  90]],
  ];

  const WIND_COLOR_SCALE = [
    '#002864','#0050a0','#00a0c0','#00c8a0',
    '#a0e000','#ffff00','#ff8000','#ff0000',
  ];

  // == Smooth colour interpolation ==========================================
  function colorAt(pal, val) {
    if (val <= pal[0][0]) return pal[0][1];
    if (val >= pal[pal.length - 1][0]) return pal[pal.length - 1][1];
    for (let k = 1; k < pal.length; k++) {
      if (val <= pal[k][0]) {
        const lo = pal[k - 1], hi = pal[k];
        const t = (val - lo[0]) / (hi[0] - lo[0]);
        return [
          Math.round(lo[1][0] + t * (hi[1][0] - lo[1][0])),
          Math.round(lo[1][1] + t * (hi[1][1] - lo[1][1])),
          Math.round(lo[1][2] + t * (hi[1][2] - lo[1][2])),
        ];
      }
    }
    return pal[pal.length - 1][1];
  }

  // == Grid helpers =========================================================
  function gridVal(data, nx, ny, col, row) {
    if (col < 0 || col >= nx || row < 0 || row >= ny) return null;
    const v = data[row * nx + col];
    if (v === null || v === undefined || !isFinite(v)) return null;
    return v;
  }

  // == Canvas renderer: 4x upscale, land-aware ==============================
  // Pixels are only coloured when the NEAREST grid cell is valid (ocean).
  // Between valid cells, bilinear interpolation produces smooth gradients.
  // image-rendering: pixelated on the overlay prevents browser bleed at edges.
  function buildDataURL(gridPayload, pal, skipLandMask) {
    const { header: h, data } = gridPayload;
    const nx = h.nx, ny = h.ny;
    const half = Math.round(nx / 2);

    const reordered = new Array(nx * ny);
    for (let j = 0; j < ny; j++) {
      for (let i = 0; i < nx; i++) {
        reordered[j * nx + i] = data[j * nx + ((i + half) % nx)];
      }
    }

    const outW = nx * UPSCALE;
    const outH = ny * UPSCALE;
    const canvas = document.createElement('canvas');
    canvas.width  = outW;
    canvas.height = outH;
    const ctx = canvas.getContext('2d');
    const img = ctx.createImageData(outW, outH);
    const px  = img.data;

    for (let oy = 0; oy < outH; oy++) {
      const gy = oy / UPSCALE;
      const j0 = Math.floor(gy);
      const j1 = Math.min(j0 + 1, ny - 1);
      const ty = gy - j0;

      for (let ox = 0; ox < outW; ox++) {
        const gx = ox / UPSCALE;
        const i0 = Math.floor(gx);
        const i1 = Math.min(i0 + 1, nx - 1);
        const tx = gx - i0;
        const base = (oy * outW + ox) * 4;

        // Nearest grid cell for this output pixel
        const nearI = tx < 0.5 ? i0 : i1;
        const nearJ = ty < 0.5 ? j0 : j1;
        const nearVal = gridVal(reordered, nx, ny, nearI, nearJ);

        // If nearest cell is null (land), pixel is transparent — no bleed
        if (nearVal === null && !skipLandMask) {
          px[base + 3] = 0;
          continue;
        }

        // Get 4 surrounding values for interpolation
        const v00 = gridVal(reordered, nx, ny, i0, j0);
        const v10 = gridVal(reordered, nx, ny, i1, j0);
        const v01 = gridVal(reordered, nx, ny, i0, j1);
        const v11 = gridVal(reordered, nx, ny, i1, j1);

        let val;
        if (v00 !== null && v10 !== null && v01 !== null && v11 !== null) {
          val = v00 * (1 - tx) * (1 - ty)
              + v10 * tx * (1 - ty)
              + v01 * (1 - tx) * ty
              + v11 * tx * ty;
        } else {
          val = nearVal;
          if (val === null) {
            // skipLandMask path: all neighbours null
            px[base + 3] = 0;
            continue;
          }
        }

        const c = colorAt(pal, val);
        px[base]     = c[0];
        px[base + 1] = c[1];
        px[base + 2] = c[2];
        px[base + 3] = 190;
      }
    }
    ctx.putImageData(img, 0, 0);
    return canvas.toDataURL('image/png');
  }

  // == Wind speed from U/V ==================================================
  function buildWindSpeedPayload(records) {
    if (!records || records.length < 2) return null;
    const uRec = records.find(r => r.header.parameterNumber === 2);
    const vRec = records.find(r => r.header.parameterNumber === 3);
    if (!uRec || !vRec) return null;

    const n = uRec.data.length;
    const speedData = new Array(n);
    for (let i = 0; i < n; i++) {
      const u = uRec.data[i], v = vRec.data[i];
      speedData[i] = Math.sqrt(u * u + v * v);
    }
    return { header: uRec.header, data: speedData };
  }

  // == Overlay factories ====================================================
  function makeOverlays(dataURL) {
    const opts = { opacity: 1, zIndex: 200, interactive: false, className: 'gfs-img' };
    return [
      L.imageOverlay(dataURL, [[-90, -540], [90, -180]], opts),
      L.imageOverlay(dataURL, [[-90, -180], [90,  180]], opts),
      L.imageOverlay(dataURL, [[-90,  180], [90,  540]], opts),
    ];
  }

  function makeVelocityLayer(records) {
    if (typeof L.velocityLayer !== 'function') return null;
    try {
      return L.velocityLayer({
        displayValues: true,
        displayOptions: {
          velocityType:    'GFS Wind',
          position:        'bottomright',
          emptyString:     'No wind data',
          angleConvention: 'bearingCW',
          speedUnit:       'kt',
        },
        data: records, maxVelocity: 25, velocityScale: 0.01, opacity: 0.85,
        colorScale: WIND_COLOR_SCALE,
        lineWidth: 1.5,
      });
    } catch (err) {
      console.warn('leaflet-velocity layer creation failed:', err);
      return null;
    }
  }

  // == Valid-time label ======================================================
  const WDAY = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const MON  = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  function validTimeLabel(payload) {
    try {
      const vt = payload?.grid?.validTime
              || payload?.records?.[0]?.header?.validTime;
      if (!vt) return null;
      const d  = new Date(vt);
      const hh = String(d.getUTCHours()).padStart(2, '0');
      return `${WDAY[d.getUTCDay()]} ${d.getUTCDate()} ${MON[d.getUTCMonth()]} ${hh}Z`;
    } catch (_) { return null; }
  }

  // == State =================================================================
  let _type = null, _fi = 0, _playing = false, _timer = null;
  let _speed = DEFAULT_SPEED_MS;

  const _payloads = [], _urls = [], _overlays = [];
  const _windImgUrls = [];   // wind colour overlay data URLs
  const _windVelLyrs = [];   // optional velocity particle layers
  const _status = [];
  let _shown = [];

  function _clearShown() {
    _shown.forEach(l => { try { map.removeLayer(l); } catch (_) {} });
    _shown = [];
  }

  function _fullReset() {
    _clearShown();
    _stopAnim();
    _payloads.length = _urls.length = _overlays.length =
      _windImgUrls.length = _windVelLyrs.length = _status.length = 0;
    _fi = 0;
  }

  // == Animation =============================================================
  function _nextReadyIdx(from) {
    const n = STEPS.length;
    for (let step = 1; step <= n; step++) {
      const idx = (from + step) % n;
      if (_status[idx] === 'ok') return idx;
    }
    return from;
  }

  function _stopAnim() {
    if (_timer) { clearInterval(_timer); _timer = null; }
    _playing = false;
    const btn = document.getElementById('gfsPlay');
    if (btn) btn.textContent = '▶';
  }

  function _startAnim() {
    if (_timer) clearInterval(_timer);
    _timer = setInterval(() => {
      const next = _nextReadyIdx(_fi);
      if (next !== _fi) _go(next);
    }, _speed);
    _playing = true;
    const btn = document.getElementById('gfsPlay');
    if (btn) btn.textContent = '⏸';
  }

  // == Display a frame ======================================================
  function _go(idx) {
    _fi = idx;
    const slider = document.getElementById('gfsSlider');
    if (slider) slider.value = idx;
    _updateLabel();
    _updateProgress();

    if (_status[idx] !== 'ok') {
      _setLoad('Frame F+' + String(STEPS[idx]).padStart(3,'0') + ' not loaded yet');
      return;
    }
    _setLoad('');
    _clearShown();

    if (_type === 'wind') {
      // Always show wind speed colour fill
      const imgUrl = _windImgUrls[idx];
      if (imgUrl) {
        let trio = _overlays[idx];
        if (!trio) {
          trio = makeOverlays(imgUrl);
          _overlays[idx] = trio;
        }
        trio.forEach(l => l.addTo(map));
        _shown = [...trio];
      }
      // Add velocity particles on top if available
      const vel = _windVelLyrs[idx];
      if (vel) {
        try { vel.addTo(map); _shown.push(vel); } catch (_) {}
      }
    } else {
      let trio = _overlays[idx];
      if (!trio && _urls[idx]) {
        trio = makeOverlays(_urls[idx]);
        _overlays[idx] = trio;
      }
      if (trio) {
        trio.forEach(l => l.addTo(map));
        _shown = trio;
      }
    }
  }

  // == UI ===================================================================
  function _updateLabel() {
    const el = document.getElementById('gfsTimeLabel');
    if (!el) return;
    const p   = _payloads[_fi];
    const vt  = p ? validTimeLabel(p) : null;
    const fhr = STEPS[_fi];
    const tag = fhr === 0 ? 'Analysis' : `F+${String(fhr).padStart(3,'0')}`;
    el.textContent = vt ? `${tag} • ${vt}` : tag;
  }

  function _updateProgress() {
    const total  = STEPS.length;
    const nReady = _status.filter(s => s === 'ok').length;
    const bar    = document.getElementById('gfsProgress');
    if (bar) bar.style.width = ((nReady / total) * 100).toFixed(1) + '%';
    const lbl = document.getElementById('gfsProgressLabel');
    if (lbl) lbl.textContent = `${nReady} / ${total} frames loaded`;
  }

  function _setLoad(msg) {
    const el = document.getElementById('gfsLoading');
    if (!el) return;
    el.textContent = msg;
    el.style.display = msg ? 'block' : 'none';
  }

  function _showLegend(labels, gradient) {
    document.getElementById('gfsLegendBar').style.background = gradient;
    document.getElementById('gfsLegendLabels').innerHTML =
      labels.map(l => `<span>${l}</span>`).join('');
    document.getElementById('gfsLegend').style.display = 'block';
  }

  // == Fetch a frame ========================================================
  async function _fetchFrame(idx, type) {
    _status[idx] = 'loading';
    _updateProgress();
    try {
      const url = `/api/gfs/${type}?fhr=${STEPS[idx]}&resolution=${RESOLUTION}`;
      const resp = await fetch(url);
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const p = await resp.json();
      if (p.error) throw new Error(p.error);
      _payloads[idx] = p;

      if (type === 'wind') {
        // Always build a wind speed colour overlay
        const speedGrid = buildWindSpeedPayload(p.records);
        if (speedGrid) {
          _windImgUrls[idx] = buildDataURL(speedGrid, WIND_PAL, true);
        }
        // Also try velocity particles (rendered on top)
        _windVelLyrs[idx] = makeVelocityLayer(p.records);
      } else {
        const pal = type === 'waves' ? WAVE_PAL : PERIOD_PAL;
        _urls[idx] = buildDataURL(p.grid, pal, false);
      }
      _status[idx] = 'ok';

    } catch (err) {
      console.warn(`GFS fhr=${STEPS[idx]} (${type}) error:`, err);
      _status[idx] = 'err';
    }
    _updateProgress();

    if (idx === _fi) _go(idx);

    const nReady = _status.filter(s => s === 'ok').length;
    if (nReady === AUTOPLAY_MIN_READY && !_playing && _type === type) {
      _startAnim();
    }
  }

  // == Priority fetch pool ==================================================
  function _buildPriorityQueue() {
    const phase1 = [0, 24, 48, 72, 96, 120];
    const phase2 = [12, 36, 60, 84, 108];
    const phase3 = [];
    for (let h = 6; h <= 120; h += 6) {
      if (!phase1.includes(h) && !phase2.includes(h)) phase3.push(h);
    }
    return [...phase1, ...phase2, ...phase3]
      .map(h => STEPS.indexOf(h)).filter(i => i >= 0);
  }

  async function _runFetchPool(type) {
    const queue = _buildPriorityQueue();
    async function worker() {
      while (queue.length > 0 && _type === type) {
        const idx = queue.shift();
        if (_status[idx] === 'ok' || _status[idx] === 'loading') continue;
        await _fetchFrame(idx, type);
      }
    }
    const workers = [];
    for (let i = 0; i < PARALLEL_FETCHES; i++) workers.push(worker());
    await Promise.all(workers);
  }

  // == Activate / deactivate ================================================
  async function gfsActivate(type) {
    const legendEl = document.getElementById('gfsLegend');
    const timeEl   = document.getElementById('gfsTimeCtrl');

    if (type === _type) {
      _fullReset();
      _type = null;
      document.querySelectorAll('.gfs-btn').forEach(b => b.classList.remove('active'));
      legendEl.style.display = timeEl.style.display = 'none';
      _setLoad('');
      return;
    }

    _fullReset();
    _type = type;
    document.querySelectorAll('.gfs-btn')
      .forEach(b => b.classList.toggle('active', b.dataset.layer === type));

    STEPS.forEach((_, i) => { _status[i] = 'pending'; });
    timeEl.style.display   = 'block';
    legendEl.style.display = 'block';

    const slider = document.getElementById('gfsSlider');
    if (slider) { slider.max = STEPS.length - 1; slider.value = 0; }
    _updateLabel();
    _updateProgress();

    if (type === 'waves') {
      _showLegend(
        ['0','1','2','3','4','5','6','8+ m'],
        'linear-gradient(to right,#00165c,#0044ac,#0084d8,#00c0b0,#0ac672,#82cf00,#ffe100,#ff9100,#ee4400,#d70000)'
      );
    } else if (type === 'period') {
      _showLegend(
        ['5','8','11','14','17','21+ s'],
        'linear-gradient(to right,#32329e,#0058d7,#00a8a8,#00d058,#a8da00,#ffd000,#ff6c00,#d00039)'
      );
    } else {
      _showLegend(
        ['0','5','10','15','20+ m/s'],
        'linear-gradient(to right,#002864,#0050a0,#00a0c0,#00c8a0,#a0e000,#ffff00,#ff8000,#ff0000,#b4005a)'
      );
    }

    _runFetchPool(type);
  }

  // == Control panel ========================================================
  const panel = document.createElement('div');
  panel.id = 'gfsPanel';
  panel.innerHTML = `
    <div class="gfs-title">GFS Overlays</div>
    <div class="gfs-layer-btns">
      <button class="gfs-btn" data-layer="waves">Wave Ht</button>
      <button class="gfs-btn" data-layer="period">Period</button>
      <button class="gfs-btn" data-layer="wind">Wind</button>
    </div>
    <div id="gfsTimeCtrl" style="display:none">
      <div id="gfsTimeLabel" class="gfs-time-label">&mdash;</div>
      <div class="gfs-anim-row">
        <button id="gfsPlay" class="gfs-ctrl-btn" title="Play / Pause">▶</button>
        <input  id="gfsSlider" type="range" min="0" max="${STEPS.length-1}" value="0" step="1" />
        <select id="gfsSpeed" class="gfs-speed-sel" title="Playback speed">
          <option value="1000">½×</option>
          <option value="500" selected>1×</option>
          <option value="250">2×</option>
          <option value="120">4×</option>
        </select>
      </div>
      <div class="gfs-progress-wrap" title="Frame load progress">
        <div class="gfs-progress-bg">
          <div id="gfsProgress" class="gfs-progress-fill"></div>
        </div>
        <div id="gfsProgressLabel" class="gfs-progress-label">0 / ${STEPS.length} frames loaded</div>
      </div>
    </div>
    <div id="gfsLoading"></div>
    <div id="gfsLegend" style="display:none">
      <div id="gfsLegendBar"></div>
      <div id="gfsLegendLabels"></div>
    </div>`;

  document.getElementById('map').appendChild(panel);

  panel.querySelectorAll('.gfs-btn').forEach(b =>
    b.addEventListener('click', () => gfsActivate(b.dataset.layer))
  );
  document.getElementById('gfsPlay').addEventListener('click', () =>
    _playing ? _stopAnim() : _startAnim()
  );
  document.getElementById('gfsSlider').addEventListener('input', e => {
    _stopAnim();
    _go(parseInt(e.target.value, 10));
  });
  document.getElementById('gfsSpeed').addEventListener('change', e => {
    _speed = parseInt(e.target.value, 10);
    if (_playing) { _stopAnim(); _startAnim(); }
  });

  window.gfsActivate = gfsActivate;
})();
