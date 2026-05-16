/**
 * gfs_overlay.js  v4.0
 *
 * Animated GFS wave-height / swell-period / wind overlays with smooth
 * bilinear colour interpolation for Windy-style gradient rendering.
 *
 * v4 changes from v3:
 *  - 1.0 degree resolution (65K cells vs 1M) for dramatically faster loads
 *  - 6-hourly frame interval, 21 frames over 5 days (was 3-hourly, 41 frames)
 *  - Phased loading: 6 daily key frames first, then backfill intermediate hours
 *  - Continuous bilinear colour interpolation replaces stepped colour bands
 *  - Contour lines removed; browser bilinear upscaling on the small grid-res
 *    canvas produces smooth Windy-style gradients automatically
 *  - Parallel fetch pool (3 workers) with priority-ordered queue retained
 *
 * Requires: Leaflet global `map`, optional leaflet-velocity for wind.
 * Include AFTER the map is initialised.
 */
(function initGfsOverlays() {
  'use strict';

  // == Animation frames =====================================================
  // 6-hour intervals from analysis (f000) to day 5 (f120).
  const STEPS = [];
  for (let h = 0; h <= 120; h += 6) STEPS.push(h);
  // -> [0, 6, 12, 18, 24, 30, 36, ..., 120]  (21 frames)

  const RESOLUTION       = 1.0;   // request 1.0 deg grid from /api/gfs/*
  const PARALLEL_FETCHES = 3;     // concurrent fetch workers
  const AUTOPLAY_MIN_READY = 2;
  const DEFAULT_SPEED_MS = 500;

  // == Colour palettes (value stops for interpolation) ======================
  // Wave height (m) - surf-focused blue->green->yellow->red
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

  // Peak period (s) - blue (short/chop) -> red/purple (long groundswell)
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

  const WIND_COLOR_SCALE = [
    '#002864','#0050a0','#00a0c0','#00c8a0',
    '#a0e000','#ffff00','#ff8000','#ff0000',
  ];

  // == Smooth colour interpolation ==========================================
  // Returns [r, g, b] by linearly interpolating between the two nearest
  // palette stops. Values below the first stop clamp to that colour;
  // values above the last stop clamp to the last colour.
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

  // == Canvas renderer ======================================================
  // Renders a small grid-resolution canvas (e.g. 360x181 at 1 deg) with
  // smoothly interpolated colours. The browser's default bilinear upscaling
  // (image-rendering: auto) produces Windy-style smooth gradients when
  // displayed as an L.imageOverlay stretched to the full map extent.
  function buildDataURL(gridPayload, pal) {
    const { header: h, data } = gridPayload;
    const nx = h.nx, ny = h.ny;
    const half = Math.round(nx / 2);  // shift to re-centre on -180 deg

    const canvas = document.createElement('canvas');
    canvas.width  = nx;
    canvas.height = ny;
    const ctx = canvas.getContext('2d');
    const img = ctx.createImageData(nx, ny);
    const px  = img.data;

    for (let j = 0; j < ny; j++) {
      const rowOff = j * nx;
      for (let i = 0; i < nx; i++) {
        const srcCol = (i + half) % nx;
        const v = data[rowOff + srcCol];
        const base = (j * nx + i) * 4;

        if (v === null || v === undefined || !isFinite(v)) {
          // Transparent for ocean/land gaps and missing data
          px[base + 3] = 0;
          continue;
        }

        const c = colorAt(pal, v);
        px[base]     = c[0];
        px[base + 1] = c[1];
        px[base + 2] = c[2];
        px[base + 3] = 180;  // semi-transparent overlay
      }
    }
    ctx.putImageData(img, 0, 0);
    return canvas.toDataURL('image/png');
  }

  // == Dateline-safe overlay set (3 copies: west / centre / east) ===========
  function makeOverlays(dataURL) {
    const opts = { opacity: 1, zIndex: 200, interactive: false };
    return [
      L.imageOverlay(dataURL, [[-90, -540], [90, -180]], opts),
      L.imageOverlay(dataURL, [[-90, -180], [90,  180]], opts),
      L.imageOverlay(dataURL, [[-90,  180], [90,  540]], opts),
    ];
  }

  function makeVelocityLayer(records) {
    if (typeof L.velocityLayer !== 'function') return null;
    return L.velocityLayer({
      displayValues: true,
      displayOptions: {
        velocityType:    'GFS Wind',
        position:        'bottomright',
        emptyString:     'No wind data',
        angleConvention: 'bearingCW',
        speedUnit:       'kt',
      },
      data: records, maxVelocity: 15, velocityScale: 0.007, opacity: 0.9,
      colorScale: WIND_COLOR_SCALE,
    });
  }

  // == Valid-time label parser ===============================================
  const WDAY  = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const MON   = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

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

  // == State ================================================================
  let _type    = null;
  let _fi      = 0;
  let _playing = false;
  let _timer   = null;
  let _speed   = DEFAULT_SPEED_MS;

  const _payloads = [];   // raw API JSON per frame
  const _urls     = [];   // built data URL per frame (scalar layers)
  const _overlays = [];   // lazily-built [L.ImageOverlay x 3] per frame
  const _windLyrs = [];   // L.VelocityLayer per frame (wind only)
  const _status   = [];   // 'pending' | 'loading' | 'ok' | 'err'
  let   _shown    = [];   // currently added layers

  function _clearShown() {
    _shown.forEach(l => { try { map.removeLayer(l); } catch (_) {} });
    _shown = [];
  }

  function _fullReset() {
    _clearShown();
    _stopAnim();
    _payloads.length = _urls.length = _overlays.length =
      _windLyrs.length = _status.length = 0;
    _fi = 0;
  }

  // == Animation ============================================================
  // Skip-unloaded: only cycle through frames whose status is 'ok'.
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

  // == Display a specific frame =============================================
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
      const w = _windLyrs[idx];
      if (w) { w.addTo(map); _shown = [w]; }
    } else {
      // Build the 3 imageOverlays on demand from the cached data URL
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

  // == UI helpers ===========================================================
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
    el.textContent   = msg;
    el.style.display = msg ? 'block' : 'none';
  }

  function _showLegend(labels, gradient) {
    document.getElementById('gfsLegendBar').style.background = gradient;
    document.getElementById('gfsLegendLabels').innerHTML =
      labels.map(l => `<span>${l}</span>`).join('');
    document.getElementById('gfsLegend').style.display = 'block';
  }

  // == Fetch one forecast frame =============================================
  async function _fetchFrame(idx, type) {
    _status[idx] = 'loading';
    _updateProgress();
    try {
      const url = `/api/gfs/${type}?fhr=${STEPS[idx]}&resolution=${RESOLUTION}`;
      const resp = await fetch(url);
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const p = await resp.json();
      _payloads[idx] = p;

      if (type === 'wind') {
        _windLyrs[idx] = makeVelocityLayer(p.records);
      } else {
        const pal = type === 'waves' ? WAVE_PAL : PERIOD_PAL;
        _urls[idx] = buildDataURL(p.grid, pal);
      }
      _status[idx] = 'ok';

    } catch (err) {
      console.warn(`GFS fhr=${STEPS[idx]} (${type}) error:`, err);
      _status[idx] = 'err';
    }
    _updateProgress();

    // Refresh display if we're sitting on this frame
    if (idx === _fi) _go(idx);

    // Kick off autoplay once enough frames are ready
    const nReady = _status.filter(s => s === 'ok').length;
    if (nReady === AUTOPLAY_MIN_READY && !_playing && _type === type) {
      _startAnim();
    }
  }

  // == Parallel worker pool with priority-ordered queue =====================
  // Phase 1: Daily key frames [0, 24, 48, 72, 96, 120] load first so
  //          the user sees data within seconds.
  // Phase 2: 12-hourly intermediates [12, 36, 60, 84, 108] fill the gaps.
  // Phase 3: Remaining 6-hourly frames [6, 18, 30, 42, ...] complete the set.
  function _buildPriorityQueue() {
    const phase1 = [0, 24, 48, 72, 96, 120];
    const phase2 = [12, 36, 60, 84, 108];
    const phase3 = [];
    for (let h = 6; h <= 120; h += 6) {
      if (!phase1.includes(h) && !phase2.includes(h)) phase3.push(h);
    }
    const priorityHours = [...phase1, ...phase2, ...phase3];
    return priorityHours.map(h => STEPS.indexOf(h)).filter(i => i >= 0);
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

  // == Activate / deactivate a layer ========================================
  async function gfsActivate(type) {
    const legendEl = document.getElementById('gfsLegend');
    const timeEl   = document.getElementById('gfsTimeCtrl');

    // Toggle off
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
        ['Calm','5 kt','10 kt','15+ kt'],
        'linear-gradient(to right,#002864,#0050a0,#00a0c0,#00c8a0,#a0e000,#ffff00,#ff8000,#ff0000)'
      );
    }

    // Kick off the parallel fetch pool (don't await - let it run async)
    _runFetchPool(type);
  }

  // == Build the control panel ==============================================
  const panel = document.createElement('div');
  panel.id = 'gfsPanel';
  panel.innerHTML = `
    <div class="gfs-title">GFS Overlays · 6h</div>
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

  // Wire up controls
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
