/**
 * gfs_overlay.js  v2.0
 *
 * Animated GFS wave-height / swell-period / wind overlays.
 *
 * Features:
 *  • Stormsurf-style stepped contour bands with hairline contour lines
 *  • 3-copy image overlays for seamless dateline scrolling
 *  • 6-frame animation (analysis + day 1–5) with play/pause/scrub/speed
 *  • Valid-time label from API response
 *  • Panel positioned bottom-left (clear of Leaflet layer control)
 *
 * Requires: Leaflet global `map`, optional leaflet-velocity for wind.
 * Include AFTER the map is initialised.
 */
(function initGfsOverlays() {
  'use strict';

  // ── Animation frames (forecast hours) ─────────────────────────────────
  const STEPS = [0, 24, 48, 72, 96, 120];   // 0 = analysis, then day 1–5

  // ── Stepped colour palettes  [threshold_metres_or_seconds, [r,g,b]] ───
  // Wave height (metres) – surf-focused blue→green→yellow→red
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

  // Peak period (seconds) – blue (short/chop) → red/purple (long groundswell)
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

  // Wind speed colour scale (passed to leaflet-velocity as a CSS gradient list)
  const WIND_COLOR_SCALE = [
    '#002864','#0050a0','#00a0c0','#00c8a0',
    '#a0e000','#ffff00','#ff8000','#ff0000',
  ];

  // ── Palette helpers ────────────────────────────────────────────────────
  function getBand(pal, val) {
    for (let k = pal.length - 1; k >= 0; k--)
      if (val >= pal[k][0]) return k;
    return 0;
  }

  // ── Canvas renderer: stepped bands + hairline contour lines ───────────
  // Scale=2: each 1° grid cell renders as 2×2 px → cleaner contour edges.
  function buildDataURL(gridPayload, pal) {
    const { header: h, data } = gridPayload;
    const nx = h.nx, ny = h.ny;
    const half = Math.round(nx / 2);   // shift to re-centre on –180°
    const S = 2;                        // pixel scale per grid cell

    // Pass 1 – band map in output (–180→+180°) column order
    const bands = new Int16Array(nx * ny).fill(-1);
    for (let j = 0; j < ny; j++) {
      for (let i = 0; i < nx; i++) {
        const srcCol = (i + half) % nx;
        const v = data[j * nx + srcCol];
        if (v !== null && v !== undefined && isFinite(v))
          bands[j * nx + i] = getBand(pal, v);
      }
    }

    // Pass 2 – draw fill + contour lines on canvas
    const canvas = document.createElement('canvas');
    canvas.width  = nx * S;
    canvas.height = ny * S;
    const ctx = canvas.getContext('2d');

    // Flood-fill each cell with its band colour
    for (let j = 0; j < ny; j++) {
      for (let i = 0; i < nx; i++) {
        const b = bands[j * nx + i];
        if (b < 0) continue;
        const [r, g, bl] = pal[b][1];
        ctx.fillStyle = `rgba(${r},${g},${bl},0.75)`;
        ctx.fillRect(i * S, j * S, S, S);
      }
    }

    // Draw contour lines at band-boundary edges
    ctx.strokeStyle = 'rgba(0,0,0,0.40)';
    ctx.lineWidth   = 0.8;
    for (let j = 0; j < ny; j++) {
      for (let i = 0; i < nx; i++) {
        const b = bands[j * nx + i];
        if (b < 0) continue;
        // Right-edge boundary
        if (i < nx - 1) {
          const rb = bands[j * nx + i + 1];
          if (rb >= 0 && rb !== b) {
            ctx.beginPath();
            ctx.moveTo((i + 1) * S, j * S);
            ctx.lineTo((i + 1) * S, (j + 1) * S);
            ctx.stroke();
          }
        }
        // Bottom-edge boundary
        if (j < ny - 1) {
          const bb = bands[(j + 1) * nx + i];
          if (bb >= 0 && bb !== b) {
            ctx.beginPath();
            ctx.moveTo(i * S, (j + 1) * S);
            ctx.lineTo((i + 1) * S, (j + 1) * S);
            ctx.stroke();
          }
        }
      }
    }

    return canvas.toDataURL('image/png');
  }

  // ── Three image overlays for seamless dateline wrapping ───────────────
  // West copy  : –540 → –180   (for users who pan far west)
  // Centre     : –180 →  180   (main view)
  // East copy  :  180 →  540   (for users who pan far east)
  function makeOverlays(dataURL) {
    const opts = { opacity: 1, zIndex: 200, interactive: false };
    return [
      L.imageOverlay(dataURL, [[-90, -540], [90, -180]], opts),
      L.imageOverlay(dataURL, [[-90, -180], [90,  180]], opts),
      L.imageOverlay(dataURL, [[-90,  180], [90,  540]], opts),
    ];
  }

  // ── Velocity layer factory (wind only) ────────────────────────────────
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
      data:          records,
      maxVelocity:   15,
      velocityScale: 0.007,
      opacity:       0.9,
      colorScale:    WIND_COLOR_SCALE,
    });
  }

  // ── Valid-time label parser ───────────────────────────────────────────
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

  // ══ State ══════════════════════════════════════════════════════════════
  let _type    = null;     // 'waves' | 'period' | 'wind'
  let _fi      = 0;        // current frame index
  let _playing = false;
  let _timer   = null;
  let _speed   = 1200;     // ms between frames

  // Per-frame arrays
  const _payloads = [];   // raw API JSON
  const _rasters  = [];   // [L.ImageOverlay × 3] per frame
  const _windLyrs = [];   // L.VelocityLayer per frame
  const _status   = [];   // 'pending' | 'loading' | 'ok' | 'err'
  let   _shown    = [];   // currently added layers

  // ── Layer management ─────────────────────────────────────────────────
  function _clearShown() {
    _shown.forEach(l => { try { map.removeLayer(l); } catch (_) {} });
    _shown = [];
  }

  function _fullReset() {
    _clearShown();
    _stopAnim();
    _payloads.length = _rasters.length = _windLyrs.length = _status.length = 0;
    _fi = 0;
  }

  // ── Playback ─────────────────────────────────────────────────────────
  function _stopAnim() {
    if (_timer) { clearInterval(_timer); _timer = null; }
    _playing = false;
    const btn = document.getElementById('gfsPlay');
    if (btn) btn.textContent = '▶';
  }

  function _startAnim() {
    if (_timer) clearInterval(_timer);
    _timer = setInterval(() => _go((_fi + 1) % STEPS.length), _speed);
    _playing = true;
    const btn = document.getElementById('gfsPlay');
    if (btn) btn.textContent = '⏸';
  }

  // ── Display a specific frame ─────────────────────────────────────────
  function _go(idx) {
    _fi = idx;
    const slider = document.getElementById('gfsSlider');
    if (slider) slider.value = idx;
    _updateLabel();
    _updateDots();

    if (_status[idx] !== 'ok') {
      _setLoad('Loading F+' + String(STEPS[idx]).padStart(3, '0') + '\u2026');
      return;
    }
    _setLoad('');
    _clearShown();

    if (_type === 'wind') {
      if (_windLyrs[idx]) {
        _windLyrs[idx].addTo(map);
        _shown = [_windLyrs[idx]];
      }
    } else {
      if (_rasters[idx]) {
        _rasters[idx].forEach(l => l.addTo(map));
        _shown = _rasters[idx];
      }
    }
  }

  // ── UI helpers ────────────────────────────────────────────────────────
  function _updateLabel() {
    const el = document.getElementById('gfsTimeLabel');
    if (!el) return;
    const p   = _payloads[_fi];
    const vt  = p ? validTimeLabel(p) : null;
    const fhr = STEPS[_fi];
    el.textContent = vt || (fhr === 0 ? 'Analysis' : `F+${String(fhr).padStart(3,'0')}`);
  }

  function _updateDots() {
    document.querySelectorAll('.gfs-frame-dot').forEach((dot, i) => {
      dot.className = 'gfs-frame-dot';
      if (i === _fi)             dot.classList.add('active');
      else if (_status[i]==='ok') dot.classList.add('ready');
      else if (_status[i]==='err') dot.classList.add('error');
    });
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

  // ── Fetch one forecast frame ─────────────────────────────────────────
  async function _fetchFrame(idx, type) {
    _status[idx] = 'loading';
    _updateDots();
    try {
      const resp = await fetch(`/api/gfs/${type}?fhr=${STEPS[idx]}`);
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const p = await resp.json();
      _payloads[idx] = p;

      if (type === 'wind') {
        _windLyrs[idx] = makeVelocityLayer(p.records);
      } else {
        const pal = type === 'waves' ? WAVE_PAL : PERIOD_PAL;
        _rasters[idx] = makeOverlays(buildDataURL(p.grid, pal));
      }
      _status[idx] = 'ok';

    } catch (err) {
      console.warn(`GFS fhr=${STEPS[idx]} (${type}) error:`, err);
      _status[idx] = 'err';
    }

    _updateDots();

    // Show this frame if we're still waiting on it
    if (idx === _fi) _go(idx);

    // Auto-start once we have 2 frames to cycle through
    const nReady = _status.filter(s => s === 'ok').length;
    if (nReady === 2 && !_playing && _type === type) _startAnim();
  }

  // ── Activate / deactivate a layer ────────────────────────────────────
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

    // Initialise status & UI
    STEPS.forEach((_, i) => { _status[i] = 'pending'; });
    timeEl.style.display = 'block';
    legendEl.style.display = 'block';
    const slider = document.getElementById('gfsSlider');
    if (slider) { slider.max = STEPS.length - 1; slider.value = 0; }
    _updateLabel();
    _updateDots();

    // Configure legend
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

    // Load analysis frame immediately, then prefetch the rest
    _setLoad('Loading analysis\u2026');
    await _fetchFrame(0, type);
    _go(0);

    (async () => {
      for (let i = 1; i < STEPS.length; i++) {
        await _fetchFrame(i, type);
      }
    })();
  }

  // ══ Build the control panel ════════════════════════════════════════════
  const panel = document.createElement('div');
  panel.id = 'gfsPanel';

  // Progress dots (one per frame)
  const dots = STEPS.map((_, i) =>
    `<span class="gfs-frame-dot" title="Day ${Math.floor(STEPS[i]/24)||'0'} F+${String(STEPS[i]).padStart(3,'0')}"></span>`
  ).join('');

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
        <button id="gfsPlay"  class="gfs-ctrl-btn" title="Play / Pause">▶</button>
        <input  id="gfsSlider" type="range" min="0" max="${STEPS.length-1}" value="0" step="1" />
        <select id="gfsSpeed" class="gfs-speed-sel" title="Playback speed">
          <option value="2400">½×</option>
          <option value="1200" selected>1×</option>
          <option value="600">2×</option>
        </select>
      </div>
      <div class="gfs-frame-status">${dots}</div>
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

  // Expose globally for debugging
  window.gfsActivate = gfsActivate;

})();
