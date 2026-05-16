/**
 * gfs_overlay.js
 * Windy-style GFS overlays for the allshoresurf.com wave app.
 *
 * Provides three toggle-able layers rendered on top of the Leaflet map:
 *   • Wave Height  — colour raster from /api/gfs/waves  (surf-focused palette)
 *   • Swell Period — colour raster from /api/gfs/period
 *   • Wind         — animated particle layer from /api/gfs/wind (leaflet-velocity)
 *
 * Depends on:
 *   - Leaflet (global `map` must already be initialised)
 *   - leaflet-velocity (for wind layer; gracefully disabled if absent)
 *
 * Usage: include this script AFTER the map is created.
 *   <script src="/static/gfs_overlay.js"></script>
 */
(function initGfsOverlays() {
  'use strict';

  // ── Color palettes: [stopValue, [r,g,b]] ───────────────────────────────
  // Wave height — calibrated for surf: blue (flat) → red (huge)
  const WAVE_PALETTE = [
    [ 0.0, [  0,  20,  80]],
    [ 0.5, [  0,  60, 160]],
    [ 1.0, [  0, 120, 200]],
    [ 1.5, [  0, 180, 170]],
    [ 2.0, [  0, 185, 100]],
    [ 3.0, [120, 200,   0]],
    [ 4.0, [255, 220,   0]],
    [ 5.0, [255, 140,   0]],
    [ 6.0, [230,  60,   0]],
    [ 8.0, [200,   0,   0]],
    [10.0, [140,   0, 100]],
  ];

  // Swell period — blue (choppy) → red/purple (long-period groundswell)
  const PERIOD_PALETTE = [
    [ 0,  [ 50,  50, 150]],
    [ 5,  [  0,  80, 200]],
    [ 8,  [  0, 160, 160]],
    [11,  [  0, 200,  80]],
    [14,  [160, 210,   0]],
    [17,  [255, 200,   0]],
    [21,  [255, 100,   0]],
    [25,  [200,   0,  50]],
  ];

  // ── Palette interpolation ───────────────────────────────────────────────
  function lerpPalette(palette, value) {
    if (value === null || value === undefined || !isFinite(value)) return null;
    if (value <= palette[0][0]) return palette[0][1];
    if (value >= palette[palette.length - 1][0]) return palette[palette.length - 1][1];
    for (let k = 1; k < palette.length; k++) {
      if (value <= palette[k][0]) {
        const t = (value - palette[k-1][0]) / (palette[k][0] - palette[k-1][0]);
        const a = palette[k-1][1], b = palette[k][1];
        return [
          Math.round(a[0] + t * (b[0] - a[0])),
          Math.round(a[1] + t * (b[1] - a[1])),
          Math.round(a[2] + t * (b[2] - a[2])),
        ];
      }
    }
    return palette[palette.length - 1][1];
  }

  // ── Build L.ImageOverlay from a scalar grid payload ────────────────────
  // The GFS grid runs from 0→359° longitude.  We reorder columns by shifting
  // nx/2 so the image spans −180→180°, giving Leaflet standard bounds.
  function gridToOverlay(gridPayload, palette) {
    const h = gridPayload.header;
    const data = gridPayload.data;
    const nx = h.nx, ny = h.ny;

    const canvas = document.createElement('canvas');
    canvas.width = nx;
    canvas.height = ny;
    const ctx = canvas.getContext('2d');
    const img = ctx.createImageData(nx, ny);
    const px  = img.data;
    const half = Math.round(nx / 2); // 180 for a 1° global grid

    for (let j = 0; j < ny; j++) {
      for (let i = 0; i < nx; i++) {
        const srcCol = (i + half) % nx;
        const val    = data[j * nx + srcCol];
        const base   = (j * nx + i) * 4;

        if (val === null || val === undefined || !isFinite(val)) {
          px[base + 3] = 0; // transparent over land / missing
          continue;
        }
        const rgb = lerpPalette(palette, val);
        if (!rgb) { px[base + 3] = 0; continue; }

        px[base]     = rgb[0];
        px[base + 1] = rgb[1];
        px[base + 2] = rgb[2];
        px[base + 3] = 185; // ~73 % opacity
      }
    }

    ctx.putImageData(img, 0, 0);
    return L.imageOverlay(
      canvas.toDataURL(),
      [[-90, -180], [90, 180]],
      { opacity: 1, zIndex: 200, interactive: false }
    );
  }

  // ── Layer state ─────────────────────────────────────────────────────────
  let _active = null;    // 'waves' | 'period' | 'wind' | null
  let _raster = null;    // current L.ImageOverlay
  let _wind   = null;    // current L.VelocityLayer
  const _cache = {};     // JSON cache keyed by layer type

  function _clearLayers() {
    if (_raster) { try { map.removeLayer(_raster); } catch (e) {} _raster = null; }
    if (_wind)   { try { map.removeLayer(_wind);   } catch (e) {} _wind   = null; }
  }

  // ── Legend renderer ─────────────────────────────────────────────────────
  function _showLegend(labels, gradient) {
    document.getElementById('gfsLegendBar').style.background = gradient;
    document.getElementById('gfsLegendLabels').innerHTML =
      labels.map(l => `<span>${l}</span>`).join('');
    document.getElementById('gfsLegend').style.display = 'block';
  }

  // ── Main layer-toggle entry point ───────────────────────────────────────
  async function gfsActivate(type) {
    const loadingEl = document.getElementById('gfsLoading');
    const legendEl  = document.getElementById('gfsLegend');

    // Clicking the active layer toggles it off.
    if (type === _active) {
      _clearLayers();
      _active = null;
      document.querySelectorAll('.gfs-btn').forEach(b => b.classList.remove('active'));
      legendEl.style.display = 'none';
      return;
    }

    _clearLayers();
    _active = type;
    document.querySelectorAll('.gfs-btn')
      .forEach(b => b.classList.toggle('active', b.dataset.layer === type));

    loadingEl.textContent  = 'Loading GFS data\u2026';
    loadingEl.style.display = 'block';

    try {
      // Fetch from backend (cached after first call).
      if (!_cache[type]) {
        const resp = await fetch('/api/gfs/' + type + '?fhr=0');
        if (!resp.ok) throw new Error('Server returned HTTP ' + resp.status);
        _cache[type] = await resp.json();
      }
      const payload = _cache[type];

      // ── Wind (animated particles) ──────────────────────────────────────
      if (type === 'wind') {
        if (typeof L.velocityLayer !== 'function') {
          throw new Error('leaflet-velocity is not loaded — add the CDN script to index.html');
        }
        _wind = L.velocityLayer({
          displayValues: true,
          displayOptions: {
            velocityType: 'GFS Wind',
            position:     'bottomleft',
            emptyString:  'No wind data',
            angleConvention: 'bearingCW',
            speedUnit:    'kt',
          },
          data:          payload.records,
          maxVelocity:   15,
          velocityScale: 0.007,
          opacity:       0.9,
          colorScale: [
            '#002864','#0050a0','#00a0c0','#00c8a0',
            '#a0e000','#ffff00','#ff8000','#ff0000',
          ],
        });
        _wind.addTo(map);
        _showLegend(
          ['Calm', '5 kt', '10 kt', '15+ kt'],
          'linear-gradient(to right,#002864,#0050a0,#00a0c0,#00c8a0,#a0e000,#ffff00,#ff8000,#ff0000)'
        );

      // ── Wave height raster ─────────────────────────────────────────────
      } else if (type === 'waves') {
        _raster = gridToOverlay(payload.grid, WAVE_PALETTE);
        _raster.addTo(map);
        _showLegend(
          ['0', '1', '2', '3', '4', '5', '6', '8+ m'],
          'linear-gradient(to right,#001450,#003c8c,#0078c8,#00a0a0,#00b964,#78c800,#ffdc00,#ff8c00,#e63c00,#c80000)'
        );

      // ── Swell period raster ────────────────────────────────────────────
      } else if (type === 'period') {
        _raster = gridToOverlay(payload.grid, PERIOD_PALETTE);
        _raster.addTo(map);
        _showLegend(
          ['5', '8', '11', '14', '17', '21+ s'],
          'linear-gradient(to right,#323296,#0064dc,#00b4a0,#00c864,#b4d200,#ffc800,#ff6400,#c80032)'
        );
      }

    } catch (err) {
      console.error('GFS overlay error:', err);
      loadingEl.textContent = 'Error: ' + err.message;
      _active = null;
      document.querySelectorAll('.gfs-btn').forEach(b => b.classList.remove('active'));
      return;
    }

    loadingEl.style.display = 'none';
  }

  // ── Build the control panel inside #map ─────────────────────────────────
  const panel = document.createElement('div');
  panel.id = 'gfsPanel';
  panel.innerHTML = [
    '<div class="gfs-title">GFS Overlays</div>',
    '<button class="gfs-btn" data-layer="waves">Wave Height</button>',
    '<button class="gfs-btn" data-layer="period">Swell Period</button>',
    '<button class="gfs-btn" data-layer="wind">Wind</button>',
    '<div id="gfsLoading"></div>',
    '<div id="gfsLegend">',
    '  <div id="gfsLegendBar"></div>',
    '  <div id="gfsLegendLabels"></div>',
    '</div>',
  ].join('\n');

  document.getElementById('map').appendChild(panel);
  panel.querySelectorAll('.gfs-btn').forEach(btn =>
    btn.addEventListener('click', () => gfsActivate(btn.dataset.layer))
  );

  // Expose globally for debugging / external calls if needed.
  window.gfsActivate = gfsActivate;

})();
