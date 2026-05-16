/**
 * gfs_overlay.js  v5.1
 *
 * GFS wave-height / swell-period / wind overlays with land mask.
 *
 * v5.1: HD resolution (0.5° grid, 4x bilinear upscale).
 *       50m Natural Earth coastlines for crisp land mask.
 */
(function initGfsOverlays() {
  'use strict';

  const STEPS = [];
  for (let h = 0; h <= 120; h += 6) STEPS.push(h);

  const RESOLUTION       = 0.5;
  const UPSCALE          = 4;
  const PARALLEL_FETCHES = 4;
  const AUTOPLAY_MIN_READY = 2;
  const DEFAULT_SPEED_MS = 500;

  const LAND_TOPO_URL = 'https://cdn.jsdelivr.net/npm/world-atlas@2/land-50m.json';
  const LAND_FILL     = '#0a1e2e';

  // == Colour palettes ======================================================
  const WAVE_PAL = [
    [ 0.00, [  0,  22,  92]], [ 0.50, [  0,  68, 172]],
    [ 1.00, [  0, 132, 218]], [ 1.50, [  0, 192, 176]],
    [ 2.00, [ 10, 198, 112]], [ 3.00, [130, 208,   0]],
    [ 4.00, [255, 228,   0]], [ 5.00, [255, 148,   0]],
    [ 6.00, [238,  68,   0]], [ 8.00, [215,   0,   0]],
    [10.00, [148,   0, 112]],
  ];
  const PERIOD_PAL = [
    [ 0, [ 50,  50, 158]], [ 5, [  0,  88, 215]],
    [ 8, [  0, 168, 168]], [11, [  0, 208,  88]],
    [14, [168, 218,   0]], [17, [255, 208,   0]],
    [21, [255, 108,   0]], [25, [208,   0,  58]],
  ];
  const WIND_PAL = [
    [ 0, [  0,  40, 100]], [ 2, [  0,  80, 160]],
    [ 5, [  0, 160, 192]], [ 8, [  0, 200, 160]],
    [11, [160, 224,   0]], [14, [255, 255,   0]],
    [18, [255, 128,   0]], [22, [255,   0,   0]],
    [28, [180,   0,  90]],
  ];
  const WIND_COLOR_SCALE = [
    '#002864','#0050a0','#00a0c0','#00c8a0',
    '#a0e000','#ffff00','#ff8000','#ff0000',
  ];

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

  // == Canvas renderer: 4x upscale ==========================================
  function buildDataURL(gridPayload, pal) {
    const { header: h, data } = gridPayload;
    const nx = h.nx, ny = h.ny;
    const half = Math.round(nx / 2);

    const reordered = new Array(nx * ny);
    for (let j = 0; j < ny; j++)
      for (let i = 0; i < nx; i++)
        reordered[j * nx + i] = data[j * nx + ((i + half) % nx)];

    const outW = nx * UPSCALE, outH = ny * UPSCALE;
    const canvas = document.createElement('canvas');
    canvas.width = outW; canvas.height = outH;
    const ctx = canvas.getContext('2d');
    const img = ctx.createImageData(outW, outH);
    const px = img.data;

    for (let oy = 0; oy < outH; oy++) {
      const gy = oy / UPSCALE;
      const j0 = Math.floor(gy), j1 = Math.min(j0 + 1, ny - 1);
      const ty = gy - j0;
      for (let ox = 0; ox < outW; ox++) {
        const gx = ox / UPSCALE;
        const i0 = Math.floor(gx), i1 = Math.min(i0 + 1, nx - 1);
        const tx = gx - i0;
        const base = (oy * outW + ox) * 4;

        const v00 = _gv(reordered, nx, ny, i0, j0);
        const v10 = _gv(reordered, nx, ny, i1, j0);
        const v01 = _gv(reordered, nx, ny, i0, j1);
        const v11 = _gv(reordered, nx, ny, i1, j1);

        let val;
        if (v00 !== null && v10 !== null && v01 !== null && v11 !== null) {
          val = v00*(1-tx)*(1-ty) + v10*tx*(1-ty) + v01*(1-tx)*ty + v11*tx*ty;
        } else {
          const nearI = tx < 0.5 ? i0 : i1, nearJ = ty < 0.5 ? j0 : j1;
          val = _gv(reordered, nx, ny, nearI, nearJ);
          if (val === null) { px[base+3] = 0; continue; }
        }

        const c = colorAt(pal, val);
        px[base] = c[0]; px[base+1] = c[1]; px[base+2] = c[2]; px[base+3] = 200;
      }
    }
    ctx.putImageData(img, 0, 0);
    return canvas.toDataURL('image/png');
  }

  function _gv(data, nx, ny, col, row) {
    if (col < 0 || col >= nx || row < 0 || row >= ny) return null;
    const v = data[row * nx + col];
    return (v === null || v === undefined || !isFinite(v)) ? null : v;
  }

  function buildWindSpeedPayload(records) {
    if (!records || records.length < 2) return null;
    const uRec = records.find(r => r.header.parameterNumber === 2);
    const vRec = records.find(r => r.header.parameterNumber === 3);
    if (!uRec || !vRec) return null;
    const n = uRec.data.length, speedData = new Array(n);
    for (let i = 0; i < n; i++) {
      const u = uRec.data[i], v = vRec.data[i];
      speedData[i] = Math.sqrt(u*u + v*v);
    }
    return { header: uRec.header, data: speedData };
  }

  // == Land mask ============================================================
  let _landLayer = null;
  let _landLoaded = false;
  let _landLoading = false;

  async function _ensureLandMask() {
    if (_landLoaded) return;
    if (_landLoading) {
      while (_landLoading) await new Promise(r => setTimeout(r, 100));
      return;
    }
    _landLoading = true;
    try {
      if (!map.getPane('landMask')) {
        map.createPane('landMask');
        map.getPane('landMask').style.zIndex = 450;
        map.getPane('landMask').style.pointerEvents = 'none';
      }
      const resp = await fetch(LAND_TOPO_URL);
      const topo = await resp.json();
      const geojson = topojson.feature(topo, topo.objects.land);
      _landLayer = L.geoJSON(geojson, {
        pane: 'landMask',
        style: {
          fillColor: LAND_FILL,
          fillOpacity: 1,
          stroke: true,
          color: '#14364a',
          weight: 0.5,
          opacity: 0.7,
        },
        interactive: false,
      });
      _landLoaded = true;
    } catch (err) {
      console.warn('Land mask load failed:', err);
    }
    _landLoading = false;
  }

  function _showLandMask() {
    if (_landLayer && !map.hasLayer(_landLayer)) _landLayer.addTo(map);
  }

  function _hideLandMask() {
    if (_landLayer && map.hasLayer(_landLayer)) map.removeLayer(_landLayer);
  }

  // == Overlay factories ====================================================
  function makeOverlays(dataURL) {
    if (!map.getPane('gfsData')) {
      map.createPane('gfsData');
      map.getPane('gfsData').style.zIndex = 420;
    }
    const opts = { opacity: 1, interactive: false, pane: 'gfsData', className: 'gfs-img' };
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
          velocityType: 'GFS Wind', position: 'bottomright',
          emptyString: 'No wind data', angleConvention: 'bearingCW', speedUnit: 'kt',
        },
        data: records, maxVelocity: 25, velocityScale: 0.01, opacity: 0.85,
        colorScale: WIND_COLOR_SCALE, lineWidth: 1.5,
      });
    } catch (err) {
      console.warn('leaflet-velocity failed:', err);
      return null;
    }
  }

  // == Valid-time label ======================================================
  const WDAY = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const MON  = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  function validTimeLabel(payload) {
    try {
      const vt = payload?.grid?.validTime || payload?.records?.[0]?.header?.validTime;
      if (!vt) return null;
      const d = new Date(vt), hh = String(d.getUTCHours()).padStart(2,'0');
      return `${WDAY[d.getUTCDay()]} ${d.getUTCDate()} ${MON[d.getUTCMonth()]} ${hh}Z`;
    } catch (_) { return null; }
  }

  // == State =================================================================
  let _type = null, _fi = 0, _playing = false, _timer = null;
  let _speed = DEFAULT_SPEED_MS;
  const _payloads = [], _urls = [], _overlays = [];
  const _windImgUrls = [], _windVelLyrs = [];
  const _status = [];
  let _shown = [];

  function _clearShown() {
    _shown.forEach(l => { try { map.removeLayer(l); } catch(_){} });
    _shown = [];
  }
  function _fullReset() {
    _clearShown(); _stopAnim(); _hideLandMask();
    _payloads.length = _urls.length = _overlays.length =
      _windImgUrls.length = _windVelLyrs.length = _status.length = 0;
    _fi = 0;
  }

  // == Animation =============================================================
  function _nextReadyIdx(from) {
    const n = STEPS.length;
    for (let s = 1; s <= n; s++) { const i = (from+s)%n; if (_status[i]==='ok') return i; }
    return from;
  }
  function _stopAnim() {
    if (_timer) { clearInterval(_timer); _timer = null; }
    _playing = false;
    const b = document.getElementById('gfsPlay'); if (b) b.textContent = '▶';
  }
  function _startAnim() {
    if (_timer) clearInterval(_timer);
    _timer = setInterval(() => { const n=_nextReadyIdx(_fi); if(n!==_fi)_go(n); }, _speed);
    _playing = true;
    const b = document.getElementById('gfsPlay'); if (b) b.textContent = '⏸';
  }

  // == Display frame =========================================================
  function _go(idx) {
    _fi = idx;
    const slider = document.getElementById('gfsSlider');
    if (slider) slider.value = idx;
    _updateLabel(); _updateProgress();

    if (_status[idx] !== 'ok') {
      _setLoad('Frame F+' + String(STEPS[idx]).padStart(3,'0') + ' loading...');
      return;
    }
    _setLoad(''); _clearShown();

    if (_type === 'wind') {
      const imgUrl = _windImgUrls[idx];
      if (imgUrl) {
        let trio = _overlays[idx];
        if (!trio) { trio = makeOverlays(imgUrl); _overlays[idx] = trio; }
        trio.forEach(l => l.addTo(map));
        _shown = [...trio];
      }
      const vel = _windVelLyrs[idx];
      if (vel) { try { vel.addTo(map); _shown.push(vel); } catch(_){} }
    } else {
      let trio = _overlays[idx];
      if (!trio && _urls[idx]) { trio = makeOverlays(_urls[idx]); _overlays[idx] = trio; }
      if (trio) { trio.forEach(l => l.addTo(map)); _shown = trio; }
    }

    _showLandMask();
  }

  // == UI ====================================================================
  function _updateLabel() {
    const el = document.getElementById('gfsTimeLabel'); if (!el) return;
    const p = _payloads[_fi], vt = p ? validTimeLabel(p) : null;
    const fhr = STEPS[_fi];
    const tag = fhr === 0 ? 'Analysis' : `F+${String(fhr).padStart(3,'0')}`;
    el.textContent = vt ? `${tag} • ${vt}` : tag;
  }
  function _updateProgress() {
    const total = STEPS.length, nReady = _status.filter(s => s==='ok').length;
    const bar = document.getElementById('gfsProgress');
    if (bar) bar.style.width = ((nReady/total)*100).toFixed(1)+'%';
    const lbl = document.getElementById('gfsProgressLabel');
    if (lbl) lbl.textContent = `${nReady}/${total} loaded`;
  }
  function _setLoad(msg) {
    const el = document.getElementById('gfsLoading'); if (!el) return;
    el.textContent = msg; el.style.display = msg ? 'block' : 'none';
  }
  function _showLegend(labels, gradient) {
    document.getElementById('gfsLegendBar').style.background = gradient;
    document.getElementById('gfsLegendLabels').innerHTML =
      labels.map(l => `<span>${l}</span>`).join('');
    document.getElementById('gfsLegend').style.display = 'block';
  }

  // == Fetch ==================================================================
  async function _fetchFrame(idx, type) {
    _status[idx] = 'loading'; _updateProgress();
    try {
      const url = `/api/gfs/${type}?fhr=${STEPS[idx]}&resolution=${RESOLUTION}`;
      const resp = await fetch(url);
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      const p = await resp.json();
      if (p.error) throw new Error(p.error);
      _payloads[idx] = p;

      if (type === 'wind') {
        const sg = buildWindSpeedPayload(p.records);
        if (sg) _windImgUrls[idx] = buildDataURL(sg, WIND_PAL);
        _windVelLyrs[idx] = makeVelocityLayer(p.records);
      } else {
        _urls[idx] = buildDataURL(p.grid, type==='waves' ? WAVE_PAL : PERIOD_PAL);
      }
      _status[idx] = 'ok';
    } catch (err) {
      console.warn(`GFS fhr=${STEPS[idx]} (${type}):`, err);
      _status[idx] = 'err';
    }
    _updateProgress();
    if (idx === _fi) _go(idx);
    const nReady = _status.filter(s=>s==='ok').length;
    if (nReady === AUTOPLAY_MIN_READY && !_playing && _type === type) _startAnim();
  }

  function _buildPriorityQueue() {
    const p1=[0,24,48,72,96,120], p2=[12,36,60,84,108], p3=[];
    for (let h=6;h<=120;h+=6) if(!p1.includes(h)&&!p2.includes(h)) p3.push(h);
    return [...p1,...p2,...p3].map(h=>STEPS.indexOf(h)).filter(i=>i>=0);
  }
  async function _runFetchPool(type) {
    const q = _buildPriorityQueue();
    async function w() { while(q.length>0&&_type===type){const i=q.shift();if(_status[i]==='ok'||_status[i]==='loading')continue;await _fetchFrame(i,type);} }
    await Promise.all(Array.from({length:PARALLEL_FETCHES},()=>w()));
  }

  // == Activate ==============================================================
  async function gfsActivate(type) {
    const legendEl = document.getElementById('gfsLegend');
    const timeEl = document.getElementById('gfsTimeCtrl');

    if (type === _type) {
      _fullReset(); _type = null;
      document.querySelectorAll('.gfs-btn').forEach(b => b.classList.remove('active'));
      legendEl.style.display = timeEl.style.display = 'none';
      _setLoad(''); return;
    }

    _fullReset(); _type = type;
    document.querySelectorAll('.gfs-btn')
      .forEach(b => b.classList.toggle('active', b.dataset.layer === type));
    STEPS.forEach((_,i) => { _status[i] = 'pending'; });
    timeEl.style.display = 'block'; legendEl.style.display = 'block';

    const slider = document.getElementById('gfsSlider');
    if (slider) { slider.max = STEPS.length-1; slider.value = 0; }
    _updateLabel(); _updateProgress();

    if (type === 'waves') {
      _showLegend(['0','1','2','3','4','5','6','8+ m'],
        'linear-gradient(to right,#00165c,#0044ac,#0084d8,#00c0b0,#0ac672,#82cf00,#ffe100,#ff9100,#ee4400,#d70000)');
    } else if (type === 'period') {
      _showLegend(['5','8','11','14','17','21+ s'],
        'linear-gradient(to right,#32329e,#0058d7,#00a8a8,#00d058,#a8da00,#ffd000,#ff6c00,#d00039)');
    } else {
      _showLegend(['0','5','10','15','20+ m/s'],
        'linear-gradient(to right,#002864,#0050a0,#00a0c0,#00c8a0,#a0e000,#ffff00,#ff8000,#ff0000,#b4005a)');
    }

    // Load land mask in parallel with data
    _ensureLandMask();
    _runFetchPool(type);
  }

  // == Panel =================================================================
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
        <input id="gfsSlider" type="range" min="0" max="${STEPS.length-1}" value="0" step="1" />
        <select id="gfsSpeed" class="gfs-speed-sel" title="Speed">
          <option value="1000">½×</option>
          <option value="500" selected>1×</option>
          <option value="250">2×</option>
          <option value="120">4×</option>
        </select>
      </div>
      <div class="gfs-progress-wrap">
        <div class="gfs-progress-bg"><div id="gfsProgress" class="gfs-progress-fill"></div></div>
        <div id="gfsProgressLabel" class="gfs-progress-label">0/${STEPS.length} loaded</div>
      </div>
    </div>
    <div id="gfsLoading"></div>
    <div id="gfsLegend" style="display:none">
      <div id="gfsLegendBar"></div>
      <div id="gfsLegendLabels"></div>
    </div>`;

  document.getElementById('map').appendChild(panel);
  panel.querySelectorAll('.gfs-btn').forEach(b =>
    b.addEventListener('click', () => gfsActivate(b.dataset.layer)));
  document.getElementById('gfsPlay').addEventListener('click', () =>
    _playing ? _stopAnim() : _startAnim());
  document.getElementById('gfsSlider').addEventListener('input', e => {
    _stopAnim(); _go(parseInt(e.target.value, 10));
  });
  document.getElementById('gfsSpeed').addEventListener('change', e => {
    _speed = parseInt(e.target.value, 10);
    if (_playing) { _stopAnim(); _startAnim(); }
  });

  window.gfsActivate = gfsActivate;
})();
