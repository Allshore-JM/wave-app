'use strict';
// The satellite readings (plan sections 45 and 81) in static_overlay/overlay.js, without a browser: the chunk decoder
// (real zlib bytes through the real DecompressionStream path) and every rejection, the pointer rules, the bucket index
// against a brute-force scan, the colour pin against composeTile, the store with held downloads (nearest first, the
// in-flight cap, 404 final / 503 cooldown, a newer pointer adopted only once its chunks landed, idle / abortAll keep the
// samples), the layer (split draw, empty -> empty untouched, hitAt = the top-most drawn), and the controller (the own
// entry: mode obs, the layer in its pane, the credit, the Heights | Swell switch; the box under wave height: the window
// the hover texts; the "Satellite readings" box under wave height was removed by the owner on 2026-11-05).
//   node --test tests/overlay/sat.test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const EPOCH = Date.UTC(2000, 0, 1);
const ALT_COLS = [['t', 'u4', 1, null], ['lat', 'i4', 1e-6, null], ['lon', 'i4', 1e-6, null], ['swh', 'u2', 0.001, 65535], ['model', 'u2', 0.001, 65535], ['mission', 'u1', 1, null], ['run', 'u1', 1, 255], ['flags', 'u1', 1, null]];
const SWELL_COLS = [['t', 'u4', 1, null], ['lat', 'i4', 1e-6, null], ['lon', 'i4', 1e-6, null], ['n', 'u1', 1, null], ['mission', 'u1', 1, null], ['flags', 'u1', 1, null],
  ['hs1', 'u2', 0.001, 65535], ['hs2', 'u2', 0.001, 65535], ['hs3', 'u2', 0.001, 65535], ['tp1', 'u2', 0.01, 65535], ['tp2', 'u2', 0.01, 65535], ['tp3', 'u2', 0.01, 65535],
  ['dir1', 'u2', 0.01, 65535], ['dir2', 'u2', 0.01, 65535], ['dir3', 'u2', 0.01, 65535]];
// A chunk's bytes from readings [{t (ms), lat, lon, swh, model, mission, run, flags}] (heights) or swell points.
function chunkBytes(kind, readings, header, badLen) {
  const cols = kind === 'alt' ? ALT_COLS : SWELL_COLS, n = readings.length, bufs = [];
  for (const [name, code, scale, none] of cols) {
    const size = { u1: 1, u2: 2, u4: 4, i4: 4 }[code], b = Buffer.alloc(n * size);
    for (let i = 0; i < n; i++) {
      let v = readings[i][name];
      if (name === 't') v = Math.round((v - EPOCH) / 1000);
      else if (v === undefined || v === null || Number.isNaN(v)) v = none === null ? 0 : none;
      else v = Math.round(v / scale);
      if (code === 'u1') b[i] = v; else if (code === 'u2') b.writeUInt16LE(v, i * 2); else if (code === 'u4') b.writeUInt32LE(v, i * 4); else b.writeInt32LE(v, i * 4);
    }
    bufs.push(b);
  }
  const h = Object.assign({ format: kind === 'alt' ? 'alt-v1' : 'swell-v1', n: badLen ? n + badLen : n, columns: cols, window: '20261104T12', t0: '2026-11-04T12:00:00Z', t1: '2026-11-04T15:00:00Z',
    build: '20261104T152000Z', missions: [{ id: 'j3', short: 'Jason-3', name: 'Jason-3' }, { id: 's3a', short: 'Sentinel-3A', name: 'Sentinel-3A' }],
    segments: [[0, 0, n]], runs: ['2026110406'], credit: 'Generated using E.U. Copernicus Marine Service Information' }, header || {});
  const hj = Buffer.from(JSON.stringify(h), 'utf8'), len = Buffer.alloc(4); len.writeUInt32LE(hj.length);
  const body = zlib.deflateSync(Buffer.concat(bufs));
  return new Uint8Array(Buffer.concat([Buffer.from(kind === 'alt' ? 'ALT1' : 'SWL1', 'ascii'), len, hj, body])).buffer;
}
const T0 = Date.parse('2026-11-04T12:00:00Z');
function reading(i, extra) { return Object.assign({ t: T0 + i * 60000, lat: 10 + i * 0.1, lon: -150 + i * 0.1, swh: 1 + i * 0.1, model: 1.2 + i * 0.1, mission: 0, run: 0, flags: 0 }, extra || {}); }
function pointer(kind, chunks, extra) {
  return Object.assign({ format: kind === 'alt' ? 'alt-v1' : 'swell-v1', magic: kind === 'alt' ? 'ALT1' : 'SWL1', encoding: 'deflate', build: '20261104T152000Z', published_utc: '2026-11-04T15:20:00Z', checked_utc: '2026-11-04T15:20:00Z',
    newest_utc: '2026-11-04T14:59:50Z', window_s: 10800, keep_h: 30, columns: kind === 'alt' ? ALT_COLS : SWELL_COLS,
    missions: [{ id: 'j3', short: 'Jason-3', name: 'Jason-3' }, { id: 's3a', short: 'Sentinel-3A', name: 'Sentinel-3A' }], chunks: chunks || [], model: null, source: { doi: 'x' } }, extra || {});
}
function chunkRec(kind, window, n, build) {
  const w0 = Date.parse(window.slice(0, 4) + '-' + window.slice(4, 6) + '-' + window.slice(6, 8) + 'T' + window.slice(9, 11) + ':00:00Z');
  return { key: (kind === 'alt' ? 'alt/v1' : 'sarswell/v1') + '/w/' + window + '/' + (build || '20261104T152000Z') + '.bin', window, t0: new Date(w0).toISOString().replace('.000Z', 'Z'),
    t1: new Date(w0 + 3 * 3.6e6).toISOString().replace('.000Z', 'Z'), n, bytes: 0, build: build || '20261104T152000Z', missions: ['j3'], newest_utc: new Date(w0 + 3 * 3.6e6 - 10000).toISOString().replace('.000Z', 'Z'), inputs: 'd' };
}

// ---- the module in a sandbox: Leaflet / document / fetch / timers stubbed; a 2-D context that records its calls ----
function world(opts) {
  opts = opts || {};
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  const reg = { doc: {}, win: {}, map: {} };
  const on = (t) => (ev, fn) => { (reg[t][ev] = reg[t][ev] || []).push(fn); };
  const off = (t) => (ev, fn) => { reg[t][ev] = (reg[t][ev] || []).filter((f) => f !== fn); };
  const w = { fetches: [], held: [], panes: {}, layers: [], attrs: [], canvases: [] };
  function ctx2d(el) {
    const calls = [];
    const c = { calls, globalAlpha: 1, clearRect() { calls.push(['clear']); }, beginPath() { calls.push(['begin']); }, arc(x, y, r) { calls.push(['arc', x, y, r, this.globalAlpha]); },
      fill() { calls.push(['fill', this.fillStyle, this.globalAlpha]); }, stroke() { calls.push(['stroke', this.strokeStyle, this.lineWidth]); }, moveTo(x, y) { calls.push(['move', x, y]); }, lineTo(x, y) { calls.push(['line', x, y]); },
      createImageData: (ww, hh) => ({ data: new Uint8ClampedArray(ww * hh * 4) }), putImageData() {} };
    el.calls = calls; return c;
  }
  const g = {
    L: { GridLayer: { prototype: { initialize(o) { this.options = o; this._tiles = {}; }, onRemove() {} },
      extend(p) { function C(o) { p.initialize.call(this, o); } C.prototype = Object.assign({ setOpacity() {}, addTo(m) { this._map = m; w.layers.push(this); return this; } }, p); return C; } },
      DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} } },
    document: { hidden: false, addEventListener: on('doc'), removeEventListener: off('doc'), body: { appendChild() {} },
      createElement(tag) { const el = { tag, width: 0, height: 0, children: [], style: {}, classList: { add() {}, remove() {}, contains() { return false; }, toggle() {} }, hidden: false, textContent: '',
        appendChild(c) { this.children.push(c); return c; }, removeChild(c) { this.children = this.children.filter((x) => x !== c); }, insertBefore(c) { this.children.unshift(c); return c; }, setAttribute(k, v) { this[k] = v; }, getAttribute(k) { return this[k]; },
        addEventListener(ev, fn) { (this._ls = this._ls || {})[ev] = fn; }, removeEventListener() {}, remove() {}, querySelector() { return null; }, querySelectorAll() { return []; }, closest() { return null; }, getBoundingClientRect() { return { top: 0, height: 10 }; } };
        if (tag === 'canvas') { const c = ctx2d(el); el.getContext = () => c; w.canvases.push(el); }
        if (tag === 'input') el.checked = false;
        return el; },
      createTextNode(t) { return { nodeValue: t, textContent: t }; }, activeElement: null },
    sessionStorage: { getItem: (k) => (w.saved === undefined ? null : JSON.stringify(w.saved)), setItem(k, v) { w.saved = JSON.parse(v); } },
    fetch: (url, o) => {
      w.fetches.push(url); (w.fetchOpts = w.fetchOpts || []).push([url, o || {}]);
      if (o && o.signal && o.signal.aborted) return Promise.reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
      return new Promise((res, rej) => {
        const h = { url, res, rej, sig: o && o.signal };
        w.held.push(h);
        if (h.sig) h.sig.addEventListener('abort', () => { if (!h.done) { h.done = true; rej(Object.assign(new Error('aborted'), { name: 'AbortError' })); } });
      });
    },
    setTimeout: (f, ms) => { const t = setTimeout(f, ms); if (t.unref) t.unref(); return t; }, clearTimeout, setInterval: (f, ms) => { const t = setInterval(f, ms); if (t.unref) t.unref(); return t; }, clearInterval, Date, Math, console, Promise, Map, Array, Object, Number, String, Error, TypeError, JSON, isNaN, parseInt, parseFloat, AbortController,
    Uint8Array, Uint8ClampedArray, Uint16Array, Uint32Array, Int32Array, Float32Array, Float64Array, Symbol, DataView, TextDecoder, DecompressionStream, Response, ArrayBuffer, Intl,
    matchMedia: undefined, requestAnimationFrame: undefined
  };
  g.window = g; g.addEventListener = on('win'); g.removeEventListener = off('win');
  const fn = new Function('L', 'document', 'sessionStorage', 'fetch', 'window', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'AbortController', 'DecompressionStream', 'TextDecoder', 'Response', 'requestAnimationFrame', 'cancelAnimationFrame', src);
  fn(g.L, g.document, g.sessionStorage, g.fetch, g, g.setTimeout, g.clearTimeout, g.setInterval, g.clearInterval, AbortController, DecompressionStream, TextDecoder, Response, undefined, undefined);
  w.I = g.AllshoreOverlay._internals; w.AO = g.AllshoreOverlay; w.g = g; w.reg = reg;
  // answer a held request: json, or bytes (an ArrayBuffer), or a status
  w.answer = (re, body, status) => {
    const h = w.held.find((x) => !x.done && re.test(x.url));
    if (!h) return false;
    h.done = true;
    if (status && status !== 200) { h.res({ ok: false, status, headers: { get: () => null }, json: () => Promise.reject(new Error('no')) }); return true; }
    if (body instanceof ArrayBuffer) h.res({ ok: true, status: 200, headers: { get: (k) => (k === 'content-length' ? String(body.byteLength) : null) }, arrayBuffer: () => Promise.resolve(body) });
    else h.res({ ok: true, status: 200, headers: { get: () => null }, json: () => Promise.resolve(body) });
    return true;
  };
  w.fail = (re) => { const h = w.held.find((x) => !x.done && re.test(x.url)); if (!h) return false; h.done = true; h.rej(new TypeError('Failed to fetch')); return true; };
  w.pending = (re) => w.held.filter((x) => !x.done && (!re || re.test(x.url))).map((x) => x.url);
  // a map stub: panes, layers, zoom, pixel bounds (a 512 x 512 view at zoom 3 over the Pacific), an attribution control
  const container = g.document.createElement('div'); container.clientWidth = 1200; container.clientHeight = 800; container.classList = { add() {}, remove() {}, contains() { return false; } }; container.style = { setProperty() {}, removeProperty() {} };
  w.map = { _z: opts.zoom || 3, getZoom() { return this._z; }, getPane: (n) => w.panes[n] || null, createPane: (n) => (w.panes[n] = { style: {} }), hasLayer: (l) => w.layers.indexOf(l) >= 0, removeLayer: (l) => { w.layers = w.layers.filter((x) => x !== l); if (l.onRemove) l.onRemove(w.map); },
    on: on('map'), off: off('map'), getContainer: () => container, getSize: () => ({ x: 1200, y: 800 }), getPixelBounds: () => ({ min: { x: 0, y: 600 }, max: { x: 1500, y: 1400 } }), getZoomScale: () => 1,
    containerPointToLayerPoint: () => ({ x: 0, y: 0 }), attributionControl: { addAttribution: (t) => w.attrs.push(t), removeAttribution: (t) => { w.attrs = w.attrs.filter((x) => x !== t); } }, zoomControl: null };
  return w;
}
// microtasks, immediates and the 0-ms timers the store schedules its rebuilds on
const allText = (el) => (el.textContent || '') + ' ' + (el.children || []).map(allText).join(' ');
// waits (up to ~3 s) for a condition the async decodes bring about
const until = async (fn) => { for (let i = 0; i < 300; i++) { if (fn()) return true; await new Promise((r) => setTimeout(r, 10)); } return fn(); };
const wre = (k) => new RegExp('/w/' + k + '/');
const settle = async (n) => { for (let i = 0; i < (n || 10); i++) { await new Promise((r) => setImmediate(r)); if (i % 4 === 3) await new Promise((r) => setTimeout(r, 2)); } };
const T = world();
const I = T.I;

// ------------------------------------------------------------------ the decoder
test('decodeSatChunk: a real zlib chunk decodes through DecompressionStream; every rejection; the swell format', async () => {
  const rs = [reading(0, { flags: 1 }), reading(1), reading(2, { model: NaN, run: 255 })];
  const buf = chunkBytes('alt', rs);
  const d = await I.decodeSatChunk(buf, 'alt');
  assert.equal(d.header.n, 3); assert.equal(d.cols.t[1], (rs[1].t - EPOCH) / 1000); assert.equal(d.cols.lat[2], Math.round(10.2 / 1e-6)); assert.equal(d.cols.lon[0], -150000000);
  assert.equal(d.cols.swh[1], 1100); assert.equal(d.cols.model[2], 65535); assert.equal(d.cols.run[2], 255); assert.equal(d.cols.flags[0], 1);
  await assert.rejects(I.decodeSatChunk(buf, 'swell'), /sat decode failed/, 'the other kind\'s magic');
  await assert.rejects(I.decodeSatChunk(buf.slice(0, buf.byteLength - 2), 'alt'), /sat decode failed/, 'truncated');
  await assert.rejects(I.decodeSatChunk(chunkBytes('alt', rs, null, 1), 'alt'), /sat decode failed/, 'the header claims more readings than the stream holds');
  await assert.rejects(I.decodeSatChunk(chunkBytes('alt', rs, { columns: ALT_COLS.slice(0, 7) }), 'alt'), /sat decode failed/, 'other columns');
  await assert.rejects(I.decodeSatChunk(chunkBytes('alt', rs, { segments: [[0, 0, 2]] }), 'alt'), /sat decode failed/, 'segments not covering');
  await assert.rejects(I.decodeSatChunk(chunkBytes('alt', [rs[1], rs[0]]), 'alt'), /sat decode failed/, 'a segment not time-sorted');
  await assert.rejects(I.decodeSatChunk(chunkBytes('alt', [reading(0, { mission: 5 })]), 'alt'), /sat decode failed/, 'a mission the header does not list');
  await assert.rejects(I.decodeSatChunk(chunkBytes('alt', [reading(0, { lat: 95 })]), 'alt'), /sat decode failed/, 'a latitude beyond the pole');
  await assert.rejects(I.decodeSatChunk(chunkBytes('alt', rs, { format: 'alt-v2' }), 'alt'), /sat decode failed/);
  await assert.rejects(I.decodeSatChunk(chunkBytes('alt', rs, { credit: undefined }), 'alt'), /sat decode failed/, 'no credit line');
  await assert.rejects(I.decodeSatChunk(new ArrayBuffer(4), 'alt'), /sat decode failed/);
  const sw = await I.decodeSatChunk(chunkBytes('swell', [{ t: T0, lat: -30, lon: 170, n: 2, mission: 0, flags: 0, hs1: 2.0, hs2: 1.5, hs3: NaN, tp1: 14.25, tp2: 9, tp3: NaN, dir1: 215.5, dir2: 100, dir3: NaN }]), 'swell');
  assert.equal(sw.cols.hs1[0], 2000); assert.equal(sw.cols.tp1[0], 1425); assert.equal(sw.cols.dir1[0], 21550); assert.equal(sw.cols.hs3[0], 65535); assert.equal(sw.cols.n[0], 2);
  // an odd n: the u16 / i32 columns sit unaligned in the stream and are still read right
  const odd = await I.decodeSatChunk(chunkBytes('alt', [reading(0), reading(1), reading(2), reading(3), reading(4)]), 'alt');
  assert.deepEqual(Array.from(odd.cols.swh), [1000, 1100, 1200, 1300, 1400]);
  assert.equal(I.satStride(ALT_COLS), 19); assert.equal(I.satStride(SWELL_COLS), 33);
});

test('validSatPointer: the job\'s pointer passes; the rules that keep bad data off the map', () => {
  const p = pointer('alt', [chunkRec('alt', '20261104T09', 10), chunkRec('alt', '20261104T12', 20)]);
  assert.equal(I.validSatPointer(p, 'alt'), true);
  assert.equal(I.validSatPointer(p, 'swell'), false, 'the other kind');
  const bad = (f) => { const c = JSON.parse(JSON.stringify(p)); f(c); return I.validSatPointer(c, 'alt'); };
  assert.equal(bad((c) => { c.magic = 'ALT2'; }), false);
  assert.equal(bad((c) => { c.encoding = 'none'; }), false);
  assert.equal(bad((c) => { c.columns[3][2] = 0.01; }), false, 'another scale than this file decodes');
  assert.equal(bad((c) => { c.chunks[0].key = 'sst/v1/x.bin'; }), false, 'a key outside the prefix');
  assert.equal(bad((c) => { c.chunks[0].key = 'alt/v1/../x.bin'; }), false);
  assert.equal(bad((c) => { c.chunks[1].t0 = c.chunks[0].t0; }), false, 'chunks overlap');
  assert.equal(bad((c) => { c.chunks[0].n = 700000; }), false);
  assert.equal(bad((c) => { c.chunks[0].bytes = 3e6; }), false);
  assert.equal(bad((c) => { c.chunks = new Array(13).fill(c.chunks[0]); }), false);
  assert.equal(bad((c) => { c.missions[0] = { id: 'j3' }; }), false, 'a mission without its short name');
  assert.equal(bad((c) => { c.chunks = []; }), true, 'nothing published yet is a valid, empty pointer');
  assert.equal(bad((c) => { c.chunks[0].n = 400000; c.chunks[1].n = 300000; }), false, 'more readings in all than the flat store holds: refused (never a truncated newest window)');
  assert.equal(I.satNewestMs(p), Date.parse('2026-11-04T14:59:50Z'));
  assert.equal(I.satNewestMs(pointer('alt', [chunkRec('alt', '20261104T12', 5)], { newest_utc: undefined })), Date.parse('2026-11-04T14:59:50Z'), 'from the last chunk');
});

// ------------------------------------------------------------------ pure helpers
test('satStrideFor / satRadius / satAlpha / satColourIndex: the thinning by zoom, the fade endpoints, the colour pinned to composeTile', () => {
  assert.deepEqual([0, 2, 5, 6.9, 7, 12].map(I.satStrideFor), [128, 32, 4, 2, 1, 1]);
  assert.deepEqual([1, 2, 4, 8, 12].map(I.satRadius), [1.5, 1.5, 2, 3, 3]);
  assert.deepEqual([-1, 0, 0.5, 1, 2].map(I.satAlpha), [1, 1, 0.675, 0.35, 0.35]); assert.deepEqual(I.SAT_FADE, [1, 0.35]);
  // a dot at value v takes the LUT entry composeTile takes for the code that decodes to v (the comparison's premise)
  const legend = I.SAT_FDEF.legend, lo = I.SAT_FDEF.lo, hi = I.SAT_FDEF.hi, lut = I.buildLut('hs', legend);
  for (const v of [0, 0.3, 1.234, 5.5, 12, 18.288, 20]) {
    const code = 1 + Math.round((Math.min(v, hi) - lo) / (hi - lo) * 254), d = new Uint8ClampedArray(4);
    I.composeTile(new Float64Array([code]), null, lut, lo, hi, legend[0], legend[1], d, null);
    const ci = I.satColourIndex(lo + (code - 1) / 254 * (hi - lo), legend[0], legend[1]);
    assert.deepEqual([lut[ci * 3], lut[ci * 3 + 1], lut[ci * 3 + 2]], [d[0], d[1], d[2]], 'v ' + v);
  }
  assert.equal(I.satColourIndex(-1, 0, 18.288), 0); assert.equal(I.satColourIndex(30, 0, 18.288), 255);
  for (const [tp, want] of [[4, 8.4], [8, 12.8], [14, 19.4], [20, 26], [30, 26]]) assert.ok(Math.abs(I.satArrowLength(tp) - want) < 1e-9, 'tp ' + tp);
  assert.deepEqual(I.SAT_FDEF, { lo: 0, hi: 22.86, legend: [0, 18.288], units: 'm', interpolation: 'bilinear' }, '= SCALES.hs / encode.FIELDS hs');
  assert.ok(I.SAT_ATTRIBUTION.replace(/<[^>]+>/g, '').length <= 29, 'the credit\'s visible text: ' + I.SAT_ATTRIBUTION.replace(/<[^>]+>/g, ''));
  assert.ok(/10\.48670\/moi-00179/.test(I.SAT_ATTRIBUTION) && /10\.48670\/moi-00178/.test(I.SAT_ATTRIBUTION) && !/moi-00174/.test(I.SAT_ATTRIBUTION), 'both DOIs in the title (014_002 is moi-00178)');
  assert.equal(I.MAX_SAT_CHUNKS, 20, 'the swell job keeps 56 h (19 windows)');
  assert.deepEqual(Object.keys(I.OBS_DEFS), ['sst', 'sat']); assert.equal(I.hasObs('constructor'), false); assert.equal(I.hasObs('sat'), true);
});

test('SatIndex: the bucket walk equals a brute-force scan (date line, poles, a time slice, a stride); nearest ties to the newest', () => {
  const n = 4000, lat = new Float32Array(n), lon = new Float32Array(n), ord = new Uint32Array(n), t = new Float64Array(n);
  let seed = 7; const rnd = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
  for (let i = 0; i < n; i++) { lat[i] = -90 + 180 * rnd(); lon[i] = -180 + 360 * rnd(); ord[i] = i % 97; t[i] = i; }
  lat[0] = 90; lon[0] = 180; lat[1] = -90; lon[1] = -180; lon[2] = 179.99; lon[3] = -179.99;
  const idx = new I.SatIndex(lat, lon, n), flat = { lat, lon, ord, t };
  const brute = (box, i0, i1, stride) => { const o = []; for (let j = i0; j < i1; j++) { if (stride > 1 && ord[j] % stride) continue; const la = lat[j], lo = lon[j]; if (la < box.latS || la > box.latN) continue; if (box.lonE < box.lonW ? (lo < box.lonW && lo > box.lonE) : (lo < box.lonW || lo > box.lonE)) continue; o.push(j); } return o; };
  for (const box of [{ latS: 80, latN: 90, lonW: 170, lonE: 180 }, { latS: -90, latN: -85, lonW: -180, lonE: -170 }, { latS: -5, latN: 5, lonW: 175, lonE: -175 }, { latS: 10, latN: 12, lonW: -20, lonE: -18 }]) {
    for (const [i0, i1, st] of [[0, n, 1], [1000, 3000, 1], [0, n, 4]]) {
      const got = idx.collect(box, i0, i1, st, ord);
      if (got !== null) assert.deepEqual(got, brute(box, i0, i1, st), JSON.stringify(box) + ' ' + st);
      else assert.ok(i1 - i0 <= 2 * Math.max(1, Math.ceil((box.latN - box.latS) / 0.5) + 1) * 40, 'the slice scan only where the buckets would not pay');
    }
  }
  // two readings at one spot: the newer (higher index) wins; nothing beyond the radius
  const la2 = new Float32Array([20, 20, 20]), lo2 = new Float32Array([-150, -150, -149]), or2 = new Uint32Array([0, 0, 0]), t2 = new Float64Array([1, 2, 3]);
  const ix2 = new I.SatIndex(la2, lo2, 3), f2 = { lat: la2, lon: lo2, ord: or2, t: t2 };
  assert.equal(ix2.nearest(f2, 20, -150, 3, 5, 0, 3, 1), 1);
  assert.equal(ix2.nearest(f2, 20, -150, 3, 5, 0, 1, 1), 0, 'the slice decides what is drawn');
  assert.equal(ix2.nearest(f2, 20, -149, 3, 5, 0, 3, 1), 2);
  assert.equal(ix2.nearest(f2, 20, -148, 3, 5, 0, 3, 1), -1, '1 degree at zoom 3 (5.7 px) is beyond 5 px');
  assert.equal(ix2.nearest(f2, 20, -148.9, 3, 5, 0, 3, 1), 2, '0.1 degree (0.6 px) is within it');
  assert.equal(ix2.nearest(f2, 20, -150, 3, 5, 0, 3, 2), 1, 'ord 0 passes every stride');
  assert.equal(I.satLowerBound(new Float64Array([1, 2, 2, 5]), 2), 1); assert.equal(I.satLowerBound(new Float64Array([1, 2, 2, 5]), 3), 3); assert.equal(I.satLowerBound(new Float64Array([1, 2, 2, 5]), 6), 4);
});

// ------------------------------------------------------------------ the store
function storeWorld(kind, chunks) {
  const w = world(), st = new w.I.SatStore('https://x', kind || 'alt');
  st.retryMs = 50; st.stallMs = 400; st.timeoutMs = 400;
  w.st = st; w.ptr = pointer(kind || 'alt', chunks);
  return w;
}
test('SatStore: the pointer, then the window\'s chunks nearest the reference first, <= 3 in flight; 404 final, a failure a cooldown; the flat store and the counts', async () => {
  const windows = ['20261103T12', '20261103T15', '20261103T18', '20261103T21', '20261104T00', '20261104T03', '20261104T06', '20261104T09', '20261104T12'];
  const w = storeWorld('alt', windows.map((k) => chunkRec('alt', k, 3))), st = w.st;
  const p = st.load(true);
  assert.equal(st.status, 'loading');
  assert.ok(w.answer(/alt\/v1\/latest\.json$/, w.ptr));
  assert.equal(await p, st); assert.equal(st.status, 'ok');
  const newest = Date.parse(w.ptr.newest_utc);
  st.ensure(newest - 24 * 3.6e6, newest + 1, newest);
  assert.equal(w.pending(/\.bin$/).length, 3, 'three downloads at once');
  assert.deepEqual(w.pending(/\.bin$/).map((u) => /w\/([^/]+)\//.exec(u)[1]), ['20261104T12', '20261104T09', '20261104T06'], 'nearest the reference (the newest) first');
  // a chunk lands (its own window's readings), a 404, a network failure
  const rsFor = (k) => { const w0 = Date.parse(k.slice(0, 4) + '-' + k.slice(4, 6) + '-' + k.slice(6, 8) + 'T' + k.slice(9, 11) + ':00:00Z'); return [0, 1, 2].map((i) => reading(i, { t: w0 + i * 3600000 + 1000 })); };
  assert.ok(w.answer(wre('20261104T12'), chunkBytes('alt', rsFor('20261104T12'), { window: '20261104T12' })));
  assert.ok(w.answer(wre('20261104T09'), null, 404));
  assert.ok(w.fail(wre('20261104T06')));
  assert.ok(await until(() => st.chunks.size === 1 && st.flat && w.pending(/\.bin$/).length === 3)); assert.equal(st.failed[w.ptr.chunks[7].key], true, '404: final');
  assert.equal(typeof st.failed[w.ptr.chunks[6].key], 'number', 'a failure: a cooldown');
  assert.ok(st.flat && st.flat.n === 3 && st.flat.t[0] === rsFor('20261104T12')[0].t && st.flat.swh[0] === Float32Array.from([1])[0]);
  assert.equal(st.count(newest - 24 * 3.6e6, newest + 1), 3);
  assert.equal(st.complete(newest - 24 * 3.6e6, newest + 1), false);
  assert.equal(w.pending(/\.bin$/).length, 3, 'the queue refilled');
  // answer everything else as it is asked for; the failed one comes back after its cooldown and lands too
  for (let round = 0; round < 12 && st.chunks.size < 8; round++) {
    for (const k of windows) w.answer(wre(k), chunkBytes('alt', rsFor(k), { window: k }));
    await new Promise((r) => setTimeout(r, 60));
    await until(() => w.pending(/\.bin$/).length > 0 || st.chunks.size === 8 || Object.keys(st.inflight).length === 0);
  }
  assert.equal(st.chunks.size, 8, 'every chunk but the 404 one');
  assert.equal(st.complete(newest - 24 * 3.6e6, newest + 1), true);
  assert.equal(st.flat.n, 24); for (let i = 1; i < 24; i++) assert.ok(st.flat.t[i] >= st.flat.t[i - 1], 'time-sorted');
  assert.deepEqual(st.slice(Date.parse('2026-11-04T00:00:00Z'), Date.parse('2026-11-04T03:00:00Z')), [12, 15]);
  assert.equal(st.pending(newest - 24 * 3.6e6, newest + 1), 0);
  // idle stops the queue and keeps the samples; abortAll keeps them too
  st.idle(); assert.equal(st.flat.n, 24); assert.equal(st.window, null);
  st.abortAll(); assert.equal(st.flat.n, 24); assert.equal(st.status, 'ok');
});

test('SatStore: a newer pointer is adopted only once the chunks the window needs have landed (no empty picture between pointers); dropped chunks go', async () => {
  const w = storeWorld('alt', [chunkRec('alt', '20261104T09', 2), chunkRec('alt', '20261104T12', 2)]), st = w.st;
  const p = st.load(true); w.answer(/latest\.json$/, w.ptr); await p;
  const newest = Date.parse(w.ptr.newest_utc);
  st.ensure(newest - 24 * 3.6e6, newest + 1, newest);
  const rs = (k, b) => { const w0 = Date.parse(k.slice(0, 4) + '-' + k.slice(4, 6) + '-' + k.slice(6, 8) + 'T' + k.slice(9, 11) + ':00:00Z'); return [reading(0, { t: w0 + 1000, swh: b }), reading(1, { t: w0 + 2000, swh: b })]; };
  w.answer(wre('20261104T09'), chunkBytes('alt', rs('20261104T09', 1), { window: '20261104T09' })); w.answer(wre('20261104T12'), chunkBytes('alt', rs('20261104T12', 1), { window: '20261104T12' }));
  await settle(20);
  assert.equal(st.flat.n, 4);
  // the 12Z window was rebuilt (a new key) and a 15Z window added: the recheck notes the new pointer
  const ptr2 = pointer('alt', [chunkRec('alt', '20261104T09', 2), chunkRec('alt', '20261104T12', 2, '20261104T172000Z'), chunkRec('alt', '20261104T15', 2, '20261104T172000Z')], { newest_utc: '2026-11-04T17:59:50Z', build: '20261104T172000Z' });
  const rc = st.recheck(); w.answer(/latest\.json$/, ptr2); assert.equal(await rc, true);
  assert.equal(st.pointer.build, '20261104T152000Z', 'not adopted yet'); assert.ok(st.next);
  assert.equal(st.flat.n, 4, 'the old picture stays');
  assert.deepEqual(w.pending(/\.bin$/).map((u) => /w\/([^/]+)\/(\w+)\.bin/.exec(u).slice(1).join('/')), ['20261104T12/20261104T172000Z'], 'the new pointer\'s chunk for the window is fetched (the 15Z one lies past the window)');
  w.answer(/\/w\/20261104T12\/20261104T172000Z/, chunkBytes('alt', rs('20261104T12', 2), { window: '20261104T12' }));
  await settle(20);
  assert.equal(st.pointer.build, '20261104T172000Z', 'adopted once the chunk the window needs is in');
  assert.equal(st.chunks.has(w.ptr.chunks[1].key), false, 'the superseded chunk went');
  assert.equal(st.flat.n, 4); assert.equal(st.flat.swh[2], 2, 'the rebuilt window\'s readings');
  // the window moves to the new newest (the controller does this after a recheck): the 15Z chunk is fetched then
  const newest2 = Date.parse(ptr2.newest_utc);
  st.ensure(newest2 - 24 * 3.6e6, newest2 + 1, newest2);
  assert.deepEqual(w.pending(/\.bin$/).map((u) => /w\/([^/]+)\//.exec(u)[1]), ['20261104T15']);
  w.answer(wre('20261104T15'), chunkBytes('alt', rs('20261104T15', 3), { window: '20261104T15' }));
  await settle(20);
  assert.equal(st.flat.n, 6); assert.equal(st.flat.swh[4], 3);
  // the same chunks under a new build stamp: taken quietly
  const ptr3 = Object.assign({}, ptr2, { checked_utc: '2026-11-04T18:00:00Z' });
  const rc3 = st.recheck(); w.answer(/latest\.json$/, ptr3); assert.equal(await rc3, false); assert.equal(st.next, null);
});

test('SatStore: a stalled download frees its slot (a cooldown); a pointer that fails keeps the one in hand; unsupported without DecompressionStream', async () => {
  const w = storeWorld('alt', [chunkRec('alt', '20261104T12', 2)]), st = w.st;
  const p = st.load(true); w.answer(/latest\.json$/, w.ptr); await p;
  const newest = Date.parse(w.ptr.newest_utc);
  st.ensure(newest - 24 * 3.6e6, newest + 1, newest);
  assert.equal(w.pending(/\.bin$/).length, 1);
  assert.ok(await until(() => typeof st.failed[w.ptr.chunks[0].key] === 'number'), 'silent for stallMs: a cooldown');
  assert.equal(w.held[1].done, true, 'the stalled download was dropped');
  assert.ok(await until(() => w.held.length === 3), 'asked again after the cooldown');
  st.loadedAt = 0;
  const p2 = st.load(); w.answer(/latest\.json$/, null, 503); assert.equal(await p2, st, 'a failed re-read keeps the pointer in hand');
  assert.equal(st.status, 'ok');
  // G51 P1: Off during a pointer RE-READ: the next On reads the pointer again (no latched promise), the recheck works
  st.loadedAt = 0;
  const p4 = st.load(); assert.equal(w.pending(/latest/).length, 1);
  st.abortAll(); await p4; await settle();
  assert.equal(st.loading, null); assert.equal(st.status, 'ok');
  st.loadedAt = 0; const nq = w.fetches.length;
  const p5 = st.load(); assert.equal(w.fetches.length, nq + 1, 'fetched again'); w.answer(/latest\.json$/, w.ptr); assert.equal(await p5, st);
  // a hung re-read ends by the watchdog; the next recheck fetches again
  const r1 = st.recheck(); assert.equal(w.pending(/latest/).length, 1);
  assert.ok(await until(() => st.checking === null), 'given up after timeoutMs (the sandbox timers are unref\'d: polled)');
  assert.equal(await r1, false);
  const r2 = st.recheck(); assert.equal(w.pending(/latest/).length, 1, 'a fresh request'); w.answer(/latest\.json$/, w.ptr); await r2;
  // a chunk whose header does not match the pointer's record (another window, another count) is refused for good
  const w3 = storeWorld('alt', [chunkRec('alt', '20261104T12', 1)]), st3 = w3.st;
  const p3 = st3.load(true); w3.answer(/latest\.json$/, w3.ptr); await p3;
  st3.ensure(T0, T0 + 3.6e6, T0);
  w3.answer(/\.bin$/, chunkBytes('alt', [reading(0, { t: T0 + 1000 }), reading(1, { t: T0 + 2000 })], { window: '20261104T12' }));
  assert.ok(await until(() => st3.failed[w3.ptr.chunks[0].key] === true), 'two readings for a record of one');
  const w2 = world(); const st2 = new w2.I.SatStore('https://x', 'alt'); st2.supported = () => false;
  assert.equal(await st2.load(true), null); assert.equal(st2.status, 'unsupported');
  st.abortAll();
});

// ------------------------------------------------------------------ the layer
function tile(z, x, y) { return { z, x, y }; }
test('SatLayer: dots drawn newest last (on top), hollow near land, alpha by age; empty -> empty touches nothing; the split draw; hitAt = the top-most drawn', async () => {
  const w = storeWorld('alt', [chunkRec('alt', '20261104T12', 6)]), st = w.st;
  const p = st.load(true); w.answer(/latest\.json$/, w.ptr); await p;
  // four readings at one spot (20 N, 150 W), 1 h apart, the third near land; one far away
  const rs = [0, 1, 2, 3].map((i) => reading(i, { t: T0 + i * 3.6e6, lat: 20, lon: -150, swh: 1 + i, flags: i === 2 ? 1 : 0 })).concat([reading(4, { t: T0 + 100, lat: -40, lon: 20, swh: 2 }), reading(5, { t: T0 + 4 * 3.6e6, lat: 20, lon: -150, swh: 7 })]);   // the last one sits exactly on the window's end
  rs.sort((a, b) => a.t - b.t);
  const layer = new w.I.SatLayer(); layer._map = null;                   // off a map: every tile is drawn at once (the Node stubs)
  layer.setStore(st, 'dots');
  const Z = 7, pp = w.I.pixelOf(20, -150, Z), pa = w.I.pixelOf(-40, 20, Z);   // zoom 7: every reading drawn (no stride)
  const pac = tile(Z, pp.x, pp.y), atl = tile(Z, pa.x, pa.y), key = (c) => c.x + ':' + c.y + ':' + c.z;
  const elP = w.g.document.createElement('canvas'), elA = w.g.document.createElement('canvas'), elX = w.g.document.createElement('canvas');
  layer._tiles = {}; layer._tiles[key(pac)] = { el: elP, coords: pac }; layer._tiles[key(atl)] = { el: elA, coords: atl }; layer._tiles[key(tile(Z, 3, 3))] = { el: elX, coords: tile(Z, 3, 3) };
  layer.setWindow({ t0: T0, t1: T0 + 4 * 3.6e6, ref: T0 + 3 * 3.6e6, span: 24 * 3.6e6, frame: false });
  assert.equal(elP.calls.length, 0, 'no samples yet: nothing touched');
  w.answer(/\.bin$/, chunkBytes('alt', rs, { window: '20261104T12', segments: [[0, 0, 6]] }));
  assert.ok(await until(() => elP.calls.filter((c) => c[0] === 'arc').length >= 4));
  const arcs = elP.calls.filter((c) => c[0] === 'arc');
  assert.equal(arcs.length, 4, 'the four readings at the spot (the one exactly at t1 lies outside the half-open window)');
  assert.equal(elX.calls.length, 0, 'an empty tile is never touched');
  assert.ok(arcs[0][4] < arcs[3][4] && arcs[3][4] === 1, 'drawn oldest to newest: the newest (alpha 1) last, on top');
  const fills = elP.calls.filter((c) => c[0] === 'fill');
  assert.equal(fills.length, 3, 'the near-land reading is hollow (rim only)');
  assert.equal(elA.calls.filter((c) => c[0] === 'arc').length, 1);
  assert.equal(layer.count(), 5); assert.equal(layer.complete(), true);
  const hit = layer.hitAt(20, -150, Z);
  assert.equal(st.flat.t[hit], T0 + 3 * 3.6e6, 'the top-most drawn = the newest of the stack');
  assert.equal(layer.hitAt(20, -140, Z), -1);
  const px7 = 360 / (256 * 128);                                           // one tile pixel at zoom 7, in degrees of longitude
  assert.equal(layer.hitAt(20, -150 + 4 * px7, Z), hit, '4 px away: within the dot\'s radius + 3 px');
  assert.equal(layer.hitAt(20, -150 + 6.5 * px7, Z), -1, '6.5 px away: beyond it');
  // a narrower window: the newest two only; a world copy draws the same
  layer.setWindow({ t0: T0 + 2 * 3.6e6, t1: T0 + 4 * 3.6e6, ref: T0 + 3 * 3.6e6, span: 2 * 3.6e6, frame: false });
  assert.equal(elP.calls.filter((c) => c[0] === 'arc').length, 4 + 2);
  assert.equal(st.flat.t[layer.hitAt(20, -150, Z)], T0 + 3 * 3.6e6);
  const wc = tile(Z, pac.x + 128, pac.y), copy = w.g.document.createElement('canvas'); layer._tiles[key(wc)] = { el: copy, coords: wc }; layer._draw(copy, wc);
  const a1 = elP.calls.filter((c) => c[0] === 'arc').slice(-2), a2 = copy.calls.filter((c) => c[0] === 'arc');
  assert.equal(a2.length, a1.length);
  for (let k = 0; k < a1.length; k++) assert.ok(Math.abs(a2[k][1] - a1[k][1]) < 1e-9 && Math.abs(a2[k][2] - a1[k][2]) < 1e-9, 'a world copy draws the same pixels');
  // a window set again (the own window of another reference); setWindow(null) clears the tiles that had dots and no other
  layer.setWindow({ t0: T0 - 3 * 3.6e6, t1: T0 + 3 * 3.6e6 + 1, ref: T0 + 3 * 3.6e6, span: 6 * 3.6e6, frame: false, entry: null });
  const before = elX.calls.length, pb = elP.calls.length;
  layer.setWindow(null);
  assert.equal(elX.calls.length, before, 'an empty tile stays untouched'); assert.ok(elP.calls.length > pb && elP.calls[elP.calls.length - 1][0] === 'clear');
  assert.equal(layer.hitAt(20, -150, Z), -1);
  // the split draw: only the tiles in view are drawn at once; the buffer tile waits for idle time
  layer._map = w.map; w.map._z = Z;
  w.map.getPixelBounds = () => ({ min: { x: pac.x * 256, y: pac.y * 256 }, max: { x: pac.x * 256 + 256, y: pac.y * 256 + 256 } });   // the view = the Pacific tile alone
  const n0 = elA.calls.length, p0 = elP.calls.length;
  layer.setWindow({ t0: T0, t1: T0 + 4 * 3.6e6, ref: T0 + 3 * 3.6e6, span: 24 * 3.6e6, frame: false });
  assert.equal(elA.calls.length, n0, 'the buffer tile is not drawn on the stack');
  assert.ok(elP.calls.length > p0);
  assert.ok(await until(() => elA.calls.length > n0), 'drawn in idle time');
  // arrows: one line + head per point, the length by the peak period, the colour by the dominant height
  const w2 = storeWorld('swell', [chunkRec('swell', '20261104T12', 1)]), st2 = w2.st;
  const p2 = st2.load(true); w2.answer(/latest\.json$/, w2.ptr); await p2;
  const l2 = new w2.I.SatLayer(); l2._map = null; l2.setStore(st2, 'arrows');
  const el2 = w2.g.document.createElement('canvas'); l2._tiles = {}; l2._tiles[key(pac)] = { el: el2, coords: pac };
  l2.setWindow({ t0: T0, t1: T0 + 3.6e6, ref: T0 + 3.6e6, span: 24 * 3.6e6, frame: false });
  w2.answer(/\.bin$/, chunkBytes('swell', [{ t: T0 + 1000, lat: 20, lon: -150, n: 2, mission: 0, flags: 0, hs1: 2.0, hs2: 1.5, hs3: NaN, tp1: 14, tp2: 9, tp3: NaN, dir1: 180, dir2: 100, dir3: NaN }], { window: '20261104T12', columns: SWELL_COLS, format: 'swell-v1' }));
  assert.ok(await until(() => el2.calls.filter((c) => c[0] === 'line').length >= 3));
  const mv = el2.calls.filter((c) => c[0] === 'move'), ln = el2.calls.filter((c) => c[0] === 'line');
  assert.ok(mv.length >= 2 && ln.length >= 3, 'a shaft and a head');
  assert.ok(Math.abs((mv[0][2] - ln[0][2]) - w2.I.satArrowLength(14)) < 1e-6 && Math.abs(ln[0][1] - mv[0][1]) < 1e-9, 'from 180 = travelling north: the shaft runs up the tile (y falling) by the period\'s length');
  assert.equal(st2.flat.hs[0][l2.hitAt(20, -150, Z)], 2);
  // G51 P0: two dots overlapping (1.5 px apart at zoom 8, dot radius 3): the cursor over the OLDER dot's centre, inside the
  // newer dot painted over it, reads the newer (the colour under the cursor); beyond both discs the nearest centre
  const w3 = storeWorld('alt', [chunkRec('alt', '20261104T12', 2)]), st3 = w3.st;
  const p3 = st3.load(true); w3.answer(/latest\.json$/, w3.ptr); await p3;
  const px8 = 360 / (256 * 256);
  const l3 = new w3.I.SatLayer(); l3._map = null; l3.setStore(st3, 'dots');
  const c8 = w3.I.pixelOf(0, -150, 8), t8 = tile(8, c8.x, c8.y), el3 = w3.g.document.createElement('canvas'); l3._tiles = {}; l3._tiles[key(t8)] = { el: el3, coords: t8 };
  l3.setWindow({ t0: T0, t1: T0 + 3.6e6, ref: T0 + 3.6e6, span: 24 * 3.6e6, frame: false });
  w3.answer(/\.bin$/, chunkBytes('alt', [reading(0, { t: T0 + 1000, lat: 0, lon: -150, swh: 1 }), reading(1, { t: T0 + 2000, lat: 0, lon: -150 + 1.5 * px8, swh: 6 })], { window: '20261104T12' }));
  assert.ok(await until(() => el3.calls.filter((c) => c[0] === 'arc').length === 2));
  assert.equal(st3.flat.swh[l3.hitAt(0, -150, 8)], 6, 'over the older centre: the newer dot drawn on top');
  assert.equal(st3.flat.swh[l3.hitAt(0, -150 - 2.9 * px8, 8)], 1, 'inside the older disc only');
  assert.equal(st3.flat.swh[l3.hitAt(0, -150 - 4.5 * px8, 8)], 1, 'beyond both discs: the nearest centre within the slack');
  assert.equal(l3.hitAt(0, -150 - 7 * px8, 8), -1);
  // an arrow's head and tail answer too (G51 P2): 14 s = a 19.4-px shaft at zoom 7
  assert.equal(st2.flat.hs[0][l2.hitAt(20 + 8 * (360 / (256 * 128)) * Math.cos(20 * Math.PI / 180), -150, Z)], 2, '8 px up the shaft (travelling north)');
});

// ------------------------------------------------------------------ the controller
function ctl(w, opts) {
  const ov = w.AO.create(w.map, Object.assign({ base: 'https://x/gfswave/0p25/v1', panel: w.g.document.createElement('div'), tz: 'Pacific/Honolulu', getUnit: () => 'US',
    fmtTime: (iso) => 'T' + iso.slice(11, 16), tzAbbr: () => 'HST', pageCycle: () => null, version: '9.9.9', sat: true, swell: true }, opts || {}));
  ov._syncLook = function () {}; ov._dims = () => ({ w: 1200, h: 800 });
  ov._coastFirst = (p) => p; ov.coast = null;
  return ov;
}
test('controller: the own entry mounts in mode obs with the layer in obsPane, saves the field, credits Copernicus Marine, draws the last 24 h; the panel; Off', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('sat');
  assert.equal(ov.mode, 'obs'); assert.equal(ov.field, 'sat'); assert.equal(w.saved.field, 'sat');
  assert.deepEqual(w.pending(), ['https://x/alt/v1/latest.json'], 'the heights pointer, nothing else');
  const ptr = pointer('alt', [chunkRec('alt', '20261104T09', 1), chunkRec('alt', '20261104T12', 2)]);
  w.answer(/latest\.json$/, ptr); await settle(20);
  assert.ok(ov.satLayer && w.panes.obsPane && w.panes.obsPane.style.zIndex === 260 && w.layers.indexOf(ov.satLayer) >= 0, 'the layer in its own pane');
  assert.equal(ov.satLayer.mode, 'dots'); assert.equal(ov.flow, null, 'no particles');
  const newest = Date.parse(ptr.newest_utc);
  assert.deepEqual([ov.satLayer.win.t0, ov.satLayer.win.t1, ov.satLayer.win.ref, ov.satLayer.win.frame], [newest - 24 * 3.6e6, newest + 1, newest, false]);
  assert.equal(w.pending(/\.bin$/).length, 2, 'both chunks asked for');
  assert.ok(w.attrs.length === 1 && /Copernicus Marine/.test(w.attrs[0]) && /^Overlay: /.test(w.attrs[0]));
  assert.equal(ov.last.state, 'ready');
  const t = ov.opts.panel.children.map((c) => c.textContent || '').join(' ');
  assert.ok(ov.ui.obs && ov.ui.satLine && /Readings: last 24 h, newest T14:59 HST \(\d+ h ago\) · loading 0 of 2…/.test(ov.ui.satLine.textContent), ov.ui.satLine.textContent);
  assert.ok(ov.ui.satSeg, 'the Heights | Swell switch (the swell is offered)');
  assert.ok(!ov.ui.play && !ov.ui.ribbon, 'no player');
  w.answer(wre('20261104T12'), chunkBytes('alt', [reading(0, { t: T0 + 1000 }), reading(1, { t: T0 + 2000, mission: 1 })], { window: '20261104T12', segments: [[0, 0, 1], [1, 1, 1]] }));
  w.answer(wre('20261104T09'), chunkBytes('alt', [reading(0, { t: T0 - 3.6e6 })], { window: '20261104T09' }));
  assert.ok(await until(() => ov.sat.flat && ov.sat.flat.n === 3 && /satellites$/.test(ov.ui.satLine.textContent)));
  assert.ok(/· 2 satellites$/.test(ov.ui.satLine.textContent), ov.ui.satLine.textContent);
  assert.equal(ov.satLayer.count(), 3);
  const deepText = (n) => (n.textContent || '') + ' ' + (n.children || []).map(deepText).join(' ');
  assert.ok(deepText(ov.opts.panel).indexOf(I.SAT_KEY) >= 0 && /Hollow dots/.test(I.SAT_KEY), 'the hollow dots explained on the panel (G52 B-2)');
  // the hover: a reading under the cursor, else nothing on this entry
  assert.equal(ov._satReadout(ov.satLayer.store.flat.n - 1, { lat: 10.1, lng: -149.9 }, 3), '3.6 ft · Sentinel-3A · T12:00 HST · model 4.3 ft (-0.7)');
  assert.equal(ov._satReadout(0, { lat: 10, lng: -150 }, 3), '3.3 ft · Jason-3 · T11:00 HST · model 3.9 ft (-0.6)', 'the difference of the printed values (3.3 - 3.9), not of the raw ones (-0.66)');
  // G52 B-1 / B-2 / B-5: a model-edge reading shows the model's value without a comparison, a near-land one says so, the
  // difference is that of the two numbers printed (2.5 - 1.2 = 1.3, not 1.4); B-3: Jason-3 and its OGDR feed count once
  {
    const f = ov.satLayer.store.flat, save = { flags: f.flags.slice(), swh: f.swh.slice(), model: f.model.slice(), missions: f.missions };
    f.flags[0] = I.SAT_FLAG_MODEL_EDGE; assert.equal(ov._satReadout(0, { lat: 10, lng: -150 }, 3), '3.3 ft · Jason-3 · T11:00 HST · model 3.9 ft (edge)');
    f.flags[0] = I.SAT_FLAG_NEAR_LAND; assert.equal(ov._satReadout(0, { lat: 10, lng: -150 }, 3), '3.3 ft · Jason-3 · T11:00 HST · near land · model 3.9 ft (-0.6)');
    f.flags[0] = 0; f.swh[0] = 2.5 / 3.28084 + 0.0004; f.model[0] = 1.2 / 3.28084 - 0.0004;
    assert.equal(ov._satReadout(0, { lat: 10, lng: -150 }, 3), '2.5 ft · Jason-3 · T11:00 HST · model 1.2 ft (+1.3)');
    f.missions = [{ id: 'j3', short: 'Jason-3' }, { id: 'j3_ogdr', short: 'Jason-3 OGDR' }];
    ov._satLineSync(); assert.ok(/· 1 satellite$/.test(ov.ui.satLine.textContent), 'one spacecraft: ' + ov.ui.satLine.textContent);
    f.flags.set(save.flags); f.swh.set(save.swh); f.model.set(save.model); f.missions = save.missions; ov._satLineSync();
    assert.ok(/· 2 satellites$/.test(ov.ui.satLine.textContent));
    // G52 B-6 / B-7: a needed chunk retrying after a failure, or one the bucket lacks, is named on the line
    const key = ov.sat.pointer.chunks[0].key, had = ov.sat.chunks.get(key);
    ov.sat.chunks.delete(key); ov.sat.failed[key] = Date.now() + 60000; ov._satLineSync();
    assert.ok(/loading 1 of 2… \(1 retrying\)$/.test(ov.ui.satLine.textContent), ov.ui.satLine.textContent);
    ov.sat.failed[key] = true; ov._satLineSync();
    assert.ok(/· 1 of 2 windows$/.test(ov.ui.satLine.textContent), ov.ui.satLine.textContent);
    delete ov.sat.failed[key]; ov.sat.chunks.set(key, had); ov._satLineSync();
    assert.ok(/· 2 satellites$/.test(ov.ui.satLine.textContent));
  }
  // G51 P1: a pointer adopted once its chunk landed (after the recheck resolved) moves the window to its newest reading
  const ptr2 = pointer('alt', [chunkRec('alt', '20261104T09', 1), chunkRec('alt', '20261104T12', 2, '20261104T172000Z'), chunkRec('alt', '20261104T15', 1, '20261104T172000Z')], { newest_utc: '2026-11-04T17:29:00Z', build: '20261104T172000Z' });
  ov._satRecheck(); w.answer(/latest\.json$/, ptr2); await settle(20);
  assert.equal(ov.sat.pointer.build, '20261104T152000Z', 'not adopted yet: the 12Z chunk of the new pointer is out');
  w.answer(/\/w\/20261104T12\/20261104T172000Z/, chunkBytes('alt', [reading(0, { t: T0 + 1000 }), reading(1, { t: T0 + 2000, mission: 1 })], { window: '20261104T12', segments: [[0, 0, 1], [1, 1, 1]] }));
  assert.ok(await until(() => ov.sat.pointer.build === '20261104T172000Z'));
  assert.equal(ov.satLayer.win.ref, Date.parse('2026-11-04T17:29:00Z'), 'the window follows the adopted pointer\'s newest');
  assert.deepEqual(w.pending(/\.bin$/).map((u) => /w\/([^/]+)\//.exec(u)[1]), ['20261104T15'], 'the window now reaches the 15Z chunk');
  w.answer(/\/w\/20261104T15\//, chunkBytes('alt', [reading(0, { t: T0 + 5 * 3.6e6 + 29 * 60000 })], { window: '20261104T15' }));
  assert.ok(await until(() => ov.satLayer.count() === 4 && /newest T17:29/.test(ov.ui.satLine.textContent)), ov.ui.satLine.textContent);
  // Off: the layer goes, the store keeps its samples, no more requests
  const n = w.fetches.length;
  assert.ok(ov.satTimer, 'the 12-minute recheck runs while attached');
  ov.unmount();
  assert.equal(ov.satTimer, null, 'Off clears it');
  assert.equal(ov.satLayer, null); assert.equal(w.layers.length, 0); assert.equal(w.fetches.length, n); assert.equal(ov.sat.flat.n, 4);
  assert.equal(w.attrs.length, 0);
});

test('controller: the Heights | Swell switch takes the swell store (remembered per tab); a swell point\'s hover lists every partition; the stale line', async () => {
  const w = world(), ov = ctl(w);
  ov.mount('sat');
  w.answer(/alt\/v1\/latest\.json$/, pointer('alt', [chunkRec('alt', '20261104T12', 1)])); await settle(20);
  ov.setSatKind('swell');
  assert.equal(w.saved.satKind, 'swell'); assert.equal(ov.last.state, 'loading');
  assert.deepEqual(w.pending(/latest/), ['https://x/sarswell/v1/latest.json']);
  w.answer(/sarswell\/v1\/latest\.json$/, pointer('swell', [chunkRec('swell', '20261104T12', 1)], { newest_utc: '2026-11-04T12:10:00Z' })); await settle(20);
  assert.equal(ov.satLayer.store.kind, 'swell'); assert.equal(ov.satLayer.mode, 'arrows'); assert.equal(ov.last.state, 'ready');
  assert.equal(ov.sat.window, null, 'the heights store went idle');
  w.answer(/sarswell.*\.bin$/, chunkBytes('swell', [{ t: T0 + 600000, lat: -30, lon: 170, n: 2, mission: 0, flags: 0, hs1: 2.0, hs2: 1.5, hs3: NaN, tp1: 14.25, tp2: 9, tp3: NaN, dir1: 215.5, dir2: 100, dir3: NaN }], { window: '20261104T12', columns: SWELL_COLS, format: 'swell-v1', missions: [{ id: 's1c', short: 'Sentinel-1C', name: 'Sentinel-1C' }] }));
  assert.ok(await until(() => ov.swell.flat && ov.swell.flat.n === 1));
  const now0 = Date.now; Date.now = () => T0 + 6 * 3.6e6 + 600000;
  try { assert.equal(ov._satReadout(0, { lat: -30, lng: 170 }, 3), 'Swell 1: 6.6 ft · 14 s · from 216° | Swell 2: 4.9 ft · 9 s · from 100° (Sentinel-1C, 6 h ago)'); } finally { Date.now = now0; }
  ov.setSatKind('heights'); await settle(5);
  assert.equal(ov.satLayer.store.kind, 'alt'); assert.equal(w.saved.satKind, 'heights');
  // a reload with the swell remembered starts on it
  const w2 = world(); w2.saved = { field: 'sat', satKind: 'swell' };
  const ov2 = ctl(w2); assert.equal(ov2.satKind, 'swell');
  const w3 = world(); w3.saved = { field: 'sat', satKind: 'swell' };
  const ov3 = ctl(w3, { swell: false }); assert.equal(ov3.satKind, 'heights', 'not offered: heights');
  // the stale line past 12 h
  const now1 = Date.now; Date.now = () => Date.parse('2026-11-05T05:00:00Z');
  try { ov.render({ state: 'ready' }); assert.match(allText(ov.ui.host), /newest satellite reading is 14 h old/); } finally { Date.now = now1; }
  ov.unmount();
});

// a model world: a manifest and a drawn frame faked (the frame mode needs no real decode)
function modelWorld(w, ov) {
  const frames = [...Array(6).keys()].map((i) => ({ step: i, valid_utc: new Date(T0 + i * 3.6e6).toISOString().replace('.000Z', 'Z') }));
  ov.manifest = { run: '2026110412', run_utc: '2026-11-04T12:00:00Z', published_utc: '2026-11-04T15:00:00Z', frames, fields: { hs: { lo: 0, hi: 22.86, legend: [0, 18.288], units: 'm', interpolation: 'bilinear' } }, model: {} };
  ov.pointer = { published_utc: '2026-11-04T15:00:00Z' }; ov.n = frames.length; ov.field = 'hs'; ov.mode = 'model';
  ov.layer = { field: 'hs', entry: frames[2], _clip: false, fdef: ov.manifest.fields.hs, hasFrame: () => true, hasLake: () => false, _masked: () => false, readoutAt: () => 1.8, clear() {}, _src: null, _lake: null, setSource() {} };
  ov.frameIndex = 2;
  return frames;
}
// ------------------------------------------------------------------ the gridded map (plan section 82)
const GRID_NEWEST = Date.parse('2026-11-03T18:00:00Z');
function gridPointer(extra) {
  return Object.assign({ format: 'altgrid-v1', day: '2026-11-03', run: '20261103', manifest: 'altgrid/v1/20261103/manifest-20261105T084700Z.json', complete: true,
    newest_utc: '2026-11-03T18:00:00Z', production: '20261104T073010Z' }, extra || {});
}
function gridManifest(extra, prod) {
  prod = prod || '20261104T073010Z';
  return Object.assign({ format: 'altgrid-v1', day: '2026-11-03', run: '20261103', complete: true, encoding: 'u8-linear-v2',
    grid: { cols: 720, rows: 361, lon0: -180, lat0: 90, dlon: 0.5, dlat: -0.5, registration: 'center', lon_periodic: true },
    fields: { hs: { lo: 0, hi: 22.86, legend: [0, 18.288], units: 'm', interpolation: 'bilinear' } },
    frames: [0, 6, 12, 18].map((h) => ({ step: h, valid_utc: '2026-11-03T' + String(h).padStart(2, '0') + ':00:00Z', file: 'altgrid/v1/20261103/' + prod + '/hs-' + String(h).padStart(2, '0') + '.png' })) }, extra || {});
}
const gridFrame = () => Promise.resolve({ q: new Uint8Array(720 * 361).fill(40), cols: 720, rows: 361 });
const atAge = (h, fn) => { const now0 = Date.now; Date.now = () => GRID_NEWEST + h * 3.6e6; try { return fn(); } finally { Date.now = now0; } };

test('GridStore: the pointer (no-cache), the manifest, the NEWEST step; every rejection; a failure cools down; a failed re-read keeps the frame; recheck notes a newer manifest', async () => {
  const w = world(), st = new w.I.GridStore('https://x');
  const decoded = []; st._decode = (url, sig, expect) => { decoded.push([url, expect.cols, expect.rows]); return gridFrame(); };
  const p = st.load(true);
  assert.deepEqual(w.pending(), ['https://x/altgrid/v1/latest.json']);
  w.answer(/altgrid\/v1\/latest\.json$/, gridPointer()); await settle(5);
  assert.deepEqual(w.pending(), ['https://x/altgrid/v1/20261103/manifest-20261105T084700Z.json']);
  w.answer(/manifest-/, gridManifest()); assert.equal(await p, st);
  assert.deepEqual(decoded, [['https://x/altgrid/v1/20261103/20261104T073010Z/hs-18.png', 720, 361]], 'the newest step, the half grid');
  assert.equal(st.status, 'ok'); assert.equal(st.entry.step, 18); assert.equal(st.newestMs(), GRID_NEWEST); assert.equal(st.frame.rows, 361);
  assert.ok(Math.abs(atAge(31, () => st.ageH()) - 31) < 1e-9);
  // the pointer and manifest rules
  const V = w.I.validGridPointer, M = w.I.validGridManifest, ptr = gridPointer();
  assert.ok(V(ptr) && M(gridManifest(), ptr));
  for (const bad of [{ format: 'sst-v1' }, { complete: false }, { run: '2026110' }, { manifest: 'sst/v1/x.json' }, { manifest: 'altgrid/v1/../x.json' }, { newest_utc: 'soon' }]) assert.equal(V(gridPointer(bad)), false, JSON.stringify(bad));
  const g = gridManifest().grid, f = gridManifest().fields;
  for (const bad of [{ run: '20261102' }, { encoding: 'u16' }, { grid: Object.assign({}, g, { rows: 359 }) }, { grid: Object.assign({}, g, { lat0: 89.5 }) },
    { fields: { hs: Object.assign({}, f.hs, { hi: 15 }) } }, { frames: [] }, { frames: gridManifest().frames.concat([gridManifest().frames[0]]) },
    { frames: gridManifest().frames.slice().reverse() }, { frames: [{ step: 0, valid_utc: '2026-11-03T00:00:00Z', file: 'sst/v1/a.png' }] }]) assert.equal(M(gridManifest(bad), ptr), false, JSON.stringify(Object.keys(bad)));
  // a first failure: 'failed', a cooldown for automatic loads, a viewer's action tries at once
  const s2 = new w.I.GridStore('https://y'); s2._decode = gridFrame;
  const q2 = s2.load(false); w.answer(/y\/altgrid\/v1\/latest\.json$/, null, 503);
  assert.equal(await q2, null); assert.equal(s2.status, 'failed');
  const n = w.fetches.length;
  assert.equal(await s2.load(false), null); assert.equal(w.fetches.length, n, 'within the cooldown: nothing asked');
  s2.load(true); assert.equal(w.fetches.length, n + 1, 'forced: asked at once');
  // recheck: the same manifest -> false; a newer one -> newer noted; the re-read whose manifest fails keeps the frame
  const r1 = st.recheck(); w.answer(/x\/altgrid\/v1\/latest\.json$/, gridPointer()); assert.equal(await r1, false);
  const r2 = st.recheck(); w.answer(/x\/altgrid\/v1\/latest\.json$/, gridPointer({ manifest: 'altgrid/v1/20261103/manifest-20261105T101000Z.json' })); assert.equal(await r2, true);
  const frame0 = st.frame, q3 = st.load(false);
  w.answer(/x\/altgrid\/v1\/latest\.json$/, gridPointer({ manifest: 'altgrid/v1/20261103/manifest-20261105T101000Z.json' })); await settle(5);
  w.answer(/manifest-20261105T101000Z/, null, 404);
  assert.equal(await q3, st); assert.equal(st.status, 'ok'); assert.equal(st.frame, frame0, 'a failed re-read keeps the step on the map');
  // abortAll keeps the frame
  st.newer = gridPointer({ manifest: 'altgrid/v1/20261103/manifest-20261105T120000Z.json' }); st.load(false); st.abortAll();
  assert.equal(st.status, 'ok'); assert.equal(st.loadAbort, null); assert.equal(st.frame, frame0);
});

test('controller: the "Gridded map" box (off by default, per tab) draws the newest step in the entry\'s field layer under the readings; Heights | Swell keeps it; Off clears it, the store keeps it', async () => {
  const now0 = Date.now; Date.now = () => GRID_NEWEST + 31 * 3.6e6;   // the fixture's clock, never the wall's (G54-2)
  try {
  const w = world(), ov = ctl(w, { grid: true });
  ov.grid._decode = gridFrame;
  assert.equal(ov.satGrid, false);
  ov.mount('sat');
  w.answer(/alt\/v1\/latest\.json$/, pointer('alt', [chunkRec('alt', '20261104T12', 1)])); await settle(20);
  assert.equal(ov.last.state, 'ready');
  assert.ok(ov.ui.gridBox && ov.ui.gridBox.checked === false && ov.ui.gridNote.textContent === '' && ov.ui.gridBox.getAttribute('aria-describedby') === 'ovGridNote');
  assert.ok(allText(ov.opts.panel).indexOf('Gridded map') >= 0);
  assert.equal(w.pending(/altgrid/).length, 0, 'off: nothing asked');
  ov.setSatGrid(true);
  assert.equal(w.saved.satGrid, true);
  assert.equal(ov.ui.gridNote.textContent, 'loading…');
  w.answer(/altgrid\/v1\/latest\.json$/, gridPointer()); await settle(5);
  w.answer(/manifest-/, gridManifest());
  assert.ok(await until(() => ov.layer.hasFrame()));
  assert.equal(ov.layer.field, 'sat'); assert.equal(ov.layer.entry.step, 18); assert.equal(ov.layer._lut, w.I.satLut()); assert.equal(ov.layer.fdef, w.I.SAT_FDEF);
  assert.equal(ov.layer.options.pane, 'modelPane', 'under the readings (obsPane)');
  atAge(31, () => ov._gridLineSync());
  assert.equal(atAge(31, () => ov._gridLineText()), 'CMEMS L4 · T18:00 HST (31 h ago)');
  assert.ok(/doi:10\.48670\/moi-00180/.test(allText(ov.opts.panel)), 'the credit line names the gridded product');
  assert.ok(/moi-00180/.test(w.I.SAT_ATTRIBUTION), 'the map credit\'s title too');
  // the hover: a dot gets " · grid"; open water the grid alone; the field layer's value v is what the readout hands over
  w.answer(wre('20261104T12'), chunkBytes('alt', [reading(0, { t: T0 + 1000 })], { window: '20261104T12' }));
  assert.ok(await until(() => ov.sat.flat && ov.sat.flat.n === 1));
  assert.equal(ov._satReadout(0, { lat: 10, lng: -150 }, 3, 2.5).split(' · grid ')[1], '8.2 ft');
  assert.equal(ov._gridReadout(2.5), 'grid 8.2 ft · T18:00 HST');
  assert.equal(ov._gridReadout(30), 'grid ≥ 98.4 ft · T18:00 HST');
  // through the readout's own handler (the map's mousemove): over water with no reading under the cursor, the grid alone;
  // with the box off, nothing (the entry shows nothing but readings)
  const move = (lat, lng) => { for (const f of w.reg.map.mousemove || []) f({ originalEvent: {}, latlng: { lat, lng }, containerPoint: { x: 5, y: 5 } }); return ov.readout; };
  const r = move(-40, 20);
  assert.equal(r.hidden, false); assert.ok(/^grid [\d.]+ ft · T18:00 HST$/.test(r.textContent), r.textContent);
  assert.ok(Math.abs(ov.layer.valueAt(-40, 20) - (39 / 254) * 22.86) < 1e-9, 'code 40 = (40 - 1) / 254 of the scale');
  assert.equal(w.I.CLIP_FIELDS.sat, true, 'the gridded map is clipped to the coast like the model fields');
  // Heights | Swell keeps the map
  ov.setSatKind('swell');
  w.answer(/sarswell\/v1\/latest\.json$/, pointer('swell', [chunkRec('swell', '20261104T12', 1)], { newest_utc: '2026-11-04T12:10:00Z' })); await settle(20);
  assert.ok(ov.layer.hasFrame() && ov.layer.field === 'sat', 'the map stays under the arrows');
  assert.ok(/^Readings: 24 h to T12:10 HST \(/.test(ov.ui.satLine.textContent), 'the swell line names the window\'s end: ' + ov.ui.satLine.textContent);
  ov.setSatKind('heights'); await settle(5);
  // Off: the layer clears, the store keeps its frame; On again asks nothing (fresh) and draws at once
  ov.setSatGrid(false);
  assert.equal(ov.layer.hasFrame(), false); assert.ok(ov.grid.frame); assert.equal(w.saved.satGrid, false);
  assert.equal(ov._satReadout(0, { lat: 10, lng: -150 }, 3, 2.5).indexOf('grid'), -1, 'no grid value with the box off');
  assert.equal(move(-40, 20).hidden, true, 'the box off: nothing over open water');
  const lastOf = (el, cls) => { let hit = null; (function walk(n) { if ((n.className || '').indexOf(cls) >= 0) hit = n; (n.children || []).forEach(walk); })(el); return hit; };
  assert.ok(!/moi-00180/.test(lastOf(ov.opts.panel, 'ov-sat-credit').textContent), 'the newest credit line drops the gridded DOI with the box off');
  const n = w.fetches.length;
  ov.setSatGrid(true); await settle(5);
  assert.ok(ov.layer.hasFrame()); assert.equal(w.fetches.length, n, 'nothing asked again');
  // another observation entry clears it (never under another label)
  ov.mount('sst'); await settle(5);
  assert.equal(ov.layer.hasFrame(), false);
  ov.unmount();
  // a saved box without the setting: off, no row
  const w2 = world(); w2.saved = { field: 'sat', satGrid: true };
  const ov2 = ctl(w2); assert.equal(ov2.satGrid, false); assert.equal(ov2.grid, null);
  ov2.mount('sat'); w2.answer(/alt\/v1\/latest\.json$/, pointer('alt', [chunkRec('alt', '20261104T12', 1)])); await settle(20);
  assert.equal(ov2.ui.gridBox, undefined); assert.equal(w2.pending(/altgrid/).length, 0);
  ov2.unmount();
  // a saved box with the setting: on at the mount
  const w3 = world(); w3.saved = { field: 'sat', satGrid: true };
  const ov3 = ctl(w3, { grid: true }); ov3.grid._decode = gridFrame; assert.equal(ov3.satGrid, true);
  ov3.mount('sat'); w3.answer(/alt\/v1\/latest\.json$/, pointer('alt', [chunkRec('alt', '20261104T12', 1)])); await settle(20);
  assert.deepEqual(w3.pending(/altgrid/), ['https://x/altgrid/v1/latest.json']);
  ov3.unmount();
  assert.equal(ov3.grid.status, 'idle', 'Off stops the load in progress');
  } finally { Date.now = now0; }
});

test('controller: the gridded map\'s age: the note, a warning past 48 h, hidden past 72 h; the 12-min recheck adopts a newer production in place; a failure says unavailable', async () => {
  const now0 = Date.now; Date.now = () => GRID_NEWEST + 31 * 3.6e6;   // the fixture's clock, never the wall's (G54-2)
  try {
  const w = world(), ov = ctl(w, { grid: true });
  ov.grid._decode = gridFrame;
  w.saved = { satGrid: true };
  const ovA = ctl(w, { grid: true }); ovA.grid._decode = gridFrame;
  ovA.mount('sat');
  w.answer(/alt\/v1\/latest\.json$/, pointer('alt', [chunkRec('alt', '20261104T12', 1)])); await settle(20);
  w.answer(/altgrid\/v1\/latest\.json$/, gridPointer()); await settle(5);
  w.answer(/manifest-/, gridManifest());
  assert.ok(await until(() => ovA.layer.hasFrame()));
  atAge(49, () => { ovA._gridLineSync(); });
  assert.equal(ovA._gridWarnShown, 'warn');
  assert.ok(allText(ovA.opts.panel).indexOf('The gridded map is 49 h old.') >= 0, 'the warning line (rendered at 49 h)');
  assert.ok(ovA.layer.hasFrame(), 'still drawn with the warning');
  atAge(73, () => { ovA._gridLineSync(); });
  assert.equal(ovA.layer.hasFrame(), false, 'past 72 h: hidden');
  assert.equal(atAge(73, () => ovA._gridLineText()), 'hidden: 73 h old');
  // a newer production comes in on the 12-min recheck: taken in place
  atAge(20, () => { ovA._gridLineSync(); });
  ovA._showGrid(ovA.grid); await settle(5);
  assert.ok(ovA.layer.hasFrame());
  ovA._gridRecheck();
  w.answer(/altgrid\/v1\/latest\.json$/, gridPointer({ manifest: 'altgrid/v1/20261103/manifest-20261105T101000Z.json', production: '20261104T091000Z' })); await settle(5);
  assert.equal(ovA.grid.newer.production, '20261104T091000Z');
  w.answer(/altgrid\/v1\/latest\.json$/, gridPointer({ manifest: 'altgrid/v1/20261103/manifest-20261105T101000Z.json', production: '20261104T091000Z' })); await settle(5);
  w.answer(/manifest-20261105T101000Z/, gridManifest({}, '20261104T091000Z'));
  assert.ok(await until(() => ovA.layer.entry && /20261104T091000Z/.test(ovA.layer.entry.file)), 'the new production on the map');
  ovA.unmount();
  // a store that cannot load says unavailable
  const w2 = world(); w2.saved = { satGrid: true };
  const ov2 = ctl(w2, { grid: true });
  ov2.mount('sat'); w2.answer(/alt\/v1\/latest\.json$/, pointer('alt', [chunkRec('alt', '20261104T12', 1)])); await settle(20);
  w2.answer(/altgrid\/v1\/latest\.json$/, null, 503); await settle(10);
  assert.equal(ov2.ui.gridNote.textContent, 'unavailable'); assert.equal(ov2.layer.hasFrame(), false);
  ov2.unmount(); ov.unmount();
  } finally { Date.now = now0; }
});

test('G54: never under the error panel; the timers reach the map; the hover goes with it; one render a toggle; the same frame under a new manifest is kept; the store\'s rules', async () => {
  const now0 = Date.now; Date.now = () => GRID_NEWEST + 31 * 3.6e6;
  try {
    // 1: a Swell switch that fails while the grid is loading: the error panel, and no map drawn under it
    const w = world(); w.saved = { satGrid: true };
    const ov = ctl(w, { grid: true }); ov.grid._decode = gridFrame;
    ov.mount('sat');
    w.answer(/alt\/v1\/latest\.json$/, pointer('alt', [chunkRec('alt', '20261104T12', 1)])); await settle(20);
    assert.deepEqual(w.pending(/altgrid/), ['https://x/altgrid/v1/latest.json'], 'the grid on its way');
    ov.setSatKind('swell');
    w.answer(/sarswell\/v1\/latest\.json$/, null, 503); await settle(20);
    assert.equal(ov.last.state, 'error');
    w.answer(/altgrid\/v1\/latest\.json$/, gridPointer()); await settle(5);
    w.answer(/manifest-/, gridManifest()); await settle(20);
    assert.equal(ov.layer.hasFrame(), false, 'no Copernicus map under "Overlay unavailable"');
    ov.unmount();
    // the timers: the 12-min satellite re-check reads the grid's pointer; the 30-min run check moves the age state
    const w2 = world(); w2.saved = { satGrid: true };
    const ov2 = ctl(w2, { grid: true }); ov2.grid._decode = gridFrame;
    const freshAlt = pointer('alt', [chunkRec('alt', '20261104T12', 1)], { newest_utc: '2026-11-05T18:30:00Z' });   // readings fresh at 49 h: only the grid moves the panel
    const calls = []; const j0 = ov2.grid._json; ov2.grid._json = function (url, sig, noCache, max) { calls.push([url.split('/').pop(), noCache]); return j0.call(this, url, sig, noCache, max); };
    ov2.mount('sat');
    w2.answer(/alt\/v1\/latest\.json$/, freshAlt); await settle(20);
    w2.answer(/altgrid\/v1\/latest\.json$/, gridPointer()); await settle(5);
    w2.answer(/manifest-/, gridManifest());
    assert.ok(await until(() => ov2.layer.hasFrame()));
    assert.deepEqual(calls.map((c) => c[1]), [true, false], 'the pointer no-cache, the manifest not');
    assert.deepEqual(w2.fetchOpts.filter((f) => /altgrid/.test(f[0])).map((f) => [f[0].split('/').pop(), f[1].cache || '']), [['latest.json', 'no-cache'], ['manifest-20261105T084700Z.json', '']], 'no-cache on the wire');
    ov2._satRecheck();
    assert.deepEqual(w2.pending(/altgrid/), ['https://x/altgrid/v1/latest.json'], 'the 12-min re-check reads the grid pointer');
    w2.answer(/altgrid\/v1\/latest\.json$/, gridPointer()); await settle(5);
    let renders = 0; const r0 = ov2.render; ov2.render = function (st) { renders++; return r0.call(this, st); };
    atAge(49, () => ov2._checkRun());
    assert.equal(ov2._gridWarnShown, 'warn', 'the run check moves the warning without a user action'); assert.equal(renders, 1);
    assert.equal(atAge(49, () => ov2._satStale(ov2.satLayer.store)), false, 'the readings were not what re-rendered');
    atAge(31, () => ov2._checkRun()); assert.equal(ov2._gridWarnShown, '');
    // 5: one render a toggle, even when the age state flips with it
    ov2.setSatGrid(false); renders = 0;
    Date.now = () => GRID_NEWEST + 49 * 3.6e6;                       // the step past 48 h for the whole toggle (its async tail too)
    ov2.setSatGrid(true); await settle(10);
    assert.equal(renders, 1, 'one render for one toggle'); assert.equal(ov2._gridWarnShown, 'warn');
    Date.now = () => GRID_NEWEST + 31 * 3.6e6; ov2._gridLineSync();
    // 4: the readout goes with the map
    ov2.readout.hidden = false; ov2.readout.textContent = 'grid 8.2 ft · T18:00 HST';
    ov2.setSatGrid(false);
    assert.equal(ov2.readout.hidden, true, 'no "grid …" left over a map that is gone');
    // the clear never touches another field's picture
    assert.equal(ov2.grid.status, 'loading', 'the 49-h clock made the store re-read its pointer');
    ov2.setSatGrid(true); await settle(10); assert.ok(ov2.layer.hasFrame(), 'the frame in hand is drawn at once, the re-read still running');
    w2.answer(/altgrid\/v1\/latest\.json$/, gridPointer()); await settle(5); assert.equal(ov2.grid.status, 'ok');
    ov2.layer.field = 'hs'; ov2._clearGrid(); assert.ok(ov2.layer.hasFrame(), 'not the grid: left alone'); ov2.layer.field = 'sat';
    assert.ok(w2.I.FOCUS_PARTS.indexOf('ov-sat-box') >= 0, 'the box keeps the focus through a render');
    assert.deepEqual([48, 48.01, 72, 72.01].map((h) => atAge(h, () => ov2._gridState())), ['', 'warn', 'warn', 'hide'], 'past 48 h a warning, past 72 h hidden (strictly past)');
    ov2.setSatGrid(false); ov2.satGrid = true;                       // the box state without its sync: _showGrid alone must refuse a hidden step
    Date.now = () => GRID_NEWEST + 73 * 3.6e6; ov2._showGrid(ov2.grid); await settle(5);
    assert.equal(ov2.layer.hasFrame(), false, 'the draw itself refuses a step past 72 h');
    Date.now = () => GRID_NEWEST + 31 * 3.6e6; ov2.satGrid = false;
    ov2.unmount();
    // 6 + 7: a recheck notes a newer manifest; load() takes that pointer as is (one read) and keeps the same frame file
    const w3 = world(), st = new w3.I.GridStore('https://z');
    let decodes = 0; st._decode = () => { decodes++; return gridFrame(); };
    const q = st.load(true); w3.answer(/latest\.json$/, gridPointer()); await settle(5); w3.answer(/manifest-/, gridManifest()); await q;
    const e0 = st.entry, f0 = st.frame;
    const rc = st.recheck(); w3.answer(/latest\.json$/, gridPointer({ manifest: 'altgrid/v1/20261103/manifest-20261105T101000Z.json' })); assert.equal(await rc, true);
    const n = w3.fetches.length, q2 = st.load(false); await settle(2);
    assert.deepEqual(w3.fetches.slice(n), ['https://z/altgrid/v1/20261103/manifest-20261105T101000Z.json'], 'the noted pointer: not read again');
    w3.answer(/manifest-20261105T101000Z/, gridManifest()); assert.equal(await q2, st);
    assert.equal(decodes, 1, 'the same newest file: not fetched again'); assert.equal(st.entry, e0); assert.equal(st.frame, f0);
    assert.equal(st.newer, null); assert.equal(st.pointer.manifest, 'altgrid/v1/20261103/manifest-20261105T101000Z.json');
    // the same manifest re-read: nothing beyond the pointer
    st.newer = gridPointer({ manifest: 'altgrid/v1/20261103/manifest-20261105T101000Z.json' });
    const m2 = w3.fetches.length; assert.equal(await st.load(false), st); assert.equal(w3.fetches.length, m2, 'the manifest in hand: nothing asked');
    // recheck ignores an older run; the rules for > 4 frames and a non-numeric step
    const rc2 = st.recheck(); w3.answer(/latest\.json$/, gridPointer({ run: '20261102', manifest: 'altgrid/v1/20261102/manifest-x.json' })); assert.equal(await rc2, false);
    const fr = gridManifest().frames, V = w3.I.validGridManifest, ptr = gridPointer();
    assert.equal(V(gridManifest({ frames: fr.concat([{ step: 24, valid_utc: '2026-11-04T00:00:00Z', file: 'altgrid/v1/20261103/x/hs-24.png' }]) }), ptr), false, 'at most 4 frames');
    assert.equal(V(gridManifest({ frames: fr.slice(0, 3).concat([Object.assign({}, fr[3], { step: '18' })]) }), ptr), false, 'a numeric step');
  } finally { Date.now = now0; }
});

test('controller: the own entry on a page without the swell data shows no switch; mount(\'sat\') with the setting off fails with the error panel; the loading text', async () => {
  const w = world(), ov = ctl(w, { swell: false });
  ov.mount('sat');
  w.answer(/latest\.json$/, pointer('alt', [chunkRec('alt', '20261104T12', 1)])); await settle(20);
  assert.equal(ov.ui.satSeg, undefined); assert.equal(ov.swell, null);
  ov.unmount();
  const w2 = world(), ov2 = ctl(w2, { sat: false });
  ov2.mount('sat'); await settle(5);
  assert.equal(ov2.last.state, 'error'); assert.match(ov2.last.message, /satellite waves is not offered/);
  const w3 = world(), ov3 = ctl(w3);
  ov3.mount('sat');
  assert.equal(ov3.last.state, 'loading');
  assert.ok(ov3.opts.panel.children.some((c) => c.textContent === 'Loading satellite waves…'));
  ov3.unmount(); ov2.unmount();
});
