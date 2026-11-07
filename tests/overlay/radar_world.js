'use strict';
// The overlay module in a sandbox for the radar tests (tests/overlay/radar.test.js): Leaflet, document, fetch and storage
// stubbed; held fetches answered by hand; a 2-D context that records what is put on the canvas; real PNG bytes.
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const FIX = path.join(__dirname, '..', 'fixtures', 'radar_client');
const fixture = (name) => fs.readFileSync(path.join(FIX, name));
const fixtureBytes = (name) => { const b = fixture(name); return b.buffer.slice(b.byteOffset, b.byteOffset + b.length); };
const fixtureJson = (name) => JSON.parse(fixture(name).toString('utf8'));

function chunk(type, data) {
  const len = Buffer.alloc(4); len.writeUInt32BE(data.length);
  const td = Buffer.concat([Buffer.from(type, 'ascii'), data]);
  const crc = Buffer.alloc(4); crc.writeUInt32BE(zlib.crc32(td));
  return Buffer.concat([len, td, crc]);
}
// A grey 8-bit PNG (filter 0) as the job writes them.
function png(w, h, value) {
  const raw = Buffer.alloc((w + 1) * h);
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) raw[y * (w + 1) + 1 + x] = value(x, y);
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8;
  const b = Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', zlib.deflateSync(raw, { level: 1 })), chunk('IEND', Buffer.alloc(0))]);
  return b.buffer.slice(b.byteOffset, b.byteOffset + b.length);
}

function world(opts) {
  opts = opts || {};
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  const reg = { doc: {}, win: {}, map: {} };
  const on = (t) => (ev, fn) => { (reg[t][ev] = reg[t][ev] || []).push(fn); };
  const off = (t) => (ev, fn) => { reg[t][ev] = (reg[t][ev] || []).filter((f) => f !== fn); };
  const w = { fetches: [], held: [], panes: {}, layers: [], attrs: [], canvases: [], reg };
  function ctx2d(el) {
    el.puts = 0;
    return { calls: [], globalAlpha: 1, clearRect() { el.cleared = (el.cleared || 0) + 1; el.lastImg = null; }, beginPath() {}, arc() {}, fill() {}, stroke() {}, moveTo() {}, lineTo() {}, closePath() {}, save() {}, restore() {}, setLineDash() {},
      createImageData: (ww, hh) => ({ data: new Uint8ClampedArray(ww * hh * 4) }), putImageData(img) { el.puts++; el.lastImg = new Uint8ClampedArray(img.data); } };
  }
  const g = {
    L: { GridLayer: { prototype: { initialize(o) { this.options = o; this._tiles = {}; }, onRemove() {} },
      extend(p) { function C(o) { p.initialize.call(this, o); } C.prototype = Object.assign({ setOpacity() {}, addTo(m) { this._map = m; w.layers.push(this); return this; } }, p); return C; } },
      DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} } },
    document: { hidden: false, addEventListener: on('doc'), removeEventListener: off('doc'), body: { appendChild() {} },
      createElement(tag) {
        const el = { tag, width: 0, height: 0, children: [], style: { setProperty() {}, removeProperty() {} }, classList: { _s: new Set(), add(c) { this._s.add(c); }, remove(c) { this._s.delete(c); }, contains(c) { return this._s.has(c); }, toggle(c, on2) { if (on2) this._s.add(c); else this._s.delete(c); } }, hidden: false, textContent: '', offsetHeight: 10, offsetWidth: 100,
          appendChild(c) { this.children.push(c); c.parentNode = this; return c; }, removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parentNode = null; }, insertBefore(c) { this.children.unshift(c); return c; }, setAttribute(k, v) { this[k] = v; }, getAttribute(k) { return this[k]; },
          addEventListener(ev, fn) { (this._ls = this._ls || {})[ev] = fn; }, removeEventListener() {}, remove() {}, querySelector() { return null; }, querySelectorAll() { return []; }, closest() { return null; }, contains() { return false; }, focus() {}, getBoundingClientRect() { return { left: 0, top: 0, width: 100, height: 10, right: 100, bottom: 10 }; } };
        if (tag === 'canvas') { const c = ctx2d(el); el.getContext = () => c; w.canvases.push(el); }
        if (tag === 'input') el.checked = false;
        return el;
      },
      createTextNode(t) { return { nodeValue: t, textContent: t }; }, activeElement: null },
    sessionStorage: { getItem: () => (w.saved === undefined ? null : JSON.stringify(w.saved)), setItem(k, v) { w.saved = JSON.parse(v); } },
    fetch: (url, o) => {
      w.fetches.push(url);
      if (o && o.signal && o.signal.aborted) return Promise.reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
      return new Promise((res, rej) => {
        const h = { url, res, rej, sig: o && o.signal };
        w.held.push(h);
        if (h.sig) h.sig.addEventListener('abort', () => { if (!h.done) { h.done = true; h.aborted = true; rej(Object.assign(new Error('aborted'), { name: 'AbortError' })); } });
      });
    },
    setTimeout: (f, ms) => { const t = setTimeout(f, ms); if (t && t.unref) t.unref(); return t; }, clearTimeout: (t) => clearTimeout(t),
    setInterval: (f, ms) => { const t = setInterval(f, ms); if (t && t.unref) t.unref(); return t; }, clearInterval: (t) => clearInterval(t),
    matchMedia: undefined
  };
  g.window = g; g.addEventListener = on('win'); g.removeEventListener = off('win');
  const fn = new Function('L', 'document', 'sessionStorage', 'fetch', 'window', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'AbortController', 'DecompressionStream', 'TextDecoder', 'Response', 'requestAnimationFrame', 'cancelAnimationFrame', src);
  fn(g.L, g.document, g.sessionStorage, g.fetch, g, g.setTimeout, g.clearTimeout, g.setInterval, g.clearInterval, AbortController, DecompressionStream, TextDecoder, Response, undefined, undefined);
  w.I = g.AllshoreOverlay._internals; w.AO = g.AllshoreOverlay; w.g = g;
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
  const container = g.document.createElement('div'); container.clientWidth = 1200; container.clientHeight = 800;
  const bounds = { s: 18, n: 24, w: -162, e: -154 };
  w.view = (b, z) => { Object.assign(bounds, b); if (z !== undefined) w.map._z = z; };
  w.map = { _z: opts.zoom || 6, getZoom() { return this._z; }, getPane: (n) => w.panes[n] || null, createPane: (n) => (w.panes[n] = { style: {} }), hasLayer: (l) => w.layers.indexOf(l) >= 0, removeLayer: (l) => { w.layers = w.layers.filter((x) => x !== l); if (l.onRemove) l.onRemove(w.map); },
    getBounds: () => ({ getWest: () => bounds.w, getEast: () => bounds.e, getSouth: () => bounds.s, getNorth: () => bounds.n }),
    on: on('map'), off: off('map'), getContainer: () => container, getSize: () => ({ x: 1200, y: 800 }), getPixelBounds: () => ({ min: { x: 0, y: 0 }, max: { x: 1200, y: 800 } }), getZoomScale: () => 1,
    containerPointToLayerPoint: () => ({ x: 0, y: 0 }), attributionControl: { addAttribution: (t) => w.attrs.push(t), removeAttribution: (t) => { w.attrs = w.attrs.filter((x) => x !== t); } }, zoomControl: null };
  return w;
}

const settle = async (n) => { for (let i = 0; i < (n || 12); i++) { await new Promise((r) => setImmediate(r)); if (i % 4 === 3) await new Promise((r) => setTimeout(r, 2)); } };
const until = async (fn) => { for (let i = 0; i < 300; i++) { if (fn()) return true; await new Promise((r) => setTimeout(r, 10)); } return fn(); };

// A pointer over ten-minute slots ending at endIso (the newest), holding the given regions with their real boxes.
const BOXES = { conus: [20, -130, 55, -60], alaska: [50, -176, 72, -126], hawaii: [15, -164, 26, -151], carib: [10, -90, 25, -60], guam: [9, 140, 18, 150], europe: [31, -40, 75, 58], japan: [20, 122, 47, 149], taiwan: [18, 115, 29, 127] };
function slotName(ms) { const d = new Date(ms); const p = (n) => String(n).padStart(2, '0'); return '' + d.getUTCFullYear() + p(d.getUTCMonth() + 1) + p(d.getUTCDate()) + p(d.getUTCHours()) + p(d.getUTCMinutes()); }
function pointerDoc(endIso, count, regions, entry) {
  const end = Date.parse(endIso), slots = [];
  regions = regions || ['hawaii'];
  for (let i = count - 1; i >= 0; i--) {
    const r = {}; for (const nm of regions) r[nm] = Object.assign({ o: ['europe', 'japan', 'taiwan'].indexOf(nm) >= 0 ? 0 : 4 }, entry ? entry(nm, end - i * 600000) : {});
    slots.push({ t: slotName(end - i * 600000), r });
  }
  const reg = {}; for (const nm of Object.keys(BOXES)) reg[nm] = Object.assign({ name: nm, src: { europe: 'opera', japan: 'jma', taiwan: 'cwa' }[nm] || 'mrms', box: BOXES[nm] }, nm === 'japan' ? { scale: 'jma8' } : {});
  return { format: 'radar-v1', credit: 'x', floor_dbz: 5, step_dbz: 0.5, max_code: 142, newest: {}, published_utc: new Date(end + 8 * 60000).toISOString().replace('.000Z', 'Z'), started: '2026-11-06T03:00:00Z', gone: [],
    tiers: { t0: { ppd: 10 }, t1: { cell: 20, ppd: 25 }, t2: { cell: 5, ppd: 100 } }, regions: reg, slots };
}
function frame(cols, rows, value) {
  const q = new Uint8Array(cols * rows);
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) q[r * cols + c] = value ? value(r, c) : 0;
  return { q, cols, rows };
}
module.exports = { world, png, fixture, fixtureBytes, fixtureJson, settle, until, pointerDoc, slotName, frame, BOXES };
