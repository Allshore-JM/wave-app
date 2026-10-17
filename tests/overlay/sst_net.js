'use strict';
// The SST store's network and clock for tests/overlay/sst_timers.test.js (from the G31 re-check, plan section 44):
// overlay.js loaded into a sandbox with injected timers / fetch / AbortController, a fake clock, a held network whose
// bodies are real streams of real PNG bytes (so the real decodeFrame / readBody / inflate path runs), and an
// AbortController that counts its abort listeners.
// Loads overlay.js into a fresh sandbox (the sst.test.js way) and returns its _internals.
// opts.timers: { setTimeout, clearTimeout, setInterval, clearInterval } to inject (fake clock); opts.fetch; opts.AbortController.
const fs = require('node:fs');
const path = require('node:path');
function loadOverlay(file, opts) {
  opts = opts || {};
  const src = fs.readFileSync(file, 'utf8');
  const L = {
    GridLayer: {
      prototype: { initialize(o) { this.options = o; this._tiles = {}; } },
      extend(p) { function C(o) { p.initialize.call(this, o); } C.prototype = Object.assign({ setOpacity() {}, addTo() { return this; } }, p); return C; }
    },
    DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} }
  };
  const g = { L, AllshoreOverlay: undefined };
  const t = opts.timers || { setTimeout, clearTimeout, setInterval, clearInterval };
  const fn = new Function('L', 'window', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'fetch', 'AbortController', 'DecompressionStream', 'Response', 'document', 'sessionStorage',
    src + '\n;return window.AllshoreOverlay;');
  const doc = opts.document || { hidden: false, addEventListener() {}, removeEventListener() {}, createElement() { return { getContext() { return null; } }; } };
  const ss = opts.sessionStorage || { getItem() { return null; }, setItem() {} };
  const AO = fn(L, g, t.setTimeout, t.clearTimeout, t.setInterval, t.clearInterval, opts.fetch || ((...a) => fetch(...a)), opts.AbortController || AbortController,
    'DecompressionStream' in opts ? opts.DecompressionStream : (typeof DecompressionStream === 'function' ? DecompressionStream : undefined),
    'Response' in opts ? opts.Response : (typeof Response === 'function' ? Response : undefined), doc, ss);
  return (AO || g.AllshoreOverlay)._internals;
}
// A fake clock: timers run only when advance() is called, in due order (a microtask flush between each).
function fakeClock() {
  let now = 0, seq = 0; const q = new Map();
  const c = {
    now: () => now,
    setTimeout: (f, ms) => { const id = ++seq; q.set(id, { f, at: now + Math.max(0, ms || 0) }); return id; },
    clearTimeout: (id) => { q.delete(id); },
    setInterval: (f, ms) => { const id = ++seq; q.set(id, { f, at: now + (ms || 1), every: ms || 1 }); return id; },
    clearInterval: (id) => { q.delete(id); },
    pending: () => q.size,
    sources: () => [...q.values()].map((x) => String(x.f).replace(/\s+/g, ' ').slice(0, 140) + ' @' + x.at),
    advance: async (ms) => {
      const end = now + ms;
      for (;;) {
        const due = [...q].filter(([, x]) => x.at <= end).sort((a, b) => a[1].at - b[1].at || a[0] - b[0])[0];
        if (!due) break;
        now = Math.max(now, due[1].at);
        if (due[1].every) due[1].at += due[1].every; else q.delete(due[0]);
        due[1].f();
        for (let i = 0; i < 20; i++) await Promise.resolve();
      }
      now = end;
      for (let i = 0; i < 20; i++) await Promise.resolve();
    }
  };
  return c;
}
// A fake network for the SST store under a fake clock: every fetch is held until the test answers it; the answer's body
// is a real WHATWG ReadableStream whose chunks the test pushes; an abort before the answer rejects the fetch, an abort
// after it errors the body (as browsers do). Real PNG bytes (8-bit grey, filter 0) so the real decodeFrame / readBody /
// inflate path runs. Also: a counting AbortController (listeners on every signal it made).
const zlib = require('node:zlib');
function crc32(buf) {
  let c, crc = 0xffffffff;
  for (let n = 0; n < buf.length; n++) { c = (crc ^ buf[n]) & 0xff; for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1; crc = (crc >>> 8) ^ c; }
  return (crc ^ 0xffffffff) >>> 0;
}
function chunk(type, data) {
  const len = Buffer.alloc(4); len.writeUInt32BE(data.length);
  const td = Buffer.concat([Buffer.from(type, 'ascii'), data]);
  const crc = Buffer.alloc(4); crc.writeUInt32BE(crc32(td));
  return Buffer.concat([len, td, crc]);
}
// value(x, y) -> 0..255
function png(w, h, value) {
  const raw = Buffer.alloc((w + 1) * h);
  for (let y = 0; y < h; y++) { raw[y * (w + 1)] = 0; for (let x = 0; x < w; x++) raw[y * (w + 1) + 1 + x] = value(x, y); }
  const ihdr = Buffer.alloc(13); ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4); ihdr[8] = 8; ihdr[9] = 0; ihdr[10] = 0; ihdr[11] = 0; ihdr[12] = 0;
  return Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk('IHDR', ihdr), chunk('IDAT', zlib.deflateSync(raw, { level: 1 })), chunk('IEND', Buffer.alloc(0))]);
}
function abortErr() { const e = new Error('The operation was aborted.'); e.name = 'AbortError'; return e; }
function makeNet() {
  const net = { reqs: [], log: [] };
  net.fetch = (url, init) => {
    const sig = init && init.signal;
    const r = { url: String(url), sig, state: 'pending', ctrl: null };
    net.reqs.push(r); net.log.push('fetch ' + r.url.replace(/^https?:\/\/[^/]+\//, ''));
    return new Promise((resolve, reject) => {
      r.resolve = resolve; r.reject = reject;
      if (sig) {
        if (sig.aborted) { r.state = 'aborted'; reject(abortErr()); return; }
        sig.addEventListener('abort', () => {
          if (r.state === 'pending') { r.state = 'aborted'; reject(abortErr()); }
          else if (r.state === 'streaming') { r.state = 'aborted'; try { r.ctrl.error(abortErr()); } catch (e) { /* closed */ } }
        });
      }
    });
  };
  // answer a request: status, headers {name: value}, then the body is pushed by hand (or json given at once)
  net.answer = (r, opts) => {
    opts = opts || {};
    if (r.state !== 'pending') return false;
    const headers = new Map(Object.entries(opts.headers || {}).map(([k, v]) => [k.toLowerCase(), String(v)]));
    if (opts.json !== undefined) {
      r.state = 'done';
      r.resolve({ ok: (opts.status || 200) < 400, status: opts.status || 200, headers: { get: (k) => headers.get(k.toLowerCase()) || null }, json: () => Promise.resolve(opts.json) });
      return true;
    }
    const body = new ReadableStream({ start(c) { r.ctrl = c; } });
    r.state = 'streaming';
    const resp = { ok: (opts.status || 200) < 400, status: opts.status || 200, headers: { get: (k) => headers.get(k.toLowerCase()) || null }, body,
      arrayBuffer: () => new Response(body).arrayBuffer(), blob: () => new Response(body).blob() };
    r.resolve(resp);
    return true;
  };
  net.push = (r, bytes) => { if (r.state !== 'streaming') return false; r.ctrl.enqueue(new Uint8Array(bytes)); return true; };
  net.end = (r) => { if (r.state !== 'streaming') return false; r.ctrl.close(); r.state = 'done'; return true; };
  net.fail = (r, err) => { if (r.state === 'pending') { r.state = 'failed'; r.reject(err || new TypeError('Failed to fetch')); return true; } if (r.state === 'streaming') { r.state = 'failed'; r.ctrl.error(err || new TypeError('network error')); return true; } return false; };
  net.find = (re) => net.reqs.filter((r) => re.test(r.url));
  net.last = (re) => { const a = net.find(re); return a[a.length - 1]; };
  return net;
}
// A counting AbortController: every signal it makes counts its live abort listeners.
function countingAC() {
  const all = [];
  class CAC extends AbortController {
    constructor() {
      super(); const s = this.signal, ls = new Set(); all.push(s); s.__ls = ls;
      const add = s.addEventListener.bind(s), rem = s.removeEventListener.bind(s);
      s.addEventListener = (t, f, o) => { if (t === 'abort') ls.add(f); return add(t, f, o); };
      s.removeEventListener = (t, f, o) => { if (t === 'abort') ls.delete(f); return rem(t, f, o); };
    }
  }
  CAC.all = all;
  CAC.live = () => all.reduce((n, s) => n + s.__ls.size, 0);
  return CAC;
}

const FILE = path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js');
module.exports = { loadOverlay, fakeClock, png, makeNet, countingAC, abortErr, FILE };
