'use strict';
// RRFS "Precipitation total" from now (plan section 102 B1) for static_overlay/overlay.js, no browser: the integer
// running sums (exactly a direct sum, adding and taking away, a missing hour), the totals' code scale (the binary search
// = the log formula), the engine (forward, back, a start again from k0, a new manifest or start hour, downloads in flight and progress,
// an abort mid-way keeps a consistent sum), the colours and the start hour.
//   node --test tests/overlay/rrfs_ptot.test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

function load() {
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  global.L = {
    GridLayer: {
      prototype: { initialize(o) { this.options = o; this._tiles = {}; } },
      extend(p) {
        function C(o) { p.initialize.call(this, o); }
        C.prototype = Object.assign({ setOpacity() {}, addTo() { return this; } }, p);
        return C;
      }
    },
    DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} }
  };
  global.window = global;
  new Function(src)();
  return global.AllshoreOverlay._internals;
}
const I = load();
const TILE = 256;

// a random hourly frame: dry, wet of any type and level, a few missing nodes
function rndFrame(n, seed, wetShare) {
  let x = seed >>> 0;
  const r = () => { x = (x * 1664525 + 1013904223) >>> 0; return x / 4294967296; };
  const q = new Uint8Array(n);
  for (let i = 0; i < n; i++) {
    const u = r();
    q[i] = u < 0.01 ? 0 : u < 1 - (wetShare || 0.4) ? 1 : 2 + Math.floor(r() * 250);
  }
  return q;
}
// the direct sum the plan asks for: every hour's level amount, in 0.001 mm, from scratch
function direct(frames) {
  const n = frames[0].length, sum = new Float64Array(n), bad = new Uint8Array(n);
  for (const q of frames) for (let i = 0; i < n; i++) {
    const c = q[i];
    if (c === 0) bad[i] = 1;
    else if (c >= 2) sum[i] += Math.round(I.precipMm((c - 2) % 50) * 1000);
  }
  return { sum, bad };
}

test('PRECIP_MILLI: each code’s level amount in 0.001 mm; dry 0; missing and never-written -1', () => {
  assert.equal(I.PRECIP_MILLI[0], -1); assert.equal(I.PRECIP_MILLI[1], 0);
  assert.equal(I.PRECIP_MILLI[2], 100); assert.equal(I.PRECIP_MILLI[51], 75000); assert.equal(I.PRECIP_MILLI[202], 100);
  for (let c = 252; c < 256; c++) assert.equal(I.PRECIP_MILLI[c], -1);
});

test('the running sum is EXACTLY the direct sum, forward, back and after a round trip', () => {
  const n = 5000, frames = [];
  for (let k = 0; k < 84; k++) frames.push(rndFrame(n, 1000 + k));
  const sum = new Int32Array(n), bad = new Uint16Array(n);
  for (let k = 0; k < 84; k++) {
    I.sumApply(sum, bad, frames[k], 1);
    if (k % 7 === 0 || k === 83) {
      const d = direct(frames.slice(0, k + 1));
      for (let i = 0; i < n; i++) { assert.equal(sum[i], d.sum[i]); assert.equal(bad[i] > 0, d.bad[i] > 0); }
    }
  }
  for (let k = 83; k >= 30; k--) I.sumApply(sum, bad, frames[k], -1);   // back to frames 0..29
  const d = direct(frames.slice(0, 30));
  for (let i = 0; i < n; i++) { assert.equal(sum[i], d.sum[i]); assert.equal(bad[i] > 0, d.bad[i] > 0); }
  I.sumApply(sum, bad, null, 1); assert.ok(bad.every((b) => b > 0), 'an hour without a frame: every node missing');
  I.sumApply(sum, bad, null, -1);
  for (let i = 0; i < n; i++) assert.equal(bad[i] > 0, d.bad[i] > 0);
});

test('the totals’ codes: 1 dry, 2..255 by the log scale 0.1-1000 mm (binary search = the formula)', () => {
  assert.equal(I.ptotCode(0), 1); assert.equal(I.ptotCode(-5), 1);
  const formula = (milli) => 2 + Math.max(0, Math.min(253, Math.round(253 * Math.log(milli / 1000 / 0.1) / Math.log(1e4))));
  for (const milli of [1, 50, 99, 100, 101, 250, 1000, 12345, 99999, 1e6, 5e6, 2e7]) assert.equal(I.ptotCode(milli), formula(milli), String(milli));
  let x = 7; for (let i = 0; i < 20000; i++) { x = (x * 1103515245 + 12345) % 2147483648; const milli = 1 + x % 1200000; assert.equal(I.ptotCode(milli), formula(milli)); }
  assert.ok(Math.abs(I.ptotMm(0) - 0.1) < 1e-12 && Math.abs(I.ptotMm(253) - 1000) < 1e-9);
  assert.ok(Math.abs(I.ptotMm(1) / I.ptotMm(0) - Math.pow(1e4, 1 / 253)) < 1e-12, '3.7 % steps');
  assert.equal(I.ptotOf(0), null); assert.equal(I.ptotOf(1), 0); assert.ok(Math.abs(I.ptotOf(2 + 100) - I.ptotMm(100)) < 1e-12);
  const out = I.sumCodes(Int32Array.from([0, 100, 5000, 0]), Uint16Array.from([0, 0, 0, 2]), new Uint8Array(4));
  assert.deepEqual(Array.from(out), [1, I.ptotCode(100), I.ptotCode(5000), 0]);
});

test('the totals’ colours: the dry tint, the scale by log position, alpha solid from 0.25 mm', () => {
  const lut = I.ptotLut(), rgb = [0, 0, 0];
  assert.deepEqual(Array.from(lut.slice(8, 12)), Array.from(I.RADAR_TINT));
  for (const lvl of [0, 40, 120, 253]) {
    const e = Math.round((2 + lvl) * 2) * 4;
    I.rampAt(I.RAMPS.ptot, I.ptotPos(I.ptotMm(lvl)), rgb, 0);
    assert.deepEqual(Array.from(lut.slice(e, e + 3)), rgb.map((v) => Math.max(0, Math.min(255, Math.round(v)))));
  }
  assert.ok(lut[(2 * 2) * 4 + 3] < 200, '0.1 mm see-through');
  const l25 = Math.ceil(253 * Math.log(2.5) / Math.log(1e4));
  assert.equal(lut[Math.round((2 + l25) * 2) * 4 + 3], 255);
  assert.equal(I.ptotReadout(1, 'US'), 'no precipitation'); assert.equal(I.ptotReadout(0, 'US'), null);
  assert.equal(I.ptotReadout(2 + 253, 'Metric'), '1000 mm total');
  assert.equal(I.ptotReadout(2 + 253, 'US'), '39.4 in total');
});

test('the start hour: the first frame whose hour ends after now (the hour in progress counts whole)', () => {
  const m = { frames: [0, 1, 2, 3].map((h) => ({ valid_utc: new Date(Date.UTC(2026, 10, 19, 10 + h)).toISOString().replace('.000Z', 'Z') })) };
  assert.equal(I.precipK0(m, Date.UTC(2026, 10, 19, 9, 30)), 0);
  assert.equal(I.precipK0(m, Date.UTC(2026, 10, 19, 10, 0)), 1, 'the hour ending 10:00 is over at 10:00');
  assert.equal(I.precipK0(m, Date.UTC(2026, 10, 19, 11, 59)), 2);
  assert.equal(I.precipK0(m, Date.UTC(2026, 10, 19, 14, 0)), 4, 'every frame past');
});

// ------------------------------------------------------------------------------------------------- the engine

const G1 = { cols: 40, rows: 30, lat0: 22, lon0: -160, dlat: -0.05, dlon: 0.05, registration: 'center', lon_periodic: false };
const G2 = { cols: 50, rows: 20, lat0: 19, lon0: -67, dlat: -0.05, dlon: 0.05, registration: 'center', lon_periodic: false };
function world(nFrames, opts) {
  opts = opts || {};
  const frames = [], q = { a: [], b: [] };
  for (let k = 0; k < nFrames; k++) {
    q.a.push(rndFrame(G1.cols * G1.rows, 50 + k)); q.b.push(rndFrame(G2.cols * G2.rows, 900 + k));
    const pr = {};
    if (!(opts.noA && opts.noA(k))) pr.a = `rrfs/v1/frames/C/a/precip/f${k}.png`;
    pr.b = `rrfs/v1/frames/C/b/precip/f${k}.png`;
    frames.push({ step: k + 1, valid_utc: `2026-11-19T${String(k).padStart(2, '0')}:00:00Z`, files: { a: {}, b: {} }, precip: pr });
  }
  const m = { signature: opts.sig || '0123456789abcdef', frames, regionList: [{ name: 'a', grid: G1 }, { name: 'b', grid: G2 }] };
  const downloads = [];
  const rmap = new Map();
  const ov = {
    model: 'rrfs', root: 'https://x', progress: [],
    rcache: { get: (k) => rmap.get(k) || null, set: (k, v) => { rmap.set(k, v); } },
    _totalProgress() { if (this.totals && this.totals.progress) this.progress.push(this.totals.progress.done + '/' + this.totals.progress.total); },
    _download(url, g, ctrl) {
      downloads.push(url);
      const mm = /\/(a|b)\/precip\/f(\d+)\.png$/.exec(url), arr = q[mm[1]][+mm[2]];
      if (opts.fail && opts.fail(url)) return Promise.reject(new Error('frame stalled'));
      return new Promise((res, rej) => { if (ctrl.signal.aborted) rej(Object.assign(new Error('aborted'), { name: 'AbortError' })); else res({ q: arr, cols: g.cols, rows: g.rows }); });
    }
  };
  ov.totals = new I.PrecipTotals(ov);
  return { m, q, ov, downloads, rmap };
}
const ctrl = () => new AbortController();
function expectTotal(w, bundle, name, k0, idx) {
  const part = bundle.parts.find((p) => p.name === name), frames = [];
  for (let k = k0; k <= idx; k++) frames.push(w.m.frames[k].precip[name] ? w.q[name][k] : new Uint8Array(w.q[name][k].length));
  const d = direct(frames);
  for (let i = 0; i < d.sum.length; i++) assert.equal(part.frame.q[i], d.bad[i] ? 0 : I.ptotCode(d.sum[i]), `${name} node ${i} at ${idx}`);
}

test('the engine: forward, back, start again from k0: every bundle = codes of the direct sum k0..idx', async () => {
  const w = world(40), k0 = 3;
  const b10 = await w.ov.totals.at(w.m, 10, ['a', 'b'], k0, ctrl());
  assert.equal(b10.total, true); assert.equal(b10.regional, true); assert.deepEqual(b10.names, ['a', 'b']);
  expectTotal(w, b10, 'a', k0, 10); expectTotal(w, b10, 'b', k0, 10);
  assert.equal(w.downloads.length, 2 * 8);
  const b30 = await w.ov.totals.at(w.m, 30, ['a', 'b'], k0, ctrl()); expectTotal(w, b30, 'a', k0, 30); expectTotal(w, b30, 'b', k0, 30);
  assert.equal(w.downloads.length, 2 * 28, 'forward: only the new hours');
  const b26 = await w.ov.totals.at(w.m, 26, ['a', 'b'], k0, ctrl()); expectTotal(w, b26, 'a', k0, 26);
  assert.equal(w.downloads.length, 2 * 28, 'back: hours taken away (decoded already)');
  const b4 = await w.ov.totals.at(w.m, 4, ['a'], k0, ctrl()); expectTotal(w, b4, 'a', k0, 4);   // fewer frames from k0: start again
  assert.equal(w.ov.totals.regions.a.at, 4);
  const again = await w.ov.totals.at(w.m, 10, ['a', 'b'], k0, ctrl());
  assert.equal(again, b10, 'a step done is kept (the ring around the slider)');
  const before = await w.ov.totals.at(w.m, 2, ['a', 'b'], k0, ctrl());
  assert.equal(before.before, true); assert.equal(before.parts.length, 0);
  assert.equal(w.ov.progress.filter((p) => p.endsWith('/40')).length, 41, 'a long move: 0/40, then hour by hour');
  assert.ok(w.ov.progress.includes('40/40') && !w.ov.progress.some((p) => p.endsWith('/8')), 'a short move (8, not over PTOT_CONC): no progress line');
});

test('the engine: a new manifest or start hour starts again; an hour without a frame for a region blanks it', async () => {
  const w = world(30, { noA: (k) => k === 12 });
  const b15 = await w.ov.totals.at(w.m, 15, ['a', 'b'], 5, ctrl());
  expectTotal(w, b15, 'a', 5, 15); expectTotal(w, b15, 'b', 5, 15);
  assert.ok(b15.parts[0].frame.q.every((c) => c === 0), 'region a misses hour 12: blank');
  const b11 = await w.ov.totals.at(w.m, 11, ['a'], 5, ctrl());
  assert.ok(b11.parts[0].frame.q.some((c) => c > 0), 'before the missing hour it is back');
  const k6 = await w.ov.totals.at(w.m, 11, ['a'], 6, ctrl());
  assert.equal(w.ov.totals.k0, 6); expectTotal(w, k6, 'a', 6, 11);
  const m2 = Object.assign({}, w.m, { signature: 'fedcba9876543210' });
  const s2 = await w.ov.totals.at(m2, 11, ['a'], 6, ctrl());
  assert.notEqual(s2, k6); assert.equal(w.ov.totals.sig, m2.signature); expectTotal(w, s2, 'a', 6, 11);
});

test('the engine: an abort mid-jump keeps every hour added consistent; the next request finishes it exactly', async () => {
  const w = world(40);
  const c = ctrl();
  let n = 0;
  const real = w.ov._download.bind(w.ov);
  w.ov._download = (url, g, cc) => { if (++n === 20) c.abort(); return real(url, g, cc); };
  await assert.rejects(w.ov.totals.at(w.m, 35, ['a'], 0, c), (e) => e.name === 'AbortError');
  const st = w.ov.totals.regions.a;
  assert.ok(st.at >= 3 && st.at < 35, 'some hours added: ' + st.at);
  const d = direct(w.q.a.slice(0, st.at + 1));
  for (let i = 0; i < d.sum.length; i++) assert.equal(st.sum[i], d.sum[i]);
  w.ov._download = real;
  const b = await w.ov.totals.at(w.m, 35, ['a'], 0, ctrl());
  expectTotal(w, b, 'a', 0, 35);
});

test('the engine: a failed hour rejects the step and leaves the sums at the hour before it', async () => {
  const w = world(20, { fail: (u) => /\/a\/precip\/f9\.png$/.test(u) });
  await assert.rejects(w.ov.totals.at(w.m, 15, ['a'], 0, ctrl()), /stalled/);
  const st = w.ov.totals.regions.a;
  assert.equal(st.at, 8, 'hours 0-8 added, in order; none after the failed hour 9');
  const d = direct(w.q.a.slice(0, 9));
  for (let i = 0; i < d.sum.length; i++) assert.equal(st.sum[i], d.sum[i]);
});

test('the totals draw through the typed sampler with one type: never a blend with dry ground; readout = the drawn pixel', () => {
  const g = { cols: 3, rows: 3, lat0: 1, lon0: 0, dlat: -1, dlon: 1, registration: 'center', lon_periodic: false };
  const q = Uint8Array.from([1, 1, 1, 1, 200, 1, 1, 1, 0]);
  const parts = [{ name: 'x', grid: g, frame: { q, cols: 3, rows: 3 } }];
  const at = (lat, lon) => { const p = I.pixelOf(lat, lon, 20), o = new Float64Array(1); I.sampleRegionsTyped(parts, p, p.px, p.py, 1, 1, o, I.PTOT_TAB); return o[0]; };
  assert.ok(Math.abs(at(0.01, 1.0) - 200) < 1e-6, 'at the wet node: its code');
  assert.ok(Math.abs(at(0.3, 1.3) - 200) < 1e-6, 'one wet node with most of the weight: its level, not a blend with dry');
  assert.equal(at(0.9, 0.1), 1, 'dry ground');
  assert.equal(at(-1.9, 1.9), 0, 'beside the missing corner node, too little covered weight: nothing');
});

test('the engine: hours that arrive out of order wait their turn; the sums and `at` stay exact at every step', async () => {
  const w = world(30), real = w.ov._download.bind(w.ov), seen = [];
  w.ov._download = (url, g, cc) => {
    const k = +/f(\d+)\.png$/.exec(url)[1];
    return new Promise((res) => setTimeout(res, 2 * (30 - k))).then(() => { seen.push(w.ov.totals.regions.a.at); return real(url, g, cc); });
  };
  const b = await w.ov.totals.at(w.m, 25, ['a', 'b'], 2, ctrl());
  expectTotal(w, b, 'a', 2, 25); expectTotal(w, b, 'b', 2, 25);
  assert.ok(seen.some((at) => at === 1), 'later hours landed while the first was still out (nothing added ahead of it)');
  assert.equal(w.ov.totals.regions.a.at, 25); assert.equal(w.ov.totals.regions.b.at, 25);
});
