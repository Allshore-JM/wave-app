'use strict';
// The overlay panel itself (plan section 37): the real render() and _syncUI() against the shared fake DOM
// (tests/ui/fakedom.js): its structure, the single play button, the model line, the Run / Updated / Next Update texts,
// the legend fed by the same functions as before, and the animal speed selector (menu, keyboard, outside press, aria).
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Document, Element, memStorage } = require('../ui/fakedom.js');

// The fake DOM keeps className and classList apart and has no nodeValue / remove / 2-D canvas: four shims.
Object.defineProperty(Element.prototype, 'className', {
  configurable: true,
  get() { return [...this.classList.set].join(' '); },
  set(v) { this.classList.set = new Set(String(v).split(/\s+/).filter(Boolean)); }
});
Object.defineProperty(Element.prototype, 'nodeValue', { configurable: true, get() { return this._text; }, set(v) { this._text = String(v); } });
Element.prototype.remove = function () { if (this.parentNode) this.parentNode.removeChild(this); };
Element.prototype.getContext = function () { return { createImageData: (w, h) => ({ data: new Uint8ClampedArray(w * h * 4) }), putImageData() {} }; };
// a browser keeps an element's own text when a child is appended (the fake would drop it): keep it as a text node
const append0 = Element.prototype.appendChild;
Element.prototype.appendChild = function (c) {
  if (this._text && !this.children.length && this.tagName !== '#TEXT') { const t = this.doc.createTextNode(this._text); this._text = ''; append0.call(this, t); }
  return append0.call(this, c);
};

const HS = { lo: 0, hi: 15, legend: [0, 12], units: 'm', interpolation: 'bilinear' };
const RUN_UTC = '2026-10-07T06:00:00Z', PUB = '2026-10-07T11:33:25Z';
const NOW = Date.parse('2026-10-07T12:00:00Z');                               // 2 AM HST, the morning the run went live
function manifest(extra) {
  return Object.assign({ run: '2026100706', run_utc: RUN_UTC, published_utc: PUB, fields: { hs: HS },
    model: { name: 'NOAA/NCEP GFS-Wave (WAVEWATCH III) + GFS', fields: { hs: { label: 'Wave height' } } },
    frames: [...Array(81).keys()].map((i) => ({ step: 3 * i, valid_utc: new Date(Date.parse(RUN_UTC) + 3 * i * 3.6e6).toISOString() })) }, extra || {});
}

function world(o) {
  o = o || {};
  const doc = new Document(), storage = memStorage(o.session);
  const ctl = doc.createElement('div'); ctl.classList.add('ov-ctl'); doc.body.appendChild(ctl); ctl.offsetHeight = o.ctlHeight || 0;
  const panel = doc.createElement('div'); panel.id = 'ovPanel'; ctl.appendChild(panel);
  const container = doc.createElement('div'); container.style = { setProperty() {}, removeProperty() {} }; doc.body.appendChild(container);
  const win = { innerHeight: o.innerHeight || 800, matchMedia: undefined, addEventListener() {}, removeEventListener() {} };
  const L = { GridLayer: { prototype: { initialize(x) { this.options = x; this._tiles = {}; } },
    extend(p) { function C(x) { p.initialize.call(this, x); } C.prototype = Object.assign({ setOpacity() {}, addTo() { return this; } }, p); return C; } },
    DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} } };
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.js'), 'utf8');
  new Function('L', 'document', 'sessionStorage', 'window', src)(L, doc, storage, win);
  const I = win.AllshoreOverlay._internals;
  const map = { getContainer: () => container, getPane: () => ({}), getZoom: () => 6, on() {}, off() {}, removeLayer() {} };
  const ov = win.AllshoreOverlay.create(map, { base: 'https://x/gfswave/0p25/v1', panel, tz: 'Pacific/Honolulu', getUnit: () => o.unit || 'US',
    fmtTime: () => 'Oct 7, 03:00 PM', tzAbbr: () => 'HST', pageCycle: () => o.pageCycle || null, version: '9.9.9' });
  for (const k of ['_syncLook', '_unattribute', '_unbindReadout', '_attribute']) ov[k] = function () {};
  ov._dims = () => o.dims || { w: 1200, h: 800 };
  const m = manifest(o.manifest);
  ov.manifest = m; ov.pointer = { published_utc: m.published_utc }; ov.n = m.frames.length; ov.field = 'hs';
  ov.layer = { field: 'hs', entry: m.frames[3], _clip: true, hasFrame: () => true };
  ov.frameIndex = 3;
  return { doc, panel, ctl, container, storage, I, ov, m, win };
}
function withClock(fn) {
  const now0 = Date.now, tz0 = process.env.TZ;
  Date.now = () => NOW; process.env.TZ = 'Pacific/Honolulu';
  try { return fn(); } finally { Date.now = now0; if (tz0 === undefined) delete process.env.TZ; else process.env.TZ = tz0; }
}
const text = (el) => el.textContent.replace(/\s+/g, ' ').trim();
const all = (root, sel) => root.querySelectorAll(sel);

test('desktop: head = toggle + the model name; ONE play button, then the timeline and the speed selector; no first/prev/next/last, no speed numbers', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  const head = w.panel.querySelector('.ov-head');
  assert.deepEqual(head.children.map((c) => c.className), ['ov-toggle', 'ov-title ov-model']);
  assert.equal(text(head.children[1]), 'NOAA/NCEP GFS-Wave (WAVEWATCH III)', 'the manifest name up to " + ", never the field again');
  const tog = w.panel.querySelector('.ov-toggle');
  assert.equal(tog.getAttribute('aria-expanded'), 'true'); assert.equal(tog.getAttribute('aria-controls'), 'ovDetails');
  const wrap = w.panel.querySelector('#ovDetails');
  assert.ok(wrap.classList.contains('ov-body') && wrap.querySelector('.ov-details'), 'the transport row outside the scroll box');
  assert.equal(all(w.panel, '.ov-play').length, 1, 'exactly one play button');
  const tr = w.panel.querySelector('.ov-transport');
  assert.deepEqual(tr.children.map((c) => c.className.split(' ')[0]), ['ov-btn', 'ov-timeline', 'ov-speed']);
  assert.ok(tr.children[0].classList.contains('ov-play'));
  const t = text(w.panel);
  for (const g of ['⏮', '⏭', '▶▶', '◀']) assert.ok(!t.includes(g), g);
  assert.equal(all(w.panel, 'select').length, 0, 'no speed dropdown');
  assert.ok(!/\d×/.test(t), 'no speed number on screen: ' + t);
  assert.ok(!t.includes('Model overlay'));
}));

test('meta lines: Run (UTC, with the table\'s run), Updated and Next Update in the computer zone; shortly, newer, no publish time; updated in place', () => withClock(() => {
  const w = world({ pageCycle: { run: '2026100700' } }); w.ov.render({ state: 'ready' });
  const line = (cls) => text(w.panel.querySelector('.' + cls));
  assert.equal(line('ov-run'), 'Run: 2026-10-07 06Z (UTC) — the forecast table is on run 2026100700');
  assert.match(line('ov-upd'), /^Updated: 1:33\sAM HST$/);
  assert.match(line('ov-next'), /^Next Update: about 7:35\sAM HST$/);
  const nodes = [w.ov.ui.updText, w.ov.ui.nextText];
  Date.now = () => Date.parse('2026-10-07T17:40:00Z');                        // past the expected time
  w.ov._refreshRunLine();
  assert.equal(line('ov-next'), 'Next Update: expected shortly');
  assert.deepEqual([w.ov.ui.updText, w.ov.ui.nextText], nodes, 'the same text nodes (no rebuild)');
  w.ov.newerRun = { run: '2026100712' }; w.ov._refreshRunLine();
  assert.equal(line('ov-next'), 'Next Update: a newer run is available');
  const n = world({ manifest: { published_utc: undefined } });
  n.ov.pointer = { published_utc: PUB }; n.ov.render({ state: 'ready' });
  assert.equal(n.panel.querySelector('.ov-upd').hidden, true); assert.equal(n.panel.querySelector('.ov-next').hidden, true);
  assert.equal(text(n.panel.querySelector('.ov-run')), 'Run: 2026-10-07 06Z (UTC)');
  const s = world({ pageCycle: { model: 'SWAN' } }); s.ov.render({ state: 'ready' });
  assert.equal(text(s.panel.querySelector('.ov-run')), 'Run: 2026-10-07 06Z (UTC) — the forecast table is a PacIOOS SWAN run');
}));

test('legend: the bar and the ticks come from the same functions as before (positions and labels unchanged), in the site unit', () => withClock(() => {
  for (const unit of ['US', 'Metric']) {
    const w = world({ unit }); w.ov.render({ state: 'ready' });
    const cv = w.panel.querySelector('.ov-legend').querySelector('canvas'); assert.equal(cv.width, 256); assert.equal(cv.height, 1);
    const want = w.I.legendTicks('hs', HS, unit), got = w.panel.querySelector('.ov-ticks').children;
    assert.deepEqual(got.map((s) => s.textContent), want.map((t) => t.label));
    assert.deepEqual(got.map((s) => s.style.left), want.map((t) => (t.pos * 100).toFixed(2) + '%'));
  }
}));

test('collapsed: the one-line summary (field, time on the map, hour), no details, no play; the toggle reopens', () => withClock(() => {
  const w = world(); w.ov.collapsed = true; w.ov.render({ state: 'ready' });
  assert.equal(text(w.panel.querySelector('.ov-title')), 'Wave height · Oct 7, 03:00 PM HST (+9 h)');
  assert.equal(w.panel.querySelector('#ovDetails'), null); assert.equal(all(w.panel, '.ov-play').length, 0);
  w.panel.querySelector('.ov-toggle').dispatch('click');
  assert.ok(w.panel.querySelector('#ovDetails')); assert.equal(text(w.panel.querySelector('.ov-title')), 'NOAA/NCEP GFS-Wave (WAVEWATCH III)');
}));

test('phone sheet: opens expanded when 40 % of the map holds the transport row, collapsed when it cannot; the model line in the details', () => withClock(() => {
  const w = world({ dims: { w: 375, h: 700 } }); w.ov.render({ state: 'ready' });
  const sheet = w.container.querySelector('.ov-sheet');
  assert.ok(sheet && sheet.getAttribute('role') === 'region'); assert.equal(w.panel.children.length, 0, 'the control keeps only the select');
  assert.ok(sheet.querySelector('#ovDetails'), 'expanded: 40 % of 700 px = 280 px');
  assert.equal(text(sheet.querySelector('.ov-title')), '+9 h · Oct 7, 03:00 PM HST');
  assert.equal(text(sheet.querySelector('.ov-model')), 'NOAA/NCEP GFS-Wave (WAVEWATCH III)');
  assert.equal(all(sheet, '.ov-play').length, 1);
  const s = world({ dims: { w: 375, h: 260 } }); s.ov.render({ state: 'ready' });  // 40 % = 104 px < 110
  const sh = s.container.querySelector('.ov-sheet');
  assert.equal(sh.querySelector('#ovDetails'), null, 'a short map: the one-line summary, as before');
}));

test('the speed selector: the chosen animal (decorative image + accessible name), a listbox of three, choosing saves and closes', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  const btn = w.panel.querySelector('.ov-speed-btn'), menu = w.panel.querySelector('.ov-speed-menu');
  assert.equal(btn.getAttribute('aria-haspopup'), 'listbox'); assert.equal(btn.getAttribute('aria-expanded'), 'false');
  assert.equal(btn.getAttribute('aria-label'), 'Speed: Snail, 1× speed'); assert.equal(btn.title, 'Snail: 1× speed');
  assert.equal(btn.getAttribute('data-animal'), 'snail');
  const img = btn.querySelector('img'); assert.equal(img.src, '/overlay/snail.png?v=9.9.9'); assert.equal(img.alt, '');
  assert.equal(menu.getAttribute('role'), 'listbox'); assert.equal(menu.hidden, true);
  const opts = all(menu, '[role="option"]');
  assert.deepEqual(opts.map((o) => [o.getAttribute('data-speed'), o.getAttribute('aria-selected'), o.title]),
    [['1', 'true', 'Snail: 1× speed'], ['2', 'false', 'Fish: 2× speed'], ['4', 'false', 'Shark: 4× speed']]);
  assert.deepEqual(opts.map((o) => o.querySelector('img').src), ['/overlay/snail.png?v=9.9.9', '/overlay/fish.png?v=9.9.9', '/overlay/shark.png?v=9.9.9']);
  btn.dispatch('click');
  assert.equal(menu.hidden, false); assert.equal(btn.getAttribute('aria-expanded'), 'true');
  assert.equal(w.doc.activeElement, opts[0], 'focus on the chosen animal');
  assert.equal((w.doc.listeners.pointerdown || []).length, 1, 'an outside-press listener while open');
  opts[1].dispatch('click');                                                  // the fish
  assert.equal(w.ov.speed, 2); assert.equal(w.storage.read('allshore.overlay.v1').speed, 2);
  assert.equal(menu.hidden, true); assert.equal(btn.getAttribute('aria-expanded'), 'false'); assert.equal(w.doc.activeElement, btn);
  assert.equal((w.doc.listeners.pointerdown || []).length, 0, 'the listener goes with the menu');
  assert.equal(btn.getAttribute('data-animal'), 'fish'); assert.equal(btn.querySelector('img').src, '/overlay/fish.png?v=9.9.9');
  assert.equal(btn.getAttribute('aria-label'), 'Speed: Fish, 2× speed');
  assert.deepEqual(opts.map((o) => o.getAttribute('aria-selected')), ['false', 'true', 'false']);
}));

test('the speed selector by keyboard: arrows open and wrap, Home / End, Enter chooses, Escape closes back to the button, Tab closes', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  const btn = w.panel.querySelector('.ov-speed-btn'), menu = w.panel.querySelector('.ov-speed-menu'), opts = all(menu, '[role="option"]');
  const key = (el, k) => el.dispatch('keydown', { key: k });
  let ev = key(btn, 'ArrowDown'); assert.equal(menu.hidden, false); assert.equal(ev.defaultPrevented, true); assert.equal(w.doc.activeElement, opts[0]);
  key(menu, 'ArrowDown'); assert.equal(w.doc.activeElement, opts[1]);
  key(menu, 'ArrowDown'); key(menu, 'ArrowDown'); assert.equal(w.doc.activeElement, opts[0], 'wraps');
  key(menu, 'ArrowUp'); assert.equal(w.doc.activeElement, opts[2]);
  key(menu, 'Home'); assert.equal(w.doc.activeElement, opts[0]); key(menu, 'End'); assert.equal(w.doc.activeElement, opts[2]);
  ev = key(menu, 'Enter'); assert.equal(w.ov.speed, 4); assert.equal(menu.hidden, true); assert.equal(w.doc.activeElement, btn);
  assert.equal(ev._stop, true, 'the page never sees the key');
  key(btn, 'ArrowUp'); assert.equal(w.doc.activeElement, opts[2], 'opens on the chosen one');
  ev = key(menu, 'Escape'); assert.equal(menu.hidden, true); assert.equal(w.doc.activeElement, btn); assert.equal(w.ov.speed, 4);
  assert.equal(ev._stop, true, 'Escape stays in the menu (the page would close its windows)');
  key(btn, 'ArrowDown'); ev = key(menu, 'Tab'); assert.equal(menu.hidden, true); assert.equal(ev.defaultPrevented, false, 'Tab moves on');
  ev = key(btn, 'x'); assert.equal(menu.hidden, true); assert.equal(ev.defaultPrevented, false);
  key(btn, 'ArrowDown'); key(menu, ' '); assert.equal(menu.hidden, true, 'Space chooses too');
}));

test('the speed selector closes on a press outside (not inside), on a re-render and on unmount, leaving no listener', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  let btn = w.panel.querySelector('.ov-speed-btn'), menu = w.panel.querySelector('.ov-speed-menu');
  btn.dispatch('click');
  w.doc.fire('pointerdown', { target: menu.querySelector('img') }); assert.equal(menu.hidden, false, 'a press inside keeps it');
  w.doc.fire('pointerdown', { target: w.container }); assert.equal(menu.hidden, true, 'a press on the map closes it');
  assert.notEqual(w.doc.activeElement, btn, 'an outside press does not pull focus back');
  btn.dispatch('click'); assert.equal((w.doc.listeners.pointerdown || []).length, 1);
  w.ov.render({ state: 'ready' });
  assert.equal((w.doc.listeners.pointerdown || []).length, 0, 'a re-render closes the old menu');
  btn = w.panel.querySelector('.ov-speed-btn'); btn.dispatch('click');
  w.ov.unmount();
  assert.equal((w.doc.listeners.pointerdown || []).length, 0, 'unmount closes it'); assert.equal(w.panel.children.length, 0);
  assert.equal(w.panel.classList.contains('ov-playing'), false);
}));

test('the speed menu opens upward in the sheet, and on a desktop when it would leave the window', () => withClock(() => {
  const p = world({ dims: { w: 375, h: 700 } }); p.ov.render({ state: 'ready' });
  const ps = p.container.querySelector('.ov-speed'); ps.querySelector('.ov-speed-btn').dispatch('click');
  assert.ok(ps.classList.contains('ov-up'));
  const d = world({ innerHeight: 300 }); d.ov.render({ state: 'ready' });
  const ds = d.panel.querySelector('.ov-speed'); ds.querySelector('.ov-speed-menu').rect = { left: 0, top: 250, width: 60, height: 140 };
  ds.querySelector('.ov-speed-btn').dispatch('click'); assert.ok(ds.classList.contains('ov-up'), 'bottom 390 > window 300');
  const e = world({ innerHeight: 900 }); e.ov.render({ state: 'ready' });
  const es = e.panel.querySelector('.ov-speed'); es.querySelector('.ov-speed-menu').rect = { left: 0, top: 250, width: 60, height: 140 };
  es.querySelector('.ov-speed-btn').dispatch('click'); assert.equal(es.classList.contains('ov-up'), false);
}));

test('playing: the host carries ov-playing (the chosen animal moves) and the button says Pause; paused: neither', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  const play = w.panel.querySelector('.ov-play');
  assert.equal(play.textContent, '▶'); assert.equal(play.getAttribute('aria-label'), 'Play');
  w.ov.playing = true; w.ov._syncUI();
  assert.ok(w.panel.classList.contains('ov-playing')); assert.equal(play.textContent, '❚❚'); assert.equal(play.getAttribute('aria-label'), 'Pause');
  w.ov.playing = false; w.ov._syncUI();
  assert.equal(w.panel.classList.contains('ov-playing'), false); assert.equal(play.textContent, '▶');
  w.ov.playing = true; w.ov._syncUI(); w.ov.render({ state: 'ready' });
  assert.ok(w.panel.classList.contains('ov-playing'), 'a re-render keeps the look while playing');
}));

test('no room for the details (a very short map): the one-line summary; nothing left that a later sync could touch', () => withClock(() => {
  const w = world({ ctlHeight: 900 }); w.ov.render({ state: 'ready' });       // a desktop control taller than the map allows
  assert.equal(w.panel.querySelector('#ovDetails'), null); assert.equal(w.ov.ui.play, null); assert.equal(w.ov.ui.speedSel, null);
  assert.equal(w.ov.ui.collapsed, true); assert.equal(w.panel.querySelector('.ov-toggle'), null);
  assert.match(text(w.panel.querySelector('.ov-title')), /^Wave height · /);
  w.ov.playing = true; w.ov._syncUI(); w.ov._refreshRunLine();               // must not throw
}));

test('loading and error states keep their texts; Retry remounts', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'loading' });
  assert.equal(text(w.panel), 'Loading model frame…');
  let mounted = null; w.ov.mount = (f) => { mounted = f; };
  w.ov.render({ state: 'error', message: 'frames unreachable' });
  assert.equal(text(w.panel), 'Overlay unavailable: frames unreachable Retry');
  w.panel.querySelector('.ov-retry').dispatch('click'); assert.equal(mounted, 'hs');
}));
