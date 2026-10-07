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
// and removeAttribute (the fake has none)
Element.prototype.removeAttribute = function (k) { this.attrs.delete(k); };
// and insertBefore (the fake has none): before ref, or at the end without one
Element.prototype.insertBefore = function (c, ref) {
  if (!ref) return this.appendChild(c);
  this.appendChild(c); this.children.pop();
  const i = this.children.indexOf(ref); this.children.splice(i < 0 ? this.children.length : i, 0, c); return c;
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
  const win = { innerHeight: o.innerHeight || 800, addEventListener() {}, removeEventListener() {},
    matchMedia: o.reduced ? (q) => ({ matches: /reduced-motion/.test(q) }) : undefined };
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
const headPlay = (root) => { const h = root.querySelector('.ov-head'); return h ? h.querySelector('.ov-play') : null; };   // (the fake has no descendant selectors)

test('desktop: head = toggle + the model name; ONE play button, then the timeline and the speed selector; no first/prev/next/last, no speed numbers', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  const head = w.panel.querySelector('.ov-head');
  assert.deepEqual(head.children.map((c) => c.className), ['ov-toggle', 'ov-title ov-model']);
  assert.equal(text(head.children[1]), 'GFS-Wave (WAVEWATCH III)', 'the manifest name up to " + " without the agency prefix (owner, 2026-10-07), never the field again');
  const tog = w.panel.querySelector('.ov-toggle');
  assert.equal(tog.getAttribute('aria-expanded'), 'true'); assert.equal(tog.getAttribute('aria-controls'), 'ovDetails');
  const wrap = w.panel.querySelector('#ovDetails');
  assert.ok(wrap.classList.contains('ov-body') && wrap.querySelector('.ov-details'), 'the transport row outside the scroll box');
  assert.equal(all(w.panel, '.ov-play').length, 1, 'exactly one play button');
  const tr = w.panel.querySelector('.ov-transport');
  assert.deepEqual(tr.children.map((c) => c.className.split(' ')[0]), ['ov-btn', 'ov-ribbon-wrap', 'ov-speed']);
  assert.ok(tr.children[0].classList.contains('ov-play'));
  const over = wrap.children[1];
  assert.equal(over.className, 'ov-overview'); assert.deepEqual(over.children.map((c) => c.className), ['ov-timeline']);
  assert.equal(over.children[0].getAttribute('aria-label'), 'Forecast overview: the whole run');
  const t = text(w.panel);
  assert.ok(!t.includes('Valid:'), 'the Valid line is gone: the ribbon label carries the time');
  assert.ok(!/-(day|hour) forecast/.test(t), 'no run-length line (owner, 2026-10-07)');
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

test('collapsed: the one-line summary (weekday, "Oct." date, time, zone; no field, no hour count) with the one play button in the head; the toggle reopens', () => withClock(() => {
  const w = world(); w.ov.collapsed = true; w.ov.render({ state: 'ready' });
  assert.equal(text(w.panel.querySelector('.ov-title')), 'Wed, Oct. 7, 03:00 PM HST', 'owner, 2026-10-07');
  assert.equal(w.panel.querySelector('.ov-title').className, 'ov-title');
  w.ov.target = 9; w.ov._syncUI();                                            // a seek pending: the requested time with its state
  assert.equal(text(w.panel.querySelector('.ov-title')), 'Wed, Oct. 7, 03:00 PM HST · loading…');
  w.ov.unavailable[w.ov._key(9)] = true; w.ov._syncUI();
  assert.equal(text(w.panel.querySelector('.ov-title')), 'Wed, Oct. 7, 03:00 PM HST · unavailable');
  w.ov.target = 3; w.ov._syncUI();
  assert.equal(w.panel.querySelector('#ovDetails'), null);
  assert.equal(all(w.panel, '.ov-play').length, 1, 'playback stays in reach'); assert.equal(w.ov.ui.play, headPlay(w.panel));
  assert.deepEqual(w.panel.querySelector('.ov-head').children.map((c) => c.className.split(' ').pop()), ['ov-toggle', 'ov-play-head', 'ov-title']);
  let calls = 0; w.ov.play = function () { calls++; this.playing = true; this._syncUI(); }; w.ov.pause = function () { calls++; this.playing = false; this._syncUI(); };
  w.ov.ui.play.dispatch('click'); assert.equal(w.ov.playing, true); assert.equal(w.ov.ui.play.textContent, '❚❚');
  w.ov.ui.play.dispatch('click'); assert.equal(w.ov.playing, false); assert.equal(calls, 2);
  w.panel.querySelector('.ov-toggle').dispatch('click');
  assert.ok(w.panel.querySelector('#ovDetails')); assert.equal(text(w.panel.querySelector('.ov-title')), 'GFS-Wave (WAVEWATCH III)');
  assert.equal(all(w.panel, '.ov-play').length, 1, 'expanded: only the one beside the ribbon'); assert.equal(headPlay(w.panel), null);
}));

test('phone sheet: opens expanded when 40 % of the map holds the transport row, collapsed when it cannot; the head carries the model (open) or the one-line summary (folded)', () => withClock(() => {
  const w = world({ dims: { w: 375, h: 700 } }); w.ov.render({ state: 'ready' });
  const sheet = w.container.querySelector('.ov-sheet');
  assert.ok(sheet && sheet.getAttribute('role') === 'region'); assert.equal(w.panel.children.length, 0, 'the control keeps only the select');
  assert.ok(sheet.querySelector('#ovDetails'), 'expanded: 40 % of 700 px = 280 px');
  assert.equal(text(sheet.querySelector('.ov-title')), 'GFS-Wave (WAVEWATCH III)', 'open: the model, as on desktops (the time is in the ribbon label)');
  assert.equal(all(sheet, '.ov-model').length, 1, 'the model is named once (no line in the details any more)');
  assert.equal(text(sheet.querySelector('.ov-ribbon-now')), 'Wed, Oct 7 · 5 AM HST', 'frame 3 = +9 h = 15Z = 5 AM HST (the summary line uses the fixed clock text of this harness)');
  assert.equal(all(sheet, '.ov-play').length, 1);
  const s = world({ dims: { w: 375, h: 260 } }); s.ov.render({ state: 'ready' });  // 40 % = 104 px < 110
  const sh = s.container.querySelector('.ov-sheet');
  assert.equal(sh.querySelector('#ovDetails'), null, 'a short map: the one-line summary, as before');
  assert.equal(all(sh, '.ov-play').length, 1); assert.ok(headPlay(sh), 'with the play button in the head');
  assert.equal(text(sh.querySelector('.ov-title')), 'Wed, Oct. 7, 03:00 PM HST', 'folded: the same one line as the desktop panel');
  assert.equal(s.ov.collapsed, undefined, 'folded by the size, nothing saved');
  sh.querySelector('.ov-toggle').dispatch('click');                                // the viewer opens it: the rendered state flips
  assert.equal(s.ov.collapsed, false); assert.ok(sh.querySelector('#ovDetails'));
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

test('no room for the details (a very short map): the one-line summary with the play button in the head; nothing left that a later sync could touch', () => withClock(() => {
  const w = world({ ctlHeight: 900 }); w.ov.render({ state: 'ready' });       // a desktop control taller than the map allows
  assert.equal(w.panel.querySelector('#ovDetails'), null); assert.equal(w.ov.ui.speedSel, null);
  assert.equal(w.ov.ui.play, headPlay(w.panel), 'the one play button, in the head');
  assert.equal(w.ov.ui.collapsed, true); assert.equal(w.panel.querySelector('.ov-toggle'), null);
  assert.notEqual(w.ov.collapsed, true, 'a fold belongs to the size: never saved as a collapse');
  assert.equal(text(w.panel.querySelector('.ov-title')), 'Wed, Oct. 7, 03:00 PM HST');
  w.ov.playing = true; w.ov._syncUI(); w.ov._refreshRunLine();               // must not throw
  assert.equal(w.ov.ui.play.textContent, '❚❚');
}));

test('a phone turned sideways (step-4 F5): no room for the details -> the transport row stays; even less -> one line with the play button in the head; a fold is never saved', () => withClock(() => {
  // layout heights by box (the fake DOM has no layout): head 30, transport row 62, overview 14, details 120; a box = its children
  const H = { 'ov-row ov-head': 30, 'ov-row ov-transport': 62, 'ov-overview': 14, 'ov-details': 120 };
  const sized = (w) => {
    const create0 = w.doc.createElement.bind(w.doc);
    w.doc.createElement = (tag) => {
      const el = create0(tag);
      Object.defineProperty(el, 'offsetHeight', { configurable: true, set(v) { this._oh = v; }, get() {
        if (this._oh) return this._oh;
        if (H[this.className] !== undefined) return H[this.className];
        let s = 0; for (const c of this.children || []) s += c.offsetHeight || 0; return s;
      } });
      return el;
    };
    return w;
  };
  // 812 x 322 map: 40 % = 128 px holds head + transport + overview (106 + 2) but not the details
  const w = sized(world({ dims: { w: 812, h: 322 } })); w.ov.render({ state: 'ready' });
  const sheet = w.container.querySelector('.ov-sheet');
  assert.ok(sheet.querySelector('.ov-transport'), 'the transport row stays'); assert.ok(sheet.querySelector('.ov-overview'));
  assert.equal(sheet.querySelector('.ov-details'), null, 'the details go');
  assert.equal(all(sheet, '.ov-play').length, 1); assert.equal(headPlay(sheet), null);
  assert.ok(w.ov.ui.ribbon && w.ov.ui.rb && w.ov.ui.speedSel && w.ov.ui.slider, 'ribbon, speed and overview live');
  assert.equal(w.ov.ui.runText, null); assert.equal(w.ov.ui.unavail, null);
  assert.ok(sheet.querySelector('.ov-toggle'), 'the toggle can still fold it to one line');
  assert.notEqual(w.ov.collapsed, true);
  w.ov.target = 5; w.ov._syncUI(); w.ov._refreshRunLine();                    // must not throw
  // 812 x 240: 40 % = 96 px cannot hold the transport row either -> one line, the play button in the head
  const s = sized(world({ dims: { w: 812, h: 240 } })); s.ov.render({ state: 'ready' });
  const sh = s.container.querySelector('.ov-sheet');
  assert.equal(sh.querySelector('#ovDetails'), null); assert.equal(all(sh, '.ov-play').length, 1);
  assert.equal(s.ov.ui.play, headPlay(sh)); assert.equal(s.ov.ui.ribbon, null);
  assert.notEqual(s.ov.collapsed, true, 'not saved');
  // the same overlay on a taller map (turned upright): everything is back
  s.ov._dims = () => ({ w: 375, h: 700 }); s.ov.render({ state: 'ready' });
  const up = s.container.querySelector('.ov-sheet');
  assert.ok(up.querySelector('.ov-details'), 'the details are back'); assert.ok(up.querySelector('.ov-transport'));
  assert.equal(headPlay(up), null); assert.equal(all(up, '.ov-play').length, 1);
  // the toggle is the viewer's choice: it stays across sizes
  up.querySelector('.ov-toggle').dispatch('click'); assert.equal(s.ov.collapsed, true);
  s.ov._dims = () => ({ w: 812, h: 322 }); s.ov.render({ state: 'ready' });
  const side = s.container.querySelector('.ov-sheet'); assert.equal(side.querySelector('#ovDetails'), null); assert.ok(headPlay(side));
  s.ov._dims = () => ({ w: 375, h: 700 }); s.ov.render({ state: 'ready' });
  assert.equal(s.container.querySelector('.ov-sheet').querySelector('#ovDetails'), null, 'still folded: the viewer chose it');
  s.container.querySelector('.ov-sheet').querySelector('.ov-toggle').dispatch('click');
  assert.equal(s.ov.collapsed, false); assert.ok(s.container.querySelector('.ov-sheet').querySelector('.ov-details'));
}));

test('a sheet built after the desktop panel measures its ribbon in place (step-4 F4): the left offset first; a late change re-measures once, never in a loop', () => withClock(() => {
  const w = world();
  const props = {}; w.container.style = { setProperty(k, v) { props[k] = v; }, removeProperty(k) { delete props[k]; } };
  // the sheet runs from --ov-sheet-left to the map's right edge: its ribbon gets that width less 166 px of row and padding
  const create0 = w.doc.createElement.bind(w.doc);
  w.doc.createElement = (tag) => {
    const el = create0(tag);
    Object.defineProperty(el, 'clientWidth', { configurable: true, get() { return this.classList.contains('ov-ribbon') ? 375 - (parseFloat(props['--ov-sheet-left']) || 0) - 166 : 0; } });
    return el;
  };
  w.ov._stackWidth = () => 45;
  w.ov.render({ state: 'ready' });                                            // the desktop panel: no sheet, no offset
  assert.equal(props['--ov-sheet-left'], undefined);
  let renders = 0; const r0 = w.ov.render; w.ov.render = function (st) { renders++; return r0.call(this, st); };
  w.ov._dims = () => ({ w: 375, h: 700 }); w.ov.render({ state: 'ready' });  // the window became a phone: the sheet
  assert.equal(renders, 1, 'measured right the first time'); assert.equal(w.ov.ui.ribbonW, 375 - 51 - 166);
  // the zoom column widens while the sheet is built (the end of the render sees another offset): one rebuild at the new width
  let calls = 0; w.ov._stackWidth = () => (++calls > 1 ? 95 : 45);
  renders = 0; w.ov.render({ state: 'ready' });
  assert.equal(renders, 2); assert.equal(w.ov.ui.ribbonW, 375 - 101 - 166);
  // a small change at the end (the column 5 px wider): no rebuild, the track re-placed at the new width
  let c2 = 0; w.ov._stackWidth = () => (++c2 > 1 ? 50 : 45);
  renders = 0; w.ov.render({ state: 'ready' });
  assert.equal(renders, 1); assert.equal(w.ov.ui.ribbonW, 375 - 56 - 166);
  assert.equal(w.ov.ui.track.style.transform, 'translateX(' + (w.ov.ui.ribbonW / 2 - w.ov.ui.rb.offset) + 'px)');
  // a layout that keeps changing: one rebuild, then it stops
  let k = 0; w.ov._stackWidth = () => (k++ % 2 ? 95 : 45);
  renders = 0; w.ov.render({ state: 'ready' });
  assert.equal(renders, 2, 'never a loop');
}));

test('a short desktop window (step-4 F5): the details scroll box takes the room above the zoom stack; too little -> the transport row stays, the details go', () => withClock(() => {
  // heights by box: head 30, transport 62, overview 14, details 120; the control = its panel + 12 px of padding
  const H = { 'ov-row ov-head': 30, 'ov-row ov-transport': 62, 'ov-overview': 14, 'ov-details': 120 };
  const sized = (w) => {
    const create0 = w.doc.createElement.bind(w.doc);
    const height = function () { if (H[this.className] !== undefined) return H[this.className]; let s = 0; for (const c of this.children || []) s += c.offsetHeight || 0; return s; };
    w.doc.createElement = (tag) => { const el = create0(tag); Object.defineProperty(el, 'offsetHeight', { configurable: true, get: height, set() {} }); return el; };
    Object.defineProperty(w.ctl, 'offsetHeight', { configurable: true, get() { return w.panel.children.reduce((s, c) => s + (c.offsetHeight || 0), 0) + 12; }, set() {} });
    w.ov._stackHeight = () => 200;
    return w;
  };
  // 1200 x 420: above the stack 420 - 200 - 20 - 8 = 192 px; the control without its details 118 -> 74 px for the details
  const w = sized(world({ dims: { w: 1200, h: 420 } })); w.ov.render({ state: 'ready' });
  const det = w.panel.querySelector('.ov-details');
  assert.ok(det, 'the details stay'); assert.equal(det.style.maxHeight, '74px');
  // 1200 x 380: 34 px would be left for the details: they go, the transport row stays (play, ribbon, speed)
  const s = sized(world({ dims: { w: 1200, h: 380 } })); s.ov.render({ state: 'ready' });
  assert.equal(s.panel.querySelector('.ov-details'), null); assert.ok(s.panel.querySelector('.ov-transport'));
  assert.equal(all(s.panel, '.ov-play').length, 1); assert.equal(headPlay(s.panel), null); assert.ok(s.ov.ui.ribbon);
}));

test('loading and error states keep their texts; Retry remounts', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'loading' });
  assert.equal(text(w.panel), 'Loading model frame…');
  let mounted = null; w.ov.mount = (f) => { mounted = f; };
  w.ov.render({ state: 'error', message: 'frames unreachable' });
  assert.equal(text(w.panel), 'Overlay unavailable: frames unreachable Retry');
  w.panel.querySelector('.ov-retry').dispatch('click'); assert.equal(mounted, 'hs');
}));

// ---- the compass ribbon (step 3) ----
const dispatch = (el, type, ev) => el.dispatch(type, Object.assign({ clientX: 0, clientY: 0, button: 0, pointerId: 1, key: '' }, ev || {}));
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

test('ribbon: the DOM (label above a fixed pointer, the track laid out by the frames\' times), aria slider, the label text and the transform', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  const ui = w.ov.ui, rb = w.panel.querySelector('.ov-ribbon');
  assert.equal(rb.getAttribute('role'), 'slider'); assert.equal(rb.tabIndex, 0); assert.equal(rb.getAttribute('aria-label'), 'Forecast time');
  assert.equal(rb.getAttribute('aria-valuemin'), '0'); assert.equal(rb.getAttribute('aria-valuemax'), '80');
  assert.equal(rb.getAttribute('aria-valuenow'), '3'); assert.equal(rb.getAttribute('aria-valuetext'), 'Wednesday, Oct 7, 5 AM HST (+9 h)');
  assert.deepEqual(rb.children.map((c) => c.className), ['ov-ribbon-track', 'ov-ribbon-line', 'ov-ribbon-pointer']);
  const now = w.panel.querySelector('.ov-ribbon-now');
  assert.equal(text(now), 'Wed, Oct 7 · 5 AM HST'); assert.equal(ui.stateEl.hidden, true);
  assert.equal(ui.ribbonW, w.I.RIBBON_FALLBACK_W, 'no layout in the fake DOM: the fallback width');
  const L = w.I.ribbonLayout(w.m.frames, w.m.run_utc, 'Pacific/Honolulu', w.I.ribbonScale(ui.ribbonW));
  assert.equal(ui.track.style.width, L.width + 'px');
  const days = ui.track.children.filter((c) => c.classList.contains('ov-rb-day')), ticks = ui.track.children.filter((c) => c.classList.contains('ov-rb-tick'));
  assert.deepEqual(days.map((d) => [d.textContent, d.style.left]), L.days.map((d) => [d.text, d.x + 'px']));
  assert.ok(days.every((d) => /^[A-Z][a-z]{2} \d{1,2}$/.test(d.textContent)), 'the ribbon\'s own day labels stay "Oct 8" (no weekday)');
  assert.equal(ticks.length, L.ticks.length); assert.equal(ticks.filter((t) => t.classList.contains('ov-rb-major')).length, L.ticks.filter((t) => t.major).length);
  assert.equal(ui.rb.offset, L.xs[3]); assert.equal(ui.track.style.transform, 'translateX(' + (ui.ribbonW / 2 - L.xs[3]) + 'px)');
  assert.equal(ui.track.style.transition, 'transform 150ms ease-out');
  // a jump wider than the viewport (Home) moves without a transition; the overview thumb follows
  w.ov.target = 40; w.ov.frameIndex = 40; w.ov.layer.entry = w.m.frames[40]; w.ov._syncUI();
  assert.equal(ui.track.style.transition, 'none'); assert.equal(ui.rb.offset, L.xs[40]); assert.equal(ui.slider.value, '120');
  assert.equal(rb.getAttribute('aria-valuenow'), '40');
}));

test('ribbon: a pending frame shows the REQUESTED time with "loading…", an unavailable one says so, the drawn frame stays', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  const ui = w.ov.ui;
  w.ov.target = 5; w.ov._syncUI();                                            // requested +15 h, drawn +9 h
  assert.equal(text(w.panel.querySelector('.ov-ribbon-now')), 'Wed, Oct 7 · 11 AM HST loading…');
  assert.equal(ui.stateEl.hidden, false); assert.equal(ui.ribbon.getAttribute('aria-valuetext'), 'Wednesday, Oct 7, 11 AM HST (+15 h), loading');
  assert.equal(ui.rb.offset, ui.layout.xs[5], 'the pointer sits on the requested frame');
  assert.equal(w.ov.layer.entry, w.m.frames[3], 'the picture is still the drawn frame');
  w.ov.unavailable[w.ov._key(5)] = true; w.ov._syncUI();
  assert.equal(text(w.panel.querySelector('.ov-ribbon-now')), 'Wed, Oct 7 · 11 AM HST unavailable');
  assert.match(ui.ribbon.getAttribute('aria-valuetext'), /, unavailable$/);
  w.ov.target = 3; w.ov._syncUI(); assert.equal(ui.stateEl.hidden, true);
}));

test('ribbon: a drag pauses, moves the track with the pointer, seeks the frame under the pointer (once per frame crossed) and snaps on release', async () => withClock(async () => {
  const w = world(); w.ov.render({ state: 'ready' });
  const ui = w.ov.ui, rb = ui.ribbon, seeks = []; w.ov.seek = (i) => { seeks.push(i); w.ov.target = i; };
  w.ov.playing = true; w.ov.pause = function () { this.playing = false; };
  rb.rect = { left: 100, top: 0, width: ui.ribbonW, height: 40 };
  let ev = dispatch(rb, 'pointerdown', { clientX: 200 });
  assert.equal(w.ov.playing, false, 'a scrub pauses'); assert.equal(ev.defaultPrevented, true); assert.equal(ev._stop, true, 'the map never sees the press');
  assert.ok(rb.log.includes('capture:1')); assert.equal(w.doc.activeElement, rb);
  const px = ui.layout.pxPerHour, x0 = ui.rb.offset;
  dispatch(rb, 'pointermove', { clientX: 200 - 6 * px });                     // dragged left = 6 h later
  assert.ok(Math.abs(ui.rb.offset - (x0 + 6 * px)) < 1e-9); assert.equal(ui.track.style.transition, 'none');
  assert.equal(ui.track.style.transform, 'translateX(' + (ui.ribbonW / 2 - ui.rb.offset) + 'px)');
  dispatch(rb, 'pointermove', { clientX: 200 - 7 * px }); dispatch(rb, 'pointermove', { clientX: 200 - 8 * px });
  const mid = ui.rb.offset; w.ov.target = 2; w.ov._syncUI();
  assert.equal(ui.rb.offset, mid, 'a sync during the drag never moves the ribbon under the finger'); w.ov.target = 3;
  await wait(40);
  assert.deepEqual(seeks, [6], 'one seek, for the frame nearest the LATEST position (+17 h -> +18 h), not one per move');
  dispatch(rb, 'pointermove', { clientX: 200 - 12.2 * px }); await wait(40);
  assert.deepEqual(seeks, [6, 7]);
  dispatch(rb, 'pointerup', { clientX: 200 - 12.2 * px });
  assert.equal(ui.rb.dragging, false); assert.equal(ui.rb.offset, ui.layout.xs[7], 'snapped to the nearest frame');
  assert.equal(ui.track.style.transition, 'transform 120ms ease-out'); assert.deepEqual(seeks, [6, 7], 'the release repeats no seek');
  // a tap picks the frame under the finger: 9 h right of the centre
  dispatch(rb, 'pointerdown', { clientX: 300 }); dispatch(rb, 'pointerup', { clientX: 100 + ui.ribbonW / 2 + 9 * px });
  assert.equal(seeks[seeks.length - 1], 10, '+30 h: three frames later than +21 h');
  const r = dispatch(rb, 'pointerdown', { clientX: 300, button: 2 }); assert.equal(r.defaultPrevented, false, 'a right button is ignored');
  // a move queued on the old panel never seeks after the panel was rebuilt
  const n0 = seeks.length; dispatch(rb, 'pointerdown', { clientX: 300 }); dispatch(rb, 'pointermove', { clientX: 240 });
  w.ov.render({ state: 'ready' }); await wait(40);
  assert.equal(seeks.length, n0, 'the stale queued scrub was dropped');
}));

test('ribbon: the wheel scrolls it sideways (a vertical wheel is left alone) and snaps when it stops; keys step, jump and page', async () => withClock(async () => {
  const w = world(); w.ov.render({ state: 'ready' });
  const ui = w.ov.ui, rb = ui.ribbon, calls = [];
  w.ov.seek = (i) => { calls.push(['seek', i]); w.ov.target = i; }; w.ov.step = (d) => calls.push(['step', d]);
  let ev = dispatch(rb, 'wheel', { deltaX: 0, deltaY: 40, deltaMode: 0 });
  assert.equal(ev.defaultPrevented, false, 'vertical: the details box scrolls'); assert.equal(ui.rb.offset, ui.layout.xs[3]);
  ev = dispatch(rb, 'wheel', { deltaX: 13, deltaY: 2, deltaMode: 0 });
  assert.equal(ev.defaultPrevented, true); assert.equal(ui.rb.offset, ui.layout.xs[3] + 13);
  await wait(200);
  assert.equal(ui.rb.offset, ui.layout.xs[4], 'snapped to the nearest frame once the wheel stopped');
  assert.deepEqual(calls, [['seek', 4]]);
  ev = dispatch(rb, 'wheel', { deltaX: 0, deltaY: 13, deltaMode: 0, shiftKey: true }); assert.equal(ev.defaultPrevented, true, 'shift + wheel scrolls sideways');
  await wait(200); const o4 = ui.rb.offset;
  ev = dispatch(rb, 'wheel', { deltaX: 5, deltaY: 40, deltaMode: 0 });
  assert.equal(ev.defaultPrevented, false, 'a mostly vertical wheel is left alone'); assert.equal(ui.rb.offset, o4);
  calls.length = 0;
  ev = dispatch(rb, 'keydown', { key: 'ArrowRight' }); assert.deepEqual(calls, [['step', 1]]); assert.equal(ev.defaultPrevented, true); assert.equal(ev._stop, true);
  dispatch(rb, 'keydown', { key: 'ArrowLeft' }); dispatch(rb, 'keydown', { key: 'ArrowUp' }); dispatch(rb, 'keydown', { key: 'ArrowDown' });
  assert.deepEqual(calls.slice(1), [['step', -1], ['step', 1], ['step', -1]]);
  calls.length = 0; w.ov.target = 3;
  dispatch(rb, 'keydown', { key: 'End' }); dispatch(rb, 'keydown', { key: 'Home' }); dispatch(rb, 'keydown', { key: 'PageUp' }); dispatch(rb, 'keydown', { key: 'PageDown' });
  assert.deepEqual(calls, [['seek', 80], ['seek', 0], ['seek', 8], ['seek', 0]]);
  ev = dispatch(rb, 'keydown', { key: 'x' }); assert.equal(ev.defaultPrevented, false);
}));

test('ribbon: the overview slider beneath scrubs too (pausing), and the collapsed fold leaves no ribbon for a later sync to touch', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  const seeks = []; w.ov.seek = (i) => { seeks.push(i); w.ov.target = i; }; w.ov.playing = true; w.ov.pause = function () { this.playing = false; };
  const sl = w.ov.ui.slider; sl.value = '27'; sl.dispatch('input');
  assert.deepEqual(seeks, [9]); assert.equal(w.ov.playing, false);
  const f = world({ ctlHeight: 900 }); f.ov.render({ state: 'ready' });
  assert.equal(f.ov.ui.ribbon, null); assert.equal(f.ov.ui.rb, null); f.ov.target = 5; f.ov._syncUI();
  assert.equal(f.ov._ribbonLabel().text, 'Wed, Oct 7 · 11 AM HST', 'the label still computes without a ribbon (the sheet title, tests)');
}));

test('ribbon: a resize re-measures; a width change beyond 8 px rebuilds at the new scale', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  const ui = w.ov.ui; let renders = 0; const render0 = w.ov.render; w.ov.render = function (st) { renders++; return render0.call(this, st); };
  ui.ribbon.clientWidth = ui.ribbonW + 5; w.ov._ribbonMeasure(); assert.equal(renders, 0); assert.equal(w.ov.ui.ribbonW, ui.ribbonW);
  w.ov.ui.ribbon.clientWidth = 300; w.ov._ribbonMeasure(); assert.equal(renders, 1);
}));

test('ribbon: under reduced motion the ribbon moves without a transition (also when a step is a short one)', () => withClock(() => {
  const w = world({ reduced: true }); w.ov.render({ state: 'ready' });
  w.ov.target = 4; w.ov._syncUI();
  assert.equal(w.ov.ui.track.style.transition, 'none');
  const n = world(); n.ov.render({ state: 'ready' }); n.ov.target = 4; n.ov._syncUI();
  assert.equal(n.ov.ui.track.style.transition, 'transform 150ms ease-out', 'a short step glides otherwise');
  n.ov.playing = true; n.ov.speed = 4; n.ov.target = 5; n.ov._syncUI();
  assert.equal(n.ov.ui.track.style.transition, 'none', 'at the shark speed every step is a jump');
}));

test('resize (step-4 F4): the window and the map container are watched besides the map event; one handling per size; Off unbinds', async () => {
  // The page has Leaflet re-read the map size before invalidateSize(), so the map's own 'resize' event does not fire on a
  // window resize or a rotation: without these watchers the panel kept the sheet on a desktop-sized window.
  const w = world();
  const winL = {}; w.win.addEventListener = (ev, fn) => { (winL[ev] = winL[ev] || []).push(fn); };
  w.win.removeEventListener = (ev, fn) => { winL[ev] = (winL[ev] || []).filter((f) => f !== fn); };
  const ros = [];
  w.win.ResizeObserver = function (cb) { this.cb = cb; this.observed = []; this.disconnected = false; ros.push(this); };
  w.win.ResizeObserver.prototype.observe = function (el) { this.observed.push(el); };
  w.win.ResizeObserver.prototype.disconnect = function () { this.disconnected = true; };
  const mapL = {}; w.ov.map.on = (ev, fn) => { (mapL[ev] = mapL[ev] || []).push(fn); }; w.ov.map.off = (ev, fn) => { mapL[ev] = (mapL[ev] || []).filter((f) => f !== fn); };
  let dims = { w: 1200, h: 800 }, checks = 0, renders = 0, measures = 0;
  w.ov._dims = () => dims; w.ov._checkRes = () => { checks++; }; w.ov._sizeAttribution = () => {};
  w.ov.render({ state: 'ready' });
  w.ov._bindMap();
  assert.equal(winL.resize.length, 1); assert.equal(winL.orientationchange.length, 1);
  assert.equal(ros.length, 1); assert.deepEqual(ros[0].observed, [w.container]);
  const r0 = w.ov.render, m0 = w.ov._ribbonMeasure;
  w.ov.render = function (st) { renders++; return r0.call(this, st); };
  w.ov._ribbonMeasure = function () { measures++; return m0.call(this); };
  ros[0].cb(); await wait(150);
  assert.deepEqual([renders, checks, measures], [0, 0, 0], 'the observer\'s first report (the size it started with) does nothing');
  // the window becomes a phone: three reports, one handling, and the panel moves into the sheet
  dims = { w: 375, h: 700 };
  winL.resize[0](); ros[0].cb(); winL.orientationchange[0]();
  await wait(150);
  assert.deepEqual([renders, checks], [1, 1]); assert.ok(w.container.querySelector('.ov-sheet'), 'the sheet');
  mapL.resize.forEach((f) => f()); assert.equal(renders, 1, 'the map\'s own event for the same size: nothing more');
  // the phone's map gets shorter (a browser bar): the sheet is rebuilt, its cap follows the map height
  dims = { w: 375, h: 600 }; ros[0].cb(); await wait(150);
  assert.equal(renders, 2); assert.ok(w.container.querySelector('.ov-sheet'));
  // back to a desktop size, reported by the map event this time: the panel returns to the control
  dims = { w: 1200, h: 800 }; mapL.resize.forEach((f) => f());
  assert.equal(renders, 3); assert.equal(w.container.querySelector('.ov-sheet'), null); assert.ok(w.panel.querySelector('#ovDetails'));
  ros[0].cb(); await wait(150); assert.equal(renders, 3, 'the observer reporting a size already handled: nothing');
  // a desktop HEIGHT change re-renders (the room for the details follows the map's height: G26 B-1) ...
  dims = { w: 1200, h: 700 }; ros[0].cb(); await wait(150);
  assert.deepEqual([renders, measures], [4, 0]);
  // ... a width-only change (the panel's width is fixed) re-measures the ribbon without a rebuild
  dims = { w: 1180, h: 700 }; ros[0].cb(); await wait(150);
  assert.deepEqual([renders, measures], [4, 1]);
  // Off: every watcher removed and a pending report dropped
  dims = { w: 800, h: 600 }; winL.resize[0]();
  w.ov.unmount();
  assert.equal(winL.resize.length, 0); assert.equal(winL.orientationchange.length, 0); assert.equal(ros[0].disconnected, true);
  assert.equal((mapL.resize || []).length, 0);
  await wait(150); assert.deepEqual([renders, measures, checks], [4, 1, 5]);
});

// ---- G26 fix round (step 6) ----
test('G26 A-P2-2: a tap whose finger rolls 2 px just before lifting stays on the tapped frame (the queued rAF scrub is dropped at the release)', async () => withClock(async () => {
  const w = world(); w.ov.render({ state: 'ready' });
  const ui = w.ov.ui, rb = ui.ribbon, seeks = []; w.ov.seek = (i) => { seeks.push(i); w.ov.target = i; };
  rb.rect = { left: 100, top: 0, width: ui.ribbonW, height: 40 };
  const px = ui.layout.pxPerHour, centre = 100 + ui.ribbonW / 2;
  dispatch(rb, 'pointerdown', { clientX: centre + 9 * px });
  dispatch(rb, 'pointermove', { clientX: centre + 9 * px + 2 });
  dispatch(rb, 'pointerup', { clientX: centre + 9 * px + 2 });
  assert.equal(seeks[seeks.length - 1], 6, 'the tap picked the frame under the finger (+18 h)');
  await wait(60);                                                            // the rAF queued by the 2-px move has fired by now
  assert.deepEqual(seeks, [6], 'nothing after the release'); assert.equal(w.ov.target, 6); assert.equal(ui.rb.offset, ui.layout.xs[6]);
}));

test('G26 A-P2-3: a gesture the browser cancels (or a lost capture) is never a tap: it stays on the frame it started from', () => withClock(() => {
  for (const ev of ['pointercancel', 'lostpointercapture']) {
    const w = world(); w.ov.render({ state: 'ready' });
    const ui = w.ov.ui, rb = ui.ribbon, seeks = []; w.ov.seek = (i) => { seeks.push(i); w.ov.target = i; };
    rb.rect = { left: 100, top: 0, width: ui.ribbonW, height: 40 };
    const px = ui.layout.pxPerHour, centre = 100 + ui.ribbonW / 2;
    dispatch(rb, 'pointerdown', { clientX: centre + 9 * px, clientY: 20 });
    dispatch(rb, ev, { clientX: centre + 9 * px + 2, clientY: 60 });
    assert.deepEqual(seeks.filter((s) => s !== 3), [], ev + ': no jump to the frame under the finger (' + JSON.stringify(seeks) + ')');
    assert.equal(ui.rb.offset, ui.layout.xs[3], ev + ': the ribbon snaps back to its frame');
  }
  const css = fs.readFileSync(path.join(__dirname, '..', '..', 'static_overlay', 'overlay.css'), 'utf8');
  assert.match(css, /\.ov-ribbon \{[^}]*touch-action: none/, 'nothing scrolls under the ribbon: the browser never takes a swipe on it');
}));

test('G26 B-1: on a desktop the room for the details is measured from where the control starts in the map (below the brand)', () => withClock(() => {
  const H = { 'ov-row ov-head': 30, 'ov-row ov-transport': 62, 'ov-overview': 14, 'ov-details': 120 };
  const sized = (w, top) => {
    const create0 = w.doc.createElement.bind(w.doc);
    const height = function () { if (H[this.className] !== undefined) return H[this.className]; let s = 0; for (const c of this.children || []) s += c.offsetHeight || 0; return s; };
    w.doc.createElement = (tag) => { const el = create0(tag); Object.defineProperty(el, 'offsetHeight', { configurable: true, get: height, set() {} }); return el; };
    Object.defineProperty(w.ctl, 'offsetHeight', { configurable: true, get() { return w.panel.children.reduce((s, c) => s + (c.offsetHeight || 0), 0) + 12; }, set() {} });
    w.ctl.rect = { left: 10, top: 50 + top, width: 340, height: 1 }; w.container.rect = { left: 0, top: 50, width: 1200, height: 900 };   // the map 50 px down the page
    w.ov._stackHeight = () => 200;
    return w;
  };
  // the control starts 80 px down (the brand above it): 470 - 200 - 10 - 80 - 8 = 172 px; the control without details 118 -> 54
  const a = sized(world({ dims: { w: 1200, h: 470 } }), 80); a.ov.render({ state: 'ready' });
  assert.equal(a.panel.querySelector('.ov-details').style.maxHeight, '54px');
  // 420 px: 4 px left for the details -> they go; the transport row fits (4 px to spare)
  const b = sized(world({ dims: { w: 1200, h: 420 } }), 80); b.ov.render({ state: 'ready' });
  assert.equal(b.panel.querySelector('.ov-details'), null); assert.ok(b.panel.querySelector('.ov-transport')); assert.equal(headPlay(b.panel), null);
  // 400 px: not even the transport row -> one line with the play button in the head
  const c = sized(world({ dims: { w: 1200, h: 400 } }), 80); c.ov.render({ state: 'ready' });
  assert.equal(c.panel.querySelector('#ovDetails'), null); assert.ok(headPlay(c.panel));
  // the old assumption (a control 10 px from the top) would have kept the details at 420 px (74 px) and 400 px (54 px)
  const d = sized(world({ dims: { w: 1200, h: 420 } }), 10); d.ov.render({ state: 'ready' });
  assert.equal(d.panel.querySelector('.ov-details').style.maxHeight, '74px', 'a control 10 px down: room as before');
}));

test('G26 B-2: folding and unfolding with the keyboard keeps the focus on the toggle; a click from elsewhere does not steal it', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  w.panel.querySelector('.ov-toggle').focus(); w.panel.querySelector('.ov-toggle').dispatch('click');
  assert.equal(w.ov.collapsed, true); assert.equal(w.doc.activeElement, w.panel.querySelector('.ov-toggle'), 'folded: focus on the new toggle');
  w.panel.querySelector('.ov-toggle').dispatch('click');
  assert.equal(w.ov.collapsed, false); assert.equal(w.doc.activeElement, w.panel.querySelector('.ov-toggle'), 'unfolded: still there');
  const other = w.doc.createElement('button'); w.doc.body.appendChild(other); other.focus();
  w.panel.querySelector('.ov-toggle').dispatch('click'); assert.equal(w.doc.activeElement, other);
}));

test('G26 A-P3-5: only the loading / error states and the warning lines are live regions (the time changes every frame while playing)', () => withClock(() => {
  const w = world();
  w.ov.render({ state: 'loading' }); assert.equal(w.panel.getAttribute('aria-live'), 'polite');
  w.ov.render({ state: 'ready' }); assert.equal(w.panel.getAttribute('aria-live'), null, 'the ready panel is not live');
  assert.equal(w.ov.ui.unavail.getAttribute('aria-live'), 'polite', 'the unavailable-frames note is');
  w.ov.render({ state: 'error', message: 'x' }); assert.equal(w.panel.getAttribute('aria-live'), 'polite');
}));

test('G26 A-P3-3: the speed button names its listbox (aria-controls); ArrowUp from the listbox itself goes to the last option', () => withClock(() => {
  const w = world(); w.ov.render({ state: 'ready' });
  const btn = w.panel.querySelector('.ov-speed-btn'), menu = w.panel.querySelector('.ov-speed-menu');
  assert.equal(menu.id, 'ovSpeedMenu'); assert.equal(btn.getAttribute('aria-controls'), 'ovSpeedMenu');
  btn.dispatch('click'); menu.focus();
  dispatch(menu, 'keydown', { key: 'ArrowUp' });
  const opts = all(menu, '[role="option"]'); assert.equal(w.doc.activeElement, opts[opts.length - 1]);
}));
