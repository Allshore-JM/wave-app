'use strict';
// static_ui/tools.js init(): the menu, the tool bar and the map interactions, on a fake Leaflet map over
// tests/ui/fakedom.js (pattern from the G20 reviewer A harness). Real coast data: the Hawaii crop in
// tests/fixtures/coast. Run by tests/test_ui_module.py and CI.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { fakeWindow } = require('./fakedom.js');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'tools.js'), 'utf8');
const FIX = (n) => { const b = fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'coast', n)); return b.buffer.slice(b.byteOffset, b.byteOffset + b.length); };

function makeEnv(opts) {
  opts = opts || {};
  const win = fakeWindow({ width: opts.width || 1280, height: opts.height || 800 });
  if (opts.touch) win.matchMedia = (q) => ({ matches: /hover: none/.test(q) });
  if (opts.raf) win.requestAnimationFrame = opts.raf;
  if (opts.clock) win.performance = { now: () => opts.clock.t };          // a monotonic clock the test moves
  if (opts.console) win.console = opts.console;
  const doc = win.document;
  const el = (tag, id, parent, attrs) => { const e = doc.createElement(tag); if (id) doc.register(e, id); if (parent) parent.appendChild(e); Object.entries(attrs || {}).forEach(([k, v]) => e.setAttribute(k, v)); return e; };
  const host = el('div', 'toolsHost', doc.body);
  const btn = el('button', 'toolsBtn', host);
  const menu = el('div', 'toolsMenu', host); menu.hidden = true;
  ['point', 'distance', 'area', 'exposure'].forEach((t) => el('button', null, menu, { 'data-tool': t }));
  const unitSel = el('select', 'unit', doc.body); ['US', 'Metric'].forEach((v) => { const o = el('option', null, unitSel); o.value = v; o.textContent = v; }); unitSel.value = 'US';
  const outside = el('input', 'station-search', doc.body);
  const gear = el('button', 'settingsBtn', doc.body), gearPanel = el('div', 'settingsPanel', doc.body); gearPanel.hidden = true;
  const mapEl = el('div', 'map', doc.body); mapEl.rect = { left: 0, top: 0, width: opts.mapW || 1000, height: opts.mapH || 700 };
  const handlers = {};
  let dbl = true;
  const view = { lat0: opts.lat0 == null ? 22 : opts.lat0, lng0: opts.lng0 == null ? -159 : opts.lng0, scale: opts.scale || 400, zoom: opts.zoom == null ? 8 : opts.zoom };   // px per degree
  const layers = new Set();
  const map = {
    _panes: {}, pans: [],
    getPane(n) { return this._panes[n]; }, createPane(n) { const p = doc.createElement('div'); this._panes[n] = p; return p; },
    getContainer() { return mapEl; },
    doubleClickZoom: { enabled: () => dbl, disable: () => { dbl = false; }, enable: () => { dbl = true; } },
    on(t, fn) { (handlers[t] = handlers[t] || []).push(fn); return this; },
    fire(t, e) { (handlers[t] || []).forEach((f) => f(e || {})); },
    latLngToContainerPoint(ll) { const lat = Array.isArray(ll) ? ll[0] : ll.lat, lng = Array.isArray(ll) ? ll[1] : ll.lng; return { x: (lng - view.lng0) * view.scale, y: (view.lat0 - lat) * view.scale }; },
    mouseEventToContainerPoint(e) { return { x: e.clientX - mapEl.rect.left, y: e.clientY - mapEl.rect.top }; },
    containerPointToLatLng(p) { const x = Array.isArray(p) ? p[0] : p.x, y = Array.isArray(p) ? p[1] : p.y; return { lat: view.lat0 - y / view.scale, lng: view.lng0 + x / view.scale }; },
    getZoom() { return view.zoom; },
    getCenter() { return map.containerPointToLatLng([mapEl.rect.width / 2, mapEl.rect.height / 2]); },
    getSize() { return { x: mapEl.rect.width, y: mapEl.rect.height }; },
    removeLayer(l) { layers.delete(l); },
    hasLayer(l) { return layers.has(l); },
    addControl(c) {
      const box = c.onAdd(this); box.classList.add('leaflet-control'); c._container = box; mapEl.appendChild(box);
      if (box.classList.contains('tools-bar')) {                             // the bar's innerHTML as elements (fakedom does not parse HTML)
        const head = el('div', null, box); head.classList.add('tools-bar-head'); const title = el('strong', null, head); title.classList.add('tools-bar-title');
        const fold = el('button', null, head); fold.classList.add('tools-fold');
        const x = el('button', null, head); x.classList.add('tools-x');
        const acts = el('div', null, box); acts.classList.add('tools-bar-actions'); ['undo', 'finish', 'clear', 'lock'].forEach((a) => el('button', null, acts, { 'data-act': a }));
        const body = el('div', null, box); body.classList.add('tools-bar-body');
        box.rect = opts.barRect || { left: 700, top: 60, width: 290, height: 200 };
        Object.defineProperty(box, 'offsetHeight', { get() { return box.hidden ? 0 : (box._h || box.rect.height); } });
      }
      return this;
    },
    panBy(d) { this.pans.push(d); }
  };
  let front = 0;
  function layer(kind, a, o) { return { kind, a, o, addTo(t) { (t._items || layers).add(this); return this; }, bindTooltip(txt, to) { this.tip = txt; this.tipOpts = to; return this; }, getElement() { return this._el; }, setLatLngs(x) { this.a = x; return this; }, bringToFront() { this.front = ++front; return this; } }; }
  win.L = {
    featureGroup() { return { kind: 'group', _items: new Set(), addTo(t) { (t._items || layers).add(this); return this; }, clearLayers() { this._items.clear(); }, add(x) { this._items.add(x); } }; },   // on the map = in `layers`
    Control: { extend(proto) { function C() { this.options = proto.options; } C.prototype.onAdd = proto.onAdd; C.prototype.getContainer = function () { return this._container; }; return C; } },
    DomUtil: { create(tag, cls) { const e = doc.createElement(tag); cls.split(' ').forEach((c) => e.classList.add(c)); return e; } },
    DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} },
    polygon(a, o) { return layer('polygon', a, o); }, polyline(a, o) { return layer('polyline', a, o); }, circleMarker(a, o) { return layer('circle', a, o); },
    divIcon(o) { return o; },
    ...(opts.canvas ? { canvas(o) { return { renderer: true, o, options: o }; } } : {}),
    marker(ll, o) {                                                          // the fan's element holds its 72 wedge paths
      if (o.icon && o.icon.className !== 'tools-fan') return layer('label', ll, o);   // a ring label
      const m = layer('fan', ll, o); m._el = doc.createElement('div');
      for (let k = 0; k < 72; k++) el('path', null, m._el, { 'data-k': k, stroke: new RegExp('data-k="' + k + '" [^>]*stroke="#fde047"').test(o.icon.html) ? '#fde047' : 'none' });
      m.addTo = function () { layers.add(this); return this; }; return m;
    }
  };
  const idx = { format: 'coast-v1', tier0: { max_zoom: 6 }, tier1: { cell: 5, dir: 'f', cells: opts.cells || { '20_-160': [4531, 2018] } } };
  const files = opts.files || { '/world-i.bin': FIX('hawaii-t0.bin'), '/f/20_-160.bin': FIX('oahu-t1.bin') };
  const fetches = [];
  const fetchFn = async (url) => {
    fetches.push(url);
    const ok = (b) => ({ ok: true, json: async () => b, arrayBuffer: async () => b });
    if (url.endsWith('/index.json')) return ok(idx);
    const rel = url.replace('https://c', '');
    if (files[rel]) return ok(files[rel]);
    return { ok: false, status: 404 };
  };
  new Function('window', SRC)(win);
  const layouts = [], starts = [], points = [];
  const api = win.AllshoreTools.init({ map, coastBase: 'https://c', fetch: fetchFn, getUnit: () => unitSel.value, unitSelect: unitSel,
    onLayout: (b, grew) => layouts.push(grew), onStart: (b, tool) => starts.push(tool), obstacles: () => opts.obstacles || [],
    onPoint: opts.noPoint ? null : (ll, ev) => { points.push({ ll, ev, active: win.AllshoreTools.active(), dbl: map.doubleClickZoom.enabled() }); return opts.answer ? opts.answer(ll) : undefined; } });
  const bar = mapEl.children.find((c) => c.classList.contains('tools-bar'));
  const q = (sel) => bar.querySelector(sel);
  // the body's lines as HTML (the tool writes them as elements; fakedom does not serialise)
  const body = () => { const b = q('.tools-bar-body'); return b.children.length ? b.children.map((c) => '<div class="' + c.className + '">' + c.textContent + '</div>').join('') : b.innerHTML; };
  function key(k, focus) { doc.activeElement = focus || doc.body; return doc.fire('keydown', { key: k, target: focus || doc.body, _stop: false, stopPropagation() { this._stop = true; } }); }
  function clickAt(lat, lng, ev) { map.fire('click', { latlng: { lat, lng }, originalEvent: ev || { pointerType: 'mouse' } }); }
  function pressOn(target) { doc.fire('pointerdown', { target, composedPath: () => { const p = []; for (let e = target; e; e = e.parentNode) p.push(e); return p; } }); }
  function pick(tool) { btn.dispatch('click'); menu.querySelector('[data-tool="' + tool + '"]').dispatch('click'); }
  async function settle() { for (let i = 0; i < 400 && api.state.busy; i++) await new Promise((r) => setTimeout(r, 5)); await new Promise((r) => setTimeout(r, 5)); }
  const fan = () => [...layers].find((l) => l.kind === 'fan');
  return { win, doc, map, api, s: api.state, A: win.AllshoreTools, menu, btn, bar, q, body, unitSel, outside, gear, gearPanel, key, clickAt, pressOn, pick, settle, layouts, starts, fetches, view, mapEl, layers, fan, points };
}

test('the menu starts a tool: bar shown and labelled, focus on its close button, double-click zoom off; closing restores it', () => {
  const E = makeEnv();
  E.btn.dispatch('click');
  assert.equal(E.menu.hidden, false); assert.equal(E.btn.getAttribute('aria-expanded'), 'true');
  E.menu.querySelector('[data-tool="distance"]').dispatch('click');
  assert.equal(E.menu.hidden, true);
  assert.equal(E.bar.hidden, false); assert.equal(E.bar.getAttribute('aria-labelledby'), 'toolsBarTitle');
  assert.equal(E.q('.tools-bar-title').textContent, 'Measure distance');
  assert.equal(E.doc.activeElement, E.q('.tools-x'), 'keyboard focus stays in the tool');
  assert.equal(E.A.active(), true); assert.equal(E.map.doubleClickZoom.enabled(), false);
  assert.deepEqual(E.starts, ['distance'], 'the page is told which tool started');
  assert.deepEqual(E.layouts, [true], 'the bar appeared: the page re-measures the corner and minimises a window under it');
  assert.equal(E.bar.style.maxHeight, '632px', 'the bar ends 8 px above the map\'s bottom edge (700 - 60 - 8)');
  assert.equal(E.q('.tools-bar-actions').hidden, true, 'no actions yet: the row is hidden');
  E.q('.tools-x').dispatch('click');
  assert.equal(E.A.active(), false); assert.equal(E.bar.hidden, true); assert.equal(E.map.doubleClickZoom.enabled(), true);
  assert.deepEqual(E.layouts, [true, false], 'closing re-measures the corner');
  E.btn.dispatch('click');
  assert.equal(E.doc.activeElement, E.menu.querySelector('[data-tool="point"]'), 'the opened menu takes focus on its first item (its Escape works)');
});

test('distance: the double-click\'s second click is ignored, the dblclick finishes; units relabel; Backspace undoes', () => {
  const E = makeEnv(); E.pick('distance');
  E.clickAt(21.3069, -157.8583); E.clickAt(21.3069, -157.8583);             // a double-click at one place: one point
  assert.equal(E.s.pts.length, 1);
  E.clickAt(19.7297, -155.09);
  assert.match(E.body(), /Total 21\d mi · 18\d nm/);
  E.unitSel.value = 'Metric'; E.unitSel.dispatch('change');
  assert.match(E.body(), /Total 33\d km · 18\d nm/);
  E.key('Backspace');
  assert.equal(E.s.pts.length, 1);
  E.clickAt(19.7297, -155.09); E.map.fire('dblclick');
  assert.equal(E.s.closed, true);
});

test('area: a double-click on the first point closes the outline and keeps it (G20 A P2-1); touch closes within 22 px', () => {
  const E = makeEnv(); E.pick('area');
  [[21.8, -158.3], [21.8, -157.7], [21.3, -157.7]].forEach(([a, b]) => E.clickAt(a, b));
  E.clickAt(21.8, -158.3); E.clickAt(21.8, -158.3); E.map.fire('dblclick');   // click, click, dblclick on the first point
  assert.equal(E.s.closed, true); assert.equal(E.s.pts.length, 3, 'the polygon survives the second click');
  assert.match(E.body(), /Area [\d,]+ sq mi/);
  E.clickAt(21.0, -158.0);
  assert.equal(E.s.closed, false); assert.equal(E.s.pts.length, 1, 'a click elsewhere starts a new outline');
  const T = makeEnv({ touch: true }); T.pick('area');
  assert.match(T.body(), /Tap the map to outline an area/, 'touch wording');
  [[21.8, -158.3], [21.8, -157.7], [21.3, -157.7]].forEach(([a, b]) => T.clickAt(a, b, { pointerType: 'touch' }));
  assert.match(T.body(), /Tap the first point, double-tap or Finish to close/);
  T.clickAt(21.8 - 15 / 400, -158.3, { pointerType: 'touch' });              // 15 px from the first point
  assert.equal(T.s.closed, true, 'a finger closes within 22 px');
});

test('Escape and Backspace belong to the tool only while nothing else has focus (G20 P2-2)', () => {
  const E = makeEnv(); E.pick('distance');
  E.clickAt(21.3, -157.9); E.clickAt(21.0, -157.0);
  const ev = E.key('Escape', E.outside);
  assert.equal(ev._stop, false, 'the station search keeps its Escape'); assert.equal(E.s.pts.length, 2);
  E.key('Backspace', E.outside);
  assert.equal(E.s.pts.length, 2, 'typing in a field is not an undo');
  const ev2 = E.key('Escape');
  assert.equal(ev2._stop, true); assert.equal(E.s.pts.length, 0, 'first Escape clears');
  E.key('Escape');
  assert.equal(E.A.active(), false, 'second Escape closes the tool');
});

test('a click that began on a map control or a window is not a map click (G20 B P1-2)', () => {
  const E = makeEnv(); E.pick('distance');
  const ctl = E.doc.createElement('div'); ctl.classList.add('leaflet-control');
  E.clickAt(21.3, -157.9, { pointerType: 'mouse', target: ctl, composedPath: () => [ctl, E.mapEl] });
  const detached = E.doc.createElement('button'); detached.isConnected = false;
  E.clickAt(21.3, -157.9, { pointerType: 'mouse', target: detached, composedPath: () => [detached] });
  const win = E.doc.createElement('section'); win.classList.add('fwin');
  E.clickAt(21.3, -157.9, { pointerType: 'mouse', target: win, composedPath: () => [win] });
  assert.equal(E.s.pts.length, 0);
  E.clickAt(21.3, -157.9, { pointerType: 'mouse', target: E.mapEl, composedPath: () => [E.mapEl] });
  assert.equal(E.s.pts.length, 1, 'a real map click still counts');
});

test('exposure on the Oahu fixture: result, hover restyles one wedge, touch tap picks a wedge, a mouse click moves the point', async () => {
  const E = makeEnv(); E.pick('exposure');
  assert.deepEqual(E.fetches, [], 'nothing fetched before the first click');
  E.clickAt(21.6655, -158.054);
  assert.match(E.body(), /Computing/);
  await E.settle();
  assert.equal(E.s.result && E.s.result.sectors.length, 72);
  assert.match(E.body(), /Open: W \(250°–275°\), WNW–NE \(295°–045°\)/);
  assert.match(E.body(), /Moved 0\.\d mi off the shore/);
  const fan = [...E.layers].find((l) => l.kind === 'fan');
  assert.ok(fan && fan.o.pane === 'toolsPane');
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  E.mapEl.dispatch('mousemove', { clientX: c.x, clientY: c.y + 60 });       // straight south of the point
  assert.equal(E.s.selected, 36); assert.match(E.body(), /S 180–185°: shadowed/);
  const ll = { lat: E.s.result.origin.lat + 60 / 400, lng: E.s.fanLng };    // 60 px north
  E.clickAt(ll.lat, ll.lng, { pointerType: 'touch' });
  assert.equal(E.s.selected, 0, 'a tap in the fan picks the north wedge'); assert.equal(E.s.busy, false);
  const gen = E.s.gen;
  E.clickAt(ll.lat, ll.lng, { pointerType: 'mouse' });
  assert.equal(E.s.busy, true); assert.equal(E.s.gen, gen + 1, 'a mouse click starts a new point');
  await E.settle();
});

test('exposure: a newer click wins over a slower older one; refusals beyond 75 degrees and inland (lakes)', async () => {
  const E = makeEnv(); E.pick('exposure');
  E.clickAt(21.6655, -158.054); E.clickAt(21.269, -157.829);                // Pipeline, then Waikiki at once
  await E.settle();
  assert.match(E.body(), /Open: SE–W \(145°–275°\)/, 'Waikiki, not Pipeline');
  E.clickAt(76.5, 15);
  assert.match(E.body(), /between 75°S and 75°N/);
  E.clickAt(21.5, -158.0); await E.settle();                                 // central Oahu
  assert.match(E.body(), /Click the ocean to see swell exposure \(lakes are not covered\)/);
  assert.equal([...E.layers].some((l) => l.kind === 'fan'), false);
});

test('the fan is placed clear of the tool bar and other obstacles (the map pans), and shrinks on a short map', async () => {
  const E = makeEnv({ mapW: 1000, mapH: 700, lat0: 22, lng0: -158.8, scale: 400 });
  E.pick('exposure');
  // Pipeline lands at about (298, 136): under a window placed there
  E.clickAt(21.6655, -158.054);
  await E.settle();
  assert.equal(E.map.pans.length, 1, 'placed after the result');
  const S = makeEnv({ mapW: 610, mapH: 364, lat0: 22, lng0: -158.8, scale: 400, barRect: { left: 310, top: 60, width: 290, height: 250 } });
  S.pick('exposure');
  S.clickAt(21.6655, -158.054); await S.settle();
  assert.equal(S.s.radius, 90, 'a short map (364 px) gets the small fan');
  assert.equal(S.map.pans.length, 1);
  const c = S.map.latLngToContainerPoint([S.s.result.origin.lat, S.s.fanLng]), pan = S.map.pans[0];
  const at = { x: c.x - pan[0], y: c.y - pan[1] }, ext = 90 + 16;
  assert.ok(at.x + ext <= 310 || at.y - ext >= 310, 'clear of the tool bar: ' + JSON.stringify(at));
  assert.ok(at.x - ext >= 8 && at.y - ext >= 8 && at.x + ext <= 602 && at.y + ext <= 356, 'whole inside the map: ' + JSON.stringify(at));
});

// ---- G20 re-review (fix round 2) ----
const { encodeCoast, sq } = require('./coastenc.js');
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

test('keys: Escape clears first, then closes; owned from the close button, the tools button, the gear (panel closed) and a marker', () => {
  const E = makeEnv(); E.pick('distance');
  const two = () => { E.clickAt(21.3, -157.9); E.clickAt(21.0, -157.0); };
  two();
  const ev = E.key('Escape', E.q('.tools-x'));
  assert.equal(ev._stop, true); assert.equal(E.s.pts.length, 0, 'focus on the close button: cleared');
  assert.equal(E.A.active(), true, 'the first Escape only clears');
  two(); E.key('Escape', E.btn); assert.equal(E.s.pts.length, 0, 'focus on the tools button');
  two(); E.gearPanel.hidden = false; E.key('Escape', E.gear);
  assert.equal(E.s.pts.length, 2, 'the gear with its panel open keeps its Escape');
  E.gearPanel.hidden = true; E.key('Escape', E.gear); assert.equal(E.s.pts.length, 0, 'the gear with its panel closed');
  const marker = E.doc.createElement('div'); E.mapEl.appendChild(marker);
  two(); E.key('Backspace', marker); assert.equal(E.s.pts.length, 1, 'focus on a marker (a live buoy): Backspace undoes');
  const ctl = E.doc.createElement('div'); ctl.classList.add('leaflet-control'); E.mapEl.appendChild(ctl); const inCtl = E.doc.createElement('button'); ctl.appendChild(inCtl);
  E.key('Escape', inCtl); assert.equal(E.s.pts.length, 1, 'another control keeps its keys');
  E.btn.dispatch('click');                                                   // the tools menu open: its own Escape
  E.key('Escape'); assert.equal(E.s.pts.length, 1);
});

test('touch: a double-tap on the first point keeps the outline; a double-tap finish adds no point (16 px)', () => {
  const tap = { pointerType: 'touch' }, px = (n) => n / 400;
  const T = makeEnv({ touch: true }); T.pick('area');
  [[21.8, -158.3], [21.8, -157.7], [21.3, -157.7]].forEach(([a, b]) => T.clickAt(a, b, tap));
  T.clickAt(21.8, -158.3, tap); T.clickAt(21.8 - px(10), -158.3, tap); T.map.fire('dblclick');
  assert.equal(T.s.closed, true); assert.equal(T.s.pts.length, 3, 'the second tap 10 px away is the same gesture');
  const D2 = makeEnv({ touch: true }); D2.pick('distance');
  D2.clickAt(21.3, -157.9, tap); D2.clickAt(21.0, -157.0, tap);
  D2.clickAt(20.5, -156.5, tap); D2.clickAt(20.5 - px(10), -156.5, tap); D2.map.fire('dblclick');
  assert.equal(D2.s.pts.length, 3); assert.equal(D2.s.closed, true);
  const M = makeEnv(); M.pick('distance');
  M.clickAt(21.3, -157.9); M.clickAt(21.3 - px(10), -157.9);
  assert.equal(M.s.pts.length, 2, 'a mouse keeps 4 px: two deliberate clicks 10 px apart are two points');
});

test('a press that began on a control and was released over the map is not a map click (G20 re-review)', () => {
  const E = makeEnv(); E.pick('distance');
  const ctl = E.doc.createElement('div'); ctl.classList.add('leaflet-control'); E.mapEl.appendChild(ctl);
  const onMap = { pointerType: 'mouse', target: E.mapEl, composedPath: () => [E.mapEl] };
  E.pressOn(ctl); E.clickAt(21.3, -157.9, onMap);
  assert.equal(E.s.pts.length, 0, 'selecting the bar\'s text and releasing over the map');
  E.pressOn(E.mapEl); E.clickAt(21.3, -157.9, onMap);
  assert.equal(E.s.pts.length, 1, 'the next press on the map counts');
});

test('hover restyles one wedge in place; a stale refusal never overwrites a newer message; the bar reports growth', async () => {
  const E = makeEnv(); E.pick('exposure');
  E.clickAt(21.6655, -158.054); await E.settle();
  const f = E.fan(), c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  const stroke = (k) => f._el.querySelector('[data-k="' + k + '"]').getAttribute('stroke');
  E.mapEl.dispatch('mousemove', { clientX: c.x, clientY: c.y + 60 });
  assert.equal(stroke(36), '#fde047');
  E.mapEl.dispatch('mousemove', { clientX: c.x, clientY: c.y - 60 });
  assert.equal(stroke(0), '#fde047'); assert.equal(stroke(36), 'none', 'the previous wedge is cleared');
  assert.equal(E.fan(), f, 'the fan is not rebuilt');
  assert.equal(E.q('.tools-bar-actions').hidden, false, 'Clear is shown with a result');
  E.bar._h = 320; E.clickAt(21.6655, -158.054); await E.settle();
  assert.equal(E.layouts[E.layouts.length - 1], true, 'a taller bar: the page may minimise a window it now overlaps');
  E.bar._h = 250; E.clickAt(21.269, -157.829); await E.settle();
  assert.equal(E.layouts[E.layouts.length - 1], false, 'a shorter bar: nothing to minimise');
  const S = makeEnv(); S.pick('exposure');
  S.clickAt(21.5, -158.0); S.clickAt(76.5, 15);                              // inland (refused later), then beyond 75 N
  await wait(60);
  assert.match(S.body(), /between 75°S and 75°N/, 'the older inland click does not replace the message');
});

test('resize: a visible fan is re-placed, one the user panned away from is only redrawn at the new size', async () => {
  const E = makeEnv({ mapW: 1000, mapH: 700, lat0: 22, lng0: -158.8, scale: 400 });
  E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle();
  assert.equal(E.s.radius, 120); const n = E.map.pans.length;
  E.win.fire('resize'); await wait(200);
  assert.equal(E.map.pans.length, n + 1, 'on screen: placed again');
  E.view.lng0 = -150; E.mapEl.rect = { left: 0, top: 0, width: 520, height: 700 };   // panned away; a narrower map
  E.win.fire('resize'); await wait(200);
  assert.equal(E.map.pans.length, n + 1, 'off screen: no pan back');
  assert.equal(E.s.radius, 90, 'but redrawn at the new default size');
});

test('the fan avoids the page\'s windows and shrinks to fit (the radius it was placed at is the one drawn)', async () => {
  const win = { hidden: false, getBoundingClientRect: () => ({ left: 0, top: 250, right: 700, bottom: 700, width: 700, height: 450 }) };
  const E = makeEnv({ mapW: 1000, mapH: 700, lat0: 22, lng0: -158.8, scale: 400, obstacles: [win] });
  E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle();
  assert.equal(E.s.radius, 100, 'a smaller fan above the window, not a long pan');
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]), pan = E.map.pans[E.map.pans.length - 1] || [0, 0];
  const at = { x: c.x - pan[0], y: c.y - pan[1] };
  assert.ok(at.y + 116 <= 250, 'clear of the window: ' + JSON.stringify(at));
});

test('nothing fits (a small window with the overlay panel): the fan never sits centred under the tool bar', async () => {
  const panel = { hidden: false, getBoundingClientRect: () => ({ left: 0, top: 0, right: 120, bottom: 300, width: 120, height: 300 }) };
  const E = makeEnv({ mapW: 420, mapH: 300, lat0: 22.0405, lng0: -158.679, scale: 400, barRect: { left: 120, top: 50, width: 290, height: 240 }, obstacles: [panel] });
  E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle();
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]), pan = E.map.pans[E.map.pans.length - 1] || [0, 0];
  const at = { x: c.x - pan[0], y: c.y - pan[1] };
  assert.equal(E.s.radius, 50);
  assert.ok(!(at.x >= 120 && at.x <= 410 && at.y >= 50 && at.y <= 290), 'not under the bar: ' + JSON.stringify(at));
});

test('the fan stays in the click\'s world copy across the dateline', async () => {
  const land = [[sq(179.0, -16.5, 180.0, -16.0)]];
  const E = makeEnv({ lat0: -15.4, lng0: 178.8, cells: { '-20_175': [1, 1] }, files: { '/world-i.bin': encodeCoast(land, 30), '/f/-20_175.bin': encodeCoast(land, 5) } });
  E.pick('exposure'); E.clickAt(-16.25, 179.9995); await E.settle();
  assert.ok(E.s.result, E.body());
  assert.ok(E.s.result.origin.lng < -179.99, 'evaluated east of the dateline: ' + E.s.result.origin.lng);
  assert.ok(E.s.fanLng > 180 && E.s.fanLng < 180.01, 'drawn beside the click, not a world away: ' + E.s.fanLng);
});

// ---- G20 re-check (fix round 3) ----
test('the bar grows only above its tallest height since the tool started (not the dip to "Computing…" and back)', async () => {
  const E = makeEnv(); E.pick('exposure');
  assert.deepEqual(E.layouts, [true], 'shown');
  E.bar._h = 320; E.clickAt(21.6655, -158.054); await E.settle();
  assert.equal(E.layouts[E.layouts.length - 1], true, 'taller than ever: the page may minimise a window');
  E.bar._h = 250; E.clickAt(21.269, -157.829); await E.settle();
  E.bar._h = 320; E.clickAt(21.6655, -158.054); await E.settle();
  assert.equal(E.layouts[E.layouts.length - 1], false, 'back to a height it had: a window the user opened meanwhile stays');
  E.q('.tools-x').dispatch('click'); E.bar._h = 0; E.pick('distance');
  assert.equal(E.layouts[E.layouts.length - 1], true, 'a new tool starts afresh');
});

test('the bar ends 8 px above the map\'s bottom edge, at least 60 px tall', () => {
  const E = makeEnv({ mapH: 150 }); E.pick('distance');
  assert.equal(E.bar.style.maxHeight, '82px', '150 - 60 - 8');
  const S = makeEnv({ mapH: 100 }); S.pick('distance');
  assert.equal(S.bar.style.maxHeight, '60px');
});

test('the overlay\'s phone sheet: an obstacle for the fan, not a map click when pressed, and it keeps its keys', async () => {
  const E = makeEnv({ mapW: 375, mapH: 764, lat0: 22, lng0: -158.7, scale: 400 });
  const sheet = E.doc.createElement('div'); sheet.classList.add('ov-sheet'); sheet.rect = { left: 50, top: 700, width: 325, height: 64 };
  E.mapEl.appendChild(sheet); const btn = E.doc.createElement('button'); sheet.appendChild(btn);
  E.pick('distance');
  E.pressOn(btn); E.clickAt(21.3, -157.9, { pointerType: 'touch', target: E.mapEl, composedPath: () => [E.mapEl] });
  assert.equal(E.s.pts.length, 0, 'a press on the sheet released over the map');
  E.pressOn(E.mapEl); E.clickAt(21.3, -157.9, { pointerType: 'touch', target: E.mapEl, composedPath: () => [E.mapEl] }); E.clickAt(21.0, -157.0);
  E.key('Escape', btn); assert.equal(E.s.pts.length, 2, 'Escape on a sheet button is the sheet\'s');
  // the fan: Pipeline lands at y (22 - 21.6655) * 400 = 134; move the view so it sits at the bottom, over the sheet
  E.view.lat0 = 21.6655 + 690 / 400;
  E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle();
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]), pan = E.map.pans[E.map.pans.length - 1] || [0, 0];
  assert.ok(c.y - pan[1] + E.s.radius + 16 <= 701, 'clear of the sheet (pans are whole pixels): ' + (c.y - pan[1]));
});

test('a rotation re-places a fan that was on the map, although the resize itself pushed it off (G20 re-check)', async () => {
  const E = makeEnv({ mapW: 375, mapH: 764, lat0: 22, lng0: -158.7, scale: 400 });
  E.view.lat0 = 21.6655 + 600 / 400;                                        // the fan at y 600 of a portrait map
  E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle();
  const n = E.map.pans.length;
  // rotate: 375 x 764 -> 812 x 327; Leaflet keeps the centre, so every point shifts by half the size change
  E.mapEl.rect = { left: 0, top: 0, width: 812, height: 327 };
  E.view.lng0 -= (812 - 375) / 2 / 400; E.view.lat0 += (327 - 764) / 2 / 400;
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  assert.ok(c.y > 327, 'the resize pushed the fan below the new map: ' + c.y);
  E.win.fire('resize'); await wait(200);
  assert.equal(E.map.pans.length, n + 1, 'placed again');
});

// ---- G20 re-check of fix round 3 (fix round 4) ----
test('a rotation where only the height change shows the fan was on the map: it is placed again', async () => {
  const E = makeEnv({ mapW: 812, mapH: 327, lat0: 22, lng0: -158.7, scale: 400 });
  E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle();
  // the user pans so the fan sits at (250, 300) of the landscape map
  E.view.lng0 = E.s.fanLng - 250 / 400; E.view.lat0 = E.s.result.origin.lat + 300 / 400;
  const n = E.map.pans.length;
  // rotate to 375 x 764: Leaflet keeps the centre, so the point moves by half the size change: (31.5, 518.5), below the
  // old height but on the new map, and too near the new left edge
  E.mapEl.rect = { left: 0, top: 0, width: 375, height: 764 };
  E.view.lng0 += (812 - 375) / 2 / 400; E.view.lat0 += (764 - 327) / 2 / 400;
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  assert.ok(Math.abs(c.x - 31.5) < 1e-6 && Math.abs(c.y - 518.5) < 1e-6, JSON.stringify(c));
  E.win.fire('resize'); await wait(200);
  assert.equal(E.map.pans.length, n + 1, 'placed again (judged at its pre-rotation point)');
});

test('switching tools at the same bar height still reports the bar, and a later shrink is not growth', async () => {
  const E = makeEnv(); E.pick('distance');
  const k = E.layouts.length;
  E.pick('exposure');
  assert.equal(E.layouts.length, k + 1, 'the new tool reports its bar although its height did not change');
  assert.equal(E.layouts[E.layouts.length - 1], true);
  E.bar._h = 150; E.clickAt(21.6655, -158.054); await E.settle();
  assert.equal(E.layouts[E.layouts.length - 1], false, 'shorter: not growth');
});

// ---- G20 re-check of fix round 4 (fix round 5) ----
test('placement waits for an animation frame, so "Computing…" is painted first; a hidden tab (no frames) still computes', async () => {
  const frames = [], realTimeout = global.setTimeout, delays = [];
  let mark = -1;
  const E = makeEnv({ raf: (fn) => { frames.push(fn); mark = delays.length; } });
  global.setTimeout = function (fn, ms) { delays.push(ms || 0); return realTimeout.apply(this, arguments); };
  try {
    E.pick('exposure'); E.clickAt(21.6655, -158.054);
    await wait(40);
    assert.equal(frames.length, 1, 'a frame was asked for');
    assert.equal(delays[mark], 100, 'beside the frame only the 100 ms fallback, no 0 ms timer that would run first (G20 re-check R6)');
    assert.equal(E.s.busy, true, 'nothing placed before the frame');
    assert.match(E.body(), /Computing/);
    const before = delays.length; frames.shift()();
    assert.deepEqual(delays.slice(before), [0], 'the frame queues a task (placement runs after the frame is painted, not inside it)');
  } finally { global.setTimeout = realTimeout; }
  await E.settle();
  assert.ok(E.s.result && E.fan(), 'placed once the frame ran');
  const H = makeEnv({ raf: () => {} });                                      // a hidden tab: frames never run
  H.pick('exposure'); const t0 = Date.now(); H.clickAt(21.6655, -158.054); await H.settle();
  assert.ok(H.s.result && Date.now() - t0 >= 90, 'placed after the fallback timer: ' + (Date.now() - t0) + ' ms');
});

test('the page keeps focus where it was when it minimises a window for the tool bar (templates/index.html onLayout)', () => {
  // the template's own onLayout, on a small DOM with browser focus rules: focus() on a hidden or detached element does
  // nothing, blur() leaves focus on the page, minimising a window focuses its opener (G20 re-checks R4, R5)
  const html = fs.readFileSync(path.join(__dirname, '..', '..', 'templates', 'index.html'), 'utf8').replace(/\r/g, '');
  const a = html.indexOf('onLayout: function (bar, grew) {'), b = html.indexOf('// the floating windows are obstacles for the exposure fan');
  const src = html.slice(a + 'onLayout: '.length, b).trim().replace(/,\s*$/, '');
  function run(setup) {
    const doc = { byId: {}, getElementById(id) { return this.byId[id] || null; }, contains(e) { return !!e && !e.detached; } };
    const el = (id, o) => {
      const e = Object.assign({ id, hidden: false, detached: false, classList: { set: new Set(), contains(c) { return this.set.has(c); }, add(c) { this.set.add(c); } } }, o || {});
      e.contains = (x) => { for (let y = x; y; y = y.parent) if (y === e) return true; return false; };
      e.focus = () => { if (!e.hidden && !e.detached) doc.activeElement = e; };
      e.blur = () => { if (doc.activeElement === e) doc.activeElement = doc.body; };
      e.getBoundingClientRect = () => e.rect || { left: 0, top: 0, right: 0, bottom: 0 };
      if (id) doc.byId[id] = e; return e;
    };
    doc.body = el('body'); doc.activeElement = doc.body;
    const bar = el('bar', { rect: { left: 700, top: 60, right: 990, bottom: 260 } });
    const E = { doc, bar, x: el('x', { parent: bar }), clear: el('clear', { parent: bar }), opener: el('opener'), liveOpener: el('liveOpener') };
    E.fw = el('forecastWin', { rect: { left: 600, top: 40, right: 1000, bottom: 500 } }); E.lp = el('liveBuoyPanel', { rect: { left: 650, top: 100, right: 1000, bottom: 400 } });
    E.inFw = el('fwInput', { parent: E.fw });
    el('fwMin', { click() { E.fw.classList.add('fw-min'); E.opener.focus(); } });
    el('lwMin', { click() { E.lp.classList.add('fw-min'); E.liveOpener.focus(); } });
    setup(E);
    new Function('document', 'measureTopRight', 'return ' + src)(doc, () => {})(bar, true);
    assert.ok(E.fw.classList.contains('fw-min') && (E.lp.hidden || E.lp.classList.contains('fw-min')), 'minimised');
    return doc.activeElement.id;
  }
  assert.equal(run((E) => E.x.focus()), 'x', 'back on the tool');
  assert.equal(run(() => {}), 'body', 'the page keeps it');
  assert.equal(run((E) => E.inFw.focus()), 'opener', 'inside the window: its opener');
  assert.equal(run((E) => { E.clear.focus(); E.clear.hidden = true; }), 'body', 'an element hidden since: off the opener');
  assert.equal(run((E) => { E.clear.focus(); E.clear.detached = true; }), 'body', 'an element removed since: off the opener');
  assert.equal(run((E) => { E.lp.hidden = true; }), 'body', 'one window');
});

// ---- the window projected on the map (plan section 30) ----
async function reachDrawn(E) { for (let i = 0; i < 400 && !E.s.reachOn; i++) await wait(5); }
const kinds = (E) => [...E.s.reach._items].reduce((m, l) => { m[l.kind] = (m[l.kind] || 0) + 1; return m; }, {});
test('the projection: a veil with three holes, rays and rings after a result; gone after Clear, a new click and a tool switch', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  assert.ok(E.s.reachOn && E.s.result.reach, 'drawn');
  const veil = [...E.s.reach._items].find((l) => l.kind === 'polygon');
  assert.ok(veil && veil.a.length === 4 && veil.o.pane === 'toolsReachPane', 'outer ring and three world copies of the lit ring');
  assert.ok(veil.a[2].length > 1000 && Math.abs(veil.a[1][0][1] - veil.a[2][0][1] + 360) < 1e-9, 'copies 360 degrees apart');
  const k = kinds(E);
  assert.ok(k.polyline > 30 && k.label >= 3, JSON.stringify(k));
  const rays = [...E.s.reach._items].filter((l) => l.kind === 'polyline' && !l.o.dashArray);
  assert.ok(rays.every((l) => l.o.pane === 'toolsReachPane' && l.a.length >= 2), 'every ray in the pane');
  E.q('[data-act="clear"]').dispatch('click');
  assert.ok(!E.s.reachOn && E.s.reach._items.size === 0, 'Clear removes it');
  E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  E.clickAt(21.269, -157.829);
  assert.ok(!E.s.reachOn && E.s.reach._items.size === 0, 'a new click removes the old one at once');
  await E.settle(); await reachDrawn(E);
  assert.ok(E.s.reachOn);
  E.pick('distance');
  assert.ok(!E.s.reachOn && E.s.reach._items.size === 0, 'a tool switch removes it');
});
test('the projection: a click superseded during its reach draws nothing; a unit change relabels the rings', async () => {
  const E = makeEnv(); E.pick('exposure');
  E.clickAt(21.6655, -158.054); await E.settle();
  E.clickAt(21.269, -157.829); await E.settle(); await reachDrawn(E); await wait(50);
  const veil = [...E.s.reach._items].filter((l) => l.kind === 'polygon' && l.a.length === 4);
  assert.equal(veil.length, 1, 'one projection');
  const light = [...E.s.reach._items].filter((l) => l.kind === 'polygon' && l.o.fillOpacity === 0.25);
  assert.ok(light.length === 0 || light.length === 3, 'small-island shadows: none, or one per world copy');
  light.forEach((l) => assert.ok(l.o.fillOpacity < veil[0].o.fillOpacity, 'lighter than the veil'));
  assert.ok(Math.abs(E.s.result.origin.lat - 21.27) < 0.02, 'the second click\'s');
  const labels = () => [...E.s.reach._items].filter((l) => l.kind === 'label').map((l) => l.o.icon.html);
  assert.ok(labels().length && labels().every((x) => / nm<\/span>$/.test(x)), labels().join());
  E.unitSel.value = 'Metric'; E.unitSel.dispatch('change');
  assert.ok(labels().length && labels().every((x) => / km<\/span>$/.test(x)), labels().join());
});

test('zoomed out the fan is a compass (no pan); it grows back from zoom 6.25; the dead band keeps either', async () => {
  const E = makeEnv({ zoom: 5 }); E.pick('exposure');
  const pans = E.map.pans.length;
  E.clickAt(21.6655, -158.054); await E.settle();
  assert.ok(E.s.compass && E.s.radius === 44, 'a compass: ' + E.s.radius);
  assert.equal(E.map.pans.length, pans, 'a compass is not panned into view');
  E.view.zoom = 6; E.map.fire('zoomend');
  assert.ok(E.s.compass, 'inside the dead band it stays a compass');
  E.view.zoom = 6.5; E.map.fire('zoomend');
  assert.ok(!E.s.compass && E.s.radius === 120, 'the full fan: ' + E.s.radius);
  E.view.zoom = 6; E.map.fire('zoomend');
  assert.ok(!E.s.compass, 'inside the dead band it stays full');
  E.view.zoom = 5.5; E.map.fire('zoomend');
  assert.ok(E.s.compass && E.s.radius === 44);
  // a touch tap on the compass picks its wedge (G21 B-3); on its centre it does nothing; outside it, a new point
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]), gen = E.s.gen;
  const tap = (dx, dy) => E.map.fire('click', { latlng: E.map.containerPointToLatLng([c.x + dx, c.y + dy]), originalEvent: { pointerType: 'touch' } });
  tap(0, 30);
  assert.ok(E.s.gen === gen && E.s.selected === 36, 'the south wedge: ' + E.s.selected);
  assert.match(E.body(), /<div class="tools-sector">S 180/);
  tap(0, 5);
  assert.ok(E.s.gen === gen && E.s.result, 'the centre: nothing');
  tap(0, 60);
  assert.ok(E.s.gen > gen, 'outside the compass: a new point');
});

test('the cursor readout: off the fan it reads the map (and a line to the cursor); on the fan a wedge; leaving clears it', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });             // north-west, over open water
  assert.ok(E.s.readout && E.s.readout.visible, 'a readout');
  assert.match(E.body(), /° (NW|WNW|NNW) · [\d,.]+ (mi|ft) · [\d,.]+ nm/);
  assert.match(E.body(), /In the window · swell 2 h at 14 s, 2 h at 18 s/, 'hours under a day');
  const line = [...E.layers].find((l) => l.kind === 'polyline' && l.o.dashArray === '2 4');
  assert.ok(line && line.o.pane === 'toolsReachPane', 'a line to the cursor');
  const end = line.a[line.a.length - 1], at = E.map.containerPointToLatLng([c.x - 300, c.y - 200]);
  assert.ok(Math.abs(end[0] - at.lat) < 1e-6 && Math.abs(end[1] - at.lng) < 1e-6, 'it ends at the cursor');
  E.mapEl.dispatch('mousemove', { clientX: c.x, clientY: c.y + 60 });                   // over the fan: a wedge
  assert.ok(!E.s.readout && E.s.selected >= 0, 'the wedge');
  assert.ok(![...E.layers].includes(line), 'the line goes');
  E.mapEl.dispatch('mousemove', { clientX: c.x + 20, clientY: c.y - 300 });
  assert.ok(E.s.readout);
  E.mapEl.dispatch('mouseleave', {});
  assert.ok(!E.s.readout && ![...E.layers].some((l) => l.kind === 'polyline' && l.o.dashArray === '2 4'), 'cleared');
  // panned one world west: the cursor's longitude is 360 less, and the line is drawn in that world copy
  E.view.lng0 -= 360;
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });
  const west = [...E.layers].find((l) => l.kind === 'polyline' && l.o.dashArray === '2 4'), wEnd = west.a[west.a.length - 1], wAt = E.map.containerPointToLatLng([c.x - 300, c.y - 200]);
  assert.ok(wAt.lng < -360 && Math.abs(wEnd[1] - wAt.lng) < 1e-6 && Math.abs(west.a[0][1] - (E.s.fanLng - 360)) < 1e-6, 'from the copy of the spot in that world to the cursor');
  E.view.lng0 += 360; E.mapEl.dispatch('mouseleave', {});
  E.unitSel.value = 'Metric'; E.unitSel.dispatch('change');
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });
  assert.match(E.body(), / km · [\d,.]+ nm/);
});

test('Lock: the window stays, the page has its clicks back (active() false, no crosshair, double-click zoom), a click only moves the readout', async () => {
  const E = makeEnv(); E.pick('exposure');
  const lock = E.q('[data-act="lock"]');
  assert.ok(lock.hidden, 'no Lock before a result');
  lock.dispatch('click');
  assert.ok(!E.s.locked && E.A.active(), 'and it does nothing then');
  E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  assert.ok(!lock.hidden && lock.textContent === 'Lock' && !lock.classList.contains('is-on') && !lock.hasAttribute('aria-pressed'));
  lock.dispatch('click');
  assert.ok(E.s.locked && lock.textContent === 'Unlock' && lock.classList.contains('is-on') && !lock.hasAttribute('aria-pressed'), 'the label alone says it (G21 B-11)');
  assert.equal(E.A.active(), false, 'the page\'s markers work again');
  assert.ok(!E.mapEl.classList.contains('tools-active') && E.map.doubleClickZoom.enabled(), 'no crosshair; double-click zooms');
  const gen = E.s.gen, origin = E.s.result.origin;
  E.clickAt(22.5, -160);
  assert.ok(E.s.gen === gen && E.s.result.origin === origin && E.s.reachOn, 'the point stays');
  assert.ok(E.s.readout && E.s.readout.km > 100, 'the readout moved there');
  E.key('Escape');
  assert.ok(E.s.result && E.s.locked, 'Escape with focus on the page does nothing while locked');
  E.key('Escape', E.q('.tools-x'));
  assert.ok(E.s.result && !E.s.locked, 'Escape in the bar only unlocks (G21 B-9: it is where focus is just after Lock)');
  E.key('Escape', E.q('.tools-x'));
  assert.ok(!E.s.result && !E.s.locked, 'then it clears');
  assert.ok(E.mapEl.classList.contains('tools-active') && !E.map.doubleClickZoom.enabled() && E.A.active(), 'the tool has the map again');
  E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  lock.dispatch('click'); lock.dispatch('click');
  assert.ok(!E.s.locked && E.A.active(), 'Unlock');
  lock.dispatch('click'); E.q('.tools-x').dispatch('click');
  assert.ok(!E.s.locked && !E.A.active() && E.map.doubleClickZoom.enabled(), 'closing unlocks and gives double-click back');
});

const cursorLineOf = (E) => [...E.layers].find((l) => l.kind === 'polyline' && l.o.dashArray === '2 4');
test('touch: a tap\'s compatibility mousemove neither starts nor moves the readout; a tapped readout stays through a mouseleave', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]), water = { clientX: c.x - 300, clientY: c.y - 200 };
  const now = Date.now; let skew = 0; Date.now = () => now() + skew;
  try {
    E.mapEl.dispatch('touchstart', {});
    E.mapEl.dispatch('mousemove', water);
    assert.ok(!E.s.readout, 'within a second of a touch: the tap\'s own mousemove');
    skew = 1500; E.mapEl.dispatch('pointerdown', { pointerType: 'touch' });
    E.mapEl.dispatch('mousemove', water);
    assert.ok(!E.s.readout, 'a touch pointer counts too');
    skew = 3000; E.mapEl.dispatch('pointerdown', { pointerType: 'pen' });
    E.mapEl.dispatch('mousemove', water);
    assert.ok(!E.s.readout, 'so does a pen');
    skew = 4500;
    E.mapEl.dispatch('mousemove', Object.assign({ sourceCapabilities: { firesTouchEvents: true } }, water));
    assert.ok(!E.s.readout, 'a mousemove that says it came from touch');
    E.mapEl.dispatch('pointerdown', { pointerType: 'mouse' });
    E.mapEl.dispatch('mousemove', water);
    assert.ok(E.s.readout && !E.s.pinned, 'a mouse: the hover readout');
    E.mapEl.dispatch('mouseleave', {});
    assert.ok(!E.s.readout && !cursorLineOf(E), 'leaving the map ends a hover readout');
    // locked, a tap pins the readout: a window opening over the map sends a mouseleave, and the readout stays
    E.q('[data-act="lock"]').dispatch('click');
    E.clickAt(22.5, -160, { pointerType: 'touch' });
    assert.ok(E.s.readout && E.s.pinned && cursorLineOf(E));
    E.mapEl.dispatch('mouseleave', {});
    assert.ok(E.s.readout && cursorLineOf(E), 'a tapped readout stays, with its line');
    // after Unlock, a tap on a wedge takes the readout's line as well as its text
    E.q('[data-act="lock"]').dispatch('click');
    E.map.fire('click', { latlng: E.map.containerPointToLatLng([c.x, c.y + 60]), originalEvent: { pointerType: 'touch' } });
    assert.ok(!E.s.readout && E.s.selected >= 0 && !cursorLineOf(E), 'the wedge; the line goes');
  } finally { Date.now = now; }
});

test('a new point drops the old spot\'s readout and its line at once', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });
  assert.ok(E.s.readout && cursorLineOf(E));
  E.clickAt(21.269, -157.829);
  assert.ok(E.s.busy && !E.s.readout && !cursorLineOf(E) && ![...E.layers].some((l) => l.kind === 'polyline' && l.o.color === '#0b2536'), 'gone with the old spot, casing too');
  await E.settle(); await reachDrawn(E);
  assert.ok(E.s.result && !E.s.readout);
  const c2 = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  E.mapEl.dispatch('mousemove', { clientX: c2.x - 300, clientY: c2.y - 200 });
  assert.ok(E.s.readout && cursorLineOf(E));
  E.q('[data-act="clear"]').dispatch('click');
  assert.ok(!E.s.readout && !cursorLineOf(E), 'Clear takes them too');
});

test('the spot\'s copy follows the view (as the page\'s markers do): the fan, the window and a tapped readout\'s line', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const lng0 = E.s.fanLng, veil = () => [...E.s.reach._items].find((l) => l.kind === 'polygon' && l.a.length === 4);
  const v1 = veil();
  E.view.lng0 += 100; E.map.fire('moveend');
  assert.ok(E.s.fanLng === lng0 && veil() === v1, 'a pan inside the world changes nothing');
  E.view.lng0 += 150; E.map.fire('moveend');                                    // the view's centre 250 degrees east of the spot
  assert.equal(E.s.fanLng, lng0 + 360, 'the nearest copy, as the markers\' currentWorldOffset rounds');
  E.view.lng0 -= 250; E.map.fire('moveend');
  assert.equal(E.s.fanLng, lng0, 'and back');
  // a hovered readout is read again under the pointer when the map moves under it (a keyboard pan)
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });
  const hovered = cursorLineOf(E).a.slice(-1)[0];
  E.view.lng0 += 720; E.map.fire('moveend');
  assert.equal(E.s.fanLng, lng0 + 720, 'the spot\'s copy in the world in view');
  const again = cursorLineOf(E).a.slice(-1)[0];
  assert.ok(Math.abs(again[0] - hovered[0]) < 1e-9 && Math.abs(again[1] - (hovered[1] + 720)) < 1e-9, 'under the pointer: ' + again);
  E.view.lng0 += 0.5; E.map.fire('moveend');
  assert.ok(Math.abs(cursorLineOf(E).a.slice(-1)[0][1] - (hovered[1] + 720.5)) < 1e-9, 'a small pan too');
  E.view.lng0 -= 0.5;
  E.view.lng0 -= 720; E.map.fire('moveend');
  // a tapped readout (locked, touch) follows
  E.q('[data-act="lock"]').dispatch('click');
  E.clickAt(22.5, -160, { pointerType: 'touch' });
  E.view.lng0 += 720; E.map.fire('moveend');                                    // two worlds east
  assert.equal(E.s.fanLng, lng0 + 720);
  assert.ok(Math.abs(E.fan().a[1] - (lng0 + 720)) < 1e-9, 'the fan there');
  const outer = veil().a[0];
  assert.ok(Math.abs(outer[0][1] - (lng0 + 180)) < 1e-9 && Math.abs(outer[1][1] - (lng0 + 1260)) < 1e-9, 'the window round it');
  assert.ok(veil().a.slice(1).every((h, j) => h.every((p) => Math.abs(p[1] - (E.s.fanLng + (j - 1) * 360)) < 180)), 'its holes round the fan\'s copy too');
  const end = cursorLineOf(E).a.slice(-1)[0];
  assert.ok(E.s.readout && E.s.pinned && Math.abs(end[1] - 560) < 1e-6 && Math.abs(end[0] - 22.5) < 1e-6, 'the tapped readout\'s line in that world too');
});

// ---- G21 fix round: what is drawn, the bar, Lock and touch ----
const dist = (A, o, p) => A.distanceKm(o, { lat: p[0], lng: p[1] });
test('the projection is on the map while a result stands, on its own canvas; the owner\'s style; holes, rays, arcs and labels where they belong (G21 A-1)', async () => {
  const E = makeEnv({ canvas: true }); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const A = E.A._internals, res = E.s.result, o = res.origin, shift = E.s.fanLng - o.lng, items = [...E.s.reach._items];
  assert.ok(E.layers.has(E.s.reach), 'the group is on the map');
  const pane = E.map.getPane('toolsReachPane');
  assert.ok(pane.style.zIndex === 320 && pane.style.pointerEvents === 'none', 'above the wave colours and particles, below the stations; no pointer events');
  const veil = items.find((l) => l.kind === 'polygon' && l.a.length === 4);
  assert.ok(veil.o.fillOpacity === 0.55 && veil.o.stroke === false && veil.o.pane === 'toolsReachPane' && veil.o.interactive === false);
  assert.ok(veil.o.renderer && veil.o.renderer.o.padding === 0.5 && veil.o.renderer.o.pane === 'toolsReachPane', 'its own canvas, half a map of padding (G21 B-7)');
  // the holes: the ring of the bridged reach, one per world copy round the fan's
  const wide = A.litRing(res, 100, res.reachWide), own = A.litRing(res);
  [-360, 0, 360].forEach((k, j) => {
    const h = veil.a[j + 1];
    assert.ok(h.length === wide.length && h.every((p, n) => Math.abs(p[0] - wide[n].lat) < 1e-12 && Math.abs(p[1] - (wide[n].lng + shift + k)) < 1e-9), 'hole ' + j);
  });
  assert.deepEqual(veil.a[0].map((p) => p[1]), [E.s.fanLng - 540, E.s.fanLng + 540, E.s.fanLng + 540, E.s.fanLng - 540]);
  assert.deepEqual(veil.a[0].map((p) => p[0]), [85, 85, -85, -85], 'the box covers the map to its limits');
  // the lighter strips: one layer per copy, a polygon per strip (Kaula and the other small islands seen from Pipeline)
  const strips = items.filter((l) => l.kind === 'polygon' && l.o.fillOpacity === 0.25), sp = A.stripPolygons(res, res.reachWide, 100);
  assert.ok(strips.length === 3 && sp.length >= 2, strips.length + ' layers, ' + sp.length + ' strips');
  [-360, 0, 360].forEach((k, j) => {
    const s = strips[j];
    assert.ok(s.o.stroke === false && s.a.length === sp.length && s.a.every((poly, m) => poly.length === 1 && poly[0].length === sp[m].length &&
      poly[0].every((p, q) => Math.abs(p[0] - sp[m][q].lat) < 1e-12 && Math.abs(p[1] - (sp[m][q].lng + shift + k)) < 1e-9)), 'copy ' + j);
  });
  assert.ok(own.length > 1000, '(the own ring is no longer drawn)');
  // rays: every 10th (5 degrees) that runs 300 km or more, from the spot to its reach, in three copies
  const rays = items.filter((l) => l.kind === 'polyline' && !l.o.dashArray), want = [];
  for (let i = 5; i < 720; i += 10) if (res.reach[i] >= 300) want.push(i);
  assert.equal(rays.length, 3 * want.length);
  [-360, 0, 360].forEach((k, j) => want.forEach((i, n) => {
    const line = rays[j * want.length + n].a, end = A.rayPoint(o, A.rayBearing(i), res.reach[i]), last = line[line.length - 1];
    assert.equal(line.length, A.rayPath(o, A.rayBearing(i), res.reach[i], 100, 25).length, 'pieces of 25 km or more');
    assert.ok(Math.abs(line[0][1] - (o.lng + shift + k)) < 1e-9 && Math.abs(last[0] - end.lat) < 1e-9 && Math.abs(last[1] - (end.lng + shift + k)) < 1e-9, 'ray ' + i);
  }));
  rays.forEach((l) => assert.ok(l.o.opacity === 0.5 && l.o.weight === 1 && l.o.color === '#ffffff'));
  // rings: arcs on their distance inside the window; labels on them, in the projection's pane
  const rings = A.ringsFor('US', A.ringReachKm(res)), arcs = items.filter((l) => l.kind === 'polyline' && l.o.dashArray === '4 6');
  const inWin = (b, km) => [b - 1e-6, b + 1e-6].some((x) => res.reachWide[Math.floor((((x % 360) + 360) % 360) / 0.5) % 720] >= km);   // a ray boundary: either side
  assert.ok(rings.length >= 2 && arcs.length >= 3 * rings.length, rings.length + ' rings, ' + arcs.length + ' arcs');
  arcs.forEach((l) => {
    const pts = l.a.map((p) => [p[0], p[1] - shift - Math.round((p[1] - shift - o.lng) / 360) * 360]);
    const d = dist(A, o, pts[0]), ring = rings.find((r) => Math.abs(r.km - d) < 1e-6);
    assert.ok(ring, 'on a ring: ' + d);
    pts.forEach((p) => { assert.ok(Math.abs(dist(A, o, p) - ring.km) < 1e-6); assert.ok(inWin(A.bearingDeg(o, { lat: p[0], lng: p[1] }), ring.km), 'inside the window'); });
  });
  const copyOf = (lng) => Math.round((lng - shift - o.lng) / 360);
  const perCopy = (ls, f) => [-1, 0, 1].map((k) => ls.filter((l) => copyOf(f(l)) === k).length);
  const arcCopies = perCopy(arcs, (l) => l.a[0][1]);
  assert.ok(arcCopies[0] === arcCopies[1] && arcCopies[1] === arcCopies[2], 'the arcs in each world copy: ' + arcCopies);
  const labels = items.filter((l) => l.kind === 'label').map((l) => Object.assign(l, { tip: /<span>(.*)<\/span>/.exec(l.o.icon.html)[1] }));
  assert.ok(labels.length >= 3 * rings.length && labels.length % 3 === 0);
  const labelCopies = perCopy(labels, (l) => l.a[1]);
  assert.ok(labelCopies[0] === labelCopies[1] && labelCopies[1] === labelCopies[2], 'the labels in each world copy: ' + labelCopies);
  labels.forEach((l) => {
    assert.ok(l.o.pane === 'toolsReachPane' && l.o.interactive === false && l.o.icon.className === 'tools-ring-label' && l.o.icon.iconSize === null, 'below the gridlines and stations (G21 A-11)');
    const p = [l.a[0], l.a[1] - shift - Math.round((l.a[1] - shift - o.lng) / 360) * 360], d = dist(A, o, p);
    const ring = rings.find((r) => r.label === l.tip);
    assert.ok(ring && Math.abs(d - ring.km) < 1e-6, l.tip + ' at ' + d);
    assert.ok(inWin(A.bearingDeg(o, { lat: p[0], lng: p[1] }), ring.km), 'inside the window');
    const b = A.bearingDeg(o, { lat: p[0], lng: p[1] });
    assert.ok(A.ringLabelBearings(res.reachWide, ring.km).some((x) => Math.min(Math.abs(x - b), 360 - Math.abs(x - b)) < 1e-6), 'at the bearing the rule gives: ' + b);
  });
  E.q('[data-act="clear"]').dispatch('click');
  assert.ok(!E.layers.has(E.s.reach) && !E.s.reachOn, 'off the map after Clear');
});

test('the fold button folds the bar to its title (a readout still shows under it, "Computing…" while busy); phones drop the hints (G21 B-6)', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const fold = E.bar.querySelector('.tools-fold'), title = E.q('.tools-bar-title');
  assert.match(E.body(), /tools-hint/);
  fold.dispatch('click');
  assert.ok(E.s.folded && E.bar.classList.contains('is-folded') && fold.getAttribute('aria-expanded') === 'false' && fold.getAttribute('aria-label') === 'Unfold the tool bar');
  assert.ok(E.body() === '' && E.q('.tools-bar-actions').hidden, 'the title row alone');
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });
  assert.match(E.body(), /^<div class="tools-sector">\d{3}° /, 'a readout shows under the title');
  E.mapEl.dispatch('mouseleave', {});
  assert.equal(E.body(), '');
  E.clickAt(21.269, -157.829);
  assert.equal(title.textContent, 'Computing…', 'busy, folded');
  await E.settle(); await reachDrawn(E);
  assert.equal(title.textContent, 'Swell exposure');
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });                                 // a readout while folded
  fold.dispatch('click');
  assert.ok(!E.s.folded && /tools-big/.test(E.body()) && !E.q('.tools-bar-actions').hidden, 'unfolded');
  const kids = E.q('.tools-bar-body').children;
  assert.ok(kids[0].className === 'tools-big' && kids[0].getAttribute('aria-live') === null && kids[1].className === 'tools-sector', 'each line its own kind again');
  fold.dispatch('click'); E.pick('distance');
  assert.ok(!E.s.folded && !E.bar.classList.contains('is-folded'), 'another tool starts unfolded');
  fold.dispatch('click'); E.q('.tools-x').dispatch('click'); E.pick('exposure');
  assert.ok(!E.s.folded && !E.bar.classList.contains('is-folded'), 'a new start is unfolded');
  // a phone: once a result shows, no hint lines
  const P = makeEnv({ mapW: 375, mapH: 700, barRect: { left: 75, top: 60, width: 290, height: 150 }, scale: 300, lat0: 22.3, lng0: -158.6 });
  P.pick('exposure');
  assert.match(P.body(), /Click the water/);
  P.clickAt(21.6655, -158.054); await P.settle();
  assert.ok(P.s.result && /tools-big/.test(P.body()) && /tools-sector/.test(P.body()) && !/Clear = swell/.test(P.body()), P.body());
  assert.match(P.body(), /Moved [\d.]+ mi off the shore/, 'a status line stays (G21 re-check R1-5)');
});

test('Lock: Escape only unlocks; Unlock clears a tapped readout and holds double-click again; a locked mouse click is not pinned; focus after Clear (G21 B-9, A-13, B-11)', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const lock = E.q('[data-act="lock"]');
  lock.dispatch('click');
  E.clickAt(22.5, -160, { pointerType: 'mouse' });
  assert.ok(E.s.readout && !E.s.pinned, 'a mouse click while locked: a readout that the pointer moves on');
  E.mapEl.dispatch('mouseleave', {});
  assert.ok(!E.s.readout, 'and leaving ends it');
  assert.match(E.body(), /Point at the map for bearing and distance\./, 'the locked hint');
  E.clickAt(22.5, -160, { pointerType: 'touch' });
  assert.ok(E.s.pinned && cursorLineOf(E));
  const sector = () => E.q('.tools-bar-body').children.find((x) => x.className === 'tools-sector');
  assert.equal(sector().getAttribute('aria-live'), 'polite', 'a tapped readout is announced');
  E.mapEl.dispatch('mousemove', { clientX: 100, clientY: 100 });
  assert.equal(sector().getAttribute('aria-live'), 'off', 'a hovered one is not (G21 B-11)');
  E.clickAt(22.5, -160, { pointerType: 'touch' });
  E.key('Escape', lock);
  assert.ok(E.s.result && !E.s.locked, 'Escape on the Lock button only unlocks');
  assert.ok(!E.s.readout && !cursorLineOf(E) && E.s.readoutAt === null && E.s.pinned === false, 'the tapped readout goes with the lock');
  assert.ok(!E.map.doubleClickZoom.enabled() && E.A.active(), 'the tool holds double-click zoom again');
  assert.equal(E.doc.activeElement, lock, 'focus stays on the button');
  lock.dispatch('click');
  assert.ok(E.s.locked && !E.mapEl.classList.contains('tools-active') && E.map.doubleClickZoom.enabled());
  E.q('[data-act="clear"]').dispatch('click');
  assert.ok(!E.s.locked && E.mapEl.classList.contains('tools-active') && !E.map.doubleClickZoom.enabled(), 'Clear unlocks: the crosshair and the double-click hold are back');
  assert.ok(!E.s.result && E.doc.activeElement === E.q('.tools-x'), 'after Clear, focus on the bar\'s close button');
});

test('touch guard: touchend counts, the clock is monotonic (a wall clock set back does not block a mouse) (G21 A-15)', async () => {
  const clock = { t: 5000 };
  const E = makeEnv({ clock }); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]), water = { clientX: c.x - 300, clientY: c.y - 200 };
  E.mapEl.dispatch('touchend', {});
  E.mapEl.dispatch('mousemove', water);
  assert.ok(!E.s.readout, 'a touchend within a second');
  const now = Date.now; Date.now = () => now() - 3600e3;                     // the wall clock goes back an hour
  try {
    clock.t += 1500;
    E.mapEl.dispatch('mousemove', water);
    assert.ok(E.s.readout, 'a mouse 1.5 s later, whatever the wall clock says');
  } finally { Date.now = now; }
});

test('the pointer over a control changes nothing, even above the fan; a hovered wedge\'s outline goes with a readout (G21 B-15)', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  const ctl = E.doc.createElement('div'); ctl.classList.add('leaflet-control');
  E.mapEl.dispatch('mousemove', { clientX: c.x, clientY: c.y + 60, composedPath: () => [ctl, E.mapEl] });
  assert.ok(E.s.selected === -1 && !E.s.readout, 'over a control on the fan: no wedge');
  E.mapEl.dispatch('mousemove', { clientX: c.x, clientY: c.y + 60 });
  const k = E.s.selected, path = () => E.fan()._el.children.find((x) => x.getAttribute('data-k') === String(k));
  assert.ok(k >= 0 && path().getAttribute('stroke') === '#fde047', 'the wedge, outlined');
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200, composedPath: () => [ctl, E.mapEl] });
  assert.ok(E.s.selected === k && !E.s.readout, 'over a control off the fan: unchanged');
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });
  assert.ok(E.s.readout && E.s.selected === -1 && path().getAttribute('stroke') === 'none', 'a readout: the outline goes');
  assert.ok([...E.layers].some((l) => l.kind === 'polyline' && l.o.color === '#0b2536' && l.o.weight > 3), 'the line has its dark casing');
});

test('the compass: switched at 5.75 / 6.25 exactly by the dead band, the fan redrawn at the new size; a resize keeps a compass where it is', async () => {
  const E = makeEnv({ zoom: 8 }); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const full = E.fan();
  assert.equal(full.o.icon.iconSize[0] > 200, true);
  E.view.zoom = 5.75; E.map.fire('zoomend');
  assert.ok(!E.s.compass && E.fan() === full, '5.75 is inside the dead band');
  E.view.zoom = 5.7; E.map.fire('zoomend');
  const small = E.fan();
  assert.ok(E.s.compass && small !== full && small.o.icon.iconSize[0] < 150 && !E.layers.has(full), 'a compass, redrawn: ' + small.o.icon.iconSize[0]);
  E.view.zoom = 6.2; E.map.fire('zoomend');
  assert.ok(E.s.compass && E.fan() === small);
  const pans = E.map.pans.length;
  E.mapEl.rect = Object.assign({}, E.mapEl.rect, { width: 900 }); E.win.fire('resize'); await wait(200);
  assert.ok(E.s.compass && E.s.radius === 44 && E.map.pans.length === pans, 'a resize: not moved, not grown');
  E.view.zoom = 6.25; E.map.fire('zoomend');
  assert.ok(!E.s.compass && E.fan().o.icon.iconSize[0] > 200, '6.25: the full fan');
});

test('a failure while drawing the window is logged and said in the bar; the fan stands (G21 A-4)', async () => {
  const logged = [];
  const E = makeEnv({ console: { error: (e) => logged.push(e) } }); E.pick('exposure');
  const poly = E.win.L.polygon; E.win.L.polygon = () => { throw new Error('boom'); };
  E.clickAt(21.6655, -158.054); await E.settle();
  for (let i = 0; i < 600 && !E.s.reachFailed; i++) await wait(5);          // the world index is built in slices first
  assert.ok(E.s.result && E.fan() && !E.s.reachOn, 'the fan, no window');
  assert.ok(logged.length === 1 && /boom/.test(logged[0].message), 'logged');
  assert.match(E.body(), /The window could not be drawn on the map/);
  E.win.L.polygon = poly;
  E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  assert.ok(E.s.reachOn && !/could not be drawn/.test(E.body()), 'the next spot draws');
});

test('a result that lands after the view moved a world away goes to the copy in view (G21 B-8)', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054);
  E.view.lng0 += 720;                                                       // panned two worlds east while computing (no moveend yet)
  await E.settle(); await reachDrawn(E);
  assert.ok(Math.abs(E.s.fanLng - E.s.result.origin.lng - 720) < 0.01, 'fan at ' + E.s.fanLng);
});

test('an enclosed sea gets rings at a finer step, as arcs inside it (G21 B-2, owner: arcs inside the window)', async () => {
  const land = [[sq(-6, -6, 6, -4)], [sq(-6, 4, 6, 6)], [sq(-6, -4, -4, 4)], [sq(4, -4, 6, 4)]];      // a basin about 900 km across
  const E = makeEnv({ lat0: 1.5, lng0: -1.25, scale: 100, cells: {}, files: { '/world-i.bin': encodeCoast(land, 30) } });
  E.pick('exposure'); E.clickAt(0, 0); await E.settle(); await reachDrawn(E);
  const labels = [...E.s.reach._items].filter((l) => l.kind === 'label').map((l) => /<span>(.*)<\/span>/.exec(l.o.icon.html)[1]);
  assert.ok(labels.includes('100 nm') && labels.includes('200 nm') && !labels.some((x) => /1,000/.test(x)), labels.join());
  const A = E.A._internals, o = E.s.result.origin;
  [...E.s.reach._items].filter((l) => l.kind === 'polyline' && l.o.dashArray === '4 6').forEach((l) => l.a.forEach((p) => {
    assert.ok(A.distanceKm(o, { lat: p[0], lng: p[1] - Math.round(p[1] / 360) * 360 }) < 600, 'inside the basin');
  }));
});

// ---- G21 re-check (R1) ----
test('a details line taller than its three lines is left out of the height the page sees: hovering never moves or minimises a window (R1-1)', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]), base = E.bar.offsetHeight, n = E.layouts.length;
  const line = E.q('.tools-bar-body').children.find((x) => x.className === 'tools-sector'); line.classList.add('tools-sector');
  E.win.getComputedStyle = () => ({ minHeight: '61.2px' });
  line.offsetHeight = 83; E.bar._h = base + 21;                              // a four-line readout
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });
  assert.equal(E.layouts.length, n, 'the page is not told: no window moves or is minimised');
  line.offsetHeight = 62; E.bar._h = base + 40;                              // something else grew (a new line)
  E.mapEl.dispatch('mousemove', { clientX: c.x - 310, clientY: c.y - 200 });
  assert.deepEqual(E.layouts.slice(n), [true], 'that is reported');
});

test('the window\'s canvas: padding capped by its size in pixels, freed when the tool closes; the readout\'s line stays on top of a redraw (R1-2, R1-3)', async () => {
  const E = makeEnv({ canvas: true }); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const veil = () => [...E.s.reach._items].find((l) => l.kind === 'polygon' && l.a.length === 4), rend = veil().o.renderer;
  assert.equal(rend.options.padding, 0.5, 'a 1000 x 700 map: the full half map');
  E.win.devicePixelRatio = 2; E.mapEl.rect = Object.assign({}, E.mapEl.rect, { width: 2000, height: 1400 });
  E.unitSel.value = 'Metric'; E.unitSel.dispatch('change');                 // a redraw at the new size
  const m = 2, px = (w, h, p) => (w * (1 + 2 * p)) * (h * (1 + 2 * p)) * m * m;
  assert.ok(rend.options.padding === 0.1 && px(2000, 1400, 0.1) < 16.7e6, 'a big retina map: the default tenth');
  E.mapEl.rect = Object.assign({}, E.mapEl.rect, { width: 1024, height: 1300 });
  E.unitSel.value = 'US'; E.unitSel.dispatch('change');
  assert.ok(rend.options.padding > 0.1 && rend.options.padding < 0.5 && px(1024, 1300, rend.options.padding) <= 12e6 + 1, 'a 12.9-inch iPad: under iOS Safari\'s limit, ' + rend.options.padding);
  // the readout's line on top after a redraw
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });
  E.unitSel.value = 'Metric'; E.unitSel.dispatch('change');
  const line = [...E.layers].find((l) => l.kind === 'polyline' && l.o.dashArray === '2 4'), casing = [...E.layers].find((l) => l.kind === 'polyline' && l.o.color === '#0b2536');
  assert.ok(casing.front > 0 && line.front > casing.front, 'casing, then the line, brought to the front');
  // closing frees the canvas (Leaflet adds the renderer to the map with the first path)
  E.layers.add(rend);
  E.q('.tools-x').dispatch('click');
  assert.ok(!E.layers.has(rend), 'removed with the tool');
  assert.equal(E.doc.activeElement, E.btn, 'focus back on the tools button (R1-10)');
});

test('folded, the bar still says why there is no result and that the map is locked (R1-4)', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  E.q('.tools-fold').dispatch('click');
  E.q('[data-act="lock"]').dispatch('click');
  assert.equal(E.q('.tools-bar-title').textContent, 'Swell exposure · locked');
  E.q('[data-act="lock"]').dispatch('click');
  assert.equal(E.q('.tools-bar-title').textContent, 'Swell exposure');
  E.clickAt(80, -150);                                                       // beyond 75 degrees
  assert.match(E.body(), /between 75°S and 75°N/, 'the message shows under the folded title');
});

test('compact on a short touch map (a phone on its side), and after a resize to a phone (R1-6); Escape-clear keeps focus in the bar (R1-8)', async () => {
  const L1 = makeEnv({ touch: true, mapW: 812, mapH: 322, barRect: { left: 515, top: 50, width: 290, height: 186 }, lat0: 22.3, lng0: -159.2 });
  L1.pick('exposure'); L1.clickAt(21.6655, -158.054); await L1.settle();
  assert.ok(L1.s.result && !/Clear = swell/.test(L1.body()), L1.body());
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  assert.match(E.body(), /Clear = swell/);
  E.mapEl.rect = Object.assign({}, E.mapEl.rect, { width: 375 }); E.win.fire('resize'); await wait(200);
  assert.ok(!/Clear = swell/.test(E.body()), 'the resize re-renders the bar');
  E.mapEl.rect = Object.assign({}, E.mapEl.rect, { width: 1000 }); E.win.fire('resize'); await wait(200);
  assert.match(E.body(), /Clear = swell/, 'and back');
  const clr = E.q('[data-act="clear"]');
  E.key('Escape', clr);
  assert.ok(!E.s.result && E.doc.activeElement === E.q('.tools-x'), 'Escape cleared, focus on the close button');
});

test('hover: over a control the pointer is forgotten (a later view change does not move the readout); leaving the map lets the line speak (R1-7)', async () => {
  const E = makeEnv(); E.pick('exposure'); E.clickAt(21.6655, -158.054); await E.settle(); await reachDrawn(E);
  const c = E.map.latLngToContainerPoint([E.s.result.origin.lat, E.s.fanLng]);
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });
  const before = cursorLineOf(E).a.slice(-1)[0];
  const ctl = E.doc.createElement('div'); ctl.classList.add('leaflet-control');
  E.mapEl.dispatch('mousemove', { clientX: 900, clientY: 100, composedPath: () => [ctl, E.mapEl] });
  E.view.lng0 += 1; E.map.fire('moveend');
  assert.deepEqual(cursorLineOf(E).a.slice(-1)[0], before, 'the readout stays where it was');
  E.view.lng0 -= 1;
  const sector = () => E.q('.tools-bar-body').children.find((x) => x.className === 'tools-sector');
  assert.equal(sector().getAttribute('aria-live'), 'off');
  E.mapEl.dispatch('mouseleave', {});
  assert.equal(sector().getAttribute('aria-live'), 'polite', 'after leaving the map the line speaks again');
  E.q('[data-act="lock"]').dispatch('click');
  assert.equal(sector().getAttribute('aria-live'), 'polite', 'the locked hint is announced');
  E.q('[data-act="lock"]').dispatch('click');
  E.mapEl.dispatch('mousemove', { clientX: c.x - 300, clientY: c.y - 200 });
  assert.equal(sector().getAttribute('aria-live'), 'off', 'hovering again: silent');
  E.q('[data-act="lock"]').dispatch('click');                                  // Lock from the keyboard, the pointer still on the map
  assert.equal(sector().getAttribute('aria-live'), 'polite', 'a Lock is announced without leaving the map');
});

// ---- the Forecast point tool (plan section 31) ----
test('Forecast point: one click hands the point to the page; a page that answers at once ends the tool (the markers have their clicks back)', () => {
  const E = makeEnv();
  assert.equal(E.menu.querySelector('[data-tool="point"]').hidden, false);
  E.pick('point');
  assert.equal(E.q('.tools-bar-title').textContent, 'Forecast point');
  assert.match(E.body(), /Click the sea where you want a forecast\./);
  assert.match(E.body(), /kept in this browser under My points/);
  assert.equal(E.A.active(), true); assert.equal(E.map.doubleClickZoom.enabled(), false);
  assert.equal(E.q('.tools-bar-actions').hidden, true, 'no Undo / Finish / Clear / Lock');
  E.clickAt(21.35, -518.6);                                                  // a click in another world copy
  assert.equal(E.points.length, 1);
  assert.equal(E.points[0].ll.lat, 21.35);
  assert.ok(Math.abs(E.points[0].ll.lng - -158.6) < 1e-9, 'the longitude comes back to -180..180');
  assert.equal(E.bar.hidden, true); assert.equal(E.s.tool, null, 'no promise from the page: the tool ends');
  assert.equal(E.map.doubleClickZoom.enabled(), true, 'double-click zoom is back');
  E.clickAt(20, -158);
  assert.equal(E.points.length, 1, 'the next map click belongs to the map again');
  assert.equal(E.menu.querySelector('[data-tool="point"]').getAttribute('aria-pressed'), 'false');
});

test('Forecast point: Escape and the close button end it without a point; a touch says Tap; without onPoint the item is hidden', () => {
  const E = makeEnv();
  E.pick('point'); E.key('Escape');
  assert.equal(E.s.tool, null); assert.equal(E.points.length, 0);
  E.pick('point'); E.q('.tools-x').dispatch('click');
  assert.equal(E.s.tool, null); assert.equal(E.points.length, 0);
  const T = makeEnv({ touch: true }); T.pick('point');
  assert.match(T.body(), /^<div class="tools-big">Tap the sea where you want a forecast\.<\/div>/);
  const N = makeEnv({ noPoint: true });
  assert.equal(N.menu.querySelector('[data-tool="point"]').hidden, true);
});

// G22 (owner: land is refused): the page asks the server first; the tool waits, says a refusal and stays on
test('Forecast point: the tool waits for the answer of the page; a refusal is said in the bar and the tool stays on; a forecast ends it, then opens', async () => {
  let resolve = null, reject = null, opened = [];
  const E = makeEnv({ answer: () => new Promise((res, rej) => { resolve = res; reject = rej; }) });
  E.pick('point');
  E.clickAt(21.5, -158.0);
  assert.equal(E.points.length, 1); assert.equal(E.s.busy, true);
  assert.match(E.body(), /Checking that point/);
  E.clickAt(21.7, -158.2);
  assert.equal(E.points.length, 1, 'a click while one is being checked is ignored');
  resolve({ ok: false, message: 'That point is on land or inland water. Pick a point on the sea.' }); await E.settle();
  assert.equal(E.s.tool, 'point', 'the tool stays on'); assert.equal(E.s.busy, false); assert.equal(E.bar.hidden, false);
  assert.match(E.body(), /^<div class="tools-big tools-msg">That point is on land or inland water\. Pick a point on the sea\.<\/div><div class="tools-hint">Click another point on the sea\.<\/div>$/);
  E.clickAt(21.7, -158.2);                                                     // another click: asked again
  assert.equal(E.points.length, 2);
  reject(new Error('offline')); await E.settle();
  assert.equal(E.s.tool, 'point'); assert.match(E.body(), /Could not reach the server/);
  E.clickAt(21.71, -158.21);
  resolve({ ok: true, open: () => opened.push({ active: E.A.active(), dbl: E.map.doubleClickZoom.enabled() }) }); await E.settle();
  assert.equal(E.s.tool, null); assert.equal(E.bar.hidden, true);
  assert.deepEqual(opened, [{ active: false, dbl: true }], 'the tool had ended when the point opened');
  assert.equal(E.s.msg, '');
});

test('Forecast point: closed or switched while a point is being checked: nothing opens, nothing is said', async () => {
  let resolve = null, opened = 0;
  const E = makeEnv({ answer: () => new Promise((res) => { resolve = res; }) });
  E.pick('point'); E.clickAt(21.5, -158.0);
  E.q('.tools-x').dispatch('click');
  resolve({ ok: true, open: () => { opened++; } }); await E.settle();
  assert.equal(opened, 0); assert.equal(E.s.tool, null);
  E.pick('point'); E.clickAt(21.5, -158.0);
  E.pick('distance');
  resolve({ ok: false, message: 'land' }); await E.settle();
  assert.equal(E.s.tool, 'distance'); assert.doesNotMatch(E.body(), /land/);
  E.pick('point'); E.clickAt(21.5, -158.0);
  E.pick('point');                                                             // started again: the old answer is stale
  resolve({ ok: true, open: () => { opened++; } }); await E.settle();
  assert.equal(opened, 0); assert.equal(E.s.tool, 'point'); assert.equal(E.s.busy, false);
});

test('Forecast point: Escape that ends the tool gives the focus back to the tools button (G22 K-4); a minimised window bar lets Escape reach the tool (B-13)', () => {
  const E = makeEnv();
  E.pick('point');
  E.key('Escape');
  assert.equal(E.s.tool, null); assert.equal(E.doc.activeElement, E.btn);
  E.pick('point');
  const bar = E.doc.createElement('section'); bar.classList.add('fwin'); bar.classList.add('fw-min');
  const head = E.doc.createElement('button'); bar.appendChild(head); E.doc.body.appendChild(bar);
  head.focus();
  E.key('Escape');
  assert.equal(E.s.tool, null, 'Escape on a minimised window bar ends the tool');
});

test('Forecast point: a click that began on a control is not a point; another tool replaces it', () => {
  const E = makeEnv();
  E.pick('point');
  const ctl = E.doc.createElement('div'); ctl.classList.add('leaflet-control');
  E.clickAt(20, -158, { pointerType: 'mouse', target: ctl, composedPath: () => [ctl, E.mapEl] });
  const win = E.doc.createElement('section'); win.classList.add('fwin');
  E.clickAt(20, -158, { pointerType: 'mouse', target: win, composedPath: () => [win] });
  assert.equal(E.points.length, 0, 'a click on a control or a window is not a point');
  assert.equal(E.s.tool, 'point', 'and the tool waits on');
  E.pick('distance');
  E.clickAt(21.3, -157.9);
  assert.equal(E.points.length, 0); assert.equal(E.s.pts.length, 1, 'the distance tool has the click');
});
