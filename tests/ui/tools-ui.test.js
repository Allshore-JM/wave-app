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
  const doc = win.document;
  const el = (tag, id, parent, attrs) => { const e = doc.createElement(tag); if (id) doc.register(e, id); if (parent) parent.appendChild(e); Object.entries(attrs || {}).forEach(([k, v]) => e.setAttribute(k, v)); return e; };
  const host = el('div', 'toolsHost', doc.body);
  const btn = el('button', 'toolsBtn', host);
  const menu = el('div', 'toolsMenu', host); menu.hidden = true;
  ['distance', 'area', 'exposure'].forEach((t) => el('button', null, menu, { 'data-tool': t }));
  const unitSel = el('select', 'unit', doc.body); ['US', 'Metric'].forEach((v) => { const o = el('option', null, unitSel); o.value = v; o.textContent = v; }); unitSel.value = 'US';
  const outside = el('input', 'station-search', doc.body);
  const mapEl = el('div', 'map', doc.body); mapEl.rect = { left: 0, top: 0, width: opts.mapW || 1000, height: opts.mapH || 700 };
  const handlers = {};
  let dbl = true;
  const view = { lat0: opts.lat0 == null ? 22 : opts.lat0, lng0: opts.lng0 == null ? -159 : opts.lng0, scale: opts.scale || 400 };   // px per degree
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
    getSize() { return { x: mapEl.rect.width, y: mapEl.rect.height }; },
    removeLayer(l) { layers.delete(l); },
    addControl(c) {
      const box = c.onAdd(this); box.classList.add('leaflet-control'); c._container = box; mapEl.appendChild(box);
      if (box.classList.contains('tools-bar')) {                             // the bar's innerHTML as elements (fakedom does not parse HTML)
        const head = el('div', null, box); const title = el('strong', null, head); title.classList.add('tools-bar-title');
        const x = el('button', null, head); x.classList.add('tools-x');
        const body = el('div', null, box); body.classList.add('tools-bar-body');
        const acts = el('div', null, box); ['undo', 'finish', 'clear'].forEach((a) => el('button', null, acts, { 'data-act': a }));
        box.rect = opts.barRect || { left: 700, top: 60, width: 290, height: 200 };
      }
      return this;
    },
    panBy(d) { this.pans.push(d); }
  };
  function layer(kind, a, o) { return { kind, a, o, addTo(t) { (t._items || layers).add(this); return this; }, bindTooltip(txt) { this.tip = txt; return this; }, getElement() { return this._el; } }; }
  win.L = {
    featureGroup() { return { _items: new Set(), addTo() { return this; }, clearLayers() { this._items.clear(); }, add(x) { this._items.add(x); } }; },
    Control: { extend(proto) { function C() { this.options = proto.options; } C.prototype.onAdd = proto.onAdd; C.prototype.getContainer = function () { return this._container; }; return C; } },
    DomUtil: { create(tag, cls) { const e = doc.createElement(tag); cls.split(' ').forEach((c) => e.classList.add(c)); return e; } },
    DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} },
    polygon(a, o) { return layer('polygon', a, o); }, polyline(a, o) { return layer('polyline', a, o); }, circleMarker(a, o) { return layer('circle', a, o); },
    divIcon(o) { return o; },
    marker(ll, o) { const m = layer('fan', ll, o); m._el = doc.createElement('div'); m.addTo = function () { layers.add(this); return this; }; return m; }
  };
  const idx = { format: 'coast-v1', tier0: { max_zoom: 6 }, tier1: { cell: 5, dir: 'f', cells: { '20_-160': [4531, 2018] } } };
  const fetches = [];
  const fetchFn = async (url) => {
    fetches.push(url);
    const ok = (b) => ({ ok: true, json: async () => b, arrayBuffer: async () => b });
    if (url.endsWith('/index.json')) return ok(idx);
    if (url.endsWith('/world-i.bin')) return ok(FIX('hawaii-t0.bin'));
    if (url.endsWith('/f/20_-160.bin')) return ok(FIX('oahu-t1.bin'));
    return { ok: false, status: 404 };
  };
  new Function('window', SRC)(win);
  const layouts = [], starts = [];
  const api = win.AllshoreTools.init({ map, coastBase: 'https://c', fetch: fetchFn, getUnit: () => unitSel.value, unitSelect: unitSel,
    onLayout: () => layouts.push(1), onStart: (bar) => starts.push(bar), obstacles: () => opts.obstacles || [] });
  const bar = mapEl.children.find((c) => c.classList.contains('tools-bar'));
  const q = (sel) => bar.querySelector(sel);
  const body = () => q('.tools-bar-body').innerHTML;
  function key(k, focus) { doc.activeElement = focus || doc.body; return doc.fire('keydown', { key: k, target: focus || doc.body, _stop: false, stopPropagation() { this._stop = true; } }); }
  function clickAt(lat, lng, ev) { map.fire('click', { latlng: { lat, lng }, originalEvent: ev || { pointerType: 'mouse' } }); }
  function pick(tool) { btn.dispatch('click'); menu.querySelector('[data-tool="' + tool + '"]').dispatch('click'); }
  async function settle() { for (let i = 0; i < 400 && api.state.busy; i++) await new Promise((r) => setTimeout(r, 5)); await new Promise((r) => setTimeout(r, 5)); }
  return { win, doc, map, api, s: api.state, A: win.AllshoreTools, menu, btn, bar, q, body, unitSel, outside, key, clickAt, pick, settle, layouts, starts, fetches, view, mapEl, layers };
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
  assert.equal(E.starts.length, 1, 'the page is told (it minimises a window over the bar)');
  E.q('.tools-x').dispatch('click');
  assert.equal(E.A.active(), false); assert.equal(E.bar.hidden, true); assert.equal(E.map.doubleClickZoom.enabled(), true);
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
  assert.match(E.body(), /Open: W \(250°–275°\), N \(295°–045°\)/);
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
  assert.match(E.body(), /Open: SSW \(145°–275°\)/, 'Waikiki, not Pipeline');
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
