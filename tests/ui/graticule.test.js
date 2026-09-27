'use strict';
// static_ui/graticule.js: the interval rule, the tile line positions (seams, world copies, poles), the labels and
// the setting. Run by tests/test_ui_module.py and CI.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'graticule.js'), 'utf8');
function load(win) { new Function('window', SRC)(win); return win.AllshoreGraticule; }
const I = load({})._internals;

test('the interval: the finest step whose lines stay at least MIN_PX apart at the tile zoom', () => {
  const got = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map(I.stepFor);
  assert.deepEqual(got, [60, 30, 15, 10, 5, 5, 2, 2, 2, 2, 2], '5 degrees through zoom 6, 2 degrees from zoom 7 in (owner)');
  for (let z = 1; z <= 5; z++) {
    const px = I.stepFor(z) / 360 * 256 * 2 ** z;
    assert.ok(px >= I.MIN_PX && px < I.MIN_PX * 2, 'z' + z + ': ' + px);
  }
  assert.equal(I.stepFor(0), 60, 'coarser than any step fits: the coarsest');
  I.STEPS.forEach((s) => assert.equal((360 / s) % 1, 0, s + ' divides 360'));
});

test('Mercator helpers agree with each other (and Leaflet: the equator at half the world)', () => {
  assert.equal(I.latToY(0, 3), 1024);
  for (const lat of [-80, -45, -0.25, 0, 21.3, 60, 84]) assert.ok(Math.abs(I.yToLat(I.latToY(lat, 5), 5) - lat) < 1e-9, String(lat));
  assert.equal(I.lonToX(-180, 4), 0); assert.equal(I.xToLon(I.lonToX(157.5, 4), 4), 157.5);
});

// every line pixel a tile row would draw (the core: floor(px) inside [0, 256))
function cores(coordsList, step) {
  const xs = new Set();
  coordsList.forEach((c) => I.tileLines(c, step, 3).xs.forEach((l) => { const p = Math.floor(l.px); if (p >= 0 && p < 256) xs.add(c.x * 256 + p); }));
  return xs;
}

test('adjacent tiles join: each meridian is drawn in exactly one tile, at its world pixel; neighbours bring the halos across the seam', () => {
  const z = 3, step = I.stepFor(z), row = [0, 1, 2, 3, 4, 5, 6, 7].map((x) => ({ x, y: 3, z }));
  const seen = [];
  row.forEach((c) => I.tileLines(c, step, 3).xs.forEach((l) => { const p = Math.floor(l.px); if (p >= 0 && p < 256) seen.push(c.x * 256 + p); }));
  assert.equal(seen.length, 360 / step, 'one core per meridian around the world');
  assert.equal(new Set(seen).size, seen.length, 'never twice');
  seen.forEach((wx) => { const lon = I.xToLon(wx, z); assert.ok(Math.abs(lon / step - Math.round(lon / step)) * step * (256 * 8 / 360) < 1, String(lon)); });
  // the -180 meridian sits on tile 0's left edge: tile 0 draws it, tile -1 only brings its halo
  const left = I.tileLines({ x: -1, y: 3, z }, step, 3).xs.map((l) => l.px);
  assert.ok(left.some((p) => p >= 256 && p < 259), 'the neighbour sees the line within its margin');
});

test('world copies and the dateline: a tile one world to the west or east draws exactly what the wrapped tile draws', () => {
  for (const z of [2, 3, 5, 8]) {
    const n = 2 ** z, step = I.stepFor(z);
    for (const x of [0, 1, n - 1]) {
      const base = I.tileLines({ x, y: n / 2, z }, step, 3), west = I.tileLines({ x: x - n, y: n / 2, z }, step, 3), east = I.tileLines({ x: x + n, y: n / 2, z }, step, 3);
      assert.deepEqual(west.xs.map((l) => +l.px.toFixed(6)), base.xs.map((l) => +l.px.toFixed(6)), 'z' + z + ' x' + x + ' west');
      assert.deepEqual(east.xs.map((l) => +l.px.toFixed(6)), base.xs.map((l) => +l.px.toFixed(6)), 'z' + z + ' x' + x + ' east');
      assert.deepEqual(west.xs.map((l) => l.major), base.xs.map((l) => l.major), 'the prime meridian stays major in every copy');
    }
  }
});

test('parallels: at their Mercator row, the equator major, nothing beyond the Mercator edge in the polar tiles', () => {
  const z = 4, step = I.stepFor(z);
  const eq = I.tileLines({ x: 0, y: 8, z }, step, 0).ys.find((l) => l.lat === 0);
  assert.ok(eq && eq.major && eq.px === 0, 'the equator on the top edge of tile row 8');
  const two = (ya, yb) => ({ ys: I.tileLines({ x: 0, y: ya, z }, step, 3).ys.concat(I.tileLines({ x: 0, y: yb, z }, step, 3).ys) });
  const north = two(0, 1), south = two(14, 15);                      // the two polar tile rows (85.05 to ~79 deg)
  assert.equal(I.tileLines({ x: 0, y: 0, z }, step, 3).ys.length, 0, 'the top row (85.05-82.7) holds no 10-degree parallel');
  assert.ok(north.ys.length && south.ys.length);
  north.ys.concat(south.ys).forEach((l) => { assert.ok(Math.abs(l.lat) < 85.06, String(l.lat)); assert.ok(l.px > -4 && l.px < 260); });
  assert.ok(north.ys.some((l) => l.lat === 80) && south.ys.some((l) => l.lat === -80));
  const deep = I.tileLines({ x: 3, y: 900, z: 11 }, I.stepFor(11), 3);
  deep.ys.forEach((l) => assert.equal((l.lat / 2) % 1, 0, 'zoomed in: 2-degree parallels at most: ' + l.lat));
});

test('labels read like a chart: W/E/N/S, 0° and 180°, quarter degrees trimmed, any world copy normalised', () => {
  assert.equal(I.lonLabel(-157.5), '157.5°W'); assert.equal(I.lonLabel(0), '0°'); assert.equal(I.lonLabel(180), '180°'); assert.equal(I.lonLabel(-180), '180°');
  assert.equal(I.lonLabel(200), '160°W'); assert.equal(I.lonLabel(-200), '160°E'); assert.equal(I.lonLabel(-360), '0°'); assert.equal(I.lonLabel(30.25), '30.25°E');
  assert.equal(I.latLabel(0), '0°'); assert.equal(I.latLabel(-0.25), '0.25°S'); assert.equal(I.latLabel(21), '21°N'); assert.equal(I.latLabel(-78.5), '78.5°S');
});

function fakeMap(o) {
  const z = o.z, cx = I.lonToX(o.lng, z), cy = I.latToY(o.lat, z), w = o.w, h = o.h;
  const west = I.xToLon(cx - w / 2, z), east = I.xToLon(cx + w / 2, z), north = I.yToLat(cy - h / 2, z), south = I.yToLat(cy + h / 2, z);
  return {
    getZoom: () => z, getSize: () => ({ x: w, y: h }),
    getBounds: () => ({ getWest: () => west, getEast: () => east, getNorth: () => north, getSouth: () => south }),
    latLngToContainerPoint: (ll) => ({ x: I.lonToX(ll[1], z) - (cx - w / 2), y: I.latToY(ll[0], z) - (cy - h / 2) })
  };
}

test('labelsFor: every meridian on screen along the top (world copies included), every parallel along the right edge', () => {
  const m = fakeMap({ z: 3, lat: 20, lng: -170, w: 1500, h: 800 });
  const ls = I.labelsFor(m, 15), lon = ls.filter((l) => l.kind === 'lon'), lat = ls.filter((l) => l.kind === 'lat');
  assert.ok(lon.some((l) => l.text === '180°') && lon.some((l) => l.text === '165°E') && lon.some((l) => l.text === '150°W'), 'across the dateline');
  lon.forEach((l) => { assert.ok(l.x >= 0 && l.x <= 1500); assert.equal(l.y, 0); });
  const gaps = lon.map((l) => l.x).sort((a, b) => a - b).slice(1).map((x, i, a) => x - (i ? a[i - 1] : lon.map((l) => l.x).sort((p, q) => p - q)[0]));
  gaps.forEach((g) => assert.ok(Math.abs(g - 85.33) < 0.01, 'evenly spaced: ' + g));
  assert.ok(lat.some((l) => l.text === '0°') && lat.some((l) => l.text === '30°N'));
  lat.forEach((l) => { assert.equal(l.x, 1500); assert.ok(l.y >= 0 && l.y <= 800); });
  const wide = I.labelsFor(fakeMap({ z: 2, lat: 0, lng: 0, w: 1400, h: 700 }), 30).filter((l) => l.kind === 'lon');
  assert.equal(new Set(wide.map((l) => l.text)).size < wide.length, true, 'a view wider than a world labels each copy');
});

test('the setting: on unless stored "0"; blocked storage reads on and never throws', () => {
  const mem = new Map(), st = { getItem: (k) => (mem.has(k) ? mem.get(k) : null), setItem: (k, v) => mem.set(k, v) };
  assert.equal(I.readOn(st), true);
  I.writeOn(st, false); assert.equal(mem.get(I.KEY), '0'); assert.equal(I.readOn(st), false);
  I.writeOn(st, true); assert.equal(I.readOn(st), true);
  const bad = { getItem() { throw new Error('blocked'); }, setItem() { throw new Error('blocked'); } };
  assert.equal(I.readOn(bad), true); I.writeOn(bad, false);
});

// a minimal Leaflet for create(): GridLayer.extend, a map with panes, layers and events, a DOM with rects
function harness(opts) {
  const events = {}, layers = new Set(), panes = {};
  function el(tag) {
    const e = { tagName: tag, style: {}, children: [], hidden: false, className: '', textContent: '', offsetWidth: 0, offsetHeight: 0, attrs: {},
      appendChild(c) { this.children.push(c); c.parentNode = this; return c; }, setAttribute(k, v) { this.attrs[k] = v; },
      getContext: () => null, getBoundingClientRect: () => ({ left: 0, top: 0, right: 0, bottom: 0 }), querySelectorAll: () => [] };
    return e;
  }
  const doc = { createElement: el };
  const container = el('div');
  const ctl = { getBoundingClientRect: () => ({ left: 1300, top: 0, right: 1500, bottom: 60 }) };
  container.querySelectorAll = (sel) => (sel === '.leaflet-control' ? (opts && opts.controls === false ? [] : [ctl]) : []);
  container.getBoundingClientRect = () => ({ left: 0, top: 0, right: 1500, bottom: 800 });
  const fm = fakeMap({ z: 3, lat: 20, lng: -170, w: 1500, h: 800 });
  const map = Object.assign(fm, {
    getPane: (n) => panes[n], createPane: (n) => (panes[n] = el('div')), getContainer: () => container,
    on: (evs, fn) => evs.split(' ').forEach((e) => (events[e] = events[e] || []).push(fn)),
    hasLayer: (l) => layers.has(l), removeLayer: (l) => layers.delete(l), fire: (e) => (events[e] || []).forEach((f) => f())
  });
  const L = { GridLayer: { extend: (proto) => { function C(o) { this.options = o; } Object.assign(C.prototype, proto, { addTo(m) { layers.add(this); return this; } }); return C; } } };
  const mem = new Map(); if (opts && opts.stored !== undefined) mem.set(I.KEY, opts.stored);
  const storage = { getItem: (k) => (mem.has(k) ? mem.get(k) : null), setItem: (k, v) => mem.set(k, v) };
  const win = { requestAnimationFrame: (f) => { f(); return 1; }, devicePixelRatio: 2 };
  const G = load(win).create(map, { L, document: doc, storage });
  return { G, map, layers, panes, mem, container };
}

test('create(): its own pane under the markers, on by default, labels hidden under a control, set() toggles and remembers', () => {
  const h = harness();
  assert.equal(h.panes.graticulePane.style.zIndex, 350); assert.equal(h.panes.graticulePane.style.pointerEvents, 'none');
  assert.equal(h.G.on(), true); assert.ok(h.layers.has(h.G.layer));
  assert.equal(h.G.layer.options.pane, 'graticulePane');
  const spans = h.G.labels.children;
  assert.ok(spans.length > 10);
  const under = spans.filter((s) => s.className.includes('graticule-lon') && parseFloat(s.style.left) > 1296 && !s.hidden);
  assert.equal(under.length, 0, 'no longitude label under the top-right control');
  assert.ok(spans.some((s) => s.className.includes('graticule-lon') && !s.hidden));
  h.G.set(false);
  assert.equal(h.layers.has(h.G.layer), false); assert.equal(h.mem.get(I.KEY), '0'); assert.equal(h.G.labels.hidden, true);
  h.G.set(true); assert.ok(h.layers.has(h.G.layer)); assert.equal(h.G.labels.hidden, false);
  const off = harness({ stored: '0' });
  assert.equal(off.G.on(), false); assert.equal(off.layers.has(off.G.layer), false); assert.equal(off.G.labels.hidden, true);
  const n = h.G.labels.children.length; h.map.fire('move'); assert.equal(h.G.labels.children.length, n, 'labels are pooled, not re-created');
});
