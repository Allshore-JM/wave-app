'use strict';
// G35 F2 = B-1 and B-7: the map's canvas renderer (templates/index.html, the "nearest path" block) gives a click and the
// hover to the NEAREST interactive path holding the point (Leaflet 1.9.4 gives it to the last drawn: with the touch
// reach a later neighbour stole a tap on a dot's own centre); ties: the later drawn. The touch query is any-pointer.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

const TPL = fs.readFileSync(path.join(__dirname, '..', '..', 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');
const A = TPL.indexOf('    // ---- nearest path (tests/ui/nearest.test.js runs this block) ----'), B = TPL.indexOf('    // ---- end nearest path ----');
assert.ok(A > 0 && B > A, 'the block markers');
const BLOCK = TPL.slice(A, B);

class Pt { constructor(x, y) { this.x = x; this.y = y; } distanceTo(o) { return Math.hypot(o.x - this.x, o.y - this.y); } }
// a circle marker as Leaflet's canvas sees it: radius 3, weight 1 (0.5), the renderer's tolerance
function dot(name, x, y, o = {}) {
  return { name, _point: new Pt(x, y), options: { interactive: o.interactive !== false }, moved: !!o.moved,
    _containsPoint(p) { return p.distanceTo(this._point) <= 3 + 0.5 + (o.tol === undefined ? 8 : o.tol); } };
}
function boot(opts = {}) {
  const queries = [], classes = [], timers = [];
  const L = { Canvas: { extend: (proto) => { function C(o) { this.options = o || {}; } Object.assign(C.prototype, proto); return C; } },
    DomUtil: { addClass: (el, c) => classes.push(c) } };
  const window = { matchMedia: (q) => { queries.push(q); return { matches: !!opts.coarse }; } };
  const api = new Function('L', 'window', 'setTimeout', BLOCK + '\nreturn { NearestCanvas, nearestPath, coarsePointer, COARSE_TOL };')(L, window, (fn, ms) => timers.push({ fn, ms }));
  return { api, queries, classes, timers };
}
function renderer(b, layers) {
  const r = new b.api.NearestCanvas({ tolerance: 8 }), fired = [];
  let first = null, prev = null;
  layers.forEach((l) => { const o = { layer: l, next: null }; if (prev) prev.next = o; else first = o; prev = o; });
  r._drawFirst = first;
  r._map = { mouseEventToLayerPoint: (e) => e.pt, _draggableMoved: (l) => l.moved };
  r._fireEvent = (ls, e, type) => fired.push([ls ? ls.map((l) => l.name) : false, type || e.type]);
  r._container = {};
  r._handleMouseOut = function (e) { if (this._hoveredLayer) { fired.push([[this._hoveredLayer.name], 'mouseout']); this._hoveredLayer = null; } };
  return { r, fired };
}

test('a tap on a dot\'s own centre opens that dot, not a later neighbour within the reach; between two, the nearer; a tie, the later drawn', () => {
  const b = boot();
  const { r, fired } = renderer(b, [dot('51207', 0, 0), dot('51210', 7, 0)]);   // Kaneohe Bay: 7 px apart at Oahu z10
  r._onClick({ type: 'click', pt: new Pt(0, 0) }); assert.deepEqual(fired.pop(), [['51207'], 'click'], 'Leaflet would open 51210');
  r._onClick({ type: 'click', pt: new Pt(5, 0) }); assert.deepEqual(fired.pop(), [['51210'], 'click']);
  r._onClick({ type: 'click', pt: new Pt(3.5, 0) }); assert.deepEqual(fired.pop(), [['51210'], 'click'], 'half way: the later drawn');
  r._onClick({ type: 'click', pt: new Pt(30, 0) }); assert.deepEqual(fired.pop(), [false, 'click'], 'nothing in reach');
  const lone = renderer(b, [dot('13130', 0, 0)]);
  lone.r._onClick({ type: 'click', pt: new Pt(11, 0) }); assert.deepEqual(lone.fired.pop(), [['13130'], 'click'], 'a lone dot keeps its touch reach');
});

test('a dot that is not interactive, or one dragged on a click, is passed over; a path without a point wins only alone', () => {
  const b = boot();
  const { r, fired } = renderer(b, [dot('A', 0, 0), dot('B', 2, 0, { interactive: false })]);
  r._onClick({ type: 'click', pt: new Pt(2, 0) }); assert.deepEqual(fired.pop(), [['A'], 'click']);
  const m = renderer(b, [dot('A', 0, 0), dot('M', 1, 0, { moved: true })]);
  m.r._onClick({ type: 'click', pt: new Pt(1, 0) }); assert.deepEqual(m.fired.pop(), [['A'], 'click'], 'the dragged one is not clicked');
  m.r._onClick({ type: 'mousedown', pt: new Pt(1, 0) }); assert.deepEqual(m.fired.pop(), [['M'], 'mousedown'], 'other events: the drag does not count');
  const line = { name: 'line', options: { interactive: true }, _containsPoint: () => true, moved: false };
  const l = renderer(b, [dot('A', 0, 0), line]);
  l.r._onClick({ type: 'click', pt: new Pt(1, 0) }); assert.deepEqual(l.fired.pop(), [['A'], 'click'], 'a dot beats a path of no point');
  const only = renderer(b, [line]);
  only.r._onClick({ type: 'click', pt: new Pt(1, 0) }); assert.deepEqual(only.fired.pop(), [['line'], 'click']);
});

test('hover takes the nearest too: mouseover once, mouseout on a change, throttled for 32 ms', () => {
  const b = boot();
  const { r, fired } = renderer(b, [dot('A', 0, 0), dot('B', 7, 0)]);
  r._handleMouseHover({ type: 'mousemove' }, new Pt(1, 0));
  assert.deepEqual(fired.splice(0), [[['A'], 'mouseover'], [['A'], 'mousemove']]); assert.deepEqual(b.classes, ['leaflet-interactive']);
  r._handleMouseHover({ type: 'mousemove' }, new Pt(6, 0)); assert.deepEqual(fired.splice(0), [], 'throttled');
  assert.equal(b.timers.length, 1); assert.equal(b.timers[0].ms, 32); b.timers[0].fn();
  r._handleMouseHover({ type: 'mousemove' }, new Pt(6, 0));
  assert.deepEqual(fired.splice(0), [[['A'], 'mouseout'], [['B'], 'mouseover'], [['B'], 'mousemove']]);
  b.timers[1].fn();
  r._handleMouseHover({ type: 'mousemove' }, new Pt(40, 0)); assert.deepEqual(fired.splice(0), [[['B'], 'mouseout'], [false, 'mousemove']]);
});

test('the touch reach is for any touch screen ((any-pointer: coarse): an iPad with a keyboard, a touch laptop: B-7)', () => {
  const b = boot({ coarse: true });
  assert.equal(b.api.coarsePointer(), true); assert.deepEqual(b.queries, ['(any-pointer: coarse)']); assert.equal(b.api.COARSE_TOL, 8);
  assert.equal(boot({ coarse: false }).api.coarsePointer(), false);
  assert.equal(new Function('L', 'window', 'setTimeout', BLOCK + '\nreturn coarsePointer();')({ Canvas: { extend: () => function () {} }, DomUtil: {} }, {}, () => {}), false, 'no matchMedia: a mouse');
});
