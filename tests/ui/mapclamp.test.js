'use strict';
// The page's own map-extent block (templates/index.html, between the "map extent" markers): the latitude clamp
// and the single-world minimum zoom, run against a fake Leaflet map with real spherical-Mercator maths.
// The clamp used to correct in degrees and call setView: a correction under one pixel moved nothing (Leaflet
// truncates pan offsets to whole pixels), fired moveend again and recursed until the stack overflowed.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const html = fs.readFileSync(path.join(__dirname, '..', '..', 'templates', 'index.html'), 'utf8');
const start = html.indexOf('// ---- map extent'), end = html.indexOf('// ---- end map extent ----');
assert.ok(start > 0 && end > start, 'the map extent block is marked in the template');
const block = html.slice(html.indexOf('\n', start) + 1, end);
const factory = new Function('map', 'document', 'console', 'LATITUDE_LIMIT', 'WORLD_TILE_SIZE', 'MIN_ZOOM_PADDING_PX',
  'refreshWrappedMarkerCopies', 'saveMapView',
  block + '\nreturn { calculateSingleWorldMinZoom, latitudeOvershootPx, clampMapLatitude, handleMapMoveEnd };');

const LIMIT = 85;
function mercY(lat, z) { const s = Math.sin(lat * Math.PI / 180); return 256 * Math.pow(2, z) * (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)); }
function mercLat(y, z) { const n = Math.PI - 2 * Math.PI * y / (256 * Math.pow(2, z)); return 180 / Math.PI * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n))); }

// A map like Leaflet's: the centre kept in world pixels, panBy in WHOLE pixels (a zero offset still fires
// moveend, as Leaflet's does), every moveend delivered to every listener in registration order.
function fakeMap(o) {
  const m = { w: o.w, h: o.h, z: o.z, cy: mercY(o.lat, o.z), _loaded: o.loaded !== false, _animatingZoom: false, _panAnim: null,
              pans: [], moveends: 0, listeners: [], refreshes: 0, saves: 0 };
  m.getSize = () => ({ x: m.w, y: m.h });
  m.getZoom = () => m.z;
  m.project = (ll, z) => ({ x: 0, y: mercY(Array.isArray(ll) ? ll[0] : ll.lat, z) });
  m.getCenter = () => { if (!m._loaded) throw new Error('Set map center and zoom first.'); return { lat: mercLat(m.cy, m.z), lng: 0 }; };
  m.north = () => mercLat(m.cy - m.h / 2, m.z);
  m.south = () => mercLat(m.cy + m.h / 2, m.z);
  m.fire = () => { m.moveends++; if (m.moveends > 50) throw new RangeError('Maximum call stack size exceeded (simulated)'); m.listeners.forEach((l) => l()); };
  m.panBy = (off, opts) => { assert.equal(opts.animate, false); const dy = Math.round(off[1]); m.pans.push(dy); m.cy += dy; m.fire(); };
  m.on = (ev, fn) => { if (ev === 'moveend') m.listeners.push(fn); };
  const doc = { getElementById: () => ({ clientWidth: m.w, clientHeight: m.h }) };
  m.api = factory(m, doc, { error: (...a) => { m.error = a; } }, LIMIT, 256, 2, () => { m.refreshes++; }, () => { m.saves++; });
  m.on('moveend', m.api.handleMapMoveEnd);
  return m;
}
const pxDeg = (m, lat) => 360 / (256 * Math.pow(2, m.z)) * Math.cos(lat * Math.PI / 180);   // degrees per pixel near lat

test('a view past the south pole on a tall odd-height desktop map is clamped once, to the pixel, with no recursion', () => {
  const m = fakeMap({ w: 1554, h: 817, z: 3, lat: -89 });
  m.api.handleMapMoveEnd();
  assert.ok(m.south() >= -LIMIT - pxDeg(m, -LIMIT) && m.south() <= -LIMIT + 0.001, 'south edge within a pixel of the limit: ' + m.south());
  assert.deepEqual(m.pans.length, 1, 'one pan');
  assert.equal(m.moveends, 1, 'the clamp\'s own moveend fired once and was ignored');
  assert.equal(m.refreshes, 1); assert.equal(m.saves, 1);
  const before = m.pans.length; m.api.handleMapMoveEnd();               // the sub-pixel residual: nothing more to do
  assert.equal(m.pans.length, before, 'a residual under a pixel never pans again');
  assert.equal(m.error, undefined);
});

test('past the north pole the view is pushed down; a view inside the limits is left alone', () => {
  const m = fakeMap({ w: 1554, h: 817, z: 3, lat: 84 });
  m.api.handleMapMoveEnd();
  assert.ok(m.north() <= LIMIT + pxDeg(m, LIMIT) && m.north() >= LIMIT - 0.001, String(m.north()));
  assert.ok(m.pans[0] > 0, 'panned south (down in pixels)');
  const ok = fakeMap({ w: 1554, h: 817, z: 6, lat: 21.3 });
  ok.api.handleMapMoveEnd();
  assert.deepEqual(ok.pans, []); assert.equal(ok.refreshes, 1);
});

test('a map taller than the whole band (a phone at zoom 1) is centred on the equator and does not loop', () => {
  const m = fakeMap({ w: 375, h: 700, z: 1, lat: 40 });
  m.api.handleMapMoveEnd();
  assert.ok(Math.abs(m.getCenter().lat) < pxDeg(m, 0), 'centred: ' + m.getCenter().lat);
  assert.equal(m.pans.length, 1); assert.equal(m.moveends, 1);
});

test('nothing runs before the map has a view or while an animation is in flight (its own moveend clamps later)', () => {
  const m = fakeMap({ w: 800, h: 600, z: 3, lat: -89, loaded: false });
  m.api.clampMapLatitude(); assert.deepEqual(m.pans, []);
  const a = fakeMap({ w: 800, h: 600, z: 3, lat: -89 });
  a._animatingZoom = true; a.api.clampMapLatitude(); assert.deepEqual(a.pans, []);
  a._animatingZoom = false; a._panAnim = { _inProgress: true }; a.api.clampMapLatitude(); assert.deepEqual(a.pans, []);
  a._panAnim._inProgress = false; a.api.clampMapLatitude(); assert.equal(a.pans.length, 1);
});

test('a throwing clamp is reported, not fatal: the marker refresh and the view save still run', () => {
  const m = fakeMap({ w: 800, h: 600, z: 3, lat: -89 });
  m.project = () => { throw new Error('boom'); };
  m.api.handleMapMoveEnd();
  assert.equal(m.error[0], 'latitude clamp'); assert.equal(m.refreshes, 1); assert.equal(m.saves, 1);
});

test('the minimum zoom fits one world across AND the polar band down: landscape maps unchanged, portrait maps raised', () => {
  const wide = fakeMap({ w: 1554, h: 817, z: 3, lat: 0 });
  assert.ok(Math.abs(wide.api.calculateSingleWorldMinZoom() - Math.log2(1556 / 256)) < 1e-9, 'width rule as before');
  const phone = fakeMap({ w: 375, h: 700, z: 1, lat: 0 });
  const band0 = mercY(-LIMIT, 0) - mercY(LIMIT, 0);
  const z = phone.api.calculateSingleWorldMinZoom();
  assert.ok(Math.abs(z - Math.log2(702 / band0)) < 1e-9, 'height rule wins on a phone: ' + z);
  assert.ok(mercY(-LIMIT, z) - mercY(LIMIT, z) >= 700, 'at that zoom the band is at least the map height');
  const tall = fakeMap({ w: 900, h: 1300, z: 2, lat: 0 });
  assert.ok(tall.api.calculateSingleWorldMinZoom() > Math.log2(902 / 256), 'a portrait desktop window too');
});
