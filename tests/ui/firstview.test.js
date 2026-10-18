'use strict';
// The page's first-view block (templates/index.html, between the "first view" markers; plan section 46): where the
// site opens. The tab's own view wins; else the viewer's default station; else the visitor's area from the head's geo
// race (window.__early.place), the map zoomed out until the nearest station is in view; else Hawaii. While the race
// is pending the map pane is hidden (geo-pending); a visitor who acts first keeps what they did.  node --test tests/ui/
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const html = fs.readFileSync(path.join(__dirname, '..', '..', 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');
const a = html.indexOf('// ---- first view'), b = html.indexOf('// ---- end first view ----');
assert.ok(a > 0 && b > a, 'the first view block is marked in the template');
const BLOCK = html.slice(html.indexOf('\n', a) + 1, b);
assert.ok(BLOCK.indexOf('enforceSingleWorld();') > 0 && BLOCK.indexOf('geo-pending') > 0);

const MIN_Z = 2.6, MAX_Z = 18, DEFAULT_Z = 6;
const clampLat = (lat) => Math.max(-79, Math.min(84, lat)), normLng = (lng) => ((lng + 540) % 360) - 180;

// run the block against fakes; opts: session (the saved view), search, dflt (the default station), place (a promise
// or anything else), size (the map's size)
function run(opts) {
  opts = opts || {};
  const log = { views: [], saved: 0, clamped: 0, rebuilt: 0, enforced: 0, timers: [], classes: new Set(), listeners: {}, docListeners: {} };
  const el = { classList: { add: (c) => log.classes.add(c), remove: (c) => log.classes.delete(c), contains: (c) => log.classes.has(c) },
    addEventListener: (ev, fn, o) => { (log.listeners[ev] = log.listeners[ev] || []).push({ fn, o }); } };
  const map = { setView: (ll, z, o) => { log.views.push({ lat: ll[0], lng: ll[1], z, animate: o && o.animate }); },
    getSize: () => ({ x: (opts.size || [1280, 800])[0], y: (opts.size || [1280, 800])[1] }), getContainer: () => el };
  const storage = { 'allshore.defaultStation.v1': opts.dflt === undefined ? null : (typeof opts.dflt === 'string' ? opts.dflt : JSON.stringify(opts.dflt)) };
  const fn = new Function('loadMapView', 'enforceSingleWorld', 'location', 'localStorage', 'map', 'clampLat', 'normLng', 'MAX_MAP_ZOOM', 'MIN_MAP_ZOOM', 'DEFAULT_MAP_ZOOM',
    'clampMapLatitude', 'refreshWrappedMarkerCopies', 'saveMapView', 'window', 'document', 'setTimeout', 'clearTimeout', 'URLSearchParams', BLOCK);
  fn(() => (opts.session === undefined ? null : opts.session), () => { log.enforced++; }, { search: opts.search || '' },
    { getItem: (k) => (k in storage ? storage[k] : null) }, map, clampLat, normLng, MAX_Z, MIN_Z, DEFAULT_Z,
    () => { log.clamped++; }, () => { log.rebuilt++; }, () => { log.saved++; },
    { __early: opts.early === undefined ? (opts.place === undefined ? undefined : { place: opts.place }) : opts.early },
    { addEventListener: (ev, fn2, o) => { (log.docListeners[ev] = log.docListeners[ev] || []).push({ fn: fn2, o }); } },
    (fn2, ms) => { log.timers.push({ fn: fn2, ms, cleared: false }); return log.timers.length; }, (id) => { if (log.timers[id - 1]) log.timers[id - 1].cleared = true; }, URLSearchParams);
  return { log, el, map, act: (ev) => (log.listeners[ev] || []).forEach((l) => l.fn({})), station: () => (log.docListeners['allshore:station'] || []).forEach((l) => l.fn({})),
    pending: () => log.classes.has('geo-pending') };
}
const settle = () => new Promise((r) => setImmediate(r)).then(() => new Promise((r) => setImmediate(r)));
const PAD = 1 - 1 / 1.3;                                  // the block's rule: the span times 1.3 fits the map (15 % each side)
// Mercator pixels of a point at a zoom (the world 256 px at zoom 0), to check what the view shows
function px(lat, lon, z) {
  const s = Math.sin(lat * Math.PI / 180), n = 256 * Math.pow(2, z);
  return [(lon + 180) / 360 * n, (0.5 - Math.log((1 + s) / (1 - s)) / (4 * Math.PI)) * n];
}
function inView(view, lat, lon, size, pad) {
  const c = px(view.lat, view.lng, view.z), p = px(lat, lon, view.z), f = 1 - (pad || 0);
  return Math.abs(p[0] - c[0]) <= size[0] / 2 * f && Math.abs(p[1] - c[1]) <= size[1] / 2 * f;
}

test('the tab\'s own view wins: the first view is set once and nothing else happens (no listeners, no pending class, no timer), even with a race and a default', async () => {
  const r = run({ session: { lat: 10, lng: 20, z: 5 }, place: Promise.resolve({ lat: 21.3, lon: -157.9, sid: 'HNL01', slat: 21.25, slon: -157.8 }), dflt: { id: 'HNL01', lat: 1, lon: 2 } });
  await settle();
  assert.equal(r.log.enforced, 1); assert.deepEqual(r.log.views, []); assert.equal(r.pending(), false);
  assert.deepEqual(Object.keys(r.log.listeners), []); assert.equal(r.log.timers.length, 0); assert.equal(r.log.saved, 0);
  for (const sv of [{ lat: 'x', lng: 20, z: 5 }, { lat: 10 }, null, 'bad']) {           // a damaged saved view does not count
    const q = run({ session: sv, place: Promise.resolve(null) }); await settle(); assert.equal(q.log.enforced, 1); assert.equal(q.pending(), false);
  }
});

test('a link to a station opens the map as before (no race view, no pending class); the default station\'s position at the default zoom, saved; a default without a position does nothing', async () => {
  const r = run({ search: '?station=46001', place: Promise.resolve({ lat: 21.3, lon: -157.9, sid: 'HNL01', slat: 21.25, slon: -157.8 }), dflt: { id: 'HNL01', lat: 1, lon: 2 } });
  await settle(); assert.deepEqual(r.log.views, []); assert.equal(r.pending(), false); assert.equal(r.log.timers.length, 0);
  const d = run({ dflt: { id: 'HNL01', lat: 21.25, lon: -157.8, name: 'Diamond Head', at: 1 }, place: Promise.resolve({ lat: 50, lon: 0, sid: 'X', slat: 50, slon: 1 }) });
  await settle();
  assert.deepEqual(d.log.views, [{ lat: 21.25, lng: -157.8, z: 6, animate: false }], 'the default station at zoom 6, nothing from the race');
  assert.equal(d.log.saved, 1); assert.equal(d.log.clamped, 1); assert.equal(d.log.rebuilt, 1); assert.equal(d.pending(), false);
  const e = run({ dflt: { id: 'HNL01' }, place: Promise.resolve({ lat: 50, lon: 0, sid: 'X', slat: 50, slon: 1 }) });
  await settle(); assert.deepEqual(e.log.views, [], 'no position: the default decides the station only'); assert.equal(e.pending(), false);
  for (const bad of [{ lat: 400, lon: 2 }, { lat: 21, lon: -181 }, { lat: '21', lon: 2 }, { lat: NaN, lon: 2 }]) {     // a bad position: nothing
    const f = run({ dflt: Object.assign({ id: 'HNL01' }, bad) }); await settle(); assert.deepEqual(f.log.views, [], JSON.stringify(bad));
  }
  const far = run({ place: Promise.resolve({ lat: 95, lon: 2, sid: 'S', slat: 1, slon: 2 }) }); await settle(); assert.deepEqual(far.log.views, [], 'a position off the earth');
  const g = run({ dflt: '{bad json' }); await settle(); assert.deepEqual(g.log.views, []);
});

test('the visitor\'s area: the pane is hidden while the race runs, then the view is centred on the visitor at zoom 6 with the nearest station in view, saved; the timer is cleared', async () => {
  let resolve; const place = new Promise((r) => { resolve = r; });
  const r = run({ place });
  assert.equal(r.pending(), true, 'hidden at once'); assert.deepEqual(r.log.views, []);
  assert.deepEqual(Object.keys(r.log.listeners).sort(), ['keydown', 'pointerdown', 'touchstart', 'wheel']);
  assert.ok(r.log.listeners.wheel[0].o.once && r.log.listeners.wheel[0].o.passive);
  assert.deepEqual(r.log.timers.map((t) => t.ms), [2500]);
  resolve({ lat: 21.3, lon: -157.86, sid: 'HNL01', slat: 21.25, slon: -157.8 }); await settle();
  assert.equal(r.pending(), false); assert.equal(r.log.timers[0].cleared, true);
  assert.deepEqual(r.log.views, [{ lat: 21.3, lng: -157.86, z: 6, animate: false }]);
  assert.equal(r.log.saved, 1); assert.equal(r.log.clamped, 1); assert.equal(r.log.rebuilt, 1);
  assert.ok(inView(r.log.views[0], 21.25, -157.8, [1280, 800], PAD));
});

test('a station far from the visitor (inland, or a lone coast) lowers the zoom until it is in view with 15 % padding, never below the map\'s minimum; a rounding never puts it out of view', async () => {
  const cases = [[39.74, -104.99, 36.0, -122.0], [48.86, 2.35, 45.0, -8.0], [64.1, -21.9, 58.1, -5.3], [-33.9, 18.4, -34.0, 18.5], [35.7, 139.7, 21.3, -157.9]];
  for (const [lat, lon, slat, slon] of cases) {
    for (const size of [[1280, 800], [375, 700], [1906, 985]]) {
      const r = run({ place: Promise.resolve({ lat, lon, sid: 'S', slat, slon }), size });
      await settle();
      const v = r.log.views[0]; assert.ok(v, JSON.stringify([lat, lon, size]));
      assert.equal(v.lat, clampLat(lat)); assert.equal(v.lng, normLng(lon));
      assert.ok(v.z >= MIN_Z && v.z <= DEFAULT_Z, JSON.stringify(v));
      assert.ok(Math.abs(v.z * 10 - Math.round(v.z * 10)) < 1e-9, 'a tenth of a zoom level (zoomSnap 0.1)');
      // the station is inside the view with the padding unless the minimum zoom stopped the zoom-out
      const slon2 = slon - lon > 180 ? slon - 360 : (lon - slon > 180 ? slon + 360 : slon);
      if (v.z > MIN_Z + 1e-9) assert.ok(inView(v, slat, slon2, size, PAD), 'in view with padding: ' + JSON.stringify([lat, lon, slat, slon, size, v]));
      if (v.z < DEFAULT_Z - 1e-9) assert.ok(v.z + 0.1 > DEFAULT_Z || !inView({ ...v, z: v.z + 0.1 }, slat, slon2, size, PAD), 'the largest such zoom (a tenth more and the station leaves the padded view): ' + JSON.stringify([lat, lon, slat, slon, size, v]));
    }
  }
});

test('across the date line the station counts on the visitor\'s side: a visitor at 179.9 E with the station at 179.5 W keeps zoom 6', async () => {
  const r = run({ place: Promise.resolve({ lat: -17.6, lon: 179.9, sid: 'XFJ2', slat: -17.5, slon: -179.5 }) });
  await settle();
  assert.equal(r.log.views.length, 1); assert.equal(r.log.views[0].z, 6); assert.ok(Math.abs(r.log.views[0].lng - 179.9) < 1e-9 && r.log.views[0].lat === -17.6);
  const w = run({ place: Promise.resolve({ lat: -17.6, lon: -179.9, sid: 'XFJ1', slat: -17.5, slon: 179.4 }) });
  await settle(); assert.equal(w.log.views[0].z, 6);
  const far = run({ place: Promise.resolve({ lat: 0, lon: 179.9, sid: 'F', slat: 0, slon: -160 }) });   // 20 degrees away across the line
  await settle(); assert.ok(far.log.views[0].z < 6 && far.log.views[0].z > 3, JSON.stringify(far.log.views[0]));
});

test('a slow or failed race: the pane shows again (the 2.5-s guard, or the rejection) and the map stays; a late answer changes nothing', async () => {
  let resolve; const slow = run({ place: new Promise((r) => { resolve = r; }) });
  assert.equal(slow.pending(), true);
  slow.log.timers[0].fn(); assert.equal(slow.pending(), false); assert.deepEqual(slow.log.views, []);
  resolve({ lat: 21.3, lon: -157.86, sid: 'HNL01', slat: 21.25, slon: -157.8 }); await settle();
  assert.deepEqual(slow.log.views, [], 'late: nothing'); assert.equal(slow.log.saved, 0);
  const failed = run({ place: Promise.reject(new Error('geo down')) });
  await settle(); assert.equal(failed.pending(), false); assert.deepEqual(failed.log.views, []); assert.equal(failed.log.timers[0].cleared, true);
  const none = run({ place: Promise.resolve(null) });
  await settle(); assert.equal(none.pending(), false); assert.deepEqual(none.log.views, []);
  for (const bad of [{ lat: 'x', lon: 1, sid: 'S' }, { lat: 21, sid: 'S' }, 'Honolulu']) {
    const q = run({ place: Promise.resolve(bad) }); await settle(); assert.deepEqual(q.log.views, []); assert.equal(q.pending(), false);
  }
});

test('a visitor who acts first (a pointer, wheel, key or touch on the map, or a station pick) keeps the map; the pane shows again on the answer', async () => {
  for (const act of ['pointerdown', 'wheel', 'keydown', 'touchstart', 'station']) {
    let resolve; const r = run({ place: new Promise((res) => { resolve = res; }) });
    if (act === 'station') r.station(); else r.act(act);
    resolve({ lat: 21.3, lon: -157.86, sid: 'HNL01', slat: 21.25, slon: -157.8 }); await settle();
    assert.deepEqual(r.log.views, [], act); assert.equal(r.pending(), false, act); assert.equal(r.log.saved, 0);
  }
  assert.ok(run({ place: new Promise(() => {}) }).log.docListeners['allshore:station'][0].o.once, 'the pick listener is once');
});

test('without the geo Worker (no place, or no __early at all) the map opens as before: Hawaii, nothing hidden, no listeners', async () => {
  for (const opts of [{}, { early: {} }, { early: { place: null } }, { early: { place: 'soon' } }, { early: { place: {} } }]) {
    const r = run(opts); await settle();
    assert.equal(r.log.enforced, 1); assert.deepEqual(r.log.views, []); assert.equal(r.pending(), false);
    assert.deepEqual(Object.keys(r.log.listeners), []); assert.equal(r.log.timers.length, 0);
  }
});
