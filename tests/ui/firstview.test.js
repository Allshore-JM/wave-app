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
  const log = { views: [], saved: 0, clamped: 0, rebuilt: 0, enforced: 0, forgot: 0, timers: [], classes: new Set(), listeners: {}, removed: {}, docListeners: {} };
  const el = { classList: { add: (c) => log.classes.add(c), remove: (c) => log.classes.delete(c), contains: (c) => log.classes.has(c) },
    addEventListener: (ev, fn, o) => { (log.listeners[ev] = log.listeners[ev] || []).push({ fn, o }); },
    removeEventListener: (ev, fn) => { log.removed[ev] = (log.removed[ev] || 0) + 1; log.listeners[ev] = (log.listeners[ev] || []).filter((l) => l.fn !== fn); } };
  const map = { setView: (ll, z, o) => { log.views.push({ lat: ll[0], lng: ll[1], z, animate: o && o.animate }); },
    getSize: () => ({ x: (opts.size || [1280, 800])[0], y: (opts.size || [1280, 800])[1] }), getContainer: () => el };
  const storage = { 'allshore.defaultStation.v1': opts.dflt === undefined ? null : (typeof opts.dflt === 'string' ? opts.dflt : JSON.stringify(opts.dflt)) };
  const fn = new Function('loadMapView', 'enforceSingleWorld', 'location', 'localStorage', 'map', 'clampLat', 'normLng', 'MAX_MAP_ZOOM', 'MIN_MAP_ZOOM', 'DEFAULT_MAP_ZOOM',
    'clampMapLatitude', 'refreshWrappedMarkerCopies', 'saveMapView', 'window', 'document', 'setTimeout', 'clearTimeout', 'URLSearchParams', 'forgetMapView', BLOCK);
  fn(() => (opts.session === undefined ? null : opts.session), () => { log.enforced++; }, { search: opts.search || '' },
    { getItem: (k) => (k in storage ? storage[k] : null) }, map, clampLat, normLng, MAX_Z, MIN_Z, DEFAULT_Z,
    () => { log.clamped++; }, () => { log.rebuilt++; }, () => { log.saved++; },
    { __early: opts.early === undefined ? (opts.place === undefined ? undefined : { place: opts.place }) : opts.early },
    { addEventListener: (ev, fn2, o) => { (log.docListeners[ev] = log.docListeners[ev] || []).push({ fn: fn2, o }); } },
    (fn2, ms) => { log.timers.push({ fn: fn2, ms, cleared: false }); return log.timers.length; }, (id) => { if (log.timers[id - 1]) log.timers[id - 1].cleared = true; }, URLSearchParams,
    () => { log.forgot++; });
  // act(ev, cls): the event's target is the map, or (cls) an element of that class on it; closest(sel) matches the
  // selector's classes for real, so the block must name each class it means (G32 re-check D-1: R1's P2)
  const target = (cls) => ({ closest: (sel) => (cls && sel.split(',').map((x) => x.trim().replace(/^\./, '')).indexOf(cls) >= 0 ? {} : null) });
  return { log, el, map, act: (ev, cls) => (log.listeners[ev] || []).slice().forEach((l) => l.fn({ target: target(cls) })), station: () => (log.docListeners['allshore:station'] || []).forEach((l) => l.fn({})),
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
  assert.ok(r.log.listeners.wheel[0].o.passive);
  assert.equal(r.log.forgot, 1, 'the synchronous Hawaii view is forgotten: a reload during the race asks again (G32 B-6)');
  assert.deepEqual(r.log.timers.map((t) => t.ms), [2500]);
  resolve({ lat: 21.3, lon: -157.86, sid: 'HNL01', slat: 21.25, slon: -157.8 }); await settle();
  assert.equal(r.pending(), false); assert.equal(r.log.timers[0].cleared, true);
  assert.deepEqual(Object.keys(r.log.listeners).filter((ev) => r.log.listeners[ev].length), [], 'the map listeners go with the answer');
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

test('acting on the map shows it at once; a pointer, wheel, key or touch on a control (the overlay panel, the gear, the sheet) is not acting (G32 B-8)', async () => {
  for (const ev of ['pointerdown', 'wheel', 'keydown', 'touchstart']) {
    let resolve; const r = run({ place: new Promise((res) => { resolve = res; }) });
    r.act(ev, 'leaflet-control'); r.act(ev, 'ov-sheet');
    assert.equal(r.pending(), true, ev + ' on a control or the overlay\'s sheet: still waiting');
    r.act(ev, 'leaflet-marker-icon');
    assert.equal(r.pending(), false, ev + ' on a marker on the map (no control): acting');
    r.act(ev);
    assert.equal(r.pending(), false, ev + ' on the map: shown at once');
    resolve({ lat: 21.3, lon: -157.86, sid: 'HNL01', slat: 21.25, slon: -157.8 }); await settle();
    assert.deepEqual(r.log.views, [], ev + ': the map stays where the visitor put it');
  }
  let resolve; const c = run({ place: new Promise((res) => { resolve = res; }) });
  c.act('pointerdown', 'leaflet-control'); c.act('wheel', 'ov-sheet');
  resolve({ lat: 21.3, lon: -157.86, sid: 'HNL01', slat: 21.25, slon: -157.8 }); await settle();
  assert.deepEqual(c.log.views, [{ lat: 21.3, lng: -157.86, z: 6, animate: false }], "only controls touched: the visitor's area still opens");
});

test("the forgotten view: only while the race decides (not with the tab's own view, a link, a default, or without the Worker)", async () => {
  for (const opts of [{ session: { lat: 10, lng: 20, z: 5 }, place: new Promise(() => {}) }, { search: '?station=46001', place: new Promise(() => {}) },
    { dflt: { id: 'HNL01', lat: 21, lon: -157 }, place: new Promise(() => {}) }, {}, { early: { place: null } }]) {
    const r = run(opts); await settle(); assert.equal(r.log.forgot, 0, JSON.stringify(Object.keys(opts)));
  }
});

test('without the geo Worker (no place, or no __early at all) the map opens as before: Hawaii, nothing hidden, no listeners', async () => {
  for (const opts of [{}, { early: {} }, { early: { place: null } }, { early: { place: 'soon' } }, { early: { place: {} } }]) {
    const r = run(opts); await settle();
    assert.equal(r.log.enforced, 1); assert.deepEqual(r.log.views, []); assert.equal(r.pending(), false);
    assert.deepEqual(Object.keys(r.log.listeners), []); assert.equal(r.log.timers.length, 0);
  }
});

// ---- a retired default station (G32 A-P3-3 / B-7): forgotten once the station list is in, said once in the window ----
const FR = (() => {
  const s = html.indexOf('    function tellOnPage(msg) {'), f = html.indexOf('    function forgetRetiredDefault(list) {'), e = html.indexOf('\n    }\n', f);
  assert.ok(s > 0 && f > s && e > f, 'tellOnPage .. forgetRetiredDefault are in the template');
  return html.slice(s, e + 6);
})();
function runForget(stored, list, opts) {
  opts = opts || {};
  const store = { 'allshore.defaultStation.v1': stored === undefined ? null : JSON.stringify(stored) }, notes = [], docL = {};
  const localStorage = { getItem: (k) => (k in store ? store[k] : null), removeItem: (k) => { if (opts.blocked) throw new Error('blocked'); delete store[k]; } };
  const F = { parsePointId: (id) => (/^pt_\d+[NS]_\d+[EW]$/.test(id) ? { lat: 1, lon: 2 } : null), note: (m, sid) => notes.push([m, sid]) };
  const window = { AllshoreForecast: opts.noApp ? undefined : F, __early: opts.early };
  const document = { readyState: opts.readyState, addEventListener: (ev, fn, o) => { (docL[ev] = docL[ev] || []).push({ fn, o }); } };
  new Function('localStorage', 'window', 'document', FR + (opts.noList ? '' : '\nforgetRetiredDefault(' + JSON.stringify(list) + ');'))(localStorage, window, document);
  return { store, notes, docL, window, F };
}
const LIST2 = [{ id: '51201' }, { id: '46026' }, { id: 'NW-HFO51' }];
for (let i = 0; i < 120; i++) LIST2.push({ id: 'ST' + (1000 + i) });       // a real list (734 today); under 100 entries nothing is decided

test('a default the station list still has, or a forecast point, is kept and nothing is said', () => {
  for (const d of [{ id: '46026' }, { id: ' 46026 ' }, { id: 'NW-HFO51' }, { id: 'pt_21667N_158054W' }]) {
    const r = runForget(d, LIST2);
    assert.notEqual(r.store['allshore.defaultStation.v1'], undefined, JSON.stringify(d)); assert.deepEqual(r.notes, []);
  }
  const none = runForget(undefined, LIST2); assert.deepEqual(none.notes, []);
});

test('a default POINT is never erased before forecast.js has run (the list can land first: the head asks for it): decided once the page has parsed, kept when the app never comes', () => {
  const P = { id: 'pt_21700N_158200W', lat: 21.7, lon: -158.2 };
  const r = runForget(P, LIST2, { noApp: true, readyState: 'loading' });
  assert.notEqual(r.store['allshore.defaultStation.v1'], undefined, 'kept while the app is not up'); assert.deepEqual(r.notes, []);
  const l = r.docL.DOMContentLoaded; assert.ok(l && l.length === 1 && l[0].o.once, 'decided once the page has parsed');
  r.window.AllshoreForecast = r.F; l[0].fn();
  assert.notEqual(r.store['allshore.defaultStation.v1'], undefined, 'a real point stays the default'); assert.deepEqual(r.notes, []);
  const late = runForget(P, LIST2, { noApp: true, readyState: 'interactive' });  // parsed, and still no app: kept, nothing waits
  assert.notEqual(late.store['allshore.defaultStation.v1'], undefined); assert.deepEqual(late.notes, []); assert.equal(late.docL.DOMContentLoaded, undefined);
  const bad = runForget({ id: 'pt_bad' }, LIST2, { noApp: true, readyState: 'loading' });   // an id the app refuses goes once it decides
  assert.notEqual(bad.store['allshore.defaultStation.v1'], undefined, 'not before the app has looked');
  bad.window.AllshoreForecast = bad.F; bad.docL.DOMContentLoaded[0].fn();
  assert.equal(bad.store['allshore.defaultStation.v1'], undefined); assert.equal(bad.notes.length, 1); assert.equal(bad.notes[0][1], null, 'a note for no station in particular');
  const app = runForget(P, LIST2, { readyState: 'loading' });                 // with the app already up: decided at once
  assert.notEqual(app.store['allshore.defaultStation.v1'], undefined); assert.equal(app.docL.DOMContentLoaded, undefined);
});

test('a default the list no longer has (or one the id rule refuses) is forgotten once the list is in, and the window says so once; an empty or failed list decides nothing', () => {
  const r = runForget({ id: 'OLD99', lat: 1, lon: 2 }, LIST2);
  assert.equal(r.store['allshore.defaultStation.v1'], undefined, 'forgotten');
  assert.equal(r.notes.length, 1); assert.ok(/OLD99/.test(r.notes[0][0]) && /next time/.test(r.notes[0][0])); assert.equal(r.notes[0][1], null);
  assert.equal(r.docL['allshore:forecast'], undefined, 'with the app up it is told once, and it keeps the note for the first forecast (test-site check V7-3)');
  const bad = runForget({ id: 'x y' }, LIST2); assert.equal(bad.store['allshore.defaultStation.v1'], undefined);
  for (const list of [[], null, 'x', LIST2.slice(0, 99), [{ id: '51201' }, { id: '51202' }, { id: '51203' }, { id: '51204' }, { id: '51205' }, { id: '51206' }, { id: '51207' }, { id: '51208' }, { id: '51209' }, { id: '51210' }]]) {
    const k = runForget({ id: 'OLD99' }, list); assert.notEqual(k.store['allshore.defaultStation.v1'], undefined, 'kept: ' + (Array.isArray(list) ? list.length + ' entries' : JSON.stringify(list))); assert.deepEqual(k.notes, []);   // the 10-buoy fallback list decides nothing (G32 re-check P-1)
  }
  assert.equal(runForget({ id: 'OLD99' }, LIST2.slice(0, 100)).store['allshore.defaultStation.v1'], undefined, '100 entries: judged');
  const blocked = runForget({ id: 'OLD99' }, LIST2, { blocked: true }); assert.deepEqual(blocked.notes, [], 'storage refuses: nothing said');
  const early = runForget({ id: 'OLD99' }, LIST2, { noApp: true });
  assert.deepEqual(early.notes, []); assert.equal(early.store['allshore.defaultStation.v1'], undefined, 'forgotten before the window is up');
  assert.equal(early.docL['allshore:forecast'].length, 1); assert.ok(early.docL['allshore:forecast'][0].o.once);
  early.window.AllshoreForecast = early.F; early.docL['allshore:forecast'][0].fn();
  assert.equal(early.notes.length, 1, 'said when the window is up');
});

test('a removed default the head already forgot (G35 B-8): said once at page start, whether or not the list ever lands (re-check F-2)', () => {
  for (const [list, noList] of [[LIST2, false], [[], false], [null, true]]) {
    const r = runForget(undefined, list, { early: { retired: '44062' }, noList });
    assert.equal(r.notes.length, 1, 'said' + (noList ? ' with no list at all (two failed requests)' : '')); assert.ok(/44062/.test(r.notes[0][0]) && /near you next time/.test(r.notes[0][0])); assert.equal(r.notes[0][1], null);
    assert.equal(r.window.__early.retired, null, 'once');
  }
  const later = runForget(undefined, LIST2, { early: { retired: '44062' }, noApp: true, noList: true });
  assert.deepEqual(later.notes, [], 'before the window is up: nothing yet');
  assert.equal(later.docL['allshore:forecast'].length, 1, 'kept for the first forecast');
  later.window.AllshoreForecast = later.F; later.docL['allshore:forecast'][0].fn(); assert.equal(later.notes.length, 1);
  assert.deepEqual(runForget(undefined, LIST2, { early: { retired: null } }).notes, []);
  assert.ok(html.indexOf('    tellHeadRetired();\n    function forgetRetiredDefault(list) {') > 0, 'called at page start, not from the list');
});

// ---- the page helpers the first view and the wide box rely on, run from the template (G32 re-check D-1: R1's P5 / P7 /
// P12 were killed only by source-text pins; these drive the code) ----
function cutFn(start) {
  const s = html.indexOf(start), e = html.indexOf('\n    }\n', s);
  assert.ok(s > 0 && e > s, 'in the template: ' + start);
  return html.slice(s, e + 6);
}

test('saveMapView keeps nothing while the first view waits for the visitor\'s area (geo-pending); otherwise the clamped view under mapKey (R1 P5)', () => {
  const SRC = cutFn('    function saveMapView(map) {');
  const run = (pending) => {
    const store = {};
    const map = { getContainer: () => ({ classList: { contains: (c) => pending && c === 'geo-pending' } }), getCenter: () => ({ lat: 88, lng: -158 }), getZoom: () => 3 };
    new Function('clampLat', 'mapKey', 'sessionStorage', 'MAX_MAP_ZOOM', 'MIN_MAP_ZOOM', 'mapArg', SRC + '\nsaveMapView(mapArg);')(clampLat, 'mapView', { setItem: (k, v) => { store[k] = v; } }, MAX_Z, MIN_Z, map);
    return store;
  };
  assert.deepEqual(run(true), {}, 'geo-pending: nothing kept');
  assert.deepEqual(JSON.parse(run(false).mapView), { lat: 84, lng: -158, z: 3 }, 'not pending: the view, its latitude clamped');
});

test('measureTopRight counts the map tools\' bar while it shows (the wide box narrows beside it, G32 B-1) and leaves a hidden one out (R1 P7)', () => {
  const SRC = cutFn('    function measureTopRight() {');
  const rect = (left, top, right, bottom) => ({ left, top, right, bottom, width: right - left, height: bottom - top });
  const run = (barRect) => {
    const set = {};
    const controls = [{ r: rect(1110, 10, 1270, 100), cls: 'leaflet-control' }, { r: rect(1230, 110, 1270, 150), cls: 'leaflet-control' }, { r: barRect, cls: 'leaflet-control tools-bar' }]
      .map((c) => ({ getBoundingClientRect: () => c.r, classList: { contains: (x) => c.cls.split(' ').indexOf(x) >= 0 } }));
    const map = { getContainer: () => ({ getBoundingClientRect: () => rect(0, 0, 1280, 800), querySelectorAll: (sel) => { assert.equal(sel, '.leaflet-top.leaflet-right > .leaflet-control', "the corner's own controls only: never the layer control inside the legend's panel (plan section 54)"); return controls; } }) };
    new Function('map', 'document', SRC + '\nmeasureTopRight();')(map, { documentElement: { style: { setProperty: (k, v) => { set[k] = v; } } } });
    return set;
  };
  assert.deepEqual(run(rect(970, 160, 1270, 400)), { '--map-topright-h': '400px', '--map-topright-w': '310px' }, 'the bar shown: width and height reach it');
  assert.deepEqual(run(rect(970, 160, 970, 160)), { '--map-topright-h': '150px', '--map-topright-w': '170px' }, 'the bar hidden (no size): the legend and the gear alone');
});

test('the stations list, once in, is kept, drawn, judged for a retired default and used for a pending focus, in that order (R1 P12); a refused answer is asked for once more (G35)', async () => {
  const s = html.indexOf('    const STATIONS_RETRY_MS'), e = html.indexOf('    loadForecastStations(1);\n', s);
  assert.ok(s > 0 && e > s, 'the stations load is in the template');
  const SRC = html.slice(s, e + '    loadForecastStations(1);\n'.length);
  const run = async (answers) => {
    const calls = [], timers = [];
    const out = new Function('earlyFetch', 'rebuildForecastMarkers', 'forgetRetiredDefault', 'focusStation', 'setTimeout',
      'var forecastStationsData = null, pendingFocusStation = "HNL01";\n' + SRC + '\nreturn function () { return { data: forecastStationsData, pending: pendingFocusStation }; };')(
      (name, url) => { calls.push(['fetch', name, url]); const a = answers.shift(); return a instanceof Error ? Promise.reject(a) : Promise.resolve(a); },
      (force) => calls.push(['rebuild', force]), (l) => calls.push(['forget', l]), (sid) => calls.push(['focus', sid]),
      (fn, ms) => { timers.push({ fn, ms }); return timers.length; });
    await settle();
    return { calls, timers, state: out };
  };
  const body = (list) => ({ json: () => Promise.resolve(list) });
  const r = await run([body(LIST2)]);
  assert.deepEqual(r.calls, [['fetch', 'stations', '/stations.json'], ['rebuild', true], ['forget', LIST2], ['focus', 'HNL01']]);
  assert.equal(r.state().data, LIST2); assert.equal(r.state().pending, null, 'the pending focus is spent'); assert.equal(r.timers.length, 0);
  const n = await run([body(null)]);
  assert.deepEqual(n.calls.slice(1), [['rebuild', true], ['forget', []], ['focus', 'HNL01']], 'a null body: an empty list (which decides nothing)');
  for (const bad of [new TypeError('Failed to fetch'), { ok: false, status: 503, json: () => Promise.resolve(LIST2) }]) {
    const t = await run([bad, body(LIST2)]);
    assert.deepEqual(t.calls, [['fetch', 'stations', '/stations.json']], 'nothing drawn from a refused answer');
    assert.equal(t.timers.length, 1); assert.equal(t.timers[0].ms, 4000);
    t.timers[0].fn(); await settle();
    assert.deepEqual(t.calls.slice(1), [['fetch', 'stations', '/stations.json'], ['rebuild', true], ['forget', LIST2], ['focus', 'HNL01']], 'asked once more');
  }
  const twice = await run([new TypeError('x'), new TypeError('y')]);
  twice.timers[0].fn(); await settle();
  assert.equal(twice.timers.length, 1, 'once more, not for ever'); assert.equal(twice.state().data, null);
  // a fault in the code that uses the list is logged, never a reason to ask for the list again (re-check R1)
  const logged = [], oldErr = console.error;
  console.error = (...a) => logged.push(a);
  try {
    const boom = await (async () => {
      const calls = [], timers = [];
      new Function('earlyFetch', 'rebuildForecastMarkers', 'forgetRetiredDefault', 'focusStation', 'setTimeout',
        'var forecastStationsData = null, pendingFocusStation = null;\n' + SRC)(
        (name, url) => { calls.push(['fetch', name, url]); return Promise.resolve(body(LIST2)); },
        () => { throw new TypeError('a drawing fault'); }, () => calls.push(['forget']), () => {},
        (fn, ms) => { timers.push({ fn, ms }); return timers.length; });
      await settle();
      return { calls, timers };
    })();
    assert.deepEqual(boom.calls, [['fetch', 'stations', '/stations.json']], 'asked once');
    assert.equal(boom.timers.length, 0, 'no second request for a consumer\'s fault');
    assert.equal(logged.length, 1); assert.ok(/a drawing fault/.test(String(logged[0][1])));
  } finally { console.error = oldErr; }
});
