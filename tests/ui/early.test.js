'use strict';
// The page's <head> early forecast request (templates/index.html, between the "early forecast" markers) must ask
// for exactly what the module's first load asks for (resolveInitialState + queryFor); the module takes it only then.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '..', '..', 'templates', 'index.html'), 'utf8');
const a = html.indexOf('// ---- early forecast'), b = html.indexOf('// ---- end early forecast ----');
assert.ok(a > 0 && b > a, 'the early forecast block is marked in the template');
const SWAN = ['51201', '51202'];
const NWPS = ['51003', '46026'];
const RAW = html.slice(html.indexOf('\n', a) + 1, b).replace('{{ initial_state.swan_stations|tojson }}', JSON.stringify(SWAN))
  .replace('{{ initial_state.nwps_stations|tojson }}', JSON.stringify(NWPS))
  .replace('{{ retired_stations|tojson }}', JSON.stringify(['44062', '46088']));
const block = (geo) => RAW.replace('{{ geo_base|tojson }}', JSON.stringify(geo || ''));
const BLOCK = block('');
const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'forecast.js'), 'utf8');
const w = {}; new Function('window', 'URLSearchParams', SRC)(w, URLSearchParams);
const I = w.AllshoreForecast._internals;

// opts: geo (the GEO base), dflt (the saved default station), geoAnswer (a function returning the Worker's response
// promise), stations (the list the stations request answers with, or a rejection), fetchOpts recorded per request
function runHead(search, stored, storage, opts) {
  opts = opts || {};
  const urls = [], fetches = [], timers = [];
  const keys = { 'allshore.settings.v1': stored, 'allshore.defaultStation.v1': opts.dflt };
  const stationsP = opts.stationsReject ? Promise.reject(new TypeError('Failed to fetch'))
    : Promise.resolve({ clone: () => ({ json: () => Promise.resolve(opts.stations === undefined ? [] : opts.stations) }), json: () => Promise.reject(new TypeError('body used: the map reads it later; the head must clone')) });
  stationsP.catch(() => {});
  const ctx = { URLSearchParams, JSON, location: { search }, window: { __early: { stations: stationsP } },
    setTimeout: (fn, ms) => { timers.push({ fn, ms }); return timers.length; },
    localStorage: storage || { getItem: (k) => (k in keys && keys[k] !== undefined ? (typeof keys[k] === 'string' ? keys[k] : JSON.stringify(keys[k])) : null) },
    fetch: (u, o) => { urls.push(u); fetches.push([u, o]); if (opts.geoAnswer && u === (opts.geo || '') + '/') return opts.geoAnswer(); return Promise.resolve({}); } };
  vm.runInNewContext(block(opts.geo), ctx);
  return { get early() { return ctx.window.__early.forecast; }, urls, fetches, timers, ctx, w: ctx.window.__early,
    fire: (ms) => timers.filter((t) => t.ms === ms).forEach((t) => t.fn()) };
}
const settle = () => new Promise((r) => setImmediate(r)).then(() => new Promise((r) => setImmediate(r))).then(() => new Promise((r) => setImmediate(r)));
// what the server hands the module for a GET with this address (index(): tz '' / unit US / model GFS by default)
function server(search) {
  const p = new URLSearchParams(search);
  const station = (p.get('station') || '').trim() || '51201';
  return { station, tz: p.get('tz') || '', unit: p.get('unit') === 'Metric' ? 'Metric' : 'US', model: I.modelOf(p.get('model') || ''), view: 'Table' };
}

const CASES = [
  ['?station=51201', undefined], ['', undefined], ['?station=46001&tz=UTC&unit=Metric', undefined],
  ['?station=51201&model=SWAN&view=Graph', { tz: 'Europe/Lisbon', unit: 'Metric' }], ['?station=51201&tz=', { tz: 'UTC' }],
  ['?station=51201', { tz: 'America/New_York', unit: 'US' }], ['?station=51201&unit=bogus', { unit: 'Metric' }],
  ['?station=51201', '{not json'], ['?station=51201', { tz: 5, unit: null }], ['?station=51201&model=swan', undefined],
  ['?station=51201&tz=Pacific%2FHonolulu&unit=US', { tz: 'UTC', unit: 'Metric' }],
  ['?station=46001&model=SWAN', undefined], ['?station=51202&model=SWAN', { unit: 'Metric' }],
  ['?station=51201&unit=constructor', undefined], ['?station=51201', { unit: 'toString' }],
  ['?station=51003&model=NWPS', undefined], ['?station=51003&model=nwps', { unit: 'Metric' }], ['?station=51201&model=NWPS', undefined],
  ['?station=46026&model=NWPS&tz=UTC', undefined], ['?station=51003&model=SWAN', undefined], ['?station=51003&model=GLWU', undefined],
  ['?station=51003&model=%20Nwps%20', undefined], ['?station=%2051003%20&model=NWPS', undefined],   // trimmed alike (G41 A E02)
];

test('the head asks for exactly the query of the module\'s first load, for every address / saved-settings combination', () => {
  for (const [search, stored] of CASES) {
    const h = runHead(search, stored);
    let st = {}; try { st = typeof stored === 'string' ? JSON.parse(stored) : (stored || {}); } catch (e) { st = {}; }
    const s0 = I.resolveInitialState(search, st, server(search));
    if (s0.model === 'SWAN' && SWAN.indexOf(s0.station) < 0) s0.model = 'GFS';     // the loader's normalise()
    if (s0.model === 'NWPS' && NWPS.indexOf(s0.station) < 0) s0.model = 'GFS';
    const want = I.queryFor(s0);
    assert.equal(h.early.q, want, search + ' / ' + JSON.stringify(stored));
    assert.deepEqual(h.urls, ['/api/forecast?' + want]);
  }
});

test('no early request for ?render=full (the table is in the page); a throwing storage still gets one', () => {
  assert.equal(runHead('?station=51201&render=full').early, undefined);
  const h = runHead('?station=51201', undefined, { getItem: () => { throw new Error('blocked'); } });
  assert.equal(h.early.q, I.queryFor(I.resolveInitialState('?station=51201', {}, server('?station=51201'))));
});

test('the head swallows its own rejection (offline): no unhandled rejection', async () => {
  let rejected = null;
  const ctx = { URLSearchParams, JSON, location: { search: '?station=51201' }, window: { __early: {} }, localStorage: { getItem: () => null },
    fetch: () => { const p = Promise.reject(new TypeError('Failed to fetch')); const then = p.catch.bind(p); p.catch = (fn) => { rejected = fn; return then(fn); }; return p; } };
  vm.runInNewContext(BLOCK, ctx);
  assert.equal(typeof rejected, 'function', 'a catch handler is attached at once');
  await new Promise((r) => setTimeout(r, 10));
});

// ---- plan section 46: the default station and the visitor's area (the geo Worker) ----
const GEO = 'https://geo.example.test';
const LIST = [{ id: '51201', name: 'Waimea', lat: 21.673, lon: -158.117 }, { id: 'HNL01', name: 'Diamond Head', lat: 21.25, lon: -157.8 },
  { id: '46001', name: 'Gulf of Alaska', lat: 56.3, lon: -148.0 }, { id: '52200', name: 'Guam', lat: 13.7, lon: 144.8 },
  { id: 'XFJ1', name: 'Fiji west', lat: -17.5, lon: 179.4 }, { id: 'XFJ2', name: 'Fiji east', lat: -17.5, lon: -179.5 }];
const ok = (body) => Promise.resolve({ ok: true, json: () => Promise.resolve(body) });
function moduleQuery(search, stored, sid, dflt) {
  const s0 = I.resolveInitialState(search, stored || {}, server(search), dflt);
  if (sid) s0.station = sid;
  if (s0.model === 'SWAN' && SWAN.indexOf(s0.station) < 0) s0.model = 'GFS';
  return I.queryFor(s0);
}

test('section 46: with GEO empty nothing changes (no geo request, no station promise), whatever the storage holds', () => {
  for (const [search, stored] of CASES) {
    const h = runHead(search, stored, undefined, { geo: '', stations: LIST });
    assert.ok(!h.urls.some((u) => u.includes('geo.')), search); assert.equal(h.w.station, undefined); assert.equal(h.w.place, undefined);
    assert.equal(h.timers.length, 0, 'no cap timer');
  }
});

test('section 46: a saved default station (the blue star) starts its request at once, before the saved settings and without the Worker; the address still wins; a bad default is ignored', () => {
  const dflt = { id: 'HNL01', lat: 21.25, lon: -157.8, name: 'Diamond Head', at: 1 };
  let h = runHead('', { tz: 'UTC', unit: 'Metric' }, undefined, { geo: GEO, dflt, stations: LIST });
  assert.equal(h.early.q, moduleQuery('', { tz: 'UTC', unit: 'Metric' }, null, dflt)); assert.ok(h.early.q.startsWith('station=HNL01&'));
  assert.deepEqual(h.urls, ['/api/forecast?' + h.early.q], 'no geo request'); assert.equal(h.w.station, undefined);
  h = runHead('?station=46001', undefined, undefined, { geo: GEO, dflt, stations: LIST });
  assert.ok(h.early.q.startsWith('station=46001&'), 'the address wins over the default'); assert.equal(h.urls.length, 1);
  h = runHead('?station=HNL01&model=SWAN', undefined, undefined, { geo: GEO, dflt: { id: '51201' }, stations: LIST });
  assert.equal(h.early.q, moduleQuery('?station=HNL01&model=SWAN', {}, null, { id: '51201' }), 'SWAN off a SWAN station normalised as before');
  for (const bad of [{ id: 'HNL 01' }, { id: '' }, { id: 7 }, { id: 'a'.repeat(33) }, 'HNL01', [], { lat: 1 }, '{bad json']) {
    const g = runHead('', undefined, undefined, { geo: '', dflt: bad, stations: LIST });
    assert.ok(g.early.q.startsWith('station=51201&'), JSON.stringify(bad));
  }
  const dash = runHead('', undefined, undefined, { geo: GEO, dflt: { id: 'NW-HFO51' }, stations: LIST });   // NOAA ids carry dashes
  assert.ok(dash.early.q.startsWith('station=NW-HFO51&')); assert.equal(dash.urls.length, 1);
  const padded = runHead('', undefined, undefined, { geo: GEO, dflt: { id: ' HNL01 ' }, stations: LIST });   // the one id rule: trimmed (G32 A-P3-2)
  assert.ok(padded.early.q.startsWith('station=HNL01&')); assert.equal(padded.early.q, moduleQuery('', {}, null, { id: ' HNL01 ' }));
});

test('section 46: with GEO set and no station, the Worker is asked without credentials, the nearest station of the list is chosen, its request started, and station / place resolve to it', async () => {
  let pos = { lat: 13.48, lon: 144.75, city: 'Hagatna' };
  const h = runHead('', { unit: 'Metric' }, undefined, { geo: GEO, stations: LIST, geoAnswer: () => ok(pos) });
  assert.equal(h.early, undefined, 'no forecast request before the answer');
  assert.deepEqual(h.fetches.map((f) => [f[0], f[1] && f[1].credentials]), [[GEO + '/', 'omit']], 'the Worker asked once, without credentials');
  assert.equal(typeof h.w.station.then, 'function'); assert.equal(typeof h.w.place.then, 'function');
  assert.deepEqual(h.timers.map((t) => t.ms), [2000], 'the 2-s cap');
  const sid = await h.w.station, place = await h.w.place;
  assert.equal(sid, '52200'); assert.deepEqual(Object.assign({}, place), { lat: 13.48, lon: 144.75, sid: '52200', slat: 13.7, slon: 144.8 });
  assert.equal(h.early.q, moduleQuery('', { unit: 'Metric' }, '52200'), 'the head asks for exactly the module\'s deferred first query');
  assert.deepEqual(h.urls, [GEO + '/', '/api/forecast?' + h.early.q]);
  // the date line is no seam: a visitor at 179.98 E is nearest the station at 179.5 W (0.52 deg), not the ones at 179.4 E (0.58) or 170 E
  pos = { lat: -17.6, lon: 179.98 };
  const dl = runHead('', undefined, undefined, { geo: GEO, stations: LIST.concat([{ id: 'FAR', lat: -17.5, lon: 170 }]), geoAnswer: () => ok(pos) });
  assert.equal(await dl.w.station, 'XFJ2');
});

test('an Oahu visitor opens on Waimea Bay 51201 though another station is nearer (owner, 2026-10-18); the map fits it; off the island (and without 51201 in the list) the nearest', async () => {
  const L2 = LIST.concat([{ id: '51211', name: 'Pearl Harbor Entrance', lat: 21.3, lon: -157.96 }, { id: '51208', name: 'Hanalei', lat: 22.28, lon: -159.57 },
    { id: '51203', name: 'Kaumalapau', lat: 20.79, lon: -157.0 }]);
  const at = async (pos, list) => {
    const h = runHead('', undefined, undefined, { geo: GEO, stations: list || L2, geoAnswer: () => ok(pos) });
    const sid = await h.w.station, place = await h.w.place;
    return { sid, place: place && Object.assign({}, place), q: h.early && h.early.q };
  };
  for (const [name, lat, lon] of [['Mililani', 21.4504, -158.015], ['Honolulu', 21.3069, -157.8583], ['Kailua', 21.4022, -157.7394], ['Haleiwa', 21.5928, -158.1034],
    ['Kapolei', 21.3356, -158.0581], ['Hawaii Kai', 21.2906, -157.7012], ['the box, south-west corner', 21.2, -158.32], ['the box, north-east corner', 21.76, -157.6]]) {
    const r = await at({ lat, lon, city: name });
    assert.equal(r.sid, '51201', name);
    assert.deepEqual(r.place, { lat, lon, sid: '51201', slat: 21.673, slon: -158.117 }, name + ': the first view fits Waimea Bay');
    assert.equal(r.q, moduleQuery('', {}, '51201'), name + ': the module\'s deferred first query');
  }
  // from Mililani 51211 is nearer (about 18 km against 26 km): the preference is the island's, not the distance
  const noWaimea = L2.filter((s) => s.id !== '51201');
  assert.equal((await at({ lat: 21.4504, lon: -158.015 }, noWaimea)).sid, '51211', 'no 51201 in the list: the nearest');
  assert.equal((await at({ lat: 21.4504, lon: -158.015 }, L2.concat([{ id: '51201', lat: 'x', lon: -158 }]).filter((s, i, a) => s.id !== '51201' || i === a.length - 1))).sid, '51211', 'a 51201 without a position: the nearest');
  for (const [name, lat, lon, want] of [['Lihue, Kauai', 21.9789, -159.3711, '51208'], ['Molokai', 21.09, -157.02, '51203'], ['just south of the box', 21.19, -157.9, 'HNL01'],
    ['just east of the box', 21.33, -157.59, 'HNL01'], ['Guam', 13.48, 144.75, '52200']]) {
    assert.equal((await at({ lat, lon })).sid, want, name);
  }
});

test('section 46: every failure lands on 51201 after the race: a slow Worker (the 2-s cap), a refused / broken / empty answer, a failed station list, a list that is not a list; late answers change nothing', async () => {
  const slow = runHead('', undefined, undefined, { geo: GEO, stations: LIST, geoAnswer: () => new Promise(() => {}) });
  await settle(); assert.equal(slow.early, undefined, 'still waiting');
  slow.fire(2000); assert.equal(await slow.w.station, '51201'); assert.equal(await slow.w.place, null);
  assert.ok(slow.early.q.startsWith('station=51201&'));
  const cases = [
    () => Promise.reject(new TypeError('Failed to fetch')), () => Promise.resolve({ ok: false, status: 503, json: () => Promise.resolve({}) }),
    () => Promise.resolve({ ok: true, json: () => Promise.reject(new SyntaxError('bad json')) }), () => ok({ lat: null, lon: -157 }),
    () => ok({ lat: '21.3', lon: -157 }), () => ok({ lat: 95, lon: 0 }), () => ok({ lat: 21, lon: 181 }), () => ok(null), () => ok('Honolulu')];
  for (const geoAnswer of cases) {
    const h = runHead('', undefined, undefined, { geo: GEO, stations: LIST, geoAnswer });
    assert.equal(await h.w.station, '51201', String(geoAnswer)); assert.equal(await h.w.place, null); assert.equal(h.urls.length, 2);
  }
  for (const stations of [[], [{ id: 'X', lat: 'a', lon: 1 }], [{ lat: 21, lon: -157 }], 'nope', null]) {
    const h = runHead('', undefined, undefined, { geo: GEO, stations, geoAnswer: () => ok({ lat: 21.3, lon: -157.86 }) });
    assert.equal(await h.w.station, '51201', JSON.stringify(stations));
  }
  const rej = runHead('', undefined, undefined, { geo: GEO, stationsReject: true, geoAnswer: () => ok({ lat: 21.3, lon: -157.86 }) });
  assert.equal(await rej.w.station, '51201');
  // a late answer after the cap changes nothing (the request for 51201 is the one the module takes)
  let late = null; const lateH = runHead('', undefined, undefined, { geo: GEO, stations: LIST, geoAnswer: () => new Promise((r) => { late = r; }) });
  lateH.fire(2000); assert.equal(await lateH.w.station, '51201'); const q0 = lateH.early.q;
  late({ ok: true, json: () => Promise.resolve({ lat: 21.3, lon: -157.86 }) }); await settle();
  assert.equal(lateH.early.q, q0); assert.equal(lateH.urls.length, 2);
});

test('section 46: a station in the address, or ?render=full, never asks the Worker even with GEO set', () => {
  const h = runHead('?station=46001', undefined, undefined, { geo: GEO, stations: LIST, geoAnswer: () => ok({ lat: 1, lon: 1 }) });
  assert.deepEqual(h.urls, ['/api/forecast?' + h.early.q]); assert.equal(h.w.station, undefined);
  const f = runHead('?render=full', undefined, undefined, { geo: GEO, stations: LIST, geoAnswer: () => ok({ lat: 1, lon: 1 }) });
  assert.deepEqual(f.urls, []); assert.equal(f.w.station, undefined);
});

test('a stored default the site has removed (RETIRED, the server\'s station_blank.json) opens like no default: forgotten in the head, before the module reads it (G35 B-8)', () => {
  const mk = (id) => {
    const store = { 'allshore.defaultStation.v1': JSON.stringify({ id }) }, removed = [];
    return { removed, store, getItem: (k) => (k in store ? store[k] : null), removeItem: (k) => { removed.push(k); delete store[k]; } };
  };
  const s1 = mk('44062'), h = runHead('', undefined, s1);
  assert.deepEqual(s1.removed, ['allshore.defaultStation.v1']); assert.equal(h.w.retired, '44062', 'the page says so once');
  assert.equal(new URLSearchParams(h.early.q).get('station'), '51201', 'no geo: the old fallback');
  const s2 = mk('46088'), g = runHead('', undefined, s2, { geo: 'https://geo.test' });
  assert.ok(g.urls.indexOf('https://geo.test/') >= 0, 'with geo: the visitor\'s area, as without a default');
  const s3 = mk('46026'), k = runHead('', undefined, s3);
  assert.deepEqual(s3.removed, []); assert.equal(k.w.retired, undefined); assert.equal(new URLSearchParams(k.early.q).get('station'), '46026');
  const s4 = mk('44062'), a = runHead('?station=44062', undefined, s4);
  assert.equal(new URLSearchParams(a.early.q).get('station'), '44062', 'a link still opens the removed station (its note)');
});
