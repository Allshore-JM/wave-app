'use strict';
// Plan section 52 B: the page's "buoy links" block (templates/index.html, between its markers) run with fakes: the live
// window's "Forecast for this point", the forecast window's "Live buoy", and the live dots' touch pads; with the G35 fix
// round: the alias table, links back to where the visitor came from, far links moving the map, the plain words to 2 km,
// the narrow text, the screen-reader label, a removed station's live buoy, the twin under the dot, one pad pass a round.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

const TPL = fs.readFileSync(path.join(__dirname, '..', '..', 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');
const A = TPL.indexOf('    // ---- buoy links (plan section 52'), B = TPL.indexOf('    // ---- end buoy links ----');
assert.ok(A > 0 && B > A, 'the block markers');
const BLOCK = TPL.slice(A, B);

function el(id) {
  const e = { id, hidden: true, textContent: '', title: '', value: '', listeners: {}, attrs: {}, sets: 0,
    addEventListener(t, fn) { (this.listeners[t] = this.listeners[t] || []).push(fn); },
    setAttribute(k, v) { this.attrs[k] = String(v); }, removeAttribute(k) { delete this.attrs[k]; }, getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; },
    click(detail) { (this.listeners.click || []).forEach((fn) => fn({ detail: detail === undefined ? 1 : detail })); } };
  return e;
}
// a marker whose element records the pad
function marker() {
  const st = { props: {}, sets: 0, setProperty(k, v) { this.props[k] = v; this.sets++; } };
  return { style: st, getElement: () => ({ style: st }) };
}

function boot(opt = {}) {
  const els = {}; ['lwForecast', 'lwLinks', 'fwLive', 'unit', 'station'].forEach((id) => { els[id] = el(id); });
  els.unit.value = opt.unit || 'US';
  const docL = {}, events = [], focused = [], opened = [], pans = [], winL = {};
  const document = {
    getElementById: (id) => els[id] || null,
    addEventListener: (t, fn) => { (docL[t] = docL[t] || []).push(fn); },
    dispatchEvent: (e) => { events.push(e); (docL[e.type] || []).forEach((fn) => fn(e)); },
  };
  class CustomEvent { constructor(type, init) { this.type = type; this.detail = (init || {}).detail; } }
  const media = { coarse: opt.coarse !== false, narrow: !!opt.narrow };
  const window = {
    matchMedia: (q) => ({ matches: q === '(any-pointer: coarse)' ? media.coarse : q === '(max-width: 400px)' ? media.narrow : false }),
    addEventListener: (t, fn) => { (winL[t] = winL[t] || []).push(fn); },
  };
  const layers = new Set(['forecast', 'points']);
  const center = { lat: 0, lng: opt.centerLng || 0 };
  const map = {
    latLngToContainerPoint: ([lat, lng]) => ({ x: lng * 1000, y: -lat * 1000 }),   // 1 px = 0.001 degree
    hasLayer: (l) => layers.has(l), getCenter: () => center, panTo: (ll) => pans.push(ll),
  };
  const env = {
    forecastStationsData: opt.forecast || [], liveStationsData: opt.live || [], liveDrawn: opt.drawn || [],
    tideDrawn: opt.tides || [], windDrawn: opt.winds || [], savedPointsData: opt.points || [], liveShownStation: null,
  };
  const copies = opt.copies || ((list) => list.map((s) => ({ s, lng: s.lon, o: 0 })));
  const api = new Function('document', 'window', 'CustomEvent', 'map', 'env', 'forecastLayer', 'pointLayer',
    'getSelectedUnit', 'buoyDisplayLabel', 'saveLayerVisibility', 'openLiveBuoy', 'focusWindowHeader', 'visibleCopies',
    'with (env) {\n' + BLOCK + '\nreturn { linkFor, linkKey, linkId, linkAway, forecastLinkFor, liveLinkFor, syncBuoyLinks, liveTargets, requestLiveTargets,\n' +
    '  LINK_KM, LINK_ID_KM, LINK_PLAIN_KM, LIVE_TAP_MAX, setLive: function (s) { env.liveShownStation = s; } };\n}')(
    document, window, CustomEvent, map, env, 'forecast', 'points',
    () => els.unit.value, (s) => String(s.id).replace(/^ndbc:/, ''), () => {},
    (s) => { opened.push(s); env.liveShownStation = s; (docL['__live'] || []).forEach((fn) => fn()); },   // the page shows it and syncs
    (id) => focused.push(id), copies);
  docL.__live = [() => api.syncBuoyLinks()];
  return { api, els, env, events, focused, opened, pans, layers, media, center, winL,
    fire: (t, d) => (docL[t] || []).forEach((fn) => fn({ detail: d })) };
}

// 41113 (Cape Canaveral nearshore) and its forecast point 0.29 km away; an unrelated forecast point 0.2 km from it
const BUOY = { id: 'ndbc:41113', lat: 28.400, lon: -80.534, name: 'Canaveral Nearshore, FL' };
const TWIN = { id: '41113', lat: 28.4026, lon: -80.534, name: 'Canaveral Nearshore' };   // 0.29 km north
const OTHER = { id: 'TPC10', lat: 28.4018, lon: -80.534 };           // 0.2 km north: nearer, but another id

test('linkFor: the same id within 400 km beats a nearer station of another id; else the nearest within 1 km; never beyond', () => {
  const b = boot({ forecast: [OTHER, TWIN], live: [BUOY] }), A = b.api;
  const l = A.linkFor(BUOY, [OTHER, TWIN]);
  assert.equal(l.s, TWIN); assert.ok(Math.abs(l.km - 0.289) < 0.01, String(l.km));
  assert.equal(A.linkFor({ id: 'cdip:999', lat: 28.400, lon: -80.534 }, [OTHER, TWIN]).s, OTHER, 'no id: the nearest within 1 km');
  assert.equal(A.linkFor({ id: 'x:1', lat: 28.400, lon: -80.534 }, [{ id: 'far', lat: 28.410, lon: -80.534 }]), null, '1.1 km: none');
  const moved = { id: '41002', lat: 31.0, lon: -74.8 }, live41002 = { id: 'ndbc:41002', lat: 32.3, lon: -77.6 };
  assert.equal(A.linkFor(live41002, [moved]).s, moved, 'a moved buoy (NOAA\'s point ~300 km away) keeps its own forecast point');
  assert.equal(A.linkFor(live41002, [{ id: '41002', lat: 36.5, lon: -77.6 }]), null, '467 km: too far even for its own id');
  assert.equal(A.linkKey('ndbc:hnl10'), 'HNL10'); assert.equal(A.linkKey('51201'), '51201');
  assert.equal(A.linkFor({ id: 'a', lat: NaN, lon: 1 }, [TWIN]), null);
  assert.equal(A.LINK_KM, 1.0); assert.equal(A.LINK_ID_KM, 400); assert.equal(A.LINK_PLAIN_KM, 2.0);
});

test('the alias table (G35 decision 3, B-4): a buoy another network lists under its own number links to its forecast point both ways', () => {
  const majuro = { id: '52201', lat: 7.083, lon: 171.392 }, cdip = { id: 'cdip:163', lat: 7.1219, lon: 171.392 };   // 4.3 km apart
  const m2 = { id: '62091', lat: 53.48, lon: -5.425 }, mi = { id: 'mi-ie:M2', lat: 53.495, lon: -5.425 };
  const brit = { id: '62163', lat: 47.55, lon: -8.47 }, cm = { id: 'cmems:GL_TS_MO_6200163', lat: 47.578, lon: -8.47 };
  const b = boot({ forecast: [majuro, m2, brit], live: [cdip, mi, cm] }), A = b.api;
  assert.equal(A.linkId('cdip:163'), '52201'); assert.equal(A.linkId('MI-IE:m6'), '62095'); assert.equal(A.linkId('cmems:IR_TS_MO_6200084'), '62084');
  assert.equal(A.linkId('cmems:NO_TS_MO_6202108'), 'NO_TS_MO_6202108', 'not the 6200NNN form: its own id');
  for (const [live, f] of [[cdip, majuro], [mi, m2], [cm, brit]]) {
    assert.equal(A.linkFor(live, b.env.forecastStationsData).s, f, live.id);
    assert.equal(A.linkFor(f, b.env.liveStationsData).s, live, f.id + ' back');
  }
  assert.equal(A.linkFor({ id: 'cdip:164', lat: 7.1219, lon: 171.392 }, [majuro]), null, 'another CDIP number: no alias');
});

test('the live window: "Forecast for this point" for a twin, the id and distance for a moved buoy, nothing without a link; a click loads it in the forecast window', () => {
  const far = { id: '41041', lat: 14.6, lon: -46.0 }, liveFar = { id: 'ndbc:41041', lat: 14.33, lon: -46.08 };
  const b = boot({ forecast: [TWIN, far], live: [BUOY, liveFar] }), A = b.api, E = b.els;
  A.setLive(BUOY); A.syncBuoyLinks();
  assert.equal(E.lwLinks.hidden, false); assert.equal(E.lwForecast.textContent, 'Forecast for this point');
  assert.equal(E.lwForecast.title, 'Canaveral Nearshore', 'the title adds the station\'s name (B-6)'); assert.equal(E.lwForecast.getAttribute('aria-label'), null);
  E.lwForecast.click();
  assert.equal(E.station.value, '41113');
  const ev = b.events.filter((e) => e.type === 'allshore:station').pop();
  assert.deepEqual(ev.detail, { sid: '41113', source: 'map' }, 'as a click on its dot (Buoy Local, the window expands)');
  assert.deepEqual(b.focused, ['fwHeader'], 'a pointer click moves the focus into the forecast window (V7-2)'); assert.deepEqual(b.pans, [], 'a twin: the map stays');
  E.lwForecast.click(0); assert.deepEqual(b.focused, ['fwHeader', 'fwHeader'], 'a keyboard press too');
  A.setLive(liveFar); A.syncBuoyLinks();
  assert.equal(E.lwForecast.textContent, 'Forecast point 41041 · 19 mi away', '10 and over: whole units');
  assert.equal(E.lwForecast.getAttribute('aria-label'), 'Forecast point 41041, 19 miles away', 'in words for a screen reader (B-6)');
  assert.equal(E.lwForecast.title, '', 'no name: no title');
  E.unit.value = 'Metric'; A.syncBuoyLinks(); assert.equal(E.lwForecast.textContent, 'Forecast point 41041 · 31 km away');
  assert.equal(E.lwForecast.getAttribute('aria-label'), 'Forecast point 41041, 31 kilometres away');
  b.env.forecastStationsData = [TWIN, { id: '41041', lat: 14.375, lon: -46.08 }]; A.syncBuoyLinks();
  assert.equal(E.lwForecast.textContent, 'Forecast point 41041 · 5.0 km away', 'under 10: one decimal');
  A.setLive({ id: 'aodn:Tantabiddi', lat: -21.9, lon: 113.9 }); A.syncBuoyLinks();
  assert.equal(E.lwLinks.hidden, true, 'no forecast point near: no button');
  A.setLive(null); A.syncBuoyLinks(); assert.equal(E.lwLinks.hidden, true);
});

test('F7: the plain words up to 2 km (the same spot to a visitor), the distance rounded before the 10 test; the narrow text on a phone (B-5)', () => {
  const live = { id: 'ndbc:46014', lat: 39.235, lon: -123.974 };
  const at = (km) => ({ id: '46014', lat: 39.235 + km / 111.195, lon: -123.974 });
  const b = boot({ forecast: [at(1.5)], live: [live] }), A = b.api, E = b.els;
  A.setLive(live); A.syncBuoyLinks();
  assert.equal(E.lwForecast.textContent, 'Forecast for this point', '1.5 km, its own id: the plain words');
  b.env.forecastStationsData = [at(2.5)]; A.syncBuoyLinks();
  assert.equal(E.lwForecast.textContent, 'Forecast point 46014 · 1.6 mi away');
  assert.equal(A.linkAway(9.96 * 1.609344), '10 mi away', '9.96 rounds to 10: whole units'); assert.equal(A.linkAway(9.94 * 1.609344), '9.9 mi away');
  b.media.narrow = true; A.syncBuoyLinks();
  assert.equal(E.lwForecast.textContent, 'Forecast point · 1.6 mi', 'narrow: the distance only');
  assert.equal(E.lwForecast.getAttribute('aria-label'), 'Forecast point 46014, 1.6 miles away', 'the label keeps the id');
  assert.ok((b.winL.resize || []).length === 1, 'a resize re-writes the text');
});

test('the forecast window: "Live buoy" follows the station shown (a pick or a forecast landing); never for My points or an unknown id; a click opens the buoy', () => {
  const b = boot({ forecast: [TWIN, { id: '51201', lat: 21.67, lon: -158.12 }], live: [BUOY] }), E = b.els;
  assert.equal(E.fwLive.hidden, true, 'nothing shown yet');
  b.fire('allshore:forecast', { station: '41113' });
  assert.equal(E.fwLive.hidden, false); assert.equal(E.fwLive.textContent, 'Live buoy'); assert.equal(E.fwLive.title, 'Canaveral Nearshore, FL');
  E.fwLive.click(); assert.deepEqual(b.opened, [BUOY]);
  assert.deepEqual(b.focused, ['lwHeader'], 'a pointer click moves the focus into the live window: Escape then closes it, not the window behind (V7-2)');
  E.fwLive.click(0); assert.deepEqual(b.focused, ['lwHeader', 'lwHeader']);
  b.fire('allshore:station', { sid: '51201' }); assert.equal(E.fwLive.hidden, true, 'no live buoy near 51201 in this list');
  b.fire('allshore:station', { sid: 'pt_28400N_80534W' }); assert.equal(E.fwLive.hidden, true, 'one of My points: never');
  b.fire('allshore:station', { sid: 'NOPE' }); assert.equal(E.fwLive.hidden, true, 'not in the list: never');
  b.fire('allshore:forecast', {}); assert.equal(E.fwLive.hidden, true, 'an event without a station changes nothing');
  const n = b.opened.length; E.fwLive.click(); assert.equal(b.opened.length, n, 'no link: a click opens nothing');
});

test('a removed station (its answer has its position, not a dot): the sheltered note offers its live buoy (G35 B-8)', () => {
  const live = { id: 'ndbc:46088', lat: 48.333, lon: -123.167 };
  const b = boot({ forecast: [TWIN], live: [BUOY, live] }), E = b.els;
  b.fire('allshore:station', { sid: '46088' }); assert.equal(E.fwLive.hidden, true, 'no dot, no position yet');
  b.fire('allshore:forecast', { station: '46088', lat: 48.333, lon: -123.167, final: true, reason: 'sheltered' });
  assert.equal(E.fwLive.hidden, false, 'asked again with the answer\'s position'); assert.equal(E.fwLive.textContent, 'Live buoy');
  E.fwLive.click(); assert.deepEqual(b.opened, [live]);
  assert.equal(E.lwLinks.hidden, false, 'its window links back to where the visitor came from'); assert.equal(E.lwForecast.textContent, 'Forecast for this point');
});

test('decision 3: a window opened by the other\'s link links back to where the visitor came from (B-3), until it shows another station', () => {
  // live 44088 and its nearest forecast point 44014 (0.33 km); live 44014 at 0.82 km from the point (the same id wins)
  const F = { id: '44014', lat: 36.611, lon: -74.836 }, L88 = { id: 'ndbc:44088', lat: 36.614, lon: -74.836 }, L14 = { id: 'ndbc:44014', lat: 36.6184, lon: -74.836 };
  const b = boot({ forecast: [F], live: [L88, L14] }), A = b.api, E = b.els;
  A.setLive(L88); A.syncBuoyLinks(); assert.equal(E.lwForecast.textContent, 'Forecast for this point');
  E.lwForecast.click();                                                      // the forecast window shows 44014
  assert.equal(E.fwLive.hidden, false); E.fwLive.click();
  assert.deepEqual(b.opened, [L88], 'back to 44088, not the same-id buoy 44014');
  assert.equal(E.lwForecast.textContent, 'Forecast for this point'); E.lwForecast.click();
  assert.equal(E.station.value, '44014');
  b.fire('allshore:station', { sid: '44014' }); E.fwLive.click(); assert.deepEqual(b.opened, [L88, L88], 'still the way back');
  // another pick of the same station by the visitor: the usual link again
  b.fire('allshore:station', { sid: 'X' }); b.fire('allshore:station', { sid: '44014' }); E.fwLive.click();
  assert.deepEqual(b.opened[2], L14, 'a fresh pick: the same id');
  // the live window: after the forecast link opened 44014's buoy, its link goes back to 44014; another buoy: its own
  A.setLive(L88); A.syncBuoyLinks(); assert.equal(E.lwLinks.hidden, false);
});

test('decision 4: a link to a station farther than 1 km moves the map there, on the copy of the world in view', () => {
  const far = { id: '41041', lat: 14.6, lon: -46.0 }, liveFar = { id: 'ndbc:41041', lat: 14.33, lon: -46.08 };
  const b = boot({ forecast: [TWIN, far], live: [BUOY, liveFar], centerLng: 300 }), A = b.api, E = b.els;
  A.setLive(liveFar); A.syncBuoyLinks(); E.lwForecast.click();
  assert.deepEqual(b.pans, [[14.6, 314.0]], 'the copy east of the date line the map shows');
  b.fire('allshore:station', { sid: '41041' }); E.fwLive.click();
  assert.equal(b.pans.length, 2); assert.equal(b.pans[1][0], 14.33);
  A.setLive(BUOY); A.syncBuoyLinks(); E.lwForecast.click(); assert.equal(b.pans.length, 2, 'a twin: no move');
});

test('the links follow new lists (the cache is keyed by the lists themselves)', () => {
  const b = boot({ forecast: [TWIN], live: [] }), A = b.api;
  b.fire('allshore:forecast', { station: '41113' }); assert.equal(b.els.fwLive.hidden, true, 'no live list yet');
  b.env.liveStationsData = [BUOY]; A.syncBuoyLinks(); assert.equal(b.els.fwLive.hidden, false, 'the live list landed');
  b.env.forecastStationsData = []; A.syncBuoyLinks(); assert.equal(b.els.fwLive.hidden, true, 'a new forecast list without it');
});

const mk = () => marker();
const at = (id, lat, lon, active) => ({ marker: mk(), lat, lng: lon, s: { id, lat, lon }, active: !!active });

test('touch pads: half the way to the nearest other icon (8 px at most); a flag on the buoy leaves the dot as it was; the twin forecast dot under the dot does not count; mice: nothing', () => {
  // live dots at the coordinates below; the dot is drawn 6.5 px right of its point (7.5 px when active)
  const lone = at('ndbc:1', 10, 10), withFlag = at('ndbc:2', 20, 20), nearTide = at('ndbc:3', 30, 30), twinOnly = at('ndbc:41113', 28.4026, -80.5405),
    nearOther = at('ndbc:5', 40, 40), active = at('ndbc:6', 50, 50, true), pair1 = at('ndbc:7', 60, 60), pair2 = at('ndbc:8', 60, 60.03);
  const drawn = [lone, withFlag, nearTide, twinOnly, nearOther, active, pair1, pair2];
  const twinUnder = { id: '41113', lat: 28.4026, lon: -80.534 };                 // the twin's dot at the live dot's centre
  const b = boot({
    forecast: [twinUnder, { id: 'F5', lat: 40, lon: 40.0215 }],              // F5: 15 px from ndbc:5's dot centre (6.5 + 15)
    live: drawn.map((d) => d.s), drawn,
    winds: [{ s: { lat: 20, lon: 20 }, lng: 20 }],                         // the buoy's own flag: 6.5 px from its dot
    tides: [{ s: { lat: 30, lon: 30.0265 }, lng: 30.0265, off: [0, 0] }],    // 20 px right of ndbc:3's dot centre
    points: [{ id: 'pt_x', lat: 50, lon: 50.0275 }],                       // 20 px right of the active dot's centre (7.5 + 20)
  });
  b.api.liveTargets();
  const pad = (d) => d.marker.style.props['--live-tap'];
  assert.equal(pad(lone), '8px', 'nothing near: the full pad');
  assert.equal(pad(withFlag), '0px', 'its own wind flag 6.5 px away: as before (the flag stays reachable)');
  assert.equal(pad(nearTide), '4px', 'a tide icon 20 px away: floor(10 - 5.5)');
  assert.equal(pad(twinOnly), '8px', 'its own forecast point under the dot does not count (the window links it)');
  assert.equal(pad(nearOther), '2px', 'another forecast point 15 px away: floor(7.5 - 5.5)');
  assert.equal(pad(active), '2px', 'the active dot (15 px) and one of My points 20 px away: floor(10 - 7.5)');
  assert.equal(pad(pair1), '8px'); assert.equal(pad(pair2), '8px', 'two live dots 30 px apart: floor(15 - 5.5) capped at 8');
  const sets = drawn.map((d) => d.marker.style.sets);
  b.api.liveTargets(); assert.deepEqual(drawn.map((d) => d.marker.style.sets), sets, 'written only when it changes');
  b.layers.delete('forecast'); b.api.liveTargets(); assert.equal(pad(nearOther), '8px', 'the forecast layer unticked: its dots do not count');
  const m = boot({ coarse: false, live: [lone.s], drawn: [at('ndbc:1', 10, 10)] });
  m.api.liveTargets(); assert.equal(m.env.liveDrawn[0].marker.style.sets, 0, 'a mouse: no pad written');
});

test('B-12: the twin counts like any icon once the two dots part (zoomed in); B19: an earlier dot counts for a later one; B23: a forecast copy is measured where it is drawn', () => {
  // at 80 N a pixel of this fake map is 19 m: the twin 16 px from the dot's centre is 0.43 km away (a twin: within 1 km)
  const L = at('ndbc:41113', 80, 10), twin = { id: '41113', lat: 80, lon: 10.0225 };
  const b = boot({ forecast: [twin], live: [L.s], drawn: [L] });
  assert.ok(b.api.forecastLinkFor(L.s).km < b.api.LINK_KM, 'still its twin');
  b.api.liveTargets();
  assert.equal(L.marker.style.props['--live-tap'], '2px', 'apart: half the way (floor(8 - 5.5))');
  const near = { id: '41113', lat: 80, lon: 10.0077 };                        // 1.2 px from the dot's centre: under it
  const L2 = at('ndbc:41113', 80, 10), c = boot({ forecast: [near], live: [L2.s], drawn: [L2] });
  c.api.liveTargets(); assert.equal(L2.marker.style.props['--live-tap'], '8px', 'under the dot: left out');
  const first = at('ndbc:A', 10, 10), second = at('ndbc:B', 10, 10.012);         // 12 px apart
  const d = boot({ live: [first.s, second.s], drawn: [first, second] });
  d.api.liveTargets();
  assert.equal(second.marker.style.props['--live-tap'], '0px', 'the later dot sees the earlier one (floor(6 - 5.5))');
  // a live dot drawn on the copy east of the date line; a forecast dot of the same copy 15 px from it
  const east = at('ndbc:E', 0, 360.0), f = { id: 'FE', lat: 0, lon: 0.0215 };
  const e = boot({ forecast: [f], live: [east.s], drawn: [east], copies: (list) => list.map((s) => ({ s, lng: s.lon + 360, o: 1 })) });
  e.api.liveTargets(); assert.equal(east.marker.style.props['--live-tap'], '2px', 'measured on the drawn copy');
});

test('K-3: one pad pass a round of rebuilds, however many ask', async () => {
  const one = at('ndbc:1', 10, 10), b = boot({ live: [one.s], drawn: [one] });
  let passes = 0;
  const real = one.marker.getElement;
  one.marker.getElement = () => { passes++; return real(); };
  b.api.requestLiveTargets(); b.api.requestLiveTargets(); b.api.requestLiveTargets();
  assert.equal(passes, 0, 'not on the rebuild\'s own stack');
  await Promise.resolve(); await Promise.resolve();
  assert.equal(passes, 1, 'once after the round');
  b.api.requestLiveTargets(); await Promise.resolve(); await Promise.resolve(); assert.equal(passes, 2, 'the next round asks again');
});

test('the page wiring: the block between its markers, the two buttons, the canvas renderer, the pad CSS, the phone order, the marker click through openLiveBuoy, the syncs after each list lands', () => {
  assert.ok(TPL.includes('<button type="button" id="fwLive" class="btn btn-sm btn-outline-primary" hidden>Live buoy</button>'));
  assert.ok(TPL.includes('<div id="lwLinks" class="lw-links" hidden><button type="button" id="lwForecast" class="btn btn-sm btn-outline-primary">Forecast for this point</button></div>'));
  assert.ok(TPL.indexOf('id="lwLinks"') > TPL.indexOf('<div id="lwBody" class="fw-body">'), 'first in the live window\'s body');
  assert.ok(TPL.includes("    const coarseQuery = window.matchMedia ? window.matchMedia('(any-pointer: coarse)') : null;"));
  assert.ok(TPL.includes('    const mapRenderer = new NearestCanvas({ tolerance: coarsePointer() ? COARSE_TOL : 0 });'));
  assert.ok(TPL.includes('      renderer: mapRenderer,'));
  assert.ok(TPL.includes("      mapRenderer.options.tolerance = coarsePointer() ? COARSE_TOL : 0;\n      refreshWrappedMarkerCopies(true);"), 'the reach follows the query');
  assert.ok(TPL.includes("@media (any-pointer: coarse) { .live-buoy-dot::after { content: ''; position: absolute; inset: calc(-2px - var(--live-tap, 0px)); border-radius: 50%; } }"));
  assert.ok(TPL.includes('@media (any-pointer: coarse) { #lwForecast, #fwLive { min-height: 44px; } }'), 'finger-sized links (B-5)');
  assert.ok(!/@media \(pointer: coarse\)/.test(TPL), 'every touch rule on any touch screen (B-7)');
  assert.ok(TPL.includes('.fwin.fw-front:not(.fw-min) { z-index: 3510; }'));
  assert.ok(/marker\.on\('click', function \(e\) \{\n\s+if \(window\.AllshoreTools[^\n]+\n\s+openLiveBuoy\(s\);\n/.test(TPL), 'the dot opens the buoy through openLiveBuoy');
  assert.ok(TPL.includes("liveShownStation = station;\n      if (typeof syncBuoyLinks === 'function') syncBuoyLinks();"));
  assert.ok(TPL.includes("      if (typeof requestLiveTargets === 'function') requestLiveTargets();   // the live dots' pads after every round (plan section 52)"));
  assert.ok(TPL.includes("          rebuildForecastMarkers(true);\n          if (typeof syncBuoyLinks === 'function') syncBuoyLinks();"), 'B25: after the forecast list lands');
  assert.ok(TPL.includes("          rebuildLiveBuoyMarkers(!info.same);        // an unchanged list keeps its markers (no rebuild, no flash)\n          if (typeof syncBuoyLinks === 'function') syncBuoyLinks();"), 'B26: after the live list lands');
});
