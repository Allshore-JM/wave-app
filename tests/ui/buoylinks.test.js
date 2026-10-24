'use strict';
// Plan section 52 B: the page's "buoy links" block (templates/index.html, between its markers) run with fakes: the live
// window's "Forecast for this point", the forecast window's "Live buoy", and the live dots' touch pads.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

const TPL = fs.readFileSync(path.join(__dirname, '..', '..', 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');
const A = TPL.indexOf('    // ---- buoy links (plan section 52'), B = TPL.indexOf('    // ---- end buoy links ----');
assert.ok(A > 0 && B > A, 'the block markers');
const BLOCK = TPL.slice(A, B);

function el(id) {
  const e = { id, hidden: true, textContent: '', title: '', value: '', listeners: {}, props: {}, sets: 0,
    addEventListener(t, fn) { (this.listeners[t] = this.listeners[t] || []).push(fn); },
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
  const docL = {}, events = [], focused = [], opened = [];
  const document = {
    getElementById: (id) => els[id] || null,
    addEventListener: (t, fn) => { (docL[t] = docL[t] || []).push(fn); },
    dispatchEvent: (e) => { events.push(e); (docL[e.type] || []).forEach((fn) => fn(e)); },
  };
  class CustomEvent { constructor(type, init) { this.type = type; this.detail = (init || {}).detail; } }
  const coarse = opt.coarse !== false;
  const window = { matchMedia: (q) => ({ matches: q === '(pointer: coarse)' ? coarse : false }) };
  const layers = new Set(['forecast', 'points']);
  const map = {
    latLngToContainerPoint: ([lat, lng]) => ({ x: lng * 1000, y: -lat * 1000 }),   // 1 px = 0.001 degree
    hasLayer: (l) => layers.has(l),
  };
  const env = {
    forecastStationsData: opt.forecast || [], liveStationsData: opt.live || [], liveDrawn: opt.drawn || [],
    tideDrawn: opt.tides || [], windDrawn: opt.winds || [], savedPointsData: opt.points || [], liveShownStation: null,
  };
  const api = new Function('document', 'window', 'CustomEvent', 'map', 'env', 'forecastLayer', 'pointLayer',
    'getSelectedUnit', 'buoyDisplayLabel', 'saveLayerVisibility', 'openLiveBuoy', 'focusWindowHeader', 'visibleCopies',
    'with (env) {\n' + BLOCK + '\nreturn { linkFor, linkKey, forecastLinkFor, liveLinkFor, syncBuoyLinks, liveTargets, LINK_KM, LINK_ID_KM, LIVE_TAP_MAX,\n' +
    '  setLive: function (s) { env.liveShownStation = s; } };\n}')(
    document, window, CustomEvent, map, env, 'forecast', 'points',
    () => els.unit.value, (s) => String(s.id).replace(/^ndbc:/, ''), () => {}, (s) => opened.push(s), (id) => focused.push(id),
    (list) => list.map((s) => ({ s, lng: s.lon, o: 0 })));
  return { api, els, env, events, focused, opened, layers, fire: (t, d) => (docL[t] || []).forEach((fn) => fn({ detail: d })) };
}

// 41113 (Cape Canaveral nearshore) and its forecast point 0.29 km away; an unrelated forecast point 0.2 km from it
const BUOY = { id: 'ndbc:41113', lat: 28.400, lon: -80.534 };
const TWIN = { id: '41113', lat: 28.4026, lon: -80.534 };            // 0.29 km north
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
  assert.equal(A.LINK_KM, 1.0); assert.equal(A.LINK_ID_KM, 400);
});

test('the live window: "Forecast for this point" for a twin, the id and distance for a moved buoy, nothing without a link; a click loads it in the forecast window', () => {
  const far = { id: '41041', lat: 14.6, lon: -46.0 }, liveFar = { id: 'ndbc:41041', lat: 14.33, lon: -46.08 };
  const b = boot({ forecast: [TWIN, far], live: [BUOY, liveFar] }), A = b.api, E = b.els;
  A.setLive(BUOY); A.syncBuoyLinks();
  assert.equal(E.lwLinks.hidden, false); assert.equal(E.lwForecast.textContent, 'Forecast for this point'); assert.equal(E.lwForecast.title, 'Forecast point 41113');
  E.lwForecast.click();
  assert.equal(E.station.value, '41113');
  const ev = b.events.filter((e) => e.type === 'allshore:station').pop();
  assert.deepEqual(ev.detail, { sid: '41113', source: 'map' }, 'as a click on its dot (Buoy Local, the window expands)');
  assert.deepEqual(b.focused, [], 'a pointer click leaves the focus');
  E.lwForecast.click(0); assert.deepEqual(b.focused, ['fwHeader'], 'a keyboard press moves it into the forecast window');
  A.setLive(liveFar); A.syncBuoyLinks();
  assert.equal(E.lwForecast.textContent, 'Forecast point 41041 · 19 mi away', '10 and over: whole units');
  E.unit.value = 'Metric'; A.syncBuoyLinks(); assert.equal(E.lwForecast.textContent, 'Forecast point 41041 · 31 km away');
  b.env.forecastStationsData = [TWIN, { id: '41041', lat: 14.375, lon: -46.08 }]; A.syncBuoyLinks();
  assert.equal(E.lwForecast.textContent, 'Forecast point 41041 · 5.0 km away', 'under 10: one decimal');
  A.setLive({ id: 'aodn:Tantabiddi', lat: -21.9, lon: 113.9 }); A.syncBuoyLinks();
  assert.equal(E.lwLinks.hidden, true, 'no forecast point near: no button');
  A.setLive(null); A.syncBuoyLinks(); assert.equal(E.lwLinks.hidden, true);
});

test('the forecast window: "Live buoy" follows the station shown (a pick or a forecast landing); never for My points or an unknown id; a click opens the buoy', () => {
  const b = boot({ forecast: [TWIN, { id: '51201', lat: 21.67, lon: -158.12 }], live: [BUOY] }), E = b.els;
  assert.equal(E.fwLive.hidden, true, 'nothing shown yet');
  b.fire('allshore:forecast', { station: '41113' });
  assert.equal(E.fwLive.hidden, false); assert.equal(E.fwLive.textContent, 'Live buoy'); assert.equal(E.fwLive.title, 'Live buoy 41113');
  E.fwLive.click(); assert.deepEqual(b.opened, [BUOY]); assert.deepEqual(b.focused, []);
  E.fwLive.click(0); assert.deepEqual(b.focused, ['lwHeader']);
  b.fire('allshore:station', { sid: '51201' }); assert.equal(E.fwLive.hidden, true, 'no live buoy near 51201 in this list');
  b.fire('allshore:station', { sid: 'pt_28400N_80534W' }); assert.equal(E.fwLive.hidden, true, 'one of My points: never');
  b.fire('allshore:station', { sid: 'NOPE' }); assert.equal(E.fwLive.hidden, true, 'not in the list: never');
  b.fire('allshore:forecast', {}); assert.equal(E.fwLive.hidden, true, 'an event without a station changes nothing');
  const n = b.opened.length; E.fwLive.click(); assert.equal(b.opened.length, n, 'no link: a click opens nothing');
  b.els.lwForecast.click(); assert.equal(b.els.station.value, '', "nor the live window's (no buoy shown)");
});

test('the links follow new lists (the cache is keyed by the lists themselves)', () => {
  const b = boot({ forecast: [TWIN], live: [] }), A = b.api;
  b.fire('allshore:forecast', { station: '41113' }); assert.equal(b.els.fwLive.hidden, true, 'no live list yet');
  b.env.liveStationsData = [BUOY]; A.syncBuoyLinks(); assert.equal(b.els.fwLive.hidden, false, 'the live list landed');
  b.env.forecastStationsData = []; A.syncBuoyLinks(); assert.equal(b.els.fwLive.hidden, true, 'a new forecast list without it');
});

test('touch pads: half the way to the nearest other icon (8 px at most); a flag on the buoy leaves the dot as it was; the twin forecast dot does not count; mice: nothing', () => {
  // live dots at the coordinates below; the dot is drawn 6.5 px right of its point (7.5 px when active)
  const mk = () => marker();
  const at = (id, lat, lon, active) => ({ marker: mk(), lat, lng: lon, s: { id, lat, lon }, active: !!active });
  const lone = at('ndbc:1', 10, 10), withFlag = at('ndbc:2', 20, 20), nearTide = at('ndbc:3', 30, 30), twinOnly = at('ndbc:41113', 28.400, -80.534),
    nearOther = at('ndbc:5', 40, 40), active = at('ndbc:6', 50, 50, true), pair1 = at('ndbc:7', 60, 60), pair2 = at('ndbc:8', 60, 60.03);
  const drawn = [lone, withFlag, nearTide, twinOnly, nearOther, active, pair1, pair2];
  const b = boot({
    forecast: [TWIN, { id: 'F5', lat: 40, lon: 40.0215 }],                 // F5: 15 px from ndbc:5's dot centre (6.5 + 15)
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
  assert.equal(pad(twinOnly), '8px', 'its own forecast point does not count (the window links it)');
  assert.equal(pad(nearOther), '2px', 'another forecast point 15 px away: floor(7.5 - 5.5)');
  assert.equal(pad(active), '2px', 'the active dot (15 px) and one of My points 20 px away: floor(10 - 7.5)');
  assert.equal(pad(pair1), '8px'); assert.equal(pad(pair2), '8px', 'two live dots 30 px apart: floor(15 - 5.5) capped at 8');
  const sets = drawn.map((d) => d.marker.style.sets);
  b.api.liveTargets(); assert.deepEqual(drawn.map((d) => d.marker.style.sets), sets, 'written only when it changes');
  b.layers.delete('forecast'); b.api.liveTargets(); assert.equal(pad(nearOther), '8px', 'the forecast layer unticked: its dots do not count');
  const m = boot({ coarse: false, live: [lone.s], drawn: [at('ndbc:1', 10, 10)] });
  m.api.liveTargets(); assert.equal(m.env.liveDrawn[0].marker.style.sets, 0, 'a mouse: no pad written');
});

test('the page wiring: the block between its markers, the two buttons, the canvas tolerance, the pad CSS, the phone order, the marker click through openLiveBuoy', () => {
  assert.ok(TPL.includes('<button type="button" id="fwLive" class="btn btn-sm btn-outline-primary" hidden>Live buoy</button>'));
  assert.ok(TPL.includes('<div id="lwLinks" class="lw-links" hidden><button type="button" id="lwForecast" class="btn btn-sm btn-outline-primary">Forecast for this point</button></div>'));
  assert.ok(TPL.indexOf('id="lwLinks"') > TPL.indexOf('<div id="lwBody" class="fw-body">'), 'first in the live window\'s body');
  assert.ok(TPL.includes("renderer: L.canvas({ tolerance: (window.matchMedia && window.matchMedia('(pointer: coarse)').matches) ? 8 : 0 }),"));
  assert.ok(TPL.includes("@media (pointer: coarse) { .live-buoy-dot::after { content: ''; position: absolute; inset: calc(-2px - var(--live-tap, 0px)); border-radius: 50%; } }"));
  assert.ok(TPL.includes('.fwin.fw-front:not(.fw-min) { z-index: 3510; }'));
  assert.ok(/marker\.on\('click', function \(e\) \{\n\s+if \(window\.AllshoreTools[^\n]+\n\s+openLiveBuoy\(s\);\n/.test(TPL), 'the dot opens the buoy through openLiveBuoy');
  assert.ok(TPL.includes("liveShownStation = station;\n      if (typeof syncBuoyLinks === 'function') syncBuoyLinks();"));
  assert.ok(TPL.includes("      if (typeof liveTargets === 'function') liveTargets();   // the live dots' pads after every round (plan section 52)"));
});
