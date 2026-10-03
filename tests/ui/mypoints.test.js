'use strict';
// My points on the page (plan section 31, G22): the page's OWN script blocks from templates/index.html (the
// favourites picker, the "My points" block, the AllshoreForecast.init call) run with static_ui/forecast.js against a
// small DOM (pagedom.js). Nothing of the page's logic is re-typed here: the blocks are cut out of the template by
// their first and last lines (the harness is reviewer A's, G22).
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { Document_, storage } = require('./pagedom');

const ROOT = path.join(__dirname, '..', '..');
const TPL = fs.readFileSync(path.join(ROOT, 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');
const SRC = fs.readFileSync(path.join(ROOT, 'static_ui', 'forecast.js'), 'utf8');

function cut(from, to) {
  const a = TPL.indexOf(from); if (a < 0) throw new Error('not in the template: ' + from);
  const b = TPL.indexOf(to, a); if (b < 0) throw new Error('not in the template: ' + to);
  return TPL.slice(a, b);
}
const PICKER = cut('(function initStationPicker() {', '    // the picker just made the forecast bar taller');
const BLOCK = cut("    (function () {\n      var F = window.AllshoreForecast, select = document.getElementById('station');", "    (function () {\n      var sel = document.getElementById('station');");
const INIT = cut("    (function () {\n      var sel = document.getElementById('station');", '  </script>\n  <!-- Map tools');

const tick = () => new Promise((r) => setImmediate(r));
async function settle(n) { for (let i = 0; i < (n || 10); i++) await tick(); }

function payload(station, over) {
  const labels = [];
  for (let i = 0; i < 24; i++) labels.push(`Saturday, September 26, 2026 ${(i % 12) + 1}:00 ${i < 12 ? 'AM' : 'PM'}`);
  const g = (v) => ({ s1: labels.map(() => v), s2: labels.map(() => null), s3: [], s4: [], s5: [], s6: [], combined: labels.map(() => v + 1) });
  const isPt = station.indexOf('pt_') === 0;
  return Object.assign({ station, error: null, table_html: '<table class="forecast-compact"><tr><td>1.0</td></tr></table>',
    graph_data: { labels, height: g(2), period: g(10), direction: g(300), units: 'ft' },
    graph_header: { cycle: '20261002 12 UTC', location: isPt ? 'somewhere (nearest model point x, 5 km away)' : station + ' (21.67N 158.12W)', tz: 'Pacific/Honolulu' },
    model: 'GFS', swan_available: false, wind_complete: true,
    point: isPt ? { id: station, lat: 1, lon: 1, cell_lat: 1, cell_lon: 1, cell_km: 5, grid: 'g16', run: '2026100212', age_hours: 7.0 } : undefined }, over || {});
}
const REFUSED = (station, reason, error) => ({ station, error, final: true, reason, table_html: null, graph_data: null, graph_header: null, model: 'GFS', swan_available: false, point: null });
const LAND = (station) => REFUSED(station, 'land', 'That point is on land or inland water. Pick a point on the sea.');
const BUSY = (station) => ({ station, error: 'The server is busy with other forecast points; try again in a moment', busy: true, table_html: null, graph_data: null, graph_header: null, model: 'GFS', swan_available: false, point: null });

function boot(opts) {
  opts = opts || {};
  const doc = new Document_();
  const el = (tag, id, parent, attrs) => { const e = doc.createElement(tag); if (id) e.id = id; (parent || doc.body).appendChild(e); Object.entries(attrs || {}).forEach(([k, v]) => e.setAttribute(k, v)); return e; };
  const host = el('div', 'settingsHost'); el('button', 'settingsBtn', host); const panel = el('div', 'settingsPanel', host); panel.hidden = true;
  const tz = el('select', 'tz', panel); [['', '(Buoy Local)'], ['UTC', 'UTC']].forEach(([v, t]) => { const o = el('option', null, tz); o.value = v; o.textContent = t; });
  const unit = el('select', 'unit', panel); ['US', 'Metric'].forEach((v) => { const o = el('option', null, unit); o.value = v; o.textContent = v; });
  const live = el('section', 'liveBuoyPanel'); live.hidden = true; el('div', 'lwHeader', live);
  const w = el('section', 'forecastWin'); const header = el('div', 'fwHeader', w);
  const field = el('div', 'fwTitle', header, { class: 'station-field' }); el('label', null, field, { class: 'form-label sr-only', for: 'station' });
  const picker = el('div', null, field, { class: 'station-picker' }); const trig = el('div', null, picker, { class: 'station-trigger' });
  const star = el('button', 'stationFavToggle', trig, { class: 'sr-star' }); const trigger = el('button', 'stationTrigger', trig);
  const current = el('span', 'stationCurrent', trigger); current.textContent = opts.station || '51201';
  const results = el('ul', 'stationResults', picker, { class: 'station-results' }); results.hidden = true;
  const status = el('span', 'stationStatus', picker, { class: 'sr-only' });
  const select = el('select', 'station', field);
  const server = opts.station || '51201';
  if (server.indexOf('pt_') === 0) { const o = el('option', null, select, { 'data-point': '' }); o.value = server; o.textContent = 'served'; }   // the no-script option (G22 B-5)
  if (opts.unknown) { const o = el('option', null, select, { 'data-unknown': '' }); o.value = server; o.textContent = server; }   // an id that is no station (G22 R-B12)
  [['51201', '51201 — Waimea Bay, HI'], ['46001', '46001 — Gulf of Alaska']].forEach(([v, t]) => { const o = el('option', null, select); o.value = v; o.textContent = t; });
  select.selectedIndex = Math.max(0, select.options.findIndex((o) => o.value === server));
  el('span', 'fwCycle', header); el('span', 'fwBusy', header).hidden = true; el('button', 'fwMin', header); el('button', 'fwMax', header);
  const toolbar = el('div', 'fwToolbar', w);
  const viewBar = el('div', 'viewBar', toolbar); ['Table', 'Graph'].forEach((v) => el('button', null, viewBar, { 'data-view': v }));
  const modelBar = el('div', 'modelBar', toolbar); ['GFS', 'SWAN'].forEach((v) => el('button', null, modelBar, { 'data-model': v }));
  const rangeBar = el('div', 'rangeBar', toolbar); ['0', '7', '3'].forEach((v) => el('button', null, rangeBar, { 'data-days': v }));
  const body = el('div', 'fwBody', w); const note = el('div', 'fwNote', body); note.hidden = true;
  const errBox = el('div', 'fwError', body); errBox.hidden = true; el('div', 'forecastMeta', body);
  const table = el('div', 'forecastTable', body); el('div', 'forecastLoading', table);
  const graphs = el('div', 'graphs', body); graphs.hidden = true;
  ['heightChart', 'periodChart', 'directionChart'].forEach((id) => { const box = el('div', null, graphs, { class: 'chart-box' }); el('canvas', id, box); });
  el('div', 'fwResize', w);

  const local = opts.localStorage || storage(opts.local, opts.storageOpts);
  const calls = [], confirms = [], winListeners = {}, layerShown = [];
  const win = {
    document: doc, innerWidth: 1280, innerHeight: 800, location: { search: opts.search || '' },
    matchMedia() { return { matches: false }; },
    addEventListener(t, fn) { (winListeners[t] = winListeners[t] || []).push(fn); },
    history: { urls: [], replaceState(a, b, url) { this.urls.push(url); win.location.search = url; } },
    sessionStorage: storage(), Chart: undefined,
    prompt() { return opts.prompt === undefined ? null : opts.prompt; },
    confirm(msg) { confirms.push(msg); return opts.confirm !== false; },
    fetch(url, o) {
      return new Promise((res, rej) => {
        const c = { url, station: new URLSearchParams(url.split('?')[1]).get('station'), aborted: false, done: false,
          release(d, st) { c.done = true; res({ ok: !st || st < 400, status: st || 200, json: () => Promise.resolve(d) }); }, fail() { c.done = true; rej(new TypeError('Failed to fetch')); } };
        if (o && o.signal) o.signal.addEventListener('abort', () => { c.aborted = true; rej(Object.assign(new Error('aborted'), { name: 'AbortError' })); });
        calls.push(c);
      });
    },
  };
  Object.defineProperty(win, 'localStorage', opts.localStorageThrows ? { get() { throw new Error('SecurityError'); } } : { value: local });
  win.window = win;
  win.__initial = Object.assign({ station: server, tz: '', unit: 'US', model: 'GFS', view: 'Table', swan_available: false, swan_stations: ['51201'], inline: false }, opts.initial || {});
  new Function('window', 'URLSearchParams', SRC)(win, URLSearchParams);
  const F = win.AllshoreForecast;
  const markers = [];
  const setSavedPoints = function (list) { markers.push(list.map((p) => Object.assign({}, p))); };
  const showPointLayer = function () { layerShown.push(true); };
  const events = [];
  ['allshore:station', 'allshore:forecast', 'allshore:points'].forEach((t) => doc.addEventListener(t, (e) => events.push([t, e.detail ? JSON.parse(JSON.stringify(e.detail)) : null])));
  const env = ['window', 'document', 'location', 'localStorage', 'CustomEvent', 'URLSearchParams', 'setSavedPoints', 'showPointLayer', 'loadChartJs', 'closeLiveBuoyPanel',
    'enforceSingleWorld', 'liveWin', 'liveDetailSeq'];
  const args = () => [win, doc, win.location, opts.localStorageThrows ? undefined : local, CustomEvent, URLSearchParams, setSavedPoints, showPointLayer,
    () => Promise.reject(new Error('no charts here')), () => {}, () => {}, null, 0];
  new Function(...env, PICKER.replace(/\n\s*$/, '') + '\n')(...args());
  new Function(...env, BLOCK)(...args());
  new Function(...env, 'var liveWin = null, liveDetailSeq = 0;\n' + INIT.replace('var liveWin', 'var _unused'))(...args());
  const app = F._internals.app();
  const P = win.__allshorePoints;
  return { doc, win, F, app, P, calls, markers, events, confirms, winListeners, layerShown, local, select, star, trigger, current, results, status, errBox, note, table,
    last: () => calls[calls.length - 1],
    station: () => app.state.station,
    stored: () => P.store.list().map((p) => p.id + (p.name ? '=' + p.name : '')),
    stationEvents: () => events.filter((e) => e[0] === 'allshore:station').map((e) => e[1].sid),
    lastMarks: () => markers.length ? markers[markers.length - 1] : [],
    openPicker() { if (results.hidden) trigger.click(); },
    row(id) { return results.children.find((li) => li.children[0] && li.children[0].dataset.sid === id); },
  };
}

// ---- the tool asks first (G22: the owner refuses land) ----
test('a tool click: the server is asked first; only a forecast opens the window, from the cache, and keeps the point', async () => {
  const b = boot(); await settle(); b.last().release(payload('51201')); await settle();
  const asked = b.P.add({ lat: 21.7, lng: -158.2 });
  assert.equal(typeof asked.then, 'function');
  await settle();
  assert.equal(b.last().station, 'pt_21700N_158200W');
  assert.equal(b.station(), '51201', 'nothing on screen changed while the point is checked');
  assert.deepEqual(b.stored(), []);
  b.last().release(payload('pt_21700N_158200W')); await settle();
  const r = await asked;
  assert.equal(r.ok, true); assert.deepEqual(b.stored(), [], 'not kept before it is opened');
  const n = b.calls.length;
  r.open(); await settle();
  assert.equal(b.calls.length, n, 'opened from the cache: no second request');
  assert.equal(b.station(), 'pt_21700N_158200W');
  assert.deepEqual(b.stored(), ['pt_21700N_158200W']);
  assert.equal(b.star.textContent, '★');
  assert.deepEqual(b.lastMarks().map((m) => [m.id, m.kept]), [['pt_21700N_158200W', true]]);
  assert.equal(b.layerShown.length, 1, 'the My points layer is shown (G22 B-17)');
  assert.equal(b.status.textContent, 'Kept in My points'); assert.equal(b.note.hidden, true);
});

test('a refusal (land, sheltered water, no data): nothing opens, nothing is kept, the message goes back to the tool; the star cannot keep it later', async () => {
  const b = boot(); await settle(); b.last().release(payload('51201')); await settle();
  for (const [reason, error] of [['land', 'That point is on land or inland water. Pick a point on the sea.'], ['sheltered', 'No forecast here: the wave model’s nearest points lie beyond land (sheltered water).']]) {
    const asked = b.P.add({ lat: 21.5, lng: -158.0 }); await settle();
    b.last().release(REFUSED('pt_21500N_158000W', reason, error)); await settle();
    const r = await asked;
    assert.deepEqual([r.ok, r.message], [false, error]);
  }
  assert.equal(b.station(), '51201'); assert.deepEqual(b.stored(), []);
  assert.deepEqual(b.stationEvents(), [], 'no window opened');
  assert.equal(b.P.refused('pt_21500N_158000W'), true);
  // the same point from a link later: refused again, the star stays off, no marker for it
  b.doc.dispatchEvent(new CustomEvent('allshore:station', { detail: { sid: 'pt_21500N_158000W', source: 'map' } })); await settle();
  b.last().release(LAND('pt_21500N_158000W')); await settle();
  assert.equal(b.star.disabled, true, 'nothing to keep (G22 A-13)');
  b.star.click();
  assert.deepEqual(b.stored(), []);
  assert.equal(b.lastMarks().some((m) => m.id === 'pt_21500N_158000W'), false);
  // a busy server and a failure are NOT final: the point may be tried again and kept
  const busy = b.P.add({ lat: 22.5, lng: -158.0 }); await settle(); b.last().release(BUSY('pt_22500N_158000W')); await settle();
  assert.equal((await busy).ok, false); assert.equal(b.P.refused('pt_22500N_158000W'), false);
  const down = b.P.add({ lat: 22.6, lng: -158.0 }); await settle(); b.last().fail();
  await assert.rejects(down, 'a network failure rejects: the tool says it could not reach the server');
});

test('a point that cannot be kept says so in the window (G22 B-3): My points full, storage refused, storage switched off', async () => {
  const full = {}; full['allshore.points.v1'] = JSON.stringify(Array.from({ length: 50 }, (_, i) => ({ id: `pt_${10000 + i}N_150000W` })));
  const b = boot({ local: full }); await settle(); b.last().release(payload('51201')); await settle();
  const asked = b.P.add({ lat: 21.7, lng: -158.2 }); await settle(); b.last().release(payload('pt_21700N_158200W')); await settle();
  (await asked).open(); await settle();
  assert.equal(b.station(), 'pt_21700N_158200W', 'the forecast still opens');
  assert.equal(b.stored().length, 50); assert.equal(b.note.hidden, false);
  assert.match(b.note.textContent, /My points is full \(50\): this point is not kept/);
  assert.equal(b.star.textContent, '☆');
  // the note goes with the point
  b.app.loader.load({ station: '51201' }); await settle();
  assert.equal(b.note.hidden, true);
  for (const o of [{ storageOpts: { throwOnWrite: true } }, { localStorageThrows: true }]) {
    const c = boot(o); await settle(); c.last().release(payload('51201')); await settle();
    const a = c.P.add({ lat: 21.7, lng: -158.2 }); await settle(); c.last().release(payload('pt_21700N_158200W')); await settle();
    (await a).open(); await settle();
    assert.equal(c.note.hidden, false); assert.match(c.note.textContent, /could not keep the point/);
    assert.doesNotMatch(c.status.textContent, /Kept/, 'never "Kept" when nothing was kept (G22 A-7)');
    assert.equal(c.status.textContent, '', 'said once: in the window, its own status line (G22 R-A14)');
    assert.deepEqual(c.stored(), []);
  }
});

test('a linked point not kept has a marker of its own (dashed); the no-script option is taken over by the optgroup', async () => {
  const b = boot({ station: 'pt_21700N_158200W', search: '?station=pt_21700N_158200W' }); await settle();
  assert.equal(b.select.querySelectorAll('option[data-point]').length, 0, 'the server’s option went (G22 B-5)');
  b.last().release(payload('pt_21700N_158200W')); await settle();
  assert.deepEqual(b.lastMarks().map((m) => [m.id, m.kept]), [['pt_21700N_158200W', false]], 'K-2');
  assert.equal(b.select.value, 'pt_21700N_158200W');
  assert.deepEqual(b.stored(), []);
  b.star.click();
  assert.deepEqual(b.stored(), ['pt_21700N_158200W']);
  assert.deepEqual(b.lastMarks().map((m) => [m.id, m.kept]), [['pt_21700N_158200W', true]]);
});

test('another tab changed My points: this one follows (G22 B-9)', async () => {
  const b = boot(); await settle(); b.last().release(payload('51201')); await settle();
  b.local.m.set('allshore.points.v1', JSON.stringify([{ id: 'pt_20000N_160000W', name: 'Other tab' }]));
  b.winListeners.storage.forEach((fn) => fn({ key: 'allshore.points.v1' }));
  assert.ok(b.select.options.some((o) => o.value === 'pt_20000N_160000W'));
  assert.equal(b.lastMarks()[0].id, 'pt_20000N_160000W');
  b.openPicker();
  b.row('pt_20000N_160000W').children[0].click(); await settle();
  assert.equal(b.station(), 'pt_20000N_160000W', 'its row works');
});

test('labels: the coordinates are never what gets cut, the name is isolated; Remove asks before dropping a named point', async () => {
  const name = 'A very long name for my favourite reef break';
  const b = boot({ local: { 'allshore.points.v1': JSON.stringify([{ id: 'pt_21667N_158054W', name: 'Pipeline' }, { id: 'pt_21700N_158200W', name }]) } });
  await settle(); b.last().release(payload('51201')); await settle();
  b.openPicker();
  const pick = b.row('pt_21667N_158054W').children[0];
  assert.equal(pick.classList.contains('lbl-split'), true);
  assert.deepEqual(pick.children.map((c) => [c.className, c.textContent]), [['lbl-name', 'Pipeline'], ['lbl-coord', ' — 21.667N 158.054W']]);
  assert.equal(pick.children[0].getAttribute('dir'), 'auto');
  assert.equal(b.F.cleanName(name), 'A very long name for my favourite reef b', '40 characters');
  assert.equal(b.F.cleanName('‮evil‬ ' + 'x'.repeat(38) + '🌊'), 'evil ' + 'x'.repeat(35), 'bidi controls go; nothing is cut in half');
  assert.equal(b.F.cleanName('a'.repeat(39) + '🌊'), 'a'.repeat(39) + '🌊');
  // Remove: a named point asks first; "no" keeps it
  const no = boot({ confirm: false, local: { 'allshore.points.v1': JSON.stringify([{ id: 'pt_21667N_158054W', name: 'Pipeline' }]) } });
  await settle(); no.last().release(payload('51201')); await settle();
  no.openPicker(); no.row('pt_21667N_158054W').children[2].click();
  assert.deepEqual(no.stored(), ['pt_21667N_158054W=Pipeline']); assert.match(no.confirms[0], /Remove "Pipeline" from My points\?/);
  b.row('pt_21667N_158054W').children[2].click();
  assert.deepEqual(b.stored(), [`pt_21700N_158200W=${b.F.cleanName(name)}`]);
});

test('the picker by keyboard: arrows, Home and End move between the rows; ArrowUp from the first goes back to the trigger (G22 B-6)', async () => {
  const pts = Array.from({ length: 5 }, (_, i) => ({ id: `pt_${20000 + i}N_150000W` }));
  const b = boot({ local: { 'allshore.points.v1': JSON.stringify(pts) } }); await settle(); b.last().release(payload('51201')); await settle();
  b.openPicker();
  const picks = b.results.querySelectorAll('.fav-select');
  assert.equal(picks.length, 5);
  const key = (k) => b.doc.activeElement.fire('keydown', { key: k });
  picks[0].focus(); key('ArrowDown'); assert.equal(b.doc.activeElement, picks[1]);
  key('End'); assert.equal(b.doc.activeElement, picks[4]);
  key('ArrowDown'); assert.equal(b.doc.activeElement, picks[4], 'stays on the last');
  key('Home'); assert.equal(b.doc.activeElement, picks[0]);
  b.row(pts[2].id).children[1].focus(); key('ArrowDown'); assert.equal(b.doc.activeElement, picks[3], 'from Rename: the next row');
  picks[0].focus(); key('ArrowUp'); assert.equal(b.doc.activeElement, b.trigger);
});

test('a failed load clears the last table and meta (G22 B-7); a refusal is said once (K-5); nautical zones read as UTC offsets (B-2)', async () => {
  const b = boot(); await settle(); b.last().release(payload('51201')); await settle();
  assert.match(b.table.innerHTML, /forecast-compact/);
  b.app.loader.load({ station: 'pt_21700N_158200W' }); await settle(); b.last().fail(); await settle();
  assert.equal(b.table.children.length + b.table.textContent.length, 0);
  assert.equal(b.doc.getElementById('forecastMeta').textContent, '');
  assert.match(b.errBox.textContent, /Could not load the forecast/);
  b.app.loader.load({ station: 'pt_21500N_158000W' }); await settle(); b.last().release(LAND('pt_21500N_158000W')); await settle();
  assert.equal(b.table.textContent, ''); assert.match(b.errBox.textContent, /on land/); assert.equal(b.errBox.querySelectorAll('button').length, 0);
  b.app.loader.load({ station: 'pt_24500N_157900W' }); await settle();
  b.last().release(payload('pt_24500N_157900W', { graph_header: { cycle: 'x', location: 'y', tz: 'Etc/GMT+11' } })); await settle();
  assert.match(b.doc.getElementById('forecastMeta').textContent, /Time Zone: UTC−11$/);
  const Z = b.F.zoneLabel;
  assert.deepEqual(['Etc/GMT+11', 'Etc/GMT-5', 'Etc/GMT-14', 'Etc/GMT', 'Etc/UTC', 'Etc/GMT+0', 'Pacific/Honolulu', '', null].map(Z),
    ['UTC−11', 'UTC+5', 'UTC+14', 'UTC', 'UTC', 'UTC', 'Pacific/Honolulu', '', '']);
});

test('the title keeps its whole label as a tooltip after a rename (G22 A-14)', async () => {
  const b = boot({ prompt: 'Pipe', local: { 'allshore.points.v1': JSON.stringify([{ id: 'pt_21667N_158054W' }]) } });
  await settle(); b.last().release(payload('51201')); await settle();
  b.openPicker(); b.row('pt_21667N_158054W').children[0].click(); await settle(); b.last().release(payload('pt_21667N_158054W')); await settle();
  b.openPicker(); b.row('pt_21667N_158054W').children[1].click();
  assert.equal(b.current.title, '⁨Pipe⁩ — 21.667N 158.054W');
  assert.equal(b.current.textContent, 'Pipe — 21.667N 158.054W');
});

// ---- G22 re-check (fix round 2) ----
test('a late answer: the visitor opened another forecast while the point was checked; that one stays (G22 R-A6)', async () => {
  const b = boot(); await settle(); b.last().release(payload('51201')); await settle();
  const asked = b.P.add({ lat: 21.7, lng: -158.2 }); await settle();
  const pt = b.last();
  b.doc.dispatchEvent(new CustomEvent('allshore:station', { detail: { sid: '46001', source: 'picker' } })); await settle();
  pt.release(payload('pt_21700N_158200W')); await settle();
  assert.deepEqual(await asked, { ok: false, cancel: true });
  assert.deepEqual(b.stored(), []); assert.notEqual(b.station(), 'pt_21700N_158200W');
});

test('a refusal is forgotten when a forecast for the point lands (a later run has data there; G22 R-A8)', async () => {
  const b = boot({ station: 'pt_21500N_158000W', search: '?station=pt_21500N_158000W' }); await settle();
  b.last().release(LAND('pt_21500N_158000W')); await settle();
  assert.equal(b.P.refused('pt_21500N_158000W'), true); assert.equal(b.star.disabled, true);
  assert.match(b.star.getAttribute('aria-label'), /^No forecast here, nothing to keep/, 'the disabled star says why (G22 R-A14)');
  b.app.loader.load({}); await settle(); b.last().release(payload('pt_21500N_158000W')); await settle();
  assert.equal(b.P.refused('pt_21500N_158000W'), false); assert.equal(b.star.disabled, false);
  assert.ok(b.lastMarks().some((m) => m.id === 'pt_21500N_158000W' && m.kept === false), 'its marker is back');
  assert.match(b.star.getAttribute('aria-label'), /^Keep this point in My points/);
  b.star.click(); assert.deepEqual(b.stored(), ['pt_21500N_158000W']);
  assert.equal(b.layerShown.length, 1, 'kept with the star: the My points layer is shown (G22 R-A15)');
});

test('a failed load leaves nothing of the last forecast: run text, model bar, note (G22 R-A9, R-A10); Retry keeps the focus in the window (R-B8)', async () => {
  const full = {}; full['allshore.points.v1'] = JSON.stringify(Array.from({ length: 50 }, (_, i) => ({ id: `pt_${10000 + i}N_150000W` })));
  const b = boot({ local: full }); await settle(); b.last().release(payload('51201', { swan_available: true })); await settle();
  const cycle = b.doc.getElementById('fwCycle'), bar = b.doc.getElementById('modelBar');
  assert.notEqual(cycle.textContent, ''); assert.equal(bar.hidden, false);
  b.app.loader.load({ station: '46001' }); await settle(); b.last().fail(); await settle();   // a SWAN station's bar was showing
  assert.equal(cycle.textContent, ''); assert.equal(bar.hidden, true);
  b.app.loader.load({ station: '51201' }); await settle(); b.last().release(payload('51201', { swan_available: true })); await settle();
  const asked = b.P.add({ lat: 21.7, lng: -158.2 }); await settle(); b.last().release(payload('pt_21700N_158200W')); await settle();
  (await asked).open(); await settle();
  assert.equal(b.note.hidden, false);
  b.app.loader.load({ station: '46001' }); await settle(); b.last().fail(); await settle();
  assert.equal(cycle.textContent, ''); assert.equal(b.note.hidden, true);
  const retry = b.errBox.querySelectorAll('button')[0];
  retry.focus(); retry.click(); await settle();
  assert.equal(b.doc.activeElement, b.doc.getElementById('fwBody'), 'the button is gone: the focus is in the window, not on the page');
  b.last().release(payload('46001')); await settle();
  assert.equal(b.station(), '46001');
});

test('an id that is no station keeps its name in the title, also after a refusal (G22 R-A11, R-B12)', async () => {
  const b = boot({ station: 'bad!id', unknown: true }); await settle();
  b.last().release(REFUSED('bad!id', 'invalid', 'Invalid station id')); await settle();
  assert.equal(b.current.textContent, 'bad!id');
  b.openPicker();
  assert.equal(b.results.querySelectorAll('.fav-select').some((x) => x.dataset.sid === 'bad!id'), false, 'not a row of the list');
});

test('names are cut by what a visitor sees as one character (G22 R-A12); a stored name that is not text is dropped (R-A13)', () => {
  const b = boot();
  const fam = '\u{1F468}‍\u{1F469}‍\u{1F467}', flag = '\u{1F1FA}\u{1F1F8}', tone = '\u{1F44D}\u{1F3FD}';
  for (const e of [fam, flag, tone]) {
    assert.equal(b.F.cleanName('a'.repeat(39) + e), 'a'.repeat(39) + e, 'whole at 40: ' + e);
    assert.equal(b.F.cleanName('a'.repeat(40) + e), 'a'.repeat(40), 'never half of it: ' + e);
  }
  assert.equal(b.F.cleanName({ toString: 1 }), ''); assert.equal(b.F.cleanName(null), '');
  const c = boot({ local: { 'allshore.points.v1': JSON.stringify([{ id: 'pt_1N_1E', name: { toString: 1 } }]) } });
  assert.deepEqual(c.stored(), ['pt_1N_1E'], 'the window starts; the name is dropped');
});

test('another tab changed My points while the list had the focus: it stays on the same row (G22 R-A16)', async () => {
  const pts = [{ id: 'pt_20000N_150000W' }, { id: 'pt_20001N_150000W', name: 'Two' }];
  const b = boot({ local: { 'allshore.points.v1': JSON.stringify(pts) } }); await settle(); b.last().release(payload('51201')); await settle();
  b.openPicker();
  b.row('pt_20001N_150000W').children[1].focus();                                    // its Rename button
  b.local.m.set('allshore.points.v1', JSON.stringify([{ id: 'pt_30000N_150000W' }].concat(pts)));
  b.winListeners.storage.forEach((fn) => fn({ key: 'allshore.points.v1' }));
  assert.equal(b.doc.activeElement, b.row('pt_20001N_150000W').children[1]);
  b.row('pt_20000N_150000W').children[0].focus();
  b.local.m.set('allshore.points.v1', JSON.stringify([{ id: 'pt_30000N_150000W' }]));        // its row is gone
  b.winListeners.storage.forEach((fn) => fn({ key: 'allshore.points.v1' }));
  assert.equal(b.doc.activeElement, b.row('pt_30000N_150000W').children[0], 'the first row then');
});

test('a storage whose reads throw is not used: nothing is said kept (G22 re-check X23); a removal the browser refuses says so', async () => {
  const bad = { getItem() { throw new Error('denied'); }, setItem() {}, removeItem() {} };
  const b = boot({ localStorage: bad }); await settle(); b.last().release(payload('51201')); await settle();
  const a = b.P.add({ lat: 21.7, lng: -158.2 }); await settle(); b.last().release(payload('pt_21700N_158200W')); await settle();
  (await a).open(); await settle();
  assert.match(b.note.textContent, /could not keep the point/); assert.deepEqual(b.stored(), []);
  b.P.say('unremoved', 'pt_21700N_158200W');
  assert.match(b.note.textContent, /could not change My points/); assert.equal(b.status.textContent, '');
});

test('textTip makes text, never HTML (K-8 and A-20 pinned by behaviour, G22 re-check X32)', () => {
  const line = TPL.split('\n').find((l) => l.indexOf('function textTip(t)') >= 0);
  const doc = new Document_();
  const textTip = new Function('document', line.trim() + '\nreturn textTip;')(doc);
  const el = textTip('<img src=x onerror=alert(1)> Reef');
  assert.equal(el.textContent, '<img src=x onerror=alert(1)> Reef'); assert.equal(el.children.length, 0);
  assert.equal(textTip(null).textContent, '');
});

test('the overlay\'s and the live panel\'s zone label: a nautical zone as the window writes it (G22 R-B4, found on the test site)', () => {
  const a = TPL.indexOf('    function tzAbbr(iso, tz) {'), b = TPL.indexOf('\n    }\n', a);
  assert.ok(a > 0 && b > a);
  const b0 = boot();
  const tzAbbr = new Function('window', TPL.slice(a, b + 6) + '\nreturn tzAbbr;')(b0.win);
  const iso = '2026-10-03T01:00:00Z';
  assert.equal(tzAbbr(iso, 'Etc/GMT+8'), b0.F.zoneLabel('Etc/GMT+8'));
  assert.equal(tzAbbr(iso, 'Etc/GMT+8'), 'UTC−8');
  assert.equal(tzAbbr(iso, 'Etc/GMT-12'), 'UTC+12');
  assert.equal(tzAbbr(iso, 'Pacific/Honolulu'), 'HST', 'a civil zone keeps its own abbreviation');
  assert.equal(new Function('window', TPL.slice(a, b + 6) + '\nreturn tzAbbr;')({})(iso, 'Etc/GMT+8'), 'GMT-8', 'before the module loads: Intl');
});
