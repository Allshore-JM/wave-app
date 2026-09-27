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
const BLOCK = html.slice(html.indexOf('\n', a) + 1, b);
const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'forecast.js'), 'utf8');
const w = {}; new Function('window', 'URLSearchParams', SRC)(w, URLSearchParams);
const I = w.AllshoreForecast._internals;

function runHead(search, stored, storage) {
  const urls = [];
  const ctx = { URLSearchParams, JSON, location: { search }, window: { __early: {} },
    localStorage: storage || { getItem: (k) => (k === 'allshore.settings.v1' && stored !== undefined ? (typeof stored === 'string' ? stored : JSON.stringify(stored)) : null) },
    fetch: (u) => { urls.push(u); return Promise.resolve({}); } };
  vm.runInNewContext(BLOCK, ctx);
  return { early: ctx.window.__early.forecast, urls };
}
// what the server hands the module for a GET with this address (index(): tz '' / unit US / model GFS by default)
function server(search) {
  const p = new URLSearchParams(search);
  const station = (p.get('station') || '').trim() || '51201';
  return { station, tz: p.get('tz') || '', unit: p.get('unit') === 'Metric' ? 'Metric' : 'US', model: (p.get('model') || '').toUpperCase() === 'SWAN' ? 'SWAN' : 'GFS', view: 'Table' };
}

const CASES = [
  ['?station=51201', undefined], ['', undefined], ['?station=46001&tz=UTC&unit=Metric', undefined],
  ['?station=51201&model=SWAN&view=Graph', { tz: 'Europe/Lisbon', unit: 'Metric' }], ['?station=51201&tz=', { tz: 'UTC' }],
  ['?station=51201', { tz: 'America/New_York', unit: 'US' }], ['?station=51201&unit=bogus', { unit: 'Metric' }],
  ['?station=51201', '{not json'], ['?station=51201', { tz: 5, unit: null }], ['?station=51201&model=swan', undefined],
  ['?station=51201&tz=Pacific%2FHonolulu&unit=US', { tz: 'UTC', unit: 'Metric' }],
];

test('the head asks for exactly the query of the module\'s first load, for every address / saved-settings combination', () => {
  for (const [search, stored] of CASES) {
    const h = runHead(search, stored);
    let st = {}; try { st = typeof stored === 'string' ? JSON.parse(stored) : (stored || {}); } catch (e) { st = {}; }
    const want = I.queryFor(I.resolveInitialState(search, st, server(search)));
    assert.equal(h.early.q, want, search + ' / ' + JSON.stringify(stored));
    assert.deepEqual(h.urls, ['/api/forecast?' + want]);
  }
});

test('no early request for ?render=full (the table is in the page); a throwing storage still gets one', () => {
  assert.equal(runHead('?station=51201&render=full').early, undefined);
  const h = runHead('?station=51201', undefined, { getItem: () => { throw new Error('blocked'); } });
  assert.equal(h.early.q, I.queryFor(I.resolveInitialState('?station=51201', {}, server('?station=51201'))));
});
