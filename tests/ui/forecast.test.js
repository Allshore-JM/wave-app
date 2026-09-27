'use strict';
// Unit tests for static_ui/forecast.js (plan section 25), evaluated in Node without a DOM.
//   node --test tests/ui/
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

function load() {
  const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'forecast.js'), 'utf8');
  const win = {};
  new Function('window', 'URLSearchParams', src)(win, URLSearchParams);
  return win.AllshoreForecast;
}
const F = load(), I = F._internals;

test('the module touches nothing at load and exposes init', () => {
  assert.equal(typeof F.init, 'function');
  assert.doesNotThrow(() => F.init());
});

test('resolveInitialState: URL beats saved settings beats the server; an empty tz in the URL means Buoy Local', () => {
  const server = { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  assert.deepEqual(I.resolveInitialState('', {}, server), { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' });
  assert.deepEqual(I.resolveInitialState('', { tz: 'Pacific/Honolulu', unit: 'Metric' }, server),
    { station: '51201', tz: 'Pacific/Honolulu', unit: 'Metric', model: 'GFS', view: 'Table' });
  assert.deepEqual(I.resolveInitialState('?station=46001&tz=&unit=US&model=swan&view=Graph', { tz: 'UTC', unit: 'Metric' }, server),
    { station: '46001', tz: '', unit: 'US', model: 'SWAN', view: 'Graph' }, 'URL wins, incl. an explicit empty tz');
  assert.equal(I.resolveInitialState('?unit=Furlongs', {}, server).unit, 'US', 'unknown unit -> US');
  assert.equal(I.resolveInitialState('?model=ECMWF', {}, server).model, 'GFS', 'unknown model -> GFS');
  assert.equal(I.resolveInitialState('?view=Chart', {}, server).view, 'Table');
  assert.equal(I.resolveInitialState('', { unit: 5, tz: null }, server).unit, 'US', 'a corrupted setting is ignored');
  assert.equal(I.resolveInitialState('', {}, {}).station, '51201');
});

test('urlFor writes the station always, tz/unit when they differ from the saved settings (else the defaults), model/view when not default; queryFor carries the fetch params', () => {
  const base = { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  assert.equal(I.urlFor(base), '?station=51201');
  assert.equal(I.urlFor({ ...base, tz: 'Pacific/Honolulu', unit: 'Metric', model: 'SWAN', view: 'Graph' }),
    '?station=51201&tz=Pacific%2FHonolulu&unit=Metric&model=SWAN&view=Graph');
  const saved = { tz: 'Pacific/Honolulu', unit: 'Metric' };
  assert.equal(I.urlFor({ ...base, tz: 'Pacific/Honolulu', unit: 'Metric' }, saved), '?station=51201', 'the saved settings are the reload\'s assumption');
  assert.equal(I.urlFor(base, saved), '?station=51201&tz=&unit=US', 'Buoy Local and US must be named when the saved settings differ');
  for (const st of [{ ...base }, { ...base, tz: 'UTC', unit: 'Metric' }, { ...base, tz: '' }]) {
    for (const sv of [{}, saved, { tz: 'UTC' }, { unit: 5, tz: null }]) {
      const back = I.resolveInitialState(I.urlFor(st, sv), sv, { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' });
      assert.deepEqual(back, st, 'round trip with saved ' + JSON.stringify(sv) + ' for ' + JSON.stringify(st));
    }
  }
  assert.equal(I.queryFor({ ...base, view: 'Graph' }), 'station=51201&tz=&unit=US&model=GFS&compact=1');
  assert.deepEqual(I.resolveInitialState(I.urlFor({ ...base, unit: 'Metric', view: 'Graph' }), {}, {}),
    { ...base, unit: 'Metric', view: 'Graph' }, 'urlFor round-trips through resolveInitialState');
});

test('keyOf separates station, tz, unit and model, and ignores the view', () => {
  const s = { station: '51201', tz: '', unit: 'US', model: 'GFS', view: 'Table' };
  assert.equal(I.keyOf(s), I.keyOf({ ...s, view: 'Graph' }));
  for (const k of ['station', 'tz', 'unit', 'model']) assert.notEqual(I.keyOf(s), I.keyOf({ ...s, [k]: s[k] + 'x' }), k);
});

test('clampGeometry keeps the whole window on screen below the top bar, shrinking it to fit but never below the minimum', () => {
  const min = { w: 360, h: 220 };
  assert.deepEqual(I.clampGeometry({ x: 100, y: 120, w: 800, h: 400 }, 1280, 800, 56, min), { x: 100, y: 120, w: 800, h: 400 }, 'already inside');
  assert.deepEqual(I.clampGeometry({ x: 900, y: 600, w: 800, h: 400 }, 1280, 800, 56, min), { x: 472, y: 392, w: 800, h: 400 }, 'pulled back');
  assert.deepEqual(I.clampGeometry({ x: -50, y: 0, w: 2000, h: 1200 }, 1280, 800, 56, min), { x: 8, y: 64, w: 1264, h: 728 }, 'shrunk to the viewport');
  assert.deepEqual(I.clampGeometry({ x: 10, y: 70, w: 100, h: 50 }, 1280, 800, 56, min), { x: 10, y: 70, w: 360, h: 220 }, 'minimum size');
  const tiny = I.clampGeometry({ x: 0, y: 0, w: 900, h: 600 }, 300, 250, 56, min);   // a viewport smaller than the minimum
  assert.ok(tiny.w <= 284 && tiny.h <= 178 && tiny.x >= 8 && tiny.y >= 64, JSON.stringify(tiny));
});

test('readJson / writeJson never throw: missing, corrupted or foreign values read as empty; a throwing storage is survived', () => {
  const m = new Map(), st = { getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)) };
  assert.deepEqual(I.readJson(st, I.SETTINGS_KEY), {});
  for (const raw of ['5', '"x"', '[1]', 'null', '{bad']) { m.set(I.SETTINGS_KEY, raw); assert.deepEqual(I.readJson(st, I.SETTINGS_KEY), {}, raw); }
  assert.equal(I.writeJson(st, I.SETTINGS_KEY, { unit: 'Metric' }), true);
  assert.deepEqual(I.readJson(st, I.SETTINGS_KEY), { unit: 'Metric' });
  const bad = { getItem() { throw new Error('SecurityError'); }, setItem() { throw new Error('QuotaExceeded'); } };
  assert.deepEqual(I.readJson(bad, I.WINDOW_KEY), {});
  assert.equal(I.writeJson(bad, I.WINDOW_KEY, {}), false);
});
