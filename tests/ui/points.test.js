'use strict';
// Forecast points in the page (plan section 31): static_ui/forecast.js's point ids (the server's rule, checked against
// point_forecast.point_id through tests/fixtures/point_ids.json), the visitor's store, and the graphs' handling of a
// point's rows (hourly to +120 h, then 3-hourly). Node, no DOM.
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

function memory() {
  const m = new Map();
  return { m, getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)) };
}

test('pointId gives the server\'s id for every case point_forecast.point_id was asked (tests/fixtures/point_ids.json)', () => {
  const fx = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'fixtures', 'point_ids.json'), 'utf8'));
  assert.ok(fx.cases.length > 500);
  const bad = fx.cases.filter(([lat, lon, id]) => F.pointId(lat, lon) !== id);
  assert.deepEqual(bad, []);
  for (const [, , id] of fx.cases) {
    const c = F.parsePointId(id);
    assert.ok(c, id);
    assert.equal(F.pointId(c.lat, c.lon), id, 'an id round-trips through its coordinates');
  }
});

test('pointId and parsePointId: one spelling per point; anything else is refused', () => {
  assert.equal(F.pointId(21.667, -158.054), 'pt_21667N_158054W');
  assert.equal(F.pointId(0, 0), 'pt_0N_0E');
  assert.equal(F.pointId(52.45, 180), 'pt_52450N_180000W');
  assert.equal(F.pointId(21.35, -518.6), 'pt_21350N_158600W', 'a click in another world copy');
  for (const bad of [[NaN, 0], [0, Infinity], [90.001, 0], [-91, 0], ['x', 1]]) assert.equal(F.pointId(bad[0], bad[1]), null, String(bad));
  assert.deepEqual(F.parsePointId('pt_21667N_158054W'), { lat: 21.667, lon: -158.054 });
  for (const bad of ['pt_021667N_158054W', 'pt_0S_0E', 'pt_0N_0W', 'pt_21667N_180000E', 'pt_90001N_0E', 'pt_1N_180001W', 'pt_21667n_158054W',
    ' pt_21667N_158054W', 'pt_21667N_158054W ', 'pt_21.667N_158.054W', '51201', '', null, undefined, 5, {}, 'pt_123456N_5E']) {
    assert.equal(F.parsePointId(bad), null, String(bad));
  }
  assert.ok(F.isPointId('pt_x') && !F.isPointId('51201') && !F.isPointId(null));
  assert.equal(F.fmtPoint(21.667, -158.054), '21.667N 158.054W');
  assert.equal(F.fmtPoint(-14.4, 170.7), '14.400S 170.700E');
  assert.equal(F.pointLabel({ lat: 21.667, lon: -158.054, name: '' }), '21.667N 158.054W');
  assert.equal(F.pointLabel({ lat: 21.667, lon: -158.054, name: 'Pipeline' }), 'Pipeline — 21.667N 158.054W', 'the coordinates always show');
});

test('cleanName: no control characters, single spaces, 40 characters at most', () => {
  assert.equal(F.cleanName('  Haleiwa\u0007 outer \n\t reef  '), 'Haleiwa outer reef');
  assert.equal(F.cleanName('x'.repeat(60)).length, I.POINT_NAME_MAX);
  assert.equal(F.cleanName('a b\u0085c'), 'a b c');
  assert.equal(F.cleanName(null), '');
  assert.equal(F.cleanName('<img src=x onerror=alert(1)>'), '<img src=x onerror=alert(1)>', 'kept as text: the page writes names with textContent only');
});

test('the store: add (newest first), exists, rename, remove; the coordinates always come from the id', () => {
  const st = memory(), s = F.createPointStore(st);
  assert.deepEqual(s.list(), []);
  assert.equal(s.add('pt_21667N_158054W'), 'added');
  assert.equal(s.add('pt_14400S_170700W', 'Pago'), 'added');
  assert.equal(s.add('pt_21667N_158054W'), 'exists');
  assert.equal(s.add('pt_021667N_158054W'), 'invalid');
  assert.deepEqual(s.list().map((p) => p.id), ['pt_14400S_170700W', 'pt_21667N_158054W']);
  assert.deepEqual(s.get('pt_14400S_170700W'), { id: 'pt_14400S_170700W', lat: -14.4, lon: -170.7, name: 'Pago' });
  assert.equal(s.rename('pt_21667N_158054W', '  Pipeline  '), true);
  assert.equal(s.label('pt_21667N_158054W'), 'Pipeline — 21.667N 158.054W');
  assert.equal(s.label('pt_5N_5E'), '0.005N 0.005E', 'an id not kept still has a label');
  assert.equal(s.label('nope'), null);
  assert.equal(s.rename('pt_5N_5E', 'x'), false);
  assert.equal(s.remove('pt_14400S_170700W'), true);
  assert.equal(s.remove('pt_14400S_170700W'), false);
  assert.ok(s.has('pt_21667N_158054W') && !s.has('pt_14400S_170700W'));
  assert.deepEqual(JSON.parse(st.m.get(I.POINTS_KEY)), [{ id: 'pt_21667N_158054W', lat: 21.667, lon: -158.054, name: 'Pipeline' }]);
});

test('the store holds 50 points at most; a full store keeps the ones it has', () => {
  const s = F.createPointStore(memory());
  for (let i = 0; i < 50; i++) assert.equal(s.add(F.pointId(i / 10, i / 10)), 'added');
  assert.equal(F.POINTS_MAX, 50);
  assert.equal(s.add('pt_77000N_77000E'), 'full');
  assert.equal(s.list().length, 50);
  assert.equal(s.add(F.pointId(0.1, 0.1)), 'exists', 'an existing one is not "full"');
});

test('what the store reads back: bad entries are dropped, duplicates kept once, names cleaned, the id decides the place', () => {
  const st = memory();
  st.setItem(I.POINTS_KEY, JSON.stringify([
    { id: 'pt_21667N_158054W', lat: 99, lon: 99, name: 'Pipe\u0000line' },              // the stored coordinates are not trusted
    { id: 'pt_21667N_158054W', name: 'twice' }, { id: 'pt_0S_0E' }, { id: 51201 }, null, 'pt_1N_1E', { id: 'pt_1N_1E', name: 5 },
  ]));
  assert.deepEqual(F.createPointStore(st).list(), [
    { id: 'pt_21667N_158054W', lat: 21.667, lon: -158.054, name: 'Pipe line' }, { id: 'pt_1N_1E', lat: 0.001, lon: 0.001, name: '5' }]);
  for (const raw of ['{', '"x"', '{"a":1}', '5', 'null']) { st.setItem(I.POINTS_KEY, raw); assert.deepEqual(F.createPointStore(st).list(), [], raw); }
  st.setItem(I.POINTS_KEY, JSON.stringify(Array.from({ length: 80 }, (_, i) => ({ id: F.pointId(i, i) }))));
  assert.equal(F.createPointStore(st).list().length, 50, 'never more than 50, whatever was stored');
});

test('a storage that throws (private mode, quota): nothing is kept and nothing breaks', () => {
  const bad = { getItem() { throw new Error('SecurityError'); }, setItem() { throw new Error('QuotaExceededError'); } };
  const s = F.createPointStore(bad);
  assert.deepEqual(s.list(), []);
  assert.equal(s.add('pt_1N_1E'), 'unsaved');
  assert.equal(s.has('pt_1N_1E'), false);
  const ro = memory(); ro.setItem = () => { throw new Error('QuotaExceededError'); };
  assert.equal(F.createPointStore(ro).add('pt_1N_1E'), 'unsaved');
});

// a point's rows: hourly to +120 h, then 3-hourly to +384 h (the server's labels, in a zone 2 h off the 3-hourly UTC steps)
function pointLabels() {
  const days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
  const months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
  const hours = [];
  for (let h = 0; h <= 120; h++) hours.push(h);
  for (let h = 123; h <= 384; h += 3) hours.push(h);
  return hours.map((h) => {
    const d = new Date(Date.UTC(2026, 9, 1, 12 + h - 10));                  // run 2026100112, shown in Hawaii time
    const hh = d.getUTCHours(), h12 = hh % 12 || 12;
    return `${days[d.getUTCDay()]}, ${months[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.getUTCFullYear()} ${h12}:00 ${hh < 12 ? 'AM' : 'PM'}`;
  });
}

test('rangeWindow with the rows\' times: 7 and 3 days of a point\'s 209 rows, not 168 and 72 of them', () => {
  const labels = pointLabels(), times = labels.map(I.parseLabel);
  assert.equal(labels.length, 209);
  assert.deepEqual(I.rangeWindow(209, 7, times), { min: 0, max: 135 }, '+165 h is row 135 (3-hourly after +120 h), the last before +168 h');
  assert.equal(labels[135], 'Wednesday, October 7, 2026 11:00 PM');
  assert.deepEqual(I.rangeWindow(209, 3, times), { min: 0, max: 71 });
  assert.deepEqual(I.rangeWindow(209, 0, times), { min: 0, max: 208 });
  assert.deepEqual(I.rangeWindow(209, 30, times), { min: 0, max: 208 });
  assert.deepEqual(I.rangeWindow(385, 7), { min: 0, max: 167 }, 'without times: one row an hour, as before');
  assert.deepEqual(I.rangeWindow(5, 7, [new Date(1), new Date(2)]), { min: 0, max: 4 }, 'times of another length are not used');
});

test('the date labels: every day of a point\'s run, also where its 3-hourly rows never fall on midnight', () => {
  const labels = pointLabels(), parsed = labels.map(I.parseLabel);
  assert.ok(parsed.slice(121).every((d) => d.getHours() !== 0), 'Hawaii time: the 3-hourly rows are 2, 5, 8 ... 23 h');
  const mids = I.dayStarts(parsed);
  const shown = parsed.map((d, i) => I.dateTick({ width: 4000, min: 0, max: 208 }, parsed, mids, i)).filter(Boolean);
  assert.deepEqual(shown, ['10/2', '10/3', '10/4', '10/5', '10/6', '10/7', '10/8', '10/9', '10/10', '10/11', '10/12', '10/13', '10/14', '10/15', '10/16', '10/17']);
  assert.equal(parsed[mids[4]].getHours(), 0, 'hourly rows: the midnight');
  assert.equal(parsed[mids[5]].getHours(), 2, '3-hourly rows: the first row of the day');
  const noons = I.noonStarts(parsed);
  assert.ok(noons.every((i) => parsed[i].getHours() >= 12 && parsed[i].getHours() < 15));
  assert.equal(noons.length, 16);
  // hourly rows (a station): exactly the midnights and the noons, as before
  const st = []; for (let i = 0; i < 385; i++) st.push(new Date(2026, 8, 26, 14 + i));
  assert.deepEqual(I.dayStarts(st), st.map((d, i) => (d.getHours() === 0 ? i : -1)).filter((i) => i >= 0));
  assert.deepEqual(I.noonStarts(st), st.map((d, i) => (d.getHours() === 12 ? i : -1)).filter((i) => i >= 0));
  assert.deepEqual(I.dayStarts([new Date(2026, 0, 1, 0), new Date(2026, 0, 1, 1)]), [0], 'a series starting at midnight labels its first day');
});
