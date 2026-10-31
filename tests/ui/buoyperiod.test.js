'use strict';
// The live window's period cells (templates/index.html: isNum, fmtPeriod, buildRecentTable), plan section 69: a buoy
// without a peak period (Taiwan CWA's report a mean period only) printed "0.0 s" (Number(null) is 0).
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..', '..');
const TPL = fs.readFileSync(process.env.TEMPLATE || path.join(ROOT, 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');

function cut(from, to) {
  const a = TPL.indexOf(from); if (a < 0) throw new Error('not in the template: ' + from);
  const b = TPL.indexOf(to, a); if (b < 0) throw new Error('not in the template: ' + to);
  return TPL.slice(a, b);
}
const SRC = cut('    function isNum(v)', '    function fmtDir(d)') + cut('    function buildRecentTable(', '    function buildPartitionTable(');
const page = new Function('fmtTimeInTz', 'tzAbbr', 'fmtHeightM', 'fmtTemp', 'fmtDir',
  SRC + '\nreturn { isNum, fmtPeriod, buildRecentTable };')(
  (t) => t, () => 'GMT', (m) => (m == null ? '—' : m.toFixed(1) + ' m'), (c) => c + ' °C', (d) => (d == null ? '—' : d + '°'));

test('a missing period is a dash, never 0.0 s', () => {
  for (const v of [null, undefined, '', 'x', NaN]) assert.equal(page.fmtPeriod(v), '—');
  assert.equal(page.fmtPeriod(0), '0.0 s');
  assert.equal(page.fmtPeriod('12.34'), '12.3 s');
  assert.equal(page.isNum(0), true);
  assert.equal(page.isNum(null), false);
});

test('the Tp column is left out when no row has one and a mean period exists', () => {
  const cwa = [{ time_utc: 'a', hs_m: 1.1, tp_s: null, mean_period_s: 5.1, dir_deg: 101 },
               { time_utc: 'b', hs_m: 1.3, tp_s: null, mean_period_s: 5.0, dir_deg: 78 }];
  const t = page.buildRecentTable(cwa, true, 'Asia/Taipei');
  assert.ok(!t.includes('>Tp<') && t.includes('Avg&nbsp;Pd') && !t.includes('0.0') && t.includes('5.1'));
  const mixed = page.buildRecentTable([{ time_utc: 'a', hs_m: 1, tp_s: null, mean_period_s: 5 }, { time_utc: 'b', hs_m: 1, tp_s: 9, mean_period_s: 5 }], true);
  assert.ok(mixed.includes('>Tp<') && mixed.includes('<td>9.0</td>') && mixed.includes('<td>—</td>'));
  const tpOnly = page.buildRecentTable([{ time_utc: 'a', hs_m: 1, tp_s: null }], true);
  assert.ok(tpOnly.includes('>Tp<'));                            // no period at all: the column stays (a dash)
});
