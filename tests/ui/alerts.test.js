'use strict';
// static_ui/alerts.js (plan section 90): the polyline decoder against the builder's own decoder on a real served
// answer, the zone colours (strongest in force; pending and watches yellow dashed; ended gone), the popup's order and
// text-only content, world copies, and create(): nothing asked before the page is ready, below zoom 5, while hidden
// or with the layer off; the 2-minute refresh, "loading" / failure retries and notes; the lines under the forecast dots
// on the shared canvas; the credit once; a minute's tick turning a pending product to its colour; a running map tool
// taking the click.   node --test tests/ui/alerts.test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'alerts.js'), 'utf8');
function load(win) { new Function('window', SRC)(win); return win.AllshoreAlerts; }
const A = load({});
const I = A._internals;
const FIX = path.join(__dirname, '..', 'fixtures', 'alerts');
const SERVED = JSON.parse(fs.readFileSync(path.join(FIX, 'served.json'), 'utf8'));
const EXPECT = JSON.parse(fs.readFileSync(path.join(FIX, 'served_expect.json'), 'utf8'));
const NOW = EXPECT.now;                                     // 2026-11-08 23:30 UTC, the answer's own time
const settle = async () => { for (let i = 0; i < 8; i++) await new Promise((r) => setImmediate(r)); };
const H = 3600;

function alert(cls, onset, ends, extra) {
  return Object.assign({ id: cls + onset, event: cls === 'beach' ? 'Rip Current Statement' : 'High Surf ' + cls, cls,
                         sent: onset - 600, onset, ends, expires: ends, zones: ['HIZ007'] }, extra || {});
}

test('the decoder: Google\'s own example, damaged strings refused', () => {
  assert.deepEqual(I.decodePolyline('_p~iF~ps|U_ulLnnqC_mqNvxq`@'), [[38.5, -120.2], [40.7, -120.95], [43.252, -126.453]]);
  assert.equal(I.decodePolyline('_p~iF~ps|'), null, 'cut short inside the second value');
  assert.equal(I.decodePolyline('_p~iF~ps|U'), null, 'one point is no line');
  assert.equal(I.decodePolyline('_p~iF~ps|U_ulLnnqC_mqNvxq`@ '), null, 'a character outside the alphabet');
  assert.equal(I.decodePolyline(''), null);
  assert.equal(I.decodePolyline(7), null);
  assert.equal(I.decodePolyline('~~~~~~~~~~~?'), null, 'a value longer than 35 bits');
});

test('the decoder = the builder\'s decoder on every line of a real served answer', () => {
  const zones = Object.keys(SERVED.zones);
  assert.equal(zones.length, 22);
  let n = 0;
  zones.forEach((z) => {
    const want = EXPECT.lines[z];
    SERVED.zones[z].lines.forEach((s, i) => {
      const p = I.decodePolyline(s);
      assert.ok(p, z + ' line ' + i + ' decodes');
      assert.equal(p.length, want[i][0]);
      assert.deepEqual(p[0], [want[i][1][1], want[i][1][0]], 'first point (builder gives lon, lat)');
      assert.deepEqual(p[p.length - 1], [want[i][2][1], want[i][2][0]]);
      n++;
    });
  });
  assert.equal(n, 270);
});

test('the zone colour: the strongest product in force; pending and watches yellow dashed; ended gone', () => {
  const C = I.COLORS, t = NOW;
  const s = (list) => I.zoneStyle(list, t);
  assert.deepEqual(s([alert('advisory', t - H, t + H), alert('warning', t - H, t + H)]),
    { cls: 'warning', color: C.warning, dashed: false, weight: I.WEIGHT_WARNING });
  assert.equal(s([alert('flood_statement', t - H, t + H)]).color, C.flood_statement);
  assert.equal(s([alert('beach', t - H, t + H)]).color, C.advisory, 'rip / beach statements orange like advisories');
  assert.equal(s([alert('flood_statement', t - H, t + H), alert('beach', t - H, t + H)]).cls, 'beach');
  assert.deepEqual(s([alert('warning', t + H, t + 2 * H)]), { cls: 'pending', color: C.watch, dashed: true, weight: I.WEIGHT },
    'a warning issued but not yet in effect: yellow dashed');
  assert.equal(s([alert('warning', t + H, t + 2 * H), alert('advisory', t - H, t + H)]).cls, 'advisory',
    'a pending warning does not outrank an advisory in force');
  assert.equal(s([alert('watch', t - H, t + H)]).dashed, true, 'a watch in effect: yellow dashed');
  assert.equal(s([alert('warning', t - 2 * H, t)]), null, 'ended at this second: gone');
  assert.equal(s([alert('mystery', t - H, t + H)]), null);
  assert.equal(s([]), null);
  assert.equal(s([alert('advisory', t - H, null, { expires: t + H })]).cls, 'advisory', 'no end of its own: the expiry');
  assert.equal(s([alert('advisory', null, t + H, { onset: null, sent: t + 60 })]).dashed, true, 'no onset: from issue');
});

test('the popup\'s order: in force by strength, then pending by onset; ended left out', () => {
  const t = NOW;
  const list = [alert('advisory', t + 3 * H, t + 4 * H, { id: 'p2' }), alert('flood_statement', t - H, t + H, { id: 'f' }),
                alert('warning', t - H, t + H, { id: 'w' }), alert('advisory', t + H, t + 4 * H, { id: 'p1' }),
                alert('warning', t - 3 * H, t - H, { id: 'gone' })];
  assert.deepEqual(I.popupOrder(list, t).map((a) => a.id), ['w', 'f', 'p1', 'p2']);
});

test('world copies: every 360 whose copy meets the view', () => {
  assert.deepEqual(I.worldOffsets(-170, -150), [0]);
  assert.deepEqual(I.worldOffsets(170, 200), [0, 360]);
  assert.deepEqual(I.worldOffsets(-200, -170), [-360, 0]);
  assert.deepEqual(I.worldOffsets(190, 260), [360]);
  assert.deepEqual(I.worldOffsets(-180, 180), [0]);
  assert.equal(I.worldOffsets(-2000, 2000).length, 4, 'never more than four copies');
});

test('times in the zone\'s own time; an unknown zone falls back to UTC', () => {
  const t = Date.UTC(2026, 10, 9, 4, 0) / 1000;           // Mon 9 Nov 04:00 UTC = Sun 8 Nov 6:00 PM HST
  assert.equal(I.fmtTime(t, 'Pacific/Honolulu'), 'Sun 6:00 PM HST');
  assert.equal(I.fmtTime(t, 'Not/AZone'), 'Mon 4:00 AM UTC');
  const a = alert('warning', NOW - H, t);
  assert.equal(I.whenText(a, NOW, 'Pacific/Honolulu'), 'In effect until Sun 6:00 PM HST');
  assert.equal(I.whenText(alert('advisory', t, t + 2 * H), NOW, 'Pacific/Honolulu'), 'From Sun 6:00 PM HST until Sun 8:00 PM HST');
  assert.equal(I.whenText(alert('advisory', NOW - H, null, { expires: t }), NOW, 'Pacific/Honolulu'), 'In effect until at least Sun 6:00 PM HST');
  assert.equal(I.whenText(alert('watch', NOW - H, t), NOW, 'Pacific/Honolulu'), 'Watch in effect until Sun 6:00 PM HST');
});

function fakeDoc() {
  const listeners = {};
  return {
    hidden: false, readyState: 'complete',
    createElement(tag) {
      return { tag, className: '', textContent: '', style: {}, children: [], href: '', target: '', rel: '',
               appendChild(c) { this.children.push(c); return c; } };
    },
    addEventListener(t, f) { (listeners[t] = listeners[t] || []).push(f); },
    fire(t) { (listeners[t] || []).forEach((f) => f()); }
  };
}
const texts = (node) => [node.textContent].concat(...node.children.map(texts)).filter(Boolean);

test('the popup: the products as text (never HTML), the zone link only for a forecast zone', () => {
  const doc = fakeDoc(), t = NOW;
  const evil = '<img src=x onerror=alert(1)>';
  const list = [alert('advisory', t + H, t + 2 * H, { headline: 'LATER', description: evil, instruction: 'Stay out.', office: 'NWS Honolulu HI' }),
                alert('warning', t - H, t + H, { headline: 'NOW', description: 'Big surf.' })];
  const box = I.buildPopup(doc, 'HIZ007', { name: 'Oahu North Shore', tz: 'Pacific/Honolulu' }, list, t);
  const tx = texts(box);
  assert.equal(tx[0], 'Oahu North Shore');
  assert.ok(tx.indexOf('High Surf warning') < tx.indexOf('High Surf advisory (not yet in effect)'), 'in force first');
  assert.ok(tx.includes(evil), 'the provider\'s words stay text');
  assert.ok(tx.includes('Stay out.') && tx.includes('NOW') && tx.includes('LATER'));
  assert.ok(tx.some((x) => /^Issued .* by NWS Honolulu HI$/.test(x)));
  assert.ok(tx.some((x) => /^Issued .* by the NWS$/.test(x)), 'no office: "the NWS"');
  const items = box.children.filter((c) => c.className === 'alert-item');
  assert.equal(items[0].style.borderLeftColor, I.COLORS.warning);
  assert.equal(items[1].style.borderLeftColor, I.COLORS.watch, 'a pending product: the yellow mark');
  const link = box.children[box.children.length - 1];
  assert.equal(link.href, 'https://forecast.weather.gov/MapClick.php?zoneid=HIZ007');
  assert.equal(link.rel, 'noopener');
  const county = I.buildPopup(doc, 'TXC123', { name: 'X', tz: 'America/Chicago' }, list, t);
  assert.ok(!county.children.some((c) => c.className === 'alert-link'), 'a county zone: no forecast-zone link');
});

test('the answer: shapes checked; unchanged zones keep their decoded points; unknown classes left out', () => {
  assert.equal(I.takeAnswer(null), null);
  assert.equal(I.takeAnswer({ format: 'x', state: 'ok', alerts: [], zones: {} }), null);
  assert.equal(I.takeAnswer({ format: I.FORMAT, state: 'ok', alerts: {}, zones: {} }), null);
  assert.equal(I.takeAnswer({ format: I.FORMAT, state: 'ok', alerts: [], zones: [] }), null);
  const d1 = I.takeAnswer(SERVED, null);
  assert.equal(d1.state, 'ok');
  assert.equal(d1.alerts.length, 10);
  assert.equal(Object.keys(d1.byZone).length, 22);
  assert.equal(d1.byZone.TXZ451.length, 2, 'a zone named by two products');
  const j2 = JSON.parse(JSON.stringify(SERVED));
  j2.alerts.push({ event: 'Odd', cls: 'mystery', zones: ['ORZ103'] }, { event: 'X', cls: 'warning', zones: ['NOPE99'] }, null);
  j2.zones.ORZ103.lines = j2.zones.ORZ103.lines.slice(0, 1);
  j2.zones.BAD000 = { name: 'bad', tz: 'UTC', lines: ['~'] };
  const d2 = I.takeAnswer(j2, d1.zones);
  assert.equal(d2.alerts.length, 11, 'the warning naming an unknown zone is kept (draws nowhere), the odd class is not');
  assert.ok(!d2.byZone.NOPE99 && !d2.zones.BAD000, 'a zone without a readable line is left out');
  assert.equal(d2.zones.CAZ103.lines, d1.zones.CAZ103.lines, 'an unchanged zone keeps its points');
  assert.notEqual(d2.zones.ORZ103.lines, d1.zones.ORZ103.lines);
  assert.equal(d2.zones.ORZ103.lines.length, 1);
});

// ---- create(): a fake Leaflet, map, clock and fetch ----
function fakeL() {
  class Path {
    constructor(ll, o) { this.ll = ll; this.options = o || {}; this.handlers = {}; }
    on(t, f) { this.handlers[t] = f; return this; }
    bindTooltip(t) { this.tip = t; return this; }
    addTo(g) { g.addLayer(this); return this; }
    bringToBack() { const it = this._g.items; it.splice(it.indexOf(this), 1); it.unshift(this); return this; }
    _clickTolerance() { return (this.options.weight || 0) / 2 + 8; }
    closestLayerPoint(p) { return this.near === undefined ? null : { x: p.x, y: p.y, distance: this.near }; }
  }
  Path.extend = function (props) { class Sub extends Path {} Object.assign(Sub.prototype, props); return Sub; };
  class Popup {
    constructor(o) { this.o = o; }
    setLatLng(ll) { this.ll = ll; return this; }
    setContent(c) { this.c = c; return this; }
    openOn(m) { m.opened = this; return this; }
  }
  return { Polyline: Path, polyline: (ll, o) => new Path(ll, o), popup: (o) => new Popup(o) };
}
function fakeGroup() {
  return { items: [], clearLayers() { this.items = []; }, addLayer(x) { x._g = this; this.items.push(x); } };
}
function fakeMap(layer) {
  const ev = {};
  const m = {
    zoom: 9, west: -161, east: -154, layers: new Set([layer]), attr: [], closed: 0, opened: null,
    hasLayer(l) { return this.layers.has(l); }, getZoom() { return this.zoom; },
    getBounds() { const w = this.west, e = this.east; const b = { pad() { return b; }, getWest: () => w, getEast: () => e }; return b; },
    on(types, f) { types.split(' ').forEach((t) => (ev[t] = ev[t] || []).push(f)); },
    fire(t, e) { (ev[t] || []).forEach((f) => f(e)); },
    attributionControl: { addAttribution(c) { m.attr.push('+' + c); }, removeAttribution(c) { m.attr.push('-' + c); } },
    getSize() { return { x: 1280, y: 800 }; }, closePopup() { this.closed++; }
  };
  return m;
}
function fakeClock(start) {
  const c = { t: start, timers: new Map(), id: 0 };
  c.now = () => c.t;
  c.set = (f, ms) => { const id = ++c.id; c.timers.set(id, { f, at: c.t + ms }); return id; };
  c.clear = (id) => { c.timers.delete(id); };
  c.advance = (ms) => {
    const end = c.t + ms;
    for (;;) {
      let next = null;
      for (const [id, x] of c.timers) if (x.at <= end && (!next || x.at < next[1].at)) next = [id, x];
      if (!next) break;
      c.timers.delete(next[0]); c.t = next[1].at; next[1].f();
    }
    c.t = end;
  };
  return c;
}
function answer(body, ok = true, status = 200) {
  return Promise.resolve({ ok, status, json: async () => (typeof body === 'function' ? body() : body) });
}
function rig(opts) {
  opts = opts || {};
  const L = fakeL(), layer = fakeGroup(), map = fakeMap(layer), doc = fakeDoc(), clk = fakeClock(NOW * 1000);
  const calls = [], notes = [];
  let start = null, coarse = false;
  const st = {
    L, layer, map, doc, clk, calls, notes, reply: () => answer(SERVED), toolOn: false,
    set coarse(v) { coarse = v; },
    start: () => start()
  };
  st.A = A.create(map, {
    L, document: doc, layer, renderer: { r: 1 }, now: clk.now, timers: { set: clk.set, clear: clk.clear },
    fetch: (u, o) => { calls.push([u, o]); return st.reply(); },
    coarse: () => coarse, tip: (t) => ({ tip: t }), onNote: (t) => notes.push(t),
    tool: () => st.toolOn, whenReady: (s) => { start = s; }
  });
  return st;
}

test('nothing is asked before the page is ready, below zoom 5, while hidden or with the layer off', async () => {
  const r = rig();
  r.map.fire('moveend');
  assert.equal(r.calls.length, 0, 'before the start');
  r.map.zoom = 4.9; r.start(); await settle();
  assert.equal(r.calls.length, 0, 'below zoom 5');
  r.doc.hidden = true; r.map.zoom = 5; r.map.fire('zoomend'); await settle();
  assert.equal(r.calls.length, 0, 'a hidden tab');
  r.map.layers.clear(); r.doc.hidden = false; r.doc.fire('visibilitychange'); await settle();
  assert.equal(r.calls.length, 0, 'the layer off');
  r.map.layers.add(r.layer); r.map.fire('overlayadd', { layer: r.layer }); await settle();
  assert.equal(r.calls.length, 1);
  assert.equal(r.calls[0][0], '/api/alerts/coastal');
  assert.equal(r.calls[0][1].cache, 'no-cache', 'revalidated by the ETag');
});

test('drawn: every zone\'s lines in force or pending, under the dots, halos lowest; one credit; refreshed every 2 minutes', async () => {
  const r = rig();
  r.map.west = -170; r.map.east = -60;                     // the US mainland and Hawaii in one view
  r.start(); await settle();
  const s = r.A.state();
  assert.equal(s.drawn, 270, 'one line per zone line (22 zones)');
  assert.equal(r.layer.items.length, 540, 'a halo under each');
  const halos = r.layer.items.slice(0, 270), lines = r.layer.items.slice(270);
  assert.ok(halos.every((h) => h.options.interactive === false && h.options.color === 'rgba(0,0,0,0.55)'), 'halos first (lowest)');
  assert.ok(lines.every((l) => l.options.interactive === true && l.options.bubblingMouseEvents === false));
  assert.ok(lines.every((l) => l.options.renderer.r === 1 && halos[0].options.renderer.r === 1), 'the map\'s own canvas');
  assert.equal(lines[0]._clickTolerance(), lines[0].options.weight / 2 + 8 + I.HIT_PX, 'a wider reach than the line');
  const color = (z) => lines.find((l) => r.A.state().data.byZone[z] && l.ll === r.A.state().data.zones[z].lines[0]).options;
  assert.equal(color('ORZ103').color, I.COLORS.warning);
  assert.equal(color('ORZ103').weight, I.WEIGHT_WARNING);
  assert.equal(color('CAZ103').color, I.COLORS.watch, 'Eureka\'s advisory from 10 AM tomorrow: pending');
  assert.equal(color('CAZ103').dashArray, I.DASH);
  assert.equal(color('FLZ168').color, I.COLORS.flood_statement);
  assert.equal(color('PRZ001').color, I.COLORS.advisory, 'a rip current statement');
  assert.equal(color('TXZ451').color, I.COLORS.advisory, 'rip current outranks the flood statement on the same zone');
  assert.ok(lines.every((l) => l.tip && l.tip.tip.includes(' · ')), 'a hover label on a mouse screen');
  assert.deepEqual(r.map.attr, ['+' + I.CREDIT]);
  assert.deepEqual(r.notes, ['loading…', '']);
  assert.equal(r.calls.length, 1);
  r.map.fire('moveend'); await settle();
  assert.equal(r.calls.length, 1, 'a move asks nothing within the 2 minutes');
  assert.equal(r.layer.items.length, 540, 'the same set: not redrawn');
  r.clk.advance(I.REFRESH_MS); await settle();
  assert.equal(r.calls.length, 2, 'asked again after 2 minutes');
  assert.deepEqual(r.map.attr, ['+' + I.CREDIT], 'the credit added once');
});

test('world copies: a view across the date line draws both copies', async () => {
  const r = rig();
  r.reply = () => answer(Object.assign({}, SERVED, { alerts: SERVED.alerts.filter((a) => a.zones.includes('ORZ103')) }));
  r.map.west = 100; r.map.east = 260;
  r.start(); await settle();
  const lines = r.layer.items.filter((l) => l.options.interactive);
  const n = SERVED.zones.ORZ103.lines.length;
  assert.equal(lines.length, 2 * n);
  const lngs = lines.map((l) => l.ll[0][1]).sort((a, b) => a - b);
  assert.ok(lngs[0] < -120 && lngs[lngs.length - 1] > 230, 'the copy east of the date line is shifted by 360');
});

test('a minute later the pending advisory turns orange at its onset; the Oregon warnings go at their end', async () => {
  const r = rig();
  r.map.west = -170; r.map.east = -60;
  r.start(); await settle();
  const z = (zone) => r.layer.items.find((l) => l.options.interactive && l.ll === r.A.state().data.zones[zone].lines[0]);
  assert.equal(z('CAZ103').options.color, I.COLORS.watch);
  r.reply = () => new Promise(() => {});                   // the server never answers again: the page judges by itself
  const onset = Date.UTC(2026, 10, 9, 18, 0) / 1000;     // CAZ103's onset; the Oregon warnings ended at 06:00 UTC
  r.clk.advance((onset - NOW) * 1000 + I.TICK_MS);
  await settle();
  assert.equal(z('CAZ103').options.color, I.COLORS.advisory);
  assert.equal(z('CAZ103').options.dashArray, null);
  assert.equal(z('ORZ103'), undefined, 'ended: not drawn');
});

test('"loading" retries in 20 s; a failure keeps the held lines and retries in a minute; the notes follow', async () => {
  const r = rig();
  r.reply = () => answer({ format: I.FORMAT, state: 'loading', updated: null, alerts: [], zones: {} });
  r.start(); await settle();
  assert.equal(r.A.state().note, 'loading…');
  r.reply = () => answer(SERVED);
  r.clk.advance(I.LOADING_RETRY_MS); await settle();
  assert.equal(r.calls.length, 2);
  assert.equal(r.A.state().note, '');
  const drawn = r.A.state().drawn;
  assert.ok(drawn > 0);
  r.reply = () => answer({ error: 'x' }, false, 503);
  r.clk.advance(I.REFRESH_MS); await settle();
  assert.equal(r.calls.length, 3);
  assert.equal(r.A.state().drawn, drawn, 'the held products stay');
  assert.equal(r.A.state().note, '', 'a held good list: no note');
  r.clk.advance(I.FAIL_RETRY_MS - 1); await settle();
  assert.equal(r.calls.length, 3);
  r.clk.advance(1); await settle();
  assert.equal(r.calls.length, 4, 'retried after a minute');
  const r2 = rig();
  r2.reply = () => Promise.reject(new Error('offline'));
  r2.start(); await settle();
  assert.equal(r2.A.state().note, 'unavailable');
  r2.reply = () => answer(Object.assign({}, SERVED, { state: 'stale', alerts: [], zones: {} }));
  r2.clk.advance(I.FAIL_RETRY_MS); await settle();
  assert.equal(r2.A.state().note, 'unavailable', 'the server holds nothing recent');
  r2.reply = () => answer({ nope: 1 });
  r2.clk.advance(I.FAIL_RETRY_MS); await settle();
  assert.equal(r2.A.state().note, 'unavailable', 'an answer of another shape');
});

test('the layer off: lines, credit, note and timers go; zoom out: lines go, nothing asked', async () => {
  const r = rig();
  r.start(); await settle();
  assert.ok(r.A.state().drawn > 0);
  r.map.zoom = 4; r.map.fire('zoomend'); await settle();
  assert.equal(r.A.state().drawn, 0);
  assert.equal(r.layer.items.length, 0);
  assert.equal(r.map.attr[r.map.attr.length - 1], '-' + I.CREDIT);
  r.clk.advance(I.REFRESH_MS); await settle();
  assert.equal(r.calls.length, 1, 'below the gate the timer asks nothing');
  r.map.zoom = 9; r.map.fire('zoomend'); await settle();
  assert.equal(r.calls.length, 2, 'back at the gate after the 2 minutes: asked');
  r.map.layers.clear(); r.map.fire('overlayremove', { layer: r.layer }); await settle();
  assert.equal(r.A.state().drawn, 0);
  assert.equal(r.A.state().fetchTimer, false);
  assert.equal(r.A.state().tickTimer, false);
  r.clk.advance(10 * I.REFRESH_MS); await settle();
  assert.equal(r.calls.length, 2);
});

test('a click opens the zone\'s products; a running map tool takes it instead; touch screens get no hover label', async () => {
  const r = rig();
  r.start(); await settle();
  const line = r.layer.items.find((l) => l.options.interactive && l.ll === r.A.state().data.zones.ORZ103.lines[0]);
  r.toolOn = true;
  line.handlers.click({ latlng: { lat: 44.6, lng: -124.1 } });
  assert.equal(r.map.opened, null, 'the tool took the click');
  r.toolOn = false;
  line.handlers.click({ latlng: { lat: 44.6, lng: -124.1 } });
  assert.ok(r.map.opened, 'a popup');
  assert.equal(r.A.state().popupZone, 'ORZ103');
  assert.equal(r.map.opened.o.className, 'alert-popup');
  assert.ok(texts(r.map.opened.c).includes('Central Coast of Oregon'));
  assert.equal(r.map.opened.o.maxWidth, 340, 'a desktop popup: 340 px wide at most');
  r.map.getSize = () => ({ x: 375, y: 812 });
  line.handlers.click({ latlng: { lat: 44.6, lng: -124.1 } });
  assert.equal(r.map.opened.o.maxWidth, 375 - I.POP_CHROME, 'a phone: the popup and its pan padding fit the screen');
  r.map.getSize = () => ({ x: 240, y: 400 });
  line.handlers.click({ latlng: { lat: 44.6, lng: -124.1 } });
  assert.equal(r.map.opened.o.maxWidth, 200, 'never below 200 px');
  line.near = 2.5;
  assert.equal(line._lineDistance({ x: 1, y: 1 }), 2.5, "the distance from the line to the pointer: the map canvas ranks lines by it");
  line.near = undefined;
  assert.equal(line._lineDistance({ x: 1, y: 1 }), 1e5, 'no point on the line: last among lines');
  line.near = NaN;
  assert.equal(line._lineDistance({ x: 1, y: 1 }), 1e5);
  const r2 = rig();
  r2.coarse = true;
  r2.start(); await settle();
  assert.ok(r2.layer.items.every((l) => !l.tip));
});
