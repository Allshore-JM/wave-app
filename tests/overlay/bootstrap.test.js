'use strict';
// The page's overlay bootstrap: the gated inline script of templates/index.html (flag on), run against a
// fake DOM with a virtual clock. Reload restore, Off or a pick before the restore, hidden tabs, corrupted
// storage, the load / deferred-table / idle waits and the back/forward cache. tests/test_overlay_flag.py
// checks that this extraction is exactly what Flask renders.  node --test tests/overlay/
// (Derived from the G8 reviewer A harness, scratchpad g8a/boot_sim.js.)
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const SUBS = { model_frames_base: 'https://frames.example/gfswave/0p25/v1', overlay_asset_version: '9.9.9', forecast_tz_name: 'Pacific/Honolulu', overlay_models: ['gfswave'], overlay_sst: false };
function gatedScript(subs) {
  subs = Object.assign({}, SUBS, subs || {});
  const tpl = fs.readFileSync(path.join(__dirname, '..', '..', 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');
  const a = tpl.indexOf('{%- if model_overlays %}'), b = tpl.indexOf('{%- endif %}', a);
  const block = tpl.slice(a, b), s = block.indexOf('<script>') + '<script>'.length, e = block.indexOf('</script>', s);
  let src = block.slice(s, e);
  for (const k of Object.keys(subs)) src = src.split('{{ ' + k + '|tojson }}').join(JSON.stringify(subs[k]));
  return src;
}
const SRC = gatedScript(), SRC_SST = gatedScript({ overlay_sst: true });
const KEY = 'allshore.overlay.v1';

function page(opt) {
  opt = opt || {};
  const log = [], store = new Map();
  if (opt.raw !== undefined) store.set(KEY, opt.raw); else if (opt.init) store.set(KEY, JSON.stringify(opt.init));
  const sessionStorage = opt.throwingStorage ? { getItem() { throw new Error('SecurityError'); }, setItem() { throw new Error('SecurityError'); } }
    : { getItem: (k) => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)) };
  const listeners = { win: {}, doc: {} };
  const on = (t) => (ev, fn, o) => { (listeners[t][ev] = listeners[t][ev] || []).push({ fn, once: !!(o && o.once) }); };
  function fire(t, ev, e) { const ls = (listeners[t][ev] || []).slice(); listeners[t][ev] = (listeners[t][ev] || []).filter((l) => !l.once); ls.forEach((l) => l.fn(e || {})); }
  let now = 0;
  const timers = [];
  const setTimeout = (fn, ms) => { timers.push({ at: now + (ms || 0), fn }); return timers.length; };
  function advance(ms) {
    const end = now + ms;
    for (;;) { timers.sort((x, y) => x.at - y.at); const t = timers[0]; if (!t || t.at > end) break; timers.shift(); now = t.at; t.fn(); }
    now = end;
  }
  const OPTS = opt.sst ? ['', 'hs', 'tp', 'wind', 'sst'] : ['', 'hs', 'tp', 'wind'];
  const sel = { _v: '', _ls: [], get value() { return this._v; }, set value(v) { v = String(v); this._v = OPTS.includes(v) ? v : ''; },
    addEventListener(ev, fn) { this._ls.push(fn); }, userPick(v) { this.value = v; this._ls.forEach((f) => f({})); } };
  const panel = { textContent: '' }, unit = { _ls: [], addEventListener(ev, fn) { this._ls.push(fn); } };
  let tableLoading = !!opt.deferredTable, scriptEl = null;
  const idleQ = [], byId = {};
  function element(tag) {
    const el = { tag, children: [], attrs: {}, _ls: {}, sheet: null, className: '', textContent: '',
      addEventListener(ev, fn) { (this._ls[ev] = this._ls[ev] || []).push(fn); }, remove() {},
      setAttribute(k, v) { this.attrs[k] = String(v); }, getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; },
      appendChild(c) { this.children.push(c); const reg = (x) => { if (x.id) byId[x.id] = x; (x.children || []).forEach(reg); }; reg(c); return c; },
      fire(ev) { (this._ls[ev] || []).forEach((f) => f({ target: this })); } };
    return el;
  }
  const gear = opt.settings ? element('div') : null;
  if (gear) gear.appendChild(Object.assign(element('label'), { className: 'grid-setting' }));   // the page's own gridlines row
  const document = {
    hidden: !!opt.hidden, readyState: opt.complete ? 'complete' : 'loading',
    getElementById: (id) => ({ ovField: sel, ovPanel: panel, unit, settingsPanel: gear })[id] || byId[id] || (id === 'forecastLoading' && tableLoading ? {} : null),
    addEventListener: on('doc'), removeEventListener() {}, querySelector: () => null,
    createElement: (tag) => element(tag),
    head: { appendChild: (el) => { if (el.tag === 'script') scriptEl = el; } }
  };
  const overlays = [];
  function makeAO() {
    return { create: (m, o) => {
      const ov = { field: null, mounts: [], opts: o,
        mount(f, r) { this.field = f; this.mounts.push([f, r]); log.push('mount ' + f + ' ' + r); },
        unmount() { this.field = null; log.push('unmount'); }, refresh() { log.push('refresh'); }, _persist() { log.push('persist'); },
        setIsobars(v) { log.push('isobars ' + v); }, setParticleOpacity(v) { log.push('particles ' + v); } };
      overlays.push(ov); log.push('create'); return ov;
    } };
  }
  const local = opt.local === undefined ? null : new Map(Object.entries(opt.local || {}));
  const localStorage = opt.throwingLocal ? { getItem() { throw new Error('SecurityError'); }, setItem() { throw new Error('SecurityError'); } }
    : local ? { getItem: (k) => (local.has(k) ? local.get(k) : null), setItem: (k, v) => local.set(k, String(v)) } : undefined;
  const window = { addEventListener: on('win'), requestIdleCallback: opt.noRIC ? undefined : (fn) => { idleQ.push(fn); },
    AllshoreOverlay: opt.assetsLoaded === false ? undefined : makeAO(), localStorage };
  const boxes = [];
  const L = { Control: { extend: (p) => function () { this.onAdd = p.onAdd; } }, DomUtil: { create: () => { const b = { innerHTML: '' }; boxes.push(b); return b; } },
    DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} }, TileLayer: opt.TileLayer };
  const map = { addControl: (c) => c.onAdd() };
  if (opt.layers) map.eachLayer = (fn) => opt.layers.forEach(fn);
  if (opt.zoom) { map.zoomControl = { tag: 'zoom' }; map.removed = []; map.removeControl = (c) => map.removed.push(c); }
  new Function('window', 'document', 'sessionStorage', 'L', 'map', 'getSelectedUnit', 'fmtTimeInTz', 'tzAbbr', 'setTimeout', opt.sst ? SRC_SST : SRC)(
    window, document, sessionStorage, L, map, () => 'US', () => '', () => '', setTimeout);
  return {
    log, store, sel, panel, overlays, listeners, map, boxes, gear, local, byId,
    read: () => { try { return JSON.parse(store.get(KEY) || 'null'); } catch (e) { return store.get(KEY); } },
    load() { document.readyState = 'complete'; fire('win', 'load'); },
    idle() { idleQ.splice(0).forEach((f) => f({})); },
    idleQueued: () => idleQ.length,
    advance,
    hide() { document.hidden = true; fire('doc', 'visibilitychange'); },
    show() { document.hidden = false; fire('doc', 'visibilitychange'); },
    tableArrives() { tableLoading = false; },
    pageshow(persisted) { fire('win', 'pageshow', { persisted }); },
    unitChange() { unit._ls.forEach((f) => f({})); },
    assetsArrive() { window.AllshoreOverlay = makeAO(); if (scriptEl && scriptEl.onload) scriptEl.onload(); }
  };
}
const settle = async () => { for (let i = 0; i < 6; i++) await new Promise((r) => setImmediate(r)); };
// load event, then the (absent) deferred table, then the idle callback
async function loadedAndIdle(p) { p.load(); p.advance(0); p.idle(); await settle(); }

test('the template block is the whole bootstrap and holds no template syntax after the four substitutions', () => {
  assert.ok(SRC.indexOf('Optional model overlays') >= 0 && SRC.indexOf("'allshore.overlay.v1'") >= 0);
  assert.ok(!/\{\{|\{%/.test(SRC), 'template syntax left in the script');
  assert.ok(SRC.indexOf('"https://frames.example/gfswave/0p25/v1"') >= 0 && SRC.indexOf('"9.9.9"') >= 0);
});

test('a reload restores the saved layer after load and idle, with "Loading" shown meanwhile', async () => {
  const p = page({ init: { field: 'hs', t: 1, at: 2, playing: true } });
  assert.equal(p.sel.value, 'hs'); assert.equal(p.panel.textContent, 'Loading…'); assert.deepEqual(p.log, []);
  p.idle(); await settle(); assert.deepEqual(p.log, [], 'nothing before the load event');
  await loadedAndIdle(p);
  assert.deepEqual(p.log, ['create', 'mount hs true']);
  // the unit select still reaches the module
  p.unitChange(); assert.equal(p.log[p.log.length - 1], 'refresh');
});

test('Off before the restore wins; another layer picked while the restore waits keeps the saved time (G8 R1 P3-2)', async () => {
  let p = page({ init: { field: 'hs', t: 1, at: 2, playing: true } });
  p.sel.userPick(''); assert.equal(p.panel.textContent, '');
  await loadedAndIdle(p);
  assert.deepEqual(p.log, []); assert.equal(p.read().field, ''); assert.equal(p.read().playing, false);
  p.sel.userPick('hs'); await settle();                                          // then a pick from Off: fresh
  assert.deepEqual(p.log, ['create', 'mount hs false']); assert.equal(p.read().t, undefined); assert.equal(p.read().at, undefined);
  p = page({ init: { field: 'hs', t: 1, at: 2, playing: true } });
  p.sel.userPick('tp'); await settle();                                          // a switch while "Wave height" waits to be restored
  assert.deepEqual(p.log, ['create', 'mount tp true']);
  const st = p.read(); assert.equal(st.field, 'tp'); assert.equal(st.t, 1); assert.equal(st.at, 2); assert.equal(st.playing, true);
  await loadedAndIdle(p);
  assert.deepEqual(p.log, ['create', 'mount tp true'], 'the queued restore stands down');
  p.sel.userPick('wind'); await settle();                                        // later switches: an ordinary switch (the module keeps the time)
  assert.deepEqual(p.overlays[0].mounts, [['tp', true], ['wind', false]]); assert.equal(p.read().t, 1);
  // Off after the load but before the idle callback: nothing mounts
  p = page({ init: { field: 'hs' } }); p.load(); p.advance(0); p.sel.userPick(''); p.idle(); await settle();
  assert.deepEqual(p.log, []); assert.equal(p.read().field, '');
});

test('switching layers while one is shown keeps the saved time; Off then a pick drops it (G8 B-P3-6)', async () => {
  const p = page({ init: { field: 'hs', t: 5, at: 6, playing: true } });
  await loadedAndIdle(p);
  p.sel.userPick('tp'); await settle();
  assert.equal(p.read().field, 'tp'); assert.equal(p.read().t, 5); assert.equal(p.read().at, 6);
  p.sel.userPick(''); await settle();
  assert.equal(p.read().field, ''); assert.equal(p.read().playing, false); assert.ok(p.log.indexOf('unmount') > 0);
  p.sel.userPick('hs'); await settle();
  assert.equal(p.read().field, 'hs'); assert.equal(p.read().t, undefined); assert.equal(p.read().at, undefined);
  assert.deepEqual(p.overlays[0].mounts, [['hs', true], ['tp', false], ['hs', false]]);
});

test('a hidden tab waits: one listener, the restore when shown, nothing more on later hide/show', async () => {
  const p = page({ init: { field: 'hs' }, hidden: true });
  await loadedAndIdle(p);
  assert.deepEqual(p.log, []); assert.equal((p.listeners.doc.visibilitychange || []).length, 1);
  p.show(); await settle(); assert.deepEqual(p.log, ['create', 'mount hs true']);
  p.hide(); p.show(); await settle(); assert.deepEqual(p.log, ['create', 'mount hs true']);
});

test('corrupted or foreign storage values give Off, and the next pick writes a proper object (G8 A-P3-3)', async () => {
  for (const raw of ['{"field":"__proto__"}', '{"field":"constructor"}', '{"field":["hs"]}', '5', 'null', 'garbage', '{"field":"HS"}', '"hs"', '[1,2]']) {
    const p = page({ raw });
    assert.equal(p.sel.value, '', raw); assert.equal(p.panel.textContent, '', raw);
    await loadedAndIdle(p);
    assert.deepEqual(p.log, [], raw);
    p.sel.userPick('tp'); await settle();
    const st = p.read();
    assert.ok(st && typeof st === 'object' && !Array.isArray(st) && st.field === 'tp', raw + ' -> ' + JSON.stringify(st));
  }
  const p = page({ throwingStorage: true });                                  // nothing readable: a first open -> Wave Height
  await loadedAndIdle(p); assert.deepEqual(p.log, ['create', 'mount hs true']);
  p.sel.userPick(''); p.sel.userPick('tp'); await settle();
  assert.deepEqual(p.log, ['create', 'mount hs true', 'unmount', 'mount tp false'], 'storage that throws never breaks the control');
});

test('assets still loading when the restore comes due after Off then the same layer: one mount, the user\'s', async () => {
  const p = page({ init: { field: 'hs' }, assetsLoaded: false });
  p.sel.userPick(''); p.sel.userPick('hs');
  await loadedAndIdle(p);
  p.assetsArrive(); p.advance(3000); await settle();
  assert.deepEqual(p.log, ['create', 'mount hs false']);
});

test('no requestIdleCallback: a 200 ms timer instead; no load event: the restore starts after 5 s anyway (G8 A-P3-8)', async () => {
  let p = page({ init: { field: 'tp' }, noRIC: true });
  p.load(); p.advance(199); await settle(); assert.deepEqual(p.log, []);
  p.advance(1); await settle(); assert.deepEqual(p.log, ['create', 'mount tp true']);
  p = page({ init: { field: 'hs' } });
  p.advance(4999); p.idle(); await settle(); assert.deepEqual(p.log, [], 'waiting for the load event');
  p.advance(1); p.idle(); await settle(); assert.deepEqual(p.log, ['create', 'mount hs true'], 'a stalled subresource does not hold it forever');
  p.load(); p.advance(10000); p.idle(); await settle(); assert.deepEqual(p.log, ['create', 'mount hs true'], 'started once');
});

test('a deferred forecast table comes first: the restore waits for it, 3 s at most (G8 B-P3-5)', async () => {
  let p = page({ init: { field: 'hs' }, deferredTable: true });
  p.load(); p.advance(1000); p.idle(); await settle();
  assert.deepEqual(p.log, []); assert.equal(p.idleQueued(), 0);
  p.tableArrives(); p.advance(100); p.idle(); await settle();
  assert.deepEqual(p.log, ['create', 'mount hs true']);
  p = page({ init: { field: 'hs' }, deferredTable: true });
  p.load(); p.advance(2900); p.idle(); await settle(); assert.deepEqual(p.log, []);
  p.advance(100); p.idle(); await settle(); assert.deepEqual(p.log, ['create', 'mount hs true'], 'a table that never arrives');
});

test('back/forward cache: a page shown again writes its own state back to the tab (G8 A-P3-4)', async () => {
  let p = page({ init: { field: 'hs', t: 5, at: 6 } });
  await loadedAndIdle(p);
  p.store.set(KEY, JSON.stringify({ field: '', playing: false, t: 5, at: 6 }));      // another page of the tab switched Off
  p.pageshow(false); assert.equal(p.read().field, '', 'an ordinary load does nothing');
  p.pageshow(true);
  assert.equal(p.read().field, 'hs'); assert.equal(p.log[p.log.length - 1], 'persist');
  p = page({ init: { field: '' } });                                                    // this page is Off; another turned hs on
  await loadedAndIdle(p);
  p.store.set(KEY, JSON.stringify({ field: 'hs', playing: true, t: 5, at: 6 }));
  p.pageshow(true);
  assert.equal(p.read().field, ''); assert.equal(p.read().playing, false); assert.deepEqual(p.log, []);
});

test('the page hands its satellite imagery layer to the module (the wind overlay swaps it); none found -> null', async () => {
  function TL(url) { this._url = url; }
  const img = new TL('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}');
  const other = new TL('https://tiles.example/{z}/{x}/{y}.png');
  const p = page({ complete: true, TileLayer: TL, layers: [{ _url: 'World_Imagery (not a tile layer)' }, other, img] });
  p.sel.userPick('wind'); await settle();
  assert.equal(p.overlays[0].opts.baseLayer, img);
  assert.equal(p.overlays[0].opts.version, '9.9.9', 'the module learns its version (the speed illustrations are served at it)');
  const q = page({ complete: true, TileLayer: TL, layers: [other] }); q.sel.userPick('wind'); await settle();
  assert.equal(q.overlays[0].opts.baseLayer, null);
  const r = page({ complete: true }); r.sel.userPick('hs'); await settle();                     // no eachLayer / TileLayer: harmless
  assert.equal(r.overlays[0].opts.baseLayer, null);
});

test('the page drops the +/- zoom buttons (the Home button and every other way to zoom stay); a map without them is left alone', () => {
  const p = page({ zoom: true });
  assert.deepEqual(p.map.removed, [{ tag: 'zoom' }]);
  const q = page();                                                            // no zoomControl on the map: nothing to do, no error
  assert.equal(q.map.removed, undefined);
});

test('G16-B P2-1: the page hands every landed forecast to the overlay: its zone updates in place and the panel refreshes', async () => {
  const p = page({ complete: true });
  p.sel.userPick('hs'); await settle();
  const ov = p.overlays[0];
  ov.opts = ov.opts || {}; ov.refresh = function () { p.log.push('refresh'); };
  p.listeners.doc['allshore:forecast'].forEach((l) => l.fn({ detail: { station: '51201', tz: 'UTC', model: 'GFS', view: 'Table' } }));
  assert.equal(ov.opts.tz, 'UTC'); assert.ok(p.log.includes('refresh'));
  p.listeners.doc['allshore:forecast'].forEach((l) => l.fn({ detail: { station: '51201', tz: '', model: 'GFS', view: 'Table' } }));
  assert.equal(ov.opts.tz, 'UTC', 'an empty zone leaves the last one');
});

test('G16 re-review N-P2-1: a forecast that lands BEFORE the overlay exists sets the zone the overlay is created with', async () => {
  const p = page({ complete: true });
  p.listeners.doc['allshore:forecast'].forEach((l) => l.fn({ detail: { station: '51201', tz: 'UTC', model: 'GFS', view: 'Table' } }));
  p.sel.userPick('hs'); await settle();
  assert.equal(p.overlays[0].opts.tz, 'UTC', 'created with the zone of the forecast on screen, not the server one');
});

test('plan section 26: a new tab first opens shows Wave Height at "now", not playing; Off chosen there is remembered for the tab', async () => {
  const p = page({});
  assert.equal(p.sel.value, 'hs'); assert.equal(p.panel.textContent, 'Loading…'); assert.deepEqual(p.log, []);
  await loadedAndIdle(p);
  assert.deepEqual(p.log, ['create', 'mount hs true'], 'through the ordinary restore: nothing saved -> the frame at now, no playback');
  assert.equal(p.overlays[0].opts.tz !== undefined, true);
  p.sel.userPick(''); await settle();
  assert.equal(p.read().field, ''); assert.equal(p.read().playing, false);
  const q = page({ raw: p.store.get(KEY) });
  assert.equal(q.sel.value, ''); await loadedAndIdle(q); assert.deepEqual(q.log, [], 'the next page of the tab stays Off');
});

test('section 43: the page hands the offered models to the module (MODEL_OVERLAY_MODELS, rendered as MODELS)', async () => {
  const p = page({ complete: true });
  p.sel.userPick('hs'); await settle();
  assert.deepEqual(p.overlays[0].opts.models, ['gfswave']);
  assert.ok(SRC.indexOf('var MODELS = ["gfswave"];') >= 0 && SRC.indexOf('models: MODELS') >= 0);
});

test('section 44: "Sea surface temperature" is offered only with SST_OVERLAY (SST); a saved sst without it starts Off; the module is told', async () => {
  assert.ok(SRC.indexOf('var SST = false;') >= 0 && SRC_SST.indexOf('var SST = true;') >= 0 && SRC.indexOf('sst: SST') >= 0);
  // the control's markup: the option only when offered
  const off = page({ complete: true }), on = page({ complete: true, sst: true });
  assert.ok(off.boxes.length && off.boxes[0].innerHTML.indexOf('value="sst"') < 0);
  assert.ok(on.boxes[0].innerHTML.indexOf('<option value="sst">Sea surface temperature</option></select>') >= 0);
  // a tab whose save says sst: restored with the setting on, Off without it
  const p1 = page({ init: { field: 'sst' }, sst: true });
  assert.equal(p1.sel.value, 'sst');
  await loadedAndIdle(p1);
  assert.deepEqual(p1.overlays[0].mounts, [['sst', true]]);
  assert.equal(p1.overlays[0].opts.sst, true);
  const p2 = page({ init: { field: 'sst' } });
  assert.equal(p2.sel.value, '');
  await loadedAndIdle(p2);
  assert.equal(p2.overlays.length, 0, 'nothing mounted');
  // a pick of sst and back: the module mounts each; the save follows the select
  const p3 = page({ sst: true, init: { field: '' } });
  p3.sel.userPick('sst'); await settle();
  assert.deepEqual(p3.overlays[0].mounts, [['sst', false]]); assert.equal(p3.read().field, 'sst');
  p3.sel.userPick('hs'); await settle();
  assert.deepEqual(p3.overlays[0].mounts[1], ['hs', false]); assert.equal(p3.read().field, 'hs');
  assert.equal(page({ sst: false, init: { field: '' } }).overlays.length, 0);
});

// ---- the settings gear's overlay rows (plan section 46) ----
const ISO_KEY = 'allshore.isobars.v1', PART_KEY = 'allshore.particles.v1';
async function created(p) { await loadedAndIdle(p); assert.equal(p.overlays.length, 1, 'the module was made'); return p.overlays[0]; }

test('section 46: two rows join the gear after its own rows (isobars on, particles at 100 %), and the module is made with them', async () => {
  const p = page({ settings: true, local: {}, init: { field: 'wind' } });
  const kids = p.gear.children;
  assert.equal(kids.length, 3, 'the gridlines row, then the two overlay rows');
  assert.equal(kids[0].className, 'grid-setting', 'appended after the page\'s rows');
  const iso = p.byId.ovIsobars, part = p.byId.ovParticles;
  assert.equal(iso.type, 'checkbox'); assert.equal(iso.checked, true);
  assert.equal(kids[1].children[1].textContent, 'Pressure isobars (wind)');
  assert.equal(part.type, 'range'); assert.deepEqual([part.min, part.max, part.step, part.value], ['0', '100', '5', '100']);
  assert.equal(part.getAttribute('aria-valuetext'), '100%'); assert.equal(kids[2].getAttribute('for'), 'ovParticles');
  assert.equal(kids[2].children[0].textContent, 'Particles');
  const ov = await created(p);
  assert.equal(ov.opts.isobars, true); assert.equal(ov.opts.particles, 1);
});

test('section 46: without the gear panel (or with the rows already there) nothing is added; the module still gets the saved values', async () => {
  const p = page({ local: { [ISO_KEY]: '0', [PART_KEY]: '35' }, init: { field: 'hs' } });
  assert.equal(p.gear, null); assert.equal(p.byId.ovIsobars, undefined);
  const ov = await created(p);
  assert.equal(ov.opts.isobars, false); assert.equal(ov.opts.particles, 0.35);
});

test('section 46: saved values: only "0" turns the isobars off; the particles take 0-100 in whole numbers, anything else is 100; no storage at all is the defaults', async () => {
  const cases = [
    [{}, true, 100], [{ [ISO_KEY]: '0' }, false, 100], [{ [ISO_KEY]: '1' }, true, 100], [{ [ISO_KEY]: 'false' }, true, 100],
    [{ [PART_KEY]: '0' }, true, 0], [{ [PART_KEY]: '40' }, true, 40], [{ [PART_KEY]: '100' }, true, 100],
    [{ [PART_KEY]: '150' }, true, 100], [{ [PART_KEY]: '-5' }, true, 100], [{ [PART_KEY]: '4.5' }, true, 100], [{ [PART_KEY]: 'abc' }, true, 100], [{ [PART_KEY]: '' }, true, 100]
  ];
  for (const [local, iso, part] of cases) {
    const p = page({ settings: true, local, init: { field: 'hs' } });
    assert.equal(p.byId.ovIsobars.checked, iso, JSON.stringify(local)); assert.equal(p.byId.ovParticles.value, String(part), JSON.stringify(local));
    const ov = await created(p);
    assert.equal(ov.opts.isobars, iso); assert.equal(ov.opts.particles, part / 100);
  }
  for (const opt of [{ settings: true }, { settings: true, throwingLocal: true }]) {         // no localStorage / one that throws
    const p = page(Object.assign({ init: { field: 'hs' } }, opt));
    assert.equal(p.byId.ovIsobars.checked, true); assert.equal(p.byId.ovParticles.value, '100');
    p.byId.ovIsobars.checked = false; p.byId.ovIsobars.fire('change');                     // a change that cannot be stored never throws
    p.byId.ovParticles.value = '30'; p.byId.ovParticles.fire('input');
    const ov = await created(p);
    assert.equal(ov.opts.isobars, false, 'the choice holds for the page'); assert.equal(ov.opts.particles, 0.3);
  }
});

test('section 46: a change is stored at once and reaches the module once it exists; the slider says "off" at 0', async () => {
  const p = page({ settings: true, local: {}, init: { field: '' } });           // Off: no module yet
  const iso = p.byId.ovIsobars, part = p.byId.ovParticles;
  iso.checked = false; iso.fire('change');
  assert.equal(p.local.get(ISO_KEY), '0');
  part.value = '0'; part.fire('input');
  assert.equal(p.local.get(PART_KEY), '0'); assert.equal(part.getAttribute('aria-valuetext'), 'off');
  assert.ok(!p.log.some((l) => /isobars|particles/.test(l)), 'nothing forwarded without the module');
  p.sel.userPick('wind'); await settle();
  const ov = p.overlays[0]; assert.equal(ov.opts.isobars, false); assert.equal(ov.opts.particles, 0, 'the module is made with the changed values');
  iso.checked = true; iso.fire('change');
  part.value = '55'; part.fire('input');
  assert.equal(p.local.get(ISO_KEY), '1'); assert.equal(p.local.get(PART_KEY), '55'); assert.equal(part.getAttribute('aria-valuetext'), '55%');
  assert.deepEqual(p.log.filter((l) => /isobars|particles/.test(l)), ['isobars true', 'particles 0.55']);
  part.value = '250'; part.fire('input'); assert.equal(p.local.get(PART_KEY), '100', 'clamped');
  assert.equal(p.log[p.log.length - 1], 'particles 1');
});
