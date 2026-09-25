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

const SUBS = { model_frames_base: 'https://frames.example/gfswave/0p25/v1', overlay_asset_version: '9.9.9', forecast_tz_name: 'Pacific/Honolulu' };
function gatedScript() {
  const tpl = fs.readFileSync(path.join(__dirname, '..', '..', 'templates', 'index.html'), 'utf8').replace(/\r\n/g, '\n');
  const a = tpl.indexOf('{%- if model_overlays %}'), b = tpl.indexOf('{%- endif %}', a);
  const block = tpl.slice(a, b), s = block.indexOf('<script>') + '<script>'.length, e = block.indexOf('</script>', s);
  let src = block.slice(s, e);
  for (const k of Object.keys(SUBS)) src = src.split('{{ ' + k + '|tojson }}').join(JSON.stringify(SUBS[k]));
  return src;
}
const SRC = gatedScript();
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
  const OPTS = ['', 'hs', 'tp', 'wind'];
  const sel = { _v: '', _ls: [], get value() { return this._v; }, set value(v) { v = String(v); this._v = OPTS.includes(v) ? v : ''; },
    addEventListener(ev, fn) { this._ls.push(fn); }, userPick(v) { this.value = v; this._ls.forEach((f) => f({})); } };
  const panel = { textContent: '' }, unit = { _ls: [], addEventListener(ev, fn) { this._ls.push(fn); } };
  let tableLoading = !!opt.deferredTable, scriptEl = null;
  const idleQ = [];
  const document = {
    hidden: !!opt.hidden, readyState: opt.complete ? 'complete' : 'loading',
    getElementById: (id) => ({ ovField: sel, ovPanel: panel, unit })[id] || (id === 'forecastLoading' && tableLoading ? {} : null),
    addEventListener: on('doc'), removeEventListener() {}, querySelector: () => null,
    createElement: (tag) => ({ tag, addEventListener() {}, remove() {}, sheet: null }),
    head: { appendChild: (el) => { if (el.tag === 'script') scriptEl = el; } }
  };
  const overlays = [];
  function makeAO() {
    return { create: () => {
      const ov = { field: null, mounts: [],
        mount(f, r) { this.field = f; this.mounts.push([f, r]); log.push('mount ' + f + ' ' + r); },
        unmount() { this.field = null; log.push('unmount'); }, refresh() { log.push('refresh'); }, _persist() { log.push('persist'); } };
      overlays.push(ov); log.push('create'); return ov;
    } };
  }
  const window = { addEventListener: on('win'), requestIdleCallback: opt.noRIC ? undefined : (fn) => { idleQ.push(fn); },
    AllshoreOverlay: opt.assetsLoaded === false ? undefined : makeAO() };
  const L = { Control: { extend: (p) => function () { this.onAdd = p.onAdd; } }, DomUtil: { create: () => ({ innerHTML: '' }) },
    DomEvent: { disableClickPropagation() {}, disableScrollPropagation() {} } };
  const map = { addControl: (c) => c.onAdd() };
  new Function('window', 'document', 'sessionStorage', 'L', 'map', 'getSelectedUnit', 'fmtTimeInTz', 'tzAbbr', 'setTimeout', SRC)(
    window, document, sessionStorage, L, map, () => 'US', () => '', () => '', setTimeout);
  return {
    log, store, sel, panel, overlays, listeners,
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

test('the template block is the whole bootstrap and holds no template syntax after the three substitutions', () => {
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

test('Off or another layer picked before the restore wins; a pick from Off starts fresh (no saved time)', async () => {
  let p = page({ init: { field: 'hs', t: 1, at: 2, playing: true } });
  p.sel.userPick(''); assert.equal(p.panel.textContent, '');
  await loadedAndIdle(p);
  assert.deepEqual(p.log, []); assert.equal(p.read().field, ''); assert.equal(p.read().playing, false);
  p = page({ init: { field: 'hs', t: 1, at: 2, playing: true } });
  p.sel.userPick('tp'); await settle();
  assert.deepEqual(p.log, ['create', 'mount tp false']);
  const st = p.read(); assert.equal(st.field, 'tp'); assert.equal(st.playing, false); assert.equal(st.t, undefined); assert.equal(st.at, undefined);
  await loadedAndIdle(p);
  assert.deepEqual(p.log, ['create', 'mount tp false'], 'the queued restore stands down');
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
  const p = page({ throwingStorage: true });
  await loadedAndIdle(p); p.sel.userPick('hs'); await settle();
  assert.deepEqual(p.log, ['create', 'mount hs false'], 'storage that throws never breaks the control');
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
  p = page({});                                                                          // this page is Off; another turned hs on
  await loadedAndIdle(p);
  p.store.set(KEY, JSON.stringify({ field: 'hs', playing: true, t: 5, at: 6 }));
  p.pageshow(true);
  assert.equal(p.read().field, ''); assert.equal(p.read().playing, false); assert.deepEqual(p.log, []);
});
