'use strict';
// A small fake DOM for tests/ui: enough of Element / Document / window for static_ui/forecast.js.
// Not a browser: layout numbers come from what a test sets (rect, clientHeight, offsetHeight).

function matches(el, sel) {
  return sel.split(',').some((part) => {
    const s = part.trim();
    if (!s) return false;
    if (s[0] === '[') return el.attrs.has(s.slice(1, -1));
    if (s[0] === '#') return el.id === s.slice(1);
    if (s[0] === '.') return el.classList.contains(s.slice(1));
    return el.tagName === s.toUpperCase();
  });
}

class ClassList {
  constructor() { this.set = new Set(); }
  add(c) { this.set.add(c); }
  remove(c) { this.set.delete(c); }
  contains(c) { return this.set.has(c); }
  toggle(c, on) { if (on === undefined) on = !this.set.has(c); if (on) this.set.add(c); else this.set.delete(c); return on; }
}

class Element {
  constructor(doc, tag, id) {
    this.doc = doc; this.tagName = tag.toUpperCase(); this.id = id || ''; this.children = []; this.parentNode = null;
    this.attrs = new Map(); this.classList = new ClassList(); this.listeners = {}; this.hidden = false; this.disabled = false;
    this.type = ''; this.value = ''; this._text = ''; this._html = ''; this.rect = { left: 0, top: 0, width: 0, height: 0 };
    this.clientHeight = 0; this.offsetHeight = 0; this.style = new Proxy({ setProperty(k, v) { this[k] = v; } }, {});
    this.isConnected = true; this.className = '';
    this.log = [];                                                             // what a test wants to see (focus, capture...)
  }
  get firstChild() { return this.children[0] || null; }
  appendChild(c) { if (c.parentNode) c.parentNode.removeChild(c); c.parentNode = this; this.children.push(c); this._text = ''; return c; }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) { this.children.splice(i, 1); c.parentNode = null; } return c; }
  contains(x) { for (let e = x; e; e = e.parentNode) if (e === this) return true; return false; }
  closest(sel) { for (let e = this; e; e = e.parentNode) if (e.attrs && matches(e, sel)) return e; return null; }
  querySelectorAll(sel) { const out = []; const walk = (e) => { e.children.forEach((c) => { if (matches(c, sel)) out.push(c); walk(c); }); }; walk(this); return out; }
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  setAttribute(k, v) { this.attrs.set(k, String(v)); if (k === 'id') this.id = String(v); }
  getAttribute(k) { return this.attrs.has(k) ? this.attrs.get(k) : null; }
  hasAttribute(k) { return this.attrs.has(k); }
  get textContent() { return this.children.length ? this.children.map((c) => c.textContent).join('') : this._text; }
  set textContent(v) { this.children.forEach((c) => { c.parentNode = null; }); this.children = []; this._text = String(v); this._html = ''; }
  get innerHTML() { return this._html || this.textContent; }
  set innerHTML(v) { this.children.forEach((c) => { c.parentNode = null; }); this.children = []; this._html = String(v); this._text = String(v).replace(/<[^>]+>/g, ''); }
  addEventListener(type, fn, opts) {
    const rec = { fn, opts: opts || {} };
    (this.listeners[type] = this.listeners[type] || []).push(rec);
    if (opts && opts.signal) opts.signal.addEventListener('abort', () => this.removeEventListener(type, fn));
  }
  removeEventListener(type, fn) { const l = this.listeners[type]; if (!l) return; const i = l.findIndex((r) => r.fn === fn); if (i >= 0) l.splice(i, 1); }
  listenerCount(type) { return (this.listeners[type] || []).length; }
  dispatch(type, ev) {                                                         // bubbles up the parents
    ev = Object.assign({ type, target: this, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, stopPropagation() { this._stop = true; } }, ev || {});
    for (let e = this; e && !ev._stop; e = e.parentNode) { ev.currentTarget = e; (e.listeners[type] || []).slice().forEach((r) => r.fn.call(e, ev)); }
    if (!ev._stop) (this.doc.listeners[type] || []).slice().forEach((r) => r.fn.call(this.doc, ev));
    return ev;
  }
  getBoundingClientRect() { return Object.assign({ right: this.rect.left + this.rect.width, bottom: this.rect.top + this.rect.height }, this.rect); }
  focus() { this.doc.activeElement = this; this.log.push('focus'); }
  setPointerCapture(id) { this.log.push('capture:' + id); }
  getContext() { return { canvas: this }; }
  get options() { return this.children.filter((c) => c.tagName === 'OPTION'); }
}
// a <select>: value takes only when an option has it
class Select extends Element {
  get value() { return this._value || ''; }
  set value(v) { this._value = this.options.some((o) => o.value === v) ? v : ''; }
}

class Document {
  constructor() { this.byId = new Map(); this.listeners = {}; this.activeElement = null; this.body = this.createElement('body'); this.hidden = false; }
  createElement(tag) { const el = tag === 'select' ? new Select(this, tag) : new Element(this, tag); return el; }
  createTextNode(t) { const el = new Element(this, '#text'); el._text = String(t); el.attrs = new Map(); return el; }
  getElementById(id) { return this.byId.get(id) || null; }
  register(el, id) { el.id = id; this.byId.set(id, el); return el; }
  addEventListener(type, fn) { (this.listeners[type] = this.listeners[type] || []).push({ fn }); }
  removeEventListener(type, fn) { const l = this.listeners[type]; if (!l) return; const i = l.findIndex((r) => r.fn === fn); if (i >= 0) l.splice(i, 1); }
  fire(type, ev) { ev = Object.assign({ type, target: this, stopPropagation() {}, preventDefault() {} }, ev || {}); (this.listeners[type] || []).slice().forEach((r) => r.fn.call(this, ev)); return ev; }
  dispatchEvent(ev) { (this.listeners[ev.type] || []).slice().forEach((r) => r.fn.call(this, ev)); return true; }
}

function memStorage(init) {
  const m = new Map(Object.entries(init || {}).map(([k, v]) => [k, typeof v === 'string' ? v : JSON.stringify(v)]));
  return { getItem: (k) => (m.has(k) ? m.get(k) : null), setItem: (k, v) => m.set(k, String(v)), map: m,
    read: (k) => { try { return JSON.parse(m.get(k)); } catch (e) { return undefined; } } };
}

function fakeWindow(opts) {
  opts = opts || {};
  const doc = new Document();
  const listeners = {};
  const win = {
    document: doc, innerWidth: opts.width || 1280, innerHeight: opts.height || 800, phone: !!opts.phone,
    matchMedia(q) { return { matches: /max-width: 500px/.test(q) && win.phone }; },
    addEventListener(t, fn) { (listeners[t] = listeners[t] || []).push(fn); },
    fire(t) { (listeners[t] || []).forEach((f) => f({})); },
    location: { search: opts.search || '' },
    history: { urls: [], replaceState(a, b, url) { this.urls.push(url); } },
    sessionStorage: memStorage(opts.session), localStorage: memStorage(opts.local),
    fetch: null, Chart: undefined
  };
  return win;
}

// The page's forecast-window markup as ids (the same contract as templates/index.html).
function buildPage(win) {
  const doc = win.document, el = (tag, id, parent, attrs) => {
    const e = doc.createElement(tag); if (id) doc.register(e, id); if (parent) parent.appendChild(e);
    Object.entries(attrs || {}).forEach(([k, v]) => e.setAttribute(k, v)); return e;
  };
  const topBar = el('header', 'topBar', doc.body); topBar.offsetHeight = 56;
  const sel = el('select', 'station', topBar);
  [['51201', '51201 — Waimea Bay, HI'], ['46001', '46001 — Gulf of Alaska']].forEach(([v, t]) => { const o = el('option', null, sel); o.value = v; o.textContent = t; });
  sel.value = '51201';
  const trigger = el('button', 'stationTrigger', topBar);
  const gear = el('button', 'settingsBtn', topBar); const panel = el('div', 'settingsPanel', topBar); panel.hidden = true;
  const tz = el('select', 'tz', panel); [['', '(Buoy Local)'], ['Pacific/Honolulu', 'Honolulu'], ['UTC', 'UTC']].forEach(([v, t]) => { const o = el('option', null, tz); o.value = v; o.textContent = t; });
  const unit = el('select', 'unit', panel); [['US', 'US'], ['Metric', 'Metric']].forEach(([v, t]) => { const o = el('option', null, unit); o.value = v; o.textContent = t; });
  const live = el('div', 'liveBuoyPanel', doc.body); live.style.display = 'none';
  const w = el('section', 'forecastWin', doc.body); w.rect = { left: 84, top: 300, width: 1180, height: 480 };
  const header = el('div', 'fwHeader', w); el('strong', 'fwTitle', header); el('span', 'fwCycle', header); el('span', 'fwBusy', header).hidden = true;
  el('button', 'fwMin', header); el('button', 'fwMax', header);
  const toolbar = el('div', 'fwToolbar', w);
  const viewBar = el('div', 'viewBar', toolbar); ['Table', 'Graph'].forEach((v) => el('button', null, viewBar, { 'data-view': v }));
  const modelBar = el('div', 'modelBar', toolbar); ['GFS', 'SWAN'].forEach((v) => el('button', null, modelBar, { 'data-model': v }));
  const rangeBar = el('div', 'rangeBar', toolbar); ['0', '7', '3'].forEach((v) => el('button', null, rangeBar, { 'data-days': v }));
  const body = el('div', 'fwBody', w); body.clientHeight = 400;
  el('div', 'fwError', body).hidden = true; el('div', 'forecastMeta', body);
  const table = el('div', 'forecastTable', body); el('div', 'forecastLoading', table);
  const graphs = el('div', 'graphs', body); graphs.hidden = true;
  ['heightChart', 'periodChart', 'directionChart'].forEach((id) => { const box = el('div', null, graphs); box.classList.add('chart-box'); el('canvas', id, box); });
  el('div', 'fwResize', w);
  return { topBar, sel, trigger, gear, panel, tz, unit, live, w, header, viewBar, modelBar, rangeBar, body, table, graphs };
}

// A Chart.js stand-in that records instances.
function fakeChart() {
  const made = [];
  function Chart(ctx, cfg) {
    this.canvas = ctx.canvas; this.config = cfg; this.data = cfg.data; this.options = cfg.options; this.destroyed = false; this.updates = 0; this.resizes = 0;
    this.scales = { x: { min: undefined, max: undefined, getPixelForValue: (i) => i * 10 } }; this.tooltip = { setActiveElements() {} };
    made.push(this);
  }
  Chart.prototype.destroy = function () { this.destroyed = true; };
  Chart.prototype.update = function () { this.updates++; };
  Chart.prototype.resize = function () { this.resizes++; };
  Chart.prototype.setActiveElements = function (a) { this.active = a; };
  Chart.prototype.getElementsAtEventForMode = function (evt) { return [{ index: evt.index || 0 }]; };
  Chart.made = made;
  return Chart;
}

module.exports = { fakeWindow, buildPage, fakeChart, memStorage, Element, Document };
