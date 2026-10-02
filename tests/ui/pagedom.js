'use strict';
// A small DOM for running the page's own script blocks (templates/index.html) in Node: the favourites picker, the
// "My points" block and AllshoreForecast.init (written by reviewer A in G22, plan section 31). Enough of Element /
// select / optgroup / events / storage; no layout. A <select> behaves as a browser's drop-down does.

function parseSimple(sel) {                      // "tag.cls#id[attr="v"]" -> a test function
  const m = /^([a-zA-Z0-9]+)?((?:[.#][\w-]+)*)((?:\[[^\]]+\])*)$/.exec(sel.trim());
  if (!m) throw new Error('selector not supported: ' + sel);
  const tag = m[1] ? m[1].toUpperCase() : null;
  const cls = [], ids = [];
  (m[2].match(/[.#][\w-]+/g) || []).forEach((t) => (t[0] === '.' ? cls : ids).push(t.slice(1)));
  const attrs = (m[3].match(/\[[^\]]+\]/g) || []).map((a) => { const k = /^\[([\w-]+)(?:="([^"]*)")?\]$/.exec(a); if (!k) throw new Error('attr selector: ' + a); return [k[1], k[2]]; });
  return (el) => (!tag || el.tagName === tag) && cls.every((c) => el.classList.contains(c)) && ids.every((i) => el.id === i) &&
    attrs.every(([k, v]) => (v === undefined ? el.hasAttribute(k) : el.getAttribute(k) === v));
}
function matcher(sel) { const parts = sel.split(',').map(parseSimple); return (el) => parts.some((f) => f(el)); }

class ClassList {
  constructor(el) { this.el = el; }
  _set() { return new Set((this.el._class || '').split(/\s+/).filter(Boolean)); }
  _write(s) { this.el._class = Array.from(s).join(' '); }
  add(c) { const s = this._set(); s.add(c); this._write(s); }
  remove(c) { const s = this._set(); s.delete(c); this._write(s); }
  contains(c) { return this._set().has(c); }
  toggle(c, on) { const s = this._set(); if (on === undefined) on = !s.has(c); if (on) s.add(c); else s.delete(c); this._write(s); return on; }
}

class Node_ {
  constructor(doc, tag) {
    this.doc = doc; this.tagName = tag.toUpperCase(); this.children = []; this.parentNode = null; this.attrs = new Map(); this._class = '';
    this.classList = new ClassList(this); this.listeners = {}; this.hidden = false; this.disabled = false; this.type = ''; this._text = ''; this._html = null;
    this.style = {}; this.title = ''; this.log = []; this.rect = { left: 0, top: 0, width: 100, height: 20 };
    this.clientHeight = 400; this.clientWidth = 100; this.offsetWidth = 100; this.scrollWidth = 700; this.offsetParent = {};
    const self = this;
    this.dataset = new Proxy({}, { set(t, k, v) { self.attrs.set('data-' + String(k).replace(/[A-Z]/g, (c) => '-' + c.toLowerCase()), String(v)); return true; },
      get(t, k) { return self.attrs.get('data-' + String(k).replace(/[A-Z]/g, (c) => '-' + c.toLowerCase())); } });
  }
  get ownerDocument() { return this.doc; }
  get id() { return this.attrs.get('id') || ''; }
  set id(v) { this.attrs.set('id', String(v)); }
  get className() { return this._class; }
  set className(v) { this._class = String(v); }
  get isConnected() { for (let e = this; e; e = e.parentNode) if (e === this.doc.body) return true; return false; }
  get firstChild() { return this.children[0] || null; }
  get lastChild() { return this.children[this.children.length - 1] || null; }
  _changed() { for (let e = this; e; e = e.parentNode) if (e.tagName === 'SELECT') { e._fix(); break; } }
  appendChild(c) { if (c.parentNode) c.parentNode.removeChild(c); c.parentNode = this; this.children.push(c); this._html = null; this._changed(); return c; }
  insertBefore(c, ref) { if (c.parentNode) c.parentNode.removeChild(c); c.parentNode = this; const i = ref ? this.children.indexOf(ref) : -1; if (i < 0) this.children.push(c); else this.children.splice(i, 0, c); this._changed(); return c; }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) { this.children.splice(i, 1); c.parentNode = null; } this._changed(); return c; }
  contains(x) { for (let e = x; e; e = e.parentNode) if (e === this) return true; return false; }
  closest(sel) { const f = matcher(sel); for (let e = this; e; e = e.parentNode) if (e.attrs && f(e)) return e; return null; }
  _walk(fn) { this.children.forEach((c) => { fn(c); c._walk(fn); }); }
  querySelectorAll(sel) { const f = matcher(sel), out = []; this._walk((c) => { if (c.tagName !== '#TEXT' && f(c)) out.push(c); }); return out; }
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  setAttribute(k, v) { if (k === 'class') this._class = String(v); else this.attrs.set(k, String(v)); }
  getAttribute(k) { return k === 'class' ? this._class : (this.attrs.has(k) ? this.attrs.get(k) : null); }
  hasAttribute(k) { return this.attrs.has(k); }
  get textContent() { return this.children.length ? this.children.map((c) => c.textContent).join('') : (this._html !== null ? this._html.replace(/<[^>]+>/g, '') : this._text); }
  set textContent(v) { this.children.forEach((c) => { c.parentNode = null; }); this.children = []; this._text = String(v); this._html = null; this._changed(); }
  get innerHTML() { return this._html !== null ? this._html : this.textContent; }
  set innerHTML(v) { this.children.forEach((c) => { c.parentNode = null; }); this.children = []; this._html = String(v); this.doc.htmlWrites.push([this.id || this.tagName, String(v).slice(0, 200)]); }
  addEventListener(type, fn, opts) { (this.listeners[type] = this.listeners[type] || []).push(fn); if (opts && opts.signal) opts.signal.addEventListener('abort', () => this.removeEventListener(type, fn)); }
  removeEventListener(type, fn) { const l = this.listeners[type]; if (!l) return; const i = l.indexOf(fn); if (i >= 0) l.splice(i, 1); }
  // an event that bubbles to the document
  fire(type, init) {
    const ev = Object.assign({ type, target: this, defaultPrevented: false, preventDefault() { this.defaultPrevented = true; }, stopPropagation() { this._stop = true; } }, init || {});
    for (let e = this; e && !ev._stop; e = e.parentNode) { ev.currentTarget = e; (e.listeners[type] || []).slice().forEach((fn) => fn.call(e, ev)); }
    if (!ev._stop) (this.doc.listeners[type] || []).slice().forEach((fn) => fn.call(this.doc, ev));
    return ev;
  }
  click() { if (!this.disabled) this.fire('click'); }
  focus() { this.doc.activeElement = this; this.log.push('focus'); }
  blur() { if (this.doc.activeElement === this) this.doc.activeElement = this.doc.body; }
  getBoundingClientRect() { const r = this.rect; return { left: r.left, top: r.top, width: r.width, height: r.height, right: r.left + r.width, bottom: r.top + r.height }; }
  setPointerCapture() {}
  getContext() { return { canvas: this }; }
}
class Option_ extends Node_ {
  constructor(doc) { super(doc, 'option'); this._value = null; }
  get value() { return this._value !== null ? this._value : this.textContent; }
  set value(v) { this._value = String(v); }
}
class Select_ extends Node_ {
  constructor(doc) { super(doc, 'select'); this._sel = null; }
  get options() { const out = []; this._walk((c) => { if (c.tagName === 'OPTION') out.push(c); }); return out; }
  // As a browser's drop-down (size 1, not multiple): selectedness belongs to the OPTION. Assigning a value no option
  // has selects nothing (selectedIndex -1, value ''); whenever options are inserted or removed and none is selected,
  // the first option becomes selected (HTML's "selectedness setting algorithm").
  get selectedIndex() { return this._sel ? this.options.indexOf(this._sel) : -1; }
  set selectedIndex(i) { this._sel = this.options[i] || null; }
  get value() { return this._sel ? this._sel.value : ''; }
  set value(v) { this._sel = this.options.find((x) => x.value === String(v)) || null; }
  _fix() { const o = this.options; if (!this._sel || o.indexOf(this._sel) < 0) this._sel = o.length ? o[0] : null; }
}
class Document_ {
  constructor() { this.listeners = {}; this.htmlWrites = []; this.body = new Node_(this, 'body'); this.activeElement = this.body; this.hidden = false; this.title = 'Allshore Surf'; }
  createElement(tag) { return tag === 'select' ? new Select_(this) : tag === 'option' ? new Option_(this) : new Node_(this, tag); }
  createTextNode(t) { const n = new Node_(this, '#text'); n._text = String(t); return n; }
  getElementById(id) { let hit = null; this.body._walk((c) => { if (!hit && c.id === id) hit = c; }); return hit; }
  querySelector(sel) { return this.body.querySelector(sel); }
  querySelectorAll(sel) { return this.body.querySelectorAll(sel); }
  contains(x) { return this.body.contains(x); }
  addEventListener(type, fn) { (this.listeners[type] = this.listeners[type] || []).push(fn); }
  removeEventListener(type, fn) { const l = this.listeners[type]; if (!l) return; const i = l.indexOf(fn); if (i >= 0) l.splice(i, 1); }
  dispatchEvent(ev) { (this.listeners[ev.type] || []).slice().forEach((fn) => fn.call(this, ev)); return true; }
}
function storage(init, opts) {
  opts = opts || {};
  const m = new Map(Object.entries(init || {}));
  return { m, getItem(k) { if (opts.throwOnRead) throw new Error('SecurityError'); return m.has(k) ? m.get(k) : null; },
    setItem(k, v) { if (opts.throwOnWrite || (opts.quota && String(v).length > opts.quota)) { const e = new Error('QuotaExceededError'); e.name = 'QuotaExceededError'; throw e; } m.set(k, String(v)); },
    removeItem(k) { m.delete(k); } };
}
module.exports = { Document_, Node_, Select_, Option_, storage };
