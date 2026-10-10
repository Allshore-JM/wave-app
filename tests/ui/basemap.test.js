'use strict';
// static_ui/basemap.js (plan section 39, step 5b): Esri's tilemap decides which tiles exist; a missing tile is drawn from
// the closest coarser tile Esri has. Pure helpers, the availability cache, and the tile layer against a stand-in Leaflet.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const SRC = fs.readFileSync(path.join(__dirname, '..', '..', 'static_ui', 'basemap.js'), 'utf8');
function load(win) { new Function('window', SRC)(win); return win.AllshoreBasemap; }
const B = load({});
const I = B._internals;
const IMAGERY = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}';
const RELIEF = 'https://server.arcgisonline.com/ArcGIS/rest/services/Elevation/World_Hillshade/MapServer/tile/{z}/{y}/{x}';
const flush = async (n) => { for (let i = 0; i < (n || 20); i++) await new Promise((r) => setImmediate(r)); };

// a tilemap stand-in: `have(z, x, y)` says which tiles exist; it answers the window asked, as Esri does
function tilemapServer(have, opts) {
  const calls = [];
  const fetchJson = (url) => {
    calls.push(url);
    const m = /\/tilemap\/(\d+)\/(\d+)\/(\d+)\/(\d+)\/(\d+)\?f=json$/.exec(url);
    if (!m) return Promise.reject(new Error('bad url ' + url));
    const [z, top, left, h, w] = m.slice(1).map(Number);
    if (opts && opts.fail && opts.fail(z)) return Promise.reject(new Error('network'));
    const data = [];
    for (let y = top; y < top + h; y++) for (let x = left; x < left + w; x++) data.push(have(z, x, y) ? 1 : 0);
    return Promise.resolve({ adjusted: false, location: { left, top, width: w, height: h }, data, valid: true });
  };
  return { calls, fetchJson };
}

test('pure helpers: the tilemap address, windows, a bit in an answered (maybe smaller) window, a tile\'s part of a coarser tile, the LRU', () => {
  assert.equal(I.tilemapBase(IMAGERY), 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer');
  assert.equal(I.tilemapBase(RELIEF), 'https://server.arcgisonline.com/ArcGIS/rest/services/Elevation/World_Hillshade/MapServer');
  assert.equal(I.tilemapBase('https://tile.openstreetmap.org/{z}/{x}/{y}.png'), null);
  assert.equal(I.tilemapBase(IMAGERY + '?token=x'), null); assert.equal(I.tilemapBase('http://x/MapServer/tile/{z}/{y}/{x}'), null); assert.equal(I.tilemapBase(null), null);
  assert.equal(I.windowKey(14, 1030, 7135), '14/7104/1024');
  assert.equal(I.windowUrl('B', 14, 1030, 7135), 'B/tilemap/14/7104/1024/32/32?f=json');
  const win = { location: { left: 1020, top: 7120, width: 4, height: 2 }, data: [0, 1, 0, 0, 1, 0, 0, 1] };
  assert.equal(I.bitAt(win, 1021, 7120), 1); assert.equal(I.bitAt(win, 1020, 7120), 0); assert.equal(I.bitAt(win, 1023, 7121), 1);
  assert.equal(I.bitAt(win, 1024, 7120), null, 'outside a window the server cut down'); assert.equal(I.bitAt(win, 1021, 7122), null);
  assert.equal(I.bitAt({ location: win.location }, 1021, 7120), null); assert.equal(I.bitAt(null, 0, 0), null);
  assert.equal(I.bitAt({ location: { left: 0, top: 0, width: 2, height: 2 }, data: [1] }, 1, 1), null, 'a short data array');
  assert.equal(I.bitAt({ location: { left: 0, top: 0, width: 0, height: 2 }, data: [] }, 0, 0), null);
  assert.deepEqual(I.cropOf(18, 1000, 2000, 2), { z: 16, x: 250, y: 500, sx: 0, sy: 0, sw: 64 });
  assert.deepEqual(I.cropOf(18, 1003, 2001, 2), { z: 16, x: 250, y: 500, sx: 192, sy: 64, sw: 64 });
  assert.deepEqual(I.cropOf(15, 5, 7, 0, 512), { z: 15, x: 5, y: 7, sx: 0, sy: 0, sw: 512 });
  assert.deepEqual(I.cropOf(15, 5, 7, 3), { z: 12, x: 0, y: 0, sx: 160, sy: 224, sw: 32 });
  const c = I.lru(2); c.set('a', 1); c.set('b', 2); c.get('a'); c.set('c', 3);
  assert.equal(c.get('b'), undefined, 'the least recently used goes'); assert.equal(c.get('a'), 1); assert.equal(c.size(), 2);
});

test('availability: below zoom 13 no request; one request per 32 x 32 window; a missing tile finds the closest coarser tile Esri has', async () => {
  // Esri has everything to zoom 15 here, nothing from 16 (the open ocean, say)
  const srv = tilemapServer((z) => z <= 15);
  const av = I.createAvailability('B', srv.fetchJson);
  assert.equal(await av.has(12, 5, 5), true); assert.equal(srv.calls.length, 0);
  assert.equal(await av.resolve(15, 100, 200), 0); assert.deepEqual(srv.calls, ['B/tilemap/15/192/96/32/32?f=json']);
  assert.equal(await av.resolve(18, 800, 1600), 3, 'zoom 18 -> the zoom-15 tile');
  assert.deepEqual(srv.calls.slice(1), ['B/tilemap/18/1600/800/32/32?f=json', 'B/tilemap/17/800/384/32/32?f=json', 'B/tilemap/16/384/192/32/32?f=json', 'B/tilemap/15/192/96/32/32?f=json'].slice(0, 3),
    'one window per level; the zoom-15 window was asked already');
  const n = srv.calls.length;
  await Promise.all([av.resolve(18, 801, 1600), av.resolve(18, 831, 1631), av.resolve(17, 400, 800)]);
  assert.equal(srv.calls.length, n, 'tiles in windows already asked: no new request');
  await av.resolve(18, 832, 1600); assert.equal(srv.calls.length, n + 2, 'the next window east: its zoom-18 and zoom-17 windows (zoom 16 and 15 were asked)');
  // a partial window (a coast): tile by tile
  const coast = tilemapServer((z, x) => z <= 14 || x % 2 === 0);
  const av2 = I.createAvailability('B', coast.fetchJson);
  assert.equal(await av2.resolve(17, 64, 64), 0); assert.equal(await av2.resolve(17, 65, 64), 1, 'its parent (zoom 16, x 32) exists');
  assert.equal(await av2.resolve(17, 67, 64), 2, 'parent x 33 missing too: zoom 15, x 16');
});

test('availability: a tilemap that cannot be read or is not valid counts as "the tile exists"; nothing anywhere = the tile itself', async () => {
  const down = tilemapServer(() => false, { fail: () => true });
  const av = I.createAvailability('B', down.fetchJson);
  assert.equal(await av.has(16, 1, 1), true); assert.equal(await av.resolve(16, 1, 1), 0);
  const invalid = I.createAvailability('B', () => Promise.resolve({ valid: false, data: [0], location: { left: 0, top: 0, width: 1, height: 1 } }));
  assert.equal(await invalid.resolve(16, 0, 0), 0);
  const thrown = I.createAvailability('B', () => { throw new Error('sync'); });
  assert.equal(await thrown.resolve(16, 0, 0), 0, 'a fetch that throws at once is a failure too');
  const none = tilemapServer((z) => z < 13 ? true : false);
  const av3 = I.createAvailability('B', none.fetchJson);
  assert.equal(await av3.resolve(14, 3, 3), 2, 'zoom 12 is never asked: Esri has it');
  const nothing = I.createAvailability('B', tilemapServer(() => false).fetchJson);
  assert.equal(await nothing.resolve(13 + I.MAX_UP + 1, 0, 0), 0, 'past MAX_UP levels: the tile itself (the placeholder at worst)');
});

// ---- the layer against a stand-in Leaflet ----
function fakeL() {
  function TileLayer(url, options) { this.initialize(url, options); }
  TileLayer.prototype.initialize = function (url, options) { this._url = url; this.options = Object.assign({}, options); };
  TileLayer.prototype.createTile = function (coords) { return { base: true, coords }; };
  TileLayer.prototype._tileOnLoad = function (done, tile) { done(null, tile); };
  TileLayer.prototype._tileOnError = function (done, tile) { done(new Error('tile error'), tile); };
  TileLayer.prototype.getTileUrl = function (c) { return 'TILE/' + c.z + '/' + c.y + '/' + c.x; };
  TileLayer.prototype.getTileSize = function () { return { x: 256, y: 256 }; };
  TileLayer.extend = function (props) {
    const Parent = this;
    function C(url, options) { this.initialize(url, options); }
    C.prototype = Object.create(Parent.prototype); Object.assign(C.prototype, props); C.prototype.constructor = C;
    return C;
  };
  return { TileLayer, Util: {
    template: (str, data) => str.replace(/\{ *([\w_ -]+) *\}/g, (m, k) => { if (data[k] === undefined) throw new Error('No value provided for variable ' + m); return data[k]; }),
    extend: (a, ...b) => Object.assign(a, ...b), bind: (fn, obj, ...a) => (...b) => fn.apply(obj, a.concat(b)) },
    DomEvent: { on: (el, type, fn) => { el.listeners[type] = fn; } } };
}
function fakeDoc() {
  const drawn = [];
  return { drawn, createElement(tag) {
    if (tag === 'canvas') {
      return { width: 0, height: 0, getContext() { return { imageSmoothingEnabled: false, imageSmoothingQuality: 'low', drawImage(...a) { drawn.push(a); } }; },
        toDataURL(type, q) { return 'data:' + type + ';q=' + q + ';' + drawn.length; } };
    }
    return { tag, attrs: {}, listeners: {}, parentNode: { attached: true }, src: '', setAttribute(k, v) { this.attrs[k] = v; } };
  } };
}
function makeLayer(have, extra) {
  const L = fakeL(), doc = fakeDoc(), srv = tilemapServer(have), images = [];
  const loadImage = (url) => { images.push(url); return (extra && extra.loadImage) ? extra.loadImage(url) : Promise.resolve({ img: url }); };
  const Cls = I.layerClass(L, { fetchJson: srv.fetchJson, loadImage, document: doc });
  const layer = new Cls((extra && extra.url) || IMAGERY, { maxZoom: 18 });
  layer._map = {};
  return { layer, doc, srv, images, L };
}

test('the layer: below zoom 13 (and for a service without a tilemap) Leaflet\'s own tile; a tile Esri has loads as usual', async () => {
  const { layer, srv } = makeLayer(() => true);
  assert.deepEqual(layer.createTile({ z: 12, x: 1, y: 2 }, () => {}), { base: true, coords: { z: 12, x: 1, y: 2 } });
  assert.equal(srv.calls.length, 0);
  const done = [];
  const t = layer.createTile({ z: 15, x: 10, y: 20 }, (e, tile) => done.push([e, tile]));
  assert.equal(t.tag, 'img'); assert.equal(t.alt, ''); assert.equal(t.attrs.role, 'presentation'); assert.equal(t.src, '', 'nothing asked before the tilemap answers');
  await flush();
  assert.equal(t.src, 'TILE/15/20/10');
  t.listeners.load(); assert.deepEqual(done, [[null, t]]);
  const osm = makeLayer(() => false, { url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png' });
  assert.equal(osm.layer.createTile({ z: 16, x: 1, y: 1 }, () => {}).base, true);
});

test('the layer: a missing tile is drawn from the closest coarser tile Esri has (its part, scaled into 256 px); tiles of one window share its request and the coarser image', async () => {
  const { layer, doc, srv, images } = makeLayer((z) => z <= 14);
  const a = layer.createTile({ z: 16, x: 4001, y: 8002 }, () => {}), b = layer.createTile({ z: 16, x: 4002, y: 8003 }, () => {});
  await flush();
  assert.deepEqual(images, ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/14/2000/1000'], 'one coarser tile, loaded once');
  assert.match(a.src, /^data:image\/jpeg;q=0\.92;/); assert.match(b.src, /^data:image\/jpeg;/);
  const c1 = I.cropOf(16, 4001, 8002, 2), c2 = I.cropOf(16, 4002, 8003, 2);
  assert.deepEqual(doc.drawn[0].slice(1), [c1.sx, c1.sy, c1.sw, c1.sw, 0, 0, 256, 256]);
  assert.deepEqual(doc.drawn[1].slice(1), [c2.sx, c2.sy, c2.sw, c2.sw, 0, 0, 256, 256]);
  assert.equal(srv.calls.filter((u) => /\/tilemap\/16\//.test(u)).length, 1, 'one zoom-16 window for both');
});

test('the layer: a tile removed before the answers (a zoom, a pan, the layer off) is never given a source; a coarser tile that fails gives the tile itself', async () => {
  const { layer } = makeLayer((z) => z <= 14);
  const gone = layer.createTile({ z: 15, x: 3, y: 3 }, () => {});
  gone.parentNode = null;
  await flush();
  assert.equal(gone.src, '');
  const off = layer.createTile({ z: 15, x: 4, y: 4 }, () => {});
  layer._map = null; await flush(); assert.equal(off.src, '');
  const failing = makeLayer((z) => z <= 14, { loadImage: () => Promise.reject(new Error('404')) });
  const t = failing.layer.createTile({ z: 15, x: 5, y: 5 }, () => {});
  await flush();
  assert.equal(t.src, 'TILE/15/5/5', 'the tile itself (the placeholder at worst)');
  const t2 = failing.layer.createTile({ z: 15, x: 4, y: 5 }, () => {});         // the same coarser tile (14/2/2) as t
  await flush();
  assert.deepEqual(failing.images.slice(0, 2), [failing.images[0], failing.images[0]], 'a failed coarser image is asked again next time');
  assert.equal(failing.images.length, 2);
  assert.equal(t2.src, 'TILE/15/5/4');
  // removed WHILE its coarser image loads: no source when the image lands
  let release;
  const held = makeLayer((z) => z <= 14, { loadImage: () => new Promise((res) => { release = res; }) });
  const late = held.layer.createTile({ z: 15, x: 7, y: 7 }, () => {});
  await flush();
  assert.equal(held.images.length, 1, 'the coarser image is being loaded');
  late.parentNode = null;
  release({ img: 'x' }); await flush();
  assert.equal(late.src, '', 'a tile gone meanwhile stays without a source');
  assert.equal(held.doc.drawn.length, 0, 'and nothing is drawn for it');
});

test('tileLayer() builds the layer on the page\'s Leaflet; the relief\'s template works too', async () => {
  const L = fakeL(), win = { L, fetch: () => Promise.resolve({ ok: true, json: () => Promise.resolve(null) }), Image: function () {}, document: fakeDoc() };
  const AB = load(win);
  const imagery = AB.tileLayer(IMAGERY, { maxZoom: 18 }), relief = AB.tileLayer(RELIEF, { maxNativeZoom: 16 });
  assert.ok(imagery instanceof L.TileLayer && relief instanceof L.TileLayer);
  assert.equal(relief._urlFor(14, 3, 5), 'https://server.arcgisonline.com/ArcGIS/rest/services/Elevation/World_Hillshade/MapServer/tile/14/5/3');
  assert.ok(imagery._avail && relief._avail);
  relief._map = {};
  const t = relief.createTile({ z: 15, x: 1, y: 1 }, () => {});
  await flush();
  assert.equal(t.src, 'TILE/15/1/1', 'an unreadable tilemap (null): the tile itself');
});
