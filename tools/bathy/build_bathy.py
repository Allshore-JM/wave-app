"""Build, check and publish the bathymetry tiles of one region (plan section 93b).

    python tools/bathy/build_bathy.py build  --region hawaii --out build/bathy [--version 1] [--box lon0,lat0,lon1,lat1]
    python tools/bathy/build_bathy.py build  --region gebco  --out build/bathy --gebco gebco_2026_geotiff.zip [--land f]
    python tools/bathy/build_bathy.py check  build/bathy/hawaii/v1
    python tools/bathy/build_bathy.py upload build/bathy/hawaii/v1 [--replace]     (R2_* environment, as coast-build)

A region (tools/bathy/bathy_sources.py) is built from its sources, best first, as data tiles (tools/bathy/bathy_tiles.py) from its
finest zoom (`zmax`, the finest source's) down to `zmin`:

1. The region is cut in BLOCKS: one tile of zoom zmax - 3 each, i.e. 8 x 8 tiles (2,048 px) at zmax. Only blocks under a
   source's footprint are read. Each block is read once (GDAL warp to EPSG:3857, the pixel grid of zoom zmax, 8 pixels of
   margin on every side; "average" resampling from a finer source, "bilinear" from a coarser one such as the 1/3
   arc-second sets), merged best first (the lowest source id wins), and kept on local disk as depth codes.
2. The water rule (tools/bathy/bathy_water.py), on those stored codes at zmax: a pixel is water when it is at or below
   the region's mean higher high water and 4-connected to water deeper than 20 m. Each block is labelled on its own and
   the labels are joined across block edges, so the result is that of labelling the whole region at full resolution.
3. Each block is cut into its tiles at zmax, zmax - 1, zmax - 2 and zmax - 3 (halving: a coarse pixel is water when 2 of
   its 4 are; its depth their mean). The margin gives every tile its one-pixel apron from the same read, so neighbouring
   tiles agree on their shared edge. A pixel with a value that is not water is LAND: no depth, the source plane's
   bathy_tiles.LAND_ID, so a client knows the survey's own land from "outside the survey" (G61 B-1). A tile is written at a
   zoom only where a water pixel's source earns it (`zmax` and `deep` per source: zoom_tables); land alone never makes a
   tile.
4. Coarser zooms down to zmin are built from the zmax - 3 tiles, their aprons from the neighbouring tiles of their zoom.

GEBCO (region "gebco", zooms 0-7) is read from the GEBCO GeoTIFF set (the zip as published; its eight quadrants are
required, then extracted one at a time): zoom 7 tiles straight from the grid, water = elevation below 0 AND outside GSHHG's
land (bathy_sources.LAND: the site's own coast-v1 overview, pinned by hash; lakes inside the land are land), so land and
lake beds below sea level (the Dead Sea, the Caspian) never read as sea depth. Then GEBCO's water is carried two cells
onto the land next to it (gebco_fill: each new cell the mean of its neighbours with a value, flagged "filled"), so it
reaches the shoreline the client clips it to (G61 B-3). Coarser zooms by halving. Every GEBCO pixel carries the "coarse"
flag.

The output directory holds t/<z>/<x>/<y>.png, idx/<x>_<y>.bin (per zoom-10 tile: one bit per tile at each zoom from 11 to
zmax), index.json (sources, presence of zooms <= 10, the zoom-10 table, counts, a digest of every data file and the
licence) and LICENSE.txt. `upload` publishes it under static/bathy/<region>/v<N>/ (files, LICENSE, then index.json) and then
records the region in static/bathy/regions.json. Without --replace it refuses a different build under the same version and
pointing regions.json back to an older version; `check` refuses an empty build.
"""
import argparse
import functools
import hashlib
import json
import math
import os
import shutil
import ssl
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bathy_sources as S   # noqa: E402
import bathy_tiles as T     # noqa: E402
import bathy_water as W     # noqa: E402

FORMAT = "bathy-v1"
BLOCK_LEVELS = 3                  # a block = one tile of zoom zmax - 3 = 8 x 8 tiles at zmax
MARGIN = 2 ** BLOCK_LEVELS        # 8 px at zmax -> the 1-px apron at zmax - 3
REGIONS_KEY = "static/bathy/regions.json"
PREFIX = "static/bathy"

GDAL_ENV = {
    "AWS_NO_SIGN_REQUEST": "YES", "AWS_REGION": "us-east-1", "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif,.TIF,.vrt,.zip", "GDAL_HTTP_MAX_RETRY": "5", "GDAL_HTTP_RETRY_DELAY": "3",
    "VSI_CACHE": "TRUE", "VSI_CACHE_SIZE": "200000000", "GDAL_CACHEMAX": "512",
}


def gdal_env():
    """GDAL's settings, as environment variables (also the pool's initializer). Certifi's trust list (plan section 57)."""
    for k, v in GDAL_ENV.items():
        os.environ.setdefault(k, v)
    try:
        import certifi
        os.environ.setdefault("CURL_CA_BUNDLE", certifi.where())
        os.environ.setdefault("SSL_CERT_FILE", certifi.where())
    except ImportError:
        pass


class _Inline:
    """An executor that runs the work in this process (workers=0: tests and debugging)."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def map(self, fn, items, chunksize=1):
        return map(fn, items)


def pool(workers):
    """A process pool of `workers` processes, or this process for workers=0."""
    if workers == 0:
        gdal_env()
        return _Inline()
    return ProcessPoolExecutor(workers, initializer=gdal_env)


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


# ---------------------------------------------------------------- the region's setup

def region_config(name, box=None, local=None):
    """The region's settings and its sources, as plain data for the worker processes."""
    reg = dict(S.REGIONS[name])
    srcs = []
    for sid in reg["sources"]:
        s = dict(S.SOURCES[sid], key=sid)
        if local and sid in local:
            s["local"] = local[sid]
        s["gdal"] = S.gdal_path(s)
        srcs.append(s)
    srcs.sort(key=lambda s: s["id"])          # best first = the lowest id first, whatever order the region lists them in
    ids = [s["id"] for s in srcs]
    if len(set(ids)) != len(ids) or any(not 1 <= i <= 31 or i == T.LAND_ID for i in ids):
        raise ValueError("%s: source ids %s must be distinct, 1-31 and not %d (the land mark)" % (name, ids, T.LAND_ID))
    reg.update(name=name, srcs=srcs, zmax=max(s["zmax"] for s in srcs))
    if not 0 <= reg["zmin"] <= reg["zmax"]:
        raise ValueError("%s: zmin %s outside 0..zmax %d" % (name, reg["zmin"], reg["zmax"]))
    if box:
        b = reg["box"]
        reg["box"] = (max(b[0], box[0]), max(b[1], box[1]), min(b[2], box[2]), min(b[3], box[3]))
    return reg


# A named client: the frame host's edge refuses Python's default "Python-urllib" user agent (HTTP 403).
USER_AGENT = "allshoresurf-bathy-build/1.0"


def _https_bytes(url, timeout=60):
    import certifi
    ctx = ssl.create_default_context(cafile=certifi.where())
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r:
        return r.read()


def _https_text(url, timeout=60):
    return _https_bytes(url, timeout).decode("utf-8")


def vrt_rects(text):
    """lon/lat rectangles (lon0, lat0, lon1, lat1) of every source a GDAL VRT places."""
    root = ET.fromstring(text)
    gt = [float(v) for v in root.find("GeoTransform").text.replace(",", " ").split()]
    out = []
    for r in root.iter("DstRect"):
        x, y = float(r.get("xOff")), float(r.get("yOff"))
        w, h = float(r.get("xSize")), float(r.get("ySize"))
        lon0, lon1 = gt[0] + x * gt[1], gt[0] + (x + w) * gt[1]
        lat_a, lat_b = gt[3] + y * gt[5], gt[3] + (y + h) * gt[5]
        out.append((min(lon0, lon1), min(lat_a, lat_b), max(lon0, lon1), max(lat_a, lat_b)))
    return out


def footprint(src):
    """The lon/lat rectangles where a source may have data."""
    if src.get("local"):
        import rasterio
        with rasterio.open(src["gdal"]) as d:
            b = d.bounds
        return [(b.left, b.bottom, b.right, b.top)]
    return vrt_rects(_https_text("https://%s.s3.amazonaws.com/%s" % (S.BUCKET, src["path"])))


def block_keys(reg):
    """The blocks (tiles of zoom zmax - 3) under any source footprint, inside the region's box."""
    bz = reg["zmax"] - BLOCK_LEVELS
    keys = set()
    box = reg["box"]
    for src in reg["srcs"]:
        for r in footprint(src):
            c = (max(r[0], box[0]), max(r[1], box[1]), min(r[2], box[2]), min(r[3], box[3]))
            if c[0] >= c[2] or c[1] >= c[3]:
                continue
            x0, y0, x1, y1 = T.tiles_in_box(c, bz)
            keys.update((x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1))
    return sorted(keys)


# ---------------------------------------------------------------- pass 1: read the blocks

def read_field(reg, key):
    """(elevation float32, source ids uint8) of block `key`, (2048 + 16) px square at zoom zmax; NaN = no data."""
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.transform import from_bounds
    from rasterio.vrt import WarpedVRT
    bz = reg["zmax"] - BLOCK_LEVELS
    px = T.TILE * 2 ** BLOCK_LEVELS
    size = px + 2 * MARGIN
    b = T.tile_bounds(bz, key[0], key[1], apron=MARGIN, px=px)
    elev = np.full((size, size), np.nan, np.float32)
    src_id = np.zeros((size, size), np.uint8)
    ground = (b[2] - b[0]) / size * math.cos(math.radians(T.xy_to_lonlat(0.0, (b[1] + b[3]) / 2)[1]))
    for s in reg["srcs"]:
        coarser = s["res_m"] > 1.25 * ground      # enlarging: bilinear, not "average" (which would copy 2 x 2 blocks)
        with rasterio.open(s["gdal"]) as d:
            kw = dict(crs="EPSG:3857", transform=from_bounds(*b, size, size), width=size, height=size,
                      resampling=Resampling.bilinear if coarser else Resampling.average, nodata=np.nan,
                      dtype="float32")
            nodata = d.nodata if d.nodata is not None else s.get("nodata")
            if nodata is None:
                raise ValueError("%s declares no nodata value: name it in bathy_sources (a fill value would read as a "
                                 "depth)" % s["key"])
            kw["src_nodata"] = nodata
            with WarpedVRT(d, **kw) as v:
                a = v.read(1)
        a[~(np.abs(a) < S.SANE_M)] = np.nan
        a += np.float32(s["offset"])
        take = np.isnan(elev) & np.isfinite(a)
        elev[take] = a[take]
        src_id[take] = s["id"]
    return elev, src_id


def _work_path(reg, key):
    return os.path.join(reg["work"], "b_%d_%d.npz" % key)


def pass1(args):
    """Read one block, keep it on disk as depth codes, return the summary of its low areas at the finest zoom
    (bathy_water.summarise), or None when its core holds no low pixel."""
    reg, key = args
    elev, src_id = read_field(reg, key)
    code = T.encode_depth(-elev)
    px = T.TILE * 2 ** BLOCK_LEVELS
    core = code[MARGIN:MARGIN + px, MARGIN:MARGIN + px]
    low = W.low_of(core, reg["mhhw"])
    if not low.any():
        return key, None
    np.savez_compressed(_work_path(reg, key), code=code, src=src_id)
    return key, W.summarise(low, W.deep_of(core))


def _sea_path(reg, key):
    return os.path.join(reg["work"], "s_%d_%d.npy" % key)


def pass_sea(args):
    """The block's sea mask at the finest zoom (its low areas labelled again, mapped through `keep`), kept on disk for
    its own tiles and its neighbours' aprons."""
    reg, key, keep = args
    px = T.TILE * 2 ** BLOCK_LEVELS
    with np.load(_work_path(reg, key)) as f:
        core = f["code"][MARGIN:MARGIN + px, MARGIN:MARGIN + px]
    np.save(_sea_path(reg, key), T.pack_bits(W.sea_of(W.low_of(core, reg["mhhw"]), keep)))
    return key


def _sea_get(reg):
    px = T.TILE * 2 ** BLOCK_LEVELS

    def get(key):
        p = _sea_path(reg, key)
        if not os.path.exists(p):
            return None
        return T.unpack_bits(np.load(p).tobytes(), (px, px))
    return get


# ---------------------------------------------------------------- pass 2: cut the blocks into tiles

def tile_path(out, z, x, y):
    return os.path.join(out, "t", str(z), str(x), "%d.png" % y)


def write_tile(out, z, x, y, depth258, src258):
    """A tile file: depth codes, and the source plane where there is water or the region's land (LAND_ID)."""
    p = tile_path(out, z, x, y)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    keep = np.isfinite(depth258) | (src258 == T.LAND_ID)
    with open(p, "wb") as f:
        f.write(T.pack_tile(T.encode_depth(depth258), np.where(keep, src258, 0)))


def zoom_tables(reg):
    """Per source id (0-31): the finest zoom its pixels earn, and its deep rule (pixels deeper than deep_m earn deep_z
    only; none = inf)."""
    zmax = np.zeros(32, np.int16)
    deep_m = np.full(32, np.inf, np.float32)
    deep_z = np.zeros(32, np.int16)
    for s in reg["srcs"]:
        zmax[s["id"]] = s["zmax"]
        if s.get("deep"):
            deep_m[s["id"]], deep_z[s["id"]] = s["deep"]
    return zmax, deep_m, deep_z


def earns(depth, src, tables, z):
    """bool: the water pixels whose source earns a tile at zoom z (land and no-data pixels never do)."""
    zmax, deep_m, deep_z = tables
    i = src & T.SRC_BITS
    wet = np.isfinite(depth)
    zm = np.where(wet & (np.where(wet, depth, 0) > deep_m[i]), deep_z[i], zmax[i])
    return wet & (zm >= z)


def save_core(work, z, x, y, depth256, src256):
    d = os.path.join(work, "c", str(z))
    os.makedirs(d, exist_ok=True)
    np.savez_compressed(os.path.join(d, "%d_%d.npz" % (x, y)), code=T.encode_depth(depth256), src=src256)


def pass2(args):
    """Water = low and joined to the sea (its own and its neighbours' finest-zoom sea masks for the margin); every other
    pixel with a value is land (LAND_ID on the source plane, no depth). Then the block's tiles at zmax .. zmax - 3, each
    written where a water pixel's source earns its zoom. Returns the tiles written."""
    reg, key = args
    z = reg["zmax"]
    tables = zoom_tables(reg)
    px = T.TILE * 2 ** BLOCK_LEVELS
    with np.load(_work_path(reg, key)) as f:
        code, src = f["code"], f["src"]
    sea = W.sea_around(key, _sea_get(reg), px, MARGIN)
    wet = W.low_of(code, reg["mhhw"]) & sea
    depth = np.where(wet, T.DECODE[code], np.nan).astype(np.float32)
    src = np.where(wet, src, np.where(code > 0, T.LAND_ID, 0)).astype(np.uint8)
    written = []
    m, k = MARGIN, 2 ** BLOCK_LEVELS
    while True:
        for j in range(k):
            for i in range(k):
                y0, x0 = m - 1 + T.TILE * j, m - 1 + T.TILE * i
                sub = depth[y0:y0 + T.SIDE, x0:x0 + T.SIDE]
                ssub = src[y0:y0 + T.SIDE, x0:x0 + T.SIDE]
                if z >= reg["zmin"] and earns(sub[1:-1, 1:-1], ssub[1:-1, 1:-1], tables, z).any():   # never below zmin
                    x, y = key[0] * k + i, key[1] * k + j
                    write_tile(reg["out"], z, x, y, sub, ssub)
                    written.append((z, x, y))
        if k == 1:
            core = depth[m:m + T.TILE, m:m + T.TILE]
            score = src[m:m + T.TILE, m:m + T.TILE]
            if np.isfinite(core).any() or (score == T.LAND_ID).any():    # land too: the coarser zooms keep it
                save_core(reg["work"], z, key[0], key[1], core, score)
            break
        depth, src = T.pool2(depth, src)
        m, k, z = m // 2, k // 2, z - 1
    os.remove(_work_path(reg, key))
    return written


# ---------------------------------------------------------------- coarser zooms, from saved tile cores

@functools.lru_cache(maxsize=1536)
def _core(work, z, x, y):
    p = os.path.join(work, "c", str(z), "%d_%d.npz" % (x, y))
    if not os.path.exists(p):
        return None
    with np.load(p) as f:
        return T.DECODE[f["code"]], f["src"].copy()


def core_keys(work, z):
    d = os.path.join(work, "c", str(z))
    if not os.path.isdir(d):
        return []
    return sorted(tuple(int(v) for v in n[:-4].split("_")) for n in os.listdir(d) if n.endswith(".npz"))


def _assemble(work, z, x, y, a, wrap=False):
    """(depth, src) of the saved core z/x/y with an `a`-pixel apron from its neighbouring cores (missing = no data)."""
    n = 2 ** z
    size = T.TILE + 2 * a
    depth = np.full((size, size), np.nan, np.float32)
    src = np.zeros((size, size), np.uint8)
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            nx, ny = x + dx, y + dy
            if wrap:
                nx %= n
            if not (0 <= nx < n and 0 <= ny < n):
                continue
            c = _core(work, z, nx, ny)
            if c is None:
                continue
            d, s = c
            ys = slice(a, a + T.TILE) if dy == 0 else (slice(0, a) if dy < 0 else slice(a + T.TILE, size))
            xs = slice(a, a + T.TILE) if dx == 0 else (slice(0, a) if dx < 0 else slice(a + T.TILE, size))
            sy = slice(0, T.TILE) if dy == 0 else (slice(T.TILE - a, T.TILE) if dy < 0 else slice(0, a))
            sx = slice(0, T.TILE) if dx == 0 else (slice(T.TILE - a, T.TILE) if dx < 0 else slice(0, a))
            depth[ys, xs] = d[sy, sx]
            src[ys, xs] = s[sy, sx]
    return depth, src


def encode_level(work, out, z, wrap=False, tables=None):
    """Write every saved core of zoom z that holds water (with `tables`: water whose source earns zoom z) as a tile, its
    apron from the neighbouring cores (missing = no data). A core of land only is never a tile; it lends its edge."""
    written = []
    for (x, y) in core_keys(work, z):
        depth, src = _assemble(work, z, x, y, 1, wrap)
        core = depth[1:-1, 1:-1]
        keep = earns(core, src[1:-1, 1:-1], tables, z) if tables is not None else np.isfinite(core)
        if not keep.any():
            continue
        write_tile(out, z, x, y, depth, src)
        written.append((z, x, y))
    return written


def coarse_levels(work, out, zfrom, zmin, wrap=False, tables=None):
    """Zooms zfrom - 1 .. zmin from the saved cores of zoom zfrom: halve 2 x 2 children, save, encode."""
    written = []
    for z in range(zfrom - 1, zmin - 1, -1):
        parents = sorted({(x >> 1, y >> 1) for (x, y) in core_keys(work, z + 1)})
        for (px, py) in parents:
            big = np.full((2 * T.TILE, 2 * T.TILE), np.nan, np.float32)
            bs = np.zeros((2 * T.TILE, 2 * T.TILE), np.uint8)
            for dy in (0, 1):
                for dx in (0, 1):
                    c = _core(work, z + 1, 2 * px + dx, 2 * py + dy)
                    if c is not None:
                        big[dy * T.TILE:(dy + 1) * T.TILE, dx * T.TILE:(dx + 1) * T.TILE] = c[0]
                        bs[dy * T.TILE:(dy + 1) * T.TILE, dx * T.TILE:(dx + 1) * T.TILE] = c[1]
            d, s = T.pool2(big, bs)
            if np.isfinite(d).any() or (s == T.LAND_ID).any():
                save_core(work, z, px, py, d, s)
        written += encode_level(work, out, z, wrap=wrap, tables=tables)
    return written


# ---------------------------------------------------------------- GEBCO

def gebco_files(path, work):
    """Yield local GeoTIFF paths of the GEBCO grid: a directory's .tif files, or a zip's members extracted one by one."""
    if os.path.isdir(path):
        for n in sorted(os.listdir(path)):
            if n.lower().endswith(".tif"):
                yield os.path.join(path, n), False
        return
    with zipfile.ZipFile(path) as z:
        for info in sorted(z.infolist(), key=lambda i: i.filename):
            if not info.filename.lower().endswith(".tif"):
                continue
            dst = os.path.join(work, "gebco_" + os.path.basename(info.filename))
            with z.open(info) as fi, open(dst, "wb") as fo:
                shutil.copyfileobj(fi, fo, 16 << 20)
            yield dst, True


def gebco_corners(path):
    """(west, south) of every GeoTIFF in the GEBCO set, read from the headers only (a zip's members through /vsizip/, so
    nothing is extracted). A list: a duplicated quadrant shows as two entries."""
    import rasterio
    if os.path.isdir(path):
        names = [os.path.join(path, n) for n in sorted(os.listdir(path)) if n.lower().endswith(".tif")]
    else:
        with zipfile.ZipFile(path) as z:
            base = "/vsizip/" + os.path.abspath(path).replace(os.sep, "/") + "/"
            names = [base + i.filename for i in z.infolist() if i.filename.lower().endswith(".tif")]
    corners = []
    for n in names:
        with rasterio.open(n) as d:
            corners.append((int(round(d.bounds.left)), int(round(d.bounds.bottom))))
    return corners


def land_file(path, work):
    """The GSHHG land overview (coast-v1, bathy_sources.LAND): a given local file, else downloaded; checked by hash."""
    if not path:
        path = os.path.join(work, "land-world-i.bin")
        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(_https_bytes(S.LAND["url"]))
        got = hashlib.sha256(open(path, "rb").read()).hexdigest()
        if got != S.LAND["sha256"]:
            raise ValueError("the land overview's hash %s is not the pinned %s" % (got[:16], S.LAND["sha256"][:16]))
    return path


@functools.lru_cache(maxsize=2)
def land_pieces(path):
    """(bbox float64 (n, 4) lon0, lat0, lon1, lat1; rings per piece as (lon, lat) float64 arrays) of a coast-v1 file."""
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "coast"))
    import build_coast as C
    d = C.decode_file(open(path, "rb").read())
    q = float(d["q"])
    boxes, rings = [], []
    for p in d["pieces"]:
        boxes.append([v / q for v in p["b"]])
        rings.append([(ix / q, iy / q) for ix, iy in p["rings"]])
    return np.array(boxes, np.float64).reshape(-1, 4), rings


def _merc(lon, lat):
    lat = np.clip(lat, -85.0511287798, 85.0511287798)
    return T.R * np.radians(lon), T.R * np.log(np.tan(np.pi / 4 + np.radians(lat) / 2))


def land_mask(path, z, x, y, px=T.TILE):
    """bool (px, px): True where a pixel of tile z/x/y lies on GSHHG land (a lake inside the land counts as land)."""
    from PIL import Image, ImageDraw
    boxes, rings = land_pieces(path)
    x0, y0, x1, y1 = T.tile_bounds(z, x, y)
    lon0, lat0 = T.xy_to_lonlat(x0, y0)
    lon1, lat1 = T.xy_to_lonlat(x1, y1)
    hit = np.flatnonzero((boxes[:, 0] <= lon1) & (boxes[:, 2] >= lon0) & (boxes[:, 1] <= lat1) & (boxes[:, 3] >= lat0))
    img = Image.new("L", (px, px), 0)
    if hit.size:
        draw = ImageDraw.Draw(img)
        res = (x1 - x0) / px
        for i in hit:
            for lon, lat in rings[i]:
                mx, my = _merc(lon, lat)
                pts = list(zip(((mx - x0) / res).tolist(), ((y1 - my) / res).tolist()))
                draw.polygon(pts, fill=1)
    return np.asarray(img, np.uint8) > 0


def gebco_tile(args):
    """One zoom-7 GEBCO core (256 px) from one GeoTIFF: water = elevation below 0 and not on GSHHG land."""
    path, work, land, x, y = args
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.transform import from_bounds
    from rasterio.vrt import WarpedVRT
    with rasterio.open(path) as d:
        kw = dict(crs="EPSG:3857", transform=from_bounds(*T.tile_bounds(7, x, y), T.TILE, T.TILE),
                  width=T.TILE, height=T.TILE, resampling=Resampling.average, nodata=np.nan, dtype="float32")
        if d.nodata is not None:
            kw["src_nodata"] = d.nodata
        with WarpedVRT(d, **kw) as v:
            a = v.read(1)
    a[~(np.abs(a) < S.SANE_M)] = np.nan
    wet = np.isfinite(a) & (a < 0)
    if wet.any():
        wet &= ~land_mask(land, 7, x, y)
    if not wet.any():
        return None
    depth = np.where(wet, -a, np.nan).astype(np.float32)
    src = np.where(wet, S.SOURCES["gebco"]["id"] | T.FLAG_COARSE, 0).astype(np.uint8)
    save_core(work, 7, x, y, depth, src)
    return (x, y)


def _grow(depth, steps, inside=None):
    """`steps` rings of cells added around the values of `depth` (NaN = none): each new cell the mean of its 8
    neighbours that have a value, one ring per step. `inside` (bool, the shape of `depth`) marks the cells that exist:
    a cell beyond the map's edge (past a pole) never takes a value, so it never passes one on."""
    d = depth.astype(np.float64)
    h, w = d.shape
    for _ in range(steps):
        p = np.pad(d, 1, constant_values=np.nan)
        nb = np.stack([p[1 + dy:1 + dy + h, 1 + dx:1 + dx + w] for dy in (-1, 0, 1) for dx in (-1, 0, 1) if dy or dx])
        ok = np.isfinite(nb)
        n = ok.sum(axis=0)
        tot = np.where(ok, nb, 0.0).sum(axis=0)
        new = np.isnan(d) & (n > 0)
        if inside is not None:
            new &= inside
        d[new] = tot[new] / n[new]
    return d.astype(np.float32)


GEBCO_FILL_STEPS = 2      # cells of GEBCO water carried onto the land next to it (owner decision 2026-11-12)


def gebco_fill(work, z=7, steps=GEBCO_FILL_STEPS, wrap=True):
    """Carry GEBCO's water `steps` cells onto the land next to it, flagged filled (and coarse), so the GEBCO layer
    reaches the shoreline the client clips it to: GEBCO's water stops short of GSHHG's coast by up to a cell or two (G61
    B-3). Every zoom-z core and every neighbour of one is redone from the ORIGINAL cores with a `steps`-pixel apron, so the
    result equals filling the whole world at once (tiles agree on their shared edges). Returns the cells filled."""
    n = 2 ** z
    cand = set()
    for (x, y) in core_keys(work, z):
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                nx, ny = x + dx, y + dy
                if wrap:
                    nx %= n
                if 0 <= nx < n and 0 <= ny < n:
                    cand.add((nx, ny))
    tmp = os.path.join(work, "fill_%d" % z)
    os.makedirs(tmp, exist_ok=True)
    flag = S.SOURCES["gebco"]["id"] | T.FLAG_COARSE | T.FLAG_FILLED
    c = slice(steps, steps + T.TILE)
    size = T.TILE + 2 * steps
    done, filled = [], 0
    for (x, y) in sorted(cand):
        depth, src = _assemble(work, z, x, y, steps, wrap)
        if not np.isfinite(depth).any():
            continue
        rows = y * T.TILE - steps + np.arange(size)          # world pixel rows / columns of the assembled block
        cols = x * T.TILE - steps + np.arange(size)
        inside = (rows >= 0) & (rows < n * T.TILE)
        inside = inside[:, None] & (np.ones(size, bool) if wrap else ((cols >= 0) & (cols < n * T.TILE)))[None, :]
        grown = _grow(depth, steps, inside)[c, c]
        new = np.isnan(depth[c, c]) & np.isfinite(grown)
        if not new.any():
            continue                                     # nothing to add here: the original core stays as it is
        s = np.where(new, flag, src[c, c]).astype(np.uint8)
        np.savez_compressed(os.path.join(tmp, "%d_%d.npz" % (x, y)), code=T.encode_depth(grown), src=s)
        done.append((x, y))
        filled += int(new.sum())
    dst = os.path.join(work, "c", str(z))
    os.makedirs(dst, exist_ok=True)
    for (x, y) in done:                                  # only now: every candidate was computed from the originals
        os.replace(os.path.join(tmp, "%d_%d.npz" % (x, y)), os.path.join(dst, "%d_%d.npz" % (x, y)))
    shutil.rmtree(tmp, ignore_errors=True)
    _core.cache_clear()
    return filled


def build_gebco(reg, gebco, workers, land=None):
    import rasterio
    z = 7
    written = []
    land = land_file(land, reg["work"])
    corners = gebco_corners(gebco)
    if sorted(corners) != sorted(S.GEBCO_QUADRANTS):
        raise ValueError("the GEBCO set holds the quadrants %s, not the eight expected" % sorted(corners))
    for path, extracted in gebco_files(gebco, reg["work"]):
        with rasterio.open(path) as d:
            b = d.bounds
        x0, y0, x1, y1 = T.tiles_in_box((b.left, max(b.bottom, -85.05), b.right, min(b.top, 85.05)), z)
        jobs = []
        for x in range(x0, x1 + 1):
            for y in range(y0, y1 + 1):
                c0 = T.xy_to_lonlat(*T.tile_bounds(z, x, y)[:2])
                c1 = T.xy_to_lonlat(*T.tile_bounds(z, x, y)[2:])
                cl, ct = (c0[0] + c1[0]) / 2, (c0[1] + c1[1]) / 2
                if b.left <= cl < b.right and b.bottom <= ct < b.top:     # the tile's centre decides its file
                    jobs.append((path, reg["work"], land, x, y))
        with pool(workers) as ex:
            done = [r for r in ex.map(gebco_tile, jobs, chunksize=8) if r]
        log("gebco %s: %d of %d tiles with water" % (os.path.basename(path), len(done), len(jobs)))
        if extracted:
            os.remove(path)
    filled = gebco_fill(reg["work"], z)
    log("gebco: %d cells carried onto the land next to the sea (flagged filled)" % filled)
    written += encode_level(reg["work"], reg["out"], z, wrap=True)
    written += coarse_levels(reg["work"], reg["out"], z, reg["zmin"], wrap=True)
    return written


# ---------------------------------------------------------------- build

def build(name, out_root, version=1, box=None, workers=None, gebco=None, local=None, keep_work=False, land=None):
    gdal_env()
    t0 = time.time()
    reg = region_config(name, box=box, local=local)
    out = os.path.join(out_root, name, "v%d" % version)
    if os.path.exists(out):
        shutil.rmtree(out)
    os.makedirs(out)
    work = os.path.join(out_root, "_work_" + name)
    if os.path.exists(work):
        shutil.rmtree(work)
    os.makedirs(work)
    reg.update(out=out, work=work)
    workers = (os.cpu_count() or 2) if workers is None else workers
    _core.cache_clear()
    if name == "gebco":
        if not gebco:
            raise SystemExit("--gebco <zip or directory> is needed for the gebco region")
        written = build_gebco(reg, gebco, workers, land=land)
    else:
        keys = block_keys(reg)
        log("%s: %d blocks of zoom %d to read (zmax %d)" % (name, len(keys), reg["zmax"] - BLOCK_LEVELS, reg["zmax"]))
        blocks = {}
        with pool(workers) as ex:
            for key, summary in ex.map(pass1, [(reg, k) for k in keys], chunksize=1):
                if summary is not None:
                    blocks[key] = summary
        log("pass 1: %d blocks hold low ground (%.0f s)" % (len(blocks), time.time() - t0))
        keep = W.sea_masks(blocks)
        with pool(workers) as ex:
            list(ex.map(pass_sea, [(reg, k, keep[k]) for k in sorted(blocks)], chunksize=1))
        written = []
        with pool(workers) as ex:
            for w in ex.map(pass2, [(reg, k) for k in sorted(blocks)], chunksize=1):
                written += w
        log("pass 2: %d tiles at zooms %d-%d (%.0f s)" % (len(written), reg["zmax"] - BLOCK_LEVELS, reg["zmax"],
                                                         time.time() - t0))
        written += coarse_levels(work, out, reg["zmax"] - BLOCK_LEVELS, reg["zmin"], tables=zoom_tables(reg))
    _core.cache_clear()
    index = write_index(reg, out, version)
    if not keep_work:
        shutil.rmtree(work, ignore_errors=True)
    log("%s v%d: %s tiles, %.1f MB, digest %s (%.0f s)" % (name, version, index["count"], index["bytes"] / 1e6,
                                                         index["digest"][:16], time.time() - t0))
    return index


# ---------------------------------------------------------------- index

def scan_tiles(out):
    """{z: set of (x, y)} of the tile files under out/t."""
    found = {}
    root = os.path.join(out, "t")
    if not os.path.isdir(root):
        return found
    for zs in os.listdir(root):
        for xs in os.listdir(os.path.join(root, zs)):
            for f in os.listdir(os.path.join(root, zs, xs)):
                if f.endswith(".png"):
                    found.setdefault(int(zs), set()).add((int(xs), int(f[:-4])))
    return found


def presence(found, zmin, zmax):
    """(levels, t10): bit-packed presence of zooms <= 10 over their tile box, and {"x_y": [zooms 11.. present?]} bitsets
    of the zoom-10 cells (one bit per tile, row-major, zoom 11 first). Returns (levels, idx) where idx maps (x10, y10) to
    the bytes of its idx file."""
    levels = {}
    for z in range(zmin, min(10, zmax) + 1):
        tiles = found.get(z, set())
        if not tiles:
            continue
        xs, ys = [t[0] for t in tiles], [t[1] for t in tiles]
        x0, y0 = min(xs), min(ys)
        w, h = max(xs) - x0 + 1, max(ys) - y0 + 1
        m = np.zeros((h, w), bool)
        for (x, y) in tiles:
            m[y - y0, x - x0] = True
        levels[str(z)] = {"x0": x0, "y0": y0, "w": w, "h": h, "bits": T.b64(T.pack_bits(m))}
    idx = {}
    if zmax > 10:
        cells = {}
        for z in range(11, zmax + 1):
            for (x, y) in found.get(z, ()):
                cells.setdefault((x >> (z - 10), y >> (z - 10)), {}).setdefault(z, []).append((x, y))
        for (cx, cy), by_z in cells.items():
            parts = []
            for z in range(11, zmax + 1):
                k = 2 ** (z - 10)
                m = np.zeros((k, k), bool)
                for (x, y) in by_z.get(z, ()):
                    m[y - cy * k, x - cx * k] = True
                parts.append(T.pack_bits(m))
            idx[(cx, cy)] = b"".join(parts)
    return levels, idx


def file_digest(out):
    """sha256 over every data file and LICENSE.txt (relative path + its sha256), sorted. Returns (hex, count, bytes):
    count and bytes are the data files' (t/ and idx/)."""
    h = hashlib.sha256()
    count = size = 0
    for r in data_files(out) + ["LICENSE.txt"]:
        p = os.path.join(out, r)
        if not os.path.exists(p):
            continue
        data = open(p, "rb").read()
        h.update(r.encode() + b"\0" + hashlib.sha256(data).digest())
        if r != "LICENSE.txt":
            count += 1
            size += len(data)
    return h.hexdigest(), count, size


def write_index(reg, out, version):
    found = scan_tiles(out)
    levels, idx = presence(found, reg["zmin"], reg["zmax"])
    if idx:
        os.makedirs(os.path.join(out, "idx"), exist_ok=True)
        for (x, y), data in idx.items():
            with open(os.path.join(out, "idx", "%d_%d.bin" % (x, y)), "wb") as f:
                f.write(data)
    with open(os.path.join(out, "LICENSE.txt"), "w", encoding="utf-8") as f:
        f.write(licence_text(reg))
    digest, count, size = file_digest(out)
    sources = {str(s["id"]): {k: s[k] for k in ("name", "year", "res_m", "datum", "licence", "credit")}
               for s in reg["srcs"]}
    index = {
        "format": FORMAT, "region": reg["name"], "version": version, "land_id": T.LAND_ID,
        "built_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "box": [round(v, 6) for v in reg["box"]], "zmin": reg["zmin"], "zmax": reg["zmax"], "mhhw": reg["mhhw"],
        "sources": sources, "levels": levels,
        "t10": sorted("%d_%d" % k for k in idx), "idx": {"dir": "idx", "zooms": [11, reg["zmax"]]} if idx else None,
        "tiles": {str(z): len(v) for z, v in sorted(found.items())},
        "count": count, "bytes": size, "digest": digest,
    }
    with open(os.path.join(out, "index.json"), "w") as f:
        json.dump(index, f, indent=1, sort_keys=True)
    return index


def licence_text(reg):
    lines = ["Bathymetry tiles of allshoresurf.com, region %s." % reg["name"], "",
             "Depths are approximate and not for navigation. Built from the sources below by Allshore Surf:",
             "resampled to the web map's tile grid, land and water not joined to the sea removed, values",
             "quantised (1 cm to 400 m, 0.1 m to 2,500 m, 2.5 m deeper). Depth below the source's datum.", ""]
    for s in reg["srcs"]:
        lines += ["- %s (%s), %s; vertical datum %s. %s. Licence: %s." % (
            s["name"], s["year"], "%d m grid" % s["res_m"], s["datum"], s["credit"], s["licence"])]
    if reg["name"] == "gebco":
        lines += ["- Land mask: %s. %s. Licence: %s. Used only to tell land from sea; not redistributed." % (
            S.LAND["name"], S.LAND["credit"], S.LAND["licence"]),
                  "- Where GEBCO's water stops short of the shoreline, its depth is carried up to %d cells onto the land "
                  "next to it (marked estimated)." % GEBCO_FILL_STEPS]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- check

def check(out, log_fn=print):
    """Validate a built directory. Returns the number of problems (0 = good)."""
    problems = []
    try:
        index = json.load(open(os.path.join(out, "index.json")))
    except (OSError, ValueError) as exc:
        log_fn("no readable index.json: %s" % exc)
        return 1
    if index.get("format") != FORMAT:
        problems.append("format %r" % index.get("format"))
    found = scan_tiles(out)
    ids = {int(k) for k in index.get("sources", {})}
    for z, tiles in sorted(found.items()):
        if not (index["zmin"] <= z <= index["zmax"]):
            problems.append("zoom %d outside %d-%d" % (z, index["zmin"], index["zmax"]))
        for (x, y) in tiles:
            if not (0 <= x < 2 ** z and 0 <= y < 2 ** z):
                problems.append("%d/%d/%d: outside the zoom's tile range" % (z, x, y))
            p = tile_path(out, z, x, y)
            try:
                code, sf = T.unpack_tile(open(p, "rb").read())
            except (OSError, ValueError) as exc:
                problems.append("%s: %s" % (p, exc))
                continue
            if (code > T.C3).any():
                problems.append("%d/%d/%d: unused codes" % (z, x, y))
            land = (code == 0) & (sf == T.LAND_ID)
            if (((code == 0) & (sf != 0) & ~land) | ((code > 0) & ((sf == 0) | ((sf & T.SRC_BITS) == T.LAND_ID)))).any():
                problems.append("%d/%d/%d: source plane disagrees with the depth plane" % (z, x, y))
            bad = set(np.unique(sf[code > 0] & T.SRC_BITS).tolist()) - ids
            if bad:
                problems.append("%d/%d/%d: unknown source ids %s" % (z, x, y, sorted(bad)))
            if not (code[1:-1, 1:-1] > 0).any():
                problems.append("%d/%d/%d: no water inside the tile" % (z, x, y))
    if {str(z): len(v) for z, v in sorted(found.items())} != index.get("tiles"):
        problems.append("tile counts differ from the index")
    levels, idx = presence(found, index["zmin"], index["zmax"])
    if levels != index.get("levels"):
        problems.append("zoom <= 10 presence differs from the files")
    if sorted("%d_%d" % k for k in idx) != index.get("t10"):
        problems.append("the zoom-10 table differs from the files")
    for (x, y), data in idx.items():
        p = os.path.join(out, "idx", "%d_%d.bin" % (x, y))
        if not os.path.exists(p) or open(p, "rb").read() != data:
            problems.append("idx %d_%d.bin missing or different" % (x, y))
    digest, count, size = file_digest(out)
    if (digest, count, size) != (index.get("digest"), index.get("count"), index.get("bytes")):
        problems.append("digest / count / bytes differ from the index")
    if not os.path.exists(os.path.join(out, "LICENSE.txt")):
        problems.append("no LICENSE.txt")
    if not found:
        problems.append("no tiles: an empty build is never published")
    for p in problems[:50]:
        log_fn("PROBLEM " + p)
    log_fn("check %s v%s: %d tiles %s, %.1f MB, %d problems" % (
        index.get("region"), index.get("version"), count, index.get("tiles"), size / 1e6, len(problems)))
    return len(problems)


# ---------------------------------------------------------------- upload

def r2():
    sys.path.insert(0, os.path.join(os.path.dirname(HERE), "model_frames"))
    import publish as P
    client = P.r2_client(os.environ["R2_ACCOUNT_ID"], os.environ["R2_ACCESS_KEY_ID"], os.environ["R2_SECRET_ACCESS_KEY"])
    return P.Store(client, os.environ["R2_BUCKET"])


IMMUTABLE = "public, max-age=31536000, immutable"
POINTER = "public, max-age=300"


def data_files(out):
    rel = []
    for sub in ("t", "idx"):
        for dp, _, fs in os.walk(os.path.join(out, sub)):
            rel += [os.path.relpath(os.path.join(dp, f), out).replace(os.sep, "/") for f in fs]
    return sorted(rel)


def upload(out, store=None, replace=False, workers=16, log_fn=print):
    """Publish a built directory: data files, LICENSE.txt, index.json last; then the region's line in regions.json.

    Refused without replace=True: a DIFFERENT build under the same version, and pointing regions.json back to an OLDER
    version than the one it names (a rollback is deliberate). A replace first deletes the old build's index.json (no
    client reads the half-written mix as a whole build), then writes the new files, deletes the old build's files the
    new one does not have, and writes the new index.json last. Returns True on success."""
    index = json.load(open(os.path.join(out, "index.json")))
    store = store or r2()
    name, version = index["region"], index["version"]
    prefix = "%s/%s/v%d" % (PREFIX, name, version)
    regions = store.get_json(REGIONS_KEY) or {"format": "bathy-regions-v1", "regions": {}}
    live = (regions.get("regions") or {}).get(name) or {}
    if isinstance(live.get("v"), int) and live["v"] > version and not replace:
        log_fn("refusing: regions.json names %s v%d; pointing it back to v%d needs --replace" % (name, live["v"], version))
        return False
    have = store.get_json(prefix + "/index.json")
    if have is not None and have.get("digest") != index["digest"]:
        if not replace:
            log_fn("refusing: %s/ already holds a different build (%s, this %s); use a new --version, or --replace"
                   % (prefix, str(have.get("digest"))[:16], index["digest"][:16]))
            return False
        log_fn("REPLACING a different build under %s/: its index.json goes first" % prefix)
        store.delete_keys([prefix + "/index.json"], prefix)
    same = have is not None and have.get("digest") == index["digest"]
    files = [] if same else data_files(out)

    def put(rel):
        ct = "image/png" if rel.endswith(".png") else "application/octet-stream"
        store.put(prefix + "/" + rel, open(os.path.join(out, rel), "rb").read(), ct, IMMUTABLE)

    if not same:
        done = 0
        with ThreadPoolExecutor(workers) as ex:
            for _ in ex.map(put, files):
                done += 1
                if done % 5000 == 0:
                    log_fn("uploaded %d/%d" % (done, len(files)))
        if have is not None:
            keep = {prefix + "/" + r for r in files} | {prefix + "/LICENSE.txt", prefix + "/index.json"}
            extra = sorted(k for k in store.list_keys(prefix + "/") if k not in keep)
            if extra:
                store.delete_keys(extra, prefix)
                log_fn("deleted %d files of the replaced build" % len(extra))
        store.put(prefix + "/LICENSE.txt", open(os.path.join(out, "LICENSE.txt"), "rb").read(),
                  "text/plain; charset=utf-8", IMMUTABLE)
        store.put(prefix + "/index.json", open(os.path.join(out, "index.json"), "rb").read(), "application/json",
                  IMMUTABLE)
    back = store.get_json(prefix + "/index.json")
    if not back or back.get("digest") != index["digest"]:
        raise RuntimeError("index.json read back does not match the upload")
    reg = S.REGIONS[name]
    regions.setdefault("regions", {})[name] = {
        "v": version, "box": index["box"], "zmin": index["zmin"], "zmax": index["zmax"],
        "prio": reg["prio"], "digest": index["digest"][:16], "built": index["built_utc"]}
    regions["updated_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    store.put(REGIONS_KEY, json.dumps(regions, indent=1, sort_keys=True).encode(), "application/json", POINTER)
    if (store.get_json(REGIONS_KEY) or {}).get("regions", {}).get(name, {}).get("v") != version:
        raise RuntimeError("regions.json read back does not name this version")
    if same:
        log_fn("%s/ already holds this build (%s): only regions.json was written" % (prefix, index["digest"][:16]))
    else:
        log_fn("published %d files + LICENSE.txt + index.json under %s/ (digest %s); regions.json updated"
               % (len(files), prefix, index["digest"][:16]))
    return True


# ---------------------------------------------------------------- command line

def join_box(argv):
    """"--box -158.1,21.6,..." -> "--box=-158.1,21.6,...": argparse would take a value starting with "-" for an option."""
    out, i = [], 0
    while i < len(argv):
        if argv[i] == "--box" and i + 1 < len(argv):
            out.append("--box=" + argv[i + 1])
            i += 2
        else:
            out.append(argv[i])
            i += 1
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--region", required=True, choices=sorted(S.REGIONS))
    b.add_argument("--out", required=True)
    b.add_argument("--version", type=int, default=1)
    b.add_argument("--box", help="lon0,lat0,lon1,lat1: build only this part (dry runs)")
    b.add_argument("--workers", type=int)
    b.add_argument("--gebco", help="the GEBCO GeoTIFF zip or a directory of its GeoTIFFs (region gebco)")
    b.add_argument("--land", help="a local copy of the GSHHG land overview (region gebco; default: downloaded)")
    b.add_argument("--keep-work", action="store_true")
    c = sub.add_parser("check")
    c.add_argument("dir")
    u = sub.add_parser("upload")
    u.add_argument("dir")
    u.add_argument("--replace", action="store_true")
    a = ap.parse_args(join_box(sys.argv[1:] if argv is None else list(argv)))
    if a.cmd == "build":
        box = tuple(float(v) for v in a.box.split(",")) if a.box else None
        index = build(a.region, a.out, version=a.version, box=box, workers=a.workers, gebco=a.gebco,
                      keep_work=a.keep_work, land=a.land)
        return 1 if check(os.path.join(a.out, a.region, "v%d" % a.version)) else (0 if index["count"] else 1)
    if a.cmd == "check":
        return 1 if check(a.dir) else 0
    if check(a.dir):
        print("refusing to upload a directory that fails the check")
        return 1
    return 0 if upload(a.dir, replace=a.replace) else 1


if __name__ == "__main__":
    sys.exit(main())
