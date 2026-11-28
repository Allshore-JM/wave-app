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

The 1-m lidar regions (plan section 96, bathy_sources) add three things. Their `fill` sources (the CUDEM grids under the
lidar) are read and merged under the lidar but never decide which blocks are read: the blocks are those under the lidar's
own footprints, plus `ring` blocks around them (fill only) so the water rule reaches the deep water beyond the lidar.
Inside the lidar's edge the depth blends linearly from the lidar to the fill (flagged "blended"), so no step shows where
the lidar ends: over bathy_sources.FEATHER_PX pixels at the survey's edge and round big holes, narrower round small holes
(the width follows the gap's own size: feather). Where two of a region's own surveys meet (Rincon: NGS 2019 and
USACE 2018; Niihau: NGS 2021 and USACE 2013) the older one fades in under the newer one by the same rule (owner's
choice, 2026-11-14), so no step shows at the hand-over either; the fade never lifts a pixel the newer survey has as
water above mean sea level (owner, 2026-11-14: water stays water). A source with a `geoid` (NAD83(PA11) ellipsoid heights) has the geoid
height N subtracted, read bilinear onto the block's own grid. Their `margin` (pixels read around a block) is wider than
the default 8 so the blend is exact at every pixel a tile uses; their `zmin` keeps the coarser zooms to the CUDEM regions.
A footprint read from a projected VRT is transformed to lon/lat.

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
import bathy_bluetopo as BT  # noqa: E402
import bathy_dep as D       # noqa: E402
import bathy_fetch as F     # noqa: E402
import bathy_lakes as L     # noqa: E402
import bathy_stac as STAC   # noqa: E402
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
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif,.TIF,.tiff,.TIFF,.vrt,.zip", "GDAL_HTTP_MAX_RETRY": "5", "GDAL_HTTP_RETRY_DELAY": "3",
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

def margin_of(reg):
    """Pixels read around a block at the region's finest zoom (a lidar region's blend needs more than the default 8)."""
    return reg.get("margin", MARGIN)


def region_config(name, box=None, local=None):
    """The region's settings and its sources, as plain data for the worker processes. `local` maps a source key to a
    local file, and "geoid:<name>" to a local geoid grid (tests)."""
    reg = dict(S.REGIONS[name])
    srcs = []
    for sid, fill in [(k, False) for k in reg["sources"]] + [(k, True) for k in reg.get("fill", ())]:
        s = dict(S.SOURCES[sid], key=sid, fill=fill)
        if local and sid in local:
            s["local"] = local[sid]
        s["gdal"] = S.gdal_path(s)
        if s.get("geoid"):
            s["geoid_path"] = (local or {}).get("geoid:" + s["geoid"]) or S.GEOIDS[s["geoid"]]
            if not (local and "geoid:" + s["geoid"] in local) and committed_grid(s["geoid_path"]):
                check_datum_grid(s["geoid_path"])
        srcs.append(s)
    srcs.sort(key=lambda s: s["id"])          # best first = the lowest id first, whatever order the region lists them in
    ids = [s["id"] for s in srcs]
    if len(set(ids)) != len(ids) or any(not 1 <= i <= 31 or i == T.LAND_ID for i in ids):
        raise ValueError("%s: source ids %s must be distinct, 1-31 and not %d (the land mark)" % (name, ids, T.LAND_ID))
    own = [s for s in srcs if not s["fill"]]
    if not own:
        raise ValueError("%s: no source of its own (fill sources never drive a region)" % name)
    if any(f["id"] < max(s["id"] for s in own) for f in srcs if f["fill"]):
        raise ValueError("%s: a fill source's id must be above every own source's (the lidar wins)" % name)
    if margin_of(reg) < MARGIN or margin_of(reg) % MARGIN:
        raise ValueError("%s: margin %s must be a multiple of %d" % (name, margin_of(reg), MARGIN))
    if (reg.get("fill") or sum(1 for s in own if s.get("survey_fade")) > 1) and margin_of(reg) < 3 * S.FEATHER_PX:
        raise ValueError("%s: margin %s below 3 x FEATHER_PX (the blend looks 3 x %d px around a pixel)" % (
            name, margin_of(reg), S.FEATHER_PX))
    if any(s.get("water_all") for s in own) and not all(s.get("water_all") for s in own):
        raise ValueError("%s: water_all must hold for every own source or none" % name)
    reg.update(name=name, srcs=srcs, zmax=max(s["zmax"] for s in own), water_all=bool(own[0].get("water_all")),
               exclude_boxes=[tuple(S.REGIONS[n]["box"]) for n in reg.get("exclude", ())])
    if reg.get("cut"):
        reg["keep_box"] = tuple(S.REGIONS[name]["box"])     # the configured box, whatever --box narrowed
    r = reg.get("reach", 0)
    if isinstance(r, bool) or not isinstance(r, (int, float)) or not 0 <= r <= 2:
        raise ValueError("%s: reach %r must be degrees, 0-2" % (name, r))
    if r and not reg.get("cut"):
        raise ValueError("%s: reach needs cut (what is read past the box is cut after the water rule)" % name)
    if reg.get("exclude_late") and not reg.get("exclude_boxes"):
        raise ValueError("%s: exclude_late without exclude" % name)
    pr = reg.get("presence")
    if pr is not None:
        if (not isinstance(pr, (list, tuple)) or not pr or any(n not in S.REGIONS or n == name for n in pr)
                or len(set(pr)) != len(pr)):
            raise ValueError("%s: presence %r must list other regions by name" % (name, pr))
        if any(max(S.SOURCES[k]["zmax"] for k in S.REGIONS[n]["sources"]) < reg["zmax"] - BLOCK_LEVELS for n in pr):
            raise ValueError("%s: a presence region ends before zoom %d" % (name, reg["zmax"] - BLOCK_LEVELS))
        reg["presence"] = list(pr)
    if not 0 <= reg["zmin"] <= reg["zmax"]:
        raise ValueError("%s: zmin %s outside 0..zmax %d" % (name, reg["zmin"], reg["zmax"]))
    seed = reg.get("seed")
    if seed is not None:
        if seed not in S.SEED_FILES:
            raise ValueError("%s: seed %r must be one of %s" % (name, seed, sorted(S.SEED_FILES)))
        if reg["water_all"]:
            raise ValueError("%s: seed has no meaning for a water_all region (no connectivity test)" % name)
        sm = reg.get("seed_m", S.SEED_M)
        if isinstance(sm, bool) or not isinstance(sm, (int, float)) or not 100.0 <= sm <= 20000.0:
            raise ValueError("%s: seed_m %r must be metres, 100-20000" % (name, sm))
        if local and "seed:" + seed in local:
            reg["seed_path"] = local["seed:" + seed]
    mh = reg.get("mhhw")
    if isinstance(mh, str):
        if mh not in S.MHHW_GRIDS:
            raise ValueError("%s: mhhw %r names no grid (have %s)" % (name, mh, sorted(S.MHHW_GRIDS)))
        reg["mhhw_path"] = (local or {}).get("mhhw:" + mh) or (S.MHHW_GRID if mh == "grid" else S.MHHW_GRIDS[mh])
        if not (local and "mhhw:" + mh in local):
            check_datum_grid(reg["mhhw_path"])
    elif isinstance(mh, bool) or not isinstance(mh, (int, float)) or not -1.0 <= mh <= 5.0:
        raise ValueError('%s: mhhw %r must be metres above mean sea level (-1..5) or a grid ("grid", "grid:<name>")'
                         % (name, mh))
    for s in srcs:
        k = s.get("kind")
        if k == "stac" and not s.get("local"):
            st = s.get("stac") or {}
            if not (isinstance(st.get("collection"), str) and st["collection"].startswith("https://")
                    and isinstance(st.get("items"), int) and st["items"] > 0 and STAC.SHA_RE.match(str(st.get("digest")))):
                raise ValueError("%s: stac source %s needs a collection https URL, an item count and a digest pin"
                                 % (name, s["key"]))
        if k == "tilezip" and not s.get("local"):
            if not s.get("mirror") or not s.get("crs"):
                raise ValueError("%s: tilezip source %s needs its mirror manifest and its CRS" % (name, s["key"]))
    if box:
        b = reg["box"]
        reg["box"] = (max(b[0], box[0]), max(b[1], box[1]), min(b[2], box[2]), min(b[3], box[3]))
    if r:
        reg["reach_box"] = reach_box(reg["box"], r)
    return reg


def reach_box(box, r):
    """The box grown by r degrees each way (latitudes held inside +-85): what a region with `reach` reads, so a river or
    bay whose mouth lies in the neighbour's box still joins the sea; the part outside the region's own box is cut after
    the water rule (keep_box)."""
    w, s, e, n = box
    return (w - r, max(s - r, -85.0), e + r, min(n + r, 85.0))


def read_box(reg):
    """The lon/lat box a region reads: its box, grown by its reach (reach_box)."""
    return reg.get("reach_box") or reg["box"]


def committed_grid(path):
    """A datum grid committed beside the source table (bathy_sources.DATUM_DIR), as opposed to a remote geoid such as
    "/vsicurl/https://cdn.proj.org/..." (which os.path.isabs calls absolute on Linux)."""
    if path.startswith("/vsi"):
        return False
    return os.path.normcase(os.path.dirname(os.path.abspath(path))) == os.path.normcase(os.path.abspath(S.DATUM_DIR))


def check_datum_grid(path):
    """A committed datum grid must be the one pinned in bathy_sources.DATUM_GRIDS: a changed grid moves every depth."""
    name = os.path.basename(path)
    want = S.DATUM_GRIDS.get(name)
    if not want:
        raise ValueError("%s: no pinned sha256 in bathy_sources.DATUM_GRIDS" % name)
    if not os.path.exists(path):
        raise ValueError("%s: the datum grid is missing (tools/bathy/build_datum_grids.py writes it)" % path)
    with open(path, "rb") as f:
        got = hashlib.sha256(f.read()).hexdigest()
    if got != want:
        raise ValueError("%s: hash %s is not the pinned %s" % (name, got[:16], want[:16]))
    return path


# A named client: the frame host's edge refuses Python's default "Python-urllib" user agent (HTTP 403).
USER_AGENT = "allshoresurf-bathy-build/1.0"


# Waits between tries of a download or an upload that met a passing fault (2026-11-16: NOAA's bucket answered one
# HTTP 500 to a VRT read; R2 answered "Reduce your concurrent request rate" to one upload): five tries in ~3 minutes.
RETRY_WAITS = (5, 15, 45, 120)
_sleep = time.sleep              # tests replace it
STORAGE_BUSY_CODES = ("ServiceUnavailable", "SlowDown", "TooManyRequests", "RequestLimitExceeded", "InternalError")


def http_transient(exc):
    """A download fault worth another try: HTTP 429 or 5xx, a dropped or timed-out connection. Not 4xx: a missing or
    refused file stays missing."""
    import http.client
    import urllib.error
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code == 429 or exc.code >= 500
    return isinstance(exc, (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException))


def storage_busy(exc):
    """An upload fault worth another try: the store's slow-down / unavailable answers that outlasted botocore's own
    quick retries, or a dropped connection."""
    response = getattr(exc, "response", None)
    code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
    if code in STORAGE_BUSY_CODES:
        return True
    try:
        from botocore.exceptions import ConnectionError as BotoConnectionError, ReadTimeoutError
    except ImportError:
        return isinstance(exc, (ConnectionError, TimeoutError))
    return isinstance(exc, (BotoConnectionError, ReadTimeoutError, ConnectionError, TimeoutError))


def retrying(fn, transient, what, waits=None, log_fn=print):
    """fn() once, then again after each wait while it fails with a passing fault; anything else, or the last try's
    fault, is raised as it came."""
    waits = RETRY_WAITS if waits is None else waits
    for wait in tuple(waits) + (None,):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001: classified by `transient`
            if wait is None or not transient(exc):
                raise
            log_fn("%s: %s: %s; trying again in %d s" % (what, type(exc).__name__, str(exc)[:200], wait))
            _sleep(wait)


def _https_bytes(url, timeout=60):
    import certifi
    ctx = ssl.create_default_context(cafile=certifi.where())
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    def once():
        with urllib.request.urlopen(req, context=ctx, timeout=timeout) as r:
            return r.read()
    return retrying(once, http_transient, "download " + url.split("?")[0])


def _https_text(url, timeout=60):
    return _https_bytes(url, timeout).decode("utf-8")


FOOT_PAD = 0.001       # degrees added around a footprint transformed from a projected CRS (its curved edges)


def to_lonlat(crs, rects, pad=FOOT_PAD):
    """Rectangles in `crs` units -> lon/lat rectangles holding their four corners, grown by `pad` degrees."""
    from rasterio.warp import transform
    xs, ys = [], []
    for (x0, y0, x1, y1) in rects:
        xs += [x0, x1, x0, x1]
        ys += [y0, y0, y1, y1]
    lon, lat = transform(crs, "EPSG:4326", xs, ys)
    out = []
    for i in range(len(rects)):
        lo, la = lon[4 * i:4 * i + 4], lat[4 * i:4 * i + 4]
        out.append((min(lo) - pad, min(la) - pad, max(lo) + pad, max(la) + pad))
    return out


def vrt_rects(text):
    """lon/lat rectangles (lon0, lat0, lon1, lat1) of every source a GDAL VRT places. A VRT in a projected CRS (its SRS
    element) has its rectangles transformed; a geographic one is read as lon/lat as it is."""
    root = ET.fromstring(text)
    gt = [float(v) for v in root.find("GeoTransform").text.replace(",", " ").split()]
    out = []
    for r in root.iter("DstRect"):
        x, y = float(r.get("xOff")), float(r.get("yOff"))
        w, h = float(r.get("xSize")), float(r.get("ySize"))
        lon0, lon1 = gt[0] + x * gt[1], gt[0] + (x + w) * gt[1]
        lat_a, lat_b = gt[3] + y * gt[5], gt[3] + (y + h) * gt[5]
        out.append((min(lon0, lon1), min(lat_a, lat_b), max(lon0, lon1), max(lat_a, lat_b)))
    srs = root.find("SRS")
    if out and srs is not None and (srs.text or "").strip():
        from rasterio.crs import CRS
        crs = CRS.from_user_input(srs.text.strip())
        if not crs.is_geographic:
            return to_lonlat(crs, out)
    return out


def footprint(src):
    """The lon/lat rectangles where a source may have data."""
    if src.get("tiles") is not None:                    # a tiled source (BlueTopo): its tiles' outlines
        return [r for (_, r) in src["tiles"]]
    if src.get("kind") == "dep" and not src.get("local"):
        return D.rects(D.tiles(src.get("skip_tiles", ())))     # from DEP's tile grid, no network
    if src.get("local"):
        import rasterio
        with rasterio.open(src["gdal"]) as d:
            b = d.bounds
            rect = [(b.left, b.bottom, b.right, b.top)]
            if d.crs is not None and not d.crs.is_geographic:
                return to_lonlat(d.crs, rect)
        return rect
    return vrt_rects(_https_text("https://%s.s3.amazonaws.com/%s" % (S.BUCKET, src["path"])))


def clip_rects(rects, box):
    """The rectangles cut to the lon/lat box, empty ones dropped: a national set's footprint inside one region."""
    out = []
    for r in rects:
        c = (max(r[0], box[0]), max(r[1], box[1]), min(r[2], box[2]), min(r[3], box[3]))
        if c[0] < c[2] and c[1] < c[3]:
            out.append(c)
    return out


def clip_box(clip, box):
    """The box a source's footprint is cut to: the region's (`clip` True, a national set: this region's part), or the
    part of the source's own (west, south, east, north) `clip` inside the region's (a fill that only matters there)."""
    if clip is True:
        return tuple(box)
    if not (isinstance(clip, (tuple, list)) and len(clip) == 4 and clip[0] < clip[2] and clip[1] < clip[3]):
        raise ValueError("clip %r must be True or (west, south, east, north)" % (clip,))
    return (max(clip[0], box[0]), max(clip[1], box[1]), min(clip[2], box[2]), min(clip[3], box[3]))


def outside_box(box, srcs, tol=FOOT_PAD + 1e-6):
    """(source key, rectangle) of every OWN source's footprint rectangle reaching outside `box` by more than `tol`
    degrees (a projected footprint is grown by FOOT_PAD). Fill sources reach where they like."""
    bad = []
    for s in srcs:
        if s.get("fill"):
            continue
        for r in s["rects"]:
            if r[0] < box[0] - tol or r[1] < box[1] - tol or r[2] > box[2] + tol or r[3] > box[3] + tol:
                bad.append((s["key"], tuple(round(v, 4) for v in r)))
    return bad


PUBLIC = "https://models.allshoresurf.com"     # the bucket's public host: what the site itself reads


def idx_offsets(zmax):
    """{zoom: (byte offset, byte length)} of each zoom's bitset inside an idx file of a region whose finest zoom is
    zmax: zooms 11..zmax, one bit per tile of the zoom-10 cell's k x k (k = 2 ** (z - 10)), each zoom's bits padded to
    whole bytes (bathy_tiles.pack_bits)."""
    out, off = {}, 0
    for z in range(11, zmax + 1):
        n = ((2 ** (z - 10)) ** 2 + 7) // 8
        out[z] = (off, n)
        off += n
    return out


def published_presence(names, z, fetch=None, root=PUBLIC, workers=8):
    """{(x, y)} of the tiles of zoom z (11 or finer) holding a PUBLISHED tile of any of the named regions, at zoom z or
    any finer zoom (a finer tile marks its parent), read from the bucket as the site reads it: regions.json -> each
    region's index.json -> its idx files. A lidar region reads only the blocks where the published base map has water
    (its `presence`): most of a lidar survey's footprint is land. Raises when a region is not published or a file is
    damaged: an empty set would build nothing without a word."""
    fetch = fetch or _https_bytes
    if not 11 <= z <= 20:
        raise ValueError("presence: zoom %r must be 11-20 (the idx files hold zooms 11 and finer)" % (z,))
    reg_doc = json.loads(fetch("%s/%s" % (root, REGIONS_KEY)).decode("utf-8"))
    jobs = []
    for name in names:
        ent = (reg_doc.get("regions") or {}).get(name)
        if not ent:
            raise ValueError("presence: region %s is not published (regions.json)" % name)
        base = "%s/%s/%s/v%d" % (root, PREFIX, name, int(ent["v"]))
        index = json.loads(fetch(base + "/index.json").decode("utf-8"))
        zmax = int(index["zmax"])
        if zmax < z:
            raise ValueError("presence: region %s ends at zoom %d, before %d" % (name, zmax, z))
        if not index.get("idx") or index["idx"].get("zooms") != [11, zmax]:
            raise ValueError("presence: region %s has no idx files for zooms 11-%d" % (name, zmax))
        for cell in index.get("t10") or ():
            cx, cy = (int(v) for v in cell.split("_"))
            jobs.append((name, base + "/%s/%s.bin" % (index["idx"]["dir"], cell), cx, cy, zmax))

    def one(job):
        name, url, cx, cy, zmax = job
        data = fetch(url)
        offs = idx_offsets(zmax)
        want = sum(n for (_, n) in offs.values())
        if len(data) != want:
            raise ValueError("presence: %s: %d bytes, expected %d" % (url, len(data), want))
        got = set()
        for zz in range(z, zmax + 1):
            k = 2 ** (zz - 10)
            o, n = offs[zz]
            m = T.unpack_bits(data[o:o + n], (k, k))
            rows, cols = np.nonzero(m)
            sh = zz - z
            for r, c in zip(rows.tolist(), cols.tolist()):
                got.add(((cx * k + c) >> sh, (cy * k + r) >> sh))
        return got

    out = set()
    with ThreadPoolExecutor(max(1, workers)) as ex:
        for got in ex.map(one, jobs):
            out |= got
    return out


def rects_of(src):
    """The source's footprint: worked out once (build() stores it in the source), else read now."""
    return src["rects"] if "rects" in src else footprint(src)


def _presence(args):
    return D.presence(args)


def dep_blocks(reg, src, workers):
    """The blocks (tiles of zoom zmax - 3) holding a usable pixel of a DEP source, from one full-resolution read of each
    of its tiles inside the region's box (the satellite depths are sparse: most of a tile's footprint is empty ocean,
    and a block read costs about the same whether it holds data or not)."""
    bz = reg["zmax"] - BLOCK_LEVELS
    tiles = D.tiles_in(D.tiles(src.get("skip_tiles", ())), read_box(reg))
    t0 = time.time()
    found = set()
    with pool(workers) as ex:
        for _, f in ex.map(_presence, [(t, bz, src.get("max_depth"), None, src.get("offset", 0.0), S.SANE_M)
                                       for t in tiles], chunksize=1):
            found |= f
    log("%s: %d blocks hold a usable pixel, from %d tiles (%.0f s)" % (src["key"], len(found), len(tiles),
                                                                     time.time() - t0))
    return found


def sheets_in_regions(key, sheets, tol=FOOT_PAD + 1e-6):
    """Every sheet (name, lon/lat rect) of source `key` must lie inside the box (grown by its reach) of one region that
    reads the source as its own: a region's sources are cut to its box, so a sheet outside every box (one a renewed
    pin brought in) would never be drawn, without a word. Raises ValueError naming them."""
    boxes = []
    for r in S.REGIONS.values():
        if key in r.get("sources", ()):
            boxes.append(reach_box(tuple(r["box"]), r.get("reach", 0)) if r.get("reach") else tuple(r["box"]))
    out = [n for (n, b) in sheets
           if not any(b[0] >= w - tol and b[1] >= s - tol and b[2] <= e + tol and b[3] <= nn + tol
                      for (w, s, e, nn) in boxes)]
    if out:
        raise ValueError("%s: %d sheet(s) lie outside every region that reads the source, e.g. %s: add or widen a "
                         "region" % (key, len(out), out[:5]))


PREPASS_FACTOR = 16     # a sheet's pre-pass reads every 16th pixel (its overview, where the file has one)
PREPASS_HIGH_M = 5.0    # a pre-pass pixel at or below this (source datum, offset added) is low ground worth a block


def _sheet_blocks(args):
    """{(x, y)} of the blocks (zoom bz) under the low ground of one sheet, from a 1/PREPASS_FACTOR read (nearest), grown
    by one block each way."""
    path, bz, nodata, offset = args
    import rasterio
    from rasterio.enums import Resampling
    from rasterio.warp import transform
    with rasterio.open(path) as d:
        h, w = max(1, d.height // PREPASS_FACTOR), max(1, d.width // PREPASS_FACTOR)
        a = d.read(1, out_shape=(h, w), resampling=Resampling.nearest)
        nd = d.nodata if d.nodata is not None else nodata
        tr = d.transform @ d.transform.scale(d.width / w, d.height / h)
        crs = d.crs
    with np.errstate(invalid="ignore"):
        ok = np.isfinite(a) & (np.abs(a) < S.SANE_M) & (a + np.float32(offset) <= PREPASS_HIGH_M)
    if nd is not None:
        ok &= a != np.float32(nd)
    rr, cc = np.nonzero(ok)
    if not rr.size:
        return set()
    xs = tr.c + (cc + 0.5) * tr.a
    ys = tr.f + (rr + 0.5) * tr.e
    lon, lat = transform(crs, "EPSG:4326", xs.tolist(), ys.tolist())
    n = 2 ** bz
    lon, lat = np.asarray(lon), np.clip(np.asarray(lat), -85.0, 85.0)
    s = np.sin(np.radians(lat))
    tx = np.floor((lon + 180.0) / 360.0 * n).astype(np.int64)
    ty = np.floor((0.5 - np.log((1 + s) / (1 - s)) / (4 * math.pi)) * n).astype(np.int64)
    core = set(zip(tx.tolist(), ty.tolist()))
    return {(x + dx, y + dy) for (x, y) in core for dx in (-1, 0, 1) for dy in (-1, 0, 1)
            if 0 <= x + dx < n and 0 <= y + dy < n}


def sheet_blocks(reg, src, workers):
    """The blocks holding low ground of a sheet source (kinds stac and tilezip: most of a sheet's rectangle is land or
    empty), one pre-pass read per sheet."""
    bz = reg["zmax"] - BLOCK_LEVELS
    t0 = time.time()
    found = set()
    with pool(workers) as ex:
        for f in ex.map(_sheet_blocks, [(p, bz, src.get("nodata"), src.get("offset", 0.0)) for (p, _) in src["tiles"]],
                        chunksize=1):
            found |= f
    log("%s: %d blocks hold low ground, from %d sheets (%.0f s)" % (src["key"], len(found), len(src["tiles"]),
                                                                  time.time() - t0))
    return found


def block_keys(reg, presence=None):
    """The blocks (tiles of zoom zmax - 3) under the footprint of any of the region's OWN sources (never a fill source's),
    inside the region's box, plus `ring` blocks around them (read for the fill and the water rule only). With
    `presence` (a set of blocks: published_presence) only the footprint blocks in it are kept, before the ring."""
    bz = reg["zmax"] - BLOCK_LEVELS
    keys = set()
    box = read_box(reg)
    for src in reg["srcs"]:
        if src.get("fill"):
            continue
        if "blocks" in src:          # the blocks found to hold a usable pixel (dep_blocks): exact, not the footprint
            bx0, by0, bx1, by1 = T.tiles_in_box(box, bz)
            keys.update(k for k in src["blocks"] if bx0 <= k[0] <= bx1 and by0 <= k[1] <= by1)
            continue
        for r in rects_of(src):
            c = (max(r[0], box[0]), max(r[1], box[1]), min(r[2], box[2]), min(r[3], box[3]))
            if c[0] >= c[2] or c[1] >= c[3]:
                continue
            x0, y0, x1, y1 = T.tiles_in_box(c, bz)
            keys.update((x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1))
    if presence is not None:
        keys &= presence
    ring = reg.get("ring", 0)
    if ring and keys:
        x0, y0, x1, y1 = T.tiles_in_box(box, bz)
        grown = set()
        for (x, y) in keys:
            for dy in range(-ring, ring + 1):
                for dx in range(-ring, ring + 1):
                    if x0 <= x + dx <= x1 and y0 <= y + dy <= y1:
                        grown.add((x + dx, y + dy))
        keys = grown
    if reg.get("exclude_boxes") and not reg.get("exclude_late"):
        keys = {k for k in keys if not _inside_any(bz, k, reg["exclude_boxes"])}
    return sorted(keys)


def _inside_any(z, key, boxes):
    """True when tile z/key lies wholly inside one of the lon/lat boxes."""
    b = T.tile_bounds(z, key[0], key[1])
    (lon0, lat0), (lon1, lat1) = T.xy_to_lonlat(b[0], b[1]), T.xy_to_lonlat(b[2], b[3])
    return any(w <= lon0 and lon1 <= e and s <= lat0 and lat1 <= n for (w, s, e, n) in boxes)


def bluetopo_scheme(work, local=None):
    """A local path of BlueTopo's newest tile scheme: the tests' own ("bluetopo:scheme" in `local`), else downloaded."""
    if local and "bluetopo:scheme" in local:
        return local["bluetopo:scheme"]
    dest = os.path.join(work, "bluetopo_scheme.gpkg")
    log("BlueTopo tile scheme: %s" % BT.fetch_scheme(dest, _https_bytes))
    return dest


def dep_vrt(src, work):
    """The local VRT over a "dep" source's COGs, in `work`: every listed tile but the source's skip_tiles (those are
    never read, by the pre-pass or the build)."""
    return D.write_vrt(os.path.join(work, src["key"] + ".vrt"), D.tiles(src.get("skip_tiles", ())))


def exclude_mask(b, size, boxes):
    """bool (size, size): the pixels of the EPSG:3857 grid b whose centre lies inside one of the lon/lat boxes
    (longitudes taken modulo 360, so a grid reaching past +-180 is judged by its real longitude)."""
    res = (b[2] - b[0]) / size
    lon = np.degrees((b[0] + (np.arange(size) + 0.5) * res) / T.R)
    lon = (lon + 180.0) % 360.0 - 180.0
    lat = np.degrees(2 * np.arctan(np.exp((b[3] - (np.arange(size) + 0.5) * (b[3] - b[1]) / size) / T.R)) - np.pi / 2)
    out = np.zeros((size, size), bool)
    for (w, s, e, n) in boxes:
        cx = (lon >= w) & (lon <= e)
        cy = (lat >= s) & (lat <= n)
        if cx.any() and cy.any():
            out |= cy[:, None] & cx[None, :]
    return out


def drop_mask(b, size, exclude=None, keep=None):
    """bool (size, size) of the EPSG:3857 grid b, or None: pixels whose centre lies inside one of the `exclude` boxes
    (another region's surveys) or outside the `keep` box (a region cut to its own box: its neighbour draws there)."""
    m = exclude_mask(b, size, exclude) if exclude else None
    if keep:
        out = ~exclude_mask(b, size, [keep])
        m = out if m is None else (m | out)
    return m


def _cut(depth, src, mask):
    """Nothing at the masked pixels (in place): no depth, no source."""
    if mask is not None and mask.any():
        depth[mask] = np.nan
        src[mask] = 0


def _hits(rects, lonlat_box):
    """True when any rectangle overlaps the lon/lat box."""
    a0, b0, a1, b1 = lonlat_box
    return any(r[0] < a1 and r[2] > a0 and r[1] < b1 and r[3] > b0 for r in rects)


# ---------------------------------------------------------------- pass 1: read the blocks

def _warp(path, b, size, resampling, nodata=None, key="?"):
    """One raster warped onto the block grid (EPSG:3857 bounds b, size x size px) as float32, NaN where it has none."""
    import rasterio
    from rasterio.transform import from_bounds
    from rasterio.vrt import WarpedVRT
    with rasterio.open(path) as d:
        kw = dict(crs="EPSG:3857", transform=from_bounds(*b, size, size), width=size, height=size,
                  resampling=resampling, nodata=np.nan, dtype="float32")
        nd = d.nodata if d.nodata is not None else nodata
        if nd is not None:
            kw["src_nodata"] = nd
        elif key != "geoid":
            raise ValueError("%s declares no nodata value: name it in bathy_sources (a fill value would read as a "
                             "depth)" % key)
        with WarpedVRT(d, **kw) as v:
            return v.read(1)


def feather(main, main_id, fill, fill_id, px=None):
    """The merged field: the region's own sources where they have a value, the fill sources elsewhere; inside the own
    sources' edge the depth goes linearly from the fill to the own value, flagged FLAG_BLENDED. Returns (elevation,
    source ids).

    The fade's width follows the size of the gap it surrounds (owner's choice, plan section 96, option C): a lidar
    pixel at distance d from its nearest fill-only pixel q blends when d < width, where width = the gap's depth near q
    (the largest distance from a fill-only pixel to the lidar within px of q, a square window), clipped to
    FEATHER_MIN_PX .. px. The survey's outer edge and big holes keep the full px; a hole a few pixels across gets a
    fade a few pixels wide, so the reef round it stays the lidar's own. Every quantity is measured within 3 x px of
    the pixel, so a block read with a margin of at least 3 x px gives the same values as a bigger read (blocks agree
    at their shared edges; region_config holds the margin to it).

    read_field also uses it between a region's own surveys: `main` = the newer surveys merged so far, `fill` = the
    next older one (owner's choice, 2026-11-14: the older survey fades in at the newer one's edge)."""
    px = S.FEATHER_PX if px is None else px
    elev = np.where(np.isfinite(main), main, fill).astype(np.float32)
    ids = np.where(np.isfinite(main), main_id, np.where(np.isfinite(fill), fill_id, 0)).astype(np.uint8)
    fillonly = np.isnan(main) & np.isfinite(fill)
    if fillonly.any() and px > 0:
        from scipy import ndimage
        d, (iy, ix) = ndimage.distance_transform_edt(~fillonly, return_indices=True)
        depth = ndimage.maximum_filter(ndimage.distance_transform_edt(fillonly), size=2 * px + 1, mode="nearest")
        width = np.clip(depth[iy, ix], S.FEATHER_MIN_PX, px).astype(np.float32)
        del iy, ix, depth
        band = np.isfinite(main) & np.isfinite(fill) & (d < width)
        if band.any():
            w = (d[band] / width[band]).astype(np.float32)
            elev[band] = w * main[band] + (1 - w) * fill[band]
            ids[band] = main_id[band] | T.FLAG_BLENDED
    return elev, ids


def block_geom(reg, key):
    """Block `key` at the region's finest zoom: (EPSG:3857 bounds with the margin, side px with the margin, margin px,
    core px)."""
    m = margin_of(reg)
    px = T.TILE * 2 ** BLOCK_LEVELS
    return T.tile_bounds(reg["zmax"] - BLOCK_LEVELS, key[0], key[1], apron=m, px=px), px + 2 * m, m, px


def grid_mhhw(reg):
    return isinstance(reg.get("mhhw"), str)


def mhhw_field(reg, key):
    """MHHW above mean sea level over block `key` (with its margin), metres: the region's number, or for mhhw "grid" the
    Lower-48 grid read bilinearly onto the block (a block outside the grid stops the build)."""
    if not grid_mhhw(reg):
        return float(reg["mhhw"])
    from rasterio.enums import Resampling
    b, size, _, _ = block_geom(reg, key)
    a = _warp(reg.get("mhhw_path") or S.MHHW_GRID, b, size, Resampling.bilinear, key="geoid")
    if not np.isfinite(a).all():
        raise ValueError("%s: block %s reaches outside the MHHW grid" % (reg["name"], key))
    return a


def low_threshold(reg, key):
    """The smallest depth code that counts as low over block `key` (with its margin): one number, or one per pixel for a
    region whose mhhw is "grid" (bathy_water.low_by)."""
    if not grid_mhhw(reg):
        return W.low_code(reg["mhhw"])
    return T.encode_depth(-mhhw_field(reg, key))


def _thr_of(reg, f):
    return f["thr"] if "thr" in f.files else W.low_code(reg["mhhw"])


def _read(s, b, size, resampling, ll):
    """One source on the block grid: its file, or for a tiled source (BlueTopo) every tile meeting the block, the first
    tile with a value winning (the finest resolution first: bathy_bluetopo.tiles_in)."""
    if s.get("tiles") is None:
        return _warp(s["gdal"], b, size, resampling, s.get("nodata"), s["key"])
    a = np.full((size, size), np.nan, np.float32)
    for path, r in s["tiles"]:
        if not _hits([r], ll):
            continue
        t = _warp(path, b, size, resampling, s.get("nodata"), s["key"])
        take = np.isnan(a) & np.isfinite(t)
        a[take] = t[take]
    return a


def read_field(reg, key):
    """(elevation float32, source ids uint8) of block `key`, (2048 + 2 margin) px square at zoom zmax; NaN = no data.
    Own sources are merged by id (the lower id wins: the newer survey); where an older own source meets the newer
    ones it fades in under them (feather, the same gap-size rule), never lifting the newer surveys' water above mean
    sea level (survey_fade). The fill sources are merged first-valid beneath
    them, then blended at the own sources' edge (feather)."""
    from rasterio.enums import Resampling
    b, size, m, px = block_geom(reg, key)
    (lon0, lat0), (lon1, lat1) = T.xy_to_lonlat(b[0], b[1]), T.xy_to_lonlat(b[2], b[3])
    ll = (min(lon0, lon1), min(lat0, lat1), max(lon0, lon1), max(lat0, lat1))
    layers = {False: [np.full((size, size), np.nan, np.float32), np.zeros((size, size), np.uint8)],
              True: [np.full((size, size), np.nan, np.float32), np.zeros((size, size), np.uint8)]}
    ground = (b[2] - b[0]) / size * math.cos(math.radians(T.xy_to_lonlat(0.0, (b[1] + b[3]) / 2)[1]))
    geoids = {}
    mhhw = None
    any_fill = False
    ex = (exclude_mask(b, size, reg["exclude_boxes"]) if reg.get("exclude_boxes") and not reg.get("exclude_late")
          else None)        # exclude_late: the water rule sees the other box's water; pass2 cuts it
    for s in reg["srcs"]:
        if "rects" in s and not _hits(s["rects"], ll):
            continue
        coarser = s["res_m"] > 1.25 * ground      # enlarging: bilinear, not "average" (which would copy 2 x 2 blocks)
        a = _read(s, b, size, Resampling.bilinear if coarser else Resampling.average, ll)
        if s.get("geoid_path"):
            if not (np.abs(a) < S.SANE_M).any():   # nothing real in the block: the geoid is never read
                continue
            g = s["geoid_path"]
            if g not in geoids:
                n = _warp(g, b, size, Resampling.bilinear, key="geoid")
                n[~(np.abs(n) < S.GEOID_SANE_M)] = np.nan
                geoids[g] = n
            a = a - geoids[g]                      # ellipsoid height -> geoid height; NaN where the grid has none
        a[~(np.abs(a) < S.SANE_M)] = np.nan       # after the geoid: a fill value minus N is still a fill value
        a += np.float32(s["offset"])
        if s.get("water_all"):
            with np.errstate(invalid="ignore"):
                a[a >= 0] = np.nan          # every value is taken as water: one at or above sea level is not drawn
        if s.get("max_depth") is not None:
            with np.errstate(invalid="ignore"):
                a[a < -np.float32(s["max_depth"])] = np.nan     # deeper than the source is trusted: left out
        if ex is not None:
            a[ex] = np.nan                                     # another region's own surveys hold this ground
        if s.get("clip_area"):
            # a source clipped to (west, south, east, north): the warp reads the whole block, so the pixels outside
            # the clip are dropped here (a 1-m fill must not win over the 3-m grid past the edge it was clipped to)
            a[~exclude_mask(b, size, [s["clip_area"]])] = np.nan
        elev, ids = layers[bool(s.get("fill"))]
        if not s.get("fill") and s.get("survey_fade") and (np.isfinite(elev) & np.isfinite(a)).any():
            # an older survey meeting the newer ones: it fills their gaps and fades in at their edge (feather);
            # where the two do not overlap this is the plain merge below
            if mhhw is None:
                mhhw = mhhw_field(reg, key)
            layers[False] = list(survey_fade(elev, ids, a, s["id"], mhhw))
            continue
        take = np.isnan(elev) & np.isfinite(a)
        elev[take] = a[take]
        ids[take] = s["id"]
        any_fill = any_fill or bool(s.get("fill"))
    main, main_id = layers[False]
    if not any_fill:
        return main, main_id
    return feather(main, main_id, *layers[True])


def survey_fade(newer, newer_id, older, older_id, mhhw):
    """The older survey faded in under the newer ones (feather), except that a pixel the newer surveys hold as water
    (at or below `mhhw`) is never lifted above mean sea level, or above its own value when that is already higher (a
    reef flat between 0 and mhhw): the fade may make such water shallower but never turns it into land (owner,
    2026-11-14). For mhhw below 0 the cap is mhhw itself. `mhhw` is a number or one value per pixel (mhhw "grid")."""
    e, i = feather(newer, newer_id, older, np.full(older.shape, older_id, np.uint8))
    m = np.asarray(mhhw, np.float32)
    with np.errstate(invalid="ignore"):
        wet = np.isfinite(newer) & (newer <= m)
        cap = np.maximum(newer, np.minimum(np.float32(0.0), m))
        lift = wet & (e > cap)
    e[lift] = cap[lift]
    return e, i


def save_land_core(reg, key, core):
    """A block with no low pixel (high land only, G61b F1): no tile of its own, but its land is the region's land at the
    coarser zooms, so its core at the block zoom is kept as a land-only core (the coarser levels never write a tile
    without water). A block with no value at all keeps nothing."""
    if not (core > 0).any():
        return
    depth = np.full(core.shape, np.nan, np.float32)
    src = np.where(core > 0, T.LAND_ID, 0).astype(np.uint8)
    for _ in range(BLOCK_LEVELS):
        depth, src = T.pool2(depth, src)
    if (src == T.LAND_ID).any():
        save_core(reg["work"], reg["zmax"] - BLOCK_LEVELS, key[0], key[1], depth, src)


def _work_path(reg, key):
    return os.path.join(reg["work"], "b_%d_%d.npz" % key)


def pass1(args):
    """Read one block, keep it on disk as depth codes, return the summary of its low areas at the finest zoom
    (bathy_water.summarise), or None when its core holds no low pixel."""
    reg, key = args
    elev, src_id = read_field(reg, key)
    code = T.encode_depth(-elev)
    px = T.TILE * 2 ** BLOCK_LEVELS
    m = margin_of(reg)
    core = code[m:m + px, m:m + px]
    if reg.get("water_all"):
        # every pixel the source gives is water (DEP masked land itself): no land, no connectivity test
        if not (core > 0).any():
            return key, None
        np.savez_compressed(_work_path(reg, key), code=code, src=src_id)
        return key, True
    thr = low_threshold(reg, key)
    low = W.low_by(core, thr if np.ndim(thr) == 0 else thr[m:m + px, m:m + px])
    if not low.any():
        save_land_core(reg, key, core)
        return key, None
    extra = {} if np.ndim(thr) == 0 else {"thr": thr}
    np.savez_compressed(_work_path(reg, key), code=code, src=src_id, **extra)
    return key, W.summarise(low, seed_of(reg, key, core))


def seed_of(reg, key, core):
    """The pixels of block `key`'s core that seed the open water: deeper than DEEP_M, or for a region with `seed` "sea" /
    "lake" (bathy_sources.SEED_M) on the shoreline file's water side and either at least seed_m from its shore or
    deeper than DEEP_M. bool (px, px) at the finest zoom; only low pixels count (bathy_water.summarise)."""
    deep = W.deep_of(core)
    if not reg.get("seed"):
        return deep
    water, far = seed_masks(reg, key)
    return water & (far | deep)


def seed_masks(reg, key):
    """(water, far) of block `key`'s core at the finest zoom, from the region's shoreline file: water = the file's water
    side ("sea": not GSHHG land; "lake": inside a lake, outside its islands), far = water at least seed_m from the
    file's shore. Computed at the block zoom (one 256-px tile plus enough margin to see every shore within seed_m),
    then each pixel repeated 2 ** BLOCK_LEVELS times."""
    from scipy import ndimage
    zb = reg["zmax"] - BLOCK_LEVELS
    x0, y0, x1, y1 = T.tile_bounds(zb, key[0], key[1])
    res = (x1 - x0) / T.TILE
    lat = T.xy_to_lonlat((x0 + x1) / 2, (y0 + y1) / 2)[1]
    ground = res * float(np.cos(np.radians(lat)))
    seed_m = float(reg.get("seed_m", S.SEED_M))
    k = int(np.ceil(seed_m / ground)) + 1
    n = T.TILE + 2 * k
    lake = reg["seed"] == "lake"
    mask = shore_raster(reg["seed_path"], (x0 - k * res, y0 - k * res, x1 + k * res, y1 + k * res), n, holes=lake)
    water = mask if lake else ~mask
    if water.all():
        far = water.copy()
    elif not water.any():
        far = water
    else:
        far = ndimage.distance_transform_edt(water) * ground >= seed_m
    rep = 2 ** BLOCK_LEVELS
    out = []
    for a in (water, far):
        c = a[k:k + T.TILE, k:k + T.TILE]
        out.append(np.repeat(np.repeat(c, rep, axis=0), rep, axis=1))
    return out[0], out[1]


def _sea_path(reg, key):
    return os.path.join(reg["work"], "s_%d_%d.npy" % key)


def pass_sea(args):
    """The block's sea mask at the finest zoom (its low areas labelled again, mapped through `keep`), kept on disk for
    its own tiles and its neighbours' aprons."""
    reg, key, keep = args
    px = T.TILE * 2 ** BLOCK_LEVELS
    m = margin_of(reg)
    with np.load(_work_path(reg, key)) as f:
        core = f["code"][m:m + px, m:m + px]
        thr = _thr_of(reg, f)
    if np.ndim(thr):
        thr = thr[m:m + px, m:m + px]
    np.save(_sea_path(reg, key), T.pack_bits(W.sea_of(W.low_by(core, thr), keep)))
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
        thr = _thr_of(reg, f)
    if reg.get("water_all"):
        wet = code > 0
    else:
        sea = W.sea_around(key, _sea_get(reg), px, margin_of(reg))
        wet = W.low_by(code, thr) & sea
    depth = np.where(wet, T.DECODE[code], np.nan).astype(np.float32)
    src = np.where(wet, src, np.where(code > 0, T.LAND_ID, 0)).astype(np.uint8)
    m, k = margin_of(reg), 2 ** BLOCK_LEVELS
    late = reg.get("exclude_boxes") if reg.get("exclude_late") else None
    if reg.get("keep_box") or late:
        # outside the region's own box (or inside a box it leaves out late): nothing (the neighbouring region draws
        # there); cut after the water rule, which needs the whole block
        b = T.tile_bounds(reg["zmax"] - BLOCK_LEVELS, key[0], key[1], apron=m, px=T.TILE * k)
        _cut(depth, src, drop_mask(b, depth.shape[0], late, reg.get("keep_box")))
    written = []
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
        if reg.get("exclude_boxes") or reg.get("keep_box"):
            # the cut at every zoom, not only at zmax: a coarser pixel whose centre lies in another region's box (or
            # outside this region's own box, keep_box) holds nothing of this region
            b = T.tile_bounds(reg["zmax"] - BLOCK_LEVELS, key[0], key[1], apron=m, px=T.TILE * k)
            _cut(depth, src, drop_mask(b, depth.shape[0], reg.get("exclude_boxes"), reg.get("keep_box")))
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


def coarse_levels(work, out, zfrom, zmin, wrap=False, tables=None, exclude=None, keep=None):
    """Zooms zfrom - 1 .. zmin from the saved cores of zoom zfrom: halve 2 x 2 children, save, encode. With `exclude`
    (lon/lat boxes), a pixel whose centre lies inside one holds nothing; with `keep` (one box), a pixel whose centre
    lies outside it holds nothing (the same rules as at zmax, at every zoom)."""
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
            if exclude or keep:
                _cut(d, s, drop_mask(T.tile_bounds(z, px, py), T.TILE, exclude, keep))
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


def land_file(path, work, kind="sea"):
    """A pinned shoreline overview (coast-v1): "sea" = the GSHHG land (bathy_sources.LAND), "lake" = the Great Lakes
    (bathy_sources.LAKES). A given local file is used as it is, else downloaded into `work`; checked by hash."""
    if not path:
        fname, spec = S.SEED_FILES[kind]
        path = os.path.join(work, fname)
        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(_https_bytes(spec["url"]))
        got = hashlib.sha256(open(path, "rb").read()).hexdigest()
        if got != spec["sha256"]:
            raise ValueError("the %s overview's hash %s is not the pinned %s" % (
                "land" if kind == "sea" else kind, got[:16], spec["sha256"][:16]))
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
    return shore_raster(path, T.tile_bounds(z, x, y), px)


def _ring_area(lon, lat):
    """Signed area (degrees^2) of a ring; > 0 = anticlockwise (an outer ring of coast-v1), < 0 = clockwise (a hole)."""
    return 0.5 * float(np.sum(lon * np.roll(lat, -1) - np.roll(lon, -1) * lat))


def shore_raster(path, bounds, px, holes=False):
    """bool (px, px) over EPSG:3857 `bounds` (x0, y0, x1, y1; row 0 at y1): True inside the coast-v1 file's polygons.
    holes=True (the lakes set: islands are clockwise rings of the same piece) fills the anticlockwise rings first and
    then clears the clockwise ones; otherwise every ring is filled (the land set has no holes)."""
    from PIL import Image, ImageDraw
    boxes, rings = land_pieces(path)
    x0, y0, x1, y1 = bounds
    lon0, lat0 = T.xy_to_lonlat(x0, y0)
    lon1, lat1 = T.xy_to_lonlat(x1, y1)
    hit = np.flatnonzero((boxes[:, 0] <= lon1) & (boxes[:, 2] >= lon0) & (boxes[:, 1] <= lat1) & (boxes[:, 3] >= lat0))
    img = Image.new("L", (px, px), 0)
    if hit.size:
        draw = ImageDraw.Draw(img)
        res = (x1 - x0) / px
        later = []
        for i in hit:
            for lon, lat in rings[i]:
                mx, my = _merc(lon, lat)
                pts = list(zip(((mx - x0) / res).tolist(), ((y1 - my) / res).tolist()))
                if holes and _ring_area(lon, lat) < 0:
                    later.append(pts)
                else:
                    draw.polygon(pts, fill=1)
        for pts in later:
            draw.polygon(pts, fill=0)
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
        if reg.get("seed"):
            reg["seed_path"] = land_file(reg.get("seed_path") or (land if reg["seed"] == "sea" else None), work,
                                         reg["seed"])
        scheme = None
        whole = tuple(S.REGIONS[name]["box"])        # the configured box, whatever --box narrowed, grown by the reach
        if reg.get("reach"):
            whole = reach_box(whole, reg["reach"])
        for s in reg["srcs"]:
            if s.get("kind") == "lakegrid" and not s.get("local"):
                s["gdal"] = s["local"] = L.fetch(s["lake"], work, USER_AGENT)     # NCEI's archive, checked by hash
            if s.get("kind") == "dep" and not s.get("local"):
                s["gdal"] = dep_vrt(s, work)
            if s.get("kind") in ("bluetopo", "stac", "tilezip"):
                if s.get("local"):          # tests: local tile files (a list, or one path)
                    files = s["local"] if isinstance(s["local"], (list, tuple)) else [s["local"]]
                    s["tiles"] = [(f, r) for f in files for r in footprint({"local": f, "gdal": f})]
                elif s["kind"] == "bluetopo":
                    if scheme is None:
                        scheme = BT.read_scheme(bluetopo_scheme(work, local))
                    s["tiles"] = BT.tiles_in(scheme, read_box(reg), s["bt_res"])
                elif s["kind"] == "stac":
                    # the pinned collection's sheets meeting the box (lon/lat outlines from the items, no file read);
                    # every sheet must lie inside a region that reads the source (none silently left out)
                    items = STAC.read_items(s["stac"], _https_bytes)
                    sheets_in_regions(s["key"], [(k, r) for (k, r, _) in items])
                    s["tiles"] = [(p, r) for (_, r, p) in items if _hits([r], whole)]
                else:
                    # the mirrored archives (bathy_mirror) meeting the box, checked by hash; each one's GeoTIFF taken
                    # out and checked; every archive must lie inside a region that reads the source
                    man = F.manifest(s["mirror"])
                    sheets_in_regions(s["key"], [(f["sha256"][:16], tuple(f["bbox"])) for f in man["files"]])
                    files = F.fetch_mirror(s["mirror"], work, s["crs"], log=log, box=whole,
                                           get=lambda u, p, ua: retrying(lambda: L.download(u, p, ua),
                                                                         http_transient, "download " + u))
                    s["tiles"] = [(f, r) for f in files for r in footprint({"local": f, "gdal": f})
                                  if _hits([r], whole)]
                log("%s: %d %s tiles meet the box" % (s["key"], len(s["tiles"]), s["kind"]))
            s["rects"] = footprint(s)     # once, here: the workers read only the blocks a source reaches
            if s.get("clip"):
                area = clip_box(s["clip"], whole)
                s["rects"] = clip_rects(s["rects"], area)
                if s["clip"] is not True:
                    s["clip_area"] = area      # cut per pixel too (read_field): a block reaches past the clip
            if s.get("kind") == "dep" and not s.get("local") and not s.get("fill"):
                s["blocks"] = dep_blocks(reg, s, workers)
            if s.get("kind") in ("stac", "tilezip") and not s.get("fill"):
                s["blocks"] = sheet_blocks(reg, s, workers)
        bad = outside_box(whole, reg["srcs"])   # the configured box (+ reach), whatever --box narrowed
        if bad:
            raise ValueError("%s: %d footprint rectangle(s) outside the region's box %s, e.g. %s: widen the box"
                             % (name, len(bad), whole, bad[:3]))
        pres = None
        if reg.get("presence"):
            pres = published_presence(reg["presence"], reg["zmax"] - BLOCK_LEVELS)
            log("%s: %d blocks of zoom %d hold published water of %s" % (name, len(pres), reg["zmax"] - BLOCK_LEVELS,
                                                                         ", ".join(reg["presence"])))
        keys = block_keys(reg, pres)
        log("%s: %d blocks of zoom %d to read (zmax %d)" % (name, len(keys), reg["zmax"] - BLOCK_LEVELS, reg["zmax"]))
        blocks = {}
        with pool(workers) as ex:
            for key, summary in ex.map(pass1, [(reg, k) for k in keys], chunksize=1):
                if summary is not None:
                    blocks[key] = summary
        log("pass 1: %d blocks hold low ground (%.0f s)" % (len(blocks), time.time() - t0))
        if not reg.get("water_all"):
            keep = W.sea_masks(blocks)
            with pool(workers) as ex:
                list(ex.map(pass_sea, [(reg, k, keep[k]) for k in sorted(blocks)], chunksize=1))
        written = []
        with pool(workers) as ex:
            for w in ex.map(pass2, [(reg, k) for k in sorted(blocks)], chunksize=1):
                written += w
        log("pass 2: %d tiles at zooms %d-%d (%.0f s)" % (len(written), reg["zmax"] - BLOCK_LEVELS, reg["zmax"],
                                                         time.time() - t0))
        written += coarse_levels(work, out, reg["zmax"] - BLOCK_LEVELS, reg["zmin"], wrap=bool(reg.get("wrap")),
                                 tables=zoom_tables(reg), exclude=reg.get("exclude_boxes"),
                                 keep=reg.get("keep_box"))
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
    sources = {str(s["id"]): dict({k: s[k] for k in ("name", "year", "res_m", "datum", "licence", "credit", "zmax")},
                                  deep=list(s["deep"]) if s.get("deep") else None,
                                  **({"short": s["short"]} if s.get("short") else {}))
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
    if any(s.get("fill") for s in reg["srcs"]):
        lines += ["- Near the lidar's edge the depth blends from the lidar to the grid beneath it (marked blended): "
                  "within %d pixels at the survey's edge, narrower round small gaps." % S.FEATHER_PX]
    if sum(1 for s in reg["srcs"] if not s.get("fill") and s.get("survey_fade")) > 1:
        lines += ["- Where two lidar surveys meet, the older one blends in under the newer one (marked blended): "
                  "within %d pixels where the newer survey ends, narrower round small gaps; the newer survey's "
                  "water never becomes land." % S.FEATHER_PX]
    dep = [s for s in reg["srcs"] if s.get("kind") == "dep"]
    for s in dep:
        lines += ["- Digital Earth Pacific Satellite-Derived Bathymetry (Sentinel-2, 10 m), Pacific Community (SPC); "
                  "%s; accessed 2026-11-15 from https://registry.opendata.aws/dep-satellite-derived-bathymetry. "
                  "Contains modified Copernicus Sentinel data 2024. Licence: CC BY 4.0." % D.TILES_NOTE,
                  "- A satellite estimate, often off by 2 m: depths deeper than %g m are left out (the regional grid "
                  "shows there), every pixel DEP gives is water, and nothing is used where the US territories' own "
                  "surveys are (%s)." % (s["max_depth"], ", ".join(reg.get("exclude", ())) or "none")]
    if any(s.get("kind") == "lakegrid" for s in reg["srcs"]):
        lines += ["- Great Lakes: depths below the lake's Low Water Datum (IGLD 1985: Superior 183.2 m, Michigan-Huron "
                  "176.0 m, St Clair 174.4 m, Erie 173.5 m, Ontario 74.2 m), the lake's chart datum, not mean sea "
                  "level. The lake grids are NOAA NCEI's (NGDC 1996-1999 compilations, Superior a draft), downloaded "
                  "from www.ngdc.noaa.gov; the lidar DEMs' heights are taken below the same datum (the two NAVD88 "
                  "surveys 0.01 m lower). Lake St Clair from Lake Erie's grid, 0.9 m lower."]
    if reg.get("mhhw") == "grid:nz":
        lines += ["- The water's edge (mean higher high water above mean sea level, 0.28-1.49 m): the EOT20 global ocean "
                  "tide model (Hart-Davis et al. 2021, DGFI-TUM; SEANOE doi:10.17882/79489; CC BY 4.0), predicted for "
                  "19 years by Allshore Surf and corrected at 12 UHSLC tide gauges."]
    if any(s.get("kind") == "stac" for s in reg["srcs"]):
        lines += ["- LINZ data: Toitu Te Whenua Land Information New Zealand, CC BY 4.0, read from s3://nz-coastal; "
                  "not endorsed by LINZ."]
    keys = {s.get("key") for s in reg["srcs"]}
    if "nsw-marine18" in keys:
        lines += ["- NSW data: Department of Planning, Industry and Environment, CC BY 4.0, ordered through ELVIS "
                  "(elevation.fsdf.org.au) and kept unchanged under static/bathy-src/; not endorsed by the NSW "
                  "Government."]
    if reg.get("mhhw") == "grid:eu":
        lines += ["- The water's edge (mean higher high water above mean sea level): the EOT20 global ocean tide model "
                  "(Hart-Davis et al. 2021, DGFI-TUM; SEANOE doi:10.17882/79489; CC BY 4.0), predicted for 19 years by "
                  "Allshore Surf and corrected at 121 tide gauges from their TICON-4 tidal constants (DGFI-TUM, SEANOE "
                  "2025; CC BY 4.0)."]
    if "emodnet-dtm24" in keys:
        lines += ["- This data product was created by EMODnet: EMODnet's mean-sea-level version of the DTM 2024 tiles, "
                  "cut to the site's boxes (static/bathy-src/emodnet-dtm-2024/)."]
    if keys & {"litto3d-naq", "litto3d-fin"}:
        lines += ["- Litto3D (Shom - IGN), Licence Ouverte 2.0: the packs' 1-m DEM, kept under static/bathy-src/; heights "
                  "above IGN69 converted to mean sea level with a grid from Shom's References Altimetriques Maritimes "
                  "(Shom, 2025, doi:10.17183/MAREE_COURANTS_RAM)."]
    if "infomar" in keys:
        lines += ["- INFOMAR survey grids (Geological Survey Ireland & Marine Institute), kept under static/bathy-src/; "
                  "heights above Lowest Astronomical Tide converted to mean sea level with an EOT20 grid corrected at "
                  "9 Irish National Tide Gauge Network stations (Marine Institute, CC BY 4.0); where surveys overlap the "
                  "newest is used."]
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
    tables = None
    if index.get("sources") and all("zmax" in s for s in index["sources"].values()):
        tables = zoom_tables({"srcs": [dict(id=int(k), zmax=s["zmax"], deep=s.get("deep"))
                                       for k, s in index["sources"].items()]})
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
            elif tables is not None:
                c = code[1:-1, 1:-1]
                if not earns(np.where(c > 0, T.DECODE[c], np.nan), sf[1:-1, 1:-1], tables, z).any():
                    problems.append("%d/%d/%d: no water pixel whose source earns zoom %d" % (z, x, y, z))
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

    def put_key(key, body, ct, cache):
        retrying(lambda: store.put(key, body, ct, cache), storage_busy, "upload " + key, log_fn=log_fn)

    def put(rel):
        ct = "image/png" if rel.endswith(".png") else "application/octet-stream"
        put_key(prefix + "/" + rel, open(os.path.join(out, rel), "rb").read(), ct, IMMUTABLE)

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
        put_key(prefix + "/LICENSE.txt", open(os.path.join(out, "LICENSE.txt"), "rb").read(),
                  "text/plain; charset=utf-8", IMMUTABLE)
        put_key(prefix + "/index.json", open(os.path.join(out, "index.json"), "rb").read(), "application/json",
                  IMMUTABLE)
    back = store.get_json(prefix + "/index.json")
    if not back or back.get("digest") != index["digest"]:
        raise RuntimeError("index.json read back does not match the upload")
    reg = S.REGIONS[name]
    regions.setdefault("regions", {})[name] = {
        "v": version, "box": index["box"], "zmin": index["zmin"], "zmax": index["zmax"],
        "prio": reg["prio"], "digest": index["digest"][:16], "built": index["built_utc"]}
    regions["updated_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    put_key(REGIONS_KEY, json.dumps(regions, indent=1, sort_keys=True).encode(), "application/json", POINTER)
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
