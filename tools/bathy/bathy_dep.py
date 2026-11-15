"""Digital Earth Pacific's satellite-derived depths (plan section 97): the tile grid, a local VRT over the remote COGs,
and the tiles' lon/lat footprints. No network: the tile list is tools/bathy/dep_tiles.json (listed 2026-11-15).

DEP publishes one Cloud-Optimised GeoTIFF per 96-km tile of its grid, EPSG:3832 (WGS 84 / PDC Mercator, central
meridian 150 E), origin (-3,000,000, -4,000,000) m, 10-m pixels (9,600 x 9,600 a tile), float32 elevation (negative
below sea level), NaN where there is no estimate. Tile (x, y) covers x0 = -3e6 + 96,000 x .. x0 + 96,000 and
y0 = -4e6 + 96,000 y .. y0 + 96,000. EPSG:3832 runs on across 180 degrees, so the grid is one plain rectangle there.
"""
import json
import math
import os
from xml.sax.saxutils import escape

HERE = os.path.dirname(os.path.abspath(__file__))
TILES_FILE = os.path.join(HERE, "dep_tiles.json")
TILES_NOTE = ("version 0-0-1, a 2024 Sentinel-2 composite (files labelled 2024-07/2024-12), the 499 tiles listed "
              "2026-11-15")

URL = "https://dep-public-data.s3.us-west-2.amazonaws.com/"
KEY = "dep_s2_sdb/0-0-1/{x:03d}/{y:03d}/2024-07/2024-12/dep_s2_sdb_{x:03d}_{y:03d}_2024-07/2024-12_depth.tif"
CRS = "EPSG:3832"
LON0 = 150.0                 # the CRS's central meridian
A = 6378137.0                # WGS 84 semi-major axis (the Mercator's x = A * (lon - LON0) in radians)
X0, Y0 = -3000000.0, -4000000.0
TILE_M = 96000.0
RES = 10.0
PX = 9600


def tiles(skip=()):
    """[(x, y)] of DEP's tiles, sorted, without those in `skip`."""
    with open(TILES_FILE) as f:
        doc = json.load(f)
    out = sorted({(int(x), int(y)) for x, y in doc["tiles"]} - {tuple(t) for t in skip})
    if not out:
        raise ValueError("no DEP tiles listed in %s" % TILES_FILE)
    return out


def bounds(x, y):
    """(xmin, ymin, xmax, ymax) of tile (x, y) in EPSG:3832 metres."""
    return X0 + x * TILE_M, Y0 + y * TILE_M, X0 + (x + 1) * TILE_M, Y0 + (y + 1) * TILE_M


def vrt_text(tile_list, url=URL):
    """A GDAL VRT placing every tile's COG (by /vsicurl/) on one EPSG:3832 grid. GDAL opens a tile's file only when a
    read reaches it (each source carries its size, type and block size)."""
    xs = [t[0] for t in tile_list]
    ys = [t[1] for t in tile_list]
    gx0, gx1, gy0, gy1 = min(xs), max(xs), min(ys), max(ys)
    w, h = (gx1 - gx0 + 1) * PX, (gy1 - gy0 + 1) * PX
    top = Y0 + (gy1 + 1) * TILE_M
    parts = ['<VRTDataset rasterXSize="%d" rasterYSize="%d">' % (w, h),
             "  <SRS>%s</SRS>" % CRS,
             "  <GeoTransform>%.1f, %.1f, 0.0, %.1f, 0.0, %.1f</GeoTransform>" % (X0 + gx0 * TILE_M, RES, top, -RES),
             '  <VRTRasterBand dataType="Float32" band="1">',
             "    <NoDataValue>nan</NoDataValue>"]
    for (x, y) in tile_list:
        name = "/vsicurl/" + url + KEY.format(x=x, y=y)
        parts += ["    <SimpleSource>",
                  '      <SourceFilename relativeToVRT="0">%s</SourceFilename>' % escape(name),
                  "      <SourceBand>1</SourceBand>",
                  '      <SourceProperties RasterXSize="%d" RasterYSize="%d" DataType="Float32" BlockXSize="512" '
                  'BlockYSize="512"/>' % (PX, PX),
                  '      <SrcRect xOff="0" yOff="0" xSize="%d" ySize="%d"/>' % (PX, PX),
                  '      <DstRect xOff="%d" yOff="%d" xSize="%d" ySize="%d"/>' % ((x - gx0) * PX, (gy1 - y) * PX, PX, PX),
                  "    </SimpleSource>"]
    parts += ["  </VRTRasterBand>", "</VRTDataset>", ""]
    return "\n".join(parts)


def write_vrt(path, tile_list, url=URL):
    with open(path, "w", encoding="utf-8") as f:
        f.write(vrt_text(tile_list, url))
    return path


def _lat(y):
    """Latitude (degrees) of EPSG:3832 northing y (the ellipsoidal Mercator's inverse)."""
    from pyproj import Transformer
    t = Transformer.from_crs(CRS, "EPSG:4326", always_xy=True)
    return t.transform(0.0, y)[1]


def lon_of(x):
    """Longitude (degrees, not wrapped) of EPSG:3832 easting x."""
    return LON0 + math.degrees(x / A)


def rects(tile_list, pad=0.001):
    """lon/lat rectangles (lon0, lat0, lon1, lat1) of the tiles, grown by `pad` degrees; a tile across 180 degrees
    gives two (one each side), so no rectangle spans the world."""
    lat_cache = {}
    out = []
    for (x, y) in tile_list:
        x0, y0, x1, y1 = bounds(x, y)
        for v in (y0, y1):
            if v not in lat_cache:
                lat_cache[v] = _lat(v)
        la0, la1 = lat_cache[y0] - pad, lat_cache[y1] + pad
        lo0, lo1 = lon_of(x0) - pad, lon_of(x1) + pad
        if lo1 <= 180.0:
            out.append((max(lo0, -180.0), la0, lo1, la1))
        elif lo0 >= 180.0:
            out.append((lo0 - 360.0, la0, min(lo1 - 360.0, 180.0), la1))
        else:
            out.append((lo0, la0, 180.0, la1))
            out.append((-180.0, la0, lo1 - 360.0, la1))
    return out

# ---------------------------------------------------------------- presence: which blocks hold a usable pixel

GROW_M = 30.0                # a pixel this close (EPSG:3832 metres) to a block counts for it (the warp's reach)
BAND = 512                   # rows read at a time (one block row of the COG): ~20 MB of float32


def tile_path(x, y, url=URL):
    """GDAL's path of tile (x, y)'s COG."""
    return "/vsicurl/" + url + KEY.format(x=x, y=y)


def _merc_tile_y(lat, n):
    """EPSG:3857 XYZ row (floor) of latitudes `lat` (degrees, array) at n = 2 ** z tiles; clamped to 0..n-1."""
    import numpy as np
    lat = np.clip(lat, -85.0511287798, 85.0511287798)
    my = 6378137.0 * np.log(np.tan(np.pi / 4 + np.radians(lat) / 2))
    w = 2 * np.pi * 6378137.0
    return np.clip(np.floor((w / 2 - my) / w * n).astype(np.int64), 0, n - 1)


def _merc_tile_x(lon, n):
    """EPSG:3857 XYZ column of longitudes `lon` (degrees, array, any value: taken modulo 360) at n = 2 ** z tiles."""
    import numpy as np
    lon = np.mod(np.asarray(lon, dtype=np.float64) + 180.0, 360.0)
    return np.clip(np.floor(lon / 360.0 * n).astype(np.int64), 0, n - 1)


def tile_axes(x, y, bz, grow_m=GROW_M):
    """For DEP tile (x, y): per column the first and last zoom-bz tile column its pixel (grown by grow_m) reaches, and
    the same per row (rows from the top). EPSG:3832 and EPSG:3857 are both Mercator, so columns map to longitudes and
    rows to latitudes on their own (the 180th meridian taken modulo 360)."""
    import numpy as np
    from pyproj import Transformer
    n = 2 ** bz
    x0, y0, x1, y1 = bounds(x, y)
    c = np.arange(PX, dtype=np.float64)
    left, right = x0 + c * RES - grow_m, x0 + (c + 1) * RES + grow_m
    lon = LON0 + np.degrees(np.concatenate([left, right]) / A)
    tx = _merc_tile_x(lon, n)
    col_lo, col_hi = tx[:PX], tx[PX:]
    top, bot = y1 - c * RES + grow_m, y1 - (c + 1) * RES - grow_m
    t = Transformer.from_crs(CRS, "EPSG:4326", always_xy=True)
    _, lat = t.transform(np.zeros(2 * PX), np.concatenate([top, bot]))
    ty = _merc_tile_y(np.asarray(lat), n)
    row_lo, row_hi = ty[:PX], ty[PX:]            # the top edge is further north: the smaller row number
    return col_lo, col_hi, row_lo, row_hi


def band_blocks(ok, r0, col_lo, col_hi, row_lo, row_hi):
    """The (tile x, tile y) pairs of zoom bz holding a True pixel of `ok` (a band of rows r0.. of the DEP tile)."""
    import numpy as np
    found = set()
    rows = np.flatnonzero(ok.any(axis=1))
    if not rows.size:
        return found
    rl, rh = row_lo[r0 + rows], row_hi[r0 + rows]
    for ty in range(int(rl.min()), int(rh.max()) + 1):
        pick = rows[(rl <= ty) & (rh >= ty)]
        if not pick.size:
            continue
        cols = np.flatnonzero(ok[pick].any(axis=0))
        for tx in np.union1d(col_lo[cols], col_hi[cols]):
            found.add((int(tx), ty))
    return found


OPEN_TRIES = 3               # a tile that will not open is asked again this many times in all, then the build stops
OPEN_WAIT_S = 5.0            # seconds between the tries (a test sets 0)


def usable(a, max_depth=None, sane_m=12000.0, offset=0.0):
    """bool: pixels holding a usable depth, by the build's own rules in the build's order (read_field): finite and not
    a fill value (|value| < sane_m) as read; then, the source's offset added, below sea level (DEP's water: a value at
    or above 0 m is never drawn) and no deeper than max_depth."""
    import numpy as np
    with np.errstate(invalid="ignore"):
        ok = np.isfinite(a) & (np.abs(a) < sane_m)
        v = a + np.float32(offset) if offset else a
        ok &= v < 0
        if max_depth is not None:
            ok &= v >= -float(max_depth)
    return ok


def open_tile(path, label, tries=None, wait_s=None):
    """The opened tile. A tile listed in dep_tiles.json that will not open (403, 404, 503, a broken header) is asked
    again (`OPEN_TRIES` in all, `OPEN_WAIT_S` apart), then the build STOPS: its blocks would otherwise be silently
    empty, and the build and its check would still pass."""
    import time
    import rasterio
    tries = OPEN_TRIES if tries is None else tries
    wait_s = OPEN_WAIT_S if wait_s is None else wait_s
    err = None
    for i in range(max(1, tries)):
        if i:
            time.sleep(wait_s)
        try:
            return rasterio.open(path)
        except rasterio.errors.RasterioIOError as e:
            err = e
    raise RuntimeError("DEP tile %s could not be opened after %d tries (%s): the tile list may be out of date"
                       % (label, max(1, tries), err))


def presence(args):
    """(tile, the set of zoom-bz tiles holding a usable pixel of DEP tile (x, y) within grow_m metres): the blocks a
    build has to read. The tile's full resolution is read once, a band of rows at a time (DEP's overviews are drawn by
    nearest and miss pixels, so they cannot say where data is). args = ((x, y), bz, max_depth[, path[, offset[,
    sane_m]]]). A tile that will not open stops the build (open_tile); a wrong size raises."""
    (x, y), bz, max_depth = args[:3]
    path = args[3] if len(args) > 3 and args[3] else tile_path(x, y)
    offset = args[4] if len(args) > 4 and args[4] is not None else 0.0
    sane_m = args[5] if len(args) > 5 and args[5] is not None else 12000.0
    from rasterio.windows import Window
    col_lo, col_hi, row_lo, row_hi = tile_axes(x, y, bz)
    found = set()
    with open_tile(path, "%03d_%03d" % (x, y)) as d:
        if d.width != PX or d.height != PX:
            raise ValueError("DEP tile %03d_%03d is %d x %d, not %d x %d" % (x, y, d.width, d.height, PX, PX))
        for r0 in range(0, PX, BAND):
            h = min(BAND, PX - r0)
            ok = usable(d.read(1, window=Window(0, r0, PX, h)), max_depth, sane_m, offset)
            found |= band_blocks(ok, r0, col_lo, col_hi, row_lo, row_hi)
    return (x, y), found


def tiles_in(tile_list, box, pad=0.001):
    """The tiles whose footprint (rects) meets the lon/lat box."""
    out = []
    for t in tile_list:
        for r in rects([t], pad):
            if r[0] < box[2] and box[0] < r[2] and r[1] < box[3] and box[1] < r[3]:
                out.append(t)
                break
    return out
