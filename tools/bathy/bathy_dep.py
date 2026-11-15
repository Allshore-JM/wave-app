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
TILES_NOTE = ("version 0-0-1, Sentinel-2 composite 2024-07 to 2024-12, the 499 tiles listed 2026-11-15")

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
