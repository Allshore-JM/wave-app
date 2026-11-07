"""JMA high-resolution precipitation nowcast tiles for the radar job (plan section 86): the observed frames only, never the forecast.

The tiles behind JMA's own "nowcast" page, an undocumented website backend (Public Data License 1.0, equal to CC BY 4.0: cite JMA and
say the data was edited). A frame every 5 minutes, labelled in UTC, out about 20-50 s after its label, kept at least 5 days:
  https://www.jma.go.jp/bosai/jmatile/data/nowc/<stamp>/none/<stamp>/surf/hrpns/6/<x>/<y>.png     stamp = YYYYMMDDHHMM00
Web Mercator, 256 pixels a tile, about 2 km a pixel at zoom 6. Only tiles holding rain exist; every other tile of a published frame,
and any tile off the grid, answers 200 with one 334-byte fully transparent RGBA picture (a dry tile, not a missing one). A frame not
yet published answers 404 on every tile. So: 404 = no frame; palette picture = rain; RGBA with no opaque pixel = dry; anything else
(an opaque RGBA pixel, a colour off the table, another size or mode) = a changed feed, a ValueError.

A pixel holds one of 8 rain-rate classes, not a reflectivity. Each class is stored as the job's code of the dBZ at the class's geometric
middle (Z = 200 R^1.6), so the colours match the other regions; the client snaps to these codes and its hover says the class.

Coverage: JMA publishes it: <stamp>/none/<stamp>/surf/hrpns_nd/data.geojson (gzip) is a world polygon whose hole is the area with data
(about 400 km round its radars). The tiles reach beyond Japan's waters to Taiwan: the mask stops at CUT_WEST so Taiwan keeps its own island.
"""
import gzip
import io
import json
import math

import numpy as np
from PIL import Image

BASE = "https://www.jma.go.jp/bosai/jmatile/data/nowc"
ZOOM = 6
XS, YS = range(53, 59), range(22, 29)   # the census's tiles: 42, reaching the whole data area (20.0-46.7 N, 120.4-148.9 E)
PROBE = (56, 25)                        # asked first: a frame not yet out costs one request, not 42
TILE = 256
MAX_TILE_BYTES = 256 * 1024             # the largest seen is 5.9 KB
MAX_OUTLINE_BYTES = 1024 * 1024         # 168 KB on the wire
MAX_OUTLINE_UNPACKED = 8 * 1024 * 1024  # 994 KB
CUT_WEST = 122.5
# (rgb on the palette, mm/h from, mm/h to, the code of the class's middle)
CLASSES = (
    ((242, 242, 255), 0, 1, 28),
    ((160, 210, 255), 1, 5, 49),
    ((33, 140, 255), 5, 10, 65),
    ((0, 65, 255), 10, 20, 75),
    ((250, 245, 0), 20, 30, 82),
    ((255, 153, 0), 30, 50, 89),
    ((255, 40, 0), 50, 80, 96),
    ((180, 0, 104), 80, None, 102),
)
CLASS_CODES = tuple(c[3] for c in CLASSES)
_CLASS_OF = {c[0]: i + 1 for i, c in enumerate(CLASSES)}
_CODE_OF = np.array((1,) + CLASS_CODES, dtype=np.uint8)   # class number (0 = dry) -> code (1 = covered, no echo)


def stamp_of(when):
    return when.strftime("%Y%m%d%H%M00")


def tile_url(stamp, x, y):
    return f"{BASE}/{stamp}/none/{stamp}/surf/hrpns/{ZOOM}/{x}/{y}.png"


def outline_url(stamp):
    return f"{BASE}/{stamp}/none/{stamp}/surf/hrpns_nd/data.geojson"


def tile_list():
    """The probe first, then every other tile."""
    return [PROBE] + [(x, y) for x in XS for y in YS if (x, y) != PROBE]


def decode_tile(body):
    """A tile's PNG -> uint8 (256, 256) of class numbers (0 = no rain; 1 .. 8 = CLASSES)."""
    im = Image.open(io.BytesIO(body))
    if im.size != (TILE, TILE):
        raise ValueError(f"a tile of {im.size}")
    if im.mode == "RGBA":
        if im.getchannel("A").getextrema()[1] != 0:
            raise ValueError("an RGBA tile with opaque pixels")
        return np.zeros((TILE, TILE), dtype=np.uint8)
    if im.mode != "P":
        raise ValueError(f"a tile in mode {im.mode}")
    idx = np.asarray(im, dtype=np.uint8)
    palette = im.getpalette("RGB") or []
    tr = im.info.get("transparency")
    lut = np.zeros(256, dtype=np.uint8)
    for i in np.unique(idx):
        i = int(i)
        alpha = (tr[i] if i < len(tr) else 255) if isinstance(tr, (bytes, bytearray)) else (0 if tr == i else 255)
        if alpha == 0:
            continue
        rgb = tuple(palette[3 * i:3 * i + 3])
        if rgb not in _CLASS_OF:
            raise ValueError(f"colour {rgb} is not on the rain-rate table")
        lut[i] = _CLASS_OF[rgb]
    return lut[idx]


def merc_row(lat):
    """Latitude (degrees) -> the zoom-6 pixel row, from the top of the world."""
    rad = np.radians(np.asarray(lat, dtype=np.float64))
    return (1.0 - np.arcsinh(np.tan(rad)) / math.pi) / 2.0 * (1 << ZOOM) * TILE


def merc_col(lon):
    return (np.asarray(lon, dtype=np.float64) + 180.0) / 360.0 * (1 << ZOOM) * TILE


def lattice_index(s, w, n, e, ppd):
    """(mosaic rows, mosaic columns) of the region's lattice pixel centres: separable, Mercator rows being a function of latitude alone.
    -1 = outside the mosaic."""
    lats = n - (np.arange((n - s) * ppd) + 0.5) / ppd
    lons = w + (np.arange((e - w) * ppd) + 0.5) / ppd
    rows = np.floor(merc_row(lats)).astype(np.int64) - YS[0] * TILE
    cols = np.floor(merc_col(lons)).astype(np.int64) - XS[0] * TILE
    rows[(rows < 0) | (rows >= len(YS) * TILE)] = -1
    cols[(cols < 0) | (cols >= len(XS) * TILE)] = -1
    return rows, cols


def mosaic(tiles):
    """{(x, y): class array} of every tile -> one picture."""
    out = np.zeros((len(YS) * TILE, len(XS) * TILE), dtype=np.uint8)
    for (x, y), arr in tiles.items():
        r, c = (y - YS[0]) * TILE, (x - XS[0]) * TILE
        out[r:r + TILE, c:c + TILE] = arr
    return out


def parse_outline(body):
    """The data-area file -> rings [(lon, lat) arrays] whose even-odd fill is the area with data: every ring but the world's."""
    if body[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
            body = gz.read(MAX_OUTLINE_UNPACKED + 1)
        if len(body) > MAX_OUTLINE_UNPACKED:
            raise ValueError("the outline is too large")
    doc = json.loads(body)
    polygons = []
    for feature in doc["features"]:
        geom = feature["geometry"]
        if geom["type"] == "Polygon":
            polygons.append(geom["coordinates"])
        elif geom["type"] == "MultiPolygon":
            polygons += geom["coordinates"]
        else:
            raise ValueError(f"an outline of {geom['type']}")
    rings = []
    world = 0
    for polygon in polygons:
        for ring in polygon:
            arr = np.asarray(ring, dtype=np.float64)
            if arr.ndim != 2 or arr.shape[1] != 2 or len(arr) < 4 or not np.isfinite(arr).all():
                raise ValueError("a malformed outline ring")
            if arr[:, 0].max() - arr[:, 0].min() > 300:
                world += 1
            else:
                rings.append(arr)
    if world != 1 or not rings:
        raise ValueError(f"{world} world rings and {len(rings)} data rings in the outline")
    return rings


def fill_rings(rings, s, w, n, e, ppd):
    """Bool (rows, cols) of the region's lattice: a pixel centre inside an odd number of rings. Scanline, one row at a time."""
    x0 = np.concatenate([r[:-1, 0] for r in rings] + [r[-1:, 0] for r in rings])
    y0 = np.concatenate([r[:-1, 1] for r in rings] + [r[-1:, 1] for r in rings])
    x1 = np.concatenate([r[1:, 0] for r in rings] + [r[:1, 0] for r in rings])
    y1 = np.concatenate([r[1:, 1] for r in rings] + [r[:1, 1] for r in rings])
    rows, cols = (n - s) * ppd, (e - w) * ppd
    out = np.zeros((rows, cols), dtype=bool)
    for r in range(rows):
        lat = n - (r + 0.5) / ppd
        crossing = (y0 <= lat) != (y1 <= lat)
        if not crossing.any():
            continue
        xs = x0[crossing] + (lat - y0[crossing]) / (y1[crossing] - y0[crossing]) * (x1[crossing] - x0[crossing])
        xs = np.sort(xs)
        # columns whose centre w + (c + 0.5) / ppd lies between each pair of crossings
        first = np.ceil((xs[0::2] - w) * ppd - 0.5).astype(np.int64)
        last = np.ceil((xs[1::2] - w) * ppd - 0.5).astype(np.int64)
        for a, b in zip(np.clip(first, 0, cols), np.clip(last, 0, cols)):
            out[r, a:b] = True
    return out


def coverage(rings, s, w, n, e, ppd):
    """The mask of the region's lattice, cut at CUT_WEST; refuses an area nobody would believe (a broken file reads as "no radar")."""
    mask = fill_rings(rings, s, w, n, e, ppd)
    mask[:, :int(round((CUT_WEST - w) * ppd))] = False
    area = mask.sum() / float(ppd * ppd)
    if not 150 <= area <= 800:
        raise ValueError(f"the data area is {area:.0f} square degrees")
    return mask


def lattice(classes, rows, cols, mask):
    """The mosaic's classes on the region's lattice -> (codes, rain pixels the mask dropped)."""
    pic = np.zeros((len(rows), len(cols)), dtype=np.uint8)
    rr, cc = np.nonzero(rows >= 0)[0], np.nonzero(cols >= 0)[0]
    pic[np.ix_(rr, cc)] = classes[np.ix_(rows[rr], cols[cc])]
    dropped = int(np.count_nonzero(pic[~mask]))
    out = _CODE_OF[pic]
    out[~mask] = 0
    return out, dropped
