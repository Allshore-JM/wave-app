"""Bathymetry data tiles (plan section 93b): the tile grid, the depth code and the tile file. Pure numpy + Pillow.

Tiles follow the web map's XYZ grid (EPSG:3857, 256 px). Every tile file carries a one-pixel apron so a tile's
pixels can be interpolated up to its edge from its own file. A tile file is ONE 8-bit grey PNG of 258 x 774 px,
three planes stacked top to bottom:

    rows   0-257  the depth code's high byte
    rows 258-515  the depth code's low byte
    rows 516-773  the source id (bits 0-4) and flags (bit 5 filled / estimated, bit 6 blended, bit 7 coarse)

so the site's existing 8-bit grey PNG decoder reads it unchanged. The depth code (16 bits) is depth in metres,
positive down (negative = above mean sea level, e.g. a reef flat):

    0              no data, or land
    1 .. 41001     -10 .. 400 m in 0.01 m steps        code = 1 + round((d + 10) * 100)
    41002 .. 62001 400 .. 2,500 m in 0.1 m steps       code = 41001 + round((d - 400) * 10)
    62002 .. 65401 2,500 .. 11,000 m in 2.5 m steps    code = 62001 + round((d - 2500) / 2.5)
    65402 ..       unused (refused by the checker and decoded as no data)
"""
import base64
import io
import math

import numpy as np
from PIL import Image

R = 6378137.0
WORLD = 2 * math.pi * R
TILE = 256
APRON = 1
SIDE = TILE + 2 * APRON              # 258
ROWS = 3 * SIDE                      # 774

C1, C2, C3 = 41001, 62001, 65401     # the last code of each step range
DEPTH_MIN, DEPTH_MAX = -10.0, 11000.0

SRC_BITS = 0x1F
PNG_SIG = bytes((0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A))   # the PNG signature
FLAG_FILLED, FLAG_BLENDED, FLAG_COARSE = 0x20, 0x40, 0x80


# ---------------------------------------------------------------- tile grid

def tile_bounds(z, x, y, apron=0, px=TILE):
    """(x0, y0, x1, y1) of tile z/x/y in EPSG:3857 metres, grown by `apron` pixels of a `px`-pixel tile."""
    s = WORLD / 2 ** z
    p = s / px
    return (-WORLD / 2 + x * s - apron * p, WORLD / 2 - (y + 1) * s - apron * p,
            -WORLD / 2 + (x + 1) * s + apron * p, WORLD / 2 - y * s + apron * p)


def lonlat_to_xy(lon, lat):
    """EPSG:4326 -> EPSG:3857 metres (latitude clamped to the web map's +-85.0511)."""
    lat = max(-85.0511287798, min(85.0511287798, lat))
    return R * math.radians(lon), R * math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))


def xy_to_lonlat(x, y):
    return math.degrees(x / R), math.degrees(2 * math.atan(math.exp(y / R)) - math.pi / 2)


def tile_of(lon, lat, z):
    """The tile (x, y) holding lon/lat at zoom z (fractional parts dropped)."""
    mx, my = lonlat_to_xy(lon, lat)
    n = 2 ** z
    x = int((mx + WORLD / 2) / WORLD * n)
    y = int((WORLD / 2 - my) / WORLD * n)
    return min(max(x, 0), n - 1), min(max(y, 0), n - 1)


def tiles_in_box(box, z):
    """(x0, y0, x1, y1) inclusive tile range covering box = (lon0, lat0, lon1, lat1) at zoom z."""
    x0, y1 = tile_of(box[0], box[1], z)
    x1, y0 = tile_of(box[2], box[3], z)
    return x0, y0, x1, y1


# ---------------------------------------------------------------- depth code

def encode_depth(d):
    """Depth in metres (float array; NaN = no data) -> uint16 code array."""
    d = np.asarray(d, np.float64)
    out = np.zeros(d.shape, np.uint16)
    ok = np.isfinite(d)
    v = np.clip(d[ok], DEPTH_MIN, DEPTH_MAX)
    c = np.empty(v.shape, np.int64)
    a = v <= 400.0
    c[a] = 1 + np.round((v[a] + 10.0) * 100.0).astype(np.int64)
    b = (v > 400.0) & (v <= 2500.0)
    c[b] = C1 + np.round((v[b] - 400.0) * 10.0).astype(np.int64)
    e = v > 2500.0
    c[e] = C2 + np.round((v[e] - 2500.0) / 2.5).astype(np.int64)
    out[ok] = np.clip(c, 1, C3).astype(np.uint16)
    return out


def decode_table():
    """float32[65536]: code -> depth in metres (NaN for 0 and for unused codes)."""
    c = np.arange(65536, dtype=np.float64)
    t = np.full(65536, np.nan)
    a = (c >= 1) & (c <= C1)
    t[a] = (c[a] - 1) / 100.0 - 10.0
    b = (c > C1) & (c <= C2)
    t[b] = 400.0 + (c[b] - C1) / 10.0
    e = (c > C2) & (c <= C3)
    t[e] = 2500.0 + (c[e] - C2) * 2.5
    return t.astype(np.float32)


DECODE = decode_table()


def step_at(d):
    """The code's step size (m) at depth d: the largest error of a round trip is half of it."""
    return 0.01 if d <= 400 else (0.1 if d <= 2500 else 2.5)


# ---------------------------------------------------------------- tile file

def pack_tile(code, srcflag):
    """258 x 258 uint16 codes + 258 x 258 uint8 source/flags -> PNG bytes (258 x 774, 8-bit grey)."""
    code = np.asarray(code, np.uint16)
    srcflag = np.asarray(srcflag, np.uint8)
    if code.shape != (SIDE, SIDE) or srcflag.shape != (SIDE, SIDE):
        raise ValueError("tile planes must be %d x %d" % (SIDE, SIDE))
    img = np.vstack([(code >> 8).astype(np.uint8), (code & 0xFF).astype(np.uint8), srcflag])
    bio = io.BytesIO()
    Image.fromarray(img, "L").save(bio, "PNG", optimize=True)
    return bio.getvalue()


def unpack_tile(png):
    """PNG bytes -> (codes uint16 258 x 258, source/flags uint8 258 x 258). Raises ValueError on a bad file, also on a
    PNG the site's own decoder (overlay.js parsePng: 8-bit grey, not interlaced) would refuse."""
    head = bytes(png[:29])
    if len(head) < 29 or head[:8] != PNG_SIG or head[12:16] != b"IHDR":
        raise ValueError("not a PNG")
    if head[24] != 8 or head[25] != 0 or head[28] != 0:
        raise ValueError("not an 8-bit grey, non-interlaced PNG (bit depth %d, colour type %d, interlace %d)"
                         % (head[24], head[25], head[28]))
    im = Image.open(io.BytesIO(png))
    if im.mode != "L" or im.size != (SIDE, ROWS):
        raise ValueError("not a bathymetry tile: mode %s size %s" % (im.mode, im.size))
    a = np.asarray(im, np.uint8)
    code = (a[:SIDE].astype(np.uint16) << 8) | a[SIDE:2 * SIDE].astype(np.uint16)
    return code, a[2 * SIDE:].copy()


# ---------------------------------------------------------------- bitsets

def pack_bits(mask):
    """bool 2-D array -> bytes, row-major, bit i of the array = (byte[i >> 3] >> (i & 7)) & 1."""
    return np.packbits(np.asarray(mask, bool).ravel(), bitorder="little").tobytes()


def unpack_bits(data, shape):
    n = shape[0] * shape[1]
    return np.unpackbits(np.frombuffer(data, np.uint8), count=n, bitorder="little").astype(bool).reshape(shape)


def b64(data):
    return base64.b64encode(data).decode("ascii")


# ---------------------------------------------------------------- pooling (one zoom coarser)

def pool2(depth, src):
    """Halve a (2h, 2w) depth field (NaN = no water) with its source/flags plane.

    A coarse pixel is water when at least 2 of its 4 fine pixels are; its depth is the mean of those, its source the
    lowest non-zero source id among them (lower ids are the finer sources), its flags the OR of theirs."""
    h, w = depth.shape[0] // 2, depth.shape[1] // 2
    d = depth[:2 * h, :2 * w].reshape(h, 2, w, 2)
    s = src[:2 * h, :2 * w].reshape(h, 2, w, 2)
    wet = np.isfinite(d)
    n = wet.sum(axis=(1, 3))
    tot = np.where(wet, d, 0.0).sum(axis=(1, 3))
    out = np.where(n >= 2, tot / np.maximum(n, 1), np.nan).astype(np.float32)
    ids = np.where(wet, s & SRC_BITS, 99).min(axis=(1, 3))
    f = np.where(wet, s & 0xE0, 0).astype(np.uint8)
    flags = np.bitwise_or.reduce(np.bitwise_or.reduce(f, axis=3), axis=1)
    sout = np.where(n >= 2, (np.where(ids == 99, 0, ids) | flags), 0).astype(np.uint8)
    return out, sout
