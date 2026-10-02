"""Format "apt1" of the forecast-point product: shared by the job that writes it (points.py) and by
whatever reads it. Pure: numpy and the standard library only (no eccodes, no network, no R2).

The product holds, for every sea cell of the model grids and every forecast step of one run, the combined wave
height, the wind sea, three swell partitions and the wind, as NOAA's gridded GFS-Wave files give them.

Grids (GRIDS, in priority order; the rows that hold wave data do not overlap and leave no gap):
  g16  gfswave global.0p16   1/6 deg   52.167 N .. 12.5 S   (the model's own grid there)
  s25  gfswave gsouth.0p25   1/4 deg   12.75 S .. 79.5 S    (the model's own grid there)
  n25  gfswave global.0p25   1/4 deg   52.25 N .. 90 N      (NOAA's interpolated global grid)
NOAA's global.0p16 FILE spans 52.5 N .. 15 S, but its first two and last fifteen rows carry wind and no waves
(the edge bands of the model's mosaic: the neighbouring grid has that water). So the other two grids are stored up
to the first row g16 has no waves for ("rows": first, one past last; rows outside it are never stored), and each
build checks that the rows with waves are still the ones in "data" (first, last; None where sea ice decides).
A cell is a grid point: row r, column c -> latitude lat0 - r / per_deg, longitude c / per_deg (0..360 E,
periodic). A reader takes the nearest sea cell of any grid; on a tie, the earlier grid.

Values are uint16 (little-endian). Land and ice cells are not stored at all (they are not in the mask). Inside a
tile MISSING = 65535 means "no value at this step": a partition the model did not find, or a sea cell that has no
waves at this step although it had some at the build's first step. FIELDS gives the plane order; KINDS the scale:
value = code / scale.
  height  m     x100   (0.01 m, the bulletins' precision)
  period  s     x10    (0.1 s; the partition periods are PEAK periods)
  direction deg x1     (degrees true the waves / the wind come FROM; 360 is stored as 0)
  speed   m/s   x10

One tile = one object, a block of `tile` x `tile` cells (4 degrees), holding only its sea cells:
  b"APT1" | u32le header length | header (JSON, UTF-8) | bitmap | payload
  bitmap   the block's sea mask, rows x cols, row-major, packed 8 cells to a byte with the FIRST cell in the
           most significant bit (np.packbits), the last byte padded with zero bits: bit set = sea cell
  payload  one xz stream of the uint16 planes [field, step, cell]; the cells are the bitmap's set bits in
           row-major order
One mask per grid: b"APM1" | u32le header length | header (JSON) | one zlib stream of the mask [nj, ni] packed the
same way.
"""
import json
import lzma
import struct
import zlib

import numpy as np

FORMAT = "apt1"
MISSING = 65535
TILE_MAGIC, MASK_MAGIC = b"APT1", b"APM1"
XZ_PRESET = 6
# What a decoder accepts at most (they run in the web server): a real tile's planes are 15 fields x 209 steps x
# 576 cells x 2 bytes = 3.6 MB and its header ~350 bytes; decoding xz preset 6 takes ~9 MB.
MAX_HEADER_BYTES = 4096
MAX_TILE_BYTES = 16 << 20
XZ_MEMLIMIT = 64 << 20

KINDS = {
    "height":    {"scale": 100, "units": "m"},
    "period":    {"scale": 10,  "units": "s"},
    "direction": {"scale": 1,   "units": "deg true, FROM"},
    "speed":     {"scale": 10,  "units": "m/s"},
}

# (name, kind, NOAA name, GRIB2 identity: discipline, parameter category, parameter number, type of first fixed
# surface, sequence number). Surface type 1 = the surface; 241 = "ordered sequence of data" (the swell partitions
# 1..3, ordered by NOAA by height at each step: partition n at one step is not the same swell train as n at the
# next. On the interpolated n25 grid the order does not always hold, and the wind sea can exceed the combined height).
FIELDS = (
    ("hs",   "height",    "HTSGW",   (10, 0, 3, 1, None)),      # significant height of combined wind waves and swell
    ("ws_h", "height",    "WVHGT",   (10, 0, 5, 1, None)),      # wind sea
    ("ws_t", "period",    "WVPER",   (10, 0, 6, 1, None)),
    ("ws_d", "direction", "WVDIR",   (10, 0, 4, 1, None)),
    ("s1_h", "height",    "SWELL 1", (10, 0, 8, 241, 1)),
    ("s1_t", "period",    "SWPER 1", (10, 0, 9, 241, 1)),
    ("s1_d", "direction", "SWDIR 1", (10, 0, 7, 241, 1)),
    ("s2_h", "height",    "SWELL 2", (10, 0, 8, 241, 2)),
    ("s2_t", "period",    "SWPER 2", (10, 0, 9, 241, 2)),
    ("s2_d", "direction", "SWDIR 2", (10, 0, 7, 241, 2)),
    ("s3_h", "height",    "SWELL 3", (10, 0, 8, 241, 3)),
    ("s3_t", "period",    "SWPER 3", (10, 0, 9, 241, 3)),
    ("s3_d", "direction", "SWDIR 3", (10, 0, 7, 241, 3)),
    ("wind", "speed",     "WIND",    (0, 2, 1, 1, None)),       # wind speed the wave model was forced with
    ("wdir", "direction", "WDIR",    (0, 2, 0, 1, None)),
)
FIELD_NAMES = tuple(f[0] for f in FIELDS)
FIELD_KIND = {f[0]: f[1] for f in FIELDS}
PARTITIONS = (("ws_h", "ws_t", "ws_d"), ("s1_h", "s1_t", "s1_d"), ("s2_h", "s2_t", "s2_d"), ("s3_h", "s3_t", "s3_d"))

GRIDS = (
    {"name": "g16", "tag": "global.0p16", "ni": 2160, "nj": 406, "lat0": 52.5,  "per_deg": 6, "rows": (0, 406),
     "data": (2, 390), "tile": 24},
    {"name": "s25", "tag": "gsouth.0p25", "ni": 1440, "nj": 277, "lat0": -10.5, "per_deg": 4, "rows": (9, 277),
     "data": (9, None), "tile": 16},
    {"name": "n25", "tag": "global.0p25", "ni": 1440, "nj": 721, "lat0": 90.0,  "per_deg": 4, "rows": (0, 152),
     "data": (None, 151), "tile": 16},
)
GRID_BY_NAME = {g["name"]: g for g in GRIDS}


def grid_lat(grid, row):
    return grid["lat0"] - row / grid["per_deg"]


def grid_lon(grid, col):
    """Longitude of a column in -180 <= lon < 180."""
    lon = (col % grid["ni"]) / grid["per_deg"]
    return lon - 360.0 if lon >= 180.0 else lon


TIE = 1e-6       # NOAA's files hold two decimals, so a tenth's ties (12.65 s) are common: they all go up


def quantise(values, kind):
    """float (NaN = missing) -> uint16 codes at the kind's scale, half up; a direction of 360 is 0. Give it the
    decoder's float64: through float32 a tie lands on either side."""
    v = np.asarray(values, dtype=np.float64)
    scale = KINDS[kind]["scale"]
    with np.errstate(invalid="ignore"):
        q = np.floor(v * scale + 0.5 + TIE)
        if kind == "direction":
            q = np.where(q >= 360 * scale, q - 360 * scale, q)
        q = np.clip(q, 0, MISSING - 1)
    return np.where(np.isfinite(v), q, MISSING).astype("<u2")


def dequantise(codes, kind):
    """uint16 codes -> float64, NaN where missing."""
    c = np.asarray(codes)
    return np.where(c == MISSING, np.nan, c.astype(np.float64) / KINDS[kind]["scale"])


def _pack(magic, header, *parts):
    h = json.dumps(header, separators=(",", ":"), sort_keys=True, allow_nan=False).encode("utf-8")
    return b"".join((magic, struct.pack("<I", len(h)), h) + parts)


def _no_constant(name):
    raise ValueError(f"{name} is not a number a header may hold")


def _unpack(blob, magic):
    """-> (header dict, offset of what follows). Anything else raises ValueError (never another exception)."""
    if len(blob) < 8 or blob[:4] != magic:
        raise ValueError(f"not a {magic.decode()} object")
    (n,) = struct.unpack("<I", blob[4:8])
    if n > MAX_HEADER_BYTES or 8 + n > len(blob):
        raise ValueError("bad header length")
    try:
        header = json.loads(bytes(blob[8:8 + n]).decode("utf-8"), parse_constant=_no_constant)
    except (ValueError, RecursionError) as exc:                 # bad UTF-8 and bad JSON are ValueErrors
        raise ValueError(f"bad header ({exc.__class__.__name__})") from None
    if not isinstance(header, dict) or header.get("format") != FORMAT:
        raise ValueError("unknown format")
    return header, 8 + n


def _int(header, key, lo, hi):
    """header[key] as a plain int in lo..hi (a float, a bool or a string is refused, not converted)."""
    v = header.get(key)
    if type(v) is not int or not lo <= v <= hi:
        raise ValueError(f"bad header: {key}")
    return v


def encode_tile(header, bitmap, planes, preset=XZ_PRESET):
    """header: the tile's own description (see tile_header); bitmap: bool [rows, cols]; planes: uint16
    [field, step, cell] with the cells in the bitmap's row-major order."""
    bitmap = np.asarray(bitmap, dtype=bool)
    planes = np.ascontiguousarray(planes, dtype="<u2")
    cells = int(bitmap.sum())
    if bitmap.shape != (header["rows"], header["cols"]) or cells != header["cells"] or cells == 0:
        raise ValueError("bitmap does not match the header")
    if planes.shape != (len(header["fields"]), header["steps"], cells):
        raise ValueError(f"planes {planes.shape} do not match the header")
    if planes.nbytes > MAX_TILE_BYTES:
        raise ValueError("tile too large for the format")
    return _pack(TILE_MAGIC, header, np.packbits(bitmap).tobytes(),
                 lzma.compress(planes.tobytes(), format=lzma.FORMAT_XZ, preset=preset))


def tile_header(run, grid, tr, tc, rows, cols, cells, steps):
    t = grid["tile"]
    return {"format": FORMAT, "run": run, "grid": grid["name"], "tile": [tr, tc], "row0": tr * t, "col0": tc * t,
            "rows": rows, "cols": cols, "cells": cells, "steps": steps, "fields": list(FIELD_NAMES)}


def decode_tile(blob, steps=None, fields=None):
    """-> (header, bitmap bool [rows, cols], planes uint16 [field, step, cell]). Raises ValueError on anything
    that is not a well-formed tile, and never unpacks more than MAX_TILE_BYTES (the claim is checked before the
    payload is touched, the payload against the claim). A reader passes the manifest's number of `steps` and its
    `fields`: a tile that holds anything else is refused."""
    header, pos = _unpack(blob, TILE_MAGIC)
    rows, cols = _int(header, "rows", 1, 64), _int(header, "cols", 1, 64)
    cells, nsteps = _int(header, "cells", 1, rows * cols), _int(header, "steps", 1, 1024)
    names = header.get("fields")
    if not isinstance(names, list) or not 0 < len(names) <= 64 or not all(isinstance(n, str) for n in names):
        raise ValueError("bad header: fields")
    if (steps is not None and nsteps != steps) or (fields is not None and names != list(fields)):
        raise ValueError("the tile does not hold the steps / fields asked for")
    steps, nf = nsteps, len(names)
    if nf * steps * cells * 2 > MAX_TILE_BYTES:
        raise ValueError("tile too large for the format")
    nb = (rows * cols + 7) // 8
    if pos + nb > len(blob):
        raise ValueError("truncated tile")
    bitmap = np.unpackbits(np.frombuffer(blob, np.uint8, nb, pos))[:rows * cols].astype(bool).reshape(rows, cols)
    if int(bitmap.sum()) != cells:
        raise ValueError("bitmap does not match the header")
    want = nf * steps * cells * 2
    d = lzma.LZMADecompressor(format=lzma.FORMAT_XZ, memlimit=XZ_MEMLIMIT)
    try:
        raw = d.decompress(blob[pos + nb:], max_length=want + 1)
    except lzma.LZMAError as exc:
        raise ValueError(f"bad tile payload: {exc}") from None
    if len(raw) != want or not d.eof or d.unused_data:
        raise ValueError("tile payload has the wrong size")
    return header, bitmap, np.frombuffer(raw, "<u2").reshape(nf, steps, cells)


def cell_index(bitmap, row, col):
    """Index on the planes' cell axis of the block's (row, col) (both relative to the block), or -1 for a cell
    that is not sea."""
    rows, cols = bitmap.shape
    if not (0 <= row < rows and 0 <= col < cols) or not bitmap[row, col]:
        return -1
    return int(np.count_nonzero(bitmap.ravel()[:row * cols + col]))


def encode_mask(run, grid, mask):
    mask = np.asarray(mask, dtype=bool)
    if mask.shape != (grid["nj"], grid["ni"]):
        raise ValueError("mask does not match the grid")
    header = {"format": FORMAT, "run": run, "grid": grid["name"], "ni": grid["ni"], "nj": grid["nj"],
              "cells": int(mask.sum())}
    return _pack(MASK_MAGIC, header, zlib.compress(np.packbits(mask).tobytes(), 9))


def decode_mask(blob):
    """-> (header, mask bool [nj, ni])."""
    header, pos = _unpack(blob, MASK_MAGIC)
    ni, nj = _int(header, "ni", 1, 8192), _int(header, "nj", 1, 4096)
    cells = _int(header, "cells", 0, ni * nj)
    nb = (ni * nj + 7) // 8
    d = zlib.decompressobj()
    try:
        raw = d.decompress(blob[pos:], nb + 1)
    except zlib.error as exc:
        raise ValueError(f"bad mask payload: {exc}") from None
    if len(raw) != nb or not d.eof or d.unused_data:
        raise ValueError("mask payload has the wrong size")
    mask = np.unpackbits(np.frombuffer(raw, np.uint8))[:ni * nj].astype(bool).reshape(nj, ni)
    if int(mask.sum()) != cells:
        raise ValueError("mask does not match the header")
    return header, mask


def layout(mask, tile):
    """The sea cells of a grid in TILE-MAJOR order (tile row, tile column, then row-major inside the tile).
    -> (cells: flat indices into [nj, ni]; tiles: int array [n, 4] of (tile row, tile column, start, count)),
    so every tile is one run of the cell axis and its cells are in its bitmap's order."""
    mask = np.asarray(mask, dtype=bool)
    nj, ni = mask.shape
    flat = np.flatnonzero(mask)                                   # ascending = row-major
    ntc = -(-ni // tile)
    tid = (flat // ni) // tile * ntc + (flat % ni) // tile
    order = np.argsort(tid, kind="stable")
    cells, tid = flat[order], tid[order]
    ids, start, count = np.unique(tid, return_index=True, return_counts=True)
    tiles = np.stack([ids // ntc, ids % ntc, start, count], axis=1).astype(np.int64) if ids.size else np.zeros((0, 4), np.int64)
    return cells, tiles


def tile_bitmap(mask, tile, tr, tc):
    """The block's part of the grid mask (clipped at the grid's last row / column)."""
    return mask[tr * tile:(tr + 1) * tile, tc * tile:(tc + 1) * tile]
