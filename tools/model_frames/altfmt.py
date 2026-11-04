"""The satellite chunk format (plan sections 45 and 81): one binary object per 3-hour window of readings.

Layout (little-endian throughout):
  magic (4 bytes: "ALT1" for the altimeter wave heights, "SWL1" for the Sentinel-1 swell partitions)
  u32   header length
  header JSON (UTF-8): {format, window, t0, t1, build, n, columns: [[name, dtype, scale, none], ...], missions,
                        segments: [[mission index, start, count], ...], ...}
  ONE zlib stream (RFC 1950: the browser's DecompressionStream('deflate')) of the columns concatenated, each column
  n values of its dtype, in the header's column order.

Column dtypes: u1 u2 u4 i4 (numpy codes). A column's `scale` turns the stored integer into its value (lat i4 x 1e-6,
swh u2 x 0.001 m); `none` is the stored integer that means "no value" (null when every value is present).
Readings are grouped in segments, one per mission, time-sorted (non-decreasing) inside each segment; the segments
cover the n readings exactly, in order, with no overlap.

The columns of the two formats live here so the jobs and the tests share one definition; the client decodes the
header's `columns` (and checks them against its own copy).
"""
import json
import struct
import zlib

import numpy as np

MAGIC_ALT = b"ALT1"
MAGIC_SWELL = b"SWL1"
EPOCH = "2000-01-01T00:00:00Z"                 # t = seconds since this (u4: good to 2136)
MAX_HEADER_BYTES = 1 << 20                     # a header is a few KB (the inputs list); nothing larger is read
MAX_CHUNK_BYTES = 2 * 1024 * 1024              # the job refuses to publish a larger chunk; the client refuses to read one
WARN_CHUNK_BYTES = 1024 * 1024
MAX_READINGS = 600_000                         # per chunk (a 3-h window holds ~44k altimeter readings)
SWELL_PARTS = 3                                # swell partition slots per point

NONE_U2 = 65535
NONE_U1 = 255
# name, dtype, scale, none
ALT_COLUMNS = [
    ["t", "u4", 1, None],                      # seconds since EPOCH
    ["lat", "i4", 1e-6, None],                 # degrees north
    ["lon", "i4", 1e-6, None],                 # degrees east, -180..180
    ["swh", "u2", 0.001, NONE_U2],             # significant wave height, m
    ["model", "u2", 0.001, NONE_U2],           # the collocated model wave height, m
    ["mission", "u1", 1, None],                # index into the header's missions
    ["run", "u1", 1, NONE_U1],                 # index into the header's runs (the model run the value came from)
    ["flags", "u1", 1, None],                  # FLAG_NEAR_LAND | FLAG_MODEL_EDGE
]
FLAG_NEAR_LAND = 1
FLAG_MODEL_EDGE = 2
SWELL_COLUMNS = [
    ["t", "u4", 1, None],
    ["lat", "i4", 1e-6, None],
    ["lon", "i4", 1e-6, None],
    ["n", "u1", 1, None],                      # partitions present (0..SWELL_PARTS), the dominant first
    ["mission", "u1", 1, None],
    ["flags", "u1", 1, None],
] + [[f"hs{k}", "u2", 0.001, NONE_U2] for k in range(1, SWELL_PARTS + 1)] \
  + [[f"tp{k}", "u2", 0.01, NONE_U2] for k in range(1, SWELL_PARTS + 1)] \
  + [[f"dir{k}", "u2", 0.01, NONE_U2] for k in range(1, SWELL_PARTS + 1)]   # degrees true the swell comes FROM
FORMATS = {MAGIC_ALT: ("alt-v1", ALT_COLUMNS), MAGIC_SWELL: ("swell-v1", SWELL_COLUMNS)}
_DTYPES = {"u1": np.uint8, "u2": np.uint16, "u4": np.uint32, "i4": np.int32}
_LIMITS = {"t": (0, 2 ** 32 - 1), "lat": (-90_000_000, 90_000_000), "lon": (-180_000_000, 180_000_000)}


def dtype_of(code):
    if code not in _DTYPES:
        raise ValueError(f"unknown column dtype {code!r}")
    return np.dtype(_DTYPES[code]).newbyteorder("<")


def stride_of(columns):
    return sum(dtype_of(c[1]).itemsize for c in columns)


def to_stored(value, scale, none, dtype):
    """Values (float, NaN = none) -> the stored integers of a column (rounded)."""
    v = np.asarray(value, dtype=np.float64)
    out = np.rint(v / scale)
    info = np.iinfo(dtype_of(dtype))
    if none is None:
        if np.isnan(v).any():
            raise ValueError("a column without a none value holds NaN")
    else:
        out = np.where(np.isnan(v), none, out)
    if ((out < info.min) | (out > info.max)).any():
        raise ValueError("a value does not fit its column")
    return out.astype(dtype_of(dtype))


def from_stored(arr, scale, none):
    """Stored integers -> float64 values, NaN where `none`."""
    out = arr.astype(np.float64) * scale
    if none is not None:
        out = np.where(arr == none, np.nan, out)
    return out


def encode_chunk(magic, header, columns, arrays, level=6):
    """-> the chunk bytes. `arrays` maps column name -> numpy array of the column's dtype (or anything astype takes),
    all of one length n; the header gets format / columns / n filled in. Segments are checked: they cover 0..n in
    order, each time-sorted."""
    if magic not in FORMATS:
        raise ValueError("unknown chunk magic")
    fmt, cols = FORMATS[magic]
    n = None
    parts = []
    for name, code, _scale, _none in cols:
        src = np.asarray(arrays[name])
        dt = dtype_of(code)
        if src.dtype.kind not in "iu":
            raise ValueError(f"column {name}: stored integers expected (use to_stored)")
        info = np.iinfo(dt)
        if src.size and ((src < info.min) | (src > info.max)).any():
            raise ValueError(f"column {name}: a value does not fit {code}")
        a = np.ascontiguousarray(src.astype(dt))
        if n is None:
            n = a.shape[0]
        if a.ndim != 1 or a.shape[0] != n:
            raise ValueError(f"column {name}: shape {a.shape} is not ({n},)")
        parts.append(a.tobytes())
    if n is None:
        n = 0
    if n > MAX_READINGS:
        raise ValueError(f"{n} readings exceed the chunk limit")
    segs = header.get("segments") or []
    check_segments(segs, n, np.frombuffer(parts[0], dtype=dtype_of(cols[0][1])) if n else None)
    body = zlib.compress(b"".join(parts), level)
    head = dict(header, format=fmt, n=n, columns=[list(c) for c in cols], epoch=EPOCH)
    hjson = json.dumps(head, separators=(",", ":"), sort_keys=True, allow_nan=False).encode("utf-8")
    if len(hjson) > MAX_HEADER_BYTES:
        raise ValueError("chunk header too large")
    out = magic + struct.pack("<I", len(hjson)) + hjson + body
    if len(out) > MAX_CHUNK_BYTES:
        raise ValueError(f"chunk of {len(out)} bytes exceeds {MAX_CHUNK_BYTES}")
    return out


def check_segments(segs, n, t):
    """Segments [[mission, start, count], ...] cover readings 0..n-1 in order, each with non-decreasing times."""
    pos = 0
    for s in segs:
        if not (isinstance(s, (list, tuple)) and len(s) == 3 and all(isinstance(x, int) and x >= 0 for x in s)):
            raise ValueError("bad segment")
        m, start, count = s
        if start != pos or count < 1 or m > 254:
            raise ValueError("segments must be contiguous, in order, non-empty")
        if t is not None and count > 1 and (np.diff(t[start:start + count].astype(np.int64)) < 0).any():
            raise ValueError("a segment is not time-sorted")
        pos += count
    if pos != n:
        raise ValueError("segments do not cover the readings")


def decode_chunk(buf, magic=None, max_bytes=MAX_CHUNK_BYTES):
    """-> (header, {name: numpy array of stored integers}) after every check the client makes (magic, lengths, the
    column list against this module's, ranges, segments). `magic` pins the format expected."""
    if not isinstance(buf, (bytes, bytearray, memoryview)):
        raise ValueError("chunk is not bytes")
    buf = bytes(buf)
    if len(buf) > max_bytes:
        raise ValueError("chunk too large")
    if len(buf) < 8:
        raise ValueError("chunk truncated")
    mg = buf[:4]
    if mg not in FORMATS or (magic is not None and mg != magic):
        raise ValueError("bad chunk magic")
    fmt, cols = FORMATS[mg]
    hlen = struct.unpack("<I", buf[4:8])[0]
    if hlen > MAX_HEADER_BYTES or 8 + hlen > len(buf):
        raise ValueError("bad chunk header length")
    try:
        header = json.loads(buf[8:8 + hlen].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        raise ValueError("bad chunk header")
    if not isinstance(header, dict) or header.get("format") != fmt or header.get("columns") != [list(c) for c in cols]:
        raise ValueError("chunk header does not match the format")
    n = header.get("n")
    if not isinstance(n, int) or n < 0 or n > MAX_READINGS:
        raise ValueError("bad reading count")
    stride = stride_of(cols)
    try:
        d = zlib.decompressobj()
        raw = d.decompress(buf[8 + hlen:], n * stride + 1)             # bounded: never more than the header's n + 1 byte
        if d.unconsumed_tail or not d.eof:
            raise ValueError("chunk stream length does not match the header")
    except zlib.error:
        raise ValueError("bad chunk stream")
    if len(raw) != n * stride:
        raise ValueError("chunk stream length does not match the header")
    out, pos = {}, 0
    for name, code, _scale, _none in cols:
        dt = dtype_of(code)
        size = n * dt.itemsize
        out[name] = np.frombuffer(raw, dtype=dt, count=n, offset=pos).copy()
        pos += size
        lim = _LIMITS.get(name)
        if lim and n and ((out[name] < lim[0]) | (out[name] > lim[1])).any():
            raise ValueError(f"column {name} out of range")
    segs = header.get("segments")
    if not isinstance(segs, list):
        raise ValueError("chunk header has no segments")
    check_segments(segs, n, out[cols[0][0]] if n else None)
    missions = header.get("missions")
    if not isinstance(missions, list) or (n and not missions):
        raise ValueError("chunk header has no missions")
    if n and (out["mission"] >= len(missions)).any():
        raise ValueError("a reading names a mission the header does not list")
    for m, start, count in segs:
        if (out["mission"][start:start + count] != m).any():
            raise ValueError("a segment's readings name another mission")
    if mg == MAGIC_ALT:
        runs = header.get("runs")
        if not isinstance(runs, list):
            raise ValueError("chunk header has no runs")
        if n and ((out["run"] != NONE_U1) & (out["run"] >= len(runs))).any():
            raise ValueError("a reading names a run the header does not list")
        if n and (out["flags"] & np.uint8(255 ^ (FLAG_NEAR_LAND | FLAG_MODEL_EDGE))).any():
            raise ValueError("unknown flag bits")
    else:
        if n and ((out["n"] > SWELL_PARTS) | (out["flags"] & np.uint8(255 ^ FLAG_NEAR_LAND)).astype(bool)).any():
            raise ValueError("bad partition count or flag bits")
        for k in range(1, SWELL_PARTS + 1):
            dk = out[f"dir{k}"]
            if n and ((dk != NONE_U2) & (dk > 36000)).any():
                raise ValueError("a direction beyond 360 degrees")
    return header, out


def values(header, cols):
    """The decoded columns as floats (NaN = none), by the header's column spec."""
    out = {}
    for name, _code, scale, none in header["columns"]:
        out[name] = from_stored(cols[name], scale, none)
    return out
