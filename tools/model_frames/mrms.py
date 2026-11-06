"""NOAA MRMS (Multi-Radar/Multi-Sensor) reflectivity files for the radar job (plan section 85): one small GRIB2 reader.

AWS Open Data bucket noaa-mrms-pds, anonymous:
  {DOMAIN}/SeamlessHSR_00.00/{YYYYMMDD}/MRMS_SeamlessHSR_00.00_{YYYYMMDD-HHMMSS}.grib2.gz
one file every 2 minutes on the even minute in every domain, 33-56 s later (checked 2026-11-05/06). Each is one GRIB2 message:
discipline 209 (NSSL's local table), category 8, number 8; grid template 0, north to south, longitudes 0..360; PNG packing
(data representation template 41), 16 bits, no bitmap. Values: -999 = no radar coverage, -99 = no echo, else dBZ (down to about
-27). The reader is pure Python + Pillow because the job's environment (the SST job's: h5py, pillow, numpy) has no eccodes; a
message of any other shape is a ValueError, never a guess.
"""
import gzip
import io
import struct

import numpy as np
from PIL import Image

BUCKET_URL = "https://noaa-mrms-pds.s3.amazonaws.com"
PRODUCT = "SeamlessHSR_00.00"
IDENTITY = (209, 8, 8)                  # discipline, category, number of SeamlessHSR in NSSL's table
NO_COVERAGE, NO_ECHO = -999.0, -99.0
MAX_GZ_BYTES = 16_000_000               # a real file is 12 KB .. 1.6 MB
MAX_RAW_BYTES = 64_000_000


def url_of(domain, when):
    """The file of `domain` at the even-minute datetime `when`."""
    return (f"{BUCKET_URL}/{domain}/{PRODUCT}/{when:%Y%m%d}/"
            f"MRMS_{PRODUCT}_{when:%Y%m%d-%H%M%S}.grib2.gz")


def gunzip(data):
    """The GRIB2 bytes of a .gz body; a truncated or oversized body is a ValueError."""
    if len(data) > MAX_GZ_BYTES:
        raise ValueError(f"{len(data)} gzip bytes are too many")
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as g:
            raw = g.read(MAX_RAW_BYTES + 1)
    except (OSError, EOFError) as exc:
        raise ValueError(f"not a whole gzip file: {exc}") from None
    if len(raw) > MAX_RAW_BYTES:
        raise ValueError("the GRIB message is too large")
    return raw


def sections(raw):
    """{section number: its bytes} of a single-message GRIB2 file; the discipline is sections[0][6]."""
    if len(raw) < 20 or raw[:4] != b"GRIB" or raw[7] != 2:
        raise ValueError("not a GRIB2 message")
    total = struct.unpack(">Q", raw[8:16])[0]
    if total != len(raw) or raw[total - 4:total] != b"7777":
        raise ValueError(f"the message says {total} bytes, the file holds {len(raw)} (or no end marker)")
    out, p = {0: raw[:16]}, 16
    while p < total - 4:
        if p + 5 > total - 4:
            raise ValueError("a section runs past the end")
        n, num = struct.unpack(">IB", raw[p:p + 5])
        if n < 5 or p + n > total - 4 or num not in (1, 2, 3, 4, 5, 6, 7):
            raise ValueError(f"section {num} of {n} bytes at {p} is not possible")
        if num in out:
            raise ValueError("more than one field in the file")
        out[num] = raw[p:p + n]
        p += n
    for need in (1, 3, 4, 5, 6, 7):
        if need not in out:
            raise ValueError(f"section {need} is missing")
    return out


def read(raw):
    """A GRIB2 message -> {ni, nj, lat1, lon1 (-180..180), lat2, lon2, di, dj, ref, binary_scale, decimal_scale, x (uint16
    array nj x ni, row 0 = lat1)}. Anything but the verified layout is a ValueError."""
    s = sections(raw)
    s3, s4, s5, s6, s7 = s[3], s[4], s[5], s[6], s[7]
    if len(s3) < 72 or struct.unpack(">H", s3[12:14])[0] != 0:
        raise ValueError("the grid is not latitude / longitude (template 0)")
    if len(s4) < 11 or len(s5) < 21 or len(s6) < 6 or len(s7) < 6:
        raise ValueError("a section is too short")
    ident = (s[0][6], s4[9], s4[10])
    if ident != IDENTITY:
        raise ValueError(f"parameter {ident} is not SeamlessHSR {IDENTITY}")
    ni, nj = struct.unpack(">II", s3[30:38])
    lat1, lon1 = struct.unpack(">iI", s3[46:54])
    lat2, lon2 = struct.unpack(">iI", s3[55:63])
    di, dj = struct.unpack(">II", s3[63:71])
    scan = s3[71]
    if scan != 0:
        raise ValueError(f"scanning mode {scan} is not north to south, west to east")
    if struct.unpack(">H", s5[9:11])[0] != 41 or s5[19] != 16:
        raise ValueError("the packing is not 16-bit PNG (template 41)")
    if s6[5] != 255:
        raise ValueError("the message carries a bitmap")
    ref = struct.unpack(">f", s5[11:15])[0]
    bscale, dscale = struct.unpack(">HH", s5[15:19])
    bscale = -(bscale & 0x7FFF) if bscale & 0x8000 else bscale         # GRIB's sign-and-magnitude: the top bit is the sign
    dscale = -(dscale & 0x7FFF) if dscale & 0x8000 else dscale
    try:
        with Image.open(io.BytesIO(s7[5:])) as im:
            if im.format != "PNG" or im.mode not in ("I;16", "I;16B"):
                raise ValueError(f"a {im.format} {im.mode} picture")
            if im.size != (ni, nj):
                raise ValueError(f"picture {im.size} for grid {ni} x {nj}")
            x = np.array(im, dtype=np.uint16)
    except (OSError, SyntaxError) as exc:                              # Pillow's words for a damaged picture
        raise ValueError(f"the PNG cannot be read: {exc}") from None
    return {"ni": ni, "nj": nj, "lat1": lat1 / 1e6, "lon1": _lon(lon1 / 1e6), "lat2": lat2 / 1e6, "lon2": _lon(lon2 / 1e6),
            "di": di / 1e6, "dj": dj / 1e6, "ref": float(ref), "binary_scale": bscale, "decimal_scale": dscale, "x": x}


def _lon(deg):
    return deg - 360.0 if deg > 180.0 else deg


def value_table(msg):
    """dBZ of every 16-bit packed value: (ref + x 2^E) / 10^D, float32; the sentinels exact within a rounding."""
    x = np.arange(65536, dtype=np.float64)
    return ((msg["ref"] + x * 2.0 ** msg["binary_scale"]) / 10.0 ** msg["decimal_scale"]).astype(np.float32)
