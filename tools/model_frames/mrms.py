"""NOAA MRMS (Multi-Radar/Multi-Sensor) reflectivity files for the radar job (plan section 85): one small GRIB2 reader.

AWS Open Data bucket noaa-mrms-pds, anonymous:
  {DOMAIN}/SeamlessHSR_00.00/{YYYYMMDD}/MRMS_SeamlessHSR_00.00_{YYYYMMDD-HHMMSS}.grib2.gz
one file every 2 minutes on the even minute in every domain, 13-96 s later (CONUS the latest, median 59 s; checked 2026-11-06). Each is one GRIB2 message:
discipline 209 (NSSL's local table), category 8, number 8; grid template 0, north to south, longitudes 0..360; PNG packing
(data representation template 41), 16 bits, no bitmap. Values: -999 = no radar coverage, -99 = no echo, else dBZ (down to about
-27). The reader is pure Python + Pillow because the job's environment (the SST job's: h5py, pillow, numpy) has no eccodes; a
message of any other shape is a ValueError, never a guess.

Precipitation type (plan section 89): PrecipFlag_00.00 (209, 6, 0; 8-bit PNG; -3 no coverage, 0 none, 1 warm stratiform rain, 3 snow,
6 convective rain, 7 rain with hail, 10 cool stratiform rain, 91 / 96 tropical rain) at the reflectivity file's own minute, and the
model's wet-bulb temperature Model_WetBulbTemp_00.50 (209, 7, 1; degrees C in hundredths; -999 missing), hourly (Guam every 6 h),
the hour HH out about HH+1:25. Both on the reflectivity's grid in every domain (checked 2026-11-08). Their PNGs are 8, 16 or 24 bits
(the file's range decides: CONUS's wet-bulb is 24-bit RGB).
"""
import gzip
import io
import struct

import numpy as np
from PIL import Image

BUCKET_URL = "https://noaa-mrms-pds.s3.amazonaws.com"
PRODUCT = "SeamlessHSR_00.00"
FLAG_PRODUCT = "PrecipFlag_00.00"
TW_PRODUCT = "Model_WetBulbTemp_00.50"
IDENTITY = (209, 8, 8)                  # discipline, category, number of SeamlessHSR in NSSL's table
FLAG_IDENTITY = (209, 6, 0)
TW_IDENTITY = (209, 7, 1)
MODES = {8: ("L",), 16: ("I;16", "I;16B"), 24: ("RGB",)}             # the PNG Pillow reads for each bit depth
NO_COVERAGE, NO_ECHO = -999.0, -99.0
MAX_GZ_BYTES = 16_000_000               # a real file is 12 KB .. 1.6 MB
TW_MAX_GZ_BYTES = 48_000_000            # the wet-bulb file: CONUS 15.6 MB
MAX_RAW_BYTES = 64_000_000


def url_of(domain, when, product=PRODUCT):
    """The file of `domain` at the datetime `when` (an even minute; the wet-bulb's whole hours)."""
    return (f"{BUCKET_URL}/{domain}/{product}/{when:%Y%m%d}/"
            f"MRMS_{product}_{when:%Y%m%d-%H%M%S}.grib2.gz")


def gunzip(data, max_gz=None):
    """The GRIB2 bytes of a .gz body (at most max_gz bytes, else MAX_GZ_BYTES); a truncated or oversized body is a ValueError."""
    max_gz = MAX_GZ_BYTES if max_gz is None else max_gz
    if len(data) > max_gz:
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


def read(raw, identity=IDENTITY, bits=(16,)):
    """A GRIB2 message -> {ni, nj, lat1, lon1 (-180..180), lat2, lon2, di, dj, ref, binary_scale, decimal_scale, bits, x (array
    nj x ni, row 0 = lat1: uint8, uint16 or uint32 by the bit depth)}. Anything but the verified layout of `identity` with one of the
    bit depths `bits` is a ValueError."""
    s = sections(raw)
    s3, s4, s5, s6, s7 = s[3], s[4], s[5], s[6], s[7]
    if len(s3) < 72 or struct.unpack(">H", s3[12:14])[0] != 0:
        raise ValueError("the grid is not latitude / longitude (template 0)")
    if len(s4) < 11 or len(s5) < 21 or len(s6) < 6 or len(s7) < 6:
        raise ValueError("a section is too short")
    ident = (s[0][6], s4[9], s4[10])
    if ident != tuple(identity):
        raise ValueError(f"parameter {ident} is not the expected {tuple(identity)}")
    ni, nj = struct.unpack(">II", s3[30:38])
    lat1, lon1 = struct.unpack(">iI", s3[46:54])
    lat2, lon2 = struct.unpack(">iI", s3[55:63])
    di, dj = struct.unpack(">II", s3[63:71])
    scan = s3[71]
    if scan != 0:
        raise ValueError(f"scanning mode {scan} is not north to south, west to east")
    nbits = s5[19]
    if struct.unpack(">H", s5[9:11])[0] != 41 or nbits not in bits or nbits not in MODES:
        raise ValueError(f"the packing is not {'/'.join(str(b) for b in bits)}-bit PNG (template 41)")
    if s6[5] != 255:
        raise ValueError("the message carries a bitmap")
    ref = struct.unpack(">f", s5[11:15])[0]
    bscale, dscale = struct.unpack(">HH", s5[15:19])
    bscale = -(bscale & 0x7FFF) if bscale & 0x8000 else bscale         # GRIB's sign-and-magnitude: the top bit is the sign
    dscale = -(dscale & 0x7FFF) if dscale & 0x8000 else dscale
    try:
        with Image.open(io.BytesIO(s7[5:])) as im:
            if im.format != "PNG" or im.mode not in MODES[nbits]:
                raise ValueError(f"a {im.format} {im.mode} picture for {nbits} bits")
            if im.size != (ni, nj):
                raise ValueError(f"picture {im.size} for grid {ni} x {nj}")
            if nbits == 24:
                a = np.array(im, dtype=np.uint8)
                x = (a[..., 0].astype(np.uint32) << 16) | (a[..., 1].astype(np.uint32) << 8) | a[..., 2].astype(np.uint32)
            else:
                x = np.array(im, dtype=np.uint8 if nbits == 8 else np.uint16)
    except (OSError, SyntaxError, Image.DecompressionBombError) as exc:    # Pillow's words for a damaged picture
        raise ValueError(f"the PNG cannot be read: {exc}") from None
    return {"ni": ni, "nj": nj, "lat1": lat1 / 1e6, "lon1": _lon(lon1 / 1e6), "lat2": lat2 / 1e6, "lon2": _lon(lon2 / 1e6),
            "di": di / 1e6, "dj": dj / 1e6, "ref": float(ref), "binary_scale": bscale, "decimal_scale": dscale, "bits": nbits, "x": x}


def _lon(deg):
    return deg - 360.0 if deg > 180.0 else deg


def value_table(msg):
    """The value of every packed value of an 8- or 16-bit message: (ref + x 2^E) / 10^D, float32; the sentinels exact within a
    rounding."""
    if msg.get("bits", 16) > 16:
        raise ValueError("a table needs an 8- or 16-bit message")
    x = np.arange(1 << msg.get("bits", 16), dtype=np.float64)
    return ((msg["ref"] + x * 2.0 ** msg["binary_scale"]) / 10.0 ** msg["decimal_scale"]).astype(np.float32)


def values(msg):
    """Every point's value, float32 (any bit depth; a 24-bit value is exact in float32)."""
    if msg["bits"] <= 16:
        return value_table(msg)[msg["x"]]
    x = msg["x"].astype(np.float64)
    return ((msg["ref"] + x * 2.0 ** msg["binary_scale"]) / 10.0 ** msg["decimal_scale"]).astype(np.float32)


def same_grid(a, b):
    """Two messages on one grid (the type files must lie on the reflectivity's own)."""
    return all(a[k] == b[k] for k in ("ni", "nj")) and all(abs(a[k] - b[k]) < 1e-6 for k in ("lat1", "lon1", "di", "dj"))
