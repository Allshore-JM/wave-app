"""Quantise a float grid to the 8-bit frame format the browser decodes.

Frame format (documented in manifest.json as encoding "u8-linear-v1"):
  value 0          -> missing (land / no data); rendered transparent
  value q in 1..255 -> lo + (q - 1) / 254 * (hi - lo)   (values above hi clamp to 255)
Stored as 8-bit greyscale PNG. Half-resolution variant = nearest-neighbour 720x361.
"""
import io

import numpy as np
from PIL import Image

FIELDS = {
    # name: (range lo, hi, units, interpolation hint for the client)
    "hs":   (0.0, 12.0, "m", "bilinear"),
    "tp":   (4.0, 22.0, "s", "nearest"),
    "wind": (0.0, 30.868, "m/s", "bilinear"),   # 60 kt
}
ENCODING = "u8-linear-v1"


def quantize(grid, lo, hi):
    q = np.clip(np.round((grid - lo) / (hi - lo) * 254.0) + 1.0, 1, 255)
    return np.where(np.isnan(grid), 0, q).astype(np.uint8)


def dequantize(q, lo, hi):
    out = lo + (q.astype(np.float64) - 1.0) / 254.0 * (hi - lo)
    return np.where(q == 0, np.nan, out)


def quantum(lo, hi):
    return (hi - lo) / 254.0


def to_png(q, size=None):
    im = Image.fromarray(q, "L")
    if size:
        im = im.resize(size, Image.NEAREST)
    buf = io.BytesIO()
    im.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def encode_frame(grid, name):
    """-> {'full': png bytes, 'half': png bytes, 'stats': {...}} for one field."""
    lo, hi, units, _ = FIELDS[name]
    q = quantize(grid, lo, hi)
    valid = ~np.isnan(grid)
    return {
        "full": to_png(q),
        "half": to_png(q, (720, 361)),
        "stats": {
            "min": float(np.nanmin(grid)) if valid.any() else None,
            "max": float(np.nanmax(grid)) if valid.any() else None,
            "valid_points": int(valid.sum()),
            "clamped_points": int(np.count_nonzero(grid > hi)),
        },
    }
