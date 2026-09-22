"""Quantise a float grid to the 8-bit frame format the browser decodes.

Frame format, encoding "u8-linear-v2" (machine-readable copy in the manifest's encoding_spec):
  q = 0            -> missing (land / no data); rendered transparent; a bilinear sampler must
                      treat it as an ABSENT neighbour, never as lo
  q in 1..255      -> value = lo + (q - 1) / 254 * (hi - lo)
  q = 1  also means "<= lo" (clamped low), q = 255 also means ">= hi" (clamped high); the
  manifest carries clamped_low/high counts per frame and the readout must print "<= lo" / ">= hi".
Encoding ranges are WIDER than the legend ranges the owner chose, so no ocean value is silently
rewritten (G1: 5.5 % of ocean points have Tp below the 4 s legend floor). Legends stay
Hs 0-12 m, Tp 4-22 s, wind 0-60 kt on the client.

Stored as 8-bit greyscale PNG (1440x721). Half-resolution variant = exact subsample q[::2, ::2]
(361x720): half pixel (i, j) IS full pixel (2i, 2j) -> lat 90 - 0.5 i, lon -180 + 0.5 j.
"""
import io

import numpy as np
from PIL import Image

KT = 1852.0 / 3600.0                    # 1 knot in m/s

FIELDS = {
    # name: dict(lo, hi = ENCODING range; legend = display range; units; interpolation hint)
    "hs":   {"lo": 0.0, "hi": 15.0,     "legend": [0.0, 12.0],    "units": "m",   "interpolation": "bilinear"},
    "tp":   {"lo": 1.0, "hi": 30.0,     "legend": [4.0, 22.0],    "units": "s",   "interpolation": "nearest"},
    "wind": {"lo": 0.0, "hi": 80 * KT,  "legend": [0.0, 60 * KT], "units": "m/s", "interpolation": "bilinear"},
}
ENCODING = "u8-linear-v2"
ENCODING_SPEC = {
    "missing": 0, "min_code": 1, "max_code": 255,
    "formula": "value = lo + (q - 1) / 254 * (hi - lo)",
    "clamp": "both", "low_code_means": "<= lo", "high_code_means": ">= hi",
    "bilinear_missing": "treat q=0 as absent neighbour",
}


def quantize(grid, lo, hi):
    g = np.asarray(grid, dtype=np.float64)
    q = np.clip(np.round((g - lo) / (hi - lo) * 254.0) + 1.0, 1, 255)
    return np.where(np.isnan(g), 0, q).astype(np.uint8)


def dequantize(q, lo, hi):
    out = lo + (q.astype(np.float64) - 1.0) / 254.0 * (hi - lo)
    return np.where(q == 0, np.nan, out)


def quantum(lo, hi):
    return (hi - lo) / 254.0


def half_res(q):
    """Exact 2x subsample keeping the (+90 N, -180 E) origin: shape (361, 720)."""
    return np.ascontiguousarray(q[::2, ::2])


def to_png(q):
    im = Image.fromarray(q, "L")
    buf = io.BytesIO()
    im.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def encode_frame(grid, name):
    """-> {'full': png bytes, 'half': png bytes, 'stats': {...}} for one field."""
    f = FIELDS[name]
    lo, hi = f["lo"], f["hi"]
    g = np.asarray(grid, dtype=np.float64)
    q = quantize(g, lo, hi)
    valid = ~np.isnan(g)
    return {
        "full": to_png(q),
        "half": to_png(half_res(q)),
        "stats": {
            "min": round(float(np.nanmin(g)), 4) if valid.any() else None,
            "max": round(float(np.nanmax(g)), 4) if valid.any() else None,
            "valid_points": int(valid.sum()),
            "clamped_low": int(np.count_nonzero(g < lo)),
            "clamped_high": int(np.count_nonzero(g > hi)),
        },
    }
