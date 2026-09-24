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

Coastal fill (wave height and peak period only; manifest "fill" = FILL_INFO): GFS-Wave marks every
0.25-degree cell that touches land as missing, so the field would stop ~20 km short of the coast.
Before quantisation, missing cells within FILL_CELLS cells of real data get the mean of their
present 8-neighbours, one Jacobi pass per ring (longitude periodic, nothing beyond the poles);
real values are never changed and deep inland stays missing. The browser clips the result to the
GSHHG coastline, so the fill is only ever seen over water. 4 passes is the smallest count for
which the half-resolution grid (every 2nd cell) still reaches every coastline pixel.
"""
import io

import numpy as np
from PIL import Image

KT = 1852.0 / 3600.0                    # 1 knot in m/s

FIELDS = {
    # name: dict(lo, hi = ENCODING range; legend = display range; units; interpolation hint)
    "hs":   {"lo": 0.0, "hi": 15.0,     "legend": [0.0, 12.0],    "units": "m",   "interpolation": "bilinear", "fill": True},
    "tp":   {"lo": 1.0, "hi": 30.0,     "legend": [4.0, 22.0],    "units": "s",   "interpolation": "nearest",  "fill": True},
    "wind": {"lo": 0.0, "hi": 80 * KT,  "legend": [0.0, 60 * KT], "units": "m/s", "interpolation": "bilinear", "fill": False},
}
FILL_CELLS = 4
FILL_INFO = {
    "fields": sorted(n for n, f in FIELDS.items() if f["fill"]), "cells": FILL_CELLS,
    "method": "mean of present 8-neighbours, one Jacobi pass per ring, longitude periodic",
    "note": "nearshore values are extrapolated from the nearest model cells; clipped to the coastline in the browser",
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


def fill_coast(grid, cells=FILL_CELLS):
    """-> (filled float64 copy, bool mask of the cells that were filled). Only NaN cells change."""
    g = np.array(grid, dtype=np.float64)                    # a copy: the caller's grid is untouched
    missing0 = np.isnan(g)
    rows, cols = g.shape
    p = np.full((rows + 2, cols + 2), np.nan)
    for _ in range(cells):
        p[1:-1, 1:-1] = g
        p[1:-1, 0] = g[:, -1]                               # longitude is periodic
        p[1:-1, -1] = g[:, 0]                               # (rows 0 and -1 stay NaN: nothing beyond the poles)
        tot = np.zeros((rows, cols))
        n = np.zeros((rows, cols), np.int16)
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                if dy == 0 and dx == 0:
                    continue
                s = p[1 + dy:rows + 1 + dy, 1 + dx:cols + 1 + dx]
                ok = ~np.isnan(s)
                tot += np.where(ok, s, 0.0)
                n += ok
        target = np.isnan(g) & (n > 0)
        if not target.any():
            break
        g[target] = tot[target] / n[target]                 # Jacobi: every mean uses the previous pass
    return g, missing0 & ~np.isnan(g)


def half_res(q):
    """Exact 2x subsample keeping the (+90 N, -180 E) origin: shape (361, 720)."""
    return np.ascontiguousarray(q[::2, ::2])


def to_png(q):
    im = Image.fromarray(q, "L")
    buf = io.BytesIO()
    im.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def encode_frame(grid, name, fill=None):
    """-> {'full': png bytes, 'half': png bytes, 'stats': {...}} for one field. The coastal fill
    (default: FIELDS[name]["fill"]) runs before quantisation and before the half subsample; the
    stats describe the MODEL values only, plus how many cells the fill added."""
    f = FIELDS[name]
    lo, hi = f["lo"], f["hi"]
    g = np.asarray(grid, dtype=np.float64)
    if f["fill"] if fill is None else fill:
        filled, added = fill_coast(g)
    else:
        filled, added = g, np.zeros(g.shape, bool)
    q = quantize(filled, lo, hi)
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
            "filled_points": int(added.sum()),
        },
    }
