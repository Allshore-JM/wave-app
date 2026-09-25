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

Coastal fill (wave height and peak period only; manifest "fill" = FILL_INFO): GFS-Wave leaves cells
with enough land in them empty (a land-fraction threshold), so without help the field stops short
of many coasts. Before quantisation, empty cells that are within FILL_CELLS cells of model data AND
allowed by fill_allow.png (the node's 0.25-degree box, or a neighbour's, holds GSHHG land; built by
make_fill_mask.py) are filled, ring by ring (longitude periodic, nothing beyond the poles):
  hs  the mean of the present 8-neighbours of the previous ring (smooth; the client is bilinear)
  tp  the value of the nearest model cell (Euclidean in grid cells), so every filled node carries a
      period the model really has, never a mean of two swell regimes; the browser then
      interpolates between nodes bilinearly, like hs
Model values never change; wind is never filled; open water more than one cell from land (in
practice the sea-ice pack the model masks) is never filled -- along ice-bound coasts the fill can
still reach up to one cell (about 28 km) over coastal sea ice. The browser clips the result to the same GSHHG coast, so the fill is
only seen over water. Coverage: after k rings a coastline point is drawn on full frames when its
nearest grid node is within k cells of model data, on half frames within k - 1 (the nearest half
node is within one cell of the nearest full node, and a present nearest node always carries the
bilinear weight >= 0.25 the client needs). k = 4 trades reach into bays against extrapolation
distance; no k reaches everything (the model has no cells at all in the Black Sea, for example).
"""
import functools
import io
import os

import numpy as np
from PIL import Image

KT = 1852.0 / 3600.0                    # 1 knot in m/s

FULL_HALF = ("full", "half")
FIELDS = {
    # name: dict(lo, hi = ENCODING range; legend = display range; units; interpolation hint; resolutions
    # published). Direction fields (circular: degrees true, the direction waves / wind come FROM) are
    # data for the animation, not drawn as colour: they are coded circularly (see quantize_circular),
    # interpolated as angles by the client, and filled only by nearest (never a mean of angles).
    "hs":   {"lo": 0.0, "hi": 15.0,     "legend": [0.0, 12.0],    "units": "m",   "interpolation": "bilinear", "fill": True,  "resolutions": FULL_HALF},
    "tp":   {"lo": 1.0, "hi": 30.0,     "legend": [4.0, 22.0],    "units": "s",   "interpolation": "bilinear", "fill": True,  "resolutions": FULL_HALF},
    "wind": {"lo": 0.0, "hi": 80 * KT,  "legend": [0.0, 60 * KT], "units": "m/s", "interpolation": "bilinear", "fill": False, "resolutions": FULL_HALF},
    "pdir": {"lo": 0.0, "hi": 360.0,    "legend": [0.0, 360.0],   "units": "deg", "interpolation": "circular", "fill": True,  "resolutions": FULL_HALF,
             "circular": True, "convention": "from"},
    "wdir": {"lo": 0.0, "hi": 360.0,    "legend": [0.0, 360.0],   "units": "deg", "interpolation": "circular", "fill": False, "resolutions": ("half",),
             "circular": True, "convention": "from"},
}
FILL_CELLS = 4
FILL_VERSION = 2                          # bump whenever the fill's output changes for the same input
FILL_METHODS = {"hs": "mean", "tp": "nearest", "pdir": "nearest"}
assert all(FILL_METHODS[n] == "nearest" for n, f in FIELDS.items() if f.get("circular") and f["fill"]), "angles are never averaged"
FILL_ALLOW_PNG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fill_allow.png")
FILL_ALLOW_SHA256 = "35c4e29f2122794b9d17ab491920aaac7f55cb76aa703c9eaa08ca5baf7e8c7d"
_FILLED = sorted(n for n, f in FIELDS.items() if f["fill"])
FILL_INFO = None if not _FILLED else {
    "version": FILL_VERSION, "fields": _FILLED, "cells": FILL_CELLS,
    "methods": {n: FILL_METHODS[n] for n in _FILLED},
    "limit": "only within one cell of GSHHG land; may extend up to one cell over coastal sea ice",
    "mask": FILL_ALLOW_SHA256[:16],
    "note": "nearshore values are extrapolated from the nearest model cells; clipped to the coastline in the browser",
}


def fill_key(block):
    """What identifies a fill for the re-publish guard: everything that changes the pixels (version,
    fields, rings, per-field method, the allow mask) -- wording (note, limit) is not a different fill."""
    if not block:
        return None
    return (block.get("version", 1), tuple(block.get("fields", ())), block.get("cells"),
            tuple(sorted((block.get("methods") or {}).items())), block.get("mask"))


@functools.lru_cache(maxsize=1)
def fill_allow():
    """The static (721, 1440) bool mask of nodes the fill may write, verified against its pinned hash."""
    raw = open(FILL_ALLOW_PNG, "rb").read()
    import hashlib
    if hashlib.sha256(raw).hexdigest() != FILL_ALLOW_SHA256:
        raise RuntimeError("fill_allow.png does not match FILL_ALLOW_SHA256 (rebuild with make_fill_mask.py and re-pin)")
    a = np.array(Image.open(io.BytesIO(raw)).convert("L")) > 0
    if a.shape != (721, 1440):
        raise RuntimeError("fill_allow.png must be 1440x721")
    a.setflags(write=False)
    return a
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


def quantize_circular(grid):
    """Degrees -> codes on a circle of 254 steps: q = 1 + (round(v * 254 / 360) mod 254), so 0 and 360
    (and anything wrapping) share code 1 and code 255 is never used. Decoding is the ordinary
    u8-linear-v2 formula over lo = 0, hi = 360: value = (q - 1) * 360 / 254; error <= 360 / 508 deg."""
    g = np.asarray(grid, dtype=np.float64)
    k = np.mod(np.round(np.mod(g, 360.0) * 254.0 / 360.0), 254.0)
    return np.where(np.isnan(g), 0, k + 1.0).astype(np.uint8)


def dequantize(q, lo, hi):
    out = lo + (q.astype(np.float64) - 1.0) / 254.0 * (hi - lo)
    return np.where(q == 0, np.nan, out)


def quantum(lo, hi):
    return (hi - lo) / 254.0


def fill_coast(grid, cells=FILL_CELLS, allow=None, method="mean"):
    """-> (filled float64 copy, bool mask of the cells that were filled). Only NaN cells change, and
    only where `allow` (bool, same shape; None = everywhere) is true. The filled SET is the same for
    both methods: the cells reached ring by ring through allowed cells. method "mean" gives each the
    mean of its present 8-neighbours of the previous ring (Jacobi); "nearest" gives it the value of
    the Euclidean-nearest model cell (in grid cells, ties by a fixed offset order). A cell reached in
    <= cells rings has a model cell at Chebyshev distance <= cells, i.e. Euclidean <= cells * sqrt(2),
    so searching a Chebyshev window of floor(cells * sqrt(2)) finds the true nearest."""
    g = np.array(grid, dtype=np.float64)                    # a copy: the caller's grid is untouched
    missing0 = np.isnan(g)
    rows, cols = g.shape
    ok_to_fill = np.ones(g.shape, bool) if allow is None else np.asarray(allow, bool)
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
        target = np.isnan(g) & (n > 0) & ok_to_fill
        if not target.any():
            break
        g[target] = tot[target] / n[target]                 # Jacobi: every mean uses the previous pass
    added = missing0 & ~np.isnan(g)
    if method == "nearest" and added.any():
        g = _nearest_values(np.asarray(grid, dtype=np.float64), added, int(cells * 2 ** 0.5))
    elif method not in ("mean", "nearest"):
        raise ValueError(method)
    return g, added


def _nearest_values(model, targets, cells):
    """`model` with every target cell set to the value of its nearest model cell within `cells`
    (Chebyshev window; every target has one, since it was reached in <= cells rings)."""
    rows, cols = model.shape
    out = model.copy()
    todo = targets.copy()
    offs = sorted(((dy, dx) for dy in range(-cells, cells + 1) for dx in range(-cells, cells + 1) if dy or dx),
                  key=lambda o: (o[0] ** 2 + o[1] ** 2, abs(o[0]), o[0], o[1]))
    pad = np.full((rows + 2 * cells, cols), np.nan)
    pad[cells:-cells] = model
    for dy, dx in offs:
        src = np.roll(pad[cells + dy:cells + dy + rows], -dx, axis=1)       # src[i, j] = model[i + dy, j + dx]
        hit = todo & ~np.isnan(src)
        out[hit] = src[hit]
        todo &= ~hit
        if not todo.any():
            break
    if todo.any():
        raise AssertionError("a filled cell has no model cell within the window")
    return out


def half_res(q):
    """Exact 2x subsample keeping the (+90 N, -180 E) origin: shape (361, 720)."""
    return np.ascontiguousarray(q[::2, ::2])


def to_png(q):
    im = Image.fromarray(q, "L")
    buf = io.BytesIO()
    im.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def encode_frame(grid, name, fill=None, allow=None):
    """-> {'full': png bytes, 'half': png bytes, 'stats': {...}} for one field ('full' / 'half' only for
    the field's resolutions). The coastal fill (default: FIELDS[name]["fill"]) runs before
    quantisation and before the half subsample; the stats describe the MODEL values only, plus how
    many cells the fill added. `allow` defaults to fill_allow()."""
    f = FIELDS[name]
    lo, hi = f["lo"], f["hi"]
    g = np.asarray(grid, dtype=np.float64)
    if f["fill"] if fill is None else fill:
        filled, added = fill_coast(g, allow=fill_allow() if allow is None else allow, method=FILL_METHODS[name])
    else:
        filled, added = g, np.zeros(g.shape, bool)
    q = quantize_circular(filled) if f.get("circular") else quantize(filled, lo, hi)
    valid = ~np.isnan(g)
    out = {}
    if "full" in f["resolutions"]:
        out["full"] = to_png(q)
    if "half" in f["resolutions"]:
        out["half"] = to_png(half_res(q))
    return dict(out, **{
        "stats": {
            "min": round(float(np.nanmin(g)), 4) if valid.any() else None,
            "max": round(float(np.nanmax(g)), 4) if valid.any() else None,
            "valid_points": int(valid.sum()),
            "clamped_low": int(np.count_nonzero(g < lo)),
            "clamped_high": int(np.count_nonzero(g > hi)),
            "filled_points": int(added.sum()),
        },
    })
