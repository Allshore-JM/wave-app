"""Condition-bin schema: map (Hs, Tp, dir, tide) -> a discrete bin, and enumerate bins.

Axes come from the spot config `bin_axes`. Intervals are half-open [e_i, e_{i+1}) with the
last edge inclusive. Below the lowest edge: axis `below_range` = "null" (whole condition is
below threshold -> no bin) or "clamp" (use lowest bin, mark extrapolated). Above the top
edge: clamp to the top bin, mark extrapolated.

Wraparound (`"wrap": true`, direction axes): edges are stored monotonic-in-wrapped-space
and may exceed 360 (e.g. [270, 305, 340, 375, 405] = W..NE crossing north; 375 means 15).
Inputs below edges[0] are normalized by +360. A wrapped axis has ONE contiguous
out-of-window arc (e.g. 45..270), so any value landing outside the edges after
normalization is None ("outside window") -- never clamped/extrapolated: a direction the
spot doesn't work on is below-threshold, not an extreme of the modeled range.
"""
from __future__ import annotations

from itertools import product

AXES_ORDER = ["hs_swell_ft", "tp_s", "dir_deg", "tide_m"]


def _bin_index_1d(value: float, axis: dict):
    """Return (index, extrapolated, reason). index is None when the value has no bin; reason
    in {in_range, clamped_low, clamped_high, below_range, outside_window}."""
    edges = axis["edges"]
    n = len(edges) - 1
    if axis.get("wrap"):
        v = value % 360
        if v < edges[0]:
            v += 360
        if v < edges[0] or v > edges[-1]:
            return None, False, "outside_window"   # the single wrapped out-of-window arc
        value = v
    if value < edges[0]:
        if axis.get("below_range", "null") == "clamp":
            return 0, True, "clamped_low"
        return None, False, "below_range"
    if value > edges[-1]:
        return n - 1, True, "clamped_high"
    for i in range(n):
        lo, hi = edges[i], edges[i + 1]
        if (lo <= value < hi) or (i == n - 1 and lo <= value <= hi):
            return i, False, "in_range"
    return n - 1, True, "clamped_high"  # exactly the top edge (handled above, defensive)


def bin_of(hs_ft: float, tp_s: float, dir_deg: float, tide_m: float, axes: dict):
    """Return {'index': (..), 'extrapolated': bool} or None (condition below threshold)."""
    values = {"hs_swell_ft": hs_ft, "tp_s": tp_s, "dir_deg": dir_deg, "tide_m": tide_m}
    idx = []
    extrap = False
    for key in AXES_ORDER:
        i, ex, _why = _bin_index_1d(values[key], axes[key])
        if i is None:
            return None
        idx.append(i)
        extrap = extrap or ex
    return {"index": tuple(idx), "extrapolated": extrap}


def bin_of_detail(hs_ft: float, tp_s: float, dir_deg: float, tide_m: float, axes: dict) -> dict:
    """Same decision as bin_of(), plus WHY per axis -- so the live endpoint can render the
    right degraded state: a direction outside a wrapped window is 'outside_window' (the spot
    does not work on it), an Hs/Tp below the floor is 'below_threshold', and a clamped value
    is 'extrapolated'. Never raises; index is None exactly when bin_of would be None."""
    values = {"hs_swell_ft": hs_ft, "tp_s": tp_s, "dir_deg": dir_deg, "tide_m": tide_m}
    detail, extrap = {}, False
    for key in AXES_ORDER:
        i, ex, why = _bin_index_1d(values[key], axes[key])
        detail[key] = {"index": i, "extrapolated": ex, "reason": why}
        extrap = extrap or ex
    missing = [k for k in AXES_ORDER if detail[k]["index"] is None]
    if missing:
        reason = ("outside_window"
                  if any(detail[k]["reason"] == "outside_window" for k in missing)
                  else "below_threshold")
        return {"index": None, "extrapolated": extrap, "axes": detail, "reason": reason}
    return {"index": tuple(detail[k]["index"] for k in AXES_ORDER),
            "extrapolated": extrap, "axes": detail, "reason": None}


def bin_center(index: tuple[int, ...], axes: dict) -> dict:
    """Representative center values (midpoint of each axis interval) for a bin index."""
    out = {}
    for key, i in zip(AXES_ORDER, index):
        edges = axes[key]["edges"]
        out[key] = 0.5 * (edges[i] + edges[i + 1])
    return out


def _tide_tag(lo: float, hi: float, edges: list[float]) -> str:
    mid = (lo + hi) / 2
    if mid < -0.05:
        return "LO"
    if mid <= 0.05 and len(edges) > 3:   # a genuine middle bin exists (3+ tide bins)
        return "MID"
    return "HI"


def bin_key(index: tuple[int, ...], axes: dict) -> str:
    e = {k: axes[k]["edges"] for k in AXES_ORDER}
    i0, i1, i2, i3 = index
    if len(e["tide_m"]) - 1 > 3:
        tide_tag = str(i3)   # LO/MID/HI saturates at 3 bins -- numeric keeps keys unique
    else:
        tide_tag = _tide_tag(e["tide_m"][i3], e["tide_m"][i3 + 1], e["tide_m"])
    d_lo = e["dir_deg"][i2] % 360        # render wrapped edges mod 360 (375 -> 15)
    d_hi = e["dir_deg"][i2 + 1] % 360
    return (f"hs{e['hs_swell_ft'][i0]:g}-{e['hs_swell_ft'][i0+1]:g}"
            f"_tp{e['tp_s'][i1]:g}-{e['tp_s'][i1+1]:g}"
            f"_d{d_lo:g}-{d_hi:g}"
            f"_t{tide_tag}")


def iterate_bins(axes: dict):
    """Yield every bin as {'index', 'key', 'center'} across all axes."""
    ranges = [range(len(axes[k]["edges"]) - 1) for k in AXES_ORDER]
    for index in product(*ranges):
        yield {"index": index, "key": bin_key(index, axes), "center": bin_center(index, axes)}


def n_bins(axes: dict) -> int:
    total = 1
    for k in AXES_ORDER:
        total *= len(axes[k]["edges"]) - 1
    return total
