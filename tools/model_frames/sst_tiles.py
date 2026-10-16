"""Sea surface temperature tiles (plan section 44): NASA JPL MUR SST v4.1 cut into a three-tier pyramid.

Pure numpy; no I/O. The source grid is MUR's: PX pixels per degree (100), pixel centres on the 1/PX-degree lattice,
latitudes -89.99..89.99 (17999 rows; here NORTH-UP: row R at lat_h = 90*PX - 1 - R, lat_h = latitude in 1/PX
degrees), longitudes -179.99..180.00 (36000 columns; column C at lon_h = -180*PX + 1 + C, periodic). Every tier sits
on the same lattice, so nothing is resampled:
  t2  every pixel (0.01 deg), 5-deg cells
  t1  the mean of the 5x5 pixels centred on each 0.05-deg node, 20-deg cells
  t0  the mean of the 25x25 pixels centred on each 0.25-deg node: the overlay frames' grid (1440 x 721,
      centre-registered, lon0 -180, lat0 90), the whole globe in one picture
A cell named "<lat0>_<lon0>" (its south-west corner, whole degrees, like the coastline chunks) covers
lat0 <= lat < lat0 + cell, lon0 <= lon < lon0 + cell with n = cell / res nodes a side. Its picture holds n + 2 nodes a
side (a one-node apron shared with the neighbouring cells, so a point anywhere in the cell finds its four bilinear
neighbours in the cell's own picture): row r / column c is centred at lat = lat0 + cell - r * res,
lon = lon0 + (c - 1) * res (rows north to south). Codes: 0 = no value, else v = lo + (code - 1) / 254 * (hi - lo) in
degrees C, lo / hi per cell (t1, t2; listed in the manifest) or the fixed RANGE (t0).

Which pixels carry a value: MUR's open sea (mask 1); MUR's own analysed values on land-flagged pixels within
COAST_PX pixels of open sea (MUR extends its analysis smoothly over the coast: next to the sea those values differ from
the sea pixel by 0.003-0.03 deg C, like two sea pixels; 2026-10-14 file); then FILL_PASSES rings of the mean of valued
neighbours over the remaining land pixels, so water the precise GSHHG coastline has inside MUR's coarser land mask gets
a value (the client clips land with GSHHG). Lakes (mask 5) and ice-covered water (mask 9 / 13: sea-ice fraction >= 15 %
in every such pixel) stay empty, as the wave overlays leave ice empty. t1 / t0 nodes average the valued (unfilled)
pixels of their block; a node without one but with land in its block takes the mean of its valued neighbour nodes
(NODE_FILL rings).
"""
import math

import numpy as np

PX = 100                              # MUR pixels per degree
NONE = -32768                         # no value (MUR's fill; lakes, ice, open land far from the sea)
FILLABLE = -32767                     # land without a used value: the coastal fill may give it one
RAW_MIN = -30000                      # every analysed value is above this (SST >= -1.8 C is raw -26800)
COAST_PX = 3                          # MUR's land values used within this many pixels (Chebyshev) of open sea
FILL_PASSES = 2                       # rings of the coastal mean fill at t2 (0.01 deg)
NODE_FILL = 1                         # rings of the node fill at t1 / t0
RANGE = (-2.0, 35.0)                  # t0's fixed code range, degrees C (the client's legend)
MASK_SEA, MASK_LAND = 1, 2
TIERS = (                             # step in source pixels, cell in degrees (t0: the whole globe)
    {"name": "t2", "step": 1, "cell": 5},
    {"name": "t1", "step": 5, "cell": 20},
)
T0_STEP = 25
NEIGH8 = ((-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1))   # fixed order: same sums in every cell


class Geo:
    """The source lattice for `px` pixels per degree (MUR: 100). Tests use a coarser one; the code is the same."""

    def __init__(self, px=PX):
        self.px = px
        self.rows = 180 * px - 1
        self.cols = 360 * px

    def row(self, lat_h):
        """North-up row of each latitude (1/px deg); -1 where the source has no row (|lat| >= 90)."""
        r = 90 * self.px - 1 - np.asarray(lat_h)
        return np.where((r >= 0) & (r < self.rows), r, -1)

    def col(self, lon_h):
        """Column of each longitude (1/px deg), periodic."""
        return (np.asarray(lon_h) + 180 * self.px - 1) % self.cols

    def res(self, step):
        return step / self.px


def to_c(raw):
    """MUR raw int16 -> degrees C: kelvin = raw * 0.001 + 298.15, so C = 25 + raw / 1000."""
    return 25.0 + np.asarray(raw, dtype=np.float64) / 1000.0


def _dilate(b, k):
    """Chebyshev dilation by k pixels: rows clamp (the band carries margins), columns periodic."""
    out = b.copy()
    for d in range(1, k + 1):
        out[d:] |= b[:-d]
        out[:-d] |= b[d:]
    rows = out.copy()
    for d in range(1, k + 1):
        out |= np.roll(rows, d, axis=1)
        out |= np.roll(rows, -d, axis=1)
    return out


def value_grid(sst, mask, band=1000):
    """IN PLACE on `sst` (north-up raw int16): open-sea values kept, MUR's land values within COAST_PX of open sea
    kept, other land -> FILLABLE, everything else (lakes, ice, fill) -> NONE. Returns counts for the stats."""
    if sst.shape != mask.shape:
        raise ValueError("sst and mask shapes differ")
    rows = sst.shape[0]
    counts = {"sea": 0, "coast_land": 0, "fillable": 0, "lake": 0, "ice": 0}
    for b0 in range(0, rows, band):
        b1 = min(rows, b0 + band)
        a0, a1 = max(0, b0 - COAST_PX), min(rows, b1 + COAST_PX)
        m = mask[a0:a1]
        near = _dilate(m == MASK_SEA, COAST_PX)[b0 - a0:b0 - a0 + (b1 - b0)]
        mb, sb = mask[b0:b1], sst[b0:b1]
        sea = (mb == MASK_SEA) & (sb != NONE)
        land = mb == MASK_LAND
        coast = land & near & (sb != NONE)
        if np.any(sb[sea | coast] <= RAW_MIN):
            raise ValueError("an analysed value below the expected floor (raw <= %d)" % RAW_MIN)
        counts["sea"] += int(sea.sum())
        counts["coast_land"] += int(coast.sum())
        counts["fillable"] += int((land & ~coast).sum())
        counts["lake"] += int(((mb & 4) != 0).sum())
        counts["ice"] += int(((mb & 8) != 0).sum() + ((mb & 16) != 0).sum())
        sb[land & ~coast] = FILLABLE
        sb[~(sea | coast | land)] = NONE
    return counts


def coastal_fill(block, passes=FILL_PASSES):
    """int16 block (values / FILLABLE / NONE) -> float64 deg C with NaN for none: `passes` rings of the mean of valued
    8-neighbours over FILLABLE pixels. Pixels within `passes` of the block's edge are not exact (their neighbours outside
    the block are unknown): the caller crops them."""
    v = np.where(block > FILLABLE, to_c(block), np.nan)
    open_ = block == FILLABLE
    h, w = v.shape
    filled = 0
    for _ in range(passes):
        acc = np.zeros((h, w))
        cnt = np.zeros((h, w), dtype=np.int32)
        have = ~np.isnan(v)
        for dy, dx in NEIGH8:
            ys, yd = (slice(dy, h), slice(0, h - dy)) if dy >= 0 else (slice(0, h + dy), slice(-dy, h))
            xs, xd = (slice(dx, w), slice(0, w - dx)) if dx >= 0 else (slice(0, w + dx), slice(-dx, w))
            hv = have[ys, xs]
            acc[yd, xd] += np.where(hv, v[ys, xs], 0.0)
            cnt[yd, xd] += hv
        t = open_ & ~have & (cnt > 0)
        v[t] = acc[t] / cnt[t]
        open_ &= ~t
        filled += int(t.sum())
    return v, filled


def t2_cell(val, geo, lat0, lon0, cell=5):
    """(n+2) x (n+2) deg C (NaN = none) for the t2 cell at (lat0, lon0), rows north to south; + the filled count
    inside it (interior only, for the stats)."""
    n = cell * geo.px
    m = 1 + FILL_PASSES
    i = np.arange(n - 1 + m, -m - 1, -1)                  # north to south: interior 0..n-1, apron -1 and n, margin
    j = np.arange(-m, n + m)
    rr = geo.row(geo.px * lat0 + i)
    cc = geo.col(geo.px * lon0 + j)
    block = np.full((i.size, j.size), NONE, dtype=np.int16)
    ok = rr >= 0
    if ok.any():
        block[ok] = val[np.ix_(rr[ok], cc)]
    v, _ = coastal_fill(block)
    k = FILL_PASSES
    out = v[k:-k, k:-k]
    inner = block[m:-m, m:-m]
    filled_inside = int(np.count_nonzero((inner == FILLABLE) & ~np.isnan(out[1:-1, 1:-1])))
    return out, filled_inside


def node_means(val, geo, step):
    """Block means of the valued pixels of each node on the `step`-pixel lattice -> (nodes deg C with NaN,
    land-pixel count per node). Rows north to south (lat = 90 - step/px * a), columns from -180."""
    half = (step - 1) // 2
    nrows = 180 * geo.px // step + 1
    ncols = 360 * geo.px // step
    out = np.full((nrows, ncols), np.nan)
    land = np.zeros((nrows, ncols), dtype=np.int32)
    for a in range(nrows):
        c = 90 * geo.px - step * a
        rr = geo.row(np.arange(c - half, c + half + 1))
        rr = rr[rr >= 0]
        if not rr.size:
            continue
        blk = np.roll(val[rr], half + 1, axis=1).reshape(rr.size, ncols, step)
        good = blk > FILLABLE
        cnt = good.sum(axis=(0, 2))
        tot = np.where(good, blk.astype(np.int64), 0).sum(axis=(0, 2))
        has = cnt > 0
        out[a, has] = 25.0 + tot[has] / (1000.0 * cnt[has])
        land[a] = (blk == FILLABLE).sum(axis=(0, 2))
    return out, land


def node_fill(nodes, land, passes=NODE_FILL):
    """Rings of the mean of valued 8-neighbour nodes over empty nodes that have land in their block. Columns periodic,
    rows clamp. In place; returns the count filled."""
    h, w = nodes.shape
    filled = 0
    for _ in range(passes):
        have = ~np.isnan(nodes)
        acc = np.zeros((h, w))
        cnt = np.zeros((h, w), dtype=np.int32)
        for dy, dx in NEIGH8:
            v = np.roll(np.where(have, nodes, 0.0), -dx, axis=1)
            hv = np.roll(have, -dx, axis=1)
            if dy > 0:
                acc[:-dy] += v[dy:]
                cnt[:-dy] += hv[dy:]
            elif dy < 0:
                acc[-dy:] += v[:dy]
                cnt[-dy:] += hv[:dy]
            else:
                acc += v
                cnt += hv
        t = ~have & (land > 0) & (cnt > 0)
        nodes[t] = acc[t] / cnt[t]
        filled += int(t.sum())
    return filled


def node_cell(nodes, geo, step, lat0, lon0, cell):
    """(n+2) x (n+2) cut of a node grid (from node_means) for the cell at (lat0, lon0), rows north to south."""
    n = cell * geo.px // step
    a0 = (90 * geo.px - geo.px * lat0) // step
    i = np.arange(n, -2, -1)                             # node i <-> lat0 + i * res; rows north to south
    a = a0 - i
    k = ((geo.px * lon0 + 180 * geo.px) // step + np.arange(-1, n + 1)) % nodes.shape[1]
    out = np.full((i.size, k.size), np.nan)
    ok = (a >= 0) & (a < nodes.shape[0])
    out[ok] = nodes[np.ix_(a[ok], k)]
    return out


def cell_range(v):
    """(lo, hi) in deg C, two decimals, lo < hi, around every value of the cell (apron included: none clamps)."""
    fin = v[np.isfinite(v)]
    lo = math.floor(float(fin.min()) * 100) / 100
    hi = math.ceil(float(fin.max()) * 100) / 100
    if hi <= lo:
        hi = round(lo + 0.01, 2)
    return round(lo, 2), round(hi, 2)


def quantize(v, lo, hi):
    """deg C (NaN = none) -> uint8 codes: 0 none, else 1 + round((v - lo) / (hi - lo) * 254), clipped to 1..255."""
    q = np.zeros(v.shape, dtype=np.uint8)
    fin = np.isfinite(v)
    q[fin] = np.clip(np.rint((v[fin] - lo) / (hi - lo) * 254.0) + 1, 1, 255).astype(np.uint8)
    return q


def dequantize(q, lo, hi):
    out = np.full(q.shape, np.nan)
    nz = q > 0
    out[nz] = lo + (q[nz].astype(np.float64) - 1) / 254.0 * (hi - lo)
    return out


def cell_names(cell):
    """Every cell of a tier, south-west corners on multiples of the cell size from 0 (the client's coastCellsForTile:
    floor(lat / cell) * cell), so 20-deg cells run -100..80 (the outer rows half off the world)."""
    return [(lat0, lon0) for lat0 in range(math.floor(-90 / cell) * cell, 90, cell) for lon0 in range(-180, 180, cell)]


def cell_name(lat0, lon0):
    return f"{lat0}_{lon0}"
