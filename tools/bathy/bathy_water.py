"""The water rule (plan section 93b): a pixel is water when its elevation is at or below the region's mean higher high
water AND it is connected to the open sea. Inland ponds, quarries and marshes below that level drop out; harbours,
lagoons and fishponds joined to the sea stay.

Connectivity is decided one zoom coarser than the build's finest zoom, on a mask that is "low" where ANY of the 2 x 2
fine pixels is low (so a one-pixel channel keeps its connection), with 4-neighbour connectivity. The build works in
blocks (one tile of a coarser zoom each); every block is labelled on its own and the labels are joined across block
edges with a union-find, so the result is the same as labelling the whole region at once. "The open sea" = every
connected low area holding a pixel deeper than bathy_sources.DEEP_M.
"""
import numpy as np
from scipy import ndimage

import bathy_tiles as T


def pool_any(mask):
    """bool (2h, 2w) -> bool (h, w): True where any of the 2 x 2 is True."""
    h, w = mask.shape[0] // 2, mask.shape[1] // 2
    return mask[:2 * h, :2 * w].reshape(h, 2, w, 2).any(axis=(1, 3))


def _label(mask):
    """Connected low areas, 4-neighbour joins (a diagonal touch is not a channel). One function for both passes of
    sea_masks: the second pass re-labels a block (its labels are not kept in memory) and must number them the same way."""
    return ndimage.label(mask)


class _Union:
    def __init__(self):
        self.parent = {}

    def find(self, a):
        p = self.parent
        root = a
        while p.get(root, root) != root:
            root = p[root]
        while p.get(a, a) != root:
            nxt = p[a]
            p[a] = root
            a = nxt
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            if ra < rb:
                ra, rb = rb, ra
            self.parent[ra] = rb


def sea_masks(blocks):
    """blocks: {(bx, by): (packed_low, packed_deep, n)} with n x n coarse masks (bit-packed, tiles.pack_bits).

    Returns {(bx, by): packed sea mask} for every block: True where the coarse pixel belongs to a low area that reaches
    the open sea anywhere in the region. Block (bx, by)'s right neighbour is (bx + 1, by), its lower one (bx, by + 1)."""
    uf = _Union()
    offsets, edges, n0 = {}, {}, None
    deep_roots = set()
    off = 0
    for key in sorted(blocks):
        low_b, deep_b, n = blocks[key]
        n0 = n0 or n
        low = T.unpack_bits(low_b, (n, n))
        deep = T.unpack_bits(deep_b, (n, n)) & low
        lab, count = _label(low)
        offsets[key] = off
        g = np.where(lab > 0, lab + off, 0)
        edges[key] = (g[0, :].copy(), g[-1, :].copy(), g[:, 0].copy(), g[:, -1].copy())
        for v in np.unique(g[deep]):
            deep_roots.add(int(v))
        off += count
    for (bx, by), (top, bottom, left, right) in edges.items():
        r = edges.get((bx + 1, by))
        if r is not None:
            both = (right > 0) & (r[2] > 0)
            for a, b in zip(right[both], r[2][both]):
                uf.union(int(a), int(b))
        d = edges.get((bx, by + 1))
        if d is not None:
            both = (bottom > 0) & (d[0] > 0)
            for a, b in zip(bottom[both], d[0][both]):
                uf.union(int(a), int(b))
    sea_roots = {uf.find(v) for v in deep_roots}
    out = {}
    for key in sorted(blocks):
        low_b, _, n = blocks[key]
        lab, count = _label(T.unpack_bits(low_b, (n, n)))
        off = offsets[key]
        keep = np.zeros(count + 1, bool)
        for i in range(1, count + 1):
            keep[i] = uf.find(i + off) in sea_roots
        out[key] = T.pack_bits(keep[lab] & (lab > 0))
    return out


def sea_around(key, sea, n, m):
    """The coarse sea mask of block `key` grown by m pixels on every side from its 8 neighbours' masks (missing = no sea).
    Returns bool (n + 2m, n + 2m)."""
    bx, by = key
    out = np.zeros((n + 2 * m, n + 2 * m), bool)
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            b = sea.get((bx + dx, by + dy))
            if b is None:
                continue
            a = T.unpack_bits(b, (n, n))
            ys = slice(m, m + n) if dy == 0 else (slice(0, m) if dy < 0 else slice(m + n, n + 2 * m))
            xs = slice(m, m + n) if dx == 0 else (slice(0, m) if dx < 0 else slice(m + n, n + 2 * m))
            sy = slice(0, n) if dy == 0 else (slice(n - m, n) if dy < 0 else slice(0, m))
            sx = slice(0, n) if dx == 0 else (slice(n - m, n) if dx < 0 else slice(0, m))
            out[ys, xs] = a[sy, sx]
    return out
