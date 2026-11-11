"""The water rule (plan section 93b): a pixel is water when its stored elevation is at or below the region's mean
higher high water AND it is connected to the open sea. Inland ponds, quarries and marshes below that level drop out;
harbours, lagoons and fishponds joined to the sea stay.

Everything is decided on the stored depth codes (bathy_tiles: 1 cm steps near the surface), so every pass of the build
sees the same low pixels: low = stored depth >= -MHHW, deep = stored depth > bathy_sources.DEEP_M.

Connectivity is decided at the build's FINEST zoom (G61a P2-1: a coarser "any of 2 x 2" mask let ponds behind a one- or
two-pixel wall join the sea), with 4-neighbour joins (a diagonal touch is not a channel). The build works in blocks; each
block is labelled on its own (`summarise`: its label count, the labels holding deep water, its four edge rows of labels)
and the labels are joined across block edges with a union-find (`sea_masks`), so the result is the same as labelling the
whole region at once. "The open sea" = every connected low area holding a deep pixel. A block's sea mask is then its
labels mapped through `keep` (`sea_of`): the block is labelled again from the same codes, so the numbering is the same.
"""
import numpy as np
from scipy import ndimage

import bathy_sources as S
import bathy_tiles as T

def low_code(mhhw):
    """The smallest depth code that counts as low (stored depth >= -mhhw)."""
    return int(T.encode_depth(np.array([-float(mhhw)]))[0])


def deep_code():
    """Codes above this are deeper than bathy_sources.DEEP_M."""
    return int(T.encode_depth(np.array([float(S.DEEP_M)]))[0])


def low_of(code, mhhw):
    return np.asarray(code) >= low_code(mhhw)


def deep_of(code):
    return np.asarray(code) > deep_code()


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


def summarise(low, deep):
    """One block's low mask (bool n x n) and deep mask -> (label count, int32 labels holding deep low pixels,
    (top, bottom, left, right) int32 edge rows of labels; 0 = not low)."""
    lab, count = _label(low)
    deep_labels = np.unique(lab[deep & low]).astype(np.int32)
    deep_labels = deep_labels[deep_labels > 0]
    edges = (lab[0, :].astype(np.int32), lab[-1, :].astype(np.int32), lab[:, 0].astype(np.int32),
             lab[:, -1].astype(np.int32))
    return count, deep_labels, edges


def sea_masks(blocks):
    """blocks: {(bx, by): summarise(...)}. Returns {(bx, by): keep} with keep a bool array of the block's label count
    + 1: True for the labels reaching the open sea anywhere in the region. Block (bx, by)'s right neighbour is
    (bx + 1, by), its lower one (bx, by + 1)."""
    uf = _Union()
    offsets, deep_roots = {}, set()
    off = 0
    for key in sorted(blocks):
        count, deep_labels, _ = blocks[key]
        offsets[key] = off
        deep_roots.update(int(v) + off for v in deep_labels)
        off += count

    def glob(key, a):
        a = np.asarray(a, np.int64)
        return np.where(a > 0, a + offsets[key], 0)

    def join(a, b):
        both = (a > 0) & (b > 0)
        if both.any():
            for u, v in np.unique(np.stack([a[both], b[both]], axis=1), axis=0).tolist():
                uf.union(u, v)

    for key in sorted(blocks):
        bx, by = key
        _, _, (top, bottom, left, right) = blocks[key]
        r = blocks.get((bx + 1, by))
        if r is not None:
            join(glob(key, right), glob((bx + 1, by), r[2][2]))
        d = blocks.get((bx, by + 1))
        if d is not None:
            join(glob(key, bottom), glob((bx, by + 1), d[2][0]))
    sea_roots = {uf.find(v) for v in deep_roots}
    roots = np.array(sorted(sea_roots), np.int64)
    joined = sorted(uf.parent)
    out = {}
    for key in sorted(blocks):
        count = blocks[key][0]
        off = offsets[key]
        # a label never joined to another is its own root
        keep = np.isin(np.arange(off, off + count + 1, dtype=np.int64), roots)
        keep[0] = False
        lo = np.searchsorted(joined, off + 1)
        hi = np.searchsorted(joined, off + count, side="right")
        for g in joined[lo:hi]:
            keep[g - off] = uf.find(g) in sea_roots
        out[key] = keep
    return out


def sea_of(low, keep):
    """The block's sea mask: its low mask labelled again, mapped through keep."""
    lab, count = _label(low)
    if count + 1 != len(keep):
        raise ValueError("the block's labels changed between the passes (%d vs %d)" % (count + 1, len(keep)))
    return keep[lab] & (lab > 0)


def sea_from_masks(masks):
    """{key: (low, deep)} -> {key: sea mask}: the whole rule in one call (tests and small builds)."""
    keep = sea_masks({k: summarise(low, deep) for k, (low, deep) in masks.items()})
    return {k: sea_of(masks[k][0], keep[k]) for k in masks}


def sea_around(key, get, n, m):
    """The sea mask of block `key` grown by m pixels on every side from its 8 neighbours' masks. get(key) -> bool
    (n, n) or None (missing = no sea). Returns bool (n + 2m, n + 2m)."""
    bx, by = key
    out = np.zeros((n + 2 * m, n + 2 * m), bool)
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            a = get((bx + dx, by + dy))
            if a is None:
                continue
            ys = slice(m, m + n) if dy == 0 else (slice(0, m) if dy < 0 else slice(m + n, n + 2 * m))
            xs = slice(m, m + n) if dx == 0 else (slice(0, m) if dx < 0 else slice(m + n, n + 2 * m))
            sy = slice(0, n) if dy == 0 else (slice(n - m, n) if dy < 0 else slice(0, m))
            sx = slice(0, n) if dx == 0 else (slice(n - m, n) if dx < 0 else slice(0, m))
            out[ys, xs] = a[sy, sx]
    return out
