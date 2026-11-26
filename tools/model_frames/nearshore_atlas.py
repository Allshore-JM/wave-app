"""Sweet-spot atlas (plan section 109, stage 3): which buoy swell is theoretically best at each place.

A matrix of single test swells (buoy height x peak period x direction x tide) is run through the nearshore engine. Each
ride's 0-100 score is laid on the 30 m map cells it passes (and their neighbours, since a break line moves a little
with every change of swell). Per cell the atlas keeps the best case and the window of cases that reach SHARE of it.

Heights here are BUOY swell heights (Hs at the offshore buoy), in feet in every printed text.
Built once per table and parameter version; the browser only reads the summary."""
import math

import numpy as np

import nearshore_engine as E

FT = 3.281
SIZES_FT = (2, 3, 4, 5, 6, 8, 10, 12, 15, 20)
PERIODS_S = (9, 11, 13, 15, 17, 20)
DIRS_DEG = tuple(range(270, 360, 10)) + (0, 10, 20, 30)
TIDES_M = (-0.2, 0.1, 0.4)               # low, mid, high against mean sea level (Haleiwa's range is about 0.6 m)
TIDE_NAMES = ("low", "mid", "high")
CELL_M = 30.0
SHARE = 0.75                             # the window = the cases around the best one that keep this share of its score
MIN_BEST = 20.0                          # cells whose best score is under this are left blank (nothing surfable passes)


def conditions(sizes=SIZES_FT, periods=PERIODS_S, dirs=DIRS_DEG, tides=TIDES_M):
    return [(s, t, d, z) for s in sizes for t in periods for d in dirs for z in tides]


def cell_shape(table, cell=CELL_M):
    return int(math.ceil(table.ny * table.dx / cell)), int(math.ceil(table.nx * table.dx / cell))


def lay(lines, shape, cell=CELL_M):
    """One case's rides -> (score[cy, cx] uint8, flag[cy, cx] uint8: 1 = a break line passes, 2 = it closes out there).
    Each vertex marks its own cell and the eight around it; a cell keeps the best score laid on it."""
    score = np.zeros(shape, np.uint8); flag = np.zeros(shape, np.uint8)
    if not lines["x"].size: return score, flag
    cy = (lines["y"] // cell).astype(int); cx = (lines["x"] // cell).astype(int)
    sc = np.clip(np.rint(lines["score"]), 0, 100).astype(np.uint8); cl = np.where(lines["band"] == 0, 2, 1).astype(np.uint8)
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            y = cy + dy; x = cx + dx; ok = (y >= 0) & (y < shape[0]) & (x >= 0) & (x < shape[1])
            np.maximum.at(score, (y[ok], x[ok]), sc[ok])
            if dy == 0 and dx == 0: np.maximum.at(flag, (y[ok], x[ok]), cl[ok])
    return score, flag


def build(table, conds=None, p=E.PARAMS, cell=CELL_M, progress=None):
    """-> Q[ncond, cy, cx] uint8 scores, FL[ncond, cy, cx] uint8 flags, and the condition list."""
    conds = conditions() if conds is None else conds
    shape = cell_shape(table, cell); Q = np.zeros((len(conds),) + shape, np.uint8); FL = np.zeros_like(Q)
    cache = {}
    for n, (ft, tp, th, tide) in enumerate(conds):
        key = (ft, tp, th)
        if key not in cache:
            cache.clear(); f, d, S = E.jonswap(ft / FT, tp, th); cache[key] = E.weights(f, d, S, table.F, table.D)[0]
        r = E.analyse(table, [dict(W=cache[key], tp=tp)], tide, p)
        Q[n], FL[n] = lay(r["lines"], shape, cell)
        if progress and n % 100 == 0: progress(n, len(conds))
    return Q, FL, conds


def _run(ok, at):
    """The unbroken run of True in `ok` that holds index `at` -> (first, last)."""
    lo = hi = at
    while lo > 0 and ok[lo - 1]: lo -= 1
    while hi < len(ok) - 1 and ok[hi + 1]: hi += 1
    return lo, hi


def _blur(a, axis):
    """1-2-1 blur along one axis with the ends repeated."""
    lo = np.take(a, [0], axis); hi = np.take(a, [-1], axis); p = np.concatenate((lo, a, hi), axis)
    n = a.shape[axis]; return 0.25 * np.take(p, range(0, n), axis) + 0.5 * a + 0.25 * np.take(p, range(2, n + 2), axis)


def summarise(Q, FL, conds, sizes=SIZES_FT, periods=PERIODS_S, dirs=DIRS_DEG, tides=TIDES_M, share=SHARE, min_best=MIN_BEST, tide_names=TIDE_NAMES,
              smooth=True):
    """Per cell: the best case and the window around it. -> list of dicts (only cells whose best score reaches min_best):
    (with smooth, scores are first blurred 1-2-1 across sizes, periods and directions)
    cy, cx; best (ft, s, deg, tide index, score); size_ft (lo, hi) = the unbroken run of sizes around the best one that
    keep `share` of its score, at the best period, direction and tide; dir_deg (from, to) likewise across directions;
    by_period {s: (lo, hi)} the same size run at each other period that reaches the share; closes_deg (from, to) or
    None; starts_ft; tide: 'any' or the best tide's name."""
    ns, nt, nd, nz = len(sizes), len(periods), len(dirs), len(tides)
    A = Q.reshape(ns, nt, nd, nz, *Q.shape[1:]).astype(np.float32); Fg = FL.reshape(ns, nt, nd, nz, *FL.shape[1:])
    if smooth:                                           # one case's score jumps as rides split and join: a sweet spot is a
        for ax in (0, 1, 2): A = _blur(A, ax)            # plateau across neighbouring sizes, periods and directions, not a spike
    out = []
    best = A.reshape(-1, *Q.shape[1:]).max(0)
    for cy, cx in zip(*np.nonzero(best >= min_best)):
        a = A[..., cy, cx]; fl = Fg[..., cy, cx]; top = int(round(float(a.max()))); need = share * float(a.max())
        i, j, k, z = np.unravel_index(int(a.argmax()), a.shape)
        lo, hi = _run(a[:, j, k, z] >= need, i); k0, k1 = _run(a[i, j, :, z] >= need, k)
        byp = {}
        for q in range(nt):
            col = a[:, q, k, z]
            if col.max() >= need:
                q0, q1 = _run(col >= need, int(col.argmax())); byp[int(periods[q])] = (sizes[q0], sizes[q1])
        # closes out: directions where a line passes this cell at the working sizes and is a closeout more often than not
        seen = (fl[lo:hi + 1, j, :, z] > 0).sum(0); shut = (fl[lo:hi + 1, j, :, z] == 2).sum(0)
        weak = a[lo:hi + 1, j, :, z].max(0) < 0.3 * top
        cl = [n for n in range(nd) if seen[n] > 0 and shut[n] * 2 > seen[n] and weak[n]]
        any_line = (fl[:, j, k, z] > 0); starts = sizes[int(np.argmax(any_line))] if any_line.any() else None
        tide_free = all(a[i, j, k, q] >= need for q in range(nz))
        out.append(dict(cy=int(cy), cx=int(cx), best=(int(sizes[i]), int(periods[j]), int(dirs[k]), int(z), top), size_ft=(sizes[lo], sizes[hi]),
                        dir_deg=(dirs[k0], dirs[k1]), by_period=byp, closes_deg=(dirs[cl[0]], dirs[cl[-1]]) if cl else None, starts_ft=starts,
                        tide="any" if tide_free else tide_names[z]))
    return out


def text(c):
    """The readout line for one cell, e.g. 'Best here: buoy 6-10 ft @ 15 s from 340-010, mid tide'."""
    def rng(lo, hi, unit=""):
        return f"{lo}{unit}" if lo == hi else f"{lo}-{hi}{unit}"
    ft, tp, th, _, _ = c["best"]; s = c["size_ft"]; d = c["dir_deg"]
    t = f"Best here: buoy {rng(*s)} ft @ {tp} s from {d[0]:03d}" + (f"-{d[1]:03d}" if d[1] != d[0] else "")
    if c["tide"] != "any": t += f", {c['tide']} tide"
    others = [f"{rng(*v)} ft @ {q} s" for q, v in sorted(c["by_period"].items()) if q != tp and v]
    if others: t += " (" + ", ".join(others[:3]) + ")"
    if c["closes_deg"]: t += f". Closes out from {c['closes_deg'][0]:03d}" + (f"-{c['closes_deg'][1]:03d}" if c["closes_deg"][1] != c["closes_deg"][0] else "")
    return t
