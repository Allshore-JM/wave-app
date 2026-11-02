"""Swell partitioning of a 2-D wave spectrum (plan section 59): the watershed of Hanson and Phillips (2001), as the
wave models use it, in numpy.

Input: freqs (Hz, ascending, log-spaced or not), dirs (degrees FROM, evenly spaced round the full circle, in
either sense: a ValueError otherwise), E[nf, nd] in m2/Hz/degr (nan =
exception = treated as 0). The spectrum is smoothed (3x3, wrapping in direction), every bin follows its steepest-ascent
neighbour to a local maximum (8 neighbours, wrap in direction, clamp in frequency) and the bins of one maximum form a
partition. Two adjacent partitions can be merged when the pass between them is shallow (the highest boundary value at
least SADDLE x the lower of the two peaks; off by default, SADDLE = 1.0); a partition holding under SMALL_FRACTION of the energy and under SMALL_HS
is merged into the neighbour it touches most, so every joule stays in some partition. Per partition: Hs = 4 sqrt(m0),
the peak period from a parabolic fit of the partition's frequency spectrum on log f, the direction = the energy-weighted
circular mean of the FROM directions. The result lists the partitions by height, at most MAX_PARTS (the bulletins' six),
with the count before the cap and the whole spectrum's Hs. Partitions under MIN_HS are not listed (and not counted);
their energy stays in the whole spectrum's Hs.

Ties: NOAA's spectra are integers x a factor, so equal values are common. The smoothed field and the frequency
spectra are compared after rounding each value's mantissa to TIE_BITS bits, so sums equal but for float rounding are
equal: a plateau's bins go to its highest flat index (one peak per plateau; which bin is
arbitrary), and of two equal frequency peaks the lower frequency (the longer period) is the peak.
"""
import math

import numpy as np

SADDLE = 1.0            # merge two peaks when the pass between them is at least this share of the lower peak; 1.0 = never
                        # (measured at 51003 vs NOAA's GFS tables: no merging matched their systems best, 242 of 315)
SMALL_FRACTION = 0.02   # a partition under this share of the energy AND ...
SMALL_HS = 0.15         # ... under this height (m) is merged into its biggest neighbour
MAX_PARTS = 6
MIN_HS = 0.05           # partitions under this height are left out of the list (they hold no wave to speak of)
TIE_BITS = 40           # mantissa bits kept before comparing (see Ties above; a double has 52)


def bin_widths(freqs):
    """Frequency bin widths from the midpoints between neighbours; the end bins get the inner half twice."""
    f = np.asarray(freqs, float)
    if f.size == 1:
        return np.array([f[0]])
    mids = (f[1:] + f[:-1]) / 2
    lo = np.concatenate(([f[0] - (mids[0] - f[0])], mids))
    hi = np.concatenate((mids, [f[-1] + (f[-1] - mids[-1])]))
    return hi - lo


def _smooth(E):
    """3x3 mean with the directions wrapping and the frequency edges repeated."""
    P = np.pad(E, ((1, 1), (0, 0)), mode="edge")
    P = np.concatenate([P[:, -1:], P, P[:, :1]], axis=1)
    nf, nd = E.shape
    out = np.zeros_like(E)
    for i in range(3):
        for j in range(3):
            out += P[i:i + nf, j:j + nd]
    return out / 9.0


def _quantize(a):
    """a with each value's binary mantissa rounded to TIE_BITS bits: values equal but for float rounding become equal
    (bar the rare pair straddling a rounding boundary); small values keep their size, zeros stay zero."""
    m, e = np.frexp(a)
    return np.ldexp(np.round(m * 2.0 ** TIE_BITS) / 2.0 ** TIE_BITS, e)


def _labels(S):
    """The watershed labels of S: each bin's peak (a flat index), -1 where S is 0."""
    nf, nd = S.shape
    idx = np.arange(nf * nd).reshape(nf, nd)
    best_val = S.copy()
    best_idx = idx.copy()
    for di in (-1, 0, 1):
        ii = np.clip(np.arange(nf) + di, 0, nf - 1)
        for dj in (-1, 0, 1):
            if di == 0 and dj == 0:
                continue
            jj = (np.arange(nd) + dj) % nd
            N = S[ii][:, jj]
            nidx = idx[ii][:, jj]
            # ties (a flat top, as in a symmetric synthetic spectrum) go to the higher index, so every plateau has one peak
            take = (N > best_val) | ((N == best_val) & (nidx > best_idx))
            best_val = np.where(take, N, best_val)
            best_idx = np.where(take, nidx, best_idx)
    up = best_idx.ravel()
    lab = up.copy()
    for _ in range(nf * nd):                # follow the pointers, one step a round, until every bin sits on its peak
        nxt = up[lab]
        if np.array_equal(nxt, lab):
            break
        lab = nxt
    lab[S.ravel() <= 0] = -1
    return lab.reshape(nf, nd)


def _adjacent(lab):
    """Counts of boundary bin pairs {(a, b): n} and the highest pass {(a, b): max over pairs of min(S)} are built by the
    caller; here the neighbour pairs (a, b), a < b, with the bins on each side (wrap in direction)."""
    nf, nd = lab.shape
    pairs = []
    for di, dj in ((0, 1), (1, 0), (1, 1), (1, -1)):
        ii = np.arange(nf - di if di else nf)
        src = lab[ii]
        jj = (np.arange(nd) + dj) % nd
        dst = lab[ii + di][:, jj]
        m = (src >= 0) & (dst >= 0) & (src != dst)
        pairs.append((src[m], dst[m], ii[:, None].repeat(nd, 1)[m], np.tile(np.arange(nd), (ii.size, 1))[m],
                      (ii[:, None] + di).repeat(nd, 1)[m], np.tile(jj, (ii.size, 1))[m]))
    return pairs


def partition(freqs, dirs, E, max_parts=MAX_PARTS):
    f = np.asarray(freqs, float)
    d = np.asarray(dirs, float) % 360.0
    E = np.nan_to_num(np.asarray(E, float), nan=0.0, posinf=0.0, neginf=0.0)
    E = np.where(E < 0, 0.0, E)
    nf, nd = E.shape
    if f.size != nf or d.size != nd:
        raise ValueError("shape mismatch")
    step = np.diff(np.concatenate([d, d[:1]])) % 360.0
    if not (np.allclose(step, 360.0 / nd, atol=1e-6) or np.allclose(step, 360.0 - 360.0 / nd, atol=1e-6)):
        raise ValueError("directions must go evenly round the full circle")
    df = bin_widths(f)
    dd = 360.0 / nd
    W = E * df[:, None] * dd              # energy per bin (m2)
    total = float(W.sum())
    hs_all = 4.0 * math.sqrt(total) if total > 0 else 0.0
    if total <= 0:
        return {"hs": 0.0, "n": 0, "parts": []}

    S = _quantize(_smooth(E))
    lab = _labels(S)
    # merge shallow passes and small partitions until stable
    while True:
        ids = [int(x) for x in np.unique(lab) if x >= 0]
        if len(ids) <= 1:
            break
        m0 = {p: float(W[lab == p].sum()) for p in ids}
        peak = {p: float(S[lab == p].max()) for p in ids}
        touch = {}
        passes = {}
        for src, dst, i1, j1, i2, j2 in _adjacent(lab):
            lo = np.minimum(S[i1, j1], S[i2, j2])
            for a, b, v in zip(src.tolist(), dst.tolist(), lo.tolist()):
                k = (a, b) if a < b else (b, a)
                touch[k] = touch.get(k, 0) + 1
                if v > passes.get(k, -1.0):
                    passes[k] = v
        merged = False
        # shallow passes first, the shallowest (highest relative pass) first
        cands = sorted(((passes[k] / min(peak[k[0]], peak[k[1]]), k) for k in passes if min(peak[k[0]], peak[k[1]]) > 0),
                       reverse=True)
        for ratio, (a, b) in cands:
            if SADDLE < 1.0 and ratio >= SADDLE:
                keep, go = (a, b) if m0[a] >= m0[b] else (b, a)
                lab[lab == go] = keep
                merged = True
                break
        if merged:
            continue
        # small partitions into the neighbour they touch most
        small = [p for p in ids if m0[p] < SMALL_FRACTION * total and 4 * math.sqrt(m0[p]) < SMALL_HS]
        for p in sorted(small, key=lambda q: m0[q]):
            nb = {}
            for (a, b), n in touch.items():
                if a == p:
                    nb[b] = nb.get(b, 0) + n
                elif b == p:
                    nb[a] = nb.get(a, 0) + n
            if nb:
                keep = _merge_target(nb, m0)
                lab[lab == p] = keep
                merged = True
                break
        if not merged:
            break

    parts = []
    th = np.deg2rad(d)[None, :]
    for p in [int(x) for x in np.unique(lab) if x >= 0]:
        mask = lab == p
        Wp = np.where(mask, W, 0.0)
        m0 = float(Wp.sum())
        if m0 <= 0:
            continue
        hs = 4.0 * math.sqrt(m0)
        e1 = Wp.sum(axis=1) / df           # the partition's frequency spectrum (m2/Hz)
        k = int(np.argmax(_quantize(e1)))   # the first (lowest-frequency) of equal peaks
        fp = _parabolic_peak(f, e1, k)
        wx = float((Wp * np.cos(th)).sum())
        wy = float((Wp * np.sin(th)).sum())
        mdir = math.degrees(math.atan2(wy, wx)) % 360.0
        if mdir >= 360.0:                    # -1e-15 % 360 is 360.0
            mdir = 0.0
        parts.append({"hs": hs, "tp": 1.0 / fp, "dir": mdir, "m0": m0, "share": m0 / total})
    parts = [p for p in parts if p["hs"] >= MIN_HS]
    parts.sort(key=lambda p: -p["hs"])
    n = len(parts)
    return {"hs": hs_all, "n": n, "parts": parts[:max_parts]}


def _merge_target(touching, m0):
    """The neighbour a small partition joins: the one it shares the most boundary with, the bigger one on a tie."""
    return max(touching, key=lambda q: (touching[q], m0[q]))


def _parabolic_peak(f, e1, k):
    """The peak frequency from a parabola through log(e1) at the peak bin and its neighbours, on log f (the bins are
    log-spaced; a peaked spectrum is near-Gaussian there, so the log-parabola is exact for one); the bin itself at the
    ends, when a neighbour is empty, or when the fit has no maximum."""
    if 0 < k < f.size - 1 and e1[k] > 0 and e1[k - 1] > 0 and e1[k + 1] > 0:
        x = np.log(f[k - 1:k + 2])
        y = np.log(e1[k - 1:k + 2])
        denom = (x[0] - x[1]) * (x[0] - x[2]) * (x[1] - x[2])
        a = (x[2] * (y[1] - y[0]) + x[1] * (y[0] - y[2]) + x[0] * (y[2] - y[1])) / denom
        b = (x[2] ** 2 * (y[0] - y[1]) + x[1] ** 2 * (y[2] - y[0]) + x[0] ** 2 * (y[1] - y[2])) / denom
        if a < 0:
            xv = -b / (2 * a)
            if x[0] <= xv <= x[2]:
                return float(np.exp(xv))
    return float(f[k])


# ---------------------------------------------------------------- partition_v2 (plan section 77)
#
# The split of the site's "GFS Spectral" table, measured on NOAA's own station spectra (plan sections 76a-76e): no
# smoothing in the swell band (below SMOOTH_F), the sea band smoothed 3x3 before the watershed; touching pieces within
# DMAX degrees and LNR (log-ratio) in peak period rejoin when the pass between them is at least PASS_MIN of the lower
# peak; pieces under FRAG_HS and FRAG_SHARE of the energy fold into the touching piece nearest in (period, direction)
# within FRAG_DMAX / FRAG_LNR, a piece under TINY_HS with no such neighbour into the biggest one it touches; with the
# station's wind, pieces whose energy lies mostly inside the wind-sea region of the spectrum (phase speed below
# 1.7 x U10 cos(direction - wind)) and within WS_WIND_DMAX of the wind join one another within WS_DMAX. The constants
# are the measured settings of section 76e (the scratch e76_settings.json), pinned by a test.
V2 = dict(dmax=30.0, lnr=0.12, pass_min=0.6, frag_hs=0.25, frag_share=0.05, frag_dmax=45.0, frag_lnr=0.30,
          tiny_hs=0.15, ws_dmax=45.0, ws_wind_dmax=60.0, smooth_f=0.20)
RULES = {"version": "v2", "smoothing": "3x3 above smooth_f Hz only", "rejoin": "within dmax deg and lnr in ln(Tp) "
         "across a pass >= pass_min x the lower peak", "fragments": "under frag_hs m and frag_share of the energy "
         "join the nearest touching piece within frag_dmax / frag_lnr; under tiny_hs the biggest touching piece",
         "wind_sea": "pieces mostly inside 1.7 U10 cos(dir - wind) > c and within ws_wind_dmax of the wind join "
         "within ws_dmax of each other", "settings": V2}


def _ang(a, b):
    return abs((a - b + 180.0) % 360.0 - 180.0)


def _piece(mask, S, W, Wm, th):
    Wp = np.where(mask, W, 0.0)
    e1 = Wp.sum(axis=1)
    Sp = np.where(mask, S, -1.0)
    flat = int(np.argmax(Sp))
    return {"m0": float(Wp.sum()), "peak": float(Sp.max()), "e1": e1, "pk": flat,
            "wx": float((Wp * np.cos(th)).sum()), "wy": float((Wp * np.sin(th)).sum()),
            "m0w": float(Wp[Wm].sum()) if Wm is not None else 0.0}


def _derive(p, f, df, nd):
    """hs, tp (from the first of equal peaks of the piece's frequency spectrum), dir, pi, pj of a piece record."""
    e1 = p["e1"] / df
    k = int(np.argmax(_quantize(e1)))
    return {"hs": 4.0 * math.sqrt(p["m0"]) if p["m0"] > 0 else 0.0, "tp": 1.0 / f[k], "kf": k,
            "dir": math.degrees(math.atan2(p["wy"], p["wx"])) % 360.0, "pi": p["pk"] // nd, "pj": p["pk"] % nd}


def _pieces(lab, S, W, Wm, th, f, df):
    """Per label: the piece record (+ derived values); touch counts and the highest pass per touching pair."""
    nd = lab.shape[1]
    info = {}
    for p in [int(x) for x in np.unique(lab) if x >= 0]:
        rec = _piece(lab == p, S, W, Wm, th)
        rec.update(_derive(rec, f, df, nd))
        info[p] = rec
    touch, passes = {}, {}
    for src, dst, i1, j1, i2, j2 in _adjacent(lab):
        lo = np.minimum(S[i1, j1], S[i2, j2])
        for a, b, v in zip(src.tolist(), dst.tolist(), lo.tolist()):
            k = (a, b) if a < b else (b, a)
            touch[k] = touch.get(k, 0) + 1
            if v > passes.get(k, -1.0):
                passes[k] = v
    return info, touch, passes


def _fold(lab, info, touch, passes, keep, go, f, df):
    """Merge piece `go` into `keep` in place: the labels, the records (sums added, the higher peak kept), and the
    pair tables (counts added, passes the higher of the two), so no recompute over the grid is needed."""
    nd = lab.shape[1]
    lab[lab == go] = keep
    a, b = info[keep], info.pop(go)
    a["m0"] += b["m0"]
    a["m0w"] += b["m0w"]
    a["e1"] = a["e1"] + b["e1"]
    a["wx"] += b["wx"]
    a["wy"] += b["wy"]
    if b["peak"] > a["peak"] or (b["peak"] == a["peak"] and b["pk"] > a["pk"]):
        a["peak"], a["pk"] = b["peak"], b["pk"]
    a.update(_derive(a, f, df, nd))
    for tab, fold in ((touch, lambda x, y: x + y), (passes, max)):
        for (p, q) in [k for k in tab if go in k]:
            v = tab.pop((p, q))
            other = q if p == go else p
            if other == keep:
                continue
            k = (keep, other) if keep < other else (other, keep)
            tab[k] = fold(tab[k], v) if k in tab else v


def _merge_pair(lab, info, touch, passes, a, b, f, df):
    keep, go = (a, b) if (info[a]["m0"], -a) >= (info[b]["m0"], -b) else (b, a)
    _fold(lab, info, touch, passes, keep, go, f, df)


def partition_v2(freqs, dirs, E, wind=None, **opts):
    """-> (parts, labels): parts = [{"hs", "tp", "dir", "m0", "ws", "label"}] by height, every piece with energy;
    labels = the watershed field after the merges. `wind` = (u10 m/s, direction FROM deg) or None. Deterministic:
    ties in every choice break by label."""
    o = dict(V2, **opts)
    f = np.asarray(freqs, float)
    d = np.asarray(dirs, float) % 360.0
    E = np.nan_to_num(np.asarray(E, float), nan=0.0, posinf=0.0, neginf=0.0)
    E = np.where(E < 0, 0.0, E)
    nf, nd = E.shape
    if f.size != nf or d.size != nd:
        raise ValueError("shape mismatch")
    df = bin_widths(f)
    W = E * df[:, None] * (360.0 / nd)
    total = float(W.sum())
    if total <= 0:
        return [], None
    Es = E.copy()
    if o["smooth_f"] > 0:
        sm = _smooth(E)
        band = f >= o["smooth_f"]
        Es[band] = sm[band]
    S = _quantize(Es)
    lab = _labels(S)
    th = np.deg2rad(d)[None, :]
    Wm = None
    ud = None
    if wind is not None and np.isfinite(wind[0]) and np.isfinite(wind[1]) and wind[0] > 0:
        u, ud = float(wind[0]), float(wind[1])
        c = 9.81 / (2 * math.pi * f)[:, None]
        Wm = 1.7 * u * np.cos(np.radians(d[None, :] - ud)) > c
    info, touch, passes = _pieces(lab, S, W, Wm, th, f, df)
    while len(info) > 1:
        # the rejoin: the closest pair in (direction, period) across a shallow enough pass
        best = None
        for (a, b), pv in passes.items():
            A, B = info[a], info[b]
            da = _ang(A["dir"], B["dir"])
            dl = abs(math.log(A["tp"] / B["tp"]))
            if da <= o["dmax"] and dl <= o["lnr"]:
                lowpk = min(A["peak"], B["peak"])
                if lowpk > 0 and pv >= o["pass_min"] * lowpk:
                    key = (da / o["dmax"] + dl / o["lnr"], a, b)
                    if best is None or key < best:
                        best = key
        if best:
            _merge_pair(lab, info, touch, passes, best[1], best[2], f, df)
            continue
        # fragments: the smallest first
        merged = False
        frags = sorted((p for p in info if info[p]["hs"] < o["frag_hs"] and info[p]["m0"] < o["frag_share"] * total),
                       key=lambda q: (info[q]["m0"], q))
        for p in frags:
            nb = {}
            for (a, b), n in touch.items():
                if a == p:
                    nb[b] = nb.get(b, 0) + n
                elif b == p:
                    nb[a] = nb.get(a, 0) + n
            if not nb:
                continue
            P = info[p]
            near = None
            for q in nb:
                da = _ang(P["dir"], info[q]["dir"])
                dl = abs(math.log(P["tp"] / info[q]["tp"]))
                if da <= o["frag_dmax"] and dl <= o["frag_lnr"]:
                    key = (da / o["frag_dmax"] + dl / o["frag_lnr"], q)
                    if near is None or key < near:
                        near = key
            if near:
                _fold(lab, info, touch, passes, near[1], p, f, df)
                merged = True
                break
            if P["hs"] < o["tiny_hs"]:
                _fold(lab, info, touch, passes, _merge_target(nb, {q: info[q]["m0"] for q in nb}), p, f, df)
                merged = True
                break
        if not merged:
            break
    if Wm is not None:
        while len(info) > 1:
            wsp = {p for p, r in info.items() if r["m0"] > 0 and r["m0w"] >= 0.5 * r["m0"]
                   and _ang(r["dir"], ud) <= o["ws_wind_dmax"]}
            cand = [(_ang(info[a]["dir"], info[b]["dir"]), a, b) for (a, b) in touch
                    if a in wsp and b in wsp and _ang(info[a]["dir"], info[b]["dir"]) <= o["ws_dmax"]]
            if not cand:
                break
            _, a, b = min(cand)
            _merge_pair(lab, info, touch, passes, a, b, f, df)
    parts = []
    for p in sorted(info):
        r = info[p]
        if r["m0"] <= 0:
            continue
        e1 = r["e1"] / df
        fp = _parabolic_peak(f, e1, r["kf"])
        mdir = r["dir"]
        if mdir >= 360.0:
            mdir = 0.0
        parts.append({"hs": r["hs"], "tp": 1.0 / fp, "dir": mdir, "m0": r["m0"], "label": p,
                      "ws": bool(Wm is not None and r["m0w"] >= 0.5 * r["m0"])})
    parts.sort(key=lambda q: (-q["hs"], q["label"]))
    return parts, lab


def pieces_recomputed(lab, S, W, Wm, th, f, df):
    """A fresh computation of the piece tables (the tests compare them with the folded ones)."""
    return _pieces(lab, S, W, Wm, th, f, df)
