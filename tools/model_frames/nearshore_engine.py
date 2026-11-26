"""Nearshore analysis engine (plan section 109, stage 1): from an offshore 2-D spectrum and a SWAN transfer table to
breaking zones, break lines, peel angles and rides. Pure numpy (the publishers' environment has no scipy).

Grid: arrays are [ny, nx]; x runs alongshore, y offshore (row 0 is the shore side), spacing dx metres. The grid's x axis
points `alpc` degrees anticlockwise from east (SWAN's alpc), so y points offshore at alpc + 90.
Directions are nautical, degrees the waves come FROM.

Steps (one call to analyse()):
1. weights()      offshore energy per table bin, per swell train.
2. Table.apply()  E(x) = sum W K2: unbroken Hs and the energy-weighted wave direction, per train.
3. shelter()      marched shoreward along each column: breaking starts where the set-wave height reaches gamma x depth,
                  then the energy flux decays toward the stable height (Dally, Dean and Dalrymple 1985) until the waves
                  have reformed. Gives T = the share of the linear energy flux left (so waves that broke on an outer
                  reef arrive smaller inside) and the breaking zone. Nothing is taken off before a wave's own break
                  line, so the outermost line is the plain criterion the spike checked against satellite whitewater.
4. break_lines()  the zone's seaward edge as polylines (marching squares on the breaking margin), smoothed over a
                  share of the local wavelength: a wave cannot feel reef shapes much shorter than itself.
5. per vertex     peel angle (crest against break line; 0 = closeout), ride side, needed surfer speed, breaker type.
6. rides()        runs of one side between closeouts: length and a 0-100 score.

Heights: Hs is never compared with depth directly. The set wave is H1/10 = 1.27 Hs (Rayleigh); H1/100 = 1.67 Hs."""
import math

import numpy as np

G = 9.81
H10, H100 = 1.27, 1.67           # Rayleigh: mean of the highest tenth / hundredth over Hs

PARAMS = dict(
    version=1,
    gamma=0.9,                   # set-wave height / depth at the break line (bigwave's value for H1/10)
    stable=0.55,                 # set-wave height / depth the broken wave decays toward (Hs / depth about 0.43 on a reef flat)
    zone_off=0.60,               # the zone ends where set-wave height / depth falls under this (the waves have reformed)
    dally_k=0.15,                # Dally's decay constant
    crest_smooth_m=20.0,         # the breaking trigger is smoothed alongshore by this Gaussian sigma (a crest breaks as one)
    lateral_mix=0.25,            # sideways mixing of the flux share per row (0 = none; 0.25 = a 1-2-1 blur)
    wet=0.3,                     # water under this depth (m) is treated as dry
    margin_smooth_m=0.0,         # Gaussian sigma applied to the contoured field (0 = none; the line itself is smoothed)
    line_step_m=10.0,            # vertex spacing of a break line
    line_smooth_wl=0.2,          # Gaussian sigma along the line, as a share of the wavelength at breaking ...
    tangent_wl=0.25,             # ... and the tangent's half-baseline
    scale_m=(15.0, 60.0),        # both kept inside these metres
    min_line_m=30.0,             # shorter pieces are dropped
    bands=(15.0, 30.0, 50.0, 70.0),          # closeout | too fast | ideal | easy | soft shoulder
    closeout_gap_m=20.0,         # a closeout section longer than this ends a ride
    peel_score=((0, 0.0), (15, 0.0), (30, 0.6), (35, 1.0), (50, 1.0), (70, 0.6), (90, 0.3)),
    ride_full_m=150.0,           # ride length at which the length term reaches 1
    speed_base=8.0, speed_per_sqrt_h=3.0,    # makeable surfer speed (m/s) = base + k sqrt(breaking height, m)
    breaker_score=(0.7, 1.0, 0.4),           # spilling, plunging, collapsing
    reform_zero=0.5, reform_full=0.9,        # a wave that kept under / over this share of its energy flux scores 0 / in full
    slope_reach_m=50.0,          # the reef slope is measured over this distance seaward along the wave path
    mixed_share=0.4, mixed_deg=20.0,         # a second train this strong and this far off in direction = mixed swell
)
BAND_NAMES = ("closeout", "too fast", "ideal", "easy", "soft shoulder")
BREAKER_NAMES = ("spilling", "plunging", "collapsing")


# ---------------------------------------------------------------------------------------------------------------------
# table
class Table:
    """K2[nb, ny*nx] (energy gain per offshore bin) and the local wave direction per bin, on the surf-band grid.
    F (Hz) and D (deg FROM) list the bins as F x D in row order."""

    def __init__(self, F, D, K2, DIR, dep, dx, alpc):
        self.F = np.asarray(F, float); self.D = np.asarray(D, float)
        self.dep = np.asarray(dep, np.float32); self.ny, self.nx = self.dep.shape
        self.dx = float(dx); self.alpc = float(alpc)
        nb = len(self.F) * len(self.D)
        K2 = np.nan_to_num(np.asarray(K2, np.float32).reshape(nb, -1))
        r = np.radians(np.nan_to_num(np.asarray(DIR, np.float32).reshape(nb, -1)))
        self.K2 = K2; self.KS = K2 * np.sin(r); self.KC = K2 * np.cos(r)
        a = math.radians(self.alpc)
        self.ux = (math.cos(a), math.sin(a)); self.uy = (-math.sin(a), math.cos(a))   # grid axes as (east, north)

    @classmethod
    def from_runs(cls, root, dx, F, D, rows=None):
        """Development loader: the per-bin SWAN outputs of the model folder (runs/tab_<dx>m/f<f>_d<ddd>/hs.npy, dir.npy
        for a unit offshore Hs, so K2 = hs^2) and grid/grid_<dx>m.{npz,json}. rows = keep only the first `rows` rows."""
        import json
        import os
        g = np.load(os.path.join(root, "grid", f"grid_{dx:g}m.npz")); info = json.load(open(os.path.join(root, "grid", f"grid_{dx:g}m.json")))
        dep = g["dep"][:rows]; K2 = np.zeros((len(F) * len(D),) + dep.shape, np.float32); DIR = np.zeros_like(K2)
        for a, f in enumerate(F):
            for b, t in enumerate(D):
                d = os.path.join(root, "runs", f"tab_{dx:g}m", f"f{f:.4f}_d{int(t):03d}")
                hs = np.load(os.path.join(d, "hs.npy"))[:rows].astype(np.float32); hs[~(hs > 0)] = 0.0
                dr = np.load(os.path.join(d, "dir.npy"))[:rows].astype(np.float32); dr[~(dr > -900)] = 0.0
                K2[a * len(D) + b] = hs * hs; DIR[a * len(D) + b] = dr
        return cls(F, D, K2, DIR, dep, dx, info["alpc"])

    # ---- the packed table (nearshore_table.pack): K = sqrt(K2) on a log scale in one byte, the direction in one byte
    K_MIN, K_MAX = 0.02, 6.0

    @classmethod
    def encode_k(cls, K2):
        k = np.sqrt(np.maximum(np.nan_to_num(np.asarray(K2, np.float32)), 0.0))
        c = 1.0 + 254.0 * np.log(np.clip(k, cls.K_MIN, cls.K_MAX) / cls.K_MIN) / math.log(cls.K_MAX / cls.K_MIN)
        return np.where(k < cls.K_MIN / 2, 0, np.rint(c)).astype(np.uint8)

    @classmethod
    def decode_k2(cls, code):
        k = cls.K_MIN * (cls.K_MAX / cls.K_MIN) ** ((code.astype(np.float32) - 1.0) / 254.0)
        return np.where(code == 0, 0.0, k * k).astype(np.float32)

    @classmethod
    def from_npz(cls, path):
        z = np.load(path)
        t = cls(z["F"], z["D"], cls.decode_k2(z["K"]), z["DIR"].astype(np.float32) * (360.0 / 256.0), z["dep_cm"].astype(np.float32) / 100.0,
                float(z["dx"]), float(z["alpc"]))
        t.geo = dict(lon0=float(z["lon0"]), lat0=float(z["lat0"]), xpc=float(z["xpc"]), ypc=float(z["ypc"])); t.version = str(z["version"])
        return t

    def lonlat(self, x, y):
        """Grid metres (x alongshore, y offshore) -> lon, lat, on the local flat-earth frame the grid was built in."""
        g = self.geo; e = g["xpc"] + np.asarray(x) * self.ux[0] + np.asarray(y) * self.uy[0]; n = g["ypc"] + np.asarray(x) * self.ux[1] + np.asarray(y) * self.uy[1]
        ky = math.pi / 180.0 * 6371008.8; kx = ky * math.cos(math.radians(g["lat0"]))
        return g["lon0"] + e / kx, g["lat0"] + n / ky

    def xy(self, lon, lat):
        """The inverse of lonlat(): lon, lat -> grid metres (x alongshore, y offshore)."""
        g = self.geo; ky = math.pi / 180.0 * 6371008.8; kx = ky * math.cos(math.radians(g["lat0"]))
        de = (np.asarray(lon) - g["lon0"]) * kx - g["xpc"]; dn = (np.asarray(lat) - g["lat0"]) * ky - g["ypc"]
        return de * self.ux[0] + dn * self.ux[1], de * self.uy[0] + dn * self.uy[1]

    def apply(self, W):
        """W[nf, nd] energy (m2) per bin -> Hs[ny, nx] (m) and the mean direction FROM (deg)."""
        w = np.asarray(W, np.float32).reshape(-1)
        E = w @ self.K2; s = w @ self.KS; c = w @ self.KC
        hs = 4.0 * np.sqrt(np.maximum(E, 0.0)).reshape(self.ny, self.nx)
        return hs, (np.degrees(np.arctan2(s, c)) % 360.0).reshape(self.ny, self.nx)

    def travel(self, mdir):
        """Unit travel vector in grid axes (tx alongshore, ty offshore) from a direction-FROM field."""
        r = np.radians(mdir); te, tn = -np.sin(r), -np.cos(r)
        return te * self.ux[0] + tn * self.ux[1], te * self.uy[0] + tn * self.uy[1]


def bin_widths(freqs):
    f = np.asarray(freqs, float)
    if f.size == 1: return np.array([f[0]])
    mids = (f[1:] + f[:-1]) / 2
    return np.concatenate((mids, [2 * f[-1] - mids[-1]])) - np.concatenate(([2 * f[0] - mids[0]], mids))


def weights(freqs, dirs, E, F, D, mask=None, half_width=7.5):
    """Energy (m2) of a spectrum E[nf, nd] (m2/Hz/deg, dirs deg FROM on an even grid) allotted to the table's bins.
    Frequency: linear between the two neighbouring table frequencies (clamped at the ends). Direction: the nearest
    table direction; energy further than half_width from every table direction cannot reach the coast and is dropped.
    -> W[len(F), len(D)], and the share of the energy that was dropped."""
    f = np.asarray(freqs, float); d = np.asarray(dirs, float); E = np.nan_to_num(np.asarray(E, float))
    if mask is not None: E = np.where(mask, E, 0.0)
    F = np.asarray(F, float); D = np.asarray(D, float)
    dth = abs(((d[1] - d[0] + 180.0) % 360.0) - 180.0) if d.size > 1 else 360.0
    e = E * bin_widths(f)[:, None] * dth                                   # m2 per (f, theta) cell
    rel = np.abs(((d[:, None] - D[None, :] + 180.0) % 360.0) - 180.0)      # [nd, nD]
    near = rel.argmin(1); ok = rel.min(1) <= half_width + dth / 2 + 1e-9
    W = np.zeros((F.size, D.size)); total = e.sum(); kept = 0.0
    if F.size == 1:
        hi = lo = np.zeros(f.size, int); wf = np.zeros(f.size)
    else:
        hi = np.clip(np.searchsorted(F, f), 1, F.size - 1); lo = hi - 1
        wf = np.clip((f - F[lo]) / (F[hi] - F[lo]), 0.0, 1.0)
    for k in range(d.size):
        if not ok[k]: continue
        np.add.at(W[:, near[k]], lo, (1 - wf) * e[:, k]); np.add.at(W[:, near[k]], hi, wf * e[:, k]); kept += e[:, k].sum()
    return W, (1.0 - kept / total if total > 0 else 0.0)


def jonswap(hs, tp, th0, s=30.0, gam=5.0):
    """A single-peaked test swell: JONSWAP in frequency, cos^2s in direction. -> freqs, dirs, E (m2/Hz/deg)."""
    f = np.arange(0.03, 0.2501, 0.0025); fp = 1.0 / tp
    sig = np.where(f <= fp, 0.07, 0.09)
    S = f ** -5.0 * np.exp(-1.25 * (f / fp) ** -4.0) * gam ** np.exp(-((f - fp) ** 2) / (2 * sig ** 2 * fp ** 2))
    S *= (hs / 4.0) ** 2 / (S * bin_widths(f)).sum()
    d = np.arange(0.0, 360.0, 5.0)
    c = np.abs(np.cos(np.radians(((d - th0 + 180.0) % 360.0) - 180.0) / 2.0)) ** (2 * s); c /= c.sum() * 5.0
    return f, d, S[:, None] * c[None, :]


# ---------------------------------------------------------------------------------------------------------------------
# sheltering and the breaking zone
def shelter(hset_lin, h, dx, ty=None, p=PARAMS):
    """March every column shoreward. hset_lin = unbroken set-wave height, h = depth + tide, ty = offshore component of
    the unit travel vector (negative = shoreward; None = straight in).
    -> T[ny, nx] the share of the linear energy flux that arrives at each cell, and Z[ny, nx] the breaking zone.
    A column starts breaking where hset_lin sqrt(T) >= gamma h; while breaking, the flux relaxes toward the stable
    height (stable x h) over the distance h / dally_k; breaking stops under zone_off x h."""
    ny, nx = hset_lin.shape; T = np.ones((ny, nx), np.float32); Z = np.zeros((ny, nx), bool)
    cur = np.ones(nx); on = np.zeros(nx, bool); xs = np.arange(nx); mix = p["lateral_mix"]
    ds = dx / (np.clip(-ty, 0.5, 1.0) if ty is not None else np.ones((ny, nx)))
    sg = p["crest_smooth_m"] / dx; kc = None
    if sg > 0:
        r = max(1, int(math.ceil(3 * sg))); kc = np.exp(-0.5 * (np.arange(-r, r + 1) / sg) ** 2); kc /= kc.sum()
    for j in range(ny - 1, -1, -1):
        hj = h[j]; wet = hj > p["wet"]; hh = np.maximum(hj, 1e-3)
        # Land shadows are already in the table (SWAN saw the land). So water behind a dry cell does not start from
        # zero: it takes the share its wet neighbours in the row above carried, and the row is mixed sideways a
        # little each step (the columns are not rays; the sea's directional spread fills in behind a reef).
        ok = np.isfinite(cur)
        cur = np.interp(xs, xs[ok], cur[ok]) if ok.any() else np.zeros(nx)
        if mix > 0: cur = (1 - 2 * mix) * cur + mix * (np.concatenate((cur[:1], cur[:-1])) + np.concatenate((cur[1:], cur[-1:])))
        ratio = hset_lin[j] * np.sqrt(cur) / hh
        if kc is not None:                               # a crest breaks as one: the trigger is read along the row
            rr = np.pad(np.where(wet, ratio, 0.0), len(kc) // 2, mode="edge"); ww = np.pad(wet.astype(float), len(kc) // 2, mode="edge")
            ratio = np.where(wet, np.convolve(rr, kc, mode="valid") / np.maximum(np.convolve(ww, kc, mode="valid"), 1e-6), ratio)
        on = wet & ((ratio >= p["gamma"]) | (on & (ratio >= p["zone_off"])))
        T[j] = np.where(wet, cur, 0.0); Z[j] = on
        tst = np.minimum((p["stable"] * hh / np.maximum(hset_lin[j], 1e-6)) ** 2, 1.0)
        dec = tst + (cur - tst) * np.exp(-p["dally_k"] * ds[j] / hh)
        cur = np.where(on & (cur > tst), dec, cur)
        cur = np.where(wet, cur, np.nan)
    return T, Z


# ---------------------------------------------------------------------------------------------------------------------
# geometry helpers
def gauss2(a, sigma_cells):
    """Separable Gaussian blur with edge replication."""
    if sigma_cells <= 0: return a
    r = max(1, int(math.ceil(3 * sigma_cells))); x = np.arange(-r, r + 1); k = np.exp(-0.5 * (x / sigma_cells) ** 2); k /= k.sum()
    out = np.pad(a, ((r, r), (0, 0)), mode="edge"); out = sum(k[i] * out[i:i + a.shape[0]] for i in range(2 * r + 1))
    out = np.pad(out, ((0, 0), (r, r)), mode="edge"); return sum(k[i] * out[:, i:i + a.shape[1]] for i in range(2 * r + 1))


def sample(a, x, y, dx):
    """Bilinear sample of a[ny, nx] at metres (x, y), clamped to the grid."""
    ny, nx = a.shape; fx = np.clip(np.asarray(x, float) / dx, 0, nx - 1.001); fy = np.clip(np.asarray(y, float) / dx, 0, ny - 1.001)
    i = fx.astype(int); j = fy.astype(int); u = fx - i; v = fy - j
    return (a[j, i] * (1 - u) * (1 - v) + a[j, i + 1] * u * (1 - v) + a[j + 1, i] * (1 - u) * v + a[j + 1, i + 1] * u * v)


_CASES = {1: (("L", "B"),), 2: (("B", "R"),), 3: (("L", "R"),), 4: (("R", "T"),), 5: (("L", "T"), ("B", "R")), 6: (("B", "T"),),
          7: (("L", "T"),), 8: (("L", "T"),), 9: (("B", "T"),), 10: (("L", "B"), ("R", "T")), 11: (("R", "T"),), 12: (("L", "R"),),
          13: (("B", "R"),), 14: (("L", "B"),)}


def contours(M, dx):
    """Zero contours of M[ny, nx] as a list of [n, 2] arrays of (x, y) metres (marching squares, linear crossings)."""
    ny, nx = M.shape; P = M > 0
    code = (P[:-1, :-1] * 1 + P[:-1, 1:] * 2 + P[1:, 1:] * 4 + P[1:, :-1] * 8)
    jj, ii = np.nonzero((code > 0) & (code < 15))
    if not jj.size: return []
    nh = ny * nx

    def edge(side, j, i):                                # edge ids: horizontal (j, i)-(j, i+1), vertical (j, i)-(j+1, i)
        if side == "B": return j * nx + i
        if side == "T": return (j + 1) * nx + i
        if side == "L": return nh + j * nx + i
        return nh + j * nx + i + 1

    a, b = [], []
    cc = code[jj, ii]
    for c, pairs in _CASES.items():
        m = cc == c
        if not m.any(): continue
        for s0, s1 in pairs:
            a.append(edge(s0, jj[m], ii[m])); b.append(edge(s1, jj[m], ii[m]))
    a = np.concatenate(a); b = np.concatenate(b)
    ids = np.unique(np.concatenate((a, b)))
    horiz = ids < nh; e = np.where(horiz, ids, ids - nh); ej, ei = e // nx, e % nx
    v0 = M[ej, ei]; v1 = np.where(horiz, M[ej, np.minimum(ei + 1, nx - 1)], M[np.minimum(ej + 1, ny - 1), ei])
    t = np.clip(v0 / np.where(v0 == v1, 1.0, v0 - v1), 0.0, 1.0)
    px = (ei + np.where(horiz, t, 0.0)) * dx; py = (ej + np.where(horiz, 0.0, t)) * dx
    pos = {int(k): n for n, k in enumerate(ids)}
    nbr = {}
    for s, (u, w) in enumerate(zip(a.tolist(), b.tolist())):
        nbr.setdefault(u, []).append((w, s)); nbr.setdefault(w, []).append((u, s))
    used = np.zeros(a.size, bool); out = []
    for s in range(a.size):
        if used[s]: continue
        used[s] = True; chain = [int(a[s]), int(b[s])]
        for end in (1, 0):
            while True:
                tip = chain[-1] if end else chain[0]; nxt = None
                for w, s2 in nbr[tip]:
                    if not used[s2]: nxt = (w, s2); break
                if nxt is None: break
                used[nxt[1]] = True
                if end: chain.append(nxt[0])
                else: chain.insert(0, nxt[0])
        idx = np.array([pos[c] for c in chain]); out.append(np.column_stack((px[idx], py[idx])))
    return out


def resample(xy, step):
    """Polyline -> vertices every `step` metres along it (ends kept)."""
    d = np.hypot(*np.diff(xy, axis=0).T); s = np.concatenate(([0.0], np.cumsum(d)))
    if s[-1] < step: return xy
    t = np.linspace(0.0, s[-1], int(round(s[-1] / step)) + 1)
    return np.column_stack((np.interp(t, s, xy[:, 0]), np.interp(t, s, xy[:, 1])))


def smooth_line(xy, sigma_pts, closed=False):
    """Gaussian smoothing along an evenly resampled line; open ends are renormalised, closed loops wrap."""
    if sigma_pts <= 0 or len(xy) < 3: return xy
    r = max(1, int(math.ceil(3 * sigma_pts))); k = np.exp(-0.5 * (np.arange(-r, r + 1) / sigma_pts) ** 2)
    out = np.empty_like(xy)
    for c in range(2):
        if closed:
            out[:, c] = np.convolve(np.pad(xy[:, c], r, mode="wrap"), k / k.sum(), mode="valid")
        else:
            n = len(xy); out[:, c] = np.convolve(xy[:, c], k)[r:r + n] / np.convolve(np.ones(n), k)[r:r + n]
    return out


# ---------------------------------------------------------------------------------------------------------------------
# break lines, peel, rides
LINE_FIELDS = ("x", "y", "hb", "depth", "peel", "side", "speed", "xi", "breaker", "band", "ride", "ride_m", "score", "mixed", "line", "share")


def interp1(pairs, v):
    xs = np.array([q[0] for q in pairs], float); ys = np.array([q[1] for q in pairs], float)
    return np.interp(v, xs, ys)


def break_lines(M, Z, hset, h, tx, ty, dx, tp, p=PARAMS, mixed=None, T=None):
    """Seaward edges of the breaking zone as smoothed polylines with per-vertex values.
    M = breaking margin (set height - gamma x depth), Z = zone mask, tx/ty = unit travel vector of the dominant train,
    tp = that train's peak period (s). -> dict of arrays (LINE_FIELDS), one entry per vertex, `line` numbering the pieces."""
    step = p["line_step_m"]; out = {k: [] for k in LINE_FIELDS}; nline = 0; nride = 0
    # The field that is contoured: the breaking margin outside the zone (negative) and at its first cells (positive),
    # and a positive plateau through the rest of the zone, where the margin has fallen again as the wave decays. Its
    # zero line is the zone's edge; on the seaward side it sits where the margin itself crosses zero.
    Ms = np.where(Z, np.where(M > 0, M, 0.3), np.minimum(np.where(h > p["wet"], M, -p["gamma"]), -1e-3))
    Ms = gauss2(Ms, p["margin_smooth_m"] / dx)
    gy, gx = np.gradient(Ms, dx); Zf = Z.astype(np.float32)
    for xy in contours(Ms, dx):
        closed = len(xy) > 3 and np.hypot(*(xy[0] - xy[-1])) <= 1.5 * dx
        xy = resample(xy, step)
        if len(xy) < 3: continue
        x, y = xy[:, 0], xy[:, 1]
        ux, uy = sample(tx, x, y, dx), sample(ty, x, y, dx)
        onset = (sample(gx, x, y, dx) * ux + sample(gy, x, y, dx) * uy > 0) & \
                (sample(Zf, x - 1.5 * dx * ux, y - 1.5 * dx * uy, dx) < 0.5)   # margin rises along the path, and open water lies seaward
        hv = sample(h, x, y, dx); hv = hv[hv > p["wet"]]
        wl = tp * math.sqrt(G * float(np.median(hv))) if hv.size else p["scale_m"][0]      # wavelength at breaking
        xy = smooth_line(xy, float(np.clip(p["line_smooth_wl"] * wl, *p["scale_m"])) / step, closed)
        edges = np.flatnonzero(np.diff(np.concatenate(([False], onset, [False])).astype(int)))
        for a, b in zip(edges[::2], edges[1::2]):
            if (b - a - 1) * step < p["min_line_m"]: continue
            seg = xy[a:b]; n = len(seg); x, y = seg[:, 0], seg[:, 1]
            k = max(1, int(round(float(np.clip(p["tangent_wl"] * wl, *p["scale_m"])) / step)))
            lo = np.clip(np.arange(n) - k, 0, n - 1); hi = np.clip(np.arange(n) + k, 0, n - 1)
            tau = seg[hi] - seg[lo]; tau /= np.maximum(np.hypot(tau[:, 0], tau[:, 1]), 1e-9)[:, None]
            ux, uy = sample(tx, x, y, dx), sample(ty, x, y, dx)
            along = tau[:, 0] * ux + tau[:, 1] * uy                         # travel component along the line
            peel = np.degrees(np.arcsin(np.clip(np.abs(along), 0.0, 1.0)))
            # the break point runs along the line the way the wave travels; "right" = to the right of a surfer
            # facing shore (looking along the travel vector). In (x, y) the right of (ux, uy) is (uy, -ux).
            sgn = np.where(along >= 0, 1.0, -1.0); side = np.sign(sgn * (tau[:, 0] * uy - tau[:, 1] * ux))
            hb = np.maximum(sample(hset, x, y, dx), 0.0); dep = np.maximum(sample(h, x, y, dx), p["wet"])
            speed = np.sqrt(G * dep) / np.maximum(np.sin(np.radians(peel)), 0.05)
            reach = p["slope_reach_m"]; slope = (sample(h, x - reach * ux, y - reach * uy, dx) - dep) / reach
            xi = np.maximum(slope, 0.0) / np.sqrt(np.maximum(hb, 0.05) / (G * tp * tp / (2 * math.pi)))
            breaker = np.digitize(xi, (0.4, 2.0)); band = np.digitize(peel, p["bands"])
            # share = the part of the open-sea energy flux still in the wave as it reaches the line (1 = it has not
            # broken further out; low = a reform behind an outer reef)
            share = sample(T, x - 1.5 * dx * ux, y - 1.5 * dx * uy, dx) if T is not None else np.ones(n)
            rid, rlen, score = rides(peel, side, hb, speed, breaker, step, p, share)
            for key, val in (("x", x), ("y", y), ("hb", hb), ("depth", dep), ("peel", peel), ("side", side), ("speed", speed), ("xi", xi),
                             ("breaker", breaker), ("band", band), ("ride", np.where(rid >= 0, rid + nride, -1)), ("ride_m", rlen), ("score", score),
                             ("mixed", sample(mixed, x, y, dx) > 0.5 if mixed is not None else np.zeros(n, bool)), ("line", np.full(n, nline)), ("share", share)):
                out[key].append(val)
            nline += 1; nride += int(rid.max()) + 1 if rid.size and rid.max() >= 0 else 0
    return {k: (np.concatenate(v) if v else np.zeros(0)) for k, v in out.items()}


def rides(peel, side, hb, speed, breaker, step, p=PARAMS, share=None):
    """Split one break line into rides: runs of one side, ended by a closeout section longer than closeout_gap_m or a
    change of side. -> ride index per vertex (-1 = none), the ride's length (m) and its 0-100 score."""
    n = len(peel); rid = np.full(n, -1); rlen = np.zeros(n); score = np.zeros(n)
    close = peel < p["bands"][0]; gap = max(1, int(round(p["closeout_gap_m"] / step)))
    vmax = p["speed_base"] + p["speed_per_sqrt_h"] * np.sqrt(np.maximum(hb, 0.0))
    term = interp1(p["peel_score"], peel) * np.clip((1.5 * vmax - speed) / (0.5 * vmax), 0.0, 1.0) * np.asarray(p["breaker_score"])[breaker]
    if share is not None: term = term * np.clip((share - p["reform_zero"]) / (p["reform_full"] - p["reform_zero"]), 0.0, 1.0)
    k = 0; i = 0
    while i < n:
        if close[i] or side[i] == 0: i += 1; continue
        j = i; last = i; run = 0
        while j < n and side[j] in (side[i], 0) or (j < n and close[j]):
            if close[j]:
                run += 1
                if run > gap: break
            else:
                run = 0; last = j
            j += 1
        sl = slice(i, last + 1); m = ~close[sl]
        length = (last - i) * step
        if length >= step:
            rid[sl] = k; rlen[sl] = length
            score[sl] = 100.0 * float(term[sl][m].mean()) * min(length / p["ride_full_m"], 1.0); k += 1
        i = last + 1
    return rid, rlen, score


# ---------------------------------------------------------------------------------------------------------------------
def analyse(table, trains, tide=0.0, p=PARAMS, lines=True):
    """trains: list of dict(W=[nf, nd] energy per table bin, tp=peak period s), largest first or not.
    -> dict: hs (broken-wave Hs), hset (set height H1/10), T, zone, h, mdir (dominant train), lines (see break_lines)."""
    h = table.dep.astype(float) + tide
    per = [table.apply(t["W"]) for t in trains]
    e = np.stack([q[0] ** 2 for q in per]); etot = e.sum(0); hs_lin = np.sqrt(etot)
    dom = e.argmax(0); mdir = np.choose(dom, [q[1] for q in per]) if len(per) > 1 else per[0][1]
    tx, ty = table.travel(mdir)
    w = np.array([float(np.sum(t["W"])) for t in trains])
    T, Z = shelter(H10 * hs_lin, h, table.dx, ty, p)
    hs = hs_lin * np.sqrt(T); hset = H10 * hs
    res = dict(hs=hs, hs_lin=hs_lin, hset=hset, T=T, zone=Z, h=h, mdir=mdir, dom=dom)
    if lines:
        mixed = None
        if len(per) > 1:
            srt = np.sort(e, axis=0); second = srt[-2] / np.maximum(srt[-1], 1e-12)
            d2 = np.choose(np.argsort(e, axis=0)[-2], [q[1] for q in per])
            mixed = ((second >= p["mixed_share"]) & (np.abs(((d2 - mdir + 180) % 360) - 180) >= p["mixed_deg"])).astype(np.float32)
        tp = float(trains[int(np.argmax(w))]["tp"])
        res["lines"] = break_lines(hset - p["gamma"] * h, Z, hset, h, tx, ty, table.dx, tp, p, mixed, T)
    return res
