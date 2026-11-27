"""Tide prediction from harmonic constants (plan section 112): numpy only.

h(t) = sum_i f_i(t) * A_i * cos(G_i(t) + u_i(t) - g_i)

* G_i: the equilibrium argument, Doodson's coefficients (tau, s, h, p, N, Ps, 90-degree multiples) times the
  astronomical mean longitudes (Meeus / ASTRO5 polynomials, as pyTMD and the FES models use);
* f_i, u_i: the nodal modulation (the 18.61-year lunar node cycle), Schureman's formulas as the FES models apply them,
  TABULATED once a month in `tide_constituents.json` by `tools/tides/make_constituent_table.py` (pyTMD, MIT licence,
  `corrections='FES'`) and interpolated linearly in time: they change by under 2 degrees a year;
* A_i, g_i: the station's amplitude (metres) and Greenwich phase lag (degrees).

The table also carries each constituent's Doodson coefficients, so the app needs no tide library. Checked against
NOAA's own predictions from NOAA's own constants and against pyTMD (tests/test_tide_harmonic.py).
"""
import json
import math
import os

import numpy as np

_MJD_UNIX = 40587.0                 # MJD of 1970-01-01T00:00Z
_MJD_J2000 = 51544.5
_CENTURY = 36525.0
TABLE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tide_constituents.json")

# other names for the same constituent (TICON, NOAA, FES) -> the table's name
ALIASES = {
    "lam2": "lambda2", "lda2": "lambda2", "la2": "lambda2", "lmb2": "lambda2",
    "rho": "rho1", "rh1": "rho1", "sig1": "sigma1", "sgm": "sigma1", "sigma": "sigma1",
    "the1": "theta1", "tta1": "theta1", "ups1": "upsilon1", "ksi": "xi1",
    "e2": "eps2", "ep2": "eps2", "epsilon2": "eps2",
    "oo1": "oo1", "ko1": "oo1", "msq": "msqm", "mtm": "mtm",
    "smn": "msn2", "2mk3": "2mk3", "mo3": "mo3",
}


def _norm(name):
    n = str(name).strip().lower().replace("_", "")
    return ALIASES.get(n, n)


def _poly(c, t):
    out = np.zeros_like(t, dtype=float)
    for k, ck in enumerate(c):
        out = out + ck * t ** k
    return out


def mean_longitudes(mjd):
    """(S, H, P, N, Ps) in degrees: Moon, Sun, lunar perigee, lunar ascending node (decreasing), solar perigee.
    Meeus' coefficients as in ASTRO5 (pyTMD `method='ASTRO5'`)."""
    t = (np.asarray(mjd, dtype=float) - _MJD_J2000) / _CENTURY
    lunar = [218.3164477, 481267.88123421, -1.5786e-3, 1.855835e-6, -1.53388e-8]
    elong = [297.8501921, 445267.1114034, -1.8819e-3, 1.83195e-6, -8.8445e-9]
    s = _poly(lunar, t)
    h = _poly([a - b for a, b in zip(lunar, elong)], t)
    p = _poly([83.3532465, 4069.0137287, -1.032e-2, -1.249172e-5], t)
    n = _poly([125.04452, -1934.136261, 2.0708e-3, 2.22222e-6], t)
    ps = 282.94 + 1.7192 * t
    return s, h, p, n, ps


class ConstituentTable:
    """The constituents' Doodson coefficients and monthly f / u (u in radians, unwrapped along time)."""

    def __init__(self, doc):
        self.names = list(doc["names"])
        self.index = {n: i for i, n in enumerate(self.names)}
        self.coef = np.asarray(doc["coef"], dtype=float)            # (7, nc)
        self.mjd = np.asarray(doc["mjd"], dtype=float)              # (nm,)
        self.f = np.asarray(doc["f"], dtype=float)                  # (nm, nc)
        self.u = np.asarray(doc["u"], dtype=float)                  # (nm, nc), radians, unwrapped
        if self.coef.shape != (7, len(self.names)) or self.f.shape != (len(self.mjd), len(self.names)) \
                or self.u.shape != self.f.shape:
            raise ValueError("tide constituent table shapes do not agree")

    def has(self, name):
        return _norm(name) in self.index

    def first_mjd(self):
        return float(self.mjd[0])

    def last_mjd(self):
        return float(self.mjd[-1])


_TABLE = None


def table():
    global _TABLE
    if _TABLE is None:
        with open(TABLE_PATH, encoding="utf-8") as fh:
            _TABLE = ConstituentTable(json.load(fh))
    return _TABLE


# Per-source naming of the constituents whose definition differs between conventions: TICON and NOAA define SA as
# the Sun's mean longitude alone (Schureman), the FES models as pyTMD does (Foreman: minus the solar perigee).
CONVENTIONS = {"ticon": {"sa": "sa_h"}, "noaa": {"sa": "sa_h"}, "fes": {}}


def prepare(consts, tab=None, convention="fes"):
    """[(name, amplitude m, phase deg), ...] -> (indexes, amplitudes, phases rad, dropped names). Duplicate names keep
    the first; a name the table does not know is dropped (returned, for the build's log). `convention`: the source's
    definitions (CONVENTIONS)."""
    tab = tab or table()
    remap = CONVENTIONS[convention]
    idx, amp, ph, dropped, seen = [], [], [], [], set()
    for name, a, g in consts:
        n = _norm(name)
        n = remap.get(n, n)
        if n not in tab.index:
            dropped.append(name)
            continue
        if n in seen:
            continue
        seen.add(n)
        idx.append(tab.index[n])
        amp.append(float(a))
        ph.append(math.radians(float(g)))
    return np.asarray(idx, dtype=int), np.asarray(amp), np.asarray(ph), dropped


def predict(prepared, epochs, tab=None):
    """Heights (m, about the constants' mean level) at the UNIX times `epochs` (seconds, UTC)."""
    tab = tab or table()
    idx, amp, ph = prepared[0], prepared[1], prepared[2]
    ep = np.atleast_1d(np.asarray(epochs, dtype=float))
    if idx.size == 0:
        return np.zeros_like(ep)
    mjd = ep / 86400.0 + _MJD_UNIX
    if mjd.min() < tab.first_mjd() or mjd.max() > tab.last_mjd():
        raise ValueError("time outside the nodal table (MJD %.0f-%.0f)" % (tab.first_mjd(), tab.last_mjd()))
    s, h, p, n, ps = mean_longitudes(mjd)
    tau = 15.0 * 24.0 * np.mod(mjd, 1.0) - s + h
    fargs = np.stack([tau, s, h, p, n, ps, np.full_like(mjd, 90.0)], axis=1)       # (T, 7)
    g = np.radians(fargs @ tab.coef[:, idx])                                          # (T, k)
    f = np.empty_like(g)
    u = np.empty_like(g)
    for j, c in enumerate(idx):
        f[:, j] = np.interp(mjd, tab.mjd, tab.f[:, c])
        u[:, j] = np.interp(mjd, tab.mjd, tab.u[:, c])
    return (f * amp * np.cos(g + u - ph)).sum(axis=1)


def extremes(epochs, heights):
    """High and low waters of a regular series: [(epoch, height, 'H'|'L')], each refined by the parabola through the
    sample and its two neighbours (time to the second, height to the mm)."""
    t = np.asarray(epochs, dtype=float)
    y = np.asarray(heights, dtype=float)
    if t.size < 3:
        return []
    step = t[1] - t[0]
    d = np.diff(y)
    out = []
    for i in range(1, y.size - 1):
        a, b = d[i - 1], d[i]
        if a > 0 >= b or a < 0 <= b:
            if a == 0 and b == 0:
                continue
            kind = "H" if a > 0 else "L"
            y0, y1, y2 = y[i - 1], y[i], y[i + 1]
            den = y0 - 2.0 * y1 + y2
            off = 0.5 * (y0 - y2) / den if den != 0 else 0.0
            off = max(-1.0, min(1.0, off))
            ht = y1 - 0.25 * (y0 - y2) * off
            out.append((float(t[i] + off * step), round(float(ht), 3), kind))
    # a flat top sampled twice gives two equal turning points of the same kind back to back: keep the first
    clean = []
    for e in out:
        if clean and clean[-1][2] == e[2]:
            if (e[2] == "H" and e[1] > clean[-1][1]) or (e[2] == "L" and e[1] < clean[-1][1]):
                clean[-1] = e
            continue
        clean.append(e)
    return clean


def lowest_astronomical_tide(prepared, start_epoch, years=18.61, step_s=3600.0, tab=None):
    """The lowest height of an hourly prediction over `years` from `start_epoch` (the LAT, below the constants' mean
    level: a negative number). Computed once per station by the build tools."""
    tab = tab or table()
    n = int(years * 365.2422 * 86400.0 / step_s)
    lo = None
    for a in range(0, n, 50000):
        ep = start_epoch + step_s * np.arange(a, min(n, a + 50000))
        v = float(predict(prepared, ep, tab).min())
        lo = v if lo is None else min(lo, v)
    return lo
