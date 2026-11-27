"""Write tide_constituents.json, the table tide_harmonic.py predicts with (plan section 112). Developer tool: needs
pyTMD (MIT licence; `pip install pyTMD`), not the site.

    python -I tools/tides/make_constituent_table.py [extra constituent names ...]

For every constituent: Doodson's coefficients (tau, s, h, p, N, Ps, 90-degree multiples; pyTMD's coefficients_table
with corrections='FES') and the nodal modulation f / u (Schureman, `corrections='FES'`) on the 15th of every month
2010-2050, u unwrapped along time so linear interpolation never crosses a 2*pi jump. A name pyTMD does not know is
reported and left out.
"""
import json
import os
import sys

import numpy as np
import pyTMD.astro
import pyTMD.constituents as C

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "..", "tide_constituents.json")

# NOAA's 37, TICON-3/4's constituents, FES2022b's 34 (pyTMD names)
NAMES = [
    "sa", "ssa", "mm", "msf", "mf", "mt", "msqm", "mtm",
    "2q1", "sigma1", "q1", "rho1", "o1", "tau1", "beta1", "m1", "chi1", "pi1", "p1", "s1", "k1", "psi1", "phi1",
    "theta1", "j1", "so1", "oo1", "ups1",
    "eps2", "2n2", "mu2", "n2", "nu2", "gamma2", "alpha2", "m2", "beta2", "delta2", "lambda2", "l2", "t2", "s2",
    "r2", "k2", "msn2", "eta2", "mks2", "2sm2", "2mn2", "mns2", "mnus2", "2ms2", "snk2", "ma2", "mb2",
    "3n2",
    "mo3", "m3", "so3", "mk3", "sk3", "2mk3", "s3", "r3", "t3",
    "mn4", "m4", "sn4", "ms4", "mk4", "s4", "sk4", "n4", "2mn4", "3ms4",
    "2mk5", "2sk5", "m5", "2mo5",
    "2mn6", "m6", "msn6", "2ms6", "2mk6", "2sm6", "msk6", "s6",
    "3mk7", "m7", "m8", "s8",
]


def main(extra):
    names, skipped = [], []
    for n in NAMES + [e.lower() for e in extra]:
        if n in names:
            continue
        try:
            C.coefficients_table([n], corrections="FES")
            C.nodal_modulation(np.array([0.0]), np.array([0.0]), [n], corrections="FES")
            names.append(n)
        except Exception as exc:                     # pyTMD does not know it
            skipped.append("%s (%s)" % (n, type(exc).__name__))
    coef = np.asarray(C.coefficients_table(names, corrections="FES"), dtype=float)   # (7, nc)
    # "sa_h": the solar annual tide as NOAA (Schureman) and TICON define it, the Sun's mean longitude alone; pyTMD's
    # "sa" (Foreman / t_tide, used for the FES models) subtracts the solar perigee, ~283 deg apart. Checked 2026-11-27:
    # TICON constants at NOAA gauges match NOAA's predictions better with "sa_h" (Annapolis 3.1 -> 1.6 cm RMS).
    names.append("sa_h")
    coef = np.c_[coef, [0, 0, 1, 0, 0, 0, 0]]
    months = []
    for y in range(2010, 2051):
        for m in range(1, 13):
            months.append((y, m))
    mjd = np.array([_mjd(y, m, 15) for y, m in months], dtype=float)
    s, h, p, n_, ps = pyTMD.astro.mean_longitudes(mjd, method="ASTRO5")
    u, f = C.nodal_modulation(n_, p, names[:-1], corrections="FES")
    u = np.c_[np.unwrap(np.asarray(u, dtype=float), axis=0), np.zeros(len(mjd))]
    f = np.c_[np.asarray(f, dtype=float), np.ones(len(mjd))]
    doc = {
        "source": "pyTMD %s, corrections='FES' (Schureman nodal modulation), ASTRO5 mean longitudes" % _ver(),
        "names": names,
        "coef": np.round(coef, 6).tolist(),
        "mjd": mjd.tolist(),
        "f": np.round(f, 6).tolist(),
        "u": np.round(u, 6).tolist(),
    }
    with open(OUT, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, separators=(",", ":"))
    print("wrote %s: %d constituents, %d months" % (os.path.normpath(OUT), len(names), len(mjd)))
    if skipped:
        print("not known to pyTMD:", ", ".join(skipped))


def _mjd(y, m, d):
    a = (14 - m) // 12
    yy = y + 4800 - a
    mm = m + 12 * a - 3
    jdn = d + (153 * mm + 2) // 5 + 365 * yy + yy // 4 - yy // 100 + yy // 400 - 32045
    return jdn - 2400001 + 0.0          # MJD at 00:00 UTC


def _ver():
    try:
        import pyTMD
        return pyTMD.__version__
    except Exception:
        return "?"


if __name__ == "__main__":
    main(sys.argv[1:])
