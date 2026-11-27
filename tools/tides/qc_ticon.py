"""Clock check of the TICON gauges against the GOT5.5 ocean-tide model (plan section 112). Developer tool.

    python -I tools/tides/qc_ticon.py --got GOT5.5/extrapolated [--out tools/tides/ticon_qc.json]

Some GESLA records carry a clock error (a local-time series taken as UTC): Hong Kong's TICON gauge runs 58 min
early against the Hong Kong Observatory's own tables, Chatham's 46 min (the islands' UTC+12:45). A clock error shifts
EVERY constituent by the same time, i.e. each phase lag by speed x error. For each gauge: the time shift that best
explains its M2, S2, N2, K1 and O1 phase lags against GOT5.5 (NASA GSFC, Ray 2024; anonymous; the extrapolated grids,
1/8 deg) at the same point, weighted by amplitude, and how well one shift explains them all.

Estuaries, fjords and harbours genuinely run late against an offshore model cell: on 238 gauges whose truth is known
(an agency's own table within 2 km agrees with TICON within 5 min) the shift against GOT5.5 has a median of 5 min but
a 95th percentile of 61 min, so a plain limit would drop good gauges. The rule kept is the clock error's signature: a
shift within HOUR_TOL_MIN of a whole number of hours (not zero), the semidiurnal and diurnal bands agreeing within
BAND_AGREE_MIN. It flags Hong Kong (-59.6 min; the Observatory's table: -58.4) and gauges that look like local time
taken as UTC (Esbjerg +56, Split +61, the L9 platform +52); of 3 flagged gauges with an agency's table near, Brisbane
Bar and Narvik are correct (real estuary / fjord lag), both hidden by the agency's station anyway. Smaller clock errors
(e.g. 20-45 min) are not seen by this check.
"""
import argparse
import json
import math
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))
OUT = os.path.join(HERE, "ticon_qc.json")
SPEED = {"m2": 28.9841042, "s2": 30.0, "n2": 28.4397295, "k1": 15.0410686, "o1": 13.9430356}   # deg / hour
HOUR_TOL_MIN = 8.0
BAND_AGREE_MIN = 10.0
MIN_AMP_M = 0.03


def read_got(path):
    import h5py
    with h5py.File(path, "r") as h:
        lat = np.asarray(h["latitude"][:], dtype=float)           # GOT's "lat" / "lon" are bare dimension indexes
        lon = np.asarray(h["longitude"][:], dtype=float) % 360.0
        amp = np.asarray(h["amplitude"][:], dtype=float)
        pha = np.asarray(h["phase"][:], dtype=float)
    bad = (amp > 1e30) | ~np.isfinite(amp)
    amp = np.where(bad, np.nan, amp / 100.0)
    pha = np.where(bad, np.nan, pha)
    return lat, lon, amp * np.exp(-1j * np.radians(pha))          # complex, built once per grid


def at(grid, plat, plon):
    """The complex constituent at a point: bilinear over the 4 cells when all have values, else the nearest valid
    cell within 3 cells, else None."""
    lat, lon, z = grid
    i = (plat - lat[0]) / (lat[1] - lat[0])
    j = ((plon % 360.0) - lon[0]) / (lon[1] - lon[0])
    i0, j0 = int(math.floor(i)), int(math.floor(j))
    n = len(lon)
    if not (0 <= i0 < len(lat) - 1):
        return None
    c = [z[i0, j0 % n], z[i0 + 1, j0 % n], z[i0, (j0 + 1) % n], z[i0 + 1, (j0 + 1) % n]]
    if all(np.isfinite(v) for v in c):
        fi, fj = i - i0, j - j0
        return c[0] * (1 - fi) * (1 - fj) + c[1] * fi * (1 - fj) + c[2] * (1 - fi) * fj + c[3] * fi * fj
    best = None
    for a in range(max(0, i0 - 3), min(len(lat), i0 + 5)):
        for b in range(j0 - 3, j0 + 5):
            v = z[a, b % n]
            if np.isfinite(v):
                d = (a - i) ** 2 + (b - j) ** 2
                if best is None or d < best[0]:
                    best = (d, v)
    return best[1] if best else None


def shift(gauge_consts, model):
    """(shift minutes, semidiurnal shift, diurnal shift) explaining the gauge's phase lags against the model's, or None
    when too few constituents are big enough in both."""
    rows = []
    for name, sp in SPEED.items():
        g = gauge_consts.get(name)
        m = model.get(name)
        if g is None or m is None or g[0] < MIN_AMP_M or abs(m) < MIN_AMP_M:
            continue
        dphi = (g[1] - math.degrees(-np.angle(m)) + 180.0) % 360.0 - 180.0         # gauge lag - model lag
        rows.append((name, dphi / sp * 60.0, min(g[0], abs(m))))
    if len(rows) < 2:
        return None
    w = np.array([r[2] for r in rows])
    dt = np.array([r[1] for r in rows])
    semi = [r for r in rows if r[0] in ("m2", "s2", "n2")]
    di = [r for r in rows if r[0] in ("k1", "o1")]
    band = lambda rr: (sum(r[1] * r[2] for r in rr) / sum(r[2] for r in rr)) if rr else None
    return float((dt * w).sum() / w.sum()), band(semi), band(di)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--got", required=True)
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--report", help="write every gauge's shifts (CSV) here")
    args = ap.parse_args()
    d = json.load(open(os.path.join(ROOT, "tide_global.json"), encoding="utf-8"))
    f = d["fields"]
    names = d["constituents"]
    grids = {c: read_got(os.path.join(args.got, "%sx.nc" % c)) for c in SPEED}
    flagged, rows = {}, []
    for r in d["stations"]:
        s = dict(zip(f, r))
        gc = {names[i]: (a / 10000.0, p / 10.0) for i, a, p in s["c"]}
        model = {c: at(grids[c], s["lat"], s["lon"]) for c in SPEED}
        res = shift(gc, model)
        if res is None:
            continue
        tot, semi, di = res
        rows.append((s["id"], tot, semi, di))
        consistent = semi is not None and di is not None and abs(semi - di) <= BAND_AGREE_MIN
        k = round(tot / 60.0)
        if k != 0 and abs(tot - 60.0 * k) <= HOUR_TOL_MIN and consistent:
            flagged[s["id"]] = round(tot, 1)
    with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
        json.dump({"model": "GOT5.5 extrapolated (NASA GSFC)", "hour_tol_min": HOUR_TOL_MIN,
                   "band_agree_min": BAND_AGREE_MIN, "flagged": flagged}, fh, indent=1, sort_keys=True)
    if args.report:
        with open(args.report, "w", encoding="utf-8") as fh:
            fh.write("id,shift,semi,di\n")
            for i, a, b, c in rows:
                fh.write("%s,%.1f,%s,%s\n" % (i, a, "" if b is None else "%.1f" % b, "" if c is None else "%.1f" % c))
    print("checked %d gauges, flagged %d" % (len(rows), len(flagged)))


if __name__ == "__main__":
    main()
