"""Plan section 112: the harmonic tide predictor (tide_harmonic.py) against pyTMD and against NOAA's own predictions."""
import copy
import json
import math
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import tide_harmonic as H  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "tides_global")
NOAA = os.path.join(FIX, "noaa")


def _ep(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp()


def test_table_shapes_and_names():
    tab = H.table()
    assert len(tab.names) >= 80 and tab.coef.shape == (7, len(tab.names))
    for n in ("m2", "s2", "k1", "o1", "m4", "ms4", "mn4", "sa", "ssa", "3n2", "2mo5", "r3", "t3", "eps2"):
        assert tab.has(n), n
    for alias in ("LAM2", "RHO", "SGM", "EP2", "LAMBDA2"):
        assert tab.has(alias), alias
    assert tab.coef[:, tab.index["m2"]].tolist() == [2, 0, 0, 0, 0, 0, 0]
    assert np.all(np.diff(tab.mjd) > 0)


def test_mean_longitudes_at_j2000():
    s, h, p, n, ps = H.mean_longitudes(np.array([51544.5]))
    assert abs(float(s[0]) - 218.3164477) < 1e-6 and abs(float(p[0]) - 83.3532465) < 1e-6
    assert abs(float(n[0]) - 125.04452) < 1e-6 and abs(float(h[0]) % 360.0 - 280.46626) < 1e-4


def test_predict_equals_pytmd_golden():
    g = json.load(open(os.path.join(FIX, "pytmd_golden.json")))
    prep = H.prepare(g["consts"])
    assert prep[3] == []
    got = H.predict(prep, np.array(g["epochs"]))
    assert np.abs(got - np.array(g["heights"])).max() < 0.002          # m, over 2020-2049 with a 14 m range


def _noaa_table():
    """The table with NOAA's practice: nodal factors frozen at the middle of the year predicted (2026-07-02)."""
    tab = H.table()
    t2 = copy.deepcopy(tab)
    mid = datetime(2026, 7, 2, 12, tzinfo=timezone.utc).timestamp() / 86400.0 + 40587.0
    for c in range(len(t2.names)):
        t2.f[:, c] = np.interp(mid, tab.mjd, tab.f[:, c])
        t2.u[:, c] = np.interp(mid, tab.mjd, tab.u[:, c])
    return t2


@pytest.mark.parametrize("sid,name", [("1612340", "Honolulu"), ("9410170", "San Diego"), ("8443970", "Boston"),
                                      ("9447130", "Seattle")])
def test_noaa_gate(sid, name):
    """NOAA's predictions from NOAA's own constants, with NOAA's conventions (SA = the Sun's longitude, nodal factors
    at mid-year; NOAA's M1 is a different constituent, speed 14.4967 deg/h, so it is left out): RMS < 2 cm, every high
    / low found within 10 min and 5 cm."""
    hc = json.load(open(os.path.join(NOAA, "harcon_%s.json" % sid)))["HarmonicConstituents"]
    tab = _noaa_table()
    prep = H.prepare([(c["name"], c["amplitude"], c["phase_GMT"]) for c in hc if c["amplitude"] > 0 and c["name"] != "M1"],
                     tab, convention="noaa")
    assert prep[3] == []
    pr = json.load(open(os.path.join(NOAA, "pred_h_%s.json" % sid)))["predictions"]
    t = np.array([_ep(p["t"]) for p in pr])
    v = np.array([float(p["v"]) for p in pr])
    mine = H.predict(prep, t, tab)
    z0 = (v - mine).mean()
    d = v - mine - z0
    assert np.sqrt((d ** 2).mean()) < 0.02, name
    t6 = np.arange(t[0], t[-1] + 1, 360.0)
    ex = H.extremes(t6, H.predict(prep, t6, tab) + z0)
    hilo = json.load(open(os.path.join(NOAA, "pred_hilo_%s.json" % sid)))["predictions"]
    inner = [e for e in hilo if t6[0] + 3600 < _ep(e["t"]) < t6[-1] - 3600]
    for e in inner:
        te, he = _ep(e["t"]), float(e["v"])
        best = min(ex, key=lambda x: abs(x[0] - te))
        assert best[2] == e["type"][0], (name, e)
        assert abs(best[0] - te) <= 600 and abs(best[1] - he) <= 0.05, (name, e, best)
    assert abs(len([x for x in ex if t6[0] + 3600 < x[0] < t6[-1] - 3600]) - len(inner)) <= 1


def test_extremes_of_a_pure_cosine_are_exact():
    period = 12.42 * 3600
    t = np.arange(0, 3 * 86400, 360.0)
    y = 1.5 * np.cos(2 * math.pi * (t - 1000.0) / period)
    ex = H.extremes(t, y)
    highs = [e for e in ex if e[2] == "H"]
    assert abs(highs[0][0] - 1000.0) < 20 and abs(highs[1][0] - (1000.0 + period)) < 20
    assert abs(highs[0][1] - 1.5) < 0.002
    kinds = [e[2] for e in ex]
    assert kinds[0] == "H" and all(a != b for a, b in zip(kinds, kinds[1:]))


def test_extremes_flat_top_counts_once():
    t = np.arange(0, 10 * 360.0, 360.0)
    y = np.array([0, 1, 2, 3, 3, 2, 1, 0, 1, 2], dtype=float)
    ex = H.extremes(t, y)
    assert [e[2] for e in ex] == ["H", "L"]


def test_unknown_names_dropped_and_duplicates_kept_once():
    idx, amp, ph, dropped = H.prepare([("M2", 1.0, 10.0), ("XYZ9", 0.2, 0.0), ("m2", 5.0, 0.0), ("3L2", 0.01, 0)])
    assert dropped == ["XYZ9", "3L2"] and amp.tolist() == [1.0]


def test_outside_the_nodal_table_raises():
    prep = H.prepare([("M2", 1.0, 0.0)])
    with pytest.raises(ValueError):
        H.predict(prep, np.array([datetime(2055, 1, 1, tzinfo=timezone.utc).timestamp()]))


def test_lowest_astronomical_tide_of_m2_alone():
    prep = H.prepare([("M2", 1.0, 0.0)])
    lat = H.lowest_astronomical_tide(prep, datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
    fmax = H.table().f[:, H.table().index["m2"]].max()
    assert -fmax - 0.005 <= lat <= -fmax + 0.005           # the M2 node factor peaks at ~1.037


@pytest.mark.parametrize("gauge", ["alameda-9414750-usa-noaa", "annapolis-8575512-usa-noaa", "apra_harbor-1630000-usa-noaa",
                                   "aguadilla-9759412-usa-noaa", "adak_island-9461380-usa-noaa"])
def test_ticon_constants_reproduce_noaa_predictions(gauge):
    """End to end: TICON-4's constants (cm) at NOAA gauges, predicted here with convention 'ticon', against NOAA's own
    predictions for 2026-12: RMS under 3 cm (measured 0.8-2.7 cm). With pyTMD's SA instead the RMS grows (Annapolis
    1.6 -> 3.1 cm): TICON defines SA as NOAA does."""
    d = json.load(open(os.path.join(FIX, "ticon_noaa_pairs.json")))[gauge]
    prep = H.prepare([(n, a / 100.0, g) for n, a, g in d["consts"] if a > 0], convention="ticon")
    t = np.array([_ep(x[0]) for x in d["noaa_pred"]])
    v = np.array([x[1] for x in d["noaa_pred"]])
    r = v - H.predict(prep, t)
    r -= r.mean()
    assert np.sqrt((r ** 2).mean()) < 0.03, gauge


def test_ticon_sa_convention_matters():
    d = json.load(open(os.path.join(FIX, "ticon_noaa_pairs.json")))["annapolis-8575512-usa-noaa"]
    t = np.array([_ep(x[0]) for x in d["noaa_pred"]])
    v = np.array([x[1] for x in d["noaa_pred"]])
    out = {}
    for conv in ("ticon", "fes"):
        r = v - H.predict(H.prepare([(n, a / 100.0, g) for n, a, g in d["consts"] if a > 0], convention=conv), t)
        out[conv] = np.sqrt(((r - r.mean()) ** 2).mean())
    assert out["ticon"] < 0.02 < out["fes"]
