"""partition_v2 (plan section 77): the measured constants, parity with the scratch split on real spectra, the folded
piece tables against a recompute, determinism, synthetic systems, the direction convention."""
import json
import math
import os
import random
import sys

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import partition2d as P2  # noqa: E402
import ww3spec  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "gfswinds")
EXPECT = json.load(open(os.path.join(FIX, "gfsspec_expect.json")))


def member(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return ww3spec.read(f.read())


def ang(a, b):
    return abs((a - b + 180.0) % 360.0 - 180.0)


def test_the_constants_are_the_measured_settings_of_section_76e():
    """scratch e76_settings.json (GFS): dmax 30, lnr 0.12, pass_min 0.6, frag_hs 0.25, ws_dmax 45, smooth_f 0.2;
    the rest e76.py's DEFAULT. (e76.py's own DEFAULT had lnr 0.20 and smooth_f 0: the settings file is the truth.)"""
    assert P2.V2 == dict(dmax=30.0, lnr=0.12, pass_min=0.6, frag_hs=0.25, frag_share=0.05, frag_dmax=45.0,
                         frag_lnr=0.30, tiny_hs=0.15, ws_dmax=45.0, ws_wind_dmax=60.0, smooth_f=0.20)
    assert P2.RULES["settings"] is P2.V2 and P2.RULES["version"] == "v2"


@pytest.mark.parametrize("name", sorted(EXPECT))
def test_the_real_members_split_as_pinned(name):
    """The rows the scratch partitioner (section 76e) gave on these spectra, hour for hour: height 0.01 m, period
    0.1 s, direction 1 deg, the wind-sea flag."""
    sp = member(name)
    for (when, E, u, ud), e in zip(sp.steps, EXPECT[name]["steps"]):
        parts, lab = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud))
        got = [(p["hs"], p["tp"], p["dir"], p["ws"]) for p in parts]
        assert len(got) == len(e["parts"]), (when, got, e["parts"])
        for g, x in zip(got, e["parts"]):
            assert g[0] == pytest.approx(x[0], abs=0.005)
            assert g[1] == pytest.approx(x[1], abs=0.05)
            assert ang(g[2], x[2]) <= 0.5
            assert g[3] == x[3]
        assert sum(p["m0"] for p in parts) == pytest.approx((ww3spec.hs_of(sp.freqs, sp.dirs, E) / 4) ** 2, rel=1e-9)
        assert lab.shape == E.shape


def test_the_wind_sea_lies_with_the_wind_from_direction():
    """13130 at hour 0: U10 11.07 m/s FROM 49.5 deg; the flagged piece's direction is within 30 deg of it (a TO
    reading would be ~180 off)."""
    sp = member("gfswave_13130_member.spec")
    when, E, u, ud = sp.steps[0]
    parts, _ = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud))
    ws = [p for p in parts if p["ws"]]
    assert ws and ang(ws[0]["dir"], ud) <= 30.0 and ws[0]["tp"] < 9.0
    swell = [p for p in parts if not p["ws"]]
    assert swell and all(p["tp"] > 9.0 for p in swell)


def _gaussian(freqs, dirs, fp, dp, hs, sf=0.12, sd=25.0):
    f = np.asarray(freqs)[:, None]
    d = np.asarray(dirs)[None, :]
    dd = ((d - dp + 180.0) % 360.0) - 180.0
    E = np.exp(-0.5 * (np.log(f / fp) / sf) ** 2) * np.exp(-0.5 * (dd / sd) ** 2)
    df = P2.bin_widths(np.asarray(freqs))
    m0 = float((E * df[:, None] * (360.0 / len(dirs))).sum())
    return E * (hs / 4.0) ** 2 / m0


FREQS = 0.035 * 1.07 ** np.arange(50)
DIRS = (265.0 - 10.0 * np.arange(36)) % 360.0            # the files' order


def test_one_system_is_one_part_with_its_height_period_and_direction():
    E = _gaussian(FREQS, DIRS, 1 / 14.0, 320.0, 2.0)
    parts, _ = P2.partition_v2(FREQS, DIRS, E)
    assert len(parts) == 1
    p = parts[0]
    assert p["hs"] == pytest.approx(2.0, rel=0.01) and p["tp"] == pytest.approx(14.0, rel=0.03)
    assert ang(p["dir"], 320.0) < 1.0 and p["ws"] is False


def test_two_separated_systems_stay_two_and_a_close_pair_rejoins():
    E = _gaussian(FREQS, DIRS, 1 / 14.0, 320.0, 2.0) + _gaussian(FREQS, DIRS, 1 / 7.0, 40.0, 1.5)
    parts, _ = P2.partition_v2(FREQS, DIRS, E)
    assert [round(p["tp"]) for p in parts] == [14, 7]
    # two humps 20 deg and 5 % apart in period with a shallow pass between them: one system
    E2 = _gaussian(FREQS, DIRS, 1 / 14.0, 320.0, 2.0) + _gaussian(FREQS, DIRS, 1 / 13.3, 340.0, 1.9)
    parts2, _ = P2.partition_v2(FREQS, DIRS, E2)
    assert len(parts2) == 1
    assert parts2[0]["hs"] == pytest.approx(math.hypot(2.0, 1.9), rel=0.02)


def test_a_close_pair_across_a_deep_pass_stays_two():
    """The rejoin needs a shallow pass (pass_min): two narrow humps 28 deg apart at one period, with a deep valley
    between them, are two systems (G48a mutant P3: without the pass rule they would rejoin)."""
    E = _gaussian(FREQS, DIRS, 1 / 14.0, 305.0, 2.0, sd=4.0) + _gaussian(FREQS, DIRS, 1 / 14.0, 333.0, 1.9, sd=4.0)
    parts, _ = P2.partition_v2(FREQS, DIRS, E)
    assert len(parts) == 2 and sorted(round(p["dir"]) for p in parts) == [305, 334]
    one, _ = P2.partition_v2(FREQS, DIRS, E, pass_min=0.0)              # the rule switched off: one system
    assert len(one) == 1


def test_a_system_straddling_north_has_a_mean_near_north():
    E = _gaussian(FREQS, DIRS, 1 / 12.0, 2.0, 1.5, sd=30.0)
    parts, _ = P2.partition_v2(FREQS, DIRS, E)
    assert len(parts) == 1 and ang(parts[0]["dir"], 2.0) < 1.5


def test_a_speck_folds_into_its_neighbour_and_the_energy_is_kept():
    E = _gaussian(FREQS, DIRS, 1 / 14.0, 320.0, 2.0)
    E[30, 5] += 0.02 * E.max()                           # a one-bin bump in the sea band, far from the swell's peak
    parts, _ = P2.partition_v2(FREQS, DIRS, E)
    df = P2.bin_widths(FREQS)
    total = float((E * df[:, None] * 10.0).sum())
    assert sum(p["m0"] for p in parts) == pytest.approx(total, rel=1e-9)
    assert all(p["hs"] >= 0.25 for p in parts)


def test_the_wind_sea_rule_joins_wind_pieces_and_leaves_swell_alone():
    E = _gaussian(FREQS, DIRS, 1 / 14.0, 320.0, 2.0) + _gaussian(FREQS, DIRS, 1 / 5.0, 55.0, 1.0, sd=10.0) \
        + _gaussian(FREQS, DIRS, 1 / 3.5, 95.0, 0.8, sd=10.0)
    without, _ = P2.partition_v2(FREQS, DIRS, E)
    with_wind, _ = P2.partition_v2(FREQS, DIRS, E, wind=(12.0, 75.0))
    assert len(without) == 3 and len(with_wind) == 2
    ws = [p for p in with_wind if p["ws"]]
    assert len(ws) == 1 and ws[0]["hs"] == pytest.approx(math.hypot(1.0, 0.8), rel=0.02)
    assert [p for p in with_wind if not p["ws"]][0]["tp"] == pytest.approx(14.0, rel=0.03)
    # the wind limited to its own direction: a wind from the swell's side joins nothing across the circle
    other, _ = P2.partition_v2(FREQS, DIRS, E, wind=(12.0, 320.0))
    assert len(other) == 3


def test_determinism_and_empty_input():
    sp = member("gfswave_13130_member.spec")
    when, E, u, ud = sp.steps[3]
    a, la = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud))
    b, lb = P2.partition_v2(sp.freqs, sp.dirs, E.copy(), wind=(u, ud))
    assert a == b and np.array_equal(la, lb)
    assert P2.partition_v2(sp.freqs, sp.dirs, np.zeros_like(E)) == ([], None)
    with pytest.raises(ValueError):
        P2.partition_v2(sp.freqs[:-1], sp.dirs, E)


def _state(sp, k):
    when, E, u, ud = sp.steps[k]
    f, d = sp.freqs, sp.dirs
    df = P2.bin_widths(f)
    W = E * df[:, None] * 10.0
    Es = E.copy()
    sm = P2._smooth(E)
    Es[f >= 0.2] = sm[f >= 0.2]
    S = P2._quantize(Es)
    lab = P2._labels(S)
    th = np.deg2rad(d)[None, :]
    c = 9.81 / (2 * math.pi * f)[:, None]
    Wm = 1.7 * u * np.cos(np.radians(d[None, :] - ud)) > c
    return lab, S, W, Wm, th, f, df


def _same(info, touch, passes, info2, touch2, passes2):
    assert set(info) == set(info2) and touch == touch2 and set(passes) == set(passes2)
    for k in passes:
        assert passes[k] == passes2[k]
    for p in info:
        a, b = info[p], info2[p]
        for key in ("m0", "m0w", "wx", "wy", "peak", "hs", "dir", "tp"):
            assert a[key] == pytest.approx(b[key], rel=1e-9, abs=1e-12), (p, key)
        assert a["pk"] == b["pk"] and a["kf"] == b["kf"]
        assert np.allclose(a["e1"], b["e1"], rtol=1e-9, atol=1e-15)


def test_folded_piece_tables_equal_a_recompute_on_real_spectra():
    """After every merge the piece records and the pair tables are updated in place; on 46 real spectra a random
    sequence of merges leaves them equal to a fresh computation from the labels."""
    rng = random.Random(77)
    checked = 0
    for name in EXPECT:
        sp = member(name)
        for k in range(len(sp.steps)):
            lab, S, W, Wm, th, f, df = _state(sp, k)
            info, touch, passes = P2._pieces(lab, S, W, Wm, th, f, df)
            for _ in range(6):
                if len(info) < 2 or not touch:
                    break
                a, b = rng.choice(sorted(touch))
                keep, go = (a, b) if rng.random() < 0.5 else (b, a)
                P2._fold(lab, info, touch, passes, keep, go, f, df)
                info2, touch2, passes2 = P2.pieces_recomputed(lab, S, W, Wm, th, f, df)
                _same(info, touch, passes, info2, touch2, passes2)
                checked += 1
    assert checked >= 100
