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
    the rest e76.py's DEFAULT. (e76.py's own DEFAULT had lnr 0.20 and smooth_f 0: the settings file is the truth.)
    The shoulder split's settings are the K1 study's pick (scratch swell76/k1study.py, variant P3d50s20)."""
    assert P2.V2 == dict(dmax=30.0, lnr=0.12, pass_min=0.6, frag_hs=0.25, frag_share=0.05, frag_dmax=45.0,
                         frag_lnr=0.30, tiny_hs=0.15, ws_dmax=45.0, ws_wind_dmax=60.0, smooth_f=0.20,
                         sub_fpow=3.0, sub_dmax=50.0, sub_lnr=0.2, sub_min_hs=0.3, sub_min_share=0.2, sub_ws_share=0.3)
    assert P2.RULES["settings"] is P2.V2 and P2.RULES["version"] == "v2.1" and "shoulder" in P2.RULES


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


def test_the_shoulder_split_finds_a_broad_sea_on_a_swells_flank():
    """Section 77 K1: a tall swell (14 s from 320) with a broad sea (9 s from 20, wide in period) on its flank under
    an 11 m/s wind from 60. On the raw grid the ridge rises straight from the sea to the swell's peak, so the plain
    watershed folds the two into one system with a smeared direction (333); the shoulder split keeps them apart,
    the sea flagged as wind sea. The parts' energies sum to the whole."""
    E = _gaussian(FREQS, DIRS, 1 / 14.0, 320.0, 2.0) + _gaussian(FREQS, DIRS, 1 / 9.0, 20.0, 1.2, sf=0.4, sd=40.0)
    plain, _ = P2.partition_v2(FREQS, DIRS, E, wind=(11.0, 60.0), sub_fpow=0.0)
    assert [p for p in plain if p["hs"] >= 0.3] and len([p for p in plain if p["hs"] >= 0.3]) == 1
    assert ang(plain[0]["dir"], 333.0) < 3.0
    parts, _ = P2.partition_v2(FREQS, DIRS, E, wind=(11.0, 60.0))
    big = [p for p in parts if p["hs"] >= 0.3]
    assert len(big) == 2
    swell, sea = big
    assert swell["tp"] == pytest.approx(14.0, rel=0.03) and ang(swell["dir"], 320.0) < 4.0 and not swell["ws"]
    assert 7.5 < sea["tp"] < 9.5 and ang(sea["dir"], 20.0) < 10.0 and sea["ws"]
    assert sea["hs"] == pytest.approx(1.2, abs=0.2)
    df = P2.bin_widths(FREQS)
    assert sum(p["m0"] for p in parts) == pytest.approx(float((E * df[:, None] * 10.0).sum()), rel=1e-9)


def test_the_shoulder_split_keeps_waimeas_merged_hour_apart_like_its_neighbours():
    """The owner's K1 finding: run 2026110118 at 51201, Monday 10 AM HST (+26 h) merged the NW groundswell and the
    NE sea that the hours before and after keep apart (NOAA's bulletin merges that hour too). The fixture holds
    hour 0 and hours +25, +26, +27: the plain watershed folds +26 into one part, the shoulder split gives every one
    of the three hours its two systems, and +26's lie between its neighbours'."""
    sp = member("gfswave_51201_member3.spec")
    assert [w.strftime("%d %H") for w, *_ in sp.steps] == ["01 18", "02 19", "02 20", "02 21"]
    when, E, u, ud = sp.steps[2]
    plain, _ = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud), sub_fpow=0.0)
    assert len([p for p in plain if p["hs"] >= 0.3]) == 1 and 2.7 < plain[0]["hs"] < 2.9
    rows = []
    for when, E, u, ud in sp.steps[1:]:
        parts, lab = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud))
        big = [p for p in parts if p["hs"] >= 0.3]
        assert len(big) == 2, (when, big)
        sea = [p for p in big if p["ws"]]
        swell = [p for p in big if not p["ws"]]
        assert len(sea) == 1 and len(swell) == 1, (when, big)
        assert 7.5 <= sea[0]["tp"] <= 9.5 and (sea[0]["dir"] <= 40 or sea[0]["dir"] >= 355), (when, sea)
        assert 13.5 <= swell[0]["tp"] <= 15.0 and 320 <= swell[0]["dir"] <= 335, (when, swell)
        assert sum(p["m0"] for p in parts) == pytest.approx((ww3spec.hs_of(sp.freqs, sp.dirs, E) / 4) ** 2, rel=1e-9)
        assert lab is not None and len(np.unique(lab[lab >= 0])) == len(parts)
        rows.append((sea[0], swell[0]))
    (s1, w1), (s2, w2), (s3, w3) = rows
    for a, b, c in ((s1, s2, s3), (w1, w2, w3)):            # the merged hour's systems between its neighbours'
        assert min(a["hs"], c["hs"]) - 0.3 <= b["hs"] <= max(a["hs"], c["hs"]) + 0.3
        assert abs(b["tp"] - a["tp"]) <= 1.5 and abs(b["tp"] - c["tp"]) <= 1.5
        assert ang(b["dir"], a["dir"]) <= 20 and ang(b["dir"], c["dir"]) <= 20


def test_the_shoulder_split_leaves_one_wind_sea_in_two_humps_alone_and_needs_distinct_peaks():
    """Two humps that both lie in the wind-sea region are one wind sea (sub_ws_share): the split must not divide a
    young sea by its own directional turning. And a sub-split needs the peaks far apart in direction AND period:
    the swell's own longer-period tail never splits off."""
    sea = _gaussian(FREQS, DIRS, 1 / 7.0, 40.0, 1.3, sf=0.3, sd=30.0) + _gaussian(FREQS, DIRS, 1 / 4.0, 70.0, 1.0, sf=0.3, sd=30.0)
    parts, _ = P2.partition_v2(FREQS, DIRS, sea, wind=(12.0, 55.0))
    assert len([p for p in parts if p["hs"] >= 0.3]) == 1 and parts[0]["ws"]
    # the same humps with no wind: nothing marks them as one wind sea; whatever the raw watershed gives, the
    # shoulder split adds no part whose peak is within sub_dmax of another's
    no_wind, _ = P2.partition_v2(FREQS, DIRS, sea)
    big = [p for p in no_wind if p["hs"] >= 0.3]
    assert len(big) <= 2
    swell = _gaussian(FREQS, DIRS, 1 / 14.0, 320.0, 2.0) + _gaussian(FREQS, DIRS, 1 / 10.0, 325.0, 1.0, sf=0.3, sd=25.0)
    parts, _ = P2.partition_v2(FREQS, DIRS, swell)
    assert len([p for p in parts if p["hs"] >= 0.3]) == 1


def test_the_shoulder_split_is_off_with_sub_fpow_zero_and_deterministic():
    sp = member("gfswave_51201_member3.spec")
    when, E, u, ud = sp.steps[2]
    a, la = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud))
    b, lb = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud))
    assert [(p["hs"], p["tp"], p["dir"], p["label"]) for p in a] == [(p["hs"], p["tp"], p["dir"], p["label"]) for p in b]
    assert np.array_equal(la, lb)
    off, _ = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud), sub_fpow=0.0)
    assert len(off) < len(a)


def test_a_shoulder_part_folds_into_its_nearest_part_not_the_biggest():
    """Mutant M14 of the shoulder split: a small f^3 sub-part folds into the touching part nearest in (direction,
    period), never into the biggest one. Run 2026110118 at 51201, +33 h (Mon 2 Nov 5 PM HST, one of the owner's K1
    hours): the NE sea's short-period tail is nearest the sea; folded into the groundswell instead, the sea would
    read 1.55 m from 13 deg and the swell 1.96 m from 345 (the merge the owner saw). The rule decides 221 of 9,240
    real hours (scratch spec77/k1clauses.py)."""
    sp = member("gfswave_51201_k1pins.spec")
    when, E, u, ud = sp.steps[1]
    assert when.strftime("%d %H") == "03 03"
    parts, _ = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud))
    big = [p for p in parts if p["hs"] >= 0.3]
    assert len(big) == 2, parts
    sea = [p for p in big if p["ws"]]
    swell = [p for p in big if not p["ws"]]
    assert len(sea) == 1 and len(swell) == 1, big
    assert sea[0]["hs"] > 1.75 and 8.0 <= sea[0]["tp"] <= 9.5 and 10 <= sea[0]["dir"] <= 35, sea
    assert swell[0]["hs"] < 1.8 and 13.0 <= swell[0]["tp"] <= 14.5 and 320 <= swell[0]["dir"] <= 335, swell


def test_the_wind_sea_veto_keeps_a_young_sea_in_one_piece():
    """Mutant M5 (sub_ws_share): two f^3 humps that both lie mostly inside the wind-sea region are one wind sea however
    far apart their peaks (a young sea turning with the wind is not two systems). Synthetic: 6-s and 4-s humps 60 deg
    apart under a 12 m/s wind between them. Real: run 2026110118 at 51201, +184 / +185 h, the 7-s NE trade sea stays
    one piece beside the 12-s NW swell; without the veto it reads a 7.5-s "swell" from 359 plus a 6-s sea (the veto
    decides 131 of 9,240 real hours)."""
    sea = _gaussian(FREQS, DIRS, 1 / 6.0, 30.0, 1.3, sf=0.3, sd=25.0) + _gaussian(FREQS, DIRS, 1 / 4.0, 90.0, 1.0, sf=0.3, sd=25.0)
    parts, _ = P2.partition_v2(FREQS, DIRS, sea, wind=(12.0, 60.0))
    assert len(parts) == 1 and parts[0]["ws"] and parts[0]["hs"] == pytest.approx(math.hypot(1.3, 1.0), rel=0.02)
    sp = member("gfswave_51201_k1pins.spec")
    assert [w.strftime("%d %H") for w, *_ in sp.steps] == ["01 18", "03 03", "09 10", "09 11"]
    for when, E, u, ud in sp.steps[2:]:
        parts, _ = P2.partition_v2(sp.freqs, sp.dirs, E, wind=(u, ud))
        ws = [p for p in parts if p["ws"]]
        assert len(ws) == 1 and 1.5 < ws[0]["hs"] < 1.8 and 6.8 <= ws[0]["tp"] <= 7.6 and 15 <= ws[0]["dir"] <= 35, (when, parts)
        assert all(p["hs"] < 1.2 or p["tp"] > 10.0 for p in parts if not p["ws"]), (when, parts)


def test_a_shoulder_part_needs_a_share_of_the_piece_and_a_height_of_its_own():
    """Mutants M8 / M9: the two floors are separate rules. With the height floor off, the share rule alone folds a
    0.5-m hump on a 2.5-m swell (4 % of the energy); with the share rule off, the height floor alone folds a 0.2-m
    hump on a 1-m swell (4 % too, and under 0.3 m)."""
    big = _gaussian(FREQS, DIRS, 1 / 14.0, 320.0, 2.5) + _gaussian(FREQS, DIRS, 1 / 9.0, 20.0, 0.5, sf=0.4, sd=40.0)
    parts, _ = P2.partition_v2(FREQS, DIRS, big, wind=(11.0, 60.0), sub_min_hs=0.0)
    assert len(parts) == 1, parts
    small = _gaussian(FREQS, DIRS, 1 / 14.0, 320.0, 1.0) + _gaussian(FREQS, DIRS, 1 / 9.0, 20.0, 0.2, sf=0.4, sd=40.0)
    parts, _ = P2.partition_v2(FREQS, DIRS, small, wind=(11.0, 60.0), sub_min_share=0.0)
    assert len(parts) == 1, parts


def test_a_piece_under_twice_the_height_floor_is_never_sub_split():
    """Mutant M13: the shoulder split skips a piece under 2 x sub_min_hs (two parts of sub_min_hs each cannot both lie
    in it with room to spare): a 0.57-m piece holding a 0.45-m swell and a broad 0.35-m sea stays one system; the
    same shapes at 2.0 m and 1.2 m are the K1 split above. The gate is the switch: lowered, the split happens."""
    E = _gaussian(FREQS, DIRS, 1 / 14.0, 320.0, 0.45) + _gaussian(FREQS, DIRS, 1 / 9.0, 20.0, 0.35, sf=0.5, sd=55.0)
    parts, _ = P2.partition_v2(FREQS, DIRS, E, wind=(11.0, 60.0))
    assert len(parts) == 1 and parts[0]["hs"] == pytest.approx(math.hypot(0.45, 0.35), rel=0.02), parts
    two, _ = P2.partition_v2(FREQS, DIRS, E, wind=(11.0, 60.0), sub_min_hs=0.2)
    assert len([p for p in two if p["hs"] >= 0.25]) == 2, two


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
