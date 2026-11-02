"""The WAVEWATCH III point-spectra reader (plan section 77): the real members, its conventions, hostile bodies."""
import json
import math
import os
import sys

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import gfs_winds as W  # noqa: E402
import ww3spec  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "gfswinds")
EXPECT = json.load(open(os.path.join(FIX, "gfsspec_expect.json")))


def member(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return f.read()


@pytest.mark.parametrize("name", sorted(EXPECT))
def test_a_real_member_reads_with_its_header_and_every_step(name):
    sp = ww3spec.read(member(name))
    exp = EXPECT[name]
    assert (len(sp.freqs), len(sp.dirs)) == (exp["nf"], exp["nd"]) == (50, 36)
    assert len(sp.steps) == len(exp["steps"])
    assert np.all(np.diff(sp.freqs) > 0) and sp.freqs[0] == pytest.approx(0.035)
    for (when, E, u, ud), e in zip(sp.steps, exp["steps"]):
        assert when.strftime("%Y%m%d%H") == e["time"]
        assert E.shape == (50, 36) and np.all(E >= 0)
        assert (u, ud) == (e["u10"], e["udir"])


def test_directions_decode_to_the_10_degree_grid_in_the_files_order_from_true_north():
    sp = ww3spec.read(member("gfswave_13130_member.spec"))
    # the file prints 1.48, 1.31, ... rad "going to": 84.8, 75.0, ... deg; FROM = +180, so 264.8 -> 265, 255, ...
    assert sp.dirs[:3].tolist() == [265.0, 255.0, 245.0] == EXPECT["gfswave_13130_member.spec"]["dirs_first3"]
    steps = np.diff(np.concatenate([sp.dirs, sp.dirs[:1]])) % 360.0
    assert np.all(steps == 350.0)                       # one sense, a constant 10-degree step, the circle once
    assert sorted(sp.dirs.tolist()) == [5.0 + 10 * k for k in range(36)]
    # not re-sorted: the energy columns stay beside the directions they were printed with
    assert sp.dirs[0] != 5.0


def test_the_total_height_equals_the_bulletins_within_two_percent_per_degree_density():
    """E is per degree (the file's per-radian values x pi/180) in [frequency, direction] order: a missing scale or a
    transposed reshape fails this (the bin widths vary with frequency, so a transpose changes the sum)."""
    for name, exp in EXPECT.items():
        sp = ww3spec.read(member(name))
        checked = 0
        for (when, E, u, ud), e in zip(sp.steps, exp["steps"]):
            if e["hst_bulletin"]:
                assert ww3spec.hs_of(sp.freqs, sp.dirs, E) == pytest.approx(e["hst_bulletin"], rel=0.02)
                checked += 1
        assert checked >= 6
    # the transpose would differ by more than that on a real spectrum
    sp = ww3spec.read(member("gfswave_13130_member.spec"))
    when, E, u, ud = sp.steps[0]
    bad = E.ravel().reshape(36, 50).T                  # the same values read as if directions varied fastest
    wrong = ww3spec.hs_of(sp.freqs, sp.dirs, bad)
    assert abs(wrong - EXPECT["gfswave_13130_member.spec"]["steps"][0]["hst_bulletin"]) > 0.02 * 1.8


def test_the_point_line_winds_equal_the_winds_jobs_parser_at_every_step():
    for name in EXPECT:
        data = member(name)
        sp = ww3spec.read(data)
        winds = W.spec_point_winds(data)
        assert len(winds) == len(sp.steps)
        for when, E, u, ud in sp.steps:
            assert winds[when] == (u, ud)


def _synthetic(nf=50, nd=36, steps=2, point=True, dirs_deg=None, values=None):
    freqs = 0.035 * 1.07 ** np.arange(nf)
    if dirs_deg is None:
        step = 360.0 / nd                                          # WW3's grid: half a step off zero, descending
        dirs_deg = (step / 2 - step * np.arange(nd)) % 360.0       # (85, 75, ... for 36): the file's sense, TO
    lines = [f"'WAVEWATCH III SPECTRA'     {nf:2d}    {nd:2d}     1 'spectral resolution for points'"]
    lines += [" ".join(f"{v:.3E}" for v in freqs[i:i + 8]) for i in range(0, nf, 8)]
    rad = np.radians(dirs_deg)
    lines += [" ".join(f"{v:.3E}" for v in rad[i:i + 7]) for i in range(0, nd, 7)]
    for s in range(steps):
        lines.append(f"20261101 {12 + s:02d}0000")
        if point:
            lines.append("'51201     '  21.67-158.12     472.7   8.09  71.6   0.05 139.8")
        vals = values if values is not None else [f"{1e-3 * (k % 7 + 1):.3E}" for k in range(nf * nd)]
        lines += [" ".join(vals[i:i + 7]) for i in range(0, len(vals), 7)]
    return "\n".join(lines).encode() + b"\n"


def test_a_synthetic_file_round_trips_its_values_frequency_fastest():
    nf, nd = 4, 6
    vals = [f"{(k + 1) * 1e-3:.3E}" for k in range(nf * nd)]
    sp = ww3spec.read(_synthetic(nf, nd, steps=1, dirs_deg=(30.0 + 60.0 * np.arange(nd)) % 360.0, values=vals))
    when, E, u, ud = sp.steps[0]
    assert when.strftime("%Y%m%d%H%M") == "202611011200" and (u, ud) == (8.09, 71.6)
    # value k sits at direction k // nf, frequency k % nf; per degree = per radian x pi/180
    for k in range(nf * nd):
        assert E[k % nf, k // nf] == pytest.approx((k + 1) * 1e-3 * math.pi / 180.0, rel=1e-6)
    assert sp.dirs.tolist() == [210.0, 270.0, 330.0, 30.0, 90.0, 150.0]


def test_a_step_without_its_point_line_or_with_too_few_numbers():
    with pytest.raises(ValueError, match="no point line"):
        ww3spec.read(_synthetic(4, 6, steps=1, point=False))
    data = _synthetic(4, 6, steps=1).replace(b"472.7   8.09  71.6   0.05 139.8", b"472.7")
    sp = ww3spec.read(data)
    assert math.isnan(sp.steps[0][2]) and math.isnan(sp.steps[0][3])


@pytest.mark.parametrize("damage", ["short", "count", "junk", "magic", "header", "nosteps", "grid", "twice", "inf"])
def test_hostile_bodies_raise_value_error(damage):
    good = _synthetic(4, 6, steps=2)
    if damage == "short":
        bad = good[:len(good) - 40]
    elif damage == "count":
        bad = good.replace(b"'     ", b"'     ", 1).replace(b"'WAVEWATCH III SPECTRA'      4", b"'WAVEWATCH III SPECTRA'      5")
    elif damage == "junk":
        bad = good.replace(b"3.000E-03", b"3.0x0E-03", 1)
    elif damage == "magic":
        bad = b"'WAVEWATCH II SPECTRA'" + good[len(b"'WAVEWATCH III SPECTRA'"):]
    elif damage == "header":
        bad = good.replace(b"3.500E-02", b"", 1)
    elif damage == "nosteps":
        bad = good.split(b"20261101 120000")[0]
    elif damage == "grid":
        bad = _synthetic(4, 6, steps=1, dirs_deg=np.array([30.0, 90.0, 150.0, 210.0, 270.0, 300.0]))
    elif damage == "twice":
        bad = _synthetic(4, 6, steps=1, dirs_deg=np.array([30.0, 90.0, 150.0, 210.0, 270.0, 30.0]))
    else:
        bad = good.replace(b"3.000E-03", b"3.000E+999", 1)
    with pytest.raises(ValueError):
        ww3spec.read(bad)


def test_limits_on_size_and_step_count():
    with pytest.raises(ValueError, match="too large"):
        ww3spec.read(b"'WAVEWATCH III SPECTRA' 50 36 1 'x'\n" + b" " * ww3spec.MAX_MEMBER)
    with pytest.raises(ValueError, match="steps"):
        ww3spec.read(_synthetic(4, 6, steps=ww3spec.MAX_STEPS + 1))
    with pytest.raises(ValueError):
        ww3spec.read("text, not bytes")
