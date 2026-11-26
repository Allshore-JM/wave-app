"""The nearshore analysis engine (plan section 109, stage 1): analytic cases on made-up grids. No table file, no network."""
import math
import os
import sys

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import nearshore_engine as E  # noqa: E402

DX = 10.0


def beach(direction, ny=120, nx=80, slope=0.02, shore_row=10, depth=None):
    """A plane beach, x east and y north (offshore), one table bin with K2 = 1 and one wave direction everywhere."""
    y = (np.arange(ny) - shore_row) * DX
    dep = np.repeat((slope * y)[:, None], nx, axis=1) if depth is None else np.repeat(np.asarray(depth, float)[:, None], nx, axis=1)
    return E.Table([0.07], [0.0], np.ones((1, ny, nx)), np.full((1, ny, nx), float(direction)), dep, DX, 0.0)


def swell(hs):
    return [dict(W=np.array([[(hs / 4.0) ** 2]]), tp=14.0)]


def test_apply_returns_the_input_height_and_the_table_direction():
    tab = beach(330.0)
    hs, mdir = tab.apply(swell(2.0)[0]["W"])
    assert np.allclose(hs, 2.0, atol=1e-5) and np.allclose(mdir, 330.0, atol=1e-3)
    tx, ty = tab.travel(mdir)                              # from 330: travelling toward 150 = east and south
    assert tx[0, 0] == pytest.approx(math.sin(math.radians(30)), abs=1e-6) and ty[0, 0] == pytest.approx(-math.cos(math.radians(30)), abs=1e-6)


def test_weights_keep_the_energy_inside_the_span_and_drop_the_rest():
    F = [0.05, 0.0625, 0.0833, 0.125]; D = [300.0, 315.0, 330.0, 345.0, 0.0]
    f, d, S = E.jonswap(2.0, 14.0, 330.0)
    W, lost = E.weights(f, d, S, F, D)
    assert lost < 0.02 and 4 * math.sqrt(W.sum()) == pytest.approx(2.0, rel=0.02)
    assert W.sum(0).argmax() == 2                          # the 330 column holds the most
    W2, lost2 = E.weights(f, d, *[E.jonswap(2.0, 14.0, 150.0)[2]], F, D)
    assert lost2 > 0.98 and W2.sum() < 0.02 * W.sum()      # a swell heading offshore never reaches the coast
    half = np.zeros(S.shape, bool); half[: len(f) // 2] = True
    Wa, _ = E.weights(f, d, S, F, D, mask=half); Wb, _ = E.weights(f, d, S, F, D, mask=~half)
    assert np.allclose(Wa + Wb, W)                         # masks split the energy between swell trains


def test_straight_in_swell_closes_out_on_a_plane_beach_at_the_criterion_depth():
    tab = beach(0.0); r = E.analyse(tab, swell(1.5)); L = r["lines"]
    assert L["x"].size > 20 and int(L["line"].max()) == 0
    hb = E.H10 * 1.5; y_expect = (10 + hb / E.PARAMS["gamma"] / 0.02 / DX) * DX
    assert np.allclose(L["y"], y_expect, atol=DX)          # set height = gamma x depth
    assert np.all(L["peel"] < 2.0) and np.all(L["band"] == 0)
    assert np.allclose(L["hb"], hb, rtol=0.03) and np.all(L["ride"] == -1)
    assert not r["zone"][-1].any() and r["zone"][int(y_expect / DX) - 2].all()
    assert np.allclose(r["T"][int(y_expect / DX) + 3], 1.0)   # nothing is taken off seaward of the line


@pytest.mark.parametrize("direction, peel, side", [(35.0, 35.0, 1), (325.0, 35.0, -1), (45.0, 45.0, 1), (295.0, 65.0, -1), (20.0, 20.0, 1)])
def test_oblique_swell_peels_at_its_angle_to_the_beach(direction, peel, side):
    """On a straight beach the break line is shore-parallel, so the peel angle is the wave's angle to the shore normal.
    From the north-east the break runs west: to the right of a surfer facing the shore (south)."""
    L = E.analyse(beach(direction), swell(1.5))["lines"]
    assert L["x"].size > 20
    assert np.allclose(L["peel"], peel, atol=1.0) and np.all(L["side"] == side)
    assert np.all(L["band"] == np.digitize(peel, E.PARAMS["bands"]))
    assert L["ride_m"].max() >= 700 and np.all(L["ride"] == 0)   # one ride, the length of the beach
    c = math.sqrt(E.G * E.H10 * 1.5 / E.PARAMS["gamma"])
    assert np.allclose(L["speed"], c / math.sin(math.radians(peel)), rtol=0.05)


def test_tide_moves_the_break_line_shoreward():
    lo = E.analyse(beach(0.0), swell(1.5), tide=0.0)["lines"]["y"].mean()
    hi = E.analyse(beach(0.0), swell(1.5), tide=0.4)["lines"]["y"].mean()
    assert hi == pytest.approx(lo - 0.4 / 0.02, abs=DX)


def bar_depth(ny=160):
    """Offshore to shore: 12 m deep water, a reef at 2 m, a 7 m lagoon, then a beach."""
    d = np.full(ny, 12.0); d[90:100] = 2.0; d[40:90] = 7.0; d[:40] = np.linspace(-1.0, 7.0, 40); return d


def test_an_outer_reef_shelters_the_water_behind_it():
    tab = beach(0.0, ny=160, depth=bar_depth())
    r = E.analyse(tab, swell(2.0)); T, Z = r["T"][:, 5], r["zone"][:, 5]
    assert T[120] == 1.0 and Z[95] and not Z[120]                     # breaks on the reef, not before it
    assert 0.05 < T[60] < 0.6 and not Z[60]                           # reformed and smaller in the lagoon
    assert r["hset"][60, 5] <= E.PARAMS["zone_off"] * 2.0 * 1.05      # left the reef at about the stable height for 2 m
    assert Z[:40].any()                                               # and breaks again on the beach
    lines = r["lines"]; assert len(np.unique(lines["line"])) == 2     # two break lines: the reef and the beach
    small = E.analyse(tab, swell(0.5))
    assert np.allclose(small["T"][45:, 5], 1.0) and not small["zone"][90:100].any()   # a small swell passes over the reef


def test_water_behind_dry_reef_takes_its_neighbours_share():
    d = bar_depth(); tab = beach(0.0, ny=160, depth=d)
    dep = tab.dep.copy(); dep[90:100, 30:40] = -0.5                   # a dry patch of reef in ten columns
    tab2 = E.Table([0.07], [0.0], np.ones((1, 160, 80)), np.zeros((1, 160, 80)), dep, DX, 0.0)
    T = E.analyse(tab2, swell(0.5), lines=False)["T"]
    assert np.all(T[90:100, 30:40] == 0.0) and np.allclose(T[60, 30:40], 1.0)   # the land shadow is the table's job


def test_contours_trace_a_circle():
    y, x = np.mgrid[0:120, 0:120] * DX
    loops = E.contours(300.0 ** 2 - ((x - 600.0) ** 2 + (y - 600.0) ** 2), DX)
    assert len(loops) == 1
    xy = loops[0]; length = np.hypot(*np.diff(xy, axis=0).T).sum()
    assert length == pytest.approx(2 * math.pi * 300.0, rel=0.01)
    assert np.allclose(np.hypot(xy[:, 0] - 600.0, xy[:, 1] - 600.0), 300.0, atol=1.0)
    assert np.hypot(*(xy[0] - xy[-1])) < 1e-6                        # closed


def test_contours_handle_an_empty_field_and_open_lines():
    assert E.contours(np.full((20, 20), -1.0), DX) == []
    y, x = np.mgrid[0:20, 0:30] * DX
    lines = E.contours(y - 95.0, DX)
    assert len(lines) == 1 and np.allclose(lines[0][:, 1], 95.0) and len(lines[0]) == 30


def test_rides_split_at_long_closeouts_and_at_a_change_of_side():
    one = np.ones(25); hb = np.full(25, 2.0); slow = np.full(25, 6.0); plunging = np.ones(25, int)
    peel = np.array([40.0] * 10 + [5.0] * 5 + [40.0] * 10)
    rid, rlen, score = E.rides(peel, one, hb, slow, plunging, 10.0)
    assert list(rid[:10]) == [0] * 10 and list(rid[10:15]) == [-1] * 5 and list(rid[15:]) == [1] * 10
    assert rlen[0] == 90.0 and score[0] == pytest.approx(100.0 * 90.0 / 150.0)
    peel2 = np.array([40.0] * 10 + [5.0] * 2 + [40.0] * 13)             # a 20 m closeout section is ridden through
    rid2, rlen2, _ = E.rides(peel2, one, hb, slow, plunging, 10.0)
    assert np.all(rid2 == 0) and rlen2[0] == 240.0
    side = np.array([1.0] * 12 + [-1.0] * 13)                           # a peak: a right on one side, a left on the other
    rid3, rlen3, _ = E.rides(np.full(25, 40.0), side, hb, slow, plunging, 10.0)
    assert set(rid3[:12]) == {0} and set(rid3[12:]) == {1}


def test_score_falls_for_fast_sections_weak_breakers_and_short_rides():
    n = 30; one = np.ones(n); hb = np.full(n, 2.0); peel = np.full(n, 40.0)
    base = E.rides(peel, one, hb, np.full(n, 6.0), np.ones(n, int), 10.0)[2][0]
    assert base == pytest.approx(100.0)
    vmax = E.PARAMS["speed_base"] + E.PARAMS["speed_per_sqrt_h"] * math.sqrt(2.0)
    assert E.rides(peel, one, hb, np.full(n, 1.25 * vmax), np.ones(n, int), 10.0)[2][0] == pytest.approx(50.0)
    assert E.rides(peel, one, hb, np.full(n, 1.6 * vmax), np.ones(n, int), 10.0)[2][0] == 0.0
    assert E.rides(peel, one, hb, np.full(n, 6.0), np.zeros(n, int), 10.0)[2][0] == pytest.approx(70.0)
    assert E.rides(np.full(n, 20.0), one, hb, np.full(n, 6.0), np.ones(n, int), 10.0)[2][0] < 30.0
    assert E.rides(peel[:6], one[:6], hb[:6], np.full(6, 6.0), np.ones(6, int), 10.0)[2][0] == pytest.approx(100.0 * 50.0 / 150.0)


def test_two_trains_the_bigger_sets_the_direction_and_a_crossed_second_is_flagged():
    tab = E.Table([0.07], [330.0, 30.0], np.ones((2, 120, 80)),
                  np.stack([np.full((120, 80), 330.0), np.full((120, 80), 30.0)]), beach(0.0).dep, DX, 0.0)
    big = dict(W=np.array([[(1.5 / 4) ** 2, 0.0]]), tp=14.0); small = dict(W=np.array([[0.0, (1.2 / 4) ** 2]]), tp=10.0)
    r = E.analyse(tab, [small, big]); L = r["lines"]
    assert np.allclose(r["mdir"], 330.0, atol=1e-3) and np.all(L["side"] == -1) and np.all(L["mixed"])
    assert np.allclose(r["hs_lin"], math.hypot(1.5, 1.2), atol=1e-4)
    weak = dict(W=np.array([[0.0, (0.3 / 4) ** 2]]), tp=10.0)
    assert not E.analyse(tab, [weak, big])["lines"]["mixed"].any()


def test_the_engine_needs_numpy_only():
    src = open(os.path.join(ROOT, "tools", "model_frames", "nearshore_engine.py"), encoding="utf-8").read()
    assert "scipy" not in src.replace("no scipy", "")
