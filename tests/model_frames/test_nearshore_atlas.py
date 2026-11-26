"""The sweet-spot atlas (plan section 109, stage 3): laying rides on cells, the per-cell summary and its readout text."""
import os
import sys

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import nearshore_atlas as A  # noqa: E402
import nearshore_engine as E  # noqa: E402

SIZES, PERIODS, DIRS, TIDES = (2, 4, 6, 8), (10, 15), (300, 330, 0), (0.0, 0.3)


def lines(x, y, score, band):
    n = len(x); z = np.zeros(n)
    return dict(x=np.array(x, float), y=np.array(y, float), score=np.array(score, float), band=np.array(band), hb=z, depth=z, peel=z,
                side=z, speed=z, xi=z, breaker=z, ride=z, ride_m=z, mixed=z, line=z, share=z + 1)


def test_a_ride_marks_its_cell_and_the_eight_around_it():
    score, flag = A.lay(lines([95.0, 155.0], [95.0, 95.0], [60.4, 30.0], [2, 0]), (6, 9))
    assert score[3, 3] == 60 and score[2:5, 2:5].min() == 60 and score[0, 0] == 0
    assert score[3, 5] == 30 and score[3, 6] == 30 and score[3, 4] == 60   # the better ride wins the shared cells
    assert flag[3, 3] == 1 and flag[3, 5] == 2 and flag.sum() == 3  # the flag marks only the cell the line is in
    empty = A.lay(lines([], [], [], []), (6, 9)); assert not empty[0].any() and not empty[1].any()
    edge, _ = A.lay(lines([5.0], [5.0], [50.0], [2]), (6, 9)); assert edge[0, 0] == 50 and edge[1, 1] == 50   # no wrap at the border


def grid():
    conds = A.conditions(SIZES, PERIODS, DIRS, TIDES); shape = (len(SIZES), len(PERIODS), len(DIRS), len(TIDES), 2, 3)
    return conds, np.zeros(shape, np.uint8), np.zeros(shape, np.uint8)


def summ(Q, FL, conds, smooth=False):
    return A.summarise(Q.reshape(len(conds), 2, 3), FL.reshape(len(conds), 2, 3), conds, SIZES, PERIODS, DIRS, TIDES, tide_names=("low", "high"), smooth=smooth)


def test_summary_finds_the_best_case_its_window_and_the_closeout_directions():
    conds, Q, FL = grid()
    Q[1:3, 1, 1:3, :, 0, 1] = 60; Q[2, 1, 2, 0, 0, 1] = 80          # 4-6 ft @ 15 s from 330-000 works; 6 ft from 000 is best
    Q[2:4, 0, 2, :, 0, 1] = 65; Q[0, 1, 2, :, 0, 1] = 0             # at 10 s it takes 6-8 ft
    FL[1:, :, :, :, 0, 1] = 1; FL[1:, 1, 0, :, 0, 1] = 2            # a line passes from 4 ft up; from 300 it closes out
    Q[0, 0, 0, 0, 1, 2] = 10                                        # a cell where nothing reaches the floor
    cells = summ(Q, FL, conds)
    assert len(cells) == 1
    c = cells[0]
    assert (c["cy"], c["cx"]) == (0, 1) and c["best"] == (6, 15, 0, 0, 80)
    assert c["size_ft"] == (4, 6) and c["dir_deg"] == (330, 0) and c["by_period"] == {10: (6, 8), 15: (4, 6)}
    assert c["closes_deg"] == (300, 300) and c["starts_ft"] == 4 and c["tide"] == "any"
    assert A.text(c) == "Best here: buoy 4-6 ft @ 15 s from 330-000 (6-8 ft @ 10 s). Closes out from 300"


def test_summary_names_the_tide_when_only_one_works():
    conds, Q, FL = grid()
    Q[1, 0, 2, 1, 1, 0] = 70; Q[1, 0, 2, 0, 1, 0] = 20; FL[1, 0, 2, :, 1, 0] = 1
    c = summ(Q, FL, conds)[0]
    assert c["best"] == (4, 10, 0, 1, 70) and c["tide"] == "high" and c["closes_deg"] is None
    assert A.text(c) == "Best here: buoy 4 ft @ 10 s from 000, high tide"


def test_build_runs_the_matrix_through_the_engine():
    ny, nx = 120, 80
    dep = np.repeat((0.02 * (np.arange(ny) - 10) * 10.0)[:, None], nx, axis=1)
    tab = E.Table([0.07], [0.0], np.ones((1, ny, nx)), np.full((1, ny, nx), 40.0), dep, 10.0, 0.0)   # every swell arrives at 40 deg
    conds = A.conditions((3, 6), (14,), (0,), (0.0, 0.4))
    Q, FL, got = A.build(tab, conds)
    assert got == conds and Q.shape == (4,) + A.cell_shape(tab) and Q.max() == 70      # a long ideal-angle ride on a gentle (spilling) beach
    rows = [np.nonzero(q.max(1))[0].mean() for q in Q]
    assert rows[2] > rows[0] and rows[1] < rows[0]                 # bigger breaks further out, high tide further in
    assert (FL == 2).sum() == 0 and (FL == 1).sum() > 0


def test_smoothing_prefers_a_plateau_to_a_lone_spike():
    conds, Q, FL = grid()
    Q[0, 0, 0, :, 0, 0] = 90                                        # one lucky case
    Q[1:4, 1, 1:3, :, 0, 0] = 70                                    # a block of good cases
    raw = summ(Q, FL, conds)[0]; got = summ(Q, FL, conds, smooth=True)[0]
    assert raw["best"][:3] == (2, 10, 300) and got["best"][1] == 15 and got["best"][0] in (4, 6, 8) and got["best"][2] in (330, 0)
    assert got["size_ft"][0] <= 4 and got["size_ft"][1] >= 6
