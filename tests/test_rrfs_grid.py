"""rrfs_grid (plan section 99): the pinned RRFS projections against eccodes' own node coordinates (committed tables,
tests/fixtures/rrfs/nodes_<grid>.json, 50 nodes per grid incl. the four corners), the edge rule, the wind rotation, and
the geometry pin."""
import json
import math
import os

import numpy as np
import pytest

import rrfs_grid as G

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "rrfs")


def _nodes(name):
    with open(os.path.join(FIX, f"nodes_{name}.json")) as f:
        return json.load(f)["nodes"]


@pytest.mark.parametrize("name", G.ORDER)
def test_forward_hits_the_real_nodes(name):
    nodes = np.array(_nodes(name), dtype=float)
    fi, fj = G.forward(name, nodes[:, 2], nodes[:, 3])
    assert np.max(np.abs(fi - nodes[:, 1])) < 0.02
    assert np.max(np.abs(fj - nodes[:, 0])) < 0.02


@pytest.mark.parametrize("name", G.ORDER)
def test_self_check_passes_on_real_nodes_and_fails_on_shifted(name):
    nodes = _nodes(name)
    g = G.GRIDS[name]
    lats = np.full((g["Ny"], g["Nx"]), np.nan)
    lons = np.full((g["Ny"], g["Nx"]), np.nan)
    # a sparse array with only the real nodes: sample exactly them
    js = np.array([n[0] for n in nodes])
    is_ = np.array([n[1] for n in nodes])
    fi, fj = G.forward(name, np.array([n[2] for n in nodes]), np.array([n[3] for n in nodes]))
    assert max(np.max(np.abs(fi - is_)), np.max(np.abs(fj - js))) < 0.05
    # a one-cell shift of the claimed origin must be caught by self_check
    lats[js, is_] = [n[2] for n in nodes]
    lons[js, is_] = [n[3] for n in nodes]
    lats2 = np.roll(lats, 1, axis=1)
    lons2 = np.roll(lons, 1, axis=1)
    keep = ~np.isnan(lats2)
    sub_j, sub_i = np.nonzero(keep)
    fi2, fj2 = G.forward(name, lats2[keep], lons2[keep])
    assert max(np.max(np.abs(fi2 - sub_i)), np.max(np.abs(fj2 - sub_j))) > 0.9


def test_self_check_raises_on_a_bad_grid():
    g = G.GRIDS["hi"]
    jj, ii = np.mgrid[0:g["Ny"], 0:g["Nx"]]
    nodes = np.array(_nodes("hi"), dtype=float)
    # build fake arrays one row north of the truth: every node off by one row
    lat_row = (nodes[:, 2].max() - nodes[:, 2].min()) / (g["Ny"] - 1)
    lats = 18.072699 + (jj + 1) * lat_row
    lons = 198.474999 + ii * 0.0234
    with pytest.raises(G.GeometryError):
        G.self_check("hi", lats, lons, sample=200)


def test_cell_inside_outside_and_edges():
    assert G.cell("hi", 21.30, -157.85) is not None             # Honolulu
    assert G.cell("hi", 21.30, 202.15) == G.cell("hi", 21.30, -157.85)   # 0-360 and +-180 agree
    assert G.cell("hi", 30.0, -157.85) is None                  # north of the grid
    assert G.cell("conus", 21.30, -157.85) is None
    n0 = _nodes("hi")[0]                                       # the south-west corner node: on the edge -> None
    assert (n0[0], n0[1]) == (0, 0) and G.cell("hi", n0[2], n0[3]) is None
    assert G.cell("hi", float("nan"), 0.0) is None


@pytest.mark.parametrize("lat, lon, grid", [(21.3, -157.85, "hi"), (18.47, -66.10, "pr"), (37.75, -122.82, "conus"),
                                             (60.58, -146.80, "ak"), (41.0, -70.0, "conus")])
def test_locate_picks_the_right_grid(lat, lon, grid):
    assert G.locate(lat, lon)[0] == grid


def test_locate_outside_every_grid():
    assert G.locate(28.18, -15.82) is None          # Canaries
    assert G.locate(-33.9, 151.2) is None           # Sydney


def test_rotation_mercator_is_identity():
    u, v = G.rotate("hi", 3.0, -4.0, -157.0)
    assert (float(u), float(v)) == (3.0, -4.0)


def test_rotation_lambert_sign_and_size():
    # at LoV no turn; at the Oregon coast (-124.53) a = sin(38.5) * (-27.03 deg) = -16.83 deg
    u, v = G.rotate("conus", 0.0, 10.0, 262.5 - 360)
    assert abs(float(u)) < 1e-12 and abs(float(v) - 10.0) < 1e-12
    a = G.rotation_angle("conus", -124.53)
    assert math.degrees(float(a)) == pytest.approx(math.sin(math.radians(38.5)) * (-124.53 + 97.5), abs=1e-9)
    # a grid-north wind (v > 0, u = 0) at a negative angle gains a westward... check the matrix explicitly
    ue, ve = G.rotate("conus", 0.0, 10.0, -124.53)
    assert float(ue) == pytest.approx(math.sin(float(a)) * 10.0)
    assert float(ve) == pytest.approx(math.cos(float(a)) * 10.0)


def test_rotation_polar_stereo_uses_n_one_and_orientation():
    a = G.rotation_angle("ak", -146.80)
    assert math.degrees(float(a)) == pytest.approx(-146.80 - (210.0 - 360.0))


def test_speed_dir_from_convention():
    s, d = G.speed_dir(0.0, -5.0)       # blowing toward the south = FROM the north
    assert float(s) == pytest.approx(5.0) and float(d) == pytest.approx(0.0)
    s, d = G.speed_dir(-5.0, 0.0)       # toward the west = FROM the east
    assert float(d) == pytest.approx(90.0)


def _meta(name, **over):
    g = G.GRIDS[name]
    m = {"gridType": g["gridType"], "Nx": g["Nx"], "Ny": g["Ny"], "latitudeOfFirstGridPointInDegrees": g["lat1"],
         "longitudeOfFirstGridPointInDegrees": g["lon1"], "LaDInDegrees": g["LaD"], "DxInMetres": g["Dx"],
         "DyInMetres": g["Dy"], "scanningMode": 64, "shapeOfTheEarth": 6}
    if "LoV" in g:
        m["LoVInDegrees"] = g["LoV"]
    if "Latin1" in g:
        m["Latin1InDegrees"], m["Latin2InDegrees"] = g["Latin1"], g["Latin2"]
    m.update(over)
    return m


@pytest.mark.parametrize("name", G.ORDER)
def test_check_geometry_accepts_the_pin(name):
    G.check_geometry(name, _meta(name))


def test_check_geometry_polar_orientation_key():
    m = _meta("ak")
    del m["LoVInDegrees"]
    m["orientationOfTheGridInDegrees"] = 210.0
    G.check_geometry("ak", m)


@pytest.mark.parametrize("over", [{"Nx": 322}, {"scanningMode": 0}, {"shapeOfTheEarth": 1}, {"DxInMetres": 2501.0},
                                  {"latitudeOfFirstGridPointInDegrees": 18.08}, {"gridType": "lambert"}])
def test_check_geometry_rejects_changes(over):
    with pytest.raises(G.GeometryError):
        G.check_geometry("hi", _meta("hi", **over))


def test_check_geometry_rejects_lambert_lov_change():
    with pytest.raises(G.GeometryError):
        G.check_geometry("conus", _meta("conus", LoVInDegrees=265.0))
