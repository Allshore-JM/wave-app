"""Bathymetry data tiles (tools/bathy/bathy_tiles.py, plan section 93b): the depth code, the tile file, bitsets and the
halving rule. numpy + Pillow only, so these run in every environment."""
import math
import os
import sys

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))

import bathy_tiles as T  # noqa: E402


# ---------------------------------------------------------------- depth code

@pytest.mark.parametrize("d, code", [
    (-10.0, 1), (-9.99, 2), (0.0, 1001), (0.01, 1002), (12.34, 2235), (400.0, 41001),
    (400.1, 41002), (400.06, 41002), (2500.0, 62001), (2502.5, 62002), (11000.0, 65401),
])
def test_encode_depth_at_every_step_boundary(d, code):
    assert int(T.encode_depth(np.array([d]))[0]) == code


def test_encode_depth_clips_and_marks_no_data():
    c = T.encode_depth(np.array([np.nan, -50.0, 20000.0, np.inf, -np.inf]))
    assert c.tolist() == [0, 1, 65401, 0, 0]


def test_decode_table_is_the_inverse_and_unused_codes_are_none():
    assert np.isnan(T.DECODE[0])
    assert np.isnan(T.DECODE[65402]) and np.isnan(T.DECODE[65535])
    for c in (1, 1001, 41001, 41002, 62001, 62002, 65401):
        d = float(T.DECODE[c])
        assert int(T.encode_depth(np.array([d]))[0]) == c


def test_round_trip_error_is_at_most_half_a_step():
    rng = np.random.default_rng(3)
    d = np.concatenate([rng.uniform(-10, 400, 20000), rng.uniform(400, 2500, 20000), rng.uniform(2500, 11000, 20000)])
    back = T.DECODE[T.encode_depth(d)].astype(np.float64)
    step = np.where(d <= 400, 0.01, np.where(d <= 2500, 0.1, 2.5))
    # float32 table: 1 ulp at 11,000 m is ~0.001 m
    assert np.all(np.abs(back - d) <= step / 2 + 1e-3)


def test_step_at():
    assert T.step_at(5) == 0.01 and T.step_at(400) == 0.01 and T.step_at(401) == 0.1 and T.step_at(3000) == 2.5


# ---------------------------------------------------------------- tile file

def test_tile_file_round_trip_and_layout():
    rng = np.random.default_rng(1)
    code = rng.integers(0, 65402, (T.SIDE, T.SIDE)).astype(np.uint16)
    sf = rng.integers(0, 256, (T.SIDE, T.SIDE)).astype(np.uint8)
    png = T.pack_tile(code, sf)
    c2, s2 = T.unpack_tile(png)
    assert np.array_equal(c2, code) and np.array_equal(s2, sf)
    from PIL import Image
    import io
    im = Image.open(io.BytesIO(png))
    assert im.mode == "L" and im.size == (T.SIDE, T.ROWS)
    a = np.asarray(im)
    assert np.array_equal(a[:T.SIDE], (code >> 8).astype(np.uint8))      # high byte first
    assert np.array_equal(a[T.SIDE:2 * T.SIDE], (code & 0xFF).astype(np.uint8))
    assert np.array_equal(a[2 * T.SIDE:], sf)


def test_tile_file_refuses_other_shapes():
    with pytest.raises(ValueError):
        T.pack_tile(np.zeros((256, 256), np.uint16), np.zeros((256, 256), np.uint8))
    from PIL import Image
    import io
    for mode, size in (("L", (258, 258)), ("RGB", (258, 774)), ("I;16", (258, 774))):
        bio = io.BytesIO()
        Image.new(mode, size).save(bio, "PNG")
        with pytest.raises(ValueError):
            T.unpack_tile(bio.getvalue())


# ---------------------------------------------------------------- grid

def test_tile_bounds_and_tile_of_agree():
    for z, lon, lat in ((15, -158.05, 21.66), (12, 144.79, 13.45), (7, -170.7, -14.27), (0, 0.0, 0.0)):
        x, y = T.tile_of(lon, lat, z)
        x0, y0, x1, y1 = T.tile_bounds(z, x, y)
        mx, my = T.lonlat_to_xy(lon, lat)
        assert x0 <= mx < x1 and y0 <= my < y1


def test_tile_bounds_apron_grows_by_whole_pixels():
    b = T.tile_bounds(10, 30, 450)
    a = T.tile_bounds(10, 30, 450, apron=1)
    p = (b[2] - b[0]) / T.TILE
    assert a[0] == pytest.approx(b[0] - p) and a[3] == pytest.approx(b[3] + p)
    big = T.tile_bounds(7, 3, 55, apron=8, px=2048)
    assert (big[2] - big[0]) / 2064 == pytest.approx(T.WORLD / 128 / 2048)


def test_xy_lonlat_round_trip():
    for lon, lat in ((-158.0, 21.5), (145.0, 13.5), (-65.0, 18.2), (179.9, -84.9)):
        lo, la = T.xy_to_lonlat(*T.lonlat_to_xy(lon, lat))
        assert lo == pytest.approx(lon) and la == pytest.approx(lat)


# ---------------------------------------------------------------- bitsets

def test_bits_round_trip_little_bit_order():
    m = np.zeros((4, 4), bool)
    m[0, 0] = m[0, 3] = m[1, 0] = m[3, 3] = True
    b = T.pack_bits(m)
    assert b == bytes([0b00011001, 0b10000000])
    assert np.array_equal(T.unpack_bits(b, (4, 4)), m)
    m2 = np.random.default_rng(2).random((7, 9)) < 0.3
    assert np.array_equal(T.unpack_bits(T.pack_bits(m2), m2.shape), m2)


# ---------------------------------------------------------------- halving

def test_pool2_two_of_four_rule_mean_source_and_flags():
    d = np.full((4, 4), np.nan, np.float32)
    s = np.zeros((4, 4), np.uint8)
    # top-left block: 3 water (ids 11, 10, 10) -> water, mean, lowest id, OR of flags
    d[0, 0], d[0, 1], d[1, 0] = 2.0, 4.0, 6.0
    s[0, 0], s[0, 1], s[1, 0] = 11 | T.FLAG_FILLED, 10, 10 | T.FLAG_BLENDED
    # top-right block: 1 water -> not water
    d[0, 2] = 5.0
    s[0, 2] = 10
    # bottom-right: 2 water -> water
    d[2, 2], d[3, 3] = 10.0, 20.0
    s[2, 2], s[3, 3] = 31 | T.FLAG_COARSE, 31 | T.FLAG_COARSE
    out, so = T.pool2(d, s)
    assert out[0, 0] == pytest.approx(4.0)
    assert int(so[0, 0]) == 10 | T.FLAG_FILLED | T.FLAG_BLENDED
    assert np.isnan(out[0, 1]) and so[0, 1] == 0
    assert np.isnan(out[1, 0]) and so[1, 0] == 0
    assert out[1, 1] == pytest.approx(15.0) and int(so[1, 1]) == 31 | T.FLAG_COARSE


def test_pool2_ignores_flags_and_ids_of_dry_pixels():
    d = np.full((2, 2), np.nan, np.float32)
    s = np.full((2, 2), 3 | T.FLAG_FILLED, np.uint8)      # a dry pixel never carries a source; if one did, it is ignored
    d[0, 0], d[1, 1] = 1.0, 3.0
    s[0, 0], s[1, 1] = 12, 12
    out, so = T.pool2(d, s)
    assert out[0, 0] == pytest.approx(2.0) and int(so[0, 0]) == 12


def test_pool2_odd_shape_drops_the_last_row_and_column():
    out, so = T.pool2(np.ones((5, 7), np.float32), np.full((5, 7), 10, np.uint8))
    assert out.shape == (2, 3) and so.shape == (2, 3)


def test_mercator_constants():
    assert T.WORLD == pytest.approx(2 * math.pi * 6378137.0)
    assert T.SIDE == 258 and T.ROWS == 774
