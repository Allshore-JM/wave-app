"""Plan section 97: the pre-pass that lists the blocks a Digital Earth Pacific build has to read (bathy_dep.presence,
build_bathy.dep_blocks). DEP's depths are sparse (a tile's footprint is mostly empty ocean) and a block read costs about
the same with or without data, so the build reads only the blocks found to hold a usable pixel."""
import math
import os
import sys

import numpy as np
import pytest

rasterio = pytest.importorskip("rasterio")
pytest.importorskip("pyproj")
from rasterio.transform import from_origin  # noqa: E402
from rasterio.windows import Window  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))
import bathy_dep as D  # noqa: E402
import bathy_sources as S  # noqa: E402
import bathy_tiles as T  # noqa: E402
import build_bathy as B  # noqa: E402

BZ = 11                                      # dep-sdb's zmax 14 - BLOCK_LEVELS


def _centre_tile(x, y, r, c, z=BZ):
    """The zoom-z tile holding the centre of pixel (row r, column c) of DEP tile (x, y), by pyproj (independent)."""
    from pyproj import Transformer
    x0, y0, x1, y1 = D.bounds(x, y)
    t = Transformer.from_crs("EPSG:3832", "EPSG:4326", always_xy=True)
    lon, lat = t.transform(x0 + (c + 0.5) * D.RES, y1 - (r + 0.5) * D.RES)
    lon = (lon + 180.0) % 360.0 - 180.0
    return T.tile_of(lon, lat, z)


def test_the_axes_hold_each_pixel_centre_and_reach_the_grow_distance():
    x, y = 35, 39                                                   # Fiji
    col_lo, col_hi, row_lo, row_hi = D.tile_axes(x, y, BZ)
    rng = np.random.default_rng(97)
    for r, c in zip(rng.integers(0, D.PX, 200), rng.integers(0, D.PX, 200)):
        tx, ty = _centre_tile(x, y, int(r), int(c))
        assert col_lo[c] <= tx <= col_hi[c] and row_lo[r] <= ty <= row_hi[r]
    # a pixel touches two blocks only within the grow distance of a block edge: never more than two
    assert ((col_hi - col_lo) <= 1).all() and ((row_hi - row_lo) <= 1).all()
    assert (col_hi != col_lo).sum() < D.PX // 50 and (row_hi != row_lo).sum() < D.PX // 50
    # each block edge inside the tile is shared by the pixels within GROW_M (+ one pixel) of it
    n = 2 ** BZ
    for c in np.flatnonzero(col_hi != col_lo)[:5]:
        edge_lon = col_hi[c] / n * 360.0 - 180.0
        px_lon = D.lon_of(D.bounds(x, y)[0] + (c + 0.5) * D.RES)
        metres = abs(math.radians(px_lon - edge_lon)) * D.A
        assert metres <= D.GROW_M + D.RES


def test_the_axes_of_the_tile_across_180_degrees_wrap():
    x = 66                                                          # 179.97 E .. 179.17 W
    col_lo, col_hi, _, _ = D.tile_axes(x, 40, BZ)
    n = 2 ** BZ
    assert col_lo[0] == n - 1 and col_hi[-1] < 10                    # west of 180: the last column; east: the first
    k = int(np.flatnonzero(col_lo == n - 1).max())                   # the last pixel reaching back west of 180
    assert col_hi[k] == 0                                             # it reaches the first column too


def _brute(ok, r0, col_lo, col_hi, row_lo, row_hi):
    out = set()
    for r, c in zip(*np.nonzero(ok)):
        for tx in {int(col_lo[c]), int(col_hi[c])}:
            for ty in range(int(row_lo[r0 + r]), int(row_hi[r0 + r]) + 1):
                out.add((tx, ty))
    return out


def test_band_blocks_equals_a_pixel_by_pixel_listing():
    rng = np.random.default_rng(11)
    for x, y, r0 in ((35, 39, 0), (66, 40, 4096), (91, 66, 9216)):   # Fiji, 180 degrees, Oahu (the last band)
        axes = D.tile_axes(x, y, BZ)
        h = min(D.BAND, D.PX - r0)
        for trial in range(4):
            ok = np.zeros((h, D.PX), bool)
            ok[rng.integers(0, h, 40), rng.integers(0, D.PX, 40)] = True
            if trial == 3:
                ok[:] = False
            assert D.band_blocks(ok, r0, *axes) == _brute(ok, r0, *axes)


def test_usable_keeps_finite_values_down_to_the_cap():
    a = np.array([np.nan, -12.0, -12.01, -0.5, 3.0, -99999.0, np.inf], np.float32)
    assert D.usable(a, 12.0).tolist() == [False, True, False, True, True, False, False]
    assert D.usable(a).tolist() == [False, True, True, True, True, False, False]


def _sparse_tile(path, x, y, pixels):
    """A full-size DEP tile on disk, NaN but for `pixels` {(row, col): value} (unwritten blocks read as NaN)."""
    x0, y0, x1, y1 = D.bounds(x, y)
    with rasterio.open(str(path), "w", driver="GTiff", width=D.PX, height=D.PX, count=1, dtype="float32",
                       crs="EPSG:3832", transform=from_origin(x0, y1, D.RES, D.RES), nodata=float("nan"),
                       tiled=True, blockxsize=512, blockysize=512, compress="deflate", sparse_ok=True) as d:
        for (r, c), v in pixels.items():
            d.write(np.array([[v]], np.float32), 1, window=Window(c, r, 1, 1))
    return str(path)


def test_presence_reads_a_tile_and_lists_its_blocks(tmp_path):
    B.gdal_env()
    x, y = 66, 40
    col_lo, col_hi, row_lo, row_hi = D.tile_axes(x, y, BZ)
    edge_c = int(np.flatnonzero(col_lo == 2 ** BZ - 1).max())        # reaches both sides of 180 degrees
    edge_r = int(np.flatnonzero(row_hi != row_lo)[0])                # on a block-row edge
    pixels = {(100, 3000): -5.0,          # one block
              (200, 5000): -13.0,         # deeper than the cap: nothing
              (edge_r, 4000): -1.0,       # two block rows
              (9599, edge_c): -2.0,       # two block columns, across 180 degrees (the last row: the last band)
              (5000, 6000): 99999.0}      # a fill value: nothing
    path = _sparse_tile(tmp_path / "t.tif", x, y, pixels)
    tile, found = D.presence(((x, y), BZ, 12.0, path))
    assert tile == (x, y)
    want = set()
    for (r, c), v in pixels.items():
        if -12.0 <= v < 12000:
            ok = np.zeros((1, D.PX), bool)
            ok[0, c] = True
            want |= _brute(ok, r, col_lo, col_hi, row_lo, row_hi)
    assert found == want
    assert len({ty for tx, ty in found if tx == 1}) == 2               # the block-row edge pixel: two rows
    assert {tx for tx, _ in found} >= {2 ** BZ - 1, 0}


def test_presence_of_a_missing_tile_is_empty(tmp_path):
    B.gdal_env()
    assert D.presence(((35, 39), BZ, 12.0, str(tmp_path / "none.tif"))) == ((35, 39), set())


def test_presence_refuses_a_tile_of_another_size(tmp_path):
    B.gdal_env()
    x0, y0, x1, y1 = D.bounds(35, 39)
    p = str(tmp_path / "small.tif")
    with rasterio.open(p, "w", driver="GTiff", width=96, height=96, count=1, dtype="float32", crs="EPSG:3832",
                       transform=from_origin(x0, y1, 1000.0, 1000.0), nodata=float("nan")) as d:
        d.write(np.full((96, 96), -3.0, np.float32), 1)
    with pytest.raises(ValueError, match="9600"):
        D.presence(((35, 39), BZ, 12.0, p))


def test_tiles_in_picks_the_tiles_meeting_a_box_on_either_side_of_180():
    tiles = D.tiles(S.SOURCES["dep-sdb"]["skip_tiles"])
    fiji = D.tiles_in(tiles, (177.0, -18.0, 177.5, -17.5))
    assert fiji and all(any(r[0] < 177.5 and 177.0 < r[2] for r in D.rects([t])) for t in fiji)
    east = D.tiles_in(tiles, (-179.4, -17.4, -178.8, -17.0))
    assert east and all(t[0] >= 66 for t in east)
    west = D.tiles_in(tiles, (179.99, -17.5, 180.0, -17.0))          # the column across 180, from either side
    east180 = D.tiles_in(tiles, (-180.0, -17.5, -179.99, -17.0))
    assert west and east180 and {t[0] for t in west} == {t[0] for t in east180} == {66}
    assert D.tiles_in(tiles, (-100.0, 40.0, -99.0, 41.0)) == []       # nowhere near DEP's grid


def _sdb_region(monkeypatch, box=(150.0, -20.0, 180.0, -10.0)):
    monkeypatch.setitem(S.REGIONS, "sdb", dict(box=box, sources=["dep-sdb"], mhhw=0.0, zmin=8, prio=5, wrap=True))
    return B.region_config("sdb")


def test_block_keys_take_the_found_blocks_not_the_footprint(monkeypatch):
    reg = _sdb_region(monkeypatch)
    s = reg["srcs"][0]
    s["rects"] = [(150.0, -20.0, 180.0, -10.0)]                      # would be thousands of blocks
    inside = T.tile_of(170.0, -15.0, BZ)
    outside = T.tile_of(-170.0, -15.0, BZ)                          # beyond the box
    s["blocks"] = {inside, outside}
    assert B.block_keys(reg) == [inside]
    s["blocks"] = set()
    assert B.block_keys(reg) == []


def test_dep_blocks_reads_only_the_tiles_meeting_the_box_and_joins_their_blocks(monkeypatch):
    reg = _sdb_region(monkeypatch, box=(177.0, -18.0, 177.5, -17.5))
    s = reg["srcs"][0]
    seen = []

    def fake(args):
        t, bz, cap = args[:3]
        seen.append((t, bz, cap))
        return t, {(t[0], t[1] + 1000)}
    monkeypatch.setattr(D, "presence", fake)
    found = B.dep_blocks(reg, s, workers=0)
    tiles = D.tiles_in(D.tiles(s["skip_tiles"]), reg["box"])
    assert sorted(t for t, _, _ in seen) == sorted(tiles)
    assert all(bz == BZ and cap == 12.0 for _, bz, cap in seen)
    assert found == {(t[0], t[1] + 1000) for t in tiles}
    assert not set(s["skip_tiles"]) & {t for t, _, _ in seen}


def test_build_asks_for_the_blocks_of_a_remote_dep_source_only(tmp_path, monkeypatch):
    calls = []

    def fake(reg, src, workers):
        calls.append(src["key"])
        return set()                                                 # nothing found: no block is read
    monkeypatch.setattr(B, "dep_blocks", fake)
    read = []
    monkeypatch.setattr(B, "pass1", lambda a: read.append(a) or (a[1], None))
    index = B.build("pacific-sdb", str(tmp_path / "out"), version=1, box=(177.0, -18.0, 177.5, -17.5), workers=0)
    assert calls == ["dep-sdb"] and read == [] and index["count"] == 0
    assert not os.path.exists(os.path.join(str(tmp_path / "out"), "_work_pacific-sdb"))
