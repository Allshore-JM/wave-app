"""The open-water seed of plan section 98: a region with `seed` "sea" (the Lower 48's coasts) or "lake" (the Great Lakes)
finds its open water from a shoreline file: low pixels on the file's water side that are at least seed_m from its shore,
or deeper than DEEP_M there. Lidar that stops 10-19 m deep (south Oregon) then still reaches the sea, and a deep quarry
on the land is no sea. Without `seed` the deep rule is unchanged.
Needs scipy and rasterio (the bathy-build workflow installs them); skipped elsewhere."""
import hashlib
import os
import sys

import numpy as np
import pytest

pytest.importorskip("scipy")
rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))
sys.path.insert(0, os.path.join(ROOT, "tools", "coast"))

import bathy_sources as S  # noqa: E402
import bathy_tiles as T  # noqa: E402
import build_bathy as B  # noqa: E402
import build_coast as C  # noqa: E402

LAND = T.LAND_ID
Q = 10000


def _ring(lon0, lat0, lon1, lat1, clockwise=False):
    lons = [lon0, lon1, lon1, lon0]
    lats = [lat0, lat0, lat1, lat1]
    if clockwise:
        lons, lats = lons[::-1], lats[::-1]
    return (np.round(np.array(lons) * Q).astype(np.int64), np.round(np.array(lats) * Q).astype(np.int64))


def _coast(path, pieces):
    with open(path, "wb") as f:
        f.write(C.encode_file(pieces, 30, Q))
    B.land_pieces.cache_clear()
    return path


def _rc(bounds, px, lon, lat):
    x0, y0, x1, y1 = bounds
    mx, my = T.lonlat_to_xy(lon, lat)
    return int((y1 - my) / (y1 - y0) * px), int((mx - x0) / (x1 - x0) * px)


# ---------------------------------------------------------------- the raster

def test_shore_raster_fills_lakes_and_clears_their_islands(tmp_path):
    """The lakes set: an anticlockwise lake ring with a clockwise island ring in the same piece. holes=True gives the
    lake without its island; the land set's rule (every ring filled) would fill the island too."""
    path = _coast(str(tmp_path / "lakes.bin"), [[_ring(-87.0, 43.0, -86.0, 44.0),
                                                  _ring(-86.6, 43.4, -86.4, 43.6, clockwise=True)]])
    b = T.tile_bounds(7, *T.tile_of(-86.5, 43.5, 7))
    lake = B.shore_raster(path, b, 256, holes=True)
    filled = B.shore_raster(path, b, 256)
    assert lake[_rc(b, 256, -86.8, 43.2)] and not lake[_rc(b, 256, -86.5, 43.5)]     # lake water / the island
    assert filled[_rc(b, 256, -86.5, 43.5)]                                           # without holes: filled
    assert not lake[_rc(b, 256, -87.2, 43.5)] and not lake[_rc(b, 256, -86.5, 44.2)]  # outside the lake


def test_land_mask_is_the_filled_shore_raster(tmp_path):
    path = _coast(str(tmp_path / "land.bin"), [[_ring(176.0, 0.0, 177.0, 1.0)]])
    x, y = T.tile_of(176.5, 0.5, 7)
    assert np.array_equal(B.land_mask(path, 7, x, y), B.shore_raster(path, T.tile_bounds(7, x, y), 256))


# ---------------------------------------------------------------- the seed masks

def _block_reg(path, seed="sea", seed_m=1000.0, zmax=12):
    return dict(name="t", zmax=zmax, seed=seed, seed_m=seed_m, seed_path=path)


@pytest.mark.parametrize("seed_m,lat0", [(500.0, 21.2), (1000.0, 21.2), (2000.0, 21.2), (2000.0, 60.2)])
def test_sea_seed_is_water_at_least_seed_m_from_the_land(tmp_path, seed_m, lat0):
    """Land west of 158.05 W: the block's water is every pixel east of it, the far mask only those seed_m or more from
    the shore (one block-zoom pixel of slack: the mask is computed at the block zoom). At 60 N a Mercator metre is half a
    ground metre: the distance is in ground metres."""
    path = _coast(str(tmp_path / "land.bin"), [[_ring(-160.0, lat0 - 1.5, -158.05, lat0 + 1.5)]])
    zb = 9
    key = T.tile_of(-158.05, lat0, zb)
    reg = _block_reg(path, seed_m=seed_m)
    water, far = B.seed_masks(reg, key)
    n = T.TILE * 8
    assert water.shape == far.shape == (n, n) and water.dtype == bool
    x0, y0, x1, y1 = T.tile_bounds(zb, *key)
    lons = np.array([T.xy_to_lonlat(x0 + (i + 0.5) * (x1 - x0) / n, y1)[0] for i in range(n)])
    row = n // 2
    lat = T.xy_to_lonlat(x0, y1 - (row + 0.5) * (y1 - y0) / n)[1]
    m_per_deg = 111320.0 * np.cos(np.radians(lat))
    dist = (lons - (-158.05)) * m_per_deg                   # metres east of the shore (negative = on the land)
    slack = 2 * (x1 - x0) / T.TILE * np.cos(np.radians(lat))
    on_land = dist < -slack
    at_sea = dist > slack
    assert not water[row, on_land].any() and water[row, at_sea].all()
    assert far[row, dist > seed_m + slack].all() and not far[row, dist < seed_m - slack].any()
    assert not (far & ~water).any()


def test_a_shore_just_outside_the_block_still_counts(tmp_path):
    """The shore 500 m west of the block's west edge: the block is all water, but its first ~500 m are nearer than
    seed_m (1000 m) to that shore, so not far. The margin read around the block has to reach it."""
    zb = 9
    key = T.tile_of(-158.0, 21.2, zb)
    x0, y0, x1, y1 = T.tile_bounds(zb, *key)
    west = T.xy_to_lonlat(x0, y0)[0]
    m_per_deg = 111320.0 * np.cos(np.radians(21.2))
    shore = west - 500.0 / m_per_deg
    path = _coast(str(tmp_path / "land.bin"), [[_ring(shore - 2.0, 20.0, shore, 22.5)]])
    water, far = B.seed_masks(_block_reg(path), key)
    assert water.all()
    n = T.TILE * 8
    step = (x1 - x0) / n * np.cos(np.radians(21.2))            # ground metres per finest pixel
    row = n // 2
    near_cols = int((1000.0 - 500.0) / step)
    slack = 16                                                  # two block-zoom pixels
    assert not far[row, :max(near_cols - slack, 1)].any()
    assert far[row, near_cols + slack:].all()


def test_a_block_far_from_any_shore_is_all_far(tmp_path):
    path = _coast(str(tmp_path / "land.bin"), [[_ring(-160.0, 20.0, -159.5, 20.5)]])
    water, far = B.seed_masks(_block_reg(path), T.tile_of(-150.0, 30.0, 9))
    assert water.all() and far.all()


def test_a_block_inside_the_land_has_no_water(tmp_path):
    path = _coast(str(tmp_path / "land.bin"), [[_ring(-120.0, 30.0, -100.0, 45.0)]])
    water, far = B.seed_masks(_block_reg(path), T.tile_of(-110.0, 38.0, 9))
    assert not water.any() and not far.any()


def test_lake_seed_reads_the_lake_side(tmp_path):
    path = _coast(str(tmp_path / "lakes.bin"), [[_ring(-88.0, 42.0, -86.0, 45.0)]])
    reg = _block_reg(path, seed="lake")
    water, far = B.seed_masks(reg, T.tile_of(-87.0, 43.5, 9))         # the lake's middle: all water, all far
    assert water.all() and far.all()
    water, far = B.seed_masks(reg, T.tile_of(-90.0, 43.5, 9))         # inland west of the lake: no lake water
    assert not water.any()


def test_seed_of_without_seed_is_the_deep_rule():
    core = np.full((64, 64), int(T.encode_depth(np.array([5.0]))[0]), np.uint16)
    core[:8, :8] = int(T.encode_depth(np.array([S.DEEP_M + 5]))[0])
    reg = {"name": "t", "zmax": 12}
    assert np.array_equal(B.seed_of(reg, (0, 0), core), B.W.deep_of(core))


def test_seed_of_takes_deep_water_only_on_the_water_side(tmp_path, monkeypatch):
    n = T.TILE * 8
    water = np.zeros((n, n), bool)
    water[:, n // 2:] = True
    far = np.zeros((n, n), bool)
    far[:, -10:] = True
    monkeypatch.setattr(B, "seed_masks", lambda reg, key: (water, far))
    core = np.full((n, n), int(T.encode_depth(np.array([30.0]))[0]), np.uint16)   # deep everywhere
    seed = B.seed_of({"name": "t", "zmax": 12, "seed": "sea"}, (0, 0), core)
    assert np.array_equal(seed, water)                        # deep counts only where the shore file says water
    core[:] = int(T.encode_depth(np.array([5.0]))[0])         # shallow everywhere: only the far pixels
    seed = B.seed_of({"name": "t", "zmax": 12, "seed": "sea"}, (0, 0), core)
    assert np.array_equal(seed, far)


# ---------------------------------------------------------------- whole builds

def _shallow_box(tmp_path, monkeypatch, seed):
    """Land (+5 m) west of 158.05 W with a deep quarry (-30 m) in it; a shallow shelf (-10 m, never deeper than DEEP_M)
    east of it. The shoreline file puts the land west of 158.05 W too."""
    w, e, s, n, res = -158.25, -158.0, 21.20, 21.24, 0.0004
    width, height = int(round((e - w) / res)), int(round((n - s) / res))
    lo, la = np.meshgrid(w + (np.arange(width) + 0.5) * res, n - (np.arange(height) + 0.5) * res)
    data = np.where(lo < -158.05, 5.0, -10.0)
    data = np.where((lo > -158.24) & (lo < -158.22), -30.0, data)
    path = str(tmp_path / "shelf.tif")
    with rasterio.open(path, "w", driver="GTiff", width=width, height=height, count=1, dtype="float32",
                       crs="EPSG:4326", transform=from_origin(w, n, res, res), nodata=-9999.0) as d:
        d.write(data.astype(np.float32), 1)
    land = _coast(str(tmp_path / "land.bin"), [[_ring(-160.0, 20.0, -158.05, 22.5)]])
    monkeypatch.setitem(S.SOURCES, "shelf", dict(S.CUDEM, id=10, zmax=12, year="2099", path="shelf.tif",
                                                  datum="test", name="shelf", credit="test", licence="test"))
    reg = dict(box=(w, s, e, n), sources=["shelf"], mhhw=0.4, zmin=7, prio=10)
    if seed:
        reg.update(seed="sea", seed_m=1000.0)
    monkeypatch.setitem(S.REGIONS, "shelf", reg)
    B._core.cache_clear()
    local = {"shelf": path}
    if seed:
        local["seed:sea"] = land
    B.build("shelf", str(tmp_path / "out"), version=1, workers=1, local=local)
    B._core.cache_clear()
    return os.path.join(str(tmp_path / "out"), "shelf", "v1")


def _px(out, z, lon, lat):
    x, y = T.tile_of(lon, lat, z)
    p = B.tile_path(out, z, x, y)
    if not os.path.exists(p):
        return None
    code, sf = T.unpack_tile(open(p, "rb").read())
    x0, y0, x1, y1 = T.tile_bounds(z, x, y)
    s = (x1 - x0) / T.TILE
    i = int((T.lonlat_to_xy(lon, lat)[0] - x0) / s) + 1
    j = int((y1 - T.lonlat_to_xy(lon, lat)[1]) / s) + 1
    return int(code[j, i]), int(sf[j, i])


def test_the_sea_seed_finds_a_shelf_with_no_deep_water_and_drops_the_quarry(tmp_path, monkeypatch):
    out = _shallow_box(tmp_path, monkeypatch, seed=True)
    shelf = _px(out, 12, -158.01, 21.22)
    assert shelf is not None and shelf[0] > 0 and shelf[1] == 10, shelf        # the shelf is water
    near = _px(out, 12, -158.045, 21.22)                                         # 500 m off the shore: joined to it
    assert near is not None and near[0] > 0, near
    quarry = _px(out, 12, -158.23, 21.22)
    assert quarry is None or quarry[0] == 0, quarry                              # the deep quarry on the land: no sea
    assert B.check(out, log_fn=lambda *a: None) == 0


def test_without_the_seed_the_shelf_has_no_sea_and_the_quarry_does(tmp_path, monkeypatch):
    """The old rule on the same data, for contrast: no pixel deeper than 20 m on the shelf, so no sea there, and the
    deep quarry seeds itself."""
    out = _shallow_box(tmp_path, monkeypatch, seed=False)
    shelf = _px(out, 12, -158.01, 21.22)
    assert shelf is None or shelf[0] == 0, shelf
    quarry = _px(out, 12, -158.23, 21.22)
    assert quarry is not None and quarry[0] > 0, quarry


# ---------------------------------------------------------------- settings and files

@pytest.mark.parametrize("extra", [dict(seed="ocean"), dict(seed="sea", seed_m=50.0), dict(seed="sea", seed_m=True),
                                   dict(seed="sea", seed_m="1000")])
def test_region_config_refuses_bad_seed_settings(monkeypatch, extra):
    monkeypatch.setitem(S.SOURCES, "s0", dict(S.CUDEM, id=10, path="x.tif", year="2099"))
    monkeypatch.setitem(S.REGIONS, "bad", dict(box=(0, 0, 1, 1), sources=["s0"], mhhw=0.4, zmin=7, prio=10, **extra))
    with pytest.raises(ValueError, match="seed"):
        B.region_config("bad")


def test_region_config_refuses_a_seed_on_a_water_all_region(monkeypatch):
    monkeypatch.setitem(S.SOURCES, "s0", dict(S.CUDEM, id=10, path="x.tif", year="2099", water_all=True))
    monkeypatch.setitem(S.REGIONS, "bad", dict(box=(0, 0, 1, 1), sources=["s0"], mhhw=0.4, zmin=7, prio=10,
                                               seed="sea"))
    with pytest.raises(ValueError, match="water_all"):
        B.region_config("bad")


def test_every_lower48_region_seeds_from_the_sea():
    for name in S.US_BOXES:
        assert S.REGIONS[name]["seed"] == "sea"
        reg = B.region_config(name)
        assert reg["seed"] == "sea" and "seed_path" not in reg          # the file is fetched by build(), by hash


def test_the_lakes_file_is_checked_by_its_hash(tmp_path):
    work = tmp_path / "w"
    work.mkdir()
    (work / "lakes-i.bin").write_bytes(b"not the pinned file")
    with pytest.raises(ValueError, match="lake overview"):
        B.land_file(None, str(work), "lake")
    assert S.SEED_FILES["lake"][0] == "lakes-i.bin" and S.SEED_FILES["sea"][0] == "land-world-i.bin"
    assert len(S.LAKES["sha256"]) == 64 and S.LAKES["url"].endswith("/static/coast/lakes-v1/lakes-i.bin")


def test_the_pinned_lakes_hash_is_the_published_file_when_a_copy_is_at_hand():
    """The committed hash = the lakes-v1 overview (section 50). The file is not in the repository; when a local copy
    exists (ALLSHORE_LAKES_I), it is checked."""
    p = os.environ.get("ALLSHORE_LAKES_I")
    if not p or not os.path.exists(p):
        pytest.skip("no local lakes-i.bin")
    assert hashlib.sha256(open(p, "rb").read()).hexdigest() == S.LAKES["sha256"]
