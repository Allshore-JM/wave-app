"""The datum grids of plan section 98: a region's high-tide line taken per pixel from a grid (mhhw "grid"), a source's
NAVD88 heights turned into heights above mean sea level by a grid (geoid "msl_navd88"), and the hash pins that keep a
changed grid from moving every depth.
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

import bathy_sources as S  # noqa: E402
import bathy_tiles as T  # noqa: E402
import bathy_water as W  # noqa: E402
import build_bathy as B  # noqa: E402
import build_datum_grids as G  # noqa: E402

BOX = (-158.25, 21.20, -158.0, 21.24)          # open sea south-west of Oahu (as the lidar tests)
SPLIT = -158.125                               # the datum grids change here
SHORE = 21.215                                 # deep sea south of it, a flat 0.3 m above sea level north of it


def _tif_fn(path, w, e, s, n, fn, res, nodata=-9999.0):
    width, height = int(round((e - w) / res)), int(round((n - s) / res))
    lon = w + (np.arange(width) + 0.5) * res
    lat = n - (np.arange(height) + 0.5) * res
    vals = fn(*np.meshgrid(lon, lat)).astype(np.float32)
    with rasterio.open(path, "w", driver="GTiff", width=width, height=height, count=1, dtype="float32",
                       crs="EPSG:4326", transform=from_origin(w, n, res, res), nodata=nodata) as d:
        d.write(vals, 1)
    return path


def _grid(path, west, east):
    """A datum grid over the whole of every block the box touches (zoom-10 blocks reach ~0.35 deg beyond it): `west`
    west of SPLIT, `east` east of it; NaN nodata, as build_datum_grids writes."""
    return _tif_fn(path, -159.0, -157.0, 20.5, 22.0, lambda lon, lat: np.where(lon < SPLIT, west, east), 0.01,
                   nodata=float("nan"))


def _source(monkeypatch, key, **extra):
    s = dict(S.CUDEM, id=10, zmax=13, res_m=10, year="2099", path="d.tif", datum="test", name="dem",
             credit="NOAA test", licence="test", nodata=-9999.0)
    s.update(extra)
    monkeypatch.setitem(S.SOURCES, key, s)


def _region(monkeypatch, name, key, mhhw):
    monkeypatch.setitem(S.REGIONS, name, dict(box=BOX, sources=[key], mhhw=mhhw, zmin=12, prio=10))


def _block_lonlat(reg, key):
    b, size, _, _ = B.block_geom(reg, key)
    xs = b[0] + (np.arange(size) + 0.5) * (b[2] - b[0]) / size
    ys = b[3] - (np.arange(size) + 0.5) * (b[3] - b[1]) / size
    return np.array([T.xy_to_lonlat(x, 0.0)[0] for x in xs]), np.array([T.xy_to_lonlat(0.0, y)[1] for y in ys])


# ---------------------------------------------------------------- the pieces

def test_low_by_takes_one_threshold_or_one_per_pixel():
    code = T.encode_depth(np.array([[-0.5, -0.3, -0.1, 2.0]], np.float32))
    one = W.low_code(0.4)
    assert W.low_by(code, one).tolist() == [[False, True, True, True]]
    assert W.low_of(code, 0.4).tolist() == W.low_by(code, one).tolist()
    per = T.encode_depth(-np.array([[0.6, 0.1, 0.6, 0.1]], np.float32))     # mhhw per pixel
    assert W.low_by(code, per).tolist() == [[True, False, True, True]]
    # each pixel judged as if the whole region had its own mhhw
    for i, m in enumerate((0.6, 0.1, 0.6, 0.1)):
        assert W.low_by(code, per)[0, i] == W.low_of(code, m)[0, i]
    # a pixel exactly on the high-tide line is low (water), one code above it is not
    edge = np.array([one - 1, one, one + 1])
    assert W.low_by(edge, one).tolist() == [False, True, True]
    assert W.low_by(edge, np.array([one, one, one])).tolist() == [False, True, True]


def test_survey_fade_caps_each_pixel_by_its_own_mhhw():
    """The newer survey (-0.2 m, water) ends at column 40, the older one is land (+3 m): the fade lifts the newer water
    towards +3 m. Where the pixel's MHHW is 0.5 m the fade is held at sea level; where it is -0.5 m (-0.2 m is above
    it: not water there) it is not held. Per pixel = the region-wide answer of each pixel's own value."""
    newer = np.full((3, 60), np.nan, np.float32)
    newer[:, :40] = -0.2
    older = np.full((3, 60), 3.0, np.float32)
    ids = np.where(np.isfinite(newer), 1, 0).astype(np.uint8)
    m = np.where(np.arange(60) < 30, 0.5, -0.5).astype(np.float32)[None, :].repeat(3, axis=0)
    e, i = B.survey_fade(newer, ids, older, 2, m)
    hi, ii = B.survey_fade(newer, ids, older, 2, 0.5)
    lo, il = B.survey_fade(newer, ids, older, 2, -0.5)
    assert np.array_equal(i, ii) and np.array_equal(i, il)
    assert np.allclose(e, np.where(m == 0.5, hi, lo))
    band = (i & T.FLAG_BLENDED) > 0
    assert band[:, 16:30].any() and band[:, 30:40].any()
    assert np.all(e[:, :30][band[:, :30]] <= 0.0 + 1e-6)          # held at sea level
    assert np.any(e[:, 30:40][band[:, 30:40]] > 0.5)               # not held: the older land shows through the fade


def test_the_datum_grids_are_named_and_the_mhhw_rule_is_checked(tmp_path, monkeypatch):
    assert S.GEOIDS["msl_navd88"] == os.path.join(S.DATUM_DIR, "us_msl_navd88.tif")
    assert S.MHHW_GRID == os.path.join(S.DATUM_DIR, "us_mhhw_msl.tif")
    assert B.committed_grid(S.GEOIDS["msl_navd88"]) and B.committed_grid(S.MHHW_GRID)
    assert not B.committed_grid(S.GEOIDS["egm2008"])               # a remote geoid is never hash-checked
    assert not B.committed_grid(str(tmp_path / "us_mhhw_msl.tif"))
    _source(monkeypatch, "dg-dem")
    for bad in (True, False, "Grid", "grid2", None, 5.5, -1.5, float("nan")):
        _region(monkeypatch, "dg", "dg-dem", bad)
        with pytest.raises(ValueError, match="mhhw"):
            B.region_config("dg")
    for good in (0, 0.4, -1.0, 5.0):
        _region(monkeypatch, "dg", "dg-dem", good)
        assert B.region_config("dg")["mhhw"] == good
    _region(monkeypatch, "dg", "dg-dem", "grid")
    local = {"dg-dem": "unused.tif", "mhhw:grid": str(tmp_path / "m.tif")}
    assert B.region_config("dg", local=local)["mhhw_path"] == local["mhhw:grid"]     # a local grid is not hash-checked
    monkeypatch.setattr(S, "DATUM_GRIDS", {})
    with pytest.raises(ValueError, match="no pinned sha256"):     # the committed grid, not pinned yet
        B.region_config("dg")


def test_check_datum_grid_needs_the_pinned_hash(tmp_path, monkeypatch):
    path = _grid(str(tmp_path / "us_mhhw_msl.tif"), 0.6, 0.1)
    sha = hashlib.sha256(open(path, "rb").read()).hexdigest()
    monkeypatch.setattr(S, "DATUM_GRIDS", {})
    with pytest.raises(ValueError, match="no pinned sha256"):
        B.check_datum_grid(path)
    monkeypatch.setattr(S, "DATUM_GRIDS", {"us_mhhw_msl.tif": sha})
    assert B.check_datum_grid(path) == path
    monkeypatch.setattr(S, "DATUM_GRIDS", {"us_mhhw_msl.tif": "0" * 64})
    with pytest.raises(ValueError, match="is not the pinned"):
        B.check_datum_grid(path)
    monkeypatch.setattr(S, "DATUM_GRIDS", {"us_mhhw_msl.tif": sha, "us_msl_navd88.tif": sha})
    with pytest.raises(ValueError, match="missing"):
        B.check_datum_grid(str(tmp_path / "us_msl_navd88.tif"))
    # region_config checks the committed grids (DATUM_DIR) and the source geoids that live there
    monkeypatch.setattr(S, "DATUM_DIR", str(tmp_path))
    monkeypatch.setattr(S, "MHHW_GRID", path)
    monkeypatch.setitem(S.GEOIDS, "msl_navd88", path)
    _source(monkeypatch, "dg-dem", geoid="msl_navd88")
    _region(monkeypatch, "dg", "dg-dem", "grid")
    reg = B.region_config("dg")
    assert reg["mhhw_path"] == path and reg["srcs"][0]["geoid_path"] == path
    monkeypatch.setattr(S, "DATUM_GRIDS", {"us_mhhw_msl.tif": "f" * 64})
    with pytest.raises(ValueError, match="is not the pinned"):
        B.region_config("dg")
    _region(monkeypatch, "dg", "dg-dem", 0.4)                       # the source's geoid alone is checked too
    with pytest.raises(ValueError, match="is not the pinned"):
        B.region_config("dg")


def test_mhhw_field_reads_the_grid_and_refuses_a_block_beyond_it(tmp_path, monkeypatch):
    _source(monkeypatch, "dg-dem")
    _region(monkeypatch, "dg", "dg-dem", "grid")
    local = {"dg-dem": "unused.tif", "mhhw:grid": _grid(str(tmp_path / "m.tif"), 0.6, 0.1)}
    B.gdal_env()
    reg = B.region_config("dg", local=local)
    key = T.tile_of(-158.15, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    m = B.mhhw_field(reg, key)
    lons, lats = _block_lonlat(reg, key)
    _, size, _, _ = B.block_geom(reg, key)
    assert m.shape == (size, size)
    west, east = lons < SPLIT - 0.02, lons > SPLIT + 0.02
    assert west.any() and east.any()
    assert np.allclose(m[:, west], 0.6, atol=1e-5) and np.allclose(m[:, east], 0.1, atol=1e-5)
    thr = B.low_threshold(reg, key)
    assert thr.shape == m.shape and thr.dtype == T.encode_depth(np.zeros(1, np.float32)).dtype
    assert np.all(thr[:, west] == W.low_code(0.6)) and np.all(thr[:, east] == W.low_code(0.1))
    _region(monkeypatch, "dg", "dg-dem", 0.4)
    reg4 = B.region_config("dg")
    assert B.mhhw_field(reg4, key) == 0.4 and B.low_threshold(reg4, key) == W.low_code(0.4)
    # a grid that ends inside the block stops the build (no silent region-wide fallback)
    small = _tif_fn(str(tmp_path / "small.tif"), -158.3, -158.0, 21.1, 21.3, lambda lon, lat: lon * 0 + 0.5, 0.01,
                    nodata=float("nan"))
    _region(monkeypatch, "dg", "dg-dem", "grid")
    reg = B.region_config("dg", local={"dg-dem": "unused.tif", "mhhw:grid": small})
    with pytest.raises(ValueError, match="outside the MHHW grid"):
        B.mhhw_field(reg, key)


def test_a_navd88_source_is_turned_into_depth_below_mean_sea_level(tmp_path, monkeypatch):
    """A source at -4.2 m NAVD88 everywhere; mean sea level stands 0.5 m above NAVD88 in the west, 1.1 m in the east:
    -4.7 m and -5.3 m below mean sea level."""
    _source(monkeypatch, "dg-dem", geoid="msl_navd88")
    _region(monkeypatch, "dg", "dg-dem", 0.4)
    w, s, e, n = BOX
    local = {"dg-dem": _tif_fn(str(tmp_path / "dem.tif"), w, e, s, n, lambda lon, lat: lon * 0 - 4.2, 0.0004),
             "geoid:msl_navd88": _grid(str(tmp_path / "msl.tif"), 0.5, 1.1)}
    B.gdal_env()
    reg = B.region_config("dg", local=local)
    assert reg["srcs"][0]["geoid_path"] == local["geoid:msl_navd88"]
    key = T.tile_of(-158.15, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src = B.read_field(reg, key)
    lons, lats = _block_lonlat(reg, key)
    inside = (lats > s + 0.002) & (lats < n - 0.002)
    west = (lons > w + 0.002) & (lons < SPLIT - 0.02)
    east = (lons > SPLIT + 0.02) & (lons < e - 0.002)
    assert west.any() and east.any() and inside.any()
    rows = elev[inside]
    assert np.allclose(rows[:, west], -4.7, atol=1e-3) and np.allclose(rows[:, east], -5.3, atol=1e-3)
    assert np.all(src[inside][:, west | east] == 10)


# ---------------------------------------------------------------- a whole build with the per-pixel high-tide line

def _flat_build(tmp_path, monkeypatch, name, mhhw, grid=None):
    """Deep sea (-30 m) south of SHORE; north of it a flat 0.3 m above sea level, and north of 21.225 N a flat 0.5 m
    above it (joined to the deep sea through the lower flat). Returns a reader of the built zoom-13 tiles, the output
    folder and the local paths."""
    w, s, e, n = BOX
    _source(monkeypatch, "dg-dem")
    _region(monkeypatch, name, "dg-dem", mhhw)
    local = {"dg-dem": _tif_fn(str(tmp_path / ("%s.tif" % name)), w, e, s, n,
                               lambda lon, lat: np.where(lat < SHORE, -30.0, np.where(lat < 21.225, 0.3, 0.5)),
                               0.0004)}
    if grid:
        local["mhhw:grid"] = grid
    B._core.cache_clear()
    B.build(name, str(tmp_path / ("out_" + name)), version=1, workers=1, local=local)
    B._core.cache_clear()
    out = os.path.join(str(tmp_path / ("out_" + name)), name, "v1")

    def at(lon, lat):
        x, y = T.tile_of(lon, lat, 13)
        code, sf = T.unpack_tile(open(B.tile_path(out, 13, x, y), "rb").read())
        x0, y0, x1, y1 = T.tile_bounds(13, x, y)
        p = (x1 - x0) / T.TILE
        mx, my = T.lonlat_to_xy(lon, lat)
        i, j = int((mx - x0) / p) + 1, int((y1 - my) / p) + 1
        return code[j, i], sf[j, i]
    return at, out, local


def test_a_build_judges_the_high_tide_line_per_pixel(tmp_path, monkeypatch):
    """MHHW 0.1 m west of SPLIT, 0.6 m east of it: both flats (0.3 and 0.5 m above sea level) are land in the west and
    water in the east. One region-wide 0.4 m makes the 0.3-m flat water and the 0.5-m flat land on both sides."""
    grid = _grid(str(tmp_path / "mhhw.tif"), 0.1, 0.6)
    at, out, local = _flat_build(tmp_path, monkeypatch, "dg", "grid", grid)
    for lat, depth in ((21.220, -0.3), (21.232, -0.5)):
        for lon in (-158.22, -158.18, -158.15):                   # MHHW 0.1 m: land
            code, sf = at(lon, lat)
            assert code == 0 and sf == T.LAND_ID, (lon, lat)
        for lon in (-158.10, -158.06, -158.03):                   # MHHW 0.6 m: water
            code, sf = at(lon, lat)
            assert code > 0 and sf == 10 and T.DECODE[code] == pytest.approx(depth, abs=0.02), (lon, lat)
    for lon in (-158.22, -158.03):                                # the deep sea: water either way
        code, sf = at(lon, 21.205)
        assert code > 0 and T.DECODE[code] == pytest.approx(30.0, abs=0.05)
    # pixel by pixel along the 0.3-m flat across the grid's step: water exactly where the block's own MHHW (as
    # mhhw_field reads it, margin included) puts the flat at or below the high-tide line
    reg = B.region_config("dg", local=local)
    x13, y13 = T.tile_of(-158.126, 21.220, 13)
    code, sf = T.unpack_tile(open(B.tile_path(out, 13, x13, y13), "rb").read())
    x0, y0, x1, y1 = T.tile_bounds(13, x13, y13)
    j = int((y1 - T.lonlat_to_xy(-158.126, 21.220)[1]) / ((x1 - x0) / T.TILE))
    per = 2 ** B.BLOCK_LEVELS
    key = (x13 // per, y13 // per)
    _, _, m, _ = B.block_geom(reg, key)
    mh = B.mhhw_field(reg, key)
    r = m + (y13 - key[1] * per) * T.TILE + j
    c = m + (x13 - key[0] * per) * T.TILE
    flat = T.encode_depth(np.full(T.TILE, -0.3, np.float32))
    want = flat >= T.encode_depth(-mh[r, c:c + T.TILE].astype(np.float32))
    assert want.any() and not want.all()                          # the step lies inside this tile
    got, gsrc = code[j + 1, 1:T.TILE + 1], sf[j + 1, 1:T.TILE + 1]
    assert ((got > 0) == want).all()
    assert np.all(got[want] == flat[want]) and np.all(gsrc[want] == 10) and np.all(gsrc[~want] == T.LAND_ID)
    at4 = _flat_build(tmp_path, monkeypatch, "dg4", 0.4)[0]
    for lon in (-158.22, -158.03):
        code, sf = at4(lon, 21.220)
        assert code > 0 and sf == 10, lon
        code, sf = at4(lon, 21.232)
        assert code == 0 and sf == T.LAND_ID, lon


# Surveyed MSL - NAVD88 and MHHW - MSL at NOAA gauges (CO-OPS datums, metres; checked 2026-11-15) the committed grids
# must reproduce: the grids are interpolated from these very stations, so a grid built from the wrong field, sign or
# station set misses them by far more than the 0.05 m allowed.
GAUGES = {"San Diego": (32.714, -117.174, 0.77, 0.85), "Seattle": (47.602, -122.339, 1.31, 1.44),
          "Galveston": (29.310, -94.793, 0.15, 0.20), "Miami": (25.768, -80.130, -0.29, 0.38),
          "The Battery": (40.700, -74.014, -0.06, 0.76), "Boston": (42.355, -71.052, -0.09, 1.55),
          "Eastport": (44.904, -66.982, -0.07, 2.92)}


def test_the_committed_datum_grids_are_the_pinned_ones_and_match_the_gauges():
    names = {"us_msl_navd88.tif": 2, "us_mhhw_msl.tif": 3}
    assert set(S.DATUM_GRIDS) == set(names) | {"nz_msl_nzvd2016.tif", "nz_mhhw_msl.tif", "eu_mhhw_msl.tif",
                                              "ie_msl_lat.tif", "fr_msl_ign69.tif"}
    for name, col in names.items():
        path = os.path.join(S.DATUM_DIR, name)
        with open(path, "rb") as f:
            assert hashlib.sha256(f.read()).hexdigest() == S.DATUM_GRIDS[name]
        assert B.check_datum_grid(path) == path
        with rasterio.open(path) as d:
            a = d.read(1)
            assert np.isfinite(a).all()                                   # IDW everywhere: no hole in the box
            s, w, n, e = G.BOX
            assert (d.bounds.left, d.bounds.bottom, d.bounds.right, d.bounds.top) == pytest.approx((w, s, e, n))
            for k, g in GAUGES.items():
                r, c = d.index(g[1], g[0])
                assert abs(float(a[r, c]) - g[col]) <= 0.05, (name, k, float(a[r, c]))
    assert os.path.getsize(os.path.join(S.DATUM_DIR, "us_datum_stations.json")) > 100000  # the offline rebuild's input


def test_every_grid_datum_region_s_blocks_lie_inside_the_datum_grids():
    """A block reaches past its region's box (the reach for the water rule, then whole tiles at the block zoom): run #50
    (2026-11-16) stopped us-fl-south (23.97 N), us-me-nh (66.45 W) and would have stopped us-or-wa (125.51 W) at the old
    grid edge. Every block of the full box, corner to corner, must read the grids with at least one grid cell to spare."""
    import math
    from pyproj import Transformer
    tr = Transformer.from_crs(3857, 4326, always_xy=True)
    import build_tide_grids as TG
    grids = {"grid": (G.BOX, G.STEP), "grid:nz": (TG.BOX, TG.STEP),        # each grid's (S, W, N, E) and cell size
             "grid:eu": (TG.EU["box"], TG.STEP)}
    names = [n for n, r in S.REGIONS.items() if isinstance(r.get("mhhw"), str)]
    assert len(names) >= 15 and {S.REGIONS[n]["mhhw"] for n in names} == set(grids)
    for name in names:
        r = B.region_config(name)
        (s0, w0, n0, e0), m = grids[r["mhhw"]]
        z = r["zmax"] - B.BLOCK_LEVELS
        w, s, e, n = r["box"]
        rc = r.get("reach", 0)
        k = 2 ** z
        x0, x1 = int((w - rc + 180) / 360 * k), int((e + rc + 180) / 360 * k)

        def ty(lat):
            return int((1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * k)

        for key in [(x0, ty(n + rc)), (x1, ty(s - rc))]:
            b = B.block_geom(r, key)[0]
            lon, lat = tr.transform([b[0], b[2]], [b[1], b[3]])
            assert w0 + m <= min(lon) and max(lon) <= e0 - m, (name, key, lon)
            assert s0 + m <= min(lat) and max(lat) <= n0 - m, (name, key, lat)
