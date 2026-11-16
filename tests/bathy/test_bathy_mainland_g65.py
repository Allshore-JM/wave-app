"""Regression tests for review G65a of plan section 98 (the Lower-48 and Great Lakes bathymetry builder): the reach a
region reads past its box and the cut back to it, leaving another region's box out after the water rule (exclude_late),
footprints clipped to a box (clip, clip_area), BlueTopo's tile scheme and its tiles read one by one, the datum-grid
station rules, the region and source tables. Needs rasterio (the bathy-build workflow installs it); skipped elsewhere."""
import math
import os
import sqlite3
import struct
import sys

import numpy as np
import pytest

rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))

import bathy_bluetopo as BT  # noqa: E402
import bathy_sources as S  # noqa: E402
import bathy_tiles as T  # noqa: E402
import build_bathy as B  # noqa: E402
import build_datum_grids as DG  # noqa: E402

BOX = (-158.25, 21.20, -158.0, 21.24)          # open sea south-west of Oahu (as the lidar and datum tests)
SPLIT_LON = -158.125                           # the clipped source holds the west half of the box
SPLIT_LAT = 21.22                              # the deep source ends here


def _tif_fn(path, w, e, s, n, fn, res, nodata=-9999.0):
    width, height = int(round((e - w) / res)), int(round((n - s) / res))
    lon = w + (np.arange(width) + 0.5) * res
    lat = n - (np.arange(height) + 0.5) * res
    vals = fn(*np.meshgrid(lon, lat)).astype(np.float32)
    with rasterio.open(path, "w", driver="GTiff", width=width, height=height, count=1, dtype="float32",
                       crs="EPSG:4326", transform=from_origin(w, n, res, res), nodata=nodata) as d:
        d.write(vals, 1)
    return path


def _source(monkeypatch, key, **extra):
    s = dict(S.CUDEM, id=10, zmax=13, res_m=10, year="2099", path="d.tif", datum="test", name="dem",
             credit="NOAA test", licence="test", nodata=-9999.0)
    s.update(extra)
    monkeypatch.setitem(S.SOURCES, key, s)


def _region(monkeypatch, name, sources, **extra):
    r = dict(box=BOX, sources=list(sources), mhhw=0.4, zmin=12, prio=10)
    r.update(extra)
    monkeypatch.setitem(S.REGIONS, name, r)


def _centres(z, x, y):
    """lon, lat (each 258 long) of the pixel centres of a stored tile (1-px apron on every side)."""
    x0, y0, x1, y1 = T.tile_bounds(z, x, y)
    p = (x1 - x0) / T.TILE
    k = np.arange(T.SIDE)
    mx = x0 + (k - 0.5) * p
    my = y1 - (k - 0.5) * p
    return np.degrees(mx / T.R), np.degrees(2 * np.arctan(np.exp(my / T.R)) - np.pi / 2), p


# ---------------------------------------------------------------- reach and the boxes

def test_reach_box_grows_every_side_and_holds_the_latitudes_inside_85():
    assert B.reach_box((-10.0, 0.0, 10.0, 20.0), 0.3) == (-10.3, -0.3, 10.3, 20.3)
    assert B.reach_box((-10.0, 80.0, 10.0, 84.0), 2.0) == (-12.0, 78.0, 12.0, 85.0)
    assert B.reach_box((-10.0, -84.0, 10.0, -80.0), 2.0) == (-12.0, -85.0, 12.0, -78.0)
    assert B.read_box({"box": BOX}) == BOX
    assert B.read_box({"box": BOX, "reach_box": (1, 2, 3, 4)}) == (1, 2, 3, 4)


@pytest.mark.parametrize("reach", [True, -0.1, 2.5, "0.3", None])
def test_region_config_refuses_a_reach_that_is_not_degrees(monkeypatch, reach):
    _source(monkeypatch, "g65-a")
    _region(monkeypatch, "g65r", ["g65-a"], cut=True, reach=reach)
    with pytest.raises(ValueError, match="reach"):
        B.region_config("g65r")


def test_region_config_wants_cut_with_a_reach_and_exclude_with_exclude_late(monkeypatch):
    _source(monkeypatch, "g65-a")
    _region(monkeypatch, "g65r", ["g65-a"], reach=0.2)
    with pytest.raises(ValueError, match="needs cut"):
        B.region_config("g65r")
    _region(monkeypatch, "g65r", ["g65-a"], exclude_late=True)
    with pytest.raises(ValueError, match="exclude_late without exclude"):
        B.region_config("g65r")


def test_region_config_grows_the_narrowed_box_and_keeps_the_configured_one(monkeypatch):
    _source(monkeypatch, "g65-a")
    _region(monkeypatch, "g65r", ["g65-a"], cut=True, reach=0.2)
    narrow = (-158.2, 21.21, -158.1, 21.23)
    reg = B.region_config("g65r", box=narrow)
    assert reg["box"] == narrow
    assert reg["reach_box"] == B.reach_box(narrow, 0.2)
    assert reg["keep_box"] == BOX                    # the cut follows the configured box, not --box
    plain = B.region_config("g65r")
    assert plain["reach_box"] == B.reach_box(BOX, 0.2) and plain["keep_box"] == BOX
    _region(monkeypatch, "g65n", ["g65-a"])
    assert "reach_box" not in B.region_config("g65n") and "keep_box" not in B.region_config("g65n")


def _keys(box, z=10):
    x0, y0, x1, y1 = T.tiles_in_box(box, z)
    return {(x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)}


def test_block_keys_follow_the_reach_and_exclude_late_keeps_the_blocks():
    wide = [(-158.6, 20.9, -157.6, 21.6)]
    reg = {"zmax": 13, "box": BOX, "srcs": [{"rects": wide, "fill": False}]}
    assert set(B.block_keys(reg)) == _keys(BOX)
    reg["reach_box"] = B.reach_box(BOX, 0.2)
    assert set(B.block_keys(reg)) == _keys(B.reach_box(BOX, 0.2))
    assert _keys(B.reach_box(BOX, 0.2)) > _keys(BOX)
    # a fill source never drives the blocks
    reg2 = {"zmax": 13, "box": BOX, "srcs": [{"rects": wide, "fill": True}]}
    assert B.block_keys(reg2) == []
    # another region's box holding a block wholly: left out before the read, unless exclude_late
    big = (-159.0, 20.5, -157.0, 22.0)
    reg["exclude_boxes"] = [big]
    assert B.block_keys(reg) == []
    reg["exclude_late"] = True
    assert set(B.block_keys(reg)) == _keys(B.reach_box(BOX, 0.2))
    # a box holding no block wholly leaves every block
    reg2 = {"zmax": 13, "box": BOX, "srcs": [{"rects": wide, "fill": False}], "exclude_boxes": [(-158.1, 21.21, -158.05, 21.22)]}
    assert set(B.block_keys(reg2)) == _keys(BOX)


def test_drop_mask_cuts_outside_the_keep_box_and_inside_the_exclude_boxes():
    z, x, y = 10, 61, 449
    b = T.tile_bounds(z, x, y)
    lon, lat, _ = _centres(z, x, y)
    lon, lat = lon[1:-1], lat[1:-1]               # tile_bounds without apron: the 256 inner centres
    size = T.TILE
    assert B.drop_mask(b, size) is None
    mid_lon, mid_lat = float(lon[100]), float(lat[100])
    w, s, e, n = float(lon[0]) - 1, float(lat[-1]) - 1, float(lon[-1]) + 1, float(lat[0]) + 1
    keep = (w, s, mid_lon, n)                     # columns 0..100 kept (centre on the edge counts as inside)
    m = B.drop_mask(b, size, keep=keep)
    assert not m[:, :101].any() and m[:, 101:].all()
    ex = (w, mid_lat, e, n)                       # rows 0..100 (north of mid_lat, inclusive) left out
    m = B.drop_mask(b, size, exclude=[ex])
    assert m[:101, :].all() and not m[101:, :].any()
    m = B.drop_mask(b, size, exclude=[ex], keep=keep)
    assert m[:101, :].all() and m[:, 101:].all() and not m[101:, :101].any()
    # longitudes judged modulo 360: a box given in -180..180 cuts a grid reaching past +180
    assert B.exclude_mask(b, size, [(w + 360 - 360, s, e, n)]).all()


def test_clip_rects_and_clip_box():
    rects = [(0.0, 0.0, 10.0, 10.0), (20.0, 20.0, 30.0, 30.0), (40.0, 40.0, 50.0, 50.0), (0.0, 25.0, 5.0, 30.0)]
    assert B.clip_rects(rects, (5.0, 5.0, 25.0, 25.0)) == [(5.0, 5.0, 10.0, 10.0), (20.0, 20.0, 25.0, 25.0)]
    assert B.clip_rects([(0.0, 0.0, 5.0, 5.0)], (5.0, 0.0, 9.0, 9.0)) == []          # touching only: empty
    assert B.clip_box(True, [1.0, 2.0, 3.0, 4.0]) == (1.0, 2.0, 3.0, 4.0)
    assert B.clip_box((0.0, 0.0, 10.0, 10.0), (5.0, -5.0, 20.0, 8.0)) == (5.0, 0.0, 10.0, 8.0)
    for bad in [(1.0, 2.0, 3.0), (3.0, 0.0, 1.0, 5.0), (0.0, 5.0, 3.0, 1.0), "x", 1]:
        with pytest.raises(ValueError, match="clip"):
            B.clip_box(bad, (0.0, 0.0, 1.0, 1.0))


def test_outside_box_names_own_rectangles_past_the_box_only():
    srcs = [{"key": "own", "fill": False, "rects": [(0.0, 0.0, 5.0, 5.0), (4.0, 4.0, 11.0, 9.0)]},
            {"key": "fill", "fill": True, "rects": [(-50.0, -50.0, 50.0, 50.0)]}]
    assert B.outside_box((0.0, 0.0, 10.0, 10.0), srcs) == [("own", (4.0, 4.0, 11.0, 9.0))]
    assert B.outside_box((0.0, 0.0, 11.0, 10.0), srcs) == []


def test_hits_is_a_strict_overlap():
    assert B._hits([(0.0, 0.0, 1.0, 1.0)], (0.5, 0.5, 2.0, 2.0))
    assert not B._hits([(0.0, 0.0, 1.0, 1.0)], (1.0, 0.0, 2.0, 1.0))
    assert not B._hits([], (0.0, 0.0, 1.0, 1.0))


# ---------------------------------------------------------------- a whole build: reach, cut and clip

def _build(tmp_path, monkeypatch, name, reach):
    """A: a deep source (-30 m) south of SPLIT_LAT, reaching past the box; B: a shallower one (-10 m) clipped to the
    box's west half (clip). The region cuts to its box; its reach lets it read past it."""
    _source(monkeypatch, "g65-a", id=10)
    _source(monkeypatch, "g65-b", id=11, clip=(BOX[0], BOX[1], SPLIT_LON, BOX[3]))
    _region(monkeypatch, name, ["g65-a", "g65-b"], zmin=8, cut=True, reach=reach)
    local = {"g65-a": _tif_fn(str(tmp_path / "a.tif"), -158.30, -157.95, 21.15, SPLIT_LAT,
                              lambda lon, lat: np.full(lon.shape, -30.0), 0.0004),
             "g65-b": _tif_fn(str(tmp_path / "b.tif"), -158.30, -157.95, 21.15, 21.29,
                              lambda lon, lat: np.full(lon.shape, -10.0), 0.0004)}
    B._core.cache_clear()
    try:
        B.build(name, str(tmp_path / ("out_" + name)), version=1, workers=1, local=local)
    finally:
        B._core.cache_clear()
    return os.path.join(str(tmp_path / ("out_" + name)), name, "v1")


def test_a_reach_build_reads_past_its_box_and_cuts_back_to_it_at_every_zoom(tmp_path, monkeypatch):
    out = _build(tmp_path, monkeypatch, "g65cut", 0.2)
    found = B.scan_tiles(out)
    assert sorted(found) == list(range(8, 14))
    seen = {z: [0, 0, 0] for z in found}          # pixels checked: deep, clipped, empty (inside the box)
    for z, tiles in found.items():
        for (x, y) in tiles:
            code, sf = T.unpack_tile(open(B.tile_path(out, z, x, y), "rb").read())
            lon, lat, p = _centres(z, x, y)
            LON, LAT = np.meshgrid(lon, lat)
            src = sf & T.SRC_BITS
            outside = (LON < BOX[0]) | (LON > BOX[2]) | (LAT < BOX[1]) | (LAT > BOX[3])
            assert not code[outside].any() and not src[outside].any(), (z, x, y)
            m = 1.5 * math.degrees(p / T.R)       # a pixel and a half from every edge
            inner = (LON > BOX[0] + m) & (LON < BOX[2] - m) & (LAT > BOX[1] + m) & (LAT < BOX[3] - m)
            deep = inner & (LAT < SPLIT_LAT - m)
            clipped = inner & (LAT > SPLIT_LAT + m) & (LON < SPLIT_LON - m)
            empty = inner & (LAT > SPLIT_LAT + m) & (LON > SPLIT_LON + m)
            assert (src[deep] == 10).all() and (code[deep] > 0).all(), (z, x, y)
            assert (src[clipped] == 11).all() and (code[clipped] > 0).all(), (z, x, y)
            assert not code[empty].any() and not src[empty].any(), (z, x, y)
            assert np.allclose(T.DECODE[code[deep]], 30.0, atol=0.02)
            assert np.allclose(T.DECODE[code[clipped]], 10.0, atol=0.02)
            seen[z][0] += int(deep.sum())
            seen[z][1] += int(clipped.sum())
            seen[z][2] += int(empty.sum())
    for z, (d, c, e) in seen.items():
        assert d and c and e, (z, seen[z])


def test_without_its_reach_the_same_region_is_refused_for_footprints_past_its_box(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="outside the region's box"):
        _build(tmp_path, monkeypatch, "g65nor", 0)


def test_a_clip_area_cuts_each_pixel_of_the_block(tmp_path, monkeypatch):
    _source(monkeypatch, "g65-c", id=10)
    _region(monkeypatch, "g65clip", ["g65-c"])
    path = _tif_fn(str(tmp_path / "c.tif"), BOX[0], BOX[2], BOX[1], BOX[3], lambda lon, lat: np.full(lon.shape, -10.0),
                   0.0004)
    reg = B.region_config("g65clip", local={"g65-c": path})
    reg["srcs"][0]["clip_area"] = (BOX[0], BOX[1], SPLIT_LON, BOX[3])
    B.gdal_env()
    key = T.tile_of(SPLIT_LON, SPLIT_LAT, reg["zmax"] - 3)          # the block the split runs through
    assert key in set(B.block_keys(dict(reg, srcs=[dict(reg["srcs"][0], rects=[BOX])])))
    elev, ids = B.read_field(reg, key)
    b, size, _, _ = B.block_geom(reg, key)
    lon = np.degrees((b[0] + (np.arange(size) + 0.5) * (b[2] - b[0]) / size) / T.R)
    lat = np.degrees(2 * np.arctan(np.exp((b[3] - (np.arange(size) + 0.5) * (b[3] - b[1]) / size) / T.R)) - np.pi / 2)
    LON, LAT = np.meshgrid(lon, lat)
    inbox = (LAT > BOX[1] + 0.001) & (LAT < BOX[3] - 0.001)
    west = inbox & (LON > BOX[0] + 0.001) & (LON < SPLIT_LON - 0.001)
    east = inbox & (LON > SPLIT_LON + 0.001) & (LON < BOX[2] - 0.001)
    assert west.any() and east.any()
    assert np.isfinite(elev[west]).all() and (ids[west] == 10).all()
    assert np.isnan(elev[east]).all() and not ids[east].any()


EAST = (SPLIT_LON, BOX[1], BOX[2], BOX[3])     # another region's box: the east half of BOX


def _late_region(monkeypatch, name, late, **extra):
    _source(monkeypatch, "g65-l", id=10)
    _region(monkeypatch, "g65east", ["g65-l"], box=EAST)
    _region(monkeypatch, name, ["g65-l"], exclude=["g65east"], exclude_late=late, **extra)


def _block_lonlat(reg, key):
    b, size, _, _ = B.block_geom(reg, key)
    lon = np.degrees((b[0] + (np.arange(size) + 0.5) * (b[2] - b[0]) / size) / T.R)
    lat = np.degrees(2 * np.arctan(np.exp((b[3] - (np.arange(size) + 0.5) * (b[3] - b[1]) / size) / T.R)) - np.pi / 2)
    return np.meshgrid(lon, lat)


@pytest.mark.parametrize("late", [True, False])
def test_read_field_reads_an_exclude_late_box_and_cuts_an_early_one(tmp_path, monkeypatch, late):
    _late_region(monkeypatch, "g65rf", late)
    path = _tif_fn(str(tmp_path / "l.tif"), BOX[0], BOX[2], BOX[1], BOX[3], lambda lon, lat: np.full(lon.shape, -30.0),
                   0.0004)
    reg = B.region_config("g65rf", local={"g65-l": path})
    B.gdal_env()
    key = T.tile_of(SPLIT_LON, SPLIT_LAT, reg["zmax"] - 3)
    elev, ids = B.read_field(reg, key)
    LON, LAT = _block_lonlat(reg, key)
    inbox = (LAT > BOX[1] + 0.001) & (LAT < BOX[3] - 0.001)
    west = inbox & (LON > BOX[0] + 0.001) & (LON < SPLIT_LON - 0.001)
    east = inbox & (LON > SPLIT_LON + 0.001) & (LON < BOX[2] - 0.001)
    assert west.any() and east.any()
    assert np.isfinite(elev[west]).all() and (ids[west] == 10).all()
    if late:     # the water rule must see the other box's water: read it, cut it later (pass2)
        assert np.isfinite(elev[east]).all() and (ids[east] == 10).all()
    else:
        assert np.isnan(elev[east]).all() and not ids[east].any()


def test_an_exclude_late_build_leaves_the_other_box_empty_at_every_zoom(tmp_path, monkeypatch):
    _late_region(monkeypatch, "g65lb", True, zmin=8)
    path = _tif_fn(str(tmp_path / "l.tif"), BOX[0], BOX[2], BOX[1], BOX[3], lambda lon, lat: np.full(lon.shape, -30.0),
                   0.0004)
    out_root = str(tmp_path / "out")
    B._core.cache_clear()
    try:
        B.build("g65lb", out_root, version=1, workers=1, local={"g65-l": path})
    finally:
        B._core.cache_clear()
    out = os.path.join(out_root, "g65lb", "v1")
    found = B.scan_tiles(out)
    assert max(found) == 13
    west_px = {z: 0 for z in found}
    for z, tiles in found.items():
        for (x, y) in tiles:
            code, sf = T.unpack_tile(open(B.tile_path(out, z, x, y), "rb").read())
            lon, lat, p = _centres(z, x, y)
            LON, LAT = np.meshgrid(lon, lat)
            m = 1.5 * math.degrees(p / T.R)
            inner = (LAT > BOX[1] + m) & (LAT < BOX[3] - m)
            east = inner & (LON > SPLIT_LON + m) & (LON < BOX[2] - m)
            west = inner & (LON > BOX[0] + m) & (LON < SPLIT_LON - m)
            assert not code[east].any() and not (sf[east] & T.SRC_BITS).any(), (z, x, y)
            west_px[z] += int((code[west] > 0).sum())
    assert all(west_px[z] for z in found), west_px


class _Stop(Exception):
    pass


def test_bluetopo_tiles_are_chosen_over_the_reach_not_only_the_box(tmp_path, monkeypatch):
    _source(monkeypatch, "g65-bt", id=10, kind="bluetopo", bt_res=(4,), path=None, nodata=float("nan"))
    _region(monkeypatch, "g65bt", ["g65-bt"], cut=True, reach=0.2)
    asked = []

    def tiles_in(scheme, box, res):
        asked.append((scheme, tuple(box), tuple(res)))
        raise _Stop()

    monkeypatch.setattr(BT, "read_scheme", lambda p: "SCHEME:" + os.path.basename(p))
    monkeypatch.setattr(BT, "tiles_in", tiles_in)
    with pytest.raises(_Stop):
        B.build("g65bt", str(tmp_path / "out"), version=1, workers=1,
                local={"bluetopo:scheme": str(tmp_path / "scheme.gpkg")})
    assert asked == [("SCHEME:scheme.gpkg", B.reach_box(BOX, 0.2), (4,))]


# ---------------------------------------------------------------- BlueTopo: tiles one by one

def test_read_takes_each_pixel_from_the_first_tile_with_a_value(monkeypatch):
    calls = []

    def warp(path, b, size, resampling, nodata=None, key="?"):
        calls.append(path)
        a = np.full((size, size), {"fine": 1.0, "coarse": 2.0, "far": 9.0}[path], np.float32)
        if path == "fine":
            a[:, : size // 2] = np.nan                 # the fine tile has nothing on the left half
        return a
    monkeypatch.setattr(B, "_warp", warp)
    s = {"key": "bt", "tiles": [("fine", (0.0, 0.0, 1.0, 1.0)), ("coarse", (0.0, 0.0, 1.0, 1.0)),
                                ("far", (5.0, 5.0, 6.0, 6.0))]}
    a = B._read(s, None, 4, None, (0.2, 0.2, 0.8, 0.8))
    assert calls == ["fine", "coarse"]                 # a tile not meeting the block is never read
    assert (a[:, :2] == 2.0).all() and (a[:, 2:] == 1.0).all()
    # a source without tiles: its own file
    calls.clear()
    monkeypatch.setattr(B, "_warp", lambda path, *a, **k: calls.append(path) or np.zeros((4, 4), np.float32))
    B._read({"key": "f", "tiles": None, "gdal": "file.vrt"}, None, 4, None, (0, 0, 1, 1))
    assert calls == ["file.vrt"]


def test_footprint_of_a_tiled_source_is_its_tiles():
    assert B.footprint({"tiles": [("p1", (1.0, 2.0, 3.0, 4.0)), ("p2", (5.0, 6.0, 7.0, 8.0))]}) == [
        (1.0, 2.0, 3.0, 4.0), (5.0, 6.0, 7.0, 8.0)]
    assert B.footprint({"tiles": []}) == []


LINK = BT.HOST + "BlueTopo/%s/BlueTopo_%s_20250101.tiff"


def _geom(x0, y0, x1, y1, srs=4326, little=True, flags=None):
    order = "<" if little else ">"
    f = ((1 << 1) | (1 if little else 0)) if flags is None else flags
    return b"GP" + bytes([0, f]) + struct.pack(order + "i", srs) + struct.pack(order + "4d", x0, x1, y0, y1)


def _scheme(path, rows, tables=("tiles",)):
    con = sqlite3.connect(str(path))
    con.execute("create table gpkg_contents (table_name text, data_type text)")
    for t in tables:
        con.execute("insert into gpkg_contents values (?, 'features')", (t,))
        con.execute('create table "%s" (tile text, geom blob, GeoTIFF_Link text, Resolution text)' % t)
    con.executemany('insert into "%s" values (?, ?, ?, ?)' % tables[0], rows)
    con.commit()
    con.close()
    return str(path)


def test_read_scheme_and_tiles_in_finest_first(tmp_path):
    rows = [("BH4AA111", _geom(-80.0, 30.0, -79.0, 31.0), LINK % ("BH4AA111", "BH4AA111"), "4m"),
            ("BH2AA222", _geom(-80.0, 30.0, -79.5, 30.5), LINK % ("BH2AA222", "BH2AA222"), "2m"),
            ("BH8AA333", _geom(-80.0, 30.0, -78.0, 32.0), LINK % ("BH8AA333", "BH8AA333"), "8m"),
            ("BH4AA444", _geom(-70.0, 40.0, -69.0, 41.0), LINK % ("BH4AA444", "BH4AA444"), "4m"),
            ("BH4AA555", _geom(-80.0, 30.0, -79.0, 31.0), None, "4m"),                    # not delivered
            ("BH4AA666", _geom(-80.0, 30.0, -79.0, 31.0), "", "4m")]
    sch = BT.read_scheme(_scheme(tmp_path / "s.gpkg", rows))
    assert [t[0] for t in sch] == ["BH4AA111", "BH2AA222", "BH8AA333", "BH4AA444"]
    assert sch[0][1] == (-80.0, 30.0, -79.0, 31.0) and sch[0][2] == 4
    assert sch[0][3] == "/vsis3/%s/BlueTopo/BH4AA111/BlueTopo_BH4AA111_20250101.tiff" % BT.BUCKET
    box = (-79.9, 30.1, -79.1, 30.9)
    got = BT.tiles_in(sch, box, (2, 4))
    assert [p.split("/")[-2] for p, _ in got] == ["BH2AA222", "BH4AA111"]          # finest first, 8 m and far out
    assert [p.split("/")[-2] for p, _ in BT.tiles_in(sch, box, 8)] == ["BH8AA333"]
    assert BT.tiles_in(sch, (0.0, 0.0, 1.0, 1.0), (2, 4, 8)) == []
    # an edge touching the box is not a meeting
    assert [p.split("/")[-2] for p, _ in BT.tiles_in(sch, (-79.0, 30.1, -78.5, 30.9), 4)] == []


@pytest.mark.parametrize("row,msg", [
    (("BH4AA111", _geom(-80.0, 30.0, -79.0, 31.0), LINK % ("BH4XX999", "BH4XX999"), "4m"), "links to"),
    (("BH4AA111", _geom(-80.0, 30.0, -79.0, 31.0), LINK % ("BH4AA111", "BH4AA111"), "4 m"), "has resolution"),
    (("BH4AA111", _geom(-80.0, 30.0, -79.0, 31.0), LINK % ("BH4AA111", "BH4AA111"), None), "has resolution"),
    (("BH4AA111", _geom(-80.0, 30.0, -79.0, 31.0), "https://example.com/x.tiff", "4m"), "not a BlueTopo tile link"),
    (("BH4AA111", _geom(-80.0, 30.0, -79.0, 31.0, srs=3857), LINK % ("BH4AA111", "BH4AA111"), "4m"), "srs"),
])
def test_read_scheme_refuses_a_changed_scheme(tmp_path, row, msg):
    with pytest.raises(ValueError, match=msg):
        BT.read_scheme(_scheme(tmp_path / "s.gpkg", [row]))


def test_read_scheme_wants_one_feature_table_and_a_delivered_tile(tmp_path):
    with pytest.raises(ValueError, match="2 feature tables"):
        BT.read_scheme(_scheme(tmp_path / "two.gpkg", [], tables=("a", "b")))
    with pytest.raises(ValueError, match="no delivered tile"):
        BT.read_scheme(_scheme(tmp_path / "none.gpkg",
                               [("BH4AA111", _geom(-80.0, 30.0, -79.0, 31.0), None, "4m")]))


def test_envelope_reads_the_standard_header_only():
    assert BT.envelope(_geom(-80.0, 30.0, -79.0, 31.0)) == (-80.0, 30.0, -79.0, 31.0)
    assert BT.envelope(_geom(-80.0, 30.0, -79.0, 31.0, little=False)) == (-80.0, 30.0, -79.0, 31.0)
    for bad, msg in [(_geom(-80.0, 30.0, -79.0, 31.0, srs=3857), "srs"),
                     (_geom(-80.0, 30.0, -79.0, 31.0, flags=1), "envelope"),
                     (_geom(-79.0, 30.0, -80.0, 31.0), "out of range"),
                     (_geom(-80.0, 30.0, 190.0, 31.0), "out of range"),
                     (b"GP\x00\x03" + b"\x00" * 20, "not a GeoPackage"),
                     (b"XX\x00\x03" + b"\x00" * 40, "not a GeoPackage"),
                     ("text", "not a GeoPackage")]:
        with pytest.raises(ValueError, match=msg):
            BT.envelope(bad)


def test_newest_scheme_by_date_in_a_namespaced_listing():
    xml = ('<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
           "<Contents><Key>BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme_20250301_120000.gpkg</Key></Contents>"
           "<Contents><Key>BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme_20260101_000000.gpkg</Key></Contents>"
           "<Contents><Key>BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme_20251231_235959.gpkg</Key></Contents>"
           "<Contents><Key>BlueTopo/_BlueTopo_Tile_Scheme/readme.txt</Key></Contents></ListBucketResult>")
    assert BT.newest_scheme(xml).endswith("20260101_000000.gpkg")
    assert BT.newest_scheme(xml.replace(' xmlns="http://s3.amazonaws.com/doc/2006-03-01/"', "")).endswith(
        "20260101_000000.gpkg")
    with pytest.raises(ValueError, match="no BlueTopo tile scheme"):
        BT.newest_scheme("<ListBucketResult><Contents><Key>x</Key></Contents></ListBucketResult>")


def test_fetch_scheme_checks_the_listing_and_the_file(tmp_path):
    key = "BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme_20260101_000000.gpkg"
    listing = ("<ListBucketResult><Contents><Key>%s</Key></Contents></ListBucketResult>" % key).encode()
    good = b"SQLite format 3\x00" + b"\x00" * 2000
    dest = str(tmp_path / "s.gpkg")

    def fake(listing_body, data):
        return lambda url: listing_body if url == BT.listing_url() else (data if url == BT.HOST + key else None)
    assert BT.fetch_scheme(dest, fake(listing, good)) == key
    assert open(dest, "rb").read() == good
    for listing_body, data, msg in [(listing + b" " * BT.MAX_LISTING_BYTES, good, "listing"),
                                    (listing, b"PK\x03\x04" + b"\x00" * 2000, "not a GeoPackage"),
                                    (listing, b"SQLite format 3\x00", "not a GeoPackage"),
                                    (listing, b"SQLite format 3\x00" + b"\x00" * BT.MAX_SCHEME_BYTES,
                                     "not a GeoPackage")]:
        with pytest.raises(ValueError, match=msg):
            BT.fetch_scheme(str(tmp_path / "x.gpkg"), fake(listing_body, data))


# ---------------------------------------------------------------- the datum grids' station rules

def _stations():
    return {"a": {"lat": 30.0, "lon": -80.0, "MSL": 1.2, "NAVD88": 0.5, "MHHW": 1.9},
            "b": {"lat": 31.0, "lon": -80.0, "MSL": 0.4, "NAVD88": 0.6, "MHHW": 1.0},
            "c": {"lat": 32.0, "lon": -80.0, "MSL": 2.0, "NAVD88": 1.0, "MHHW": 2.5},
            "d": {"lat": 33.0, "lon": -80.0, "MSL": 3.0, "NAVD88": 2.0, "MHHW": 3.1},
            "e": {"lat": 34.0, "lon": -80.0, "MSL": 1.0, "NAVD88": 0.9, "MHHW": 1.3},
            "typo": {"lat": 35.0, "lon": -80.0, "MSL": 20.0, "NAVD88": 0.0, "MHHW": 26.0},
            "none": {"lat": 36.0, "lon": -80.0, "MSL": 1.0, "NAVD88": None, "MHHW": None}}


def test_points_take_msl_above_navd88_and_mhhw_above_msl():
    lat, lon, val, ids = DG.points(_stations(), "msl_navd88")
    assert ids.tolist() == ["a", "b", "c", "d", "e"]                 # typo and the station without NAVD88 left out
    assert np.allclose(val, [0.7, -0.2, 1.0, 1.0, 0.1])
    lat, lon, val, ids = DG.points(_stations(), "mhhw_msl")
    assert ids.tolist() == ["a", "b", "c", "d", "e"]
    assert np.allclose(val, [0.7, 0.6, 0.5, 0.1, 0.3])
    few = {k: v for k, v in _stations().items() if k in ("a", "b", "c", "d")}
    with pytest.raises(ValueError, match="too few stations"):
        DG.points(few, "msl_navd88")


def test_idw_weighs_by_the_inverse_square_with_a_1_km_floor():
    slat = np.array([0.0, 0.0, 0.0])
    slon = np.array([0.1, -0.2, 1.0])                  # 11.1 km, twice that, far
    sval = np.array([1.0, 4.0, 100.0])
    v = DG.idw(np.array([0.0]), np.array([0.0]), slat, slon, sval, near=2)
    assert v[0] == pytest.approx((4 * 1.0 + 1 * 4.0) / 5, abs=1e-6)       # weights 4 : 1
    near = DG.idw(np.array([0.0]), np.array([0.0]), np.array([0.0, 0.0]), np.array([0.001, 0.005]),
                  np.array([2.0, 6.0]), near=2)
    assert near[0] == pytest.approx(4.0, abs=1e-6)                        # both inside 1 km: equal weights


def test_drop_outliers_drops_the_worst_one_at_a_time_and_keeps_near_plus_one():
    n = 9
    slat = np.zeros(n)
    slon = np.arange(n) * 0.1
    sval = slon * 0.5
    sval[4] += 2.0
    ids = np.array(["s%d" % i for i in range(n)])
    lat, lon, val, kept, dropped = DG.drop_outliers(slat, slon, sval, ids, 0.3, near=4)
    assert [d[0] for d in dropped] == ["s4"] and dropped[0][1] > 1.0
    assert kept.tolist() == [i for i in ids.tolist() if i != "s4"]
    small = slice(0, 5)                                  # exactly near + 1 stations: nothing is ever dropped
    _, _, _, k2, d2 = DG.drop_outliers(slat[small], slon[small], sval[small], ids[small], 0.3, near=4)
    assert d2 == [] and len(k2) == 5
    mid = slon * 0.5
    mid[4] += 0.5                                        # a half-metre step: above 0.3 m, not above 0.9 m
    assert [d[0] for d in DG.drop_outliers(slat, slon, mid, ids, 0.3, near=4)[4]] == ["s4"]
    assert DG.drop_outliers(slat, slon, mid, ids, 0.5, near=4)[4] == []        # at the limit: kept (strictly above)
    assert DG.LOO_MAX == {"msl_navd88": 0.3}              # MHHW-MSL keeps every station


# ---------------------------------------------------------------- the region and source tables

def _inside(box, lat, lon):
    return box[0] <= lon <= box[2] and box[1] <= lat <= box[3]


def _owners(lat, lon, names):
    out = []
    for n in names:
        r = S.REGIONS[n]
        if _inside(r["box"], lat, lon) and not any(_inside(S.REGIONS[e]["box"], lat, lon) for e in r.get("exclude", ())):
            out.append(n)
    return out


OCEAN = list(S.US_BOXES)
LAKES = list(S.LAKE_BOXES)


@pytest.mark.parametrize("lat,lon,region", [
    (30.32, -81.66, "us-gulf-east"),       # Jacksonville: the St Johns, read through the reach to its mouth
    (30.40, -81.40, "us-fl-east"),         # the St Johns' mouth
    (40.50, -74.30, "us-sne-ny"),          # the Raritan
    (42.74, -73.69, "us-sne-ny"),          # the Hudson at Troy
    (30.71, -81.45, "us-sc-ga"),           # the St Marys' mouth (left out of the eastern Gulf late)
    (30.75, -81.70, "us-sc-ga"),           # the eastern Gulf's corner north of 30.7 N and east of 82.0 W
    (43.05, -79.00, "lake-erie"),          # the Niagara River
    (46.45, -84.20, "lake-huron"),         # the lower St Marys River
    (46.50, -84.45, "lake-superior"),      # Huron's corner west of 84.36 W
    (42.60, -82.70, "lake-stclair"),
    (43.50, -79.70, "lake-ontario"),       # Huron's corner over Lake Ontario
])
def test_each_place_belongs_to_one_region(lat, lon, region):
    names = LAKES if region.startswith("lake-") else OCEAN
    assert _owners(lat, lon, names) == [region]


@pytest.mark.parametrize("names", [OCEAN, LAKES])
def test_region_boxes_overlap_only_where_one_leaves_the_other_out(names):
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            ra, rb = S.REGIONS[a]["box"], S.REGIONS[b]["box"]
            w, s, e, n = max(ra[0], rb[0]), max(ra[1], rb[1]), min(ra[2], rb[2]), min(ra[3], rb[3])
            if w < e and s < n:
                assert b in S.REGIONS[a].get("exclude", ()) or a in S.REGIONS[b].get("exclude", ()), (a, b)


def test_the_ocean_regions_and_their_sources_are_pinned():
    for name in OCEAN:
        r = S.REGIONS[name]
        assert r["cut"] is True and r["reach"] == S.REACH_DEG and r["mhhw"] == "grid" and r["seed"] == "sea"
        assert r["zmin"] == 8 and r["prio"] == 10
        assert r["sources"] == list(S.US_BOXES[name][1])
    assert S.REACH_DEG == 0.3
    assert S.REGIONS["us-gulf-east"]["exclude"] == ["us-sc-ga"] and S.REGIONS["us-gulf-east"]["exclude_late"] is True
    assert not any(S.REGIONS[n].get("exclude_late") for n in OCEAN if n != "us-gulf-east")
    assert S.US_BOXES["us-fl-east"][0] == (-81.6, 26.0, -79.7, 30.7)
    assert S.US_BOXES["us-gulf-east"][0] == (-88.45, 26.0, -81.6, 31.2)
    assert S.US_BOXES["us-sne-ny"][0] == (-74.5, 40.4, -69.8, 42.8)       # the Hudson and the Raritan
    assert S.US_BOXES["us-me-nh"][0][1] == 42.8                            # south to Cape Ann
    assert S.NAVD88["geoid"] == "msl_navd88" and S.NAVD88["offset"] == 0.0
    bt4, bt8, bt16 = S.SOURCES["bluetopo4"], S.SOURCES["bluetopo8"], S.SOURCES["bluetopo16"]
    assert (bt4["id"], bt4["bt_res"], bt4["zmax"], bt4["deep"]) == (16, (2, 4), 15, (100.0, 13))
    assert (bt8["id"], bt8["bt_res"], bt8["zmax"], bt8["deep"]) == (22, (8,), 14, (100.0, 13))
    assert (bt16["id"], bt16["bt_res"], bt16["zmax"]) == (23, (16,), 13)
    for k in ("bluetopo4", "bluetopo8", "bluetopo16", "cudem-us", "cudem-us-b", "cudem3-us", "cudem3-us-b"):
        s = S.SOURCES[k]
        assert s["clip"] is True and s["geoid"] == "msl_navd88" and s["offset"] == 0.0, k
    for k in ("bluetopo4", "bluetopo8", "bluetopo16"):
        assert S.SOURCES[k]["kind"] == "bluetopo" and math.isnan(S.SOURCES[k]["nodata"])
    ids = [S.SOURCES[k]["id"] for k in ("cudem-us", "cudem-us-b", "bluetopo4", "cudem3-us", "cudem3-us-b",
                                        "bluetopo8", "bluetopo16")]
    assert ids == sorted(ids) and len(set(ids)) == len(ids)          # CUDEM 1/9 first, BlueTopo 16 m last


def test_every_ocean_region_passes_region_config():
    for name in OCEAN:
        reg = B.region_config(name)
        assert reg["keep_box"] == tuple(S.REGIONS[name]["box"])
        assert reg["reach_box"] == B.reach_box(tuple(S.REGIONS[name]["box"]), S.REACH_DEG)
