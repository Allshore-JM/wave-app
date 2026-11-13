"""The bathymetry rebuild of plan section 93b (G61 decisions, 2026-11-12): the land marker on the source plane, a second,
coarser source filling a finer one's gaps (bilinear, its own earned zooms, the deep rule), land-only cores never written,
the GEBCO shore fill (equal to filling the whole world at once), the check's plane rules and the source-id rules.
Needs scipy and rasterio (the bathy-build workflow installs them); skipped elsewhere."""
import os
import shutil
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
import build_bathy as B  # noqa: E402

LAND = T.LAND_ID


# ---------------------------------------------------------------- the land rule when halving

def _pool_one(depths, srcs):
    d = np.array(depths, np.float32).reshape(2, 2)
    s = np.array(srcs, np.uint8).reshape(2, 2)
    out, so = T.pool2(d, s)
    return float(out[0, 0]), int(so[0, 0])


def test_pool2_keeps_land_apart_from_outside_the_survey():
    nan = np.nan
    # four land pixels -> land
    d, s = _pool_one([nan] * 4, [LAND] * 4)
    assert np.isnan(d) and s == LAND
    # one land, one water, two outside: land and water make two -> land (no depth: one water pixel is not water)
    d, s = _pool_one([nan, 5.0, nan, nan], [LAND, 10, 0, 0])
    assert np.isnan(d) and s == LAND
    # one land, three outside -> outside (an edge of the survey that is mostly outside stays outside)
    d, s = _pool_one([nan] * 4, [LAND, 0, 0, 0])
    assert np.isnan(d) and s == 0
    # two land, two outside -> land
    d, s = _pool_one([nan] * 4, [LAND, LAND, 0, 0])
    assert np.isnan(d) and s == LAND
    # two water, two land -> water, the water's own source and flags (land never adds a flag)
    d, s = _pool_one([4.0, 6.0, nan, nan], [10 | T.FLAG_FILLED, 20, LAND, LAND])
    assert d == pytest.approx(5.0) and s == (10 | T.FLAG_FILLED)
    # one water, three outside -> outside
    d, s = _pool_one([4.0, nan, nan, nan], [10, 0, 0, 0])
    assert np.isnan(d) and s == 0
    # a source byte with LAND_ID's bits under water is never taken for land (land = no depth AND the land id)
    d, s = _pool_one([4.0, 6.0, 8.0, nan], [10, 10, 10, LAND])
    assert d == pytest.approx(6.0) and s == 10


# ---------------------------------------------------------------- earned zooms

def test_zoom_tables_and_earns():
    reg = {"srcs": [dict(id=10, zmax=15), dict(id=20, zmax=14, deep=(200.0, 12))]}
    tables = B.zoom_tables(reg)
    depth = np.array([5.0, 5.0, 150.0, 250.0, np.nan, np.nan], np.float32)
    src = np.array([10, 20, 20, 20 | T.FLAG_FILLED, LAND, 0], np.uint8)
    assert list(B.earns(depth, src, tables, 15)) == [True, False, False, False, False, False]
    assert list(B.earns(depth, src, tables, 14)) == [True, True, True, False, False, False]
    assert list(B.earns(depth, src, tables, 13)) == [True, True, True, False, False, False]
    assert list(B.earns(depth, src, tables, 12)) == [True, True, True, True, False, False]
    # exactly at the deep line is not deeper than it
    assert B.earns(np.array([200.0], np.float32), np.array([20], np.uint8), tables, 14)[0]
    # an unknown id (0) earns nothing, even with a depth
    assert not B.earns(np.array([5.0], np.float32), np.array([0], np.uint8), tables, 1)[0]


# ---------------------------------------------------------------- encode_level and land-only cores

def test_encode_level_writes_no_land_only_core_and_lends_its_edge(tmp_path):
    work, out = str(tmp_path / "w"), str(tmp_path / "o")
    z = 5
    nan = np.full((T.TILE, T.TILE), np.nan, np.float32)
    B._core.cache_clear()
    B.save_core(work, z, 10, 10, nan, np.full((T.TILE, T.TILE), LAND, np.uint8))           # land only
    B.save_core(work, z, 11, 10, np.full((T.TILE, T.TILE), 12.0, np.float32), np.full((T.TILE, T.TILE), 10, np.uint8))
    written = B.encode_level(work, out, z)
    assert written == [(z, 11, 10)]
    code, sf = T.unpack_tile(open(B.tile_path(out, z, 11, 10), "rb").read())
    assert (code[1:-1, 0] == 0).all() and (sf[1:-1, 0] == LAND).all()      # the left apron = the land core's edge
    assert (code[1:-1, -1] == 0).all() and (sf[1:-1, -1] == 0).all()        # nothing to the right: outside
    # with earned zooms: a source that earns only zoom 4 writes nothing at zoom 5
    shutil.rmtree(out)
    B._core.cache_clear()
    assert B.encode_level(work, out, z, tables=B.zoom_tables({"srcs": [dict(id=10, zmax=4)]})) == []
    assert B.encode_level(work, out, z, tables=B.zoom_tables({"srcs": [dict(id=10, zmax=5)]})) == [(z, 11, 10)]
    B._core.cache_clear()


# ---------------------------------------------------------------- the GEBCO shore fill

def _grow_world(world, steps):
    """Independent of build_bathy._grow: the whole world at once, columns wrapping (np.roll), rows not."""
    d = world.astype(np.float64).copy()
    h, w = d.shape
    for _ in range(steps):
        tot = np.zeros_like(d)
        n = np.zeros(d.shape, np.int64)
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                if not (dy or dx):
                    continue
                sh = np.roll(d, -dx, axis=1)                 # value at column (c + dx), wrapping
                if dy:
                    sh2 = np.full_like(sh, np.nan)
                    if dy > 0:
                        sh2[:-1] = sh[1:]
                    else:
                        sh2[1:] = sh[:-1]
                    sh = sh2
                ok = np.isfinite(sh)
                tot += np.where(ok, sh, 0.0)
                n += ok
        new = np.isnan(d) & (n > 0)
        d[new] = tot[new] / n[new]
    return d


def test_gebco_fill_equals_filling_the_whole_world(tmp_path):
    z, steps = 2, 2
    n = 2 ** z
    size = n * T.TILE
    rng = np.random.default_rng(93)
    yy, xx = np.mgrid[0:size, 0:size]
    water = np.zeros((size, size), bool)
    # blobs, some across tile edges, one across the date line (columns 0 / size - 1), one touching the world's top row
    for (cx, cy, r) in ((300, 300, 120), (512, 700, 90), (5, 500, 60), (size - 3, 200, 40), (800, 2, 30),
                        (255, 900, 25), (768, 256, 3)):
        dx = np.minimum(np.abs(xx - cx), size - np.abs(xx - cx))
        water |= dx ** 2 + (yy - cy) ** 2 <= r ** 2
    raw = np.where(water, rng.uniform(50.0, 3000.0, (size, size)), np.nan).astype(np.float32)
    world = np.where(water, T.DECODE[T.encode_depth(raw)], np.nan).astype(np.float32)   # what the cores hold
    gid = S.SOURCES["gebco"]["id"] | T.FLAG_COARSE
    work = str(tmp_path / "w")
    B._core.cache_clear()
    saved = set()
    for ty in range(n):
        for tx in range(n):
            blk = world[ty * T.TILE:(ty + 1) * T.TILE, tx * T.TILE:(tx + 1) * T.TILE]
            if np.isfinite(blk).any():
                B.save_core(work, z, tx, ty, blk, np.where(np.isfinite(blk), gid, 0).astype(np.uint8))
                saved.add((tx, ty))
    filled = B.gebco_fill(work, z, steps=steps)          # the default: the world wraps at the date line
    expect = _grow_world(world, steps)
    new = np.isnan(world) & np.isfinite(expect)
    assert filled == int(new.sum()) and filled > 0
    for ty in range(n):
        for tx in range(n):
            sl = (slice(ty * T.TILE, (ty + 1) * T.TILE), slice(tx * T.TILE, (tx + 1) * T.TILE))
            c = B._core(work, z, tx, ty)
            if c is None:
                assert not np.isfinite(expect[sl]).any(), (tx, ty)
                continue
            d, s = c
            got_code = T.encode_depth(d)
            want_code = T.encode_depth(expect[sl].astype(np.float32))
            assert np.array_equal(got_code > 0, want_code > 0), (tx, ty)
            assert np.abs(got_code.astype(int) - want_code.astype(int)).max() <= 1, (tx, ty)
            old = np.isfinite(world[sl])
            assert np.array_equal(got_code[old], T.encode_depth(world[sl])[old])            # never moved
            assert (s[old] == gid).all()
            nw = new[sl]
            assert (s[nw] == (gid | T.FLAG_FILLED)).all()
            assert (s[~old & ~nw] == 0).all()
            if (tx, ty) not in saved:
                assert nw.any()                                   # a new core only where cells were added
    assert not os.path.exists(os.path.join(work, "fill_%d" % z))
    B._core.cache_clear()


def test_gebco_fill_reaches_two_cells_by_default():
    """Owner decision 2026-11-12: GEBCO water carried one or two cells onto the land; the build uses two."""
    assert B.GEBCO_FILL_STEPS == 2


def test_coarse_levels_keep_land_only_cores(tmp_path):
    """A core of land only is saved when halved (the land rule), so a coarser tile that also holds water keeps it: water
    at core 0/0, land only at 2/0 and 3/0 of zoom 3; their zoom-1 parent shows the land on its right half."""
    work, out = str(tmp_path / "w"), str(tmp_path / "o")
    B._core.cache_clear()
    water = np.full((T.TILE, T.TILE), 50.0, np.float32)
    nan = np.full((T.TILE, T.TILE), np.nan, np.float32)
    B.save_core(work, 3, 0, 0, water, np.full((T.TILE, T.TILE), 10, np.uint8))
    for x in (2, 3):
        B.save_core(work, 3, x, 0, nan, np.full((T.TILE, T.TILE), LAND, np.uint8))
    B.coarse_levels(work, out, 3, 1)
    d, s = B._core(work, 1, 0, 0)
    assert np.isfinite(d[:T.TILE // 4, :T.TILE // 4]).all()
    assert (s[:T.TILE // 4, T.TILE // 2:] == LAND).all()
    B._core.cache_clear()


def test_gebco_fill_twice_is_four_cells(tmp_path):
    """The fill reaches exactly `steps` cells: a lone water pixel grows to a (2 * steps + 1) square."""
    z = 1
    work = str(tmp_path / "w")
    B._core.cache_clear()
    d = np.full((T.TILE, T.TILE), np.nan, np.float32)
    d[100, 100] = 1000.0
    B.save_core(work, z, 0, 0, d, np.where(np.isfinite(d), 31 | T.FLAG_COARSE, 0).astype(np.uint8))
    assert B.gebco_fill(work, z, steps=2, wrap=True) == 24
    got, _ = B._core(work, z, 0, 0)
    fin = np.isfinite(got)
    assert fin.sum() == 25 and fin[98:103, 98:103].all()
    assert np.allclose(got[fin], 1000.0)
    B._core.cache_clear()


# ---------------------------------------------------------------- a region of two sources

SRC_A_RES, SRC_B_RES = 0.0004, 0.002
WEST, EAST, SOUTH, NORTH = -158.12, -157.38, 21.12, 21.64
A_EAST = -157.6                                       # the fine source ends here
POND = (-158.100, -158.090, 21.30, 21.31)
SHORE = -158.05


def field(lon, lat):
    """Elevation (m, up). West of -157.5 a shelf (0 to -13.75 m), east of it a slope to -73.75 m; a pond in the land."""
    lon = np.asarray(lon, np.float64)
    lat = np.asarray(lat, np.float64)
    e = np.where(lon < -157.5, -25.0 * (lon - SHORE), -13.75 - 500.0 * (lon + 157.5))
    pond = (lon >= POND[0]) & (lon < POND[1]) & (lat >= POND[2]) & (lat < POND[3])
    return np.where(pond, -3.0, e)


def _write(path, west, east, res):
    w = int(round((east - west) / res))
    h = int(round((NORTH - SOUTH) / res))
    lons = west + (np.arange(w) + 0.5) * res
    lats = NORTH - (np.arange(h) + 0.5) * res
    data = field(lons[None, :], lats[:, None]).astype(np.float32)
    with rasterio.open(path, "w", driver="GTiff", width=w, height=h, count=1, dtype="float32", crs="EPSG:4326",
                       transform=from_origin(west, NORTH, res, res), nodata=-9999.0, tiled=True, blockxsize=256,
                       blockysize=256, compress="deflate") as d:
        d.write(data, 1)


@pytest.fixture(scope="module")
def two(tmp_path_factory, request):
    root = tmp_path_factory.mktemp("bathy2")
    a, b = str(root / "a.tif"), str(root / "b.tif")
    _write(a, WEST, A_EAST, SRC_A_RES)
    _write(b, WEST, EAST, SRC_B_RES)
    mp = pytest.MonkeyPatch()
    mp.setitem(S.SOURCES, "fine", dict(S.CUDEM, id=10, zmax=12, year="2099", path="a.tif", datum="test", name="fine",
                                       credit="test", licence="test"))
    mp.setitem(S.SOURCES, "coarse", dict(S.CUDEM3, id=20, zmax=12, res_m=200, deep=(20.0, 11), year="2098",
                                         path="b.tif", datum="test", name="coarse", credit="test", licence="test"))
    mp.setitem(S.REGIONS, "two", dict(box=(WEST, SOUTH, EAST, NORTH), sources=["coarse", "fine"], mhhw=0.4, zmin=7,
                                      prio=10))
    request.addfinalizer(mp.undo)
    out_root = str(root / "out")
    index = B.build("two", out_root, version=1, workers=2, local={"fine": a, "coarse": b})
    return dict(out=os.path.join(out_root, "two", "v1"), index=index)


def _tile(out, z, x, y):
    p = B.tile_path(out, z, x, y)
    if not os.path.exists(p):
        return None
    return T.unpack_tile(open(p, "rb").read())


def _lonlat(z, x, y, i, j):
    x0, y0, x1, y1 = T.tile_bounds(z, x, y)
    p = (x1 - x0) / T.TILE
    return T.xy_to_lonlat(x0 + (i + 0.5) * p, y1 - (j + 0.5) * p)


def test_two_sources_build_and_check_clean(two):
    assert B.check(two["out"], log_fn=lambda s: None) == 0
    idx = two["index"]
    assert idx["land_id"] == LAND
    assert sorted(int(k) for k in idx["sources"]) == [10, 20]


def test_deep_water_of_the_coarse_source_earns_only_its_zoom(two):
    found = B.scan_tiles(two["out"])
    # zoom 12 tile x=257 (-157.41 to -157.32) holds only the coarse source's water, all deeper than 20 m: not written
    assert all(x <= 256 for (x, _) in found[12])
    assert any(x == 256 for (x, _) in found[12])               # shallow coarse water (13.75-57 m) earns zoom 12
    assert any(x == 128 for (x, _) in found[11])               # its zoom-11 parent is there


def test_the_coarse_source_is_read_bilinear_and_loses_to_the_fine_one(two):
    out = two["out"]
    found = B.scan_tiles(out)
    rng = np.random.default_rng(11)
    checked = {10: 0, 20: 0}
    for z in (12, 11):
        for (x, y) in sorted(found[z]):
            code, sf = _tile(out, z, x, y)
            for _ in range(60):
                i, j = (int(v) for v in rng.integers(0, 256, 2))
                lon, lat = _lonlat(z, x, y, i, j)
                if not (WEST + 0.02 < lon < EAST - 0.02 and SOUTH + 0.02 < lat < NORTH - 0.02):
                    continue
                if abs(lon + 157.5) < 0.006 or abs(lon - A_EAST) < 0.006 or abs(lon - SHORE) < 0.02:
                    continue                                   # the kink, the fine source's edge, the shoreline
                if POND[0] - 0.005 < lon < POND[1] + 0.005 and POND[2] - 0.005 < lat < POND[3] + 0.005:
                    continue
                c = int(code[j + 1, i + 1])
                if c == 0:
                    continue
                sid = int(sf[j + 1, i + 1]) & T.SRC_BITS
                e = float(field(lon, lat))
                d = float(T.DECODE[c])
                if lon < A_EAST:
                    assert sid == 10, (z, lon, lat)
                    tol = 0.6 * 25.0 * SRC_A_RES * (2 if z == 11 else 1) + 0.02
                else:
                    assert sid == 20, (z, lon, lat)
                    # bilinear on a linear field: near exact; "average" would copy the 0.002-degree cells (up to 0.5 m)
                    tol = 0.05
                assert abs(d - (-e)) <= tol, (z, lon, lat, sid, d, -e)
                checked[sid] += 1
    assert checked[10] > 100 and checked[20] > 100


def test_land_is_marked_and_outside_is_not(two):
    out = two["out"]
    found = B.scan_tiles(out)
    land_px = outside_px = 0
    for z in (9, 10):
        for (x, y) in sorted(found[z]):
            code, sf = _tile(out, z, x, y)
            p = (T.tile_bounds(z, x, y)[2] - T.tile_bounds(z, x, y)[0]) / T.TILE
            for j in range(0, 256, 4):
                for i in range(0, 256, 4):
                    lon, lat = _lonlat(z, x, y, i, j)
                    deg = p / 111320.0 * 3                     # three pixels, in degrees (roughly)
                    inside = WEST + deg < lon < EAST - deg and SOUTH + deg < lat < NORTH - deg
                    outside = lon < WEST - deg or lon > EAST + deg or lat < SOUTH - deg or lat > NORTH + deg
                    c, s = int(code[j + 1, i + 1]), int(sf[j + 1, i + 1])
                    if outside:
                        assert c == 0 and s == 0, (z, x, y, lon, lat, s)
                        outside_px += 1
                    elif inside and lon < SHORE - deg * 3 and float(field(lon, lat)) > 1.0:
                        assert c == 0 and s == LAND, (z, x, y, lon, lat, c, s)
                        land_px += 1
    assert land_px > 50 and outside_px > 50


def test_the_pond_is_land(two):
    out = two["out"]
    lon, lat = (POND[0] + POND[1]) / 2, (POND[2] + POND[3]) / 2
    for z in (11, 12):
        x, y = T.tile_of(lon, lat, z)
        t = _tile(out, z, x, y)
        if t is None:
            continue
        code, sf = t
        x0, y0, x1, y1 = T.tile_bounds(z, x, y)
        px, py = T.lonlat_to_xy(lon, lat)
        i = int((px - x0) / (x1 - x0) * T.TILE)
        j = int((y1 - py) / (y1 - y0) * T.TILE)
        assert code[j + 1, i + 1] == 0 and sf[j + 1, i + 1] == LAND, z


def test_coarser_zooms_keep_the_land(two):
    found = B.scan_tiles(two["out"])
    assert 7 in found
    hits = 0
    for (x, y) in found[7]:
        code, sf = _tile(two["out"], 7, x, y)
        hits += int(((code == 0) & (sf == LAND)).sum())
    assert hits > 0


# ---------------------------------------------------------------- the check's plane rules

def test_check_refuses_planes_that_disagree(two, tmp_path):
    work = str(tmp_path / "copy")
    shutil.copytree(two["out"], work)
    z = 12
    x, y = next((x, y) for (x, y) in sorted(B.scan_tiles(work)[z])
                if ((_tile(work, z, x, y)[0] == 0) & (_tile(work, z, x, y)[1] == LAND)).any())
    code, sf = _tile(work, z, x, y)
    jl, il = (int(v[0]) for v in np.nonzero((code == 0) & (sf == LAND)))
    jw, iw = (int(v[0]) for v in np.nonzero(code > 0))
    p = B.tile_path(work, z, x, y)
    for mutate in (lambda c, s: s.__setitem__((jl, il), 10),            # no depth, a source id (not land)
                   lambda c, s: s.__setitem__((jw, iw), LAND),          # a depth under the land mark
                   lambda c, s: s.__setitem__((jw, iw), LAND | T.FLAG_FILLED),
                   lambda c, s: s.__setitem__((jw, iw), 0)):            # a depth with no source
        c2, s2 = code.copy(), sf.copy()
        mutate(c2, s2)
        open(p, "wb").write(T.pack_tile(c2, s2))
        msgs = []
        B.check(work, log_fn=msgs.append)
        assert any("source plane disagrees with the depth plane" in m for m in msgs)
    open(p, "wb").write(T.pack_tile(code, sf))
    assert B.check(work, log_fn=lambda s: None) == 0                  # the land mark itself is fine


# ---------------------------------------------------------------- source ids

@pytest.mark.parametrize("ids", [(10, 10), (10, 30), (0, 10), (10, 32)])
def test_region_config_refuses_bad_source_ids(monkeypatch, ids):
    names = []
    for k, i in enumerate(ids):
        name = "s%d" % k
        monkeypatch.setitem(S.SOURCES, name, dict(S.CUDEM, id=i, path="x.tif", year="2099"))
        names.append(name)
    monkeypatch.setitem(S.REGIONS, "bad", dict(box=(0, 0, 1, 1), sources=names, mhhw=0.4, zmin=7, prio=10))
    with pytest.raises(ValueError):
        B.region_config("bad")


def test_every_published_region_has_good_ids():
    for name in ("hawaii", "marianas", "samoa", "prvi", "gebco"):    # the real ones (a fixture may add a test region)
        reg = B.region_config(name)
        ids = [s["id"] for s in reg["srcs"]]
        assert ids == sorted(ids) and len(set(ids)) == len(ids) and LAND not in ids
        for s in reg["srcs"]:
            if s.get("deep"):
                assert s["deep"][1] <= s["zmax"]
            if s["key"].startswith("cudem3"):            # the 1/3" grid: ~10 m, earns zoom 14, deeper than 200 m zoom 12
                assert (s["zmax"], s["res_m"], tuple(s["deep"])) == (14, 10, (200.0, 12))
            elif s["key"].startswith("cudem"):
                assert (s["zmax"], s.get("deep")) == (15, None)
        if name != "gebco":
            assert {s["key"].split("-")[0] for s in reg["srcs"]} == {"cudem", "cudem3"}


def _land_box(tmp_path, monkeypatch, pond, land_m):
    """Land west of 158.05 W at `land_m` above the sea, open deep sea east of it; with `pond`, a low pond (not joined to
    the sea) in z9 tile 30 (158.906-158.203 W). Built as region "lnd" (zmax 12, so tile 30 is one finest-zoom block)."""
    w, e, s, n, res = -158.25, -158.0, 21.20, 21.24, 0.0004      # open sea south-west of Oahu on GSHHG
    width, height = int(round((e - w) / res)), int(round((n - s) / res))
    lons = w + (np.arange(width) + 0.5) * res
    lats = n - (np.arange(height) + 0.5) * res
    lo, la = np.meshgrid(lons, lats)
    data = np.where(lo < -158.05, land_m, -60.0)
    if pond:
        data = np.where((lo > -158.24) & (lo < -158.22), -3.0, data)
    data = data.astype(np.float32)
    path = str(tmp_path / "l.tif")
    with rasterio.open(path, "w", driver="GTiff", width=width, height=height, count=1, dtype="float32",
                       crs="EPSG:4326", transform=from_origin(w, n, res, res), nodata=-9999.0) as d:
        d.write(data, 1)
    monkeypatch.setitem(S.SOURCES, "lnd", dict(S.CUDEM, id=10, zmax=12, year="2099", path="l.tif", datum="test",
                                                name="lnd", credit="test", licence="test"))
    monkeypatch.setitem(S.REGIONS, "lnd", dict(box=(w, s, e, n), sources=["lnd"], mhhw=0.4, zmin=7, prio=10))
    B._core.cache_clear()
    B.build("lnd", str(tmp_path / "out"), version=1, workers=1, local={"lnd": path})
    B._core.cache_clear()
    return os.path.join(str(tmp_path / "out"), "lnd", "v1")


def _pixel(out, z, lon, lat):
    x, y = T.tile_of(lon, lat, z)
    t = _tile(out, z, x, y)
    assert t is not None, (z, x, y)
    code, sf = t
    x0, y0, x1, y1 = T.tile_bounds(z, x, y)
    p = (x1 - x0) / T.TILE
    i = int((T.lonlat_to_xy(lon, lat)[0] - x0) / p) + 1               # +1: the apron
    j = int((y1 - T.lonlat_to_xy(lon, lat)[1]) / p) + 1
    return int(code[j, i]), int(sf[j, i])


@pytest.mark.parametrize("pond", [True, False])
@pytest.mark.parametrize("land_m", [5.0, 50.0])
def test_a_block_of_land_only_keeps_its_land_at_coarser_zooms(tmp_path, monkeypatch, pond, land_m):
    """A finest-zoom block holding land only saves its land for the coarser zooms: with a pond (low ground, read by pass 2)
    and without one (high land only, no low pixel: G61b F1, it used to read "outside" there). Land at +50 m clips to the
    code's top (+10 m, code 1) and is still land (G61b F5: no test had land above +10 m)."""
    out = _land_box(tmp_path, monkeypatch, pond, land_m)
    for z in (7, 8):
        for lon in (-158.245, -158.23, -158.21):
            assert _pixel(out, z, lon, 21.22) == (0, LAND), (z, lon)
    # no tile of its own at its zooms (no water there), and the build checks clean
    assert _tile(out, 9, *T.tile_of(-158.23, 21.22, 9)) is None
    assert B.check(out, log_fn=lambda *a: None) == 0


def test_save_land_core_keeps_nothing_for_an_empty_block(tmp_path):
    reg = {"work": str(tmp_path), "zmax": 12}
    B.save_land_core(reg, (30, 200), np.zeros((T.TILE * 8, T.TILE * 8), np.uint16))
    assert not os.path.exists(os.path.join(str(tmp_path), "c"))
    core = np.zeros((T.TILE * 8, T.TILE * 8), np.uint16)
    core[:16, :16] = 1                                        # +10 m and above: code 1, still land
    B.save_land_core(reg, (30, 200), core)
    with np.load(os.path.join(str(tmp_path), "c", "9", "30_200.npz")) as f:
        assert (f["src"][:2, :2] == LAND).all() and (f["src"][2:, :] == 0).all() and (f["code"] == 0).all()


def test_read_field_averages_a_finer_grid(tmp_path, monkeypatch):
    """A source finer than the block's pixels is averaged, not point-sampled (G61b F5): stripes of -10 m and -30 m two
    metres wide read about -20 m in every pixel; bilinear sampling would scatter them between -10 and -30."""
    w, s, step = -158.02, 21.20, 0.00002
    width, height = 1000, 1000
    lo = w + (np.arange(width) + 0.5) * step
    row = np.where((np.arange(width) // 1) % 2 == 0, -10.0, -30.0).astype(np.float32)
    data = np.tile(row, (height, 1))
    path = str(tmp_path / "f.tif")
    with rasterio.open(path, "w", driver="GTiff", width=width, height=height, count=1, dtype="float32",
                       crs="EPSG:4326", transform=from_origin(w, s + height * step, step, step), nodata=-9999.0) as d:
        d.write(data, 1)
    monkeypatch.setitem(S.SOURCES, "fine", dict(S.CUDEM, id=10, zmax=12, res_m=1, year="2099", path="f.tif",
                                                 datum="test", name="fine", credit="test", licence="test"))
    monkeypatch.setitem(S.REGIONS, "fine", dict(box=(w, s, w + width * step, s + height * step), sources=["fine"],
                                                 mhhw=0.4, zmin=7, prio=10))
    B.gdal_env()
    reg = B.region_config("fine", local={"fine": path})
    key = T.tile_of(w + 0.01, s + 0.01, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src = B.read_field(reg, key)
    inner = elev[np.isfinite(elev)]
    assert inner.size > 100 and lo.size == width
    core = np.sort(inner)[inner.size // 10: -inner.size // 10]      # pixels half on the grid's edge left out
    assert abs(float(np.median(core)) + 20.0) < 0.5
    assert float(core.std()) < 1.5


def test_check_holds_tiles_to_their_sources_earned_zooms(tmp_path, monkeypatch):
    """check() refuses a tile at a zoom no water pixel's source earns (G61b F6); an index without the zoom rules (v1)
    is not held to it."""
    import json
    out = _land_box(tmp_path, monkeypatch, False, 5.0)
    assert B.check(out, log_fn=lambda *a: None) == 0
    p = os.path.join(out, "index.json")
    index = json.load(open(p))
    assert index["sources"]["10"]["zmax"] == 12 and index["sources"]["10"]["deep"] is None
    index["sources"]["10"]["zmax"] = 11
    json.dump(index, open(p, "w"))
    lines = []
    assert B.check(out, log_fn=lines.append) > 0
    assert any("no water pixel whose source earns zoom 12" in l for l in lines)
    del index["sources"]["10"]["zmax"]
    json.dump(index, open(p, "w"))
    assert B.check(out, log_fn=lambda *a: None) == 0
