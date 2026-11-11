"""The bathymetry builder (tools/bathy/build_bathy.py, plan section 93b) on synthetic GeoTIFFs: the water rule across
block edges, values against the source field, tile aprons that agree with the neighbouring tile, the coarser zooms, the
index, the check and the upload order. Needs scipy and rasterio (the bathy-build workflow installs them); skipped elsewhere."""
import json
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


# ---------------------------------------------------------------- the water rule on its own

def _pack(m):
    return T.pack_bits(m)


def test_sea_masks_join_across_the_right_and_lower_edges():
    n = 8
    # block (0,0): a channel along row 3 reaching its right edge, no deep water; a pond at (6,1)
    low00 = np.zeros((n, n), bool)
    low00[3, 2:] = True
    low00[6, 1] = True
    # block (1,0): the channel continues on row 3 and turns down to the bottom edge at column 4
    low10 = np.zeros((n, n), bool)
    low10[3, :5] = True
    low10[3:, 4] = True
    # block (1,1): continues down from column 4 to a deep pool
    low11 = np.zeros((n, n), bool)
    low11[:5, 4] = True
    low11[4:7, 3:6] = True
    deep11 = np.zeros((n, n), bool)
    deep11[5, 4] = True
    none = np.zeros((n, n), bool)
    blocks = {(0, 0): (_pack(low00), _pack(none), n), (1, 0): (_pack(low10), _pack(none), n),
              (1, 1): (_pack(low11), _pack(deep11), n)}
    sea = W.sea_masks(blocks)
    s00 = T.unpack_bits(sea[(0, 0)], (n, n))
    s10 = T.unpack_bits(sea[(1, 0)], (n, n))
    s11 = T.unpack_bits(sea[(1, 1)], (n, n))
    assert np.array_equal(s00, low00 & ~(np.arange(n)[:, None] == 6))      # the channel, not the pond
    assert np.array_equal(s10, low10)
    assert np.array_equal(s11, low11)
    assert not s00[6, 1]


def test_sea_masks_do_not_join_diagonally_or_through_a_gap():
    n = 4
    a = np.zeros((n, n), bool)
    a[3, 3] = True                           # touches block (1,1) only at a corner
    b = np.zeros((n, n), bool)
    b[0, 0] = True
    deep = np.zeros((n, n), bool)
    deep[0, 0] = True
    c = np.zeros((n, n), bool)
    c[1, 0] = True                           # right neighbour of a, but on another row than a's low pixel
    blocks = {(0, 0): (_pack(a), _pack(np.zeros((n, n), bool)), n), (1, 1): (_pack(b), _pack(deep), n),
              (1, 0): (_pack(c), _pack(c), n)}
    sea = W.sea_masks(blocks)
    assert not T.unpack_bits(sea[(0, 0)], (n, n)).any()
    assert T.unpack_bits(sea[(1, 1)], (n, n))[0, 0]
    assert T.unpack_bits(sea[(1, 0)], (n, n))[1, 0]
    # inside one block too: a low pixel touching the sea only at a corner stays dry (4-neighbour joins)
    low = np.zeros((n, n), bool)
    low[0, 0] = low[1, 1] = True
    deep = np.zeros((n, n), bool)
    deep[0, 0] = True
    got = T.unpack_bits(W.sea_masks({(0, 0): (_pack(low), _pack(deep), n)})[(0, 0)], (n, n))
    assert got[0, 0] and not got[1, 1]


def test_deep_outside_the_low_mask_seeds_nothing():
    n = 4
    low = np.zeros((n, n), bool)
    low[0, 0] = True
    deep = np.zeros((n, n), bool)
    deep[3, 3] = True                        # deep but not low (a bad input): never a seed
    sea = W.sea_masks({(0, 0): (_pack(low), _pack(deep), n)})
    assert not T.unpack_bits(sea[(0, 0)], (n, n)).any()


def test_sea_around_places_the_neighbours_margins():
    n, m = 6, 2
    sea = {(5 + dx, 5 + dy): _pack(np.ones((n, n), bool)) for dx in (-1, 0, 1) for dy in (-1, 0, 1)}
    out = W.sea_around((5, 5), sea, n, m)
    assert out.shape == (n + 2 * m, n + 2 * m) and out.all()
    del sea[(6, 5)]                                         # the right neighbour missing -> its margin stays dry
    out = W.sea_around((5, 5), sea, n, m)
    assert not out[m:m + n, m + n:].any() and out[m:m + n, :m + n].all()
    # the margin is the neighbour's EDGE, not its far side
    left = np.zeros((n, n), bool)
    left[:, n - 1] = True                                   # only the left neighbour's last column is sea
    sea = {(5, 5): _pack(np.zeros((n, n), bool)), (4, 5): _pack(left)}
    out = W.sea_around((5, 5), sea, n, m)
    assert out[m:m + n, m - 1].all() and not out[m:m + n, 0].any()


def test_pool_any():
    m = np.zeros((4, 4), bool)
    m[1, 1] = True
    assert W.pool_any(m).tolist() == [[True, False], [False, False]]


# ---------------------------------------------------------------- the VRT footprint parser

def test_vrt_rects_reads_dst_rects_in_lon_lat():
    vrt = """<VRTDataset rasterXSize="400" rasterYSize="200">
      <GeoTransform> -160.0, 0.01, 0.0, 22.0, 0.0, -0.01</GeoTransform>
      <VRTRasterBand dataType="Float32" band="1">
        <ComplexSource><DstRect xOff="100" yOff="50" xSize="100" ySize="25" /></ComplexSource>
        <ComplexSource><DstRect xOff="0" yOff="0" xSize="10" ySize="10" /></ComplexSource>
      </VRTRasterBand></VRTDataset>"""
    r = B.vrt_rects(vrt)
    assert r[0] == pytest.approx((-159.0, 21.25, -158.0, 21.5))
    assert r[1] == pytest.approx((-160.0, 21.9, -159.9, 22.0))


# ---------------------------------------------------------------- a whole region from a synthetic GeoTIFF

SRC_RES = 0.0004
WEST, EAST, SOUTH, NORTH = -158.12, -157.38, 21.12, 21.64
POND = (-158.100, -158.090, 21.30, 21.31)          # a basin below MHHW inside the land: not water
ISLAND = (-157.72, -157.70, 21.40, 21.42)            # land inside the sea: no colour
SHORE = -158.05                                       # elevation 0 here


def field(lon, lat):
    """Elevation (m, positive up). West of -157.5 (block x=31 at zoom 9) a shallow shelf (0 to -13.75 m, no water deeper
    than 20 m: its sea is only reached through block x=32); east of it a slope to -73.75 m."""
    lon = np.asarray(lon, np.float64)
    lat = np.asarray(lat, np.float64)
    e = np.where(lon < -157.5, -25.0 * (lon - SHORE), -13.75 - 500.0 * (lon + 157.5))
    pond = (lon >= POND[0]) & (lon < POND[1]) & (lat >= POND[2]) & (lat < POND[3])
    isl = (lon >= ISLAND[0]) & (lon < ISLAND[1]) & (lat >= ISLAND[2]) & (lat < ISLAND[3])
    e = np.where(pond, -3.0, e)
    e = np.where(isl, 3.0, e)
    return e


def _write_source(path):
    w = int(round((EAST - WEST) / SRC_RES))
    h = int(round((NORTH - SOUTH) / SRC_RES))
    lons = WEST + (np.arange(w) + 0.5) * SRC_RES
    lats = NORTH - (np.arange(h) + 0.5) * SRC_RES
    data = field(lons[None, :], lats[:, None]).astype(np.float32)
    data[:3, :3] = -9999.0                              # a nodata corner
    with rasterio.open(path, "w", driver="GTiff", width=w, height=h, count=1, dtype="float32", crs="EPSG:4326",
                       transform=from_origin(WEST, NORTH, SRC_RES, SRC_RES), nodata=-9999.0,
                       tiled=True, blockxsize=256, blockysize=256, compress="deflate") as d:
        d.write(data, 1)


@pytest.fixture(scope="module")
def region(tmp_path_factory, request):
    root = tmp_path_factory.mktemp("bathy")
    src = str(root / "synth.tif")
    _write_source(src)
    mp = pytest.MonkeyPatch()
    mp.setitem(S.SOURCES, "synth", dict(S.CUDEM, id=10, zmax=12, year="2099", path="synth.tif", datum="test MSL",
                                        name="synthetic", credit="test", licence="test"))
    mp.setitem(S.REGIONS, "synth", dict(box=(WEST, SOUTH, EAST, NORTH), sources=["synth"], mhhw=0.4, zmin=7, prio=10))
    request.addfinalizer(mp.undo)
    out_root = str(root / "out")
    index = B.build("synth", out_root, version=3, workers=2, local={"synth": src})
    return dict(root=root, out=os.path.join(out_root, "synth", "v3"), index=index, src=src)


def _tile(out, z, x, y):
    p = B.tile_path(out, z, x, y)
    if not os.path.exists(p):
        return None
    return T.unpack_tile(open(p, "rb").read())


def _pixel_lonlat(z, x, y, i, j):
    """lon/lat of core pixel (row j, col i), 0-based inside the 256-px tile."""
    x0, y0, x1, y1 = T.tile_bounds(z, x, y)
    p = (x1 - x0) / T.TILE
    return T.xy_to_lonlat(x0 + (i + 0.5) * p, y1 - (j + 0.5) * p)


def test_region_builds_and_checks_clean(region):
    idx = region["index"]
    assert idx["format"] == B.FORMAT and idx["region"] == "synth" and idx["version"] == 3
    assert idx["zmin"] == 7 and idx["zmax"] == 12
    assert set(idx["tiles"]) == {"7", "8", "9", "10", "11", "12"}
    assert idx["tiles"]["12"] > 50
    assert B.check(region["out"], log_fn=lambda s: None) == 0
    assert os.path.exists(os.path.join(region["out"], "LICENSE.txt"))
    assert not os.path.exists(os.path.join(os.path.dirname(region["out"]), "..", "_work_synth"))


def test_values_match_the_source_field(region):
    out = region["out"]
    rng = np.random.default_rng(7)
    checked = 0
    for z in (12, 11):
        for (x, y) in sorted(B.scan_tiles(out)[z]):
            code, sf = _tile(out, z, x, y)
            for _ in range(40):
                i, j = (int(v) for v in rng.integers(0, 256, 2))
                lon, lat = _pixel_lonlat(z, x, y, i, j)
                if not (WEST + 0.01 < lon < EAST - 0.01 and SOUTH + 0.01 < lat < NORTH - 0.01):
                    continue
                if abs(lon + 157.5) < 0.004 or abs(lon - SHORE) < 0.01:
                    continue                                        # the slope's kink, the shoreline
                near_feature = any(b[0] - 0.003 < lon < b[1] + 0.003 and b[2] - 0.003 < lat < b[3] + 0.003
                                   for b in (POND, ISLAND))
                if near_feature:
                    continue
                e = float(field(lon, lat))
                c = int(code[j + 1, i + 1])
                if e > 0.4:
                    assert c == 0, (z, x, y, i, j, lon, lat, e, c)
                else:
                    assert c > 0, (z, x, y, i, j, lon, lat, e)
                    d = float(T.DECODE[c])
                    grad = 25.0 if lon < -157.5 else 500.0
                    tol = 0.6 * grad * SRC_RES * (2 if z == 11 else 1) + 0.02
                    assert abs(d - (-e)) <= tol, (z, lon, lat, d, -e)
                    assert int(sf[j + 1, i + 1]) & T.SRC_BITS == 10
                checked += 1
    assert checked > 500


def test_pond_and_island_have_no_water(region):
    out = region["out"]
    z = 12
    for (x, y) in B.scan_tiles(out)[z]:
        code, _ = _tile(out, z, x, y)
        for jj in range(0, 256, 2):
            for ii in range(0, 256, 2):
                lon, lat = _pixel_lonlat(z, x, y, ii, jj)
                for b in (POND, ISLAND):
                    if b[0] + 0.0012 < lon < b[1] - 0.0012 and b[2] + 0.0012 < lat < b[3] - 0.0012:
                        assert code[jj + 1, ii + 1] == 0, (b, lon, lat)


def test_the_shelf_is_water_through_the_neighbouring_block(region):
    """West of -157.5 nothing is deeper than 20 m: its water joins the sea only through block x=32."""
    out = region["out"]
    lon, lat = -157.8, 21.35
    x, y = T.tile_of(lon, lat, 12)
    code, _ = _tile(out, 12, x, y)
    assert (code[1:-1, 1:-1] > 0).mean() > 0.95


def test_aprons_equal_the_neighbouring_tiles_edges(region):
    """A tile's one-pixel apron holds the next tile's edge pixel: across tile edges inside a block, across BLOCK edges
    (the x=31 / x=32 boundary at zoom 9 = column 255 / 256 at zoom 12) and at the coarser zooms."""
    out = region["out"]
    found = B.scan_tiles(out)
    pairs = 0
    for z, tiles in found.items():
        for (x, y) in tiles:
            if (x + 1, y) in tiles:
                a, _ = _tile(out, z, x, y)
                b, _ = _tile(out, z, x + 1, y)
                assert np.array_equal(a[1:-1, -1], b[1:-1, 1]), (z, x, y)
                assert np.array_equal(a[1:-1, -2], b[1:-1, 0]), (z, x, y)
                pairs += 1
            if (x, y + 1) in tiles:
                a, _ = _tile(out, z, x, y)
                b, _ = _tile(out, z, x, y + 1)
                assert np.array_equal(a[-1, 1:-1], b[1, 1:-1]), (z, x, y)
                assert np.array_equal(a[-2, 1:-1], b[0, 1:-1]), (z, x, y)
                pairs += 1
    assert pairs > 40
    assert any(x == 255 for (x, _) in found[12]) and any(x == 256 for (x, _) in found[12])


def test_coarser_zoom_is_the_halving_of_the_finer(region):
    out = region["out"]
    found = B.scan_tiles(out)
    for (x, y) in sorted(found[11])[:6]:
        big = np.full((512, 512), np.nan, np.float32)
        bs = np.zeros((512, 512), np.uint8)
        for dy in (0, 1):
            for dx in (0, 1):
                t = _tile(out, 12, 2 * x + dx, 2 * y + dy)
                if t is not None:
                    big[dy * 256:(dy + 1) * 256, dx * 256:(dx + 1) * 256] = T.DECODE[t[0][1:-1, 1:-1]]
                    bs[dy * 256:(dy + 1) * 256, dx * 256:(dx + 1) * 256] = t[1][1:-1, 1:-1]
        d, s = T.pool2(big, bs)
        code, sf = _tile(out, 11, x, y)
        expect = T.encode_depth(d)
        # pass 2 halves the float field before the 1-cm code: at most one code step apart
        assert np.max(np.abs(code[1:-1, 1:-1].astype(int) - expect.astype(int))) <= 1
        assert np.array_equal(code[1:-1, 1:-1] > 0, expect > 0)


def test_index_presence_and_idx_files(region):
    out, idx = region["out"], region["index"]
    found = B.scan_tiles(out)
    lv = idx["levels"]["9"]
    m = T.unpack_bits(__import__("base64").b64decode(lv["bits"]), (lv["h"], lv["w"]))
    for (x, y) in found[9]:
        assert m[y - lv["y0"], x - lv["x0"]]
    assert m.sum() == len(found[9])
    assert idx["idx"] == {"dir": "idx", "zooms": [11, 12]}
    for name in idx["t10"]:
        cx, cy = (int(v) for v in name.split("_"))
        data = open(os.path.join(out, "idx", name + ".bin"), "rb").read()
        assert len(data) == 1 + 2                        # zoom 11: 4 bits -> 1 byte; zoom 12: 16 bits -> 2 bytes
        b11 = T.unpack_bits(data[:1], (2, 2))
        b12 = T.unpack_bits(data[1:], (4, 4))
        for (x, y) in found.get(11, ()):
            if (x >> 1, y >> 1) == (cx, cy):
                assert b11[y - 2 * cy, x - 2 * cx]
        assert b11.sum() == sum(1 for (x, y) in found.get(11, ()) if (x >> 1, y >> 1) == (cx, cy))
        assert b12.sum() == sum(1 for (x, y) in found.get(12, ()) if (x >> 2, y >> 2) == (cx, cy))


def test_check_finds_a_changed_or_extra_tile(region, tmp_path):
    import shutil
    work = str(tmp_path / "copy")
    shutil.copytree(region["out"], work)
    assert B.check(work, log_fn=lambda s: None) == 0
    z, (x, y) = 12, sorted(B.scan_tiles(work)[12])[0]
    code, sf = T.unpack_tile(open(B.tile_path(work, z, x, y), "rb").read())
    code[5, 5] = 65500                                  # an unused code
    open(B.tile_path(work, z, x, y), "wb").write(T.pack_tile(code, sf))
    msgs = []
    assert B.check(work, log_fn=msgs.append) >= 2         # the unused code and the digest
    assert any("unused codes" in m for m in msgs) and any("digest" in m for m in msgs)
    shutil.rmtree(work)
    shutil.copytree(region["out"], work)
    os.makedirs(os.path.join(work, "t", "13", "1"))
    open(os.path.join(work, "t", "13", "1", "1.png"), "wb").write(T.pack_tile(np.zeros((258, 258), np.uint16),
                                                                            np.zeros((258, 258), np.uint8)))
    msgs = []
    assert B.check(work, log_fn=msgs.append) > 0
    assert any("outside" in m for m in msgs) and any("no water inside" in m for m in msgs)


# ---------------------------------------------------------------- upload (a fake store)

class FakeStore:
    def __init__(self):
        self.objects, self.order = {}, []

    def put(self, key, body, content_type, cache_control):
        self.objects[key] = (bytes(body), content_type, cache_control)
        self.order.append(key)

    def get_json(self, key):
        o = self.objects.get(key)
        return None if o is None else json.loads(o[0].decode("utf-8"))


def test_upload_writes_files_then_licence_then_index_then_regions(region):
    st = FakeStore()
    assert B.upload(region["out"], store=st, workers=4, log_fn=lambda s: None)
    pre = "static/bathy/synth/v3/"
    keys = st.order
    assert keys[-1] == B.REGIONS_KEY
    assert keys[-2] == pre + "index.json" and keys[-3] == pre + "LICENSE.txt"
    files = keys[:-3]
    assert len(files) == region["index"]["count"]
    assert all(k.startswith(pre + "t/") or k.startswith(pre + "idx/") for k in files)
    assert st.objects[pre + "index.json"][2] == B.IMMUTABLE
    assert st.objects[B.REGIONS_KEY][2] == B.POINTER
    png = [k for k in files if k.endswith(".png")][0]
    assert st.objects[png][1] == "image/png"
    reg = st.get_json(B.REGIONS_KEY)["regions"]["synth"]
    assert reg["v"] == 3 and reg["zmax"] == 12 and reg["digest"] == region["index"]["digest"][:16]
    # the same build again: nothing but regions.json is written
    st.order.clear()
    assert B.upload(region["out"], store=st, workers=4, log_fn=lambda s: None)
    assert st.order == [B.REGIONS_KEY]


def test_upload_refuses_a_different_build_under_the_same_version(region):
    st = FakeStore()
    pre = "static/bathy/synth/v3/"
    st.put(pre + "index.json", json.dumps({"digest": "0" * 64}).encode(), "application/json", B.IMMUTABLE)
    st.order.clear()
    msgs = []
    assert B.upload(region["out"], store=st, log_fn=msgs.append) is False
    assert st.order == [] and any("refusing" in m for m in msgs)
    assert B.upload(region["out"], store=st, replace=True, log_fn=msgs.append)
    assert st.get_json(pre + "index.json")["digest"] == region["index"]["digest"]


def test_upload_keeps_the_other_regions_in_regions_json(region):
    st = FakeStore()
    st.put(B.REGIONS_KEY, json.dumps({"format": "bathy-regions-v1", "regions": {"gebco": {"v": 1}}}).encode(),
           "application/json", B.POINTER)
    assert B.upload(region["out"], store=st, log_fn=lambda s: None)
    regs = st.get_json(B.REGIONS_KEY)["regions"]
    assert regs["gebco"] == {"v": 1} and regs["synth"]["v"] == 3


# ---------------------------------------------------------------- GEBCO: zoom 7 from the grid, the world wrap

def _write_gebco(path, west, east, south, north, res=1 / 64):
    w = int(round((east - west) / res))
    h = int(round((north - south) / res))
    lons = west + (np.arange(w) + 0.5) * res
    lats = north - (np.arange(h) + 0.5) * res
    e = (-2000.0 - 10.0 * lats[:, None] + 0.0 * lons[None, :]).astype(np.float32)
    e[: h // 8, : w // 8] = 50.0                          # a land patch
    with rasterio.open(path, "w", driver="GTiff", width=w, height=h, count=1, dtype="float32", crs="EPSG:4326",
                       transform=from_origin(west, north, res, res)) as d:
        d.write(e, 1)


def test_gebco_builds_zoom_7_and_wraps_at_the_date_line(tmp_path, monkeypatch):
    gdir = tmp_path / "gebco"
    gdir.mkdir()
    _write_gebco(str(gdir / "east.tif"), 174.375, 180.0, -5.625, 5.625)
    _write_gebco(str(gdir / "west.tif"), -180.0, -174.375, -5.625, 5.625)
    monkeypatch.setitem(S.REGIONS, "gebco", dict(S.REGIONS["gebco"], zmin=5))
    out_root = str(tmp_path / "out")
    jobs = []
    real = B.gebco_tile
    monkeypatch.setattr(B, "gebco_tile", lambda args: jobs.append(args) or real(args))
    idx = B.build("gebco", out_root, version=1, workers=0, gebco=str(gdir))
    # every zoom-7 tile is read once, from the file that holds its centre (a tile touching a file's edge from
    # outside is not asked of that file: its read would be empty, or partial, and could replace the right one)
    keys = [(x, y) for (_, _, x, y) in jobs]
    assert len(keys) == len(set(keys)) and {x for (x, _) in keys} == {0, 1, 126, 127}
    for (path, _, x, y) in jobs:
        c0 = T.xy_to_lonlat(*T.tile_bounds(7, x, y)[:2])
        c1 = T.xy_to_lonlat(*T.tile_bounds(7, x, y)[2:])
        assert ("east" in path) == ((c0[0] + c1[0]) / 2 > 0), (path, x, y)
    out = os.path.join(out_root, "gebco", "v1")
    assert B.check(out, log_fn=lambda s: None) == 0
    found = B.scan_tiles(out)
    assert set(found) == {5, 6, 7}
    xs = {x for (x, _) in found[7]}
    assert xs == {0, 1, 126, 127}
    # the wrap: tile x=0's left apron = tile x=127's last core column
    y = sorted(y for (x, y) in found[7] if x == 0)[1]
    a, sa = _tile(out, 7, 0, y)
    b, _ = _tile(out, 7, 127, y)
    assert np.array_equal(a[1:-1, 0], b[1:-1, -2])
    assert (a[1:-1, 0] > 0).any()
    # every GEBCO pixel carries the coarse flag and id 31
    wet = sa > 0
    assert np.all((sa[wet] & T.SRC_BITS) == 31) and np.all(sa[wet] & T.FLAG_COARSE)
    # values: depth = 2000 + 10 * lat (within the average of the grid cells under a pixel)
    i, j = 128, 128
    lon, lat = _pixel_lonlat(7, 0, y, i, j)
    assert float(T.DECODE[a[j + 1, i + 1]]) == pytest.approx(2000.0 + 10.0 * lat, abs=0.2)
    assert idx["zmax"] == 7 and idx["idx"] is None and idx["t10"] == []
    # every tile's apron = its neighbours' edges, right and down, at zooms 5-7, across the date line too
    pairs = 0
    for z, tiles in found.items():
        n = 2 ** z
        for (x, y) in tiles:
            a, _ = _tile(out, z, x, y)
            r = ((x + 1) % n, y)
            if r in tiles:
                b, _ = _tile(out, z, *r)
                assert np.array_equal(a[1:-1, -1], b[1:-1, 1]) and np.array_equal(a[1:-1, -2], b[1:-1, 0]), (z, x, y)
                pairs += 1
            if (x, y + 1) in tiles:
                b, _ = _tile(out, z, x, y + 1)
                assert np.array_equal(a[-1, 1:-1], b[1, 1:-1]) and np.array_equal(a[-2, 1:-1], b[0, 1:-1]), (z, x, y)
                pairs += 1
    assert pairs >= 12
