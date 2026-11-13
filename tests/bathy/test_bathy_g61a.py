"""The bathymetry builder after its G61a review (plan section 93b; docs/reviews/bathy-G61a-adversarial.md): ponds behind
one- and two-pixel walls stay dry (connectivity at full resolution), fill values never read as depths, GEBCO's land and
lakes below sea level stay dry (the GSHHG mask), the GEBCO set must hold its eight quadrants, the sources merge best first,
the check refuses what the site could not draw, and the upload never points regions.json backwards by accident.
Needs scipy and rasterio (the bathy-build workflow installs them); skipped elsewhere."""
import json
import os
import shutil
import sys
import zipfile
import zlib

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

QUIET = dict(log_fn=lambda s: None)


def _source(path, west, north, res_x, res_y, e, nodata=-9999.0):
    h, w = e.shape
    kw = dict(driver="GTiff", width=w, height=h, count=1, dtype="float32", crs="EPSG:4326",
              transform=from_origin(west, north, res_x, res_y), tiled=True, blockxsize=256, blockysize=256,
              compress="deflate")
    if nodata is not None:
        kw["nodata"] = nodata
    with rasterio.open(path, "w", **kw) as d:
        d.write(e.astype(np.float32), 1)


def _depth_at(out, z, lon, lat):
    """The stored depth (m) at a point, None when no tile holds it, 0.0 when the tile holds no water there."""
    x, y = T.tile_of(lon, lat, z)
    p = B.tile_path(out, z, x, y)
    if not os.path.exists(p):
        return None
    code, _ = T.unpack_tile(open(p, "rb").read())
    x0, y0, x1, y1 = T.tile_bounds(z, x, y)
    mx, my = T.lonlat_to_xy(lon, lat)
    i = int((mx - x0) / (x1 - x0) * 256)
    j = int((y1 - my) / (y1 - y0) * 256)
    c = int(code[j + 1, i + 1])
    return float(T.DECODE[c]) if c else 0.0


# ---------------------------------------------------------------- thin walls (G61a P2-1) and fill values

Z = 12
DLON = 360.0 / (2 ** Z * 256)              # one zoom-12 pixel in longitude
K0 = int((-157.9 + 180) / DLON)
WALL_WEST = -180 + K0 * DLON               # a zoom-12 pixel edge
NCOL = 600
SUB = 4
WALL_SOUTH, WALL_NORTH = 21.40, 21.50
RES_LAT = 0.00008
PONDS = {                                  # name: (first column, end column, lat0, lat1)
    "A inland, no wall": (300, 330, 21.43, 21.44),
    "B behind a 1-px wall (column 200)": (201, 240, 21.45, 21.46),
    "C behind a 1-px wall (column 399)": (360, 399, 21.465, 21.475),
    "D behind a 2-px wall (columns 200-201)": (202, 240, 21.415, 21.425),
    "E behind a 2-px wall (columns 201-202)": (203, 240, 21.476, 21.486),
}
FILL = (50, 60, 21.405, 21.412)            # a patch of -3.4e38 (a fill value the file does not declare) in the open sea


@pytest.fixture(scope="module")
def walls(tmp_path_factory, request):
    """A land bar (zoom-12 columns 200-399) between two deep seas, with ponds below sea level inside it."""
    root = tmp_path_factory.mktemp("walls")
    w = NCOL * SUB
    h = int(round((WALL_NORTH - WALL_SOUTH) / RES_LAT))
    col = np.arange(w) // SUB
    lats = WALL_NORTH - (np.arange(h) + 0.5) * RES_LAT
    e = np.full((h, w), -30.0, np.float32)
    e[:, (col >= 200) & (col < 400)] = 5.0
    for c0, c1, la0, la1 in PONDS.values():
        e[np.ix_((lats > la0) & (lats < la1), (col >= c0) & (col < c1))] = -3.0
    rows_e = (lats > PONDS["E behind a 2-px wall (columns 201-202)"][2]) & \
             (lats < PONDS["E behind a 2-px wall (columns 201-202)"][3])
    e[np.ix_(rows_e, col == 200)] = -30.0           # a notch of sea up to the wall
    e[np.ix_((lats > FILL[2]) & (lats < FILL[3]), (col >= FILL[0]) & (col < FILL[1]))] = -3.4e38
    src = str(root / "walls.tif")
    _source(src, WALL_WEST, WALL_NORTH, DLON / SUB, RES_LAT, e)
    mp = pytest.MonkeyPatch()
    mp.setitem(S.SOURCES, "walls", dict(S.CUDEM, id=10, zmax=Z, year="2099", path="walls.tif", datum="test",
                                        name="walls", credit="test", licence="test"))
    mp.setitem(S.REGIONS, "walls", dict(box=(WALL_WEST, WALL_SOUTH, WALL_WEST + NCOL * DLON, WALL_NORTH),
                                        sources=["walls"], mhhw=0.4, zmin=10, prio=1))
    request.addfinalizer(mp.undo)
    B.build("walls", str(root / "out"), workers=0, local={"walls": src})
    return os.path.join(str(root / "out"), "walls", "v1")


def _lon(column):
    return WALL_WEST + (column + 0.5) * DLON


@pytest.mark.parametrize("name", sorted(PONDS))
def test_a_pond_behind_a_thin_wall_stays_dry(walls, name):
    c0, c1, la0, la1 = PONDS[name]
    for column in (c0, (c0 + c1) // 2, c1 - 1):
        d = _depth_at(walls, Z, _lon(column), (la0 + la1) / 2)
        assert not d, (name, column, d)


def test_the_seas_beside_the_walls_are_water(walls):
    assert _depth_at(walls, Z, _lon(100), 21.455) == pytest.approx(30.0, abs=0.02)
    assert _depth_at(walls, Z, _lon(500), 21.455) == pytest.approx(30.0, abs=0.02)
    # the notch reaching the wall of pond E is sea (joined at full resolution, one pixel wide)
    la0, la1 = PONDS["E behind a 2-px wall (columns 201-202)"][2:]
    assert _depth_at(walls, Z, _lon(200), (la0 + la1) / 2) == pytest.approx(30.0, abs=0.02)
    # the land bar itself holds nothing
    assert not _depth_at(walls, Z, _lon(250), 21.44)


def test_a_fill_value_never_reads_as_a_depth(walls):
    """A source pixel of -3.4e38 that the file does not declare as nodata is dropped (bathy_sources.SANE_M), never stored
    as an 11-km depth, and the open sea around it stays water."""
    mid = ((FILL[0] + FILL[1]) // 2, (FILL[2] + FILL[3]) / 2)
    assert not _depth_at(walls, Z, _lon(mid[0]), mid[1])
    assert _depth_at(walls, Z, _lon(FILL[0] - 5), mid[1]) == pytest.approx(30.0, abs=0.02)
    assert B.check(walls, **QUIET) == 0
    for z, tiles in B.scan_tiles(walls).items():
        for (x, y) in tiles:
            code, _ = T.unpack_tile(open(B.tile_path(walls, z, x, y), "rb").read())
            assert float(np.nanmax(T.DECODE[code])) < 31.0, (z, x, y)


# ---------------------------------------------------------------- sources: best first, a nodata value required

def _two_sources(tmp_path, monkeypatch, first_id, second_id):
    """Two overlapping sources: 'near' (2 m everywhere it has data) over the western two thirds, 'far' (7 m) over the
    eastern two thirds. Both listed, 'far' first."""
    west, east, south, north = -157.92, -157.86, 21.40, 21.46
    res = 0.0002
    w, h = int(round((east - west) / res)), int(round((north - south) / res))
    a = np.full((h, w), -2.0, np.float32)
    a[:, 2 * w // 3:] = -9999.0
    b = np.full((h, w), -7.0, np.float32)
    b[:, : w // 3] = -9999.0
    _source(str(tmp_path / "near.tif"), west, north, res, res, a)
    _source(str(tmp_path / "far.tif"), west, north, res, res, b)
    for key, sid in (("near", first_id), ("far", second_id)):
        monkeypatch.setitem(S.SOURCES, key, dict(S.CUDEM, id=sid, zmax=12, year="2099", path=key, datum="t", name=key,
                                                 credit="t", licence="t"))
    monkeypatch.setitem(S.REGIONS, "two", dict(box=(west, south, east, north), sources=["far", "near"], mhhw=0.4,
                                              zmin=10, prio=1))
    return B.region_config("two", local={"near": str(tmp_path / "near.tif"), "far": str(tmp_path / "far.tif")})


def test_the_lowest_source_id_wins_where_sources_overlap(tmp_path, monkeypatch):
    reg = _two_sources(tmp_path, monkeypatch, 5, 20)
    assert [s["id"] for s in reg["srcs"]] == [5, 20]                   # listed far-first, merged near-first
    key = T.tile_of(-157.89, 21.43, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src = B.read_field(reg, key)
    x0, y0, x1, y1 = T.tile_bounds(reg["zmax"] - B.BLOCK_LEVELS, *key, apron=B.MARGIN, px=2048)
    size = elev.shape[0]

    def at(lon, lat):
        mx, my = T.lonlat_to_xy(lon, lat)
        i = int((mx - x0) / (x1 - x0) * size)
        j = int((y1 - my) / (y1 - y0) * size)
        return float(elev[j, i]), int(src[j, i])

    assert at(-157.89, 21.43) == (-2.0, 5)                           # both have data: the lower id
    assert at(-157.915, 21.43) == (-2.0, 5)                          # only near
    assert at(-157.865, 21.43) == (-7.0, 20)                         # only far
    reg = _two_sources(tmp_path, monkeypatch, 20, 5)                  # swap the ids: far wins the overlap now
    assert [s["key"] for s in reg["srcs"]] == ["far", "near"]
    elev, src = B.read_field(reg, key)
    assert at(-157.89, 21.43) == (-7.0, 5)


def test_a_source_without_a_nodata_value_is_refused(tmp_path, monkeypatch):
    e = np.full((100, 100), -3.0, np.float32)
    _source(str(tmp_path / "bare.tif"), -157.9, 21.45, 0.0002, 0.0002, e, nodata=None)
    monkeypatch.setitem(S.SOURCES, "bare", dict(S.CUDEM, id=9, zmax=12, year="x", path="bare", datum="x", name="x",
                                                credit="x", licence="x"))
    monkeypatch.setitem(S.REGIONS, "bare", dict(box=(-157.9, 21.43, -157.88, 21.45), sources=["bare"], mhhw=0.4,
                                               zmin=10, prio=1))
    reg = B.region_config("bare", local={"bare": str(tmp_path / "bare.tif")})
    with pytest.raises(ValueError, match="nodata"):
        B.read_field(reg, T.tile_of(-157.89, 21.44, 9))
    monkeypatch.setitem(S.SOURCES, "bare", dict(S.SOURCES["bare"], nodata=-9999.0))    # named in bathy_sources: fine
    reg = B.region_config("bare", local={"bare": str(tmp_path / "bare.tif")})
    elev, _ = B.read_field(reg, T.tile_of(-157.89, 21.44, 9))
    assert np.nanmax(elev) == pytest.approx(-3.0)


# ---------------------------------------------------------------- GEBCO: the land mask and the quadrants

def _coast_file(path, squares):
    """A coast-v1 file (tools/coast's format) of land squares (lon0, lat0, lon1, lat1), counter-clockwise."""
    q = C.Q
    pieces = []
    for lon0, lat0, lon1, lat1 in squares:
        ix = np.array([lon0, lon1, lon1, lon0], np.float64) * q
        iy = np.array([lat0, lat0, lat1, lat1], np.float64) * q
        pieces.append([(np.round(ix).astype(np.int64), np.round(iy).astype(np.int64))])
    with open(path, "wb") as f:
        f.write(C.encode_file(pieces, 30, q))
    return path


def test_land_mask_marks_the_land_pixels_of_a_tile(tmp_path):
    land = _coast_file(str(tmp_path / "land.bin"), [(176.0, 0.0, 177.0, 1.0)])
    B.land_pieces.cache_clear()
    x, y = T.tile_of(176.5, 0.5, 7)
    m = B.land_mask(land, 7, x, y)
    assert m.shape == (256, 256) and m.dtype == bool
    x0, y0, x1, y1 = T.tile_bounds(7, x, y)

    def pix(lon, lat):
        mx, my = T.lonlat_to_xy(lon, lat)
        return int((y1 - my) / (y1 - y0) * 256), int((mx - x0) / (x1 - x0) * 256)

    assert m[pix(176.5, 0.5)] and m[pix(176.05, 0.95)]
    assert not m[pix(175.8, 0.5)] and not m[pix(176.5, 1.3)] and not m[pix(177.1, 0.5)]
    # the square covers 1 x 1 degree of a 2.8125-degree tile: about an eighth of its pixels
    assert 0.10 < m.mean() < 0.16
    # a tile the land does not touch: nothing
    assert not B.land_mask(land, 7, *T.tile_of(-170.0, 40.0, 7)).any()


def _write_gebco(path, west, east, south, north, res=1 / 64):
    w = int(round((east - west) / res))
    h = int(round((north - south) / res))
    lats = north - (np.arange(h) + 0.5) * res
    e = np.repeat((-2000.0 - 10.0 * lats)[:, None], w, axis=1).astype(np.float32)
    with rasterio.open(path, "w", driver="GTiff", width=w, height=h, count=1, dtype="float32", crs="EPSG:4326",
                       transform=from_origin(west, north, res, res)) as d:
        d.write(e, 1)


@pytest.fixture
def gebco_pair(tmp_path, monkeypatch):
    gdir = tmp_path / "gebco"
    gdir.mkdir()
    _write_gebco(str(gdir / "east.tif"), 174.375, 180.0, -5.625, 5.625)
    _write_gebco(str(gdir / "west.tif"), -180.0, -174.375, -5.625, 5.625)
    monkeypatch.setattr(S, "GEBCO_QUADRANTS", {(174, -6), (-180, -6)})
    monkeypatch.setitem(S.REGIONS, "gebco", dict(S.REGIONS["gebco"], zmin=6))
    return gdir


def test_gebco_corners_from_a_directory_and_from_the_zip(gebco_pair, tmp_path):
    assert sorted(B.gebco_corners(str(gebco_pair))) == [(-180, -6), (174, -6)]
    zp = str(tmp_path / "gebco.zip")
    with zipfile.ZipFile(zp, "w") as z:
        for n in ("east.tif", "west.tif"):
            z.write(str(gebco_pair / n), "gebco_2026_geotiff/" + n)
        z.writestr("gebco_2026_geotiff/README.txt", "not a grid")
    assert sorted(B.gebco_corners(zp)) == [(-180, -6), (174, -6)]
    assert not [n for n in os.listdir(str(tmp_path)) if n.endswith(".tif")]       # read through /vsizip/, nothing out


def test_gebco_below_sea_level_inside_the_land_stays_dry(gebco_pair, tmp_path):
    """The GEBCO grid's elevation is below 0 everywhere here; the GSHHG land square (a basin like the Dead Sea, or a lake
    inside the land) must stay dry, the sea around it wet."""
    land = _coast_file(str(tmp_path / "land.bin"), [(176.0, 0.0, 177.0, 1.0)])
    B.land_pieces.cache_clear()
    out_root = str(tmp_path / "out")
    B.build("gebco", out_root, version=1, workers=0, gebco=str(gebco_pair), land=land)
    out = os.path.join(out_root, "gebco", "v1")
    assert B.check(out, **QUIET) == 0
    assert not _depth_at(out, 7, 176.5, 0.5)
    assert _depth_at(out, 7, 176.5, 2.0) == pytest.approx(2000.0 + 10.0 * 2.0, abs=0.2)
    assert _depth_at(out, 7, 175.5, 0.5) == pytest.approx(2005.0, abs=0.2)
    lic = open(os.path.join(out, "LICENSE.txt"), encoding="utf-8").read()
    assert S.LAND["name"] in lic and S.LAND["licence"] in lic


def test_gebco_refuses_a_set_without_its_quadrants(gebco_pair, tmp_path):
    land = _coast_file(str(tmp_path / "land.bin"), [(176.0, 0.0, 177.0, 1.0)])
    os.remove(str(gebco_pair / "west.tif"))
    with pytest.raises(ValueError, match="quadrants"):
        B.build("gebco", str(tmp_path / "out"), version=1, workers=0, gebco=str(gebco_pair), land=land)
    _write_gebco(str(gebco_pair / "west.tif"), -180.0, -174.375, -5.625, 5.625)
    _write_gebco(str(gebco_pair / "west2.tif"), -180.0, -174.375, -5.625, 5.625)      # a quadrant twice
    with pytest.raises(ValueError, match="quadrants"):
        B.build("gebco", str(tmp_path / "out2"), version=1, workers=0, gebco=str(gebco_pair), land=land)


def test_the_land_file_is_checked_by_its_hash(tmp_path, monkeypatch):
    work = tmp_path / "w"
    work.mkdir()
    (work / "land-world-i.bin").write_bytes(b"not the pinned file")
    with pytest.raises(ValueError, match="hash"):
        B.land_file(None, str(work))
    given = str(tmp_path / "given.bin")
    open(given, "wb").write(b"x")
    assert B.land_file(given, str(work)) == given                   # a given local copy is the caller's choice


# ---------------------------------------------------------------- the check: what the site could not draw

@pytest.fixture(scope="module")
def built(walls, tmp_path_factory):
    return walls


def _copy(src, tmp_path):
    dst = str(tmp_path / "copy")
    shutil.copytree(src, dst)
    return dst


def _first_tile(out, z=None):
    found = B.scan_tiles(out)
    z = max(found) if z is None else z
    return z, sorted(found[z])[0]


def test_check_refuses_a_disagreeing_source_plane(built, tmp_path):
    out = _copy(built, tmp_path)
    z, (x, y) = _first_tile(out)
    code, sf = T.unpack_tile(open(B.tile_path(out, z, x, y), "rb").read())
    wet = np.argwhere(code > 0)[0]
    sf[tuple(wet)] = 0
    open(B.tile_path(out, z, x, y), "wb").write(T.pack_tile(code, sf))
    msgs = []
    assert B.check(out, log_fn=msgs.append) > 0
    assert any("source plane disagrees" in m for m in msgs)


def test_check_refuses_an_unknown_source_id(built, tmp_path):
    out = _copy(built, tmp_path)
    z, (x, y) = _first_tile(out)
    code, sf = T.unpack_tile(open(B.tile_path(out, z, x, y), "rb").read())
    sf[code > 0] = 7
    open(B.tile_path(out, z, x, y), "wb").write(T.pack_tile(code, sf))
    msgs = []
    assert B.check(out, log_fn=msgs.append) > 0
    assert any("unknown source ids [7]" in m for m in msgs)


def test_check_refuses_a_changed_idx_file(built, tmp_path):
    out = _copy(built, tmp_path)
    names = sorted(os.listdir(os.path.join(out, "idx")))
    assert names
    p = os.path.join(out, "idx", names[0])
    data = bytearray(open(p, "rb").read())
    data[0] ^= 0x01
    open(p, "wb").write(bytes(data))
    msgs = []
    assert B.check(out, log_fn=msgs.append) > 0
    assert any("idx %s missing or different" % names[0] in m for m in msgs)


def _png_with_ihdr(png, bit_depth=None, colour=None, interlace=None):
    b = bytearray(png)
    if bit_depth is not None:
        b[24] = bit_depth
    if colour is not None:
        b[25] = colour
    if interlace is not None:
        b[28] = interlace
    crc = zlib.crc32(bytes(b[12:29])) & 0xFFFFFFFF
    b[29:33] = crc.to_bytes(4, "big")
    return bytes(b)


def test_the_tile_reader_refuses_pngs_the_site_cannot_decode(built, tmp_path):
    good = T.pack_tile(np.zeros((258, 258), np.uint16), np.zeros((258, 258), np.uint8))
    assert good[24] == 8 and good[25] == 0 and good[28] == 0          # the writer: 8-bit grey, not interlaced
    for bad in (_png_with_ihdr(good, interlace=1), _png_with_ihdr(good, bit_depth=16), _png_with_ihdr(good, colour=2),
                b"GIF89a" + good[6:], good[:20]):
        with pytest.raises(ValueError):
            T.unpack_tile(bad)
    out = _copy(built, tmp_path)
    z, (x, y) = _first_tile(out)
    p = B.tile_path(out, z, x, y)
    body = open(p, "rb").read()
    open(p, "wb").write(_png_with_ihdr(body, interlace=1))
    msgs = []
    assert B.check(out, log_fn=msgs.append) > 0
    assert any("non-interlaced" in m for m in msgs)


def test_check_refuses_an_empty_build(tmp_path):
    out = str(tmp_path / "empty")
    os.makedirs(out)
    reg = dict(name="empty", zmin=10, zmax=12, box=(0, 0, 1, 1), mhhw=0.4,
               srcs=[dict(id=10, zmax=12, name="x", year="x", res_m=3, datum="x", licence="x", credit="x")])
    index = B.write_index(reg, out, 1)
    assert index["count"] == 0
    msgs = []
    assert B.check(out, log_fn=msgs.append) > 0
    assert any("empty build" in m for m in msgs)


def test_the_digest_covers_the_licence_and_every_byte(built, tmp_path):
    out = _copy(built, tmp_path)
    d0, n0, s0 = B.file_digest(out)
    with open(os.path.join(out, "LICENSE.txt"), "a", encoding="utf-8") as f:
        f.write("changed\n")
    d1, n1, s1 = B.file_digest(out)
    assert d1 != d0 and (n1, s1) == (n0, s0)                           # the licence is in the digest, not the counts
    msgs = []
    assert B.check(out, log_fn=msgs.append) > 0 and any("digest" in m for m in msgs)
    # a same-size change of one data file changes the digest
    tmp = str(tmp_path / "flat")
    os.makedirs(os.path.join(tmp, "t", "1", "1"))
    open(os.path.join(tmp, "t", "1", "1", "1.png"), "wb").write(b"abc")
    a = B.file_digest(tmp)
    open(os.path.join(tmp, "t", "1", "1", "1.png"), "wb").write(b"abd")
    b = B.file_digest(tmp)
    assert a[0] != b[0] and a[1:] == b[1:]


# ---------------------------------------------------------------- upload: the rules (a fake store)

class Store:
    def __init__(self):
        self.objects, self.log = {}, []

    def put(self, key, body, content_type, cache_control):
        self.objects[key] = (bytes(body), content_type, cache_control)
        self.log.append(("put", key))

    def get_json(self, key):
        o = self.objects.get(key)
        return None if o is None else json.loads(o[0].decode("utf-8"))

    def list_keys(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix))

    def delete_keys(self, keys, what="keys"):
        for k in keys:
            self.objects.pop(k, None)
            self.log.append(("del", k))


def _pre(out):
    i = json.load(open(os.path.join(out, "index.json")))
    return "static/bathy/%s/v%d" % (i["region"], i["version"]), i


def test_upload_refuses_to_point_regions_json_back_to_an_older_version(built):
    st = Store()
    st.put(B.REGIONS_KEY, json.dumps({"format": "bathy-regions-v1", "regions": {"walls": {"v": 5}}}).encode(),
           "application/json", B.POINTER)
    st.log.clear()
    msgs = []
    assert B.upload(built, store=st, log_fn=msgs.append) is False
    assert st.log == [] and any("pointing it back" in m for m in msgs)
    assert B.upload(built, store=st, replace=True, **QUIET)
    assert st.get_json(B.REGIONS_KEY)["regions"]["walls"]["v"] == 1
    # the same or a newer version needs no replace
    st2 = Store()
    st2.put(B.REGIONS_KEY, json.dumps({"regions": {"walls": {"v": 1}}}).encode(), "application/json", B.POINTER)
    assert B.upload(built, store=st2, **QUIET)


def test_a_replace_hides_the_old_build_first_and_removes_its_extra_files(built):
    pre, index = _pre(built)
    st = Store()
    st.put(pre + "/index.json", json.dumps({"digest": "0" * 64}).encode(), "application/json", B.IMMUTABLE)
    st.put(pre + "/t/99/1/1.png", b"old", "image/png", B.IMMUTABLE)
    st.put(pre + "/idx/0_0.bin", b"old", "application/octet-stream", B.IMMUTABLE)
    st.put("static/bathy/other/v1/index.json", b"{}", "application/json", B.IMMUTABLE)
    st.log.clear()
    assert B.upload(built, store=st, replace=True, **QUIET)
    log = st.log
    assert log[0] == ("del", pre + "/index.json")                    # no client reads the half-written mix
    puts = [k for (op, k) in log if op == "put"]
    assert puts[-3:] == [pre + "/LICENSE.txt", pre + "/index.json", B.REGIONS_KEY]
    dels = [i for i, (op, k) in enumerate(log) if op == "del" and k != pre + "/index.json"]
    assert {log[i][1] for i in dels} == {pre + "/t/99/1/1.png", pre + "/idx/0_0.bin"}
    assert all(i < log.index(("put", pre + "/index.json")) for i in dels)
    assert "static/bathy/other/v1/index.json" in st.objects                 # another region's files untouched
    assert st.get_json(pre + "/index.json")["digest"] == index["digest"]
    assert set(st.list_keys(pre + "/")) == {pre + "/" + r for r in B.data_files(built)} | \
        {pre + "/LICENSE.txt", pre + "/index.json"}


def test_the_same_build_again_writes_only_regions_json_and_says_so(built):
    st = Store()
    assert B.upload(built, store=st, **QUIET)
    st.log.clear()
    msgs = []
    assert B.upload(built, store=st, log_fn=msgs.append)
    assert st.log == [("put", B.REGIONS_KEY)]
    assert any("only regions.json was written" in m for m in msgs)


class _LosesWrites(Store):
    def __init__(self, lose):
        super().__init__()
        self.lose = lose

    def put(self, key, body, content_type, cache_control):
        if not key.endswith(self.lose):
            super().put(key, body, content_type, cache_control)


def test_a_write_that_does_not_read_back_fails_loud(built):
    with pytest.raises(RuntimeError, match="index.json"):
        B.upload(built, store=_LosesWrites("/index.json"), **QUIET)
    with pytest.raises(RuntimeError, match="regions.json"):
        B.upload(built, store=_LosesWrites("regions.json"), **QUIET)


# ---------------------------------------------------------------- the command line

def test_a_box_starting_with_a_minus_sign_reaches_the_build(monkeypatch, tmp_path):
    assert B.join_box(["build", "--box", "-158.1,21.6,-157.6,21.7", "--region", "hawaii"]) == \
        ["build", "--box=-158.1,21.6,-157.6,21.7", "--region", "hawaii"]
    assert B.join_box(["build", "--box"]) == ["build", "--box"]
    assert B.join_box(["check", "d"]) == ["check", "d"]
    got = {}

    def fake_build(name, out, **kw):
        got.update(kw, name=name)
        return {"count": 1}
    monkeypatch.setattr(B, "build", fake_build)
    monkeypatch.setattr(B, "check", lambda d, log_fn=print: 0)
    assert B.main(["build", "--region", "hawaii", "--out", str(tmp_path), "--box", "-158.1,21.6,-157.6,21.7"]) == 0
    assert got["name"] == "hawaii" and got["box"] == (-158.1, 21.6, -157.6, 21.7)
    assert B.main(["build", "--region", "gebco", "--out", str(tmp_path), "--gebco", "g.zip", "--land", "l.bin"]) == 0
    assert got["gebco"] == "g.zip" and got["land"] == "l.bin"
    monkeypatch.setattr(B, "build", lambda name, out, **kw: {"count": 0})
    assert B.main(["build", "--region", "hawaii", "--out", str(tmp_path)]) == 1      # nothing built = a failure


def test_a_region_zoom_range_is_checked_and_no_tile_falls_below_it(walls, monkeypatch):
    """The walls region keeps zmin 10 above its block zoom (zmax 12 - 3 = 9): the build writes nothing at zoom 9."""
    assert min(B.scan_tiles(walls)) == 10
    monkeypatch.setitem(S.REGIONS, "walls", dict(S.REGIONS["walls"], zmin=13))
    with pytest.raises(ValueError, match="zmin"):
        B.region_config("walls", local={"walls": "x.tif"})


def test_a_gebco_fill_value_never_reads_as_a_depth(gebco_pair, tmp_path):
    """A patch of -3.4e38 in the GEBCO grid (no nodata declared): the averaged pixels there are dropped, never stored as
    the deepest code, and the sea around them keeps its depth."""
    path = str(gebco_pair / "east.tif")
    with rasterio.open(path, "r+") as d:
        e = d.read(1)
        r0 = int(round((5.625 - 3.0) * 64))
        c0 = int(round((176.5 - 174.375) * 64))
        e[r0 - 4:r0 + 4, c0 - 4:c0 + 4] = -3.4e38
        d.write(e, 1)
    land = _coast_file(str(tmp_path / "land.bin"), [(10.0, 40.0, 11.0, 41.0)])
    B.land_pieces.cache_clear()
    out_root = str(tmp_path / "out")
    B.build("gebco", out_root, version=1, workers=0, gebco=str(gebco_pair), land=land)
    out = os.path.join(out_root, "gebco", "v1")
    assert B.check(out, **QUIET) == 0
    for z, tiles in B.scan_tiles(out).items():
        for (x, y) in tiles:
            code, _ = T.unpack_tile(open(B.tile_path(out, z, x, y), "rb").read())
            assert float(np.nanmax(T.DECODE[code])) < 2100.0, (z, x, y)
    assert not _depth_at(out, 7, 176.5, 3.0)
    assert _depth_at(out, 7, 176.5, 1.5) == pytest.approx(2015.0, abs=0.2)


def test_check_refuses_a_tile_outside_its_zooms_range(built, tmp_path):
    out = _copy(built, tmp_path)
    z, (x, y) = _first_tile(out)
    body = open(B.tile_path(out, z, x, y), "rb").read()
    bad = B.tile_path(out, z, 2 ** z + 3, y)
    os.makedirs(os.path.dirname(bad), exist_ok=True)
    open(bad, "wb").write(body)
    msgs = []
    assert B.check(out, log_fn=msgs.append) > 0
    assert any("outside the zoom's tile range" in m for m in msgs)


def test_downloads_send_a_named_user_agent(monkeypatch):
    """The frame host's edge answers 403 to Python's default user agent: bathy-build #10 (GEBCO, 2026-11-11) failed
    fetching the land mask (static/coast/v1/world-i.bin) that way. Every builder download names its client."""
    import urllib.request
    import build_bathy as B
    seen = []

    class Answer:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"ok"

    def fake(req, context=None, timeout=None):
        seen.append(req)
        return Answer()

    monkeypatch.setattr(urllib.request, "urlopen", fake)
    assert B._https_bytes(S.LAND["url"]) == b"ok"
    assert B._https_text("https://example.invalid/x.vrt") == "ok"
    for req in seen:
        assert isinstance(req, urllib.request.Request)
        ua = req.get_header("User-agent")
        assert ua == B.USER_AGENT and "urllib" not in ua.lower()
