"""The European source preparation (plan section 111b, tools/bathy/bathy_prep.py): the prep list checks, the parts, the
three kinds on small made-up archives (Litto3D 1-km ASCII tiles in a 7z, an INFOMAR survey GeoTIFF in a zip, an EMODnet
ASCII tile in a zip), the upload that never writes a key twice, and the merge into the builder's manifest."""
import hashlib
import io
import json
import os
import shutil
import sys
import zipfile

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))
import bathy_mirror as M  # noqa: E402
import bathy_prep as P    # noqa: E402

rasterio = pytest.importorskip("rasterio")
SHOM = "https://services.data.shom.fr/INSPIRE/telechargement/prepackageGroup/G/prepackage/%s/file/%s.7z"
GSI = "https://gsi.geodata.gov.ie/downloads/Marine/Data/Downloads/2023/X/%s.zip"
EMOD = "https://downloads.emodnet-bathymetry.eu/v12/%s_2024.msl.zip"


def _list(tmp_path, kind="litto3d", files=None, **over):
    base = dict(set="litto3d-test", kind=kind, mirror="mirror_test.json", crs="EPSG:2154", credit="Shom - IGN",
                licence="Licence Ouverte 2.0", derived="the 1-m DEM", low_m=6.0, deep_m=45.0, nodata=9999.0,
                boxes=[[-12.0, 43.0, 3.0, 61.0]],
                files=files or [dict(url=SHOM % ("0150_6770", "0150_6770"), bytes=10, md5="0" * 32, name="0150_6770")])
    base.update(over)
    p = tmp_path / "prep.json"
    p.write_text(json.dumps(base))
    return str(p)


# ---------------------------------------------------------------------------------------------------- the list

def test_the_committed_prep_lists_load_and_name_only_allowed_hosts():
    seen = {}
    for name in ("prep_litto3d_naq.json", "prep_litto3d_fin.json", "prep_infomar.json", "prep_emodnet.json"):
        p = P.load_list(os.path.join(ROOT, "tools", "bathy", name))
        seen[p["kind"]] = seen.get(p["kind"], 0) + len(p["files"])
    assert seen["litto3d"] == 324 + 292 and seen["emodnet"] == 6 and seen["infomar"] >= 250
    ie = P.load_list(os.path.join(ROOT, "tools", "bathy", "prep_infomar.json"))
    years = [f["year"] for f in ie["files"]]
    assert years == sorted(years, reverse=True)          # newest first: the builder takes the first value at a pixel


@pytest.mark.parametrize("bad, msg", [
    (dict(kind="zip"), "kind"),
    (dict(set="Bad Set"), "set"),
    (dict(credit=""), "credit"),
    (dict(files=[dict(url="https://example.com/a.7z")]), "only https links"),
    (dict(files=[dict(url="http://services.data.shom.fr/a.7z")]), "only https links"),
    (dict(files=[dict(url=SHOM % ("a", "a"), md5="xyz")]), "md5"),
    (dict(files=[dict(url=SHOM % ("a", "a"), bytes=7 * 1024 ** 3)]), "bytes"),
    (dict(files=[dict(url=SHOM % ("a", "a")), dict(url=SHOM % ("a", "a"))]), "twice"),
    (dict(low_m=None), "low_m"),
])
def test_a_bad_prep_list_is_refused(tmp_path, bad, msg):
    with pytest.raises(ValueError, match=msg):
        P.load_list(_list(tmp_path, **bad))


def test_parts_split_the_list_without_gaps_or_overlap():
    files = list(range(23))
    got = [P.part_of(files, "%d/5" % i) for i in range(1, 6)]
    assert sorted(x for g in got for x in g) == files
    with pytest.raises(ValueError):
        P.part_of(files, "6/5")


# ---------------------------------------------------------------------------------------------------- litto3d

def _asc(path, a, x0, y0, centre=True):
    nd = -99999.0
    b = np.where(np.isnan(a), nd, a)
    with open(path, "w") as f:
        f.write("ncols %d\nnrows %d\n" % (a.shape[1], a.shape[0]))
        f.write(("xllcenter %.3f\nyllcenter %.3f\n" if centre else "xllcorner %.3f\nyllcorner %.3f\n") % (x0, y0))
        f.write("cellsize 1.0000\nnodata_value -99999\n")
        for row in b:
            f.write(" ".join("%.2f" % v for v in row) + "\n")


def test_read_asc_places_a_centre_registered_grid_and_drops_nodata(tmp_path):
    a = np.array([[1.0, np.nan], [3.5, -2.25]], np.float32)
    _asc(tmp_path / "t.asc", a, 150000.0, 6766001.0)
    v, left, top, cs = P.read_asc(str(tmp_path / "t.asc"))
    assert (left, top, cs) == (149999.5, 6766002.5, 1.0)       # lower-left centre + half a cell + two rows
    assert np.isnan(v[0, 1]) and v[1, 1] == pytest.approx(-2.25)


def _pack(tmp_path, tiles):
    """A fake Litto3D pack: {name: (array, x0 lower-left centre, y0)} as MNT1m/*.asc members plus a point cloud."""
    py7zr = pytest.importorskip("py7zr")
    src = tmp_path / "src"
    for name, (a, x0, y0) in tiles.items():
        d = src / "0150_6770" / ("LITTO3D_FRA_%s_IGN69" % name) / "MNT1m"
        d.mkdir(parents=True)
        _asc(d / ("LITTO3D_FRA_%s_MNT_IGN69.asc" % name), a, x0, y0)
        s = src / "0150_6770" / ("LITTO3D_FRA_%s_IGN69" % name) / "Semis_points"
        s.mkdir(parents=True)
        (s / "points.xyz").write_text("not wanted\n")
    arc = tmp_path / "pack.7z"
    with py7zr.SevenZipFile(arc, "w") as z:
        z.writeall(src / "0150_6770", "0150_6770")
    return str(arc)


def test_a_litto3d_pack_keeps_only_its_low_tiles_in_one_grid(tmp_path):
    low = np.full((4, 4), 8.0, np.float32)
    low[2:, :] = -3.0                                     # seabed in the lower half
    high = np.full((4, 4), 20.0, np.float32)              # dry land only: left out
    arc = _pack(tmp_path, {"0150_6767": (low, 150000.0, 6766001.0), "0154_6767": (high, 150004.0, 6766001.0),
                           "0150_6763": (low, 150000.0, 6765997.0)})
    work = tmp_path / "w"
    work.mkdir()
    plist = P.load_list(_list(tmp_path))
    tif = P.prep_litto3d(arc, str(work), plist, "0150_6770", log=lambda *a: None)
    with rasterio.open(tif) as d:
        assert d.crs.to_string() == "EPSG:2154" and d.nodata == -99999.0
        assert (d.width, d.height) == (4, 8)              # the two low tiles, stacked; the dry tile left out
        assert (d.bounds.left, d.bounds.top) == (149999.5, 6766004.5)
        a = d.read(1)
        assert a[0, 0] == 8.0 and a[3, 0] == -3.0 and a[7, 3] == -3.0
    assert not [p for p in os.listdir(work) if p.startswith("x_")]       # the unpacked tiles are gone
    (tmp_path / "d").mkdir()
    dry = _pack(tmp_path / "d", {"0154_6767": (high, 150004.0, 6766001.0)})
    assert P.prep_litto3d(dry, str(work), plist, "dry", log=lambda *a: None) is None


def test_a_7z_member_with_a_parent_path_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(P, "list_7z", lambda a: ["0150_6770/../../evil/MNT1m/x.asc"])
    with pytest.raises(ValueError, match="unsafe"):
        P.unpack_7z("x.7z", str(tmp_path))


# ---------------------------------------------------------------------------------------------------- infomar

def _survey_zip(tmp_path, crs="EPSG:32629", nd=-3.4028235e38):
    from rasterio.transform import from_origin
    a = np.full((60, 50), nd, np.float32)
    a[10:20, 5:15] = -12.0                                # shallow: kept
    a[30:40, 20:30] = -80.0                               # deeper than 45 m below LAT: dropped
    a[45:50, 40:45] = -44.0                               # kept: it sets the window's far corner
    tif = tmp_path / "s.tif"
    x0, y0, cs = (500000.0, 5900000.0, 2.0) if crs != "EPSG:4326" else (-9.5, 53.0, 0.00003)
    with rasterio.open(tif, "w", driver="GTiff", width=50, height=60, count=1, dtype="float32", crs=crs,
                       transform=from_origin(x0, y0, cs, cs), nodata=nd) as d:
        d.write(a, 1)
    z = tmp_path / "s.zip"
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zz:
        zz.write(tif, "BY_TEST_2m_U29N.tif")
        zz.writestr("BY_TEST_2m_U29N.tfw", "2\n0\n0\n-2\n0\n0\n")
    os.remove(tif)
    return str(z)


def test_an_infomar_survey_is_cut_to_its_shallow_pixels(tmp_path):
    work = tmp_path / "w"
    work.mkdir()
    plist = P.load_list(_list(tmp_path, kind="infomar", crs="EPSG:32629",
                              files=[dict(url=GSI % "a", name="a")]))
    tif = P.prep_infomar(_survey_zip(tmp_path), str(work), plist, "a", log=lambda *a: None)
    with rasterio.open(tif) as d:
        assert d.crs.to_string() == "EPSG:32629" and d.nodata == -9999.0
        assert (d.width, d.height) == (40, 40)            # rows 10-49, columns 5-44
        assert d.bounds.left == pytest.approx(500010.0) and d.bounds.top == pytest.approx(5899980.0)
        a = d.read(1)
    assert a[0, 0] == -12.0 and a[a != -9999.0].min() == -44.0 and (a[20:30, 15:25] == -9999.0).all()
    assert not os.path.exists(os.path.join(str(work), "in_a.tif"))


def test_an_infomar_survey_in_another_zone_is_warped_to_the_list_crs(tmp_path):
    work = tmp_path / "w"
    work.mkdir()
    plist = P.load_list(_list(tmp_path, kind="infomar", crs="EPSG:32629", files=[dict(url=GSI % "b", name="b")]))
    tif = P.prep_infomar(_survey_zip(tmp_path, crs="EPSG:32630"), str(work), plist, "b", log=lambda *a: None)
    with rasterio.open(tif) as d:
        assert d.crs.to_string() == "EPSG:32629"
        a = d.read(1)
        assert ((a >= -45.0) | (a == -9999.0)).all() and (a == -12.0).any()


def test_a_survey_with_nothing_shallow_is_left_out(tmp_path):
    from rasterio.transform import from_origin
    tif = tmp_path / "deep.tif"
    with rasterio.open(tif, "w", driver="GTiff", width=8, height=8, count=1, dtype="float32", crs="EPSG:32629",
                       transform=from_origin(0, 0, 2, 2), nodata=-1e30) as d:
        d.write(np.full((8, 8), -150.0, np.float32), 1)
    z = tmp_path / "deep.zip"
    with zipfile.ZipFile(z, "w") as zz:
        zz.write(tif, "deep.tif")
    plist = P.load_list(_list(tmp_path, kind="infomar", crs="EPSG:32629", files=[dict(url=GSI % "c", name="c")]))
    assert P.prep_infomar(str(z), str(tmp_path), plist, "c", log=lambda *a: None) is None


def test_a_zip_member_with_a_parent_path_is_refused(tmp_path):
    z = tmp_path / "e.zip"
    with zipfile.ZipFile(z, "w") as zz:
        zz.writestr("../evil.tif", b"x")
    with pytest.raises(ValueError, match="unsafe"):
        P.zip_tif_member(str(z))


# ---------------------------------------------------------------------------------------------------- emodnet

def _emodnet_zip(tmp_path):
    a = np.full((30, 40), 9999.0, np.float32)
    a[5:25, 5:35] = -50.0
    p = tmp_path / "E9_MSL_2024.asc"
    with open(p, "w") as f:
        f.write("ncols 40\nnrows 30\nxllcorner -10.0\nyllcorner 40.0\ncellsize 0.25\nNODATA_value 9999.0\n")
        for row in a:
            f.write(" ".join("%.1f" % v for v in row) + "\n")
    z = tmp_path / "E9.zip"
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zz:
        zz.write(p, "E9_MSL_2024.asc")
    return str(z)


def test_an_emodnet_tile_is_cut_to_each_box_it_meets(tmp_path):
    plist = P.load_list(_list(tmp_path, kind="emodnet", crs="EPSG:4326",
                              boxes=[[-9.0, 44.0, -6.0, 46.0], [-5.0, 41.0, -2.0, 43.0], [20.0, 0.0, 21.0, 1.0]],
                              files=[dict(url=EMOD % "E9", name="E9")]))
    tifs = P.prep_emodnet(_emodnet_zip(tmp_path), str(tmp_path), plist, "E9", log=lambda *a: None)
    assert len(tifs) == 2                                 # the third box misses the tile
    with rasterio.open(tifs[0]) as d:
        assert d.crs.to_string() == "EPSG:4326" and d.nodata == 9999.0
        assert (d.bounds.left, d.bounds.bottom, d.bounds.right, d.bounds.top) == pytest.approx((-9.0, 44.0, -6.0, 46.0))
        a = d.read(1)
        assert (a[:, 1:] == -50.0).all() and (a[:, 0] == 9999.0).all()     # the fixture's water starts at -8.75


# ---------------------------------------------------------------------------------------------------- run / merge

class FakeClient:
    def __init__(self, store):
        self.store = store

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.store.objects[Key])}


class FakeStore:
    bucket = "b"

    def __init__(self):
        self.objects = {}
        self.client = FakeClient(self)

    def list_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]

    def put(self, key, body, ct, cache):
        self.objects[key] = body


def test_run_uploads_each_prepared_zip_once_and_merge_keeps_the_list_order(tmp_path, monkeypatch):
    src = _emodnet_zip(tmp_path)
    files = [dict(url=EMOD % t, name=t) for t in ("A1", "B2")]
    plist = P.load_list(_list(tmp_path, kind="emodnet", set="emodnet-test", crs="EPSG:4326",
                              boxes=[[-9.0, 44.0, -6.0, 46.0]], files=files))

    def fake_fetch(url, dest, **kw):
        shutil.copyfile(src, dest)
        return os.path.getsize(dest), "0" * 64
    monkeypatch.setattr(P, "fetch_retry", fake_fetch)
    store = FakeStore()
    work = tmp_path / "w"
    work.mkdir()
    p2 = P.run(plist, "2/2", str(work), store, log=lambda *a: None)
    p1 = P.run(plist, "1/2", str(work), store, log=lambda *a: None)
    assert len(store.objects) == 2
    writes = dict(store.objects)
    again = P.run(plist, "1/2", str(work), store, log=lambda *a: None)
    assert again["rows"] == p1["rows"] and store.objects == writes      # a rerun: the same bytes, no new key
    doc = P.merge(plist, [p2, p1])
    assert [r["url"] for r in doc["files"]] == [EMOD % "A1", EMOD % "B2"]
    row = p1["rows"][0]
    key = M.key_of(plist, row)
    assert key.startswith("static/bathy-src/emodnet-test/") and key.endswith(".zip")
    body = store.objects[key]
    assert hashlib.sha256(body).hexdigest() == row["sha256"] and len(body) == row["bytes"]
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        assert z.namelist() == [row["member"]] and z.testzip() is None
    assert not os.listdir(work)                           # nothing left behind
    twin = dict(p2, rows=[dict(p2["rows"][0], sha256=row["sha256"])])
    with pytest.raises(ValueError, match="twice"):        # two archives that prepare to the same bytes: refused
        P.merge(plist, [twin, p1])


def test_merge_orders_rows_as_the_list_and_refuses_a_missing_archive(tmp_path):
    files = [dict(url=GSI % n, name=n) for n in ("new", "mid", "old")]
    plist = P.load_list(_list(tmp_path, kind="infomar", crs="EPSG:32629", files=files))
    row = lambda n, h: dict(url=GSI % n, bytes=10, sha256=h * 64, member=n + ".tif", bbox=[-9.0, 53.0, -8.0, 54.0])
    parts = [dict(rows=[row("old", "c"), row("new", "a")], left_out=[]), dict(rows=[], left_out=[GSI % "mid"])]
    doc = P.merge(plist, parts)
    assert [r["member"] for r in doc["files"]] == ["new.tif", "old.tif"] and doc["derived"] == "the 1-m DEM"
    m = tmp_path / "m.json"
    m.write_text(json.dumps(doc))
    assert M.load(str(m))["set"] == "litto3d-test"
    assert "prepared from the publisher's archives" in M.readme(doc)
    with pytest.raises(ValueError, match="in no part"):
        P.merge(plist, [dict(rows=[row("new", "a")], left_out=[])])


def test_an_infomar_survey_is_cut_to_the_list_box(tmp_path):
    work = tmp_path / "w"
    work.mkdir()
    # the fixture's grid starts at UTM 29N x 500000 (9 W); a box from 8.99925 W cuts away its first ~25 columns
    plist = P.load_list(_list(tmp_path, kind="infomar", crs="EPSG:32629", boxes=[[-8.99925, 52.0, -8.0, 54.0]],
                              files=[dict(url=GSI % "d", name="d")]))
    tif = P.prep_infomar(_survey_zip(tmp_path), str(work), plist, "d", log=lambda *a: None)
    with rasterio.open(tif) as d:
        assert d.bounds.left >= 500040.0
        a = d.read(1)
    assert not (a == -12.0).any() and (a == -44.0).any()   # the west block is outside; the east block stays


def test_a_prepared_grid_reaching_far_past_its_box_is_refused(tmp_path):
    from rasterio.transform import from_origin
    tif = tmp_path / "far.tif"
    with rasterio.open(tif, "w", driver="GTiff", width=4, height=4, count=1, dtype="float32", crs="EPSG:4326",
                       transform=from_origin(-12.0, 54.0, 0.1, 0.1), nodata=-9999.0) as d:
        d.write(np.zeros((4, 4), np.float32), 1)
    near = dict(boxes=[[-11.9, 53.0, -5.0, 55.0]])
    assert P.check_fit(str(tif), near) == str(tif)          # 0.1 past the box: inside FIT_PAD
    with pytest.raises(ValueError, match="reaches past"):
        P.check_fit(str(tif), dict(boxes=[[-11.0, 53.0, -5.0, 55.0]]))


def test_a_compound_utm_plus_lat_crs_counts_as_its_utm_zone():
    from rasterio.crs import CRS as RCRS
    wkt = RCRS.from_epsg(32629).to_wkt()
    comp = RCRS.from_wkt('COMPD_CS["WGS 84 / UTM zone 29N + LAT depth",%s,VERT_CS["LAT depth",VERT_DATUM["Lowest '
                         'Astronomic Tide",2005],UNIT["metre",1],AXIS["Depth",DOWN]]]' % wkt)
    assert P.same_horizontal(comp, "EPSG:32629") and not P.same_horizontal(comp, "EPSG:32630")
    assert P.same_horizontal(RCRS.from_epsg(32629), "EPSG:32629")
