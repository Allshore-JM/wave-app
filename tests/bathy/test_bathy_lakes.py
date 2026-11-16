"""The Great Lakes of plan section 98: NCEI's lake grids (tools/bathy/bathy_lakes.py: one tar.gz per lake, checked by its
pinned size and SHA-256, one GeoTIFF taken out of it and checked), the twelve lake regions and their sources
(bathy_sources.LAKE_BOXES), and a small lake build through the real builder. Needs rasterio and scipy (the bathy-build
workflow installs them); skipped elsewhere."""
import hashlib
import io
import os
import re
import sys
import tarfile

import numpy as np
import pytest

pytest.importorskip("scipy")
rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))
sys.path.insert(0, os.path.join(ROOT, "tools", "coast"))

import bathy_lakes as L  # noqa: E402
import bathy_sources as S  # noqa: E402
import bathy_tiles as T  # noqa: E402
import build_bathy as B  # noqa: E402
import build_coast as C  # noqa: E402

Q = 10000
LAKE_REGIONS = sorted(n for n in S.REGIONS if n.startswith("lake-"))
GRID_REGIONS = [n for n in LAKE_REGIONS if not n.endswith("-dem")]
DEM_REGIONS = [n for n in LAKE_REGIONS if n.endswith("-dem")]


# ---------------------------------------------------------------- small files

def _tif(path, bounds=(-88.0004, 41.6196, -84.4996, 46.0904), crs="EPSG:4269", nodata=-9999.0, count=1, data=None):
    """A small GeoTIFF over `bounds` (west, south, east, north): the shape NCEI's lake grids have."""
    w, s, e, n = bounds
    if data is None:
        data = np.full((40, 70), -12.0, dtype=np.float32)
    h, wd = data.shape
    with rasterio.open(path, "w", driver="GTiff", width=wd, height=h, count=count, dtype="float32", crs=crs,
                       transform=from_origin(w, n, (e - w) / wd, (n - s) / h), nodata=nodata) as d:
        for b in range(1, count + 1):
            d.write(data.astype(np.float32), b)
    return path


def _archive(path, members):
    """A tar.gz of (name, bytes | None for a symlink to the GeoTIFF's name, or a TarInfo type) members."""
    with tarfile.open(path, "w:gz") as tf:
        for name, body in members:
            ti = tarfile.TarInfo(name)
            if body is None:
                ti.type = tarfile.SYMTYPE
                ti.linkname = "michigan_lld.tif"
                tf.addfile(ti)
            elif body == "dir":
                ti.type = tarfile.DIRTYPE
                tf.addfile(ti)
            else:
                ti.size = len(body)
                tf.addfile(ti, io.BytesIO(body))
    return path


def _pin(monkeypatch, archive, lake="michigan", bounds=None):
    """Pin the archive's own size and hash for `lake` (the real pins are NCEI's)."""
    data = open(archive, "rb").read()
    spec = dict(L.GRIDS[lake], sha256=hashlib.sha256(data).hexdigest(), bytes=len(data))
    if bounds:
        spec["bounds"] = bounds
    monkeypatch.setitem(L.GRIDS, lake, spec)


def _good_archive(tmp_path, extra=()):
    tif = _tif(str(tmp_path / "src.tif"))
    body = open(tif, "rb").read()
    return _archive(str(tmp_path / "michigan.tar.gz"),
                    [("michigan_lld", "dir"), ("michigan_lld/michigan_lld.tif", body)] + list(extra)), body


# ---------------------------------------------------------------- the download

def test_fetch_downloads_checks_and_takes_out_one_geotiff(tmp_path, monkeypatch):
    src, body = _good_archive(tmp_path, extra=[("michigan_lld/readme.txt", b"hello"), ("evil.sh", b"rm -rf /")])
    _pin(monkeypatch, src)
    asked = []

    def get(url, path, ua):
        asked.append((url, ua))
        with open(path, "wb") as f:
            f.write(open(src, "rb").read())

    work = str(tmp_path / "work")
    out = L.fetch("michigan", work, "test-agent", get=get)
    assert asked == [(L.url("michigan"), "test-agent")]
    assert L.url("michigan") == ("https://www.ngdc.noaa.gov/mgg/greatlakes/michigan/data/geotiff/"
                                 "michigan_lld.geotiff.tar.gz")
    assert out == os.path.join(work, "lake_michigan", "michigan_lld.tif")
    assert open(out, "rb").read() == body
    folder = sorted(os.listdir(os.path.join(work, "lake_michigan")))
    assert folder == ["michigan_lld.geotiff.tar.gz", "michigan_lld.tif"]      # nothing else unpacked
    assert not os.path.exists(os.path.join(work, "evil.sh"))
    assert L.fetch("michigan", work, "test-agent", get=get) == out             # the GeoTIFF again: no download
    assert len(asked) == 1


def test_fetch_keeps_a_downloaded_archive_and_takes_a_local_one(tmp_path, monkeypatch):
    src, _ = _good_archive(tmp_path)
    _pin(monkeypatch, src)

    def get(*a):
        raise AssertionError("no download wanted")

    out = L.fetch("michigan", str(tmp_path / "w1"), "ua", archive=src, get=get)       # a local copy, still checked
    assert os.path.exists(out)
    folder = os.path.join(str(tmp_path / "w2"), "lake_michigan")
    os.makedirs(folder)
    with open(os.path.join(folder, "michigan_lld.geotiff.tar.gz"), "wb") as f:      # an earlier download
        f.write(open(src, "rb").read())
    assert os.path.exists(L.fetch("michigan", str(tmp_path / "w2"), "ua", get=get))


@pytest.mark.parametrize("what", ["bytes", "sha256"])
def test_fetch_refuses_an_archive_that_is_not_the_pinned_one(tmp_path, monkeypatch, what):
    src, _ = _good_archive(tmp_path)
    _pin(monkeypatch, src)
    spec = dict(L.GRIDS["michigan"])
    spec[what] = spec[what] + 1 if what == "bytes" else "0" * 64
    monkeypatch.setitem(L.GRIDS, "michigan", spec)
    with pytest.raises(ValueError, match="not the pinned"):
        L.fetch("michigan", str(tmp_path / "w"), "ua", archive=src)
    assert not os.path.exists(os.path.join(str(tmp_path / "w"), "lake_michigan", "michigan_lld.tif"))


def test_fetch_refuses_an_unknown_lake(tmp_path):
    with pytest.raises(ValueError, match="no Great Lakes grid"):
        L.fetch("champlain", str(tmp_path), "ua", get=lambda *a: None)


def test_a_kept_geotiff_is_still_checked(tmp_path, monkeypatch):
    src, _ = _good_archive(tmp_path)
    _pin(monkeypatch, src)
    out = L.fetch("michigan", str(tmp_path / "w"), "ua", archive=src)
    _tif(out, bounds=(-87.0, 41.6196, -84.4996, 46.0904))                      # changed since: other bounds
    with pytest.raises(ValueError, match="bounds"):
        L.fetch("michigan", str(tmp_path / "w"), "ua", archive=src)


def test_the_download_names_itself_and_keeps_tls_checks(monkeypatch, tmp_path):
    """download() sends the named user agent and uses certifi's list (never an unverified context)."""
    seen = {}

    class Answer(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def urlopen(req, context=None, timeout=None):
        seen["ua"] = req.get_header("User-agent")
        seen["verify"] = context.verify_mode
        seen["timeout"] = timeout
        return Answer(b"abc")

    monkeypatch.setattr(L.urllib.request, "urlopen", urlopen)
    path = str(tmp_path / "a.tar.gz")
    L.download("https://example.invalid/x", path, "allshoresurf-bathy-build/1.0")
    assert open(path, "rb").read() == b"abc" and not os.path.exists(path + ".part")
    assert seen["ua"] == "allshoresurf-bathy-build/1.0"
    import ssl
    assert seen["verify"] == ssl.CERT_REQUIRED and seen["timeout"] == 120


# ---------------------------------------------------------------- what is taken out of the archive

def test_extract_takes_the_dot_slash_name_too(tmp_path):
    body = open(_tif(str(tmp_path / "src.tif")), "rb").read()
    arc = _archive(str(tmp_path / "a.tar.gz"), [("./michigan_lld/michigan_lld.tif", body)])
    dest = str(tmp_path / "d")
    os.makedirs(dest)
    assert open(L.extract_tif(arc, "michigan", dest), "rb").read() == body


@pytest.mark.parametrize("members, match", [
    ([("michigan_lld/other.tif", b"x")], "holds no"),
    ([("../michigan_lld/michigan_lld.tif", b"x")], "holds no"),                # never a name climbing out
    ([("/michigan_lld/michigan_lld.tif", b"x")], "holds no"),
    ([("huron_lld/huron_lld.tif", b"x")], "holds no"),                         # another lake's grid
    ([("michigan_lld/michigan_lld.tif", None)], "not a regular file"),         # a symlink
    ([("michigan_lld/michigan_lld.tif", "dir")], "not a regular file"),
    ([("michigan_lld/michigan_lld.tif", b"")], "bytes"),                       # empty
])
def test_extract_refuses_what_is_not_the_lakes_geotiff(tmp_path, members, match):
    arc = _archive(str(tmp_path / "a.tar.gz"), members)
    dest = str(tmp_path / "d")
    os.makedirs(dest)
    with pytest.raises(ValueError, match=match):
        L.extract_tif(arc, "michigan", dest)
    assert os.listdir(dest) == []
    assert not os.path.exists(str(tmp_path / "michigan_lld.tif"))


def test_extract_refuses_a_member_over_the_size_cap(tmp_path, monkeypatch):
    arc = _archive(str(tmp_path / "a.tar.gz"), [("michigan_lld/michigan_lld.tif", b"x" * 100)])
    monkeypatch.setattr(L, "MAX_TIF", 99)
    dest = str(tmp_path / "d")
    os.makedirs(dest)
    with pytest.raises(ValueError, match="allowed 1..99"):
        L.extract_tif(arc, "michigan", dest)
    monkeypatch.setattr(L, "MAX_TIF", 100)
    assert os.path.getsize(L.extract_tif(arc, "michigan", dest)) == 100


def test_extract_takes_the_first_of_two_members_of_that_name(tmp_path):
    arc = _archive(str(tmp_path / "a.tar.gz"), [("michigan_lld/michigan_lld.tif", b"first"),
                                                ("michigan_lld/michigan_lld.tif", b"second!")])
    dest = str(tmp_path / "d")
    os.makedirs(dest)
    assert open(L.extract_tif(arc, "michigan", dest), "rb").read() == b"first"


# ---------------------------------------------------------------- the GeoTIFF's own checks

def test_check_tif_takes_the_pinned_shape(tmp_path):
    p = _tif(str(tmp_path / "ok.tif"))
    assert L.check_tif(p, "michigan") == p
    q = _tif(str(tmp_path / "near.tif"), bounds=(-88.005, 41.615, -84.495, 46.095))   # within BOUNDS_TOL
    assert L.check_tif(q, "michigan") == q


@pytest.mark.parametrize("kw, match", [
    (dict(crs="EPSG:32616", bounds=(400000.0, 4600000.0, 700000.0, 5100000.0)), "not geographic"),
    (dict(nodata=-99999.0), "nodata"),
    (dict(nodata=None), "nodata"),
    (dict(count=2), "bands"),
    (dict(bounds=(-88.0004, 41.6196, -84.4996, 46.2)), "bounds"),
    (dict(bounds=(-92.2004, 45.9996, -83.9996, 49.5004)), "bounds"),            # Superior's grid under Michigan's name
])
def test_check_tif_refuses_another_shape(tmp_path, kw, match):
    p = _tif(str(tmp_path / "bad.tif"), **kw)
    with pytest.raises(ValueError, match=match):
        L.check_tif(p, "michigan")


def test_the_pins_cover_every_lake_grid_a_source_names():
    lakes = {s["lake"] for s in S.SOURCES.values() if s.get("kind") == "lakegrid"}
    assert lakes == set(L.GRIDS) == {"superior", "michigan", "huron", "erie", "ontario"}
    for lake, spec in L.GRIDS.items():
        assert re.fullmatch(r"[0-9a-f]{64}", spec["sha256"]), lake
        assert 1_000_000 < spec["bytes"] < 500_000_000, lake
        w, s, e, n = spec["bounds"]
        assert -93 < w < e < -75 and 40 < s < n < 50, lake


# ---------------------------------------------------------------- the twelve lake regions

def test_twelve_lake_regions_in_pairs_over_one_box():
    assert len(GRID_REGIONS) == 6 and len(DEM_REGIONS) == 6
    assert sorted(GRID_REGIONS) == sorted(S.LAKE_BOXES)
    for name in GRID_REGIONS:
        g, d = S.REGIONS[name], S.REGIONS[name + "-dem"]
        box, level, grid, dems, ex = S.LAKE_BOXES[name]
        assert g["box"] == d["box"] == box
        assert g["sources"] == [grid] and d["fill"] == [grid] and d["sources"] == dems
        assert g["mhhw"] == d["mhhw"] == level
        assert g["seed"] == d["seed"] == "lake" and g["cut"] and d["cut"]
        assert (g["zmin"], g["prio"]) == (5, 10) and (d["zmin"], d["prio"], d["margin"]) == (12, 12, 72)
        assert g["exclude"] == ex and set(d["exclude"]) == set(ex) | {e + "-dem" for e in ex}


def test_lake_levels_sit_above_low_water_datum():
    """mhhw for a lake = its mean level above its Low Water Datum (the water rule: elevation <= the level)."""
    want = {"lake-superior": 0.19, "lake-michigan": 0.39, "lake-huron": 0.33, "lake-stclair": 0.69,
            "lake-erie": 0.77, "lake-ontario": 0.49}
    assert {n: S.REGIONS[n]["mhhw"] for n in GRID_REGIONS} == want


def test_lake_sources_take_their_lakes_low_water_datum():
    lwd = S.LAKE_LWD
    assert lwd == {"superior": 183.2, "michigan": 176.0, "huron": 176.0, "stclair": 174.4, "erie": 173.5,
                   "ontario": 74.2}
    lake_of = {"lake-superior": "superior", "lake-michigan": "michigan", "lake-huron": "huron",
               "lake-stclair": "stclair", "lake-erie": "erie", "lake-ontario": "ontario"}
    for name, lake in lake_of.items():
        for sid in S.REGIONS[name + "-dem"]["sources"]:
            s = S.SOURCES[sid]
            assert s["lake_lwd"] == lwd[lake], sid
            navd = "NAVD88" in s["datum"]
            want = -round(lwd[lake] + S.NAVD88_IGLD85_M, 3) if navd else -lwd[lake]
            assert s["offset"] == pytest.approx(want, abs=1e-9), sid
            assert s["zmax"] == 15 and s["clip"] and s["id"] < S.SOURCES[S.REGIONS[name]["sources"][0]]["id"], sid
        grid = S.SOURCES[S.REGIONS[name]["sources"][0]]
        assert grid["kind"] == "lakegrid" and grid["zmax"] == 11 and grid["clip"]
    stc = S.SOURCES["lakegrid-stclair"]
    assert stc["lake"] == "erie" and stc["offset"] == -0.9                     # Erie's grid, 0.9 m lower datum
    assert all(S.SOURCES["lakegrid-" + k]["offset"] == 0.0 for k in ("superior", "michigan", "huron", "erie",
                                                                        "ontario"))


def test_lake_regions_configure_and_never_mix_ids():
    for name in LAKE_REGIONS:
        reg = B.region_config(name)
        ids = [s["id"] for s in reg["srcs"]]
        assert len(set(ids)) == len(ids) and T.LAND_ID not in ids, name
        if name.endswith("-dem"):
            assert reg["zmax"] == 15 and any(s["fill"] for s in reg["srcs"])
        else:
            assert reg["zmax"] == 11


def _overlap(a, b):
    return a[0] < b[2] and b[0] < a[2] and a[1] < b[3] and b[1] < a[3]


def test_overlapping_lake_boxes_cut_each_other():
    """Where two lakes' boxes overlap, the later one in the list leaves the other's box out (no pixel drawn twice)."""
    for i, a in enumerate(GRID_REGIONS):
        for b in GRID_REGIONS[i + 1:]:
            if _overlap(S.REGIONS[a]["box"], S.REGIONS[b]["box"]):
                ea, eb = S.REGIONS[a].get("exclude", ()), S.REGIONS[b].get("exclude", ())
                assert b in ea or a in eb, (a, b)


def test_lake_boxes_stay_off_every_other_region():
    others = [n for n in S.REGIONS if not n.startswith("lake-") and n != "gebco"]
    for name in LAKE_REGIONS:
        for o in others:
            assert not _overlap(S.REGIONS[name]["box"], S.REGIONS[o]["box"]), (name, o)


def test_region_names_and_count_fit_the_client():
    assert all(re.fullmatch(r"[a-z0-9_-]{1,32}", n) for n in S.REGIONS)
    assert len(S.REGIONS) <= 64


def test_every_lake_source_names_a_credit_the_short_credit_can_use():
    for name in LAKE_REGIONS:
        for sid in S.REGIONS[name]["sources"] + S.REGIONS[name].get("fill", []):
            s = S.SOURCES[sid]
            assert s["credit"].split()[0] in ("NOAA", "USACE", "Michigan"), sid
            assert s["licence"] and s["year"], sid


# ---------------------------------------------------------------- a small lake build

W, E, SO, N, RES = -87.0, -86.8, 43.0, 43.04, 0.0004


def _ring(lon0, lat0, lon1, lat1, clockwise=False):
    lons, lats = [lon0, lon1, lon1, lon0], [lat0, lat0, lat1, lat1]
    if clockwise:
        lons, lats = lons[::-1], lats[::-1]
    return (np.round(np.array(lons) * Q).astype(np.int64), np.round(np.array(lats) * Q).astype(np.int64))


def _lake_build(tmp_path, monkeypatch, local_grid=True, fetched=None):
    """Land (+5 m) west of 86.95 W with a deep pond (-30 m) in it; the lake (-8 m, never deeper than DEEP_M) east of it
    with an island (+3 m). The shoreline file holds the lake (anticlockwise) and its island (clockwise)."""
    width, height = int(round((E - W) / RES)), int(round((N - SO) / RES))
    lo, la = np.meshgrid(W + (np.arange(width) + 0.5) * RES, N - (np.arange(height) + 0.5) * RES)
    data = np.where(lo < -86.95, 5.0, -8.0)
    data = np.where((lo > -86.99) & (lo < -86.97), -30.0, data)
    data = np.where((lo > -86.88) & (lo < -86.86) & (la > 43.015) & (la < 43.025), 3.0, data)
    grid = _tif(str(tmp_path / "michigan_lld.tif"), bounds=(W, SO, E, N), data=data)
    coast = str(tmp_path / "lakes.bin")
    with open(coast, "wb") as f:
        f.write(C.encode_file([[_ring(-86.95, 42.9, -86.5, 43.2),
                                _ring(-86.88, 43.015, -86.86, 43.025, clockwise=True)]], 30, Q))
    B.land_pieces.cache_clear()
    monkeypatch.setitem(S.SOURCES, "lktest", dict(S.LAKEGRID, lake="michigan", year="2099",
                                                    credit="NOAA test grid", name="test lake grid"))
    monkeypatch.setitem(S.REGIONS, "lktest", dict(box=(W, SO, E, N), sources=["lktest"], mhhw=0.39, zmin=8,
                                                    prio=10, cut=True, seed="lake"))
    if fetched is not None:
        def fetch(lake, work, ua, archive=None, get=None):
            fetched.append((lake, work, ua))
            return grid
        monkeypatch.setattr(B.L, "fetch", fetch)
    B._core.cache_clear()
    local = {"seed:lake": coast}
    if local_grid:
        local["lktest"] = grid
    B.build("lktest", str(tmp_path / "out"), version=1, workers=1, local=local)
    B._core.cache_clear()
    return os.path.join(str(tmp_path / "out"), "lktest", "v1")


def _px(out, z, lon, lat):
    x, y = T.tile_of(lon, lat, z)
    p = B.tile_path(out, z, x, y)
    if not os.path.exists(p):
        return None
    code, sf = T.unpack_tile(open(p, "rb").read())
    x0, y0, x1, y1 = T.tile_bounds(z, x, y)
    s = (x1 - x0) / T.TILE
    mx, my = T.lonlat_to_xy(lon, lat)
    return int(code[int((y1 - my) / s) + 1, int((mx - x0) / s) + 1]), int(sf[int((y1 - my) / s) + 1,
                                                                             int((mx - x0) / s) + 1])


def test_a_lake_build_reads_the_lake_below_its_datum_and_drops_pond_and_island(tmp_path, monkeypatch):
    out = _lake_build(tmp_path, monkeypatch)
    lake = _px(out, 11, -86.83, 43.03)
    assert lake is not None and lake[1] & T.SRC_BITS == 20, lake               # the lake grid's id
    assert T.DECODE[lake[0]] == pytest.approx(8.0, abs=0.05), lake            # 8 m below the Low Water Datum
    near = _px(out, 11, -86.945, 43.02)                                        # beside the shore, joined to the lake
    assert near is not None and near[0] > 0, near
    pond = _px(out, 11, -86.98, 43.02)
    assert pond is None or pond[0] == 0, pond                                  # a deep pond on the land: no lake
    island = _px(out, 11, -86.87, 43.02)
    assert island is not None and island[0] == 0, island
    assert B.check(out, log_fn=lambda *a: None) == 0
    text = open(os.path.join(out, "LICENSE.txt"), encoding="utf-8").read()
    assert "Low Water Datum" in text and "Lake St Clair" in text


def test_a_lake_build_without_a_local_grid_fetches_ncei_archive(tmp_path, monkeypatch):
    fetched = []
    out = _lake_build(tmp_path, monkeypatch, local_grid=False, fetched=fetched)
    assert len(fetched) == 1
    lake, work, ua = fetched[0]
    assert lake == "michigan" and ua == B.USER_AGENT and os.path.isabs(work)
    near = _px(out, 11, -86.83, 43.03)
    assert near is not None and near[0] > 0


def test_a_licence_without_a_lake_grid_says_nothing_of_lakes():
    reg = B.region_config("hawaii")
    assert "Low Water Datum" not in B.licence_text(reg)
