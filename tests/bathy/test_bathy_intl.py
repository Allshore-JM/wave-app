"""International coasts (plan section 111a): the pinned STAC collection (LINZ), the mirrored archive sets (NSW), the
regions' configuration, and whole builds from local files in each source's own CRS and datum.
Needs scipy and rasterio (the bathy-build workflow installs them); skipped elsewhere."""
import hashlib
import io
import json
import os
import sys
import zipfile

import numpy as np
import pytest

pytest.importorskip("scipy")
rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin  # noqa: E402
from rasterio.warp import transform as warp_transform  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))

import bathy_fetch as F  # noqa: E402
import bathy_mirror as M  # noqa: E402
import bathy_sources as S  # noqa: E402
import bathy_stac as ST  # noqa: E402
import bathy_tiles as T  # noqa: E402
import build_bathy as B  # noqa: E402

INTL = ("au-nsw-s", "au-nsw-n", "nz-north", "nz-central", "nz-south")


# ---------------------------------------------------------------- the STAC collection

BASE = "https://nz-coastal.s3.ap-southeast-2.amazonaws.com/new-zealand/new-zealand/dem_1m/2193/"


def _item(sheet, bbox, asset=None):
    return json.dumps({"type": "Feature", "id": sheet, "bbox": list(bbox),
                       "assets": {"visual": {"href": asset or "./%s.tiff" % sheet}}}).encode()


def _collection(items, cid="COL1"):
    """(spec, files): a collection document listing `items` {sheet: raw item bytes} and its pinned spec."""
    links = [{"rel": "root", "href": "../catalog.json"}]
    links += [{"rel": "item", "href": "./%s.json" % k, "file:checksum": "1220" + hashlib.sha256(v).hexdigest()}
              for k, v in items.items()]
    col = json.dumps({"type": "Collection", "id": cid, "links": links}).encode()
    files = {BASE + "collection.json": col}
    files.update({BASE + "%s.json" % k: v for k, v in items.items()})
    spec = dict(collection=BASE + "collection.json", id=cid, items=len(items),
                digest=ST.list_digest([(l["href"], l["file:checksum"]) for l in links if l["rel"] == "item"]))
    return spec, files


def test_the_pinned_collection_gives_each_sheet_its_outline_and_cog():
    spec, files = _collection({"BQ31": _item("BQ31", (174.573, -41.487, 174.869, -41.158)),
                               "AU26": _item("AU26", (173.131, -35.009, 173.395, -34.683))})
    got = ST.read_items(spec, files.__getitem__)
    assert got == [("AU26", (173.131, -35.009, 173.395, -34.683), "/vsicurl/" + BASE + "AU26.tiff"),
                   ("BQ31", (174.573, -41.487, 174.869, -41.158), "/vsicurl/" + BASE + "BQ31.tiff")]


def test_a_changed_collection_stops_the_build():
    items = {"AU26": _item("AU26", (173.131, -35.009, 173.395, -34.683))}
    spec, files = _collection(items)
    get = files.__getitem__
    with pytest.raises(ValueError, match="renew the pin"):           # a sheet added: count and digest differ
        ST.read_items(dict(spec, items=2), get)
    with pytest.raises(ValueError, match="renew the pin"):
        ST.read_items(dict(spec, digest="0" * 64), get)
    with pytest.raises(ValueError, match="not the pinned"):
        ST.read_items(dict(spec, id="OTHER"), get)
    bad = dict(files)
    bad[BASE + "AU26.json"] = _item("AU26", (173.0, -35.0, 173.4, -34.6))        # the item changed, its link did not
    with pytest.raises(ValueError, match="checksum"):
        ST.read_items(spec, bad.__getitem__)
    for item in (_item("AU26", (173.4, -35.0, 173.1, -34.6)),              # west of east
                 _item("AU26", (173.1, -35.0, 173.4, -34.6), asset="https://elsewhere/x.tiff"),
                 _item("AU27", (173.1, -35.0, 173.4, -34.6))):              # names another sheet
        spec2, files2 = _collection({"AU26": item})
        with pytest.raises(ValueError):
            ST.read_items(spec2, files2.__getitem__)
    with pytest.raises(ValueError, match="https"):
        ST.read_items(dict(spec, collection="http://x/collection.json"), get)


def test_the_linz_source_is_pinned_and_read_by_window():
    s = S.SOURCES["linz-nz-coastal"]
    assert s["kind"] == "stac" and s["stac"]["collection"] == S.LINZ_COLLECTION and s["stac"]["items"] == 65
    assert ST.SHA_RE.match(s["stac"]["digest"]) and s["stac"]["id"] == "01JXTPWYDWTP9TRXV5CTW6MTCX"
    assert s["geoid"] == "nz_msl_nzvd2016" and s["nodata"] == -9999.0 and s["licence"] == "CC BY 4.0"
    assert s["zmax"] == 17 and s["deep"] == (30.0, 15) and s["short"] == "LINZ" and s["clip"] is True
    assert ".tiff" in B.GDAL_ENV["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"].split(",")


# ---------------------------------------------------------------- the mirrored archive sets

def _tif(path, crs, x0, y0, n, res, fn, nodata=-3.402820018375656e38):
    xs = x0 + (np.arange(n) + 0.5) * res
    ys = y0 + n * res - (np.arange(n) + 0.5) * res
    X, Y = np.meshgrid(xs, ys)
    with rasterio.open(path, "w", driver="GTiff", width=n, height=n, count=1, dtype="float32", crs=crs,
                       transform=from_origin(x0, y0 + n * res, res, res), nodata=nodata) as d:
        d.write(fn(X, Y).astype(np.float32), 1)
    return path


MEMBER = "NSW Government - DCCEEW/DEM/5 Metre/NSW_Marine_5m_TopoBathy_DEM.tif"


def _set(tmp_path, tifs, member=MEMBER):
    """A manifest (absolute path) of one zip per GeoTIFF, and the 'bucket' {url: bytes} the zips are served from."""
    served, files = {}, []
    for t in tifs:
        b = io.BytesIO()
        with zipfile.ZipFile(b, "w", zipfile.ZIP_DEFLATED) as z:
            z.write(t, member)
            z.writestr("NSW Government - DCCEEW/DEM/5 Metre/readme_metadata.html", "x")
        data = b.getvalue()
        files.append(dict(url="https://elvis-downloads.s3.amazonaws.com/DATA_%d.zip" % len(files), bytes=len(data),
                          sha256=hashlib.sha256(data).hexdigest(), member=MEMBER, bbox=[151.0, -34.0, 151.5, -33.5]))
        served[data] = data
    m = dict(set="nsw-test", credit="NSW DPIE test", licence="CC BY 4.0", files=files)
    p = tmp_path / "mirror_test.json"
    p.write_text(json.dumps(m))
    by_url = {"%s/%s" % (F.PUBLIC, M.key_of(m, f)): d for f, d in zip(files, served)}
    return str(p), by_url


def _get(by_url):
    def get(url, path, ua):
        with open(path, "wb") as f:
            f.write(by_url[url])
    return get


def test_a_mirrored_set_comes_out_as_checked_geotiffs(tmp_path):
    t = _tif(str(tmp_path / "a.tif"), "EPSG:7856", 340000.0, 6250000.0, 64, 5.0, lambda x, y: x * 0 - 12.0)
    man, by_url = _set(tmp_path, [t])
    work = str(tmp_path / "work")
    got = F.fetch_mirror(man, work, "EPSG:7856", get=_get(by_url), log=lambda *a: None)
    assert len(got) == 1 and os.path.basename(got[0]).endswith(".tif")
    with rasterio.open(got[0]) as d:
        assert d.crs.to_string() == "EPSG:7856" and float(d.read(1)[3, 3]) == -12.0
    assert not [n for n in os.listdir(os.path.dirname(got[0])) if n.endswith(".zip")]    # the archive is gone
    # again: the GeoTIFF is reused, nothing downloaded
    assert F.fetch_mirror(man, work, "EPSG:7856", get=None, log=lambda *a: None) == got


def test_a_mirrored_set_refuses_a_changed_archive_a_wrong_crs_or_a_missing_member(tmp_path):
    t = _tif(str(tmp_path / "a.tif"), "EPSG:7856", 340000.0, 6250000.0, 16, 5.0, lambda x, y: x * 0 - 1.0)
    man, by_url = _set(tmp_path, [t])
    changed = {u: d[:40] + bytes([d[40] ^ 0xFF]) + d[41:] for u, d in by_url.items()}     # one byte flipped
    with pytest.raises(ValueError, match="hash"):
        F.fetch_mirror(man, str(tmp_path / "w1"), "EPSG:7856", get=_get(changed), log=lambda *a: None)
    with pytest.raises(ValueError, match="CRS"):
        F.fetch_mirror(man, str(tmp_path / "w2"), "EPSG:7855", get=_get(by_url), log=lambda *a: None)
    m = json.load(open(man))
    m["files"][0]["member"] = "elsewhere.tif"
    open(man, "w").write(json.dumps(m))
    with pytest.raises(ValueError, match="no member"):
        F.fetch_mirror(man, str(tmp_path / "w3"), "EPSG:7856", get=_get(by_url), log=lambda *a: None)


def test_the_nsw_source_reads_the_committed_mirror():
    s = S.SOURCES["nsw-marine18"]
    assert s["kind"] == "tilezip" and s["crs"] == "EPSG:7856" and s["zmax"] == 15 and s["res_m"] == 5
    m = M.load(os.path.join(S.DATUM_DIR, s["mirror"]))
    assert m["set"] == "nsw-marine-2018" and len(m["files"]) == 10 and s["short"] == "NSW"
    assert s["offset"] == 0.0 and "geoid" not in s and "AHD" in s["datum"]
    assert "Department of Planning, Industry and Environment" in s["credit"] and s["licence"] == "CC BY 4.0"


# ---------------------------------------------------------------- the regions

def test_the_international_regions():
    for name in INTL:
        r = S.REGIONS[name]
        assert r["cut"] is True and r["seed"] == "sea" and r["reach"] == S.INTL_REACH and r["zmin"] == 8
        assert r["prio"] == 20 and not r.get("fill") and not r.get("presence")
    assert S.REGIONS["au-nsw-s"]["mhhw"] == S.REGIONS["au-nsw-n"]["mhhw"] == 0.7
    assert all(S.REGIONS[n]["mhhw"] == "grid:nz" for n in ("nz-north", "nz-central", "nz-south"))
    # LINZ sheets that stop 300-700 m past the GSHHG shore need a nearer open-water seed than the default 1 km
    assert all(S.REGIONS[n]["seed_m"] == S.NZ_SEED_M == 300.0 for n in ("nz-north", "nz-central", "nz-south"))
    assert all("seed_m" not in S.REGIONS[n] for n in ("au-nsw-s", "au-nsw-n"))
    # no two international boxes overlap (a shared edge is allowed), and none meets a US region
    boxes = [S.REGIONS[n]["box"] for n in INTL]
    for i, a in enumerate(boxes):
        for b in boxes[i + 1:]:
            assert not (a[0] < b[2] and b[0] < a[2] and a[1] < b[3] and b[1] < a[3]), (a, b)
        for n, r in S.REGIONS.items():
            if n in INTL or n in ("gebco", "pacific-sdb"):
                continue
            b = r["box"]
            assert not (a[0] < b[2] and b[0] < a[2] and a[1] < b[3] and b[1] < a[3]), n
    # DEP's Pacific region (its box reaches 28.5 S) has no tile within half a degree of them
    import bathy_dep as D
    dep = D.rects(D.tiles(S.SOURCES["dep-sdb"]["skip_tiles"]))
    for b in boxes:
        assert not [r for r in dep if r[0] < b[2] + 0.5 and r[2] > b[0] - 0.5 and r[1] < b[3] + 0.5 and r[3] > b[1] - 0.5]


def test_the_regions_configure(monkeypatch):
    for name in INTL:
        reg = B.region_config(name)                     # the committed grids are found and their pins hold
        assert reg["zmax"] == (15 if name.startswith("au-") else 17) and reg["keep_box"] == S.REGIONS[name]["box"]
        if name.startswith("nz-"):
            assert reg["mhhw_path"] == S.MHHW_GRIDS["grid:nz"]
            assert reg["srcs"][0]["geoid_path"] == S.GEOIDS["nz_msl_nzvd2016"]
    monkeypatch.setitem(S.REGIONS, "bad", dict(S.REGIONS["nz-south"], mhhw="grid:xx"))
    with pytest.raises(ValueError, match="names no grid"):
        B.region_config("bad")
    monkeypatch.setitem(S.SOURCES, "linz-nz-coastal", dict(S.SOURCES["linz-nz-coastal"],
                                                           stac=dict(S.SOURCES["linz-nz-coastal"]["stac"], digest="x")))
    with pytest.raises(ValueError, match="digest pin"):
        B.region_config("nz-south")
    monkeypatch.setitem(S.SOURCES, "nsw-marine18", dict(S.SOURCES["nsw-marine18"], crs=None))
    with pytest.raises(ValueError, match="mirror manifest and its CRS"):
        B.region_config("au-nsw-s")


def test_the_licence_names_the_sources_and_the_tide_model():
    for name, want in (("nz-south", ["Toitu Te Whenua", "EOT20", "CC BY 4.0", "NZVD2016"]),
                       ("au-nsw-n", ["Department of Planning, Industry and Environment", "ELVIS", "CC BY 4.0", "AHD"])):
        text = B.licence_text(B.region_config(name))
        for w in want:
            assert w in text, (name, w)


# ---------------------------------------------------------------- whole builds from local files

LON0, LAT0, LON1, LAT1 = 151.30, -33.81, 151.34, -33.79       # open sea off Manly (NSW); the shore at 151.32


def _region(monkeypatch, name, key, mhhw):
    monkeypatch.setitem(S.REGIONS, name, dict(box=(LON0, LAT0, LON1, LAT1), sources=[key], mhhw=mhhw, zmin=12,
                                              prio=20, cut=True))


def _at(out, z):
    def at(lon, lat):
        x, y = T.tile_of(lon, lat, z)
        code, sf = T.unpack_tile(open(B.tile_path(out, z, x, y), "rb").read())
        x0, y0, x1, y1 = T.tile_bounds(z, x, y)
        p = (x1 - x0) / T.TILE
        mx, my = T.lonlat_to_xy(lon, lat)
        return code[int((y1 - my) / p) + 1, int((mx - x0) / p) + 1], sf[int((y1 - my) / p) + 1, int((mx - x0) / p) + 1]
    return at


def _projected_tif(path, crs, fn, res, nodata):
    """A GeoTIFF in `crs` over the test box (plus a margin), values fn(lon, lat)."""
    xs, ys = warp_transform("EPSG:4326", crs, [LON0 - 0.03, LON1 + 0.03], [LAT0 - 0.03, LAT1 + 0.03])
    x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
    n = int((x1 - x0) / res)
    m = int((y1 - y0) / res)
    gx = x0 + (np.arange(n) + 0.5) * res
    gy = y1 - (np.arange(m) + 0.5) * res
    X, Y = np.meshgrid(gx, gy)
    lon, lat = warp_transform(crs, "EPSG:4326", X.ravel().tolist(), Y.ravel().tolist())
    v = fn(np.array(lon).reshape(X.shape), np.array(lat).reshape(X.shape)).astype(np.float32)
    with rasterio.open(path, "w", driver="GTiff", width=n, height=m, count=1, dtype="float32", crs=crs,
                       transform=from_origin(x0, y1, res, res), nodata=nodata) as d:
        d.write(v, 1)
    return path


def test_a_tilezip_build_in_mga56_and_ahd(tmp_path, monkeypatch):
    """Sea at -25 m AHD east of 151.32 E, a 0.5-m beach west of it: with MHHW 0.7 the beach is water joined to the
    sea; the depths are AHD taken as mean sea level (offset 0)."""
    src = dict(S.SOURCES["nsw-marine18"], id=1, zmax=13)
    monkeypatch.setitem(S.SOURCES, "t-nsw", src)
    _region(monkeypatch, "t-nsw", "t-nsw", 0.7)
    t = _projected_tif(str(tmp_path / "nsw.tif"), "EPSG:7856",
                       lambda lon, lat: np.where(lon > 151.32, -25.0, 0.5), 5.0, -3.402820018375656e38)
    B._core.cache_clear()
    index = B.build("t-nsw", str(tmp_path / "out"), version=1, workers=0, local={"t-nsw": [t]})
    out = os.path.join(str(tmp_path / "out"), "t-nsw", "v1")
    assert B.check(out, log_fn=lambda *a: None) == 0 and index["count"] > 0
    assert index["sources"]["1"]["short"] == "NSW" and "Department of Planning" in open(
        os.path.join(out, "LICENSE.txt"), encoding="utf-8").read()
    at = _at(out, 13)
    code, sf = at(151.335, -33.80)
    assert sf == 1 and T.DECODE[code] == pytest.approx(25.0, abs=0.05)
    code, sf = at(151.305, -33.80)
    assert sf == 1 and T.DECODE[code] == pytest.approx(-0.5, abs=0.05)       # the beach, below MHHW: water


def test_a_stac_build_in_nztm_takes_nzvd2016_to_mean_sea_level(tmp_path, monkeypatch):
    """A local 'sheet' in NZTM (EPSG:2193) at -25 m NZVD2016 with MSL at -0.15 m NZVD2016: 24.85 m below MSL. The
    region's MHHW comes from a per-pixel grid (grid:nz)."""
    lon0, lat0 = 174.80, -41.32                                     # Wellington's south coast
    w, s, e, n = lon0, lat0, lon0 + 0.04, lat0 + 0.02
    monkeypatch.setitem(S.REGIONS, "t-nz", dict(box=(w, s, e, n), sources=["t-linz"], mhhw="grid:nz", zmin=12,
                                                prio=20, cut=True))
    monkeypatch.setitem(S.SOURCES, "t-linz", dict(S.SOURCES["linz-nz-coastal"], zmax=13, deep=None))
    xs, ys = warp_transform("EPSG:4326", "EPSG:2193", [w - 0.03, e + 0.03], [s - 0.03, n + 0.03])
    sheet = _tif(str(tmp_path / "sheet.tif"), "EPSG:2193", min(xs), min(ys), int((max(ys) - min(ys)) / 4.0) + 1, 4.0,
                 lambda x, y: x * 0 - 25.0, nodata=-9999.0)

    def flat(path, v):
        a = np.full((40, 40), v, np.float32)
        with rasterio.open(path, "w", driver="GTiff", width=40, height=40, count=1, dtype="float32",
                           crs="EPSG:4326", transform=from_origin(174.0, -40.5, 0.05, 0.05),
                           nodata=float("nan")) as d:
            d.write(a, 1)
        return path
    local = {"t-linz": [sheet], "geoid:nz_msl_nzvd2016": flat(str(tmp_path / "msl.tif"), -0.15),
             "mhhw:grid:nz": flat(str(tmp_path / "mhhw.tif"), 0.6)}
    B._core.cache_clear()
    index = B.build("t-nz", str(tmp_path / "out"), version=1, workers=0, local=local)
    out = os.path.join(str(tmp_path / "out"), "t-nz", "v1")
    assert B.check(out, log_fn=lambda *a: None) == 0 and index["mhhw"] == "grid:nz"
    code, sf = _at(out, 13)(lon0 + 0.02, lat0 + 0.01)
    assert sf == 1 and T.DECODE[code] == pytest.approx(24.85, abs=0.02)
    assert "EOT20" in open(os.path.join(out, "LICENSE.txt"), encoding="utf-8").read()


def test_the_committed_nz_grids_are_pinned_and_match_the_ports_and_gauges():
    """The MSL grid is interpolated from the ports themselves (exact there); the MHHW grid is EOT20 at 1/8-degree cell
    centres plus the gauges' correction, so a gauge inside a harbour (Wellington, Bluff, Lyttelton) sits up to 0.07 m
    above its cell's value (measured 2026-11-26)."""
    st = json.load(open(os.path.join(S.DATUM_DIR, "nz_datum_stations.json"), encoding="utf-8"))
    assert len(st["ports"]) == 10 and len(st["gauges"]) == 12
    for name in ("nz_msl_nzvd2016.tif", "nz_mhhw_msl.tif"):
        path = os.path.join(S.DATUM_DIR, name)
        assert hashlib.sha256(open(path, "rb").read()).hexdigest() == S.DATUM_GRIDS[name]
        with rasterio.open(path) as d:
            a = d.read(1)
            assert np.isfinite(a).all() and d.crs.to_epsg() == 4326
    assert S.GEOIDS["nz_msl_nzvd2016"] == os.path.join(S.DATUM_DIR, "nz_msl_nzvd2016.tif")
    with rasterio.open(S.GEOIDS["nz_msl_nzvd2016"]) as d:
        for p in st["ports"].values():
            assert float(next(d.sample([(p["lon"], p["lat"])]))[0]) == pytest.approx(p["msl_nzvd2016"], abs=0.002)
        assert -0.21 <= d.read(1).min() and d.read(1).max() <= -0.03
    with rasterio.open(S.MHHW_GRIDS["grid:nz"]) as d:
        for g in st["gauges"].values():
            v = float(next(d.sample([(g["lon"], g["lat"])]))[0])
            assert -0.08 <= g["mhhw_msl"] - v <= 0.08, g["name"]
        assert 0.0 < d.read(1).min() and d.read(1).max() <= 1.5
        # over the regions' own boxes the lowest cell is by Cook Strait (0.28 m, off Makara), the amphidrome off
        # Puysegur (0.03 m) lies outside them
        a, t = d.read(1), d.transform
        lon = t.c + (np.arange(a.shape[1]) + 0.5) * t.a
        lat = t.f + (np.arange(a.shape[0]) + 0.5) * t.e
        for name in ("nz-north", "nz-central", "nz-south"):
            w, s_, e, n_ = S.REGIONS[name]["box"]
            sub = a[np.ix_((lat >= s_) & (lat <= n_), (lon >= w) & (lon <= e))]
            assert sub.min() >= 0.25, (name, float(sub.min()))


def test_a_sheet_outside_every_region_stops_the_build():
    """G77 P2-1: the sources are cut to their regions' boxes, so a sheet a renewed pin brings in outside all of them
    would never be drawn: the build stops instead. The committed NSW archives all lie inside au-nsw-s / au-nsw-n."""
    m = F.manifest("mirror_nsw.json")
    B.sheets_in_regions("nsw-marine18", [(f["sha256"][:16], tuple(f["bbox"])) for f in m["files"]])
    B.sheets_in_regions("linz-nz-coastal", [("BQ31", (174.573, -41.487, 174.869, -41.158)),
                                            ("CH11", (168.429, -46.928, 168.769, -46.593))])
    with pytest.raises(ValueError, match="outside every region"):
        B.sheets_in_regions("linz-nz-coastal", [("CI01", (166.0, -48.0, 166.4, -47.6))])      # south of nz-south
    with pytest.raises(ValueError, match="outside every region"):
        B.sheets_in_regions("nsw-marine18", [("qld", (153.3, -28.0, 153.6, -27.5))])           # north of au-nsw-n


def test_a_mirrored_set_reads_only_the_archives_meeting_the_box_and_fetches_a_bad_copy_again(tmp_path):
    t = _tif(str(tmp_path / "a.tif"), "EPSG:7856", 340000.0, 6250000.0, 16, 5.0, lambda x, y: x * 0 - 1.0)
    man, by_url = _set(tmp_path, [t])                      # its bbox: 151.0-151.5 E, 34.0-33.5 S
    calls = []

    def never(url, path, ua):
        calls.append(url)
    assert F.fetch_mirror(man, str(tmp_path / "w1"), "EPSG:7856", get=never, box=(152.0, -33.0, 153.0, -32.0),
                          log=lambda *a: None) == [] and calls == []
    served = []

    def flaky(url, path, ua):                              # a damaged copy first, the right one second
        served.append(url)
        d = by_url[url]
        with open(path, "wb") as f:
            f.write(d if len(served) > 1 else d[:40] + bytes([d[40] ^ 0xFF]) + d[41:])
    got = F.fetch_mirror(man, str(tmp_path / "w2"), "EPSG:7856", get=flaky, box=(151.2, -33.8, 151.3, -33.7),
                         log=lambda *a: None)
    assert len(got) == 1 and len(served) == 2
    m = json.load(open(man))
    m["files"][0]["bbox"] = [151.5, -34.0, 151.0, -33.5]   # west of east
    open(man, "w").write(json.dumps(m))
    with pytest.raises(ValueError, match="bbox"):
        F.manifest(man)


def test_the_temporary_dry_run_workflow_is_gone():
    """The five regions were dry-run on the runner four times (runs 45949043774, 45952959741, 45956034869,
    46038647069; the last after the NZ seed change: all green, read-back medians <= 0.02 m); the push-triggered
    workflow must never reach production."""
    assert not os.path.exists(os.path.join(ROOT, ".github", "workflows", "bathy-dryrun-intl.yml"))
