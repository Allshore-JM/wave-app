"""The Pacific satellite depths of plan section 97 (Digital Earth Pacific): DEP's tile grid and the VRT the builder writes
over its COGs (incl. the column across 180 degrees), the region's settings (the 12-m cap, the four turbid tiles left out,
the US territories' boxes excluded, every value water), and a whole build of a small synthetic "dep" region.
Needs scipy and rasterio (the bathy-build workflow installs them); skipped elsewhere."""
import os
import sys
import xml.etree.ElementTree as ET

import numpy as np
import pytest

pytest.importorskip("scipy")
rasterio = pytest.importorskip("rasterio")
from rasterio.transform import from_origin  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))

import bathy_dep as D  # noqa: E402
import bathy_sources as S  # noqa: E402
import bathy_tiles as T  # noqa: E402
import build_bathy as B  # noqa: E402

TURBID = {(20, 30), (21, 30), (22, 30), (24, 31)}
US = ("hawaii", "marianas", "samoa", "prvi", "hawaii-lidar", "guam-lidar", "pr-lidar", "nwhi-lidar")


# ---------------------------------------------------------------- DEP's grid and the VRT over it

def test_the_tile_list_is_dep_s_499_tiles_on_its_grid():
    every = D.tiles()
    assert len(every) == 499 and len(set(every)) == 499
    assert min(x for x, _ in every) == 9 and max(x for x, _ in every) == 130
    assert min(y for _, y in every) == 8 and max(y for _, y in every) == 69
    assert TURBID <= set(every)
    kept = D.tiles(skip=sorted(TURBID))
    assert len(kept) == 495 and not TURBID & set(kept)


def test_tile_bounds_and_longitudes_follow_the_pacific_mercator():
    assert D.bounds(0, 0) == (-3000000.0, -4000000.0, -2904000.0, -3904000.0)
    x0, _, x1, _ = D.bounds(66, 20)
    assert D.lon_of(x0) == pytest.approx(179.968, abs=0.001)      # column 66 runs across 180 degrees
    assert D.lon_of(x1) == pytest.approx(180.830, abs=0.001)
    assert D.lon_of(0.0) == 150.0                                  # the central meridian


def test_rects_split_the_tile_across_180_and_never_span_the_world():
    (r,) = D.rects([(30, 40)])
    assert -180 <= r[0] < r[2] <= 180
    pieces = D.rects([(66, 40)])
    assert len(pieces) == 2
    east, west = pieces
    assert east[2] == 180.0 and east[0] == pytest.approx(179.967, abs=0.002)
    assert west[0] == -180.0 and west[2] == pytest.approx(-179.169, abs=0.002)
    assert east[1] == west[1] and east[3] == west[3]
    every = D.rects(D.tiles())
    assert all(w < e and e - w < 2.0 for (w, s, e, n) in every)
    lats = [v for (w, s, e, n) in every for v in (s, n)]
    assert min(lats) > -28.5 and max(lats) < 24.5                    # inside the region's box


def test_the_vrt_places_each_tile_on_one_3832_grid():
    tiles = [(65, 40), (66, 40), (66, 41)]
    root = ET.fromstring(D.vrt_text(tiles, url="https://example.invalid/"))
    assert root.attrib == {"rasterXSize": str(2 * D.PX), "rasterYSize": str(2 * D.PX)}
    assert root.find("SRS").text == "EPSG:3832"
    gt = [float(v) for v in root.find("GeoTransform").text.split(",")]
    assert gt == [D.X0 + 65 * D.TILE_M, D.RES, 0.0, D.Y0 + 42 * D.TILE_M, 0.0, -D.RES]
    band = root.find("VRTRasterBand")
    assert band.find("NoDataValue").text == "nan"
    placed = {}
    for src in band.findall("SimpleSource"):
        name = src.find("SourceFilename").text
        dst = src.find("DstRect").attrib
        placed[name.split("dep_s2_sdb_")[1][:7]] = (int(dst["xOff"]), int(dst["yOff"]))
        assert name.startswith("/vsicurl/https://example.invalid/dep_s2_sdb/0-0-1/")
        assert name.endswith("_2024-07/2024-12_depth.tif")
    # rows run north to south: the northern tile (y 41) on top
    assert placed == {"065_040": (0, D.PX), "066_040": (D.PX, D.PX), "066_041": (D.PX, 0)}
    # each pixel's geolocation = its tile's own bounds
    for (x, y), (xo, yo) in (((65, 40), placed["065_040"]), ((66, 41), placed["066_041"])):
        bx0, by0, bx1, by1 = D.bounds(x, y)
        assert gt[0] + xo * gt[1] == bx0 and gt[3] + yo * gt[5] == by1


def test_the_vrt_opens_and_reads_a_local_tile_by_its_bounds(tmp_path):
    """The VRT writer's arithmetic against GDAL itself: a local file named like DEP's, read back through the VRT."""
    x, y = 66, 40
    key = D.KEY.format(x=x, y=y)
    path = tmp_path / key
    path.parent.mkdir(parents=True)
    bx0, by0, bx1, by1 = D.bounds(x, y)
    n = 96
    data = np.full((n, n), np.nan, np.float32)
    data[10, 20] = -7.5
    with rasterio.open(str(path), "w", driver="GTiff", width=n, height=n, count=1, dtype="float32", crs="EPSG:3832",
                       transform=from_origin(bx0, by1, D.TILE_M / n, D.TILE_M / n), nodata=float("nan")) as d:
        d.write(data, 1)
    text = D.vrt_text([(x, y)], url=str(tmp_path).replace("\\", "/") + "/")
    text = text.replace("/vsicurl/", "").replace('rasterXSize="9600"', 'rasterXSize="%d"' % n)
    text = text.replace('rasterYSize="9600"', 'rasterYSize="%d"' % n).replace("9600", str(n))
    text = text.replace("%.1f, 0.0" % D.RES, "%.1f, 0.0" % (D.TILE_M / n)).replace("-%.1f<" % D.RES,
                                                                                  "-%.1f<" % (D.TILE_M / n))
    vrt = tmp_path / "t.vrt"
    vrt.write_text(text, encoding="utf-8")
    with rasterio.open(str(vrt)) as d:
        assert d.crs.to_epsg() == 3832 and tuple(d.bounds) == (bx0, by0, bx1, by1)
        a = d.read(1)
    assert a[10, 20] == pytest.approx(-7.5) and np.isnan(a).sum() == n * n - 1


# ---------------------------------------------------------------- the real region's settings

def test_the_pacific_region_is_set_up_as_decided():
    reg = B.region_config("pacific-sdb")
    (s,) = reg["srcs"]
    assert s["key"] == "dep-sdb" and s["kind"] == "dep" and s["id"] == 12
    assert s["max_depth"] == 12.0 and s["water_all"] and reg["water_all"] and reg["wrap"]
    assert set(map(tuple, s["skip_tiles"])) == TURBID
    assert (s["zmax"], s["res_m"]) == (14, 10) and reg["zmin"] == 8
    assert S.REGIONS["gebco"]["prio"] < reg["prio"] < S.REGIONS["hawaii"]["prio"]
    assert reg["exclude_boxes"] == [tuple(S.REGIONS[n]["box"]) for n in US]
    assert s["credit"].split()[0] == "DEP" and "topobathy" not in s["name"].lower()     # the client's short credit
    assert "often off by 2 m" in s["name"]
    assert s["licence"] == "CC BY 4.0"
    assert S.gdal_path(S.SOURCES["dep-sdb"]) is None    # the builder writes its VRT (no fixed NOAA path)
    assert B.footprint(s)                             # from the tile grid, no network


def test_a_region_mixing_water_all_and_other_sources_is_refused(monkeypatch):
    monkeypatch.setitem(S.REGIONS, "mix", dict(box=(170.0, -20.0, 180.0, -10.0), sources=["dep-sdb", "cudem-samoa"],
                                               mhhw=0.0, zmin=8, prio=5))
    with pytest.raises(ValueError):
        B.region_config("mix")


# ---------------------------------------------------------------- exclusion, the cap and water_all, synthetic

W, E, SO, NO = 178.20, 178.45, -17.80, -17.70        # Fiji's Beqa lagoon
RES = 0.0002
EX = (178.30, -17.80, 178.35, -17.70)                # a band the test excludes


def _dep_tif(path, fn, w=W, e=E, s=SO, n=NO, res=RES):
    width, height = int(round((e - w) / res)), int(round((n - s) / res))
    lons = w + (np.arange(width) + 0.5) * res
    lats = n - (np.arange(height) + 0.5) * res
    with rasterio.open(path, "w", driver="GTiff", width=width, height=height, count=1, dtype="float32",
                       crs="EPSG:4326", transform=from_origin(w, n, res, res), nodata=float("nan")) as d:
        d.write(fn(lons[None, :], lats[:, None]).astype(np.float32), 1)
    return path


def _depth(lon, lat):
    """-5 m west of 178.25, -11.9 / -12.1 m in two strips, -20 m east of 178.40 (beyond the cap), NaN in a pocket;
    a -0.3 m strip (very shallow water: drawn), and a strip at or above sea level (+0.5 m south, 0 m north: never
    drawn, G64a F4)."""
    lon = np.broadcast_to(lon, np.broadcast(lon, lat).shape)
    out = np.full(lon.shape, -5.0)
    out = np.where((lon > 178.235) & (lon < 178.245), -0.3, out)
    out = np.where((lon > 178.26) & (lon < 178.28), -11.9, out)
    out = np.where((lon > 178.36) & (lon < 178.38), -12.1, out)
    out = np.where((lon > 178.385) & (lon < 178.395), np.where(np.broadcast_to(lat, lon.shape) > -17.75, 0.0, 0.5),
                   out)
    out = np.where(lon > 178.40, -20.0, out)
    lat = np.broadcast_to(lat, lon.shape)
    return np.where((lat > -17.76) & (lat < -17.74) & (lon > 178.21) & (lon < 178.23), np.nan, out)


def _region(tmp_path, monkeypatch, zmin=10):
    local = {"sdb-t": _dep_tif(str(tmp_path / "sdb.tif"), _depth)}
    monkeypatch.setitem(S.SOURCES, "sdb-t", dict(S.SOURCES["dep-sdb"], zmax=13, path=None))
    monkeypatch.setitem(S.REGIONS, "ex-t", dict(box=EX, sources=["cudem-samoa"], mhhw=0.4, zmin=8, prio=10))
    monkeypatch.setitem(S.REGIONS, "sdb", dict(box=(W, SO, E, NO), sources=["sdb-t"], mhhw=0.0, zmin=zmin, prio=5,
                                               wrap=True, exclude=["ex-t"]))
    return local


def _pixel(out, z, lon, lat):
    x, y = T.tile_of(lon, lat, z)
    p = B.tile_path(out, z, x, y)
    if not os.path.exists(p):
        return None, None
    code, sf = T.unpack_tile(open(p, "rb").read())
    x0, y0, x1, y1 = T.tile_bounds(z, x, y)
    px = (x1 - x0) / T.TILE
    mx, my = T.lonlat_to_xy(lon, lat)
    i, j = int((mx - x0) / px) + 1, int((y1 - my) / px) + 1
    return float(T.DECODE[code[j, i]]) if code[j, i] else None, int(sf[j, i])


def test_read_field_caps_the_depth_and_excludes_other_regions_boxes(tmp_path, monkeypatch):
    local = _region(tmp_path, monkeypatch)
    B.gdal_env()
    reg = B.region_config("sdb", local=local)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    key = T.tile_of(178.27, -17.75, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src = B.read_field(reg, key)
    m = B.margin_of(reg)
    px = T.TILE * 2 ** B.BLOCK_LEVELS
    size = px + 2 * m
    b = T.tile_bounds(reg["zmax"] - B.BLOCK_LEVELS, key[0], key[1], apron=m, px=px)
    lons = np.array([T.xy_to_lonlat(b[0] + (i + 0.5) * (b[2] - b[0]) / size, 0.0)[0] for i in range(size)])
    lats = np.array([T.xy_to_lonlat(0.0, b[3] - (j + 0.5) * (b[3] - b[1]) / size)[1] for j in range(size)])
    j = int(np.argmin(np.abs(lats + 17.72)))
    row, rid = elev[j], src[j]

    def at(lon):
        return row[int(np.argmin(np.abs(lons - lon)))], rid[int(np.argmin(np.abs(lons - lon)))]
    assert at(178.255) == (pytest.approx(-5.0), 12)
    assert at(178.27) == (pytest.approx(-11.9), 12)                  # 11.9 m: kept
    v, i = at(178.37)
    assert np.isnan(v) and i == 0                                    # 12.1 m: beyond the cap, left out
    v, i = at(178.32)
    assert np.isnan(v) and i == 0                                    # inside the excluded box
    v, i = at(178.42)
    assert np.isnan(v) and i == 0                                    # 20 m


def test_a_dep_build_keeps_every_value_as_water_and_nothing_as_land(tmp_path, monkeypatch):
    local = _region(tmp_path, monkeypatch)
    seen = []
    coarse = B.coarse_levels

    def watch(*a, **k):
        seen.append(k.get("wrap"))
        return coarse(*a, **k)
    monkeypatch.setattr(B, "coarse_levels", watch)
    monkeypatch.setattr(B, "dep_blocks", lambda *a: pytest.fail("a local source is never pre-read"))
    B._core.cache_clear()
    index = B.build("sdb", str(tmp_path / "out"), version=1, workers=1, local=local)
    assert seen == [True]                          # the coarse zooms join across 180 degrees (a wrap region)
    B._core.cache_clear()
    out = os.path.join(str(tmp_path / "out"), "sdb", "v1")
    assert set(index["sources"]) == {"12"}
    assert {int(z) for z in index["tiles"]} == {10, 11, 12, 13}
    for z, tiles in B.scan_tiles(out).items():
        for (x, y) in tiles:
            code, sf = T.unpack_tile(open(B.tile_path(out, z, x, y), "rb").read())
            ids = sf & T.SRC_BITS
            assert not (ids == T.LAND_ID).any(), (z, x, y)               # no land marks: GEBCO shows through
            assert set(np.unique(ids[code > 0])) <= {12}
            assert not ((code > 0) & (T.DECODE[code] > 12.0 + 0.01)).any()
    d, s = _pixel(out, 13, 178.255, -17.72)
    assert d == pytest.approx(5.0, abs=0.02) and s & T.SRC_BITS == 12
    assert _pixel(out, 13, 178.27, -17.72)[0] == pytest.approx(11.9, abs=0.02)
    for lon in (178.32, 178.37, 178.42):                                  # excluded, capped, deep: nothing
        d, s = _pixel(out, 13, lon, -17.72)
        assert d is None and (s is None or s & T.SRC_BITS == 0), lon
    # an enclosed shallow patch is water too (DEP masked the land; no connectivity test)
    d, _ = _pixel(out, 13, 178.215, -17.73)
    assert d == pytest.approx(5.0, abs=0.02)
    # G64a F7: water 0.3 m deep is published as water (every block here is shallow: none is dropped for lack of
    # deep water); G64a F4: a value at or above sea level is never drawn as water, at any zoom
    for z in (13, 12):
        d, s = _pixel(out, z, 178.24, -17.72)
        assert d == pytest.approx(0.3, abs=0.02) and s & T.SRC_BITS == 12, z
        for lat in (-17.72, -17.78):                                       # 0 m north, +0.5 m south
            d, s = _pixel(out, z, 178.39, lat)
            assert d is None and (s is None or s & T.SRC_BITS == 0), (z, lat)
    assert B.check(out, log_fn=lambda *a: None) == 0
    lic = open(os.path.join(out, "LICENSE.txt"), encoding="utf-8").read()
    assert "Digital Earth Pacific" in lic and "deeper than 12 m are left out" in lic and "Copernicus Sentinel" in lic
    assert "ex-t" in lic


def test_the_excluded_box_holds_nothing_at_any_zoom_or_in_any_apron(tmp_path, monkeypatch):
    """G64a F3: the cut holds at the top zoom, the zooms pass 2 pools, and the coarse zooms built from the saved cores
    (zmin 8: two of them here), in every tile's apron too: never a pixel whose centre lies inside the box."""
    local = _region(tmp_path, monkeypatch, zmin=8)
    B._core.cache_clear()
    B.build("sdb", str(tmp_path / "out"), version=1, workers=1, local=local)
    B._core.cache_clear()
    out = os.path.join(str(tmp_path / "out"), "sdb", "v1")
    zooms, inside = set(), 0
    for z, tiles in B.scan_tiles(out).items():
        for (x, y) in tiles:
            code, sf = T.unpack_tile(open(B.tile_path(out, z, x, y), "rb").read())
            mask = B.exclude_mask(T.tile_bounds(z, x, y, apron=T.APRON), T.SIDE, [EX])
            assert not code[mask].any() and not (sf[mask] & T.SRC_BITS).any(), (z, x, y)
            assert code[~mask].any(), (z, x, y)            # the tile holds water outside the box
            zooms.add(z)
            inside += int(mask.sum())
    assert zooms == set(range(8, 14)) and inside > 0
    assert B.check(out, log_fn=lambda *a: None) == 0


def test_the_vrt_written_for_dep_never_lists_the_skipped_tiles(tmp_path):
    """G64a F7: the four turbid tiles are never read: they are not in the VRT the build writes."""
    path = B.dep_vrt(dict(S.SOURCES["dep-sdb"], key="dep-sdb"), str(tmp_path))
    root = ET.parse(path).getroot()
    names = [s.find("SourceFilename").text for s in root.find("VRTRasterBand").findall("SimpleSource")]
    held = {tuple(int(v) for v in n.split("dep_s2_sdb_")[1][:7].split("_")) for n in names}
    assert len(names) == len(held) == 495
    assert not held & TURBID and held == set(D.tiles(sorted(TURBID)))


def test_blocks_wholly_inside_an_excluded_box_are_never_read(monkeypatch):
    monkeypatch.setitem(S.REGIONS, "ex-t", dict(box=(170.0, -30.0, 180.0, 30.0), sources=["cudem-samoa"], mhhw=0.4,
                                                zmin=8, prio=10))
    monkeypatch.setitem(S.REGIONS, "sdb", dict(box=(150.0, -20.0, 180.0, -10.0), sources=["dep-sdb"], mhhw=0.0, zmin=8,
                                               prio=5, wrap=True, exclude=["ex-t"]))
    reg = B.region_config("sdb")
    s = reg["srcs"][0]
    s["rects"] = [(175.0, -18.0, 176.0, -17.0), (165.0, -18.0, 166.0, -17.0)]
    keys = B.block_keys(reg)
    z = reg["zmax"] - B.BLOCK_LEVELS
    lons = {round(T.xy_to_lonlat(*T.tile_bounds(z, x, y)[:2])[0]) for (x, y) in keys}
    assert keys and all(lon < 170 for lon in lons)


def test_the_exclude_mask_judges_a_grid_past_180_by_its_real_longitude():
    half = T.R * np.pi                                   # 3857's x at 180 degrees
    y0, y1 = T.lonlat_to_xy(0.0, -15.0)[1], T.lonlat_to_xy(0.0, -14.0)[1]
    b = (half - 10000.0, y0, half + 10000.0, y1)         # 20 km straddling the antimeridian, east side past 180
    m = B.exclude_mask(b, 20, [(-180.0, -16.0, -170.0, -13.0)])
    assert not m[:, :10].any() and m[:, 10:].all()       # the part past 180 is -180..-179.9: inside the box
    m = B.exclude_mask(b, 20, [(170.0, -16.0, 180.0, -13.0)])
    assert m[:, :10].all() and not m[:, 10:].any()


def test_licence_names_the_dep_citation_once_per_source():
    reg = B.region_config("pacific-sdb")
    text = B.licence_text(reg)
    assert text.count("registry.opendata.aws/dep-satellite-derived-bathymetry") == 1
    assert "accessed 2026-11-15" in text and "Contains modified Copernicus Sentinel data 2024" in text
    assert B.licence_text(B.region_config("pacific-sdb")) == text                  # stable: no date of today
