"""The 1-m lidar regions of plan section 96: fill sources that never drive the blocks, the ring of fill-only blocks, the
per-region margin and zoom floor, the blend at the lidar's edge, the newer survey winning, the geoid conversion of the
ellipsoid-height surveys, footprints read from projected VRTs, and a whole build of a small lidar region.
Needs scipy and rasterio (the bathy-build workflow installs them); skipped elsewhere."""
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
import build_bathy as B  # noqa: E402

LIDAR_REGIONS = ("hawaii-lidar", "guam-lidar", "pr-lidar", "nwhi-lidar")


# ---------------------------------------------------------------- the real regions' settings

def test_the_lidar_regions_are_set_up_as_planned():
    for name in LIDAR_REGIONS:
        reg = B.region_config(name)
        own = [s for s in reg["srcs"] if not s["fill"]]
        fill = [s for s in reg["srcs"] if s["fill"]]
        assert reg["zmax"] == 17 and reg["prio"] == 20 and B.margin_of(reg) == 32, name
        assert all(s["zmax"] == 17 and s["res_m"] == 1 for s in own), name
        assert all(f["id"] > max(s["id"] for s in own) for f in fill), name
        for s in own:                              # the client's short credit and label (overlay.js bathyShort / bathyLabel)
            assert s["credit"].split()[0] in ("NOAA", "USACE") and "topobathy" not in s["name"].lower(), s["key"]
            assert s.get("nodata") is not None, s["key"]
        if name == "nwhi-lidar":
            assert reg["zmin"] == 8 and not fill and not reg.get("ring")
        else:
            assert reg["zmin"] == 16 and reg.get("ring") == 1 and {f["key"].split("-")[0] for f in fill} == {"cudem",
                                                                                                          "cudem3"}
    hi = B.region_config("hawaii-lidar")
    first = hi["srcs"][0]
    assert first["key"] == "ngs-niihau21" and first["geoid"] == "egm2008"      # the newer Niihau survey wins
    assert first["geoid_path"] == S.GEOIDS["egm2008"] and first["offset"] == pytest.approx(-0.3)
    assert [s["key"] for s in B.region_config("pr-lidar")["srcs"] if not s["fill"]] == ["ngs-pr19", "usace-pr18"]
    assert B.region_config("nwhi-lidar")["srcs"][0]["geoid"] == "egm2008"
    for name in ("hawaii-lidar", "guam-lidar", "pr-lidar"):
        assert all(s.get("geoid") is None for s in B.region_config(name)["srcs"] if s["key"] != "ngs-niihau21")


def test_region_config_refuses_a_bad_lidar_region(monkeypatch):
    monkeypatch.setitem(S.SOURCES, "lo", dict(S.LIDAR, id=5, path="a.vrt", year="2099", name="x", credit="NOAA x",
                                             datum="t", nodata=-1.0))
    monkeypatch.setitem(S.SOURCES, "hi", dict(S.CUDEM, id=3, path="b.tif", year="2099"))
    base = dict(box=(0, 0, 1, 1), mhhw=0.4, zmin=16, prio=20, margin=32)
    monkeypatch.setitem(S.REGIONS, "bad", dict(base, sources=["lo"], fill=["hi"]))       # fill id below the lidar's
    with pytest.raises(ValueError):
        B.region_config("bad")
    monkeypatch.setitem(S.REGIONS, "bad", dict(base, sources=[], fill=["hi"]))          # fill only: nothing of its own
    with pytest.raises(ValueError, match="no source of its own"):
        B.region_config("bad")
    monkeypatch.setitem(S.SOURCES, "hi", dict(S.CUDEM, id=10, path="b.tif", year="2099"))
    for margin in (12, 4, 0):
        monkeypatch.setitem(S.REGIONS, "bad", dict(base, sources=["lo"], fill=["hi"], margin=margin))
        with pytest.raises(ValueError):
            B.region_config("bad")
    monkeypatch.setitem(S.REGIONS, "bad", dict(base, sources=["lo"], fill=["hi"], margin=16))
    reg = B.region_config("bad")
    assert reg["zmax"] == 17 and B.margin_of(reg) == 16                     # the fill's zmax 15 never sets the region's
    # a fill finer than the region's own sources still never sets its zmax
    monkeypatch.setitem(S.SOURCES, "lo", dict(S.LIDAR, id=5, path="a.vrt", year="2099", name="x", credit="NOAA x",
                                             datum="t", nodata=-1.0, zmax=14))
    monkeypatch.setitem(S.REGIONS, "bad", dict(base, sources=["lo"], fill=["hi"], zmin=8))
    assert B.region_config("bad")["zmax"] == 14


# ---------------------------------------------------------------- blocks: the lidar's footprint only, plus a ring

def test_fill_sources_never_drive_the_blocks_and_the_ring_grows_them():
    box = (-158.5, 21.0, -157.5, 21.8)
    own = (-158.10, 21.30, -158.05, 21.35)
    reg = dict(zmax=17, box=box, srcs=[dict(fill=False, rects=[own]), dict(fill=True, rects=[box])])
    bz = 17 - B.BLOCK_LEVELS
    x0, y0, x1, y1 = T.tiles_in_box(own, bz)
    lidar = {(x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)}
    assert set(B.block_keys(reg)) == lidar                     # the fill's footprint (the whole box) adds nothing
    reg["ring"] = 1
    grown = {(x + dx, y + dy) for (x, y) in lidar for dx in (-1, 0, 1) for dy in (-1, 0, 1)}
    assert set(B.block_keys(reg)) == grown
    # the ring stops at the box's edge
    reg["box"] = (own[0], own[1], own[2], own[3])
    bx0, by0, bx1, by1 = T.tiles_in_box(reg["box"], bz)
    assert set(B.block_keys(reg)) == {(x, y) for (x, y) in grown if bx0 <= x <= bx1 and by0 <= y <= by1}
    # no own footprint in the box: no blocks at all, ring or not
    reg["srcs"][0]["rects"] = [(10.0, 10.0, 10.1, 10.1)]
    assert B.block_keys(reg) == []


def test_vrt_rects_reads_projected_footprints():
    from rasterio.warp import transform
    vrt = ('<VRTDataset rasterXSize="1000" rasterYSize="1000"><SRS dataAxisToSRSAxisMapping="1,2">%s</SRS>'
           '<GeoTransform> 6.0e+05, 1.0, 0.0, 2.4e+06, 0.0, -1.0</GeoTransform><VRTRasterBand dataType="Float32" band="1">'
           '<ComplexSource><SourceFilename>a.tif</SourceFilename><DstRect xOff="100" yOff="200" xSize="500" '
           'ySize="300"/></ComplexSource></VRTRasterBand></VRTDataset>')
    (r,) = B.vrt_rects(vrt % "EPSG:32604")                                  # UTM zone 4 N
    lon, lat = transform("EPSG:32604", "EPSG:4326", [600100, 600600, 600100, 600600], [2399800, 2399800, 2399500,
                                                                                         2399500])
    assert r[0] < min(lon) and r[2] > max(lon) and r[1] < min(lat) and r[3] > max(lat)
    assert r[0] > min(lon) - 0.0015 and r[2] < max(lon) + 0.0015 and r[1] > min(lat) - 0.0015 and r[3] < max(lat) + 0.0015
    assert -158.1 < r[0] < -158.0 and 21.6 < r[1] < 21.8                     # off Oahu in degrees, not metres
    # geographic SRS (or none): the transform's units are degrees already, read as they are
    geo = vrt.replace("6.0e+05", "-158.0").replace("2.4e+06", "21.5").replace(", 1.0, 0.0", ", 0.0001, 0.0") \
        .replace("0.0, -1.0", "0.0, -0.0001")
    for srs in ("EPSG:4326", ""):
        (g,) = B.vrt_rects(geo % srs)
        assert g == pytest.approx((-157.99, 21.45, -157.94, 21.48))
    (g,) = B.vrt_rects(geo.replace('<SRS dataAxisToSRSAxisMapping="1,2">%s</SRS>', ""))
    assert g == pytest.approx((-157.99, 21.45, -157.94, 21.48))


def test_a_local_projected_footprint_is_in_lon_lat(tmp_path):
    path = str(tmp_path / "u.tif")
    with rasterio.open(path, "w", driver="GTiff", width=100, height=100, count=1, dtype="float32", crs="EPSG:32604",
                       transform=from_origin(600000, 2400000, 10, 10), nodata=-9999.0) as d:
        d.write(np.zeros((100, 100), np.float32), 1)
    (r,) = B.footprint(dict(local=path, gdal=path))
    assert -158.1 < r[0] < r[2] < -158.0 and 21.6 < r[1] < r[3] < 21.8


# ---------------------------------------------------------------- the blend at the lidar's edge

def test_feather_blends_inside_the_lidar_edge_only():
    n, px = 60, 12
    main = np.full((3, n), np.nan, np.float32)
    main[:, :30] = -10.0                                       # the lidar ends at column 30
    fill = np.full((3, n), -50.0, np.float32)
    main_id = np.where(np.isfinite(main), 2, 0).astype(np.uint8)
    fill_id = np.full((3, n), 10, np.uint8)
    elev, ids = B.feather(main, main_id, fill, fill_id, px=px)
    row, rid = elev[1], ids[1]
    # beyond the band: the lidar's own value and id, no flag
    assert np.all(row[:30 - px + 1] == -10.0) and np.all(rid[:30 - px + 1] == 2)
    # in the band: between the two, deeper towards the edge, flagged, the lidar's id kept
    band = row[30 - px + 1:30]
    assert np.all((band < -10.0) & (band > -50.0)) and np.all(np.diff(band) < 0)
    assert np.all(rid[30 - px + 1:30] == (2 | T.FLAG_BLENDED))
    assert band[-1] == pytest.approx(-10 * (1 / px) - 50 * (1 - 1 / px))   # one pixel from the fill: 1/px lidar
    # past the edge: the fill's own value and id
    assert np.all(row[30:] == -50.0) and np.all(rid[30:] == 10)
    # no fill-only pixel anywhere (a hole-free block inside the lidar): nothing changes
    e2, i2 = B.feather(main[:, :30], main_id[:, :30], fill[:, :30], fill_id[:, :30], px=px)
    assert np.array_equal(e2, main[:, :30]) and np.array_equal(i2, main_id[:, :30])
    # a lidar hole is blended round its rim too
    holed = np.full((41, 41), -10.0, np.float32)
    holed[20, 20] = np.nan
    e3, i3 = B.feather(holed, np.where(np.isfinite(holed), 2, 0).astype(np.uint8), np.full((41, 41), -50.0, np.float32),
                       np.full((41, 41), 10, np.uint8), px=4)
    assert e3[20, 20] == -50.0 and i3[20, 20] == 10 and i3[20, 21] == (2 | T.FLAG_BLENDED) and i3[20, 30] == 2
    assert e3[20, 21] == pytest.approx(-10 * 0.25 - 50 * 0.75)
    # a band pixel where the fill itself has no value keeps the lidar's value and id (nothing to blend towards)
    gap = fill.copy()
    gap[1, 25] = np.nan
    e4, i4 = B.feather(main, main_id, gap, fill_id, px=px)
    assert e4[1, 25] == -10.0 and i4[1, 25] == 2
    assert i4[1, 24] == (2 | T.FLAG_BLENDED) and i4[1, 26] == (2 | T.FLAG_BLENDED)


# ---------------------------------------------------------------- read_field and a whole build on synthetic surveys

W, E, SO, NO, RES = -158.25, -158.0, 21.20, 21.24, 0.0001     # open sea south-west of Oahu
NEW_E, OLD_E = -158.15, -158.10                                # the new survey west of NEW_E, the old west of OLD_E
GEOID_N = 15.0


def _tif(path, w, e, s, n, value, res=RES, crs="EPSG:4326"):
    width, height = int(round((e - w) / res)), int(round((n - s) / res))
    with rasterio.open(path, "w", driver="GTiff", width=width, height=height, count=1, dtype="float32", crs=crs,
                       transform=from_origin(w, n, res, res), nodata=-9999.0) as d:
        d.write(np.full((height, width), value, np.float32), 1)
    return path


def _surveys(tmp_path, monkeypatch, extra=None):
    """Region "lid": a new survey in ellipsoid heights (id 1, west of NEW_E: -5 m after the geoid and the -0.3 m step), an
    old one (id 2, west of OLD_E: -10 m), a 50-m-deep fill beneath the whole box (id 10, earns zoom 11 only); a constant
    geoid of 15 m. zmax 13 keeps the blocks small (zoom 10)."""
    local = {
        "lid-new": _tif(str(tmp_path / "new.tif"), -158.20, NEW_E, SO, NO, -5.0 + GEOID_N + 0.3),
        "lid-old": _tif(str(tmp_path / "old.tif"), -158.20, OLD_E, SO, NO, -10.0),
        "lid-fill": _tif(str(tmp_path / "fill.tif"), W, E, SO, NO, -50.0, res=0.0004),
        "geoid:egm2008": _tif(str(tmp_path / "geoid.tif"), -159.0, -157.0, 20.0, 22.0, GEOID_N, res=0.05),
    }
    common = dict(zmax=13, res_m=10, year="2099", datum="test", credit="NOAA test", licence="test", nodata=-9999.0)
    new = dict(S.LIDAR, **common)
    new.update(S.EGM2008)                                     # its datum and the -0.3 m step
    new.update(id=1, path="n.vrt", name="new")
    monkeypatch.setitem(S.SOURCES, "lid-new", new)
    monkeypatch.setitem(S.SOURCES, "lid-old", dict(S.LIDAR, **common, id=2, path="o.vrt", name="old"))
    monkeypatch.setitem(S.SOURCES, "lid-fill", dict(S.CUDEM, id=10, zmax=11, year="2099", path="f.tif", datum="test",
                                                    name="fill", credit="NOAA test", licence="test"))
    names = ["lid-new", "lid-old"] + (extra or [])
    monkeypatch.setitem(S.REGIONS, "lid", dict(box=(W, SO, E, NO), sources=names, fill=["lid-fill"], mhhw=0.4, zmin=12,
                                               prio=20, ring=1, margin=32))
    return local


def _column_lons(reg, key):
    m = B.margin_of(reg)
    px = T.TILE * 2 ** B.BLOCK_LEVELS
    size = px + 2 * m
    b = T.tile_bounds(reg["zmax"] - B.BLOCK_LEVELS, key[0], key[1], apron=m, px=px)
    xs = b[0] + (np.arange(size) + 0.5) * (b[2] - b[0]) / size
    ys = b[3] - (np.arange(size) + 0.5) * (b[3] - b[1]) / size
    return np.array([T.xy_to_lonlat(x, 0.0)[0] for x in xs]), np.array([T.xy_to_lonlat(0.0, y)[1] for y in ys])


def test_read_field_newer_survey_wins_geoid_and_blend(tmp_path, monkeypatch):
    local = _surveys(tmp_path, monkeypatch, extra=["lid-far"])
    # a fourth survey far away, whose file does not exist: its footprint misses the block, so it is never opened
    monkeypatch.setitem(S.SOURCES, "lid-far", dict(S.LIDAR, id=3, zmax=13, res_m=10, year="2099", path="missing.vrt",
                                                   name="far", credit="NOAA far", licence="t", datum="t", nodata=-1.0))
    B.gdal_env()
    reg = B.region_config("lid", local=local)
    for s in reg["srcs"]:
        s["rects"] = [(10.0, 10.0, 10.5, 10.5)] if s["key"] == "lid-far" else B.footprint(s)
    key = T.tile_of(-158.15, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src = B.read_field(reg, key)
    lons, lats = _column_lons(reg, key)
    j = int(np.argmin(np.abs(lats - 21.22)))
    row, rid = elev[j], src[j]
    blend = S.FEATHER_PX * (lons[1] - lons[0]) + 0.0005
    new = (lons > -158.20 + blend) & (lons < NEW_E - 0.001)
    old = (lons > NEW_E + 0.001) & (lons < OLD_E - blend)
    fill = (lons > OLD_E + 0.001) & (lons < E - 0.001)
    assert new.sum() > 20 and old.sum() > 20 and fill.sum() > 20
    assert np.allclose(row[new], -5.0, atol=1e-3) and np.all(rid[new] == 1)       # ellipsoid -> geoid -> -0.3 m
    assert np.allclose(row[old], -10.0, atol=1e-3) and np.all(rid[old] == 2)
    assert np.allclose(row[fill], -50.0, atol=1e-3) and np.all(rid[fill] == 10)
    # the lidar's east edge: blended, deeper towards the fill, flagged; the new/old seam is never blended (both own)
    edge = (lons > OLD_E - blend + 0.0008) & (lons < OLD_E - 0.0008)
    assert edge.sum() > 5 and np.all(rid[edge] == (2 | T.FLAG_BLENDED)) and np.all(np.diff(row[edge]) < 0)
    assert np.all((row[edge] < -10.0) & (row[edge] > -50.0))
    seam = (lons > NEW_E - 0.003) & (lons < NEW_E + 0.003)
    assert not np.any(rid[seam] & T.FLAG_BLENDED)


def test_a_lidar_build_writes_its_zooms_only_where_the_lidar_is(tmp_path, monkeypatch):
    local = _surveys(tmp_path, monkeypatch)
    B._core.cache_clear()
    index = B.build("lid", str(tmp_path / "out"), version=1, workers=1, local=local)
    B._core.cache_clear()
    out = os.path.join(str(tmp_path / "out"), "lid", "v1")
    assert set(index["tiles"]) <= {"12", "13"} and index["tiles"].get("13")      # never below the region's zmin
    assert set(index["sources"]) == {"1", "2", "10"}
    found = B.scan_tiles(out)
    seen_blend = seen_fill = False
    for z, tiles in found.items():
        for (x, y) in tiles:
            code, sf = T.unpack_tile(open(B.tile_path(out, z, x, y), "rb").read())
            ids = sf[1:-1, 1:-1] & T.SRC_BITS
            wet = code[1:-1, 1:-1] > 0
            assert (wet & np.isin(ids, (1, 2))).any(), (z, x, y)            # every tile holds lidar water
            seen_blend |= bool((sf & T.FLAG_BLENDED).any())
            seen_fill |= bool((wet & (ids == 10)).any())
            lon0, lat1 = T.xy_to_lonlat(*T.tile_bounds(z, x, y)[0::3])
            assert lon0 < OLD_E                                              # no tile wholly east of the lidar
    assert seen_blend and seen_fill
    # a pixel deep inside the new survey reads 5 m with its id, one inside the old 10 m
    for lon, depth, sid in ((-158.17, 5.0, 1), (-158.125, 10.0, 2)):
        x, y = T.tile_of(lon, 21.22, 13)
        code, sf = T.unpack_tile(open(B.tile_path(out, 13, x, y), "rb").read())
        x0, y0, x1, y1 = T.tile_bounds(13, x, y)
        p = (x1 - x0) / T.TILE
        i = int((T.lonlat_to_xy(lon, 21.22)[0] - x0) / p) + 1
        j = int((y1 - T.lonlat_to_xy(lon, 21.22)[1]) / p) + 1
        assert T.DECODE[code[j, i]] == pytest.approx(depth, abs=0.02) and sf[j, i] & T.SRC_BITS == sid
    assert B.check(out, log_fn=lambda *a: None) == 0
    assert "blends from the lidar" in open(os.path.join(out, "LICENSE.txt"), encoding="utf-8").read()
    # the check refuses a tile one zoom below the region's zmin (12), e.g. one a ring block wrote by mistake
    z, (x, y) = 13, sorted(found["13"] if "13" in found else found[13])[0]
    stray = B.tile_path(out, 11, x // 4, y // 4)
    os.makedirs(os.path.dirname(stray), exist_ok=True)
    with open(B.tile_path(out, z, x, y), "rb") as src, open(stray, "wb") as dst:
        dst.write(src.read())
    said = []
    assert B.check(out, log_fn=said.append) >= 1 and any("zoom 11 outside 12-13" in s for s in said)


# ---------------------------------------------------------------- G63a fix round: settings, guards and test gaps

def test_lidar_regions_share_their_cudem_regions_mhhw_and_hold_their_footprints():
    pairs = {"hawaii-lidar": "hawaii", "guam-lidar": "marianas", "pr-lidar": "prvi", "nwhi-lidar": "hawaii"}
    for lid, base in pairs.items():
        assert S.REGIONS[lid]["mhhw"] == S.REGIONS[base]["mhhw"], lid          # one water rule on both layers
    assert S.REGIONS["guam-lidar"]["box"][0] <= 144.43                         # the offshore banks west of 144.5 E
    pr = B.region_config("pr-lidar")
    assert {"cudem-usvi", "cudem3-usvi"} <= {s["key"] for s in pr["srcs"] if s["fill"]}
    assert all(s["fill"] for s in pr["srcs"] if "usvi" in s["key"])


def test_outside_box_flags_own_footprints_beyond_the_box_only():
    box = (-158.0, 21.0, -157.0, 22.0)
    pad = B.FOOT_PAD
    srcs = [dict(key="in", fill=False, rects=[(-157.9, 21.1, -157.1, 21.9)]),
            dict(key="padded", fill=False, rects=[(-158.0 - pad, 21.0 - pad, -157.0 + pad, 22.0 + pad)]),
            dict(key="fill", fill=True, rects=[(-170.0, 10.0, -150.0, 30.0)])]
    assert B.outside_box(box, srcs) == []                      # inside, or out by the footprint's own padding only
    for r in [(-158.01, 21.1, -157.5, 21.5), (-157.5, 20.99, -157.4, 21.5), (-157.5, 21.5, -156.99, 21.6),
              (-157.5, 21.5, -157.4, 22.01)]:
        out = B.outside_box(box, srcs + [dict(key="out", fill=False, rects=[r])])
        assert [k for k, _ in out] == ["out"], r


def test_a_build_refuses_a_footprint_outside_the_regions_box(tmp_path, monkeypatch):
    local = _surveys(tmp_path, monkeypatch)
    lid = dict(S.REGIONS["lid"], box=(-158.18, SO, E, NO))     # the new survey starts at -158.20: outside
    monkeypatch.setitem(S.REGIONS, "lid", lid)
    with pytest.raises(ValueError, match="outside the region's box"):
        B.build("lid", str(tmp_path / "out"), version=1, workers=1, local=local)
    # a narrower --box never hides the check (the configured box decides), and inside it the build goes ahead
    monkeypatch.setitem(S.REGIONS, "lid", dict(lid, box=(W, SO, E, NO)))
    B._core.cache_clear()
    B.build("lid", str(tmp_path / "out2"), version=1, workers=1, local=local, box=(-158.16, SO, E, NO))
    B._core.cache_clear()


def test_hits_needs_a_real_overlap():
    rects = [(0.0, 0.0, 1.0, 1.0)]
    assert B._hits(rects, (0.5, 0.5, 2.0, 2.0)) and B._hits(rects, (-1.0, -1.0, 0.1, 0.1))
    assert B._hits(rects, (0.2, 0.2, 0.3, 0.3)) and B._hits(rects, (-5.0, -5.0, 5.0, 5.0))
    for box in [(1.0, 0.0, 2.0, 1.0), (-1.0, 0.0, 0.0, 1.0), (0.0, 1.0, 1.0, 2.0), (0.0, -1.0, 1.0, 0.0),
                (2.0, 2.0, 3.0, 3.0)]:
        assert not B._hits(rects, box), box                    # touching edges or apart: no read
    assert B._hits([(5.0, 5.0, 6.0, 6.0), (0.0, 0.0, 1.0, 1.0)], (0.5, 0.5, 0.6, 0.6))    # any rectangle


def test_a_fill_id_must_be_above_every_own_id(monkeypatch):
    common = dict(year="2099", name="x", credit="NOAA x", datum="t", nodata=-1.0)
    monkeypatch.setitem(S.SOURCES, "a", dict(S.LIDAR, id=1, path="a.vrt", **common))
    monkeypatch.setitem(S.SOURCES, "b", dict(S.LIDAR, id=5, path="b.vrt", **common))
    monkeypatch.setitem(S.SOURCES, "f", dict(S.CUDEM, id=3, path="f.tif", year="2099"))
    monkeypatch.setitem(S.REGIONS, "mix", dict(box=(0, 0, 1, 1), mhhw=0.4, zmin=16, prio=20, margin=32,
                                               sources=["a", "b"], fill=["f"]))
    with pytest.raises(ValueError, match="above every own"):     # above the lowest own id, below the highest
        B.region_config("mix")
    monkeypatch.setitem(S.SOURCES, "f", dict(S.CUDEM, id=6, path="f.tif", year="2099"))
    assert [s["id"] for s in B.region_config("mix")["srcs"]] == [1, 5, 6]


def _tif_fn(path, w, e, s, n, fn, res=RES):
    width, height = int(round((e - w) / res)), int(round((n - s) / res))
    lon = w + (np.arange(width) + 0.5) * res
    lat = n - (np.arange(height) + 0.5) * res
    vals = fn(*np.meshgrid(lon, lat)).astype(np.float32)
    with rasterio.open(path, "w", driver="GTiff", width=width, height=height, count=1, dtype="float32",
                       crs="EPSG:4326", transform=from_origin(w, n, res, res), nodata=-9999.0) as d:
        d.write(vals, 1)
    return path


BX = -180.0 + 62 * 360.0 / 1024                               # a zoom-10 block edge: -158.203125


def test_the_blend_sees_a_lidar_edge_in_the_neighbouring_block(tmp_path, monkeypatch):
    """The lidar ends 10 px east of a block edge: the block to the west must still blend its last pixels (it sees the
    edge through its 32-px margin; with the default 8 px it would not, and the two blocks would disagree)."""
    local = _surveys(tmp_path, monkeypatch, extra=["lid-cross"])
    px13 = 360.0 / (2 ** 13 * T.TILE)
    edge = BX + 10 * px13
    local["lid-cross"] = _tif(str(tmp_path / "cross.tif"), -158.24, edge, SO, NO, -8.0)
    monkeypatch.setitem(S.SOURCES, "lid-cross", dict(S.LIDAR, id=3, zmax=13, res_m=10, year="2099", path="c.vrt",
                                                     name="cross", credit="NOAA c", licence="t", datum="t",
                                                     nodata=-9999.0))
    B.gdal_env()
    reg = B.region_config("lid", local=local)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    key = T.tile_of(-158.21, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    assert T.tile_bounds(10, *key)[2] == pytest.approx(T.lonlat_to_xy(BX, 21.22)[0])   # the block ends at BX
    elev, src = B.read_field(reg, key)
    lons, lats = _column_lons(reg, key)
    j = int(np.argmin(np.abs(lats - 21.22)))
    m = B.margin_of(reg)
    px = T.TILE * 2 ** B.BLOCK_LEVELS
    last = m + px - 1                                          # the core's last column, just west of BX
    for k in range(0, 10):                                     # 10.5-20 px from the lidar's edge: blended
        assert src[j, last - k] == (3 | T.FLAG_BLENDED), k
        assert -50.0 < elev[j, last - k] < -8.0
    for k in range(16, 30):                                    # 26+ px from it: the lidar's own value
        assert src[j, last - k] == 3 and elev[j, last - k] == pytest.approx(-8.0, abs=1e-3), k


def test_a_build_keeps_a_sloping_survey_in_place_and_a_lagoon_joined_across_a_block_edge(tmp_path, monkeypatch):
    """A survey deepening 1 m per 0.001 deg eastwards (a 24-px shift of the read core would move every value by 4 m),
    and a lagoon walled by land in one block whose only opening is a channel into the next block, beside a walled pond
    3-11 px west of the block edge (a read core shifted by 24 px would count the pond in the other block: the two
    passes' labels would differ and the build would stop)."""
    def slope(lon, lat):
        return -(5.0 + (lon + 158.25) * 1000.0)

    def land(lon, lat):
        v = np.full(lon.shape, 5.0)
        lagoon = (lon > -158.212) & (lon < -158.206) & (lat > 21.218) & (lat < 21.222)
        channel = (lon >= -158.2065) & (lat > 21.2195) & (lat < 21.2205)
        pond = (lon > -158.2050) & (lon < -158.2036) & (lat > 21.2236) & (lat < 21.2246)
        v[lagoon | channel | pond] = -3.0
        return v

    local = {
        "lag-land": _tif_fn(str(tmp_path / "land.tif"), -158.215, -158.195, 21.214, 21.226, land),
        "lag-slope": _tif_fn(str(tmp_path / "slope.tif"), -158.25, -158.16, 21.20, 21.24, slope),
        "lag-fill": _tif(str(tmp_path / "fill.tif"), -158.25, -158.15, 21.20, 21.24, -50.0, res=0.0004),
    }
    common = dict(zmax=13, res_m=10, year="2099", datum="test", credit="NOAA test", licence="test", nodata=-9999.0)
    monkeypatch.setitem(S.SOURCES, "lag-land", dict(S.LIDAR, **common, id=1, path="l.vrt", name="land"))
    monkeypatch.setitem(S.SOURCES, "lag-slope", dict(S.LIDAR, **common, id=2, path="s.vrt", name="slope"))
    monkeypatch.setitem(S.SOURCES, "lag-fill", dict(S.CUDEM, id=10, zmax=11, year="2099", path="f.tif", datum="test",
                                                    name="fill", credit="NOAA test", licence="test"))
    monkeypatch.setitem(S.REGIONS, "lag", dict(box=(-158.25, 21.20, -158.15, 21.24), sources=["lag-land", "lag-slope"],
                                               fill=["lag-fill"], mhhw=0.4, zmin=12, prio=20, ring=1, margin=32))
    B._core.cache_clear()
    B.build("lag", str(tmp_path / "out"), version=1, workers=1, local=local)
    B._core.cache_clear()
    out = os.path.join(str(tmp_path / "out"), "lag", "v1")

    def at(lon, lat):
        x, y = T.tile_of(lon, lat, 13)
        code, sf = T.unpack_tile(open(B.tile_path(out, 13, x, y), "rb").read())
        x0, y0, x1, y1 = T.tile_bounds(13, x, y)
        p = (x1 - x0) / T.TILE
        mx, my = T.lonlat_to_xy(lon, lat)
        i, j = int((mx - x0) / p) + 1, int((y1 - my) / p) + 1
        clon = T.xy_to_lonlat(x0 + (i - 0.5) * p, 0.0)[0]       # the pixel's own centre
        return code[j, i], sf[j, i], clon

    for lon in (-158.245, -158.24, -158.23, -158.18, -158.17):  # the sloping survey, both blocks: exactly in place
        code, sf, clon = at(lon, 21.205)
        assert code > 0 and sf & T.SRC_BITS == 2
        assert T.DECODE[code] == pytest.approx(5.0 + (clon + 158.25) * 1000.0, abs=0.03), lon
    assert BX - 0.012 < -158.209 < BX                           # the lagoon lies wholly in the western block
    code, sf, _ = at(-158.209, 21.220)
    assert code > 0 and sf == 1 and T.DECODE[code] == pytest.approx(3.0, abs=0.02)   # water: joined via the next block
    code, sf, _ = at(-158.200, 21.220)                          # the channel in the eastern block
    assert code > 0 and sf == 1
    code, sf, _ = at(-158.2135, 21.2155)                        # the wall: land
    assert code == 0 and sf == T.LAND_ID
    assert BX - 24 * 360.0 / (2 ** 13 * T.TILE) < -158.2050 and -158.2036 < BX   # the pond: within 24 px of BX
    code, sf, _ = at(-158.2043, 21.2241)                        # the walled pond: below sea level, cut off: land
    assert code == 0 and sf == T.LAND_ID


def test_the_geoid_is_subtracted_before_the_junk_filter_and_its_own_junk_is_dropped(tmp_path, monkeypatch):
    """A geoid grid with an undeclared fill value (-32767) north of 21.23 N: the ellipsoid survey becomes NaN there (the
    older survey beneath takes the pixel), never 32,777 m; a block whose raw values are all junk never reads the geoid."""
    local = _surveys(tmp_path, monkeypatch)
    local["geoid:egm2008"] = _tif_fn(str(tmp_path / "geoid2.tif"), -159.0, -157.0, 20.0, 22.0,
                                     lambda lon, lat: np.where(lat > 21.23, -32767.0, GEOID_N), res=0.005)
    B.gdal_env()
    reg = B.region_config("lid", local=local)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    key = T.tile_of(-158.17, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src = B.read_field(reg, key)
    lons, lats = _column_lons(reg, key)
    i = int(np.argmin(np.abs(lons - (-158.17))))
    for lat, depth, sid in ((21.21, -5.0, 1), (21.238, -10.0, 2)):
        j = int(np.argmin(np.abs(lats - lat)))
        assert src[j, i] == sid and elev[j, i] == pytest.approx(depth, abs=1e-3), lat
    assert np.nanmax(elev) < 1.0                                # no 32,777-m mountain anywhere
    # raw values that are all junk (not the declared nodata): skipped before the geoid is read
    local["lid-new"] = _tif(str(tmp_path / "junk.tif"), -158.20, NEW_E, SO, NO, 1.0e30)
    reg = B.region_config("lid", local=local)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    calls = []
    real = B._warp

    def counting(path, b, size, resampling, nodata=None, key="?"):
        calls.append(key)
        return real(path, b, size, resampling, nodata, key)

    monkeypatch.setattr(B, "_warp", counting)
    elev, src = B.read_field(reg, key)
    assert "geoid" not in calls and "lid-new" in calls and not (src == 1).any()
