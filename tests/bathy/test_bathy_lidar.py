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
        assert reg["zmax"] == 17 and reg["prio"] == 20, name
        # a region with fill reads 3 x FEATHER_PX around a block (the blend's reach); the NW Islands have no fill
        assert B.margin_of(reg) == (72 if fill else 32), name
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
    base = dict(box=(0, 0, 1, 1), mhhw=0.4, zmin=16, prio=20, margin=72)
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
    for margin in (16, 64):                                 # a multiple of 8, but short of the blend's 3 x 24 px reach
        monkeypatch.setitem(S.REGIONS, "bad", dict(base, sources=["lo"], fill=["hi"], margin=margin))
        with pytest.raises(ValueError, match="FEATHER_PX"):
            B.region_config("bad")
    monkeypatch.setitem(S.REGIONS, "nofill", dict(base, sources=["lo"], margin=16))    # no fill: no blend, no rule
    assert B.margin_of(B.region_config("nofill")) == 16
    # two surveys of its own and no fill: the older fades in under the newer, so the same 3 x 24 px reach is needed
    monkeypatch.setitem(S.SOURCES, "lo2", dict(S.LIDAR, id=6, path="c.vrt", year="2098", name="y",
                                              credit="NOAA y", datum="t", nodata=-1.0))
    for margin in (16, 64):
        monkeypatch.setitem(S.REGIONS, "two", dict(base, sources=["lo", "lo2"], margin=margin))
        with pytest.raises(ValueError, match="FEATHER_PX"):
            B.region_config("two")
    monkeypatch.setitem(S.REGIONS, "two", dict(base, sources=["lo", "lo2"], margin=72))
    assert B.margin_of(B.region_config("two")) == 72
    monkeypatch.setitem(S.REGIONS, "bad", dict(base, sources=["lo"], fill=["hi"], margin=72))
    reg = B.region_config("bad")
    assert reg["zmax"] == 17 and B.margin_of(reg) == 72                     # the fill's zmax 15 never sets the region's
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
    # a lidar hole is blended round its rim too, over the narrowest width (a one-pixel hole: FEATHER_MIN_PX)
    holed = np.full((41, 41), -10.0, np.float32)
    holed[20, 20] = np.nan
    e3, i3 = B.feather(holed, np.where(np.isfinite(holed), 2, 0).astype(np.uint8), np.full((41, 41), -50.0, np.float32),
                       np.full((41, 41), 10, np.uint8), px=4)
    assert S.FEATHER_MIN_PX == 2
    assert e3[20, 20] == -50.0 and i3[20, 20] == 10 and i3[20, 21] == (2 | T.FLAG_BLENDED) and i3[20, 30] == 2
    assert e3[20, 21] == pytest.approx(-10 * 0.5 - 50 * 0.5)
    assert i3[20, 22] == 2 and e3[20, 22] == -10.0                           # d = 2: not under the width 2
    assert i3[21, 21] == (2 | T.FLAG_BLENDED)                                # d = 1.41: blended

    # a band pixel where the fill itself has no value keeps the lidar's value and id (nothing to blend towards)
    gap = fill.copy()
    gap[1, 25] = np.nan
    e4, i4 = B.feather(main, main_id, gap, fill_id, px=px)
    assert e4[1, 25] == -10.0 and i4[1, 25] == 2
    assert i4[1, 24] == (2 | T.FLAG_BLENDED) and i4[1, 26] == (2 | T.FLAG_BLENDED)


def _holes_field(n=360, seed=7):
    """A lidar field with gaps of every size: one-pixel specks, small discs, a big disc, a long narrow channel, the
    survey's outer edge on the east, and a hole beside the edge."""
    rng = np.random.default_rng(seed)
    yy, xx = np.mgrid[0:n, 0:n]
    main = (-5.0 - 0.02 * xx - 0.01 * yy).astype(np.float32)
    gap = np.zeros((n, n), bool)
    gap[:, n - 40:] = True                                         # the survey ends 40 px from the east side
    for cy, cx, r in [(60, 60, 3), (60, 140, 6), (200, 120, 40), (300, 300, 10), (120, 300, 2.5)]:
        gap |= (yy - cy) ** 2 + (xx - cx) ** 2 <= r * r
    gap[150:154, 20:330] = True                                    # a channel 4 px wide, joined to the outer edge
    for _ in range(40):
        gap[rng.integers(0, n), rng.integers(0, n - 40)] = True    # one-pixel specks
    main[gap] = np.nan
    fill = np.full((n, n), -50.0, np.float32) + (xx * 0.03).astype(np.float32)
    main_id = np.where(np.isfinite(main), 2, 0).astype(np.uint8)
    return main, main_id, fill, np.full((n, n), 10, np.uint8), gap


def test_feather_fades_over_the_gap_size_small_gaps_narrow_big_gaps_and_the_edge_full():
    """Owner's option C: the fade round a gap is about as wide as the gap is deep, never under FEATHER_MIN_PX, never
    over FEATHER_PX; the survey's outer edge and a big hole keep the full width."""
    main, main_id, fill, fill_id, gap = _holes_field()
    px = 24
    elev, ids = B.feather(main, main_id, fill, fill_id, px=px)
    blended = (ids & T.FLAG_BLENDED) != 0
    from scipy import ndimage
    d = ndimage.distance_transform_edt(~gap)

    def reach(mask):                                               # how far from the gap the blend reaches round it
        ring = ndimage.binary_dilation(mask, iterations=px + 2) & ~gap
        return d[ring & blended].max() if (ring & blended).any() else 0.0

    disc = np.zeros_like(gap)
    yy, xx = np.mgrid[0:gap.shape[0], 0:gap.shape[1]]
    disc[(yy - 60) ** 2 + (xx - 60) ** 2 <= 9] = True               # radius 3: a fade about 3 px
    assert 2.0 <= reach(disc) < 3.2                               # its depth: sqrt(10), the nearest lidar pixel
    disc[:] = False
    disc[(yy - 200) ** 2 + (xx - 120) ** 2 <= 1600] = True          # radius 40: the full 24
    assert px - 1 <= reach(disc) < px
    edge = np.zeros_like(gap)
    edge[:, -40:] = True
    assert px - 1 <= reach(edge & ~ndimage.binary_dilation(disc, iterations=60)) < px
    # never wider than px anywhere, never at a pixel whose fill has no value, the lidar's own id kept
    assert d[blended].max() < px and np.all(ids[blended] == (2 | T.FLAG_BLENDED))
    # every blended depth lies between the lidar's and the fill's, the rest is the lidar's own or the fill's own
    lo, hi = np.minimum(main, fill), np.maximum(main, fill)
    assert np.all((elev[blended] >= lo[blended] - 1e-4) & (elev[blended] <= hi[blended] + 1e-4))
    assert np.array_equal(elev[~blended & ~gap], main[~blended & ~gap])
    assert np.array_equal(elev[gap], fill[gap])
    # fewer lidar pixels blended than with a full-width band round every gap (what the old rule did)
    assert blended.sum() < ((d < px) & ~gap).sum() * 0.9


def test_feather_gives_every_block_the_same_values_whatever_its_read_area():
    """The width is measured within 3 x px of a pixel, so a block read with a margin of 3 x px gives exactly the values
    of the whole field (a hole cut by the read area's edge never gets a different fade: no seam at block edges)."""
    main, main_id, fill, fill_id, gap = _holes_field()
    px = 24
    whole_e, whole_i = B.feather(main, main_id, fill, fill_id, px=px)
    n, m, core = main.shape[0], 3 * px, 64
    checked = 0
    for y0 in range(m, n - m - core + 1, 37):
        for x0 in range(m, n - m - core + 1, 41):
            sl = (slice(y0 - m, y0 + core + m), slice(x0 - m, x0 + core + m))
            e, i = B.feather(main[sl], main_id[sl], fill[sl], fill_id[sl], px=px)
            c = (slice(m, m + core), slice(m, m + core))
            w = (slice(y0, y0 + core), slice(x0, x0 + core))
            assert np.array_equal(e[c], whole_e[w]) and np.array_equal(i[c], whole_i[w]), (y0, x0)
            checked += 1
    assert checked >= 16
    # with a margin of only px (what a 32-px margin would have been for px 24) some block disagrees: the rule needs 3 x
    bad = 0
    for y0 in range(px, n - px - core + 1, 13):
        sl = (slice(y0 - px, y0 + core + px), slice(150 - px, 150 + core + px))
        e, _ = B.feather(main[sl], main_id[sl], fill[sl], fill_id[sl], px=px)
        bad += not np.array_equal(e[px:px + core, px:px + core], whole_e[y0:y0 + core, 150:150 + core])
    assert bad > 0


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
                                               prio=20, ring=1, margin=72))
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
    new = (lons > -158.20 + blend) & (lons < NEW_E - blend)
    old = (lons > NEW_E + 0.001) & (lons < OLD_E - blend)
    fill = (lons > OLD_E + 0.001) & (lons < E - 0.001)
    assert new.sum() > 20 and old.sum() > 20 and fill.sum() > 20
    assert np.allclose(row[new], -5.0, atol=1e-3) and np.all(rid[new] == 1)       # ellipsoid -> geoid -> -0.3 m
    assert np.allclose(row[old], -10.0, atol=1e-3) and np.all(rid[old] == 2)
    assert np.allclose(row[fill], -50.0, atol=1e-3) and np.all(rid[fill] == 10)
    # the lidar's east edge: blended, deeper towards the fill, flagged
    edge = (lons > OLD_E - blend + 0.0008) & (lons < OLD_E - 0.0008)
    assert edge.sum() > 5 and np.all(rid[edge] == (2 | T.FLAG_BLENDED)) and np.all(np.diff(row[edge]) < 0)
    assert np.all((row[edge] < -10.0) & (row[edge] > -50.0))
    # where the new survey ends the old one fades in under it (owner, 2026-11-14): the new id flagged, between the two
    seam = (lons > NEW_E - blend + 0.0008) & (lons < NEW_E - 0.0008)
    assert seam.sum() > 5 and np.all(rid[seam] == (1 | T.FLAG_BLENDED)) and np.all(np.diff(row[seam]) < 0)
    assert np.all((row[seam] < -5.0) & (row[seam] > -10.0))
    beyond = (lons > NEW_E + 0.0008) & (lons < NEW_E + 0.01)                  # the old survey's own side: as it was
    assert np.all(rid[beyond] == 2) and np.allclose(row[beyond], -10.0, atol=1e-3)


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
    monkeypatch.setitem(S.REGIONS, "mix", dict(box=(0, 0, 1, 1), mhhw=0.4, zmin=16, prio=20, margin=72,
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
    edge through its 72-px margin; with the default 8 px it would not, and the two blocks would disagree). The other
    surveys start well east of it, so the gap beyond the edge is wide and its fade the full FEATHER_PX."""
    local = _surveys(tmp_path, monkeypatch, extra=["lid-cross"])
    px13 = 360.0 / (2 ** 13 * T.TILE)
    edge = BX + 10 * px13
    local["lid-cross"] = _tif(str(tmp_path / "cross.tif"), -158.24, edge, SO, NO, -8.0)
    local["lid-new"] = _tif(str(tmp_path / "new2.tif"), -158.17, NEW_E, SO, NO, -5.0 + GEOID_N + 0.3)
    local["lid-old"] = _tif(str(tmp_path / "old2.tif"), -158.17, OLD_E, SO, NO, -10.0)
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
        # the land survey reaches well beyond the wall, the lagoon and the pond: the older survey fades in at its edge
        # (owner, 2026-11-14), over up to 24 px, and must not reach them
        "lag-land": _tif_fn(str(tmp_path / "land.tif"), -158.225, -158.185, 21.208, 21.232, land),
        "lag-slope": _tif_fn(str(tmp_path / "slope.tif"), -158.25, -158.16, 21.20, 21.24, slope),
        "lag-fill": _tif(str(tmp_path / "fill.tif"), -158.25, -158.15, 21.20, 21.24, -50.0, res=0.0004),
    }
    common = dict(zmax=13, res_m=10, year="2099", datum="test", credit="NOAA test", licence="test", nodata=-9999.0)
    monkeypatch.setitem(S.SOURCES, "lag-land", dict(S.LIDAR, **common, id=1, path="l.vrt", name="land"))
    monkeypatch.setitem(S.SOURCES, "lag-slope", dict(S.LIDAR, **common, id=2, path="s.vrt", name="slope"))
    monkeypatch.setitem(S.SOURCES, "lag-fill", dict(S.CUDEM, id=10, zmax=11, year="2099", path="f.tif", datum="test",
                                                    name="fill", credit="NOAA test", licence="test"))
    monkeypatch.setitem(S.REGIONS, "lag", dict(box=(-158.25, 21.20, -158.15, 21.24), sources=["lag-land", "lag-slope"],
                                               fill=["lag-fill"], mhhw=0.4, zmin=12, prio=20, ring=1, margin=72))
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


# ---------------------------------------------------------------- where two lidar surveys meet (owner, 2026-11-14)

def _row(reg, key, lat=21.22):
    elev, src = B.read_field(reg, key)
    lons, lats = _column_lons(reg, key)
    j = int(np.argmin(np.abs(lats - lat)))
    return elev, src, lons, lats, j


def _run(mask, start, step):
    """How many pixels from `start` (inclusive) in direction `step` are True before the first False."""
    n, i = 0, start
    while 0 <= i < len(mask) and mask[i]:
        n, i = n + 1, i + step
    return n


def test_an_older_survey_fades_in_under_the_newer_by_the_gap_size(tmp_path, monkeypatch):
    """The newer survey (-5 m) has a small hole the older one (-10 m) fills: round the hole the fade is a few pixels wide
    (the hole's own size); where the newer survey ends, with a wide stretch of the older one beyond, the full FEATHER_PX.
    Every faded pixel keeps the newer survey's id with the blended flag and lies between the two surveys."""
    local = _surveys(tmp_path, monkeypatch)
    hole = (-158.1805, 21.2195, -158.1795, 21.2205)             # about 6 x 6 pixels at zoom 13

    def new_fn(lon, lat):
        v = np.full(lon.shape, -5.0 + GEOID_N + 0.3)
        v[(lon > hole[0]) & (lon < hole[2]) & (lat > hole[1]) & (lat < hole[3])] = -9999.0
        return v

    local["lid-new"] = _tif_fn(str(tmp_path / "newhole.tif"), -158.20, NEW_E, SO, NO, new_fn)
    B.gdal_env()
    reg = B.region_config("lid", local=local)
    # the build's LICENSE.txt says so (two lidar surveys in the region)
    assert "Where two lidar surveys meet, the older one blends in under the newer one" in B.licence_text(reg)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    key = T.tile_of(-158.17, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src, lons, lats, j = _row(reg, key)
    row, rid = elev[j], src[j]
    blended = (rid & T.FLAG_BLENDED) > 0
    # the hole: the old survey's own value and id
    inside = np.where((lons > hole[0] + 0.0002) & (lons < hole[2] - 0.0002))[0]
    assert len(inside) >= 2 and np.all(rid[inside] == 2) and np.allclose(row[inside], -10.0, atol=1e-3)
    # round the hole: a narrow fade (FEATHER_MIN_PX .. a few px), the newer survey's own value beyond it
    holecols = np.where((rid & T.SRC_BITS) == 2)[0]
    holecols = holecols[(lons[holecols] > hole[0] - 0.001) & (lons[holecols] < hole[2] + 0.001)]
    west, east = _run(blended, holecols.min() - 1, -1), _run(blended, holecols.max() + 1, 1)
    assert S.FEATHER_MIN_PX <= west <= 4 and S.FEATHER_MIN_PX <= east <= 4, (west, east)
    for i in (holecols.min() - west - 2, holecols.max() + east + 2):
        assert rid[i] == 1 and row[i] == pytest.approx(-5.0, abs=1e-3)
    # where the newer survey ends (NEW_E), with the old survey beyond: the full width
    first_old = int(np.where((lons > NEW_E) & ((rid & T.SRC_BITS) == 2))[0].min())
    edge = _run(blended, first_old - 1, -1)
    assert S.FEATHER_PX - 4 <= edge <= S.FEATHER_PX, edge
    # (the newer survey's west edge at -158.20 fades into the fill beneath, not into the older survey: left out here)
    faded = blended & (lons > hole[2] + 0.001) & (lons < NEW_E)
    assert np.all(rid[faded] == (1 | T.FLAG_BLENDED))
    assert np.all((row[faded] < -5.0 + 1e-4) & (row[faded] > -10.0 - 1e-4))


def test_surveys_that_never_overlap_are_merged_as_before(tmp_path, monkeypatch):
    """Two surveys of one region that do not overlap (different islands): no fade between them; feather runs once per
    block, for the fill beneath. A region with one survey likewise."""
    local = _surveys(tmp_path, monkeypatch)
    local["lid-old"] = _tif(str(tmp_path / "old_apart.tif"), NEW_E + 0.01, OLD_E, SO, NO, -10.0)
    B.gdal_env()
    reg = B.region_config("lid", local=local)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    calls = []
    real = B.feather

    def counting(main, main_id, fill, fill_id, px=None):
        calls.append(sorted(set(np.unique(fill_id[np.isfinite(fill)]).tolist())))
        return real(main, main_id, fill, fill_id, px)

    monkeypatch.setattr(B, "feather", counting)
    key = T.tile_of(-158.17, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src, lons, lats, j = _row(reg, key)
    assert calls == [[10]], calls                              # only the fill's
    row, rid = elev[j], src[j]
    near = (lons > NEW_E - 0.003) & (lons < NEW_E - 0.0002)     # the newer survey beside the gap: fades to the FILL
    assert np.all((rid[near] & T.SRC_BITS) == 1)
    assert np.all(row[near] <= -5.0 + 1e-4) and np.all(row[near] > -50.0)
    # one survey only: the same single call
    monkeypatch.setitem(S.REGIONS, "lid", dict(S.REGIONS["lid"], sources=["lid-new"]))
    reg = B.region_config("lid", local=local)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    calls.clear()
    B.read_field(reg, key)
    assert calls == [[10]], calls


def test_the_survey_fade_sees_an_edge_in_the_neighbouring_block(tmp_path, monkeypatch):
    """The newer survey ends 10 px east of a block edge and the older one covers both blocks: the block to the west must
    fade its last pixels toward the older survey (it sees the newer survey's edge through its 72-px margin)."""
    local = _surveys(tmp_path, monkeypatch)
    px13 = 360.0 / (2 ** 13 * T.TILE)
    edge = BX + 10 * px13
    local["lid-new"] = _tif(str(tmp_path / "new3.tif"), -158.24, edge, SO, NO, -5.0 + GEOID_N + 0.3)
    local["lid-old"] = _tif(str(tmp_path / "old3.tif"), -158.24, OLD_E, SO, NO, -10.0)
    B.gdal_env()
    reg = B.region_config("lid", local=local)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    key = T.tile_of(-158.21, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    assert T.tile_bounds(10, *key)[2] == pytest.approx(T.lonlat_to_xy(BX, 21.22)[0])
    elev, src, lons, lats, j = _row(reg, key)
    m = B.margin_of(reg)
    last = m + T.TILE * 2 ** B.BLOCK_LEVELS - 1
    for k in range(0, 10):                                     # 10.5-20 px from the newer survey's edge: faded
        assert src[j, last - k] == (1 | T.FLAG_BLENDED), k
        assert -10.0 < elev[j, last - k] < -5.0
    for k in range(16, 30):                                    # 26+ px from it: the newer survey's own value
        assert src[j, last - k] == 1 and elev[j, last - k] == pytest.approx(-5.0, abs=1e-3), k
    # the same pixels read as part of the east block (its own margin) give the same values
    east = (key[0] + 1, key[1])
    elev2, src2, lons2, lats2, j2 = _row(reg, east)
    for k in range(0, 10):
        i2 = m - 1 - k                                          # the east block's margin column for the same pixel
        assert lons2[i2] == pytest.approx(lons[last - k], abs=1e-9)
        assert src2[j2, i2] == src[j, last - k] and elev2[j2, i2] == pytest.approx(elev[j, last - k], abs=1e-4), k


def test_unmarked_surveys_keep_the_plain_merge_and_the_small_margin(tmp_path, monkeypatch):
    """The fade between surveys is for lidar surveys only (survey_fade): the stage-1 regions' two CUDEM grids, both
    their own sources, keep the first-valid merge and the 8-px margin, so their published builds do not change."""
    local = _surveys(tmp_path, monkeypatch)
    for sid in ("lid-new", "lid-old"):
        monkeypatch.setitem(S.SOURCES, sid, dict(S.SOURCES[sid], survey_fade=False))
    monkeypatch.setitem(S.REGIONS, "lid", dict(S.REGIONS["lid"], fill=[], margin=8))
    B.gdal_env()
    reg = B.region_config("lid", local=local)                  # two own sources, no fill: 8 px allowed
    assert B.margin_of(reg) == 8
    assert not any("blends in under the newer" in line for line in B.licence_text(reg).splitlines())
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    key = T.tile_of(-158.17, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src, lons, lats, j = _row(reg, key)
    row, rid = elev[j], src[j]
    new = (lons > -158.195) & (lons < NEW_E - 0.0002)
    old = (lons > NEW_E + 0.0002) & (lons < OLD_E - 0.0002)
    assert np.all(rid[new] == 1) and np.allclose(row[new], -5.0, atol=1e-3)     # no fade at the hand-over
    assert np.all(rid[old] == 2) and np.allclose(row[old], -10.0, atol=1e-3)
    # the same region with lidar surveys refuses that margin
    for sid in ("lid-new", "lid-old"):
        monkeypatch.setitem(S.SOURCES, sid, dict(S.SOURCES[sid], survey_fade=True))
    with pytest.raises(ValueError, match="FEATHER_PX"):
        B.region_config("lid", local=local)


def test_every_lidar_source_is_marked_and_no_cudem_grid_is():
    """The owner's rule applies to the lidar surveys only: the LIDAR template carries the mark, the CUDEM ones do not."""
    for sid, s in S.SOURCES.items():
        if s.get("zmax") == 17:
            assert s.get("survey_fade") is True, sid
        else:
            assert not s.get("survey_fade"), sid


def test_the_survey_fade_never_turns_the_newer_surveys_water_into_land():
    """Owner, 2026-11-14: where the older survey is land, the fade may make the newer survey's water shallower but never
    lifts it above mean sea level (or above its own value on a reef flat between 0 and mhhw)."""
    n = 120
    newer = np.full((n, n), np.nan, np.float32)
    newer[:, :60] = -1.5                     # shallow water ending at column 60
    newer[:10, :60] = 0.2                    # a reef flat (water: below mhhw 0.4)
    newer[20:24, 30:34] = np.nan             # a small hole the older survey fills with land
    older = np.full((n, n), 5.0, np.float32)  # land everywhere
    ids = np.where(np.isfinite(newer), 1, 0).astype(np.uint8)
    e, i = B.survey_fade(newer, ids, older, 2, 0.4)
    plain, _ = B.feather(newer, ids, older, np.full(older.shape, 2, np.uint8))
    water = np.isfinite(newer)
    assert (plain[water] > 0.4).any()                       # without the cap the fade makes land of it
    assert np.all(e[water] <= np.maximum(newer[water], 0.0))
    assert np.all(e[water] <= 0.4)                          # every newer water pixel stays water
    assert e[0, 59] == pytest.approx(0.2) and e[30, 59] == pytest.approx(0.0)
    assert np.all(e[~water] == 5.0) and np.all(i[~water] == 2)
    assert np.all((i[water] & T.SRC_BITS) == 1)
    # deeper than the older survey: the fade deepens, the cap never touches it
    deep = np.full((n, n), -20.0, np.float32)
    e2, _ = B.survey_fade(newer, ids, deep, 2, 0.4)
    p2, _ = B.feather(newer, ids, deep, np.full(deep.shape, 2, np.uint8))
    assert np.array_equal(e2, p2)
    # newer survey land (above mhhw) is never capped
    newer2 = np.where(water, np.float32(1.0), np.nan).astype(np.float32)
    e3, _ = B.survey_fade(newer2, ids, older, 2, 0.4)
    p3, _ = B.feather(newer2, ids, older, np.full(older.shape, 2, np.uint8))
    assert np.array_equal(e3, p3)
    # a region whose water line is below 0: capped at the water line
    e4, _ = B.survey_fade(newer, ids, older, 2, -0.2)
    wet = water & (newer <= -0.2)
    assert np.all(e4[wet] <= -0.2)


def test_read_field_keeps_the_newer_surveys_water_where_the_older_one_is_land(tmp_path, monkeypatch):
    """The cap through read_field: the newer survey (-1 m) ends at NEW_E, the older one is land (+5 m) beyond it."""
    local = _surveys(tmp_path, monkeypatch)
    local["lid-new"] = _tif(str(tmp_path / "new_shallow.tif"), -158.20, NEW_E, SO, NO, -1.0 + GEOID_N + 0.3)
    local["lid-old"] = _tif(str(tmp_path / "old_land.tif"), -158.20, OLD_E, SO, NO, 5.0)
    B.gdal_env()
    reg = B.region_config("lid", local=local)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    key = T.tile_of(-158.17, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src, lons, lats, j = _row(reg, key)
    row, rid = elev[j], src[j]
    inner = (lons > -158.194) & (lons < OLD_E - 0.006)          # clear of the fill's fades at the box edge and OLD_E
    newer = inner & ((rid & T.SRC_BITS) == 1)
    assert newer.sum() > 100
    assert np.all(row[newer] <= 0.0 + 1e-6) and np.all(row[newer] >= -1.0 - 1e-4)
    faded = newer & ((rid & T.FLAG_BLENDED) > 0)
    assert faded.sum() >= S.FEATHER_PX - 4 and np.all(row[faded] > -1.0 + 1e-4)   # it does fade, up to sea level
    assert row[faded].max() == pytest.approx(0.0, abs=1e-6)     # held at sea level (unheld it would reach about +4.8 m)
    older = inner & ((rid & T.SRC_BITS) == 2)
    assert older.sum() > 100 and np.all(row[older] == pytest.approx(5.0))


def test_three_surveys_each_older_one_fades_in_under_all_newer(tmp_path, monkeypatch):
    """A third, still older survey (id 3, -20 m, east to -158.05) fades in where the merged first two end (OLD_E), the
    way the second fades in where the first ends (NEW_E)."""
    local = _surveys(tmp_path, monkeypatch, extra=["lid-oldest"])
    local["lid-oldest"] = _tif(str(tmp_path / "oldest.tif"), -158.20, -158.05, SO, NO, -20.0)
    monkeypatch.setitem(S.SOURCES, "lid-oldest", dict(S.LIDAR, zmax=13, res_m=10, year="2097", datum="test",
                                                      credit="NOAA z", licence="test", nodata=-9999.0, id=3,
                                                      path="z.vrt", name="z"))
    B.gdal_env()
    reg = B.region_config("lid", local=local)
    for s in reg["srcs"]:
        s["rects"] = B.footprint(s)
    key = T.tile_of(-158.12, 21.22, reg["zmax"] - B.BLOCK_LEVELS)
    elev, src, lons, lats, j = _row(reg, key)
    row, rid = elev[j], src[j]
    first3 = int(np.where((lons > OLD_E) & ((rid & T.SRC_BITS) == 3))[0].min())
    edge = _run((rid & T.FLAG_BLENDED) > 0, first3 - 1, -1)
    assert S.FEATHER_PX - 4 <= edge <= S.FEATHER_PX, edge
    faded = ((rid & T.FLAG_BLENDED) > 0) & (lons > NEW_E + 0.0008) & (lons < OLD_E)
    assert faded.sum() > 5 and np.all(rid[faded] == (2 | T.FLAG_BLENDED))
    assert np.all((row[faded] < -10.0 + 1e-4) & (row[faded] > -20.0 - 1e-4))
    beyond = (lons > OLD_E + 0.0008) & (lons < -158.056)       # clear of its own fade into the fill at -158.05
    assert beyond.sum() > 100
    assert np.all(rid[beyond] == 3) and np.allclose(row[beyond], -20.0, atol=1e-3)
    assert "Where two lidar surveys meet" in B.licence_text(reg)


def test_the_survey_sentence_only_with_two_marked_surveys(tmp_path, monkeypatch):
    """The licence sentence appears only for two or more marked own surveys: not for one survey with fill (Guam), not for
    a region of unmarked CUDEM grids."""
    local = _surveys(tmp_path, monkeypatch)
    sentence = "Where two lidar surveys meet"
    text = B.licence_text(B.region_config("lid", local=local))
    assert sentence in text and "water never becomes land" in text and "within 24 pixels" in text
    monkeypatch.setitem(S.REGIONS, "lid", dict(S.REGIONS["lid"], sources=["lid-new"]))
    assert sentence not in B.licence_text(B.region_config("lid", local=local))
    assert sentence not in B.licence_text(B.region_config("hawaii"))
    assert sentence in B.licence_text(B.region_config("pr-lidar"))
    assert sentence not in B.licence_text(B.region_config("guam-lidar"))
