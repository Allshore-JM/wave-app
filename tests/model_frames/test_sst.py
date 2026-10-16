"""Sea surface temperature tiles job (plan section 44): offline tests (no network, no R2; h5py only for read_mur).

The tiling code runs unchanged on a coarser lattice (Geo(px=20): 0.05-deg pixels) so a whole synthetic world fits in a
test: a smooth periodic temperature field, an island whose coastal ring carries MUR-style values (used) and whose
interior carries far ones (ignored), a lake inside it and an ice cap near the pole. The real MUR day (2026-10-14) was
built and read back separately (plan section 44 progress).
"""
import io
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import publish as P        # noqa: E402
import sst as S            # noqa: E402
import sst_tiles as T      # noqa: E402
from test_job import FakeClient   # noqa: E402

PXT = 20                    # test lattice: 20 pixels per degree


def field_c(lat, lon):
    """A smooth, periodic field in deg C (no jump across the date line)."""
    return 15.0 + 0.1 * lat + 2.0 * np.sin(np.radians(lon)) + 0.5 * np.cos(np.radians(3 * lon))


def raw_of(c):
    return np.rint((np.asarray(c) - 25.0) * 1000.0).astype(np.int16)


def world(px=PXT):
    """(sst raw, mask) north-up on Geo(px): sea everywhere, an island at 10..12 N, 20..22 E (land, mask 2) with MUR-like
    values on its outer ring (1 px, to be used) and in its middle (far from the sea, to be ignored), a lake inside it
    (mask 5), ice (mask 9) north of 80 N."""
    g = T.Geo(px)
    lat_h = 90 * px - 1 - np.arange(g.rows)
    lon_h = -180 * px + 1 + np.arange(g.cols)
    lat = (lat_h / px)[:, None]
    lon = (lon_h / px)[None, :]
    sst = raw_of(field_c(lat, lon) + 0 * lon)
    mask = np.full(sst.shape, T.MASK_SEA, dtype=np.int8)
    rows = (lat[:, 0] >= 10) & (lat[:, 0] < 12)
    cols = (lon[0] >= 20) & (lon[0] < 22)
    isl = rows[:, None] & cols[None, :]
    mask[isl] = T.MASK_LAND
    sst[isl] = T.NONE
    r = np.nonzero(rows)[0]
    c = np.nonzero(cols)[0]
    ring = np.zeros_like(isl)
    ring[r[0], c] = ring[r[-1], c] = True
    ring[r, c[0]] = ring[r, c[-1]] = True
    sst[ring] = raw_of(field_c(lat, lon) + 0 * lon)[ring]                       # coastal values (used)
    mid = (r[len(r) // 2], c[len(c) // 2])
    sst[mid] = raw_of(30.0)                                                     # a far inland value (ignored)
    sst[r[10], c[2]] = raw_of(29.0)                                             # 3 px from the sea: used (COAST_PX)
    sst[r[12], c[3]] = raw_of(29.5)                                             # 4 px from the sea: not used
    mask[r[len(r) // 2 - 3]:r[len(r) // 2 - 1], c[len(c) // 2 + 3]:c[len(c) // 2 + 6]] = 5   # a lake
    ice = lat[:, 0] > 80
    mask[ice] = 9
    return sst, mask, isl, mid


def valued_world(px=PXT):
    sst, mask, isl, mid = world(px)
    counts = T.value_grid(sst, mask)
    return sst, mask, isl, mid, counts


# ------------------------------------------------------------------ lattice
def test_geo_maps_rows_and_columns_on_the_mur_lattice():
    g = T.Geo()
    assert (g.rows, g.cols) == (17999, 36000)
    assert list(g.row([8999, 0, -8999, 9000, -9000])) == [0, 8999, 17998, -1, -1]
    assert list(g.col([-17999, 0, 18000, -18000, 18001])) == [0, 17999, 35999, 35999, 0]   # 180 E = -180 E; periodic
    assert g.res(1) == 0.01 and g.res(5) == 0.05 and g.res(25) == 0.25


def test_celsius_from_raw_is_exact():
    assert T.to_c(np.int16(0)) == 25.0
    assert T.to_c(np.int16(-26800)) == pytest.approx(-1.8)     # MUR's freezing floor
    assert T.to_c(raw_of(27.345)) == pytest.approx(27.345)


# ------------------------------------------------------------------ which pixels carry a value
def test_value_grid_keeps_sea_and_coastal_land_values_only():
    sst, mask, isl, mid, counts = valued_world()
    g = T.Geo(PXT)
    assert counts["coast_land"] > 0 and counts["lake"] > 0 and counts["ice"] > 0
    assert counts["lake"] == int(((mask & 4) != 0).sum()) and counts["ice"] == int((mask == 9).sum()), \
        "lake ice (13) counted once, as lake (G31a F9)"
    assert sst[mid] == T.FILLABLE, "a land value far from the sea is not used"
    r = np.nonzero(isl.any(axis=1))[0]
    c = np.nonzero(isl.any(axis=0))[0]
    assert sst[r[10], c[2]] == raw_of(29.0), "MUR's land value 3 px from the sea is used"
    assert sst[r[12], c[3]] == T.FILLABLE, "4 px from the sea it is not"
    ring_r = np.nonzero(isl.any(axis=1))[0][0]
    ring_c = np.nonzero(isl[ring_r])[0][3]
    assert sst[ring_r, ring_c] > T.FILLABLE, "MUR's value on a coastal land pixel is used"
    assert (sst[mask == 5] == T.NONE).all(), "lakes stay empty"
    assert (sst[mask == 9] == T.NONE).all(), "ice-covered water stays empty"
    assert (sst[(mask == T.MASK_SEA)] > T.FILLABLE).all()
    assert g.rows == sst.shape[0]


def test_value_grid_refuses_a_value_below_the_freezing_floor():
    sst, mask, _, _ = world()
    sst[1800, 100] = -31000                                     # a sea pixel (row 100 would be in the ice cap)
    with pytest.raises(ValueError):
        T.value_grid(sst, mask)


def test_coastal_fill_reaches_two_pixels_with_neighbour_means_and_never_lakes():
    b = np.full((9, 9), T.FILLABLE, dtype=np.int16)
    b[:, 0] = raw_of(20.0)                                     # sea on the west edge
    b[4, 1] = T.NONE                                           # a lake pixel next to the sea
    v, filled = T.coastal_fill(b, passes=2)
    assert np.isnan(v[4, 1]), "a lake pixel is never filled"
    assert np.all(np.isfinite(v[[0, 1, 2, 3, 5, 6, 7, 8], 1])) and np.all(np.isfinite(v[:, 2]))
    assert np.all(np.isnan(v[:, 3:])), "nothing beyond two pixels"
    assert v[0, 1] == pytest.approx(20.0) and v[0, 2] == pytest.approx(20.0)
    assert filled == int(np.isfinite(v[:, 1:]).sum())


# ------------------------------------------------------------------ t2 cells
def test_t2_cell_follows_the_documented_geometry_at_the_date_line_and_the_pole():
    sst, mask, isl, mid, _ = valued_world()
    g = T.Geo(PXT)
    for lat0, lon0 in ((-5, 175), (45, -180), (85, 0), (-90, 10)):
        v, _ = T.t2_cell(sst, g, lat0, lon0, 5)
        n = 5 * PXT
        assert v.shape == (n + 2, n + 2)
        r = np.arange(n + 2)[:, None]
        c = np.arange(n + 2)[None, :]
        lat = lat0 + 5 - r / PXT
        lon = lon0 + (c - 1) / PXT
        lon_w = (lon + 180) % 360 - 180
        want = T.to_c(raw_of(field_c(lat, lon_w)))
        sea = (np.abs(lat) < 80) & (np.abs(lat) < 90 - 0.5 / PXT)
        assert np.allclose(v[sea[:, 0]][:, :], want[sea[:, 0]], atol=1e-9, equal_nan=False), (lat0, lon0)
        if lat0 == 85:
            assert np.isnan(v[0]).all(), "no source row at 90 N"
        if lat0 == -90:
            assert np.isnan(v[-1]).all() and np.isnan(v[-2]).all(), "no source row at or below -90"


def test_neighbouring_t2_cells_agree_on_every_shared_node_also_where_the_fill_works():
    sst, mask, isl, mid, _ = valued_world()
    g = T.Geo(PXT)
    w, _ = T.t2_cell(sst, g, 10, 15, 5)                          # 15..20 E and 20..25 E: the island starts at 20 E
    e, _ = T.t2_cell(sst, g, 10, 20, 5)
    n = 5 * PXT
    # w's east apron column (c = n + 1, lon 20.0) is e's first interior column (c = 1); e's west apron is w's last
    assert np.array_equal(w[:, n + 1], e[:, 1], equal_nan=True)
    assert np.array_equal(w[:, n], e[:, 0], equal_nan=True)
    s_, _ = T.t2_cell(sst, g, 5, 20, 5)                          # the cell to the south: 5..10 N
    assert np.array_equal(e[-1], s_[1], equal_nan=True) and np.array_equal(e[-2], s_[0], equal_nan=True)


def test_island_fill_covers_the_coast_and_leaves_the_middle_empty():
    sst, mask, isl, mid, _ = valued_world()
    g = T.Geo(PXT)
    v, filled = T.t2_cell(sst, g, 10, 20, 5)
    assert filled > 0
    # the island occupies rows/cols of lat 10..12 N, lon 20..22 E: 3 px from its edge (ring + 2 fill rings) have values
    n = 5 * PXT
    lat = 10 + 5 - np.arange(n + 2) / PXT
    lon = 20 + (np.arange(n + 2) - 1) / PXT
    r_mid = int(np.argmin(np.abs(lat - 11.0)))
    c_mid = int(np.argmin(np.abs(lon - 21.0)))
    assert np.isnan(v[r_mid, c_mid]), "the middle of a 2-degree island stays empty (the far inland value is not used)"
    c_edge = int(np.argmin(np.abs(lon - 20.0)))
    assert np.isfinite(v[r_mid, c_edge:c_edge + 3]).all()
    assert np.isnan(v[r_mid, c_edge + 3]), "the fill reaches two pixels past MUR's coastal values, no further"


# ------------------------------------------------------------------ t1 / t0 nodes
def test_node_means_are_block_means_and_node_cells_follow_the_geometry():
    sst, mask, isl, mid, _ = valued_world()
    g = T.Geo(PXT)
    step = 5
    nodes, land = T.node_means(sst, g, step)
    assert nodes.shape == (180 * PXT // step + 1, 360 * PXT // step)
    res = g.res(step)
    a, k = 400, 1000                                            # a node: lat 90 - a*res, lon -180 + k*res
    c_lat_h, c_lon_h = 90 * PXT - step * a, -180 * PXT + step * k
    blk = [T.to_c(sst[g.row(c_lat_h + dy), g.col(c_lon_h + dx)]) for dy in range(-2, 3) for dx in range(-2, 3)]
    assert nodes[a, k] == pytest.approx(np.mean(blk))
    cell = 20
    for lat0, lon0 in ((0, 160), (-100, -180), (80, 0)):
        v = T.node_cell(nodes, g, step, lat0, lon0, cell)
        n = cell * PXT // step
        assert v.shape == (n + 2, n + 2)
        for r, c in ((0, 0), (1, 1), (n, n), (n + 1, n + 1), (17, 33)):
            lat = lat0 + cell - r * res
            lon = (lon0 + (c - 1) * res + 180) % 360 - 180
            aa = round((90 - lat) / res)
            kk = round((lon + 180) / res)
            if 0 <= aa < nodes.shape[0]:
                assert v[r, c] == nodes[aa, kk] or (np.isnan(v[r, c]) and np.isnan(nodes[aa, kk]))
            else:
                assert np.isnan(v[r, c])


def test_node_fill_fills_only_empty_coastal_nodes_next_to_values_across_the_date_line():
    nodes = np.full((5, 8), np.nan)
    land = np.zeros((5, 8), dtype=np.int32)
    nodes[2, 0] = 10.0
    land[2, 7] = land[2, 1] = land[0, 4] = 1                  # [2,7] touches [2,0] across the date line
    n = T.node_fill(nodes, land)
    assert n == 2 and nodes[2, 7] == 10.0 and nodes[2, 1] == 10.0
    assert np.isnan(nodes[0, 4]), "no valued neighbour"
    assert np.isnan(nodes[1, 0]), "no land in its block: never filled (open ice, lakes)"


# ------------------------------------------------------------------ codes
def test_quantize_round_trip_within_half_a_step_and_ranges_cover_every_value():
    rng = np.random.default_rng(3)
    v = rng.uniform(-1.8, 33.9, size=(60, 60))
    v[5, 5] = np.nan
    lo, hi = T.cell_range(v)
    assert lo <= np.nanmin(v) and hi >= np.nanmax(v) and hi > lo
    q = T.quantize(v, lo, hi)
    assert q[5, 5] == 0 and (q[np.isfinite(v)] >= 1).all()
    back = T.dequantize(q, lo, hi)
    assert np.nanmax(np.abs(back - v)) <= (hi - lo) / 508 + 1e-12
    flat = np.full((3, 3), 26.0)
    lo2, hi2 = T.cell_range(flat)
    assert hi2 > lo2 and np.allclose(T.dequantize(T.quantize(flat, lo2, hi2), lo2, hi2), 26.0, atol=(hi2 - lo2) / 508)


def test_cell_names_follow_the_clients_floor_rule():
    names5 = T.cell_names(5)
    names20 = T.cell_names(20)
    assert len(names5) == 36 * 72 and len(names20) == 10 * 18
    assert names20[0] == (-100, -180) and names20[-1] == (80, 160)
    rng = np.random.default_rng(1)
    s5, s20 = set(names5), set(names20)
    for lat, lon in zip(rng.uniform(-89.999, 89.999, 500), rng.uniform(-180, 179.999, 500)):
        for cell, s in ((5, s5), (20, s20)):
            assert (math.floor(lat / cell) * cell, math.floor(lon / cell) * cell) in s
    assert T.cell_name(-5, -180) == "-5_-180"


# ------------------------------------------------------------------ the whole build
@pytest.fixture(scope="module")
def built():
    sst, mask, isl, mid = world()
    src = (sst.copy(), mask.copy())
    up = S.Uploads(None)
    seen = {}
    real_put = up.put

    def put(key, body, ctype=S.PNG, cache=P.IMMUTABLE):
        seen[key] = body
        real_put(key, body, ctype, cache)
    up.put = put
    arrays = [sst, mask]
    del sst, mask
    manifest, stats = S.build("20261014", arrays, up, file_name="x.nc", build_id="20261015T110000Z")
    assert arrays == [], "build takes the arrays over (G31a F8)"
    return manifest, stats, seen, src


def test_build_lists_every_tile_it_stored_with_its_range_and_size(built):
    manifest, stats, seen, _ = built
    assert manifest["format"] == "sst-v1" and manifest["day"] == "2026-10-14" and manifest["run"] == "20261014"
    assert manifest["complete"] is True and manifest["range"] == [-2.0, 35.0]
    names = [t["name"] for t in manifest["tiers"]]
    assert names == ["t2", "t1"]
    total = 1
    for t in manifest["tiers"]:
        assert t["apron"] == 1 and t["dir"] == f"sst/v1/20261014/20261015T110000Z/{t['name']}"
        assert t["px"] == round(t["cell"] / t["res"])
        for name, (lo, hi, nbytes) in t["cells"].items():
            body = seen[f"{t['dir']}/{name}.png"]
            assert len(body) == nbytes and lo < hi
            im = Image.open(io.BytesIO(body))
            assert im.mode == "L" and im.size == (t["px"] + 2, t["px"] + 2)
        total += len(t["cells"])
    assert set(seen) == {k for k in seen if k.endswith(".png")} and len(seen) == total
    assert manifest["tier0"]["key"] in seen and manifest["tier0"]["key"] == "sst/v1/20261014/20261015T110000Z/t0.png"
    assert manifest["build"] == "20261015T110000Z"
    assert stats["tiers"]["t2"]["cells"] == len(manifest["tiers"][0]["cells"])


def test_build_values_read_back_within_half_a_step(built):
    manifest, stats, seen, (src, srcmask) = built
    t2 = manifest["tiers"][0]
    g = T.Geo(PXT)
    rng = np.random.default_rng(5)
    checked = 0
    while checked < 300:
        R = int(rng.integers(0, g.rows))
        C = int(rng.integers(0, g.cols))
        if srcmask[R, C] != T.MASK_SEA or abs(90 - (R + 1) / PXT) > 79:
            continue
        lat = (90 * PXT - 1 - R) / PXT
        lon = (-180 * PXT + 1 + C) / PXT
        lon = (lon + 180) % 360 - 180
        lat0, lon0 = math.floor(lat / 5) * 5, math.floor(lon / 5) * 5
        lo, hi, _ = t2["cells"][f"{lat0}_{lon0}"]
        q = np.array(Image.open(io.BytesIO(seen[f"{t2['dir']}/{lat0}_{lon0}.png"])))
        r = round((lat0 + 5 - lat) * PXT)
        c = round((lon - lon0) * PXT) + 1
        v = lo + (int(q[r, c]) - 1) / 254 * (hi - lo)
        assert abs(v - T.to_c(src[R, C])) <= (hi - lo) / 508 + 1e-9
        checked += 1
    t0 = np.array(Image.open(io.BytesIO(seen[manifest["tier0"]["key"]])))
    gr = manifest["tier0"]["grid"]
    assert t0.shape == (gr["rows"], gr["cols"]) == (180 * PXT // T.T0_STEP + 1, 360 * PXT // T.T0_STEP)


def test_the_t0_grid_is_the_overlay_frames_grid_at_mur_resolution():
    g = S.t0_grid(T.Geo())
    assert g == {"cols": 1440, "rows": 721, "lon0": -180.0, "lat0": 90.0, "dlon": 0.25, "dlat": -0.25,
                 "registration": "center", "lon_periodic": True}


def test_publish_writes_the_stats_then_the_manifest_then_the_pointer_last(built):
    manifest, stats, _, _ = built
    fc = FakeClient()
    store = P.Store(fc, "b")
    mkey, ptr = S.publish_manifest(store, "20261014", manifest, stats)
    order = [k for op, k in fc.log if op == "put"]
    assert order == [S.stats_key("20261014", manifest["published_utc"]), mkey, S.latest_key()]
    assert fc.objects[S.latest_key()]["cc"] == P.POINTER and fc.objects[mkey]["cc"] == P.IMMUTABLE
    assert ptr == {"format": "sst-v1", "day": "2026-10-14", "run": "20261014", "manifest": mkey, "complete": True,
                   "published_utc": manifest["published_utc"]}
    body = json.loads(fc.objects[mkey]["body"])
    assert body["stats"] == order[0] and body["tiers"][0]["cells"] == manifest["tiers"][0]["cells"]


# ------------------------------------------------------------------ the bucket
def _day(fc, day, complete=True):
    fc.objects[f"sst/v1/{day}/{day}T120000Z/t0.png"] = {"body": b"x", "ct": "image/png", "cc": ""}
    fc.objects[f"sst/v1/{day}/{day}T120000Z/t2/0_0.png"] = {"body": b"x", "ct": "image/png", "cc": ""}
    if complete:
        fc.objects[f"sst/v1/{day}/manifest-x.json"] = {"body": b"{}", "ct": "", "cc": ""}


def test_prune_keeps_three_complete_days_and_the_live_one_and_never_touches_other_prefixes():
    fc = FakeClient()
    store = P.Store(fc, "b")
    for d in ("20261008", "20261009", "20261010", "20261011", "20261012"):
        _day(fc, d)
    _day(fc, "20261013", complete=False)                       # a build in progress
    _day(fc, "20261001", complete=False)                       # an old crash leftover
    fc.objects["sst/v1/latest.json"] = {"body": json.dumps({"run": "20261009"}).encode(), "ct": "", "cc": ""}
    fc.objects["sst/v1/failed/20261007.json"] = {"body": b"{}", "ct": "", "cc": ""}
    fc.objects["sst/v1/failed/20261011.json"] = {"body": b"{}", "ct": "", "cc": ""}
    fc.objects["gfswave/0p25/v1/2026101400/hs/f000.png"] = {"body": b"x", "ct": "", "cc": ""}
    deleted = S.prune(store, keep=3)
    assert deleted == ["20261001", "20261008"]
    left = {k.split("/")[2] for k in fc.objects if k.startswith("sst/v1/") and k.split("/")[2].isdigit()}
    assert left == {"20261009", "20261010", "20261011", "20261012", "20261013"}
    assert "sst/v1/failed/20261007.json" not in fc.objects, "a failure record older than every kept day goes"
    assert "sst/v1/failed/20261011.json" in fc.objects and "gfswave/0p25/v1/2026101400/hs/f000.png" in fc.objects
    assert not [k for k in fc.objects if k.startswith("sst/v1/20261008/")], "a pruned day goes with its build directories"


def test_parse_listing_keeps_only_mur_day_files_in_order():
    text = ("Name,lastModified,size,description\n"
            "20261014090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc,1792062043791,753551757,\n"
            "20261012090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc,1792062097411,765434126,\n"
            "20261013120000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc,1,2,\n"
            "readme.txt,1,2,\n")
    assert S.parse_listing(text) == [("20261012", "20261012090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", 765434126),
                                     ("20261014", "20261014090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", 753551757)]


class _Resp:
    def __init__(self, body, status=200, fail_after=None):
        self.body, self.status, self.fail_after, self.pos = body, status, fail_after, 0

    def read(self, n=-1):
        if self.fail_after is not None and self.pos >= self.fail_after:
            raise ConnectionResetError("cut")
        end = len(self.body) if n < 0 else min(len(self.body), self.pos + n)
        if self.fail_after is not None:
            end = min(end, self.fail_after)
        out = self.body[self.pos:end]
        self.pos = end
        return out

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_download_restarts_from_byte_zero_and_insists_on_the_listed_size(tmp_path, monkeypatch):
    """ERDDAP answers every Range request with 416 (G31a F1): a cut transfer starts again from byte 0, whole."""
    monkeypatch.setattr(S, "MIN_FILE_BYTES", 10)
    monkeypatch.setattr(S.time, "sleep", lambda s: None)
    data = bytes(range(256)) * 400
    calls = []

    def erddap(url, headers=None, timeout=None):
        calls.append(dict(headers or {}))
        if headers and "Range" in headers:
            raise IOError("HTTP Error 416: Requested Range Not Satisfiable")
        if len(calls) == 1:
            return _Resp(data, fail_after=30000)
        return _Resp(data)
    dest = str(tmp_path / "f.nc")
    S.download("f.nc", len(data), dest, get=erddap, log=lambda *a: None)
    assert open(dest, "rb").read() == data and calls == [{}, {}], "no Range header, ever"

    calls.clear()
    with pytest.raises(IOError):                                # cut every time: given up after `tries`
        S.download("f.nc", len(data), dest, get=lambda *a, **k: _Resp(data, fail_after=500), tries=3,
                   log=lambda *a: None)
    with pytest.raises(IOError):
        S.download("f.nc", len(data) + 5, dest, get=lambda *a, **k: _Resp(data), tries=2, log=lambda *a: None)
    with pytest.raises(ValueError):
        S.download("f.nc", len(data) - 5, dest, get=lambda *a, **k: _Resp(data), tries=2, log=lambda *a: None)
    with pytest.raises(ValueError):
        S.download("f.nc", 5, dest, get=erddap)                 # a listed size no MUR day has


def test_download_gives_up_at_its_deadline_instead_of_the_workflow_timeout(tmp_path, monkeypatch):
    """G31a F3: a slow mirror is a normal failure (recorded, backed off), never the 60-minute workflow kill."""
    monkeypatch.setattr(S, "MIN_FILE_BYTES", 10)
    monkeypatch.setattr(S.time, "sleep", lambda s: None)
    data = b"x" * 100000
    clock = [0.0]

    class Slow(_Resp):
        def read(self, n=-1):
            clock[0] += 60.0                                   # a minute a chunk
            return super().read(1000)
    tries = []

    def get(url, headers=None, timeout=None):
        tries.append(1)
        return Slow(data)
    with pytest.raises(S.DeadlineExceeded):
        S.download("f.nc", len(data), str(tmp_path / "f.nc"), get=get, deadline_s=600, clock=lambda: clock[0],
                   log=lambda *a: None)
    assert len(tries) == 1, "the deadline is not retried"
    assert S.DOWNLOAD_DEADLINE_S <= 45 * 60


def _listing_text(*days):
    return "Name,lastModified,size,description\n" + "".join(
        f"{d}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc,1,{400_000_000 + i},\n" for i, d in enumerate(days))


def _fake_get(days):
    def get(url, headers=None, timeout=None):
        if url == S.LISTING:
            return _Resp(_listing_text(*days).encode())
        raise AssertionError("no download expected: " + url)
    return get


def test_main_does_nothing_when_the_newest_day_is_live_and_raises_the_stale_alarm_once(monkeypatch):
    fc = FakeClient()
    store = P.Store(fc, "b")
    today = datetime.now(timezone.utc)
    newest = (today - timedelta(days=1)).strftime("%Y%m%d")
    fc.objects[S.latest_key()] = {"body": json.dumps({"run": newest}).encode(), "ct": "", "cc": ""}
    assert S.main([], get=_fake_get([newest]), store=store) == 0
    assert S.stale_key() not in fc.objects

    old = (today - timedelta(days=6)).strftime("%Y%m%d")
    fc.objects[S.latest_key()] = {"body": json.dumps({"run": old}).encode(), "ct": "", "cc": ""}
    assert S.main([], get=_fake_get([old]), store=store) == 1, "a 6-day-old newest day fails the run (GitHub mails it)"
    assert S.main([], get=_fake_get([old]), store=store) == 0, "...once in STALE_ALARM_EVERY_H"


def test_main_never_regresses_the_pointer_and_stays_quiet_through_a_listing_outage(monkeypatch):
    fc = FakeClient()
    store = P.Store(fc, "b")
    today = datetime.now(timezone.utc)
    live = (today - timedelta(days=1)).strftime("%Y%m%d")
    older = (today - timedelta(days=2)).strftime("%Y%m%d")
    fc.objects[S.latest_key()] = {"body": json.dumps({"run": live}).encode(), "ct": "", "cc": ""}
    assert S.main([], get=_fake_get([older]), store=store) == 0, "an older listed day never replaces the live one"

    def down(url, headers=None, timeout=None):
        raise IOError("HTTP Error 503")
    assert S.main([], get=down, store=store) == 0, "a listing outage while the live day is fresh: no mail (G31a F4)"
    assert S.stale_key() not in fc.objects
    old = (today - timedelta(days=6)).strftime("%Y%m%d")
    fc.objects[S.latest_key()] = {"body": json.dumps({"run": old}).encode(), "ct": "", "cc": ""}
    assert S.main([], get=down, store=store) == 1, "...but the live day going stale still mails, once"
    assert S.main([], get=down, store=store) == 0
    assert S.main(["--dry-run"], get=down, store=store) == 1, "a dry run reports the outage itself"


def test_a_manual_day_raises_no_stale_alarm_and_a_day_that_keeps_failing_does(monkeypatch):
    fc = FakeClient()
    store = P.Store(fc, "b")
    today = datetime.now(timezone.utc)
    live = (today - timedelta(days=6)).strftime("%Y%m%d")
    fc.objects[S.latest_key()] = {"body": json.dumps({"run": live}).encode(), "ct": "", "cc": ""}
    assert S.main(["--day", live], get=_fake_get([live]), store=store) == 0, "--day is not about freshness (G31a F5)"
    assert S.stale_key() not in fc.objects
    newer = (today - timedelta(days=1)).strftime("%Y%m%d")
    for _ in range(S.FAILED_MAX_ATTEMPTS):
        S.record_failure(store, newer, "boom")
    assert S.main([], get=_fake_get([newer]), store=store) == 1, "a day that keeps failing ends in the alarm (G31a F6)"


def test_main_waits_after_a_recent_failure_and_publishes_a_new_day_end_to_end(monkeypatch, tmp_path):
    fc = FakeClient()
    store = P.Store(fc, "b")
    day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
    prev = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y%m%d")
    fc.objects[S.latest_key()] = {"body": json.dumps({"run": prev}).encode(), "ct": "", "cc": ""}
    S.record_failure(store, day, "boom")
    assert S.main([], get=_fake_get([day]), store=store) == 0, "a fresh live day: the back-off is quiet"
    assert not [k for k in fc.objects if k.startswith(f"sst/v1/{day}/")]

    fc.objects.pop(S.failed_key(day))
    monkeypatch.setattr(S, "MIN_FILE_BYTES", 10)
    body = b"m" * 1000

    def get(url, headers=None, timeout=None):
        if url == S.LISTING:
            return _Resp(("Name,lastModified,size,\n" + f"{day}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc,1,{len(body)},\n").encode())
        return _Resp(body)
    w = world()

    def reader(path, d):
        assert d == day and open(path, "rb").read() == body
        return w[0].copy(), w[1].copy()
    assert S.main([], get=get, store=store, reader=reader) == 0
    puts = [k for op, k in fc.log if op == "put"]
    assert puts[-1] == S.latest_key(), "the pointer is written last"
    ptr = json.loads(fc.objects[S.latest_key()]["body"])
    assert ptr["run"] == day and ptr["manifest"] in fc.objects
    man = json.loads(fc.objects[ptr["manifest"]]["body"])
    for t in man["tiers"]:
        for name in t["cells"]:
            assert f"{t['dir']}/{name}.png" in fc.objects
    t0 = man["tier0"]["key"]
    assert t0.startswith(f"sst/v1/{day}/") and t0.endswith("/t0.png") and fc.objects[t0]["cc"] == P.IMMUTABLE


def test_main_records_a_failed_build_and_rejects_bad_arguments(monkeypatch):
    fc = FakeClient()
    store = P.Store(fc, "b")
    day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
    monkeypatch.setattr(S, "MIN_FILE_BYTES", 1)

    def get(url, headers=None, timeout=None):
        if url == S.LISTING:
            return _Resp(f"x\n{day}090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc,1,3,\n".encode())
        return _Resp(b"abc")

    def reader(path, d):
        raise ValueError("unexpected analysed_sst scaling")
    with pytest.raises(ValueError):
        S.main([], get=get, store=store, reader=reader)
    rec = json.loads(fc.objects[S.failed_key(day)]["body"])
    assert rec["attempts"] == 1 and "scaling" in rec["last_error"]
    assert S.latest_key() not in fc.objects
    assert S.main(["--keep", "0"]) == 2 and S.main(["--day", "2026101"]) == 2 and S.main(["--file", "x.nc"]) == 2


def test_read_mur_checks_the_file_and_returns_north_up_arrays(tmp_path):
    h5py = pytest.importorskip("h5py")
    g = T.Geo(PXT)
    path = str(tmp_path / "mur.nc")

    def write(scale=0.001, t_day="20261014"):
        with h5py.File(path, "w") as f:
            lat = (-90 + 1 / PXT) + np.arange(g.rows) / PXT
            lon = (-180 + 1 / PXT) + np.arange(g.cols) / PXT
            f["lat"] = lat.astype(np.float32)
            f["lon"] = lon.astype(np.float32)
            when = datetime.strptime(t_day + "09", "%Y%m%d%H").replace(tzinfo=timezone.utc)
            f["time"] = np.array([int((when - datetime(1981, 1, 1, tzinfo=timezone.utc)).total_seconds())], dtype=np.int32)
            sst = np.zeros((1, g.rows, g.cols), dtype=np.int16)
            sst[0, 0, :] = 111                                    # the SOUTH row in the file
            d = f.create_dataset("analysed_sst", data=sst)
            d.attrs["scale_factor"] = np.array([scale])
            d.attrs["add_offset"] = np.array([298.15])
            d.attrs["_FillValue"] = np.array([-32768], dtype=np.int16)
            f.create_dataset("mask", data=np.ones((1, g.rows, g.cols), dtype=np.int8))
    write()
    sst, mask = S.read_mur(path, "20261014", geo=g)
    assert sst.shape == (g.rows, g.cols) and (sst[-1] == 111).all() and (sst[0] == 0).all(), "north-up"
    write(scale=0.01)
    with pytest.raises(ValueError):
        S.read_mur(path, "20261014", geo=g)
    write(t_day="20261013")
    with pytest.raises(ValueError):
        S.read_mur(path, "20261014", geo=g)


def test_every_node_of_a_cell_picture_apron_included_reads_back_within_half_a_step(built):
    manifest, stats, seen, (src, srcmask) = built
    sst = src.copy()
    T.value_grid(sst, srcmask.copy())
    g = T.Geo(PXT)
    t2 = manifest["tiers"][0]
    checked = 0
    for name in ("10_20", "10_15", "5_20", "-5_175", "40_-180", "75_100"):
        lat0, lon0 = (int(x) for x in name.split("_"))
        want, _ = T.t2_cell(sst, g, lat0, lon0, 5)
        lo, hi, _ = t2["cells"][name]
        got = T.dequantize(np.array(Image.open(io.BytesIO(seen[f"{t2['dir']}/{name}.png"]))), lo, hi)
        assert np.array_equal(np.isnan(got), np.isnan(want)), name
        fin = np.isfinite(want)
        assert np.max(np.abs(got[fin] - want[fin])) <= (hi - lo) / 508 + 1e-9, name   # the apron never clamps
        checked += 1
    assert checked == 6


def test_a_day_built_again_never_reuses_a_tile_key(monkeypatch):
    """G31a F2: a tile's bytes depend on its cell's lo / hi, so a rebuild (--force, a code fix) writes new keys."""
    def keys(bid, bump):
        sst, mask, _, _ = world()
        sst[sst > T.FILLABLE] += bump
        up = S.Uploads(None)
        got = []
        real = up.put
        up.put = lambda key, body, *a, **k: (got.append(key), real(key, body, *a, **k))
        man, _ = S.build("20261014", [sst, mask], up, log=lambda *a: None, build_id=bid)
        return set(got), man
    a, ma = keys("20261015T110000Z", 0)
    b, mb = keys("20261015T120000Z", 7)
    assert a and b and not (a & b)
    assert all(k.startswith("sst/v1/20261014/20261015T110000Z/") for k in a)
    assert ma["tiers"][0]["cells"]["10_20"] != mb["tiers"][0]["cells"]["10_20"]


def test_every_tile_is_stored_before_build_returns_with_a_slow_bucket():
    """The manifest must never name a tile still on its way (the drain before the manifest)."""
    import threading
    import time as _t
    fc = FakeClient()
    lock = threading.Lock()
    real = fc.put_object

    def slow(*a, **kw):
        _t.sleep(0.002)
        with lock:
            return real(*a, **kw)
    fc.put_object = slow
    up = S.Uploads(P.Store(fc, "b"), workers=4, backlog=64)
    sst, mask, _, _ = world()
    man, _ = S.build("20261014", [sst, mask], up, log=lambda *a: None, build_id="B")
    for t in man["tiers"]:
        for name in t["cells"]:
            assert f"{t['dir']}/{name}.png" in fc.objects
    assert man["tier0"]["key"] in fc.objects
    up.close()


def test_coastal_node_means_take_valued_pixels_only():
    sst, mask, isl, mid, _ = valued_world()
    g = T.Geo(PXT)
    step = 5
    nodes, land = T.node_means(sst, g, step)
    hits = 0
    for a in range(nodes.shape[0]):
        for k in range(nodes.shape[1]):
            if land[a, k] == 0:
                continue
            c_lat_h, c_lon_h = 90 * PXT - step * a, -180 * PXT + step * k
            px = [sst[g.row(c_lat_h + dy), g.col(c_lon_h + dx)] for dy in range(-2, 3) for dx in range(-2, 3)]
            good = [T.to_c(v) for v in px if v > T.FILLABLE]
            assert land[a, k] == sum(1 for v in px if v == T.FILLABLE)
            if good:
                assert nodes[a, k] == pytest.approx(np.mean(good)), (a, k)
                hits += 1
            else:
                assert np.isnan(nodes[a, k])
    assert hits > 10, "the island's coastal nodes were checked"


@pytest.mark.parametrize("dy,dx", [(dy, dx) for dy in (-1, 0, 1) for dx in (-1, 0, 1) if dy or dx])
def test_node_fill_takes_each_neighbour_from_its_own_side(dy, dx):
    nodes = np.full((5, 8), np.nan)
    land = np.zeros((5, 8), dtype=np.int32)
    nodes[2 + dy, 3 + dx] = 7.0
    land[2, 3] = 1
    land[2 - dy, 3 - dx] = 1                                    # the opposite side: two away from the value
    land[2 - 2 * dy, 3 - 2 * dx] = 1
    assert T.node_fill(nodes, land, passes=1) == 1
    assert nodes[2, 3] == 7.0 and np.isnan(nodes[2 - dy, 3 - dx])


def test_node_fill_does_not_wrap_across_the_poles():
    nodes = np.full((5, 8), np.nan)
    land = np.ones((5, 8), dtype=np.int32)
    nodes[4, 3] = 1.0                                           # the south pole row
    T.node_fill(nodes, land, passes=1)
    assert np.isnan(nodes[0]).all(), "row 0 is not row 4's neighbour"
    assert (nodes[3, 2:5] == 1.0).all() and nodes[4, 2] == 1.0 and nodes[4, 4] == 1.0
    nodes = np.full((5, 8), np.nan)
    nodes[0, 3] = 1.0                                           # the north pole row
    T.node_fill(nodes, land, passes=1)
    assert np.isnan(nodes[4]).all(), "row 4 is not row 0's neighbour"
    assert (nodes[1, 2:5] == 1.0).all()


def test_lake_ice_counts_as_lake_once():
    sst, mask, _, _ = world()
    lake = np.argwhere(mask == 5)
    mask[tuple(lake[0])] = 13
    mask[tuple(lake[1])] = 13
    counts = T.value_grid(sst, mask)
    assert counts["lake"] == int(((mask == 5) | (mask == 13)).sum())
    assert counts["ice"] == int((mask == 9).sum())


def _mur_file(path, g, lat_shift=0.0, fill=-32768, rows=None):
    import h5py
    with h5py.File(path, "w") as f:
        f["lat"] = ((-90 + 1 / PXT) + np.arange(g.rows) / PXT + lat_shift).astype(np.float32)
        f["lon"] = ((-180 + 1 / PXT) + np.arange(g.cols) / PXT).astype(np.float32)
        when = datetime(2026, 10, 14, 9, tzinfo=timezone.utc)
        f["time"] = np.array([int((when - datetime(1981, 1, 1, tzinfo=timezone.utc)).total_seconds())], dtype=np.int32)
        n = rows or g.rows
        d = f.create_dataset("analysed_sst", data=np.zeros((1, n, g.cols), dtype=np.int16))
        d.attrs["scale_factor"] = np.array([0.001])
        d.attrs["add_offset"] = np.array([298.15])
        d.attrs["_FillValue"] = np.array([fill], dtype=np.int16)
        f.create_dataset("mask", data=np.ones((1, n, g.cols), dtype=np.int8))


def test_read_mur_refuses_other_axes_shapes_and_fill_values(tmp_path):
    pytest.importorskip("h5py")
    g = T.Geo(PXT)
    path = str(tmp_path / "mur.nc")
    _mur_file(path, g)
    S.read_mur(path, "20261014", geo=g)
    for kw in ({"lat_shift": 0.05}, {"fill": -999}, {"rows": g.rows - 1}):
        _mur_file(path, g, **kw)
        with pytest.raises(ValueError):
            S.read_mur(path, "20261014", geo=g)


def test_a_local_build_never_touches_the_bucket(tmp_path):
    fc = FakeClient()
    store = P.Store(fc, "b")
    src = tmp_path / "x.nc"
    src.write_bytes(b"x")
    w = world()
    out = tmp_path / "out"
    assert S.main(["--local", str(out), "--file", str(src), "--day", "20261014"], store=store,
                  reader=lambda path, d: (w[0].copy(), w[1].copy())) == 0
    assert fc.objects == {}, "--local writes files only"
    assert (out / "sst" / "v1" / "latest.json").exists()


def test_coastal_fill_needs_one_valued_neighbour_and_the_pinned_constants():
    b = np.full((5, 5), T.NONE, dtype=np.int16)
    b[2, 2] = T.FILLABLE
    b[1, 1] = raw_of(20.0)
    v, n = T.coastal_fill(b, passes=1)
    assert n == 1 and v[2, 2] == pytest.approx(20.0)
    assert (T.COAST_PX, T.FILL_PASSES, T.NODE_FILL) == (3, 2, 1)
    assert (S.STALE_DAYS, S.STALE_ALARM_EVERY_H, S.FAILED_MAX_ATTEMPTS, S.FAILED_RETRY_AFTER_S) == (4, 12, 6, 3600)


def test_the_temporary_dry_run_workflow_is_gone():
    wf = os.path.join(ROOT, ".github", "workflows")
    assert not os.path.exists(os.path.join(wf, "sst-dryrun.yml"))
    assert not os.path.exists(os.path.join(ROOT, "tools", "model_frames", "sst_dryrun_check.py"))


# ------------------------------------------------------------------ the workflow
def test_the_sst_workflow_is_pinned_hourly_separate_and_kept_alive():
    import re
    wf = os.path.join(ROOT, ".github", "workflows")
    text = open(os.path.join(wf, "model-sst.yml"), encoding="utf-8").read()
    assert "python tools/model_frames/sst.py" in text and "group: model-sst" in text and 'cron: "20 * * * *"' in text
    uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
    assert uses and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses)
    pkgs = text.split("create-args: >-", 1)[1].split("cache-environment", 1)[0].split()
    assert all(re.fullmatch(r"[a-z0-9-]+=[0-9][0-9A-Za-z.]*", p) for p in pkgs) and any(p.startswith("h5py=") for p in pkgs)
    assert "eccodes" not in text, "the SST job needs no GRIB decoder"
    keep = open(os.path.join(wf, "model-frames-keepalive.yml"), encoding="utf-8").read()
    assert "gh workflow enable model-sst.yml" in keep
    tests = open(os.path.join(wf, "model-frames-tests.yml"), encoding="utf-8").read()
    assert tests.count('".github/workflows/model-sst.yml"') == 2 and "h5py==" in tests
    cfg = open(os.path.join(ROOT, "tools", "model_frames", "trigger", "wrangler.jsonc"), encoding="utf-8").read()
    assert "model-sst.yml" not in cfg, "a daily product: the hourly cron suffices, the 10-minute trigger does not dispatch it"
