"""Coast set v2 builder (tools/coast/build_coast_v2.py, plan section 103): the zoom-15 pixel class rules, the exact
pixel trace, holes, specks, the box seam, the cell split, unchanged cells, q = 1e5, determinism and --check.
Synthetic tiles and a synthetic v1 set only (no network). Needs shapely and Pillow (skipped without them)."""
import hashlib
import json
import os
import sys

import numpy as np
import pytest

pytest.importorskip("shapely")
pytest.importorskip("PIL")
import shapely  # noqa: E402
from shapely.geometry import Polygon, box as sbox  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "coast"))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))

import build_coast as BC      # noqa: E402
import build_coast_v2 as V2   # noqa: E402
import bathy_tiles as T       # noqa: E402

OUT, LAND, WATER = V2.OUT, V2.LAND, V2.WATER
# Zoom-15 tile 2275 holds the -155 meridian (at column ~142); the test region lives around it at ~19.5 N.
X0, Y0 = 2275, T.tile_of(-155.0, 19.51, 15)[1]


# ---------------------------------------------------------------- fixtures

def write_tile(base, z, x, y, cls):
    """A bathy tile file whose 256 x 256 core holds the class array (water = 5 m deep)."""
    cls = np.asarray(cls, np.uint8)
    code = np.where(cls == WATER, T.encode_depth(np.array([5.0]))[0], 0).astype(np.uint16)
    src = np.where(cls == LAND, T.LAND_ID, np.where(cls == WATER, 10, 0)).astype(np.uint8)
    code, src = np.pad(code, 1, mode="edge"), np.pad(src, 1, mode="edge")
    p = os.path.join(base, "t", str(z), str(x), "%d.png" % y)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    open(p, "wb").write(T.pack_tile(code, src))


def write_region(tiles_dir, name, v, tiles, digest="synthetic"):
    """tiles: {(z, x, y): class array}. -> the region's directory."""
    base = os.path.join(tiles_dir, name, "v%d" % v)
    counts = {}
    for (z, x, y), cls in tiles.items():
        write_tile(base, z, x, y, cls)
        counts[str(z)] = counts.get(str(z), 0) + 1
    json.dump({"version": v, "region": name, "tiles": counts, "digest": digest},
              open(os.path.join(base, "index.json"), "w"))
    return base


def tile_box(xa, ya, xb, yb, z=15):
    """lon/lat box of the zoom-z tiles xa..xb x ya..yb."""
    n = 2 ** z
    lon0, lon1 = xa / n * 360 - 180, (xb + 1) / n * 360 - 180
    lat1 = float(np.degrees(np.arctan(np.sinh(np.pi * (1 - 2 * ya / n)))))
    lat0 = float(np.degrees(np.arctan(np.sinh(np.pi * (1 - 2 * (yb + 1) / n)))))
    return (lon0, lat0, lon1, lat1)


def write_v1(v1_dir, cells, tier0_pieces=None):
    """A coast-v1 land set: cells = {name: [ring (lon, lat) list, ...]} (one ring per piece, CCW, q = 1e4)."""
    os.makedirs(os.path.join(v1_dir, "f"), exist_ok=True)
    listing, digests = {}, {}
    for name, rings in cells.items():
        pieces = []
        for ring in rings:
            a = np.asarray(ring, np.float64)
            pieces.append([BC.quantize_ring(a[:, 0], a[:, 1])])
        body = BC.encode_file(pieces, 5)
        open(os.path.join(v1_dir, "f", name + ".bin"), "wb").write(body)
        listing[name] = [len(body), sum(len(r[0]) for pc in pieces for r in pc)]
        digests[name] = hashlib.sha256(body).hexdigest()
    t0 = BC.encode_file(tier0_pieces or [[BC.quantize_ring(np.array([10.0, 11, 11, 10]), np.array([10.0, 10, 11, 11]))]], 30)
    open(os.path.join(v1_dir, "world-i.bin"), "wb").write(t0)
    index = {"format": "coast-v1", "format_key": BC.FORMAT_KEY, "q": BC.Q, "source": BC.SOURCE, "land_levels": [1, 5],
             "tier0": {"file": "world-i.bin", "res": "i", "cell": 30, "max_zoom": 6, "bytes": len(t0),
                       "sha256": hashlib.sha256(t0).hexdigest(), "pieces": 1, "vertices": 4},
             "tier1": {"dir": "f", "res": "f", "cell": 5, "min_zoom": 7, "cells": listing,
                       "bytes": sum(v[0] for v in listing.values()), "vertices": sum(v[1] for v in listing.values()),
                       "sha256": BC.cells_digest(digests)}}
    json.dump(index, open(os.path.join(v1_dir, "index.json"), "w"))
    return index


def centre(X, Y):
    """lon/lat of the centre of global zoom-15 pixel X, Y."""
    return float(V2.px_lon(X + 0.5)), float(V2.px_lat(Y + 0.5))


def cell_of(lon, lat):
    return BC.cell_name(int(np.floor(lat / 5)) * 5, int(np.floor(lon / 5)) * 5)


def land_at(out_dir, lon, lat):
    p = os.path.join(out_dir, "f", cell_of(lon, lat) + ".bin")
    if not os.path.exists(p):
        return False
    d = BC.decode_file(open(p, "rb").read())
    return BC.point_in_pieces(lon, lat, d["pieces"], d["q"])


def winding_at(out_dir, lon, lat):
    p = os.path.join(out_dir, "f", cell_of(lon, lat) + ".bin")
    d = BC.decode_file(open(p, "rb").read())
    px, py, w = lon * d["q"], lat * d["q"], 0
    for pc in d["pieces"]:
        for ix, iy in pc["rings"]:
            x, y = ix.astype(np.float64), iy.astype(np.float64)
            xn, yn = np.roll(x, -1), np.roll(y, -1)
            up = (y <= py) & (yn > py); down = (y > py) & (yn <= py)
            cross = (xn - x) * (py - y) - (px - x) * (yn - y)
            w += int(np.count_nonzero(up & (cross > 0))) - int(np.count_nonzero(down & (cross < 0)))
    return w


def full(c):
    return np.full((256, 256), c, np.uint8)


def far_v1_cells():
    return {"40_-125": [[(-124.5, 40.5), (-124.0, 40.5), (-124.0, 41.0), (-124.5, 41.0)]]}


@pytest.fixture
def world(tmp_path):
    """A region of 3 x 3 zoom-15 tiles around tile X0/Y0 (zmin 14) with a far, untouched v1 cell."""
    def make(tiles, v1_cells=None, box=None, zmin=14, name="hawaii"):
        tdir = str(tmp_path / "tiles")
        write_region(tdir, name, 2, tiles)
        v1 = str(tmp_path / "v1")
        write_v1(v1, dict(far_v1_cells(), **(v1_cells or {})))
        reg = {name: dict(v=2, box=box or tile_box(X0 - 1, Y0 - 1, X0 + 1, Y0 + 1), zmin=zmin, digest="synthetic",
                          tiles_sha256=None)}
        return tdir, v1, reg
    return make


def build(tmp_path, tdir, v1, reg, out="out"):
    o = str(tmp_path / out)
    V2.build(v1, tdir, o, regions=reg, log=lambda m: None)
    return o


# ---------------------------------------------------------------- the pixel trace

def test_mask_polygons_reproduce_the_mask_exactly():
    rng = np.random.default_rng(7)
    m = rng.random((60, 70)) < 0.45
    m[10, 10] = m[11, 11] = True; m[10, 11] = m[11, 10] = False        # a diagonal pair
    polys = V2.mask_polygons(m, 1000, 2000)
    g = shapely.union_all(polys)
    jj, ii = np.mgrid[0:60, 0:70]
    got = shapely.contains_xy(g, 1000 + ii.ravel() + 0.5, 2000 + jj.ravel() + 0.5).reshape(m.shape)
    assert (got == m).all()
    assert abs(g.area - m.sum()) < 1e-9
    for p in polys:                                                     # integer grid coordinates only
        c = np.asarray(p.exterior.coords)
        assert (c == np.round(c)).all()


def test_diagonal_pixels_stay_separate_pieces():
    m = np.zeros((4, 4), bool)
    m[1, 1] = m[2, 2] = True
    polys = V2.mask_polygons(m)
    assert len(polys) == 2 and sorted(p.area for p in polys) == [1.0, 1.0]


def test_specks_dropped_and_counted():
    m = np.zeros((20, 20), bool)
    m[2:12, 2:12] = True
    m[6, 6] = False                       # a 1-px hole
    m[16, 16] = True                      # a 1-px island
    kept, n_land, n_hole = V2.drop_specks(shapely.union_all(V2.mask_polygons(m)), V2.SPECK_PX2)
    assert (n_land, n_hole) == (1, 1)
    assert len(kept) == 1 and not kept[0].interiors and kept[0].area == 100


# ---------------------------------------------------------------- rule 1

def test_class_rules_present_ancestor_none_outside(world, tmp_path):
    t15 = full(WATER)
    t15[:, :100] = LAND
    t15[:40, 100:] = OUT                         # "outside" pixels: GSHHG decides
    anc = full(WATER)                            # the zoom-14 parent of X0-1 / X0 (x0 >> 1)
    sib = X0 - 1                                 # missing at 15: its quadrant of the parent is all land
    qx, qy = (sib - ((X0 >> 1) << 1)) * 128, (Y0 - ((Y0 >> 1) << 1)) * 128
    anc[qy:qy + 128, qx:qx + 128] = LAND
    tiles = {(15, X0, Y0): t15, (14, X0 >> 1, Y0 >> 1): anc}
    # GSHHG v1: land over the outside strip of the present tile and over half of tile X0+1 (no tile at any zoom)
    lon_a, lat_top = centre(X0 * 256 + 150, Y0 * 256)
    lon_b, _ = centre(X0 * 256 + 255, Y0 * 256)
    _, lat_bot = centre(X0 * 256, Y0 * 256 + 255)
    lon_c, _ = centre((X0 + 1) * 256 + 128, Y0 * 256)
    ring = [(lon_a, lat_bot), (lon_c, lat_bot), (lon_c, lat_top + 0.001), (lon_a, lat_top + 0.001)]
    assert (X0 + 1) >> 1 != X0 >> 1                     # tile X0+1 has no parent tile
    tdir, v1, reg = world(tiles, {cell_of(lon_a, lat_bot): [ring]})
    t = V2.Tiles(tdir, "hawaii", reg["hawaii"])
    v1i = V2.load_v1(v1)
    stats = dict(z15=0, ancestor=0, none=0, gshhg_px=0)
    bx, by = X0 // 8, Y0 // 8
    cls = V2.block_class(t, bx, by, v1, v1i, stats)
    assert cls is not None
    ox, oy = (X0 - bx * 8) * 256, (Y0 - by * 8) * 256
    tile = cls[oy:oy + 256, ox:ox + 256]
    assert (tile[50:, :100] == LAND).all() and (tile[50:, 100:] == WATER).all()        # present: its own class
    assert (tile[:40, 160:250] == LAND).all()                    # outside + GSHHG land -> land
    assert (tile[:40, 100:140] == OUT).all()                     # outside + GSHHG water -> stays outside (not land)
    sx = (sib - bx * 8) * 256
    if 0 <= sx < 2048:
        assert (cls[oy:oy + 256, sx:sx + 256] == LAND).all()     # missing at 15: the ancestor's quadrant (all land)
    nx = (X0 + 1 - bx * 8) * 256
    if 0 <= nx < 2048:
        none = cls[oy:oy + 256, nx:nx + 256]
        assert (none[60:, :120] == LAND).all() and (none[60:, 140:] == OUT).all()   # no tile: GSHHG decides
    assert stats["z15"] >= 1 and stats["ancestor"] >= 1 and stats["none"] >= 1 and stats["gshhg_px"] > 0


def test_ancestor_water_is_upsampled():
    """The 1/3" case: no zoom-15 tile, the zoom-14 parent holds water and land in that quadrant."""
    class Fake(V2.Tiles):
        def __init__(self):
            self.reg = dict(zmin=13, box=(-180, -80, 180, 80))
            self.present = {15: set(), 14: {(10, 20)}, 13: set()}
            self._anc, self.anomalies = {}, 0

        def cls(self, z, x, y):
            c = full(WATER); c[:, :64] = LAND
            return c
    f = Fake()
    a = f.ancestor(20, 40)
    assert a == (14, 10, 20)
    s = f.anc_slice(20, 40, a)
    assert s.shape == (128, 128) and (s[:, :64] == LAND).all() and (s[:, 64:] == WATER).all()
    assert f.anc_slice(21, 40, a).shape == (128, 128) and (f.anc_slice(21, 40, a) == WATER).all()
    assert f.ancestor(40, 40) is None


# ---------------------------------------------------------------- build: holes, seam, split, unchanged, q, determinism

def island_with_lagoon():
    t = full(WATER)
    t[40:200, 40:200] = LAND
    t[100:140, 100:140] = WATER          # sea water enclosed by land (a CW hole)
    return t


def test_holes_are_clockwise_and_read_as_water(world, tmp_path):
    tdir, v1, reg = world({(15, X0 - 1, Y0): island_with_lagoon(), (14, (X0 - 1) >> 1, Y0 >> 1): full(WATER)})
    out = build(tmp_path, tdir, v1, reg)
    lon, lat = centre((X0 - 1) * 256 + 120, Y0 * 256 + 120)
    assert land_at(out, *centre((X0 - 1) * 256 + 60, Y0 * 256 + 60))
    assert not land_at(out, lon, lat)
    assert not land_at(out, *centre((X0 - 1) * 256 + 20, Y0 * 256 + 20))
    d = BC.decode_file(open(os.path.join(out, "f", cell_of(lon, lat) + ".bin"), "rb").read())
    holes = [r for pc in d["pieces"] for r in pc["rings"][1:]]
    assert len(holes) == 1 and BC.signed_area(holes[0][0].astype(float), holes[0][1].astype(float)) < 0
    assert V2.check(out, log=lambda m: None, regions=reg) == []


def test_box_edge_crossing_has_no_gap_or_overlap(world, tmp_path):
    """A GSHHG ring crossing the box's east edge joins the v2 land inside the box with winding exactly 1."""
    t = full(WATER)
    t[100:160, :] = LAND                       # a land band running to the box's east edge (tile X0+1 is the last)
    box = tile_box(X0 - 1, Y0 - 1, X0 + 1, Y0 + 1)
    _, lat_n = centre(0, Y0 * 256 + 80)        # GSHHG's ring is taller than the band: rows 80-180
    _, lat_s = centre(0, Y0 * 256 + 180)
    ring = [(box[2] - 0.0005, lat_s), (box[2] + 0.002, lat_s), (box[2] + 0.002, lat_n), (box[2] - 0.0005, lat_n)]
    tdir, v1, reg = world({(15, X0 + 1, Y0): t, (14, (X0 + 1) >> 1, Y0 >> 1): full(WATER)},
                          {cell_of(box[2], lat_n): [ring]})
    out = build(tmp_path, tdir, v1, reg)
    idx = json.load(open(os.path.join(out, "index.json")))
    assert idx["v1_rings_crossing_box"] == 1
    lat = centre(0, Y0 * 256 + 130)[1]
    for dx in (-3e-5, -1e-5, 1e-5, 3e-5, 0.001):
        assert winding_at(out, box[2] + dx, lat) == 1, dx
    assert winding_at(out, box[2] + 0.003, lat) == 0
    # GSHHG's part inside the box is gone (the box's class decides: water at row 90); outside it GSHHG stays
    lat90 = centre(0, Y0 * 256 + 90)[1]
    assert not land_at(out, box[2] - 0.0002, lat90)
    assert winding_at(out, box[2] + 0.0002, lat90) == 1
    assert V2.check(out, log=lambda m: None, regions=reg) == []


def test_split_at_minus_155_keeps_both_halves(world, tmp_path):
    t = full(WATER)
    t[60:200, 60:220] = LAND                   # straddles -155 (column ~142 of tile 2275)
    tdir, v1, reg = world({(15, X0, Y0): t, (14, X0 >> 1, Y0 >> 1): full(WATER)})
    out = build(tmp_path, tdir, v1, reg)
    idx = json.load(open(os.path.join(out, "index.json")))
    lat = centre(0, Y0 * 256 + 120)[1]
    assert sorted(idx["changed"]) == sorted([cell_of(-155.001, lat), cell_of(-154.999, lat)])
    for lon in (-155.0003, -155.00001, -154.99999, -154.9997):
        assert winding_at(out, lon, lat) == 1, lon
    assert V2.check(out, log=lambda m: None, regions=reg) == []


def test_unchanged_cells_byte_identical_and_q(world, tmp_path):
    tdir, v1, reg = world({(15, X0 - 1, Y0): island_with_lagoon(), (14, (X0 - 1) >> 1, Y0 >> 1): full(WATER)})
    out = build(tmp_path, tdir, v1, reg)
    assert open(os.path.join(out, "f", "40_-125.bin"), "rb").read() == open(os.path.join(v1, "f", "40_-125.bin"), "rb").read()
    assert open(os.path.join(out, "world-i.bin"), "rb").read() == open(os.path.join(v1, "world-i.bin"), "rb").read()
    idx = json.load(open(os.path.join(out, "index.json")))
    assert idx["kind"] == "land2" and idx["format"] == "coast-v1" and idx["q"] == V2.Q2
    for name in idx["changed"]:
        d = BC.decode_file(open(os.path.join(out, "f", name + ".bin"), "rb").read())
        assert d["q"] == V2.Q2
    assert BC.decode_file(open(os.path.join(out, "f", "40_-125.bin"), "rb").read())["q"] == BC.Q
    assert "NOAA" in open(os.path.join(out, "LICENSE.txt"), encoding="utf-8").read()


def test_q1e5_round_trip():
    ring = Polygon([(-155.123456, 19.5), (-155.1, 19.5), (-155.1, 19.512345), (-155.123456, 19.512345)])
    pieces = V2.cell_pieces(ring)
    d = BC.decode_file(BC.encode_file(pieces, 5, q=V2.Q2))
    ix, iy = d["pieces"][0]["rings"][0]
    assert d["q"] == 100000
    assert set(np.round(ix / 1e5, 5)) == {-155.12346, -155.1} and set(np.round(iy / 1e5, 5)) == {19.5, 19.51234}
    assert BC.signed_area(ix.astype(float), iy.astype(float)) > 0


def test_build_is_deterministic(world, tmp_path):
    tiles = {(15, X0, Y0): island_with_lagoon(), (14, X0 >> 1, Y0 >> 1): full(WATER)}
    tdir, v1, reg = world(tiles)
    a = build(tmp_path, tdir, v1, reg, "a")
    b = build(tmp_path, tdir, v1, reg, "b")
    ia, ib = (json.load(open(os.path.join(o, "index.json"))) for o in (a, b))
    assert ia["tier1"]["sha256"] == ib["tier1"]["sha256"] and ia["tier1"]["cells"] == ib["tier1"]["cells"]


def test_check_catches_a_changed_unchanged_cell(world, tmp_path):
    tdir, v1, reg = world({(15, X0, Y0): island_with_lagoon(), (14, X0 >> 1, Y0 >> 1): full(WATER)})
    out = build(tmp_path, tdir, v1, reg)
    assert V2.check(out, log=lambda m: None, regions=reg) == []
    p = os.path.join(out, "f", "40_-125.bin")
    other = BC.encode_file([[BC.quantize_ring(np.array([-124.5, -124.1, -124.1, -124.5]),
                                              np.array([40.5, 40.5, 41.0, 41.0]))]], 5)
    open(p, "wb").write(other)
    probs = V2.check(out, log=lambda m: None, regions=reg)
    assert any("rebuild v1" in p for p in probs)


def test_incomplete_tiles_are_refused(world, tmp_path):
    tdir, v1, reg = world({(15, X0, Y0): island_with_lagoon()})
    ip = os.path.join(tdir, "hawaii", "v2", "index.json")
    ix = json.load(open(ip)); ix["tiles"]["15"] = 2
    json.dump(ix, open(ip, "w"))
    with pytest.raises(ValueError, match="incomplete"):
        V2.Tiles(tdir, "hawaii", reg["hawaii"])


# ---------------------------------------------------------------- the real configuration

def test_regions_match_the_bathy_builder():
    import bathy_sources as S
    for name, reg in V2.REGIONS.items():
        assert tuple(S.REGIONS[name]["box"]) == reg["box"]
        assert S.REGIONS[name]["zmin"] == reg["zmin"]


def test_expected_changed_cells_lie_under_the_boxes():
    cands = {c for r in V2.REGIONS.values() for c in V2.cells_overlapping(r["box"])}
    assert set(V2.EXPECTED_CHANGED) <= cands
    for lon, lat, _ in V2.HAWAII_PROBES:
        assert any(r["box"][0] <= lon <= r["box"][2] and r["box"][1] <= lat <= r["box"][3] for r in V2.REGIONS.values())
        assert cell_of(lon, lat) in V2.EXPECTED_CHANGED


def test_check_refuses_other_kinds(tmp_path):
    json.dump({"kind": "lakes"}, open(tmp_path / "index.json", "w"))
    assert V2.check(str(tmp_path), log=lambda m: None) != []


def test_workflow_builds_coast_v2_with_pinned_shapely():
    """coast-build.yml set coast-v2: v1 from the pinned GSHHG, then v2 from the pinned tiles; shapely pinned in both pip
    lines (the output hash PIN_TIER1_SHA256 depends on its version, FORMAT_KEY records major.minor)."""
    wf = open(os.path.join(ROOT, ".github", "workflows", "coast-build.yml"), encoding="utf-8").read()
    assert "--v1 build/coast/v1 --tiles build/bathy-tiles --fetch --out build/coast/v2" in wf
    assert "coast-v2) CMD=(python tools/coast/build_coast_v2.py --out build/coast/v2 --upload static/coast/v2);;" in wf
    pips = [line for line in wf.splitlines() if "pip install" in line]
    assert len(pips) == 2 and all("shapely==2.2.0" in p and "pillow==" in p for p in pips)
    assert "|shapely=2.2" in V2.FORMAT_KEY or not shapely.__version__.startswith("2.2")


# ---------------------------------------------------------------- the upload gates (G70 P2-2)
def test_upload_refuses_every_unproven_build(world, tmp_path, monkeypatch):
    """--upload calls build_coast.upload only for a build that passes --check, comes from the pinned inputs (v1, the
    bathymetry digests and tile hashes) and matches PIN_TIER1_SHA256; every other case returns 1 without uploading."""
    tdir, v1, reg = world({(15, X0, Y0): island_with_lagoon(), (14, X0 >> 1, Y0 >> 1): full(WATER)})
    out = build(tmp_path, tdir, v1, reg)
    sha = json.load(open(os.path.join(out, "index.json")))["tier1"]["sha256"]
    calls = []
    monkeypatch.setattr(V2.BC, "upload", lambda *a, **k: calls.append(a) or True)
    args = ["--out", out, "--upload", "static/coast/v2"]
    # 1. not from the pinned inputs (a synthetic build): refused
    assert V2.main(args) == 1 and calls == []
    # 2. "real", but a region's tile hash unpinned: refused
    monkeypatch.setattr(V2, "is_real", lambda index, regions=None: True)
    monkeypatch.setattr(V2, "check", lambda *a, **k: [])
    monkeypatch.setattr(V2, "REGIONS", {n: dict(r, tiles_sha256=None) for n, r in V2.REGIONS.items()})
    assert V2.main(args) == 1 and calls == []
    # 3. tiles pinned, but no tier-1 pin / a different one: refused
    monkeypatch.setattr(V2, "REGIONS", {n: dict(r, tiles_sha256="x" * 64) for n, r in V2.REGIONS.items()})
    monkeypatch.setattr(V2, "PIN_TIER1_SHA256", None)
    assert V2.main(args) == 1 and calls == []
    monkeypatch.setattr(V2, "PIN_TIER1_SHA256", "0" * 64)
    assert V2.main(args) == 1 and calls == []
    # 4. a failing --check: refused, whatever the pins say
    monkeypatch.setattr(V2, "PIN_TIER1_SHA256", sha)
    monkeypatch.setattr(V2, "check", lambda *a, **k: ["a problem"])
    assert V2.main(args) == 1 and calls == []
    # 5. everything proven: one upload, of this directory, with its own LICENSE.txt
    monkeypatch.setattr(V2, "check", lambda *a, **k: [])
    assert V2.main(args) == 0 and len(calls) == 1 and calls[0][0] == out and calls[0][1] == "static/coast/v2"


def test_check_counts_v2_vertices_on_a_box_line(world, tmp_path, monkeypatch):
    """G70 P3-4: a real build with v2 land on a region box line is a problem (S1 has none)."""
    t = full(WATER)
    t[100:160, :] = LAND                       # runs to the box's east edge
    tdir, v1, reg = world({(15, X0 + 1, Y0): t, (14, (X0 + 1) >> 1, Y0 >> 1): full(WATER)})
    out = build(tmp_path, tdir, v1, reg)
    monkeypatch.setattr(V2, "is_real", lambda index, regions=None: True)
    probs = V2.check(out, log=lambda m: None, regions=reg)
    assert any("box line" in p for p in probs)
