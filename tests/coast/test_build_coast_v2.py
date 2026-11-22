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
    for name, reg in V2.ALL_REGIONS.items():
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
    """coast-build.yml sets coast-v2 / coast-v3: v1 from the pinned GSHHG, then the set from the pinned tiles; shapely
    pinned in both pip lines (the output hash PIN_TIER1_SHA256 depends on its version, FORMAT_KEY records major.minor)."""
    wf = open(os.path.join(ROOT, ".github", "workflows", "coast-build.yml"), encoding="utf-8").read()
    assert ('--set "$V" --v1 build/coast/v1 --tiles build/bathy-tiles --fetch --out "build/coast/$V"' in wf
            and 'V="${SET#coast-}"' in wf)
    assert "coast-v2) CMD=(python tools/coast/build_coast_v2.py --out build/coast/v2 --upload static/coast/v2);;" in wf
    for v in sorted(V2.SETS):                          # every later set: built, kept as an artifact and uploaded
        if v == "v2":
            continue
        assert ("coast-%s) CMD=(python tools/coast/build_coast_v2.py --set %s --out build/coast/%s --upload static/coast/%s);;"
                % (v, v, v, v)) in wf
        assert "github.event.inputs.set == 'coast-%s' && '%s'" % (v, v) in wf
        assert wf.count("github.event.inputs.set == 'coast-%s'" % v) >= 4 and "coast-%s)" % v in wf
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


# ---------------------------------------------------------------- the coast sets (plan section 104)

def test_v2_set_is_the_published_s1_configuration():
    """Set v2 is exactly what static/coast/v2 was built with (the published bytes stay reproducible)."""
    st = V2.SETS["v2"]
    assert st["regions"] == ["hawaii", "nwhi-lidar"]
    assert st["pin"] == "9a0ca75f08d7b2fb6bc4d6df0a43cb1653ddabbcb1fd4b1b5cf77cc6d21923f7"
    assert st["v1_sqdeg"] == 1.456635 and st["same_cells"] == {}
    assert V2.format_key({n: V2.ALL_REGIONS[n] for n in st["regions"]}) == (
        "coast-v1-land2|base=coast-v1|z=15|block=8|tol=0.7|speck=2|q=100000|rule=tile>ancestor>gshhg|"
        "boxes=hawaii:v2:-160.5,18.5,-154.5,22.5;nwhi-lidar:v1:-168.5,22.5,-160.5,24.5|shapely=%s"
        % ".".join(shapely.__version__.split(".")[:2]))


def test_later_sets_keep_hawaii_and_add_regions():
    names = sorted(V2.SETS, key=lambda n: int(n[1:]))
    for prev_name, name in zip([None] + names[:-1], names):
        st = V2.SETS[name]
        assert set(st["regions"]) <= set(V2.ALL_REGIONS)
        cands = {c for r in st["regions"] for c in V2.cells_overlapping(V2.ALL_REGIONS[r]["box"])}
        assert set(st["expected_changed"]) <= cands, name
        for lon, lat, _ in st["probes"]:
            assert any(V2.ALL_REGIONS[r]["box"][0] <= lon <= V2.ALL_REGIONS[r]["box"][2] and
                       V2.ALL_REGIONS[r]["box"][1] <= lat <= V2.ALL_REGIONS[r]["box"][3] for r in st["regions"]), (lon, lat)
            assert cell_of(lon, lat) in st["expected_changed"], (name, lon, lat)
        if prev_name:                                   # each set builds on the one before it
            prev = V2.SETS[prev_name]
            assert set(prev["regions"]) <= set(st["regions"])
            assert sorted(st["same_cells"]) == sorted(prev["expected_changed"])
            assert not st["expected_changed"] or set(prev["expected_changed"]) <= set(st["expected_changed"])
            assert all(p in st["probes"] for p in prev["probes"])


def test_use_set_switches_and_restores():
    try:
        V2.use_set("v3")
        assert V2.SET == "v3" and "prvi" in V2.REGIONS and "prvi:v2:-68,17.25,-64.25,19" in V2.FORMAT_KEY
        assert V2.SAME_CELLS == V2.V2_HAWAII_CELLS and "Puerto Rico" in V2.NOAA_CREDIT
    finally:
        V2.use_set("v2")
    assert V2.SET == "v2" and set(V2.REGIONS) == {"hawaii", "nwhi-lidar"} and V2.SAME_CELLS == {}


def test_check_flags_a_hawaii_cell_that_is_not_v2s(world, tmp_path, monkeypatch):
    """A later set must carry Hawaii's published v2 cells byte for byte."""
    t = full(WATER)
    t[100:160, 20:200] = LAND
    tdir, v1, reg = world({(15, X0 + 1, Y0): t, (14, (X0 + 1) >> 1, Y0 >> 1): full(WATER)})
    out = build(tmp_path, tdir, v1, reg)
    idx = json.load(open(os.path.join(out, "index.json")))
    cell = idx["changed"][0]
    sha = hashlib.sha256(open(os.path.join(out, idx["tier1"]["dir"], cell + ".bin"), "rb").read()).hexdigest()
    monkeypatch.setattr(V2, "is_real", lambda index, regions=None: True)
    monkeypatch.setattr(V2, "SAME_CELLS", {cell: sha})
    assert not any("published v2 cell" in p for p in V2.check(out, log=lambda m: None, regions=reg))
    monkeypatch.setattr(V2, "SAME_CELLS", {cell: "0" * 64})
    assert any("%s: not byte-identical to the published v2 cell" % cell in p
               for p in V2.check(out, log=lambda m: None, regions=reg))


def test_a_region_may_set_its_own_speck_and_tolerance(world, tmp_path):
    """Section 104: a region's own "speck" (and "tol") replaces the defaults for its trace only; an islet of 16 px^2
    stays with the default (2) and goes with speck 32."""
    t = full(WATER)
    t[100:160, 20:200] = LAND                  # an island, kept either way
    t[30:34, 100:104] = LAND                   # a 4 x 4 px islet (16 px^2)
    tdir, v1, reg = world({(15, X0, Y0): t, (14, X0 >> 1, Y0 >> 1): full(WATER)})
    islet = (float(V2.px_lon(X0 * 256 + 102)), float(V2.px_lat(Y0 * 256 + 32)))
    assert land_at(build(tmp_path, tdir, v1, reg, "a"), *islet)
    reg2 = {n: dict(r, speck=32.0, tol=1.0) for n, r in reg.items()}
    out = build(tmp_path, tdir, v1, reg2, "b")
    assert not land_at(out, *islet)
    assert land_at(out, float(V2.px_lon(X0 * 256 + 110)), float(V2.px_lat(Y0 * 256 + 130)))


def test_v3_regions_carry_their_own_params_in_the_format_key():
    try:
        V2.use_set("v3")
        for n in ("prvi", "marianas", "samoa"):
            assert V2.REGIONS[n]["speck"] == 32.0 and V2.REGIONS[n]["tol"] == 1.0
            assert ":speck=32:tol=1" in V2.FORMAT_KEY.split(n + ":")[1].split(";")[0]
        assert "speck" not in V2.REGIONS["hawaii"] and "speck" not in V2.REGIONS["nwhi-lidar"]
    finally:
        V2.use_set("v2")


# ---------------------------------------------------------------- G71 fixes

class _Resp:
    def __init__(self, status, body=b""):
        self.status, self.reason, self.headers, self._b = status, "x", {}, body

    def read(self):
        return self._b


def _fake_http(monkeypatch, answers):
    """HTTPSConnection whose responses come from `answers` (a status, a (status, body) pair or an exception)."""
    made = []

    class Conn:
        def __init__(self, host, context=None, timeout=None):
            self.closed = False
            made.append(self)

        def request(self, method, path, headers=None):
            self.path = path
            assert headers["User-Agent"] == V2.UA

        def getresponse(self):
            a = answers.pop(0)
            if isinstance(a, Exception):
                raise a
            return _Resp(*a) if isinstance(a, tuple) else _Resp(a)

        def close(self):
            self.closed = True
    monkeypatch.setattr(V2.http.client, "HTTPSConnection", Conn)
    monkeypatch.setattr(V2.time, "sleep", lambda s: None)
    monkeypatch.setattr(V2, "_ssl_context", lambda: None)
    V2._conn.__dict__.clear()
    return made


def test_get_keeps_one_connection_and_retries_on_a_fresh_one(monkeypatch):
    import urllib.error
    made = _fake_http(monkeypatch, [(200, b"a"), (200, b"b"), ConnectionResetError(), (200, b"c"), 500, (200, b"d")])
    assert V2._get("https://h.example/x?y=1") == b"a" and made[0].path == "/x?y=1"
    assert V2._get("https://h.example/z") == b"b" and len(made) == 1          # kept alive
    assert V2._get("https://h.example/z") == b"c" and len(made) == 2 and made[0].closed   # a reset: a fresh connection
    assert V2._get("https://h.example/z") == b"d"                            # a 500 is retried
    made = _fake_http(monkeypatch, [404])
    with pytest.raises(urllib.error.HTTPError):
        V2._get("https://h.example/missing")
    made = _fake_http(monkeypatch, [301, (200, b"never")])
    with pytest.raises(urllib.error.HTTPError):                              # a redirect is not followed
        V2._get("https://h.example/moved")
    _fake_http(monkeypatch, [503, 503, 503, 503])
    with pytest.raises(urllib.error.HTTPError):
        V2._get("https://h.example/down", tries=4)
    V2._conn.__dict__.clear()


def test_upload_prefix_must_be_the_sets_own(world, tmp_path, monkeypatch):
    tdir, v1, reg = world({(15, X0, Y0): island_with_lagoon(), (14, X0 >> 1, Y0 >> 1): full(WATER)})
    out = build(tmp_path, tdir, v1, reg)
    calls = []
    monkeypatch.setattr(V2.BC, "upload", lambda *a, **k: calls.append(a) or True)
    monkeypatch.setattr(V2, "check", lambda *a, **k: [])
    monkeypatch.setattr(V2, "is_real", lambda index, regions=None: True)
    try:
        for args in (["--out", out, "--upload", "static/coast/v3"],                       # set v2 under v3's prefix
                     ["--set", "v3", "--out", out, "--upload", "static/coast/v2", "--replace"]):   # v3 over the live v2
            assert V2.main(args) == 1 and calls == [], args
    finally:
        V2.use_set("v2")
    assert V2.SET == "v2"


def test_v3_upload_gates(world, tmp_path, monkeypatch):
    """--set v3: a build that is not from v3's pinned inputs (a v2 build, a synthetic one) or not v3's pin is refused,
    and the set is v2 again afterwards (use_set restored)."""
    tdir, v1, reg = world({(15, X0, Y0): island_with_lagoon(), (14, X0 >> 1, Y0 >> 1): full(WATER)})
    out = build(tmp_path, tdir, v1, reg)
    calls = []
    monkeypatch.setattr(V2.BC, "upload", lambda *a, **k: calls.append(a) or True)
    args = ["--set", "v3", "--out", out, "--upload", "static/coast/v3"]
    try:
        assert V2.main(args) == 1 and calls == []                       # synthetic: not v3's inputs
        idx = json.load(open(os.path.join(out, "index.json")))
        assert not V2.is_real(idx, {n: V2.ALL_REGIONS[n] for n in V2.SETS["v2"]["regions"]})
        V2.use_set("v3")
        fake = dict(idx, bathy={n: {"v": r["v"], "digest": r["digest"], "sha256": r["tiles_sha256"]}
                                for n, r in V2.ALL_REGIONS.items() if n in V2.SETS["v2"]["regions"]},
                    base_v1={"tier0_sha": V2.V1_PIN_TIER0_SHA256, "tier1_sha": V2.V1_PIN_TIER1_SHA256})
        assert not V2.is_real(fake)                                      # a v2 build is not a v3 build
        fake["bathy"].update({n: {"v": r["v"], "digest": r["digest"], "sha256": r["tiles_sha256"]}
                              for n, r in V2.ALL_REGIONS.items() if n in V2.SETS["v3"]["regions"]})
        assert V2.is_real(fake)
    finally:
        V2.use_set("v2")
    assert V2.SET == "v2" and V2.PIN_TIER1_SHA256 == V2.SETS["v2"]["pin"] and V2.SAME_CELLS == {}


def test_index_records_the_regions_built(world, tmp_path):
    """G71 P3-7: build() and check() take the format key from the regions they are given, not the active set."""
    t = full(WATER)
    t[100:160, 20:200] = LAND
    tdir, v1, reg = world({(15, X0, Y0): t, (14, X0 >> 1, Y0 >> 1): full(WATER)})
    reg2 = {n: dict(r, speck=32.0, tol=1.0) for n, r in reg.items()}
    out = build(tmp_path, tdir, v1, reg2)
    idx = json.load(open(os.path.join(out, "index.json")))
    assert ":speck=32:tol=1" in idx["format_key"] and idx["format_key"] == V2.format_key(reg2)
    assert not any("format_key" in p for p in V2.check(out, log=lambda m: None, regions=reg2))
    assert any("format_key" in p for p in V2.check(out, log=lambda m: None, regions=reg))


# ---------------------------------------------------------------- seam regions (section 104, the mainland)

PXDEG = 360.0 / V2.WPX          # one zoom-15 pixel in longitude degrees


def _seam_world(world, margin):
    """A land band running east across a box line that cuts a pixel 0.6 px short of the tile edge, GSHHG's land
    continuing past the line (the mainland case: the box cuts the continent)."""
    t = full(LAND)
    t[:100, :] = WATER                         # water in the north part of the tile, land below
    full_box = tile_box(X0 - 1, Y0 - 1, X0 + 1, Y0 + 1)
    box = (full_box[0], full_box[1], full_box[2] - 0.6 * PXDEG, full_box[3])
    lat_n = centre(0, Y0 * 256 + 100)[1] + 0.0004
    lat_s = centre(0, Y0 * 256 + 255)[1] - 0.00002     # the tiles' land ends with tile Y0 (below: water)
    ring = [(box[2] - 0.0005, lat_s), (box[2] + 0.003, lat_s), (box[2] + 0.003, lat_n), (box[2] - 0.0005, lat_n)]
    tdir, v1, reg = world({(15, X0 + 1, Y0): t, (14, (X0 + 1) >> 1, Y0 >> 1): full(WATER)},
                          {cell_of(box[2], lat_n): [ring]}, box=box)
    reg = {n: dict(r, margin_px=margin) if margin else dict(r) for n, r in reg.items()}
    return tdir, v1, reg, box


def test_seam_region_meets_gshhg_exactly_on_an_off_grid_box_line(world, tmp_path):
    tdir, v1, reg, box = _seam_world(world, 2)
    out = build(tmp_path, tdir, v1, reg, "seam")
    lat = centre(0, Y0 * 256 + 200)[1]
    for dx in (-2e-5, -1e-6, 1e-6, 2e-5, 0.001):
        assert winding_at(out, box[2] + dx, lat) == 1, dx         # land on both sides, no crack, no double cover
    assert V2.check(out, log=lambda m: None, regions=reg) == []
    # (without the margin the traced land stops on the pixel grid, up to half a pixel short of the line: a crack the
    # output's 1e-5-degree quantum can hide or not, so the margin is what makes the join exact)


def test_seam_box_line_steps_are_measured(world, tmp_path, monkeypatch):
    """Where the traced shore and GSHHG's disagree at a seam box line, an edge runs along the line: measured, and a
    step longer than SEAM_STEP_MAX_M is a problem."""
    tdir, v1, reg, box = _seam_world(world, 2)
    out = build(tmp_path, tdir, v1, reg, "steps")
    msgs = []
    monkeypatch.setattr(V2, "is_real", lambda index, regions=None: True)
    monkeypatch.setattr(V2, "SEAM_STEP_MAX_M", 600)
    assert not any("seam box line" in p for p in V2.check(out, log=msgs.append, regions=reg))
    assert any("seam box lines:" in m for m in msgs)
    monkeypatch.setattr(V2, "SEAM_STEP_MAX_M", 1)
    assert any("seam box line" in p for p in V2.check(out, log=lambda m: None, regions=reg))


def test_exclusion_box_is_left_to_gshhg(world, tmp_path):
    """Section 104 (Puget Sound): inside a region's exclusion box GSHHG decides, whatever the tiles say."""
    t = full(WATER)
    t[100:160, 20:120] = LAND                  # an island the tiles have, in the west half
    box = tile_box(X0 - 1, Y0 - 1, X0 + 1, Y0 + 1)
    lon_mid = centre(X0 * 256 + 128, 0)[0]
    ex = (lon_mid, box[1], box[2], box[3])      # the east half of the box is excluded
    lat_a, lat_b = centre(0, Y0 * 256 + 60)[1], centre(0, Y0 * 256 + 40)[1]
    gs_lon0, gs_lon1 = centre(X0 * 256 + 170, 0)[0], centre(X0 * 256 + 200, 0)[0]
    ring = [(gs_lon0, lat_a), (gs_lon1, lat_a), (gs_lon1, lat_b), (gs_lon0, lat_b)]   # GSHHG's islet in the east half
    tdir, v1, reg = world({(15, X0, Y0): t, (14, X0 >> 1, Y0 >> 1): full(WATER)}, {cell_of(gs_lon0, lat_a): [ring]})
    reg = {n: dict(r, exclude=[ex]) for n, r in reg.items()}
    out = build(tmp_path, tdir, v1, reg)
    assert land_at(out, *centre(X0 * 256 + 60, Y0 * 256 + 130))                      # the tiles' island (not excluded)
    assert land_at(out, *centre(X0 * 256 + 185, Y0 * 256 + 50))                      # GSHHG's islet (excluded part)
    assert not land_at(out, *centre(X0 * 256 + 185, Y0 * 256 + 130))


def test_gshhg_fill_matches_the_per_pixel_test():
    """The quadtree fill (section 104) gives exactly the per-pixel contains_xy answer, also for pixel centres lying on
    the polygon's boundary (not inside) and on a ragged wanted set."""
    n = 300
    lons = -155.0 + (np.arange(n) + 0.5) * 0.001
    lats = 20.0 - (np.arange(n) + 0.5) * 0.001          # north to south, like a block's rows
    ring = [(lons[40], lats[250]), (lons[260], lats[250]), (lons[260], lats[60]), (lons[150], lats[20]),
            (lons[40], lats[60])]                       # vertices and edges on pixel centres
    gs = Polygon(ring, [[(lons[100], lats[200]), (lons[100], lats[120]), (lons[180], lats[120]), (lons[180], lats[200])]])
    shapely.prepare(gs)
    rng = np.random.default_rng(7)
    for want in (np.ones((n, n), bool), rng.random((n, n)) < 0.6):
        rr, cc = np.nonzero(want)
        old = np.zeros_like(want)
        hit = shapely.contains_xy(gs, lons[cc], lats[rr])
        old[rr[hit], cc[hit]] = True
        new = V2.gshhg_fill(gs, lons, lats, want)
        assert (old == new).all()
        assert old.any() and not old.all()


def test_split_big_cuts_only_big_pieces_on_the_split_lines(monkeypatch):
    """Section 104 rung a: a piece over SPLIT_VERTS is cut into its parts in the SPLIT_DEG squares (area kept, holes
    kept, every part inside one square); smaller pieces are left whole."""
    monkeypatch.setattr(V2, "SPLIT_VERTS", 500)
    t = np.linspace(0, 2 * np.pi, 2001)[:-1]
    r = 0.6 + 0.05 * np.sin(40 * t)
    big = Polygon(np.column_stack([-123.6 + r * np.cos(t), 46.6 + r * np.sin(t)]),
                  [np.column_stack([-123.6 + 0.1 * np.cos(-t[::20]), 46.6 + 0.1 * np.sin(-t[::20])])])
    small = Polygon([(-122.2, 46.1), (-121.9, 46.1), (-121.9, 46.4)])        # crosses -122 and 46.25, 4 vertices
    geom, cut = V2.split_big(shapely.MultiPolygon([big, small]))
    parts = V2.polygons(geom)
    assert cut == 1
    assert abs(sum(p.area for p in parts) - big.area - small.area) < 1e-12
    assert any(p.equals(small) for p in parts)
    for p in parts:
        if p.equals(small):
            continue
        x0, y0, x1, y1 = p.bounds
        assert np.floor(x0 / 0.25 + 1e-9) == np.ceil(x1 / 0.25 - 1e-9) - 1     # one column of squares
        assert np.floor(y0 / 0.25 + 1e-9) == np.ceil(y1 / 0.25 - 1e-9) - 1     # one row
    assert shapely.union_all(parts).symmetric_difference(shapely.MultiPolygon([big, small])).area < 1e-12


def test_split_is_recorded_for_seam_sets_only(world, tmp_path, monkeypatch):
    """index.json "split" (deg, cells cut) and the format key's split term come with a seam region only: the v2 / v3
    keys and indexes stay as published."""
    tdir, v1, reg, box = _seam_world(world, 2)
    monkeypatch.setattr(V2, "SPLIT_VERTS", 3)                 # every piece is "big": the band is cut where it can be
    out = build(tmp_path, tdir, v1, reg, "split")
    idx = json.load(open(os.path.join(out, "index.json")))
    assert idx["split"]["deg"] == V2.SPLIT_DEG and isinstance(idx["split"]["cells"], list)
    assert V2.format_key(reg).endswith("|split=%g>%d" % (V2.SPLIT_DEG, 3))
    assert V2.check(out, log=lambda m: None, regions=reg) == []
    for name in ("v2", "v3"):
        regs = {n: V2.ALL_REGIONS[n] for n in V2.SETS[name]["regions"]}
        assert "split" not in V2.format_key(regs)


def test_seam_steps_cancel_split_cuts_and_keep_one_way_stretches():
    """Edges along a seam box line: a split cut (both ways, land on both sides) cancels; a stretch one way only is a
    step, measured in metres (along x = const in latitude degrees, along y = const in longitude at that latitude)."""
    iv = {("45_-125", "y", 47.0): [(-124.0, -123.75, True), (-124.0, -123.75, False),
                                   (-123.70, -123.69, True)],
          ("45_-125", "x", -119.5): [(40.0, 40.25, False), (40.0, 40.25, True)]}
    steps = V2.seam_step_lengths(iv, q=100000)
    assert len(steps) == 1 and steps[0][0] == "45_-125"
    assert abs(steps[0][1] - 0.01 * 111320.0 * np.cos(np.radians(47.0))) < 1.0
    assert V2.seam_step_lengths({("c", "x", 1.0): [(0.0, 0.002, True)]}, q=100000) == [("c", 222.6)]
    assert V2.seam_step_lengths({("c", "x", 1.0): [(0.0, 0.000004, True)]}, q=100000) == []     # under half a unit


def test_exclusion_in_a_seam_region_keeps_v1_rings_exactly(world, tmp_path):
    """G72 #3: in a seam region the exclusion box is applied at the cell stage, so GSHHG's islet inside it comes out as
    v1's own ring (its vertices, q 1e4 scaled to 1e5), not a traced, speck-filtered, simplified copy of its pixels."""
    t = full(WATER)
    t[100:160, 20:120] = LAND
    box = tile_box(X0 - 1, Y0 - 1, X0 + 1, Y0 + 1)
    lon_mid = centre(X0 * 256 + 128, 0)[0]
    ex = (lon_mid, box[1], box[2], box[3])
    lat_a, lat_b = centre(0, Y0 * 256 + 60)[1], centre(0, Y0 * 256 + 40)[1]
    gs_lon0, gs_lon1 = centre(X0 * 256 + 170, 0)[0], centre(X0 * 256 + 200, 0)[0]
    ring = [(gs_lon0, lat_a), (gs_lon1, lat_a), (gs_lon1, lat_b), (gs_lon0, lat_b)]
    tdir, v1, reg = world({(15, X0, Y0): t, (14, X0 >> 1, Y0 >> 1): full(WATER)}, {cell_of(gs_lon0, lat_a): [ring]})
    reg = {n: dict(r, exclude=[ex], margin_px=2) for n, r in reg.items()}
    out = build(tmp_path, tdir, v1, reg, "exseam")
    d = BC.decode_file(open(os.path.join(out, "f", cell_of(gs_lon0, lat_a) + ".bin"), "rb").read())
    want = {(int(round(x * 1e4)) * 10, int(round(y * 1e4)) * 10) for x, y in ring}
    shells = [set(zip(pc["rings"][0][0].tolist(), pc["rings"][0][1].tolist())) for pc in d["pieces"]]
    assert want in shells
    assert land_at(out, *centre(X0 * 256 + 60, Y0 * 256 + 130))                      # the tiles' island stays
    assert V2.check(out, log=lambda m: None, regions=reg) == []


# ---------------------------------------------------------------- section 105: the East/Gulf two-tier regions

def _synthetic_band(name, x_lo, x_hi, y_lo, y_hi, pad=4):
    """A band dict (as load_band returns) whose region `name` has band px [x_lo, x_hi) x [y_lo, y_hi) at zoom 10."""
    x0, y0 = x_lo - pad, y_lo - pad
    w, h = x_hi - x_lo + 2 * pad, y_hi - y_lo + 2 * pad
    runs = [[y, x_lo - x0, x_hi - x0] for y in range(y_lo - y0, y_hi - y0)]
    return {"format": "coast-band-v1", "zoom": V2.BAND_Z, "r_km": 9.0, "n_km": 0.4, "force_in": [], "sha256": "s" * 64,
            "_cache": {}, "regions": {name: {"x0": x0, "y0": y0, "w": w, "h": h, "runs": runs}}}


def _two_tier_world(tmp_path, channel_rows=False):
    """One two-tier region on 3 x 3 zoom-15 tiles around X0/Y0, zoom 12 the coarsest. Land: a block across tile X0's
    middle rows; two water channels 8 zoom-15 px (one zoom-12 px) wide cut through it, at columns 96-103 (inside the
    band: tile X0's west half) and 200-207 (outside it). channel_rows: one horizontal channel crossing the band line
    instead (rows 120-127)."""
    X, Y = X0 * 256, Y0 * 256
    zx = X0 >> 3

    def cls_at(gx, gy):                          # class of global zoom-15 px arrays
        c = np.full(gx.shape, WATER, np.uint8)
        land = (gx >= X) & (gx < X + 256) & (gy >= Y + 64) & (gy < Y + 192)
        if channel_rows:
            land &= ~((gy >= Y + 120) & (gy < Y + 128))
        else:
            land &= ~((gx >= X + 96) & (gx < X + 104)) & ~((gx >= X + 200) & (gx < X + 208))
        c[land] = LAND
        return c
    tiles = {}
    for tx in range(X0 - 1, X0 + 2):
        for ty in range(Y0 - 1, Y0 + 2):
            gx, gy = np.meshgrid(tx * 256 + np.arange(256), ty * 256 + np.arange(256))
            tiles[(15, tx, ty)] = cls_at(gx, gy)
    for gyt in range((Y0 - 1) >> 3, ((Y0 + 1) >> 3) + 1):     # zoom 12: majority of each 8 x 8 zoom-15 block
        gx, gy = np.meshgrid(zx * 2048 + np.arange(2048), gyt * 2048 + np.arange(2048))
        c = cls_at(gx, gy).reshape(256, 8, 256, 8)
        tiles[(12, zx, gyt)] = np.where((c == LAND).sum(axis=(1, 3)) * 2 > 64, LAND, WATER).astype(np.uint8)
    tdir = str(tmp_path / "tiles")
    write_region(tdir, "us-test", 1, tiles)
    v1 = str(tmp_path / "v1")
    write_v1(v1, far_v1_cells())
    box = tile_box(X0 - 1, Y0 - 1, X0 + 1, Y0 + 1)
    reg = {"us-test": dict(v=1, box=box, zmin=12, digest="synthetic", tiles_sha256=None, speck=V2.S2_SPECK_PX2,
                           tol=V2.S2_TOL_PX, margin_px=V2.SEAM_MARGIN_PX, mode="two_tier")}
    # the band: tile X0's west half (a zoom-10 px = 32 zoom-15 px), all its rows
    band = _synthetic_band("us-test", X0 * 8, X0 * 8 + 4, Y0 * 8 - 8, Y0 * 8 + 16)
    return tdir, v1, reg, band


@pytest.fixture
def east_order(monkeypatch):
    monkeypatch.setattr(V2, "EAST_ORDER", ["us-test"] + V2.EAST_ORDER)


def test_two_tier_keeps_a_creek_in_the_band_and_closes_it_outside(tmp_path, monkeypatch, east_order):
    pytest.importorskip("scipy")
    tdir, v1, reg, band = _two_tier_world(tmp_path)
    monkeypatch.setattr(V2, "BAND", band)
    out = build(tmp_path, tdir, v1, reg)
    X, Y = X0 * 256, Y0 * 256
    assert land_at(out, *centre(X + 60, Y + 128))                  # land, inside the band (fine)
    assert not land_at(out, *centre(X + 99, Y + 128))              # the creek inside the band stays water (fine tier)
    assert land_at(out, *centre(X + 160, Y + 128))                 # land, outside the band (coarse)
    assert land_at(out, *centre(X + 203, Y + 128))                 # the creek outside the band closes (coarse tier)
    assert not land_at(out, *centre(X + 60, Y + 20))               # the sea, both tiers
    assert not land_at(out, *centre(X + 200, Y + 230))
    idx = json.load(open(os.path.join(out, "index.json")))
    assert idx["bathy"]["us-test"]["mode"] == "two_tier" and idx["band"]["coarse"]["z"] == 12
    assert V2.check(out, log=lambda m: None, regions=reg) == []


def test_two_tier_join_has_no_crack_on_the_band_line(tmp_path, monkeypatch, east_order):
    pytest.importorskip("scipy")
    tdir, v1, reg, band = _two_tier_world(tmp_path)
    monkeypatch.setattr(V2, "BAND", band)
    out = build(tmp_path, tdir, v1, reg)
    line = float(V2.band_px_lon(X0 * 8 + 4))
    lat = centre(0, Y0 * 256 + 100)[1]
    for dx in (-2e-5, -1e-6, 1e-6, 2e-5):
        assert winding_at(out, line + dx, lat) == 1, dx           # land both sides of the band line, covered once


def test_band_line_steps_are_measured(tmp_path, monkeypatch, east_order):
    """A creek crossing the band line: water in the fine tier, closed (land) in the coarse one -> an edge along the
    band line, one way: measured, and over SEAM_STEP_MAX_M it is a problem."""
    pytest.importorskip("scipy")
    tdir, v1, reg, band = _two_tier_world(tmp_path, channel_rows=True)
    monkeypatch.setattr(V2, "BAND", band)
    out = build(tmp_path, tdir, v1, reg)
    msgs = []
    monkeypatch.setattr(V2, "is_real", lambda index, regions=None: True)
    assert not any("seam box line" in p for p in V2.check(out, log=msgs.append, regions=reg))
    assert any("seam box/band lines:" in m for m in msgs)
    monkeypatch.setattr(V2, "SEAM_STEP_MAX_M", 1)
    assert any("seam box line" in p for p in V2.check(out, log=lambda m: None, regions=reg))


def test_two_tier_reads_only_the_tiles_the_band_needs(tmp_path, monkeypatch, east_order):
    tdir, v1, reg, band = _two_tier_world(tmp_path)
    monkeypatch.setattr(V2, "BAND", band)
    name, r = "us-test", reg["us-test"]
    blocks, want = V2.two_tier_expect(tdir, name, r)
    assert (X0 >> 3, Y0 >> 3) in blocks
    expect = {z: set() for z in range(12, 16)}
    for z, x, y in [t for t in V2.region_listed(tdir, name, r) if t[0] <= 12] + want:
        expect[z].add((x, y))
    t = V2.Tiles(tdir, name, r, expect=expect)                    # extra tiles on disk are ignored
    assert t.present[15] == expect[15]
    expect[15].add((X0 + 50, Y0))                                 # one the band needs but the disk lacks
    with pytest.raises(ValueError, match="lacks 1"):
        V2.Tiles(tdir, name, r, expect=expect)


def test_owned_boxes_follow_the_east_order():
    regs = V2.ALL_REGIONS
    ga = V2.owned_box("us-gulf-east", regs)
    sc = V2.owned_box("us-sc-ga", regs)
    assert sc.equals(sbox(*regs["us-sc-ga"]["box"]))               # us-sc-ga wins its corner
    assert not ga.intersects(sbox(-81.9, 30.8, -81.7, 31.1))       # the corner both boxes cover
    assert ga.area == pytest.approx(sbox(*regs["us-gulf-east"]["box"]).area - 0.4 * 0.5, rel=1e-9)
    for n in V2.EAST_ORDER:
        assert V2.ALL_REGIONS[n]["mode"] in ("full", "two_tier")


def test_band_rule_drops_a_narrow_mouthed_bay():
    pytest.importorskip("scipy")
    import make_coast_band as MB
    water = np.zeros((400, 400), bool)
    water[:, :200] = True                                         # the open sea, west
    water[150:250, 260:380] = True                                # a bay inland ...
    water[195:205, 200:260] = True                                # ... joined by a 10-px mouth
    band = MB.band_mask(water, px_m=100.0, r_km=2.0, n_km=0.4)    # 20-px disk, 4-px band
    assert band[200, 199] and band[200, 203]                       # the open coast and 0.4 km inland of it
    assert not band[200, 320]                                      # the bay: its mouth is narrower than 2 R
    assert not band[200, 230]                                      # the channel beyond N


def test_v5_band_file_is_pinned_and_covers_the_two_tier_regions():
    st = V2.SETS["v5"]
    band = V2.load_band(os.path.join(ROOT, "tools", "coast", st["band"]), st["band_sha256"])
    assert set(band["regions"]) == {n for n in V2.EAST_ORDER if V2.ALL_REGIONS[n]["mode"] == "two_tier"}
    assert band["order"] == V2.EAST_ORDER and band["r_km"] == 9.0 and band["n_km"] == 0.4
    assert band["bathy"] == {n: V2.ALL_REGIONS[n]["digest"] for n in V2.EAST_ORDER}
    assert len(band["force_in"]) == 2
    with pytest.raises(ValueError, match="pinned"):
        V2.load_band(os.path.join(ROOT, "tools", "coast", st["band"]), "0" * 64)


def test_v5_format_key_records_the_band_and_modes():
    try:
        V2.use_set("v5")
        assert ":mode=two_tier" in V2.FORMAT_KEY and ":mode=full" in V2.FORMAT_KEY
        assert "|band=%s|" % V2.SETS["v5"]["band_sha256"][:16] in V2.FORMAT_KEY and "coarse=z12" in V2.FORMAT_KEY
        V2.use_set("v4")
        assert "mode=" not in V2.FORMAT_KEY and "band=" not in V2.FORMAT_KEY and V2.BAND is None
    finally:
        V2.use_set("v2")


def test_seam_allow_names_one_stretch_only():
    step = ("30_-95", 854.0, "y", 30.6, -91.3071)
    allow = V2.SETS["v5"]["seam_allow"]
    assert V2.seam_allowed(step, allow)
    assert not V2.seam_allowed(("30_-95", 950.0, "y", 30.6, -91.3071), allow)        # longer than allowed
    assert not V2.seam_allowed(("30_-95", 854.0, "y", 30.6, -91.20), allow)          # elsewhere on the line
    assert not V2.seam_allowed(("30_-90", 854.0, "y", 30.6, -91.3071), allow)        # another cell
    iv = {("c", "y", 30.6): [(-91.31, -91.30, True)]}
    (d,) = V2.seam_step_lengths(iv, q=100000, detail=True)
    assert d[0] == "c" and d[2:4] == ("y", 30.6) and abs(d[4] + 91.305) < 1e-9
    assert V2.stale_allows([step], allow) == [] and V2.stale_allows([], allow) == list(allow)   # G73: no stale entry


def test_seam_chains_join_a_staircase_the_per_line_gate_misses():
    """G73 #2: a disagreement that follows a diagonal band boundary is many short one-way steps on alternate x and y
    lines (each well under 600 m), but one chain several km long; matched two-way edges and a named stretch drop out."""
    d = 0.001                                                   # ~111 m in latitude
    iv = {}
    for k in range(10):                                         # a staircase: up d on x = k*d, right d on y = (k+1)*d
        iv.setdefault(("c", "x", k * d), []).append((k * d, (k + 1) * d, True))
        iv.setdefault(("c", "y", (k + 1) * d), []).append((k * d, (k + 1) * d, True))
    steps = V2.seam_step_lengths(iv)
    assert max(m for _, m in steps) < 120
    chains = V2.seam_chains(iv)
    assert len(chains) == 1 and 2000 < chains[0][0] < 2300     # 20 legs of ~111 m (x legs a little shorter: cos lat)
    two_way = {k: v + [(lo, hi, not up) for lo, hi, up in v] for k, v in iv.items()}
    assert V2.seam_chains(two_way) == []                       # a split cut (land both sides) cancels
    allowed = {("c", "x", 0.0, d / 2)}
    assert len(V2.seam_chains(iv, allowed)) == 1 and V2.seam_chains(iv, allowed)[0][0] < chains[0][0]


def test_edge_fill_carries_the_data_across_a_box_line():
    """G73 #3: the bathy tiles mark the pixels on a region's box line 'outside'; edge_fill gives those within px of the
    line the nearest data class, so a river stays water and a beach stays land across the line, and pixels farther
    than px from the line stay 'outside' (GSHHG fills them)."""
    O, L, W = V2.OUT, V2.LAND, V2.WATER
    n = 20
    lons = np.arange(n) * 0.001 - 80.0
    lats = 30.0 - np.arange(n) * 0.001
    cls = np.full((n, n), L, np.uint8)
    cls[8:12, :] = W                                           # a river running east
    cls[:, 0:3] = W                                            # the sea to the west
    cls[:, 10:] = O                                            # the box line at column 10; outside beyond it
    box = (lons[0] - 1.0, lats[-1] - 1.0, lons[10], lats[0] + 1.0)
    before = cls.copy()
    k = V2.edge_fill(cls, lons, lats, box)
    assert k == 3 * n                                          # columns 10-12: the line and 2 px beyond
    assert (cls[8:12, 10:13] == W).all() and (cls[:8, 10:13] == L).all() and (cls[12:, 10:13] == L).all()
    assert (cls[:, 13:] == O).all() and (cls[:, :10] == before[:, :10]).all()
    far = np.full((n, n), O, np.uint8); far[:, :3] = L         # data only far from every box line: nothing filled
    assert V2.edge_fill(far, lons, lats, box) == 0 and (far[:, 3:] == O).all()
