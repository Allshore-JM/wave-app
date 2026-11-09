"""The coastal hazards layer's zone coastlines (tools/alerts/build_alert_coast.py, plan section 90): the zone
shapefile reader, the coastline cut and the encoded lines, on synthetic inputs; and the committed alert_coast.json."""
import io
import json
import math
import os
import struct
import sys
import zipfile

import numpy as np
import pytest

pytest.importorskip("matplotlib")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "alerts"))
sys.path.insert(0, os.path.join(ROOT, "tools", "coast"))

import build_alert_coast as A  # noqa: E402
import build_coast as C  # noqa: E402


# ---------------------------------------------------------------- synthetic inputs
FIELDS = [("STATE", 2), ("ZONE", 3), ("CWA", 3), ("NAME", 24), ("STATE_ZONE", 5), ("TIME_ZONE", 2)]


def shapefile_zip(rows):
    """rows: [(dict of FIELDS, [ring (n x 2 lon / lat, closed)])] -> the zip bytes of a .shp + .dbf pair."""
    recs = []
    for i, (_row, rings) in enumerate(rows):
        pts = np.concatenate([np.asarray(r, float) for r in rings])
        parts, o = [], 0
        for r in rings:
            parts.append(o)
            o += len(r)
        body = struct.pack("<i4d", 5, pts[:, 0].min(), pts[:, 1].min(), pts[:, 0].max(), pts[:, 1].max())
        body += struct.pack("<ii", len(rings), len(pts)) + struct.pack("<%di" % len(parts), *parts)
        body += pts.astype("<f8").tobytes()
        recs.append(struct.pack(">ii", i + 1, len(body) // 2) + body)
    shp = struct.pack(">i", 9994) + b"\0" * 20 + struct.pack(">i", (100 + sum(map(len, recs))) // 2)
    shp += struct.pack("<ii", 1000, 5) + b"\0" * 64
    shp += b"".join(recs)
    rlen = 1 + sum(n for _, n in FIELDS)
    hlen = 32 + 32 * len(FIELDS) + 1
    dbf = struct.pack("<B3BIHH", 3, 26, 1, 1, len(rows), hlen, rlen) + b"\0" * 20
    for name, n in FIELDS:
        dbf += name.encode("ascii").ljust(11, b"\0") + b"C" + b"\0" * 4 + bytes([n]) + b"\0" * 15
    dbf += b"\r"
    for row, _rings in rows:
        dbf += b" " + b"".join(str(row[name]).encode("latin-1").ljust(n)[:n] for name, n in FIELDS)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("z_test.shp", shp)
        z.writestr("z_test.dbf", dbf)
    return buf.getvalue()


def box(w, e, s, n):
    return [(w, s), (e, s), (e, n), (w, n), (w, s)]


def zone(state, num, name, ring_list):
    return ({"STATE": state, "ZONE": num, "CWA": "HFO", "NAME": name, "STATE_ZONE": state + num, "TIME_ZONE": "H"},
            ring_list)


def coast_cache(tmp_path, cells):
    """A coast cache folder holding the given land rings: {"20_-160": [ring lon / lat (CCW, not closed)]}."""
    d = tmp_path / "coast_v1"
    d.mkdir()
    have = {}
    for name, rings in cells.items():
        pieces = []
        for r in rings:
            r = np.asarray(r, float)
            pieces.append([C.quantize_ring(r[:, 0], r[:, 1])])
        buf = C.encode_file(pieces, 5)
        (d / (name + ".bin")).write_bytes(buf)
        have[name] = [len(buf), sum(len(r) for r in rings)]
    (d / "index.json").write_text(json.dumps({"tier1": {"cells": have, "sha256": "test"}}))
    for name, sha in (("lakeoutlines-v1", "lakes"), ("lakes-v1", "great")):
        lk = tmp_path / ("coast_" + name)
        lk.mkdir()
        (lk / "index.json").write_text(json.dumps({"tier1": {"cells": {}, "sha256": sha}}))
    return str(tmp_path)


ISLAND = [(-158.5, 21.0), (-157.5, 21.0), (-157.5, 22.0), (-158.5, 22.0)]      # CCW


def island_zones():
    """North half: two rows of one zone (split at -158.0), drawn past the shore; south half: a zone that stops 0.44 km
    inside the shore (so its coast is reached by the near rule only); an inland state's zone (left out)."""
    return [
        zone("HI", "001", "North A", [box(-158.6, -158.0, 21.5, 22.1)]),
        zone("HI", "001", "North A", [box(-158.0, -157.4, 21.5, 22.1)]),
        zone("HI", "002", "South B", [box(-158.496, -157.504, 21.004, 21.5)]),
        zone("CO", "003", "Inland", [box(-158.6, -157.4, 20.0, 23.0)]),
    ]


# ---------------------------------------------------------------- unit pieces
def test_polyline_encoding_matches_googles_example():
    pts = [(-120.2, 38.5), (-120.95, 40.7), (-126.453, 43.252)]
    s = A.encode(pts)
    assert s == "_p~iF~ps|U_ulLnnqC_mqNvxq`@"
    assert A.decode(s) == pytest.approx(pts)


def test_polyline_round_trip_and_bad_strings():
    rng = np.random.default_rng(3)
    P = np.cumsum(rng.normal(0, 0.01, (300, 2)), axis=0) + (-157.9, 21.4)
    back = np.array(A.decode(A.encode(P)))
    assert np.abs(back - P).max() <= 0.5e-5 + 1e-12
    for bad in ["_p~iF~ps|U_", "\x1f", "~~~~~~~~~~"]:
        with pytest.raises(ValueError):
            A.decode(bad)


def test_ugc_ids():
    assert A.ugc({"STATE": "HI", "ZONE": "007"}) == "HIZ007"


def test_zone_time_zones_from_nws_letters():
    assert A.zone_tz({"STATE": "HI", "TIME_ZONE": "H"}) == "Pacific/Honolulu"
    assert A.zone_tz({"STATE": "AK", "TIME_ZONE": "h"}) == "America/Adak"
    assert A.zone_tz({"STATE": "FL", "TIME_ZONE": "CE"}) == "America/Chicago"
    assert A.zone_tz({"STATE": "MP", "TIME_ZONE": "G"}) == "Pacific/Saipan"
    assert A.zone_tz({"STATE": "GU", "TIME_ZONE": "G"}) == "Pacific/Guam"
    assert A.zone_tz({"STATE": "PR", "TIME_ZONE": "V"}) == "America/Puerto_Rico"
    assert A.zone_tz({"STATE": "XX", "TIME_ZONE": "Q"}) is None


def test_shapefile_reader_rows_and_rings():
    rows, shapes = A.read_shapefile(shapefile_zip(island_zones()))
    assert [r["STATE_ZONE"] for r in rows] == ["HI001", "HI001", "HI002", "CO003"]
    assert rows[2]["NAME"] == "South B" and rows[2]["CWA"] == "HFO"
    assert len(shapes) == 4 and all(len(s) == 1 for s in shapes)
    assert np.allclose(shapes[2][0][0], (-158.496, 21.004))
    with pytest.raises(ValueError):
        A.read_shapefile(b"PK not a zip")


def test_edge_counts_mark_shared_edges():
    rows, shapes = A.read_shapefile(shapefile_zip(island_zones()))
    cnt = A.edge_counts(shapes[:3])
    shared = A._edge_keys(np.array([(-158.0, 21.5), (-158.0, 22.1)]))[1][0]
    assert cnt[shared] == 2                                  # the split between the two rows of HIZ001
    outer = A._edge_keys(np.array([(-158.496, 21.004), (-157.504, 21.004)]))[1][0]
    assert cnt[outer] == 1


def test_densify_flags_new_lines_and_cell_line_edges():
    # one ring (edge = j -> i): coast along 21 N and 159 W, then the builder's clip along 25 N and 160 W (cell lines)
    xi = np.array([-159.0, -159.0, -160.0, -160.0])
    yi = np.array([21.0, 25.0, 25.0, 21.0])
    xj = np.array([-160.0, -159.0, -159.0, -160.0])
    yj = np.array([21.0, 21.0, 25.0, 25.0])
    x, y, start, skip = A.densify((xi, yi, xj, yj), step=0.5)
    assert len(x) == 20 and start[0] and not start[1:].any()  # one ring, continuous
    assert skip.tolist() == [False] * 10 + [True] * 10
    assert (x[0], y[0]) == (-160.0, 21.0) and (x[9], y[9]) == (-159.0, 24.5)
    # a second ring that does not continue the first starts a new line
    x2, y2, start2, _ = A.densify((np.r_[xi, -150.0], np.r_[yi, 21.0], np.r_[xj, -151.0], np.r_[yj, 21.0]), step=0.5)
    assert start2.tolist() == [True] + [False] * 19 + [True, False]


def test_runs_join_a_ring_across_its_start_point():
    # a closed square ring of 8 points, 0.001 apart; zone 0 holds the end and the start, zone 1 the middle
    x = np.array([0, 1, 2, 2, 2, 1, 0, 0], float) * 0.001
    y = np.array([0, 0, 0, 1, 2, 2, 2, 1], float) * 0.001
    start = np.zeros(8, bool)
    start[0] = True
    skip = np.zeros(8, bool)
    zone = np.array([0, 0, 1, 1, 1, 1, 0, 0])
    got = A.runs(x, y, start, skip, zone, step=0.001)
    assert [(z, len(P)) for z, P in got] == [(0, 4), (1, 4)]
    assert got[0][1].tolist() == [[0, 0.002], [0, 0.001], [0, 0], [0.001, 0]]
    # not closed (the last point far from the first): two lines
    x2 = x.copy()
    x2[-2:] += 0.01
    assert [(z, len(P)) for z, P in A.runs(x2, y, start, skip, zone, step=0.001)] == [(0, 2), (1, 4), (0, 2)]


def test_runs_never_cross_a_line_start_or_zone_change():
    x = np.arange(10.0)
    y = np.zeros(10)
    start = np.zeros(10, bool)
    start[0] = start[6] = True
    skip = np.zeros(10, bool)
    zone = np.array([0, 0, 0, 1, 1, 1, 1, 1, -1, -1])
    got = [(z, P[:, 0].tolist()) for z, P in A.runs(x, y, start, skip, zone)]
    assert got == [(0, [0.0, 1.0, 2.0]), (1, [3.0, 4.0, 5.0]), (1, [6.0, 7.0])]


def test_simplify_keeps_ends_and_corners():
    P = np.array([(0, 0), (0.5, 0.0001), (1, 0), (1, 0.5), (1, 1)], float)
    S = A.simplify(P, tol=0.001)
    assert S.tolist() == [[0, 0], [1, 0], [1, 1]]


def _near(edges):
    """A near lookup from {zone index: [(x0, y0, x1, y1)]} (every edge an outer edge)."""
    pieces = [(zi, [np.array([(a, b), (c, d)])]) for zi, es in edges.items() for a, b, c, d in es]
    cnt = A.edge_counts([rings for _zi, rings in pieces])
    return A.near_map(pieces, cnt)


def test_near_zone_finds_the_closest_outer_edge():
    near = _near({4: [(-158.0, 21.0, -158.0, 21.01)]})
    q = A.near_zone(near, -158.0 + 0.004, 21.005)                    # 0.42 km east of the edge
    assert q[0] == 4 and q[1] == pytest.approx(-158.0) and q[2] == pytest.approx(21.005)
    assert q[3] == pytest.approx(0.004 * 111.2 * math.cos(math.radians(21.005)), rel=1e-3)
    assert A.near_zone(near, -158.0 + 0.04, 21.005) is None          # 4 km away


def test_near_zone_takes_the_nearest_zone_not_the_lowest_id():
    # G59 F2: the old rule searched square rings of grid cells and took the lowest zone id among the first ring found
    near = _near({1: [(-158.0, 21.0, -158.0, 21.01)], 2: [(-157.99, 21.0, -157.99, 21.01)]})
    assert A.near_zone(near, -157.9915, 21.005)[0] == 2              # 0.16 km from zone 2, 0.88 km from zone 1
    assert A.near_zone(near, -157.9985, 21.005)[0] == 1
    k = math.cos(math.radians(21.005))                               # the reach is km, not grid cells
    assert A.near_zone(near, -158.0 - 1.45 / (111.2 * k), 21.005) is not None
    assert A.near_zone(near, -158.0 - 1.55 / (111.2 * k), 21.005) is None


def test_near_ok_refuses_a_way_along_the_coast_or_across_water():
    q = (0, -158.0, 21.01, 1.112)                                    # the edge 1.11 km due north
    assert A.near_ok(-158.0, 21.0, q, tangent=(1.0, 0.0))            # coast runs east-west: straight inland, fine
    assert not A.near_ok(-158.0, 21.0, q, tangent=(0.0, 1.0))        # coast runs north: the way follows the coast
    assert not A.near_ok(-158.0, 21.0, q, tangent=(1.0, 0.0), water=lambda lo, la: la > 21.004)
    assert A.near_ok(-158.0, 21.0, q, tangent=(1.0, 0.0), water=lambda lo, la: np.zeros(len(lo), bool))
    # within NEAR_END_KM of either end the water test does not look (the raster's error beside a shore)
    seen = []
    A.near_ok(-158.0, 21.0, q, water=lambda lo, la: seen.extend(la.tolist()) or np.zeros(len(lo), bool))
    assert seen and all(21.0013 < v < 21.0087 for v in seen)
    seen.clear()
    assert A.near_ok(-158.0, 21.0, (0, -158.0, 21.0018, 0.2), water=lambda lo, la: seen.extend(la) or lo > 0)
    assert not seen                                                  # a 0.2-km way: nothing far enough from both ends
    assert A.near_ok(-158.0, 21.0, (0, -158.0, 21.0002, 0.022), tangent=(0.0, 1.0))   # within 50 m: as is


def _edges(rings):
    """decode_coast-style edges (edge = vertex j -> vertex i) of closed rings given without the closing point."""
    xi, yi, xj, yj = [], [], [], []
    for r in rings:
        r = np.asarray(r, float)
        nxt = np.roll(r, -1, axis=0)
        xj += r[:, 0].tolist()
        yj += r[:, 1].tolist()
        xi += nxt[:, 0].tolist()
        yi += nxt[:, 1].tolist()
    return tuple(np.array(v) for v in (xi, yi, xj, yj))


def test_raster_fills_rings_even_odd_with_holes():
    # a 1-degree square ring with a 0.2-degree square hole (reversed), inside cell 20_-160
    outer = [(-159.0, 21.0), (-158.0, 21.0), (-158.0, 22.0), (-159.0, 22.0)]
    hole = [(-158.6, 21.4), (-158.6, 21.6), (-158.4, 21.6), (-158.4, 21.4)]
    e = _edges([outer, hole])
    g = A.raster(e, 20, -160, res=0.05)

    def at(lat, lon):
        return bool(g[int((lat - 20) / 0.05), int((lon + 160) / 0.05)])
    assert at(21.2, -158.8) and at(21.9, -158.1)
    assert not at(21.5, -158.5) and not at(20.9, -158.5) and not at(21.5, -157.9)
    assert g.sum() == 20 * 20 - 4 * 4
    t = A.raster(e, 21, -159, res=0.05, size=1)                      # a 1-degree tile of it
    assert t.shape == (20, 20) and t.sum() == 20 * 20 - 16


def test_lake_islands_keeps_only_rings_the_outlines_lack():
    lake = [(-83.0, 45.0), (-82.0, 45.0), (-82.0, 46.0), (-83.0, 46.0)]
    big = [(-82.8, 45.2), (-82.8, 45.4), (-82.6, 45.4), (-82.6, 45.2)]     # an island lakeoutlines holds too
    small = [(-82.3, 45.7), (-82.3, 45.72), (-82.28, 45.72), (-82.28, 45.7)]
    got = A.lake_islands(_edges([lake, big, small]), _edges([lake, big]))
    assert len(got[0]) == 4 and np.allclose(sorted(got[0]), sorted(_edges([small])[0]))
    assert A.lake_islands(None, _edges([lake])) is None
    assert len(A.lake_islands(_edges([small]), None)[0]) == 4


class _Grid:
    """A Land stand-in: land where f(lon, lat)."""
    def __init__(self, f):
        self.f = f

    def at(self, lon, lat, shore=True):
        return self.f(np.asarray(lon), np.asarray(lat))


def test_open_sea_needs_one_long_line_out_to_sea_within_the_fan():
    # a coast along 21 N facing south; a lagoon shore facing south across a 5-km lagoon to a barrier island
    x, y, nx, ny = np.array([-158.0]), np.array([21.0]), np.array([0.0]), np.array([-1.0])
    assert A.open_sea(_Grid(lambda lo, la: la > 21.0), x, y, nx, ny)[0]
    barrier = _Grid(lambda lo, la: (la > 21.0) | ((la < 20.955) & (la > 20.94)))
    assert not A.open_sea(barrier, x, y, nx, ny)[0]
    # the barrier ends 3 km east: a line within 60 degrees of straight out escapes past it
    gap = _Grid(lambda lo, la: (la > 21.0) | ((la < 20.955) & (la > 20.94) & (lo < -157.97)))
    assert A.open_sea(gap, x, y, nx, ny)[0]
    assert not A.open_sea(gap, x, y, nx, ny, fan=0)[0]
    # 99 km of water, then land: not open; 101 km: open
    assert not A.open_sea(_Grid(lambda lo, la: (la > 21.0) | (la < 21.0 - 99.0 / 111.2)), x, y, nx, ny, fan=0)[0]
    assert A.open_sea(_Grid(lambda lo, la: (la > 21.0) | (la < 21.0 - 101.0 / 111.2)), x, y, nx, ny, fan=0)[0]


def test_even_out_fills_short_gaps_and_drops_short_pieces():
    n = 60                                                          # 60 points 0.1 km apart along the equator
    x = np.arange(n) * (0.1 / 111.2)
    y = np.zeros(n)
    start = np.zeros(n, bool)
    start[0] = True
    elig = np.ones(n, bool)
    f = np.ones(n, bool)
    f[10:15] = False                                                # a 0.5-km closed gap between open: filled
    f[30:] = False                                                  # a 3-km closed stretch: kept ...
    f[52:56] = True                                                 # ... with a 0.4-km open speck in it: dropped
    got = A.even_out(f, x, y, start, elig)
    assert got[:30].all() and not got[30:].any()
    # a gap beside an ineligible point is not filled
    e2 = elig.copy()
    e2[16] = False
    f2 = np.ones(n, bool)
    f2[12:16] = False
    assert not A.even_out(f2, x, y, start, e2)[12:16].any()


def test_tangents_never_cross_a_ring_start():
    x = np.array([0, 1, 2, 3, 10, 10, 10], float) * 0.001
    y = np.array([0, 0, 0, 0, 0, 1, 2], float) * 0.001
    start = np.array([1, 0, 0, 0, 1, 0, 0], bool)
    tx, ty = A.tangents(x, y, start, pts=3)
    assert np.allclose(tx[:4], 1) and np.allclose(ty[:4], 0)
    assert np.allclose(tx[4:], 0) and np.allclose(ty[4:], 1)


# ---------------------------------------------------------------- the whole build
def test_build_cuts_the_island_coast_by_zone(tmp_path):
    cache = coast_cache(tmp_path, {"20_-160": [ISLAND]})
    zones, coast_ids = A.build(shapefile_zip(island_zones()), cache, log=lambda *a: None)
    assert set(zones) == {"HIZ001", "HIZ002"}               # CO003 is not a coastal state
    assert coast_ids == {"v1": "test", "lakeoutlines-v1": "lakes", "lakes-v1": "great"}
    north = [np.array(A.decode(s)) for s in zones["HIZ001"]["lines"]]
    south = [np.array(A.decode(s)) for s in zones["HIZ002"]["lines"]]
    assert len(north) == 1                                  # the split between its two rows does not cut the line
    km = sum(A.length_km(P) for P in north)
    want = 2 * 0.5 * 111.2 + 1.0 * 111.2 * math.cos(math.radians(22.0))
    assert abs(km - want) / want < 0.03
    assert A.nearest_km(north, 22.0, -158.0) < 0.2 and A.nearest_km(north, 21.0, -158.0) > 50
    assert A.nearest_km(south, 21.0, -158.0) < 0.2 and A.nearest_km(south, 22.0, -158.0) > 50
    assert zones["HIZ002"]["name"] == "South B" and zones["HIZ001"]["cwa"] == "HFO"
    assert zones["HIZ001"]["tz"] == "Pacific/Honolulu"


def test_build_leaves_out_coast_beyond_the_near_reach(tmp_path):
    cache = coast_cache(tmp_path, {"20_-160": [ISLAND]})
    far = [zone("HI", "002", "Far", [box(-158.4, -157.6, 21.1, 21.9)])]           # 10 km inside every shore
    zones, _ = A.build(shapefile_zip(far), cache, log=lambda *a: None)
    assert zones == {}


def test_write_and_check(tmp_path):
    cache = coast_cache(tmp_path, {"20_-160": [ISLAND]})
    zones, coast_ids = A.build(shapefile_zip(island_zones()), cache, log=lambda *a: None)
    out = tmp_path / "alert_coast.json"
    doc = A.write(zones, coast_ids, "abc", str(out))
    back = json.loads(out.read_text(encoding="utf-8"))
    assert back == doc and back["format"] == A.FORMAT and back["zones_sha256"] == "abc"
    probs = A.check(back, log=lambda *a: None)
    assert any("zones (expected" in p for p in probs)       # two zones, not a real build
    assert any(p.startswith("probe HIZ007") for p in probs)


# ---------------------------------------------------------------- the committed file
@pytest.fixture(scope="module")
def committed():
    with open(os.path.join(ROOT, "alert_coast.json"), encoding="utf-8") as f:
        return json.load(f)


def test_committed_file_passes_its_check(committed):
    assert committed["zones_file"] == A.ZONES_FILE and committed["zones_sha256"] == A.ZONES_SHA256
    assert committed["rules"] == A.RULES
    assert A.check(committed, log=lambda *a: None) == []


def test_committed_file_covers_the_coastal_states(committed):
    states = {z[:2] for z in committed["zones"]}
    assert states >= {"HI", "CA", "OR", "WA", "AK", "TX", "FL", "NC", "NY", "MA", "ME", "MI", "WI", "PR", "GU", "AS"}
    assert all(z[2] == "Z" and z[3:].isdigit() and len(z) == 6 for z in committed["zones"])
