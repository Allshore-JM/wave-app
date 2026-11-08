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
FIELDS = [("STATE", 2), ("ZONE", 3), ("CWA", 3), ("NAME", 24), ("STATE_ZONE", 5)]


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
    return ({"STATE": state, "ZONE": num, "CWA": "HFO", "NAME": name, "STATE_ZONE": state + num}, ring_list)


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
    lk = tmp_path / "coast_lakeoutlines-v1"
    lk.mkdir()
    (lk / "index.json").write_text(json.dumps({"tier1": {"cells": {}, "sha256": "lakes"}}))
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


def test_near_zone_finds_the_closest_outer_edge():
    nmap = {(int((-158.0 + 180) // A.GRID), int((21.0 + 90) // A.GRID)): {4}}
    assert A.near_zone(nmap, -158.0 + 0.004, 21.0 + 0.002) == 4      # 0.5 km away
    assert A.near_zone(nmap, -158.0 + 0.04, 21.0) is None            # 4 km away


# ---------------------------------------------------------------- the whole build
def test_build_cuts_the_island_coast_by_zone(tmp_path):
    cache = coast_cache(tmp_path, {"20_-160": [ISLAND]})
    zones, coast_ids = A.build(shapefile_zip(island_zones()), cache, log=lambda *a: None)
    assert set(zones) == {"HIZ001", "HIZ002"}               # CO003 is not a coastal state
    assert coast_ids == {"v1": "test", "lakeoutlines-v1": "lakes"}
    north = [np.array(A.decode(s)) for s in zones["HIZ001"]["lines"]]
    south = [np.array(A.decode(s)) for s in zones["HIZ002"]["lines"]]
    assert len(north) == 1                                  # the split between its two rows does not cut the line
    km = sum(A.length_km(P) for P in north)
    want = 2 * 0.5 * 111.2 + 1.0 * 111.2 * math.cos(math.radians(22.0))
    assert abs(km - want) / want < 0.03
    assert A.nearest_km(north, 22.0, -158.0) < 0.2 and A.nearest_km(north, 21.0, -158.0) > 50
    assert A.nearest_km(south, 21.0, -158.0) < 0.2 and A.nearest_km(south, 22.0, -158.0) > 50
    assert zones["HIZ002"]["name"] == "South B" and zones["HIZ001"]["cwa"] == "HFO"


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
    assert committed["rules"] == {"step_deg": A.STEP, "near_km": A.NEAR_KM, "tol_deg": A.TOL, "min_km": A.MIN_KM}
    assert A.check(committed, log=lambda *a: None) == []


def test_committed_file_covers_the_coastal_states(committed):
    states = {z[:2] for z in committed["zones"]}
    assert states >= {"HI", "CA", "OR", "WA", "AK", "TX", "FL", "NC", "NY", "MA", "ME", "MI", "WI", "PR", "GU", "AS"}
    assert all(z[2] == "Z" and z[3:].isdigit() and len(z) == 6 for z in committed["zones"])
