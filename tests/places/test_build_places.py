"""Places builder (tools/places/build_places.py): Natural Earth border lines and places, GeoNames towns, the lines-v1
and places-v1 formats, the cell cut and the --check gate. Synthetic sources only (no network)."""
import hashlib
import io
import json
import math
import os
import re
import sys
import zipfile

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "places"))

import build_places as P  # noqa: E402

Q = P.Q
QUIET = dict(log=lambda *a: None)


# ---------------------------------------------------------------- names and ranks

def test_clean_name_and_fold():
    assert P.clean_name("  Hale‘iwa\n\tTown  ") == "Hale‘iwa Town"
    assert P.clean_name(None) == "" and P.clean_name("\x00\x07") == ""
    long = "a" + "é" * 40                                       # 1 + 80 bytes
    cut = P.clean_name(long)
    assert len(cut.encode("utf-8")) == 59 and cut.endswith("é")  # never half a character
    assert P.fold("Hale‘iwa") == "haleiwa" and P.fold("Wahiawā") == "wahiawa" and P.fold("São Paulo") == "saopaulo"
    assert P.fold(None) == ""


def test_rank_of():
    assert P.rank_of(10, capital=True) == 0 and P.rank_of(5_000_000, state_capital=True) == 1
    assert [P.rank_of(p) for p in (2_000_000, 1_000_000, 999_999, 100_000, 10_000, 9_999, 0)] == [2, 2, 3, 3, 4, 5, 5]


# ---------------------------------------------------------------- line geometry

def test_split_dateline_cuts_at_the_exact_latitude_both_ways():
    runs = P.split_dateline([170.0, -170.0], [0.0, 10.0])
    assert len(runs) == 2
    (ax, ay), (bx, by) = runs
    assert ax.tolist() == [170.0, 180.0] and np.isclose(ay[-1], 5.0)
    assert bx.tolist() == [-180.0, -170.0] and by[0] == ay[-1] and by[-1] == 10.0
    runs = P.split_dateline([-175.0, 175.0, 170.0], [0.0, 2.0, 4.0])
    assert runs[0][0].tolist() == [-175.0, -180.0] and runs[1][0].tolist() == [180.0, 175.0, 170.0]
    assert np.isclose(runs[0][1][-1], 1.0) and runs[1][1][0] == runs[0][1][-1]
    x, y = np.array([1.0, 2.0, 3.0]), np.array([0.0, 0.0, 1.0])
    assert P.split_dateline(x, y)[0][0] is not None and len(P.split_dateline(x, y)) == 1


def test_simplify_keeps_the_ends_and_the_bumps():
    x = np.linspace(0, 10, 101); y = np.zeros(101); y[50] = 0.5
    sx, sy = P.simplify(x, y, 0.1)
    assert np.allclose(sx, [0.0, 4.9, 5.0, 5.1, 10.0]) and np.allclose(sy, [0.0, 0.0, 0.5, 0.0, 0.0])
    sx, sy = P.simplify(x, np.zeros(101), 0.1)
    assert sx.tolist() == [0.0, 10.0]
    lx, ly = P.simplify(np.array([0.0, 1.0]), np.array([0.0, 1.0]), 1.0)
    assert lx.tolist() == [0.0, 1.0]


def test_split_line_cells_cuts_exactly_on_the_cell_lines_and_runs_join():
    runs = P.split_line_cells([5.0, 25.0], [5.0, 15.0], 10)
    assert [(r[0], r[1]) for r in runs] == [(0, 0), (0, 10), (10, 10), (10, 20)]
    for (la0, lo0, x, y) in runs:
        assert x.min() >= lo0 and x.max() <= lo0 + 10 and y.min() >= la0 and y.max() <= la0 + 10
    for a, b in zip(runs, runs[1:]):
        assert a[2][-1] == b[2][0] and a[3][-1] == b[3][0]                       # the same point ends one and starts the next
    assert runs[0][2][-1] == 10.0 and runs[1][3][-1] == 10.0 and runs[2][2][-1] == 20.0
    length = sum(np.hypot(np.diff(r[2]), np.diff(r[3])).sum() for r in runs)
    assert math.isclose(length, math.hypot(20, 10))
    along = P.split_line_cells([10.0, 10.0], [2.0, 8.0], 10)                     # along a cell line: the cell to its east
    assert [(r[0], r[1]) for r in along] == [(0, 10)]
    touch = P.split_line_cells([5.0, 10.0, 15.0], [1.0, 2.0, 3.0], 10)          # a vertex on the line: no new point
    assert [(r[0], r[1]) for r in touch] == [(0, 0), (0, 10)] and len(touch[0][2]) == 2 and touch[1][2][0] == 10.0
    neg = P.split_line_cells([-175.0, -165.0], [-5.0, 5.0], 10)
    assert [(r[0], r[1]) for r in neg] == [(-10, -180), (0, -170)]
    edge = P.split_line_cells([175.0, 180.0], [0.0, 0.0], 10)                   # ends on the antimeridian: still cell 170
    assert [(r[0], r[1]) for r in edge] == [(0, 170)]
    west = P.split_line_cells([10.0, 5.0], [5.0, 5.0], 10)                      # starts on a cell line, runs west: cell 0
    south = P.split_line_cells([5.0, 5.0], [10.0, 5.0], 10)
    assert [(r[0], r[1]) for r in west] == [(0, 0)] and [(r[0], r[1]) for r in south] == [(0, 0)]


def test_quantize_line_drops_repeats_and_degenerates():
    r = P.quantize_line([0.0, 0.00001, 1.0], [0.0, 0.0, 1.0])
    assert r[0].tolist() == [0, 10000] and r[1].tolist() == [0, 10000]
    assert P.quantize_line([1.0, 1.00001], [2.0, 2.0]) is None
    assert P.quantize_line([1.0, 2.0], [0.0, 0.0], q=1000)[0].tolist() == [1000, 2000]


def test_chain_lines_joins_shared_ends_of_the_same_zoom_only():
    a = (np.array([0, 1]), np.array([0, 0]), 20)
    b = (np.array([1, 2]), np.array([0, 1]), 20)
    c = (np.array([3, 2]), np.array([1, 1]), 20)                                 # reversed: its end meets b's end
    d = (np.array([2, 5]), np.array([1, 5]), 30)                                 # another zoom: stays apart
    e = (np.array([9, 8]), np.array([9, 8]), 20)                                 # joined to nothing
    out = P.chain_lines([b, d, c, a, e])
    lines20 = [l for l in out if l[2] == 20]
    assert len(out) == 3 and len(lines20) == 2
    joined = max(lines20, key=lambda l: len(l[0]))
    pts = list(zip(joined[0].tolist(), joined[1].tolist()))
    assert pts in ([(0, 0), (1, 0), (2, 1), (3, 1)], [(3, 1), (2, 1), (1, 0), (0, 0)])
    assert [l for l in out if l[2] == 30][0][0].tolist() == [2, 5]
    loop = P.chain_lines([(np.array([0, 1]), np.array([0, 0]), 0), (np.array([1, 0]), np.array([0, 1]), 0),
                          (np.array([0, 0]), np.array([1, 0]), 0)])
    assert len(loop) == 1 and len(loop[0][0]) == 4                                # a closed ring stays one line


# ---------------------------------------------------------------- lines-v1

def lines_sample():
    l1 = (np.array([100, 200, 300]), np.array([50, 60, 40]), 0)
    l2 = (np.array([310, 400]), np.array([45, 90]), 37)
    l3 = (np.array([-1800000, -1790000]), np.array([-900000, -890000]), 255)
    return [(0, [l1, l2]), (1, [l3])]


def test_lines_roundtrip_header_and_relative_first_points():
    buf = P.encode_lines(lines_sample(), q=1000)
    d = P.decode_lines(buf)
    assert d["cell"] == 10 and d["q"] == 1000 and d["kinds"] == 3 and d["bbox"] == [-1800000, -900000, 400, 90]
    got = [(pc["kind"], [(l[0].tolist(), l[1].tolist(), l[2]) for l in pc["lines"]]) for pc in d["pieces"]]
    want = [(k, [(l[0].tolist(), l[1].tolist(), l[2]) for l in ls]) for k, ls in lines_sample()]
    assert got == want
    assert d["pieces"][0]["b"] == [100, 40, 400, 90]
    vals = P.decode_varints(buf[P.LINES_HEADER.size:])
    assert P.unzz(vals[16:18]).tolist() == [10, 5]                                # line 2 starts from line 1's last point
    assert P.decode_lines(P.encode_lines([]))["pieces"] == []


def test_decode_lines_refuses_damaged_files():
    buf = P.encode_lines(lines_sample())
    with pytest.raises(ValueError):
        P.decode_lines(b"XXXX" + buf[4:])
    with pytest.raises(ValueError):
        P.decode_lines(buf[:-1])                                                  # cut inside the stream
    with pytest.raises(ValueError):
        P.decode_lines(buf + b"\x05")                                             # a trailing value
    with pytest.raises(ValueError):
        P.decode_lines(buf[:10])
    bad = P.encode_lines([(0, [(np.array([0, 1]), np.array([0, 1]), 0)])])
    bad = bad[:P.LINES_HEADER.size + 4] + bytes([7]) + bad[P.LINES_HEADER.size + 5:]   # kind 7
    with pytest.raises(ValueError):
        P.decode_lines(bad)
    with pytest.raises(ValueError):
        P.decode_varints(b"\x80\x80\x80\x80\x80\x01")                              # longer than 5 bytes


# ---------------------------------------------------------------- places-v1

def place(name, lat, lon, pop=5000, mz10=90, rank=5, flags=0):
    return {"name": name, "lat": lat, "lon": lon, "pop": pop, "mz10": mz10, "rank": rank, "flags": flags}


def test_places_roundtrip_utf8_names_and_the_antimeridian():
    ps = [place("Tokyo", 35.6895, 139.6917, 35_000_000, 17, 0, 1), place("Hale‘iwa", 21.5928, -158.1034, 3970, 100, 5, 2),
          place("Wahiawā", 21.5028, -158.0236), place("Edge", 66.5, 180.0)]
    buf = P.encode_places(ps)
    d = P.decode_places(buf)
    assert [p["name"] for p in d["places"]] == ["Tokyo", "Hale‘iwa", "Wahiawā", "Edge"]
    assert d["places"][1]["lat"] == 215928 and d["places"][1]["lon"] == -1581034 and d["places"][3]["lon"] == -1800000
    assert [(p["rank"], p["mz10"], p["pop"], p["flags"]) for p in d["places"][:2]] == [(0, 17, 35_000_000, 1), (5, 100, 3970, 2)]
    assert d["q"] == Q and d["bbox"] == [-1800000, 215028, 1396917, 665000]
    assert P.decode_places(P.encode_places([]))["places"] == []
    assert P.decode_places(P.encode_places(ps, q=1000))["places"][0]["lat"] == 35690


def test_decode_places_refuses_damaged_files():
    buf = P.encode_places([place("A", 1, 2), place("B", 3, 4)])
    with pytest.raises(ValueError):
        P.decode_places(b"XXXX" + buf[4:])
    with pytest.raises(ValueError):
        P.decode_places(buf[:-2])                                                 # a name lost
    with pytest.raises(ValueError):
        P.decode_places(buf + b"\nC")                                            # a name too many
    with pytest.raises(ValueError):
        P.decode_places(buf[:-1] + b"\xff")                                       # not UTF-8
    with pytest.raises(ValueError):
        P.decode_places(P.PLACES_HEADER.pack(b"PLC1", 10, 0, Q, 1, 999, 0, 0, 0, 0) + b"\x01")   # stream past the end
    with pytest.raises(ValueError):
        P.decode_places(P.encode_places([]) + b"stray")
    one = P.encode_places([place("A", 1, 2)])
    _m, _c, _z, q, _n, nstream, *bb = P.PLACES_HEADER.unpack_from(one, 0)
    short = (P.PLACES_HEADER.pack(b"PLC1", 10, 0, q, 2, nstream, *bb)
             + one[P.PLACES_HEADER.size:P.PLACES_HEADER.size + nstream] + bytes([65, 10, 66]))   # "A\nB": two names
    with pytest.raises(ValueError):
        P.decode_places(short)                                                    # two names, one record


# ---------------------------------------------------------------- sources

def write_geojson(path, features):
    json.dump({"type": "FeatureCollection", "features": features}, open(path, "w", encoding="utf-8"))


def line_feature(coords, cla, mz=0, multi=False):
    return {"type": "Feature", "properties": {"FEATURECLA": cla, "MIN_ZOOM": mz},
            "geometry": {"type": "MultiLineString" if multi else "LineString", "coordinates": coords}}


def point_feature(name, lat, lon, pop, mz, cla="Populated place", cap=0, ascii_name=None):
    return {"type": "Feature", "properties": {"name": name, "nameascii": ascii_name or name, "pop_max": pop, "min_zoom": mz,
                                              "featurecla": cla, "adm0cap": cap},
            "geometry": {"type": "Point", "coordinates": [lon, lat]}}


def gn_row(gid, name, lat, lon, code, pop, ascii_name=None, fclass="P"):
    f = [str(gid), name, ascii_name or name, "", "%.5f" % lat, "%.5f" % lon, fclass, code, "XX", "", "", "", "", "",
         str(pop), "", "0", "Etc/UTC", "2026-01-01"]
    return "\t".join(f)


def sources(tmp_path):
    """A small world: a border across two cells and a state line along a cell line (tier 0 and 1), a state line shown
    only from zoom 8 (tier 1 only), a border across the antimeridian, an overlay limit (dropped), Natural Earth places
    of every rank, GeoNames towns (one a duplicate of a Natural Earth place)."""
    ne = tmp_path / "ne"
    ne.mkdir()
    write_geojson(ne / P.NE_FILES["admin0"][0], [
        line_feature([[5.0, 5.0], [15.0, 5.0], [25.0, 15.0]], "International boundary (verify)"),
        line_feature([[[175.0, 60.0], [-175.0, 62.0]], [[1.0, 1.0], [2.0, 2.0]]], "Disputed (please verify)", multi=True),
        line_feature([[30.0, 30.0], [31.0, 31.0]], "Overlay limit"),
        line_feature([[40.0 + k * 0.01, 40.0 + k * 0.01] for k in range(51)], "International boundary (verify)"),   # straight
        {"type": "Feature", "properties": {"FEATURECLA": "International boundary (verify)"}, "geometry": None},
    ])
    write_geojson(ne / P.NE_FILES["admin1"][0], [
        line_feature([[-120.0, 39.0], [-120.0, 42.0]], "Admin-1 statistical boundary", mz=2),
        line_feature([[-110.0, 31.0], [-109.0, 32.0]], "Admin-1 boundary", mz=8.6),
        line_feature([[-100.0, 31.0], [-99.0, 32.0]], "Unrecognized Admin-1 boundary", mz=2),
    ])
    write_geojson(ne / P.NE_FILES["places"][0], [
        point_feature("Capital", 48.8566, 2.3522, 11_000_000, 1.7, cla="Admin-0 capital", cap=1),
        point_feature("Honolulu", 21.3088, -157.8599, 786_000, 4, cla="Admin-1 capital"),
        point_feature("Wahiawa", 21.5031, -158.0236, 174_766, 6.7),
        point_feature("Smalltown", 21.40, -157.75, 20_000, 8.1),
        point_feature("Midzoom", 45.0, 5.0, 50_000, 7.5),                            # exactly the split: a cell, not tier 0
    ])
    rows = [gn_row(1, "Wahiawā", 21.5028, -158.0236, "PPL", 17_821, "Wahiawa"),         # Natural Earth's Wahiawa
            gn_row(2, "Hale‘iwa", 21.5928, -158.1034, "PPL", 3_970, "Hale'iwa"),
            gn_row(3, "Kailua", 21.4022, -157.7394, "PPL", 38_635),
            gn_row(4, "Neighbourhood", 21.30, -157.85, "PPLX", 50_000),                    # a section: left out
            gn_row(5, "Tiny", 21.35, -157.90, "PPL", 999),                                  # too small
            gn_row(6, "Nowhere", 0.5, 0.5, "PPL", 5_000, fclass="A"),                      # not a populated place
            gn_row(7, "Capital Ville", 10.0, 10.0, "PPLC", 1_500),
            gn_row(8, "Edge Town", 66.0, 180.0, "PPL", 2_000),
            gn_row(9, "Thousand", -30.0, 150.0, "PPL", 1_000),                            # the limits exactly
            gn_row(10, "Five Thousand", -30.5, 150.5, "PPL", 5_000),
            gn_row(11, "Twenty Thousand", -31.0, 151.0, "PPL", 20_000)]
    gz = tmp_path / "cities1000.zip"
    with zipfile.ZipFile(gz, "w") as z:
        z.writestr(zipfile.ZipInfo("cities1000.txt", date_time=(2026, 10, 22, 3, 4, 5)), "\n".join(rows) + "\n")
    return str(ne), str(gz)


def test_read_lines_keeps_the_classes_and_min_zoom(tmp_path):
    ne, _gz = sources(tmp_path)
    a0 = P.read_lines(os.path.join(ne, P.NE_FILES["admin0"][0]), P.ADMIN0_CLASSES, P.KIND_ADMIN0)
    a1 = P.read_lines(os.path.join(ne, P.NE_FILES["admin1"][0]), P.ADMIN1_CLASSES, P.KIND_ADMIN1)
    assert len(a0) == 4 and all(l[2] == 0 for l in a0)                         # the overlay limit and the null geometry left out
    assert [(l[2], l[3]) for l in a1] == [(1, 20), (1, 86)]                     # statistical lines are state lines


def test_read_geonames_filters_and_ranks(tmp_path):
    _ne, gz = sources(tmp_path)
    gn, date = P.read_geonames(gz)
    assert date == "2026-10-22"
    by = {g["name"]: g for g in gn}
    assert sorted(by) == ["Capital Ville", "Edge Town", "Five Thousand", "Hale‘iwa", "Kailua", "Thousand",
                          "Twenty Thousand", "Wahiawā"]
    assert [by[n]["mz10"] for n in ("Thousand", "Five Thousand", "Twenty Thousand")] == [100, 90, 80]
    assert by["Capital Ville"]["rank"] == 0 and by["Capital Ville"]["flags"] == P.FLAG_CAPITAL | P.FLAG_GEONAMES
    assert [by[n]["mz10"] for n in ("Kailua", "Wahiawā", "Hale‘iwa")] == [80, 90, 100]
    assert by["Edge Town"]["lon"] == -180.0
    assert by["Hale‘iwa"]["keys"] == {"haleiwa"}


def test_dedupe_by_name_within_five_km_and_across_the_antimeridian():
    ne = [dict(place("Springfield", 40.0, -90.0), keys={"springfield"}), dict(place("Dateline", 10.0, 179.99), keys={"dateline"})]
    gn = [dict(place("Springfield", 40.03, -90.0), keys={"springfield"}),          # 3.3 km: the same place
          dict(place("Springfield", 40.10, -90.0), keys={"springfield"}),          # 11 km: another place
          dict(place("Elsewhere", 40.0, -90.001), keys={"elsewhere"}),             # another name
          dict(place("Dateline", 10.0, -179.99), keys={"dateline"})]               # 2.2 km across 180
    kept, dropped = P.dedupe(ne, gn)
    assert dropped == 2 and [(g["name"], g["lat"]) for g in kept] == [("Springfield", 40.10), ("Elsewhere", 40.0)]


# ---------------------------------------------------------------- build + check

def build(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    ne, gz = sources(tmp_path)
    out = str(tmp_path / "v1")
    return P.build(ne, gz, out, **QUIET), out


def test_build_and_check_on_synthetic_sources(tmp_path):
    index, out = build(tmp_path)
    assert P.check(out, **QUIET) == []
    assert index["format"] == "places-v1" and index["format_key"] == P.FORMAT_KEY and index["q"] == Q
    assert index["sources"]["natural_earth"]["commit"] == P.NE_COMMIT and index["sources"]["geonames"]["date"] == "2026-10-22"
    assert index["counts"]["geonames_dupes"] == 1 and index["counts"]["geonames_kept"] == 7 and index["counts"]["ne_places"] == 5
    mid = P.decode_places(open(os.path.join(out, "c", "40_0.p.bin"), "rb").read())
    assert [(p["name"], p["mz10"]) for p in mid["places"]] == [("Midzoom", 75)]
    straight0 = [l for pc in P.decode_lines(open(os.path.join(out, P.LINES_TIER0), "rb").read())["pieces"]
                 for l in pc["lines"] if l[0][0] == 40 * P.TIER0_Q]
    straight1 = P.decode_lines(open(os.path.join(out, "c", "40_40.l.bin"), "rb").read())["pieces"][0]["lines"]
    assert len(straight0) == 1 and len(straight0[0][0]) == 2 and len(straight1[0][0]) == 51   # tier 0 is simplified
    dl = P.decode_lines(open(os.path.join(out, P.LINES_TIER0), "rb").read())
    assert dl["q"] == P.TIER0_Q
    assert {pc["kind"] for pc in dl["pieces"]} == {0, 1}
    assert all(l[2] < P.MZ_SPLIT for pc in dl["pieces"] for l in pc["lines"])  # the zoom-8.6 state line is not in tier 0
    dp = P.decode_places(open(os.path.join(out, P.PLACES_TIER0), "rb").read())
    assert dp["q"] == P.TIER0_Q and [p["name"] for p in dp["places"]] == ["Capital", "Honolulu", "Wahiawa"]
    cells = index["tier1"]["cells"]
    assert {"0_0", "0_10", "10_20", "60_170", "60_-180", "30_-120", "40_-120", "30_-110", "20_-160", "10_10"} <= set(cells)
    assert [cells[n][0] > 0 for n in ("0_0", "0_10", "10_20", "10_10")] == [True, True, True, False]   # 10_10: a town only
    oahu = P.decode_places(open(os.path.join(out, "c", "20_-160.p.bin"), "rb").read())
    assert oahu["q"] == Q and [p["name"] for p in oahu["places"]] == ["Kailua", "Smalltown", "Hale‘iwa"]
    assert cells["20_-160"][0] == 0 and not os.path.exists(os.path.join(out, "c", "20_-160.l.bin"))
    late = P.decode_lines(open(os.path.join(out, "c", "30_-110.l.bin"), "rb").read())
    assert [(pc["kind"], [l[2] for l in pc["lines"]]) for pc in late["pieces"]] == [(1, [86])]
    east = P.decode_lines(open(os.path.join(out, "c", "60_170.l.bin"), "rb").read())
    west = P.decode_lines(open(os.path.join(out, "c", "60_-180.l.bin"), "rb").read())
    e, w = east["pieces"][0]["lines"][0], west["pieces"][0]["lines"][0]
    assert e[0][-1] == 180 * Q and w[0][0] == -180 * Q and e[1][-1] == w[1][0]    # the same latitude on both sides
    edge = P.decode_places(open(os.path.join(out, "c", "60_-180.p.bin"), "rb").read())
    assert edge["places"][0]["lon"] == -180 * Q
    assert P.main(["--check", out]) == 0


def test_cell_cap_keeps_the_largest(tmp_path, monkeypatch):
    monkeypatch.setattr(P, "CELL_CAP", 2)
    index, out = build(tmp_path)
    assert index["counts"]["capped"] == 1
    oahu = P.decode_places(open(os.path.join(out, "c", "20_-160.p.bin"), "rb").read())
    assert [p["name"] for p in oahu["places"]] == ["Kailua", "Smalltown"]
    assert P.check(out, **QUIET) == []


def rewrite(path, data):
    open(path, "wb").write(data)


def test_check_catches_damage(tmp_path):
    index, out = build(tmp_path)
    ip = os.path.join(out, "index.json")
    json.dump(dict(index, format_key="places-v1|other"), open(ip, "w"))
    assert any("format_key" in p for p in P.check(out, **QUIET))
    json.dump(index, open(ip, "w"))
    cell = os.path.join(out, "c", "0_10.l.bin")
    d = P.decode_lines(open(cell, "rb").read())
    moved = [(pc["kind"], [(l[0] + 10 * Q, l[1], l[2]) for l in pc["lines"]]) for pc in d["pieces"]]
    rewrite(cell, P.encode_lines(moved))                                       # a vertex moved a cell east
    probs = P.check(out, **QUIET)
    assert any("outside its cell 0_10" in p for p in probs) and any("size mismatch" in p or "content hash" in p for p in probs)
    index, out = build(tmp_path / "b")
    cell = os.path.join(out, "c", "20_-160.p.bin")
    raw = open(cell, "rb").read()
    rewrite(cell, raw[:-1] + b"\xff")                                          # a name is no longer UTF-8
    assert any("decode failed" in p for p in P.check(out, **QUIET))
    index, out = build(tmp_path / "c")
    ps = P.decode_places(open(os.path.join(out, "c", "20_-160.p.bin"), "rb").read())["places"]
    wrong = [dict(p, lat=p["lat"] / Q + 10.0, lon=p["lon"] / Q) for p in ps]   # into the next cell north
    rewrite(os.path.join(out, "c", "20_-160.p.bin"), P.encode_places(wrong))
    assert any("outside its cell 20_-160" in p for p in P.check(out, **QUIET))
    index, out = build(tmp_path / "d")
    tp = os.path.join(out, P.PLACES_TIER0)
    order = P.decode_places(open(tp, "rb").read())["places"]
    flipped = [dict(p, lat=p["lat"] / P.TIER0_Q, lon=p["lon"] / P.TIER0_Q) for p in order[::-1]]
    rewrite(tp, P.encode_places(flipped, q=P.TIER0_Q))
    probs = P.check(out, **QUIET)
    assert any("out of rank order" in p for p in probs) and any("tier 0" in p and "hash" in p for p in probs)
    os.remove(os.path.join(out, "c", "10_20.l.bin"))
    assert any(p.startswith("tier 1: files") for p in P.check(out, **QUIET))
    assert P.main(["--check", out]) == 1
    assert P.main(["--out", out, "--upload", "static/places/v1"]) == 1         # refuses before touching boto3


def test_check_runs_the_real_gates_for_the_pinned_natural_earth(tmp_path, monkeypatch):
    ne, gz = sources(tmp_path)
    pinned = {k: (f, hashlib.sha256(open(os.path.join(ne, f), "rb").read()).hexdigest()) for k, (f, _h) in P.NE_FILES.items()}
    monkeypatch.setattr(P, "NE_FILES", pinned)
    out = str(tmp_path / "v1")
    P.build(ne, gz, out, **QUIET)
    probs = P.check(out, **QUIET)
    assert any("Natural Earth places, expected" in p for p in probs) and any("GeoNames towns kept" in p for p in probs)
    assert any(p.startswith("probe: no kind-0") for p in probs) and any("no place 'tokyo'" in p for p in probs)
    assert not any("'honolulu'" in p for p in probs) and not any("'haleiwa'" in p for p in probs)   # these are in the sources
    assert not any("-120.0,42.0" in p for p in probs)                          # the statistical state line came through
    monkeypatch.setattr(P, "NE_PLACES_RANGE", (5, 5)); monkeypatch.setattr(P, "GN_MIN_KEPT", 7)
    monkeypatch.setattr(P, "LINE_PROBES", [(1, -120.0, 42.0), (0, 15.0, 5.0)])
    monkeypatch.setattr(P, "PLACE_PROBES", [("tier0", "honolulu", 1, 40), ("20_-160", "haleiwa", 5, 100)])
    assert P.check(out, **QUIET) == []


class FakeS3:
    def __init__(self):
        self.objects, self.log = {}, []

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        self.objects[Key] = {"body": Body, "ct": ContentType, "cc": CacheControl}
        self.log.append(Key)

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key]["body"])}


def test_upload_publishes_every_file_with_the_places_licence(tmp_path):
    index, out = build(tmp_path)
    s3 = FakeS3()
    assert P.C.upload(out, "static/places/v1", s3=s3, bucket="b", files=P.files_of, license_file=P.LICENSE_FILE, **QUIET)
    keys = [k.split("static/places/v1/")[1] for k in s3.log]
    assert keys[0] == "LICENSE.txt" and keys[-1] == "index.json"
    assert set(keys[1:-1]) == set(P.files_of(index)) and "world-lines.bin" in keys and "c/20_-160.p.bin" in keys
    assert "c/20_-160.l.bin" not in keys
    lic = s3.objects["static/places/v1/LICENSE.txt"]["body"].decode("utf-8")
    assert "CC BY 4.0" in lic and "public domain" in lic and P.NE_COMMIT in lic
    assert all(o["cc"] == "public, max-age=31536000, immutable" for o in s3.objects.values())
    n = len(s3.log)
    assert P.C.upload(out, "static/places/v1", s3=s3, bucket="b", files=P.files_of, license_file=P.LICENSE_FILE, **QUIET)
    assert s3.log[n:] == ["static/places/v1/LICENSE.txt", "static/places/v1/index.json"]    # the same build: nothing else
    idx = json.load(open(os.path.join(out, "index.json")))
    idx["tier0"]["sha256"] = "0" * 64
    json.dump(idx, open(os.path.join(out, "index.json"), "w"))
    n = len(s3.log)
    assert P.C.upload(out, "static/places/v1", s3=s3, bucket="b", files=P.files_of, license_file=P.LICENSE_FILE, **QUIET) is False
    assert len(s3.log) == n


def test_main_needs_its_inputs(tmp_path):
    with pytest.raises(SystemExit):
        P.main(["--out", str(tmp_path / "x")])
    with pytest.raises(SystemExit):
        P.main(["--upload", "static/places/v1"])


def test_places_workflow_is_pinned():
    text = open(os.path.join(ROOT, ".github", "workflows", "coast-build.yml"), encoding="utf-8").read()
    assert P.NE_COMMIT in text and "static/places/v1" in text and "static/coast/lakeoutlines-v1" in text
    for f, h in P.NE_FILES.values():
        assert f in text and h in text                                          # every Natural Earth file pinned by hash
    assert "cities1000.zip" in text and "tests/places" in text and "tools/places/**" in text
    assert 'case "$SET" in coast|lakes|lakeoutlines|places)' in text


def test_a_town_on_a_cell_edge_is_filed_by_its_stored_position(tmp_path):
    """Real build: a town at lon 9.99996 rounds to 10.0000 in the file, so it belongs to the cell east of 10 E."""
    gz = tmp_path / "cities1000.zip"
    with zipfile.ZipFile(gz, "w") as z:
        z.writestr("cities1000.txt", gn_row(1, "Edge", 5.00004, 9.99996, "PPL", 5000) + "\n")
    gn, _date = P.read_geonames(str(gz))
    assert (gn[0]["lat"], gn[0]["lon"]) == (5.0, 10.0) and P.place_cell(gn[0]) == (0, 10)
    d = P.decode_places(P.encode_places(gn))
    assert P.check_places(d, (0, 10, 10)) == [] and P.check_places(d, (0, 0, 10)) != []
    ne = tmp_path / "ne.geojson"
    write_geojson(ne, [point_feature("Edge NE", 5.00004, 9.99996, 5000, 8)])
    p = P.read_ne_places(str(ne))[0]
    assert (p["lat"], p["lon"]) == (5.0, 10.0) and P.place_cell(p) == (0, 10)
