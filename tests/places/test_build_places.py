"""Places builder (tools/places/build_places.py): Natural Earth border lines and places, GeoNames towns, the lines-v1
and places-v1 formats, the cell cut and the --check gate. Synthetic sources only (no network)."""
import hashlib
import io
import json
import math
import os
import re
import struct
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


def point_feature(name, lat, lon, pop, mz, cla="Populated place", cap=0, ascii_name=None, gnid=0):
    """A feature of the full Natural Earth places file (upper-case properties, GEONAMESID)."""
    props = {"NAME": name, "NAMEASCII": ascii_name or name, "POP_MAX": pop, "MIN_ZOOM": mz, "FEATURECLA": cla,
             "ADM0CAP": cap, "GEONAMESID": gnid}
    if mz is None:
        del props["MIN_ZOOM"]
    return {"type": "Feature", "properties": props, "geometry": {"type": "Point", "coordinates": [lon, lat]}}


def gn_row(gid, name, lat, lon, code, pop, ascii_name=None, fclass="P", alt=""):
    f = [str(gid), name, ascii_name or name, alt, "%.5f" % lat, "%.5f" % lon, fclass, code, "XX", "", "", "", "", "",
         str(pop), "", "0", "Etc/UTC", "2026-01-01"]
    return "\t".join(f)


def sources(tmp_path):
    """A small world.
    Lines: a border across two cells, a disputed line across the antimeridian (kind 2), an overlay limit (left out), a
    straight densely sampled border, a state line along a cell line (tier 0 and 1), a state line shown only from zoom 8
    (tier 1 only), an unrecognised state line and a state line across water (both left out).
    Places: Natural Earth places of every rank; GeoNames towns: one IS a Natural Earth place by id, one has a Natural
    Earth name 8 km away, one a Natural Earth place 2 km away among its alternate names, one by name within 5 km, two
    GeoNames twins, a merged " / " area, a row whose alternate names hold a line separator."""
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
        line_feature([[-76.3, 37.5], [-75.9, 37.7]], "Admin-1 boundary indicator", mz=2),                     # across water
    ])
    write_geojson(ne / P.NE_FILES["places"][0], [
        point_feature("Capital", 48.8566, 2.3522, 11_000_000, 1.7, cla="Admin-0 capital", cap=1),
        point_feature("Honolulu", 21.3088, -157.8599, 786_000, 4, cla="Admin-1 capital", gnid=12),
        point_feature("Wahiawa", 21.5031, -158.0236, 174_766, 6.7),
        point_feature("Smalltown", 21.40, -157.75, 20_000, 8.1),
        point_feature("Midzoom", 45.0, 5.0, 50_000, 7.5),                            # exactly the split: a cell, not tier 0
        point_feature("Ganzhou", 25.85, 114.93, 2_000_000, 5),
        point_feature("København", 55.6761, 12.5683, 1_200_000, 3, ascii_name="Kobenhavn"),
    ])
    rows = [gn_row(1, "Wahiawā", 21.5028, -158.0236, "PPL", 17_821, "Wahiawa"),       # NE's Wahiawa, same name
            gn_row(2, "Hale‘iwa", 21.5928, -158.1034, "PPL", 3_970, "Hale'iwa"),
            gn_row(3, "Kailua", 21.4022, -157.7394, "PPL", 38_635),
            gn_row(4, "Neighbourhood", 21.30, -157.85, "PPLX", 50_000),                    # a section: left out
            gn_row(5, "Tiny", 21.35, -157.90, "PPL", 999),                                  # too small
            gn_row(6, "Nowhere", 0.5, 0.5, "PPL", 5_000, fclass="A"),                      # not a populated place
            gn_row(7, "Capital Ville", 10.0, 10.0, "PPLC", 1_500),
            gn_row(8, "Edge Town", 66.0, 180.0, "PPL", 2_000),
            gn_row(9, "Thousand", -30.0, 150.0, "PPL", 1_000),                            # the limits exactly
            gn_row(10, "Five Thousand", -30.5, 150.5, "PPL", 5_000),
            gn_row(11, "Twenty Thousand", -31.0, 151.0, "PPL", 20_000),
            gn_row(12, "Urban Honolulu", 21.32, -157.84, "PPL", 350_000),                  # Honolulu's GEONAMESID
            gn_row(13, "Ganzhou", 25.92, 114.93, "PPLA2", 1_977_253),                      # 7.8 km from NE's Ganzhou
            gn_row(14, "Copenhagen", 55.68, 12.59, "PPLC", 1_150_000, alt="Kopenhagen,København,Copenhague"),
            gn_row(15, "Makakilo", 21.35, -158.09, "PPL", 18_000),
            gn_row(16, "Makakilo", 21.355, -158.09, "PPL", 1_500),                         # a GeoNames twin 0.6 km away
            gn_row(17, "Aliamanu / Salt Lakes", 21.36, -157.91, "PPLS", 12_000),
            gn_row(18, "Aliamanu / Foster", 21.36, -157.92, "PPL", 12_000),               # " / ": a merged area
            gn_row(19, "Linesep", -20.0, 30.0, "PPL", 3_000, alt="A B,C")]
    gz = tmp_path / "cities1000.zip"
    with zipfile.ZipFile(gz, "w") as z:
        z.writestr(zipfile.ZipInfo("cities1000.txt", date_time=(2026, 10, 22, 3, 4, 5)), "\n".join(rows) + "\n")
    return str(ne), str(gz)


def test_read_lines_maps_classes_to_kinds_and_keeps_min_zoom(tmp_path):
    ne, _gz = sources(tmp_path)
    a0 = P.read_lines(os.path.join(ne, P.NE_FILES["admin0"][0]), P.ADMIN0_CLASSES)
    a1 = P.read_lines(os.path.join(ne, P.NE_FILES["admin1"][0]), P.ADMIN1_CLASSES)
    assert [l[2] for l in a0] == [0, 2, 2, 0]                                     # disputed = kind 2; overlay limit, null left out
    assert [(l[2], l[3]) for l in a1] == [(1, 20), (1, 86)]                     # statistical lines kept; unrecognised, indicator out
    assert set(P.ADMIN0_CLASSES.values()) == {0, 2} and set(P.ADMIN1_CLASSES.values()) == {1}
    assert "Admin-1 boundary indicator" not in P.ADMIN1_CLASSES and "Overlay limit" not in P.ADMIN0_CLASSES


def test_read_geonames_filters_ranks_and_keeps_rows_whole(tmp_path):
    _ne, gz = sources(tmp_path)
    gn, date, slashes = P.read_geonames(gz)
    assert date == "2026-10-22" and slashes == 1
    by = {g["name"]: g for g in gn}
    assert sorted(by) == ["Capital Ville", "Copenhagen", "Edge Town", "Five Thousand", "Ganzhou", "Hale‘iwa", "Kailua",
                          "Linesep", "Makakilo", "Thousand", "Twenty Thousand", "Urban Honolulu", "Wahiawā"]
    assert [by[n]["mz10"] for n in ("Thousand", "Five Thousand", "Twenty Thousand")] == [100, 90, 80]
    assert by["Capital Ville"]["rank"] == 0 and by["Capital Ville"]["flags"] == P.FLAG_CAPITAL | P.FLAG_GEONAMES
    assert [by[n]["mz10"] for n in ("Kailua", "Wahiawā", "Hale‘iwa")] == [80, 90, 100]
    assert by["Edge Town"]["lon"] == -180.0 and by["Hale‘iwa"]["keys"] == {"haleiwa"}
    assert by["Urban Honolulu"]["id"] == 12 and by["Linesep"]["alt"] == "A B,C"   # a U+2028 never splits a row
    assert by["Linesep"]["lat"] == -20.0


def test_read_ne_places_reads_the_full_file_and_its_ranks(tmp_path):
    path = tmp_path / "p.geojson"
    write_geojson(path, [point_feature("A", 1.0, 1.0, 5, 3, cla="Admin-0 capital", cap=0, gnid=77),     # a capital by class
                         point_feature("B", 2.0, 2.0, 5, 3, cla="Admin-1 region capital"),             # not a state capital
                         point_feature("C", 3.0, 3.0, 5, None, cla="Admin-1 capital"),                 # no min_zoom
                         point_feature("D", 4.0, 4.0, 5, 4, ascii_name="Dee")])
    a, b, c, d = P.read_ne_places(str(path))
    assert (a["rank"], a["flags"], a["gnid"]) == (0, P.FLAG_CAPITAL, 77)
    assert b["rank"] == 5 and c["rank"] == 1 and c["mz10"] == P.MZ_SPLIT and d["gnid"] is None
    assert d["keys"] == {"d", "dee"}
    simple = tmp_path / "s.geojson"                                               # the simple file's lower-case names still read
    write_geojson(simple, [{"type": "Feature", "properties": {"name": "E", "pop_max": 9, "min_zoom": 2, "featurecla": "x"},
                            "geometry": {"type": "Point", "coordinates": [5.0, 5.0]}}])
    e, = P.read_ne_places(str(simple))
    assert (e["name"], e["pop"], e["mz10"]) == ("E", 9, 20)


def gplace(name, lat, lon, pop=5000, rank=5, gid=None, alt="", keys=None):
    return dict(place(name, lat, lon, pop=pop, rank=rank), id=gid, alt=alt, keys=keys or {P.fold(name)})


def test_dedupe_by_id_name_alternate_names_and_twins():
    ne = [dict(place("Springfield", 40.0, -90.0), keys={"springfield"}, gnid=None),
          dict(place("Dateline", 10.0, 179.99), keys={"dateline"}, gnid=None),
          dict(place("Big City", 30.0, 30.0), keys={"bigcity"}, gnid=500),
          dict(place("Osaka", 34.69, 135.50), keys={"osaka"}, gnid=None),
          dict(place("Polar", 70.0, 20.0), keys={"polar"}, gnid=None)]
    gn = [gplace("Springfield", 40.10, -90.0),                                     # 11 km, same name: the same place
          gplace("Springfield", 40.16, -90.0),                                     # 17.8 km: another place
          gplace("Elsewhere", 40.0, -90.001),                                      # another name
          gplace("Dateline", 10.0, -179.99),                                       # 2.2 km across 180
          gplace("Downtown", 31.0, 31.0, gid=500),                                 # IS Big City (id), whatever the name / distance
          gplace("Osaka-shi", 34.70, 135.51, keys={"osakashi", "osaka"}),          # its ascii name is Osaka
          gplace("Ville", 30.02, 30.0, alt="Big City,Grande"),                     # 2.2 km, Big City among its alternate names
          gplace("Ville2", 30.08, 30.0, alt="Big City"),                           # 8.9 km: alternate names only count within 5 km
          gplace("Polar", 70.0, 20.35),                                            # 13.3 km at 70 N: same name within 15 km
          gplace("Twin", 50.0, 50.0, pop=9000, rank=5),
          gplace("Twin", 50.01, 50.0, pop=2000, rank=5),                           # 1.1 km: the smaller twin goes
          gplace("Twin", 50.05, 50.0, pop=1500, rank=5)]                           # 5.6 km: kept
    kept, st = P.dedupe(ne, gn)
    assert st == {"id": 1, "name": 5, "twin": 1}
    assert [(g["name"], g["lat"]) for g in kept] == [("Twin", 50.0), ("Elsewhere", 40.0), ("Springfield", 40.16),
                                                     ("Ville2", 30.08), ("Twin", 50.05)]
    assert len(list(P.grid_near({(899, 0): [1]}, 89.9, 0.0, 15))) == 1


def test_grid_near_reaches_the_radius_at_every_latitude():
    for lat in (0.0, 45.0, 70.0, 84.0):
        g = {}
        P.grid_add(g, {"lat": lat, "lon": 0.0})
        lon = 14.9 / (111.19 * math.cos(math.radians(lat)))                       # 14.9 km east
        assert P.haversine_km(lat, 0.0, lat, lon) < 15.0
        assert len(list(P.grid_near(g, lat, lon, 15))) == 1, lat


def test_dedupe_alternate_names_only_within_5_km_and_the_grid_reaches_two_buckets_north():
    ne = [dict(place("Near", 20.0, 20.0), keys={"near"}, gnid=None),
          dict(place("Far City", 20.08, 20.0), keys={"farcity"}, gnid=None),
          dict(place("Edge", 0.099, 40.0), keys={"edge"}, gnid=None)]
    gn = [gplace("Town", 20.0, 20.009, alt="Far City"),     # Near 0.9 km away; Far City, its alternate name, 8.9 km: kept
          gplace("Edge", 0.233, 40.0)]                      # 14.9 km north, two 0.1-degree buckets up: the same place
    kept, st = P.dedupe(ne, gn)
    assert [g["name"] for g in kept] == ["Town"] and st["name"] == 1


def test_a_geonames_town_matches_by_its_ascii_name(tmp_path):
    gz = tmp_path / "cities1000.zip"
    with zipfile.ZipFile(gz, "w") as z:
        z.writestr("cities1000.txt", gn_row(1, "Київ", 50.45, 30.52, "PPLC", 2_900_000, ascii_name="Kyiv") + "\n")
    gn, _date, _slashes = P.read_geonames(str(gz))
    assert gn[0]["keys"] == {"kyiv"}
    kept, st = P.dedupe([dict(place("Kyiv", 50.4501, 30.5234), keys={"kyiv"}, gnid=None)], gn)
    assert kept == [] and st["name"] == 1


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
    c = index["counts"]
    assert (c["ne_places"], c["geonames_read"], c["geonames_slashes"], c["geonames_dupes_id"], c["geonames_dupes_name"],
            c["geonames_twins"], c["geonames_kept"]) == (7, 14, 1, 1, 3, 1, 9)
    mid = P.decode_places(open(os.path.join(out, "c", "40_0.p.bin"), "rb").read())
    assert [(p["name"], p["mz10"]) for p in mid["places"]] == [("Midzoom", 75)]
    straight0 = [l for pc in P.decode_lines(open(os.path.join(out, P.LINES_TIER0), "rb").read())["pieces"]
                 for l in pc["lines"] if l[0][0] == 40 * P.TIER0_Q]
    straight1 = P.decode_lines(open(os.path.join(out, "c", "40_40.l.bin"), "rb").read())["pieces"][0]["lines"]
    assert len(straight0) == 1 and len(straight0[0][0]) == 2 and len(straight1[0][0]) == 51   # tier 0 is simplified
    dl = P.decode_lines(open(os.path.join(out, P.LINES_TIER0), "rb").read())
    assert dl["q"] == P.TIER0_Q and dl["kinds"] == 7
    assert {pc["kind"] for pc in dl["pieces"]} == {0, 1, 2}
    assert all(l[2] < P.MZ_SPLIT for pc in dl["pieces"] for l in pc["lines"])  # the zoom-8.6 state line is not in tier 0
    dp = P.decode_places(open(os.path.join(out, P.PLACES_TIER0), "rb").read())
    assert dp["q"] == P.TIER0_Q
    assert [p["name"] for p in dp["places"]] == ["Capital", "Honolulu", "Ganzhou", "København", "Wahiawa"]
    cells = index["tier1"]["cells"]
    assert {"0_0", "0_10", "10_20", "60_170", "60_-180", "30_-120", "40_-120", "30_-110", "20_-160", "10_10"} <= set(cells)
    assert "30_-80" not in cells                                                   # the state line across water is left out
    assert [cells[n][0] > 0 for n in ("0_0", "0_10", "10_20", "10_10")] == [True, True, True, False]   # 10_10: a town only
    oahu = P.decode_places(open(os.path.join(out, "c", "20_-160.p.bin"), "rb").read())
    assert oahu["q"] == Q and [p["name"] for p in oahu["places"]] == ["Kailua", "Smalltown", "Makakilo", "Hale‘iwa"]
    assert cells["20_-160"][0] == 0 and not os.path.exists(os.path.join(out, "c", "20_-160.l.bin"))
    assert "20_110" not in cells and "50_10" not in cells                          # the Ganzhou and Copenhagen twins are gone
    late = P.decode_lines(open(os.path.join(out, "c", "30_-110.l.bin"), "rb").read())
    assert [(pc["kind"], [l[2] for l in pc["lines"]]) for pc in late["pieces"]] == [(1, [86])]
    east = P.decode_lines(open(os.path.join(out, "c", "60_170.l.bin"), "rb").read())
    west = P.decode_lines(open(os.path.join(out, "c", "60_-180.l.bin"), "rb").read())
    e, w = east["pieces"][0]["lines"][0], west["pieces"][0]["lines"][0]
    assert east["pieces"][0]["kind"] == west["pieces"][0]["kind"] == P.KIND_DISPUTED
    assert e[0][-1] == 180 * Q and w[0][0] == -180 * Q and e[1][-1] == w[1][0]    # the same latitude on both sides
    edge = P.decode_places(open(os.path.join(out, "c", "60_-180.p.bin"), "rb").read())
    assert edge["places"][0]["lon"] == -180 * Q
    assert len(index["tier1"]["lines_sha256"]) == 64
    assert P.main(["--check", out]) == 0


def test_cell_cap_keeps_the_largest(tmp_path, monkeypatch):
    monkeypatch.setattr(P, "CELL_CAP", 2)
    index, out = build(tmp_path)
    assert index["counts"]["capped"] == 2
    oahu = P.decode_places(open(os.path.join(out, "c", "20_-160.p.bin"), "rb").read())
    assert [p["name"] for p in oahu["places"]] == ["Kailua", "Smalltown"]
    assert P.check(out, **QUIET) == []


def rewrite(path, data):
    open(path, "wb").write(data)


def reindex(out):
    """Make the index agree with the files again (what a buggy builder would write): sizes and hashes."""
    ip = os.path.join(out, "index.json")
    index = json.load(open(ip))
    bl = open(os.path.join(out, P.LINES_TIER0), "rb").read(); bp = open(os.path.join(out, P.PLACES_TIER0), "rb").read()
    t0 = index["tier0"]
    t0["lines"].update(bytes=len(bl), sha256=P.sha(bl), lines=sum(len(pc["lines"]) for pc in P.decode_lines(bl)["pieces"]))
    t0["places"].update(bytes=len(bp), sha256=P.sha(bp), count=len(P.decode_places(bp)["places"]))
    t0["sha256"] = P.tier0_digest(P.sha(bl), P.sha(bp))
    digests, cells = {}, {}
    for name in index["tier1"]["cells"]:
        lb = pb = 0
        for ext in (".l", ".p"):
            path = os.path.join(out, "c", name + ext + ".bin")
            if os.path.exists(path):
                b = open(path, "rb").read()
                digests[name + ext] = P.sha(b)
                lb, pb = (len(b), pb) if ext == ".l" else (lb, len(b))
        if lb or pb:
            cells[name] = [lb, pb]
    index["tier1"].update(cells=cells, sha256=P.C.cells_digest(digests),
                          lines_sha256=P.C.cells_digest({k: v for k, v in digests.items() if k.endswith(".l")}))
    json.dump(index, open(ip, "w"))


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


def test_check_catches_a_wrong_build_whose_index_agrees(tmp_path):
    """G34a F2: damage with the index regenerated to match the files."""
    index, out = build(tmp_path)
    tp = os.path.join(out, P.PLACES_TIER0)
    d = P.decode_places(open(tp, "rb").read())
    bad = [dict(p, lat=p["lat"] / P.TIER0_Q, lon=p["lon"] / P.TIER0_Q) for p in d["places"]]
    raw = P.encode_places(bad, q=P.TIER0_Q)
    rewrite(tp, raw[:8] + struct.pack("<I", Q) + raw[12:])                    # the header says q 10000 over q 1000 data
    reindex(out)
    assert any("tier 0 places: header q 10000, expected 1000" in p for p in P.check(out, **QUIET))
    index, out = build(tmp_path / "b")
    tl = os.path.join(out, P.LINES_TIER0)
    d = P.decode_lines(open(tl, "rb").read())
    rewrite(tl, P.encode_lines([(pc["kind"], pc["lines"]) for pc in d["pieces"]], q=Q))   # tier 0 lines at the tier-1 q
    reindex(out)
    assert any("tier 0 lines: header q 10000 / cell 10, expected 1000 / 10" in p for p in P.check(out, **QUIET))
    index, out = build(tmp_path / "c")
    oahu = os.path.join(out, "c", "20_-160.p.bin")
    ps = P.decode_places(open(oahu, "rb").read())["places"]
    rewrite(oahu, P.encode_places([dict(p, lat=p["lat"] / Q, lon=p["lon"] / Q) for p in ps[:1]]))   # three towns lost
    reindex(out)
    assert P.check(out, **QUIET) == []                                            # without the real gates a lost town is invisible ...
    monkeypatch_real(out)
    probs = P.check(out, **QUIET)
    assert any("GeoNames towns decoded" in p for p in probs)                      # ... with them it is counted from the files


def monkeypatch_real(out):
    """Mark a synthetic build as built from the pinned Natural Earth files (its index), for the real gates' counters."""
    ip = os.path.join(out, "index.json")
    index = json.load(open(ip))
    index["sources"]["natural_earth"]["files"] = {f: h for f, h in P.NE_FILES.values()}
    json.dump(index, open(ip, "w"))


@pytest.fixture
def real_gates(monkeypatch):
    """The real gates set up to fit the synthetic world, so a test can break one thing at a time."""
    monkeypatch.setattr(P, "PIN_TIER0_SHA256", None); monkeypatch.setattr(P, "PIN_LINES1_SHA256", None)
    monkeypatch.setattr(P, "NE_PLACES_RANGE", (7, 7)); monkeypatch.setattr(P, "GN_MIN_KEPT", 6)
    monkeypatch.setattr(P, "CELLS_RANGE", (1, 100))
    monkeypatch.setattr(P, "LINE_PROBES", [(1, -120.0, 42.0), (0, 15.0, 5.0), (2, 1.0, 1.0)])
    monkeypatch.setattr(P, "NO_LINE_PROBES", [(1, -76.1, 37.6)])
    monkeypatch.setattr(P, "PLACE_PROBES", [("tier0", "honolulu", 1, 40), ("20_-160", "haleiwa", 5, 100)])
    monkeypatch.setattr(P, "TWIN_PROBES", [("ganzhou", "ganzhou", 20), ("kobenhavn", "copenhagen", 20),
                                           ("honolulu", "urbanhonolulu", 20)])
    return monkeypatch


def test_check_runs_the_real_gates_for_the_pinned_natural_earth(tmp_path, monkeypatch):
    index, out = build(tmp_path)
    monkeypatch_real(out)
    probs = P.check(out, **QUIET)
    assert any(p.startswith("pin: tier 0") for p in probs) and any(p.startswith("pin: tier-1 lines") for p in probs)
    assert any("Natural Earth places decoded" in p for p in probs) and any("GeoNames towns decoded" in p for p in probs)
    assert any(p.startswith("counts: ") and "tier-1 cells" in p for p in probs)
    assert any(p.startswith("probe: tier 0 has no kind-0") for p in probs) and any("no place 'tokyo'" in p for p in probs)
    assert any("twin probe: no Natural Earth place 'tokyo'" in p for p in probs)
    assert not any("'honolulu'" in p for p in probs) and not any("'haleiwa'" in p for p in probs)   # these are in the sources
    assert not any("-120.0,42.0" in p for p in probs)                          # the statistical state line came through
    assert not any("where none belongs" in p for p in probs)                   # the line across water is not there


def test_real_gates_pass_a_fitting_build_and_name_what_breaks(tmp_path, real_gates):
    index, out = build(tmp_path)
    monkeypatch_real(out)
    assert P.check(out, **QUIET) == []
    real_gates.setattr(P, "PIN_TIER0_SHA256", "0" * 64)
    assert any(p.startswith("pin: tier 0") for p in P.check(out, **QUIET))
    real_gates.setattr(P, "PIN_TIER0_SHA256", index["tier0"]["sha256"])
    real_gates.setattr(P, "PIN_LINES1_SHA256", index["tier1"]["lines_sha256"])
    assert P.check(out, **QUIET) == []
    real_gates.setattr(P, "LINE_PROBES", [(1, 15.0, 5.0)])                       # a kind-0 vertex is no kind-1 vertex
    assert any(p.startswith("probe: tier 0 has no kind-1") for p in P.check(out, **QUIET))


def test_real_gates_count_places_from_the_files_not_the_index(tmp_path, real_gates):
    real_gates.setattr(P, "GN_MIN_KEPT", 1); real_gates.setattr(P, "NE_PLACES_RANGE", (1, 100))
    index, out = build(tmp_path)
    monkeypatch_real(out)
    assert P.check(out, **QUIET) == []
    oahu = os.path.join(out, "c", "20_-160.p.bin")
    ps = P.decode_places(open(oahu, "rb").read())["places"]
    gone = [p for p in ps if p["name"] != "Smalltown"]                           # a Natural Earth place lost
    assert len(gone) == len(ps) - 1
    rewrite(oahu, P.encode_places([dict(p, lat=p["lat"] / Q, lon=p["lon"] / Q) for p in gone]))
    reindex(out)
    probs = P.check(out, **QUIET)
    assert any("Natural Earth places decoded" in p for p in probs) and not any("GeoNames towns" in p for p in probs)
    index, out = build(tmp_path / "b")
    monkeypatch_real(out)
    oahu = os.path.join(out, "c", "20_-160.p.bin")
    ps = P.decode_places(open(oahu, "rb").read())["places"]
    gone = [p for p in ps if p["name"] != "Kailua"]                               # a GeoNames town lost
    rewrite(oahu, P.encode_places([dict(p, lat=p["lat"] / Q, lon=p["lon"] / Q) for p in gone]))
    reindex(out)
    probs = P.check(out, **QUIET)
    assert any("GeoNames towns decoded" in p for p in probs) and not any("Natural Earth places" in p for p in probs)


def test_check_compares_the_tier1_lines_hash(tmp_path):
    index, out = build(tmp_path)
    ip = os.path.join(out, "index.json")
    json.dump(dict(index, tier1=dict(index["tier1"], lines_sha256="0" * 64)), open(ip, "w"))
    assert P.check(out, **QUIET) == ["tier 1: lines hash != index (tier1.lines_sha256)"]


def test_real_gates_catch_a_twin_and_a_line_across_water(tmp_path, real_gates):
    real_gates.setattr(P, "dedupe", lambda ne, gn: (gn, {"id": 0, "name": 0, "twin": 0}))
    real_gates.setattr(P, "GN_MIN_KEPT", 1)
    index, out = build(tmp_path)
    monkeypatch_real(out)
    probs = P.check(out, **QUIET)
    assert any("twin probe: GeoNames 'Ganzhou'" in p for p in probs)
    assert any("twin probe: GeoNames 'Copenhagen'" in p for p in probs)
    assert any("twin probe: GeoNames 'Urban Honolulu'" in p for p in probs)


def test_real_gates_catch_a_state_line_across_water(tmp_path, real_gates):
    real_gates.setattr(P, "ADMIN1_CLASSES", dict(P.ADMIN1_CLASSES, **{"Admin-1 boundary indicator": P.KIND_ADMIN1}))
    index, out = build(tmp_path)
    monkeypatch_real(out)
    probs = P.check(out, **QUIET)
    assert any("kind-1 line near -76.1,37.6, where none belongs" in p and p.startswith("probe: tier 1") for p in probs)
    assert any("kind-1 line near -76.1,37.6, where none belongs" in p and p.startswith("probe: tier 0") for p in probs)


def test_check_branches_for_names_zooms_counts_and_repeats(tmp_path):
    index, out = build(tmp_path)
    cell = os.path.join(out, "c", "20_-160.p.bin")
    ps = P.decode_places(open(cell, "rb").read())["places"]
    rewrite(cell, P.encode_places([dict(ps[0], name="Two  Spaces", lat=ps[0]["lat"] / Q, lon=ps[0]["lon"] / Q)]
                                  + [dict(p, lat=p["lat"] / Q, lon=p["lon"] / Q) for p in ps[1:]]))
    reindex(out)
    assert any("bad name 'Two  Spaces'" in p for p in P.check(out, **QUIET))
    index, out = build(tmp_path / "b")
    tp = os.path.join(out, P.PLACES_TIER0)
    ps = P.decode_places(open(tp, "rb").read())["places"]
    late = [dict(p, lat=p["lat"] / P.TIER0_Q, lon=p["lon"] / P.TIER0_Q) for p in ps]
    late[-1]["mz10"] = 80
    rewrite(tp, P.encode_places(late, q=P.TIER0_Q))
    reindex(out)
    assert any("min_zoom 80 belongs in a cell" in p for p in P.check(out, **QUIET))
    index, out = build(tmp_path / "c")
    ip = os.path.join(out, "index.json")
    idx = json.load(open(ip)); idx["tier0"]["lines"]["lines"] += 1
    json.dump(idx, open(ip, "w"))
    assert any("tier 0 lines: line count != index" in p for p in P.check(out, **QUIET))
    rep = P.encode_lines([(0, [(np.array([10000, 10000, 20000]), np.array([10000, 10000, 20000]), 0)])])
    assert any("repeated point" in p for p in P.check_lines(P.decode_lines(rep), (0, 0, 10)))


def test_upload_needs_the_real_gates(tmp_path, capsys):
    index, out = build(tmp_path)
    assert P.check(out, **QUIET) == []                                            # a clean synthetic build ...
    assert P.main(["--out", out, "--upload", "static/places/v1"]) == 1           # ... is not uploaded: not the pinned sources
    assert "real-build gates off" in capsys.readouterr().out
    P.check(out, log=print)
    assert "real-build gates OFF" in capsys.readouterr().out


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
    assert "world-lines.bin" in keys and "world-places.bin" in keys and "c/20_-160.p.bin" in keys
    on_disk = {P.LINES_TIER0, P.PLACES_TIER0} | {"c/" + n for n in os.listdir(os.path.join(out, "c"))}
    assert set(keys[1:-1]) == on_disk and "c/20_-160.l.bin" not in keys
    lic = s3.objects["static/places/v1/LICENSE.txt"]["body"].decode("utf-8")
    assert "CC BY 4.0" in lic and "public domain" in lic and P.NE_COMMIT in lic and "8,000" in lic
    assert all(o["cc"] == "public, max-age=31536000, immutable" for o in s3.objects.values())
    n = len(s3.log)
    assert P.C.upload(out, "static/places/v1", s3=s3, bucket="b", files=P.files_of, license_file=P.LICENSE_FILE, **QUIET)
    assert s3.log[n:] == ["static/places/v1/LICENSE.txt", "static/places/v1/index.json"]    # the same build: nothing else
    idx = json.load(open(os.path.join(out, "index.json")))
    idx["tier1"]["sha256"] = "0" * 64
    json.dump(idx, open(os.path.join(out, "index.json"), "w"))
    n, msgs = len(s3.log), []
    assert P.C.upload(out, "static/places/v1", s3=s3, bucket="b", files=P.files_of, license_file=P.LICENSE_FILE,
                      log=msgs.append) is False
    assert len(s3.log) == n and any("tier1 000000000000" in m for m in msgs)     # the refusal names the tier-1 hashes


def test_tier0_digest_covers_both_files():
    assert len({P.tier0_digest("a", "b"), P.tier0_digest("a", "c"), P.tier0_digest("c", "b")}) == 3


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
    assert P.NE_FILES["places"][0] == "ne_10m_populated_places.geojson"         # the full file: it carries GEONAMESID
    assert "cities1000.zip" in text and "tests/places" in text and "tools/places/**" in text
    assert 'case "$SET" in coast|lakes|lakeoutlines|places)' in text


def test_a_town_on_a_cell_edge_is_filed_by_its_stored_position(tmp_path):
    """Real build: a town at lon 9.99996 rounds to 10.0000 in the file, so it belongs to the cell east of 10 E."""
    gz = tmp_path / "cities1000.zip"
    with zipfile.ZipFile(gz, "w") as z:
        z.writestr("cities1000.txt", gn_row(1, "Edge", 5.00004, 9.99996, "PPLA", 5000) + "\n")
    gn, _date, _s = P.read_geonames(str(gz))
    assert (gn[0]["lat"], gn[0]["lon"]) == (5.0, 10.0) and P.place_cell(gn[0]) == (0, 10)
    assert gn[0]["rank"] == 1                                                     # PPLA: a state capital
    d = P.decode_places(P.encode_places(gn))
    assert P.check_places(d, (0, 10, 10)) == [] and P.check_places(d, (0, 0, 10)) != []
    ne = tmp_path / "ne.geojson"
    write_geojson(ne, [point_feature("Edge NE", 5.00004, 9.99996, 5000, 8)])
    p = P.read_ne_places(str(ne))[0]
    assert (p["lat"], p["lon"]) == (5.0, 10.0) and P.place_cell(p) == (0, 10)
    assert P.place_cell({"lat": 90.0, "lon": 0.0}) == (80, 0)                    # the pole belongs to the last row of cells


def test_small_details_the_mutants_found():
    assert P.clean_name("Granada‎") == "Granada"                            # a format character (LRM) goes
    cut = P.clean_name("a" * 58 + " éx")                                    # the cut ends on the space: stripped
    assert cut == "a" * 58
    with pytest.raises(ValueError):
        P.decode_varints(b"\x81")
    with pytest.raises(ValueError):
        P.decode_varints(b"\x01\x81")
    sx, _sy = P.simplify(np.array([0.0, 10.0, 20.0]), np.array([0.0, 0.05, 0.0]), 0.1)
    assert sx.tolist() == [0.0, 20.0]                                             # a 0.05-degree bump on a 20-degree segment
    runs = P.split_dateline([170.0, -170.0, 170.0], [0.0, 10.0, 20.0])          # two crossings
    assert len(runs) == 3 and runs[1][0][0] == -180.0 and runs[1][0][-1] == -180.0 and runs[2][0][0] == 180.0
    assert runs[0][1][-1] == runs[1][1][0] and runs[1][1][-1] == runs[2][1][0]
    two = P.encode_lines([(2, [(np.array([0, 5]), np.array([0, 5]), 0)])])
    assert P.decode_lines(two)["pieces"][0]["kind"] == 2
    bad = two[:P.LINES_HEADER.size + 4] + bytes([3]) + two[P.LINES_HEADER.size + 5:]
    with pytest.raises(ValueError):
        P.decode_lines(bad)                                                       # kind 3 does not exist
