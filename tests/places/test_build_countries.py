"""Country names builder (tools/places/build_countries.py, plan section 114): the name rule, short forms, island list,
area tiers, the end rule, the wrap, deterministic output, and the committed static_ui/countries.json. Synthetic
sources only (no network); the committed file is checked with the real-build probes."""
import hashlib
import json
import math
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "places"))

import build_countries as B  # noqa: E402

COMMITTED = os.path.join(ROOT, "static_ui", "countries.json")
# The committed file's bytes: a change must be deliberate (rebuild from the pinned file, then update this pin).
PIN_SHA256 = "067e126976e07229d7f7b8288e4d69e65962f6bc1c607fd722e47fbb810c2693"


def square(lon, lat, half):
    """A lon/lat square of side 2 * half degrees around (lon, lat)."""
    return [[lon - half, lat - half], [lon + half, lat - half], [lon + half, lat + half], [lon - half, lat + half],
            [lon - half, lat - half]]


def feature(name, lon, lat, half, name_en=None, type_="Sovereign country", holes=()):
    return {"type": "Feature",
            "properties": {"NAME": name, "NAME_EN": name_en or name, "TYPE": type_, "LABEL_X": lon, "LABEL_Y": lat},
            "geometry": {"type": "Polygon", "coordinates": [square(lon, lat, half)] + [square(lon, lat, h) for h in holes]}}


def every_hand_name():
    """Features for every ISLANDS and SHORT_FORMS name (the builder refuses a hand-list name that matches nothing)."""
    out = []
    for i, n in enumerate(sorted(B.ISLANDS | set(B.SHORT_FORMS))):
        out.append(feature(n, -170 + i * 2.0, -40.0, 0.05))
    return out


def built(features):
    src = json.dumps({"type": "FeatureCollection", "features": features}).encode("utf-8")
    d = json.loads(B.build(src).decode("utf-8"))
    return {r[0]: r for r in d["countries"]}, d


# ---------------------------------------------------------------- pieces

def test_ring_area_matches_the_sphere():
    # a 1 x 1 degree square on the equator: R^2 * dlon * (sin 0.5 - sin -0.5) = 12,364 km2
    want = B.R_KM ** 2 * math.radians(1.0) * 2 * math.sin(math.radians(0.5))
    assert B.ring_area_km2(square(0, 0, 0.5)) == pytest.approx(want, rel=1e-3)
    assert B.ring_area_km2(list(reversed(square(0, 0, 0.5)))) == pytest.approx(want, rel=1e-3)   # either winding
    g = {"type": "Polygon", "coordinates": [square(0, 0, 0.5), square(0, 0, 0.25)]}
    assert B.geometry_area_km2(g) == pytest.approx(want * 0.75, rel=1e-2)                         # the hole subtracted
    mg = {"type": "MultiPolygon", "coordinates": [[square(0, 0, 0.5)], [square(10, 0, 0.5)]]}
    assert B.geometry_area_km2(mg) == pytest.approx(2 * want, rel=1e-3)


def test_base_name_rule():
    assert B.base_name({"NAME": "China", "NAME_EN": "People's Republic of China"}) == "China"
    assert B.base_name({"NAME": "Dem. Rep. Congo", "NAME_EN": "Democratic Republic of the Congo"}) == "Democratic Republic of the Congo"
    assert B.base_name({"NAME": "N. Cyprus", "NAME_EN": "Turkish Republic of Northern Cyprus"}) == "Northern Cyprus"
    assert B.base_name({"NAME": "eSwatini", "NAME_EN": "Eswatini"}) == "Eswatini"
    assert B.base_name({"NAME": "Fr. Polynesia", "NAME_EN": None}) == "Fr. Polynesia"            # no NAME_EN: keep NAME
    # abbreviations without a dot (G80 A-6): "Saint" spelled out, as Saint Lucia
    assert B.base_name({"NAME": "St-Martin", "NAME_EN": "Saint Martin"}) == "Saint Martin"
    assert B.base_name({"NAME": "St-Barthélemy", "NAME_EN": "Saint Barthélemy"}) == "Saint Barthélemy"
    assert B.SHORT_FORMS["Wallis and Futuna"] == "Wallis & Futuna"                       # pairs use "&" (decision 5)


def test_wrap_at_the_space_nearest_the_middle():
    assert B.wrap("French Polynesia") == "French Polynesia"                  # 16: one line
    assert B.wrap("Central African Rep.") == "Central\nAfrican Rep."
    assert B.wrap("Fr. Southern & Antarctic Lands") == "Fr. Southern &\nAntarctic Lands"
    assert B.wrap("Abcdefghijklmnopqrstu") == "Abcdefghijklmnopqrstu"       # no space: one line
    # never right after a one-letter abbreviation (G80 A-6)
    assert B.wrap("S. Georgia & S. Sandwich Is.") == "S. Georgia &\nS. Sandwich Is."
    assert B.wrap("Abcdefgh S. Ijklmnopq") == "Abcdefgh\nS. Ijklmnopq"
    assert B.wrap("Ab C. Defghijklmnopqr") == "Ab\nC. Defghijklmnopqr"     # the other space, however far from the middle
    assert B.wrap("X. Abcdefghijklmnopqr") == "X. Abcdefghijklmnopqr".replace(" ", "\n", 1)   # no other space: still cut


def test_start_by_area_tier_and_islands():
    # zooms (x 10) after the screen's widest view (owner, 2026-11-27)
    assert B.start10_of(1_500_000, False) == (0, 0) and B.start10_of(1_499_999, False) == (10, 1)
    assert B.start10_of(500_000, False) == (10, 1) and B.start10_of(499_999, False) == (18, 2)
    assert B.start10_of(100_000, False) == (18, 2) and B.start10_of(99_999, False) == (25, 3)
    assert B.start10_of(20_000, False) == (25, 3) and B.start10_of(19_999, False) == (32, 4)
    assert B.start10_of(0, False) == (32, 4)
    assert B.start10_of(500, True) == (B.ISLAND_ZOOM10, B.ISLAND_SIZE) == (15, 3)   # an island nation comes earlier
    assert B.start10_of(600_000, True) == (10, 1)                              # ... never later than its area tier


def test_end_rule():
    # a country 900 px across at zoom z: sqrt(area) = 900 * m/px(z); m/px(z) = M_PER_PX_Z0 * cos(lat) / 2^z
    side_km = 900 * B.M_PER_PX_Z0 / 2 ** 5.0 / 1000.0                          # exactly 900 px at zoom 5.0 on the equator
    assert B.end10_of(side_km ** 2, 0.0) == 50
    assert B.end10_of((side_km * 1.01) ** 2, 0.0) == 49                       # a little bigger: gone a little sooner
    assert B.end10_of(side_km ** 2, 60.0) == 40                               # cos 60 = 1/2: one zoom sooner
    assert B.end10_of(1.0, 0.0) == B.END_CAP10                                # tiny: the cap
    assert B.end10_of(1e12, 0.0) == 10                                        # never before zoom 1


# ---------------------------------------------------------------- build

def test_build_applies_every_rule():
    feats = every_hand_name() + [
        feature("Bigland", 0, 0, 4.0),                                   # ~790,000 km2 -> tier 1
        feature("Hugeland", 60, 0, 5.0),                                 # ~1.23 M km2 -> tier 1 too
        feature("Smallia", 30, 0, 0.5),                                  # ~12,000 km2 -> tier 4
        feature("Lease Base", 40, 0, 0.5, type_="Lease"),                # left out by type
        feature("Akrotiri", 41, 0, 0.5),                                 # left out by name
    ]
    by, d = built(feats)
    assert "Lease Base" not in by and "Akrotiri" not in by
    assert by["Bigland"][3:] == [10, by["Bigland"][4], 1] and by["Smallia"][3] == 32 and by["Smallia"][5] == 4
    assert by["Fiji"][3] == 15 and by["Fiji"][5] == B.ISLAND_SIZE
    assert "USA" in by and "United States of America" not in by and "DR Congo" in by
    assert "Fr. Southern &\nAntarctic Lands" in by                     # a short form still long: wrapped
    assert by["Bigland"][1:3] == [0.0, 0.0]
    order = [r[3] for r in d["countries"]]
    assert order == sorted(order)                                        # by start
    tier1 = [r[0] for r in d["countries"] if r[3] == 10]
    assert tier1.index("Hugeland") < tier1.index("Bigland")              # then the larger first
    assert d["count"] == len(d["countries"]) and d["format"] == B.FORMAT


def test_build_refuses_a_hand_list_name_that_matches_nothing():
    feats = [f for f in every_hand_name() if f["properties"]["NAME"] != "Tuvalu"]
    with pytest.raises(ValueError, match="Tuvalu"):
        built(feats)


def test_build_is_deterministic():
    feats = every_hand_name() + [feature("Bigland", 0, 0, 4.0)]
    src = json.dumps({"type": "FeatureCollection", "features": feats}).encode("utf-8")
    src2 = json.dumps({"type": "FeatureCollection", "features": list(reversed(feats))}).encode("utf-8")
    a, b = B.build(src), B.build(src2)
    assert json.loads(a)["countries"] == json.loads(b)["countries"]


def test_check_flags_bad_files():
    good = {"format": B.FORMAT, "count": 1, "countries": [["Bigland", 0.0, 0.0, 10, 60, 1]]}
    assert B.check(json.dumps(good).encode(), real=False) == []
    for bad, what in ((["Bigland", 0.0, 0.0, 45, 70, 1], "zooms"), (["Bigland", 0.0, 0.0, 10, 75, 1], "zooms"),
                      (["Bigland", 0.0, 0.0, 10, 5, 1], "zooms"), (["Bigland", 0.0, 0.0, 1.5, 60, 1], "zooms"),
                      (["Bigland", 95.0, 0.0, 10, 60, 1], "position"), (["Bigland", 0.0, 0.0, 10, 60, 7], "size"),
                      (["", 0.0, 0.0, 10, 60, 1], "label"), (["Central African Republic", 0.0, 0.0, 10, 60, 1], "wrapped")):
        p = B.check(json.dumps(dict(good, countries=[bad])).encode(), real=False)
        assert any(what in x for x in p), (bad, p)
    assert any("count" in x for x in B.check(json.dumps(dict(good, count=2)).encode(), real=False))
    assert B.check(b"{", real=False)[0].startswith("not JSON")
    # G80 A-9: problems, never an exception, for a text position, a top-level list, booleans
    for bad in (["Bigland", "0", 0.0, 10, 60, 1], ["Bigland", True, 0.0, 10, 60, 1], ["Bigland", 0.0, None, 10, 60, 1]):
        assert any("position" in x for x in B.check(json.dumps(dict(good, countries=[bad])).encode(), real=False)), bad
    assert any("zooms" in x for x in B.check(json.dumps(dict(good, countries=[["Bigland", 0.0, 0.0, True, 60, 1]])).encode(), real=False))
    assert any("size" in x for x in B.check(json.dumps(dict(good, countries=[["Bigland", 0.0, 0.0, 10, 60, True]])).encode(), real=False))
    assert B.check(b"[]", real=False) == ["not a countries object"]
    assert B.check(json.dumps(dict(good, countries={"a": 1})).encode(), real=False) == ["countries is not a list"]


def test_check_finds_a_long_name_left_unshortened():
    # G80 A-9: every SHORT_FORMS key is longer than WRAP_CHARS, so an unshortened one arrives wrapped
    data = open(COMMITTED, "rb").read().decode("utf-8")
    assert '"Bosnia & Herz."' in data
    bad = data.replace('"Bosnia & Herz."', '"Bosnia and\\nHerzegovina"')
    assert bad != data
    assert any("Bosnia and Herzegovina" in x and "long name" in x for x in B.check(bad.encode("utf-8"))), B.check(bad.encode("utf-8"))


def test_ne_commit_is_the_places_pin():
    pytest.importorskip("numpy")
    import build_places as P
    assert B.NE_COMMIT == P.NE_COMMIT


# ---------------------------------------------------------------- the committed file

def test_committed_file_passes_the_real_checks_and_the_pin():
    data = open(COMMITTED, "rb").read()
    assert B.check(data) == []
    assert hashlib.sha256(data).hexdigest() == PIN_SHA256
    by = {r[0]: r for r in json.loads(data)["countries"]}
    # the owner's examples: the largest at world view, Spain a little closer, the islands at ocean views, all gone by 7
    assert {"Russia", "Canada", "USA", "China", "Brazil", "Australia"} <= {k for k, r in by.items() if r[3] == 0}
    assert by["Spain"][3] == 10 and by["Fiji"][3] == 15 and by["Tonga"][3] == 15
    assert max(r[4] for r in by.values()) == 70
    assert by["Russia"][4] < by["USA"][4] < by["Spain"][4]
    assert all("\n" in k or len(k) <= B.WRAP_CHARS or " " not in k for k in by)
