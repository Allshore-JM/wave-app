"""Island names builder (tools/places/build_islands.py, plan section 115): the name rule and formal spellings, the
country duplicates, skips and the addition, groups against islands, the label point, the end rule and its cap,
deterministic output, and the committed static_ui/islands.json. Synthetic sources only (no network)."""
import hashlib
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "places"))

import build_islands as B  # noqa: E402
import build_countries as C  # noqa: E402

COMMITTED = os.path.join(ROOT, "static_ui", "islands.json")
# The committed file's bytes: a change must be deliberate (rebuild from the pinned file, then update this pin).
PIN_SHA256 = "e4c743bbce9ecf77e8dbd54417c93fe2edea2292f2af35f2401dbc61bd080b5d"


def square(lon, lat, half):
    return [[lon - half, lat - half], [lon + half, lat - half], [lon + half, lat + half], [lon - half, lat + half],
            [lon - half, lat - half]]


def feature(name, name_en, cla="Island", min_label=5.4, lon=0.0, lat=0.0, half=0.1, geom=None):
    return {"type": "Feature", "properties": {"FEATURECLA": cla, "NAME": name, "NAME_EN": name_en, "MIN_LABEL": min_label},
            "geometry": geom or {"type": "Polygon", "coordinates": [square(lon, lat, half)]}}


def every_hand_name():
    """One synthetic feature per hand-list entry, so a build never refuses for a missing one."""
    fs = [feature(n, n, "Island group") for n in B.SKIP] + [feature(n, n) for n in B.NOT_ISLANDS]
    fs += [feature(n, n, "Island group", lon=50 + i) for i, n in enumerate(sorted(B.ALIAS_DUPES))]
    fs += [feature(k, k, lon=-60 + 3 * i, lat=-40) for i, k in enumerate(sorted(B.NAME_FIXES))]
    fs += [feature(n, "Wrong", lon=100, lat=60) for n in B.NAME_EN_FIXES]
    return fs


def build(extra, countries=frozenset()):
    src = json.dumps({"type": "FeatureCollection", "features": every_hand_name() + extra}).encode()
    return json.loads(B.build(src, countries=set(countries)))["islands"]


def by_label(rows):
    return {r[0].replace("\n", " "): r for r in rows}


def test_names_formal_spellings_and_fixes():
    rows = by_label(build([]))
    for en, label in (("Oahu", "Oʻahu"), ("Kauai", "Kauaʻi"), ("Lanai", "Lānaʻi"), ("Hawaii", "Hawaiʻi"),
                      ("Savai'i", "Savaiʻi"), ("Honshu", "Honshū"), ("Galapagos Islands", "Galápagos Islands"),
                      ("Chuk-to", "Jeju Island"), ("Mainland Tasmania", "Tasmania"), ("Ryan Bullard", "Utupua")):
        assert label in rows and en not in rows, (en, label)
    assert "ʻ" in "Oʻahu" and all("‘" not in k for k in rows)
    assert "Bear Islands" in rows and "Wrong" not in rows, "NAME_EN_FIXES: the feature's own English name is wrong"
    assert "Kahoʻolawe" in rows and rows["Kahoʻolawe"][3] == 54, "added by hand"


def test_english_name_first_else_name():
    rows = by_label(build([feature("Sicilia", "Sicily", lon=14, lat=37.5), feature("Nias", None, lon=97, lat=1)]))
    assert "Sicily" in rows and "Sicilia" not in rows
    assert "Nias" in rows, "no NAME_EN: Natural Earth's NAME"


def test_left_out_regions_not_islands_country_repeats_and_aliases():
    rows = by_label(build([feature("CUBA", "Cuba", lon=-79, lat=21.5), feature("Gotland", "Gotland", lon=18.5, lat=57.5)],
                          countries={"cuba"}))
    assert "Cuba" not in rows, "repeats a country label"
    assert "Gotland" in rows
    for n in list(B.SKIP) + list(B.NOT_ISLANDS) + list(B.ALIAS_DUPES):
        assert n not in rows, n


def test_a_hand_name_that_matches_nothing_is_refused():
    src = json.dumps({"type": "FeatureCollection", "features": every_hand_name()[1:]}).encode()
    with pytest.raises(ValueError, match="match no island"):
        B.build(src, countries=set())


def test_group_or_island_by_the_name_not_natural_earths_class():
    rows = by_label(build([feature("Maui", "Maui", "Island group", lon=-156.3, lat=20.8),
                           feature("Pribilof Islands", "Pribilof Islands", "Island", lon=-170, lat=57),
                           feature("Svalbard", "Svalbard", "Island group", lon=20, lat=79)]))
    assert rows["Maui"][5] == 1, "Natural Earth files Maui as a group"
    assert rows["Pribilof Islands"][5] == 0 and rows["Svalbard"][5] == 0
    assert B.kind_of("Lau Group") == 0 and B.kind_of("Ralik Chain") == 0 and B.kind_of("Bali") == 1


def test_label_point_lies_on_land_for_a_crescent():
    # a C-shaped island: its centroid falls in the bay, the pole of inaccessibility on land
    c = [[0, 0], [3, 0], [3, 1], [1, 1], [1, 2], [3, 2], [3, 3], [0, 3], [0, 0]]
    lon, lat = B.polylabel([c])
    assert B._ring_dist(lon, lat, [[(x, y) for x, y in c]]) > 0, (lon, lat)
    assert lon < 1, "in the solid back of the C, not in the bay"
    sq_lon, sq_lat = B.polylabel([square(10, 20, 1)])
    assert abs(sq_lon - 10) < 0.02 and abs(sq_lat - 20) < 0.07          # a 2x2-degree "square" is taller than wide at 20 N
    mp = {"type": "MultiPolygon", "coordinates": [[square(0, 0, 0.1)], [square(50, 10, 1)]]}
    lon, lat = B.label_point(mp)
    assert abs(lon - 50) < 0.05 and abs(lat - 10) < 0.05, "the largest part"


def test_start_end_rule_min_span_and_cap():
    assert B.end10_of(1545, 21.5, 53) == pytest.approx(116, abs=1), "Oʻahu about 900 px across near zoom 11.7"
    assert B.end10_of(0.5, 0, 60) == B.END_CAP10 == 120, "a tiny atoll: never after zoom 12"
    assert B.end10_of(5_000_000, 70, 40) == 55, "a huge one: kept at least MIN_SPAN10 after it came"
    rows = by_label(build([feature("Bali", "Bali", min_label=5.4, lon=115.2, lat=-8.4, half=0.35)]))
    assert rows["Bali"][3] == 54, "start = Natural Earth's MIN_LABEL x 10"


def test_duplicates_within_500_km_are_one_the_larger_kept_far_apart_both():
    rows = build([feature("Batu Is.", "Batu Islands", "Island group", lon=98.4, lat=-0.4, half=0.3),
                  feature("Batu Is.", "Batu Islands", "Island group", lon=98.6, lat=-0.2, half=0.1),
                  feature("Flores", "Flores", lon=120.4, lat=-8.5, half=0.5),
                  feature("Flores", "Flores", min_label=6.7, lon=-31.2, lat=39.4, half=0.05)])
    batu = [r for r in rows if r[0] == "Batu Islands"]
    assert len(batu) == 1 and abs(batu[0][2] - 98.4) < 0.1, "the larger kept"
    assert len([r for r in rows if r[0] == "Flores"]) == 2, "Indonesia and the Azores"


def test_long_labels_wrap_as_the_country_names_do():
    rows = by_label(build([feature("Arctic Institute I.", "Arkticheskiy Institut Islands", "Island group", lon=81.7, lat=75.4)]))
    assert "Arctic Institute Islands" in rows, "the English name (G81 A-4)"
    assert rows["Arctic Institute Islands"][0] == "Arctic Institute\nIslands"
    assert B.wrap is C.wrap


def test_build_is_deterministic():
    src = json.dumps({"type": "FeatureCollection", "features": every_hand_name()}).encode()
    assert B.build(src, countries=set()) == B.build(src, countries=set())


def test_check_flags_bad_files():
    good = {"format": B.FORMAT, "count": 1, "islands": [["Bigland", 0.0, 0.0, 40, 70, 1]]}
    assert B.check(json.dumps(good).encode(), real=False) == []
    for bad, what in ((["Bigland", 0.0, 0.0, 5, 70, 1], "zooms"), (["Bigland", 0.0, 0.0, 40, 50, 1], "zooms"),
                      (["Bigland", 0.0, 0.0, 40, 125, 1], "zooms"), (["Bigland", "0", 0.0, 40, 70, 1], "position"),
                      (["Bigland", True, 0.0, 40, 70, 1], "position"), (["Bigland", 0.0, 0.0, 40, 70, 2], "kind"),
                      (["", 0.0, 0.0, 40, 70, 1], "label"), (["O‘ahu", 0.0, 0.0, 40, 70, 1], "okina"),
                      (["Savai'i", 0.0, 0.0, 40, 70, 1], "okina"),
                      (["Niuafoʼou", 0.0, 0.0, 40, 70, 1], "okina"), (["Niuafo’ou", 0.0, 0.0, 40, 70, 1], "okina"), (["Arkticheskiy Institut Islands", 0.0, 0.0, 40, 70, 0], "wrapped")):
        p = B.check(json.dumps(dict(good, islands=[bad])).encode(), real=False)
        assert any(what in x for x in p), (bad, p)
    assert B.check(json.dumps(dict(good, islands=[["D'Entrecasteaux\nIslands", 0.0, 0.0, 40, 70, 0]])).encode(), real=False) == []
    assert B.check(b"[]", real=False) == ["not an islands object"]
    assert B.check(b"{", real=False)[0].startswith("not JSON")


def test_ne_commit_is_the_places_pin():
    import build_places as P
    assert B.NE_COMMIT == P.NE_COMMIT == C.NE_COMMIT


def test_committed_file_passes_the_real_checks_and_the_pin():
    data = open(COMMITTED, "rb").read()
    assert hashlib.sha256(data).hexdigest() == PIN_SHA256
    assert B.check(data) == []
    d = json.loads(data)
    rows = by_label(d["islands"])
    assert set(n for n in ("Oʻahu", "Maui", "Kauaʻi", "Molokaʻi", "Lānaʻi", "Niʻihau", "Kahoʻolawe", "Hawaiʻi")) <= set(rows)
    assert rows["Oʻahu"][5] == 1 and rows["Hawaiian Islands"][5] == 0
    assert 20.9 < rows["Oʻahu"][1] < 21.8 and -158.4 < rows["Oʻahu"][2] < -157.6, "the label point on the island"
