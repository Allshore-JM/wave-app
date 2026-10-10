"""Wind stations (plan section 39), step 1: the committed snapshot wind_stations.json and the tool that builds it
(tools/wind/fetch_stations.py). The tool's network calls are not made here; its parsers, kinds, aliases and the coast
rule are tested on small inputs and the Hawaii crop of the coast data (tests/fixtures/coast/hawaii-t0.bin)."""
import gzip
import importlib.util
import json
import math
import os
import re
from zoneinfo import ZoneInfo

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SNAPSHOT = os.path.join(ROOT, "wind_stations.json")


def _tool():
    spec = importlib.util.spec_from_file_location("wind_fetch_stations",
                                                  os.path.join(ROOT, "tools", "wind", "fetch_stations.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


W = _tool()


@pytest.fixture(scope="module")
def snap():
    with open(SNAPSHOT, encoding="utf-8") as f:
        doc = json.load(f)
    return doc, [dict(zip(doc["fields"], r)) for r in doc["stations"]]


# ------------------------------------------------------------------ the snapshot

def test_snapshot_shape(snap):
    doc, rows = snap
    assert doc["fields"] == W.FIELDS == ["id", "name", "lat", "lon", "kind", "src", "tz", "alias"]
    assert all(len(r) == len(W.FIELDS) for r in doc["stations"])
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", doc["captured"])
    assert "public domain" in doc["source"]


def test_ids_are_namespaced_unique_and_match_their_source(snap):
    _, rows = snap
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids))
    kinds = {"coops": {"gauge"}, "ndbc": {"gauge", "buoy", "cman", "station"}, "metar": {"airport"}}
    for r in rows:
        src, _, native = r["id"].partition(":")
        assert src == r["src"] and src in kinds, r
        assert r["kind"] in kinds[src] and r["kind"] in W.KINDS, r
        assert re.fullmatch(r"\d{7}" if src == "coops" else r"[A-Z0-9]{3,8}", native), r


def test_names_positions_and_zones(snap):
    _, rows = snap
    zones = {}
    for r in rows:
        assert isinstance(r["name"], str) and r["name"].strip() == r["name"] and r["name"], r
        assert W.usable_position(r["lat"], r["lon"]), r
        zones.setdefault(r["tz"], ZoneInfo(r["tz"]))                   # every zone is a real zone


def test_aliases_name_ndbc_relays_never_drawn_twice(snap):
    _, rows = snap
    ndbc = {r["id"][5:] for r in rows if r["src"] == "ndbc"}
    aliases = [r["alias"] for r in rows if r["alias"]]
    assert len(aliases) == len(set(aliases))
    for r in rows:
        if r["alias"]:
            assert r["src"] == "coops" and re.fullmatch(r"[A-Z0-9]{5}", r["alias"]), r
            assert r["alias"] not in ndbc, r                         # the relay is the gauge, not a second flag


def test_hawaii_and_counts(snap):
    _, rows = snap
    by = {r["id"]: r for r in rows}
    assert by["coops:1612340"]["alias"] == "OOUH1" and by["coops:1612340"]["tz"] == "Pacific/Honolulu"
    for gauge in ("1611400", "1612401", "1612480", "1615680", "1617433", "1617760"):
        assert by["coops:" + gauge]["alias"], gauge                  # all seven Hawaii gauges relayed by NDBC
    assert {"ndbc:51003", "ndbc:51000", "metar:PHNL", "metar:PHOG", "metar:PHTO"} <= set(by)
    count = {}
    for r in rows:
        count[r["kind"]] = count.get(r["kind"], 0) + 1
    assert count["gauge"] >= 200 and count["buoy"] >= 150 and count["airport"] >= 1000, count


# ------------------------------------------------------------------ NDBC

LATEST = """#STN       LAT      LON  YYYY MM DD hh mm WDIR WSPD   GST WVHT
#text      deg      deg   yr mo day hr mn degT  m/s   m/s   m
51003    19.151 -160.617 2026 10 10 01 10 120   4.0   5.0   MM
51201    21.671 -158.118 2026 10 10 00 56  MM    MM    MM  1.8
OOUH1    21.303 -157.865 2026 10 10 00 54 190   3.6   6.2   MM
BAD01   -99.990  -99.990 2026 10 10 00 54 190   3.6   6.2   MM
short    1 2 3
"""


def test_parse_latest_obs_keeps_rows_with_a_wind_speed():
    got = W.parse_latest_obs(LATEST)
    assert got == {"51003": (19.151, -160.617), "OOUH1": (21.303, -157.865)}


def test_parse_station_table():
    text = ("# STATION_ID | OWNER | TTYPE | HULL | NAME | PAYLOAD\n#\n"
            "oouh1|O|Water Level Observation Network||1612340 - Honolulu, HI||21.303 N\n"
            "51003|N|3-meter discus buoy|3D37|WESTERN  HAWAII - 205 NM SW of Honolulu, HI|SCOOP\n"
            "broken|line\n")
    got = W.parse_station_table(text)
    assert got["OOUH1"] == {"owner": "O", "ttype": "Water Level Observation Network", "name": "1612340 - Honolulu, HI"}
    assert got["51003"]["ttype"] == "3-meter discus buoy" and "BROKEN" not in got


@pytest.mark.parametrize("ttype,sid,kind", [
    ("Water Level Observation Network", "OOUH1", "gauge"),
    ("C-MAN Station", "PLSF1", "cman"),
    ("3-meter discus buoy", "51003", "buoy"),
    ("Waverider Buoy", "46239", "buoy"),
    ("Lightship", "62103", "buoy"),                          # anchored
    ("Uncrewed Surface Vehicle", "46012", "buoy"),           # holds the buoy's station
    ("Oil Platform", "KGRY", "station"),
    ("NERRS Weather Station", "TKPN6", "station"),
    ("", "22101", "buoy"),
    ("", "STRAT", "station"),
    ("Drifting Buoy", "41X01", None),
    ("Long Island Ferry", "44Y01", None),
    ("Ship", "SHIP", None),
    ("Wave Glider", "G1", None),
    ("Saildrone", "SD1", None),
])
def test_ndbc_kind(ttype, sid, kind):
    assert W.ndbc_kind(sid, ttype) == kind


@pytest.mark.parametrize("name,want", [
    ("1612340 - Honolulu, HI", ("1612340", "Honolulu, HI")),
    ("Castle Island (NOS) 8444069", ("8444069", "Castle Island (NOS) 8444069")),
    ("Turkey Point Hudson River NERRS, NY (NOS 8518962)", ("8518962", "Turkey Point Hudson River NERRS, NY (NOS 8518962)")),
    ("WESTERN HAWAII - 205 NM SW of Honolulu, HI", (None, "WESTERN HAWAII - 205 NM SW of Honolulu, HI")),
    ("Station 12345678 long number", (None, "Station 12345678 long number")),
    ("Depth 1234567.5", (None, "Depth 1234567.5")),
    ("", (None, "")),
])
def test_coops_id_in_name(name, want):
    assert W.coops_id_in_name(name) == want


def test_assign_aliases_by_name_then_distance_one_per_gauge():
    coops = {"1612340": (21.3033, -157.8645), "8518962": (42.0142, -73.9392), "9999999": (10.0, 10.0)}
    ndbc = {
        "OOUH1": {"lat": 21.303, "lon": -157.865, "kind": "gauge", "coops_ref": "1612340"},     # by name
        "TWIN1": {"lat": 21.3033, "lon": -157.8645, "kind": "cman", "coops_ref": None},         # same gauge, 0 m: taken
        "TKPN6": {"lat": 42.0143, "lon": -73.9393, "kind": "station", "coops_ref": None},       # ~15 m: by distance
        "FAR01": {"lat": 10.004, "lon": 10.0, "kind": "buoy", "coops_ref": None},               # 445 m: its own station
        "GONE1": {"lat": 0.0, "lon": 1.0, "kind": "gauge", "coops_ref": "7777777"},             # names a gauge not kept
    }
    alias, log = W.assign_aliases(ndbc, coops)
    assert alias == {"1612340": "OOUH1", "8518962": "TKPN6"}
    assert [(x[0], x[1], x[2]) for x in log] == [("OOUH1", "1612340", "name"), ("TKPN6", "8518962", "distance")]


def test_usable_position_and_site_name():
    assert W.usable_position(21.3, -157.9) and W.usable_position("21.3", "-157.9")
    for lat, lon in ((-99.99, -99.99), (0, 0), (84.5, 0), (-79.5, 0), (0, 181), (None, 1), ("x", 1), (math.nan, 1)):
        assert not W.usable_position(lat, lon), (lat, lon)
    assert W.site_name("Honolulu  Intl", "PHNL") == "Honolulu Intl"
    for blank in ("", "Unk", "unknown", "kxer", None):
        assert W.site_name(blank, "KXER") == "KXER"
    assert W.site_name("HONOLULU", "PHNL", lambda n: n.title()) == "Honolulu"


# ------------------------------------------------------------------ METAR

CACHE_CSV = (
    "raw_text,station_id,observation_time,latitude,longitude,temp_c,dewpoint_c,wind_dir_degrees,wind_speed_kt,wind_gust_kt\n"
    '"METAR PHNL 100053Z 15011KT",PHNL,2026-10-10T00:53:00.000Z,21.3150,-157.9240,29.4,24.4,150,11,\n'
    '"METAR K3L4 100135Z AUTO VRB05G11KT",K3L4,2026-10-10T01:35:00.000Z,40.1,-75.2,,,0,5,11\n'
    '"METAR XXXX 100135Z AUTO /////KT",XXXX,2026-10-10T01:35:00.000Z,40.1,-75.2,,,,,\n'
    '"METAR BADP 100135Z 10005KT",BADP,2026-10-10T01:35:00.000Z,-99.99,-99.99,,,100,5,\n')


def test_parse_metar_cache_plain_and_gzip():
    want = {"PHNL": (21.315, -157.924), "K3L4": (40.1, -75.2)}      # VRB (dir 0, speed 5) still reports wind
    assert W.parse_metar_cache(CACHE_CSV.encode()) == want
    assert W.parse_metar_cache(gzip.compress(CACHE_CSV.encode())) == want
    with pytest.raises(ValueError):
        W.parse_metar_cache(b"a,b,c\n1,2,3\n")


def test_parse_metar_sites_keeps_metar_sites_with_positions():
    sites = [{"id": "PHNL", "site": "Honolulu  Intl", "lat": 21.31505, "lon": -157.924, "siteType": ["METAR", "TAF"]},
             {"id": "32012", "site": "Woods Hole Stratus", "lat": 19.7, "lon": -85.6, "siteType": []},
             {"id": "KTAF", "site": "Taf only", "lat": 30.0, "lon": -90.0, "siteType": ["TAF"]},
             {"id": "ENUN", "site": "", "lat": -99.99, "lon": -99.99, "siteType": ["METAR"]}]
    raw = gzip.compress(json.dumps(sites).encode())
    assert W.parse_metar_sites(raw) == {"PHNL": ("Honolulu Intl", 21.31505, -157.924)}


def test_metar_reporting_batches_and_survives_a_lost_batch():
    asked = []

    def fetch(url):
        ids = re.search(r"ids=([^&]+)", url).group(1).split(",")
        asked.append(ids)
        assert "hours=24" in url and "format=json" in url
        if "0LOST" in ids:
            raise OSError("timeout")
        return [{"icaoId": i, "wspd": None if i.startswith("Q") else 5} for i in ids] + [{"icaoId": "OTHER", "wspd": 3}]

    ids = ["A%02d" % k for k in range(20)] + ["Q1", "0LOST"]          # sorted: the lost batch holds 0LOST, not Q1
    got = W.metar_reporting(ids, fetch=fetch, pause=0)
    assert all(len(b) <= W.METAR_API_IDS for b in asked) and sum(len(b) for b in asked) == len(ids)
    lost = next(b for b in asked if "0LOST" in b)
    assert "Q1" not in lost
    assert got == {i for i in ids if i.startswith("A") and i not in lost}   # no Q (no speed), no OTHER (not asked)


# ------------------------------------------------------------------ the coast

def _hawaii_edges():
    import point_forecast as P
    with open(os.path.join(HERE, "fixtures", "coast", "hawaii-t0.bin"), "rb") as f:
        return P.decode_coast(f.read())


def test_coast_distance_on_the_hawaii_crop():
    idx = W.CoastIndex(_hawaii_edges())
    assert idx.distance_km(21.31505, -157.924, 30) < 2                # Honolulu airport, on the shore
    assert idx.distance_km(21.478, -158.044, 30) < 15                 # Wheeler, inland Oahu: still within 30 km
    assert math.isinf(idx.distance_km(19.151, -160.617, 30))         # buoy 51003, far at sea
    d = idx.distance_km(21.0, -158.5, 100)
    assert 20 < d < 100 and math.isinf(idx.distance_km(21.0, -158.5, d - 1))


def test_coast_index_ignores_cell_lines_and_wraps_the_date_line():
    import numpy as np
    # a square island 179.9 E .. 179.9 W across the date line is two pieces; the cut at 180 is a cell line
    xi = np.array([179.9, 180.0, 179.9, -180.0, -179.9, -179.9])
    yi = np.array([0.0, 0.0, 0.1, 0.1, 0.1, 0.0])
    xj = np.array([179.9, 180.0, 179.9, -180.0, -179.9, -179.9])
    yj = np.array([0.1, 0.1, 0.0, 0.0, 0.0, 0.1])
    idx = W.CoastIndex((xi, yi, xj, yj))
    assert idx.dropped == 2                                          # the two edges along 180
    assert abs(idx.distance_km(0.05, -179.8, 50) - 0.1 * 111.32) < 0.2   # east of the island: its east coast
    assert abs(idx.distance_km(0.05, 179.8, 50) - 0.1 * 111.32) < 0.2    # west of it, across the line
    assert abs(idx.distance_km(0.05, 180.0, 50) - 0.1 * 111.32) < 0.2    # on the cut: 11 km from either coast


def test_coast_index_finds_coast_only_across_the_date_line():
    import numpy as np
    # an island's east coast runs from 179.95 E to exactly 180 (not a cell line: a diagonal); the point lies east of
    # the line, so the coast is only found through the buckets' wrap (stored at column 720 = 0, looked up from -1)
    idx = W.CoastIndex((np.array([179.95]), np.array([0.0]), np.array([180.0]), np.array([0.05])))
    assert idx.dropped == 0
    d = idx.distance_km(0.05, -179.99, 30)
    assert abs(d - 0.01 * 111.32) < 0.05, d
    idx2 = W.CoastIndex((np.array([179.8]), np.array([0.0]), np.array([179.8]), np.array([0.1])))
    assert abs(idx2.distance_km(0.05, -179.95, 30) - 0.25 * 111.32) < 0.1


def test_in_land_on_the_hawaii_crop():
    edges = _hawaii_edges()
    got = W.in_land(edges, [21.48, 21.0, 19.6, 21.315], [-158.0, -158.5, -155.5, -157.6])
    assert got == [True, False, True, False]                         # central Oahu, sea, Big Island, east of Oahu
