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

import wind_sources as WS

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
MORE_MET = ("eccc", "jma", "smhi", "fmi", "dmi", "dwd", "ipma", "mei", "cwa")   # cwa: plan section 69
MORE = MORE_MET + ("iem",)


@pytest.fixture(scope="module")
def snap():
    with open(SNAPSHOT, encoding="utf-8") as f:
        doc = json.load(f)
    return doc, [dict(zip(doc["fields"], r)) for r in doc["stations"]]


# ------------------------------------------------------------------ the snapshot

def test_snapshot_shape(snap):
    doc, rows = snap
    assert doc["fields"] == W.FIELDS == ["id", "name", "lat", "lon", "kind", "src", "tz", "alias", "wx", "net"]
    assert all(len(r) == len(W.FIELDS) for r in doc["stations"])
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", doc["captured"])
    assert "public domain" in doc["source"]


def test_ids_are_namespaced_unique_and_match_their_source(snap):
    _, rows = snap
    ids = [r["id"] for r in rows]
    assert len(ids) == len(set(ids))
    kinds = {"coops": {"gauge"}, "ndbc": {"gauge", "buoy", "cman", "station"}, "metar": {"airport"}, "nws": set(W.NWS_KINDS),
             "iem": {"road", "airport"}}
    kinds.update({src: {"met"} for src in MORE_MET})                  # plan section 67: national weather services
    native_re = {"coops": r"\d{7}", "jma": r"\d{5}", "dwd": r"10\d{3}", "dmi": r"\d{5}", "fmi": r"\d{6}",
                 "smhi": r"\d{4,6}", "ipma": r"\d{7,8}", "eccc": r"[A-Z][A-Z0-9]{3}", "mei": r"[A-Z][A-Z-]{2,23}",
                 "iem": r"[A-Z0-9]{3,6}", "cwa": r"[A-Z0-9]{6}"}
    for r in rows:
        src, _, native = r["id"].partition(":")
        assert src == r["src"] and src in kinds, r
        assert r["kind"] in kinds[src] and r["kind"] in W.KINDS, r
        assert re.fullmatch(native_re.get(src, r"[A-Z0-9]{3,8}"), native), r
        assert WS.ID_RE.fullmatch(r["id"]), r                        # the server's own id rule takes every row


def test_names_positions_and_zones(snap):
    _, rows = snap
    zones = {}
    for r in rows:
        assert isinstance(r["name"], str) and r["name"].strip() == r["name"] and r["name"], r
        assert W.usable_position(r["lat"], r["lon"]), r
        zones.setdefault(r["tz"], ZoneInfo(r["tz"]))                   # every zone is a real zone


def test_wx_letters_only_for_gauges(snap):
    _, rows = snap
    for r in rows:
        if r["src"] == "coops":
            assert r["wx"] is None or (re.fullmatch(r"a?w?p?h?", r["wx"]) and r["wx"]), r
        else:
            assert r["wx"] is None, r
    by = {r["id"]: r for r in rows}
    assert by["coops:1612340"]["wx"] == "awp"
    assert sum(1 for r in rows if r["src"] == "coops" and r["wx"] and "a" in r["wx"]) >= 150
    assert W.wx_letters([["Wind", 1, ""], ["Air Temperature", 1, ""], ["Relative Humidity", 1, ""], ["Water Temperature", 0, ""]]) == "ah"
    assert W.wx_letters([["Wind", 1, ""]]) is None and W.wx_letters(None) is None and W.wx_letters([["x"]]) is None


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
    nws = [r for r in rows if r["src"] == "nws"]
    assert 200 <= len(nws) <= 600 and count["utility"] >= 50 and count["mesonet"] >= 20, count    # step 5c: Hawaii
    assert all(18.8 < r["lat"] < 22.3 and -160.3 < r["lon"] < -154.7 and r["tz"] == "Pacific/Honolulu" for r in nws)
    others = [r for r in rows if r["src"] != "nws" and 18 < r["lat"] < 23 and -161 < r["lon"] < -154]
    for r in nws:                                                        # never within 300 m of another kept station
        assert W.too_close(others_rows(others), r["lat"], r["lon"]) is None, r
    assert not any(re.fullmatch(r"\d{5}", r["id"][4:]) for r in nws), "NDBC's buoys are NDBC's"


# ------------------------------------------------------------------ plan section 67: the added sources

def test_net_only_for_iem_rows_and_their_kind(snap):
    _, rows = snap
    for r in rows:
        if r["src"] == "iem":
            m = re.fullmatch(r"([A-Z]{2})_(RWIS|ASOS)", r["net"] or "")
            assert m, r
            assert r["kind"] == {"RWIS": "road", "ASOS": "airport"}[m.group(2)], r
        else:
            assert r["net"] is None, r


def test_added_sources_counts_zones_and_landmarks(snap):
    _, rows = snap
    by = {r["id"]: r for r in rows}
    count = {src: sum(1 for r in rows if r["src"] == src) for src in MORE}
    bands = {"eccc": (250, 400), "jma": (480, 680), "iem": (420, 700), "smhi": (85, 140), "fmi": (80, 130),
             "dmi": (40, 75), "dwd": (20, 45), "ipma": (65, 110), "mei": (10, 18)}
    for src, (lo, hi) in bands.items():
        assert lo <= count[src] <= hi, (src, count[src])          # measured 2026-10-30: 300 575 534 103 101 51 28 84 10
    assert all(r["tz"] == "Asia/Tokyo" for r in rows if r["src"] == "jma")
    assert all(r["tz"] in ("Europe/Lisbon", "Atlantic/Azores", "Atlantic/Madeira") for r in rows if r["src"] == "ipma")
    assert all(r["tz"] == "Europe/Dublin" for r in rows if r["src"] == "mei")
    assert all(r["tz"].startswith("America/") for r in rows if r["src"] in ("eccc", "iem") and r["lon"] > -170),         "North American feeds in North American zones"
    for sid in ("jma:44132", "eccc:CAHD", "mei:MALIN-HEAD", "mei:ROCHES-POINT", "dwd:10015", "dwd:10020", "dwd:10091"):
        assert sid in by, sid                                          # Tokyo, Halifax Dockyard, Malin Head, Helgoland ...
    assert by["mei:ROCHES-POINT"]["name"] == "Roche's Point"
    assert not [r["id"] for r in rows if r["src"] in MORE and r["name"] == r["name"].upper()
                and re.search(r"[A-Z]{4,}", r["name"])],         "agency names in capitals are case-fixed (road names like 'I-94 @ US-421' have no word to fix)"


def test_added_sources_never_duplicate_a_kept_station(snap):
    _, rows = snap
    metar = [r for r in rows if r["src"] == "metar"]
    metar_ids = {r["id"].split(":")[1] for r in metar}
    added = [r for r in rows if r["src"] in MORE]
    for r in added:
        native = r["id"].split(":")[1]
        assert native not in metar_ids, r                              # ECCC's NAV CANADA files, IEM's ASOS sites
        if r["src"] == "iem" and len(native) == 3:
            assert not ({"K" + native, "P" + native, "T" + native} & metar_ids), r
    grid = {}
    for m in metar:
        grid.setdefault((math.floor(m["lat"] * 10), math.floor(m["lon"] * 10)), []).append(m)
    for r in added:                                                    # no added station on an airfield (500 m)
        cy, cx = math.floor(r["lat"] * 10), math.floor(r["lon"] * 10)
        near = [m["id"] for dy in (-1, 0, 1) for dx in (-1, 0, 1) for m in grid.get((cy + dy, cx + dx), ())
                if W.km_between(r["lat"], r["lon"], m["lat"], m["lon"]) * 1000 <= W.AIRPORT_SPOT_M]
        assert not near, (r["id"], near)
    pts = sorted((r["lat"], r["lon"], r["id"]) for r in rows)
    for r in rows:                                                     # DWD's coarse positions: 2 km from anything kept
        if r["src"] != "dwd":
            continue
        close = [sid for la, lo, sid in pts if sid != r["id"] and abs(la - r["lat"]) < 0.03
                 and W.km_between(r["lat"], r["lon"], la, lo) * 1000 <= 2000]
        assert not close, (r["id"], close)


def test_no_weather_service_station_on_an_airfield(snap):
    _, rows = snap
    metar = [r for r in rows if r["src"] == "metar"]
    for r in rows:
        if r["kind"] != "met":
            continue
        near = [m["id"] for m in metar if abs(m["lat"] - r["lat"]) < 0.03
                and W.km_between(r["lat"], r["lon"], m["lat"], m["lon"]) * 1000 <= 1500]
        assert not near, (r["id"], r["name"], near)                  # the airport keeps one flag: its METAR


def test_dwd_rows_are_german_stations(snap):
    _, rows = snap
    dwd = [r for r in rows if r["src"] == "dwd"]
    assert dwd and all(r["id"].startswith("dwd:10") for r in dwd), "WMO block 10: DWD's own stations, not its SYNOP relays"
    assert all(47.2 < r["lat"] < 55.2 and 5.8 < r["lon"] < 15.1 for r in dwd)


def others_rows(rows):
    return [[r["id"], r["name"], r["lat"], r["lon"]] for r in rows]


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


@pytest.mark.parametrize("sid,provider,sub,kind", [
    ("001HE", "MesoWest", "HECO", "utility"),
    ("010HE", "", "", "utility"),                            # a utility station the API lists without a provider
    ("001HI", "MesoWest", "U-HAWAII", "mesonet"),
    ("KXAH1", "MesoWest", "SCAN", "mesonet"),
    ("AHMH1", "RAWS", "", "raws"),
    ("AP834", "APRSWXNET", "", "cwop"),
    ("AHUH1", "HADS", "", "hads"),
    ("XYZ", "", "", "weather"),
    ("ILOH1", "NOS-NWLON", "NWLON", None),                   # a tide gauge: the CO-OPS feed's
    ("PHBK", "ASOS", "", None),                              # airports: the METAR feed's
    ("PHHI", "OTHER-MTR", "", None),
    ("PHMU", "NonFedAWOS", "SAI", None),
    ("51202", "", "", None),                                 # an NDBC buoy
])
def test_nws_kind(sid, provider, sub, kind):
    assert W.nws_kind(sid, provider, sub) == kind


def test_nws_stations_pages_and_keeps_usable_positions():
    page = json.loads(open(os.path.join(HERE, "fixtures", "wind", "nws_stations.json"), encoding="utf-8").read())
    page1 = dict(page, pagination={"next": "NEXT"})
    page2 = {"@graph": [{"stationIdentifier": "ZZZ", "name": "Nowhere", "geometry": "POINT(0 0)", "provider": ""},
                        {"stationIdentifier": "far", "name": "Far", "geometry": "POINT(-156.1 20.9)", "provider": "RAWS", "timeZone": "Pacific/Honolulu"}],
             "pagination": {"next": "LAST"}}
    answers = {"NEXT": page2, "LAST": {"@graph": []}}
    calls = []

    def fetch(url):
        calls.append(url)
        return answers.get(url, page1)
    W.NWS_PAUSE_S, pause = 0, W.NWS_PAUSE_S
    try:
        got = W.nws_stations("HI", fetch)
    finally:
        W.NWS_PAUSE_S = pause
    assert calls[0] == W.NWS_API + "/stations?state=HI&limit=500" and calls[1:] == ["NEXT", "LAST"]
    assert got["001HE"] == {"name": "MECO Kealaloloa Ridge", "lat": 20.80393, "lon": -156.54018, "provider": "MesoWest",
                            "sub": "HECO", "tz": "Pacific/Honolulu"}
    assert "ZZZ" not in got and got["FAR"]["lat"] == 20.9 and got["FAR"]["provider"] == "RAWS"
    assert len(got) == len(page["@graph"]) + 1


def test_nws_usable_probes_the_newest_rows_and_keeps_a_cache(capsys):
    obs = json.loads(open(os.path.join(HERE, "fixtures", "wind", "nws_obs.json"), encoding="utf-8").read())
    calls = []

    def fetch(url):
        calls.append(url)
        if "001HE" in url:
            return obs
        if "BAD" in url:
            raise IOError("down")
        return {"@graph": []}
    cache = {"OLD": [1000.0, 5], "FRESH": [1_000_000.0, None]}
    got = W.nws_usable(["001HE", "NONE", "BAD", "OLD", "FRESH"], fetch, pause=0, cache=cache, now=1_000_100.0)
    assert got == {"001HE": max(r["t"] for r in __import__("wind_sources").parse_nws_observations(obs))}
    assert len(calls) == 4 and all("limit=%d" % W.NWS_PROBE_LIMIT in u and "start=" in u for u in calls)
    assert "FRESH" not in "".join(calls) and cache["FRESH"] == [1_000_000.0, None], "a fresh cache answer is reused"
    assert cache["001HE"] == [1_000_100.0, got["001HE"]] and cache["NONE"] == [1_000_100.0, None]
    assert cache["OLD"][0] == 1_000_100.0 and "BAD" not in cache, "a lost probe is not remembered"
    assert "nws probe BAD" in capsys.readouterr().err


def test_nws_name():
    clean = _tool().tide_tool().clean_name
    assert W.nws_name("WH6BXK KAHULUI", "AW209", "cwop", clean) == "WH6BXK Kahului"
    assert W.nws_name("KH6HHG Makawao", "AP834", "cwop", clean) == "KH6HHG Makawao"
    assert W.nws_name("AHUMOA", "AHMH1", "raws", clean) == "Ahumoa"
    assert W.nws_name("MECO Kealaloloa Ridge", "001HE", "utility", clean) == "MECO Kealaloloa Ridge"
    assert W.nws_name("  ", "XYZ", "weather", clean) == "XYZ" and W.nws_name("KH6ABC", "AQ1", "cwop", clean) == "KH6ABC"
    assert W.nws_name("BELLOWS AFS AT WAIMANALO", "BELH1", "hads", clean) == "Bellows AFS at Waimanalo"
    assert W.nws_name("HONOULIULI PHB", "HOFH1", "raws", clean) == "Honouliuli PHB"
    assert W.nws_name("PORT ALLEN NEAR HANAPEPE 2SW", "PAKH1", "hads", clean) == "Port Allen near Hanapepe 2SW"
    assert W.nws_name("AT THE TOP", "X", "raws", clean) == "At the Top" and W.nws_name("KANEOHE MCBH", "X", "raws", clean) == "Kaneohe MCBH"


def test_too_close():
    rows = [["coops:1612340", "Honolulu", 21.30333, -157.86453], ["metar:PHNL", "Honolulu Intl", 21.315, -157.924]]
    assert W.too_close(rows, 21.3034, -157.8645) == "coops:1612340"
    assert W.too_close(rows, 21.31, -157.86) is None and W.too_close(rows, 21.3034, -157.8645, metres=5) is None


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


# ------------------------------------------------------------------ G28 step 7a: one per spot, place names

@pytest.mark.parametrize("name,want", [
    ("Southwest Bering Sea - 142Nm Nne Of Attu Is, Ak", "Southwest Bering Sea - 142NM NNE of Attu Is, AK"),
    ("Tillamook, Or - 85 NM WNW of Tillamook, OR", "Tillamook, OR - 85 NM WNW of Tillamook, OR"),
    ("Isle Of Man Arpt", "Isle of Man Arpt"), ("East Of Sw Pass, La", "East of SW Pass, LA"),
    ("Honolulu Intl, HI, US", "Honolulu Intl, HI, US"), ("W Hawaii", "W Hawaii"), ("St Paul Mxak, Ak", "St Paul MXAK, AK"),
])
def test_place_names_through_the_case_rules(name, want):
    assert W._fix_case(name, places=True) == want
    assert W._fix_case("East Of Sw Pass, La") == "East of Sw Pass, La", "places=False: the NWS rule only"
    assert W._fix_case("Ak Cape, Ak", places=True) == "Ak Cape, AK", "a state code only after a comma (re-check R39)"


def test_one_per_spot_keeps_a_gauge_then_metar_then_ndbc():
    rows = [["coops:1", "g", 21.0, -157.0, "gauge", "coops"], ["ndbc:A", "n", 21.0003, -157.0, "buoy", "ndbc"],
            ["metar:KX", "m", 25.0, -90.0, "airport", "metar"], ["ndbc:KX", "n", 25.0004, -90.0, "station", "ndbc"],
            ["metar:CWGR", "a", 47.4, -61.8, "airport", "metar"], ["metar:CYGR", "b", 47.4, -61.8, "airport", "metar"],
            ["ndbc:FAR", "f", 21.002, -157.0, "buoy", "ndbc"]]                  # 222 m away: kept
    kept, dropped = W.one_per_spot(rows, fresh={"metar:CYGR"})
    assert [r[0] for r in kept] == ["coops:1", "metar:KX", "metar:CYGR", "ndbc:FAR"], "input order kept"
    assert sorted(dropped) == [("metar:CWGR", "metar:CYGR"), ("ndbc:A", "coops:1"), ("ndbc:KX", "metar:KX")]
    kept2, _ = W.one_per_spot(rows)
    assert "metar:CWGR" in [r[0] for r in kept2], "neither reporting: the id order decides"


def test_one_per_spot_drops_ndbc_copies_of_a_kept_metar_id_and_two_metars_on_one_airfield():
    """G28 re-check R1-N6: NDBC's copy of a platform whose METAR is kept goes at any distance (KGVW: 332 m), and two METAR
    sites within AIRPORT_SPOT_M are one airfield (CXAT / CYAB 358 m); other NDBC stations and airports keep 100 m."""
    rows = [["metar:KGVW", "a", 29.13, -94.5469, "airport", "metar"], ["ndbc:KGVW", "b", 29.13, -94.55, "station", "ndbc"],
            ["metar:CXAT", "c", 73.0, -85.02, "airport", "metar"], ["metar:CYAB", "d", 73.003, -85.029, "airport", "metar"],
            ["metar:KFAR", "e", 30.0, -90.0, "airport", "metar"], ["metar:KFAZ", "f", 30.006, -90.0, "airport", "metar"],
            ["ndbc:B1", "g", 40.0, -70.0, "buoy", "ndbc"], ["ndbc:B2", "h", 40.003, -70.0, "buoy", "ndbc"]]
    kept, dropped = W.one_per_spot(rows)
    assert W.AIRPORT_SPOT_M == 500.0
    assert sorted(dropped) == [("metar:CYAB", "metar:CXAT"), ("ndbc:KGVW", "metar:KGVW")]
    assert [r[0] for r in kept] == ["metar:KGVW", "metar:CXAT", "metar:KFAR", "metar:KFAZ", "ndbc:B1", "ndbc:B2"],         "667 m apart: two airfields; 333 m apart: two buoys"


def test_respot_rewrites_only_the_spots_and_names_and_keeps_the_capture_time(tmp_path):
    src = {"source": "S", "captured": "2026-10-11T21:44:38Z", "fields": W.FIELDS, "stations": [
        ["metar:KGVW", "Galveston", 29.13, -94.5469, "airport", "metar", "America/Chicago", None, None],
        ["ndbc:KGVW", "Galveston 209A", 29.13, -94.55, "station", "ndbc", "America/Chicago", None, None],
        ["ndbc:KOZA2", "Kotzebue Mxak, AK", 66.9, -162.6, "station", "ndbc", "America/Nome", None, None],
        ["nws:X1", "Somewhere", 29.13, -94.5469, "utility", "nws", "America/Chicago", None, None]]}
    f = tmp_path / "w.json"
    f.write_text(json.dumps(src), encoding="utf-8")
    assert W.main(["--respot", "--out", str(f)]) == 0
    doc = json.loads(f.read_text(encoding="utf-8"))
    assert doc["captured"] == "2026-10-11T21:44:38Z" and doc["source"] == "S"
    assert [r[0] for r in doc["stations"]] == ["metar:KGVW", "ndbc:KOZA2", "nws:X1"], "NWS rows were placed at build time"
    assert doc["stations"][1][1] == "Kotzebue MXAK, AK"


def test_the_snapshot_has_no_ndbc_copy_of_a_kept_metar_and_one_metar_per_airfield(snap):
    _, rows = snap
    metar = {r["id"].split(":")[1] for r in rows if r["src"] == "metar"}
    assert [r["id"] for r in rows if r["src"] == "ndbc" and r["id"].split(":")[1] in metar] == []
    air = sorted((r["lat"], r["lon"], r["id"]) for r in rows if r["src"] == "metar")
    close = [(a[2], b[2]) for i, a in enumerate(air) for b in air[i + 1:i + 40]
             if abs(b[0] - a[0]) < 0.01 and W.km_between(a[0], a[1], b[0], b[1]) * 1000 <= W.AIRPORT_SPOT_M]
    assert close == [], close
    assert not [r["name"] for r in rows if "Mxak" in r["name"]]


def test_the_snapshot_has_one_station_per_spot(snap):
    _, rows = snap
    pts = sorted((r["lat"], r["lon"], r["id"]) for r in rows)
    close = []
    for i, (la, lo, sid) in enumerate(pts):
        for la2, lo2, sid2 in pts[i + 1:]:
            if la2 - la > 0.001:
                break
            if W.km_between(la, lo, la2, lo2) * 1000 <= W.SPOT_M:
                close.append((sid, sid2))
    assert close == [], close
