"""Plan section 73: the WMO WIS2 wind stations (the national weather services' wis2box nodes), the Hong Kong Observatory
and SMN Argentina (wind_more.py) on real answers captured 2026-11-01 (tests/fixtures/wind/more, by
tests/capture_wis2_fixtures.py), their providers with a fake fetch, and the WIS2 history branch."""
import io
import json
import os
import sys
import zipfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import buoy_sources  # noqa: E402
import wind_more as M  # noqa: E402
import wind_sources as W  # noqa: E402

FX = os.path.join(ROOT, "tests", "fixtures", "wind", "more")
CAPE_POINT, AGULHAS, MARKEN = "0-20000-0-68916", "0-20000-0-68920", "0-20000-0-68163"
MINICOY, COLABA, UNA = "0-20000-0-43369", "0-20000-0-43057", "0-20000-0-42077"
T02Z, T03Z = 1793498400, 1793502000                                # 2026-11-01 02:00Z and 03:00Z


def fx(name):
    with open(os.path.join(FX, name), "rb") as f:
        return f.read()


def feature(wid, name, value, units, when="2026-11-01T03:00:00Z", lon=18.5, lat=-34.4):
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {"wigos_station_identifier": wid, "name": name, "value": value, "units": units,
                           "reportTime": when}}


def doc(*features):
    return json.dumps({"type": "FeatureCollection", "features": list(features)})


# ---------------------------------------------------------------------------------------------- WIS2 parsers

def saws_docs():
    return [fx("wis2_saws_wind_speed.json"), fx("wis2_saws_wind_direction.json"),
            fx("wis2_saws_maximum_wind_gust_speed.json")]


def test_wis2_features_join_speed_direction_and_gust():
    by, pos = M.parse_wis2_features(saws_docs())
    assert sorted(by) == [MARKEN, CAPE_POINT, AGULHAS]
    assert by[CAPE_POINT][T02Z] == {"s": 1.5, "d": 70.0, "g": 3.6}           # m/s, degrees FROM
    assert pos[CAPE_POINT] == (-34.35, 18.48333)                             # (lat, lon) from GeoJSON (lon, lat)
    rows = M.wis2_readings(by)
    assert rows[CAPE_POINT][-1] == {"t": T02Z, "s": 1.5, "g": 3.6, "d": 70}
    assert rows[MARKEN][-1] == {"t": T03Z, "s": 0.0, "g": 1.0, "d": None}   # BUFR direction 0: calm, no direction
    assert all(a["t"] < b["t"] for rs in rows.values() for a, b in zip(rs, rs[1:]))


def test_wis2_calm_reports_and_the_one_filter_node():
    by, _ = M.parse_wis2_features([fx("wis2_imd_wind_speed.json"), fx("wis2_imd_wind_direction.json")])
    rows = M.wis2_readings(by)
    assert sorted(rows) == [UNA, COLABA, MINICOY]
    assert all(r == [{"t": T03Z, "s": 0.0, "g": None, "d": None}] for r in rows.values())


def test_wis2_units_ids_times_and_values():
    good = feature(CAPE_POINT, "wind_speed", 10, "knots")
    north = feature(CAPE_POINT, "wind_direction", 360, "deg")
    kelvin = feature(CAPE_POINT, "air_temperature", 293.15, "K")
    pa = feature(CAPE_POINT, "pressure_reduced_to_mean_sea_level", 101320, "Pa")
    bad = [feature(CAPE_POINT, "wind_speed", 5, "km/h", when="2026-11-01T02:00:00Z"),    # units not expected
           feature("68916", "wind_speed", 5, "m/s"), feature("0-20000-0-6891X!", "wind_speed", 5, "m/s"),
           feature(CAPE_POINT, "wind_speed", "n/a", "m/s", when="2026-11-01T01:00:00Z"),
           feature(CAPE_POINT, "wind_speed", 5, "m/s", when="yesterday"),
           feature(CAPE_POINT, "visibility", 5, "m"),                                   # a variable not used
           {"type": "Feature", "geometry": None, "properties": None}]
    by, pos = M.parse_wis2_features([doc(good, north, kelvin, pa, *bad)])
    assert list(by) == [CAPE_POINT] and list(by[CAPE_POINT]) == [T03Z]
    v = by[CAPE_POINT][T03Z]
    assert v["s"] == pytest.approx(10 * M.KT) and v["d"] == 360 and v["at"] == pytest.approx(20.0)
    assert v["p"] == pytest.approx(1013.2)
    r = M.wis2_readings(by)[CAPE_POINT][0]
    assert r["d"] == 0 and r["s"] == pytest.approx(5.1, abs=0.05)              # 360 is north, kept as 0
    assert M.wis2_wid(" 0-20000-0-68916 ") == CAPE_POINT and M.wis2_wid("0-454-2-ab12") == "0-454-2-AB12"
    assert M.wis2_wid("68916") is None and M.wis2_wid(None) is None and M.wis2_wid("0-20000-0-") is None
    assert M.wis2_wmo(CAPE_POINT) == "68916" and M.wis2_wmo("0-454-2-AB12") is None


def test_wis2_direction_zero_with_wind_is_no_direction():
    by, _ = M.parse_wis2_features([doc(feature(CAPE_POINT, "wind_speed", 5, "m/s"),
                                       feature(CAPE_POINT, "wind_direction", 0, "deg"))])
    assert M.wis2_readings(by)[CAPE_POINT] == [{"t": T03Z, "s": 5.0, "g": None, "d": None}]   # 0 is not north (360 is)


def test_wis2_a_report_without_speed_is_no_reading():
    by, _ = M.parse_wis2_features([doc(feature(CAPE_POINT, "wind_direction", 90, "deg"),
                                       feature(CAPE_POINT, "maximum_wind_gust_speed", 9, "m/s"))])
    assert M.wis2_readings(by) == {}


def test_wis2_stations_collection():
    st = M.parse_wis2_stations(fx("wis2_saws_stations.json"))
    assert st[CAPE_POINT] == {"name": "CAPE POINT (68916-0)", "lat": -34.3532, "lon": 18.4894}
    assert M.parse_wis2_stations(fx("wis2_imd_stations.json"))[MINICOY]["name"] == "MINICOY"
    odd = {"features": [{"id": "0-454-2-AB1", "geometry": None, "properties": {"name": "  Odd   place "}},
                        {"id": "no", "properties": {}}]}
    assert M.parse_wis2_stations(odd) == {"0-454-2-AB1": {"name": "Odd place", "lat": None, "lon": None}}


def test_wis2_history_fixture_carries_the_weather():
    by, _ = M.parse_wis2_features([fx("wis2_saws_history_68916.json")])
    rows = M.wis2_readings(by)[CAPE_POINT]
    assert len(rows) == 23 and rows[0]["t"] == 1793419200 and rows[-1]["t"] == T02Z
    assert rows[-1] == {"t": T02Z, "s": 1.5, "g": 3.6, "d": 70, "at": 15.9, "dp": 11.5, "rh": 75, "p": 1021.4}
    assert rows[0]["d"] == 0                                                    # a 360-degree report: north


# ---------------------------------------------------------------------------------------------- HKO and SMN parsers

def test_hko_csv_units_words_and_time():
    obs = M.parse_hko_csv(fx("hko_latest_10min_wind.csv"))
    assert len(obs) == 30
    t = 1793503200                                                              # 202611011120 Hong Kong time = 03:20Z
    assert obs["CENTRALPIER"] == [{"t": t, "s": round(16 / 3.6, 1), "g": round(23 / 3.6, 1), "d": 90}]
    assert obs["STANLEY"][0]["d"] is None and obs["STANLEY"][0]["s"] == round(19 / 3.6, 1)          # N/A
    assert obs["TSEUNGKWANO"][0]["d"] is None                                   # Variable
    assert obs["TAKWULING"][0]["d"] == 135 and obs["WAGLANISLAND"][0]["d"] == 45
    assert "HONGKONGSEASCHOO" in obs and M.hko_id("King's Park") == "KINGSPARK"
    calm = M.parse_hko_csv("202611011120,Sai Kung,Calm,N/A,3\n202611011120,X,Northwest,n/a,4\nbad,row\n")
    assert calm["SAIKUNG"] == [{"t": t, "s": 0.0, "g": round(3 / 3.6, 1), "d": None}]
    assert "X" not in calm                                                      # no speed: no reading


def test_hko_positions_cover_the_snapshot():
    snap = json.load(open(os.path.join(ROOT, "wind_stations.json"), encoding="utf-8"))
    rows = [dict(zip(snap["fields"], r)) for r in snap["stations"]]
    hk = {r["id"].partition(":")[2] for r in rows if r["src"] == "hko"}
    assert hk == {M.hko_id(n) for n in M.HKO_POSITIONS} and len(hk) == 26
    assert "CHEKLAPKOK" not in hk                                               # the airport: its METAR stands


def test_smn_tiempo_presente():
    obs = M.parse_smn_now(fx("smn_tiepre.zip"))
    assert len(obs) == 120
    assert obs["Azul"] == [{"t": T03Z, "s": round(14 / 3.6, 1), "g": None, "d": 90, "at": 15.6, "rh": 94, "p": 994.8}]
    assert obs["Benito Juárez"][0]["t"] == T03Z - 3 * 3600                     # 31-octubre 21:00, UTC-3
    line = ("Uno;01-setiembre-2026;12:00;x;10 km;5.0;No se calcula; 50;Calma;1000 / \n"
            "Dos;01-noviembre-2026;12:00;x;10 km;5.0;No se calcula; 50;Sudsudoeste  18;1000 / \n"
            "Tres;01-noviembre-2026;12:00;x;10 km;5.0;No se calcula; 50;Remolino  18;1000 / \n"
            "Cuatro;01-noviembre-2026;12:00;x;10 km;5.0;No se calcula; 50;;1000 / \n"
            "Cinco;31-brumario-2026;12:00;x;10 km;5.0;No se calcula; 50;Este  9;1000 / \n")
    o = M.parse_smn_now(line.encode("latin-1"))
    assert o["Uno"][0]["s"] == 0.0 and o["Uno"][0]["d"] is None                # Calma; "setiembre" is September
    assert o["Uno"][0]["t"] == 1788274800                                       # 2026-09-01 12:00 UTC-3 = 15:00Z
    assert o["Dos"][0]["d"] == 202 and o["Tres"][0]["d"] is None               # whole degrees; an unknown word: none
    assert set(o) == {"Uno", "Dos", "Tres"}


def test_smn_station_list_and_names():
    st = M.parse_smn_stations(fx("smn_estaciones.zip"))
    assert len(st) == 118
    assert st["89034"] == {"name": "BASE BELGRANO II", "province": "ANTARTIDA", "lat": -77.8667, "lon": -34.6167,
                           "icao": "SAYB"}
    assert st["87148"]["name"] == "PRESIDENCIA ROQUE SAENZ PEÑA AERO"          # a wrapped name joined
    now = M.parse_smn_now(fx("smn_tiepre.zip"))
    m = M.smn_match(list(now), st)
    assert len(m) == 118 and set(now) - set(m) == {"Mount Pleasant Airport (Islas Malvinas)", "Cipolletti"}
    assert m["Azul"] == next(w for w, s in st.items() if s["name"] == "AZUL AERO")
    assert M.smn_match(["Base"], {"1": {"name": "BASE A"}, "2": {"name": "BASE B"}}) == {}   # two candidates
    two = io.BytesIO()
    with zipfile.ZipFile(two, "w") as z:
        z.writestr("a.txt", "x")
        z.writestr("b.txt", "y")
    with pytest.raises(ValueError):
        M._zip_text(two.getvalue())


# ---------------------------------------------------------------------------------------------- providers

class Fetch:
    """url substring -> body (bytes / str) or an exception; records the urls asked."""

    def __init__(self, table):
        self.table, self.asked = dict(table), []

    def __call__(self, url, max_bytes, headers=None):
        self.asked.append(url)
        for k, v in self.table.items():
            if k in url:
                if isinstance(v, Exception):
                    raise v
                return v.encode("utf-8") if isinstance(v, str) else v
        raise IOError("HTTP 404")


@pytest.fixture
def clock(monkeypatch):
    t = {"now": 1793505600.0}                                       # 2026-11-01T04:00Z
    monkeypatch.setattr(buoy_sources.time, "time", lambda: t["now"])
    return t


def wis2_stations(**extra_nodes):
    st = {w: {"id": "wis2:" + w, "name": w, "lat": 0.0, "lon": 0.0, "net": "saws"} for w in (CAPE_POINT, AGULHAS, MARKEN)}
    st.update({w: {"id": "wis2:" + w, "name": w, "lat": 0.0, "lon": 0.0, "net": "imd"} for w in (MINICOY, COLABA, UNA)})
    for w, node in extra_nodes.items():
        st[w] = {"id": "wis2:" + w, "name": w, "lat": 0.0, "lon": 0.0, "net": node}
    return st


def node_url(node, name):
    base, coll = M.WIS2_NODES[node][:2]
    return "%s/collections/%s/items?f=json&name=%s&" % (base, coll, name)


def wis2_fetch():
    return Fetch({node_url("saws", "wind_speed"): fx("wis2_saws_wind_speed.json"),
                  node_url("saws", "wind_direction"): fx("wis2_saws_wind_direction.json"),
                  node_url("saws", "maximum_wind_gust_speed"): fx("wis2_saws_maximum_wind_gust_speed.json"),
                  node_url("imd", "wind_speed"): fx("wis2_imd_wind_speed.json"),
                  node_url("imd", "wind_direction"): fx("wis2_imd_wind_direction.json")})


def test_wis2_provider_asks_each_node_its_variables(clock):
    f = wis2_fetch()
    p = M.Wis2Provider(wis2_stations(), f)
    assert p.units() == ["imd", "saws"] and p.per_refresh == 2 and p.refused == ()
    lst = {e["local_id"]: e for e in p._fetch_stations()}
    assert set(lst) == {CAPE_POINT, AGULHAS, MARKEN, MINICOY, COLABA, UNA}
    assert lst[CAPE_POINT]["wind"] == [1.5, 3.6, 70] and lst[MINICOY]["wind"] == [0.0, None, None]
    saws = [u for u in f.asked if "weathersa" in u]
    imd = [u for u in f.asked if "imd.gov.in" in u]
    assert len(saws) == 3 and any("maximum_wind_gust_speed" in u for u in saws)
    assert len(imd) == 2 and not any("gust" in u for u in imd)                 # IMD's node reports no gust
    since = W.iso_z(clock["now"] - M.WIS2_LATEST_S)
    assert all("datetime=%s/.." % since in u for u in f.asked)
    nodes = p.status()["nodes"]
    assert nodes["saws"]["readings"] == 3 and nodes["imd"]["error"] is None


def test_wis2_one_node_failing_keeps_the_others(clock):
    f = wis2_fetch()
    f.table = dict({M.WIS2_NODES["gmet"][0]: IOError("HTTP 403")}, **f.table)   # a refusal: that node's failure only
    p = M.Wis2Provider(wis2_stations(**{"0-20000-0-65472": "gmet"}), f, per_refresh=3)
    lst = p._fetch_stations()
    assert {e["local_id"] for e in lst} == {CAPE_POINT, AGULHAS, MARKEN, MINICOY, COLABA, UNA}
    assert "HTTP 403" in p.status()["nodes"]["gmet"]["error"] and p.status()["paused_s"] == 0
    assert p._fetch_stations()                                         # not paused
    f.table = {"": IOError("HTTP 500")}
    with pytest.raises(RuntimeError, match="every request failed"):  # every node failing: a failed fetch (keep-last)
        p._fetch_stations()
    assert p.status()["nodes"]["saws"]["error"].endswith("HTTP 500") and p.status()["nodes"]["saws"]["ok"]


def test_wis2_round_robin_and_stations_of_another_node(clock):
    f = wis2_fetch()
    st = wis2_stations()
    st[AGULHAS]["net"] = "imd"                                         # in the snapshot under another node
    p = M.Wis2Provider(st, f, per_refresh=1)
    first = {e["local_id"] for e in p._fetch_stations()}
    assert first == {MINICOY, COLABA, UNA}                             # imd first (sorted), AGULHAS not in its answer
    second = {e["local_id"] for e in p._fetch_stations()}
    assert second == first | {CAPE_POINT, MARKEN}                      # saws's answer: its own stations only


def test_hko_and_smn_feeds_keep_their_series(clock):
    hk = {M.hko_id(n): {"id": "hko:" + M.hko_id(n), "name": n, "lat": 22.3, "lon": 114.2} for n in M.HKO_POSITIONS}
    csv_now = fx("hko_latest_10min_wind.csv")
    f = Fetch({"latest_10min_wind.csv": csv_now})
    p = M.HkoProvider(hk, f)
    lst = {e["local_id"]: e for e in p._fetch_stations()}
    assert len(lst) == 26 and "CHEKLAPKOK" not in lst and p.series
    f.table["latest_10min_wind.csv"] = csv_now.replace(b"202611011120", b"202611011130")
    clock["now"] += 600
    p._fetch_stations()
    assert [r["t"] for r in p.hist("CENTRALPIER", 0)] == [1793503200, 1793503800]
    sm = {"87641": {"id": "smn:87641", "name": "Azul", "lat": -36.8, "lon": -59.9},
          "99999": {"id": "smn:99999", "name": "Nowhere", "lat": 0.0, "lon": 0.0}}
    s = M.SmnProvider(sm, Fetch({"dato=tiepre": fx("smn_tiepre.zip")}))
    lst = s._fetch_stations()
    assert [e["local_id"] for e in lst] == ["87641"] and lst[0]["wind"] == [round(14 / 3.6, 1), None, 90]
    assert [r["t"] for r in s.hist("87641", 0)] == [T03Z] and s.hist("99999", 0) == []
    with pytest.raises(RuntimeError, match="no reading"):
        M.SmnProvider({"99999": sm["99999"]}, Fetch({"dato=tiepre": fx("smn_tiepre.zip")}))._fetch_stations()


def test_make_providers_builds_the_section_73_feeds():
    groups = {"wis2": wis2_stations(), "hko": {"CENTRALPIER": {"id": "hko:CENTRALPIER", "name": "Central Pier"}},
              "smn": {"87641": {"id": "smn:87641", "name": "Azul"}}}
    ps = M.make_providers(groups, Fetch({}), per_refresh={"wis2": 5})
    assert [p.source for p in ps] == ["WIS2", "HKO", "SMN"]
    assert ps[0].per_refresh == 5
    assert M.make_providers(groups, Fetch({}), enabled={"wis2": False, "hko": False})[0].source == "SMN"
    for p in ps:
        assert p.attribution_text.startswith("Source: ") and p.source_url.startswith("https://")


# ---------------------------------------------------------------------------------------------- the windows' history

def test_wis2_history_filters_the_station_and_names_the_node(clock):
    hist = json.loads(fx("wis2_saws_history_68916.json"))
    hist["features"].append(feature(AGULHAS, "wind_speed", 9.9, "m/s", when="2026-11-01T02:00:00Z"))
    st = {"wis2:" + CAPE_POINT: {"id": "wis2:" + CAPE_POINT, "name": "Cape Point", "kind": "met", "src": "wis2",
                                 "tz": "Africa/Johannesburg", "net": "saws"},
          "wis2:" + MINICOY: {"id": "wis2:" + MINICOY, "name": "Minicoy", "kind": "met", "src": "wis2",
                              "tz": "Asia/Kolkata", "net": "nowhere"}}
    f = Fetch({"wigos_station_identifier=" + CAPE_POINT: json.dumps(hist)})
    svc = W.WindHistory(st, f, now=lambda: clock["now"])
    status, out = svc.history("wis2:" + CAPE_POINT)
    assert status == "ok" and len(out["t"]) == 23 and 9.9 not in out["s"]   # the node's answer filtered here
    assert out["source"] == W.ATTRIBUTION["wis2:saws"] and out["net"] == "saws" and out["at"][-1] == 15.9
    u = f.asked[-1]
    assert u.startswith(M.WIS2_NODES["saws"][0]) and "name=" not in u      # one property filter only (IMD's node)
    assert "datetime=%s/.." % W.iso_z(int(clock["now"]) - W.HISTORY_S) in u
    f.table["items?f=json"] = doc()                                        # every node would answer
    status, _ = svc.history("wis2:" + MINICOY)
    assert status == "error" and not any(MINICOY in u for u in f.asked)   # a node the site does not know: not asked


def test_wis2_history_agency_failure_and_bad_shape(clock):
    st = {"wis2:" + CAPE_POINT: {"id": "wis2:" + CAPE_POINT, "name": "C", "kind": "met", "src": "wis2", "tz": "UTC",
                                 "net": "saws"}}
    assert W.WindHistory(st, Fetch({"items": IOError("HTTP 503")}),
                         now=lambda: clock["now"]).history("wis2:" + CAPE_POINT)[0] == "error"
    assert W.WindHistory(st, Fetch({"items": "[1, 2]"}),
                         now=lambda: clock["now"]).history("wis2:" + CAPE_POINT)[0] == "error"
    status, out = W.WindHistory(st, Fetch({"items": doc()}), now=lambda: clock["now"]).history("wis2:" + CAPE_POINT)
    assert status == "ok" and out["t"] == [] and out["note"] == W.NO_HISTORY


def test_id_rule_and_attribution_for_section_73():
    for sid in ("wis2:" + CAPE_POINT, "wis2:0-454-2-AB12", "hko:CENTRALPIER", "hko:HONGKONGSEASCHOO", "smn:87641"):
        assert W.valid_id(sid), sid
    for bad in ("wis2:68916", "wis2:0-20000-0-", "wis2:0-123456-0-1", "hko:central pier", "smn:", "wis3:0-1-0-1"):
        assert not W.valid_id(bad), bad
    assert {"wis2:" + n for n in M.WIS2_NODES} <= set(W.ATTRIBUTION)
    assert {"wis2", "hko", "smn"} <= set(W.MORE_SOURCES) and {"hko", "smn"} <= set(W.MORE_KEPT)
    assert "wis2" not in W.MORE_KEPT                                       # its windows ask the node
