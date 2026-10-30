"""Plan section 67: the station tool for the added wind sources (tools/wind/more_stations.py): the coast-and-lake rule,
the spot rule and DWD's German-station filter, on small inputs and the coast fixtures (no network)."""
import importlib.util
import os
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def _tool():
    spec = importlib.util.spec_from_file_location("wind_more_stations",
                                                  os.path.join(ROOT, "tools", "wind", "more_stations.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


T = _tool()


def _edges(name):
    import point_forecast as P
    with open(os.path.join(HERE, "fixtures", "coast", name), "rb") as f:
        return P.decode_coast(f.read())


# ------------------------------------------------------------------ the coast rule

def test_coast_or_large_lake_shore_within_30_km_or_at_sea():
    sea, lake = _edges("hawaii-t0.bin"), _edges("lakeoutlines-i.bin")   # the Hawaii crop + the published lake set
    counts = {"sea": 0, "lake": 0, "offshore": 0, "inland": 0}
    coastal = T.make_coastal(sea, lake, counts)
    assert T.FS.CoastIndex(sea).distance_km(19.68, -155.47, 30) > 30       # the Big Island's saddle: 40 km inland
    pos = {"honolulu": (21.31505, -157.924), "wheeler": (21.478, -158.044), "saddle": (19.68, -155.47),
           "buoy": (19.151, -160.617), "clewiston": (26.754, -80.934), "placeholder": (-99.99, -99.99)}
    assert coastal(pos) == {"honolulu", "wheeler", "buoy", "clewiston"}   # Clewiston: Lake Okeechobee's shore
    assert counts == {"sea": 2, "lake": 1, "offshore": 1, "inland": 1}, counts


# ------------------------------------------------------------------ the spot rule

def _c(src, local, lat, lon, kind="met", net=None, icao=None):
    return {"src": src, "local": local, "name": local, "lat": lat, "lon": lon, "kind": kind, "net": net, "icao": icao}


def test_spot_rule_metar_twins_airfields_ranks_duplicates_and_dwd_2_km():
    rows = [["metar:CWEB", "Webster", 50.0, -100.0, "airport", "metar", "America/Winnipeg", None, None, None],
            ["metar:KX50", "X", 30.0, -90.0, "airport", "metar", "America/Chicago", None, None, None],
            ["ndbc:ABCD1", "b", 40.0, -70.0, "station", "ndbc", "America/New_York", None, None, None]]
    cands = [_c("iem", "R1", 60.0, 20.0, kind="road", net="AK_RWIS"),       # 44 m from jma:2: JMA ranks first
             _c("eccc", "CWEB", 51.0, -101.0, icao="CWEB"),                 # the METAR id itself (a NAV CANADA file)
             _c("iem", "X50", 31.0, -91.0, kind="airport", net="FL_ASOS", icao="X50"),   # 3-letter ASOS id: KX50
             _c("jma", "11001", 30.003, -90.0),                             # 333 m from an airfield
             _c("smhi", "1", 40.0005, -70.0),                               # 56 m from an NDBC station
             _c("dwd", "10001", 60.0135, 20.0),                             # 1.46 km from jma:2
             _c("dwd", "10002", 60.03, 20.0),                               # 3.3 km: kept
             _c("jma", "2", 60.0004, 20.0),
             _c("jma", "2", 61.0, 21.0)]                                    # the same id twice
    kept, dropped = T.place(cands, rows)
    assert [r[0] for r in kept] == ["jma:2", "dwd:10002"]
    assert kept[0] == ["jma:2", "2", 60.0004, 20.0, "met", "jma", None, None, None, None]
    why = {cid: (w, k) for cid, w, k in dropped}
    assert why["eccc:CWEB"] == ("METAR id", "metar:CWEB")
    assert why["iem:X50"] == ("METAR id", "metar:KX50")
    assert why["jma:11001"] == ("airfield", "metar:KX50")             # a weather service's station on the airfield
    assert why["smhi:1"] == ("spot", "ndbc:ABCD1")
    assert why["dwd:10001"] == ("spot", "jma:2") and why["iem:R1"] == ("spot", "jma:2")
    assert [d for d in dropped if d[1] == "duplicate id"] == [("jma:2", "duplicate id", "jma:2")]


# ------------------------------------------------------------------ DWD: Germany's own stations

def test_dwd_keeps_only_its_own_german_stations(monkeypatch):
    listed = ["02418", "10020", "06680", "10015"]                       # Karlstad (Sweden), Sylt, Saentis, Helgoland

    class M:
        DWD_POI, DWD_CATALOG, DWD_CSV = "poi", "cat", "csv/{sid}"

        @staticmethod
        def parse_dwd_listing(text):
            return list(listed)

        @staticmethod
        def parse_dwd_catalog(text):
            return {s: {"name": "LIST/SYLT" if s == "10020" else "X", "lat": 55.0, "lon": 8.4, "icao": None} for s in listed}

        @staticmethod
        def parse_dwd_poi(raw):
            return [{"t": time.time() - 600}]

    class TT:
        @staticmethod
        def clean_name(n):
            return n.title()

    asked = []
    monkeypatch.setattr(T, "get", lambda url, **kw: asked.append(url) or b"")
    monkeypatch.setattr(T.time, "sleep", lambda s: None)
    out = T.dwd(M, T.Cache(None), lambda pos: set(pos), TT)
    assert [o["local"] for o in out] == ["10015", "10020"]
    assert [o["name"] for o in out] == ["X", "List/Sylt"]
    assert "csv/02418" not in asked and "csv/06680" not in asked, "other countries' stations are never read"


def test_place_name_fixes_only_names_in_capitals():
    class TT:
        @staticmethod
        def clean_name(n):
            return n.title()
    assert T.place_name("  ROCHES   POINT ", TT) == "Roches Point"
    assert T.place_name("Malin Head", TT) == "Malin Head"


# ------------------------------------------------------------------ the airfield rule (step 4 finding)

def test_a_weather_services_station_on_an_airfield_is_the_airports():
    rows = [["metar:EKCH", "Kobenhavn Intl", 55.618, 12.656, "airport", "metar", "Europe/Paris", None, None, None],
            ["metar:EIDW", "Dublin Arpt", 53.421, -6.270, "airport", "metar", "Europe/Dublin", None, None, None]]
    cands = [_c("dmi", "06180", 55.614, 12.645),                           # 0.8 km: the airfield's own station
             _c("mei", "DUBLIN", 53.428, -6.241),                          # 2.1 km, named like the airport
             _c("smhi", "1", 55.635, 12.656),                              # 1.9 km, another name: kept
             _c("dmi", "2", 55.618, 12.68),                                # 1.5 km +: "Flygplats" in its name
             _c("iem", "R9", 55.62, 12.648, kind="road", net="AK_RWIS")]   # 0.6 km road weather: the 500 m rule only
    cands[3]["name"] = "Kastrup Flygplats"
    kept, dropped = T.place(cands, rows)
    why = {cid: (w, k) for cid, w, k in dropped}
    assert why["dmi:06180"] == ("airfield", "metar:EKCH") and why["mei:DUBLIN"] == ("airfield", "metar:EIDW")
    assert why["dmi:2"] == ("airfield", "metar:EKCH")
    assert [r[0] for r in kept] == ["smhi:1", "iem:R9"]
    assert T.fold("Københavns  Lufthavn!") == "kobenhavns lufthavn"


def test_respot_places_the_snapshots_rows_again_without_the_network(tmp_path):
    src = {"source": "S", "captured": "2026-10-30T21:35:05Z", "fields": T.FS.FIELDS, "stations": [
        ["metar:EKCH", "Kobenhavn Intl", 55.618, 12.656, "airport", "metar", "Europe/Paris", None, None, None],
        ["dmi:06180", "Københavns Lufthavn", 55.614, 12.645, "met", "dmi", "Europe/Paris", None, None, None],
        ["dmi:06183", "Drogden Fyr", 55.536, 12.712, "met", "dmi", "Europe/Paris", None, None, None]]}
    f = tmp_path / "w.json"
    import json
    f.write_text(json.dumps(src), encoding="utf-8")
    assert T.main(["--respot", "--out", str(f), "--only", "dmi"]) == 0
    doc = json.loads(f.read_text(encoding="utf-8"))
    assert [r[0] for r in doc["stations"]] == ["metar:EKCH", "dmi:06183"]
    assert doc["captured"] == "2026-10-30T21:35:05Z" and doc["stations"][1][6] == "Europe/Paris"


# ------------------------------------------------------------------ G44: Canadian airfields, NDBC twins (P2-3, P3-2)

def test_an_airports_own_station_stands_for_its_field_among_the_new_rows():
    rows = [["ndbc:KP53", "Munising", 46.41, -86.65, "station", "ndbc", "America/Detroit", None, None, None]]
    cands = [_c("eccc", "CZHY", 60.840, -115.783),                  # Hay River climate station, 0.11 km from CYHY
             _c("eccc", "CYHY", 60.839, -115.782),                  # the NAV CANADA airport station: placed first
             _c("eccc", "COSM", 46.486, -84.505),                   # "Sault Ste. Marie Airport", 0.94 km from CYAM
             _c("eccc", "CYAM", 46.485, -84.517),
             _c("eccc", "CWXX", 46.600, -84.517),                   # 12.7 km away: kept
             _c("iem", "P53", 46.415, -86.655, kind="airport", net="MI_ASOS", icao="P53")]
    cands[2]["name"] = "Sault Ste. Marie Airport"
    kept, dropped = T.place(cands, rows)
    why = {cid: (w, k) for cid, w, k in dropped}
    assert [r[0] for r in kept] == ["eccc:CYAM", "eccc:CYHY", "eccc:CWXX"]
    assert why["eccc:CZHY"] == ("airfield", "eccc:CYHY") and why["eccc:COSM"] == ("airfield", "eccc:CYAM")
    assert why["iem:P53"] == ("NDBC id", "ndbc:KP53")
    assert T.field_rank(cands[1]) == 0 and T.field_rank(cands[2]) == 1 and T.field_rank(cands[0]) == 2
    assert not T.is_field(_c("iem", "X", 1, 1, kind="road"))


def test_fixed_zones_cover_the_one_country_agencies():
    assert T.FIXED_TZ == {"jma": "Asia/Tokyo", "fmi": "Europe/Helsinki", "smhi": "Europe/Stockholm",
                          "dwd": "Europe/Berlin", "mei": "Europe/Dublin"}
