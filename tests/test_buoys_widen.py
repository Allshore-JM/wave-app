"""Plan section 63: the US widen of the live-buoy layer (NDBC bulk report, GLOS Seagull, IOOS sensors).
Real answers cut by tests/capture_buoy_widen_fixtures.py, served by a fake http keyed on the URL."""
import copy
import json
import os
from datetime import datetime, timezone

import pytest
import requests

import buoy_sources as B

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "buoys_widen")
# the fixtures were captured 2026-10-30 ~04:25 UTC
CAPTURED = datetime(2026, 10, 30, 4, 25, tzinfo=timezone.utc).timestamp()


def fixture(name, mode="r"):
    with open(os.path.join(FIX, name), mode, **({"encoding": "utf-8"} if mode == "r" else {})) as f:
        return f.read()


class Resp:
    def __init__(self, body, status=200):
        self.status_code = status
        self.content = body.encode("utf-8") if isinstance(body, str) else body
        self.text = self.content.decode("utf-8", "replace")

    def json(self):
        return json.loads(self.text)


class FakeHttp:
    """routes: [(url substring, body | callable(url) -> body | Resp)]; every GET is recorded."""

    def __init__(self, routes):
        self.routes = list(routes)
        self.calls = []

    def get(self, url, timeout=None, **kw):
        self.calls.append(url)
        for key, body in self.routes:
            if key in url:
                if callable(body):
                    body = body(url)
                return body if isinstance(body, Resp) else Resp(body)
        return Resp("not found", 404)


# ------------------------------------------------------------------ NDBC bulk report

def ndbc_http(**over):
    routes = dict(latest=fixture("ndbc_latest_obs.txt"), rt=fixture("ndbc_realtime2_44137.txt"))
    routes.update(over)
    return FakeHttp([("latest_obs/latest_obs.txt", routes["latest"]), ("realtime2/44137.txt", routes["rt"])])


def ndbc_list(http=None, spectral=("46026", "41009"), names=None):
    p = B.NdbcBulkProvider(http=http or ndbc_http(), spectral_ids=lambda: set(spectral),
                           names=(lambda: names) if names is not None else (lambda: {"44137": "East Scotia Slope"}))
    lst, ok, err = p._build_station_list()
    return p, {s["id"]: s for s in lst}, ok, err


def test_ndbc_bulk_lists_wave_rows_without_a_spectral_file():
    p, by, ok, err = ndbc_list()
    assert ok and err is None
    # 46026 / 41009: spectral (the NDBC provider lists them); 22101: KMA placeholder; KPTN6: wind only
    assert set(by) == {"ndbcw:41110", "ndbcw:44137", "ndbcw:45007", "ndbcw:45029", "ndbcw:62145"} - \
        ({"ndbcw:45007"} if "45007" not in fixture("ndbc_latest_obs.txt") else set())
    s = by["ndbcw:44137"]
    assert s["name"] == "East Scotia Slope" and s["source"] == "NDBCW"
    assert s["latest_time"] == "2026-10-30T04:00:00Z"
    assert s["capabilities"]["recent_history"] and not s["capabilities"]["spectra"]
    assert not s["capabilities"]["directional"]                  # Canada's DFO buoys: no MWD
    assert by["ndbcw:41110"]["capabilities"]["directional"]       # MWD 112
    assert by["ndbcw:41110"]["name"] == "41110"                   # no name known: the number


def test_ndbc_bulk_latest_values_in_site_units():
    p, by, ok, err = ndbc_list()
    o = p.latest("41110")
    assert o == {"time_utc": "2026-10-30T04:00:00Z", "hs_m": 1.3, "tp_s": 8.0, "mean_period_s": 5.0,
                 "dir_deg": 112.0, "sst_c": 21.3, "dir_kind": "from"}
    o = p.latest("62145")                                         # UK platform: height only
    assert o["hs_m"] == 0.4 and o["tp_s"] is None and o["dir_deg"] is None


def test_ndbc_bulk_without_the_spectral_set_still_lists_and_says_so(caplog):
    def broken():
        raise RuntimeError("NDBC listing down")
    p = B.NdbcBulkProvider(http=ndbc_http(), spectral_ids=broken, names=None)
    lst, ok, err = p._build_station_list()
    assert ok and "ndbcw:46026" in {s["id"] for s in lst}         # the merge's position dedup still applies
    assert "unavailable" in caplog.text


@pytest.mark.parametrize("body", [Resp("oops", 503), "<html>maintenance</html>",
                                  "#STN LAT LON\n44137 42 -62\n"])
def test_ndbc_bulk_failures_raise_for_keep_last(body):
    p = B.NdbcBulkProvider(http=ndbc_http(latest=body))
    lst, ok, err = p._build_station_list()
    assert not ok and lst == [] and err


def test_ndbc_bulk_byte_cap():
    big = fixture("ndbc_latest_obs.txt") + ("#" + "x" * 100 + "\n") * 12000
    p = B.NdbcBulkProvider(http=ndbc_http(latest=big))
    lst, ok, err = p._build_station_list()
    assert not ok and "cap" in err


def test_ndbc_bulk_detail_history_newest_first_and_cached():
    http = ndbc_http()
    p, by, ok, err = ndbc_list(http=http)
    d = p.detail("44137")
    rec = d["recent"]
    assert rec and rec[0]["time_utc"] == "2026-10-30T04:00:00Z"   # newest first
    assert all(r["hs_m"] is not None for r in rec)
    assert len(rec) <= 48 and d["latest"]["hs_m"] == 0.9
    n = len(http.calls)
    p.detail("44137")
    assert len(http.calls) == n                                   # kept a few minutes
    p.detail("44137", fresh_s=0)                                  # a refresh press asks again
    assert len(http.calls) == n + 1


def test_ndbc_bulk_detail_refuses_odd_ids_and_survives_a_failed_file():
    http = ndbc_http()
    p, by, ok, err = ndbc_list(http=http)
    n = len(http.calls)
    assert p.detail("../44137") == {"latest": None, "recent": []}
    assert len(http.calls) == n
    d = p.detail("41110")                                         # realtime2 404: the latest only
    assert d["latest"]["hs_m"] == 1.3 and d["recent"] == []


def test_parse_ndbc_realtime_waves_rows_and_limit():
    lines = fixture("ndbc_realtime2_44137.txt").splitlines()
    obs = B.parse_ndbc_realtime_waves(lines)
    assert obs[-1]["time_utc"] == "2026-10-30T04:00:00Z" and obs[0]["time_utc"] < obs[-1]["time_utc"]
    assert len(B.parse_ndbc_realtime_waves(lines, max_rows=5)) == 5
    gap = lines[:2] + [lines[2].replace(" 0.9 ", "  MM ", 1)] + lines[3:]
    assert len(B.parse_ndbc_realtime_waves(gap)) == len(obs) - 1   # a row without a height is left out
    with pytest.raises(ValueError):
        B.parse_ndbc_realtime_waves(["<html>"])


# ------------------------------------------------------------------ GLOS Seagull

# the mirror's "newest wave height" answer, as ERDDAP writes it (header, units, one row)
GLOS_PROBE_YES = "time,sea_surface_wave_significant_height\nUTC,m\n2026-10-28T23:50:00Z,0.20900000631809235\n"
IOOS_PROBE_YES = "time,sea_surface_wave_significant_height\nUTC,m\n2026-10-30T03:00:00Z,1.2\n"
NO_MATCH = Resp('Error {\n    code=404;\n    message="Not Found: Your query produced no matching results. (nRows = 0)";\n}\n', 404)


def glos_http(mirror=None, params=None, roster=None, probe=None):
    return FakeHttp([
        ("tabledap/obs_", probe if probe is not None else GLOS_PROBE_YES),
        ("obs-datasets.geojson", roster if roster is not None else fixture("glos_roster.geojson")),
        ("api/v1/parameters", params if params is not None else fixture("glos_parameters.json")),
        ("allDatasets.csv", mirror if mirror is not None else fixture("glos_mirror.csv")),
        ("obsDatasetId=3&", fixture("glos_obs_3.json")),
        ("obsDatasetId=139&", fixture("glos_obs_139.json")),
    ])


def glos(http=None, now=CAPTURED):
    p = B.GlosProvider(http=http or glos_http())
    p.clock = staticmethod(lambda: now)
    lst, ok, err = p._build_station_list()
    return p, {s["id"]: s for s in lst}, ok, err


def test_glos_lists_activated_wave_platforms_and_leaves_silent_ones_out():
    p, by, ok, err = glos()
    assert ok
    # 45204 (dataset 260): 'activated' in the roster but nothing since 10-15 in the mirror;
    # GRIM4 recovered; SRLM4 has no wave height
    assert set(by) == {"glos:45013", "glos:45029", "glos:SPOT-1360"}
    s = by["glos:SPOT-1360"]
    assert s["name"] == "Marquette" and s["capabilities"]["directional"] and s["source"] == "GLOS"


def test_glos_without_the_mirror_lists_every_activated_platform():
    p, by, ok, err = glos(http=glos_http(mirror=Resp("down", 502)))
    assert ok and "glos:45204" in by


def test_glos_detail_names_series_converts_kelvin_and_fills_within_30_min():
    p, by, ok, err = glos()
    d = p.detail("45029")
    o = d["latest"]
    assert o["hs_m"] is not None and 0 < o["sst_c"] < 40          # stored in kelvin
    assert o["dir_deg"] is not None and o["tp_s"] is not None      # Tp stamped minutes from Hs: filled
    rec = d["recent"]
    assert rec[0]["time_utc"] == o["time_utc"] and rec[0]["time_utc"] > rec[-1]["time_utc"]
    sp = p.detail("SPOT-1360")["latest"]
    peak = [q for q in json.loads(fixture("glos_obs_139.json"))[0]["parameters"] if q["parameter_id"] == 3010]
    assert sp["dir_deg"] == peak[0]["observations"][0]["value"]    # the peak direction is preferred


def test_glos_detail_cache_refresh_and_unknown():
    http = glos_http()
    p, by, ok, err = glos(http=http)
    p.detail("45029")
    n = len(http.calls)
    p.detail("45029")
    assert len(http.calls) == n
    p.detail("45029", fresh_s=0)
    assert len(http.calls) == n + 1
    assert p.detail("NOPE") == {"latest": None, "recent": []}


def test_glos_obs_start_date_is_yesterday_utc():
    http = glos_http()
    p, by, ok, err = glos(http=http)
    p.detail("45013")
    assert any("obsDatasetId=2&startDate=2026-10-29" in u for u in http.calls)


@pytest.mark.parametrize("roster", [Resp("x", 500), "{\"type\": \"FeatureCollection\"}", "not json"])
def test_glos_roster_failures_raise(roster):
    p, by, ok, err = glos(http=glos_http(roster=roster))
    assert not ok and by == {}


def test_kelvin_rule():
    assert B._kelvin_to_c(288.15) == 15.0 and B._kelvin_to_c(14.2) == 14.2 and B._kelvin_to_c(None) is None


# ------------------------------------------------------------------ IOOS sensors

def ioos_http(live=None, index=None, one=None, probe=None):
    return FakeHttp([
        ("categorize/standard_name", index if index is not None else fixture("ioos_index.csv")),
        ("allDatasets.csv", live if live is not None else fixture("ioos_alldatasets.csv")),
        ("tabledap/cap2wave-capers-nearshore-wave.csv", one if one is not None else fixture("ioos_cap2wave.csv")),
        ("orderByMax", probe if probe is not None else IOOS_PROBE_YES),
    ])


def ioos(http=None, covered=None, now=CAPTURED):
    p = B.IoosSensorsProvider(http=http or ioos_http(), covered_ids=(lambda: covered) if covered is not None else None)
    p.clock = staticmethod(lambda: now)
    lst, ok, err = p._build_station_list()
    return p, {s["id"]: s for s in lst}, ok, err


def test_ioos_ids_names_and_copies():
    p, by, ok, err = ioos()
    assert ok
    assert {"ioos:41065", "ioos:41110", "ioos:45135", "ioos:SIPF1"} <= set(by)
    assert by["ioos:41065"]["name"] == "Capers Nearshore Wave, SC"
    # two datasets of 41065 (the network's and the regional association's copy): the newer is kept
    assert p._ds["41065"]["dataset"] == "cap2wave-capers-nearshore-wave"
    assert by["ioos:41065"]["latest_time"] == "2026-10-30T03:55:31Z"     # its newest wave height (maxTime: 02:55:31)
    assert by["ioos:45135"]["name"] == "45135 - Prince Edward Pt"


def test_ioos_moving_platforms_left_out():
    live = fixture("ioos_alldatasets.csv").replace("-80.445,-80.445,27.862,27.862",
                                                     "-80.445,-79.9,27.862,27.862")
    p, by, ok, err = ioos(http=ioos_http(live=live))
    assert "ioos:SIPF1" not in by and "ioos:41065" in by


def test_ioos_detail_names_only_the_datasets_variables_and_its_operator():
    http = ioos_http()
    p, by, ok, err = ioos(http=http)
    d = p.detail("41065")
    url = [u for u in http.calls if "cap2wave-capers-nearshore-wave.csv" in u and "orderByMax" not in u][0]
    cols = url.split(".csv?", 1)[1].split("&", 1)[0].split(",")
    assert cols[0] == "time" and "sea_surface_wave_significant_height" in cols
    assert all(c in p._ds["41065"]["vars"] for c in cols)          # ERDDAP refuses a variable it lacks
    o = d["latest"]
    assert o["hs_m"] is not None and o["tp_s"] is not None and o["dir_deg"] is not None
    assert d["recent"][0]["time_utc"] == o["time_utc"]
    assert "Coastal Ocean Research and Monitoring Program" in d["attribution_text"]


def test_ioos_rows_without_height_or_failed_qc_left_out():
    lines = fixture("ioos_cap2wave.csv").splitlines()
    lines[2] = lines[2].replace(",1.26,2,", ",NaN,2,")
    lines[3] = lines[3].replace(",1.29,2,", ",1.29,4,")
    p, by, ok, err = ioos(http=ioos_http(one="\n".join(lines) + "\n"))
    d = p.detail("41065")
    times = {r["time_utc"] for r in d["recent"]}
    assert "2026-10-29T04:55:31Z" not in times and "2026-10-29T05:55:31Z" not in times


@pytest.mark.parametrize("over", [dict(index="a,b\n1,2\n"), dict(live=Resp("x", 500)), dict(index=Resp("x", 503)),
                                  dict(live="a,b\nc,d\n1,2\n")])
def test_ioos_list_failures_raise(over):
    p, by, ok, err = ioos(http=ioos_http(**over))
    assert not ok


def test_ioos_local_id_and_name_rules():
    f = B._ioos_local_id
    assert f("gov-ndbc-44008", "x") == "44008" and f("ism-cencoos-wmo_46042", "x") == "46042"
    assert f("edu_ucsd_cdip_181", "181 - Rincon, PUERTO RICO (41115)") == "41115"
    assert f("x", "[SFC Buoy] 44063 - Annapolis, MD") == "44063"
    assert f("ism-secoora-edu_fit_sipf1_x", "no number") == "edu_fit_sipf1_x"
    assert B._ioos_name("(41070) - Ponce de Leon Inlet Wave, FL") == "Ponce de Leon Inlet Wave, FL"
    assert B._ioos_name("[SFC Buoy] 44063 - Annapolis, MD") == "44063 - Annapolis, MD"


def test_erddap_index_variables_from_the_summary():
    s = "x" + chr(92) + "nVARIABLES:" + chr(92) + "ntime (seconds)" + chr(92) + "nsea_x (A, m)" + chr(92) + "n"
    assert B._erddap_index_variables(s) == ["time", "sea_x"]


# ------------------------------------------------------------------ merge + priority

def st(src, local, lat, lon, name=None, caps=None):
    return {"id": "%s:%s" % (src.lower(), local), "source": src, "name": name or local, "lat": lat, "lon": lon,
            "capabilities": caps or {}}


def test_same_buoy_number_merges_across_feeds_despite_positions():
    nd = st("NDBC", "41056", 18.26, -65.46)
    io = st("IOOS", "41056", 17.10, -65.30)                         # 130 km off (the ERDDAP's range corner)
    kept = B.merge_stations([[nd], [io]])
    assert [k["id"] for k in kept] == ["ndbc:41056"] and kept[0]["also_sources"] == ["IOOS"]


def test_buoy_number_rule_limits():
    a = st("NDBC", "41056", 18.0, -65.0)
    far = st("IOOS", "41056", 22.0, -60.0)                          # > 300 km: not the same buoy
    other = st("IOOS", "41057", 18.0005, -65.3)
    cdip = st("CDIP", "41056", 17.0, -65.0)                         # CDIP numbers are its own
    kept = B.merge_stations([[a], [far, other], [cdip]])
    assert len(kept) == 4


def test_priorities_keep_national_and_spectral_first():
    P = B._station_priority
    assert P(st("NDBC", "1", 0, 0)) > P(st("CDIP", "1", 0, 0)) > P(st("GLOS", "1", 0, 0))
    assert P(st("GLOS", "1", 0, 0)) > P(st("IOOS", "1", 0, 0)) > P(st("NDBCW", "1", 0, 0)) > P(st("CMEMS", "1", 0, 0))
    # GLOS ranks just below AODN (they never share a buoy) so the merged list keeps its old order
    assert P(st("AODN", "1", 0, 0)) > P(st("GLOS", "1", 0, 0))


# ------------------------------------------------------------------ the route

def test_latest_route_passes_refresh_and_operator(monkeypatch):
    import app as A
    p = B.IoosSensorsProvider(http=ioos_http())
    p._build_station_list()
    seen = []
    real = p.detail

    def spy(local, fresh_s=None):
        seen.append(fresh_s)
        return real(local, fresh_s=fresh_s)
    monkeypatch.setattr(p, "detail", spy)
    monkeypatch.setattr(A, "get_buoy_providers", lambda: [p])
    c = A.app.test_client()
    r = c.get("/api/buoys/ioos:41065/latest")
    j = r.get_json()
    assert r.status_code == 200 and j["latest"]["hs_m"] is not None
    assert "Coastal Ocean Research and Monitoring Program" in j["attribution_text"]
    c.get("/api/buoys/ioos:41065/latest", headers={A.REFRESH_HEADER: "1"})
    assert seen == [None, A.REFRESH_MIN_AGE_S]


def test_providers_make_no_request_at_construction():
    for cls in (B.NdbcBulkProvider, B.GlosProvider, B.IoosSensorsProvider):
        http = FakeHttp([])
        cls(http=http)
        assert http.calls == []


# ------------------------------------------------------------------ pins found by the mutation pass

def _roster_with(*edits):
    """The roster fixture plus copies of 45013's feature edited by each callable."""
    doc = json.loads(fixture("glos_roster.geojson"))
    base = [x for x in doc["features"] if x["properties"]["org_platform_id"] == "45013"][0]
    for edit in edits:
        f = copy.deepcopy(base)
        edit(f["properties"])
        doc["features"].append(f)
    return json.dumps(doc)


def test_buoy_number_only_for_numbers():
    assert B._buoy_number(st("IOOS", "41056", 0, 0)) == "41056"
    assert B._buoy_number(st("GLOS", "SPOT-1360", 0, 0)) == ""
    assert B._buoy_number(st("CDIP", "41056", 0, 0)) == ""
    a = st("GLOS", "SPOT-1360", 45.0, -86.0)
    b = st("IOOS", "SPOT-1360", 45.5, -86.0)          # 55 km apart; a Spotter's serial is not a buoy number
    assert len(B.merge_stations([[a], [b]])) == 2


def test_glos_recovered_platform_with_waves_left_out():
    roster = _roster_with(lambda p: p.update(platform_event="recovered", org_platform_id="45999", obs_dataset_id=999))
    p, by, ok, err = glos(http=glos_http(roster=roster))
    assert ok and "glos:45999" not in by and "glos:45013" in by


def test_glos_direction_capability_id_fallback_and_a_platform_listed_twice():
    def nodir(p):
        p.update(org_platform_id="45998", obs_dataset_id=998)
        p["parameters"] = [q for q in p["parameters"] if "wave_from_direction" not in (q.get("standard_name") or "")]
    roster = _roster_with(nodir,
                          lambda p: p.update(org_platform_id="no such id!", obs_dataset_id=997),
                          lambda p: p.update(obs_dataset_id=996))       # 45013 again: the first row wins
    p, by, ok, err = glos(http=glos_http(roster=roster))
    assert not by["glos:45998"]["capabilities"]["directional"]
    assert by["glos:45013"]["capabilities"]["directional"]
    assert "glos:997" in by                                             # an odd platform id: the dataset id
    assert p._dataset_by_id["45013"] == 2


def test_glos_water_temperature_from_the_shallowest_sensor():
    p, by, ok, err = glos()
    o = p.detail("45029")["latest"]
    pars = {x["parameter_id"]: x for x in json.loads(fixture("glos_parameters.json"))}
    series = {}
    for block in json.loads(fixture("glos_obs_3.json")):
        for par in block["parameters"]:
            meta = pars.get(par["parameter_id"])
            if meta and meta.get("standard_name") == "sea_water_temperature":
                series[meta.get("depth")] = sorted((q["timestamp"][:19], q["value"]) for q in par["observations"])

    def at(depth):
        t = o["time_utc"][:19]
        ep = datetime.fromisoformat(t).replace(tzinfo=timezone.utc).timestamp()
        best = [v for ts, v in series[depth]
                if 0 <= ep - datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp() <= 1800]
        return round(best[-1] - 273.15, 2)
    assert at(0.0) != at(1.0)                       # the two sensors differ at this time
    assert o["sst_c"] == at(0.0)


def test_ioos_position_is_the_middle_of_its_range_and_direction_from_its_variables():
    live = fixture("ioos_alldatasets.csv")
    wide = live.replace("-79.6188,-79.6188,32.8016,32.8016", "-79.6188,-79.5188,32.8016,32.9016")
    assert wide != live
    p, by, ok, err = ioos(http=ioos_http(live=wide))
    s = by["ioos:41065"]
    assert abs(s["lat"] - 32.8516) < 1e-9 and abs(s["lon"] + 79.5688) < 1e-9
    assert s["capabilities"]["directional"]
    assert not by["ioos:45135"]["capabilities"]["directional"]       # wind direction only


def test_ioos_detail_kept_a_few_minutes_and_asked_again_on_refresh():
    http = ioos_http()
    p, by, ok, err = ioos(http=http)
    p.detail("41065")
    n = len(http.calls)
    p.detail("41065")
    assert len(http.calls) == n
    p.detail("41065", fresh_s=0)
    assert len(http.calls) == n + 1


def test_ioos_partitions_from_swell_and_wind_sea_columns():
    p = B.IoosSensorsProvider(http=FakeHttp([]))
    hdr = ["time", p.HS, "sea_surface_swell_wave_significant_height", "sea_surface_swell_wave_period",
           "sea_surface_swell_wave_from_direction", "sea_surface_wind_wave_significant_height"]
    o = p._obs(["2026-10-30T02:00:00Z", "1.5", "1.1", "12.5", "300", "0.6"], {c: i for i, c in enumerate(hdr)})
    assert o["partitions"] == [{"label": "Swell", "hs_m": 1.1, "period_s": 12.5, "dir_deg": 300.0},
                               {"label": "Wind sea", "hs_m": 0.6, "period_s": None, "dir_deg": None}]
    cols = p._columns(hdr)
    assert "sea_surface_swell_wave_period" in cols and "sea_surface_wind_wave_period" not in cols


def test_ndbc_bulk_detail_takes_the_newer_history_reading():
    text = fixture("ndbc_latest_obs.txt")
    older = text.replace("44137    42.260  -62.030 2026 10 30 04 00", "44137    42.260  -62.030 2026 10 30 03 00")
    assert older != text
    p, by, ok, err = ndbc_list(http=ndbc_http(latest=older))
    assert p.latest("44137")["time_utc"] == "2026-10-30T03:00:00Z"
    assert p.detail("44137")["latest"]["time_utc"] == "2026-10-30T04:00:00Z"


def test_parse_ndbc_latest_waves_drops_impossible_positions():
    text = fixture("ndbc_latest_obs.txt")
    bad = text.replace("41110    34.144", "41110    94.144")
    assert bad != text and "41110" in B.parse_ndbc_latest_waves(text)
    assert "41110" not in B.parse_ndbc_latest_waves(bad)


def test_glos_parameter_list_read_once_a_day():
    http = glos_http()
    p, by, ok, err = glos(http=http)
    p._build_station_list()
    assert sum("api/v1/parameters" in u for u in http.calls) == 1


# ------------------------------------------------------------------ section 63 test-site finding V3-1
# A marker whose wave sensor is silent opened an empty window (GLOS 45005 and ECCC's 451xx; IOOS 44009, 62305), and
# IOOS's stated time range could lag its rows by a day (gov-ndbc-46004): the window query was refused.

def test_glos_platform_without_a_recent_wave_height_left_out_and_verdicts_kept():
    def probe(url):
        return NO_MATCH if "obs_3.csv" in url else GLOS_PROBE_YES
    http = glos_http(probe=probe)
    p, by, ok, err = glos(http=http)
    assert ok and "glos:45029" not in by and {"glos:45013", "glos:SPOT-1360"} <= set(by)
    assert p.detail("45029") == {"latest": None, "recent": []}           # nothing to open
    asked = [u for u in http.calls if "tabledap/obs_" in u]
    assert sorted(asked) == sorted(B.GlosProvider.PROBE_URL % ds for ds in (2, 3, 139))   # the silent 260 is not asked
    n = len(http.calls)
    p._build_station_list()                                              # within PROBE_TTL: nothing asked again
    assert len([u for u in http.calls[n:] if "tabledap/obs_" in u]) == 0
    http.routes[0] = ("tabledap/obs_", Resp("down", 503))                # past the TTL the mirror fails: verdicts kept
    p.clock = staticmethod(lambda: CAPTURED + B.GlosProvider.PROBE_TTL + 1)
    n = len(http.calls)
    lst, ok, err = p._build_station_list()
    assert len([u for u in http.calls[n:] if "tabledap/obs_" in u]) == 3
    ids = {s["id"] for s in lst}
    assert "glos:45029" not in ids and "glos:45013" in ids


def test_glos_unknown_probe_keeps_the_platform_and_ndbc_covered_ones_are_not_asked():
    p, by, ok, err = glos(http=glos_http(probe=Resp("down", 500)))
    assert {"glos:45013", "glos:45029", "glos:SPOT-1360"} <= set(by)      # never asked successfully: listed
    http = glos_http(probe=NO_MATCH)
    p = B.GlosProvider(http=http, covered_ids=lambda: {"45029", "45013"})
    p.clock = staticmethod(lambda: CAPTURED)
    lst, ok, err = p._build_station_list()
    ids = {s["id"] for s in lst}
    assert ids == {"glos:45013", "glos:45029"}                            # NDBC lists them: not asked, kept
    assert all("obs_2.csv" not in u and "obs_3.csv" not in u for u in http.calls)


def test_ioos_station_without_a_recent_wave_height_left_out_and_its_time_from_the_probe():
    def probe(url):
        return NO_MATCH if "gov-ndbc-45135" in url else IOOS_PROBE_YES
    http = ioos_http(probe=probe)
    p, by, ok, err = ioos(http=http)
    assert ok and "ioos:45135" not in by and "ioos:41110" in by
    assert by["ioos:41110"]["latest_time"] == "2026-10-30T03:00:00Z"     # the probe's newest, not maxTime 2026-10-29T22:00
    p, by, ok, err = ioos(http=ioos_http(probe=Resp("x", 502)))
    assert by["ioos:41110"]["latest_time"] == "2026-10-29T22:00:00Z"     # unknown: kept with maxTime
    http = ioos_http(probe=NO_MATCH)
    p, by, ok, err = ioos(http=http, covered={"45135", "41110"})
    assert {"ioos:45135", "ioos:41110"} <= set(by) and "ioos:SIPF1" not in by
    assert all("gov-ndbc-45135" not in u for u in http.calls)


def test_ioos_window_never_starts_after_the_datasets_stated_end():
    http = ioos_http()
    p, by, ok, err = ioos(http=http)
    p._ds["41065"]["maxtime"] = "2026-10-29T00:00:00Z"                    # stated end a day behind (gov-ndbc-46004)
    p.detail("41065")
    url = [u for u in http.calls if "cap2wave-capers-nearshore-wave.csv" in u and "orderByMax" not in u][-1]
    assert "&time%3E=2026-10-28T23:00:00Z&" in url
    p._cache = B._DetailCache(300)
    p._ds["41065"]["maxtime"] = "2026-10-30T04:00:00Z"
    p.detail("41065")
    url = [u for u in http.calls if "cap2wave-capers-nearshore-wave.csv" in u and "orderByMax" not in u][-1]
    assert "&time%3E=2026-10-29T04:25:00Z&" in url                        # 24 hours back (the clock is pinned)


def test_erddap_wave_newest_rules():
    def f(body):
        return B._erddap_wave_newest(FakeHttp([("x", body)]), "http://x", 5)
    assert f(IOOS_PROBE_YES) == ("yes", "2026-10-30T03:00:00Z")
    assert f(NO_MATCH) == ("no", None)
    assert f("time,sea_surface_wave_significant_height\nUTC,m\n2026-10-30T03:00:00Z,NaN\n") == ("no", None)
    assert f(Resp("x", 500)) == ("unknown", None) and f("not,a,csv\n1,2,3\n") == ("unknown", None)
    two = "time,sea_surface_wave_significant_height\nUTC,m\n2026-10-30T01:00:00Z,1\n2026-10-30T03:00:00Z,NaN\n"
    assert f(two) == ("yes", "2026-10-30T01:00:00Z")


def test_ioos_verdict_kept_when_a_later_probe_fails():
    def probe(url):
        return NO_MATCH if "gov-ndbc-45135" in url else IOOS_PROBE_YES
    http = ioos_http(probe=probe)
    p, by, ok, err = ioos(http=http)
    assert "ioos:45135" not in by
    http.routes[-1] = ("orderByMax", Resp("x", 503))
    lst, ok, err = p._build_station_list()
    assert ok and "ioos:45135" not in {s["id"] for s in lst} and "ioos:41110" in {s["id"] for s in lst}


def test_json_array_items_one_at_a_time():
    items = [{"a": 1}, {"b": [1, 2]}, 3, "x"]
    text = "  " + json.dumps(items, indent=2) + "  "
    assert list(B._json_array_items(text)) == items
    assert list(B._json_array_items("[]")) == []
    for bad in ('{"a": 1}', "", "[1, 2", "[1, }", "x]"):
        with pytest.raises(ValueError):
            list(B._json_array_items(bad))


def test_glos_parameter_map_keeps_only_the_names_the_window_uses():
    p, by, ok, err = glos()
    names = {v[0] for v in p._params.values()}
    used = {n for _key, ns in B.GlosProvider.FIELDS for n in ns}
    assert names and names <= used
    assert "air_temperature" not in names and "sea_water_temperature" in names
