"""Plan section 67: the nine new wind feeds (wind_more.py) on real answers captured 2026-10-30 (tests/fixtures/wind/more),
their providers with a fake fetch, and the history dispatch in wind_sources.WindHistory."""
import json
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import buoy_sources  # noqa: E402
import wind_more as M  # noqa: E402
import wind_sources as W  # noqa: E402

FX = os.path.join(ROOT, "tests", "fixtures", "wind", "more")


def fx(name, mode="r"):
    with open(os.path.join(FX, name), mode, **({} if "b" in mode else {"encoding": "utf-8"})) as f:
        return f.read()


# ---------------------------------------------------------------------------------------------- parsers

def test_swob_auto_reading_units_and_suppressed_direction():
    sid, lat, lon, r = M.parse_swob(fx("swob_cweb_auto.xml"))
    assert (lat, lon) == (49.3832, -126.5447)
    assert r["t"] == 1793390400                                    # 2026-10-30T20:00Z
    assert r["s"] == round(3.4 / 3.6, 1)                           # km/h -> m/s
    assert r["d"] is None                                          # its qa_summary is -10 (suppressed)
    assert r["g"] is not None and r["at"] == 9.4 and r["rh"] == 88


def test_swob_manual_report_and_qa_rule():
    _sid, _la, _lo, r = M.parse_swob(fx("swob_cwae_man.xml"))
    assert r["s"] == 0.0 and r["d"] is None                        # calm
    el = M.swob_elements('<element name="avg_wnd_spd_10m_pst10mts" uom="km/h" value="36"><qualifier name="qa_summary" '
                         'uom="unitless" value="10"/></element><element name="avg_wnd_spd_10m_pst2mts" uom="km/h" '
                         'value="18"><qualifier name="qa_summary" uom="unitless" value="100"/></element>')
    assert M._swob_value(el, ("avg_wnd_spd_10m_pst10mts", "avg_wnd_spd_10m_pst2mts"), "speed") == 5.0   # doubtful skipped
    el = M.swob_elements('<element name="x" uom="knots" value="10"/>')
    assert M._swob_value(el, ("x",), "speed") == pytest.approx(5.14444)
    el = M.swob_elements('<element name="x" uom="km/h" value="MSNG"/>')
    assert M._swob_value(el, ("x",), "speed") is None


def test_swob_listing_and_pick():
    L = M.parse_swob_listing(fx("swob_latest.html"))
    assert "CWEB" in L and L["CWEB"]["AUTO"][0] == "CWEB-AUTO-swob.xml"
    assert not any("minute" in f for e in L.values() for f, _ in e.values())
    assert M.swob_pick({"MAN": ("a", "t1")}) == ("a", "t1")
    assert M.swob_pick({"AUTO": ("b", "t2"), "MAN": ("a", "t1")}) == ("b", "t2")
    st = M.parse_eccc_station_list(fx("swob_station_list.csv"))
    assert st["CAAB"]["lat"] == 47.85773 and st["CAAB"]["provider"] == "MSC"


def test_jma_map_directions_calm_and_quality():
    t = M.jma_time(fx("jma_time.txt"))
    obs = M.parse_jma_map(fx("jma_map.json"), t)
    assert obs["11001"]["s"] == 4.0 and obs["11001"]["d"] == 202   # code 9 = SSW (9 x 22.5)
    assert obs["11176"]["d"] is None                               # direction 0: calm
    assert all(r["g"] is None for r in obs.values())               # the map has no gust
    assert M.jma_direction(16) == 0 and M.jma_direction(1) == 22.5 and M.jma_direction(17) is None
    bad = M.parse_jma_map({"1": {"wind": [5.0, 5], "windDirection": [3, 0]}}, t)
    assert bad == {}                                               # a value of quality 5 is left out
    assert M.jma_map_name(t) == re.sub(r"\D", "", fx("jma_time.txt").strip()[:16]) + "00"


def test_jma_point_file_ascending_without_the_days_highest_gust():
    rows = M.parse_jma_point(fx("jma_point_44132.json"))
    assert len(rows) >= 6 and [r["t"] for r in rows] == sorted(r["t"] for r in rows)
    doc = json.loads(fx("jma_point_44132.json"))
    assert all("gustTime" in e for e in doc.values())                 # the file's "gust" is the day's highest so far
    assert all(r["g"] is None for r in rows) and rows[0]["s"] == 2.2
    table = M.parse_jma_table(fx("jma_table.json"))
    assert table["11001"] == {"name": "Cape Soya", "lat": 45.52, "lon": 141.935}


def test_iem_currents_and_history_knots():
    cur = M.parse_iem_currents(fx("iem_currents_ca_rwis.json"))
    r, e = cur["CD004"]
    assert r["s"] == round(5.6371493 * M.KT, 1) and r["g"] == round(10.691145 * M.KT, 1) and r["d"] == 238
    assert e["network"] == "CA_RWIS"
    rows = M.parse_iem_history(fx("iem_obhistory.json"))
    assert rows and [x["t"] for x in rows] == sorted(x["t"] for x in rows)


def test_smhi_three_parameters_join():
    s, d, g = (M.parse_smhi_param(fx(f"smhi_{n}.json")) for n in (4, 3, 21))
    joined = M.smhi_readings(s, d, g)
    assert joined and all(r["s"] is not None for rs in joined.values() for r in rs)
    k = next(k for k in joined if k in d and k in g)
    r = joined[k][-1]
    assert r["d"] == W._direction(d[k][-1][1]) or r["s"] == 0
    day = M.parse_smhi_param(fx("smhi_day_71420.json"))
    assert list(day) == ["71420"] and len(day["71420"]) > 10
    assert M.parse_smhi_param({"station": [{"key": "1", "value": [{"date": 1000, "value": "3", "quality": "R"}]}]}) == {}
    st = M.parse_smhi_stations(fx("smhi_stations.json"))
    assert st["154860"]["lat"] == 65.53


def test_fmi_positions_match_stations():
    obs = M.parse_fmi_obs(fx("fmi_obs.xml"))
    assert len(obs) > 100
    r = obs["100683"][0]
    assert r == {"t": 1793392200, "s": 2.4, "g": 4.0, "d": 342}
    st = M.parse_fmi_stations(fx("fmi_stations.xml"))
    assert st["100683"]["name"] == "Porvoo Kilpilahti satama"


def test_dmi_join_and_stations():
    s, d, g = (M.parse_dmi_items(fx(n)) for n in ("dmi_ws.json", "dmi_wd.json", "dmi_wm.json"))
    joined = M.dmi_readings(s, d, g)
    assert "04231" in joined and joined["04231"][-1]["s"] == 2.8
    st = M.parse_dmi_stations(fx("dmi_stations.json"))
    assert all(v["lat"] for v in st.values())


def test_dwd_poi_and_catalogue():
    rows = M.parse_dwd_poi(fx("dwd_10637.csv", "rb"))
    assert len(rows) >= 20 and rows[-1]["t"] > rows[0]["t"]
    assert rows[-1]["at"] == 8.3 and rows[-1]["s"] == round(9.0 / 3.6, 1)
    cat = M.parse_dwd_catalog(fx("dwd_catalog.cfg"))
    assert cat["10637"] == {"name": "FRANKFURT/M", "lat": 50.05, "lon": 8.6, "icao": "EDDF"}   # deg.min
    assert cat["01025"]["icao"] is None
    assert M._degmin("-8.40") == pytest.approx(-8.6667, abs=1e-3)


def test_ipma_obs_history_and_directions():
    obs = M.parse_ipma_obs(fx("ipma_obs.geojson"))
    r = obs["1200501"][0]
    assert r["s"] == 2.2 and r["d"] == 315                          # code 8 = NW
    hist = M.parse_ipma_history(fx("ipma_observations.json"))
    assert len(hist["1210881"]) >= 20
    assert M.parse_ipma_obs({"features": [{"properties": {"idEstacao": 1, "time": "2026-10-30T18:00:00",
                                                         "intensidadeVento": -99.0, "idDireccVento": 0}}]}) == {}


def test_mei_today_local_time_and_kmh():
    rows = M.parse_mei_today(fx("mei_valentia_today.json"))
    assert rows[-1]["t"] == 1793390400                              # 20:00 Europe/Dublin (UTC+0 in late October)
    assert rows[-1]["s"] == round(13 / 3.6, 1) and rows[-1]["d"] == 180 and rows[-1]["g"] is None
    assert M.mei_slug("Roche's Point") == "roches-point" and M.mei_slug("Malin Head") == "malin-head"


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


def _st(ids, **extra):
    return {i: dict({"id": i, "name": i, "lat": 0.0, "lon": 0.0}, **extra) for i in ids}


@pytest.fixture
def clock(monkeypatch):
    t = {"now": 1793394000.0}                                       # 2026-10-30T21:00Z
    monkeypatch.setattr(buoy_sources.time, "time", lambda: t["now"])
    return t


def test_jma_provider_lists_snapshot_stations_only(clock):
    f = Fetch({"latest_time.txt": fx("jma_time.txt"), "data/map/": fx("jma_map.json")})
    p = M.JmaProvider(_st(["11001", "44132"]), f)
    lst = p._fetch_stations()
    assert [e["local_id"] for e in lst] == ["11001", "44132"]
    assert lst[0]["wind"] == [4.0, None, 202]
    assert any("/data/map/20261031055000.json" in u for u in f.asked)


def test_one_file_feed_keeps_readings_and_raises_on_failure(clock):
    f = Fetch({"latest_time.txt": fx("jma_time.txt"), "data/map/": fx("jma_map.json")})
    p = M.JmaProvider(_st(["11001"]), f)
    assert p._fetch_stations()
    f.table["data/map/"] = IOError("HTTP 500")
    with pytest.raises(IOError):
        p._fetch_stations()                                          # the base class keeps the last list
    f.table["data/map/"] = json.dumps({})
    assert p._fetch_stations()[0]["local_id"] == "11001"             # a station missing from one answer keeps its reading
    clock["now"] += M.KEEP_S + 3600
    assert p._fetch_stations() == []                                 # ... until it is KEEP_S old


def test_ipma_series_and_hist(clock):
    f = Fetch({"observations.json": fx("ipma_observations.json")})
    p = M.IpmaProvider(_st(["1210881"]), f)
    p._fetch_stations()
    rows = p.hist("1210881", clock["now"] - 86400)
    assert len(rows) >= 20 and rows[-1]["at"] is not None
    assert p.hist("nope", 0) == []
    assert M.JmaProvider(_st(["1"]), f).hist("1", 0) is None         # a feed without a series


def test_paced_feed_round_robin_failures_and_pause(clock):
    calls = []

    class P(M.PacedMore):
        source, key, per_refresh = "DWD", "dwd", 2

        def one(self, u):
            calls.append(u)
            if u == "C":
                raise IOError("HTTP 500")
            return {u: [W.reading(clock["now"] - 60, 3.0, None, 90)]}

    p = P(_st(["A", "B", "C", "D"]), Fetch({}))
    assert [e["local_id"] for e in p._fetch_stations()] == ["A", "B"]
    assert [e["local_id"] for e in p._fetch_stations()] == ["A", "B", "D"]      # C failed, D answered
    assert calls == ["A", "B", "C", "D"]

    class R(P):
        def one(self, u):
            raise IOError("HTTP 429")
    r = R(_st(["A", "B"]), Fetch({}))
    with pytest.raises(RuntimeError, match="paused"):
        r._fetch_stations()
    with pytest.raises(RuntimeError, match="paused after a refusal"):
        r._fetch_stations()

    class F(P):
        def one(self, u):
            raise IOError("HTTP 500")
    with pytest.raises(RuntimeError, match="every request failed"):
        F(_st(["A", "B"]), Fetch({}))._fetch_stations()


def test_iem_provider_asks_per_network(clock):
    f = Fetch({"network=CA_RWIS": fx("iem_currents_ca_rwis.json")})
    p = M.IemProvider(_st(["CD004"], net="CA_RWIS"), f)
    lst = p._fetch_stations()
    assert [e["local_id"] for e in lst] == ["CD004"] and p.units() == ["CA_RWIS"]


def test_eccc_provider_reads_only_changed_files(clock):
    f = Fetch({"CWEB-AUTO-swob.xml": fx("swob_cweb_auto.xml"), "swob-ml/latest/": fx("swob_latest.html")})
    p = M.EcccProvider(_st(["CWEB"]), f)
    assert p._fetch_stations()[0]["local_id"] == "CWEB"
    n = len(f.asked)
    p._fetch_stations()
    assert len(f.asked) == n + 1                                    # the listing only: the file did not change
    assert len(p.hist("CWEB", 0)) == 1


def test_make_providers_switches():
    groups = {"jma": _st(["1"]), "eccc": _st(["X"]), "dwd": {}}
    ps = M.make_providers(groups, Fetch({}), enabled={"eccc": False})
    assert [p.source for p in ps] == ["JMA"]
    assert all(p.attribution_text.startswith("Source: ") for p in M.make_providers(groups, Fetch({})))


# ---------------------------------------------------------------------------------------------- the service

def test_id_rule_and_attribution():
    for sid in ("eccc:CWEB", "jma:44132", "iem:CD004", "smhi:71420", "fmi:100683", "dmi:06041", "dwd:10637",
                "ipma:1210881", "mei:MALIN-HEAD", "metar:PHNL", "nws:016HE"):
        assert W.valid_id(sid), sid
    for bad in ("eccc:", "jma:a b", "foo:123", "iem:../x", "mei:" + "X" * 17):
        assert not W.valid_id(bad), bad
    assert set(W.MORE_SOURCES) == set(M.SOURCES) <= set(W.ATTRIBUTION)


def _svc(stations, fetch, now=1793394000.0):
    return W.WindHistory(stations, fetch, now=lambda: now)


def test_history_dispatch_per_source(clock):
    st = {"jma:44132": {"id": "jma:44132", "name": "Tokyo", "kind": "met", "src": "jma", "tz": "Asia/Tokyo"},
          "dwd:10637": {"id": "dwd:10637", "name": "Frankfurt", "kind": "met", "src": "dwd", "tz": "Europe/Berlin"},
          "ipma:1210881": {"id": "ipma:1210881", "name": "Olhao", "kind": "met", "src": "ipma", "tz": "Europe/Lisbon"},
          "eccc:CWEB": {"id": "eccc:CWEB", "name": "Estevan Point", "kind": "met", "src": "eccc", "tz": "America/Vancouver"}}
    f = Fetch({"point/44132/": fx("jma_point_44132.json"), "10637-BEOB": fx("dwd_10637.csv", "rb")})
    svc = _svc(st, f, now=1793394000.0 + 3 * 3600)
    status, out = svc.history("jma:44132")
    assert status == "ok" and out["source"] == W.ATTRIBUTION["jma"] and len(out["t"]) >= 6 and not any(out.get("g") or [])
    status, out = svc.history("dwd:10637")
    assert status == "ok" and len(out["t"]) >= 20 and out["at"]
    status, out = svc.history("ipma:1210881")                        # no series hooked: no history, never an error
    assert status == "ok" and out["t"] == [] and out["note"] == W.NO_HISTORY
    ip = M.IpmaProvider({"1210881": st["ipma:1210881"]}, Fetch({"observations.json": fx("ipma_observations.json")}))
    ip._fetch_stations()
    svc.more_hist["ipma"] = ip.hist
    svc._cache.clear()
    status, out = svc.history("ipma:1210881")
    assert status == "ok" and len(out["t"]) >= 20 and out["note"] is None
    ec = M.EcccProvider({"CWEB": st["eccc:CWEB"]}, Fetch({"CWEB-AUTO": fx("swob_cweb_auto.xml"),
                                                           "swob-ml/latest/": fx("swob_latest.html")}))
    ec._fetch_stations()
    svc.more_hist["eccc"], svc.more_started["eccc"] = ec.hist, 1793394000.0
    status, out = svc.history("eccc:CWEB")
    assert out["t"] == [1793390400] and out["note"] == W.MORE_RESTART_NOTE


def test_history_budget_is_busy_never_cached(clock):
    st = {"dwd:10637": {"id": "dwd:10637", "name": "F", "kind": "met", "src": "dwd", "tz": "UTC"}}
    svc = _svc(st, Fetch({"10637-BEOB": fx("dwd_10637.csv", "rb")}))
    svc.more_buckets["dwd"] = W.TokenBucket(1)
    svc.more_buckets["dwd"].take()
    status, _ = svc.history("dwd:10637")
    assert status == "busy"
    assert svc._get(("dwd:10637",)) is None


def test_history_agency_failure_is_an_error(clock):
    st = {"jma:44132": {"id": "jma:44132", "name": "T", "kind": "met", "src": "jma", "tz": "Asia/Tokyo"}}
    status, out = _svc(st, Fetch({"point/": IOError("HTTP 503")})).history("jma:44132")
    assert status == "error"


def test_unknown_source_has_no_history_branch():
    src = open(os.path.join(ROOT, "wind_sources.py"), encoding="utf-8").read()
    body = src[src.index("    def _build(self, sid, st):"):src.index("def conditions(rows):")]
    assert 'elif src == "coops":' in body and re.search(r"\n        else:\s+# a source with no history", body)


# ------------------------------------------------------------------ mutation pins (section 67, step 2)

def test_ipma_every_direction_class():
    feats = [{"properties": {"idEstacao": i, "time": "2026-10-30T18:00:00", "intensidadeVento": 3.0, "idDireccVento": i}}
             for i in range(1, 9)]
    obs = M.parse_ipma_obs({"features": feats})
    assert [obs[str(i)][0]["d"] for i in range(1, 9)] == [0, 45, 90, 135, 180, 225, 270, 315]   # 1 = N .. 8 = NW


def test_a_reading_far_in_the_future_is_not_believed(clock):
    now = clock["now"]

    class P(M.MoreProvider):
        source, key = "JMA", "jma"

        def _readings(self):
            return {"AHEAD": [W.reading(now + 3600, 5.0, None, 90)], "SOON": [W.reading(now + 300, 4.0, None, 90)]}
    assert [e["local_id"] for e in P(_st(["AHEAD", "SOON"]), Fetch({}))._fetch_stations()] == ["SOON"]


def test_eccc_reads_at_most_per_refresh_files_oldest_first(clock):
    f = Fetch({"CAAB-AUTO": fx("swob_cweb_auto.xml"), "CWEB-AUTO": fx("swob_cweb_auto.xml"),
               "CWVF-AUTO": fx("swob_cweb_auto.xml"), "swob-ml/latest/": fx("swob_latest.html")})
    p = M.EcccProvider(_st(["CAAB", "CWEB", "CWVF"]), f, per_refresh=2)
    assert sorted(e["local_id"] for e in p._fetch_stations()) == ["CAAB", "CWVF"]    # listed 20:01 and 20:00
    assert sorted(u.rsplit("/", 1)[1] for u in f.asked[1:]) == ["CAAB-AUTO-swob.xml", "CWVF-AUTO-swob.xml"]  # 2 workers
    n = len(f.asked)
    assert sorted(e["local_id"] for e in p._fetch_stations()) == ["CAAB", "CWEB", "CWVF"]
    assert [u.rsplit("/", 1)[1] for u in f.asked[n + 1:]] == ["CWEB-AUTO-swob.xml"]  # the rest the next refresh
