"""Wind stations (plan section 39), step 2: the feeds' parsers, the three providers, the merged table, the history
service, the routes, the scheduler hook and /healthz. No test reaches the network: every fetch is a fake over the
captured fixtures (tests/fixtures/wind, tests/capture_wind_fixtures.py)."""
import calendar
import gzip
import json
import os
import sys
import threading
import time as _time
import zlib

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import buoy_sources as B  # noqa: E402
import fake_buoy_providers as F  # noqa: E402
import wind_sources as W  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "wind")


def fx(name, mode="rb"):
    with open(os.path.join(FIX, name), mode, **({} if mode == "rb" else {"encoding": "utf-8"})) as f:
        return f.read()


def first_t(text):
    return W.parse_realtime2(text, 0, 1)[0]["t"]


STATIONS = {
    "coops:1612340": {"id": "coops:1612340", "name": "Honolulu", "lat": 21.30333, "lon": -157.86453, "kind": "gauge",
                      "src": "coops", "tz": "Pacific/Honolulu", "alias": "OOUH1"},
    "coops:1611400": {"id": "coops:1611400", "name": "Nawiliwili", "lat": 21.9544, "lon": -159.3561, "kind": "gauge",
                      "src": "coops", "tz": "Pacific/Honolulu", "alias": "NWWH1"},
    "coops:1612401": {"id": "coops:1612401", "name": "Pearl Harbor", "lat": 21.3675, "lon": -157.9639, "kind": "gauge",
                      "src": "coops", "tz": "Pacific/Honolulu", "alias": None},
    "ndbc:51003": {"id": "ndbc:51003", "name": "Western Hawaii", "lat": 19.151, "lon": -160.617, "kind": "buoy",
                   "src": "ndbc", "tz": "Pacific/Honolulu", "alias": None},
    "ndbc:HRRH1": {"id": "ndbc:HRRH1", "name": "Lono Circle", "lat": 21.43, "lon": -157.8, "kind": "station",
                   "src": "ndbc", "tz": "Pacific/Honolulu", "alias": None},
    "metar:PHNL": {"id": "metar:PHNL", "name": "Honolulu Intl", "lat": 21.315, "lon": -157.924, "kind": "airport",
                   "src": "metar", "tz": "Pacific/Honolulu", "alias": None},
    "metar:KSFO": {"id": "metar:KSFO", "name": "San Francisco Intl", "lat": 37.62, "lon": -122.37, "kind": "airport",
                   "src": "metar", "tz": "America/Los_Angeles", "alias": None},
    "metar:KGMU": {"id": "metar:KGMU", "name": "Greenville", "lat": 34.85, "lon": -82.35, "kind": "airport",
                   "src": "metar", "tz": "America/New_York", "alias": None},
}


class FakeFetch:
    """fetch(url, max_bytes, headers) over a table of url -> bytes (or an Exception to raise, or a callable)."""

    def __init__(self, table=None):
        self.table = dict(table or {})
        self.calls = []

    def __call__(self, url, max_bytes, headers=None):
        self.calls.append((url, max_bytes, dict(headers or {})))
        for key, val in self.table.items():
            if url.startswith(key) if key.endswith("?") or key.endswith("=") else url == key:
                if isinstance(val, Exception):
                    raise val
                return val(url, max_bytes, headers) if callable(val) else val
        raise IOError("no fixture for " + url)

    def count(self, part):
        return sum(1 for u, _, _ in self.calls if part in u)


def coops_table():
    """CO-OPS answers per gauge: Honolulu a reading, Nawiliwili NOAA's error, Pearl Harbor unreachable."""
    return {W.coops_url("1612340", date="latest"): fx("coops_latest.json"),
            W.coops_url("1611400", date="latest"): fx("coops_error.json"),
            W.coops_url("1612401", date="latest"): IOError("timeout")}


def make_fakes(now_epoch):
    """The three providers over STATIONS, their fetches answered by the fixtures (the NDBC file, the METAR cache as
    plain CSV, CO-OPS per gauge), on a frozen clock."""
    fetch = FakeFetch({W.NDBC_LATEST_URL: fx("latest_obs.txt"), W.METAR_CACHE_URL: fx("metars.csv")})
    fetch.table.update(coops_table())
    provs = W.make_providers(STATIONS, fetch, coops_workers=2)
    return provs, fetch


@pytest.fixture
def clock(monkeypatch):
    newest = max(r["t"] for r in W.parse_latest_obs(fx("latest_obs.txt", "r")).values())
    t = F.FrozenTime(newest + 600)                                     # ten minutes after the newest NDBC row
    monkeypatch.setattr(B, "time", t)
    return t


# ------------------------------------------------------------------ readings and parsers

def test_reading_checks_its_fields():
    assert W.reading(1791651240, "1.0", "3.5", "74.0") == {"t": 1791651240, "s": 1.0, "g": 3.5, "d": 74}
    assert W.reading(1791651240, 2.26, None, 359.6) == {"t": 1791651240, "s": 2.3, "g": None, "d": 0}
    assert W.reading("x", 1, 1, 1) is None and W.reading(1, "MM", 1, 1) is None and W.reading(1, -1, 1, 1) is None
    assert W.reading(1, 200, 1, 1) is None                                  # above MAX_SPEED_MS
    assert W.reading(1, 1, "MM", "MM") == {"t": 1, "s": 1.0, "g": None, "d": None}
    assert W.reading(1, 1, 1, 361)["d"] is None and W.reading(1, 1, 1, -1)["d"] is None
    assert W.reading(1, float("nan"), 1, 1) is None and W.reading(1, 1, 1, float("inf"))["d"] is None


LATEST = """#STN       LAT      LON  YYYY MM DD hh mm WDIR WSPD   GST WVHT
#text      deg      deg   yr mo day hr mn degT  m/s   m/s   m
51003    19.151 -160.617 2026 10 10 01 10 120   4.0   5.0   MM
51201    21.671 -158.118 2026 10 10 00 56  MM    MM    MM  1.8
OOUH1    21.303 -157.865 2026 10 10 00 54 190   3.6    MM   MM
BADT1    21.303 -157.865 2026 13 10 00 54 190   3.6   6.2   MM
short 1 2
"""


def test_parse_latest_obs_rows_with_a_speed():
    got = W.parse_latest_obs(LATEST)
    assert got == {"51003": {"t": calendar.timegm((2026, 10, 10, 1, 10, 0)), "s": 4.0, "g": 5.0, "d": 120},
                   "OOUH1": {"t": calendar.timegm((2026, 10, 10, 0, 54, 0)), "s": 3.6, "g": None, "d": 190}}


def test_parse_latest_obs_fixture():
    got = W.parse_latest_obs(fx("latest_obs.txt", "r"))
    assert {"51003", "OOUH1", "ILOH1"} <= set(got) and "51201" not in got    # 51201 reports waves, no wind
    for r in got.values():
        assert isinstance(r["t"], int) and r["s"] >= 0 and (r["g"] is None or r["g"] >= 0)
        assert r["d"] is None or 0 <= r["d"] < 360


RT2 = """#YY  MM DD hh mm WDIR WSPD GST  WVHT
#yr  mo dy hr mn degT m/s  m/s     m
2026 10 10 16 18  50  1.5  3.6    MM
2026 10 10 16 12  MM   MM   MM    MM
2026 10 10 16 06  60  2.0  4.0    MM
2026 10 09 16 06  70  2.5  4.5    MM
2026 10 09 15 00  80  3.0  5.0    MM
"""


def test_parse_realtime2_stops_at_since_and_caps():
    t0 = calendar.timegm((2026, 10, 10, 16, 18, 0))
    rows = W.parse_realtime2(RT2, t0 - 86400)
    assert [r["t"] for r in rows] == [t0, t0 - 720]                        # 16:18, 16:06; the MM row skipped
    rows = W.parse_realtime2(RT2, t0 - 86400 - 720)                         # a row exactly at since is kept
    assert [r["t"] for r in rows] == [t0, t0 - 720, t0 - 86400 - 720] and rows[2]["d"] == 70
    assert len(W.parse_realtime2(RT2, 0, 2)) == 2
    assert W.parse_realtime2(RT2, t0 + 1) == []


def test_parse_realtime2_fixture_is_24_hours_newest_first():
    text = fx("realtime2_OOUH1.txt", "r")
    t0 = first_t(text)
    rows = W.parse_realtime2(text, t0 - W.HISTORY_S)
    assert 150 <= len(rows) <= W.HISTORY_ROWS                               # 6-minute rows: ~240 in a day
    assert all(a["t"] > b["t"] for a, b in zip(rows, rows[1:])) and rows[-1]["t"] >= t0 - W.HISTORY_S


def test_parse_coops_wind_and_error():
    latest = W.parse_coops_wind(json.loads(fx("coops_latest.json")))
    assert len(latest) == 1 and latest[0]["s"] >= 0 and 0 <= latest[0]["d"] < 360
    day = W.parse_coops_wind(json.loads(fx("coops_24.json")))
    assert 200 <= len(day) <= 241 and all(a["t"] < b["t"] for a, b in zip(day, day[1:]))
    assert W.coops_error(json.loads(fx("coops_error.json"))).startswith("No data")
    assert W.coops_error(json.loads(fx("coops_latest.json"))) is None
    assert W.parse_coops_wind({"data": [{"t": "bad", "s": "1"}, "x", {"t": "2026-10-10 16:48", "s": "x"}]}) == []
    assert W.coops_url("1612340", date="latest").startswith(W.COOPS_API + "?product=wind&station=1612340&")
    assert "application=allshoresurf.com" in W.coops_url("1612340", range=24)


@pytest.mark.parametrize("dir_v,spd,gst,want", [
    (150, 11, "", (5.7, None, 150)),
    (360, 10, 15, (5.1, 7.7, 0)),              # north is written 360
    (0, 5, 11, (2.6, 5.7, None)),               # 0 with a speed: variable
    ("VRB", 5, None, (2.6, None, None)),        # the API's spelling
    (0, 0, "", (0.0, None, None)),              # calm
    (90, 0, "", (0.0, None, None)),             # calm with a direction given: none
    ("", 7, "", (3.6, None, None)),
    (90, "", "", None),                         # no speed: no reading
])
def test_metar_reading_rules(dir_v, spd, gst, want):
    r = W._metar_reading(1791651180, dir_v, spd, gst)
    assert (r is None and want is None) or (r["s"], r["g"], r["d"]) == want


METAR_CSV = ("raw_text,station_id,observation_time,latitude,longitude,wind_dir_degrees,wind_speed_kt,wind_gust_kt\n"
             '"METAR PHNL 101653Z 05005KT",PHNL,2026-10-10T16:53:00.000Z,21.3,-157.9,50,5,\n'
             '"METAR PHNL 101553Z 04004KT",PHNL,2026-10-10T15:53:00.000Z,21.3,-157.9,40,4,\n'
             '"METAR K3L4 101635Z VRB05G11KT",K3L4,2026-10-10T16:35:00.000Z,40.1,-75.2,0,5,11\n'
             '"METAR YKSC 101703Z 00000KT",YKSC,2026-10-10T17:03:00.000Z,-30,140,0,0,\n'
             '"METAR KGMU 101700Z",KGMU,2026-10-10T17:00:00.000Z,34,-82,,,\n'
             '"METAR BAD",BAD,not-a-time,1,1,90,5,\n')


def test_parse_metar_cache_newest_per_station_plain_and_gzip():
    want = {"PHNL": {"t": 1791651180, "s": 2.6, "g": None, "d": 50},
            "K3L4": {"t": 1791650100, "s": 2.6, "g": 5.7, "d": None},
            "YKSC": {"t": 1791651780, "s": 0.0, "g": None, "d": None}}
    assert W.parse_metar_cache(METAR_CSV.encode()) == want
    assert W.parse_metar_cache(gzip.compress(METAR_CSV.encode())) == want
    with pytest.raises(ValueError):
        W.parse_metar_cache(b"a,b\n1,2\n")


def test_parse_metar_cache_fixture():
    got = W.parse_metar_cache(fx("metars.csv"))
    assert {"PHNL", "KSFO", "EGLL"} <= set(got) and "KGMU" not in got        # KGMU reported no wind
    assert got["KSFO"]["g"] is not None and got["KSFO"]["g"] >= got["KSFO"]["s"]
    assert got["YKSC"] == {"t": got["YKSC"]["t"], "s": 0.0, "g": None, "d": None}   # calm


def test_parse_metar_history_fixture_and_shapes():
    rows = W.parse_metar_history(json.loads(fx("metar_history.json")))
    assert 20 <= len(rows) <= 60 and all(isinstance(r["t"], int) for r in rows)
    assert W.parse_metar_history([{"obsTime": 1, "wdir": "VRB", "wspd": 3}, {"obsTime": 2}, "x", {"obsTime": 3, "wdir": 90, "wspd": 0}]) == [
        {"t": 1, "s": 1.5, "g": None, "d": None}, {"t": 3, "s": 0.0, "g": None, "d": None}]
    assert W.parse_metar_history({"not": "a list"}) == []


def test_gunzip_bounded_cuts_a_bomb():
    assert W.gunzip_bounded(gzip.compress(b"x" * 1000), 1000) == b"x" * 1000
    with pytest.raises(ValueError):
        W.gunzip_bounded(gzip.compress(b"x" * 1001), 1000)
    bomb = gzip.compress(b"\0" * (4 * 1024 * 1024))                         # 4 MB of zeros gzips to ~4 KB
    with pytest.raises(ValueError):
        W.gunzip_bounded(bomb, 1024 * 1024)


# ------------------------------------------------------------------ the snapshot

def test_snapshot_loads_and_ids_validate():
    stations, doc = W.load_stations(os.path.join(os.path.dirname(HERE), "wind_stations.json"))
    assert len(stations) > 2000 and all(W.valid_id(s) for s in stations)
    lst = W.client_list(doc)
    assert lst["fields"] == doc["fields"] and len(lst["stations"]) == len(stations)
    assert not W.valid_id("coops:1612340/x") and not W.valid_id("ndbc:oouh1") and not W.valid_id(5)
    assert set(W.by_source(stations)) == {"coops", "ndbc", "metar"}


# ------------------------------------------------------------------ providers

def test_make_providers_and_aliases():
    provs = W.make_providers(STATIONS, FakeFetch(), coops_workers=20)
    assert [p.source for p in provs] == ["NDBC", "METAR", "COOPS"]
    ndbc, metar, coops = provs
    assert set(ndbc.stations) == {"51003", "HRRH1"} and set(metar.stations) == {"PHNL", "KSFO", "KGMU"}
    assert set(coops.stations) == {"1612340", "1611400", "1612401"} and coops.workers == 8
    assert ndbc.aliases["OOUH1"]["id"] == "coops:1612340" and ndbc.aliases["NWWH1"]["id"] == "coops:1611400"
    assert all(p.extra_keys == ("wind", "alias_of") for p in provs)
    assert ndbc.list_ttl_sec == 300 and metar.list_ttl_sec == 300 and coops.list_ttl_sec == 600


def test_ndbc_provider_lists_its_stations_and_the_relays(clock):
    provs, fetch = make_fakes(clock.time())
    lst, version, _ = provs[0].list_stations_versioned()
    by = {e["id"]: e for e in lst}
    assert version == 1 and "ndbc:51003" in by and "ndbc:HRRH1" in by and "ndbc:51201" not in by and "ndbc:ILOH1" not in by
    e = by["ndbc:51003"]
    assert e["wind"][0] >= 0 and e["latest_time"].endswith("Z") and e["is_stale"] is False and e["lat"] == 19.151
    relay = by["ndbc:OOUH1"]
    assert relay["alias_of"] == "coops:1612340" and relay["lat"] == 21.30333 and relay["name"] == "Honolulu"
    assert "alias_of" not in e
    assert fetch.calls[0][2]["User-Agent"] == W.USER_AGENT and fetch.calls[0][1] == W.NDBC_LATEST_MAX
    assert provs[0].latest("51003") == {"time_utc": e["latest_time"], "wind": e["wind"]}
    assert provs[0].latest("51201") is None


def test_stale_readings_are_flagged_past_two_hours(clock):
    provs, _ = make_fakes(clock.time())
    clock.now += 3 * 3600
    lst = provs[0].list_stations_versioned()[0]
    assert all(e["is_stale"] for e in lst)                                   # every fixture row is now > 2 h old


def test_metar_provider_lists_the_snapshot_airports_with_wind(clock):
    provs, fetch = make_fakes(clock.time())
    lst = provs[1].list_stations_versioned()[0]
    ids = {e["id"] for e in lst}
    assert ids == {"metar:PHNL", "metar:KSFO"}                               # KGMU reported no wind; EGLL not in the snapshot
    assert fetch.calls[-1][2]["User-Agent"] == W.USER_AGENT


def test_coops_provider_one_failure_is_a_missing_reading_all_failures_fail(clock):
    provs, fetch = make_fakes(clock.time())
    coops = provs[2]
    lst, version, _ = coops.list_stations_versioned()
    assert [e["id"] for e in lst] == ["coops:1612340"] and version == 1    # 1611400 no data, 1612401 unreachable
    assert fetch.count("station=1612340") == 1 and fetch.count("station=1612401") == 1
    assert "application=allshoresurf.com" in fetch.calls[-1][0]
    # every request failing: the fetch fails and the last good list is kept
    fetch.table = {k: IOError("down") for k in fetch.table}
    clock.now += coops.list_ttl_sec + 1
    lst2, version2, _ = coops.list_stations_versioned()
    assert lst2 == lst and version2 == 1 and coops.status()["last_error"].startswith("RuntimeError: every CO-OPS")


def test_coops_provider_takes_the_newest_row_of_an_answer():
    two = json.dumps({"data": [{"t": "2026-10-10 16:42", "s": "2.0", "d": "80", "g": "3.0"},
                               {"t": "2026-10-10 16:48", "s": "1.0", "d": "74", "g": "3.5"}]}).encode()
    coops = W.CoopsWindProvider({"1612340": STATIONS["coops:1612340"]}, FakeFetch({W.coops_url("1612340", date="latest"): two}))
    assert coops._one("1612340") == ("1612340", {"t": 1791650880, "s": 1.0, "g": 3.5, "d": 74}, None)
    assert coops._fetch_stations()[0]["wind"] == [1.0, 3.5, 74]
    assert W.CoopsWindProvider({}, FakeFetch())._fetch_stations() == []


# ------------------------------------------------------------------ the merged table

def test_build_latest_rows_relays_and_missing():
    t = "2026-10-10T16:48:00Z"
    ndbc = [{"id": "ndbc:51003", "wind": [6.0, 8.0, 130], "latest_time": t},
            {"id": "ndbc:OOUH1", "wind": [3.6, 6.2, 190], "latest_time": t, "alias_of": "coops:1612340"},
            {"id": "ndbc:NWWH1", "wind": [2.0, 3.0, 90], "latest_time": "2026-10-10T16:00:00Z", "alias_of": "coops:1611400"},
            {"id": "ndbc:ZZZZZ", "wind": [1.0, None, None], "latest_time": t},              # not in the snapshot
            {"id": "ndbc:HRRH1", "wind": "bad", "latest_time": t},                          # malformed: skipped
            {"id": "ndbc:HRRH1", "wind": [0.0, None, None]}]                                # no time: skipped
    coops = [{"id": "coops:1612340", "wind": [1.0, 3.5, 74], "latest_time": t}]
    out = W.build_latest(STATIONS, {"ndbc": ndbc, "coops": coops, "metar": None}, 1791651300.5)
    assert out["fields"] == ["id", "t", "s", "g", "d"] and out["now"] == 1791651300 and out["stale_s"] == W.STALE_S
    rows = {r[0]: r for r in out["rows"]}
    assert [r[0] for r in out["rows"]] == sorted(rows)
    assert rows["coops:1612340"] == ["coops:1612340", 1791650880, 1.0, 3.5, 74]           # CO-OPS's own reading wins
    assert rows["coops:1611400"] == ["coops:1611400", 1791648000, 2.0, 3.0, 90]           # the relay fills the gap
    assert rows["ndbc:51003"][1:] == [1791650880, 6.0, 8.0, 130]
    assert "ndbc:ZZZZZ" not in rows and "ndbc:HRRH1" not in rows
    assert out["missing"] == ["coops:1612401", "ndbc:HRRH1"]                 # metar has no list yet: not missing


def test_build_latest_counts_a_listed_empty_source_as_missing():
    out = W.build_latest(STATIONS, {"coops": [], "ndbc": None, "metar": None}, 0)
    assert out["rows"] == [] and out["missing"] == sorted(s for s in STATIONS if s.startswith("coops:"))
    assert W.build_latest({}, {}, 0)["rows"] == []


# ------------------------------------------------------------------ the history service

class Clock:
    def __init__(self, t):
        self.t = float(t)

    def __call__(self):
        return self.t


def history_fetch():
    return FakeFetch({W.NDBC_RT2_URL % "51003": fx("realtime2_51003.txt"),
                      W.NDBC_RT2_URL % "OOUH1": fx("realtime2_OOUH1.txt"),
                      W.coops_url("1612340", range=24): fx("coops_24.json"),
                      W.coops_url("1611400", range=24): fx("coops_error.json"),
                      W.METAR_API_URL % "PHNL": fx("metar_history.json")})


def test_history_ndbc_is_the_last_24_hours_ascending_and_cached():
    fetch = history_fetch()
    now = first_t(fx("realtime2_51003.txt", "r")) + 60
    clock = Clock(now)
    svc = W.WindHistory(STATIONS, fetch, now=clock)
    status, p = svc.history("ndbc:51003")
    assert status == "ok" and p["id"] == "ndbc:51003" and p["kind"] == "buoy" and p["tz"] == "Pacific/Honolulu"
    assert p["hours"] == 24 and p["source"] == W.ATTRIBUTION["ndbc"] and p["via"] is None and p["note"] is None
    assert len(p["t"]) == len(p["s"]) == len(p["g"]) == len(p["d"]) and 0 < len(p["t"]) <= W.HISTORY_ROWS
    assert all(a < b for a, b in zip(p["t"], p["t"][1:])) and p["t"][0] >= now - W.HISTORY_S and p["t"][-1] == now - 60
    assert fetch.calls[0][1] == W.NDBC_RT2_MAX and fetch.calls[0][2]["User-Agent"] == W.USER_AGENT
    assert svc.history("ndbc:51003")[1] is p and fetch.count("51003") == 1   # cached
    clock.t += svc.ok_ttl + 1
    svc.history("ndbc:51003")
    assert fetch.count("51003") == 2 and svc.entries() == 1


def test_history_coops_and_metar_and_notes():
    fetch = history_fetch()
    rows = W.parse_coops_wind(json.loads(fx("coops_24.json")))
    svc = W.WindHistory(STATIONS, fetch, now=Clock(rows[-1]["t"] + 60))
    status, p = svc.history("coops:1612340")
    assert status == "ok" and p["src"] == "coops" and p["alias"] == "OOUH1" and len(p["t"]) >= 200 and p["via"] is None
    status, p = svc.history("coops:1611400")                                # NOAA: no data -> ok, empty, a note
    assert status == "ok" and p["t"] == [] and p["note"] == W.NO_HISTORY
    hist = W.parse_metar_history(json.loads(fx("metar_history.json")))
    svc = W.WindHistory(STATIONS, fetch, now=Clock(max(r["t"] for r in hist) + 60))
    status, p = svc.history("metar:PHNL")
    assert status == "ok" and p["kind"] == "airport" and 20 <= len(p["t"]) <= 60 and p["source"] == W.ATTRIBUTION["metar"]
    assert fetch.calls[-1][2]["User-Agent"] == W.USER_AGENT


def test_history_gauge_falls_back_on_its_ndbc_relay():
    fetch = history_fetch()
    fetch.table[W.coops_url("1612340", range=24)] = IOError("NOAA down")
    now = first_t(fx("realtime2_OOUH1.txt", "r")) + 60
    svc = W.WindHistory(STATIONS, fetch, now=Clock(now))
    status, p = svc.history("coops:1612340")
    assert status == "ok" and p["via"] == "ndbc" and p["source"] == W.ATTRIBUTION["ndbc"] and len(p["t"]) >= 150
    assert fetch.count("OOUH1") == 1
    fetch.table[W.coops_url("1612401", range=24)] = IOError("NOAA down")   # no relay: an error, cached a minute
    status, p = svc.history("coops:1612401")
    assert status == "error" and p == {"error": W.UNAVAILABLE}
    assert svc.history("coops:1612401")[0] == "error" and fetch.count("1612401") == 1
    assert fetch.count("realtime2") == 1                                     # no relay: NDBC was not asked for one


def test_history_unknown_errors_and_ttls():
    fetch = history_fetch()
    fetch.table[W.NDBC_RT2_URL % "51003"] = IOError("timeout")
    clock = Clock(1791651300)
    svc = W.WindHistory(STATIONS, fetch, now=clock)
    assert svc.history("nope")[0] == "final" and svc.history("ndbc:51201")[0] == "final" and fetch.calls == []
    assert svc.history("ndbc:51003") == ("error", {"error": W.UNAVAILABLE})
    clock.t += svc.error_ttl - 1
    svc.history("ndbc:51003")
    assert fetch.count("51003") == 1
    clock.t += 2
    svc.history("ndbc:51003")
    assert fetch.count("51003") == 2
    fetch.table[W.NDBC_RT2_URL % "51003"] = b"\xff\xfe not a feed"           # unreadable: no rows, a note
    clock.t += svc.error_ttl + 1
    assert svc.history("ndbc:51003")[1]["note"] == W.NO_HISTORY


def test_history_coops_and_metar_cut_at_24_hours():
    old, new = "2026-10-09 10:00", "2026-10-10 16:48"                      # 30 h and a few minutes before now
    doc = json.dumps({"data": [{"t": old, "s": "9.0", "d": "180", "g": "12.0"}, {"t": new, "s": "1.0", "d": "74", "g": "3.5"}]}).encode()
    fetch = FakeFetch({W.coops_url("1612340", range=24): doc,
                       W.METAR_API_URL % "PHNL": json.dumps([{"obsTime": 1791651180, "wdir": 50, "wspd": 5},
                                                             {"obsTime": 1791651180 - 30 * 3600, "wdir": 50, "wspd": 20}]).encode()})
    svc = W.WindHistory(STATIONS, fetch, now=Clock(1791651180 + 600))
    assert svc.history("coops:1612340")[1]["t"] == [1791650880]
    assert svc.history("metar:PHNL")[1]["t"] == [1791651180]


def test_history_dedupes_sorts_and_caps():
    rows = [{"t": 10 + k, "s": 1.0, "g": None, "d": None} for k in range(400)] + [{"t": 10, "s": 2.0, "g": None, "d": 5}]
    svc = W.WindHistory(STATIONS, FakeFetch(), now=Clock(10 + 400))
    svc._ndbc = lambda local, since: list(reversed(rows))
    status, p = svc.history("ndbc:51003")
    assert status == "ok" and len(p["t"]) == W.HISTORY_ROWS and p["t"] == list(range(110, 410))
    assert p["t"][0] > 10                                                    # the oldest rows are the ones cut


def test_history_lru_and_token_bucket():
    fetch = history_fetch()
    bucket = W.TokenBucket(rate=2, now=lambda: 0.0)
    svc = W.WindHistory(STATIONS, fetch, now=Clock(1791700000), max_entries=2, bucket=bucket)
    assert svc.history("metar:PHNL")[0] == "ok" and svc.history("metar:KSFO")[0] == "error"   # no fixture for KSFO
    fetch.table[W.METAR_API_URL % "KGMU"] = b"[]"
    assert svc.history("metar:KGMU") == ("busy", {"error": W.BUSY})          # the third request within the minute
    assert fetch.count("KGMU") == 0 and svc.entries() == 2
    assert svc.history("metar:KGMU")[0] == "busy"                             # busy is never cached: asked again ...
    bucket._now = lambda: 60.0                                                # ... and answered once the bucket refills
    assert svc.history("metar:KGMU")[1]["note"] == W.NO_HISTORY and fetch.count("KGMU") == 1
    assert svc.entries() == 2 and svc._get(("h", "metar:PHNL")) is None       # LRU: the oldest entry went


def test_token_bucket_refills_at_its_rate():
    t = {"v": 0.0}
    b = W.TokenBucket(rate=60, now=lambda: t["v"])
    assert all(b.take() for _ in range(60)) and not b.take()
    t["v"] = 1.0
    assert b.take() and not b.take()                                         # one a second
    t["v"] = 1000.0
    assert sum(1 for _ in range(100) if b.take()) == 60                      # never more than the rate held


def test_history_singleflight_and_busy():
    gate = threading.Event()

    def slow(url, max_bytes, headers=None):
        gate.wait(5)
        return fx("realtime2_51003.txt")
    fetch = FakeFetch({W.NDBC_RT2_URL % "51003": slow, W.NDBC_RT2_URL % "OOUH1": slow})
    svc = W.WindHistory(STATIONS, fetch, now=Clock(first_t(fx("realtime2_51003.txt", "r")) + 60), builds=1, wait_s=0.3)
    out = {}

    def ask(sid, k):
        out[k] = svc.history(sid)
    ts = [threading.Thread(target=ask, args=("ndbc:51003", k)) for k in range(4)]
    for th in ts:
        th.start()
    _time.sleep(0.1)
    status, _ = svc.history("coops:1612340")                                 # another key while the one build runs
    assert status == "busy"
    gate.set()
    for th in ts:
        th.join(5)
    assert fetch.count("51003") == 1 and {v[0] for v in out.values()} <= {"ok", "busy"}
    assert sum(1 for v in out.values() if v[0] == "ok") >= 1
    # the waiters on the one key gave up after wait_s (the build held its lock longer): busy, not a long wait
    assert sum(1 for v in out.values() if v[0] == "busy") >= 2


def test_after_fork_resets_the_service_locks():
    svc = W.WindHistory(STATIONS, FakeFetch(), now=Clock(0))
    lock, sem = svc._lock, svc._builds
    svc._inflight["x"] = [threading.Lock(), 1]
    W._after_fork()
    assert svc._lock is not lock and svc._builds is not sem and svc._inflight == {}


# ------------------------------------------------------------------ the routes

@pytest.fixture
def winds(monkeypatch, clock):
    """Wind stations ON over the fake providers and a fake history service (nothing reaches the network)."""
    provs, fetch = make_fakes(clock.time())
    hfetch = history_fetch()
    svc = W.WindHistory(STATIONS, hfetch, now=Clock(first_t(fx("realtime2_51003.txt", "r")) + 60))
    doc = {"captured": "2026-10-10T00:00:00Z", "source": "test", "fields": ["id", "name", "lat", "lon", "kind", "src", "tz", "alias"],
           "stations": [[s[k] for k in ("id", "name", "lat", "lon", "kind", "src", "tz", "alias")] for s in STATIONS.values()]}
    monkeypatch.setattr(A, "WIND_ENABLED", True)
    monkeypatch.setattr(A, "_WINDS", {"svc": svc, "list": A._json_payload_and_etag(W.client_list(doc)),
                                      "providers": provs, "stations": dict(STATIONS)})
    monkeypatch.setattr(A, "_WIND_MEMO", {"key": None, "payload": None, "etag": None, "built_ts": None, "build_s": None})
    monkeypatch.setattr(A, "_WIND_BG", {"ticks": 0, "prebuilds": 0, "errors": 0, "last_error": None, "last_tick_ts": None})
    return provs, fetch, hfetch, A.app.test_client()


def test_route_station_list_is_cached_for_hours_with_an_etag(winds):
    _, _, _, c = winds
    r = c.get("/api/wind/stations")
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=21600"
    assert r.headers["CDN-Cache-Control"] == "max-age=21600" and len(r.get_json()["stations"]) == len(STATIONS)
    assert c.get("/api/wind/stations", headers={"If-None-Match": r.headers["ETag"]}).status_code == 304


def test_route_latest_inline_path_builds_once(winds):
    provs, fetch, _, c = winds
    r = c.get("/api/wind/latest")
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=120"
    assert r.headers["CDN-Cache-Control"] == "no-store" and "X-Wind-Stations-Partial" not in r.headers
    d = r.get_json()
    ids = {row[0] for row in d["rows"]}
    assert {"coops:1612340", "ndbc:51003", "ndbc:HRRH1", "metar:PHNL", "metar:KSFO"} <= ids
    assert "coops:1611400" in d["missing"] and "coops:1612401" in d["missing"] and "metar:KGMU" in d["missing"]
    n = len(fetch.calls)
    r2 = c.get("/api/wind/latest")
    assert r2.headers["ETag"] == r.headers["ETag"] and len(fetch.calls) == n   # lists fresh: no fetch, memo hit


def test_route_history_contract(winds):
    _, _, hfetch, c = winds
    r = c.get("/api/wind/coops:9999999/history")
    assert r.status_code == 404 and r.headers["Cache-Control"] == "no-store" and hfetch.calls == []
    assert c.get("/api/wind/ndbc:51201/history").status_code == 404        # NDBC serves it; not in the snapshot
    assert c.get("/api/wind/bad%20id/history").status_code == 404
    r = c.get("/api/wind/ndbc:51003/history")
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=300" and r.get_json()["src"] == "ndbc"
    hfetch.table[W.coops_url("1612401", range=24)] = IOError("down")
    r = c.get("/api/wind/coops:1612401/history")
    assert r.status_code == 503 and r.headers["Retry-After"] == "5" and r.get_json()["retry"] is True
    assert r.headers["Cache-Control"] == "no-store"


def test_routes_off_say_so(winds, monkeypatch):
    _, _, _, c = winds
    monkeypatch.setattr(A, "WIND_ENABLED", False)
    for u in ("/api/wind/stations", "/api/wind/latest", "/api/wind/ndbc:51003/history"):
        r = c.get(u)
        assert r.status_code == 503 and r.get_json()["error"] == A.WIND_OFF and r.headers["Cache-Control"] == "no-store"
    assert A.get_wind_providers() == [] and c.get("/healthz").get_json()["wind"] == {"enabled": False}


def test_routes_without_the_snapshot_say_so(monkeypatch):
    monkeypatch.setattr(A, "WIND_ENABLED", True)
    monkeypatch.setattr(A, "WIND_STATIONS_PATH", os.path.join(HERE, "nope.json"))
    monkeypatch.setattr(A, "_WINDS", {"svc": None, "list": None, "providers": None, "stations": None})
    c = A.app.test_client()
    assert c.get("/api/wind/latest").status_code == 503 and A.get_wind_providers() == []
    assert c.get("/healthz").get_json()["wind"]["error"]


# ------------------------------------------------------------------ the scheduler and /healthz

class RecordingRunner:
    def __init__(self):
        self.jobs, self.queued, self.running, self.waiting, self.workers = [], 0, 0, 0, 0

    def submit(self, fn, name="x"):
        self.jobs.append((name, fn))

    def run(self, n=None):
        jobs, self.jobs = (self.jobs[:n], self.jobs[n:]) if n else (self.jobs, [])
        for _, fn in jobs:
            fn()
        return [name for name, _ in jobs]


@pytest.fixture
def bg(winds, monkeypatch):
    """The background service ON with fake buoy providers beside the fake wind providers, a recording runner."""
    bprovs = F.make_providers()
    monkeypatch.setattr(A, "get_buoy_providers", lambda: bprovs)
    monkeypatch.setattr(A, "_buoy_tz_cached", F.fake_tz)
    monkeypatch.setattr(A, "_LIVE_STATIONS_MEMO", {"key": None, "payload": None, "etag": None})
    monkeypatch.setattr(A, "_LIVE_BG", {"started": False, "thread": None, "started_ts": None, "warm_ts": None, "ticks": 0,
                                        "last_tick_ts": None, "last_tick_s": None, "prebuilds": 0, "errors": 0,
                                        "last_error": None, "tz_loaded": False, "build_failures": 0})
    monkeypatch.setattr(A, "LIVE_BACKGROUND", True)
    monkeypatch.setattr(A, "start_live_background", lambda: True)
    rec = RecordingRunner()
    prev = B.set_refresh_runner(rec)
    yield bprovs, winds[0], rec, winds[3]
    B.set_refresh_runner(prev)


def test_scheduler_pass_queues_the_wind_feeds_after_the_buoys_and_prebuilds(bg):
    bprovs, wprovs, rec, c = bg
    out = A._live_tick(bprovs)
    names = [n for n, _ in rec.jobs]
    assert names[:len(bprovs)] == ["buoy-refresh-%s" % p.source for p in A._live_providers_ordered(bprovs)]
    assert names[len(bprovs):] == ["buoy-refresh-NDBC", "buoy-refresh-METAR", "buoy-refresh-COOPS"]
    assert out["warm"] is False and A._WIND_BG["ticks"] == 1 and A._WIND_BG["prebuilds"] == 1   # an empty table
    rec.run()
    A._live_tick(bprovs)
    assert A._WIND_BG["prebuilds"] == 2 and A._WIND_MEMO["key"] == (("NDBC", 1), ("METAR", 1), ("COOPS", 1))
    assert A._live_tick(bprovs)["built"] is False and A._WIND_BG["prebuilds"] == 2   # nothing new: no build


def test_a_failing_wind_pass_never_breaks_the_buoy_pass(bg, monkeypatch):
    bprovs, _, rec, _ = bg
    monkeypatch.setattr(A, "_wind_tick", lambda providers=None: 1 / 0)
    out = A._live_tick(bprovs)
    assert out["built"] is True and A._LIVE_BG["ticks"] == 1 and A._LIVE_BG["errors"] == 0
    assert A._WIND_BG["errors"] == 1 and A._WIND_BG["last_error"].startswith("tick:")


def test_route_latest_with_the_service_on_never_waits(bg):
    bprovs, wprovs, rec, c = bg
    r = c.get("/api/wind/latest")
    assert r.status_code == 200 and r.get_json()["rows"] == [] and r.headers["Cache-Control"] == "no-store"
    assert r.headers["X-Wind-Stations-Partial"] == "NDBC,METAR,COOPS"
    assert [n for n, _ in rec.jobs] == ["buoy-refresh-NDBC", "buoy-refresh-METAR", "buoy-refresh-COOPS"]  # cold: queued
    rec.run(2)                                                               # NDBC and METAR published
    A._live_tick(bprovs)                                                     # the scheduler builds them
    r = c.get("/api/wind/latest")
    assert r.headers["X-Wind-Stations-Partial"] == "COOPS" and r.headers["Cache-Control"] == "no-store"
    d = r.get_json()
    rows = {row[0]: row for row in d["rows"]}
    assert "ndbc:51003" in rows and "metar:PHNL" in rows
    assert "coops:1612340" in rows and "coops:1611400" not in rows           # Honolulu from its NDBC relay meanwhile
    relay = rows["coops:1612340"]
    assert not any(s.startswith("coops:") for s in d["missing"])              # CO-OPS has no list yet: not "missing"
    rec.run()
    A._LIVE_WAKE.clear()
    r = c.get("/api/wind/latest")                                            # COOPS published, not built yet: old table
    assert r.headers["X-Wind-Stations-Partial"] == "COOPS" and A._LIVE_WAKE.is_set()   # ... and the scheduler is woken
    A._live_tick(bprovs)
    r = c.get("/api/wind/latest")
    assert "X-Wind-Stations-Partial" not in r.headers and r.headers["Cache-Control"] == "public, max-age=120"
    d = r.get_json()
    own = next(row for row in d["rows"] if row[0] == "coops:1612340")
    latest = W.parse_coops_wind(json.loads(fx("coops_latest.json")))[0]
    assert own != relay and own[1:] == [latest["t"], latest["s"], latest["g"], latest["d"]]   # CO-OPS's own reading now
    assert "coops:1611400" in d["missing"] and "coops:1612401" in d["missing"]


def test_healthz_reports_the_wind_feeds_without_touching_warm(bg):
    bprovs, wprovs, rec, c = bg
    rec.run()                                                                # nothing queued yet: no-op
    A._live_tick(bprovs)
    rec.run(len(bprovs))                                                     # the buoy feeds only
    A._live_tick(bprovs)
    body = c.get("/healthz").get_json()
    assert body["warm"] is True and body["missing"] == [] and body["ok"] is True   # the wind feeds are still cold ...
    w = body["wind"]
    assert w["enabled"] is True and w["missing"] == ["NDBC", "METAR", "COOPS"] and w["memo"]["complete"] is False
    assert [s["source"] for s in w["providers"]] == ["NDBC", "METAR", "COOPS"] and w["ticks"] == 2
    rec.run()
    A._live_tick(bprovs)
    w = c.get("/healthz").get_json()["wind"]
    assert w["missing"] == [] and w["memo"]["complete"] is True and w["memo"]["current"] is True and w["memo"]["bytes"] > 100
    assert w["history_entries"] == 0 and w["errors"] == 0
