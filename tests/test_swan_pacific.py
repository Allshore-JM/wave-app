"""The Pacific-island SWAN tables and the SWAN freshness rule (plan section 64).

The station map: five NOAA-listed Pacific buoys (Guam, Saipan, American Samoa, Palau) join the 12 Hawaii stations on
the SWAN button; the tables are read by the same code (two real tables cut to 30 rows, captured by
tests/capture_swan_pacific_fixtures.py). The freshness rule is the NWPS rule of section 59: every table fetch and a
background probe record each region's publish time (Last-Modified); past 36 h the answer carries the age; past 72 h
the station is served as one without SWAN. Nothing reaches the network: HTTP.get and the probe's HEAD are fakes.
"""
import os
import re
import threading
import time
from datetime import datetime, timezone

import pytest

import app as A

HERE = os.path.dirname(os.path.abspath(__file__))
FIX = {"cdip196": "swan_buoy_52202.table", "cdip219": "swan_buoy_52212.table", "cdip106": "swan_buoy_sample.table"}
LAST_MODIFIED = "Thu, 29 Oct 2026 23:00:01 GMT"
PUBLISHED = datetime(2026, 10, 29, 23, 0, 1, tzinfo=timezone.utc).timestamp()
PACIFIC = ("52200", "52202", "52211", "51209", "52212")


class Resp:
    def __init__(self, text, status=200, headers=None):
        self.status_code, self.text = status, text
        self.headers = {"Last-Modified": LAST_MODIFIED} if headers is None else headers


class Site:
    """A fake PacIOOS: HTTP.get serves the fixture tables, the probe's HEAD their headers; both counted."""
    def __init__(self):
        self.gets, self.heads, self.fail, self.status, self.now = [], [], set(), 200, PUBLISHED + 3600
        self.last_modified = LAST_MODIFIED

    def cdip(self, url):
        return url.rsplit("buoy.", 1)[1].split(".table")[0]

    def get(self, url, timeout=15, **kw):
        c = self.cdip(url)
        self.gets.append(c)
        if c in self.fail:
            raise IOError("timed out")
        name = FIX.get(c, FIX["cdip106"])
        return Resp(open(os.path.join(HERE, "fixtures", name), encoding="ascii").read(), self.status,
                    {"Last-Modified": self.last_modified} if self.last_modified else {})

    def head(self, url):
        c = self.cdip(url)
        self.heads.append(c)
        if c in self.fail:
            raise IOError("timed out")
        return Resp("", self.status, {"Last-Modified": self.last_modified} if self.last_modified else {})


def reset():
    for rec in A._SWAN_UPDATED.values():
        rec.update(updated=None, ts=0.0, failed=0.0, error=None)
    A._SWAN_BG.update(thread=None, started=0)
    A._FORECAST_CACHE.clear()
    A._FORECAST_INFLIGHT.clear()


@pytest.fixture
def site(monkeypatch):
    s = Site()
    reset()
    monkeypatch.setattr(A.HTTP, "get", s.get)
    monkeypatch.setattr(A, "_swan_head", s.head)
    monkeypatch.setattr(A, "_swan_now", lambda: s.now)
    monkeypatch.setattr(A, "_gfs_wind_cover", lambda sid, first: {})
    monkeypatch.setattr(A, "SWAN_PROBE", True)
    yield s
    reset()


def hours(h):
    return PUBLISHED + h * 3600


# ------------------------------------------------------------------------------------------------ the station map

def test_the_seventeen_stations_their_regions_and_the_model_rule():
    assert len(A.SWAN_STATIONS) == 17 and set(PACIFIC) <= set(A.SWAN_STATIONS)
    assert A.SWAN_STATIONS["52202"] == "cdip196" and A.SWAN_STATIONS["52212"] == "cdip219"
    listed = {sid for sid, _ in A.get_station_list()}
    coords, zones = A.load_station_coords(), {s: A.get_station_tz(s) for s in A.SWAN_STATIONS}
    assert set(A.SWAN_STATIONS) <= listed                              # every one a GFS forecast station
    assert all(s in coords and zones[s] for s in A.SWAN_STATIONS)
    assert zones["52202"] == "Pacific/Guam" and zones["52211"] == "Pacific/Saipan"
    assert zones["51209"] == "Pacific/Pago_Pago" and zones["52212"] == "Pacific/Palau"
    assert set(A.SWAN_STATION_REGION) == set(A.SWAN_STATIONS) and set(A.SWAN_STATION_REGION.values()) == set(A.SWAN_REGION)
    assert {s for s, r in A.SWAN_STATION_REGION.items() if r == "hawaii"} == set(A.SWAN_STATIONS) - set(PACIFIC)
    assert A.SWAN_STATION_REGION["52200"] == "mariana" == A.SWAN_STATION_REGION["52211"]
    assert A.SWAN_STATION_REGION["51209"] == "samoa" and A.SWAN_STATION_REGION["52212"] == "palau"
    assert set(A.SWAN_REGION.values()) <= set(A.SWAN_STATIONS.values())   # a representative table per region
    # the three Guam / Saipan buoys are NWPS stations too (office GUM): the bar shows GFS | SWAN | NWPS there
    assert {"52200", "52202", "52211"} <= set(A.NWPS_STATIONS) and not {"51209", "52212"} & set(A.NWPS_STATIONS)
    for sid, asked, used in (("52202", "SWAN", "SWAN"), ("52202", "swan", "SWAN"), ("52202", "NWPS", "NWPS"),
                             ("51209", "SWAN", "SWAN"), ("51209", "NWPS", "GFS"), ("52212", "SWAN", "SWAN"),
                             ("46001", "SWAN", "GFS"), ("45007", "SWAN", "GLWU"), ("52202", None, "GFS")):
        assert A.resolve_model(sid, asked) == used, (sid, asked)
    assert A.resolve_model("52202", "SWAN", None, False) == "GFS", "the caller's judgement (swan_ok) is honoured"
    assert A.resolve_model("46001", "SWAN", None, True) == "GFS", "... but never makes a SWAN station"


# ------------------------------------------------------------------------------------------------------ the tables

@pytest.mark.parametrize("sid,cdip,zone,first", [("52202", "cdip196", "Pacific/Guam", datetime(2026, 10, 29, 12)),
                                                ("52212", "cdip219", "Pacific/Palau", datetime(2026, 10, 29, 6))])
def test_a_pacific_table_through_the_real_parser(site, sid, cdip, zone, first):
    """The Guam table starts at 06Z (shown from 12Z); Palau's 14-day run at 00Z (shown from 06Z)."""
    firsts = []
    site.gets.clear()
    A._gfs_wind_cover = lambda s, first: (firsts.append((s, first)), {})[1]
    cycle, location, _, rows, tz, err = A.parse_swan(sid)
    assert err is None and site.gets == [cdip]
    assert cycle == "Cycle : PacIOOS SWAN updated 20261029 23 UTC"
    assert location.startswith(f"Location : {sid} (") and tz == zone
    assert len(rows) == 30 - 6, "the 6-hour spin-up is dropped"
    assert all(len(r) == 23 for r in rows)
    assert firsts == [(sid, first)], "the wind back-fill asks from the first shown row (the first time + 6 h)"
    assert rows[0][1] in ("10:00 PM", "3:00 PM"), rows[0]        # 12Z in Guam (UTC+10) / 06Z in Palau (UTC+9)
    assert rows[0][-1] and rows[0][-1] > 0.5, "combined height in feet"
    hs_ft = [r[2] for r in rows if r[2] is not None]
    assert hs_ft and max(hs_ft) < 40
    # the regions' publish time was recorded by the fetch
    rec = A._SWAN_UPDATED[A._swan_region(sid)]
    assert rec["updated"] == PUBLISHED and rec["ts"] == site.now and rec["error"] is None
    assert A._SWAN_UPDATED["hawaii"]["updated"] is None, "only the station's own region"


def test_the_locations_say_east_longitudes_and_south_latitudes(site):
    _, loc, _, _, _, err = A.parse_swan("52212")
    assert err is None and "E)" in loc and "N " in loc
    _, loc, _, _, _, err = A.parse_swan("51209")
    assert err is None and "S " in loc and "W)" in loc


# ---------------------------------------------------------------------------------------------- the record and the probe

def test_last_modified_parsing():
    assert A._swan_last_modified({"Last-Modified": LAST_MODIFIED}) == PUBLISHED
    assert A._swan_last_modified({"Last-Modified": "soon"}) is None
    assert A._swan_last_modified({}) is None and A._swan_last_modified(None) is None


def test_a_fetch_without_a_last_modified_is_a_good_read_that_keeps_the_held_time(site):
    A._swan_record("mariana", PUBLISHED - 86400)
    site.now = hours(2)
    site.last_modified = None
    cycle, *_ = A.parse_swan("52202")
    assert cycle == "Cycle : PacIOOS SWAN (latest run)"
    rec = A._SWAN_UPDATED["mariana"]
    assert rec["updated"] == PUBLISHED - 86400 and rec["ts"] == site.now


def test_a_failed_fetch_notes_the_error_and_keeps_the_record(site):
    A._swan_record("samoa", PUBLISHED)
    site.now = hours(1)
    site.fail.add("cdip189")
    _, _, _, rows, _, err = A.parse_swan("51209")
    assert rows is None and err == "Could not download SWAN forecast for 51209"
    rec = A._SWAN_UPDATED["samoa"]
    assert rec["updated"] == PUBLISHED and rec["failed"] == site.now and "OSError" in rec["error"]
    assert A.swan_offered("51209"), "a failed read withdraws nothing"


def test_the_probe_records_a_region_and_a_failed_probe_keeps_it(site):
    A._swan_probe("palau")
    assert site.heads == ["cdip219"]
    rec = A._SWAN_UPDATED["palau"]
    assert rec["updated"] == PUBLISHED and rec["ts"] == site.now and rec["error"] is None
    site.fail.add("cdip219")
    site.now = hours(3)
    A._swan_probe("palau")
    assert rec["updated"] == PUBLISHED and rec["failed"] == site.now and rec["error"]
    site.fail.clear()
    site.status = 503
    site.now = hours(4)
    A._swan_probe("palau")
    assert rec["error"] == "RuntimeError: HTTP 503" and rec["updated"] == PUBLISHED
    site.status = 200
    site.last_modified = "Fri, 30 Oct 2026 23:00:00 GMT"
    A._swan_probe("palau")
    assert rec["updated"] == PUBLISHED + 24 * 3600 - 1 and rec["error"] is None


def test_the_tick_probes_the_oldest_due_region_one_thread_at_a_time(site, monkeypatch):
    # nothing held: every region due; the oldest record (all equal: the first in dict order) first
    assert A._swan_due_region(site.now) == "hawaii"
    gate = threading.Event()
    seen = []

    def slow_head(url):
        seen.append(site.cdip(url))
        gate.wait(5)
        return Resp("", 200, {"Last-Modified": LAST_MODIFIED})
    monkeypatch.setattr(A, "_swan_head", slow_head)
    assert A._swan_tick() is True
    t = A._SWAN_BG["thread"]
    assert t is not None and t.name == "swan-probe" and t.daemon
    assert A._swan_tick() is False, "one probe at a time"
    gate.set()
    t.join(5)
    assert seen == ["cdip106"] and A._SWAN_UPDATED["hawaii"]["updated"] == PUBLISHED
    assert A._SWAN_BG["started"] == 1
    # the next pass takes the next region; a region read within the TTL is not due
    assert A._swan_due_region(site.now) == "mariana"
    site.now += A._SWAN_PROBE_TTL - 1
    assert A._swan_due_region(site.now) == "mariana"
    for r in ("mariana", "samoa", "palau"):
        A._swan_record(r, PUBLISHED)
    assert A._swan_due_region(site.now) is None
    site.now += A._SWAN_PROBE_TTL
    assert A._swan_due_region(site.now) == "hawaii"
    # a failed region waits the negative TTL
    A._swan_failed("hawaii", IOError("x"))
    assert A._swan_due_region(site.now) == "mariana"
    site.now += A._SWAN_PROBE_NEG_TTL
    assert A._swan_due_region(site.now) == "hawaii"
    # the switch
    monkeypatch.setattr(A, "SWAN_PROBE", False)
    assert A._swan_tick() is False


def test_the_tick_rides_on_the_live_scheduler_pass_and_the_fork_reset(site, monkeypatch):
    calls = []
    monkeypatch.setattr(A, "_swan_tick", lambda: calls.append(1))
    monkeypatch.setattr(A, "_live_schedule", lambda p: 0)
    monkeypatch.setattr(A, "_live_prebuild", lambda p: False)
    monkeypatch.setattr(A, "_live_warm", lambda p: False)
    monkeypatch.setattr(A, "_nwps_tick", lambda: False)
    monkeypatch.setattr(A, "WIND_ENABLED", False)
    A._live_tick([])
    assert calls == [1]
    monkeypatch.setattr(A, "_swan_tick", lambda: 1 / 0)
    A._live_tick([])                                                    # a raising probe pass never stops the scheduler
    A._SWAN_BG["thread"] = threading.current_thread()
    lock = A._SWAN_LOCK
    A._live_after_fork()
    assert A._SWAN_LOCK is not lock and A._SWAN_BG["thread"] is None


# ----------------------------------------------------------------------------------------------- the age and the hide

def test_the_age_note_the_hide_and_the_held_record(site):
    assert A.swan_age_h("52202") is None and A.swan_offered("52202"), "nothing held: offered"
    site.now = hours(40)
    A._swan_record("mariana", PUBLISHED)                                # read now: 40 h after the publish
    assert A.swan_age_h("52202") == pytest.approx(40) and A.swan_offered("52202")
    site.now = hours(72)
    A._swan_record("mariana", PUBLISHED)                                # the probe reads it every 5 min
    assert A.swan_offered("52202") and A.resolve_model("52202", "SWAN") == "SWAN"
    site.now = hours(72.01)
    A._swan_record("mariana", PUBLISHED)
    assert not A.swan_offered("52202") and A.resolve_model("52202", "SWAN") == "GFS"
    assert A.swan_offered("51201"), "another region: nothing held, offered"
    assert not A.swan_offered("46001")
    # the record decides for _SWAN_HELD_S after its last good read, then nothing (offered again)
    rec = A._SWAN_UPDATED["mariana"]
    rec["ts"] = site.now - A._SWAN_HELD_S + 1
    assert not A.swan_offered("52202")
    rec["ts"] = site.now - A._SWAN_HELD_S
    assert A.swan_age_h("52202") is None and A.swan_offered("52202")


def test_the_offered_set_equals_the_per_station_rule(site):
    assert A.swan_offered_set() == set(A.SWAN_STATIONS)
    site.now = hours(76)
    A._swan_record("mariana", PUBLISHED)                                # read 76 h after the publish: stale, held 4 h
    A._swan_record("palau", PUBLISHED)
    site.now = hours(80)
    A._swan_record("samoa", PUBLISHED)
    want = {s for s in A.SWAN_STATIONS if A.swan_offered(s)}
    assert A.swan_offered_set() == want == set(A.SWAN_STATIONS) - {"52200", "52202", "52211", "52212", "51209"}
    A._SWAN_UPDATED["mariana"]["ts"] = site.now - A._SWAN_HELD_S       # the held record expired: offered again
    assert A.swan_offered_set() == {s for s in A.SWAN_STATIONS if A.swan_offered(s)} and "52202" in A.swan_offered_set()
    site.now = hours(72)                                                # exactly 72.0 h: still offered (<=)
    A._swan_record("palau", PUBLISHED)
    assert "52212" in A.swan_offered_set() and A.swan_offered("52212")


def test_the_cycle_age_and_the_wind_rule():
    cyc = "Cycle : PacIOOS SWAN updated 20261029 23 UTC"
    A._swan_now = lambda: PUBLISHED - 1 + 50 * 3600
    try:
        assert A._swan_cycle_age_h(cyc) == pytest.approx(50)
        assert A._swan_cycle_age_h("Cycle : PacIOOS SWAN (latest run)") is None
        assert A._swan_cycle_age_h("Cycle : NOAA NWPS 20261029 23 UTC (NWS GUM)") is None
        assert A._swan_cycle_age_h(None) is None
        assert A._swan_wind_final(cyc) and A._old_run_wind_final(cyc)
        blank = (cyc, "Location : 52202", None, [["d", "t"] + [None] * 18 + [None, None, 1.0]], "Pacific/Guam", None)
        assert A._forecast_entry_ttl(blank) == A._FORECAST_CACHE_TTL, "an old table's blank wind cells are final"
        A._swan_now = lambda: PUBLISHED - 1 + 36 * 3600                    # exactly 36.0 h: final (G43: the run covering
        assert A._swan_wind_final(cyc) and A._forecast_entry_ttl(blank) == A._FORECAST_CACHE_TTL   # the first row is gone)
        A._swan_now = lambda: PUBLISHED + 35.99 * 3600
        assert not A._swan_wind_final(cyc) and not A._old_run_wind_final(cyc)
        assert A._forecast_entry_ttl(blank) == A._WIND_NEG_TTL, "a fresh table's blank wind cells may still heal"
        gfs = ("Cycle : 20261029 18 UTC", "Location : 52202", None, blank[3], "Pacific/Guam", None)
        assert A._forecast_entry_ttl(gfs) == A._WIND_NEG_TTL, "a GFS table: unchanged"
    finally:
        A._swan_now = time.time


def test_an_old_tables_blank_wind_cells_count_as_complete(site):
    # the Site fake's wind cover is empty: every SWAN row's wind cells are blank (G43 F1)
    site.now = hours(30)
    p = A.compute_forecast_payload("52202", None, "US", "SWAN", compact=True)
    assert p["model"] == "SWAN" and p["wind_complete"] is False, "a fresh table's blanks may still heal"
    A._FORECAST_CACHE.clear()
    site.now = hours(50)
    p = A.compute_forecast_payload("52202", None, "US", "SWAN", compact=True)
    assert p["model"] == "SWAN" and p["wind_complete"] is True, "an old table's blanks are final: no minute re-asks"


def test_a_record_never_goes_back_and_a_1970_header_counts_as_none(site):
    t2 = PUBLISHED + 86400
    A._swan_record("hawaii", t2)
    A._swan_record("hawaii", PUBLISHED)                                 # a fetch that started before the probe (G43 F3)
    assert A._SWAN_UPDATED["hawaii"]["updated"] == t2
    site.now = hours(30)
    A._swan_record("hawaii", 0)                                         # nginx mtime 0 (G43 F5): a read without a time
    assert A._SWAN_UPDATED["hawaii"]["updated"] == t2 and A._SWAN_UPDATED["hawaii"]["ts"] == site.now
    A._swan_record("palau", 0)
    assert A._SWAN_UPDATED["palau"]["updated"] is None and A.swan_offered("52212")
    assert A._swan_health()["regions"]["palau"]["age_h"] is None


def test_the_tick_starts_nothing_when_no_region_is_due(site):
    for r in A.SWAN_REGION:
        A._swan_record(r, PUBLISHED)
    assert A._swan_tick() is False and A._SWAN_BG["thread"] is None and A._SWAN_BG["started"] == 0


def test_the_payload_carries_the_age_and_falls_back_to_gfs_past_72_h(site, monkeypatch):
    gfs_calls = []

    def gfs(sid, tz=None):
        gfs_calls.append(sid)
        return ("Cycle : 20261030 12 UTC", f"Location : {sid} (13.68N 144.81E)", None,
                [["Friday, October 30, 2026", "10:00 PM"] + [None] * 18 + [5.0, 90, 2.0]], "Pacific/Guam", None)
    monkeypatch.setattr(A, "parse_bull", gfs)
    site.now = hours(35.9)
    p = A.compute_forecast_payload("52202", None, "US", "SWAN", compact=True)
    assert p["model"] == "SWAN" and p["swan_available"] is True and p["nwps_available"] is True
    assert p["swan_age_hours"] == 35.9 and "nwps_age_hours" not in p and p["error"] is None
    assert p["graph_header"]["cycle"] == "PacIOOS SWAN updated 20261029 23 UTC" and gfs_calls == []
    A._FORECAST_CACHE.clear()
    site.now = hours(72) - 1                                            # exactly 72.0 h by the cycle line: still SWAN
    p = A.compute_forecast_payload("52202", None, "US", "SWAN", compact=True)
    assert p["model"] == "SWAN" and p["swan_available"] is True and p["swan_age_hours"] == 72.0 and gfs_calls == []
    A._FORECAST_CACHE.clear()
    site.now = hours(72.5)                                              # the read itself found the table too old
    p = A.compute_forecast_payload("52202", None, "US", "SWAN", compact=True)
    assert p["model"] == "GFS" and p["swan_available"] is False and gfs_calls == ["52202"]
    assert "swan_age_hours" not in p and p["graph_header"]["cycle"] == "20261030 12 UTC"
    assert not A.swan_offered("52202"), "and the record now says so for every later answer"
    p = A.compute_forecast_payload("52202", None, "US", "SWAN", compact=True)
    assert p["model"] == "GFS" and p["swan_available"] is False and gfs_calls == ["52202", "52202"]
    assert site.gets.count("cdip196") == 2, "a stale region is not even read"
    # a table of another model never gets a SWAN age
    assert "swan_age_hours" not in A.compute_forecast_payload("52202", None, "US", "GFS", compact=True)


def test_the_cache_key_follows_the_held_publish_time(site):
    A.parse_swan("52212")
    A.parse_swan("52212")
    assert site.gets.count("cdip219") == 1, "the second call is served from the cache"
    site.last_modified = "Fri, 30 Oct 2026 23:00:00 GMT"
    A._swan_record("palau", A._swan_last_modified({"Last-Modified": site.last_modified}))   # the probe saw a new table
    cycle, *_ = A.parse_swan("52212")
    assert site.gets.count("cdip219") == 2 and cycle == "Cycle : PacIOOS SWAN updated 20261030 23 UTC"


# ------------------------------------------------------------------------------------------------ the page and /healthz

def test_healthz_reports_the_swan_regions(site):
    site.now = hours(1)
    A._swan_record("mariana", PUBLISHED)
    site.now = hours(10)
    A._swan_failed("palau", IOError("nope"))
    with A.app.test_client() as c:
        body = c.get("/healthz").get_json()
    s = body["swan"]
    assert s["stations"] == 17 and set(s["regions"]) == set(A.SWAN_REGION) and s["probes_started"] == 0
    assert s["regions"]["mariana"] == {"updated_utc": "2026-10-29T23:00:01Z", "age_h": 10.0, "read_age_s": 32400,
                                       "last_error": None}
    assert s["regions"]["palau"]["last_error"] == "OSError: nope" and s["regions"]["palau"]["age_h"] is None
    assert s["regions"]["hawaii"]["read_age_s"] is None


def page(client, q):
    import json
    import re
    b = client.get(q).get_data(as_text=True)
    return b, json.loads(re.search(r"window\.__initial = (\{.*?\});", b).group(1))


def test_the_page_offers_three_models_at_guam_and_withdraws_a_stale_region(site, monkeypatch):
    import re
    with A.app.test_client() as c:
        b, st = page(c, "/?station=52202&model=SWAN")
        assert st["model"] == "SWAN" and st["swan_available"] is True and st["nwps_available"] is True
        assert len(st["swan_stations"]) == 17 and "52202" in st["swan_stations"] and "51209" in st["swan_stations"]
        assert re.search(r'data-model="SWAN" aria-pressed="true"', b) and not re.search(r'data-model="NWPS"[^>]*hidden', b)
        assert re.search(r'<option value="SWAN" selected>SWAN</option>', b) and '<option value="NWPS"' in b
        b, st = page(c, "/?station=51209&model=NWPS")
        assert st["model"] == "GFS" and st["swan_available"] is True and st["nwps_available"] is False
        assert re.search(r'data-model="NWPS" aria-pressed="false" hidden', b) and not re.search(r'data-model="SWAN"[^>]*hidden', b)
        # the Mariana tables stale: the three Guam / Saipan stations lose SWAN, the other regions keep it
        site.now = hours(73)
        A._swan_record("mariana", PUBLISHED)                            # a probe just read the three-day-old table
        b, st = page(c, "/?station=52202&model=SWAN")
        assert st["model"] == "GFS" and st["swan_available"] is False
        assert sorted(st["swan_stations"]) == sorted(set(A.SWAN_STATIONS) - {"52200", "52202", "52211"})
        assert re.search(r'data-model="SWAN" aria-pressed="false" hidden', b) and not re.search(r'id="modelBar" hidden', b)
        b, st = page(c, "/?station=51201&model=SWAN")
        assert st["model"] == "SWAN" and st["swan_available"] is True and len(st["swan_stations"]) == 14


def test_render_full_seeds_the_age_and_the_meta_line_note(site, monkeypatch):
    monkeypatch.setattr(A, "parse_bull", lambda sid, tz=None: (None, None, None, None, "UTC", "no"))
    site.now = hours(40.4)
    with A.app.test_client() as c:
        b, st = page(c, "/?station=52212&model=SWAN&render=full")
        assert st["inline"] is True and st["model"] == "SWAN" and st["swan_age_hours"] == 40.4
        assert "PacIOOS SWAN updated 20261029 23 UTC · 40 h old &nbsp;|&nbsp;" in b
        site.now = hours(35)
        b, st = page(c, "/?station=52212&model=SWAN&render=full")
        assert st["swan_age_hours"] == 35.0 and "h old" not in b.split('id="forecastMeta"', 1)[1][:300]
        # the note's threshold is the model's own, not the lower of the two (G43 F4)
        monkeypatch.setattr(A, "_NWPS_NOTE_H", 10)
        site.now = hours(20)
        b, st = page(c, "/?station=52212&model=SWAN&render=full")
        assert st["swan_age_hours"] == 20.0 and "h old" not in b.split('id="forecastMeta"', 1)[1][:300]


def test_render_full_follows_the_reads_judgement_for_the_whole_region(site, monkeypatch):
    # the inline read withdrew the station (its table's own cycle line is too old) while the held set still offered it:
    # the page's flag AND list follow the read, and the region's siblings go with it (G43 F2 / M20)
    gfs = ("Cycle : 20261030 12 UTC", "Location : 52202 (13.68N 144.81E)", None,
           [["Friday, October 30, 2026", "10:00 PM"] + [None] * 18 + [5.0, 90, 2.0]], "Pacific/Guam", None)
    monkeypatch.setattr(A, "parse_bull", lambda sid, tz=None: gfs)
    monkeypatch.setattr(A, "swan_offered_set", lambda: set(A.SWAN_STATIONS))
    site.now = hours(72.5)
    with A.app.test_client() as c:
        b, st = page(c, "/?station=52202&model=SWAN&render=full")
        assert st["model"] == "GFS" and st["swan_available"] is False
        assert sorted(st["swan_stations"]) == sorted(set(A.SWAN_STATIONS) - {"52200", "52202", "52211"})
        assert re.search(r'data-model="SWAN" aria-pressed="false" hidden', b)
        # no reverse case: a station the held set withdrew is asked for as GFS before any read (the first page test)


def test_the_api_error_answer_judges_swan_by_the_rule(site, monkeypatch):
    monkeypatch.setattr(A, "compute_forecast_payload", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    with A.app.test_client() as c:
        j = c.get("/api/forecast?station=52202&model=SWAN").get_json()
        assert j["model"] == "SWAN" and j["swan_available"] is True and j["error"]
        site.now = hours(73)
        A._swan_record("mariana", PUBLISHED)
        j = c.get("/api/forecast?station=52202&model=SWAN").get_json()
        assert j["model"] == "GFS" and j["swan_available"] is False, "a withdrawn station stays withdrawn (G43 M11)"
