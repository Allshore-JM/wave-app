"""The NWPS forecast tables on the site (plan section 59, step 4): the NWPS job's station files, read from the frames
bucket (never NOMADS), turned into the forecast table's rows; the model rule; the run fallback; the checks on what the
bucket answers; the GFS station winds joined; the payload; /healthz.

The bucket is a fake _points_fetch serving the real published file of 51003 (hfo's 2026102806 run,
tests/fixtures/nwps/site_51003.json, captured by tests/capture_nwps_site_fixture.py) and synthetic pointers; the GFS
winds are a fake get_station_wind. Nothing reaches the network.
"""
import copy
import json
import os
from datetime import datetime, timedelta

import pytest
import pytz

import app as A

HERE = os.path.dirname(os.path.abspath(__file__))
DOC = json.load(open(os.path.join(HERE, "fixtures", "nwps", "site_51003.json"), encoding="utf-8"))
RUN = DOC["run"]                                                   # 2026102806


class Bucket:
    def __init__(self):
        self.objects, self.asked, self.fail = {}, [], set()

    def put(self, key, value):
        self.objects[key] = value if isinstance(value, bytes) else json.dumps(value).encode()

    def fetch(self, url, max_bytes, headers=None, retry_dropped=False):
        key = url.split("models.test/", 1)[1].split("?", 1)[0]
        self.asked.append(key)
        if key in self.fail:
            raise IOError("timed out")
        if key not in self.objects:
            raise IOError("HTTP 404")
        body = self.objects[key]
        if len(body) > max_bytes:
            raise IOError("body too large")
        return body


def pointer(runs=(RUN,), stations=("51003",), office="hfo", run=None):
    return {"format": "nwps-pointer-v1", "checked_utc": "2026-10-28T21:52:10Z",
            "offices": {office: {"run": run or runs[0], "region": "pr", "published_utc": "2026-10-28T21:52:10Z",
                                 "stations": list(stations), "runs": list(runs)}}}


def doc_for(run, sid="51003", **change):
    d = copy.deepcopy(DOC)
    d.update(run=run, station=sid, start_utc=f"{run[:4]}-{run[4:6]}-{run[6:8]}T{run[8:]}:00:00Z", **change)
    return d


@pytest.fixture
def bucket(monkeypatch):
    b = Bucket()
    monkeypatch.setattr(A, "_points_fetch", b.fetch)
    monkeypatch.setenv("POINTS_ROOT", "https://models.test")
    A._NWPS_PTR.update(ts=0.0, good=0.0, doc=None, error=None, failed=0.0)
    A._FORECAST_CACHE.clear()
    clock = {"now": datetime.strptime(RUN, "%Y%m%d%H") + timedelta(hours=10)}     # the run 10 h old (G41 K-7 rules)
    monkeypatch.setattr(A, "_nwps_utcnow", lambda: clock["now"])
    b.clock = clock
    winds = {}
    monkeypatch.setattr(A, "get_station_wind", lambda sid, date_str=None, run_str=None, keep_s=None: winds.get(
        (date_str, run_str), {}))
    b.winds = winds
    yield b
    A._NWPS_PTR.update(ts=0.0, good=0.0, doc=None, error=None, failed=0.0)
    A._FORECAST_CACHE.clear()


def gfs_winds(start, hours, speed=5.0):
    return {start + timedelta(hours=h): (speed, 45.0) for h in range(hours)}


# ----------------------------------------------------------------------------------------------- the station set

def test_the_station_file_and_the_model_rule(tmp_path):
    assert len(A.NWPS_STATIONS) == 112 and A.NWPS_STATIONS["51003"]["office"] == "hfo"
    listed = {sid for sid, _ in A.get_station_list()}
    assert set(A.NWPS_STATIONS) <= listed                             # every one a GFS forecast station
    assert not set(A.NWPS_STATIONS) & set(A.SWAN_STATIONS) and not set(A.NWPS_STATIONS) & set(A.GLWU_STATIONS)
    for sid, asked, used in (("51003", "NWPS", "NWPS"), ("51003", "nwps", "NWPS"), ("51003", "SWAN", "GFS"),
                             ("51201", "NWPS", "GFS"), ("51201", "SWAN", "SWAN"), ("13130", "NWPS", "GFS"),
                             ("45007", "NWPS", "GLWU"), ("46026", None, "GFS"), ("46240", "NWPS", "GFS")):
        assert A.resolve_model(sid, asked) == used, (sid, asked)
    bad = tmp_path / "s.json"
    bad.write_text(json.dumps({"stations": {"1": {"office": "HFO!", "lat": 1, "lon": 1}, "2": {"office": "hfo"},
                                            "3": {"office": "hfo", "lat": 19.0, "lon": -160.0}}}))
    assert list(A._load_nwps_stations(str(bad))) == ["3"]
    bad.write_text("{")
    assert A._load_nwps_stations(str(bad)) == {}


# ---------------------------------------------------------------------------------------------------- the rows

def test_rows_from_the_real_file_feet_rank_and_wind(bucket):
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    run_dt = datetime.strptime(RUN, "%Y%m%d%H")
    bucket.winds[(None, None)] = gfs_winds(run_dt, 400)
    cycle, location, _mr, rows, tz, err = A._parse_nwps_uncached("51003")
    assert err is None and tz == "Pacific/Honolulu" and len(rows) == 145
    assert cycle == f"Cycle : NOAA NWPS {RUN[:8]} {RUN[8:]} UTC (NWS HFO)"
    assert location == "Location : 51003 (19.29N 160.57W)"           # the NWPS point, not the GFS one (G41 B-3)
    first = rows[0]
    local = run_dt.replace(tzinfo=pytz.utc).astimezone(pytz.timezone("Pacific/Honolulu"))
    assert first[1] == local.strftime("%I:%M %p").lstrip("0") and first[0].startswith(local.strftime("%A"))
    # hour 0's partitions [1.24 m 13.5 s 323] [0.9 4.5 70] [0.85 8.6 0] [0.78 15.4 187] [0.77 7.3 118], in feet and
    # in the site's rank (height squared x period): 323, 187, 0, 118, 70
    assert [first[4 + 3 * g] for g in range(5)] == [323, 187, 0, 118, 70] and first[17:20] == [None, None, None]
    assert first[2] == round(1.24 * 3.28084, 2) and first[3] == 13.5
    assert first[20:22] == [5.0, 45] and first[22] == round(DOC["hs"][0] * 3.28084, 2)


def test_an_hour_without_a_spectrum_is_left_out(bucket):
    d = doc_for(RUN)
    d["hs"][3], d["parts"][3] = None, None
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", d)
    rows = A._parse_nwps_uncached("51003")[3]
    assert len(rows) == 144


def test_the_wind_back_fill_covers_hours_before_the_latest_gfs_run(bucket):
    run_dt = datetime.strptime(RUN, "%Y%m%d%H")                       # 06Z: the latest GFS run 12Z starts 6 h later
    bucket.winds[(None, None)] = gfs_winds(run_dt + timedelta(hours=6), 380, speed=7.0)
    bucket.winds[(RUN[:8], RUN[8:])] = gfs_winds(run_dt, 380, speed=3.0)
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    rows = A._parse_nwps_uncached("51003")[3]
    assert [r[20] for r in rows[:8]] == [3.0] * 6 + [7.0] * 2           # the older run only where the latest has nothing
    assert all(r[20] is not None for r in rows)


def test_without_gfs_winds_the_waves_still_come(bucket):
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    rows = A._parse_nwps_uncached("51003")[3]
    assert len(rows) == 145 and all(r[20] is None and r[21] is None for r in rows)


# ------------------------------------------------------------------------------------------------- which run

def test_the_newest_run_listing_the_station_else_the_kept_runs(bucket):
    older = (datetime.strptime(RUN, "%Y%m%d%H") - timedelta(hours=6)).strftime("%Y%m%d%H")
    bucket.put(f"nwps/v1/hfo/{older}/51003.json", doc_for(older))
    newer = (datetime.strptime(RUN, "%Y%m%d%H") + timedelta(hours=12)).strftime("%Y%m%d%H")
    bucket.put("nwps/v1/latest.json", pointer(runs=(newer, RUN, older), stations=("51004",)))
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    cycle = A._parse_nwps_uncached("51003")[0]
    assert cycle == f"Cycle : NOAA NWPS {RUN[:8]} {RUN[8:]} UTC (NWS HFO)"          # newest first among the kept runs
    assert not any(f"hfo/{newer}/" in k for k in bucket.asked)                    # the newest does not list it
    A._NWPS_PTR.update(ts=0.0, doc=None, failed=0.0)
    bucket.put("nwps/v1/latest.json", pointer(runs=(RUN, older)))
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", b"<html>not json")       # listed but refused: the run before
    assert A._parse_nwps_uncached("51003")[0] == f"Cycle : NOAA NWPS {older[:8]} {older[8:]} UTC (NWS HFO)"
    bucket.fail.add(f"nwps/v1/hfo/{RUN}/51003.json")                  # the bucket not answering: no older run asked
    bucket.asked.clear()                                              # (G41 A P3-4)
    assert A._parse_nwps_uncached("51003")[-1] == "Could not reach the NWPS forecast (NOAA NWPS)"
    assert not any(f"hfo/{older}/" in k for k in bucket.asked)
    bucket.fail.clear()
    bucket.objects.pop(f"nwps/v1/hfo/{older}/51003.json")
    err = A._parse_nwps_uncached("51003")[-1]
    assert err == "No NWPS forecast found for 51003"


@pytest.mark.parametrize("change", [
    {"station": "51004"}, {"run": "2026102700"}, {"office": "mtr"}, {"format": "nwps-v2"},
    {"hours": list(range(146))}, {"hours": [1, 0] + list(range(2, 145))}, {"hs": [9e9] * 145},
    {"hs": [True] * 145}, {"parts": [[[1.0, 10.0, 360]]] * 145}, {"parts": [[[1.0, 10.0]]] * 145},
    {"parts": [[[1.0, 10.0, 90]]] * 7 * 145}, {"parts": [[[1.0, 10.0, 90]] * 7] * 145},
    {"parts": [[[float("nan"), 10.0, 90]]] * 145},
    {"hours": [0, 0] + list(range(2, 145))}, {"hours": list(range(1, 146))},       # G41 A P3-6: S14, S15
    {"parts": [[[1.0, 40.5, 90]]] * 145},                                          # S18: Tp over 40 s
])
def test_a_file_must_be_the_stations_the_runs_and_sane(bucket, change):
    bad = doc_for(RUN)
    if change.get("run"):
        bad["run"] = change["run"]
    else:
        bad.update(change)
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", json.dumps(bad, allow_nan=True).encode())
    assert A._parse_nwps_uncached("51003")[-1] == "No NWPS forecast found for 51003"


# ------------------------------------------------------------------------------------------------ the pointer

def test_no_bucket_a_bad_pointer_and_the_last_good_one(bucket, monkeypatch):
    assert A._parse_nwps_uncached("13130")[-1] == "No NWPS forecast for 13130"
    bucket.put("nwps/v1/hfo/" + RUN + "/51003.json", DOC)
    for bad in ({"offices": {}}, {"format": "nwps-pointer-v1", "offices": {"HFO": {}}},
                {"format": "nwps-pointer-v1", "offices": {"hfo": {"run": "2026", "runs": [], "stations": []}}}, b"{"):
        A._NWPS_PTR.update(ts=0.0, good=0.0, doc=None, failed=0.0)   # a minute later each time
        bucket.put("nwps/v1/latest.json", bad)
        assert A._parse_nwps_uncached("51003")[-1] == "Could not reach the NWPS forecast (NOAA NWPS)"
    assert A._nwps_health()["last_error"]
    bucket.put("nwps/v1/latest.json", pointer())
    A._NWPS_PTR.update(ts=0.0)
    assert A._parse_nwps_uncached("51003")[-1] == "Could not reach the NWPS forecast (NOAA NWPS)"   # within the minute
    A._NWPS_PTR.update(ts=0.0, failed=0.0)                            # a minute later
    assert A._parse_nwps_uncached("51003")[-1] is None                # read, then kept
    bucket.fail.add("nwps/v1/latest.json")
    A._NWPS_PTR.update(ts=0.0)
    assert A._parse_nwps_uncached("51003")[-1] is None                # the last good pointer serves
    A._NWPS_PTR.update(ts=0.0, good=0.0)
    assert A._parse_nwps_uncached("51003")[-1] == "Could not reach the NWPS forecast (NOAA NWPS)"   # past 6 h
    bucket.fail.discard("nwps/v1/latest.json")
    A._NWPS_PTR.update(ts=0.0, good=0.0, doc=None, failed=0.0)
    bucket.put("nwps/v1/latest.json", pointer(office="mtr", stations=("46026",)))
    assert A._parse_nwps_uncached("51003")[-1] == "No NWPS forecast found for 51003"   # its office has no run
    monkeypatch.delenv("POINTS_ROOT")
    monkeypatch.setattr(A, "_points_root", lambda: "")
    assert A._parse_nwps_uncached("51003")[-1] == A.NWPS_UNAVAILABLE


def test_the_pointer_is_asked_once_per_ttl(bucket):
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    A._parse_nwps_uncached("51003")
    A._parse_nwps_uncached("51003")
    assert bucket.asked.count("nwps/v1/latest.json") == 1
    h = A._nwps_health()
    assert h["stations"] == 112 and h["offices"] == 1 and h["pointer_age_s"] is not None and h["last_error"] is None


# ------------------------------------------------------------------------------------------------- the payload

def test_the_payload_of_an_nwps_station(bucket):
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    bucket.winds[(None, None)] = gfs_winds(datetime.strptime(RUN, "%Y%m%d%H"), 400)
    p = A.compute_forecast_payload("51003", None, "US", "NWPS", compact=True)
    assert p["error"] is None and p["model"] == "NWPS" and p["nwps_available"] is True and p["swan_available"] is False
    assert p["wind_complete"] is True and len(p["graph_data"]["labels"]) == 145 and p["tz_label"] == "Pacific/Honolulu"
    assert "NOAA NWPS" in p["graph_header"]["cycle"] and p["table_html"] and p["lat"] is not None
    q = A.compute_forecast_payload("51003", "UTC", "Metric", "NWPS", compact=True)
    assert q["tz_label"] == "UTC" and q["error"] is None
    assert A.compute_forecast_payload("13130", None, "US", "NWPS")["nwps_available"] is False


def test_healthz_reports_nwps(monkeypatch):
    with A.app.test_client() as c:
        body = c.get("/healthz").get_json()
    assert set(body["nwps"]) == {"stations", "pointer_age_s", "offices", "last_error"}


# ------------------------------------------------------------------------------- G41 fixes (plan section 59, step 8)

def ago(hours):
    return datetime.strptime(RUN, "%Y%m%d%H") + timedelta(hours=hours)


def test_the_location_names_the_nwps_point_of_the_file_else_the_station_files(bucket):
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", doc_for(RUN, point={"lat": 19.5, "lon": 199.75}))
    assert A._parse_nwps_uncached("51003")[1] == "Location : 51003 (19.50N 160.25W)"
    A._FORECAST_CACHE.clear()
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", doc_for(RUN, point="x"))
    assert A._parse_nwps_uncached("51003")[1] == "Location : 51003 (19.29N 160.57W)"   # nwps_stations.json's
    p = A.compute_forecast_payload("51003", None, "US", "NWPS", compact=True)
    assert p["graph_header"]["location"] == "51003 (19.29N 160.57W)"
    assert (p["lat"], p["lon"]) == (A.load_station_coords()["51003"]["lat"], A.load_station_coords()["51003"]["lon"])


@pytest.mark.parametrize("bad", ["2026139999", "2026022912", "20261028", "2026102806x"])
def test_a_pointer_run_must_be_a_date_in_run_or_runs(bucket, bad):
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    for p in (pointer(runs=(bad,), run=bad), pointer(runs=(RUN, bad))):
        A._NWPS_PTR.update(ts=0.0, good=0.0, doc=None, failed=0.0)
        bucket.put("nwps/v1/latest.json", p)
        assert A._parse_nwps_uncached("51003")[-1] == "Could not reach the NWPS forecast (NOAA NWPS)"
    assert A._nwps_run_ok("2026102806") and not A._nwps_run_ok(None) and not A._nwps_run_ok(2026102806)


def test_at_most_eight_kept_runs_are_tried(bucket):
    start = datetime.strptime(RUN, "%Y%m%d%H")
    runs = [(start - timedelta(hours=6 * i)).strftime("%Y%m%d%H") for i in range(50)]
    bucket.put("nwps/v1/latest.json", pointer(runs=runs))
    assert A._parse_nwps_uncached("51003")[-1] == "No NWPS forecast found for 51003"
    files = [k for k in bucket.asked if k.endswith("/51003.json")]
    assert len(files) == A._NWPS_MAX_RUNS == 8 and files[0].endswith(f"/{runs[0]}/51003.json")


def test_the_run_loop_stops_when_the_time_is_up(bucket, monkeypatch):
    bucket.put("nwps/v1/latest.json", pointer(runs=(RUN,)))
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    monkeypatch.setattr(A, "_NWPS_RUNS_BUDGET_S", -1)
    assert A._parse_nwps_uncached("51003")[-1] == "Could not reach the NWPS forecast (NOAA NWPS)"
    assert not any(k.endswith("/51003.json") for k in bucket.asked)


def test_a_failed_cold_pointer_read_waits_a_minute_and_a_held_one_the_ttl(bucket):
    bucket.fail.add("nwps/v1/latest.json")
    for _ in range(5):
        assert A._parse_nwps_uncached("51003")[-1] == "Could not reach the NWPS forecast (NOAA NWPS)"
    assert bucket.asked.count("nwps/v1/latest.json") == 1                         # G41 A P3-3
    A._NWPS_PTR["failed"] -= A._NWPS_PTR_NEG_TTL + 1                              # a minute later
    bucket.fail.clear()
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    assert A._parse_nwps_uncached("51003")[-1] is None
    assert bucket.asked.count("nwps/v1/latest.json") == 2
    A._NWPS_PTR["ts"] -= A._NWPS_PTR_TTL + 1                                      # past the TTL: one read, it fails
    bucket.fail.add("nwps/v1/latest.json")
    for _ in range(3):
        assert A._parse_nwps_uncached("51003")[-1] is None                        # the held pointer serves
    assert bucket.asked.count("nwps/v1/latest.json") == 3                         # asked again only after the TTL (S06)


def test_one_pointer_read_at_a_time(bucket, monkeypatch):
    import threading
    import time as _t
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    gate, reads = threading.Event(), []
    real = bucket.fetch

    def slow(url, max_bytes, **kw):
        if "latest.json" in url:
            reads.append(url)
            gate.wait(5)
        return real(url, max_bytes, **kw)
    monkeypatch.setattr(A, "_points_fetch", slow)
    out = []
    ts = [threading.Thread(target=lambda: out.append(A._nwps_pointer("https://models.test"))) for _ in range(4)]
    for t in ts:
        t.start()
    _t.sleep(0.3)
    gate.set()
    for t in ts:
        t.join(10)
    assert len(reads) == 1 and len(out) == 4 and all(o is not None for o in out)    # cold: they wait for the one read
    reads.clear()
    gate.clear()
    A._NWPS_PTR["ts"] -= A._NWPS_PTR_TTL + 1                                      # the TTL edge, a pointer held
    t0 = threading.Thread(target=lambda: A._nwps_pointer("https://models.test"))
    t0.start()
    _t.sleep(0.2)
    began = _t.monotonic()
    held = [A._nwps_pointer("https://models.test") for _ in range(3)]
    assert _t.monotonic() - began < 1 and all(h is not None for h in held)        # the others get the held one at once
    gate.set()
    t0.join(10)
    assert len(reads) == 1


def test_the_fork_hook_renews_both_nwps_locks():
    old = (A._NWPS_LOCK, A._NWPS_FETCH_LOCK)
    A._live_after_fork()
    assert A._NWPS_LOCK is not old[0] and A._NWPS_FETCH_LOCK is not old[1]          # S46


def fake_gfs(rows):
    return lambda sid, tz=None: ("Cycle : 20261028 06 UTC", "Location : 51003 (19.22N 160.82W)", None, rows,
                                 "Pacific/Honolulu", None)


def test_a_run_older_than_36_h_is_said_and_one_older_than_72_h_not_offered(bucket, monkeypatch):
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    rows = A._parse_nwps_uncached("51003")[3]
    monkeypatch.setattr(A, "parse_bull", fake_gfs(rows))
    p = A.compute_forecast_payload("51003", None, "US", "NWPS", compact=True)
    assert p["model"] == "NWPS" and p["nwps_age_hours"] == 10.0 and p["nwps_available"] is True
    bucket.clock["now"] = ago(40)
    p = A.compute_forecast_payload("51003", None, "US", "NWPS", compact=True)
    assert p["model"] == "NWPS" and p["nwps_age_hours"] == 40.0                    # the page's "· 40 h old"
    assert A.nwps_offered("51003") and A.resolve_model("51003", "NWPS") == "NWPS"
    bucket.clock["now"] = ago(72)
    assert A.nwps_offered("51003")                                                # 72 h: still offered
    bucket.clock["now"] = ago(73)                                                 # the pointer held says too old:
    assert not A.nwps_offered("51003") and A.resolve_model("51003", "NWPS") == "GFS"
    p = A.compute_forecast_payload("51003", None, "US", "NWPS", compact=True)
    assert p["model"] == "GFS" and p["nwps_available"] is False and "nwps_age_hours" not in p
    assert A.compute_forecast_payload("51003", None, "US", "GFS")["nwps_available"] is False
    assert not A.nwps_offered("13130") and A.nwps_offered("46026")                # another office: no run held -> offered


def test_a_stale_run_found_only_by_the_read_is_answered_as_gfs(bucket, monkeypatch):
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    rows = A._parse_nwps_uncached("51003")[3]
    A._NWPS_PTR.update(ts=0.0, good=0.0, doc=None, failed=0.0)                    # nothing held: offered
    A._FORECAST_CACHE.clear()
    monkeypatch.setattr(A, "parse_bull", fake_gfs(rows))
    bucket.clock["now"] = ago(80)
    assert A.nwps_offered("51003")
    p = A.compute_forecast_payload("51003", None, "US", "NWPS", compact=True)
    assert p["model"] == "GFS" and p["nwps_available"] is False and p["table_html"]


def test_a_stale_runs_wind_gap_is_kept_the_full_time(bucket):
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", DOC)
    data = A._parse_nwps_uncached("51003")                                       # no GFS winds at all
    assert A._forecast_entry_ttl(data) == A._WIND_NEG_TTL                         # 10 h old: may still heal
    p = A.compute_forecast_payload("51003", None, "US", "NWPS", compact=True)
    assert p["wind_complete"] is False
    bucket.clock["now"] = ago(49)                                                 # its early hours older than every
    assert A._forecast_entry_ttl(data) == A._FORECAST_CACHE_TTL                   # GFS winds run kept (G41 A P3-5)
    A._FORECAST_CACHE.clear()
    assert A.compute_forecast_payload("51003", None, "US", "NWPS", compact=True)["wind_complete"] is True
    gfs = ("Cycle : 20261028 06 UTC",) + data[1:]
    assert A._forecast_entry_ttl(gfs) == A._WIND_NEG_TTL                           # a GFS table: unchanged


def test_a_14z_run_back_fills_from_the_12z_gfs_table(bucket):
    run = "2026102814"
    run_dt = datetime.strptime(run, "%Y%m%d%H")
    bucket.put("nwps/v1/latest.json", pointer(runs=(run,)))
    bucket.put(f"nwps/v1/hfo/{run}/51003.json", doc_for(run))
    bucket.clock["now"] = run_dt + timedelta(hours=8)
    bucket.winds[(None, None)] = gfs_winds(run_dt + timedelta(hours=4), 380, speed=7.0)   # the latest: 18Z
    bucket.winds[("20261028", "12")] = gfs_winds(run_dt - timedelta(hours=2), 380, speed=3.0)
    rows = A._parse_nwps_uncached("51003")[3]
    assert [r[20] for r in rows[:6]] == [3.0] * 4 + [7.0] * 2                     # S20: the cycle holding 14Z


def test_a_six_part_hour_keeps_its_sixth_system(bucket):
    d = doc_for(RUN)
    d["parts"][0] = [[1.0, 14.0, 300], [0.9, 12.0, 200], [0.8, 10.0, 100], [0.7, 8.0, 50], [0.6, 6.0, 20],
                     [0.5, 4.0, 10]]
    bucket.put("nwps/v1/latest.json", pointer())
    bucket.put(f"nwps/v1/hfo/{RUN}/51003.json", d)
    row = A._parse_nwps_uncached("51003")[3][0]
    assert row[17:20] == [round(0.5 * 3.28084, 2), 4.0, 10]                       # S28


def test_the_error_path_says_where_nwps_is_offered(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("x")
    monkeypatch.setattr(A, "compute_forecast_payload", boom)
    with A.app.test_client() as c:
        assert c.get("/api/forecast?station=51003").get_json()["nwps_available"] is True     # S42
        assert c.get("/api/forecast?station=13130").get_json()["nwps_available"] is False


def test_the_wind_caches_hold_the_nwps_back_fills():
    assert A._GFS_WINDS_KEEP_RUNS == 8 and A._WIND_CACHE_MAX == 256               # G41 B-5


def test_the_age_is_the_run_that_can_serve_the_station(bucket):
    """A newer run that does not list the station does not make it fresh: its kept run decides (mutant M18)."""
    newer = (datetime.strptime(RUN, "%Y%m%d%H") + timedelta(hours=60)).strftime("%Y%m%d%H")
    A._NWPS_PTR.update(doc=pointer(runs=(newer, RUN), stations=("51004",)))   # held; 51003 only in RUN
    bucket.clock["now"] = ago(80)
    assert A._nwps_held_run("51003") == RUN and not A.nwps_offered("51003")
    assert A._nwps_held_run("51004") is None and A.nwps_offered("51004") is False    # not an NWPS station at all
    A._NWPS_PTR.update(doc=pointer(runs=(newer, RUN), stations=("51003",)))
    assert A._nwps_held_run("51003") == newer and A.nwps_offered("51003")

