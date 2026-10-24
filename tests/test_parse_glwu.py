"""The Great Lakes forecast tables (plan section 50, step 5): NOAA's GLWU bulletins, which the GLWU job copies into the
frames bucket, read by the site (never NOMADS): the shared bulletin parser on a real bulletin, the run's NDFD winds, the
newest kept run that has the station, the model rule, the station file, the payload, the cache, the overlay setting.

The bucket is a fake _points_fetch serving the real bulletin of run 2026102119 (tests/fixtures/glwu/45007.bull) and
synthetic pointers / manifests / winds; nothing reaches the network.
"""
import json
import os
import re
import sys
from datetime import datetime

import pytest

import app as A

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import glwu_fetch as G  # noqa: E402

BULL = open(os.path.join(HERE, "fixtures", "glwu", "45007.bull"), encoding="ascii").read()
RUN = "2026102119"


def bull_for(run, sid="45007", text=BULL):
    """The fixture bulletin as NOAA would write it for another run / station (the 01Z / 07Z hours space-padded)."""
    lat, lon = G.STATIONS[sid]
    text = re.sub(r"Location : 45007\s+\([^)]*\)", f"Location : {sid}   ({lat:.2f}N  {-lon:.2f}W)", text)
    return text.replace("Cycle    : 20261021 19 UTC", f"Cycle    : {run[:8]} {int(run[8:]):2d} UTC")


class Bucket:
    """A fake frames bucket for _points_fetch: objects by key, a request log, 404 for anything else."""

    def __init__(self):
        self.objects, self.asked, self.fail = {}, [], set()

    def put(self, key, value):
        self.objects[key] = value if isinstance(value, bytes) else (value.encode() if isinstance(value, str) else json.dumps(value).encode())

    def fetch(self, url, max_bytes, headers=None):
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


def winds_doc(run, stations):
    return {"run": run, "steps": list(range(150)), "units": {"speed": "m/s", "dir": "deg true, FROM"},
            "stations": stations, "source": "test"}


def publish(bucket, run, ids, winds=None, bulls=None):
    bucket.put(f"glwu/v1/{run}/manifest-x.json", {"run": run, "complete": True, "bulletins": {"dir": f"glwu/v1/{run}/bull", "ids": ids},
                                                  "winds": f"glwu/v1/{run}/winds.json"})
    for sid in (bulls if bulls is not None else ids):
        bucket.put(f"glwu/v1/{run}/bull/{sid}.bull", bull_for(run, sid))
    if winds is not None:
        bucket.put(f"glwu/v1/{run}/winds.json", winds_doc(run, winds))


def point_to(bucket, run):
    bucket.put("glwu/v1/latest.json", {"run": run, "manifest": f"glwu/v1/{run}/manifest-x.json", "complete": True, "encoding": "u8-linear-v2"})


@pytest.fixture
def bucket(monkeypatch):
    b = Bucket()
    monkeypatch.setattr(A, "_points_fetch", b.fetch)
    monkeypatch.setenv("POINTS_ROOT", "https://models.test")
    A._GLWU_RUN.update(ts=0.0, run=None, manifest=None, good=0.0)
    A._GLWU_WINDS.clear()
    with A._CACHE_LOCK:
        for k in [k for k in A._FORECAST_CACHE if k[2] == "GLWU"]:
            del A._FORECAST_CACHE[k]
    return b


def test_the_station_file_is_the_jobs_stations_named_and_zoned():
    """glwu_stations.json = glwu_fetch.STATIONS (ids and positions), each with a name and a US-Eastern / Central zone;
    kept out of the GFS station files (their pins and the point time-zone rule stay as reviewed)."""
    st = A.GLWU_STATIONS
    assert set(st) == set(G.STATIONS) and len(st) == 72
    for sid, v in st.items():
        assert (v["lat"], v["lon"]) == G.STATIONS[sid], sid
        assert v["name"].strip() and v["name"] == v["name"].strip(), sid
        assert v["tz"] in ("America/New_York", "America/Chicago", "America/Detroit", "America/Toronto"), (sid, v["tz"])
    assert st["45012"]["name"].startswith("EAST Lake Ontario") and st["IndDunesNP"]["name"] == "Indiana Dunes National Park, IN"
    assert st["45174"]["tz"] == "America/Chicago" and st["BARN6"]["tz"] == "America/New_York"
    for name in ("station_list.json", "station_coords.json", "station_timezones.json"):
        data = json.load(open(os.path.join(ROOT, name), encoding="utf-8"))
        assert not set(data) & set(st), name
    assert A.get_station_tz("45174") == "America/Chicago" and A.get_station_tz("51201") == "Pacific/Honolulu"


def test_the_station_list_and_the_map_carry_the_lake_stations(monkeypatch):
    monkeypatch.setattr(A, "load_station_metadata", lambda: {"45007": {"name": "NDBC's own text", "lat": 0, "lon": 0}})
    monkeypatch.setattr(A, "stations_data_cache", None)
    lst = A.get_station_list()
    ids = [s for s, _ in lst]
    listed = [str(s) for s in json.load(open(os.path.join(ROOT, "station_list.json"), encoding="utf-8"))]
    assert ids[:len(listed)] == listed
    assert ids[len(listed):] == list(A.GLWU_STATIONS) and len(ids) == len(listed) + 72
    assert dict(lst)["45007"] == A.GLWU_STATIONS["45007"]["name"], "the lake file's name, not NDBC's live text"
    data = {d["id"]: d for d in A.get_stations_data()}
    assert data["IndDunesNP"] == {"id": "IndDunesNP", "name": "Indiana Dunes National Park, IN", "lat": 41.63, "lon": -87.21}
    assert data["45007"]["name"] == A.GLWU_STATIONS["45007"]["name"]
    monkeypatch.setattr(A, "stations_data_cache", None)


def test_resolve_model():
    assert A.VALID_MODELS == ("GFS", "SWAN", "GLWU")
    for req in (None, "", "GFS", "swan", "GLWU", "nonsense"):
        assert A.resolve_model("45007", req) == "GLWU", req                  # a lake station: always its own model
    assert A.resolve_model("51201", "GLWU") == "GFS" and A.resolve_model("46026", "glwu") == "GFS"   # never off the lakes
    assert A.resolve_model("51201", "swan") == "SWAN" and A.resolve_model("46026", "SWAN") == "GFS"
    assert A.resolve_model("51201", None) == "GFS"


def test_the_shared_parser_reads_a_real_glwu_bulletin():
    """_parse_bull_text on run 2026102119's bulletin of 45007: 150 hourly rows from 19Z, the 23 columns, directions
    turned TO -> FROM, heights in feet, the run's winds joined by the hour; the zone from the lake file."""
    run_dt = datetime(2026, 10, 21, 19)
    wind = {run_dt: (4.1, 200), datetime(2026, 10, 21, 20): (4.3, 210)}
    c, loc, model_run, rows, tz, err = A._parse_bull_text(BULL, "45007", None, "20261021", "19", wind)
    assert err is None and tz == "America/New_York" and "20261021 19 UTC" in c and "45007" in loc
    assert model_run == "Model Run: Wednesday, October 21, 2026 03:00 PM"
    assert len(rows) == 150 and all(len(r) == 23 for r in rows)
    first = rows[0]
    assert first[:2] == ["Wednesday, October 21, 2026", "3:00 PM"]
    assert first[2] == round(0.21 * 3.28084, 2) and first[3] == 2.6 and first[4] == 192    # "0.21 2.6 12" (TO) -> 192 FROM
    assert first[20:22] == [4.1, 200] and rows[1][20:22] == [4.3, 210] and rows[2][20:22] == [None, None]
    assert first[-1] == round(0.21 * 3.28084, 2)
    assert rows[-1][:2] == ["Tuesday, October 27, 2026", "8:00 PM"]                         # +149 h
    # the GFS path's own call: the same rows (the extraction changed nothing)
    c2, _, _, rows2, tz2, _ = A._parse_bull_text(BULL, "45007", "UTC", "20261021", "19", {})
    assert rows2[0][:2] == ["Wednesday, October 21, 2026", "7:00 PM"] and tz2 == "UTC" and rows2[0][20:22] == [None, None]


def test_the_newest_run_with_the_station_and_its_winds(bucket):
    """The newest run when its manifest lists the station; else the kept runs before it, newest first (the 01Z / 07Z
    runs lost their bulletins to a job bug on 2026-10-22); the Cycle line names the run read; the winds of THAT run."""
    publish(bucket, "2026102119", ["45007", "BARN6"], winds={"45007": [[4.1, 200], [4.3, 210]] + [None] * 148})
    publish(bucket, "2026102201", [], winds={"45007": [[9.9, 90]] * 150}, bulls=[])
    point_to(bucket, "2026102201")
    c, loc, _, rows, tz, err = A.parse_glwu("45007")
    assert err is None and c == "Cycle : NOAA GLWU 20261021 19 UTC", c
    assert rows[0][20:22] == [4.1, 200] and rows[1][20:22] == [4.3, 210] and rows[2][20:22] == [None, None], "19Z's winds, not 01Z's"
    assert bucket.asked[:3] == ["glwu/v1/latest.json", "glwu/v1/2026102201/manifest-x.json", "glwu/v1/2026102119/bull/45007.bull"], bucket.asked
    # the newest run lists it: read from there, with its (space-padded 01Z) cycle rewritten
    publish(bucket, "2026102207", ["45007"], winds={"45007": [[7.0, 270]] * 150})
    point_to(bucket, "2026102207")
    A._GLWU_RUN.update(ts=0.0)
    with A._CACHE_LOCK:
        A._FORECAST_CACHE.pop(("45007", "", "GLWU"), None)
    c, _, model_run, rows, _, err = A.parse_glwu("45007")
    assert err is None and c == "Cycle : NOAA GLWU 20261022 07 UTC" and model_run == "Model Run: Thursday, October 22, 2026 03:00 AM"
    assert [k for k in bucket.asked if k.endswith(".bull")][-1] == "glwu/v1/2026102207/bull/45007.bull"
    # a station no kept run has: the error, after the newest and three runs before it
    bucket.asked.clear()
    c, _, _, rows, _, err = A._parse_glwu_uncached("N_OLCOTT")
    assert rows is None and err == "No Great Lakes forecast found for N_OLCOTT"
    assert [k for k in bucket.asked if k.endswith(".bull")] == [f"glwu/v1/{r}/bull/N_OLCOTT.bull" for r in ("2026102201", "2026102119", "2026102113")]


def test_a_bulletin_must_be_the_stations_and_the_runs(bucket):
    publish(bucket, RUN, ["45007"], winds={})
    bucket.put(f"glwu/v1/{RUN}/bull/45007.bull", bull_for(RUN, "BARN6"))            # another station's text under the name
    point_to(bucket, RUN)
    assert A._parse_glwu_uncached("45007")[5] == "No Great Lakes forecast found for 45007"
    bucket.put(f"glwu/v1/{RUN}/bull/45007.bull", bull_for("2026102113"))            # another run's
    assert A._parse_glwu_uncached("45007")[5] == "No Great Lakes forecast found for 45007"
    bucket.put(f"glwu/v1/{RUN}/bull/45007.bull", bull_for(RUN))
    assert A._parse_glwu_uncached("45007")[5] is None
    # "1" is not "11": an 01Z bulletin is not the 11Z run's
    assert A._glwu_bulletin("https://models.test", "2026102211", "45007") is None
    bucket.put("glwu/v1/2026102201/bull/45007.bull", bull_for("2026102201"))
    assert "Cycle : NOAA GLWU 20261022 01 UTC" in A._glwu_bulletin("https://models.test", "2026102201", "45007")
    assert A._glwu_bulletin("https://models.test", "2026102211", "45007") is None


def test_no_bucket_no_pointer_and_the_last_good_run(bucket, monkeypatch):
    monkeypatch.delenv("POINTS_ROOT")
    monkeypatch.delenv("MODEL_FRAMES_BASE", raising=False)
    assert A._parse_glwu_uncached("45007")[5] == A.GLWU_UNAVAILABLE
    monkeypatch.setenv("POINTS_ROOT", "https://models.test")
    assert A._parse_glwu_uncached("45007")[5] == "Could not reach the Great Lakes forecast (NOAA GLWU)"
    assert A._parse_glwu_uncached("51201")[5] == "No Great Lakes forecast for 51201"
    publish(bucket, RUN, ["45007"], winds={})
    point_to(bucket, RUN)
    assert A._parse_glwu_uncached("45007")[5] is None
    # the pointer becomes unreadable: the last good run serves (asked again after the TTL, not on every request)
    bucket.fail.add("glwu/v1/latest.json")
    A._GLWU_RUN["ts"] = 0.0
    assert A._parse_glwu_uncached("45007")[5] is None
    n = len(bucket.asked)
    assert A._parse_glwu_uncached("45007")[5] is None and "glwu/v1/latest.json" not in bucket.asked[n:]
    A._GLWU_RUN.update(ts=0.0, good=0.0)                                               # past _GLWU_RUN_KEEP
    assert A._parse_glwu_uncached("45007")[5] == "Could not reach the Great Lakes forecast (NOAA GLWU)"
    # a pointer that names another prefix or an incomplete run is not read
    bucket.fail.clear()
    bucket.put("glwu/v1/latest.json", {"run": RUN, "manifest": f"gfswave/0p25/v1/{RUN}/manifest-x.json", "complete": True})
    assert A._glwu_latest("https://models.test") == (None, None)
    bucket.put("glwu/v1/latest.json", {"run": RUN, "manifest": f"glwu/v1/{RUN}/manifest-x.json", "complete": False})
    assert A._glwu_latest("https://models.test") == (None, None)
    # winds that cannot be read leave the Wind column blank, never the table
    point_to(bucket, RUN)
    bucket.put(f"glwu/v1/{RUN}/winds.json", winds_doc("2026102113", {"45007": [[5.0, 90]] * 150}))   # another run's winds under the key
    A._GLWU_WINDS.clear()
    rows = A._parse_glwu_uncached("45007")[3]
    assert rows and all(r[20] is None for r in rows)


def test_the_payload_of_a_lake_station(bucket):
    publish(bucket, RUN, ["45007"], winds={"45007": [[4.1, 200]] + [None] * 149})
    point_to(bucket, RUN)
    p = A.compute_forecast_payload("45007", None, "US", "SWAN", compact=True)
    assert p["error"] is None and p["model"] == "GLWU" and p["swan_available"] is False
    assert p["wind_complete"] is True, "a run's winds are fixed: nothing to ask again for"
    assert (p["lat"], p["lon"]) == (42.67, -87.03) and p["tz_label"] == "America/New_York"
    assert p["graph_header"]["cycle"] == "NOAA GLWU 20261021 19 UTC" and len(p["graph_data"]["labels"]) == 150
    assert p["graph_data"]["sky"] and p["summary_html"] and "45007" in p["graph_header"]["location"]
    # the cache keeps it the full time although the Wind column has gaps
    entry = A._FORECAST_CACHE[("45007", "", "GLWU")]
    assert entry["ttl"] == A._FORECAST_CACHE_TTL
    assert A._forecast_entry_ttl(entry["data"]) == A._WIND_NEG_TTL, "(the GFS rule would have shortened it)"
    n = len(bucket.asked)
    assert A.compute_forecast_payload("45007", None, "Metric", "", compact=True)["error"] is None and len(bucket.asked) == n, "served from the cache"


def test_the_overlay_setting(monkeypatch):
    for env, want in (("1", True), ("0", False), ("", False), ("true", False)):
        monkeypatch.setenv("GLWU_OVERLAY", env)
        assert A._glwu_overlay_enabled() is want, env
    monkeypatch.setenv("MODEL_OVERLAYS", "1")
    monkeypatch.setenv("MODEL_FRAMES_BASE", "https://frames.example/gfswave/0p25/v1")
    monkeypatch.setattr(A, "get_station_tz", lambda sid: "Pacific/Honolulu")
    monkeypatch.setenv("GLWU_OVERLAY", "1")
    assert A._overlay_context("51201", None, None)["overlay_glwu"] is True
    monkeypatch.delenv("GLWU_OVERLAY")
    assert A._overlay_context("51201", None, None)["overlay_glwu"] is False


def test_g33_n3_a_newest_manifest_without_bulletins_tries_the_kept_runs(bucket):
    """G33 N3: a newest manifest with no bulletins block (only a partial build lacks it) lists no station: the kept runs
    are tried and the table is served, never "Could not reach"."""
    older = "2026102113"
    publish(bucket, older, ["45007"], winds={"45007": [[5.0, 270]] * 150})
    bucket.put(f"glwu/v1/{RUN}/manifest-x.json", {"run": RUN, "complete": True})          # no bulletins block
    point_to(bucket, RUN)
    cycle, location, model_run, rows, tz, err = A.parse_glwu("45007")
    assert err is None and rows and len(rows) == 150
    assert "20261021 13 UTC" in cycle


def test_g33_q6_a_wind_cell_of_another_length_is_dropped(bucket):
    """G33 Q6: only [speed, direction] pairs join the Wind column; a 3-long (or 1-long) cell leaves that hour blank."""
    cells = [[5.0, 270]] * 150
    cells[0] = [5.0, 270, 1]
    cells[1] = [5.0]
    publish(bucket, RUN, ["45007"], winds={"45007": cells})
    point_to(bucket, RUN)
    winds = A._glwu_winds("https://models.test", RUN, "45007")
    run_dt = datetime.strptime(RUN, "%Y%m%d%H")
    assert run_dt not in winds and run_dt.replace(hour=20) not in winds
    assert len(winds) == 148
