"""The GFS spectral table on the site (plan section 77, step 4): the job's product (gfswave/spectral/v1: one binary per
run, an index and the pointer) read from the frames bucket in the background, held in memory for two runs and served as
the GFS table when its run is the site's run or newer; NOAA's own split of the site's run served meanwhile, labelled;
"GFSB" = NOAA's split as a model button; the flag off = today's behaviour.

The table is built here with the job's own encoder (tools/model_frames/gfs_spectral.py), so the reader is tested
against the real format. The bucket is a fake; the bulletin is the repo's real fixture; nothing reaches the network.
"""
import calendar
import hashlib
import json
import os
import sys
import threading
from datetime import datetime, timedelta, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import app as A  # noqa: E402
import gfs_spectral as G  # noqa: E402

BASE = "https://models.test/gfswave/0p25/v1"
BULL = open(os.path.join(HERE, "fixtures", "gfswave_51201_2026100112.bull"), encoding="utf-8").read()
RUN_A, RUN_B, RUN_C = "2026100112", "2026100118", "2026100200"


def part(hs, tp, d, ws=False, label=1):
    return {"hs": hs, "tp": tp, "dir": d, "ws": ws, "label": label}


def station_rows(run, seed=0, steps=range(385)):
    """{datetime: (hs_total, parts)} of a synthetic station: two swells + a wind sea, the numbers moving with the hour."""
    base = datetime.strptime(run, "%Y%m%d%H")
    rows = {}
    for h in steps:
        hs1, hs2 = 1.5 + (h % 7) * 0.1 + seed, 0.6 + (h % 5) * 0.05
        rows[base + timedelta(hours=h)] = (round((hs1 ** 2 + hs2 ** 2 + 0.3 ** 2) ** 0.5, 2),
                                          [part(hs1, 14.2, 327, label=1), part(hs2, 8.9, 20, label=2),
                                           part(0.3, 4.1, 60, ws=True, label=3)])
    return rows


def table(run, stations):
    """(index bytes, spectral.bin bytes) of {sid: rows}, encoded by the job's encoder and laid out like its index."""
    run_dt = datetime.strptime(run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    body, slots = bytearray(), {}
    for sid, rows in stations.items():
        rec, present = G.encode_steps(rows, run_dt)
        assert present == len(rows)
        slots[sid] = len(slots)
        body += rec
    doc = {"format": G.FORMAT, "run": run, "steps": list(range(G.STEPS)), "stations": slots, "count": len(slots),
           "record_bytes": G.RECORD_BYTES, "missing": G.MISSING, "parts": G.PARTS, "rules": {"version": "v2"},
           "sha256": hashlib.sha256(body).hexdigest()}
    return json.dumps(doc).encode(), bytes(body)


class Bucket:
    def __init__(self):
        self.objects, self.asked = {}, []

    def put_run(self, run, stations):
        idx, body = table(run, stations)
        self.objects[f"gfswave/spectral/v1/{run}/index.json"] = idx
        self.objects[f"gfswave/spectral/v1/{run}/spectral.bin"] = body
        return idx, body

    def point(self, run):
        self.objects["gfswave/spectral/v1/latest.json"] = json.dumps({"run": run, "index": f"gfswave/spectral/v1/{run}/index.json"}).encode()

    def fetch(self, url, max_bytes, headers=None, retry_dropped=False):
        key = url.split("models.test/", 1)[1]
        self.asked.append(key)
        if key not in self.objects:
            raise A.HTTPStatusError(404)
        if isinstance(self.objects[key], Exception):
            raise self.objects[key]
        body = self.objects[key]
        if len(body) > max_bytes:
            raise A.BodyTooLarge("too large")
        return body

    def capped_get(self, url, max_bytes, headers, timeout, max_s, session=None, byte_range=None, retry_dropped=False):
        assert timeout == A._GSPEC_TABLE_TIMEOUT and max_s == A._GSPEC_TABLE_MAX_S and retry_dropped is True
        return self.fetch(url, max_bytes)


class Clock:
    def __init__(self, t=1_700_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def reset():
    with A._GSPEC_LOCK:
        A._GSPEC["runs"].clear()
        A._GSPEC.update(ptr_run=None, ptr_ts=0.0, failed=0.0, failing_since=0.0, error=None, thread=None, started=0,
                        fetched=0, spectral=0, fallback=0, prior=0, listed=None)
    with A._CACHE_LOCK:
        A._FORECAST_CACHE.clear()
        A._FORECAST_INFLIGHT.clear()


@pytest.fixture
def world(monkeypatch):
    """The flag on; a bucket with run A for 51201 and 41113 (41113 lacking every 50th step); the site's run = A; the
    bulletin = the repo's fixture (run A); the winds a fake that records the run asked for."""
    b = Bucket()
    b.put_run(RUN_A, {"51201": station_rows(RUN_A), "41113": station_rows(RUN_A, seed=0.5, steps=[h for h in range(385) if h % 50])})
    b.point(RUN_A)
    monkeypatch.setattr(A, "_points_fetch", b.fetch)
    monkeypatch.setattr(A, "_capped_get", b.capped_get)
    monkeypatch.setenv("MODEL_FRAMES_BASE", BASE)
    monkeypatch.delenv("POINTS_ROOT", raising=False)
    monkeypatch.setattr(A, "GFS_SPECTRAL", True)
    monkeypatch.setattr(A, "get_station_list", lambda: [("51201", "Waimea Bay, HI"), ("41113", "Cape Canaveral"),
                                                        ("46001", "Gulf of Alaska"), ("45007", "Lake Michigan")])
    site = {"run": (RUN_A[:8], RUN_A[8:])}
    monkeypatch.setattr(A, "get_latest_run", lambda: site["run"])
    fetched = []

    def fetch_bull(date_str, run_str, sid):
        fetched.append((date_str, run_str, sid))
        return BULL.replace("51201", sid) if sid in ("51201", "41113", "46001") else (None, "no bulletin")[0], None
    monkeypatch.setattr(A, "_fetch_bulletin", lambda d, r, s: ((BULL.replace("51201", s), None) if s in ("51201", "41113", "46001")
                                                                else (None, f"Could not download .bull for {s}")) if not fetched.append((d, r, s)) else None)
    winds = []

    def wind(sid, date_str=None, run_str=None, keep_s=None):
        winds.append((sid, date_str, run_str))
        base = datetime.strptime((date_str or RUN_A[:8]) + (run_str or RUN_A[8:]), "%Y%m%d%H")
        return {base + timedelta(hours=h): (5.0 + h % 3, 45.0) for h in range(385)}
    monkeypatch.setattr(A, "get_station_wind", wind)
    clock = Clock()
    monkeypatch.setattr(A.time, "time", clock)
    monkeypatch.setattr(A, "_nwps_utcnow", lambda: datetime(2026, 10, 1, 18))
    reset()
    yield b, site, fetched, winds, clock
    reset()


# ------------------------------------------------------------------------------------- the format and the reader

def test_the_site_decodes_the_jobs_record_and_pins_its_constants():
    assert (A._GSPEC_STEPS, A._GSPEC_PARTS, A._GSPEC_VALUES, A._GSPEC_RECORD, A._GSPEC_MISSING, A._GSPEC_FLAG_SHIFT) == \
        (G.STEPS, G.PARTS, G.VALUES_PER_STEP, G.RECORD_BYTES, G.MISSING, G.FLAG_SHIFT)
    assert A.GFSSPEC_FORMAT == G.FORMAT and A.GFSSPEC_PREFIX == G.PREFIX
    run_dt = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
    rows = station_rows(RUN_A, steps=[0, 1, 384])
    rows[datetime(2026, 10, 1, 13)] = (2.0, [part(2.0, 12.0, 359, ws=True)] + [part(0.2 + k * 0.01, 5.0, k, label=k) for k in range(8)])
    rec, present = G.encode_steps(rows, run_dt)
    assert present == 3
    ours, theirs = A._gspec_decode(rec), G.decode_record(rec)
    assert ours == theirs
    assert ours[2] is None and ours[0][1][0] == (1.5, 14.2, 327, False) and ours[1][1][0] == (2.0, 12.0, 359, True)
    assert len(ours[1][1]) == 6, "the six most powerful parts"
    assert len(ours[384][1]) == 3


def test_the_table_check_refuses_every_damage():
    idx, body = table(RUN_A, {"51201": station_rows(RUN_A, steps=[0, 1])})
    doc = json.loads(idx)
    assert A._gspec_check(doc, body, RUN_A) is None
    assert "not a gfsspec-v1" in A._gspec_check(dict(doc, format="gfswinds-v1"), body, RUN_A)
    assert "not a gfsspec-v1" in A._gspec_check(doc, body, RUN_B)
    for k, v in (("steps", list(range(384))), ("record_bytes", 1540), ("missing", 0), ("parts", 5)):
        assert A._gspec_check(dict(doc, **{k: v}), body, RUN_A) == "another layout", k
    assert A._gspec_check(dict(doc, count=2), body, RUN_A) == "a bad station list"
    assert A._gspec_check(dict(doc, stations={"51201": 1}), body, RUN_A) == "bad slots"
    assert "spectral.bin is" in A._gspec_check(doc, body + b"\0", RUN_A)
    assert A._gspec_check(doc, body[:-1] + bytes([body[-1] ^ 1]), RUN_A) == "spectral.bin does not match its index"
    assert A._gspec_check("nonsense", body, RUN_A)


def test_the_refresh_holds_the_pointers_run_and_two_runs_at_most(world):
    b, site, *_ = world
    assert A._gspec_refresh(A._gspec_root()) is True
    assert A._gspec_health()["held"] == [RUN_A] and A._gspec_health()["stations"] == [2]
    assert b.asked == [f"gfswave/spectral/v1/latest.json", f"gfswave/spectral/v1/{RUN_A}/index.json", f"gfswave/spectral/v1/{RUN_A}/spectral.bin"]
    A._gspec_refresh(A._gspec_root())                        # the same pointer: nothing fetched again
    assert b.asked[3:] == ["gfswave/spectral/v1/latest.json"]
    b.put_run(RUN_B, {"51201": station_rows(RUN_B)}); b.point(RUN_B)
    A._gspec_refresh(A._gspec_root())
    assert A._gspec_health()["held"] == [RUN_A, RUN_B]
    b.put_run(RUN_C, {"51201": station_rows(RUN_C)}); b.point(RUN_C)
    A._gspec_refresh(A._gspec_root())
    assert A._gspec_health()["held"] == [RUN_B, RUN_C], "the oldest let go"
    b.point(RUN_A)                                           # a pointer that went back: not fetched (it could never be kept)
    n = len(b.asked)
    A._gspec_refresh(A._gspec_root())
    assert A._gspec_health()["held"] == [RUN_B, RUN_C] and A._gspec_health()["pointer_run"] == RUN_A
    assert A._gspec_health()["fetched"] == 3 and A._gspec_health()["last_error"] is None
    assert b.asked[n:] == ["gfswave/spectral/v1/latest.json"], "the body of a run behind the held ones is never asked (G48 A P3-2)"


def test_a_damaged_or_missing_table_is_refused_and_remembered(world):
    b, site, _, _, clock = world
    good = b.objects[f"gfswave/spectral/v1/{RUN_A}/spectral.bin"]
    b.objects[f"gfswave/spectral/v1/{RUN_A}/spectral.bin"] = good[:-1] + bytes([good[-1] ^ 1])
    assert A._gspec_refresh(A._gspec_root()) is False
    h = A._gspec_health()
    assert h["held"] == [] and "does not match" in h["last_error"] and h["pointer_run"] == RUN_A
    assert A._gspec_due(clock()) is False
    clock.t += A._GSPEC_PTR_NEG_TTL
    assert A._gspec_due(clock()) is True, "asked again after the negative TTL"
    b.objects[f"gfswave/spectral/v1/{RUN_A}/spectral.bin"] = good
    assert A._gspec_refresh(A._gspec_root()) is True and A._gspec_health()["held"] == [RUN_A]
    assert A._gspec_due(clock()) is False
    clock.t += A._GSPEC_PTR_TTL
    assert A._gspec_due(clock()) is True
    del b.objects["gfswave/spectral/v1/latest.json"]
    assert A._gspec_refresh(A._gspec_root()) is False and A._gspec_health()["held"] == [RUN_A], "a failed read keeps the held run"
    b.objects["gfswave/spectral/v1/latest.json"] = b"{\"run\": \"nonsense\"}"
    assert A._gspec_refresh(A._gspec_root()) is False and "unsupported" in A._gspec_health()["last_error"]
    b.point(RUN_B)
    b.objects[f"gfswave/spectral/v1/{RUN_B}/index.json"] = b"x" * (A._GSPEC_MAX["index"] + 1)
    assert A._gspec_refresh(A._gspec_root()) is False and A._gspec_health()["held"] == [RUN_A]


def test_the_scheduler_pass_starts_one_read_at_a_time_and_only_when_due(world, monkeypatch):
    b, site, _, _, clock = world
    started = []

    class FakeThread:
        def __init__(self, target=None, args=(), name=None, daemon=None):
            self.target, self.args, self.alive = target, args, True
            started.append(name)

        def start(self):
            pass

        def is_alive(self):
            return self.alive

        def run(self):
            self.target(*self.args)
            self.alive = False
    monkeypatch.setattr(A.threading, "Thread", FakeThread)
    assert A._gspec_tick() is True and started == ["gspec-fetch"]
    assert A._gspec_tick() is False, "one read at a time"
    t = A._GSPEC["thread"]
    t.run()
    assert A._gspec_health()["held"] == [RUN_A]
    assert A._gspec_tick() is False, "not due before the TTL"
    clock.t += A._GSPEC_PTR_TTL
    assert A._gspec_tick() is True and A._GSPEC["started"] == 2
    monkeypatch.setattr(A, "GFS_SPECTRAL", False)
    clock.t += A._GSPEC_PTR_TTL
    A._GSPEC["thread"].run()
    assert A._gspec_tick() is False, "off: nothing read"


def test_the_fork_hook_renews_the_lock_and_forgets_the_thread():
    old = A._GSPEC_LOCK
    A._GSPEC["thread"] = object()
    A._live_after_fork()
    assert A._GSPEC_LOCK is not old and A._GSPEC["thread"] is None


# ------------------------------------------------------------------------------------- the table served

def expect_rows(sid, run, local_tz="Pacific/Honolulu"):
    """What the spectral rows of a synthetic station read like (feet, FROM, ranked by Hs^2 x Tp)."""
    import pytz
    tz = pytz.timezone(local_tz)
    return A.rank_rows(A.gspec_rows(A._gspec_decode(table(run, {sid: station_rows(run)})[1][:G.RECORD_BYTES]),
                                    datetime.strptime(run, "%Y%m%d%H"), tz,
                                    {datetime.strptime(run, "%Y%m%d%H") + timedelta(hours=h): (5.0 + h % 3, 45.0) for h in range(385)}))


def test_the_spectral_rows_serve_when_the_held_run_is_the_sites_or_newer(world):
    b, site, fetched, winds, _ = world
    A._gspec_refresh(A._gspec_root())
    data, split, note = A.parse_gfs_spectral("51201", None)
    cycle, location, model_run, rows, tz, err = data
    assert err is None and split == "spectral" and note is None
    assert cycle == f"Cycle : {RUN_A[:8]} {RUN_A[8:]} UTC" and location == "Location : 51201 (21.67N 158.12W)"
    assert model_run is None and tz == "Pacific/Honolulu"
    assert len(rows) == 385 and all(len(r) == 23 for r in rows)
    assert rows == expect_rows("51201", RUN_A)
    r0 = rows[0]
    assert r0[0] in ("Thursday, October 1, 2026", "Thursday, October 01, 2026") and r0[1] == "2:00 AM"   # 12Z = 2 AM HST (the day
    #                                                                               zero-padded on Windows, as nwps_rows does)
    assert (r0[2], r0[3], r0[4]) == (round(1.5 * 3.28084, 2), 14.2, 327), "the most powerful swell first, in feet, FROM"
    assert (r0[5], r0[6], r0[7]) == (round(0.6 * 3.28084, 2), 8.9, 20)
    assert (r0[8], r0[9], r0[10]) == (round(0.3 * 3.28084, 2), 4.1, 60) and r0[11:20] == [None] * 9
    assert r0[20:22] == [5.0, 45] and r0[22] == round(1.64 * 3.28084, 2)
    assert winds and all(w == ("51201", RUN_A[:8], RUN_A[8:]) for w in winds), "the wind of the SPECTRAL run"
    assert fetched == [], "no bulletin asked for"
    # a station whose record lacks steps: those hours are left out
    data, _, _ = A.parse_gfs_spectral("41113", "UTC")
    assert len(data[3]) == 385 - len([h for h in range(385) if h % 50 == 0]) and data[4] == "UTC"
    # the site's run older than the held one: still ours
    site["run"] = ("20261001", "06")
    data, split, note = A.parse_gfs_spectral("51201", None)
    assert split == "spectral" and data[0] == f"Cycle : {RUN_A[:8]} {RUN_A[8:]} UTC"
    assert A._gspec_health()["served"] == {"spectral": 3, "fallback": 0, "prior": 0}


def test_the_payload_carries_the_split_and_noaas_split_is_the_gfsb_button(world):
    b, site, fetched, winds, _ = world
    A._gspec_refresh(A._gspec_root())
    out = A.compute_forecast_payload("51201", None, "US", "GFS", compact=True)
    assert out["model"] == "GFS" and out["split"] == "spectral" and "split_note" not in out
    assert out["gfsb_available"] is True and out["error"] is None and out["wind_complete"] is True
    assert out["graph_header"]["cycle"] == f"{RUN_A[:8]} {RUN_A[8:]} UTC" and "21.67N 158.12W" in out["graph_header"]["location"]
    assert "Swell" in out["table_html"] and out["summary_html"]
    noaa = A.compute_forecast_payload("51201", None, "US", "GFSB", compact=True)
    assert noaa["model"] == "GFSB" and noaa["split"] == "noaa" and "split_note" not in noaa and noaa["gfsb_available"] is True
    assert fetched == [(RUN_A[:8], RUN_A[8:], "51201")], "the bulletin, once"
    assert noaa["graph_data"]["height"]["s1"] != out["graph_data"]["height"]["s1"], "two different splits of the run"
    assert noaa["graph_header"]["cycle"] == out["graph_header"]["cycle"]
    # a request for the button's model where it is not offered: a point, a lake station, an unlisted station
    assert A.resolve_model("pt_21667N_158054W", "GFSB") == "GFS" and A.resolve_model("45007", "gfsb") == "GLWU"
    assert A.resolve_model("NW-HFO51", "GFSB") == "GFS" and A.resolve_model("51201", "gfsb") == "GFSB"
    assert A.resolve_model("51201", "GFSB", nwps_ok=False, swan_ok=False) == "GFSB"
    assert A.VALID_MODELS == ("GFS", "SWAN", "GLWU", "NWPS", "GFSB", "ECMWF", "ICON", "AIFS", "NWS")
    unlisted = A.compute_forecast_payload("NW-HFO51", None, "US", "GFS", compact=True)
    assert unlisted["gfsb_available"] is False and "split" not in unlisted, "not covered: the bulletin as before"


def test_the_prior_runs_spectral_table_serves_meanwhile_with_the_note(world):
    """Section 77c (owner, 2026-11-02): while the site's run R is newer than the held spectral run S, S's own table
    serves (its own wind, its own Cycle line) with "<R hour> UTC run spectral analysis in progress" (one cycle behind)
    or "... delayed"; past _GSPEC_PRIOR_MAX_H NOAA's split of R serves instead; the new run serves the moment it is
    held (the key carries S and R)."""
    b, site, fetched, winds, clock = world
    A._gspec_refresh(A._gspec_root())
    site["run"] = (RUN_B[:8], RUN_B[8:])                   # the site moved on; the job one cycle behind
    data, split, note = A.parse_gfs_spectral("51201", None)
    assert split == "spectral" and note == f"{RUN_B[8:]} UTC run spectral analysis in progress"
    assert data[0] == f"Cycle : {RUN_A[:8]} {RUN_A[8:]} UTC" and data[-1] is None, "the prior run's own table"
    assert winds[-1] == ("51201", RUN_A[:8], RUN_A[8:]), "the wind of the run shown"
    assert fetched == [], "no bulletin asked for"
    out = A.compute_forecast_payload("51201", None, "US", "GFS", compact=True)
    assert out["model"] == "GFS" and out["split"] == "spectral" and out["split_note"] == f"{RUN_B[8:]} UTC run spectral analysis in progress"
    assert out["graph_header"]["cycle"] == f"{RUN_A[:8]} {RUN_A[8:]} UTC"
    site["run"] = (RUN_C[:8], RUN_C[8:])                   # two cycles behind: still ours, "delayed"
    data, split, note = A.parse_gfs_spectral("51201", None)
    assert split == "spectral" and note == f"{RUN_C[8:]} UTC run spectral analysis delayed"
    late = (datetime.strptime(RUN_A, "%Y%m%d%H") + timedelta(hours=A._GSPEC_PRIOR_MAX_H + 6)).strftime("%Y%m%d%H")
    site["run"] = (late[:8], late[8:])                      # past the limit: NOAA's split of the site's run
    data, split, note = A.parse_gfs_spectral("51201", None)
    assert split == "noaa" and note == "Swell split: NOAA's" and fetched[-1] == (late[:8], late[8:], "51201")
    edge = (datetime.strptime(RUN_A, "%Y%m%d%H") + timedelta(hours=A._GSPEC_PRIOR_MAX_H)).strftime("%Y%m%d%H")
    site["run"] = (edge[:8], edge[8:])                      # exactly at the limit: still ours
    assert A.parse_gfs_spectral("51201", None)[1:] == ("spectral", f"{edge[8:]} UTC run spectral analysis delayed")
    # the job catches up: the new run's table at once, no note
    site["run"] = (RUN_C[:8], RUN_C[8:])
    b.put_run(RUN_C, {"51201": station_rows(RUN_C)}); b.point(RUN_C)
    A._gspec_refresh(A._gspec_root())
    data, split, note = A.parse_gfs_spectral("51201", None)
    assert split == "spectral" and note is None and data[0] == f"Cycle : {RUN_C[:8]} {RUN_C[8:]} UTC"
    assert winds[-1] == ("51201", RUN_C[:8], RUN_C[8:])
    h = A._gspec_health()["served"]
    assert h["prior"] == 4 and h["fallback"] == 1, h


def test_nothing_held_says_shortly_until_the_reads_have_failed_half_an_hour(world):
    b, site, fetched, _, clock = world
    data, split, note = A.parse_gfs_spectral("51201", None)
    assert split == "noaa" and note == "Swell split: NOAA's (ours arrives shortly)", "a cold process: the fetch is coming"
    del b.objects["gfswave/spectral/v1/latest.json"]
    A._gspec_refresh(A._gspec_root())
    clock.t += A._GSPEC_FAILING_SHORTLY_S - 1
    with A._CACHE_LOCK:
        A._FORECAST_CACHE.clear()
    assert A.parse_gfs_spectral("51201", None)[2] == "Swell split: NOAA's (ours arrives shortly)"
    clock.t += 2
    with A._CACHE_LOCK:
        A._FORECAST_CACHE.clear()
    assert A.parse_gfs_spectral("51201", None)[2] == "Swell split: NOAA's"
    b.point(RUN_A)
    A._gspec_refresh(A._gspec_root())
    with A._CACHE_LOCK:
        A._FORECAST_CACHE.clear()
    assert A.parse_gfs_spectral("51201", None)[1] == "spectral"


def test_a_station_the_run_lacks_and_a_site_with_no_run(world):
    b, site, fetched, _, _ = world
    A._gspec_refresh(A._gspec_root())
    data, split, note = A.parse_gfs_spectral("46001", None)              # listed, not in the table
    assert split == "noaa" and note == "Swell split: NOAA's" and data[-1] is None   # nothing is on its way (G48 A P3-5)
    out = A.compute_forecast_payload("46001", None, "US", "GFS", compact=True)
    assert out["split"] == "noaa" and out["gfsb_available"] is True
    b.put_run(RUN_B, {"51201": station_rows(RUN_B)}); b.point(RUN_B)   # run B lands without 41113 (left out of it)
    A._gspec_refresh(A._gspec_root())
    site["run"] = (RUN_B[:8], RUN_B[8:])
    data, split, note = A.parse_gfs_spectral("41113", None)             # held in A only: the lag rule would say "shortly"
    assert split == "noaa" and note == "Swell split: NOAA's", "the newest held run is the site's and lacks it: nothing is on its way"
    assert A.parse_gfs_spectral("51201", None)[1] == "spectral"
    site["run"] = (RUN_C[:8], RUN_C[8:])     # the site moves on before the job: B is no longer the site's run, still lacks it
    data, split, note = A.parse_gfs_spectral("41113", None)
    assert split == "noaa" and note == "Swell split: NOAA's", "never A's table as 'delayed': the job left 41113 out of B (77c review)"
    assert A._gspec_prior_note(RUN_A, RUN_C) is None and A._gspec_split_note(RUN_A, RUN_C) == "Swell split: NOAA's"
    assert A.parse_gfs_spectral("51201", None)[1:] == ("spectral", f"{RUN_C[8:]} UTC run spectral analysis in progress")
    site["run"] = (None, None)                                          # no bulletin run found: ours still serves
    data, split, note = A.parse_gfs_spectral("51201", None)
    assert split == "spectral" and data[-1] is None
    data, split, note = A.parse_gfs_spectral("46001", None)
    assert split == "noaa" and note is None and data[-1] is None       # parse_bull's own 30-min cache still answers
    with A._CACHE_LOCK:
        A._FORECAST_CACHE.clear()                                       # a cold process: the bulletin path's own error
    data, split, note = A.parse_gfs_spectral("46001", None)
    assert split == "noaa" and note is None and data[-1] == "No recent run found."


def test_the_flag_off_is_todays_table(world, monkeypatch):
    b, site, fetched, _, _ = world
    A._gspec_refresh(A._gspec_root())
    monkeypatch.setattr(A, "GFS_SPECTRAL", False)
    out = A.compute_forecast_payload("51201", None, "US", "GFS", compact=True)
    assert out["gfsb_available"] is False and "split" not in out and "split_note" not in out
    assert out["graph_header"]["cycle"] == "20261001 12 UTC" and fetched == [(RUN_A[:8], RUN_A[8:], "51201")]
    assert A.resolve_model("51201", "GFSB") == "GFS"
    assert A.compute_forecast_payload("51201", None, "US", "GFSB", compact=True)["model"] == "GFS"
    assert A._gspec_tick() is False and A._gspec_health()["enabled"] is False


def test_an_old_gfs_runs_blank_early_wind_cells_are_final(monkeypatch):
    monkeypatch.setattr(A, "_nwps_utcnow", lambda: datetime(2026, 10, 3, 12))
    assert A._gfs_wind_final("Cycle : 20261001 12 UTC") is True                        # 48 h
    assert A._gfs_wind_final("Cycle : 20261001 13 UTC") is False
    assert A._gfs_wind_final("Cycle    : 20261001 12 UTC") is True, "the bulletin's own spacing"
    assert A._gfs_wind_final("Cycle    : 20261001  0 UTC") is True, "the bulletin's one-digit hour at 00Z / 06Z (G48 B P3-3)"
    assert A._gfs_wind_final("Cycle : 20261003  0 UTC") is False                        # 12 h, the same spelling
    assert A._gfs_cycle_run("Cycle    : 20261102  0 UTC") == "2026110200" and A._gfs_cycle_run("Cycle : 20261102 06 UTC") == "2026110206"
    assert A._gfs_cycle_run("Cycle : 20261102 123 UTC") is None and A._gfs_cycle_run(None) is None
    assert A._gfs_wind_final("Cycle : NOAA GLWU 20261001 12 UTC") is False and A._gfs_wind_final(None) is False
    assert A._old_run_wind_final("Cycle : 20261001 12 UTC") is True
    rows = [["d", "t"] + [None] * 18 + [None, None, 1.0]]
    assert A._forecast_entry_ttl(("Cycle : 20261001 12 UTC", "", None, rows, "UTC", None)) == A._FORECAST_CACHE_TTL
    assert A._forecast_entry_ttl(("Cycle : 20261003 06 UTC", "", None, rows, "UTC", None)) == A._WIND_NEG_TTL


def test_healthz_reports_the_spectral_table(world):
    with A.app.test_client() as c:
        body = c.get("/healthz").get_json()
    sp = body["gfs"]["spectral"]
    assert set(sp) == {"enabled", "pointer_run", "held", "stations", "pointer_age_s", "last_error", "reads_started",
                       "fetched", "served"}
    assert sp["enabled"] is True and sp["held"] == [] and sp["served"] == {"spectral": 0, "fallback": 0, "prior": 0}


def test_a_slow_bucket_never_holds_a_request(world, monkeypatch):
    """The request path reads memory only: with the bucket hanging, a table answer comes back at once (NOAA's split)."""
    b, site, fetched, _, _ = world

    def hang(*a, **k):
        raise AssertionError("a request thread reached the bucket")
    monkeypatch.setattr(A, "_points_fetch", hang)
    monkeypatch.setattr(A, "_capped_get", hang)
    out = A.compute_forecast_payload("51201", None, "US", "GFS", compact=True)
    assert out["split"] == "noaa" and out["error"] is None


# ------------------------------------------------------------------------------------- G48 (reviewer A's test gaps)

def test_a_new_site_run_is_served_at_once_and_the_note_follows_the_failing_clock(world, monkeypatch):
    """NOAA's split served meanwhile is the NEW run's: parse_bull's run-less entry is dropped when the site's run moves
    (G48 A P3-1); the fallback's own key carries (S, R) and lives a minute, so the note is judged again within a minute
    of the failing clock passing half an hour (P3-6: the 60-s life and the key's (S, R) pinned by behaviour)."""
    b, site, fetched, _, clock = world

    def bull(d, r, s):                                     # the bulletin of the run asked for (its own Cycle line)
        fetched.append((d, r, s))
        return BULL.replace("51201", s).replace("Cycle    : 20261001 12 UTC", f"Cycle    : {d} {r} UTC"), None
    monkeypatch.setattr(A, "_fetch_bulletin", bull)
    data, split, note = A.parse_gfs_spectral("51201", None)          # nothing held yet: NOAA's split of run A
    assert split == "noaa" and note == "Swell split: NOAA's (ours arrives shortly)"
    assert data[0] == "Cycle    : 20261001 12 UTC" and fetched == [(RUN_A[:8], RUN_A[8:], "51201")]
    clock.t += 5
    site["run"] = (RUN_B[:8], RUN_B[8:])                               # the site moved on within the minute
    out = A.compute_forecast_payload("51201", None, "US", "GFSB", compact=True)   # the button first: NOAA's split of the NEW run
    assert out["graph_header"]["cycle"] == "20261001 18 UTC" and fetched[-1] == (RUN_B[:8], RUN_B[8:], "51201") and len(fetched) == 2,         "the run-less entry was dropped for the button's table too"
    data, split, note = A.parse_gfs_spectral("51201", None)
    assert data[0] == "Cycle    : 20261001 18 UTC" and len(fetched) == 2, "NOAA's split of the NEW run meanwhile, from the same entry"
    clock.t += 30
    assert A.parse_gfs_spectral("51201", None)[0][0] == "Cycle    : 20261001 18 UTC" and len(fetched) == 2, "within the minute the fallback entry serves"
    # the note follows the failing clock within a minute: nothing held, the reads failing
    del b.objects["gfswave/spectral/v1/latest.json"]
    A._gspec_refresh(A._gspec_root())                                 # failing since now
    t0 = clock.t
    clock.t = t0 + A._GSPEC_FAILING_SHORTLY_S - 1
    assert A.parse_gfs_spectral("51201", None)[2] == "Swell split: NOAA's (ours arrives shortly)"
    clock.t = t0 + A._GSPEC_FAILING_SHORTLY_S + A._GSPEC_FALLBACK_TTL + 1
    assert A.parse_gfs_spectral("51201", None)[2] == "Swell split: NOAA's", "judged again within a minute of the half hour"
    assert len(fetched) == 3 and fetched[-1][:2] == (RUN_B[:8], RUN_B[8:]), "parse_bull's own 30-min entry expired meanwhile: the same run asked for once more"


def test_one_station_in_two_zones_differs_under_the_spectral_key(world):
    """The spectral key carries the zone: one station asked in two zones gives two tables (G48 A spectral_key_no_tz)."""
    b, site, *_ = world
    A._gspec_refresh(A._gspec_root())
    hst, utc = A.parse_gfs_spectral("51201", "Pacific/Honolulu")[0], A.parse_gfs_spectral("51201", "UTC")[0]
    assert hst[4] == "Pacific/Honolulu" and utc[4] == "UTC"
    assert hst[3][0][1] != utc[3][0][1] and hst[3][0][2:] == utc[3][0][2:], "the same hour, its own clock in each zone"
    assert A.compute_forecast_payload("51201", "UTC", "US", "GFS", compact=True)["tz_label"] == "UTC"
    assert A.compute_forecast_payload("51201", "Pacific/Honolulu", "US", "GFS", compact=True)["tz_label"] == "Pacific/Honolulu"


def test_the_flag_off_holds_the_scheduler_read_even_when_due(world, monkeypatch):
    """With the flag off the scheduler reads nothing whatever the pointer's age (G48 A flag_ignored_root: the flag, not
    the pointer TTL, is what holds the read)."""
    b, *_ = world
    monkeypatch.setattr(A, "GFS_SPECTRAL", False)
    with A._GSPEC_LOCK:
        A._GSPEC.update(ptr_ts=0.0, failed=0.0)                        # due by every clock
    assert A._gspec_due(A.time.time()) is True and A._gspec_root() == ""
    n = len(b.asked)
    assert A._gspec_tick() is False and b.asked[n:] == []
    monkeypatch.setattr(A, "GFS_SPECTRAL", True)
    assert A._gspec_tick() is True
    A._GSPEC["thread"].join(5)
    assert b.asked[n:n + 1] == ["gfswave/spectral/v1/latest.json"] and A._gspec_health()["held"] == [RUN_A]


def test_a_stale_nwps_or_swan_table_falls_to_our_split(world, monkeypatch):
    """An NWPS or SWAN table past its 72-h hide line falls to the GFS table of the day, our split with its keys, not
    the bare bulletin (G48 A nwps_stale_to_bull / swan_stale_to_bull)."""
    b, site, fetched, _, _ = world
    A._gspec_refresh(A._gspec_root())
    rows = [["d", "t"] + [None] * 21]
    monkeypatch.setitem(A.NWPS_STATIONS, "51201", dict(A.NWPS_STATIONS["41004"], office="hfo"))
    monkeypatch.setattr(A, "nwps_offered", lambda s: s == "51201")
    monkeypatch.setattr(A, "parse_nwps", lambda s, tz: ("Cycle : NOAA NWPS 20260901 00 UTC (NWS HFO)", "Location : 51201", None, rows, "Pacific/Honolulu", None))
    out = A.compute_forecast_payload("51201", None, "US", "NWPS", compact=True)     # the office's run is 30 days old
    assert out["model"] == "GFS" and out["nwps_available"] is False and out["split"] == "spectral"
    assert out["graph_header"]["cycle"] == f"{RUN_A[:8]} {RUN_A[8:]} UTC" and fetched == []
    monkeypatch.setattr(A, "_swan_now", lambda: calendar.timegm((2026, 10, 1, 18, 0, 0)))
    monkeypatch.setattr(A, "swan_offered", lambda s: s == "51201")
    monkeypatch.setattr(A, "parse_swan", lambda s, tz: ("Cycle : PacIOOS SWAN updated 20260901 00 UTC", "Location : 51201", None, rows, "Pacific/Honolulu", None))
    out = A.compute_forecast_payload("51201", None, "US", "SWAN", compact=True)
    assert out["model"] == "GFS" and out["swan_available"] is False and out["split"] == "spectral" and fetched == []


def test_an_old_runs_blank_early_wind_is_final_at_the_payload_level(world, monkeypatch):
    """C4 at the payload level: a run 48 h or older with blank early wind cells answers wind_complete True for our
    split and for NOAA's (G48 A wind_complete_no_gfs, P3-3); a young run answers False."""
    b, site, fetched, winds, clock = world
    A._gspec_refresh(A._gspec_root())
    base = datetime.strptime(RUN_A, "%Y%m%d%H")
    monkeypatch.setattr(A, "get_station_wind", lambda sid, d=None, r=None, keep_s=None: {base + timedelta(hours=h): (5.0, 45.0) for h in range(6, 385)})
    monkeypatch.setattr(A, "_nwps_utcnow", lambda: base + timedelta(hours=49))
    out = A.compute_forecast_payload("51201", None, "US", "GFS", compact=True)
    assert out["split"] == "spectral" and out["wind_complete"] is True
    outb = A.compute_forecast_payload("51201", None, "US", "GFSB", compact=True)
    assert outb["model"] == "GFSB" and outb["wind_complete"] is True
    monkeypatch.setattr(A, "_nwps_utcnow", lambda: base + timedelta(hours=10))
    with A._CACHE_LOCK:
        A._FORECAST_CACHE.clear()
    assert A.compute_forecast_payload("51201", None, "US", "GFS", compact=True)["wind_complete"] is False
    assert A.compute_forecast_payload("51201", None, "US", "GFSB", compact=True)["wind_complete"] is False


def test_rank_rows_reorders_parts_stored_out_of_power_order(world):
    """The site ranks every hour's parts itself (Hs^2 x Tp): a record whose parts are stored out of order still shows
    the most powerful first (G48 A rows_unranked: the job's encoder stores them in order, so this is the safety net)."""
    b, site, *_ = world
    idx, body = b.put_run(RUN_A, {"51201": station_rows(RUN_A)})
    body = bytearray(body)
    body[4:10], body[10:16] = body[10:16], body[4:10]                  # step 0: part 0 (1.5 m) <-> part 1 (0.6 m)
    doc = json.loads(idx)
    doc["sha256"] = hashlib.sha256(body).hexdigest()
    b.objects[f"gfswave/spectral/v1/{RUN_A}/index.json"] = json.dumps(doc).encode()
    b.objects[f"gfswave/spectral/v1/{RUN_A}/spectral.bin"] = bytes(body)
    A._gspec_refresh(A._gspec_root())
    dec = A._gspec_decode(A._gspec_held_for("51201")[1])
    assert dec[0][1][0][0] == 0.6 and dec[0][1][1][0] == 1.5, "stored out of power order"
    rows = A.parse_gfs_spectral("51201", None)[0][3]
    assert rows[0][2] == round(1.5 * 3.28084, 2) and rows[0][5] == round(0.6 * 3.28084, 2), "ranked on the way out"


def test_the_button_needs_a_frames_root_and_the_exception_answer_carries_it(world, monkeypatch):
    """GFS_SPECTRAL=1 with no bucket configured offers nothing (G48 A P3-5 b); /api/forecast's exception answer carries
    gfsb_available like the other two flags, so a network failure never hides the button (A P3-4)."""
    b, *_ = world
    assert A.gfsb_offered("51201") is True
    monkeypatch.delenv("MODEL_FRAMES_BASE")
    assert A.gfsb_offered("51201") is False and A.resolve_model("51201", "GFSB") == "GFS"
    assert A.compute_forecast_payload("51201", None, "US", "GFSB", compact=True)["gfsb_available"] is False
    monkeypatch.setenv("MODEL_FRAMES_BASE", BASE)
    monkeypatch.setattr(A, "compute_forecast_payload", lambda *a, **k: 1 / 0)
    with A.app.test_client() as c:
        out = c.get("/api/forecast?station=51201&compact=1").get_json()
        assert out["error"] and out["gfsb_available"] is True and out["swan_available"] is True and out["model"] == "GFS"
        out = c.get("/api/forecast?station=45007&compact=1").get_json()
        assert out["error"] and out["gfsb_available"] is False and out["model"] == "GLWU"
