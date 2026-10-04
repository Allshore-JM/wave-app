"""Live buoys at once (plan section 36), app half: the background service, the non-blocking
route, the partial answer, the memo prebuild and /healthz.

LIVE_BACKGROUND is False in the test process (tests/conftest.py), so the route takes the
pre-section-36 inline path by default (pinned by the golden replay). These tests switch the
flag on for the route and drive the scheduler by hand (`_live_tick`) with a recording runner,
or run the real thread briefly.
"""
import json
import os
import sys
import threading
import time as _time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import buoy_sources as B  # noqa: E402
import fake_buoy_providers as F  # noqa: E402

LIVE = "/api/buoys/live-stations"
REAL_START = A.start_live_background


class RecordingRunner:
    def __init__(self):
        self.jobs = []
        self.queued = 0
        self.running = 0
        self.waiting = 0
        self.workers = 0

    def submit(self, fn, name="x"):
        self.jobs.append((name, fn))

    def run(self, n=None):
        jobs, self.jobs = (self.jobs[:n], self.jobs[n:]) if n else (self.jobs, [])
        for _, fn in jobs:
            fn()
        return [name for name, _ in jobs]


def _fresh_bg():
    return {"started": False, "thread": None, "started_ts": None, "warm_ts": None, "ticks": 0,
            "last_tick_ts": None, "last_tick_s": None, "prebuilds": 0, "errors": 0, "last_error": None,
            "tz_loaded": False, "build_failures": 0}


@pytest.fixture
def bg(monkeypatch):
    """The service ON for the route/scheduler, a recording runner, fresh state, fake providers."""
    provs = F.make_providers()
    monkeypatch.setattr(A, "get_buoy_providers", lambda: provs)
    monkeypatch.setattr(A, "_buoy_tz_cached", F.fake_tz)
    monkeypatch.setattr(A, "_LIVE_STATIONS_MEMO", {"key": None, "payload": None, "etag": None})
    monkeypatch.setattr(A, "_LIVE_BG", _fresh_bg())
    monkeypatch.setattr(A, "LIVE_BACKGROUND", True)
    # a request's before_request fallback would start the REAL scheduler thread (which would
    # outlive the test and poll the real providers): stubbed; the thread test restores it
    monkeypatch.setattr(A, "start_live_background", lambda: True)
    monkeypatch.setattr(B, "time", F.FrozenTime())
    rec = RecordingRunner()
    prev = B.set_refresh_runner(rec)
    yield provs, rec, A.app.test_client()
    B.set_refresh_runner(prev)
    t = A._LIVE_BG.get("thread")                  # no scheduler thread survives a test
    assert t is None or not t.is_alive()


def _ids(resp):
    return sorted(s["id"] for s in resp.get_json())


# ------------------------------- the non-blocking route -------------------------------

def test_route_never_waits_and_says_what_is_missing(bg):
    provs, rec, c = bg
    r = c.get(LIVE)
    assert r.status_code == 200 and r.get_json() == []
    assert r.headers["X-Live-Stations-Partial"] == ",".join(p.source for p in provs)
    assert r.headers["Cache-Control"] == "no-store" and r.headers["CDN-Cache-Control"] == "no-store"
    assert r.headers["ETag"] == '"%s"' % A._LIVE_EMPTY[1]
    assert sum(c.fetch_calls for c in F.FAKE_CLASSES) == 0          # nothing fetched on the request
    assert [n for n, _ in rec.jobs] == ["buoy-refresh-%s" % s for s in A.LIVE_WARM_ORDER]   # queued once each, cheap first
    c.get(LIVE)
    assert len(rec.jobs) == len(provs)                               # a second request queues nothing new
    rec.run(2)                                                       # NDBC + CDIP land
    A._LIVE_WAKE.clear()
    r = c.get(LIVE)                                                  # not built yet: the last build, and a wake-up
    assert r.get_json() == [] and A._LIVE_WAKE.is_set()
    assert r.headers["X-Live-Stations-Partial"] == ",".join(p.source for p in provs)   # what the SERVED bytes lack
    A._live_tick(provs)                                              # the scheduler builds them
    r = c.get(LIVE)
    assert r.headers["X-Live-Stations-Partial"] == ",".join(p.source for p in provs[2:])
    assert r.headers["Cache-Control"] == "no-store"
    assert {s["source"] for s in r.get_json()} == {"NDBC", "CDIP"}
    # merged like a complete answer: the co-located CDIP buoy is the NDBC marker's duplicate
    assert _ids(r) == sorted(s["id"] for s in json.loads(A._build_live_stations_payload(
        [provs[0].snapshot()[0], provs[1].snapshot()[0]])[0]))
    rec.run()                                                        # everyone
    A._live_tick(provs)
    r = c.get(LIVE)
    assert "X-Live-Stations-Partial" not in r.headers
    assert r.headers["Cache-Control"] == "public, max-age=900"
    assert r.headers["CDN-Cache-Control"] == "no-store"
    r304 = c.get(LIVE, headers={"If-None-Match": r.headers["ETag"]})
    assert r304.status_code == 304 and "X-Live-Stations-Partial" not in r304.headers


def test_complete_answer_equals_the_inline_path_bytes(bg, monkeypatch):
    provs, rec, c = bg
    c.get(LIVE)
    rec.run()
    A._live_tick(provs)
    r_bg = c.get(LIVE)
    assert "X-Live-Stations-Partial" not in r_bg.headers
    # the same providers through the inline (golden) path, in a fresh memo
    monkeypatch.setattr(A, "LIVE_BACKGROUND", False)
    monkeypatch.setattr(A, "_LIVE_STATIONS_MEMO", {"key": None, "payload": None, "etag": None})
    r_inline = c.get(LIVE)
    assert r_inline.data == r_bg.data and r_inline.headers["ETag"] == r_bg.headers["ETag"]


def test_route_serves_the_prebuilt_memo_without_a_build(bg, monkeypatch):
    provs, rec, c = bg
    A._live_tick(provs)
    rec.run()
    assert A._live_tick(provs)["built"] is True                      # the scheduler prebuilt it
    builds = []
    real = A._build_live_stations_payload
    monkeypatch.setattr(A, "_build_live_stations_payload", lambda lists: builds.append(1) or real(lists))
    r = c.get(LIVE)
    assert r.status_code == 200 and builds == []
    assert len(r.get_json()) > 0


def test_a_publish_between_passes_is_served_from_the_last_build_and_wakes_the_scheduler(bg, monkeypatch):
    provs, rec, c = bg
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    r0 = c.get(LIVE)
    cdip = next(p for p in provs if p.source == "CDIP")
    cdip._list_ts = 0.0                                              # due again
    A._LIVE_WAKE.clear()
    assert cdip.schedule_refresh()                                   # (what the scheduler's pass does)
    rec.run()                                                        # CDIP republished (new data, new version)
    assert A._LIVE_WAKE.is_set()                                     # the publish woke the scheduler
    builds = []
    real = A._build_live_stations_payload
    monkeypatch.setattr(A, "_build_live_stations_payload", lambda lists: builds.append(1) or real(lists))
    r1 = c.get(LIVE)
    assert builds == [] and r1.data == r0.data                       # the request never builds: the last build
    assert "X-Live-Stations-Partial" not in r1.headers               # complete (one version behind)
    assert c.get("/healthz").get_json()["memo"]["current"] is False
    assert A._live_tick(provs)["built"] is True and builds == [1]    # the scheduler builds it
    assert c.get("/healthz").get_json()["memo"]["current"] is True


def test_the_route_never_builds_nor_waits_for_a_build(bg, monkeypatch):
    """Built inside requests, the merge and the time zones held every server thread on the test site."""
    provs, rec, c = bg
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    r0 = c.get(LIVE)
    for p in provs:                                                  # newer lists everywhere
        p._list_ts = 0.0
        p.schedule_refresh()
    rec.run()
    boom = lambda *a, **k: (_ for _ in ()).throw(AssertionError("a request built the list"))
    monkeypatch.setattr(A, "_build_live_stations_payload", boom)
    monkeypatch.setattr(A, "_live_memo_bytes", boom)
    monkeypatch.setattr(A, "_buoy_tz_cached", boom)
    held = threading.Event(), threading.Event()

    def hold():                                                      # a build in progress elsewhere
        with A._LIVE_BUILD_LOCK:
            held[0].set()
            held[1].wait(5)
    t = threading.Thread(target=hold)
    t.start()
    held[0].wait(5)
    try:
        t0 = _time.perf_counter()
        r = c.get(LIVE)
        assert _time.perf_counter() - t0 < 1.0
        assert r.status_code == 200 and r.data == r0.data
    finally:
        held[1].set()
        t.join(5)


def test_a_kept_failure_does_not_wake_the_scheduler(bg):
    provs, rec, c = bg
    A._live_tick(provs)
    rec.run()
    p = next(p for p in provs if p.source == "CDIP")
    p._fetch_stations = lambda: (_ for _ in ()).throw(RuntimeError("down"))
    p._list_ts = 0.0
    A._LIVE_WAKE.clear()
    p.refresh()
    assert p.status()["last_error"] and not A._LIVE_WAKE.is_set()    # same version: nothing to rebuild


def test_stale_lists_are_served_at_once_while_refreshing(bg):
    provs, rec, c = bg
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    r1 = c.get(LIVE)
    for p in provs:
        p._list_ts = 0.0                                             # everyone due
    r2 = c.get(LIVE)                                                 # served at once, from the lists in hand
    assert r2.data == r1.data and "X-Live-Stations-Partial" not in r2.headers
    assert sum(c.fetch_calls for c in F.FAKE_CLASSES) == len(provs)  # no inline fetch
    assert len(rec.jobs) == 0                                        # the route itself queues only MISSING providers
    assert A._live_tick(provs)["scheduled"] == [p.source for p in A._live_providers_ordered(provs)]


def test_inline_path_when_the_service_is_off(bg, monkeypatch):
    provs, rec, c = bg
    monkeypatch.setattr(A, "LIVE_BACKGROUND", False)
    r = c.get(LIVE)
    assert "X-Live-Stations-Partial" not in r.headers and len(r.get_json()) > 0
    assert sum(c.fetch_calls for c in F.FAKE_CLASSES) == len(provs)  # fetched inline (cold)
    assert rec.jobs == []


# ------------------------------- the scheduler -------------------------------

def test_warm_up_schedules_everyone_cheap_first_and_prebuilds(bg):
    provs, rec, c = bg
    out = A._live_tick(provs)                                         # cold: everyone is due
    assert out["scheduled"] == A.LIVE_WARM_ORDER                      # the fakes carry every source
    assert out["warm"] is False and out["built"] is True              # an (empty) memo for the empty key
    assert A._LIVE_BG["ticks"] == 1 and A._LIVE_BG["warm_ts"] is None
    names = rec.run()
    assert names == ["buoy-refresh-%s" % s for s in A.LIVE_WARM_ORDER]
    out = A._live_tick(provs)
    assert out["warm"] is True and out["built"] is True and out["scheduled"] == []
    assert A._LIVE_BG["warm_ts"] is not None and A._LIVE_BG["prebuilds"] == 2
    assert A._live_tick(provs)["built"] is False                      # nothing changed: no build


def test_a_scheduler_restart_leaves_fresh_lists_alone(bg):
    provs, rec, c = bg
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    fetched = sum(c.fetch_calls for c in F.FAKE_CLASSES)
    A._LIVE_BG["thread"] = None                                       # as after a dead thread
    assert A._live_tick(provs)["scheduled"] == []                     # a restart's first pass: nothing due
    assert rec.jobs == [] and sum(c.fetch_calls for c in F.FAKE_CLASSES) == fetched


def test_due_providers_only_and_order(bg):
    provs, rec, c = bg
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    for src in ("CMEMS", "NDBC"):
        next(p for p in provs if p.source == src)._list_ts = 0.0
    out = A._live_tick(provs)
    assert out["scheduled"] == ["NDBC", "CMEMS"]                      # due ones, warm order
    assert [n for n, _ in rec.jobs] == ["buoy-refresh-NDBC", "buoy-refresh-CMEMS"]
    assert A._live_tick(provs)["scheduled"] == []                     # pending: not queued twice


def test_a_raising_provider_check_never_stops_the_pass(bg, monkeypatch):
    provs, rec, c = bg
    bad = next(p for p in provs if p.source == "CDIP")
    monkeypatch.setattr(bad, "refresh_due", lambda now=None: (_ for _ in ()).throw(RuntimeError("boom")))
    out = A._live_tick(provs)
    assert "CDIP" not in out["scheduled"] and len(out["scheduled"]) == len(provs) - 1
    assert A._LIVE_BG["errors"] == 1 and "CDIP: boom" in A._LIVE_BG["last_error"]
    monkeypatch.setattr(A, "_live_prebuild", lambda provs: (_ for _ in ()).throw(RuntimeError("build boom")))
    out = A._live_tick(provs)
    assert A._LIVE_BG["errors"] == 3 and "build boom" in A._LIVE_BG["last_error"] and out["built"] is False


def test_scheduler_thread_warms_and_stops(bg, monkeypatch):
    provs, rec, c = bg
    B.set_refresh_runner(B.InlineRunner())
    monkeypatch.setattr(A, "LIVE_TICK_SEC", 0.05)
    monkeypatch.setattr(A, "LIVE_WARM_TICK_SEC", 0.02)
    order = []
    real_tick = A._live_tick
    monkeypatch.setattr(A, "_live_tick", lambda *a, **k: order.append("tick") or real_tick(*a, **k))
    monkeypatch.setattr(A, "get_tz_finder", lambda: order.append("tz") or object())
    monkeypatch.setattr(A, "start_live_background", REAL_START)
    try:
        assert A.start_live_background() is True
        t = A._LIVE_BG["thread"]
        n = threading.active_count()
        assert A.start_live_background() is True                      # idempotent: the same thread
        assert A._LIVE_BG["thread"] is t and threading.active_count() == n
        assert A._live_scheduler_alive()
        deadline = _time.time() + 5
        while A._LIVE_BG["warm_ts"] is None and _time.time() < deadline:
            _time.sleep(0.01)
        assert A._LIVE_BG["warm_ts"] is not None and A._LIVE_BG["tz_loaded"] is True
        assert order[:3] == ["tick", "tz", "tick"]                      # refreshes queued before the tz finder loads
        r = c.get(LIVE)
        assert "X-Live-Stations-Partial" not in r.headers and len(r.get_json()) > 0
        assert c.get("/healthz").status_code == 200
    finally:
        A.stop_live_background()
    assert not A._live_scheduler_alive()


def test_service_off_never_starts_a_thread(bg, monkeypatch):
    monkeypatch.setattr(A, "LIVE_BACKGROUND", False)
    assert REAL_START() is False
    assert A._LIVE_BG["thread"] is None


def test_fallback_restarts_a_dead_scheduler(bg, monkeypatch):
    provs, rec, c = bg
    starts = []
    monkeypatch.setattr(A, "start_live_background", lambda: starts.append(1) or True)
    c.get("/healthz")
    assert starts == [1]                                              # no thread -> started on the request
    monkeypatch.setattr(A, "_live_scheduler_alive", lambda: True)
    c.get("/healthz")
    assert starts == [1]


# ------------------------------- /healthz -------------------------------

def test_healthz_503_until_warm_then_200(bg):
    provs, rec, c = bg
    A._LIVE_BG["started_ts"] = _time.time()
    r = c.get("/healthz")
    assert r.status_code == 503
    body = r.get_json()
    assert body["ok"] is False and body["warm"] is False and body["missing"] == [p.source for p in provs]
    assert r.headers["Cache-Control"] == "no-store" and r.headers["CDN-Cache-Control"] == "no-store"
    assert rec.jobs == [] and sum(c.fetch_calls for c in F.FAKE_CLASSES) == 0   # never triggers work
    A._live_tick(provs)
    rec.run(1)                                                       # NDBC alone is not warm
    A._live_tick(provs)
    r = c.get("/healthz")
    assert r.status_code == 503 and r.get_json()["missing"] == [p.source for p in provs if p.source != "NDBC"]
    assert A._LIVE_BG["warm_ts"] is None
    rec.run()
    r = c.get("/healthz")                                            # every provider answered, the list not built yet
    assert r.status_code == 503 and r.get_json()["warm"] is True and r.get_json()["memo"]["complete"] is False
    A._live_tick(provs)
    r = c.get("/healthz")
    body = r.get_json()
    assert r.status_code == 200 and body["ok"] and body["warm"] and body["missing"] == []
    by = {s["source"]: s for s in body["providers"]}
    assert by["NDBC"]["stations"] == 3 and by["NDBC"]["version"] == 1 and by["NDBC"]["last_error"] is None
    assert by["SMHI"]["stations"] == 0 and "SMHI down" in by["SMHI"]["last_error"]
    assert by["SMHI"]["due_in_s"] == pytest.approx(300, abs=1)
    assert body["memo"]["sources"] == len(provs) and body["memo"]["bytes"] > 100
    assert body["ticks"] == 3 and body["prebuilds"] == 3 and body["errors"] == 0
    assert body["runner"] == {"waiting": 0, "running": 0, "workers": 0}


def test_healthz_200_after_the_deadline_even_when_a_feed_is_dead(bg, monkeypatch):
    provs, rec, c = bg
    A._LIVE_BG["started_ts"] = _time.time() - A.LIVE_WARM_DEADLINE_SEC - 1
    assert c.get("/healthz").status_code == 503                        # no pass yet: not serving lists
    A._live_tick(provs)                                                  # a pass; nothing has answered
    r = c.get("/healthz")
    assert r.status_code == 200 and r.get_json()["ok"] is True and r.get_json()["warm"] is False


def test_healthz_never_fails_for_15_seconds_after_a_restart(bg, monkeypatch):
    """Render stops routing to an instance failing its check for 15 s and restarts it after 60 s, a
    restarted one too (G25 A-2): a dead feed (MI-IE, 124 s per attempt) must not hold /healthz at 503."""
    provs, rec, c = bg
    clock = F.FrozenTime()
    monkeypatch.setattr(A, "time", clock)
    monkeypatch.setattr(B, "time", clock)
    A._LIVE_BG["started_ts"] = clock.now
    assert A.LIVE_WARM_DEADLINE_SEC < 15
    A._live_tick(provs)                                                  # the first pass queues everyone
    by = {name: fn for name, fn in rec.jobs}
    for src in [s for s in A.LIVE_WARM_ORDER if s != "MI-IE"]:          # every feed but MI-IE answers at once
        by["buoy-refresh-%s" % src]()
    A._live_tick(provs)
    worst, run = 0, 0
    for second in range(0, 200):
        clock.now = A._LIVE_BG["started_ts"] + second
        A._live_tick(provs)                                              # the scheduler's passes go on
        if c.get("/healthz").status_code == 503:
            run += 1
            worst = max(worst, run)
        else:
            run = 0
    assert worst <= A.LIVE_WARM_DEADLINE_SEC + 1 and worst < 15        # never long enough for Render to act
    r = c.get("/healthz").get_json()
    assert r["ok"] and not r["warm"] and r["missing"] == ["MI-IE"]


def test_healthz_waits_for_the_full_list_only_within_the_deploy_grace(bg, monkeypatch):
    provs, rec, c = bg
    clock = F.FrozenTime()
    monkeypatch.setattr(A, "time", clock)
    A._LIVE_BG["started_ts"] = clock.now
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)                                                  # everyone answered within a second
    assert c.get("/healthz").status_code == 200                          # complete: ready at once


def test_healthz_reports_builds_that_keep_failing(bg, monkeypatch):
    """A memo build that keeps failing served [] forever while /healthz said nothing (G25 A-6). It is REPORTED but never
    a 503: its cause is in the data, a restart meets it again, and Render would restart the whole site in a loop
    (re-check N-1)."""
    provs, rec, c = bg
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    assert c.get("/healthz").status_code == 200
    real = A._live_prebuild
    monkeypatch.setattr(A, "_live_prebuild", lambda provs: (_ for _ in ()).throw(RuntimeError("build boom")))
    for i in range(A.LIVE_BUILD_FAIL_MAX):
        A._live_tick(provs)
        body = c.get("/healthz").get_json()
        assert body["build_failures"] == i + 1
        assert body["build_failing"] is (i + 1 >= A.LIVE_BUILD_FAIL_MAX)
    assert A.LIVE_BUILD_FAIL_MAX == 3
    r = c.get("/healthz")
    assert r.status_code == 200 and r.get_json()["build_failing"] is True     # reported, the site stays in rotation
    monkeypatch.setattr(A, "_live_prebuild", real)
    A._live_tick(provs)                                                  # a good build: healthy again
    body = c.get("/healthz").get_json()
    assert body["build_failures"] == 0 and body["build_failing"] is False


def test_healthz_200_with_the_service_off(bg, monkeypatch):
    monkeypatch.setattr(A, "LIVE_BACKGROUND", False)
    _, _, c = bg
    r = c.get("/healthz")
    assert r.status_code == 200 and r.get_json()["background"] is False


# ------------------------------- the diagnostic knob -------------------------------

def test_break_providers_knob(bg):
    provs, rec, c = bg
    assert A._break_providers(provs, "aodn, CMEMS") == {"aodn", "cmems"}
    A._live_tick(provs)
    rec.run()
    by = {p.source: p for p in provs}
    assert by["AODN"].snapshot()[0] == [] and "switched off" in by["AODN"].status()["last_error"]
    assert by["CMEMS"].snapshot()[0] == [] and "switched off" in by["CMEMS"].status()["last_error"]
    assert len(by["NDBC"].snapshot()[0]) == 3
    assert A._break_providers(provs, "") == set()


def test_thread_runner_starts_jobs_in_submission_order_bounded():
    r = B.ThreadRunner(2)
    started, lock = [], threading.Lock()
    gate = threading.Event()

    def job(i):
        def run():
            with lock:
                started.append(i)
            if i < 2:
                gate.wait(5)
        return run
    for i in range(5):
        r.submit(job(i), "j%d" % i)
    _time.sleep(0.1)
    assert r.running == 2 and r.queued == 5 and sorted(started) == [0, 1]   # two running, three waiting
    gate.set()
    deadline = _time.time() + 5
    while r.queued and _time.time() < deadline:
        _time.sleep(0.01)
    assert started[2:] == [2, 3, 4]                                      # FIFO: the queue order
    assert r.running == 0 and r.queued == 0 and len(r._threads) == 2


def test_a_publish_wakes_the_scheduler_thread_at_once(bg, monkeypatch):
    provs, rec, c = bg
    B.set_refresh_runner(B.InlineRunner())
    monkeypatch.setattr(A, "LIVE_TICK_SEC", 60)                       # a pass only when woken
    monkeypatch.setattr(A, "LIVE_WARM_TICK_SEC", 60)
    monkeypatch.setattr(A, "get_tz_finder", lambda: object())
    monkeypatch.setattr(A, "start_live_background", REAL_START)
    try:
        A.start_live_background()
        deadline = _time.time() + 5
        while not A._LIVE_STATIONS_MEMO.get("key") or any(k[1] is None for k in A._LIVE_STATIONS_MEMO["key"]):
            assert _time.time() < deadline, "warm-up not built"
            _time.sleep(0.01)
        before = A._LIVE_BG["prebuilds"]
        cdip = next(p for p in provs if p.source == "CDIP")
        cdip._list_ts = 0.0
        cdip.refresh()                                                # publishes v2 -> wakes the loop
        deadline = _time.time() + 5
        while A._LIVE_BG["prebuilds"] == before:
            assert _time.time() < deadline, "the publish did not wake the scheduler"
            _time.sleep(0.01)
        assert dict(A._LIVE_STATIONS_MEMO["key"])["CDIP"] == 2
        _time.sleep(0.1)
        ticks = A._LIVE_BG["ticks"]
        _time.sleep(0.3)
        assert A._LIVE_BG["ticks"] == ticks                            # back asleep: no busy loop
        t0 = _time.time()
    finally:
        A.stop_live_background()
    assert _time.time() - t0 < 2.0                                    # stop wakes the sleeping loop


# ------------------------------- fork safety -------------------------------

def test_importing_the_app_starts_no_thread():
    """A gunicorn master that preloads the app forks its workers AFTER the import: a thread started at import would run
    in the master, and the worker would inherit the locks it held but not the thread (found on the test site)."""
    import subprocess
    code = ("import os, threading; os.environ['LIVE_BACKGROUND'] = '1'; import app; "
            "names = [t.name for t in threading.enumerate()]; "
            "assert names == ['MainThread'], names; "
            "c = app.app.test_client(); c.get('/healthz'); "
            "assert 'live-scheduler' in [t.name for t in threading.enumerate()]; "
            "app.stop_live_background(); print('ok')")
    env = dict(os.environ, LIVE_BACKGROUND="1")
    r = subprocess.run([sys.executable, "-c", code], cwd=os.path.dirname(HERE), env=env, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0 and r.stdout.strip().endswith("ok"), r.stderr[-2000:]


def test_a_forked_child_starts_clean(bg, monkeypatch):
    """os.register_at_fork(after_in_child=...): every lock a parent's thread might have held is replaced, the runner is
    dropped (its threads are gone), the providers' in-flight / queued flags are cleared, and the service can start."""
    provs, rec, c = bg
    for name in ("_LIVE_BG_LOCK", "_LIVE_BUILD_LOCK", "_TZ_FINDER_LOCK", "_CACHE_LOCK", "_LIVE_STOP", "_LIVE_WAKE"):
        monkeypatch.setattr(A, name, getattr(A, name))               # restored after the test
    monkeypatch.setattr(B, "_RUNNER_LOCK", B._RUNNER_LOCK)
    monkeypatch.setattr(B, "_REFRESH_RUNNER", B._REFRESH_RUNNER)
    # the parent's state at the fork: locks held by threads that will not exist, jobs queued and in flight
    held = [A._LIVE_BUILD_LOCK, A._TZ_FINDER_LOCK, A._CACHE_LOCK, A._LIVE_BG_LOCK, B._RUNNER_LOCK, A._BUOY_PROVIDERS_LOCK]
    monkeypatch.setattr(A, "_BUOY_PROVIDERS_LOCK", A._BUOY_PROVIDERS_LOCK)
    A._LIVE_BG.update(ticks=7, build_failures=2, last_tick_ts=1.0)
    runner_lock, wake, stop = B._RUNNER_LOCK, A._LIVE_WAKE, A._LIVE_STOP
    monkeypatch.setattr(B, "_NONBLOCKING", True)
    for lk in held:
        lk.acquire()
    p = provs[0]
    p._lock.acquire()
    p._refresh_lock.acquire()
    p._refresh_pending = p._refreshing = True
    A._LIVE_BG.update(thread=threading.Thread(target=lambda: None), started=True, started_ts=1.0, warm_ts=2.0)
    try:
        A._live_after_fork()
        B._reset_after_fork()
        for name in ("_LIVE_BG_LOCK", "_LIVE_BUILD_LOCK", "_TZ_FINDER_LOCK", "_CACHE_LOCK"):
            assert not getattr(A, name).locked(), name
        assert not p._lock.locked() and not p._refresh_lock.locked()
        assert p._refresh_pending is False and p._refreshing is False
        assert B._REFRESH_RUNNER is None and not B._RUNNER_LOCK.locked() and B._RUNNER_LOCK is not runner_lock
        assert A._LIVE_WAKE is not wake and A._LIVE_STOP is not stop
        assert B._NONBLOCKING is False
        assert A._LIVE_BG["thread"] is None and A._LIVE_BG["started_ts"] is None and A._LIVE_BG["warm_ts"] is None
        assert A._LIVE_BG["ticks"] == 0 and A._LIVE_BG["build_failures"] == 0 and A._LIVE_BG["last_tick_ts"] is None
        assert not A._BUOY_PROVIDERS_LOCK.locked()
        assert all(not q._lock.locked() for q in provs)
        B.set_refresh_runner(rec)
        assert p.schedule_refresh() is True                          # the child can queue and run refreshes
        rec.run()
        assert p.snapshot() is not None
        A._live_tick(provs)
        assert c.get("/healthz").status_code == 503                  # answers (no lock left held)
    finally:
        for lk in held:
            try:
                lk.release()
            except RuntimeError:
                pass


def test_providers_are_registered_for_the_fork_reset():
    p = F.FakeCDIP(http=None)
    assert p in B._PROVIDERS


def test_healthz_reports_a_stalled_scheduler(bg):
    """The first test-site failure answered 200 after the deadline while its scheduler was frozen."""
    provs, rec, c = bg
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    assert c.get("/healthz").status_code == 200
    A._LIVE_BG["last_tick_ts"] = _time.time() - A.LIVE_STALL_SEC - 1   # no pass for longer than LIVE_STALL_SEC
    r = c.get("/healthz")
    assert r.status_code == 503 and r.get_json()["stalled"] is True and r.get_json()["warm"] is True
    A._live_tick(provs)                                                  # a pass: healthy again
    r = c.get("/healthz")
    assert r.status_code == 200 and r.get_json()["stalled"] is False
    A._LIVE_BG.update(last_tick_ts=None, started_ts=_time.time() - A.LIVE_STALL_SEC - 1)   # never a pass since the start
    assert c.get("/healthz").get_json()["stalled"] is True



# ------------------------------- G25 fixes -------------------------------

def test_a_cold_providers_latest_answers_still_loading_at_once(bg):
    """A click on a remembered marker whose provider has no list yet held a server thread for the whole
    list fetch (G25 A-1): with the service on it answers 503 + Retry-After at once and queues the refresh."""
    provs, rec, c = bg
    t0 = _time.perf_counter()
    r = c.get("/api/buoys/cdip:106/latest")
    assert _time.perf_counter() - t0 < 0.5
    assert r.status_code == 503 and r.get_json()["retry"] is True
    assert r.headers["Retry-After"] == "5" and r.headers["Cache-Control"] == "no-store"
    assert r.headers["CDN-Cache-Control"] == "no-store"
    assert F.FakeCDIP.fetch_calls == 0 and [n for n, _ in rec.jobs] == ["buoy-refresh-CDIP"]
    rec.run()                                                            # its list arrives
    r = c.get("/api/buoys/cdip:106/latest")
    assert r.status_code == 200 and r.get_json()["latest"]["hs_m"] == 1.5


def test_the_cold_path_never_waits_while_the_service_runs(bg):
    """Defence in depth for every detail() / latest() that asks for the list: [] at once + a refresh queued."""
    provs, rec, c = bg
    p = next(p for p in provs if p.source == "CEFAS")
    B.set_nonblocking(True)
    try:
        assert p.list_stations_versioned() == ([], 0, 0.0)
        assert F.FakeCEFAS.fetch_calls == 0 and len(rec.jobs) == 1
        assert p.snapshot() is None                                      # nothing published by the shortcut
    finally:
        B.set_nonblocking(False)
    lst, v, _ = p.list_stations_versioned()                               # without the service: the inline fetch
    assert v == 1 and len(lst) == 2


def test_start_and_stop_switch_the_non_blocking_cold_path(bg, monkeypatch):
    provs, rec, c = bg
    B.set_refresh_runner(B.InlineRunner())
    monkeypatch.setattr(A, "get_tz_finder", lambda: object())
    monkeypatch.setattr(A, "start_live_background", REAL_START)
    try:
        A.start_live_background()
        assert B._NONBLOCKING is True
    finally:
        A.stop_live_background()
    assert B._NONBLOCKING is False


def test_publish_listeners_run_outside_the_refresh_lock(bg):
    """The docstrings said "outside every lock" while _notify_publish ran under _refresh_lock (G25 A-4)."""
    provs, rec, c = bg
    p = next(p for p in provs if p.source == "CDIP")
    seen = []

    def listener(prov):
        seen.append((prov.source, prov._refresh_lock.locked(), prov._lock.locked()))
        prov.refresh_due()                                               # touching the provider must not deadlock
    B.add_publish_listener(listener)
    try:
        p.refresh()                                                      # the inline path
        p._list_ts = 0.0
        assert p.schedule_refresh()
        rec.run()                                                        # the scheduled path
    finally:
        B.remove_publish_listener(listener)
    assert seen == [("CDIP", False, False), ("CDIP", False, False)]


@pytest.mark.parametrize("raw,want", [("abc", 30.0), ("nan", 30.0), ("inf", 30.0), ("-inf", 30.0), ("1e999", 30.0),
                                      ("0", 1.0), ("-5", 1.0), ("0.5", 1.0), ("45", 45.0), ("99999", 600.0)])
def test_env_knobs_are_guarded(monkeypatch, raw, want):
    """A bad LIVE_TICK_SEC broke the import, burnt a core or killed the thread (G25 A-5)."""
    monkeypatch.setenv("LIVE_TICK_SEC", raw)
    assert A._env_number("LIVE_TICK_SEC", 30, 1, 600) == want


@pytest.mark.parametrize("raw", ["inf", "-inf", "nan", "1e999", "abc", ""])
def test_a_bad_edge_ttl_never_breaks_the_route(monkeypatch, raw):
    monkeypatch.setenv("LIVE_STATIONS_EDGE_TTL", raw)
    assert A._live_stations_edge_ttl() == 0
    assert A._live_stations_cdn_headers() == {"CDN-Cache-Control": "no-store"}


def test_the_app_imports_with_bad_knobs_and_the_deploy_grace_stays_under_15_seconds():
    import subprocess
    env = dict(os.environ, LIVE_BACKGROUND="1", LIVE_TICK_SEC="abc", LIVE_WARM_DEADLINE_SEC="90",
               LIVE_REFRESH_WORKERS="inf", LIVE_STATIONS_EDGE_TTL="nan")
    code = ("import app, buoy_sources as B; print(app.LIVE_TICK_SEC, app.LIVE_WARM_DEADLINE_SEC, "
            "B.get_refresh_runner().workers, app._live_stations_edge_ttl())")
    r = subprocess.run([sys.executable, "-c", code], cwd=os.path.dirname(HERE), env=env, capture_output=True,
                       text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
    assert r.stdout.split() == ["30.0", "14.0", "3", "0"]
    for tick, want in (("0", "1.0"), ("-5", "1.0"), ("99999", "120.0"), ("45", "45.0")):   # never a busy loop, never > 120 s
        env.update(LIVE_TICK_SEC=tick)
        env.pop("LIVE_WARM_DEADLINE_SEC", None)
        r = subprocess.run([sys.executable, "-c", code], cwd=os.path.dirname(HERE), env=env, capture_output=True,
                           text=True, timeout=120)
        assert r.returncode == 0 and r.stdout.split()[:2] == [want, "10.0"], (tick, r.stdout, r.stderr[-500:])


def test_only_the_recorded_scheduler_thread_keeps_running(bg, monkeypatch):
    """stop + start overlapping a pass left two or three loops running (G25 A-7)."""
    provs, rec, c = bg
    B.set_refresh_runner(B.InlineRunner())
    monkeypatch.setattr(A, "LIVE_TICK_SEC", 0.02)
    monkeypatch.setattr(A, "LIVE_WARM_TICK_SEC", 0.02)
    monkeypatch.setattr(A, "get_tz_finder", lambda: object())
    monkeypatch.setattr(A, "start_live_background", REAL_START)
    try:
        A.start_live_background()
        first = A._LIVE_BG["thread"]
        with A._LIVE_BG_LOCK:
            A._LIVE_BG["thread"] = None                                  # as after a stop that did not wait
        A.start_live_background()
        second = A._LIVE_BG["thread"]
        assert second is not first
        first.join(2)
        assert not first.is_alive() and second.is_alive()
        assert second.daemon is True                                     # never holds the process at exit
        alive = [t for t in threading.enumerate() if t.name == "live-scheduler"]
        assert alive == [second]
    finally:
        A.stop_live_background()


def test_one_provider_set_when_the_first_requests_race(monkeypatch):
    """The first request and the scheduler thread could build two provider sets: every feed fetched twice at
    boot (G25 A-8)."""
    monkeypatch.setattr(A, "_BUOY_PROVIDERS", None)
    real = A.NDBCBuoyProvider
    monkeypatch.setattr(A, "NDBCBuoyProvider", lambda http=None: (_time.sleep(0.2), real(http=http))[1])   # a slow construction
    got = []
    ts = [threading.Thread(target=lambda: got.append(A.get_buoy_providers())) for _ in range(6)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(5)
    assert len(got) == 6 and len({id(x) for x in got}) == 1


def test_age_s_is_the_age_of_the_list_in_hand(monkeypatch):
    """After a kept failure age_s read TTL - 300 for a list published long before (G25 A-9)."""
    clock = F.FrozenTime()
    monkeypatch.setattr(B, "time", clock)
    p = F.FakeCDIP(http=None)
    p.list_stations_versioned()
    clock.now += 4000                                                    # past the 3600 s TTL
    p._fetch_stations = lambda: (_ for _ in ()).throw(RuntimeError("down"))
    p.refresh()
    st = p.status()
    assert st["age_s"] == 4000.0 and st["good_age_s"] == 4000.0 and st["last_error"]


def test_passes_happen_without_a_wake(bg, monkeypatch):
    """An unbounded wake wait passed the whole suite (G25 A-11): passes run every LIVE_TICK_SEC on their own."""
    provs, rec, c = bg
    B.set_refresh_runner(B.InlineRunner())
    monkeypatch.setattr(A, "LIVE_TICK_SEC", 0.05)
    monkeypatch.setattr(A, "LIVE_WARM_TICK_SEC", 0.05)
    monkeypatch.setattr(A, "get_tz_finder", lambda: object())
    monkeypatch.setattr(A, "start_live_background", REAL_START)
    monkeypatch.setattr(A, "_live_wake", lambda *a: None)                 # nothing ever wakes it
    monkeypatch.setattr(B, "_PUBLISH_LISTENERS", [])
    try:
        A.start_live_background()
        _time.sleep(0.15)
        before = A._LIVE_BG["ticks"]
        _time.sleep(0.4)
        assert A._LIVE_BG["ticks"] >= before + 4
    finally:
        A.stop_live_background()


def test_a_failed_tz_load_does_not_end_the_loop_and_is_tried_once(bg, monkeypatch):
    provs, rec, c = bg
    B.set_refresh_runner(B.InlineRunner())
    monkeypatch.setattr(A, "LIVE_TICK_SEC", 0.02)
    monkeypatch.setattr(A, "LIVE_WARM_TICK_SEC", 0.02)
    calls = []
    monkeypatch.setattr(A, "get_tz_finder", lambda: calls.append(1) or (_ for _ in ()).throw(RuntimeError("no tz")))
    monkeypatch.setattr(A, "start_live_background", REAL_START)
    try:
        A.start_live_background()
        _time.sleep(0.3)
        assert A._live_scheduler_alive() and A._LIVE_BG["ticks"] >= 5
        assert calls == [1]                                              # one attempt; builds load it lazily
        assert len(c.get(LIVE).get_json()) > 0                           # lists still built
    finally:
        A.stop_live_background()


def test_the_warm_tick_gives_way_to_the_normal_tick(bg, monkeypatch):
    provs, rec, c = bg
    B.set_refresh_runner(B.InlineRunner())
    monkeypatch.setattr(A, "LIVE_TICK_SEC", 60)
    monkeypatch.setattr(A, "LIVE_WARM_TICK_SEC", 0.01)
    monkeypatch.setattr(A, "get_tz_finder", lambda: object())
    monkeypatch.setattr(A, "start_live_background", REAL_START)
    try:
        A.start_live_background()
        deadline = _time.time() + 5
        while A._LIVE_BG["warm_ts"] is None:
            assert _time.time() < deadline
            _time.sleep(0.01)
        _time.sleep(0.1)
        ticks = A._LIVE_BG["ticks"]
        _time.sleep(0.3)
        assert A._LIVE_BG["ticks"] == ticks                              # warm: the 60 s tick, no 10 ms loop
    finally:
        A.stop_live_background()


def test_a_dead_scheduler_thread_is_restarted_by_the_next_request(bg, monkeypatch):
    provs, rec, c = bg
    B.set_refresh_runner(B.InlineRunner())
    monkeypatch.setattr(A, "get_tz_finder", lambda: object())
    monkeypatch.setattr(A, "start_live_background", REAL_START)
    dead = threading.Thread(target=lambda: None)
    dead.start()
    dead.join()
    A._LIVE_BG["thread"] = dead                                          # died (e.g. by an exception)
    try:
        c.get("/robots.txt")
        assert A._live_scheduler_alive() and A._LIVE_BG["thread"] is not dead
    finally:
        A.stop_live_background()



@pytest.mark.parametrize("raw,want", [("500", 10), ("0", 1), ("-3", 1), ("4", 4), ("inf", 3), ("abc", 3)])
def test_refresh_workers_are_clamped(monkeypatch, raw, want):
    monkeypatch.setenv("LIVE_REFRESH_WORKERS", raw)
    B.set_refresh_runner(None)                                           # a fresh default runner (conftest restores)
    assert B.get_refresh_runner().workers == want



# ------------------------------- G25 re-check (R1) -------------------------------

def test_a_name_that_is_not_text_never_breaks_the_merged_list(bg):
    """A platform with a numeric id and no description failed every build ('int'.upper()): re-check N-1."""
    provs, rec, c = bg
    cefas = next(p for p in provs if p.source == "CEFAS")
    cefas._fetch_stations = lambda: [{"local_id": 62050, "name": None, "lat": 50.1, "lon": -4.2, "latest_time": F.FRESH},
                                     {"local_id": "X", "name": 7, "lat": 50.2, "lon": -4.3, "latest_time": F.FRESH}]
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    assert A._LIVE_BG["build_failures"] == 0
    names = {s["id"]: s["name"] for s in c.get(LIVE).get_json() if s["source"] == "CEFAS"}
    assert names == {"cefas:62050": "62050", "cefas:X": "7"}


def test_a_position_that_is_not_a_finite_number_is_dropped(bg):
    """json.dumps writes NaN, which every browser's JSON.parse rejects: one bad position broke the list (re-check N-9)."""
    provs, rec, c = bg
    cdip = next(p for p in provs if p.source == "CDIP")
    cdip._fetch_stations = lambda: [{"local_id": "a", "name": "a", "lat": float("nan"), "lon": -117.0},
                                    {"local_id": "b", "name": "b", "lat": 32.0, "lon": float("inf")},
                                    {"local_id": "c", "name": "c", "lat": "NaN", "lon": "-117.1"},
                                    {"local_id": "d", "name": "d", "lat": "32.5", "lon": "-117.2"}]
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    r = c.get(LIVE)
    body = r.get_data(as_text=True)
    assert "NaN" not in body and "Infinity" not in body
    json.loads(body, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))   # strict JSON
    assert [s["id"] for s in r.get_json() if s["source"] == "CDIP"] == ["cdip:d"]


def test_the_stall_limit_follows_a_long_tick(bg, monkeypatch):
    """With a quiet site the scheduler sleeps a whole tick: a 300 s tick read "stalled" for 119 s of every cycle and
    Render would restart it (re-check N-3). The tick is clamped to 120 s and the limit is max(180, 3 ticks)."""
    provs, rec, c = bg
    clock = F.FrozenTime()
    monkeypatch.setattr(A, "time", clock)
    monkeypatch.setattr(A, "LIVE_TICK_SEC", 120)
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    A._LIVE_BG["started_ts"] = clock.now - 1000
    clock.now += 359
    assert c.get("/healthz").get_json()["stalled"] is False
    clock.now += 2
    r = c.get("/healthz")
    assert r.get_json()["stalled"] is True and r.status_code == 503


def test_every_worker_held_by_a_hung_fetch_asks_for_a_restart(bg, monkeypatch):
    """No overall deadline on a fetch: slow-drip feeds can hold every refresh worker forever and no list refreshes
    again (re-check N-7). /healthz says so (a restart frees them); fewer stuck fetches are reported only."""
    provs, rec, c = bg
    A._live_tick(provs)
    rec.run()
    A._live_tick(provs)
    runner = B.ThreadRunner(3)                                           # three real workers (none started yet)
    B.set_refresh_runner(runner)
    assert c.get("/healthz").status_code == 200
    now = B.time.time()                                                  # the providers' clock
    hung = provs[:2]
    for p in hung:
        p._refreshing, p._fetch_started_ts = True, now - A.LIVE_FETCH_STUCK_SEC - 1
    provs[2]._refreshing, provs[2]._fetch_started_ts = True, now - 30    # a slow fetch is not a hung one
    provs[3]._fetch_started_ts = now - 5000                              # finished long ago: not in flight
    body = c.get("/healthz").get_json()
    assert body["fetch_stuck"] is False and body["ok"] is True
    flights = {s["source"]: s["in_flight_s"] for s in body["providers"]}
    assert all(flights[p.source] > A.LIVE_FETCH_STUCK_SEC for p in hung)
    assert 29 <= flights[provs[2].source] <= 90
    assert flights[provs[3].source] is None
    provs[2]._fetch_started_ts = now - A.LIVE_FETCH_STUCK_SEC - 1
    r = c.get("/healthz")
    assert r.status_code == 503 and r.get_json()["fetch_stuck"] is True
    for p in provs[:3]:
        p._refreshing = False
    runner.shutdown()


def test_healthz_runner_counts_waiting_and_running_jobs(bg):
    """`waiting` = submitted and not started; /healthz shows it, not the old queued total (re-check N49/N50)."""
    provs, rec, c = bg
    runner = B.ThreadRunner(1)
    B.set_refresh_runner(runner)
    gate = threading.Event()
    runner.submit(lambda: gate.wait(5), "a")
    runner.submit(lambda: None, "b")
    deadline = _time.time() + 5
    while runner.running < 1 and _time.time() < deadline:
        _time.sleep(0.01)
    threads = list(runner._threads)
    try:
        assert c.get("/healthz").get_json()["runner"] == {"waiting": 1, "running": 1, "workers": 1}
    finally:
        gate.set()
        runner.shutdown()
    assert runner.queued == 0 and runner.waiting == 0
    assert threads and not any(t.is_alive() for t in threads)


def test_a_first_failure_publishes_its_age(monkeypatch):
    """age_s for a first failure's [] read ~1.8e9 s (re-check N47)."""
    clock = F.FrozenTime()
    monkeypatch.setattr(B, "time", clock)
    p = F.FakeSMHI(http=None)
    p.list_stations_versioned()
    clock.now += 42
    assert p.status()["age_s"] == 42.0


def test_the_time_zone_finder_is_built_once_under_concurrency(monkeypatch):
    """get_tz_finder re-checks under its lock: two concurrent first callers build one finder (re-check RA17)."""
    import timezonefinder
    built = []

    class Slow:
        def __init__(self):
            built.append(1)
            _time.sleep(0.2)
    monkeypatch.setattr(timezonefinder, "TimezoneFinder", Slow)
    monkeypatch.setattr(A, "tz_finder", None)
    got = []
    ts = [threading.Thread(target=lambda: got.append(A.get_tz_finder())) for _ in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(5)
    assert built == [1] and len({id(x) for x in got}) == 1


def test_the_in_flight_time_counts_from_this_fetch(monkeypatch):
    """in_flight_s starts with each fetch (not at 0 or at an earlier fetch): re-check N-7's clock."""
    clock = F.FrozenTime()
    monkeypatch.setattr(B, "time", clock)
    p = F.FakeNDBC(http=None)
    seen = []
    plain = p._fetch_stations

    def fetch():
        clock.now += 7
        seen.append(p.status()["in_flight_s"])
        return plain()
    p._fetch_stations = fetch
    p.list_stations_versioned()
    assert seen == [7.0] and p.status()["in_flight_s"] is None
    clock.now += 5000
    p.refresh()
    assert seen == [7.0, 7.0]


def test_a_stopped_runner_starts_workers_again_for_new_jobs():
    """shutdown() forgets its workers, so a later job gets a fresh one instead of waiting forever."""
    runner = B.ThreadRunner(1)
    first, second = threading.Event(), threading.Event()
    runner.submit(first.set, "a")
    assert first.wait(5)
    runner.shutdown()
    runner.submit(second.set, "b")
    try:
        assert second.wait(5)
    finally:
        runner.shutdown()
