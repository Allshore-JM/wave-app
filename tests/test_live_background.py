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
            "tz_loaded": False}


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
    assert body["runner"] == {"queued": 0, "running": 0, "workers": 0}


def test_healthz_200_after_the_deadline_even_when_a_feed_is_dead(bg, monkeypatch):
    provs, rec, c = bg
    A._LIVE_BG["started_ts"] = _time.time() - A.LIVE_WARM_DEADLINE_SEC - 1
    r = c.get("/healthz")
    assert r.status_code == 200 and r.get_json()["ok"] is True and r.get_json()["warm"] is False


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
    held = [A._LIVE_BUILD_LOCK, A._TZ_FINDER_LOCK, A._CACHE_LOCK, A._LIVE_BG_LOCK]
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
        assert B._REFRESH_RUNNER is None and not B._RUNNER_LOCK.locked()
        assert A._LIVE_BG["thread"] is None and A._LIVE_BG["started_ts"] is None and A._LIVE_BG["warm_ts"] is None
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
