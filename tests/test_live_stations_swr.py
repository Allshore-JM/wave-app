"""Live buoys at once (plan section 36), provider half: stale-while-revalidate in BuoyProvider.

A provider's list in hand is served at once; a due refresh is a background job on the module
runner (one per provider at a time); a failure keeps the last good list and retries after
retry_after_sec (a FIRST failure too); an empty list after a good one counts as a failure;
only a provider with no list makes callers wait, and concurrent cold callers wait on ONE
fetch. The four providers that used to swallow their own request errors now raise, so the
keep-last rule covers them; CMEMS swaps its file map atomically.

The golden replay (tests/test_live_stations_memo.py) runs under the inline runner from
tests/conftest.py and proves the bytes and the fetch schedule unchanged.
"""
import os
import sys
import threading
import time as _time

import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import buoy_sources as B  # noqa: E402
import fake_buoy_providers as F  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_fetch_counters():
    for c in F.FAKE_CLASSES:          # class-level counters: never carry across tests
        c.fetch_calls = 0
    yield
    for c in F.FAKE_CLASSES:
        c.fetch_calls = 0


@pytest.fixture
def clock(monkeypatch):
    frozen = F.FrozenTime()
    monkeypatch.setattr(B, "time", frozen)
    return frozen


class RecordingRunner:
    """Collects jobs instead of running them: the caller must not wait for a due refresh."""
    def __init__(self):
        self.jobs = []

    def submit(self, fn, name="x"):
        self.jobs.append((name, fn))

    def run_all(self):
        jobs, self.jobs = self.jobs, []
        for _, fn in jobs:
            fn()


@pytest.fixture
def recorder():
    r = RecordingRunner()
    prev = B.set_refresh_runner(r)
    yield r
    B.set_refresh_runner(prev)


def _flaky(p, fail):
    orig = p._fetch_stations
    fail.append(0)

    def fetch():
        fail[1] += 1
        if fail[0]:
            raise RuntimeError("upstream down")
        return orig()
    p._fetch_stations = fetch


# ------------------------------ stale-while-revalidate ------------------------------

def test_stale_list_is_served_at_once_and_one_job_is_queued(clock, recorder):
    p = F.FakeCDIP(http=None)
    lst1, v1, ts1 = p.list_stations_versioned()           # cold: fetched inline (no job)
    assert v1 == 1 and len(lst1) == 2 and recorder.jobs == []
    clock.now += p.list_ttl_sec * p.refresh_at + 1          # due by the refresh_at rule
    assert p.refresh_due()
    for _ in range(3):
        lst, v, ts = p.list_stations_versioned()
        assert (lst, v, ts) == (lst1, v1, ts1)             # the list in hand, at once
    assert len(recorder.jobs) == 1                          # one job, not three
    assert F.FakeCDIP.fetch_calls == 1                      # nothing fetched inline
    assert p.status()["pending"] is True
    recorder.run_all()
    lst2, v2, _ = p.list_stations_versioned()
    assert v2 == 2 and lst2 == lst1 and F.FakeCDIP.fetch_calls == 2
    assert p.status()["pending"] is False and not p.refresh_due()


def test_not_due_before_refresh_at(clock, recorder):
    p = F.FakeCDIP(http=None)
    p.list_stations_versioned()
    clock.now += p.list_ttl_sec * p.refresh_at - 1
    assert not p.refresh_due()
    p.list_stations_versioned()
    assert recorder.jobs == []


def test_hard_expiry_still_counts_as_due(clock, recorder):
    p = F.FakeCDIP(http=None)
    p.list_stations_versioned()
    p._list_ts = 0.0                                        # the tests' way of forcing a refresh
    assert p.refresh_due()
    p.list_stations_versioned()
    assert len(recorder.jobs) == 1


def test_failure_keeps_the_list_and_retries_after_retry_after(clock, recorder):
    p = F.FakeCDIP(http=None)
    fail = [False]
    _flaky(p, fail)
    lst1, v1, _ = p.list_stations_versioned()
    fail[0] = True
    clock.now += p.list_ttl_sec + 1
    p.list_stations_versioned()
    recorder.run_all()                                      # the failing refresh
    lst, v, _ = p.list_stations_versioned()
    assert lst == lst1 and v == v1                          # kept, same version
    st = p.status()
    assert st["last_error"] and "upstream down" in st["last_error"]
    assert st["due_in_s"] == pytest.approx(p.retry_after_sec, abs=1)
    assert not p.refresh_due()
    clock.now += p.retry_after_sec
    assert p.refresh_due()
    fail[0] = False
    p.list_stations_versioned()
    recorder.run_all()
    lst2, v2, _ = p.list_stations_versioned()
    assert lst2 == lst1 and v2 == v1 + 1 and p.status()["last_error"] is None


def test_first_failure_retries_after_retry_after_sec(clock, recorder):
    p = F.FakeSMHI(http=None)                               # raises from the start
    lst, v, _ = p.list_stations_versioned()                 # cold: inline, publishes []
    assert lst == [] and v == 1 and F.FakeSMHI.fetch_calls == 1
    clock.now += p.retry_after_sec - 1
    assert not p.refresh_due()
    clock.now += 2
    assert p.refresh_due()                                  # not a whole TTL any more
    p.list_stations_versioned()
    assert len(recorder.jobs) == 1


def test_empty_list_after_a_good_one_is_kept(clock, recorder):
    p = F.FakeCDIP(http=None)
    lst1, v1, _ = p.list_stations_versioned()
    p._fetch_stations = lambda: []
    clock.now += p.list_ttl_sec + 1
    p.list_stations_versioned()
    recorder.run_all()
    lst, v, _ = p.list_stations_versioned()
    assert lst == lst1 and v == v1
    assert "empty list" in p.status()["last_error"]
    # ... but an empty list from a provider that never had one is a real (empty) publish
    q = F.FakeQLD(http=None)
    assert q.list_stations_versioned()[:2] == ([], 1) and q.status()["last_error"] is None


def test_cold_callers_wait_on_one_fetch():
    """Four threads ask a provider with no list while its fetch takes a while: one upstream
    fetch, every caller gets its publish (the default THREAD runner is in use here: a cold
    caller never goes through the runner anyway)."""
    B.set_refresh_runner(B.ThreadRunner(2))
    p = F.FakeCDIP(http=None)
    orig = p._fetch_stations
    started = threading.Event()

    def slow():
        started.set()
        _time.sleep(0.25)
        return orig()
    p._fetch_stations = slow
    results = []
    seen = []

    def watch():                                            # while the fetch is in flight
        started.wait(5)
        seen.append((p.status()["in_flight"], p.schedule_refresh()))
    threading.Thread(target=watch).start()

    def go():
        results.append(p.list_stations_versioned())
    ts = [threading.Thread(target=go) for _ in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(5)
    assert len(results) == 4 and F.FakeCDIP.fetch_calls == 1
    assert {r[1] for r in results} == {1} and all(len(r[0]) == 2 for r in results)
    assert not p._refresh_lock.locked() and p.status()["in_flight"] is False
    assert seen == [(True, False)]                          # in flight: no second job queued


def test_thread_runner_bounds_concurrency_and_counts():
    r = B.ThreadRunner(2)
    gate = threading.Event()
    peak = [0]
    n = [0]
    lock = threading.Lock()

    def job():
        with lock:
            n[0] += 1
            peak[0] = max(peak[0], n[0])
        gate.wait(5)
        with lock:
            n[0] -= 1
    for i in range(5):
        r.submit(job, "j%d" % i)
    deadline = _time.time() + 5
    while r.running < 2 and _time.time() < deadline:
        _time.sleep(0.01)
    _time.sleep(0.1)
    assert r.running == 2 and r.queued == 5 and peak[0] == 2
    gate.set()
    deadline = _time.time() + 5
    while r.queued and _time.time() < deadline:
        _time.sleep(0.01)
    assert r.queued == 0 and r.running == 0 and peak[0] == 2


def test_thread_runner_survives_a_raising_job_and_a_background_refresh_publishes():
    r = B.ThreadRunner(1)
    B.set_refresh_runner(r)

    def boom():
        raise RuntimeError("job died")
    r.submit(boom, "boom")
    p = F.FakeCDIP(http=None)
    p.list_stations_versioned()
    p._list_ts = 0.0
    assert p.schedule_refresh() is True
    assert p.schedule_refresh() is False                    # one at a time
    deadline = _time.time() + 5
    while p.status()["pending"] and _time.time() < deadline:
        _time.sleep(0.01)
    assert p.snapshot()[1] == 2 and F.FakeCDIP.fetch_calls == 2


def test_scheduled_job_skips_when_no_longer_due(clock, recorder):
    """A cold caller refreshed inline while the job waited: the job does not fetch again."""
    p = F.FakeCDIP(http=None)
    p.list_stations_versioned()
    p._list_ts = 0.0
    p.list_stations_versioned()                             # queues the job
    p.refresh()                                             # someone forces a refresh first
    assert F.FakeCDIP.fetch_calls == 2
    recorder.run_all()
    assert F.FakeCDIP.fetch_calls == 2 and p.status()["pending"] is False


def test_status_fields(clock):
    p = F.FakeCDIP(http=None)
    st = p.status()
    assert st["version"] is None and st["stations"] is None and st["due_in_s"] == 0.0
    p.list_stations_versioned()
    st = p.status()
    assert st["version"] == 1 and st["stations"] == 2 and st["age_s"] == 0.0
    assert st["due_in_s"] == pytest.approx(p.list_ttl_sec * p.refresh_at, abs=1)
    assert st["last_error"] is None and st["last_duration_s"] is not None
    assert st["in_flight"] is False and st["pending"] is False


# ------------------------------ the raising providers ------------------------------

class _Resp:
    def __init__(self, status=200, body="[]"):
        self.status_code = status
        self.text = body

    def json(self):
        import json
        return json.loads(self.text)

    def iter_lines(self, decode_unicode=False, chunk_size=None):
        yield from self.text.splitlines()

    def close(self):
        pass


class _HTTP:
    def __init__(self, resp=None, exc=None):
        self.resp, self.exc = resp, exc

    def get(self, url, **kw):
        if self.exc:
            raise self.exc
        return self.resp


@pytest.mark.parametrize("cls", [B.AusWavesProvider, B.CefasWaveNetProvider, B.SmhiProvider,
                                 B.CopernicusProvider])
def test_request_errors_and_non_200_raise_so_keep_last_applies(cls, clock):
    with pytest.raises(requests.RequestException):
        cls(http=_HTTP(exc=requests.ConnectionError("down")))._fetch_stations()
    with pytest.raises(RuntimeError):
        cls(http=_HTTP(_Resp(503, "[]")))._fetch_stations()
    # ... and through the base class: a good list survives such a failure
    p = cls(http=_HTTP(_Resp(503, "[]")))
    p._list_cache = [{"id": "x"}]
    p._list_ts = p._list_ok_ts = clock.now
    p._list_version = 3
    p._list_ts = 0.0
    lst, v, _ = p.refresh()
    assert lst == [{"id": "x"}] and v == 3 and "HTTP 503" in p.status()["last_error"]


def test_cmems_file_map_is_swapped_whole(monkeypatch):
    """detail() never sees a half-built map: the old map stays until the new list is complete."""
    import capture_cmems_golden as G
    sample = open(G.SAMPLE, encoding="utf-8").read()
    monkeypatch.setattr(B, "time", F.FrozenTime(G.NOW_EPOCH))      # the sample's files are "recent"
    p = B.CopernicusProvider(http=G.FakeHTTP(sample))
    p._fetch_stations()
    before = dict(p._file_by_id)
    assert before
    seen = []

    class Boom(G._Resp):
        def iter_lines(self, decode_unicode=False, chunk_size=None):
            seen.append(dict(p._file_by_id))                 # observed while the new index streams
            yield from list(super().iter_lines(decode_unicode))[:50]
            raise requests.exceptions.ChunkedEncodingError("cut")

    class H:
        def get(self, url, **kw):
            return Boom(sample)
    p.http = H()
    with pytest.raises(requests.RequestException):
        p._fetch_stations()
    assert p._file_by_id == before and seen == [before]
    # ... and while the new map is being BUILT (haversine_km runs once per platform in that
    # loop) the old map is still the one in place
    during = []
    real_hav = B.haversine_km

    def spy(*a):
        during.append(dict(p._file_by_id))
        return real_hav(*a)
    monkeypatch.setattr(B, "haversine_km", spy)
    p.http = G.FakeHTTP(sample)
    out = p._fetch_stations()
    assert len(out) == len(before) and during and all(d == before for d in during)


def test_a_feed_that_keeps_failing_does_not_republish(clock, recorder):
    """A dead feed retried every retry_after_sec published a new (empty) version each time, so the merged list was
    rebuilt for nothing (seen on the test site: the Marine Institute's ERDDAP timing out for hours)."""
    seen = []
    B.add_publish_listener(seen.append)
    try:
        p = F.FakeSMHI(http=None)
        assert p.list_stations_versioned()[:2] == ([], 1) and seen == [p]
        for _ in range(3):
            clock.now += p.retry_after_sec + 1
            p.list_stations_versioned()
            recorder.run_all()
        assert p.snapshot()[1] == 1 and seen == [p] and F.FakeSMHI.fetch_calls == 4
        assert "SMHI down" in p.status()["last_error"]
    finally:
        B.remove_publish_listener(seen.append)
