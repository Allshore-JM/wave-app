"""Where the GFS-Wave station bulletins come from (section 40, then section 53).

Section 40 (2026-10-11): NOMADS refused the production server's address and every GFS forecast said "No recent run
found"; the run probe and the .bull were asked from NOAA's AWS copy first. Section 53 (2026-10-25): from the 10-22 18Z
run on that copy holds only run archives, so the bulletin comes from the run's bulletin archive by one byte-range read
(gfs_bulletins), NOMADS's per-station file second; the run probe HEADs the archive first. All faked -- no network.
"""
import logging
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "tools"))
import app  # noqa: E402
import gfs_bulletins as GB  # noqa: E402
import bull_archive as A  # noqa: E402
from bull_archive import S3, NOMADS  # noqa: E402

DATE, RUN = "20261025", "12"
IDS = ["13130", "41113", "51201", "46026"]


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(app, "NOMADS_DISABLED", False)
    monkeypatch.setattr(app, "BULL_ARCHIVE", True)
    stats = GB.Stats()
    monkeypatch.setattr(GB, "STATS", stats)
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
        app._WIND_CACHE.clear()
        app._RUN_CACHE.update({"ts": 0.0, "value": (None, None)})
    yield
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
        app._WIND_CACHE.clear()
        app._RUN_CACHE.update({"ts": 0.0, "value": (None, None)})


def _world(monkeypatch, runs=((DATE, RUN),), nomads_runs=None, **kw):
    """A NOAA with an archive (and NOMADS's per-station copies) for each run; the site's index = the archive's."""
    archives, nomads = {}, {}
    data = None
    for d, r in runs:
        data, texts = A.make_archive(d + r, IDS)
        archives[(d, r)] = data
    for d, r in (runs if nomads_runs is None else nomads_runs):
        for sid in IDS:
            nomads[(d, r, sid)] = A.bulletin(sid, d + r)
    fake = A.FakeNoaa(archives, nomads, **kw)
    if data is not None:
        doc = A.index_doc(runs[-1][0] + runs[-1][1], data)
        monkeypatch.setattr(GB, "INDEX", GB.parse_index(doc))
        monkeypatch.setattr(GB, "ARCHIVE_BYTES", doc["archive_bytes"])
    monkeypatch.setattr(app.HTTP, "head", fake.head)
    monkeypatch.setattr(app._PROBE_HTTP, "head", fake.head)
    monkeypatch.setattr(app.HTTP, "get", fake.get)
    monkeypatch.setattr(app._BULL_HTTP, "get", fake.get)
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: {})
    return fake


def _pin_run(monkeypatch):
    monkeypatch.setattr(app, "get_latest_run", lambda: (DATE, RUN))


def _today(monkeypatch, hour=17, minute=40):
    real = app.datetime

    class _D(real):
        @classmethod
        def utcnow(cls):
            return real(2026, 10, 25, hour, minute)
    monkeypatch.setattr(app, "datetime", _D)


# ---------------- the run probe ----------------

def test_the_run_probe_heads_the_archive_first(monkeypatch):
    _today(monkeypatch)                                                # 17:40: 18Z is still to come, 12Z the newest
    fake = _world(monkeypatch)
    assert app._detect_latest_run() == (DATE, RUN)
    heads = [(c[1], c[2]) for c in fake.calls if c[0] == "HEAD"]
    assert heads == [(S3, GB.archive_url(DATE, RUN, app.NOAA_BUCKET))], "one HEAD: nothing newer for NOMADS to have"
    assert not [c for c in fake.calls if c[0] == "GET"]
    assert GB.STATS.snapshot()["layout_suspect"] is None


def test_the_run_probe_falls_back_to_nomads(monkeypatch):
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=(), nomads_runs=((DATE, "06"),))
    monkeypatch.setattr(GB, "INDEX", {"51201": (1536, 100)})
    assert app._detect_latest_run() == (DATE, "06")
    assert fake.calls[-1][1] == NOMADS and fake.calls[-1][2].endswith("/bulls.t06z/gfswave.51201.bull")


def test_an_archive_of_another_length_counts_but_is_flagged(monkeypatch):
    _today(monkeypatch)
    fake = _world(monkeypatch, nomads_runs=(), archive_length=12345)  # NOMADS has nothing: the archive alone decides
    assert app._detect_latest_run() == (DATE, RUN)
    snap = GB.STATS.snapshot()
    assert "12345" in snap["layout_suspect"] and snap["probe_layouts"] == 1
    assert snap["reads"]["layout"] == 0, "a probe is not a read"
    fake.archive_length = None                                         # the next probe sees the index's size again
    assert app._detect_latest_run() == (DATE, RUN)
    assert GB.STATS.snapshot()["layout_suspect"] is None and GB.STATS.snapshot()["probe_layouts"] == 1


@pytest.mark.parametrize("flag,hosts", [("BULL_ARCHIVE", {NOMADS}), ("NOMADS_DISABLED", {S3})])
def test_the_diagnostic_switches_shape_the_probe(monkeypatch, flag, hosts):
    _today(monkeypatch)
    fake = _world(monkeypatch)
    monkeypatch.setattr(app, flag, flag == "NOMADS_DISABLED")
    assert app._detect_latest_run() == (DATE, RUN)
    assert {c[1] for c in fake.calls} == hosts


def test_with_no_index_the_probe_asks_nomads_only(monkeypatch):
    _today(monkeypatch)
    fake = _world(monkeypatch)
    monkeypatch.setattr(GB, "INDEX", {})
    assert app._detect_latest_run() == (DATE, RUN)
    assert {c[1] for c in fake.calls} == {NOMADS}


def test_no_run_anywhere_logs_both_answers(monkeypatch, caplog):
    _today(monkeypatch)
    _world(monkeypatch, runs=(), s3=None, nomads_all=ConnectionError("refused"))
    monkeypatch.setattr(GB, "INDEX", {"51201": (1536, 100)})
    with caplog.at_level(logging.WARNING, logger=app.logger.name):
        assert app._detect_latest_run() == (None, None)
    msg = " ".join(r.getMessage() for r in caplog.records)
    assert f"{S3} HTTP 404" in msg and f"{NOMADS} ConnectionError" in msg


# ---------------- the bulletin ----------------

def test_the_bulletin_is_one_range_read_of_the_archive(monkeypatch):
    _pin_run(monkeypatch)
    fake = _world(monkeypatch)
    _, _, _, rows, _, err = app._parse_bull_uncached("41113", None)
    assert err is None and len(rows) == 385
    gets = [c for c in fake.calls if c[0] == "GET"]
    off, size = GB.INDEX["41113"]
    assert gets == [("GET", S3, GB.archive_url(DATE, RUN, app.NOAA_BUCKET), f"bytes={off - 512}-{off + size - 1}")]
    assert GB.STATS.snapshot()["reads"]["ok"] == 1 and GB.STATS.snapshot()["nomads_fallbacks"] == 0


def test_the_table_from_the_archive_is_the_table_from_nomads(monkeypatch):
    _pin_run(monkeypatch)
    _world(monkeypatch)
    from_archive = app._parse_bull_uncached("51201", "Pacific/Honolulu")
    monkeypatch.setattr(app, "BULL_ARCHIVE", False)
    from_nomads = app._parse_bull_uncached("51201", "Pacific/Honolulu")
    assert from_archive == from_nomads and from_archive[-1] is None


@pytest.mark.parametrize("trouble", ["no archive", "range ignored", "short", "s3 down", "foreign bytes"])
def test_the_bulletin_falls_back_to_nomads(monkeypatch, trouble, caplog):
    _pin_run(monkeypatch)
    kw = {"range ignored": {"range_mode": "200"}, "short": {"range_mode": "short"},
          "s3 down": {"s3": ConnectionError("down")}}.get(trouble, {})
    fake = _world(monkeypatch, **kw)
    if trouble == "no archive":
        fake.archives.clear()
    if trouble == "foreign bytes":
        fake.archives[(DATE, RUN)] = A.plant(fake.archives[(DATE, RUN)], GB.INDEX, "41113", "51201")
    with caplog.at_level(logging.WARNING, logger=app.logger.name):
        _, _, _, rows, _, err = app._parse_bull_uncached("41113", None)
    assert err is None and rows
    assert fake.urls(NOMADS, "GET") == [f"{app.NOAA_BASE}/gfs.{DATE}/{RUN}/wave/station/bulls.t12z/gfswave.41113.bull"]
    assert GB.STATS.snapshot()["nomads_fallbacks"] == 1
    assert any("not from the AWS archive" in r.getMessage() for r in caplog.records)


def test_a_station_outside_the_index_goes_to_nomads(monkeypatch):
    _pin_run(monkeypatch)
    fake = _world(monkeypatch)
    fake.nomads[(DATE, RUN, "99999")] = A.bulletin("99999", DATE + RUN)
    assert app._parse_bull_uncached("99999", None)[-1] is None
    assert fake.urls(S3) == [] and len(fake.urls(NOMADS)) == 1


def test_nomads_text_is_checked_too(monkeypatch):
    _pin_run(monkeypatch)
    fake = _world(monkeypatch)
    fake.archives.clear()
    fake.nomads[(DATE, RUN, "41113")] = A.bulletin("41113", DATE + "06")     # another run under this run's name
    assert app._parse_bull_uncached("41113", None)[-1] == "No .bull file found for 41113"


def test_a_nomads_body_over_the_cap_is_refused(monkeypatch, caplog):
    _pin_run(monkeypatch)
    fake = _world(monkeypatch)
    fake.archives.clear()
    fake.nomads[(DATE, RUN, "41113")] = A.bulletin("41113", DATE + RUN) + " " * (GB.MAX_SIZE + 1)
    with caplog.at_level(logging.WARNING, logger=app.logger.name):
        assert app._parse_bull_uncached("41113", None)[-1] == "No .bull file found for 41113"   # the archive 404'd
    assert any("body too large" in r.getMessage() for r in caplog.records)
    assert any("BodyTooLarge" in r.getMessage() for r in caplog.records)     # its own type (G41 re-check F-5)


@pytest.mark.parametrize("s3,nomads,want", [
    (None, 404, "No .bull file found for 41113"),                       # the archive 404s too (cleared below)
    (ConnectionError("x"), ConnectionError("y"), "Could not download .bull for 41113"),
    (ConnectionError("x"), 404, "No .bull file found for 41113"),
    (None, ConnectionError("y"), "No .bull file found for 41113"),      # the archive answered (404): not "download"
])
def test_bulletin_errors(monkeypatch, s3, nomads, want):
    _pin_run(monkeypatch)
    fake = _world(monkeypatch, s3=s3, nomads_all=nomads)
    fake.archives.clear()
    assert app._parse_bull_uncached("41113", None)[-1] == want


def test_nomads_disabled_serves_from_the_archive_alone(monkeypatch):
    monkeypatch.setattr(app, "NOMADS_DISABLED", True)
    _pin_run(monkeypatch)
    fake = _world(monkeypatch)
    monkeypatch.setattr(app, "resolve_model", lambda s, m, ok=None, sok=None: "GFS")
    payload = app.compute_forecast_payload("51201", None, "US", "GFS")
    assert payload["error"] is None and payload["table_html"]
    assert {c[1] for c in fake.calls} == {S3}


def test_nomads_refusing_everything_still_serves_the_tables(monkeypatch):
    """The 2026-10-11 outage again: NOMADS answers 403 to everything; AWS has the run's archive."""
    _pin_run(monkeypatch)
    fake = _world(monkeypatch, nomads_all=403)
    monkeypatch.setattr(app, "resolve_model", lambda s, m, ok=None, sok=None: "GFS")
    payload = app.compute_forecast_payload("51201", None, "US", "GFS")
    assert payload["error"] is None and payload["table_html"]
    assert not fake.urls(NOMADS, "GET")


def test_healthz_reports_the_bulletin_sources(monkeypatch):
    _pin_run(monkeypatch)
    _world(monkeypatch)
    app._parse_bull_uncached("41113", None)
    h = app._bull_health()
    assert h["archive"] is True and h["nomads"] is True and h["index"]["members"] == len(IDS)
    assert h["reads"]["ok"] == 1 and h["last_run"] == DATE + RUN


def test_a_range_read_must_be_answered_with_206(monkeypatch):
    """A 200 of the right length is still not a range answer (a proxy that dropped the Range header)."""
    def get(url, headers=None, **k):
        a, b = (int(v) for v in headers["Range"][len("bytes="):].split("-"))
        return A.Resp(200, b"x" * (b - a + 1))
    monkeypatch.setattr(app._BULL_HTTP, "get", get)
    with pytest.raises(app.HTTPStatusError):
        app._bull_range("https://noaa-gfs-bdp-pds.s3.amazonaws.com/x", 0, 99)
    monkeypatch.setattr(app._BULL_HTTP, "get", lambda url, headers=None, **k: A.Resp(
        206, b"x" * 100, {"Content-Range": "bytes 0-99/1000"}))
    assert app._bull_range("https://noaa-gfs-bdp-pds.s3.amazonaws.com/x", 0, 99) == b"x" * 100


# ---------------- G36 fixes (plan section 53, step 7) ----------------

import http.client  # noqa: E402
import threading  # noqa: E402

import requests  # noqa: E402
from urllib3.exceptions import ProtocolError  # noqa: E402


def _dropped():
    """What requests raises when the server closed an idle keep-alive connection before answering."""
    return requests.exceptions.ConnectionError(
        ProtocolError("Connection aborted.", http.client.RemoteDisconnected("Remote end closed connection without response")))


class _Flaky:
    """A session whose first `n` GETs raise `exc`, then answers like `fake`."""

    def __init__(self, fake_get, exc, n=1):
        self.fake_get, self.exc, self.n, self.calls = fake_get, exc, n, 0

    def get(self, url, **k):
        self.calls += 1
        if self.calls <= self.n:
            raise self.exc
        return self.fake_get(url, **k)


def test_a_dropped_keep_alive_is_asked_once_more_and_the_archive_serves(monkeypatch):
    """G36 B-1: NOAA's bucket closes an idle keep-alive connection after ~5.6 s; the read meeting it is asked again."""
    _pin_run(monkeypatch)
    fake = _world(monkeypatch)
    flaky = _Flaky(fake.get, _dropped())
    monkeypatch.setattr(app._BULL_HTTP, "get", flaky.get)
    _, _, _, rows, _, err = app._parse_bull_uncached("41113", None)
    assert err is None and len(rows) == 385 and flaky.calls == 2
    snap = GB.STATS.snapshot()
    assert snap["reads"]["ok"] == 1 and snap["reads"]["transport"] == 0 and snap["nomads_fallbacks"] == 0
    assert not fake.urls(NOMADS, "GET")


@pytest.mark.parametrize("exc,calls", [
    (_dropped(), 2),                                                    # dropped twice: given up after the retry
    (requests.exceptions.ReadTimeout("slow"), 1),                       # a timeout is never repeated
    (requests.exceptions.ConnectTimeout("silent"), 1),
    (requests.exceptions.ConnectionError(ConnectionRefusedError("refused")), 1),
    (ConnectionError("down"), 1),
])
def test_only_a_dropped_connection_is_asked_again(monkeypatch, exc, calls):
    flaky = _Flaky(lambda url, **k: A.Resp(206, b"x" * 100, {"Content-Range": "bytes 0-99/1000"}), exc, n=2)
    monkeypatch.setattr(app._BULL_HTTP, "get", flaky.get)
    with pytest.raises(Exception) as got:
        app._bull_range("https://noaa-gfs-bdp-pds.s3.amazonaws.com/x", 0, 99)
    assert got.value is exc and flaky.calls == calls


def test_no_retry_without_the_flag_or_without_time_left(monkeypatch):
    ok = lambda url, **k: A.Resp(200, b"{}")                           # noqa: E731
    flaky = _Flaky(ok, _dropped())
    monkeypatch.setattr(app, "_POINT_HTTP", flaky)
    with pytest.raises(requests.exceptions.ConnectionError):
        app._capped_get("https://models.test/x", 100, None, (1, 1), 5)  # the forecast points' reads: as before
    assert flaky.calls == 1
    flaky = _Flaky(ok, _dropped())
    monkeypatch.setattr(app, "_POINT_HTTP", flaky)
    with pytest.raises(requests.exceptions.ConnectionError):
        app._capped_get("https://models.test/x", 100, None, (1, 1), 0.5, retry_dropped=True)   # < 1 s left
    assert flaky.calls == 1
    flaky = _Flaky(ok, _dropped())
    monkeypatch.setattr(app, "_POINT_HTTP", flaky)
    assert app._capped_get("https://models.test/x", 100, None, (1, 1), 5, retry_dropped=True) == b"{}"
    assert flaky.calls == 2


@pytest.mark.parametrize("mode", ["other range", "no range header"])
def test_a_range_answer_must_name_the_asked_bytes(monkeypatch, mode, caplog):
    """G36 A P3-4: a 206 of the right length from another offset (or without Content-Range) is a transport failure,
    not a layout change."""
    _pin_run(monkeypatch)
    fake = _world(monkeypatch, range_mode=mode)
    with caplog.at_level(logging.WARNING, logger=app.logger.name):
        assert app._parse_bull_uncached("41113", None)[-1] is None      # NOMADS serves
    snap = GB.STATS.snapshot()
    assert snap["reads"]["transport"] == 1 and snap["reads"]["layout"] == 0 and snap["last_layout_read"] is None
    assert any("Content-Range" in r.getMessage() for r in caplog.records)
    assert len(fake.urls(NOMADS, "GET")) == 1


def test_nomads_is_asked_only_for_cycles_newer_than_the_archive(monkeypatch):
    """G36 B-2: the archive is asked for every cycle first; NOMADS only for what is newer."""
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=((DATE, "06"),), nomads_runs=((DATE, "12"), (DATE, "06")))
    assert app._detect_latest_run() == (DATE, "12")                    # NOMADS a cycle ahead of AWS
    assert fake.urls(S3) == [GB.archive_url(DATE, "12", app.NOAA_BUCKET), GB.archive_url(DATE, "06", app.NOAA_BUCKET)]
    assert [u.split("/")[-2] for u in fake.urls(NOMADS)] == ["bulls.t12z"], "never for 06Z: the archive has it"
    fake.nomads.clear()
    fake.calls.clear()
    assert app._detect_latest_run() == (DATE, "06")
    assert len(fake.urls(NOMADS)) == 1


def test_a_silent_nomads_is_asked_once_per_probe(monkeypatch):
    """G36 B-2: a NOMADS that never answers cost 31 s per cycle, ~124 s per probe."""
    _today(monkeypatch, hour=2, minute=30)                             # no cycle of today is 3 h old: yesterday's only
    fake = _world(monkeypatch, runs=(("20261024", "18"),), nomads_all=requests.exceptions.ReadTimeout("silent"))
    assert app._detect_latest_run() == ("20261024", "18")
    assert len(fake.urls(NOMADS)) == 0, "nothing newer than the archive is old enough to be on NOMADS"
    _today(monkeypatch, hour=8, minute=30)                             # 00Z of today could be out: NOMADS asked once
    fake.calls.clear()
    assert app._detect_latest_run() == ("20261024", "18")
    assert len(fake.urls(NOMADS)) == 1
    _today(monkeypatch)                                                # 12Z, 06Z, 00Z newer than the archive: one ask
    fake.calls.clear()
    assert app._detect_latest_run() == ("20261024", "18")
    assert len(fake.urls(NOMADS)) == 1 and len(fake.urls(S3)) == 4


def test_a_silent_archive_is_asked_once_and_nomads_decides(monkeypatch):
    _today(monkeypatch)
    fake = _world(monkeypatch, s3=requests.exceptions.ConnectTimeout("silent"))
    assert app._detect_latest_run() == (DATE, RUN)
    assert len(fake.urls(S3)) == 1 and fake.urls(NOMADS)[-1].endswith("/bulls.t12z/gfswave.51201.bull")


@pytest.mark.parametrize("hour,minute,first", [(17, 40, "12"), (15, 0, "12"), (14, 59, "06"), (2, 0, None)])
def test_cycles_younger_than_three_hours_are_not_asked(monkeypatch, hour, minute, first):
    _today(monkeypatch, hour=hour, minute=minute)
    fake = _world(monkeypatch, runs=())
    app._detect_latest_run()
    heads = fake.urls(S3)
    if first is None:
        assert heads[0] == GB.archive_url("20261024", "18", app.NOAA_BUCKET)
    else:
        assert heads[0] == GB.archive_url(DATE, first, app.NOAA_BUCKET)


def test_a_dropped_connection_on_a_probe_head_is_asked_again(monkeypatch):
    _today(monkeypatch)
    fake = _world(monkeypatch, nomads_runs=())                        # only the archive's answer can find the run
    state = {"n": 0}

    def head(url, **k):
        state["n"] += 1
        if state["n"] == 1:
            raise _dropped()
        return fake.head(url, **k)
    monkeypatch.setattr(app._PROBE_HTTP, "head", head)
    assert app._detect_latest_run() == (DATE, RUN) and state["n"] == 2 and fake.urls(NOMADS) == []


def test_the_probe_heads_are_one_attempt_with_a_short_timeout(monkeypatch):
    _today(monkeypatch)
    seen = []

    def head(url, **k):
        seen.append(k.get("timeout"))
        return A.Resp(404)
    monkeypatch.setattr(app._PROBE_HTTP, "head", head)
    monkeypatch.setattr(app.HTTP, "head", lambda *a, **k: pytest.fail("the retrying session is not used"))
    app._detect_latest_run()
    assert seen and set(seen) == {app._PROBE_TIMEOUT} and max(app._PROBE_TIMEOUT) <= 5
    assert app._PROBE_HTTP.get_adapter("https://x").max_retries.total == 0


def test_one_probe_at_a_time(monkeypatch):
    """Four visitors arriving while the cached run is stale: one probe, all four get its answer."""
    go, started, calls = threading.Event(), threading.Event(), []

    def probe():
        calls.append(1)
        started.set()
        go.wait(5)
        return (DATE, RUN)
    monkeypatch.setattr(app, "_detect_latest_run", probe)
    out = []
    ts = [threading.Thread(target=lambda: out.append(app.get_latest_run())) for _ in range(4)]
    [t.start() for t in ts]
    assert started.wait(5)
    go.set()
    [t.join(5) for t in ts]
    assert out == [(DATE, RUN)] * 4 and len(calls) == 1


def test_a_failed_probe_keeps_serving_the_last_run(monkeypatch):
    answers = [(DATE, RUN), (None, None), (None, None), ("20261025", "18")]
    monkeypatch.setattr(app, "_detect_latest_run", lambda: answers.pop(0))
    assert app.get_latest_run() == (DATE, RUN)
    with app._CACHE_LOCK:
        app._RUN_CACHE["ts"] -= app._RUN_CACHE_TTL + 1
    assert app.get_latest_run() == (DATE, RUN), "NOAA unreachable: the last run's tables stay"
    assert app._RUN_CACHE["failed"] is True and len(answers) == 2
    assert app.get_latest_run() == (DATE, RUN) and len(answers) == 2, "the failure is remembered for 2 minutes"
    with app._CACHE_LOCK:
        app._RUN_CACHE["ts"] -= app._RUN_NEG_TTL + 1
        app._RUN_CACHE["good_ts"] -= app._RUN_KEEP                     # and the last run is now too old to keep
    assert app.get_latest_run() == (None, None) and len(answers) == 1
    with app._CACHE_LOCK:
        app._RUN_CACHE["ts"] -= app._RUN_NEG_TTL + 1
    assert app.get_latest_run() == ("20261025", "18") and app._RUN_CACHE["failed"] is False


def test_a_forked_child_gets_a_fresh_probe_lock():
    old = app._RUN_PROBE_LOCK
    old.acquire()
    try:
        app._live_after_fork()
        assert app._RUN_PROBE_LOCK is not old and app._RUN_PROBE_LOCK.acquire(blocking=False)
        app._RUN_PROBE_LOCK.release()
    finally:
        old.release()


def test_yesterdays_same_cycle_is_asked_too(monkeypatch):
    """The probe walks today's cycles, then yesterday's: the archive of yesterday's 12Z (not today's) is found."""
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=(("20261024", "12"),), nomads_runs=())
    assert app._detect_latest_run() == ("20261024", "12")
    heads = fake.urls(S3)
    assert heads[-1] == GB.archive_url("20261024", "12", app.NOAA_BUCKET) and len(heads) == 5
    assert heads[3] == GB.archive_url("20261024", "18", app.NOAA_BUCKET)
    assert [u.split("/gfs.")[1][:8] for u in fake.urls(NOMADS)] == ["20261025"] * 3 + ["20261024"]


# ---------------- G36 re-check fixes (plan section 53, fix round 2) ----------------

import time  # noqa: E402

from urllib3.exceptions import ConnectTimeoutError, MaxRetryError, ReadTimeoutError  # noqa: E402


def _chain(*levels):
    """An exception raised the way urllib3 / requests raise them: each level `raise ... from` the one below."""
    exc = None
    for make in levels:
        try:
            if exc is None:
                raise make()
            raise make() from exc
        except BaseException as e:            # noqa: B902
            exc = e
    return exc


def _real_read_timeout():
    """requests' ReadTimeout > urllib3's ReadTimeoutError > TimeoutError, as the re-check saw them."""
    inner = _chain(lambda: TimeoutError("timed out"),
                   lambda: ReadTimeoutError(None, "https://x/y", "Read timed out. (read timeout=10)"))
    return requests.exceptions.ReadTimeout(inner)


def _real_connect_timeout():
    """ConnectTimeout > MaxRetryError > ConnectTimeoutError > TimeoutError."""
    inner = _chain(lambda: TimeoutError("timed out"),
                   lambda: ConnectTimeoutError(None, "Connection to x timed out. (connect timeout=3)"))
    return requests.exceptions.ConnectTimeout(_chain(lambda: inner, lambda: MaxRetryError(None, "https://x/y", inner)))


def _ok_206(url, **k):
    return A.Resp(206, b"x" * 100, {"Content-Range": "bytes 0-99/1000"})


@pytest.mark.parametrize("inner", [ConnectionResetError(10054, "reset by peer"),
                                   ConnectionAbortedError(10053, "aborted"), BrokenPipeError(32, "broken pipe")])
def test_a_reset_an_abort_or_a_broken_pipe_is_asked_again(monkeypatch, inner):
    flaky = _Flaky(_ok_206, requests.exceptions.ConnectionError(ProtocolError("Connection aborted.", inner)))
    monkeypatch.setattr(app._BULL_HTTP, "get", flaky.get)
    assert app._bull_range("https://noaa-gfs-bdp-pds.s3.amazonaws.com/x", 0, 99) == b"x" * 100 and flaky.calls == 2


@pytest.mark.parametrize("make", [_real_read_timeout, _real_connect_timeout])
def test_a_real_timeout_chain_is_never_asked_again(monkeypatch, make):
    """The timeouts as urllib3 really raises them (TimeoutError at the bottom of the chain) are not repeated."""
    exc = make()
    assert not app._dropped_connection(exc)
    flaky = _Flaky(_ok_206, exc, n=2)
    monkeypatch.setattr(app._BULL_HTTP, "get", flaky.get)
    with pytest.raises(type(exc)):
        app._bull_range("https://noaa-gfs-bdp-pds.s3.amazonaws.com/x", 0, 99)
    assert flaky.calls == 1


def test_the_second_attempt_stays_inside_the_time_left(monkeypatch):
    """Re-check F-2: the retry's connect and read timeouts are clipped to what is left of max_s."""
    clock = {"t": 100.0}
    monkeypatch.setattr(app.time, "monotonic", lambda: clock["t"])
    seen = []

    def get(url, timeout=None, **k):
        seen.append(timeout)
        if len(seen) == 1:
            clock["t"] += 5.5                            # the server held the request, then dropped it
            raise _dropped()
        return _ok_206(url)
    session = type("S", (), {"get": staticmethod(get)})()
    got = app._capped_get("https://x/y", 100, None, (3, 10), 8, session=session, byte_range=(0, 99), retry_dropped=True)
    assert got == b"x" * 100 and seen == [(3, 10), (2.5, 2.5)]


def test_a_range_answer_from_a_longer_offset_is_refused(monkeypatch):
    """`bytes 15360-...` is not an answer to a request from 1536."""
    monkeypatch.setattr(app._BULL_HTTP, "get", lambda url, **k: A.Resp(206, b"x" * 100,
                                                                       {"Content-Range": "bytes 15360-15459/99999"}))
    with pytest.raises(IOError):
        app._bull_range("https://noaa-gfs-bdp-pds.s3.amazonaws.com/x", 1536, 1635)


def _first_answers(monkeypatch, fake, answers):
    """HEADs whose URL holds a key answer the listed statuses first, then as the fake NOAA does."""
    def head(url, **k):
        for key, seq in answers.items():
            if key in url and seq:
                fake.calls.append(("HEAD", url.split("/")[2], url, None))
                return A.Resp(seq.pop(0), headers={"Content-Length": str(GB.ARCHIVE_BYTES)})
        return fake.head(url, **k)
    monkeypatch.setattr(app._PROBE_HTTP, "head", head)


def test_a_transient_5xx_on_the_newest_archive_is_asked_again(monkeypatch):
    """Re-check F-1: one 503 (S3 "SlowDown") on the newest archive's HEAD: asked once more, the newest run found."""
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=((DATE, "06"), (DATE, "12")), nomads_runs=())
    _first_answers(monkeypatch, fake, {"/gfs.20261025/12/": [503]})
    assert app._detect_latest_run() == (DATE, "12")
    assert fake.urls(S3) == [GB.archive_url(DATE, "12", app.NOAA_BUCKET)] * 2


def test_a_5xx_that_stays_never_makes_an_older_run_look_newest(monkeypatch):
    """NOMADS off, the newest archive answers 503 twice: the probe fails and the run already found stays (it never
    goes back to the older archive that is there)."""
    monkeypatch.setattr(app, "NOMADS_DISABLED", True)
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=((DATE, "06"), (DATE, "12")))
    assert app.get_latest_run() == (DATE, "12")
    with app._CACHE_LOCK:
        app._RUN_CACHE["ts"] -= app._RUN_CACHE_TTL + 1
    fake.calls.clear()
    _first_answers(monkeypatch, fake, {"/gfs.20261025/12/": [503, 503]})
    assert app.get_latest_run() == (DATE, "12") and app._RUN_CACHE["failed"] is True
    assert fake.urls(S3) == [GB.archive_url(DATE, "12", app.NOAA_BUCKET)] * 2, "06Z is never asked"


def test_a_403_from_the_archive_stops_its_scan_and_nomads_decides(monkeypatch):
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=((DATE, "06"), (DATE, "12")))
    _first_answers(monkeypatch, fake, {"/gfs.20261025/12/wave/station/gfswave": [403]})
    assert app._detect_latest_run() == (DATE, "12")
    assert len(fake.urls(S3)) == 1 and fake.urls(NOMADS)[0].endswith("/bulls.t12z/gfswave.51201.bull")


def test_nomads_403_means_not_written_yet_and_its_5xx_stops_asking(monkeypatch):
    """NOMADS answers 403 for a cycle it has not written: the probe goes on to the next newer cycle. A 5xx (asked
    twice) stops asking NOMADS: the archive's run is served."""
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=((DATE, "00"),), nomads_runs=((DATE, "06"),))
    _first_answers(monkeypatch, fake, {"/bulls.t12z/": [403]})
    assert app._detect_latest_run() == (DATE, "06")
    fake.calls.clear()
    _first_answers(monkeypatch, fake, {"/bulls.t12z/": [503, 503]})
    assert app._detect_latest_run() == (DATE, "00")
    assert [u.split("/")[-2] for u in fake.urls(NOMADS)] == ["bulls.t12z", "bulls.t12z"], "06Z is not asked"


def _clocked_heads(monkeypatch, fake, step):
    """Each HEAD takes `step` seconds on a fake clock; returns the timeouts the HEADs were given."""
    clock = {"t": 1000.0}
    monkeypatch.setattr(app.time, "monotonic", lambda: clock["t"])
    seen = []

    def head(url, **k):
        seen.append(k.get("timeout"))
        clock["t"] += step
        return fake.head(url, **k)
    monkeypatch.setattr(app._PROBE_HTTP, "head", head)
    return seen


def test_the_probe_stops_at_its_budget(monkeypatch):
    """Re-check F-3: no HEAD is started past _PROBE_BUDGET_S, and each one's timeouts are clipped to what is left."""
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=())                          # every archive 404: the scan would go on
    seen = _clocked_heads(monkeypatch, fake, 6)
    assert app._detect_latest_run() == (None, None)
    assert seen == [(3, 5), (3, 5), (3, 3)] and not fake.urls(NOMADS)


def test_out_of_time_the_archive_found_is_served(monkeypatch):
    _today(monkeypatch, hour=23, minute=50)                      # 18Z is old enough to ask
    fake = _world(monkeypatch, runs=((DATE, "12"),), nomads_runs=((DATE, "18"),))
    _clocked_heads(monkeypatch, fake, 8)
    assert app._detect_latest_run() == (DATE, "12"), "NOMADS's 18Z is not asked once the budget is spent"
    assert not fake.urls(NOMADS)


def test_a_caller_that_finds_a_probe_running_gets_the_run_held(monkeypatch):
    """Re-check F-3: while one request probes, the others take the run held instead of waiting."""
    go, started = threading.Event(), threading.Event()

    def probe():
        started.set()
        go.wait(5)
        return (DATE, "18")
    monkeypatch.setattr(app, "_detect_latest_run", probe)
    with app._CACHE_LOCK:
        app._RUN_CACHE.update(ts=time.time() - app._RUN_CACHE_TTL - 1, value=(DATE, RUN), good=(DATE, RUN),
                              good_ts=time.time() - 3600, failed=False)
    out = []
    t = threading.Thread(target=lambda: out.append(app.get_latest_run()))
    t.start()
    assert started.wait(5)
    t0 = time.time()
    assert app.get_latest_run() == (DATE, RUN) and time.time() - t0 < 1, "served at once, not after the probe"
    go.set()
    t.join(5)
    assert out == [(DATE, "18")] and app.get_latest_run() == (DATE, "18")


@pytest.mark.parametrize("held", ["none", "too old"])
def test_with_no_run_held_a_caller_waits_for_the_probe(monkeypatch, held):
    """Nothing to serve (no run yet, or the last one older than _RUN_KEEP): the caller waits for the probe."""
    if held == "too old":
        with app._CACHE_LOCK:
            app._RUN_CACHE.update(ts=time.time() - app._RUN_CACHE_TTL - 1, value=(DATE, RUN), good=(DATE, RUN),
                                  good_ts=time.time() - app._RUN_KEEP - 1, failed=False)
    go, started = threading.Event(), threading.Event()

    def probe():
        started.set()
        go.wait(5)
        return (DATE, "18")
    monkeypatch.setattr(app, "_detect_latest_run", probe)
    out = []
    ts = [threading.Thread(target=lambda: out.append(app.get_latest_run())) for _ in range(2)]
    ts[0].start()
    assert started.wait(5)
    ts[1].start()
    ts[1].join(0.3)
    assert ts[1].is_alive(), "nothing to serve: it waits for the probe"
    go.set()
    [t.join(5) for t in ts]
    assert out == [(DATE, "18")] * 2


def test_healthz_reports_the_run_probe(monkeypatch):
    with app._CACHE_LOCK:
        now = time.time()
        app._RUN_CACHE.update(ts=now - 30, value=(DATE, RUN), good=(DATE, RUN), good_ts=now - 7200, failed=True)
    r = app._run_health()
    assert r["run"] == DATE + RUN and r["failed"] is True and r["good_run"] == DATE + RUN
    assert 29 <= r["probe_age_s"] <= 32 and 7199 <= r["good_age_s"] <= 7202
    with app.app.test_client() as c:
        assert c.get("/healthz").get_json()["gfs"]["run"]["run"] == DATE + RUN


def test_a_forked_child_gets_a_fresh_stats_lock():
    old = GB.STATS.lock
    old.acquire()
    try:
        app._live_after_fork()
        assert GB.STATS.lock is not old and GB.STATS.lock.acquire(blocking=False)
        GB.STATS.lock.release()
    finally:
        old.release()


# ---------------- fix round 3 (the fresh check of fix round 2) ----------------


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_each_retried_status_is_asked_once_more(monkeypatch, status):
    """Fresh check F1: every status in _PROBE_RETRY_STATUS gets its second ask: the newest run is found."""
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=((DATE, "06"), (DATE, "12")), nomads_runs=())
    _first_answers(monkeypatch, fake, {"/gfs.20261025/12/": [status]})
    assert app._detect_latest_run() == (DATE, "12")
    assert fake.urls(S3) == [GB.archive_url(DATE, "12", app.NOAA_BUCKET)] * 2


@pytest.mark.parametrize("status", [301, 400, 410, 501])
def test_any_other_archive_answer_stops_its_scan(monkeypatch, status):
    """Fresh check F1: only a 404 means "not there"; a 3xx, another 4xx or a 501 is not asked again and never makes the
    older archive the newest (NOMADS off: nothing confirmed, the probe fails)."""
    monkeypatch.setattr(app, "NOMADS_DISABLED", True)
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=((DATE, "06"), (DATE, "12")))
    _first_answers(monkeypatch, fake, {"/gfs.20261025/12/": [status, status]})
    assert app._detect_latest_run() == (None, None)
    assert fake.urls(S3) == [GB.archive_url(DATE, "12", app.NOAA_BUCKET)], "asked once, 06Z never"


@pytest.mark.parametrize("answers,asked", [([301], 1), ([500, 500], 2), ([410], 1)])
def test_any_other_nomads_answer_stops_asking_nomads(monkeypatch, answers, asked):
    """Fresh check F1: from NOMADS only a 403 or 404 means "not written yet"; a 3xx, another 4xx or a 5xx asked twice
    stops asking NOMADS and the archive's run is served."""
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=((DATE, "00"),), nomads_runs=((DATE, "06"),))
    _first_answers(monkeypatch, fake, {"/bulls.t12z/": list(answers)})
    assert app._detect_latest_run() == (DATE, "00")
    assert [u.split("/")[-2] for u in fake.urls(NOMADS)] == ["bulls.t12z"] * asked, "06Z is not asked"


def _clocked(monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(app.time, "monotonic", lambda: clock["t"])
    return clock


def test_a_late_drop_gives_the_retry_the_time_left(monkeypatch):
    """Fresh check F1 (P5): the second HEAD's timeouts are clipped to what is left when it starts."""
    clock = _clocked(monkeypatch)
    seen = []

    def head(url, timeout=None, **k):
        seen.append(timeout)
        if len(seen) == 1:
            clock["t"] += 4.0                            # the HEAD was held, then dropped unanswered
            raise _dropped()
        return A.Resp(200, headers={"Content-Length": "7"})
    monkeypatch.setattr(app._PROBE_HTTP, "head", head)
    assert app._probe_head("https://x/y", 1006.0) == (200, 7)
    assert seen == [(3, 5), (2.0, 2.0)]


@pytest.mark.parametrize("first", ["drop", 503])
@pytest.mark.parametrize("taken", [5.5, 7.0])
def test_no_second_ask_without_a_second_of_the_probe_left(monkeypatch, first, taken):
    """Fresh check F2: a drop or a 503 that comes with less than _RETRY_MIN_LEFT_S of the probe left (or after its
    deadline) is not asked again: one HEAD, its own answer stands."""
    clock = _clocked(monkeypatch)
    calls = []

    def head(url, timeout=None, **k):
        calls.append(timeout)
        clock["t"] += taken
        if first == "drop":
            raise _dropped()
        return A.Resp(503, headers={"Content-Length": "0"})
    monkeypatch.setattr(app._PROBE_HTTP, "head", head)
    got = app._probe_head("https://x/y", 1006.0)
    assert len(calls) == 1
    assert got == ((None, "ConnectionError") if first == "drop" else (503, 0))


def test_a_late_503_on_the_newest_archive_ends_the_scan(monkeypatch):
    """Fresh check F2 through the probe: the newest archive's 503 comes 14.5 s into the budget: not asked again, the
    scan stops, nothing older is asked."""
    monkeypatch.setattr(app, "NOMADS_DISABLED", True)
    _today(monkeypatch)
    fake = _world(monkeypatch, runs=((DATE, "06"), (DATE, "12")))
    clock = _clocked(monkeypatch)

    def head(url, **k):
        clock["t"] += 14.5
        fake.calls.append(("HEAD", url.split("/")[2], url, None))
        return A.Resp(503, headers={"Content-Length": "0"})
    monkeypatch.setattr(app._PROBE_HTTP, "head", head)
    assert app._detect_latest_run() == (None, None) and len(fake.urls(S3)) == 1


def test_the_retry_reads_its_body_within_the_same_time_limit(monkeypatch):
    """Fresh check F1 (C5): after a dropped first attempt the second one's body must still end within the first max_s;
    a body that trickles past it is cut."""
    clock = _clocked(monkeypatch)

    class Trickle(A.Resp):
        def iter_content(self, n=65536, **k):
            for i in range(10):
                clock["t"] += 1.0
                yield b"x" * 10
    calls = []

    def get(url, timeout=None, **k):
        calls.append(timeout)
        if len(calls) == 1:
            clock["t"] += 2.0
            raise _dropped()
        return Trickle(206, b"", {"Content-Range": "bytes 0-99/1000"})
    session = type("S", (), {"get": staticmethod(get)})()
    with pytest.raises(IOError, match="too slow"):
        app._capped_get("https://x/y", 100, None, (3, 10), 8, session=session, byte_range=(0, 99), retry_dropped=True)
    assert len(calls) == 2 and clock["t"] <= 1000.0 + 8 + 1.0


def test_a_range_answer_with_another_end_is_refused(monkeypatch):
    """Fresh check F1 (R5): the same start but another end is not the asked range."""
    monkeypatch.setattr(app._BULL_HTTP, "get", lambda url, **k: A.Resp(206, b"x" * 100,
                                                                       {"Content-Range": "bytes 1536-9999/99999"}))
    with pytest.raises(IOError):
        app._bull_range("https://noaa-gfs-bdp-pds.s3.amazonaws.com/x", 1536, 1635)


def test_healthz_run_after_a_good_probe(monkeypatch):
    """Fresh check F1 (H1-H5): each field from its own source."""
    with app._CACHE_LOCK:
        now = time.time()
        app._RUN_CACHE.update(ts=now - 60, value=(DATE, "12"), good=(DATE, "06"), good_ts=now - 600, failed=False)
    r = app._run_health()
    assert r == {"run": DATE + "12", "failed": False, "probe_age_s": r["probe_age_s"], "good_run": DATE + "06",
                 "good_age_s": r["good_age_s"]}
    assert 59 <= r["probe_age_s"] <= 62 and 599 <= r["good_age_s"] <= 602


def test_healthz_run_with_the_kept_run_expired_and_before_any_probe(monkeypatch):
    with app._CACHE_LOCK:
        now = time.time()
        app._RUN_CACHE.update(ts=now - 10, value=(None, None), good=(DATE, RUN), good_ts=now - app._RUN_KEEP - 60,
                              failed=True)
    r = app._run_health()
    assert r["run"] is None and r["failed"] is True and r["good_run"] == DATE + RUN
    with app._CACHE_LOCK:
        app._RUN_CACHE.update(ts=0.0, value=(None, None), good=None, good_ts=0.0, failed=False)
    assert app._run_health() == {"run": None, "failed": False, "probe_age_s": None, "good_run": None,
                                 "good_age_s": None}


def test_a_clipped_timeout_keeps_a_tenth_of_a_second_at_least():
    """Fresh check F1 (K1/K2): each part at most the time left, never under 0.1 s."""
    assert app._clip_timeout((3, 5), 10) == (3, 5)
    assert app._clip_timeout((3, 5), 0.5) == (0.5, 0.5)
    assert app._clip_timeout((3, 5), -2.0) == (0.1, 0.1)
    assert app._clip_timeout(10, 4) == 4
