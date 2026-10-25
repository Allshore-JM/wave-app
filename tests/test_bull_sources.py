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
    monkeypatch.setattr(app.HTTP, "get", fake.get)
    monkeypatch.setattr(app._BULL_HTTP, "get", fake.get)
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: {})
    return fake


def _pin_run(monkeypatch):
    monkeypatch.setattr(app, "get_latest_run", lambda: (DATE, RUN))


def _today(monkeypatch):
    real = app.datetime

    class _D(real):
        @classmethod
        def utcnow(cls):
            return real(2026, 10, 25, 17, 40)
    monkeypatch.setattr(app, "datetime", _D)


# ---------------- the run probe ----------------

def test_the_run_probe_heads_the_archive_first(monkeypatch):
    _today(monkeypatch)
    fake = _world(monkeypatch)
    assert app._detect_latest_run() == (DATE, RUN)
    heads = [(c[1], c[2]) for c in fake.calls if c[0] == "HEAD"]
    assert heads[-1] == (S3, GB.archive_url(DATE, RUN, app.NOAA_BUCKET))
    assert (S3, GB.archive_url(DATE, "18", app.NOAA_BUCKET)) == heads[0], "the newest cycle first"
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
    _world(monkeypatch, archive_length=12345)
    assert app._detect_latest_run() == (DATE, RUN)
    assert "12345" in GB.STATS.snapshot()["layout_suspect"]


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
    monkeypatch.setattr(app, "resolve_model", lambda s, m: "GFS")
    payload = app.compute_forecast_payload("51201", None, "US", "GFS")
    assert payload["error"] is None and payload["table_html"]
    assert {c[1] for c in fake.calls} == {S3}


def test_nomads_refusing_everything_still_serves_the_tables(monkeypatch):
    """The 2026-10-11 outage again: NOMADS answers 403 to everything; AWS has the run's archive."""
    _pin_run(monkeypatch)
    fake = _world(monkeypatch, nomads_all=403)
    monkeypatch.setattr(app, "resolve_model", lambda s, m: "GFS")
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
    monkeypatch.setattr(app._BULL_HTTP, "get", lambda url, headers=None, **k: A.Resp(206, b"x" * 100))
    assert app._bull_range("https://noaa-gfs-bdp-pds.s3.amazonaws.com/x", 0, 99) == b"x" * 100
