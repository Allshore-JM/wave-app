"""Where the GFS-Wave station bulletins come from (hotfix 2026-10-11).

NOMADS refused the production server's address and every GFS forecast said "No recent run found." The run probe
and the .bull are now asked from NOAA's copy on AWS Open Data first and NOMADS second; the per-station .spec (the
Wind column) exists only on NOMADS. All monkeypatched -- no network.
"""
import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures")
with open(os.path.join(FIX, "gfswave_bull_modern.txt")) as _f:
    BULL = _f.read()
S3, NOMADS = "noaa-gfs-bdp-pds.s3.amazonaws.com", "nomads.ncep.noaa.gov"


class _Resp:
    def __init__(self, status, text=""):
        self.status_code = status
        self.text = text
        self.headers = {}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def iter_lines(self, **k):
        return iter(())


class FakeNoaa:
    """Answers by host: `answers[host]` is a status, an exception, or a function (url -> status)."""

    def __init__(self, **answers):
        self.answers = {S3: answers.get("s3", 200), NOMADS: answers.get("nomads", 200)}
        self.calls = []

    def _status(self, url):
        host = url.split("/")[2]
        self.calls.append((host, url))
        a = self.answers[host]
        if isinstance(a, Exception):
            raise a
        return a(url) if callable(a) else a

    def head(self, url, **k):
        return _Resp(self._status(url))

    def get(self, url, **k):
        st = self._status(url)
        return _Resp(st, BULL if st == 200 and url.endswith(".bull") else "")

    def hosts(self, suffix):
        return [h for h, u in self.calls if u.endswith(suffix)]


@pytest.fixture(autouse=True)
def _clear_caches(monkeypatch):
    monkeypatch.setattr(app, "NOMADS_DISABLED", False)
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
        app._WIND_CACHE.clear()
        app._RUN_CACHE.update({"ts": 0.0, "value": (None, None)})
    yield
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()
        app._WIND_CACHE.clear()
        app._RUN_CACHE.update({"ts": 0.0, "value": (None, None)})


def _noaa(monkeypatch, **answers):
    fake = FakeNoaa(**answers)
    monkeypatch.setattr(app.HTTP, "head", fake.head)
    monkeypatch.setattr(app.HTTP, "get", fake.get)
    return fake


def _run_on(run):
    """Only `run` (e.g. '/12/') is published; anything else 404s (S3) / 403s (NOMADS, as it answers today)."""
    return lambda status_missing: (lambda url: 200 if run in url else status_missing)


def test_the_run_probe_asks_the_aws_copy_first(monkeypatch):
    fake = _noaa(monkeypatch)
    assert app._detect_latest_run()[0]
    assert fake.hosts(".bull") == [S3], "found on AWS: NOMADS never asked"


def test_the_run_probe_falls_back_to_nomads(monkeypatch):
    fake = _noaa(monkeypatch, s3=404, nomads=_run_on("/12/")(403))
    date, run = app._detect_latest_run()
    assert run == "12" and fake.calls[-1][0] == NOMADS
    assert set(fake.hosts(".bull")) == {S3, NOMADS}


def test_the_run_probe_with_nomads_refusing_uses_aws(monkeypatch):
    fake = _noaa(monkeypatch, s3=_run_on("/06/")(404), nomads=403)
    assert app._detect_latest_run()[1] == "06"
    assert fake.calls[-1][0] == S3


def test_no_run_anywhere_logs_both_answers(monkeypatch, caplog):
    _noaa(monkeypatch, s3=404, nomads=ConnectionError("refused"))
    with caplog.at_level(logging.WARNING, logger=app.logger.name):
        assert app._detect_latest_run() == (None, None)
    msg = " ".join(r.getMessage() for r in caplog.records)
    assert f"{S3} HTTP 404" in msg and f"{NOMADS} ConnectionError" in msg


def test_the_bulletin_comes_from_aws_when_it_has_it(monkeypatch):
    monkeypatch.setattr(app, "get_latest_run", lambda: ("20261011", "12"))
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: {})
    fake = _noaa(monkeypatch)
    _, _, _, rows, _, err = app._parse_bull_uncached("51201", None)
    assert err is None and rows
    assert fake.hosts(".bull") == [S3]
    assert fake.calls[0][1] == ("https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20261011/12/wave/station/"
                                "bulls.t12z/gfswave.51201.bull")


def test_the_bulletin_falls_back_to_nomads(monkeypatch, caplog):
    monkeypatch.setattr(app, "get_latest_run", lambda: ("20261011", "12"))
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: {})
    fake = _noaa(monkeypatch, s3=404)
    with caplog.at_level(logging.WARNING, logger=app.logger.name):
        _, _, _, rows, _, err = app._parse_bull_uncached("51201", None)
    assert err is None and rows and fake.hosts(".bull") == [S3, NOMADS]
    assert any("HTTP 404 from " + S3 in r.getMessage() for r in caplog.records)


def test_the_aws_copy_failing_to_connect_falls_back_too(monkeypatch):
    monkeypatch.setattr(app, "get_latest_run", lambda: ("20261011", "12"))
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: {})
    _noaa(monkeypatch, s3=ConnectionError("down"))
    assert app._parse_bull_uncached("51201", None)[-1] is None


@pytest.mark.parametrize("s3,nomads,want", [
    (404, 403, "No .bull file found for 51201"),
    (ConnectionError("x"), ConnectionError("y"), "Could not download .bull for 51201"),
    (ConnectionError("x"), 404, "No .bull file found for 51201"),
])
def test_bulletin_errors(monkeypatch, s3, nomads, want):
    monkeypatch.setattr(app, "get_latest_run", lambda: ("20261011", "12"))
    _noaa(monkeypatch, s3=s3, nomads=nomads)
    assert app._parse_bull_uncached("51201", None)[-1] == want


def test_nomads_refusing_everything_still_serves_the_forecast_without_wind(monkeypatch):
    """The 2026-10-11 outage: NOMADS answers 403 to everything (probe, .bull, .spec); AWS has the run."""
    fake = _noaa(monkeypatch, s3=_run_on("/12/")(404), nomads=403)
    monkeypatch.setattr(app, "resolve_model", lambda s, m: "GFS")
    payload = app.compute_forecast_payload("51201", None, "US", "GFS")
    assert payload["error"] is None and payload["table_html"]
    assert payload["wind_complete"] is False, "no .spec: a gappy forecast, kept briefly so the wind comes back"
    assert NOMADS in fake.hosts(".spec") and set(fake.hosts(".bull")) <= {S3, NOMADS}


def test_nomads_disabled_asks_only_aws(monkeypatch):
    monkeypatch.setattr(app, "NOMADS_DISABLED", True)
    fake = _noaa(monkeypatch, s3=_run_on("/12/")(404))
    monkeypatch.setattr(app, "resolve_model", lambda s, m: "GFS")
    payload = app.compute_forecast_payload("51201", None, "US", "GFS")
    assert payload["error"] is None and payload["wind_complete"] is False
    assert {h for h, _ in fake.calls} == {S3}
