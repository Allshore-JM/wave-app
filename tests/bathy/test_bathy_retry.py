"""Retries of passing faults in the bathymetry builder (2026-11-16: NOAA answered one HTTP 500 to a VRT read and R2
one "Reduce your concurrent request rate" to an upload; each ended a region's build)."""
import io
import os
import sys
import urllib.error
import urllib.request

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "tools", "bathy"))
import build_bathy as B  # noqa: E402


@pytest.fixture
def no_sleep(monkeypatch):
    waits = []
    monkeypatch.setattr(B, "_sleep", waits.append)
    return waits


def _http(code):
    return urllib.error.HTTPError("https://x.invalid/f", code, "msg", {}, io.BytesIO(b""))


class Busy(Exception):
    def __init__(self, code):
        super().__init__("An error occurred (%s) when calling the PutObject operation" % code)
        self.response = {"Error": {"Code": code}}


def test_http_faults_worth_another_try():
    assert B.http_transient(_http(500)) and B.http_transient(_http(503)) and B.http_transient(_http(429))
    assert not B.http_transient(_http(404)) and not B.http_transient(_http(403))
    assert B.http_transient(urllib.error.URLError("reset")) and B.http_transient(TimeoutError())
    assert B.http_transient(ConnectionResetError()) and not B.http_transient(ValueError("bad vrt"))


def test_storage_faults_worth_another_try():
    for code in ("ServiceUnavailable", "SlowDown", "TooManyRequests", "InternalError"):
        assert B.storage_busy(Busy(code))
    assert not B.storage_busy(Busy("AccessDenied")) and not B.storage_busy(Busy("NoSuchBucket"))
    assert not B.storage_busy(ValueError("x")) and B.storage_busy(ConnectionResetError())


def test_retrying_waits_then_succeeds(no_sleep):
    calls = []

    def fn():
        calls.append(1)
        if len(calls) < 3:
            raise _http(500)
        return "ok"
    assert B.retrying(fn, B.http_transient, "t", log_fn=lambda s: None) == "ok"
    assert len(calls) == 3 and no_sleep == list(B.RETRY_WAITS[:2])


def test_retrying_gives_up_after_the_last_wait(no_sleep):
    calls = []

    def fn():
        calls.append(1)
        raise _http(502)
    with pytest.raises(urllib.error.HTTPError):
        B.retrying(fn, B.http_transient, "t", log_fn=lambda s: None)
    assert len(calls) == len(B.RETRY_WAITS) + 1 and no_sleep == list(B.RETRY_WAITS)


def test_retrying_raises_a_lasting_fault_at_once(no_sleep):
    calls = []

    def fn():
        calls.append(1)
        raise _http(404)
    with pytest.raises(urllib.error.HTTPError):
        B.retrying(fn, B.http_transient, "t", log_fn=lambda s: None)
    assert len(calls) == 1 and no_sleep == []


def test_download_asks_again_after_a_server_error(monkeypatch, no_sleep):
    answers = [_http(500), _http(503), b"<VRTDataset/>"]

    class Answer:
        def __init__(self, body):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return self.body

    def fake(req, context=None, timeout=None):
        a = answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return Answer(a)
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    assert B._https_text("https://example.invalid/x.vrt") == "<VRTDataset/>"
    assert answers == [] and len(no_sleep) == 2


def test_download_does_not_ask_again_for_a_missing_file(monkeypatch, no_sleep):
    calls = []

    def fake(req, context=None, timeout=None):
        calls.append(req)
        raise _http(404)
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    with pytest.raises(urllib.error.HTTPError):
        B._https_bytes("https://example.invalid/missing.vrt")
    assert len(calls) == 1 and no_sleep == []
