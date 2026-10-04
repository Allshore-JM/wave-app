"""The Marine Institute (MI-IE) per-station fallback (plan section 36; owner, 2026-10-04: fix before production).

`IrishMarineProvider.detail()` for a buoy that is not in the list in hand asked the ERDDAP server directly, on the
visitor's request thread, with the shared session (40 s read timeout, two automatic retries: ~2 minutes when the
server is down, as it was all day on 2026-10-04). Since the page draws the buoys it remembers, a remembered Irish
buoy could be clicked on such a day. Now: no direct request while the list's last refresh failed (the server is
known to be down); otherwise one attempt with short timeouts.
"""
import os
import sys
import time

import pytest
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import buoy_sources as B  # noqa: E402

HDR = ("station_id,time,latitude,longitude,WaveHeight,WavePeriod,Tp,MeanWaveDirection,Hmax,SeaTemperature,SprTp\n"
       ",UTC,degrees_north,degrees_east,m,s,s,degrees_true,m,degree_C,degrees\n")
LIST_CSV = HDR + "M6,2026-10-04T10:00:00Z,53.07,-15.88,3.1,7.0,11.0,250,5.0,14.0,30\n"
M2_CSV = HDR + "M2,2026-10-04T10:00:00Z,53.48,-5.43,1.2,5.0,8.0,120,2.0,13.0,25\n"


class Resp:
    def __init__(self, text, status=200):
        self.text, self.status_code = text, status


class FakeHTTP:
    """Answers the list query and the per-station query; records every call and its timeout."""
    def __init__(self, list_ok=True, station_raises=False, station_csv=M2_CSV):
        self.calls = []
        self.list_ok, self.station_raises, self.station_csv = list_ok, station_raises, station_csv

    def get(self, url, timeout=None, **kw):
        self.calls.append((url, timeout))
        if "station_id=%22" in url:
            if self.station_raises:
                raise requests.ReadTimeout("read timed out")
            return Resp(self.station_csv)
        if not self.list_ok:
            raise requests.ConnectionError("Max retries exceeded (read timeout=40)")
        return Resp(LIST_CSV)


def test_a_known_down_server_is_not_asked_again_on_a_request_thread():
    http = FakeHTTP(list_ok=False)
    p = B.IrishMarineProvider(http=http)
    p.refresh()                                                   # the list fails (published [], error kept)
    assert p.status()["last_error"] and p.snapshot()[0] == []
    calls = len(http.calls)
    t0 = time.perf_counter()
    d = p.detail("M2")                                            # a remembered buoy clicked on a bad day
    assert time.perf_counter() - t0 < 0.1
    assert len(http.calls) == calls                               # no request to the server that is down
    assert d == {"latest": None, "recent": []}


def test_otherwise_one_short_attempt_for_a_buoy_outside_the_list():
    http = FakeHTTP()
    p = B.IrishMarineProvider(http=http)
    p.refresh()
    assert p.status()["last_error"] is None and [s["id"] for s in p.snapshot()[0]] == ["mi-ie:M6"]
    d = p.detail("M2")
    station_calls = [c for c in http.calls if "station_id=%22" in c[0]]
    assert len(station_calls) == 1 and station_calls[0][1] == B.IrishMarineProvider.FALLBACK_TIMEOUT
    assert d["latest"]["hs_m"] == 1.2 and d["recent"]
    assert B.IrishMarineProvider.FALLBACK_TIMEOUT[0] + B.IrishMarineProvider.FALLBACK_TIMEOUT[1] < 15


def test_a_failing_direct_attempt_answers_instead_of_raising():
    http = FakeHTTP(station_raises=True)
    p = B.IrishMarineProvider(http=http)
    p.refresh()
    assert p.detail("M2") == {"latest": None, "recent": []}
    assert p.detail("M6")["latest"]["hs_m"] == 3.1                 # a listed buoy: from the list, no request


def test_the_direct_attempt_never_goes_through_the_retrying_session(monkeypatch):
    """The app's shared session retries twice (Retry total=2): the fallback uses a plain requests.get instead."""
    seen = []
    monkeypatch.setattr(B.requests, "get", lambda url, timeout=None, **kw: seen.append(timeout) or Resp(M2_CSV))

    class Session(requests.Session):
        def get(self, *a, **kw):
            raise AssertionError("the retrying session was used")
    p = B.IrishMarineProvider(http=Session())
    p._list_cache, p._list_version, p._last_error = [], 1, None   # a list in hand, M2 not in it ...
    p._list_ts = p._due_ts = time.time() + 1000                    # ... and no refresh due
    d = p.detail("M2")
    assert seen == [B.IrishMarineProvider.FALLBACK_TIMEOUT] and d["latest"]["hs_m"] == 1.2
