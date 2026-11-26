"""AusWaves refuses servers (2026-11-26: 403 {"code":"waf_forbidden"}). The provider must not get around
it (no browser User-Agent / Referer) and must fail QUIETLY: a refusal raises AccessDenied, the base class keeps
the last good list for keep_on_failure_sec and then shows none, asks again only once per list TTL (never every
retry_after_sec), never keeps a list past its expiry, and logs the refusal once (repeats at DEBUG).
Refreshes run inline here (tests/conftest.py installs the inline runner)."""
import json
import logging
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import buoy_sources as B  # noqa: E402
import fake_buoy_providers as F  # noqa: E402

WAF = json.dumps({"code": "waf_forbidden", "message": "Access denied", "data": {"status": 403}})


class _Resp:
    def __init__(self, status=200, body="[]"):
        self.status_code = status
        self.text = body

    def json(self):
        return json.loads(self.text)


class _HTTP:
    """Answers `resp`; records every call's url + headers."""
    def __init__(self, resp):
        self.resp = resp
        self.calls = []

    def get(self, url, **kw):
        self.calls.append((url, kw.get("headers") or {}))
        return self.resp


def _catalogue(now):
    return json.dumps([{"id": 7, "is_enabled": "1", "drifting": "0", "last_update": str(int(now) - 600),
                        "lat": "-31.6", "lng": "115.6", "web_display_name": "Yanchep Lagoon"}])


@pytest.fixture
def clock(monkeypatch):
    frozen = F.FrozenTime()
    monkeypatch.setattr(B, "time", frozen)
    return frozen


def _warnings(caplog):
    return [r for r in caplog.records if r.levelno >= logging.WARNING and "AusWaves" in r.getMessage()]


# ------------------------------ the provider ------------------------------

def test_headers_name_the_site_and_copy_no_browser():
    http = _HTTP(_Resp(200, "[]"))
    B.AusWavesProvider(http=http)._fetch_stations()
    B.AusWavesProvider(http=http).detail("7")
    assert len(http.calls) == 2
    for _url, headers in http.calls:
        ua = headers.get("User-Agent", "")
        assert "allshoresurf.com" in ua and "Mozilla" not in ua
        assert "Referer" not in headers


@pytest.mark.parametrize("status,body,text", [
    (403, WAF, "HTTP 403 from the AusWaves catalogue (waf_forbidden)"),
    (401, json.dumps({"code": "rest_forbidden"}), "HTTP 401 from the AusWaves catalogue (rest_forbidden)"),
    (403, "<html>denied</html>", "HTTP 403 from the AusWaves catalogue"),
])
def test_refusal_raises_access_denied(status, body, text):
    with pytest.raises(B.AccessDenied) as ei:
        B.AusWavesProvider(http=_HTTP(_Resp(status, body)))._fetch_stations()
    assert str(ei.value) == text


def test_other_errors_are_not_a_refusal():
    with pytest.raises(RuntimeError) as ei:
        B.AusWavesProvider(http=_HTTP(_Resp(503, "[]")))._fetch_stations()
    assert not isinstance(ei.value, B.AccessDenied)


def test_refused_buoy_window_answers_no_data():
    assert B.AusWavesProvider(http=_HTTP(_Resp(403, WAF))).detail("7") == {"latest": None, "recent": []}


# ------------------------------ through the base class ------------------------------

def test_denied_from_the_start_is_quiet_and_asked_once_per_ttl(clock, caplog):
    caplog.set_level(logging.DEBUG, logger=B._log.name)
    http = _HTTP(_Resp(403, WAF))
    p = B.AusWavesProvider(http=http)
    lst, v, _ = p.list_stations_versioned()
    assert lst == [] and len(http.calls) == 1
    st = p.status()
    assert st["stations"] == 0
    assert st["last_error"] == "AccessDenied: HTTP 403 from the AusWaves catalogue (waf_forbidden)"
    assert st["due_in_s"] == pytest.approx(p.list_ttl_sec, abs=1)      # 30 min, not retry_after_sec
    assert len(_warnings(caplog)) == 2                                   # said once (fetch + refresh lines)
    clock.now += p.retry_after_sec + 1
    p.list_stations_versioned()
    assert len(http.calls) == 1                                          # not asked again after 5 min
    for _ in range(4):
        clock.now += p.list_ttl_sec + 1
        assert p.list_stations_versioned()[:2] == ([], v)                # nothing new published
    assert len(http.calls) == 5
    assert len(_warnings(caplog)) == 2                                   # the repeats went to DEBUG


def test_denied_keeps_the_last_list_then_drops_it_on_time(clock, caplog):
    caplog.set_level(logging.DEBUG, logger=B._log.name)
    http = _HTTP(_Resp(200, _catalogue(clock.now)))
    p = B.AusWavesProvider(http=http)
    lst1, v1, _ = p.list_stations_versioned()
    assert [s["name"] for s in lst1] == ["Yanchep Lagoon"]
    t_ok = clock.now
    http.resp = _Resp(403, WAF)
    caplog.clear()
    clock.now += p.list_ttl_sec + 1
    while clock.now - t_ok < p.keep_on_failure_sec:
        lst, v, _ = p.list_stations_versioned()
        assert (lst, v) == (lst1, v1)                                    # kept, same version
        st = p.status()
        assert st["due_in_s"] <= p.list_ttl_sec + 1
        clock.now += st["due_in_s"] + 1                                  # to the next due refresh
    # the last retry was capped at the expiry: never more than a second past keep_on_failure_sec
    assert clock.now - t_ok <= p.keep_on_failure_sec + 3
    lst, v, _ = p.list_stations_versioned()
    assert lst == [] and v == v1 + 1                                     # dropped: no markers
    assert len(_warnings(caplog)) == 2                                   # one refusal, said once
    # allowed again: the next due refresh brings the markers back by itself
    http.resp = _Resp(200, _catalogue(clock.now))
    clock.now += p.list_ttl_sec + 1
    lst, _, _ = p.list_stations_versioned()
    assert [s["name"] for s in lst] == ["Yanchep Lagoon"] and p.status()["last_error"] is None


def test_a_hiccup_still_retries_soon_and_warns(clock, caplog):
    caplog.set_level(logging.DEBUG, logger=B._log.name)
    http = _HTTP(_Resp(503, "[]"))
    p = B.AusWavesProvider(http=http)
    p.list_stations_versioned()
    assert p.status()["due_in_s"] == pytest.approx(p.retry_after_sec, abs=1)
    clock.now += p.retry_after_sec + 1
    p.list_stations_versioned()
    assert len(http.calls) == 2 and len(_warnings(caplog)) == 4          # unchanged: each failure warns
