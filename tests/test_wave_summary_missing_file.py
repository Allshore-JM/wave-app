"""Bug fix: /api/ndbc/station/<id>/wave-summary answered HTTP 500 when NDBC has no .spec file
for the station (a retired buoy, or one that dropped out of the realtime set). It now answers
404 with a clean JSON error, like the components route. Everything else is byte-identical to
the golden captured from the unchanged code (tests/fixtures/wave_summary_golden.json).
"""
import json
import os
import sys

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import capture_wave_summary_golden as G  # noqa: E402

GOLDEN = json.load(open(os.path.join(HERE, "fixtures", "wave_summary_golden.json"), encoding="utf-8"))


def _run(monkeypatch):
    saved = (A._fetch_text, A.load_station_metadata, A.get_station_tz)
    try:
        return G.run_scenarios(A)
    finally:
        A._fetch_text, A.load_station_metadata, A.get_station_tz = saved


def test_unchanged_scenarios_are_byte_identical(monkeypatch):
    rec = _run(monkeypatch)
    for name in ("ok", "other_error"):
        assert rec[name]["status"] == GOLDEN[name]["status"], name
        assert rec[name]["body"] == GOLDEN[name]["body"], name
        assert rec[name]["content_type"] == GOLDEN[name]["content_type"], name


def test_missing_file_is_404_not_500(monkeypatch):
    rec = _run(monkeypatch)
    old, new = GOLDEN["missing_file"], rec["missing_file"]
    assert old["status"] == 500                       # the bug, as captured
    assert new["status"] == 404
    body = json.loads(new["body"])
    assert body["station"] == "51201"
    assert body["error"].startswith("NDBC wave summary not available")
    assert set(body) == {"station", "error"}         # same shape the frontend already renders
    assert new["content_type"] == old["content_type"]


def test_non_404_http_errors_still_500(monkeypatch):
    def fetch(url, timeout=25):
        resp = requests.Response(); resp.status_code = 503; resp.url = url; resp.reason = "Unavailable"
        resp.raise_for_status()
    monkeypatch.setattr(A, "_fetch_text", fetch)
    r = A.app.test_client().get("/api/ndbc/station/51201/wave-summary")
    assert r.status_code == 500 and "503" in r.get_json()["error"]
