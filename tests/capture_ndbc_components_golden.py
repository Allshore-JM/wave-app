"""Capture golden responses of /api/ndbc/station/<id>/components from the UNCHANGED code.

Run ONCE on the pre-change commit:  python tests/capture_ndbc_components_golden.py
Writes tests/fixtures/ndbc_components_golden.json. Release B (parallel optional-file
downloads) must reproduce every scenario's status + raw body byte for byte.

Fixtures: real NDBC realtime2 files for 51201 captured 2026-09-09 (tests/fixtures/ndbc_51201).
Clock is pinned via _ndbc_row_is_recent(now_utc=NOW) so the freshness gate is deterministic.
Scenarios:
  full            all six files present
  no_required     .data_spec fetch raises              -> 404 error JSON
  no_swdir        .swdir raises (falls back to swdir2)
  no_optional     all five optional files raise
  stale           clock moved 30 days ahead            -> no_recent_reports
  cached          second call of `full` (served from NDBC_COMPONENT_CACHE)
Each scenario records which URLs were requested, in order, so the fetch SET stays identical
(order is allowed to differ once downloads run concurrently; tests compare sorted sets).
"""
import functools
import json
import os
import sys
from datetime import datetime, timedelta

import pytz

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

FIX = os.path.join(HERE, "fixtures", "ndbc_51201")
NOW = datetime(2026, 9, 9, 22, 0, tzinfo=pytz.utc)
META = {"51201": {"id": "51201", "name": "Waimea Bay, HI", "lat": 21.671, "lon": -158.117}}


def _file(suffix):
    with open(os.path.join(FIX, "51201." + suffix), encoding="utf-8") as fh:
        return fh.read()


def make_fetch(fail_suffixes=(), calls=None, delay=0.0):
    def fetch(url, timeout=25):
        if calls is not None:
            calls.append(url.rsplit("/", 1)[-1])
        if delay:
            import time
            time.sleep(delay)
        suffix = url.rsplit(".", 1)[-1]
        if suffix in fail_suffixes:
            raise RuntimeError("HTTP 404 for " + url)
        return _file(suffix)
    return fetch


def run_scenarios(app_module):
    A = app_module
    real_recent = A._ndbc_row_is_recent
    A.get_live_ndbc_wave_stations = lambda: list(META.values())
    A.get_station_tz = lambda sid: "Pacific/Honolulu"
    client = A.app.test_client()
    out = {}

    def run(name, fail=(), now=NOW, repeat=1):
        A.NDBC_COMPONENT_CACHE.clear()
        A._ndbc_row_is_recent = functools.partial(real_recent, now_utc=now)
        calls = []
        A._fetch_text = make_fetch(fail, calls)
        for _ in range(repeat):
            r = client.get("/api/ndbc/station/51201/components")
        out[name] = {"status": r.status_code, "body": r.get_data(as_text=True),
                     "content_type": r.headers.get("Content-Type"),
                     "cache_control": r.headers.get("Cache-Control"), "fetched": calls}
    run("full")
    run("no_required", fail=("data_spec",))
    run("no_swdir", fail=("swdir",))
    run("no_optional", fail=("swdir", "swdir2", "swr1", "swr2", "spec"))
    run("stale", now=NOW + timedelta(days=30))
    run("cached", repeat=2)
    A._ndbc_row_is_recent = real_recent
    return out


if __name__ == "__main__":
    import app
    rec = run_scenarios(app)
    path = os.path.join(HERE, "fixtures", "ndbc_components_golden.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(rec, fh, indent=1, sort_keys=True)
    for k, v in rec.items():
        print(k, v["status"], len(v["body"]), v["fetched"])
    print("wrote", path)
