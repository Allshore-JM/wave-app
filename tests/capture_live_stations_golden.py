"""Capture golden responses of /api/buoys/live-stations from the UNCHANGED implementation.

Run ONCE on the pre-change commit:  python tests/capture_live_stations_golden.py
Writes tests/fixtures/live_stations_golden.json. The memo tests replay the same sequence
against the new code and compare raw bytes + ETag step by step (never against the new builder).

Sequence (frozen clock, fake providers, deterministic tz):
  steps 1-3   three identical GETs (idempotence across repeated requests)
  step  4     conditional GET with the step-3 ETag (304 path)
  steps 5-7   AODN forced to refresh with IDENTICAL data, then three GETs
  steps 8-10  AODN refreshed with CHANGED data (new station), then three GETs
  step  11    GET /api/buoys/cdip:106/latest (provider attribution/capabilities path)
Also records the provider caches (deep) after the sequence so the new code can prove it leaves
them byte-identical.
"""
import json
import os
import sys
import copy

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import fake_buoy_providers as F  # noqa: E402


def run_sequence(app_module, buoy_sources_module):
    """Drive the sequence against whatever implementation is imported. Returns the record."""
    frozen = F.FrozenTime()
    buoy_sources_module.time = frozen
    provs = F.make_providers()
    app_module.get_buoy_providers = lambda: provs
    app_module._buoy_tz_cached = F.fake_tz
    client = app_module.app.test_client()
    steps = []

    def get(step, headers=None):
        r = client.get("/api/buoys/live-stations", headers=headers or {})
        steps.append({"step": step, "status": r.status_code, "etag": r.headers.get("ETag"),
                      "cache_control": r.headers.get("Cache-Control"),
                      "content_type": r.headers.get("Content-Type"),
                      "body": r.get_data(as_text=True),
                      "fetch_calls": {c.source: c.fetch_calls for c in F.FAKE_CLASSES}})
        return r

    for s in (1, 2, 3):
        get(s)
    et = steps[-1]["etag"]
    get(4, {"If-None-Match": et})
    aodn = next(p for p in provs if p.source == "AODN")
    aodn._list_ts = 0.0                       # force refresh, same data
    for s in (5, 6, 7):
        get(s)
    FakeAODN = F.FakeAODN
    FakeAODN.rows = FakeAODN.rows + [{"local_id": "HOB", "name": "Hobart", "lat": -42.9, "lon": 147.4,
                                      "latest_time": F.FRESH}]
    aodn._list_ts = 0.0                       # force refresh, changed data
    for s in (8, 9, 10):
        get(s)
    FakeAODN.rows = FakeAODN.rows[:-1]        # restore class fixture
    r = client.get("/api/buoys/cdip:106/latest")
    steps.append({"step": 11, "status": r.status_code, "etag": r.headers.get("ETag"),
                  "cache_control": r.headers.get("Cache-Control"),
                  "content_type": r.headers.get("Content-Type"),
                  "body": r.get_data(as_text=True)})
    caches = {p.source: copy.deepcopy(p._list_cache) for p in provs}
    return {"steps": steps, "provider_caches_after": caches}


if __name__ == "__main__":
    import app as A
    import buoy_sources as B
    rec = run_sequence(A, B)
    out = os.path.join(HERE, "fixtures", "live_stations_golden.json")
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(rec, fh, indent=1, sort_keys=True)
    for s in rec["steps"]:
        print(s["step"], s["status"], s["etag"], len(s["body"]), s.get("fetch_calls"))
    print("wrote", out)
