"""Capture golden responses of /api/ndbc/station/<id>/wave-summary from the UNCHANGED code.

Run ONCE on the pre-fix commit:  python tests/capture_wave_summary_golden.py
Scenarios:
  ok            .spec fixture present (tests/fixtures/ndbc_51201/51201.spec), clock pinned
  missing_file  NDBC returns 404 for the .spec (requests.HTTPError from raise_for_status)
  other_error   any other exception while fetching
The fix changes ONLY `missing_file` (500 -> 404 with a clean message); `ok` and `other_error`
must stay byte-identical.
"""
import functools
import json
import os
import sys
from datetime import datetime

import pytz
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

NOW = datetime(2026, 9, 9, 22, 0, tzinfo=pytz.utc)
SPEC = open(os.path.join(HERE, "fixtures", "ndbc_51201", "51201.spec"), encoding="utf-8").read()
META = {"51201": {"name": "Waimea Bay, HI", "lat": 21.671, "lon": -158.117}}


def _http_404(url):
    resp = requests.Response()
    resp.status_code = 404
    resp.url = url
    resp.reason = "Not Found"
    resp.raise_for_status()          # raises requests.HTTPError exactly like _fetch_text does


def make_fetch(mode):
    def fetch(url, timeout=25):
        if mode == "ok":
            return SPEC
        if mode == "missing_file":
            _http_404(url)
        raise RuntimeError("upstream exploded")
    return fetch


def run_scenarios(A):
    real = A._parse_noaa_station_wave_summary
    A._parse_noaa_station_wave_summary = functools.partial(real, now_utc=NOW)
    A.load_station_metadata = lambda: META
    A.get_station_tz = lambda sid: "Pacific/Honolulu"
    client = A.app.test_client()
    out = {}
    try:
        for mode in ("ok", "missing_file", "other_error"):
            A._fetch_text = make_fetch(mode)
            r = client.get("/api/ndbc/station/51201/wave-summary")
            out[mode] = {"status": r.status_code, "body": r.get_data(as_text=True),
                         "content_type": r.headers.get("Content-Type")}
    finally:
        A._parse_noaa_station_wave_summary = real
    return out


if __name__ == "__main__":
    import app
    rec = run_scenarios(app)
    path = os.path.join(HERE, "fixtures", "wave_summary_golden.json")
    json.dump(rec, open(path, "w", encoding="utf-8"), indent=1, sort_keys=True)
    for k, v in rec.items():
        print(k, v["status"], v["body"][:110])
    print("wrote", path)
