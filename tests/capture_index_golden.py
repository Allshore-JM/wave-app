"""Capture the rendered home page with fixed inputs (no network).

Run ONCE on a commit that changes the flag-off page ON PURPOSE (a UI restructure), never after an
overlay-only commit:  python tests/capture_index_golden.py
Writes tests/fixtures/index_golden.json: raw HTML for the default shell, the render=full page (the
no-JS path), the Graph view, a POST and a SWAN station, keyed by scenario. tests/test_overlay_flag.py
replays the same scenarios with MODEL_OVERLAYS unset and asserts byte-equality, so the flag-off page
is proven untouched by every later overlay commit.
Baseline: feat/forecast-window (the forecast window, plan section 25), 2026-09-26.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

PAYLOAD = {
    "station": "51201", "error": None,
    "table_html": "<table id='golden'><tr><td>fixed</td></tr></table>",
    "tz_label": "HST", "lat": 21.67, "lon": -158.12,
    "graph_data": {"labels": ["Wednesday, September 23, 2026 2:00 AM"], "height": {"s1": [1.0], "combined": [1.2]},
                   "period": {"s1": [12.0]}, "direction": {"s1": [300]}, "units": "ft"},
    "graph_header": {"cycle": "20260922 12 UTC", "location": "51201 (21.67N 158.12W)", "tz": "HST"},
    "model": "GFS", "swan_available": True,
}
STATIONS = [("51201", "Waimea Bay, HI"), ("51001", "NW Hawaii"), ("46001", "Gulf of Alaska")]
SCENARIOS = [
    ("default", "GET", "/?station=51201", None, False),
    ("render_full", "GET", "/?station=51201&unit=Metric&tz=Pacific/Honolulu&render=full", None, True),
    ("graph", "GET", "/?station=51201&view=Graph", None, True),
    ("post", "POST", "/", {"station": "46001", "unit": "US", "tz": "", "model": "GFS", "view": "Table"}, True),
    ("swan_station_model", "GET", "/?station=51201&model=SWAN", None, True),
]


def run_scenarios(app_module):
    A = app_module
    saved = {k: getattr(A, k) for k in ("compute_forecast_payload", "_forecast_is_cached", "get_station_list")}
    A.get_station_list = lambda: list(STATIONS)
    A.compute_forecast_payload = lambda *a, **k: dict(PAYLOAD)
    client = A.app.test_client()
    out = {}
    try:
        for name, method, path, data, cached in SCENARIOS:
            A._forecast_is_cached = (lambda *a, **k: True) if cached else (lambda *a, **k: False)
            r = client.open(path, method=method, data=data)
            out[name] = {"status": r.status_code, "body": r.get_data(as_text=True),
                         "headers": {k: r.headers.get(k) for k in ("Content-Type", "Cache-Control")}}
    finally:
        for k, v in saved.items():
            setattr(A, k, v)
    return out


if __name__ == "__main__":
    os.environ.pop("MODEL_OVERLAYS", None)
    import app
    rec = run_scenarios(app)
    path = os.path.join(HERE, "fixtures", "index_golden.json")
    json.dump(rec, open(path, "w", encoding="utf-8"), indent=1, sort_keys=True)
    for k, v in rec.items():
        print(k, v["status"], len(v["body"]), "bytes")
    print("wrote", path)
