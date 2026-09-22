"""Model overlays: the page is byte-identical with the flag off; assets and markup only with it on.

Golden: tests/fixtures/index_golden.json captured by tests/capture_index_golden.py on the code
BEFORE any overlay change (fixed forecast payload + station list, no network).
"""
import json
import os
import re
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402
import capture_index_golden as G  # noqa: E402

GOLDEN = json.load(open(os.path.join(HERE, "fixtures", "index_golden.json"), encoding="utf-8"))


def test_flag_off_page_is_byte_identical(monkeypatch):
    monkeypatch.delenv("MODEL_OVERLAYS", raising=False)
    rec = G.run_scenarios(A)
    for name, old in GOLDEN.items():
        assert rec[name]["status"] == old["status"], name
        assert rec[name]["body"] == old["body"], "flag-off HTML changed in scenario %s" % name
        assert rec[name]["headers"] == old["headers"], name
        assert "/overlay/" not in rec[name]["body"] and "modelPane" not in rec[name]["body"]


def test_flag_on_adds_only_the_gated_block(monkeypatch):
    monkeypatch.setenv("MODEL_OVERLAYS", "1")
    monkeypatch.setenv("MODEL_FRAMES_BASE", "https://frames.example/gfswave/0p25/v1/")
    monkeypatch.setattr(A, "get_station_tz", lambda sid: "Pacific/Honolulu")
    rec = G.run_scenarios(A)
    for name, old in GOLDEN.items():
        body = rec[name]["body"]
        assert 'id="ovField"' in body and "modelPane" in body and "/overlay/overlay.js?v=" in body
        assert '"https://frames.example/gfswave/0p25/v1"' in body           # trailing slash stripped
        # removing the gated block gives back exactly the golden page
        stripped = re.sub(r"  <script>\n    // Optional model overlays.*?</script>\n\n", "", body, flags=re.S)
        assert stripped == old["body"], name
    # tz rule: explicit ?tz wins, else the station zone
    assert '"Pacific/Honolulu"' in rec["table_inline"]["body"]
    assert '"Pacific/Honolulu"' in rec["table_deferred"]["body"]          # from get_station_tz


def test_asset_route_headers_and_containment(monkeypatch):
    c = A.app.test_client()
    for name, ct in (("overlay.js", "application/javascript"), ("overlay.css", "text/css")):
        r = c.get("/overlay/" + name + "?v=1")
        assert r.status_code == 200 and r.headers["Content-Type"].startswith(ct)
        assert r.headers["Cache-Control"] == "public, max-age=31536000, immutable"
        assert r.headers["CDN-Cache-Control"] == "max-age=31536000"
    for bad in ("../app.py", "app.py", "overlay.js/../../app.py", "nope.js"):
        assert c.get("/overlay/" + bad).status_code == 404


def test_overlay_js_syntax_and_contract_strings():
    js = open(os.path.join(os.path.dirname(HERE), "static_overlay", "overlay.js"), encoding="utf-8").read()
    for needle in ("'modelPane'", "pointerEvents", "createImageBitmap", "valueAt", "unmount", "AllshoreOverlay"):
        assert needle in js
    if subprocess.run(["node", "--version"], capture_output=True).returncode == 0:
        r = subprocess.run(["node", "--check", os.path.join(os.path.dirname(HERE), "static_overlay", "overlay.js")], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr


def test_rendered_inline_scripts_parse_with_flag_on(monkeypatch):
    if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
        pytest.skip("node not available")
    monkeypatch.setenv("MODEL_OVERLAYS", "1")
    monkeypatch.setenv("MODEL_FRAMES_BASE", "https://frames.example/x")
    monkeypatch.setattr(A, "get_station_tz", lambda sid: "Pacific/Honolulu")
    rec = G.run_scenarios(A)
    for name in ("table_inline", "graph"):
        blocks = re.findall(r"<script(?![^>]*src=)[^>]*>(.*?)</script>", rec[name]["body"], flags=re.S)
        assert len(blocks) >= 4
        for i, b in enumerate(blocks):
            path = os.path.join(HERE, f"_blk_{name}_{i}.js")
            open(path, "w", encoding="utf-8").write(b)
            try:
                r = subprocess.run(["node", "--check", path], capture_output=True, text=True)
                assert r.returncode == 0, (name, i, r.stderr[:300])
            finally:
                os.remove(path)
