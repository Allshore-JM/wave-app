"""The geo Worker (tools/geo, plan section 46): the visitor's approximate position from the IP address, for the site's
first view. Config pins + its Node tests (no network). It lives beside the frame job's tests because the same CI job
runs them (.github/workflows/model-frames-tests.yml has node, and watches tools/geo/**)."""
import os
import re
import shutil
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GEO = os.path.join(ROOT, "tools", "geo")


def test_geo_worker_config_and_unit_tests():
    cfg = open(os.path.join(GEO, "wrangler.jsonc"), encoding="utf-8").read()
    assert '"name": "allshore-geo"' in cfg and '"workers_dev": false' in cfg
    assert re.search(r'\{ "pattern": "geo\.allshoresurf\.com", "custom_domain": true \}', cfg)
    assert "crons" not in cfg and "GITHUB_TOKEN" not in cfg
    js = open(os.path.join(GEO, "worker.js"), encoding="utf-8").read()
    assert '"Cache-Control": "no-store"' in js and "request.cf" in js
    for origin in ("https://allshoresurf.com", "https://www.allshoresurf.com", "https://wave-app-clean.onrender.com"):
        assert f'"{origin}"' in js
    assert "*" not in re.findall(r'"Access-Control-Allow-Origin"\] = ([^;]+);', js)[0], "never a wildcard"
    for word in ("localStorage", "KV", "env.DB", "console.log"):
        assert word not in js, f"the Worker stores and logs nothing ({word})"
    pkg = open(os.path.join(GEO, "package.json"), encoding="utf-8").read()
    assert '"build": "node --check worker.js && node --test worker.test.js"' in pkg
    tests = open(os.path.join(ROOT, ".github", "workflows", "model-frames-tests.yml"), encoding="utf-8").read()
    assert tests.count('"tools/geo/**"') == 2
    if not shutil.which("node"):
        if os.environ.get("CI"):                                        # G32a A-7: never a silent skip on the CI runner
            pytest.fail("node is missing on the CI runner")
        pytest.skip("node not available")
    r = subprocess.run(["node", "--test", "worker.test.js"], cwd=GEO, capture_output=True, text=True)
    assert r.returncode == 0, (r.stdout[-2000:], r.stderr[-2000:])
