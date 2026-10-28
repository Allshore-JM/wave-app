"""Capture one published NWPS station file from the frames bucket (plan section 59, step 4) for tests/test_parse_nwps.py:
tests/fixtures/nwps/site_51003.json (hfo's run as the job published it). python -I tests/capture_nwps_site_fixture.py [run]"""
import json
import os
import sys
import urllib.request

BASE = "https://models.allshoresurf.com/nwps/v1"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "nwps", "site_51003.json")


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "allshoresurf.com test fixture"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def main(run=None):
    if not run:
        run = json.loads(get(f"{BASE}/latest.json"))["offices"]["hfo"]["run"]
    body = get(f"{BASE}/hfo/{run}/51003.json")
    json.loads(body)
    with open(OUT, "wb") as f:
        f.write(body)
    print(OUT, run, len(body))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
