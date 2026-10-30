"""Capture the Pacific-island SWAN table fixtures (plan section 64) from PacIOOS (network, two requests):

    python tests/capture_swan_pacific_fixtures.py [--from DIR]

tests/fixtures/swan_buoy_52202.table   Ritidian Point, Guam (buoy.cdip196.table), cut to its first ROWS data rows
tests/fixtures/swan_buoy_52212.table   Ngaraard, Palau (buoy.cdip219.table, the 14-day run), cut the same way

The header lines ("%": the run / table id, the column names, the units) are kept whole; the first ROWS data rows
follow (the parser drops the 6-hour spin-up, so a 30-row cut shows 24 rows). --from DIR reads already downloaded
copies named <cdip id>.table instead of asking PacIOOS.
"""
import os
import sys

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = "https://www.pacioos.hawaii.edu/ssi/wavebuoy/swan_bull/tables"
TABLES = {"52202": "cdip196", "52212": "cdip219"}
ROWS = 30


def cut(text, rows):
    out, n = [], 0
    for line in text.splitlines():
        s = line.strip()
        if s and not s.startswith("%"):
            n += 1
            if n > rows:
                break
        out.append(line.rstrip())
    return "\n".join(out) + "\n"


def main(argv):
    src = argv[argv.index("--from") + 1] if "--from" in argv else None
    for sid, cdip in TABLES.items():
        if src:
            text = open(os.path.join(src, f"{cdip}.table"), encoding="ascii").read()
        else:
            r = requests.get(f"{BASE}/buoy.{cdip}.table", timeout=30,
                             headers={"User-Agent": "allshoresurf.com (test fixtures)"})
            r.raise_for_status()
            text = r.text
        assert f"Table:{sid}" in text, (sid, cdip)
        path = os.path.join(HERE, "fixtures", f"swan_buoy_{sid}.table")
        with open(path, "w", encoding="ascii", newline="\n") as f:
            f.write(cut(text, ROWS))
        print(path, len(cut(text, ROWS).splitlines()), "lines")


if __name__ == "__main__":
    main(sys.argv[1:])
