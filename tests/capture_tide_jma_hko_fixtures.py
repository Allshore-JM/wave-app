"""Capture JMA and HKO tide answers for tests/test_tide_jma.py and tests/test_tide_hko.py (plan section 112, step 3).
Run once (python -I tests/capture_tide_jma_hko_fixtures.py); the files are committed. Everything is trimmed to what
the tests read, the real lines and rows unchanged:

  jma/TK_2026.txt      Tokyo's 2026 year file, 2026-12-15 .. 12-31 (JST)   } the New-Year two-file case
  jma/TK_2027.txt      Tokyo's 2027 year file, 2027-01-01 .. 01-15          }
  jma/station2026.html station2026.php: the table head and the rows of WN (Wakkanai), TK (Tokyo), CC (Chichijima),
                       MC (Minamitorishima), NH (Naha)
  hko/HLT_QUB_2026.json / HHOT_QUB_2026.json   Quarry Bay, year 2026, rows 12-15 .. 12-31 (HKT)
  hko/HLT_QUB_2027.json / HHOT_QUB_2027.json   year 2027, rows 01-01 .. 01-15
  hko/predtide_uc.js   HKO's prediction-page script: only its STN table (code, [lon, lat], names)
  hko/probe_QUB_20260101.json  one day of highs and lows (build_station_list's probe)
  hko/unknown.html     the answer for an unknown station (HTML, HTTP 200)
"""
import json
import os
import re
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tide_providers import hko, jma  # noqa: E402

OUT = os.path.join(ROOT, "tests", "fixtures", "tides_national")
UA = "allshoresurf.com tide stations (https://allshoresurf.com)"
JMA_ROWS = ("WN", "TK", "CC", "MC", "NH")


def grab(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        if r.status != 200:
            raise SystemExit("HTTP %s from %s" % (r.status, url))
        return r.read()


def write(rel, body):
    path = os.path.join(OUT, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(body)
    print(rel, len(body))


def jma_year(year, keep):
    body = grab(jma.TXT_URL.format(year=year, code="TK"))
    lines = [ln for ln in body.decode("ascii").split("\n") if ln.strip()]
    kept = [ln for ln in lines if keep(int(ln[74:76]), int(ln[76:78]))]
    write("jma/TK_%d.txt" % year, ("\n".join(kept) + "\n").encode("ascii"))


def jma_list():
    html = grab(jma.LIST_URL.format(year=2026)).decode("utf-8")
    a = html.index('<table class="data2"')
    z = html.index("</table>", a)
    table = html[a:z]
    first = table.index('<tr  class="mtx"><td>1</td>')
    rows = re.findall(r'<tr  class="mtx"><td>\d+</td>.*?</tr>', table[first:], re.S)
    keep = [r for r in rows if re.search(r"<td>(%s)</td><td><a" % "|".join(JMA_ROWS), r)]
    if len(keep) != len(JMA_ROWS):
        raise SystemExit("JMA list: rows %d of %d" % (len(keep), len(JMA_ROWS)))
    doc = ('<!DOCTYPE html>\n<html lang="ja"><head><meta charset="UTF-8"></head><body>\n'
           + table[:first] + "\n".join(keep) + "\n</table>\n</body></html>\n")
    write("jma/station2026.html", doc.encode("utf-8"))


def hko_year(kind, year, keep):
    doc = json.loads(grab(hko.url(kind, "QUB", year)))
    doc["data"] = [r for r in doc["data"] if keep(int(r[0]), int(r[1]))]
    write("hko/%s_QUB_%d.json" % (kind, year), json.dumps(doc, separators=(",", ":")).encode("utf-8"))


def hko_script():
    js = grab(hko.LIST_URL).decode("utf-8")
    lines = [ln for ln in js.splitlines() if re.match(r"\s*[A-Z0-9]{3}\s*:\s*\{\s*latlon", ln)]
    if len(lines) < 14:
        raise SystemExit("HKO script: %d station lines" % len(lines))
    write("hko/predtide_uc.js", ("var STN = {\n" + "\n".join(lines) + "\n};\n").encode("utf-8"))


def main():
    jma_year(2026, lambda m, d: m == 12 and d >= 15)
    jma_year(2027, lambda m, d: m == 1 and d <= 15)
    jma_list()
    for kind in ("HLT", "HHOT"):
        hko_year(kind, 2026, lambda m, d: m == 12 and d >= 15)
        hko_year(kind, 2027, lambda m, d: m == 1 and d <= 15)
    hko_script()
    write("hko/probe_QUB_20260101.json", grab(hko.url("HLT", "QUB", 2026, 1, 1)))
    write("hko/unknown.html", grab(hko.url("HLT", "XXX", 2026, 1, 1)))


if __name__ == "__main__":
    main()
