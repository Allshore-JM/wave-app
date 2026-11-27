"""Capture CHS (Canada) and Kartverket (Norway) answers for tests/test_tide_chs.py and tests/test_tide_kartverket.py
(plan section 112, step 3). Run once (python -I tests/capture_tide_chs_kartverket_fixtures.py); the files are committed.

Each agency's folder under fixtures/tides_national/ has index.json {url: file}: the tests' fake get serves exactly the
answers the real fetch() received. The window is BEGIN .. BEGIN + 2 days (2026-11-26 00:00 UTC).
  chs/stations.json        the station list cut to KEEP + DROP (kept by build_station_list / left out by it)
  chs/pa_hilo.json         07795 Point Atkinson highs and lows (asked 13 h wider on each side)
  chs/pa_wlp_1/2.json      its 15-minute curve, fetched with CURVE_CHUNK_S = 1 day so the merge of pieces is real
  kartverket/stationlist.xml   the first 10 stations
  kartverket/svg_tab.xml / svg_pre.xml   SVG Stavanger highs and lows, 10-minute curve
  kartverket/error_code.xml / error_time.xml   Kartverket's error answers (an unknown code, a bad time; HTTP 200)
"""
import json
import os
import re
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
from tide_providers import chs, kartverket  # noqa: E402

OUT = os.path.join(HERE, "fixtures", "tides_national")
BEGIN = 1795651200                     # 2026-11-26 00:00 UTC
END = BEGIN + 2 * 86400
UA = "allshoresurf.com tide stations (https://allshoresurf.com)"
CHS_KEEP = ("07795", "00490", "01825", "03104", "04140", "08074")   # Point Atkinson, Halifax, Richibucto Bar
#                                      (discontinued, diurnal), Bouee K136 (virtual, accented), Iqaluit, Campbell River


def real_get(url, headers=None, data=None):
    h = {"User-Agent": UA}
    h.update(headers or {})
    with urllib.request.urlopen(urllib.request.Request(url, headers=h, data=data), timeout=120) as r:
        if r.status != 200:
            raise RuntimeError("HTTP %s" % r.status)
        return r.read()


class Recorder:
    def __init__(self, folder):
        self.folder = os.path.join(OUT, folder)
        os.makedirs(self.folder, exist_ok=True)
        self.index, self.names = {}, []

    def save(self, url, name, body):
        with open(os.path.join(self.folder, name), "wb") as f:
            f.write(body)
        self.index[url] = name
        print(name, len(body))

    def get_as(self, names):
        """A get that saves each answer under the next of names."""
        it = iter(names)

        def get(url, headers=None, data=None):
            body = real_get(url, headers, data)
            self.save(url, next(it), body)
            return body
        return get

    def write_index(self):
        with open(os.path.join(self.folder, "index.json"), "w", encoding="utf-8", newline="\n") as f:
            json.dump(self.index, f, indent=1, sort_keys=True)


def capture_chs():
    rec = Recorder("chs")
    url = chs.API + "/stations"
    full = json.loads(real_get(url, headers=chs.HEADERS))
    keep = [s for s in full if s.get("code") in CHS_KEEP]
    codes = lambda s: {t.get("code") for t in s.get("timeSeries") or []}          # noqa: E731
    drop = [next(s for s in full if "wlp" in codes(s) and "wlp-hilo" not in codes(s)),   # a curve, no highs/lows
            next(s for s in full if not codes(s) & {"wlp", "wlp-hilo"})]                # observations only
    rec.save(url, "stations.json", json.dumps(keep + drop, ensure_ascii=False, indent=0).encode("utf-8"))
    pa = next(s for s in full if s["code"] == "07795")
    st = {"id": "chs:07795", "local": {"uuid": pa["id"], "code": "07795"}}
    chs.CURVE_CHUNK_S = 86400
    chs.fetch(st, BEGIN, END, rec.get_as(["pa_hilo.json", "pa_wlp_1.json", "pa_wlp_2.json"]))
    rec.write_index()


def capture_kartverket():
    rec = Recorder("kartverket")
    body = real_get(kartverket.STATIONLIST_URL, headers=kartverket.HEADERS).decode("utf-8")
    lines = body.splitlines()
    locs = [i for i, l in enumerate(lines) if re.match(r"\s*<location ", l)]
    cut = lines[:locs[0]] + [lines[i] for i in locs[:10]] + lines[locs[-1] + 1:]
    rec.save(kartverket.STATIONLIST_URL, "stationlist.xml", ("\n".join(cut) + "\n").encode("utf-8"))
    st = {"id": "kv:svg", "local": {"code": "SVG"}}
    kartverket.fetch(st, BEGIN, END, rec.get_as(["svg_tab.xml", "svg_pre.xml"]))
    bad_code = kartverket.data_url("XXQ", BEGIN, END, "tab")
    rec.save(bad_code, "error_code.xml", real_get(bad_code, headers=kartverket.HEADERS))
    bad_time = kartverket.data_url("SVG", BEGIN, END, "tab").replace("fromtime=", "fromtime=garbage")
    rec.save(bad_time, "error_time.xml", real_get(bad_time, headers=kartverket.HEADERS))
    rec.write_index()


if __name__ == "__main__":
    capture_chs()
    capture_kartverket()
