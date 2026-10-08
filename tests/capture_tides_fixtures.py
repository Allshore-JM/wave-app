"""Capture NOAA CO-OPS answers for tests/test_tides.py (plan section 38). Run once; the files are committed.

The window starts 2026-10-07 00:00 UTC (BEGIN in the tests); the curve 12 h and the extremes 24 h earlier, as
tide_sources asks.
  honolulu_*      1612340 harmonic (30-minute curve + highs/lows)
  nawiliwili_*    1611400 harmonic, the reference of
  waimea_hilo     1611401 subordinate (highs/lows only; offsets H +7 min, L +18 min)
  sandiego_* / lajolla_*  two harmonic stations ~15 km apart: La Jolla's curve shaped onto San Diego's extremes is
                  compared with San Diego's own curve (the subordinate method, where the truth is known)
  water_level     1612340 observed, last 48 h (whenever captured)
  error           1611401 asked for a 30-minute curve (a subordinate has none): NOAA's error answer
"""
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tide_sources as T  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "tides")
BEGIN = 1791331200                     # 2026-10-07 00:00 UTC


def grab(name, url):
    with urllib.request.urlopen(url, timeout=30) as r:
        body = r.read()
    json.loads(body)
    with open(os.path.join(OUT, name + ".json"), "wb") as f:
        f.write(body)
    print(name, len(body))


def main():
    os.makedirs(OUT, exist_ok=True)
    hilo = dict(product="predictions", interval="hilo", begin_date=T._stamp(BEGIN - 2 * T.LEAD_S), range=T.HILO_HOURS)
    curve = dict(product="predictions", interval="30", begin_date=T._stamp(BEGIN - T.LEAD_S), range=T.CURVE_HOURS)
    for name, sid in (("honolulu", "1612340"), ("nawiliwili", "1611400"), ("sandiego", "9410170"), ("lajolla", "9410230")):
        grab(name + "_hilo", T.url(sid, **hilo))
        grab(name + "_30", T.url(sid, **curve))
    grab("waimea_hilo", T.url("1611401", **hilo))
    grab("water_level", T.url("1612340", product="water_level", range=48))
    grab("error", T.url("1611401", **curve))


if __name__ == "__main__":
    main()
