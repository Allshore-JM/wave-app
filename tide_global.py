"""Tide predictions outside NOAA's network for the map's Tide layer (plan section 112).

Station ids carry a source prefix (NOAA's stay bare, served by tide_sources.TideService as before):

  ticon:<gauge>   a tide gauge's harmonic constants from TICON-4 / TICON-3 (DGFI-TUM, CC BY 4.0), predicted here by
                  tide_harmonic (no upstream request); tide_global.json, built by tools/tides/build_global.py

Every answer has the NOAA payload's shape (tide_sources: begin, window, step 1800 s, v[], hilo[[t, h, k]], night,
events, moon, method, source, datum, units), so the page's tide window draws it unchanged. Heights are metres above the
station's lowest astronomical tide (LAT), computed per station over 18.61 years from its own constants (checked against
Norway's chart datum, which is LAT by law: within 3 cm at 11 of 12 gauges).
"""
import json
import os
import re
import threading
import time
from collections import OrderedDict

import numpy as np

import tide_harmonic as H
import tide_sources as T

GLOBAL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tide_global.json")
ID_RE = re.compile(r"(ticon):[a-z0-9][a-z0-9._-]{0,79}")
EX_STEP_S = 360                       # the highs and lows are found on a 6-minute series
TICON_CREDIT = ("Predicted from tide-gauge harmonic constants: TICON-4 (Hart-Davis, Dettmering & Seitz, DGFI-TUM), "
                "CC BY 4.0, from GESLA records")
DATUM_LAT = "LAT"


def valid_id(sid):
    return isinstance(sid, str) and bool(ID_RE.fullmatch(sid))


def load(path=GLOBAL_PATH):
    """tide_global.json -> ({id: station}, doc). A station carries its prepared constants ('prep') and LAT ('lat0')."""
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    fields = doc["fields"]
    names = doc["constituents"]
    tab = H.table()
    out = {}
    for row in doc["stations"]:
        s = dict(zip(fields, row))
        consts = [(names[i], a / 10000.0, g / 10.0) for i, a, g in s["c"]]
        s["prep"] = H.prepare(consts, tab, convention=doc.get("convention", "ticon"))
        s["type"] = "G"                                    # a gauge's constants (NOAA's are R / S)
        s["obs"] = False
        del s["c"]
        out[s["id"]] = s
    return out, doc


def client_rows(stations):
    """The page's rows [id, name, lat, lon, type, tz, obs] (tide_sources.client_list's fields)."""
    return [[s["id"], s["name"], s["lat"], s["lon"], s["type"], s["tz"], False] for s in stations.values()]


def harmonic_payload(st, begin, credit, method):
    """The window's answer for a station predicted from constants: the 30-min curve over [begin - LEAD, begin + SPAN +
    LEAD], the highs and lows from a 6-min series over the window +- 2 LEAD, heights above LAT."""
    start = begin - T.LEAD_S
    grid_t = start + T.STEP_S * np.arange(T.POINTS)
    lat0 = float(st["lat0"])
    v = H.predict(st["prep"], grid_t) + lat0
    ex_t = np.arange(begin - 2 * T.LEAD_S, begin + T.SPAN_S + 2 * T.LEAD_S + 1, EX_STEP_S, dtype=float)
    ex = H.extremes(ex_t, H.predict(st["prep"], ex_t) + lat0)
    out = {
        "id": st["id"], "name": st["name"], "lat": st["lat"], "lon": st["lon"], "tz": st["tz"], "type": st["type"],
        "obs": False, "datum": DATUM_LAT, "units": "m",
        "begin": start, "window": [begin, begin + T.SPAN_S], "step": T.STEP_S,
        "v": [round(float(x), 3) for x in v],
        "hilo": [[int(round(t / 60.0)) * 60, round(float(h), 3), k] for t, h, k in ex],
        "night": None, "events": None, "moon": None,
        "method": method, "ref": None, "ref_name": None, "source": credit,
    }
    a, z = start, begin + T.SPAN_S + T.LEAD_S
    evs = T.sky_events(st["lat"], st["lon"], a, z)
    out["night"] = T.night_bands(st["lat"], st["lon"], a, z, evs)
    out["events"] = T.sun_moon(evs)
    out["moon"] = T.moon_samples(st["lat"], a, z)
    return out


class GlobalTideService:
    """forecast(sid) -> (status, payload) like TideService. Harmonic stations are computed here, kept per station per
    UTC day (LRU)."""

    def __init__(self, stations, now=time.time, max_entries=256):
        self.stations = stations
        self._now = now
        self._max = max_entries
        self._cache = OrderedDict()
        self._lock = threading.Lock()

    def forecast(self, sid):
        st = self.stations.get(sid) if valid_id(sid) else None
        if st is None:
            return "final", {"id": sid, "error": "Unknown tide station"}
        begin = T.begin_of(self._now())
        key = (sid, begin)
        with self._lock:
            hit = self._cache.get(key)
            if hit is not None:
                self._cache.move_to_end(key)
                return "ok", hit
        try:
            payload = harmonic_payload(st, begin, TICON_CREDIT, "ticon")
        except ValueError as exc:                       # outside the nodal table (after 2050) or a broken station
            return "final", {"id": sid, "error": "No tide prediction for this station (%s)" % exc}
        with self._lock:
            self._cache[key] = payload
            while len(self._cache) > self._max:
                self._cache.popitem(last=False)
        return "ok", payload

    def observed(self, sid, fresh_s=None):
        return "final", {"id": sid, "t": [], "v": [], "note": "This station does not report its water level"}
