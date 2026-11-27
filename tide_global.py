"""Tide predictions outside NOAA's network for the map's Tide layer (plan section 112).

Station ids carry a source prefix (NOAA's stay bare, served by tide_sources.TideService as before):

  ticon:<gauge>   a tide gauge's harmonic constants from TICON-4 / TICON-3 (DGFI-TUM, CC BY 4.0), predicted here by
                  tide_harmonic (no upstream request); tide_global.json, built by tools/tides/build_global.py
  <agency>:<id>   an agency's own official predictions (tide_providers/: chs, kv, jma, hko, linz, mi, qld), fetched at
                  most once a station per UTC day; tide_national.json, built by tools/tides/build_national.py. A TICON
                  gauge within OFFICIAL_KM of an official station is not listed (the agency's table is what locals read).

Every answer has the NOAA payload's shape (tide_sources: begin, window, step 1800 s, v[], hilo[[t, h, k]], night,
events, moon, method, source, datum, units), so the page's tide window draws it unchanged. Heights are metres above the
station's lowest astronomical tide (LAT), computed per station over 18.61 years from its own constants (checked against
Norway's chart datum, which is LAT by law: within 3 cm at 11 of 12 gauges).
"""
import importlib
import json
import math
import os
import re
import threading
import time
from collections import OrderedDict

import numpy as np

import tide_harmonic as H
import tide_sources as T
from tide_providers import TideFetchError

HERE = os.path.dirname(os.path.abspath(__file__))
GLOBAL_PATH = os.path.join(HERE, "tide_global.json")
NATIONAL_PATH = os.path.join(HERE, "tide_national.json")
# prefix -> module in tide_providers/ (the agencies whose official predictions are shown)
PROVIDERS = {"chs": "chs", "kv": "kartverket", "jma": "jma", "hko": "hko", "linz": "linz", "mi": "marine_institute",
             "qld": "queensland"}
ID_RE = re.compile(r"(ticon|chs|kv|jma|hko|linz|mi|qld):[a-z0-9][a-z0-9._-]{0,79}")
OFFICIAL_KM = 2.0
ERROR_TTL_S = 60                      # a failed agency fetch is answered "retry" for this long without asking again
WAIT_S = 2.0                          # a second visitor of a station being fetched waits this long, then "busy"
MAX_CURVE_GAP_S = 3 * 3600            # no curve drawn across a gap in an agency's series longer than this
EX_STEP_S = 360                       # the highs and lows are found on a 6-minute series
TICON_CREDIT = ("Predicted from tide-gauge harmonic constants: TICON-4 (Hart-Davis, Dettmering & Seitz, DGFI-TUM), "
                "CC BY 4.0, from GESLA records")
DATUM_LAT = "LAT"


def valid_id(sid):
    return isinstance(sid, str) and bool(ID_RE.fullmatch(sid))


def _km(a_lat, a_lon, b_lat, b_lon):
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    h = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(b_lon - a_lon) / 2) ** 2)
    return 12742.0 * math.asin(min(1.0, math.sqrt(h)))


def load_national(path=NATIONAL_PATH):
    """tide_national.json -> {id: station} (type O), only stations of a known agency module; {} without the file."""
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    fields = doc["fields"]
    out = {}
    for row in doc["stations"]:
        s = dict(zip(fields, row))
        if s["id"].split(":", 1)[0] in PROVIDERS and valid_id(s["id"]):
            s["type"] = "O"
            s["obs"] = False
            out[s["id"]] = s
    return out


def load(path=GLOBAL_PATH, national_path=NATIONAL_PATH):
    """({id: station}, doc): the TICON gauges (type G, with prepared constants 'prep' and LAT 'lat0') and the agencies'
    stations (type O, tide_national.json); a gauge within OFFICIAL_KM of an agency's station is left out."""
    official = load_national(national_path)
    cells = {}
    for s in official.values():
        cells.setdefault((math.floor(s["lat"]), math.floor(s["lon"])), []).append(s)

    def near_official(lat, lon):
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                for o in cells.get((math.floor(lat) + dy, (math.floor(lon) + dx + 180) % 360 - 180), []):
                    if _km(lat, lon, o["lat"], o["lon"]) <= OFFICIAL_KM:
                        return True
        return False
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
        if near_official(s["lat"], s["lon"]):
            continue
        out[s["id"]] = s
    out.update(official)
    return out, doc


def client_rows(stations):
    """The page's rows [id, name, lat, lon, type, tz, obs] (tide_sources.client_list's fields): type G for a TICON
    gauge, O for an agency's station."""
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


def curve_grid(curve, start, n=T.POINTS, step=T.STEP_S):
    """An agency's curve [(epoch, m)] (any regular step) on the window's grid start + i * step: linear between samples
    up to 15 minutes apart, a Catmull-Rom cubic between coarser ones (JMA's and HKO's hourly heights: a straight line
    would cut the tops); None where the series has a gap over MAX_CURVE_GAP_S or does not reach."""
    pts = sorted({int(t): float(v) for t, v in curve}.items())
    if len(pts) < 2:
        return [None] * n
    ts = np.array([p[0] for p in pts], dtype=float)
    vs = np.array([p[1] for p in pts], dtype=float)
    out = [None] * n
    for i in range(n):
        t = start + i * step
        j = int(np.searchsorted(ts, t, side="right")) - 1
        if j < 0 or j >= len(ts) or ts[j] > t:
            continue
        if ts[j] == t:
            out[i] = round(float(vs[j]), 3)
            continue
        if j + 1 >= len(ts) or ts[j + 1] - ts[j] > MAX_CURVE_GAP_S:
            continue
        x = (t - ts[j]) / (ts[j + 1] - ts[j])
        if ts[j + 1] - ts[j] <= 900 or j == 0 or j + 2 >= len(ts):
            v = vs[j] + (vs[j + 1] - vs[j]) * x
        else:
            p0, p1, p2, p3 = vs[j - 1], vs[j], vs[j + 1], vs[j + 2]
            v = 0.5 * ((2 * p1) + (-p0 + p2) * x + (2 * p0 - 5 * p1 + 4 * p2 - p3) * x * x
                       + (-p0 + 3 * p1 - 3 * p2 + p3) * x ** 3)
        out[i] = round(float(v), 3)
    return out


def official_payload(st, begin, mod, got):
    """The window's answer from an agency's fetch() result."""
    start = begin - T.LEAD_S
    hilo = sorted((int(round(t / 60.0)) * 60, round(float(h), 3), k) for t, h, k in got.get("hilo") or []
                  if k in ("H", "L"))
    curve = got.get("curve") or []
    if curve:
        grid, method = curve_grid(curve, start), "official"
    elif hilo:
        grid, method = T.cosine_grid(hilo, start), "official-cosine"
    else:
        raise TideFetchError("the agency answered no predictions")
    if not any(v is not None for v in grid):
        raise TideFetchError("the agency's predictions do not cover the window")
    lo, hi = begin - 2 * T.LEAD_S, begin + T.SPAN_S + 2 * T.LEAD_S
    out = {
        "id": st["id"], "name": st["name"], "lat": st["lat"], "lon": st["lon"], "tz": st["tz"], "type": "O",
        "obs": False, "datum": mod.DATUM, "units": "m",
        "begin": start, "window": [begin, begin + T.SPAN_S], "step": T.STEP_S, "v": grid,
        "hilo": [[t, h, k] for t, h, k in hilo if lo <= t <= hi],
        "night": None, "events": None, "moon": None,
        "method": method, "ref": None, "ref_name": None, "source": mod.CREDIT, "agency": mod.NAME,
        "link": mod.page_url(st),
    }
    a, z = start, begin + T.SPAN_S + T.LEAD_S
    evs = T.sky_events(st["lat"], st["lon"], a, z)
    out["night"] = T.night_bands(st["lat"], st["lon"], a, z, evs)
    out["events"] = T.sun_moon(evs)
    out["moon"] = T.moon_samples(st["lat"], a, z)
    return out


def provider(prefix):
    return importlib.import_module("tide_providers." + PROVIDERS[prefix])


class GlobalTideService:
    """forecast(sid) -> (status, payload) like TideService. TICON stations are computed here; an agency's station is
    fetched through get(url, headers=None, data=None) at most once per UTC day, its failures remembered ERROR_TTL_S.
    Both are kept per station per UTC day (LRU). buckets: {prefix: TokenBucket} limiting each agency's requests (a
    fetch takes its module's MAX_REQUESTS); cap: the request-path build cap shared with the other services."""

    def __init__(self, stations, get=None, now=time.time, max_entries=256, buckets=None, cap=None):
        self.stations = stations
        self._get = get
        self._now = now
        self._max = max_entries
        self._cache = OrderedDict()
        self._errors = {}
        self._building = {}
        self._lock = threading.Lock()
        self._buckets = buckets or {}
        self._cap = cap

    def _put(self, key, payload):
        with self._lock:
            self._cache[key] = payload
            while len(self._cache) > self._max:
                self._cache.popitem(last=False)

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
        if st["type"] == "G":
            try:
                payload = harmonic_payload(st, begin, TICON_CREDIT, "ticon")
            except ValueError as exc:                   # outside the nodal table (after 2050) or a broken station
                return "final", {"id": sid, "error": "No tide prediction for this station (%s)" % exc}
            self._put(key, payload)
            return "ok", payload
        return self._official(st, key, begin)

    def _official(self, st, key, begin):
        sid = st["id"]
        prefix = sid.split(":", 1)[0]
        with self._lock:
            err_until = self._errors.get(key)
            if err_until is not None and self._now() < err_until:
                return "error", {"id": sid, "error": "The agency's tide predictions could not be read"}
            ev = self._building.get(key)
            mine = ev is None
            if mine:
                ev = self._building[key] = threading.Event()
        if not mine:                                    # another visitor is fetching this station: wait a moment
            ev.wait(WAIT_S)
            with self._lock:
                hit = self._cache.get(key)
            return ("ok", hit) if hit is not None else ("busy", {"id": sid, "error": "Busy"})
        try:
            if self._get is None:
                return "error", {"id": sid, "error": "The agency's tide predictions are not available here"}
            mod = provider(prefix)
            bucket = self._buckets.get(prefix)
            if bucket is not None and not bucket.take(mod.MAX_REQUESTS):
                return "busy", {"id": sid, "error": "Busy"}
            if self._cap is not None and not self._cap.acquire(timeout=WAIT_S):
                return "busy", {"id": sid, "error": "Busy"}
            try:
                lo, hi = begin - 2 * T.LEAD_S, begin + T.SPAN_S + 2 * T.LEAD_S
                payload = official_payload(st, begin, mod, mod.fetch(st, lo, hi, self._get))
            finally:
                if self._cap is not None:
                    self._cap.release()
        except Exception as exc:                        # an agency's failure of any kind: "retry", asked again in a minute
            with self._lock:
                self._errors[key] = self._now() + ERROR_TTL_S
            return "error", {"id": sid, "error": "The agency's tide predictions could not be read (%s)"
                             % type(exc).__name__}
        finally:
            with self._lock:
                self._building.pop(key, None)
            ev.set()
        self._put(key, payload)
        with self._lock:
            self._errors.pop(key, None)
        return "ok", payload

    def observed(self, sid, fresh_s=None):
        return "final", {"id": sid, "t": [], "v": [], "note": "This station does not report its water level"}
