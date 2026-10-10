"""Live wind stations (plan section 39): the latest wind reading of every station on the map's wind layer and a
station's last 24 hours, from four public-domain NOAA / NWS feeds.

The stations are a committed snapshot (wind_stations.json, built by tools/wind/fetch_stations.py): ids "coops:1612340"
(a NOAA tide gauge's weather sensors), "ndbc:51003" (NDBC buoys, C-MAN and other fixed stations), "metar:PHNL"
(airports and offshore platforms within 30 km of a coast or at sea) and "nws:001HE" (the land weather stations the
NWS API lists for Hawaii: HECO / HELCO / MECO, the University of Hawaii, RAWS, CWOP, HADS and others, step 5c). A
reading is {t: epoch seconds, s: speed m/s, g: gust m/s or None, d: direction the wind blows FROM in degrees true,
or None when calm or variable}; a HISTORY row may also carry the weather the same report holds (WX_FIELDS: at / wt air
and water temperature C, dp dew point C, rh humidity %, p pressure hPa, pt its 3-hour change hPa, vis visibility km,
wx weather words), step 5d: the window shows the current conditions and a 24-hour temperature chart.

The latest readings come from four BuoyProvider subclasses (buoy_sources: the section-36 stale-while-revalidate
lists, keep-last on a failed fetch, the shared refresh runner and the fork reset) kept in a list of their OWN
(app.get_wind_providers): never in the live-buoy list, so the live memo, route and golden stay as they are.
  NDBC   one GET of data/latest_obs/latest_obs.txt (every station with a wind speed now; ~5-minute file)
  METAR  one GET of aviationweather.gov's metars.cache.csv.gz (the last ~80 minutes of reports, whole knots)
  COOPS  one datagetter request per gauge (product=wind&date=latest, 6-minute readings), a few at a time, PACED
  NWS    one api.weather.gov request per station (its newest observations of the last 2 hours), PACED the same way
The merged answer (build_latest) is a columnar table the page draws its flags from. A station's history comes from
WindHistory (the tide service's cache / per-key lock / busy core): NDBC realtime2/<ID>.txt cut at 24 h, CO-OPS
product=wind&range=24, the METAR API's hours=24 and the NWS API's observations?start= each behind a token bucket
(the METAR API allows 100 requests a minute; the NWS API's limit is not published).
Every upstream request goes through the caller's fetch(url, max_bytes, headers=None) -> bytes (app._points_fetch:
one attempt, a wall-clock cap); a User-Agent names the site, as aviationweather.gov asks.
Terms: NOAA / NWS data are public domain; attribution requested (ATTRIBUTION)."""
import calendar
import csv
import gzip
import io
import json
import logging
import math
import os
import re
import threading
import time
import weakref
import zlib
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import buoy_sources

_log = logging.getLogger(__name__)

NDBC_LATEST_URL = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"
NDBC_RT2_URL = "https://www.ndbc.noaa.gov/data/realtime2/%s.txt"
COOPS_API = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
COOPS_APPLICATION = "allshoresurf.com"         # CO-OPS asks every client to name itself
METAR_CACHE_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
METAR_API_URL = "https://aviationweather.gov/api/data/metar?ids=%s&format=json&hours=24"
NWS_API = "https://api.weather.gov"
NWS_OBS_URL = NWS_API + "/stations/%s/observations?start=%s&limit=%d"   # (station, start ISO, limit); newest first
USER_AGENT = "allshoresurf.com live wind (https://allshoresurf.com)"
HEADERS = {"User-Agent": USER_AGENT}
NWS_HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/ld+json"}   # ld+json: the same rows without GeoJSON
ATTRIBUTION = {"ndbc": "NOAA National Data Buoy Center (ndbc.noaa.gov)",
               "coops": "NOAA CO-OPS (tidesandcurrents.noaa.gov)",
               "metar": "NWS Aviation Weather Center METAR (aviationweather.gov)",
               "nws": "NWS API observations (api.weather.gov): HECO / HELCO / MECO, University of Hawaii, RAWS, CWOP and "
                      "other networks via MADIS"}
KT_MS = 0.514444                               # one knot in m/s (METAR speeds are whole knots)
MAX_SPEED_MS = 120.0                           # above this a reading is garbage (the record gust is ~113 m/s)
STALE_S = 2 * 3600                             # a reading older than this is drawn grey (airports report hourly)
HISTORY_S = 24 * 3600
HISTORY_ROWS = 300                             # 6-minute readings over 24 h are 240; the cap bounds the answer
NDBC_LATEST_MAX = 1024 * 1024                  # latest_obs.txt is ~105 KB
NDBC_RT2_MAX = 2 * 1024 * 1024                 # 45 days of 6-minute rows are ~1 MB
METAR_CACHE_MAX = 2 * 1024 * 1024              # ~280 KB gzipped ...
METAR_CACHE_INFLATED_MAX = 16 * 1024 * 1024    # ... ~3.5 MB inflated (a bomb is cut here)
METAR_API_MAX = 1024 * 1024
COOPS_MAX = 256 * 1024
COOPS_TTL_S = 60                               # the CO-OPS feed's refresh period: one slice of gauges per refresh
COOPS_PER_REFRESH = 24                         # gauges asked per refresh (232 gauges: every gauge about every 10 minutes)
COOPS_PAUSE_S = 120                            # after NOAA refuses (HTTP 403): no CO-OPS request for this long
COOPS_KEEP_S = 6 * 3600                        # a CO-OPS reading older than this is dropped (the flag then says "no reading")
METAR_BUCKET_PER_MIN = 60                      # the API's limit is 100 requests a minute per client
NWS_TTL_S = 60                                 # the NWS feed's refresh period (one slice of stations per refresh)
NWS_PER_REFRESH = 25                           # stations asked per refresh (Hawaii's ~300: every station about every 12 min)
NWS_PAUSE_S = 120                              # after the API refuses (HTTP 403 / 429): no NWS request for this long
NWS_KEEP_S = 6 * 3600                          # an NWS reading older than this is dropped
NWS_FEED_WINDOW_S = 2 * 3600                   # the feed asks each station's observations of the last 2 hours ...
NWS_FEED_LIMIT = 6                             # ... the newest 6 (the top of an hour is often a gust-only row)
NWS_FEED_MAX = 512 * 1024                      # a row is ~3 KB
NWS_HISTORY_LIMIT = 500                        # the API's most per request (a 5-minute station has 288 rows in 24 h)
NWS_HISTORY_MAX = 4 * 1024 * 1024
NWS_BUCKET_PER_MIN = 30                        # histories; with the feed's 25 a minute the server stays under ~55 a minute
NWS_QC_REJECTED = ("X", "B")                   # MADIS quality control: rejected / subjectively bad values are dropped
NWS_UNITS = {"wmoUnit:km_h-1": 1 / 3.6, "wmoUnit:m_s-1": 1.0, "wmoUnit:[kn_i]": KT_MS, "wmoUnit:mi_h-1": 0.44704}
NWS_TEMP_UNITS = {"wmoUnit:degC": (1.0, 0.0), "wmoUnit:degF": (5 / 9, -32 * 5 / 9), "wmoUnit:K": (1.0, -273.15)}
NWS_PRESSURE_UNITS = {"wmoUnit:Pa": 0.01, "wmoUnit:hPa": 1.0, "wmoUnit:mbar": 1.0}
NWS_LENGTH_UNITS = {"wmoUnit:m": 0.001, "wmoUnit:km": 1.0, "wmoUnit:[mi_i]": 1.609344}
WX_FIELDS = ("at", "wt", "dp", "rh", "p", "pt", "vis", "wx")   # the weather a history row may carry (see the module docstring)
CONDITIONS_S = 3 * 3600                        # the current conditions: each field's newest value within this of the newest row
TREND_S = 3 * 3600                             # the pressure's change over this long ("rising" / "falling" on the page)
COOPS_WX_PRODUCTS = {"a": ("air_temperature", "at"), "w": ("water_temperature", "wt"), "p": ("air_pressure", "p"),
                     "h": ("humidity", "rh")}   # the snapshot's `wx` letters -> (datagetter product, the row's field)
NMI_KM = 1.852
SM_KM = 1.609344
METAR_COVER = {"CLR": "Clear", "SKC": "Clear", "CAVOK": "Clear", "FEW": "A few clouds", "SCT": "Scattered clouds",
               "BKN": "Mostly cloudy", "OVC": "Overcast", "OVX": "Sky obscured"}
METAR_WX = {"RA": "rain", "DZ": "drizzle", "SN": "snow", "SG": "snow grains", "GR": "hail", "GS": "small hail",
            "PL": "ice pellets", "IC": "ice crystals", "UP": "precipitation", "BR": "mist", "FG": "fog", "HZ": "haze",
            "FU": "smoke", "DU": "dust", "SA": "sand", "VA": "volcanic ash", "SQ": "squalls", "FC": "funnel cloud",
            "PO": "dust whirls", "DS": "dust storm", "SS": "sandstorm", "PY": "spray"}
METAR_WX_DESC = {"SH": "showers", "TS": "thunderstorm", "FZ": "freezing", "BL": "blowing", "DR": "drifting",
                 "MI": "shallow", "BC": "patches of", "PR": "partial"}
ID_RE = re.compile(r"(coops|ndbc|metar|nws):[A-Z0-9]{3,8}")
_NUM = re.compile(r"-?\d+(\.\d+)?$")

UNAVAILABLE = "The station's agency could not be reached; try again in a moment"
BUSY = "The server is busy with other wind stations; try again in a moment"
NO_HISTORY = "No wind readings in the last 24 hours"
NDBC_NO_FILE = "NDBC publishes no 24-hour history for this station; the flag shows its latest report"


class WindError(Exception):
    """A passing failure (the feed could not be fetched or read): cached a minute, then asked again."""


class WindBusy(WindError):
    """Too many builds or METAR requests at once: never cached, the page asks again in a moment."""


class WindNoFile(WindError):
    """NDBC publishes no 24-hour file for the station (HTTP 404): a known, lasting answer, not a passing failure."""


# ---------------------------------------------------------------------------------------------- readings

def _speed(v):
    """A speed in m/s from a number or a numeric string; None when missing or not a plausible speed."""
    try:
        s = float(v)
    except (TypeError, ValueError):
        return None
    return s if math.isfinite(s) and 0.0 <= s <= MAX_SPEED_MS else None


def _direction(v):
    """A direction in degrees true [0, 360) from a number or a numeric string; None when missing or out of range."""
    try:
        d = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(d) or d < 0 or d > 360:
        return None
    return int(round(d)) % 360


def reading(t, s, g=None, d=None):
    """A reading dict with its fields checked (t epoch seconds; s, g m/s; d degrees FROM). None without t or s. A
    speed that rounds to 0.0 is calm: it keeps no direction (step 5c D)."""
    try:
        t = int(t)
    except (TypeError, ValueError):
        return None
    s = _speed(s)
    if s is None:
        return None
    s = round(s, 1)
    return {"t": t, "s": s, "g": (round(_speed(g), 1) if _speed(g) is not None else None), "d": _direction(d) if s > 0 else None}


def _num_in(v, lo, hi, digits=1):
    """A number within [lo, hi] rounded to `digits`, else None (missing, "MM", out of range, not finite)."""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x) or x < lo or x > hi:
        return None
    return int(round(x)) if digits == 0 else round(x, digits)


def _temp(v): return _num_in(v, -90.0, 60.0)                           # degC
def _pct(v): return _num_in(v, 0.0, 100.0, 0)
def _hpa(v): return _num_in(v, 800.0, 1100.0)
def _hpa_delta(v): return _num_in(v, -60.0, 60.0)
def _km(v): return _num_in(v, 0.0, 500.0)


def _words(v, limit=48):
    """Weather words: a short plain string, or None."""
    w = " ".join(str(v or "").split())
    return w[:limit] if w else None


def with_weather(r, **fields):
    """The reading with the weather fields that have a value (keys of WX_FIELDS); the others left out."""
    if r is None:
        return None
    for k, v in fields.items():
        if k in WX_FIELDS and v is not None:
            r[k] = v
    return r


def humidity_from(at, dp):
    """Relative humidity (%) from air temperature and dew point (degC; Magnus, Alduchov-Eskridge constants), or None."""
    if at is None or dp is None:
        return None
    try:
        rh = 100.0 * math.exp(17.625 * dp / (243.04 + dp)) / math.exp(17.625 * at / (243.04 + at))
    except (OverflowError, ZeroDivisionError):
        return None
    return int(round(max(1.0, min(100.0, rh))))


def iso_z(t):
    return datetime.fromtimestamp(int(t), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------------------------- NDBC

def _ndbc_epoch(parts):
    """Epoch seconds from the YYYY MM DD hh mm columns (UTC), or None."""
    try:
        y, mo, d, h, mi = (int(x) for x in parts)
        return calendar.timegm((y, mo, d, h, mi, 0))
    except (TypeError, ValueError, OverflowError):
        return None


def parse_latest_obs(text):
    """latest_obs.txt -> {ID: reading} for every row with a wind speed (WSPD not "MM"). The header names the
    columns (#STN LAT LON YYYY MM DD hh mm WDIR WSPD GST ...); "MM" is NOAA's missing value."""
    out, cols = {}, None
    for line in text.splitlines():
        if line.startswith("#"):
            if cols is None:
                cols = line.lstrip("#").split()
            continue
        parts = line.split()
        if not cols or len(parts) != len(cols):
            continue
        row = dict(zip(cols, parts))
        t = _ndbc_epoch([row.get(k) for k in ("YYYY", "MM", "DD", "hh", "mm")])
        r = reading(t, row.get("WSPD"), row.get("GST"), row.get("WDIR"))
        if r is not None:
            out[row["STN"].upper()] = r
    return out


def parse_realtime2(text, since, max_rows=HISTORY_ROWS):
    """realtime2/<ID>.txt (newest first; #YY MM DD hh mm WDIR WSPD GST ...) -> readings with t >= since, newest first,
    at most max_rows. Rows without a wind speed are skipped; the scan stops at the first row older than since."""
    out, cols = [], None
    for line in text.splitlines():
        if line.startswith("#"):
            if cols is None:
                cols = [c.lstrip("#") for c in line.split()]
            continue
        parts = line.split()
        if not cols or len(parts) != len(cols):
            continue
        row = dict(zip(cols, parts))
        t = _ndbc_epoch([row.get(k) for k in ("YY", "MM", "DD", "hh", "mm")])
        if t is None:
            continue
        if t < since:
            break
        r = reading(t, row.get("WSPD"), row.get("GST"), row.get("WDIR"))
        if r is not None:
            vis = _km(row.get("VIS"))
            with_weather(r, at=_temp(row.get("ATMP")), wt=_temp(row.get("WTMP")), dp=_temp(row.get("DEWP")),
                         p=_hpa(row.get("PRES")), pt=_hpa_delta(row.get("PTDY")),
                         vis=round(vis * NMI_KM, 1) if vis is not None else None)        # VIS in nautical miles
            out.append(r)
            if len(out) >= max_rows:
                break
    return out


# ---------------------------------------------------------------------------------------------- CO-OPS

def coops_url(station, **params):
    q = dict(product="wind", station=station, units="metric", time_zone="gmt", format="json",
             application=COOPS_APPLICATION)
    q.update(params)
    return COOPS_API + "?" + "&".join("%s=%s" % (k, v) for k, v in q.items())


def coops_error(doc):
    """NOAA's error message in a datagetter answer (HTTP 200), or None."""
    if isinstance(doc, dict) and isinstance(doc.get("error"), dict):
        return str(doc["error"].get("message") or "error")
    return None


def _coops_epoch(text):
    """"2026-10-10 16:48" (GMT) -> epoch seconds, or None."""
    try:
        return calendar.timegm(time.strptime(str(text).strip(), "%Y-%m-%d %H:%M"))
    except (TypeError, ValueError):
        return None


def parse_coops_series(doc):
    """datagetter product=air_temperature / water_temperature / air_pressure / humidity (units=metric) -> {epoch: value}
    for the rows carrying a number; an error answer or anything else -> {}."""
    out = {}
    rows = doc.get("data") if isinstance(doc, dict) else None
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        t = _coops_epoch(row.get("t"))
        v = _num_in(row.get("v"), -1e6, 1e6)
        if t is not None and v is not None:
            out[t] = v
    return out


def parse_coops_wind(doc):
    """datagetter product=wind (units=metric: s and g in m/s, d degrees true) -> readings in the order given."""
    out = []
    rows = doc.get("data") if isinstance(doc, dict) else None
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        r = reading(_coops_epoch(row.get("t")), row.get("s"), row.get("g"), row.get("d"))
        if r is not None:
            out.append(r)
    return out


# ---------------------------------------------------------------------------------------------- METAR

def gunzip_bounded(raw, limit=METAR_CACHE_INFLATED_MAX):
    """Inflate a gzip body, at most `limit` bytes of output (ValueError beyond: a decompression bomb)."""
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    out = d.decompress(raw, limit + 1)
    if len(out) > limit or d.unconsumed_tail:
        raise ValueError("gzip body too large")
    return out


def _metar_reading(t, dir_v, spd_kt, gst_kt):
    """A reading from METAR fields: speeds in whole knots; a direction of 0 (calm, or "VRB" variable) or a non-number
    ("VRB" in the API) is no direction; a calm report (speed 0) has none either."""
    s = _speed(spd_kt)
    if s is None:
        return None
    try:
        raw = float(dir_v)                                             # "VRB" (the API) is no number
    except (TypeError, ValueError):
        raw = None
    d = None if raw is None or raw == 0 or s == 0 else _direction(raw)   # 360 = north; 0 = calm / variable
    g = _speed(gst_kt)
    return reading(t, s * KT_MS, g * KT_MS if g is not None else None, d)


def _iso_epoch(text):
    """"2026-10-10T01:38:00.000Z" -> epoch seconds, or None."""
    try:
        return calendar.timegm(time.strptime(str(text).strip()[:19], "%Y-%m-%dT%H:%M:%S"))
    except (TypeError, ValueError):
        return None


def parse_metar_cache(raw):
    """metars.cache.csv(.gz) -> {ID: reading} for the reports carrying a wind speed (the newest per station: the
    file lists several reports of one station, newest first)."""
    if raw[:2] == b"\x1f\x8b":
        raw = gunzip_bounded(raw)
    rd = csv.reader(io.StringIO(raw.decode("utf-8", "replace")))
    head = next(rd, [])
    try:
        ix = [head.index(c) for c in ("station_id", "observation_time", "wind_dir_degrees", "wind_speed_kt",
                                      "wind_gust_kt")]
    except ValueError:
        raise ValueError("metar cache header")
    out = {}
    for row in rd:
        if len(row) <= max(ix):
            continue
        sid = row[ix[0]].strip().upper()
        r = _metar_reading(_iso_epoch(row[ix[1]]), row[ix[2]], row[ix[3]], row[ix[4]])
        if r is not None and (sid not in out or r["t"] > out[sid]["t"]):
            out[sid] = r
    return out


def metar_visibility_km(v):
    """visib in statute miles ("10+" = 10 or more, a number, "1/2") -> km, or None."""
    text = str(v if v is not None else "").strip()
    if not text:
        return None
    more = text.endswith("+")
    text = text.rstrip("+")
    try:
        if "/" in text:
            a, b = text.split("/", 1)
            miles = float(a) / float(b)
        else:
            miles = float(text)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    km = _km(miles * SM_KM)
    return km if km is None or not more else max(km, 16.0)


def metar_weather_words(wx, cover):
    """Plain words for a METAR's present weather ("-SHRA" -> "light showers of rain", "VCTS" -> "thunderstorm nearby",
    "BR" -> "mist"; several groups joined), else the sky cover's words ("FEW" -> "A few clouds"); None without either."""
    groups = []
    for code in str(wx or "").split():
        c = code.upper()
        if c in ("NSW", "NOSIG"):
            continue
        words = []
        if c.startswith("+"):
            words.append("heavy"); c = c[1:]
        elif c.startswith("-"):
            words.append("light"); c = c[1:]
        nearby = c.startswith("VC")
        if nearby:
            c = c[2:]
        if c.startswith("RE"):
            c = c[2:]; words.append("recent")
        desc = c[:2] if c[:2] in METAR_WX_DESC else None
        if desc:
            c = c[2:]
        phen = []
        while c:
            phen.append(METAR_WX.get(c[:2], c[:2].lower())); c = c[2:]
        if desc and phen:
            words.append(METAR_WX_DESC[desc] + (" of " if desc == "SH" else " with " if desc == "TS" else " ") + " and ".join(phen))
        elif desc:
            words.append(METAR_WX_DESC[desc])
        elif phen:
            words.append(" and ".join(phen))
        if nearby:
            words.append("nearby")
        if words:
            groups.append(" ".join(words))
    if groups:
        return _words(", ".join(groups))
    return METAR_COVER.get(str(cover or "").strip().upper())


def parse_metar_history(doc):
    """api/data/metar (format=json) -> readings in the order given. obsTime is epoch seconds; wdir an integer or
    "VRB"; wspd / wgst whole knots (wgst absent without a gust); temp / dewp degC; slp (else altim) hPa; visib
    statute miles; wxString + cover -> weather words."""
    out = []
    for row in doc if isinstance(doc, list) else []:
        if not isinstance(row, dict):
            continue
        r = _metar_reading(row.get("obsTime"), row.get("wdir"), row.get("wspd"), row.get("wgst"))
        if r is not None:
            with_weather(r, at=_temp(row.get("temp")), dp=_temp(row.get("dewp")),
                         p=_hpa(row.get("slp")) if _hpa(row.get("slp")) is not None else _hpa(row.get("altim")),
                         vis=metar_visibility_km(row.get("visib")), wx=metar_weather_words(row.get("wxString"), row.get("cover")))
            out.append(r)
    return out


# ---------------------------------------------------------------------------------------------- NWS API

def _nws_epoch(text):
    """"2026-10-10T20:50:00+00:00" (or "...Z") -> epoch seconds, or None."""
    try:
        t = datetime.fromisoformat(str(text).strip().replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return int(t.timestamp())


def _nws_quantity(q, units=None):
    """An observation field {"unitCode", "value", "qualityControl"} -> a float (in m/s when `units` converts the
    code), or None when missing, rejected by quality control or in an unknown unit."""
    if not isinstance(q, dict) or q.get("value") is None or q.get("qualityControl") in NWS_QC_REJECTED:
        return None
    try:
        v = float(q["value"])
    except (TypeError, ValueError):
        return None
    if units is None:
        return v
    k = units.get(str(q.get("unitCode") or ""))
    return v * k if k is not None else None


def parse_nws_observations(doc):
    """A stations/<ID>/observations answer (ld+json "@graph", or GeoJSON "features") -> readings in the order given
    (newest first), only the rows with a wind speed: speeds converted to m/s by their unit code, a calm row (speed
    0) has no direction."""
    items = doc.get("@graph") if isinstance(doc, dict) else None
    if items is None and isinstance(doc, dict):
        items = [f.get("properties") for f in doc.get("features") or [] if isinstance(f, dict)]
    out = []
    for row in items or []:
        if not isinstance(row, dict):
            continue
        s = _nws_quantity(row.get("windSpeed"), NWS_UNITS)
        if s is None:
            continue
        d = None if s == 0 else _nws_quantity(row.get("windDirection"))
        r = reading(_nws_epoch(row.get("timestamp")), s, _nws_quantity(row.get("windGust"), NWS_UNITS), d)
        if r is not None:
            p = _nws_quantity(row.get("seaLevelPressure"), NWS_PRESSURE_UNITS)
            if p is None:
                p = _nws_quantity(row.get("barometricPressure"), NWS_PRESSURE_UNITS)
            with_weather(r, at=_temp(_nws_temp(row.get("temperature"))), dp=_temp(_nws_temp(row.get("dewpoint"))),
                         rh=_pct(_nws_quantity(row.get("relativeHumidity"))), p=_hpa(p),
                         vis=_km(_nws_quantity(row.get("visibility"), NWS_LENGTH_UNITS)), wx=_words(row.get("textDescription")))
            out.append(r)
    return out


def _nws_temp(q):
    """A temperature field -> degC (degC, degF or K by its unit code), or None."""
    v = _nws_quantity(q)
    if v is None:
        return None
    k = NWS_TEMP_UNITS.get(str((q or {}).get("unitCode") or ""))
    return v * k[0] + k[1] if k else None


# ---------------------------------------------------------------------------------------------- the snapshot

def load_stations(path):
    """wind_stations.json -> ({id: station dict}, the document). Fields per row are named in the file's "fields"."""
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    fields = doc["fields"]
    out = {}
    for row in doc["stations"]:
        s = dict(zip(fields, row))
        out[s["id"]] = s
    return out, doc


def client_list(doc):
    """The page's copy of the list: the file's rows as they are (id, name, lat, lon, kind, src, tz, alias)."""
    return {"captured": doc.get("captured"), "source": doc.get("source"), "fields": list(doc["fields"]),
            "stations": [list(row) for row in doc["stations"]]}


def valid_id(sid):
    return isinstance(sid, str) and bool(ID_RE.fullmatch(sid))


def by_source(stations):
    """{src: {local id: station}} over the snapshot."""
    out = {}
    for sid, st in stations.items():
        src, _, local = sid.partition(":")
        out.setdefault(src, {})[local] = st
    return out


# ---------------------------------------------------------------------------------------------- providers

class WindProvider(buoy_sources.BuoyProvider):
    """A live-wind source: its station list is the snapshot's stations of its kind that have a reading NOW, each
    entry carrying the reading under "wind" (the base class copies extra_keys) and latest_time (so is_stale is set
    past stale_after_sec). fetch(url, max_bytes, headers=None) -> bytes raises on any failure; a failure of the
    whole fetch raises out of _fetch_stations, so the base class keeps the last good list (keep-last)."""
    license_label = "Public domain (US Govt)"
    capabilities = buoy_sources._caps(bulk=True, recent_history=True)
    extra_keys = ("wind", "alias_of")
    stale_after_sec = STALE_S
    warm_rank = 0

    def __init__(self, stations, fetch):
        super().__init__(http=None)
        self.stations = dict(stations)          # local id -> snapshot station
        self._fetch = fetch

    def _entry(self, local, r, extra=None):
        st = self.stations.get(local) or {}
        e = {"local_id": local, "name": st.get("name") or local, "lat": st.get("lat"), "lon": st.get("lon"),
             "wind": [r["s"], r["g"], r["d"]], "latest_time": iso_z(r["t"])}
        if extra:
            e.update(extra)
        return e

    def latest(self, local_id):
        snap = self.snapshot()
        for e in (snap[0] if snap else []):
            if e["id"].split(":", 1)[1] == local_id:
                return {"time_utc": e.get("latest_time"), "wind": e.get("wind")}
        return None


class NdbcWindProvider(WindProvider):
    """Every NDBC station with a wind speed in latest_obs.txt that the snapshot lists; the relays of CO-OPS gauges
    (a gauge's `alias` in the snapshot) are listed too, marked alias_of the gauge's id, so the merged table can
    fall back on NDBC's copy when CO-OPS has no reading for the gauge."""
    source = "NDBC"
    source_name = "NOAA NDBC"
    source_url = "https://www.ndbc.noaa.gov"
    attribution_text = "Source: " + ATTRIBUTION["ndbc"]
    list_ttl_sec = 300
    warm_rank = 0

    def __init__(self, stations, fetch, aliases=None):
        super().__init__(stations, fetch)
        self.aliases = dict(aliases or {})      # NDBC id -> the gauge's snapshot station (id "coops:1612340")

    def _fetch_stations(self):
        body = self._fetch(NDBC_LATEST_URL, NDBC_LATEST_MAX, HEADERS)
        obs = parse_latest_obs(body.decode("utf-8", "replace"))
        out = []
        for local, r in obs.items():
            if local in self.stations:
                out.append(self._entry(local, r))
            elif local in self.aliases:
                g = self.aliases[local]
                out.append(self._entry(local, r, {"alias_of": g["id"], "name": g.get("name") or local,
                                                  "lat": g.get("lat"), "lon": g.get("lon")}))
        return out


class MetarProvider(WindProvider):
    """Every snapshot airport with a wind speed in the METAR cache (the last ~80 minutes of reports)."""
    source = "METAR"
    source_name = "NWS Aviation Weather Center"
    source_url = "https://aviationweather.gov"
    attribution_text = "Source: " + ATTRIBUTION["metar"]
    list_ttl_sec = 300
    warm_rank = 1

    def _fetch_stations(self):
        body = self._fetch(METAR_CACHE_URL, METAR_CACHE_MAX, HEADERS)
        obs = parse_metar_cache(body)
        return [self._entry(local, r) for local, r in obs.items() if local in self.stations]


class PacedProvider(WindProvider):
    """A feed that asks ONE request per station, PACED: each refresh (every list_ttl_sec) asks the next
    `per_refresh` stations in turn, `workers` at a time, and merges their readings into the ones kept, so every
    station is asked about every (stations / per_refresh) minutes and the agency sees a steady trickle. A station
    whose request fails keeps its last reading; the agency's "no data" drops it; a reading older than keep_s is
    dropped. When EVERY request of a slice fails the fetch fails (keep-last applies); an answer in refused_codes
    (HTTP 403 / 429) pauses the feed for pause_s (the kept readings stay). Subclasses give the codes, the messages
    and _one(local) -> (local, reading or None, error text or None)."""
    refused_codes = ("403",)
    pause_s = COOPS_PAUSE_S
    keep_s = COOPS_KEEP_S
    unit = "stations"
    every_msg = "every request failed (%s)"
    refused_msg = "the agency refused a request (HTTP %s): paused for %d s"
    paused_msg = "paused after the agency's HTTP %s (%d s left)"

    def __init__(self, stations, fetch, workers=2, per_refresh=COOPS_PER_REFRESH):
        super().__init__(stations, fetch)
        self.workers = max(1, min(8, int(workers)))
        self.per_refresh = max(1, min(500, int(per_refresh)))
        self._ids = sorted(self.stations)
        self._cursor = 0                        # the next station to ask (round robin over _ids)
        self._readings = {}                     # local id -> the latest reading kept
        self._paused_until = 0.0

    def _slice(self):
        """The next per_refresh stations in turn (all of them when there are fewer)."""
        n = len(self._ids)
        if not n:
            return []
        start = self._cursor % n
        out = [self._ids[(start + i) % n] for i in range(min(self.per_refresh, n))]
        self._cursor = (start + len(out)) % n
        return out

    def _one(self, local):
        raise NotImplementedError

    def _refused(self, err):
        return any(("HTTP %s" % c) in err for c in self.refused_codes)

    def _fetch_stations(self):
        now = buoy_sources.time.time()
        if now < self._paused_until:
            raise RuntimeError(self.paused_msg % ("/".join(self.refused_codes), round(self._paused_until - now)))
        ids = self._slice()
        if not ids:
            return []
        failures, last, refused = 0, None, None
        with ThreadPoolExecutor(self.workers) as pool:
            for local, r, err in pool.map(self._one, ids):
                if err:
                    failures += 1
                    last = err
                    if self._refused(err):
                        refused = err
                elif r is None:
                    self._readings.pop(local, None)                    # the agency: no data for the station now
                else:
                    self._readings[local] = r
        if refused:
            self._paused_until = buoy_sources.time.time() + self.pause_s
            code = next((c for c in self.refused_codes if ("HTTP %s" % c) in refused), self.refused_codes[0])
            raise RuntimeError(self.refused_msg % (code, self.pause_s))
        if failures == len(ids):
            raise RuntimeError(self.every_msg % last)
        if failures:
            _log.info("wind provider %s: %d of %d %s could not be asked (%s)", self.source, failures, len(ids),
                      self.unit, last)
        cut = now - self.keep_s
        for local in [k for k, r in self._readings.items() if r["t"] < cut]:
            del self._readings[local]
        return [self._entry(local, r) for local, r in sorted(self._readings.items())]

    def status(self, now=None):
        s = super().status(now)
        now = buoy_sources.time.time() if now is None else now
        s.update(readings=len(self._readings), per_refresh=self.per_refresh, cursor=self._cursor,
                 paused_s=round(max(0.0, self._paused_until - now), 1))
        return s


class CoopsWindProvider(PacedProvider):
    """The snapshot gauges' latest 6-minute readings, one datagetter request per gauge, paced (PacedProvider):
    NOAA answered HTTP 403 to everything from the server for about a minute after 232 requests in a few seconds
    (step 5, F1: new tide windows failed then too); 24 gauges a minute asks every gauge about every 10 minutes."""
    source = "COOPS"
    source_name = "NOAA CO-OPS"
    source_url = "https://tidesandcurrents.noaa.gov"
    attribution_text = "Source: " + ATTRIBUTION["coops"]
    list_ttl_sec = COOPS_TTL_S
    warm_rank = 2
    refused_codes = ("403",)
    pause_s = COOPS_PAUSE_S
    keep_s = COOPS_KEEP_S
    unit = "gauges"
    every_msg = "every CO-OPS request failed (%s)"
    refused_msg = "NOAA refused a CO-OPS request (HTTP %s): paused for %d s"
    paused_msg = "paused after NOAA's HTTP %s (%d s left)"

    def _one(self, local):
        """(local id, reading or None, failed): failed is a fetch / parse failure, not NOAA's "no data"."""
        try:
            doc = json.loads(self._fetch(coops_url(local, date="latest"), COOPS_MAX, HEADERS))
        except Exception as e:
            return local, None, "%s: %s" % (type(e).__name__, e)
        if coops_error(doc):
            return local, None, None
        rows = parse_coops_wind(doc)
        return local, (max(rows, key=lambda r: r["t"]) if rows else None), None


class NwsProvider(PacedProvider):
    """The NWS API's land weather stations (step 5c; Hawaii first): one observations request per station for its
    newest NWS_FEED_LIMIT rows of the last NWS_FEED_WINDOW_S, paced like the CO-OPS feed (the API's rate limit is
    not published; it answers 403 or 429 to abuse). The newest row with a wind speed is the reading; an empty
    answer (no observations, or a station the API no longer knows) drops the station's reading."""
    source = "NWS"
    source_name = "NWS API"
    source_url = "https://api.weather.gov"
    attribution_text = "Source: " + ATTRIBUTION["nws"]
    list_ttl_sec = NWS_TTL_S
    warm_rank = 3
    refused_codes = ("403", "429")
    pause_s = NWS_PAUSE_S
    keep_s = NWS_KEEP_S
    unit = "stations"
    every_msg = "every NWS API request failed (%s)"
    refused_msg = "the NWS API refused a request (HTTP %s): paused for %d s"
    paused_msg = "paused after the NWS API's HTTP %s (%d s left)"

    def __init__(self, stations, fetch, workers=2, per_refresh=NWS_PER_REFRESH):
        super().__init__(stations, fetch, workers, per_refresh)

    def _one(self, local):
        start = iso_z(buoy_sources.time.time() - NWS_FEED_WINDOW_S)
        try:
            doc = json.loads(self._fetch(NWS_OBS_URL % (local, start, NWS_FEED_LIMIT), NWS_FEED_MAX, NWS_HEADERS))
        except Exception as e:
            return local, None, "%s: %s" % (type(e).__name__, e)
        rows = parse_nws_observations(doc)
        return local, (max(rows, key=lambda r: r["t"]) if rows else None), None


def make_providers(stations, fetch, coops_workers=2, coops_per_refresh=COOPS_PER_REFRESH, nws_workers=2,
                   nws_per_refresh=NWS_PER_REFRESH):
    """The four providers over the snapshot's stations, in warm order (cheap, quick feeds first)."""
    groups = by_source(stations)
    aliases = {st["alias"]: st for sid, st in stations.items() if st.get("alias") and sid.startswith("coops:")}
    return [NdbcWindProvider(groups.get("ndbc", {}), fetch, aliases),
            MetarProvider(groups.get("metar", {}), fetch),
            CoopsWindProvider(groups.get("coops", {}), fetch, coops_workers, coops_per_refresh),
            NwsProvider(groups.get("nws", {}), fetch, nws_workers, nws_per_refresh)]


# ---------------------------------------------------------------------------------------------- the merged table

FIELDS = ["id", "t", "s", "g", "d"]


def build_latest(stations, lists, now):
    """The page's table of latest readings. lists: {src: the provider's snapshot list (lean entries), or None for a
    provider with no list yet (it contributes nothing; the route names it as partial)}. rows: [id, t, s, g, d] per
    station with a reading, in id order; a gauge without a CO-OPS reading takes its NDBC relay's (alias_of).
    missing: the snapshot ids of the sources that HAVE a list (even an empty one) but no reading now."""
    rows, have, relays = {}, set(), {}
    listed = {src for src, lst in lists.items() if lst is not None}
    for lst in lists.values():
        for e in lst or []:
            w = e.get("wind")
            t = buoy_sources._z_epoch({"time_utc": e.get("latest_time")})
            if not isinstance(w, (list, tuple)) or len(w) != 3 or t is None:
                continue
            if e.get("alias_of"):
                relays[e["alias_of"]] = (int(t), w)
                continue
            sid = e["id"]
            if sid in stations:
                rows[sid] = [sid, int(t), w[0], w[1], w[2]]
                have.add(sid)
    for sid, (t, w) in relays.items():
        if sid in stations and sid not in rows:
            rows[sid] = [sid, t, w[0], w[1], w[2]]
            have.add(sid)
    missing = sorted(sid for sid in stations if sid not in have and sid.split(":", 1)[0] in listed)
    return {"now": int(now), "stale_s": STALE_S, "fields": list(FIELDS), "units": {"s": "m/s", "g": "m/s", "d": "deg"},
            "rows": [rows[k] for k in sorted(rows)], "missing": missing}


# ---------------------------------------------------------------------------------------------- history

class TokenBucket:
    """`rate` tokens a minute, at most `rate` held: take() is True when one was available."""

    def __init__(self, rate=METAR_BUCKET_PER_MIN, now=time.monotonic):
        self.rate, self._now = float(rate), now
        self._tokens = float(rate)
        self._at = now()
        self._lock = threading.Lock()

    def take(self):
        with self._lock:
            t = self._now()
            self._tokens = min(self.rate, self._tokens + (t - self._at) * self.rate / 60.0)
            self._at = t
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


_SERVICES = weakref.WeakSet()


class WindHistory:
    """A station's readings of the last 24 hours, cached, one upstream request per station at a time (the tide
    service's core: answers are (status, payload) with status "ok", "final", "busy" or "error"; "busy" is never
    cached). fetch(url, max_bytes, headers=None) -> bytes raises on failure; now() -> epoch seconds."""

    def __init__(self, stations, fetch, now=time.time, max_entries=256, ok_ttl=600, error_ttl=60, builds=2,
                 wait_s=2.0, bucket=None, nws_bucket=None):
        self.stations = stations
        self._fetch = fetch
        self._now = now
        self.max_entries = max_entries
        self.ok_ttl, self.error_ttl = ok_ttl, error_ttl
        self._builds_n, self.wait_s = builds, wait_s
        self.bucket = bucket or TokenBucket()
        self.nws_bucket = nws_bucket or TokenBucket(NWS_BUCKET_PER_MIN)
        self._reset_locks()
        self._cache = OrderedDict()           # key -> (expires, status, payload)
        _SERVICES.add(self)

    def _reset_locks(self):
        self._lock = threading.Lock()
        self._inflight = {}
        self._builds = threading.BoundedSemaphore(self._builds_n)

    # -- cache
    def _get(self, key):
        with self._lock:
            hit = self._cache.get(key)
            if hit is None:
                return None
            if hit[0] <= self._now():
                del self._cache[key]
                return None
            self._cache.move_to_end(key)
            return hit[1], hit[2]

    def _put(self, key, status, payload, ttl):
        with self._lock:
            self._cache[key] = (self._now() + ttl, status, payload)
            self._cache.move_to_end(key)
            while len(self._cache) > self.max_entries:
                self._cache.popitem(last=False)

    def entries(self):
        with self._lock:
            return len(self._cache)

    def _key_lock(self, key):
        with self._lock:
            e = self._inflight.get(key)
            if e is None:
                e = self._inflight[key] = [threading.Lock(), 0]
            e[1] += 1
            return e

    def _drop_key(self, key, e):
        with self._lock:
            e[1] -= 1
            if e[1] <= 0 and self._inflight.get(key) is e:
                del self._inflight[key]

    def _answer(self, key, build):
        """Cached answer for key, else one build at a time (others wait up to wait_s, then 'busy')."""
        hit = self._get(key)
        if hit is not None:
            return hit
        deadline = time.monotonic() + self.wait_s
        e = self._key_lock(key)
        try:
            if not e[0].acquire(timeout=self.wait_s):
                return "busy", {"error": BUSY}
            try:
                hit = self._get(key)
                if hit is not None:
                    return hit
                if not self._builds.acquire(timeout=max(0.0, deadline - time.monotonic())):
                    return "busy", {"error": BUSY}
                try:
                    status, payload = build()
                except WindBusy:
                    return "busy", {"error": BUSY}                       # never cached: asked again in a moment
                except WindError as exc:
                    _log.info("wind history %s: %s", key, exc)
                    status, payload = "error", {"error": UNAVAILABLE}
                finally:
                    self._builds.release()
                self._put(key, status, payload, self.ok_ttl if status == "ok" else self.error_ttl)
                return status, payload
            finally:
                e[0].release()
        finally:
            self._drop_key(key, e)

    # -- the feeds
    def _body(self, url, max_bytes):
        try:
            return self._fetch(url, max_bytes, HEADERS)
        except Exception as exc:
            raise WindError(str(exc) or "fetch failed") from None

    def _json(self, url, max_bytes):
        try:
            return json.loads(self._body(url, max_bytes))
        except ValueError:
            raise WindError("the answer is not JSON") from None

    def _ndbc(self, local, since):
        try:
            text = self._body(NDBC_RT2_URL % local, NDBC_RT2_MAX).decode("utf-8", "replace")
        except WindError as exc:
            if "HTTP 404" in str(exc):                                 # no realtime2 file (8 Korean buoys, step 5 F4)
                raise WindNoFile(str(exc)) from None
            raise
        return parse_realtime2(text, since)

    def _coops(self, local, since):
        doc = self._json(coops_url(local, range=24), COOPS_MAX)
        if coops_error(doc):
            return []                                                  # NOAA has nothing for the last 24 h
        return [r for r in parse_coops_wind(doc) if r["t"] >= since]

    def _coops_weather(self, local, letters, rows):
        """The gauge's own weather products (COOPS_WX_PRODUCTS, only the letters the snapshot lists) merged into the
        wind rows at the same minute; a product that cannot be read or has no data adds nothing (logged)."""
        by_t = {r["t"]: r for r in rows}
        for letter in str(letters or ""):
            prod = COOPS_WX_PRODUCTS.get(letter)
            if not prod or not by_t:
                continue
            product, key = prod
            try:
                doc = self._json(coops_url(local, product=product, range=24), COOPS_MAX)
            except WindError as exc:
                _log.info("wind history coops:%s: %s not read (%s)", local, product, exc)
                continue
            if coops_error(doc):
                continue
            check = _temp if key in ("at", "wt") else _hpa if key == "p" else _pct
            for t, v in parse_coops_series(doc).items():
                r = by_t.get(t)
                if r is not None and check(v) is not None:
                    r[key] = check(v)

    def _metar(self, local, since):
        if not self.bucket.take():
            raise WindBusy("the METAR request budget is used up")
        doc = self._json(METAR_API_URL % local, METAR_API_MAX)
        return [r for r in parse_metar_history(doc) if r["t"] >= since]

    def _nws(self, local, since):
        if not self.nws_bucket.take():
            raise WindBusy("the NWS API request budget is used up")
        try:
            doc = json.loads(self._fetch(NWS_OBS_URL % (local, iso_z(since), NWS_HISTORY_LIMIT), NWS_HISTORY_MAX, NWS_HEADERS))
        except ValueError:
            raise WindError("the answer is not JSON") from None
        except Exception as exc:
            raise WindError(str(exc) or "fetch failed") from None
        return [r for r in parse_nws_observations(doc) if r["t"] >= since]

    def history(self, sid):
        st = self.stations.get(sid) if valid_id(sid) else None
        if st is None:
            return "final", {"id": sid, "error": "Unknown wind station"}
        return self._answer(("h", sid), lambda: self._build(sid, st))

    def _build(self, sid, st):
        now = self._now()
        since = int(now) - HISTORY_S
        src, _, local = sid.partition(":")
        note, via, no_file = None, None, False
        if src == "ndbc":
            try:
                rows = self._ndbc(local, since)
            except WindNoFile:
                rows, no_file = [], True
        elif src == "metar":
            rows = self._metar(local, since)
        elif src == "nws":
            rows = self._nws(local, since)
        else:
            try:
                rows = self._coops(local, since)
                self._coops_weather(local, st.get("wx"), rows)
            except WindError as exc:
                if not st.get("alias"):
                    raise
                _log.info("wind history %s: CO-OPS did not answer (%s); NDBC's relay %s instead", sid, exc, st["alias"])
                via = "ndbc"
                try:
                    rows = self._ndbc(st["alias"], since)              # NDBC's relay of the gauge
                except WindNoFile:
                    rows, no_file = [], True
        rows = sorted({r["t"]: r for r in rows}.values(), key=lambda r: r["t"])[-HISTORY_ROWS:]
        if not rows:
            note = NDBC_NO_FILE if no_file else NO_HISTORY
        for r in rows:                                                 # humidity from the dew point where not reported
            if r.get("rh") is None and r.get("at") is not None and r.get("dp") is not None:
                r["rh"] = humidity_from(r["at"], r["dp"])
        out = {"id": sid, "name": st.get("name"), "kind": st.get("kind"), "src": src, "tz": st.get("tz"),
               "alias": st.get("alias"), "lat": st.get("lat"), "lon": st.get("lon"), "hours": HISTORY_S // 3600,
               "units": {"s": "m/s", "g": "m/s", "d": "deg", "at": "degC", "wt": "degC", "dp": "degC", "rh": "%",
                         "p": "hPa", "pt": "hPa/3h", "vis": "km"}, "stale_s": STALE_S, "now": int(now),
               "t": [r["t"] for r in rows], "s": [r["s"] for r in rows], "g": [r["g"] for r in rows],
               "d": [r["d"] for r in rows], "source": ATTRIBUTION[via or src], "via": via, "note": note}
        for k in WX_FIELDS:                                            # only the fields some row carries
            if any(r.get(k) is not None for r in rows):
                out[k] = [r.get(k) for r in rows]
        out["conditions"] = conditions(rows)
        return "ok", out


def conditions(rows):
    """The current conditions from history rows (ascending): for each WX field the newest value within CONDITIONS_S
    of the newest row, as [t, value]; "trend": the pressure's change over TREND_S in hPa (the newest row's pt, else
    the newest pressure minus the one closest to TREND_S earlier, within half an hour of it), or None."""
    out = {}
    if not rows:
        return out
    newest = rows[-1]["t"]
    for k in WX_FIELDS:
        for r in reversed(rows):
            if r["t"] < newest - CONDITIONS_S:
                break
            if r.get(k) is not None:
                out[k] = [r["t"], r[k]]
                break
    trend = None
    if out.get("pt") is not None:
        trend = out["pt"][1]
    elif out.get("p") is not None:
        t_now, p_now = out["p"]
        best = None
        for r in rows:
            if r.get("p") is None:
                continue
            off = abs((t_now - r["t"]) - TREND_S)
            if off <= 1800 and (best is None or off < best[0]):
                best = (off, r["p"])
        if best is not None:
            trend = round(p_now - best[1], 1)
    out["trend"] = trend
    return out


def _after_fork():
    for svc in list(_SERVICES):
        svc._reset_locks()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)
