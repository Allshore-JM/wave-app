"""Live wind stations (plan section 39): the latest wind reading of every station on the map's wind layer and a
station's last 24 hours, from five public-domain NOAA / NWS feeds.

The stations are a committed snapshot (wind_stations.json, built by tools/wind/fetch_stations.py): ids "coops:1612340"
(a NOAA tide gauge's weather sensors), "ndbc:51003" (NDBC buoys, C-MAN and other fixed stations), "metar:PHNL"
(airports and offshore platforms within 30 km of a coast or at sea) and "nws:001HE" (the land weather stations the
NWS API lists for Hawaii: HECO / HELCO / MECO, the University of Hawaii, RAWS, CWOP, HADS and others, step 5c). A
reading is {t: epoch seconds, s: speed m/s, g: gust m/s or None, d: direction the wind blows FROM in degrees true,
or None when calm or variable}; a HISTORY row may also carry the weather the same report holds (WX_FIELDS: at / wt air
and water temperature C, dp dew point C, rh humidity %, p pressure hPa, pt its 3-hour change hPa, vis visibility km,
wx weather words), step 5d: the window shows the current conditions and a 24-hour temperature chart.

The latest readings come from five BuoyProvider subclasses (buoy_sources: the section-36 stale-while-revalidate
lists, keep-last on a failed fetch, the shared refresh runner and the fork reset) kept in a list of their OWN
(app.get_wind_providers): never in the live-buoy list, so the live memo, route and golden stay as they are.
  NDBC   one GET of data/latest_obs/latest_obs.txt (every station with a wind speed now; ~5-minute file)
  METAR  one GET of aviationweather.gov's metars.cache.csv.gz (the last ~80 minutes of reports, whole knots)
  COOPS  one datagetter request per gauge (product=wind&date=latest, 6-minute readings), a few at a time, PACED
  NWS    one api.weather.gov request per station (its newest observations of the last 2 hours), PACED the same way
  MADIS  one GET of NOAA MADIS's public surface viewer (every report of every Hawaii station received in the last hour):
         the NWS stations' BACKUP (plan section 52 D; the NWS API's ingest stalled nationwide on 2026-10-22 while MADIS,
         its upstream, kept the data). Its entries are relays (alias_of "nws:<id>"): the merged table takes the newer
         of a station's NWS reading and its MADIS one, as it takes a gauge's NDBC relay.
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
from array import array
from bisect import bisect_left
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait
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
                      "other networks via MADIS",
               "madis": "NOAA MADIS (madis.ncep.noaa.gov): HECO / HELCO / MECO, University of Hawaii, RAWS, CWOP and "
                        "other networks",
               "nws+madis": "NWS API observations (api.weather.gov) and NOAA MADIS (madis.ncep.noaa.gov): HECO / "
                            "HELCO / MECO, University of Hawaii, RAWS, CWOP and other networks",
               # plan section 67 (the feeds and their licences: wind_more.SOURCES)
               "eccc": "Environment and Climate Change Canada (dd.weather.gc.ca, SWOB-ML); ECCC Data Servers End-use "
                       "Licence",
               "jma": "Japan Meteorological Agency website (www.jma.go.jp, AMeDAS), converted and thinned by "
                      "allshoresurf.com; Public Data License 1.0",
               "iem": "Iowa Environmental Mesonet, Iowa State University (mesonet.agron.iastate.edu): state DOT road "
                      "weather and airport stations",
               "smhi": "SMHI, Swedish Meteorological and Hydrological Institute (opendata.smhi.se), CC BY 4.0, units converted by allshoresurf.com",
               "fmi": "Finnish Meteorological Institute (opendata.fmi.fi), CC BY 4.0, units converted by allshoresurf.com",
               "dmi": "DMI, Danish Meteorological Institute (opendataapi.dmi.dk), CC BY 4.0, units converted by allshoresurf.com",
               "dwd": "Deutscher Wetterdienst (opendata.dwd.de), CC BY 4.0, units converted by allshoresurf.com",
               "ipma": "IPMA, Instituto Portugues do Mar e da Atmosfera (api.ipma.pt), units converted by "
                       "allshoresurf.com",
               "mei": "Met Eireann (met.ie), CC BY 4.0, units converted by allshoresurf.com"}
CC_BY = "https://creativecommons.org/licenses/by/4.0/"
# the licence text a window's source line links to (G44 P3-9: CC BY 4.0 asks for a link to the licence)
LICENCE_URL = {"smhi": CC_BY, "fmi": CC_BY, "dmi": CC_BY, "dwd": CC_BY, "mei": CC_BY,
               "jma": "https://www.jma.go.jp/jma/en/copyright.html",
               "eccc": "https://eccc-msc.github.io/open-data/licence/readme_en/"}
MORE_SOURCES = ("eccc", "jma", "iem", "smhi", "fmi", "dmi", "dwd", "ipma", "mei")   # plan section 67 (wind_more.py)
MORE_KEPT = ("eccc", "ipma", "mei")             # their windows' 24 hours are the feed's own, kept in memory
MORE_RESTART_NOTE = "Earlier hours are not kept after a server restart."
MORE_RESTART_EMPTY = "No readings since the server restarted; earlier hours are not kept after a restart."
# NOAA's restrictions page for MADIS asks pages that show mesonet observations to carry an accuracy disclaimer: shown in
# the window of every NWS-network station (HECO / UH / RAWS / CWOP ...), whichever feed brought the reading (G35).
MESONET_DISCLAIMER = ("NOAA and its data providers disclaim liability of any kind whatsoever, including, without "
                      "limitation, liability for quality, performance, merchantability and fitness for a particular "
                      "purpose arising out of the use or inability to use the data.")
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
COOPS_ON_DEMAND_PER_MIN = 30                   # CO-OPS requests a minute for windows (wind histories AND tide windows share
COOPS_ON_DEMAND_BURST = 15                     # it: G28 A-1); at most this many at once. The feed's own 24 a minute come on top
NDBC_HISTORY_PER_MIN = 30                      # NDBC realtime2 files (0.2-1 MB each) for the windows
WX_DEADLINE_S = 10.0                           # a gauge's weather products: one deadline for all of them (asked together, A-6)
NDBC_KEEP_S = 6 * 3600                         # an NDBC reading kept this long after its station's newest line had no wind (B-4)
WIND_REFRESH_WORKERS = 2                       # the wind feeds' own refresh threads (never the live buoys' workers: A-5)
NWS_QC_REJECTED = ("X", "B")                   # MADIS quality control: rejected / subjectively bad values are dropped
# NOAA MADIS's public surface viewer (no account: the "public" categories, which hold every provider the NWS stations
# come from): EVERY record of the Hawaii stations RECEIVED in the last 59 minutes (timefilter=1: receipt time; recwin=4:
# all records, not the newest per station), as CSV in an HTML <PRE>; ~290 KB, 4-40 s. Receipt time catches MADIS's
# 15-60 minute latency (the records reach ~2 h back in observation time), so polls 5 minutes apart miss nothing (55).
MADIS_HI_URL = ("https://madis-data.ncep.noaa.gov/madisPublic1/cgi-bin/madisXmlPublicDir?rdr=&time=0&minbck=-59"
                "&minfwd=0&recwin=4&timefilter=1&dfltrsel=2&state=HI&latll=0.0&lonll=0.0&latur=90.0&lonur=0.0&stanam="
                "&stasel=0&pvdrsel=0&varsel=2&qctype=0&qcsel=0&xml=5&csvmiss=0")
MADIS_MAX = 2 * 1024 * 1024
MADIS_TTL_S = 300                              # the live poll every 5 minutes (one request)
MADIS_KEEP_S = 6 * 3600                        # a MADIS reading kept this long (a station missing from one answer keeps it)
MADIS_MISSING = -99999.0
# The 24-hour history (plan section 55): every reading of the snapshot's NWS stations the live polls and the back-fill
# bring is kept MADIS_HIST_S (the window's 24 h and an hour's margin). After a start (and after a gap in the live polls
# longer than MADIS_GAP_S) the hours the server has not seen are asked by OBSERVATION time, MADIS_WINDOW_S at a time
# (time=YYYYMMDD_HH00&minbck=-119 returns the records observed in (T - 2 h, T]: two hourly files), newest window first,
# ONE request a refresh, a refresh due MADIS_BACKFILL_TTL_S after the last one ENDED while windows remain (the live
# poll keeps its 5 minutes); a window that fails MADIS_BACKFILL_TRIES times is given up (its hours stay missing,
# unless it was given up while the live poll failed too: then it is asked again once the live poll answers).
MADIS_HIST_S = HISTORY_S + 3600
MADIS_WINDOW_S = 2 * 3600
MADIS_BACKFILL_WINDOWS = HISTORY_S // MADIS_WINDOW_S
MADIS_BACKFILL_TTL_S = 15                      # the pause after a window, from its END (G38 P2-2)
MADIS_BACKFILL_RETRY_S = 120                   # the pause after a FAILED window
MADIS_BACKFILL_TRIES = 3
MADIS_LIVE_EVERY_S = MADIS_TTL_S * 0.9         # the live poll's turn (the base class's refresh_at of its 5 minutes)
MADIS_GAP_S = 50 * 60                          # live polls further apart than this: the hours between are asked again
MADIS_GAP_MARGIN_S = 2 * 3600                  # ... from this long before the last good poll (reports arrive up to ~70 min late)
MADIS_HIST_FIELDS = ("s", "g", "d", "at", "dp", "rh", "p")   # a history row's values, in the store's order
MADIS_HIST_INTS = ("d", "rh")
NWS_UNITS = {"wmoUnit:km_h-1": 1 / 3.6, "wmoUnit:m_s-1": 1.0, "wmoUnit:[kn_i]": KT_MS, "wmoUnit:mi_h-1": 0.44704}
NWS_TEMP_UNITS = {"wmoUnit:degC": (1.0, 0.0), "wmoUnit:degF": (5 / 9, -32 * 5 / 9), "wmoUnit:K": (1.0, -273.15)}
NWS_PRESSURE_UNITS = {"wmoUnit:Pa": 0.01, "wmoUnit:hPa": 1.0, "wmoUnit:mbar": 1.0}
NWS_LENGTH_UNITS = {"wmoUnit:m": 0.001, "wmoUnit:km": 1.0, "wmoUnit:[mi_i]": 1.609344}
WX_FIELDS = ("at", "wt", "dp", "rh", "p", "pt", "vis", "wx")   # the weather a history row may carry (see the module docstring)
WX_FLAGS = ("vg",)                             # vg = 1: the row's visibility is "or more" (METAR "10+", "6+": G28 A-3)
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
ID_RE = re.compile(r"(coops|ndbc|metar|nws):[A-Z0-9]{3,8}|(eccc|jma|iem|smhi|fmi|dmi|dwd|ipma|mei):[A-Z0-9][A-Z0-9_-]{1,15}")
COOPS_NO_DATA_RE = re.compile(r"no data was found", re.I)   # the only CO-OPS message that means "nothing for the station"
NO_NEW = object()                              # a paced station answered without a new reading: keep the one in hand
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
        if (k in WX_FIELDS or k in WX_FLAGS) and v is not None:
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


def coops_no_data(msg):
    """True when NOAA's message says it has no data for the station ("No data was found ..."). Any other message
    (a throttle, a hiccup) is a failure that may pass (G28 A-4; section 38 learned the same for the tides)."""
    return bool(msg) and bool(COOPS_NO_DATA_RE.search(str(msg)))


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


def metar_visibility(v):
    """visib in statute miles ("10+" / "6+" = that or more, a number, "1/2") -> (km, more) or (None, False). "6+" is how
    the API writes the international 9999 (10 km or more): 9.7 km and "or more", never a made-up 16 km (G28 A-3)."""
    text = str(v if v is not None else "").strip()
    more = text.endswith("+")
    km = metar_visibility_km(text.rstrip("+"))
    return (km, bool(more)) if km is not None else (None, False)


def metar_visibility_km(v):
    """visib in statute miles (a number, "1/2"; a trailing "+" ignored) -> km, or None."""
    text = str(v if v is not None else "").strip()
    if not text:
        return None
    text = text.rstrip("+")
    try:
        if "/" in text:
            a, b = text.split("/", 1)
            miles = float(a) / float(b)
        else:
            miles = float(text)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    return _km(miles * SM_KM)


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
        if desc or phen:                                               # "+" / "VC" alone say nothing
            groups.append(" ".join(words))
    if groups:
        return _words(", ".join(groups))
    return METAR_COVER.get(str(cover or "").strip().upper())


def parse_metar_history(doc):
    """api/data/metar (format=json) -> readings in the order given. obsTime is epoch seconds; wdir an integer or
    "VRB"; wspd / wgst whole knots (wgst absent without a gust); temp / dewp degC; altim (else slp) hPa: one quantity
    for every row, so the 3-hour trend compares like with like (US SPECIs carry no slp: G28 A-8); visib statute miles
    (vg = 1 for "or more"); wxString + cover -> weather words."""
    out = []
    for row in doc if isinstance(doc, list) else []:
        if not isinstance(row, dict):
            continue
        r = _metar_reading(row.get("obsTime"), row.get("wdir"), row.get("wspd"), row.get("wgst"))
        if r is not None:
            vis, more = metar_visibility(row.get("visib"))
            with_weather(r, at=_temp(row.get("temp")), dp=_temp(row.get("dewp")),
                         p=_hpa(row.get("altim")) if _hpa(row.get("altim")) is not None else _hpa(row.get("slp")),
                         vis=vis, vg=1 if more and vis is not None else None,
                         wx=metar_weather_words(row.get("wxString"), row.get("cover")))
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

def parse_madis_rows(text, now=None, stats=None):
    """MADIS's surface viewer answer (xml=5: CSV lines in an HTML <PRE>) -> {station id: [readings with weather,
    ascending, one per observation time]}. The columns are found by the header line's names (STAID, OBDATE MM/DD/YYYY,
    OBTIME HH:MM UTC, then each variable followed by its QC descriptor: TD dew point K, RH %, T air K, DD direction,
    FF / FFGUST m/s, ALTSE altimeter Pa); -99999 is missing; a value whose QC is X or B (rejected) is dropped. A
    station's two records of one time keep the later line; a record more than 10 minutes in the future is dropped.
    stats (a dict, optional) gets "rows": the station records read (a valid time and id) and "wind": the stations
    with a wind reading. Raises ValueError when the answer holds no header (an error page, a changed format)."""
    head, out, rows = None, {}, 0
    for raw in text.splitlines():
        line = re.sub(r"<[^>]*>", "", raw).strip()
        if not line or "," not in line:
            continue
        cells = [c.strip() for c in line.split(",")]
        if head is None:
            if cells[0] == "STAID":
                head = {}
                for i, name in enumerate(cells):
                    if name and name != "D" and name not in head:
                        head[name] = i
                if not all(k in head for k in ("STAID", "OBDATE", "OBTIME", "FF")):
                    raise ValueError("MADIS: the header lacks STAID / OBDATE / OBTIME / FF")
            continue
        try:
            t = calendar.timegm(datetime.strptime(cells[head["OBDATE"]] + " " + cells[head["OBTIME"]], "%m/%d/%Y %H:%M").timetuple())
        except (ValueError, IndexError):
            continue
        if now is not None and t > now + 600:
            continue

        def val(name):
            i = head.get(name)
            if i is None or i + 1 >= len(cells):
                return None
            try:
                v = float(cells[i])
            except ValueError:
                return None
            if v <= MADIS_MISSING or not math.isfinite(v) or cells[i + 1] in NWS_QC_REJECTED:
                return None
            return v
        sid = cells[head["STAID"]].upper()
        if not re.fullmatch(r"[A-Z0-9]{3,8}", sid):
            continue
        rows += 1
        r = reading(t, val("FF"), val("FFGUST"), val("DD"))
        if r is None:
            continue
        k = lambda v: None if v is None else v - 273.15
        alt = val("ALTSE")
        at, dp = _temp(k(val("T"))), _temp(k(val("TD")))
        r = with_weather(r, at=at, dp=dp, rh=_pct(val("RH")), p=_hpa(alt / 100.0 if alt is not None else None))
        out.setdefault(sid, {})[r["t"]] = r
    if head is None:
        raise ValueError("MADIS: no table in the answer")
    if stats is not None:
        stats.update(rows=rows, wind=len(out))
    return {sid: [by_t[t] for t in sorted(by_t)] for sid, by_t in out.items()}


def parse_madis_csv(text, now=None, stats=None):
    """parse_madis_rows' newest reading of each station: {station id: reading with weather}."""
    return {sid: rows[-1] for sid, rows in parse_madis_rows(text, now, stats).items()}


def madis_window_url(url, end):
    """The viewer's url (any of its queries) asking for the records OBSERVED in (end - 2 h, end] (end: epoch seconds,
    on the hour): time=YYYYMMDD_HH00, minbck=-119, minfwd=0, recwin=4 (all records), timefilter=0 (observation time).
    Every other parameter of url is kept (the state, the variables, the CSV form)."""
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
    parts = urlsplit(url)
    q = parse_qsl(parts.query, keep_blank_values=True)
    want = {"time": time.strftime("%Y%m%d_%H%M", time.gmtime(end)), "minbck": "-119", "minfwd": "0",
            "recwin": "4", "timefilter": "0"}
    seen, out = set(), []
    for k, v in q:
        if k in want:
            if k in seen:
                continue
            v = want[k]
        seen.add(k)
        out.append((k, v))
    out += [(k, v) for k, v in want.items() if k not in seen]
    return urlunsplit(parts._replace(query=urlencode(out, safe="_")))


class MadisSeries:
    """One station's readings in two arrays, ascending by time (t: epoch seconds as doubles; v: MADIS_HIST_FIELDS per
    reading, NaN for a missing value): ~64 bytes a reading, so the 24 hours of ~300 stations stay a few MB."""
    __slots__ = ("t", "v")
    N = len(MADIS_HIST_FIELDS)

    def __init__(self):
        self.t = array("d")
        self.v = array("d")

    def __len__(self):
        return len(self.t)

    def add(self, r):
        """A reading in its place; a reading of a time already held replaces it (a corrected report)."""
        t = float(r["t"])
        vals = array("d", [math.nan if r.get(k) is None else float(r[k]) for k in MADIS_HIST_FIELDS])
        i = bisect_left(self.t, t)
        n = self.N
        if i < len(self.t) and self.t[i] == t:
            self.v[i * n:(i + 1) * n] = vals
            return False
        self.t.insert(i, t)
        self.v[i * n:i * n] = vals
        return True

    def prune(self, cut):
        """Drops the readings older than cut; returns how many."""
        i = bisect_left(self.t, float(cut))
        if i:
            del self.t[:i]
            del self.v[:i * self.N]
        return i

    def rows(self, since):
        """The readings at or after since, as reading dicts (the weather fields only where held)."""
        out, n = [], self.N
        for i in range(bisect_left(self.t, float(since)), len(self.t)):
            r = {"t": int(self.t[i])}
            for j, k in enumerate(MADIS_HIST_FIELDS):
                x = self.v[i * n + j]
                if math.isnan(x):
                    if k in ("s", "g", "d"):
                        r[k] = None
                    continue
                r[k] = int(x) if k in MADIS_HIST_INTS else x
            out.append(r)
        return out


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

    def __init__(self, stations, fetch):
        super().__init__(http=None)
        self.stations = dict(stations)          # local id -> snapshot station
        self._fetch = fetch

    def refresh_runner(self):
        return get_wind_runner()                # never the live buoys' workers (G28 A-5)

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

    def __init__(self, stations, fetch, aliases=None):
        super().__init__(stations, fetch)
        self.aliases = dict(aliases or {})      # NDBC id -> the gauge's snapshot station (id "coops:1612340")
        self._kept = {}                         # local id -> (the latest wind reading, alias extra or None)

    def _fetch_stations(self):
        """latest_obs.txt holds ONE line per station, its newest report; an hourly buoy's newest is often its wave-only
        line ("MM" wind), so each station keeps its last wind reading until a newer one comes or it is NDBC_KEEP_S
        old (it is drawn grey past STALE_S like any old reading): G28 B-4, the buoys read "no reading" up to 45 % of
        every hour before."""
        body = self._fetch(NDBC_LATEST_URL, NDBC_LATEST_MAX, HEADERS)
        obs = parse_latest_obs(body.decode("utf-8", "replace"))
        for local, r in obs.items():
            old = self._kept.get(local)
            if old is not None and old[0]["t"] > r["t"]:
                continue
            if local in self.stations:
                self._kept[local] = (r, None)
            elif local in self.aliases:
                g = self.aliases[local]
                self._kept[local] = (r, {"alias_of": g["id"], "name": g.get("name") or local,
                                         "lat": g.get("lat"), "lon": g.get("lon")})
        cut = buoy_sources.time.time() - NDBC_KEEP_S
        for local in [k for k, (r, _) in self._kept.items() if r["t"] < cut]:
            del self._kept[local]
        return [self._entry(local, r, extra) for local, (r, extra) in sorted(self._kept.items())]


class MetarProvider(WindProvider):
    """Every snapshot airport with a wind speed in the METAR cache (the last ~80 minutes of reports)."""
    source = "METAR"
    source_name = "NWS Aviation Weather Center"
    source_url = "https://aviationweather.gov"
    attribution_text = "Source: " + ATTRIBUTION["metar"]
    list_ttl_sec = 300

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
    (HTTP 403 / 429) stops the rest of the slice before it is sent (G28 A-2: those stations are asked first after the
    pause) and pauses the feed for pause_s (the kept readings stay). Subclasses give the codes, the messages and
    _one(local) -> (local, reading / None (no data: dropped) / NO_NEW (keep the one in hand), error text or None)."""
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
        self._newest_t = None                   # the newest reading the feed ever brought (kept past keep_s: the stall note)

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
        failures, last, refused, skipped = 0, None, None, []
        stop = threading.Event()

        def ask(local):
            if stop.is_set():                                          # refused: the rest of the slice is not sent
                return local, None, None, True
            local, r, err = self._one(local)
            if err and self._refused(err):
                stop.set()
            return local, r, err, False

        with ThreadPoolExecutor(self.workers) as pool:
            for local, r, err, skip in pool.map(ask, ids):
                if skip:
                    skipped.append(local)
                elif err:
                    failures += 1
                    last = err
                    if self._refused(err):
                        refused = err
                elif r is NO_NEW:
                    pass                                               # nothing newer: the reading in hand stays
                elif r is None:
                    self._readings.pop(local, None)                    # the agency: no data for the station now
                else:
                    self._readings[local] = r
                    if self._newest_t is None or r["t"] > self._newest_t:
                        self._newest_t = r["t"]
        if refused:
            if skipped:
                self._cursor = self._ids.index(skipped[0])             # the stations not asked go first after the pause
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
                 paused_s=round(max(0.0, self._paused_until - now), 1),
                 newest_age_s=round(now - self._newest_t) if self._newest_t is not None else None)
        return s

    def newest_t(self):
        """The newest reading's time this feed ever brought (epoch s), None before the first."""
        return self._newest_t


class CoopsWindProvider(PacedProvider):
    """The snapshot gauges' latest 6-minute readings, one datagetter request per gauge, paced (PacedProvider):
    NOAA answered HTTP 403 to everything from the server for about a minute after 232 requests in a few seconds
    (step 5, F1: new tide windows failed then too); 24 gauges a minute asks every gauge about every 10 minutes."""
    source = "COOPS"
    source_name = "NOAA CO-OPS"
    source_url = "https://tidesandcurrents.noaa.gov"
    attribution_text = "Source: " + ATTRIBUTION["coops"]
    list_ttl_sec = COOPS_TTL_S
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
        msg = coops_error(doc)
        if msg:
            if coops_no_data(msg):
                return local, None, None                               # NOAA has no reading for the gauge now
            return local, None, "NOAA: %s" % msg[:120]                 # a passing message: the reading in hand stays
        rows = parse_coops_wind(doc)
        return local, (max(rows, key=lambda r: r["t"]) if rows else None), None


class NwsProvider(PacedProvider):
    """The NWS API's land weather stations (step 5c; Hawaii first): one observations request per station for its
    newest NWS_FEED_LIMIT rows of the last NWS_FEED_WINDOW_S, paced like the CO-OPS feed (the API's rate limit is
    not published; it answers 403 or 429 to abuse). The newest row with a wind speed is the reading; an answer
    without one (no wind in the last 2 hours, or a station the API no longer knows) keeps the reading in hand until
    it is keep_s old (owner decision 12)."""
    source = "NWS"
    source_name = "NWS API"
    source_url = "https://api.weather.gov"
    attribution_text = "Source: " + ATTRIBUTION["nws"]
    list_ttl_sec = NWS_TTL_S
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
        return local, (max(rows, key=lambda r: r["t"]) if rows else NO_NEW), None


class MadisProvider(WindProvider):
    """The NWS stations' backup (plan section 52 D) and their 24 hours (plan section 55). The live poll (every 5 min):
    one GET of NOAA MADIS's public surface viewer, every report of the state received in the last hour; the snapshot's
    NWS stations among them become relay entries (alias_of "nws:<id>"), so the merged table shows the newer of a
    station's NWS API reading and its MADIS one. Each station keeps its last MADIS reading until a newer one comes or it
    is MADIS_KEEP_S old; the weather of the report (air, dew point, humidity, altimeter) is kept for the station's window
    (row()). Every reading also goes into the station's history (hist()), kept MADIS_HIST_S. With backfill, the hours
    not yet seen (24 h after a start; the hours between two live polls further apart than MADIS_GAP_S) are asked by
    observation time, one 2-hour window a refresh (refresh_due: MADIS_BACKFILL_TTL_S after the last refresh ENDED while
    windows remain, MADIS_BACKFILL_RETRY_S after a failed window), newest first. A failed or unreadable LIVE answer
    raises (keep-last); a window that fails (or answers with no station record or no wind reading, MADIS's known bad
    answers) is tried again, MADIS_BACKFILL_TRIES times in all, then given up; a window given up while the live poll
    was failing too is asked again once the live poll answers. A window's refresh is not the feed's refresh: it never
    clears the live poll's error, never counts as a good fetch, and publishes a new list version only when the list
    changed (G38 P3-1). fetch: a slower one than the other feeds' (the viewer takes 4-40 s to answer); window_fetch
    (default fetch): slower still for the windows, which MADIS answers only when its query is done (20-55 s seen; on
    the test site 2026-10-26 two tries were cut by the live fetch's 45-s read timeout)."""
    source = "MADIS"
    source_name = "NOAA MADIS"
    source_url = "https://madis.ncep.noaa.gov"
    attribution_text = "Source: " + ATTRIBUTION["madis"]
    list_ttl_sec = MADIS_TTL_S

    def __init__(self, stations, fetch, url=MADIS_HI_URL, backfill=False, window_fetch=None):
        super().__init__(stations, fetch)
        self.url = url
        self.backfill = backfill
        self._wfetch = window_fetch or fetch    # the back-fill windows' fetch (a longer read timeout)
        self._kept = {}                         # local id -> the latest reading (with weather)
        self._newest_t = None                   # the newest reading MADIS ever brought (G35 F5: the stall rule, /healthz)
        self._last_rows = self._last_wind = None   # the last answer's station records / those with a wind reading
        self._hist_lock = threading.Lock()      # the history is read by request threads (WindHistory) while a refresh writes
        self._hist = {}                         # local id -> MadisSeries
        self._bf = []                           # back-fill windows still to ask: [end epoch, tries], newest first
        self._bf_queued = self._bf_done = self._bf_gave_up = 0
        self._bf_next = 0.0                     # when the next window's refresh is due
        self._lost = set()                      # windows given up while the live poll failed too: asked again later
        self._live_tried = None                 # the last live poll asked (any outcome)
        self._live_ok = None                    # the last live poll that answered with wind (or the start's queue)
        self._live_bad = False                  # the last live poll failed or brought no wind
        self._turn = None                       # "live" / "window" while _fetch_and_publish runs (None: decide)

    def _reset_after_fork(self):
        super()._reset_after_fork()
        self._hist_lock = threading.Lock()      # a forked child has none of the parent's threads (G38 P3-4)

    # -- the refresh (on the wind runner, under the base class's singleflight)
    def _live_turn(self, now):
        """The live poll's turn: always without back-fill, before the first poll, with no window left, and when
        MADIS_LIVE_EVERY_S passed since the last live poll was asked."""
        return (not self.backfill or not self._bf or self._live_tried is None
                or now - self._live_tried >= MADIS_LIVE_EVERY_S)

    def _fetch_and_publish(self):
        """The live poll's refresh is the base class's (an error, a good fetch, the retry rule). A window's refresh is
        not (G38 P3-1): it keeps the live poll's error and good age as they are and publishes a new version only when
        the list changed (a window reading can be a station's newest; a reading kept 6 h can expire)."""
        if self._live_turn(buoy_sources.time.time()):
            self._turn = "live"
            try:
                return super()._fetch_and_publish()
            finally:
                self._turn = None
        self._turn = "window"
        with self._lock:
            self._refreshing = True
            self._fetch_started_ts = buoy_sources.time.time()
        try:
            out, ok, _ = self._build_station_list()
        finally:
            self._turn = None
            with self._lock:
                self._refreshing = False
        with self._lock:
            if ok and self._list_cache is not None and out != self._list_cache:
                self._list_cache = out
                self._list_version += 1
                self._pub_ts = buoy_sources.time.time()
            return self._list_cache, self._list_version, self._list_ts

    def _fetch_stations(self):
        now = buoy_sources.time.time()
        pause = MADIS_BACKFILL_TTL_S
        try:
            if self.backfill and self._live_tried is None and not self._bf_queued:
                self._queue(now - HISTORY_S, now)                 # the 24 hours before the first poll
                # the queue covers the hours up to now: a first live poll that fails or brings no wind leaves the gap
                # rule measuring from here (G38 P2-3)
                self._live_ok = now
            if (self._turn or ("live" if self._live_turn(now) else "window")) == "live":
                self._live(now)
            elif not self._window(now):
                pause = MADIS_BACKFILL_RETRY_S
        finally:
            # the pause runs from the END of this refresh: a window takes 20-55 s (G38 P2-2)
            self._bf_next = buoy_sources.time.time() + pause
        self._prune(now)
        return [self._entry(local, r, {"alias_of": "nws:" + local}) for local, r in sorted(self._kept.items())]

    def _live(self, now):
        self._live_tried = now
        self._live_bad = True                   # until this poll brings wind
        body = self._fetch(self.url, MADIS_MAX, HEADERS)
        stats = {}
        obs = parse_madis_rows(body.decode("latin-1"), now, stats)
        self._last_rows, self._last_wind = stats.get("rows"), stats.get("wind")
        if not stats.get("rows"):
            # seen 2026-10-25 on the test site (re-check R1-2): the header and no station record at all; the kept
            # readings stay, but say so
            _log.warning("MADIS: an answer with no station records (the kept readings stay)")
        elif not stats.get("wind"):
            # seen 2026-10-24 (G35 F5): an answer whose every wind field is -99999; the kept readings stay, but say so
            _log.warning("MADIS: %d station records and not one wind reading (the kept readings stay)", stats["rows"])
        self._take(obs, now)
        if not stats.get("wind"):
            return                              # an answer with no wind counts as no answer for the gap rule (55)
        self._live_bad = False
        if self.backfill:
            if self._lost:
                # windows given up while the live poll failed too (MADIS down): asked again now that it answers
                self._add([e for e in self._lost if e > now - HISTORY_S])
                self._lost.clear()
            if self._live_ok is not None and now - self._live_ok > MADIS_GAP_S:
                # the live polls stopped for a while (failures, windless answers, a stalled runner): the hours between
                # are asked again, from MADIS_GAP_MARGIN_S before the last good poll (MADIS reports arrive up to ~70 min
                # late: G38 P3-3)
                self._queue(self._live_ok - MADIS_GAP_MARGIN_S, now)
        self._live_ok = now

    def _window(self, now):
        """The next window: True when it was read, False when it failed (tried again or given up)."""
        end, tries = self._bf[0]
        try:
            body = self._wfetch(madis_window_url(self.url, end), MADIS_MAX, HEADERS)
            stats = {}
            obs = parse_madis_rows(body.decode("latin-1"), now, stats)
            # MADIS's known bad answers are no answer for a window either (G38 P2-1)
            if not stats.get("rows"):
                raise ValueError("an answer with no station records")
            if not stats.get("wind"):
                raise ValueError("%d station records and not one wind reading" % stats["rows"])
        except Exception as e:
            self._bf[0][1] = tries = tries + 1
            if tries >= MADIS_BACKFILL_TRIES:
                self._bf.pop(0)
                self._bf_gave_up += 1
                if self._live_bad:
                    self._lost.add(end)
                _log.warning("MADIS: the window ending %s given up after %d tries (%s: %s)%s", iso_z(end), tries,
                             type(e).__name__, e, "; asked again once the live poll answers" if self._live_bad else "")
            else:
                _log.info("MADIS: the window ending %s failed (%s: %s); asked again", iso_z(end), type(e).__name__, e)
            return False
        self._bf.pop(0)
        self._bf_done += 1
        n = self._take(obs, now)
        _log.info("MADIS: the window ending %s read (%d new readings; %d windows left)", iso_z(end), n, len(self._bf))
        return True

    def _queue(self, start, end):
        """The 2-hour windows covering (start, end], ends on the hour, newest first, MADIS_BACKFILL_WINDOWS at most,
        merged into the queue (no window twice)."""
        top = int(end) // 3600 * 3600
        ends = []
        e = top
        while e > start and len(ends) < MADIS_BACKFILL_WINDOWS:
            ends.append(e)
            e -= MADIS_WINDOW_S
        self._add(ends)

    def _add(self, ends):
        """Window ends into the queue (no window twice), newest first."""
        have = {w[0] for w in self._bf}
        new = [[e, 0] for e in sorted(set(ends), reverse=True) if e not in have]
        self._bf = sorted(self._bf + new, key=lambda w: -w[0])
        self._bf_queued += len(new)

    def _take(self, obs, now):
        """Readings of an answer into the latest view and the history (snapshot stations only); returns how many the
        history did not hold yet."""
        cut = now - MADIS_HIST_S
        added = 0
        with self._hist_lock:
            for local, rows in obs.items():
                if local not in self.stations:
                    continue
                for r in rows:
                    if r["t"] < cut:
                        continue
                    ser = self._hist.get(local)
                    if ser is None:
                        ser = self._hist[local] = MadisSeries()
                    added += ser.add(r)
                r = rows[-1]
                old = self._kept.get(local)
                if old is None or r["t"] > old["t"]:
                    self._kept[local] = r
                    if self._newest_t is None or r["t"] > self._newest_t:
                        self._newest_t = r["t"]
        return added

    def _prune(self, now):
        cut = now - MADIS_KEEP_S
        for local in [k for k, r in self._kept.items() if r["t"] < cut]:
            del self._kept[local]
        cut = now - MADIS_HIST_S
        with self._hist_lock:
            for local in list(self._hist):
                ser = self._hist[local]
                ser.prune(cut)
                if not len(ser):
                    del self._hist[local]

    def refresh_due(self, now=None):
        """The base class's rule (the live poll every 5 minutes, a retry after a failure) or, while back-fill windows
        remain, the pause after the last refresh ended (MADIS_BACKFILL_TTL_S; MADIS_BACKFILL_RETRY_S after a failed
        window)."""
        if super().refresh_due(now):
            return True
        now = buoy_sources.time.time() if now is None else now
        return bool(self._bf) and now >= self._bf_next

    # -- readers
    def row(self, local):
        """The station's latest MADIS reading with its weather (a copy), or None."""
        r = self._kept.get(local)
        return dict(r) if r else None

    def hist(self, local, since):
        """The station's MADIS readings at or after since (new dicts, ascending), [] when none."""
        with self._hist_lock:
            ser = self._hist.get(local)
            return ser.rows(since) if ser is not None else []

    def filling(self):
        """True while the hours before the first live poll (or a gap) are still being asked (the window says so)."""
        if not self.backfill:
            return False
        return self._live_tried is None or bool(self._bf)

    def status(self, now=None):
        s = super().status(now)
        now = buoy_sources.time.time() if now is None else now
        with self._hist_lock:
            stations = len(self._hist)
            rows = sum(len(x) for x in self._hist.values())
            oldest = min((x.t[0] for x in self._hist.values() if len(x)), default=None)
        if not self.backfill:
            bf = None
        elif not self._bf and self._bf_queued:
            bf = "done"
        else:
            bf = "%d of %d" % (self._bf_done + self._bf_gave_up, self._bf_queued)
        s.update(readings=len(self._kept), last_rows=self._last_rows, last_wind=self._last_wind,
                 newest_age_s=round(now - self._newest_t) if self._newest_t is not None else None,
                 hist_stations=stations, hist_rows=rows,
                 hist_oldest_age_s=round(now - oldest) if oldest is not None else None,
                 backfill=bf, backfill_left=len(self._bf), backfill_gave_up=self._bf_gave_up,
                 backfill_lost=len(self._lost))
        return s

    def newest_t(self):
        """The newest reading's time MADIS ever brought (epoch s), None before the first."""
        return self._newest_t


def make_providers(stations, fetch, coops_workers=2, coops_per_refresh=COOPS_PER_REFRESH, nws_workers=2,
                   nws_per_refresh=NWS_PER_REFRESH, madis_url=None, madis_fetch=None, madis_backfill=True,
                   madis_window_fetch=None, more=True, more_enabled=None, more_per_refresh=None):
    """The providers over the snapshot's stations, in warm order (cheap, quick feeds first); with madis_url the NWS
    stations' MADIS backup too (its own fetch: the viewer is slow), which also back-fills their 24 hours (plan section
    55) unless madis_backfill is False (madis_window_fetch: the windows' fetch, default madis_fetch)."""
    groups = by_source(stations)
    aliases = {st["alias"]: st for sid, st in stations.items() if st.get("alias") and sid.startswith("coops:")}
    out = [NdbcWindProvider(groups.get("ndbc", {}), fetch, aliases),
           MetarProvider(groups.get("metar", {}), fetch),
           CoopsWindProvider(groups.get("coops", {}), fetch, coops_workers, coops_per_refresh),
           NwsProvider(groups.get("nws", {}), fetch, nws_workers, nws_per_refresh)]
    if madis_url:
        out.append(MadisProvider(groups.get("nws", {}), madis_fetch or fetch, madis_url, backfill=madis_backfill,
                                 window_fetch=madis_window_fetch))
    if more:                                    # plan section 67: Canada, Japan, IEM, Europe (more_enabled: env switches)
        import wind_more
        out += wind_more.make_providers(groups, fetch, more_enabled, more_per_refresh)
    return out


# ---------------------------------------------------------------------------------------------- the merged table

FIELDS = ["id", "t", "s", "g", "d"]


def build_latest(stations, lists, now, newest=None):
    """The page's table of latest readings. lists: {src: the provider's snapshot list (lean entries), or None for a
    provider with no list yet (it contributes nothing; the route names it as partial)}. rows: [id, t, s, g, d] per
    station with a reading, in id order; a station takes its relay's reading (alias_of: a gauge's NDBC copy, an NWS
    station's MADIS one) when it has no reading of its own or the relay's is newer (a relay keeps updating while its
    primary cannot be asked or has stalled). fed_by: {id: the relay's source} for the rows a relay gave.
    newest: {src: the newest reading time the feed ever brought} (the page notes a stalled feed); None leaves it empty.
    missing: the snapshot ids of the sources that HAVE a list (even an empty one) but no reading now."""
    rows, have, relays = {}, set(), {}
    listed = {src for src, lst in lists.items() if lst is not None}
    for src, lst in lists.items():
        for e in lst or []:
            w = e.get("wind")
            t = buoy_sources._z_epoch({"time_utc": e.get("latest_time")})
            if not isinstance(w, (list, tuple)) or len(w) != 3 or t is None:
                continue
            if e.get("alias_of"):
                old = relays.get(e["alias_of"])
                if old is None or int(t) > old[0]:
                    relays[e["alias_of"]] = (int(t), w, src)
                continue
            sid = e["id"]
            if sid in stations:
                rows[sid] = [sid, int(t), w[0], w[1], w[2]]
                have.add(sid)
    fed = {}
    for sid, (t, w, src) in relays.items():
        if sid in stations and (sid not in rows or t > rows[sid][1]):
            rows[sid] = [sid, t, w[0], w[1], w[2]]
            have.add(sid)
            fed[sid] = src
    missing = sorted(sid for sid in stations if sid not in have and sid.split(":", 1)[0] in listed)
    return {"now": int(now), "stale_s": STALE_S, "fields": list(FIELDS), "units": {"s": "m/s", "g": "m/s", "d": "deg"},
            "rows": [rows[k] for k in sorted(rows)], "missing": missing, "fed_by": {k: fed[k] for k in sorted(fed)},
            "newest": {k: int(v) for k, v in sorted((newest or {}).items()) if v is not None}}


# ---------------------------------------------------------------------------------------------- history

_BUCKETS = weakref.WeakSet()


class TokenBucket:
    """`rate` tokens a minute, at most `burst` held (default: rate): take(n) is True when n were available (then
    taken), else False and nothing is taken."""

    def __init__(self, rate=METAR_BUCKET_PER_MIN, now=time.monotonic, burst=None):
        self.rate, self._now = float(rate), now
        self.burst = float(burst if burst is not None else rate)
        self._tokens = self.burst
        self._at = now()
        self._lock = threading.Lock()
        _BUCKETS.add(self)

    def take(self, n=1):
        with self._lock:
            t = self._now()
            self._tokens = min(self.burst, self._tokens + (t - self._at) * self.rate / 60.0)
            self._at = t
            if self._tokens >= n:
                self._tokens -= n
                return True
            return False

    def _reset_lock(self):
        self._lock = threading.Lock()


_SERVICES = weakref.WeakSet()


def _held_is_better(held, rebuilt):
    """A refresh's good rebuild against the answer held (G39 V3-2 / G39-3): True when the held answer's newest reading
    (payload "t", epoch seconds, ascending) is later, or the same and the rebuild is only partial ("partial": an
    agency's budget used up, weather products missing) while the held one is not. Unknown times -> False (the rebuild
    is taken)."""
    def newest(p):
        t = p.get("t") if isinstance(p, dict) else None
        if not isinstance(t, list) or not t:
            return None
        v = t[-1]
        return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None
    a, b = newest(held), newest(rebuilt)
    if a is None or b is None:
        return False
    return a > b or (a == b and bool(rebuilt.get("partial")) and not held.get("partial"))


class WindHistory:
    """A station's readings of the last 24 hours, cached, one upstream request per station at a time (the tide
    service's core: answers are (status, payload) with status "ok", "final", "busy" or "error"; "busy" is never
    cached). fetch(url, max_bytes, headers=None) -> bytes raises on failure; now() -> epoch seconds. Every upstream
    request takes a token from its agency's budget first (METAR, NWS, CO-OPS (shared with the tide windows: G28 A-1),
    NDBC); an empty budget answers "busy". cap: an optional build cap shared with the other request-path services
    (G28 A-6) instead of this service's own `builds`."""

    def __init__(self, stations, fetch, now=time.time, max_entries=256, ok_ttl=600, error_ttl=60, builds=2,
                 wait_s=2.0, bucket=None, nws_bucket=None, coops_bucket=None, ndbc_bucket=None, cap=None):
        self.stations = stations
        self._fetch = fetch
        self._now = now
        self.max_entries = max_entries
        self.ok_ttl, self.error_ttl = ok_ttl, error_ttl
        self._builds_n, self.wait_s = builds, wait_s
        self.bucket = bucket or TokenBucket()
        self.nws_bucket = nws_bucket or TokenBucket(NWS_BUCKET_PER_MIN)
        self.coops_bucket = coops_bucket or TokenBucket(COOPS_ON_DEMAND_PER_MIN, burst=COOPS_ON_DEMAND_BURST)
        self.ndbc_bucket = ndbc_bucket or TokenBucket(NDBC_HISTORY_PER_MIN)
        self._cap = cap
        self.madis = None                     # local id -> the station's latest MADIS reading or None (plan section 52 D)
        self.madis_hist = None                # (local id, since) -> the station's MADIS readings since (plan section 55)
        self.madis_filling = None             # () -> True while MADIS is still back-filling the earlier hours
        self.more_hist = {}                   # src -> (local id, since) -> the feed's own 24 hours (plan section 67)
        self.more_buckets = {}                # src -> TokenBucket: each section-67 agency's window budget
        self.more_started = {}                # src -> when its in-memory feed started (the restart note)
        self._reset_locks()
        self._cache = OrderedDict()           # key -> (expires, status, payload, built, asked)
        _SERVICES.add(self)

    def _reset_locks(self):
        self._lock = threading.Lock()
        self._inflight = {}
        self._builds = self._cap if self._cap is not None else threading.BoundedSemaphore(self._builds_n)

    # -- cache
    def _get(self, key, fresh_s=None):
        """The key's (status, payload), or None. fresh_s (a refresh, plan section 56): an entry built or last asked
        for again at least fresh_s seconds ago counts as missing, so a station is asked again at most that often."""
        with self._lock:
            hit = self._cache.get(key)
            if hit is None:
                return None
            now = self._now()
            if hit[0] <= now:
                del self._cache[key]
                return None
            if fresh_s is not None and now - max(hit[3], hit[4]) >= fresh_s:
                return None
            self._cache.move_to_end(key)
            return hit[1], hit[2]

    def _put(self, key, status, payload, ttl):
        with self._lock:
            now = self._now()
            self._cache[key] = (now + ttl, status, payload, now, now)    # expires, status, payload, built, asked
            self._cache.move_to_end(key)
            while len(self._cache) > self.max_entries:
                self._cache.popitem(last=False)

    def _keep_after_refresh(self, key, rebuilt=None):
        """A refresh whose build did not succeed (rebuilt None), or whose good rebuild is older or thinner than the answer
        held (rebuilt = its payload: a refresh never makes an answer worse, G39 V3-2 / G39-3: _held_is_better): when the key still holds a
        good answer it stays for everyone (keep-last) with its 'asked' time set now (the per-station floor counts the
        attempt too) and is returned as (status, payload); else None."""
        with self._lock:
            hit = self._cache.get(key)
            now = self._now()
            if hit is None or hit[0] <= now or hit[1] != "ok":
                return None
            if rebuilt is not None and not _held_is_better(hit[2], rebuilt):
                return None
            self._cache[key] = hit[:4] + (now,)
            return hit[1], hit[2]

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

    def _answer(self, key, build, fresh_s=None):
        """Cached answer for key, else one build at a time (others wait up to wait_s, then 'busy'). fresh_s: a refresh
        (plan section 56): see _get; a refresh that fails leaves a good answer in place (_keep_after_refresh)."""
        hit = self._get(key, fresh_s)
        if hit is not None:
            return hit
        stuck = getattr(self._builds, "stuck", None)                # a shared cap: every slot held too long -> at once
        if stuck is not None and stuck(self.wait_s):
            return "busy", {"error": BUSY}
        deadline = time.monotonic() + self.wait_s
        e = self._key_lock(key)
        try:
            if not e[0].acquire(timeout=self.wait_s):
                return "busy", {"error": BUSY}
            try:
                hit = self._get(key, fresh_s)                          # a refresh that just finished serves this one
                if hit is not None:
                    return hit
                if not self._builds.acquire(timeout=max(0.0, deadline - time.monotonic())):
                    return "busy", {"error": BUSY}
                ttl = None
                try:
                    res = build()                                      # (status, payload) or (status, payload, ttl)
                    status, payload = res[0], res[1]
                    if len(res) > 2:
                        ttl = res[2]
                except WindBusy:
                    return "busy", {"error": BUSY}                       # never cached: asked again in a moment
                except WindError as exc:
                    _log.info("wind history %s: %s", key, exc)
                    status, payload = "error", {"error": UNAVAILABLE}
                finally:
                    self._builds.release()
                if ttl is None:
                    ttl = self.ok_ttl if status == "ok" else self.error_ttl
                if fresh_s is not None:
                    kept = self._keep_after_refresh(key, payload if status == "ok" else None)
                    if kept is not None:
                        return kept if status == "ok" else (status, payload)   # older data: the held answer; a failure: told
                self._put(key, status, payload, ttl)
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

    @staticmethod
    def _take(bucket, what, n=1):
        if not bucket.take(n):
            raise WindBusy("the %s request budget is used up" % what)

    def _ndbc(self, local, since):
        self._take(self.ndbc_bucket, "NDBC")
        try:
            text = self._body(NDBC_RT2_URL % local, NDBC_RT2_MAX).decode("utf-8", "replace")
        except WindError as exc:
            if "HTTP 404" in str(exc):                                 # no realtime2 file (8 Korean buoys, step 5 F4)
                raise WindNoFile(str(exc)) from None
            raise
        return parse_realtime2(text, since)

    def _coops(self, local, since):
        doc = self._json(coops_url(local, range=24), COOPS_MAX)
        msg = coops_error(doc)
        if msg:
            if coops_no_data(msg):
                return []                                              # NOAA has nothing for the last 24 h
            raise WindError("NOAA: %s" % msg[:120])                    # a passing message: 60 s, the NDBC relay (A-4)
        return [r for r in parse_coops_wind(doc) if r["t"] >= since]

    @staticmethod
    def _wx_products(letters):
        return [COOPS_WX_PRODUCTS[c] for c in str(letters or "") if c in COOPS_WX_PRODUCTS]

    def _coops_weather(self, local, letters, rows):
        """The gauge's own weather products (COOPS_WX_PRODUCTS, only the letters the snapshot lists) merged into the
        wind rows at the same minute, asked together under ONE deadline (WX_DEADLINE_S: a slow NOAA must not hold a
        request thread for one request after another, G28 A-6); a product that cannot be read in time or says
        anything but "no data" adds nothing (logged). -> True when every product was read (NOAA's "no data" counts),
        False when one was left out, late or failed: that answer is kept a minute only (fix round 7c verification)."""
        by_t = {r["t"]: r for r in rows}
        prods = self._wx_products(letters)
        if not prods or not by_t:
            return True
        # one token each, only if free at once: a product without one is left out like a late one (the wind rows
        # took theirs; a window never waits for all of its products' tokens at once: G28 re-check R1-N2)
        asked = [p for p in prods if self.coops_bucket.take(1)]
        complete = len(asked) == len(prods)
        if not complete:
            _log.info("wind history coops:%s: %d weather products left out (the CO-OPS budget)", local, len(prods) - len(asked))
        prods = asked
        if not prods:
            return False

        def get(prod):
            try:
                return self._json(coops_url(local, product=prod[0], range=24), COOPS_MAX), None
            except WindError as exc:
                return None, exc

        pool = ThreadPoolExecutor(min(4, len(prods)))
        try:
            futs = [pool.submit(get, prod) for prod in prods]
            done, _ = wait(futs, timeout=WX_DEADLINE_S)
        finally:
            pool.shutdown(wait=False)                                 # a late answer is dropped, never waited for
        for (product, key), fut in zip(prods, futs):
            if fut not in done:
                _log.info("wind history coops:%s: %s not read in %g s", local, product, WX_DEADLINE_S)
                complete = False
                continue
            doc, exc = fut.result()
            if exc is not None:
                _log.info("wind history coops:%s: %s not read (%s)", local, product, exc)
                complete = False
                continue
            msg = coops_error(doc)
            if msg:
                if not coops_no_data(msg):
                    _log.info("wind history coops:%s: %s: NOAA says %s", local, product, msg[:120])
                    complete = False
                continue
            check = _temp if key in ("at", "wt") else _hpa if key == "p" else _pct
            for t, v in parse_coops_series(doc).items():
                r = by_t.get(t)
                if r is not None and check(v) is not None:
                    r[key] = check(v)
        return complete

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

    def _more(self, src, local, st, since, now):
        """(rows, note, short) of a section-67 station: the feed's own 24 hours in memory (ECCC, IPMA, Met Eireann:
        with a note when the server has not run 24 hours yet, and kept only a minute while it holds no row of the
        station: the feed may read it any moment, G44 P3-7), else its agency's history behind the agency's budget."""
        if src in MORE_KEPT:
            get = self.more_hist.get(src)
            rows = get(local, since) if get else []
            started = self.more_started.get(src)
            young = bool(started and started > since)
            if not rows:
                return [], (MORE_RESTART_EMPTY if young else None), young
            return rows, (MORE_RESTART_NOTE if young else None), False
        import wind_more
        bucket = self.more_buckets.get(src)
        if bucket is None:
            bucket = self.more_buckets.setdefault(src, TokenBucket(wind_more.BUCKET_PER_MIN))
        self._take(bucket, wind_more.SOURCES[src]["short"])
        return wind_more.history(src, local, st, since, self._fetch, now) or [], None, False

    def history(self, sid, fresh_s=None):
        """fresh_s: a refresh from the wind window (plan section 56): an answer at least that old is built again."""
        st = self.stations.get(sid) if valid_id(sid) else None
        if st is None:
            return "final", {"id": sid, "error": "Unknown wind station"}
        return self._answer(("h", sid), lambda: self._build(sid, st), fresh_s)

    def _build(self, sid, st):
        now = self._now()
        since = int(now) - HISTORY_S
        src, _, local = sid.partition(":")
        note, via, no_file, partial, filling = None, None, False, False, False
        if src == "ndbc":
            try:
                rows = self._ndbc(local, since)
            except WindNoFile:
                rows, no_file = [], True
        elif src == "metar":
            rows = self._metar(local, since)
        elif src == "nws":
            # MADIS's readings of the station (its 24 hours and its latest; plan sections 52 D and 55), then the NWS
            # API's (it stalled 2026-10-22): one row per time, the NWS API's where both have one
            mrows = self.madis_hist(local, since) if self.madis_hist else []
            m = self.madis(local) if self.madis else None
            if m and m["t"] >= since and not any(r["t"] == m["t"] for r in mrows):
                mrows.append(m)
            try:
                rows = self._nws(local, since)
            except WindBusy:
                if not mrows:
                    raise
                rows, partial = [], True                               # MADIS's readings now, the API's in a minute (G38 P3-2)
            except WindError:
                if not mrows:
                    raise
                rows = []
            nws_t = {r["t"] for r in rows}
            extra = [r for r in mrows if r["t"] not in nws_t]
            if extra:
                via = "madis" if not rows else "nws+madis"
                rows = rows + extra
            if self.madis_filling and self.madis_filling():
                partial, filling = True, True
        elif src in MORE_SOURCES:                                      # plan section 67
            rows, note, partial = self._more(src, local, st, since, now)
        elif src == "coops":
            self._take(self.coops_bucket, "CO-OPS")                    # the wind rows; the products take theirs as they go
            try:
                rows = self._coops(local, since)
                partial = not self._coops_weather(local, st.get("wx"), rows)
            except WindBusy:
                raise
            except WindError as exc:
                if not st.get("alias"):
                    raise
                _log.info("wind history %s: CO-OPS did not answer (%s); NDBC's relay %s instead", sid, exc, st["alias"])
                via = "ndbc"
                try:
                    rows = self._ndbc(st["alias"], since)              # NDBC's relay of the gauge
                except WindNoFile:
                    rows, no_file = [], True
        else:                                                          # a source with no history (never served)
            rows = []
        rows = sorted({r["t"]: r for r in rows}.values(), key=lambda r: r["t"])[-HISTORY_ROWS:]
        if not rows and not note:
            note = NDBC_NO_FILE if no_file else NO_HISTORY
        for r in rows:                                                 # humidity from the dew point where not reported
            if r.get("rh") is None and r.get("at") is not None and r.get("dp") is not None:
                r["rh"] = humidity_from(r["at"], r["dp"])
        out = {"id": sid, "name": st.get("name"), "kind": st.get("kind"), "src": src, "tz": st.get("tz"),
               "alias": st.get("alias"), "net": st.get("net"), "lat": st.get("lat"), "lon": st.get("lon"),
               "hours": HISTORY_S // 3600,
               "units": {"s": "m/s", "g": "m/s", "d": "deg", "at": "degC", "wt": "degC", "dp": "degC", "rh": "%",
                         "p": "hPa", "pt": "hPa/3h", "vis": "km"}, "stale_s": STALE_S, "now": int(now),
               "t": [r["t"] for r in rows], "s": [r["s"] for r in rows], "g": [r["g"] for r in rows],
               "d": [r["d"] for r in rows], "source": ATTRIBUTION[via or src], "via": via, "note": note}
        if LICENCE_URL.get(via or src):                                # the licence text the source line links to
            out["licence_url"] = LICENCE_URL[via or src]
        if src == "nws":                                               # mesonet observations: NOAA's disclaimer
            out["disclaimer"] = MESONET_DISCLAIMER
        if filling:                                                    # MADIS is still asking the earlier hours
            out["filling"] = True
        for k in WX_FIELDS + WX_FLAGS:                                 # only the fields some row carries
            if any(r.get(k) is not None for r in rows):
                out[k] = [r.get(k) for r in rows]
        out["conditions"] = conditions(rows)
        if partial:                       # weather products missing / MADIS filling / the NWS budget: asked again in a minute
            out["partial"] = True         # (a refresh never replaces a full answer with it: _held_is_better)
            return "ok", out, self.error_ttl
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
                if k == "vis" and r.get("vg"):
                    out["vg"] = [r["t"], 1]                            # that visibility is "or more"
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


_WIND_RUNNER = None
_WIND_RUNNER_LOCK = threading.Lock()


def get_wind_runner():
    """The wind feeds' own refresh runner (WIND_REFRESH_WORKERS threads, env WIND_REFRESH_WORKERS 1-4): a slow or hung
    agency here never holds one of the live buoys' workers (G28 A-5)."""
    global _WIND_RUNNER
    with _WIND_RUNNER_LOCK:
        if _WIND_RUNNER is None:
            try:
                n = int(float(os.environ.get("WIND_REFRESH_WORKERS", WIND_REFRESH_WORKERS)))
            except (ValueError, OverflowError):
                n = WIND_REFRESH_WORKERS
            _WIND_RUNNER = buoy_sources.ThreadRunner(max(1, min(4, n)))
        return _WIND_RUNNER


def set_wind_runner(runner):
    """Install a runner (None: the default again on the next use). Returns the previous one."""
    global _WIND_RUNNER
    with _WIND_RUNNER_LOCK:
        prev, _WIND_RUNNER = _WIND_RUNNER, runner
    return prev


def _after_fork():
    global _WIND_RUNNER, _WIND_RUNNER_LOCK
    _WIND_RUNNER_LOCK = threading.Lock()
    _WIND_RUNNER = None                         # its threads are not in the child
    for svc in list(_SERVICES):
        svc._reset_locks()
    for b in list(_BUCKETS):
        b._reset_lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)
