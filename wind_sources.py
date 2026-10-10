"""Live wind stations (plan section 39): the latest wind reading of every station on the map's wind layer and a
station's last 24 hours, from three public-domain NOAA / NWS feeds.

The stations are a committed snapshot (wind_stations.json, built by tools/wind/fetch_stations.py): ids "coops:1612340"
(a NOAA tide gauge's weather sensors), "ndbc:51003" (NDBC buoys, C-MAN and other fixed stations) and "metar:PHNL"
(airports and offshore platforms within 30 km of a coast or at sea). A reading is {t: epoch seconds, s: speed m/s,
g: gust m/s or None, d: direction the wind blows FROM in degrees true, or None when calm or variable}.

The latest readings come from three BuoyProvider subclasses (buoy_sources: the section-36 stale-while-revalidate
lists, keep-last on a failed fetch, the shared refresh runner and the fork reset) kept in a list of their OWN
(app.get_wind_providers): never in the live-buoy list, so the live memo, route and golden stay as they are.
  NDBC   one GET of data/latest_obs/latest_obs.txt (every station with a wind speed now; ~5-minute file)
  METAR  one GET of aviationweather.gov's metars.cache.csv.gz (the last ~80 minutes of reports, whole knots)
  COOPS  one datagetter request per gauge (product=wind&date=latest, 6-minute readings), a few at a time
The merged answer (build_latest) is a columnar table the page draws its flags from. A station's history comes from
WindHistory (the tide service's cache / per-key lock / busy core): NDBC realtime2/<ID>.txt cut at 24 h, CO-OPS
product=wind&range=24, the METAR API's hours=24 behind a token bucket (the API allows 100 requests a minute).
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
USER_AGENT = "allshoresurf.com live wind (https://allshoresurf.com)"
HEADERS = {"User-Agent": USER_AGENT}
ATTRIBUTION = {"ndbc": "NOAA National Data Buoy Center (ndbc.noaa.gov)",
               "coops": "NOAA CO-OPS (tidesandcurrents.noaa.gov)",
               "metar": "NWS Aviation Weather Center METAR (aviationweather.gov)"}
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
ID_RE = re.compile(r"(coops|ndbc|metar):[A-Z0-9]{3,8}")
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
    """A reading dict with its fields checked (t epoch seconds; s, g m/s; d degrees FROM). None without t or s."""
    try:
        t = int(t)
    except (TypeError, ValueError):
        return None
    s = _speed(s)
    if s is None:
        return None
    return {"t": t, "s": round(s, 1), "g": (round(_speed(g), 1) if _speed(g) is not None else None), "d": _direction(d)}


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


def parse_metar_history(doc):
    """api/data/metar (format=json) -> readings in the order given. obsTime is epoch seconds; wdir an integer or
    "VRB"; wspd / wgst whole knots (wgst absent without a gust)."""
    out = []
    for row in doc if isinstance(doc, list) else []:
        if not isinstance(row, dict):
            continue
        r = _metar_reading(row.get("obsTime"), row.get("wdir"), row.get("wspd"), row.get("wgst"))
        if r is not None:
            out.append(r)
    return out


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


class CoopsWindProvider(WindProvider):
    """The snapshot gauges' latest 6-minute readings, one datagetter request per gauge, PACED: NOAA answered
    HTTP 403 to everything from the server for about a minute after 232 requests in a few seconds (step 5, F1: new
    tide windows failed then too), so each refresh (every COOPS_TTL_S) asks the next `per_refresh` gauges in turn,
    `workers` at a time, and merges their readings into the ones kept: every gauge is asked about every 10 minutes
    and NOAA sees a steady trickle. A gauge whose request fails keeps its last reading; NOAA's "no data" drops it;
    a reading older than COOPS_KEEP_S is dropped. When EVERY request of a slice fails the fetch fails (keep-last
    applies); a 403 pauses the feed for COOPS_PAUSE_S (the kept readings stay)."""
    source = "COOPS"
    source_name = "NOAA CO-OPS"
    source_url = "https://tidesandcurrents.noaa.gov"
    attribution_text = "Source: " + ATTRIBUTION["coops"]
    list_ttl_sec = COOPS_TTL_S
    warm_rank = 2

    def __init__(self, stations, fetch, workers=2, per_refresh=COOPS_PER_REFRESH):
        super().__init__(stations, fetch)
        self.workers = max(1, min(8, int(workers)))
        self.per_refresh = max(1, min(500, int(per_refresh)))
        self._ids = sorted(self.stations)
        self._cursor = 0                        # the next gauge to ask (round robin over _ids)
        self._readings = {}                     # local id -> the latest reading kept
        self._paused_until = 0.0

    def _slice(self):
        """The next per_refresh gauges in turn (all of them when there are fewer)."""
        n = len(self._ids)
        if not n:
            return []
        start = self._cursor % n
        out = [self._ids[(start + i) % n] for i in range(min(self.per_refresh, n))]
        self._cursor = (start + len(out)) % n
        return out

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

    def _fetch_stations(self):
        now = buoy_sources.time.time()
        if now < self._paused_until:
            raise RuntimeError("paused after NOAA's HTTP 403 (%d s left)" % round(self._paused_until - now))
        ids = self._slice()
        if not ids:
            return []
        failures, last, forbidden = 0, None, False
        with ThreadPoolExecutor(self.workers) as pool:
            for local, r, err in pool.map(self._one, ids):
                if err:
                    failures += 1
                    last = err
                    if "HTTP 403" in err:
                        forbidden = True
                elif r is None:
                    self._readings.pop(local, None)                    # NOAA: no data for the gauge now
                else:
                    self._readings[local] = r
        if forbidden:
            self._paused_until = buoy_sources.time.time() + COOPS_PAUSE_S
            raise RuntimeError("NOAA refused a CO-OPS request (HTTP 403): paused for %d s" % COOPS_PAUSE_S)
        if failures == len(ids):
            raise RuntimeError("every CO-OPS request failed (%s)" % last)
        if failures:
            _log.info("wind provider COOPS: %d of %d gauges could not be asked (%s)", failures, len(ids), last)
        cut = now - COOPS_KEEP_S
        for local in [k for k, r in self._readings.items() if r["t"] < cut]:
            del self._readings[local]
        return [self._entry(local, r) for local, r in sorted(self._readings.items())]

    def status(self, now=None):
        s = super().status(now)
        now = buoy_sources.time.time() if now is None else now
        s.update(readings=len(self._readings), per_refresh=self.per_refresh, cursor=self._cursor,
                 paused_s=round(max(0.0, self._paused_until - now), 1))
        return s


def make_providers(stations, fetch, coops_workers=2, coops_per_refresh=COOPS_PER_REFRESH):
    """The three providers over the snapshot's stations, in warm order (cheap, quick feeds first)."""
    groups = by_source(stations)
    aliases = {st["alias"]: st for sid, st in stations.items() if st.get("alias") and sid.startswith("coops:")}
    return [NdbcWindProvider(groups.get("ndbc", {}), fetch, aliases),
            MetarProvider(groups.get("metar", {}), fetch),
            CoopsWindProvider(groups.get("coops", {}), fetch, coops_workers, coops_per_refresh)]


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
                 wait_s=2.0, bucket=None):
        self.stations = stations
        self._fetch = fetch
        self._now = now
        self.max_entries = max_entries
        self.ok_ttl, self.error_ttl = ok_ttl, error_ttl
        self._builds_n, self.wait_s = builds, wait_s
        self.bucket = bucket or TokenBucket()
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

    def _metar(self, local, since):
        if not self.bucket.take():
            raise WindBusy("the METAR request budget is used up")
        doc = self._json(METAR_API_URL % local, METAR_API_MAX)
        return [r for r in parse_metar_history(doc) if r["t"] >= since]

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
        else:
            try:
                rows = self._coops(local, since)
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
        out = {"id": sid, "name": st.get("name"), "kind": st.get("kind"), "src": src, "tz": st.get("tz"),
               "alias": st.get("alias"), "lat": st.get("lat"), "lon": st.get("lon"), "hours": HISTORY_S // 3600,
               "units": {"s": "m/s", "g": "m/s", "d": "deg"}, "stale_s": STALE_S, "now": int(now),
               "t": [r["t"] for r in rows], "s": [r["s"] for r in rows], "g": [r["g"] for r in rows],
               "d": [r["d"] for r in rows], "source": ATTRIBUTION[via or src], "via": via, "note": note}
        return "ok", out


def _after_fork():
    for svc in list(_SERVICES):
        svc._reset_locks()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)
