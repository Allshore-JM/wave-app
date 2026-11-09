"""Weather at land forecast points (plan section 91).

A Forecast-point click on land (beyond the 300-m shore band) shows a weather table instead of a refusal. The numbers
come from Open-Meteo (open-meteo.com: free for non-commercial use, data CC BY 4.0) for four global models in ONE call
per point, and from the US National Weather Service's grid forecast (api.weather.gov, public domain) at US points.

Rows sit at each model's OWN steps (owner, 2026-11-09: no interpolated hours): Open-Meteo returns every model hourly,
filling the hours between a model's native steps; this module keeps only the native ones (step tables below, measured
against the answers' spans and the models' published schedules) and sums the hourly precipitation over each step, so a
row's amount is the model's own amount for its step.

Pure parsing plus an injected `fetch(url, max_bytes, headers) -> bytes` (the site's capped fetch; it raises on any
failure, an `HTTPStatusError`-like exception with `.status` for another answer than 200). Nothing here touches the
network at import."""

from __future__ import annotations

import collections
import json
import math
import threading
import time
from array import array
from datetime import datetime, timedelta, timezone

OM_FORECAST = "https://api.open-meteo.com/v1/forecast"
OM_META = "https://api.open-meteo.com/data/{}/static/meta.json"
NWS_API = "https://api.weather.gov"
USER_AGENT = "allshoresurf.com weather (https://allshoresurf.com)"
OM_HEADERS = {"User-Agent": USER_AGENT}
NWS_HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/geo+json"}

OM_MODELS = ("GFS", "ECMWF", "ICON", "AIFS")                 # the Open-Meteo tabs, in the bar's order
MODELS = OM_MODELS + ("NWS",)                                 # NWS only at US points
OM_NAME = {"GFS": "gfs_global", "ECMWF": "ecmwf_ifs025", "ICON": "icon_global", "AIFS": "ecmwf_aifs025_single"}
# the metadata files that give each model's run (the first names the run shown; GFS's global model takes its surface
# fields from two grids that finish at different times: both are in the cache key)
OM_META_NAMES = {"GFS": ("ncep_gfs013", "ncep_gfs025"), "ECMWF": ("ecmwf_ifs025",), "ICON": ("dwd_icon",),
                 "AIFS": ("ecmwf_aifs025_single",)}
LABEL = {"GFS": "GFS", "ECMWF": "ECMWF IFS", "ICON": "DWD ICON", "AIFS": "ECMWF AIFS", "NWS": "NWS"}
OM_VARS = ("temperature_2m", "dew_point_2m", "relative_humidity_2m", "cloud_cover", "precipitation", "rain",
           "showers", "snowfall", "weather_code", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m")
OM_DAYS = 16
OM_PAST_DAYS = 1                                              # so the first row's 24-h total is complete
# Open-Meteo counts a request with more than 10 variables or more than 2 weeks as fractional extra calls (its pricing
# page): 12 variables x 4 models over 17 days ~ 5.8 calls.
OM_COST = math.ceil(len(OM_VARS) * len(OM_MODELS) / 10 * (OM_DAYS + OM_PAST_DAYS) / 14)
OM_DAILY_LIMIT = 8000                                         # counted calls a UTC day (the free tier: 10,000)
OM_PER_MIN = 300                                              # counted calls a minute (the free tier: 600)
OM_MAX_BYTES = 1_000_000
META_MAX_BYTES = 64_000
META_TTL_S = 600                                              # a model's run is read again after ten minutes
META_KEEP_S = 6 * 3600                                        # a failed read keeps the last run this long
FAIL_TTL_S = 60                                               # a failed forecast call is not repeated sooner
BLOCK_S = 60                                                  # Open-Meteo said 429: nobody asks for a minute
BLOCK_LONG_S = {"hourly": 600, "daily": 1800}                 # ... or longer when its words name a longer limit


def block_for(detail):
    """How long nobody asks Open-Meteo after a 429 whose body says `detail` (its "reason": "Minutely API request
    limit exceeded ...", "Hourly ...", "Daily ..."). Unknown words: a minute. The daily limit is asked again every 30
    minutes, never all day (a shared address's count can fall when the others stop)."""
    low = (detail or "").lower()
    for word, secs in BLOCK_LONG_S.items():
        if word in low:
            return secs
    return BLOCK_S
OM_CACHE_POINTS = 32

NWS_POINT_TTL_S = 24 * 3600
NWS_GRID_TTL_S = 30 * 60
NWS_PER_MIN = 60
NWS_MAX_BYTES = 2_000_000
NWS_CACHE = 32
# where the NWS forecasts (a box first, so a point abroad never asks; /points answers 404 for the rest)
NWS_BOXES = ((24.0, 50.0, -125.0, -66.0), (51.0, 72.0, -180.0, -129.0), (51.0, 54.0, 172.0, 180.0),
             (18.0, 23.0, -161.0, -154.0), (17.0, 19.0, -68.0, -64.0), (13.0, 21.0, 144.0, 146.5),
             (-15.0, -11.0, -171.0, -168.0))

THUNDER = frozenset((95, 96, 99))
ICE = frozenset((56, 57, 66, 67))
SNOW = frozenset((71, 73, 75, 77, 85, 86))
RAIN = frozenset((51, 53, 55, 61, 63, 65, 80, 81, 82))
TYPE_MIN_MM = 0.1                                             # a step with less shows no type


class WeatherError(Exception):
    """The weather could not be read now (asking again may work)."""


class WeatherBusy(WeatherError):
    """The site's share of Open-Meteo or NWS is used up for the moment."""


def _bucket(rate):
    try:
        import wind_sources
        return wind_sources.TokenBucket(rate)
    except Exception:                                         # pragma: no cover - wind_sources is part of the site
        return None


def nws_box(lat, lon):
    """True when the point lies where the NWS forecasts (a coarse box test)."""
    return any(a <= lat <= b and c <= lon <= d for a, b, c, d in NWS_BOXES)


# ----------------------------------------------------------------------------- native steps

def om_step_h(model, init, t):
    """The length in hours of the model's native step ending at t (UTC, naive), or None when t is not a native step.
    init: the model's run (UTC, naive) or None (then the long-range rule from the start)."""
    h = (t - init).total_seconds() / 3600 if init is not None else 1e9
    if model == "GFS":                                        # hourly to +120 h, then 3-hourly to +384 h
        return 1 if h <= 120 else (3 if t.hour % 3 == 0 else None)
    if model == "ICON":                                       # hourly to +78 h, then 3-hourly to +180 h
        return 1 if h <= 78 else (3 if t.hour % 3 == 0 else None)
    if model == "ECMWF":                                      # 3-hourly to +144 h, then 6-hourly to +360 h
        if h <= 144:
            return 3 if t.hour % 3 == 0 else None
        return 6 if t.hour % 6 == 0 else None
    if model == "AIFS":                                       # 6-hourly
        return 6 if t.hour % 6 == 0 else None
    raise ValueError(model)


def precip_type(mm, codes, rain_mm=None, snow_cm=None):
    """'Thunder' > 'Ice' > 'Mix' > 'Snow' > 'Rain' from the WMO weather codes of a step's hours (and Open-Meteo's
    rain / snowfall split); '' when the step's amount is under TYPE_MIN_MM or unknown."""
    if mm is None or mm < TYPE_MIN_MM:
        return ""
    cs = {int(c) for c in codes if c is not None and not (isinstance(c, float) and math.isnan(c))}
    if cs & THUNDER:
        return "Thunder"
    if cs & ICE:
        return "Ice"
    snow = bool(cs & SNOW) or (snow_cm or 0) > 0
    rain = bool(cs & RAIN) or (rain_mm or 0) > 0
    if snow and rain:
        return "Mix"
    if snow:
        return "Snow"
    return "Rain"


def _num(v):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) else v


def _t(s):
    """'2026-11-09T06:00' (GMT) -> naive UTC datetime."""
    return datetime.strptime(s[:16], "%Y-%m-%dT%H:%M")


def parse_open_meteo(raw):
    """Open-Meteo's answer -> {"times": [naive UTC], "elev": m, "m": {model: {var: array('d') NaN = none}}}."""
    doc = json.loads(raw)
    if not isinstance(doc, dict) or doc.get("error"):
        raise WeatherError((doc or {}).get("reason") or "Open-Meteo answered without data")
    h = doc.get("hourly") or {}
    times = [_t(s) for s in h.get("time") or []]
    if not times:
        raise WeatherError("Open-Meteo answered without hours")
    n = len(times)
    out = {"times": times, "elev": doc.get("elevation"), "m": {}}
    for model, name in OM_NAME.items():
        cols = {}
        for v in OM_VARS:
            a = h.get(f"{v}_{name}")
            if a is None or len(a) != n:
                a = [None] * n
            cols[v] = array("d", (float("nan") if x is None else float(x) for x in a))
        out["m"][model] = cols
    return out


def om_rows(doc, model, init, now):
    """The model's rows from `now` (UTC, naive; floored to the hour) at its native steps, while it has data:
    [{t, step, temp, dew, rh, cloud, p, p24, type, wind, gust, dir}] (deg C, %, mm, m/s, deg FROM)."""
    times, cols = doc["times"], doc["m"][model]
    idx = {t: i for i, t in enumerate(times)}
    start = now.replace(minute=0, second=0, microsecond=0)

    def val(v, i):
        return _num(cols[v][i])

    def window(t, hours):
        out = []
        for k in range(hours):
            i = idx.get(t - timedelta(hours=k))
            if i is None:
                return None
            out.append(i)
        return out

    def summed(ix, *vs):
        if ix is None:
            return None
        tot = 0.0
        for i in ix:
            for v in vs:
                x = val(v, i)
                if x is None:
                    return None
                tot += x
        return tot

    rows = []
    for i, t in enumerate(times):
        if t < start:
            continue
        step = om_step_h(model, init, t)
        if not step:
            continue
        temp, wind = val("temperature_2m", i), val("wind_speed_10m", i)
        if temp is None or wind is None:
            continue
        hrs = window(t, step)
        p = summed(hrs, "precipitation")
        rain = summed(hrs, "rain", "showers")
        snow = summed(hrs, "snowfall")
        codes = [val("weather_code", j) for j in (hrs or [])]
        rows.append({"t": t, "step": step, "temp": temp, "dew": val("dew_point_2m", i),
                     "rh": val("relative_humidity_2m", i), "cloud": val("cloud_cover", i),
                     "p": None if p is None else round(p, 2), "p24": _round(summed(window(t, 24), "precipitation")),
                     "type": precip_type(p, codes, rain, snow), "wind": wind,
                     "gust": val("wind_gusts_10m", i), "dir": val("wind_direction_10m", i)})
    return rows


def _round(v, nd=2):
    return None if v is None else round(v, nd)


# ----------------------------------------------------------------------------- NWS

def _iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def _dur(s):
    """ISO 8601 duration ('PT3H', 'P1DT18H', 'P4D') -> timedelta."""
    if not s.startswith("P"):
        raise ValueError(s)
    days = hours = minutes = 0
    body = s[1:]
    if "T" in body:
        dpart, tpart = body.split("T", 1)
    else:
        dpart, tpart = body, ""
    if dpart:
        if not dpart.endswith("D"):
            raise ValueError(s)
        days = int(dpart[:-1])
    num = ""
    for ch in tpart:
        if ch.isdigit():
            num += ch
        elif ch == "H":
            hours, num = int(num), ""
        elif ch == "M":
            minutes, num = int(num), ""
        elif ch == "S":
            num = ""
        else:
            raise ValueError(s)
    return timedelta(days=days, hours=hours, minutes=minutes)


def nws_series(grid, key, factor=1.0):
    """A gridpoint layer -> [(start, end, value)] in UTC, value x factor (None kept)."""
    layer = grid.get(key) or {}
    uom = layer.get("uom") or ""
    f = factor
    if key in ("windSpeed", "windGust") and uom.endswith("m_s-1"):
        f = 1.0                                               # already m/s
    elif key in ("windSpeed", "windGust"):
        f = 1 / 3.6                                           # km/h (the API's usual unit)
    out = []
    for v in layer.get("values") or []:
        start, dur = v["validTime"].split("/")
        a = _iso(start)
        x = v.get("value")
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            x = float(x) * f
        out.append((a, a + _dur(dur), x))
    return out


def _at(series, t):
    for a, b, x in series:
        if a <= t < b:
            return x
    return None


def nws_type(weather_value):
    """The NWS `weather` entries at a time -> 'Thunder' / 'Ice' / 'Mix' / 'Snow' / 'Rain' / ''."""
    kinds = set()
    for w in weather_value or []:
        if not isinstance(w, dict) or not w.get("weather"):
            continue
        k = str(w["weather"])
        if "thunder" in k:
            kinds.add("t")
        if "freezing" in k or "sleet" in k or "ice" in k:
            kinds.add("i")
        if "snow" in k:
            kinds.add("s")
        if "rain" in k or "drizzle" in k or "shower" in k:
            kinds.add("r")
    if "t" in kinds:
        return "Thunder"
    if "i" in kinds:
        return "Ice"
    if "s" in kinds and "r" in kinds:
        return "Mix"
    if "s" in kinds:
        return "Snow"
    return "Rain" if "r" in kinds else ""


def nws_step_h(start, t):
    """The NWS grid's own resolution (the National Digital Forecast Database): hourly to +36 h, 3-hourly to +72 h,
    6-hourly after (counted from the first row)."""
    h = (t - start).total_seconds() / 3600
    if h < 36:
        return 1
    if h < 72:
        return 3 if t.hour % 3 == 0 else None
    return 6 if t.hour % 6 == 0 else None


def nws_rows(grid, now):
    """The NWS grid's rows from `now`. Precipitation comes in blocks (mostly 6 h): a row shows the blocks that END in
    its step; the 24-h total sums the blocks ending in the 24 h before the row (None until 24 h of blocks exist)."""
    temp = nws_series(grid, "temperature")
    if not temp:
        raise WeatherError("NWS answered without temperatures")
    dew, rh, sky_ = nws_series(grid, "dewpoint"), nws_series(grid, "relativeHumidity"), nws_series(grid, "skyCover")
    wind, gust, wdir = nws_series(grid, "windSpeed"), nws_series(grid, "windGust"), nws_series(grid, "windDirection")
    qpf = [(a, b, x) for a, b, x in nws_series(grid, "quantitativePrecipitation") if x is not None]
    wx = nws_series(grid, "weather")
    end = max(b for _, b, _ in temp)
    first_block = min((a for a, _, _ in qpf), default=None)
    start = now.replace(minute=0, second=0, microsecond=0)
    rows, prev, t = [], None, start
    while t < end:
        step = nws_step_h(start, t)
        if step:
            tv, wv = _at(temp, t), _at(wind, t)
            if tv is not None and wv is not None:
                lo = prev if prev is not None else t - timedelta(hours=step)
                ending = [x for a, b, x in qpf if lo < b <= t]
                p = round(sum(ending), 2) if ending else None
                p24 = None
                if first_block is not None and first_block <= t - timedelta(hours=24):
                    p24 = round(sum(x for a, b, x in qpf if t - timedelta(hours=24) < b <= t), 2)
                typ = nws_type(_at(wx, t)) if (p is None or p >= TYPE_MIN_MM) else ""
                rows.append({"t": t, "step": step, "temp": tv, "dew": _at(dew, t), "rh": _at(rh, t),
                             "cloud": _at(sky_, t), "p": p, "p24": p24, "type": typ, "wind": wv,
                             "gust": _at(gust, t), "dir": _at(wdir, t)})
                prev = t
        t += timedelta(hours=1)
    return rows


# ----------------------------------------------------------------------------- the service

class WeatherService:
    """Open-Meteo and NWS answers for land points, cached. One Open-Meteo call per point per model run (the cache key
    carries every model's run from Open-Meteo's metadata, read again every ten minutes); NWS grids 30 minutes."""

    def __init__(self, fetch, now=time.time, mono=time.monotonic):
        self.fetch, self.now, self.mono = fetch, now, mono
        self._lock = threading.Lock()
        self._keylocks = {}
        self._meta = {}                                       # name -> {"at": mono, "ok": mono, "init": epoch}
        self._om = collections.OrderedDict()                  # (lat, lon) -> {"runs": tuple, "doc": parsed}
        self._fail = {}                                       # (lat, lon) -> mono of the last failure
        self._blocked = 0.0                                   # mono until which Open-Meteo is not asked
        self._nws_pts = collections.OrderedDict()             # (lat, lon) -> (mono, grid url or None, place)
        self._nws_grid = collections.OrderedDict()            # url -> (mono, grid properties)
        self._day = (None, 0.0)
        self.om_bucket = _bucket(OM_PER_MIN)
        self.nws_bucket = _bucket(NWS_PER_MIN)
        self.stats = {"om_calls": 0, "om_cached": 0, "nws_calls": 0, "busy": 0, "errors": 0, "last_error": None}

    # ---- helpers
    def _klock(self, key):
        with self._lock:
            lk = self._keylocks.get(key)
            if lk is None:
                lk = self._keylocks[key] = threading.Lock()
                if len(self._keylocks) > 256:                 # forget idle ones
                    for k in [k for k, v in self._keylocks.items() if k != key and not v.locked()][:128]:
                        self._keylocks.pop(k, None)
            return lk

    def _err(self, msg):
        with self._lock:
            self.stats["errors"] += 1
            self.stats["last_error"] = msg[:200]

    def reset_after_fork(self):
        self._lock = threading.Lock()
        self._keylocks = {}
        for b in (self.om_bucket, self.nws_bucket):
            if b is not None and hasattr(b, "_reset_lock"):
                b._reset_lock()

    def _budget(self, cost):
        """Take `cost` counted calls from the minute bucket and the day's count, or raise WeatherBusy."""
        today = datetime.fromtimestamp(self.now(), timezone.utc).date()
        with self._lock:
            day, used = self._day
            if day != today:
                day, used = today, 0.0
            if used + cost > OM_DAILY_LIMIT:
                self.stats["busy"] += 1
                raise WeatherBusy("daily")
            if self.om_bucket is not None and not self.om_bucket.take(cost):
                self.stats["busy"] += 1
                raise WeatherBusy("minute")
            self._day = (day, used + cost)

    # ---- Open-Meteo
    def runs(self):
        """{model: run init (naive UTC) or None}, plus the cache-key tuple."""
        inits, key = {}, []
        for model in OM_MODELS:
            first = None
            for name in OM_META_NAMES[model]:
                ep = self._meta_init(name)
                key.append(ep)
                if first is None:
                    first = ep
            inits[model] = None if first is None else datetime.fromtimestamp(first, timezone.utc).replace(tzinfo=None)
        return inits, tuple(key)

    def _meta_init(self, name):
        t = self.mono()
        with self._lock:
            m = self._meta.get(name)
            if m and t - m["at"] < META_TTL_S:
                return m.get("init")
            if m:
                m["at"] = t                                   # one reader at a time asks again
        try:
            raw = self.fetch(OM_META.format(name), META_MAX_BYTES, OM_HEADERS)
            init = int(json.loads(raw)["last_run_initialisation_time"])
        except Exception as exc:
            self._err(f"meta {name}: {exc}")
            with self._lock:
                m = self._meta.setdefault(name, {"at": t, "ok": None, "init": None})
                m["at"] = t
                if m.get("ok") is None or t - m["ok"] > META_KEEP_S:
                    m["init"] = None
                return m.get("init")
        with self._lock:
            self._meta[name] = {"at": t, "ok": t, "init": init}
        return init

    def open_meteo(self, lat, lon):
        """The parsed Open-Meteo answer for the point (cached per model run); raises WeatherError / WeatherBusy."""
        key = (round(lat, 3), round(lon, 3))
        inits, runs = self.runs()
        with self._klock(key):
            t = self.mono()
            with self._lock:
                hit = self._om.get(key)
                if hit and hit["runs"] == runs and None not in runs:
                    self._om.move_to_end(key)
                    self.stats["om_cached"] += 1
                    return hit["doc"], inits
                if hit and None in runs and t - hit["at"] < META_TTL_S * 3:
                    return hit["doc"], inits                  # runs unknown: a recent answer stands
                if t < self._blocked:                         # before the failure memory: a 429 is "busy"
                    self.stats["busy"] += 1
                    raise WeatherBusy("blocked")
                failed = self._fail.get(key)
                if failed is not None and t - failed < FAIL_TTL_S:
                    raise WeatherError("recent failure")
            self._budget(OM_COST)
            url = (f"{OM_FORECAST}?latitude={key[0]}&longitude={key[1]}&hourly={','.join(OM_VARS)}"
                   f"&models={','.join(OM_NAME[m] for m in OM_MODELS)}&timezone=GMT&forecast_days={OM_DAYS}"
                   f"&past_days={OM_PAST_DAYS}&wind_speed_unit=ms")
            try:
                with self._lock:
                    self.stats["om_calls"] += 1
                doc = parse_open_meteo(self.fetch(url, OM_MAX_BYTES, OM_HEADERS))
            except Exception as exc:
                status = getattr(exc, "status", None)
                with self._lock:
                    self._fail[key] = self.mono()
                    if status == 429:
                        self._blocked = self.mono() + block_for(getattr(exc, "detail", ""))
                self._err(f"open-meteo {key}: {exc}")
                if status == 429:
                    raise WeatherBusy("429") from exc
                raise WeatherError(str(exc)) from exc
            with self._lock:
                self._fail.pop(key, None)
                self._om[key] = {"runs": runs, "doc": doc, "at": self.mono()}
                self._om.move_to_end(key)
                while len(self._om) > OM_CACHE_POINTS:
                    self._om.popitem(last=False)
            return doc, inits

    # ---- NWS
    def _nws_get(self, url):
        if self.nws_bucket is not None and not self.nws_bucket.take(1):
            with self._lock:
                self.stats["busy"] += 1
            raise WeatherBusy("nws")
        with self._lock:
            self.stats["nws_calls"] += 1
        return self.fetch(url, NWS_MAX_BYTES, NWS_HEADERS)

    def nws_grid(self, lat, lon):
        """The NWS grid forecast's properties for the point, or None where the NWS has none; raises WeatherError."""
        if not nws_box(lat, lon):
            return None
        key = (round(lat, 3), round(lon, 3))
        with self._klock(("nws",) + key):
            t = self.mono()
            with self._lock:
                hit = self._nws_pts.get(key)
            if hit and t - hit[0] < NWS_POINT_TTL_S:
                url = hit[1]
            else:
                try:
                    raw = self._nws_get(f"{NWS_API}/points/{key[0]},{key[1]}")
                    url = json.loads(raw)["properties"]["forecastGridData"]
                except WeatherBusy:
                    raise
                except Exception as exc:
                    if getattr(exc, "status", None) == 404:
                        url = None                            # outside the NWS's grids
                    else:
                        self._err(f"nws points {key}: {exc}")
                        raise WeatherError(str(exc)) from exc
                with self._lock:
                    self._nws_pts[key] = (t, url)
                    while len(self._nws_pts) > NWS_CACHE * 4:
                        self._nws_pts.popitem(last=False)
            if not url or not str(url).startswith(NWS_API + "/"):
                return None
            with self._lock:
                g = self._nws_grid.get(url)
            if g and t - g[0] < NWS_GRID_TTL_S:
                return g[1]
            try:
                grid = json.loads(self._nws_get(url))["properties"]
            except WeatherBusy:
                raise
            except Exception as exc:
                self._err(f"nws grid {url}: {exc}")
                if g:
                    return g[1]                               # a held grid stands through a failed refresh
                raise WeatherError(str(exc)) from exc
            with self._lock:
                self._nws_grid[url] = (t, grid)
                while len(self._nws_grid) > NWS_CACHE:
                    self._nws_grid.popitem(last=False)
            return grid

    # ---- the table
    def table(self, model, lat, lon, now=None):
        """{"model", "run" (naive UTC or None), "rows", "elev", "source"} for the point, or None (NWS abroad)."""
        now = now or datetime.now(timezone.utc).replace(tzinfo=None)
        if model == "NWS":
            grid = self.nws_grid(lat, lon)
            if grid is None:
                return None
            run = None
            try:
                run = _iso(grid.get("updateTime"))
            except Exception:
                pass
            elev = (grid.get("elevation") or {}).get("value")
            return {"model": "NWS", "run": run, "rows": nws_rows(grid, now), "elev": elev, "source": "nws"}
        doc, inits = self.open_meteo(lat, lon)
        rows = om_rows(doc, model, inits.get(model), now)
        if not rows:
            raise WeatherError(f"no {model} rows")
        return {"model": model, "run": inits.get(model), "rows": rows, "elev": doc.get("elev"), "source": "open-meteo"}

    def health(self):
        with self._lock:
            day, used = self._day
            return dict(self.stats, day_used=round(used, 1), day_limit=OM_DAILY_LIMIT, cached_points=len(self._om),
                        nws_grids=len(self._nws_grid), blocked_s=max(0, round(self._blocked - self.mono())))
