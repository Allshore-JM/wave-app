from flask import Flask, render_template, request, jsonify
import requests
import json
import os
from datetime import datetime, timedelta
import pytz
# TimezoneFinder is imported lazily to avoid heavy startup cost on Render.
from calendar import monthrange
from bs4 import BeautifulSoup
import re
import math
import time
import xml.etree.ElementTree as ET

app = Flask(__name__)

# --- Jinja filter: format a datetime in a given IANA time zone ---
from datetime import datetime, timezone
import pytz  # keeping your existing library

@app.template_filter("in_tz")
def jinja_in_tz(dt, tz_name, fmt="%b %d, %Y %I:%M %p"):
    """
    Format dt (aware/naive/ISO string) in tz_name. Naive is treated as UTC.
    tz_name should be an IANA name like 'Pacific/Honolulu'.
    """
    # Accept strings (prefer ISO 8601, fallback to "YYYYMMDD HH" seen in Cycle lines)
    if isinstance(dt, str):
        # Try ISO first
        try:
            if dt.endswith("Z"):
                dt = datetime.fromisoformat(dt[:-1] + "+00:00")
            else:
                dt = datetime.fromisoformat(dt)
        except Exception:
            # Try a Cycle-like "YYYYMMDD HH" pattern
            try:
                import re
                m = re.search(r"(\d{8})\s+(\d{2})", dt)
                if m:
                    dt = datetime.strptime(m.group(1) + " " + m.group(2), "%Y%m%d %H")
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    return dt  # give up; return as-is
            except Exception:
                return dt  # return as-is if unparseable

    # Treat naive as UTC
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)

    # Resolve target tz
    try:
        tz = pytz.timezone(tz_name or "UTC")
    except Exception:
        tz = pytz.utc

    return dt.astimezone(tz).strftime(fmt)


# Lazy TimezoneFinder instance
tz_finder = None


def get_tz_finder():
    global tz_finder
    if tz_finder is None:
        from timezonefinder import TimezoneFinder
        tz_finder = TimezoneFinder()
    return tz_finder

# Caches
STATION_META = None          # station_id -> {name, lat, lon}
STATION_COORDS = None        # station_id -> {lat, lon}
BULLET_STATIONS = None
stations_data_cache = None

# NOAA URLs
NOAA_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"

# ---------------------- HTTP session, logging, caches (Phase 2a) ----------------
import time
import logging
import threading
import hashlib
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:  # pragma: no cover - urllib3 always present with requests
    Retry = None

# Configure only our own logger (don't call basicConfig, which mutates the root
# logger and can interfere with gunicorn's logging depending on import order).
logger = logging.getLogger("waveapp")
if not logger.handlers:
    _log_handler = logging.StreamHandler()
    _log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(_log_handler)
logger.setLevel(logging.INFO)
logger.propagate = False


def _build_http_session() -> requests.Session:
    """A shared session with connection pooling + automatic retries/backoff."""
    s = requests.Session()
    if Retry is not None:
        retry = Retry(
            total=2, connect=2, read=2, backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "HEAD"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
    return s


HTTP = _build_http_session()

# Single lock guarding the in-memory caches (gunicorn serves multiple threads).
_CACHE_LOCK = threading.Lock()


def _fresh(ts: float, ttl: int) -> bool:
    return (time.time() - ts) < ttl


def _evict_oldest(cache: dict, max_entries: int) -> None:
    """Drop oldest entries until len <= max_entries. Caller must hold _CACHE_LOCK.

    Works for caches keyed by dicts carrying either a 'ts' or 'timestamp' field.
    """
    while len(cache) > max_entries:
        oldest = min(cache, key=lambda k: cache[k].get("ts", cache[k].get("timestamp", 0)))
        cache.pop(oldest, None)


def _json_cached(data, max_age: int):
    """JSON response with ETag + Cache-Control; honors If-None-Match -> 304.

    If-None-Match is matched per RFC 7232: tolerates the ``W/`` weak prefix, a
    comma-separated list of tags, and the ``*`` wildcard, so a CDN in front
    (Render fronts requests with Cloudflare) that rewrites the validator still
    revalidates to 304 instead of re-sending the full body.
    """
    payload = json.dumps(data, separators=(",", ":"), sort_keys=True)
    etag = hashlib.md5(payload.encode("utf-8")).hexdigest()

    inm = request.headers.get("If-None-Match", "")
    supplied = []
    for tok in inm.split(","):
        tok = tok.strip()
        if not tok:
            continue
        if tok.startswith("W/"):
            tok = tok[2:]
        supplied.append(tok.strip().strip('"'))
    matched = inm.strip() == "*" or etag in supplied

    # Keep Content-Type identical across 200 and 304 (RFC 7232).
    if matched:
        resp = app.response_class(status=304, mimetype="application/json")
    else:
        resp = app.response_class(payload, mimetype="application/json")
    resp.headers["Cache-Control"] = f"public, max-age={max_age}"
    resp.headers["ETag"] = f'"{etag}"'
    return resp


# Latest GFS-wave run detection cache (runs publish ~4x/day).
_RUN_CACHE = {"ts": 0.0, "value": (None, None)}
_RUN_CACHE_TTL = 30 * 60

# Parsed .bull forecast cache: (station_id, tz_name) -> {"ts", "data"}.
_FORECAST_CACHE = {}
_FORECAST_CACHE_TTL = 30 * 60
_FORECAST_CACHE_MAX = 64

# Timezones
HST = pytz.timezone("Pacific/Honolulu")
UTC = pytz.utc

# Curated fallback stations
DEFAULT_STATIONS = {
    "51201": {"name": "Buoy 51201", "lat": 21.67, "lon": -158.12},
    "51202": {"name": "Buoy 51202", "lat": 21.45, "lon": -157.90},
    "51203": {"name": "Buoy 51203", "lat": 21.55, "lon": -157.95},
    "51211": {"name": "Buoy 51211", "lat": 21.32, "lon": -157.53},
    "51212": {"name": "Buoy 51212", "lat": 21.27, "lon": -157.47},
    "51213": {"name": "Buoy 51213", "lat": 21.17, "lon": -157.17},
    "51001": {"name": "Buoy 51001", "lat": 16.87, "lon": -156.47},
    "51002": {"name": "Buoy 51002", "lat": 12.38, "lon": -157.49},
    "51003": {"name": "Buoy 51003", "lat": 23.69, "lon": -162.25},
    "51004": {"name": "Buoy 51004", "lat": 25.84, "lon": -162.09},
}

# ----------------------------- Static station files -----------------------------

def load_station_coords() -> dict:
    """Load precomputed lat/lon from station_coords.json if present."""
    global STATION_COORDS
    if STATION_COORDS is not None:
        return STATION_COORDS
    base_dir = os.path.dirname(os.path.abspath(__file__))
    coord_path = os.path.join(base_dir, 'station_coords.json')
    coords = {}
    try:
        with open(coord_path, 'r') as f:
            data = json.load(f)
            for sid, info in data.items():
                try:
                    lat = float(info.get('lat'))
                    lon = float(info.get('lon'))
                    coords[str(sid).strip()] = {'lat': lat, 'lon': lon}
                except Exception:
                    continue
    except Exception:
        coords = {}
    STATION_COORDS = coords
    return STATION_COORDS

def load_station_metadata():
    """Fetch NDBC station_table for names and coarse lat/lon; fallback to defaults."""
    global STATION_META
    if STATION_META is not None:
        return STATION_META
    station_url = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
    meta = {}
    try:
        res = HTTP.get(station_url, timeout=30)
        res.raise_for_status()
        for line in res.text.splitlines():
            if not line or line.startswith('#'):
                continue
            parts = line.split('|')
            if len(parts) < 7:
                continue
            station_id = parts[0].strip()
            if not station_id:
                continue
            name = parts[4].strip() or station_id
            location_field = parts[6].strip()
            tokens = location_field.split()
            if len(tokens) >= 4:
                try:
                    lat_val = float(tokens[0]); lat_dir = tokens[1].upper()
                    lon_val = float(tokens[2]); lon_dir = tokens[3].upper()
                    lat = lat_val if lat_dir == 'N' else -lat_val
                    lon = lon_val if lon_dir == 'E' else -lon_val
                    meta[station_id] = {'name': name, 'lat': lat, 'lon': lon}
                except Exception:
                    continue
        STATION_META = meta
    except Exception:
        STATION_META = DEFAULT_STATIONS.copy()
    return STATION_META

def get_station_list() -> list[tuple[str, str]]:
    """Read station_list.json; fallback to DEFAULT_STATIONS."""
    stations = []
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(base_dir, 'station_list.json')
        with open(json_path, 'r') as f:
            station_ids = json.load(f)
        meta = {}
        try:
            meta = load_station_metadata()
        except Exception:
            meta = {}
        for sid in station_ids:
            sid_str = str(sid).strip()
            if not sid_str:
                continue
            info = meta.get(sid_str)
            name = info['name'] if info and 'name' in info else sid_str
            stations.append((sid_str, name))
        if stations:
            return stations
    except Exception:
        pass
    return [(sid, info.get('name', sid)) for sid, info in DEFAULT_STATIONS.items()]

def get_stations_data():
    """Return list of {id, name, lat, lon} used by /stations.json."""
    global stations_data_cache
    if stations_data_cache is not None:
        return stations_data_cache
    try:
        ids_with_names = get_station_list()
        id_list = [sid for sid, _ in ids_with_names]
    except Exception:
        id_list = []
    try:
        meta = load_station_metadata()
    except Exception:
        meta = {}
    coords_map = load_station_coords()
    data_list = []
    for sid in id_list:
        name = sid
        info = meta.get(sid)
        if info and 'name' in info:
            name = info['name']
        lat = lon = None
        if sid in coords_map:
            lat = coords_map[sid]['lat']
            lon = coords_map[sid]['lon']
        if (lat is None or lon is None) and sid in DEFAULT_STATIONS:
            fallback_info = DEFAULT_STATIONS[sid]
            lat = fallback_info.get('lat')
            lon = fallback_info.get('lon')
        if lat is not None and lon is not None:
            data_list.append({'id': sid, 'name': name, 'lat': lat, 'lon': lon})
    stations_data_cache = data_list
    return stations_data_cache

@app.route('/stations.json')
def stations_json():
    # Station geometry is effectively static; allow client/CDN caching + 304s.
    return _json_cached(get_stations_data(), max_age=3600)


# Small inline wave icon so /favicon.ico stops 404-ing (and adds light branding).
_FAVICON_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">'
    '<rect width="32" height="32" rx="6" fill="#0b2536"/>'
    '<path d="M2 20c3 0 3-4 6-4s3 4 6 4 3-4 6-4 3 4 6 4 3-4 4-4" '
    'fill="none" stroke="#00e5ff" stroke-width="2.5" stroke-linecap="round"/>'
    '<path d="M2 25c3 0 3-4 6-4s3 4 6 4 3-4 6-4 3 4 6 4 3-4 4-4" '
    'fill="none" stroke="#4094ff" stroke-width="2.5" stroke-linecap="round"/>'
    '</svg>'
)


@app.route('/favicon.ico')
def favicon():
    resp = app.response_class(_FAVICON_SVG, mimetype="image/svg+xml")
    resp.headers["Cache-Control"] = "public, max-age=604800"
    return resp

# ----------------------------- NOAA run detection ------------------------------

def _detect_latest_run():
    """
    Find the most recent available GFS wave run by probing 18/12/06/00 of today and yesterday.
    Up to 8 serial HEAD requests; wrapped by the cached get_latest_run() below.
    """
    now = datetime.utcnow()
    run_hours = [18, 12, 6, 0]
    for delta_day in [0, 1]:
        check_date = now - timedelta(days=delta_day)
        yyyymmdd = check_date.strftime("%Y%m%d")
        for hour in run_hours:
            run_str = f"{hour:02d}"
            url = f"{NOAA_BASE}/gfs.{yyyymmdd}/{run_str}/wave/station/bulls.t{run_str}z/"
            test_file = f"{url}gfswave.51201.bull"
            try:
                resp = HTTP.head(test_file, timeout=10)
                if resp.status_code == 200:
                    return yyyymmdd, run_str
            except Exception as exc:
                logger.debug("run probe failed for %s: %r", test_file, exc)
                continue
    return None, None


def get_latest_run():
    """Cached wrapper around _detect_latest_run() — probe at most once per TTL.

    The previous code re-probed NOAA (up to 8 serial HEADs) on *every* page load,
    which dominated cold-start latency. Runs only publish ~4x/day, so a 30-minute
    cache is safe and removes that cost from nearly all requests.
    """
    with _CACHE_LOCK:
        cached = _RUN_CACHE["value"]
        if cached[0] and _fresh(_RUN_CACHE["ts"], _RUN_CACHE_TTL):
            return cached
    result = _detect_latest_run()
    if result and result[0]:
        with _CACHE_LOCK:
            _RUN_CACHE["ts"] = time.time()
            _RUN_CACHE["value"] = result
    else:
        logger.warning("get_latest_run: no recent GFS-wave run detected")
    return result

# ----------------------------- Bulletin parser ---------------------------------

def _safe_tzname_for_latlon(lat, lon):
    try:
        finder = get_tz_finder()
        name = finder.timezone_at(lat=lat, lng=lon)
        return name or 'UTC'
    except Exception:
        return 'UTC'

def _parse_header_coords(location_str: str):
    """Parse '(21.67N 158.12W)' from the Location header."""
    import re
    lat = lon = None
    if not location_str:
        return None, None
    m = re.search(r"\(([-+]?\d+(?:\.\d+)?)\s*([NS])\s+([-+]?\d+(?:\.\d+)?)\s*([EW])\)", location_str)
    if m:
        try:
            lat_val = float(m.group(1)); lat_dir = m.group(2).upper()
            lon_val = float(m.group(3)); lon_dir = m.group(4).upper()
            lat = lat_val if lat_dir == 'N' else -lat_val
            lon = lon_val if lon_dir == 'E' else -lon_val
        except Exception:
            lat = lon = None
    return lat, lon

def _strip_header_prefix(line: str, key: str) -> str:
    """Remove leading 'Cycle :' or 'Location :' (case/space tolerant)."""
    import re
    if not line:
        return ""
    m = re.match(rf"^\s*{re.escape(key)}\s*:?\s*(.*)$", line, flags=re.IGNORECASE)
    return (m.group(1) if m else line).strip()

def _fmt_latlon(lat: float | None, lon: float | None) -> str | None:
    if lat is None or lon is None:
        return None
    lat_hemi = "N" if lat >= 0 else "S"
    lon_hemi = "E" if lon >= 0 else "W"
    return f"{abs(lat):.2f}{lat_hemi} {abs(lon):.2f}{lon_hemi}"

def _resolve_day_hour_ts(cycle_dt_utc: datetime, day_val: int, hour_val: int, last_dt_utc: datetime | None) -> datetime:
    """
    Build a *correct* UTC datetime for 'day & hour' rows.

    Rules:
    - If day < cycle_day -> roll to NEXT calendar month (with year rollover).
    - Validate day against month length.
    - If the result is before the cycle time (e.g., same day but earlier hour), add one day.
    - Guarantee strictly increasing timestamps: if <= last row, bump by 1 hour until monotonic.
    """
    y = cycle_dt_utc.year
    m = cycle_dt_utc.month
    # roll to next month if needed
    if day_val < cycle_dt_utc.day:
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    # cap/validate day
    dim = monthrange(y, m)[1]
    d = min(max(1, day_val), dim)
    dt = datetime(y, m, d, int(hour_val))
    if dt < cycle_dt_utc:
        dt += timedelta(days=1)
    if last_dt_utc is not None and dt <= last_dt_utc:
        # rows are hourly; ensure strictly increasing
        delta_hours = int(((last_dt_utc - dt).total_seconds() // 3600) + 1)
        dt = dt + timedelta(hours=delta_hours)
    return dt

def parse_bull(station_id: str, target_tz_name: str | None = None):
    """Cached forecast retrieval (bounded, TTL'd) wrapping _parse_bull_uncached().

    Keyed by (station_id, tz). Only successful parses are cached. This is the
    single cached forecast path; a future JSON endpoint should call this too so
    both share one cache rather than re-fetching/parsing NOAA per request.
    """
    key = (station_id, target_tz_name or "")
    with _CACHE_LOCK:
        entry = _FORECAST_CACHE.get(key)
        if entry and _fresh(entry["ts"], _FORECAST_CACHE_TTL):
            return entry["data"]
    data = _parse_bull_uncached(station_id, target_tz_name)
    # data[-1] is the error field; only cache clean results.
    if data and not data[-1]:
        with _CACHE_LOCK:
            _FORECAST_CACHE[key] = {"ts": time.time(), "data": data}
            _evict_oldest(_FORECAST_CACHE, _FORECAST_CACHE_MAX)
    return data


def _parse_bull_uncached(station_id: str, target_tz_name: str | None = None):
    """
    Fetch and parse .bull for station. Returns:
    (cycle_str, location_str, model_run_str, rows, tz_name, error)
    rows: [date_str, time_str, s1_hs, s1_tp, s1_dir, ..., s6_hs, s6_tp, s6_dir, combined_hs]
    """
    date_str, run_str = get_latest_run()
    if not date_str:
        return None, None, None, None, 'UTC', "No recent run found."

    bull_url = f"{NOAA_BASE}/gfs.{date_str}/{run_str}/wave/station/bulls.t{run_str}z/gfswave.{station_id}.bull"
    try:
        resp = HTTP.get(bull_url, timeout=15)
    except Exception as exc:
        logger.warning("could not download .bull for %s: %r", station_id, exc)
        return None, None, None, None, 'UTC', f"Could not download .bull for {station_id}"
    if resp.status_code != 200 or not resp.text:
        return None, None, None, None, 'UTC', f"No .bull file found for {station_id}"

    lines = resp.text.splitlines()
    # Headers
    cycle_line = next((l for l in lines if l.lower().strip().startswith("cycle")), lines[0] if lines else "")
    location_line = next((l for l in lines if l.lower().strip().startswith("location")), lines[1] if len(lines) > 1 else "")
    cycle_str = cycle_line.strip()
    location_str = location_line.strip()

    # coords -> timezone
    lat, lon = _parse_header_coords(location_str)
    tz_name_from_loc = _safe_tzname_for_latlon(lat, lon) if (lat is not None and lon is not None) else 'UTC'
    effective_tz_name = tz_name_from_loc
    if target_tz_name:
        try:
            _ = pytz.timezone(target_tz_name)
            effective_tz_name = target_tz_name
        except Exception:
            pass

    # detect 'day & hour' format
    uses_day_hour_format = any("day &" in line.lower() for line in lines[:10])

    rows = []
    model_run_str = None

    if uses_day_hour_format:
        # Cycle datetime
        import re
        m = re.search(r"(\d{8})\s*(\d{2})", cycle_str)
        cycle_date_str = date_str
        cycle_hour_str = run_str
        if m:
            cycle_date_str = m.group(1)
            cycle_hour_str = m.group(2)
        cycle_dt_utc = datetime.strptime(f"{cycle_date_str} {cycle_hour_str}", "%Y%m%d %H")
        model_run_local = cycle_dt_utc.replace(tzinfo=UTC).astimezone(pytz.timezone(effective_tz_name))
        try:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %-d, %Y %I:%M %p")
        except Exception:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %d, %Y %I:%M %p").lstrip('0')

        # iterate day/hour rows
        M_TO_FT = 3.28084
        last_dt_utc = None
        for line in lines:
            s = line.strip()
            if not s.startswith("|"):
                continue
            if "Hst" in s or "---" in s:
                continue
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if not parts:
                continue

            # first cell: "day hour"
            day_hour = parts[0].split()
            if len(day_hour) < 2:
                continue
            try:
                day_val = int(day_hour[0])
                hour_val = int(day_hour[1])
            except ValueError:
                continue

            # Combined sea height (m) is in second cell
            combined_hs_m = None
            tok = parts[1].split()[0].replace('*', '') if parts[1].split() else None
            if tok:
                try:
                    combined_hs_m = float(tok)
                except ValueError:
                    combined_hs_m = None

            # swell groups in remaining cells
            swell_groups = []
            for f in parts[2:]:
                if not f:
                    swell_groups.append((None, None, None))
                    continue
                toks = [t.replace('*', '') for t in f.split() if t.replace('*', '') != ""]
                if len(toks) < 3:
                    swell_groups.append((None, None, None))
                else:
                    try:
                        hs = float(toks[0])
                        tp = float(toks[1])
                        dr = int(round((float(toks[2]) + 180) % 360))
                        swell_groups.append((hs, tp, dr))
                    except Exception:
                        swell_groups.append((None, None, None))

            while len(swell_groups) < 6:
                swell_groups.append((None, None, None))
            swell_groups = swell_groups[:6]

            # *** FIXED month transition ***
            forecast_dt_utc = _resolve_day_hour_ts(cycle_dt_utc, day_val, hour_val, last_dt_utc)
            last_dt_utc = forecast_dt_utc

            # localize
            try:
                local_tz = pytz.timezone(effective_tz_name)
            except Exception:
                local_tz = UTC
            local_dt = forecast_dt_utc.replace(tzinfo=UTC).astimezone(local_tz)

            try:
                date_str_local = local_dt.strftime("%A, %B %-d, %Y")
            except Exception:
                date_str_local = local_dt.strftime("%A, %B %d, %Y").lstrip('0')
            time_str_local = local_dt.strftime("%I:%M %p").lstrip('0')

            combined_hs_ft = None if combined_hs_m is None else combined_hs_m * M_TO_FT

            row = [date_str_local, time_str_local]
            for hs_m, tp_val, dir_val in swell_groups:
                if hs_m is None:
                    row.extend([None, None, None])
                else:
                    row.extend([hs_m * M_TO_FT, tp_val, dir_val])
            row.append(combined_hs_ft)
            rows.append(row)

    else:
        # Older "Hr" format (already robust across months)
        start_idx = None
        for idx, line in enumerate(lines):
            if line.strip().startswith("Hr"):
                start_idx = idx + 1
                break
        if start_idx is None:
            return cycle_str, location_str, None, None, effective_tz_name, "Data section not found in .bull file."

        import re
        m_old = re.search(r"(\d{8})\s*(\d{2})", cycle_str)
        cycle_date_str_old = date_str
        cycle_hour_str_old = run_str
        if m_old:
            cycle_date_str_old = m_old.group(1)
            cycle_hour_str_old = m_old.group(2)
        cycle_dt_utc_old = datetime.strptime(f"{cycle_date_str_old} {cycle_hour_str_old}", "%Y%m%d %H")
        model_run_local_old = cycle_dt_utc_old.replace(tzinfo=UTC).astimezone(pytz.timezone(effective_tz_name))
        try:
            model_run_str = "Model Run: " + model_run_local_old.strftime("%A, %B %-d, %Y %I:%M %p")
        except Exception:
            model_run_str = "Model Run: " + model_run_local_old.strftime("%A, %B %d, %Y %I:%M %p").lstrip('0')

        for line in lines[start_idx:]:
            parts = line.split()
            if len(parts) < 20:
                continue
            try:
                hr_offset = float(parts[0])
            except ValueError:
                continue
            utc_dt = cycle_dt_utc_old + timedelta(hours=hr_offset)
            try:
                local_tz = pytz.timezone(effective_tz_name)
            except Exception:
                local_tz = UTC
            local_dt = utc_dt.replace(tzinfo=UTC).astimezone(local_tz)
            try:
                date_str_local = local_dt.strftime("%A, %B %-d, %Y")
            except Exception:
                date_str_local = local_dt.strftime("%A, %B %d, %Y").lstrip('0')
            time_str_local = local_dt.strftime("%I:%M %p").lstrip('0')
            row = [date_str_local, time_str_local]
            idx_base = 6
            for _ in range(6):
                hs_val = tp_val = dir_val = None
                tokens_collected = 0
                while tokens_collected < 3 and idx_base < len(parts):
                    tok_clean = parts[idx_base].replace('*', '')
                    idx_base += 1
                    if tok_clean == '':
                        continue
                    if tokens_collected == 0:
                        try:
                            hs_val = float(tok_clean) * 3.28084
                            tokens_collected += 1
                            continue
                        except ValueError:
                            continue
                    if tokens_collected == 1:
                        try:
                            tp_val = float(tok_clean)
                            tokens_collected += 1
                            continue
                        except ValueError:
                            continue
                    if tokens_collected == 2:
                        try:
                            dir_val = int(round((float(tok_clean) + 180) % 360))
                            tokens_collected += 1
                            continue
                        except ValueError:
                            continue
                if tokens_collected < 3:
                    row.extend([None, None, None])
                else:
                    row.extend([hs_val, tp_val, dir_val])

            combined_hs_ft = None
            for tok in reversed(parts):
                tok_clean = tok.replace('*', '')
                if tok_clean == '':
                    continue
                try:
                    combined_hs_ft = float(tok_clean) * 3.28084
                    break
                except ValueError:
                    continue
            row.append(combined_hs_ft)
            rows.append(row)

    # rounding
    for r in rows:
        idx_num = 2
        for _ in range(6):
            if r[idx_num] is not None:
                r[idx_num] = round(r[idx_num], 2)
            idx_num += 1
            if r[idx_num] is not None:
                r[idx_num] = round(r[idx_num], 1)
            idx_num += 1
            if r[idx_num] is not None:
                try:
                    r[idx_num] = int(round(r[idx_num]))
                except Exception:
                    pass
            idx_num += 1
        if r[-1] is not None:
            r[-1] = round(r[-1], 2)

    if not rows:
        return cycle_str, location_str, model_run_str, None, effective_tz_name, "No data rows parsed from .bull file."

    return cycle_str, location_str, model_run_str, rows, effective_tz_name, None

# -------------------------- Table HTML builder (safe) ---------------------------

def build_html_table(cycle_str: str, location_str: str, model_run_str: str | None,
                     rows: list[list], tz_label: str, unit: str) -> str:
    group_colors = [
        {"header": "#C00000", "subheader": "#F8B4B4", "data": "#F9DCDC"},
        {"header": "#ED7D31", "subheader": "#FBE5D6", "data": "#FDE7D4"},
        {"header": "#FFC000", "subheader": "#FFF2CC", "data": "#FFF9E5"},
        {"header": "#00B050", "subheader": "#D5E8D4", "data": "#EAF3E8"},
        {"header": "#00B0F0", "subheader": "#D9EAF6", "data": "#ECF5FB"},
        {"header": "#92D050", "subheader": "#E2F0D9", "data": "#F2F8EE"},
    ]
    combined_colors = {"header": "#7030A0", "subheader": "#D9D2E9", "data": "#EDE9F4"}

    html = '<table class="table table-bordered table-sm">\n'
    # Everything that should stay locked while the body scrolls lives in <thead>
    # (position: sticky): the Cycle/Location/TZ info rows first, then the two
    # column-header rows.
    n_cols = 2 + len(group_colors) * 3 + 1
    html += '<thead>\n'
    html += f'<tr><td colspan="{n_cols}" class="forecast-info">{cycle_str}</td></tr>\n'
    html += f'<tr><td colspan="{n_cols}" class="forecast-info">{location_str}</td></tr>\n'
    html += f'<tr><td colspan="{n_cols}" class="forecast-info">Time Zone: {tz_label}</td></tr>\n'
    html += '<tr>'
    html += '<th rowspan="2" scope="col">Date</th><th rowspan="2" scope="col">Time</th>'
    for idx, col in enumerate(group_colors, start=1):
        html += f'<th colspan="3" scope="colgroup" style="background-color:{col["header"]}; color:white; text-align:center;">Swell {idx}</th>'
    html += f'<th scope="colgroup" style="background-color:{combined_colors["header"]}; color:white; text-align:center;">Combined</th>'
    html += '</tr>\n'

    # subheaders
    hs_unit_label = '(ft)' if unit == 'US' else '(m)'
    html += '<tr>'
    for col in group_colors:
        html += f'<th scope="col" style="background-color:{col["subheader"]}; text-align:center;">Hs<br>{hs_unit_label}</th>'
        html += f'<th scope="col" style="background-color:{col["subheader"]}; text-align:center;">Tp<br>(s)</th>'
        html += f'<th scope="col" style="background-color:{col["subheader"]}; text-align:center;">Dir<br>(d)</th>'
    html += f'<th scope="col" style="background-color:{combined_colors["subheader"]}; text-align:center;">Hs<br>{hs_unit_label}</th>'
    html += '</tr>\n'
    html += '</thead>\n'

    # rows
    html += '<tbody>\n'
    for row in rows:
        # style rules
        try:
            parsed_time = datetime.strptime(row[1], "%I:%M %p").time()
        except Exception:
            parsed_time = None
        bold_start = datetime.strptime("6:00:00 AM", "%I:%M:%S %p").time()
        bold_end = datetime.strptime("7:00:00 PM", "%I:%M:%S %p").time()
        dashed_start_evening = datetime.strptime("8:00:00 PM", "%I:%M:%S %p").time()
        dashed_end_morning = datetime.strptime("5:00:00 AM", "%I:%M:%S %p").time()
        border_style = ""
        fw = "normal"
        if parsed_time is not None:
            if bold_start <= parsed_time <= bold_end:
                border_style = "border:1px solid #000;"
                fw = "bold"
            elif parsed_time >= dashed_start_evening or parsed_time <= dashed_end_morning:
                border_style = "border:1px dashed #999;"
                fw = "normal"

        html += '<tr>'
        date_style = f'font-weight:bold; {border_style} padding:4px 8px;'
        html += f'<td style="{date_style}">{row[0]}</td>'
        time_style = f'font-weight:{fw}; {border_style} padding:4px 8px;'
        html += f'<td style="{time_style}">{row[1]}</td>'

        idx = 2
        for col in group_colors:
            # Hs
            val = row[idx]
            display_val = None if val is None else (val if unit == 'US' else (val / 3.28084))
            hs_str = "" if display_val is None else f"{display_val:.2f}"
            cell_style = f'background-color:{col["data"]}; text-align:right; font-weight:{fw}; {border_style} padding:4px 8px;'
            html += f'<td style="{cell_style}">{hs_str}</td>'
            idx += 1
            # Tp
            val = row[idx]
            tp_str = "" if val is None else f"{val:.1f}"
            html += f'<td style="{cell_style}">{tp_str}</td>'
            idx += 1
            # Dir
            val = row[idx]
            dir_str = "" if val is None else f"{val}"
            html += f'<td style="{cell_style}">{dir_str}</td>'
            idx += 1

        # Combined
        val = row[-1]
        display_comb = None if val is None else (val if unit == 'US' else (val / 3.28084))
        comb_str = "" if display_comb is None else f"{display_comb:.2f}"
        comb_style = f'background-color:{combined_colors["data"]}; text-align:right; font-weight:{fw}; {border_style} padding:4px 8px;'
        html += f'<td style="{comb_style}">{comb_str}</td>'
        html += '</tr>\n'

    html += '</tbody>\n'
    html += '</table>'
    return html

# ------------------------------ Flask routes -----------------------------------

# NDBC / GFS station ids are short and alphanumeric, with hyphens/underscores
# for some grid points (e.g. "NW-HFO60"). Validate before the value is ever
# interpolated into an outbound NOAA URL, to block path-traversal characters.
_STATION_RE = re.compile(r"[A-Za-z0-9_-]{1,32}")


def compute_forecast_payload(station: str, tz: str | None, unit: str) -> dict:
    """Shared, cached forecast computation for both the homepage and /api/forecast.

    parse_bull() underneath is cached (Phase 2a), so on a warm cache this only
    re-runs the cheap HTML/graph packing. Returns a JSON-serializable dict.
    """
    out = {
        "station": station,
        "error": None,
        "table_html": None,
        "tz_label": "",
        "lat": None,
        "lon": None,
        "graph_data": None,
        "graph_header": None,
    }
    if not station:
        return out
    if not _STATION_RE.fullmatch(station):
        out["error"] = "Invalid station id"
        return out

    cycle_str, location_str, model_run_str, rows, effective_tz_name, parse_error = parse_bull(
        station, tz or None
    )
    out["error"] = parse_error
    if rows is None:
        return out

    tz_label = effective_tz_name
    out["tz_label"] = tz_label
    out["table_html"] = build_html_table(cycle_str, location_str, model_run_str, rows, tz_label, unit)

    # single map marker if coords JSON has it
    coords_map = load_station_coords()
    sid_str = str(station).strip()
    if sid_str in coords_map:
        out["lat"] = coords_map[sid_str]['lat']
        out["lon"] = coords_map[sid_str]['lon']

    # ----- pack graph data -----
    labels = [f"{r[0]} {r[1]}" for r in rows]
    def pick(array_index):
        return [r[array_index] for r in rows]
    def hs_idx(g): return 2 + g*3
    def tp_idx(g): return 3 + g*3
    def dr_idx(g): return 4 + g*3

    # Feet from the parsed table rows (rows are feet already)
    height_ft = {
        "s1": pick(hs_idx(0)), "s2": pick(hs_idx(1)), "s3": pick(hs_idx(2)),
        "s4": pick(hs_idx(3)), "s5": pick(hs_idx(4)), "s6": pick(hs_idx(5)),
        "combined": [r[-1] for r in rows],
    }
    period = {
        "s1": pick(tp_idx(0)), "s2": pick(tp_idx(1)), "s3": pick(tp_idx(2)),
        "s4": pick(tp_idx(3)), "s5": pick(tp_idx(4)), "s6": pick(tp_idx(5)),
    }
    direction = {
        "s1": pick(dr_idx(0)), "s2": pick(dr_idx(1)), "s3": pick(dr_idx(2)),
        "s4": pick(dr_idx(3)), "s5": pick(dr_idx(4)), "s6": pick(dr_idx(5)),
    }

    if unit == "Metric":
        FT_TO_M = 0.3048
        height = {
            k: [None if v is None else round(v * FT_TO_M, 2) for v in arr]
            for k, arr in height_ft.items()
        }
        graph_units = "m"
    else:
        height = height_ft
        graph_units = "ft"

    out["graph_data"] = {
        "labels": labels, "height": height, "period": period, "direction": direction,
        "units": graph_units,
        "cycle": cycle_str or "", "location": location_str or "", "tz": tz_label or "",
    }

    cycle_clean = _strip_header_prefix(cycle_str, "Cycle")
    loc_clean   = _strip_header_prefix(location_str, "Location")
    lat, lon = _parse_header_coords(location_str)
    latlon_fmt = _fmt_latlon(lat, lon)
    loc_display = f"{station} ({latlon_fmt})" if latlon_fmt else loc_clean
    out["graph_header"] = {"cycle": cycle_clean, "location": loc_display, "tz": tz_label or ""}
    return out


def _forecast_is_cached(station: str, tz: str | None) -> bool:
    """True if parse_bull already has this (station, tz) cached and fresh."""
    key = (station, tz or "")
    with _CACHE_LOCK:
        entry = _FORECAST_CACHE.get(key)
        return bool(entry and _fresh(entry["ts"], _FORECAST_CACHE_TTL))


@app.route("/api/forecast")
def api_forecast():
    """JSON forecast for one station — same cached path the homepage uses."""
    station = (request.args.get("station") or "51201").strip()
    tz = request.args.get("tz", "")
    unit = request.args.get("unit", "US") or "US"
    try:
        return jsonify(compute_forecast_payload(station, tz or None, unit))
    except Exception as exc:  # always return JSON the client can render
        logger.warning("forecast payload failed for %s: %r", station, exc)
        return jsonify({
            "station": station, "error": "Forecast temporarily unavailable",
            "table_html": None, "tz_label": "", "lat": None, "lon": None,
            "graph_data": None, "graph_header": None,
        })


@app.route("/", methods=["GET", "POST"])
def index():
    stations = get_station_list()
    timezones = sorted(pytz.common_timezones)
    unit_options = ["US", "Metric"]

    selected_view = (request.values.get("view") or "Table")
    if request.method == "POST":
        selected_station = (request.form.get("station") or "").strip()
        selected_tz = request.form.get("tz") or ""
        selected_unit = request.form.get("unit") or "US"
    else:
        selected_station = (request.args.get("station", "") or "").strip()
        selected_tz = request.args.get("tz", "")
        selected_unit = request.args.get("unit", "US") or "US"

    if not selected_station:
        selected_station = "51201"

    # Shell-first: defer ONLY the Table view on a cold cache so the page never
    # blocks on NOAA. The browser then pulls /api/forecast and injects the table.
    # Graph view, already-cached forecasts, and ?render=full all render inline.
    force_render = request.values.get("render") == "full"
    defer_forecast = (
        selected_view == "Table"
        and not force_render
        and bool(selected_station)
        and not _forecast_is_cached(selected_station, selected_tz or None)
    )

    payload = None
    if selected_station and not defer_forecast:
        payload = compute_forecast_payload(selected_station, selected_tz or None, selected_unit)

    return render_template(
        "index.html",
        stations=stations,
        selected_station=selected_station,
        timezones=timezones,
        selected_tz=selected_tz,
        tz_label=(payload["tz_label"] if payload else ""),
        units=unit_options,
        selected_unit=selected_unit,
        table_html=(payload["table_html"] if payload else None),
        error=(payload["error"] if payload else None),
        selected_lat=(payload["lat"] if payload else None),
        selected_lon=(payload["lon"] if payload else None),
        selected_view=selected_view,
        graph_data=(payload["graph_data"] if payload else None),
        graph_header=(payload["graph_header"] if payload else None),
        defer_forecast=defer_forecast,
    )


# -------------------------- NDBC live buoy overlay ------------------------------
import re
import math
import time
import xml.etree.ElementTree as ET

NDBC_ACTIVE_XML = "https://www.ndbc.noaa.gov/activestations.xml"
NDBC_REALTIME_DIR = "https://www.ndbc.noaa.gov/data/realtime2/"
NDBC_CACHE_TTL_SECONDS = 30 * 60
NDBC_COMPONENT_TTL_SECONDS = 30 * 60
NDBC_COMPONENT_CACHE_MAX = 100

NDBC_STATIONS_CACHE = {
    "timestamp": 0,
    "data": []
}

NDBC_COMPONENT_CACHE = {}

def _cache_valid(cache_timestamp: float, ttl: int = NDBC_CACHE_TTL_SECONDS) -> bool:
    return (time.time() - cache_timestamp) < ttl

def _fetch_text(url: str, timeout: int = 25) -> str:
    resp = HTTP.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text

def _parse_active_ndbc_stations() -> dict:
    xml_text = _fetch_text(NDBC_ACTIVE_XML)
    root = ET.fromstring(xml_text)
    stations = {}
    for st in root.iter("station"):
        sid = st.attrib.get("id")
        if not sid:
            continue
        try:
            lat = float(st.attrib.get("lat"))
            lon = float(st.attrib.get("lon"))
        except Exception:
            continue
        stations[sid] = {
            "id": sid,
            "name": st.attrib.get("name", sid),
            "lat": lat,
            "lon": lon,
            "owner": st.attrib.get("owner", ""),
            "pgm": st.attrib.get("pgm", ""),
            "type": st.attrib.get("type", ""),
        }
    return stations

def _stations_with_live_spectral_wave_data() -> set:
    html = _fetch_text(NDBC_REALTIME_DIR)
    return set(re.findall(r'href="([A-Za-z0-9]+)\.data_spec"', html))

def get_live_ndbc_wave_stations() -> list:
    with _CACHE_LOCK:
        if NDBC_STATIONS_CACHE["data"] and _cache_valid(NDBC_STATIONS_CACHE["timestamp"]):
            return NDBC_STATIONS_CACHE["data"]
    # Fetch/parse outside the lock so concurrent requests don't serialize on NOAA.
    active = _parse_active_ndbc_stations()
    live_ids = _stations_with_live_spectral_wave_data()
    stations = []
    for sid in sorted(live_ids):
        info = active.get(sid)
        if not info:
            continue
        stations.append({
            **info,
            "has_live_wave_components": True,
            "source": "NDBC realtime spectral wave data"
        })
    with _CACHE_LOCK:
        NDBC_STATIONS_CACHE["timestamp"] = time.time()
        NDBC_STATIONS_CACHE["data"] = stations
    return stations

@app.route("/api/ndbc/live-wave-stations")
def api_ndbc_live_wave_stations():
    return _json_cached(get_live_ndbc_wave_stations(), max_age=900)

# ----------------------- NDBC spectral component parser -------------------------

def _parse_ndbc_spectral_file(text: str) -> list:
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 7:
            continue
        try:
            yy = int(parts[0])
            mm = int(parts[1])
            dd = int(parts[2])
            hh = int(parts[3])
            minute = int(parts[4])
        except Exception:
            continue
        year = 2000 + yy if yy < 100 else yy
        timestamp = datetime(year, mm, dd, hh, minute, tzinfo=pytz.utc)
        pairs = re.findall(r"([-+]?\d+(?:\.\d+)?|MM)\s*\(([-+]?\d+(?:\.\d+)?)\)", line)
        freqs = []
        vals = []
        for value, freq in pairs:
            if value == "MM":
                continue
            try:
                vals.append(float(value))
                freqs.append(float(freq))
            except Exception:
                continue
        if freqs and vals:
            rows.append({
                "timestamp_utc": timestamp,
                "freqs": freqs,
                "values": vals
            })
    return rows

def _latest_spectral_row(rows: list) -> dict | None:
    return max(rows, key=lambda r: r["timestamp_utc"]) if rows else None

def _bin_widths(freqs: list) -> list:
    if len(freqs) == 1:
        return [0.01]
    edges = []
    for i, f in enumerate(freqs):
        if i == 0:
            first_mid = (freqs[0] + freqs[1]) / 2.0
            edges.append(freqs[0] - (first_mid - freqs[0]))
        else:
            edges.append((freqs[i - 1] + freqs[i]) / 2.0)
    last_mid = (freqs[-2] + freqs[-1]) / 2.0
    edges.append(freqs[-1] + (freqs[-1] - last_mid))
    return [max(edges[i + 1] - edges[i], 0.0) for i in range(len(freqs))]

def _smooth3(values: list) -> list:
    if len(values) < 3:
        return values[:]
    out = []
    for i in range(len(values)):
        left = values[i - 1] if i > 0 else values[i]
        mid = values[i]
        right = values[i + 1] if i < len(values) - 1 else values[i]
        out.append((left + mid + right) / 3.0)
    return out

def _circular_mean_deg(degrees: list, weights: list) -> float | None:
    x = 0.0
    y = 0.0
    for deg, w in zip(degrees, weights):
        if deg is None or not (math.isfinite(deg) and math.isfinite(w)):
            continue
        rad = math.radians(deg)
        x += w * math.cos(rad)
        y += w * math.sin(rad)
    if abs(x) < 1e-12 and abs(y) < 1e-12:
        return None
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0

def _compass_from_degrees(deg: float | None) -> str | None:
    if deg is None:
        return None
    points = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
    return points[int((deg + 11.25) / 22.5) % 16]


def _smooth5(values: list) -> list:
    """Five-point weighted smoothing that preserves broad shoulders better than a 3-point average."""
    if len(values) < 5:
        return _smooth3(values)
    out = []
    weights = [1, 2, 3, 2, 1]
    half = 2
    for i in range(len(values)):
        num = 0.0
        den = 0.0
        for offset, w in zip(range(-half, half + 1), weights):
            j = min(max(i + offset, 0), len(values) - 1)
            num += values[j] * w
            den += w
        out.append(num / den if den else values[i])
    return out

def _angular_diff_deg(a: float | None, b: float | None) -> float | None:
    """Smallest absolute angular difference between two directions."""
    if a is None or b is None:
        return None
    try:
        if not (math.isfinite(float(a)) and math.isfinite(float(b))):
            return None
        return abs((float(a) - float(b) + 180.0) % 360.0 - 180.0)
    except Exception:
        return None

def _weighted_mean(values: list, weights: list) -> float | None:
    num = 0.0
    den = 0.0
    for v, w in zip(values, weights):
        try:
            if v is None:
                continue
            v = float(v)
            w = float(w)
            if not (math.isfinite(v) and math.isfinite(w)):
                continue
            num += v * w
            den += w
        except Exception:
            continue
    return None if den <= 0 else num / den

def _directional_spread_deg(degrees: list, weights: list) -> float | None:
    """
    Approximate circular directional spread from weighted resultant length.
    Smaller values mean a cleaner/directionally consistent component.
    """
    x = 0.0
    y = 0.0
    total = 0.0
    for deg, w in zip(degrees, weights):
        if deg is None:
            continue
        try:
            deg = float(deg)
            w = float(w)
            if not (math.isfinite(deg) and math.isfinite(w)) or w <= 0:
                continue
        except Exception:
            continue
        rad = math.radians(deg)
        x += w * math.cos(rad)
        y += w * math.sin(rad)
        total += w
    if total <= 0:
        return None
    r = min(1.0, max(0.0, math.hypot(x, y) / total))
    if r <= 0:
        return 180.0
    try:
        return math.degrees(math.sqrt(max(0.0, -2.0 * math.log(r))))
    except Exception:
        return None

def _align_spectral_values(source_row: dict | None, target_freqs: list, tolerance: float = 0.00075) -> list:
    """
    Align values from a directional/realtime file onto the density frequencies.
    Most NDBC files use matching frequency bins, but this tolerates small differences.
    """
    if not source_row:
        return [None] * len(target_freqs)

    src_freqs = source_row.get("freqs") or []
    src_vals = source_row.get("values") or []
    if not src_freqs or not src_vals:
        return [None] * len(target_freqs)

    exact = {round(float(f), 5): v for f, v in zip(src_freqs, src_vals)}
    aligned = []

    for f in target_freqs:
        key = round(float(f), 5)
        if key in exact:
            aligned.append(exact[key])
            continue

        nearest_i = min(range(len(src_freqs)), key=lambda i: abs(float(src_freqs[i]) - float(f)))
        if abs(float(src_freqs[nearest_i]) - float(f)) <= tolerance:
            aligned.append(src_vals[nearest_i])
        else:
            aligned.append(None)

    return aligned

def _parse_ndbc_spec_summary_rows(text: str) -> list:
    """
    Parse NDBC realtime .spec rows.

    Expected columns commonly include:
    YY MM DD hh mm WVHT SwH SwP WWH WWP SwD WWD STEEPNESS APD MWD

    Heights in this file are meters. Periods are seconds.
    """
    rows = []
    if not text:
        return rows

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split()
        if len(parts) < 14:
            continue

        try:
            yy = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            hour = int(parts[3])
            minute = int(parts[4])
            year = 2000 + yy if yy < 100 else yy
            dt_utc = datetime(year, month, day, hour, minute, tzinfo=pytz.utc)
        except Exception:
            continue

        def num_or_none(idx):
            try:
                val = parts[idx]
                if val in {"MM", "-", "--", "---"}:
                    return None
                return float(val)
            except Exception:
                return None

        def str_or_none(idx):
            try:
                val = parts[idx]
                if val in {"MM", "-", "--", "---"}:
                    return None
                return val
            except Exception:
                return None

        rows.append({
            "timestamp_utc": dt_utc,
            "wvht_m": num_or_none(5),
            "swh_m": num_or_none(6),
            "swp_sec": num_or_none(7),
            "wwh_m": num_or_none(8),
            "wwp_sec": num_or_none(9),
            "swd": str_or_none(10),
            "wwd": str_or_none(11),
            "steepness": str_or_none(12),
            "apd_sec": num_or_none(13),
            "mwd": str_or_none(14) if len(parts) > 14 else None,
        })

    rows.sort(key=lambda r: r["timestamp_utc"], reverse=True)
    return rows

def _estimate_separation_frequency(summary_row: dict | None) -> tuple[float, str]:
    """
    Estimate the swell/wind-wave separation frequency.

    NDBC's station-page Wave Summary is a simplified swell/wind-wave product.
    The public realtime .spec rows do not consistently expose Sep_Freq, so we
    use the midpoint between NOAA SwP and WWP when available, otherwise 0.125 Hz
    (8 seconds) as a practical default.
    """
    default_sep = 1.0 / 8.0

    if summary_row:
        swp = summary_row.get("swp_sec")
        wwp = summary_row.get("wwp_sec")
        try:
            if swp and wwp and float(swp) > 0 and float(wwp) > 0:
                swell_f = 1.0 / float(swp)
                wind_f = 1.0 / float(wwp)
                if wind_f > swell_f:
                    return ((swell_f + wind_f) / 2.0, "midpoint of NOAA SwP and WWP")
        except Exception:
            pass

    return (default_sep, "default 8-second separation")

def _find_spectral_peaks_v2(freqs: list, density: list, sep_freq: float | None = None) -> list:
    """
    More sensitive peak finder:
    - uses 5-point smoothing,
    - allows lower prominence for smaller shoulder peaks,
    - forces a representative peak on each side of the swell/wind-sea separation when energy exists.
    """
    smooth = _smooth5(density)
    if not smooth:
        return []

    max_val = max(smooth)
    if max_val <= 0:
        return []

    peaks = []
    min_density = max_val * 0.018
    min_prominence = max_val * 0.018

    for i in range(1, len(smooth) - 1):
        is_peak = smooth[i] >= smooth[i - 1] and smooth[i] >= smooth[i + 1]
        if not is_peak or smooth[i] < min_density:
            continue

        left_window = smooth[max(0, i - 5): i + 1]
        right_window = smooth[i: min(len(smooth), i + 6)]
        left_min = min(left_window) if left_window else smooth[i]
        right_min = min(right_window) if right_window else smooth[i]
        prominence = smooth[i] - max(left_min, right_min)

        if prominence >= min_prominence or smooth[i] >= max_val * 0.12:
            peaks.append(i)

    # If there are no local peaks, use the dominant bin.
    if not peaks:
        peaks = [smooth.index(max_val)]

    # Force sub-band peaks so wind sea and swell are both represented if they have meaningful energy.
    if sep_freq is not None:
        df = _bin_widths(freqs)
        total_m0 = sum(e * w for e, w in zip(density, df))
        for selector in [
            lambda f: f <= sep_freq,
            lambda f: f > sep_freq,
        ]:
            idxs = [i for i, f in enumerate(freqs) if selector(f)]
            if not idxs:
                continue
            band_m0 = sum(density[i] * df[i] for i in idxs)
            if total_m0 > 0 and band_m0 / total_m0 >= 0.035:
                best = max(idxs, key=lambda i: smooth[i])
                peaks.append(best)

    # De-duplicate and keep close peaks only if the valley between them is meaningful.
    peaks = sorted(set(peaks))
    filtered = []

    for p in sorted(peaks, key=lambda idx: smooth[idx], reverse=True):
        keep = True
        for existing in filtered:
            lo = min(p, existing)
            hi = max(p, existing)
            if hi - lo <= 2:
                keep = False
                break
            valley = min(smooth[lo: hi + 1])
            smaller_peak = min(smooth[p], smooth[existing])
            if smaller_peak > 0 and valley / smaller_peak > 0.82 and abs(hi - lo) <= 4:
                keep = False
                break
        if keep:
            filtered.append(p)

    return sorted(filtered)

def _candidate_direction_splits(freqs: list, density: list, dirs: list | None, total_m0: float) -> dict:
    """
    Identify split points where adjacent or neighboring frequency bins have a large directional change.
    The return dict maps split index -> reason.
    """
    if not dirs or total_m0 <= 0:
        return {}

    df = _bin_widths(freqs)
    splits = {}
    max_density = max(density) if density else 0.0

    for i in range(1, len(freqs) - 2):
        left_dir = dirs[i]
        right_dir = dirs[i + 1]
        diff = _angular_diff_deg(left_dir, right_dir)

        if diff is None or diff < 48:
            continue

        local_energy = (density[i] * df[i]) + (density[i + 1] * df[i + 1])
        if max_density > 0 and max(density[i], density[i + 1]) < max_density * 0.025:
            continue
        if local_energy / total_m0 < 0.006:
            continue

        # Check that there is at least some energy on both sides of the split.
        left_start = max(0, i - 3)
        right_end = min(len(freqs), i + 5)
        left_m0 = sum(density[j] * df[j] for j in range(left_start, i + 1))
        right_m0 = sum(density[j] * df[j] for j in range(i + 1, right_end))
        if left_m0 / total_m0 < 0.006 or right_m0 / total_m0 < 0.006:
            continue

        splits[i] = "direction shift"

    return splits

def _broad_band_directional_split(start: int, end: int, freqs: list, density: list,
                                  dirs: list | None, total_m0: float) -> tuple[int, str] | None:
    """
    Split a broad spectral band if its two halves have enough energy and clearly different directions.
    This catches cases where two swell trains overlap in period and do not form a strong valley.
    """
    if not dirs or end - start < 5 or total_m0 <= 0:
        return None

    df = _bin_widths(freqs)
    best = None

    for split in range(start + 2, end - 2):
        left_idxs = list(range(start, split + 1))
        right_idxs = list(range(split + 1, end + 1))

        left_w = [density[i] * df[i] for i in left_idxs]
        right_w = [density[i] * df[i] for i in right_idxs]

        left_m0 = sum(left_w)
        right_m0 = sum(right_w)

        if left_m0 / total_m0 < 0.025 or right_m0 / total_m0 < 0.025:
            continue

        left_dir = _circular_mean_deg([dirs[i] for i in left_idxs], left_w)
        right_dir = _circular_mean_deg([dirs[i] for i in right_idxs], right_w)
        diff = _angular_diff_deg(left_dir, right_dir)

        if diff is None or diff < 42:
            continue

        balance = min(left_m0, right_m0) / max(left_m0, right_m0)
        score = diff * balance

        if best is None or score > best[0]:
            best = (score, split, diff)

    if best:
        return (best[1], f"broad-band directional split ({best[2]:.0f}°)")

    return None

def _make_segments_from_splits(n: int, split_reasons: dict) -> list:
    split_points = sorted(i for i in split_reasons.keys() if 0 <= i < n - 1)
    segments = []
    start = 0
    for split in split_points:
        if split >= start:
            segments.append((start, split))
            start = split + 1
    if start <= n - 1:
        segments.append((start, n - 1))
    return [(s, e) for s, e in segments if e >= s]

def _partition_spectrum_v2(freqs: list, density: list,
                           directions: list | None = None,
                           directions2: list | None = None,
                           r1_values: list | None = None,
                           r2_values: list | None = None,
                           sep_freq: float | None = None) -> list:
    """
    Direction-assisted spectral partitioning.

    Improvements over the original frequency-only partition:
    - lower-threshold peak/shoulder detection,
    - forced swell/wind-sea separation when energy exists on both sides,
    - extra split points where wave direction changes strongly across the spectrum,
    - broad-band directional splits where two systems overlap in period,
    - confidence/method/energy fields for frontend display.
    """
    paired = []
    for i, (f, e) in enumerate(zip(freqs, density)):
        try:
            f = float(f)
            e = float(e)
            if not (math.isfinite(f) and math.isfinite(e)) or f <= 0 or e < 0:
                continue
        except Exception:
            continue

        d1 = directions[i] if directions and i < len(directions) else None
        d2 = directions2[i] if directions2 and i < len(directions2) else None
        r1 = r1_values[i] if r1_values and i < len(r1_values) else None
        r2 = r2_values[i] if r2_values and i < len(r2_values) else None
        paired.append((f, e, d1, d2, r1, r2))

    paired.sort(key=lambda x: x[0])

    if len(paired) < 5:
        return []

    freqs = [p[0] for p in paired]
    density = [p[1] for p in paired]
    dirs = [p[2] for p in paired]
    dirs2 = [p[3] for p in paired]
    r1s = [p[4] for p in paired]
    r2s = [p[5] for p in paired]

    # Prefer alpha1/mean direction; fall back to alpha2 where alpha1 is missing.
    effective_dirs = [
        d1 if d1 is not None else d2
        for d1, d2 in zip(dirs, dirs2)
    ]

    df = _bin_widths(freqs)
    total_m0 = sum(e * w for e, w in zip(density, df))
    if total_m0 <= 0:
        return []

    smooth = _smooth5(density)
    peaks = _find_spectral_peaks_v2(freqs, density, sep_freq)

    split_reasons = {}

    # Split at valleys between spectral peaks.
    for p1, p2 in zip(peaks[:-1], peaks[1:]):
        valley = min(range(p1, p2 + 1), key=lambda i: smooth[i])
        if 0 <= valley < len(freqs) - 1:
            split_reasons[valley] = "peak-valley split"

    # Split at swell/wind-sea separation if both sides contain meaningful energy.
    if sep_freq is not None:
        sep_candidates = [i for i, f in enumerate(freqs[:-1]) if f <= sep_freq < freqs[i + 1]]
        if sep_candidates:
            sep_i = sep_candidates[0]
            left_m0 = sum(density[i] * df[i] for i in range(0, sep_i + 1))
            right_m0 = sum(density[i] * df[i] for i in range(sep_i + 1, len(freqs)))
            if left_m0 / total_m0 >= 0.025 and right_m0 / total_m0 >= 0.025:
                split_reasons[sep_i] = "NOAA swell/wind separation"

    # Add strong adjacent direction shifts.
    for split_i, reason in _candidate_direction_splits(freqs, density, effective_dirs, total_m0).items():
        # Do not overcrowd split points.
        if all(abs(split_i - existing) > 1 for existing in split_reasons):
            split_reasons[split_i] = reason

    # Add broad-band direction splits iteratively.
    for _ in range(2):
        added = False
        for start, end in _make_segments_from_splits(len(freqs), split_reasons):
            candidate = _broad_band_directional_split(start, end, freqs, density, effective_dirs, total_m0)
            if candidate:
                split_i, reason = candidate
                if all(abs(split_i - existing) > 1 for existing in split_reasons):
                    split_reasons[split_i] = reason
                    added = True
        if not added:
            break

    segments = _make_segments_from_splits(len(freqs), split_reasons)

    components = []

    for start, end in segments:
        f_part = freqs[start: end + 1]
        e_part = density[start: end + 1]
        df_part = df[start: end + 1]
        dir_part = effective_dirs[start: end + 1]
        r1_part = r1s[start: end + 1]
        r2_part = r2s[start: end + 1]

        m0 = sum(e * w for e, w in zip(e_part, df_part))
        energy_pct = (m0 / total_m0) * 100.0 if total_m0 > 0 else 0.0
        hs_m = 4.0 * math.sqrt(max(m0, 0.0))
        hs_ft = hs_m * 3.28084

        # Keep smaller components than before, but screen out tiny/noisy partitions.
        if hs_ft < 0.20 and energy_pct < 1.0:
            continue

        local_peak_rel = max(range(len(e_part)), key=lambda i: smooth[start + i])
        peak_idx = start + local_peak_rel
        peak_f = freqs[peak_idx]
        peak_period = 1.0 / peak_f if peak_f else None

        weights = [e * w for e, w in zip(e_part, df_part)]
        mean_dir = _circular_mean_deg(dir_part, weights)
        spread = _directional_spread_deg(dir_part, weights)
        mean_r1 = _weighted_mean(r1_part, weights)
        mean_r2 = _weighted_mean(r2_part, weights)

        if sep_freq is not None:
            comp_type = "swell" if peak_f <= sep_freq else "wind sea"
        else:
            comp_type = "swell" if (peak_period and peak_period >= 8.0) else "wind sea"

        boundary_methods = []
        if start > 0 and (start - 1) in split_reasons:
            boundary_methods.append(split_reasons[start - 1])
        if end in split_reasons:
            boundary_methods.append(split_reasons[end])

        if any("direction" in m.lower() for m in boundary_methods):
            method = "Direction-assisted split"
        elif any("NOAA" in m for m in boundary_methods):
            method = "Swell/wind separation"
        elif any("peak-valley" in m for m in boundary_methods):
            method = "Peak + valley"
        else:
            method = "Dominant spectral peak"

        # Confidence score based on energy share, direction consistency, and directional moment quality.
        confidence_score = 0
        if energy_pct >= 8:
            confidence_score += 2
        elif energy_pct >= 2.5:
            confidence_score += 1

        if spread is not None:
            if spread <= 35:
                confidence_score += 2
            elif spread <= 60:
                confidence_score += 1

        if mean_r1 is not None:
            if mean_r1 >= 0.45:
                confidence_score += 2
            elif mean_r1 >= 0.25:
                confidence_score += 1

        if method == "Direction-assisted split":
            confidence_score += 1

        if confidence_score >= 5:
            confidence = "High"
        elif confidence_score >= 3:
            confidence = "Medium"
        else:
            confidence = "Low"

        components.append({
            "component": len(components) + 1,
            "type": comp_type,
            "height_ft": round(hs_ft, 1),
            "height_m": round(hs_m, 2),
            "peak_period_sec": round(peak_period, 1) if peak_period else None,
            "peak_frequency_hz": round(peak_f, 4),
            "direction_deg": round(mean_dir) if mean_dir is not None else None,
            "direction_compass": _compass_from_degrees(mean_dir),
            "energy_m0": round(m0, 5),
            "energy_percent": round(energy_pct, 1),
            "frequency_min_hz": round(min(f_part), 4),
            "frequency_max_hz": round(max(f_part), 4),
            "confidence": confidence,
            "method": method,
            "directional_spread_deg": round(spread, 1) if spread is not None else None,
            "mean_r1": round(mean_r1, 2) if mean_r1 is not None else None,
            "mean_r2": round(mean_r2, 2) if mean_r2 is not None else None,
        })

    # Sort surf-relevant components by swell/wind type, then longer period, then height.
    components.sort(
        key=lambda c: (
            0 if c["type"] == "swell" else 1,
            -(c["peak_period_sec"] or 0),
            -c["height_ft"]
        )
    )

    # Keep the list readable, but allow more than the old algorithm.
    components = components[:8]

    for idx, c in enumerate(components, start=1):
        c["component"] = idx

    return components

def _match_direction_row(density_row: dict, direction_rows: list) -> dict | None:
    for row in direction_rows:
        if row["timestamp_utc"] == density_row["timestamp_utc"]:
            return row
    return _latest_spectral_row(direction_rows)

@app.route("/api/ndbc/station/<station_id>/components")
def api_ndbc_station_components(station_id):
    station_id = station_id.strip().upper()

    with _CACHE_LOCK:
        cached = NDBC_COMPONENT_CACHE.get(station_id)
    if cached and _cache_valid(cached["timestamp"], ttl=NDBC_COMPONENT_TTL_SECONDS):
        return jsonify(cached["data"])

    station_meta = {
        s["id"]: s for s in get_live_ndbc_wave_stations()
    }.get(station_id, {"id": station_id, "name": station_id})

    try:
        density_text = _fetch_text(f"{NDBC_REALTIME_DIR}{station_id}.data_spec")
    except Exception as exc:
        return jsonify({"station": station_id, "error": f"NDBC file not available: {exc}"}), 404

    # Directional moments. These are optional because not every station has every file.
    optional_files = {}
    for key, suffix in {
        "swdir": "swdir",
        "swdir2": "swdir2",
        "swr1": "swr1",
        "swr2": "swr2",
        "spec": "spec",
    }.items():
        try:
            optional_files[key] = _fetch_text(f"{NDBC_REALTIME_DIR}{station_id}.{suffix}", timeout=20)
        except Exception:
            optional_files[key] = None

    density_rows = _parse_ndbc_spectral_file(density_text)

    if not density_rows:
        return jsonify({"station": station_id, "error": "No spectral rows parsed"}), 404

    density_row = _latest_spectral_row(density_rows)

    def latest_matching_row(file_key: str) -> dict | None:
        rows = _parse_ndbc_spectral_file(optional_files.get(file_key)) if optional_files.get(file_key) else []
        return _match_direction_row(density_row, rows) if rows else None

    swdir_row = latest_matching_row("swdir")
    swdir2_row = latest_matching_row("swdir2")
    swr1_row = latest_matching_row("swr1")
    swr2_row = latest_matching_row("swr2")

    summary_rows = _parse_ndbc_spec_summary_rows(optional_files.get("spec") or "")
    summary_row = None

    for row in summary_rows:
        if row["timestamp_utc"] == density_row["timestamp_utc"]:
            summary_row = row
            break

    if summary_row is None and summary_rows:
        summary_row = summary_rows[0]

    sep_freq, sep_source = _estimate_separation_frequency(summary_row)

    freqs = density_row["freqs"]
    density_vals = density_row["values"]

    directions = _align_spectral_values(swdir_row, freqs)
    directions2 = _align_spectral_values(swdir2_row, freqs)
    r1_values = _align_spectral_values(swr1_row, freqs)
    r2_values = _align_spectral_values(swr2_row, freqs)

    # If the main direction file is missing, fall back to the second direction file.
    if not any(v is not None for v in directions):
        directions = directions2

    components = _partition_spectrum_v2(
        freqs,
        density_vals,
        directions=directions,
        directions2=directions2,
        r1_values=r1_values,
        r2_values=r2_values,
        sep_freq=sep_freq,
    )

    df = _bin_widths(freqs)
    total_m0 = sum(e * w for e, w in zip(density_vals, df))
    total_hs_m = 4.0 * math.sqrt(max(total_m0, 0.0))

    try:
        hst = pytz.timezone("Pacific/Honolulu")
        timestamp_hst = density_row["timestamp_utc"].astimezone(hst).strftime("%Y-%m-%d %I:%M %p HST")
    except Exception:
        timestamp_hst = None

    noaa_summary = None
    if summary_row:
        noaa_summary = {
            "wvht_ft": round(summary_row["wvht_m"] * 3.28084, 1) if summary_row.get("wvht_m") is not None else None,
            "swh_ft": round(summary_row["swh_m"] * 3.28084, 1) if summary_row.get("swh_m") is not None else None,
            "swp_sec": summary_row.get("swp_sec"),
            "swd": summary_row.get("swd"),
            "wwh_ft": round(summary_row["wwh_m"] * 3.28084, 1) if summary_row.get("wwh_m") is not None else None,
            "wwp_sec": summary_row.get("wwp_sec"),
            "wwd": summary_row.get("wwd"),
            "steepness": summary_row.get("steepness"),
            "apd_sec": summary_row.get("apd_sec"),
        }

    result = {
        "station": station_id,
        "name": station_meta.get("name", station_id),
        "lat": station_meta.get("lat"),
        "lon": station_meta.get("lon"),
        "timestamp_utc": density_row["timestamp_utc"].isoformat().replace("+00:00", "Z"),
        "timestamp_hst": timestamp_hst,
        "total_height_ft": round(total_hs_m * 3.28084, 1),
        "total_height_m": round(total_hs_m, 2),
        "sep_frequency_hz": round(sep_freq, 4) if sep_freq else None,
        "sep_period_sec": round(1.0 / sep_freq, 1) if sep_freq else None,
        "separation_source": sep_source,
        "algorithm": "direction-assisted spectral partition v2",
        "algorithm_note": (
            "Experimental components are derived from NDBC spectral density plus directional moment files "
            "(.swdir, .swdir2, .swr1, .swr2). They may reveal additional wave systems beyond NOAA's simplified "
            "swell/wind-wave summary and are not intended to exactly duplicate NOAA SwH/SwP/WWH/WWP."
        ),
        "noaa_summary": noaa_summary,
        "components": components,
        "spectrum": [
            {
                "frequency_hz": round(f, 4),
                "period_sec": round(1.0 / f, 2) if f else None,
                "density_m2_per_hz": round(e, 5),
                "direction_deg": round(directions[i]) if i < len(directions) and directions[i] is not None else None,
                "direction2_deg": round(directions2[i]) if i < len(directions2) and directions2[i] is not None else None,
                "r1": round(r1_values[i], 3) if i < len(r1_values) and r1_values[i] is not None else None,
                "r2": round(r2_values[i], 3) if i < len(r2_values) and r2_values[i] is not None else None,
            }
            for i, (f, e) in enumerate(zip(freqs, density_vals))
        ]
    }

    with _CACHE_LOCK:
        NDBC_COMPONENT_CACHE[station_id] = {"timestamp": time.time(), "data": result}
        _evict_oldest(NDBC_COMPONENT_CACHE, NDBC_COMPONENT_CACHE_MAX)
    return jsonify(result)

NDBC_STATION_PAGE = "https://www.ndbc.noaa.gov/station_page.php?station={station_id}"
NDBC_SPEC_SUMMARY_URL = "https://www.ndbc.noaa.gov/data/realtime2/{station_id}.spec"


def _parse_noaa_station_wave_summary(station_id: str, hours: int = 24) -> dict:
    """
    Read the NDBC realtime .spec wave-summary file and return the same basic
    Wave Summary values shown on the NDBC station page.

    NDBC .spec heights are in meters. The NDBC station page displays English
    units, so heights are converted to feet and rounded to 1 decimal place.
    """
    station_id = station_id.strip().upper()

    station_page_url = NDBC_STATION_PAGE.format(station_id=station_id)
    spec_url = NDBC_SPEC_SUMMARY_URL.format(station_id=station_id)

    spec_text = _fetch_text(spec_url, timeout=30)

    # Try to get station name/coordinates for title and local time conversion.
    station_name = station_id
    lat = None
    lon = None

    try:
        meta = load_station_metadata().get(station_id, {})
        station_name = meta.get("name", station_id)
        lat = meta.get("lat")
        lon = meta.get("lon")
    except Exception:
        pass

    if (lat is None or lon is None) and station_id in DEFAULT_STATIONS:
        fallback = DEFAULT_STATIONS[station_id]
        station_name = fallback.get("name", station_name)
        lat = fallback.get("lat")
        lon = fallback.get("lon")

    tz_name = "UTC"
    if lat is not None and lon is not None:
        tz_name = _safe_tzname_for_latlon(lat, lon)

    try:
        local_tz = pytz.timezone(tz_name)
    except Exception:
        local_tz = pytz.utc

    def height_m_to_ft_str(value: str) -> str:
        value = str(value).strip()
        if value in {"MM", "-", "--", "---", ""}:
            return ""
        try:
            return f"{float(value) * 3.28084:.1f}"
        except Exception:
            return ""

    def passthrough(value: str) -> str:
        value = str(value).strip()
        if value in {"MM", "-", "--", "---"}:
            return ""
        return value

    rows = []

    for raw_line in spec_text.splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        parts = line.split()

        # Expected .spec columns:
        # YY MM DD hh mm WVHT SwH SwP WWH WWP SwD WWD STEEPNESS APD MWD
        if len(parts) < 14:
            continue

        try:
            yy = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            hour = int(parts[3])
            minute = int(parts[4])
        except Exception:
            continue

        year = 2000 + yy if yy < 100 else yy

        try:
            dt_utc = datetime(year, month, day, hour, minute, tzinfo=pytz.utc)
        except Exception:
            continue

        dt_local = dt_utc.astimezone(local_tz)

        date_local = dt_local.strftime("%Y-%m-%d")
        time_local = dt_local.strftime("%I:%M %p").lower()

        row = {
            "_dt_utc": dt_utc,
            "_dt_local": dt_local,
            "date": date_local,
            "time": time_local,

            # Convert meter heights to ft to match the NDBC station-page display.
            "wvht": height_m_to_ft_str(parts[5]),
            "swh": height_m_to_ft_str(parts[6]),
            "swp": passthrough(parts[7]),
            "wwh": height_m_to_ft_str(parts[8]),
            "wwp": passthrough(parts[9]),
            "swd": passthrough(parts[10]),
            "wwd": passthrough(parts[11]),
            "steepness": passthrough(parts[12]),
            "apd": passthrough(parts[13]),
        }

        rows.append(row)

    # NDBC realtime files are usually newest-first, but sort just to be safe.
    rows.sort(key=lambda r: r["_dt_utc"], reverse=True)

    if rows:
        latest_dt = rows[0]["_dt_utc"]
        cutoff = latest_dt - timedelta(hours=hours)

        rows = [
            r for r in rows
            if r["_dt_utc"] >= cutoff
        ]

    latest = {}

    if rows:
        latest_row = rows[0]
        latest = {
            "wvht": latest_row.get("wvht"),
            "swh": latest_row.get("swh"),
            "swp": latest_row.get("swp"),
            "swd": latest_row.get("swd"),
            "wwh": latest_row.get("wwh"),
            "wwp": latest_row.get("wwp"),
            "wwd": latest_row.get("wwd"),
            "steepness": latest_row.get("steepness"),
            "apd": latest_row.get("apd"),
        }

        latest_local = latest_row["_dt_local"]
        latest_utc = latest_row["_dt_utc"]

        local_time = latest_local.strftime("%I:%M %p").lstrip("0").lower()
        local_tz_abbr = latest_local.tzname() or tz_name
        as_of_local = f"as of ({local_time} {local_tz_abbr})"
        as_of_gmt = latest_utc.strftime("%H%M GMT on %m/%d/%Y")
    else:
        as_of_local = None
        as_of_gmt = None

    # Remove internal datetime objects before jsonify.
    for r in rows:
        r.pop("_dt_utc", None)
        r.pop("_dt_local", None)

    return {
        "station": station_id,
        "title": f"Station {station_id} - {station_name}",
        "source_url": station_page_url,
        "data_source_url": spec_url,
        "as_of_local": as_of_local,
        "as_of_gmt": as_of_gmt,
        "unit_system": "Imperial",
        "latest": latest,
        "rows": rows,
        "row_count": len(rows),
        "columns": [
            {"key": "date", "label": "Date"},
            {"key": "time", "label": "Time"},
            {"key": "wvht", "label": "WVHT", "unit": "ft"},
            {"key": "swh", "label": "SwH", "unit": "ft"},
            {"key": "swp", "label": "SwP", "unit": "sec"},
            {"key": "swd", "label": "SwD"},
            {"key": "wwh", "label": "WWH", "unit": "ft"},
            {"key": "wwp", "label": "WWP", "unit": "sec"},
            {"key": "wwd", "label": "WWD"},
            {"key": "steepness", "label": "Steepness"},
            {"key": "apd", "label": "APD", "unit": "sec"},
        ]
    }

@app.route("/api/ndbc/station/<station_id>/wave-summary")
def api_ndbc_station_wave_summary(station_id):
    try:
        return jsonify(_parse_noaa_station_wave_summary(station_id, hours=24))
    except Exception as exc:
        return jsonify({
            "station": station_id,
            "error": str(exc)
        }), 500

if __name__ == "__main__":
    app.run(debug=True)
