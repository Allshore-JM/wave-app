from flask import Flask, render_template, request, send_file, jsonify
import pandas as pd  # still used elsewhere if you keep Excel features
import requests
import json
import os
from io import BytesIO
from datetime import datetime, timedelta
import pytz
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from timezonefinder import TimezoneFinder
from calendar import monthrange

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


# Instantiate once
tz_finder = TimezoneFinder()

# Caches
STATION_META = None          # station_id -> {name, lat, lon}
STATION_COORDS = None        # station_id -> {lat, lon}
BULLET_STATIONS = None
stations_data_cache = None

# NOAA URLs
NOAA_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"

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

# ---- PacIOOS SWAN integration ----
import requests
import pytz
import pandas as pd
from datetime import datetime

SWAN_ERDDAP_JSON = "https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu_lon180.json"

def _erddap_time_sel(days_back: int = 10) -> str:
    # 10 days by default; change to suit
    hours = days_back * 24
    return f"[last-{hours}:1:last]"

def fetch_swan_point_timeseries_erddap(lat: float, lon: float, tz_name: str | None, unit: str, days_back: int = 10):
    """
    Pull SWAN (Oʻahu) point time series via ERDDAP JSON at nearest grid point.
    Returns (labels, hs_vals, tp_vals, dir_vals, tz_label, cycle_str, location_str)
    """
    # ERDDAP lon is -180..180 (lon180 dataset), so pass lon directly
    tsel = _erddap_time_sel(days_back)
    depth_sel = "[(0.0)]"  # SWAN has a single surface level
    lat_sel = f"[({lat:.4f}):1:({lat:.4f})]"
    lon_sel = f"[({lon:.4f}):1:({lon:.4f})]"

    # Order of dims: time, depth, latitude, longitude
    query = (
        "time,"
        f"shgt{tsel}{depth_sel}{lat_sel}{lon_sel},"
        f"mper{tsel}{depth_sel}{lat_sel}{lon_sel},"
        f"mdir{tsel}{depth_sel}{lat_sel}{lon_sel}"
    )
    url = f"{SWAN_ERDDAP_JSON}?{query}"

    r = requests.get(url, timeout=30)
    r.raise_for_status()
    js = r.json()
    rows = js.get("table", {}).get("rows", [])
    if not rows:
        return None, None, None, None, None, None, None

    # time to labels in target tz
    target_tz = pytz.timezone(tz_name or "Pacific/Honolulu")
    times_utc = [pd.to_datetime(t, utc=True) for t, *_ in rows]
    labels = [t.astimezone(target_tz).strftime("%-m/%-d/%y %I:%M %p") for t in times_utc]

    # variables
    hs_m   = [float(x) if x is not None else None for _, x, *_ in rows]
    tp_s   = [float(x) if x is not None else None for *_, x, _ in rows]
    dirdeg = [float(x) if x is not None else None for *_, x in rows]

    # units
    if unit.upper() == "US":
        hs_vals = [v * 3.28084 if v is not None else None for v in hs_m]
    else:
        hs_vals = hs_m

    # header strings
    cycle_str = f"Cycle : {times_utc[0].strftime('%Y%m%d %H')} UTC"
    location_str = f"Location : {lat:.2f}N {abs(lon):.2f}{'W' if lon < 0 else 'E'}"

    return labels, hs_vals, tp_s, dirdeg, (tz_name or "Pacific/Honolulu"), cycle_str, location_str


# Minimal starter list. Expand freely (IDs & coords are yours to define).
SWAN_STATIONS_DEFAULT = {
    # Oʻahu (examples around the island)
    "SWAN_HALEIWA":  {"name": "Haleʻiwa (Oʻahu)",  "lat": 21.671, "lon": -158.118, "tz": "Pacific/Honolulu"},
    "SWAN_MAKAPUU":  {"name": "Makapuʻu (Oʻahu)",  "lat": 21.306, "lon": -157.652, "tz": "Pacific/Honolulu"},
    "SWAN_WAIKIKI":  {"name": "Waikīkī (Oʻahu)",   "lat": 21.273, "lon": -157.825, "tz": "Pacific/Honolulu"},
    "SWAN_MAKAHA":   {"name": "Mākaha (Oʻahu)",    "lat": 21.474, "lon": -158.216, "tz": "Pacific/Honolulu"}
}

def load_swan_station_map() -> dict:
    """
    If you later add swan_stations.json (same shape as DEFAULT_STATIONS),
    read it here and fall back to SWAN_STATIONS_DEFAULT if not present.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base_dir, 'swan_stations.json')
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
            # normalize
            out = {}
            for sid, info in data.items():
                out[str(sid).strip()] = {
                    "name": info.get("name", str(sid)),
                    "lat": float(info["lat"]),
                    "lon": float(info["lon"]),
                    "tz": info.get("tz", "Pacific/Honolulu"),
                }
            return out
        except Exception:
            pass
    return SWAN_STATIONS_DEFAULT.copy()

def _lon_to_east(lon_deg):
    """Convert [-180..180] to [0..360] range for SWAN (ERDDAP/THREDDS use degrees_east)."""
    return lon_deg if lon_deg >= 0 else lon_deg + 360.0

# Choose the SWAN island dataset by nearest island center (simple and robust).
_SWAN_ISLANDS = [
    {
        "id": "swan_oahu",
        "label": "Oahu",
        "center": (21.45, -157.95),
        "best_url": "https://pae-paha.pacioos.hawaii.edu/thredds/dodsC/swan_oahu/SWAN_Oahu_Regional_Wave_Model_best.ncd",
        # variables: shgt(m), pper(s), pdir(deg-from). :contentReference[oaicite:1]{index=1}
    },
    {
        "id": "swan_kauai",
        "label": "Kauai",
        "center": (22.05, -159.50),
        "best_url": "https://pae-paha.pacioos.hawaii.edu/thredds/dodsC/swan_kauai/SWAN_Kauai_Regional_Wave_Model_best.ncd",
        # variables as above. :contentReference[oaicite:2]{index=2}
    },
    {
        "id": "swan_maui",
        "label": "Maui",
        "center": (20.90, -156.50),
        "best_url": "https://pae-paha.pacioos.hawaii.edu/thredds/dodsC/swan_maui/SWAN_Maui_Regional_Wave_Model_best.ncd",
        # variables as above. :contentReference[oaicite:3]{index=3}
    },
    {
        "id": "swan_bigi",
        "label": "Big Island",
        "center": (19.70, -155.60),
        "best_url": "https://pae-paha.pacioos.hawaii.edu/thredds/dodsC/swan_bigi/SWAN_Big_Island_Regional_Wave_Model_best.ncd",
        # variables as above. :contentReference[oaicite:4]{index=4}
    },
]

def _nearest_island_for(lat, lon):
    from math import radians, cos, sin, asin, sqrt
    def hav(a,b,c,d):
        # haversine distance in km
        R=6371.0
        dlat=radians(c-a); dlon=radians(d-b)
        a_=sin(dlat/2)**2 + cos(radians(a))*cos(radians(c))*sin(dlon/2)**2
        return 2*R*asin(sqrt(a_))
    best = None
    for isl in _SWAN_ISLANDS:
        d = hav(lat, lon, isl["center"][0], isl["center"][1])
        if best is None or d < best[0]:
            best = (d, isl)
    return best[1]

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
        res = requests.get(station_url, timeout=30)
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

def get_station_list_for_model(model: str) -> list[tuple[str, str]]:
    if (model or "").upper() == "SWAN":
        mp = load_swan_station_map()
        return [(sid, info["name"]) for sid, info in mp.items()]
    # default GFS
    return get_station_list()

def get_stations_data_for_model(model: str):
    if (model or "").upper() == "SWAN":
        mp = load_swan_station_map()
        out = []
        for sid, info in mp.items():
            out.append({
                "id": sid,
                "name": info["name"],
                "lat": info["lat"],
                "lon": info["lon"],
                "tz": info.get("tz", "Pacific/Honolulu"),
            })
        return out
    return get_stations_data()

@app.route('/stations.json')
def stations_json():
    model = (request.args.get("model") or "GFS").upper()
    return jsonify(get_stations_data_for_model(model))

# ----------------------------- NOAA run detection ------------------------------

def get_latest_run():
    """
    Find the most recent available GFS wave run by probing 18/12/06/00 of today and yesterday.
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
                resp = requests.head(test_file, timeout=10)
                if resp.status_code == 200:
                    return yyyymmdd, run_str
            except Exception:
                continue
    return None, None

# --- SWAN via PacIOOS ERDDAP (robust point time series) ---
SWAN_ERDDAP_JSON = "https://pae-paha.pacioos.hawaii.edu/erddap/griddap/swan_oahu_lon180.json"

def _erddap_time_sel(days_back: int | None = None,
                     start_dt_utc: datetime | None = None,
                     end_dt_utc: datetime | None = None) -> str:
    """
    Build the griddap [time] selector. If start/end are given, use them.
    Else use 'last-N' hours to avoid out-of-range requests.
    """
    def iso(dt: datetime) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.utc)
        return dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if start_dt_utc and end_dt_utc:
        return f'[("{iso(start_dt_utc)}"):1:("{iso(end_dt_utc)}")]'
    hours = int((days_back or 10) * 24)  # default ~10 days
    return f"[last-{hours}:1:last]"

def fetch_swan_point_timeseries(lat: float,
                                lon: float,
                                tz_name: str | None,
                                unit: str,
                                days_back: int | None = None,
                                start_dt_utc: datetime | None = None,
                                end_dt_utc: datetime | None = None):
    """
    Get SWAN shgt/mper/mdir at nearest grid point to (lat, lon) using ERDDAP.
    Returns a graph_data dict compatible with your Chart.js view and an optional message.
    """
    # Build selectors
    tsel = _erddap_time_sel(days_back, start_dt_utc, end_dt_utc)
    # ERDDAP will snap to nearest grid cell when you pass a single value in brackets.
    lat_sel = f"[({lat:.4f}):1:({lat:.4f})]"
    lon_sel = f"[({lon:.4f}):1:({lon:.4f})]"

    query = (
        "time,"
        f"shgt{tsel}{lat_sel}{lon_sel},"
        f"mper{tsel}{lat_sel}{lon_sel},"
        f"mdir{tsel}{lat_sel}{lon_sel}"
    )
    url = f"{SWAN_ERDDAP_JSON}?{query}"

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    js = resp.json()
    rows = js.get("table", {}).get("rows", [])
    if not rows:
        return None, "No SWAN data was returned for that point."

    # Parse rows -> arrays
    times_utc, hs_m, tp_s, dir_deg = [], [], [], []
    for t_iso, hs, mper, mdir in rows:
        times_utc.append(pd.to_datetime(t_iso, utc=True))
        hs_m.append(float(hs) if hs is not None else None)
        tp_s.append(float(mper) if mper is not None else None)
        dir_deg.append(float(mdir) if mdir is not None else None)

    # Labels in target time zone (buoy-local or selected)
    try:
        target_tz = pytz.timezone(tz_name or "UTC")
    except Exception:
        target_tz = pytz.utc
    labels = [dt.astimezone(target_tz).strftime("%-m/%-d/%y %I:%M %p") for dt in times_utc]

    # Units
    if unit.upper() == "US":
        M2FT = 3.28084
        hs_vals = [v * M2FT if v is not None else None for v in hs_m]
        height_units = "ft"
    else:
        hs_vals = hs_m
        height_units = "m"

    # Build graph_data in the same shape your front-end expects
    nulls = [None] * len(labels)
    graph_data = {
        "header": {
            "cycle": "SWAN (last available)",
            "location": f"{lat:.2f}N {abs(lon):.2f}{'W' if lon < 0 else 'E'}",
            "tz": tz_name or "UTC",
        },
        "labels": labels,
        "height": {
            "s1": nulls, "s2": nulls, "s3": nulls, "s4": nulls, "s5": nulls, "s6": nulls,
            "combined": hs_vals, "units": height_units,
        },
        "period": {
            "s1": nulls, "s2": nulls, "s3": nulls, "s4": nulls, "s5": nulls, "s6": nulls,
            "combined": tp_s, "units": "s",
        },
        "direction": {
            "s1": nulls, "s2": nulls, "s3": nulls, "s4": nulls, "s5": nulls, "s6": nulls,
            "combined": dir_deg, "units": "deg",
        },
    }
    return graph_data, None


# ----------------------------- Bulletin parser ---------------------------------

def _safe_tzname_for_latlon(lat, lon):
    try:
        name = tz_finder.timezone_at(lat=lat, lng=lon)
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
    """
    Fetch and parse NOAA GFS .bull for a station.
    Returns a 6‑tuple:
      (cycle_str, location_str, model_run_str, rows, tz_name, error)
    rows schema per row:
      [date_str, time_str,
       s1_hs_ft, s1_tp_s, s1_dir_deg,  s2_hs_ft, s2_tp_s, s2_dir_deg,  ... s6_* ...,
       combined_hs_ft]
    """
    date_str, run_str = get_latest_run()
    if not date_str:
        return None, None, None, None, 'UTC', "No recent run found."

    bull_url = f"{NOAA_BASE}/gfs.{date_str}/{run_str}/wave/station/bulls.t{run_str}z/gfswave.{station_id}.bull"
    try:
        resp = requests.get(bull_url, timeout=15)
    except Exception as e:
        return None, None, None, None, 'UTC', f"Could not download .bull for {station_id}: {e}"
    if resp.status_code != 200 or not resp.text:
        return None, None, None, None, 'UTC', f"No .bull file found for {station_id}"

    lines = resp.text.splitlines()

    # --- Headers ---
    cycle_line = next((l for l in lines if l.lower().strip().startswith("cycle")), lines[0] if lines else "")
    location_line = next((l for l in lines if l.lower().strip().startswith("location")), lines[1] if len(lines) > 1 else "")
    cycle_str = cycle_line.strip()
    location_str = location_line.strip()

    # --- Resolve time zone from location (fallback to requested tz, then UTC) ---
    lat, lon = _parse_header_coords(location_str)
    tz_name_from_loc = _safe_tzname_for_latlon(lat, lon) if (lat is not None and lon is not None) else 'UTC'
    effective_tz_name = tz_name_from_loc
    if target_tz_name:
        try:
            _ = pytz.timezone(target_tz_name)
            effective_tz_name = target_tz_name
        except Exception:
            pass

    # --- Decide format: new "day & hour" vs old "Hr" table ---
    uses_day_hour_format = any("day &" in line.lower() for line in lines[:10])

    rows: list[list] = []
    model_run_str = None

    if uses_day_hour_format:
        # Cycle datetime (UTC) for this run
        import re
        m = re.search(r"(\d{8})\s*(\d{2})", cycle_str)
        cycle_date_str = date_str
        cycle_hour_str = run_str
        if m:
            cycle_date_str = m.group(1)
            cycle_hour_str = m.group(2)
        cycle_dt_utc = datetime.strptime(f"{cycle_date_str} {cycle_hour_str}", "%Y%m%d %H")

        # Pretty model run string in the *effective* time zone
        model_run_local = cycle_dt_utc.replace(tzinfo=UTC).astimezone(pytz.timezone(effective_tz_name))
        try:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %-d, %Y %I:%M %p")
        except Exception:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %d, %Y %I:%M %p").lstrip('0')

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

            # first cell -> "day hour"
            day_hour = parts[0].split()
            if len(day_hour) < 2:
                continue
            try:
                day_val = int(day_hour[0])
                hour_val = int(day_hour[1])
            except ValueError:
                continue

            # Combined sea Hs (m) is second cell, first token
            combined_hs_m = None
            first = parts[1].split()
            if first:
                tok = first[0].replace('*', '')
                try:
                    combined_hs_m = float(tok)
                except ValueError:
                    combined_hs_m = None

            # 6 swell groups (hs[m], tp[s], dir[deg]), may be missing
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
                        hs_m = float(toks[0])
                        tp   = float(toks[1])
                        dr   = int(round((float(toks[2]) + 180) % 360))  # convert "to" vs "from"
                        swell_groups.append((hs_m, tp, dr))
                    except Exception:
                        swell_groups.append((None, None, None))

            while len(swell_groups) < 6:
                swell_groups.append((None, None, None))
            swell_groups = swell_groups[:6]

            # Build an increasing UTC timestamp for each row
            forecast_dt_utc = _resolve_day_hour_ts(cycle_dt_utc, day_val, hour_val, last_dt_utc)
            last_dt_utc = forecast_dt_utc

            # Local strings
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

            # Assemble row
            row = [date_str_local, time_str_local]
            for hs_m, tp_val, dir_val in swell_groups:
                if hs_m is None:
                    row.extend([None, None, None])
                else:
                    row.extend([hs_m * M_TO_FT, tp_val, dir_val])

            combined_hs_ft = None if combined_hs_m is None else combined_hs_m * M_TO_FT
            row.append(combined_hs_ft)
            rows.append(row)

    else:
        # Old "Hr" format
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

            # 6 groups, but data columns may include blanks; walk tokens defensively
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

    # --- Round data consistently ---
    for r in rows:
        idx_num = 2
        for _ in range(6):
            if r[idx_num] is not None:       # Hs
                r[idx_num] = round(r[idx_num], 2)
            idx_num += 1
            if r[idx_num] is not None:       # Tp
                r[idx_num] = round(r[idx_num], 1)
            idx_num += 1
            if r[idx_num] is not None:       # Dir
                try:
                    r[idx_num] = int(round(r[idx_num]))
                except Exception:
                    pass
            idx_num += 1
        if r[-1] is not None:                # Combined
            r[-1] = round(r[-1], 2)

    # --- Done ---
    return cycle_str, location_str, model_run_str, rows, effective_tz_name, None


def parse_swan(station_id: str, target_tz_name: str | None = None):
    """
    SWAN via PacIOOS ERDDAP JSON (Oʻahu).
    Returns the same 6-tuple shape as parse_bull:
      (cycle_str, location_str, model_run_str, rows, effective_tz_name, error)

    rows schema per row:
      [date_str, time_str,
       s1_hs_ft, s1_tp_s, s1_dir_deg,  s2_* ... s6_* (all None for SWAN),
       combined_hs_ft]
    """
    swan_map = load_swan_station_map()
    st = swan_map.get(str(station_id))
    if not st:
        return None, None, None, None, "Pacific/Honolulu", f"Unknown SWAN station {station_id}"

    lat = float(st["lat"])
    lon = float(st["lon"])
    tz_for_station = target_tz_name or st.get("tz") or "Pacific/Honolulu"

    try:
        labels, hs_vals, tp_vals, dirdeg, tz_label, cycle_str, location_coord_str = \
            fetch_swan_point_timeseries_erddap(lat, lon, tz_for_station, unit="US", days_back=10)
        if labels is None:
            return None, None, None, None, tz_for_station, "No SWAN data returned for that point."
    except Exception as e:
        return None, None, None, None, tz_for_station, f"Could not open SWAN dataset: {e}"

    # Build rows: leave S1..S6 empty (SWAN provides combined sea state),
    # put the SWAN Hs into the Combined column (in feet; table will convert to meters if needed).
    rows = []
    for i, lbl in enumerate(labels):
        if " " in lbl:
            date_cell, time_cell = lbl.split(" ", 1)
        else:
            date_cell, time_cell = lbl, ""
        row = [date_cell, time_cell]
        for _ in range(6):
            row.extend([None, None, None])  # S1..S6
        comb = hs_vals[i] if hs_vals is not None else None
        row.append(comb)                     # Combined
        rows.append(row)

    # Round like parse_bull
    for r in rows:
        idx = 2
        for _ in range(6):
            if r[idx] is not None: r[idx] = round(r[idx], 2)  # Hs
            idx += 1
            if r[idx] is not None: r[idx] = round(r[idx], 1)  # Tp
            idx += 1
            if r[idx] is not None:
                try: r[idx] = int(round(r[idx]))              # Dir
                except Exception: pass
            idx += 1
        if r[-1] is not None: r[-1] = round(r[-1], 2)         # Combined

    # Headers to match your table style
    ns = "N" if lat >= 0 else "S"
    ew = "E" if lon >= 0 else "W"
    location_str = f"Location : {station_id} ({abs(lat):.2f}{ns} {abs(lon):.2f}{ew})"
    model_run_str = "PacIOOS SWAN (ERDDAP)"

    return cycle_str, location_str, model_run_str, rows, tz_label, None

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

    total_cols = 2 + len(group_colors) * 3 + 1
    html = '<table class="table table-bordered table-sm">\n'
    html += f'<tr><td colspan="{total_cols}"><strong>{cycle_str}</strong></td></tr>\n'
    html += f'<tr><td colspan="{total_cols}"><strong>{location_str}</strong></td></tr>\n'
    html += f'<tr><td colspan="{total_cols}"><strong>Time Zone: {tz_label}</strong></td></tr>\n'

    # headers
    html += '<tr>'
    html += '<th rowspan="2">Date</th><th rowspan="2">Time</th>'
    for idx, col in enumerate(group_colors, start=1):
        html += f'<th colspan="3" style="background-color:{col["header"]}; color:white; text-align:center;">Swell {idx}</th>'
    html += f'<th style="background-color:{combined_colors["header"]}; color:white; text-align:center;">Combined</th>'
    html += '</tr>\n'

    # subheaders
    hs_unit_label = '(ft)' if unit == 'US' else '(m)'
    html += '<tr>'
    for col in group_colors:
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Hs<br>{hs_unit_label}</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Tp<br>(s)</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Dir<br>(d)</th>'
    html += f'<th style="background-color:{combined_colors["subheader"]}; text-align:center;">Hs<br>{hs_unit_label}</th>'
    html += '</tr>\n'

    # rows
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

    html += '</table>'
    return html

# ------------------------------ Flask routes -----------------------------------

@app.route("/", methods=["GET", "POST"])
def index():
    timezones = sorted(pytz.common_timezones)
    unit_options = ["US", "Metric"]
    model_options = ["GFS", "SWAN"]

    # pull selections (GET or POST)
    selected_view  = (request.values.get("view")  or "Table")
    selected_unit  = (request.values.get("unit")  or "US")
    selected_tz    = (request.values.get("tz")    or "")
    selected_model = (request.values.get("model") or "GFS").upper()
    selected_station = (request.values.get("station") or "")

    # default station (keep your old default for GFS; pick one for SWAN)
    if not selected_station:
        selected_station = "51201" if selected_model == "GFS" else "SWAN_HALEIWA"

    # Get the correct station list for the dropdown
    stations = get_station_list_for_model(selected_model)

    table_html = None
    error = None
    tz_label = ""
    selected_lat = None
    selected_lon = None
    graph_data = None
    graph_header = None  # (cycle, location, tz)

    if selected_station:
        if selected_model == "SWAN":
            # Use the SWAN parser to produce rows in the SAME shape as GFS, then
            # flow through the same table/graph pipeline (no early return).
            cycle_str, location_str, model_run_str, rows, effective_tz_name, parse_error = parse_swan(
                selected_station, selected_tz or None
            )
        else:
            cycle_str, location_str, model_run_str, rows, effective_tz_name, parse_error = parse_bull(
                selected_station, selected_tz or None
            )

        error = parse_error
        if rows is not None:
            tz_label = effective_tz_name
            table_html = build_html_table(cycle_str, location_str, model_run_str, rows, tz_label, selected_unit)

            # for single marker focus: look up lat/lon in the right list
            if selected_model == "SWAN":
                mp = load_swan_station_map()
                st = mp.get(str(selected_station))
                if st:
                    selected_lat, selected_lon = st["lat"], st["lon"]
            else:
                coords_map = load_station_coords()
                sid_str = str(selected_station).strip()
                if sid_str in coords_map:
                    selected_lat = coords_map[sid_str]['lat']
                    selected_lon = coords_map[sid_str]['lon']

            # ----- pack graph data (unchanged) -----
            labels = [f"{r[0]} {r[1]}" for r in rows]
            def pick(array_index):
                return [r[array_index] for r in rows]

            def hs_idx(g): return 2 + g*3
            def tp_idx(g): return 3 + g*3
            def dr_idx(g): return 4 + g*3

            height = {
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

            graph_data = {
                "labels": labels,
                "height": height,
                "period": period,
                "direction": direction,
                "units": "ft" if selected_unit == "US" else "m",
                "header": {
                    "cycle": cycle_str.replace("Cycle :", "").strip(),
                    "location": location_str.replace("Location :", "").strip(),
                    "tz": tz_label
                }
            }
            graph_header = graph_data["header"]


    return render_template(
        "index.html",
        stations=stations,
        timezones=timezones,
        units=unit_options,
        models=model_options,          # <-- stays; template needs this
        selected_station=selected_station,
        selected_tz=selected_tz,
        selected_unit=selected_unit,
        selected_view=selected_view,
        selected_model=selected_model, # <-- stays; template needs this
        table_html=table_html,
        graph_data=graph_data,
        graph_header=graph_header,
        error=error
    )



if __name__ == "__main__":
    app.run(debug=True)
