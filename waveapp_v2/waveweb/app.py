from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from flask import Flask, abort, jsonify, render_template, request
from timezonefinder import TimezoneFinder

# If you later want to plug in your package's helper, keep this import:
# from waveapp_v2.wavecore import hourly_series_from_day_hour

# ------------------------------------------------------------------------------
# App & logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("waveweb")

BASE_DIR = Path(__file__).resolve().parent
SEARCH_BASES: Tuple[Path, ...] = (BASE_DIR, BASE_DIR.parent, BASE_DIR.parent.parent)

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))

# One process-wide finder; re-use to avoid disk reads per request
_TF = TimezoneFinder()

# ------------------------------------------------------------------------------
# JSON helpers
# ------------------------------------------------------------------------------

def _read_json_anywhere(filename: str) -> Any:
    for base in SEARCH_BASES:
        path = base / filename
        if path.exists():
            logger.info("Loading %s", path)
            with path.open("r") as f:
                return json.load(f)
    raise FileNotFoundError(f"{filename} not found in any of: {', '.join(str(b) for b in SEARCH_BASES)}")


def _safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load_station_coords() -> Dict[str, Dict[str, float]]:
    """
    Returns: { station_id: { 'lat': float, 'lon': float } }
    Accepts dict or list forms.
    """
    try:
        raw = _read_json_anywhere("station_coords.json")
    except FileNotFoundError as e:
        logger.warning(str(e))
        return {}

    out: Dict[str, Dict[str, float]] = {}
    if isinstance(raw, dict):
        items = list(raw.items())
    elif isinstance(raw, list):
        items = []
        for obj in raw:
            if isinstance(obj, dict):
                sid = obj.get("id") or obj.get("station") or obj.get("sid")
                if sid:
                    items.append((str(sid), obj))
    else:
        logger.warning("Unexpected type for station_coords.json: %s", type(raw))
        return out

    for sid, meta in items:
        if not isinstance(meta, dict):
            continue
        lat = meta.get("lat") or meta.get("latitude") or meta.get("lat_dd") or meta.get("latDeg")
        lon = meta.get("lon") or meta.get("lng") or meta.get("longitude") or meta.get("lon_dd") or meta.get("lonDeg")
        latf, lonf = _safe_float(lat), _safe_float(lon)
        if latf is not None and lonf is not None:
            out[str(sid)] = {"lat": latf, "lon": lonf}
    return out


def get_station_list() -> List[Tuple[str, str]]:
    """
    Returns a list of (station_id, station_name) tuples.
    Accepts dict or list forms.
    """
    try:
        raw = _read_json_anywhere("station_list.json")
    except FileNotFoundError as e:
        logger.warning(str(e))
        return []

    if isinstance(raw, dict):
        out: List[Tuple[str, str]] = []
        for sid, meta in raw.items():
            name = meta.get("name", str(sid)) if isinstance(meta, dict) else str(sid)
            out.append((str(sid), name))
        return out

    if isinstance(raw, list):
        return [(str(sid), str(sid)) for sid in raw]

    logger.warning("Unexpected type for station_list.json: %s", type(raw))
    return []

# ------------------------------------------------------------------------------
# Timezone & series helpers
# ------------------------------------------------------------------------------

def tz_for_station(station_id: str) -> str:
    """Infer IANA timezone from station coordinates; default UTC if unknown."""
    coords = load_station_coords()
    c = coords.get(str(station_id))
    if not c:
        return "UTC"
    tz_name = _TF.timezone_at(lng=c["lon"], lat=c["lat"])
    return tz_name or "UTC"


def _parse_start_day(day_str: Optional[str], tz_name: str) -> str:
    """Return YYYY-MM-DD start day; default to 'today' in the station's timezone."""
    if day_str:
        try:
            datetime.strptime(day_str, "%Y-%m-%d")
            return day_str
        except ValueError:
            abort(400, description="start_day must be YYYY-MM-DD")
    # today in station tz
    now_local = datetime.now(pytz.timezone(tz_name))
    return now_local.strftime("%Y-%m-%d")


def build_hourly_rows(start_day: str, start_hour: int, hours: int, tz_name: str) -> List[Dict[str, Any]]:
    """
    Create DST-safe 1-hour increments in the given timezone.
    Returns a list of rows suitable for table rendering.
    """
    if hours < 1 or hours > 2000:
        abort(400, description="hours must be between 1 and 2000")
    if not (0 <= start_hour <= 23):
        abort(400, description="start_hour must be 0..23")

    tz = pytz.timezone(tz_name)
    # Start at local midnight + start_hour in the station's timezone
    y, m, d = [int(x) for x in start_day.split("-")]
    local_dt = tz.localize(datetime(y, m, d, start_hour, 0, 0))

    rows: List[Dict[str, Any]] = []
    cur = local_dt
    for i in range(hours):
        # Normalize handles DST skips/repeats
        cur = tz.normalize(cur)
        utc_dt = cur.astimezone(pytz.UTC)
        rows.append({
            "i": i,
            "local": cur.isoformat(),
            "tz_abbr": cur.tzname(),
            "utc": utc_dt.isoformat(),
            "utc_offset_min": int((cur.utcoffset() or timedelta(0)).total_seconds() // 60),
        })
        cur = cur + timedelta(hours=1)
    return rows

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")

@app.get("/healthz")
def healthz():
    return "ok", 200

@app.get("/api/stations")
def api_stations():
    listing = get_station_list()
    coords = load_station_coords()
    rows: List[Dict[str, Any]] = []
    for sid, name in listing:
        c = coords.get(sid)
        if c:
            rows.append({"id": sid, "name": name, "lat": c["lat"], "lon": c["lon"]})
    return jsonify(rows)

# Back-compat for UIs still fetching this
@app.get("/stations.json")
def stations_json_alias():
    return api_stations()

# ---- NEW: station-scoped series (recommended) --------------------------------

@app.get("/api/station/<station_id>/series")
def api_station_series(station_id: str):
    tz_name = tz_for_station(station_id)
    try:
        start_hour = int(request.args.get("start_hour", 0))
        hours = int(request.args.get("hours", 72))
    except (TypeError, ValueError):
        abort(400, description="start_hour and hours must be integers")
    start_day = _parse_start_day(request.args.get("start_day"), tz_name)

    # If you later want to switch to your package helper, do it here:
    # rows = hourly_series_from_day_hour(start_day, start_hour, hours, tz_name)
    rows = build_hourly_rows(start_day, start_hour, hours, tz_name)

    payload = {
        "station": str(station_id),
        "tz": tz_name,
        "rows": rows
    }
    return jsonify(payload)

# ---- Compat: querystring variant ---------------------------------------------

@app.get("/api/data")
def api_data():
    """
    Compatibility endpoint: /api/data?station=41009&start_day=YYYY-MM-DD&start_hour=0&hours=72
    """
    station_id = request.args.get("station") or request.args.get("id")
    if not station_id:
        abort(400, description="station is required")
    return api_station_series(station_id)

# ---- Existing route kept (now auto-infers tz if station provided) ------------

@app.get("/api/series")
def api_series():
    """
    Returns a series using 1-hour increments. If a station is provided,
    timezone is detected from its coordinates automatically.
    Query:
      - station (optional) e.g. 41009
      - start_day (YYYY-MM-DD)
      - start_hour (int) default 0
      - hours (int) default 72
      - tz (str) OPTIONAL; ignored if station is given
    """
    station_id = request.args.get("station")
    if station_id:
        return api_station_series(station_id)

    try:
        start_hour = int(request.args.get("start_hour", 0))
        hours = int(request.args.get("hours", 72))
    except (TypeError, ValueError):
        abort(400, description="start_hour and hours must be integers")
    tz_name = request.args.get("tz", "UTC")
    start_day = _parse_start_day(request.args.get("start_day"), tz_name)
    rows = build_hourly_rows(start_day, start_hour, hours, tz_name)
    return jsonify({"station": None, "tz": tz_name, "rows": rows})

# ---- Quick debugging route ---------------------------------------------------

@app.get("/api/timezone/<station_id>")
def api_station_timezone(station_id: str):
    return jsonify({"station": station_id, "tz": tz_for_station(station_id)})

# ------------------------------------------------------------------------------
# Local dev
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
