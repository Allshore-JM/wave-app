from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from flask import Flask, abort, jsonify, render_template, request
from timezonefinder import TimezoneFinder

# ------------------------------------------------------------------------------
# App & logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("wave-app")

BASE_DIR = Path(__file__).resolve().parent
SEARCH_BASES: Tuple[Path, ...] = (BASE_DIR, BASE_DIR.parent, BASE_DIR.parent.parent)

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))
_TF = TimezoneFinder()

# ------------------------------------------------------------------------------
# Helpers to read local JSON
# ------------------------------------------------------------------------------

def _read_json_anywhere(filename: str) -> Any:
    for base in SEARCH_BASES:
        p = base / filename
        if p.exists():
            log.info("Loading %s", p)
            with p.open("r") as f:
                return json.load(f)
    return None  # be tolerant; return empty later


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
    raw = _read_json_anywhere("station_coords.json")
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
    raw = _read_json_anywhere("station_list.json")
    if isinstance(raw, dict):
        out: List[Tuple[str, str]] = []
        for sid, meta in raw.items():
            name = meta.get("name", str(sid)) if isinstance(meta, dict) else str(sid)
            out.append((str(sid), name))
        return out
    if isinstance(raw, list):
        return [(str(sid), str(sid)) for sid in raw]
    return []

# ------------------------------------------------------------------------------
# Time / series helpers
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
    """Return YYYY-MM-DD start day; default to 'today' in the given timezone."""
    if day_str:
        try:
            datetime.strptime(day_str, "%Y-%m-%d")
            return day_str
        except ValueError:
            abort(400, description="start_day must be YYYY-MM-DD")
    now_local = datetime.now(pytz.timezone(tz_name))
    return now_local.strftime("%Y-%m-%d")


def _safe_int(val: Any, default: int) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


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
    y, m, d = [int(x) for x in start_day.split("-")]
    cur = tz.localize(datetime(y, m, d, start_hour, 0, 0))

    rows: List[Dict[str, Any]] = []
    for i in range(hours):
        cur = tz.normalize(cur)  # handles DST skips/repeats
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


def rows_as_table(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    headers = ["#", "Local", "TZ", "UTC", "UTC offset (min)"]
    data = [[r["i"], r["local"], r["tz_abbr"], r["utc"], r["utc_offset_min"]] for r in rows]
    return {"headers": headers, "rows": data}

# ------------------------------------------------------------------------------
# Request logging & no-cache (helps when validating in Render)
# ------------------------------------------------------------------------------

@app.before_request
def _log_request():
    log.info("REQ %s %s qs=%s", request.method, request.path, dict(request.args))


@app.after_request
def _no_cache(resp):
    resp.headers["Cache-Control"] = "no-store"
    return resp

# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.route("/")
def index():
    # Your existing template should render the map. If you use a SPA, keep this.
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

# Back-compat for some UIs
@app.get("/stations.json")
def stations_json_alias():
    return api_stations()

# --- Core generator -----------------------------------------------------------

def _serve_series_for(station_id: str,
                      start_day_q: Optional[str],
                      start_hour_q: Optional[str],
                      hours_q: Optional[str],
                      tz_q: Optional[str]):

    tz_name = tz_q or tz_for_station(station_id)
    start_hour = _safe_int(start_hour_q, 0)
    hours = _safe_int(hours_q, 72)
    start_day = _parse_start_day(start_day_q, tz_name)

    rows = build_hourly_rows(start_day, start_hour, hours, tz_name)

    # Respond with both shapes: dict-per-row and table
    payload = {
        "station": str(station_id),
        "tz": tz_name,
        "rows": rows,
        "table": rows_as_table(rows),
    }
    return jsonify(payload)

# --- Primary endpoint (recommended) ------------------------------------------

@app.get("/api/station/<station_id>/series")
def api_station_series(station_id: str):
    return _serve_series_for(
        station_id=station_id,
        start_day_q=request.args.get("start_day") or request.args.get("start_date") or request.args.get("day"),
        start_hour_q=request.args.get("start_hour") or request.args.get("hour") or request.args.get("start_hr"),
        hours_q=request.args.get("hours") or request.args.get("len") or request.args.get("count"),
        tz_q=request.args.get("tz"),
    )

# --- Compatibility endpoints (to catch legacy UI calls) -----------------------

# Querystring-based
@app.get("/api/data")
@app.get("/getData")
@app.get("/get_data")
@app.get("/getBuoyData")
@app.get("/get_buoy_data")
@app.get("/series")
@app.get("/timeseries")
def api_data_aliases():
    station_id = (
        request.args.get("station")
        or request.args.get("id")
        or request.args.get("sid")
    )
    if not station_id:
        abort(400, description="station is required")
    return api_station_series(station_id)

# Path-style
@app.get("/buoy/data")
@app.get("/buoy/<station_id>/data")
@app.get("/station/<station_id>/data")
def api_path_aliases(station_id: Optional[str] = None):
    sid = station_id or request.args.get("station")
    if not sid:
        abort(400, description="station is required")
    return api_station_series(sid)

# Existing series route (kept, with auto-tz if station is provided)
@app.get("/api/series")
def api_series():
    station_id = request.args.get("station")
    if station_id:
        return api_station_series(station_id)

    tz_name = request.args.get("tz", "UTC")
    start_hour = _safe_int(request.args.get("start_hour"), 0)
    hours = _safe_int(request.args.get("hours"), 72)
    start_day = _parse_start_day(request.args.get("start_day"), tz_name)
    rows = build_hourly_rows(start_day, start_hour, hours, tz_name)
    return jsonify({"station": None, "tz": tz_name, "rows": rows, "table": rows_as_table(rows)})

# Quick helper to see which TZ a station maps to
@app.get("/api/timezone/<station_id>")
def api_station_timezone(station_id: str):
    return jsonify({"station": station_id, "tz": tz_for_station(station_id)})

# API-focused 404 so Network tab points you to a working URL immediately
@app.errorhandler(404)
def _api_404(e):
    if request.path.startswith("/api") or request.path.startswith("/get") or request.path.startswith("/buoy"):
        return jsonify({
            "error": "not_found",
            "path": request.path,
            "hint": "Use /api/station/<id>/series (preferred) or /api/data?station=<id>. "
                    "Compat routes also exist: /getData, /get_data, /getBuoyData, /get_buoy_data, "
                    "/series, /timeseries, /buoy/<id>/data, /station/<id>/data."
        }), 404
    return e
# ------------------------------------------------------------------------------
# Local dev
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Render uses gunicorn; this is for local testing
    app.run(host="0.0.0.0", port=5000, debug=True)
