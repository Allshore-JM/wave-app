from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

from flask import Flask, render_template, request, jsonify, abort

# Time/index helper from your package
from waveapp_v2.wavecore import hourly_series_from_day_hour

# ------------------------------------------------------------------------------
# App & logging
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("waveweb")

BASE_DIR = Path(__file__).resolve().parent
# Search for data files here, in order, to support different layouts:
SEARCH_BASES: Tuple[Path, ...] = (BASE_DIR, BASE_DIR.parent, BASE_DIR.parent.parent)

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))

# ------------------------------------------------------------------------------
# Helpers for data loading
# ------------------------------------------------------------------------------

def _read_json_anywhere(filename: str) -> Any:
    """
    Load JSON from the first location where the file exists among SEARCH_BASES.
    Raises FileNotFoundError if not found anywhere.
    """
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
    Returns a mapping: { station_id: { 'lat': float, 'lon': float } }.

    Accepts either:
      - dict: { "KMLB": {"lat": 28.102, "lon": -80.626}, ... }
      - list: [ {"id": "KMLB", "lat": 28.102, "lon": -80.626}, ... ]
    Also tolerates alternate key names for latitude/longitude.
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
        # Convert list of objects into (id, obj) pairs
        items = []
        for obj in raw:
            if not isinstance(obj, dict):
                continue
            sid = obj.get("id") or obj.get("station") or obj.get("sid")
            if sid:
                items.append((str(sid), obj))
    else:
        logger.warning("Unexpected type for station_coords.json: %s", type(raw))
        return out

    for sid, meta in items:
        if not isinstance(meta, dict):
            continue

        # Accept a range of common latitude/longitude field names
        lat = (
            meta.get("lat")
            or meta.get("latitude")
            or meta.get("lat_dd")
            or meta.get("latDeg")
        )
        lon = (
            meta.get("lon")
            or meta.get("lng")
            or meta.get("longitude")
            or meta.get("lon_dd")
            or meta.get("lonDeg")
        )

        latf, lonf = _safe_float(lat), _safe_float(lon)
        if latf is not None and lonf is not None:
            out[str(sid)] = {"lat": latf, "lon": lonf}

    return out


def get_station_list() -> List[Tuple[str, str]]:
    """
    Returns a list of (station_id, station_name) tuples.

    Accepts either:
      - dict: { "KMLB": {"name": "Melbourne"}, ... }
      - list: [ "KMLB", "KJAX", ... ]  (name falls back to id)
    """
    try:
        raw = _read_json_anywhere("station_list.json")
    except FileNotFoundError as e:
        logger.warning(str(e))
        return []

    if isinstance(raw, dict):
        out: List[Tuple[str, str]] = []
        for sid, meta in raw.items():
            if isinstance(meta, dict):
                name = meta.get("name", str(sid))
            else:
                name = str(sid)
            out.append((str(sid), name))
        return out

    if isinstance(raw, list):
        return [(str(sid), str(sid)) for sid in raw]

    logger.warning("Unexpected type for station_list.json: %s", type(raw))
    return []


# ------------------------------------------------------------------------------
# Routes
# ------------------------------------------------------------------------------

@app.route("/")
def index():
    """
    Render the main page. The template should fetch markers from /api/stations.
    """
    return render_template("index.html")


@app.get("/healthz")
def healthz():
    return "ok", 200


@app.get("/api/stations")
def api_stations():
    """
    Join the station list (id, name) with coords and return a list of objects:
      [{ "id": "...", "name": "...", "lat": ..., "lon": ... }, ...]
    """
    listing = get_station_list()
    coords = load_station_coords()

    rows: List[Dict[str, Any]] = []
    for sid, name in listing:
        c = coords.get(sid)
        if c and "lat" in c and "lon" in c:
            rows.append({"id": sid, "name": name, "lat": c["lat"], "lon": c["lon"]})

    return jsonify(rows)


# Backward compatibility: if the existing frontend fetches /stations.json
@app.get("/stations.json")
def stations_json_alias():
    return api_stations()


@app.get("/api/series")
def api_series():
    """
    Returns a time series with 1‑hour increments, using your helper
    hourly_series_from_day_hour(day_str, start_hour, hours, tz_str).

    Query params:
      - station (optional, forwarded if your helper needs it in the future)
      - start_day (YYYY-MM-DD)   REQUIRED
      - start_hour (int)         default 0
      - hours (int)              default 72
      - tz (str)                 default 'UTC' (e.g., 'America/New_York')
    """
    day = request.args.get("start_day")
    if not day:
        abort(400, description="start_day (YYYY-MM-DD) is required")

    try:
        start_hour = int(request.args.get("start_hour", 0))
        hours = int(request.args.get("hours", 72))
    except (TypeError, ValueError):
        abort(400, description="start_hour and hours must be integers")

    tz = request.args.get("tz", "UTC")
    # NOTE: If your helper needs the station id, you can plumb it through here.
    series = hourly_series_from_day_hour(day, start_hour, hours, tz)
    return jsonify(series)


# ------------------------------------------------------------------------------
# Local dev
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Run locally with: python app.py
    # On Render, gunicorn will serve: waveapp_v2.waveweb.app:app
    app.run(host="0.0.0.0", port=5000, debug=True)
