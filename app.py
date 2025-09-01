import os
import re
import json
import math
import time
import pytz
import html
import requests
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any

from flask import Flask, jsonify, render_template, request
from timezonefinder import TimezoneFinder

# --------------------------------------------------------------------------------------
# Flask app
# --------------------------------------------------------------------------------------
app = Flask(__name__)

# --------------------------------------------------------------------------------------
# Networking & caching
# --------------------------------------------------------------------------------------
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "wave-app/1.0 (+https://wave-app-clean.onrender.com)"} )

TF = TimezoneFinder()

BULL_TTL_SEC = 15 * 60
BULL_CACHE: Dict[str, Tuple[float, str]] = {}           # key = station, value = (ts, text)
STATION_CACHE: Optional[List[Dict[str, Any]]] = None    # list of {id, name, lat, lon}
STATION_CACHE_TS: float = 0.0
STATION_CACHE_TTL = 12 * 3600

COMMON_TZ = [
    "",  # (Buoy Local)
    "Pacific/Honolulu",
    "America/Anchorage", "America/Los_Angeles", "America/Denver",
    "America/Chicago", "America/New_York",
    "UTC"
]

UNITS = ["US", "SI"]  # US: ft; SI: m

# --------------------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------------------

def _to_float(x: str) -> Optional[float]:
    x = (x or "").strip()
    if not x or x in {"-", ""}:
        return None
    try:
        return float(x)
    except Exception:
        return None

def _ft_to_m(val_ft: Optional[float]) -> Optional[float]:
    if val_ft is None:
        return None
    return val_ft * 0.3048

def _fmt_hhmm_ampm(dt_local: datetime) -> str:
    # e.g., "8/30/25 12:00 AM" or simplified formats used elsewhere
    return dt_local.strftime("%-m/%-d/%y %-I:%M %p") if os.name != "nt" else dt_local.strftime("%m/%d/%y %I:%M %p").lstrip("0").replace(" 0", " ")

def _station_tz_from_latlon(lat: float, lon: float) -> Optional[str]:
    try:
        tz = TF.timezone_at(lat=lat, lon=lon)
        return tz
    except Exception:
        return None

def _now_utc() -> datetime:
    return datetime.utcnow().replace(tzinfo=pytz.UTC)

# --------------------------------------------------------------------------------------
# Station catalog
# --------------------------------------------------------------------------------------

def parse_ndbc_station_table(txt: str) -> List[Dict[str, Any]]:
    """
    Parse NDBC station_table.txt.

    Expected line example (varies):
      41113  28.40 N  80.53 W  ...  Indian River Inlet, FL
    """
    stations: List[Dict[str, Any]] = []
    for raw in txt.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        # Try the common "ID  lat N/S  lon E/W  name..." shape.
        m = re.match(
            r"^([A-Za-z0-9\-]+)\s+([0-9.+-]+)\s*([NS])\s+([0-9.+-]+)\s*([EW])\s+(.*)$",
            line
        )
        if not m:
            continue
        sid, lat, ns, lon, ew, name = m.groups()
        lat = float(lat)
        lon = float(lon)
        if ns.upper() == "S":
            lat = -lat
        if ew.upper() == "W":
            lon = -lon
        stations.append({"id": sid, "name": name.strip(), "lat": lat, "lon": lon})
    return stations

def load_stations() -> List[Dict[str, Any]]:
    global STATION_CACHE, STATION_CACHE_TS
    # In-memory cache with TTL
    if STATION_CACHE and (time.time() - STATION_CACHE_TS) < STATION_CACHE_TTL:
        return STATION_CACHE

    # 1) Look for a local file (keeps your existing repo behavior).
    for path in ("stations.json", "data/stations.json", "static/stations.json"):
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and "stations" in data:
                    STATION_CACHE = data["stations"]
                else:
                    STATION_CACHE = data
                STATION_CACHE_TS = time.time()
                return STATION_CACHE

    # 2) Fall back to NDBC if no local JSON is present.
    try:
        url = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
        r = HTTP.get(url, timeout=25)
        r.raise_for_status()
        STATION_CACHE = parse_ndbc_station_table(r.text)
        STATION_CACHE_TS = time.time()
        return STATION_CACHE
    except Exception:
        # Minimal fallback so the app still renders.
        STATION_CACHE = [
            {"id": "41113", "name": "Cape Canaveral Nearshore", "lat": 28.40, "lon": -80.53},
            {"id": "51001", "name": "NW Hawaii", "lat": 23.36, "lon": -162.22},
        ]
        STATION_CACHE_TS = time.time()
        return STATION_CACHE

def station_meta(sid: str) -> Optional[Dict[str, Any]]:
    for s in load_stations():
        if s.get("id") == sid:
            return s
    return None

# --------------------------------------------------------------------------------------
# NOMADS (GFS Wave BULL) fetch helpers
# --------------------------------------------------------------------------------------

def _candidate_gfs_runs(now_utc: datetime) -> List[Tuple[str, int]]:
    """
    Build a list of candidate (yyyymmdd, HH) cycles to try, newest first.
    """
    cycles = [18, 12, 6, 0]
    out: List[Tuple[str, int]] = []
    day = now_utc.date()
    # consider today and yesterday
    for back in range(0, 3):
        d = day - timedelta(days=back)
        for hh in cycles:
            if back == 0:
                # only use up to the last completed cycle today
                if now_utc.hour < hh:
                    continue
            out.append((d.strftime("%Y%m%d"), hh))
    return out

def fetch_bull_text(sid: str) -> str:
    """
    Get the latest BULL file for station sid from the GFS wave station directory.
    Uses an in-memory TTL cache.
    """
    now = time.time()
    cached = BULL_CACHE.get(sid)
    if cached and (now - cached[0]) < BULL_TTL_SEC:
        return cached[1]

    text: Optional[str] = None
    for ymd, hh in _candidate_gfs_runs(_now_utc()):
        base = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.{ymd}/{hh:02d}/wave/station/bulls.t{hh:02d}z"
        url = f"{base}/gfswave.{sid}.bull"
        try:
            r = HTTP.get(url, timeout=25)
            if r.ok and "Cycle" in r.text and "Location" in r.text:
                text = r.text
                break
        except Exception:
            pass

    if not text:
        raise RuntimeError(f"BULL file not found for {sid}")

    BULL_CACHE[sid] = (now, text)
    return text

# --------------------------------------------------------------------------------------
# Parser for BULL text
# --------------------------------------------------------------------------------------

@dataclass
class ParsedRow:
    dt_local: datetime
    hs: Dict[str, Optional[float]]   # s1..s6 + combined
    tp: Dict[str, Optional[float]]   # s1..s6
    dd: Dict[str, Optional[float]]   # s1..s6

@dataclass
class ParseResult:
    cycle_utc: datetime
    loc_text: str
    tz_name: str
    rows: List[ParsedRow]

def parse_bull(sid: str, tz_override: Optional[str]) -> ParseResult:
    """
    Parse nomads GFS station BULL for a station.
    """
    txt = fetch_bull_text(sid)

    # header: Cycle & Location (lat/lon)
    m_cycle = re.search(r"Cycle\s*:\s*(\d{8})\s+(\d{2})\s*UTC", txt)
    if not m_cycle:
        raise RuntimeError("Cycle not found in BULL file")
    ymd, hh = m_cycle.groups()
    cycle_utc = datetime.strptime(ymd + hh, "%Y%m%d%H").replace(tzinfo=pytz.UTC)

    m_loc = re.search(r"Location\s*:\s*(.+)", txt)
    loc_text = m_loc.group(1).strip() if m_loc else sid

    # Try to get lat/lon from parentheses if present: "41113 (28.40N 80.53W)"
    lat, lon = None, None
    m_ll = re.search(r"\(([-0-9.]+)\s*([NS])\s+([-0-9.]+)\s*([EW])\)", loc_text)
    if m_ll:
        lat_val, ns, lon_val, ew = m_ll.groups()
        lat = float(lat_val)
        if ns.upper() == "S":
            lat = -lat
        lon = float(lon_val)
        if ew.upper() == "W":
            lon = -lon

    # Determine effective timezone name
    guessed_tz = None
    if lat is not None and lon is not None:
        guessed_tz = _station_tz_from_latlon(lat, lon)

    tz_name = (tz_override or "").strip() or guessed_tz or "UTC"
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        tz_name = "UTC"
        tz = pytz.UTC

    # find the start of the data table (look for the header line containing 'Date' and 'Time')
    data_start_idx = None
    lines = txt.splitlines()
    for i, line in enumerate(lines):
        if "Date" in line and "Time" in line and "Swell 1" in line:
            data_start_idx = i + 1
            break
    if data_start_idx is None:
        raise RuntimeError("Data header not found in BULL file")

    rows: List[ParsedRow] = []

    # Parse pipe-delimited rows
    for raw in lines[data_start_idx:]:
        line = raw.strip()
        if not line or set(line) <= {"-", "+"}:
            continue
        if "|" not in line:
            # likely end of table
            break
        parts = [c.strip() for c in line.split("|")]

        # Expect: Date | Time | (3 columns per swell) * 6 | Combined
        # Date example: "Saturday, August 30, 2025"
        # Time example: "8:00 AM"
        try:
            date_str = parts[0]
            time_str = parts[1]
        except Exception:
            continue

        # Build datetime in the *local* (effective) timezone
        try:
            d = datetime.strptime(date_str, "%A, %B %d, %Y")
        except Exception:
            # Defensive: if the month/day format ever appears, try a numeric pattern
            m_md = re.match(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", date_str)
            if not m_md:
                continue
            mo, da, yy = m_md.groups()
            yy = int(yy)
            if yy < 100:
                yy += 2000
            d = datetime(int(yy), int(mo), int(da))

        try:
            t = datetime.strptime(time_str, "%I:%M %p").time()
        except Exception:
            continue

        naive = datetime(d.year, d.month, d.day, t.hour, t.minute)
        dt_local = tz.localize(naive)

        # Extract swell blocks
        # parts[2:] => 6 swells, each Hs | Tp | Dir; then last column = Combined Hs
        hs: Dict[str, Optional[float]] = {}
        tp: Dict[str, Optional[float]] = {}
        dd: Dict[str, Optional[float]] = {}

        # there should be 6 * 3 = 18 numeric cells for swells, then one more for combined
        # but some swells may be blank
        cursor = 2
        for k in range(1, 7):
            hs[f"s{k}"] = _to_float(parts[cursor]) if cursor < len(parts) else None
            tp[f"s{k}"] = _to_float(parts[cursor + 1]) if (cursor + 1) < len(parts) else None
            dd[f"s{k}"] = _to_float(parts[cursor + 2]) if (cursor + 2) < len(parts) else None
            cursor += 3

        combined = _to_float(parts[cursor]) if cursor < len(parts) else None
        hs["combined"] = combined

        rows.append(ParsedRow(dt_local=dt_local, hs=hs, tp=tp, dd=dd))

    # Guard against any accidental backward time jumps (keep strictly non-decreasing)
    fixed: List[ParsedRow] = []
    last_dt = None
    for r in rows:
        if last_dt and r.dt_local < last_dt:
            # Nudge forward to keep continuity (shouldn't normally happen with full dates)
            r = ParsedRow(dt_local=last_dt + timedelta(hours=1), hs=r.hs, tp=r.tp, dd=r.dd)
        fixed.append(r)
        last_dt = r.dt_local

    return ParseResult(cycle_utc=cycle_utc, loc_text=loc_text, tz_name=tz_name, rows=fixed)

# --------------------------------------------------------------------------------------
# Presentation builders
# --------------------------------------------------------------------------------------

def build_graph_payload(parsed: ParseResult, units: str) -> Dict[str, Any]:
    """
    Build the graph JSON payload expected by the template.
    """
    use_ft = (units or "US").upper() == "US"
    labels: List[str] = []
    height = {f"s{k}": [] for k in range(1, 7)}
    height["combined"] = []
    period = {f"s{k}": [] for k in range(1, 7)}
    direction = {f"s{k}": [] for k in range(1, 7)}

    for r in parsed.rows:
        labels.append(_fmt_hhmm_ampm(r.dt_local))
        for k in range(1, 7):
            h = r.hs[f"s{k}"]
            if not use_ft:
                h = _ft_to_m(h)
            height[f"s{k}"].append(h)
            period[f"s{k}"].append(r.tp[f"s{k}"])
            direction[f"s{k}"].append(r.dd[f"s{k}"])
        # combined
        ch = r.hs.get("combined")
        if not use_ft:
            ch = _ft_to_m(ch)
        height["combined"].append(ch)

    payload = {
        "units": "ft" if use_ft else "m",
        "labels": labels,
        "height": height,
        "period": period,
        "direction": direction,
    }
    return payload

def build_table_html(parsed: ParseResult, units: str) -> str:
    """
    Lightweight HTML table (keeps your structure: Date | Time | Swells | Combined).
    """
    use_ft = (units or "US").upper() == "US"
    unit_lbl = "ft" if use_ft else "m"

    def fmt_num(v: Optional[float]) -> str:
        if v is None:
            return ""
        return f"{v:.2f}" if not use_ft else f"{v:.2f}"

    # Header (Cycle / Location / Time Zone)
    head = (
        '<table class="table table-sm table-bordered table-striped">'
        "<thead>"
        "<tr><th colspan='22'>"
        f"Cycle : {html.escape(parsed.cycle_utc.strftime('%Y%m%d %H UTC'))}"
        f" &nbsp; | &nbsp; Location : {html.escape(parsed.loc_text)}"
        f" &nbsp; | &nbsp; Time Zone: {html.escape(parsed.tz_name)}"
        "</th></tr>"
        "<tr>"
        "<th>Date</th><th>Time</th>"
        "<th colspan='3'>Swell 1</th>"
        "<th colspan='3'>Swell 2</th>"
        "<th colspan='3'>Swell 3</th>"
        "<th colspan='3'>Swell 4</th>"
        "<th colspan='3'>Swell 5</th>"
        "<th colspan='3'>Swell 6</th>"
        f"<th>Combined Hs ({unit_lbl})</th>"
        "</tr>"
        "<tr>"
        "<th></th><th></th>"
        f"<th>Hs ({unit_lbl})</th><th>Tp (s)</th><th>Dir (°)</th>"
        f"<th>Hs ({unit_lbl})</th><th>Tp (s)</th><th>Dir (°)</th>"
        f"<th>Hs ({unit_lbl})</th><th>Tp (s)</th><th>Dir (°)</th>"
        f"<th>Hs ({unit_lbl})</th><th>Tp (s)</th><th>Dir (°)</th>"
        f"<th>Hs ({unit_lbl})</th><th>Tp (s)</th><th>Dir (°)</th>"
        f"<th>Hs ({unit_lbl})</th><th>Tp (s)</th><th>Dir (°)</th>"
        f"<th>Hs ({unit_lbl})</th>"
        "</tr>"
        "</thead><tbody>"
    )

    body = []
    for r in parsed.rows:
        dt = r.dt_local
        # Split into Date & Time columns for readability
        date_col = dt.strftime("%A, %B %-d, %Y") if os.name != "nt" else dt.strftime("%A, %B %d, %Y").replace(" 0", " ")
        time_col = dt.strftime("%-I:%M %p") if os.name != "nt" else dt.strftime("%I:%M %p").lstrip("0")
        row = [
            f"<td>{html.escape(date_col)}</td>",
            f"<td>{html.escape(time_col)}</td>",
        ]
        for k in range(1, 7):
            h = r.hs[f"s{k}"]
            if not use_ft:
                h = _ft_to_m(h)
            row.append(f"<td style='text-align:right'>{fmt_num(h)}</td>")
            row.append(f"<td style='text-align:right'>{fmt_num(r.tp[f's{k}'])}</td>")
            row.append(f"<td style='text-align:right'>{fmt_num(r.dd[f's{k}'])}</td>")
        ch = r.hs.get("combined")
        if not use_ft:
            ch = _ft_to_m(ch)
        row.append(f"<td style='text-align:right'>{fmt_num(ch)}</td>")
        body.append("<tr>" + "".join(row) + "</tr>")

    tail = "</tbody></table>"
    return head + "".join(body) + tail

# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------

@app.route("/stations.json")
def stations_json():
    """
    Marker feed for the Leaflet map.
    """
    return jsonify(load_stations())

@app.route("/")
def index():
    # ---- query params
    stations = load_stations()
    station_ids = [s["id"] for s in stations]
    # default to a common nearshore if available, else first
    default_station = "41113" if "41113" in station_ids else (station_ids[0] if station_ids else "41113")
    sid = request.args.get("station", default_station).strip()

    # Timezone override from dropdown (blank means "use buoy local")
    tz_override = (request.args.get("tz") or "").strip() or None

    units = (request.args.get("unit") or "US").upper()
    if units not in UNITS:
        units = "US"

    view = (request.args.get("view") or "Table").title()
    if view not in {"Table", "Graph"}:
        view = "Table"

    error: Optional[str] = None
    table_html = ""
    graph_data: Optional[Dict[str, Any]] = None
    graph_header: Optional[Dict[str, str]] = None

    try:
        parsed = parse_bull(sid, tz_override=tz_override)
        # If user left dropdown on "(Buoy Local)", tz_override is None and parsed.tz_name is the station's local tz
        table_html = build_table_html(parsed, units)
        graph_data = build_graph_payload(parsed, units)

        # Header info for graph view
        graph_header = {
            "cycle": parsed.cycle_utc.strftime("%Y%m%d %H UTC"),
            "location": parsed.loc_text,
            "tz": parsed.tz_name,
        }
        # For older templates that referenced graph_data.header, keep it available:
        graph_data["header"] = graph_header

    except Exception as ex:
        error = f"Unable to load forecast for {sid}: {ex}"

    # Dropdowns
    tz_list = COMMON_TZ
    selected_tz = tz_override or (graph_header["tz"] if graph_header else "")

    # Keep the stations select options (id, name)
    station_options = [(s["id"], s.get("name", s["id"])) for s in stations]

    return render_template(
        "index.html",
        stations=station_options,
        selected_station=sid,
        timezones=tz_list,
        selected_tz=selected_tz,
        units=UNITS,
        selected_unit=units,
        selected_view=view,
        error=error,
        table_html=table_html,
        graph_data=graph_data,
        graph_header=graph_header
    )

# --------------------------------------------------------------------------------------
# Health
# --------------------------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify(ok=True)

# --------------------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------------------
if __name__ == "__main__":
    # For local testing
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), debug=True)
