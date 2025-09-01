import os
import re
import io
import json
import math
import time
import pytz
import gzip
import html
import typing as t
import requests
from datetime import datetime, timedelta, timezone
from functools import lru_cache

from flask import Flask, request, render_template, send_from_directory, jsonify

import pandas as pd
from bs4 import BeautifulSoup
from timezonefinder import TimezoneFinder

app = Flask(__name__)

# ---------------------------------------------------------------------
# Station list (id, name, lat, lon) loader & simple caches
# ---------------------------------------------------------------------

STATIONS_CSV_URL = "https://www.ndbc.noaa.gov/ndbc_assets/stations/station_table.txt"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "wave-app/1.0"})

# One global TimezoneFinder instance + cache for fast lookups
tz_finder = TimezoneFinder(in_memory=True)

# Cache for per-coordinate timezones so we can quickly assign a local tz to each station.
_STATION_TZ_CACHE: dict[tuple[float, float], str] = {}
def tz_for_latlon(lat: float, lon: float) -> str:
    """Return an IANA time zone name for a lat/lon. Falls back to nearest or UTC."""
    key = (round(float(lat), 4), round(float(lon), 4))
    if key in _STATION_TZ_CACHE:
        return _STATION_TZ_CACHE[key]
    tz = tz_finder.timezone_at(lat=float(lat), lng=float(lon))
    if not tz:
        # Fallback to nearest timezone; works well offshore.
        try:
            tz = tz_finder.closest_timezone_at(lat=float(lat), lng=float(lon))
        except Exception:
            tz = None
    if not tz:
        tz = "UTC"
    _STATION_TZ_CACHE[key] = tz
    return tz

@lru_cache(maxsize=1)
def load_station_table() -> pd.DataFrame:
    """
    Load the official NDBC station table.
    Returns DataFrame with columns: id, name, lat, lon
    """
    try:
        resp = SESSION.get(STATIONS_CSV_URL, timeout=20)
        resp.raise_for_status()
        text = resp.text
    except Exception:
        # Fallback: a tiny baked-in list if remote fails (keeps app running)
        text = """# id|name|lat|lon
41113|NE FLORIDA (200 NM E OF JACKSONVILLE)|28.40|-80.53
51003|NORTHWEST HAWAII|25.78|-160.05
42040|MID GULF|28.79|-90.07
"""
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = re.split(r"[|,\s]{2,}|[|]", line)
        # Try to be tolerant of messy formats
        if len(parts) >= 4:
            sid = parts[0].strip()
            name = parts[1].strip()
            try:
                lat = float(parts[2].strip())
                lon = float(parts[3].strip())
            except Exception:
                continue
            rows.append((sid, name, lat, lon))
    df = pd.DataFrame(rows, columns=["id", "name", "lat", "lon"])
    df.drop_duplicates(subset=["id"], inplace=True)
    return df


@lru_cache(maxsize=1)
def load_station_coords() -> dict[str, dict]:
    df = load_station_table()
    return {str(r.id): {"name": r.name, "lat": float(r.lat), "lon": float(r.lon)} for r in df.itertuples(index=False)}


def get_station_list() -> list[tuple[str, str]]:
    """(id, name) tuples for the Station dropdown."""
    coords = load_station_coords()
    items = [(sid, meta["name"]) for sid, meta in coords.items()]
    items.sort(key=lambda x: x[0])
    return items


def get_stations_data() -> list[dict]:
    """Return [{id,name,lat,lon,tz}] used by /stations.json (cached)."""
    coords = load_station_coords()
    data_list: list[dict] = []
    for sid, meta in coords.items():
        lat = meta["lat"]
        lon = meta["lon"]
        name = meta["name"]
        data_list.append({
            "id": sid,
            "name": name,
            "lat": lat,
            "lon": lon,
            "tz": tz_for_latlon(lat, lon)  # <-- ADD tz so the frontend can auto-apply the station's local time
        })
    return data_list


# ---------------------------------------------------------------------
# Forecast (BULL) retrieval & parsing
# ---------------------------------------------------------------------

BULL_URL = "https://www.ndbc.noaa.gov/data/forecast/{sid}.bull"

def fetch_bull_text(station_id: str) -> str | None:
    try:
        url = BULL_URL.format(sid=station_id)
        r = SESSION.get(url, timeout=20)
        if r.status_code == 200 and r.text.strip():
            return r.text
    except Exception:
        pass
    return None


def _parse_dt_token(token: str) -> tuple[int, int]:
    """Parse tokens like '2AM', '12PM' -> (hour0_23, minute)"""
    token = token.strip().upper()
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(AM|PM)?$", token)
    if not m:
        # handle 'NOON' / 'MIDNIGHT'
        if token == "NOON":
            return (12, 0)
        if token == "MIDNIGHT":
            return (0, 0)
        # fallback
        return (0, 0)
    hh = int(m.group(1))
    mm = int(m.group(2)) if m.group(2) else 0
    ampm = m.group(3)
    if ampm == "AM":
        if hh == 12:
            hh = 0
    elif ampm == "PM":
        if hh != 12:
            hh += 12
    return (hh, mm)


def parse_bull(station_id: str, target_tz_name: str | None = None) -> tuple[str, str, str, list[dict] | None, str, str | None]:
    """
    Return (cycle_str, location_str, model_run_str, rows, effective_tz_name, parse_error)
    rows = list of dicts with local datetime + values
    """
    text = fetch_bull_text(station_id)
    if not text:
        return "", "", "", None, "UTC", f"No forecast found for station {station_id}"

    # pull cycle, location etc.
    cycle_match = re.search(r"^Cycle\s*:\s*([0-9]{10}\s+UTC)", text, flags=re.M)
    cycle_str = cycle_match.group(1).strip() if cycle_match else ""

    location_match = re.search(r"^Location\s*:\s*(.+)$", text, flags=re.M)
    location_str = location_match.group(1).strip() if location_match else ""

    # Try to extract station coordinates from Location line
    lat, lon = None, None
    m = re.search(r"\(\s*([-+]?\d+(?:\.\d+)?)\s*([NS])\s+([-+]?\d+(?:\.\d+)?)\s*([EW])\)", location_str)
    if m:
        try:
            lat_val = float(m.group(1)); lat_dir = m.group(2).upper()
            lon_val = float(m.group(3)); lon_dir = m.group(4).upper()
            lat = lat_val if lat_dir == 'N' else -lat_val
            lon = lon_val if lon_dir == 'E' else -lon_val
        except Exception:
            lat, lon = None, None
    if lat is None or lon is None:
        coords = load_station_coords().get(str(station_id))
        if coords:
            lat = coords["lat"]
            lon = coords["lon"]

    # Decide which timezone to use
    if target_tz_name:
        tz_name = target_tz_name
    else:
        tz_name = tz_for_latlon(lat, lon) if (lat is not None and lon is not None) else "UTC"

    try:
        local_tz = pytz.timezone(tz_name)
    except Exception:
        local_tz = pytz.UTC
        tz_name = "UTC"

    # Body rows — normalize to continuous datetimes across month boundaries
    # The BULL lines include a date + multiple (time, values...) tokens per line.
    # We parse all timestamps in UTC (from the bulletin), then convert to local_tz.
    rows: list[dict] = []
    # Find each line with a "Day Mon DD" date followed by times/values
    # We accept both "31 Aug" and "Aug 31" styles.
    date_line_re = re.compile(r"^(?:[A-Za-z]{3}\s+\d{1,2}\s|[A-Za-z]{3,9}\s+\d{1,2}\s)", re.M)
    lines = text.splitlines()
    # We’ll track the last seen UTC datetime to ensure strictly increasing order
    last_utc: datetime | None = None

    # minor helpers
    def _fmt_float(tok: str) -> float | None:
        tok = tok.strip().replace("--", "")
        if tok == "" or tok == "NA":
            return None
        try:
            return float(tok)
        except Exception:
            return None

    # Crude tokenization of the actual data lines
    current_date_utc: datetime | None = None
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        # Date line?
        m = re.match(r"^(?P<dow>[A-Za-z]{3,9}),?\s+(?P<mon>[A-Za-z]{3,9})\s+(?P<day>\d{1,2}),?\s+(?P<year>\d{4})", line)
        if m:
            # Build midnight UTC for this date, then we’ll add times below
            mon = m.group("mon").strip().title()
            month_num = datetime.strptime(mon[:3], "%b").month
            day = int(m.group("day"))
            year = int(m.group("year"))
            current_date_utc = datetime(year, month_num, day, 0, 0, tzinfo=timezone.utc)
            continue

        # Try rows like " 12 AM  Hs Tp Dir   Hs Tp Dir ..."
        # Extract time tokens first
        if current_date_utc is None:
            continue

        # Find all tokens that look like times (e.g., "12 AM", "4 PM", "2:00 AM")
        time_tokens = re.findall(r"(\d{1,2}(?::\d{2})?\s*(?:AM|PM))", line, flags=re.I)
        if not time_tokens:
            continue

        # Extract swell columns in order after each time; we accept up to 6 swells + combined at the end
        # We will be conservative: split by whitespace and parse the known columns (Hs, Tp, Dir).
        # The bulletin can be messy, so we just fish out numeric triplets after each time token.
        parts = line.split()
        # Build a lookup of index -> parsed time (24h)
        indices = []
        for match in re.finditer(r"\d{1,2}(?::\d{2})?\s*(?:AM|PM)", line, flags=re.I):
            # count how many tokens precede this time
            left = line[:match.start()].split()
            indices.append(len(left))

        # Now walk each time “slot”
        for i, start_idx in enumerate(indices):
            t_tok = parts[start_idx] + ("" if ":" in parts[start_idx] else f" {parts[start_idx+1]}")
            if ":" not in parts[start_idx] and (start_idx + 1) < len(parts) and parts[start_idx+1].upper() in ("AM", "PM"):
                # time token consumed two parts, skip second
                val_start = start_idx + 2
            else:
                val_start = start_idx + 1

            hh, mm = _parse_dt_token(t_tok)
            dt_utc = current_date_utc.replace(hour=hh, minute=mm)

            # Enforce strictly increasing UTC across the entire file (fix month/day boundaries)
            if last_utc and dt_utc <= last_utc:
                # bump by 1 day until strictly larger
                while dt_utc <= last_utc:
                    dt_utc += timedelta(days=1)

            last_utc = dt_utc

            # Parse up to 6 swells of triples (Hs, Tp, Dir) followed by a Combined Hs at the end (if present).
            # We’ll be tolerant of missing values.
            values = []
            j = val_start
            # Try to read 6 triples
            for _s in range(6):
                if j + 2 >= len(parts):
                    values.append((None, None, None))
                else:
                    hs = _fmt_float(parts[j])
                    tp = _fmt_float(parts[j + 1])
                    dr = _fmt_float(parts[j + 2])
                    values.append((hs, tp, dr))
                j += 3

            # Combined Hs if available
            combined = _fmt_float(parts[j]) if j < len(parts) else None

            # Convert to local tz for display / graph labels
            local_dt = dt_utc.astimezone(local_tz)

            rows.append({
                "utc": dt_utc,
                "local": local_dt,
                "values": values,
                "combined": combined
            })

    # Sort by UTC (already strictly increasing) then produce strings for header
    rows.sort(key=lambda r: r["utc"])
    model_run_str = ""
    if rows:
        first_local = rows[0]["local"]
        model_run_str = first_local.strftime("%Y-%m-%d %H:%M %Z")

    return cycle_str, location_str, model_run_str, rows, tz_name, None


# ---------------------------------------------------------------------
# Table/Graph builders
# ---------------------------------------------------------------------

def build_html_table(cycle_str: str, location_str: str, model_run_str: str, rows: list[dict],
                     tz_label: str, selected_unit: str) -> str:
    """
    Render the table (string of HTML). Styling is done inline to avoid template bloat.
    """
    # Header
    out = io.StringIO()
    out.write('<div class="table-responsive">\n')
    out.write('<table class="table table-sm table-striped align-middle">\n')
    out.write('<thead><tr><th colspan="25" style="font-weight:600;">')
    out.write(f'Cycle : {html.escape(cycle_str)} &nbsp;&nbsp; | &nbsp;&nbsp; ')
    out.write(f'Location : {html.escape(location_str)} &nbsp;&nbsp; | &nbsp;&nbsp; ')
    out.write(f'Time Zone: {html.escape(tz_label)}')
    out.write('</th></tr>\n')

    # Column headers
    out.write('<tr>')
    out.write('<th style="white-space:nowrap;">Date</th>')
    out.write('<th style="white-space:nowrap;">Time</th>')
    for i in range(1, 7):
        out.write(f'<th colspan="3" class="text-center">Swell {i}</th>')
    out.write('<th class="text-center">Combined</th>')
    out.write('</tr>\n')

    out.write('<tr>')
    out.write('<th></th><th></th>')
    for _i in range(6):
        if selected_unit == "Metric":
            out.write('<th>Hs (m)</th><th>Tp (s)</th><th>Dir (°)</th>')
        else:
            out.write('<th>Hs (ft)</th><th>Tp (s)</th><th>Dir (°)</th>')
    out.write('<th>Hs</th>')
    out.write('</tr></thead>\n<tbody>\n')

    # Body
    for r in rows:
        dt_local: datetime = r["local"]
        out.write('<tr>')
        out.write(f'<td>{dt_local.strftime("%A, %B %d, %Y")}</td>')
        out.write(f'<td>{dt_local.strftime("%-I:%M %p")}</td>' if os.name != "nt" else f'<td>{dt_local.strftime("%I:%M %p").lstrip("0")}</td>')

        for (hs, tp, dr) in r["values"]:
            if selected_unit == "Metric" and hs is not None:
                hs_disp = hs * 0.3048  # feet -> meters
            else:
                hs_disp = hs
            out.write(f'<td class="text-end">{"" if hs_disp is None else f"{hs_disp:.1f}"}</td>')
            out.write(f'<td class="text-end">{"" if tp is None else f"{tp:.1f}"}</td>')
            out.write(f'<td class="text-end">{"" if dr is None else f"{dr:.0f}"}</td>')

        comb = r["combined"]
        if selected_unit == "Metric" and comb is not None:
            comb = comb * 0.3048
        out.write(f'<td class="text-end">{"" if comb is None else f"{comb:.1f}"}</td>')
        out.write('</tr>\n')

    out.write('</tbody></table></div>')
    return out.getvalue()


def build_graph_payload(rows: list[dict], selected_unit: str) -> dict:
    labels = []
    s1h = []; s2h = []; s3h = []; s4h = []; s5h = []; s6h = []; combo = []
    s1p = []; s2p = []; s3p = []; s4p = []; s5p = []; s6p = []
    s1d = []; s2d = []; s3d = []; s4d = []; s5d = []; s6d = []

    for r in rows:
        dt: datetime = r["local"]
        # label format currently used by the app (month/day and hour only, 12h)
        lab = dt.strftime("%-m/%-d %-I%p") if os.name != "nt" else dt.strftime("%m/%d %I%p").lstrip("0").replace("/0", "/")
        labels.append(lab)

        vals = r["values"]
        def feet_to_m(x):
            return None if x is None else (x * 0.3048)

        # heights
        hs = [vals[i][0] if i < len(vals) else None for i in range(6)]
        if selected_unit == "Metric":
            hs = [feet_to_m(x) for x in hs]

        s1h.append(hs[0]); s2h.append(hs[1]); s3h.append(hs[2]); s4h.append(hs[3]); s5h.append(hs[4]); s6h.append(hs[5])
        combo.append((feet_to_m(r["combined"]) if selected_unit == "Metric" else r["combined"]))

        # periods
        ps = [vals[i][1] if i < len(vals) else None for i in range(6)]
        s1p.append(ps[0]); s2p.append(ps[1]); s3p.append(ps[2]); s4p.append(ps[3]); s5p.append(ps[4]); s6p.append(ps[5])

        # directions
        ds = [vals[i][2] if i < len(vals) else None for i in range(6)]
        s1d.append(ds[0]); s2d.append(ds[1]); s3d.append(ds[2]); s4d.append(ds[3]); s5d.append(ds[4]); s6d.append(ds[5])

    return {
        "units": ("m" if selected_unit == "Metric" else "ft"),
        "labels": labels,
        "height": {"s1": s1h, "s2": s2h, "s3": s3h, "s4": s4h, "s5": s5h, "s6": s6h, "combined": combo},
        "period": {"s1": s1p, "s2": s2p, "s3": s3p, "s4": s4p, "s5": s5p, "s6": s6p},
        "direction": {"s1": s1d, "s2": s2d, "s3": s3d, "s4": s4d, "s5": s5d, "s6": s6d}
    }


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------

@app.route("/favicon.ico")
def favicon():
    return send_from_directory(os.path.join(app.root_path, "static"), "favicon.ico")

@app.route("/stations.json")
def stations_json():
    return jsonify(get_stations_data())


@app.route("/", methods=["GET", "POST"])
def index():
    stations = get_station_list()
    timezones = sorted(pytz.common_timezones)
    unit_options = ["US", "Metric"]

    selected_station = ""
    selected_tz = ""
    selected_unit = "US"
    selected_view = "Table"

    if request.method == "POST":
        selected_station = request.form.get("station") or ""
        selected_tz = request.form.get("tz") or ""
        selected_unit = request.form.get("unit") or "US"
        selected_view = request.form.get("view") or "Table"
    else:
        selected_station = request.args.get("station") or ""
        selected_tz = request.args.get("tz") or ""
        selected_unit = request.args.get("unit") or "US"
        selected_view = request.args.get("view") or "Table"

    table_html = None
    graph_data = None
    error = None
    tz_label = ""
    selected_lat = None
    selected_lon = None

    if selected_station:
        # Pass selected_tz when provided, otherwise let the parser determine
        cycle_str, location_str, model_run_str, rows, effective_tz_name, parse_error = parse_bull(
            selected_station, selected_tz or None
        )
        error = parse_error
        if rows is not None:
            tz_label = effective_tz_name
            table_html = build_html_table(cycle_str, location_str, model_run_str, rows, tz_label, selected_unit)
            graph_data = build_graph_payload(rows, selected_unit)

            coords_map = load_station_coords()
            sid_str = str(selected_station).strip()
            if sid_str in coords_map:
                selected_lat = coords_map[sid_str]['lat']; selected_lon = coords_map[sid_str]['lon']
            elif location_str:
                m = re.search(r"\(\s*([-+]?\d+(?:\.\d+)?)\s*([NS])\s+([-+]?\d+(?:\.\d+)?)\s*([EW])\)", location_str)
                if m:
                    try:
                        lat_val = float(m.group(1)); lat_dir = m.group(2).upper()
                        lon_val = float(m.group(3)); lon_dir = m.group(4).upper()
                        selected_lat = lat_val if lat_dir == 'N' else -lat_val
                        selected_lon = lon_val if lon_dir == 'E' else -lon_val
                    except Exception:
                        pass

    return render_template(
        "index.html",
        stations=stations,
        selected_station=selected_station,
        timezones=timezones,
        selected_tz=selected_tz or "",  # we keep the dropdown value; header uses tz_label
        tz_label=tz_label,
        units=unit_options,
        selected_unit=selected_unit,
        selected_view=selected_view,
        graph_data=graph_data,
        table_html=table_html,
        error=error,
        selected_lat=selected_lat,
        selected_lon=selected_lon,
    )


if __name__ == "__main__":
    app.run(debug=True)
