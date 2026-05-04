from flask import Flask, render_template, request, send_file, jsonify
import requests
import json
import os
from datetime import datetime, timedelta
import pytz
# TimezoneFinder is imported lazily to avoid heavy startup cost on Render.
from calendar import monthrange
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

@app.route('/stations.json')
def stations_json():
    return jsonify(get_stations_data())

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
        resp = requests.get(bull_url, timeout=15)
    except Exception:
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
    stations = get_station_list()
    timezones = sorted(pytz.common_timezones)
    unit_options = ["US", "Metric"]

    selected_station = ""
    selected_tz = ""
    selected_unit = "US"
    selected_view = (request.values.get("view") or "Table")
    if request.method == "POST":
        selected_station = request.form.get("station") or ""
        selected_tz = request.form.get("tz") or ""
        selected_unit = request.form.get("unit") or "US"
    else:
        selected_station = request.args.get("station", "")
        selected_tz = request.args.get("tz", "")
        selected_unit = request.args.get("unit", "US") or "US"

    if not selected_station:
        selected_station = "51201"

    table_html = None
    error = None
    tz_label = ""
    selected_lat = None
    selected_lon = None
    graph_data = None
    graph_header = None  # (cycle, location, tz)

    if selected_station:
        cycle_str, location_str, model_run_str, rows, effective_tz_name, parse_error = parse_bull(
            selected_station, selected_tz or None
        )
        error = parse_error
        if rows is not None:
            tz_label = effective_tz_name
            table_html = build_html_table(cycle_str, location_str, model_run_str, rows, tz_label, selected_unit)

            # for map single marker if coords JSON has it
            coords_map = load_station_coords()
            sid_str = str(selected_station).strip()
            if sid_str in coords_map:
                selected_lat = coords_map[sid_str]['lat']
                selected_lon = coords_map[sid_str]['lon']

            # ----- pack graph data -----
            labels = [f"{r[0]} {r[1]}" for r in rows]
            def pick(array_index):
                return [r[array_index] for r in rows]

            # indices per swell
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

            # NEW: convert graph heights to meters when Metric is selected
            if selected_unit == "Metric":
                FT_TO_M = 0.3048
                height = {
                    k: [None if v is None else round(v * FT_TO_M, 2) for v in arr]
                    for k, arr in height_ft.items()
                }
                graph_units = "m"
            else:
                height = height_ft
                graph_units = "ft"

            graph_data = {
                "labels": labels,
                "height": height,
                "period": period,
                "direction": direction,
                "units": graph_units,  # now matches the numeric units
                "cycle": cycle_str or "",
                "location": location_str or "",
                "tz": tz_label or ""
            }

            # Graph header should NOT include the leading words; clean them.
            cycle_clean = _strip_header_prefix(cycle_str, "Cycle")
            loc_clean   = _strip_header_prefix(location_str, "Location")
            lat, lon = _parse_header_coords(location_str)
            latlon_fmt = _fmt_latlon(lat, lon)
            # Prefer "station_id (lat lon)" like "51201 (21.67N 158.12W)" when coords exist
            loc_display = f"{selected_station} ({latlon_fmt})" if latlon_fmt else loc_clean

            graph_header = {
                "cycle": cycle_clean,
                "location": loc_display,
                "tz": tz_label or ""
            }

    return render_template(
        "index.html",
        stations=stations,
        selected_station=selected_station,
        timezones=timezones,
        selected_tz=selected_tz,
        tz_label=tz_label,
        units=unit_options,
        selected_unit=selected_unit,
        table_html=table_html,
        error=error,
        selected_lat=selected_lat,
        selected_lon=selected_lon,
        selected_view=request.values.get("view") or "Table",
        graph_data=graph_data,
        graph_header=graph_header
    )


# -------------------------- NDBC live buoy overlay ------------------------------
import re
import math
import time
import xml.etree.ElementTree as ET

NDBC_ACTIVE_XML = "https://www.ndbc.noaa.gov/activestations.xml"
NDBC_REALTIME_DIR = "https://www.ndbc.noaa.gov/data/realtime2/"
NDBC_CACHE_TTL_SECONDS = 15 * 60

NDBC_STATIONS_CACHE = {
    "timestamp": 0,
    "data": []
}

NDBC_COMPONENT_CACHE = {}

def _cache_valid(cache_timestamp: float, ttl: int = NDBC_CACHE_TTL_SECONDS) -> bool:
    return (time.time() - cache_timestamp) < ttl

def _fetch_text(url: str, timeout: int = 25) -> str:
    resp = requests.get(url, timeout=timeout)
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
    if NDBC_STATIONS_CACHE["data"] and _cache_valid(NDBC_STATIONS_CACHE["timestamp"]):
        return NDBC_STATIONS_CACHE["data"]
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
    NDBC_STATIONS_CACHE["timestamp"] = time.time()
    NDBC_STATIONS_CACHE["data"] = stations
    return stations

@app.route("/api/ndbc/live-wave-stations")
def api_ndbc_live_wave_stations():
    return jsonify(get_live_ndbc_wave_stations())

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

def _find_spectral_peaks(freqs: list, density: list) -> list:
    smooth = _smooth3(density)
    max_val = max(smooth) if smooth else 0.0
    if max_val <= 0.0:
        return []
    min_prominence = max_val * 0.06
    peaks = []
    for i in range(1, len(smooth) - 1):
        if smooth[i] > smooth[i - 1] and smooth[i] >= smooth[i + 1]:
            left_min = min(smooth[max(0, i - 4): i + 1])
            right_min = min(smooth[i : min(len(smooth), i + 5)])
            prominence = smooth[i] - max(left_min, right_min)
            if prominence >= min_prominence:
                peaks.append(i)
    if not peaks:
        peaks = [smooth.index(max(smooth))]
    filtered = []
    min_spacing = 2
    for p in sorted(peaks, key=lambda idx: smooth[idx], reverse=True):
        if all(abs(p - existing) >= min_spacing for existing in filtered):
            filtered.append(p)
    return sorted(filtered)

def _partition_spectrum(freqs: list, density: list, directions: list | None = None) -> list:
    paired = []
    for i, (f, e) in enumerate(zip(freqs, density)):
        if f and math.isfinite(f) and e is not None and math.isfinite(e) and e >= 0.0:
            d = directions[i] if directions and i < len(directions) else None
            paired.append((f, e, d))
    paired.sort(key=lambda x: x[0])
    if len(paired) < 5:
        return []
    freqs = [p[0] for p in paired]
    density = [p[1] for p in paired]
    dirs = [p[2] for p in paired]
    smooth = _smooth3(density)
    peaks = _find_spectral_peaks(freqs, density)
    boundaries = [0]
    for p1, p2 in zip(peaks[:-1], peaks[1:]):
        valley = min(range(p1, p2 + 1), key=lambda i: smooth[i])
        boundaries.append(valley)
    boundaries.append(len(freqs) - 1)
    df = _bin_widths(freqs)
    components = []
    for i, peak_idx in enumerate(peaks):
        start = boundaries[i]
        end = boundaries[i + 1]
        f_part = freqs[start: end + 1]
        e_part = density[start: end + 1]
        df_part = df[start: end + 1]
        dir_part = dirs[start: end + 1]
        m0 = sum(e * w for e, w in zip(e_part, df_part))
        hs_m = 4.0 * math.sqrt(max(m0, 0.0))
        hs_ft = hs_m * 3.28084
        if hs_ft < 0.25:
            continue
        peak_f = freqs[peak_idx]
        peak_period = 1.0 / peak_f if peak_f else None
        weights = [e * w for e, w in zip(e_part, df_part)]
        mean_dir = _circular_mean_deg(dir_part, weights) if directions else None
        components.append({
            "component": len(components) + 1,
            "type": "swell" if (peak_period and peak_period >= 8.0) else "wind sea",
            "height_ft": round(hs_ft, 1),
            "height_m": round(hs_m, 2),
            "peak_period_sec": round(peak_period, 1) if peak_period else None,
            "peak_frequency_hz": round(peak_f, 4),
            "direction_deg": round(mean_dir) if mean_dir is not None else None,
            "direction_compass": _compass_from_degrees(mean_dir),
            "energy_m0": round(m0, 4),
            "frequency_min_hz": round(min(f_part), 4),
            "frequency_max_hz": round(max(f_part), 4),
        })
    components.sort(key=lambda c: (0 if c["type"] == "swell" else 1, -(c["peak_period_sec"] or 0), -c["height_ft"]))
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
    cached = NDBC_COMPONENT_CACHE.get(station_id)
    if cached and _cache_valid(cached["timestamp"], ttl=10 * 60):
        return jsonify(cached["data"])
    station_meta = {s["id"]: s for s in get_live_ndbc_wave_stations()}.get(station_id, {"id": station_id, "name": station_id})
    try:
        density_text = _fetch_text(f"{NDBC_REALTIME_DIR}{station_id}.data_spec")
    except Exception as exc:
        return jsonify({"station": station_id, "error": f"NDBC file not available: {exc}"}), 404
    try:
        direction_text = _fetch_text(f"{NDBC_REALTIME_DIR}{station_id}.swdir")
    except Exception:
        direction_text = None
    density_rows = _parse_ndbc_spectral_file(density_text)
    if not density_rows:
        return jsonify({"station": station_id, "error": "No spectral rows parsed"}), 404
    density_row = _latest_spectral_row(density_rows)
    direction_rows = _parse_ndbc_spectral_file(direction_text) if direction_text else []
    direction_row = _match_direction_row(density_row, direction_rows) if direction_rows else None
    freqs = density_row["freqs"]
    density_vals = density_row["values"]
    directions = None
    if direction_row:
        direction_by_freq = {round(f, 5): v for f, v in zip(direction_row["freqs"], direction_row["values"])}
        directions = [direction_by_freq.get(round(f, 5)) for f in freqs]
    components = _partition_spectrum(freqs, density_vals, directions)
    df = _bin_widths(freqs)
    total_m0 = sum(e * w for e, w in zip(density_vals, df))
    total_hs_m = 4.0 * math.sqrt(max(total_m0, 0.0))
    try:
        HST = pytz.timezone("Pacific/Honolulu")
        timestamp_hst = density_row["timestamp_utc"].astimezone(HST).strftime("%Y-%m-%d %I:%M %p HST")
    except Exception:
        timestamp_hst = None
    result = {
        "station": station_id,
        "name": station_meta.get("name", station_id),
        "lat": station_meta.get("lat"),
        "lon": station_meta.get("lon"),
        "timestamp_utc": density_row["timestamp_utc"].isoformat().replace("+00:00", "Z"),
        "timestamp_hst": timestamp_hst,
        "total_height_ft": round(total_hs_m * 3.28084, 1),
        "total_height_m": round(total_hs_m, 2),
        "components": components,
        "spectrum": [
            {
                "frequency_hz": round(f, 4),
                "period_sec": round(1.0 / f, 2) if f else None,
                "density_m2_per_hz": round(e, 5)
            }
            for f, e in zip(freqs, density_vals)
        ]
    }
    NDBC_COMPONENT_CACHE[station_id] = {"timestamp": time.time(), "data": result}
    return jsonify(result)



if __name__ == "__main__":
    app.run(debug=True)
