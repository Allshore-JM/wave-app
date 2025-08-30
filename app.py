from flask import Flask, render_template, request, jsonify
import requests
import json
import os
from datetime import datetime, timedelta
from calendar import monthrange
import re
import pytz
from timezonefinder import TimezoneFinder

app = Flask(__name__)

# ---- Time / tz helpers ----
tz_finder = TimezoneFinder()
UTC = pytz.utc

# ---- Caches ----
STATION_META = None            # station_id -> {name, lat, lon}
STATION_COORDS = None          # station_id -> {lat, lon}
BULLET_STATIONS = None         # set of station_ids with .bull

# ---- NOAA ----
NOAA_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"

# ---- Default fallback stations (for UI if metadata fetch fails) ----
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

stations_data_cache = None


# ===================== Coordinates / Station list =====================

def load_station_coords() -> dict:
    """Load static station coordinates from station_coords.json if present."""
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
    """NDBC station table for names/lat/lon. Falls back to DEFAULT_STATIONS."""
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
        return STATION_META
    except Exception:
        STATION_META = DEFAULT_STATIONS.copy()
        return STATION_META


def get_station_list() -> list[tuple[str, str]]:
    """UI dropdown list (from station_list.json if present)."""
    stations: list[tuple[str, str]] = []
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(base_dir, 'station_list.json')
        with open(json_path, 'r') as f:
            station_ids = json.load(f)
        meta = {}
        try:
            meta = load_station_metadata()
        except Exception:
            pass
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
    """Return [{id,name,lat,lon}] for /stations.json (cached)."""
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
            lat = coords_map[sid]['lat']; lon = coords_map[sid]['lon']
        if (lat is None or lon is None) and sid in DEFAULT_STATIONS:
            fallback = DEFAULT_STATIONS[sid]
            lat = fallback.get('lat'); lon = fallback.get('lon')
        if lat is not None and lon is not None:
            data_list.append({'id': sid, 'name': name, 'lat': lat, 'lon': lon})
    stations_data_cache = data_list
    return stations_data_cache


@app.route('/stations.json')
def stations_json():
    return jsonify(get_stations_data())


# ===================== Latest run helpers =====================

def get_latest_run():
    """Find most recent run by probing last 2 days (18/12/06/00z)."""
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


# ===================== .bull parsing =====================

def _month_add(year: int, month: int, add: int) -> tuple[int, int]:
    m0 = (month - 1) + add
    y = year + (m0 // 12)
    m = (m0 % 12) + 1
    return y, m


def _build_monotonic_dt(cycle_dt_utc: datetime, prev_dt_utc: datetime | None,
                        day_val: int, hour_val: int) -> datetime:
    """
    Construct a UTC datetime for (day-of-month, hour) that is >= cycle_dt_utc
    and strictly >= prev_dt_utc (if provided), rolling into future months as
    needed and clamping invalid days (e.g., Feb 30 -> Feb 28/29).
    """
    offset = 0
    while True:
        y, m = _month_add(cycle_dt_utc.year, cycle_dt_utc.month, offset)
        last = monthrange(y, m)[1]
        d = min(day_val, last)
        candidate = datetime(y, m, d, hour_val)
        if candidate < cycle_dt_utc:
            offset += 1
            continue
        if prev_dt_utc is not None and candidate <= prev_dt_utc:
            offset += 1
            continue
        return candidate


def parse_bull(station_id: str, target_tz_name: str | None = None):
    """Return (cycle_str, location_str, model_run_str, rows, tz_used, error)."""
    date_str, run_str = get_latest_run()
    if not date_str:
        return None, None, None, None, 'UTC', "No recent run found."

    bull_url = f"{NOAA_BASE}/gfs.{date_str}/{run_str}/wave/station/bulls.t{run_str}z/gfswave.{station_id}.bull"
    try:
        resp = requests.get(bull_url, timeout=15)
    except Exception:
        return None, None, None, None, 'UTC', f"Network error retrieving {station_id}"
    if resp.status_code != 200:
        return None, None, None, None, 'UTC', f"No .bull file found for {station_id}"

    lines = resp.text.splitlines()
    if not lines:
        return None, None, None, None, 'UTC', "Downloaded .bull file is empty."

    # Header lines
    cycle_line = next((l for l in lines if l.lower().strip().startswith("cycle")), None)
    location_line = next((l for l in lines if l.lower().strip().startswith("location")), None)
    cycle_str = (cycle_line or "").strip()
    location_str = (location_line or "").strip()

    # Buoy lat/lon -> local tz
    lat = lon = None
    tz_name = 'UTC'
    if location_str:
        m = re.search(r"\(([-+]?\d+(?:\.\d+)?)\s*([NS])\s+([-+]?\d+(?:\.\d+)?)\s*([EW])\)", location_str)
        if m:
            try:
                lat_val = float(m.group(1)); lat_dir = m.group(2).upper()
                lon_val = float(m.group(3)); lon_dir = m.group(4).upper()
                lat = lat_val if lat_dir == 'N' else -lat_val
                lon = lon_val if lon_dir == 'E' else -lon_val
            except Exception:
                pass
    if lat is not None and lon is not None:
        try:
            cand = tz_finder.timezone_at(lat=lat, lng=lon)
            if cand:
                tz_name = cand
        except Exception:
            pass

    effective_tz_name = tz_name
    if target_tz_name:
        try:
            _ = pytz.timezone(target_tz_name)
            effective_tz_name = target_tz_name
        except Exception:
            pass

    # Detect format
    uses_day_hour_format = any("day &" in line.lower() for line in lines[:10])

    rows = []
    model_run_str = None

    if uses_day_hour_format:
        # Cycle datetime
        m = re.search(r"(\d{8})\s*(\d{2})", cycle_str or "")
        cycle_date_str = date_str
        cycle_hour_str = run_str
        if m:
            cycle_date_str = m.group(1)
            cycle_hour_str = m.group(2)
        try:
            cycle_dt_utc = datetime.strptime(f"{cycle_date_str} {cycle_hour_str}", "%Y%m%d %H")
        except Exception:
            cycle_dt_utc = datetime.strptime(f"{date_str} {run_str}", "%Y%m%d %H")

        # Model run (in effective tz)
        try:
            model_run_local = cycle_dt_utc.replace(tzinfo=UTC).astimezone(pytz.timezone(effective_tz_name))
        except Exception:
            model_run_local = cycle_dt_utc
        try:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %-d, %Y %I:%M %p")
        except Exception:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %d, %Y %I:%M %p").lstrip('0')

        # Parse data rows
        M_TO_FT = 3.28084
        prev_utc = cycle_dt_utc
        for line in lines:
            t = line.strip()
            if not t.startswith("|"):
                continue
            if "Hst" in t or "---" in t:
                continue
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) < 2:
                continue

            # day & hour
            dht = parts[0].split()
            if len(dht) < 2:
                continue
            try:
                day_val = int(dht[0])
                hour_val = int(dht[1])
            except ValueError:
                continue

            # combined
            hst_tokens = parts[1].split()
            combined_hs_m = None
            if hst_tokens:
                try:
                    combined_hs_m = float(hst_tokens[0].replace('*', ''))
                except ValueError:
                    pass

            # 6 swells
            swell_groups = []
            for swell_field in parts[2:]:
                raw = swell_field.split()
                cleaned = []
                for tok in raw:
                    tokc = tok.replace('*', '')
                    if tokc:
                        cleaned.append(tokc)
                if len(cleaned) < 3:
                    swell_groups.append((None, None, None))
                else:
                    try:
                        hs_val = float(cleaned[0])
                        tp_val = float(cleaned[1])
                        dir_raw = int(float(cleaned[2]))
                        dir_val = (dir_raw + 180) % 360
                        swell_groups.append((hs_val, tp_val, dir_val))
                    except ValueError:
                        swell_groups.append((None, None, None))
            while len(swell_groups) < 6:
                swell_groups.append((None, None, None))
            if len(swell_groups) > 6:
                swell_groups = swell_groups[:6]

            # *** Month-safe, monotonic UTC timestamp ***
            forecast_dt_utc = _build_monotonic_dt(cycle_dt_utc, prev_utc, day_val, hour_val)
            prev_utc = forecast_dt_utc

            # Localize
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

            combined_hs_ft = combined_hs_m * M_TO_FT if combined_hs_m is not None else None
            row = [date_str_local, time_str_local]
            for hs_m, tp_val, dir_val in swell_groups:
                if hs_m is None:
                    row.extend([None, None, None])
                else:
                    row.extend([hs_m * M_TO_FT, tp_val, dir_val])
            row.append(combined_hs_ft)
            rows.append(row)

    else:
        # Older format ("Hr" offset)
        start_idx = None
        for idx, line in enumerate(lines):
            if line.strip().startswith("Hr"):
                start_idx = idx + 1
                break
        if start_idx is None:
            return cycle_str, location_str, None, None, effective_tz_name, "Data section not found in .bull file."

        m_old = re.search(r"(\d{8})\s*(\d{2})", cycle_str or "")
        cycle_date_str_old = date_str
        cycle_hour_str_old = run_str
        if m_old:
            cycle_date_str_old = m_old.group(1)
            cycle_hour_str_old = m_old.group(2)
        try:
            cycle_dt_utc_old = datetime.strptime(f"{cycle_date_str_old} {cycle_hour_str_old}", "%Y%m%d %H")
        except Exception:
            cycle_dt_utc_old = datetime.strptime(f"{date_str} {run_str}", "%Y%m%d %H")

        try:
            model_run_local_old = cycle_dt_utc_old.replace(tzinfo=UTC).astimezone(pytz.timezone(effective_tz_name))
        except Exception:
            model_run_local_old = cycle_dt_utc_old
        try:
            model_run_str = "Model Run: " + model_run_local_old.strftime("%A, %B %-d, %Y %I:%M %p")
        except Exception:
            model_run_str = "Model Run: " + model_run_local_old.strftime("%A, %B %d, %Y %I:%M %p").lstrip('0')

        M_TO_FT = 3.28084
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
            for _swell in range(6):
                hs_val = tp_val = dir_val = None
                tokens_collected = 0
                while tokens_collected < 3 and idx_base < len(parts):
                    tok = parts[idx_base]; idx_base += 1
                    tok_clean = tok.replace('*', '')
                    if not tok_clean:
                        continue
                    if tokens_collected == 0:
                        try:
                            hs_val = float(tok_clean) * M_TO_FT; tokens_collected += 1; continue
                        except ValueError:
                            continue
                    if tokens_collected == 1:
                        try:
                            tp_val = float(tok_clean); tokens_collected += 1; continue
                        except ValueError:
                            continue
                    if tokens_collected == 2:
                        try:
                            dir_raw = int(float(tok_clean)); dir_val = (dir_raw + 180) % 360
                            tokens_collected += 1; continue
                        except ValueError:
                            continue
                if tokens_collected < 3:
                    row.extend([None, None, None])
                else:
                    row.extend([hs_val, tp_val, dir_val])

            combined_hs_ft = None
            for tok in reversed(parts):
                tok_clean = tok.replace('*', '')
                if not tok_clean:
                    continue
                try:
                    combined_hs_ft = float(tok_clean) * M_TO_FT; break
                except ValueError:
                    continue
            row.append(combined_hs_ft)
            rows.append(row)

    # Round numbers
    for r in rows:
        idx_num = 2
        for _ in range(6):
            if r[idx_num] is not None: r[idx_num] = round(r[idx_num], 2)
            idx_num += 1
            if r[idx_num] is not None: r[idx_num] = round(r[idx_num], 1)
            idx_num += 1
            if r[idx_num] is not None:
                try: r[idx_num] = int(round(r[idx_num]))
                except Exception: pass
            idx_num += 1
        if r[-1] is not None: r[-1] = round(r[-1], 2)

    if not rows:
        return cycle_str, location_str, None, None, effective_tz_name, "No data rows parsed from .bull file."
    return cycle_str, location_str, model_run_str, rows, effective_tz_name, None


# ===================== Table & Graph payloads =====================

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
    html += '<tr>'
    html += '<th rowspan="2">Date</th><th rowspan="2">Time</th>'
    for idx, col in enumerate(group_colors, start=1):
        html += f'<th colspan="3" style="background-color:{col["header"]}; color:white; text-align:center;">Swell {idx}</th>'
    html += f'<th style="background-color:{combined_colors["header"]}; color:white; text-align:center;">Combined</th>'
    html += '</tr>\n<tr>'
    hs_unit_label = '(ft)' if unit == 'US' else '(m)'
    for col in group_colors:
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Hs<br>{hs_unit_label}</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Tp<br>(s)</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Dir<br>(d)</th>'
    html += f'<th style="background-color:{combined_colors["subheader"]}; text-align:center;">Hs<br>{hs_unit_label}</th>'
    html += '</tr>\n'

    for row in rows:
        # simple styling for day/night rows (unchanged)
        try:
            parsed_time = datetime.strptime(row[1], "%I:%M %p").time()
        except Exception:
            try:
                parsed_time = datetime.strptime(row[1], "%I:%M:%S %p").time()
            except Exception:
                parsed_time = None
        bold_start = datetime.strptime("6:00:00 AM", "%I:%M:%S %p").time()
        bold_end = datetime.strptime("7:00:00 PM", "%I:%M:%S %p").time()
        dashed_start_evening = datetime.strptime("8:00:00 PM", "%I:%M:%S %p").time()
        dashed_end_morning = datetime.strptime("5:00:00 AM", "%I:%M:%S %p").time()
        border_style = ""; font_weight_row = "normal"
        if parsed_time is not None:
            if bold_start <= parsed_time <= bold_end:
                border_style = "border:1px solid #000;"; font_weight_row = "bold"
            elif parsed_time >= dashed_start_evening or parsed_time <= dashed_end_morning:
                border_style = "border:1px dashed #999;"; font_weight_row = "normal"

        html += '<tr>'
        html += f'<td style="font-weight:bold; {border_style} padding:4px 8px;">{row[0]}</td>'
        html += f'<td style="font-weight:{font_weight_row}; {border_style} padding:4px 8px;">{row[1]}</td>'

        idx = 2
        for col in group_colors:
            # Hs
            val = row[idx]; idx += 1
            hs_str = "" if val is None else (f"{val:.2f}" if unit == 'US' else f"{(val/3.28084):.2f}")
            html += f'<td style="background-color:{col["data"]}; text-align:right; font-weight:{font_weight_row}; {border_style} padding:4px 8px;">{hs_str}</td>'
            # Tp
            val = row[idx]; idx += 1
            tp_str = "" if val is None else f"{val:.1f}"
            html += f'<td style="background-color:{col["data"]}; text-align:right; font-weight:{font_weight_row}; {border_style} padding:4px 8px;">{tp_str}</td>'
            # Dir
            val = row[idx]; idx += 1
            dir_str = "" if val is None else f"{val}"
            html += f'<td style="background-color:{col["data"]}; text-align:right; font-weight:{font_weight_row}; {border_style} padding:4px 8px;">{dir_str}</td>'

        # Combined
        val = row[-1]
        comb_str = "" if val is None else (f"{val:.2f}" if unit == 'US' else f"{(val/3.28084):.2f}")
        html += f'<td style="background-color:{combined_colors["data"]}; text-align:right; font-weight:{font_weight_row}; {border_style} padding:4px 8px;">{comb_str}</td>'
        html += '</tr>\n'

    html += '</table>'
    return html


def _fmt_short(dt_obj: datetime) -> str:
    """Return M/D/YY h:mm AM/PM without leading zero on month/day."""
    try:
        return dt_obj.strftime("%-m/%-d/%y %I:%M %p").replace(" 0", " ")
    except Exception:
        s = dt_obj.strftime("%m/%d/%y %I:%M %p")
        m, d, rest = s.split('/', 2)
        return f"{int(m)}/{int(d)}/{rest}"


def build_graph_payload(rows: list[list], unit: str) -> dict:
    """Chart.js‑ready payload from parsed rows."""
    labels = []
    height = {f"s{i}": [] for i in range(1, 7)}
    period = {f"s{i}": [] for i in range(1, 7)}
    direction = {f"s{i}": [] for i in range(1, 7)}
    combined = []

    to_m = (unit != 'US')

    for r in rows:
        d_str, t_str = r[0], r[1]
        dt_obj = None
        try:
            dt_obj = datetime.strptime(d_str, "%A, %B %d, %Y")
        except Exception:
            pass
        if dt_obj is not None:
            try:
                tm = datetime.strptime(t_str, "%I:%M %p").time()
            except Exception:
                try:
                    tm = datetime.strptime(t_str, "%I:%M:%S %p").time()
                except Exception:
                    tm = None
            if tm:
                dt_obj = datetime.combine(dt_obj.date(), tm)
        labels.append(_fmt_short(dt_obj) if dt_obj else f"{d_str} {t_str}")

        idx = 2
        for i in range(1, 7):
            hs = r[idx]; tp = r[idx+1]; dd = r[idx+2]
            if hs is not None and to_m:
                hs = hs / 3.28084
            height[f"s{i}"].append(hs if hs is not None else None)
            period[f"s{i}"].append(tp if tp is not None else None)
            direction[f"s{i}"].append(dd if dd is not None else None)
            idx += 3

        comb = r[-1]
        if comb is not None and to_m:
            comb = comb / 3.28084
        combined.append(comb if comb is not None else None)

    return {
        "labels": labels,
        "units": "m" if to_m else "ft",
        "height": {**height, "combined": combined},
        "period": period,
        "direction": direction,
    }


# ===================== Routes =====================

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
        selected_station = request.args.get("station", "")
        selected_tz = request.args.get("tz", "")
        selected_unit = request.args.get("unit", "US") or "US"
        selected_view = request.args.get("view", "Table") or "Table"

    if not selected_station:
        selected_station = "51201"

    table_html = None
    graph_data = None
    error = None
    tz_label = ""
    selected_lat = None
    selected_lon = None
    cycle_str = ""
    location_str = ""

    if selected_station:
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
                        selected_lat = None; selected_lon = None

    return render_template(
        "index.html",
        stations=stations,
        selected_station=selected_station,
        timezones=timezones,
        selected_tz=selected_tz or tz_label,     # show station tz unless user overrides
        units=unit_options,
        selected_unit=selected_unit,
        selected_view=selected_view,
        graph_data=graph_data,
        graph_meta={"cycle": cycle_str, "location": location_str, "tz": tz_label} if graph_data else None,
        table_html=table_html,
        error=error,
        selected_lat=selected_lat,
        selected_lon=selected_lon,
    )


if __name__ == "__main__":
    app.run(debug=True)
