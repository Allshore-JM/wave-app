from flask import Flask, render_template, request, jsonify
import requests
import json
import os
from datetime import datetime, timedelta
import pytz
from timezonefinder import TimezoneFinder

app = Flask(__name__)

# ---- Timezone helper ----
tz_finder = TimezoneFinder()
UTC = pytz.utc

# ---- Caches ----
STATION_META = None          # station_id -> {name, lat, lon}
STATION_COORDS = None        # station_id -> {lat, lon}
BULLET_STATIONS = None       # set of station_ids
stations_data_cache = None   # list of {id,name,lat,lon}

# ---- NOAA endpoints ----
NOAA_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"

# ---- Fallback stations (for offline/partial data) ----
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


# =========================
# Station/metadata helpers
# =========================
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
                coords[str(sid).strip()] = {
                    'lat': float(info.get('lat')),
                    'lon': float(info.get('lon'))
                }
            except Exception:
                continue
    except Exception:
        coords = {}
    STATION_COORDS = coords
    return STATION_COORDS


def get_station_list() -> list[tuple[str, str]]:
    """Read station_list.json if present; otherwise fall back to DEFAULT_STATIONS."""
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
    # fallback list
    return [(sid, info.get('name', sid)) for sid, info in DEFAULT_STATIONS.items()]


def get_stations_data():
    """Build [{id,name,lat,lon}] once for /stations.json."""
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
        if sid in meta and 'name' in meta[sid]:
            name = meta[sid]['name']
        lat = lon = None
        if sid in coords_map:
            lat = coords_map[sid]['lat']; lon = coords_map[sid]['lon']
        if (lat is None or lon is None) and sid in DEFAULT_STATIONS:
            lat = DEFAULT_STATIONS[sid].get('lat')
            lon = DEFAULT_STATIONS[sid].get('lon')
        if lat is not None and lon is not None:
            data_list.append({'id': sid, 'name': name, 'lat': lat, 'lon': lon})

    stations_data_cache = data_list
    return stations_data_cache


@app.route('/stations.json')
def stations_json():
    return jsonify(get_stations_data())


def load_station_metadata():
    """Fetch NDBC station table; fall back to DEFAULT_STATIONS on failure."""
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
            name = parts[4].strip() if parts[4].strip() else station_id
            loc = parts[6].strip().split()
            if len(loc) >= 4:
                try:
                    lat_val, lat_dir = float(loc[0]), loc[1].upper()
                    lon_val, lon_dir = float(loc[2]), loc[3].upper()
                    lat = lat_val if lat_dir == 'N' else -lat_val
                    lon = lon_val if lon_dir == 'E' else -lon_val
                    meta[station_id] = {'name': name, 'lat': lat, 'lon': lon}
                except Exception:
                    continue
        STATION_META = meta
    except Exception:
        STATION_META = DEFAULT_STATIONS.copy()
    return STATION_META


# =========================
# Run discovery
# =========================
def get_latest_run():
    """
    Find the most recent GFS wave run by testing for a known file.
    Checks today then yesterday for 18z,12z,06z,00z.
    """
    now = datetime.utcnow()
    for delta_day in [0, 1]:
        yyyymmdd = (now - timedelta(days=delta_day)).strftime("%Y%m%d")
        for hour in [18, 12, 6, 0]:
            run = f"{hour:02d}"
            test = (f"{NOAA_BASE}/gfs.{yyyymmdd}/{run}/wave/station/"
                    f"bulls.t{run}z/gfswave.51201.bull")
            try:
                r = requests.head(test, timeout=10)
                if r.status_code == 200:
                    return yyyymmdd, run
            except Exception:
                continue
    return None, None


# =========================
# .bull parser
# =========================
def parse_bull(station_id: str, target_tz_name: str | None = None):
    """
    Parse a station's .bull and return:
    (cycle_str, location_str, model_run_str, rows, effective_tz, error)
    rows: [date_str, time_str, s1_hs, s1_tp, s1_dir, ... s6_dir, combined_hs]
    """
    date_str, run_str = get_latest_run()
    if not date_str:
        return None, None, None, None, 'UTC', "No recent run found."

    url = (f"{NOAA_BASE}/gfs.{date_str}/{run_str}/wave/station/"
           f"bulls.t{run_str}z/gfswave.{station_id}.bull")
    try:
        resp = requests.get(url, timeout=15)
    except Exception:
        return None, None, None, None, 'UTC', f"Network error retrieving {station_id}"
    if resp.status_code != 200:
        return None, None, None, None, 'UTC', f"No .bull file found for {station_id}"

    lines = resp.text.splitlines()
    if not lines:
        return None, None, None, None, 'UTC', "Downloaded .bull file is empty."

    # Header fields
    cycle_line = next((l for l in lines if l.lower().strip().startswith("cycle")), None)
    location_line = next((l for l in lines if l.lower().strip().startswith("location")), None)
    if not cycle_line and len(lines) > 0:
        cycle_line = lines[0]
    if not location_line and len(lines) > 1:
        location_line = lines[1]
    cycle_str = cycle_line.strip() if cycle_line else ""
    location_str = location_line.strip() if location_line else ""

    # Lat/Lon -> timezone
    import re
    lat = lon = None
    tz_name = 'UTC'
    if location_str:
        m = re.search(r"\(([-+]?\d+(?:\.\d+)?)\s*([NS])\s+([-+]?\d+(?:\.\d+)?)\s*([EW])\)", location_str)
        if m:
            try:
                lat_val, lat_dir = float(m.group(1)), m.group(2).upper()
                lon_val, lon_dir = float(m.group(3)), m.group(4).upper()
                lat = lat_val if lat_dir == 'N' else -lat_val
                lon = lon_val if lon_dir == 'E' else -lon_val
            except Exception:
                lat = lon = None
    if lat is not None and lon is not None:
        try:
            tz_guess = tz_finder.timezone_at(lat=lat, lng=lon)
            if tz_guess:
                tz_name = tz_guess
        except Exception:
            tz_name = 'UTC'

    # Effective timezone (user override if valid)
    effective_tz_name = tz_name
    if target_tz_name:
        try:
            _ = pytz.timezone(target_tz_name)
            effective_tz_name = target_tz_name
        except Exception:
            pass

    # Detect new vs old format
    uses_day_hour_format = any("day &" in line.lower() for line in lines[:10])
    rows = []

    # Parse cycle timestamp
    m_cycle = re.search(r"(\d{8})\s*(\d{2})", cycle_str or "")
    cycle_date_str = date_str
    cycle_hour_str = run_str
    if m_cycle:
        cycle_date_str = m_cycle.group(1)
        cycle_hour_str = m_cycle.group(2)
    try:
        cycle_dt_utc = datetime.strptime(f"{cycle_date_str} {cycle_hour_str}", "%Y%m%d %H")
    except Exception:
        cycle_dt_utc = datetime.strptime(f"{date_str} {run_str}", "%Y%m%d %H")

    # Utility: monotonic builder for Day/Hour rows
    def build_monotonic_dt(last_dt_utc: datetime, day_val: int, hour_val: int) -> datetime:
        """
        Create a candidate datetime in UTC for the given (day, hour) relative to
        the cycle's month, then add whole days until it is > last_dt_utc.
        This guarantees strictly increasing datetimes across month wraps.
        """
        # Start from the first of the cycle month at requested hour
        base = datetime(cycle_dt_utc.year, cycle_dt_utc.month, 1, hour_val)
        # Move forward (day_val - 1) days; this auto-rolls into next month if needed
        candidate = base + timedelta(days=max(0, day_val - 1))
        # Ensure strictly increasing sequence
        while candidate <= last_dt_utc:
            candidate += timedelta(days=1)
        return candidate

    if uses_day_hour_format:
        # Model run (formatted in effective timezone)
        try:
            model_run_local = cycle_dt_utc.replace(tzinfo=UTC).astimezone(pytz.timezone(effective_tz_name))
        except Exception:
            model_run_local = cycle_dt_utc
        try:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %-d, %Y %I:%M %p")
        except Exception:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %d, %Y %I:%M %p").lstrip('0')

        M_TO_FT = 3.28084
        # Set last_dt to just before the model run so the first row may equal or exceed it
        last_dt_utc = cycle_dt_utc - timedelta(hours=1)

        for line in lines:
            raw = line.strip()
            if not raw.startswith("|"):
                continue
            if "Hst" in raw or "---" in raw:
                continue

            parts = [p.strip() for p in line.split("|") if p.strip()]
            if not parts:
                continue

            dh = parts[0].split()
            if len(dh) < 2:
                continue
            try:
                day_val = int(dh[0])
                hour_val = int(dh[1])
            except ValueError:
                continue

            # Build a strictly increasing UTC forecast time
            forecast_dt_utc = build_monotonic_dt(last_dt_utc, day_val, hour_val)
            last_dt_utc = forecast_dt_utc  # advance cursor

            # Combined height (meters)
            combined_hs_m = None
            hst_tokens = parts[1].split()
            if hst_tokens:
                try:
                    combined_hs_m = float(hst_tokens[0].replace('*', ''))
                except ValueError:
                    combined_hs_m = None

            # Parse swell groups
            swell_groups = []
            for field in parts[2:]:
                if not field:
                    swell_groups.append((None, None, None)); continue
                toks = [t.replace('*', '') for t in field.split() if t.replace('*', '') != ""]
                if len(toks) < 3:
                    swell_groups.append((None, None, None))
                else:
                    try:
                        hs_m = float(toks[0]); tp = float(toks[1]); dir_raw = int(float(toks[2]))
                        swell_groups.append((hs_m, tp, (dir_raw + 180) % 360))
                    except ValueError:
                        swell_groups.append((None, None, None))

            while len(swell_groups) < 6:
                swell_groups.append((None, None, None))
            if len(swell_groups) > 6:
                swell_groups = swell_groups[:6]

            # Convert UTC -> effective local
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

            # Assemble row (convert Hs from m->ft)
            row = [date_str_local, time_str_local]
            for hs_m, tp, ddir in swell_groups:
                if hs_m is None:
                    row.extend([None, None, None])
                else:
                    row.extend([hs_m * M_TO_FT, tp, ddir])
            row.append((combined_hs_m * M_TO_FT) if combined_hs_m is not None else None)
            rows.append(row)

    else:
        # Older "Hr" format (already monotonic by definition)
        # Locate data start
        start_idx = None
        for i, line in enumerate(lines):
            if line.strip().startswith("Hr"):
                start_idx = i + 1
                break
        if start_idx is None:
            return cycle_str, location_str, None, None, effective_tz_name, "Data section not found in .bull file."

        try:
            model_run_local = cycle_dt_utc.replace(tzinfo=UTC).astimezone(pytz.timezone(effective_tz_name))
        except Exception:
            model_run_local = cycle_dt_utc
        try:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %-d, %Y %I:%M %p")
        except Exception:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %d, %Y %I:%M %p").lstrip('0')

        for line in lines[start_idx:]:
            parts = line.split()
            if len(parts) < 20:
                continue
            try:
                hr_offset = float(parts[0])
            except ValueError:
                continue

            utc_dt = cycle_dt_utc + timedelta(hours=hr_offset)
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
            idx = 6
            for _ in range(6):
                hs = tp = ddir = None
                got = 0
                while got < 3 and idx < len(parts):
                    tok = parts[idx].replace('*', '')
                    idx += 1
                    if not tok:
                        continue
                    if got == 0:
                        try:
                            hs = float(tok) * 3.28084; got += 1; continue
                        except ValueError:
                            continue
                    if got == 1:
                        try:
                            tp = float(tok); got += 1; continue
                        except ValueError:
                            continue
                    if got == 2:
                        try:
                            ddir = (int(float(tok)) + 180) % 360; got += 1; continue
                        except ValueError:
                            continue
                row.extend([hs if got == 3 else None,
                            tp if got == 3 else None,
                            ddir if got == 3 else None])
            # combined Hs
            comb = None
            for tok in reversed(parts):
                t = tok.replace('*', '')
                if not t:
                    continue
                try:
                    comb = float(t) * 3.28084
                    break
                except ValueError:
                    continue
            row.append(comb)
            rows.append(row)

    # ---- Round numeric values for presentation ----
    for r in rows:
        j = 2
        for _ in range(6):
            if r[j] is not None:        # Hs
                r[j] = round(r[j], 2)
            j += 1
            if r[j] is not None:        # Tp
                r[j] = round(r[j], 1)
            j += 1
            if r[j] is not None:        # Dir
                try:
                    r[j] = int(round(r[j]))
                except Exception:
                    pass
            j += 1
        if r[-1] is not None:           # Combined
            r[-1] = round(r[-1], 2)

    if not rows:
        return cycle_str, location_str, None, None, effective_tz_name, "No data rows parsed from .bull file."

    # SUCCESS
    return cycle_str, location_str, model_run_str if 'model_run_str' in locals() else None, rows, effective_tz_name, None


# =========================
# Table builder (unchanged)
# =========================
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
    for i, col in enumerate(group_colors, 1):
        html += (f'<th colspan="3" style="background-color:{col["header"]}; color:white; '
                 f'text-align:center;">Swell {i}</th>')
    html += f'<th style="background-color:{combined_colors["header"]}; color:white; text-align:center;">Combined</th>'
    html += '</tr>\n<tr>'
    hs_unit = '(ft)' if unit == 'US' else '(m)'
    for col in group_colors:
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Hs<br>{hs_unit}</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Tp<br>(s)</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Dir<br>(d)</th>'
    html += f'<th style="background-color:{combined_colors["subheader"]}; text-align:center;">Hs<br>{hs_unit}</th>'
    html += '</tr>\n'

    for row in rows:
        # Day vs night styling (kept as in prior working version)
        try:
            t = datetime.strptime(row[1], "%I:%M %p").time()
        except Exception:
            try:
                t = datetime.strptime(row[1], "%I:%M:%S %p").time()
            except Exception:
                t = None
        bold_start = datetime.strptime("6:00:00 AM", "%I:%M:%S %p").time()
        bold_end   = datetime.strptime("7:00:00 PM", "%I:%M:%S %p").time()
        dashed_eve = datetime.strptime("8:00:00 PM", "%I:%M:%S %p").time()
        dashed_mrn = datetime.strptime("5:00:00 AM", "%I:%M:%S %p").time()

        border = ""; weight = "normal"
        if t is not None:
            if bold_start <= t <= bold_end:
                border = "border:1px solid #000;"; weight = "bold"
            elif t >= dashed_eve or t <= dashed_mrn:
                border = "border:1px dashed #999;"

        html += '<tr>'
        html += f'<td style="font-weight:bold; {border} padding:4px 8px;">{row[0]}</td>'
        html += f'<td style="font-weight:{weight}; {border} padding:4px 8px;">{row[1]}</td>'

        idx = 2
        for col in group_colors:
            # Hs
            v = row[idx]; idx += 1
            hs = "" if v is None else (f"{v:.2f}" if unit == 'US' else f"{(v/3.28084):.2f}")
            html += (f'<td style="background-color:{col["data"]}; text-align:right; '
                     f'font-weight:{weight}; {border} padding:4px 8px;">{hs}</td>')
            # Tp
            v = row[idx]; idx += 1
            tp = "" if v is None else f"{v:.1f}"
            html += (f'<td style="background-color:{col["data"]}; text-align:right; '
                     f'font-weight:{weight}; {border} padding:4px 8px;">{tp}</td>')
            # Dir
            v = row[idx]; idx += 1
            dd = "" if v is None else f"{v}"
            html += (f'<td style="background-color:{col["data"]}; text-align:right; '
                     f'font-weight:{weight}; {border} padding:4px 8px;">{dd}</td>')

        # Combined
        v = row[-1]
        comb = "" if v is None else (f"{v:.2f}" if unit == 'US' else f"{(v/3.28084):.2f}")
        html += (f'<td style="background-color:{combined_colors["data"]}; text-align:right; '
                 f'font-weight:{weight}; {border} padding:4px 8px;">{comb}</td>')
        html += '</tr>\n'

    html += '</table>'
    return html


# =========================
# Graph helper (unchanged)
# =========================
def _fmt_short(dt_obj: datetime) -> str:
    try:
        return dt_obj.strftime("%-m/%-d/%y %I:%M %p").replace(" 0", " ")
    except Exception:
        s = dt_obj.strftime("%m/%d/%y %I:%M %p")
        m, d, rest = s.split('/', 2)
        return f"{int(m)}/{int(d)}/{rest}"


def build_graph_payload(rows: list[list], unit: str) -> dict:
    labels = []
    height = {f"s{i}": [] for i in range(1, 7)}
    period = {f"s{i}": [] for i in range(1, 7)}
    direction = {f"s{i}": [] for i in range(1, 7)}
    combined = []
    to_m = (unit != 'US')

    for r in rows:
        d_str, t_str = r[0], r[1]
        dt = None
        try:
            d = datetime.strptime(d_str, "%A, %B %d, %Y")
        except Exception:
            d = None
        if d:
            try:
                tm = datetime.strptime(t_str, "%I:%M %p").time()
            except Exception:
                try:
                    tm = datetime.strptime(t_str, "%I:%M:%S %p").time()
                except Exception:
                    tm = None
            if tm:
                dt = datetime.combine(d.date(), tm)
        labels.append(_fmt_short(dt) if dt else f"{d_str} {t_str}")

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


# =========================
# Routes
# =========================
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
    selected_lat = selected_lon = None

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
            sid = str(selected_station).strip()
            if sid in coords_map:
                selected_lat = coords_map[sid]['lat']; selected_lon = coords_map[sid]['lon']
            elif location_str:
                import re
                m = re.search(r"\(\s*([-+]?\d+(?:\.\d+)?)\s*([NS])\s+([-+]?\d+(?:\.\d+)?)\s*([EW])\)", location_str)
                if m:
                    try:
                        lat = float(m.group(1)); lat_dir = m.group(2).upper()
                        lon = float(m.group(3)); lon_dir = m.group(4).upper()
                        selected_lat = lat if lat_dir == 'N' else -lat
                        selected_lon = lon if lon_dir == 'E' else -lon
                    except Exception:
                        selected_lat = selected_lon = None

    return render_template(
        "index.html",
        stations=stations,
        selected_station=selected_station,
        timezones=timezones,
        selected_tz=selected_tz or tz_label,
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
