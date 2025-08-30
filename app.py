from flask import Flask, render_template, request, jsonify
import requests
import json
import os
from datetime import datetime, timedelta
import calendar
import pytz
from timezonefinder import TimezoneFinder

app = Flask(__name__)

# Instantiate once
tz_finder = TimezoneFinder()

# Caches
STATION_META = None          # station_id -> { name, lat, lon }
STATION_COORDS = None        # station_id -> { lat, lon }
BULLET_STATIONS = None       # set of ids with .bull

# NOAA base
NOAA_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"

UTC = pytz.utc

# Fallback stations (unchanged)
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


# -------------------- helpers: static station coords --------------------
def load_station_coords() -> dict:
    """Load station_coords.json (if present)."""
    global STATION_COORDS
    if STATION_COORDS is not None:
        return STATION_COORDS
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, "station_coords.json")
    coords = {}
    try:
        with open(path, "r") as f:
            data = json.load(f)
            for sid, info in data.items():
                try:
                    coords[str(sid).strip()] = {
                        "lat": float(info.get("lat")),
                        "lon": float(info.get("lon")),
                    }
                except Exception:
                    continue
    except Exception:
        coords = {}
    STATION_COORDS = coords
    return STATION_COORDS


# -------------------- station list from station_list.json --------------------
def get_station_list() -> list[tuple[str, str]]:
    stations: list[tuple[str, str]] = []
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base_dir, "station_list.json")
        with open(path, "r") as f:
            ids = json.load(f)
        meta = {}
        try:
            meta = load_station_metadata()
        except Exception:
            pass
        for sid in ids:
            sid = str(sid).strip()
            if not sid:
                continue
            name = meta.get(sid, {}).get("name", sid)
            stations.append((sid, name))
        if stations:
            return stations
    except Exception:
        pass
    # fallback
    return [(sid, info.get("name", sid)) for sid, info in DEFAULT_STATIONS.items()]


def get_stations_data():
    """[{id,name,lat,lon}] for /stations.json."""
    global stations_data_cache
    if stations_data_cache is not None:
        return stations_data_cache
    id_list = [sid for sid, _ in get_station_list()]
    meta = {}
    try:
        meta = load_station_metadata()
    except Exception:
        pass
    coords_map = load_station_coords()
    data = []
    for sid in id_list:
        name = meta.get(sid, {}).get("name", sid)
        lat = lon = None
        if sid in coords_map:
            lat = coords_map[sid]["lat"]
            lon = coords_map[sid]["lon"]
        elif sid in DEFAULT_STATIONS:
            lat = DEFAULT_STATIONS[sid]["lat"]
            lon = DEFAULT_STATIONS[sid]["lon"]
        if lat is not None and lon is not None:
            data.append({"id": sid, "name": name, "lat": lat, "lon": lon})
    stations_data_cache = data
    return data


@app.route("/stations.json")
def stations_json():
    return jsonify(get_stations_data())


# -------------------- load NDBC station metadata --------------------
def load_station_metadata():
    global STATION_META
    if STATION_META is not None:
        return STATION_META
    url = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
    meta = {}
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        for line in r.text.splitlines():
            if not line or line.startswith("#"):
                continue
            parts = line.split("|")
            if len(parts) < 7:
                continue
            sid = parts[0].strip()
            if not sid:
                continue
            name = parts[4].strip() or sid
            loc = parts[6].strip().split()
            if len(loc) >= 4:
                try:
                    lat = float(loc[0]) * (1 if loc[1].upper() == "N" else -1)
                    lon = float(loc[2]) * (1 if loc[3].upper() == "E" else -1)
                    meta[sid] = {"name": name, "lat": lat, "lon": lon}
                except Exception:
                    continue
        STATION_META = meta
    except Exception:
        STATION_META = DEFAULT_STATIONS.copy()
    return STATION_META


# -------------------- discover latest run --------------------
def get_latest_run():
    now = datetime.utcnow()
    for delta_day in [0, 1]:
        day = (now - timedelta(days=delta_day)).strftime("%Y%m%d")
        for hour in [18, 12, 6, 0]:
            run = f"{hour:02d}"
            test = f"{NOAA_BASE}/gfs.{day}/{run}/wave/station/bulls.t{run}z/gfswave.51201.bull"
            try:
                if requests.head(test, timeout=10).status_code == 200:
                    return day, run
            except Exception:
                pass
    return None, None


# -------------------- .bull parser (month-safe) --------------------
def _tz_from_latlon(lat, lon, default="UTC"):
    try:
        tzn = tz_finder.timezone_at(lat=lat, lng=lon)
        if tzn:
            return tzn
    except Exception:
        pass
    return default


def _absolute_dt_from_day_hour(cycle_dt_utc: datetime, day_val: int, hour_val: int) -> datetime:
    """
    Convert bulletin 'day hour' to an absolute UTC datetime with correct month rollover.
    Rule: if day_val < cycle_day -> next month; else current month.
    If that still predates the cycle (e.g. same day but earlier hour), bump one day.
    """
    y, m = cycle_dt_utc.year, cycle_dt_utc.month
    if day_val < cycle_dt_utc.day:
        m += 1
        if m > 12:
            m = 1
            y += 1
    # clamp day to valid range
    last = calendar.monthrange(y, m)[1]
    d = min(day_val, last)
    try:
        dt = datetime(y, m, d, hour_val)
    except ValueError:
        # extremely rare: clamp again if necessary
        d = min(d, calendar.monthrange(y, m)[1])
        dt = datetime(y, m, d, hour_val)
    if dt < cycle_dt_utc:
        dt = dt + timedelta(days=1)
    return dt


def parse_bull(station_id: str, target_tz_name: str | None = None):
    date_str, run_str = get_latest_run()
    if not date_str:
        return None, None, None, None, "UTC", "No recent run found."

    url = f"{NOAA_BASE}/gfs.{date_str}/{run_str}/wave/station/bulls.t{run_str}z/gfswave.{station_id}.bull"
    try:
        resp = requests.get(url, timeout=15)
    except Exception:
        return None, None, None, None, "UTC", f"Network error retrieving {station_id}"
    if resp.status_code != 200:
        return None, None, None, None, "UTC", f"No .bull file found for {station_id}"

    lines = resp.text.splitlines()
    if not lines:
        return None, None, None, None, "UTC", "Downloaded .bull file is empty."

    # header
    cycle_line = next((l for l in lines if l.lower().strip().startswith("cycle")), lines[0] if lines else "")
    location_line = next((l for l in lines if l.lower().strip().startswith("location")), lines[1] if len(lines) > 1 else "")
    cycle_str = cycle_line.strip()
    location_str = location_line.strip()

    # coords -> tz
    import re
    lat = lon = None
    if location_str:
        m = re.search(r"\(([-+]?\d+(?:\.\d+)?)\s*([NS])\s+([-+]?\d+(?:\.\d+)?)\s*([EW])\)", location_str)
        if m:
            lat = float(m.group(1)) * (1 if m.group(2).upper() == "N" else -1)
            lon = float(m.group(3)) * (1 if m.group(4).upper() == "E" else -1)
    buoy_tz = _tz_from_latlon(lat, lon, "UTC") if lat is not None and lon is not None else "UTC"
    effective_tz_name = buoy_tz
    if target_tz_name:
        try:
            _ = pytz.timezone(target_tz_name)
            effective_tz_name = target_tz_name
        except Exception:
            pass

    # format detection
    uses_day_hour = any("day &" in ln.lower() for ln in lines[:10])

    rows = []
    model_run_str = None

    if uses_day_hour:
        # parse cycle UTC
        m = re.search(r"(\d{8})\s*(\d{2})", cycle_str)
        cyc_date = date_str
        cyc_hour = run_str
        if m:
            cyc_date, cyc_hour = m.group(1), m.group(2)
        cycle_dt_utc = datetime.strptime(f"{cyc_date} {cyc_hour}", "%Y%m%d %H")

        # model run in local tz (string kept for graph/table header only)
        try:
            model_run_local = cycle_dt_utc.replace(tzinfo=UTC).astimezone(pytz.timezone(effective_tz_name))
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %d, %Y %I:%M %p").lstrip('0')
        except Exception:
            model_run_str = None

        M_TO_FT = 3.28084
        for line in lines:
            s = line.strip()
            if not s.startswith("|") or "Hst" in s or "---" in s:
                continue
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) < 2:
                continue

            # day & hour
            dh = parts[0].split()
            if len(dh) < 2:
                continue
            try:
                day_val = int(dh[0]); hour_val = int(dh[1])
            except ValueError:
                continue

            # absolute UTC forecast time (month-safe)
            forecast_dt_utc = _absolute_dt_from_day_hour(cycle_dt_utc, day_val, hour_val)

            # localize to effective tz
            try:
                local_tz = pytz.timezone(effective_tz_name)
            except Exception:
                local_tz = UTC
            local_dt = forecast_dt_utc.replace(tzinfo=UTC).astimezone(local_tz)
            date_str_local = local_dt.strftime("%A, %B %d, %Y").lstrip('0')
            time_str_local = local_dt.strftime("%I:%M %p").lstrip('0')

            # combined Hs (meters in file)
            combined_hs_m = None
            try:
                combined_hs_m = float(parts[1].split()[0].replace("*", ""))
            except Exception:
                combined_hs_m = None

            # swells
            swell_groups = []
            for fld in parts[2:]:
                if not fld:
                    swell_groups.append((None, None, None))
                    continue
                raw = [t.replace("*", "") for t in fld.split() if t.replace("*", "")]
                if len(raw) < 3:
                    swell_groups.append((None, None, None))
                else:
                    try:
                        hs_m = float(raw[0])
                        tp = float(raw[1])
                        dr = int(float(raw[2]))
                        swell_groups.append((hs_m, tp, (dr + 180) % 360))
                    except Exception:
                        swell_groups.append((None, None, None))
            while len(swell_groups) < 6:
                swell_groups.append((None, None, None))
            if len(swell_groups) > 6:
                swell_groups = swell_groups[:6]

            row = [date_str_local, time_str_local]
            for hs_m, tp, dd in swell_groups:
                row.extend([(hs_m * M_TO_FT) if hs_m is not None else None,
                            tp if tp is not None else None,
                            dd if dd is not None else None])
            row.append((combined_hs_m * M_TO_FT) if combined_hs_m is not None else None)
            rows.append(row)

    else:
        # Older format (unchanged from your working version)
        start_idx = None
        for idx, ln in enumerate(lines):
            if ln.strip().startswith("Hr"):
                start_idx = idx + 1
                break
        if start_idx is None:
            return cycle_str, location_str, None, None, effective_tz_name, "Data section not found in .bull file."

        m = re.search(r"(\d{8})\s*(\d{2})", cycle_str)
        cyc_date = date_str
        cyc_hour = run_str
        if m:
            cyc_date, cyc_hour = m.group(1), m.group(2)
        cycle_dt_utc = datetime.strptime(f"{cyc_date} {cyc_hour}", "%Y%m%d %H")

        try:
            local = cycle_dt_utc.replace(tzinfo=UTC).astimezone(pytz.timezone(effective_tz_name))
            model_run_str = "Model Run: " + local.strftime("%A, %B %d, %Y %I:%M %p").lstrip('0')
        except Exception:
            model_run_str = None

        for ln in lines[start_idx:]:
            parts = ln.split()
            if len(parts) < 20:
                continue
            try:
                hr_offset = float(parts[0])
            except ValueError:
                continue
            utc_dt = cycle_dt_utc + timedelta(hours=hr_offset)
            try:
                lt = pytz.timezone(effective_tz_name)
            except Exception:
                lt = UTC
            local_dt = utc_dt.replace(tzinfo=UTC).astimezone(lt)
            date_str_local = local_dt.strftime("%A, %B %d, %Y").lstrip('0')
            time_str_local = local_dt.strftime("%I:%M %p").lstrip('0')
            row = [date_str_local, time_str_local]

            idx_base = 6
            for _ in range(6):
                hs = tp = dd = None
                got = 0
                while got < 3 and idx_base < len(parts):
                    tok = parts[idx_base].replace("*", "")
                    idx_base += 1
                    if not tok:
                        continue
                    try:
                        if got == 0:
                            hs = float(tok) * 3.28084; got += 1; continue
                        if got == 1:
                            tp = float(tok); got += 1; continue
                        if got == 2:
                            dd = (int(float(tok)) + 180) % 360; got += 1; continue
                    except Exception:
                        continue
                if got < 3:
                    row.extend([None, None, None])
                else:
                    row.extend([hs, tp, dd])

            comb = None
            for tok in reversed(parts):
                t = tok.replace("*", "")
                if not t:
                    continue
                try:
                    comb = float(t) * 3.28084
                    break
                except Exception:
                    continue
            row.append(comb)
            rows.append(row)

    # rounding (as before)
    for r in rows:
        i = 2
        for _ in range(6):
            if r[i] is not None: r[i] = round(r[i], 2)
            i += 1
            if r[i] is not None: r[i] = round(r[i], 1)
            i += 1
            if r[i] is not None:
                try:
                    r[i] = int(round(r[i]))
                except Exception:
                    pass
            i += 1
        if r[-1] is not None:
            r[-1] = round(r[-1], 2)

    if not rows:
        return cycle_str, location_str, model_run_str, None, effective_tz_name, "No data rows parsed."

    return cycle_str, location_str, model_run_str, rows, effective_tz_name, None


# -------------------- HTML table (unchanged) --------------------
def build_html_table(cycle_str, location_str, model_run_str, rows, tz_label, unit):
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
    html += '<tr><th rowspan="2">Date</th><th rowspan="2">Time</th>'
    for idx, col in enumerate(group_colors, start=1):
        html += f'<th colspan="3" style="background-color:{col["header"]}; color:white; text-align:center;">Swell {idx}</th>'
    html += f'<th style="background-color:{combined_colors["header"]}; color:white; text-align:center;">Combined</th></tr>\n'
    html += '<tr>'
    hs_unit_label = '(ft)' if unit == 'US' else '(m)'
    for col in group_colors:
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Hs<br>{hs_unit_label}</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Tp<br>(s)</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Dir<br>(d)</th>'
    html += f'<th style="background-color:{combined_colors["subheader"]}; text-align:center;">Hs<br>{hs_unit_label}</th></tr>\n'

    from datetime import datetime as _dt
    for row in rows:
        try:
            parsed_time = _dt.strptime(row[1], "%I:%M %p").time()
        except Exception:
            try:
                parsed_time = _dt.strptime(row[1], "%I:%M:%S %p").time()
            except Exception:
                parsed_time = None
        bold_start = _dt.strptime("6:00 AM", "%I:%M %p").time()
        bold_end = _dt.strptime("7:00 PM", "%I:%M %p").time()
        dashed_start = _dt.strptime("8:00 PM", "%I:%M %p").time()
        dashed_end = _dt.strptime("5:00 AM", "%I:%M %p").time()
        border_style = ""
        fw = "normal"
        if parsed_time is not None:
            if bold_start <= parsed_time <= bold_end:
                border_style = "border:1px solid #000;"; fw = "bold"
            elif parsed_time >= dashed_start or parsed_time <= dashed_end:
                border_style = "border:1px dashed #999;"

        html += '<tr>'
        html += f'<td style="font-weight:bold; {border_style} padding:4px 8px;">{row[0]}</td>'
        html += f'<td style="font-weight:{fw}; {border_style} padding:4px 8px;">{row[1]}</td>'
        i = 2
        for col in group_colors:
            val = row[i]; i += 1
            hs_str = "" if val is None else f"{(val if unit=='US' else val/3.28084):.2f}"
            html += f'<td style="background-color:{col["data"]}; text-align:right; font-weight:{fw}; {border_style} padding:4px 8px;">{hs_str}</td>'
            val = row[i]; i += 1
            html += f'<td style="background-color:{col["data"]}; text-align:right; font-weight:{fw}; {border_style} padding:4px 8px;">{"" if val is None else f"{val:.1f}"}}</td>'
            val = row[i]; i += 1
            html += f'<td style="background-color:{col["data"]}; text-align:right; font-weight:{fw}; {border_style} padding:4px 8px;">{"" if val is None else val}</td>'
        val = row[-1]
        comb = "" if val is None else f"{(val if unit=='US' else val/3.28084):.2f}"
        html += f'<td style="background-color:{combined_colors["data"]}; text-align:right; font-weight:{fw}; {border_style} padding:4px 8px;">{comb}</td>'
        html += '</tr>\n'
    html += '</table>'
    return html


# -------------------- graph payload --------------------
def _fmt_short(dt_obj: datetime) -> str:
    try:
        return dt_obj.strftime("%-m/%-d/%y %I:%M %p").replace(" 0", " ")
    except Exception:
        s = dt_obj.strftime("%m/%d/%y %I:%M %p")
        m, d, rest = s.split("/", 2)
        return f"{int(m)}/{int(d)}/{rest}"


def build_graph_payload(rows: list[list], unit: str) -> dict:
    labels = []
    height = {f"s{i}": [] for i in range(1, 7)}
    period = {f"s{i}": [] for i in range(1, 7)}
    direction = {f"s{i}": [] for i in range(1, 7)}
    combined = []

    to_m = (unit != "US")

    for r in rows:
        d_str, t_str = r[0], r[1]
        # Parse back to datetime for consistent, sortable labels
        dt_date = datetime.strptime(d_str, "%A, %B %d, %Y")
        try:
            tm = datetime.strptime(t_str, "%I:%M %p").time()
        except Exception:
            try:
                tm = datetime.strptime(t_str, "%I:%M:%S %p").time()
            except Exception:
                tm = None
        if tm:
            dt_date = datetime.combine(dt_date.date(), tm)
        labels.append(_fmt_short(dt_date))

        idx = 2
        for i in range(1, 7):
            hs = r[idx]; tp = r[idx + 1]; dd = r[idx + 2]
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


# -------------------- routes --------------------
@app.route("/", methods=["GET", "POST"])
def index():
    stations = get_station_list()
    timezones = sorted(pytz.common_timezones)
    unit_options = ["US", "Metric"]

    selected_station = request.values.get("station", "") or "51201"
    selected_tz = request.values.get("tz", "")
    selected_unit = request.values.get("unit", "US") or "US"
    selected_view = request.values.get("view", "Table") or "Table"

    table_html = None
    graph_data = None
    error = None
    tz_label = ""
    selected_lat = None
    selected_lon = None
    graph_header = None  # NEW

    cycle_str = location_str = model_run_str = None

    if selected_station:
        cycle_str, location_str, model_run_str, rows, effective_tz_name, parse_error = parse_bull(
            selected_station, selected_tz or None
        )
        error = parse_error
        if rows is not None:
            tz_label = effective_tz_name
            table_html = build_html_table(cycle_str, location_str, model_run_str, rows, tz_label, selected_unit)
            graph_data = build_graph_payload(rows, selected_unit)
            graph_header = {
                "cycle": cycle_str or "",
                "location": location_str or "",
                "tz": tz_label or "",
            }

            # lat/lon for centering a marker if desired
            coords_map = load_station_coords()
            if str(selected_station) in coords_map:
                ll = coords_map[str(selected_station)]
                selected_lat, selected_lon = ll["lat"], ll["lon"]
            elif location_str:
                import re
                m = re.search(r"\(\s*([-+]?\d+(?:\.\d+)?)\s*([NS])\s+([-+]?\d+(?:\.\d+)?)\s*([EW])\)", location_str)
                if m:
                    try:
                        lat = float(m.group(1)) * (1 if m.group(2).upper() == "N" else -1)
                        lon = float(m.group(3)) * (1 if m.group(4).upper() == "E" else -1)
                        selected_lat, selected_lon = lat, lon
                    except Exception:
                        pass

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
        graph_header=graph_header,
        table_html=table_html,
        error=error,
        selected_lat=selected_lat,
        selected_lon=selected_lon,
    )


if __name__ == "__main__":
    app.run(debug=True)
