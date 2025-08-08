from flask import Flask, render_template, request, send_file
import pandas as pd  # still used for fallback Excel generation if needed
import requests
from io import BytesIO
from datetime import datetime, timedelta
import pytz
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment
from timezonefinder import TimezoneFinder

app = Flask(__name__)

# Instantiate a timezone finder object once to avoid repeated initialization.
tz_finder = TimezoneFinder()

# Caches for station metadata and available bulletin stations. These will be populated
# on demand to avoid repeated network requests. See load_station_metadata() and
# get_bullet_station_ids().
STATION_META = None  # Maps station_id -> { 'name': str, 'lat': float, 'lon': float }
BULLET_STATIONS = None  # Set of station_ids that currently have a .bull file available

# ===== NOAA Station List URL =====
# This URL points to the bulls.readme file, which lists the available buoy stations
STATION_LIST_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/wave/station/bulls.readme"

# ===== Base NOAA URL Pattern =====
# The pattern for locating GFS wave data. The date (YYYYMMDD) and run hour (HH) will be inserted.
NOAA_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"

# ===== Timezones =====
# The app displays times in UTC and Hawaii Standard Time (HST). HST is used instead of the local timezone because many users
# of buoy data in Hawaii prefer local time display. The user’s timezone is configured via pytz.
HST = pytz.timezone("Pacific/Honolulu")
UTC = pytz.utc


def get_station_list():
    """
    Build a list of available stations by cross-referencing station metadata with the
    currently available GFS wave bulletins.  This function first ensures that the
    station metadata and the list of available bulletin station IDs are loaded, then
    constructs a list of (station_id, station_name) tuples for display in the UI.

    Returns:
        list: A list of tuples (id, name) sorted by ID.  If metadata cannot be
        loaded, a fallback list containing just ('51201', 'Example Station') is returned.
    """
    try:
        ids = get_bullet_station_ids()
        meta = load_station_metadata()
        stations = []
        for sid in sorted(ids):
            info = meta.get(sid)
            # Use the provided name if available; otherwise just use the ID
            name = info.get('name', sid) if info else sid
            stations.append((sid, name))
        # If no stations found, provide fallback
        if not stations:
            return [("51201", "Example Station")]
        return stations
    except Exception:
        return [("51201", "Example Station")]


def load_station_metadata():
    """
    Load buoy station metadata including latitude, longitude and station name from the
    NDBC station table.  The station table is a pipe-delimited text file where each
    row has the format:

        STATION_ID | OWNER | TTYPE | HULL | NAME | PAYLOAD | LOCATION | TIMEZONE | FORECAST | NOTE

    This function downloads the station table once and parses it into a dictionary
    mapping station IDs to dictionaries with keys 'name', 'lat' and 'lon'.  Lat/lon
    values are parsed from the LOCATION column in decimal degrees (e.g., "21.671 N
    158.118 W ...").  South and west latitudes/longitudes are stored as negative.

    Returns:
        dict: A dictionary keyed by station ID with values {'name': str, 'lat': float, 'lon': float}.
    """
    global STATION_META
    if STATION_META is not None:
        return STATION_META
    station_url = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
    meta = {}
    try:
        res = requests.get(station_url, timeout=30)
        res.raise_for_status()
        for line in res.text.splitlines():
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            parts = line.split('|')
            if len(parts) < 7:
                continue
            station_id = parts[0].strip()
            # Skip non-numeric station ids to focus on buoy-like stations
            if not station_id:
                continue
            name = parts[4].strip() if parts[4].strip() else station_id
            location_field = parts[6].strip()
            # Parse the decimal latitude and longitude at the beginning of the location field
            # Example: "21.671 N 158.118 W (21°40'15" N 158°7'5" W)"
            tokens = location_field.split()
            if len(tokens) >= 4:
                try:
                    lat_val = float(tokens[0])
                    lat_dir = tokens[1].upper()
                    lon_val = float(tokens[2])
                    lon_dir = tokens[3].upper()
                    lat = lat_val if lat_dir == 'N' else -lat_val
                    lon = lon_val if lon_dir == 'E' else -lon_val
                    meta[station_id] = {'name': name, 'lat': lat, 'lon': lon}
                except Exception:
                    # Skip entries with unparseable lat/lon
                    continue
        STATION_META = meta
        return STATION_META
    except Exception:
        STATION_META = {}
        return STATION_META


def get_bullet_station_ids():
    """
    Retrieve the set of station IDs for which a GFS wave bulletin is currently available.
    This function inspects the directory listing of the most recent run and extracts
    station identifiers from the filenames of the .bull files (e.g., gfswave.51201.bull).
    The result is cached to avoid repeated network calls.

    Returns:
        set: A set of station ID strings.
    """
    global BULLET_STATIONS
    if BULLET_STATIONS is not None:
        return BULLET_STATIONS
    date_str, run_str = get_latest_run()
    if not date_str:
        BULLET_STATIONS = set()
        return BULLET_STATIONS
    url = f"{NOAA_BASE}/gfs.{date_str}/{run_str}/wave/station/bulls.t{run_str}z/"
    ids = set()
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        # Extract station IDs from anchor hrefs
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(res.text, "html.parser")
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if href.startswith('gfswave.') and href.endswith('.bull'):
                # Format: gfswave.{station_id}.bull
                parts = href.split('.')
                if len(parts) >= 2:
                    sid = parts[1]
                    ids.add(sid)
        BULLET_STATIONS = ids
        return BULLET_STATIONS
    except Exception:
        BULLET_STATIONS = set()
        return BULLET_STATIONS


def get_latest_run():
    """
    Determine the most recent available GFS model run.

    This function checks the last two days for model runs in descending order of availability (18z, 12z, 06z, 00z).
    It returns the date as a string (YYYYMMDD) and the run hour (HH) as a two-digit string. If no recent run is
    available, it returns (None, None).
    """
    now = datetime.utcnow()
    run_hours = [18, 12, 6, 0]
    for delta_day in [0, 1]:
        check_date = now - timedelta(days=delta_day)
        yyyymmdd = check_date.strftime("%Y%m%d")
        for hour in run_hours:
            run_str = f"{hour:02d}"
            # Construct a URL for buoy 51201 as a test case to check run availability
            url = f"{NOAA_BASE}/gfs.{yyyymmdd}/{run_str}/wave/station/bulls.t{run_str}z/"
            test_file = f"{url}gfswave.51201.bull"
            resp = requests.head(test_file)
            # If the test file exists, we assume the run is valid for all buoys
            if resp.status_code == 200:
                return yyyymmdd, run_str
    # If no run is found in the last two days, return None
    return None, None


def parse_bull(station_id: str):
    """
    Fetch and parse the .bull file for a given station.

    The NOAA .bull files contain marine forecast data for buoy stations. This parser supports both the
    traditional "Hr" style bulletins as well as the newer "day & hour" style bulletins. The returned
    structure focuses on data relevant to the "Table View" worksheet: local date and time (HST),
    six swell groups (each with height, period and direction) and the combined sea height.

    Parameters:
        station_id (str): The buoy station identifier (e.g., '51201').

    Returns:
        tuple: A tuple of (cycle_str, location_str, rows, tz_name, error). If successful, error is None and rows is
               a list where each entry corresponds to a forecast row with the following order:
               [date_str, time_str, s1_hs, s1_tp, s1_dir, ..., s6_hs, s6_tp, s6_dir, combined_hs].
               cycle_str and location_str are strings extracted from the bulletin header. tz_name is the
               IANA timezone name for the buoy location (derived from its latitude/longitude) and is used
               to label the table. If the timezone cannot be determined, tz_name will be 'UTC'.
    """
    # Determine the latest available run (date and hour)
    date_str, run_str = get_latest_run()
    if not date_str:
        # No recent run available; return with UTC as the default timezone
        return None, None, None, 'UTC', "No recent run found."

    # Download the .bull file for this station and run
    bull_url = f"{NOAA_BASE}/gfs.{date_str}/{run_str}/wave/station/bulls.t{run_str}z/gfswave.{station_id}.bull"
    resp = requests.get(bull_url, timeout=10)
    if resp.status_code != 200:
        return None, None, None, 'UTC', f"No .bull file found for {station_id}"

    lines = resp.text.splitlines()
    if not lines:
        return None, None, None, 'UTC', "Downloaded .bull file is empty."

    # Attempt to extract the cycle and location lines from the header. The .bull header typically contains
    # lines like "Cycle    : 20250807 12 UTC" and "Location : 51201 (21.67N 158.12W)". We look for
    # these prefixes case-insensitively and fall back to the first lines if not found.
    cycle_line = next((l for l in lines if l.lower().startswith("cycle")), None)
    location_line = next((l for l in lines if l.lower().startswith("location")), None)
    if not cycle_line and len(lines) > 0:
        cycle_line = lines[0]
    if not location_line and len(lines) > 1:
        location_line = lines[1]
    cycle_str = cycle_line.strip() if cycle_line else ""
    location_str = location_line.strip() if location_line else ""

    # Determine the buoy's latitude and longitude from the location string. Example format:
    # "Location : 51201      (21.67N 158.12W)". We'll extract the numbers and hemisphere letters
    # within the parentheses. If parsing fails, lat and lon will remain None.
    import re
    lat = lon = None
    tz_name = 'UTC'
    if location_str:
        m = re.search(r"\(([-+]?\d+(?:\.\d+)?)\s*([NS])\s+([-+]?\d+(?:\.\d+)?)\s*([EW])\)", location_str)
        if m:
            try:
                lat_val = float(m.group(1))
                lat_dir = m.group(2)
                lon_val = float(m.group(3))
                lon_dir = m.group(4)
                lat = lat_val if lat_dir.upper() == 'N' else -lat_val
                lon = lon_val if lon_dir.upper() == 'E' else -lon_val
            except Exception:
                lat = lon = None
    # Determine the timezone using TimezoneFinder if coordinates were successfully parsed
    if lat is not None and lon is not None:
        try:
            tz_name_candidate = tz_finder.timezone_at(lat=lat, lng=lon)
            if tz_name_candidate:
                tz_name = tz_name_candidate
        except Exception:
            tz_name = 'UTC'

    # Detect file format: newer files contain a 'day & hour' notation near the top
    uses_day_hour_format = any("day &" in line.lower() for line in lines[:10])

    rows = []  # will hold the parsed forecast rows

    if uses_day_hour_format:
        # ----- Newer format parser: data rows delineated by '|', containing day, hour, Hst and up to six swells -----
        # Attempt to parse the cycle date/time (YYYYMMDD HH) from the cycle_str
        m = re.search(r"(\d{8})\s*(\d{2})", cycle_str)
        cycle_date_str = date_str
        cycle_hour_str = run_str
        if m:
            cycle_date_str = m.group(1)
            cycle_hour_str = m.group(2)
        try:
            cycle_dt_utc = datetime.strptime(f"{cycle_date_str} {cycle_hour_str}", "%Y%m%d %H")
        except Exception:
            cycle_dt_utc = datetime.strptime(f"{date_str} {run_str}", "%Y%m%d %H")
        # Conversion factor (meters to feet)
        M_TO_FT = 3.28084
        for line in lines:
            striped = line.strip()
            if not striped.startswith("|"):
                continue
            # Skip header or separator lines
            if "Hst" in striped or "---" in striped:
                continue
            # Split into fields; remove empty segments
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if not parts:
                continue
            # The first field holds day and hour
            day_hour = parts[0].split()
            if len(day_hour) < 2:
                continue
            try:
                day_val = int(day_hour[0])
                hour_val = int(day_hour[1])
            except ValueError:
                continue
            # Combined height (Hst) is in the second field; take the first numeric token
            # Remove any asterisk (*) that may precede the value before conversion
            hst_tokens = parts[1].split()
            combined_hs_m = None
            if hst_tokens:
                try:
                    combined_hs_m = float(hst_tokens[0].replace('*', ''))
                except ValueError:
                    combined_hs_m = None
            # Swell groups start at parts[2]; each has up to 3 values (hs, tp, dir)
            swell_groups = []
            for swell_field in parts[2:]:
                # Each swell field may contain asterisks as separate tokens marking flagged data.
                # We ignore any '*' tokens and use the following numeric values.
                if not swell_field:
                    swell_groups.append((None, None, None))
                    continue
                # Split the field into tokens and remove standalone '*' tokens.
                raw_tokens = swell_field.split()
                cleaned_tokens = []
                for tok in raw_tokens:
                    # Remove any '*' characters attached to numeric values (e.g., '*0.11').
                    tok_clean = tok.replace('*', '')
                    # Skip tokens that were just '*' (become empty after removal)
                    if tok_clean == '':
                        continue
                    cleaned_tokens.append(tok_clean)
                # We need at least three numeric tokens to parse Hs, Tp, Dir.
                if len(cleaned_tokens) < 3:
                    swell_groups.append((None, None, None))
                else:
                    try:
                        hs_val = float(cleaned_tokens[0])
                        tp_val = float(cleaned_tokens[1])
                        dir_raw = int(float(cleaned_tokens[2]))  # handle floats disguised as ints
                        # Correct direction by 180 degrees (mod 360)
                        dir_val = (dir_raw + 180) % 360
                        swell_groups.append((hs_val, tp_val, dir_val))
                    except ValueError:
                        swell_groups.append((None, None, None))
            # Ensure exactly six swell groups
            while len(swell_groups) < 6:
                swell_groups.append((None, None, None))
            if len(swell_groups) > 6:
                swell_groups = swell_groups[:6]
            # Compute forecast UTC datetime by replacing day and hour on the cycle date; adjust forward if earlier than cycle
            try:
                forecast_dt_utc = cycle_dt_utc.replace(day=day_val, hour=hour_val)
            except ValueError:
                # In case of invalid day (e.g., February 30), skip
                continue
            if forecast_dt_utc < cycle_dt_utc:
                # Adjust into the future if forecast time is before cycle time
                while forecast_dt_utc < cycle_dt_utc:
                    forecast_dt_utc += timedelta(days=1)
            # Convert to the local timezone for this buoy
            try:
                local_tz = pytz.timezone(tz_name)
            except Exception:
                local_tz = UTC
            local_dt = forecast_dt_utc.replace(tzinfo=UTC).astimezone(local_tz)
            # Format date and time strings in the desired presentation
            try:
                date_str_local = local_dt.strftime("%A, %B %-d, %Y")
            except Exception:
                # Fallback for platforms without %-d (e.g., Windows)
                date_str_local = local_dt.strftime("%A, %B %d, %Y").lstrip('0')
            time_str_local = local_dt.strftime("%I:%M:%S %p").lstrip('0')
            # Convert combined height to feet
            combined_hs_ft = None
            if combined_hs_m is not None:
                combined_hs_ft = combined_hs_m * M_TO_FT
            # Assemble row
            row = [date_str_local, time_str_local]
            for hs_m, tp_val, dir_val in swell_groups:
                if hs_m is None:
                    row.extend([None, None, None])
                else:
                    row.extend([hs_m * M_TO_FT, tp_val, dir_val])
            row.append(combined_hs_ft)
            rows.append(row)
    else:
        # ----- Older format parser: header contains "Hr" followed by swell data -----
        # Locate the line starting with 'Hr' to identify where data begins
        start_idx = None
        for idx, line in enumerate(lines):
            if line.strip().startswith("Hr"):
                start_idx = idx + 1
                break
        if start_idx is None:
            # If we cannot locate the data section, propagate an error along with the timezone name
            return cycle_str, location_str, None, tz_name, "Data section not found in .bull file."
        # For each subsequent line, parse the forecast hour and swell data
        for line in lines[start_idx:]:
            parts = line.split()
            if len(parts) < 20:
                continue
            try:
                hr_offset = float(parts[0])
            except ValueError:
                continue
            utc_dt = datetime.strptime(f"{date_str} {run_str}", "%Y%m%d %H") + timedelta(hours=hr_offset)
            try:
                local_tz = pytz.timezone(tz_name)
            except Exception:
                local_tz = UTC
            local_dt = utc_dt.replace(tzinfo=UTC).astimezone(local_tz)
            try:
                date_str_local = local_dt.strftime("%A, %B %-d, %Y")
            except Exception:
                date_str_local = local_dt.strftime("%A, %B %d, %Y").lstrip('0')
            time_str_local = local_dt.strftime("%I:%M:%S %p").lstrip('0')
            row = [date_str_local, time_str_local]
            # Extract 6 swell groups (Hs, Tp, Dir)
            idx_base = 6
            for _swell in range(6):
                # Extract up to the next 3 tokens, skipping any standalone '*' tokens.
                hs_val = tp_val = dir_val = None
                tokens_collected = 0
                # We'll accumulate numeric tokens until we have 3 or run out.
                while tokens_collected < 3 and idx_base < len(parts):
                    tok = parts[idx_base]
                    idx_base += 1
                    # Remove any '*' characters attached to numeric values.
                    tok_clean = tok.replace('*', '')
                    # Skip tokens that were just '*' (become empty after removal)
                    if tok_clean == '':
                        continue
                    if tokens_collected == 0:
                        try:
                            hs_val = float(tok_clean) * 3.28084  # convert to ft
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
                            dir_raw = int(float(tok_clean))
                            dir_val = (dir_raw + 180) % 360
                            tokens_collected += 1
                            continue
                        except ValueError:
                            continue
                # If fewer than 3 numeric values collected, set group as None
                if tokens_collected < 3:
                    row.extend([None, None, None])
                else:
                    row.extend([hs_val, tp_val, dir_val])
            # Combined height is the last numeric field in the line (might include '*' tokens)
            # We search from the end backwards to find the first numeric token, ignoring '*' markers.
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

    # Round numeric values: Hs and Combined to 2 decimals, Tp to 1 decimal, Direction to int
    for i, r in enumerate(rows):
        # Starting index after date and time
        idx_num = 2
        for g in range(6):
            # Hs
            if r[idx_num] is not None:
                r[idx_num] = round(r[idx_num], 2)
            idx_num += 1
            # Tp
            if r[idx_num] is not None:
                r[idx_num] = round(r[idx_num], 1)
            idx_num += 1
            # Direction (keep as int)
            if r[idx_num] is not None:
                try:
                    r[idx_num] = int(round(r[idx_num]))
                except Exception:
                    pass
            idx_num += 1
        # Combined Hs
        if r[-1] is not None:
            r[-1] = round(r[-1], 2)

    if not rows:
        return cycle_str, location_str, None, tz_name, "No data rows parsed from .bull file."
    return cycle_str, location_str, rows, tz_name, None

# -----------------------------------------------------------------------------
# Table Formatting and Export Helpers
# -----------------------------------------------------------------------------
def build_html_table(cycle_str: str, location_str: str, rows: list[list], tz_name: str):
    """
    Build an HTML table string to display the parsed data in a format similar to the
    provided Excel "Table View" sheet. This function applies background colors
    and simple styling for headers, subheaders, and data rows.

    Parameters:
        cycle_str (str): Text describing the model cycle (e.g., "Cycle    : 20250807 12 UTC").
        location_str (str): Text describing the station location (e.g., "Location : 51201 (21.67N 158.12W)").
        rows (list): A list where each element is a list representing a data row. Each
                     row entry must be in the order: [date_str, time_str, s1_hs, s1_tp, s1_dir, ..., s6_hs, s6_tp, s6_dir, combined_hs].

    Returns:
        str: A complete HTML table ready for embedding in the template.
    """
    # Define color palette for the six swell groups and the combined column. The palette uses
    # darker colors for headers, lighter shades for subheaders, and very light shades for data.
    group_colors = [
        {"header": "#C00000", "subheader": "#F8CBAD", "data": "#FDE9D9"},  # Swell 1
        {"header": "#E46C0A", "subheader": "#F9D5B5", "data": "#FCE9DB"},  # Swell 2
        {"header": "#FFC000", "subheader": "#FFF2CC", "data": "#FFF9E5"},  # Swell 3
        {"header": "#00B050", "subheader": "#D5E8D4", "data": "#EAF3E8"},  # Swell 4
        {"header": "#00B0F0", "subheader": "#D9EAF6", "data": "#ECF5FB"},  # Swell 5
        {"header": "#92D050", "subheader": "#E2F0D9", "data": "#F2F8EE"},  # Swell 6
    ]
    combined_colors = {"header": "#7030A0", "subheader": "#D9D2E9", "data": "#EDE9F4"}
    total_cols = 2 + len(group_colors) * 3 + 1  # 2 for date/time, 3 per swell, 1 for combined
    html = '<table class="table table-bordered table-sm">\n'
    # Cycle row
    html += f'<tr><td colspan="{total_cols}"><strong>{cycle_str}</strong></td></tr>\n'
    # Location row
    html += f'<tr><td colspan="{total_cols}"><strong>{location_str}</strong></td></tr>\n'
    # Time zone row
    html += f'<tr><td colspan="{total_cols}"><strong>{tz_name}</strong></td></tr>\n'
    # Header: group names
    html += '<tr>'
    html += '<th rowspan="2">Date</th>'
    html += '<th rowspan="2">Time</th>'
    for idx, col in enumerate(group_colors, start=1):
        html += f'<th colspan="3" style="background-color:{col["header"]}; color:white; text-align:center;">Swell {idx}</th>'
    # Combined column header (do not rowspan; the units row below will align under this column)
    html += f'<th style="background-color:{combined_colors["header"]}; color:white; text-align:center;">Combined</th>'
    html += '</tr>\n'
    # Subheader: units
    html += '<tr>'
    for col in group_colors:
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Hs<br>(ft)</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Tp<br>(s)</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Dir<br>(d)</th>'
    # Combined units
    html += f'<th style="background-color:{combined_colors["subheader"]}; text-align:center;">Hs<br>(ft)</th>'
    html += '</tr>\n'
    # Data rows
    for row in rows:
        html += '<tr>'
        # Date (bold)
        html += f'<td style="font-weight:bold;">{row[0]}</td>'
        # Time
        html += f'<td>{row[1]}</td>'
        idx = 2
        for col in group_colors:
            # Hs (ft)
            val = row[idx]
            hs_str = "" if val is None else f"{val:.2f}"
            html += f'<td style="background-color:{col["data"]}; text-align:right;">{hs_str}</td>'
            idx += 1
            # Tp (s)
            val = row[idx]
            tp_str = "" if val is None else f"{val:.1f}"
            html += f'<td style="background-color:{col["data"]}; text-align:right;">{tp_str}</td>'
            idx += 1
            # Direction (d)
            val = row[idx]
            dir_str = "" if val is None else f"{val}"
            html += f'<td style="background-color:{col["data"]}; text-align:right;">{dir_str}</td>'
            idx += 1
        # Combined Hs
        val = row[-1]
        comb_str = "" if val is None else f"{val:.2f}"
        html += f'<td style="background-color:{combined_colors["data"]}; text-align:right;">{comb_str}</td>'
        html += '</tr>\n'
    html += '</table>'
    return html

def build_excel_workbook(cycle_str: str, location_str: str, rows: list[list], tz_name: str):
    """
    Create an Excel workbook replicating the "Table View" formatting using openpyxl. Colors and
    layout are approximated from the provided template: group headers, unit rows, and colored
    columns for each swell and combined height. Date and time are separated into their own
    columns and formatted accordingly.

    Parameters:
        cycle_str (str): Cycle description.
        location_str (str): Location description.
        rows (list of lists): Parsed data rows as produced by parse_bull().

    Returns:
        BytesIO: An in-memory bytes buffer containing the Excel file.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Table View"
    # Define colors matching the HTML table
    group_colors = [
        {"header": "C00000", "subheader": "F8CBAD", "data": "FDE9D9"},
        {"header": "E46C0A", "subheader": "F9D5B5", "data": "FCE9DB"},
        {"header": "FFC000", "subheader": "FFF2CC", "data": "FFF9E5"},
        {"header": "00B050", "subheader": "D5E8D4", "data": "EAF3E8"},
        {"header": "00B0F0", "subheader": "D9EAF6", "data": "ECF5FB"},
        {"header": "92D050", "subheader": "E2F0D9", "data": "F2F8EE"},
    ]
    combined_colors = {"header": "7030A0", "subheader": "D9D2E9", "data": "EDE9F4"}
    total_cols = 2 + len(group_colors) * 3 + 1
    # Row counters
    row_idx = 1
    # Cycle row (merged)
    ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=total_cols)
    cell = ws.cell(row=row_idx, column=1, value=cycle_str)
    cell.font = Font(bold=True)
    row_idx += 1
    # Location row
    ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=total_cols)
    cell = ws.cell(row=row_idx, column=1, value=location_str)
    cell.font = Font(bold=True)
    row_idx += 1
    # Time zone row
    ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=total_cols)
    cell = ws.cell(row=row_idx, column=1, value=tz_name)
    cell.font = Font(bold=True)
    row_idx += 1
    # Header: group names
    ws.cell(row=row_idx, column=1, value="Date").font = Font(bold=True)
    ws.cell(row=row_idx, column=2, value="Time").font = Font(bold=True)
    col_idx = 3
    for idx, colors in enumerate(group_colors, start=1):
        ws.merge_cells(start_row=row_idx, start_column=col_idx, end_row=row_idx, end_column=col_idx + 2)
        hdr_cell = ws.cell(row=row_idx, column=col_idx, value=f"Swell {idx}")
        hdr_cell.fill = PatternFill(start_color=colors["header"], end_color=colors["header"], fill_type="solid")
        hdr_cell.font = Font(bold=True, color="FFFFFF")
        hdr_cell.alignment = Alignment(horizontal="center")
        col_idx += 3
    # Combined header
    ws.merge_cells(start_row=row_idx, start_column=col_idx, end_row=row_idx, end_column=col_idx)
    comb_cell = ws.cell(row=row_idx, column=col_idx, value="Combined")
    comb_cell.fill = PatternFill(start_color=combined_colors["header"], end_color=combined_colors["header"], fill_type="solid")
    comb_cell.font = Font(bold=True, color="FFFFFF")
    comb_cell.alignment = Alignment(horizontal="center")
    # Subheader row
    row_idx += 1
    col_idx = 3
    for colors in group_colors:
        # Hs
        cell = ws.cell(row=row_idx, column=col_idx, value="Hs (ft)")
        cell.fill = PatternFill(start_color=colors["subheader"], end_color=colors["subheader"], fill_type="solid")
        cell.alignment = Alignment(horizontal="center")
        cell.font = Font(bold=True)
        col_idx += 1
        # Tp
        cell = ws.cell(row=row_idx, column=col_idx, value="Tp (s)")
        cell.fill = PatternFill(start_color=colors["subheader"], end_color=colors["subheader"], fill_type="solid")
        cell.alignment = Alignment(horizontal="center")
        cell.font = Font(bold=True)
        col_idx += 1
        # Dir
        cell = ws.cell(row=row_idx, column=col_idx, value="Dir (d)")
        cell.fill = PatternFill(start_color=colors["subheader"], end_color=colors["subheader"], fill_type="solid")
        cell.alignment = Alignment(horizontal="center")
        cell.font = Font(bold=True)
        col_idx += 1
    # Combined subheader
    cell = ws.cell(row=row_idx, column=col_idx, value="Hs (ft)")
    cell.fill = PatternFill(start_color=combined_colors["subheader"], end_color=combined_colors["subheader"], fill_type="solid")
    cell.alignment = Alignment(horizontal="center")
    cell.font = Font(bold=True)
    # Data rows
    for data_row in rows:
        row_idx += 1
        # Date
        ws.cell(row=row_idx, column=1, value=data_row[0]).font = Font(bold=True)
        # Time
        ws.cell(row=row_idx, column=2, value=data_row[1])
        col_idx = 3
        data_iter = iter(data_row[2:])
        for colors in group_colors:
            # Hs
            hs_val = next(data_iter)
            cell = ws.cell(row=row_idx, column=col_idx, value=hs_val if hs_val is not None else "")
            cell.fill = PatternFill(start_color=colors["data"], end_color=colors["data"], fill_type="solid")
            cell.number_format = "0.00"
            col_idx += 1
            # Tp
            tp_val = next(data_iter)
            cell = ws.cell(row=row_idx, column=col_idx, value=tp_val if tp_val is not None else "")
            cell.fill = PatternFill(start_color=colors["data"], end_color=colors["data"], fill_type="solid")
            cell.number_format = "0.0"
            col_idx += 1
            # Dir
            dir_val = next(data_iter)
            cell = ws.cell(row=row_idx, column=col_idx, value=dir_val if dir_val is not None else "")
            cell.fill = PatternFill(start_color=colors["data"], end_color=colors["data"], fill_type="solid")
            col_idx += 1
        # Combined Hs
        combined_val = data_row[-1]
        cell = ws.cell(row=row_idx, column=col_idx, value=combined_val if combined_val is not None else "")
        cell.fill = PatternFill(start_color=combined_colors["data"], end_color=combined_colors["data"], fill_type="solid")
        cell.number_format = "0.00"
    # Adjust column widths for readability
    ws.column_dimensions['A'].width = 25
    ws.column_dimensions['B'].width = 12
    col = 3
    # Each numeric column narrower
    for _ in range(len(group_colors) * 3 + 1):
        ws.column_dimensions[chr(64 + col)].width = 8
        col += 1
    # Return workbook in-memory
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio


@app.route("/", methods=["GET", "POST"])
def index():
    """
    Home route for the application.

    Displays a form allowing the user to select a buoy station. Upon submission, the latest wave forecast
    is fetched and rendered as an HTML table that closely follows the formatting of the provided Excel
    "Table View" sheet. If any errors occur during retrieval or parsing, they are displayed to the user.
    """
    stations = get_station_list()
    # Build a list of station data (id, name, lat, lon) for the map.  We cross reference
    # the bullet station IDs with the station metadata to get coordinates.  If
    # metadata is missing for a station, it will be omitted from the map.
    stations_data = []
    try:
        bullet_ids = get_bullet_station_ids()
        meta = load_station_metadata()
        for sid in bullet_ids:
            info = meta.get(sid)
            if info and 'lat' in info and 'lon' in info:
                stations_data.append({'id': sid, 'name': info.get('name', sid), 'lat': info['lat'], 'lon': info['lon']})
    except Exception:
        stations_data = []
    selected_station = request.form.get("station") if request.method == "POST" else request.args.get('station', "")
    table_html = None
    error = None
    # If a station is selected (via GET or POST), parse its bulletin and build the table
    if selected_station:
        cycle_str, location_str, rows, tz_name, error = parse_bull(selected_station)
        if rows is not None:
            table_html = build_html_table(cycle_str, location_str, rows, tz_name)
    return render_template("index.html", stations=stations, stations_data=stations_data, selected_station=selected_station,
                           table_html=table_html, error=error)


@app.route("/download/<station_id>")
def download(station_id: str):
    """
    Download endpoint.

    Generates an Excel file that matches the styling of the "Table View" worksheet. The file includes the
    cycle information, location, header rows, units and colored columns for each swell and the combined
    height. If parsing fails, an error message is returned to the user.
    """
    cycle_str, location_str, rows, tz_name, error = parse_bull(station_id)
    if rows is None:
        return f"Error: {error}", 404
    bio = build_excel_workbook(cycle_str, location_str, rows, tz_name)
    filename = f"{station_id}_table_view.xlsx"
    return send_file(bio, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


if __name__ == "__main__":
    app.run(debug=True)
