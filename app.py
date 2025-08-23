from flask import Flask, render_template, request, send_file, jsonify
import pandas as pd  # still used for fallback Excel generation if needed
import requests
import json
import os
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

# -----------------------------------------------------------------------------
# Coordinate database
#
# To avoid making network requests for every buoy when rendering the map, the
# application can load precomputed latitude/longitude values for each buoy
# station from a static JSON file.  The file ``station_coords.json`` is
# generated offline (for example from the provided Excel spreadsheet) and
# placed alongside this module.  When present, it contains an object mapping
# station IDs to ``{"lat": float, "lon": float}``.  This allows the map
# to display all stations immediately without having to fetch .bull files.
STATION_COORDS = None  # Maps station_id -> { 'lat': float, 'lon': float }

def load_station_coords() -> dict:
    """
    Load the static station coordinates mapping from ``station_coords.json``.

    If the file exists in the same directory as this module, it is parsed
    once and cached.  The resulting dictionary maps each station ID to a
    dictionary with ``lat`` and ``lon`` keys.  If the file is missing or
    unreadable, an empty dictionary is returned.

    Returns:
        dict: A mapping of station ID strings to dictionaries with keys
        ``'lat'`` and ``'lon'``.
    """
    global STATION_COORDS
    if STATION_COORDS is not None:
        return STATION_COORDS
    base_dir = os.path.dirname(os.path.abspath(__file__))
    coord_path = os.path.join(base_dir, 'station_coords.json')
    coords = {}
    try:
        with open(coord_path, 'r') as f:
            data = json.load(f)
            # Ensure values are floats
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

# -----------------------------------------------------------------------------
# Default station metadata fallback
#
# When the application is unable to retrieve the station list or metadata from
# the NOAA servers (for example due to network restrictions), we fall back to
# a small curated set of buoy stations.  Each entry provides an approximate
# latitude/longitude and a human‑readable name.  This ensures that the map
# displays multiple markers and the dropdown contains more than a single
# placeholder buoy even when remote resources are unavailable.  You can add
# additional stations here as desired.  Latitude values are positive for
# northern hemisphere and negative for southern; longitude values are negative
# for western hemisphere and positive for eastern.
DEFAULT_STATIONS = {
    # Hawaiian region buoys
    "51201": {"name": "Buoy 51201", "lat": 21.67, "lon": -158.12},
    "51202": {"name": "Buoy 51202", "lat": 21.45, "lon": -157.90},
    "51203": {"name": "Buoy 51203", "lat": 21.55, "lon": -157.95},
    "51211": {"name": "Buoy 51211", "lat": 21.32, "lon": -157.53},
    "51212": {"name": "Buoy 51212", "lat": 21.27, "lon": -157.47},
    "51213": {"name": "Buoy 51213", "lat": 21.17, "lon": -157.17},
    # Other North Pacific buoys
    "51001": {"name": "Buoy 51001", "lat": 16.87, "lon": -156.47},
    "51002": {"name": "Buoy 51002", "lat": 12.38, "lon": -157.49},
    "51003": {"name": "Buoy 51003", "lat": 23.69, "lon": -162.25},
    "51004": {"name": "Buoy 51004", "lat": 25.84, "lon": -162.09},
}
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

# Cache for compiled station metadata used by the interactive map.  This list is
# computed once on demand and reused for subsequent requests.  Each element
# contains the station ID, name, latitude and longitude.
stations_data_cache = None


def get_station_list() -> list[tuple[str, str]]:
    """
    Build a list of available stations for the dropdown using the static
    ``station_list.json`` file.

    The application should show *only* those buoys for which a corresponding
    GFS wave bulletin exists.  To avoid depending on live network queries
    that may return inconsistent or outdated results, the list of valid
    station identifiers is stored in a JSON file (`station_list.json`) in
    the same directory as this module.  Each element of the JSON array
    contains a buoy station ID (as a string).  This file is generated
    offline from the user's provided Excel spreadsheet and represents
    the definitive set of stations that have .bull files.

    For each ID in the JSON list, we attempt to retrieve a human‑readable
    name from the station metadata loaded by ``load_station_metadata()``.
    If metadata is unavailable or the station isn't present, the ID itself
    is used as the name.  Should the JSON file be missing or unreadable,
    the function falls back to the curated ``DEFAULT_STATIONS`` list.  No
    network lookups to NOAA are performed here so the dropdown reflects
    exactly the station list from the Excel spreadsheet.

    Returns:
        list[tuple[str, str]]: A list of (station_id, station_name) tuples.
    """
    stations: list[tuple[str, str]] = []
    try:
        # Determine the path to station_list.json relative to this file
        base_dir = os.path.dirname(os.path.abspath(__file__))
        json_path = os.path.join(base_dir, 'station_list.json')
        with open(json_path, 'r') as f:
            station_ids = json.load(f)
        # Load station metadata for names, if available
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
        # Ignore any exceptions and fall through to default stations
        pass
    # If the JSON file cannot be read or yielded no stations, fall back to the
    # curated defaults.  We do not return metadata or bullet station lists to
    # avoid accidentally including buoys without .bull files.
    return [(sid, info.get('name', sid)) for sid, info in DEFAULT_STATIONS.items()]

def get_stations_data():
    """
    Build and return a list of station metadata dictionaries: {id, name, lat, lon}.
    This function caches its results so that the expensive operations to
    retrieve the bullet station list and station metadata file are only
    performed once.  The returned list is used by the /stations.json
    endpoint to provide station data to the front‑end.  Stations with
    missing or invalid latitude/longitude are excluded from the result.

    Returns:
        list[dict]: A list of station dictionaries with keys 'id', 'name', 'lat', 'lon'.
    """
    global stations_data_cache
    if stations_data_cache is not None:
        return stations_data_cache
    # Build station data for all stations that appear in the UI dropdown.  We first try
    # to load the list of station IDs via get_station_list().  If this fails, we fall back
    # to the set of bullet stations.  For each station ID, we attempt to look up its
    # latitude/longitude in the station metadata; if not found, we make a best-effort attempt
    # to parse the location from the station's most recent .bull file.  Stations without
    # coordinates are omitted.
    # Build the list of station IDs from the static station list.  We do not
    # fall back to the live bulletin station list here because the user
    # explicitly provided a complete list of stations with available
    # bulletins via ``station_list.json``.  If reading this list fails,
    # get_station_list() will return the curated DEFAULT_STATIONS; in that
    # case the map and dropdown will reflect only those entries.
    try:
        ids_with_names = get_station_list()
        id_list = [sid for sid, _ in ids_with_names]
    except Exception:
        id_list = []
    try:
        meta = load_station_metadata()
    except Exception:
        meta = {}
    # Load static coordinate mapping if available.  This avoids expensive
    # network calls to parse each station's .bull file for its location.
    coords_map = load_station_coords()
    data_list = []
    for sid in id_list:
        # Default name is the station ID itself
        name = sid
        # Prefer metadata name if available
        info = meta.get(sid)
        if info and 'name' in info:
            name = info['name']
        # Retrieve latitude and longitude from the precomputed coordinates
        lat = lon = None
        if sid in coords_map:
            lat = coords_map[sid]['lat']
            lon = coords_map[sid]['lon']
        # Fall back to curated defaults if coordinates missing
        if (lat is None or lon is None) and sid in DEFAULT_STATIONS:
            fallback_info = DEFAULT_STATIONS[sid]
            lat = fallback_info.get('lat')
            lon = fallback_info.get('lon')
        # Only include stations with valid coordinates
        if lat is not None and lon is not None:
            data_list.append({
                'id': sid,
                'name': name,
                'lat': lat,
                'lon': lon,
            })
    stations_data_cache = data_list
    return stations_data_cache

@app.route('/stations.json')
def stations_json():
    """
    JSON endpoint returning the list of station metadata dictionaries used by
    the front‑end map.  This decouples the potentially very large list of
    stations from the HTML template, preventing the page from exceeding
    browser size limits and allowing the client to fetch the data
    asynchronously.
    """
    return jsonify(get_stations_data())


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
            # Skip empty station ids
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
        # On success, cache and return the metadata dictionary
        STATION_META = meta
        return STATION_META
    except Exception:
        # If we encounter any exception while downloading or parsing
        # the station table (for example due to network restrictions),
        # fall back to the predefined DEFAULT_STATIONS dictionary.  This
        # ensures that we still have a useful set of buoys to display
        # even when external resources are unavailable.
        STATION_META = DEFAULT_STATIONS.copy()
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


def parse_bull(station_id: str, target_tz_name: str | None = None):
    """
    Fetch and parse the .bull file for a given station.

    The NOAA .bull files contain marine forecast data for buoy stations.  This parser supports both the
    traditional ``Hr`` style bulletins as well as the newer ``day & hour`` style bulletins.  The
    returned structure focuses on data relevant to the "Table View" worksheet: local date and time,
    six swell groups (each with height, period and direction) and the combined sea height.  In
    addition to the cycle and location strings, this function returns a formatted description of
    the model run time in the selected timezone.  You may specify a target timezone using the
    ``target_tz_name`` parameter; if omitted, the buoy's local timezone (determined from its
    latitude/longitude) will be used.

    Parameters:
        station_id (str): The buoy station identifier (e.g., ``'51201'``).
        target_tz_name (str, optional): An IANA timezone name indicating which timezone to use
            when formatting all date/time values.  If ``None`` or invalid, the buoy's local
            timezone will be used.  Examples include ``'Pacific/Honolulu'`` or ``'UTC'``.

    Returns:
        tuple: ``(cycle_str, location_str, model_run_str, rows, tz_name, error)``.  If
        parsing succeeds, ``error`` is ``None`` and ``rows`` is a list where each element
        corresponds to a forecast row with the following order: ``[date_str, time_str, s1_hs,
        s1_tp, s1_dir, ..., s6_hs, s6_tp, s6_dir, combined_hs]``.  ``cycle_str`` and
        ``location_str`` are the raw header strings extracted from the bulletin.  ``model_run_str``
        is a human‑readable string describing when the model was run (e.g., ``"Model Run:
        Thursday, August 7, 2025 12:00 PM"``) in the selected timezone.  ``tz_name`` is the
        timezone actually used for date/time conversions (either the supplied ``target_tz_name``
        or the buoy's local timezone).  If an error occurs, ``rows`` will be ``None`` and
        ``error`` will contain a descriptive message.  ``model_run_str`` may be ``None`` when
        parsing fails.
    """
    # Determine the latest available run (date and hour)
    date_str, run_str = get_latest_run()
    if not date_str:
        # No recent run available; return with UTC as the default timezone
        # Cycle, location, model_run_str are unknown in this case.
        return None, None, None, None, 'UTC', "No recent run found."

    # Download the .bull file for this station and run
    bull_url = f"{NOAA_BASE}/gfs.{date_str}/{run_str}/wave/station/bulls.t{run_str}z/gfswave.{station_id}.bull"
    resp = requests.get(bull_url, timeout=10)
    if resp.status_code != 200:
        # Could not download the .bull file for this station/run.  Return default values.
        return None, None, None, None, 'UTC', f"No .bull file found for {station_id}"

    lines = resp.text.splitlines()
    if not lines:
        # Downloaded .bull file is empty; nothing to parse.
        return None, None, None, None, 'UTC', "Downloaded .bull file is empty."

    # Attempt to extract the cycle and location lines from the header. The .bull header typically contains
    # lines like "Cycle    : 20250807 12 UTC" and "Location : 51201 (21.67N 158.12W)". We look for
    # these prefixes case-insensitively and fall back to the first lines if not found.
    # Normalize whitespace before matching so that leading spaces do not
    # prevent detection of the cycle and location headers.  Some bulletins
    # prefix these fields with varying amounts of whitespace.
    cycle_line = next((l for l in lines if l.lower().strip().startswith("cycle")), None)
    location_line = next((l for l in lines if l.lower().strip().startswith("location")), None)
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

    # Determine the effective timezone.  Use the target_tz_name if provided and valid,
    # otherwise fall back to the buoy's local timezone determined above.  If both are
    # unavailable, default to UTC.
    effective_tz_name = tz_name
    if target_tz_name:
        try:
            # Validate the provided timezone name
            _ = pytz.timezone(target_tz_name)
            effective_tz_name = target_tz_name
        except Exception:
            # Ignore invalid timezone names and keep the buoy's local timezone
            pass

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

        # Compute the model run time in the effective timezone.  We compute this once
        # outside the row loop as it depends only on the cycle date/time.  The formatted
        # string is returned to the caller so it can be displayed in the table header.
        try:
            model_run_local = cycle_dt_utc.replace(tzinfo=UTC).astimezone(pytz.timezone(effective_tz_name))
        except Exception:
            model_run_local = cycle_dt_utc
        try:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %-d, %Y %I:%M %p")
        except Exception:
            model_run_str = "Model Run: " + model_run_local.strftime("%A, %B %d, %Y %I:%M %p").lstrip('0')

        # Conversion factor (meters to feet)
        M_TO_FT = 3.28084
            # Track the last forecast datetime to enforce 1-hour increments
        last_forecast_dt_utc = None
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
            
          # Derive forecast UTC datetime.
    # Use the first parsed timestamp as an anchor and then add
    # exactly 1 hour for each subsequent row. This removes the
    # end‑of‑month “jumping” seen when simply replacing day/hour
    # fields on the cycle date.
    if last_forecast_dt_utc is None:
        # On the first row, build the anchor timestamp by replacing
        # the day and hour on the cycle date.
        try:
            forecast_dt_utc = cycle_dt_utc.replace(day=day_val, hour=hour_val)
        except ValueError:
            # Invalid day (e.g., February 30) – skip row.
            continue
        # If the anchor is earlier than the cycle time, roll forward
        # one or more days until it is strictly >= the cycle time.
        while forecast_dt_utc < cycle_dt_utc:
            forecast_dt_utc += timedelta(days=1)
    else:
        # For subsequent rows, increment the previous timestamp by
        # exactly one hour.
        forecast_dt_utc = last_forecast_dt_utc + timedelta(hours=1)mezone (either the selected timezone or the buoy's local timezone)
            # Use effective_tz_name instead of tz_name so that the user-selected timezone is respected.
            try:
                local_tz = pytz.timezone(effective_tz_name)
            except Exception:
                # Fallback to UTC if the effective timezone cannot be resolved
                local_tz = UTC
            # Convert the forecast UTC time into the effective local timezone
            local_dt = forecast_dt_utc.replace(tzinfo=UTC).astimezone(local_tz)
            # Format date and time strings in the desired presentation
            try:
                date_str_local = local_dt.strftime("%A, %B %-d, %Y")
            except Exception:
                # Fallback for platforms without %-d (e.g., Windows)
                date_str_local = local_dt.strftime("%A, %B %d, %Y").lstrip('0')
            # Format the local time without seconds.  Use hours and minutes only,
            # trimming any leading zero to match the desired display (e.g., "8:00 AM").
            time_str_local = local_dt.strftime("%I:%M %p").lstrip('0')
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
                last_forecast_dt_utc = forecast_dt_utc
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
            # Return an error if we cannot locate the data section in the old format.  The
            # model run string is not available in this branch, so set it to None.
            return cycle_str, location_str, None, None, effective_tz_name, "Data section not found in .bull file."

        # Before parsing the data rows, compute the model run time using the cycle date/time.
        # We attempt to extract the cycle date and hour from the header; if not available we
        # fall back to the latest run information (date_str/run_str).
        m_old = re.search(r"(\d{8})\s*(\d{2})", cycle_str)
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

        # For each subsequent line, parse the forecast hour and swell data
        for line in lines[start_idx:]:
            parts = line.split()
            if len(parts) < 20:
                continue
            try:
                hr_offset = float(parts[0])
            except ValueError:
                continue
            # Anchor the forecast time to the cycle date/time and add the hour offset
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
            # Format local time without seconds (hours and minutes only) for older format rows
            time_str_local = local_dt.strftime("%I:%M %p").lstrip('0')
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
        # No valid data rows were parsed.  Return model_run_str as None and propagate the timezone.
        return cycle_str, location_str, None, None, effective_tz_name, "No data rows parsed from .bull file."
    # Successful parse: return the header strings, the computed model run string, the rows,
    # the effective timezone name, and no error.
    return cycle_str, location_str, model_run_str if 'model_run_str' in locals() else None, rows, effective_tz_name, None

# -----------------------------------------------------------------------------
# Table Formatting and Export Helpers
# -----------------------------------------------------------------------------
def build_html_table(cycle_str: str, location_str: str, model_run_str: str | None, rows: list[list], tz_label: str, unit: str) -> str:
    """
    Build an HTML table string that closely follows the appearance of the provided
    Excel "Table View" worksheet.  The table begins with several metadata
    rows, including the cycle description, the station location, the model
    run time (if available) and the currently selected time zone.  The
    subsequent header and unit rows are colour coded per swell group, and
    numeric values are right-aligned.

    Parameters:
        cycle_str: Raw cycle description extracted from the bulletin (e.g.,
            ``"Cycle    : 20250807 12 UTC"``).
        location_str: Raw location description (e.g., ``"Location : 51201 (21.67N 158.12W)"``).
        model_run_str: Human‑readable description of the model run time, or
            ``None`` if unavailable (e.g., ``"Model Run: Thursday, August 7, 2025 12:00 PM"``).
        rows: Parsed forecast rows.  Each row must be a list with elements
            ``[date_str, time_str, s1_hs, s1_tp, s1_dir, ..., s6_hs, s6_tp, s6_dir, combined_hs]``.
        tz_label: The name of the timezone being used for date/time formatting.
        unit: Unit system for height values.  ``'US'`` displays heights in feet
            (Hs values remain unchanged) and labels the header ``(ft)``; ``'Metric'``
            converts heights to metres and labels the header ``(m)``.

    Returns:
        A string containing the HTML markup for the complete table.
    """
    # Define color palette for the six swell groups and the combined column. The palette uses
    # darker colors for headers, lighter shades for subheaders, and very light shades for data.
    # Define color palette for the six swell groups and the combined column. To improve
    # contrast between the first two groups, the palette uses a darker red for Swell 1
    # and a richer orange for Swell 2.  Subsequent groups retain their previous
    # colours.  Each group specifies colours for the header, subheader and data
    # rows.  Combined column colours are defined separately below.
    group_colors = [
        {
            "header": "#C00000",  # dark red for Swell 1
            # Use more pronounced red shades for subheader and data to enhance contrast.  The previous
            # colours (#F4CCCC and #FCE5E5) were very pale and not clearly distinguishable from
            # Swell 2’s orange.  Switch to deeper shades of red for both the subheader and data
            # cells so that the entire Swell 1 column reads as red.  These values have been
            # chosen empirically to provide strong contrast while remaining light enough for
            # readability.
            "subheader": "#F8B4B4",
            "data": "#F9DCDC",
        },
        {
            "header": "#ED7D31",  # stronger orange for Swell 2 to contrast with Swell 1
            "subheader": "#FBE5D6",
            "data": "#FDE7D4",
        },
        {
            "header": "#FFC000",
            "subheader": "#FFF2CC",
            "data": "#FFF9E5",
        },
        {
            "header": "#00B050",
            "subheader": "#D5E8D4",
            "data": "#EAF3E8",
        },
        {
            "header": "#00B0F0",
            "subheader": "#D9EAF6",
            "data": "#ECF5FB",
        },
        {
            "header": "#92D050",
            "subheader": "#E2F0D9",
            "data": "#F2F8EE",
        },
    ]
    combined_colors = {"header": "#7030A0", "subheader": "#D9D2E9", "data": "#EDE9F4"}
    total_cols = 2 + len(group_colors) * 3 + 1  # 2 for date/time, 3 per swell, 1 for combined
    html = '<table class="table table-bordered table-sm">\n'
    # Cycle row
    html += f'<tr><td colspan="{total_cols}"><strong>{cycle_str}</strong></td></tr>\n'
    # Location row
    html += f'<tr><td colspan="{total_cols}"><strong>{location_str}</strong></td></tr>\n'
    # Model run row is intentionally omitted.  In the original Excel template, this
    # row shows the model run time, but the current requirements specify that it
    # should not be displayed in the table view.  Therefore, we do not insert
    # the model_run_str into the table at all.
    # Time zone row
    html += f'<tr><td colspan="{total_cols}"><strong>Time Zone: {tz_label}</strong></td></tr>\n'
    # Header: group names
    html += '<tr>'
    html += '<th rowspan="2">Date</th>'
    html += '<th rowspan="2">Time</th>'
    for idx, col in enumerate(group_colors, start=1):
        html += f'<th colspan="3" style="background-color:{col["header"]}; color:white; text-align:center;">Swell {idx}</th>'
    # Combined column header (do not rowspan; the units row below will align under this column)
    html += f'<th style="background-color:{combined_colors["header"]}; color:white; text-align:center;">Combined</th>'
    html += '</tr>\n'
    # Subheader: units.  Adjust the Hs unit label based on the selected unit system.
    html += '<tr>'
    hs_unit_label = '(ft)' if unit == 'US' else '(m)'
    for col in group_colors:
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Hs<br>{hs_unit_label}</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Tp<br>(s)</th>'
        html += f'<th style="background-color:{col["subheader"]}; text-align:center;">Dir<br>(d)</th>'
    # Combined units
    html += f'<th style="background-color:{combined_colors["subheader"]}; text-align:center;">Hs<br>{hs_unit_label}</th>'
    html += '</tr>\n'
    # Data rows
    for row in rows:
        # Determine styling based on the time of day.  Rows with times between 6:00 AM
        # and 7:00 PM (inclusive) are bold with a solid border.  Rows between
        # 8:00 PM and 5:00 AM (inclusive) receive a dashed border and normal font
        # weight.  All other rows retain the default styling.
        try:
            # Parse the time string which no longer includes seconds (e.g., "6:00 AM").
            # Attempt to parse using the hours and minutes format.  If that fails,
            # fall back to parsing a seconds‑inclusive format.  Should both
            # attempts fail, we leave parsed_time as None and apply no special styling.
            parsed_time = datetime.strptime(row[1], "%I:%M %p").time()
        except Exception:
            try:
                parsed_time = datetime.strptime(row[1], "%I:%M:%S %p").time()
            except Exception:
                parsed_time = None
        # Define time ranges
        bold_start = datetime.strptime("6:00:00 AM", "%I:%M:%S %p").time()
        bold_end = datetime.strptime("7:00:00 PM", "%I:%M:%S %p").time()
        dashed_start_evening = datetime.strptime("8:00:00 PM", "%I:%M:%S %p").time()
        dashed_end_morning = datetime.strptime("5:00:00 AM", "%I:%M:%S %p").time()
        border_style = ""
        font_weight_row = "normal"
        if parsed_time is not None:
            # Bold rows between 6 AM and 7 PM inclusive
            if bold_start <= parsed_time <= bold_end:
                border_style = "border:1px solid #000;"
                font_weight_row = "bold"
            # Dashed rows if time is >= 8 PM or <= 5 AM (cross‑midnight range)
            elif parsed_time >= dashed_start_evening or parsed_time <= dashed_end_morning:
                border_style = "border:1px dashed #999;"
                font_weight_row = "normal"
        # Generate the table row.  We add padding to each cell to widen
        # the columns for readability.  The date cell is always bold,
        # regardless of the row style.
        html += '<tr>'
        # Date cell (always bold).  Increase horizontal and vertical padding to
        # make the columns wider and improve readability.
        date_style = f'font-weight:bold; {border_style} padding:4px 8px;'
        html += f'<td style="{date_style}">{row[0]}</td>'
        # Time cell (row-level font weight) with increased padding
        time_style = f'font-weight:{font_weight_row}; {border_style} padding:4px 8px;'
        html += f'<td style="{time_style}">{row[1]}</td>'
        idx = 2
        for col in group_colors:
            # Hs (ft)
            val = row[idx]
            # Convert height to metres if metric is selected
            if val is None:
                hs_str = ""
            else:
                display_val = val if unit == 'US' else (val / 3.28084)
                hs_str = f"{display_val:.2f}"
            cell_style = f'background-color:{col["data"]}; text-align:right; font-weight:{font_weight_row}; {border_style} padding:4px 8px;'
            html += f'<td style="{cell_style}">{hs_str}</td>'
            idx += 1
            # Tp (s)
            val = row[idx]
            tp_str = "" if val is None else f"{val:.1f}"
            cell_style = f'background-color:{col["data"]}; text-align:right; font-weight:{font_weight_row}; {border_style} padding:4px 8px;'
            html += f'<td style="{cell_style}">{tp_str}</td>'
            idx += 1
            # Direction (d)
            val = row[idx]
            dir_str = "" if val is None else f"{val}"
            cell_style = f'background-color:{col["data"]}; text-align:right; font-weight:{font_weight_row}; {border_style} padding:4px 8px;'
            html += f'<td style="{cell_style}">{dir_str}</td>'
            idx += 1
        # Combined Hs
        val = row[-1]
        if val is None:
            comb_str = ""
        else:
            display_comb = val if unit == 'US' else (val / 3.28084)
            comb_str = f"{display_comb:.2f}"
        cell_style = f'background-color:{combined_colors["data"]}; text-align:right; font-weight:{font_weight_row}; {border_style} padding:4px 8px;'
        html += f'<td style="{cell_style}">{comb_str}</td>'
        html += '</tr>\n'
    html += '</table>'
    return html
app.py

def build_excel_workbook(cycle_str: str, location_str: str, model_run_str: str | None, rows: list[list], tz_label: str, unit: str) -> BytesIO:
    """
    Create an Excel workbook that mirrors the formatting of the Excel "Table View"
    worksheet.  This version includes additional rows for the model run time
    (if available) and the selected time zone.  Colours and layout are
    approximated from the original template: each swell group has a distinct
    header colour, subheader colour and data fill, and the combined column
    uses its own colours.  Date and time occupy the first two columns.


    Parameters:
        cycle_str: The model cycle description string.
        location_str: The station location description string.
        model_run_str: A descriptive string indicating when the model was run,
            or ``None`` if unavailable.
        rows: Parsed forecast rows as returned by ``parse_bull()``.
        tz_label: The name of the time zone used for date/time conversions.
        unit: Unit system for height values.  ``'US'`` leaves heights in feet
            and labels the units row ``(ft)``; ``'Metric'`` converts heights to
            metres and labels the units row ``(m)``.

    Returns:
        A ``BytesIO`` object containing the generated Excel file.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "Table View"
    # Define colors matching the HTML table
    # Colour palette for the six swell groups.  The second group uses a richer
    # orange to better contrast with the first group.  Each entry defines
    # colours for header, subheader and data rows, without the leading '#'
    # because openpyxl expects hex strings without prefix.
    group_colors = [
        {
            "header": "C00000",
            # Use deeper red shades for Swell 1 subheader and data.  The previous colours were
            # too pale and blended with the Swell 2 orange.  These new values are more
            # saturated to ensure the Swell 1 columns are clearly identifiable as red.
            "subheader": "F8B4B4",
            "data": "F9DCDC",
        },
        {"header": "ED7D31", "subheader": "FBE5D6", "data": "FDE7D4"},
        {"header": "FFC000", "subheader": "FFF2CC", "data": "FFF9E5"},
        {"header": "00B050", "subheader": "D5E8D4", "data": "EAF3E8"},
        {"header": "00B0F0", "subheader": "D9EAF6", "data": "ECF5FB"},
        {"header": "92D050", "subheader": "E2F0D9", "data": "F2F8EE"},
    ]
    combined_colors = {"header": "7030A0", "subheader": "D9D2E9", "data": "EDE9F4"}
    total_cols = 2 + len(group_colors) * 3 + 1
    # Row counters
    row_idx = 1
    # Cycle row
    ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=total_cols)
    cell = ws.cell(row=row_idx, column=1, value=cycle_str)
    cell.font = Font(bold=True)
    row_idx += 1
    # Location row
    ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=total_cols)
    cell = ws.cell(row=row_idx, column=1, value=location_str)
    cell.font = Font(bold=True)
    row_idx += 1
    # Omit the model run row entirely.  Although the Excel template originally included
    # a row displaying the model run time, the current requirements specify that this
    # information should not appear in the table.  Therefore, we do not insert the
    # model_run_str into the worksheet and leave this row unused.
    # Time zone row
    ws.merge_cells(start_row=row_idx, start_column=1, end_row=row_idx, end_column=total_cols)
    cell = ws.cell(row=row_idx, column=1, value=f"Time Zone: {tz_label}")
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
    # Determine units label for Hs depending on the selected system
    hs_unit_label = "(ft)" if unit == 'US' else "(m)"
    for colors in group_colors:
        # Hs
        cell = ws.cell(row=row_idx, column=col_idx, value=f"Hs {hs_unit_label}")
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
    cell = ws.cell(row=row_idx, column=col_idx, value=f"Hs {hs_unit_label}")
    cell.fill = PatternFill(start_color=combined_colors["subheader"], end_color=combined_colors["subheader"], fill_type="solid")
    cell.alignment = Alignment(horizontal="center")
    cell.font = Font(bold=True)
    # Data rows
    for data_row in rows:
        row_idx += 1
        # Determine styling based on the time string in the row.  Rows with
        # times between 06:00 AM and 07:00 PM (inclusive) should appear
        # bold with a solid border.  Rows within the broader range of
        # 05:00 AM through 08:00 PM inclusive but outside the primary
        # range should use a dashed border and normal font weight.  All
        # remaining rows retain default styling (no additional border and
        # normal weight).  We parse the time string to a datetime.time
        # object for comparison.  If parsing fails, the row is treated
        # as default.
        parsed_time = None
        try:
            parsed_time = datetime.strptime(data_row[1], "%I:%M:%S %p").time()
        except Exception:
            parsed_time = None
        bold_start = datetime.strptime("6:00:00 AM", "%I:%M:%S %p").time()
        bold_end = datetime.strptime("7:00:00 PM", "%I:%M:%S %p").time()
        dashed_start_evening = datetime.strptime("8:00:00 PM", "%I:%M:%S %p").time()
        dashed_end_morning = datetime.strptime("5:00:00 AM", "%I:%M:%S %p").time()
        is_bold_row = False
        border_style_name = None
        if parsed_time is not None:
            if bold_start <= parsed_time <= bold_end:
                is_bold_row = True
                border_style_name = "thin"
            elif parsed_time >= dashed_start_evening or parsed_time <= dashed_end_morning:
                is_bold_row = False
                border_style_name = "dashed"
        # Define a border object if a style is specified.  Use black for the
        # border colour.  Otherwise, set border to None.
        if border_style_name:
            border_obj = Border(
                left=Side(style=border_style_name, color="000000"),
                right=Side(style=border_style_name, color="000000"),
                top=Side(style=border_style_name, color="000000"),
                bottom=Side(style=border_style_name, color="000000"),
            )
        else:
            border_obj = None
        # Date cell: always bold, but border depends on row classification
        date_cell = ws.cell(row=row_idx, column=1, value=data_row[0])
        date_cell.font = Font(bold=True)
        if border_obj:
            date_cell.border = border_obj
        # Time cell: bold if in bold range, normal otherwise
        time_cell = ws.cell(row=row_idx, column=2, value=data_row[1])
        time_cell.font = Font(bold=is_bold_row)
        if border_obj:
            time_cell.border = border_obj
        # Process swell groups and combined values
        col_idx = 3
        data_iter = iter(data_row[2:])
        for colors in group_colors:
            # Hs
            hs_val = next(data_iter)
            # Convert Hs values to metres if metric units are requested
            display_hs = hs_val
            if hs_val is not None and unit != 'US':
                display_hs = hs_val / 3.28084
            cell = ws.cell(row=row_idx, column=col_idx, value=display_hs if display_hs is not None else "")
            cell.fill = PatternFill(start_color=colors["data"], end_color=colors["data"], fill_type="solid")
            cell.number_format = "0.00"
            cell.font = Font(bold=is_bold_row)
            if border_obj:
                cell.border = border_obj
            col_idx += 1
            # Tp
            tp_val = next(data_iter)
            cell = ws.cell(row=row_idx, column=col_idx, value=tp_val if tp_val is not None else "")
            cell.fill = PatternFill(start_color=colors["data"], end_color=colors["data"], fill_type="solid")
            cell.number_format = "0.0"
            cell.font = Font(bold=is_bold_row)
            if border_obj:
                cell.border = border_obj
            col_idx += 1
            # Dir
            dir_val = next(data_iter)
            cell = ws.cell(row=row_idx, column=col_idx, value=dir_val if dir_val is not None else "")
            cell.fill = PatternFill(start_color=colors["data"], end_color=colors["data"], fill_type="solid")
            cell.font = Font(bold=is_bold_row)
            if border_obj:
                cell.border = border_obj
            col_idx += 1
        # Combined Hs
        combined_val = data_row[-1]
        display_comb = combined_val
        if combined_val is not None and unit != 'US':
            display_comb = combined_val / 3.28084
        cell = ws.cell(row=row_idx, column=col_idx, value=display_comb if display_comb is not None else "")
        cell.fill = PatternFill(start_color=combined_colors["data"], end_color=combined_colors["data"], fill_type="solid")
        cell.number_format = "0.00"
        cell.font = Font(bold=is_bold_row)
        if border_obj:
            cell.border = border_obj
    # Adjust column widths for readability.  Increase the widths slightly to
    # ensure that all data is visible and the table is easier to read.
    ws.column_dimensions['A'].width = 30  # Date column
    ws.column_dimensions['B'].width = 15  # Time column
    col = 3
    # Widen numeric columns
    for _ in range(len(group_colors) * 3 + 1):
        # Convert numeric index to column letter.  The ASCII offset 64
        # corresponds to 'A' for 1, so 64 + col yields the column letter.
        letter = chr(64 + col)
        ws.column_dimensions[letter].width = 10
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

    The home page presents an interactive map for station selection and a
    fallback form containing a station dropdown and a timezone dropdown.  When
    the user selects a station (via form submission or clicking a map
    marker), the latest wave forecast is retrieved, parsed and displayed in
    a table that mirrors the provided Excel "Table View" worksheet.  Users
    can select a time zone from the dropdown; if none is selected, the
    buoy's local time zone is used.  Any parsing or retrieval errors are
    surfaced to the user.
    """
    # Build list of available stations for the dropdown
    stations = get_station_list()
    # Build list of common time zones for the timezone dropdown.  Using
    # pytz.common_timezones yields a manageable set of well‑known zones.
    timezones = sorted(pytz.common_timezones)
    # Unit options for height values.  The default is US units (feet).
    unit_options = ["US", "Metric"]

    # Determine which station and timezone have been selected.  The station
    # can be supplied via a POST form field or a query parameter.  The
    # timezone can similarly be supplied via "tz" in either POST or GET.
    selected_station = ""
    selected_tz = ""
    selected_unit = "US"
    if request.method == "POST":
        selected_station = request.form.get("station") or ""
        selected_tz = request.form.get("tz") or ""
        selected_unit = request.form.get("unit") or "US"
    else:
        selected_station = request.args.get("station", "")
        selected_tz = request.args.get("tz", "")
        selected_unit = request.args.get("unit", "US") or "US"
    # Set defaults when not provided: use Pacific/Honolulu for timezone and buoy 51201 for station
           if not selected_station:
            selected_station = "51201"
            selected_tz ="""Pacific/Honolulu"
    if not selected_station:
        selected_station = "51201"
            # Determine timezone based on station coordinates, overriding selected_tz if possible
          try:
        coords_map = load_station_coords()
        sid_str = str(selected_station).strip() if selected_station else None
        if sid_str and sid_str in coords_map:
            lat = coords_map[sid_str]['lat']
            lon = coords_map[sid_str]['lon']
                                                            tz_guess = tz_finder.timezone_at(lat=lat, lng=lon)
            if tz_guess:
                selected_tz = tz_guess
    except Exception:
        pass


    table_html = None
    error = None
    tz_label = ""
    # Latitude and longitude of the currently selected station.  These
    # values are passed to the template so that the map can display a
    # marker only for the selected buoy.  They default to ``None`` until
    # determined below.
    selected_lat: float | None = None
    selected_lon: float | None = None
    # If a station is selected, fetch and parse the bulletin using the
    # selected timezone (if any).  The parse_bull() function returns
    # cycle/location strings, a model run string, the data rows, the
    # effective timezone name actually used and any error message.
    if selected_station:
        cycle_str, location_str, model_run_str, rows, effective_tz_name, parse_error = parse_bull(selected_station, selected_tz or None)
        error = parse_error
        if rows is not None:
            # Use the effective timezone name (returned by parse_bull) as the label
        # Sort rows chronologically to ensure month wrap after August 31 continues into September
            try:
                rows = sorted(rows, key=lambda r: datetime.strptime(f"{r[0]} {r[1]}", '%A, %B %d, %Y %I:%M %p'))
            except Exception:
                pass
Sort rows chronologically
            tz_label = effective_tz_name
            table_html = build_html_table(cycle_str, location_str, model_run_str, rows, tz_label, selected_unit)
           


            # Retrieve latitude and longitude from the precomputed station coordinates if available.
            coords_map = load_station_coords()
            sid_str = str(selected_station).strip()
            if sid_str in coords_map:
                selected_lat = coords_map[sid_str]['lat']
                selected_lon = coords_map[sid_str]['lon']
            else:
                # Fallback: attempt to parse coordinates from the location string.  This
                # remains as a backup in case the static coordinates are missing or
                # the buoy ID is not found in the precomputed mapping.  The pattern
                # searches for numbers and N/E/S/W indicators inside parentheses,
                # e.g. "Location : 51201 (21.67N 158.12W)".  Optional whitespace
                # before the numbers is allowed to support single‑digit latitudes.
                if location_str:
                    import re
                    m = re.search(r"\(\s*([-+]?\d+(?:\.\d+)?)\s*([NS])\s+([-+]?\d+(?:\.\d+)?)\s*([EW])\)", location_str)
                    if m:
                        try:
                            lat_val = float(m.group(1))
                            lat_dir = m.group(2).upper()
                            lon_val = float(m.group(3))
                            lon_dir = m.group(4).upper()
                            selected_lat = lat_val if lat_dir == 'N' else -lat_val
                            selected_lon = lon_val if lon_dir == 'E' else -lon_val
                        except Exception:
                            selected_lat = None
                            selected_lon = None
    # Render template with all context variables.  Also pass the timezones
    # list and the currently selected timezone for the dropdown.
    return render_template(
        "index.html",
        stations=stations,
        selected_station=selected_station,
        timezones=timezones,
        selected_tz=selected_tz or tz_label,
        units=unit_options,
        selected_unit=selected_unit,
        table_html=table_html,
        error=error,
        selected_lat=selected_lat,
        selected_lon=selected_lon,
    )


@app.route("/download/<station_id>")
def download(station_id: str):
    """
    Download endpoint.

    Generates an Excel file formatted like the "Table View" worksheet.  The
    timezone for date/time formatting can be supplied via a query parameter
    ``tz``.  The Excel workbook includes the cycle description, station
    location, model run time (if available), time zone label and all parsed
    forecast data with coloured columns.  If parsing fails, a plain text
    error message is returned.
    """
    # Retrieve the desired timezone from the query string; this may be
    # empty or invalid.  parse_bull() will handle validation and fall
    # back to the buoy's local timezone if necessary.
    tz_param = request.args.get("tz", "")
    unit_param = request.args.get("unit", "US") or "US"
    cycle_str, location_str, model_run_str, rows, effective_tz_name, error = parse_bull(station_id, tz_param or None)
    if rows is None:
        return f"Error: {error}", 404
    # Use the effective timezone name as the label in the Excel file and convert heights if needed
    bio = build_excel_workbook(cycle_str, location_str, model_run_str, rows, effective_tz_name, unit_param)
    filename = f"{station_id}_table_view.xlsx"
    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    app.run(debug=True)
