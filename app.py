from flask import Flask, render_template, request, send_file
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)

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
    Fetch the list of buoy stations from NOAA's bulls.readme file.

    Returns a list of tuples where each tuple contains the station ID and name. If fetching fails,
    a fallback station is returned.
    """
    try:
        r = requests.get(STATION_LIST_URL, timeout=10)
        r.raise_for_status()
        stations = []
        for line in r.text.splitlines():
            parts = line.split()
            # Valid station lines start with a numeric ID followed by station name
            if len(parts) >= 2 and parts[0].isdigit():
                stations.append((parts[0], " ".join(parts[1:])))
        return stations
    except Exception:
        # Fallback if the station list cannot be fetched
        return [("51201", "Example Station")]


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

    This function fetches the latest .bull file for the station and extracts the necessary data into a pandas
    DataFrame. The DataFrame columns are structured to match the Excel 'Table View' with multi-level headers.

    Parameters:
        station_id (str): The identifier of the buoy station (e.g., '51201').

    Returns:
        tuple: A tuple containing the final DataFrame and an optional error message. If successful, the error is None.
    """
    date_str, run_str = get_latest_run()
    if not date_str:
        return None, "No recent run found."

    # Construct the URL to download the .bull file for the station
    bull_url = f"{NOAA_BASE}/gfs.{date_str}/{run_str}/wave/station/bulls.t{run_str}z/gfswave.{station_id}.bull"
    resp = requests.get(bull_url, timeout=10)
    if resp.status_code != 200:
        return None, f"No .bull file found for {station_id}"

    lines = resp.text.splitlines()

    # Extract cycle and location information from the first two lines
    cycle_info = lines[0].strip() if lines else ""
    loc_info = lines[1].strip() if len(lines) > 1 else ""

    # Determine where the data begins by finding the 'Hr' header line
    data_start = None
    for idx, line in enumerate(lines):
        if line.strip().startswith("Hr"):
            data_start = idx + 1
            break

    if data_start is None:
        return None, "Data section not found in .bull file."

    # Define the column names to match the Table View (time and six swells)
    col_names = ["Time", "UTC Time", "HST Time"]
    for swell in range(1, 7):
        col_names.extend([
            f"Swell {swell} Hs",
            f"Swell {swell} Tp",
            f"Swell {swell} Dir",
        ])
    col_names.append("Combined Hs")

    data_rows = []
    for line in lines[data_start:]:
        parts = line.split()
        # The expected length of each data line is at least 20 elements. If shorter, skip.
        if len(parts) < 20:
            continue
        try:
            hr = float(parts[0])
        except ValueError:
            continue
        # Compute absolute times based on the run timestamp plus forecast hour
        utc_time = datetime.strptime(f"{date_str} {run_str}", "%Y%m%d %H") + timedelta(hours=hr)
        hst_time = utc_time.replace(tzinfo=UTC).astimezone(HST)
        row = [hr, utc_time, hst_time]

        # Extract swell data: 6 swell groups, each with Hs, Tp, Dir
        for swell_idx in range(1, 7):
            base = 3 * (swell_idx - 1) + 6
            try:
                hs = float(parts[base])
                tp = float(parts[base + 1])
                direction = int(parts[base + 2])
                row.extend([hs, tp, direction])
            except (IndexError, ValueError):
                row.extend([None, None, None])
        # Combined Hs is the last field in the line
        try:
            row.append(float(parts[-1]))
        except (IndexError, ValueError):
            row.append(None)

        data_rows.append(row)

    df = pd.DataFrame(data_rows, columns=col_names)

    # Construct multi-level headers similar to the Excel Table View
    header1 = ["Time", "UTC Time", "HST Time"]
    header2 = ["", "", ""]
    for swell in range(1, 7):
        header1.extend([f"Swell {swell}", "", ""])
        header2.extend(["Hs", "Tp", "Dir"])
    header1.append("Combined")
    header2.append("Hs")
    units = ["", "", ""] + ["(ft)", "(s)", "(d)"] * 6 + ["(ft)"]

    df.columns = pd.MultiIndex.from_tuples(zip(header1, header2))

    # Create metadata rows to insert at the top of the DataFrame
    meta_row = pd.DataFrame([[cycle_info] + [""] * (df.shape[1] - 1)], columns=df.columns)
    unit_row = pd.DataFrame([units], columns=df.columns)
    blank_row = pd.DataFrame([[""] * df.shape[1]], columns=df.columns)

    # Combine metadata and data to form the final DataFrame
    final_df = pd.concat([meta_row, unit_row, blank_row, df], ignore_index=True)
    return final_df, None


@app.route("/", methods=["GET", "POST"])
def index():
    """
    Home route for the application.
    Presents a form with a station dropdown and displays the parsed table when submitted.
    """
    stations = get_station_list()
    selected_station = request.form.get("station") if request.method == "POST" else "51201"
    table_html = None
    error = None
    
    if request.method == "POST":
        df, error = parse_bull(selected_station)
        if df is not None:
            # Convert DataFrame to HTML table with Bootstrap classes
            table_html = df.to_html(classes="table table-bordered table-sm", index=False, escape=False)
    
    return render_template("index.html", stations=stations, selected_station=selected_station,
                           table_html=table_html, error=error)


@app.route("/download/<station_id>")
def download(station_id: str):
    """
    Endpoint to download the parsed data as an Excel file.
    Fetches the latest data for the given station and returns an Excel file as a response.
    """
    df, error = parse_bull(station_id)
    if df is None:
        return f"Error: {error}", 404
    # Write DataFrame to an in-memory Excel file
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    filename = f"{station_id}_table_view.xlsx"
    return send_file(output, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


if __name__ == "__main__":
    app.run(debug=True)
