from flask import Flask, render_template, request, jsonify
import json
from pathlib import Path

# Import our time index helper
from waveapp_v2.wavecore import hourly_series_from_day_hour

# Base directory for this file
BASE_DIR = Path(__file__).resolve().parent

app = Flask(__name__, template_folder=str(BASE_DIR / 'templates'))


def load_station_coords():
    """Load station latitude/longitude mapping from a JSON file."""
    coords_path = BASE_DIR.parent / 'station_coords.json'
    with open(coords_path, 'r') as f:
        coords = json.load(f)
    return coords


def get_station_list():
    """Return list of (id, name) tuples for available stations."""
    list_path = BASE_DIR.parent / 'station_list.json'
    with open(list_path, 'r') as f:
        stations = json.load(f)
    return [(sid, meta['name']) for sid, meta in stations.items()]


@app.route('/stations.json')
def stations_json():
    """Serve station metadata for map markers."""
    coords = load_station_coords()
    stations = [
        {'id': sid, 'lat': info['lat'], 'lon': info['lon']}
        for sid, info in coords.items()
    ]
    return jsonify(stations)


@app.route('/', methods=['GET', 'POST'])
def index():
    """
    Render the main page with station, timezone and unit dropdowns.
    Currently does not perform bulletin parsing; the time index logic is
    encapsulated in wavecore.timeindex for future integration.
    """
    stations = get_station_list()
    timezones = ['UTC']  # placeholder timezones; actual options determined by buoy
    units = ['US', 'Metric']
    return render_template(
        'index.html',
        stations=stations,
        selected_station=None,
        timezones=timezones,
        selected_tz='UTC',
        units=units,
        selected_unit='US',
        table_html=None,
        error=None,
    )


if __name__ == '__main__':
    app.run(debug=True)
