from flask import Flask, render_template, request, jsonify
import json
from pathlib import Path

# Import our time index helper
from waveapp_v2.wavecore import hourly_series_from_day_hour

# Base directory for this file
BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = Path(__file__).resolve().parents[2]

app = Flask(__name__, template_folder=str(BASE_DIR / 'templates'))


def load_station_coords():
    """Load station latitude/longitude mapping from a JSON file."""
    coords_path = ROOT_DIR / 'station_coords.json'
    try:
        with open(coords_path, 'r') as f:
            coords = json.load(f)
    except FileNotFoundError:
        coords = {}
    return coords


def get_station_list():
    """Return list of (id, name) tuples for available stations."""
    list_path = ROOT_DIR / 'station_list.json'
    try:
        with open(list_path, 'r') as f:
            stations = json.load(f)
    except FileNotFoundError:
        stations = {}
    return [(sid, meta.get('name', sid)) for sid, meta in stations.items()]


@app.route('/stations.json')
def stations_json():
    """Serve station metadata as JSON."""
    coords = load_station_coords()
    return jsonify(coords)


@app.route('/')
def index():
    """Render the main page."""
    stations = get_station_list()
    return render_template('index.html', stations=stations)
