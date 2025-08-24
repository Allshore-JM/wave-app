-from flask import Flask, render_template, request, jsonify
-import json
-from pathlib import Path
-
-# Import our time index helper
-from waveapp_v2.wavecore import hourly_series_from_day_hour
-
-# Base directory for this file
-BASE_DIR = Path(__file__).resolve().parent
-ROOT_DIR = Path(__file__).resolve().parents[2]
-
-app = Flask(__name__, template_folder=str(BASE_DIR / 'templates'))
+from flask import Flask, render_template, request, jsonify, abort
+import json
+import logging
+from pathlib import Path
+from waveapp_v2.wavecore import hourly_series_from_day_hour
+
+logging.basicConfig(level=logging.INFO)
+BASE_DIR = Path(__file__).resolve().parent
+# Search for data files here, in order:
+_SEARCH_BASES = (BASE_DIR, BASE_DIR.parent, BASE_DIR.parent.parent)
+app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))
 
+def _load_json_anywhere(filename: str):
+    """Load JSON from current dir, parent, or repo root (first found)."""
+    for base in _SEARCH_BASES:
+        p = base / filename
+        if p.exists():
+            with p.open("r") as f:
+                return json.load(f)
+    raise FileNotFoundError(f"{filename} not found in {_SEARCH_BASES}")
 
-def load_station_coords():
-    """Load station latitude/longitude mapping from a JSON file."""
-    coords_path = BASE_DIR / 'station_coords.json'
-    with open(coords_path, 'r') as f:
-        coords = json.load(f)
-    return coords
+def load_station_coords():
+    """Return {id: {'lat': float, 'lon': float}}."""
+    try:
+        coords = _load_json_anywhere("station_coords.json")
+    except FileNotFoundError:
+        logging.warning("station_coords.json not found")
+        return {}
+    out = {}
+    # Accept dict mapping or list of objects
+    if isinstance(coords, dict):
+        items = coords.items()
+    elif isinstance(coords, list):
+        items = []
+        for obj in coords:
+            sid = str(obj.get("id") or obj.get("station") or obj.get("sid") or "")
+            if sid:
+                items.append((sid, obj))
+    else:
+        logging.warning("Unexpected coords type: %s", type(coords))
+        return out
+    for sid, v in items:
+        if not isinstance(v, dict):
+            continue
+        lat = v.get("lat") or v.get("latitude") or v.get("lat_dd") or v.get("latDeg")
+        lon = v.get("lon") or v.get("lng") or v.get("longitude") or v.get("lon_dd") or v.get("lonDeg")
+        try:
+            if lat is not None and lon is not None:
+                out[str(sid)] = {"lat": float(lat), "lon": float(lon)}
+        except (TypeError, ValueError):
+            continue
+    return out
 
-def get_station_list():
-    """Return list of (id, name) tuples for available stations."""
-    list_path = ROOT_DIR / 'station_list.json'
-    with open(list_path, 'r') as f:
-        stations = json.load(f)
-    # Expected dict: {id: { "name": ...}}
-    return [(sid, meta.get('name', sid)) for sid, meta in stations.items()]
+def get_station_list():
+    """Return list of (id, name) tuples; accept dict OR list JSON."""
+    try:
+        stations = _load_json_anywhere("station_list.json")
+    except FileNotFoundError:
+        logging.warning("station_list.json not found")
+        return []
+    if isinstance(stations, dict):
+        return [
+            (str(sid), meta.get("name", str(sid)))
+            for sid, meta in stations.items()
+            if isinstance(meta, dict)
+        ]
+    elif isinstance(stations, list):
+        return [(str(sid), str(sid)) for sid in stations]
+    else:
+        logging.warning("Unexpected station_list type: %s", type(stations))
+        return []
 
 @app.route("/")
 def index():
     return render_template("index.html")
 
+@app.get("/api/stations")
+def api_stations():
+    listing = get_station_list()
+    coords = load_station_coords()
+    rows = []
+    for sid, name in listing:
+        c = coords.get(sid)
+        if not c:
+            continue
+        rows.append({"id": sid, "name": name, "lat": c["lat"], "lon": c["lon"]})
+    return jsonify(rows)
+
 @app.get("/api/series")
 def api_series():
-    # existing logic…
+    sid = request.args.get("station")
+    day = request.args.get("start_day")
+    try:
+        start_hour = int(request.args.get("start_hour", 0))
+        hours = int(request.args.get("hours", 72))
+    except (TypeError, ValueError):
+        return abort(400, "start_hour/hours must be integers")
+    tz = request.args.get("tz", "UTC")
+    series = hourly_series_from_day_hour(day, start_hour, hours, tz)
+    return jsonify(series)
