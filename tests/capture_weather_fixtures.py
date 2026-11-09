"""Capture the land-weather fixtures (plan section 91) into tests/fixtures/weather/.

    python -I tests/capture_weather_fixtures.py            # asks Open-Meteo and api.weather.gov (4 paced requests)
    python -I tests/capture_weather_fixtures.py --from DIR # trims earlier captures (om_oahu.json, nws_*.json) instead

om_oahu.json is Open-Meteo's answer for central Oahu (21.45 N 158.0 W) to the site's own request (four models, the
twelve hourly variables, 16 days + 1 past day, GMT, m/s); nws_points_oahu.json and nws_grid_oahu.json are the NWS
point and its grid forecast (the grid trimmed to the series the reader uses); nws_points_paris_404.json is the NWS
answer outside its grids. The tests pin values computed from these files, never live data.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "fixtures", "weather")
sys.path.insert(0, os.path.dirname(HERE))

GRID_KEYS = ("@id", "updateTime", "validTimes", "elevation", "temperature", "dewpoint", "relativeHumidity",
             "skyCover", "windSpeed", "windDirection", "windGust", "weather", "quantitativePrecipitation",
             "snowfallAmount")


def trim_grid(raw):
    doc = json.loads(raw)
    props = doc.get("properties") or {}
    doc["properties"] = {k: props[k] for k in GRID_KEYS if k in props}
    return json.dumps(doc, separators=(",", ":")).encode()


def get(url, headers):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def live():
    import weather_sources as W
    lat, lon = 21.45, -158.0
    url = (f"{W.OM_FORECAST}?latitude={lat}&longitude={lon}&hourly={','.join(W.OM_VARS)}"
           f"&models={','.join(W.OM_NAME[m] for m in W.OM_MODELS)}&timezone=GMT&forecast_days={W.OM_DAYS}"
           f"&past_days={W.OM_PAST_DAYS}&wind_speed_unit=ms")
    files = {}
    s, b = get(url, W.OM_HEADERS)
    assert s == 200, s
    files["om_oahu.json"] = b
    time.sleep(1)
    s, b = get(f"{W.NWS_API}/points/{lat},{lon}", W.NWS_HEADERS)
    assert s == 200, s
    files["nws_points_oahu.json"] = b
    time.sleep(1)
    s, b = get(json.loads(b)["properties"]["forecastGridData"], W.NWS_HEADERS)
    assert s == 200, s
    files["nws_grid_oahu.json"] = trim_grid(b)
    time.sleep(1)
    s, b = get(f"{W.NWS_API}/points/48.85,2.35", W.NWS_HEADERS)
    assert s == 404, s
    files["nws_points_paris_404.json"] = b
    return files


def from_dir(d):
    def rd(name):
        with open(os.path.join(d, name), "rb") as f:
            return f.read()
    return {"om_oahu.json": rd("om_oahu.json"), "nws_points_oahu.json": rd("nws_points_oahu.json"),
            "nws_grid_oahu.json": trim_grid(rd("nws_grid_oahu.json")),
            "nws_points_paris_404.json": rd("nws_points_paris.json")}


def main(argv):
    files = from_dir(argv[argv.index("--from") + 1]) if "--from" in argv else live()
    os.makedirs(OUT, exist_ok=True)
    for name, body in files.items():
        with open(os.path.join(OUT, name), "wb") as f:
            f.write(body)
        print(name, len(body))


if __name__ == "__main__":
    main(sys.argv[1:])
