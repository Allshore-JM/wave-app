"""Build station_timezones.json: each forecast buoy's NEAREST CIVIL (DST-aware) timezone.

Why: TimezoneFinder.timezone_at() returns longitude-banded nautical Etc/GMT zones for
open water. Those observe no DST and are often an hour off from the adjacent coast, so
forecast/observation times read "slightly off". Instead we assign each buoy the timezone
of the nearest civil (land) region, which observes DST and matches the coast users expect.

Policy: nearest civil zone for every buoy that has land within ~1100 km; buoys with no
land in range (true open ocean) keep the nautical Etc/GMT zone from timezone_at().

Method: for each buoy (lat,lon from station_coords.json), if timezone_at_land() returns a
zone at the point use it; otherwise an expanding-ring search calls timezone_at_land() on
points around the buoy. Sample count per ring is proportional to radius (~14 km arc
spacing at every radius) so small islands (e.g. Midway atoll) are not skipped. Longitudes
are wrapped to [-180,180) so antimeridian buoys work. First ring with any hit -> nearest
hitting sample (haversine).

Run:  python tools/build_station_timezones.py   (writes ../station_timezones.json)
Re-run whenever station_coords.json changes. The app reads the output via
load_station_timezones()/get_station_tz() and falls back to a live lookup if a buoy is
absent, so the file is an optimization+correction layer, not a hard dependency.
"""
import json
import math
import os
from timezonefinder import TimezoneFinder

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
COORDS_PATH = os.path.join(ROOT, "station_coords.json")
OUT_PATH = os.path.join(ROOT, "station_timezones.json")
# "Land in range" is defined by the ring search extent below (~10 deg ~= 1100 km).
# If no civil land is found within that radius the buoy is treated as open ocean.

tf = TimezoneFinder()

# Radius rings (degrees): fine near the buoy, coarser far out.
RINGS = (
    [round(0.1 * i, 2) for i in range(1, 21)]        # 0.1 .. 2.0
    + [round(2.0 + 0.25 * i, 2) for i in range(1, 17)]  # 2.25 .. 6.0
    + [round(6.0 + 0.5 * i, 2) for i in range(1, 9)]    # 6.5 .. 10.0
)


def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(min(1.0, math.sqrt(a)))


def _land_at(lat, lon):
    try:
        return tf.timezone_at_land(lat=lat, lng=lon)
    except Exception:
        return None


def nearest_civil(lat, lon):
    """Return (tz_name, distance_km) of the nearest civil land zone, or (None, None)."""
    here = _land_at(lat, lon)
    if here:
        return here, 0.0
    lat_rad = math.radians(lat)
    for r in RINGS:
        n = max(16, int(50 * r))  # ~14 km arc spacing -> do not skip small islands
        best, best_d = None, 1e9
        for i in range(n):
            th = 2 * math.pi * i / n
            la = lat + r * math.cos(th)
            if la > 89 or la < -89:
                continue
            lo = ((lon + (r * math.sin(th) / max(0.2, math.cos(lat_rad))) + 180) % 360) - 180
            tz = _land_at(la, lo)
            if tz:
                d = _haversine_km(lat, lon, la, lo)
                if d < best_d:
                    best_d, best = d, tz
        if best:
            return best, best_d
    return None, None


def main():
    coords = json.load(open(COORDS_PATH))
    out = {}
    kept_nautical = 0
    for sid, c in coords.items():
        lat, lon = c.get("lat"), c.get("lon")
        if lat is None or lon is None:
            continue
        civ, _dist = nearest_civil(lat, lon)
        if civ is not None:
            out[sid] = civ
        else:
            # No civil land within the search radius -> open ocean: keep nautical zone.
            try:
                naut = tf.timezone_at(lat=lat, lng=lon)
            except Exception:
                naut = None
            out[sid] = naut or "UTC"
            kept_nautical += 1
    json.dump(out, open(OUT_PATH, "w"), indent=0, sort_keys=True)
    print("wrote %s (%d buoys, %d kept nautical)" % (OUT_PATH, len(out), kept_nautical))


if __name__ == "__main__":
    main()
