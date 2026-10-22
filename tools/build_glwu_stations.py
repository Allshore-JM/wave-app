"""Build glwu_stations.json: the Great Lakes forecast stations (plan section 50, the lake tables).

The GLWU job publishes a bulletin for each station in tools/model_frames/glwu_fetch.STATIONS (the 45xxx buoys, the
C-MAN / NOS sites and NOAA's named beach points; the rip-current points left out). The site lists them as forecast
stations beside the GFS-Wave ones. Their names, positions and zones live in their OWN file, not in station_list.json /
station_coords.json / station_timezones.json: the forecast-point time-zone rule reads every coordinate of those
(app._nearest_station_tz), and their pins (tests/test_station_list.py) keep it as reviewed.

Each entry: {"name", "lat", "lon", "tz"}. The name is NDBC's station-table name (matched case-insensitively; a leading
"<NOS id> - " and a trailing "(nnn)" dropped), else a name written here for NOAA's named beach points. The zone is the
nearest civil (DST-aware) zone, by tools/build_station_timezones.nearest_civil (the GFS stations' rule).

Run:  python tools/build_glwu_stations.py      (network: NDBC's station table; writes ../glwu_stations.json)
Re-run when glwu_fetch.STATIONS changes.
"""
import json
import os
import re
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT_PATH = os.path.join(ROOT, "glwu_stations.json")
sys.path.insert(0, os.path.join(HERE, "model_frames"))
sys.path.insert(0, HERE)
import glwu_fetch as G  # noqa: E402

TABLE_URL = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
# NOAA's named beach points have no NDBC row: names from their places (the bulletin id is NOAA's shorthand)
BEACH_NAMES = {
    "CAPEVINCEN": "Cape Vincent, NY",
    "ELONGPOINT": "East of Long Point, Lake Erie",
    "IndDunesNP": "Indiana Dunes National Park, IN",
    "IndDunesSP": "Indiana Dunes State Park, IN",
    "LITTLESABL": "Little Sable Point, MI",
    "N_GREENBAY": "North Green Bay, MI",
    "N_HAMLIN": "Hamlin Beach, NY",
    "N_MICHIGAN": "Northern Lake Michigan, MI",
    "N_OLCOTT": "Olcott Beach, NY",
    "Silver_Bch": "Silver Beach, St. Joseph, MI",
    "SODUSBAY": "Sodus Bay, NY",
    "STURGEONPT": "Sturgeon Point, NY",
    "S_GREENBAY": "South Green Bay, WI",
    "WhihalaBch": "Whihala Beach, Whiting, IN",
}


def clean_name(name):
    """NDBC's name without a leading NOS id ("9052030 - Oswego, NY") or a trailing number ("Schuyler Reef, NY (251)")."""
    name = re.sub(r"^\d{5,}\s*-\s*", "", name.strip())
    name = re.sub(r"\s*\(\d+\)\s*$", "", name)
    return re.sub(r"\s+", " ", name).strip()


def ndbc_names(text):
    """{lower-case id: name} from NDBC's station table (pipe-separated; the name is the fifth field)."""
    out = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("|")
        if len(parts) > 4 and parts[0].strip() and parts[4].strip():
            out[parts[0].strip().lower()] = clean_name(parts[4])
    return out


# NDBC's own text with an obvious slip ("EAST Lake America" for the Lake Ontario buoy)
NAME_FIXES = {"45012": "EAST Lake Ontario - 20NM North Northeast of Rochester, NY"}


def build(names, zone_of):
    out = {}
    for sid, (lat, lon) in sorted(G.STATIONS.items()):
        name = NAME_FIXES.get(sid) or names.get(sid.lower()) or BEACH_NAMES.get(sid)
        if not name:
            raise SystemExit(f"{sid}: no NDBC name and no name written here")
        tz = zone_of(lat, lon)
        if not tz:
            raise SystemExit(f"{sid}: no civil zone found")
        out[sid] = {"name": name, "lat": lat, "lon": lon, "tz": tz}
    return out


def main():
    import build_station_timezones as Z
    req = urllib.request.Request(TABLE_URL, headers={"User-Agent": "allshoresurf.com station build"})
    with urllib.request.urlopen(req, timeout=60) as r:
        text = r.read().decode("utf-8", "replace")
    out = build(ndbc_names(text), lambda lat, lon: Z.nearest_civil(lat, lon)[0])
    with open(OUT_PATH, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, indent=1, sort_keys=True)
        f.write("\n")
    zones = {}
    for v in out.values():
        zones[v["tz"]] = zones.get(v["tz"], 0) + 1
    print(f"wrote {OUT_PATH}: {len(out)} stations, zones {zones}")


if __name__ == "__main__":
    main()
