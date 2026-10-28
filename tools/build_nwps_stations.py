"""Build nwps_stations.json: the forecast stations NOAA NWPS publishes 2-D spectra for (plan section 59).

For every listed GFS station (station_list.json) the last DAYS days of every NWS office on NOMADS are surveyed (paced
listings); a CG1 spectra file named with the station id is a candidate. Its head is read (one ranged request): the
point's position and whether its first time holds a spectrum. A station is kept when the point lies within MAX_KM of
NDBC's current position of the buoy (NDBC's station table) and the file holds data; when two offices publish the
station, the office whose point lies nearest the buoy wins. The stations stay in the GFS files unchanged (they are GFS
stations); this file only names which office serves each and where NOAA's point is.

Entry: {"office", "region", "lat", "lon", "ndbc_lat", "ndbc_lon", "km"}; "left_out" records the rejected ids with the
reason (nodata / far / no NDBC position), for the record. Run (network, ~5 min paced):

    python tools/build_nwps_stations.py            writes ../nwps_stations.json
    python tools/build_nwps_stations.py --check    prints the survey, writes nothing
Re-run by hand when NOAA adds or moves points (the blank-point tool's pattern, section 52).
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT_PATH = os.path.join(ROOT, "nwps_stations.json")
sys.path.insert(0, os.path.join(HERE, "model_frames"))
import nwps_fetch as N  # noqa: E402

TABLE_URL = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
MAX_KM = 25.0
DAYS = 3


def km_between(lat1, lon1, lat2, lon2):
    import math
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def ndbc_positions(text):
    """{id: (lat, lon)} from NDBC's station table (field 7: "19.196 N 160.639 W (...)")."""
    out = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("|")
        if len(parts) < 8:
            continue
        toks = parts[6].split()
        try:
            lat = float(toks[0]) * (1 if toks[1] == "N" else -1)
            lon = float(toks[2]) * (1 if toks[3] == "E" else -1)
        except (IndexError, ValueError):
            continue
        out[parts[0].strip().upper()] = (lat, lon)
    return out


def survey(stations, now=None, log=print):
    """{sid: [(office, region, run day, cycle, url)]} for every station with a CG1 spectra file in the last DAYS days;
    the newest cycle per office."""
    now = now or datetime.now(timezone.utc)
    found = {}
    for office, region in N.OFFICES.items():
        newest = None
        for day in N.recent_days(now, DAYS):
            for hh in sorted(N.cycles(region, day, office), reverse=True):
                files = N.spectra_listing(region, day, office, hh)
                if files and len(files) > 1:
                    newest = (day, hh, files)
                    break
            if newest:
                break
        if not newest:
            log(f"{office}: no run with spectra in the last {DAYS} days")
            continue
        day, hh, files = newest
        hits = [sid for sid in files if sid and sid.upper() in stations]
        log(f"{office}: run {day:%Y%m%d}{hh:02d}, {len(files) - (1 if '' in files else 0)} spectra files, {len(hits)} of ours")
        for sid in hits:
            found.setdefault(sid.upper(), []).append((office, region, day, hh, N.spc_url(region, day, office, hh, sid)))
    return found


def build(found, positions, read_head=N.spectra_head, log=print):
    out, left_out = {}, {}
    for sid in sorted(found):
        pos = positions.get(sid)
        if not pos:
            left_out[sid] = "no NDBC position"
            continue
        best = None
        reasons = []
        for office, region, day, hh, url in found[sid]:
            info = N.head_info(read_head(url))
            if not info:
                reasons.append(f"{office}: not a spectra file")
                continue
            lon, lat, has_data = info
            if not has_data:
                reasons.append(f"{office}: nodata")
                continue
            km = km_between(lat, lon, pos[0], pos[1])
            if km > MAX_KM:
                reasons.append(f"{office}: far ({km:.0f} km from the buoy)")
                continue
            if best is None or km < best["km"]:
                best = {"office": office, "region": region, "lat": round(lat, 4), "lon": round(lon, 4),
                        "ndbc_lat": pos[0], "ndbc_lon": pos[1], "km": round(km, 1)}
        if best:
            out[sid] = best
        else:
            left_out[sid] = "; ".join(reasons) or "no file"
        log(f"{sid}: {best['office'] + ' %.1f km' % best['km'] if best else 'LEFT OUT: ' + left_out[sid]}")
    return out, left_out


def main(argv):
    check = "--check" in argv
    stations = set(json.load(open(os.path.join(ROOT, "station_list.json"), encoding="utf-8")))
    req = urllib.request.Request(TABLE_URL, headers={"User-Agent": "allshoresurf.com station build"})
    with urllib.request.urlopen(req, timeout=60) as r:
        positions = ndbc_positions(r.read().decode("utf-8", "replace"))
    found = survey(stations)
    out, left_out = build(found, positions)
    doc = {"format": "nwps-stations-v1", "built_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "max_km": MAX_KM, "stations": out, "left_out": left_out}
    print(f"{len(out)} stations kept, {len(left_out)} left out")
    if check:
        return 0
    with open(OUT_PATH, "w", encoding="utf-8", newline="\n") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
        f.write("\n")
    print(f"wrote {OUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
