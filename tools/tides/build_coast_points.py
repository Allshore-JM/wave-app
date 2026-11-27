"""Coast points for FES2022 model tides where no tide station is near (plan section 113 C).

    python -I tools/tides/build_coast_points.py [--coastline ne_10m_coastline.geojson]

Walks Natural Earth's 10 m coastline (public domain; the pinned file below, checked by sha256 and cached beside this
script, never committed) and keeps one point every SPACING_KM along each coast line (the first vertex of a line too,
so a small island gets one), at |lat| < MAX_LAT (owner, 2026-11-27: every sea below 66 degrees; the polar coasts with
their sea ice are left out). A point is dropped within NEAR_KM of any tide station the layer already has (NOAA,
TICON, the agencies' snapshot, whether or not a keyed agency is on) or of any forecast point (those get their own model
point: fes_points.py), and within SPACING_KM / 2 of a coast point already kept (lines that meet, islands close
together). Writes the committed tools/tides/coast_points.json: id ("model:c" + the position in 1/100 degree, so an id
stays the same while its point does), lat, lon, zone (the live buoys' nearest-civil-zone rule, app._nearest_civil_tz).
fes_points.py then keeps a point only where AVISO's model has ocean within 15 km and land within 30 km (a point in a
lake or the Caspian drops out there).
"""
import argparse
import hashlib
import json
import math
import os
import sys
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)
import build_global as BG  # noqa: E402

NE_COMMIT = "ca96624a56bd078437bca8184e78163e5039ad19"
NE_URL = ("https://raw.githubusercontent.com/nvkelso/natural-earth-vector/%s/geojson/ne_10m_coastline.geojson"
          % NE_COMMIT)
NE_SHA256 = "6f75ae0e0de157b14946e2255eb1f5486d9a13819032e26d4610852d296788f6"
NE_CACHE = os.path.join(HERE, ".ne_10m_coastline.geojson")
OUT = os.path.join(HERE, "coast_points.json")
SPACING_KM = 25.0
NEAR_KM = 15.0
MAX_LAT = 66.0


def coastline(path=None):
    """The coastline's lines [[(lon, lat), ...], ...] from the pinned file (downloaded once, sha256 checked)."""
    if not path:
        path = NE_CACHE
        if not os.path.exists(path):
            print("downloading", NE_URL)
            req = urllib.request.Request(NE_URL, headers={"User-Agent": "allshoresurf.com tools"})
            with urllib.request.urlopen(req, timeout=300) as r:
                body = r.read()
            if hashlib.sha256(body).hexdigest() != NE_SHA256:
                raise SystemExit("the coastline file is not the pinned one (sha256)")
            with open(path, "wb") as fh:
                fh.write(body)
    with open(path, "rb") as fh:
        body = fh.read()
    if hashlib.sha256(body).hexdigest() != NE_SHA256:
        raise SystemExit("%s is not the pinned coastline (sha256)" % path)
    lines = []
    for ft in json.loads(body.decode("utf-8"))["features"]:
        g = ft.get("geometry") or {}
        parts = g.get("coordinates") or []
        for part in (parts if g.get("type") == "MultiLineString" else [parts]):
            if len(part) >= 1:
                lines.append([(float(x), float(y)) for x, y in part])
    return lines


def walk(lines, spacing=SPACING_KM):
    """One point every `spacing` km along each line, from its first vertex: [(lat, lon), ...]."""
    out = []
    for line in lines:
        out.append((line[0][1], line[0][0]))
        carry = 0.0                                    # km walked since the last point
        for (x0, y0), (x1, y1) in zip(line[:-1], line[1:]):
            seg = BG.km(y0, x0, y1, x1)
            if seg <= 0:
                continue
            t = spacing - carry
            while t <= seg:
                u = t / seg
                lon = x0 + u * (((x1 - x0) + 180.0) % 360.0 - 180.0)    # across the date line too
                out.append((y0 + u * (y1 - y0), (lon + 180.0) % 360.0 - 180.0))
                t += spacing
            carry = seg - (t - spacing)
    return out


def taken_points():
    """Every tide station (NOAA, TICON, the agencies: all of them) and every forecast point."""
    pts = []
    for name in ("tide_stations.json", "tide_global.json", "tide_national.json"):
        path = os.path.join(ROOT, name)
        if os.path.exists(path):
            d = json.load(open(path, encoding="utf-8"))
            f = d["fields"]
            pts += [(float(r[f.index("lat")]), float(r[f.index("lon")])) for r in d["stations"]]
    coords = json.load(open(os.path.join(ROOT, "station_coords.json"), encoding="utf-8"))
    pts += [(float(c["lat"]), (float(c["lon"]) + 180.0) % 360.0 - 180.0) for c in coords.values()]
    return pts


def point_id(lat, lon):
    return "model:c%05d%05d" % (int(round((lat + 90.0) * 100)), int(round(((lon + 180.0) % 360.0) * 100)) % 36000)


def select(samples, taken, spacing=SPACING_KM, near=NEAR_KM, max_lat=MAX_LAT):
    """The samples to keep: below max_lat, more than `near` km from every taken point and more than spacing / 2 from
    a kept one; ids unique."""
    grid_taken = BG.Grid(taken)
    kept, grid_kept, ids = [], BG.Grid([]), set()
    for lat, lon in samples:
        if abs(lat) >= max_lat or grid_taken.near(lat, lon, near) or grid_kept.near(lat, lon, spacing / 2.0):
            continue
        pid = point_id(lat, lon)
        if pid in ids:
            continue
        ids.add(pid)
        kept.append((pid, round(lat, 4), round(lon, 4)))
        grid_kept.cells.setdefault((math.floor(lat), math.floor(lon)), []).append((lat, lon))
    return kept


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coastline", help="the pinned ne_10m_coastline.geojson (else downloaded and cached)")
    args = ap.parse_args()
    import app as A                                    # the live buoys' nearest-civil-zone rule
    lines = coastline(args.coastline)
    samples = walk(lines)
    kept = select(samples, taken_points())
    # south of 60 degrees (Antarctica): UTC (the nearest-zone rule names zones a continent away there)
    rows = [[pid, lat, lon, "UTC" if lat < -60.0 else A._nearest_civil_tz(lat, lon)] for pid, lat, lon in kept]
    doc = {"source": "Natural Earth 10 m coastline (public domain) @ %s, every %g km, |lat| < %g, %g km clear of tide "
                     "stations and forecast points (tools/tides/build_coast_points.py)" % (NE_COMMIT[:7], SPACING_KM,
                                                                                            MAX_LAT, NEAR_KM),
           "built": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
           "fields": ["id", "lat", "lon", "tz"], "points": rows}
    with open(OUT, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, separators=(",", ":"))
    print("lines %d, samples %d, kept %d -> %s" % (len(lines), len(samples), len(rows), OUT))


if __name__ == "__main__":
    main()
