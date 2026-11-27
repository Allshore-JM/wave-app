"""Coast points for FES2022 model tides where no tide station is near (plan section 113 C).

    python -I tools/tides/build_coast_points.py [--coastline ne_10m_coastline.geojson] [--geonames cities1000.zip]

Walks Natural Earth's 10 m coastline (public domain; the pinned file below, checked by sha256 and cached beside this
script, never committed; only its "Coastline" features: G80-9 left out its "Null island") and keeps one point every
SPACING_KM along each coast line (the first vertex of a line too, so a small island gets one), at |lat| < MAX_LAT
(owner, 2026-11-27: every sea below 66 degrees; the polar coasts with their sea ice are left out). A point is dropped
within NEAR_KM of any tide station the layer already has (NOAA, TICON, the agencies' snapshot: all of them, a keyed
agency too, so Taiwan has no coast points: G80-7) and within SPACING_KM / 2 of a coast point already kept (lines that
meet, islands close together). Forecast points do not block coast points here: fes_points.py drops a coast point
within NEAR_KM of a forecast point that got a model tide (G80-7: one that did not still leaves its coast covered).

Writes the committed tools/tides/coast_points.json: id ("model:c" + the walked position in 1/100 degree: an id stays the
same while its point does; the stored lat / lon are that position rounded to 1/10,000 degree, so a few ids differ from
point_id(stored lat, lon) in the last digit), lat, lon, zone (the live buoys' nearest-civil-zone rule; UTC south of
60 S), and its NAME (G80-1: a proper place name, never a surf spot, owner 2026-11-27): "Off <GeoNames town>" for the
point nearest a town within OFF_KM, else "<km> km <direction> of <town>" (the nearest within FAR_KM), else its position;
every name is unique (another point near the same town gets its distance and direction). The names are made here, once,
so the station list keeps its ETag from one job run to the next (G80-6).

fes_points.py then keeps a coast point only where AVISO's model has ocean within COAST_NEAREST_KM of it (a point in a
lake, the Caspian or a lagoon behind a barrier beach gets no tide from the sea beyond: G80-3).
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
OFF_KM = 15.0                            # "Off <town>" within this
FAR_KM = 300.0                           # "<km> km <direction> of <town>" within this; farther: the position
COMPASS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def coastline(path=None):
    """The coastline's lines [[(lon, lat), ...], ...] from the pinned file (downloaded once, sha256 checked); only the
    features classed "Coastline"."""
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
    return lines_of(json.loads(body.decode("utf-8")))


def lines_of(doc):
    lines = []
    for ft in doc["features"]:
        if (ft.get("properties") or {}).get("featurecla") != "Coastline":
            continue
        g = ft.get("geometry") or {}
        parts = g.get("coordinates") or []
        for part in (parts if g.get("type") == "MultiLineString" else [parts]):
            if len(part) >= 1:
                lines.append([(float(x), float(y)) for x, y in part])
    return lines


def _lon(x):
    return (float(x) + 180.0) % 360.0 - 180.0


def walk(lines, spacing=SPACING_KM):
    """One point every `spacing` km along each line, from its first vertex: [(lat, lon), ...], lon in [-180, 180)."""
    out = []
    for line in lines:
        out.append((line[0][1], _lon(line[0][0])))
        carry = 0.0                                    # km walked since the last point
        for (x0, y0), (x1, y1) in zip(line[:-1], line[1:]):
            seg = BG.km(y0, x0, y1, x1)
            if seg <= 0:
                continue
            t = spacing - carry
            while t <= seg:
                u = t / seg
                lon = x0 + u * (((x1 - x0) + 180.0) % 360.0 - 180.0)    # across the date line too
                out.append((y0 + u * (y1 - y0), _lon(lon)))
                t += spacing
            carry = seg - (t - spacing)
    return out


def taken_points():
    """Every tide station (NOAA, TICON, the agencies: all of them)."""
    pts = []
    for name in ("tide_stations.json", "tide_global.json", "tide_national.json"):
        path = os.path.join(ROOT, name)
        if os.path.exists(path):
            d = json.load(open(path, encoding="utf-8"))
            f = d["fields"]
            pts += [(float(r[f.index("lat")]), _lon(r[f.index("lon")])) for r in d["stations"]]
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


def bearing(lat1, lon1, lat2, lon2):
    """The 8-point compass direction from (lat1, lon1) to (lat2, lon2)."""
    p1, p2, dl = math.radians(lat1), math.radians(lat2), math.radians(lon2 - lon1)
    b = math.degrees(math.atan2(math.sin(dl) * math.cos(p2), math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)))
    return COMPASS[int(((b % 360.0) + 22.5) // 45.0) % 8]


def far_grid(places_pts):
    """A Grid with 5-degree cells for the FAR_KM search (BG.Grid looks one 1-degree cell around)."""
    g = BG.Grid([])
    g.cells = {}
    for p in places_pts:
        g.cells.setdefault((math.floor(p[0] / 5.0), math.floor(p[1] / 5.0)), []).append(p)
    return g


def nearest_far(g, lat, lon, d_km):
    best = None
    cy, cx = math.floor(lat / 5.0), math.floor(lon / 5.0)
    reach = 1 + int(d_km // 300.0)
    for dy in range(-reach, reach + 1):
        for dx in range(-reach - 1, reach + 2):
            for p in g.cells.get((cy + dy, ((cx + dx + 36) % 72) - 36), []):
                d = BG.km(lat, lon, p[0], p[1])
                if d <= d_km and (best is None or d < best[0]):
                    best = (d, p)
    return best


def names(points, places_pts):
    """{id: name} for [(id, lat, lon), ...] (see the module text): unique names of real places."""
    near = BG.Grid(places_pts)
    far = far_grid(places_pts)
    out, by_name = {}, {}
    for pid, lat, lon in points:
        hit = near.near(lat, lon, OFF_KM)
        if hit:
            d, p = hit[0]
        else:
            got = nearest_far(far, lat, lon, FAR_KM)
            d, p = got if got else (None, None)
        if p is None:
            out[pid] = "Coast %.2f, %.2f" % (lat, lon)
            continue
        out[pid] = ("Off %s" % p[2]) if d <= OFF_KM else "%d km %s of %s" % (int(round(d)), bearing(p[0], p[1], lat, lon), p[2])
        by_name.setdefault(out[pid], []).append((d, pid, lat, lon, p))
    for name, group in by_name.items():               # "Off X" twice: the nearest keeps it, the others get km + direction
        if len(group) < 2:
            continue
        group.sort()
        for d, pid, lat, lon, p in group[1:]:
            out[pid] = "%d km %s of %s" % (max(1, int(round(d))), bearing(p[0], p[1], lat, lon), p[2])
    seen = {}
    for pid in sorted(out):                            # anything still the same (two points one km apart): its position
        if out[pid] in seen:
            lat, lon = next((la, lo) for i, la, lo in points if i == pid)
            out[pid] = "%s (%.2f, %.2f)" % (out[pid], lat, lon)
        seen[out[pid]] = pid
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coastline", help="the pinned ne_10m_coastline.geojson (else downloaded and cached)")
    ap.add_argument("--geonames", help="GeoNames cities1000.zip (else build_global's cached download)")
    args = ap.parse_args()
    import app as A                                    # the live buoys' nearest-civil-zone rule
    lines = coastline(args.coastline)
    samples = walk(lines)
    kept = select(samples, taken_points())
    places = BG.load_geonames(args.geonames)
    places_pts = [p for cell in places.cells.values() for p in cell]
    named = names(kept, places_pts)
    # south of 60 degrees (Antarctica): UTC (the nearest-zone rule names zones a continent away there)
    rows = [[pid, lat, lon, "UTC" if lat < -60.0 else A._nearest_civil_tz(lat, lon), named[pid]] for pid, lat, lon in kept]
    doc = {"source": "Natural Earth 10 m coastline (public domain) @ %s, every %g km, |lat| < %g, %g km clear of tide "
                     "stations; names from GeoNames cities1000 (CC BY 4.0) (tools/tides/build_coast_points.py)"
                     % (NE_COMMIT[:7], SPACING_KM, MAX_LAT, NEAR_KM),
           "built": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
           "fields": ["id", "lat", "lon", "tz", "name"], "points": rows}
    with open(OUT, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(doc, fh, separators=(",", ":"), ensure_ascii=False)
    print("lines %d, samples %d, kept %d -> %s" % (len(lines), len(samples), len(rows), OUT))


if __name__ == "__main__":
    main()
