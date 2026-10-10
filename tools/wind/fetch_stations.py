"""Build wind_stations.json: the stations of the map's wind layer (plan section 39).

Per station: id ("coops:1612340", "ndbc:51003", "metar:PHNL"), name, lat, lon, kind, src, tz (the nearest civil zone,
the live buoys' rule), alias (a CO-OPS gauge's NDBC id when NDBC relays it under one: "OOUH1").

kind: "gauge"   a NOAA tide gauge's weather sensors (CO-OPS, 6-minute readings; or NDBC's relay of a gauge CO-OPS does
                not list with an active wind sensor)
      "buoy"    an NDBC buoy (moored: drifting buoys move and are left out)
      "cman"    an NDBC C-MAN coastal station
      "station" another fixed NDBC station (platforms, towers, partners' weather stations)
      "airport" a METAR station within COAST_KM of a coastline, or one at sea (offshore platforms: OFFSHORE_METAR)

Sources (public domain, NOAA / NWS):
- CO-OPS: mdapi stations.json?type=met, then one sensors.json per station (four at a time; --sensors-cache keeps them):
  a station is kept when its "Wind" sensor is active (status 1).
- NDBC: data/latest_obs/latest_obs.txt (stations with a wind speed now) + data/stations/station_table.txt (type, name).
  An NDBC station that relays a kept CO-OPS gauge (its name starts with the gauge's id, "1612340 - Honolulu, HI", or it
  lies within ALIAS_M of one) is not drawn twice: it becomes that gauge's alias.
- METAR (aviationweather.gov, with a User-Agent): data/cache/stations.cache.json.gz (sites) + data/cache/metars.cache.csv.gz
  (the last ~80 minutes of reports). A coastal site missing from the cache is kept when the API shows a wind report in the
  last 24 hours (api/data/metar, a few ids at a time: the API answers 400 reports at most, and allows 100 requests a
  minute).
- The coastline: the site's own GSHHG tier-0 file (models.allshoresurf.com/static/coast/v1/world-i.bin, ~1 km; the
  exposure tool's), its cell-line edges left out (they lie inside land). Lakes count as land (GSHHG level 1).

Run:  python tools/wind/fetch_stations.py --sensors-cache <file>     (writes ../../wind_stations.json)
The site never asks these lists at runtime: re-run now and then and commit the file."""
import argparse
import csv
import gzip
import io
import json
import math
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
OUT = os.path.join(ROOT, "wind_stations.json")
MD = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi"
NDBC_LATEST = "https://www.ndbc.noaa.gov/data/latest_obs/latest_obs.txt"
NDBC_TABLE = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
AWC = "https://aviationweather.gov"
METAR_CACHE = AWC + "/data/cache/metars.cache.csv.gz"
METAR_SITES = AWC + "/data/cache/stations.cache.json.gz"
COAST_URL = "https://models.allshoresurf.com/static/coast/v1/world-i.bin"
UA = "allshoresurf.com wind station snapshot (https://allshoresurf.com)"
FIELDS = ["id", "name", "lat", "lon", "kind", "src", "tz", "alias"]
KINDS = ("gauge", "buoy", "cman", "station", "airport")
COAST_KM = 30.0                 # owner, 2026-10-09: airports within 30 km of the coast only
OFFSHORE_METAR = True           # a METAR site at sea (an offshore platform) is no inland airport: kept
ALIAS_M = 300.0                 # an NDBC station this close to a kept CO-OPS gauge is that gauge
CELL_DEG = 30                   # tier 0's cells: edges along these lines close the clipped pieces (inside land)
BUCKET_DEG = 0.5
METAR_API_IDS = 15              # ids per API request (24 h of hourly reports stays under its 400)
METAR_API_PAUSE_S = 0.8         # 75 requests a minute at most (the API's limit is 100)
LAT_LIMITS = (-79.0, 84.0)      # the map's own latitude limits (templates/index.html LAT_LIMIT_SOUTH / _NORTH)
MOVING = re.compile(r"\b(drift\w*|ferry|ships?|glider|saildrone)\b")   # moves: left out (a "Lightship" is anchored)
_COOPS_IN_NAME = re.compile(r"\s*(\d{7})\s*-\s*(.*)$")
_COOPS_ANYWHERE = re.compile(r"(?<![\d.])(\d{7})(?![\d.])")
_NUM = re.compile(r"-?\d+(\.\d+)?$")


def get_bytes(url, tries=3, headers=None, timeout=60):
    req = urllib.request.Request(url, headers=dict(headers or {}, **{"User-Agent": UA}))
    for k in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception:
            if k == tries - 1:
                raise
            time.sleep(1 + 2 * k)


def get_json(url, tries=3):
    return json.loads(get_bytes(url, tries))


def tide_tool():
    """The tide snapshot tool (tools/tides/fetch_stations.py: same file name, so loaded by path)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("tide_fetch_stations",
                                                  os.path.join(ROOT, "tools", "tides", "fetch_stations.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ------------------------------------------------------------------ NDBC

def parse_latest_obs(text):
    """latest_obs.txt -> {ID: (lat, lon)} for the stations whose row has a wind speed (WSPD not "MM")."""
    out, cols = {}, None
    for line in text.splitlines():
        if line.startswith("#"):
            if cols is None:
                cols = line.lstrip("#").split()
            continue
        parts = line.split()
        if not cols or len(parts) != len(cols):
            continue
        row = dict(zip(cols, parts))
        if not _NUM.match(row.get("WSPD", "MM")):
            continue
        try:
            lat, lon = float(row["LAT"]), float(row["LON"])
        except (KeyError, ValueError):
            continue
        if usable_position(lat, lon):
            out[row["STN"].upper()] = (lat, lon)
    return out


def parse_station_table(text):
    """station_table.txt -> {ID: {"owner", "ttype", "name"}} (ids upper case; HTML entities left as NOAA writes them)."""
    out = {}
    for line in text.splitlines():
        if line.startswith("#") or "|" not in line:
            continue
        p = line.split("|")
        if len(p) < 5:
            continue
        out[p[0].strip().upper()] = {"owner": p[1].strip(), "ttype": p[2].strip(), "name": p[4].strip()}
    return out


def ndbc_kind(sid, ttype):
    """The layer's kind for an NDBC station, or None for one that moves (drifting buoys, ferries, ships, gliders)."""
    t = (ttype or "").lower()
    if MOVING.search(t):
        return None
    if "water level observation network" in t:
        return "gauge"
    if "c-man" in t:
        return "cman"
    if "buoy" in t or "lightship" in t or "uncrewed surface vehicle" in t:   # moored, or holding station (46012)
        return "buoy"
    if not t:
        return "buoy" if re.fullmatch(r"\d{5}", sid) else "station"
    return "station"


def coops_id_in_name(name):
    """NDBC names a relayed CO-OPS gauge "1612340 - Honolulu, HI" -> ("1612340", "Honolulu, HI"); a few name the gauge
    elsewhere ("Castle Island (NOS) 8444069", "Turkey Point Hudson River NERRS, NY (NOS 8518962)") -> (that id, the name
    as written). (None, name) when the name holds no 7-digit number."""
    m = _COOPS_IN_NAME.match(name or "")
    if m:
        return m.group(1), m.group(2)
    m = _COOPS_ANYWHERE.search(name or "")
    return (m.group(1) if m else None), name


def usable_position(lat, lon):
    """A position the map can show: inside its latitude limits, a real longitude, not NOAA's / AWC's placeholders
    (-99.99, -99.99) or (0, 0)."""
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(lat) and math.isfinite(lon)) or (lat == 0 and lon == 0):
        return False
    return LAT_LIMITS[0] <= lat <= LAT_LIMITS[1] and -180 <= lon <= 180


def site_name(name, sid, clean=lambda n: n):
    """A METAR site's name for the map: AWC's, cleaned; its id when AWC has none ("", "Unk", or the id itself)."""
    n = " ".join(str(name or "").split())
    if not n or n.lower() in ("unk", "unknown", sid.lower()):
        return sid
    return clean(n) or sid


def km_between(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * math.asin(min(1.0, math.sqrt(a)))


def assign_aliases(ndbc, coops):
    """ndbc: {ID: {"lat", "lon", "kind", "coops_ref"}}; coops: {id: (lat, lon)} (the kept gauges).
    -> ({coops id: NDBC ID}, [(NDBC ID, coops id, how, metres)]): each NDBC station that IS a kept gauge (its name names
    the gauge, or it lies within ALIAS_M of one: the nearest), at most one alias per gauge (the named one first, then the
    nearest)."""
    cand = []
    for sid, s in ndbc.items():
        ref = s.get("coops_ref")
        if ref in coops:
            la, lo = coops[ref]
            cand.append((0, km_between(s["lat"], s["lon"], la, lo) * 1000, sid, ref, "name"))
            continue
        best = None
        for cid, (la, lo) in coops.items():
            if abs(la - s["lat"]) > 0.01:
                continue
            m = km_between(s["lat"], s["lon"], la, lo) * 1000
            if m <= ALIAS_M and (best is None or m < best[0]):
                best = (m, cid)
        if best:
            cand.append((1, best[0], sid, best[1], "distance"))
    alias, used, log = {}, set(), []
    for rank, m, sid, cid, how in sorted(cand):
        if cid in alias or sid in used:
            continue
        alias[cid] = sid
        used.add(sid)
        log.append((sid, cid, how, round(m)))
    return alias, log


# ------------------------------------------------------------------ the coast

class CoastIndex:
    """Distance to the nearest coastline edge (km), from GSHHG ring edges (point_forecast.decode_coast's (xi, yi, xj, yj)
    in degrees). Edges along the data's cell lines are left out: they close the clipped pieces inside land."""

    def __init__(self, edges, cell_deg=CELL_DEG, bucket_deg=BUCKET_DEG):
        import numpy as np
        xi, yi, xj, yj = (np.asarray(a, dtype=float) for a in edges)

        def on_line(a):
            return np.abs(a - np.round(a / cell_deg) * cell_deg) < 1e-9
        keep = ~(((xi == xj) & on_line(xi)) | ((yi == yj) & on_line(yi)))
        self.xi, self.yi, self.xj, self.yj = xi[keep], yi[keep], xj[keep], yj[keep]
        self.dropped = int((~keep).sum())
        self.b = bucket_deg
        self.nlon = int(round(360 / bucket_deg))
        self.buckets = {}
        r0 = np.floor(np.minimum(self.yi, self.yj) / bucket_deg).astype(int)
        r1 = np.floor(np.maximum(self.yi, self.yj) / bucket_deg).astype(int)
        c0 = np.floor((np.minimum(self.xi, self.xj) + 180) / bucket_deg).astype(int)
        c1 = np.floor((np.maximum(self.xi, self.xj) + 180) / bucket_deg).astype(int)
        for k in range(len(r0)):
            for r in range(r0[k], r1[k] + 1):
                for c in range(c0[k], c1[k] + 1):
                    self.buckets.setdefault((r, c % self.nlon), []).append(k)

    def distance_km(self, lat, lon, limit_km):
        """The nearest edge's distance (km, local flat-earth: exact enough within a few tens of km), or inf when no edge
        lies within limit_km."""
        import numpy as np
        lon = (lon + 180.0) % 360.0 - 180.0
        coslat = max(0.05, math.cos(math.radians(lat)))
        dlat, dlon = limit_km / 110.57, min(180.0, limit_km / (111.32 * coslat))
        ks = set()
        for r in range(int(math.floor((lat - dlat) / self.b)), int(math.floor((lat + dlat) / self.b)) + 1):
            for c in range(int(math.floor((lon - dlon + 180) / self.b)), int(math.floor((lon + dlon + 180) / self.b)) + 1):
                ks.update(self.buckets.get((r, c % self.nlon), ()))
        if not ks:
            return math.inf
        k = np.fromiter(ks, dtype=int)
        kx = 111.32 * coslat
        ax = ((self.xi[k] - lon + 540.0) % 360.0 - 180.0) * kx
        bx = ((self.xj[k] - lon + 540.0) % 360.0 - 180.0) * kx
        ay = (self.yi[k] - lat) * 110.57
        by = (self.yj[k] - lat) * 110.57
        dx, dy = bx - ax, by - ay
        ll = dx * dx + dy * dy
        with np.errstate(divide="ignore", invalid="ignore"):
            t = np.where(ll > 0, np.clip(-(ax * dx + ay * dy) / ll, 0.0, 1.0), 0.0)
        d = float(np.min(np.hypot(ax + t * dx, ay + t * dy)))
        return d if d <= limit_km else math.inf


def in_land(edges, lats, lons):
    """Even-odd point-in-land over the whole tier-0 file (every ring is closed, so rings east of a point add an even
    count): point_forecast.land_parity on lat-sorted chunks. -> list of bools in the input order."""
    import numpy as np
    sys.path.insert(0, ROOT)
    import point_forecast as P
    lats, lons = np.asarray(lats, dtype=float), np.asarray(lons, dtype=float)
    order = np.argsort(lats)
    out = np.zeros(len(lats), dtype=bool)
    for a in range(0, len(order), 64):
        idx = order[a:a + 64]
        out[idx] = P.land_parity(edges, lons[idx], lats[idx])
    return out.tolist()


# ------------------------------------------------------------------ METAR

def parse_metar_cache(raw):
    """metars.cache.csv(.gz) -> {ID: (lat, lon)} for the reports carrying a wind speed."""
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    rd = csv.reader(io.StringIO(raw.decode("utf-8", "replace")))
    head = next(rd, [])
    try:
        i_id, i_lat, i_lon, i_spd = (head.index(c) for c in ("station_id", "latitude", "longitude", "wind_speed_kt"))
    except ValueError:
        raise ValueError("metar cache header")
    out = {}
    for r in rd:
        if len(r) <= max(i_id, i_lat, i_lon, i_spd) or not r[i_spd].strip():
            continue
        try:
            la, lo = float(r[i_lat]), float(r[i_lon])
        except ValueError:
            continue
        if usable_position(la, lo):
            out[r[i_id].strip().upper()] = (la, lo)
    return out


def parse_metar_sites(raw):
    """stations.cache.json(.gz) -> {ID: (name, lat, lon)} for the sites that issue METARs."""
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    out = {}
    for s in json.loads(raw):
        if "METAR" not in (s.get("siteType") or []) or not s.get("id"):
            continue
        if usable_position(s.get("lat"), s.get("lon")):
            out[str(s["id"]).upper()] = (" ".join(str(s.get("site") or "").split()), float(s["lat"]), float(s["lon"]))
    return out


def metar_reporting(ids, fetch=None, pause=METAR_API_PAUSE_S):
    """The ids among `ids` with a wind report in the last 24 hours (api/data/metar, METAR_API_IDS at a time)."""
    fetch = fetch or (lambda url: json.loads(get_bytes(url) or b"[]"))
    ids, seen = sorted(ids), set()
    for a in range(0, len(ids), METAR_API_IDS):
        batch = ids[a:a + METAR_API_IDS]
        try:
            rows = fetch(f"{AWC}/api/data/metar?ids={','.join(batch)}&hours=24&format=json") or []
        except Exception as e:                                  # one lost batch drops its stations, it never stops the build
            print(f"  metar api batch {a}: {e}", file=sys.stderr)
            rows = []
        for r in rows:
            if r.get("wspd") is not None and str(r.get("icaoId") or "").upper() in batch:
                seen.add(str(r["icaoId"]).upper())
        if pause:
            time.sleep(pause)
    return seen


# ------------------------------------------------------------------ the build

def write_doc(path, rows, source):
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write('{"source":%s,"captured":%s,"fields":%s,"stations":[\n' % (
            json.dumps(source), json.dumps(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
            json.dumps(FIELDS)))
        f.write(",\n".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in rows))
        f.write("\n]}\n")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--sensors-cache", default=None, help="JSON file keeping CO-OPS sensors.json answers between runs")
    ap.add_argument("--coast", default=None, help="a local copy of world-i.bin (else it is downloaded)")
    ap.add_argument("--no-metar-api", action="store_true", help="airports: only those in the METAR cache now")
    args = ap.parse_args(argv)

    os.environ.setdefault("LIVE_BACKGROUND", "0")
    sys.path.insert(0, ROOT)
    import app as A                                            # the live buoys' nearest-civil-zone rule
    import point_forecast as P
    TT = tide_tool()                                           # clean_name, POSITION_FIX, the zone check

    # ---- CO-OPS: met stations with an active wind sensor
    met = get_json(MD + "/stations.json?type=met")["stations"]
    cache = {}
    if args.sensors_cache and os.path.exists(args.sensors_cache):
        cache = json.load(open(args.sensors_cache))

    def sensors(sid):
        try:
            d = get_json(f"{MD}/stations/{sid}/sensors.json")
        except Exception:
            return sid, None
        time.sleep(0.1)
        return sid, [[s.get("name"), s.get("status")] for s in (d.get("sensors") or [])]  # null: no sensors listed

    todo = [s["id"] for s in met if cache.get(s["id"]) is None]
    with ThreadPoolExecutor(4) as pool:
        for sid, ss in pool.map(sensors, todo):
            cache[sid] = ss
    if args.sensors_cache:
        json.dump(cache, open(args.sensors_cache, "w"))
    unknown = [s["id"] for s in met if cache.get(s["id"]) is None]
    if unknown:                                                # never a silent gap: a lost answer stops the build
        print(f"CO-OPS sensors unknown for {unknown}: run again", file=sys.stderr)
        return 2
    coops = {}
    for s in met:
        if any(x[0] == "Wind" and x[1] == 1 for x in cache[s["id"]]) and usable_position(s.get("lat"), s.get("lng")):
            lat, lon = TT.POSITION_FIX.get(s["id"], (round(float(s["lat"]), 5), round(float(s["lng"]), 5)))
            coops[s["id"]] = {"name": TT.clean_name(s["name"]), "lat": lat, "lon": lon, "corr": s.get("timezonecorr")}
    print(f"CO-OPS: {len(met)} met stations, {len(coops)} with an active wind sensor", flush=True)

    # ---- NDBC: stations with wind now
    latest = parse_latest_obs(get_bytes(NDBC_LATEST).decode("utf-8", "replace"))
    table = parse_station_table(get_bytes(NDBC_TABLE).decode("utf-8", "replace"))
    ndbc, moving = {}, []
    for sid, (lat, lon) in latest.items():
        t = table.get(sid, {})
        kind = ndbc_kind(sid, t.get("ttype"))
        if kind is None:
            moving.append(sid)
            continue
        ref, rest = coops_id_in_name(t.get("name", ""))
        ndbc[sid] = {"lat": lat, "lon": lon, "kind": kind, "coops_ref": ref,
                     "name": TT.clean_name(" ".join((rest or "").split())) or sid}
    alias, alias_log = assign_aliases(ndbc, {c: (s["lat"], s["lon"]) for c, s in coops.items()})
    aliased = {sid for sid in alias.values()}
    print(f"NDBC: {len(latest)} with wind now, {len(moving)} moving left out {sorted(moving)}, "
          f"{len(aliased)} relays of kept gauges", flush=True)

    # ---- METAR: coastal sites with wind
    coast_raw = open(args.coast, "rb").read() if args.coast else get_bytes(COAST_URL)
    edges = P.decode_coast(coast_raw)
    index = CoastIndex(edges)
    sites = parse_metar_sites(get_bytes(METAR_SITES))
    now = parse_metar_cache(get_bytes(METAR_CACHE))
    cand = {sid: (sites[sid][1], sites[sid][2]) if sid in sites else now[sid] for sid in set(sites) | set(now)}
    dist = {sid: index.distance_km(la, lo, COAST_KM) for sid, (la, lo) in cand.items()}
    far = [sid for sid in cand if dist[sid] > COAST_KM]
    land = dict(zip(far, in_land(edges, [cand[s][0] for s in far], [cand[s][1] for s in far])))
    coastal = {s for s in cand if dist[s] <= COAST_KM}
    offshore = {s for s in far if not land[s]} if OFFSHORE_METAR else set()
    keep_pos = coastal | offshore
    reporting = {s for s in keep_pos if s in now}
    ask = sorted(keep_pos - reporting)
    if ask and not args.no_metar_api:
        print(f"METAR: asking the API about {len(ask)} coastal sites not in the cache", flush=True)
        reporting |= metar_reporting(ask)
    print(f"METAR: {len(cand)} sites, {len(coastal)} within {COAST_KM:g} km of a coast, {len(offshore)} at sea, "
          f"{len(cand) - len(keep_pos)} inland left out; {len(reporting)} report wind "
          f"({len(reporting & set(now))} in the cache now)", flush=True)

    # ---- the rows
    rows = []
    for cid, s in sorted(coops.items()):
        rows.append(["coops:" + cid, s["name"], s["lat"], s["lon"], "gauge", "coops",
                     A._nearest_civil_tz(s["lat"], s["lon"]), alias.get(cid)])
    for sid, s in sorted(ndbc.items()):
        if sid in aliased:
            continue
        lat, lon = round(s["lat"], 4), round(s["lon"], 4)
        rows.append(["ndbc:" + sid, s["name"], lat, lon, s["kind"], "ndbc", A._nearest_civil_tz(lat, lon), None])
    for sid in sorted(reporting):
        name, lat, lon = sites.get(sid, (sid, cand[sid][0], cand[sid][1]))
        lat, lon = round(lat, 4), round(lon, 4)
        rows.append(["metar:" + sid, site_name(name, sid, TT.clean_name), lat, lon, "airport", "metar",
                     A._nearest_civil_tz(lat, lon), None])

    bad = TT.zone_mismatches([[r[0][6:], r[1], r[2], r[3], r[4], r[5], r[6]] for r in rows if r[5] == "coops"],
                             {c: s["corr"] for c, s in coops.items()})
    if bad:
        print("ZONE CHECK FAILED (CO-OPS: derived zone vs NOAA's timezonecorr, > %d h):" % TT.ZONE_TOL_H, file=sys.stderr)
        for b in bad:
            print("  %s %s: %s vs %s" % b, file=sys.stderr)
        return 2
    write_doc(args.out, rows, "NOAA CO-OPS (tidesandcurrents.noaa.gov), NOAA NDBC (ndbc.noaa.gov) and NWS Aviation "
              "Weather Center (aviationweather.gov) station lists, public domain; coastlines GSHHG 2.3.7")
    by = {}
    for r in rows:
        by[r[4]] = by.get(r[4], 0) + 1
    print(f"wrote {args.out}: {len(rows)} stations {by}; aliases {len(alias_log)}: "
          f"{sum(1 for x in alias_log if x[2] == 'name')} by name, {sum(1 for x in alias_log if x[2] == 'distance')} "
          f"by distance {[x for x in alias_log if x[2] == 'distance']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
