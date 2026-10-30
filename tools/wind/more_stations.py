"""Add plan section 67's wind stations to wind_stations.json: Environment Canada (SWOB-ML), Japan (JMA AMeDAS), US road
weather and airports (Iowa Environmental Mesonet) and six European agencies (SMHI, FMI, DMI, DWD, IPMA, Met Eireann).

The rows of tools/wind/fetch_stations.py are kept as they are; this pass replaces only its own sources' rows. A station
is added when (owner, 2026-10-30):
- it reports wind now (each agency's latest answer; ECCC and DWD files read once per station, kept in --cache 12 hours);
- it lies within COAST_KM of a coastline (the site's GSHHG tier-0 file) or of a large lake's shore (the lake-outline set,
  lakes >= 100 km2: the Great Lakes, Winnipeg, Ladoga, Biwa ...), or at sea;
- no station is already kept at its spot: within SPOT_M of any kept station, within AIRPORT_SPOT_M of a METAR site (an
  airport's own weather station is the airport), the METAR id itself (ECCC's NAV CANADA files, IEM's ASOS sites, DWD's
  ICAO; also an NDBC id: NDBC relays some Great Lakes airports as KPxx), a national weather service's station on an
  airfield (within AIRFIELD_M of an airfield site, or within AIRFIELD_NAMED_M when its name says airport or begins like
  the site's; airfield sites = the METAR sites and the new rows that are an airport's own station: ECCC's NAV CANADA
  CYxx ids and stations named like an airport, placed first so the airport's station stands for the field: G44 P2-3),
  and for DWD (coarse 0.01-degree positions) within DWD_NEAR_M of a kept station. Order: SPOT_RANK_MORE (the national
  services, then DWD, then IEM), airfield sites first, id.

Zones: the nearest civil zone (app._nearest_civil_tz), except FIXED_TZ for the agencies whose stations all keep one
country's clock (a coastal or offshore point can fall nearer another country's zone: G44 P3-1).

Kinds: "met" (a national weather service's station), "road" (a state DOT road-weather station, IEM XX_RWIS), "airport"
(IEM XX_ASOS: the non-federal AWOS sites without a METAR). `net` = the IEM network (the feed asks per network).

Run:  python tools/wind/more_stations.py --cache <dir>   (reads and rewrites ../../wind_stations.json)"""
import argparse
import csv
import io
import json
import math
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)
import fetch_stations as FS                                   # noqa: E402  (the coast index, the writer, the helpers)

OUT = FS.OUT
LAKES_URL = "https://models.allshoresurf.com/static/coast/lakeoutlines-v1/lakeoutlines-i.bin"
COAST_KM = FS.COAST_KM
SPOT_M = FS.SPOT_M
AIRPORT_SPOT_M = FS.AIRPORT_SPOT_M
DWD_NEAR_M = 2000.0
AIRFIELD_M = 1500.0                                            # a weather service's station this near a METAR site is
AIRFIELD_NAMED_M = 2500.0                                      # the airfield's (as is one up to 2.5 km whose name says so)
AIRFIELD_WORDS = re.compile(r"\b(airport|aeroport|aeroporto|aerodromo|aerodrome|flygplats|lufthavn|flyvestation|"
                            r"lentoasema|lentokentta|flughafen|flugplatz|airfield|intl|arpt)\b")
DWD_WMO_BLOCK = "10"                                           # DWD's own stations: WMO block 10 = Germany (its
                                                               # poi list also relays other countries' SYNOP reports)
SPOT_RANK_MORE = {"eccc": 4, "jma": 4, "smhi": 4, "fmi": 4, "dmi": 4, "ipma": 4, "mei": 4, "dwd": 5, "iem": 6}
FIXED_TZ = {"jma": "Asia/Tokyo", "fmi": "Europe/Helsinki", "smhi": "Europe/Stockholm", "dwd": "Europe/Berlin",
            "mei": "Europe/Dublin"}
CACHE_S = 12 * 3600
PAUSE_S = 0.25                                                 # between one-station reads (ECCC, DWD, Met Eireann)
IEM_PAUSE_S = 1.0
# IEM networks asked: the US coastal and Great Lakes states (+ Puerto Rico) road weather and airport networks
IEM_STATES = ["AK", "AL", "CA", "CT", "DE", "FL", "GA", "HI", "IL", "IN", "LA", "MA", "MD", "ME", "MI", "MN", "MS", "NC",
              "NH", "NJ", "NY", "OH", "OR", "PA", "PR", "RI", "SC", "TX", "VA", "WA", "WI"]
IEM_KINDS = {"RWIS": "road", "ASOS": "airport"}


def get(url, **kw):
    return FS.get_bytes(url, **kw)


class Cache:
    """A JSON file of {key: [epoch, value]} kept between runs (one-station answers)."""

    def __init__(self, path):
        self.path = path
        self.d = json.load(open(path, encoding="utf-8")) if path and os.path.exists(path) else {}

    def get(self, key):
        e = self.d.get(key)
        return e[1] if e and time.time() - e[0] < CACHE_S else None

    def put(self, key, value):
        self.d[key] = [time.time(), value]

    def save(self):
        if self.path:
            json.dump(self.d, open(self.path, "w", encoding="utf-8"))


def place_name(name, TT):
    n = " ".join(str(name or "").split())
    if n and n == n.upper():
        n = FS._fix_case(TT.clean_name(n), places=True)
    return n


# ---------------------------------------------------------------------------------------------- the sources

def eccc(M, cache, coastal, TT):
    listing = M.parse_swob_listing(get(M.ECCC_LATEST).decode("utf-8", "replace"))
    sites = M.parse_eccc_station_list(get(M.ECCC_STATIONS).decode("utf-8", "replace"))
    cand = {sid: s for sid, s in sites.items() if sid in listing}
    near = coastal({sid: (s["lat"], s["lon"]) for sid, s in cand.items()})
    out, nowind = [], []
    for sid in sorted(near):
        key = "eccc:" + sid
        r = cache.get(key)
        if r is None:
            f, _when = M.swob_pick(listing[sid])
            try:
                _sid, _la, _lo, rd = M.parse_swob(get(M.ECCC_LATEST + f, timeout=30))
            except Exception as e:                           # noqa: BLE001
                print(f"  ECCC {sid}: {e}", flush=True)
                continue
            r = bool(rd and rd.get("s") is not None)
            cache.put(key, r)
            time.sleep(PAUSE_S)
        if not r:
            nowind.append(sid)
            continue
        s = cand[sid]
        out.append({"src": "eccc", "local": sid, "name": place_name(s["name"], TT), "lat": round(s["lat"], 4),
                    "lon": round(s["lon"], 4), "kind": "met", "net": None, "icao": sid})
    print(f"ECCC: {len(listing)} stations in the latest folder, {len(cand)} with a position, {len(near)} coastal, "
          f"{len(nowind)} without a wind reading, {len(out)} kept before the spot rule", flush=True)
    return out


def jma(M, cache, coastal, TT):
    t = M.jma_time(get(M.JMA_LATEST_TIME).decode().strip())
    now = M.parse_jma_map(get(M.JMA_MAP.format(stamp=M.jma_map_name(t))), t)
    table = M.parse_jma_table(get(M.JMA_TABLE))
    cand = {sid: table[sid] for sid in now if sid in table}
    near = coastal({sid: (s["lat"], s["lon"]) for sid, s in cand.items()})
    out = [{"src": "jma", "local": sid, "name": cand[sid]["name"], "lat": cand[sid]["lat"], "lon": cand[sid]["lon"],
            "kind": "met", "net": None, "icao": None} for sid in sorted(near)]
    print(f"JMA: {len(table)} AMeDAS stations, {len(cand)} with wind now, {len(near)} coastal", flush=True)
    return out


def iem(M, cache, coastal, TT):
    nets = {n["id"] for n in json.loads(get(M.IEM_NETWORKS))["data"]}
    ask = [f"{st}_{k}" for st in IEM_STATES for k in IEM_KINDS if f"{st}_{k}" in nets]
    pos, meta = {}, {}
    for net in ask:
        try:
            cur = M.parse_iem_currents(get(M.IEM_CURRENTS.format(net=net), timeout=60))
        except Exception as e:                               # noqa: BLE001
            print(f"  IEM {net}: {e}", flush=True)
            continue
        for sid, (r, e) in cur.items():
            if not r or e.get("lat") is None or e.get("lon") is None or sid in meta:
                continue
            pos[sid] = (float(e["lat"]), float(e["lon"]))
            meta[sid] = (net, e.get("name") or sid)
        time.sleep(IEM_PAUSE_S)
    near = coastal(pos)
    out = [{"src": "iem", "local": sid, "name": place_name(meta[sid][1], TT), "lat": round(pos[sid][0], 4),
            "lon": round(pos[sid][1], 4), "kind": IEM_KINDS[meta[sid][0].split("_")[1]], "net": meta[sid][0],
            "icao": sid} for sid in sorted(near)]
    print(f"IEM: {len(ask)} networks asked, {len(pos)} stations with wind now, {len(near)} coastal", flush=True)
    return out


def smhi(M, cache, coastal, TT):
    now = M.parse_smhi_param(get(M.SMHI_LATEST.format(p=4)))
    st = M.parse_smhi_stations(get(M.SMHI_STATIONS))
    cand = {k: st[k] for k in now if k in st}
    near = coastal({k: (s["lat"], s["lon"]) for k, s in cand.items()})
    out = [{"src": "smhi", "local": k, "name": cand[k]["name"], "lat": round(cand[k]["lat"], 4),
            "lon": round(cand[k]["lon"], 4), "kind": "met", "net": None, "icao": None} for k in sorted(near)]
    print(f"SMHI: {len(cand)} stations with wind now, {len(near)} coastal", flush=True)
    return out


def fmi(M, cache, coastal, TT):
    start = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:00Z")
    now = M.parse_fmi_obs(get(M.FMI_WFS + f"&bbox={M.FMI_BBOX}&starttime={start}", timeout=90))
    st = M.parse_fmi_stations(get(M.FMI_STATIONS, timeout=90))
    cand = {k: st[k] for k in now if k in st}
    near = coastal({k: (s["lat"], s["lon"]) for k, s in cand.items()})
    out = [{"src": "fmi", "local": k, "name": cand[k]["name"], "lat": round(cand[k]["lat"], 4),
            "lon": round(cand[k]["lon"], 4), "kind": "met", "net": None, "icao": None} for k in sorted(near)]
    print(f"FMI: {len(st)} stations, {len(cand)} with wind now, {len(near)} coastal", flush=True)
    return out


def dmi(M, cache, coastal, TT):
    items = M.parse_dmi_items(get(M.DMI_ITEMS.format(p="wind_speed")))
    st = M.parse_dmi_stations(get(M.DMI_STATIONS, timeout=90))
    pos = {sid: (rows[-1][3], rows[-1][2]) for sid, rows in items.items() if rows[-1][2] is not None}
    near = coastal(pos)
    out = [{"src": "dmi", "local": sid, "name": (st.get(sid) or {}).get("name") or sid, "lat": round(pos[sid][0], 4),
            "lon": round(pos[sid][1], 4), "kind": "met", "net": None, "icao": None} for sid in sorted(near)]
    print(f"DMI: {len(pos)} stations with wind now, {len(near)} coastal", flush=True)
    return out


def dwd(M, cache, coastal, TT):
    ids = [sid for sid in M.parse_dwd_listing(get(M.DWD_POI).decode("utf-8", "replace"))
           if sid.startswith(DWD_WMO_BLOCK)]
    cat = M.parse_dwd_catalog(get(M.DWD_CATALOG).decode("latin-1", "replace"))
    cand = {sid: cat[sid] for sid in ids if sid in cat}
    near = coastal({sid: (s["lat"], s["lon"]) for sid, s in cand.items()})
    out, nowind = [], []
    for sid in sorted(near):
        key = "dwd:" + sid
        r = cache.get(key)
        if r is None:
            try:
                rows = M.parse_dwd_poi(get(M.DWD_CSV.format(sid=sid), timeout=30))
            except Exception as e:                           # noqa: BLE001
                print(f"  DWD {sid}: {e}", flush=True)
                continue
            r = bool(rows and time.time() - rows[-1]["t"] < 6 * 3600)
            cache.put(key, r)
            time.sleep(PAUSE_S)
        if not r:
            nowind.append(sid)
            continue
        s = cand[sid]
        out.append({"src": "dwd", "local": sid, "name": place_name(s["name"], TT), "lat": s["lat"], "lon": s["lon"],
                    "kind": "met", "net": None, "icao": s["icao"]})
    print(f"DWD: {len(ids)} station files, {len(cand)} in the catalogue, {len(near)} coastal, {len(nowind)} without a "
          f"recent wind reading", flush=True)
    return out


def ipma(M, cache, coastal, TT):
    now = M.parse_ipma_obs(get(M.IPMA_OBS))
    st = M.parse_ipma_stations(get(M.IPMA_STATIONS))
    cand = {k: st[k] for k in now if k in st}
    near = coastal({k: (s["lat"], s["lon"]) for k, s in cand.items()})
    out = [{"src": "ipma", "local": k, "name": cand[k]["name"], "lat": round(cand[k]["lat"], 4),
            "lon": round(cand[k]["lon"], 4), "kind": "met", "net": None, "icao": None} for k in sorted(near)]
    print(f"IPMA: {len(cand)} stations with wind now, {len(near)} coastal", flush=True)
    return out


def mei(M, cache, coastal, TT):
    names = [r["Station"].strip() for r in csv.DictReader(io.StringIO(get(M.MEI_LIST).decode("utf-8", "replace")))]
    known = {n: M.MEI_POSITIONS[n] for n in names if n in M.MEI_POSITIONS}
    near = coastal(known)
    out, nodata = [], []
    for n in sorted(near):
        slug = M.mei_slug(n)
        key = "mei:" + slug
        r = cache.get(key)
        if r is None:
            try:
                r = bool(M.parse_mei_today(get(M.MEI_TODAY.format(slug=slug), timeout=30)))
            except Exception:                                # noqa: BLE001
                r = False
            cache.put(key, r)
            time.sleep(PAUSE_S)
        if not r:
            nodata.append(n)
            continue
        out.append({"src": "mei", "local": slug.upper(), "name": n, "lat": known[n][0], "lon": known[n][1],
                    "kind": "met", "net": None, "icao": None})
    print(f"Met Eireann: {len(names)} stations listed, {len(known)} with a known position, {len(near)} coastal, "
          f"no answer from {nodata}", flush=True)
    return out


GATHER = {"eccc": eccc, "jma": jma, "iem": iem, "smhi": smhi, "fmi": fmi, "dmi": dmi, "dwd": dwd, "ipma": ipma,
          "mei": mei}


# ---------------------------------------------------------------------------------------------- the spot rule

class Spots:
    """The kept stations on a 0.1-degree grid (searched one cell around: >= 2.3 km at the map's 84 N)."""

    def __init__(self, rows):
        self.grid = {}
        self.metar_ids, self.ndbc_ids, self.fields = set(), set(), set()
        for r in rows:
            self.add(r)

    def add(self, r, field=False):
        self.grid.setdefault((math.floor(r[2] * 10), math.floor(r[3] * 10)), []).append(r)
        if r[5] == "metar":
            self.metar_ids.add(r[0][6:])
        elif r[5] == "ndbc":
            self.ndbc_ids.add(r[0][5:])
        if r[5] == "metar" or field:
            self.fields.add(r[0])

    def near(self, lat, lon, metres, only_src=None, fields=False):
        return next(iter(self.near_all(lat, lon, metres, only_src, fields)), None)

    def near_all(self, lat, lon, metres, only_src=None, fields=False):
        out = []
        cy, cx = math.floor(lat * 10), math.floor(lon * 10)
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                for k in self.grid.get((cy + dy, (cx + dx + 1800) % 3600 - 1800), ()):
                    if only_src and k[5] != only_src:
                        continue
                    if fields and k[0] not in self.fields:
                        continue
                    if FS.km_between(lat, lon, k[2], k[3]) * 1000 <= metres:
                        out.append(k)
        out.sort(key=lambda k: FS.km_between(lat, lon, k[2], k[3]))
        return out


FOLD_LETTERS = str.maketrans({"ø": "o", "Ø": "O", "æ": "ae", "Æ": "AE", "ß": "ss", "ð": "d", "Ð": "D", "þ": "th",
                              "Þ": "Th", "ł": "l", "Ł": "L", "œ": "oe", "Œ": "OE"})   # letters NFKD does not split


def fold(name):
    """A name for comparing: no accents, lower case, words of letters and digits."""
    n = str(name or "").translate(FOLD_LETTERS)
    n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode("ascii").lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", n).split())


def is_field(c):
    """A new row that is an airport's own station (an airfield site for the rows after it): ECCC's NAV CANADA ids
    (CYxx), or a national weather service's station named like an airport."""
    if c["kind"] != "met":
        return False
    return (c["src"] == "eccc" and str(c["local"]).upper().startswith("CY")) or bool(AIRFIELD_WORDS.search(fold(c["name"])))


def field_rank(c):
    """Placing order inside a source: an airport's NAV CANADA station first, then stations named like an airport,
    then the rest (the airport's own station stands for its field)."""
    if c["kind"] == "met" and c["src"] == "eccc" and str(c["local"]).upper().startswith("CY"):
        return 0
    return 1 if is_field(c) else 2


def airfield(c, spots):
    """The airfield site (a METAR site, or a kept airport station: is_field) whose airfield a national weather
    service's station stands on: within AIRFIELD_M, or within AIRFIELD_NAMED_M when the station's name says airport or
    begins like the site's name. None for road weather stations (IEM's airports are matched by their METAR id)."""
    if c["kind"] != "met":
        return None
    near = spots.near(c["lat"], c["lon"], AIRFIELD_M, fields=True)
    if near:
        return near
    name = fold(c["name"])
    first = name.split(" ")[0] if name else ""
    for k in spots.near_all(c["lat"], c["lon"], AIRFIELD_NAMED_M, fields=True):
        if AIRFIELD_WORDS.search(name) or (first and fold(k[1]).split(" ")[0] == first):
            return k
    return None


def twin_ids(c):
    ids = set()
    icao = (c.get("icao") or "").upper()
    if icao:
        ids |= {icao}
        if c["src"] == "iem" and len(icao) == 3:
            ids |= {"K" + icao, "P" + icao, "T" + icao}
    return sorted(ids)


def metar_twin(c, spots):
    return next((i for i in twin_ids(c) if i in spots.metar_ids), None)


def ndbc_twin(c, spots):
    """NDBC's id of the same site (NDBC relays some Great Lakes airports' reports as KPxx: G44 P3-2)."""
    return next((i for i in twin_ids(c) if i in spots.ndbc_ids), None)


def place(cands, rows):
    """-> (the new rows' candidates kept, [(dropped id, why, kept id)])."""
    spots = Spots(rows)
    order = sorted(cands, key=lambda c: (SPOT_RANK_MORE[c["src"]], c["src"], field_rank(c), c["local"]))
    kept, dropped, seen = [], [], set()
    for c in order:
        cid = f"{c['src']}:{c['local']}"
        if cid in seen:
            dropped.append((cid, "duplicate id", cid))
            continue
        twin = metar_twin(c, spots)
        if twin:
            dropped.append((cid, "METAR id", "metar:" + twin))
            continue
        twin = ndbc_twin(c, spots)
        if twin:
            dropped.append((cid, "NDBC id", "ndbc:" + twin))
            continue
        k = airfield(c, spots)
        if k:
            dropped.append((cid, "airfield", k[0]))
            continue
        k = spots.near(c["lat"], c["lon"], AIRPORT_SPOT_M, only_src="metar")
        k = k or spots.near(c["lat"], c["lon"], DWD_NEAR_M if c["src"] == "dwd" else SPOT_M)
        if k:
            dropped.append((cid, "spot", k[0]))
            continue
        seen.add(cid)
        row = [cid, c["name"], c["lat"], c["lon"], c["kind"], c["src"], None, None, None, c["net"]]
        spots.add(row, field=is_field(c))
        kept.append(row)
    return kept, dropped


# ---------------------------------------------------------------------------------------------- the coast rule

def make_coastal(sea_edges, lake_edges, counts):
    """-> coastal({id: (lat, lon)}) = the ids within COAST_KM of a sea coast (sea_edges: the world tier-0 coast) or of a
    large lake's shore (lake_edges: the lake-outline set), or at sea; counts {sea, lake, offshore, inland} add up."""
    sea, lake = FS.CoastIndex(sea_edges), FS.CoastIndex(lake_edges)

    def coastal(pos):
        keep, far = set(), []
        for k, (la, lo) in pos.items():
            if not FS.usable_position(la, lo):
                continue
            if sea.distance_km(la, lo, COAST_KM) <= COAST_KM:
                keep.add(k)
                counts["sea"] += 1
            elif lake.distance_km(la, lo, COAST_KM) <= COAST_KM:
                keep.add(k)
                counts["lake"] += 1
            else:
                far.append(k)
        if far:
            land = FS.in_land(sea_edges, [pos[k][0] for k in far], [pos[k][1] for k in far])
            for k, on_land in zip(far, land):
                if on_land:
                    counts["inland"] += 1
                else:
                    keep.add(k)
                    counts["offshore"] += 1
        return keep
    return coastal


def respot(out_path, doc, rows, only):
    """The snapshot's rows of the sources `only` placed again by the spot rule (positions, names kept; zones kept
    except FIXED_TZ's)."""
    cands, old = [], {}
    for r in doc["stations"]:
        r = list(r) + [None] * (len(FS.FIELDS) - len(r))
        if r[5] not in only:
            continue
        src, _, local = r[0].partition(":")
        old[r[0]] = r
        cands.append({"src": src, "local": local, "name": r[1], "lat": r[2], "lon": r[3], "kind": r[4], "net": r[9],
                      "icao": local if src in ("eccc", "iem") else None})
    new, dropped = place(cands, rows)
    for r in new:
        r[6] = FIXED_TZ.get(r[5]) or old[r[0]][6]
    print(f"respot: {len(dropped)} left out {dropped}", flush=True)
    FS.write_doc(out_path, rows + sorted(new, key=lambda r: r[0]), doc["source"], captured=doc.get("captured"))
    print(f"wrote {out_path}: {len(rows) + len(new)} stations", flush=True)
    return 0


# ---------------------------------------------------------------------------------------------- the build

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--cache", default=None, help="a directory keeping the one-station answers between runs")
    ap.add_argument("--coast", default=None, help="a local copy of world-i.bin")
    ap.add_argument("--lakes", default=None, help="a local copy of lakeoutlines-i.bin")
    ap.add_argument("--only", default=",".join(GATHER), help="sources to (re)build, comma-separated")
    ap.add_argument("--respot", action="store_true", help="no network: place the snapshot's own rows of --only again "
                    "(after a change to the spot rule), so nothing comes or goes with the agencies' current answers")
    args = ap.parse_args(argv)
    os.environ.setdefault("LIVE_BACKGROUND", "0")
    import app as A
    import point_forecast as P
    import wind_more as M
    TT = FS.tide_tool()

    with open(args.out, encoding="utf-8") as f:
        doc = json.load(f)
    assert doc["fields"] == FS.FIELDS[:len(doc["fields"])], doc["fields"]
    only = [s.strip() for s in args.only.split(",") if s.strip()]
    rows = [list(r) + [None] * (len(FS.FIELDS) - len(r)) for r in doc["stations"] if r[5] not in only]
    print(f"kept {len(rows)} rows of {len(doc['stations'])}; building {only}", flush=True)
    if args.respot:
        return respot(args.out, doc, rows, only)

    sea_raw = open(args.coast, "rb").read() if args.coast else get(FS.COAST_URL)
    lake_raw = open(args.lakes, "rb").read() if args.lakes else get(LAKES_URL)
    sea_edges, lake_edges = P.decode_coast(sea_raw), P.decode_coast(lake_raw)
    counts = {"sea": 0, "lake": 0, "offshore": 0, "inland": 0}
    coastal = make_coastal(sea_edges, lake_edges, counts)

    cache = Cache(os.path.join(args.cache, "more_stations_cache.json") if args.cache else None)
    cands, failed = [], []
    for src in only:
        try:
            cands += GATHER[src](M, cache, coastal, TT)
        except Exception as e:                               # noqa: BLE001
            failed.append((src, repr(e)))
            print(f"{src}: FAILED {e!r}", file=sys.stderr, flush=True)
        cache.save()
    if failed:
        print(f"sources failed {failed}: nothing written", file=sys.stderr)
        return 2
    new, dropped = place(cands, rows)
    for r in new:
        r[6] = FIXED_TZ.get(r[5]) or A._nearest_civil_tz(r[2], r[3])
    why = {}
    for _cid, w, _k in dropped:
        why[w] = why.get(w, 0) + 1
    print(f"coast rule {counts}; spot rule: {len(dropped)} left out {why}: {dropped[:40]}", flush=True)
    by = {}
    for r in new:
        by[r[5]] = by.get(r[5], 0) + 1
    out = rows + sorted(new, key=lambda r: r[0])
    source = doc["source"]
    extra = ("; plan section 67: ECCC SWOB-ML, JMA AMeDAS, Iowa Environmental Mesonet, SMHI, FMI, DMI, DWD, IPMA and "
             "Met Eireann station lists; lake shores GSHHG 2.3.7 level 2")
    if extra not in source:
        source += extra
    FS.write_doc(args.out, out, source)
    print(f"wrote {args.out}: {len(out)} stations, added {len(new)} {by}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
