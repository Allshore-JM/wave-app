"""Multi-source live-buoy providers for the worldwide "Live buoys" map layer.

Each provider yields a lean STATION LIST (namespaced ids + lat/lon + capabilities +
attribution) for map markers, and a per-station LATEST observation (bulk wave params).
Heavier per-buoy detail (24h summary, directional spectra) is added per source where
supported. The NDBC provider lives in app.py (it wraps the existing NDBC parsing); the
ERDDAP-based providers here are self-contained (no app.py import -> no circular import).

Conventions (normalize everything here so the render path stays simple):
  - heights in METERS, periods in SECONDS, directions in DEGREES, timestamps ISO-8601 UTC ("...Z").
  - wave direction is the "coming FROM" convention (matches NDBC).
Resilience: every network call has a timeout and fails soft -- a down/slow source contributes no
markers and never raises out of list_stations()/latest().
"""
import csv
import io
import logging
import math
import re
import time
import threading
from datetime import datetime, timedelta, timezone

import requests

_log = logging.getLogger(__name__)


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(min(1.0, math.sqrt(a)))


# Static capability template; providers override the True/False per their data.
def _caps(bulk=False, recent_history=False, directional=False, spectra=False, partitions=False):
    return {"bulk": bulk, "recent_history": recent_history, "directional": directional,
            "spectra": spectra, "partitions": partitions}


class BuoyProvider:
    """Base class. Subclasses set the metadata + capabilities and implement the fetches."""
    source = "?"
    source_name = ""
    source_url = ""
    license_label = ""
    attribution_text = ""
    stale_after_sec = 12 * 3600          # obs older than this -> is_stale
    capabilities = _caps()
    list_ttl_sec = 3600                  # station-list cache TTL
    timeout = 30

    def __init__(self, http=None):
        self.http = http or requests.Session()
        self._lock = threading.Lock()
        self._list_cache = None
        self._list_ts = 0.0
        # Monotonic publish counter: every (re)fetch -- successful, empty, or failed -- bumps it,
        # so a snapshot's (list, version) pair identifies exactly which publish it came from.
        self._list_version = 0
        # Singleflight for the refresh itself: concurrent callers that find the list expired
        # wait here and then return the ONE freshly published list instead of each fetching.
        self._refresh_lock = threading.Lock()

    # --- interface ---
    def _fetch_stations(self):
        """Return raw [{local_id, name, lat, lon}] (no namespacing). Override."""
        raise NotImplementedError

    def latest(self, local_id):
        """Return latest bulk obs (SI) {time_utc, hs_m, tp_s, dir_deg} or None. Override."""
        raise NotImplementedError

    # --- shared ---
    def list_stations(self):
        """Cached, fail-soft lean station list with namespaced ids + capabilities + attribution."""
        return self.list_stations_versioned()[0]

    def _snapshot_if_fresh(self):
        """(list, version, published_ts) under the lock, or None when expired/never fetched."""
        with self._lock:
            if self._list_cache is not None and (time.time() - self._list_ts) < self.list_ttl_sec:
                return self._list_cache, self._list_version, self._list_ts
        return None

    def list_stations_versioned(self):
        """Same as list_stations() but returns (list, version, published_ts) read together under
        one lock, so a caller can key derived work on the exact publish it saw. Refresh timing
        is unchanged: the first call after list_ttl_sec fetches inline; concurrent expired
        callers wait for that fetch and get its result (the same list they would have obtained
        from their own duplicate fetch today, minus the duplicate upstream hit)."""
        snap = self._snapshot_if_fresh()
        if snap is not None:
            return snap
        with self._refresh_lock:
            snap = self._snapshot_if_fresh()        # published while we waited?
            if snap is not None:
                return snap
            out = self._build_station_list()
            with self._lock:
                self._list_cache = out
                self._list_ts = time.time()
                self._list_version += 1
                return out, self._list_version, self._list_ts

    def _build_station_list(self):
        """One upstream fetch -> lean entries. Fail-soft: any error -> [] (no markers rather
        than a broken map)."""
        out = []
        now = time.time()
        try:
            for s in self._fetch_stations():
                lat, lon = s.get("lat"), s.get("lon")
                if lat is None or lon is None:
                    continue
                entry = {
                    "id": "%s:%s" % (self.source.lower(), s["local_id"]),
                    "source": self.source,
                    "source_name": self.source_name,
                    "source_url": self.source_url,
                    "license_label": self.license_label,
                    "attribution_text": self.attribution_text,
                    "name": s.get("name") or s["local_id"],
                    "lat": float(lat),
                    "lon": float(lon),
                    # a provider may set per-station capabilities (e.g. only some AODN
                    # buoys publish spectra); otherwise use the provider default.
                    "capabilities": dict(s.get("capabilities") or self.capabilities),
                    "is_stale": False,
                    "dup_of": None,
                }
                # Providers that fetch the latest obs at list-build time pass 'latest_time';
                # mark the marker stale if that obs is older than stale_after_sec.
                lt = s.get("latest_time")
                if lt:
                    entry["latest_time"] = lt
                    ep = _z_epoch({"time_utc": lt})
                    if ep is not None and (now - ep) > self.stale_after_sec:
                        entry["is_stale"] = True
                out.append(entry)
        except Exception as e:
            _log.warning("buoy provider %s: station-list fetch failed (%s)", self.source, e)
            out = []                          # fail soft: no markers rather than a broken map
        return out

    def detail(self, local_id):
        """Return {latest, recent}. Base: latest obs only (no history). Sources with
        recent_history override this to also return a recent obs list (newest-first)."""
        return {"latest": self.latest(local_id), "recent": []}


def _erddap_rows(http, url, timeout):
    """GET an ERDDAP .csv and return (header, [data rows]) using a real CSV parser
    (ERDDAP quotes commas inside fields, e.g. station names)."""
    r = http.get(url, timeout=timeout)
    if r.status_code != 200:
        return [], []
    rows = list(csv.reader(io.StringIO(r.text)))
    if len(rows) < 3:
        return (rows[0] if rows else []), []
    return rows[0], rows[2:]                   # rows[1] is the units row


class CDIPProvider(BuoyProvider):
    """CDIP / Scripps -- open ERDDAP wave_agg. Bulk Hs/Tp/Dp here (Phase 1a);
    directional spectra are a later phase. waveDp is the peak direction (deg, coming-from)."""
    source = "CDIP"
    source_name = "CDIP / Scripps"
    source_url = "https://cdip.ucsd.edu"
    license_label = "Public domain (USACE)"
    attribution_text = "Source: CDIP, Scripps Institution of Oceanography, UC San Diego"
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, directional=True)   # spectra/partitions wired in Phase 1b
    BASE = "https://erddap.cdip.ucsd.edu/erddap/tabledap/wave_agg"

    def _fetch_stations(self):
        # ACTIVE subset only: stations with data in the last 2 days (the live buoys). The
        # full wave_agg also contains many decommissioned stations; the time constraint
        # filters them out and is fast (~0.7s). csv-parsed so quoted names are safe.
        hdr, rows = _erddap_rows(
            self.http,
            self.BASE + ".csv?station_id,metaStationName,latitude,longitude"
                        "&time%3E=now-2days&distinct()",
            self.timeout)
        if not hdr:
            return []
        ix = {c: i for i, c in enumerate(hdr)}
        out = []
        for row in rows:
            try:
                out.append({
                    "local_id": row[ix["station_id"]],
                    "name": "CDIP %s%s" % (
                        row[ix["station_id"]],
                        (" - " + row[ix["metaStationName"]]) if row[ix["metaStationName"]] else ""),
                    "lat": float(row[ix["latitude"]]),
                    "lon": float(row[ix["longitude"]]),
                })
            except (KeyError, ValueError, IndexError):
                continue
        return out

    def latest(self, local_id):
        # Time-constrained single-station orderByMax -> light + reliable (404 if no such station).
        url = (self.BASE + ".csv?station_id,time,waveHs,waveTp,waveDp"
               "&station_id=%22" + str(local_id) + "%22&time%3E=now-3days&orderByMax(%22time%22)")
        hdr, rows = _erddap_rows(self.http, url, self.timeout)
        if not hdr or not rows:
            return None
        ix = {c: i for i, c in enumerate(hdr)}
        try:
            r0 = rows[0]
            def num(k):
                v = r0[ix[k]]
                return float(v) if v not in ("", "NaN") else None
            return {
                "time_utc": r0[ix["time"]],
                "hs_m": num("waveHs"),
                "tp_s": num("waveTp"),
                "dir_deg": num("waveDp"),
                "dir_kind": "from",
            }
        except (KeyError, ValueError, IndexError):
            return None


def _wfs_rows(http, url, timeout, headers=None):
    """GET a GeoServer WFS .csv. Unlike ERDDAP there is NO units row -> rows[1:] are data."""
    r = http.get(url, timeout=timeout, headers=headers)
    if r.status_code != 200 or r.text.lstrip().startswith("<"):   # XML ServiceException -> treat as empty
        return [], []
    rows = list(csv.reader(io.StringIO(r.text)))
    if len(rows) < 2:
        return (rows[0] if rows else []), []
    return rows[0], rows[1:]


def _z(t):
    """Normalize a naive ISO timestamp (already UTC) to a trailing-Z form."""
    if not t:
        return None
    return t if t.endswith("Z") else t + "Z"


def _iso_to_z(dt):
    """Convert an offset-aware ISO string (e.g. '...+10:00') to UTC '...Z'."""
    if not dt:
        return None
    try:
        d = datetime.fromisoformat(dt)
        if d.tzinfo is not None:
            d = d.astimezone(timezone.utc)
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return dt


def _unix_to_z(ts):
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError, OSError):
        return None


def _parse_naive(t):
    try:
        return datetime.strptime(t, "%Y-%m-%dT%H:%M:%S")
    except (TypeError, ValueError):
        return None


def _opendap_array(http, base_url, projection, timeout, n=None):
    """GET one OPeNDAP .ascii projection (e.g. 'ENERGY.ENERGY[669:1:669][0:1:38]') and
    return its trailing numeric values. Grid members are addressed as VAR.VAR to get the
    bare data array (no coordinate maps). Returns the last n values when n is given."""
    r = http.get(base_url + ".ascii?" + projection, timeout=timeout)
    if r.status_code != 200:
        return []
    body = r.text.split("\n", 1)[1] if "\n" in r.text else r.text
    body = re.sub(r"\[\d+\]", "", body)              # strip [index] markers
    vals = []
    for tok in re.findall(r"-?\d+\.?\d*(?:[eE][-+]?\d+)?", body):
        try:
            vals.append(float(tok))
        except ValueError:
            pass
    return vals[-n:] if (n and len(vals) >= n) else vals


def _z_epoch(o):
    try:
        return datetime.strptime(o["time_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc).timestamp()
    except (KeyError, TypeError, ValueError):
        return None


def _recent_window(obs, hours=24, max_rows=48):
    """From a chronological (oldest->newest) obs list, keep the last `hours` (relative
    to the newest obs, so a clock skew can't empty it), downsample to <= max_rows while
    always keeping the newest, and return NEWEST-FIRST for a readable table."""
    rows = [o for o in obs if o and _z_epoch(o) is not None]
    if not rows:
        return []
    newest = _z_epoch(rows[-1])
    cutoff = newest - hours * 3600
    win = [o for o in rows if _z_epoch(o) >= cutoff] or rows[-max_rows:]
    if len(win) > max_rows:
        step = (len(win) + max_rows - 1) // max_rows
        win = [o for i, o in enumerate(win) if (len(win) - 1 - i) % step == 0]
    return list(reversed(win))


class AODNProvider(BuoyProvider):
    """AODN / IMOS national near-real-time wave buoys (Australia). The open WFS 'map'
    layer carries every site's full recent timeseries; we fetch it once (cached) and
    group to the latest row per site -> station list AND latest obs in one request.
    Bulk parameters only (Hs / peak period / mean period / peak direction); the NRT
    product has no spectra. Some buoys are non-directional (blank direction)."""
    source = "AODN"
    source_name = "AODN / IMOS (Australia)"
    source_url = "https://portal.aodn.org.au"
    license_label = "CC BY 4.0"
    attribution_text = ("Source: Australia's Integrated Marine Observing System (IMOS), "
                        "a NCRIS facility, via AODN")
    stale_after_sec = 15 * 3600          # NRT aggregator: dispatch normally lags several hours
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 1800
    timeout = 90
    BASE = "https://geoserver-123.aodn.org.au/geoserver/ows"
    LAYER = "aodn:aodn_wave_nrt_v2_timeseries_map"
    PROPS = ("site_name,institution,wave_buoy_type,water_depth,time_end,TIME,"
             "significant_wave_height,wave_mean_period,peak_wave_direction,peak_wave_period,geom")
    _POINT = re.compile(r"POINT\s*\(\s*([-0-9.]+)\s+([-0-9.]+)")
    # IMOS/UWA sites that publish full directional spectra (WAVE-SPECTRA) -> can drive
    # NDBC-style partitions + a spectral-density chart. The rest are bulk-only.
    THREDDS = ("https://thredds.aodn.org.au/thredds/dodsC/IMOS/COASTAL-WAVE-BUOYS/"
               "WAVE-BUOYS/REALTIME/WAVE-SPECTRA/")
    SPECTRA_SITES = frozenset({
        "APOLLO-BAY", "BENGELLO", "BOB", "BRIGHTON", "CAPE-BRIDGEWATER", "CEDUNA",
        "CENTRAL", "COCKBURN-SOUND", "COLLAROY-NARRABEEN", "DONGARA-OFFSHORE",
        "FENTON-PATCHES", "HILLARYS", "MANINGRIDA", "MISSION-BEACH",
        "NORTH-KANGAROO-ISLAND", "OCEAN-BEACH", "SHARK-BAY-0-2", "STORM-BAY",
        "TANTABIDDI", "TATHRA", "TORBAY-WEST", "WILSONS-PROM", "WOOLI",
    })
    spectra_ttl_sec = 1200

    def __init__(self, http=None):
        super().__init__(http)
        self._latest_by_id = {}
        self._recent_by_id = {}
        self._spec_cache = {}                 # local_id -> (ts, spectrum dict)

    @classmethod
    def _spectra_code(cls, local_id):
        code = (local_id or "").upper().replace(" ", "-")
        return code if code in cls.SPECTRA_SITES else None

    @staticmethod
    def _num(row, ix, col):
        if col not in ix or len(row) <= ix[col]:
            return None
        v = row[ix[col]]
        try:
            return float(v) if v not in ("", "NaN") else None
        except ValueError:
            return None

    def _obs(self, row, ix, time_utc):
        return {
            "time_utc": time_utc,
            "hs_m": self._num(row, ix, "significant_wave_height"),
            "tp_s": self._num(row, ix, "peak_wave_period"),
            "mean_period_s": self._num(row, ix, "wave_mean_period"),
            "dir_deg": self._num(row, ix, "peak_wave_direction"),
            "dir_kind": "from",
        }

    def _fetch_stations(self):
        url = (self.BASE + "?service=WFS&version=1.0.0&request=GetFeature&typeName="
               + self.LAYER + "&outputFormat=csv&propertyName=" + self.PROPS)
        hdr, rows = _wfs_rows(self.http, url, self.timeout)
        if not hdr:
            return []
        ix = {c: i for i, c in enumerate(hdr)}
        if any(c not in ix for c in ("site_name", "time_end", "TIME", "geom")):
            return []
        bysite = {}                        # the map layer carries each site's full series
        for row in rows:
            if len(row) <= ix["geom"]:
                continue
            s = row[ix["site_name"]]
            if s:
                bysite.setdefault(s, []).append(row)
        out, latest_by, recent_by = [], {}, {}
        for s, srows in bysite.items():
            srows.sort(key=lambda r: r[ix["TIME"]])       # TIME is the per-obs timestamp
            last = srows[-1]
            m = self._POINT.search(last[ix["geom"]] or "")
            if not m:
                continue
            lon, lat = float(m.group(1)), float(m.group(2))
            # time_end is CONSTANT per site = latest obs in UTC; the per-row TIME column
            # carries an offset (~+10h AEST). Derive that offset from the newest row and
            # convert every row's TIME back to UTC.
            end_utc = _parse_naive(last[ix["time_end"]])
            max_t = _parse_naive(last[ix["TIME"]])
            offset = (max_t - end_utc) if (end_utc and max_t) else None
            obs = []
            for r in srows:
                tl = _parse_naive(r[ix["TIME"]])
                if tl is not None and offset is not None:
                    tu = (tl - offset).strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    tu = _z(r[ix["time_end"]])             # fallback (collapses history)
                obs.append(self._obs(r, ix, tu))
            latest_by[s] = obs[-1]
            recent_by[s] = _recent_window(obs)
            inst = last[ix["institution"]] if "institution" in ix else ""
            st = {
                "local_id": s,
                "name": ("%s - %s" % (s, inst)) if inst else s,
                "lat": lat, "lon": lon,
                "latest_time": obs[-1]["time_utc"],
            }
            if self._spectra_code(s):     # this buoy publishes full directional spectra
                st["capabilities"] = _caps(bulk=True, recent_history=True,
                                           directional=True, spectra=True, partitions=True)
            out.append(st)
        self._latest_by_id = latest_by
        self._recent_by_id = recent_by
        return out

    def detail(self, local_id):
        if local_id not in self._latest_by_id:
            self.list_stations()          # (re)build the map cache
        return {"latest": self._latest_by_id.get(local_id),
                "recent": self._recent_by_id.get(local_id, [])}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]

    def spectrum(self, local_id, count=25):
        """Last `count` directional spectra (chronological) for a spectra-capable site,
        via THREDDS OPeNDAP. Returns {freqs:[39], steps:[{time_utc, energy[39],
        alpha1[39], alpha2[39], r1[39], r2[39]}]} or None. alpha1 = mean wave direction
        per bin in the NDBC 'from' convention. One batched request per variable."""
        code = self._spectra_code(local_id)
        if not code:
            return None
        ckey = (local_id, count)
        with self._lock:
            hit = self._spec_cache.get(ckey)
            if hit and (time.time() - hit[0]) < self.spectra_ttl_sec:
                return hit[1]
        now = datetime.utcnow()
        url = (self.THREDDS + code + "/%04d/IMOS_COASTAL-WAVE-BUOYS_%04d%02d01_%s_"
               "RT_WAVE-SPECTRA_monthly.nc" % (now.year, now.year, now.month, code))
        try:
            dds = self.http.get(url + ".dds", timeout=self.timeout)
            if dds.status_code != 200:
                return None
            m = re.search(r"TIME\s*=\s*(\d+)", dds.text)
            if not m:
                return None
            n = int(m.group(1))
            i1 = n - 1
            i0 = max(0, n - count)
            k = i1 - i0 + 1
            sl = "[%d:1:%d]" % (i0, i1)
            freqs = _opendap_array(self.http, url, "FREQUENCY[0:1:38]", self.timeout, 39)
            energy = _opendap_array(self.http, url, "ENERGY.ENERGY%s[0:1:38]" % sl, self.timeout)
            a1 = _opendap_array(self.http, url, "A1.A1%s[0:1:38]" % sl, self.timeout)
            b1 = _opendap_array(self.http, url, "B1.B1%s[0:1:38]" % sl, self.timeout)
            a2 = _opendap_array(self.http, url, "A2.A2%s[0:1:38]" % sl, self.timeout)
            b2 = _opendap_array(self.http, url, "B2.B2%s[0:1:38]" % sl, self.timeout)
            tvals = _opendap_array(self.http, url, "TIME%s" % sl, self.timeout)
        except (requests.RequestException, ValueError):
            return None
        if len(freqs) != 39 or len(energy) < 39:
            return None

        def rows(v):                                  # reshape flat row-major -> k x 39
            v = v[-(k * 39):]
            out = [v[r * 39:(r + 1) * 39] for r in range(k)]
            return [row for row in out if len(row) == 39]
        E, A1, B1, A2, B2 = rows(energy), rows(a1), rows(b1), rows(a2), rows(b2)
        tvals = tvals[-k:]
        if not E or not (len(E) == len(A1) == len(B1) == len(A2) == len(B2)):
            return None

        def ang(av, bv, half=False):
            return [math.degrees(0.5 * math.atan2(bv[j], av[j]) if half
                                 else math.atan2(bv[j], av[j])) % 360.0 for j in range(len(av))]
        steps = []
        for r in range(len(E)):
            t = None
            if r < len(tvals):
                t = (datetime(1950, 1, 1) + timedelta(days=tvals[r])).strftime("%Y-%m-%dT%H:%M:%SZ")
            steps.append({
                "time_utc": t,
                "energy": E[r],
                "alpha1": ang(A1[r], B1[r]),
                "alpha2": ang(A2[r], B2[r], half=True),
                "r1": [math.hypot(A1[r][j], B1[r][j]) for j in range(39)],
                "r2": [math.hypot(A2[r][j], B2[r][j]) for j in range(39)],
            })
        spec = {"freqs": freqs, "steps": steps}
        with self._lock:
            self._spec_cache[ckey] = (time.time(), spec)
        return spec


class QLDProvider(BuoyProvider):
    """Queensland DETSI Coastal Data System -- 9 live coastal wave buoys, open JSON
    (apps.des.qld.gov.au, CORS-enabled). Positions are static (moored), so the station
    list is hardcoded; latest obs (30-min cadence) is fetched per site on demand."""
    source = "QLD"
    source_name = "Queensland DETSI"
    source_url = "https://www.qld.gov.au/environment/coasts-waterways/beach/monitoring"
    license_label = "CC BY 4.0"
    attribution_text = "Source: State of Queensland (DETSI), Coastal Data System"
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 6 * 3600
    timeout = 40
    FEED = "https://apps.des.qld.gov.au/data-sets/wave/wave-%s.json"
    SITES = [
        # (id, name, lat, lon)  -- moored, fixed positions
        (59,   "Albatross Bay (Weipa)", -12.687017, 141.685167),
        (4183, "Brisbane",              -27.49,      153.6341),
        (54,   "Caloundra",             -26.847133,  153.156133),
        (96,   "Emu Park",              -23.30275,   151.069017),
        (60,   "Gladstone",             -23.8950666, 151.5026333),
        (4740, "Mackay",                -21.03592,   149.5482),
        (4,    "Mooloolaba",            -26.566567,  153.181117),
        (52,   "North Moreton Bay",     -26.899783,  153.2822),
        (10,   "Townsville",            -19.176133,  147.074953),
    ]

    def _fetch_stations(self):
        return [{"local_id": str(i), "name": nm, "lat": lat, "lon": lon}
                for (i, nm, lat, lon) in self.SITES]

    @staticmethod
    def _obs(rec):
        def num(k):
            v = rec.get(k)
            try:
                return float(v) if v not in (None, "", "NaN") else None
            except (TypeError, ValueError):
                return None
        return {
            "time_utc": _iso_to_z(rec.get("DateTime")),
            "hs_m": num("Hsig"),
            "hmax_m": num("Hmax"),
            "tp_s": num("Tp"),
            "tz_s": num("Tz"),
            "dir_deg": num("Direction"),
            "sst_c": num("SST"),
            "dir_kind": "from",
        }

    def detail(self, local_id):
        try:
            r = self.http.get(self.FEED % local_id, timeout=self.timeout)
            if r.status_code != 200:
                return {"latest": None, "recent": []}
            arr = r.json()
        except (ValueError, requests.RequestException):
            return {"latest": None, "recent": []}
        if not arr:
            return {"latest": None, "recent": []}
        obs = [self._obs(rec) for rec in arr]      # feed is chronological (newest last)
        return {"latest": obs[-1], "recent": _recent_window(obs)}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


class AusWavesProvider(BuoyProvider):
    """AusWaves (UWA / IMOS) -- national Sofar Spotter network (Australia). Open
    WordPress REST, but the endpoints reject non-browser requests, so we send a
    browser User-Agent + Referer (no token/key needed). The list gives the catalogue
    + per-buoy recency; the per-buoy feed gives the timeseries (rows are NOT sorted,
    so we take the newest by unix time; -9999 = missing). Bulk + sea/swell params."""
    source = "AusWaves"
    source_name = "AusWaves (UWA / IMOS)"
    source_url = "https://auswaves.org"
    license_label = "CC BY 4.0"
    attribution_text = ("Source: AusWaves (University of Western Australia) and "
                        "Australia's Integrated Marine Observing System (IMOS)")
    stale_after_sec = 12 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 1800
    timeout = 40
    LIST = "https://auswaves.org/wp-json/waves/v1/list?type=all"
    BUOY = "https://auswaves.org/wp-json/waves/v1/buoys/%s"
    FRESH_SEC = 24 * 3600                  # skip catalogue entries not updated within this
    HEADERS = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
        "Referer": "https://auswaves.org/",
        "Accept": "application/json",
    }

    def _fetch_stations(self):
        try:
            r = self.http.get(self.LIST, headers=self.HEADERS, timeout=self.timeout)
            if r.status_code != 200:
                return []
            catalogue = r.json()
        except (ValueError, requests.RequestException):
            return []
        now = time.time()
        out = []
        for s in catalogue:
            try:
                if str(s.get("is_enabled")) != "1":
                    continue
                if str(s.get("drifting")) in ("1", "true", "True"):
                    continue              # drifting buoys have no fixed marker position
                lu = s.get("last_update")
                lu = float(lu) if lu not in (None, "", "0") else 0.0
                if not lu or (now - lu) > self.FRESH_SEC:
                    continue              # stale / decommissioned
                lat = float(s["lat"])
                lon = float(s["lng"])
                name = s.get("web_display_name") or s.get("label") or str(s["id"])
                out.append({"local_id": str(s["id"]), "name": name, "lat": lat, "lon": lon,
                            "latest_time": _unix_to_z(lu)})
            except (KeyError, ValueError, TypeError):
                continue
        return out

    @staticmethod
    def _utime(rec):
        try:
            return int(rec.get("Time (UNIX/UTC)") or 0)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _obs(cls, rec):
        def num(k):
            v = rec.get(k)
            if v is None:
                return None
            try:
                f = float(str(v).strip())
            except ValueError:
                return None
            return None if f <= -9990 else f
        ut = cls._utime(rec)
        obs = {
            "time_utc": _unix_to_z(ut) if ut else None,
            "hs_m": num("Hsig (m)"),
            "tp_s": num("Tp (s)"),
            "mean_period_s": num("Tm (s)"),
            "dir_deg": num("Dp (deg)"),
            "sst_c": num("SST (degC)"),
            "dir_kind": "from",
        }
        # Precomputed 2-way sea/swell split (Sofar Spotter/Smart-Mooring units only;
        # intermittently populated -> include a partition only when its height is real).
        parts = []
        sw_h = num("Hsig_swell (m)")
        if sw_h is not None:
            parts.append({"label": "Swell", "hs_m": sw_h, "period_s": num("Tm_swell (s)"),
                          "dir_deg": num("Dm_swell (deg)"), "spread_deg": num("DmSpr_swell (deg)")})
        se_h = num("Hsig_sea (m)")
        if se_h is not None:
            parts.append({"label": "Wind sea", "hs_m": se_h, "period_s": num("Tm_sea (s)"),
                          "dir_deg": num("Dm_sea (deg)"), "spread_deg": num("DmSpr_sea (deg)")})
        if parts:
            obs["partitions"] = parts
        return obs

    def detail(self, local_id):
        try:
            r = self.http.get(self.BUOY % local_id, headers=self.HEADERS, timeout=self.timeout)
            if r.status_code != 200:
                return {"latest": None, "recent": []}
            d = r.json()
        except (ValueError, requests.RequestException):
            return {"latest": None, "recent": []}
        data = d.get("data") if isinstance(d, dict) else None
        if not data:
            return {"latest": None, "recent": []}
        rows = sorted((rec for rec in data if self._utime(rec) > 0), key=self._utime)
        obs = [self._obs(rec) for rec in rows]     # chronological (the array is unsorted)
        if not obs:
            return {"latest": None, "recent": []}
        return {"latest": obs[-1], "recent": _recent_window(obs)}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


class IrishMarineProvider(BuoyProvider):
    """Marine Institute Ireland -- Irish Weather Buoy Network (IWBNetwork) realtime ERDDAP.
    Combined met+wave buoys (M2/M3/M5/M6). NOTE: the 'IWaveBNetwork*' datasets are stale;
    the live data is in 'IWBNetwork'. One call (last 2 days, orderBy station_id,time) ->
    station list + latest + 24h history per buoy. SI units, degrees-true 'from'."""
    source = "MI-IE"
    source_name = "Marine Institute (Ireland)"
    source_url = "https://www.marine.ie"
    license_label = "CC BY 4.0"
    attribution_text = "Source: Marine Institute Ireland, Irish Weather Buoy Network"
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 1800
    timeout = 40
    BASE = "https://erddap.marine.ie/erddap/tabledap/IWBNetwork"
    COLS = ("station_id,time,latitude,longitude,WaveHeight,WavePeriod,Tp,"
            "MeanWaveDirection,Hmax,SeaTemperature,SprTp")

    def __init__(self, http=None):
        super().__init__(http)
        self._latest_by_id = {}
        self._recent_by_id = {}

    @staticmethod
    def _obs(row, ix):
        def num(c):
            if c not in ix or ix[c] >= len(row):
                return None
            v = row[ix[c]]
            try:
                return float(v) if v not in ("", "NaN") else None
            except ValueError:
                return None
        return {
            "time_utc": _z(row[ix["time"]]) if "time" in ix and ix["time"] < len(row) else None,
            "hs_m": num("WaveHeight"),
            "tp_s": num("Tp"),
            "mean_period_s": num("WavePeriod"),
            "dir_deg": num("MeanWaveDirection"),
            "dir_spread_deg": num("SprTp"),
            "hmax_m": num("Hmax"),
            "sst_c": num("SeaTemperature"),
            "dir_kind": "from",
        }

    def _fetch_stations(self):
        # ONE call -> station list + latest + 24h history per buoy (only 4 buoys). Robust
        # vs per-click ERDDAP timeouts that previously could leave a buoy history-less.
        url = (self.BASE + ".csv?" + self.COLS +
               "&time%3E=now-2days&orderBy(%22station_id,time%22)")
        hdr, rows = _erddap_rows(self.http, url, self.timeout)
        if not hdr:
            return []
        ix = {c: i for i, c in enumerate(hdr)}
        bysite = {}
        for row in rows:
            try:
                bysite.setdefault(row[ix["station_id"]], []).append(row)
            except (KeyError, IndexError):
                continue
        out, latest, recent = [], {}, {}
        for sid, srows in bysite.items():
            obs = [self._obs(r, ix) for r in srows]      # chronological per station
            if not obs:
                continue
            try:
                lat = float(srows[-1][ix["latitude"]])
                lon = float(srows[-1][ix["longitude"]])
            except (KeyError, ValueError, IndexError):
                continue
            latest[sid] = obs[-1]
            recent[sid] = _recent_window(obs)
            out.append({"local_id": sid, "name": "Ireland %s" % sid, "lat": lat, "lon": lon,
                        "latest_time": obs[-1].get("time_utc")})
        self._latest_by_id = latest
        self._recent_by_id = recent
        return out

    def detail(self, local_id):
        if local_id not in self._latest_by_id:
            self.list_stations()
        rec = self._recent_by_id.get(local_id)
        if rec:
            return {"latest": self._latest_by_id.get(local_id), "recent": rec}
        # Cache empty (the shared list-build failed/was slow) -> direct per-station history
        # fetch so a buoy is never left history-less. Belt-and-braces vs the prior single-row bug.
        url = (self.BASE + ".csv?" + self.COLS + "&station_id=%22" + str(local_id) +
               "%22&time%3E=now-2days&orderBy(%22time%22)")
        hdr, rows = _erddap_rows(self.http, url, self.timeout)
        if hdr and rows:
            ix = {c: i for i, c in enumerate(hdr)}
            obs = [self._obs(r, ix) for r in rows]
            if obs:
                return {"latest": obs[-1], "recent": _recent_window(obs)}
        return {"latest": self._latest_by_id.get(local_id), "recent": []}

    def latest(self, local_id):
        if local_id not in self._latest_by_id:
            self.list_stations()
        return self._latest_by_id.get(local_id)


class CefasWaveNetProvider(BuoyProvider):
    """CEFAS WaveNet (UK) -- one open JSON Summary call returns every platform with its
    LATEST values. Aggregates Cefas + Met Office + Channel Coastal Observatory + Marine
    Institute buoys (~86 platforms). Latest-only (open history needs registration)."""
    source = "CEFAS"
    source_name = "CEFAS WaveNet (UK)"
    source_url = "https://wavenet.cefas.co.uk"
    license_label = "Open Government Licence"
    attribution_text = ("Source: Cefas WaveNet (incl. Met Office, Channel Coastal "
                        "Observatory, Marine Institute partner buoys)")
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, directional=True)     # latest-only, no recent history
    list_ttl_sec = 1800
    timeout = 40
    URL = "https://wavenet-api.cefas.co.uk/api/Summary"

    def __init__(self, http=None):
        super().__init__(http)
        self._latest_by_id = {}

    def _fetch_stations(self):
        try:
            r = self.http.get(self.URL, timeout=self.timeout)
            if r.status_code != 200:
                return []
            data = r.json()
        except (ValueError, requests.RequestException):
            return []
        out, latest = [], {}
        for p in data:
            try:
                lat = float(p["latitude"])
                lon = float(p["longitude"])
                pid = p["platformId"]
            except (KeyError, ValueError, TypeError):
                continue
            res = {}
            for item in (p.get("results") or []):
                k = item.get("identifier")
                if not k:
                    continue
                try:
                    res[k] = float(item.get("value"))
                except (TypeError, ValueError):
                    res[k] = None
            if res.get("Hm0") is None:        # only platforms reporting a live wave height
                continue
            latest[pid] = {
                "time_utc": _z(p.get("timestamp")),
                "hs_m": res.get("Hm0"),
                "tp_s": res.get("Tpeak"),
                "mean_period_s": res.get("Tz"),
                "dir_deg": res.get("W_PDIR"),
                "dir_spread_deg": res.get("W_SPR"),
                "sst_c": res.get("TEMP"),
                "dir_kind": "from",
            }
            out.append({"local_id": pid, "name": p.get("description") or pid,
                        "lat": lat, "lon": lon, "latest_time": latest[pid].get("time_utc")})
        self._latest_by_id = latest
        return out

    def detail(self, local_id):
        if local_id not in self._latest_by_id:
            self.list_stations()
        return {"latest": self._latest_by_id.get(local_id), "recent": []}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


def _station_priority(st):
    """Higher = preferred when two sources report the same physical buoy. Richness-first
    so the kept marker carries the best data: NDBC spectra > AODN spectra > AusWaves
    sea/swell split > clean agency bulk > AODN bulk."""
    caps = st.get("capabilities") or {}
    src = st.get("source")
    if src == "NDBC":
        return 100
    if caps.get("spectra"):            # AODN spectra buoys -> NDBC-style partitions
        return 90
    if src == "AusWaves":              # precomputed sea/swell split (where populated)
        return 70
    if src == "MI-IE":                 # Ireland ERDDAP (bulk + 24h history)
        return 65
    if src == "RWS":                   # Netherlands Waterinfo (bulk + history)
        return 62
    if src == "QLD":                   # clean 30-min agency JSON (+ SST)
        return 60
    if src == "SMHI":                  # Sweden (bulk + history)
        return 60
    if src == "CEFAS":                 # UK WaveNet aggregate (latest-only)
        return 58
    if src == "CDIP":
        return 55
    if src == "AODN":                  # bulk only
        return 50
    if src == "CMEMS":                 # pan-EU/global aggregator -> fills gaps, national wins
        return 30
    return 40


class SmhiProvider(BuoyProvider):
    """SMHI (Sweden) open oceanographic API -- Skagerrak/Kattegat/Baltic wave buoys. Station
    list from the Hs-parameter endpoint; per-station detail merges several per-parameter
    'latest-day' time series (the API is one call per parameter per station). SI, 'from'."""
    source = "SMHI"
    source_name = "SMHI (Sweden)"
    source_url = "https://www.smhi.se"
    license_label = "CC BY 4.0"
    attribution_text = "Source: SMHI (Swedish Meteorological and Hydrological Institute)"
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 3600
    timeout = 40
    BASE = "https://opendata-download-ocobs.smhi.se/api/version/latest"
    PARAMS = (("hs_m", 1), ("tp_s", 9), ("mean_period_s", 10), ("dir_deg", 7), ("hmax_m", 11))

    def _fetch_stations(self):
        try:
            d = self.http.get(self.BASE + "/parameter/1.json", timeout=self.timeout).json()
        except (ValueError, requests.RequestException):
            return []
        out = []
        for s in d.get("station", []):
            if not s.get("active"):
                continue
            try:
                out.append({"local_id": str(s["key"]), "name": s.get("name") or str(s["key"]),
                            "lat": float(s["latitude"]), "lon": float(s["longitude"])})
            except (KeyError, ValueError, TypeError):
                continue
        return out

    def _series(self, key, param):
        try:
            r = self.http.get(self.BASE + "/parameter/%d/station/%s/period/latest-day/data.json"
                              % (param, key), timeout=self.timeout)
            if r.status_code != 200:
                return {}
            vals = r.json().get("value") or []
        except (ValueError, requests.RequestException):
            return {}
        out = {}
        for v in vals:
            try:
                out[int(v["date"])] = float(v["value"])
            except (KeyError, TypeError, ValueError):
                pass
        return out

    def detail(self, local_id):
        series = {key: self._series(local_id, p) for key, p in self.PARAMS}
        times = sorted(series["hs_m"].keys())
        if not times:
            return {"latest": None, "recent": []}
        obs = []
        for t in times:
            row = {"time_utc": _unix_to_z(t // 1000), "dir_kind": "from"}
            for key, _ in self.PARAMS:
                row[key] = series[key].get(t)
            obs.append(row)
        return {"latest": obs[-1], "recent": _recent_window(obs)}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


class RwsProvider(BuoyProvider):
    """Rijkswaterstaat (Netherlands) Waterinfo -- North Sea offshore wave stations. Open POST
    JSON API on the new ddapi20 host. Heights are in CENTIMETRES (->/100=m). Hm0/Tm02/Th0.
    Per-station detail merges per-grootheid series by timestamp (one POST per parameter)."""
    source = "RWS"
    source_name = "Rijkswaterstaat (Netherlands)"
    source_url = "https://waterinfo.rws.nl"
    license_label = "Public domain (CC0)"
    attribution_text = "Source: Rijkswaterstaat (Netherlands), Waterinfo"
    stale_after_sec = 6 * 3600
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 6 * 3600
    timeout = 40
    URL = ("https://ddapi20-waterwebservices.rijkswaterstaat.nl/"
           "ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen")
    SITES = [
        # (code, name, lat, lon) -- fixed offshore platforms/buoys
        ("europlatform", "Europlatform", 51.99781, 3.275071),
        ("goeree.lichteiland", "Goeree Lichteiland", 51.925034, 3.668416),
        ("eurogeul.e13", "Eurogeul E13", 52.009184, 3.741804),
        ("ijgeul", "IJgeul", 52.462272, 4.482472),
        ("hollandsekust.zuid.alpha", "Hollandse Kust Zuid Alpha", 52.232547, 4.19569),
        ("f3", "F3", 54.853199, 4.726133),
        ("j6", "J6", 53.816632, 2.95001),
        ("l9", "L9", 53.616667, 4.966667),
    ]
    GROOTHEDEN = (("Hm0", "hs_m", 0.01), ("Tm02", "mean_period_s", 1.0), ("Th0", "dir_deg", 1.0))

    def _fetch_stations(self):
        return [{"local_id": code, "name": name, "lat": lat, "lon": lon}
                for (code, name, lat, lon) in self.SITES]

    def _series(self, code, grootheid):
        now = datetime.now(timezone.utc)
        body = {
            "AquoPlusWaarnemingMetadata": {"AquoMetadata": {
                "Compartiment": {"Code": "OW"}, "Grootheid": {"Code": grootheid}}},
            "Locatie": {"Code": code, "Coordinatenstelsel": "ETRS89"},
            "Periode": {
                "Begindatumtijd": (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S.000+00:00"),
                "Einddatumtijd": now.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")},
        }
        try:
            r = self.http.post(self.URL, json=body, timeout=self.timeout)
            if r.status_code != 200:
                return {}
            d = r.json()
        except (ValueError, requests.RequestException):
            return {}
        out = {}
        for w in (d.get("WaarnemingenLijst") or []):
            for m in (w.get("MetingenLijst") or []):
                t = m.get("Tijdstip")
                v = (m.get("Meetwaarde") or {}).get("Waarde_Numeriek")
                if t is None or v is None:
                    continue
                try:
                    fv = float(v)
                    if fv < 1e5:                       # RWS missing-value sentinel is huge
                        out[t] = fv
                except (TypeError, ValueError):
                    pass
        return out

    def detail(self, local_id):
        series = {}                            # key -> {timestamp: scaled value}
        for i, (g, key, sc) in enumerate(self.GROOTHEDEN):
            if i:
                time.sleep(0.4)                # RWS load-sheds rapid POSTs -> pace them out
            series[key] = {t: v * sc for t, v in self._series(local_id, g).items()}
        hs = series.get("hs_m") or {}
        if not hs:
            return {"latest": None, "recent": []}

        def newest(s):
            return s[max(s)] if s else None
        # 24h history on the Hs timeline; other params filled where their timestamp aligns.
        obs = []
        for t in sorted(hs.keys()):
            row = {"time_utc": _iso_to_z(t), "dir_kind": "from"}
            for g, key, sc in self.GROOTHEDEN:
                row[key] = series[key].get(t)
            obs.append(row)
        # Latest reading: each parameter's OWN most-recent value (they can lag each other a
        # step, and some gauges report no direction), so the current cards aren't left blank.
        latest = {"time_utc": _iso_to_z(max(hs.keys())), "dir_kind": "from"}
        for g, key, sc in self.GROOTHEDEN:
            latest[key] = newest(series.get(key))
        return {"latest": latest, "recent": _recent_window(obs)}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


class CopernicusProvider(BuoyProvider):
    """Copernicus Marine in-situ NRT -- the open, anonymous-S3 pan-EU/global aggregator.
    The index CSV yields the station list + positions + latest-obs time (no NetCDF needed);
    per-platform values + history come from the platform's daily NetCDF on demand (h5netcdf).
    It re-bundles the national/EuroGOOS feeds -> LOWEST dedup priority. ~6-30h NRT latency.
    Scoped to a European bounding box for now (widen BBOX for worldwide coverage)."""
    source = "CMEMS"
    source_name = "Copernicus Marine in-situ"
    source_url = "https://marine.copernicus.eu"
    license_label = "Copernicus Marine Service (free)"
    attribution_text = ("Source: Copernicus Marine Service in-situ TAC "
                        "(E.U. Copernicus Marine Service Information)")
    stale_after_sec = 36 * 3600          # NRT dispatch lag ~6-30h
    capabilities = _caps(bulk=True, recent_history=True, directional=True)
    list_ttl_sec = 3 * 3600
    timeout = 60
    S3 = "https://s3.waw3-1.cloudferro.com/mdl-native-01/native/"
    DATASET = ("INSITU_GLO_PHYBGCWAV_DISCRETE_MYNRT_013_030/"
               "cmems_obs-ins_glo_phybgcwav_mynrt_na_irr_202311/")
    BBOX = (30.0, 73.0, -30.0, 42.0)     # (lat_min, lat_max, lon_min, lon_max): Europe
    LIVE_MAX_AGE = 4 * 86400             # only platforms whose latest file is this fresh

    def __init__(self, http=None):
        super().__init__(http)
        self._file_by_id = {}            # local_id -> relative .nc path (latest per platform)

    def _fetch_stations(self):
        try:
            idx = self.http.get(self.S3 + self.DATASET + "index_latest.txt",
                                timeout=self.timeout).text
        except requests.RequestException:
            return []
        latmin, latmax, lonmin, lonmax = self.BBOX
        now = time.time()
        best = {}
        for r in csv.reader(io.StringIO(idx)):
            if not r or r[0].startswith("#") or len(r) < 8:
                continue
            if "VHM0" not in r[-1] and "VAVH" not in r[-1]:    # parameters column
                continue
            try:
                la = (float(r[2]) + float(r[3])) / 2.0
                lo = (float(r[4]) + float(r[5])) / 2.0
            except (ValueError, IndexError):
                continue
            if not (latmin <= la <= latmax and lonmin <= lo <= lonmax):
                continue
            tend = r[7].strip()
            tz = tend if tend.endswith("Z") else tend + "Z"
            ep = _z_epoch({"time_utc": tz})
            if ep is None or (now - ep) > self.LIVE_MAX_AGE:   # skip long-inactive platforms
                continue
            fn = r[1]
            pid = fn.split("/")[-1].rsplit("_", 1)[0]          # GL_TS_MO_6200064_DATE.nc -> GL_TS_MO_6200064
            if pid not in best or tend > best[pid][0]:
                best[pid] = (tend, fn, la, lo, tz)
        out = []
        self._file_by_id = {}
        for pid, (tend, fn, la, lo, tz) in best.items():
            self._file_by_id[pid] = fn
            name = pid.split("_")[-1].replace("-", " ") if "_" in pid else pid
            out.append({"local_id": pid, "name": name, "lat": la, "lon": lo, "latest_time": tz})
        return out

    def detail(self, local_id):
        fn = self._file_by_id.get(local_id)
        if not fn:
            self.list_stations()
            fn = self._file_by_id.get(local_id)
        if not fn:
            return {"latest": None, "recent": []}
        try:
            import h5netcdf
            import numpy as np
            data = self.http.get(self.S3 + fn, timeout=self.timeout).content
            ds = h5netcdf.File(io.BytesIO(data), "r")
        except Exception:
            return {"latest": None, "recent": []}
        try:
            tvar = np.asarray(ds["TIME"][:]).astype("float64").ravel()

            def col(*names):
                for nm in names:
                    if nm in ds.variables:
                        a = np.asarray(ds[nm][:]).astype("float64")
                        if a.ndim > 1:
                            a = a[:, 0]                 # surface (DEPTH=0)
                        a = a.ravel()
                        a[a > 1e30] = np.nan            # _FillValue ~9.97e36
                        return a
                return None
            hs, tp = col("VHM0", "VAVH"), col("VTPK")
            mp, dr = col("VTZA", "VTZM", "VTM02"), col("VMDR", "VPED")
        except Exception:
            return {"latest": None, "recent": []}

        def val(a, i):
            if a is None or i >= len(a):
                return None
            return float(a[i]) if np.isfinite(a[i]) else None
        obs = []
        for i in range(len(tvar)):
            t = datetime(1950, 1, 1) + timedelta(days=float(tvar[i]))
            h = val(hs, i)
            if h is None:
                continue                                # keep only rows with a real Hs
            obs.append({"time_utc": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "hs_m": h,
                        "tp_s": val(tp, i), "mean_period_s": val(mp, i),
                        "dir_deg": val(dr, i), "dir_kind": "from"})
        if not obs:
            return {"latest": None, "recent": []}
        return {"latest": obs[-1], "recent": _recent_window(obs)}

    def latest(self, local_id):
        return self.detail(local_id)["latest"]


def _name_key(name):
    """Normalized buoy designation for cross-source matching: strip generic descriptors +
    source words + punctuation so 'Ireland M6' and 'M6 Buoy' both reduce to 'M6'. Used as a
    SECONDARY dedup signal (with a looser radius) for the same physical buoy relayed by two
    networks at slightly different reported positions. Keeps distinguishing words like
    INNER/OUTER/NORTH so 'Newcastle Inner' != 'Newcastle Outer'."""
    if not name:
        return ""
    n = name.upper()
    for w in ("WAVENET", "BUOY", "SITE", "STATION", "IRELAND", "WAVE", "OFFSHORE", "INSHORE"):
        n = n.replace(w, " ")
    return re.sub(r"[^A-Z0-9]", "", n)


def merge_stations(provider_lists, radius_km=1.0, name_radius_km=10.0):
    """Flatten provider station lists and physically dedup co-located buoys, keeping the
    RICHEST source per cluster (see _station_priority: spectra > sea/swell split > bulk).
    Two cross-source markers merge when within radius_km, OR within the looser name_radius_km
    if their normalized names match (catches the SAME buoy relayed by two networks at slightly
    different reported positions, e.g. Marine Institute M6 vs CEFAS 'M6 Buoy' ~3.6 km apart).
    A hidden duplicate is marked dup_of the kept one; the kept marker lists the extra
    source(s) in 'also_sources'.
    """
    allst = [st for lst in provider_lists for st in lst]
    allst.sort(key=_station_priority, reverse=True)   # stable: ties keep provider order
    kept, keptkey = [], []
    for st in allst:
        nk = _name_key(st.get("name"))
        dup = None
        for idx, k in enumerate(kept):
            if k["source"] == st["source"]:
                continue
            dist = haversine_km(k["lat"], k["lon"], st["lat"], st["lon"])
            if dist <= radius_km or (len(nk) >= 2 and nk == keptkey[idx] and dist <= name_radius_km):
                dup = k
                break
        if dup is not None:
            st = dict(st, dup_of=dup["id"])
            dup.setdefault("also_sources", [])
            if st["source"] not in dup["also_sources"]:
                dup["also_sources"].append(st["source"])
        else:
            kept.append(st)
            keptkey.append(nk)
    return kept
