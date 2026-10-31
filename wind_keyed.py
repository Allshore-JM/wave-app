"""Live wind stations from the agencies that ask for a key (plan section 69): KNMI (the Netherlands), MET Norway's
Frost (Norway) and Taiwan's Central Weather Administration.

Each key comes from the environment (KNMI_API_KEY, FROST_CLIENT_ID, CWA_API_KEY: set in Render on both services); a
source without its key is left out. A key is never written to an answer, a log line, /healthz or a page: KNMI's and
Frost's go in request headers; CWA's goes in the URL's query (its API takes it nowhere else), so every message that may
carry a CWA URL passes through redact() first.

Sources:
- knmi  KNMI Data Platform, Open Data API, dataset 10-minute-in-situ-meteorological-observations v1.0: one NetCDF file
        every 10 minutes holding every station (ff mean wind m/s, dd direction, gff / fx gust; ta, rh, pp). The newest
        file names come from one list call; a file's download address is a short-lived signed URL (never logged). The
        stations are the file's own (all kept: the whole country is near the sea or the IJsselmeer). The window's 24
        hours are kept in memory; after a start the earlier files are read a few a refresh (the back-fill).
- frost MET Norway Frost API (frost.met.no), HTTP Basic with the client id. The sources reporting wind_speed are read
        once a day and kept when tools/wind/norway_coast30.bin marks their place (within 30 km of the sea or a large
        lake, or at sea: the section-39 rule). Latest readings in batches; a window asks the station's 24 hours.
- cwa   CWA open data O-A0003-001 (10-minute observations, 363 stations): the snapshot holds the coastal ones (built
        offline from the keyless copy of the same file); readings every 10 minutes with the key; the 24 hours in
        memory. CWA's gust field is the day's highest so far (as JMA's): not shown."""
import base64
import calendar
import io
import json
import math
import os
import re
import struct
import threading
import time
from datetime import datetime, timezone

import buoy_sources
import wind_more as M
import wind_sources as W

KNMI_API = "https://api.dataplatform.knmi.nl/open-data/v1/datasets/10-minute-in-situ-meteorological-observations/versions/1.0"
KNMI_LIST = KNMI_API + "/files?maxKeys={n}&orderBy=lastModified&sorting=desc"
KNMI_URL = KNMI_API + "/files/{name}/url"
FROST = "https://frost.met.no"
FROST_SOURCES = FROST + "/sources/v0.jsonld?types=SensorSystem&elements=wind_speed&country=NO&fields=id,name,geometry,masl"
FROST_OBS = FROST + "/observations/v0.jsonld?sources={src}&referencetime={ref}&elements={el}&timeoffsets=default&levels=default"
FROST_LATEST = "&maxage=PT2H&limit=1"
FROST_GUST = "max(wind_speed_of_gust PT10M)"
FROST_PAGE = "https://seklima.met.no/"
CWA_API = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/{id}?format=JSON&Authorization={key}"
CWA_S3 = "https://cwaopendata.s3.ap-northeast-1.amazonaws.com/Observation/{id}.json"   # keyless copy: the snapshot tool
CWA_WIND = "O-A0003-001"
TAIPEI_OFFSET = 8 * 3600
SOURCES = {
    "knmi": {"name": "KNMI", "short": "KNMI", "url": "https://dataplatform.knmi.nl/",
             "licence": "Source: KNMI, Royal Netherlands Meteorological Institute (CC BY 4.0, units converted by "
                        "allshoresurf.com)"},
    "frost": {"name": "MET Norway", "short": "MET Norway", "url": "https://frost.met.no/",
              "licence": "Data from The Norwegian Meteorological Institute (MET Norway, CC BY 4.0)"},
    "cwa": {"name": "Central Weather Administration", "short": "CWA", "url": "https://opendata.cwa.gov.tw/",
            "licence": "Central Weather Administration (Taiwan), 10-minute observations O-A0003-001. The Open Data is "
                       "made available to the public under the Open Government Data License, https://data.gov.tw/license"},
}
KEYS = tuple(SOURCES)
ENV = {"knmi": "KNMI_API_KEY", "frost": "FROST_CLIENT_ID", "cwa": "CWA_API_KEY"}
KEPT = ("knmi", "cwa")                           # the windows' 24 hours are the feed's own, in memory
MAX_FILE = 8 * 1024 * 1024                       # one KNMI file (a few hundred KB)
MAX_JSON = 4 * 1024 * 1024
FROST_BATCH = 50                                 # sources per latest request
KNMI_LIST_N = 150                                # the list call's names: 24 h of 10-minute files and a margin
KNMI_BACKFILL = 6                                # older files read per refresh while the 24 hours are not complete
_KEY_IN_URL = re.compile(r"(Authorization=)[^&\s'\"]+", re.I)
_SIGNED = re.compile(r"(https?://[^\s'\"?]+)\?[^\s'\"]*(?:Signature|X-Amz-|sig=|token=)[^\s'\"]*", re.I)


def keys_from_env(env=None):
    """{source: key} of the sources whose key is set (empty values left out)."""
    env = os.environ if env is None else env
    return {k: env.get(v, "").strip() for k, v in ENV.items() if env.get(v, "").strip()}


def redact(text, keys=()):
    """text with every CWA key value, every given key and every signed URL's query replaced."""
    s = str(text or "")
    for k in keys or ():
        if k:
            s = s.replace(k, "[key]")
    s = _KEY_IN_URL.sub(r"\1[key]", s)
    return _SIGNED.sub(r"\1?[signed]", s)


def frost_auth(client_id):
    return {"User-Agent": W.USER_AGENT,
            "Authorization": "Basic " + base64.b64encode((client_id + ":").encode("utf-8")).decode("ascii")}


def knmi_auth(key):
    return {"User-Agent": W.USER_AGENT, "Authorization": key}


# ============================================================================================== NetCDF files (KNMI)

class NcError(ValueError):
    pass


def read_netcdf(data):
    """{name: (values, attrs)} of a NetCDF file's variables: NetCDF-4 (HDF5, via h5py) or the classic formats
    (CDF-1 / CDF-2, read here). values: a numpy array (strings as str); attrs: {name: value}."""
    if data[:4] == b"\x89HDF":
        return _read_hdf5(data)
    if data[:3] == b"CDF" and data[3:4] in (b"\x01", b"\x02"):
        return _read_cdf(data)
    raise NcError("not a NetCDF file")


def _decode(v):
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace").strip("\x00 ").strip()
    return v


def _read_hdf5(data):
    import h5py
    import numpy as np
    out = {}
    with h5py.File(io.BytesIO(data), "r") as f:
        def visit(name, obj):
            if isinstance(obj, h5py.Dataset):
                v = obj[()]
                if isinstance(v, np.ndarray) and v.dtype.kind in ("S", "O"):
                    v = np.array([_decode(x) for x in v.ravel()], dtype=object).reshape(v.shape)
                elif isinstance(v, bytes):
                    v = _decode(v)
                attrs = {k: _decode(a) if isinstance(a, bytes) else a for k, a in obj.attrs.items()}
                out[name.rsplit("/", 1)[-1]] = (v, attrs)
        f.visititems(visit)
    return out


_CDF_TYPES = {1: ("b", 1), 2: ("c", 1), 3: ("h", 2), 4: ("i", 4), 5: ("f", 4), 6: ("d", 8)}


def _read_cdf(data):
    """The classic NetCDF formats (CDF-1, CDF-2): enough for a file of fixed-size and record variables."""
    import numpy as np
    pos = [4]
    off64 = data[3] == 2

    def u32():
        v = struct.unpack_from(">I", data, pos[0])[0]
        pos[0] += 4
        return v

    def name():
        n = u32()
        s = data[pos[0]:pos[0] + n].decode("utf-8", "replace")
        pos[0] += n + (-n % 4)
        return s

    def values(t, n):
        code, size = _CDF_TYPES[t]
        raw = data[pos[0]:pos[0] + n * size]
        pos[0] += n * size + (-(n * size) % 4)
        if t == 2:
            return raw.decode("utf-8", "replace").strip("\x00 ")
        return list(struct.unpack(">%d%s" % (n, code), raw)) if n else []

    def attrs():
        tag, n = u32(), u32()
        out = {}
        if tag not in (0, 12):
            raise NcError("bad attribute list")
        for _ in range(n):
            k = name()
            t, m = u32(), u32()
            if t not in _CDF_TYPES:
                raise NcError("bad attribute type")
            v = values(t, m)
            out[k] = v[0] if isinstance(v, list) and len(v) == 1 else v
        return out

    numrecs = u32()
    tag, n = u32(), u32()
    dims = []
    for _ in range(n if tag == 10 else 0):
        dims.append((name(), u32()))
    attrs()                                              # global
    tag, n = u32(), u32()
    var = []
    for _ in range(n if tag == 11 else 0):
        k = name()
        nd = u32()
        ids = [u32() for _ in range(nd)]
        a = attrs()
        t, vsize = u32(), u32()
        begin = struct.unpack_from(">Q" if off64 else ">I", data, pos[0])[0]
        pos[0] += 8 if off64 else 4
        var.append((k, ids, a, t, vsize, begin))
    rec_vars = [v for v in var if v[1] and dims[v[1][0]][1] == 0]
    recsize = sum(v[4] for v in rec_vars) if len(rec_vars) > 1 else (rec_vars[0][4] if rec_vars else 0)
    out = {}
    for k, ids, a, t, vsize, begin in var:
        if t not in _CDF_TYPES:
            continue
        code, size = _CDF_TYPES[t]
        shape = [dims[i][1] for i in ids]
        is_rec = bool(ids) and shape[0] == 0
        per = 1
        for s in (shape[1:] if is_rec else shape):
            per *= s
        dt = {"b": ">i1", "c": "S1", "h": ">i2", "i": ">i4", "f": ">f4", "d": ">f8"}[code]
        if is_rec:
            chunks = [data[begin + r * recsize: begin + r * recsize + per * size] for r in range(numrecs)]
            raw = b"".join(chunks)
            shape = [numrecs] + shape[1:]
        else:
            raw = data[begin: begin + per * size]
        if len(raw) < per * size * (numrecs if is_rec else 1):
            raise NcError("the file is short")
        arr = np.frombuffer(raw, dtype=dt)
        if t == 2:                                       # char arrays: the last dimension is the string's length
            arr = arr.reshape(shape) if shape else arr
            if arr.ndim >= 1:
                strs = np.apply_along_axis(lambda r: b"".join(r).decode("utf-8", "replace").strip("\x00 "), -1, arr)
                out[k] = (strs, a)
            continue
        out[k] = (arr.reshape(shape) if shape else arr, a)
    return out


KNMI_NAMES = {"id": ("station", "stations", "station_id", "stationid", "wsi"),
              "name": ("stationname", "station_name", "name"),
              "lat": ("lat", "latitude"), "lon": ("lon", "longitude"),
              "time": ("time",), "s": ("ff", "ff_10m"), "d": ("dd",), "g": ("gff", "fx", "fxs"),
              "at": ("ta",), "rh": ("rh",), "p": ("pp",)}


def _pick(vars_, key):
    for n in KNMI_NAMES[key]:
        if n in vars_:
            return vars_[n]
    return None


def _nc_time(values, attrs):
    """Epoch seconds of the newest time value (units "seconds|minutes|hours|days since YYYY-MM-DD[ HH:MM:SS]")."""
    units = str(attrs.get("units") or "")
    m = re.match(r"\s*(seconds|minutes|hours|days)\s+since\s+(\d{4})-(\d\d)-(\d\d)(?:[T ](\d\d):(\d\d)(?::(\d\d))?)?", units)
    if not m:
        raise NcError("unknown time units %r" % units[:40])
    base = calendar.timegm((int(m.group(2)), int(m.group(3)), int(m.group(4)), int(m.group(5) or 0),
                            int(m.group(6) or 0), int(m.group(7) or 0)))
    k = {"seconds": 1, "minutes": 60, "hours": 3600, "days": 86400}[m.group(1)]
    flat = [float(x) for x in list(getattr(values, "ravel", lambda: values)())] if hasattr(values, "ravel") else [float(values)]
    flat = [x for x in flat if math.isfinite(x)]
    if not flat:
        raise NcError("no time value")
    return int(round(base + max(flat) * k))


def _col(arr, i, n):
    """Station i's newest value of a [station], [station, time] or [time, station] variable (n stations: the axis
    whose length is n is the stations'), as a float or None."""
    import numpy as np
    a = np.asarray(arr)
    try:
        if a.ndim == 0:
            v = a.item()
        elif a.ndim == 1:
            v = a[i]
        elif a.ndim == 2 and a.shape[0] == n and a.shape[1] != n:
            v = a[i, -1]
        elif a.ndim == 2 and a.shape[1] == n:
            v = a[-1, i]
        elif a.ndim == 2:
            v = a[i, -1]
        else:
            return None
        v = float(v)
    except (IndexError, TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _fill(attrs):
    for k in ("_FillValue", "missing_value"):
        v = attrs.get(k)
        try:
            return float(v[0] if isinstance(v, (list, tuple)) else v)
        except (TypeError, ValueError, IndexError):
            continue
    return None


def parse_knmi(data, file_name=None):
    """(stations {id: {name, lat, lon}}, readings {id: [reading]}) of one 10-minute file. Raises NcError naming the
    variables seen when the file lacks one this reader needs (so /healthz says what to fix)."""
    import numpy as np
    vars_ = read_netcdf(data)
    need = {k: _pick(vars_, k) for k in ("id", "lat", "lon", "time", "s")}
    absent = [k for k, v in need.items() if v is None]
    if absent:
        raise NcError("the file has no %s (variables: %s)" % ("/".join(absent), ", ".join(sorted(vars_))[:300]))
    ids = [str(_decode(x)).strip() for x in np.asarray(need["id"][0]).ravel()]
    n = len(ids)
    names = _pick(vars_, "name")
    names = [str(_decode(x)).strip() for x in np.asarray(names[0]).ravel()] if names is not None else ids
    try:
        t = _nc_time(*need["time"])
    except NcError:
        m = re.search(r"(\d{12})", file_name or "")
        if not m:
            raise
        t = calendar.timegm(time.strptime(m.group(1), "%Y%m%d%H%M"))
    lat, lon = need["lat"][0], need["lon"][0]
    fields = {}
    for k in ("s", "d", "g", "at", "rh", "p"):
        v = _pick(vars_, k)
        fields[k] = (v[0], _fill(v[1])) if v is not None else None
    stations, out = {}, {}
    for i, sid in enumerate(ids):
        sid = re.sub(r"[^A-Za-z0-9_-]", "", sid)[:16]
        if not sid:
            continue
        la, lo = _col(lat, i, n), _col(lon, i, n)
        if la is None or lo is None or not (-90 <= la <= 90 and -180 <= lo <= 360):
            continue
        stations[sid] = {"name": names[i] if i < len(names) and names[i] else sid, "lat": round(la, 4),
                         "lon": round(((lo + 180) % 360) - 180, 4)}

        def val(k):
            f = fields.get(k)
            if f is None:
                return None
            x = _col(f[0], i, n)
            return None if x is None or (f[1] is not None and x == f[1]) else x
        r = W.reading(t, val("s"), val("g"), val("d"))
        if r is None:
            continue
        out[sid] = [W.with_weather(r, at=W._temp(val("at")), rh=W._pct(val("rh")), p=W._hpa(val("p")))]
    return stations, out


def knmi_file_names(doc):
    """The list call's file names, newest first."""
    files = doc.get("files") if isinstance(doc, dict) else None
    if not isinstance(files, list):
        raise ValueError("the KNMI list answer has no files")
    return [str(f.get("filename")) for f in files if isinstance(f, dict) and f.get("filename")]


def knmi_file_time(name):
    m = re.search(r"(\d{12})", name or "")
    return calendar.timegm(time.strptime(m.group(1), "%Y%m%d%H%M")) if m else None


# ============================================================================================== Frost

def parse_frost_sources(doc):
    """{SN id: {name, lat, lon}} of Frost's sources answer."""
    out = {}
    for s in (doc or {}).get("data") or ():
        try:
            sid = str(s["id"])
            lon, lat = s["geometry"]["coordinates"][:2]
            lat, lon = float(lat), float(lon)
        except (KeyError, TypeError, ValueError, IndexError):
            continue
        if not re.fullmatch(r"SN\d{1,8}", sid) or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        out[sid] = {"name": frost_name(s.get("name") or sid), "lat": round(lat, 4), "lon": round(lon, 4)}
    return out


_SMALL = {"I", "PA", "VED", "OG", "AV"}


def frost_name(name):
    """Frost's upper-case names in place-name case ("OSLO - BLINDERN" -> "Oslo - Blindern"; "LISTA FYR")."""
    n = " ".join(str(name or "").split())
    if n != n.upper():
        return n
    words = []
    for w in n.split(" "):
        if w in _SMALL and words:
            words.append(w.lower())
        elif re.fullmatch(r"[A-ZÆØÅ]{1,3}\d*|\d+", w) and len(w) <= 2 and w not in ("AS",):
            words.append(w)
        else:
            words.append("-".join(p[:1] + p[1:].lower() for p in w.split("-")))
    return " ".join(words)


def parse_frost_obs(doc, gust=FROST_GUST):
    """{SN id: [reading, ...]} of an observations answer (one or many reference times per source)."""
    rows = {}
    for d in (doc or {}).get("data") or ():
        try:
            sid = str(d["sourceId"]).split(":", 1)[0]
            t = M._epoch_iso(d["referenceTime"])
        except (KeyError, TypeError, ValueError):
            continue
        if t is None:
            continue
        vals = {}
        for o in d.get("observations") or ():
            try:
                vals[o["elementId"]] = float(o["value"])
            except (KeyError, TypeError, ValueError):
                continue
        r = W.reading(t, vals.get("wind_speed"), vals.get(gust), vals.get("wind_from_direction"))
        if r is not None:
            rows.setdefault(sid, {})[t] = r
    return {sid: [v[t] for t in sorted(v)] for sid, v in rows.items()}


class CoastMask:
    """tools/wind/norway_coast30.bin: a JSON header line ({lat0, lon0, step, rows, cols}: lat0/lon0 the south-west
    corner) then the cells' bits, row by row from the south, packed most significant bit first (1 = keep)."""

    def __init__(self, data):
        head, _, bits = data.partition(b"\n")
        h = json.loads(head)
        self.lat0, self.lon0, self.step = float(h["lat0"]), float(h["lon0"]), float(h["step"])
        self.rows, self.cols = int(h["rows"]), int(h["cols"])
        if len(bits) * 8 < self.rows * self.cols:
            raise ValueError("the coast mask is short")
        self.bits = bits

    @classmethod
    def load(cls, path):
        with open(path, "rb") as f:
            return cls(f.read())

    def keep(self, lat, lon):
        r = int(math.floor((lat - self.lat0) / self.step))
        c = int(math.floor((lon - self.lon0) / self.step))
        if not (0 <= r < self.rows and 0 <= c < self.cols):
            return False
        i = r * self.cols + c
        return bool(self.bits[i >> 3] & (0x80 >> (i & 7)))


MASK_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tools", "wind", "norway_coast30.bin")


# ============================================================================================== CWA

def _walk_list(doc, key):
    """The first list found under `key` anywhere in doc (the REST answer wraps it in records, the file in cwaopendata)."""
    todo = [doc]
    seen = 0
    while todo and seen < 10000:
        o = todo.pop(0)
        seen += 1
        if isinstance(o, dict):
            v = o.get(key)
            if isinstance(v, list):
                return v
            todo.extend(v for v in o.values() if isinstance(v, (dict, list)))
        elif isinstance(o, list):
            todo.extend(v for v in o if isinstance(v, (dict, list)))
    return []


def _cwa_num(v, lo=-1e9, hi=1e9):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x) or x <= -99 or x < lo or x > hi:
        return None
    return x


def cwa_time(s):
    """Epoch seconds of CWA's ISO time with an offset ("2026-11-01T01:50:00+08:00")."""
    try:
        return int(datetime.fromisoformat(str(s).strip()).timestamp())
    except (TypeError, ValueError):
        return None


def cwa_wgs84(st):
    for c in ((st.get("GeoInfo") or {}).get("Coordinates") or ()):
        if str(c.get("CoordinateName")).upper() == "WGS84":
            la, lo = _cwa_num(c.get("StationLatitude"), -90, 90), _cwa_num(c.get("StationLongitude"), -180, 180)
            if la is not None and lo is not None:
                return la, lo
    return None


def parse_cwa_wind(doc):
    """(stations {StationId: {name, lat, lon, alt}}, readings {StationId: [reading]}) of an O-A0003-001 answer (the REST
    answer or the keyless file). WindSpeed m/s and WindDirection degrees (both -99 when missing); no gust (GustInfo is
    the day's highest so far); air temperature and humidity (the pressure is the station's, not sea level)."""
    stations, out = {}, {}
    for st in _walk_list(doc, "Station"):
        if not isinstance(st, dict):
            continue
        sid = str(st.get("StationId") or "").strip()
        if not re.fullmatch(r"[A-Z0-9]{2,12}", sid):
            continue
        pos = cwa_wgs84(st)
        if pos is None:
            continue
        geo = st.get("GeoInfo") or {}
        stations[sid] = {"name": str(st.get("StationName") or sid).strip() or sid, "lat": round(pos[0], 4),
                         "lon": round(pos[1], 4), "alt": _cwa_num(geo.get("StationAltitude"))}
        t = cwa_time((st.get("ObsTime") or {}).get("DateTime"))
        we = st.get("WeatherElement") or {}
        r = W.reading(t, _cwa_num(we.get("WindSpeed"), 0, W.MAX_SPEED_MS), None, _cwa_num(we.get("WindDirection"), 0, 360))
        if r is None:
            continue
        out[sid] = [W.with_weather(r, at=W._temp(_cwa_num(we.get("AirTemperature"))),
                                   rh=W._pct(_cwa_num(we.get("RelativeHumidity"))))]
    return stations, out


# ============================================================================================== the feeds

class KeyedProvider(M.MoreProvider):
    """A section-69 feed: wind_more's feed with this module's source text and a key (never shown anywhere)."""
    meta = None

    def __init__(self, stations, fetch, key):
        self.meta = SOURCES[self.key]
        super().__init__(stations, fetch)
        self._secret = key

    def _clean(self, exc):
        return redact("%s: %s" % (type(exc).__name__, exc), (self._secret,))

    def _fetch_stations(self):
        try:
            return super()._fetch_stations()
        except Exception as exc:                         # the base class keeps the message: never with a key in it
            raise RuntimeError(self._clean(exc)) from None


class DiscoveredProvider(KeyedProvider):
    """A feed whose stations come from the agency at run time (KNMI, Frost): extra_stations() hands them to the site's
    station list (wind_stations.json plus these), versioned so the list is rebuilt only when it changes."""
    kind = "met"
    tz = "UTC"

    def __init__(self, fetch, key):
        super().__init__({}, fetch, key)
        self._extra_version = 0

    def _set_stations(self, found):
        """found {local: {name, lat, lon}} -> self.stations (snapshot-shaped dicts); bumps the version on a change."""
        new = {}
        for local, s in found.items():
            new[local] = {"id": "%s:%s" % (self.key, local), "name": s["name"], "lat": s["lat"], "lon": s["lon"],
                          "kind": s.get("kind") or self.kind, "src": self.key, "tz": s.get("tz") or self.tz,
                          "alias": None, "wx": None, "net": None}
        if new != self.stations:
            self.stations = new
            self._extra_version += 1

    def extra_stations(self):
        """(version, [station dict]) of the stations found so far."""
        return self._extra_version, [dict(s) for _k, s in sorted(self.stations.items())]


class KnmiProvider(DiscoveredProvider):
    """KNMI: every 2 minutes one list call (the newest KNMI_LIST_N file names), then the newest file not read yet and,
    while the 24 hours are not complete, up to KNMI_BACKFILL older ones (each: one URL call, one download)."""
    source, key, list_ttl_sec, series = "KNMI", "knmi", 120, True
    tz = "Europe/Amsterdam"
    empty_fails = False                          # a refresh with no new file is ordinary

    def __init__(self, fetch, key):
        super().__init__(fetch, key)
        self._read = set()                       # file names read (kept for 25 hours)
        self.headers = knmi_auth(key)

    def _file(self, name):
        doc = json.loads(self._fetch(KNMI_URL.format(name=name), 64 * 1024, self.headers))
        url = doc.get("temporaryDownloadUrl") if isinstance(doc, dict) else None
        if not isinstance(url, str) or not url.startswith("https://"):
            raise ValueError("no download address for %s" % name)
        return self._fetch(url, MAX_FILE, {"User-Agent": W.USER_AGENT})

    def _readings(self):
        now = buoy_sources.time.time()
        names = knmi_file_names(json.loads(self._fetch(KNMI_LIST.format(n=KNMI_LIST_N), 256 * 1024, self.headers)))
        cut = now - M.SERIES_S
        fresh = [n for n in names if n not in self._read and (knmi_file_time(n) or now) >= cut]
        ask = fresh[:1 + KNMI_BACKFILL]
        out, found, errors = {}, dict(self.stations), []
        for name in ask:
            try:
                st, obs = parse_knmi(self._file(name), name)
            except Exception as exc:                     # noqa: BLE001
                errors.append(self._clean(exc))
                continue
            self._read.add(name)
            for k, s in st.items():
                found.setdefault(k, s)
            for k, rows in obs.items():
                out.setdefault(k, []).extend(rows)
        self._read = {n for n in self._read if (knmi_file_time(n) or now) >= now - M.SERIES_S - 3600}
        if ask and len(errors) == len(ask):
            raise RuntimeError("every KNMI file failed (%s)" % errors[-1])
        if found:
            self._set_stations(found)
        return out

    def filling(self):
        """True while the 24 hours are not read yet (fewer than ~140 files since the start)."""
        return len(self._read) < 140


class FrostProvider(DiscoveredProvider):
    """Frost: the sources once a day (kept by the coast mask), the latest readings every 10 minutes in batches."""
    source, key, list_ttl_sec = "FROST", "frost", 600
    tz = "Europe/Oslo"
    empty_fails = False
    sources_ttl = 24 * 3600

    def __init__(self, fetch, key, mask=None):
        super().__init__(fetch, key)
        self.headers = frost_auth(key)
        self.mask = mask
        self._sources_at = 0.0
        self._gust = FROST_GUST                  # dropped (None) when Frost refuses the element

    def _sources(self, now):
        if self.stations and now - self._sources_at < self.sources_ttl:
            return
        found = parse_frost_sources(json.loads(self._fetch(FROST_SOURCES, MAX_JSON, self.headers)))
        mask = self.mask or CoastMask.load(MASK_PATH)
        self.mask = mask
        kept = {}
        for k, s in found.items():
            if mask.keep(s["lat"], s["lon"]):
                s["tz"] = "Arctic/Longyearbyen" if s["lat"] > 74 else "Europe/Oslo"
                kept[k] = s
        if not kept:
            raise RuntimeError("no Frost source inside the coast mask")
        self._set_stations(kept)
        self._sources_at = now

    def _elements(self):
        return "wind_speed,wind_from_direction" + ("," + self._gust if self._gust else "")

    def _batch(self, ids):
        url = FROST_OBS.format(src=",".join(ids), ref="latest", el=self._elements()) + FROST_LATEST
        try:
            return parse_frost_obs(json.loads(self._fetch(url, MAX_JSON, self.headers)), self._gust)
        except Exception as exc:                         # noqa: BLE001
            msg = str(exc)
            if "HTTP 404" in msg or "HTTP 412" in msg:   # no data for these sources now: an ordinary answer
                return {}
            if "HTTP 400" in msg and self._gust:         # the gust element refused: asked again without it
                self._gust = None
                return self._batch(ids)
            raise

    def _readings(self):
        now = buoy_sources.time.time()
        self._sources(now)
        ids = sorted(self.stations)
        out, errors, n = {}, [], 0
        deadline = now + M.REFRESH_S
        for i in range(0, len(ids), FROST_BATCH):
            if buoy_sources.time.time() > deadline:
                break
            n += 1
            try:
                out.update(self._batch(ids[i:i + FROST_BATCH]))
            except Exception as exc:                     # noqa: BLE001
                errors.append(self._clean(exc))
                if "HTTP 429" in str(exc) or "HTTP 403" in str(exc):
                    break
        if n and len(errors) == n:
            raise RuntimeError("every Frost request failed (%s)" % errors[-1])
        return out


class CwaWindProvider(KeyedProvider):
    """CWA: O-A0003-001 every 10 minutes (one request, the key in the URL: redacted everywhere)."""
    source, key, list_ttl_sec, series = "CWA", "cwa", 600, True

    def _readings(self):
        _st, obs = parse_cwa_wind(json.loads(self._fetch(CWA_API.format(id=CWA_WIND, key=self._secret), MAX_JSON,
                                                         W.HEADERS)))
        return obs


def make_providers(groups, fetch, keys, mask=None):
    """The section-69 feeds whose key is set (keys: {source: key}); groups: the snapshot's {src: {local: station}}
    (CWA's stations are in the snapshot; KNMI's and Frost's are found at run time)."""
    out = []
    if keys.get("knmi"):
        out.append(KnmiProvider(fetch, keys["knmi"]))
    if keys.get("frost"):
        out.append(FrostProvider(fetch, keys["frost"], mask))
    if keys.get("cwa") and groups.get("cwa"):
        out.append(CwaWindProvider(groups["cwa"], fetch, keys["cwa"]))
    return out


# ============================================================================================== the windows' history

def history(src, local, since, now, fetch, keys):
    """A Frost station's readings since (ascending): one observations request over the window. Raises W.WindError on
    a failure (its message never holds the key)."""
    if src != "frost" or not keys.get("frost"):
        return None
    url = FROST_OBS.format(src=local, ref="%s/%s" % (W.iso_z(since), W.iso_z(now + 60)),
                           el="wind_speed,wind_from_direction," + FROST_GUST)
    try:
        body = fetch(url, MAX_JSON, frost_auth(keys["frost"]))
    except Exception as exc:                             # noqa: BLE001
        msg = str(exc)
        if "HTTP 404" in msg or "HTTP 412" in msg:
            return []
        if "HTTP 400" in msg:                            # without the gust element
            try:
                body = fetch(FROST_OBS.format(src=local, ref="%s/%s" % (W.iso_z(since), W.iso_z(now + 60)),
                                              el="wind_speed,wind_from_direction"), MAX_JSON, frost_auth(keys["frost"]))
            except Exception as exc2:                    # noqa: BLE001
                raise W.WindError(redact(str(exc2), (keys["frost"],)) or "fetch failed") from None
        else:
            raise W.WindError(redact(msg, (keys["frost"],)) or "fetch failed") from None
    try:
        rows = parse_frost_obs(json.loads(body)).get(local, [])
    except (ValueError, RecursionError, AttributeError, TypeError):
        raise W.WindError("the answer is not JSON") from None
    return [r for r in rows if r["t"] >= since]
