"""More live wind stations (plan section 67): Environment Canada, Japan, US road weather and six European agencies.

The parsers of each agency's answers (pure functions, no network; the snapshot tool tools/wind/more_stations.py and the
feeds share them). Every reading is wind_sources.reading(t, s, g, d) (t epoch seconds; s, g m/s; d degrees FROM, None
when calm or not reported) with the weather fields the report holds (wind_sources.with_weather: at air temperature degC,
dp dew point degC, rh %, p sea-level pressure hPa).

Sources (all anonymous; licences in SOURCES):
- eccc  ECCC MSC Datamart SWOB-ML (dd.weather.gc.ca): one XML file per station and report; the latest folder's listing
        names every file with its time. Wind km/h (10-minute mean at 10 m), gust = the 10 minutes' maximum. An element
        whose qa_summary is below 15 (suppressed -10, missing -1, error 0, doubtful 10: SWOB-ML user guide) is left out.
- jma   JMA AMeDAS (www.jma.go.jp/bosai/amedas): one map file every 10 minutes (JST), m/s and a 16-point direction
        (1 = NNE ... 16 = N, 0 = calm), quality flag beside each value; per-station 3-hour point files with gusts.
- iem   Iowa Environmental Mesonet (mesonet.agron.iastate.edu): currents.json per network (state DOT road weather
        XX_RWIS, airports XX_ASOS), knots; obhistory.json per station for 24 hours.
- smhi  SMHI metobs: latest-hour files per parameter (4 speed m/s, 3 direction, 21 gust), quality G / Y / R (R left out).
- fmi   FMI WFS multipointcoverage (windspeedms, winddirection, windgust), 10-minute steps, positions per step.
- dmi   DMI metObs v2 items (wind_speed / wind_dir: the last 10 minutes' mean; wind_max: their highest 3-second mean).
- dwd   DWD weather_reports/poi <id>-BEOB.csv: semicolon CSV, three header rows, newest first, km/h, "---" missing.
- ipma  IPMA obs-surface.geojson: hourly, m/s, an 8-class direction (1 = N ... 8 = NW; 9 = N as well), -99 missing,
        time UTC.
- mei   Met Eireann prodapi observations/<slug>/today: hourly rows of the day, km/h, local time (Europe/Dublin)."""
import calendar
import csv
import io
import json
import math
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import wind_sources as W

KMH = 1 / 3.6
KT = 0.514444
SOURCES = {
    "eccc": {"name": "Environment and Climate Change Canada", "short": "ECCC", "url": "https://eccc-msc.github.io/open-data/",
             "licence": "Data Source: Environment and Climate Change Canada (ECCC Data Servers End-use Licence)"},
    "jma": {"name": "Japan Meteorological Agency", "short": "JMA", "url": "https://www.jma.go.jp/bosai/amedas/",
            "licence": "Source: Japan Meteorological Agency website (AMeDAS), converted and thinned by allshoresurf.com "
                       "(Public Data License 1.0)"},
    "iem": {"name": "Iowa Environmental Mesonet", "short": "IEM", "url": "https://mesonet.agron.iastate.edu/",
            "licence": "Iowa Environmental Mesonet (Iowa State University): state DOT road weather and airport stations"},
    "smhi": {"name": "SMHI", "short": "SMHI", "url": "https://www.smhi.se/data",
             "licence": "SMHI open data (CC BY 4.0, units converted by allshoresurf.com)"},
    "fmi": {"name": "Finnish Meteorological Institute", "short": "FMI", "url": "https://en.ilmatieteenlaitos.fi/open-data",
            "licence": "Finnish Meteorological Institute open data (CC BY 4.0, units converted by allshoresurf.com)"},
    "dmi": {"name": "DMI", "short": "DMI", "url": "https://www.dmi.dk/friedata",
            "licence": "Danish Meteorological Institute (DMI) open data (CC BY 4.0, units converted by allshoresurf.com)"},
    "dwd": {"name": "Deutscher Wetterdienst", "short": "DWD", "url": "https://opendata.dwd.de/",
            "licence": "Deutscher Wetterdienst (DWD) open data (CC BY 4.0, units converted by allshoresurf.com)"},
    "ipma": {"name": "IPMA", "short": "IPMA", "url": "https://www.ipma.pt/",
             "licence": "IPMA, Instituto Portugues do Mar e da Atmosfera"},
    "mei": {"name": "Met Eireann", "short": "Met Eireann", "url": "https://www.met.ie/",
            "licence": "Met Eireann (CC BY 4.0, units converted by allshoresurf.com)"},
}
KEYS = tuple(SOURCES)
ECCC_DD = "https://dd.weather.gc.ca/today/observations"
ECCC_LATEST = ECCC_DD + "/swob-ml/latest/"
ECCC_STATIONS = ECCC_DD + "/doc/swob-xml_station_list.csv"
ECCC_DAY = ECCC_DD + "/swob-ml/{ymd}/{sid}/"                    # one folder per station and UTC day
JMA = "https://www.jma.go.jp/bosai/amedas"
JMA_LATEST_TIME = JMA + "/data/latest_time.txt"
JMA_MAP = JMA + "/data/map/{stamp}.json"
JMA_POINT = JMA + "/data/point/{sid}/{ymd}_{hh}.json"             # 3-hour files, hh = 00, 03, ... 21 (JST)
JMA_TABLE = JMA + "/const/amedastable.json"
JMA_PAGE = "https://www.jma.go.jp/bosai/amedas/#amdno={sid}"
IEM = "https://mesonet.agron.iastate.edu"
IEM_CURRENTS = IEM + "/api/1/currents.json?network={net}"
IEM_HISTORY = IEM + "/api/1/obhistory.json?station={sid}&network={net}&date={ymd}"
IEM_NETWORKS = IEM + "/api/1/networks.json"
IEM_PAGE = IEM + "/sites/site.php?station={sid}&network={net}"
SMHI = "https://opendata-download-metobs.smhi.se/api/version/1.0/parameter/{p}"
SMHI_LATEST = SMHI + "/station-set/all/period/latest-hour/data.json"
SMHI_DAY = SMHI + "/station/{sid}/period/latest-day/data.json"
SMHI_STATIONS = SMHI.format(p=4) + ".json"
FMI_WFS = ("https://opendata.fmi.fi/wfs?service=WFS&version=2.0.0&request=getFeature&storedquery_id="
           "fmi::observations::weather::multipointcoverage&parameters=windspeedms,winddirection,windgust,temperature,"
           "humidity,pressure&timestep=10")
FMI_BBOX = "19,59,32,71"
FMI_STATIONS = ("https://opendata.fmi.fi/wfs?service=WFS&version=2.0.0&request=getFeature&storedquery_id="
                "fmi::ef::stations&networkid=121")
DMI = "https://opendataapi.dmi.dk/v2/metObs/collections"
DMI_ITEMS = DMI + "/observation/items?parameterId={p}&period=latest-10-minutes&limit=1000"
DMI_DAY = DMI + "/observation/items?parameterId={p}&stationId={sid}&datetime={start}/{end}&limit=500"
DMI_STATIONS = DMI + "/station/items?limit=10000"
DWD_POI = "https://opendata.dwd.de/weather/weather_reports/poi/"
DWD_CSV = DWD_POI + "{sid}-BEOB.csv"
DWD_CATALOG = "https://www.dwd.de/DE/leistungen/met_verfahren_mosmix/mosmix_stationskatalog.cfg?view=nasPublication&nn=16102"
IPMA = "https://api.ipma.pt/open-data/observation/meteorology/stations"
IPMA_OBS = IPMA + "/obs-surface.geojson"
IPMA_HISTORY = IPMA + "/observations.json"
IPMA_STATIONS = IPMA + "/stations.json"
MEI_LIST = "https://www.met.ie/latest-reports/observations/download"
MEI_TODAY = "https://prodapi.metweb.ie/observations/{slug}/today"
# Met Eireann's stations have no published positions in its open data: these are NOAA's (isd-history, CTRY EI). The
# others (Athenry, Dunsany, Knock, Markree, Moore Park, Mt Dillon, Newport, Phoenix Park) are left out (mostly inland).
MEI_POSITIONS = {"Ballyhaise": (54.050, -7.300), "Belmullet": (54.233, -10.000), "Casement": (53.302, -6.451),
                 "Claremorris": (53.717, -8.983), "Cork": (51.841, -8.491), "Dublin": (53.421, -6.270),
                 "Finner": (54.483, -8.233), "Gurteen": (53.017, -8.000), "Johnstown Castle": (52.283, -6.483),
                 "Mace Head": (53.317, -9.900), "Malin Head": (55.367, -7.333), "Mullingar": (53.533, -7.367),
                 "Oak Park": (52.850, -6.900), "Roche's Point": (51.800, -8.250), "Shannon": (52.702, -8.925),
                 "Sherkin Island": (51.467, -9.417), "Valentia": (51.933, -10.250)}
SWOB_QA_OK = 15                                 # suspect/warning 15, inconsistent 20, passed 100 kept
JMA_QUALITY_OK = (0, 1)                         # normal, quasi-normal
JST = timezone(timedelta(hours=9))
DUBLIN = ZoneInfo("Europe/Dublin")
_NUM = re.compile(r"-?\d+(\.\d+)?$")


def _f(v):
    """A finite float from a number or a numeric string, else None."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        x = float(v)
    else:
        s = str(v if v is not None else "").strip().replace(",", ".")
        if not _NUM.match(s):
            return None
        x = float(s)
    return x if math.isfinite(x) else None


def _scaled(v, k):
    x = _f(v)
    return None if x is None else x * k


def _wx(r, at=None, dp=None, rh=None, p=None):
    """The reading with its weather (checked ranges; humidity from the dew point when the report has none)."""
    at, dp = W._temp(at), W._temp(dp)
    rh = W._pct(rh)
    if rh is None:
        rh = W.humidity_from(at, dp)
    return W.with_weather(r, at=at, dp=dp, rh=rh, p=W._hpa(p))


def _epoch_iso(s):
    """Epoch seconds from an ISO time with a zone (Z or +hh:mm), else None."""
    try:
        d = datetime.fromisoformat(str(s).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        return None
    return int(d.timestamp())


def _epoch_naive(s, tz):
    """Epoch seconds from a zone-less ISO time read in `tz`, else None."""
    try:
        d = datetime.fromisoformat(str(s).strip())
    except ValueError:
        return None
    return int(d.replace(tzinfo=tz).timestamp()) if d.tzinfo is None else int(d.timestamp())


# ---------------------------------------------------------------------------------------------- ECCC SWOB-ML

_SWOB_EL = re.compile(r'<element\s+name="([^"]+)"\s+uom="([^"]*)"\s+value="([^"]*)"[^>]*?(/>|>(.*?)</element>)', re.S)
_SWOB_QA = re.compile(r'name="qa_summary"[^>]*?value="(-?\d+)"')
_LISTING = re.compile(r'href="([A-Z0-9]{3,8})-(AUTO|MAN)-swob\.xml">[^<]*</a>\s*(\d{4}-\d\d-\d\d \d\d:\d\d)')


def swob_elements(text):
    """SWOB-ML text -> {element name: (uom, value string, qa or None)} (the first of each name)."""
    out = {}
    for name, uom, value, _end, inner in _SWOB_EL.findall(text):
        if name in out:
            continue
        m = _SWOB_QA.search(inner or "")
        out[name] = (uom, value, int(m.group(1)) if m else None)
    return out


def _swob_value(el, names, unit=None):
    """The first named element with a usable value (qa none or >= SWOB_QA_OK) as a float in SI (km/h, knots -> m/s)."""
    for n in names:
        e = el.get(n)
        if not e or (e[2] is not None and e[2] < SWOB_QA_OK):
            continue
        x = _f(e[1])
        if x is None:
            continue
        u = e[0].lower()
        if unit == "speed":
            x = x * KMH if u == "km/h" else x * KT if u in ("kt", "knots", "knot") else x if u == "m/s" else None
            if x is None:
                continue
        return x
    return None


def parse_swob(text):
    """One SWOB-ML file -> (station id or None, lat, lon, reading or None)."""
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    el = swob_elements(text)
    t = _epoch_iso(el.get("date_tm", ("", ""))[1]) if "date_tm" in el else None
    lat, lon = _f(el.get("lat", ("", ""))[1]), _f(el.get("long", ("", ""))[1])
    sid = (el.get("icao_stn_id") or el.get("tc_id") or ("", ""))[1].strip() or None
    s = _swob_value(el, ("avg_wnd_spd_10m_pst10mts", "avg_wnd_spd_10m_pst2mts"), "speed")
    d = _swob_value(el, ("avg_wnd_dir_10m_pst10mts", "avg_wnd_dir_10m_pst2mts"))
    g = _swob_value(el, ("max_wnd_gst_spd_10m_pst10mts", "max_wnd_spd_10m_pst10mts"), "speed")
    r = W.reading(t, s, g, d) if t is not None else None
    r = _wx(r, at=_swob_value(el, ("air_temp",)), dp=_swob_value(el, ("dwpt_temp",)), rh=_swob_value(el, ("rel_hum",)),
            p=_swob_value(el, ("mslp",)))
    return sid, lat, lon, r


def parse_swob_listing(html):
    """The latest folder's listing -> {station id: {"AUTO" | "MAN": (file name, listed "YYYY-MM-DD HH:MM")}}
    (the -minute- files are not matched)."""
    out = {}
    for sid, kind, when in _LISTING.findall(html):
        out.setdefault(sid, {})[kind] = (f"{sid}-{kind}-swob.xml", when)
    return out


def swob_pick(entry):
    """The file to read for a station: its AUTO (hourly) report, else its MAN one. -> (file, listed) or None."""
    return entry.get("AUTO") or entry.get("MAN")


def parse_eccc_station_list(text):
    """swob-xml_station_list.csv -> {id: {"name", "lat", "lon", "provider", "wmo"}}."""
    out = {}
    for row in csv.DictReader(io.StringIO(text)):
        sid = (row.get("IATA_ID") or "").strip()
        lat, lon = _f(row.get("Latitude")), _f(row.get("Longitude"))
        if not sid or lat is None or lon is None:
            continue
        out[sid] = {"name": (row.get("Name") or sid).strip(), "lat": lat, "lon": lon,
                    "provider": (row.get("Data_Provider") or "").strip(), "wmo": (row.get("WMO_ID") or "").strip()}
    return out


# ---------------------------------------------------------------------------------------------- JMA AMeDAS

JMA_DIR = 22.5


def jma_direction(code):
    """JMA's 16-point code (1 = NNE ... 16 = N) -> degrees; 0 (calm) or anything else -> None."""
    try:
        c = int(code)
    except (TypeError, ValueError):
        return None
    return (c * JMA_DIR) % 360 if 1 <= c <= 16 else None


def _jma_val(e, key):
    v = e.get(key)
    if not isinstance(v, list) or len(v) < 2 or v[1] not in JMA_QUALITY_OK:
        return None
    return _f(v[0])


def jma_time(s):
    """A JMA stamp ("2026-10-31T06:10:00+09:00" or "20261031061000", JST) -> epoch seconds, or None."""
    s = str(s or "").strip()
    if re.fullmatch(r"\d{14}", s):
        try:
            return int(datetime.strptime(s, "%Y%m%d%H%M%S").replace(tzinfo=JST).timestamp())
        except ValueError:
            return None
    return _epoch_iso(s)


def jma_map_name(t):
    """The map file for an epoch: data/map/YYYYMMDDHHMM00.json in JST."""
    return datetime.fromtimestamp(int(t), JST).strftime("%Y%m%d%H%M00")


def _jma_reading(t, e):
    """No gust: JMA's "gust" (with its "gustTime") is the day's highest gust so far, kept from midnight JST, not the
    10-minute gust."""
    s = _jma_val(e, "wind")
    d = jma_direction(e["windDirection"][0]) if isinstance(e.get("windDirection"), list) and \
        e["windDirection"][1:2] and e["windDirection"][1] in JMA_QUALITY_OK else None
    r = W.reading(t, s, None, d)
    return _wx(r, at=_jma_val(e, "temp"), rh=_jma_val(e, "humidity"), p=_jma_val(e, "normalPressure"))


def parse_jma_map(doc, t):
    """A map file (JSON object {station: {element: [value, quality]}}) taken at t -> {station: reading}."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    out = {}
    for sid, e in (doc or {}).items():
        if isinstance(e, dict):
            r = _jma_reading(t, e)
            if r:
                out[str(sid)] = r
    return out


def parse_jma_point(doc):
    """A point file ({"YYYYMMDDHHMMSS": {element: [value, quality]}}, 10-minute rows) -> readings ascending."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    rows = []
    for k, e in (doc or {}).items():
        t = jma_time(k)
        if t is not None and isinstance(e, dict):
            r = _jma_reading(t, e)
            if r:
                rows.append(r)
    return sorted(rows, key=lambda r: r["t"])


def parse_jma_table(doc):
    """amedastable.json -> {station: {"name", "lat", "lon"}} (positions in degrees from [deg, min])."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    out = {}
    for sid, e in (doc or {}).items():
        try:
            lat = float(e["lat"][0]) + float(e["lat"][1]) / 60
            lon = float(e["lon"][0]) + float(e["lon"][1]) / 60
        except (KeyError, TypeError, ValueError, IndexError):
            continue
        out[str(sid)] = {"name": (e.get("enName") or e.get("kjName") or sid).strip(), "lat": round(lat, 4),
                         "lon": round(lon, 4)}
    return out


# ---------------------------------------------------------------------------------------------- IEM

def _iem_reading(e):
    t = _epoch_iso(e.get("utc_valid"))
    if t is None:
        return None
    r = W.reading(t, _scaled(e.get("sknt"), KT), _scaled(e.get("gust"), KT), e.get("drct"))
    f2c = (lambda v: None if _f(v) is None else (_f(v) - 32) * 5 / 9)
    p = _f(e.get("mslp"))
    if p is None and _f(e.get("alti")) is not None:
        p = _f(e.get("alti")) * 33.8639
    return _wx(r, at=f2c(e.get("tmpf")), dp=f2c(e.get("dwpf")), rh=e.get("relh"), p=p)


def parse_iem_currents(doc):
    """currents.json -> {station: (reading or None, entry)} (the entry keeps name / lat / lon / network)."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    out = {}
    for e in (doc or {}).get("data") or []:
        sid = str(e.get("station") or "").strip()
        if sid:
            out[sid] = (_iem_reading(e), e)
    return out


def parse_iem_history(doc):
    """obhistory.json -> readings ascending."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    rows = [r for r in (_iem_reading(e) for e in (doc or {}).get("data") or []) if r]
    return sorted(rows, key=lambda r: r["t"])


# ---------------------------------------------------------------------------------------------- SMHI

SMHI_PARAMS = {"s": 4, "d": 3, "g": 21}


def parse_smhi_param(doc):
    """One parameter's latest-hour (or latest-day) answer -> {station key: [(t, value), ...] ascending} (quality R left
    out)."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    out = {}
    stations = (doc or {}).get("station")
    if isinstance(stations, dict) and "value" in doc:         # a single station's answer (latest-day)
        stations = [{"key": stations.get("key"), "value": doc["value"]}]
    for st in stations or []:
        key = str(st.get("key") or "")
        vals = []
        for v in st.get("value") or []:
            if str(v.get("quality") or "").upper() == "R":
                continue
            x, t = _f(v.get("value")), v.get("date")
            if x is not None and isinstance(t, (int, float)):
                vals.append((int(t // 1000), x))
        if key and vals:
            out[key] = sorted(vals)
    return out


def smhi_readings(s, d, g):
    """The three parameters' {key: [(t, v)]} -> {key: [reading, ...] ascending}, joined on time."""
    out = {}
    for key, rows in s.items():
        dd, gg = dict(d.get(key, ())), dict(g.get(key, ()))
        rs = [W.reading(t, v, gg.get(t), dd.get(t)) for t, v in rows]
        rs = [r for r in rs if r]
        if rs:
            out[key] = rs
    return out


def parse_smhi_stations(doc):
    """parameter/4.json -> {key: {"name", "lat", "lon", "active"}}."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    out = {}
    for st in (doc or {}).get("station") or []:
        lat, lon = _f(st.get("latitude")), _f(st.get("longitude"))
        if lat is not None and lon is not None and st.get("key"):
            out[str(st["key"])] = {"name": str(st.get("name") or st["key"]).strip(), "lat": lat, "lon": lon,
                                   "active": bool(st.get("active"))}
    return out


# ---------------------------------------------------------------------------------------------- FMI

_FMI_POINT = re.compile(r'<gml:Point gml:id="point-(\d+)"[^>]*>.*?<gml:name>([^<]*)</gml:name>\s*<gml:pos>([^<]*)</gml:pos>',
                        re.S)
_FMI_FIELD = re.compile(r'<swe:field\s+name="([^"]+)"')
_FMI_POSITIONS = re.compile(r"<gmlcov:positions>(.*?)</gmlcov:positions>", re.S)
_FMI_VALUES = re.compile(r"<gml:doubleOrNilReasonTupleList>(.*?)</gml:doubleOrNilReasonTupleList>", re.S)


def parse_fmi_obs(text):
    """A multipointcoverage answer -> {fmisid: [reading, ...] ascending} (positions matched to stations by their point
    coordinates)."""
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    pos_to_id = {}
    for fid, _name, pos in _FMI_POINT.findall(text):
        p = pos.split()
        if len(p) >= 2:
            pos_to_id[(p[0], p[1])] = fid
    fields = _FMI_FIELD.findall(text)
    mp, mv = _FMI_POSITIONS.search(text), _FMI_VALUES.search(text)
    if not mp or not mv or not fields:
        return {}
    pos = [ln.split() for ln in mp.group(1).strip().splitlines() if ln.strip()]
    vals = [ln.split() for ln in mv.group(1).strip().splitlines() if ln.strip()]
    ix = {n: i for i, n in enumerate(fields)}
    out = {}
    for p, v in zip(pos, vals):
        if len(p) < 3 or len(v) != len(fields):
            continue
        fid = pos_to_id.get((p[0], p[1]))
        t = _f(p[2])
        if fid is None or t is None:
            continue

        def get(name):
            return _f(v[ix[name]]) if name in ix else None
        r = W.reading(int(t), get("windspeedms"), get("windgust"), get("winddirection"))
        r = _wx(r, at=get("temperature"), dp=get("dewpoint"), rh=get("humidity"), p=get("pressure"))
        if r:
            out.setdefault(fid, []).append(r)
    for fid in out:
        out[fid].sort(key=lambda r: r["t"])
    return out


_FMI_FACILITY = re.compile(r"<ef:EnvironmentalMonitoringFacility.*?</ef:EnvironmentalMonitoringFacility>", re.S)


def parse_fmi_stations(text):
    """fmi::ef::stations -> {fmisid: {"name", "lat", "lon"}}."""
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    out = {}
    for block in _FMI_FACILITY.findall(text):
        fid = re.search(r'stationcode/fmisid">(\d+)<', block)
        name = re.search(r"<ef:name>([^<]*)</ef:name>", block)
        pos = re.search(r"<gml:pos>([^<]*)</gml:pos>", block)
        if not fid or not pos:
            continue
        p = pos.group(1).split()
        lat, lon = _f(p[0]), _f(p[1]) if len(p) > 1 else None
        if lat is not None and lon is not None:
            out[fid.group(1)] = {"name": (name.group(1) if name else fid.group(1)).strip(), "lat": lat, "lon": lon}
    return out


# ---------------------------------------------------------------------------------------------- DMI

DMI_PARAMS = {"s": "wind_speed", "d": "wind_dir", "g": "wind_max"}


def parse_dmi_items(doc):
    """metObs items (one parameter) -> {stationId: [(t, value, lon, lat)] ascending}."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    out = {}
    for f in (doc or {}).get("features") or []:
        p = f.get("properties") or {}
        sid, t, x = str(p.get("stationId") or ""), _epoch_iso(p.get("observed")), _f(p.get("value"))
        c = (f.get("geometry") or {}).get("coordinates") or [None, None]
        if sid and t is not None and x is not None:
            out.setdefault(sid, []).append((t, x, _f(c[0]), _f(c[1])))
    for sid in out:
        out[sid].sort()
    return out


def dmi_readings(s, d, g):
    """The three parameters' answers -> {stationId: [reading, ...] ascending}, joined on time."""
    out = {}
    for sid, rows in s.items():
        dd = {t: v for t, v, *_ in d.get(sid, ())}
        gg = {t: v for t, v, *_ in g.get(sid, ())}
        rs = [W.reading(t, v, gg.get(t), dd.get(t)) for t, v, *_ in rows]
        rs = [r for r in rs if r]
        if rs:
            out[sid] = rs
    return out


def parse_dmi_stations(doc):
    """metObs station collection -> {stationId: {"name", "lat", "lon"}} (the entries valid now: no validTo)."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    out = {}
    for f in (doc or {}).get("features") or []:
        p = f.get("properties") or {}
        c = (f.get("geometry") or {}).get("coordinates") or [None, None]
        sid = str(p.get("stationId") or "")
        if not sid or p.get("validTo") or _f(c[0]) is None or _f(c[1]) is None:
            continue
        out[sid] = {"name": str(p.get("name") or sid).strip(), "lat": _f(c[1]), "lon": _f(c[0]),
                    "params": list(p.get("parameterId") or [])}
    return out


# ---------------------------------------------------------------------------------------------- DWD

DWD_COLS = {"s": "mean_wind_speed_during last_10_min_at_10_meters_above_ground",
            "d": "mean_wind_direction_during_last_10 min_at_10_meters_above_ground",
            "g": "maximum_wind_speed_last_hour",
            "at": "dry_bulb_temperature_at_2_meter_above_ground", "dp": "dew_point_temperature_at_2_meter_above_ground",
            "rh": "relative_humidity", "p": "pressure_reduced_to_mean_sea_level"}


def parse_dwd_poi(text):
    """<id>-BEOB.csv -> readings ascending (rows: date "30.10.26", time "20:00" UTC; comma decimals; "---" missing)."""
    if isinstance(text, bytes):
        text = text.decode("latin-1", "replace")
    lines = text.splitlines()
    if len(lines) < 4:
        return []
    head = [h.strip() for h in lines[0].split(";")]
    ix = {k: head.index(v) for k, v in DWD_COLS.items() if v in head}
    if "s" not in ix:
        return []
    rows = []
    for ln in lines[3:]:
        c = ln.split(";")
        if len(c) < len(head):
            continue
        try:
            t = calendar.timegm(datetime.strptime(c[0].strip() + " " + c[1].strip(), "%d.%m.%y %H:%M").timetuple())
        except ValueError:
            continue

        def get(k, scale=1.0):
            return None if k not in ix else _scaled(c[ix[k]], scale)
        r = W.reading(t, get("s", KMH), get("g", KMH), get("d"))
        r = _wx(r, at=get("at"), dp=get("dp"), rh=get("rh"), p=get("p"))
        if r:
            rows.append(r)
    return sorted(rows, key=lambda r: r["t"])


def _degmin(v):
    """DWD's deg.min ("50.03" = 50 deg 03 min) -> degrees."""
    x = _f(v)
    if x is None:
        return None
    sign = -1 if x < 0 else 1
    x = abs(x)
    d = int(x)
    return sign * (d + round((x - d) * 100) / 60)


def parse_dwd_catalog(text):
    """MOSMIX station catalogue (fixed width: ID ICAO NAME LAT LON ELEV, positions in deg.min) -> {id: {"name", "lat",
    "lon", "icao"}}."""
    out = {}
    for ln in text.splitlines()[2:]:
        if len(ln) < 50:
            continue
        sid, icao, name = ln[0:5].strip(), ln[6:10].strip(), ln[11:31].strip()
        rest = ln[31:].split()
        if len(rest) < 2 or not sid:
            continue
        lat, lon = _degmin(rest[0]), _degmin(rest[1])
        if lat is None or lon is None:
            continue
        out[sid] = {"name": name, "lat": round(lat, 4), "lon": round(lon, 4), "icao": None if icao.startswith("-") else icao}
    return out


_DWD_LISTING = re.compile(r'href="([A-Z0-9]{5})-BEOB\.csv"')


def parse_dwd_listing(html):
    return sorted(set(_DWD_LISTING.findall(html)))


# ---------------------------------------------------------------------------------------------- IPMA

IPMA_DIR = {1: 0, 2: 45, 3: 90, 4: 135, 5: 180, 6: 225, 7: 270, 8: 315, 9: 0}   # 9 = N too (IPMA: descDirVento "N")


def _ipma_val(v):
    x = _f(v)
    return None if x is None or x <= -90 else x


def parse_ipma_obs(doc):
    """obs-surface.geojson -> {idEstacao: [reading, ...] ascending} (time UTC; -99 missing; idDireccVento 1..8 = N..NW,
    9 = N, 0 = calm or not reported)."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    out = {}
    for f in (doc or {}).get("features") or []:
        p = f.get("properties") or {}
        sid = str(p.get("idEstacao") or "")
        t = _epoch_naive(p.get("time"), timezone.utc)
        if not sid or t is None:
            continue
        code = p.get("idDireccVento")
        r = W.reading(t, _ipma_val(p.get("intensidadeVento")), None, IPMA_DIR.get(code if isinstance(code, int) else -1))
        r = _wx(r, at=_ipma_val(p.get("temperatura")), rh=_ipma_val(p.get("humidade")), p=_ipma_val(p.get("pressao")))
        if r:
            out.setdefault(sid, []).append(r)
    for sid in out:
        out[sid].sort(key=lambda r: r["t"])
    return out


def parse_ipma_history(doc):
    """observations.json ({"YYYY-MM-DDTHH:MM": {idEstacao: {...} | null}}, UTC) -> {idEstacao: [reading] ascending}."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    out = {}
    for when, stations in (doc or {}).items():
        t = _epoch_naive(when, timezone.utc)
        if t is None or not isinstance(stations, dict):
            continue
        for sid, p in stations.items():
            if not isinstance(p, dict):
                continue
            code = p.get("idDireccVento")
            r = W.reading(t, _ipma_val(p.get("intensidadeVento")), None,
                          IPMA_DIR.get(code if isinstance(code, int) else -1))
            r = _wx(r, at=_ipma_val(p.get("temperatura")), rh=_ipma_val(p.get("humidade")), p=_ipma_val(p.get("pressao")))
            if r:
                out.setdefault(str(sid), []).append(r)
    for sid in out:
        out[sid].sort(key=lambda r: r["t"])
    return out


def parse_ipma_stations(doc):
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    out = {}
    for f in doc or []:
        p = f.get("properties") or {}
        c = (f.get("geometry") or {}).get("coordinates") or [None, None]
        sid = str(p.get("idEstacao") or "")
        if sid and _f(c[0]) is not None and _f(c[1]) is not None:
            out[sid] = {"name": str(p.get("localEstacao") or sid).strip(), "lat": _f(c[1]), "lon": _f(c[0])}
    return out


# ---------------------------------------------------------------------------------------------- Met Eireann

def mei_slug(name):
    """A station name -> prodapi's slug ("Malin Head" -> "malin-head")."""
    return re.sub(r"[^a-z0-9]+", "-", name.lower().replace("'", "")).strip("-")


def parse_mei_today(doc):
    """prodapi observations/<slug>/today (newest first; km/h; date "30-10-2026" + reportTime "20:00" local) -> readings
    ascending."""
    if isinstance(doc, (bytes, str)):
        doc = json.loads(doc)
    rows = []
    for e in doc if isinstance(doc, list) else []:
        try:
            local = datetime.strptime(f"{e.get('date')} {e.get('reportTime')}", "%d-%m-%Y %H:%M")
        except (TypeError, ValueError):
            continue
        t = int(local.replace(tzinfo=DUBLIN).timestamp())
        r = W.reading(t, _scaled(e.get("windSpeed"), KMH), _scaled(e.get("windGust"), KMH), e.get("windDirection"))
        r = _wx(r, at=e.get("temperature"), rh=e.get("humidity"), p=e.get("pressure"))
        if r:
            rows.append(r)
    return sorted(rows, key=lambda r: r["t"])


# ============================================================================================== the feeds (step 2)

import threading                                 # noqa: E402
from concurrent.futures import ThreadPoolExecutor   # noqa: E402

import buoy_sources                              # noqa: E402

KEEP_S = 6 * 3600                                # a reading kept this long after its station's last report
SERIES_S = W.HISTORY_S + 3600                    # an in-memory 24-hour series is kept this long
AHEAD_S = 600                                    # a reading this far ahead of the server's clock is not believed
REFRESH_S = 90                                   # a paced refresh asks nothing new after this long (G44 P3-8)
BUCKET_PER_MIN = 30                              # each agency's window (history) requests a minute
MAX_LIST = 4 * 1024 * 1024                       # one answer's byte cap (the JMA map is ~250 KB; IPMA's 24 h ~1 MB)
MAX_STATION = 512 * 1024                         # one station's file (SWOB ~10 KB; DWD ~7 KB; IEM history ~100 KB)
HEADERS = W.HEADERS


def _json_body(fetch, url, cap=MAX_LIST, headers=HEADERS):
    return json.loads(fetch(url, cap, headers))


class MoreProvider(W.WindProvider):
    """A section-67 feed: each station keeps its newest reading until a newer one comes or it is KEEP_S old (a station
    missing from one answer keeps it); with `series` every reading also goes into the station's 24 hours in memory
    (W.MadisSeries: the window's history for the agencies with no cheap per-station history: ECCC, IPMA, Met
    Eireann). Subclasses: _readings() -> {local: [reading, ...]} (raises on a failed fetch: keep-last)."""
    license_label = "Open data"
    series = False
    key = ""
    empty_fails = True                           # one answer for every station: one with no reading at all failed

    meta = None                                  # a feed of another module (plan section 69) brings its own source text

    def __init__(self, stations, fetch):
        super().__init__(stations, fetch)
        meta = self.meta or SOURCES[self.key]
        self.source_name, self.source_url = meta["name"], meta["url"]
        lic = meta["licence"]
        self.attribution_text = lic if re.match(r"(Data )?Source:", lic) else "Source: " + lic
        self._kept = {}
        self._hist = {}
        self._hist_lock = threading.Lock()
        self._newest_t = None

    def _readings(self):
        raise NotImplementedError

    def _merge(self, obs, now):
        cut = now - SERIES_S
        with self._hist_lock:
            for local, rows in obs.items():
                if local not in self.stations:
                    continue
                rows = [r for r in rows or () if r["t"] <= now + AHEAD_S]    # a clock far ahead: that row not believed
                if not rows:
                    continue
                newest = max(rows, key=lambda r: r["t"])
                old = self._kept.get(local)
                if old is None or newest["t"] >= old["t"]:
                    self._kept[local] = newest
                if self._newest_t is None or newest["t"] > self._newest_t:
                    self._newest_t = newest["t"]
                if self.series:
                    ser = self._hist.get(local)
                    if ser is None:
                        ser = self._hist[local] = W.MadisSeries()
                    for r in rows:
                        if r["t"] >= cut:
                            ser.add(r)
            for local in [k for k, r in self._kept.items() if r["t"] < now - KEEP_S]:
                del self._kept[local]
            for local in list(self._hist):
                self._hist[local].prune(cut)
                if not len(self._hist[local]):
                    del self._hist[local]

    def _fetch_stations(self):
        now = buoy_sources.time.time()
        obs = self._readings()
        if self.empty_fails and not any(rows for local, rows in (obs or {}).items() if local in self.stations):
            raise RuntimeError("the answer holds no reading of any station (an empty or unexpected answer)")
        self._merge(obs, now)
        with self._hist_lock:
            return [self._entry(local, r) for local, r in sorted(self._kept.items())]

    def hist(self, local, since):
        """The station's readings since, from memory (series feeds), else None."""
        if not self.series:
            return None
        with self._hist_lock:
            ser = self._hist.get(local)
            return ser.rows(since) if ser is not None else []

    def newest_t(self):
        return self._newest_t

    def status(self, now=None):
        s = super().status(now)
        with self._hist_lock:
            s.update(readings=len(self._kept), series_stations=len(self._hist) if self.series else None)
        return s

    def _reset_after_fork(self):
        super()._reset_after_fork()
        self._hist_lock = threading.Lock()


class JmaProvider(MoreProvider):
    """JMA AMeDAS: latest_time.txt, then that time's map file (every station, 10-minute readings, no gust)."""
    source, key, list_ttl_sec = "JMA", "jma", 600

    def _readings(self):
        t = jma_time(self._fetch(JMA_LATEST_TIME, 4096, HEADERS).decode("ascii", "replace"))
        if t is None:
            raise RuntimeError("latest_time.txt holds no time")
        obs = parse_jma_map(_json_body(self._fetch, JMA_MAP.format(stamp=jma_map_name(t))), t)
        return {k: [r] for k, r in obs.items()}


class SmhiProvider(MoreProvider):
    """SMHI: the latest-hour file of each wind parameter (speed, direction, gust) for every station."""
    source, key, list_ttl_sec = "SMHI", "smhi", 1200

    def _readings(self):
        p = {k: parse_smhi_param(_json_body(self._fetch, SMHI_LATEST.format(p=n))) for k, n in SMHI_PARAMS.items()}
        return smhi_readings(p["s"], p["d"], p["g"])


class FmiProvider(MoreProvider):
    """FMI: one WFS answer over Finland for the last 30 minutes (10-minute steps)."""
    source, key, list_ttl_sec = "FMI", "fmi", 600

    def _readings(self):
        start = W.iso_z(buoy_sources.time.time() - 1800)
        return parse_fmi_obs(self._fetch(FMI_WFS + f"&bbox={FMI_BBOX}&starttime={start}", MAX_LIST, HEADERS))


class DmiProvider(MoreProvider):
    """DMI: the latest 10 minutes of each wind parameter (wind_speed, wind_dir, wind_max)."""
    source, key, list_ttl_sec = "DMI", "dmi", 600

    def _readings(self):
        p = {k: parse_dmi_items(_json_body(self._fetch, DMI_ITEMS.format(p=n))) for k, n in DMI_PARAMS.items()}
        return dmi_readings(p["s"], p["d"], p["g"])


class IpmaProvider(MoreProvider):
    """IPMA: observations.json (every station's last 24 hours, hourly): the latest table and the windows' history."""
    source, key, list_ttl_sec, series = "IPMA", "ipma", 1800, True

    def _readings(self):
        return parse_ipma_history(_json_body(self._fetch, IPMA_HISTORY))


class PacedMore(MoreProvider):
    """A feed asking one request per unit (a station, or an IEM network), `per_refresh` units a refresh in turn,
    `workers` at a time; a unit that fails keeps its readings; every request of a refresh failing is a failed fetch
    (keep-last); an HTTP 403 / 429 stops the refresh and pauses the feed pause_s (what answered before it is kept; the
    refused and the unasked units go first after the pause); after REFRESH_S a refresh asks nothing new (those units
    go first next time). Subclasses: units() and one(unit) -> {local: [readings]}."""
    workers = 2
    per_refresh = 8
    pause_s = 300
    refused = ("HTTP 403", "HTTP 429")
    empty_fails = False                          # a refresh asks some units: no new reading is an ordinary answer

    def __init__(self, stations, fetch, per_refresh=None):
        super().__init__(stations, fetch)
        if per_refresh:
            self.per_refresh = max(1, min(200, int(per_refresh)))
        self._cursor = 0
        self._paused_until = 0.0

    def units(self):
        return sorted(self.stations)

    def one(self, unit):
        raise NotImplementedError

    def _readings(self):
        now = buoy_sources.time.time()
        if now < self._paused_until:
            raise RuntimeError("paused after a refusal (%d s left)" % round(self._paused_until - now))
        units = self.units()
        if not units:
            return {}
        start = self._cursor % len(units)
        ask = [units[(start + i) % len(units)] for i in range(min(self.per_refresh, len(units)))]
        self._cursor = (start + len(ask)) % len(units)
        out, errors = {}, []
        stop = threading.Event()
        deadline = buoy_sources.time.time() + REFRESH_S

        def run(u):
            if stop.is_set() or buoy_sources.time.time() > deadline:
                return u, None, None, "skip"
            try:
                return u, self.one(u), None, None
            except Exception as e:                       # noqa: BLE001
                msg = "%s: %s" % (type(e).__name__, e)
                if any(c in msg for c in self.refused):
                    stop.set()
                    return u, None, msg, "refused"
                return u, None, msg, None

        later = []                                       # refused or not asked: these go first next time
        with ThreadPoolExecutor(self.workers) as pool:
            for u, obs, err, what in pool.map(run, ask):
                if what:
                    later.append(u)
                if err:
                    errors.append(err)
                elif what is None:
                    out.update(obs or {})
        if later:
            self._cursor = units.index(later[0])
        refusal = next((e for e in errors if any(c in e for c in self.refused)), None)
        if refusal:
            if out:                                      # what answered before the refusal is kept (G44 P3-4)
                self._merge(out, buoy_sources.time.time())
            self._paused_until = buoy_sources.time.time() + self.pause_s
            raise RuntimeError("the agency refused a request: paused for %d s (%s)" % (self.pause_s, refusal))
        if errors and len(errors) == len(ask):
            raise RuntimeError("every request failed (%s)" % errors[-1])
        return out

    def status(self, now=None):
        s = super().status(now)
        now = buoy_sources.time.time() if now is None else now
        s.update(per_refresh=self.per_refresh, cursor=self._cursor, paused_s=round(max(0.0, self._paused_until - now), 1))
        return s


class IemProvider(PacedMore):
    """IEM: currents.json per network (the snapshot's `net`), 8 networks a refresh every 2 minutes."""
    source, key, list_ttl_sec, per_refresh = "IEM", "iem", 120, 8

    def units(self):
        return sorted({st.get("net") for st in self.stations.values() if st.get("net")})

    def one(self, net):
        cur = parse_iem_currents(_json_body(self._fetch, IEM_CURRENTS.format(net=net)))
        return {sid: [r] for sid, (r, e) in cur.items() if r and (self.stations.get(sid) or {}).get("net") == net}


class DwdProvider(PacedMore):
    """DWD: one poi CSV per station (its last ~24 hourly reports), 15 a refresh every 5 minutes."""
    source, key, list_ttl_sec, per_refresh = "DWD", "dwd", 300, 15

    def one(self, sid):
        rows = parse_dwd_poi(self._fetch(DWD_CSV.format(sid=sid), MAX_STATION, HEADERS))
        return {sid: rows[-3:]} if rows else {}


class MeiProvider(PacedMore):
    """Met Eireann: prodapi observations/<slug>/today per station (the day's hourly rows), kept as the 24 hours."""
    source, key, list_ttl_sec, per_refresh, series = "MEI", "mei", 300, 4, True

    def one(self, sid):
        rows = parse_mei_today(_json_body(self._fetch, MEI_TODAY.format(slug=sid.lower()), MAX_STATION))
        return {sid: rows} if rows else {}


class EcccProvider(MoreProvider):
    """ECCC: the latest folder's listing each refresh (5 minutes), then the snapshot stations whose file is newer than
    the one last read, at most per_refresh a refresh (the oldest first), workers at a time. Each file adds to the
    station's 24 hours in memory (ECCC keeps per-station day folders, but a window would cost ~24 requests)."""
    source, key, list_ttl_sec, series = "ECCC", "eccc", 300, True
    per_refresh = 60
    workers = 2
    empty_fails = False                          # a refresh reads only the files that changed: none is ordinary

    def __init__(self, stations, fetch, per_refresh=None):
        super().__init__(stations, fetch)
        if per_refresh:
            self.per_refresh = max(1, min(500, int(per_refresh)))
        self._seen = {}                                  # station -> the listed time of the file last read

    def _readings(self):
        listing = parse_swob_listing(self._fetch(ECCC_LATEST, MAX_LIST, HEADERS).decode("utf-8", "replace"))
        due = []
        for sid in self.stations:
            pick = swob_pick(listing.get(sid) or {})
            if pick and self._seen.get(sid) != pick[1]:
                due.append((pick[1], sid, pick[0]))
        due.sort()
        out, errors = {}, []
        deadline = buoy_sources.time.time() + REFRESH_S

        def run(item):
            when, sid, f = item
            if buoy_sources.time.time() > deadline:     # not asked: read next time (G44 P3-8)
                return sid, when, None, "skip"
            try:
                _s, _la, _lo, r = parse_swob(self._fetch(ECCC_LATEST + f, MAX_STATION, HEADERS))
                return sid, when, r, None
            except Exception as e:                       # noqa: BLE001
                return sid, when, None, "%s: %s" % (type(e).__name__, e)

        ask = due[:self.per_refresh]
        skipped = 0
        with ThreadPoolExecutor(self.workers) as pool:
            for sid, when, r, err in pool.map(run, ask):
                if err == "skip":
                    skipped += 1
                    continue
                if err:
                    errors.append(err)
                    continue
                self._seen[sid] = when
                if r:
                    out[sid] = [r]
        if errors and len(errors) == len(ask) - skipped:
            raise RuntimeError("every SWOB file failed (%s)" % errors[-1])
        return out


PROVIDERS = {"eccc": EcccProvider, "jma": JmaProvider, "iem": IemProvider, "smhi": SmhiProvider, "fmi": FmiProvider,
             "dmi": DmiProvider, "dwd": DwdProvider, "ipma": IpmaProvider, "mei": MeiProvider}
PACED = ("eccc", "iem", "dwd", "mei")


def make_providers(groups, fetch, enabled=None, per_refresh=None):
    """The section-67 feeds over the snapshot's stations ({src: {local: station}}), one per source with stations;
    enabled: {key: bool} (the env switches; default on); per_refresh: {key: n} for the paced feeds."""
    out = []
    for key, cls in PROVIDERS.items():
        if (enabled or {}).get(key, True) is False or not groups.get(key):
            continue
        n = (per_refresh or {}).get(key)
        out.append(cls(groups[key], fetch, n) if key in PACED else cls(groups[key], fetch))
    return out


# ============================================================================================== the windows' history

SHAPE_ERRORS = (AttributeError, TypeError, KeyError, IndexError, ValueError, RecursionError)


def history(src, local, st, since, fetch, now):
    """A station's readings since (ascending) from its agency, for the sources with a per-station history: JMA (the 3-hour
    point files), IEM (obhistory for each of the station's local days in the window: IEM's day is the station's own),
    SMHI (latest-day per parameter), FMI (one WFS answer), DMI (each parameter over the window), DWD (the poi CSV).
    Raises W.WindError on a failure, also for an answer of an unexpected shape. None for the sources whose history is
    kept in memory (ECCC, IPMA, Met Eireann)."""
    try:
        rows = _history(src, local, st, since, fetch, now)
    except W.WindError:
        raise
    except SHAPE_ERRORS as exc:                          # a 200 answer of another shape (G44 P2-1)
        raise W.WindError("the answer has an unexpected shape (%s)" % type(exc).__name__) from None
    return None if rows is None else [r for r in rows if r["t"] >= since]


def local_days(since, now, tz):
    """The calendar dates in zone tz (an IANA name; UTC when unknown) from since's to now's, oldest first."""
    try:
        z = ZoneInfo(tz) if tz else timezone.utc
    except Exception:                                    # noqa: BLE001
        z = timezone.utc
    d, last = datetime.fromtimestamp(since, z).date(), datetime.fromtimestamp(now, z).date()
    out = []
    while d <= last and len(out) < 4:
        out.append(d)
        d += timedelta(days=1)
    return out


def _history(src, local, st, since, fetch, now):
    def body(url, cap=MAX_STATION):
        try:
            return fetch(url, cap, HEADERS)
        except Exception as exc:                         # noqa: BLE001
            raise W.WindError(str(exc) or "fetch failed") from None

    def js(url, cap=MAX_STATION):
        try:
            return json.loads(body(url, cap))
        except (ValueError, RecursionError):
            raise W.WindError("the answer is not JSON") from None

    if src == "jma":
        rows = []
        end = datetime.fromtimestamp(now, JST)
        first = datetime.fromtimestamp(since, JST)
        h = first.replace(hour=first.hour - first.hour % 3, minute=0, second=0, microsecond=0)
        while h <= end:
            try:
                rows += parse_jma_point(js(JMA_POINT.format(sid=local, ymd=h.strftime("%Y%m%d"), hh=h.strftime("%H"))))
            except W.WindError:
                if h + timedelta(hours=3) <= end:          # only the newest file may not be written yet
                    raise
            h += timedelta(hours=3)
    elif src == "iem":
        net = st.get("net") or ""
        rows = []
        for d in local_days(since, now, st.get("tz")):
            rows += parse_iem_history(js(IEM_HISTORY.format(sid=local, net=net, ymd=d.isoformat()), MAX_LIST))
    elif src == "smhi":
        p = {}
        for k, n in SMHI_PARAMS.items():
            try:
                p[k] = parse_smhi_param(js(SMHI_DAY.format(p=n, sid=local)))
            except W.WindError:
                if k == "s":
                    raise
                p[k] = {}
        joined = smhi_readings(p["s"], p["d"], p["g"])
        rows = joined.get(local) or (next(iter(joined.values())) if len(joined) == 1 else [])
    elif src == "fmi":
        url = FMI_WFS + f"&fmisid={local}&starttime={W.iso_z(since)}"
        rows = parse_fmi_obs(body(url, MAX_LIST)).get(local, [])
    elif src == "dmi":
        p = {}
        for k, n in DMI_PARAMS.items():
            p[k] = parse_dmi_items(js(DMI_DAY.format(p=n, sid=local, start=W.iso_z(since), end=W.iso_z(now)), MAX_LIST))
        rows = dmi_readings(p["s"], p["d"], p["g"]).get(local, [])
    elif src == "dwd":
        rows = parse_dwd_poi(body(DWD_CSV.format(sid=local)))
    else:
        return None
    return rows
