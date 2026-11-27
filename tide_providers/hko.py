"""Hong Kong Observatory tide predictions for the Tide layer (plan section 112, step 3).

Source: HKO's open data API (DATA.GOV.HK), https://data.weather.gov.hk/weatherAPI/opendata/opendata.php
(documentation: https://data.weather.gov.hk/weatherAPI/doc/HKO_Open_Data_API_Documentation.pdf):

    dataType=HLT   times and heights of the astronomical high and low tides
                   {"fields": ["Month", "Date", "Time", "Height(m)", x4], "data": [["11", "27", "0522", "0.37", ...]]}
                   up to 4 (time, height) pairs a day, "" where there are fewer; NOT marked high or low (we tell them
                   apart by their neighbours, and by the hourly heights where the neighbours do not settle it)
    dataType=HHOT  hourly heights {"fields": ["MM", "DD", "01", ..., "24"], "data": [["11", "01", "2.54", ...]]}:
                   column h is h:00 of that day, "24" the next day's 00:00 (HKO's own chart reads them so)

with station=<code>&year=<YYYY> (month and day optional: a whole year is one request, so a fetch is 2 requests, 4
around New Year). Times are Hong Kong Time (UTC+8), heights metres above Chart Datum (0.146 m below the Hong Kong
Principal Datum; close to the lowest astronomical tide). An unknown station or year is answered with an HTML message
and HTTP 200. The documentation says years 2022-2024; the service answers 2025-2028 too (checked 2026-11-26).

Stations: the 15 of HKO's own prediction page (www.hko.gov.hk/en/tide/predtide.htm), read from its script
(files/predtide_uc.js: code, decimal position, English and Chinese names); each is asked once for a day of highs and
lows and left out if it has none. Chi Ma Wan and Lok On Pai: their gauges closed in 1997 and 1999 and HKO warns its
predictions there may have large errors; that is said in their names.

Licence: DATA.GOV.HK terms of use: identify the source, acknowledge the Government's intellectual property, give
proper attribution to the Government, the data provider (HKO) and DATA.GOV.HK."""
import json
import re
import time
from datetime import datetime, timedelta, timezone

from . import TideFetchError

PREFIX = "hko"
NAME = "Hong Kong Observatory"
CREDIT = ("Tide predictions: Hong Kong Observatory, Government of the Hong Kong SAR (intellectual property of the "
          "Government), via DATA.GOV.HK; times converted to UTC by allshoresurf.com.")
DATUM = "chart datum (≈ LAT)"
MAX_REQUESTS = 4                      # highs/lows + hourly heights, for one year (two around New Year)

API = "https://data.weather.gov.hk/weatherAPI/opendata/opendata.php"
LIST_URL = "https://www.hko.gov.hk/en/tide/files/predtide_uc.js"
PAGE_URL = "https://www.hko.gov.hk/en/tide/predtide.htm?s={code}"
UA = "allshoresurf.com tide stations (https://allshoresurf.com)"
HKT = timezone(timedelta(hours=8))
CURVE_PAD_S = 3 * 3600                # hourly heights kept this far outside [begin, end] (the grid's cubic needs them)
CODE_RE = re.compile(r"[A-Z0-9]{3}")
# CLK: { latlon: [113.945277,22.319999], name: "Chek Lap Kok (E)",  nameUC: "赤鱲角(東)", ...
STN_RE = re.compile(r'\b([A-Z0-9]{3})\s*:\s*\{\s*latlon\s*:\s*\[\s*([-0-9.]+)\s*,\s*([-0-9.]+)\s*\]\s*,'
                    r'\s*name\s*:\s*"([^"]*)"(?:\s*,\s*nameUC\s*:\s*"([^"]*)")?')
CLOSED = {"CMW": "gauge closed 1997", "LOP": "gauge closed 1999"}   # HKO: "large errors may exist"


def _get(get, url):
    try:
        return get(url, headers={"User-Agent": UA})
    except Exception as exc:                          # any transport failure or non-200 answer
        raise TideFetchError("HKO could not be reached (%s)" % type(exc).__name__) from exc


def url(data_type, code, year, month=None, day=None):
    q = "%s?dataType=%s&lang=en&rformat=json&station=%s&year=%d" % (API, data_type, code, int(year))
    if month is not None:
        q += "&month=%d" % int(month)
        if day is not None:
            q += "&day=%d" % int(day)
    return q


def _rows(body):
    try:
        doc = json.loads(body)
    except (ValueError, TypeError):
        raise TideFetchError("HKO: not a JSON answer (an unknown station or year?)") from None
    rows = doc.get("data") if isinstance(doc, dict) else None
    if not isinstance(rows, list) or not isinstance(doc.get("fields"), list):
        raise TideFetchError("HKO: unexpected answer")
    return rows


def _midnight(year, mm, dd):
    try:
        return int(datetime(int(year), int(mm), int(dd), tzinfo=HKT).timestamp())
    except (TypeError, ValueError):
        raise TideFetchError("HKO: no such date %r-%r-%r" % (year, mm, dd)) from None


def _metres(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        raise TideFetchError("HKO: unreadable height %r" % (s,)) from None


def parse_hourly(body, year):
    """HHOT for one year -> [(epoch, m)] in time order. Column "01".."24" is that hour of the day (HKT)."""
    out = []
    for r in _rows(body):
        if not isinstance(r, list) or len(r) != 26:
            raise TideFetchError("HKO hourly heights: a row of %s columns" % (len(r) if isinstance(r, list) else "?"))
        t0 = _midnight(year, r[0], r[1])
        for h in range(1, 25):
            v = r[1 + h]
            if v is None or str(v).strip() == "":
                continue
            out.append((t0 + h * 3600, _metres(v)))
    out.sort()
    return out


def parse_extremes(body, year):
    """HLT for one year -> [(epoch, m)] in time order (not yet marked high or low)."""
    out = []
    for r in _rows(body):
        if not isinstance(r, list) or len(r) < 2 or len(r) % 2:
            raise TideFetchError("HKO highs and lows: an unexpected row")
        t0 = _midnight(year, r[0], r[1])
        for i in range(2, len(r), 2):
            hhmm, v = str(r[i] or "").strip(), str(r[i + 1] or "").strip()
            if not hhmm and not v:
                continue                               # fewer than 4 highs and lows this day
            if not re.fullmatch(r"\d{4}", hhmm) or int(hhmm[:2]) > 24 or int(hhmm[2:]) > 59:
                raise TideFetchError("HKO highs and lows: no such time %r" % hhmm)
            out.append((t0 + (int(hhmm[:2]) * 60 + int(hhmm[2:])) * 60, _metres(v)))
    out.sort()
    return out


def _interp(curve, t):
    """The hourly curve at t (linear), or None outside it or across a gap of more than 2 h."""
    lo, hi = 0, len(curve) - 1
    if not curve or t < curve[0][0] or t > curve[-1][0]:
        return None
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if curve[mid][0] <= t:
            lo = mid
        else:
            hi = mid
    (ta, va), (tb, vb) = curve[lo], curve[hi]
    if tb == ta:
        return va
    if tb - ta > 7200:
        return None
    return va + (vb - va) * (t - ta) / (tb - ta)


def classify(extremes, curve=()):
    """[(t, m)] -> [(t, m, 'H'|'L')]: higher than both neighbours = high, lower than both = low; where the neighbours
    do not settle it (an equal one, or a sequence that does not alternate), the hourly heights an hour either side
    do; an extreme neither settles is left out. (The first and last have one neighbour, which settles them.)"""
    out = []
    n = len(extremes)
    for i, (t, h) in enumerate(extremes):
        nb = [extremes[j][1] for j in (i - 1, i + 1) if 0 <= j < n]
        kind = None
        if nb and all(h > x for x in nb):
            kind = "H"
        elif nb and all(h < x for x in nb):
            kind = "L"
        else:
            side = [_interp(curve, t - 3600), _interp(curve, t + 3600)]
            if None not in side:
                kind = "H" if all(h > x for x in side) else "L" if all(h < x for x in side) else None
        if kind is not None:
            out.append((t, h, kind))
    return out


def years_for(begin, end):
    a = datetime.fromtimestamp(begin - CURVE_PAD_S, HKT).year
    z = datetime.fromtimestamp(end + CURVE_PAD_S, HKT).year
    return list(range(a, z + 1))


def fetch(st, begin, end, get):
    code = str(st["local"]["code"]).upper()
    if not CODE_RE.fullmatch(code):
        raise TideFetchError("not an HKO station code: %r" % code)
    curve, ext = [], []
    for year in years_for(begin, end):
        ext += parse_extremes(_get(get, url("HLT", code, year)), year)
        curve += parse_hourly(_get(get, url("HHOT", code, year)), year)
    curve = sorted(set(curve))
    ext = sorted(set(ext))
    if not curve or not ext:
        raise TideFetchError("HKO: no predictions in the answer")
    hilo = classify(ext, curve)
    curve = [p for p in curve if begin - CURVE_PAD_S <= p[0] <= end + CURVE_PAD_S]
    hilo = [p for p in hilo if begin <= p[0] <= end]
    if not curve:
        raise TideFetchError("HKO: nothing inside the window")
    return {"curve": curve, "hilo": hilo}


# ------------------------------------------------------------------------------------------- the station list

def parse_station_script(body):
    """predtide_uc.js -> [(code, lat, lon, name, name_zh)] (its STN table: latlon is [lon, lat])."""
    text = body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else str(body)
    out, seen = [], set()
    for code, lon, lat, name, zh in STN_RE.findall(text):
        try:
            lat, lon = float(lat), float(lon)
        except ValueError:
            continue
        if code in seen or not (21.5 <= lat <= 23.0 and 113.5 <= lon <= 114.7) or not name.strip():
            continue
        seen.add(code)
        out.append((code, lat, lon, name.strip(), (zh or "").strip()))
    return out


def build_station_list(get, year=None):
    year = int(year or time.gmtime().tm_year)
    found = parse_station_script(_get(get, LIST_URL))
    if not found:
        raise TideFetchError("HKO station list: no stations read")
    out = []
    for code, lat, lon, name, zh in found:
        try:                                           # one day of highs and lows: does HKO predict this station?
            if not parse_extremes(get(url("HLT", code, year, 1, 1), headers={"User-Agent": UA}), year):
                continue
        except Exception:
            continue
        if code in CLOSED:                             # HKO: "large errors may exist" (plan section 112: not shown)
            continue
        local = {"code": code}
        if zh:
            local["name_zh"] = zh
        out.append({"id": "%s:%s" % (PREFIX, code.lower()), "name": name, "lat": round(lat, 5), "lon": round(lon, 5),
                    "local": local})
    return out


def page_url(st):
    return PAGE_URL.format(code=str(st["local"]["code"]).upper())
