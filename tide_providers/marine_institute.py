"""Ireland: the Marine Institute's tide predictions on its ERDDAP server (plan section 112, step 3).

Source: https://erddap.marine.ie/erddap/tabledap/ (no key). Two datasets (checked 2026-11-27, both 2026-01-01 to
2029-01-01):

- IMI_TidePrediction_HighLow: the highs and lows, stationID / time (UTC, to the nearest 5 minutes) / tide_time_category
  ("HIGH" | "LOW") / Water_Level_ODMalin (metres relative to Ordnance Datum Malin, so most lows are negative). 38
  stations, ids like "Lahinch".
- imiTidePrediction: the curve every 5 minutes at the same 38 places, Water_Level ("sea surface height above local
  LAT", centimetre steps) and Water_Level_ODM (OD Malin). Its ids carry "_MODELLED" where the predictions come from the
  MI's hydrodynamic model rather than a tide gauge ("Lahinch_MODELLED"); positions are the same. orderByClosest
  ("time/30minutes") thins it on the server to the half hours (35 days: ~60 KB instead of ~380 KB).
- Every query must be percent-encoded (a raw > or " is HTTP 400). No rows is HTTP 404 ("Your query produced no
  matching results"), as is an unknown station.

Datum: Water_Level - Water_Level_ODM is a constant per station: the MI's own height of its local LAT above OD Malin
(Lahinch 2.68 m, Galway 2.949, Arklow 0.891). build_station_list reads it once (the mean over 3 hours of 5-minute rows,
"local": {"offset"}) and fetch() adds it to every OD Malin height, so highs, lows and curve are all metres above the
MI's LAT. For a station the curve dataset lacked, the fallback is minus its lowest predicted low of LOW_YEAR (the
instruction of plan section 112 for this agency); both numbers are kept in "local". Measured 2026-11-27: the MI's LAT
lies 0-17 cm below the lowest low of 2027 at 35 of 38 stations (LAT is the lowest tide of 18.6 years), and 2-3 cm above
it at three modelled ones (Crosshaven, Kinsale, Dungarvan), so a 2027 low there reads -0.03 m, as the MI's own
Water_Level does.

Terms: CC BY 4.0 (the datasets' licence attribute).
"""
import csv
import io
import math
from datetime import datetime, timezone
from urllib.parse import quote

from tide_providers import TideFetchError

PREFIX = "mi"
NAME = "Marine Institute (Ireland)"
CREDIT = "Tide predictions: Marine Institute, Ireland, licensed under CC BY 4.0."
DATUM_MODE = "agency"                  # "agency": the MI's own local LAT; "lowest": minus the lowest low of LOW_YEAR
DATUM = "LAT (Marine Institute)" if DATUM_MODE == "agency" else "≈ LAT (lowest predicted tide of a year)"
MAX_REQUESTS = 2                       # the highs and lows + the curve

BASE = "https://erddap.marine.ie/erddap/tabledap/"
HILO_DS = "IMI_TidePrediction_HighLow"
CURVE_DS = "imiTidePrediction"
HEADERS = {"User-Agent": "allshoresurf.com tide stations (https://allshoresurf.com)"}
LOW_YEAR = 2027                        # the full year whose lowest low is the fallback datum
LAT_SPAN = ("2027-01-01T00:00:00Z", "2027-01-01T03:00:00Z")   # the agency LAT is read over these 3 hours (5-min rows)
CURVE_STEP_S = 1800
KIND = {"HIGH": "H", "LOW": "L"}
MODELLED = "_MODELLED"


def _get(get, url):
    try:
        return get(url, headers=HEADERS)
    except TideFetchError:
        raise
    except Exception as exc:                             # a status (404 = no rows), a timeout ...
        raise TideFetchError("Marine Institute could not be read: %s" % type(exc).__name__) from exc


def stamp(epoch):
    return datetime.fromtimestamp(int(epoch), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def query(dataset, variables, *constraints):
    """An ERDDAP tabledap .csv URL, every constraint percent-encoded ('time>=2026-11-27T00:00:00Z',
    'stationID="Lahinch"', 'orderByMin("stationID,Water_Level_ODMalin")')."""
    return BASE + dataset + ".csv?" + ",".join(variables) + "".join(
        "&" + quote(c, safe="=") for c in constraints)


def parse_time(text):
    return int(datetime.strptime(text.strip(), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())


def rows(body, columns):
    """An ERDDAP .csv answer (header, units row, data) -> [dict] with the asked columns; a missing column or anything
    that is not that CSV raises."""
    try:
        text = body.decode("utf-8")
    except (AttributeError, UnicodeDecodeError) as exc:
        raise TideFetchError("Marine Institute answered something that is not CSV") from exc
    r = list(csv.reader(io.StringIO(text)))
    if len(r) < 2 or any(c not in r[0] for c in columns):
        raise TideFetchError("Marine Institute answered an unexpected table")
    ix = [r[0].index(c) for c in columns]
    return [dict(zip(columns, (row[i] for i in ix))) for row in r[2:] if len(row) >= len(r[0])]


def _num(text):
    v = float(text)
    if not math.isfinite(v) or not -20 < v < 20:
        raise ValueError(text)
    return v


# ---------------------------------------------------------------------------------------------- the station list

def _stations(get, dataset):
    out = {}
    for r in rows(_get(get, query(dataset, ["stationID", "longitude", "latitude"], "distinct()")),
                  ["stationID", "longitude", "latitude"]):
        try:
            la, lo = float(r["latitude"]), float(r["longitude"])
        except ValueError:
            continue
        if math.isfinite(la) and math.isfinite(lo):
            out[r["stationID"].strip()] = (la, lo)
    return out


def _lowest(get, year):
    """{HighLow stationID: lowest predicted low of the year (OD Malin)}: one request (orderByMin on the server)."""
    url = query(HILO_DS, ["stationID", "time", "Water_Level_ODMalin"], "time>=%d-01-01T00:00:00Z" % year,
                "time<%d-01-01T00:00:00Z" % (year + 1), 'tide_time_category="LOW"',
                'orderByMin("stationID,Water_Level_ODMalin")')
    out = {}
    for r in rows(_get(get, url), ["stationID", "Water_Level_ODMalin"]):
        try:
            out[r["stationID"].strip()] = _num(r["Water_Level_ODMalin"])
        except ValueError:
            continue
    return out


def _agency_lat(get):
    """{curve stationID: the MI's local LAT above OD Malin}: the mean of Water_Level - Water_Level_ODM over LAT_SPAN
    (one request, every station). Both are rounded to the centimetre, so one difference is the constant +- 1 cm
    (Galway: 2.94 or 2.95); over 37 samples the mean finds it to a few millimetres."""
    cols = ["stationID", "Water_Level", "Water_Level_ODM"]
    seen = {}
    for r in rows(_get(get, query(CURVE_DS, cols, "time>=" + LAT_SPAN[0], "time<=" + LAT_SPAN[1])), cols):
        try:
            seen.setdefault(r["stationID"].strip(), []).append(_num(r["Water_Level"]) - _num(r["Water_Level_ODM"]))
        except ValueError:
            continue
    return {k: round(sum(v) / len(v), 3) for k, v in seen.items() if v}


def build_station_list(get):
    """The 38 stations of the highs-and-lows dataset with their curve id (the same id or id + "_MODELLED", else the
    curve station at the same position) and the datum offset."""
    hl = _stations(get, HILO_DS)
    curve = _stations(get, CURVE_DS)
    low = _lowest(get, LOW_YEAR)
    lat = _agency_lat(get)
    out, seen = [], set()
    for sid, (la, lo) in sorted(hl.items()):
        cid = next((c for c in (sid, sid + MODELLED) if c in curve), None)
        if cid is None:
            cid = next((c for c, p in curve.items() if abs(p[0] - la) < 1e-3 and abs(p[1] - lo) < 1e-3), None)
        local_id = sid.lower()
        if not (-90 <= la <= 90 and -180 <= lo <= 180) or local_id in seen or sid not in low:
            continue                                     # no position, or no predicted lows in LOW_YEAR
        agency = lat.get(cid) if cid else None
        lowest = round(-low[sid], 3)
        offset = agency if (DATUM_MODE == "agency" and agency is not None) else lowest
        seen.add(local_id)
        out.append({"id": "%s:%s" % (PREFIX, local_id), "name": sid.replace("_", " "), "lat": round(la, 5),
                    "lon": round(lo, 5),
                    "local": {"station": sid, "curve": cid, "offset": offset,
                              "datum": "agency LAT" if offset == agency else "lowest low %d" % LOW_YEAR,
                              "lat_odm": agency, "lowest_low_odm": low[sid], "low_year": LOW_YEAR}})
    out.sort(key=lambda s: s["id"])
    return out


def page_url(st):
    """The ERDDAP table of the station's highs and lows from yesterday to a week ahead."""
    sid = (st.get("local") or {}).get("station")
    if not sid:
        return None
    return (BASE + HILO_DS + ".htmlTable?time,tide_time_category,Water_Level_ODMalin&"
            + quote('stationID="%s"' % sid, safe="=") + "&time%3E=now-1day&time%3C=now%2B7days")


# ---------------------------------------------------------------------------------------------- fetch

def parse_hilo(body, offset):
    out = {}
    for r in rows(body, ["time", "tide_time_category", "Water_Level_ODMalin"]):
        k = KIND.get(r["tide_time_category"].strip().upper())
        try:
            t, v = parse_time(r["time"]), _num(r["Water_Level_ODMalin"])
        except ValueError:
            continue                                     # NaN or an empty cell: skipped
        if k:
            out[t] = (t, round(v + offset, 3), k)
    return [out[t] for t in sorted(out)]


def parse_curve(body, offset):
    out = {}
    for r in rows(body, ["time", "Water_Level_ODM"]):
        try:
            t, v = parse_time(r["time"]), _num(r["Water_Level_ODM"])
        except ValueError:
            continue
        if t % CURVE_STEP_S == 0:                        # only the half hours (a nearest-sample stand-in is dropped)
            out[t] = (t, round(v + offset, 3))
    return [out[t] for t in sorted(out)]


def fetch(st, begin, end, get):
    """The MI's highs and lows and its half-hourly curve covering [begin, end] (two requests; one when the station has
    no curve), metres above the station's LAT."""
    loc = st.get("local") or {}
    sid, cid = loc.get("station"), loc.get("curve")
    try:
        offset = float(loc["offset"])
    except (KeyError, TypeError, ValueError):
        raise TideFetchError("no Marine Institute datum offset") from None
    if not sid or not math.isfinite(offset):
        raise TideFetchError("no Marine Institute station")
    begin, end = int(begin), int(end)
    if not 0 < end - begin <= 400 * 86400:
        raise TideFetchError("Marine Institute: the range must be 0 to 400 days")
    span = ("time>=" + stamp(begin), "time<=" + stamp(end))
    hilo = parse_hilo(_get(get, query(HILO_DS, ["time", "tide_time_category", "Water_Level_ODMalin"],
                                      'stationID="%s"' % sid, *span)), offset)
    if len(hilo) < 2 or hilo[0][0] - begin > 14 * 3600 or end - hilo[-1][0] > 14 * 3600:
        raise TideFetchError("Marine Institute's highs and lows do not cover the window")
    curve = []
    if cid:
        curve = parse_curve(_get(get, query(CURVE_DS, ["time", "Water_Level_ODM"], 'stationID="%s"' % cid, *span,
                                            'orderByClosest("time/30minutes")')), offset)
        if not curve or curve[0][0] - begin > 3600 or end - curve[-1][0] > 3600:
            raise TideFetchError("Marine Institute's curve does not cover the window")
    return {"curve": curve, "hilo": hilo}
