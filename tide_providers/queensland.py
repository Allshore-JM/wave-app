"""Queensland: Maritime Safety Queensland's tide predictions on the Queensland Government open data portal (plan
section 112, step 3).

What exists (checked 2026-11-27):

- apps.des.qld.gov.au/data-sets/storm-tides/stdtide-7dayopdata.csv (and tide-7dayopdata.csv, 58 storm-tide sites): the
  Department of the Environment's last 7 days of observed / predicted / residual water level every 10 minutes, -99 =
  missing. It ends at the time it is made, so it is no use for a forecast: not used.
- data.qld.gov.au, organisation Transport and Main Roads (MSQ): per gauge two datasets, "<place> tide gauge predicted
  interval data" (78) and "<place> tide gauge predicted high/low data" (77), each with one CSV per year (2026 and
  2027 for 77 gauges; Military Jetty ends with 2025 and Half Tide Tug Harbour has no published position, so 76 are
  used). The file names are irregular (p101584_abbot-point_2026_10min.csv, p101584_abbot-point-miros-gauge_2027_...),
  so the list of resources is read from the CKAN API (package_search), one id per year.
- The files' download links answer HTTP 202 with an AWS WAF browser challenge (x-amzn-waf-action: challenge) to any
  script, so they are not used. The same rows are in the portal's DataStore (every 2026-2027 resource is
  datastore_active), served without a challenge by the CKAN API:
  /api/3/action/datastore_search?resource_id=...&filters={"Date": [...]}&fields=...&records_format=csv. Interval rows:
  Date "dd/mm/yyyy", Time "HH:MM", Ind "9", Reading "0.794" (metres), every 10 minutes (52,560 rows a year). High/low
  rows: Ind "1" high, "-1" low. Filtering on the window's dates keeps a 35-day answer near 110 KB (curve) and 7 KB
  (highs and lows).
- Times: AEST, UTC+10, no daylight saving (the description files: "Time Zone,UTC+10 (AEST)"; the tide tables: "Time
  Zone -1000"). Heights: metres above Lowest Astronomical Tide ("Station Datum,Lowest Astronomical Tide (LAT)"; the
  tide tables: "Datum of Predictions is Lowest Astronomical Tide", Queensland Port Datum = LAT(1992)).
- Positions: not in the datasets (19 description files give degrees and minutes; the rest give none). POSITIONS below
  is transcribed from MSQ's own publications: the 2027 Queensland Tide Tables and the 2027 supplementary tide
  predictions ("blue books"), each port's heading "LAT 19° 51' S LONG 148° 07' E" (to the minute, ~1.8 km).

Highs and lows: MSQ's published ones (the high/low datasets). Only for a year without a published high/low file are
they derived from the 10-minute curve (local extremes, parabolic refinement; see extremes()).

Terms: CC BY 4.0 (every dataset's licence). The predictions are computed by the Bureau of Meteorology for MSQ; the
description files carry the Bureau's notice, kept in CREDIT.
"""
import csv
import io
import json
import math
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

from tide_providers import TideFetchError

PREFIX = "qld"
NAME = "Maritime Safety Queensland"
CREDIT = ("Tide predictions © State of Queensland (Department of Transport and Main Roads, Maritime Safety "
          "Queensland), licensed under CC BY 4.0. Based on Bureau of Meteorology information that has subsequently "
          "been modified.")
DATUM = "LAT"
MAX_REQUESTS = 4                       # per calendar year the window touches (at most 2): the curve + the highs/lows

API = "https://www.data.qld.gov.au/api/3/action/"
DATASET_PAGE = "https://www.data.qld.gov.au/dataset/"
INTERVAL_SUFFIX = "-tide-gauge-predicted-interval-data"
HILO_SUFFIX = "-tide-gauge-predicted-high-low-data"
HEADERS = {"User-Agent": "allshoresurf.com tide stations (https://allshoresurf.com)"}
AEST = timezone(timedelta(hours=10))
STEP_S = 600                           # the published interval
ROWS_MAX = 10000                       # a request's row cap (a 37-day year piece is 5,328 rows)
MIN_RANGE_M = 0.05                     # a derived high-low pair smaller than this is a wiggle of the 1-mm rounding
YEAR_RE = re.compile(r"\b(20\d\d)\b")

# The gauge's position (degrees, minutes south; degrees, minutes east) from MSQ's 2027 tide tables and blue books, by
# CKAN dataset name without the suffix. A dataset not listed here is left out.
POSITIONS = {
    "abbot-point": (19, 51, 148, 7), "amrun": (12, 55, 141, 37), "aurukun-archer-river": (13, 22, 141, 43),
    "badu-island": (10, 10, 142, 10), "boigu-island": (9, 13, 142, 13), "booby-island": (10, 36, 141, 54),
    "bowen": (20, 1, 148, 15), "brisbane-bar": (27, 21, 153, 10), "bugatti-reef-outer": (20, 4, 150, 18),
    "bundaberg": (24, 46, 152, 22), "burnett-heads": (24, 45, 152, 24), "cairns-beacon-c1": (16, 49, 145, 49),
    "cairns": (16, 55, 145, 46), "cape-ferguson": (19, 16, 147, 3), "cape-flattery": (14, 57, 145, 18),
    "cardwell": (18, 15, 146, 2), "clump-point": (17, 51, 146, 6), "coconut-island-poruma": (10, 2, 143, 3),
    "cooktown": (15, 27, 145, 15), "darnley-island-erub": (9, 35, 143, 45), "dauan-island": (9, 24, 142, 32),
    "deep-water-bend-pine-river": (27, 17, 153, 2), "fisherman-s-landing": (23, 47, 151, 10),
    "gladstone-auckland-point": (23, 49, 151, 15), "gold-coast-seaway": (27, 56, 153, 25),
    "golding-reciprocal-f-l-gladstone": (23, 56, 151, 27), "goods-island": (10, 34, 142, 8),
    "hammond-island": (10, 33, 142, 13), "hay-point": (21, 16, 149, 18),
    "inscription-point-sweers-island": (17, 6, 139, 35), "karumba-bar": (17, 25, 140, 43), "karumba": (17, 29, 140, 49),
    "kingfisher-bay-jetty": (25, 23, 153, 1), "kubin-moa-island": (10, 14, 142, 12),
    "leggatt-island": (14, 31, 144, 50),
    "lizard-island": (14, 40, 145, 26), "lucinda": (18, 31, 146, 23), "mabuiag-island": (9, 57, 142, 12),
    "mackay": (21, 7, 149, 13), "mooloolaba": (26, 41, 153, 8), "mornington-island": (16, 40, 139, 10),
    "mossman": (16, 25, 145, 24), "mourilyan": (17, 35, 146, 7), "murray-island-meer": (9, 54, 144, 2),
    "no-2-beacon-weipa": (12, 41, 141, 42), "noosa-head": (26, 22, 153, 6),
    "north-cardinal-beacon-townsville": (19, 7, 146, 54), "pinkenba": (27, 25, 153, 6), "port-alma": (23, 35, 150, 52),
    "port-douglas": (16, 29, 145, 27), "port-office-brisbane-river": (27, 28, 153, 1),
    "portland-roads": (12, 35, 143, 24),
    "red-island-point-bamaga": (10, 50, 142, 22), "rosslyn-bay": (23, 9, 150, 47), "saibai-island": (9, 22, 142, 36),
    "scarborough": (27, 11, 153, 6), "shorncliffe": (27, 19, 153, 5), "shute-harbour": (20, 17, 148, 47),
    "skardon-river-offshore": (11, 45, 141, 59), "skardon-river": (11, 45, 142, 4), "south-trees": (23, 51, 151, 18),
    "southport": (27, 57, 153, 25), "st-pauls-moa-island": (10, 11, 142, 20), "stephens-island-ugar": (9, 30, 143, 32),
    "sue-island-warraber": (10, 12, 142, 49), "tangalooma": (27, 10, 153, 22), "thursday-island": (10, 35, 142, 13),
    "tin-can-bay-snapper-creek": (25, 54, 153, 0), "townsville": (19, 15, 146, 49), "twin-island": (10, 27, 142, 26),
    "urangan-fairway-beacon": (25, 8, 152, 49), "urangan": (25, 17, 152, 54), "waddy-point-k-gari": (24, 58, 153, 20),
    "weipa": (12, 40, 141, 51), "yam-island-iama": (9, 53, 142, 45), "yorke-island-masig": (9, 44, 143, 24),
}


def _get(get, url):
    try:
        return get(url, headers=HEADERS)
    except TideFetchError:
        raise
    except Exception as exc:                             # a status, a timeout, an oversize body
        raise TideFetchError("Queensland's open data could not be read: %s" % type(exc).__name__) from exc


def _result(body):
    """A CKAN API answer -> its "result" (success true), else TideFetchError."""
    try:
        doc = json.loads(body.decode("utf-8"))
    except (AttributeError, UnicodeDecodeError, ValueError) as exc:
        raise TideFetchError("Queensland's open data answered something that is not JSON") from exc
    if not isinstance(doc, dict) or doc.get("success") is not True or not isinstance(doc.get("result"), dict):
        err = doc.get("error") if isinstance(doc, dict) else None
        raise TideFetchError("Queensland's open data answered an error: %s" % str(err)[:160])
    return doc["result"]


def search_url(suffix):
    return API + "package_search?" + urlencode({"q": "name:*%s*" % suffix, "rows": 1000})


def position(dms):
    a, b, c, d = dms
    return -round(a + b / 60.0, 5), round(c + d / 60.0, 5)


def place_name(title):
    """'Abbot Point tide gauge\u2014predicted interval data' -> 'Abbot Point'; 'Golding Channel\u2014predicted ...'
    -> 'Golding Channel'."""
    t = re.split(r"\s+tide gauge\b", str(title), maxsplit=1, flags=re.I)[0]
    t = re.split(r"[\s\u00a0\u2013\u2014-]+predicted\b", t, maxsplit=1, flags=re.I)[0]
    return re.sub(r"\s+", " ", t).strip()


# ---------------------------------------------------------------------------------------------- the station list

def _years(pkg):
    """{year: resource id} of a dataset's yearly CSVs in the DataStore."""
    out = {}
    for r in pkg.get("resources") or []:
        m = YEAR_RE.search(str(r.get("name") or ""))
        if m and str(r.get("format") or "").upper() == "CSV" and r.get("datastore_active") and r.get("id"):
            out.setdefault(m.group(1), str(r["id"]))
    return out


def _search(get, suffix):
    res = _result(_get(get, search_url(suffix)))
    return {p["name"][:-len(suffix)]: p for p in res.get("results") or []
            if isinstance(p, dict) and str(p.get("name") or "").endswith(suffix)}


def build_station_list(get, min_year=None):
    """Every MSQ gauge with a position (POSITIONS) and a predicted-interval CSV of min_year (default: this UTC year) in
    the DataStore; "local" holds the resource id of each year from min_year - 1 on, curve and highs/lows."""
    if min_year is None:
        min_year = datetime.now(timezone.utc).year
    curves, hilos = _search(get, INTERVAL_SUFFIX), _search(get, HILO_SUFFIX)
    out = []
    for slug, pkg in sorted(curves.items()):
        if slug not in POSITIONS or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,70}", slug):
            continue
        cy = {y: r for y, r in _years(pkg).items() if int(y) >= min_year - 1}
        if str(min_year) not in cy:
            continue                                     # no current predictions
        hy = {y: r for y, r in _years(hilos.get(slug) or {}).items() if int(y) >= min_year - 1}
        lat, lon = position(POSITIONS[slug])
        out.append({"id": "%s:%s" % (PREFIX, slug), "name": place_name(pkg.get("title")) or slug, "lat": lat,
                    "lon": lon, "local": {"dataset": pkg["name"], "curve": cy, "hilo": hy}})
    return out


def page_url(st):
    """The gauge's predicted-interval dataset on the open data portal (its yearly CSVs and description)."""
    name = (st.get("local") or {}).get("dataset")
    return DATASET_PAGE + name if name else None


# ---------------------------------------------------------------------------------------------- the DataStore

def _local_day(epoch):
    return datetime.fromtimestamp(int(epoch), AEST).date()


def records_url(resource_id, dates, fields):
    return API + "datastore_search?" + urlencode({
        "resource_id": resource_id, "filters": json.dumps({"Date": dates}, separators=(",", ":")),
        "fields": ",".join(fields), "sort": "_id", "limit": ROWS_MAX, "records_format": "csv"})


def parse_records(result, fields):
    """A datastore_search result (records_format=csv) -> [[epoch, ...the other fields]] in time order. Rows with a bad
    date or time are skipped; a Reading that is not a finite number (or -99, the missing mark) is skipped."""
    text = result.get("records")
    if not isinstance(text, str):
        raise TideFetchError("Queensland's open data answered no records")
    out = {}
    for row in csv.reader(io.StringIO(text)):
        if len(row) != len(fields):
            continue
        rec = dict(zip(fields, (x.strip() for x in row)))
        try:
            d, m, y = (int(x) for x in rec["Date"].split("/"))
            hh, mm = (int(x) for x in rec["Time"].split(":"))
            t = int((datetime(y, m, d, tzinfo=AEST) + timedelta(hours=hh, minutes=mm)).timestamp())
            v = float(rec["Reading"])
        except (KeyError, ValueError):
            continue
        if not math.isfinite(v) or v <= -99 or not -20 < v < 30:
            continue
        out[t] = [t, round(v, 3)] + [rec[f] for f in fields if f not in ("Date", "Time", "Reading")]
    return [out[t] for t in sorted(out)]


def extremes(curve):
    """Highs and lows of a regular curve [(epoch, m)]: each local maximum / minimum (a flat top counts once), its time
    and height from the parabola through it and its two neighbours; a high-low pair closer than MIN_RANGE_M (a wiggle
    of the rounding on a stand) is dropped, then same-kind neighbours keep the higher high / lower low."""
    ext = []
    for i in range(1, len(curve) - 1):
        (t0, y0), (t1, y1), (t2, y2) = curve[i - 1], curve[i], curve[i + 1]
        if t1 - t0 != t2 - t1:
            continue                                     # a gap: no extreme read across it
        if y1 > y0 and y1 >= y2:
            k = "H"
        elif y1 < y0 and y1 <= y2:
            k = "L"
        else:
            continue
        den = y0 - 2 * y1 + y2
        x = 0.5 * (y0 - y2) / den if den else 0.0
        x = max(-0.5, min(0.5, x))
        ext.append([int(round(t1 + x * (t1 - t0))), round(y1 - 0.25 * (y0 - y2) * x, 3), k])
    changed = True
    while changed:
        changed = False
        for i in range(len(ext) - 1):
            if ext[i][2] != ext[i + 1][2] and abs(ext[i][1] - ext[i + 1][1]) < MIN_RANGE_M:
                del ext[i:i + 2]
                changed = True
                break
        j = 0
        while j < len(ext) - 1:
            a, b = ext[j], ext[j + 1]
            if a[2] == b[2]:
                keep = a if (a[1] >= b[1]) == (a[2] == "H") else b
                ext[j:j + 2] = [keep]
                changed = True
            else:
                j += 1
    return [tuple(e) for e in ext]


def fetch(st, begin, end, get):
    """MSQ's 10-minute curve and its published highs and lows covering [begin, end]: per AEST calendar year the window
    touches, one DataStore request for the curve and one for the highs and lows (derived from the curve when that
    year has none), metres above LAT."""
    loc = st.get("local") or {}
    curves, hilos = loc.get("curve") or {}, loc.get("hilo") or {}
    begin, end = int(begin), int(end)
    if not 0 < end - begin <= 60 * 86400:
        raise TideFetchError("Queensland: the range must be 0 to 60 days")
    days, d = {}, _local_day(begin)
    while d <= _local_day(end):
        days.setdefault(str(d.year), []).append(d.strftime("%d/%m/%Y"))
        d += timedelta(days=1)
    curve, hilo, unpublished = [], [], set()
    for year, dates in sorted(days.items()):
        rid = curves.get(year)
        if not rid:
            raise TideFetchError("Queensland publishes no %s predictions for this gauge" % year)
        fields = ["Date", "Time", "Reading"]
        part = [(t, v) for t, v in parse_records(_result(_get(get, records_url(rid, dates, fields))), fields)]
        if len(part) < 0.9 * len(dates) * 86400 / STEP_S:
            raise TideFetchError("Queensland's %s curve has gaps (%d of %d rows)" % (year, len(part),
                                                                                 len(dates) * 86400 // STEP_S))
        curve.extend(part)
        hid = hilos.get(year)
        if hid:
            fields = ["Date", "Time", "Ind", "Reading"]
            rows = parse_records(_result(_get(get, records_url(hid, dates, fields))), fields)
            got = [(t, v, "H" if ind == "1" else "L") for t, v, ind in rows if ind in ("1", "-1")]
            if len(got) < len(dates):
                raise TideFetchError("Queensland's %s highs and lows are too few" % year)
            hilo.extend(got)
        else:
            unpublished.add(int(year))
    curve.sort()
    if unpublished:                                      # read on the whole curve: an extreme at the new year survives
        hilo.extend(e for e in extremes(curve) if _local_day(e[0]).year in unpublished)
    hilo.sort()
    curve = [(t, v) for t, v in curve if begin <= t <= end]
    hilo = [(t, v, k) for t, v, k in hilo if begin <= t <= end]
    if not curve or curve[0][0] - begin > STEP_S or end - curve[-1][0] > STEP_S or not hilo:
        raise TideFetchError("Queensland's predictions do not cover the window")
    return {"curve": curve, "hilo": hilo}

