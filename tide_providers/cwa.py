"""Taiwan: Central Weather Administration (CWA) one-month tide forecasts (plan section 113 B).

Source (checked 2026-11-27): open data set F-A0021-001 "潮汐預報-未來1個月潮汐預報".

- 266 named coastal places with WGS84 positions: 106 townships (8-digit ids), 77 fishing harbours (I...), 28 fishing
  spots (B...), 23 beaches (A...), 19 dive spots (N...) and 13 more coastal places (O...: CWA's surf list; the site
  shows each by its own geographic name, never as a surf spot: owner 2026-11-27, tools/tides/build_national.py
  cwa_names). A place without its own
  gauge is predicted by CWA from the nearest gauge (a mean height ratio and time offset: CWA's product note,
  https://www.cwa.gov.tw/Data/data_catalog/4-3-2.pdf). Astronomical tide only.
- Highs and lows to the minute, about 32 days from today (Taipei), rebuilt every day just after midnight Taipei time.
  No curve: the site draws a half-cosine between them.
- Each event: {"DateTime": "2026-11-28T06:26:00+08:00", "Tide": "乾潮" (low) | "滿潮" (high), "TideHeights":
  {"AboveTWVD", "AboveLocalMSL", "AboveChartDatum"}}, heights in cm as strings. AboveChartDatum (TWCD 2021: lowest low
  water = 0) was given for all 31,560 events of 2026-11-28's file; AboveTWVD is sometimes null.
- The JSON is converted from XML: a day with one tide has "Time" as an object, not a list (46 such days that day), and
  any list of one item may come unwrapped.
- Live: the REST API with the site's key (CWA_API_KEY; CWA takes the key only in the query string, so every error text
  here is generic and never carries the URL): /api/v1/rest/datastore/F-A0021-001?LocationId=<id>, one place's month
  (records.TideForecasts[...]). CWA's limit is 20,000 calls a day; the site asks once a place per UTC day.
- The station list (tools/tides/build_national.py, offline, no key) reads the same data set's daily file from CWA's
  public storage, where CWA's keyed file API sends its downloads.

Terms: the CWA open-data rules (Open Government Data License, version 1.0, compatible with CC BY 4.0): free reuse with
the attribution the rules word (the agency, the data set's name, the licence sentence and a link to the licence).
"""
import json
import os
import statistics
import threading
import time
from datetime import datetime, timezone
from urllib.parse import quote

from tide_providers import TideFetchError

PREFIX = "cwa"
NAME = "Central Weather Administration (Taiwan)"
CREDIT = ("提供機關／交通部中央氣象署 潮汐預報-未來1個月潮汐預報 (Central Weather Administration, Taiwan: one-month tide "
          "forecast F-A0021-001). 此開放資料依政府資料開放授權條款 (Open Government Data License) 進行公眾釋出，"
          "使用者於遵守本條款各項規定之前提下，得利用之。 政府資料開放授權條款: https://data.gov.tw/license")
DATUM = "chart datum (lowest low water; the lowest tides can read below zero)"   # G79-7: down to -0.20 m
MAX_REQUESTS = 1

DATASET = "F-A0021-001"
API = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/" + DATASET + "?Authorization=%s&format=JSON&LocationId=%s"
FILE_URL = "https://cwaopendata.s3.ap-northeast-1.amazonaws.com/Forecast/" + DATASET + ".json"
PAGE = "https://opendata.cwa.gov.tw/dataset/forecast/" + DATASET
HEADERS = {"User-Agent": "allshoresurf.com tide stations (https://allshoresurf.com)"}
KIND = {"滿潮": "H", "乾潮": "L"}
TZ_NAME = "Asia/Taipei"                  # Kinmen and Matsu too (the nearest-zone rule would give them China's name)
MIN_EVENTS = 20                          # a place's month has ~118 highs and lows; far fewer: not what it seems
DAILY_MAX = 2000                         # requests a UTC day, at most (G79-6: CWA's 20,000 a day are shared with the
                                         # site's CWA buoys and winds; 266 places, a window cache of 256 entries)
CD_SPREAD_CM = 5                         # G79-3: AboveChartDatum - AboveLocalMSL is one number per place; wider: CWA's
                                         # chart-datum heights are not consistent there (3 places in 2026-11-28's file)
HOLD_S = 3 * 86400                       # G79-1: a place's earlier highs and lows are kept this long

_lock = threading.Lock()
_held = {}                               # place code -> [(epoch, metres, kind)]: what CWA published before (G79-1)
_count = {"day": None, "n": 0}


def key():
    return os.environ.get("CWA_API_KEY", "").strip()


def available():
    """CWA answers only with a key: without one the places are not listed (as the keyed CWA buoys are not). CWA_TIDES=0
    turns the tides off alone, the key and the CWA buoys and winds staying (G79-2)."""
    return bool(key()) and os.environ.get("CWA_TIDES", "1").strip() != "0"


def _items(x):
    """A list as CWA's XML-converted JSON gives it: a list, one bare object, or nothing."""
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def forecasts(doc):
    """The TideForecasts entries of either answer: the REST API's {records: {TideForecasts}} or the daily file's
    {cwaopendata: {Resources: {Resource: {Data: {TideForecasts}}}}}."""
    if not isinstance(doc, dict):
        raise TideFetchError("CWA answered something that is not its JSON")
    rec = doc.get("records")
    if isinstance(rec, dict) and "TideForecasts" in rec:
        return _items(rec["TideForecasts"])
    try:
        return _items(doc["cwaopendata"]["Resources"]["Resource"]["Data"]["TideForecasts"])
    except (KeyError, TypeError):
        raise TideFetchError("CWA's answer holds no tide forecasts") from None


def _location(entry):
    loc = entry.get("Location") if isinstance(entry, dict) else None
    return loc if isinstance(loc, dict) else None


def parse_events(loc, msl_gap_cm=None):
    """One place -> [(epoch, metres above chart datum, 'H'|'L')] in time order; events without a time, a kind or a
    chart-datum height are skipped, duplicates dropped. With msl_gap_cm (a place whose chart-datum heights CWA gives
    inconsistently: G79-3) the height is AboveLocalMSL + that gap."""
    out = {}
    for day in _items((loc.get("TimePeriods") or {}).get("Daily")):
        if not isinstance(day, dict):
            continue
        for ev in _items(day.get("Time")):
            if not isinstance(ev, dict):
                continue
            k = KIND.get(str(ev.get("Tide") or "").strip())
            hs = ev.get("TideHeights") if isinstance(ev.get("TideHeights"), dict) else {}
            try:
                t = datetime.fromisoformat(str(ev.get("DateTime")))
                cm = float(hs.get("AboveLocalMSL")) + float(msl_gap_cm) if msl_gap_cm is not None \
                    else float(hs.get("AboveChartDatum"))
            except (TypeError, ValueError):
                continue
            if k is None or t.tzinfo is None or not -500 < cm < 2000:
                continue
            out[int(t.timestamp())] = (round(cm / 100.0, 3), k)
    return [(e, h, k) for e, (h, k) in sorted(out.items())]


def chart_gap(loc):
    """(median, spread) of AboveChartDatum - AboveLocalMSL in cm over a place's events, or (None, None)."""
    gaps = []
    for day in _items((loc.get("TimePeriods") or {}).get("Daily")):
        for ev in _items(day.get("Time") if isinstance(day, dict) else None):
            hs = ev.get("TideHeights") if isinstance(ev, dict) and isinstance(ev.get("TideHeights"), dict) else {}
            try:
                gaps.append(float(hs.get("AboveChartDatum")) - float(hs.get("AboveLocalMSL")))
            except (TypeError, ValueError):
                continue
    if not gaps:
        return None, None
    return statistics.median(gaps), max(gaps) - min(gaps)


def _json(body):
    try:
        return json.loads(body.decode("utf-8-sig"))
    except (AttributeError, UnicodeDecodeError, ValueError) as exc:
        raise TideFetchError("CWA answered something that is not JSON") from exc


# ---------------------------------------------------------------------------------------------- the interface

def build_station_list(get):
    """Every place of the daily file with a position and at least MIN_EVENTS highs and lows."""
    try:
        doc = _json(get(FILE_URL, headers=HEADERS))
    except TideFetchError:
        raise
    except Exception as exc:
        raise TideFetchError("CWA's daily file could not be read: %s" % type(exc).__name__) from exc
    out, seen = [], set()
    for entry in forecasts(doc):
        loc = _location(entry)
        if loc is None:
            continue
        code = str(loc.get("LocationId") or "").strip()
        name = str(loc.get("LocationName") or "").strip()
        try:
            lat, lon = float(loc.get("Latitude")), float(loc.get("Longitude"))
        except (TypeError, ValueError):
            continue
        sid = "%s:%s" % (PREFIX, code.lower())
        if (not code or not name or sid in seen or not (20 <= lat <= 27.5 and 116 <= lon <= 123)
                or len(parse_events(loc)) < MIN_EVENTS):
            continue
        seen.add(sid)
        local = {"code": code}
        gap, spread = chart_gap(loc)
        if spread is not None and spread > CD_SPREAD_CM:              # G79-3: chart datum from MSL + the median gap
            local["msl_gap_cm"] = round(gap)
        out.append({"id": sid, "name": name, "lat": round(lat, 5), "lon": round(lon, 5), "tz": TZ_NAME, "local": local})
    out = _one_per_spot(out)
    out.sort(key=lambda s: s["id"])
    return out


SPOT_M = 50.0                            # G79-9: CWA lists some places at one spot (a beach and a fishing spot ...)
RANK = {"I": 1, "A": 2, "B": 3, "N": 4, "O": 5}   # a township first (its code is digits), then harbour, beach ...


def _one_per_spot(stations):
    """Places closer than SPOT_M to a more official one are left out (the same water level, one icon)."""
    import math
    kept = []
    for s in sorted(stations, key=lambda s: (0 if s["local"]["code"][:1].isdigit() else RANK.get(s["local"]["code"][:1], 9),
                                             s["id"])):
        if all(math.hypot((s["lat"] - k["lat"]) * 111195.0,
                          (s["lon"] - k["lon"]) * 111195.0 * math.cos(math.radians(s["lat"]))) >= SPOT_M for k in kept):
            kept.append(s)
    return kept


def _spend(now=None):
    """One request of today's DAILY_MAX (UTC day), or TideFetchError (the window says to try again later)."""
    now = time.time() if now is None else now
    day = datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d")
    with _lock:
        if _count["day"] != day:
            _count["day"], _count["n"] = day, 0
        if _count["n"] >= DAILY_MAX:
            raise TideFetchError("CWA: today's request budget is spent")
        _count["n"] += 1


def _with_held(code, events, now=None):
    """CWA's month starts at Taipei's midnight, so a window that begins earlier (the site's starts at 00:00 UTC the day
    before) misses that part: the highs and lows this place had before CWA's first one now are put back in front, from
    what this server fetched earlier (G79-1). The fresh answer is kept for the next time, trimmed to HOLD_S."""
    now = time.time() if now is None else now
    with _lock:
        old = [e for e in _held.get(code, []) if e[0] < events[0][0]]
        merged = old + list(events)
        _held[code] = [e for e in merged if e[0] >= now - HOLD_S]
    return merged


def page_url(st):
    """CWA's page for the data set (CWA has no public page per place for these forecasts)."""
    return PAGE


def fetch(st, begin, end, get):
    """CWA's highs and lows for one place (its whole month: one request), metres above chart datum, those within
    [begin, end]. CWA publishes from today (Taipei) on, so the window's first hours may have none: the window shows
    what is there."""
    local = st.get("local") or {}
    code = str(local.get("code") or "").strip()
    k = key()
    if not code:
        raise TideFetchError("no CWA place code")
    if not k:
        raise TideFetchError("no CWA key on this server")
    _spend()
    failed = None
    try:
        body = get(API % (quote(k, safe=""), quote(code, safe="")), headers=HEADERS)
    except TideFetchError:
        raise
    except Exception as exc:                             # the URL holds the key: only the error's type goes on, and
        failed = type(exc).__name__                      # it is raised outside this block, so the exception that
    if failed is not None:                               # holds the URL is not its context either (G79-8)
        raise TideFetchError("CWA could not be read: %s" % failed)
    doc = _json(body)
    events = []
    for entry in forecasts(doc):
        loc = _location(entry)
        if loc is not None and str(loc.get("LocationId") or "").strip() == code:
            events = parse_events(loc, local.get("msl_gap_cm"))
            break
    if len(events) < MIN_EVENTS:
        raise TideFetchError("CWA answered %d highs and lows for %s" % (len(events), code))
    events = _with_held(code, events)
    hilo = [(t, h, kk) for t, h, kk in events if int(begin) <= t <= int(end)]
    if not hilo:
        raise TideFetchError("CWA's forecast does not reach the window")
    return {"curve": [], "hilo": hilo}
