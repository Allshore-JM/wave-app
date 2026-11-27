"""Norway: Kartverket's tide predictions for its permanent tide gauges (plan section 112, step 3).

Source: https://vannstand.kartverket.no/tideapi.php (no key; the old host api.sehavniva.no refuses connections).
Protocol: "API for water level and tides - communication protocol", rev. June 2025. What Kartverket actually sends
(checked 2026-11-26):

- tide_request=stationlist: XML <tide><stationinfo><location name code latitude longitude type/>... The protocol takes
  no parameters for it (type=perm and lang=en change nothing): 33 stations, every one type="PERM", three-letter codes
  (Svalbard's Ny-Alesund among them).
- tide_request=stationdata&stationcode=<code> (the protocol's request for the permanent gauges; locationdata at the
  gauge's own position answers the same highs and lows, plus the zone model's notes): XML <tide><stationdata><location>
  <data type="prediction" unit="cm" reflevelcode="CD"><waterlevel value="49.4" time="2026-11-26T04:01:00+00:00"
  flag="low"/>... datatype=tab: the highs and lows (flag "high" / "low"); datatype=pre: the curve (flag "pre") at
  interval=10 or 60 minutes. Both ends of fromtime / totime ("yyyy-mm-ddTHH:MM") are included. With tzone=0 the times
  carry +00:00 and are UTC (tzone=1 moves every time one hour and writes +01:00); dst=0, no daylight saving.
  Heights in centimetres. refcode=cd: chart datum, which is LAT in Norway.
- Limits: up to 366 days of data, 1,000 days of highs and lows per request (longer ones are cut, not refused); the
  predictions reach decades ahead (2045 answered). A 34-day 10-minute curve is ~340 KB of XML.
- A failure is still HTTP 200, with an <error> element: "No stations found from given stationcodes (XXQ)" (inside
  <stationdata>), "Invalid time..." (the whole document).

Terms: CC BY 4.0; "the Norwegian Mapping Authority, Hydrographic Service must be credited" (protocol, Terms of use).
The station's page on Kartverket's "Se havniva" service is the result page for the gauge's position.
"""
import math
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import urlencode

from tide_providers import TideFetchError

PREFIX = "kv"
NAME = "Kartverket (Norwegian Mapping Authority)"
CREDIT = ("Tide predictions: Norwegian Mapping Authority (Kartverket), Hydrographic Service, licensed under "
          "CC BY 4.0.")
DATUM = "chart datum (= LAT)"

API = "https://vannstand.kartverket.no/tideapi.php"
STATIONLIST_URL = API + "?tide_request=stationlist"
PAGE = "https://www.kartverket.no/en/at-sea/se-havniva/result?"
HEADERS = {"User-Agent": "allshoresurf.com tide stations (https://allshoresurf.com)"}
INTERVAL_MIN = 10                      # the curve's step (Kartverket offers 10 or 60)
MAX_SPAN_S = 366 * 86400               # Kartverket's own cap on one request's data (longer is silently cut)
MAX_REQUESTS = 2                       # the highs and lows + the curve
CODE_RE = re.compile(r"[A-Za-z]{3}")
KIND = {"high": "H", "low": "L"}


# ---------------------------------------------------------------------------------------------- Kartverket's answers

def stamp(epoch, up=False):
    """Epoch seconds -> 'yyyy-mm-ddTHH:MM' in UTC (the API's fromtime / totime, minutes only): down to the minute, or up
    with up=True (so the asked span always covers [begin, end])."""
    e = int(epoch)
    e = (e + 59) // 60 * 60 if up else e // 60 * 60
    return datetime.fromtimestamp(e, timezone.utc).strftime("%Y-%m-%dT%H:%M")


def parse_time(text):
    """'2026-11-26T04:01:00+00:00' -> epoch seconds (the offset is honoured; a time without one is refused)."""
    dt = datetime.fromisoformat(str(text).strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("time without a zone: %r" % text)
    return int(dt.timestamp())


def _xml(body):
    try:
        root = ET.fromstring(body)
    except (ET.ParseError, TypeError, ValueError) as exc:
        raise TideFetchError("Kartverket answered something that is not XML") from exc
    err = root if root.tag == "error" else root.find(".//error")
    if err is not None:
        raise TideFetchError("Kartverket answered: %s" % (err.text or "error").strip()[:160])
    return root


def parse_levels(body, code=None):
    """A stationdata answer -> [(epoch, metres, flag)] of its prediction series (of station code, when given) in time
    order, one per time; centimetres -> metres. Rows without a time or a finite number are skipped. An <error>, no
    prediction series, or a unit other than cm raises."""
    root = _xml(body)
    locs = root.findall(".//location")
    if code:
        locs = [l for l in locs if (l.get("code") or "").upper() == code.upper()]
    datas = [d for l in locs for d in l.findall("data") if d.get("type") == "prediction"]
    if not datas:
        raise TideFetchError("Kartverket answered no predictions")
    out = {}
    for d in datas:
        unit, ref = (d.get("unit") or "").lower(), (d.get("reflevelcode") or "CD").upper()
        if unit != "cm" or ref != "CD":
            raise TideFetchError("Kartverket answered unit %r on %r, not cm on chart datum" % (unit, ref))
        for w in d.findall("waterlevel"):
            try:
                t, v = parse_time(w.get("time")), float(w.get("value"))
            except (TypeError, ValueError):
                continue
            if math.isfinite(v):
                out[t] = (t, round(v / 100.0, 4), (w.get("flag") or "").lower())
    return [out[t] for t in sorted(out)]


def data_url(code, begin, end, datatype):
    q = {"tide_request": "stationdata", "stationcode": code, "fromtime": stamp(begin), "totime": stamp(end, up=True),
         "datatype": datatype, "refcode": "cd", "lang": "en", "dst": 0, "tzone": 0}
    if datatype == "pre":
        q["interval"] = INTERVAL_MIN
    return API + "?" + urlencode(q)


def _get(get, url):
    try:
        return get(url, headers=HEADERS)
    except TideFetchError:
        raise
    except Exception as exc:                             # a status, a timeout, an oversize body
        raise TideFetchError("Kartverket could not be read: %s" % type(exc).__name__) from exc


# ---------------------------------------------------------------------------------------------- the interface

def build_station_list(get):
    """Kartverket's permanent tide gauges (type PERM; all 33 of the list in November 2026)."""
    root = _xml(_get(get, STATIONLIST_URL))
    out, seen = [], set()
    for s in root.iter("location"):
        code, name = (s.get("code") or "").strip().upper(), (s.get("name") or "").strip()
        if (s.get("type") or "").upper() != "PERM" or not CODE_RE.fullmatch(code) or not name or code in seen:
            continue
        try:
            lat, lon = float(s.get("latitude")), float(s.get("longitude"))
        except (TypeError, ValueError):
            continue
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        seen.add(code)
        out.append({"id": "%s:%s" % (PREFIX, code.lower()), "name": name, "lat": lat, "lon": lon,
                    "local": {"code": code}})
    out.sort(key=lambda s: s["id"])
    return out


def page_url(st):
    """The "Se havniva" result page at the gauge's position (it names the gauge: "Tides and observed water level from
    Stavanger")."""
    try:
        lat, lon = float(st["lat"]), float(st["lon"])
    except (KeyError, TypeError, ValueError):
        return None
    return PAGE + urlencode({"latitude": "%.6f" % lat, "longitude": "%.6f" % lon})


def fetch(st, begin, end, get):
    """Kartverket's highs and lows and its 10-minute curve covering [begin, end] (two requests), metres above chart
    datum (LAT)."""
    code = (st.get("local") or {}).get("code") or ""
    if not CODE_RE.fullmatch(code):
        raise TideFetchError("no Kartverket station code")
    begin, end = int(begin), int(end)
    if not 0 < end - begin <= MAX_SPAN_S:
        raise TideFetchError("Kartverket: the range must be 0 to 366 days")
    tab = parse_levels(_get(get, data_url(code, begin, end, "tab")), code)
    hilo = [(t, v, KIND[f]) for t, v, f in tab if f in KIND and begin <= t <= end]
    pre = parse_levels(_get(get, data_url(code, begin, end, "pre")), code)
    curve = [(t, v) for t, v, _ in pre if begin <= t <= end]
    if not hilo or not curve:
        raise TideFetchError("Kartverket answered no %s" % ("highs and lows" if not hilo else "curve"))
    return {"curve": curve, "hilo": hilo}
