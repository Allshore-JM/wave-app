"""Canada: the Canadian Hydrographic Service's tide predictions from its IWLS API (plan section 112, step 3).

Source: https://api-iwls.dfo-mpo.gc.ca/api/v1 (no key). What CHS actually sends (checked 2026-11-26):

- GET /stations: a JSON list (1,577) of {id (24-hex uuid), code ("07795", always five digits), officialName,
  latitude, longitude, operating, type, timeSeries [{code, ...}]}. "operating" is about the gauge, not the
  predictions: 991 of the stations kept here have operating false (temporary, discontinued, seasonal, virtual) and CHS
  still answered 34 days of highs and lows for every one sampled (18 of 18, three of each of the six types). Kept: the 1,074 stations with both "wlp" (the curve)
  and "wlp-hilo" (the highs and lows).
- GET /stations/<uuid>/data?time-series-code=<code>&from=<ISO Z>&to=<ISO Z>[&resolution=...]: a JSON list of
  {eventDate "2026-11-26T07:57:00Z", value (metres), qcFlagCode, reviewed, timeSeriesId}, both ends included.
  wlp-hilo: every extreme in the range (34 days in one request is fine; 60 days answered too) but NOT whether each is a
  high or a low: that is read from the neighbours (classify). wlp: the date-interval cap depends on the resolution:
  "should not be bigger than 7 days" at the default one-minute step, 31 days at FIFTEEN_MINUTES or SIXTY_MINUTES
  (THIRTY_MINUTES is refused as "improperly formatted"). The curve is asked at FIFTEEN_MINUTES in consecutive pieces of
  CURVE_CHUNK_S (31 days: ~380 KB each), well inside both caps. A refused request answers HTTP 400 with
  {"status", "message", "errors"}.
- Heights are metres above chart datum (Canada: lower low water large tide / lowest normal tide, close to LAT).

Licence (https://tides.gc.ca/en/licence-agreement, modified 2023-08-14): use not for profit and not for navigation;
a derivative product must carry the notice in CREDIT "in a prominent location". The station page on tides.gc.ca is
/en/stations/<code> (a four-digit code redirects to the five-digit one).
"""
import json
import math
import re
from datetime import datetime, timezone
from urllib.parse import urlencode

from tide_providers import TideFetchError

PREFIX = "chs"
NAME = "Canadian Hydrographic Service"
# the notice the CHS licence agreement requires on a derivative product, with the User's name filled in
CREDIT = ("This product is not to be used for navigation. This product was made by or for allshoresurf.com and "
          "contains intellectual property (Data) of the Canadian Hydrographic Service of the Department of Fisheries "
          "and Oceans. The copyrights in the Data are and remain the property of His Majesty the King in Right of "
          "Canada and shall not be sold, licensed, leased, assigned or given to a third party. The incorporation of "
          "the Data in this product does not constitute an endorsement or an approval of this product by the "
          "Canadian Hydrographic Service, the Department of Fisheries and Oceans or His Majesty the King in Right of "
          "Canada.")
DATUM = "chart datum (≈ LAT)"

API = "https://api-iwls.dfo-mpo.gc.ca/api/v1"
PAGE = "https://tides.gc.ca/en/stations/"
HEADERS = {"User-Agent": "allshoresurf.com tide stations (https://allshoresurf.com)", "Accept": "application/json"}
CURVE_SERIES, HILO_SERIES = "wlp", "wlp-hilo"
RESOLUTION = "FIFTEEN_MINUTES"
CURVE_CHUNK_S = 31 * 86400             # one curve request covers at most this long: at FIFTEEN_MINUTES CHS takes 31 days
                                       # (378 KB; 32 refused, checked 2026-11-27); 2 pieces + the extremes (G76 P2-8)
MAX_SPAN_S = 35 * 86400                # the longest [begin, end] fetch() accepts (the site asks 34 days)
HILO_PAD_S = 13 * 3600                 # the extremes are asked this much wider, so the ones at begin / end have neighbours
MAX_REQUESTS = 1 + math.ceil(MAX_SPAN_S / CURVE_CHUNK_S)          # the extremes + the curve pieces: 3
UUID_RE = re.compile(r"[0-9a-f]{24}")
CODE_RE = re.compile(r"[0-9]{5}")


# ---------------------------------------------------------------------------------------------- CHS's answers

def iso(epoch):
    """Epoch seconds -> '2026-11-26T00:00:00Z' (the API's from / to)."""
    return datetime.fromtimestamp(int(epoch), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(text):
    """'2026-11-26T07:57:00Z' -> epoch seconds (an explicit offset is honoured; a time without one is refused)."""
    dt = datetime.fromisoformat(str(text).strip().replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("time without a zone: %r" % text)
    return int(dt.timestamp())


def _doc(body):
    try:
        return json.loads(body)
    except (TypeError, ValueError, UnicodeDecodeError) as exc:
        raise TideFetchError("CHS answered something that is not JSON") from exc


def parse_series(body):
    """A /data answer -> [(epoch, metres)] in time order, one per time. Rows without a time or a finite number are
    skipped; an answer that is not a list (an error document) raises."""
    doc = _doc(body)
    if not isinstance(doc, list):
        msg = doc.get("errors") or doc.get("message") if isinstance(doc, dict) else None
        raise TideFetchError("CHS answered: %s" % (str(msg)[:160] if msg else "an unexpected document"))
    out = {}
    for r in doc:
        try:
            t, v = parse_time(r["eventDate"]), float(r["value"])
        except (KeyError, TypeError, ValueError, AttributeError):
            continue
        if math.isfinite(v):
            out[t] = v
    return sorted(out.items())


def classify(extremes):
    """[(epoch, metres)] of successive extremes (CHS does not say which is which) -> [(epoch, metres, 'H'|'L')].

    An extreme above both neighbours is a high, below both a low; the first and the last are compared with their one
    neighbour. One that is neither (between its neighbours, or level with one: never seen in CHS's lists, 18 stations
    sampled) takes the opposite of the extreme before it (after it, at the start), as highs and lows alternate."""
    ex = sorted(extremes)
    n = len(ex)
    kinds = [None] * n
    for i, (_, v) in enumerate(ex):
        nb = [ex[j][1] for j in (i - 1, i + 1) if 0 <= j < n]
        if nb and all(v > w for w in nb):
            kinds[i] = "H"
        elif nb and all(v < w for w in nb):
            kinds[i] = "L"
    flip = {"H": "L", "L": "H"}
    for i in range(n):                                   # forwards: from the extreme before
        if kinds[i] is None and i > 0 and kinds[i - 1]:
            kinds[i] = flip[kinds[i - 1]]
    for i in range(n - 2, -1, -1):                       # backwards: a run at the start, from the extreme after
        if kinds[i] is None and kinds[i + 1]:
            kinds[i] = flip[kinds[i + 1]]
    if n and None in kinds:
        raise TideFetchError("CHS's highs and lows cannot be told apart")
    return [(t, v, k) for (t, v), k in zip(ex, kinds)]


def chunks(begin, end, size=None):
    """[begin, end] as consecutive (from, to) pieces of at most size seconds (CURVE_CHUNK_S); each piece starts where
    the one before ended (CHS includes both ends, so the shared instant comes twice and is kept once)."""
    size = size or CURVE_CHUNK_S
    out, a = [], int(begin)
    while True:
        b = min(a + size, int(end))
        out.append((a, b))
        if b >= end:
            return out
        a = b


def data_url(uuid, series, a, b, resolution=None):
    q = {"time-series-code": series, "from": iso(a), "to": iso(b)}
    if resolution:
        q["resolution"] = resolution
    return "%s/stations/%s/data?%s" % (API, uuid, urlencode(q))


def _get(get, url):
    try:
        return get(url, headers=HEADERS)
    except TideFetchError:
        raise
    except Exception as exc:                             # a status, a timeout, an oversize body
        raise TideFetchError("CHS could not be read: %s" % type(exc).__name__) from exc


# ---------------------------------------------------------------------------------------------- the interface

def build_station_list(get):
    """Every CHS station with both a predicted curve and predicted highs and lows (about 1,074 of 1,577)."""
    doc = _doc(_get(get, API + "/stations"))
    if not isinstance(doc, list):
        raise TideFetchError("CHS's station list is not a list")
    out, seen = [], set()
    for s in doc:
        if not isinstance(s, dict):
            continue
        series = {str(t.get("code")) for t in s.get("timeSeries") or [] if isinstance(t, dict)}
        if not {CURVE_SERIES, HILO_SERIES} <= series:
            continue
        uuid, code = str(s.get("id") or ""), str(s.get("code") or "")
        name = str(s.get("officialName") or "").strip()
        try:
            lat, lon = float(s["latitude"]), float(s["longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (UUID_RE.fullmatch(uuid) and CODE_RE.fullmatch(code) and name and -90 <= lat <= 90
                and -180 <= lon <= 180) or code in seen:
            continue
        seen.add(code)
        out.append({"id": "%s:%s" % (PREFIX, code), "name": name, "lat": lat, "lon": lon,
                    "local": {"uuid": uuid, "code": code}})
    out.sort(key=lambda s: s["id"])
    return out


def page_url(st):
    code = (st.get("local") or {}).get("code")
    return PAGE + code if code and CODE_RE.fullmatch(code) else None


def fetch(st, begin, end, get):
    """CHS's highs and lows (one request, HILO_PAD_S wider so the ends can be classified) and its 15-minute curve
    (consecutive CURVE_CHUNK_S requests) covering [begin, end]; heights in metres above chart datum."""
    uuid = (st.get("local") or {}).get("uuid") or ""
    if not UUID_RE.fullmatch(uuid):
        raise TideFetchError("no CHS station id")
    begin, end = int(begin), int(end)
    if not 0 < end - begin <= MAX_SPAN_S:
        raise TideFetchError("CHS: the range must be 0 to %d days" % (MAX_SPAN_S // 86400))
    ex = parse_series(_get(get, data_url(uuid, HILO_SERIES, begin - HILO_PAD_S, end + HILO_PAD_S)))
    hilo = [e for e in classify(ex) if begin <= e[0] <= end]
    curve = {}
    for a, b in chunks(begin, end):
        piece = parse_series(_get(get, data_url(uuid, CURVE_SERIES, a, b, RESOLUTION)))
        if not piece:
            raise TideFetchError("CHS answered no predictions for %s to %s" % (iso(a), iso(b)))
        curve.update(piece)
    if not hilo:
        raise TideFetchError("CHS answered no highs and lows")
    return {"curve": sorted((t, v) for t, v in curve.items() if begin <= t <= end), "hilo": hilo}
