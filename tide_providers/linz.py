"""New Zealand: Toitū Te Whenua LINZ tide predictions (plan section 112, step 3).

Source (checked 2026-11-27):

- https://www.linz.govt.nz/api/v1/tp: JSON list of the 322 places on LINZ's tide-predictions map, each {"name",
  "field_location": "lat,lon", "field_tide_type": "Daily Predictions" | "Offset", "field_tide_prediction_file",
  "field_tide_reference_port", "field_tide_offset_file"}. 88 are "Daily Predictions": the standard ports and the
  secondary ports LINZ predicts in full; each has a yearly CSV. The other 234 are "Offset" places: their time and height
  differences from a standard port are published only as PDF tables (sec-ports/<port>.pdf), not in a machine-readable
  form, so they are left out (LINZ's own map offers no CSV for them either).
- https://static.charts.linz.govt.nz/tide-tables/maj-ports/csv/<file> <YYYY>.csv (the space URL-encoded): the year's
  highs and lows. UTF-8 with a BOM, CRLF. Three header lines ('070,Auckland,36°51'S,174°46'E', 'Based on constituent
  set ...', 'Local Std or Daylight Time,Tidal heights in metres.'), then one row a day: day, weekday, month, year and up
  to four time,height pairs ('1,Th,1,2026,05:47,3.1,11:51,0.9,18:06,3.1,,'). Heights in metres above chart datum, one
  decimal; no H/L flag (the extremes alternate; which is which follows from the heights). Files exist for 2024-2029
  (2030: HTTP 403). The page's own settings say start 2025, end 2029.
- Times are NZ local wall-clock time, NZST (UTC+12) or NZDT (UTC+13) as the date has it. On the day daylight saving
  ends an hour of times (02:00-02:59) happens twice; such a time is placed where the tide's rhythm puts it (see
  _to_utc). On the day it starts the clock jumps 02:00 -> 03:00 and the tables print no time in the gap.

Curve: none. LINZ publishes the highs and lows only; the site draws a half-cosine between them.
Datum: chart datum, which LINZ sets at or near LAT at its ports.

Terms: CC BY 4.0. Attribution as LINZ's copyright page words it (https://www.linz.govt.nz/copyright).
"""
import json
import re
import unicodedata
from datetime import datetime
from urllib.parse import quote
from zoneinfo import ZoneInfo

from tide_providers import TideFetchError

PREFIX = "linz"
NAME = "Toitū Te Whenua Land Information New Zealand (LINZ)"
CREDIT = ("This work is based on Toitū Te Whenua Land Information New Zealand data which are licensed by Toitū Te "
          "Whenua Land Information New Zealand for re-use under the Creative Commons Attribution 4.0 International "
          "licence.")
DATUM = "chart datum"                   # LINZ chart datum sits up to 0.45 m below TICON's LAT (Wellington): not called LAT
MAX_REQUESTS = 2                       # one CSV per NZ calendar year the window touches (a 34-day window: at most 2)

LIST_URL = "https://www.linz.govt.nz/api/v1/tp"
CSV_BASE = "https://static.charts.linz.govt.nz/tide-tables/maj-ports/csv/"
PAGE = "https://www.linz.govt.nz/products-services/tides-and-tidal-streams/tide-predictions"
HEADERS = {"User-Agent": "allshoresurf.com tide stations (https://allshoresurf.com)"}
TZ = ZoneInfo("Pacific/Auckland")
DAILY = "daily predictions"
PAD_S = 86400                          # a day each side when choosing the year files (local vs UTC new year)
MIN_YEAR_EXTREMES = 600                # a year has ~1,410 highs and lows; far fewer means the file is not what it seems
HALF_M2_S = 12 * 3600 + 25 * 60        # a semidiurnal tide's period (same-kind extremes ~12 h 25 min apart)
TIME_RE = re.compile(r"(\d{1,2}):(\d{2})")


def _get(get, url):
    try:
        return get(url, headers=HEADERS)
    except TideFetchError:
        raise
    except Exception as exc:                             # a status (403 for a year not published), a timeout ...
        raise TideFetchError("LINZ could not be read: %s" % type(exc).__name__) from exc


def slug(name):
    """'Ākitio River Entrance' -> 'akitio-river-entrance' (macrons dropped, lowercase [a-z0-9-])."""
    s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")[:70]


def csv_url(file, year):
    return CSV_BASE + quote("%s %d.csv" % (file, int(year)))


# ---------------------------------------------------------------------------------------------- the CSV

def parse_rows(body):
    """A year CSV -> [naive local datetime, metres] in the file's order. Header lines and rows that are not a date are
    skipped; a pair without a valid time or number is skipped; a file without a single extreme raises."""
    try:
        text = body.decode("utf-8-sig")
    except (AttributeError, UnicodeDecodeError) as exc:
        raise TideFetchError("LINZ answered something that is not a UTF-8 CSV") from exc
    if text.lstrip().startswith(("<", "{")):
        raise TideFetchError("LINZ answered something that is not its tide CSV")
    out = []
    for line in text.splitlines():
        f = [x.strip() for x in line.split(",")]
        if len(f) < 6 or not (f[0].isdigit() and f[2].isdigit() and f[3].isdigit()):
            continue
        try:
            day = datetime(int(f[3]), int(f[2]), int(f[0]))
        except ValueError:
            continue
        for i in range(4, len(f) - 1, 2):
            m = TIME_RE.fullmatch(f[i])
            if not m or not f[i + 1]:
                continue
            hh, mm = int(m.group(1)), int(m.group(2))
            try:
                h = float(f[i + 1])
            except ValueError:
                continue
            if hh > 23 or mm > 59 or not -20 < h < 30:
                continue
            out.append([day.replace(hour=hh, minute=mm), h])
    if not out:
        raise TideFetchError("LINZ's CSV has no highs and lows")
    return out


def _candidates(local, tz=TZ):
    """The UTC epochs a NZ wall-clock time can mean: one, or two in the repeated hour when daylight saving ends. A time
    in the spring gap (none in LINZ's tables) is read as standard time."""
    out = []
    for fold in (0, 1):
        e = int(local.replace(tzinfo=tz, fold=fold).timestamp())
        if datetime.fromtimestamp(e, tz).replace(tzinfo=None) == local and e not in out:
            out.append(e)
    if not out:                                          # the spring-forward gap: the clock had not moved yet
        out.append(int(local.replace(tzinfo=tz, fold=0).timestamp()))
    return sorted(out)


def station_zone(st):
    """The wall clock of a station's table: the Chatham Islands keep their own (UTC+12:45 / +13:45; G76 re-check: read
    as mainland time they ran 45 min late); every other place New Zealand time."""
    return ZoneInfo("Pacific/Chatham") if st.get("tz") == "Pacific/Chatham" or "chatham" in str(st.get("id", "")) else TZ


def _to_utc(rows, tz=TZ):
    """[[naive local, h], ...] (time order) -> [(epoch, h), ...]. A time in the repeated autumn hour takes the reading
    nearest where its neighbours put it: the same-kind extremes two places away +- 12 h 25 min (else the midpoint of
    its neighbours), and never at or before the extreme before it."""
    cands = [_candidates(r[0], tz) for r in rows]
    out = []
    for i, (r, c) in enumerate(zip(rows, cands)):
        if len(c) > 1:
            want = []
            if i >= 2 and len(cands[i - 2]) == 1:
                want.append(out[i - 2][0] + HALF_M2_S)
            if i + 2 < len(rows) and len(cands[i + 2]) == 1:
                want.append(cands[i + 2][0] - HALF_M2_S)
            if not want and 0 < i < len(rows) - 1 and len(cands[i + 1]) == 1:
                want.append((out[i - 1][0] + cands[i + 1][0]) / 2.0)
            ok = [e for e in c if not out or e > out[-1][0]] or c
            e = min(ok, key=lambda x: abs(x - sum(want) / len(want))) if want else ok[0]
        else:
            e = c[0]
        out.append((e, r[1]))
    return out


def kinds(ext):
    """[(epoch, h)] -> [(epoch, h, 'H'|'L')]: the extremes alternate, so one parity is the highs; the one most pairs of
    neighbours agree with wins. A file where the heights do not alternate raises (it is not a table of extremes)."""
    if len(ext) < 2:
        raise TideFetchError("LINZ's CSV has too few highs and lows")
    votes = [0, 0]                                       # parity 0 = even indexes are highs
    for i in range(len(ext) - 1):
        d = ext[i][1] - ext[i + 1][1]
        if d:
            votes[(i % 2) ^ (0 if d > 0 else 1)] += 1
    total = sum(votes)
    if not total or max(votes) < 0.95 * total:
        raise TideFetchError("LINZ's highs and lows do not alternate")
    par = 0 if votes[0] >= votes[1] else 1
    return [(t, h, "H" if (i % 2) == par else "L") for i, (t, h) in enumerate(ext)]


def parse_year(body):
    """One year CSV -> [(epoch, metres, 'H'|'L')] in time order."""
    rows = parse_rows(body)
    if len(rows) < MIN_YEAR_EXTREMES:
        raise TideFetchError("LINZ's CSV has only %d highs and lows" % len(rows))
    return kinds(_to_utc(rows))


# ---------------------------------------------------------------------------------------------- the interface

def build_station_list(get, check_year="now"):
    """LINZ's "Daily Predictions" places with a position and a prediction file whose CSV of check_year (default: this
    NZ year) loads; check_year=None skips that check (one request a place: 88)."""
    if check_year == "now":
        check_year = datetime.now(TZ).year
    try:
        doc = json.loads(_get(get, LIST_URL).decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise TideFetchError("LINZ's place list is not JSON") from exc
    if not isinstance(doc, list):
        raise TideFetchError("LINZ's place list is not a list")
    out, seen = [], set()
    for p in doc:
        if not isinstance(p, dict) or str(p.get("field_tide_type") or "").strip().lower() != DAILY:
            continue
        name, file = str(p.get("name") or "").strip(), str(p.get("field_tide_prediction_file") or "").strip()
        try:
            lat, lon = (float(x) for x in str(p.get("field_location") or "").split(","))
        except ValueError:
            continue
        sid = slug(name)
        if not name or not file or not sid or sid in seen or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        if check_year is not None:
            try:
                parse_year(_get(get, csv_url(file, check_year)))
            except TideFetchError:
                continue
        seen.add(sid)
        out.append({"id": "%s:%s" % (PREFIX, sid), "name": name, "lat": round(lat, 5), "lon": round(lon, 5),
                    "local": {"file": file}})
    out.sort(key=lambda s: s["id"])
    return out


def page_url(st):
    """LINZ's tide-predictions page (its map links every port's CSV and PDF; there is no page per port)."""
    return PAGE


def _local_year(epoch):
    return datetime.fromtimestamp(int(epoch), TZ).year


def fetch(st, begin, end, get):
    """LINZ's highs and lows covering [begin, end] from the year CSVs the window touches (one or two requests), metres
    above chart datum. No curve."""
    file = str((st.get("local") or {}).get("file") or "").strip()
    if not file:
        raise TideFetchError("no LINZ prediction file")
    begin, end = int(begin), int(end)
    if not 0 < end - begin <= 366 * 86400:
        raise TideFetchError("LINZ: the range must be 0 to 366 days")
    years = range(_local_year(begin - PAD_S), _local_year(end + PAD_S) + 1)
    if len(years) > MAX_REQUESTS:
        raise TideFetchError("LINZ: the range spans more than %d years" % MAX_REQUESTS)
    rows = []
    for y in years:
        got = parse_rows(_get(get, csv_url(file, y)))
        if len(got) < MIN_YEAR_EXTREMES or any(r[0].year != y for r in got):
            raise TideFetchError("LINZ's %d CSV is not that year's table" % y)
        rows.extend(got)
    rows.sort(key=lambda r: r[0])                        # (a repeated autumn hour keeps the file's order: stable sort)
    ext = kinds(_to_utc(rows, station_zone(st)))
    if ext[0][0] > begin or ext[-1][0] < end:
        raise TideFetchError("LINZ's tables do not cover the window")
    hilo = [(t, round(h, 3), k) for t, h, k in ext if begin <= t <= end]
    if not hilo:
        raise TideFetchError("LINZ answered no highs and lows")
    return {"curve": [], "hilo": hilo}
