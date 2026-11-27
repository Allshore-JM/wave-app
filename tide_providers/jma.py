"""Japan Meteorological Agency tide tables (潮位表) for the Tide layer (plan section 112, step 3).

Source: JMA's tide-table text files, one per station per year,
https://www.data.jma.go.jp/kaiyou/data/db/tide/suisan/txt/{YYYY}/{CODE}.txt, laid out as the official format
description says (https://www.data.jma.go.jp/kaiyou/db/tide/suisan/readme.html, "テキストデータ版"): one LF-ended
line of 136 columns per day,

    cols   1-72   24 hourly heights, 3 columns each (00:00 to 23:00), cm
    cols  73-78   the date, YYMMDD (2 columns each, space-padded: "26 1 1")
    cols  79-80   the station code (2 letters / digits)
    cols  81-108  4 high waters, each a time HHMM (4 columns, space-padded: " 4 8" = 04:08) and a height (3, cm)
    cols 109-136  4 low waters, the same
    9999 / 999    no such high (low) water

Times are JST (UTC+9), heights cm above the tide-table datum (潮位表基準面, near the lowest low water; the station
list gives its depth below mean sea level). Station list: station{YYYY}.php (239 stations in 2026 and 2027; names in
Japanese only: JMA publishes no English names for the tide-table stations, so a station is "<Japanese name> (<code>)").

Licence: the Government of Japan Public Data Terms of Use v1.0 (公共データ利用規約 第1.0版, compatible with CC BY
4.0): the source must be named (出典：気象庁ホームページ) and an edit said to be one. We convert cm to metres and JST to
UTC, and the Tide layer resamples the hourly heights to 30 minutes: an edit."""
import re
import time
from datetime import datetime, timedelta, timezone
from html import unescape

from . import TideFetchError

PREFIX = "jma"
NAME = "Japan Meteorological Agency"
CREDIT = ("Source: Japan Meteorological Agency website, tide tables (出典：気象庁ホームページ「潮位表」を加工して作成); "
          "edited by allshoresurf.com: heights converted to metres, times to UTC, hourly heights interpolated.")
DATUM = "tide-table datum (JMA; the lowest tides can read below zero)"   # G76: JMA heights reach -0.34 m
MAX_REQUESTS = 2                      # one year file, two around New Year

TXT_URL = "https://www.data.jma.go.jp/kaiyou/data/db/tide/suisan/txt/{year}/{code}.txt"
LIST_URL = "https://www.data.jma.go.jp/kaiyou/db/tide/suisan/station{year}.php"
PAGE_URL = "https://www.data.jma.go.jp/kaiyou/db/tide/suisan/suisan.php?stn={code}"
UA = "allshoresurf.com tide stations (https://allshoresurf.com)"
JST = timezone(timedelta(hours=9))
LINE_LEN = 136
CURVE_PAD_S = 3 * 3600                # hourly heights kept this far outside [begin, end] (the grid's cubic needs them)
CODE_RE = re.compile(r"[A-Z0-9]{2}")
POS_RE = re.compile(r"^\s*(\d{1,3})\s*[゜°˚º]\s*(\d{1,2}(?:\.\d+)?)\s*['′’]?\s*$")
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
TAG_RE = re.compile(r"<[^>]+>")


def _get(get, url):
    try:
        return get(url, headers={"User-Agent": UA})
    except Exception as exc:                          # any transport failure or non-200 answer
        raise TideFetchError("JMA could not be reached (%s)" % type(exc).__name__) from exc


# ------------------------------------------------------------------------------------------- the year files

def _int(field, what):
    s = field.strip()
    try:
        return int(s)
    except ValueError:
        raise TideFetchError("JMA tide table: unreadable %s %r" % (what, field)) from None


def parse_year_file(body, code=None, year=None):
    """A station's year file -> (curve [(epoch, m)], hilo [(epoch, m, 'H'|'L')]), in time order, all of it. Any line
    that does not read as the format says (or names another station or year) is an error: never partial garbage."""
    try:
        text = body.decode("ascii") if isinstance(body, (bytes, bytearray)) else str(body)
    except UnicodeDecodeError:
        raise TideFetchError("JMA tide table: not a text file") from None
    curve, hilo = [], []
    days = 0
    for raw in text.split("\n"):
        line = raw.rstrip("\r")
        if not line.strip():
            continue
        if len(line) < 80 or len(line) > LINE_LEN:
            raise TideFetchError("JMA tide table: a line of %d columns (the format has %d)" % (len(line), LINE_LEN))
        line = line.ljust(LINE_LEN)
        yy, mm, dd = _int(line[72:74], "year"), _int(line[74:76], "month"), _int(line[76:78], "day")
        stn = line[78:80]
        if not CODE_RE.fullmatch(stn) or (code is not None and stn != code.upper()):
            raise TideFetchError("JMA tide table: station %r in the file of %s" % (stn, code))
        if year is not None and 2000 + yy != int(year):
            raise TideFetchError("JMA tide table: year 20%02d in the file of %s" % (yy, year))
        try:
            midnight = datetime(2000 + yy, mm, dd, tzinfo=JST)
        except ValueError:
            raise TideFetchError("JMA tide table: no such date %02d%02d%02d" % (yy, mm, dd)) from None
        t0 = int(midnight.timestamp())
        for h in range(24):
            cm = _int(line[3 * h:3 * h + 3], "hourly height")
            if cm != 999:                              # never written for an hour, but a "none" if it ever is
                curve.append((t0 + h * 3600, cm / 100.0))
        for k, start in (("H", 80), ("L", 108)):
            for i in range(4):
                f = line[start + 7 * i:start + 7 * i + 7]
                hhmm, height = f[:4], f[4:]
                if hhmm.strip() == "9999" or height.strip() == "999" or not f.strip():
                    continue                           # no such high (low) water this day
                hh = _int(hhmm[:2], "time") if hhmm[:2].strip() else 0      # " 015" = 00:15 (never seen blank)
                mi = _int(hhmm[2:], "time")
                if not (0 <= hh <= 24 and 0 <= mi < 60):
                    raise TideFetchError("JMA tide table: no such time %r" % hhmm)
                hilo.append((t0 + (hh * 60 + mi) * 60, _int(height, "height") / 100.0, k))
        days += 1
    if not days:
        raise TideFetchError("JMA tide table: empty file")
    curve.sort()
    hilo.sort()
    return curve, hilo


def years_for(begin, end):
    """The JST years whose files hold [begin - pad, end + pad]."""
    a = datetime.fromtimestamp(begin - CURVE_PAD_S, JST).year
    z = datetime.fromtimestamp(end + CURVE_PAD_S, JST).year
    return list(range(a, z + 1))


def fetch(st, begin, end, get):
    code = str(st["local"]["code"]).upper()
    if not CODE_RE.fullmatch(code):
        raise TideFetchError("not a JMA station code: %r" % code)
    curve, hilo = [], []
    for year in years_for(begin, end):
        c, h = parse_year_file(_get(get, TXT_URL.format(year=year, code=code)), code, year)
        curve += c
        hilo += h
    curve = sorted(p for p in curve if begin - CURVE_PAD_S <= p[0] <= end + CURVE_PAD_S)
    hilo = sorted(p for p in hilo if begin <= p[0] <= end)
    if not curve:
        raise TideFetchError("JMA tide table: nothing inside the window")
    return {"curve": curve, "hilo": hilo}


# ------------------------------------------------------------------------------------------- the station list

def parse_position(text):
    """"35゜39'" (degrees and minutes) -> 35.65; None when it does not read."""
    m = POS_RE.match(unescape(text))
    if not m:
        return None
    deg, minutes = int(m.group(1)), float(m.group(2))
    if minutes >= 60:
        return None
    return deg + minutes / 60.0


def _cell(html):
    return unescape(TAG_RE.sub("", html)).replace("\xa0", " ").strip()


def parse_station_list(body):
    """station{YYYY}.php -> [station dict]. Columns: number, code, name (linked), latitude N, longitude E, MSL above
    the tide-table datum (cm), ... All tide-table stations are north and east (Japan)."""
    html = body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray)) else str(body)
    out, seen = [], set()
    for row in ROW_RE.findall(html):
        tds = TD_RE.findall(row)
        if len(tds) < 6:
            continue
        code = _cell(tds[1]).upper()
        name = _cell(tds[2])
        lat, lon = parse_position(_cell(tds[3])), parse_position(_cell(tds[4]))
        if not CODE_RE.fullmatch(code) or not name or lat is None or lon is None or code in seen:
            continue
        if not (20.0 <= lat <= 46.0 and 122.0 <= lon <= 154.5):
            continue
        try:
            msl_cm = float(_cell(tds[5]))
        except ValueError:
            msl_cm = None
        seen.add(code)
        local = {"code": code, "name_ja": name}
        if msl_cm is not None:
            local["msl_cm"] = msl_cm                   # mean sea level above the tide-table datum
        out.append({"id": "%s:%s" % (PREFIX, code.lower()), "name": "%s (%s)" % (name, code),
                    "lat": round(lat, 5), "lon": round(lon, 5), "local": local})
    return out


def build_station_list(get, year=None):
    year = int(year or time.gmtime().tm_year)
    out = parse_station_list(_get(get, LIST_URL.format(year=year)))
    if not out:
        raise TideFetchError("JMA station list: no stations read")
    return out


def page_url(st):
    return PAGE_URL.format(code=str(st["local"]["code"]).upper())
