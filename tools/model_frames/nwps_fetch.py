"""NOAA NWPS access (plan section 59): the NWS offices' nearshore SWAN runs on NOMADS, their point spectra files.

  <BASE>/<region>.<YYYYMMDD>/<office>/<HH>/CG1/nwps.t<HH>z.spc2d_<id>_CG1.<office>.txt   a point's 2-D spectra,
                                                                                            hourly 0..144 h (~1 MB)
  <BASE>/<region>.<YYYYMMDD>/<office>/<HH>/CG1/<office>_nwps_CG1_<YYYYMMDD>_<HH>00.grib2    the office's wide grid
Regions ar (Alaska), er (East), pr (Pacific), sr (South), wr (West); 36 offices publish NWPS (OFFICES; 34 held our
stations on 2026-10-28). Runs are
started by the office's forecasters: one to eight a day, irregular hours, posted 1-8 h after the cycle they are named
for; NOMADS keeps ~5 days. NOMADS only (no AWS copy, checked 2026-10-28), so every request goes through glwu_fetch's
pacer (one at a time, PACE_S apart; 429 / 503 -> Throttled = NotReady).
"""
import html
import re
from datetime import datetime, timedelta, timezone

import glwu_fetch as G

BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nwps/prod"
OFFICES = {
    "aer": "ar", "afg": "ar", "ajk": "ar", "alu": "ar",
    "akq": "er", "box": "er", "car": "er", "chs": "er", "gyx": "er", "ilm": "er", "lwx": "er", "mhx": "er",
    "okx": "er", "phi": "er",
    "gum": "pr", "hfo": "pr",
    "bro": "sr", "crp": "sr", "hgx": "sr", "jax": "sr", "key": "sr", "lch": "sr", "lix": "sr", "mfl": "sr",
    "mlb": "sr", "mob": "sr", "sju": "sr", "tae": "sr", "tbw": "sr",
    "eka": "wr", "lox": "wr", "mfr": "wr", "mtr": "wr", "pqr": "wr", "sew": "wr", "sgx": "wr",
}
OFFICE_NAMES = {
    "aer": "Anchorage", "afg": "Fairbanks", "ajk": "Juneau", "alu": "Anchorage (Aleutians)",
    "akq": "Wakefield", "box": "Boston", "car": "Caribou", "chs": "Charleston", "gyx": "Gray", "ilm": "Wilmington",
    "lwx": "Sterling", "mhx": "Newport/Morehead City", "okx": "Upton", "phi": "Mount Holly",
    "gum": "Guam", "hfo": "Honolulu",
    "bro": "Brownsville", "crp": "Corpus Christi", "hgx": "Houston", "jax": "Jacksonville", "key": "Key West",
    "lch": "Lake Charles", "lix": "New Orleans", "mfl": "Miami", "mlb": "Melbourne", "mob": "Mobile",
    "sju": "San Juan", "tae": "Tallahassee", "tbw": "Tampa Bay",
    "eka": "Eureka", "lox": "Los Angeles", "mfr": "Medford", "mtr": "San Francisco", "pqr": "Portland",
    "sew": "Seattle", "sgx": "San Diego",
}
NotReady, TransportError, Throttled, request = G.NotReady, G.TransportError, G.Throttled, G.request

_ENTRY_RE = re.compile(r'<a href="([^"]+)">[^<]*</a>\s+(\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2})\s+(\S+)')


def day_dir(region, day):
    return f"{BASE}/{region}.{day:%Y%m%d}"


def cycle_dir(region, day, office, hh):
    return f"{day_dir(region, day)}/{office}/{hh:02d}"


def spc_url(region, day, office, hh, sid):
    return f"{cycle_dir(region, day, office, hh)}/CG1/nwps.t{hh:02d}z.spc2d_{sid}_CG1.{office}.txt"


def grib_name(office, day, hh):
    return f"{office}_nwps_CG1_{day:%Y%m%d}_{hh:02d}00.grib2"


def parse_listing(text):
    """[(name, modified datetime UTC, size text)] from a NOMADS directory page (names without a path)."""
    out = []
    for name, when, size in _ENTRY_RE.findall(text):
        name = html.unescape(name)
        if name.startswith("/") or name.startswith("?"):
            continue
        out.append((name, datetime.strptime(when, "%d-%b-%Y %H:%M").replace(tzinfo=timezone.utc), size))
    return out


def listing(url):
    _status, body = request(url + "/" if not url.endswith("/") else url)
    return parse_listing(body.decode("utf-8", "replace"))


def cycles(region, day, office):
    """The cycle hours an office has under one day's directory (an absent directory = none)."""
    try:
        entries = listing(f"{day_dir(region, day)}/{office}/")
    except NotReady:
        return []
    return sorted(int(n.rstrip("/")) for n, _, _ in entries if re.fullmatch(r"\d{2}/", n))


def spectra_listing(region, day, office, hh):
    """{station id: (modified, size)} of the CG1 spectra files of one cycle, plus the grib's time under "" (None = absent)."""
    try:
        entries = listing(f"{cycle_dir(region, day, office, hh)}/CG1/")
    except NotReady:
        return None
    out = {}
    for name, when, size in entries:
        m = re.fullmatch(rf"nwps\.t{hh:02d}z\.spc2d_([A-Za-z0-9]+)_CG1\.{office}\.txt", name)
        if m:
            out[m.group(1)] = (when, size)
        elif name == grib_name(office, day, hh):
            out[""] = (when, size)
    return out


def recent_days(now, days):
    return [(now - timedelta(days=k)).date() for k in range(days)]


def spectra_head(url, n=6000):
    """The first n bytes of a spectra file (its header and first time): the point's position and whether it has data."""
    _status, body = request(url, rng=f"bytes=0-{n - 1}")
    return body.decode("ascii", "replace")


def head_info(text):
    """(lon, lat, first time has data) from a spectra file's head, or None when it is not one."""
    m = re.search(r"LONLAT[^\n]*\n[^\n]*\n\s*([-\d.]+)\s+([-\d.]+)", text)
    if not text.startswith("SWAN   1") or not m:
        return None
    lon, lat = float(m.group(1)), float(m.group(2))
    if lon > 180:
        lon -= 360
    parts = text.split("date and time", 1)
    if len(parts) < 2:
        return None
    first = parts[1].lstrip().split(None, 1)[0] if parts[1].strip() else ""
    return lon, lat, first == "FACTOR"
