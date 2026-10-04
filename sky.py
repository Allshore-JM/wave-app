"""Sun and moon for the forecast tables and graphs (plan section 35).

Everything here is computed by PyEphem for a place (lat/lon) and a forecast zone (IANA name), and handed back in the
rows' own convention: naive local times in that zone, the way the parsers write a row's date and time.

- Sky state of a row (day / twilight / night) comes from the SUN'S ALTITUDE AT THE ROW'S TIME, not from comparing the
  row with that date's sunrise and sunset: in Reykjavik on 20 June the evening's sunset falls at 00:03 the next morning
  and the sun never gets 6 degrees below the horizon (USNO), and above the polar circles there are days with no sunrise
  at all; the altitude needs no special cases. Day = the sun's centre above -0.833 deg (USNO's sunrise: upper limb on a
  refracted horizon), twilight = above -6 deg (civil twilight: first light / last light), night = below. A row whose
  slot holds first light, sunrise, sunset or last light is a twilight row whatever the altitude at its start (near the
  equator twilight lasts ~23 min, so hourly rows would otherwise almost never show it).
- Events (first light, sunrise, sunset, last light, moonrise, moonset) are searched over the whole forecast range, so an
  event after midnight belongs to the row it falls in. Checked against USNO (tests/test_sky.py): within 1 min.
- The moon's phase (fraction of the synodic month since the last new moon) and lit fraction are taken at the time asked.

Without PyEphem (`AVAILABLE` False) every function returns None and the site serves its tables as before."""
import functools
import math
from datetime import datetime, timedelta, timezone

import pytz

try:
    import ephem
except ImportError:                       # the site still works: tables keep their old day/night look
    ephem = None

AVAILABLE = ephem is not None

DAY_ALT = -0.8333                         # deg, the sun's centre at sunrise / sunset (USNO convention)
TWILIGHT_ALT = -6.0                       # deg, civil twilight: first light / last light
EVENT_KINDS = ("dawn", "sunrise", "sunset", "dusk", "moonrise", "moonset")
SUN_EVENTS = frozenset(("dawn", "sunrise", "sunset", "dusk"))
EVENT_GLYPH = {"dawn": "◐", "sunrise": "☀↑", "sunset": "☀↓", "dusk": "◑",
               "moonrise": "☾↑", "moonset": "☾↓"}
EVENT_NAME = {"dawn": "First light", "sunrise": "Sunrise", "sunset": "Sunset", "dusk": "Last light",
              "moonrise": "Moonrise", "moonset": "Moonset"}
MOON_GLYPHS = ("\U0001F311", "\U0001F312", "\U0001F313", "\U0001F314",
               "\U0001F315", "\U0001F316", "\U0001F317", "\U0001F318")
MOON_NAMES = ("New moon", "Waxing crescent", "First quarter", "Waxing gibbous",
              "Full moon", "Waning gibbous", "Last quarter", "Waning crescent")
MAX_SPAN = timedelta(days=20)             # a forecast is 16 days; refuse to search more


def _observer(lat, lon):
    obs = ephem.Observer()
    obs.lat, obs.lon = math.radians(lat), math.radians(lon)
    obs.elevation = 0
    obs.pressure = 0                      # no refraction model: the horizons below carry USNO's allowances
    return obs


def _edate(utc):
    return ephem.Date(utc.replace(tzinfo=None))


def _utc(edate):
    return ephem.Date(edate).datetime().replace(tzinfo=timezone.utc)


def to_utc(tz, naive, after=None):
    """A row's naive local time -> aware UTC. In the hour a clock falls back the wall time exists twice: the first
    instant later than `after` (the previous row) is taken, so rows stay in order; a time skipped by a clock going
    forward is read as standard time."""
    cands = []
    for dst in (True, False):
        try:
            cands.append(tz.normalize(tz.localize(naive, is_dst=dst)).astimezone(timezone.utc))
        except Exception:
            pass
    if not cands:
        return None
    cands.sort()
    if after is not None:
        for c in cands:
            if c > after:
                return c
    return cands[0]


def sun_state(obs, utc):
    """'day', 'twilight' or 'night' from the sun's altitude at `utc` (aware)."""
    obs.date = _edate(utc)
    alt = math.degrees(float(ephem.Sun(obs).alt))
    if alt > DAY_ALT:
        return "day"
    if alt > TWILIGHT_ALT:
        return "twilight"
    return "night"


def moon_at(utc, lat):
    """The moon at `utc`: phase (0 new, 0.5 full), lit fraction, glyph and name. The emoji show the moon as seen from
    the northern hemisphere (a waxing crescent lit on the right); south of the equator the lit side is mirrored."""
    d = _edate(utc)
    prev, nxt = ephem.previous_new_moon(d), ephem.next_new_moon(d)
    phase = (float(d) - float(prev)) / (float(nxt) - float(prev))
    lit = float(ephem.Moon(d).moon_phase)
    i = int(phase * 8 + 0.5) % 8
    g = (8 - i) % 8 if lat < 0 else i
    return {"phase": phase, "illumination": lit, "glyph": MOON_GLYPHS[g], "name": MOON_NAMES[i],
            "pct": int(round(lit * 100))}


def _search(obs, body, horizon, use_center, rising, start, end):
    """Every rising (or setting) of `body` across the horizon in [start, end) (aware UTC)."""
    out, t = [], start
    obs.horizon = horizon
    while t < end:
        obs.date = _edate(t)
        try:
            e = (obs.next_rising if rising else obs.next_setting)(body, use_center=use_center)
        except (ephem.AlwaysUpError, ephem.NeverUpError):
            t += timedelta(hours=12)       # no crossing near this time (polar day or night): look further on
            continue
        u = _utc(e)
        if u >= end:
            break
        if not out or u > out[-1] + timedelta(minutes=1):
            out.append(u)
        t = u + timedelta(minutes=1)
    return out


@functools.lru_cache(maxsize=512)
def _events(lat2, lon2, start_ts, end_ts):
    start = datetime.fromtimestamp(start_ts, timezone.utc)
    end = datetime.fromtimestamp(end_ts, timezone.utc)
    obs = _observer(lat2, lon2)
    sun, moon = ephem.Sun(), ephem.Moon()
    found = []
    for kind, body, horizon, center, rising in (
            ("dawn", sun, "-6", True, True), ("sunrise", sun, "-0:34", False, True),
            ("sunset", sun, "-0:34", False, False), ("dusk", sun, "-6", True, False),
            ("moonrise", moon, "-0:34", False, True), ("moonset", moon, "-0:34", False, False)):
        for u in _search(obs, body, horizon, center, rising, start, end):
            found.append((u, kind))
    found.sort(key=lambda e: (e[0], EVENT_KINDS.index(e[1])))
    return tuple(found)


def events(lat, lon, start_utc, end_utc):
    """[(aware UTC, kind)] of the six events in [start_utc, end_utc), in time order. Cached per place (to 0.01 deg)
    and range (whole minutes)."""
    if not AVAILABLE or end_utc <= start_utc or end_utc - start_utc > MAX_SPAN:
        return None
    s = int(start_utc.timestamp()) // 60 * 60
    e = -(-int(end_utc.timestamp()) // 60) * 60
    return list(_events(round(lat, 2), round(lon, 2), s, e))


def local_naive(utc, tz):
    """Local wall time to the nearest minute (as USNO and almanacs print it)."""
    t = (utc + timedelta(seconds=30)).astimezone(tz).replace(tzinfo=None)
    return t.replace(second=0, microsecond=0)


def event_text(utc, tz, kind):
    """'☀↑ 6:23' (the hour without a leading zero; the row's own time says AM or PM)."""
    t = local_naive(utc, tz)
    return "%s %d:%02d" % (EVENT_GLYPH[kind], (t.hour % 12) or 12, t.minute)


def annotate_rows(times, lat, lon, tz_name, now_utc=None):
    """Per row (naive local times in `tz_name`, oldest first; None for a row whose time could not be read):
    {state, events: [{kind, text, name, time}], moon (on the first row of each run of night rows), day_first, now}.

    Row i covers [t_i, t_i+1): an hourly row shows the events of its hour, a 3-hourly row those of its three hours, and
    the last row a slot as long as the step before it. Returns None without PyEphem, coordinates or a zone."""
    if not AVAILABLE or lat is None or lon is None or not tz_name:
        return None
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        return None
    utcs, prev = [], None
    for t in times:
        u = to_utc(tz, t, prev) if t is not None else None
        utcs.append(u)
        if u is not None:
            prev = u
    known = [u for u in utcs if u is not None]
    if not known:
        return None
    ends = [None] * len(utcs)
    for i, u in enumerate(utcs):
        if u is None:
            continue
        nxt = next((v for v in utcs[i + 1:] if v is not None), None)
        if nxt is not None and nxt > u:
            ends[i] = nxt
        else:
            before = [v for v in utcs[:i] if v is not None]
            step = (u - before[-1]) if before and u > before[-1] else timedelta(hours=1)
            ends[i] = u + step
    evs = events(lat, lon, known[0], max(e for e in ends if e is not None)) or []
    now_utc = now_utc or datetime.now(timezone.utc)
    obs = _observer(lat, lon)
    out, j, prev_state, prev_date = [], 0, None, None
    for i, u in enumerate(utcs):
        if u is None:
            out.append({"state": None, "events": [], "moon": None, "day_first": False, "now": False})
            prev_state = None
            continue
        while j < len(evs) and evs[j][0] < u:
            j += 1
        mine = []
        k = j
        while k < len(evs) and evs[k][0] < ends[i]:
            eu, kind = evs[k]
            mine.append({"kind": kind, "text": event_text(eu, tz, kind), "name": EVENT_NAME[kind],
                         "time": local_naive(eu, tz)})
            k += 1
        state = sun_state(obs, u)
        if any(e["kind"] in SUN_EVENTS for e in mine):
            state = "twilight"             # first light .. last light pass in this row's slot: a transition row
        local = times[i]
        out.append({
            "state": state,
            "events": mine,
            "moon": moon_at(u, lat) if state == "night" and prev_state != "night" else None,
            "day_first": prev_date is None or local.date() != prev_date,
            "now": u <= now_utc < ends[i],
        })
        prev_state, prev_date = state, local.date()
    return out


def day_summary(lat, lon, tz_name, dates):
    """{date: {dawn, sunrise, sunset, dusk, moonrise, moonset (naive local or None), sky ('normal' | 'midnight sun' |
    'polar night'), moon (moon_at local noon)}} for the summary view. A date's events are those whose LOCAL date is
    that date (first light and sunrise the first of the day, sunset and last light the last)."""
    if not AVAILABLE or lat is None or lon is None or not tz_name or not dates:
        return None
    try:
        tz = pytz.timezone(tz_name)
    except Exception:
        return None
    first, last = min(dates), max(dates)
    start = to_utc(tz, datetime.combine(first, datetime.min.time()))
    end = to_utc(tz, datetime.combine(last + timedelta(days=1), datetime.min.time()))
    evs = events(lat, lon, start, end) or []
    obs = _observer(lat, lon)
    out = {}
    for d in sorted(set(dates)):
        row = {k: None for k in EVENT_KINDS}
        for eu, kind in evs:
            t = local_naive(eu, tz)
            if t.date() != d:
                continue
            if kind in ("dawn", "sunrise", "moonrise") and row[kind] is not None:
                continue                   # keep the first
            row[kind] = t
        noon = to_utc(tz, datetime.combine(d, datetime.min.time()) + timedelta(hours=12))
        if row["sunrise"] is None and row["sunset"] is None:
            row["sky"] = "midnight sun" if sun_state(obs, noon) == "day" else "polar night"
        else:
            row["sky"] = "normal"
        row["moon"] = moon_at(noon, lat)
        out[d] = row
    return out
