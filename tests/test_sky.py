"""Sun and moon for the forecast tables (sky.py, plan section 35).

Reference times: USNO's one-day rise/set/twilight service (https://aa.usno.navy.mil/api/rstt/oneday), fetched
2026-10-03, rounded to the minute as USNO prints them."""
import os
import sys
from datetime import date, datetime, timedelta, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

pytest.importorskip("ephem")
import sky  # noqa: E402

USNO = {
    # name: (lat, lon, zone, date, {kind: "HH:MM" or None}, lit %)
    "Honolulu": (21.31, -157.86, "Pacific/Honolulu", date(2024, 6, 21),
                 {"dawn": "05:26", "sunrise": "05:51", "sunset": "19:16", "dusk": "19:41",
                  "moonrise": "19:34", "moonset": "05:22"}, 100),
    "Sydney": (-33.86, 151.21, "Australia/Sydney", date(2026, 1, 15),
               {"dawn": "05:31", "sunrise": "05:59", "sunset": "20:09", "dusk": "20:37",
                "moonrise": "02:21", "moonset": "17:32"}, 13),
    # never darker than civil twilight; the evening's sunset falls at 00:03 the next morning
    "Reykjavik": (64.15, -21.94, "Atlantic/Reykjavik", date(2026, 6, 20),
                  {"dawn": None, "sunrise": "02:55", "sunset": "00:03", "dusk": None,
                   "moonrise": "11:33", "moonset": "01:20"}, 35),
    # the moon sets but does not rise that day
    "Nazare": (39.60, -9.07, "Europe/Lisbon", date(2026, 10, 3),
               {"dawn": "07:07", "sunrise": "07:34", "sunset": "19:16", "dusk": "19:43",
                "moonrise": None, "moonset": "15:14"}, 51),
}



@pytest.mark.parametrize("name", sorted(USNO))
def test_day_times_match_usno(name):
    lat, lon, tz, d, want, lit = USNO[name]
    got = sky.day_summary(lat, lon, tz, [d])[d]
    for kind, w in want.items():
        g = got[kind]
        if w is None:
            assert g is None, (kind, g)
        else:
            assert g is not None and g.date() == d, (kind, g)
            assert g.strftime("%H:%M") == w, (kind, g, w)          # to the minute, rounded as USNO prints
    assert abs(got["moon"]["pct"] - lit) <= 3
    assert got["sky"] == "normal"


def test_polar_day_and_night():
    lat, lon, tz = 78.22, 15.65, "Arctic/Longyearbyen"
    summer = sky.day_summary(lat, lon, tz, [date(2026, 6, 21)])[date(2026, 6, 21)]
    winter = sky.day_summary(lat, lon, tz, [date(2026, 12, 21)])[date(2026, 12, 21)]
    assert summer["sky"] == "midnight sun" and winter["sky"] == "polar night"
    assert all(summer[k] is None for k in ("dawn", "sunrise", "sunset", "dusk"))
    rows_s = sky.annotate_rows([datetime(2026, 6, 21, h) for h in range(24)], lat, lon, tz)
    rows_w = sky.annotate_rows([datetime(2026, 12, 21, h) for h in range(24)], lat, lon, tz)
    assert {r["state"] for r in rows_s} == {"day"}
    assert {r["state"] for r in rows_w} == {"night"}
    assert not any(e["kind"] in sky.SUN_EVENTS for r in rows_s + rows_w for e in r["events"])


def test_white_night_counts_as_daylight():
    """Reykjavik in June: the sun never gets 6 degrees under the horizon, so every row lies between first light and
    last light and reads as daylight (owner: no twilight look)."""
    rows = sky.annotate_rows([datetime(2026, 6, 20, h) for h in range(24)], 64.15, -21.94, "Atlantic/Reykjavik")
    assert {r["state"] for r in rows} == {"day"}
    assert [e["text"] for e in rows[0]["events"] if e["kind"] == "sunset"] == ["☀↓ 12:03"]
    obs = sky._observer(64.15, -21.94)
    assert -6 < sky.sun_alt(obs, sky.to_utc(__import__("pytz").timezone("Atlantic/Reykjavik"), datetime(2026, 6, 20, 1))) < -0.8333


def _forecast_times(start, hourly=121, total=385, step=3):
    return [start + timedelta(hours=h) for h in range(hourly)] + \
        [start + timedelta(hours=h) for h in range(hourly + 2, total, step)]


def test_every_event_lands_once_in_the_row_whose_slot_holds_it():
    """Hourly rows to +120 h, then 3-hourly (a point's table): each event of the range appears exactly once, in the
    row whose [t, next t) holds it."""
    import pytz
    tz = pytz.timezone("Pacific/Honolulu")
    times = _forecast_times(datetime(2026, 10, 3, 2, 0))
    rows = sky.annotate_rows(times, 21.67, -158.12, "Pacific/Honolulu")
    start = sky.to_utc(tz, times[0])
    end = sky.to_utc(tz, times[-1]) + timedelta(hours=3)
    want = sky.events(21.67, -158.12, start, end)
    got = [(i, e["kind"], e["time"]) for i, r in enumerate(rows) for e in r["events"]]
    assert len(got) == len(want) > 90
    for (i, kind, t), (u, wkind) in zip(got, want):
        assert kind == wkind
        lo = times[i]
        hi = times[i + 1] if i + 1 < len(times) else times[i] + timedelta(hours=3)
        assert lo <= sky.local_naive(u, tz) < hi + timedelta(minutes=1)
    late = [r for t, r in zip(times, rows) if t >= times[0] + timedelta(hours=124)]
    assert sum(1 for r in late for e in r["events"] if e["kind"] == "sunrise") >= 9       # 3-hourly rows keep them


def test_a_row_partly_in_daylight_is_a_daylight_row():
    """Owner: "any row at least partially within daylight hours should appear the same as full daylight" (daylight =
    first light to last light). Honolulu, 3 Oct 2026 (USNO): first light 6:02, sunrise 6:24, sunset 6:18 PM, last
    light 6:40 PM: the 6 AM row (6:00-7:00, first light at 6:02) and the 6 PM row (last light at 6:40) are daylight;
    the 5 AM and 7 PM rows are wholly dark."""
    times = [datetime(2026, 10, 3, h) for h in range(24)]
    rows = sky.annotate_rows(times, 21.67, -158.12, "Pacific/Honolulu")
    assert [r["state"] for r in rows] == ["night"] * 6 + ["day"] * 13 + ["night"] * 5
    assert [e["text"] for e in rows[6]["events"]] == ["◐ 6:02", "☀↑ 6:24"]
    assert [e["text"] for e in rows[18]["events"]] == ["☀↓ 6:18", "◑ 6:40"]
    # every row holding a sun event is a daylight row (first light makes it one, the others follow it)
    assert all(r["state"] == "day" for r in rows for e in r["events"] if e["kind"] in sky.SUN_EVENTS)


def test_the_minute_of_first_light_and_last_light():
    """Rows a minute apart around the exact instants: a slot that ends before first light is night, one that holds it
    is day; a row starting before last light is day, one starting after it night."""
    import pytz
    tz = pytz.timezone("Pacific/Honolulu")
    start = sky.to_utc(tz, datetime(2026, 10, 3, 0))
    evs = dict((k, u) for u, k in sky.events(21.67, -158.12, start, start + timedelta(hours=24)))
    dawn = evs["dawn"].astimezone(tz).replace(tzinfo=None, second=0, microsecond=0)    # the minute holding it
    dusk = evs["dusk"].astimezone(tz).replace(tzinfo=None, second=0, microsecond=0)
    m = timedelta(minutes=1)
    rows = sky.annotate_rows([dawn - 2 * m, dawn - m, dawn, dawn + 2 * m], 21.67, -158.12, "Pacific/Honolulu")
    assert [r["state"] for r in rows] == ["night", "night", "day", "day"]   # [d-2, d-1) and [d-1, d) end before it
    rows = sky.annotate_rows([dusk - m, dusk, dusk + m, dusk + 2 * m], 21.67, -158.12, "Pacific/Honolulu")
    assert [r["state"] for r in rows] == ["day", "day", "night", "night"]


def test_moon_badge_once_per_night_and_day_first_and_now():
    times = [datetime(2026, 10, 3, 0, 0) + timedelta(hours=h) for h in range(48)]
    now = sky.to_utc(__import__("pytz").timezone("Pacific/Honolulu"), datetime(2026, 10, 3, 14, 20))
    rows = sky.annotate_rows(times, 21.67, -158.12, "Pacific/Honolulu", now_utc=now)
    runs = 0
    for i, r in enumerate(rows):
        starts = r["state"] == "night" and (i == 0 or rows[i - 1]["state"] != "night")
        runs += starts
        assert bool(r["moon"]) == starts
    assert runs == 3                                     # the first morning, then two evenings
    assert [i for i, r in enumerate(rows) if r["day_first"]] == [0, 24]
    assert [i for i, r in enumerate(rows) if r["now"]] == [14]
    m = rows[0]["moon"]
    assert m["glyph"] in sky.MOON_GLYPHS and 0 <= m["pct"] <= 100 and m["name"] in sky.MOON_NAMES


def test_moon_glyph_is_mirrored_south_of_the_equator():
    u = datetime(2026, 1, 15, 2, 0, tzinfo=timezone.utc)                 # a waning crescent
    north, south = sky.moon_at(u, 21.0), sky.moon_at(u, -33.0)
    assert north["name"] == south["name"] == "Waning crescent"
    assert north["glyph"] == "\U0001F318" and south["glyph"] == "\U0001F312"
    full = datetime(2024, 6, 22, 1, 0, tzinfo=timezone.utc)
    assert sky.moon_at(full, 21.0)["glyph"] == sky.moon_at(full, -33.0)["glyph"] == "\U0001F315"


def test_clock_falling_back_keeps_rows_in_order():
    """Los Angeles, 1 Nov 2026: the wall time 1:00 AM happens twice; both rows are read, in order, no event twice."""
    times = [datetime(2026, 11, 1, 0), datetime(2026, 11, 1, 1), datetime(2026, 11, 1, 1), datetime(2026, 11, 1, 2)]
    import pytz
    tz = pytz.timezone("America/Los_Angeles")
    us, prev = [], None
    for t in times:
        prev = sky.to_utc(tz, t, prev)
        us.append(prev)
    assert us == sorted(us) and len(set(us)) == 4
    rows = sky.annotate_rows(times + [datetime(2026, 11, 1, h) for h in range(3, 24)], 33.9, -118.4,
                             "America/Los_Angeles")
    kinds = [e["kind"] for r in rows for e in r["events"]]
    assert kinds.count("sunrise") == 1 and kinds.count("sunset") == 1


def test_unreadable_rows_and_missing_inputs():
    rows = sky.annotate_rows([datetime(2026, 10, 3, 5), None, datetime(2026, 10, 3, 7)], 21.67, -158.12,
                             "Pacific/Honolulu")
    assert rows[1] == {"state": None, "events": [], "moon": None, "day_first": False, "now": False}
    assert rows[0]["state"] and rows[2]["state"]
    assert sky.annotate_rows([datetime(2026, 10, 3, 5)], None, None, "Pacific/Honolulu") is None
    assert sky.annotate_rows([datetime(2026, 10, 3, 5)], 21.0, -158.0, "Not/AZone") is None
    assert sky.annotate_rows([None, None], 21.0, -158.0, "Pacific/Honolulu") is None
    assert sky.day_summary(21.0, -158.0, "Pacific/Honolulu", []) is None


def test_without_ephem_nothing_is_computed(monkeypatch):
    monkeypatch.setattr(sky, "AVAILABLE", False)
    assert sky.annotate_rows([datetime(2026, 10, 3, 5)], 21.0, -158.0, "Pacific/Honolulu") is None
    assert sky.day_summary(21.0, -158.0, "Pacific/Honolulu", [date(2026, 10, 3)]) is None
    assert sky.events(21.0, -158.0, datetime(2026, 10, 3, tzinfo=timezone.utc),
                      datetime(2026, 10, 4, tzinfo=timezone.utc)) is None


def test_event_text_and_glyphs():
    import pytz
    tz = pytz.timezone("Pacific/Honolulu")
    u = sky.to_utc(tz, datetime(2026, 10, 3, 18, 17, 40))
    assert sky.event_text(u, tz, "sunset") == "☀↓ 6:18"          # rounded to the minute, 12-hour, no zero
    assert sky.event_text(sky.to_utc(tz, datetime(2026, 10, 4, 0, 40)), tz, "moonrise") == "☾↑ 12:40"
    assert set(sky.EVENT_GLYPH) == set(sky.EVENT_KINDS) == set(sky.EVENT_NAME)


def test_a_sixteen_day_forecast_is_quick():
    import time
    sky._events.cache_clear()
    times = _forecast_times(datetime(2026, 10, 3, 2, 0))
    t = time.time()
    rows = sky.annotate_rows(times, -33.86, 151.21, "Australia/Sydney")
    assert len(rows) == len(times) == 209
    assert time.time() - t < 1.0
    hits = sky._events.cache_info().hits
    sky.annotate_rows(times, -33.86, 151.21, "Australia/Sydney")
    assert sky._events.cache_info().hits == hits + 1
    assert sky.events(21.0, -158.0, datetime(2026, 1, 1, tzinfo=timezone.utc),
                      datetime(2026, 2, 1, tzinfo=timezone.utc)) is None          # > 20 days: refused


def test_the_last_row_covers_a_slot_as_long_as_the_step_before_it():
    """3-hourly rows ending at 5 AM: the 6:02 first light and 6:24 sunrise belong to the 5 AM row (5-8 AM)."""
    times = [datetime(2026, 10, 3, 20) + timedelta(hours=3 * k) for k in range(4)]          # 8 PM .. 5 AM
    rows = sky.annotate_rows(times, 21.67, -158.12, "Pacific/Honolulu")
    assert [e["kind"] for e in rows[-1]["events"]] == ["dawn", "sunrise"]
    assert rows[-1]["state"] == "day"                                        # its 5-8 AM slot holds first light (6:02)
    assert [r["state"] for r in rows] == ["night", "night", "night", "day"]


def test_day_summary_keeps_the_first_rise_and_the_last_set(monkeypatch):
    import pytz
    tz = pytz.timezone("Pacific/Honolulu")
    d = date(2026, 10, 3)

    def at(h, m):
        return sky.to_utc(tz, datetime(2026, 10, 3, h, m))
    fake = [(at(1, 0), "sunrise"), (at(2, 0), "sunset"), (at(5, 0), "sunrise"), (at(6, 0), "moonrise"),
            (at(20, 0), "moonrise"), (at(21, 0), "sunset")]
    monkeypatch.setattr(sky, "events", lambda *a: fake)
    got = sky.day_summary(21.67, -158.12, "Pacific/Honolulu", [d])[d]
    assert got["sunrise"].hour == 1 and got["sunset"].hour == 21 and got["moonrise"].hour == 6


# ------------------------------ G24 fix round ------------------------------
def test_event_texts_say_am_or_pm_when_the_half_day_differs_from_the_rows():
    """A 3-hourly 11 PM row holding a 12:40 AM moonrise says so (G24 A-10); events in the row's own half of the day
    keep the short form."""
    import pytz
    tz = pytz.timezone("Pacific/Honolulu")
    times = [datetime(2026, 10, 3, 20) + timedelta(hours=3 * k) for k in range(4)]          # 8 PM, 11 PM, 2 AM, 5 AM
    rows = sky.annotate_rows(times, 21.67, -158.12, "Pacific/Honolulu")
    assert [e["text"] for e in rows[1]["events"]] == ["☾↑ 12:40 AM"]       # 11 PM row, 4 Oct 00:40
    assert [e["text"] for e in rows[3]["events"]] == ["◐ 6:02", "☀↑ 6:25"]           # 4 Oct 5 AM row, its own half: no suffix
    noon = [datetime(2026, 10, 3, 11), datetime(2026, 10, 3, 14)]                            # an 11 AM row holding 1:38 PM
    r = sky.annotate_rows(noon + [datetime(2026, 10, 3, 17)], 21.67, -158.12, "Pacific/Honolulu")
    assert [e["text"] for e in r[0]["events"]] == ["☾↓ 1:38 PM"]
    assert all(" AM" not in e["text"] and " PM" not in e["text"]
               for row in sky.annotate_rows([datetime(2026, 10, 3, h) for h in range(24)], 21.67, -158.12, "Pacific/Honolulu")
               for e in row["events"])                                           # hourly rows: never needed


@pytest.mark.parametrize("day, hour, name", [
    (3, 19, "Waning crescent"),     # 46 % (USNO: Waning Crescent): last quarter was 03:25 that morning, 15.6 h before
    (9, 19, "New moon"),            # 10.8 h before the new moon (10-10 15:50 UTC)
    (10, 19, "Waxing crescent"),    # 13.2 h after it
    (17, 19, "First quarter"),      # 11.2 h before the first quarter (10-18 16:12 UTC)
    (18, 19, "Waxing gibbous"),     # 12.8 h after it
])
def test_moon_names_follow_usno_principal_phases_within_12_hours(day, hour, name):
    """G24 A-3: a principal phase names the moon only within 12 h of its instant; the eight glyphs keep their eighths."""
    import pytz
    u = sky.to_utc(pytz.timezone("Pacific/Honolulu"), datetime(2026, 10, day, hour))
    assert sky.moon_at(u, 21.67)["name"] == name


def test_moon_glyph_is_not_mirrored_on_the_equator():
    u = datetime(2026, 1, 15, 2, 0, tzinfo=timezone.utc)
    assert sky.moon_at(u, 0.0)["glyph"] == sky.moon_at(u, 21.0)["glyph"] == "\U0001F318"
    assert sky.moon_at(u, -0.01)["glyph"] == "\U0001F312"


def test_day_summary_refuses_what_the_event_search_refuses():
    """G24 A-5: a span the search refuses (> 20 days) gives None, not rows labelled midnight sun / polar night."""
    assert sky.day_summary(21.67, -158.12, "Pacific/Honolulu", [date(2026, 10, 3), date(2026, 10, 30)]) is None
    assert sky.day_summary(21.67, -158.12, "Pacific/Honolulu", [date(2026, 10, 3), date(2026, 10, 19)]) is not None


def test_day_summary_moon_is_taken_at_last_light():
    """G24 B-3: the day's moon at nightfall (last light), the evening the detailed table's badge describes."""
    import pytz
    tz = pytz.timezone("Pacific/Honolulu")
    d = date(2026, 10, 4)
    got = sky.day_summary(21.67, -158.12, "Pacific/Honolulu", [d])[d]
    want = sky.moon_at(sky.to_utc(tz, got["dusk"]), 21.67)
    assert abs(got["moon"]["illumination"] - want["illumination"]) < 0.002 and got["moon"]["pct"] == want["pct"]


def test_now_is_the_row_whose_half_open_slot_holds_it():
    import pytz
    tz = pytz.timezone("Pacific/Honolulu")
    times = [datetime(2026, 10, 3, h) for h in range(6)]
    at = sky.to_utc(tz, datetime(2026, 10, 3, 3))                           # exactly on the 3 AM row's start
    rows = sky.annotate_rows(times, 21.67, -158.12, "Pacific/Honolulu", now_utc=at)
    assert [i for i, r in enumerate(rows) if r["now"]] == [3]


def test_the_last_sunset_before_the_polar_night_is_found():
    """Longyearbyen (USNO's one-day service, fetched by G24 reviewer A): the last sunset before the 2026-27 polar night is
    26 Oct 2026 at 12:09 (none after it); the first sunrise after the 2025-26 polar night is 15 Feb 2026 at 11:57 (set
    12:28), then 16 Feb at 11:14. The polar transitions are where the search skips ahead (ephem raises there)."""
    import pytz
    tz = pytz.timezone("Arctic/Longyearbyen")
    lat, lon = 78.22, 15.65
    evs = sky.events(lat, lon, datetime(2026, 10, 20, tzinfo=timezone.utc), datetime(2026, 11, 5, tzinfo=timezone.utc))
    sets = [sky.local_naive(u, tz) for u, k in evs if k == "sunset"]
    assert sets[-1].strftime("%m-%d %H:%M") == "10-26 12:09"
    assert not [u for u, k in evs if k in ("sunrise", "sunset") and sky.local_naive(u, tz).date() > date(2026, 10, 26)]
    evs = sky.events(lat, lon, datetime(2026, 2, 10, tzinfo=timezone.utc), datetime(2026, 2, 20, tzinfo=timezone.utc))
    got = [(sky.local_naive(u, tz).strftime("%m-%d %H:%M"), k) for u, k in evs if k in ("sunrise", "sunset")]
    assert got[:3] == [("02-15 11:57", "sunrise"), ("02-15 12:28", "sunset"), ("02-16 11:14", "sunrise")]
