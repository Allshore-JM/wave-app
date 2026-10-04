"""The window's Summary view (plan section 35): one row per forecast day over its daylight rows (first light to last
light): significant height range + trend, the day's two most powerful swell SYSTEMS, wind, sunrise / sunset, moon."""
import html as H
import re
from datetime import date, datetime, timedelta

import pytest

import app as A


def _row(t, hs=1.0, systems=(), wind=(5.0, 90)):
    """A 23-column row at datetime t: systems = [(hs ft, tp s, dir)] in column order."""
    r = [t.strftime("%A, %B %d, %Y").replace(" 0", " "), t.strftime("%I:%M %p").lstrip("0")]
    cols = list(systems) + [(None, None, None)] * (6 - len(systems))
    for s in cols:
        r += list(s)
    return r + [wind[0], wind[1], hs]


def _ann(state):
    return {"state": state, "events": [], "moon": None, "day_first": False, "now": False}


def _days(d, sunrise=6, sunset=18, sky="normal"):
    return {d: {"sunrise": datetime.combine(d, datetime.min.time()) + timedelta(hours=sunrise) if sunrise else None,
                "sunset": datetime.combine(d, datetime.min.time()) + timedelta(hours=sunset) if sunset else None,
                "sky": sky, "moon": {"glyph": "\U0001F314", "pct": 72, "name": "Waxing gibbous"}}}


@pytest.mark.parametrize("vals, kind", [
    ([1, 1, 1, 2, 2, 2], "rising"),
    ([2, 2, 2, 1, 1, 1], "falling"),
    ([1, 1, 3, 1, 1], "peak"),
    ([1, 1.05, 1, 1.02], "steady"),
    ([1.0], "steady"),
    ([0, 0, 0], "steady"),
    ([1, 1.2, 1.5, 2.0, 2.4], "rising"),          # the maximum at the end is a rise, not a peak
])
def test_trend(vals, kind):
    times = [datetime(2026, 10, 3, 7) + timedelta(hours=i) for i in range(len(vals))]
    got, text = A._trend(vals, times)
    assert got == kind
    if kind == "peak":
        assert text == "peak 9 AM"


def test_swell_systems_follow_a_swell_across_the_hourly_column_swaps():
    """Hour by hour the two systems swap columns (re-ranked by power): the summary still sees two systems."""
    win = []
    for h in range(8):
        a, b = (3.0, 6.5, 45), (2.4, 12.0 + 0.2 * h, 325)
        win.append((None, _row(datetime(2026, 10, 3, 7 + h), systems=[a, b] if h % 2 else [b, a])))
    sys = A._swell_systems(win, lambda ft: ft)
    assert len(sys) == 2
    long, short = sys                                 # 2.4^2 x 12-13.4 s beats 3.0^2 x 6.5 s
    assert (round(long["tp_min"], 1), round(long["tp_max"], 1), long["dir"]) == (12.0, 13.4, 325)
    assert (short["tp_min"], short["tp_max"], short["dir"], short["hs_max"]) == (6.5, 6.5, 45, 3.0)


def test_swell_systems_split_on_direction_and_average_directions_on_the_circle():
    win = [(None, _row(datetime(2026, 10, 3, 9), systems=[(2.0, 12.0, 350), (2.0, 12.0, 200)])),
           (None, _row(datetime(2026, 10, 3, 10), systems=[(2.0, 12.0, 10)]))]
    sys = A._swell_systems(win, lambda ft: ft)
    assert len(sys) == 2 and sys[0]["dir"] == 0 and sys[1]["dir"] == 200
    assert A._circular_mean([350, 10], [1, 1]) == 0 and A._circular_mean([None], [1]) is None


def test_summary_days_daylight_only_with_partial_and_missing_days():
    d0, d1 = date(2026, 10, 3), date(2026, 10, 4)
    rows, ann = [], []
    for h in range(9, 24):                            # day 0 from 9 AM; day 1 only up to 2 AM: no daylight
        t = datetime(2026, 10, 3, h)
        state = "day" if 9 <= h < 18 else "night"
        rows.append(_row(t, hs=2.0 + (0.5 if h == 12 else 0), systems=[(1.5, 10.0, 300)], wind=(4.4704, 270)))
        ann.append(_ann(state))
    for h in range(0, 3):
        rows.append(_row(datetime(2026, 10, 4, h), systems=[(1.5, 10.0, 300)]))
        ann.append(_ann("night"))
    days = {**_days(d0), **_days(d1)}
    out = A.summary_days(rows, ann, days, "US")
    assert [o["date"] for o in out] == [d0]           # day 1 has no daylight row in the forecast: left out
    day = out[0]
    assert day["note"] == "from 9:00 AM" and day["hs"]["min"] == 2.0 and day["hs"]["max"] == 2.5
    assert day["hs"]["trend"] == "peak" and day["wind"] == {"min": 10, "max": 10, "dir": 270}
    metric = A.summary_days(rows, ann, days, "Metric")[0]
    assert round(metric["hs"]["max"], 3) == round(2.5 / 3.28084, 3) and metric["wind"]["min"] == 16


def test_summary_keeps_a_polar_night_with_a_note():
    d = date(2026, 12, 21)
    rows = [_row(datetime(2026, 12, 21, h)) for h in range(24)]
    out = A.summary_days(rows, [_ann("night")] * 24, _days(d, None, None, "polar night"), "US")
    assert len(out) == 1 and out[0]["note"] == "Polar night" and out[0]["hs"] is None


def test_summary_html():
    d = date(2026, 10, 3)
    rows = [_row(datetime(2026, 10, 3, h), hs=2.0 + 0.2 * h, systems=[(1.8, 13.0, 315), (1.0, 7.0, 60)],
                 wind=(6.7056, 70)) for h in range(6, 19)]
    html = A.build_summary_html(rows, [_ann("day")] * len(rows), _days(d), "US")
    assert html.isascii()                                                 # glyphs as numeric references (G24 A-1)
    assert html.startswith('<table class="table table-bordered table-sm forecast-summary">')
    heads = re.findall(r"<th[^>]*>([^<]+)</th>", html)
    assert heads == ["Day", "Sig. Wave Height", "Dominant Swell", "Second Swell", "Wind", "Sunrise", "Sunset", "Moon"]
    body = html.split("<tbody>")[1]
    assert body.count("<tr") == 1 and 'data-date="2026-10-03"' in body
    assert '<span class="sum-range">3.2&ndash;5.6 ft</span>' in body and "trend-rising" in body
    text = H.unescape(body)
    assert "\u2197\ufe0e rising" in text                                  # text presentation, not the emoji (G24 B-4)
    assert "1.8 ft</span> &middot; 13 s &middot;" in body and "315&deg; NW" in body
    assert "1.0 ft</span> &middot; 7 s &middot;" in body and "60&deg; ENE" in body
    assert "15 mph &middot; 70&deg; ENE" in body
    assert "\u2600\u2191 6:00 AM" in text and "\u2600\u2193 6:00 PM" in text
    assert 'title="Waxing gibbous, 72% illuminated">\U0001F314 72%</span>' in text
    assert A.build_summary_html([], [], {}, "US").count("<tr") == 1          # header only


def _patch_bull(monkeypatch, rows):
    monkeypatch.setattr(A, "parse_bull", lambda station, tz: ("Cycle : 20261003 00 UTC",
                                                              "Location : 51201      (21.67N 158.12W)", "",
                                                              [list(r) for r in rows], "Pacific/Honolulu", None))
    monkeypatch.setattr(A, "load_station_coords", lambda: {})


def test_the_payload_and_the_full_render_carry_the_summary(monkeypatch):
    rows = [_row(datetime(2026, 10, 3) + timedelta(hours=h), hs=2.0, systems=[(1.5, 12.0, 320)]) for h in range(48)]
    _patch_bull(monkeypatch, rows)
    d = A.compute_forecast_payload("51201", None, "US", "GFS", compact=True)
    assert d["summary_html"].count('class="sum-day') == 2                 # two whole days
    assert A.compute_forecast_payload("51201", None, "US", "GFS")["summary_html"] is None     # the classic payload
    page = A.app.test_client().get("/?station=51201&render=full").get_data(as_text=True)
    assert "forecast-summary" in page                                     # in the window's initial state (JSON)

    def boom(*a, **k):
        raise RuntimeError("no sky")
    monkeypatch.setattr(A.sky, "day_summary", boom)
    d = A.compute_forecast_payload("51201", None, "US", "GFS", compact=True)
    assert d["summary_html"] is None and "col-sun" in d["table_html"]      # the detailed table stands without it


# ------------------------------ G24 fix round ------------------------------
def _hourly(d, first_hour, last_hour, step=1, hs=2.0):
    rows, ann = [], []
    for h in range(first_hour, last_hour + 1, step):
        t = datetime.combine(d, datetime.min.time()) + timedelta(hours=h)
        rows.append(_row(t, hs=hs, systems=[(1.5, 10.0, 300)]))
        ann.append(_ann("day" if 6 <= h <= 18 else "night"))
    return rows, ann


@pytest.mark.parametrize("first, last, step, note", [
    (0, 23, 3, None),                      # a whole 3-hourly day: rows at 0, 3, ..., 21
    (2, 23, 3, None),                      # rows at 2, 5, 8, ...: its first daylight row 8 AM is NOT a cut (G24 A-2 / B-2)
    (1, 23, 3, None),                      # rows at 1, 4, ..., 22: its last daylight row 4 PM is NOT a cut either
    (0, 23, 1, None),                      # a whole hourly day
    (9, 23, 1, "from 9:00 AM"),            # the forecast starts after this day's sunrise + 1 h
    (0, 14, 1, "until 2:00 PM"),           # the forecast ends before this day's sunset - 1 h
    (9, 14, 1, "from 9:00 AM until 2:00 PM"),   # both (the elif hid the second)
    (7, 23, 1, None),                      # within an hour of sunrise: not a cut
    (0, 17, 1, None),                      # within an hour of sunset: not a cut
])
def test_the_cut_day_note_comes_from_the_forecasts_own_first_and_last_row(first, last, step, note):
    d = date(2026, 10, 9)
    rows, ann = _hourly(d, first, last, step)
    out = A.summary_days(rows, ann, _days(d), "US")
    assert out[0]["note"] == note


def test_equal_ends_are_one_height():
    d = date(2026, 10, 3)
    rows = [_row(datetime(2026, 10, 3, h), hs=1.8, systems=[(1.0, 9.0, 300)]) for h in range(6, 19)]
    body = A.build_summary_html(rows, [_ann("day")] * len(rows), _days(d), "US").split("<tbody>")[1]
    assert '<span class="sum-range">1.8 ft</span>' in body and "1.8&ndash;1.8" not in body
    rows[3] = _row(datetime(2026, 10, 3, 9), hs=1.84, systems=[(1.0, 9.0, 300)])    # 1.84 shows as 1.8 too
    body = A.build_summary_html(rows, [_ann("day")] * len(rows), _days(d), "US").split("<tbody>")[1]
    assert '<span class="sum-range">1.8 ft</span>' in body


def test_the_summary_moon_is_tonights_badge():
    """The Moon column shows the moon the detailed table's badge shows for that evening (G24 B-3); without a badge
    that day (the last day, a polar day) the day's own value."""
    d = date(2026, 10, 3)
    rows, ann = _hourly(d, 0, 23)
    badge = {"glyph": "\U0001F318", "pct": 32, "name": "Waning crescent"}
    morning = {"glyph": "\U0001F317", "pct": 40, "name": "Waning crescent"}
    ann[0] = dict(ann[0], moon=morning)                # the night that started the evening before
    ann[19] = dict(ann[19], moon=badge)                # tonight's (7 PM)
    out = A.summary_days(rows, ann, _days(d), "US")
    assert out[0]["moon"] is badge
    text = H.unescape(A.build_summary_html(rows, ann, _days(d), "US"))
    assert "\U0001F318 32%" in text and "\U0001F314 72%" not in text
    ann[19] = dict(ann[19], moon=None)
    assert A.summary_days(rows, ann, _days(d), "US")[0]["moon"]["pct"] == 72     # day_summary's value


@pytest.mark.parametrize("vals, kind", [
    ([1, 1, 1, 1, 1, 1.25], "rising"),     # thirds: the last third 1.125 >= 1.10 x 1.0 (halves would say steady)
    ([1, 1, 1, 1.12, 1.12, 1.12], "rising"),
    ([1, 1, 1, 1.08, 1.08, 1.08], "steady"),   # the 10 % band: 8 % is steady
    ([1, 1, 1, 0.92, 0.92, 0.92], "steady"),
    ([1, 1, 1, 0.88, 0.88, 0.88], "falling"),
    ([1, 1.12, 1], "peak"),                # an interior maximum 12 % above both ends
    ([1, 1.07, 1], "steady"),              # 7 %: not a peak
    ([1, 1, 1, 1, 1, 2], "rising"),        # a maximum at the window's end is a rise, never a peak
    ([2, 1, 1, 1, 1, 1], "falling"),       # nor at its start
])
def test_trend_thresholds(vals, kind):
    times = [datetime(2026, 10, 3, 7) + timedelta(hours=i) for i in range(len(vals))]
    assert A._trend(vals, times)[0] == kind


def test_swell_systems_windows_and_ranking():
    """Grouping: within 20 % of the period (at least 1.5 s) and 40 deg of the direction; ranked by summed Hs^2 x Tp;
    the height shown is the system's largest; the direction is the power-weighted mean."""
    def systems(samples):
        win = [(None, _row(datetime(2026, 10, 3, 9 + i), systems=[s])) for i, s in enumerate(samples)]
        return A._swell_systems(win, lambda ft: ft)
    # the window centres on the most powerful sample (the first here: 2.5^2 x 10 = 62.5 > 2^2 x 12.1 = 48.4)
    assert len(systems([(2.5, 10.0, 300), (2.0, 12.0, 300)])) == 1          # 2 s apart = 20 % of 10 s
    assert len(systems([(2.5, 10.0, 300), (2.0, 12.1, 300)])) == 2
    assert len(systems([(3.0, 5.0, 300), (2.0, 6.5, 300)])) == 1            # the 1.5 s floor beats 20 % of 5 s
    assert len(systems([(3.0, 5.0, 300), (2.0, 6.6, 300)])) == 2
    assert len(systems([(2.5, 10.0, 300), (2.0, 10.0, 340)])) == 1          # 40 deg
    assert len(systems([(2.5, 10.0, 300), (2.0, 10.0, 341)])) == 2
    one = systems([(3.0, 10.0, 300), (1.0, 10.0, 330)])[0]
    assert one["hs_max"] == 3.0 and one["dir"] == 303                       # power weights 90 : 10 (the mean of 300, 330)
    a, b = systems([(2.0, 16.0, 200), (2.6, 9.0, 40)])                      # 64 vs 60.8: Hs^2 x Tp, not Hs x Tp
    assert a["tp_min"] == 16.0 and b["tp_min"] == 9.0
    a, b = systems([(3.0, 8.0, 40), (2.0, 15.0, 200), (2.0, 15.0, 200), (2.0, 15.0, 200)])   # the summed power re-ranks
    assert a["tp_min"] == 15.0 and round(a["power"]) == 180 and round(b["power"]) == 72


def test_wind_speeds_round_and_the_mean_is_unweighted():
    d = date(2026, 10, 3)
    rows = [_row(datetime(2026, 10, 3, 9), wind=(4.25, 350)), _row(datetime(2026, 10, 3, 10), wind=(10.0, 10))]
    out = A.summary_days(rows, [_ann("day")] * 2, _days(d), "US")[0]
    assert out["wind"] == {"min": 10, "max": 22, "dir": 0}                  # 9.507 -> 10, 22.37 -> 22; (350 + 10) / 2


def test_rows_without_a_state_are_not_daylight():
    d = date(2026, 10, 3)
    rows = [_row(datetime(2026, 10, 3, h), hs=1.0 + h) for h in range(9, 12)]
    ann = [_ann("day"), {"state": None, "events": [], "moon": None, "day_first": False, "now": False}, _ann("day")]
    out = A.summary_days(rows, ann, _days(d), "US")[0]
    assert (out["hs"]["min"], out["hs"]["max"]) == (10.0, 12.0)            # the unreadable 10 AM row is left out
