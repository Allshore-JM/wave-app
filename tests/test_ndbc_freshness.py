"""NDBC observation freshness gate.

A buoy that stops reporting keeps its last rows in NDBC's realtime files for weeks.
The observation window must therefore be anchored to NOW, not to the newest row in
the file, so an offline buoy shows "No Recent Reports" instead of days-old readings
presented as current (the 51213 case: 2026-07-13 data served on 2026-07-25).
"""
import os
import sys
from datetime import datetime, timedelta

import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app  # noqa: E402

NOW = datetime(2026, 7, 25, 18, 0, tzinfo=pytz.utc)


def _spec_line(dt, wvht="1.5"):
    """One NDBC .spec row: YY MM DD hh mm WVHT SwH SwP WWH WWP SwD WWD STEEPNESS APD MWD"""
    return (f"{dt.year} {dt.month:02d} {dt.day:02d} {dt.hour:02d} {dt.minute:02d} "
            f"{wvht} 1.2 12.5 0.8 7.1 NE E AVERAGE 6.2 45")


def _spec_text(dts):
    head = ("#YY  MM DD hh mm WVHT  SwH  SwP  WWH  WWP SwD WWD  STEEPNESS  APD MWD\n"
            "#yr  mo dy hr mn    m    m  sec    m  sec  -   -           -  sec degT\n")
    return head + "\n".join(_spec_line(d) for d in dts) + "\n"


def _summary(monkeypatch, dts, station="51213"):
    monkeypatch.setattr(app, "_fetch_text", lambda *a, **k: _spec_text(dts))
    monkeypatch.setattr(app, "load_station_metadata",
                        lambda: {station: {"name": "Test Buoy", "lat": 21.0, "lon": -158.0}})
    # now_utc pinned so the window boundary is exact, not clock-dependent.
    return app._parse_noaa_station_wave_summary(
        station, hours=app.NDBC_MAX_AGE_HOURS, now_utc=NOW)


# ------------------------------ the reported bug -------------------------------

def test_long_offline_buoy_reports_no_recent(monkeypatch):
    # 51213-style: a full day of observations, but 12 days ago.
    stale = [NOW - timedelta(days=12) - timedelta(minutes=30 * i) for i in range(40)]
    d = _summary(monkeypatch, stale)
    assert d["no_recent_reports"] is True
    assert d["rows"] == [] and d["row_count"] == 0
    assert d["latest"] == {}                 # nothing that could read as current
    assert d["as_of_local"] is None and d["as_of_gmt"] is None
    assert d["last_report_gmt"]               # but we still say WHEN it last reported
    assert d["last_report_age_hours"] > 24


def test_window_is_anchored_to_now_not_newest_row(monkeypatch):
    # THE REGRESSION GUARD: rows spanning a 24h block that ended 3 days ago. Anchoring
    # to rows[0] (the old behavior) would keep every one of them.
    old_block = [NOW - timedelta(days=3) - timedelta(hours=i) for i in range(24)]
    d = _summary(monkeypatch, old_block)
    assert d["no_recent_reports"] is True
    assert d["row_count"] == 0


# ------------------------------ healthy buoys ----------------------------------

def test_reporting_buoy_keeps_last_24h(monkeypatch):
    fresh = [NOW - timedelta(minutes=30 * i) for i in range(48)]  # 24h of half-hourly
    d = _summary(monkeypatch, fresh)
    assert d["no_recent_reports"] is False
    assert d["row_count"] == len(fresh)
    assert d["latest"]["wvht"]                 # latest populated
    assert d["as_of_gmt"] and d["as_of_local"]
    assert d["last_report_age_hours"] < 1


def test_rows_older_than_24h_are_dropped(monkeypatch):
    mixed = [NOW - timedelta(hours=1),        # keep
             NOW - timedelta(hours=23),       # keep
             NOW - timedelta(hours=25),       # drop
             NOW - timedelta(days=9)]         # drop
    d = _summary(monkeypatch, mixed)
    assert d["no_recent_reports"] is False
    assert d["row_count"] == 2


def test_boundary_row_just_inside_window(monkeypatch):
    d = _summary(monkeypatch, [NOW - timedelta(hours=23, minutes=59)])
    assert d["no_recent_reports"] is False and d["row_count"] == 1


def test_empty_file_reports_no_recent(monkeypatch):
    d = _summary(monkeypatch, [])
    assert d["no_recent_reports"] is True
    assert d["last_report_gmt"] is None       # never reported -> nothing to cite
    assert d["last_report_age_hours"] is None


# ------------------------------ the shared helper ------------------------------

def test_ndbc_row_is_recent():
    assert app._ndbc_row_is_recent(NOW - timedelta(hours=1), now_utc=NOW) is True
    assert app._ndbc_row_is_recent(NOW - timedelta(hours=23, minutes=59), now_utc=NOW) is True
    assert app._ndbc_row_is_recent(NOW - timedelta(hours=25), now_utc=NOW) is False
    assert app._ndbc_row_is_recent(NOW - timedelta(days=12), now_utc=NOW) is False
    assert app._ndbc_row_is_recent(None, now_utc=NOW) is False
    # a future timestamp (clock skew) is not "stale"
    assert app._ndbc_row_is_recent(NOW + timedelta(hours=1), now_utc=NOW) is True
