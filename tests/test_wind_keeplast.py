"""Keep-last GFS winds (plan section 108): when the run asked has no winds yet (the GFS winds job has not put it in the
bucket and NOMADS gives nothing), the newest earlier run in the bucket stands in, labelled "GFS HHZ", and the build is
marked so the table is built again soon. 51201's SWAN table on 2026-11-22 showed only its RRFS hours without this.
The bucket and the runs are the GFS winds reader's test world (test_gfs_winds_reader). No network."""
import os
import sys
from datetime import datetime, timedelta

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import app  # noqa: E402
import rrfs_wind  # noqa: E402
from test_gfs_winds_reader import DATE, RUN, world  # noqa: E402,F401  (the fixture)

T0 = datetime(2026, 10, 25, 12)


@pytest.fixture
def no_nomads(world, monkeypatch):   # noqa: F811
    monkeypatch.setattr(app, "NOMADS_DISABLED", True)
    app._RRFS_BUILD.degraded = False
    yield world
    app._RRFS_BUILD.degraded = False


def test_the_run_asked_is_used_when_present(no_nomads):
    got = app._gfs_wind_kept("51201", DATE, RUN)
    assert got == app.get_station_wind("51201", DATE, RUN) and len(got) == 385
    assert all(len(v) == 2 for v in got.values()), "no label on the run asked"
    assert not app._rrfs_degraded_build() and app._gfs_winds_health()["keep_last"] == 0


def test_a_run_not_in_the_bucket_yet_takes_the_previous_run(no_nomads):
    b, nomads, _ = no_nomads
    got = app._gfs_wind_kept("51201", DATE, "18")
    assert len(got) == 385 and min(got) == T0
    assert {v[3] for v in got.values()} == {"GFS 12Z"} and all(v[2] is None for v in got.values())
    base = app.get_station_wind("51201", DATE, RUN)
    assert all(got[t][:2] == base[t] for t in base), "the earlier run's own values"
    assert app._rrfs_degraded_build(), "marked: the table is built again soon"
    h = app._gfs_winds_health()
    assert h["keep_last"] == 1 and h["keep_last_run"] == DATE + RUN and h["run_missing"] == 1
    assert nomads == [], "NOMADS off here; the stand-in never asks NOMADS"


def test_the_latest_run_path(no_nomads, monkeypatch):
    monkeypatch.setattr(app, "get_latest_run", lambda: ("20261026", "00"))   # two cycles past the bucket's run
    got = app._gfs_wind_kept("51201")
    assert len(got) == 385 and {v[3] for v in got.values()} == {"GFS 12Z"}


def test_only_the_bucket_is_asked_for_earlier_runs(world, monkeypatch):   # noqa: F811
    """NOMADS on but giving nothing for the run asked: the earlier cycles are read from the bucket only."""
    b, nomads, _ = world
    monkeypatch.setattr(app, "get_latest_run", lambda: (DATE, "18"))
    got = app._gfs_wind_kept("NOWHERE")                                     # not in the bucket's run, no .spec
    assert got == {} and len(nomads) == 1, "one NOMADS ask: the run asked, never the earlier ones"
    runs = {k.split("/")[3] for k in b.asked if k.endswith("index.json")}
    assert runs == {"2026102518", "2026102512", "2026102506", "2026102500", "2026102418"}, "the run asked + 4 earlier"
    assert not app._rrfs_degraded_build()


def test_nothing_within_the_steps_gives_nothing(no_nomads, monkeypatch):
    late = T0 + timedelta(hours=6 * (app._WIND_KEEPLAST_STEPS + 1))
    got = app._gfs_wind_kept("51201", late.strftime("%Y%m%d"), late.strftime("%H"))
    assert got == {} and not app._rrfs_degraded_build()
    edge = T0 + timedelta(hours=6 * app._WIND_KEEPLAST_STEPS)
    assert len(app._gfs_wind_kept("51201", edge.strftime("%Y%m%d"), edge.strftime("%H"))) == 385


def test_a_station_in_no_run_gives_nothing(no_nomads):
    assert app._gfs_wind_kept("46026", DATE, "18") == {}
    assert app._gfs_winds_health()["keep_last"] == 0


def test_the_cover_uses_it(no_nomads, monkeypatch):
    monkeypatch.setattr(app, "get_latest_run", lambda: (DATE, "18"))
    got = app._gfs_wind_cover("51201", T0 + timedelta(hours=6))
    assert len(got) == 385 and {v[3] for v in got.values()} == {"GFS 12Z"}


def test_a_built_table_with_a_stand_in_is_kept_briefly(no_nomads, monkeypatch):
    def build():
        app._gfs_wind_kept("51201", DATE, "18")
        return ("Cycle", "Loc", None, [["d", "t"] + [None] * 18 + [5.0, 90, 3.0]], "UTC", None)
    key = ("TEST108", "", "GFS")
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.pop(key, None)
    app._forecast_singleflight(key, build)
    with app._CACHE_LOCK:
        entry = app._FORECAST_CACHE.pop(key)
    assert entry["ttl"] == app._WIND_NEG_TTL


def test_merge_keeps_the_stand_in_label():
    t = datetime(2026, 11, 22, 18)
    m = rrfs_wind.merge({t: (5.0, 90, None, "GFS 12Z"), t + timedelta(hours=1): (6.0, 100)},
                        {t: (7.0, 80, 9.0, "RRFS 15Z")})
    assert m == {t: (7.0, 80, 9.0, "RRFS 15Z"), t + timedelta(hours=1): (6.0, 100, None, None)}
    assert rrfs_wind.merge({t: (5.0, 90, None, "GFS 12Z")}, {}) == {t: (5.0, 90, None, "GFS 12Z")}


def _row(t, wind):
    return [t.strftime("%A, %B %d, %Y"), t.strftime("%I:%M %p").lstrip("0")] + [1.0, 10.0, 300] + [None] * 15 \
        + app._wind_row_cells(wind, t) + [2.0]


def test_the_window_names_the_stand_in_and_shows_no_gust_column(monkeypatch):
    monkeypatch.setenv("RRFS_WIND", "1")
    ts = [T0 + timedelta(hours=h) for h in range(6)]
    wind = {t: (5.0, 90, None, "GFS 06Z") for t in ts}
    rows = [_row(t, wind) for t in ts]
    assert all(r[20] == 5.0 and r[23] == "GFS 06Z" for r in rows)
    assert app._wind_source_line(rows) == "Wind: GFS 06Z"
    html = app.build_html_table("c", "l", None, rows, "UTC", "ft", compact=True)
    assert "Gust" not in html, "a stand-in label alone is not an RRFS table"
    mixed = {**wind, ts[0]: (7.0, 80, 9.0, "RRFS 15Z")}
    rows = [_row(t, mixed) for t in ts]
    assert app._wind_source_line(rows) == f"Wind: RRFS 15Z to {app._short_date(rows[0][0])} {rows[0][1]} · then GFS 06Z"
    assert "Gust" in app.build_html_table("c", "l", None, rows, "UTC", "ft", compact=True)


def test_the_bulletin_table_asks_the_kept_winds(monkeypatch):
    """The GFS bulletin table pins its run; with that run's winds missing it takes the stand-in (not get_station_wind)."""
    bull = open(os.path.join(HERE, "fixtures", "gfswave_51201_2026100112.bull"), encoding="utf-8").read()
    monkeypatch.setattr(app, "get_latest_run", lambda: ("20261001", "12"))
    monkeypatch.setattr(app, "_fetch_bulletin", lambda d, r, s: (bull, None))
    asked = []
    base = datetime(2026, 10, 1, 12)

    def kept(sid, d=None, r=None):
        asked.append((sid, d, r))
        return {base + timedelta(hours=h): (4.0, 45.0, None, "GFS 06Z") for h in range(385)}
    monkeypatch.setattr(app, "_gfs_wind_kept", kept)
    monkeypatch.setattr(app, "get_station_wind", lambda *a, **k: {})
    cycle, loc, run, rows, tz, err = app._parse_bull_uncached("51201", "UTC")
    assert err is None and asked == [("51201", "20261001", "12")]
    assert rows and all(r[20] == 4.0 for r in rows)


def test_the_spectral_table_asks_the_kept_winds():
    import inspect
    src = inspect.getsource(app._parse_gfs_spectral_uncached)
    assert "_gfs_wind_kept(station_id, run[:8], run[8:])" in src and "get_station_wind(" not in src
