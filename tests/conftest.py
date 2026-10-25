"""Test-wide setup for the live-buoy background service (plan section 36).

* LIVE_BACKGROUND=0 before `app` is imported: no scheduler thread starts in the test process.
* WIND_STATIONS=0: the wind feeds (plan section 39) are off; their tests install fake providers and switch
  app.WIND_ENABLED on themselves, so no test ever reaches NOAA or aviationweather.gov.
* NOAA's GFS bulletin archive (plan section 53) is never reached: app._BULL_HTTP.get refuses (the archive read then
  fails as a transport error and the bulletin comes from the test's own fake NOMADS, i.e. app.HTTP.get, as before).
  The bulletin-source tests install their own fake archive on top.
* The provider refresh runners (the live buoys' and the wind feeds') are INLINE for every test (a refresh scheduled by
  list_stations_versioned runs in the calling thread, at once), so the fetch schedule of the
  live-stations golden replay (tests/fixtures/live_stations_golden.json, captured on the
  inline-refresh implementation) is unchanged. Tests that want threads install their own runner.
"""
import os
import sys

import pytest

os.environ.setdefault("LIVE_BACKGROUND", "0")
os.environ.setdefault("WIND_STATIONS", "0")      # the wind feeds (section 39) stay off unless a test switches them on

HERE = os.path.dirname(os.path.abspath(__file__))
if os.path.dirname(HERE) not in sys.path:
    sys.path.insert(0, os.path.dirname(HERE))

try:
    import buoy_sources as _B  # noqa: E402
    import wind_sources as _W  # noqa: E402
except ImportError:  # a CI job that installs only what its own tests need (coast-build: numpy + pytest)
    _B = _W = None


@pytest.fixture(autouse=True)
def _inline_refresh_runner():
    if _B is None:
        yield
        return
    prev = _B.set_refresh_runner(_B.InlineRunner())
    prev_w = _W.set_wind_runner(_B.InlineRunner())        # the wind feeds' own runner (G28 A-5), inline as well
    yield
    _B.set_refresh_runner(prev)
    _W.set_wind_runner(prev_w)


@pytest.fixture(autouse=True)
def _no_bulletin_archive(monkeypatch):
    app = sys.modules.get("app")
    if app is not None and hasattr(app, "_BULL_HTTP"):
        def refuse(url, **k):
            raise ConnectionError("tests do not reach NOAA's bulletin archive")
        monkeypatch.setattr(app._BULL_HTTP, "get", refuse)
    yield
