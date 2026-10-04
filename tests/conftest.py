"""Test-wide setup for the live-buoy background service (plan section 36).

* LIVE_BACKGROUND=0 before `app` is imported: no scheduler thread starts in the test process.
* The provider refresh runner is INLINE for every test (a refresh scheduled by
  list_stations_versioned runs in the calling thread, at once), so the fetch schedule of the
  live-stations golden replay (tests/fixtures/live_stations_golden.json, captured on the
  inline-refresh implementation) is unchanged. Tests that want threads install their own runner.
"""
import os
import sys

import pytest

os.environ.setdefault("LIVE_BACKGROUND", "0")

HERE = os.path.dirname(os.path.abspath(__file__))
if os.path.dirname(HERE) not in sys.path:
    sys.path.insert(0, os.path.dirname(HERE))

import buoy_sources as _B  # noqa: E402


@pytest.fixture(autouse=True)
def _inline_refresh_runner():
    prev = _B.set_refresh_runner(_B.InlineRunner())
    yield
    _B.set_refresh_runner(prev)
