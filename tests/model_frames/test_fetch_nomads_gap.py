"""The frames job borrows a few objects NOAA's S3 copy lacks from NOMADS (2026-11-17: S3 had no atmos f119 of the 12Z
GFS run, NOMADS had it; the overlay stayed on 06Z until 18Z was complete). Only past the grace period, only a few gaps,
only when NOMADS has every one; the grib and its .idx of a borrowed step both come from NOMADS."""
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import fetch as F  # noqa: E402

RUN = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)
GAP = "gfs.20261117/12/atmos/gfs.t12z.pgrb2.0p25.f119"


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(F, "BORROWED", {})


def _bucket(monkeypatch, gaps_by_run):
    """S3 holds every needed key of every run but the listed gaps."""
    def missing(run_dt):
        return [k for k in F.needed_keys(run_dt) if k in gaps_by_run.get(run_dt, ())]
    monkeypatch.setattr(F, "missing_objects", missing)
    monkeypatch.setattr(F, "run_is_complete", lambda run_dt: not missing(run_dt))


def test_a_hole_past_the_grace_period_is_read_from_nomads(monkeypatch):
    later = RUN + timedelta(hours=6)
    _bucket(monkeypatch, {later: F.needed_keys(later), RUN: {GAP, GAP + ".idx"}})   # 18Z still arriving
    now = RUN + timedelta(hours=F.NOMADS_GRACE_H, minutes=30)
    assert F.latest_complete_run(now, has=lambda k: True) == RUN
    assert F.BORROWED == {"2026111712": {GAP, GAP + ".idx"}}
    assert F.atmos_url(RUN, 119) == f"{F.NOMADS}/{GAP}"
    assert F.atmos_url(RUN, 118).startswith(F.S3 + "/") and F.wave_url(RUN, 119).startswith(F.S3 + "/")
    assert F.atmos_url(RUN + timedelta(hours=6), 119).startswith(F.S3 + "/")       # another run: S3


def test_only_the_grib_missing_still_moves_its_idx(monkeypatch):
    later = RUN + timedelta(hours=6)
    _bucket(monkeypatch, {later: F.needed_keys(later), RUN: {GAP}})
    assert F.latest_complete_run(RUN + timedelta(hours=7), lookback_cycles=2, has=lambda k: True) == RUN
    assert F.atmos_url(RUN, 119) == f"{F.NOMADS}/{GAP}"                            # fetch_records adds .idx to it


def test_within_the_grace_period_a_gap_is_lag(monkeypatch):
    _bucket(monkeypatch, {RUN: {GAP}, RUN - timedelta(hours=6): set()})
    now = RUN + timedelta(hours=F.NOMADS_GRACE_H) - timedelta(minutes=1)
    assert F.latest_complete_run(now, has=lambda k: True) == RUN - timedelta(hours=6)
    assert F.BORROWED == {}


def test_too_many_gaps_or_nomads_without_it(monkeypatch):
    many = {f"gfs.20261117/12/atmos/gfs.t12z.pgrb2.0p25.f{s:03d}" for s in (100, 101, 102, 103, 104)}
    later = RUN + timedelta(hours=6)
    _bucket(monkeypatch, {later: F.needed_keys(later), RUN: many, RUN - timedelta(hours=6): set()})
    now = RUN + timedelta(hours=8)
    assert F.latest_complete_run(now, has=lambda k: True) == RUN - timedelta(hours=6)
    _bucket(monkeypatch, {later: F.needed_keys(later), RUN: {GAP}, RUN - timedelta(hours=6): set()})
    asked = []
    assert F.latest_complete_run(now, has=lambda k: asked.append(k) or False) == RUN - timedelta(hours=6)
    assert asked == [GAP] and F.BORROWED == {}


def test_nomads_has_reads_a_head_and_never_raises():
    calls = []

    def ok(url, method="GET", tries=4, **k):
        calls.append((url, method))
        return 200, b""

    def gone(url, method="GET", tries=4, **k):
        raise F.NotReady("404")

    assert F.nomads_has(GAP, request=ok) and calls == [(f"{F.NOMADS}/{GAP}", "HEAD")]
    assert F.nomads_has(GAP, request=gone) is False


def test_requests_carry_a_user_agent(monkeypatch):
    seen = {}

    class Resp:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"x"

    def opener(req, timeout=None):
        seen.update(req.headers)
        return Resp()

    monkeypatch.setattr(F.urllib.request, "urlopen", opener)
    F._request("https://example/x", rng="bytes=0-1")
    assert seen.get("User-agent", "").startswith("allshoresurf.com") and seen.get("Range") == "bytes=0-1"
