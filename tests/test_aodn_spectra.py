"""AODN swell directions (2026-10-03, review P1-3): the THREDDS spectra carry Fourier moments a1, b1, a2, b2; the
directions FROM are NDBC's ALPHA1 = 270 - ARCTAN(b1, a1) and ALPHA2 = 270 - (0.5 * ARCTAN(b2, a2) + {0 or 180}).
The plain atan2 angle (used before) showed every AODN swell / wind-sea direction as 270 - the true one: Wilsons Prom
swell 23 deg NNE (from the land) where the buoy itself reported 248 deg WSW.

The real-data check uses tests/fixtures/aodn_spectra.json (tests/capture_aodn_spectra_fixture.py): one buoy's
spectra answers and its own reported peak direction at the same 25 times."""
import json
import math
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import buoy_sources as B  # noqa: E402
import capture_aodn_golden as GA  # noqa: E402

FIX = json.load(open(os.path.join(HERE, "fixtures", "aodn_spectra.json"), encoding="utf-8"))


def _diff(a, b):
    return abs((a - b + 180.0) % 360.0 - 180.0)


def _moments(from_deg, r=0.8):
    """a1, b1 of a wave train coming FROM from_deg under NDBC's convention (ALPHA1 = 270 - atan2(b1, a1))."""
    t = math.radians(270.0 - from_deg)
    return r * math.cos(t), r * math.sin(t)


@pytest.mark.parametrize("d", [0.0, 45.0, 90.0, 135.0, 180.0, 248.0, 270.0, 359.0])
def test_alpha1_returns_the_direction_the_moments_come_from(d):
    a1, b1 = _moments(d)
    assert _diff(B._ndbc_alpha1(a1, b1), d) < 1e-9
    assert 0.0 <= B._ndbc_alpha1(a1, b1) < 360.0


def test_alpha1_compass_points():
    """Waves from the north travel south: atan2 angle -90 (pointing -y), ALPHA1 0; from the east: atan2 180, ALPHA1 90."""
    assert _diff(B._ndbc_alpha1(0.0, -1.0), 0.0) < 1e-9
    assert _diff(B._ndbc_alpha1(-1.0, 0.0), 90.0) < 1e-9
    assert _diff(B._ndbc_alpha1(0.0, 1.0), 180.0) < 1e-9
    assert _diff(B._ndbc_alpha1(1.0, 0.0), 270.0) < 1e-9


@pytest.mark.parametrize("axis,alpha1", [(10.0, 5.0), (10.0, 185.0), (350.0, 2.0), (350.0, 175.0), (80.0, 300.0)])
def test_alpha2_takes_the_axis_end_nearest_alpha1(axis, alpha1):
    t = math.radians(2.0 * (270.0 - axis))                 # second moments of an axis through `axis`
    a2, b2 = 0.5 * math.cos(t), 0.5 * math.sin(t)
    got = B._ndbc_alpha2(a2, b2, alpha1)
    ends = (axis % 360.0, (axis + 180.0) % 360.0)
    assert min(_diff(got, e) for e in ends) < 1e-9          # on the axis
    assert _diff(got, alpha1) <= 90.0 + 1e-9                # the end nearer alpha1
    assert 0.0 <= got < 360.0


class Replay:
    """THREDDS answers from the fixture, keyed by the part after the monthly file's name (any month)."""
    def __init__(self):
        self.calls = []

    def get(self, url, **kw):
        self.calls.append(url)
        k = url.split("_monthly.nc", 1)[1] if "_monthly.nc" in url else None
        if k in FIX["bodies"]:
            return GA._Resp(FIX["bodies"][k])
        return GA._Resp("", 404)


def _spectrum():
    p = B.AODNProvider(http=Replay())
    spec = p.spectrum(FIX["site"])
    assert spec and len(spec["steps"]) == 25
    return p, spec


def test_real_spectra_peak_direction_equals_the_buoys_own():
    """At every one of the 25 real readings the peak band's direction = the buoy's own reported peak direction."""
    _, spec = _spectrum()
    checked = 0
    for st in spec["steps"]:
        own = FIX["peak_direction_from"].get(st["time_utc"][:16])
        if own is None:
            continue
        i = max(range(39), key=lambda j: st["energy"][j])
        assert _diff(st["alpha1"][i], own) < 0.5, (st["time_utc"], st["alpha1"][i], own)
        assert _diff(st["alpha2"][i], st["alpha1"][i]) <= 90.0
        checked += 1
    assert checked == 25


def test_components_route_gives_the_swell_from_the_buoys_side(monkeypatch):
    """The /components answer: the component holding the peak period comes from (about) the buoy's own direction."""
    import app as A
    p, spec = _spectrum()
    monkeypatch.setattr(A, "get_buoy_providers", lambda: [p])
    r = A.app.test_client().get("/api/buoys/aodn:%s/components" % FIX["site"])
    assert r.status_code == 200
    d = r.get_json()
    latest = spec["steps"][-1]
    own = FIX["peak_direction_from"][latest["time_utc"][:16]]
    peak_f = spec["freqs"][max(range(39), key=lambda j: latest["energy"][j])]
    comp = [c for c in d["components"] if c["frequency_min_hz"] - 1e-4 <= peak_f <= c["frequency_max_hz"] + 1e-4]
    assert len(comp) == 1
    assert _diff(comp[0]["direction_deg"], own) <= 30.0, (comp[0]["direction_deg"], own)
    i = max(range(39), key=lambda j: latest["energy"][j])
    assert _diff(d["spectrum"][i]["direction_deg"], own) <= 1.0
    assert d["summary"][0]["swell"] is None or d["summary"][0]["swell"]["dir_deg"] is not None
