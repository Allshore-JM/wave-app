"""Tide stations (plan section 38): NOAA answers, the curves, the cache and the routes.

Fixtures: tests/fixtures/tides (captured by tests/capture_tides_fixtures.py; the curve starts 2026-10-07 00:00 UTC)."""
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

import tide_sources as T

HERE = os.path.dirname(os.path.abspath(__file__))
FIX = os.path.join(HERE, "fixtures", "tides")
ROOT = os.path.dirname(HERE)
BEGIN = 1791331200                       # 2026-10-07 00:00 UTC: the window's start
START = BEGIN - T.LEAD_S                 # the curve's
LEAD = T.LEAD_S // T.STEP_S              # grid points before the window
NOW = BEGIN + 86400 + 6 * 3600           # 2026-10-08 06:00 UTC: begin_of(NOW) == BEGIN


def fx(name):
    with open(os.path.join(FIX, name + ".json"), "rb") as f:
        return json.loads(f.read())


def hilo(name):
    return T.parse_hilo(fx(name + "_hilo"))


def grid(name):
    return T.on_grid(T.parse_predictions(fx(name + "_30")), START)


# ------------------------------------------------------------------------------------------ parsers

def test_parsers_read_noaa_answers():
    pts = T.parse_predictions(fx("honolulu_30"))
    assert len(pts) == T.POINTS and pts[0][0] == START and pts[-1][0] == START + T.CURVE_HOURS * 3600
    assert all(b[0] - a[0] == T.STEP_S for a, b in zip(pts, pts[1:]))
    ex = hilo("honolulu")
    assert START - T.LEAD_S <= ex[0][0] < START and {k for *_, k in ex} == {"H", "L"}
    assert all(a[2] != b[2] for a, b in zip(ex, ex[1:]))                     # NOAA alternates
    obs = T.parse_water_level(fx("water_level"))
    assert len(obs) > 400 and all(isinstance(v, float) for _, v in obs)
    assert T.noaa_error(fx("error")) and "Predictions" in T.noaa_error(fx("error"))
    assert T.noaa_error(fx("honolulu_30")) is None


def test_parsers_skip_bad_rows_and_name_kinds_by_their_last_letter():
    doc = {"predictions": [{"t": "2026-10-07 01:00", "v": "1.0", "type": "HH"},
                           {"t": "2026-10-07 07:00", "v": "0.1", "type": "LL"},
                           {"t": "2026-10-07 13:00", "v": "0.9", "type": "LH"},
                           {"t": "2026-10-07 19:00", "v": "0.2", "type": "HL"},
                           {"t": "bad", "v": "1"}, {"t": "2026-10-07 20:00", "v": "x", "type": "H"},
                           {"t": "2026-10-07 21:00", "v": "1", "type": "?"}]}
    assert [k for *_, k in T.parse_hilo(doc)] == ["H", "L", "H", "L"]
    wl = {"data": [{"t": "2026-10-07 00:00", "v": ""}, {"t": "2026-10-07 00:06", "v": "0.5"}, "junk"]}
    assert T.parse_water_level(wl) == [(BEGIN + 360, 0.5)]
    for bad in ({}, {"predictions": "x"}, [], None):
        with pytest.raises(T.TideError):
            T.parse_predictions(bad)
    assert T.noaa_error({"error": "plain"}) == "plain"


def test_url_names_the_application_and_asks_in_metres_gmt_mllw():
    u = T.url("1612340", product="predictions", interval="hilo", begin_date=T._stamp(BEGIN), range=24)
    for part in ("station=1612340", "datum=MLLW", "units=metric", "time_zone=gmt", "application=allshoresurf.com",
                 "begin_date=20261007+00%3A00"):
        assert part in u


def test_ids_are_validated():
    for ok in ("1612340", "TEC1603", "TPT2885"):
        assert T.valid_id(ok)
    for bad in ("", "12", "1612340/../x", "16 12340", "a" * 11, None, 1612340):
        assert not T.valid_id(bad)


# ------------------------------------------------------------------------------------------ the curve

def test_on_grid_places_values_and_ignores_off_grid_times():
    g = T.on_grid([(BEGIN, 1.0), (BEGIN + 900, 9.0), (BEGIN + 3600, 2.0), (BEGIN - 1800, 7.0)], BEGIN, n=4)
    assert g == [1.0, None, 2.0, None]


def test_cosine_is_exact_at_extremes_monotone_between_and_refuses_gaps():
    ex = [(0, 0.0, "L"), (21600, 2.0, "H"), (43200, 0.5, "L")]
    assert T.cosine_at(ex, 0) == 0.0 and T.cosine_at(ex, 21600) == 2.0 and T.cosine_at(ex, 43200) == 0.5
    assert T.cosine_at(ex, 10800) == pytest.approx(1.0)                     # the midpoint is the mean
    rise = [T.cosine_at(ex, t) for t in range(0, 21601, 600)]
    fall = [T.cosine_at(ex, t) for t in range(21600, 43201, 600)]
    assert rise == sorted(rise) and fall == sorted(fall, reverse=True)
    assert T.cosine_at(ex, -1) is None and T.cosine_at(ex, 43201) is None
    assert T.cosine_at([(0, 0.0, "L"), (21600, 1.0, "L")], 100) is None     # same kind: an extreme is missing
    far = T.MAX_GAP_S + 60
    assert T.cosine_at([(0, 0.0, "L"), (far, 1.0, "H")], 100) is None
    assert T.cosine_at([(0, 0.0, "L"), (T.MAX_GAP_S, 1.0, "H")], 100) is not None


def test_cosine_through_honolulus_extremes_follows_noaas_curve():
    g, ex = grid("honolulu"), hilo("honolulu")
    c = T.cosine_grid(ex, START)
    err = [abs(a - b) for a, b in zip(c, g) if a is not None and b is not None]
    assert len(err) > 800 and max(err) < 0.05


def test_the_reference_method_beats_a_cosine_where_the_truth_is_known():
    """San Diego's extremes with La Jolla's curve (two harmonic stations ~15 km apart, partners within an hour) against
    San Diego's own NOAA curve: the subordinate method's worst error is a few cm; a cosine misses by ~30 cm."""
    truth, ex = grid("sandiego"), hilo("sandiego")
    ref_g, ref_ex = grid("lajolla"), hilo("lajolla")
    shaped, used = T.reference_grid(ex, ref_ex, ref_g, START, (0, 0), START)
    cos = T.cosine_grid(ex, START)
    e_ref = [abs(a - b) for a, b in zip(shaped, truth) if a is not None and b is not None]
    e_cos = [abs(a - b) for a, b in zip(cos, truth) if a is not None and b is not None]
    assert used > 800 and len(e_ref) > 800
    assert max(e_ref) < 0.05 and max(e_cos) > 0.2


def test_a_subordinate_curve_passes_through_its_own_extremes_and_uses_its_reference():
    ex, ref_ex, ref_g = hilo("waimea"), hilo("nawiliwili"), grid("nawiliwili")
    partner = T.pair_reference(ex, ref_ex, (7, 18))
    assert sum(p is not None for p in partner) >= len(ex) - 1               # NOAA: reference extreme + the offset
    hits = shaped = 0
    for i, (t, h, _) in enumerate(ex):
        if BEGIN <= t <= BEGIN + T.SPAN_S and 0 < i < len(ex) - 1:
            v, from_ref = T.reference_at(ex, ref_ex, partner, ref_g, START, t)
            assert v == pytest.approx(h, abs=1e-9)                               # exact at every extreme, either way
            hits += 1
            shaped += from_ref
            if not from_ref:                    # only where NOAA's list for this station skips a pair its reference has
                assert partner[i] is None or partner[i + 1] is None or partner[i + 1] != partner[i] + 1
    assert hits > 110 and shaped >= hits - 2
    g, used = T.reference_grid(ex, ref_ex, ref_g, START, (7, 18), START)
    assert all(v is not None for v in g)
    inside = [T.reference_at(ex, ref_ex, partner, ref_g, START, BEGIN + i * T.STEP_S)[1]
              for i in range(T.SPAN_DAYS * 48 + 1)]
    assert sum(inside) >= len(inside) - 48                                    # shaped but for a skipped pair's stretch


def test_pairing_needs_an_offset_and_a_partner_within_the_tolerance():
    sub = [(10000, 1.0, "H"), (30000, 0.0, "L")]
    ref = [(10000 - 600, 1.2, "H"), (30000 - 1200, 0.1, "L")]
    assert T.pair_reference(sub, ref, (10, 20)) == [0, 1]
    assert T.pair_reference(sub, ref, (None, 20)) == [None, 1]
    assert T.pair_reference(sub, ref, (10 + 61, 20)) == [None, 1]           # 71 min off: no partner
    late = [(10000 + 3000, 1.2, "H"), (30000 + 3000, 0.1, "L")]             # the reference 50 min AFTER (offset -50)
    assert T.pair_reference(sub, late, (-50, -50)) == [0, 1]
    assert T.pair_reference(sub, late, (50, 50)) == [None, None]           # the offset is subtracted, not added
    assert T.pair_reference(sub, [(0, 0.0, "L")], (0, 0)) == [None, None]
    # non-consecutive partners: that stretch is a cosine
    ref2 = [(9400, 1.2, "H"), (15000, 0.5, "L"), (20000, 0.9, "H"), (28800, 0.1, "L")]
    p = T.pair_reference(sub, ref2, (10, 20))
    assert p == [0, 3]
    ramp = [i * 0.01 for i in range(50)]                                    # a reference curve that would be usable
    v, from_ref = T.reference_at(sub, ref2, p, ramp, 0, 20000)
    assert not from_ref and v == pytest.approx(T.cosine_at(sub, 20000))
    v, from_ref = T.reference_at(sub, ref2, [0, 1], ramp, 0, 20000)        # consecutive partners: shaped
    assert from_ref


def test_night_bands_follow_first_and_last_light():
    import sky
    if not sky.AVAILABLE:
        pytest.skip("PyEphem not installed")
    bands = T.night_bands(21.3, -157.86, BEGIN, BEGIN + T.SPAN_S)
    assert 31 <= len(bands) <= 33
    assert all(BEGIN <= a < b <= BEGIN + T.SPAN_S for a, b in bands)
    assert all(b[0] > a[1] for a, b in zip(bands, bands[1:]))
    inner = [b - a for a, b in bands[1:-1]]
    assert all(10.5 * 3600 < d < 12.5 * 3600 for d in inner)                # Honolulu in October
    ev = T.sky_events(21.3, -157.86, BEGIN, BEGIN + T.SPAN_S)
    dusk = {t for t, k in ev if k == "dusk"}
    dawn = {t for t, k in ev if k == "dawn"}
    assert all(a in dusk for a, _ in bands[1:]) and all(b in dawn for _, b in bands[:-1])   # last light to first light
    hst = timezone(timedelta(hours=-10))
    first_light = datetime.fromtimestamp(bands[1][1], tz=hst)
    assert first_light.hour == 6                                             # ~6:0x AM HST
    dec = int(datetime(2026, 12, 10, tzinfo=timezone.utc).timestamp())
    assert T.night_bands(82.0, -60.0, dec, dec + 5 * 86400) == [[dec, dec + 5 * 86400]]   # polar night


def test_sky_events_are_searched_in_chunks_without_gaps_or_repeats():
    import sky
    if not sky.AVAILABLE:
        pytest.skip("PyEphem not installed")
    a, z = START, START + T.CURVE_HOURS * 3600
    evs = T.sky_events(21.3, -157.86, a, z)
    assert evs == sorted(evs) and len(evs) == len(set(evs))
    rises = [t for t, k in evs if k == "sunrise"]
    assert len(rises) in (32, 33)                                             # one a day, none lost at a chunk edge
    assert all(20 * 3600 < b - a < 28 * 3600 for a, b in zip(rises, rises[1:]))
    for edge in range(a + T.SKY_CHUNK_S, z, T.SKY_CHUNK_S):                    # the same events as one search there
        ref = sky.events(21.3, -157.86, datetime.fromtimestamp(edge - 86400, tz=timezone.utc),
                         datetime.fromtimestamp(edge + 86400, tz=timezone.utc))
        got = [(t, k) for t, k in evs if edge - 86400 <= t < edge + 86400]
        want = [(int(u.timestamp()), k) for u, k in ref]
        assert [k for _, k in got] == [k for _, k in want]                    # the same events ...
        assert all(abs(g[0] - x[0]) <= 2 for g, x in zip(got, want))          # ... to ephem's second
    assert T.sun_moon([(1, "dawn"), (2, "sunrise"), (3, "moonset"), (4, "dusk")]) == [[2, "sunrise"], [3, "moonset"]]
    assert T.sun_moon(None) is None
    moon = T.moon_samples(21.3, a, a + 2 * 86400)
    assert [m[0] - a for m in moon] == [i * 6 * 3600 for i in range(9)]
    assert all(0 <= m[1] < 1 and 0 <= m[2] <= 100 and isinstance(m[3], str) for m in moon)
    assert moon[1][1] > moon[0][1] or moon[1][1] < 0.05                      # the phase grows (or a new moon passed)


def test_the_window_covers_today_from_local_midnight_plus_30_days_in_every_zone():
    for hour in range(0, 24, 3):
        now = BEGIN + 86400 + hour * 3600
        b = T.begin_of(now)
        for off_h in range(-12, 15):
            local = datetime.fromtimestamp(now, tz=timezone(timedelta(hours=off_h)))
            midnight = int(local.replace(hour=0, minute=0, second=0).timestamp())
            assert b <= midnight and midnight + 30 * 86400 <= b + T.SPAN_S, (hour, off_h)


# ------------------------------------------------------------------------------------------ the service

STATIONS = {
    "1612340": {"id": "1612340", "name": "Honolulu", "lat": 21.3033, "lon": -157.8645, "type": "R", "ref": None,
                "tz": "Pacific/Honolulu", "obs": True, "oh": None, "ol": None},
    "1611400": {"id": "1611400", "name": "Nawiliwili", "lat": 21.9544, "lon": -159.3561, "type": "R", "ref": None,
                "tz": "Pacific/Honolulu", "obs": True, "oh": None, "ol": None},
    "1611401": {"id": "1611401", "name": "Waimea Bay", "lat": 21.95, "lon": -159.67, "type": "S", "ref": "1611400",
                "tz": "Pacific/Honolulu", "obs": False, "oh": 7, "ol": 18},
}


class FakeNoaa:
    """Answers the URLs tide_sources builds from the fixtures; counts and can fail or hold requests."""

    def __init__(self):
        self.calls = []
        self.fail = False
        self.hold = None

    def __call__(self, u, max_bytes):
        self.calls.append(u)
        if self.hold is not None:
            self.hold.wait(5)
        if self.fail:
            raise IOError("down")
        name = {"1612340": "honolulu", "1611400": "nawiliwili", "1611401": "waimea"}[u.split("station=")[1].split("&")[0]]
        if "product=water_level" in u:
            return json.dumps(fx("water_level")).encode()
        if "interval=hilo" in u:
            return json.dumps(fx(name + "_hilo")).encode()
        if name == "waimea":
            return json.dumps(fx("error")).encode()
        return json.dumps(fx(name + "_30")).encode()


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def service(**kw):
    noaa, clock = FakeNoaa(), Clock(NOW)
    return T.TideService(dict(STATIONS), noaa, now=clock, **kw), noaa, clock


def test_a_harmonic_station_answers_noaas_curve_and_extremes_once_a_day():
    svc, noaa, clock = service()
    status, p = svc.forecast("1612340")
    assert status == "ok" and p["method"] == "harmonic" and p["begin"] == START and p["step"] == 1800
    assert p["window"] == [BEGIN, BEGIN + T.SPAN_S]
    assert len(p["v"]) == T.POINTS and p["v"] == grid("honolulu")
    assert p["hilo"][0] == [hilo("honolulu")[0][0], hilo("honolulu")[0][1], hilo("honolulu")[0][2]]
    assert p["datum"] == "MLLW" and p["units"] == "m" and p["tz"] == "Pacific/Honolulu" and p["obs"] is True
    import sky
    assert (p["events"] is not None) == sky.AVAILABLE and (p["moon"] is not None) == sky.AVAILABLE
    assert p["night"] is None or (len(p["night"]) >= 31 and p["night"][0][0] == p["begin"])   # 02:00 HST: night from the curve's start
    if p["events"] is not None:
        kinds = {k for _, k in p["events"]}
        assert kinds == {"sunrise", "sunset", "moonrise", "moonset"}
        assert all(p["begin"] <= t < p["begin"] + T.CURVE_HOURS * 3600 for t, _ in p["events"])
        assert len(p["moon"]) == T.CURVE_HOURS // 6 + 1 and all(len(m) == 4 for m in p["moon"])
    assert len(noaa.calls) == 2
    assert svc.forecast("1612340") == (status, p) and len(noaa.calls) == 2   # cached
    clock.t += 17 * 3600                                                      # still the same UTC day
    svc.forecast("1612340")
    assert len(noaa.calls) == 2
    clock.t += 2 * 3600                                                       # the next UTC day: a new window
    assert svc._get(("p", "1612340", BEGIN)) is None                          # yesterday's window has expired
    status, p2 = svc.forecast("1612340")
    assert status == "ok" and p2["begin"] == START + 86400 and len(noaa.calls) == 4


def test_a_subordinate_station_is_shaped_on_its_references_cached_curve():
    svc, noaa, _ = service()
    status, p = svc.forecast("1611401")
    assert status == "ok" and p["method"] == "reference" and p["ref"] == "1611400" and p["type"] == "S"
    assert all(v is not None for v in p["v"])
    assert len(noaa.calls) == 3                                               # its extremes + the reference's two
    status, r = svc.forecast("1611400")                                       # the reference is cached now
    assert status == "ok" and r["method"] == "harmonic" and len(noaa.calls) == 3


def test_a_subordinate_without_a_usable_reference_falls_back_to_a_cosine():
    stations = dict(STATIONS)
    stations["1611401"] = dict(STATIONS["1611401"], ref="9999999")
    svc = T.TideService(stations, FakeNoaa(), now=Clock(NOW))
    status, p = svc.forecast("1611401")
    assert status == "ok" and p["method"] == "cosine" and p["ref"] is None
    assert p["v"] == T.cosine_grid(hilo("waimea"), START)


def test_a_reference_must_be_a_harmonic_station_with_noaas_curve():
    stations = dict(STATIONS)
    stations["1611402"] = dict(STATIONS["1611401"], id="1611402", ref="1611401")      # its reference is subordinate
    noaa = FakeNoaa()
    svc = T.TideService(stations, lambda u, m: noaa(u.replace("station=1611402", "station=1611401"), m), now=Clock(NOW))
    status, p = svc.forecast("1611402")
    assert status == "ok" and p["method"] == "cosine" and len(noaa.calls) == 1        # the S reference is never asked

    class NoCurve(FakeNoaa):                                                  # Nawiliwili answers no 30-minute curve
        def __call__(self, u, m):
            if "station=1611400" in u and "interval=30" in u:
                self.calls.append(u)
                return json.dumps(fx("error")).encode()
            return super().__call__(u, m)
    svc = T.TideService(dict(STATIONS), NoCurve(), now=Clock(NOW))
    assert svc.forecast("1611400")[1]["method"] == "cosine"
    status, p = svc.forecast("1611401")
    assert status == "ok" and p["method"] == "cosine" and p["ref"] is None   # a cosine reference is not used


def test_noaa_errors_are_final_and_failures_pass():
    svc, noaa, clock = service()
    noaa.fail = True
    status, p = svc.forecast("1612340")
    assert status == "error" and p["error"] == T.UNAVAILABLE
    n = len(noaa.calls)
    assert svc.forecast("1612340")[0] == "error" and len(noaa.calls) == n     # remembered for a minute
    noaa.fail = False
    clock.t += 61
    assert svc.forecast("1612340")[0] == "ok"
    stations = dict(STATIONS)
    stations["1611401"] = dict(STATIONS["1611401"])
    svc2 = T.TideService(stations, lambda u, m: json.dumps(fx("error")).encode(), now=Clock(NOW))
    status, p = svc2.forecast("1611401")
    assert status == "final" and p["final"] is True and p["error"] == T.NO_PREDICTIONS
    status, p = svc2.forecast("1612340")
    assert status == "final" and p["final"] is True


def test_unknown_and_malformed_ids_never_reach_noaa():
    svc, noaa, _ = service()
    for sid in ("9999999", "../etc", "", "1612340x"):
        assert svc.forecast(sid)[0] == "final" and svc.observed(sid)[0] == "final"
    assert noaa.calls == []


def test_one_build_per_station_at_a_time_and_busy_instead_of_a_long_wait():
    svc, noaa, _ = service(wait_s=0.2)
    noaa.hold = threading.Event()
    out = {}
    t = threading.Thread(target=lambda: out.setdefault("a", svc.forecast("1612340")))
    t.start()
    time.sleep(0.05)
    t0 = time.monotonic()
    assert svc.forecast("1612340")[0] == "busy"                              # the same station, being built
    assert time.monotonic() - t0 < 1.0
    noaa.hold.set()
    t.join(5)
    assert out["a"][0] == "ok"
    assert sum("1612340" in u for u in noaa.calls) == 2                      # built once


def test_build_slots_are_bounded():
    svc, noaa, _ = service(builds=1, wait_s=0.2)
    noaa.hold = threading.Event()
    t = threading.Thread(target=lambda: svc.forecast("1612340"))
    t.start()
    time.sleep(0.05)
    assert svc.forecast("1611400")[0] == "busy"                              # another station: no free slot
    noaa.hold.set()
    t.join(5)
    assert svc.forecast("1611400")[0] == "ok"


def test_the_cache_is_bounded():
    svc, noaa, _ = service(max_entries=2)
    svc.forecast("1612340")
    svc.forecast("1611400")
    svc.forecast("1611401")                                                    # touches 1611400 (its reference)
    assert len(svc._cache) == 2
    n = len(noaa.calls)
    svc.forecast("1612340")                                                    # evicted: asked again
    assert len(noaa.calls) > n


def test_observed_water_level_only_where_a_gauge_reports():
    svc, noaa, clock = service()
    status, p = svc.observed("1611401")
    assert status == "final" and p["t"] == [] and noaa.calls == []
    status, p = svc.observed("1612340")
    assert status == "ok" and len(p["t"]) == len(p["v"]) > 400 and p["datum"] == "MLLW"
    assert "product=water_level" in noaa.calls[0] and "range=48" in noaa.calls[0]
    svc.observed("1612340")
    assert len(noaa.calls) == 1
    clock.t += 901
    svc.observed("1612340")
    assert len(noaa.calls) == 2



# ------------------------------------------------------------------------------------------ G27 (plan section 38)

def _answering(msg_for):
    """A FakeNoaa whose answer for some URLs is NOAA's error body with the given message."""
    class N(FakeNoaa):
        def __call__(self, u, m):
            msg = msg_for(u)
            if msg is not None:
                self.calls.append(u)
                return json.dumps({"error": {"message": msg}}).encode()
            return super().__call__(u, m)
    return N()


def test_only_noaas_no_predictions_answer_is_final():
    """G27 A-F3: a throttle or any other NOAA message passes (a minute, then asked again); "no predictions" is final
    and kept (not asked again a minute later)."""
    assert T.final_error("No Predictions data was found. Please make sure the Datum input is valid.")
    assert T.final_error("Great Lakes stations don't have Predictions data.")
    assert not T.final_error("Request limit exceeded. Please try again later.") and not T.final_error("")
    throttle = {"on": True}
    noaa = _answering(lambda u: "Request limit exceeded. Please try again later." if throttle["on"] else None)
    clock = Clock(NOW)
    svc = T.TideService(dict(STATIONS), noaa, now=clock)
    status, p = svc.forecast("1612340")
    assert status == "error" and p["error"] == T.UNAVAILABLE
    throttle["on"] = False
    clock.t += 61
    assert svc.forecast("1612340")[0] == "ok"
    noaa = _answering(lambda u: "No Predictions data was found." if "station=1611400" in u else None)
    clock = Clock(NOW)
    svc = T.TideService(dict(STATIONS), noaa, now=clock)
    assert svc.forecast("1611400")[0] == "final"
    n = len(noaa.calls)
    clock.t += 3600                                                            # still kept an hour later (P16)
    assert svc.forecast("1611400")[0] == "final" and len(noaa.calls) == n


def test_a_passing_error_on_one_of_two_requests_is_an_error_not_a_degraded_day():
    """G27 A-F3: a throttle on the extremes alone (or the curve alone) is not a curve without dots for a day."""
    for which in ("interval=hilo", "interval=30"):
        noaa = _answering(lambda u, w=which: "Request limit exceeded." if w in u else None)
        svc = T.TideService(dict(STATIONS), noaa, now=Clock(NOW))
        assert svc.forecast("1612340")[0] == "error", which


def test_a_busy_reference_answers_busy_never_a_cosine_for_the_day():
    """G27 A-F2: while another request builds the reference, its subordinate is "busy" (not cached), then shaped."""
    svc, noaa, _ = service(wait_s=0.2)
    e = svc._key_lock(("p", "1611400", BEGIN))
    e[0].acquire()                                                             # someone is building Nawiliwili
    status, _ = svc.forecast("1611401")
    assert status == "busy" and svc._get(("p", "1611401", BEGIN)) is None     # not remembered
    e[0].release()
    svc._drop_key(("p", "1611400", BEGIN), e)
    status, p = svc.forecast("1611401")
    assert status == "ok" and p["method"] == "reference" and p["ref"] == "1611400" and p["ref_name"] == "Nawiliwili"
    assert svc._inflight == {}                                                 # every lock entry dropped (A-F12)


def test_a_failed_reference_is_an_error_for_a_minute_then_the_subordinate_is_shaped():
    """G27 A-F2: a reference that cannot be fetched now does not leave its subordinate on a cosine until midnight."""
    class RefDown(FakeNoaa):
        down = True

        def __call__(self, u, m):
            if self.down and "station=1611400" in u:
                self.calls.append(u)
                raise IOError("timed out")
            return super().__call__(u, m)
    noaa, clock = RefDown(), Clock(NOW)
    svc = T.TideService(dict(STATIONS), noaa, now=clock)
    assert svc.forecast("1611401")[0] == "error"
    noaa.down = False
    clock.t += 61
    status, p = svc.forecast("1611401")
    assert status == "ok" and p["method"] == "reference"


def test_the_method_and_reference_say_cosine_when_nothing_could_be_shaped():
    """G27 P28/P36: a harmonic reference in hand but no extreme paired (no offsets): the curve is a cosine, named so."""
    stations = dict(STATIONS)
    stations["1611401"] = dict(STATIONS["1611401"], oh=None, ol=None)
    svc = T.TideService(stations, FakeNoaa(), now=Clock(NOW))
    status, p = svc.forecast("1611401")
    assert status == "ok" and p["method"] == "cosine" and p["ref"] is None and p["ref_name"] is None
    assert p["v"] == T.cosine_grid(hilo("waimea"), START)


def test_a_request_waiting_on_a_build_gets_the_build_not_busy():
    """G27 P41: a second request for a station being built waits (up to wait_s) and receives the same answer."""
    svc, noaa, _ = service(wait_s=3.0)
    noaa.hold = threading.Event()
    out = {}
    t = threading.Thread(target=lambda: out.setdefault("a", svc.forecast("1612340")))
    t.start()
    time.sleep(0.05)
    threading.Timer(0.3, noaa.hold.set).start()
    b = svc.forecast("1612340")
    t.join(5)
    assert b[0] == "ok" and b == out["a"] and sum("1612340" in u for u in noaa.calls) == 2


def test_the_gap_guard_and_the_partner_kind():
    """G27 P07 / P39: a 21-hour stretch between extremes is a gap (19 h is drawn); a partner is of the same kind."""
    ex = [(0, 1.0, "H"), (19 * 3600, 0.0, "L"), (40 * 3600, 1.0, "H")]
    assert T.cosine_at(ex, 9 * 3600) is not None and T.cosine_at(ex, 30 * 3600) is None
    sub = [(10 * 3600, 1.0, "H")]
    assert T.pair_reference(sub, [(10 * 3600 - 600, 0.2, "L")], (10, 10)) == [None]      # an L 10 min away: no partner
    assert T.pair_reference(sub, [(10 * 3600 - 600, 0.9, "H")], (10, 10)) == [0]

def test_fork_resets_the_locks():
    svc, _, _ = service()
    svc._lock.acquire()
    T._after_fork()
    assert svc._lock.acquire(timeout=0.1)


# ------------------------------------------------------------------------------------------ the station list

def test_the_station_snapshot():
    stations, doc = T.load_stations(os.path.join(ROOT, "tide_stations.json"))
    assert doc["fields"] == ["id", "name", "lat", "lon", "type", "ref", "tz", "obs", "oh", "ol"]
    assert len(stations) > 3400
    kinds = [s["type"] for s in stations.values()]
    assert kinds.count("R") > 1200 and kinds.count("S") > 2200
    assert all(T.valid_id(i) for i in stations)
    assert all(-90 <= s["lat"] <= 90 and -180 <= s["lon"] <= 180 and s["tz"] for s in stations.values())
    subs = [s for s in stations.values() if s["type"] == "S"]
    assert all(s["ref"] in stations and s["ref"] != s["id"] for s in subs)       # no subordinate without a reference
    assert sum(1 for s in subs if s["oh"] is not None and s["ol"] is not None) >= len(subs) * 0.98
    assert stations["1612340"]["name"] == "Honolulu" and stations["1612340"]["obs"] is True
    w = stations["1611401"]
    assert (w["type"], w["ref"], w["oh"], w["ol"], w["tz"]) == ("S", "1611400", 7, 18, "Pacific/Honolulu")
    assert sum(1 for s in stations.values() if s["obs"]) > 200
    page = T.client_list(doc)
    assert page["fields"] == ["id", "name", "lat", "lon", "type", "tz", "obs"]
    assert len(page["stations"]) == len(stations) and all(len(r) == 7 for r in page["stations"])
    # G27 A-F4: NOAA's wrong positions corrected, the zone following; A-F6: abbreviations inside mixed names kept
    fixed = {"TPT2891": (-169.91667, "Pacific/Niue"), "TPT2893": (-173.98333, "Pacific/Tongatapu"),
             "TPT2897": (-174.79, "Pacific/Tongatapu"), "TWC0279": (-78.833, "America/Guayaquil")}
    for sid, (lon, tz) in fixed.items():
        assert (stations[sid]["lon"], stations[sid]["tz"]) == (lon, tz), sid
    assert stations["6835001"]["lat"] == -6.1
    names = {s["name"] for s in stations.values()}
    for good in ("Martha's Vineyard GPS Buoy", "Fort Eustis (MARAD)", "PGA Boulevard Bridge, ICWW", "Lake Worth ICW",
                 "CBBT, Chesapeake Channel", "Port O'Connor, Matagorda Bay", "Lime Tree Bay, St.Croix Island",
                 "Pago Pago Harbor, Tutuila Island", "New York (The Battery)", "Cut 1N Front Range, St Marys River Entr"):
        assert good in names, good


def test_the_station_tools_names_and_zone_check():
    """G27 A-F6 / A-F4: tools/tides/fetch_stations.py converts NOAA's capitalised names only, and stops the build when a
    derived zone disagrees with NOAA's timezonecorr by more than 3 h outside the known stale / date-line list."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("fetch_stations", os.path.join(ROOT, "tools", "tides", "fetch_stations.py"))
    FS = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(FS)
    cases = {"HONOLULU": "Honolulu", "MOKU O LOE": "Moku O Loe", "CHUUK, Moen Island": "Chuuk, Moen Island",
             "PAGO PAGO Harbor, Tutuila Island": "Pago Pago Harbor, Tutuila Island",
             "APIA (Observatory), Upolu Island": "Apia (Observatory), Upolu Island",
             "PGA Boulevard Bridge, ICWW": "PGA Boulevard Bridge, ICWW", "Little Creek, NAB": "Little Creek, NAB",
             "COX WC-53 Platform": "COX WC-53 Platform", "VACA KEY, USCG STATION, FLORIDA BAY": "Vaca Key, USCG Station, Florida Bay",
             "MARTHA'S VINEYARD": "Martha's Vineyard", "PORT O'CONNOR, MATAGORDA BAY": "Port O'Connor, Matagorda Bay",
             "ST. MARKS RIVER ENTRANCE": "St. Marks River Entrance", "  Two   spaces ": "Two spaces", "": ""}
    for raw, want in cases.items():
        assert FS.clean_name(raw) == want, raw
    rows = [["1612340", "Honolulu", 21.3, -157.86, "R", None, "Pacific/Honolulu", True, None, None],
            ["TPT2891", "Niue", -19.0, 169.9, "S", "X", "Pacific/Efate", False, 0, 0],
            ["1778000", "Apia", -13.8, -171.8, "R", None, "Pacific/Apia", False, None, None]]
    bad = FS.zone_mismatches(rows, {"1612340": "-10", "TPT2891": "-11", "1778000": "-11"})
    assert [b[0] for b in bad] == ["TPT2891"]                                  # Apia is a known date-line difference


# ------------------------------------------------------------------------------------------ the routes

@pytest.fixture
def client(monkeypatch):
    import app as A
    svc, noaa, clock = service()
    doc = {"fields": ["id", "name", "lat", "lon", "type", "ref", "tz", "obs", "oh", "ol"],
           "stations": [[s[k] for k in ("id", "name", "lat", "lon", "type", "ref", "tz", "obs", "oh", "ol")]
                        for s in STATIONS.values()], "captured": "x", "source": "y"}
    monkeypatch.setitem(A._TIDES, "svc", svc)
    monkeypatch.setitem(A._TIDES, "list", A._json_payload_and_etag(T.client_list(doc)))
    return A.app.test_client(), noaa, svc


def test_route_station_list_is_cached_for_hours_with_an_etag(client):
    c, noaa, _ = client
    r = c.get("/api/tides/stations")
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=21600"
    assert r.headers["CDN-Cache-Control"] == "max-age=21600" and r.headers["ETag"]
    body = r.get_json()
    assert body["fields"][0] == "id" and len(body["stations"]) == 3
    assert c.get("/api/tides/stations", headers={"If-None-Match": r.headers["ETag"]}).status_code == 304
    assert noaa.calls == []


def test_route_forecast_ok_final_busy_and_unknown(client, monkeypatch):
    c, noaa, svc = client
    r = c.get("/api/tides/1612340")
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=1800"
    assert r.headers["CDN-Cache-Control"] == "no-store"
    assert r.get_json()["method"] == "harmonic" and len(r.get_json()["v"]) == T.POINTS
    for bad in ("9999999", "x", "1612340x"):
        r = c.get("/api/tides/" + bad)
        assert r.status_code == 404 and r.headers["Cache-Control"] == "no-store"
    assert c.get("/api/tides/16123%2F40").status_code == 404                  # no route at all
    n = len(noaa.calls)
    assert c.get("/api/tides/9999999").status_code == 404 and len(noaa.calls) == n     # never asked upstream
    monkeypatch.setattr(svc, "forecast", lambda sid: ("busy", {"error": T.BUSY}))
    r = c.get("/api/tides/1612340")
    assert r.status_code == 503 and r.headers["Retry-After"] == "5" and r.headers["Cache-Control"] == "no-store"
    assert r.get_json()["retry"] is True
    monkeypatch.setattr(svc, "forecast", lambda sid: ("final", {"id": sid, "error": T.NO_PREDICTIONS}))
    r = c.get("/api/tides/1612340")
    assert r.status_code == 200 and r.get_json()["final"] is True and r.headers["Cache-Control"] == "public, max-age=3600"


def test_route_observed(client):
    c, noaa, _ = client
    r = c.get("/api/tides/1612340/observed")
    assert r.status_code == 200 and r.headers["Cache-Control"] == "public, max-age=300"
    assert len(r.get_json()["t"]) > 400
    r = c.get("/api/tides/1611401/observed")
    assert r.status_code == 200 and r.get_json()["final"] is True and r.get_json()["t"] == []


def test_routes_without_the_snapshot_say_so(monkeypatch):
    import app as A
    monkeypatch.setitem(A._TIDES, "svc", None)
    monkeypatch.setattr(A, "TIDE_STATIONS_PATH", os.path.join(HERE, "no-such-file.json"))
    c = A.app.test_client()
    for u in ("/api/tides/stations", "/api/tides/1612340", "/api/tides/1612340/observed"):
        r = c.get(u)
        assert r.status_code == 503 and r.headers["Cache-Control"] == "no-store"


def test_tides_stay_out_of_the_live_buoy_service():
    import app as A
    assert all("tide" not in type(p).__name__.lower() for p in A.get_buoy_providers())
    assert not any(isinstance(p, T.TideService) for p in A.get_buoy_providers())


def test_a_throttled_subordinate_and_a_reference_failed_a_moment_ago_are_failures_not_a_day_long_answer():
    """G27 A-F2 / A-F3 pins: a NOAA message other than "no predictions" on a subordinate's extremes passes; a reference
    whose failure is remembered (a direct request failed a minute ago) fails its subordinate too (not a cosine day)."""
    noaa = _answering(lambda u: "Request limit exceeded." if "station=1611401" in u else None)
    svc = T.TideService(dict(STATIONS), noaa, now=Clock(NOW))
    assert svc.forecast("1611401")[0] == "error"

    class RefDownOnce(FakeNoaa):
        down = True

        def __call__(self, u, m):
            if self.down and "station=1611400" in u:
                self.calls.append(u)
                raise IOError("timed out")
            return super().__call__(u, m)
    noaa, clock = RefDownOnce(), Clock(NOW)
    svc = T.TideService(dict(STATIONS), noaa, now=clock)
    assert svc.forecast("1611400")[0] == "error"                              # remembered under the reference's key
    noaa.down = False
    assert svc.forecast("1611401")[0] == "error"                              # within that minute: no cosine for the day
    clock.t += 61
    status, p = svc.forecast("1611401")
    assert status == "ok" and p["method"] == "reference"
