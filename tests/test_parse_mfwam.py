"""Pure fixture tests for the MFWAM (Open-Meteo) parser -- no network.

Fixture: tests/fixtures/mfwam_sample.json -- a real Open-Meteo marine response
for station 51201's coords (Waimea Bay), trimmed to 12 hourly steps and
hand-arranged to cover the edge cases:
  step 4: secondary swell (1.8m @ 14s / 308) CLEARLY dominant over primary
          (0.5m @ 6s / 40) -- Hs-desc ordering test
  step 5: wind_wave_* all null -- only 2 groups populated, compacted
  step 6: wind_wave 0.0 Hs + 0.0 period -- blank rule
  steps 9-11: all-null tail (SYNTHESIZED -- the live tail is not guaranteed;
          one probe saw 216/240 non-null, another 240/240)

Also here: the ECMWF bulk-only decision record (see the last two tests).
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import _parse_mfwam_json  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "mfwam_sample.json")
ECMWF_FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures",
                             "openmeteo_ecmwf_bulk_only.json")
TZ = None  # buoy-local (51201 -> Pacific/Honolulu via station_timezones.json)


def _load():
    with open(FIXTURE) as f:
        return json.load(f)


def _parse(data=None, tz=TZ, retrieved=None):
    return _parse_mfwam_json(data if data is not None else _load(), "51201", tz, retrieved)


def test_parses_cleanly():
    cycle, loc, mr, rows, tz, err = _parse()
    assert err is None
    assert rows
    assert tz == "Pacific/Honolulu"
    assert mr is None


def test_row_shape_21_cols():
    _, _, _, rows, _, err = _parse()
    assert err is None
    for r in rows:
        assert len(r) == 21
        assert isinstance(r[0], str) and "," in r[0]   # date string
        assert isinstance(r[1], str) and r[1][-1] == "M"  # "H:MM AM/PM"


def test_meters_to_feet_and_rounding():
    data = _load()
    h = data["hourly"]
    # step 0 is untouched real data
    exp_hs = round(h["swell_wave_height"][0] * 3.28084, 2)
    exp_comb = round(h["wave_height"][0] * 3.28084, 2)
    _, _, _, rows, _, err = _parse(data)
    assert err is None
    r0 = rows[0]
    hs_vals = [r0[2 + 3 * i] for i in range(6) if r0[2 + 3 * i] is not None]
    assert exp_hs in hs_vals          # primary swell present among groups
    assert r0[20] == exp_comb         # combined from wave_height
    for i in range(6):                # periods rounded to 1dp
        tp = r0[3 + 3 * i]
        if tp is not None:
            assert tp == round(tp, 1)


def test_direction_not_flipped():
    # Fixture step 4 primary swell dir = 40 (FROM NE). A wrong (x+180)%360
    # "fix" would turn it into 220. Also directions must be ints.
    _, _, _, rows, _, err = _parse()
    assert err is None
    r4 = rows[4]
    dirs = [r4[4 + 3 * i] for i in range(6) if r4[4 + 3 * i] is not None]
    assert 40 in dirs
    assert all(d not in (220,) for d in dirs)
    assert all(isinstance(d, int) for d in dirs)


def test_hs_desc_ordering_into_s1():
    # Step 4: secondary swell (1.8m @ 14s / 308) must land in Swell 1, ahead of
    # the smaller primary (0.5m @ 6s / 40).
    _, _, _, rows, _, err = _parse()
    assert err is None
    r4 = rows[4]
    assert r4[2] == round(1.8 * 3.28084, 2)   # S1 Hs = the dominant component
    assert r4[3] == 14.0
    assert r4[4] == 308
    # the demoted primary swell is still present in a later group
    hs = [r4[2 + 3 * i] for i in range(6) if r4[2 + 3 * i] is not None]
    assert round(0.5 * 3.28084, 2) in hs
    # descending Hs across all populated groups (wind wave may sit in between)
    assert hs == sorted(hs, reverse=True)


def test_groups_4_to_6_always_blank():
    _, _, _, rows, _, err = _parse()
    assert err is None
    for r in rows:
        for i in range(3, 6):  # groups 4..6 (only 3 MFWAM components exist)
            assert r[2 + 3 * i] is None
            assert r[3 + 3 * i] is None
            assert r[4 + 3 * i] is None


def test_null_component_is_blank_and_compacted():
    # Step 5: wind_wave nulled -> exactly 2 populated groups, compacted left.
    _, _, _, rows, _, err = _parse()
    assert err is None
    r5 = rows[5]
    populated = [i for i in range(6) if r5[2 + 3 * i] is not None]
    assert populated == [0, 1]


def test_zero_hs_zero_period_blank():
    # Step 6: wind_wave 0.0/0.0 -> blank, so again only 2 populated groups.
    _, _, _, rows, _, err = _parse()
    assert err is None
    r6 = rows[6]
    populated = [i for i in range(6) if r6[2 + 3 * i] is not None]
    assert populated == [0, 1]


def test_all_null_tail_trimmed():
    # Steps 9-11 are all-null -> trimmed; 12 fixture steps yield 9 rows.
    _, _, _, rows, _, err = _parse()
    assert err is None
    assert len(rows) == 9


def test_time_parsed_as_utc_no_z():
    # Fixture times are ISO WITHOUT Z but UTC (request pins timezone=UTC):
    # 2026-xx-xxT00:00 UTC == 2:00 PM previous day HST (UTC-10).
    data = _load()
    first_utc_hour = int(data["hourly"]["time"][0][11:13])
    _, _, _, rows, _, err = _parse(data)
    assert err is None
    expected_hst_hour = (first_utc_hour - 10) % 24
    display = expected_hst_hour % 12 or 12
    assert rows[0][1].startswith(f"{display}:00")
    # Pin AM/PM and the UTC->HST date rollover: 00:00 UTC == 2:00 PM the PREVIOUS
    # local day. A refactor that formats the date from ts_utc while the time uses
    # local_dt would show the right clock time next to a day-late date, and only
    # these two asserts would catch it. (startswith("Friday") is platform-safe
    # across the %-d/%d zero-pad difference, mirroring test_parse_swan.py.)
    assert rows[0][1].endswith("PM")
    assert rows[0][0].startswith("Friday")


def test_tz_override_applied_and_invalid_falls_back():
    _, _, _, _, tz, err = _parse(tz="America/New_York")
    assert err is None and tz == "America/New_York"
    _, _, _, _, tz2, err2 = _parse(tz="Not/AZone")
    assert err2 is None and tz2 == "Pacific/Honolulu"


def test_location_hemisphere_formatting():
    # Negative-W lon must render "158.12W", never "-158.12W".
    _, loc, _, _, _, err = _parse()
    assert err is None
    assert loc == "Location : 51201 (21.67N 158.12W)"
    assert "-" not in loc


def test_cycle_label_both_forms():
    from datetime import datetime
    c1, _, _, _, _, _ = _parse(retrieved=datetime(2026, 7, 4, 17, 0, 0))
    assert c1 == "Cycle : Meteo-France MFWAM via Open-Meteo, retrieved 20260704 17 UTC"
    assert "run" not in c1.lower().replace("retrieved", "")  # honest: retrieved, not a model run
    c2, _, _, _, _, _ = _parse(retrieved=None)
    assert c2 == "Cycle : Meteo-France MFWAM via Open-Meteo"
    assert "Open-Meteo" in c1 and "Open-Meteo" in c2


def test_malformed_json_fails_soft():
    for bad in (None, [], "nope", {"error": True, "reason": "Bad request"}, {}):
        _, _, _, rows, _, err = _parse_mfwam_json(bad, "51201", TZ, None)
        assert rows is None
        assert err and "unexpected format" in err


def test_missing_hourly_key_fails_soft():
    _, _, _, rows, _, err = _parse_mfwam_json({"latitude": 21.7}, "51201", TZ, None)
    assert rows is None and "unexpected format" in err
    _, _, _, rows2, _, err2 = _parse_mfwam_json({"hourly": {"time": []}}, "51201", TZ, None)
    assert rows2 is None and "unexpected format" in err2


def test_short_value_array_degrades_to_blanks():
    # A truncated variable array must yield blanks for the missing indices,
    # not crash.
    data = _load()
    data["hourly"]["secondary_swell_wave_height"] = data["hourly"]["secondary_swell_wave_height"][:2]
    _, _, _, rows, _, err = _parse(data)
    assert err is None
    assert rows  # still parses; later steps just lack the secondary component


def test_nonfinite_values_rejected():
    # 'inf'/NaN in any slot degrades to a blank, mirroring the SWAN hardening.
    data = _load()
    data["hourly"]["swell_wave_direction"][0] = float("inf")
    _, _, _, rows, _, err = _parse(data)
    assert err is None
    r0 = rows[0]
    # the primary swell survives with a blank direction
    assert any(r0[2 + 3 * i] is not None and r0[4 + 3 * i] is None for i in range(6))


def test_partial_rows_survive_trim():
    # Trim invariant is AND, not OR: a row is dropped ONLY when combined AND all
    # components are blank. A combined-only row and a components-only row (both
    # realistic near the run-age-dependent tail) must survive.
    data = _load()
    h = data["hourly"]
    for k in h:                       # step 7 -> combined-only
        if k not in ("time", "wave_height"):
            h[k][7] = None
    h["wave_height"][8] = None        # step 8 -> components-only
    _, _, _, rows, _, err = _parse(data)
    assert err is None
    assert len(rows) == 9             # neither partial row trimmed
    r7, r8 = rows[7], rows[8]
    assert r7[20] is not None and all(r7[2 + 3 * i] is None for i in range(6))
    assert r8[20] is None and any(r8[2 + 3 * i] is not None for i in range(3))


# ---- parse_mfwam cache wrapper (key isolation, negative cache, Date header) ----

def _clear_cache():
    import app
    with app._CACHE_LOCK:
        app._FORECAST_CACHE.clear()


def test_cache_key_isolated_and_only_clean_cached(monkeypatch):
    import app
    _clear_cache()
    clean = ("Cycle : x", "Location : x", None, [["d", "t"] + [None] * 19], "UTC", None)
    err = (None, None, None, None, "UTC", "boom")
    calls = {"n": 0}

    def stub(station_id, tz=None):
        calls["n"] += 1
        return err if calls["n"] == 1 else clean

    monkeypatch.setattr(app, "_parse_mfwam_uncached", stub)
    # pre-seed a fake GFS entry to prove MFWAM never returns it
    with app._CACHE_LOCK:
        app._FORECAST_CACHE[("51201", "", "GFS")] = {"ts": 9e18, "data": clean}

    # call 1: error -> negatively cached (short ttl), still re-probed after expiry
    r1 = app.parse_mfwam("51201", None)
    assert r1[-1] == "boom" and calls["n"] == 1
    entry = app._FORECAST_CACHE[("51201", "", "MFWAM")]
    assert entry["ttl"] == app._FORECAST_NEG_TTL
    # within the negative window a repeat is served from cache (no new call)
    assert app.parse_mfwam("51201", None)[-1] == "boom" and calls["n"] == 1

    # expire the negative entry -> re-probe yields the clean parse, long ttl
    entry["ts"] -= app._FORECAST_NEG_TTL + 1
    r2 = app.parse_mfwam("51201", None)
    assert r2[-1] is None and calls["n"] == 2
    good = app._FORECAST_CACHE[("51201", "", "MFWAM")]
    assert good["ttl"] == app._FORECAST_CACHE_TTL
    # the MFWAM key is distinct from GFS; the GFS entry is untouched
    assert ("51201", "", "GFS") in app._FORECAST_CACHE
    # a third call hits the (clean) cache
    assert app.parse_mfwam("51201", None)[-1] is None and calls["n"] == 2
    _clear_cache()


def test_junk_tz_collapses_to_default_cache_key(monkeypatch):
    # Quota guard: an invalid ?tz= must key on "" (same as buoy-local), so a
    # ?tz=junk{n} flood cannot multiply outbound Open-Meteo calls.
    import app
    _clear_cache()
    clean = ("c", "l", None, [["d", "t"] + [None] * 19], "UTC", None)
    calls = {"n": 0}

    def stub(station_id, tz=None):
        calls["n"] += 1
        return clean

    monkeypatch.setattr(app, "_parse_mfwam_uncached", stub)
    for j in range(5):
        app.parse_mfwam("51201", f"totally/bogus_{j}")
    assert calls["n"] == 1                      # all junk tz collapsed to one key
    assert ("51201", "", "MFWAM") in app._FORECAST_CACHE
    # a valid tz keys separately and does fetch once
    app.parse_mfwam("51201", "America/New_York")
    assert calls["n"] == 2
    assert ("51201", "America/New_York", "MFWAM") in app._FORECAST_CACHE
    _clear_cache()


def test_date_header_feeds_retrieved_cycle_label(monkeypatch):
    import app

    class FakeResp:
        status_code = 200
        headers = {"Date": "Fri, 04 Jul 2026 17:00:00 GMT"}

        def json(self):
            return _load()

    monkeypatch.setattr(app.HTTP, "get", lambda *a, **k: FakeResp())
    cycle, _, _, rows, _, err = app._parse_mfwam_uncached("51201", None)
    assert err is None and rows
    assert cycle == "Cycle : Meteo-France MFWAM via Open-Meteo, retrieved 20260704 17 UTC"

    class NoDate(FakeResp):
        headers = {}

    monkeypatch.setattr(app.HTTP, "get", lambda *a, **k: NoDate())
    cycle2, _, _, _, _, err2 = app._parse_mfwam_uncached("51201", None)
    assert err2 is None
    assert cycle2 == "Cycle : Meteo-France MFWAM via Open-Meteo"


# ----------------------- ECMWF bulk-only decision record -----------------------

def test_openmeteo_ecmwf_is_bulk_only_fixture():
    """Decision record (2026-07-03 research): Open-Meteo's ECMWF WAM feed
    (models=ecmwf_wam025) returns NO swell/wind-wave component data -- only
    bulk wave_height -- because ECMWF open data lacks the partition params
    (swh1-3/mwd1-3/mwp1-3 are paid-catalogue/dissemination only). This is WHY
    the site's European model is MFWAM (meteofrance_wave) rather than ECMWF.
    If ECMWF partitions ever reach Open-Meteo, the live canary below fails --
    the signal to revisit ECMWF as a fourth model.
    """
    with open(ECMWF_FIXTURE) as f:
        d = json.load(f)
    h = d["hourly"]
    component_vars = [k for k in h if k not in ("time", "wave_height")]
    assert component_vars, "fixture must include the requested component vars"
    for k in component_vars:
        assert all(v is None for v in h[k]), f"{k} unexpectedly has data"
    assert any(v is not None for v in h["wave_height"])  # bulk IS served


@pytest.mark.skipif(not os.environ.get("RUN_LIVE"),
                    reason="live canary; set RUN_LIVE=1 to probe the real API")
def test_openmeteo_ecmwf_bulk_only_live():
    """Live canary: FAILS when Open-Meteo's ecmwf_wam025 starts serving swell
    components -- i.e. when ECMWF partitions become freely available and the
    ECMWF-as-fourth-model plan should be revisited."""
    import urllib.request
    url = ("https://marine-api.open-meteo.com/v1/marine?latitude=21.67&longitude=-158.12"
           "&hourly=swell_wave_height,secondary_swell_wave_height,wind_wave_height"
           "&models=ecmwf_wam025&forecast_days=1&timezone=UTC&cell_selection=sea")
    with urllib.request.urlopen(url, timeout=30) as r:
        d = json.loads(r.read().decode())
    h = d["hourly"]
    for k in ("swell_wave_height", "secondary_swell_wave_height", "wind_wave_height"):
        # Guard against a vacuous pass: all() over an empty list is True, so a
        # response that simply omits/nulls the key would silently neutralize the
        # canary. Require the key to be present with a non-empty array first.
        assert k in h and h[k], (
            f"{k} missing/empty in ecmwf_wam025 response -- shape changed; "
            "re-verify bulk-only status before trusting this canary")
        assert all(v is None for v in h[k]), (
            f"ECMWF WAM now serves {k} on Open-Meteo -- partitions have arrived; "
            "revisit adding ECMWF as a fourth model!")
