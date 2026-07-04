"""Pure tests for the model-selection layer (available_models / resolve_model).

These gating rules are first-class invariants: GFS always and first, SWAN only
on the 12 SWAN_STATIONS, MFWAM wherever coords exist, and resolve_model honoring
only listed models (case-insensitive, stripped) with a silent GFS fallback.
No mocks/network -- uses the real SWAN_STATIONS + station-coords data.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import available_models, resolve_model, SWAN_STATIONS  # noqa: E402


def test_available_models_swan_station_order():
    # GFS first (the default), SWAN before MFWAM on a Hawaii buoy.
    assert available_models("51201") == ["GFS", "SWAN", "MFWAM"]


def test_available_models_non_swan_ocean_station():
    assert available_models("44025") == ["GFS", "MFWAM"]


def test_available_models_unknown_station_gfs_only():
    assert available_models("zzzz") == ["GFS"]


def test_swan_gate_is_exactly_the_12_stations():
    assert len(SWAN_STATIONS) == 12
    assert all("SWAN" in available_models(s) for s in SWAN_STATIONS)


def test_resolve_model_case_insensitive_and_stripped():
    assert resolve_model("51201", " mfwam ") == "MFWAM"
    assert resolve_model("51201", "swan") == "SWAN"
    assert resolve_model("51201", "GFS") == "GFS"


def test_resolve_model_silent_gfs_fallback():
    assert resolve_model("44025", None) == "GFS"     # absent param
    assert resolve_model("44025", "") == "GFS"       # empty param
    assert resolve_model("44025", "SWAN") == "GFS"   # model not offered here
    assert resolve_model("51201", "NAM") == "GFS"    # unknown model
