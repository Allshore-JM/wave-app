"""tools/tides/build_global.py rules added after the section-112 test-site check (UHSLC copies, position errors, a clock
flag before the one-per-harbour choice, zones and names), on small inputs and on the committed snapshot."""
import importlib.util
import json
import math
import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location("build_global", os.path.join(ROOT, "tools", "tides", "build_global.py"))
B = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(B)

TIDE = [("M2", 50.0, 10.0), ("S2", 20.0, 40.0), ("K1", 10.0, 200.0), ("O1", 8.0, 190.0)]


def gauge(key, lat, lon, years=10.0, quality="No obvious issues", country="GBR"):
    return {"key": key, "lat": lat, "lon": lon, "type": "Coastal", "country": country, "quality": quality,
            "years": years, "source": "x", "consts": list(TIDE), "ticon": 4}


def test_uhslc_copies_take_uhslc_position_and_become_one():
    pos = json.load(open(B.UHSLC, encoding="utf-8"))["positions"]["290"]                 # Port Stanley
    t4 = [gauge("port_stanley-290-gbr-uhslc_fd", -51.75, -57.933, years=20),
          gauge("port_stanley-290a-gbr-uhslc_rq", -52.7, -57.867, years=30)]          # TICON's copies: 106 km apart
    log = {}
    out = B.choose(t4, [], [], log)
    assert len(out) == 1 and (out[0]["lat"], out[0]["lon"]) == tuple(pos)
    assert out[0]["key"].endswith("290a-gbr-uhslc_rq")                                  # the longer record is kept
    assert log["uhslc_positions_moved"] == 1


def test_a_longitude_that_repeats_the_latitude_is_dropped():
    log = {}
    out = B.choose([gauge("tubuai-113-fra-refmar", -23.34177, -23.34177), gauge("ok-1-fra-x", -23.3, -149.4)], [], [],
                   log)
    assert [g["key"] for g in out] == ["ok-1-fra-x"] and log["position_errors_dropped"] == ["tubuai-113-fra-refmar"]


def test_a_flagged_record_cannot_shadow_a_good_one():
    best = gauge("las_palmas-9a-esp-x", 28.1, -15.4, years=40)                       # longest, but 55 min early
    good = gauge("pluz-pluz-esp-ieo", 28.1005, -15.4005, years=30)
    log = {}
    out = B.choose([best, good], [], [], log, flagged={B.station_id(best["key"])})
    assert [g["key"] for g in out] == ["pluz-pluz-esp-ieo"] and log["clock_flagged_dropped"] == 1
    assert [g["key"] for g in B.choose([best, good], [], [], {})] == [best["key"]]   # without the flag: the longest


def test_near_noaa_is_one_harbour():
    log = {}
    lat, lon = 10.0, 20.0
    near = (lat + 2.5 / 111.2, lon)                                                     # 2.5 km
    assert B.choose([gauge("a-1-gbr-x", lat, lon)], [], [near], log) == [] and B.NOAA_KM == B.DEDUP_KM == 3.0


@pytest.mark.parametrize("raw,want", [
    ("LÜBECK-BAUHOF", "Lübeck-Bauhof"), ("BHV ALTER LEUCHTTURM", "Bremerhaven Alter Leuchtturm"),
    ("LIST AUF SYLT", "List auf Sylt"), ("EIDER-SPERRWERK BP", "Eider-Sperrwerk BP"), ("LT KIEL", "Leuchtturm Kiel"),
    ("WHV ALTER VORHAFEN", "Wilhelmshaven Alter Vorhafen"), ("BÜSUM", "Büsum")])
def test_wsv_names(raw, want):
    assert B.wsv_name(raw) == want


def test_station_id_matches_the_snapshot_ids():
    assert B.station_id("las_palmas-217a-esp-uhslc_rq") == "ticon:las-palmas-217a-esp-uhslc-rq"


# --- the committed snapshot ---------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def snap():
    d = json.load(open(os.path.join(ROOT, "tide_global.json"), encoding="utf-8"))
    return [dict(zip(d["fields"], r)) for r in d["stations"]]


def _km(a, b, c, d):
    p1, p2 = math.radians(a), math.radians(c)
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(d - b) / 2) ** 2
    return 12742.0 * math.asin(min(1.0, math.sqrt(h)))


def test_snapshot_one_station_per_uhslc_gauge_and_no_position_errors(snap):
    nums = [m.group(1) for s in snap for m in [re.search(r"-(\d{3})[a-z]?-[a-z]{3}-uhslc-(?:fd|rq)$", s["id"])] if m]
    assert len(nums) == len(set(nums)) and len(nums) > 200
    assert not [s["id"] for s in snap if s["lat"] == s["lon"]]


def test_snapshot_fixes_applied_and_flags_left_out(snap):
    by = {s["id"]: s for s in snap}
    for sid, tz in B.ZONE_FIX.items():
        assert by[sid]["tz"] == tz
    for sid, name in B.NAME_FIX.items():
        assert by[sid]["name"] == name
    flagged = json.load(open(B.QC, encoding="utf-8"))["flagged"]
    assert not set(flagged) & set(by)
    assert "ticon:pluz-pluz-esp-ieo" in by                                         # Las Palmas is still on the map


def test_snapshot_wsv_names(snap):
    names = json.load(open(B.WSV, encoding="utf-8"))["names"]
    wsv = [(s, re.search(r"-(\d{7,8})-deu-wsv$", s["id"])) for s in snap]
    got = [(s["name"], B.wsv_name(names[m.group(1)])) for s, m in wsv if m]
    assert len(got) > 40 and all(a == b for a, b in got)
    assert {"Büsum", "Hörnum", "Lübeck-Bauhof"} <= {a for a, _ in got}


def test_snapshot_none_within_a_harbour_of_noaa(snap):
    d = json.load(open(os.path.join(ROOT, "tide_stations.json"), encoding="utf-8"))
    f = d["fields"]
    cells = {}
    for r in d["stations"]:
        la, lo = r[f.index("lat")], r[f.index("lon")]
        cells.setdefault((math.floor(la), math.floor(lo)), []).append((la, lo))
    for s in snap:
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                for la, lo in cells.get((math.floor(s["lat"]) + dy, math.floor(s["lon"]) + dx), []):
                    assert _km(s["lat"], s["lon"], la, lo) > B.NOAA_KM, s["id"]
