"""The forecast-station list leaves out the regional models' boundary points (plan section 32, owner 2026-10-03).

NOAA's GFS-Wave station list also writes out the edges of regional and coastal model domains as rows of points every
0.25-2 degrees; on the map they drew boxes around ocean areas. `station_list.json` (the select, the picker, the markers)
keeps the buoys and the named points only. `station_coords.json` and `station_timezones.json` stay whole: they are lookup
tables, and the forecast-point time-zone rule reads every coordinate (dropping the boundary points there would move
point time zones).
"""
import json
import os
import re

import app as A

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OFFICES = "KEY|MIA|SJU|BER|MLB|PCB|SRH|CRP|LIX|TBW|JXFL|LCH|CHS|HGX|CCTX|MOB|BRO|JAX|MNE|HNL"


def boundary(sid):
    """NWPS edges (NW-<office>), hurricane-model edges (HWRF), RW-NH edges, other countries' model edges (BKMG, CDIP,
    KNY, MDG, SYC), and the older office sets numbered 51 and up (HNL51-68 are the boxes around Oahu and Kauai)."""
    if re.match(r"^(NW-[A-Z]{3}\d+|RW-NH[12]-\d+|HWRF[a-z]-\d+|(BKMG|CDIP|KNY|MDG|SYC)\d+)$", sid):
        return True
    m = re.match(r"^(%s)(\d+)$" % OFFICES, sid)
    return bool(m and int(m.group(2)) >= 51)


def load(name):
    with open(os.path.join(ROOT, name), encoding="utf-8") as f:
        return json.load(f)


def test_the_list_has_no_boundary_points():
    ids = [str(s) for s in load("station_list.json")]
    assert len(ids) == 734 and len(set(ids)) == 734
    assert not [s for s in ids if boundary(s)]


def test_the_main_points_stay():
    ids = set(load("station_list.json"))
    keep = {"51201", "46026", "51003", "46035", "HNL01", "HNL10", "MIA01", "CHS01", "JAX02", "MLB01", "OPCP01",
            "OPCA01", "TPC00", "V14039", "MIDWAY", "GUAM", "SAIPAN", "KWAJALEIN"}
    assert keep <= ids and set(A.SWAN_STATIONS) <= ids
    assert sum(s[0].isdigit() for s in ids) == 461                     # every buoy (3FYT included)


def test_the_rule_matches_the_boxes():
    """Pins the rule itself: examples from each family, and the office points below 51 that stay."""
    for s in ("NW-HFO51", "NW-GUM113", "HWRFe-50", "RW-NH1-51", "RW-NH2-429", "BKMG01", "CDIP24", "KNY51", "MDG81",
              "SYC51", "HNL51", "HNL68", "MIA51", "SRH71"):
        assert boundary(s), s
    for s in ("HNL01", "HNL12", "MIA02", "CHS01", "JAX02", "51201", "OPCP13", "TPC56", "V14084", "NW-HFO"):
        assert not boundary(s), s


def test_every_listed_point_is_placed_and_the_lookup_tables_stay_whole():
    ids = load("station_list.json")
    coords, zones = load("station_coords.json"), load("station_timezones.json")
    assert all(s in coords and s in zones for s in ids)
    assert len(coords) == 4036 and len(zones) == 4036                 # the time-zone rule's neighbours are unchanged
    assert sum(boundary(s) for s in coords) == 3302
