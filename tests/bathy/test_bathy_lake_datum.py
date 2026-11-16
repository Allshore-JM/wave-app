"""The Great Lakes' datum line (plan section 98): the client (static_overlay/overlay.js `bathyLake`) calls a source a lake
source when its index `datum` text holds "Low Water Datum" (any case). These tests pin that every Great Lakes source's
datum text says so and that no ocean source's does, so the panel's line and the readout's dry-ground words can never
swap between a lake and the sea."""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "bathy"))

import bathy_sources as S  # noqa: E402

CLIENT_RULE = re.compile(r"low water datum", re.I)     # = overlay.js bathyLake's /low water datum/i


def _client_rule_in_overlay():
    text = open(os.path.join(ROOT, "static_overlay", "overlay.js"), encoding="utf-8").read()
    m = re.search(r"function bathyLake\(s\) \{ return (/[^/]+/i)\.test", text)
    assert m, "bathyLake not found in overlay.js"
    return m.group(1)


def _lake_regions():
    return {n: r for n, r in S.REGIONS.items() if r.get("seed") == "lake" or n.startswith("lake-")}


def _lake_keys():
    keys = set()
    for reg in _lake_regions().values():
        keys.update(list(reg.get("sources", ())) + list(reg.get("fill", ())))
    return keys


def _lake_source(src):
    return src.get("kind") == "lakegrid" or "lake_lwd" in src


def test_the_client_rule_is_the_one_pinned_here():
    assert _client_rule_in_overlay() == "/low water datum/i"


def test_every_great_lakes_source_says_low_water_datum():
    lakes = {k: s for k, s in S.SOURCES.items() if _lake_source(s)}
    assert len(lakes) >= 6 + 10           # five lake grids + St Clair, and the lidar DEMs
    for key, src in lakes.items():
        assert CLIENT_RULE.search(src["datum"]), key


def test_no_ocean_source_says_low_water_datum():
    sea = {k: s for k, s in S.SOURCES.items() if not _lake_source(s)}
    assert sea
    for key, src in sea.items():
        assert not CLIENT_RULE.search(src.get("datum", "")), key


def test_every_source_of_a_lake_region_says_low_water_datum():
    # lake-ness from the region (seed "lake" or a "lake-" name), so a new lake region whose sources carry neither
    # builder marker cannot slip past the rule
    assert len(_lake_regions()) >= 10
    for key in _lake_keys():
        src = S.SOURCES[key]
        assert CLIENT_RULE.search(src["datum"]), key
        assert _lake_source(src), key


def test_every_lake_region_holds_only_lake_sources():
    for name, reg in S.REGIONS.items():
        srcs = [S.SOURCES[k] for k in list(reg.get("sources", ())) + list(reg.get("fill", ()))]
        assert srcs, name
        if any(_lake_source(s) for s in srcs):
            assert all(_lake_source(s) for s in srcs), name
