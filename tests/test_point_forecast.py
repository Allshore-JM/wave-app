"""Forecast points (plan section 31, step 4): point ids, the reader of the points product, the tracking of swell
trains, the rows, and the /api/forecast path. No network: a small product is built with the format's own writer
(tools/model_frames/pointfmt.py) and served by a fake fetch; one real cell (NOAA station 51201's model cell, run
2026100112) and its NOAA bulletin are fixtures.

Run:  pytest tests/
"""
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime

import numpy as np
import pytest
import pytz

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import app as A                    # noqa: E402
import point_forecast as PFC       # noqa: E402

PF = PFC.pointfmt()
FIX = os.path.join(ROOT, "tests", "fixtures")
RUN = "2026100112"
STEPS = [0, 1, 2, 3, 6, 9, 120, 123, 384]
GRIDS = (                                                    # two whole circles of longitude, as the real grids are
    {"name": "a", "ni": 360, "nj": 21, "lat0": 10.0, "per_deg": 1, "tile": 8},      # 10 N .. 10 S, one degree
    {"name": "b", "ni": 720, "nj": 40, "lat0": 30.0, "per_deg": 2, "tile": 16},     # 30 N .. 10.5 N, half a degree
)


# ------------------------------- a small product --------------------------------------

def sea(grid):
    """The grid's sea mask: land is a block near 0 E, a meridian strip and the whole of 100..109 E."""
    r, c = np.mgrid[0:grid["nj"], 0:grid["ni"]]
    lon = c / grid["per_deg"]
    land = ((lon >= 100) & (lon < 110)) | ((r % 6 == 2) & (c % 9 == 4)) | ((r < 3) & (lon < 2))
    return ~land


def code(grid, name, si, r, c):
    """The stored code of a field at a step and cell: the cell and the step can be read back from it."""
    kind = PF.FIELD_KIND[name]
    if name == "hs":
        return 200 + (r * 7 + c * 3) % 50 + si
    if name in ("wind", "wdir"):
        return 55 + si if name == "wind" else (80 + r) % 360
    part = int(name[1]) if name[0] == "s" else 0                 # 0 = wind sea, 1..3 = swells
    if part == 3 and si % 2:                                      # the third swell comes and goes
        return PF.MISSING
    return {"height": 150 - 30 * part + si, "period": 60 + 40 * part, "direction": (70 + 80 * part + c) % 360}[kind]


class Product:
    """latest.json, a manifest, masks and tiles of one run, built on demand; every fetch is recorded."""

    def __init__(self, run=RUN, steps=STEPS, grids=GRIDS):
        self.run, self.steps, self.grids = run, list(steps), grids
        self.calls, self.fail, self.delay, self.lock = [], None, 0.0, threading.Lock()
        self.mkey = f"{PFC.PREFIX}/{run}/manifest-20261001T174000Z.json"
        self.overrides = {}

    def manifest(self):
        return {"schema": 1, "format": PF.FORMAT, "format_key": "x", "run": self.run, "complete": True,
                "steps": self.steps, "missing": PF.MISSING, "published_utc": "2026-10-01T17:40:00Z",
                "fields": [{"name": n, "kind": k, "grib": g, "scale": PF.KINDS[k]["scale"], "units": PF.KINDS[k]["units"]}
                           for n, k, g, _i in PF.FIELDS],
                "grids": [{"name": g["name"], "ni": g["ni"], "nj": g["nj"], "lat0": g["lat0"], "lon0": 0.0,
                           "per_deg": g["per_deg"], "tile": g["tile"], "data_rows": [0, g["nj"] - 1],
                           "mask": f"{PFC.PREFIX}/{self.run}/{g['name']}/mask.bin"} for g in self.grids]}

    def body(self, key):
        if key in self.overrides:
            return self.overrides[key]
        if key == f"{PFC.PREFIX}/latest.json":
            return json.dumps({"run": self.run, "manifest": self.mkey, "complete": True}).encode()
        if key == self.mkey:
            return json.dumps(self.manifest()).encode()
        m = re.fullmatch(rf"{PFC.PREFIX}/{self.run}/([a-z0-9]+)/(mask|(\d+)_(\d+))\.bin", key)
        if not m:
            raise IOError("HTTP 404")
        grid = next(g for g in self.grids if g["name"] == m.group(1))
        mask = sea(grid)
        if m.group(2) == "mask":
            return PF.encode_mask(self.run, grid, mask)
        tr, tc, t = int(m.group(3)), int(m.group(4)), grid["tile"]
        bitmap = PF.tile_bitmap(mask, t, tr, tc)
        if not bitmap.size or not bitmap.any():
            raise IOError("HTTP 404")
        rr, cc = np.nonzero(bitmap)
        planes = np.array([[[code(grid, n, si, tr * t + r, tc * t + c) for r, c in zip(rr, cc)]
                            for si in range(len(self.steps))] for n in PF.FIELD_NAMES], dtype=np.uint16)
        return PF.encode_tile(PF.tile_header(self.run, grid, tr, tc, bitmap.shape[0], bitmap.shape[1], len(rr), len(self.steps)),
                              bitmap, planes)

    def fetch(self, url, max_bytes):
        key = url.split("https://bucket/", 1)[1]
        with self.lock:
            self.calls.append(key)
        if self.delay:
            time.sleep(self.delay)
        if self.fail and self.fail(key):
            raise IOError("HTTP 503")
        body = self.body(key)
        if len(body) > max_bytes:
            raise IOError("body too large")
        return body

    def source(self, clock=time.time):
        return PFC.PointSource(lambda: "https://bucket", self.fetch, clock)

    def count(self, what):
        return sum(1 for k in self.calls if what in k)


@pytest.fixture
def product():
    return Product()


def cell_of(grid, lat, lon):
    return int(round((grid["lat0"] - lat) * grid["per_deg"])), int(round((lon % 360) * grid["per_deg"])) % grid["ni"]


# ------------------------------- point ids --------------------------------------------

def test_point_id_is_one_spelling_per_point():
    assert PFC.point_id(21.667, -158.054) == "pt_21667N_158054W"
    assert PFC.point_id(-17.8664, 210.7376) == "pt_17866S_149262W"        # any longitude, rounded to a thousandth
    assert PFC.point_id(0, 0) == PFC.point_id(-0.0004, -0.0004) == "pt_0N_0E"
    assert PFC.point_id(52.45, 180) == PFC.point_id(52.45, -180) == PFC.point_id(52.45, 179.9996) == "pt_52450N_180000W"
    assert PFC.point_id(-90, 12.3456) == "pt_90000S_12346E" and PFC.point_id(0.0005, 0.0005) == "pt_1N_1E"
    for ident, want in (("pt_21667N_158054W", (21.667, -158.054)), ("pt_0N_0E", (0.0, 0.0)), ("pt_90000S_12346E", (-90.0, 12.346)),
                        ("pt_52450N_180000W", (52.45, -180.0)), ("pt_5S_7E", (-0.005, 0.007))):
        assert PFC.parse_point_id(ident) == want and PFC.point_id(*want) == ident
    for bad in ("pt_021667N_158054W", "pt_0S_0E", "pt_0N_0W", "pt_21667N_180000E", "pt_90001N_0E", "pt_1N_180001W",
                "pt_21667n_158054W", "pt_21667N_158054W ", " pt_21667N_158054W", "pt_21.667N_158.054W", "pt_21667N", "pt__",
                "51201", "", None, "PT_21667N_158054W", "pt_21667N_158054W_x", "pt_-1N_5E", "pt_123456N_5E"):
        assert PFC.parse_point_id(bad) is None, bad
    with pytest.raises(ValueError):
        PFC.point_id(90.01, 0)
    assert PFC.is_point_id("pt_x") and not PFC.is_point_id("51201") and not PFC.is_point_id(None)
    assert all(A._STATION_RE.fullmatch(PFC.point_id(la, lo)) for la, lo in ((89.999, 179.999), (-89.999, -179.999)))
    assert PFC.fmt_coord(21.667, -158.054) == "21.667N 158.054W" and PFC.fmt_coord(-14.5, 170.75, 2) == "14.50S 170.75E"


def test_no_station_is_spelled_like_a_point():
    ids = [sid for sid, _name in A.get_station_list()] + list(A.SWAN_STATIONS)
    assert len(ids) > 1000 and not any(PFC.is_point_id(s) for s in ids)


# ------------------------------- the manifest -----------------------------------------

def test_check_manifest_reads_the_jobs_real_manifest():
    raw = json.load(open(os.path.join(FIX, "points_manifest.json")))
    man = PFC.check_manifest(raw)
    assert man["run"] == RUN and man["run_dt"] == datetime(2026, 10, 1, 12) and len(man["steps"]) == 209
    assert [g["name"] for g in man["grids"]] == ["g16", "s25", "n25"]
    assert man["grids"][0] == {"name": "g16", "ni": 2160, "nj": 406, "lat0": 52.5, "per_deg": 6, "tile": 24, "data_rows": (2, 390)}
    assert man["grids"][1]["data_rows"] == (9, 272) and man["grids"][2]["data_rows"] == (17, 151)
    assert man["published_utc"] == raw["published_utc"]


def test_check_manifest_refuses_what_this_reader_does_not_read(product):
    good = product.manifest()
    assert PFC.check_manifest(good)["steps"] == STEPS

    def bad(**over):
        m = json.loads(json.dumps(good))
        for path, value in over.items():
            node, keys = m, path.split("__")
            for k in keys[:-1]:
                node = node[int(k) if k.isdigit() else k]
            if value is KeyError:
                del node[keys[-1]]
            else:
                node[int(keys[-1]) if keys[-1].isdigit() else keys[-1]] = value
        return m
    cases = [bad(format="apt2"), bad(complete=False), bad(run="20261001"), bad(run=2026100112), bad(run="2026100199"),
             bad(steps=[]), bad(steps=[0, 0, 1]), bad(steps=[0, "1"]), bad(steps=[3, 2]), bad(steps=list(range(1025))),
             bad(missing=0), bad(fields__0__name="hsig"), bad(fields__2__scale=100), bad(fields__2__kind="height"),
             bad(fields=good["fields"][:-1]), bad(fields="hs"), bad(grids=[]), bad(grids="g16"), bad(grids__0="g16"),
             bad(grids__0__name="A"), bad(grids__1__name="a"), bad(grids__0__ni=361), bad(grids__0__ni="360"),
             bad(grids__0__nj=0), bad(grids__0__per_deg=1.0), bad(grids__0__per_deg=0), bad(grids__0__tile=65),
             bad(grids__0__lat0="10"), bad(grids__0__lat0=91.0), bad(grids__0__lon0=-180.0), bad(grids__0__data_rows=[5, 3]),
             bad(grids__0__data_rows=[0, 21]), bad(grids__0__data_rows=KeyError), bad(steps=KeyError), bad(run=KeyError),
             [good], "manifest", None]
    for i, m in enumerate(cases):
        with pytest.raises(PFC.PointError):
            PFC.check_manifest(m)
            raise AssertionError(f"case {i} accepted")


# ------------------------------- the nearest sea cell ---------------------------------

def test_nearest_sea_cell_reach_ties_and_the_circle(product):
    man = PFC.check_manifest(product.manifest())
    a, b = man["grids"]
    masks = {g["name"]: sea(g) for g in GRIDS}
    hit = PFC.nearest_sea_cell(man["grids"], masks, 5.2, 20.3)                 # nearest centre of grid a: 5 N 20 E
    assert (hit["grid"], hit["row"], hit["col"], hit["lat"], hit["lon"]) == ("a", 5, 20, 5.0, 20.0) and 20 < hit["km"] < 45
    assert PFC.nearest_sea_cell(man["grids"], masks, 5.45, 20.45) is None      # 70 km from every centre: out of reach
    assert PFC.nearest_sea_cell(man["grids"], masks, 5.45, 20.45, reach_km=80)["km"] > 60
    assert PFC.nearest_sea_cell(man["grids"], masks, 5.0, 104.0) is None       # land, and no sea within reach
    hit = PFC.nearest_sea_cell(man["grids"], masks, 5.0, 109.8)                # land, but the sea begins at 110 E
    assert (hit["col"], round(hit["km"], 1)) == (110, 22.2)
    hit = PFC.nearest_sea_cell(man["grids"], masks, 0.1, -0.2)                 # across 0 E: column 0, longitude 0
    assert (hit["grid"], hit["col"], hit["lon"]) == ("a", 0, 0.0)
    hit = PFC.nearest_sea_cell(man["grids"], masks, 0.1, 359.8)
    assert (hit["col"], hit["lon"]) == (0, 0.0) and 22 < hit["km"] < 26
    hit = PFC.nearest_sea_cell(man["grids"], masks, -3.0, 181.2)               # east longitudes above 180 come back as west
    assert (hit["col"], hit["lon"]) == (181, -179.0)
    hit = PFC.nearest_sea_cell(man["grids"], masks, 20.2, 50.2)                # only grid b reaches 20 N
    assert (hit["grid"], hit["lat"], hit["lon"]) == ("b", 20.0, 50.0)
    # both grids have a cell at 10 N 50 E exactly... grid b ends at 10.5 N, so a point between them takes the nearer
    assert PFC.nearest_sea_cell(man["grids"], masks, 10.2, 50.0)["grid"] == "a"
    assert PFC.nearest_sea_cell(man["grids"], masks, 10.3, 50.0)["grid"] == "b"
    # an exact tie goes to the earlier grid, whichever order they come in
    for order in (man["grids"], list(reversed(man["grids"]))):
        assert PFC.nearest_sea_cell(order, masks, 10.25, 50.0)["grid"] == order[0]["name"]
    assert PFC.nearest_sea_cell(man["grids"], {"b": masks["b"]}, 5.2, 20.3) is None    # a grid without a mask is not looked at
    assert PFC.grid_in_reach(a, 10.3) and not PFC.grid_in_reach(a, 10.5) and PFC.grid_in_reach(b, 10.2) and not PFC.grid_in_reach(b, 9.9)
    assert PFC.REACH_KM == 40.0


def test_reach_covers_the_pocket_off_the_dutch_coast():
    """G22a re-check R-3: NOAA's 1/6-degree grid has no waves on a few cells off Zeeland; Domburg's nearest stored
    cell is 23.7 km away, two columns over. The reach is a distance in km, the same on every grid."""
    grid = {"name": "g16", "ni": 2160, "nj": 406, "lat0": 52.5, "per_deg": 6, "tile": 24, "data_rows": (2, 390)}
    mask = np.zeros((406, 2160), bool)
    mask[6, 19] = True                                                          # 51.5 N 3.1667 E, the cell the live product has
    hit = PFC.nearest_sea_cell([grid], {"g16": mask}, 51.57, 3.49)
    assert (hit["row"], hit["col"]) == (6, 19) and 23 < hit["km"] < 24.5
    assert PFC.nearest_sea_cell([grid], {"g16": mask}, 51.57, 3.49, reach_km=23.0) is None
    assert PFC.nearest_sea_cell([grid], {"g16": mask}, 51.9, 3.2) is None       # 44 km: out of reach


# ------------------------------- the source: fetches and caches ----------------------

def test_source_reads_a_cell_and_fetches_each_object_once(product):
    src = product.source()
    man = src.manifest()
    assert (man["run"], man["steps"]) == (RUN, STEPS)
    cell = src.locate(man, 5.2, 20.3)
    assert (cell["grid"], cell["row"], cell["col"]) == ("a", 5, 20)
    assert product.count("/b/") == 0                                            # grid b is out of reach: its mask is not fetched
    codes = src.series(man, cell)
    assert codes.shape == (15, len(STEPS)) and codes.dtype == np.dtype("<u2")
    for fi, name in enumerate(PF.FIELD_NAMES):
        assert codes[fi].tolist() == [code(GRIDS[0], name, si, 5, 20) for si in range(len(STEPS))], name
    again = src.series(man, src.locate(src.manifest(), 5.2, 20.3))
    assert (again == codes).all()
    other = src.series(man, src.locate(man, 4.9, 22.1))                         # a neighbour in the same tile
    assert other[0, 0] == code(GRIDS[0], "hs", 0, 5, 22)
    assert [product.count(k) for k in ("latest.json", "manifest-", "/a/mask.bin", "/a/0_2.bin")] == [1, 1, 1, 1]
    assert len(product.calls) == 4 and src.stats() == {"masks": 1, "tiles": 1, "tile_bytes": src.stats()["tile_bytes"], "cells": 2}
    b = src.locate(man, 20.2, 50.2)
    assert src.series(man, b)[0, 3] == code(GRIDS[1], "hs", 3, 20, 100) and product.count("/b/1_6.bin") == 1


def test_manifest_is_read_again_after_five_minutes_and_a_new_run_drops_the_old(product):
    now = [1000.0]
    src = product.source(clock=lambda: now[0])
    man = src.manifest()
    src.series(man, src.locate(man, 5.2, 20.3))
    now[0] += 299
    assert src.manifest() is man and product.count("latest.json") == 1
    now[0] += 2
    assert src.manifest() is man                                                # the same run: the manifest is not read again
    assert (product.count("latest.json"), product.count("manifest-")) == (2, 1)
    newer = Product(run="2026100118")
    product.run, product.mkey = newer.run, newer.mkey
    now[0] += 301
    man2 = src.manifest()
    assert man2["run"] == "2026100118" and src.stats()["cells"] == 0 and src.stats()["tiles"] == 0 and src.stats()["masks"] == 0
    cell = src.locate(man2, 5.2, 20.3)
    assert src.series(man2, cell)[0, 0] == code(GRIDS[0], "hs", 0, 5, 20) and product.count("2026100118/a/0_2.bin") == 1


def test_a_bucket_that_does_not_answer(product):
    now = [1000.0]
    src = product.source(clock=lambda: now[0])
    product.fail = lambda key: True
    with pytest.raises(PFC.PointError, match="temporarily unavailable"):
        src.manifest()
    n = len(product.calls)
    with pytest.raises(PFC.PointError):                                         # not asked again for a minute
        src.manifest()
    assert len(product.calls) == n
    now[0] += PFC.POINTER_RETRY_S + 1
    product.fail = None
    man = src.manifest()
    assert man["run"] == RUN
    # with a manifest in hand: a failed pointer read serves the last one, asks again after a minute, gives up after 6 h
    product.fail = lambda key: "latest.json" in key
    now[0] += PFC.POINTER_TTL_S + 1
    n = product.count("latest.json")
    assert src.manifest() is man and src.manifest() is man and product.count("latest.json") == n + 1
    now[0] += PFC.POINTER_RETRY_S + 1
    assert src.manifest() is man and product.count("latest.json") == n + 2
    now[0] += PFC.POINTER_KEEP_S
    with pytest.raises(PFC.PointError):
        src.manifest()
    off = PFC.PointSource(lambda: "", product.fetch)
    with pytest.raises(PFC.PointError, match="not available on this server"):
        off.manifest()


def test_a_pointer_or_object_that_is_not_what_was_asked_for_is_refused(product):
    def source_with(key, body):
        p = Product()
        p.overrides[key] = body
        return p, p.source()
    latest = f"{PFC.PREFIX}/latest.json"
    for body in (b"[1]", b"{", b'{"run":"2026100112"}', json.dumps({"run": RUN, "manifest": "gfswave/0p25/v1/x.json"}).encode(),
                 json.dumps({"run": RUN, "manifest": f"{PFC.PREFIX}/{RUN}/../../secret.json"}).encode(),
                 json.dumps({"run": "2026100118", "manifest": product.mkey}).encode(), b"\xff\xfe"):
        p, src = source_with(latest, body)
        with pytest.raises(PFC.PointError):
            src.manifest()
        assert not any("secret" in k or "0p25" in k for k in p.calls)
    p, src = source_with(product.mkey, json.dumps(dict(product.manifest(), run="2026100118")).encode())   # the manifest of another run
    with pytest.raises(PFC.PointError):
        src.manifest()
    other = Product(run="2026100118")
    p, src = source_with(f"{PFC.PREFIX}/{RUN}/a/mask.bin", other.body(f"{PFC.PREFIX}/2026100118/a/mask.bin"))   # another run's mask
    with pytest.raises(PFC.PointError):
        src.locate(src.manifest(), 5.2, 20.3)
    p, src = source_with(f"{PFC.PREFIX}/{RUN}/a/mask.bin", product.body(f"{PFC.PREFIX}/{RUN}/b/mask.bin"))        # another grid's
    with pytest.raises(PFC.PointError):
        src.locate(src.manifest(), 5.2, 20.3)
    for wrong in (f"{PFC.PREFIX}/{RUN}/a/0_3.bin", f"{PFC.PREFIX}/2026100118/a/0_2.bin"):                          # another tile, another run's
        body = (other if "2026100118" in wrong else product).body(wrong)
        p, src = source_with(f"{PFC.PREFIX}/{RUN}/a/0_2.bin", body)
        man = src.manifest()
        with pytest.raises(PFC.PointError):
            src.series(man, src.locate(man, 5.2, 20.3))
    short = Product(steps=STEPS[:-1])                                           # a tile with fewer steps than the manifest says
    p, src = source_with(f"{PFC.PREFIX}/{RUN}/a/0_2.bin", short.body(f"{PFC.PREFIX}/{RUN}/a/0_2.bin"))
    man = src.manifest()
    with pytest.raises(PFC.PointError):
        src.series(man, src.locate(man, 5.2, 20.3))
    p, src = source_with(f"{PFC.PREFIX}/{RUN}/a/0_2.bin", b"APT1" + b"\x00" * 64)
    man = src.manifest()
    with pytest.raises(PFC.PointError):
        src.series(man, src.locate(man, 5.2, 20.3))


def test_the_tile_cache_is_capped_by_bytes_and_the_cell_cache_by_count(product, monkeypatch):
    src = product.source()
    man = src.manifest()
    one = len(product.body(f"{PFC.PREFIX}/{RUN}/a/0_2.bin"))
    monkeypatch.setattr(PFC, "TILE_CACHE_BYTES", int(one * 2.5))
    monkeypatch.setattr(PFC, "CELL_CACHE_MAX", 3)
    for lon in (20.0, 30.0, 40.0, 50.0):                                        # four tiles: 0_2, 0_3, 0_5, 0_6
        src.series(man, src.locate(man, 5.0, lon))
    st = src.stats()
    assert st["tiles"] == 2 and st["tile_bytes"] <= one * 2.5 and st["cells"] == 3
    n = product.count("/a/0_6.bin")
    src.series(man, src.locate(man, 5.0, 51.0))                                 # the newest tile is still there
    assert product.count("/a/0_6.bin") == n
    src.series(man, src.locate(man, 5.0, 21.0))                                 # the oldest was dropped: fetched again
    assert product.count("/a/0_2.bin") == 2
    # least recently USED: 0_6 is asked for again (a hit), then a third tile comes: 0_2 goes, 0_6 stays
    src.series(man, src.locate(man, 5.0, 52.0))
    src.series(man, src.locate(man, 5.0, 60.0))                                 # tile 0_7
    n6 = product.count("/a/0_6.bin")
    src.series(man, src.locate(man, 5.0, 53.0))
    assert product.count("/a/0_6.bin") == n6
    src.series(man, src.locate(man, 5.0, 22.0))
    assert product.count("/a/0_2.bin") == 3


def test_two_requests_for_one_tile_fetch_it_once(product):
    src = product.source()
    man = src.manifest()
    cell_a, cell_b = src.locate(man, 5.0, 20.0), src.locate(man, 5.0, 22.0)     # two cells of one tile
    product.delay = 0.15
    out = []
    threads = [threading.Thread(target=lambda c=c: out.append(src.series(man, c)[0, 0])) for c in (cell_a, cell_b, cell_a)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(out) == sorted([code(GRIDS[0], "hs", 0, 5, 20), code(GRIDS[0], "hs", 0, 5, 22), code(GRIDS[0], "hs", 0, 5, 20)])
    assert product.count("/a/0_2.bin") == 1


# ------------------------------- tracking ---------------------------------------------

def table(hours, parts):
    return [[None if p is None else (p[0], p[1], p[2]) for p in row] for row in PFC.track_partitions(hours, parts)]


def test_a_train_keeps_its_column_while_noaa_reorders_by_height():
    hours = list(range(12))
    nw = [(1.0 + 0.1 * h, 16.0 - 0.1 * h, 320.0 + h, False) for h in hours]     # grows: 1.0 -> 2.1 m, period decays
    south = [(1.5, 14.0, 190.0, False)] * 12                                    # steady: NOAA lists the higher one first
    parts = [sorted([nw[h], south[h]], key=lambda p: -p[0]) for h in hours]
    assert [p[0][2] for p in parts][:3] == [190.0, 190.0, 190.0] and parts[-1][0][2] == 331.0      # the order flips at 1.5 m
    cols = table(hours, parts)
    assert all(row[2:] == [None] * 4 for row in cols)
    first, second = [row[0] for row in cols], [row[1] for row in cols]
    assert [p[2] for p in first] == [320.0 + h for h in hours]                  # one train per column, all the way
    assert [p[2] for p in second] == [190.0] * 12
    assert sum(p[0] ** 2 for p in first) > sum(p[0] ** 2 for p in second)       # column 1 = the more energetic over the week


def test_gates_period_direction_and_the_hours_since_seen():
    tr = {"tp": 14.0, "dir": 300.0, "hour": 0}
    assert PFC._match_cost((1.0, 14.0, 300.0, False), tr, 1) == 0.0
    assert PFC._match_cost((1.0, 16.0, 300.0, False), tr, 1) == pytest.approx(2.0 / 2.1)  # the gate: 15 % of 14 s
    assert PFC._match_cost((1.0, 16.2, 300.0, False), tr, 1) is None and PFC._match_cost((1.0, 11.8, 300.0, False), tr, 1) is None
    assert PFC._match_cost((1.0, 14.0, 330.0, False), tr, 1) == pytest.approx(1.0)       # 30 degrees
    assert PFC._match_cost((1.0, 14.0, 331.0, False), tr, 1) is None
    assert PFC._match_cost((1.0, 14.0, 269.0, False), tr, 1) is None
    assert PFC._match_cost((1.0, 14.0, 10.0, False), dict(tr, dir=350.0), 1) == pytest.approx(20 / 30)   # across north
    assert PFC._match_cost((1.0, 14.0, 345.0, False), tr, 3) is not None                 # 3 h: the gates x 1.73
    assert PFC._match_cost((1.0, 14.0, 353.0, False), tr, 3) is None
    assert PFC._match_cost((1.0, 14.0, 359.0, False), tr, 12) is not None                # x 2 at most
    assert PFC._match_cost((1.0, 14.0, 1.0, False), tr, 100) is None
    short = {"tp": 5.0, "dir": 60.0, "hour": 0}
    assert PFC._match_cost((0.5, 6.0, 60.0, False), short, 1) == pytest.approx(1.0)      # at least 1 s
    assert PFC._match_cost((0.5, 5.0, 104.0, False), short, 1) is not None               # short periods: 45 degrees
    assert PFC._match_cost((0.5, 5.0, 106.0, False), short, 1) is None
    wind = dict(short, wind=True)                                                        # the wind sea is the wind sea
    assert PFC._match_cost((0.5, 3.1, 100.0, True), wind, 1) is not None
    assert PFC._match_cost((0.5, 3.1, 100.0, False), wind, 1) is None                    # re-labelled as swell: the ordinary gates
    assert PFC._match_cost((0.5, 3.1, 160.0, True), wind, 1) is None                     # not through more than a quarter turn
    assert (PFC.DIE_H, PFC.USED, PFC.NCOL, PFC.WEEK_H) == (12, 4, 6, 168)


def test_the_wind_sea_stays_in_its_column_through_a_jump_of_its_period():
    hours = list(range(8))
    swell = (1.2, 12.0, 320.0, False)
    seas = [(0.6, 5.9, 60.0, True), (0.6, 5.9, 62.0, True), (0.6, 3.1, 60.0, True), (0.6, 3.2, 61.0, True),
            (0.5, 7.0, 60.0, True), (0.5, 6.9, 60.0, True), (0.7, 4.4, 61.0, True), (0.9, 5.5, 53.0, True)]
    cols = table(hours, [[seas[h], swell] for h in hours])
    assert [row[1][1] for row in cols] == [s[1] for s in seas] and all(row[0] == swell[:3] for row in cols)
    assert all(row[2:] == [None] * 4 for row in cols)


def test_a_train_that_pauses_returns_to_its_column_within_twelve_hours_only():
    a, b = (1.5, 14.0, 300.0, False), (0.8, 9.0, 200.0, False)
    hours = list(range(0, 40))
    parts = [[a, b] if h < 5 or 10 <= h < 15 or h >= 30 else [a] for h in hours]     # b pauses for 5 h, then for 15 h
    cols = table(hours, parts)
    col_b = next(c for c in range(6) if cols[0][c] == b[:3])
    assert all(cols[h][col_b] == b[:3] for h in range(10, 15))                        # back in its own column after 5 h
    col_a = next(c for c in range(6) if cols[0][c] == a[:3])
    col_b2 = next(c for c in range(6) if cols[30][c] == b[:3])                        # after 15 h it is a new train: it takes
    assert col_b2 not in (col_a, col_b)                                               # a column that was never used
    assert all(cols[h][col_b2] == b[:3] for h in range(30, 40)) and all(cols[h][col_b] is None for h in range(15, 40))


def test_twelve_hours_unseen_is_still_the_train_thirteen_is_a_new_one():
    a, b = (1.5, 14.0, 300.0, False), (0.8, 9.0, 200.0, False)
    for back, same in ((26, True), (27, False)):                                      # last seen at hour 14
        hours = list(range(0, 32))
        cols = table(hours, [[a, b] if h <= 14 or h >= back else [a] for h in hours])
        first = next(c for c in range(6) if cols[0][c] == b[:3])
        again = next(c for c in range(6) if cols[back][c] == b[:3])
        assert (again == first) is same, back


def test_a_train_is_followed_from_where_it_was_last_seen_not_from_where_it_began():
    hours = list(range(0, 80))
    parts = [[(1.0, 18.0 - 0.1 * h, (300.0 + 0.5 * h) % 360, False)] for h in hours]   # 18 s -> 10.1 s, 300 -> 339.5 deg
    cols = table(hours, parts)
    assert all(row[0] is not None and row[1:] == [None] * 5 for row in cols)           # one train, one column, all the way


def test_two_trains_close_together_are_paired_by_the_least_change():
    hours = list(range(0, 10))
    one = [(1.0 + 0.2 * h, 12.0, 300.0, False) for h in hours]                         # grows past the other at hour 3
    two = [(1.5, 13.0, 312.0, False)] * 10                                            # within each other's gates
    parts = [sorted([one[h], two[h]], key=lambda p: -p[0]) for h in hours]            # NOAA's order: by height
    assert parts[0][0][1] == 13.0 and parts[-1][0][1] == 12.0
    cols = table(hours, parts)
    assert len({row[0][1] for row in cols}) == 1 and len({row[1][1] for row in cols}) == 1   # each column keeps its period


def test_a_new_train_takes_the_column_that_has_been_free_the_longest():
    a = (2.0, 16.0, 300.0, False)
    b, c, d, e = (0.8, 9.0, 200.0, False), (0.7, 12.0, 100.0, False), (0.6, 6.0, 40.0, False), (0.5, 20.0, 250.0, False)
    hours = list(range(0, 60))
    parts = []
    for h in hours:
        step = [a]
        if h <= 4:
            step.append(b)                                                            # b ends first,
        if h <= 9:
            step.append(c)                                                            # then c
        if h >= 30:
            step.append(d)                                                            # d arrives: a column never used
        if h >= 40:
            step.append(e)                                                            # e arrives: b's column (free since hour 4)
        parts.append(step)
    cols = table(hours, parts)
    col = lambda p, h: next(k for k in range(6) if cols[h][k] == p[:3])               # noqa: E731
    assert len({col(a, 0), col(b, 0), col(c, 0), col(d, 30)}) == 4
    assert col(e, 40) == col(b, 0) and col(e, 59) == col(b, 0)


def test_never_more_than_four_columns_and_nothing_is_lost():
    rng = np.random.default_rng(5)
    hours = list(range(0, 121)) + list(range(123, 385, 3))
    parts = []
    for h in hours:
        n = int(rng.integers(0, 5))
        parts.append([(float(rng.uniform(0.1, 3)), float(rng.uniform(3, 20)), float(rng.uniform(0, 360)), i == 0 and n > 1) for i in range(n)])
    cols = PFC.track_partitions(hours, parts)
    assert len(cols) == len(hours) and all(len(row) == PFC.NCOL for row in cols)
    for row, step in zip(cols, parts):
        assert row[4] is None and row[5] is None
        assert sorted(p for p in row if p is not None) == sorted(step)                # every partition, once
    assert PFC.track_partitions([], []) == [] and PFC.track_partitions([0], [[]]) == [[None] * 6]


def test_columns_are_ordered_by_energy_over_the_first_week():
    small, late = (0.5, 10.0, 200.0, False), (3.0, 15.0, 300.0, False)
    hours = list(range(0, 121)) + list(range(123, 385, 3))
    parts = [[small] if h <= 168 else [small, late] for h in hours]                   # the big one only after day 7
    cols = table(hours, parts)
    assert cols[0][0] == small[:3] and cols[-1][1] == late[:3]                        # the first week decides
    parts = [[small, late] if 100 <= h <= 168 else [small] for h in hours]            # three days of the big one inside the week
    cols = table(hours, parts)
    assert cols[0][1] == small[:3] and cols[hours.index(110)][0] == late[:3]
    parts = [[small, (0.5, 15.0, 300.0, False)] for h in hours]                       # equal energy: the earlier column first
    assert table(hours, parts)[0][:2] == [small[:3], (0.5, 15.0, 300.0)]
    # a 3-hourly step stands for three hours: 16 steps of 1.2 m (123..168 h) outweigh 60 hourly steps of 1.0 m
    x, y = (1.0, 10.0, 200.0, False), (1.2, 15.0, 300.0, False)
    parts = [[x] if h < 60 else [y] if 123 <= h <= 168 else [] for h in hours]
    cols = table(hours, parts)
    assert cols[hours.index(150)][0] == y[:3] and cols[0][1] == x[:3]


def test_partitions_at_needs_all_three_values():
    codes = [[PF.MISSING] * 2 for _ in PF.FIELD_NAMES]
    ix = {n: i for i, n in enumerate(PF.FIELD_NAMES)}
    for n, v in (("ws_h", 74), ("ws_t", 58), ("ws_d", 47), ("s1_h", 72), ("s1_t", 110), ("s1_d", 324), ("s2_h", 19), ("s2_t", 163),
                 ("s3_h", 18), ("s3_t", 70), ("s3_d", 0)):
        codes[ix[n]][0] = v
    assert PFC.partitions_at(codes, 0) == [(0.74, 5.8, 47.0, True), (0.72, 11.0, 324.0, False), (0.18, 7.0, 0.0, False)]   # s2 has no direction
    assert PFC.partitions_at(codes, 1) == []
    codes[ix["s1_t"]][0] = 0                                                           # a period of zero is not a wave
    assert [p[1] for p in PFC.partitions_at(codes, 0)] == [5.8, 7.0]


# ------------------------------- rows ---------------------------------------------------

def test_point_rows_have_the_bulletin_parsers_shape():
    fx = json.load(open(os.path.join(FIX, "point_51201_2026100112.json")))
    codes, steps = np.array(fx["codes"], dtype=np.uint16), fx["steps"]
    rows = PFC.point_rows(codes, steps, datetime(2026, 10, 1, 12), pytz.timezone("Pacific/Honolulu"))
    assert len(rows) == 209 and all(len(r) == 23 for r in rows)
    assert rows[0][:2] == ["Thursday, October 1, 2026", "2:00 AM"] and rows[1][1] == "3:00 AM"
    assert rows[120][:2] == ["Tuesday, October 6, 2026", "2:00 AM"] and rows[121][1] == "5:00 AM"     # hourly, then 3-hourly
    assert rows[-1][:2] == ["Saturday, October 17, 2026", "2:00 AM"]
    assert all(A._row_datetime(r) is not None for r in rows)                           # the page's own reader of the date
    ix = {n: i for i, n in enumerate(PF.FIELD_NAMES)}
    assert rows[0][22] == round(codes[ix["hs"]][0] / 100 * 3.28084, 2) == 2.72         # 0.83 m combined
    assert rows[0][20] == codes[ix["wind"]][0] / 10 and rows[0][21] == int(codes[ix["wdir"]][0])
    assert isinstance(rows[0][20], float) and isinstance(rows[0][21], int)
    groups = [rows[0][2 + 3 * g:5 + 3 * g] for g in range(6)]
    assert groups[4] == groups[5] == [None, None, None]
    got = sorted(tuple(g) for g in groups if g[0] is not None)
    want = sorted((round(p[0] * 3.28084, 2), round(p[1], 1), int(p[2])) for p in PFC.partitions_at(codes, 0))
    assert got == want and all(isinstance(g[2], int) and 0 <= g[2] < 360 for g in got)
    assert A._swell_groups(rows) == [0, 1, 2, 3] and A._swell_groups(rows, days=7) == [0, 1, 2, 3]
    ny = PFC.point_rows(codes, steps, datetime(2026, 10, 1, 12), pytz.timezone("America/New_York"))
    assert ny[0][:2] == ["Thursday, October 1, 2026", "8:00 AM"] and [r[2:] for r in ny] == [r[2:] for r in rows]
    blank = codes.copy()
    blank[ix["wind"], 5] = PF.MISSING
    blank[ix["hs"], 6] = PF.MISSING
    blank[ix["wdir"], 7] = PF.MISSING
    rows2 = PFC.point_rows(blank, steps, datetime(2026, 10, 1, 12), pytz.utc)
    assert rows2[5][20:22] == [None, None] and rows2[6][22] is None and rows2[7][21] is None and rows2[7][20] is not None
    assert PFC.point_headers(RUN, 21.667, -158.054, fx["cell"]) == (
        "Cycle : 20261001 12 UTC", "Location : 21.667N 158.054W (model cell 21.67N 158.17W, 5 km away)")


def _bulletin(text, cycle_hour=12):
    """rows[i] = (combined m, [(hs m, tp s, dir FROM), ...]) for hour i (bulletin directions are where the waves go TO)."""
    rows = []
    for line in text.splitlines():
        cells = line.split("|")
        if len(cells) < 9 or not re.fullmatch(r"\s*\d+\s+\d+\s*", cells[1]):
            continue
        assert int(cells[1].split()[1]) == (cycle_hour + len(rows)) % 24
        parts = []
        for c in cells[3:9]:
            c = c.replace("*", " ").split()
            if len(c) == 3:
                parts.append((float(c[0]), float(c[1]), (float(c[2]) + 180) % 360))
        rows.append((float(cells[2].split()[0]), parts))
    return rows


def test_the_real_cell_agrees_with_noaas_bulletin_and_the_tracking_is_smoother_than_noaas_order():
    """NOAA station 51201 (Waimea Bay) and the model cell 4.8 km from it, the same run. The bulletin comes from
    the model's spectrum at the buoy, the product from the gridded partitions of the cell: the same waves."""
    fx = json.load(open(os.path.join(FIX, "point_51201_2026100112.json")))
    codes, steps = fx["codes"], fx["steps"]
    bull = _bulletin(open(os.path.join(FIX, "gfswave_51201_2026100112.bull"), encoding="latin-1").read())
    assert len(bull) == 385 and fx["cell"]["km"] < 5
    parts = [PFC.partitions_at(codes, si) for si in range(len(steps))]
    dd = lambda a, b: abs((a - b + 180) % 360 - 180)                          # noqa: E731
    hs_err, n, hit, dt, dh, late_n, late_hit = [], 0, 0, 0.0, 0.0, 0, 0
    for si, hour in enumerate(steps):
        hst, bparts = bull[hour]
        hs_err.append(abs(codes[0][si] / 100 - hst))
        for bp in (p for p in bparts if p[0] >= 0.3):
            n += 1
            late_n += hour > 120
            near = [p for p in parts[si] if abs(p[1] - bp[1]) <= 1.5 and dd(p[2], bp[2]) <= 30]
            if near:
                p = min(near, key=lambda p: abs(p[1] - bp[1]))
                hit += 1
                late_hit += hour > 120
                dt += abs(p[1] - bp[1])
                dh += abs(p[0] - bp[0])
    assert max(hs_err) <= 0.05 and sum(hs_err) / len(hs_err) < 0.02                    # the combined height, every step
    assert n > 500 and hit / n > 0.95 and late_hit / late_n > 0.9                      # the partitions, also past +120 h
    assert dt / hit < 0.05 and dh / hit < 0.02                                         # same peak period, same height
    cols = PFC.track_partitions(steps, parts)

    def jumps(tab, ncol):
        t = d = k = 0
        for a, b in zip(tab, tab[1:]):
            for c in range(ncol):
                if c < len(a) and c < len(b) and a[c] is not None and b[c] is not None:
                    t += abs(a[c][1] - b[c][1])
                    d += dd(a[c][2], b[c][2])
                    k += 1
        return t / k, d / k
    tracked, raw = jumps(cols, 6), jumps(parts, 4)
    assert tracked[0] < raw[0] / 2 and tracked[1] < raw[1] / 2                         # period and direction jumps per step: halved
    assert all(row[4] is None and row[5] is None for row in cols)


# ------------------------------- the service ------------------------------------------

@pytest.fixture
def api(product, monkeypatch):
    monkeypatch.setattr(A, "POINTS", product.source())
    monkeypatch.setattr(A, "_POINT_CACHE", {})
    monkeypatch.setattr(A, "_buoy_tz_cached", lambda lat, lon: "Pacific/Honolulu")
    client = A.app.test_client()

    def get(station, **params):
        q = "&".join(f"{k}={v}" for k, v in dict(station=station, **params).items())
        return client.get("/api/forecast?" + q)
    return product, get


def test_api_forecast_for_a_point(api):
    product, get = api
    pid = PFC.point_id(5.2, 20.3)
    r = get(pid, compact=1)
    assert r.status_code == 200 and r.headers["Cache-Control"].startswith("private")
    d = r.get_json()
    assert d["error"] is None and d["station"] == pid and d["model"] == "GFS" and d["swan_available"] is False
    assert (d["lat"], d["lon"], d["tz_label"], d["wind_complete"]) == (5.2, 20.3, "Pacific/Honolulu", True)
    p = d["point"]
    assert {k: p[k] for k in ("id", "lat", "lon", "cell_lat", "cell_lon", "grid", "run", "run_utc", "published_utc")} == {
        "id": pid, "lat": 5.2, "lon": 20.3, "cell_lat": 5.0, "cell_lon": 20.0, "grid": "a", "run": RUN,
        "run_utc": "2026-10-01T12:00:00Z", "published_utc": "2026-10-01T17:40:00Z"}
    assert 20 < p["cell_km"] < 45 and p["age_hours"] > 0
    assert d["graph_header"] == {"cycle": "20261001 12 UTC", "tz": "Pacific/Honolulu",
                                 "location": f"5.200N 20.300E (model cell 5.00N 20.00E, {p['cell_km']:.0f} km away)"}
    g = d["graph_data"]
    assert len(g["labels"]) == len(STEPS) and g["labels"][0] == "Thursday, October 1, 2026 2:00 AM" and g["units"] == "ft"
    assert g["swells"] == ["s1", "s2", "s3", "s4"] and g["cycle"] == "Cycle : 20261001 12 UTC"
    assert g["height"]["combined"][0] == round(code(GRIDS[0], "hs", 0, 5, 20) / 100 * 3.28084, 2)
    assert g["height"]["s5"] == [None] * len(STEPS)
    html = d["table_html"]
    assert html.count("<tr>") == 2 + len(STEPS) and [f"Swell {n}" in html for n in range(1, 7)] == [True] * 4 + [False] * 2
    m = get(pid, compact=1, unit="Metric").get_json()
    assert m["graph_data"]["units"] == "m" and m["graph_data"]["height"]["combined"][0] == round(code(GRIDS[0], "hs", 0, 5, 20) / 100, 2)
    assert len(product.calls) == 4                                             # pointer, manifest, one mask, one tile: once
    ny = get(pid, tz="America/New_York").get_json()
    assert ny["tz_label"] == "America/New_York" and ny["graph_data"]["labels"][0] == "Thursday, October 1, 2026 8:00 AM"
    assert get(pid, tz="Not/AZone").get_json()["tz_label"] == "Pacific/Honolulu"
    assert len(product.calls) == 4 and len(A._POINT_CACHE) == 2


def test_api_forecast_refuses_land_bad_ids_and_reports_an_unreachable_bucket(api):
    product, get = api
    d = get(PFC.point_id(5.0, 104.0)).get_json()
    assert d["error"] == A.POINT_NO_DATA == "No model data here: land, ice or outside coverage"
    assert d["final"] is True                                                     # the page offers no Retry for it
    assert d["table_html"] is None and d["point"] is None and d["graph_data"] is None and "busy" not in d
    assert get(PFC.point_id(60.0, 20.0)).get_json()["error"] == A.POINT_NO_DATA   # outside every grid
    assert len(A._POINT_CACHE) == 0                                               # a refusal is not kept
    for bad in ("pt_05200N_20300E", "pt_5200N", "pt_0S_0E"):
        d = get(bad).get_json()
        assert d["error"] == "Invalid forecast point" and d["point"] is None and d["final"] is True
    assert get("pt_5200N_20300E!").get_json()["error"] == "Invalid station id"
    n = len(product.calls)
    product.fail = lambda key: key.endswith("_2.bin")
    d = get(PFC.point_id(5.2, 20.3)).get_json()
    assert d["error"] == "Forecast points are temporarily unavailable" and d["table_html"] is None and "final" not in d
    product.fail = None
    assert get(PFC.point_id(5.2, 20.3)).get_json()["error"] is None and len(product.calls) == n + 2


def test_points_are_off_without_a_bucket_address(monkeypatch):
    monkeypatch.setattr(A, "POINTS", PFC.PointSource(A._points_root, lambda url, n: (_ for _ in ()).throw(AssertionError("no fetch"))))
    monkeypatch.delenv("POINTS_ROOT", raising=False)
    monkeypatch.delenv("MODEL_FRAMES_BASE", raising=False)
    assert A._points_root() == ""
    d = A.app.test_client().get("/api/forecast?station=pt_5200N_20300E").get_json()
    assert d["error"] == "Forecast points are not available on this server" and d["table_html"] is None
    monkeypatch.setenv("MODEL_FRAMES_BASE", "https://models.allshoresurf.com/gfswave/0p25/v1/")
    assert A._points_root() == "https://models.allshoresurf.com"
    monkeypatch.setenv("MODEL_FRAMES_BASE", "https://pub-x.r2.dev/other/prefix")
    assert A._points_root() == ""                                                 # not a layout this code knows
    monkeypatch.setenv("POINTS_ROOT", "https://example.org/")
    assert A._points_root() == "https://example.org"


def test_points_have_their_own_cache_and_never_touch_the_stations(api, monkeypatch):
    product, get = api
    monkeypatch.setattr(A, "_FORECAST_CACHE", {("51201", "", "GFS"): {"ts": time.time(), "data": "kept"}})
    monkeypatch.setattr(A, "_POINT_CACHE_MAX", 3)
    for lon in (20.0, 21.0, 22.0, 23.0, 24.0):
        assert get(PFC.point_id(5.0, lon)).get_json()["error"] is None
    assert len(A._POINT_CACHE) == 3 and {k[1] for k in A._POINT_CACHE} == {PFC.point_id(5.0, lon) for lon in (22.0, 23.0, 24.0)}
    assert A._FORECAST_CACHE == {("51201", "", "GFS"): A._FORECAST_CACHE[("51201", "", "GFS")]} and len(A._FORECAST_CACHE) == 1
    assert all(k[0] == RUN for k in A._POINT_CACHE)                               # the run is part of the key
    assert A._POINT_CACHE_TTL == 6 * 3600 and A._POINT_BUILD_WAIT_S == 2.0
    monkeypatch.undo()
    assert A._POINT_CACHE_MAX == 64


def test_a_new_run_drops_the_old_runs_rows(api, monkeypatch):
    product, get = api
    now = [time.time()]
    monkeypatch.setattr(A, "POINTS", product.source(clock=lambda: now[0]))
    for lon in (20.0, 21.0):
        assert get(PFC.point_id(5.0, lon)).get_json()["point"]["run"] == RUN
    newer = Product(run="2026100118")
    product.run, product.mkey = newer.run, newer.mkey
    assert get(PFC.point_id(5.0, 20.0)).get_json()["point"]["run"] == RUN          # the pointer is read every five minutes
    now[0] += PFC.POINTER_TTL_S + 1
    d = get(PFC.point_id(5.0, 20.0)).get_json()
    assert d["point"]["run"] == "2026100118" and d["graph_header"]["cycle"] == "20261001 18 UTC"
    assert {k[0] for k in A._POINT_CACHE} == {"2026100118"} and len(A._POINT_CACHE) == 1


def test_a_second_request_for_a_point_still_being_built_does_not_wait_long(api, monkeypatch):
    product, get = api
    get(PFC.point_id(5.0, 20.0))
    monkeypatch.setattr(A, "_POINT_BUILD_WAIT_S", 0.05)
    product.delay = 0.5
    first = {}
    t = threading.Thread(target=lambda: first.update(get(PFC.point_id(5.0, 30.0)).get_json()))
    t.start()
    time.sleep(0.15)
    t0 = time.time()
    second = get(PFC.point_id(5.0, 30.0)).get_json()                              # the same point, its build still running
    assert second["error"] == A.POINT_BUSY and second["busy"] is True and time.time() - t0 < 0.3
    t.join()
    product.delay = 0.0
    assert first["error"] is None and get(PFC.point_id(5.0, 30.0)).get_json()["error"] is None
    assert A._POINT_INFLIGHT == {}


def test_nobody_queues_behind_a_slow_read_of_the_pointer(product, monkeypatch):
    monkeypatch.setattr(PFC, "POINTER_WAIT_S", 0.05)
    now = [1000.0]
    src = product.source(clock=lambda: now[0])
    product.delay = 0.4
    out = []
    t = threading.Thread(target=lambda: out.append(src.manifest()["run"]))
    t.start()
    time.sleep(0.1)
    t0 = time.time()
    with pytest.raises(PFC.PointError, match="temporarily unavailable"):          # no manifest yet: told so at once
        src.manifest()
    assert time.time() - t0 < 0.3
    t.join()
    assert out == [RUN]
    man = src.manifest()
    now[0] += PFC.POINTER_TTL_S + 1                                               # with one in hand: the old one, at once
    t = threading.Thread(target=src.manifest)
    t.start()
    time.sleep(0.1)
    t0 = time.time()
    assert src.manifest() is man and time.time() - t0 < 0.3
    t.join()


def test_two_point_builds_at_once_and_the_third_is_told_to_come_back(api, monkeypatch):
    product, get = api
    monkeypatch.setattr(A, "_POINT_BUILD_WAIT_S", 0.05)
    assert get(PFC.point_id(5.0, 20.0)).get_json()["error"] is None               # warm: the manifest and the mask are in
    product.delay = 0.4
    out = {}

    def ask(lon):
        out[lon] = get(PFC.point_id(5.0, lon)).get_json()
    threads = [threading.Thread(target=ask, args=(lon,)) for lon in (30.0, 40.0)]  # two tiles not yet fetched
    for t in threads:
        t.start()
    time.sleep(0.15)
    busy = get(PFC.point_id(5.0, 50.0)).get_json()                                # a third build while two are running
    assert busy["error"] == A.POINT_BUSY and busy["busy"] is True and busy["table_html"] is None and "final" not in busy
    cached = get(PFC.point_id(5.0, 20.0)).get_json()                              # a kept forecast is served all the same
    assert cached["error"] is None and "busy" not in cached
    for t in threads:
        t.join()
    assert out[30.0]["error"] is None and out[40.0]["error"] is None
    product.delay = 0.0
    assert get(PFC.point_id(5.0, 50.0)).get_json()["error"] is None               # nothing was kept of the refusal
    assert A._POINT_BUILDS.acquire(blocking=False) and A._POINT_BUILDS.acquire(blocking=False)   # both slots are free again
    assert not A._POINT_BUILDS.acquire(blocking=False)
    A._POINT_BUILDS.release()
    A._POINT_BUILDS.release()


def test_the_same_point_asked_twice_at_once_is_built_once(api):
    product, get = api
    get(PFC.point_id(5.0, 20.0))
    product.delay = 0.2
    n = len(product.calls)
    out = []
    threads = [threading.Thread(target=lambda: out.append(get(PFC.point_id(5.0, 30.0)).get_json()["error"])) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert out == [None, None, None] and len(product.calls) == n + 1              # one tile fetch for the three


def test_a_failed_build_gives_its_slot_back(api, monkeypatch):
    product, get = api
    monkeypatch.setattr(A.point_forecast, "point_rows", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    for _ in range(3):
        d = get(PFC.point_id(5.0, 20.0)).get_json()
        assert d["error"] == "Forecast temporarily unavailable"                    # the route's own answer to an exception
    assert A._POINT_BUILDS.acquire(blocking=False) and A._POINT_BUILDS.acquire(blocking=False)
    A._POINT_BUILDS.release()
    A._POINT_BUILDS.release()
    assert A._POINT_INFLIGHT == {}


def test_station_forecasts_are_untouched(monkeypatch):
    rows = [["Thursday, October 1, 2026", "2:00 AM"] + [1.0, 10.0, 300] + [None] * 15 + [5.0, 60, 1.2]]
    monkeypatch.setattr(A, "parse_bull", lambda station, tz: ("Cycle : 20261001 12 UTC", "Location : 51201 (21.67N 158.12W)", None, rows, "Pacific/Honolulu", None))
    d = A.compute_forecast_payload("51201", None, "US")
    assert sorted(d) == ["error", "graph_data", "graph_header", "lat", "lon", "model", "station", "swan_available", "table_html", "tz_label", "wind_complete"]
    assert d["graph_header"]["location"] == "51201 (21.67N 158.12W)"


def test_the_overlays_zone_for_a_point_is_the_points_civil_zone(monkeypatch):
    seen = []
    monkeypatch.setattr(A, "_buoy_tz_cached", lambda lat, lon: seen.append((lat, lon)) or "Pacific/Pago_Pago")
    assert A._effective_tz_name("pt_14400S_170700W", None) == "Pacific/Pago_Pago" and seen == [(-14.4, -170.7)]
    assert A._effective_tz_name("pt_14400S_170700W", "America/New_York") == "America/New_York"
    assert A._effective_tz_name("pt_014400S_170700W", None) == "UTC"               # not a point id: the old rule


def test_the_page_renders_for_a_point_without_javascript(api):
    product, _get = api
    pid = PFC.point_id(5.2, 20.3)
    html = A.app.test_client().get(f"/?station={pid}&render=full").get_data(as_text=True)
    assert "Swell 1" in html and "5.200N 20.300E (model cell 5.00N 20.00E" in html
    shell = A.app.test_client().get(f"/?station={pid}").get_data(as_text=True)
    assert f'"station": "{pid}"' in shell or f'"station":"{pid}"' in shell


def test_points_fetch_caps_the_body_and_refuses_other_statuses(monkeypatch):
    class Resp:
        def __init__(self, status, chunks):
            self.status_code, self.chunks = status, chunks

        def iter_content(self, n):
            return iter(self.chunks)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False
    seen = {}

    def get(url, timeout=None, stream=False):
        seen.update(url=url, timeout=timeout, stream=stream)
        return seen["resp"]
    monkeypatch.setattr(A.HTTP, "get", get)
    seen["resp"] = Resp(200, [b"ab", b"cd"])
    assert A._points_fetch("https://bucket/x", 4) == b"abcd" and seen["stream"] is True and seen["timeout"] == (4, 10)
    seen["resp"] = Resp(200, [b"ab", b"cde"])
    with pytest.raises(IOError, match="too large"):
        A._points_fetch("https://bucket/x", 4)
    for status in (404, 403, 500, 206):
        seen["resp"] = Resp(status, [b"x"])
        with pytest.raises(IOError, match=str(status)):
            A._points_fetch("https://bucket/x", 4)


def test_importing_the_app_does_not_load_numpy():
    """The web process pays for numpy (about 25 MB) only when somebody asks for a point."""
    r = subprocess.run([sys.executable, "-c", "import sys, app; print('numpy' in sys.modules, 'pointfmt' in sys.modules)"],
                       cwd=ROOT, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0 and r.stdout.strip().splitlines()[-1] == "False False", (r.stdout, r.stderr[-500:])


def test_the_pages_point_ids_are_the_servers():
    """tests/fixtures/point_ids.json: point_id for 522 inputs (edges, random, halves of a thousandth). The page's
    pointId (static_ui/forecast.js) is checked against the same file by tests/ui/points.test.js."""
    fx = json.load(open(os.path.join(FIX, "point_ids.json")))
    assert len(fx["cases"]) > 500
    assert [PFC.point_id(la, lo) for la, lo, _ in fx["cases"]] == [i for _, _, i in fx["cases"]]
