"""Forecast points (plan section 31): point ids, the reader of the points product, the coast (land, and what lies
between a point and its cell), the rank of a row's swells, the rows, and the /api/forecast path. No network: a
small product is built with the format's own writer (tools/model_frames/pointfmt.py), a small coast with the coast
builder's (tools/coast/build_coast.py), both served by a fake fetch; one real cell (NOAA station 51201's model cell,
run 2026100112), its NOAA bulletin and the real coast of Oahu are fixtures.

Run:  pytest tests/
"""
import json
import math
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

sys.path.insert(0, os.path.join(ROOT, "tools", "coast"))

import app as A                    # noqa: E402
import build_coast as BC           # noqa: E402
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


LAND = ((100.0, 110.0, -10.0, 30.0),)                        # the coast's land: (west, east, south, north) boxes


def coast_cells(boxes):
    """{cell name: coast-v1 bytes} of land boxes, cut at the 5-degree cell lines as the builder cuts polygons."""
    pieces = {}
    for w, e, lo, hi in boxes:
        for la in range(int(np.floor(lo / 5)) * 5, int(np.ceil(hi / 5)) * 5, 5):
            for ln in range(int(np.floor(w / 5)) * 5, int(np.ceil(e / 5)) * 5, 5):
                x0, x1, y0, y1 = max(w, ln), min(e, ln + 5), max(lo, la), min(hi, la + 5)
                if x1 > x0 and y1 > y0:
                    q = [int(round(v * 10000)) for v in (x0, x1, y0, y1)]
                    ring = (np.array([q[0], q[1], q[1], q[0]], dtype=np.int64), np.array([q[2], q[2], q[3], q[3]], dtype=np.int64))
                    pieces.setdefault(f"{la}_{ln}", []).append([ring])
    return {name: BC.encode_file(ps, 5) for name, ps in pieces.items()}


class Product:
    """latest.json, a manifest, masks and tiles of one run, and the coast, built on demand; every fetch is recorded."""

    def __init__(self, run=RUN, steps=STEPS, grids=GRIDS, land=LAND):
        self.run, self.steps, self.grids = run, list(steps), grids
        self.coast = coast_cells(land)
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
        if key == f"{PFC.COAST_PREFIX}/index.json":
            return json.dumps({"format": "coast-v1", "q": 10000, "tier1": {"cell": 5, "dir": "f", "cells": {
                n: [len(b), 4] for n, b in self.coast.items()}}}).encode()
        m = re.fullmatch(rf"{PFC.COAST_PREFIX}/f/(-?\d+_-?\d+)\.bin", key)
        if m:
            if m.group(1) not in self.coast:
                raise IOError("HTTP 404")
            return self.coast[m.group(1)]
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


def loc(src, man, lat, lon):
    """The point's cell (None when it has none: locate()'s reason is asked for where it matters)."""
    return src.locate(man, lat, lon)[0]


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
    assert len(ids) > 700 and not any(PFC.is_point_id(s) for s in ids)


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
    cell = loc(src, man, 5.2, 20.3)
    assert (cell["grid"], cell["row"], cell["col"]) == ("a", 5, 20)
    assert product.count("/b/") == 0                                            # grid b is out of reach: its mask is not fetched
    codes = src.series(man, cell)
    assert codes.shape == (15, len(STEPS)) and codes.dtype == np.dtype("<u2")
    for fi, name in enumerate(PF.FIELD_NAMES):
        assert codes[fi].tolist() == [code(GRIDS[0], name, si, 5, 20) for si in range(len(STEPS))], name
    again = src.series(man, loc(src, src.manifest(), 5.2, 20.3))
    assert (again == codes).all()
    other = src.series(man, loc(src, man, 4.9, 22.1))                         # a neighbour in the same tile
    assert other[0, 0] == code(GRIDS[0], "hs", 0, 5, 22)
    assert [product.count(k) for k in ("latest.json", "manifest-", "/a/mask.bin", "/a/0_2.bin", "coast/v1/index.json")] == [1, 1, 1, 1, 1]
    assert len(product.calls) == 5                             # no coast cell: the index says these waters have no land
    assert {k: src.stats()[k] for k in ("masks", "tiles", "cells", "coast")} == {"masks": 1, "tiles": 1, "cells": 2, "coast": 0}
    b = loc(src, man, 20.2, 50.2)
    assert src.series(man, b)[0, 3] == code(GRIDS[1], "hs", 3, 20, 100) and product.count("/b/1_6.bin") == 1


def test_manifest_is_read_again_after_five_minutes_and_a_new_run_drops_the_old(product):
    now = [1000.0]
    src = product.source(clock=lambda: now[0])
    man = src.manifest()
    src.series(man, loc(src, man, 5.2, 20.3))
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
    assert src.stats()["tile_bytes"] == 0                                       # the bytes are given back with the tiles (S52)
    cell = loc(src, man2, 5.2, 20.3)
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


def test_pins_from_the_g22_mutation_run(product):
    """Reviewer A's survivors that could hide a defect (G22 A-8), each pinned."""
    assert (PFC.POINTER_TTL_S, PFC.POINTER_RETRY_S, PFC.POINTER_KEEP_S, PFC.POINTER_WAIT_S) == (300, 60, 21600, 2.0)   # S39-S41
    # S49: the six hours count from the last GOOD read, however often a read fails in between
    now = [1000.0]
    src = product.source(clock=lambda: now[0])
    man = src.manifest()
    product.fail = lambda key: "latest.json" in key
    served = 0
    for _ in range(int(PFC.POINTER_KEEP_S / 61) + 20):
        now[0] += 61
        try:
            assert src.manifest() is man
            served += 1
        except PFC.PointError:
            break
    assert now[0] - 1000.0 > PFC.POINTER_KEEP_S and served * 61 <= PFC.POINTER_KEEP_S + 61
    with pytest.raises(PFC.PointError):
        src.manifest()
    product.fail = None
    # S13: a manifest that does not say it is complete is not read
    bad = product.manifest()
    del bad["complete"]
    with pytest.raises(PFC.PointError):
        PFC.check_manifest(bad)
    # S63: the mask says sea, the tile does not hold the cell: an error, never another cell's values
    p = Product()
    grid = GRIDS[0]
    mask = sea(grid)
    assert not mask[2, 4]
    mask[2, 4] = True                                                           # (8 N, 4 E) is land in the tiles
    p.overrides[f"{PFC.PREFIX}/{RUN}/a/mask.bin"] = PF.encode_mask(RUN, grid, mask)
    src = p.source()
    man = src.manifest()
    cell = loc(src, man, 8.0, 4.0)
    assert (cell["row"], cell["col"]) == (2, 4)
    with pytest.raises(PFC.PointError):
        src.series(man, cell)
    # S62: a tile of ANOTHER grid under this grid's name is refused
    p = Product()
    p.overrides[f"{PFC.PREFIX}/{RUN}/a/0_2.bin"] = p.body(f"{PFC.PREFIX}/{RUN}/b/0_2.bin")
    src = p.source()
    man = src.manifest()
    with pytest.raises(PFC.PointError):
        src.series(man, loc(src, man, 5.2, 20.3))
    # S64: a kept cell owns its values (not a view that keeps the whole decoded tile alive)
    src = product.source()
    man = src.manifest()
    codes = src.series(man, loc(src, man, 5.2, 20.3))
    assert codes.base is None and codes.nbytes == 15 * len(STEPS) * 2
    # S31 / S32: the longitude window widens with the latitude (at 80 N a degree of longitude is 19 km)
    hi = {"name": "h", "ni": 1440, "nj": 41, "lat0": 85.0, "per_deg": 4, "tile": 16, "data_rows": (0, 40)}
    m = np.zeros((41, 1440), dtype=bool)
    m[20, 47] = True                                                            # 80 N, 11.75 E
    cell = PFC.nearest_sea_cell([hi], {"h": m}, 80.0, 10.0)
    assert cell is not None and (cell["row"], cell["col"]) == (20, 47) and 33 < cell["km"] < 35
    assert PFC.nearest_sea_cell([hi], {"h": m}, 80.0, 9.5) is None              # 43 km
    assert [c["col"] for c in PFC.sea_cells_in_reach([hi], {"h": m}, 80.0, 10.0)] == [47]


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
        loc(src, src.manifest(), 5.2, 20.3)
    p, src = source_with(f"{PFC.PREFIX}/{RUN}/a/mask.bin", product.body(f"{PFC.PREFIX}/{RUN}/b/mask.bin"))        # another grid's
    with pytest.raises(PFC.PointError):
        loc(src, src.manifest(), 5.2, 20.3)
    for wrong in (f"{PFC.PREFIX}/{RUN}/a/0_3.bin", f"{PFC.PREFIX}/2026100118/a/0_2.bin"):                          # another tile, another run's
        body = (other if "2026100118" in wrong else product).body(wrong)
        p, src = source_with(f"{PFC.PREFIX}/{RUN}/a/0_2.bin", body)
        man = src.manifest()
        with pytest.raises(PFC.PointError):
            src.series(man, loc(src, man, 5.2, 20.3))
    short = Product(steps=STEPS[:-1])                                           # a tile with fewer steps than the manifest says
    p, src = source_with(f"{PFC.PREFIX}/{RUN}/a/0_2.bin", short.body(f"{PFC.PREFIX}/{RUN}/a/0_2.bin"))
    man = src.manifest()
    with pytest.raises(PFC.PointError):
        src.series(man, loc(src, man, 5.2, 20.3))
    p, src = source_with(f"{PFC.PREFIX}/{RUN}/a/0_2.bin", b"APT1" + b"\x00" * 64)
    man = src.manifest()
    with pytest.raises(PFC.PointError):
        src.series(man, loc(src, man, 5.2, 20.3))


def test_the_tile_cache_is_capped_by_bytes_and_the_cell_cache_by_count(product, monkeypatch):
    src = product.source()
    man = src.manifest()
    one = len(product.body(f"{PFC.PREFIX}/{RUN}/a/0_2.bin"))
    monkeypatch.setattr(PFC, "TILE_CACHE_BYTES", int(one * 2.5))
    monkeypatch.setattr(PFC, "CELL_CACHE_MAX", 3)
    for lon in (20.0, 30.0, 40.0, 50.0):                                        # four tiles: 0_2, 0_3, 0_5, 0_6
        src.series(man, loc(src, man, 5.0, lon))
    st = src.stats()
    assert st["tiles"] == 2 and st["tile_bytes"] <= one * 2.5 and st["cells"] == 3
    n = product.count("/a/0_6.bin")
    src.series(man, loc(src, man, 5.0, 51.0))                                 # the newest tile is still there
    assert product.count("/a/0_6.bin") == n
    src.series(man, loc(src, man, 5.0, 21.0))                                 # the oldest was dropped: fetched again
    assert product.count("/a/0_2.bin") == 2
    # least recently USED: 0_6 is asked for again (a hit), then a third tile comes: 0_2 goes, 0_6 stays
    src.series(man, loc(src, man, 5.0, 52.0))
    src.series(man, loc(src, man, 5.0, 60.0))                                 # tile 0_7
    n6 = product.count("/a/0_6.bin")
    src.series(man, loc(src, man, 5.0, 53.0))
    assert product.count("/a/0_6.bin") == n6
    src.series(man, loc(src, man, 5.0, 22.0))
    assert product.count("/a/0_2.bin") == 3


def test_two_requests_for_one_tile_fetch_it_once(product):
    src = product.source()
    man = src.manifest()
    cell_a, cell_b = loc(src, man, 5.0, 20.0), loc(src, man, 5.0, 22.0)     # two cells of one tile
    product.delay = 0.15
    out = []
    threads = [threading.Thread(target=lambda c=c: out.append(src.series(man, c)[0, 0])) for c in (cell_a, cell_b, cell_a)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(out) == sorted([code(GRIDS[0], "hs", 0, 5, 20), code(GRIDS[0], "hs", 0, 5, 22), code(GRIDS[0], "hs", 0, 5, 20)])
    assert product.count("/a/0_2.bin") == 1


# ------------------------------- the rank of a row's swells ---------------------------

def test_rank_is_height_squared_times_period_the_most_powerful_first():
    E = (None, None, None)
    # NOAA station 13130's row: 1.26 m at 6.7 s is listed first, 1.05 m at 12.5 s carries more power
    assert PFC.rank_groups([(1.26, 6.7, 202), (1.05, 12.5, 155), (0.24, 13.5, 27), E, E, E]) == [
        (1.05, 12.5, 155), (1.26, 6.7, 202), (0.24, 13.5, 27), E, E, E]
    assert PFC.swell_power(2.0, 10.0) == 40.0 and PFC.swell_power(None, 10.0) == 0.0 and PFC.swell_power(2.0, None) == 0.0
    # packed from the left, whatever the holes were; the same length
    assert PFC.rank_groups([E, (1.0, 10.0, 1), E, (2.0, 10.0, 2), E, E]) == [(2.0, 10.0, 2), (1.0, 10.0, 1), E, E, E, E]
    # equal power: the higher one first; equal again: the source's order
    assert PFC.rank_groups([(1.0, 16.0, 1), (2.0, 4.0, 2)]) == [(2.0, 4.0, 2), (1.0, 16.0, 1)]
    assert PFC.rank_groups([(1.0, 9.0, 1), (1.0, 9.0, 2)]) == [(1.0, 9.0, 1), (1.0, 9.0, 2)]
    # a system without a period ranks last among the systems, before the empty ones
    assert PFC.rank_groups([(3.0, None, 1), (0.5, 8.0, 2), E]) == [(0.5, 8.0, 2), (3.0, None, 1), E]
    assert PFC.rank_groups([]) == [] and PFC.rank_groups([E, E]) == [E, E]


def test_rank_rows_is_the_one_rule_for_stations_swan_and_points():
    row = ["Thursday, October 1, 2026", "2:00 AM", 4.13, 6.7, 22, 3.44, 12.5, 335, None, None, None, 0.79, 13.5, 207,
           None, None, None, None, None, None, 5.0, 60, 5.5]
    rows = A.rank_rows([list(row), ["short"]])
    assert rows[0][2:20] == [3.44, 12.5, 335, 4.13, 6.7, 22, 0.79, 13.5, 207] + [None] * 9
    assert rows[0][:2] == row[:2] and rows[0][20:] == row[20:] and rows[1] == ["short"]      # nothing else moves
    assert A.rank_rows(None) is None and A.rank_rows([]) == []
    again = A.rank_rows([list(rows[0])])
    assert again[0] == rows[0]                                                                # ranked rows stay as they are
    # both bulletin parsers hand their rows through it (the points build theirs in rank order)
    src = open(os.path.join(ROOT, "app.py"), encoding="utf-8").read()
    assert src.count("rank_rows(rows), effective_tz_name, None") == 2
    rows = A._parse_swan_table_text(open(os.path.join(FIX, "swan_buoy_sample.table")).read(), "51201", "Pacific/Honolulu", None)[3]
    assert len(rows) > 5
    for r in rows:                                                                            # PacIOOS's rows, re-ranked
        live = [g for g in range(6) if r[2 + 3 * g] is not None]
        pw = [PFC.swell_power(r[2 + 3 * g], r[3 + 3 * g]) for g in live]
        assert live == list(range(len(live))) and pw == sorted(pw, reverse=True)


def test_rank_rows_ranks_all_six_columns_and_keeps_partial_groups():
    """NOAA lists up to six systems: the fifth and sixth take part in the rank too (G22 R-A5: M49). A group with a
    period or a direction but no height keeps its values, after the ranked ones (G22 R-A23)."""
    row = ["d", "t", 1.0, 5.0, 10, 1.1, 6.0, 20, 1.2, 7.0, 30, 0.9, 8.0, 40, 3.0, 15.0, 50, 2.0, 12.0, 60, 5.0, 90, 4.0]
    A.rank_rows([row])
    assert row[2:20] == [3.0, 15.0, 50, 2.0, 12.0, 60, 1.2, 7.0, 30, 1.1, 6.0, 20, 0.9, 8.0, 40, 1.0, 5.0, 10]
    assert row[20:] == [5.0, 90, 4.0]
    part = ["d", "t", None, 12.0, 270, 1.0, 8.0, 90] + [None] * 12 + [5.0, 90, 4.0]
    A.rank_rows([part])
    assert part[2:8] == [1.0, 8.0, 90, None, 12.0, 270] and part[8:20] == [None] * 12
    assert PFC.rank_groups([(None, None, 10)]) == [(None, None, 10)]


def test_live_buoy_components_are_in_the_same_rank():
    src = open(os.path.join(ROOT, "app.py"), encoding="utf-8").read()
    assert 'components.sort(key=lambda c: (-point_forecast.swell_power(c["height_ft"], c["peak_period_sec"]), -c["height_ft"]))' in src
    comps = [{"height_ft": 4.1, "peak_period_sec": 6.7, "type": "wind"}, {"height_ft": 3.4, "peak_period_sec": 12.5, "type": "swell"},
             {"height_ft": 0.8, "peak_period_sec": 13.5, "type": "swell"}]
    comps.sort(key=lambda c: (-PFC.swell_power(c["height_ft"], c["peak_period_sec"]), -c["height_ft"]))
    assert [c["height_ft"] for c in comps] == [3.4, 4.1, 0.8]


# ------------------------------- the coast --------------------------------------------

def oahu():
    """The real full-resolution coast round Oahu (tests/fixtures/coast/oahu-t1.bin) as a land function."""
    blob = open(os.path.join(FIX, "coast", "oahu-t1.bin"), "rb").read()
    edges = PFC.decode_coast(blob)
    return blob, edges, lambda lats, lons: PFC.land_parity(edges, lons, lats)


def test_the_coast_decoder_and_land_test_agree_with_the_builders():
    blob, edges, land = oahu()
    pieces = BC.decode_file(blob)["pieces"]
    rng = np.random.default_rng(7)
    lons, lats = rng.uniform(-158.4, -157.5, 3000), rng.uniform(21.2, 21.8, 3000)
    got = land(lats, lons)
    want = np.array([BC.point_in_pieces(lo, la, pieces) for lo, la in zip(lons, lats)])
    assert 300 < int(got.sum()) < 2700 and (got == want).all()
    assert len(edges[0]) == len(edges[1]) == len(edges[2]) == len(edges[3]) > 1000
    for bad in (b"", blob[:39], b"XXXX" + blob[4:], blob[:-1], blob + b"\x00", blob[:40] + bytes(len(blob) - 40), blob[:60]):
        with pytest.raises(ValueError):
            PFC.decode_coast(bad)
    empty = BC.encode_file([], 5)
    assert all(len(e) == 0 for e in PFC.decode_coast(empty))
    assert not PFC.land_parity(PFC.decode_coast(empty), np.array([1.0]), np.array([1.0])).any()
    assert PFC.coast_cell_name(21.6, -158.1) == "20_-160" and PFC.coast_cell_name(-0.1, 179.99) == "-5_175"
    assert PFC.coast_cell_name(-0.1, 180.0) == "-5_-180" and PFC.coast_cell_name(90.0, 0.0) == "85_0" and PFC.coast_cell_name(-90.0, -0.001) == "-90_-5"


def test_the_servers_land_test_gives_the_shared_fixtures_answers():
    """tests/fixtures/coast/land_parity.json: the page's inLand is held to the same answers (tests/ui/tools.test.js)."""
    fx = json.load(open(os.path.join(FIX, "coast", "land_parity.json")))
    _blob, edges, _land = oahu()
    pts = np.array([p[:2] for p in fx["points"]])
    got = PFC.land_parity(edges, pts[:, 1], pts[:, 0])
    assert fx["coast"] == "oahu-t1.bin" and len(pts) == 500 and got.tolist() == [p[2] for p in fx["points"]]
    assert 100 < int(got.sum()) < 400


def test_water_is_the_sea_or_land_within_the_shore_band():
    _blob, _edges, land = oahu()
    assert bool(land(np.array([21.667]), np.array([-158.054]))[0]) is True          # the plan's Pipeline id: "land" by 70 m
    o = PFC.water_origin(land, 21.667, -158.054)                                     # ... and water by the band: the path
    assert o is not None and not land(np.array([o[0]]), np.array([o[1]]))[0]         # starts at the nearest water
    assert 0 < PFC._km(21.667, -158.054, *o) * 1000 <= PFC.SHORE_M + 1
    assert PFC.water_origin(land, 21.70, -158.20) == (21.70, -158.20)                # the open sea: the point itself
    assert PFC.water_origin(land, 21.500, -158.025) is None                          # Wahiawa, the middle of the island
    assert PFC.water_origin(land, 21.262, -157.805) is None                          # Diamond Head crater, 1 km inland
    assert (PFC.SHORE_M, PFC.SHORE_STEP_M, PFC.SHORE_DIRS) == (300.0, 50.0, 32)
    # the band is SHORE_M wide: a straight coast along the equator, land to the north
    coast = lambda lats, lons: lats > 0                                              # noqa: E731
    assert PFC.water_origin(coast, 0.0026, 10.0) is not None and PFC.water_origin(coast, 0.0028, 10.0) is None   # 289 m, 311 m


def test_the_shore_band_searches_every_ring_and_direction():
    """Water seen only at 150 m and 11.25 degrees (direction 1 of 32, the third ring) is found; the nearest ring
    wins, then the first direction clockwise from north (G22 R-A5: M02 / M03)."""
    def pond(*spots):                                                                # land everywhere but a few 3 m ponds
        def land(lats, lons):
            out = np.ones(len(lats), dtype=bool)
            for la, lo in spots:
                out &= np.hypot((lats - la) * 111195.0, (lons - lo) * 111195.0 * math.cos(math.radians(la))) > 3.0
            return out
        return land

    def at(m, deg, lat=20.0, lon=150.0):
        b = math.radians(deg)
        return lat + m * math.cos(b) / 111195.0, lon + m * math.sin(b) / (111195.0 * math.cos(math.radians(lat)))
    o = PFC.water_origin(pond(at(150.0, 11.25)), 20.0, 150.0)
    assert o is not None and abs(o[0] - at(150.0, 11.25)[0]) < 1e-9 and abs(o[1] - at(150.0, 11.25)[1]) < 1e-9
    assert PFC.water_origin(pond(at(150.0, 5.0)), 20.0, 150.0) is None             # between two directions: not seen
    near, far = at(50.0, 90.0), at(100.0, 180.0)
    assert PFC.water_origin(pond(far, near), 20.0, 150.0) == pytest.approx(near, abs=1e-9)   # the nearer ring first
    east, west = at(100.0, 90.0), at(100.0, 270.0)
    assert PFC.water_origin(pond(west, east), 20.0, 150.0) == pytest.approx(east, abs=1e-9)  # then clockwise from north
    seen = []
    PFC.water_origin(lambda la, lo: seen.append(la.copy()) or np.ones(len(la), dtype=bool), -89.999, 0.0)
    assert seen[1].min() >= -90.0                                                    # no sample beyond the pole (G22 R-A20)


def box_edges(*boxes):
    """Coast edges (xi, yi, xj, yj) of land boxes (west, east, south, north), each a closed ring."""
    xs = [[], [], [], []]
    for w, e, s_, n in boxes:
        ring = [(w, s_), (e, s_), (e, n), (w, n)]
        for k in range(4):
            (xj, yj), (xi, yi) = ring[k - 1], ring[k]
            for arr, v in zip(xs, (xi, yi, xj, yj)):
                arr.append(v)
    return tuple(np.array(a, dtype=float) for a in xs)


def blocked(edges, x1=10.3, half=0.125, start=False, end=None):
    """Whether the path from (10, 0) to (x1, 0) is blocked, judged as locate judges it (perturbed off the grid)."""
    x0, y0, y1 = 10.0 + 3.7e-9, 2.9e-9, -2.1e-9
    if end is None:
        xi, yi, xj, yj = edges
        end = bool(len(xi)) and PFC.land_parity(edges, np.array([x1 + 1.3e-9]), np.array([y1]))[0]
    runs = PFC.land_runs(PFC.path_crossings(edges, x0, y0, x1 + 1.3e-9, y1), start, end)
    return PFC.path_blocked(runs, PFC._km(0.0, 10.0, 0.0, x1), half * 111.2)


def test_the_path_to_the_cell_must_be_water():
    """Exact crossings: any stretch of land of PATH_LAND_KM or more blocks, wherever it lies; only the stretch that
    reaches the cell's centre is forgiven, up to half a cell (G22 R-A1, R-B2)."""
    z = np.zeros(0)
    assert blocked((z, z, z, z)) is False                                            # open sea
    assert blocked(box_edges((10.10, 10.11, -1, 1))) is True                         # 1.1 km of land between
    assert blocked(box_edges((10.100, 10.1005, -1, 1))) is False                     # a rock: 56 m
    assert blocked(box_edges((10.100, 10.1012, -1, 1))) is True                      # a spit of 133 m (250 m samples missed it)
    assert blocked(box_edges((10.100, 10.1005, -1, 1), (10.15, 10.1505, -1, 1))) is False   # two rocks are two rocks
    assert blocked(box_edges((10.25, 10.26, -1, 1))) is True                         # a spit inside the cell's own box
    assert blocked(box_edges((10.29, 10.31, -1, 1))) is False                        # the centre on an islet (1.1 km)
    assert blocked(box_edges((10.15, 10.31, -1, 1))) is True                         # ... on 16.7 km of land: more than half a cell
    # a polygon the builder cut at a cell line is crossed twice there: one stretch, not two (forgiven as a whole)
    assert blocked(box_edges((10.22, 10.25, -1, 1), (10.25, 10.31, -1, 1))) is False
    assert PFC.land_runs([0.2, 0.5, 0.5, 0.7], False, False) == [(0.2, 0.7)]
    assert PFC.land_runs([0.2, 0.5], False, True) is None                            # the ends disagree: cannot tell
    assert PFC.path_blocked(None, 10.0, 5.0) is True
    assert PFC.land_runs([], True, True) == [(0.0, 1.0)] and PFC.land_runs([0.4], True, False) == [(0.0, 0.4)]
    # a path through a vertex crosses once (each edge counts from its first vertex up to, not including, its second)
    diamond = box_edges()
    pts = [(10.1, 0.0), (10.15, 0.05), (10.2, 0.0), (10.15, -0.05)]
    diamond = tuple(np.array(v) for v in zip(*[(pts[k][0], pts[k][1], pts[k - 1][0], pts[k - 1][1]) for k in range(4)]))
    assert len(PFC.path_crossings(diamond, 10.0, 0.0, 10.3, 0.0)) == 2
    assert PFC.PATH_LAND_KM == 0.1


def test_land_under_the_cells_centre_is_forgiven_up_to_3_km_only():
    """G22 R2-1: half a cell (9-14 km) of forgiveness let a whole barrier island through (Moreton Bay behind North
    Stradbroke Island). An islet or a headland under the centre is forgiven, at most PATH_CENTRE_KM."""
    assert PFC.PATH_CENTRE_KM == 3.0
    assert PFC.centre_forgiven_km({"lat": 0.0, "half": 0.125}) == 3.0
    assert abs(PFC.centre_forgiven_km({"lat": 80.0, "half": 0.125}) - 0.125 * 111.2 * math.cos(math.radians(80))) < 1e-9
    # a lagoon behind a barrier island 15 km wide whose middle carries the only cell within reach: sheltered
    p = Product(land=LAND + ((99.42, 99.58, 19.9, 20.1),))
    src = p.source()
    assert src.locate(src.manifest(), 20.0, 99.75) == (None, "sheltered")
    # ... an islet of 2 km under the centre is still forgiven
    q = Product(land=LAND + ((99.49, 99.51, 19.99, 20.01),)).source()
    cell, why = q.locate(q.manifest(), 20.0, 99.75)
    assert why is None and cell["lon"] == 99.5


def test_the_shore_band_tries_its_other_water_when_the_nearest_is_a_pocket():
    """G22 R2-5: a click on the band whose nearest water is a closed pocket (an estuary, a pond) while the sea lies
    within 300 m on its other side is served from the sea's side (Honolua Bay, the Mundaka shore)."""
    block = ((99.795, 99.805, 19.998, 20.0008), (99.795, 99.805, 20.0012, 20.01),        # land round a 44 m pond
             (99.795, 99.799, 20.0008, 20.0012), (99.801, 99.805, 20.0008, 20.0012))
    src = Product(land=LAND + block).source()
    man = src.manifest()
    first = PFC.water_origin(src.land, 20.0, 99.80)
    assert first is not None and 20.0008 < first[0] < 20.0012                          # the pond, 100 m north
    origins = PFC.water_origins(src.land, 20.0, 99.80)
    assert len(origins) > 1 and origins[0] == first and any(o[0] < 19.998 for o in origins)
    assert all(PFC._km(*a, *b) >= 0.1 for i, a in enumerate(origins) for b in origins[:i])
    assert len(origins) <= PFC.SHORE_TRIES == 8
    cell, why = src.locate(man, 20.0, 99.80)
    assert why is None and cell["lon"] == 99.5                                           # from the sea south of the block
    only = PFC.water_origins(src.land, 20.0, 99.80, 1)
    assert only == [first] and PFC.water_origins(lambda la, lo: la < 1000, 20.0, 99.8) == []


def test_the_mouth_ignores_crossing_pairs_and_rocks():
    """G22 R2-3: the coast cells close their polygons along the 5-degree lines with coincident edges through water; a
    ray crossing such a pair meets no land. A rock under PATH_LAND_KM is no shore either."""
    assert PFC.first_land_km([], 10.0) is None
    assert PFC.first_land_km([0.2, 0.2], 10.0) is None                                 # a pair at one place
    assert PFC.first_land_km([0.2, 0.2, 0.5], 10.0) == 5.0                            # the pair, then the shore at 5 km
    assert PFC.first_land_km([0.1, 0.105, 0.3, 0.6], 10.0) == 3.0                      # a 50 m rock, then land at 3 km
    assert PFC.first_land_km([0.995], 10.0) is None                                    # land in the last 50 m of the look
    assert PFC.first_land_km([0.3], 10.0) == 3.0                                       # land to the end
    # a pair along a cell line beside the path (y = 0.02) and real land on the other side: no gap
    z = np.zeros(0)
    pair = (np.array([10.25, 10.05]), np.array([0.02, 0.02]), np.array([10.05, 10.25]), np.array([0.02, 0.02]))
    south = box_edges((10.05, 10.25, -0.2, -0.005))
    edges = tuple(np.concatenate([pair[k], south[k]]) for k in range(4))
    assert PFC.mouth_aperture(edges, 0.0, 10.0, 0.0, 10.3, 0.0) is None
    north = box_edges((10.10, 10.12, 0.005, 0.2))                                       # a real shore there instead: a gap
    edges = tuple(np.concatenate([pair[k], south[k], north[k]]) for k in range(4))
    assert PFC.mouth_aperture(edges, 0.0, 10.0, 0.0, 10.3, 0.0) is not None


def test_the_coast_beyond_180_is_seen_from_both_sides():
    """A box that runs past 180 gets the far side's edges shifted by 360, and the land test wraps (G22 R-A5: M24)."""
    p = Product(land=LAND + ((-179.95, -179.85, 19.0, 21.0), (179.80, 179.84, 19.0, 21.0)))
    src = p.source()
    xi, yi, xj, yj = src.edges_over(179.7, 180.3, 19.5, 20.5)
    assert len(xi) and xi.min() >= 179.79 and xi.max() <= 180.16 and (xi > 180.0).any()
    xi, yi, xj, yj = src.edges_over(-180.3, -179.7, 19.5, 20.5)
    assert len(xi) and xi.min() >= -180.21 and xi.max() <= -179.84 and (xi < -180.0).any()
    assert src.land(np.array([20.0, 20.0, 20.0]), np.array([180.1, -179.9, 179.82])).tolist() == [True, True, True]
    assert src.land(np.array([20.0]), np.array([179.9]))[0] == False                 # noqa: E712
    edges = src.edges_over(179.75, 180.25, 19.5, 20.5)
    ts = PFC.path_crossings(edges, 179.75, 20.0 + 2.9e-9, 180.25, 20.0 - 2.1e-9)
    assert len(ts) == 4                                                              # in and out of both strips


def test_locate_refuses_land_and_sheltered_water_and_takes_the_cell_over_water():
    # grid b: half a degree; the model's land (and the coast's) is 100..110 E; sea cells end at 99.5 E
    p = Product()
    src = p.source()
    man = src.manifest()
    assert src.locate(man, 20.0, 104.0) == (None, "land")
    assert src.locate(man, 60.0, 20.0) == (None, "nodata")                           # water, and no model cell within reach
    cell, why = src.locate(man, 20.0, 99.75)
    assert why is None and (cell["grid"], cell["lat"], cell["lon"]) == ("b", 20.0, 99.5)
    near = Product(land=((99.8, 110.0, -10.0, 30.0),)).source()                      # a shore 31 km from the cell at 99.5 E
    nman = near.manifest()
    assert near.locate(nman, 20.0, 99.802)[0]["lon"] == 99.5                         # 200 m "inland": the shore band
    assert near.locate(nman, 20.0, 99.804) == (None, "land")                         # 400 m
    # a spit between the point and the only cell within reach: sheltered
    q = Product(land=LAND + ((99.63, 99.70, 15.0, 25.0),))
    src = q.source()
    assert src.locate(src.manifest(), 20.0, 99.75) == (None, "sheltered")
    assert src.locate(src.manifest(), 20.0, 99.6990) == (None, "land")             # ON the spit, in the band: land (G22 R-B7)
    thin = Product(land=LAND + ((99.640, 99.6415, 15.0, 25.0),)).source()           # a spit of 157 m
    # a cell across 180 from the point: the path runs 0.15 degree east, not round the world over 100-110 E (R-A5: N15)
    east = p.source()
    cell, why = east.locate(east.manifest(), 20.0, 179.85)
    assert why is None and (cell["lat"], cell["lon"]) == (20.0, -180.0) and cell["km"] < 16
    assert thin.locate(thin.manifest(), 20.0, 99.75) == (None, "sheltered")
    # ... with a second cell within reach the other way, that one is taken (the nearer one lies beyond land)
    r = Product(land=LAND + ((99.31, 99.37, 15.0, 25.0),))                           # a wall between 99.27 E and the cell at 99.5 E
    src = r.source()
    man = src.manifest()
    masks = {"b": src.mask(man, GRIDS[1])}
    assert PFC.nearest_sea_cell(man["grids"], masks, 20.0, 99.27)["lon"] == 99.5     # the nearest: 24 km, beyond the wall
    cell, why = src.locate(man, 20.0, 99.27)
    assert why is None and (cell["lat"], cell["lon"]) == (20.0, 99.0) and 27 < cell["km"] < 29   # the next one, over water
    cells = PFC.sea_cells_in_reach(man["grids"], {"a": src.mask(man, GRIDS[0])}, 5.2, 20.3, reach_km=200.0)
    assert [c["km"] for c in cells] == sorted(c["km"] for c in cells) and (cells[0]["row"], cells[0]["col"]) == (5, 20)
    assert cells[0]["half"] == 0.5 and len(cells) > 4


def test_a_point_is_never_served_untested(product):
    """The coast data that cannot be read is "temporarily unavailable" (Retry), never a forecast and never final."""
    lat, lon = 22.0, 104.0                                                           # land: needs its coast cell
    for breaker in (lambda p: p.overrides.update({f"{PFC.COAST_PREFIX}/index.json": b"{"}),
                    lambda p: p.overrides.update({f"{PFC.COAST_PREFIX}/index.json": b"[" * 100000}),
                    lambda p: p.overrides.update({f"{PFC.COAST_PREFIX}/index.json": json.dumps({"format": "coast-v1", "q": 10000, "tier1": {"cell": 5, "dir": "f", "cells": {"../x": [50, 4]}}}).encode()}),
                    lambda p: p.overrides.update({f"{PFC.COAST_PREFIX}/f/20_100.bin": b"CST1" + bytes(60)}),
                    lambda p: p.overrides.update({f"{PFC.COAST_PREFIX}/f/20_100.bin": p.coast["20_100"][:-1]}),
                    # a WHOLE cell, but not the one the index lists (an empty one: it would turn the land into water)
                    lambda p: p.overrides.update({f"{PFC.COAST_PREFIX}/f/20_100.bin": BC.encode_file([], 5)}),
                    lambda p: setattr(p, "fail", lambda key: "coast" in key)):
        p = Product()
        src = p.source()
        man = src.manifest()
        breaker(p)
        with pytest.raises(PFC.PointError, match="temporarily unavailable"):
            src.locate(man, lat, lon)
        p.overrides.clear()
        p.fail = None
        assert src.locate(man, lat, lon) == (None, "land")                           # nothing bad was kept
    p = Product()
    src = p.source()
    man = src.manifest()
    for _ in range(3):
        src.locate(man, 22.0, 104.0)
        src.locate(man, 23.0, 104.5)
    assert p.count("coast/v1/index.json") == 1 and p.count("f/20_100.bin") == 1      # read once, kept
    assert src.stats()["coast"] == 1 and src.stats()["coast_bytes"] == 4 * 4 * 8
    src.locate(man, 12.0, 104.0)                                                     # a second cell (10_100): both kept
    assert src.stats()["coast"] == 2 and src.stats()["coast_bytes"] == 2 * 4 * 4 * 8


def test_the_coast_cache_is_capped_by_bytes(product, monkeypatch):
    monkeypatch.setattr(PFC, "COAST_CACHE_BYTES", 4 * 4 * 8 + 10)                    # room for one of the test's cells
    src = product.source()
    man = src.manifest()
    for lat in (22.0, 12.0, 2.0):
        assert src.locate(man, lat, 104.0) == (None, "land")
        assert src.stats()["coast"] == 1 and src.stats()["coast_bytes"] == 4 * 4 * 8
    assert product.count("f/20_100.bin") == 1
    src.locate(man, 22.0, 104.0)                                                     # the oldest was dropped: read again
    assert product.count("f/20_100.bin") == 2


def test_one_answer_reads_one_manifest(api, monkeypatch):
    """The cell's series is read with the manifest the cell was found with: at a run change a point must never show
    one run's values under the other run's times (G22 R-A5: S124)."""
    product, get = api
    src = PFC.PointSource(lambda: "https://bucket", product.fetch)
    seen = []
    real = src.manifest
    monkeypatch.setattr(src, "manifest", lambda: seen.append(1) or real())
    monkeypatch.setattr(A, "POINTS", src)
    assert get(PFC.point_id(5.0, 21.0)).get_json()["error"] is None and len(seen) == 1


def test_a_tile_or_mask_of_another_place_is_refused(product):
    """A tile whose header names other rows / columns or another grid, and a mask of another shape, are refused
    and not kept (G22 R-A5: S61, S62, S56)."""
    grid = GRIDS[0]
    good = product.body(f"{PFC.PREFIX}/{RUN}/a/0_2.bin")
    header, bitmap, planes = PF.decode_tile(good)
    for change in ({"row0": 8}, {"col0": 24}, {"grid": "b"}):
        p = Product()
        bad = dict(header, **change)
        p.overrides[f"{PFC.PREFIX}/{RUN}/a/0_2.bin"] = PF.encode_tile(bad, bitmap, planes)
        src = p.source()
        man = src.manifest()
        with pytest.raises(PFC.PointError):
            src.series(man, loc(src, man, 5.2, 20.3))
        assert src.stats()["tiles"] == 0 and src.stats()["cells"] == 0
    p = Product()
    small = dict(grid, nj=grid["nj"] - 1)
    p.overrides[f"{PFC.PREFIX}/{RUN}/a/mask.bin"] = PF.encode_mask(RUN, small, sea(small))
    src = p.source()
    with pytest.raises(PFC.PointError):
        src.mask(src.manifest(), src.manifest()["grids"][0])


def test_a_coast_cell_must_lie_in_its_own_box_and_a_size_mismatch_reads_the_index_again(product):
    """G22 R-A19: a valid cell of another place served under a name, or an index that gives a cell another size,
    is "temporarily unavailable"; the index is read again next time, so a corrected bucket heals without a restart."""
    p = Product()
    p.overrides[f"{PFC.COAST_PREFIX}/f/20_100.bin"] = p.coast["15_100"]                  # 15..20 N served as 20..25 N
    p.overrides[f"{PFC.COAST_PREFIX}/index.json"] = json.dumps({"format": "coast-v1", "q": 10000, "tier1": {"cell": 5, "dir": "f", "cells": {
        n: [len(p.coast["15_100"]) if n == "20_100" else len(b), 4] for n, b in p.coast.items()}}}).encode()
    src = p.source()
    with pytest.raises(PFC.PointError):
        src.land(np.array([22.0]), np.array([104.0]))
    p = Product()
    p.overrides[f"{PFC.COAST_PREFIX}/index.json"] = json.dumps({"format": "coast-v1", "q": 10000, "tier1": {"cell": 5, "dir": "f", "cells": {
        n: [len(b) + (7 if n == "20_100" else 0), 4] for n, b in p.coast.items()}}}).encode()
    src = p.source()
    with pytest.raises(PFC.PointError):
        src.land(np.array([22.0]), np.array([104.0]))
    p.overrides.clear()                                                               # the bucket is right again
    assert src.land(np.array([22.0]), np.array([104.0]))[0]
    assert p.count("coast/v1/index.json") == 2


def test_a_bad_tile_body_is_not_kept(product):
    """G22 A-3: the tile was cached before it was checked; the next request must fetch again."""
    key = f"{PFC.PREFIX}/{RUN}/a/0_2.bin"
    good = product.body(key)
    for bad in (good[:len(good) // 2], b"<html>error</html>", b"", product.body(f"{PFC.PREFIX}/{RUN}/a/0_3.bin")):
        p = Product()
        src = p.source()
        man = src.manifest()
        cell = loc(src, man, 5.2, 20.3)
        p.overrides[key] = bad
        with pytest.raises(PFC.PointError):
            src.series(man, cell)
        assert src.stats()["tiles"] == 0 and src.stats()["tile_bytes"] == 0
        del p.overrides[key]
        assert src.series(man, cell)[0, 0] == code(GRIDS[0], "hs", 0, 5, 20) and p.count("/a/0_2.bin") == 2


def test_a_request_with_an_old_manifest_does_not_refill_the_caches(product):
    """G22 A-11: at a run change, whoever still holds the old manifest gets its answer, and nothing of the old run
    goes back into the caches."""
    now = [1000.0]
    src = product.source(clock=lambda: now[0])
    old = src.manifest()
    newer = Product(run="2026100118")
    product.run, product.mkey = newer.run, newer.mkey
    now[0] += PFC.POINTER_TTL_S + 1
    new = src.manifest()
    assert new["run"] == "2026100118" and src.current_run() == "2026100118"
    product.run, product.mkey = RUN, f"{PFC.PREFIX}/{RUN}/manifest-20261001T174000Z.json"      # the old run's objects are still there
    cell = loc(src, old, 5.2, 20.3)
    assert src.series(old, cell)[0, 0] == code(GRIDS[0], "hs", 0, 5, 20)
    assert src.stats()["masks"] == 0 and src.stats()["tiles"] == 0 and src.stats()["cells"] == 0


def test_json_nested_too_deep_is_an_unavailable_product_not_a_crash(product):
    product.overrides[f"{PFC.PREFIX}/latest.json"] = b"[" * 200000
    with pytest.raises(PFC.PointError, match="temporarily unavailable"):
        product.source().manifest()


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
    for r in rows:                                                                     # every row in rank order, packed from the left
        live = [g for g in range(6) if r[2 + 3 * g] is not None]
        power = [PFC.swell_power(r[2 + 3 * g], r[3 + 3 * g]) for g in live]
        assert live == list(range(len(live))) and power == sorted(power, reverse=True)
    assert any(r[2] < r[5] for r in rows if r[5] is not None)                          # ... which is not height order
    ny = PFC.point_rows(codes, steps, datetime(2026, 10, 1, 12), pytz.timezone("America/New_York"))
    assert ny[0][:2] == ["Thursday, October 1, 2026", "8:00 AM"] and [r[2:] for r in ny] == [r[2:] for r in rows]
    blank = codes.copy()
    blank[ix["wind"], 5] = PF.MISSING
    blank[ix["hs"], 6] = PF.MISSING
    blank[ix["wdir"], 7] = PF.MISSING
    rows2 = PFC.point_rows(blank, steps, datetime(2026, 10, 1, 12), pytz.utc)
    assert rows2[5][20:22] == [None, None] and rows2[6][22] is None and rows2[7][21] is None and rows2[7][20] is not None
    # the clicked point's coordinates only (owner, 2026-10-02); the model point stays in the payload's `point`
    assert PFC.point_headers(RUN, 21.667, -158.054) == ("Cycle : 20261001 12 UTC", "Location : 21.667N 158.054W")


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


def test_hour_slots_are_every_hour_of_the_run_with_the_rows_where_they_belong():
    tz = pytz.timezone("Pacific/Honolulu")
    slots = PFC.hour_slots([0, 1, 2, 3, 6, 9, 120, 123, 384], datetime(2026, 10, 1, 12), tz)
    assert len(slots) == 385 and [i for _d, _t, i in slots[:10]] == [0, 1, 2, 3, None, None, 4, None, None, 5]
    assert slots[0][:2] == ("Thursday, October 1, 2026", "2:00 AM") and slots[4][:2] == ("Thursday, October 1, 2026", "6:00 AM")
    assert slots[384] == ("Saturday, October 17, 2026", "2:00 AM", 8) and slots[120][2] == 6 and slots[123][2] == 7
    assert sum(i is not None for _d, _t, i in slots) == 9
    syd = PFC.hour_slots(list(range(0, 80)), datetime(2026, 10, 1, 12), pytz.timezone("Australia/Sydney"))
    oct4 = [t for d, t, _i in syd if d == "Sunday, October 4, 2026"]
    assert len(oct4) == 23 and "2:00 AM" not in oct4 and oct4[:3] == ["12:00 AM", "1:00 AM", "3:00 AM"]   # the clocks go forward
    assert PFC.hour_slots([5], datetime(2026, 10, 1, 12), pytz.utc) == [("Thursday, October 1, 2026", "5:00 PM", 0)]


def test_the_real_cell_agrees_with_noaas_bulletin():
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


# ------------------------------- the service ------------------------------------------

@pytest.fixture
def api(product, monkeypatch):
    monkeypatch.setattr(A, "POINTS", product.source())
    monkeypatch.setattr(A, "_POINT_CACHE", {})
    monkeypatch.setattr(A, "_POINT_BUILD_SINCE", [])
    monkeypatch.setattr(A, "_point_tz", lambda lat, lon: "Pacific/Honolulu")
    monkeypatch.setenv("POINTS_ROOT", "https://bucket")
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
    assert p["cell_km"] == round(PFC._km(5.2, 20.3, 5.0, 20.0), 1)                                  # km, one decimal
    hours = (datetime.utcnow() - datetime(2026, 10, 1, 12)).total_seconds() / 3600
    assert abs(p["age_hours"] - hours) < 0.2                                     # HOURS since the cycle (G22 A-8 S119)
    assert d["graph_header"] == {"cycle": "20261001 12 UTC", "tz": "Pacific/Honolulu",
                                 "location": "5.200N 20.300E"}                  # the click's coordinates only (owner)
    g = d["graph_data"]
    # the graphs' time axis: one slot per HOUR of the run, the rows where they belong, nothing between (G22 B-1)
    assert len(g["labels"]) == 385 and g["labels"][0] == "Thursday, October 1, 2026 2:00 AM" and g["units"] == "ft"
    assert g["labels"][4] == "Thursday, October 1, 2026 6:00 AM" and g["labels"][-1] == "Saturday, October 17, 2026 2:00 AM"
    filled = [i for i, v in enumerate(g["height"]["combined"]) if v is not None]
    assert filled == STEPS and [i for i, v in enumerate(g["period"]["s1"]) if v is not None] == STEPS
    assert "_slots" not in p and all(not k.startswith("_") for k in p)
    assert g["swells"] == ["s1", "s2", "s3", "s4"] and g["cycle"] == "Cycle : 20261001 12 UTC"
    assert g["height"]["combined"][0] == round(code(GRIDS[0], "hs", 0, 5, 20) / 100 * 3.28084, 2)
    assert g["height"]["s5"] == [None] * 385
    for i in STEPS:                                                               # the systems of a row in rank order
        power = [g["height"][k][i] ** 2 * g["period"][k][i] for k in g["swells"] if g["height"][k][i] is not None]
        assert power == sorted(power, reverse=True) and len(power) >= 3
    html = d["table_html"]
    assert len(g["sky"]) == 385 and "col-sun" in html and "sun_events" not in g  # plan section 35: hourly slots too
    assert len(re.findall(r"<tr[ >]", html)) == 2 + len(STEPS) and [f"Swell {n}" in html for n in range(1, 7)] == [True] * 4 + [False] * 2
    m = get(pid, compact=1, unit="Metric").get_json()
    assert m["graph_data"]["units"] == "m" and m["graph_data"]["height"]["combined"][0] == round(code(GRIDS[0], "hs", 0, 5, 20) / 100, 2)
    assert len(product.calls) == 5                                             # pointer, manifest, coast index, one mask, one tile: once
    ny = get(pid, tz="America/New_York").get_json()
    assert ny["tz_label"] == "America/New_York" and ny["graph_data"]["labels"][0] == "Thursday, October 1, 2026 8:00 AM"
    assert get(pid, tz="Not/AZone").get_json()["tz_label"] == "Pacific/Honolulu"
    assert len(product.calls) == 5 and len(A._POINT_CACHE) == 2


def test_api_forecast_refuses_land_bad_ids_and_reports_an_unreachable_bucket(api, monkeypatch):
    product, get = api
    d = get(PFC.point_id(5.0, 104.0)).get_json()
    assert d["error"] == PFC.REFUSALS["land"] == ("That point is land or inland water in the site's coastline data. If it is "
                                                  "the sea, try a point a little farther from the shore.")
    assert d["final"] is True and d["reason"] == "land"                           # the page offers no Retry for it
    assert d["table_html"] is None and d["point"] is None and d["graph_data"] is None and "busy" not in d
    d = get(PFC.point_id(60.0, 20.0)).get_json()                                  # water, outside every grid
    assert d["error"] == A.POINT_NO_DATA == PFC.REFUSALS["nodata"] and (d["final"], d["reason"]) == (True, "nodata")
    monkeypatch.setattr(A, "POINTS", Product(land=LAND + ((99.63, 99.70, 15.0, 25.0),)).source())
    d = get(PFC.point_id(20.0, 99.75)).get_json()                                 # a spit between the point and its only cell
    assert d["error"] == PFC.REFUSALS["sheltered"] and (d["final"], d["reason"], d["point"]) == (True, "sheltered", None)
    monkeypatch.setattr(A, "POINTS", product.source())
    assert len(A._POINT_CACHE) == 0                                               # a refusal is not kept
    assert len({PFC.REFUSALS[k] for k in ("land", "sheltered", "nodata")}) == 3   # three answers, three texts
    for bad in ("pt_05200N_20300E", "pt_5200N", "pt_0S_0E"):
        d = get(bad).get_json()
        assert d["error"] == "Invalid forecast point" and d["point"] is None and (d["final"], d["reason"]) == (True, "invalid")
    d = get("pt_5200N_20300E!").get_json()
    assert d["error"] == "Invalid station id" and d["final"] is True              # no Retry for an id that can never be right
    n = product.count("/a/0_2.bin")
    product.fail = lambda key: key.endswith("_2.bin")
    d = get(PFC.point_id(5.2, 20.3)).get_json()
    assert d["error"] == "Forecast points are temporarily unavailable" and d["table_html"] is None and "final" not in d
    product.fail = None
    assert get(PFC.point_id(5.2, 20.3)).get_json()["error"] is None and product.count("/a/0_2.bin") == n + 2


def test_points_are_off_without_a_bucket_address(monkeypatch):
    monkeypatch.setattr(A, "POINTS", PFC.PointSource(A._points_root, lambda url, n: (_ for _ in ()).throw(AssertionError("no fetch"))))
    monkeypatch.delenv("POINTS_ROOT", raising=False)
    monkeypatch.delenv("MODEL_FRAMES_BASE", raising=False)
    assert A._points_root() == ""
    d = A.app.test_client().get("/api/forecast?station=pt_5200N_20300E").get_json()
    assert d["error"] == "Forecast points are not available on this server" and d["table_html"] is None
    assert (d["final"], d["reason"]) == (True, "off")                             # no Retry: nothing will change (G22 A-16)
    monkeypatch.setenv("POINTS_ROOT", "  https://example.org/  ")
    assert A._points_root() == "https://example.org"                              # spaces round an address are not part of it
    monkeypatch.delenv("POINTS_ROOT")
    monkeypatch.setenv("MODEL_FRAMES_BASE", " https://models.allshoresurf.com/gfswave/0p25/v1 ")
    assert A._points_root() == "https://models.allshoresurf.com"
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
    assert sorted(d) == ["error", "graph_data", "graph_header", "lat", "lon", "model", "station", "summary_html", "swan_available", "table_html", "tz_label", "wind_complete"]
    assert d["graph_header"]["location"] == "51201 (21.67N 158.12W)"


def test_the_overlays_zone_for_a_point_is_the_points_civil_zone(monkeypatch):
    seen = []
    monkeypatch.setattr(A, "_point_tz", lambda lat, lon: seen.append((lat, lon)) or "Pacific/Pago_Pago")
    assert A._effective_tz_name("pt_14400S_170700W", None) == "Pacific/Pago_Pago" and seen == [(-14.4, -170.7)]
    assert A._effective_tz_name("pt_14400S_170700W", "America/New_York") == "America/New_York"
    assert A._effective_tz_name("pt_014400S_170700W", None) == "UTC"               # not a point id: the old rule


def test_the_page_renders_for_a_point_without_javascript(api):
    product, _get = api
    pid = PFC.point_id(5.2, 20.3)
    html = A.app.test_client().get(f"/?station={pid}&render=full").get_data(as_text=True)
    assert "Swell 1" in html and "<strong>Location : </strong>5.200N 20.300E &nbsp;|" in html and "nearest model point" not in html
    shell = A.app.test_client().get(f"/?station={pid}").get_data(as_text=True)
    assert f'"station": "{pid}"' in shell or f'"station":"{pid}"' in shell


def test_points_fetch_caps_the_body_and_refuses_other_statuses(monkeypatch):
    class Sock:
        def __init__(self):
            self.down = 0

        def shutdown(self, how):
            self.down += 1

    class Resp:
        def __init__(self, status, chunks, headers=None):
            self.status_code, self.chunks, self.headers, self.closed = status, chunks, headers or {}, 0
            self.sock = Sock()
            self.raw = type("Raw", (), {"_connection": type("Conn", (), {"sock": self.sock})()})()

        def close(self):
            self.closed += 1

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
    monkeypatch.setattr(A._POINT_HTTP, "get", get)
    monkeypatch.setattr(A.HTTP, "get", lambda *a, **k: (_ for _ in ()).throw(AssertionError("the shared session retries: not for points")))
    seen["resp"] = Resp(200, [b"ab", b"cd"])
    assert A._points_fetch("https://bucket/x", 4) == b"abcd" and seen["stream"] is True and seen["timeout"] == (3, 6)
    assert not any(isinstance(a.max_retries.total, int) and a.max_retries.total > 0 for a in A._POINT_HTTP.adapters.values())   # one attempt
    assert A._POINT_FETCH_MAX_S == 8 and seen["resp"].closed == 0 and seen["resp"].sock.down == 0
    src = open(os.path.join(ROOT, "app.py"), encoding="utf-8").read()
    assert "cut short" not in src                                                    # urllib3 raises IncompleteRead itself (G22 R-A2)

    class Slow(Resp):                                                               # a body that trickles: its socket shut down
        def iter_content(self, n):                                                  # at the cap (a close would wait for the
            yield b"a"                                                              # reader's lock: G22 R-A2)
            for _ in range(100):
                if self.sock.down:
                    raise IOError("shut down")
                time.sleep(0.02)
            yield b"b"
    monkeypatch.setattr(A, "_POINT_FETCH_MAX_S", 0.1)
    seen["resp"] = Slow(200, [])
    t0 = time.time()
    with pytest.raises(IOError, match="shut down"):
        A._points_fetch("https://bucket/x", 9)
    assert time.time() - t0 < 1.0

    class Late(Resp):                                                               # the time ran out before the body came:
        def iter_content(self, n):                                                  # shut down at once, and the deadline
            return iter([b"a", b"b"])                                               # counts from the request
    monkeypatch.setattr(A, "_POINT_FETCH_MAX_S", 0.05)

    def slow_get(url, timeout=None, stream=False):
        time.sleep(0.12)
        return seen["resp"]
    monkeypatch.setattr(A._POINT_HTTP, "get", slow_get)
    seen["resp"] = Late(200, [])
    with pytest.raises(IOError, match="too slow"):
        A._points_fetch("https://bucket/x", 9)
    assert seen["resp"].sock.down == 1
    monkeypatch.setattr(A._POINT_HTTP, "get", get)
    monkeypatch.setattr(A, "_POINT_FETCH_MAX_S", 8)
    # a timer that fires once the body is read touches nothing: the socket may be the pool's again (G22 R2-8)
    fired = []

    class Hold:
        def __init__(self, t, fn):
            fired.append(fn)
            self.daemon = False

        def start(self):
            pass

        def cancel(self):
            pass
    monkeypatch.setattr(A.threading, "Timer", Hold)
    seen["resp"] = Resp(200, [b"ab"])
    assert A._points_fetch("https://bucket/x", 9) == b"ab"
    fired[-1]()
    assert seen["resp"].sock.down == 0
    seen["resp"] = Resp(200, [b"ab", b"cde"])
    with pytest.raises(IOError, match="too large"):
        A._points_fetch("https://bucket/x", 4)
    for status in (404, 403, 500, 206):
        seen["resp"] = Resp(status, [b"x"])
        with pytest.raises(IOError, match=str(status)):
            A._points_fetch("https://bucket/x", 4)


def test_a_points_time_zone_is_its_own_waters_then_the_nearest_stations(monkeypatch):
    """Owner, 2026-10-02: the nearest station's zone (G22 B-2: 340 km north of Oahu showed Etc/GMT+11, an hour off
    the station next to it)."""
    coords = {"HNL01": {"lat": 24.0, "lon": -158.0}, "46006": {"lat": 40.8, "lon": -137.48}, "MID": {"lat": 0.0, "lon": -140.0},
              "FAR": {"lat": -40.0, "lon": 100.0}}
    zones = {"HNL01": "Pacific/Honolulu", "46006": "America/Los_Angeles", "MID": "Etc/GMT+9"}
    own = {}
    monkeypatch.setattr(A, "load_station_coords", lambda: coords)
    monkeypatch.setattr(A, "get_station_tz", lambda sid: zones.get(sid))
    monkeypatch.setattr(A, "_safe_tzname_for_latlon", lambda lat, lon: own.get((lat, lon), "Etc/GMT+11"))
    monkeypatch.setattr(A, "_nearest_civil_tz", lambda lat, lon: "ring:%s" % lat)
    monkeypatch.setattr(A, "_POINT_TZ_CACHE", A.OrderedDict())
    assert A._point_tz(24.547, -157.896) == "Pacific/Honolulu"                    # 61 km from HNL01
    assert A._point_tz(40.8, -137.48) == "America/Los_Angeles"                    # at the station
    own[(21.3, -157.9)] = "Pacific/Honolulu"
    own[(36.0, -6.5)] = "Europe/Madrid"
    assert A._point_tz(36.0, -6.5) == "Europe/Madrid"                             # the point's own civil waters come first
    assert A._point_tz(0.5, -140.0) == "ring:0.5"                                 # the nearest station is nautical itself: the old rule
    assert A._point_tz(-60.0, 0.0) == "ring:-60.0"                                # no station within 1,000 km
    assert A._nearest_station_tz(33.5, -158.0) is None and A._nearest_station_tz(32.9, -158.0) == "Pacific/Honolulu"   # 1,056 km, 990 km
    assert A._nearest_station_tz(30.0, -150.0) is None                            # 667 km north and 770 km east of HNL01: 1,040 km away
    assert A.POINT_TZ_STATION_KM == 1000.0
    own[(1.0, 1.0)] = "UTC"                                                       # the lookup's own failure value is not a zone to keep
    assert A._point_tz(1.0, 1.0) == "ring:1.0"
    # the overlay's valid-time zone is the table's
    monkeypatch.setattr(A, "_POINT_TZ_CACHE", A.OrderedDict())
    assert A._effective_tz_name("pt_24547N_157896W", None) == "Pacific/Honolulu"
    # bounded: any visitor can ask for any point (G22 A-9)
    monkeypatch.setattr(A, "_TZ_CACHE_MAX", 5)
    monkeypatch.setattr(A, "_BUOY_TZ_CACHE", A.OrderedDict())
    for k in range(40):
        A._point_tz(10.0 + k, 20.0)
        A._buoy_tz_cached(10.0 + k, 20.0)
    assert len(A._POINT_TZ_CACHE) == 5 and len(A._BUOY_TZ_CACHE) == 5


def test_a_station_across_the_date_line_does_not_give_its_zone(monkeypatch):
    """G22 R-A3: 31 km off the Commander Islands (Russia, UTC+12) a point took America/Adak (UTC-9) from a station
    534 km away: a calendar day off. Only a zone ACROSS the date line is refused (12 hours or more from the point's
    nautical offset in January and in July); a same-side zone stays, however many hours off: Nome time in the Bering
    Sea (G22 R2-2: the 3-hour rule threw it away in summer and gave UTC-12, or Asia/Anadyr a day off)."""
    coords = {"46070": {"lat": 55.0, "lon": 175.3}, "46035": {"lat": 57.05, "lon": -177.58}, "HNL01": {"lat": 24.0, "lon": -158.0},
              "62163": {"lat": 47.5, "lon": -8.5}}
    zones = {"46070": "America/Adak", "46035": "America/Nome", "HNL01": "Pacific/Honolulu", "62163": "Europe/Paris"}
    monkeypatch.setattr(A, "load_station_coords", lambda: coords)
    monkeypatch.setattr(A, "get_station_tz", lambda sid: zones.get(sid))
    monkeypatch.setattr(A, "_safe_tzname_for_latlon", lambda lat, lon: "Etc/GMT-11")
    monkeypatch.setattr(A, "_nearest_civil_tz", lambda lat, lon: "Asia/Kamchatka")
    monkeypatch.setattr(A, "_POINT_TZ_CACHE", A.OrderedDict())
    assert A._point_tz(54.5, 167.0) == "Asia/Kamchatka"                           # Adak is 20-21 h from 167 E's sun time
    assert A._point_tz(57.05, -177.58) == "America/Nome"                          # same side: 3-4 h, kept (R2-2)
    assert A._point_tz(24.5, -157.9) == "Pacific/Honolulu"
    assert A._point_tz(48.0, -11.0) == "Europe/Paris"
    assert A._zone_across_date_line("America/Adak", 167.0) is True and A._zone_across_date_line("America/Adak", -176.6) is False
    assert A._zone_across_date_line("America/Nome", -177.58) is False and A._zone_across_date_line("Asia/Anadyr", -177.58) is True
    assert A._zone_across_date_line("Europe/Paris", -35.0) is False                # many hours, but the same calendar
    assert A._zone_across_date_line("Pacific/Kiritimati", -157.4) is True          # +14 beside -10: across, as the land says
    assert A._zone_across_date_line("Not/AZone", 0.0) is True and A.POINT_TZ_DATE_LINE_H == 12.0
    # the season cannot change the answer: January and July both decide
    src = open(os.path.join(ROOT, "app.py"), encoding="utf-8").read()
    assert "for m in (1, 7)]" in src and "return all(abs(o - naut) >= POINT_TZ_DATE_LINE_H for o in offs)" in src


def test_a_zone_is_shown_as_a_visitor_reads_it(api):
    """G22 R-A17: "Etc/GMT+11" is UTC-11; no raw Etc name on the no-script page or in the classic table."""
    assert PFC.zone_label("Etc/GMT+11") == "UTC\u221211" and PFC.zone_label("Etc/GMT-5") == "UTC+5"
    assert PFC.zone_label("Etc/GMT+0") == PFC.zone_label("Etc/GMT") == PFC.zone_label("Etc/UTC") == "UTC"
    assert PFC.zone_label("Pacific/Honolulu") == "Pacific/Honolulu" and PFC.zone_label(None) == ""
    html = A.build_html_table("Cycle : x", "Location : y", None, [], "Etc/GMT+11", "US")
    assert "Time Zone: UTC\u221211" in html and "Etc/" not in html
    page = A.app.test_client().get(f"/?station={PFC.point_id(5.0, 20.0)}&tz=Etc/GMT%2B11&render=full").get_data(as_text=True)
    meta = page[page.index('id="forecastMeta"'):page.index('id="forecastMeta"') + 600]
    assert "UTC\u221211" in meta and "Etc/GMT" not in meta


def test_busy_is_said_at_once_when_both_builds_are_stuck_and_one_deadline_covers_both_waits(api, monkeypatch):
    """G22 A-6: with both slots held by hanging fetches every further request used to wait its two seconds too, and
    a request could wait twice (for the point's lock, then for a slot)."""
    product, get = api
    monkeypatch.setattr(A, "_POINT_BUILD_WAIT_S", 0.4)
    gate = threading.Event()
    real = product.fetch

    def slow(url, max_bytes):
        if "/a/0_" in url:
            gate.wait(5)
        return real(url, max_bytes)
    monkeypatch.setattr(A, "POINTS", PFC.PointSource(lambda: "https://bucket", slow))
    out = {}
    th = [threading.Thread(target=lambda k=k, lon=lon: out.update({k: get(PFC.point_id(5.0, lon)).get_json()})) for k, lon in (("a", 20.0), ("b", 30.0))]
    for t in th:
        t.start()
    time.sleep(0.15)
    t0 = time.time()
    d = get(PFC.point_id(5.0, 20.0)).get_json()                                   # the same point as a build in progress, slots full
    waited = time.time() - t0
    assert d["error"] == A.POINT_BUSY and d["busy"] is True and "final" not in d and 0.2 < waited < 0.6   # ONE wait, not two
    time.sleep(0.4)                                                               # both builds now older than the wait
    t0 = time.time()
    d = get(PFC.point_id(5.0, 40.0)).get_json()
    assert d["error"] == A.POINT_BUSY and time.time() - t0 < 0.15                 # at once
    gate.set()
    for t in th:
        t.join()
    assert out["a"]["error"] is None and out["b"]["error"] is None and A._POINT_BUILD_SINCE == [] and A._POINT_INFLIGHT == {}
    assert get(PFC.point_id(5.0, 40.0)).get_json()["error"] is None
    # ONE stuck build is not "busy" for everyone: the other slot still builds (G22 R-A5: M43)
    gate.clear()
    th = threading.Thread(target=lambda: out.update({"c": get(PFC.point_id(5.0, 21.0)).get_json()}))
    th.start()
    time.sleep(0.5)                                                               # older than the wait, alone
    d = get(PFC.point_id(-5.0, 31.0)).get_json()                                   # a tile the gate does not hold
    assert d["error"] is None and not d.get("busy")
    gate.set()
    th.join()


def test_one_deadline_covers_the_wait_for_the_point_and_the_wait_for_a_slot(api, monkeypatch):
    """A request that waited for another request's attempt at the same point has only the REST of its time for a
    build slot (it used to get the whole wait a second time: G22 A-6, 3.9 s before "busy")."""
    product, get = api
    monkeypatch.setattr(A, "_POINT_BUILD_WAIT_S", 0.6)
    gate = threading.Event()
    real = product.fetch
    monkeypatch.setattr(A, "POINTS", PFC.PointSource(lambda: "https://bucket", lambda url, n: (gate.wait(5) if "/a/0_" in url else None) or real(url, n)))
    out = {}

    def ask(k, lon):
        t0 = time.time()
        out[k] = (get(PFC.point_id(5.0, lon)).get_json(), time.time() - t0)
    th = [threading.Thread(target=ask, args=a) for a in (("b", 30.0), ("d", 40.0))]   # both slots taken
    for t in th:
        t.start()
    time.sleep(0.05)
    x1 = threading.Thread(target=ask, args=("x1", 50.0))                              # waits for a slot, holding its point
    x1.start()
    time.sleep(0.2)
    ask("x2", 50.0)                                                                   # waits for x1 (0.4 s), then has 0.2 s left
    x1.join()
    assert out["x1"][0]["busy"] is True and out["x2"][0]["busy"] is True
    assert 0.5 < out["x2"][1] < 0.85                                                  # 0.6 s in all, not 0.4 + 0.6
    gate.set()
    for t in th:
        t.join()
    assert A._POINT_INFLIGHT == {} and A._POINT_BUILD_SINCE == []


def test_only_the_run_in_hand_is_kept_and_only_older_runs_are_dropped(api, monkeypatch):
    """G22 A-11: at a run change a request that still holds the old manifest gets its answer; it must neither be
    kept nor wipe the new run's rows."""
    product, get = api
    src = product.source()
    old = src.manifest()
    monkeypatch.setattr(A, "POINTS", src)
    monkeypatch.setattr(src, "manifest", lambda: old)
    monkeypatch.setattr(src, "current_run", lambda: "2026100118")                    # the pointer has moved on
    newer = ("2026100118", "pt_5000N_30000E", "")
    A._POINT_CACHE[newer] = {"ts": time.time(), "data": "the new run's rows"}
    d = get(PFC.point_id(5.0, 20.0)).get_json()
    assert d["error"] is None and d["point"]["run"] == RUN and list(A._POINT_CACHE) == [newer]
    monkeypatch.setattr(src, "current_run", lambda: RUN)                             # the usual case: this IS the run in hand
    A._POINT_CACHE[("2026100106", "pt_5000N_30000E", "")] = {"ts": time.time(), "data": "an older run's rows"}
    assert get(PFC.point_id(5.0, 20.0)).get_json()["error"] is None
    assert sorted(k[0] for k in A._POINT_CACHE) == [RUN, "2026100118"]               # the older run went, the newer stayed


def test_render_full_carries_a_refusal_as_final(api):
    product, _get = api
    html = A.app.test_client().get(f"/?station={PFC.point_id(5.0, 104.0)}&render=full").get_data(as_text=True)
    seed = html[html.index("__initial"):html.index("__initial") + 4000]
    assert '"final": true' in seed and '"reason": "land"' in seed and "That point is land or inland water" in seed
    ok = A.app.test_client().get(f"/?station={PFC.point_id(5.2, 20.3)}&render=full").get_data(as_text=True)
    assert '"final"' not in ok[ok.index("__initial"):ok.index("__initial") + 400000].split("</script>")[0]


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


# ------------------------------- a narrow mouth (owner, 2026-10-03) -------------------

def test_the_narrowest_gap_on_a_served_path_as_the_point_sees_it():
    """mouth_aperture: land on BOTH sides of the path within MOUTH_CAP_KM makes a gap; seen from the point it subtends
    atan(L / s) + atan(R / s); the narrowest counts. The path runs east along the equator from 10.0 E to 10.3 E."""
    z = np.zeros(0)
    cell = dict(lat=0.0, lon=10.3, half=0.125)

    def ap(*boxes, runs=(), cap=PFC.MOUTH_CAP_KM, half=0.0):
        e = box_edges(*boxes) if boxes else (z, z, z, z)
        return PFC.mouth_aperture(e, 0.0, 10.0, cell["lat"], cell["lon"], half, runs, cap)
    assert ap() is None                                                              # open water: no gap at all
    assert ap((10.10, 10.12, 0.005, 0.2)) is None                                    # land on one side only
    # a strait 0.01 degree (1.1 km) wide, 11.1-13.3 km out: about 5.7 degrees
    deg, at, gap = ap((10.10, 10.12, 0.005, 0.2), (10.10, 10.12, -0.2, -0.005))
    assert 4.5 < deg < 6.0 and 11.0 < at < 13.5 and abs(gap - 1.112) < 0.05
    # the same gap seen from close by looks wide (a cove's own mouth): 2.2 km at 0.3-0.6 km
    deg, at, gap = ap((10.003, 10.006, 0.01, 0.2), (10.003, 10.006, -0.2, -0.01))
    assert deg > 120 and at < 0.7
    assert ap((10.10, 10.12, 0.1, 0.2), (10.10, 10.12, -0.2, -0.1)) is None        # 11 km away on each side: beyond the 10 km look
    assert ap((10.10, 10.12, 0.1, 0.2), (10.10, 10.12, -0.2, -0.1), cap=12.0) is not None
    # samples in the path's own land runs are skipped (a rock on the path is no mouth)
    rock = ((10.10, 10.12, 0.005, 0.2), (10.10, 10.12, -0.2, -0.005))
    assert ap(*rock, runs=[(0.0, 1.0)]) is None
    # a gap inside the cell's own box (the model's one patch of sea) is not one the point is served through
    assert ap((10.28, 10.29, 0.005, 0.2), (10.28, 10.29, -0.2, -0.005)) is not None
    assert ap((10.28, 10.29, 0.005, 0.2), (10.28, 10.29, -0.2, -0.005), half=0.125) is None
    assert ap((10.10, 10.12, 0.005, 0.2), (10.10, 10.12, -0.2, -0.005), half=0.125) is not None   # outside the box: looked at
    assert PFC.mouth_aperture((z, z, z, z), 0.0, 10.0, 0.0, 10.004, 0.0) is None   # under 0.6 km: no search
    # both sides count, each at its own distance: 0.56 km north and 5.6 km south at 11 km is about 32 degrees, not 6
    deg, at, gap = ap((10.10, 10.12, 0.005, 0.2), (10.10, 10.12, -0.2, -0.05))
    assert 25 < deg < 35 and abs(gap - 6.12) < 0.05
    # the NARROWEST gap on the path counts: a wide one near the point, a narrow one farther out
    deg, at, gap = ap((10.02, 10.03, 0.02, 0.2), (10.02, 10.03, -0.2, -0.02), (10.20, 10.21, 0.002, 0.2), (10.20, 10.21, -0.2, -0.002))
    assert deg < 3 and at > 20 and abs(gap - 0.445) < 0.01
    # the point's own shore (the first MOUTH_FROM_KM) is not a mouth
    assert ap((10.0, 10.0026, 0.009, 0.2), (10.0, 10.0026, -0.2, -0.009)) is None        # walls over the first 0.29 km only
    assert (PFC.MOUTH_CAP_KM, PFC.MOUTH_DEG, PFC.MOUTH_FROM_KM) == (10.0, 70.0, 0.3)


def test_a_point_served_through_a_narrow_mouth_says_how_far_the_open_water_is():
    """Owner, 2026-10-03: "next to the location gps coordinates say 'open water _ km away'" where a point is served
    through a narrow mouth (narrower than MOUTH_DEG as the point sees it; Fort Point at the Golden Gate is 67)."""
    assert PFC.point_headers(RUN, 37.811, -122.477, {"km": 31.4, "mouth": (66.7, 2.5, 3.7)})[1] == (
        "Location : 37.811N 122.477W (open water 31 km away)")
    assert PFC.point_headers(RUN, 22.213, -159.503, {"km": 13.4, "mouth": (85.0, 1.1, 4.6)})[1] == "Location : 22.213N 159.503W"
    assert PFC.point_headers(RUN, 21.667, -158.054, {"km": 11.6, "mouth": None})[1] == "Location : 21.667N 158.054W"
    assert PFC.point_headers(RUN, 21.667, -158.054)[1] == "Location : 21.667N 158.054W"
    assert PFC.point_headers(RUN, 1.0, 1.0, {"km": 0.4, "mouth": (10.0, 0.5, 0.2)})[1].endswith("(open water 1 km away)")
    assert PFC.narrow_mouth({"mouth": (69.9, 1, 1)}) and not PFC.narrow_mouth({"mouth": (70.0, 1, 1)}) and not PFC.narrow_mouth(None)
    # PointSource.mouth over the coast cells: a strait across the path from 20 N 99.95 E to a 1/6-degree cell at 99.5 E
    strait = Product(land=LAND + ((99.60, 99.62, 20.005, 20.3), (99.60, 99.62, 19.7, 19.995))).source()
    cell = {"lat": 20.0, "lon": 99.5, "half": 1 / 12, "km": PFC._km(20.0, 99.95, 20.0, 99.5)}
    deg, at, gap = strait.mouth(20.0, 99.95, cell, 99.5, [])
    assert deg < 3 and 34 < at < 37 and abs(gap - 1.112) < 0.01                   # 0.01 degree of latitude
    assert Product().source().mouth(20.0, 99.95, cell, 99.5, []) is None             # the same path without the strait
    # across 180: the point at 179.95 E, the cell beyond the date line (its longitude unwrapped from the point)
    far = Product(land=LAND + ((-179.62, -179.60, 20.005, 20.3), (-179.62, -179.60, 19.7, 19.995))).source()
    cell = {"lat": 20.0, "lon": -179.5, "half": 1 / 12, "km": PFC._km(20.0, 179.95, 20.0, -179.5)}
    assert far.mouth(20.0, 179.95, cell, 180.5, [])[0] < 3
    # locate attaches it to the cell it serves: a 1/6-degree grid, so the path runs outside the cell's own box first
    fine = ({"name": "c", "ni": 2160, "nj": 61, "lat0": 25.0, "per_deg": 6, "tile": 30},)
    src = Product(grids=fine, land=LAND + ((99.93, 99.935, 20.005, 20.3), (99.93, 99.935, 19.7, 19.995))).source()
    cell, why = src.locate(src.manifest(), 20.0, 99.95)
    assert why is None and (cell["lat"], round(cell["lon"], 4)) == (20.0, 99.8333)
    assert cell["mouth"] is not None and 25 < cell["mouth"][0] < 45                  # 1.1 km at about 1.8 km
    assert PFC.point_headers(RUN, 20.0, 99.95, cell)[1] == "Location : 20.000N 99.950E (open water 12 km away)"
    src = Product(grids=fine).source()
    cell, why = src.locate(src.manifest(), 20.0, 99.95)
    assert why is None and "mouth" in cell and cell["mouth"] is None


def test_the_api_names_the_open_water_behind_a_narrow_mouth(api, monkeypatch):
    product, get = api
    d = get(PFC.point_id(5.2, 20.3)).get_json()
    assert d["error"] is None and d["graph_header"]["location"] == "5.200N 20.300E" and d["point"]["mouth_deg"] is None
    real = A.POINTS.locate

    def through_a_mouth(man, lat, lon):
        cell, why = real(man, lat, lon)
        return (dict(cell, mouth=(12.04, 3.0, 1.0)), why) if cell else (cell, why)
    monkeypatch.setattr(A.POINTS, "locate", through_a_mouth)
    d = get(PFC.point_id(5.2, 20.2)).get_json()
    km = PFC._km(5.2, 20.2, 5.0, 20.0)
    assert d["error"] is None and d["point"]["mouth_deg"] == 12.0
    assert d["graph_header"]["location"] == f"5.200N 20.200E (open water {int(round(km))} km away)"
