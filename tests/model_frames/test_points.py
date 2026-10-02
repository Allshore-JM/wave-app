"""Forecast-point job (tools/model_frames/points.py, pointfmt.py): offline tests (no network, no eccodes, no R2).

A small synthetic world stands in for NOAA: three grids with the real grids' structure (rows north to south,
columns from 0 E, a band of stored rows), fields whose value at every (grid, field, step, row, column) is known,
land, partitions that come and go. The build runs on it in this process and every published value is read back
through the format's own decoder.
"""
import io
import json
import os
import pickle
import sys
import threading
from datetime import datetime, timezone

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))

import fetch as F      # noqa: E402
import pointfmt as PF  # noqa: E402
import points as PT    # noqa: E402
import publish as P    # noqa: E402

RUN = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
RUNKEY = "2026100112"
SMALL = (
    {"name": "g16", "tag": "global.0p16", "ni": 48, "nj": 20, "lat0": 52.5,  "per_deg": 6, "rows": (0, 20), "tile": 8},
    {"name": "s25", "tag": "gsouth.0p25", "ni": 32, "nj": 14, "lat0": -10.5, "per_deg": 4, "rows": (3, 14), "tile": 4},
    {"name": "n25", "tag": "global.0p25", "ni": 32, "nj": 18, "lat0": 90.0,  "per_deg": 4, "rows": (0, 6),  "tile": 4},
)
UNUSED = ((0, 2, 2, 1, None), (0, 2, 3, 1, None), (10, 0, 11, 1, None), (10, 0, 10, 1, None))   # UGRD VGRD PERPW DIRPW


# ------------------------------- the synthetic world ---------------------------------

def land(grid):
    """True = land (no wave data): a block, a diagonal and one whole 8-cell corner, the same at every step."""
    r, c = np.mgrid[0:grid["nj"], 0:grid["ni"]]
    return ((r % 7 == 3) & (c % 5 < 2)) | ((r + c) % 11 == 0) | ((r < 4) & (c < 4))


def source(grid, name, step):
    """The model's value of `name` at `step` on `grid`: float64 [nj, ni], NaN = missing."""
    r, c = np.mgrid[0:grid["nj"], 0:grid["ni"]]
    kind, i = PF.FIELD_KIND[name], PF.FIELD_NAMES.index(name)
    base = (r * 37 + c * 17 + step * 5 + i * 101) % 1000 / 1000.0             # 0 .. 0.999
    v = {"height": 0.05 + base * 9.0, "period": 2.0 + base * 20.0, "direction": base * 360.0, "speed": base * 30.0}[kind]
    v = v.astype(np.float64)
    if name in ("wind", "wdir"):
        return np.where(land(grid) & ((r + c) % 3 == 0), np.nan, v)           # wind reaches over some land too
    v = np.where(land(grid), np.nan, v)
    part = next((p for p in PF.PARTITIONS if name in p), None)
    if part is not None:                                                      # a partition comes and goes as a whole
        gone = (r + 2 * c + step + PF.PARTITIONS.index(part) * 3) % (4 + PF.PARTITIONS.index(part)) == 0
        v = np.where(gone, np.nan, v)
    return v


def meta_for(grid, ident, step, **over):
    d, cat, num, surf, seq = ident
    step_deg = 1.0 / grid["per_deg"]
    m = {"discipline": d, "parameterCategory": cat, "parameterNumber": num, "typeOfFirstFixedSurface": surf,
         "scaleFactorOfFirstFixedSurface": 0, "scaledValueOfFirstFixedSurface": seq or 1,
         "productDefinitionTemplateNumber": 0, "forecastTime": step, "indicatorOfUnitOfTimeRange": 1,
         "dataDate": 20261001, "dataTime": 1200, "Ni": grid["ni"], "Nj": grid["nj"],
         "jScansPositively": 0, "iScansNegatively": 0, "jPointsAreConsecutive": 0, "alternativeRowScanning": 0,
         "latitudeOfFirstGridPointInDegrees": grid["lat0"],
         "latitudeOfLastGridPointInDegrees": grid["lat0"] - (grid["nj"] - 1) * step_deg,
         "longitudeOfFirstGridPointInDegrees": 0.0, "longitudeOfLastGridPointInDegrees": (grid["ni"] - 1) * step_deg,
         "iDirectionIncrementInDegrees": round(step_deg, 6), "jDirectionIncrementInDegrees": round(step_deg, 6),
         "missingValue": 9999.0, "gridType": "regular_ll"}
    m.update(over)
    return m


class World:
    """fetch_file / read_message for the synthetic world, with hooks the tests turn."""

    def __init__(self):
        self.fetched = []
        self.lock = threading.Lock()
        self.alter = None            # (grid, name, step, array) -> array
        self.fail = None             # (grid, step) -> exception or None

    def fetch_file(self, run_dt, grid, step):
        assert run_dt == RUN
        with self.lock:
            self.fetched.append((grid["name"], step))
        exc = self.fail(grid, step) if self.fail else None
        if exc is not None:
            raise exc
        idents = [f[3] for f in PF.FIELDS]
        order = idents[13:] + list(UNUSED[:2]) + idents[:1] + list(UNUSED[2:]) + idents[1:13]   # NOAA's file order, roughly
        return [pickle.dumps((grid["name"], ident, step)) for ident in order]

    def read_message(self, msg, wanted):
        gname, ident, step = pickle.loads(msg)
        grid = PF.GRID_BY_NAME[gname]
        meta = meta_for(grid, ident, step)
        if not wanted(meta):
            return meta, None
        name = PT.identify(meta)
        arr = source(grid, name, step)
        if self.alter:
            arr = self.alter(grid, name, step, arr)
        return meta, np.where(np.isnan(arr), 9999.0, arr).ravel()


class FakeClient:
    def __init__(self):
        self.objects, self.log, self.lock = {}, [], threading.Lock()
        self.fail_put = None

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        if self.fail_put and self.fail_put(Key):
            raise RuntimeError(f"R2 refused {Key}")
        with self.lock:
            self.objects[Key] = {"body": Body, "ct": ContentType, "cc": CacheControl}
            self.log.append(("put", Key))

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key]["body"])}

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None, **kw):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        start = int(ContinuationToken) if ContinuationToken else 0
        page, nxt = keys[start:start + 7], start + 7                     # small pages: pagination is exercised
        return {"Contents": [{"Key": k} for k in page], "IsTruncated": nxt < len(keys),
                "NextContinuationToken": str(nxt) if nxt < len(keys) else None}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)
            self.log.append(("del", o["Key"]))
        return {}


@pytest.fixture
def world(monkeypatch):
    monkeypatch.setattr(PF, "GRIDS", SMALL)
    monkeypatch.setattr(PF, "GRID_BY_NAME", {g["name"]: g for g in SMALL})
    w = World()
    monkeypatch.setattr(PT, "fetch_file", w.fetch_file)
    monkeypatch.setattr(PT, "read_message", w.read_message)
    monkeypatch.setattr(PT, "_CELLS", {})
    return w


def build(store, steps, tmp_path, upload=True):
    return PT.build_and_publish(store, RUN, steps, str(tmp_path), PT.Inline(), upload=upload, log=lambda *a: None)


def read_cell(client, grid, row, col):
    """(planes [field, step] of the cell, or None for a cell that is not sea) through the published objects."""
    _h, mask = PF.decode_mask(client.objects[PT.mask_key(RUNKEY, grid["name"])]["body"])
    if not mask[row, col]:
        return None
    t = grid["tile"]
    header, bitmap, planes = PF.decode_tile(client.objects[PT.tile_key(RUNKEY, grid["name"], row // t, col // t)]["body"])
    assert (header["row0"], header["col0"]) == (row // t * t, col // t * t)
    i = PF.cell_index(bitmap, row - header["row0"], col - header["col0"])
    assert i >= 0
    return planes[:, :, i]


# ------------------------------- the format ------------------------------------------

def test_quantise_scales_round_half_up_and_missing():
    q = PF.quantise
    assert q([0.0, 0.004, 0.005, 1.234, 1.235, 12.0], "height").tolist() == [0, 0, 1, 123, 124, 1200]
    assert q([7.44, 7.45, 20.04, 20.05], "period").tolist() == [74, 75, 200, 201]
    assert q([0.0, 0.49, 0.5, 359.49, 359.5, 360.0], "direction").tolist() == [0, 0, 1, 359, 0, 0]      # 360 is 0
    assert q([0.04, 0.05, 44.61], "speed").tolist() == [0, 1, 446]
    assert q([np.nan, np.inf, -np.inf], "height").tolist() == [PF.MISSING] * 3
    assert q([700.0, -1.0], "height").tolist() == [PF.MISSING - 1, 0]                                 # clamped, never the missing code
    assert q(np.float32(3.3), "height").dtype == np.dtype("<u2")
    # NOAA's two-decimal values that are ties at the stored precision all go up, whatever float64 makes of them
    ties = np.arange(5, 3000, 10) / 100.0                                      # 0.05, 0.15, ... 29.95
    assert q(ties, "period").tolist() == list(range(1, 301)) and q(ties, "speed").tolist() == list(range(1, 301))
    assert q(np.arange(0, 360) + 0.5, "direction").tolist() == list(range(1, 360)) + [0]
    assert q(np.arange(0, 3000) / 100.0, "height").tolist() == list(range(3000))   # two decimals are kept exactly
    back = PF.dequantise(q([1.234, np.nan, 359.6], "height"), "height")
    assert back[0] == 1.23 and np.isnan(back[1]) and back[2] == 359.6


def test_quantise_error_is_at_most_half_a_quantum():
    rng = np.random.default_rng(1)
    for kind, top in (("height", 25.0), ("period", 30.0), ("direction", 359.4), ("speed", 60.0)):
        v = rng.uniform(0, top, 20000)
        err = np.abs(PF.dequantise(PF.quantise(v, kind), kind) - v)
        assert err.max() <= 0.5 / PF.KINDS[kind]["scale"] + 1e-9, kind


def test_fields_are_the_fifteen_records_with_distinct_identities():
    assert PF.FIELD_NAMES == ("hs", "ws_h", "ws_t", "ws_d", "s1_h", "s1_t", "s1_d", "s2_h", "s2_t", "s2_d",
                              "s3_h", "s3_t", "s3_d", "wind", "wdir")
    assert len({f[3] for f in PF.FIELDS}) == 15
    assert [PF.FIELD_KIND[n] for n in ("hs", "ws_t", "s2_d", "wind", "wdir")] == ["height", "period", "direction", "speed", "direction"]
    assert {k: v["scale"] for k, v in PF.KINDS.items()} == {"height": 100, "period": 10, "direction": 1, "speed": 10}
    for part in PF.PARTITIONS:                                             # height, period, direction of ONE partition
        assert [PF.FIELD_KIND[n] for n in part] == ["height", "period", "direction"] and len({n[:2] for n in part}) == 1


def test_grids_cover_the_latitudes_once_and_tiles_are_four_degrees():
    g16, s25, n25 = PF.GRIDS
    assert [g["name"] for g in PF.GRIDS] == ["g16", "s25", "n25"] and [g["tag"] for g in PF.GRIDS] == ["global.0p16", "gsouth.0p25", "global.0p25"]
    assert (PF.grid_lat(g16, 0), PF.grid_lat(g16, g16["rows"][1] - 1)) == (52.5, -15.0) and g16["rows"] == (0, g16["nj"])
    assert PF.grid_lat(s25, s25["rows"][0]) == -15.25 and PF.grid_lat(s25, s25["rows"][1] - 1) == -79.5 and s25["rows"][1] == s25["nj"]
    assert PF.grid_lat(n25, n25["rows"][0]) == 90.0 and PF.grid_lat(n25, n25["rows"][1] - 1) == 52.75
    assert all(g["tile"] / g["per_deg"] == 4.0 and g["ni"] % g["tile"] == 0 and g["ni"] / g["per_deg"] == 360.0 for g in PF.GRIDS)
    assert PF.grid_lon(g16, 0) == 0.0 and PF.grid_lon(g16, 1080) == -180.0 and abs(PF.grid_lon(g16, 2159) + 1 / 6) < 1e-12
    assert PF.grid_lon(s25, 807) == -158.25 and PF.grid_lon(s25, 1440) == 0.0


def test_layout_is_tile_major_and_matches_each_tiles_bitmap():
    rng = np.random.default_rng(2)
    for grid in PF.GRIDS[:2]:
        mask = rng.random((grid["nj"], grid["ni"])) < 0.6
        mask[:, 5 * grid["tile"]:7 * grid["tile"]] = False                # two whole tile columns of land: no tiles there
        cells, tiles = PF.layout(mask, grid["tile"])
        assert cells.size == mask.sum() and len(set(cells.tolist())) == cells.size
        assert tiles[:, 3].sum() == cells.size and (tiles[:, 3] > 0).all()
        assert (tiles[1:, 2] == tiles[:-1, 2] + tiles[:-1, 3]).all() and tiles[0, 2] == 0     # contiguous runs, in order
        assert not ((tiles[:, 1] == 5) | (tiles[:, 1] == 6)).any()
        key = tiles[:, 0] * 1000 + tiles[:, 1]
        assert (np.diff(key) > 0).all()                                    # tile row, then tile column
        t = grid["tile"]
        for tr, tc, start, count in tiles[:: max(1, len(tiles) // 60)]:
            bitmap = PF.tile_bitmap(mask, t, tr, tc)
            rr, cc = np.nonzero(bitmap)                                    # row-major
            want = (rr + tr * t) * grid["ni"] + (cc + tc * t)
            assert cells[start:start + count].tolist() == want.tolist()
            for k in (0, count // 2, count - 1):
                assert PF.cell_index(bitmap, rr[k], cc[k]) == k
    last = PF.tile_bitmap(np.ones((406, 2160), bool), 24, 16, 89)
    assert last.shape == (22, 24)                                          # the grid's last tile row is clipped
    assert PF.cell_index(np.array([[True, False], [True, True]]), 0, 1) == -1
    assert PF.cell_index(np.array([[True, False], [True, True]]), 1, 1) == 2
    assert PF.cell_index(np.array([[True]]), 0, 5) == -1 and PF.cell_index(np.array([[True]]), -1, 0) == -1
    assert PF.layout(np.zeros((8, 8), bool), 4)[1].shape == (0, 4)


def _tile(steps=5, rows=4, cols=6, seed=3):
    rng = np.random.default_rng(seed)
    bitmap = rng.random((rows, cols)) < 0.7
    bitmap[0, 0] = True
    cells = int(bitmap.sum())
    planes = rng.integers(0, 65536, (len(PF.FIELDS), steps, cells), dtype=np.uint16)
    grid = dict(PF.GRIDS[0], tile=6)
    header = PF.tile_header(RUNKEY, grid, 2, 3, rows, cols, cells, steps)
    return header, bitmap, planes


def test_tile_round_trip_and_header():
    header, bitmap, planes = _tile()
    blob = PF.encode_tile(header, bitmap, planes)
    assert blob[:4] == b"APT1"
    h, b, p = PF.decode_tile(blob)
    assert h == header and (b == bitmap).all() and (p == planes).all() and p.dtype == np.dtype("<u2")
    assert (h["row0"], h["col0"], h["tile"], h["grid"], h["run"], h["format"]) == (12, 18, [2, 3], "g16", RUNKEY, "apt1")
    assert h["fields"] == list(PF.FIELD_NAMES)
    assert PF.encode_tile(header, bitmap, planes) == blob                  # the same bytes every time


def test_tile_encoder_refuses_what_does_not_match():
    header, bitmap, planes = _tile()
    with pytest.raises(ValueError):
        PF.encode_tile(header, bitmap, planes[:, :, :-1])
    with pytest.raises(ValueError):
        PF.encode_tile(header, bitmap, planes[:, :-1])
    with pytest.raises(ValueError):
        PF.encode_tile(dict(header, cells=header["cells"] + 1), bitmap, planes)
    with pytest.raises(ValueError):
        PF.encode_tile(header, bitmap.T, planes)
    with pytest.raises(ValueError):
        PF.encode_tile(dict(header, cells=0), np.zeros_like(bitmap), planes[:, :, :0])


def test_tile_decoder_refuses_damaged_objects():
    import lzma
    import struct
    header, bitmap, planes = _tile()
    blob = PF.encode_tile(header, bitmap, planes)
    bad = [b"", b"APT", b"XXXX" + blob[4:], blob[:40], blob[:-20], blob + b"junk",
           blob[:4] + struct.pack("<I", 10 ** 6) + blob[8:]]
    hlen = struct.unpack("<I", blob[4:8])[0]
    nb = (bitmap.size + 7) // 8
    body = blob[8 + hlen + nb:]

    def rebuilt(h, bits=None, payload=body):
        hj = json.dumps(h, separators=(",", ":"), sort_keys=True).encode()
        return b"APT1" + struct.pack("<I", len(hj)) + hj + (blob[8 + hlen:8 + hlen + nb] if bits is None else bits) + payload

    assert PF.decode_tile(rebuilt(header))[0] == header                     # the rebuild helper itself is sound
    bad += [rebuilt(dict(header, steps=header["steps"] + 1)),               # the payload is shorter than the header says
            rebuilt(dict(header, steps=header["steps"] - 1)),               # ... or longer (never decompressed past the claim)
            rebuilt(dict(header, cells=header["cells"] - 1)),
            rebuilt(dict(header, format="apt0")),
            rebuilt(dict(header, rows=0)), rebuilt(dict(header, rows=10 ** 6)), rebuilt({k: v for k, v in header.items() if k != "steps"}),
            rebuilt(header, bits=bytes(nb)),                                # an empty bitmap
            rebuilt(header, payload=lzma.compress(bytes(planes.nbytes) + b"x")),
            rebuilt(header, payload=lzma.compress(bytes(100 * 1024 * 1024))),   # a bomb: 100 MB behind a small claim
            rebuilt(header, payload=b"not xz at all")]
    for b in bad:
        with pytest.raises(ValueError):
            PF.decode_tile(b)
    # the bomb is refused without being unpacked: never more than the header's claim is decompressed
    import tracemalloc
    bomb = rebuilt(header, payload=lzma.compress(bytes(100 * 1024 * 1024)))
    tracemalloc.start()
    try:
        with pytest.raises(ValueError):
            PF.decode_tile(bomb)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < 32 * 1024 * 1024, peak                                   # the xz dictionary alone is 8 MB; unpacked it is 100 MB


def test_mask_round_trip_and_damage():
    grid = PF.GRIDS[1]
    rng = np.random.default_rng(4)
    mask = rng.random((grid["nj"], grid["ni"])) < 0.5
    blob = PF.encode_mask(RUNKEY, grid, mask)
    h, m = PF.decode_mask(blob)
    assert (m == mask).all() and h == {"format": "apt1", "run": RUNKEY, "grid": "s25", "ni": 1440, "nj": 277, "cells": int(mask.sum())}
    with pytest.raises(ValueError):
        PF.encode_mask(RUNKEY, grid, mask[:-1])
    import struct
    import zlib
    hlen = struct.unpack("<I", blob[4:8])[0]

    def rebuilt(h, payload=blob[8 + hlen:]):
        hj = json.dumps(h, separators=(",", ":"), sort_keys=True).encode()
        return b"APM1" + struct.pack("<I", len(hj)) + hj + payload
    assert (PF.decode_mask(rebuilt(h))[1] == mask).all()
    for b in (b"APM1", blob[:-5], blob + b"x", b"APT1" + blob[4:], blob[:60],
              rebuilt(dict(h, cells=h["cells"] + 1)),                          # the header's count is not the mask's
              rebuilt(dict(h, nj=h["nj"] - 1)), rebuilt(dict(h, ni=0)), rebuilt({k: v for k, v in h.items() if k != "ni"}),
              rebuilt(dict(h, format="apt0")), rebuilt(h, payload=zlib.compress(bytes(50 * 1024 * 1024))),
              rebuilt(h, payload=b"not zlib")):
        with pytest.raises(ValueError):
            PF.decode_mask(b)


# ------------------------------- NOAA side -------------------------------------------

def _grib(n, edition=2):
    body = bytes(n - 20)
    return b"GRIB\x00\x00\x0a" + bytes([edition]) + n.to_bytes(8, "big") + body + b"7777"


def test_messages_splits_whole_grib2_messages_only():
    a, b = _grib(40), _grib(123)
    assert PT.messages(a + b) == [a, b]
    for bad in (b"", a[:-1], a + b[:50], a + b"GRIB", b"JUNK" + a, _grib(40, edition=1), a[:-4] + b"8888", a + bytes(30)):
        with pytest.raises(ValueError):
            PT.messages(bad)


def test_fetch_file_maps_a_damaged_body_to_a_transport_error(monkeypatch):
    seen = {}

    def req(url, timeout=60, **kw):
        seen["url"], seen["timeout"] = url, timeout
        return 200, _grib(40) + b"GRIB-cut"
    monkeypatch.setattr(PT.F, "_request", req)
    with pytest.raises(F.TransportError):
        PT.fetch_file(RUN, PF.GRIDS[1], 123)
    assert seen["url"] == "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20261001/12/wave/gridded/gfswave.t12z.gsouth.0p25.f123.grib2"
    assert seen["timeout"] == PT.FILE_TIMEOUT_S
    monkeypatch.setattr(PT.F, "_request", lambda url, **kw: (200, _grib(40) + _grib(60)))
    assert len(PT.fetch_file(RUN, PF.GRIDS[0], 0)) == 2

    def gone(url, **kw):
        raise F.NotReady("404 " + url)
    monkeypatch.setattr(PT.F, "_request", gone)
    with pytest.raises(F.NotReady):
        PT.fetch_file(RUN, PF.GRIDS[0], 0)


def test_identify_by_grib_code_numbers():
    g = PF.GRIDS[0]
    for name, _kind, _noaa, ident in PF.FIELDS:
        assert PT.identify(meta_for(g, ident, 0)) == name
    for ident in UNUSED:                                                    # UGRD, VGRD, PERPW, DIRPW are not used
        assert PT.identify(meta_for(g, ident, 0)) is None
    assert PT.identify(meta_for(g, (10, 0, 8, 241, 4), 0)) is None          # a fourth swell is not a record we know
    assert PT.identify(meta_for(g, (10, 0, 8, 241, 2), 0, scaleFactorOfFirstFixedSurface=1)) is None
    assert PT.identify(meta_for(g, (10, 0, 8, 1, None), 0)) is None         # a swell height that is not in a sequence
    assert PT.identify(meta_for(g, (10, 0, 3, 241, 1), 0)) is None          # a combined height that is
    assert PT.identify(meta_for(g, (0, 0, 3, 1, None), 0)) is None          # another discipline's parameter 3
    assert PT.identify(meta_for(g, (10, 0, 3, 1, None), 0, scaledValueOfFirstFixedSurface=7)) == "hs"


def test_time_and_geometry_guards():
    g = PF.GRIDS[0]
    ok = meta_for(g, (10, 0, 3, 1, None), 123)
    PT.check_time(ok, RUN, 123)
    PT.check_geometry(ok, g)
    for bad in ({"forecastTime": 120}, {"dataDate": 20260930}, {"dataTime": 600}, {"indicatorOfUnitOfTimeRange": 0},
                {"productDefinitionTemplateNumber": 8}):
        with pytest.raises(ValueError):
            PT.check_time({**ok, **bad}, RUN, 123)
    for bad in ({"Ni": 1440}, {"Nj": 405}, {"gridType": "reduced_gg"}, {"jScansPositively": 1}, {"iScansNegatively": 1},
                {"jPointsAreConsecutive": 1}, {"alternativeRowScanning": 1}, {"latitudeOfFirstGridPointInDegrees": 52.25},
                {"latitudeOfLastGridPointInDegrees": -15.2}, {"longitudeOfFirstGridPointInDegrees": 0.17},
                {"longitudeOfLastGridPointInDegrees": 359.5}, {"iDirectionIncrementInDegrees": 0.25},
                {"jDirectionIncrementInDegrees": 0.25}):
        with pytest.raises(ValueError):
            PT.check_geometry({**ok, **bad}, g)
    for other in PF.GRIDS[1:]:                                              # each grid only accepts its own geometry
        PT.check_geometry(meta_for(other, (10, 0, 3, 1, None), 0), other)
        with pytest.raises(ValueError):
            PT.check_geometry(meta_for(other, (10, 0, 3, 1, None), 0), g)
    real = dict(ok, iDirectionIncrementInDegrees=0.166667, jDirectionIncrementInDegrees=0.166667,
                longitudeOfLastGridPointInDegrees=359.833376)                # NOAA's own rounding of 1/6
    PT.check_geometry(real, g)


def test_decode_file_orientation_missing_and_guards(world):
    g = SMALL[0]
    out = PT.decode_file(world.fetch_file(RUN, g, 6), g, RUN, 6)
    assert set(out) == set(PF.FIELD_NAMES)
    for name in PF.FIELD_NAMES:
        want = source(g, name, 6)
        assert out[name].shape == (g["nj"], g["ni"]) and out[name].dtype == np.float64
        assert (np.isnan(out[name]) == np.isnan(want)).all()                # 9999 -> NaN, nothing else
        assert np.allclose(out[name][~np.isnan(want)], want[~np.isnan(want)], atol=1e-4)   # row-major, rows as given
    msgs = world.fetch_file(RUN, g, 6)
    with pytest.raises(ValueError, match="records missing.*s2_t"):
        PT.decode_file([m for m in msgs if pickle.loads(m)[1] != (10, 0, 9, 241, 2)], g, RUN, 6)
    with pytest.raises(ValueError, match="two records for hs"):
        PT.decode_file(msgs + [m for m in msgs if pickle.loads(m)[1] == (10, 0, 3, 1, None)], g, RUN, 6)
    with pytest.raises(ValueError):                                          # the file of another hour
        PT.decode_file(msgs, g, RUN, 7)
    with pytest.raises(ValueError):                                          # ... or of another grid
        PT.decode_file(world.fetch_file(RUN, SMALL[1], 6), g, RUN, 6)
    world.alter = lambda grid, name, step, arr: arr * 40 if name == "s1_h" else arr      # heights of 300 m: not a height
    with pytest.raises(ValueError, match="s1_h.*not a height"):
        PT.decode_file(msgs, g, RUN, 6)
    world.alter = lambda grid, name, step, arr: -arr if name == "wdir" else arr
    with pytest.raises(ValueError, match="wdir"):
        PT.decode_file(msgs, g, RUN, 6)
    world.alter = lambda grid, name, step, arr: np.where(np.isnan(arr), arr, np.inf) if name == "wind" else arr
    with pytest.raises(ValueError):
        PT.decode_file(msgs, g, RUN, 6)
    world.alter = lambda grid, name, step, arr: arr[:-1]                     # a record of the wrong size
    with pytest.raises(ValueError, match="values for a"):
        PT.decode_file(msgs, g, RUN, 6)


def test_needed_keys_and_completeness(monkeypatch):
    keys = PT.needed_keys(RUN)
    assert len(keys) == 209 * 3 and len(set(keys)) == len(keys)
    assert keys[0] == "gfs.20261001/12/wave/gridded/gfswave.t12z.global.0p16.f000.grib2"
    assert keys[-1] == "gfs.20261001/12/wave/gridded/gfswave.t12z.global.0p25.f384.grib2"
    assert "gfs.20261001/12/wave/gridded/gfswave.t12z.gsouth.0p25.f123.grib2" in keys
    assert not any("f121" in k or k.endswith(".idx") for k in keys)
    assert PT.grid_url(RUN, PF.GRIDS[0], 5) == F.S3 + "/" + keys[15]
    present = set(keys)
    asked = []

    def listing(prefix, max_keys=1000):
        asked.append(prefix)
        return [k for k in sorted(present) if k.startswith(prefix)] + [prefix + "000.grib2.idx"]
    monkeypatch.setattr(PT.F, "list_keys", listing)
    assert PT.run_is_complete(RUN) and len(asked) == 3 and all(p.endswith(".f") for p in asked)
    present.discard(keys[400])
    assert PT.missing_objects(RUN) == [keys[400]] and not PT.run_is_complete(RUN)
    complete = {datetime(2026, 10, 1, 6, tzinfo=timezone.utc)}
    monkeypatch.setattr(PT, "run_is_complete", lambda dt: dt in complete)
    assert PT.latest_complete_run(now=datetime(2026, 10, 1, 17, 30, tzinfo=timezone.utc)) == datetime(2026, 10, 1, 6, tzinfo=timezone.utc)
    complete.clear()
    assert PT.latest_complete_run(now=datetime(2026, 10, 1, 17, 30, tzinfo=timezone.utc)) is None

    def boom(dt):
        raise F.TransportError("listing down")
    monkeypatch.setattr(PT, "run_is_complete", boom)
    with pytest.raises(F.TransportError):                                    # never read as "not there yet"
        PT.latest_complete_run()


# ------------------------------- the build -------------------------------------------

def test_every_published_value_is_the_models_value(world, tmp_path):
    client = FakeClient()
    steps = [0, 1, 2, 120, 123, 384]
    man = build(P.Store(client, "b"), steps, tmp_path)
    assert man["complete"] is False and man["steps"] == steps
    checked = 0
    for grid in SMALL:
        r0, r1 = grid["rows"]
        sea = ~land(grid)
        for row in range(grid["nj"]):
            for col in range(grid["ni"]):
                got = read_cell(client, grid, row, col)
                if not (sea[row, col] and r0 <= row < r1):
                    assert got is None                                       # land, or a row the grid does not store
                    continue
                assert got.shape == (15, len(steps))
                for si, step in enumerate(steps):
                    for fi, name in enumerate(PF.FIELD_NAMES):
                        v = source(grid, name, step)[row, col]
                        want = PF.MISSING if np.isnan(v) else int(PF.quantise(v, PF.FIELD_KIND[name]))
                        assert got[fi, si] == want, (grid["name"], row, col, step, name)
                        checked += 1
    assert checked > 60000
    # a partition that is missing is missing as a whole, and a present height keeps its period and direction
    g = SMALL[0]
    cell = read_cell(client, g, 5, 9)
    for part in PF.PARTITIONS:
        idx = [PF.FIELD_NAMES.index(n) for n in part]
        miss = cell[idx] == PF.MISSING
        assert (miss.all(0) | ~miss.any(0)).all()
    assert (cell[PF.FIELD_NAMES.index("ws_h")] == PF.MISSING).any() and (cell[PF.FIELD_NAMES.index("ws_h")] != PF.MISSING).any()
    assert (cell[0] != PF.MISSING).all()                                     # the combined height is always there


def test_objects_keys_order_and_manifest(world, tmp_path):
    client = FakeClient()
    man = build(P.Store(client, "b"), [0, 3], tmp_path)
    keys = [k for _op, k in client.log]
    assert all(k.startswith("gfswave/points/v1/2026100112/") for k in keys)
    tiles = [k for k in keys if k.endswith(".bin") and not k.endswith("mask.bin")]
    masks = [k for k in keys if k.endswith("mask.bin")]
    assert len(masks) == 3 and len(tiles) == sum(g["tiles"] for g in man["grids"]) > 20
    assert keys[-1].startswith("gfswave/points/v1/2026100112/partial-") and keys[-2].startswith("gfswave/points/v1/2026100112/stats-")
    assert max(keys.index(k) for k in tiles + masks) < len(keys) - 2          # every tile and mask before the stats and the manifest
    assert all(client.objects[k]["cc"] == P.IMMUTABLE for k in keys)
    assert all(client.objects[k]["ct"] == "application/octet-stream" for k in tiles + masks)
    assert PT.LATEST_KEY not in client.objects                                # a subset never flips the pointer
    stored = json.loads(client.objects[keys[-1]]["body"])
    assert stored["stats"] == keys[-2] and "_stats" not in stored and "_objects" not in stored
    for k in ("schema", "format", "format_key", "run", "run_utc", "steps", "fields", "grids", "files", "complete", "missing", "digest"):
        assert stored[k] == man[k]
    assert (stored["schema"], stored["format"], stored["run"], stored["run_utc"], stored["missing"]) == (1, "apt1", RUNKEY, "2026-10-01T12:00:00Z", 65535)
    assert stored["files"]["template"] == "gfswave/points/v1/2026100112/{grid}/{tr}_{tc}.bin"
    assert stored["step_schedule"] == [[0, 120, 1], [123, 384, 3]] and stored["expected_steps"] == 209
    assert [f["name"] for f in stored["fields"]] == list(PF.FIELD_NAMES)
    assert stored["fields"][2] == {"name": "ws_t", "kind": "period", "grib": "WVPER", "scale": 10, "units": "s"}
    g16, s25, n25 = stored["grids"]
    assert (g16["name"], g16["source"], g16["lat0"], g16["lon0"], g16["per_deg"], g16["rows"], g16["tile"]) == ("g16", "gfswave global.0p16", 52.5, 0.0, 6, [0, 20], 8)
    assert (s25["rows"], s25["lat_north"], s25["lat_south"]) == ([3, 14], -11.25, -13.75)
    assert n25["mask"] == "gfswave/points/v1/2026100112/n25/mask.bin"
    for g, grid in zip(stored["grids"], SMALL):
        r0, r1 = grid["rows"]
        assert g["sea_cells"] == int((~land(grid))[r0:r1].sum()) and g["bytes"] == sum(len(client.objects[k]["body"]) for k in tiles if f"/{g['name']}/" in k)
    stats = json.loads(client.objects[keys[-2]]["body"])
    assert len(stats["steps"]) == 2 * 3 and {s["grid"] for s in stats["steps"]} == {"g16", "s25", "n25"}
    assert all(s["hs_lost"] == 0 and s["hs_extra"] == 0 and s["ragged"] == 0 for s in stats["steps"])
    assert sum(len(v) for v in stats["tiles"].values()) == len(tiles)
    assert man["_objects"] == {"count": len(tiles) + 3, "bytes": sum(len(client.objects[k]["body"]) for k in tiles + masks)}
    # a tile's header says where it is and what it holds
    h, bitmap, planes = PF.decode_tile(client.objects["gfswave/points/v1/2026100112/g16/1_2.bin"]["body"])
    assert (h["run"], h["grid"], h["tile"], h["row0"], h["col0"], h["steps"]) == (RUNKEY, "g16", [1, 2], 8, 16, 2)
    assert planes.shape == (15, 2, int(bitmap.sum()))
    assert (bitmap == ~land(SMALL[0])[8:16, 16:24]).all()
    assert "gfswave/points/v1/2026100112/g16/0_0.bin" in client.objects       # partly land: its 4x4 corner is not sea
    assert not PF.decode_tile(client.objects["gfswave/points/v1/2026100112/g16/0_0.bin"]["body"])[1][:4, :4].any()
    assert world.fetched == [(g["name"], 0) for g in SMALL] + [(g["name"], 3) for g in SMALL]   # one download per file


def test_a_complete_build_flips_the_pointer_last(world, tmp_path, monkeypatch):
    monkeypatch.setattr(PT.F, "STEPS", [0, 1, 2, 3])
    client = FakeClient()
    man = build(P.Store(client, "b"), [0, 1, 2, 3], tmp_path)
    assert man["complete"] is True
    keys = [k for _op, k in client.log]
    assert keys[-1] == PT.LATEST_KEY and keys[-2].startswith("gfswave/points/v1/2026100112/manifest-") and keys[-3].startswith("gfswave/points/v1/2026100112/stats-")
    ptr = json.loads(client.objects[PT.LATEST_KEY]["body"])
    assert ptr == {"run": RUNKEY, "manifest": keys[-2], "complete": True, "format": "apt1", "format_key": PT.format_key(),
                   "published_utc": man["published_utc"], "steps": 4}
    assert client.objects[PT.LATEST_KEY]["cc"] == P.POINTER
    with pytest.raises(ValueError):
        PT.point_to(P.Store(client, "b"), RUNKEY, "x", dict(man, complete=False))


def test_a_dry_run_builds_everything_and_uploads_nothing(world, tmp_path):
    man = PT.build_and_publish(None, RUN, [0, 3], str(tmp_path), PT.Inline(), upload=False, log=lambda *a: None)
    assert man["_objects"]["count"] == sum(g["tiles"] for g in man["grids"]) + 3 and man["_objects"]["bytes"] > 0
    client = FakeClient()
    man2 = PT.build_and_publish(P.Store(client, "b"), RUN, [0, 3], str(tmp_path), PT.Inline(), upload=False, log=lambda *a: None)
    assert client.log == [] and man2["_objects"] == man["_objects"]


def test_two_builds_give_the_same_bytes_and_the_digest_follows_the_values(world, tmp_path):
    a, b, c = FakeClient(), FakeClient(), FakeClient()
    for d in "abc":
        (tmp_path / d).mkdir()
    one = build(P.Store(a, "b"), [0, 3, 6], tmp_path / "a")
    two = build(P.Store(b, "b"), [0, 3, 6], tmp_path / "b")
    bins = sorted(k for k in a.objects if k.endswith(".bin"))
    assert bins == sorted(k for k in b.objects if k.endswith(".bin")) and len(bins) > 20
    assert all(a.objects[k]["body"] == b.objects[k]["body"] for k in bins)
    assert one["digest"] == two["digest"] and len(one["digest"]) == 16
    cell = tuple(np.argwhere(~land(SMALL[2]) & (np.arange(SMALL[2]["nj"])[:, None] < 6))[2])

    def alter(grid, name, step, arr):                                        # ONE value differs by one code
        if (grid["name"], name, step) == ("n25", "s3_t", 6) and np.isfinite(arr[cell]):
            arr = arr.copy()
            arr[cell] += 0.1
        return arr
    assert np.isfinite(source(SMALL[2], "s3_t", 6)[cell])
    world.alter = alter
    three = build(P.Store(c, "b"), [0, 3, 6], tmp_path / "c")
    assert three["digest"] != one["digest"]
    st = lambda man: {(s["grid"], s["step"]): s["crc"] for s in man["_stats"]["steps"]}   # noqa: E731
    diff = [k for k in st(one) if st(one)[k] != st(three)[k]]
    assert diff == [("n25", 6)]
    assert PT.run_digest(list(reversed(one["_stats"]["steps"]))) == one["digest"]    # the order they finished in does not matter


def test_the_sea_cells_are_the_first_steps_and_drift_is_counted(world, tmp_path):
    g = SMALL[0]
    sea = np.argwhere(~land(g))
    lost, extra = tuple(sea[10]), tuple(np.argwhere(land(g))[5])

    def alter(grid, name, step, arr):
        if grid["name"] != "g16" or step != 3:
            return arr
        arr = arr.copy()
        if name == "hs":
            arr[lost] = np.nan                                               # a sea cell without a height at this step
            arr[extra] = 1.5                                                 # a height on what was land at the first step
        return arr
    world.alter = alter
    client = FakeClient()
    man = build(P.Store(client, "b"), [0, 3], tmp_path)
    cell = read_cell(client, g, *lost)
    assert cell[0, 0] != PF.MISSING and cell[0, 1] == PF.MISSING and cell[13, 1] != PF.MISSING   # its wind is still there
    assert read_cell(client, g, *extra) is None
    st = {(s["grid"], s["step"]): s for s in man["_stats"]["steps"]}
    assert (st[("g16", 3)]["hs_lost"], st[("g16", 3)]["hs_extra"]) == (1, 1)
    assert (st[("g16", 0)]["hs_lost"], st[("s25", 3)]["hs_lost"], st[("s25", 3)]["hs_extra"]) == (0, 0, 0)


def test_a_second_build_in_the_same_scratch_directory_uses_its_own_sea_cells(world, tmp_path):
    g = SMALL[0]
    build(None, [0, 3], tmp_path, upload=False)
    gone = tuple(np.argwhere(~land(g))[3])                                   # this cell is land in the second build
    world.alter = lambda grid, name, step, arr: _without(arr, gone) if (grid["name"], name) == ("g16", "hs") else arr
    client = FakeClient()
    man = build(P.Store(client, "b"), [0, 3], tmp_path)
    assert man["grids"][0]["sea_cells"] == int((~land(g)).sum()) - 1 and read_cell(client, g, *gone) is None
    for row, col in np.argwhere(~land(g))[4:40]:                             # its neighbours hold their own values
        got = read_cell(client, g, row, col)
        for fi, name in enumerate(PF.FIELD_NAMES):
            v = source(g, name, 3)[row, col]
            assert got[fi, 1] == (PF.MISSING if np.isnan(v) else int(PF.quantise(v, PF.FIELD_KIND[name])))


def _without(arr, cell):
    arr = arr.copy()
    arr[cell] = np.nan
    return arr


def test_step_record_counts_ragged_partitions_and_missing_wind():
    g = SMALL[0]
    fields = {n: source(g, n, 0) for n in PF.FIELD_NAMES}
    mask = PT.sea_mask(fields["hs"], g)
    cells, _tiles = PF.layout(mask, g["tile"])
    rec, stat = PT.step_record(fields, cells, g)
    assert rec.shape == (15, cells.size) and rec.dtype == np.dtype("<u2") and stat["ragged"] == 0
    assert stat["hs_max"] == pytest.approx(float(np.nanmax(fields["hs"])))
    assert (rec[0] == PF.quantise(fields["hs"].ravel()[cells], "height")).all()
    r, c = divmod(int(cells[7]), g["ni"])
    fields["s2_t"][r, c] = np.nan if np.isfinite(fields["s2_t"][r, c]) else 9.0
    fields["wind"][r, c] = np.nan
    _rec, stat2 = PT.step_record(fields, cells, g)
    assert stat2["ragged"] == 1 and stat2["wind_missing"] == stat["wind_missing"] + 1


def test_sea_mask_keeps_only_the_stored_rows():
    g = SMALL[1]
    hs = np.ones((g["nj"], g["ni"]), np.float32)
    hs[5, 5] = np.nan
    m = PT.sea_mask(hs, g)
    assert not m[:3].any() and m[3:].sum() == 11 * 32 - 1 and not m[5, 5]
    n = PT.sea_mask(np.ones((SMALL[2]["nj"], SMALL[2]["ni"]), np.float32), SMALL[2])
    assert n[:6].all() and not n[6:].any()


def test_no_sea_cells_or_no_scratch_space_stop_the_build(world, tmp_path, monkeypatch):
    world.alter = lambda grid, name, step, arr: np.full_like(arr, np.nan) if (grid["name"], name) == ("s25", "hs") else arr
    with pytest.raises(ValueError, match="no sea cells"):
        build(None, [0], tmp_path, upload=False)
    world.alter = None
    monkeypatch.setattr(PT.shutil, "disk_usage", lambda p: type("U", (), {"free": 1 << 20})())
    with pytest.raises(RuntimeError, match="scratch space"):
        build(None, [0], tmp_path, upload=False)


def test_an_upload_failure_stops_the_build_before_the_manifest_and_the_pointer(world, tmp_path, monkeypatch):
    monkeypatch.setattr(PT.F, "STEPS", [0, 3])
    client = FakeClient()
    client.fail_put = lambda key: key.endswith("/s25/1_3.bin")
    with pytest.raises(RuntimeError, match="R2 refused"):
        build(P.Store(client, "b"), [0, 3], tmp_path)
    assert not any("manifest" in k or "stats" in k or k == PT.LATEST_KEY for k in client.objects)


class _Future:
    def __init__(self, exc=None, done=True):
        self.exc, self._done, self.cancelled = exc, done, False

    def done(self):
        return self._done

    def result(self):
        self._done = True
        if self.exc:
            raise self.exc
        return None

    def cancel(self):
        self.cancelled = True


def test_uploads_ask_each_future_once_so_a_failure_is_never_lost():
    """A failure that completes between two looks at the same future must still be raised (run.py G12 P0-1)."""
    up = PT.Uploads(None)
    flips = _Future(RuntimeError("late failure"), done=False)

    class Flipping(_Future):
        def done(self):
            if not self._done:
                self._done = True                                             # finishes right after it was asked
                return False
            return True
    late = Flipping(RuntimeError("late failure"))
    late._done = False
    up.pending = [_Future(), late, _Future()]
    up.drain(8)                                                               # asked once: not done yet -> kept
    assert up.pending == [late]
    with pytest.raises(RuntimeError, match="late failure"):
        up.drain(8)
    up.pending = [_Future(), flips]
    with pytest.raises(RuntimeError, match="late failure"):                   # the final drain waits for every one
        up.drain(0)
    up.pending = [_Future(done=False) for _ in range(5)]
    up.drain(2)
    assert len(up.pending) == 2                                               # at most the backlog trails
    rest = list(up.pending)
    up.close()
    assert all(f.cancelled for f in rest)


def test_uploads_count_what_was_handed_over():
    client = FakeClient()
    up = PT.Uploads(P.Store(client, "b"), workers=2, backlog=1)
    for i in range(6):
        up.put(f"k{i}", b"x" * (i + 1))
    up.drain(0)
    up.close()
    assert sorted(client.objects) == [f"k{i}" for i in range(6)] and (up.count, up.bytes) == (6, 21)
    dry = PT.Uploads(None)
    dry.put("k", b"abc")
    dry.drain(0)
    assert (dry.count, dry.bytes, dry.pending) == (1, 3, [])


# ------------------------------- retention and the two prefixes ----------------------

def _seed(c, prefix, run, manifest=True, n=3):
    for i in range(n):
        c.put_object("b", f"{prefix}/{run}/g16/{i}_0.bin", b"x", "application/octet-stream", "")
    if manifest:
        c.put_object("b", f"{prefix}/{run}/manifest-20261001T000000Z.json", b"{}", "application/json", "")


def test_prune_keeps_two_complete_runs_the_pointer_and_a_build_in_progress():
    c = FakeClient()
    for run in ("2026093012", "2026093018", "2026100100", "2026100106"):
        _seed(c, PT.PREFIX, run)
    _seed(c, PT.PREFIX, "2026093006", manifest=False)                         # an old crashed build
    _seed(c, PT.PREFIX, "2026100112", manifest=False)                         # one that may still be uploading
    c.put_object("b", PT.LATEST_KEY, json.dumps({"run": "2026093018"}).encode(), "application/json", "")
    c.put_object("b", f"{PT.PREFIX}/failed/2026093006.json", b"{}", "application/json", "")
    store = P.Store(c, "b")
    assert PT.list_runs(store) == {"2026093006": False, "2026093012": True, "2026093018": True, "2026100100": True,
                                   "2026100106": True, "2026100112": False}
    assert PT.prune(store, keep=2) == ["2026093006", "2026093012"]
    left = {k.split("/")[3] for k in c.objects if k.startswith(PT.PREFIX + "/20")}
    assert left == {"2026093018", "2026100100", "2026100106", "2026100112"}
    assert f"{PT.PREFIX}/failed/2026093006.json" in c.objects and PT.LATEST_KEY in c.objects
    with pytest.raises(ValueError):
        PT.prune(store, keep=0)


def test_the_frames_and_the_points_never_prune_each_other():
    assert PT.PREFIX == "gfswave/points/v1" and P.PREFIX == "gfswave/0p25/v1"
    assert not PT.PREFIX.startswith(P.PREFIX) and not P.PREFIX.startswith(PT.PREFIX)
    c = FakeClient()
    for run in ("2026093000", "2026093006", "2026093012", "2026093018", "2026100100", "2026100106"):
        _seed(c, PT.PREFIX, run)
        c.put_object("b", f"{P.PREFIX}/{run}/hs/f000.png", b"x", "image/png", "")
        c.put_object("b", f"{P.PREFIX}/{run}/manifest-20261001T000000Z.json", b"{}", "application/json", "")
    c.put_object("b", "static/coast/v1/world-i.bin", b"x", "application/octet-stream", "")
    store = P.Store(c, "b")
    frames_before = sorted(k for k in c.objects if k.startswith(P.PREFIX))
    points_before = sorted(k for k in c.objects if k.startswith(PT.PREFIX))
    assert PT.prune(store, keep=2) == ["2026093000", "2026093006", "2026093012", "2026093018"]
    assert sorted(k for k in c.objects if k.startswith(P.PREFIX)) == frames_before
    points_after = sorted(k for k in c.objects if k.startswith(PT.PREFIX))
    assert len(points_after) == 8
    assert P.prune(store, keep=4) == ["2026093000", "2026093006"]             # the frames' own pruning
    assert P.prune_legacy(store) == 0
    assert sorted(k for k in c.objects if k.startswith(PT.PREFIX)) == points_after and points_after != points_before
    assert "static/coast/v1/world-i.bin" in c.objects
    assert set(store.list_runs()) == {"2026093012", "2026093018", "2026100100", "2026100106"}   # the frames' listing sees frames only


# ------------------------------- main: guards and bookkeeping ------------------------

@pytest.fixture
def cli(world, monkeypatch, tmp_path):
    """main() on the synthetic world with a 3-step model output and a fake R2."""
    monkeypatch.setattr(PT.F, "STEPS", [0, 1, 2])
    monkeypatch.setattr(PT, "latest_complete_run", lambda: RUN)
    client = FakeClient()
    monkeypatch.setattr(PT, "_store_from_env", lambda: P.Store(client, "the-bucket"))

    def run(*args):
        return PT.main(["--workers", "0", "--work", str(tmp_path / "work"), *args])
    return client, run


def test_main_publishes_then_short_circuits(cli, capsys):
    client, run = cli
    assert run() == 0
    ptr = json.loads(client.objects[PT.LATEST_KEY]["body"])
    assert ptr["run"] == RUNKEY and ptr["steps"] == 3
    n = len(client.log)
    assert run() == 0 and len(client.log) == n                                # already live: nothing written
    assert "already live" in capsys.readouterr().out


def test_main_never_regresses_the_pointer(cli, capsys):
    client, run = cli
    client.put_object("b", PT.LATEST_KEY, json.dumps({"run": "2026100118", "complete": True}).encode(), "application/json", "")
    assert run() == 0 and len(client.log) == 1
    assert "not regressing" in capsys.readouterr().out


def test_main_refuses_a_subset_without_dry_run_or_local_and_bad_arguments(cli, capsys, tmp_path):
    client, run = cli
    assert run("--steps", "0,1") == 2 and client.log == []
    assert run("--steps", "0,5") == 2                                          # not a step of the (patched) model output
    assert run("--steps", "1,0") == 2 and run("--steps", "1,1") == 2
    assert run("--keep", "0") == 2
    assert run("--dry-run", "--local", str(tmp_path / "out")) == 2
    assert run("--steps", "0,1", "--dry-run") == 0 and client.log == []


def test_main_run_option_is_for_dry_runs_and_local_builds_only(cli, monkeypatch, tmp_path, capsys):
    client, run = cli

    def never():
        raise AssertionError("--run must not look for the newest run")
    monkeypatch.setattr(PT, "latest_complete_run", never)
    assert run("--run", RUNKEY) == 2 and client.log == []                      # never to the bucket
    for bad in ("20261001", "2026100113", "2026-10-01T12", "202610011200", "x"):
        assert run("--run", bad, "--dry-run") == 2, bad
    assert run("--run", RUNKEY, "--dry-run") == 0 and client.log == []
    assert f"building run {RUNKEY}" in capsys.readouterr().out
    assert run("--run", RUNKEY, "--local", str(tmp_path / "o")) == 0 and client.log == []
    assert (tmp_path / "o" / "gfswave" / "points" / "v1" / "latest.json").exists()


def test_main_dry_run_never_touches_the_store(cli, monkeypatch):
    client, run = cli

    def no_store():
        raise AssertionError("a dry run must not open the store")
    monkeypatch.setattr(PT, "_store_from_env", no_store)
    assert run("--dry-run") == 0 and client.log == []


def test_main_exit_3_when_no_run_is_complete_or_an_object_vanishes(cli, world, monkeypatch, capsys):
    client, run = cli
    monkeypatch.setattr(PT, "latest_complete_run", lambda: None)
    assert run() == 3
    monkeypatch.setattr(PT, "latest_complete_run", lambda: RUN)
    world.fail = lambda grid, step: F.NotReady("404 gone") if (grid["name"], step) == ("s25", 2) else None
    assert run() == 3
    rec = json.loads(client.objects[PT.notready_key(RUNKEY)]["body"])
    assert rec["count"] == 1 and "404 gone" in rec["last_error"]
    assert PT.failed_key(RUNKEY) not in client.objects and PT.LATEST_KEY not in client.objects
    assert not any(k.endswith(".bin") for k in client.objects)                # nothing is uploaded before every step is decoded
    assert run() == 3 and json.loads(client.objects[PT.notready_key(RUNKEY)]["body"])["count"] == 2   # no back-off: it retries

    def down():
        raise F.TransportError("S3 listing: HTTP 503")
    monkeypatch.setattr(PT, "latest_complete_run", down)
    assert run() == 1


def test_main_records_a_failure_and_backs_off(cli, world, capsys):
    client, run = cli
    world.fail = lambda grid, step: F.TransportError("https://x.r2.cloudflarestorage.com/the-bucket reset") if step == 1 else None
    with pytest.raises(F.TransportError):
        run()
    rec = json.loads(client.objects[PT.failed_key(RUNKEY)]["body"])
    assert rec["attempts"] == 1 and "the-bucket" not in rec["last_error"] and "<r2-endpoint>" in rec["last_error"]
    world.fail = None
    n = len(client.log)
    assert run() == 0 and len(client.log) == n                                # waits before retrying
    assert "failed recently" in capsys.readouterr().out
    assert run("--force") == 0 and PT.LATEST_KEY in client.objects            # --force goes ahead
    client.objects[PT.failed_key("2026100200")] = {"body": json.dumps({"run": "2026100200", "attempts": 3, "last_attempt_utc": "2020-01-01T00:00:00Z"}).encode()}
    assert PT._skip_failed(P.Store(client, "b"), "2026100200") is True         # three attempts: no more
    client.objects[PT.failed_key("2026100206")] = {"body": json.dumps({"run": "2026100206", "attempts": 1, "last_attempt_utc": "2020-01-01T00:00:00Z"}).encode()}
    assert PT._skip_failed(P.Store(client, "b"), "2026100206") is False        # an old single failure is retried


def _manifest(client, **over):
    man = {"run": RUNKEY, "complete": True, "format": "apt1", "format_key": PT.format_key(), "steps": [0, 1, 2],
           "published_utc": "2026-10-01T17:40:00Z"}
    man.update(over)
    key = f"{PT.PREFIX}/{RUNKEY}/manifest-20261001T174000Z.json"
    client.put_object("b", key, json.dumps(man).encode(), "application/json", "")
    return key


def test_a_complete_run_that_is_not_pointed_to_gets_its_pointer_repaired(cli, capsys):
    client, run = cli
    key = _manifest(client)
    assert run() == 0
    assert json.loads(client.objects[PT.LATEST_KEY]["body"])["manifest"] == key
    assert not any(k.endswith(".bin") for k in client.objects)                # repaired, not rebuilt
    assert "pointer repaired" in capsys.readouterr().out
    assert run("--force") == 0 and any(k.endswith(".bin") for k in client.objects)   # --force rebuilds it


def test_a_run_published_in_another_format_is_never_rewritten(cli, capsys):
    client, run = cli
    _manifest(client, format_key="0000000000000000")
    assert run() == 2 and run("--force") == 2
    assert not any(k.endswith(".bin") for k in client.objects) and PT.LATEST_KEY not in client.objects
    assert "refusing to rewrite" in capsys.readouterr().out


def test_a_manifest_that_cannot_be_read_or_used(cli, monkeypatch):
    client, run = cli
    key = _manifest(client)
    client.objects[key]["body"] = b"[1, 2]"
    assert run() == 2
    client.objects[key]["body"] = b"{not json"
    assert run() == 2
    _manifest(client, steps=[0, 1])                                           # same format, but not the whole run
    assert run() == 0 and any(k.endswith(".bin") for k in client.objects)     # rebuilt


def test_a_transport_error_reading_the_old_manifest_is_retried_next_tick(cli, monkeypatch):
    client, run = cli
    _manifest(client)

    def boom(store, run_):
        raise ConnectionError("reset")
    monkeypatch.setattr(PT, "newest_manifest", boom)
    assert run() == 1 and PT.LATEST_KEY not in client.objects


def test_main_prunes_after_a_complete_publish_and_reports(cli, monkeypatch, tmp_path):
    client, run = cli
    for old in ("2026093000", "2026093006", "2026093012"):
        _seed(client, PT.PREFIX, old)
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert run() == 0
    assert {k.split("/")[3] for k in client.objects if k.startswith(PT.PREFIX + "/20")} == {"2026093012", RUNKEY}
    text = summary.read_text()
    assert f"points run {RUNKEY}: 3 steps, complete=True" in text and "g16" in text and "WARNING" not in text


def test_main_warns_when_the_sea_cells_drift(cli, world, monkeypatch, tmp_path):
    client, run = cli
    sea = tuple(np.argwhere(~land(SMALL[1]) & (np.arange(SMALL[1]["nj"])[:, None] >= 3))[4])

    def alter(grid, name, step, arr):
        if (grid["name"], name, step) == ("s25", "hs", 2):
            arr = arr.copy()
            arr[sea] = np.nan
        return arr
    world.alter = alter
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert run() == 0
    assert "WARNING: the sea cells differ from the first step's at 1 grid-steps (worst s25 f002: 1 without a height, 0 extra)" in summary.read_text()


def test_local_store_round_trip(cli, tmp_path, world):
    client, run = cli
    out = tmp_path / "out"
    assert run("--local", str(out)) == 0 and client.log == []                  # R2 is never touched
    ptr = json.loads((out / "gfswave" / "points" / "v1" / "latest.json").read_text())
    man = json.loads((out / ptr["manifest"]).read_text())
    assert man["complete"] is True and ptr["run"] == RUNKEY
    blob = (out / "gfswave" / "points" / "v1" / RUNKEY / "g16" / "1_2.bin").read_bytes()
    assert PF.decode_tile(blob)[0]["tile"] == [1, 2]
    assert run("--local", str(out)) == 0                                       # already live there
    lc = PT.LocalClient(str(out))
    store = P.Store(lc, "local")
    assert store.get_json("gfswave/points/v1/none.json") is None
    assert len(store.list_keys(f"{PT.PREFIX}/{RUNKEY}/g16/")) == man["grids"][0]["tiles"] + 1
    assert PT.prune(store, keep=1) == []
    store.delete_prefix(f"{PT.PREFIX}/{RUNKEY}/g16/")
    assert store.list_keys(f"{PT.PREFIX}/{RUNKEY}/g16/") == []
    with pytest.raises(ValueError):
        lc.put_object("local", "../outside.bin", b"x", "", "")


# ------------------------------- constants, workflow, executor -----------------------

def test_format_key_pins_the_format():
    """Anything that changes what a tile's bytes mean changes this key; a published run is then never rewritten.
    A deliberate format change takes a new PREFIX version and a new key here."""
    assert PT.format_key() == "d4292c0a5ad8ad9b"
    assert (PF.FORMAT, PF.MISSING, PF.XZ_PRESET, PT.PREFIX) == ("apt1", 65535, 6, "gfswave/points/v1")


def test_executor_is_inline_or_a_spawned_process_pool():
    assert isinstance(PT._executor(0), PT.Inline)
    ex = PT._executor(2)
    try:
        assert ex.__class__.__name__ == "ProcessPoolExecutor" and ex._mp_context.get_start_method() == "spawn"
    finally:
        ex.shutdown(wait=False, cancel_futures=True)
    assert pickle.loads(pickle.dumps(PT._step_task)) is PT._step_task          # a worker process can be handed the task
    assert list(PT.Inline().map(lambda x: x * 2, [1, 2])) == [2, 4]


def test_points_workflow_is_pinned_separate_and_kept_alive():
    import re
    wf = os.path.join(ROOT, ".github", "workflows")
    text = open(os.path.join(wf, "model-points.yml"), encoding="utf-8").read()
    frames = open(os.path.join(wf, "model-frames.yml"), encoding="utf-8").read()
    uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
    assert uses and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses)
    assert sorted(uses) == sorted(re.findall(r"uses:[ ]*([^ \n]+)", frames))
    env = lambda t: t.split("create-args: >-", 1)[1].split("cache-environment", 1)[0].split()   # noqa: E731
    assert env(text) == env(frames) and len(env(text)) >= 6                    # one pinned environment for both jobs
    assert re.search(r"concurrency:\s*\n\s*group: model-points\s*\n\s*cancel-in-progress: false", text)
    assert "group: model-frames" in frames and "group: model-points" not in frames
    assert "python tools/model_frames/points.py" in text and "run.py" not in text
    assert "workflow_dispatch:" in text and re.search(r'cron: "[^"]+"', text)
    assert '[ -n "$STEPS" ] && ARGS+=(--steps "$STEPS" --dry-run)' in text     # a subset is only ever a dry run
    assert 'if [ "$rc" -eq 3 ]' in text
    keep = open(os.path.join(wf, "model-frames-keepalive.yml"), encoding="utf-8").read()
    assert "gh workflow enable model-points.yml" in keep and "gh workflow enable model-frames.yml" in keep
