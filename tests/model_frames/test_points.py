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
    {"name": "g16", "tag": "global.0p16", "ni": 48, "nj": 20, "lat0": 52.5,  "per_deg": 6, "rows": (0, 20), "data": (0, 19), "tile": 8},
    {"name": "s25", "tag": "gsouth.0p25", "ni": 32, "nj": 14, "lat0": -10.5, "per_deg": 4, "rows": (3, 14), "data": (3, None), "tile": 4},
    {"name": "n25", "tag": "global.0p25", "ni": 32, "nj": 18, "lat0": 90.0,  "per_deg": 4, "rows": (0, 6),  "data": (None, 5), "tile": 4},
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
        self.meta = None             # (grid, name or None, meta) -> meta

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
        if self.meta:
            meta = self.meta(grid, PT.identify(meta), meta)
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


def test_grid_bands_follow_the_rows_noaa_has_waves_for_and_leave_no_gap():
    """NOAA's global.0p16 file spans 52.5 N .. 15 S but holds waves on rows 2..390 only (52.167 N .. 12.5 S: G22a
    P1-1, measured on three cycles). The other two grids are stored up to those rows: a band cut at the FILE's
    bounds left no wave data at all between 12.75 S and 14.85 S."""
    g16, s25, n25 = PF.GRIDS
    assert [g["name"] for g in PF.GRIDS] == ["g16", "s25", "n25"] and [g["tag"] for g in PF.GRIDS] == ["global.0p16", "gsouth.0p25", "global.0p25"]
    assert g16["rows"] == (0, g16["nj"]) and g16["data"] == (2, 390)
    assert (round(PF.grid_lat(g16, 2), 4), PF.grid_lat(g16, 390)) == (52.1667, -12.5)
    assert s25["rows"] == (9, s25["nj"]) and s25["data"] == (9, None)
    assert (PF.grid_lat(s25, 9), PF.grid_lat(s25, s25["nj"] - 1)) == (-12.75, -79.5)
    assert n25["rows"] == (0, 152) and n25["data"] == (None, 151)
    assert (PF.grid_lat(n25, 0), PF.grid_lat(n25, 151)) == (90.0, 52.25)
    # between the data rows of two neighbours: never a gap wider than one row of the coarser grid, never an overlap
    assert 0 < PF.grid_lat(g16, g16["data"][1]) - PF.grid_lat(s25, s25["data"][0]) <= 1 / s25["per_deg"]
    assert 0 < PF.grid_lat(n25, n25["data"][1]) - PF.grid_lat(g16, g16["data"][0]) <= 1 / n25["per_deg"]
    for g in PF.GRIDS:                                                        # "data" lies inside what is stored
        assert all(d is None or g["rows"][0] <= d < g["rows"][1] for d in g["data"])
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


def test_fetch_file_downloads_again_after_a_damaged_body_or_a_transport_error(monkeypatch):
    seen, naps = {"n": 0}, []
    monkeypatch.setattr(PT.time, "sleep", naps.append)

    def req(url, timeout=60, tries=4, **kw):
        seen["url"], seen["timeout"], seen["tries"] = url, timeout, tries
        seen["n"] += 1
        return 200, _grib(40) + b"GRIB-cut"
    monkeypatch.setattr(PT.F, "_request", req)
    with pytest.raises(F.TransportError, match="3 downloads"):
        PT.fetch_file(RUN, PF.GRIDS[1], 123)
    assert seen["url"] == "https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20261001/12/wave/gridded/gfswave.t12z.gsouth.0p25.f123.grib2"
    assert seen["timeout"] == PT.FILE_TIMEOUT_S == 120 and seen["n"] == PT.FILE_TRIES == 3 and naps == [5, 10]
    assert seen["tries"] == PT.REQUEST_TRIES == 2                              # a stalled file: 3 x 2 x 120 s, not 3 x 4 x 180 s
    monkeypatch.setattr(PT.F, "_request", lambda url, **kw: (200, _grib(40) + _grib(60)))
    assert len(PT.fetch_file(RUN, PF.GRIDS[0], 0)) == 2
    answers = [F.TransportError("reset"), (200, _grib(40)[:-3]), (200, _grib(40))]   # a reset, a short body, then the file

    def flaky(url, **kw):
        a = answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a
    monkeypatch.setattr(PT.F, "_request", flaky)
    assert len(PT.fetch_file(RUN, PF.GRIDS[0], 0)) == 1 and answers == []
    calls = []

    def gone(url, **kw):
        calls.append(url)
        raise F.NotReady("404 " + url)
    monkeypatch.setattr(PT.F, "_request", gone)
    with pytest.raises(F.NotReady):
        PT.fetch_file(RUN, PF.GRIDS[0], 0)
    assert len(calls) == 1                                                    # not there is not retried here


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
    world.alter = None
    # ONE used record laid out another way (same size: rows south to north) stops the file; an unused one does not
    world.meta = lambda grid, name, meta: dict(meta, jScansPositively=1) if name == "s3_d" else meta
    with pytest.raises(ValueError, match="rows north to south"):
        PT.decode_file(msgs, g, RUN, 6)
    world.meta = lambda grid, name, meta: dict(meta, forecastTime=9) if name == "wind" else meta
    with pytest.raises(ValueError, match="forecast time 9"):
        PT.decode_file(msgs, g, RUN, 6)
    world.meta = lambda grid, name, meta: dict(meta, jScansPositively=1, forecastTime=9) if name is None else meta
    assert set(PT.decode_file(msgs, g, RUN, 6)) == set(PF.FIELD_NAMES)


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
    complete = {datetime(2026, 10, 1, 6, tzinfo=timezone.utc), datetime(2026, 9, 30, 18, tzinfo=timezone.utc),
                datetime(2026, 9, 30, 0, tzinfo=timezone.utc)}
    monkeypatch.setattr(PT, "run_is_complete", lambda dt: dt in complete)
    # the 12Z cycle is not complete yet: the NEWEST complete one, not any complete one
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
    assert (s25["rows"], s25["data_rows"], s25["lat_north"], s25["lat_south"]) == ([3, 14], [3, 13], -11.25, -13.75)
    assert (g16["data_rows"], g16["lat_north"], n25["data_rows"], n25["lat_south"]) == ([0, 19], 52.5, [0, 5], 88.75)
    assert all((g["registration"], g["lon_periodic"]) == ("center", True) for g in stored["grids"])
    assert all(g["ni"] <= PF.MAX_MASK_NI and g["nj"] <= PF.MAX_MASK_NJ for g in PF.GRIDS) and len(PF.GRIDS) == 3
    assert (stored["order"], stored["dtype"], stored["codec"]) == ("field,step,cell", "<u2", "xz")
    assert "most significant bit" in stored["bitmap"] and "earlier grid" in stored["nearest"]
    assert stored["sea_cells"] == {"rule": stored["sea_cells"]["rule"], "grid_steps": 0, "lost_max": 0, "extra_max": 0}
    assert "first step" in stored["sea_cells"]["rule"]
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
    assert {k: man["sea_cells"][k] for k in ("grid_steps", "lost_max", "extra_max")} == {"grid_steps": 1, "lost_max": 1, "extra_max": 1}


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
    # the three scratch files are counted TOGETHER (they are sparse when created: each alone would always fit)
    monkeypatch.setattr(PT, "SCRATCH_SLACK", 0)
    sea = [int((~land(g))[g["rows"][0]:g["rows"][1]].sum()) for g in SMALL]
    need = sum(2 * 15 * n * 2 for n in sea)                                   # 2 steps, 15 fields, uint16
    monkeypatch.setattr(PT.shutil, "disk_usage", lambda p: type("U", (), {"free": int(need * 1.05) - 1})())
    with pytest.raises(RuntimeError, match="scratch space"):
        build(None, [0, 3], tmp_path, upload=False)
    assert not list(tmp_path.glob("*.u16"))                                   # refused before any file was made
    monkeypatch.setattr(PT.shutil, "disk_usage", lambda p: type("U", (), {"free": int(need * 1.05) + 1})())
    build(None, [0, 3], tmp_path, upload=False)


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
    for run in ("2026093006", "2026100112"):                                  # a partial manifest is not a manifest
        c.put_object("b", f"{PT.PREFIX}/{run}/partial-20261001T000000Z.json", b"{}", "application/json", "")
        c.put_object("b", f"{PT.PREFIX}/{run}/stats-20261001T000000Z.json", b"{}", "application/json", "")
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
    assert run("--steps", "1,0", "--dry-run") == 2 and run("--steps", "1,1", "--dry-run") == 2   # increasing, each once
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
    assert PT.format_key() == "948ea572f7cc5a7d"
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
# ------------------------------- G22a: what the review asked the suite to hold ----------------

def test_golden_bytes_of_a_tile_and_a_mask_read_without_numpy():
    """The format's bytes, pinned: a reader written from the description alone (no numpy, no pointfmt) must read
    what the writer wrote. Writer and reader changing together (bit order, byte order, plane order) fails here."""
    import lzma
    import struct
    import zlib
    grid = dict(PF.GRIDS[0], tile=3)
    bitmap = np.array([[1, 0, 1], [0, 1, 1]], bool)
    planes = (np.arange(15 * 2 * 4, dtype=np.uint16) * 257 + 3).reshape(15, 2, 4)      # both bytes of every value differ
    blob = PF.encode_tile(PF.tile_header(RUNKEY, grid, 5, 7, 2, 3, 4, 2), bitmap, planes)
    assert blob[:4] == b"APT1"
    (hlen,) = struct.unpack("<I", blob[4:8])
    header = json.loads(blob[8:8 + hlen])
    assert header == {"format": "apt1", "run": RUNKEY, "grid": "g16", "tile": [5, 7], "row0": 15, "col0": 21, "rows": 2,
                      "cols": 3, "cells": 4, "steps": 2, "fields": list(PF.FIELD_NAMES)}
    assert blob[8 + hlen] == 0b10101100                                       # first cell in the most significant bit
    payload = blob[8 + hlen + 1:]
    assert payload[:6] == b"\xfd7zXZ\x00"                                    # one xz stream
    raw = lzma.decompress(payload, format=lzma.FORMAT_XZ)
    values = struct.unpack("<%dH" % (15 * 2 * 4), raw)                        # little-endian uint16
    for f in range(15):
        for s in range(2):
            for c in range(4):
                assert values[(f * 2 + s) * 4 + c] == int(planes[f, s, c])    # [field, step, cell]
    assert raw[:4] == b"\x03\x00\x04\x01"                                     # 3, then 260: low byte first
    cells = [(r, c) for r in range(2) for c in range(3) if (blob[8 + hlen] >> (7 - (r * 3 + c))) & 1]
    assert cells == [(0, 0), (0, 2), (1, 1), (1, 2)] and PF.cell_index(bitmap, 1, 1) == 2
    small = {"name": "s25", "ni": 5, "nj": 2}
    mask = np.array([[1, 0, 0, 1, 1], [0, 0, 1, 0, 1]], bool)
    mblob = PF.encode_mask(RUNKEY, small, mask)
    (mlen,) = struct.unpack("<I", mblob[4:8])
    assert mblob[:4] == b"APM1" and json.loads(mblob[8:8 + mlen]) == {"format": "apt1", "run": RUNKEY, "grid": "s25", "ni": 5, "nj": 2, "cells": 5}
    assert zlib.decompress(mblob[8 + mlen:]) == bytes([0b10011001, 0b01000000])   # row-major, padded with zero bits


def test_decoders_are_bounded_and_only_ever_raise_value_error():
    """G22a P2-1: these run in the web server. Never more than MAX_TILE_BYTES unpacked, the xz dictionary capped,
    xz only, plain ints only, and nothing but ValueError for any damaged object."""
    import lzma
    import struct
    import tracemalloc
    import zlib
    header, bitmap, planes = _tile()
    good = PF.encode_tile(header, bitmap, planes)
    hlen = struct.unpack("<I", good[4:8])[0]
    nb = (bitmap.size + 7) // 8
    bits, body = good[8 + hlen:8 + hlen + nb], good[8 + hlen + nb:]

    def tile(h, payload=body, bitmap_bytes=bits, raw_header=None):
        hj = raw_header if raw_header is not None else json.dumps(h, separators=(",", ":")).encode()
        return b"APT1" + struct.pack("<I", len(hj)) + hj + bitmap_bytes + payload

    def peak_of(fn):
        tracemalloc.start()
        try:
            with pytest.raises(ValueError):
                fn()
            return tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()

    # the largest claim the header limits would allow is refused before the payload is touched
    big = dict(header, rows=64, cols=64, cells=4096, steps=1024, fields=["f%d" % i for i in range(64)])
    zeros = lzma.compress(bytes(8 << 20))
    assert peak_of(lambda: PF.decode_tile(tile(big, payload=zeros, bitmap_bytes=b"\xff" * 512))) < 4 << 20
    assert 15 * 209 * 576 * 2 < PF.MAX_TILE_BYTES == 16 << 20 and PF.XZ_MEMLIMIT == 64 << 20 and PF.MAX_HEADER_BYTES == 4096
    # a tiny stream that names a 1 GiB dictionary: liblzma is not allowed to allocate it
    size = (body[12] + 1) * 4                                                 # the xz block header: its LZMA2 dictionary byte
    head = bytearray(body[12:12 + size - 4])
    assert head[1] == 0 and head[2] == 0x21 and head[3] == 1                  # one filter (LZMA2), one property byte
    head[4] = 36                                                              # (2 | 0) << (36 / 2 + 11) = 1 GiB
    huge_dict = body[:12] + bytes(head) + struct.pack("<I", zlib.crc32(bytes(head))) + body[12 + size:]
    assert peak_of(lambda: PF.decode_tile(tile(header, payload=huge_dict))) < 32 << 20
    cases = [
        tile(header, payload=lzma.compress(planes.tobytes(), format=lzma.FORMAT_ALONE)),      # legacy .lzma is not xz
        tile(header, payload=body + lzma.compress(b"")),                                      # a second stream
        tile(None, raw_header=b'{"format":"apt1","rows":Infinity}'), tile(None, raw_header=b'{"format":"apt1","rows":NaN}'),
        tile(None, raw_header=b'{"format":"apt1","rows":1e400}'),
        tile(None, raw_header=b"[" * 2000 + b"]" * 2000), tile(None, raw_header=b"\xff\xfe"), tile(None, raw_header=b"{"),
        tile(None, raw_header=b'"apt1"'), tile(None, raw_header=b" " * 5000),
        tile(dict(header, rows=2.0)), tile(dict(header, rows=True)), tile(dict(header, cols="6")), tile(dict(header, steps=5.5)),
        tile(dict(header, cells=None)), tile(dict(header, rows=65)), tile(dict(header, cols=0)), tile(dict(header, steps=1025)),
        tile(dict(header, steps=-1)), tile(dict(header, cells=header["rows"] * header["cols"] + 1)),
        tile(dict(header, fields="hs")), tile(dict(header, fields={"hs": 1})), tile(dict(header, fields=[1, 2])),
        tile(dict(header, fields=[])), tile(dict(header, fields=["f"] * 65)),
        # the right COUNT of fields, but not a list of names
        tile(dict(header, fields="abcdefghijklmno")), tile(dict(header, fields={str(i): i for i in range(15)})),
        tile(dict(header, fields=list(range(15)))), tile(dict(header, fields=list(PF.FIELD_NAMES[:14]) + [None])),
        tile(dict(header, fields=list(PF.FIELD_NAMES[:14]) + ["hs"])),                        # a name twice
        # where the tile says it is: typed, or refused (whether it is the tile asked for is the reader's check)
        tile(dict(header, tile="x")), tile(dict(header, tile=[1])), tile(dict(header, tile=[1, 2.0])), tile(dict(header, tile=[-1, 2])),
        tile(dict(header, row0=None)), tile(dict(header, col0="18")), tile(dict(header, row0=-24)),
        tile(dict(header, run=2026100112)), tile({k: v for k, v in header.items() if k != "grid"}),
    ]
    one_h, one_b, one_p = _tile(steps=1)                                       # true is not 1: a bool is not a count
    one = PF.encode_tile(one_h, one_b, one_p)
    olen = struct.unpack("<I", one[4:8])[0]
    assert PF.decode_tile(one)[0]["steps"] == 1
    cases.append(b"APT1" + struct.pack("<I", olen + 3) + one[8:8 + olen].replace(b'"steps":1', b'"steps":true') + one[8 + olen:])
    assert b'"steps":true' in cases[-1]
    for i, blob in enumerate(cases):
        try:
            PF.decode_tile(blob)
        except ValueError:
            continue
        except Exception as exc:                                              # noqa: BLE001
            raise AssertionError(f"case {i}: {exc.__class__.__name__} instead of ValueError")
        raise AssertionError(f"case {i} was accepted")
    # what the reader asks for must be what the tile holds
    assert PF.decode_tile(good, steps=header["steps"], fields=PF.FIELD_NAMES)[0] == header
    for kw in ({"steps": header["steps"] + 1}, {"fields": PF.FIELD_NAMES[:-1]}, {"fields": list(reversed(PF.FIELD_NAMES))}):
        with pytest.raises(ValueError, match="asked for"):
            PF.decode_tile(good, **kw)
    with pytest.raises(ValueError, match="too large"):
        PF.encode_tile(dict(header, steps=1024, cells=24, rows=4, cols=6, fields=["f%d" % i for i in range(64)] * 6),
                       np.ones((4, 6), bool), np.zeros((384, 1024, 24), np.uint16))
    # the mask: same rules
    grid = PF.GRIDS[1]
    mgood = PF.encode_mask(RUNKEY, grid, np.ones((grid["nj"], grid["ni"]), bool))
    mlen = struct.unpack("<I", mgood[4:8])[0]
    mh = json.loads(mgood[8:8 + mlen])

    def mask(h, payload=mgood[8 + mlen:], raw_header=None):
        hj = raw_header if raw_header is not None else json.dumps(h, separators=(",", ":")).encode()
        return b"APM1" + struct.pack("<I", len(hj)) + hj + payload
    assert PF.decode_mask(mask(mh))[1].all()
    bomb = mask(mh, payload=zlib.compress(bytes(200 << 20)))                  # 200 MB of zeros behind a 50 KB claim
    assert peak_of(lambda: PF.decode_mask(bomb)) < 8 << 20
    claim = mask(dict(mh, ni=100000, nj=100000, cells=0), payload=zlib.compress(bytes(200 << 20)))   # ... or behind a huge one
    assert peak_of(lambda: PF.decode_mask(claim)) < 8 << 20
    largest = mask(dict(mh, ni=PF.MAX_MASK_NI, nj=PF.MAX_MASK_NJ, cells=0), payload=zlib.compress(bytes(PF.MAX_MASK_NI * PF.MAX_MASK_NJ // 8)))
    tracemalloc.start()
    assert not PF.decode_mask(largest)[1].any()                               # the largest mask a header may claim: accepted,
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    assert peak < 24 << 20, peak                                              # and cheap (it was 63 MiB at 8192 x 4096)
    assert (PF.MAX_MASK_NI, PF.MAX_MASK_NJ) == (4096, 2048)
    full = zlib.compress(np.packbits(np.ones((grid["nj"], grid["ni"]), bool)).tobytes(), 9)
    co = zlib.compressobj()
    open_ended = co.compress(np.packbits(np.ones((grid["nj"], grid["ni"]), bool)).tobytes()) + co.flush(zlib.Z_SYNC_FLUSH)
    mcases = [mask(mh, payload=open_ended),                                    # all the bytes, but the stream never ends
              mask(mh, payload=full + b"x"), mask(dict(mh, ni=4097)), mask(dict(mh, nj=2049)), mask(dict(mh, ni=1440.0)),
              mask(dict(mh, cells=-1)), mask(dict(mh, cells="5")), mask(None, raw_header=b'{"format":"apt1","ni":Infinity}'),
              mask(None, raw_header=b"[" * 2000 + b"]" * 2000)]
    for i, blob in enumerate(mcases):
        try:
            PF.decode_mask(blob)
        except ValueError:
            continue
        except Exception as exc:                                              # noqa: BLE001
            raise AssertionError(f"mask case {i}: {exc.__class__.__name__} instead of ValueError")
        raise AssertionError(f"mask case {i} was accepted")


def test_ties_go_up_whatever_float64_makes_of_them():
    ties = np.arange(5, 3000, 10) / 100.0                                      # 0.05, 0.15, ... 29.95
    below = np.nextafter(ties, 0)                                              # what (R + X * 2^E) / 10^D can give
    assert PF.quantise(below, "period").tolist() == list(range(1, 301))
    assert PF.quantise(np.nextafter(np.arange(0, 360) + 0.5, 0), "direction").tolist() == list(range(1, 360)) + [0]
    assert PF.quantise([7.4499, 7.4501], "period").tolist() == [74, 75]        # only a tie, not what is near one
    assert PF.TIE == 1e-6


def test_plausible_ranges_per_kind_and_geometry_tolerances(world):
    g = SMALL[0]
    msgs = world.fetch_file(RUN, g, 0)
    for name, factor, kind in (("s2_t", 4.0, "period"), ("ws_d", 1.2, "direction"), ("wind", 6.0, "speed"), ("hs", 5.0, "height")):
        world.alter = lambda grid, n, step, arr, name=name, factor=factor: arr * factor if n == name else arr
        with pytest.raises(ValueError, match=f"{name}.*not a {kind}"):
            PT.decode_file(msgs, g, RUN, 0)
    world.alter = None
    assert PT.PLAUSIBLE_MAX == {"height": 40.0, "period": 60.0, "direction": 360.0001, "speed": 150.0}
    for grid in PF.GRIDS:
        ok = meta_for(grid, (10, 0, 3, 1, None), 0)
        PT.check_geometry(ok, grid)
        for key, off in (("iDirectionIncrementInDegrees", 0.00007), ("jDirectionIncrementInDegrees", -0.00004),   # 1/6 is 0.166667
                         ("longitudeOfFirstGridPointInDegrees", 0.01), ("longitudeOfLastGridPointInDegrees", -0.013),
                         ("latitudeOfFirstGridPointInDegrees", -0.001), ("latitudeOfLastGridPointInDegrees", 0.01)):
            with pytest.raises(ValueError):
                PT.check_geometry({**ok, key: ok[key] + off}, grid)
            PT.check_geometry({**ok, key: ok[key] + off / 1000}, grid)         # NOAA's own rounding passes


def test_a_change_of_noaas_bands_stops_the_build(world, tmp_path):
    """G22a P1-1: the rows with waves must be the ones the grids were cut for; otherwise a hole opens unseen."""
    def empty_row(gname, row):
        def alter(grid, name, step, arr):
            if grid["name"] == gname and name == "hs":
                arr = arr.copy()
                arr[row] = np.nan
            return arr
        return alter
    for gname, row in (("g16", 0), ("g16", 19), ("s25", 3), ("n25", 5)):       # an edge row the neighbours rely on
        world.alter = empty_row(gname, row)
        with pytest.raises(ValueError, match="grid bands have changed"):
            build(None, [0], tmp_path, upload=False)
        assert not list(tmp_path.glob("*.u16"))
    for gname, row in (("s25", 13), ("n25", 0)):                               # the far edges move with the ice: fine
        world.alter = empty_row(gname, row)
        man = build(None, [0], tmp_path, upload=False)
        got = {g["name"]: g for g in man["grids"]}[gname]
        assert got["data_rows"] == ([3, 12] if gname == "s25" else [1, 5])
        # the manifest's latitudes are those of the rows with data, not of the stored band
        assert (got["lat_north"], got["lat_south"]) == ((-11.25, -13.5) if gname == "s25" else (89.75, 88.75))


def test_check_band_on_the_real_grids():
    g16 = PF.GRIDS[0]                                                          # rows 2..390, exactly
    mask = np.zeros((g16["nj"], g16["ni"]), bool)
    mask[2:391, 100] = True
    assert PT.check_band(mask, g16) == (2, 390)
    for rows in ((1, 391), (2, 390), (3, 391), (2, 392)):
        bad = np.zeros_like(mask)
        bad[rows[0]:rows[1], 100] = True
        with pytest.raises(ValueError, match="expected 2..390"):
            PT.check_band(bad, g16)
    s25 = PF.GRIDS[1]
    ms = np.zeros((s25["nj"], s25["ni"]), bool)
    ms[9:270, 5] = True
    assert PT.check_band(ms, s25) == (9, 269)
    ms[9, 5] = False
    with pytest.raises(ValueError, match="expected 9..None"):
        PT.check_band(ms, s25)


def test_sea_cells_that_move_too_much_within_a_run_fail_the_build(world, tmp_path, monkeypatch):
    g = SMALL[0]
    sea = np.argwhere(~land(g))

    def lose(n):
        def alter(grid, name, step, arr):
            if (grid["name"], name, step) == ("g16", "hs", 3):
                arr = arr.copy()
                for cell in sea[:n]:
                    arr[tuple(cell)] = np.nan
            return arr
        return alter
    limit = int(PT.DRIFT_FAIL * len(sea))
    assert PT.DRIFT_FAIL == 0.01 and limit >= 2
    world.alter = lose(limit)                                                  # at the limit: published, and said
    man = build(None, [0, 3], tmp_path, upload=False)
    assert man["sea_cells"]["lost_max"] == limit and man["sea_cells"]["grid_steps"] == 1
    world.alter = lose(limit + 1)
    client = FakeClient()
    with pytest.raises(ValueError, match="not the first step's any more"):
        build(P.Store(client, "b"), [0, 3], tmp_path)
    assert client.log == []                                                    # nothing was uploaded
    dry = np.argwhere(land(g))

    def gain(grid, name, step, arr):                                           # waves appear on what was land at the first step
        if (grid["name"], name, step) == ("g16", "hs", 3):
            arr = arr.copy()
            for cell in dry[:limit + 1]:
                arr[tuple(cell)] = 1.0
        return arr
    world.alter = gain
    with pytest.raises(ValueError, match=f"0 sea cells without waves and {limit + 1} extra"):
        build(P.Store(client, "b"), [0, 3], tmp_path)
    assert client.log == []


def test_the_first_failure_is_raised_at_once_and_the_rest_is_dropped():
    import time
    from concurrent.futures import ThreadPoolExecutor
    started = []

    def job(i):
        started.append(i)
        if i == 1:
            raise F.NotReady("404 the second file")
        time.sleep(1.0 if i == 0 else 0.05)                                    # the FIRST job is the slow one
        return i
    ex = ThreadPoolExecutor(2)
    t = time.time()
    with pytest.raises(F.NotReady):
        list(PT._results(ex, job, list(range(200))))
    seen, took = len(started), time.time() - t
    ex.shutdown(wait=True)
    assert seen < 8 and took < 0.8                                             # raised when it failed, not after the job before it
    assert len(started) < 12                                                   # and the jobs not yet started never ran
    ex = ThreadPoolExecutor(3)
    assert sorted(PT._results(ex, lambda i: i * 2, [1, 2, 3, 4])) == [2, 4, 6, 8]
    ex.shutdown()
    assert list(PT._results(PT.Inline(), lambda i: i + 1, [1, 2])) == [2, 3]   # in this process: in order, lazily
    seen = []

    def boom(i):
        seen.append(i)
        if i == 2:
            raise ValueError("stop")
    with pytest.raises(ValueError):
        list(PT._results(PT.Inline(), boom, [1, 2, 3, 4]))
    assert seen == [1, 2]


class SlowClient(FakeClient):
    """Every upload takes a moment, as on a real network; chosen keys fail after it."""

    def __init__(self, delay=0.004, fail=()):
        super().__init__()
        self.delay, self.fail = delay, tuple(fail)

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        import time
        if Key.endswith(".bin"):
            time.sleep(self.delay)
        if any(Key.endswith(f) for f in self.fail):
            raise RuntimeError(f"R2 refused {Key}")
        super().put_object(Bucket, Key, Body, ContentType, CacheControl)


def test_every_tile_and_mask_is_stored_before_the_stats_and_the_manifest_with_slow_uploads(world, tmp_path, monkeypatch):
    """G22a P2-2: with uploads still in flight when the last tile is cut, the manifest must wait for them."""
    monkeypatch.setattr(PT.F, "STEPS", [0, 3])
    client = SlowClient()
    man = build(P.Store(client, "b"), [0, 3], tmp_path)
    keys = [k for _op, k in client.log]
    bins = [k for k in keys if k.endswith(".bin")]
    assert len(bins) == sum(g["tiles"] for g in man["grids"]) + 3 == man["_objects"]["count"]
    first_json = min(i for i, k in enumerate(keys) if k.endswith(".json"))
    assert first_json == len(bins) and keys[-1] == PT.LATEST_KEY               # all of them, then stats, manifest, pointer
    assert man["_pointed"] is True


@pytest.mark.parametrize("last", ["/n25/mask.bin", "/n25/1_7.bin", "/g16/mask.bin"])
def test_a_failure_of_one_of_the_last_uploads_stops_the_build(world, tmp_path, monkeypatch, last):
    """The objects handed over last are checked by the final drain alone: a slow failure there must still stop the
    manifest and the pointer (the frames job's G12 P0-1)."""
    monkeypatch.setattr(PT.F, "STEPS", [0, 3])
    client = SlowClient(delay=0.02, fail=(last,))
    with pytest.raises(RuntimeError, match="R2 refused"):
        build(P.Store(client, "b"), [0, 3], tmp_path)
    assert not any(k.endswith(".json") for k in client.objects)


def test_a_late_upload_failure_reaches_main_as_a_failure_record(cli, monkeypatch):
    client, run = cli
    slow = SlowClient(delay=0.02, fail=("/n25/mask.bin",))
    monkeypatch.setattr(PT, "_store_from_env", lambda: P.Store(slow, "the-bucket"))
    with pytest.raises(RuntimeError, match="R2 refused"):
        run()
    assert json.loads(slow.objects[PT.failed_key(RUNKEY)]["body"])["attempts"] == 1
    assert PT.LATEST_KEY not in slow.objects and not any("manifest" in k or "stats" in k for k in slow.objects)


def test_uploads_never_run_further_behind_than_the_backlog():
    import time

    class Slow:
        bucket = "b"

        def put(self, key, body, ct, cc):
            time.sleep(0.01)
    up = PT.Uploads(Slow(), workers=2, backlog=3)
    worst = 0
    for i in range(30):
        up.put(f"k{i}", b"x")
        worst = max(worst, len(up.pending))
    up.drain(0)
    up.close()
    assert worst <= 3 and up.pending == []
    assert (PT.UPLOAD_BACKLOG, PT.UPLOAD_WORKERS, PT.COMPRESS_WORKERS, PT.WORKERS) == (64, 8, 4, 4)


def test_existing_guard_only_repairs_to_a_whole_newer_run(cli):
    client, _run = cli
    store = P.Store(client, "b")
    key = _manifest(client)
    assert PT.existing_guard(store, RUNKEY, "2026100118", False) is None and PT.LATEST_KEY not in client.objects   # never back
    assert PT.existing_guard(store, RUNKEY, RUNKEY, False) is None and PT.LATEST_KEY not in client.objects
    for bad in ({"complete": False}, {"run": "2026100106"}, {"published_utc": None}, {"steps": "all"}):
        _manifest(client, **bad)
        assert PT.existing_guard(store, RUNKEY, None, False) is None and PT.LATEST_KEY not in client.objects, bad
    _manifest(client)
    assert PT.existing_guard(store, RUNKEY, "2026100106", False) == 0
    assert json.loads(client.objects[PT.LATEST_KEY]["body"])["manifest"] == key
    assert PT.existing_guard(store, "2026100118", None, False) is None        # a run with no manifest: build it
    del client.objects[PT.LATEST_KEY]
    assert PT.existing_guard(store, RUNKEY, "2026100106", True) is None and PT.LATEST_KEY not in client.objects   # --force: rebuild
    assert (PT.NOTREADY_WARN, PT.FAILED_MAX_ATTEMPTS, PT.FAILED_RETRY_AFTER_S, PT.DRIFT_FAIL) == (6, 3, 3 * 3600, 0.01)


def test_a_pointer_write_that_failed_is_repaired_on_the_next_tick_not_after_the_back_off(cli, capsys):
    """G22a P3-4: the tiles and the manifest are stored; one PUT is missing. The failure record must not make the
    next tick wait three hours for it."""
    client, run = cli
    client.fail_put = lambda key: key == PT.LATEST_KEY
    with pytest.raises(RuntimeError, match="latest.json"):
        run()
    assert json.loads(client.objects[PT.failed_key(RUNKEY)]["body"])["attempts"] == 1
    assert any("manifest-" in k for k in client.objects) and PT.LATEST_KEY not in client.objects
    client.fail_put = None
    n = sum(1 for k in client.objects if k.endswith(".bin"))
    assert run() == 0
    assert json.loads(client.objects[PT.LATEST_KEY]["body"])["run"] == RUNKEY
    assert sum(1 for op, k in client.log if k.endswith(".bin")) == n           # repaired, not rebuilt
    assert "pointer repaired" in capsys.readouterr().out


def test_a_newer_run_that_went_live_during_the_build_keeps_the_pointer(cli, world, monkeypatch, tmp_path):
    client, run = cli
    newer = json.dumps({"run": "2026100118", "complete": True, "manifest": "m"}).encode()
    orig = world.fetch_file

    def fetch(run_dt, grid, step):                                            # another build finishes while this one decodes
        if (grid["name"], step) == ("n25", 2):
            client.put_object("b", PT.LATEST_KEY, newer, "application/json", "")
        return orig(run_dt, grid, step)
    monkeypatch.setattr(PT, "fetch_file", fetch)
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert run() == 0
    assert json.loads(client.objects[PT.LATEST_KEY]["body"])["run"] == "2026100118"
    assert any("manifest-" in k for k in client.objects)                      # this run is stored, just not pointed to
    text = summary.read_text()
    assert "went live while 2026100112 was building" in text and "latest.json does not name it" in text
    assert run("--force") == 0                                                 # --force says so explicitly, and moves it
    assert json.loads(client.objects[PT.LATEST_KEY]["body"])["run"] == RUNKEY


def test_a_partial_local_build_never_prunes_and_never_records_a_failure(cli, world, tmp_path):
    client, run = cli
    out = tmp_path / "out"
    lc = PT.LocalClient(str(out))
    for old in ("2026093000", "2026093006", "2026093012"):
        _seed(lc, PT.PREFIX, old)
    assert run("--local", str(out), "--steps", "0,1") == 0
    store = P.Store(lc, "local")
    assert set(PT.list_runs(store)) == {"2026093000", "2026093006", "2026093012", RUNKEY}
    assert store.get_json(PT.LATEST_KEY) is None
    world.fail = lambda grid, step: RuntimeError("boom") if step == 1 else None
    with pytest.raises(RuntimeError):
        run("--local", str(out), "--steps", "0,1")
    assert store.get_json(PT.failed_key(RUNKEY)) is None


def test_steps_argument_that_is_not_a_list_of_numbers_is_refused(cli):
    client, run = cli
    for bad in (",", "0,,1", "a", "0;1", ""):
        assert run("--steps", bad, "--dry-run") == (0 if bad == "" else 2), bad   # an empty value means "all"


def test_main_cleans_its_scratch_directory_and_shuts_the_executor_down(world, monkeypatch, tmp_path):
    monkeypatch.setattr(PT.F, "STEPS", [0, 1, 2])
    monkeypatch.setattr(PT, "latest_complete_run", lambda: RUN)
    made = tmp_path / "points-scratch"

    def mkdtemp(prefix=None, dir=None):
        made.mkdir()
        return str(made)
    monkeypatch.setattr(PT.tempfile, "mkdtemp", mkdtemp)
    shut = []

    class Recording(PT.Inline):
        def shutdown(self, **kw):
            shut.append(kw)
    monkeypatch.setattr(PT, "_executor", lambda workers: Recording())
    assert PT.main(["--dry-run"]) == 0
    assert not made.exists() and shut == [{"wait": False, "cancel_futures": True}]
    kept = tmp_path / "kept"
    assert PT.main(["--dry-run", "--work", str(kept)]) == 0 and (kept / "g16.u16").exists()   # a directory you named stays
    world.fail = lambda grid, step: RuntimeError("boom") if step == 2 else None
    with pytest.raises(RuntimeError):
        PT.main(["--dry-run"])
    assert not made.exists() and len(shut) == 3                                # also after a failure


def test_the_stats_and_the_digest_do_not_depend_on_the_order_the_jobs_finish_in(world, tmp_path):
    class Backwards:                                                           # worker processes finish in any order
        def map(self, fn, jobs):
            return reversed([fn(j) for j in jobs])

        def shutdown(self, **kw):
            pass
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    one = PT.build_and_publish(None, RUN, [0, 3, 6], str(tmp_path / "a"), PT.Inline(), upload=False, log=lambda *a: None)
    two = PT.build_and_publish(None, RUN, [0, 3, 6], str(tmp_path / "b"), Backwards(), upload=False, log=lambda *a: None)
    key = lambda man: [(s["step"], s["grid"]) for s in man["_stats"]["steps"]]   # noqa: E731
    assert key(one) == key(two) == [(s, g["name"]) for s in (0, 3, 6) for g in SMALL]
    assert one["digest"] == two["digest"]


def test_run_digest_tells_the_grids_apart():
    a = [{"grid": "g16", "step": 0, "crc": 1}, {"grid": "s25", "step": 0, "crc": 2}]
    b = [{"grid": "g16", "step": 0, "crc": 2}, {"grid": "s25", "step": 0, "crc": 1}]
    c = [{"grid": "g16", "step": 0, "crc": 1}, {"grid": "g16", "step": 3, "crc": 2}]
    d = [{"grid": "g16", "step": 0, "crc": 1}, {"grid": "n25", "step": 0, "crc": 2}]   # the same numbers on another grid
    assert len({PT.run_digest(a), PT.run_digest(b), PT.run_digest(c), PT.run_digest(d)}) == 4


def test_read_message_with_the_real_eccodes():
    """The one function that talks to eccodes, on a message eccodes itself builds (simple packing: no JPEG2000
    needed). Skipped where eccodes is not installed (the offline test workflow); the publisher has it."""
    eccodes = pytest.importorskip("eccodes")
    try:
        h = eccodes.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    except Exception as exc:                                                  # noqa: BLE001
        pytest.skip(f"no eccodes samples here: {exc}")
    grid = {"name": "t", "tag": "test", "ni": 8, "nj": 5, "lat0": 1.0, "per_deg": 4, "rows": (0, 5), "data": (0, 4), "tile": 4}
    want = np.arange(40, dtype=float) / 10 + 3.05                              # two decimals, ties at the stored tenth
    want[[0, 7, 13]] = 9999.0
    try:
        for k, v in (("discipline", 10), ("parameterCategory", 0), ("parameterNumber", 9), ("typeOfFirstFixedSurface", 241),
                     ("scaleFactorOfFirstFixedSurface", 0), ("scaledValueOfFirstFixedSurface", 2), ("dataDate", 20261001),
                     ("dataTime", 1200), ("indicatorOfUnitOfTimeRange", 1), ("forecastTime", 123), ("Ni", 8), ("Nj", 5),
                     ("jScansPositively", 0), ("iScansNegatively", 0), ("latitudeOfFirstGridPointInDegrees", 1.0),
                     ("latitudeOfLastGridPointInDegrees", 0.0), ("longitudeOfFirstGridPointInDegrees", 0.0),
                     ("longitudeOfLastGridPointInDegrees", 1.75), ("iDirectionIncrementInDegrees", 0.25),
                     ("jDirectionIncrementInDegrees", 0.25), ("bitmapPresent", 1), ("missingValue", 9999.0),
                     ("decimalScaleFactor", 2)):                                 # two decimals, as NOAA packs them
            eccodes.codes_set(h, k, v)
        eccodes.codes_set_values(h, want)
        msg = eccodes.codes_get_message(h)
    finally:
        eccodes.codes_release(h)
    assert PT.messages(msg) == [msg]
    meta, vals = PT.read_message(msg, lambda m: False)
    assert vals is None and PT.identify(meta) == "s2_t"
    PT.check_time(meta, RUN, 123)
    PT.check_geometry(meta, grid)
    with pytest.raises(ValueError):
        PT.check_time(meta, RUN, 120)
    meta, vals = PT.read_message(msg, lambda m: True)
    arr = np.where(vals == meta["missingValue"], np.nan, vals).reshape(5, 8)
    assert np.isnan(arr[0, 0]) and np.isnan(arr[0, 7]) and np.isnan(arr[1, 5]) and np.isnan(arr).sum() == 3
    assert arr[0, 1] == pytest.approx(3.15, abs=1e-6) and arr[4, 7] == pytest.approx(6.95, abs=1e-6)   # row-major, rows as given
    codes = PF.quantise(arr, "period")
    assert codes[0, 1] == 32 and codes[4, 7] == 70 and codes[0, 0] == PF.MISSING   # the ties go up after the packing


def test_points_workflow_text_pins():
    import re
    wf = os.path.join(ROOT, ".github", "workflows")
    text = open(os.path.join(wf, "model-points.yml"), encoding="utf-8").read()
    assert re.search(r"\npermissions:\n  contents: read\n", text) and "write" not in text.split("jobs:")[0]
    assert "timeout-minutes: 120" in text and re.search(r'cron: "3-59/10 \* \* \* \*"', text)
    for name in ("dry_run", "force"):                                          # true or false: no typo can turn into a publish
        assert re.search(rf"\n      {name}:\n        description: [^\n]+\n        type: boolean\n        default: false\n", text), name
    assert '[ -z "$STEPS" ] && [ "$DRY_RUN" = "true" ] && ARGS+=(--dry-run)' in text
    assert '[ "$FORCE" = "true" ] && ARGS+=(--force)' in text
    steps = text.split("    steps:\n", 1)[1].split("\n      - ")
    with_secrets = [s.split("\n", 1)[0] for s in steps if "secrets." in s]
    assert with_secrets == ["name: Publish points"]                            # no other step sees the R2 keys
    keep = open(os.path.join(wf, "model-frames-keepalive.yml"), encoding="utf-8").read()
    order = [keep.index(f"gh workflow enable {n}") for n in ("model-frames-keepalive.yml", "model-frames.yml", "model-points.yml")]
    assert order == sorted(order)                                              # itself first: a later failure cannot stop it
    tests = open(os.path.join(wf, "model-frames-tests.yml"), encoding="utf-8").read()
    for path in (".github/workflows/model-points.yml", ".github/workflows/model-frames-keepalive.yml", ".github/workflows/model-frames.yml"):
        assert path in tests.split("pull_request:")[0], path                   # a change of a pinned text runs the tests
        assert path in tests.split("pull_request:")[1].split("permissions:")[0], path
    assert sorted(f for f in os.listdir(wf) if "point" in f) == ["model-points.yml"]   # the dry-run helper of the review never ships
