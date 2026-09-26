"""Overlay frame job: pure-function tests (no network, no eccodes, no R2).

Pins the invariants the G1 adversarial review asked for: geometry/roll, half-res alignment,
quantiser boundaries in float64, manifest schema, completeness from listings, record guards,
pointer-only-when-complete, no pointer regression, prune protection, failure bookkeeping.
"""
import io
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pytest
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))

import decode as D   # noqa: E402
import encode as E   # noqa: E402
import fetch as F    # noqa: E402
import publish as P  # noqa: E402
import run as R      # noqa: E402

RUN = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)
META_OK = {"Ni": 1440, "Nj": 721, "latitudeOfFirstGridPointInDegrees": 90.0,
           "latitudeOfLastGridPointInDegrees": -90.0, "longitudeOfFirstGridPointInDegrees": 0.0,
           "iDirectionIncrementInDegrees": 0.25, "jDirectionIncrementInDegrees": 0.25,
           "jScansPositively": 0, "iScansNegatively": 0, "jPointsAreConsecutive": 0,
           "alternativeRowScanning": 0, "missingValue": 9999.0}


# ------------------------------- decode geometry -----------------------------------

def test_to_site_grid_rolls_longitude_and_keeps_rows():
    vals = np.arange(721 * 1440, dtype=np.float64)          # value = row*1440 + col_original
    g = D.to_site_grid(vals, META_OK)
    assert g.shape == (721, 1440) and g.dtype == np.float32
    assert g[0, 0] == 720 and g[0, 720] == 0 and g[0, 1439] == 719      # col0 = 180E == -180E, col720 = 0E
    assert g[720, 0] == 720 * 1440 + 720                                # rows untouched, row 720 = -90 S
    vals[5] = 9999.0
    assert np.isnan(D.to_site_grid(vals, META_OK)[0, 725])              # missing -> NaN, rolled too


@pytest.mark.parametrize("bad", [
    {"Ni": 1441}, {"jScansPositively": 1}, {"latitudeOfLastGridPointInDegrees": 90.0},
    {"longitudeOfFirstGridPointInDegrees": -180.0}, {"iScansNegatively": 1},
    {"jPointsAreConsecutive": 1}, {"alternativeRowScanning": 1}, {"iDirectionIncrementInDegrees": 0.5}])
def test_geometry_guards(bad):
    with pytest.raises(ValueError):
        D.to_site_grid(np.zeros(721 * 1440), {**META_OK, **bad})


def test_identity_guard():
    meta = {"shortName": "swh", "typeOfLevel": "surface", "level": 1, "stepRange": "24",
            "dataDate": 20260922, "dataTime": 1200}
    D.check_identity(meta, "HTSGW:surface", RUN, 24)
    for bad in ({"shortName": "perpw"}, {"stepRange": "27"}, {"dataDate": 20260921}, {"dataTime": 600}, {"level": 10}):
        with pytest.raises(ValueError):
            D.check_identity({**meta, **bad}, "HTSGW:surface", RUN, 24)
    wind = {"shortName": "10u", "typeOfLevel": "heightAboveGround", "level": 10, "stepRange": "0",
            "dataDate": 20260922, "dataTime": 1200}
    D.check_identity(wind, "UGRD:10 m above ground", RUN, 0)


def test_wind_speed_nan_propagates():
    u = np.array([[3.0, np.nan]], np.float32); v = np.array([[4.0, 1.0]], np.float32)
    w = D.wind_speed(u, v)
    assert w[0, 0] == 5.0 and np.isnan(w[0, 1]) and w.dtype == np.float32


# ------------------------------- encode / decode -----------------------------------

def test_quantize_boundaries_float64_bound():
    for name, f in E.FIELDS.items():
        lo, hi = f["lo"], f["hi"]
        g = np.linspace(lo, hi, 254 * 4 + 1, dtype=np.float32)          # includes exact code centres + ties
        q = E.quantize(g, lo, hi)
        back = E.dequantize(q, lo, hi)
        assert np.max(np.abs(back - g)) <= E.quantum(lo, hi) / 2 + 1e-8, name
        edge = np.array([lo, hi, lo - 1e-3, hi + 1e-3, -0.0, np.inf, -np.inf, np.nan], np.float32)
        expect = [1, 255, 1, 255, 1, 255, 1, 0]          # lo, hi, below, above, -0.0 (<= lo), +inf, -inf, NaN
        assert E.quantize(edge, lo, hi).tolist() == expect


def test_half_res_is_exact_subsample_with_same_origin():
    q = (np.arange(721 * 1440) % 251 + 1).astype(np.uint8).reshape(721, 1440)
    h = E.half_res(q)
    assert h.shape == (361, 720)
    for i, j in ((0, 0), (5, 7), (360, 719), (100, 0), (0, 719)):
        assert h[i, j] == q[2 * i, 2 * j]
    assert set(np.unique(h)) <= set(np.unique(q))                       # never blends


def test_encode_frame_png_roundtrip_and_stats():
    g = np.full((721, 1440), np.nan, np.float32)
    g[:, 700:] = 3.0
    g[0, 700] = 99.0                                                     # clamps high
    g[1, 700] = -1.0                                                     # clamps low (hs lo = 0)
    out = E.encode_frame(g, "hs", fill=False)
    full = np.array(Image.open(io.BytesIO(out["full"])))
    half = np.array(Image.open(io.BytesIO(out["half"])))
    assert full.shape == (721, 1440) and half.shape == (361, 720) and full.dtype == np.uint8
    assert np.array_equal(full, E.quantize(g, 0.0, 15.0)) and np.array_equal(half, E.half_res(full))
    s = out["stats"]
    assert s["valid_points"] == 721 * 740 and s["clamped_high"] == 1 and s["clamped_low"] == 1
    assert s["min"] == -1.0 and s["max"] == 99.0 and s["filled_points"] == 0
    assert b"gAMA" not in out["full"] and b"sRGB" not in out["full"] and b"iCCP" not in out["full"]


# ------------------------------- coastal fill ---------------------------------------

def _dilate(mask):
    """One 8-neighbour dilation, longitude periodic, nothing beyond the poles."""
    out = mask.copy()
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            sh = np.roll(mask, dx, axis=1)
            if dy == 1:
                sh = np.vstack([np.zeros((1, mask.shape[1]), bool), sh[:-1]])
            elif dy == -1:
                sh = np.vstack([sh[1:], np.zeros((1, mask.shape[1]), bool)])
            out |= sh
    return out


def test_fill_coast_only_touches_nan_and_uses_neighbour_mean():
    g = np.full((5, 6), np.nan, np.float32)
    g[2, 2], g[2, 3] = 2.0, 4.0
    before = g.copy()
    out, added = E.fill_coast(g, cells=1)
    assert np.array_equal(np.isnan(g), np.isnan(before)) and np.nanmax(np.abs(g - before)) == 0      # input untouched
    assert out.dtype == np.float64 and out[2, 2] == 2.0 and out[2, 3] == 4.0                      # model values kept
    assert out[1, 1] == 2.0 and out[1, 4] == 4.0 and out[1, 2] == 3.0 and out[3, 3] == 3.0       # means of present neighbours
    assert added.sum() == 10 and not added[2, 2] and np.isnan(out[0, 0])                         # one ring only
    out2, added2 = E.fill_coast(g, cells=2)
    assert added2.sum() == 28                                                                     # the whole 5x6 minus the two model cells
    assert out2[1, 1] == 2.0 and abs(out2[0, 0] - 2.0) < 1e-12                                   # pass 2 uses pass-1 values (Jacobi)
    assert abs(out2[0, 1] - np.mean([2.0, 3.0])) < 1e-12                                         # (1,1)=2 and (1,2)=3


def test_fill_coast_depth_and_periodic_columns():
    g = np.full((9, 20), np.nan)
    g[4, 0] = 5.0                                                                                # a model cell on the first column
    out, added = E.fill_coast(g, cells=4)
    assert out[4, 19] == 5.0 and out[4, 16] == 5.0 and out[4, 4] == 5.0                           # reaches 4 cells both ways across the seam
    assert np.isnan(out[4, 5]) and np.isnan(out[4, 15])                                         # and no further
    assert added.sum() == 9 * 9 - 1                                                              # rows 0..8 (the whole height), 9 columns
    g2 = np.full((3, 4), np.nan)
    g2[0, :] = 1.0                                                                               # a pole row: nothing wraps over the pole
    out2, _ = E.fill_coast(g2, cells=1)
    assert np.all(out2[1] == 1.0) and np.all(np.isnan(out2[2]))


def test_fill_coast_leaves_far_inland_nan_and_covers_the_half_grid():
    g = np.full((721, 1440), 1.5)
    g[200:400, 300:700] = np.nan                                                                 # a continent
    g[500:503, 1000:1004] = np.nan                                                               # an island
    g[:, 1436:] = np.nan
    g[:, :4] = np.nan                                                                            # land across the dateline
    g[0, :] = np.nan                                                                             # the pole row
    real = ~np.isnan(g)
    out, added = E.fill_coast(g)
    reach = real.copy()
    for _ in range(E.FILL_CELLS):
        reach = _dilate(reach)
    assert np.array_equal(~np.isnan(out), reach)                                                # exactly the cells within 4 of model data
    assert added.sum() == int(reach.sum() - real.sum())
    assert np.isnan(out[300, 500]) and not np.isnan(out[203, 500])                             # continent interior stays empty
    assert np.all(out[500:503, 1000:1004] == 1.5) and np.all(out[1:, 1436:] == 1.5) and np.all(out[1:, :4] == 1.5)              # island and dateline land fully covered
    # every cell within 3 of model data (where a coastline can lie) has its nearest half-grid node present,
    # so the half-resolution frame (full[::2, ::2]) draws it too
    near = real.copy()
    for _ in range(3):
        near = _dilate(near)
    rows, cols = np.nonzero(near)
    hr = np.minimum(np.round(rows / 2).astype(int) * 2, 720)
    hc = (np.round(cols / 2).astype(int) * 2) % 1440
    assert not np.isnan(out[hr, hc]).any()


def test_encode_frame_fill_before_half_and_stats():
    g = np.full((721, 1440), np.nan, np.float32)
    g[:, 700:710] = 3.0
    for name in ("hs", "tp"):
        out = E.encode_frame(g, name, allow=np.ones((721, 1440), bool))
        full = np.array(Image.open(io.BytesIO(out["full"])))
        half = np.array(Image.open(io.BytesIO(out["half"])))
        s = out["stats"]
        assert s["valid_points"] == 721 * 10 and s["filled_points"] == 721 * 8                   # 4 columns each side
        assert np.count_nonzero(full) == s["valid_points"] + s["filled_points"]
        assert np.array_equal(half, E.half_res(full)) and np.count_nonzero(half[:, 348:357]) == 361 * 9
        assert s["min"] == 3.0 and s["max"] == 3.0                                                # stats describe the model values
    w = E.encode_frame(g, "wind")
    assert w["stats"]["filled_points"] == 0                                                       # wind is never filled
    d = E.encode_frame(g, "hs")                                                                   # default: the real fill_allow mask
    assert 0 < d["stats"]["filled_points"] < 721 * 8


def test_fill_info_pins_fields_cells_version_and_key():
    assert E.FILL_INFO["fields"] == ["hs", "pdir", "tp"] and E.FILL_INFO["cells"] == E.FILL_CELLS == 4
    assert E.FILL_INFO["version"] == E.FILL_VERSION == 2
    assert E.FILL_INFO["methods"] == {"hs": "mean", "pdir": "nearest", "tp": "nearest"}          # angles: nearest, never a mean
    assert json.loads(json.dumps(E.FILL_INFO)) == E.FILL_INFO                                   # manifest-safe
    assert [n for n, f in E.FIELDS.items() if f["fill"]] == ["hs", "tp", "pdir"]
    before = dict(E.FILL_INFO, fields=["hs", "tp"], methods={"hs": "mean", "tp": "nearest"})    # the runs published before Phase B
    assert E.fill_key(before) != E.fill_key(E.FILL_INFO)                                        # the guard never rebuilds them with pdir
    assert E.FILL_INFO["mask"] == E.FILL_ALLOW_SHA256[:16]
    reworded = dict(E.FILL_INFO, note="other words", limit="x", methods={"tp": "nearest", "pdir": "nearest", "hs": "mean"})
    assert E.fill_key(reworded) == E.fill_key(E.FILL_INFO)                                      # wording and key order are not a different fill
    assert E.fill_key(dict(E.FILL_INFO, methods={"hs": "mean", "tp": "mean"})) != E.fill_key(E.FILL_INFO)   # a method change is
    assert E.fill_key(dict(E.FILL_INFO, mask="0" * 16)) != E.fill_key(E.FILL_INFO)             # a re-pinned mask is
    assert E.fill_key(None) is None and E.fill_key({}) is None
    assert E.fill_key({"fields": ["hs", "tp"], "cells": 4}) != E.fill_key(E.FILL_INFO)          # a v1 block


def test_fill_allow_mask_is_pinned_and_excludes_open_water_and_ice():
    import hashlib
    raw = open(E.FILL_ALLOW_PNG, "rb").read()
    assert hashlib.sha256(raw).hexdigest() == E.FILL_ALLOW_SHA256 and len(raw) < 20_000
    a = E.fill_allow()
    assert a.shape == (721, 1440) and a.dtype == bool and not a.flags.writeable
    assert "tier1.sha256=a4b97403aac09c0a" in Image.open(E.FILL_ALLOW_PNG).info["coast"]         # built from the published coast v1
    node = lambda lat, lon: a[int(round((90 - lat) / 0.25)), int(round((lon + 180) / 0.25)) % 1440]
    assert node(21.5, -158.0) and node(21.0, -157.9) and node(9.0, -79.5)                      # Oahu, 30 km off Honolulu, Panama
    assert not node(10.0, -150.0) and not node(-65.0, -30.0) and not node(85.0, 0.0)            # open Pacific, Weddell ice, Arctic pack
    assert node(-80.0, 179.75) and node(-80.0, -180.0)                                          # the Ross Ice Shelf front, both sides of the dateline


def test_fill_coast_allow_limits_the_set_and_nearest_never_blends():
    g = np.full((9, 12), np.nan)
    g[:, :3] = 6.9                                                                                # a Caribbean
    g[:, 9:] = 14.4                                                                               # a Pacific, 6 cells away
    allow = np.ones(g.shape, bool)
    allow[:, 5] = False                                                                           # a strip the fill may not write
    mean, am = E.fill_coast(g, allow=allow)
    near, an = E.fill_coast(g, allow=allow, method="nearest")
    assert np.array_equal(am, an)                                                                 # the same filled set for both methods
    assert not am[:, 5].any() and am[:, 3:5].all() and am[:, 6:9].all()
    assert set(np.unique(near[an]).tolist()) == {6.9, 14.4}                                     # nearest: only model values, never a blend
    assert np.all(near[:, 3:5] == 6.9) and np.all(near[:, 6:9] == 14.4)
    g2 = np.full((5, 7), np.nan)
    g2[2, 0], g2[2, 6] = 6.9, 14.4
    mid, _ = E.fill_coast(g2, cells=3)
    nr, _ = E.fill_coast(g2, cells=3, method="nearest")
    assert 6.9 < mid[2, 3] < 14.4 and nr[2, 3] in (6.9, 14.4)                                   # the mean blends two regimes; nearest does not
    with pytest.raises(ValueError):
        E.fill_coast(g2, method="median")


def test_fill_nearest_is_the_euclidean_nearest_model_cell():
    rng = np.random.default_rng(7)
    rows, cols, W = 40, 64, 9
    g = np.where(rng.random((rows, cols)) < 0.04, rng.uniform(1, 20, (rows, cols)), np.nan)
    g[:, :6] = 3.0                                                                                # a coast, and sparse islands of model cells
    out, added = E.fill_coast(g, method="nearest")
    offs = sorted(((dy, dx) for dy in range(-W, W + 1) for dx in range(-W, W + 1) if dy or dx),
                  key=lambda o: (o[0] ** 2 + o[1] ** 2, abs(o[0]), o[0], o[1]))
    checked = 0
    for i, j in zip(*np.nonzero(added)):                                                          # brute force, wide window, same tie order
        for dy, dx in offs:
            ii, jj = i + dy, (j + dx) % cols
            if 0 <= ii < rows and not np.isnan(g[ii, jj]):
                assert out[i, j] == g[ii, jj], (i, j, dy, dx)
                checked += 1
                break
    assert checked == added.sum() > 500
    far = np.full((12, 12), np.nan)
    far[0, 0], far[5, 5] = 1.0, 2.0                                                               # (4,4) is Chebyshev 4 but Euclidean 5.66 from (0,0) ...
    far[9, 4] = 9.0                                                                               # ... while (9,4) sits at Euclidean 5.0 from (4,4)
    o2, a2 = E.fill_coast(far, cells=4, method="nearest")
    assert o2[4, 4] == 2.0                                                                        # (5,5) is nearer still: d = 1.41
    far[5, 5] = np.nan
    o3, _ = E.fill_coast(far, cells=4, method="nearest")
    assert o3[4, 4] == 9.0                                                                        # beyond the Chebyshev-4 window, found by the sqrt(2) window


def test_encoding_ranges_cover_legend_and_tp_floor():
    for f in E.FIELDS.values():
        assert f["lo"] <= f["legend"][0] and f["hi"] >= f["legend"][1]
    assert E.FIELDS["tp"]["lo"] <= 1.09                                 # WW3 physical floor
    assert {n: f["interpolation"] for n, f in E.FIELDS.items()} == {"hs": "bilinear", "tp": "bilinear", "wind": "bilinear",
                                                                   "pdir": "circular", "wdir": "circular"}
    assert {n: tuple(f["resolutions"]) for n, f in E.FIELDS.items()} == {"hs": ("full", "half"), "tp": ("full", "half"),
                                                                        "wind": ("full", "half"), "pdir": ("full", "half"), "wdir": ("half",)}
    for n in ("pdir", "wdir"):
        f = E.FIELDS[n]
        assert f["circular"] is True and f["convention"] == "from" and (f["lo"], f["hi"], f["units"]) == (0.0, 360.0, "deg")
    assert abs(E.FIELDS["wind"]["legend"][1] - 30.8667) < 1e-3          # 60 kt


# ------------------------------- fetch helpers -------------------------------------

def test_steps_urls_and_needed_keys():
    # the models' full output: hourly to +120 h, then every 3 h to +384 h (209 frames)
    assert F.STEPS[:3] == [0, 1, 2] and F.STEPS[120] == 120 and F.STEPS[121:123] == [123, 126] and F.STEPS[-1] == 384
    assert len(F.STEPS) == 121 + 88 == 209 and F.STEPS == sorted(set(F.STEPS))
    assert F.STEP_SCHEDULE == [[0, 120, 1], [123, 384, 3]]
    assert [s for a, b, e in F.STEP_SCHEDULE for s in range(a, b + 1, e)] == F.STEPS
    assert F.wave_url(RUN, 3).endswith("gfs.20260922/12/wave/gridded/gfswave.t12z.global.0p25.f003.grib2")
    assert F.atmos_url(RUN, 384).endswith("gfs.20260922/12/atmos/gfs.t12z.pgrb2.0p25.f384")
    keys = F.needed_keys(RUN)
    assert len(keys) == 209 * 4 and "gfs.20260922/12/atmos/gfs.t12z.pgrb2.0p25.f000.idx" in keys
    assert "gfs.20260922/12/wave/gridded/gfswave.t12z.global.0p25.f001.grib2.idx" in keys
    assert not any(".f121" in k or ".f385" in k for k in keys)


def _listing_xml(keys, truncated=False, token=None):
    body = "".join(f"<Contents><Key>{k}</Key></Contents>" for k in keys)
    tok = f"<NextContinuationToken>{token}</NextContinuationToken>" if token else ""
    return (f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>'
            f'{"true" if truncated else "false"}</IsTruncated>{tok}{body}</ListBucketResult>').encode()


def test_completeness_from_listing_and_pagination(monkeypatch):
    all_keys = F.needed_keys(RUN)
    pages = {}

    def fake_request(url, rng=None, **kw):
        prefix = url.split("prefix=")[1].split("&")[0]
        ks = [k for k in all_keys if k.startswith(prefix) and k not in pages.get("missing", [])]
        if "continuation-token" in url:
            return 200, _listing_xml(ks[len(ks) // 2:])
        return 200, _listing_xml(ks[:len(ks) // 2], truncated=True, token="t1")
    monkeypatch.setattr(F, "_request", fake_request)
    assert F.run_is_complete(RUN)
    pages["missing"] = ["gfs.20260922/12/atmos/gfs.t12z.pgrb2.0p25.f201.idx"]     # the straggler seen live
    assert F.missing_objects(RUN) == pages["missing"]
    assert not F.run_is_complete(RUN)


def test_latest_complete_run_walks_back_and_raises_on_transport(monkeypatch):
    complete = {datetime(2026, 9, 22, 12, tzinfo=timezone.utc)}
    monkeypatch.setattr(F, "run_is_complete", lambda dt: dt in complete)
    now = datetime(2026, 9, 23, 1, 30, tzinfo=timezone.utc)
    assert F.latest_complete_run(now) == datetime(2026, 9, 22, 12, tzinfo=timezone.utc)

    def boom(dt):
        raise F.TransportError("503")
    monkeypatch.setattr(F, "run_is_complete", boom)
    with pytest.raises(F.TransportError):
        F.latest_complete_run(now)


def test_request_maps_404_to_not_ready_and_retries_5xx(monkeypatch):
    import urllib.error
    calls = []

    def fake_open(req, timeout=None):
        calls.append(req.full_url)
        raise urllib.error.HTTPError(req.full_url, 503, "busy", {}, None)
    monkeypatch.setattr(F.urllib.request, "urlopen", fake_open)
    monkeypatch.setattr(F.time, "sleep", lambda s: None)
    with pytest.raises(F.TransportError):
        F._request("http://x/a", tries=3)
    assert len(calls) == 3

    def gone(req, timeout=None):
        raise urllib.error.HTTPError(req.full_url, 404, "nf", {}, None)
    monkeypatch.setattr(F.urllib.request, "urlopen", gone)
    with pytest.raises(F.NotReady):
        F._request("http://x/a")


def test_fetch_records_guards(monkeypatch):
    idx = ("1:0:d=2026092212:WIND:surface:24 hour fcst:\n2:100:d=2026092212:HTSGW:surface:24 hour fcst:\n"
           "3:200:d=2026092212:PERPW:surface:24 hour fcst:\n")
    assert F.parse_idx(idx) == [(0, "WIND:surface"), (100, "HTSGW:surface"), (200, "PERPW:surface")]
    blobs = {"bytes=100-199": b"GRIB" + b"x" * 92 + b"7777", "bytes=200-": b"GRIB" + b"y" * 40 + b"7777"}

    def fake_request(url, rng=None, **kw):
        if url.endswith(".idx"):
            return 200, idx.encode()
        return 206, blobs[rng]
    monkeypatch.setattr(F, "_request", fake_request)
    out = F.fetch_records("http://x/f024", ["HTSGW:surface", "PERPW:surface"])
    assert set(out) == {"HTSGW:surface", "PERPW:surface"} and out["HTSGW:surface"] == blobs["bytes=100-199"]
    with pytest.raises(RuntimeError):
        F.fetch_records("http://x/f024", ["NOPE:surface"])
    monkeypatch.setattr(F, "_request", lambda url, rng=None, **kw: (200, idx.encode()) if url.endswith(".idx") else (200, b"whole file"))
    with pytest.raises(RuntimeError):                                    # 200 instead of 206
        F.fetch_records("http://x/f024", ["HTSGW:surface"])
    monkeypatch.setattr(F, "_request", lambda url, rng=None, **kw: (200, idx.encode()) if url.endswith(".idx") else (206, b"GRIB" + b"x" * 10 + b"7777"))
    with pytest.raises(RuntimeError):                                    # wrong length
        F.fetch_records("http://x/f024", ["HTSGW:surface"])
    dup = idx + "4:300:d=2026092212:HTSGW:surface:0-24 hour ave:\n"
    monkeypatch.setattr(F, "_request", lambda url, rng=None, **kw: (200, dup.encode()))
    with pytest.raises(RuntimeError):                                    # duplicate key
        F.fetch_records("http://x/f024", ["HTSGW:surface"])


# ------------------------------- publish: ordering, pointer, prune ------------------

class FakeClient:
    def __init__(self):
        self.objects = {}
        self.log = []

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        self.objects[Key] = {"body": Body, "ct": ContentType, "cc": CacheControl}
        self.log.append(("put", Key))

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key]["body"])}

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None, **kw):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        start = int(ContinuationToken) if ContinuationToken else 0
        page = keys[start:start + 2]                                     # tiny pages -> exercises pagination
        nxt = start + 2
        return {"Contents": [{"Key": k} for k in page], "IsTruncated": nxt < len(keys),
                "NextContinuationToken": str(nxt) if nxt < len(keys) else None}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)
            self.log.append(("del", o["Key"]))
        return {}


@pytest.fixture
def offline_build(monkeypatch):
    rng = np.random.default_rng(0)
    monkeypatch.setattr(R.F, "fetch_records", lambda url, keys: {k: b"x" for k in keys})
    def grid():
        g = rng.uniform(0, 10, size=(721, 1440)).astype(np.float32)
        g[:, 100:120] = np.nan                                           # a strip of "land": the fill has work to do
        return g
    monkeypatch.setattr(R.D, "decode", lambda blob, key=None, run_dt=None, step=None: (grid(), {}))
    monkeypatch.setattr(R.E, "fill_allow", lambda: np.ones((721, 1440), bool))
    return None


def test_partial_build_never_flips_pointer(offline_build):
    c = FakeClient()
    store = P.Store(c, "b")
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092206","complete":true}', "ct": "", "cc": ""}
    m = R.build_and_publish(store, RUN, [0, 3], log=lambda *a: None)
    assert m["complete"] is False
    assert json.loads(c.objects[P.LATEST_KEY]["body"])["run"] == "2026092206"       # untouched
    assert any(k.startswith(f"{P.PREFIX}/2026092212/partial-") for k in c.objects)
    assert not any(k.startswith(f"{P.PREFIX}/2026092212/manifest-") for k in c.objects)


def test_complete_build_order_frames_manifest_pointer(offline_build, monkeypatch):
    # 209 steps x 5 fields through the real fill / quantisation / PNG / upload / manifest path, on a small grid with the fixture's
    # 20-column missing strip: this test checks the order, counts and manifest, not pixel values (full-size frames took 4 min,
    # near the CI timeout)
    rows, cols = 73, 144
    small = np.tile(np.linspace(0.5, 9.5, cols, dtype=np.float32), (rows, 1))
    small[:, 100:120] = np.nan
    monkeypatch.setattr(R.D, "decode", lambda blob, key=None, run_dt=None, step=None: (small, {}))
    monkeypatch.setattr(R.E, "fill_allow", lambda: np.ones((rows, cols), bool))
    c = FakeClient()
    store = P.Store(c, "b")
    m = R.build_and_publish(store, RUN, F.STEPS, log=lambda *a: None)
    puts = [k for op, k in c.log if op == "put"]
    assert puts[-1] == P.LATEST_KEY and puts[-2].startswith(f"{P.PREFIX}/2026092212/manifest-")
    assert puts[-3].startswith(f"{P.PREFIX}/2026092212/stats-")           # sidecar BEFORE the manifest
    assert all(k.endswith(".png") for k in puts[:-3]) and len(puts) == 209 * (4 * 2 + 1) + 3   # hs tp wind pdir full+half, wdir half
    assert f"{P.PREFIX}/2026092212/half/wdir/f000.png" in puts and f"{P.PREFIX}/2026092212/wdir/f000.png" not in puts
    assert f"{P.PREFIX}/2026092212/pdir/f384.png" in puts and f"{P.PREFIX}/2026092212/half/pdir/f384.png" in puts
    assert f"{P.PREFIX}/2026092212/hs/f001.png" in puts and f"{P.PREFIX}/2026092212/hs/f121.png" not in puts
    latest = json.loads(c.objects[P.LATEST_KEY]["body"])
    assert latest == {"run": "2026092212", "manifest": puts[-2], "complete": True, "encoding": E.ENCODING,
                      "published_utc": m["published_utc"], "frames": 209}
    man = json.loads(c.objects[latest["manifest"]]["body"], parse_constant=lambda s: (_ for _ in ()).throw(ValueError(s)))
    assert man["complete"] and man["frames"][208]["valid_utc"] == "2026-10-08T12:00:00Z" and man["frames"][1]["valid_utc"] == "2026-09-22T13:00:00Z"
    assert man["schema"] == 3 and man["files"]["template"] == f"{P.PREFIX}/2026092212/{{res}}{{field}}/f{{step:03d}}.png"
    assert man["stats"] == puts[-3] and "_stats" not in man and set(man["frames"][0]) == {"step", "valid_utc"}
    assert len(c.objects[latest["manifest"]]["body"]) < 20_000                 # the custom domain serves it compressed (was < 12 KB for r2.dev)
    stats = json.loads(c.objects[man["stats"]]["body"])
    assert len(stats["frames"]) == 209 and set(stats["frames"][0]["fields"]) == {"hs", "tp", "wind", "pdir", "wdir"}
    assert all(f["pdir_mask_mismatch"] == 0 for f in stats["frames"])
    assert stats["frames"][0]["fields"]["wdir"]["bytes_full"] == 0 and stats["frames"][0]["fields"]["wdir"]["bytes_half"] > 0
    assert stats["frames"][0]["fields"]["hs"]["bytes_full"] > 0
    assert man["grid"]["registration"] == "center" and man["grid_half"]["rows"] == 361 and man["grid_half"]["dlat"] == -0.5
    assert man["encoding_spec"]["missing"] == 0 and man["fields"]["tp"]["legend"] == [4.0, 22.0]
    assert man["frame_schedule"] == [[0, 120, 1], [123, 384, 3]] and man["expected_frames"] == 209 and "frame_hours" not in man
    assert {n: f["interpolation"] for n, f in man["fields"].items()} == {"hs": "bilinear", "tp": "bilinear", "wind": "bilinear",
                                                                       "pdir": "circular", "wdir": "circular"}
    assert man["fields"]["pdir"] == {"lo": 0.0, "hi": 360.0, "legend": [0.0, 360.0], "units": "deg", "interpolation": "circular",
                                     "resolutions": ["full", "half"], "circular": True, "convention": "from"}
    assert man["fields"]["wdir"]["resolutions"] == ["half"] and man["fields"]["hs"]["resolutions"] == ["full", "half"]
    assert "circular" not in man["fields"]["hs"] and set(man["model"]["fields"]) == {"hs", "tp", "wind", "pdir", "wdir"}
    assert man["fill"] == E.FILL_INFO
    f0 = stats["frames"][0]["fields"]
    assert f0["hs"]["filled_points"] == f0["tp"]["filled_points"] == f0["pdir"]["filled_points"] == rows * 8
    assert f0["wind"]["filled_points"] == f0["wdir"]["filled_points"] == 0
    assert c.objects[puts[0]]["cc"] == P.IMMUTABLE and c.objects[P.LATEST_KEY]["cc"] == P.POINTER


def test_manifest_rejects_non_finite():
    c = FakeClient()
    with pytest.raises(ValueError):
        P.publish_manifest(P.Store(c, "b"), "2026092212", {"complete": True, "encoding": "x", "frames": [],
                                                           "published_utc": "2026-09-22T18:00:00Z", "bad": float("inf")})


def test_crash_mid_frames_leaves_pointer_and_no_manifest(offline_build, monkeypatch):
    c = FakeClient()
    store = P.Store(c, "b")
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092206","complete":true}', "ct": "", "cc": ""}
    n = {"calls": 0}
    orig = P.publish_frame

    def flaky(*a):
        n["calls"] += 1
        if n["calls"] == 4:
            raise OSError("boom")
        orig(*a)
    monkeypatch.setattr(P, "publish_frame", flaky)
    with pytest.raises(OSError):
        R.build_and_publish(store, RUN, F.STEPS, log=lambda *a: None)
    assert json.loads(c.objects[P.LATEST_KEY]["body"])["run"] == "2026092206"
    assert not any("manifest-" in k for k in c.objects)


def _seed_run(c, run, manifest=True, n=3):
    for s in range(n):
        c.objects[f"{P.PREFIX}/{run}/hs/f{s:03d}.png"] = {"body": b"", "ct": "", "cc": ""}
    if manifest:
        c.objects[f"{P.PREFIX}/{run}/manifest-x.json"] = {"body": b"{}", "ct": "", "cc": ""}


def test_prune_keeps_newest_complete_protects_latest_and_drops_old_orphans():
    c = FakeClient()
    store = P.Store(c, "b")
    for run in ("2026092000", "2026092006", "2026092012", "2026092018", "2026092100", "2026092106"):
        _seed_run(c, run)
    _seed_run(c, "2026091918", manifest=False)          # old crashed build
    _seed_run(c, "2026092112", manifest=False)          # newer than newest complete: in progress -> keep
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092000","complete":true}', "ct": "", "cc": ""}
    deleted = P.prune(store, keep=4)
    assert deleted == ["2026091918", "2026092006"]
    runs = store.list_runs()
    assert set(runs) == {"2026092000", "2026092012", "2026092018", "2026092100", "2026092106", "2026092112"}
    assert runs["2026092112"] is False
    with pytest.raises(ValueError):
        P.prune(store, keep=0)


def test_prune_legacy_removes_only_the_two_legacy_objects():
    c = FakeClient()
    store = P.Store(c, "b")
    _seed_run(c, "2026092212")
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092212","complete":true}', "ct": "", "cc": ""}
    for k in ("gfswave/0p25/latest.json", "gfswave/0p25/2026092212/hs/f000.png", "gfswave/0p25/2026092212/manifest.json"):
        c.objects[k] = {"body": b"", "ct": "", "cc": ""}
    survivors = ("other/x.json", "gfswave/0p25/v2/latest.json", "gfswave/0p25/v2/2026092212/hs/f000.png",
                 "gfswave/0p25/2026092300/hs/f000.png", "gfswave/0p25/latest.json.bak", "gfswave/0p25/manifest.json")
    for k in survivors:
        c.objects[k] = {"body": b"", "ct": "", "cc": ""}
    assert P.prune_legacy(store) == 3
    assert "gfswave/0p25/latest.json" not in c.objects and "gfswave/0p25/2026092212/hs/f000.png" not in c.objects
    assert P.LATEST_KEY in c.objects and f"{P.PREFIX}/2026092212/manifest-x.json" in c.objects
    for k in survivors:                                   # anything outside the two legacy objects is never touched
        assert k in c.objects, k
    assert P.prune_legacy(store) == 0


def test_redact_strips_endpoint_account_key_and_bucket():
    acct, keyid, bucket = "0123456789abcdef0123456789abcdef", "fedcba9876543210fedcba9876543210", "allshore-model-frames"
    msg = (f'Could not connect to the endpoint URL: "https://{acct}.r2.cloudflarestorage.com/{bucket}/x.png" '
           f"(key id {keyid}, bucket {bucket}, request id 7f3a)")
    out = P.redact(msg, bucket)
    assert acct not in out and keyid not in out and bucket not in out and "cloudflarestorage" not in out
    assert "<r2-endpoint>" in out and "<redacted>" in out and "<bucket>" in out and "request id 7f3a" in out
    assert P.redact("plain " + "x" * 600) == "plain " + "x" * 494          # capped at 500 characters
    assert P.redact(RuntimeError("boom"), None) == "boom"
    c = FakeClient()
    store = P.Store(c, bucket)
    P.record_failure(store, "2026092212", msg)
    P.record_notready(store, "2026092212", msg)
    for key in (P.failed_key("2026092212"), P.notready_key("2026092212")):
        rec = json.loads(c.objects[key]["body"])
        assert acct not in rec["last_error"] and bucket not in rec["last_error"] and "<r2-endpoint>" in rec["last_error"]


def test_trigger_worker_config_and_unit_tests():
    """The cron Worker that dispatches the workflow (tools/model_frames/trigger): cron-only, no public
    URL, aimed at this repository's workflow on the production branch; its Node tests stub fetch."""
    import re
    import shutil
    import subprocess
    trigger = os.path.join(ROOT, "tools", "model_frames", "trigger")
    cfg = open(os.path.join(trigger, "wrangler.jsonc"), encoding="utf-8").read()
    assert '"workers_dev": false' in cfg
    assert re.search(r'"crons":\s*\["5,15,25,35,45,55 \* \* \* \*"\]', cfg)
    for k, v in (("GH_REPO", "Allshore-JM/wave-app"), ("GH_WORKFLOW", "model-frames.yml"), ("GH_REF", "Live-Buoy-Update")):
        assert f'"{k}": "{v}"' in cfg, k
    js = open(os.path.join(trigger, "worker.js"), encoding="utf-8").read()
    assert "async fetch" not in js and "GITHUB_TOKEN" in js and "/dispatches" in js
    if not shutil.which("node"):
        pytest.skip("node not available")
    r = subprocess.run(["node", "--test", "worker.test.js"], cwd=trigger, capture_output=True, text=True)
    assert r.returncode == 0, (r.stdout[-2000:], r.stderr[-2000:])


def test_workflows_pin_actions_and_packages():
    """Every action by commit SHA; every conda package in the publisher and every pip package in the
    test workflow by exact version (G4 item 15)."""
    import re
    wf = os.path.join(ROOT, ".github", "workflows")
    for name in ("model-frames.yml", "model-frames-tests.yml", "model-frames-keepalive.yml"):
        text = open(os.path.join(wf, name), encoding="utf-8").read()
        uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
        assert all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses), (name, uses)
    text = open(os.path.join(wf, "model-frames.yml"), encoding="utf-8").read()
    block = text.split("create-args: >-", 1)[1].split("cache-environment", 1)[0]
    pkgs = block.split()
    assert len(pkgs) >= 6 and all(re.fullmatch(r"[a-z0-9-]+=[0-9][0-9A-Za-z.]*", p) for p in pkgs), pkgs
    text = open(os.path.join(wf, "model-frames-tests.yml"), encoding="utf-8").read()
    pip = re.search(r"pip install ([^\n]+)", text).group(1).split()
    assert all(re.fullmatch(r"[a-z0-9-]+==[0-9][0-9A-Za-z.]*", p) for p in pip), pip


def test_main_counts_not_ready_without_backoff(monkeypatch, capsys):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)
    c = FakeClient()
    _env(monkeypatch, c)

    def gone(url, keys):
        raise R.F.NotReady("gfs.x f201 vanished")
    monkeypatch.setattr(R.F, "fetch_records", gone)
    for i in range(1, 3):
        assert R.main([]) == 3
        rec = json.loads(c.objects[P.notready_key("2026092212")]["body"])
        assert rec["count"] == i and "vanished" in rec["last_error"]
    assert P.failed_key("2026092212") not in c.objects            # not a build failure: no backoff, no pointer change
    assert P.LATEST_KEY not in c.objects


def test_delete_prefix_batches_and_reports_errors():
    c = FakeClient()
    store = P.Store(c, "b")
    for i in range(2500):
        c.objects[f"{P.PREFIX}/2026092000/hs/f{i}.png"] = {"body": b"", "ct": "", "cc": ""}
    sizes = []
    real = c.delete_objects

    def counting(Bucket, Delete):
        sizes.append(len(Delete["Objects"]))
        return real(Bucket, Delete)
    c.delete_objects = counting
    assert store.delete_prefix(f"{P.PREFIX}/2026092000/") == 2500 and max(sizes) <= 1000 and sum(sizes) == 2500
    c.delete_objects = lambda Bucket, Delete: {"Errors": [{"Key": "k", "Code": "AccessDenied"}]}
    c.objects["x/2026092001/a"] = {"body": b"", "ct": "", "cc": ""}
    with pytest.raises(RuntimeError):
        store.delete_prefix("x/2026092001/")


def test_get_json_only_swallows_missing():
    class Client:
        def get_object(self, Bucket, Key):
            class Err(Exception):
                response = {"Error": {"Code": "AccessDenied"}}
            raise Err()
    with pytest.raises(Exception):
        P.Store(Client(), "b").get_json("k")
    assert P.Store(FakeClient(), "b").get_json("missing") is None


# ------------------------------- main() policies -----------------------------------

def _env(monkeypatch, client):
    monkeypatch.setattr(R.P, "r2_client", lambda *a: client)
    for k, v in {"R2_ACCOUNT_ID": "a", "R2_ACCESS_KEY_ID": "k", "R2_SECRET_ACCESS_KEY": "s", "R2_BUCKET": "b"}.items():
        monkeypatch.setenv(k, v)


def test_main_skips_when_live_and_complete(monkeypatch, capsys):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)
    c = FakeClient()
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092212","complete":true}', "ct": "", "cc": ""}
    _env(monkeypatch, c)
    assert R.main([]) == 0 and "already live" in capsys.readouterr().out


def test_main_never_regresses_pointer(monkeypatch, capsys):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)
    c = FakeClient()
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092218","complete":true}', "ct": "", "cc": ""}
    _env(monkeypatch, c)
    assert R.main([]) == 0 and "not regressing" in capsys.readouterr().out
    assert json.loads(c.objects[P.LATEST_KEY]["body"])["run"] == "2026092218"


def test_main_subset_requires_dry_run_or_allow_partial(monkeypatch, capsys):
    assert R.main(["--steps", "0,3"]) == 2
    assert R.main(["--steps", "121"]) == 2                                # not a model output hour (3-hourly after +120 h)
    assert R.main(["--steps", "385"]) == 2                                # beyond the model's +384 h
    assert R.main(["--keep", "0"]) == 2


def test_main_exit_3_without_complete_run_or_on_not_ready(monkeypatch, offline_build):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: None)
    assert R.main(["--dry-run"]) == 3
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)

    def vanish(url, keys):
        raise F.NotReady("404")
    monkeypatch.setattr(R.F, "fetch_records", vanish)
    assert R.main(["--dry-run"]) == 3


def test_main_records_failure_and_backs_off(monkeypatch, capsys):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)
    c = FakeClient()
    _env(monkeypatch, c)

    def broken(url, keys):
        raise RuntimeError("corrupt record")
    monkeypatch.setattr(R.F, "fetch_records", broken)
    with pytest.raises(RuntimeError):
        R.main([])
    rec = json.loads(c.objects[P.failed_key("2026092212")]["body"])
    assert rec["attempts"] == 1 and "corrupt" in rec["last_error"]
    assert R.main([]) == 0 and "failed recently" in capsys.readouterr().out       # backs off


def _manifest(c, run, stamp, body, prefix="manifest"):
    c.objects[f"{P.PREFIX}/{run}/{prefix}-{stamp}.json"] = {"body": body if isinstance(body, bytes) else json.dumps(body).encode(), "ct": "", "cc": ""}
    return f"{P.PREFIX}/{run}/{prefix}-{stamp}.json"


def test_fill_guard_refuses_to_rewrite_a_run_built_with_a_different_fill(monkeypatch, capsys, offline_build):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)
    c = FakeClient()
    _env(monkeypatch, c)
    old = {"run": "2026092212", "complete": True, "encoding": E.ENCODING, "frames": [{"step": 0}], "published_utc": "2026-09-22T18:00:00Z"}
    _manifest(c, "2026092212", "20260922T180000Z", old)                                           # published before the fill existed
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092212","complete":true}', "ct": "", "cc": ""}
    n = len(c.log)
    assert R.main(["--force"]) == 2 and "refusing to rewrite" in capsys.readouterr().out          # forced on the live run: refused
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092218","complete":true}', "ct": "", "cc": ""}
    assert R.main(["--force"]) == 2                                                               # forced on an OLDER run: refused, never a regression
    assert R.main(["--steps", "0,3", "--allow-partial"]) == 0                                     # (a newer run is live: nothing to do)
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092212","complete":true}', "ct": "", "cc": ""}
    assert R.main(["--steps", "0,3", "--allow-partial", "--force"]) == 2                          # a partial build would rewrite the same frame keys
    assert len(c.log) == n                                                                        # not one object written
    assert R.main(["--dry-run", "--steps", "0,3"]) == 0 and len(c.log) == n                       # a dry run never touches the bucket
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092206","complete":true}', "ct": "", "cc": ""}
    foreign = dict(old, run="2026092200", frames=[{"step": 0}] * len(F.STEPS))                   # would be repairable, but names another run
    _manifest(c, "2026092212", "20260922T190000Z", foreign)
    assert R.main([]) == 2 and len(c.log) == n                                                    # never point latest.json at a foreign manifest
    _manifest(c, "2026092212", "20260922T200000Z", {k: v for k, v in dict(old, frames=[{"step": 0}] * len(F.STEPS)).items() if k != "encoding"})
    assert R.main([]) == 2 and len(c.log) == n                                                    # nor at one without an encoding


def test_fill_guard_repairs_the_pointer_to_an_existing_complete_manifest(monkeypatch, capsys, offline_build):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)
    c = FakeClient()
    _env(monkeypatch, c)
    old = {"run": "2026092212", "complete": True, "encoding": E.ENCODING, "frames": [{"step": 0}] * len(F.STEPS), "published_utc": "2026-09-22T18:00:00Z"}
    mkey = _manifest(c, "2026092212", "20260922T180000Z", old)                                   # the pointer write after it failed
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092206","complete":true}', "ct": "", "cc": ""}
    assert R.main([]) == 0 and "pointer repaired" in capsys.readouterr().out
    assert [k for op, k in c.log] == [P.LATEST_KEY]                                              # only the pointer was written
    assert json.loads(c.objects[P.LATEST_KEY]["body"]) == {"run": "2026092212", "manifest": mkey, "complete": True, "encoding": E.ENCODING,
                                                          "published_utc": "2026-09-22T18:00:00Z", "frames": len(F.STEPS)}
    del c.objects[P.LATEST_KEY]                                                                   # latest.json lost altogether
    assert R.main([]) == 0 and json.loads(c.objects[P.LATEST_KEY]["body"])["manifest"] == mkey


def test_fill_guard_lets_the_same_fill_through_and_ignores_partials(monkeypatch, capsys, offline_build):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)
    c = FakeClient()
    _env(monkeypatch, c)
    _manifest(c, "2026092212", "20260922T170000Z", {"complete": False, "fill": None}, prefix="partial")   # partials never count
    assert R.fill_guard(P.Store(c, "b"), "2026092212", None, False) is None
    same = {"run": "2026092212", "complete": True, "encoding": E.ENCODING, "frames": [], "published_utc": "2026-09-22T18:00:00Z",
            "fill": dict(reversed(list(E.FILL_INFO.items())), note="reworded")}                  # same fill, other key order and wording
    _manifest(c, "2026092212", "20260922T180000Z", same)
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092212","complete":true}', "ct": "", "cc": ""}
    assert R.main(["--force", "--steps", "0,3", "--allow-partial"]) == 0                          # allowed: the frames would be the same build
    assert any(k.startswith(f"{P.PREFIX}/2026092212/partial-") for k in c.objects)
    _manifest(c, "2026092212", "20260922T190000Z", b"<html>not json")                            # the newest manifest is unreadable
    assert R.fill_guard(P.Store(c, "b"), "2026092212", "2026092212", False) == 2
    assert "not a valid manifest" in capsys.readouterr().out

    class Flaky(FakeClient):
        def get_object(self, Bucket, Key):
            raise RuntimeError("503 Service Unavailable")
    f = Flaky()
    f.objects = c.objects
    assert R.fill_guard(P.Store(f, "b"), "2026092212", "2026092212", False) == 1                  # transport: exit 1, retried next tick
    assert P.newest_manifest(P.Store(c, "b"), "2026092218") == (None, None)


def test_rollback_without_fill_omits_the_block_and_keeps_the_guard(monkeypatch, capsys, offline_build):
    monkeypatch.setattr(R.E, "FILL_INFO", None)                                                   # what "fill": False for hs, tp and pdir produces
    c = FakeClient()
    store = P.Store(c, "b")
    m = R.build_and_publish(store, RUN, [0], log=lambda *a: None)
    assert "fill" not in m                                                                        # the client then drops the "extrapolated" sentence
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)
    _env(monkeypatch, c)
    filled = {"run": "2026092212", "complete": True, "encoding": E.ENCODING, "frames": [{"step": 0}] * len(F.STEPS),
              "published_utc": "2026-09-22T18:00:00Z", "fill": {"version": 2, "fields": ["hs", "tp"], "cells": 4}}
    _manifest(c, "2026092212", "20260922T180000Z", filled)
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092212","complete":true}', "ct": "", "cc": ""}
    assert R.main(["--force"]) == 2 and "refusing to rewrite" in capsys.readouterr().out          # filled runs stay protected


# ------------------------------- Phase B: direction fields -------------------------

def test_wind_dir_from_conventions():
    u = np.array([5.0, 0.0, -5.0, 0.0, 3.0, np.nan, 0.0], np.float32)
    v = np.array([0.0, 5.0, 0.0, -5.0, 3.0, 1.0, np.nan], np.float32)
    d = D.wind_dir_from(u, v)
    assert d.dtype == np.float32
    assert np.allclose(d[:5], [270.0, 180.0, 90.0, 0.0, 225.0])      # blowing east = from the west; north = from the south; NE-ward = from the SW
    assert np.isnan(d[5]) and np.isnan(d[6])
    assert np.all((d[:5] >= 0) & (d[:5] < 360))


def test_quantize_circular_wraps_and_round_trips():
    v = np.array([0.0, 360.0, 359.9, 0.7, 180.0, -10.0, 725.0, np.nan])
    q = E.quantize_circular(v)
    assert q[0] == q[1] == q[2] == 1                                   # 0, 360 and 359.9 share a code
    assert q[7] == 0 and q.max() <= 254                                # missing stays 0; code 255 is never used
    back = E.dequantize(q, 0.0, 360.0)
    ang = lambda a, b: np.abs((a - b + 180.0) % 360.0 - 180.0)
    assert ang(back[5], 350.0) < 0.71 and ang(back[6], 5.0) < 0.71     # negative and > 360 inputs wrap
    x = np.linspace(0, 360, 100001)
    assert ang(E.dequantize(E.quantize_circular(x), 0.0, 360.0), x).max() <= 360.0 / 508 + 1e-9


def test_direction_frames_fill_by_nearest_and_publish_their_resolutions():
    g = np.full((721, 1440), np.nan)
    g[:, 700:710] = 350.0
    g[:, 710:720] = 10.0                                              # across north: a mean would say 180
    g[:, 715], g[:, 716] = 359.5, 360.0                               # the linear coder would give these 255
    enc = E.encode_frame(g, "pdir", allow=np.ones((721, 1440), bool))
    q = np.array(Image.open(io.BytesIO(enc["full"])))
    codes = set(np.unique(q[q > 0]).tolist())
    assert codes == set(np.unique(E.quantize_circular(np.array([350.0, 10.0, 359.5]))).tolist())   # every filled node carries a model angle
    assert q.max() <= 254 and q[0, 715] == q[0, 716] == 1                                   # through encode_frame: circular, 360 = north
    assert enc["stats"]["filled_points"] == 721 * 8 and "half" in enc
    w = E.encode_frame(np.full((721, 1440), 90.0), "wdir")
    assert "full" not in w and np.array(Image.open(io.BytesIO(w["half"]))).shape == (361, 720)
    c = FakeClient()
    P.publish_frame(P.Store(c, "b"), "2026092212", "wdir", 3, w)
    assert [k for op, k in c.log if op == "put"] == [f"{P.PREFIX}/2026092212/half/wdir/f003.png"]


def test_dirpw_identity_is_enforced():
    meta = {"shortName": "dirpw", "typeOfLevel": "surface", "level": 1, "stepRange": "24", "dataDate": 20260922, "dataTime": 1200}
    D.check_identity(meta, "DIRPW:surface", RUN, 24)
    with pytest.raises(ValueError):
        D.check_identity(dict(meta, shortName="mwd"), "DIRPW:surface", RUN, 24)
    assert R.WAVE_KEYS["pdir"] == "DIRPW:surface"


def test_pdir_coverage_mismatch_is_counted_and_reported(offline_build, monkeypatch, tmp_path):
    real = R.D.decode
    def decode(blob, key=None, run_dt=None, step=None):
        g, meta = real(blob, key, run_dt, step)
        if key == "HTSGW:surface":
            g = g.copy(); g[1, :5] = 0.0; g[2, :4] = np.nan; g[3, :3] = 0.05; g[4, :2] = 0.1   # calm, no height, ripples, the threshold
        if key == "DIRPW:surface":
            g = g.copy(); g[0, :7] = np.nan; g[1, :5] = np.nan; g[3, :3] = np.nan; g[4, :2] = np.nan
        return g, meta
    monkeypatch.setattr(R.D, "decode", decode)
    m = R.build_and_publish(None, RUN, [0, 3], upload=False, log=lambda *a: None)
    # waves of at least PDIR_WAVES_M without a direction (7 + the 2 at exactly 0.1 m) and a direction without a height (4)
    assert R.PDIR_WAVES_M == 0.1
    assert [f["pdir_mask_mismatch"] for f in m["_stats"]["frames"]] == [7 + 2 + 4] * 2
    assert [f["pdir_missing_calm"] for f in m["_stats"]["frames"]] == [5 + 3] * 2            # flat calm and ripples under 0.1 m: no particle is drawn there


def test_pdir_mismatch_warns_in_the_step_summary(offline_build, monkeypatch, tmp_path):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)
    assert R.main(["--dry-run", "--steps", "0"]) == 0
    assert "### run 2026092212" in summary.read_text() and "WARNING" not in summary.read_text()
    real = R.D.decode
    def decode(blob, key=None, run_dt=None, step=None):
        g, meta = real(blob, key, run_dt, step)
        return (np.where(np.arange(1440) < 3, np.nan, g).astype(np.float32) if key == "DIRPW:surface" else g), meta
    monkeypatch.setattr(R.D, "decode", decode)
    summary.write_text("")
    assert R.main(["--dry-run", "--steps", "0"]) == 0
    assert "WARNING: wave direction and wave height cover different cells at 1 steps (first f000)" in summary.read_text()


def test_each_published_field_decodes_to_its_own_grib_record(offline_build, monkeypatch):
    """Per-key constants end to end: a swapped key, a sign slip in the wind direction or a field
    published under another field's name shows up as a wrong decoded value in the bucket."""
    const = {"HTSGW:surface": 2.0, "PERPW:surface": 12.0, "DIRPW:surface": 45.0,
             "UGRD:10 m above ground": 5.0, "VGRD:10 m above ground": 0.0}
    assert set(const) == set(R.WAVE_KEYS.values()) | {"UGRD:10 m above ground", "VGRD:10 m above ground"}
    monkeypatch.setattr(R.D, "decode", lambda blob, key=None, run_dt=None, step=None:
                        (np.full((721, 1440), const[key], np.float32), {}))
    c = FakeClient()
    m = R.build_and_publish(P.Store(c, "b"), RUN, [0], log=lambda *a: None)
    def values(field, half):
        q = np.array(Image.open(io.BytesIO(c.objects[P.frame_key("2026092212", field, 0, half=half)]["body"])))
        f = m["fields"][field]
        assert q.min() > 0                                                     # no missing cells in a constant field
        return E.dequantize(q, f["lo"], f["hi"])
    ang = lambda a, b: np.abs((a - b + 180.0) % 360.0 - 180.0)
    assert ang(values("wdir", True), 270.0).max() <= 360.0 / 508 + 1e-9       # u = +5 blows east: FROM the west
    for half in (False, True):
        assert ang(values("pdir", half), 45.0).max() <= 360.0 / 508 + 1e-9
        assert np.abs(values("hs", half) - 2.0).max() <= 15.0 / 508 + 1e-9
        assert np.abs(values("tp", half) - 12.0).max() <= 29.0 / 508 + 1e-9
        assert np.abs(values("wind", half) - 5.0).max() <= 80 * E.KT / 508 + 1e-9


def test_wind_dir_from_never_returns_360():
    tiny = np.float32(1e-7)                                                    # atan2 -> just under 360, rounds up in float32
    d = D.wind_dir_from(np.array([tiny, 0.0, np.nan], np.float32), np.array([-1.0, -1.0, 1.0], np.float32))
    assert d[0] == 0.0 and d[1] == 0.0 and np.isnan(d[2])


# ------------------------------- follow-ups: the pipelined build and the fast nearest fill -------------------------

def _nearest_reference(model, targets, cells):
    """The whole-grid implementation published runs were built with (2026-09-25): the new one must equal it."""
    rows, cols = model.shape
    out, todo = model.copy(), targets.copy()
    offs = sorted(((dy, dx) for dy in range(-cells, cells + 1) for dx in range(-cells, cells + 1) if dy or dx),
                  key=lambda o: (o[0] ** 2 + o[1] ** 2, abs(o[0]), o[0], o[1]))
    pad = np.full((rows + 2 * cells, cols), np.nan)
    pad[cells:-cells] = model
    for dy, dx in offs:
        src = np.roll(pad[cells + dy:cells + dy + rows], -dx, axis=1)
        hit = todo & ~np.isnan(src)
        out[hit] = src[hit]
        todo &= ~hit
        if not todo.any():
            break
    return out


def test_nearest_fill_equals_the_whole_grid_reference():
    rng = np.random.default_rng(11)
    cells = int(E.FILL_CELLS * 2 ** 0.5)
    for trial in range(30):
        rows, cols = int(rng.integers(12, 70)), int(rng.integers(12, 90))
        m = rng.uniform(0, 30, (rows, cols))
        m[rng.uniform(size=(rows, cols)) < rng.uniform(0.2, 0.85)] = np.nan
        m[:2][rng.uniform(size=(2, cols)) < 0.8] = np.nan                  # the poles
        m[:, -2:][rng.uniform(size=(rows, 2)) < 0.8] = np.nan              # the dateline columns
        _, added = E.fill_coast(m, method="mean")
        assert np.array_equal(E._nearest_values(m, added, cells), _nearest_reference(m, added, cells), equal_nan=True), trial


def test_pipelined_build_publishes_the_same_bytes_with_any_number_of_encode_threads(monkeypatch):
    def build(workers):
        rng = np.random.default_rng(3)
        monkeypatch.setattr(R.F, "fetch_records", lambda url, keys: {k: b"x" for k in keys})
        def grid():
            g = rng.uniform(0, 10, size=(721, 1440)).astype(np.float32)
            g[:, 100:120] = np.nan
            return g
        monkeypatch.setattr(R.D, "decode", lambda blob, key=None, run_dt=None, step=None: (grid(), {}))
        monkeypatch.setattr(R.E, "fill_allow", lambda: np.ones((721, 1440), bool))
        monkeypatch.setattr(R, "ENCODE_WORKERS", workers)
        c = FakeClient()
        R.build_and_publish(P.Store(c, "b"), RUN, [0, 3, 6], log=lambda *a: None)
        return {k: v["body"] for k, v in c.objects.items() if k.endswith(".png")}, [k for op, k in c.log if op == "put"]
    one, puts1 = build(1)
    five, puts5 = build(5)
    assert one == five and len(one) == 3 * 9                                                   # identical frames, all of them
    for puts in (puts1, puts5):
        assert all(k.endswith(".png") for k in puts[:-2]) and puts[-2].startswith(f"{P.PREFIX}/2026092212/stats-")       # every frame first,
        assert puts[-1].startswith(f"{P.PREFIX}/2026092212/partial-")                                                  # then the sidecar and the manifest


def test_an_upload_failure_stops_the_build_before_any_manifest_or_pointer(offline_build):
    class Flaky(FakeClient):
        def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
            if "/f006.png" in Key:
                raise RuntimeError("R2 500")
            super().put_object(Bucket, Key, Body, ContentType, CacheControl)
    c = Flaky()
    with pytest.raises(RuntimeError, match="R2 500"):
        R.build_and_publish(P.Store(c, "b"), RUN, F.STEPS, log=lambda *a: None)
    assert not any("/manifest-" in k or "/stats-" in k or k == P.LATEST_KEY for k in c.objects), "nothing but frames"
    assert len(c.objects) < len(F.STEPS) * 9                                                    # the build stopped early


def test_a_step_that_is_not_ready_propagates_from_the_prefetch(offline_build, monkeypatch):
    real = R.F.fetch_records
    def fetch(url, keys):
        if "f009" in url:
            raise F.NotReady("404 " + url)
        return real(url, keys)
    monkeypatch.setattr(R.F, "fetch_records", fetch)
    done = []
    with pytest.raises(F.NotReady):
        R.build_and_publish(None, RUN, [0, 3, 6, 9, 12], upload=False, log=done.append)
    assert [d.split()[0] for d in done] == ["f000", "f003", "f006"]                              # the steps before it were built

def test_an_upload_failure_that_completes_between_two_checks_is_never_lost(offline_build, monkeypatch):
    """G12 P0-1: a failed upload whose future reports "not done" once and "done" afterwards must still raise, and the
    stats, manifest and pointer must never be written over the missing frame."""
    class Done:
        def __init__(self, value=None, exc=None):
            self.value, self.exc = value, exc
        def done(self):
            return True
        def result(self):
            if self.exc:
                raise self.exc
            return self.value
        def cancel(self):
            return False

    class FlipFlop(Done):                                   # not done at the first question, done (with its error) afterwards
        calls = 0
        def done(self):
            self.calls += 1
            return self.calls > 1

    class Inline:
        def __init__(self, *a, **k):
            pass
        def submit(self, fn, *args):
            try:
                v = fn(*args)
            except Exception as exc:                        # noqa: BLE001
                return FlipFlop(exc=exc) if fn is P.publish_frame else Done(exc=exc)
            return Done(v)
        def map(self, fn, it):
            return [fn(x) for x in it]
        def shutdown(self, **k):
            pass

    monkeypatch.setattr(R, "ThreadPoolExecutor", Inline)

    class Flaky(FakeClient):
        def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
            if "/pdir/f006.png" in Key:
                raise RuntimeError("R2 500")
            super().put_object(Bucket, Key, Body, ContentType, CacheControl)
    for steps in ([0, 3, 6, 9, 12], F.STEPS):              # the failure found by a later drain, or by the final drain(0)
        c = Flaky()
        with pytest.raises(RuntimeError, match="R2 500"):
            R.build_and_publish(P.Store(c, "b"), RUN, steps, log=lambda *a: None)
        assert not any("/manifest-" in k or "/partial-" in k or "/stats-" in k or k == P.LATEST_KEY for k in c.objects)
