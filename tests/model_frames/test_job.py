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
        out = E.encode_frame(g, name)
        full = np.array(Image.open(io.BytesIO(out["full"])))
        half = np.array(Image.open(io.BytesIO(out["half"])))
        s = out["stats"]
        assert s["valid_points"] == 721 * 10 and s["filled_points"] == 721 * 8                   # 4 columns each side
        assert np.count_nonzero(full) == s["valid_points"] + s["filled_points"]
        assert np.array_equal(half, E.half_res(full)) and np.count_nonzero(half[:, 348:357]) == 361 * 9
        assert s["min"] == 3.0 and s["max"] == 3.0                                                # stats describe the model values
    w = E.encode_frame(g, "wind")
    assert w["stats"]["filled_points"] == 0                                                       # wind is never filled


def test_fill_info_pins_fields_and_cells():
    assert E.FILL_INFO["fields"] == ["hs", "tp"] and E.FILL_INFO["cells"] == E.FILL_CELLS == 4
    assert json.loads(json.dumps(E.FILL_INFO)) == E.FILL_INFO                                   # manifest-safe, compares equal after a round trip
    assert [n for n, f in E.FIELDS.items() if f["fill"]] == ["hs", "tp"]


def test_encoding_ranges_cover_legend_and_tp_floor():
    for f in E.FIELDS.values():
        assert f["lo"] <= f["legend"][0] and f["hi"] >= f["legend"][1]
    assert E.FIELDS["tp"]["lo"] <= 1.09                                 # WW3 physical floor
    assert abs(E.FIELDS["wind"]["legend"][1] - 30.8667) < 1e-3          # 60 kt


# ------------------------------- fetch helpers -------------------------------------

def test_steps_urls_and_needed_keys():
    assert F.STEPS[0] == 0 and F.STEPS[-1] == 240 and len(F.STEPS) == 81
    assert F.wave_url(RUN, 3).endswith("gfs.20260922/12/wave/gridded/gfswave.t12z.global.0p25.f003.grib2")
    assert F.atmos_url(RUN, 240).endswith("gfs.20260922/12/atmos/gfs.t12z.pgrb2.0p25.f240")
    keys = F.needed_keys(RUN)
    assert len(keys) == 81 * 4 and "gfs.20260922/12/atmos/gfs.t12z.pgrb2.0p25.f000.idx" in keys


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
    c = FakeClient()
    store = P.Store(c, "b")
    m = R.build_and_publish(store, RUN, F.STEPS, log=lambda *a: None)
    puts = [k for op, k in c.log if op == "put"]
    assert puts[-1] == P.LATEST_KEY and puts[-2].startswith(f"{P.PREFIX}/2026092212/manifest-")
    assert puts[-3].startswith(f"{P.PREFIX}/2026092212/stats-")           # sidecar BEFORE the manifest
    assert all(k.endswith(".png") for k in puts[:-3]) and len(puts) == 81 * 3 * 2 + 3
    latest = json.loads(c.objects[P.LATEST_KEY]["body"])
    assert latest == {"run": "2026092212", "manifest": puts[-2], "complete": True, "encoding": E.ENCODING,
                      "published_utc": m["published_utc"], "frames": 81}
    man = json.loads(c.objects[latest["manifest"]]["body"], parse_constant=lambda s: (_ for _ in ()).throw(ValueError(s)))
    assert man["complete"] and man["frames"][80]["valid_utc"] == "2026-10-02T12:00:00Z"
    assert man["schema"] == 3 and man["files"]["template"] == f"{P.PREFIX}/2026092212/{{res}}{{field}}/f{{step:03d}}.png"
    assert man["stats"] == puts[-3] and "_stats" not in man and set(man["frames"][0]) == {"step", "valid_utc"}
    assert len(c.objects[latest["manifest"]]["body"]) < 12_000                 # slim: r2.dev serves it uncompressed
    stats = json.loads(c.objects[man["stats"]]["body"])
    assert len(stats["frames"]) == 81 and set(stats["frames"][0]["fields"]) == {"hs", "tp", "wind"}
    assert stats["frames"][0]["fields"]["hs"]["bytes_full"] > 0
    assert man["grid"]["registration"] == "center" and man["grid_half"]["rows"] == 361 and man["grid_half"]["dlat"] == -0.5
    assert man["encoding_spec"]["missing"] == 0 and man["fields"]["tp"]["legend"] == [4.0, 22.0]
    assert man["frame_hours"] == 3 and man["expected_frames"] == 81
    assert man["fill"] == E.FILL_INFO
    f0 = stats["frames"][0]["fields"]
    assert f0["hs"]["filled_points"] == f0["tp"]["filled_points"] == 721 * 8 and f0["wind"]["filled_points"] == 0
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
    assert R.main(["--steps", "1"]) == 2                                  # not a 3-hourly step
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


def test_main_refuses_to_republish_a_run_built_without_the_fill(monkeypatch, capsys, offline_build):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: RUN)
    c = FakeClient()
    old = {"run": "2026092212", "complete": True, "encoding": E.ENCODING, "frames": []}            # published before the fill existed
    c.objects[f"{P.PREFIX}/2026092212/manifest-20260922T180000Z.json"] = {"body": json.dumps(old).encode(), "ct": "", "cc": ""}
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092212","complete":true}', "ct": "", "cc": ""}
    _env(monkeypatch, c)
    n = len(c.log)
    assert R.main(["--force"]) == 2 and "refusing to re-publish" in capsys.readouterr().out
    assert len(c.log) == n                                                                        # not one frame written
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092206","complete":true}', "ct": "", "cc": ""}
    assert R.main([]) == 2                                                                        # nor without --force
    newer = dict(old, fill=E.FILL_INFO)                                                           # a later manifest built with the fill
    c.objects[f"{P.PREFIX}/2026092212/manifest-20260922T190000Z.json"] = {"body": json.dumps(newer).encode(), "ct": "", "cc": ""}
    assert R.published_fill(P.Store(c, "b"), "2026092212") == (True, E.FILL_INFO)
    assert R.published_fill(P.Store(c, "b"), "2026092218") == (False, None)
    assert R.main(["--dry-run", "--steps", "0,3"]) == 0                                           # a dry run never touches the bucket
