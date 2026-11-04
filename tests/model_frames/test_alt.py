"""Satellite wave-height and swell jobs (plan sections 45 and 81): offline tests (no network, no R2; h5py for the readers).

A fake bucket (FakeClient) and a fake HTTPS (`fakeweb`: the STAC collection, the CloudFerro listing, the product files
written with h5py in tmp, the frames host's pointer / manifest / PNG frames) drive alt.main end to end: the plan's
digests, the chunk round trip through altfmt.decode_chunk, the collocation against a hand-computed value, the pointer
written last and only on a change, prune, the stale alarm, the swell reader's refusals.
"""
import io
import json
import os
import re
import sys
import zlib
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import alt as A             # noqa: E402
import altfmt as F          # noqa: E402
import encode as E          # noqa: E402
import publish as P         # noqa: E402
import sarswell as S        # noqa: E402
from test_job import FakeClient   # noqa: E402

h5py = pytest.importorskip("h5py")
from PIL import Image       # noqa: E402

NOW = datetime(2026, 11, 4, 15, 20, tzinfo=timezone.utc)
EP = A.EPOCH


def ss(dt):
    return (dt - EP).total_seconds()


# ------------------------------------------------------------------ the chunk format
def test_chunk_round_trip_and_every_rejection():
    n = 5
    t = np.array([10, 20, 20, 30, 5], np.float64)                    # two segments: 0..3 (sorted) and 4
    arrays = {"t": F.to_stored(t, 1, None, "u4"), "lat": F.to_stored([1.5, -89.999999, 0, 45, 10], 1e-6, None, "i4"),
              "lon": F.to_stored([-179.5, 179.999999, 0, 10, -10], 1e-6, None, "i4"),
              "swh": F.to_stored([1.234, np.nan, 0, 30, 2.5], 0.001, F.NONE_U2, "u2"), "model": F.to_stored([1.0, 2.0, np.nan, 29.9, 2.4], 0.001, F.NONE_U2, "u2"),
              "mission": np.array([0, 0, 0, 0, 1], np.uint8), "run": np.array([0, 1, 255, 0, 0], np.uint8), "flags": np.array([0, 1, 2, 3, 0], np.uint8)}
    header = {"window": "20261104T12", "missions": [{"id": "j3"}, {"id": "s3a"}], "segments": [[0, 0, 4], [1, 4, 1]], "runs": ["2026110406", "2026110400"]}
    buf = F.encode_chunk(F.MAGIC_ALT, header, {}, arrays)
    assert buf[:4] == b"ALT1"
    h, cols = F.decode_chunk(buf)
    assert h["format"] == "alt-v1" and h["n"] == n and h["columns"] == F.ALT_COLUMNS and h["epoch"] == F.EPOCH
    v = F.values(h, cols)
    assert v["t"].tolist() == t.tolist() and abs(v["lat"][1] + 89.999999) < 1e-9 and abs(v["lon"][0] + 179.5) < 1e-9
    assert v["swh"][0] == pytest.approx(1.234) and np.isnan(v["swh"][1]) and np.isnan(v["model"][2]) and v["model"][3] == pytest.approx(29.9)
    assert np.isnan(v["run"][2]) and v["flags"].tolist() == [0, 1, 2, 3, 0]
    with pytest.raises(ValueError):
        F.decode_chunk(buf, magic=F.MAGIC_SWELL)
    with pytest.raises(ValueError):
        F.decode_chunk(b"ALT2" + buf[4:])
    with pytest.raises(ValueError):
        F.decode_chunk(buf[:-3], magic=F.MAGIC_ALT)                   # a truncated stream
    with pytest.raises(ValueError):
        F.decode_chunk(buf, max_bytes=len(buf) - 1)
    hl = int.from_bytes(buf[4:8], "little")
    big = json.loads(buf[8:8 + hl]); big["n"] = n + 1
    bj = json.dumps(big, separators=(",", ":"), sort_keys=True).encode()
    with pytest.raises(ValueError):                                     # the header claims more readings than the stream holds
        F.decode_chunk(buf[:4] + len(bj).to_bytes(4, "little") + bj + buf[8 + hl:])
    with pytest.raises(ValueError):
        F.encode_chunk(F.MAGIC_ALT, dict(header, segments=[[0, 0, 5]]), {}, arrays)   # 30 then 5: not time-sorted
    with pytest.raises(ValueError):
        F.encode_chunk(F.MAGIC_ALT, dict(header, segments=[[0, 0, 4]]), {}, arrays)   # not covered
    with pytest.raises(ValueError):
        F.encode_chunk(F.MAGIC_ALT, header, {}, dict(arrays, swh=arrays["swh"].astype(np.float64)))
    with pytest.raises(ValueError):
        F.to_stored([70.0], 0.001, F.NONE_U2, "u2")                   # does not fit
    with pytest.raises(ValueError):
        F.to_stored([np.nan], 1, None, "u4")
    # the header's lists are checked against the columns: a run the header does not list, unknown flag bits, a segment
    # naming another mission than its readings, a missing mission list
    for bad in (dict(arrays, run=np.array([0, 2, 255, 0, 0], np.uint8)), dict(arrays, flags=np.array([0, 1, 2, 3, 4], np.uint8))):
        with pytest.raises(ValueError):
            F.decode_chunk(F.encode_chunk(F.MAGIC_ALT, header, {}, bad))
    with pytest.raises(ValueError):
        F.decode_chunk(F.encode_chunk(F.MAGIC_ALT, dict(header, segments=[[1, 0, 4], [0, 4, 1]]), {}, arrays))
    with pytest.raises(ValueError):
        F.decode_chunk(F.encode_chunk(F.MAGIC_ALT, dict(header, missions=[]), {}, arrays))
    deep = "[" * 3000 + "]" * 3000
    dj = json.dumps(dict(header, format="alt-v1", n=0, columns=F.ALT_COLUMNS)).replace('"window"', '"deep": ' + deep + ', "window"').encode()
    with pytest.raises(ValueError):
        F.decode_chunk(b"ALT1" + len(dj).to_bytes(4, "little") + dj + zlib.compress(b""))
    # a stream that inflates to far more than the header's n is cut off at n x stride + 1, never inflated whole
    big = json.dumps(dict(header, format="alt-v1", n=1, columns=F.ALT_COLUMNS, segments=[[0, 0, 1]])).encode()
    with pytest.raises(ValueError):
        F.decode_chunk(b"ALT1" + len(big).to_bytes(4, "little") + big + zlib.compress(b"\0" * 5_000_000))


def test_swell_chunk_round_trip():
    arrays = {"t": F.to_stored([1, 2], 1, None, "u4"), "lat": F.to_stored([0, 1], 1e-6, None, "i4"), "lon": F.to_stored([0, 1], 1e-6, None, "i4"),
              "n": np.array([2, 1], np.uint8), "mission": np.array([0, 0], np.uint8), "flags": np.array([0, 0], np.uint8)}
    for k in range(1, 4):
        arrays[f"hs{k}"] = F.to_stored([1.5 if k < 3 else np.nan, 2.0 if k == 1 else np.nan], 0.001, F.NONE_U2, "u2")
        arrays[f"tp{k}"] = F.to_stored([14.25 if k < 3 else np.nan, 9.5 if k == 1 else np.nan], 0.01, F.NONE_U2, "u2")
        arrays[f"dir{k}"] = F.to_stored([215.5 if k < 3 else np.nan, 0 if k == 1 else np.nan], 0.01, F.NONE_U2, "u2")
    buf = F.encode_chunk(F.MAGIC_SWELL, {"missions": [{"id": "s1a"}], "segments": [[0, 0, 2]]}, {}, arrays)
    h, cols = F.decode_chunk(buf, magic=F.MAGIC_SWELL)
    v = F.values(h, cols)
    assert h["format"] == "swell-v1" and v["tp1"][0] == pytest.approx(14.25) and v["dir1"][0] == pytest.approx(215.5) and np.isnan(v["hs3"][0]) and v["n"].tolist() == [2, 1]
    assert F.stride_of(F.SWELL_COLUMNS) == 4 + 4 + 4 + 3 + 18 and F.stride_of(F.ALT_COLUMNS) == 19


# ------------------------------------------------------------------ listing, STAC, the plan
LISTING_NS = ('<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
              '<IsTruncated>{trunc}</IsTruncated>{token}{contents}{prefixes}</ListBucketResult>')


def listing_xml(entries, ns=True, token=None, prefixes=()):
    body = "".join(f"<Contents><Key>{k}</Key><LastModified>x</LastModified><Size>{s}</Size></Contents>" for k, s in entries)
    pre = "".join(f"<CommonPrefixes><Prefix>{p}</Prefix></CommonPrefixes>" for p in prefixes)
    x = LISTING_NS.format(trunc="true" if token else "false", token=f"<NextContinuationToken>{token}</NextContinuationToken>" if token else "", contents=body, prefixes=pre)
    return (x if ns else x.replace(' xmlns="http://s3.amazonaws.com/doc/2006-03-01/"', "")).encode()


def test_parse_listing_with_and_without_namespace_and_pagination():
    e = [("native/x/a.nc", 10), ("native/x/b.nc", 20)]
    assert A.parse_listing(listing_xml(e)) == (e, [], None)
    assert A.parse_listing(listing_xml(e, ns=False, token="T1")) == (e, [], "T1")
    assert A.parse_listing(listing_xml([], prefixes=["native/P/ds1/", "native/P/ds2/"]))[1] == ["native/P/ds1/", "native/P/ds2/"]
    with pytest.raises(ValueError):
        A.parse_listing(b"<Error><Code>AccessDenied</Code></Error>")


def test_file_names_the_tokens_s6a_lr_and_swot_two_productions_of_one_window_and_odd_files():
    p = A.HEIGHTS
    tok, t0, t1, prod = p.parse_file("global_vavh_l3_rt_s6a_lr_20261104T090000_20261104T120000_20261104T134512.nc")
    assert tok == "s6a_lr" and t0 == datetime(2026, 11, 4, 9, tzinfo=timezone.utc) and t1 == datetime(2026, 11, 4, 12, tzinfo=timezone.utc) and prod.hour == 13
    assert p.parse_file("global_vavh_l3_rt_swot_20261104T090000_20261104T120000_20261104T134512.nc")[0] == "swot"
    assert p.parse_file("global_vavh_l3_rt_j3_20261104T090000_20261104T120000.nc") is None
    assert p.parse_file("global_vavh_l3_rt_j3_20261104T090000_20261104T120000_20261104T134512.nc.part") is None
    assert p.parse_file("global_vavh_l3_rt_j3_20261399T090000_20261104T120000_20261104T134512.nc") is None
    # the swell product: the span from the file, the mission from the dataset
    tok, t0, t1, prod = S.SWELL.parse_file("global_s1a_l3_spc_fwk_nrt_20261104T001512_20261104T235959_20261105T061200.nc", "s1a")
    assert tok == "s1a" and t0.hour == 0 and t1.hour == 23 and prod.day == 5
    assert S.SWELL.parse_file("x_20261104T001512_20261104T235959.nc", "s1c")[3] == datetime(2026, 11, 4, 23, 59, 59, tzinfo=timezone.utc)
    assert S.SWELL.parse_file("x_20261104T001512_20261104T235959.nc", None) is None
    assert A._dataset_token(S.SWELL, "cmems-obs-wave_glo_phy_spc-fwk_nrt_s1c_l3_PT15S-i_202511") == "s1c"
    assert A._dataset_token(S.SWELL, "cmems-obs-wave_glo_phy_spc-fwk_nrt_s1c_l3_PT3H_202511") is None, "a propagated dataset is not an observation one"
    assert A._dataset_token(A.HEIGHTS, "cmems_obs-wave_glo_phy-swh_nrt_s6a-lr-l3_PT1S_202411") == "s6a_lr"
    assert A._dataset_token(A.HEIGHTS, "cmems_obs-wave_glo_phy-swh_nrt_j3-l3_PT0.2S_202411") is None, "the 5 Hz datasets are left out"


def fake_stac(datasets):
    """A STAC collection naming the datasets and each dataset's native href -> {url: json}."""
    base = f"{A.STAC}/{A.HEIGHTS.stac_id}"
    out = {f"{base}/product.stac.json": {"links": [{"rel": "item", "href": f"{d}/dataset.stac.json"} for d in datasets] + [{"rel": "self", "href": "x"}]}}
    for d, native in datasets.items():
        out[f"{base}/{d}/dataset.stac.json"] = {"id": d, "assets": {"native": {"href": native}}}
    return out


class Resp:
    def __init__(self, body):
        self.body = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.pos = 0

    def read(self, n=-1):
        end = len(self.body) if n is None or n < 0 else min(len(self.body), self.pos + n)
        out = self.body[self.pos:end]
        self.pos = end
        return out

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeWeb:
    """GETs answered from a dict of url -> bytes / json / a file path; anything else is a 404 HTTPError."""

    def __init__(self):
        self.urls, self.log, self.down = {}, [], set()

    def __call__(self, url, headers=None, timeout=None):
        import urllib.error
        self.log.append(url)
        if url in self.down:
            raise urllib.error.HTTPError(url, 503, "down", {}, None)
        if url not in self.urls:
            raise urllib.error.HTTPError(url, 404, "nf", {}, None)
        v = self.urls[url]
        if isinstance(v, str) and os.path.exists(v):
            return Resp(open(v, "rb").read())
        return Resp(v)


def test_stac_datasets_takes_the_1hz_datasets_and_their_native_hrefs_and_falls_back_to_the_bucket():
    web = FakeWeb()
    ds = {"cmems_obs-wave_glo_phy-swh_nrt_j3-l3_PT1S_202411": f"{A.S3}/{A.BUCKET}/native/{A.HEIGHTS.stac_id}/cmems_obs-wave_glo_phy-swh_nrt_j3-l3_PT1S_202411",
          "cmems_obs-wave_glo_phy-swh_nrt_j3-l3_PT0.2S_202411": f"{A.S3}/{A.BUCKET}/native/x/5hz",
          "cmems_obs-wave_glo_phy-swh_nrt_s6b-lr-l3_PT1S_202411": f"{A.S3}/{A.BUCKET}/native/{A.HEIGHTS.stac_id}/cmems_obs-wave_glo_phy-swh_nrt_s6b-lr-l3_PT1S_202411",
          "cmems_obs-wave_glo_phy-wind_nrt_j3-l3_PT1S_202411": f"{A.S3}/{A.BUCKET}/native/y"}
    web.urls.update(fake_stac(ds))
    got = A.stac_datasets(A.HEIGHTS, get=web)
    assert sorted(got) == ["cmems_obs-wave_glo_phy-swh_nrt_j3-l3_PT1S_202411", "cmems_obs-wave_glo_phy-swh_nrt_s6b-lr-l3_PT1S_202411"]
    web2 = FakeWeb()
    web2.urls[f"{A.S3}/{A.BUCKET}/?list-type=2&prefix=native/{A.HEIGHTS.stac_id}/&delimiter=/"] = listing_xml([], prefixes=[
        f"native/{A.HEIGHTS.stac_id}/cmems_obs-wave_glo_phy-swh_nrt_al-l3_PT1S_202411/", f"native/{A.HEIGHTS.stac_id}/cmems_obs-wave_glo_phy-swh_nrt_al-l3_PT0.2S_202411/"])
    assert not [u for u in web.log if "PT0.2S" in u or "wind_nrt" in u], "only the 1 Hz wave datasets' items are fetched"
    got2, how, _ = A.discover_datasets(A.HEIGHTS, {}, get=web2, log=lambda *a: None, now=NOW)
    assert how == "bucket" and list(got2) == ["cmems_obs-wave_glo_phy-swh_nrt_al-l3_PT1S_202411"]
    cached, how3, _ = A.discover_datasets(A.HEIGHTS, {"datasets": {"d": "https://h/b/p"}}, get=FakeWeb(), log=lambda *a: None, now=NOW)
    assert how3 == "pointer" and cached == {"d": "https://h/b/p"}, "STAC down: the cached list whatever its age"
    fresh = {"datasets": {"d": "https://h/b/p"}, "datasets_utc": A.utc(NOW - timedelta(hours=5))}
    web3 = FakeWeb(); web3.urls.update(fake_stac(ds))
    assert A.discover_datasets(A.HEIGHTS, fresh, get=web3, now=NOW)[1] == "pointer" and not web3.log, "a list under 6 h old: STAC is not read"
    fresh["datasets_utc"] = A.utc(NOW - timedelta(hours=7))
    assert A.discover_datasets(A.HEIGHTS, fresh, get=web3, now=NOW)[1] == "stac"


def test_windows_in_reach_and_the_plan_digests():
    p = A.HEIGHTS
    ws = A.windows_for(p, NOW)                                          # keep 30 h: 09Z two days back .. 15Z today
    assert ws[0] == datetime(2026, 11, 3, 9, tzinfo=timezone.utc) and ws[-1] == datetime(2026, 11, 4, 15, tzinfo=timezone.utc) and len(ws) == 11
    files = [A.FileRec("j3", "a.nc", "u", 10, ws[3], ws[3] + timedelta(hours=3), ws[3]), A.FileRec("s3a", "b.nc", "u", 20, ws[3], ws[3] + timedelta(hours=3), ws[3]),
             A.FileRec("j3", "c.nc", "u", 10, ws[4], ws[4] + timedelta(hours=3), ws[4])]
    pl = A.plan(p, {}, files, NOW)
    k3, k4 = A.window_key(ws[3]), A.window_key(ws[4])
    assert pl[k3]["build"] and pl[k4]["build"] and not pl[A.window_key(ws[0])]["build"], "windows without files are not built"
    d3 = pl[k3]["digest"]
    assert d3 == A.digest_of(p, files[:2]) and d3 != pl[k4]["digest"]
    ptr = {"chunks": [{"window": k3, "inputs": d3}, {"window": k4, "inputs": "old"}]}
    pl2 = A.plan(p, ptr, files, NOW)
    assert not pl2[k3]["build"] and pl2[k4]["build"], "only a changed digest rebuilds"
    assert A.plan(p, ptr, files, NOW, force=True)[k3]["build"]
    assert A.plan(p, ptr, files, NOW, only=k3)[k3]["build"] and k4 not in A.plan(p, ptr, files, NOW, only=k3)
    files[1].size = 21
    assert A.plan(p, ptr, files, NOW)[k3]["build"], "a regenerated file (another size) changes the digest"
    # a daily swell file touches every window it spans
    big = A.FileRec("s1a", "d.nc", "u", 5, ws[0] - timedelta(hours=9), ws[0] + timedelta(hours=15), ws[0])
    assert all(A.plan(S.SWELL, {}, [big], NOW)[A.window_key(w)]["build"] for w in ws[:5])
    assert not A.plan(S.SWELL, {}, [big], NOW)[A.window_key(ws[6])]["build"]


# ------------------------------------------------------------------ the files
def write_l3(path, t, lat, lon, swh, scale=0.001, fill=-32767, tunits="seconds since 2000-01-01 00:00:00", latscale=1e-6):
    with h5py.File(path, "w") as f:
        d = f.create_dataset("time", data=np.asarray(t, np.float64)); d.attrs["units"] = tunits
        d = f.create_dataset("latitude", data=np.rint(np.asarray(lat) / latscale).astype(np.int32)); d.attrs["scale_factor"] = np.array([latscale]); d.attrs["units"] = "degrees_north"
        d = f.create_dataset("longitude", data=np.rint(np.asarray(lon) / latscale).astype(np.int32)); d.attrs["scale_factor"] = np.array([latscale]); d.attrs["units"] = "degrees_east"
        raw = np.where(np.isnan(swh), fill, np.rint(np.asarray(swh, np.float64) / scale)).astype(np.int16)
        d = f.create_dataset("VAVH", data=raw); d.attrs["scale_factor"] = np.array([scale]); d.attrs["_FillValue"] = np.array([fill], np.int16); d.attrs["units"] = "m"


def test_read_l3_scales_fills_window_edges_and_longitudes_and_refuses_a_wrong_scale(tmp_path):
    path = str(tmp_path / "l3.nc")
    w0 = datetime(2026, 11, 4, 9, tzinfo=timezone.utc)
    t = np.array([ss(w0) - 1, ss(w0), ss(w0) + 100, ss(w0) + 200, ss(w0) + 3 * 3600 - 1, ss(w0) + 3 * 3600])
    write_l3(path, t, [10, 20, 30, 40, 50, 60], [190.5, -10, 0, 359.999999, 180, 10], [1.0, 2.0, np.nan, 31, 0.123, 5])
    d = A.read_l3(path, w0, w0 + timedelta(hours=3))
    assert d["t"].tolist() == [t[1], t[4]], "the window is half-open [t0, t1); the fill and 31 m dropped"
    assert d["lon"].tolist() == [-10.0, -180.0] and d["swh"].tolist() == [2.0, pytest.approx(0.123)]
    write_l3(path, t, [10] * 6, [0] * 6, [1] * 6, scale=0.01)
    with pytest.raises(ValueError):
        A.read_l3(path)
    write_l3(path, t, [10] * 6, [0] * 6, [1] * 6, tunits="seconds since 1981-01-01")
    with pytest.raises(ValueError):
        A.read_l3(path)
    write_l3(path, t, [10] * 6, [0] * 6, [1] * 6, latscale=1e-5)
    with pytest.raises(ValueError):
        A.read_l3(path)
    write_l3(path, t, [10] * 6, [0] * 6, [1] * 6, fill=-9999)
    with pytest.raises(ValueError):
        A.read_l3(path)
    write_l3(path, t, [10, 20, 30, 40, 50, 60], [190.5, -10, 0, 1, 180, 10], [1.0, 2.0, 1, 1, 0.123, 5])
    d = A.read_l3(path)
    assert d["lon"][0] == pytest.approx(-169.5) and len(d["t"]) == 6, "no window: everything with a value, lon wrapped"


# ------------------------------------------------------------------ the model
def frame_png(fn, lo=E.FIELDS["hs"]["lo"], hi=E.FIELDS["hs"]["hi"]):
    """A 721 x 1440 hs frame PNG from fn(lat, lon) -> m (NaN = absent)."""
    lat = 90 - 0.25 * np.arange(721)[:, None]
    lon = -180 + 0.25 * np.arange(1440)[None, :]
    v = fn(lat + 0 * lon, lon + 0 * lat)
    q = E.quantize(v, lo, hi)
    buf = io.BytesIO()
    Image.fromarray(q, "L").save(buf, "PNG")
    return buf.getvalue()


def manifest_json(run, lo=E.FIELDS["hs"]["lo"], hi=E.FIELDS["hs"]["hi"]):
    return {"schema": 3, "run": run, "run_utc": f"{run[:4]}-{run[4:6]}-{run[6:8]}T{run[8:]}:00:00Z", "encoding": E.ENCODING, "complete": True,
            "grid": {"cols": 1440, "rows": 721, "lon0": -180.0, "lat0": 90.0, "dlon": 0.25, "dlat": -0.25, "registration": "center", "lon_periodic": True},
            "fields": {"hs": {"lo": lo, "hi": hi, "legend": [0, 18.288], "resolutions": ["full", "half"]}}, "frames": [], "published_utc": "2026-11-04T10:30:00Z"}


def add_frames(web, run, steps, fn, root=A.FRAMES_ROOT, lo=E.FIELDS["hs"]["lo"], hi=E.FIELDS["hs"]["hi"], live=True):
    if live:
        web.urls[f"{root}/{P.PREFIX}/latest.json"] = {"run": run, "manifest": f"{P.PREFIX}/{run}/manifest-x.json", "complete": True, "published_utc": "2026-11-04T10:30:00Z"}
        web.urls[f"{root}/{P.PREFIX}/{run}/manifest-x.json"] = manifest_json(run, lo, hi)
    for s in steps:
        web.urls[f"{root}/{P.frame_key(run, 'hs', s)}"] = frame_png(lambda lat, lon, s=s: fn(lat, lon, s), lo, hi)


def test_sample_bilinear_is_the_clients_rule_and_near_land_reads_the_fill_mask():
    q = np.zeros((721, 1440), np.uint8)
    q[0:2, 0:2] = [[100, 200], [120, 220]]                              # nodes (90, -180), (90, -179.75), (89.75, -180), (89.75, -179.75)
    code, edge = A.sample_bilinear(q, [89.875, 90, 89.875], [-179.875, -180, -179.875 + 360])
    assert code[0] == pytest.approx((100 + 200 + 120 + 220) / 4) and code[1] == 100 and code[2] == pytest.approx(code[0]), "world copies sample the one world"
    assert not edge[0] and not edge[1]
    q[0, 1] = 0                                                         # an absent neighbour: dropped from the mean, edge flagged
    code, edge = A.sample_bilinear(q, [89.875], [-179.875])
    assert code[0] == pytest.approx((100 + 120 + 220) / 3) and edge[0]
    q[0, 0] = q[1, 0] = 0
    code, _ = A.sample_bilinear(q, [89.875], [-179.875])
    assert code[0] == pytest.approx(220.0) and np.isnan(A.sample_bilinear(q, [89.95], [-179.95])[0][0]), "w < 0.25 is none"
    # the date-line column pairs with column 0
    q[:] = 0; q[100, 1439] = 50; q[100, 0] = 150
    assert A.sample_bilinear(q, [65], [179.875])[0][0] == pytest.approx(100.0)
    allow = np.zeros((721, 1440), bool); allow[300, 700] = True       # node (15 N, -5 E)
    assert A.near_land_of([15.1, 15.3, 14.76], [-5.1, -5.3, -5.24], allow).tolist() == [True, False, True]


def test_collocation_matches_a_hand_computed_value_newest_run_first_and_falls_back_on_a_404(tmp_path):
    web = FakeWeb()
    # hs = 0.01 * step + 0.1 * lat + 0.02 * lon (m): linear, so bilinear + linear-in-time is exact up to the codes
    fn = lambda lat, lon, s: 2.0 + 0.01 * s + 0.01 * lat + 0.002 * lon    # noqa: E731
    add_frames(web, "2026110406", range(0, 14), fn)                     # the live run, frames to +13 h
    add_frames(web, "2026110400", [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], fn, live=False)   # the older run: frames 5.. (no 0..4)
    fr = A.Frames(get=web).load()
    assert fr.run == "2026110406" and fr.lo == 0.0 and fr.hi == E.HS_TOP
    r6 = datetime(2026, 11, 4, 6, tzinfo=timezone.utc)
    t = np.array([ss(r6) + 2.5 * 3600, ss(r6) - 0.5 * 3600, ss(r6) - 5.9 * 3600, ss(r6) + 12 * 3600, ss(r6) - 7 * 3600])
    lat = np.array([10.0, 20.0, -30.0, 0.0, 5.0]); lon = np.array([-150.0, 10.0, 170.0, 0.0, 0.0])
    model, run_idx, edge, runs = A.collocate(fr, t, lat, lon, log=lambda *a: None)
    assert runs == ["2026110406", "2026110400", "2026110318", "2026110312"]
    k = (E.HS_TOP - 0) / 254
    assert abs(model[0] - fn(10, -150, 2.5)) < k and run_idx[0] == 0, "the live run, linear between its 2 h and 3 h frames"
    assert abs(model[1] - fn(20, 10, 5.5)) < k and run_idx[1] == 1, "before the live cycle: the 00Z run at +5.5 h"
    assert np.isnan(model[2]) and run_idx[2] == 255, "the 00Z run has no frame 0: nothing older has frames either"
    assert abs(model[3] - fn(0, 0, 12)) < k and run_idx[3] == 0, "exactly on an hour: that frame (weight 1)"
    assert np.isnan(model[4]), "older than every run with frames"
    assert not edge[:2].any()
    # a frame of the live run gone (404) -> the older run serves that time
    del web.urls[f"{A.FRAMES_ROOT}/{P.frame_key('2026110406', 'hs', 3)}"]
    fr2 = A.Frames(get=web).load()
    model2, run2, _, _ = A.collocate(fr2, t[:1], lat[:1], lon[:1], log=lambda *a: None)
    assert run2[0] == 1 and abs(model2[0] - fn(10, -150, 8.5)) < k
    # a range change refuses
    web.urls[f"{A.FRAMES_ROOT}/{P.PREFIX}/2026110406/manifest-x.json"] = manifest_json("2026110406", 0, 15.0)
    with pytest.raises(A.Frames.RangeChanged):
        A.Frames(get=web).load()


# ------------------------------------------------------------------ main, end to end
def setup_world(tmp_path, now=NOW, windows=2, missions=("j3", "s3a"), fn=None):
    """A fake web: STAC, the listing of one dataset per mission with one file per window (the last `windows` windows
    ending before now), the files on disk, the frames; -> (web, files by window)."""
    web = FakeWeb()
    fn = fn or (lambda lat, lon, s: 1.0 + 0.01 * s + 0.01 * lat)
    run = (now - timedelta(hours=5)).replace(minute=0, second=0, microsecond=0)
    run = run.replace(hour=(run.hour // 6) * 6)
    add_frames(web, P.run_key(run), range(0, 37), fn)
    ds = {f"cmems_obs-wave_glo_phy-swh_nrt_{m}-l3_PT1S_202411": f"{A.S3}/{A.BUCKET}/native/{A.HEIGHTS.stac_id}/cmems_obs-wave_glo_phy-swh_nrt_{m}-l3_PT1S_202411" for m in missions}
    web.urls.update(fake_stac(ds))
    ws = [w for w in A.windows_for(A.HEIGHTS, now) if w + timedelta(hours=3) <= now][-windows:]
    by_window = {}
    for m, native in zip(missions, ds.values()):
        entries = []
        for w in ws:
            name = f"global_vavh_l3_rt_{m}_{w:%Y%m%dT%H%M%S}_{w + timedelta(hours=3):%Y%m%dT%H%M%S}_{w + timedelta(hours=4):%Y%m%dT%H%M%S}.nc"
            path = str(tmp_path / name)
            t = ss(w) + np.linspace(10, 3 * 3600 - 10, 40)
            lat = 10 + 20 * np.sin(np.linspace(0, 3, 40)) + (5 if m == "s3a" else 0)
            lon = -150 + np.linspace(0, 60, 40)
            swh = 1.5 + 0.5 * np.sin(np.linspace(0, 6, 40))
            write_l3(path, t, lat, lon, swh)
            size = os.path.getsize(path)
            key = f"{native.split('/', 4)[4]}/{w:%Y}/{w:%m}/{name}"
            web.urls[f"{A.S3}/{A.BUCKET}/{key}"] = path
            entries.append((key, size))
            by_window.setdefault(A.window_key(w), []).append((name, size, path, m))
        for month in A.months_touching(now - timedelta(hours=A.KEEP_H), now):
            web.urls[f"{A.S3}/{A.BUCKET}/?list-type=2&prefix={native.split('/', 4)[4]}/{month:%Y}/{month:%m}/&max-keys=1000"] = listing_xml([e for e in entries if f"/{month:%Y}/{month:%m}/" in e[0]])
    return web, by_window, ws


def puts(fc, prefix="alt/v1/"):
    return [k for op, k in fc.log if op == "put" and k.startswith(prefix)]


def test_main_builds_the_windows_in_reach_publishes_the_pointer_last_then_only_heartbeats(tmp_path):
    web, by_window, ws = setup_world(tmp_path)
    fc = FakeClient(); store = P.Store(fc, "b")
    assert A.main([], get=web, store=store, now=NOW) == 0
    order = puts(fc)
    assert order[-1] == "alt/v1/latest.json" and order[-2] == f"alt/v1/stats/{A.stamp(NOW)}.json" and len([k for k in order if k.startswith("alt/v1/w/")]) == 2
    ptr = json.loads(fc.objects["alt/v1/latest.json"]["body"])
    assert ptr["format"] == "alt-v1" and ptr["magic"] == "ALT1" and ptr["published_utc"] == ptr["checked_utc"] == A.utc(NOW) and ptr["build"] == A.stamp(NOW)
    assert [c["window"] for c in ptr["chunks"]] == sorted(by_window) and ptr["columns"] == F.ALT_COLUMNS and ptr["encoding"] == "deflate"
    assert [m["id"] for m in ptr["missions"]] == ["j3", "s3a"] and ptr["missions"][0]["short"] == "Jason-3" and ptr["source"]["doi"] == "10.48670/moi-00179"
    assert ptr["model"]["hs"] == [0.0, E.HS_TOP] and len(ptr["model"]["runs"]) == 4 and "datasets" in ptr and ptr["window_s"] == 10800 and ptr["keep_h"] == 30
    for c in ptr["chunks"]:
        assert c["key"] in fc.objects and fc.objects[c["key"]]["cc"] == P.IMMUTABLE and c["n"] == 80 and c["missions"] == ["j3", "s3a"] and c["inputs"]
        h, cols = F.decode_chunk(fc.objects[c["key"]]["body"], magic=F.MAGIC_ALT)
        assert h["window"] == c["window"] and h["segments"] == [[0, 0, 40], [1, 40, 40]] and h["credit"] == A.SOURCE["credit"] and h["model"]["lo"] == 0.0
        v = F.values(h, cols)
        # the readings = the file: 20 random ones per mission
        for name, _size, path, m in by_window[c["window"]]:
            d = A.read_l3(path)
            seg = [s for s in h["segments"] if h["missions"][s[0]]["id"] == m][0]
            rng = np.random.default_rng(1)
            for i in rng.integers(0, 40, 20):
                j = seg[1] + i
                assert abs(v["t"][j] - d["t"][i]) <= 1 and abs(v["lat"][j] - d["lat"][i]) <= 1e-6 and abs(v["lon"][j] - d["lon"][i]) <= 1e-6 and abs(v["swh"][j] - d["swh"][i]) <= 0.0005
        assert np.isfinite(v["model"]).all() and (v["run"] == 0).all(), "every reading collocated against the live run"
        assert (np.abs(v["model"] - (1.0 + 0.01 * ((v["t"] - ss(datetime.strptime(ptr['model']['runs'][0], '%Y%m%d%H').replace(tzinfo=timezone.utc))) / 3600) + 0.01 * v["lat"])) < E.HS_TOP / 254 + 1e-9).all()
    stats = json.loads(fc.objects[ptr["stats"]]["body"])
    assert set(stats["windows"]) == set(by_window) and stats["windows"][ptr["chunks"][0]["window"]]["obs_minus_model_open"]["n"] > 0
    # the same inputs: a heartbeat only (checked_utc moves, nothing else is written)
    n_before = len(fc.log)
    later = NOW + timedelta(minutes=10)
    assert A.main([], get=web, store=store, now=later) == 0
    assert puts(fc)[len([k for k in puts(fc)]) - 1] == "alt/v1/latest.json" and len([k for k in fc.log[n_before:] if k[0] == "put"]) == 1
    ptr2 = json.loads(fc.objects["alt/v1/latest.json"]["body"])
    assert ptr2["checked_utc"] == A.utc(later) and ptr2["published_utc"] == A.utc(NOW) and ptr2["chunks"] == ptr["chunks"] and ptr2["build"] == ptr["build"]


def test_main_rebuilds_only_the_window_whose_file_was_regenerated_keeps_old_keys_and_prunes_them_later(tmp_path):
    web, by_window, ws = setup_world(tmp_path)
    fc = FakeClient(); store = P.Store(fc, "b")
    assert A.main([], get=web, store=store, now=NOW) == 0
    ptr = json.loads(fc.objects["alt/v1/latest.json"]["body"])
    old_keys = {c["window"]: c["key"] for c in ptr["chunks"]}
    # the newest window's j3 file is regenerated: a new name (production stamp) and size
    w = ws[-1]
    name, size, path, m = [x for x in by_window[A.window_key(w)] if x[3] == "j3"][0]
    new_name = name.replace(f"{w + timedelta(hours=4):%Y%m%dT%H%M%S}", f"{w + timedelta(hours=5):%Y%m%dT%H%M%S}")
    t = ss(w) + np.linspace(10, 3 * 3600 - 10, 41)
    write_l3(str(tmp_path / new_name), t, 10 + 0 * t, -150 + 0 * t, 2.0 + 0 * t)
    native = f"native/{A.HEIGHTS.stac_id}/cmems_obs-wave_glo_phy-swh_nrt_j3-l3_PT1S_202411"
    web.urls[f"{A.S3}/{A.BUCKET}/{native}/{w:%Y}/{w:%m}/{new_name}"] = str(tmp_path / new_name)
    lk = f"{A.S3}/{A.BUCKET}/?list-type=2&prefix={native}/{w:%Y}/{w:%m}/&max-keys=1000"
    entries = A.parse_listing(web.urls[lk])[0] + [(f"{native}/{w:%Y}/{w:%m}/{new_name}", os.path.getsize(tmp_path / new_name))]
    web.urls[lk] = listing_xml(entries)
    later = NOW + timedelta(hours=1)
    assert A.main([], get=web, store=store, now=later) == 0
    ptr2 = json.loads(fc.objects["alt/v1/latest.json"]["body"])
    keys2 = {c["window"]: c["key"] for c in ptr2["chunks"]}
    assert keys2[A.window_key(ws[-2])] == old_keys[A.window_key(ws[-2])], "the untouched window keeps its key"
    assert keys2[A.window_key(w)] != old_keys[A.window_key(w)] and keys2[A.window_key(w)].endswith(f"/{A.stamp(later)}.bin")
    assert [c for c in ptr2["chunks"] if c["window"] == A.window_key(w)][0]["n"] == 81 and ptr2["published_utc"] == A.utc(later)
    assert old_keys[A.window_key(w)] in fc.objects, "the superseded build stays for the clients still reading it"
    assert [e["key"] for e in ptr2["superseded"]] == [old_keys[A.window_key(w)]] and ptr2["superseded"][0]["at"] == A.utc(later)
    # a build superseded 2 h 10 min after its stamp (review P1): the grace runs from the supersession, so it stays 2 h more
    assert A.prune(store, A.HEIGHTS, ptr2, now=later + timedelta(hours=1, minutes=59)) == []
    assert old_keys[A.window_key(w)] in fc.objects
    # 3 h later the superseded build is pruned; 40 h later the windows out of reach go whole, their failure records too
    fc.objects["alt/v1/failed/" + A.window_key(ws[-2]) + ".json"] = {"body": b"{}", "ct": "", "cc": ""}
    fc.objects["sst/v1/latest.json"] = {"body": b"{}", "ct": "", "cc": ""}
    fc.objects["alt/v1/stats/20261101T000000Z.json"] = {"body": b"{}", "ct": "", "cc": ""}
    gone = A.prune(store, A.HEIGHTS, ptr2, now=later + timedelta(hours=2, minutes=1))
    assert set(gone) == {old_keys[A.window_key(w)], "alt/v1/stats/20261101T000000Z.json"}
    # the entry leaves the pointer's superseded list with the next run; a run 3 h on carries none
    assert A.superseded_list(ptr2, ptr2["chunks"], later + timedelta(hours=3), ptr2["superseded"]) == []
    assert A.superseded_list(ptr2, ptr2["chunks"], later + timedelta(hours=1), ptr2["superseded"]) == ptr2["superseded"]
    assert A.prune(store, A.HEIGHTS, ptr2, now=later + timedelta(hours=40)) == ["alt/v1/failed/" + A.window_key(ws[-2]) + ".json"], "a window the pointer still names is never deleted whole (its old failure record goes)"
    fc.objects["alt/v1/failed/" + A.window_key(ws[-2]) + ".json"] = {"body": b"{}", "ct": "", "cc": ""}
    gone2 = A.prune(store, A.HEIGHTS, dict(ptr2, chunks=[]), now=later + timedelta(hours=40))
    assert set(gone2) >= set(keys2.values()) | {"alt/v1/failed/" + A.window_key(ws[-2]) + ".json"}
    assert ptr2["stats"] in fc.objects and ptr["stats"] not in fc.objects, "the stats the pointer's build names stay; a build no chunk names any more goes after 24 h"
    assert "sst/v1/latest.json" in fc.objects and not [k for k in fc.objects if k.startswith("alt/v1/w/")]


def test_main_carries_a_window_over_a_listing_outage_records_a_failed_download_and_backs_off(tmp_path):
    web, by_window, ws = setup_world(tmp_path)
    fc = FakeClient(); store = P.Store(fc, "b")
    assert A.main([], get=web, store=store, now=NOW) == 0
    ptr = json.loads(fc.objects["alt/v1/latest.json"]["body"])
    # the s3a dataset's listing is down: the window's digest changes (one mission fewer) and the chunk is rebuilt without it;
    # once the listing is back the digest returns and the full chunk is rebuilt (the owner's rule: what is listed is drawn)
    native = f"native/{A.HEIGHTS.stac_id}/cmems_obs-wave_glo_phy-swh_nrt_s3a-l3_PT1S_202411"
    for u in [u for u in web.urls if u.startswith(f"{A.S3}/{A.BUCKET}/?list-type=2&prefix={native}/")]:
        web.down.add(u)
    later = NOW + timedelta(minutes=20)
    assert A.main([], get=web, store=store, now=later) == 0
    ptr2 = json.loads(fc.objects["alt/v1/latest.json"]["body"])
    assert all(c["missions"] == ["j3"] and c["n"] == 40 for c in ptr2["chunks"])
    web.down.clear()
    assert A.main([], get=web, store=store, now=later + timedelta(minutes=10)) == 0
    ptr3 = json.loads(fc.objects["alt/v1/latest.json"]["body"])
    assert all(c["missions"] == ["j3", "s3a"] and c["inputs"] == o["inputs"] for c, o in zip(ptr3["chunks"], ptr["chunks"]))
    # a regenerated file (the window's digest changes) whose download fails: the window is built without it (inputs null:
    # tried again), the failure recorded; after FAILED_MAX_ATTEMPTS on the same digest the window is left alone
    w = ws[-1]
    name, size, path, m = [x for x in by_window[A.window_key(w)] if x[3] == "j3"][0]
    new_name = name.replace(f"{w + timedelta(hours=4):%Y%m%dT%H%M%S}", f"{w + timedelta(hours=5):%Y%m%dT%H%M%S}")
    native = f"native/{A.HEIGHTS.stac_id}/cmems_obs-wave_glo_phy-swh_nrt_j3-l3_PT1S_202411"
    lk = f"{A.S3}/{A.BUCKET}/?list-type=2&prefix={native}/{w:%Y}/{w:%m}/&max-keys=1000"
    entries = [e for e in A.parse_listing(web.urls[lk])[0] if not e[0].endswith(name)] + [(f"{native}/{w:%Y}/{w:%m}/{new_name}", size + 7)]
    web.urls[lk] = listing_xml(entries)
    url = f"{A.S3}/{A.BUCKET}/{native}/{w:%Y}/{w:%m}/{new_name}"
    web.urls[url] = path
    web.down.add(url)
    orig = A.time.sleep
    A.time.sleep = lambda s: None
    try:
        for i in range(A.FAILED_MAX_ATTEMPTS):
            assert A.main([], get=web, store=store, now=later + timedelta(hours=1, minutes=10 * i)) == 0
            rec = json.loads(fc.objects["alt/v1/failed/" + A.window_key(w) + ".json"]["body"])
            assert rec["attempts"] == i + 1 and "1 of 2 files missing" in rec["last_error"] and rec["digest"]
        p4 = json.loads(fc.objects["alt/v1/latest.json"]["body"])
        c = [c for c in p4["chunks"] if c["window"] == A.window_key(w)][0]
        assert c["inputs"] is None and c["missions"] == ["s3a"]
        n_puts = len(puts(fc))
        assert A.main([], get=web, store=store, now=later + timedelta(hours=2)) == 0
        assert len(puts(fc)) == n_puts + 1, "backed off: the heartbeat alone"
        web.down.clear()
        web.urls[url] = str(tmp_path / new_name)
        t = ss(w) + np.linspace(10, 3 * 3600 - 10, 41)
        write_l3(str(tmp_path / new_name), t, 10 + 0 * t, -150 + 0 * t, 2.0 + 0 * t)
        web.urls[lk] = listing_xml(entries[:-1] + [(entries[-1][0], os.path.getsize(tmp_path / new_name))])
        assert A.main([], get=web, store=store, now=later + timedelta(hours=2, minutes=10)) == 0
        p5 = json.loads(fc.objects["alt/v1/latest.json"]["body"])
        c5 = [c for c in p5["chunks"] if c["window"] == A.window_key(w)][0]
        assert c5["missions"] == ["j3", "s3a"] and c5["n"] == 81 and c5["inputs"]
    finally:
        A.time.sleep = orig


def test_stale_alarm_once_per_12h_never_with_a_window_and_a_local_build_touches_no_bucket(tmp_path):
    web, by_window, ws = setup_world(tmp_path)
    fc = FakeClient(); store = P.Store(fc, "b")
    assert A.main([], get=web, store=store, now=NOW) == 0
    late = NOW + timedelta(hours=14)                                    # the newest reading is now > 12 h old; nothing new listed
    for u in [u for u in web.urls if "list-type=2" in u]:
        web.urls[u] = listing_xml([])
    assert A.main([], get=web, store=store, now=late) == 1
    assert "alt/v1/stale-alarm.json" in fc.objects
    assert A.main([], get=web, store=store, now=late + timedelta(minutes=10)) == 0, "once in 12 h"
    alarm = fc.objects["alt/v1/stale-alarm.json"]["body"]
    assert A.main(["--window", A.window_key(ws[-1])], get=web, store=store, now=late + timedelta(hours=13)) == 1, "a manual window that finds nothing fails (red), never alarms"
    assert fc.objects["alt/v1/stale-alarm.json"]["body"] == alarm
    assert A.main([], get=web, store=store, now=late + timedelta(hours=13)) == 1
    # a local build writes files only
    fc2 = FakeClient(); store2 = P.Store(fc2, "b")
    (tmp_path / "w2").mkdir()
    web2, _, _ = setup_world(tmp_path / "w2")
    out = tmp_path / "out"
    assert A.main(["--local", str(out)], get=web2, store=store2, now=NOW) == 0
    assert fc2.objects == {} and (out / "alt" / "v1" / "latest.json").exists()
    lp = json.loads((out / "alt" / "v1" / "latest.json").read_text())
    assert len(lp["chunks"]) == 2 and all((out / c["key"]).exists() for c in lp["chunks"])
    assert A.main(["--dry-run"], get=web2, store=store2, now=NOW) == 0 and fc2.objects == {}
    assert A.main(["--keep-h", "1"]) == 2 and A.main(["--window", "2026110"]) == 2


def test_a_model_range_change_refuses_and_an_unavailable_frames_host_records_the_windows(tmp_path):
    web, by_window, ws = setup_world(tmp_path)
    fc = FakeClient(); store = P.Store(fc, "b")
    run = [u for u in web.urls if u.endswith("/manifest-x.json")][0]
    good = web.urls[run]
    web.urls[run] = manifest_json(re.search(r"/(\d{10})/", run).group(1), 0, 15.0)
    assert A.main([], get=web, store=store, now=NOW) == 1 and "alt/v1/latest.json" not in fc.objects
    assert A.main([], get=web, store=store, now=NOW + timedelta(minutes=10)) == 0, "the range alarm goes out once in 12 h (144 red runs a day otherwise)"
    assert A.main([], get=web, store=store, now=NOW + timedelta(hours=13)) == 1 and "alt/v1/latest.json" not in fc.objects
    web.urls[run] = good
    web.down.add(f"{A.FRAMES_ROOT}/{P.PREFIX}/latest.json")
    assert A.main([], get=web, store=store, now=NOW) == 1, "nothing published yet counts as stale: the alarm goes out"
    assert not [k for k in fc.objects if k.startswith("alt/v1/failed/")], "a frames outage is charged to no window (the next run simply tries again)"
    assert "alt/v1/latest.json" not in fc.objects
    web.down.clear()
    assert A.main([], get=web, store=store, now=NOW + timedelta(minutes=10)) == 0 and "alt/v1/latest.json" in fc.objects, "...and builds as soon as the host is back"
    fc.objects.pop("alt/v1/latest.json"); fc.objects.pop("alt/v1/stale-alarm.json", None)
    for k in [k for k in fc.objects if k.startswith("alt/v1/")]:
        fc.objects.pop(k)
    assert A.main(["--no-model"], get=web, store=store, now=NOW + timedelta(minutes=10)) == 0
    ptr = json.loads(fc.objects["alt/v1/latest.json"]["body"])
    h, cols = F.decode_chunk(fc.objects[ptr["chunks"][0]["key"]]["body"])
    assert (cols["model"] == F.NONE_U2).all() and (cols["run"] == 255).all() and h["model"]["lo"] is None


def test_jason3_ogdr_readings_join_as_a_mission_and_duplicates_of_the_cmems_pass_are_dropped(tmp_path):
    web, by_window, ws = setup_world(tmp_path, missions=("j3",))
    w = ws[-1]
    # an OGDR file over the newest window: 10 readings at the CMEMS times (duplicates) + 5 later ones (CMEMS not in yet)
    cm = A.read_l3(by_window[A.window_key(w)][0][2])
    t = np.concatenate([cm["t"][:10] + 0.4, cm["t"][-1] + 2 + np.arange(5) * 1.0])
    name = f"JA3_OPN_2PfS312_100_{w:%Y%m%d_%H%M%S}_{w + timedelta(hours=3):%Y%m%d_%H%M%S}.nc"
    path = str(tmp_path / name)
    with h5py.File(path, "w") as f:
        g = f.create_group("data_01")
        d = g.create_dataset("time", data=t); d.attrs["units"] = "seconds since 2000-01-01 00:00:00.0"
        d = g.create_dataset("latitude", data=np.rint(np.full(15, 12.0) / 1e-6).astype(np.int32)); d.attrs["scale_factor"] = np.array([1e-6]); d.attrs["units"] = "degrees_north"
        d = g.create_dataset("longitude", data=np.rint(np.full(15, 200.0) / 1e-6).astype(np.int32)); d.attrs["scale_factor"] = np.array([1e-6]); d.attrs["units"] = "degrees_east"
        ku = g.create_group("ku")
        d = ku.create_dataset("swh_ocean", data=np.rint(np.full(15, 1.75) / 0.001).astype(np.int16)); d.attrs["scale_factor"] = np.array([0.001]); d.attrs["units"] = "m"; d.attrs["_FillValue"] = np.array([32767], np.int16)
        q = np.zeros(15, np.int8); q[14] = 1                            # the last one flagged bad
        ku.create_dataset("swh_ocean_qual", data=q)
    d = A.read_jason3_ogdr(path)
    assert len(d["t"]) == 14 and d["lon"][0] == pytest.approx(-160.0)
    index = "https://ogdr.example/cycle312/"
    web.urls[index] = f'<html><a href="{name}">{name}</a> 2026-11-04 12:00 1.2M <a href="other.txt">x</a></html>'.encode()
    web.urls[index + name] = path
    fc = FakeClient(); store = P.Store(fc, "b")
    assert A.main(["--jason3-index", index], get=web, store=store, now=NOW) == 0
    ptr = json.loads(fc.objects["alt/v1/latest.json"]["body"])
    c = [c for c in ptr["chunks"] if c["window"] == A.window_key(w)][0]
    assert c["missions"] == ["j3", "j3_ogdr"] and c["n"] == 40 + 4, "10 duplicates within 1.5 s of the CMEMS pass dropped, the flagged one too"
    h, cols = F.decode_chunk(fc.objects[c["key"]]["body"])
    assert [m["id"] for m in h["missions"]] == ["j3", "j3_ogdr"] and h["segments"] == [[0, 0, 40], [1, 40, 4]]
    # an unreachable index is a WARNING, not a failure
    web.down.add(index)
    assert A.main(["--jason3-index", index], get=web, store=store, now=NOW + timedelta(minutes=10)) == 0
    # coverage, not point matching: a CMEMS pass (readings 60 s apart or closer) covers every OGDR reading inside it, the
    # ones CMEMS rejected included; a gap over 60 s ends a pass
    assert A.dedupe_against(np.array([10.0, 20.0]), np.array([8.4, 9.0, 11.6, 15.0, 21.0, 30.0])).tolist() == [0, 5]
    assert A.dedupe_against(np.array([10.0, 20.0, 200.0, 210.0]), np.array([100.0, 150.0, 199.0, 205.0, 212.0])).tolist() == [0, 1, 4]
    h, cols = F.decode_chunk(fc.objects[c["key"]]["body"])
    assert "NOAA" in h["credit"] and A.SOURCE["credit"] in h["credit"], "both sources credited in a chunk holding OGDR readings"


# ------------------------------------------------------------------ the swell job
def write_swell(path, t, lat, lon, hs, tp, dr, period_name="Peak period of the swell partition", dir_name="Direction the swell partition comes from (degrees true)", two_d=True):
    with h5py.File(path, "w") as f:
        d = f.create_dataset("time", data=np.asarray(t, np.float64)); d.attrs["units"] = "seconds since 2000-01-01T00:00:00Z"
        d = f.create_dataset("latitude", data=np.asarray(lat, np.float32)); d.attrs["units"] = "degrees_north"
        d = f.create_dataset("longitude", data=np.asarray(lon, np.float32)); d.attrs["units"] = "degrees_east"
        fill = -32767

        def var(name, data, scale, units, long_name):
            if scale is None:                                           # a float variable, unscaled, NaN as its fill
                data = np.asarray(data, np.float32)
                for k in range(data.shape[1] if not two_d else 1):
                    d = f.create_dataset(name if two_d else f"{name}{k + 1}", data=data if two_d else data[:, k])
                    d.attrs["units"] = units; d.attrs["long_name"] = long_name
                return
            raw = np.where(np.isnan(data), fill, np.rint(np.asarray(data, np.float64) / scale)).astype(np.int16)
            if two_d:
                d = f.create_dataset(name, data=raw)
                d.attrs["scale_factor"] = np.array([scale]); d.attrs["_FillValue"] = np.array([fill], np.int16); d.attrs["units"] = units; d.attrs["long_name"] = long_name
            else:
                for k in range(raw.shape[1]):
                    d = f.create_dataset(f"{name}{k + 1}", data=raw[:, k])
                    d.attrs["scale_factor"] = np.array([scale]); d.attrs["_FillValue"] = np.array([fill], np.int16); d.attrs["units"] = units; d.attrs["long_name"] = long_name
        var("VHM0_SW", hs, 0.001, "m", "Spectral significant wave height of the swell partition")
        var("VTPK_SW", tp, 0.01, "s", period_name)
        var("VPED_SW", dr, None, "degree", dir_name)                 # degrees do not fit an int16 at 0.01: a float variable


def test_read_swell_sorts_partitions_by_height_keeps_three_and_refuses_a_mean_period_or_a_direction_towards(tmp_path):
    path = str(tmp_path / "sw.nc")
    t = np.array([100.0, 200.0, 300.0, 400.0])
    hs = np.array([[1.0, 2.5, 0.5, 0.2], [np.nan, np.nan, np.nan, np.nan], [3.0, np.nan, 1.0, np.nan], [0.0, 1.0, np.nan, np.nan]])
    tp = np.array([[10, 14, 8, 6], [1, 1, 1, 1], [16, 1, 9, 1], [5, 12, 1, 1]], np.float64)
    dr = np.array([[100, 215.5, 300, 10], [0, 0, 0, 0], [270, 0, 90, 0], [10, 370, 0, 0]], np.float64)
    write_swell(path, t, [10, 20, 30, 40], [190, -10, 0, 20], hs, tp, dr)
    d = S.read_swell(path)
    assert d["t"].tolist() == [100.0, 300.0, 400.0], "a point without a partition is dropped"
    assert d["hs"][0].tolist() == [2.5, 1.0, 0.5] and d["tp"][0].tolist() == [14, 10, 8] and d["dir"][0].tolist() == [215.5, 100, 300], "the dominant first, three kept"
    assert d["hs"][1].tolist()[:2] == [3.0, 1.0] and np.isnan(d["hs"][1][2]) and d["lon"][0] == pytest.approx(-170.0)
    assert d["hs"][2].tolist()[0] == 1.0 and d["dir"][2][0] == pytest.approx(10.0), "a 0 m partition is no partition; 370 wraps to 10"
    d2 = S.read_swell(path, A.EPOCH + timedelta(seconds=250), A.EPOCH + timedelta(seconds=330))
    assert d2["t"].tolist() == [300.0]
    write_swell(path, t, [10, 20, 30, 40], [190, -10, 0, 20], hs, tp, dr, two_d=False)
    assert S.read_swell(path)["hs"][0].tolist() == [2.5, 1.0, 0.5], "per-partition variables read the same"
    write_swell(path, t, [10, 20, 30, 40], [190, -10, 0, 20], hs, tp, dr, period_name="Mean period of the swell partition")
    with pytest.raises(ValueError, match="peak"):
        S.read_swell(path)
    write_swell(path, t, [10, 20, 30, 40], [190, -10, 0, 20], hs, tp, dr, dir_name="Direction the swell partition propagates towards")
    with pytest.raises(ValueError, match="comes from"):
        S.read_swell(path)
    write_swell(path, t, [10, 20, 30, 40], [190, -10, 0, 20], hs, tp, dr, dir_name="Partition direction")
    with pytest.raises(ValueError, match="convention"):
        S.read_swell(path)
    # the review's wordings (P0): a "towards" direction whose text also says "clockwise from north" is refused
    for name in ("Direction towards which the swell partition propagates, clockwise from true north", "Swell partition direction (to), degrees clockwise from north",
                 "Peak direction of the partition, going towards, measured from north", "Direction the partition propagates to"):
        write_swell(path, t, [10, 20, 30, 40], [190, -10, 0, 20], hs, tp, dr, dir_name=name)
        with pytest.raises(ValueError, match="comes from|convention"):
            S.read_swell(path)
    write_swell(path, t, [10, 20, 30, 40], [190, -10, 0, 20], hs, tp, dr, dir_name="Partition direction, clockwise from true north (coming from)")
    assert S.read_swell(path)["dir"][0][0] == pytest.approx(215.5), "the reference phrase is not the convention; 'coming from' is"
    for name in ("Zero-crossing period; see the spectral peak period for comparison", "Mean period at the spectral peak", "Tm02 of the partition"):
        write_swell(path, t, [10, 20, 30, 40], [190, -10, 0, 20], hs, tp, dr, period_name=name)
        with pytest.raises(ValueError, match="peak"):
            S.read_swell(path)
    assert S.convention("sea_surface_wave_from_direction", "Direction towards which the swell goes", S.DIR_FROM_STD, S.DIR_TO_STD, S.DIR_FROM_RE, S.DIR_TO_RE, S.DIR_REF_RE) == "pos", "a CF standard name decides"
    assert S.convention("sea_surface_swell_wave_to_direction", "Direction the swell comes from", S.DIR_FROM_STD, S.DIR_TO_STD, S.DIR_FROM_RE, S.DIR_TO_RE, S.DIR_REF_RE) == "neg"
    assert S.convention("sea_surface_wave_period_at_variance_spectral_density_maximum", "", S.PERIOD_PEAK_STD, S.PERIOD_MEAN_STD, S.PERIOD_PEAK_RE, S.PERIOD_MEAN_RE) == "pos"
    assert S.convention("sea_surface_wave_mean_period", "Peak period", S.PERIOD_PEAK_STD, S.PERIOD_MEAN_STD, S.PERIOD_PEAK_RE, S.PERIOD_MEAN_RE) == "neg"


def test_swell_main_publishes_its_own_prefix_and_chunks_with_the_dominant_partition_first(tmp_path):
    web = FakeWeb()
    base = f"{A.STAC}/{S.STAC_ID}"
    did = "cmems-obs-wave_glo_phy_spc-fwk_nrt_s1a_l3_PT15S-i_202511"
    fw = "cmems-obs-wave_glo_phy_spc-fwk_nrt_s1a_l3_PT3H-i_202511"
    native = f"{A.S3}/{A.BUCKET}/native/{S.STAC_ID}/{did}"
    web.urls[f"{base}/product.stac.json"] = {"links": [{"rel": "item", "href": f"{did}/dataset.stac.json"}, {"rel": "item", "href": f"{fw}/dataset.stac.json"}]}
    web.urls[f"{base}/{did}/dataset.stac.json"] = {"id": did, "assets": {"native": {"href": native}}}
    web.urls[f"{base}/{fw}/dataset.stac.json"] = {"id": fw, "assets": {"native": {"href": native.replace(did, fw)}}}
    day0 = NOW.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    name = f"global_s1a_l3_spc_fwk_nrt_{day0:%Y%m%dT%H%M%S}_{day0 + timedelta(hours=23, minutes=59):%Y%m%dT%H%M%S}_{day0 + timedelta(hours=31):%Y%m%dT%H%M%S}.nc"
    t = ss(day0) + np.arange(0, 24 * 3600, 900, dtype=np.float64)       # a point every 15 min through the day
    n = len(t)
    hs = np.stack([1.0 + 0.5 * np.sin(np.arange(n) / 5), 2.0 + 0 * t, np.where(np.arange(n) % 3 == 0, np.nan, 0.5)], axis=1)
    tp = np.stack([9.0 + 0 * t, 14.0 + 0 * t, 6.0 + 0 * t], axis=1)
    dr = np.stack([100.0 + 0 * t, 215.0 + 0 * t, 300.0 + 0 * t], axis=1)
    path = str(tmp_path / name)
    write_swell(path, t, -30 + 0 * t, 170 + np.linspace(0, 20, n), hs, tp, dr)
    key = f"native/{S.STAC_ID}/{did}/{day0:%Y}/{day0:%m}/{name}"
    web.urls[f"{A.S3}/{A.BUCKET}/{key}"] = path
    for month in A.months_touching(NOW - timedelta(hours=A.KEEP_H), NOW):
        web.urls[f"{A.S3}/{A.BUCKET}/?list-type=2&prefix=native/{S.STAC_ID}/{did}/{month:%Y}/{month:%m}/&max-keys=1000"] = listing_xml([(key, os.path.getsize(path))] if month.month == day0.month else [])
    fc = FakeClient(); store = P.Store(fc, "b")
    assert S.main([], get=web, store=store, now=NOW) == 0
    assert not [u for u in web.log if fw in u and "list-type" in u], "the propagated (fireworks) dataset is never listed"
    assert [k for k in puts(fc, "sarswell/v1/")][-1] == "sarswell/v1/latest.json" and not puts(fc, "alt/")
    ptr = json.loads(fc.objects["sarswell/v1/latest.json"]["body"])
    assert ptr["format"] == "swell-v1" and ptr["magic"] == "SWL1" and ptr["columns"] == F.SWELL_COLUMNS and ptr["model"] is None
    assert [m["id"] for m in ptr["missions"]] == ["s1a"] and ptr["source"]["doi"] == "10.48670/moi-00174"
    wins = [c["window"] for c in ptr["chunks"]]
    assert wins[0] == A.window_key(datetime(2026, 11, 3, 9, tzinfo=timezone.utc)) and wins[-1] == A.window_key(datetime(2026, 11, 3, 21, tzinfo=timezone.utc)) and len(wins) == 5
    c = ptr["chunks"][0]
    h, cols = F.decode_chunk(fc.objects[c["key"]]["body"], magic=F.MAGIC_SWELL)
    v = F.values(h, cols)
    assert h["period"] == "peak" and h["direction"] == "from" and h["parts"] == 3 and c["n"] == 12 and h["segments"] == [[0, 0, 12]], "12 points per 3 h: the boundary point lands in one chunk only"
    assert (v["hs1"] == 2.0).all() and (v["tp1"] == 14.0).all() and (v["dir1"] == 215.0).all(), "the 2 m partition is the dominant one"
    assert (v["n"] >= 2).all() and (v["n"] == 3).sum() == 8
    # the same day again: the heartbeat (the newest point is 15 h old: daily batches are not stale at 12 h); the job's own
    # stale alarm once the newest point is older than 48 h
    assert S.main([], get=web, store=store, now=NOW + timedelta(minutes=10)) == 0 and "sarswell/v1/stale-alarm.json" not in fc.objects
    assert S.main([], get=web, store=store, now=NOW + timedelta(hours=30)) == 0
    assert S.main([], get=web, store=store, now=NOW + timedelta(hours=36)) == 1 and "sarswell/v1/stale-alarm.json" in fc.objects
    assert (S.STALE_H, S.SWELL.stale_h, A.HEIGHTS.stale_h) == (48, 48, 12)


# ------------------------------------------------------------------ the workflows and the pins
def test_the_workflows_are_pinned_dispatched_and_kept_alive():
    wf = os.path.join(ROOT, ".github", "workflows")
    for name, script, group, cron in (("model-alt.yml", "alt.py", "model-alt", '"7,37 * * * *"'), ("model-sarswell.yml", "sarswell.py", "model-sarswell", '"9,39 * * * *"')):
        text = open(os.path.join(wf, name), encoding="utf-8").read()
        assert f"python tools/model_frames/{script}" in text and f"group: {group}" in text and f"cron: {cron}" in text and "timeout-minutes: 30" in text
        uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
        assert uses and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses)
        pkgs = text.split("create-args: >-", 1)[1].split("cache-environment", 1)[0].split()
        assert all(re.fullmatch(r"[a-z0-9-]+=[0-9][0-9A-Za-z.]*", p) for p in pkgs) and any(p.startswith("h5py=") for p in pkgs) and any(p.startswith("pillow=") for p in pkgs)
        assert "eccodes" not in text
        assert "dry_run" in text and "force" in text and "window" in text
    keep = open(os.path.join(wf, "model-frames-keepalive.yml"), encoding="utf-8").read()
    assert "gh workflow enable model-alt.yml" in keep and "gh workflow enable model-sarswell.yml" in keep
    tests = open(os.path.join(wf, "model-frames-tests.yml"), encoding="utf-8").read()
    assert tests.count('".github/workflows/model-alt.yml"') == 2 and tests.count('".github/workflows/model-sarswell.yml"') == 2 and "pillow==" in tests
    cfg = open(os.path.join(ROOT, "tools", "model_frames", "trigger", "wrangler.jsonc"), encoding="utf-8").read()
    gw = re.search(r'"GH_WORKFLOW":\s*"([^"]+)"', cfg).group(1).split(",")
    assert "model-alt.yml" in gw and "model-sarswell.yml" in gw, "the 10-minute trigger dispatches both (files land every 30 min)"
    assert (A.KEEP_H, A.STALE_H, A.STALE_ALARM_EVERY_H, A.FAILED_MAX_ATTEMPTS, A.MAX_CHUNKS, A.WINDOW_S) == (30, 12, 12, 3, 12, 10800)
    assert A.RUN_LOOKBACK == (0, 6, 12, 18) and F.MAX_CHUNK_BYTES == 2 * 1024 * 1024 and F.SWELL_PARTS == 3
