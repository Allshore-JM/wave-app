"""Overlay frame job: pure-function tests (no network, no eccodes, no R2)."""
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))

import encode as E   # noqa: E402
import fetch as F    # noqa: E402
import publish as P  # noqa: E402
import run as R      # noqa: E402


# ------------------------------- encode / decode --------------------------------

def test_quantize_roundtrip_bounds_and_missing():
    rng = np.random.default_rng(1)
    for name, (lo, hi, _u, _i) in E.FIELDS.items():
        g = rng.uniform(lo, hi, size=(50, 80)).astype(np.float32)
        g[3, 4] = np.nan
        g[5, 6] = hi + 100                      # clamps
        q = E.quantize(g, lo, hi)
        assert q.dtype == np.uint8 and q[3, 4] == 0 and q[5, 6] == 255 and q[q != 0].min() >= 1
        back = E.dequantize(q, lo, hi)
        inr = ~np.isnan(g) & (g <= hi)
        assert np.nanmax(np.abs(back[inr] - g[inr])) <= E.quantum(lo, hi) / 2 + 1e-9
        assert np.isnan(back[3, 4]) and abs(back[5, 6] - hi) < 1e-9


def test_encode_frame_png_and_stats():
    g = np.full((721, 1440), np.nan, np.float32)
    g[:, 700:] = 3.0
    out = E.encode_frame(g, "hs")
    assert out["full"][:8] == b"\x89PNG\r\n\x1a\n" and out["half"][:8] == b"\x89PNG\r\n\x1a\n"
    assert out["stats"]["valid_points"] == 721 * 740 and out["stats"]["clamped_points"] == 0
    assert out["stats"]["min"] == 3.0 and out["stats"]["max"] == 3.0


# ------------------------------- fetch helpers ----------------------------------

def test_steps_and_urls():
    assert F.STEPS[0] == 0 and F.STEPS[-1] == 240 and len(F.STEPS) == 81
    run = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)
    assert F.wave_url(run, 3).endswith("gfs.20260922/12/wave/gridded/gfswave.t12z.global.0p25.f003.grib2")
    assert F.atmos_url(run, 240).endswith("gfs.20260922/12/atmos/gfs.t12z.pgrb2.0p25.f240")


def test_parse_idx_and_record_ranges(monkeypatch):
    idx = "1:0:d=2026092212:WIND:surface:24 hour fcst:\n2:696006:d=2026092212:HTSGW:surface:24 hour fcst:\n3:1131360:d=2026092212:PERPW:surface:24 hour fcst:\n"
    assert F.parse_idx(idx) == [(0, "WIND:surface"), (696006, "HTSGW:surface"), (1131360, "PERPW:surface")]
    calls = []

    def fake_get(url, rng=None, **kw):
        calls.append((url, rng))
        return idx.encode() if url.endswith(".idx") else b"GRIB" + (rng or "").encode()
    monkeypatch.setattr(F, "_get", fake_get)
    out = F.fetch_records("http://x/f024", ["HTSGW:surface", "PERPW:surface"])
    assert calls[1][1] == "bytes=696006-1131359" and calls[2][1] == "bytes=1131360-"
    assert set(out) == {"HTSGW:surface", "PERPW:surface"}
    with pytest.raises(RuntimeError):
        F.fetch_records("http://x/f024", ["NOPE:surface"])


def test_latest_complete_run_requires_both_products(monkeypatch):
    present = {"gfs.20260922/12/wave", "gfs.20260922/12/atmos", "gfs.20260922/18/wave"}   # 18Z atmos missing

    def fake_exists(url):
        return any(p in url for p in present) and url.endswith("f240.grib2.idx") or url.endswith("f240.idx") and any(p in url for p in present)
    monkeypatch.setattr(F, "exists", fake_exists)
    now = datetime(2026, 9, 23, 1, 30, tzinfo=timezone.utc)
    assert F.latest_complete_run(now) == datetime(2026, 9, 22, 12, tzinfo=timezone.utc)


# ------------------------------- publish ordering + prune -----------------------

class FakeClient:
    def __init__(self):
        self.objects = {}
        self.log = []

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        self.objects[Key] = {"body": Body, "ct": ContentType, "cc": CacheControl}
        self.log.append(("put", Key))

    def get_object(self, Bucket, Key):
        import io
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key]["body"])}

    def list_objects_v2(self, Bucket, Prefix, Delimiter=None, ContinuationToken=None):
        keys = [k for k in self.objects if k.startswith(Prefix)]
        if Delimiter:
            cps = sorted({Prefix + k[len(Prefix):].split(Delimiter)[0] + Delimiter for k in keys if Delimiter in k[len(Prefix):]})
            return {"CommonPrefixes": [{"Prefix": c} for c in cps]}
        return {"Contents": [{"Key": k} for k in keys]}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)
            self.log.append(("del", o["Key"]))


def _fake_run(store, run_dt, steps):
    """Drive build_and_publish with fake fetch/decode so it runs offline."""
    import run as RR
    rng = np.random.default_rng(0)

    def fake_records(url, keys):
        return {k: b"x" for k in keys}

    def fake_decode(blob):
        return rng.uniform(0, 10, size=(721, 1440)).astype(np.float32), {}
    RR.F.fetch_records = fake_records
    RR.D.decode = fake_decode
    return RR.build_and_publish(store, run_dt, steps, log=lambda *a: None)


def test_publish_order_frames_then_manifest_then_pointer():
    c = FakeClient()
    store = P.Store(c, "b")
    run_dt = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)
    m = _fake_run(store, run_dt, [0, 3])
    puts = [k for op, k in c.log if op == "put"]
    assert puts[-1] == P.LATEST_KEY and puts[-2] == P.manifest_key("2026092212")
    assert all(k.endswith(".png") for k in puts[:-2]) and len(puts) == 2 * 3 * 2 + 2
    assert c.objects[P.LATEST_KEY]["cc"] == P.POINTER and c.objects[puts[0]]["cc"] == P.IMMUTABLE
    assert m["complete"] is False                           # subset of steps
    latest = json.loads(c.objects[P.LATEST_KEY]["body"])
    assert latest["run"] == "2026092212" and latest["manifest"] == P.manifest_key("2026092212")
    man = json.loads(c.objects[latest["manifest"]]["body"])
    assert man["frames"][1]["valid_utc"] == "2026-09-22T15:00:00Z" and man["encoding"] == E.ENCODING
    assert man["fields"]["tp"]["interpolation"] == "nearest"


def test_crash_before_manifest_leaves_pointer_untouched():
    c = FakeClient()
    store = P.Store(c, "b")
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092206"}', "ct": "application/json", "cc": P.POINTER}
    run_dt = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)

    class Boom(Exception):
        pass
    orig = P.publish_frame
    calls = []

    def flaky(store_, run, field, step, enc):
        calls.append(1)
        if len(calls) == 4:
            raise Boom()
        orig(store_, run, field, step, enc)
    P.publish_frame = flaky
    try:
        with pytest.raises(Boom):
            _fake_run(store, run_dt, [0, 3])
    finally:
        P.publish_frame = orig
    assert json.loads(c.objects[P.LATEST_KEY]["body"])["run"] == "2026092206"
    assert P.manifest_key("2026092212") not in c.objects


def test_prune_keeps_newest_four_and_never_latest():
    c = FakeClient()
    store = P.Store(c, "b")
    for run in ("2026092000", "2026092006", "2026092012", "2026092018", "2026092100", "2026092106"):
        c.objects[f"{P.PREFIX}/{run}/hs/f000.png"] = {"body": b"", "ct": "", "cc": ""}
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092000"}', "ct": "", "cc": ""}   # pathological: latest is oldest
    removed = P.prune(store, keep=4)
    assert removed == ["2026092000", "2026092006"]
    assert f"{P.PREFIX}/2026092000/hs/f000.png" in c.objects        # protected by latest.json
    assert f"{P.PREFIX}/2026092006/hs/f000.png" not in c.objects
    assert store.list_runs() == ["2026092000", "2026092012", "2026092018", "2026092100", "2026092106"]


def test_main_skips_when_already_current(monkeypatch, capsys):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: datetime(2026, 9, 22, 12, tzinfo=timezone.utc))
    c = FakeClient()
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026092212"}', "ct": "", "cc": ""}
    monkeypatch.setattr(R.P, "r2_client", lambda *a: c)
    for k, v in {"R2_ACCOUNT_ID": "a", "R2_ACCESS_KEY_ID": "k", "R2_SECRET_ACCESS_KEY": "s", "R2_BUCKET": "b"}.items():
        monkeypatch.setenv(k, v)
    assert R.main([]) == 0 and "already published" in capsys.readouterr().out


def test_main_exit_3_without_complete_run(monkeypatch):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: None)
    assert R.main(["--dry-run"]) == 3
