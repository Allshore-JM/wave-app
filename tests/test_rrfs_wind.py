"""rrfs_wind (plan section 99): the site's reader of the RRFS job's objects. The objects are made by the JOB's own
build() + publish() (fields faked, no eccodes, no network), so the job <-> site format contract is pinned here; the
reader then reads them back from a fake bucket: the newest-source-per-hour merge, the stations and the ranged cell
reads, the checks, the failure memory and the GFS merge."""
import io
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, ROOT)
import publish as P  # noqa: E402
import rrfs as J  # noqa: E402
import rrfs_wind as W  # noqa: E402

HONOLULU = (21.30, -157.87)                          # a sea cell off Honolulu, inside the Hawaii grid
COORDS = {"51201": (21.671, -158.118)}


class FakeClient:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        self.objects[Key] = bytes(Body)

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None, **kw):
        return {"Contents": [{"Key": k} for k in sorted(self.objects) if k.startswith(Prefix)], "IsTruncated": False}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)
        return {}


def _fake_fields(monkeypatch):
    """Every field of (cycle, step): speed = cycle hour + step / 100 m/s (u), so a value names its source and step;
    gust = speed + 2; the hour's precipitation 0.5 mm."""
    g = J.G.GRIDS["hi"]
    shape = (g["Ny"], g["Nx"])
    monkeypatch.setattr(J, "grid_lonlat", lambda msg, grid: (np.zeros(shape), np.zeros(shape)))
    monkeypatch.setattr(J, "fetch_step", lambda c, grid, step, get=None: {n: (n, c, step) for n, _ in J.wanted(c, step)})

    def decode(token, grid, expect=None):
        name, c, step = token
        s = c.hour + step / 100
        val = {"u": 0.0, "v": -s, "gust": s + 2, "tmp": 300.0, "dpt": 290.0, "tcdc": 40.0,
               "apcp": 0.5, "apcp_total": 0.5 * step}.get(name, 0.0)
        return np.full(shape, val)

    monkeypatch.setattr(J, "decode", decode)


@pytest.fixture
def bucket(monkeypatch, tmp_path):
    """Publishes the 12Z run (steps 0..30) and the hourly cycles 13Z and 14Z (steps 1..18) of 2026-11-17, Hawaii only."""
    _fake_fields(monkeypatch)
    monkeypatch.setattr(J, "RUN_STEPS", list(range(0, 31)))
    store = P.Store(FakeClient(), "b")
    latest = {}
    for h in (12, 13, 14):
        c = datetime(2026, 11, 17, h, tzinfo=timezone.utc)
        work = tmp_path / f"w{h}"
        work.mkdir()
        files, docs = J.build(c, str(work), COORDS, 1e18, log=lambda s: None, grids=("hi",))
        latest = J.publish(store, c, files, docs, latest)
    return store.client.objects


def reader(objects, calls=None, root="https://models.example"):
    def get_bytes(url, cap):
        key = url.split("models.example/", 1)[1]
        if calls is not None:
            calls.append(("get", key))
        body = objects[key]
        assert len(body) <= cap
        return body

    def get_range(url, a, b):
        key = url.split("models.example/", 1)[1]
        if calls is not None:
            calls.append(("range", key, a, b))
        return objects[key][a:b + 1]

    return W.RrfsWinds(lambda: root, get_bytes, get_range)


def test_station_newest_source_wins_per_hour(bucket):
    w = reader(bucket).station("51201")
    t = lambda h: datetime(2026, 11, 17, 0) + timedelta(hours=h)            # noqa: E731
    assert w[t(12)][3] == "RRFS 12Z" and w[t(12)][0] == pytest.approx(12.00)  # only the run has 12Z itself
    assert w[t(14)][3] == "RRFS 13Z" and w[t(14)][0] == pytest.approx(13.01)  # 13Z +1 (14Z cycle starts at +1 = 15Z)
    assert w[t(15)][3] == "RRFS 14Z" and w[t(15)][0] == pytest.approx(14.01)
    assert w[t(32)][3] == "RRFS 14Z"                                          # 14Z +18
    assert w[t(33)][3] == "RRFS 12Z" and w[t(33)][0] == pytest.approx(12.21)  # beyond the cycles: the run
    assert max(w) == t(42)                                                    # the run's last step (+30 here)
    spd, d, gust, _ = w[t(20)]
    assert d == 0 and gust == pytest.approx(spd + 2)                          # from the north (v < 0)
    assert reader(bucket).station("NOPE") == {}


def test_point_reads_one_range_per_source_and_matches_the_station_rule(bucket):
    calls = []
    r = reader(bucket, calls)
    p = r.point(*HONOLULU)
    st = r.station("51201")
    assert {t: v[3] for t, v in p.items()} == {t: v[3] for t, v in st.items()}
    assert all(abs(p[t][0] - st[t][0]) < 1e-9 for t in p)
    ranges = [c for c in calls if c[0] == "range"]
    assert len(ranges) == 3 and all(c[3] - c[2] + 1 in (14 * 31, 14 * 18) for c in ranges)
    grid, j, i = J.G.locate(*HONOLULU)
    assert ranges[0][2] % (14 * 18) == 0 and ranges[0][2] // (14 * 18) == j * 321 + i
    calls.clear()
    r.point(*HONOLULU)
    assert not [c for c in calls if c[0] == "range"]                         # cached


def test_point_cells_carry_every_field(bucket):
    cells = reader(bucket).point_cells(*HONOLULU)
    v = cells[datetime(2026, 11, 17, 20)]
    assert v["tmp_k"] == pytest.approx(300.0) and v["dpt_k"] == pytest.approx(290.0)
    assert v["apcp_mm"] == pytest.approx(0.5) and v["type"] == 1 and v["kind"] == "cycle"
    assert v["cloud"] is None                                                 # hourly cycles have no cloud
    assert cells[datetime(2026, 11, 17, 12)]["cloud"] == 40                   # the run's


def test_outside_every_grid_and_no_root():
    r = W.RrfsWinds(lambda: "", None, None)
    assert r.point(28.18, -15.82) == {} and r.station("51201") == {} and r.newest() is None


def test_uses_only_the_newest_three_cycles_and_two_runs():
    doc = {"format": "rrfs-pointer-v1", "sources": [
        {"cycle": f"20261117{h:02d}", "kind": "run" if h % 6 == 0 else "cycle", "stations": "x",
         "cells": {"hi": "y"} if h >= 10 else None} for h in range(0, 24)]}
    r = W.RrfsWinds(lambda: "https://m", lambda url, cap: __import__("json").dumps(doc).encode(), None)
    assert [s["cycle"][-2:] for s in r.sources()] == ["23", "22", "21", "18", "12"]
    assert r.newest() == "2026111723"


def test_bad_pointer_and_bad_objects_fall_back_and_are_remembered(bucket):
    objs = dict(bucket)
    objs["rrfs/v1/latest.json"] = b'{"format": "something-else"}'
    r = reader(objs)
    assert r.station("51201") == {} and r.health()["pointer_fail"] == 1
    objs = dict(bucket)
    run_bin = "rrfs/v1/2026111712/stations.bin"
    objs[run_bin] = objs[run_bin][:-2] + b"\x00\x00"                          # sha256 no longer matches
    calls = []
    r = reader(objs, calls)
    w = r.station("51201")
    assert w and all(v[3] != "RRFS 12Z" for v in w.values())                 # the damaged source is left out
    assert "stations.bin of 2026111712" in r.health()["last_error"]
    n = len(calls)
    r.station("51201")
    assert not [c for c in calls[n:] if c[1] == run_bin]                     # remembered: not asked again at once


def test_cells_index_of_another_grid_size_is_refused(bucket):
    import json
    objs = dict(bucket)
    key = "rrfs/v1/2026111714/hi.json"
    doc = json.loads(objs[key])
    doc["nx"] = 320
    objs[key] = json.dumps(doc).encode()
    p = reader(objs).point(*HONOLULU)
    assert p and all(v[3] != "RRFS 14Z" for v in p.values())


def test_merge_with_gfs():
    t = datetime(2026, 11, 17, 18)
    m = W.merge({t: (5.0, 90), t + timedelta(hours=1): (6.0, 100)}, {t: (7.0, 80, 9.0, "RRFS 13Z")})
    assert m == {t: (7.0, 80, 9.0, "RRFS 13Z"), t + timedelta(hours=1): (6.0, 100, None, None)}
