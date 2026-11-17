"""rrfs_wind (plan section 99): the site's reader of the RRFS job's objects. The objects are made by the JOB's own
build() + publish() (fields faked, no eccodes, no network), so the job <-> site format contract is pinned here; the
reader then reads them back from a fake bucket: the newest-source-per-hour merge, the stations and the ranged cell
reads, the checks, the failure memory and the GFS merge."""
import io
import json
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

WALL = datetime(2026, 11, 17, 16)                # the readers' "now": the sources are fresh (G66 P2-2)
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

    return W.RrfsWinds(lambda: root, get_bytes, get_range, wall=lambda: WALL)


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
    assert v["cloud"] == 40                                                   # an hourly cycle has none: the run's fills in
    assert cells[datetime(2026, 11, 17, 12)]["cloud"] == 40                   # the run's


def test_outside_every_grid_and_no_root():
    r = W.RrfsWinds(lambda: "", None, None)
    assert r.point(28.18, -15.82) == {} and r.station("51201") == {} and r.newest() is None


def test_uses_only_the_newest_three_cycles_and_two_runs():
    doc = {"format": "rrfs-pointer-v1", "sources": [
        {"cycle": f"20261117{h:02d}", "kind": "run" if h % 6 == 0 else "cycle", "stations": "x",
         "cells": {"hi": "y"} if h >= 10 else None} for h in range(0, 24)]}
    r = W.RrfsWinds(lambda: "https://m", lambda url, cap: __import__("json").dumps(doc).encode(), None,
                    wall=lambda: datetime(2026, 11, 17, 23, 30))
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


# ------------------------------------------------------------------------------------------- G66 review fixes

def test_stale_sources_are_not_used():
    """A stalled job: cycles over 8 h and runs over 18 h past their cycle time are left out (G66 P2-2)."""
    doc = {"format": "rrfs-pointer-v1", "sources": [
        {"cycle": "2026111714", "kind": "cycle", "stations": "x"}, {"cycle": "2026111712", "kind": "run", "stations": "x"}]}

    def at(h):
        return W.RrfsWinds(lambda: "https://m", lambda url, cap: json.dumps(doc).encode(), None,
                           wall=lambda: datetime(2026, 11, 17) + timedelta(hours=h))

    assert [s["cycle"] for s in at(20).sources()] == ["2026111714", "2026111712"]
    assert [s["cycle"] for s in at(22.5).sources()] == ["2026111712"]              # the cycle is 8.5 h old
    assert at(30.5).sources() == [] and at(30.5).station("51201") == {}             # the run 18.5 h old too


def test_pointer_is_read_once_kept_through_a_failure_then_dropped():
    calls, clock = [], {"t": 0.0}
    state = {"fail": False}
    doc = {"format": "rrfs-pointer-v1", "published_utc": "P1",
           "sources": [{"cycle": "2026111714", "kind": "cycle", "stations": "x"}]}

    def get(url, cap):
        calls.append(url)
        if state["fail"]:
            raise IOError("bucket blip")
        return json.dumps(doc).encode()

    r = W.RrfsWinds(lambda: "https://m", get, None, clock=lambda: clock["t"], wall=lambda: datetime(2026, 11, 17, 16))
    assert r.pointer()["published_utc"] == "P1" and len(calls) == 1
    r.pointer()
    assert len(calls) == 1                                                  # cached POINTER_TTL_S
    state["fail"] = True
    clock["t"] = W.POINTER_TTL_S + 1
    assert r.pointer()["published_utc"] == "P1"                             # the blip keeps the last good pointer
    assert r.version().startswith("P1|")                                    # so the tables' keys do not change
    n = len(calls)
    r.pointer()
    assert len(calls) == n                                                  # retried after POINTER_FAIL_TTL_S only
    clock["t"] = W.POINTER_KEEP_S + 10
    assert r.pointer() is None and r.version() == ""                        # an hour on: given up


def test_pointer_refresh_is_single_flight():
    """While one request refreshes the pointer the others keep the last good one (G66 P2-4)."""
    import threading
    clock = {"t": 0.0}
    gate, entered = threading.Event(), threading.Event()
    doc = {"format": "rrfs-pointer-v1", "published_utc": "P1", "sources": []}
    n = {"calls": 0}

    def get(url, cap):
        n["calls"] += 1
        if n["calls"] > 1:
            entered.set()
            gate.wait(5)
        return json.dumps(doc).encode()

    r = W.RrfsWinds(lambda: "https://m", get, None, clock=lambda: clock["t"])
    r.pointer()
    clock["t"] = W.POINTER_TTL_S + 1
    th = threading.Thread(target=r.pointer)
    th.start()
    assert entered.wait(5)
    assert r.pointer()["published_utc"] == "P1" and n["calls"] == 2         # served at once, no second fetch
    gate.set()
    th.join(5)


def test_version_changes_when_a_late_run_lands():
    """A run published after a newer hourly cycle changes the key (G66 P2-5): newest() alone would not."""
    docs = [{"format": "rrfs-pointer-v1", "published_utc": "T1", "sources": [
        {"cycle": "2026111714", "kind": "cycle", "stations": "x"}, {"cycle": "2026111706", "kind": "run", "stations": "x"}]},
        {"format": "rrfs-pointer-v1", "published_utc": "T2", "sources": [
            {"cycle": "2026111714", "kind": "cycle", "stations": "x"}, {"cycle": "2026111712", "kind": "run", "stations": "x"},
            {"cycle": "2026111706", "kind": "run", "stations": "x"}]}]
    clock = {"t": 0.0, "i": 0}
    r = W.RrfsWinds(lambda: "https://m", lambda url, cap: json.dumps(docs[clock["i"]]).encode(), None,
                    clock=lambda: clock["t"], wall=lambda: datetime(2026, 11, 17, 16))
    v1, n1 = r.version(), r.newest()
    clock.update(t=W.POINTER_TTL_S + 1, i=1)
    assert r.newest() == n1 and r.version() != v1


def test_degraded_when_a_source_cannot_be_read(bucket):
    objs = dict(bucket)
    objs["rrfs/v1/2026111714/stations.bin"] = b"junk"
    r = reader(objs)
    w = r.station("51201")
    assert w and w.degraded and all(v[3] != "RRFS 14Z" for v in w.values())
    assert r.station("51201").degraded                                       # remembered: still left out, still marked
    assert not reader(bucket).station("51201").degraded

    def bad_range(url, a, b):
        if "/2026111713/" in url:
            raise IOError("timeout")
        return bucket[url.split("models.example/", 1)[1]][a:b + 1]

    r2 = W.RrfsWinds(lambda: "https://models.example", lambda url, cap: bucket[url.split("models.example/", 1)[1]],
                     bad_range, wall=lambda: WALL)
    p = r2.point(*HONOLULU)
    assert p and p.degraded and all(v[3] != "RRFS 13Z" for v in p.values())
    assert not reader(bucket).point(*HONOLULU).degraded


def test_a_bad_station_slot_drops_only_that_source(bucket):
    objs = dict(bucket)
    key = "rrfs/v1/2026111714/stations.json"
    doc = json.loads(objs[key])
    doc["stations"]["51201"][0] = 999                                        # past the end of stations.bin
    objs[key] = json.dumps(doc).encode()
    w = reader(objs).station("51201")
    assert w.degraded and {v[3] for v in w.values()} == {"RRFS 13Z", "RRFS 12Z"}  # the others still read (G66 P3-3)


def test_the_runs_f000_takes_the_older_sources_precipitation(tmp_path, bucket):
    """The newest run's f000 has no precipitation; the older run's same hour fills it (and its type), so the 24-h
    totals are not blank for a day (G66 P2-1)."""
    c = datetime(2026, 11, 17, 6, tzinfo=timezone.utc)
    work = tmp_path / "w06"
    work.mkdir()
    store = P.Store(FakeClient(), "b")
    store.client.objects.update(bucket)
    latest = json.loads(bucket["rrfs/v1/latest.json"])
    files, docs = J.build(c, str(work), COORDS, 1e18, log=lambda s: None, grids=("hi",))
    J.publish(store, c, files, docs, latest)
    cells = reader(store.client.objects).point_cells(*HONOLULU)
    v = cells[datetime(2026, 11, 17, 12)]
    assert v["label"] == "RRFS 12Z" and v["apcp_mm"] == pytest.approx(0.5) and v["type"] == 1
    assert v["spd"] == pytest.approx(12.0)                                   # the newest source's own fields stay


def test_held_cells_need_no_thread_pool(bucket, monkeypatch):
    r = reader(bucket)
    r.point(*HONOLULU)

    def boom(*a, **k):
        raise AssertionError("a pool for cells already held")

    monkeypatch.setattr(W, "ThreadPoolExecutor", boom)
    assert r.point(*HONOLULU)                                                # G66 P3-5
