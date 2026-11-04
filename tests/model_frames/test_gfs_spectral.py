"""The GFS spectral job (plan section 77): the station record, the bounded pool, the build, the publish order, the
records, prune, the stale alarm, the pins."""
import gzip
import hashlib
import io
import json
import multiprocessing.pool
import os
import re
import sys
import tarfile
import threading
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import gfs_spectral as G  # noqa: E402
import gfs_winds as W  # noqa: E402
import partition2d as P2  # noqa: E402
import publish as P  # noqa: E402
import ww3spec  # noqa: E402

RUN_DT = datetime(2026, 11, 1, 12, tzinfo=timezone.utc)
RUN = "2026110112"
FIX = os.path.join(ROOT, "tests", "fixtures", "gfswinds")
EXPECT = json.load(open(os.path.join(FIX, "gfsspec_expect.json")))
NF, ND = 8, 12
FREQS = 0.05 * 1.25 ** np.arange(NF)
DIRS_TO = (15.0 - 30.0 * np.arange(ND)) % 360.0           # the file's sense (TO), half a step off, descending


def spectrum(h):
    """A swell whose peak walks round the circle with the hour, plus a short sea in the afternoon hours."""
    f = FREQS[:, None]
    d = ((DIRS_TO + 180.0) % 360.0)[None, :]
    dd = ((d - (300.0 + h) % 360.0 + 180.0) % 360.0) - 180.0
    E = 2.0 * np.exp(-0.5 * (np.log(f / 0.08) / 0.2) ** 2) * np.exp(-0.5 * (dd / 25.0) ** 2)
    if h % 3 == 0:
        dd2 = ((d - 60.0 + 180.0) % 360.0) - 180.0
        E += 0.8 * np.exp(-0.5 * (np.log(f / 0.25) / 0.2) ** 2) * np.exp(-0.5 * (dd2 / 20.0) ** 2)
    return E


def spec_text(sid, run_dt=RUN_DT, steps=range(385), wind=lambda h: (6.0, 60.0), body=None):
    lines = [f"'WAVEWATCH III SPECTRA'     {NF:2d}    {ND:2d}     1 'spectral resolution for points'",
             " ".join(f"{v:.3E}" for v in FREQS), " ".join(f"{v:.3E}" for v in np.radians(DIRS_TO))]
    base = run_dt.replace(tzinfo=None)
    for h in steps:
        u, d = wind(h)
        lines.append((base + timedelta(hours=h)).strftime("%Y%m%d %H%M%S"))
        lines.append(f"'{sid:<10s}'  21.67-158.12     472.7  {u:5.2f} {d:5.1f}   0.05 139.8")
        E = spectrum(h) if body is None else body(h)
        vals = (E.T.ravel() * 180.0 / np.pi)          # per radian, frequency fastest
        lines += [" ".join(f"{v:.3E}" for v in vals[i:i + 7]) for i in range(0, vals.size, 7)]
    return "\n".join(lines) + "\n"


def archive(members):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
        for name, text in members.items():
            body = text.encode() if isinstance(text, str) else text
            ti = tarfile.TarInfo(name if name.startswith("./") else f"./gfswave.{name}.spec")
            ti.size = len(body)
            ti.pax_headers = {"atime": "1792840182", "ctime": "1792840751"}
            tf.addfile(ti, io.BytesIO(body))
    return gzip.compress(buf.getvalue())


LISTED = {"51201", "41113", "46026"}


def three():
    return archive({"51201": spec_text("51201"), "41113": spec_text("41113", steps=[h for h in range(385) if h != 7]),
                    "UNLISTED": spec_text("UNLISTED", steps=range(3)), "46026": spec_text("46026", steps=range(10))})


def threads(n):
    return multiprocessing.pool.ThreadPool(n)


@pytest.fixture(autouse=True)
def _small_world(monkeypatch):
    monkeypatch.setattr(W, "MIN_MEMBERS", 2)
    monkeypatch.setattr(W, "MIN_ARCHIVE_BYTES", 100)
    monkeypatch.setattr(W.time, "sleep", lambda s: None)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)


class FakeClient:
    def __init__(self):
        self.objects, self.log = {}, []

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
        page, nxt = keys[start:start + 2], start + 2
        return {"Contents": [{"Key": k} for k in page], "IsTruncated": nxt < len(keys),
                "NextContinuationToken": str(nxt) if nxt < len(keys) else None}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)
            self.log.append(("del", o["Key"]))
        return {}


class _Resp(io.BytesIO):
    def __init__(self, body=b"", length=None):
        super().__init__(body)
        self.headers = {"Content-Length": str(len(body) if length is None else length)}
        self.status = 200


class FakeNoaa:
    def __init__(self, archives=None, status=None):
        self.archives, self.status, self.calls = dict(archives or {}), status, []

    def __call__(self, req, timeout=None):
        url, method = req.full_url, req.get_method()
        self.calls.append((method, url))
        if self.status:
            raise W.urllib.error.HTTPError(url, self.status, "x", {}, None)
        m = re.search(r"gfs\.(\d{8})/(\d{2})/wave/station/gfswave\.t\d{2}z\.spec_tar\.gz$", url)
        data = self.archives.get(m.group(1) + m.group(2)) if m else None
        if data is None:
            raise W.urllib.error.HTTPError(url, 404, "not found", {}, None)
        return _Resp(b"" if method == "HEAD" else data, length=len(data))


def store():
    return P.Store(FakeClient(), "bucket")


def _main(fake, st, *args, hours=1, listed=LISTED):
    return G.main(list(args), opener=fake, store=st, now=RUN_DT + timedelta(hours=hours), listed=listed,
                  pool_factory=threads)


# ---------------- the station record ----------------

def test_a_real_member_is_split_as_the_pinned_rows():
    for name, exp in EXPECT.items():
        data = open(os.path.join(FIX, name), "rb").read()
        run_dt = datetime.strptime(exp["run"], "%Y%m%d%H").replace(tzinfo=timezone.utc)
        sid, rec, present, err, _ = G.station_record(exp["station"], data, run_dt)
        assert err is None and present == len(exp["steps"]) and len(rec) == G.RECORD_BYTES
        decoded = G.decode_record(rec)
        for e in exp["steps"]:
            h = int((datetime.strptime(e["time"], "%Y%m%d%H") - run_dt.replace(tzinfo=None)).total_seconds() // 3600)
            hs_total, parts = decoded[h]
            assert hs_total == pytest.approx(e["hs"], abs=0.006)
            want = sorted((p for p in e["parts"] if p[0] >= G.MIN_PART_HS), key=lambda p: -p[0] * p[0] * p[1])[:6]
            assert len(parts) == len(want)
            for g, x in zip(sorted(parts), sorted((round(p[0], 2), round(p[1], 1), round(p[2]) % 360, p[3]) for p in want)):
                assert g[0] == pytest.approx(x[0], abs=0.011) and g[1] == pytest.approx(x[1], abs=0.11)
                assert min(abs(g[2] - x[2]), 360 - abs(g[2] - x[2])) <= 1 and g[3] == x[3]
        hours = {int((datetime.strptime(e["time"], "%Y%m%d%H") - run_dt.replace(tzinfo=None)).total_seconds() // 3600)
                 for e in exp["steps"]}                       # a member may hold chosen hours (51201's K1 hours)
        assert all(decoded[h] is None for h in range(G.STEPS) if h not in hours)


def test_the_record_keeps_the_six_most_powerful_parts_and_the_flags():
    parts = [{"hs": 0.5 + 0.1 * k, "tp": 20.0 - 2 * k, "dir": 10 * k, "ws": k == 7, "label": k} for k in range(8)]
    parts.append({"hs": 0.04, "tp": 3.0, "dir": 359.6, "ws": False, "label": 9})       # under MIN_PART_HS
    rows = {RUN_DT.replace(tzinfo=None): (2.5, parts), RUN_DT.replace(tzinfo=None) + timedelta(hours=384): (0.0, [])}
    rec, present = G.encode_steps(rows, RUN_DT)
    assert present == 2
    d = G.decode_record(rec)
    hs_total, kept = d[0]
    assert hs_total == 2.5 and len(kept) == 6
    pw = [h * h * t for h, t, _, _ in kept]
    assert pw == sorted(pw, reverse=True)
    assert (0.5 + 0.7, 6.0, 70, True) in kept and all(h >= 0.5 + 0.2 for h, _, _, _ in kept)
    assert d[384] == (0.0, []) and d[100] is None
    # the direction keeps bits 0-11, the flag bit 12: 359 | wind sea round-trips
    rec2, _ = G.encode_steps({RUN_DT.replace(tzinfo=None): (1.0, [dict(parts[0], dir=359.4, ws=True)])}, RUN_DT)
    assert G.decode_record(rec2)[0][1] == [(0.5, 20.0, 359, True)]


def test_a_step_outside_the_run_or_off_the_hour_is_left_out():
    base = RUN_DT.replace(tzinfo=None)
    rows = {base + timedelta(hours=385): (1.0, []), base + timedelta(minutes=30): (1.0, []), base: (1.0, [])}
    rec, present = G.encode_steps(rows, RUN_DT)
    assert present == 1


def test_an_unreadable_member_is_reported_not_raised():
    sid, rec, present, err, first = G.station_record("X", b"junk", RUN_DT)
    assert rec is None and present == 0 and "ValueError" in err and first is None
    sid, rec, present, err, first = G.station_record("51201", spec_text("51201", steps=range(3)).encode(), RUN_DT)
    assert err is None and present == 3 and first == RUN_DT.replace(tzinfo=None)


def test_a_part_at_the_minimum_height_is_kept():
    base = RUN_DT.replace(tzinfo=None)
    parts = [{"hs": G.MIN_PART_HS, "tp": 10.0, "dir": 90.0, "ws": False, "label": 1},
             {"hs": G.MIN_PART_HS - 0.001, "tp": 10.0, "dir": 90.0, "ws": False, "label": 2}]
    rec, _ = G.encode_steps({base: (1.0, parts)}, RUN_DT)
    assert G.decode_record(rec)[0][1] == [(G.MIN_PART_HS, 10.0, 90, False)]


# ---------------- the build ----------------

def test_the_build_splits_the_listed_members_in_archive_order_and_skips_the_rest():
    body, doc = G.build(io.BytesIO(three()), RUN_DT, LISTED, pool_factory=threads, workers=2, log=lambda s: None)
    assert doc["stations"] == {"51201": 0, "41113": 1, "46026": 2} and doc["count"] == 3
    assert len(body) == 3 * G.RECORD_BYTES and doc["members"] == 4 and doc["left_out"] == {}
    assert doc["steps_present"] == 385 + 384 + 10 and doc["workers"] == 2 and doc["rules"] == P2.RULES
    d = G.decode_record(body[G.RECORD_BYTES:2 * G.RECORD_BYTES])
    assert d[7] is None and d[6] is not None
    hs_total, parts = d[6]
    assert len(parts) == 2 and any(p[3] for p in parts) and any(not p[3] for p in parts)   # hour 6: swell + sea
    assert hs_total == pytest.approx(ww3spec.hs_of(FREQS, (DIRS_TO + 180) % 360, spectrum(6)), abs=0.006)
    # another run's archive under this run's name (G48a P2-1): the members do not start at the run's hour. A LATER
    # run's archive (every step past the window) and an EARLIER one (379 of 385 steps inside it) are both refused.
    for off in (timedelta(days=30), timedelta(hours=6), timedelta(hours=-6)):
        with pytest.raises(ValueError, match="do not start at the run"):
            G.build(io.BytesIO(three()), RUN_DT + off, LISTED, pool_factory=threads, workers=2, log=lambda s: None)


def test_a_few_shifted_members_pass_but_a_tenth_fails_the_build():
    listed = {f"S{k:02d}" for k in range(20)}
    members = {s: spec_text(s, steps=range(2)) for s in sorted(listed)}
    members["S00"] = spec_text("S00", run_dt=RUN_DT - timedelta(hours=6), steps=range(8))   # one member of the older run
    body, doc = G.build(io.BytesIO(archive(members)), RUN_DT, listed, pool_factory=threads, workers=2, log=lambda s: None)
    assert doc["count"] == 20 and doc["left_out"] == {}
    for s in ("S01", "S02"):
        members[s] = spec_text(s, run_dt=RUN_DT - timedelta(hours=6), steps=range(8))
    with pytest.raises(ValueError, match="3 of 20 listed members do not start at the run"):
        G.build(io.BytesIO(archive(members)), RUN_DT, listed, pool_factory=threads, workers=2, log=lambda s: None)


def test_the_pool_gives_the_sequential_result():
    a, _ = G.build(io.BytesIO(three()), RUN_DT, LISTED, pool_factory=threads, workers=1, log=lambda s: None)
    b, _ = G.build(io.BytesIO(three()), RUN_DT, LISTED, pool_factory=threads, workers=3, log=lambda s: None)
    assert a == b


@pytest.mark.skipif(sys.platform == "win32", reason="the spawned workers need the module on their path (Linux: fork)")
def test_a_real_process_pool_gives_the_same_bytes():
    a, _ = G.build(io.BytesIO(three()), RUN_DT, LISTED, pool_factory=threads, workers=1, log=lambda s: None)
    b, _ = G.build(io.BytesIO(three()), RUN_DT, LISTED, workers=2, log=lambda s: None)
    assert a == b


def test_the_stream_never_runs_ahead_of_the_pool_by_more_than_the_cap(monkeypatch):
    slow = G.station_record

    def crawl(sid, data, run_dt):
        time.sleep(0.05)
        return slow(sid, data, run_dt)
    monkeypatch.setattr(G, "station_record", crawl)
    listed = {f"S{k:02d}" for k in range(12)}
    arc = archive({s: spec_text(s, steps=range(3)) for s in sorted(listed)})
    peaks = []
    real = G._Bounded.__init__

    def init(self, pool, cap):
        real(self, pool, cap)
        peaks.append(self)
    monkeypatch.setattr(G._Bounded, "__init__", init)
    body, doc = G.build(io.BytesIO(arc), RUN_DT, listed, pool_factory=threads, workers=2, log=lambda s: None)
    assert doc["count"] == 12 and peaks[0].peak <= 4 and peaks[0].peak >= 2


def test_a_bad_member_is_left_out_and_too_many_fail_the_build():
    listed = {f"S{k:02d}" for k in range(25)}
    members = {s: spec_text(s, steps=range(2)) for s in sorted(listed)}
    members["S03"] = "'WAVEWATCH III SPECTRA' 8 12 1 'x'\n junk\n"
    body, doc = G.build(io.BytesIO(archive(members)), RUN_DT, listed, pool_factory=threads, workers=2,
                        log=lambda s: None)
    assert doc["count"] == 24 and list(doc["left_out"]) == ["S03"] and "ValueError" in doc["left_out"]["S03"]
    members["S04"] = members["S03"]
    with pytest.raises(ValueError, match="unreadable"):
        G.build(io.BytesIO(archive(members)), RUN_DT, listed, pool_factory=threads, workers=2, log=lambda s: None)


def test_a_lost_worker_is_left_out_after_its_wait_not_the_whole_budget(monkeypatch):
    """A worker that answers neither callback (a process killed: G48a P3-1) costs TASK_MAX_S, not the budget."""
    orig = G.station_record

    def dies(sid, data, run_dt):
        if sid == "S03":
            raise SystemExit(5)                        # a ThreadPool worker that exits: no callback either way
        return orig(sid, data, run_dt)
    monkeypatch.setattr(G, "station_record", dies)
    monkeypatch.setattr(G, "TASK_MAX_S", 0.5)
    listed = {f"S{k:02d}" for k in range(24)}                     # one lost of 24 is under the 5 % failure share
    arc = archive({s: spec_text(s, steps=range(2)) for s in sorted(listed)})
    t0 = time.monotonic()
    body, doc = G.build(io.BytesIO(arc), RUN_DT, listed, deadline=time.monotonic() + 30, pool_factory=threads,
                        workers=2, log=lambda s: None)
    assert time.monotonic() - t0 < 10 and doc["count"] == 23
    assert list(doc["left_out"]) == ["S03"] and "worker failed: no answer" in doc["left_out"]["S03"]


def test_the_feeder_stops_at_the_deadline_while_every_slot_is_held(monkeypatch):
    """Hung tasks fill the cap; the feeder's wait for a slot honours the deadline (G48a P3-2)."""
    orig = G.station_record
    gate = threading.Event()
    hung = {"S00", "S01", "S02", "S03"}

    def hangs(sid, data, run_dt):
        if sid in hung:
            gate.wait()
        return orig(sid, data, run_dt)
    monkeypatch.setattr(G, "station_record", hangs)
    listed = {f"S{k:02d}" for k in range(16)}
    arc = archive({s: spec_text(s, steps=range(2)) for s in sorted(listed)})
    threading.Timer(4.0, gate.set).start()           # lets terminate() join afterwards
    t0 = time.monotonic()
    with pytest.raises(W.BudgetExceeded):
        G.build(io.BytesIO(arc), RUN_DT, listed, deadline=time.monotonic() + 1.5, pool_factory=threads, workers=2,
                log=lambda s: None)
    assert time.monotonic() - t0 < 3.5
    gate.set()


def test_drain_honours_the_deadline_for_a_task_that_never_answers(monkeypatch):
    """Every member submitted, one task hangs: drain's wait ends at the deadline, not at TASK_MAX_S (G48a B6)."""
    orig = G.station_record
    gate = threading.Event()

    def hangs(sid, data, run_dt):
        if sid == "S01":
            gate.wait()
        return orig(sid, data, run_dt)
    monkeypatch.setattr(G, "station_record", hangs)
    monkeypatch.setattr(G, "TASK_MAX_S", 6.0)
    listed = {"S00", "S01"}
    arc = archive({s: spec_text(s, steps=range(2)) for s in sorted(listed)})
    threading.Timer(4.0, gate.set).start()
    t0 = time.monotonic()
    with pytest.raises(W.BudgetExceeded):
        G.build(io.BytesIO(arc), RUN_DT, listed, deadline=time.monotonic() + 1.0, pool_factory=threads, workers=2,
                log=lambda s: None)
    assert time.monotonic() - t0 < 3.5
    gate.set()


def test_a_kept_part_whose_direction_rounds_to_360_is_stored_as_0():
    base = RUN_DT.replace(tzinfo=None)
    rec, _ = G.encode_steps({base: (1.0, [{"hs": 0.5, "tp": 10.0, "dir": 359.6, "ws": False, "label": 1}])}, RUN_DT)
    assert G.decode_record(rec)[0][1] == [(0.5, 10.0, 0, False)]


def test_the_budget_stops_the_build_and_terminates_the_pool():
    class Pool(multiprocessing.pool.ThreadPool):
        terminated = False

        def terminate(self):
            Pool.terminated = True
            super().terminate()
    with pytest.raises(W.BudgetExceeded):
        G.build(io.BytesIO(three()), RUN_DT, LISTED, deadline=time.monotonic() - 1, pool_factory=Pool, workers=1)
    assert Pool.terminated


def test_the_winds_jobs_output_is_byte_identical_across_the_factoring():
    """The members() generator was carved out of gfs_winds.build for this job; the winds product of a fixed synthetic
    archive is pinned (its sha256 before the change)."""
    from test_gfs_winds import three as winds_three
    body, doc = W.build(io.BytesIO(winds_three()), datetime(2026, 10, 25, 12, tzinfo=timezone.utc), log=lambda s: None)
    assert doc["count"] == 2 and hashlib.sha256(body).hexdigest() == \
        "d11bfa7fae53c7df8e9f7deb54b6fdc5a50e5f5ab2aa4ab0b91f32486ca454cb"


# ---------------- main ----------------

def test_main_publishes_the_objects_then_the_pointer_last():
    st = store()
    fake = FakeNoaa({RUN: three()})
    assert _main(fake, st) == 0
    puts = [k for op, k in st.client.log if op == "put"]
    assert puts == [G.bin_key(RUN), G.index_key(RUN), P.latest_key(G.PREFIX)]
    objs = st.client.objects
    assert objs[G.bin_key(RUN)]["cc"] == P.IMMUTABLE and objs[G.index_key(RUN)]["cc"] == P.IMMUTABLE
    assert objs[P.latest_key(G.PREFIX)]["cc"] == P.POINTER
    doc = json.loads(objs[G.index_key(RUN)]["body"])
    assert doc["sha256"] == hashlib.sha256(objs[G.bin_key(RUN)]["body"]).hexdigest()
    ptr = json.loads(objs[P.latest_key(G.PREFIX)]["body"])
    assert ptr["run"] == RUN and ptr["index"] == G.index_key(RUN) and ptr["stations"] == 3 and ptr["steps"] == 385
    assert not any(k.startswith(W.PREFIX) for k in objs), "the winds product untouched"


def test_main_already_live_and_never_regresses():
    st = store()
    fake = FakeNoaa({RUN: three()})
    assert _main(fake, st) == 0
    n, gets = len(st.client.log), sum(1 for m, _ in fake.calls if m == "GET")
    assert _main(fake, st) == 0 and len(st.client.log) == n
    assert sum(1 for m, _ in fake.calls if m == "GET") == gets
    st.client.objects[P.latest_key(G.PREFIX)]["body"] = json.dumps({"run": "2026110118", "published_utc":
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}).encode()
    assert _main(fake, st) == 0 and len(st.client.log) == n
    assert _main(fake, st, "--run", RUN) == 2, "the pointer never moves back"


def test_a_run_asked_for_by_name_is_never_rewritten_and_a_newer_one_moves_the_pointer(capsys):
    """G48a U5 / U6: `--run <the live run>` rewrites nothing (its objects are immutable); `--run <a newer run>`
    builds it and moves the pointer forward; a run older than the live one is refused."""
    st = store()
    later = "2026110118"
    fake = FakeNoaa({RUN: three(), later: archive({s: spec_text(s, run_dt=RUN_DT + timedelta(hours=6), steps=range(3))
                                                   for s in sorted(LISTED)})})
    assert _main(fake, st) == 0                                   # RUN published
    n = len(st.client.log)
    assert _main(fake, st, "--run", RUN) == 0 and len(st.client.log) == n
    assert "already published" in capsys.readouterr().out
    assert _main(fake, st, "--run", later, hours=7) == 0
    assert [k for op, k in st.client.log[n:] if op == "put"] == [G.bin_key(later), G.index_key(later),
                                                               P.latest_key(G.PREFIX)]
    assert json.loads(st.client.objects[P.latest_key(G.PREFIX)]["body"])["run"] == later
    assert _main(fake, st, "--run", RUN) == 2                     # older than the live run: refused, nothing written
    assert len([k for op, k in st.client.log if op == "put"]) == 6


def test_the_stale_alarm_also_fires_on_the_not_now_paths():
    """G48a U7: a run that cannot be served or built does not hide a stale pointer (the winds job's G36a P2-2)."""
    old = (datetime.now(timezone.utc) - timedelta(hours=19)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def stale_store():
        st = store()
        st.put(P.latest_key(G.PREFIX), json.dumps({"run": "2026103112", "published_utc": old}).encode(),
               "application/json", P.POINTER)
        return st

    class Vanishing(FakeNoaa):
        def __call__(self, req, timeout=None):
            if req.get_method() == "GET":
                raise W.urllib.error.HTTPError(req.full_url, 404, "x", {}, None)
            return super().__call__(req, timeout)
    st = stale_store()
    assert _main(Vanishing({RUN: three()}), st) == 1              # not served + stale: the alarm, once
    assert f"{G.PREFIX}/stale-alarm.json" in st.client.objects
    assert _main(Vanishing({RUN: three()}), st) == 3
    st = stale_store()
    P.record_failure(st, RUN, "boom", G.PREFIX)
    assert _main(FakeNoaa({RUN: three()}), st) == 1               # failed recently + stale: the alarm
    assert f"{G.PREFIX}/stale-alarm.json" in st.client.objects


def _fresh_pointer(st, run="2026110106"):
    st.put(P.latest_key(G.PREFIX), json.dumps({"run": run, "published_utc":
           datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}).encode(), "application/json", P.POINTER)


def test_no_run_a_404_and_the_budget_are_exit_3_with_records(monkeypatch):
    st = store()
    _fresh_pointer(st)
    assert _main(FakeNoaa({}), st) == 3

    class Vanishing(FakeNoaa):
        def __call__(self, req, timeout=None):
            if req.get_method() == "GET":
                raise W.urllib.error.HTTPError(req.full_url, 404, "x", {}, None)
            return super().__call__(req, timeout)
    assert _main(Vanishing({RUN: three()}), st) == 3
    assert json.loads(st.client.objects[P.notready_key(RUN, G.PREFIX)]["body"])["count"] == 1
    monkeypatch.setattr(G, "BUILD_BUDGET_S", -1)
    assert _main(FakeNoaa({RUN: three()}), st) == 3
    assert json.loads(st.client.objects[P.notready_key(RUN, G.PREFIX)]["body"])["count"] == 2
    assert P.latest_key(G.PREFIX) in st.client.objects and G.index_key(RUN) not in st.client.objects


def test_a_bad_archive_is_a_recorded_failure_retried_after_an_hour():
    st = store()
    _fresh_pointer(st)
    bad = archive({"./other.txt": "x", "51201": spec_text("51201", steps=range(2))})
    fake = FakeNoaa({RUN: bad})
    assert _main(fake, st) == 1
    rec = json.loads(st.client.objects[P.failed_key(RUN, G.PREFIX)]["body"])
    assert rec["attempts"] == 1 and "ValueError" in rec["last_error"]
    gets = sum(1 for m, _ in fake.calls if m == "GET")
    assert _main(fake, st) == 3 and sum(1 for m, _ in fake.calls if m == "GET") == gets
    assert json.loads(st.client.objects[P.latest_key(G.PREFIX)]["body"])["run"] == "2026110106"


def test_the_stale_alarm_fires_once_past_18_hours():
    st = store()
    old = (datetime.now(timezone.utc) - timedelta(hours=19)).strftime("%Y-%m-%dT%H:%M:%SZ")
    st.put(P.latest_key(G.PREFIX), json.dumps({"run": "2026103112", "published_utc": old}).encode(),
           "application/json", P.POINTER)
    assert _main(FakeNoaa({}), st) == 1
    assert _main(FakeNoaa({}), st) == 3
    assert f"{G.PREFIX}/stale-alarm.json" in st.client.objects


def test_dry_run_and_local_write_files_and_publish_nothing(tmp_path):
    st = store()
    assert G.main(["--dry-run", "--out", str(tmp_path)], opener=FakeNoaa({RUN: three()}), store=st,
                  now=RUN_DT + timedelta(hours=1), listed=LISTED, pool_factory=threads) == 0
    assert st.client.log == []
    doc = json.load(open(tmp_path / "index.json"))
    assert doc["count"] == 3 and (tmp_path / "spectral.bin").stat().st_size == 3 * G.RECORD_BYTES
    local = tmp_path / "arc"
    local.mkdir()
    (local / os.path.basename(W.archive_key(RUN_DT))).write_bytes(three())
    assert G.main(["--local", str(local), "--run", RUN, "--out", str(tmp_path / "o")], listed=LISTED,
                  pool_factory=threads) == 0
    assert (tmp_path / "o" / "spectral.bin").read_bytes() == (tmp_path / "spectral.bin").read_bytes()
    assert G.main(["--local", str(local)], listed=LISTED) == 2
    assert G.main(["--run", "2026110113"], listed=LISTED) == 2


def test_prune_keeps_four_runs_under_its_prefix_only():
    st = store()
    for run in ["2026103100", "2026103106", "2026103112", "2026103118", "2026110100", "2026110106"]:
        st.put(G.bin_key(run), b"x", "application/octet-stream", P.IMMUTABLE)
        st.put(G.index_key(run), b"{}", "application/json", P.IMMUTABLE)
    st.put(W.winds_key("2026103100"), b"x", "application/octet-stream", P.IMMUTABLE)
    st.put(f"{G.PREFIX}/2026110112/spectral.bin", b"x", "application/octet-stream", P.IMMUTABLE)   # being written
    st.put(P.failed_key("2026103100", G.PREFIX), b"{}", "application/json", P.POINTER)
    st.put(P.latest_key(G.PREFIX), json.dumps({"run": "2026110106"}).encode(), "application/json", P.POINTER)
    assert G.prune(st) == ["2026103100", "2026103106"]
    keys = set(st.client.objects)
    assert G.index_key("2026103112") in keys and G.index_key("2026103100") not in keys
    assert W.winds_key("2026103100") in keys and f"{G.PREFIX}/2026110112/spectral.bin" in keys
    assert P.failed_key("2026103100", G.PREFIX) not in keys


def test_workflow_trigger_keepalive_and_tests_paths():
    wf = os.path.join(ROOT, ".github", "workflows")
    text = open(os.path.join(wf, "model-gfsspec.yml"), encoding="utf-8").read()
    assert "group: model-gfsspec" in text and "python tools/model_frames/gfs_spectral.py" in text
    assert 'cron: "*/10 * * * *"' in text and 'if [ "$rc" -eq 3 ]' in text and "timeout-minutes: 40" in text
    assert "nproc" in text
    assert not os.path.exists(os.path.join(wf, "gfsspec-dryrun.yml"))             # the step-2 dry run was temporary
    assert text.split("create-args: >-", 1)[1].split("cache-environment", 1)[0] == \
        open(os.path.join(wf, "model-gfswinds.yml"), encoding="utf-8").read().split("create-args: >-", 1)[1].split(
            "cache-environment", 1)[0]
    uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
    assert uses and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses)
    keep = open(os.path.join(wf, "model-frames-keepalive.yml"), encoding="utf-8").read()
    assert "gh workflow enable model-gfsspec.yml" in keep
    tests = open(os.path.join(wf, "model-frames-tests.yml"), encoding="utf-8").read()
    assert tests.count('".github/workflows/model-gfsspec.yml"') == 2
    cfg = open(os.path.join(ROOT, "tools", "model_frames", "trigger", "wrangler.jsonc"), encoding="utf-8").read()
    assert ('"GH_WORKFLOW": "model-frames.yml,model-points.yml,model-ecmwf.yml,model-glwu.yml,model-gfswinds.yml,'
            'model-gfsspec.yml,model-nwps.yml,model-alt.yml,model-sarswell.yml"' in cfg)
    assert P.GFSSPEC_PREFIX == "gfswave/spectral/v1" and G.PREFIX == P.GFSSPEC_PREFIX
    assert G.BUILD_BUDGET_S == 25 * 60 and G.KEEP == 4 and G.SRC.stale_fail_h == 18


def test_the_listed_stations_come_from_the_station_list():
    ids = G.listed_stations()
    assert "51201" in ids and "46026" in ids and len(ids) > 700
    with pytest.raises(ValueError):
        G.listed_stations(os.path.join(ROOT, "nwps_stations.json"))
