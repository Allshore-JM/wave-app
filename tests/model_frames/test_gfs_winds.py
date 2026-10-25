"""The GFS winds job (plan section 53 B): tools/model_frames/gfs_winds.py on a synthetic spectra archive written the way
NOAA writes gfswave.tHHz.spec_tar.gz (a gzip of a pax tar of ./gfswave.<id>.spec members). No network, no secrets."""
import gzip
import io
import json
import os
import re
import sys
import tarfile
from datetime import datetime, timedelta, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import gfs_winds as W  # noqa: E402
import publish as P  # noqa: E402

RUN_DT = datetime(2026, 10, 25, 12, tzinfo=timezone.utc)
RUN = "2026102512"
FIX = os.path.join(ROOT, "tests", "fixtures", "gfswinds")


def spec_text(sid, run_dt=RUN_DT, steps=range(385), wind=lambda h: (5.0 + h / 100, (h * 7) % 360 + 0.5)):
    """A .spec file: the header, then per step a datetime line, the point line and a few spectral lines."""
    lines = ["'WAVEWATCH III SPECTRA'     50    36     1 'spectral resolution for points'",
             " 0.350E-01 0.375E-01 0.401E-01"]
    base = run_dt.replace(tzinfo=None)
    for h in steps:
        t = base + timedelta(hours=h)
        u, d = wind(h)
        lines.append(t.strftime("%Y%m%d %H%M%S"))
        lines.append(f"'{sid:<10s}'  21.67-158.12     472.7  {u:5.2f} {d:5.1f}   0.05 139.8")
        lines += ["  0.176E-09  0.760E-08  0.231E-08  0.146E-07", "  0.232E-04  0.421E-04  0.540E-05"]
    return "\n".join(lines) + "\n"


def archive(members, pax=True):
    """gzip(tar) bytes of {name: text} (name without ./gfswave. ... .spec unless it starts with './')."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
        for name, text in members.items():
            body = text.encode() if isinstance(text, str) else text
            ti = tarfile.TarInfo(name if name.startswith("./") else f"./gfswave.{name}.spec")
            ti.size = len(body)
            if pax:
                ti.pax_headers = {"atime": "1792840182", "ctime": "1792840751"}
            tf.addfile(ti, io.BytesIO(body))
    return gzip.compress(buf.getvalue())


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
    """`archives[run_name]` = bytes; HEAD answers its length, GET streams it; `status` forces an HTTP error."""

    def __init__(self, archives=None, status=None, error=None):
        self.archives, self.status, self.error, self.calls = dict(archives or {}), status, error, []

    def __call__(self, req, timeout=None):
        url, method = req.full_url, req.get_method()
        self.calls.append((method, url))
        if self.error:
            raise self.error
        if self.status:
            raise W.urllib.error.HTTPError(url, self.status, "x", {}, None)
        m = re.search(r"gfs\.(\d{8})/(\d{2})/wave/station/gfswave\.t\d{2}z\.spec_tar\.gz$", url)
        data = self.archives.get(m.group(1) + m.group(2)) if m else None
        if data is None:
            raise W.urllib.error.HTTPError(url, 404, "not found", {}, None)
        return _Resp(b"" if method == "HEAD" else data, length=len(data))


def store():
    return P.Store(FakeClient(), "bucket")


def now_after(run_dt=RUN_DT, hours=6):
    return run_dt + timedelta(hours=hours)


def three():
    return archive({"51201": spec_text("51201"), "41113": spec_text("41113", steps=[h for h in range(385) if h != 7]),
                    "NOWIND": "'WAVEWATCH III SPECTRA'  50 36 1 'x'\n 0.35E-01\n"})


# ---------------- the parser ----------------

def test_point_winds_read_the_first_quoted_line_after_each_time():
    got = W.spec_point_winds(spec_text("51201", steps=range(3)).encode())
    assert got == {datetime(2026, 10, 25, 12): (5.0, 0.5), datetime(2026, 10, 25, 13): (5.01, 7.5),
                   datetime(2026, 10, 25, 14): (5.02, 14.5)}


def test_point_winds_match_the_sites_parser_on_the_repo_fixture_and_a_real_member():
    sys.path.insert(0, ROOT)
    import app                                                          # noqa: E402  the site's own parser
    for path in (os.path.join(ROOT, "tests", "fixtures", "gfswave_spec_sample.spec"),
                 os.path.join(FIX, "gfswave_13130_member.spec")):
        data = open(path, "rb").read()
        mine = W.spec_point_winds(data)
        site = app._parse_spec_wind_text(data.decode("ascii").splitlines())
        assert mine == site and len(mine) >= 8, path


def test_point_lines_with_too_few_numbers_or_junk_are_skipped():
    text = ("20261025 120000\n'51201     '  21.67-158.12  472.7  8.09\n"
            "20261025 130000\n'51201     '  21.67-158.12  472.7  nan 71.6\n"
            "20261025 140000\n\n'51201     '  21.67-158.12  472.7  8.10  72.0  0.0 0.0\n")
    assert W.spec_point_winds(text.encode()) == {datetime(2026, 10, 25, 14): (8.10, 72.0)}


def test_a_station_record_round_trips_and_marks_missing_steps():
    winds = W.spec_point_winds(spec_text("41113", steps=[0, 1, 384]).encode())
    winds[datetime(2026, 10, 30, 12, 30)] = (1.0, 1.0)                  # not on the hour: left out
    winds[datetime(2026, 11, 12, 12)] = (1.0, 1.0)                      # past +384 h: left out
    rec, present = W.encode_station(winds, RUN_DT)
    assert len(rec) == W.RECORD_BYTES and present == 3
    out = W.decode_record(rec)
    assert out[0] == (5.0, 0.5) and out[1] == (5.01, 7.5) and out[384] == (8.84, 168.5)
    assert out[2] is None and sum(v is not None for v in out) == 3


def test_values_round_trip_exactly_at_the_files_precision():
    for u in (0.0, 0.01, 12.34, 99.99, 655.34):
        for d in (0.0, 0.1, 71.6, 359.9, 360.0):
            rec, _ = W.encode_station({datetime(2026, 10, 25, 12): (u, d)}, RUN_DT)
            assert W.decode_record(rec)[0] == (u, d), (u, d)
    rec, present = W.encode_station({datetime(2026, 10, 25, 12): (-1.0, 10.0),
                                     datetime(2026, 10, 25, 13): (1.0, 400.0)}, RUN_DT)
    assert present == 0, "out of range values are missing, never wrapped"


# ---------------- the build ----------------

def test_the_build_keeps_every_station_with_wind():
    body, doc = W.build(io.BytesIO(three()), RUN_DT, log=lambda *a: None)
    assert doc["format"] == "gfswinds-v1" and doc["run"] == RUN and doc["steps"] == list(range(385))
    assert doc["stations"] == {"51201": 0, "41113": 1} and doc["count"] == 2 and doc["left_out"] == ["NOWIND"]
    assert doc["members"] == 3 and len(body) == 2 * W.RECORD_BYTES
    a = W.decode_record(body[:W.RECORD_BYTES])
    b = W.decode_record(body[W.RECORD_BYTES:])
    assert a[100] == (6.0, 340.5) and b[7] is None and b[8] == (5.08, 56.5)


@pytest.mark.parametrize("bad", ["name", "dir", "twice", "size", "few"])
def test_the_build_refuses_an_archive_it_cannot_trust(bad, monkeypatch):
    members = {"51201": spec_text("51201", steps=range(3)), "41113": spec_text("41113", steps=range(3))}
    if bad == "name":
        members["./other.txt"] = "x"
    if bad == "size":
        monkeypatch.setattr(W, "MEMBER_MAX", 100)
    if bad == "few":
        monkeypatch.setattr(W, "MIN_MEMBERS", 3)
    data = archive(members)
    if bad in ("dir", "twice"):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tf:
            for sid in ("51201", "51201" if bad == "twice" else "41113"):
                body = spec_text(sid, steps=range(3)).encode()
                ti = tarfile.TarInfo(f"./gfswave.{sid}.spec")
                ti.size = len(body)
                tf.addfile(ti, io.BytesIO(body))
            if bad == "dir":
                ti = tarfile.TarInfo("./gfswave.X.spec")
                ti.type = tarfile.DIRTYPE
                tf.addfile(ti)
        data = gzip.compress(buf.getvalue())
    with pytest.raises(ValueError):
        W.build(io.BytesIO(data), RUN_DT, log=lambda *a: None)


def test_a_cut_stream_fails_the_build():
    data = three()
    with pytest.raises(Exception):
        W.build(io.BytesIO(data[:len(data) // 2]), RUN_DT, log=lambda *a: None)


def test_the_budget_stops_the_build():
    with pytest.raises(TimeoutError):
        W.build(io.BytesIO(three()), RUN_DT, deadline=0, log=lambda *a: None)


# ---------------- discovery ----------------

def test_discovery_takes_the_newest_complete_cycle():
    fake = FakeNoaa({"2026102506": b"x" * 200, "2026102512": b"x" * 200})
    assert W.newest_complete(now_after(hours=7), head=lambda u: W.head_size(u, opener=fake)) == (RUN_DT, 200)
    assert [u for m, u in fake.calls][0].endswith("gfs.20261025/18/wave/station/gfswave.t18z.spec_tar.gz")


def test_a_small_archive_is_not_a_run():
    fake = FakeNoaa({"2026102512": b"x" * 50, "2026102506": b"x" * 200})
    run_dt, _ = W.newest_complete(now_after(hours=1), head=lambda u: W.head_size(u, opener=fake))
    assert run_dt == RUN_DT - timedelta(hours=6)


def test_head_failures_after_their_tries_are_transport_errors():
    with pytest.raises(W.TransportError):
        W.head_size("https://x/y", opener=FakeNoaa(error=ConnectionError("down")))
    assert W.head_size("https://x/y", opener=FakeNoaa(status=403)) is None


# ---------------- main ----------------

def _main(fake, st, *args, hours=1):
    return W.main(list(args), opener=fake, store=st, now=now_after(hours=hours))


def test_main_publishes_the_objects_then_the_pointer_last():
    st = store()
    fake = FakeNoaa({RUN: three()})
    assert _main(fake, st) == 0
    puts = [k for op, k in st.client.log if op == "put"]
    assert puts == [W.winds_key(RUN), W.index_key(RUN), P.latest_key(W.PREFIX)]
    objs = st.client.objects
    assert objs[W.winds_key(RUN)]["cc"] == P.IMMUTABLE and objs[W.index_key(RUN)]["cc"] == P.IMMUTABLE
    assert objs[P.latest_key(W.PREFIX)]["cc"] == P.POINTER
    doc = json.loads(objs[W.index_key(RUN)]["body"])
    import hashlib
    assert doc["sha256"] == hashlib.sha256(objs[W.winds_key(RUN)]["body"]).hexdigest()
    ptr = json.loads(objs[P.latest_key(W.PREFIX)]["body"])
    assert ptr["run"] == RUN and ptr["index"] == W.index_key(RUN) and ptr["stations"] == 2 and ptr["steps"] == 385


def test_main_does_nothing_when_the_run_is_live_and_never_regresses():
    st = store()
    fake = FakeNoaa({RUN: three()})
    assert _main(fake, st) == 0
    n, gets = len(st.client.log), sum(1 for m, _ in fake.calls if m == "GET")
    assert _main(fake, st) == 0 and len(st.client.log) == n, "already live: no put"
    assert sum(1 for m, _ in fake.calls if m == "GET") == gets, "and no download"
    newer = "2026102518"
    st.client.objects[P.latest_key(W.PREFIX)]["body"] = json.dumps({"run": newer, "published_utc":
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}).encode()
    assert _main(fake, st) == 0 and len(st.client.log) == n


def test_no_complete_run_is_exit_3_and_a_404_while_reading_is_notready():
    st = store()
    st.put(P.latest_key(W.PREFIX), json.dumps({"run": "2026102506", "published_utc":
           datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}).encode(), "application/json", P.POINTER)
    assert _main(FakeNoaa({}), st) == 3

    class Vanishing(FakeNoaa):                                          # listed by HEAD, gone at GET
        def __call__(self, req, timeout=None):
            if req.get_method() == "GET":
                raise W.urllib.error.HTTPError(req.full_url, 404, "x", {}, None)
            return super().__call__(req, timeout)
    assert _main(Vanishing({RUN: three()}), st) == 3
    assert json.loads(st.client.objects[P.notready_key(RUN, W.PREFIX)]["body"])["count"] == 1


def test_a_bad_archive_is_a_recorded_failure_retried_after_an_hour():
    st = store()
    bad = archive({"./other.txt": "x", "51201": spec_text("51201", steps=range(2))})
    fake = FakeNoaa({RUN: bad})
    assert _main(fake, st) == 1
    rec = json.loads(st.client.objects[P.failed_key(RUN, W.PREFIX)]["body"])
    assert rec["attempts"] == 1 and "ValueError" in rec["last_error"]
    gets = sum(1 for m, _ in fake.calls if m == "GET")
    assert _main(fake, st) == 3 and sum(1 for m, _ in fake.calls if m == "GET") == gets, "waits an hour"
    assert P.latest_key(W.PREFIX) not in st.client.objects


def test_the_stale_alarm_fires_once_past_18_hours():
    st = store()
    old = (datetime.now(timezone.utc) - timedelta(hours=19)).strftime("%Y-%m-%dT%H:%M:%SZ")
    st.put(P.latest_key(W.PREFIX), json.dumps({"run": "2026102412", "published_utc": old}).encode(),
           "application/json", P.POINTER)
    assert _main(FakeNoaa({}), st) == 1
    assert _main(FakeNoaa({}), st) == 3, "at most once every 6 h"
    assert f"{W.PREFIX}/stale-alarm.json" in st.client.objects


def test_dry_run_publishes_nothing_and_can_write_local_files(tmp_path):
    st = store()
    assert W.main(["--dry-run", "--out", str(tmp_path)], opener=FakeNoaa({RUN: three()}), store=st,
                  now=now_after(hours=1)) == 0
    assert st.client.log == []
    doc = json.load(open(tmp_path / "index.json"))
    assert doc["count"] == 2 and (tmp_path / "winds.bin").stat().st_size == 2 * W.RECORD_BYTES


def test_prune_keeps_four_runs_and_only_its_prefix():
    st = store()
    for i, run in enumerate(["2026102400", "2026102406", "2026102412", "2026102418", "2026102500", "2026102506"]):
        st.put(W.winds_key(run), b"x", "application/octet-stream", P.IMMUTABLE)
        st.put(W.index_key(run), b"{}", "application/json", P.IMMUTABLE)
    st.put(f"{W.PREFIX}/2026102512/winds.bin", b"x", "x", P.IMMUTABLE)      # a later build still writing
    st.put("gfswave/0p25/v1/2026102400/x.png", b"x", "x", P.IMMUTABLE)
    st.put(P.latest_key(W.PREFIX), json.dumps({"run": "2026102506"}).encode(), "application/json", P.POINTER)
    assert W.prune(st) == ["2026102400", "2026102406"]
    keys = set(st.client.objects)
    assert "gfswave/0p25/v1/2026102400/x.png" in keys and f"{W.PREFIX}/2026102512/winds.bin" in keys
    assert W.index_key("2026102412") in keys and W.index_key("2026102400") not in keys


def test_a_bad_run_argument_is_refused():
    assert W.main(["--run", "2026102513"], opener=FakeNoaa({}), store=store()) == 2


# ---------------- the schedule ----------------

def test_workflow_trigger_keepalive_and_tests_paths():
    wf = os.path.join(ROOT, ".github", "workflows")
    text = open(os.path.join(wf, "model-gfswinds.yml"), encoding="utf-8").read()
    assert "group: model-gfswinds" in text and "python tools/model_frames/gfs_winds.py" in text
    assert 'cron: "*/10 * * * *"' in text and 'if [ "$rc" -eq 3 ]' in text and "timeout-minutes: 30" in text
    assert text.split("create-args: >-", 1)[1].split("cache-environment", 1)[0] == \
        open(os.path.join(wf, "model-glwu.yml"), encoding="utf-8").read().split("create-args: >-", 1)[1].split(
            "cache-environment", 1)[0], "one cached environment for every publisher"
    uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
    assert uses and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses)
    keep = open(os.path.join(wf, "model-frames-keepalive.yml"), encoding="utf-8").read()
    assert "gh workflow enable model-gfswinds.yml" in keep
    tests = open(os.path.join(wf, "model-frames-tests.yml"), encoding="utf-8").read()
    assert tests.count('".github/workflows/model-gfswinds.yml"') == 2 and tests.count('"tests/fixtures/gfswinds/**"') == 2
    cfg = open(os.path.join(ROOT, "tools", "model_frames", "trigger", "wrangler.jsonc"), encoding="utf-8").read()
    assert ('"GH_WORKFLOW": "model-frames.yml,model-points.yml,model-ecmwf.yml,model-glwu.yml,model-gfswinds.yml"'
            in cfg)
    assert P.GFSWINDS_PREFIX == "gfswave/winds/v1" and W.PREFIX == P.GFSWINDS_PREFIX
