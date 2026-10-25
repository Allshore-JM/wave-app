"""The site's GFS wind reader (plan section 53 C): get_station_wind reads the GFS winds job's table from the frames
bucket first (gfswave/winds/v1/<RUN>/index.json + winds.bin), NOMADS's .spec second. The table is built here with the
job's own encoder (tools/model_frames/gfs_winds.py), so the reader is tested against the real format. No network."""
import hashlib
import json
import os
import sys
import threading
from datetime import datetime, timedelta, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import app  # noqa: E402
import gfs_winds as W  # noqa: E402

DATE, RUN = "20261025", "12"
RUN_DT = datetime(2026, 10, 25, 12, tzinfo=timezone.utc)
BASE = "https://models.test/gfswave/0p25/v1"
PRE = "gfswave/winds/v1/2026102512"


def spec(sid, steps=range(385)):
    lines = ["'WAVEWATCH III SPECTRA'     50    36     1 'spectral resolution for points'", " 0.350E-01"]
    for h in steps:
        lines.append((datetime(2026, 10, 25, 12) + timedelta(hours=h)).strftime("%Y%m%d %H%M%S"))
        lines.append(f"'{sid:<10s}'  21.67-158.12     472.7  {(h % 97) * 0.29 + 0.01:5.2f} {(h * 13) % 3600 / 10:5.1f}")
        lines.append("  0.176E-09  0.760E-08")
    return "\n".join(lines) + "\n"


def table(stations):
    """(index bytes, winds.bin bytes) of {sid: spec text}, encoded by the job."""
    body, slots = bytearray(), {}
    for sid, text in stations.items():
        rec, present = W.encode_station(W.spec_point_winds(text.encode()), RUN_DT)
        slots[sid] = len(slots)
        body += rec
    doc = {"format": "gfswinds-v1", "run": DATE + RUN, "steps": list(range(385)), "stations": slots,
           "count": len(slots), "record_bytes": 1540, "missing": 65535, "sha256": hashlib.sha256(body).hexdigest()}
    return json.dumps(doc).encode(), bytes(body)


class Bucket:
    def __init__(self):
        self.objects, self.asked, self.delay = {}, [], 0

    def fetch(self, url, max_bytes, headers=None):
        key = url.split("models.test/", 1)[1]
        self.asked.append(key)
        if self.delay:
            import time as _t
            _t.sleep(self.delay)
        if key not in self.objects:
            raise app.HTTPStatusError(404)
        if isinstance(self.objects[key], Exception):
            raise self.objects[key]
        body = self.objects[key]
        if len(body) > max_bytes:
            raise IOError("body too large")
        return body


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def world(monkeypatch):
    b = Bucket()
    idx, body = table({"51201": spec("51201"), "41113": spec("41113", steps=[h for h in range(385) if h % 50])})
    b.objects[f"{PRE}/index.json"], b.objects[f"{PRE}/winds.bin"] = idx, body
    monkeypatch.setattr(app, "_points_fetch", b.fetch)
    monkeypatch.setenv("MODEL_FRAMES_BASE", BASE)
    monkeypatch.delenv("POINTS_ROOT", raising=False)
    monkeypatch.setattr(app, "GFS_WINDS", True)
    monkeypatch.setattr(app, "NOMADS_DISABLED", False)
    nomads = []

    class _Resp:
        def __init__(self, text):
            self.status_code, self.text, self.headers = (200 if text else 404), text, {}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def close(self):
            pass

        def iter_content(self, chunk_size=65536, **k):
            b2 = self.text.encode()
            return (b2[i:i + chunk_size] for i in range(0, len(b2), chunk_size))

    def nomads_get(url, **k):
        nomads.append(url)
        sid = url.rsplit("gfswave.", 1)[1][:-len(".spec")]
        return _Resp(spec(sid) if sid != "NOWHERE" else "")
    monkeypatch.setattr(app.HTTP, "get", nomads_get)
    clock = Clock()
    monkeypatch.setattr(app.time, "monotonic", clock)
    with app._CACHE_LOCK:
        app._WIND_CACHE.clear()
    with app._GFS_WINDS_LOCK:
        app._GFS_WINDS_RUNS.clear()
        app._GFS_WINDS_FAILED.clear()
        app._GFS_WINDS_INFLIGHT.clear()
        for k in app._GFS_WINDS_STATS:
            app._GFS_WINDS_STATS[k] = None if k in ("last_error", "last_run") else 0
    yield b, nomads, clock
    with app._CACHE_LOCK:
        app._WIND_CACHE.clear()
    with app._GFS_WINDS_LOCK:
        app._GFS_WINDS_RUNS.clear()
        app._GFS_WINDS_FAILED.clear()


def _fresh():
    with app._CACHE_LOCK:
        app._WIND_CACHE.clear()


def test_the_table_comes_first_and_equals_the_spec_parser(world):
    b, nomads, _ = world
    got = app.get_station_wind("51201", DATE, RUN)
    assert nomads == [] and len(got) == 385
    assert got == app._parse_spec_wind_text(spec("51201").splitlines()), "the same floats as the .spec parser"
    assert app._gfs_winds_health()["r2"] == 1 and app._gfs_winds_health()["last_run"] == DATE + RUN


def test_missing_steps_stay_missing(world):
    got = app.get_station_wind("41113", DATE, RUN)
    assert len(got) == 385 - 8 and datetime(2026, 10, 25, 12) not in got


def test_the_table_is_read_once_per_run(world):
    b, _, _ = world
    app.get_station_wind("51201", DATE, RUN)
    _fresh()
    app.get_station_wind("41113", DATE, RUN)
    assert b.asked.count(f"{PRE}/index.json") == 1 and b.asked.count(f"{PRE}/winds.bin") == 1


def test_a_station_not_in_the_run_reads_nomads(world):
    _, nomads, _ = world
    got = app.get_station_wind("46026", DATE, RUN)
    assert len(got) == 385 and nomads[0].endswith("/bulls.t12z/gfswave.46026.spec")
    h = app._gfs_winds_health()
    assert h["not_in_run"] == 1 and h["nomads"] == 1


def test_a_run_not_published_yet_reads_nomads_and_is_asked_again_after_two_minutes(world):
    b, nomads, clock = world
    assert len(app.get_station_wind("51201", DATE, "06")) == 385 and len(nomads) == 1
    asked = len(b.asked)
    _fresh()
    app.get_station_wind("51201", DATE, "06")
    assert len(b.asked) == asked, "remembered as missing"
    assert app._gfs_winds_health()["run_missing"] == 1
    clock.t += app._GFS_WINDS_MISS_TTL + 1
    _fresh()
    app.get_station_wind("51201", DATE, "06")
    assert len(b.asked) == asked + 1


@pytest.mark.parametrize("damage", ["sha", "run", "count", "short", "steps", "slots", "format", "timeout"])
def test_a_damaged_table_is_refused_and_nomads_serves(world, damage):
    b, nomads, clock = world
    idx = json.loads(b.objects[f"{PRE}/index.json"])
    body = b.objects[f"{PRE}/winds.bin"]
    if damage == "sha":
        body = body[:-1] + bytes([body[-1] ^ 1])
    if damage == "run":
        idx["run"] = "2026102506"
    if damage == "count":
        idx["count"] = 3
    if damage == "short":
        body = body[:-1540]
    if damage == "steps":
        idx["steps"] = list(range(384))
    if damage == "slots":
        idx["stations"]["41113"] = 0
    if damage == "format":
        idx["format"] = "gfswinds-v2"
    b.objects[f"{PRE}/index.json"], b.objects[f"{PRE}/winds.bin"] = json.dumps(idx).encode(), body
    if damage == "timeout":
        b.objects[f"{PRE}/winds.bin"] = IOError("too slow")
    got = app.get_station_wind("51201", DATE, RUN)
    assert len(got) == 385 and len(nomads) == 1
    h = app._gfs_winds_health()
    assert h["errors"] == 1 and h["last_error"] and h["runs_held"] == []
    asked = len(b.asked)
    _fresh()
    app.get_station_wind("51201", DATE, RUN)
    assert len(b.asked) == asked, "not asked again for a minute"
    clock.t += app._GFS_WINDS_ERR_TTL + 1
    _fresh()
    app.get_station_wind("51201", DATE, RUN)
    assert len(b.asked) > asked


def test_switched_off_or_without_a_bucket_reads_nomads_only(world, monkeypatch):
    b, nomads, _ = world
    monkeypatch.setattr(app, "GFS_WINDS", False)
    app.get_station_wind("51201", DATE, RUN)
    assert b.asked == [] and len(nomads) == 1
    monkeypatch.setattr(app, "GFS_WINDS", True)
    monkeypatch.delenv("MODEL_FRAMES_BASE")
    _fresh()
    app.get_station_wind("51201", DATE, RUN)
    assert b.asked == [] and len(nomads) == 2


def test_nomads_refusing_leaves_the_table_serving(world, monkeypatch):
    monkeypatch.setattr(app, "NOMADS_DISABLED", True)
    assert len(app.get_station_wind("51201", DATE, RUN)) == 385
    assert app.get_station_wind("46026", DATE, RUN) == {}, "not in the run and no NOMADS: blank, as before"


def test_the_swan_backfill_keeps_an_older_run_from_the_table(world):
    got = app.get_station_wind("51201", DATE, RUN, keep_s=app._WIND_PAST_RUN_TTL)
    assert len(got) == 385
    assert app._WIND_CACHE[("51201", DATE, RUN)]["ttl"] == app._WIND_PAST_RUN_TTL


def test_four_runs_are_held(world):
    b, _, _ = world
    for run in ("00", "06", "18"):
        idx = json.loads(b.objects[f"{PRE}/index.json"])
        idx["run"] = DATE + run
        b.objects[f"gfswave/winds/v1/{DATE}{run}/index.json"] = json.dumps(idx).encode()
        b.objects[f"gfswave/winds/v1/{DATE}{run}/winds.bin"] = b.objects[f"{PRE}/winds.bin"]
    b.objects["gfswave/winds/v1/2026102418/index.json"] = json.dumps(dict(json.loads(b.objects[f"{PRE}/index.json"]),
                                                                          run="2026102418")).encode()
    b.objects["gfswave/winds/v1/2026102418/winds.bin"] = b.objects[f"{PRE}/winds.bin"]
    for d, r in ((DATE, "00"), (DATE, "06"), (DATE, "12"), (DATE, "18"), ("20261024", "18")):
        assert app._gfs_winds_run(d, r) is not None
    assert app._gfs_winds_health()["runs_held"] == ["2026102506", "2026102512", "2026102518", "2026102418"]


def test_one_read_per_run_under_concurrent_requests(world):
    b, _, _ = world
    b.delay = 0.05                                                     # the threads queue on the run's lock
    out = []
    ts = [threading.Thread(target=lambda: out.append(app._gfs_winds_run(DATE, RUN))) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(out) == 8 and all(o is not None for o in out)
    assert b.asked.count(f"{PRE}/index.json") == 1


def test_healthz_reports_the_winds(world):
    app.get_station_wind("51201", DATE, RUN)
    with app.app.test_client() as c:
        h = c.get("/healthz").get_json()
    w = h["gfs"]["winds"]
    assert w["enabled"] is True and w["r2"] == 1 and w["runs_held"] == [DATE + RUN]
    assert "bulletins" in h["gfs"]


def test_a_forked_child_starts_with_a_fresh_lock(world):
    old = app._GFS_WINDS_LOCK
    app._live_after_fork()
    assert app._GFS_WINDS_LOCK is not old and app._GFS_WINDS_INFLIGHT == {}


def test_a_body_shorter_than_its_count_is_refused_even_with_a_matching_digest(world):
    """The digest alone does not prove the layout: a short body would make the slot read fail."""
    b, nomads, _ = world
    body = b.objects[f"{PRE}/winds.bin"][:1540]
    idx = json.loads(b.objects[f"{PRE}/index.json"])
    idx["sha256"] = hashlib.sha256(body).hexdigest()
    b.objects[f"{PRE}/index.json"], b.objects[f"{PRE}/winds.bin"] = json.dumps(idx).encode(), body
    assert len(app.get_station_wind("41113", DATE, RUN)) == 385 and len(nomads) == 1


def test_a_missing_run_waits_two_minutes_and_an_error_one(world):
    b, _, clock = world
    app.get_station_wind("51201", DATE, "06")                         # not published: 404
    asked = len(b.asked)
    clock.t += app._GFS_WINDS_ERR_TTL + 1
    _fresh()
    app.get_station_wind("51201", DATE, "06")
    assert len(b.asked) == asked, "a missing run is not asked again after only a minute"


def test_the_runs_held_are_the_last_used(world):
    b, _, _ = world
    idx0 = json.loads(b.objects[f"{PRE}/index.json"])
    runs = [("20261024", "18"), (DATE, "00"), (DATE, "06"), (DATE, "18")]
    for d, r in runs:
        b.objects[f"gfswave/winds/v1/{d}{r}/index.json"] = json.dumps(dict(idx0, run=d + r)).encode()
        b.objects[f"gfswave/winds/v1/{d}{r}/winds.bin"] = b.objects[f"{PRE}/winds.bin"]
    for d, r in [(DATE, RUN)] + runs[:3]:
        app._gfs_winds_run(d, r)
    app._gfs_winds_run(DATE, RUN)                                      # used again: the newest in the order
    app._gfs_winds_run(DATE, "18")                                     # a fifth run: the least recently used goes
    assert app._gfs_winds_health()["runs_held"] == ["2026102500", "2026102506", "2026102512", "2026102518"]
