"""The radar job's source table (plan section 86): each source's reach, readiness and time, and that one source cannot cost another its
frames. MRMS and OPERA staying byte-identical is test_radar_golden.py. Nothing reaches the network."""
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import radar as R  # noqa: E402

NOW = datetime(2026, 11, 6, 12, 6, 0, tzinfo=timezone.utc)
POINTER = "radar/v1/latest.json"


class Store:
    bucket = "fake"

    def __init__(self):
        self.objects = {}

    def put(self, key, body, content_type, cache_control):
        self.objects[key] = body

    def get_json(self, key):
        import json
        return json.loads(self.objects[key].decode()) if key in self.objects else None

    def delete_prefix(self, prefix):
        for k in [k for k in self.objects if k.startswith(prefix)]:
            del self.objects[k]


def lattice():
    a = np.ones((200, 400), np.uint8)
    a[20:80, 20:80] = 60
    return a


@pytest.fixture
def two(monkeypatch):
    """Two made-up sources, the first able to misbehave."""
    calls = {"a": [], "b": []}
    behave = {"a": None, "b": None}

    def reader(name):
        def read(region, dt, now, get):
            calls[name].append(R.slot_name(dt))
            if behave[name]:
                behave[name](dt)
            return lattice(), 0, None
        return read

    monkeypatch.setattr(R, "REGIONS", {"ra": ("A", "a", None, 20, -160, 22, -156, 1), "rb": ("B", "b", None, 50, 0, 52, 4, 1)})
    monkeypatch.setattr(R, "SOURCE", {
        "a": {"read": reader("a"), "ready_s": 0, "reach_h": 1, "backfill_s": 10, "spend_s": 45},
        "b": {"read": reader("b"), "ready_s": 0, "reach_h": 1, "backfill_s": 10, "spend_s": 60}})
    monkeypatch.setattr(R, "SOURCES", ("a", "b"))
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    return calls, behave


def tick(store, now=NOW, clock=None, args=()):
    kw = {"clock": clock} if clock else {}
    return R.main(list(args), get=lambda *a, **k: None, store=store, now=now, **kw)


# ---------------------------------------------------------------- the table
def test_the_real_table_keeps_the_order_and_the_clocks_of_stage_one():
    assert R.SOURCES[:2] == ("mrms", "opera")
    assert R.SOURCE["mrms"]["ready_s"] == R.MRMS_READY_S and R.SOURCE["opera"]["ready_s"] == 0
    assert R.SOURCE["mrms"]["backfill_s"] == R.BACKFILL_S / 2 and R.SOURCE["opera"]["backfill_s"] == R.BACKFILL_S
    assert R.SOURCE["mrms"]["spend_s"] is None and R.SOURCE["opera"]["spend_s"] is None
    assert R.SOURCE["mrms"]["reach_h"] is None and R.SOURCE["opera"]["reach_h"] is None


def test_every_region_belongs_to_a_source_in_the_table():
    assert {v[1] for v in R.REGIONS.values()} == set(R.SOURCES)


def test_a_reach_stops_the_plan_at_its_limit(two):
    names = [x[0] for x in R.plan(R.empty_pointer(), NOW, "a")]
    assert names == [R.slot_name(R.slot_of(NOW) - timedelta(minutes=10 * i)) for i in range(6)]   # 12:00 back to 11:10; 11:00 is 66 minutes old


def test_a_slot_younger_than_its_source_allows_is_not_taken(two, monkeypatch):
    monkeypatch.setitem(R.SOURCE["a"], "ready_s", 450)                 # 7:30, as Taiwan
    names = [x[0] for x in R.plan(R.empty_pointer(), NOW, "a")]
    assert R.slot_name(R.slot_of(NOW)) not in names                    # 6 min old
    assert names[0] == R.slot_name(R.slot_of(NOW) - timedelta(minutes=10))


def test_a_source_is_asked_for_nothing_past_its_reach(two):
    calls, _ = two
    assert tick(Store()) == 0
    assert min(calls["a"]) == R.slot_name(R.slot_of(NOW) - timedelta(minutes=50))


# ---------------------------------------------------------------- one source cannot cost another
def test_a_source_that_breaks_leaves_the_next_published_and_the_pointer_good(two):
    _calls, behave = two

    def boom(dt):
        raise RuntimeError("a bug in the first source")

    behave["a"] = boom
    store = Store()
    assert tick(store) == 0
    p = store.get_json(POINTER)
    assert R.good_pointer(p)
    assert any("rb" in s["r"] for s in p["slots"]) and not any("ra" in s["r"] for s in p["slots"])


def test_a_broken_source_does_not_stop_the_prune(two):
    calls, behave = two
    store = Store()
    tick(store)
    assert [k for k in store.objects if "/ra/" in k]

    def boom(dt):
        raise RuntimeError("a bug")

    behave["a"] = boom
    calls["a"].clear()
    tick(store, now=NOW + timedelta(hours=26))                         # everything retired on this tick
    tick(store, now=NOW + timedelta(hours=27, minutes=10))             # its grace over: deleted though source a still breaks
    old = R.slot_name(R.slot_of(NOW))
    assert not [k for k in store.objects if f"/{old}/" in k]


def test_a_slow_source_is_stopped_at_its_own_spend_and_the_next_gets_its_turn(two):
    calls, behave = two
    t = {"v": 0.0}

    def slow(dt):
        t["v"] += 30                                                   # each frame of source a takes 30 s

    behave["a"] = slow
    store = Store()
    tick(store, clock=lambda: t["v"])
    assert len(calls["a"]) == 2                                        # 0 s and 30 s start; 60 s is past its 45 s spend
    assert calls["b"], "the next source still ran"


def test_the_backfill_of_a_source_counts_from_its_own_turn(two):
    calls, behave = two
    t = {"v": 0.0}

    def slow(dt):
        t["v"] += 3

    behave["a"] = slow
    store = Store()
    tick(store, clock=lambda: t["v"])
    # a's slots: three recent at any time, older ones until 10 s into its turn
    assert 3 <= len(calls["a"]) <= 5
    # b starts about 15 s in: a clock counted from the tick's start would give b no older slots at all
    assert len(calls["b"]) == 6


def test_the_tick_deadline_still_caps_a_source_with_its_own_spend(two):
    calls, _ = two
    t = {"v": R.DEADLINE_S - 1.0}
    store = Store()
    first = iter([0.0])
    tick(store, clock=lambda: next(first, t["v"] + 2))
    assert calls["a"] == [] and calls["b"] == []


# ---------------------------------------------------------------- when a region joined
def test_a_new_region_does_not_alarm_on_its_first_failure(two):
    _calls, behave = two
    store = Store()
    tick(store)                                                        # both regions live
    p = store.get_json(POINTER)
    del p["began"]["rb"]
    del p["regions"]["rb"]
    for s in p["slots"]:
        s["r"].pop("rb", None)
    p["started"] = R.A.utc(NOW - timedelta(days=9))
    import json
    store.objects[POINTER] = json.dumps(p).encode()

    def missing(dt):
        raise R.Missing("HTTP 404")

    behave["b"] = missing
    later = NOW + timedelta(minutes=20)
    assert tick(store, now=later) == 0, "the job started nine days ago, but this region only joined now"
    p = store.get_json(POINTER)
    assert p["began"]["rb"] == R.A.utc(later)
    assert p["began"]["ra"] == R.A.utc(NOW)
    assert store.get_json("radar/v1/stale-alarm.json") is None


def test_a_new_region_alarms_once_it_has_been_silent_long_enough(two):
    _calls, behave = two
    store = Store()

    def missing(dt):
        raise R.Missing("HTTP 404")

    behave["b"] = missing
    assert tick(store) == 0
    assert tick(store, now=NOW + timedelta(hours=R.SOURCE_STALE_H + 1)) == 1


def test_a_listed_region_keeps_counting_from_the_jobs_start(two):
    """The pointer production holds today has no `began`: a silent stage-1 region must not get a fresh six hours from this release."""
    _calls, behave = two
    store = Store()
    tick(store)
    p = store.get_json(POINTER)
    del p["began"]
    for s in p["slots"]:
        s["r"].pop("rb", None)
    p["started"] = R.A.utc(NOW - timedelta(days=9))
    import json
    store.objects[POINTER] = json.dumps(p).encode()

    def missing(dt):
        raise R.Missing("HTTP 404")

    behave["b"] = missing
    assert tick(store, now=NOW + timedelta(minutes=10)) == 1
    assert store.get_json(POINTER)["began"]["rb"] == R.A.utc(NOW - timedelta(days=9))


@pytest.mark.parametrize("began", ["a string", {"ra": "not a time"}, {"ra": 5}])
def test_a_pointer_with_a_bad_began_is_refused(began):
    p = R.empty_pointer()
    p["began"] = began
    assert not R.good_pointer(p)


def test_a_job_drops_the_slot_entries_of_regions_it_does_not_write(two):
    """A job rolled back past a region's merge: the pointer it writes names none of that region's frames, at once, with no new frame."""
    import json
    store = Store()
    assert tick(store) == 0
    pointer = store.get_json(POINTER)
    kept = {slot["t"]: dict(slot["r"]) for slot in pointer["slots"]}
    assert kept
    for slot in pointer["slots"]:
        slot["r"]["mars"] = {"o": 0}
    store.objects[POINTER] = json.dumps(pointer).encode()
    assert tick(store) == 0
    after = store.get_json(POINTER)
    assert {slot["t"]: slot["r"] for slot in after["slots"]} == kept
    assert R.drop_unlisted(after) is False


def test_a_pointer_of_stage_one_is_good():
    p = R.empty_pointer()
    assert "began" not in p and R.good_pointer(p)


# ---------------------------------------------------------------- time limits per call
def test_a_source_can_ask_for_shorter_read_limits(monkeypatch):
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    seen = []

    class Answer:
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self, n=-1):
            return b""

    def get(url, timeout=60):
        seen.append(timeout)
        return Answer()

    R.fetch("https://x/y", get, 10, timeout=8)
    R.fetch("https://x/y", get, 10)
    assert seen == [8, R.FETCH_TIMEOUT_S]


# ---------------------------------------------------------------- the workflow
def test_a_repository_variable_can_switch_a_source_off():
    text = open(os.path.join(ROOT, ".github", "workflows", "model-radar.yml"), encoding="utf-8").read()
    assert "SOURCES: ${{ vars.RADAR_SOURCES }}" in text
    assert re.search(r'\[ -n "\$SOURCES" \] && ARGS\+=\(--sources "\$SOURCES"\)', text)
    assert 'exit 2' in text.split("bad RADAR_SOURCES")[1][:20], "a bad value stops the run rather than reach the job's argument line"


def test_the_job_refuses_a_source_that_does_not_exist():
    assert R.main(["--sources", "mrms,nope"], get=lambda *a, **k: None, store=Store(), now=NOW) == 2
