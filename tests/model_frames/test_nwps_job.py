"""The NWPS job (plan section 59, step 2): tools/model_frames/nwps.py against a fake NOMADS (directory pages and the real
spectra fixture moved to each run) and a fake R2. No network, no secrets."""
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, HERE)
import nwps as J  # noqa: E402
import nwps_fetch as N  # noqa: E402
import publish as Pub  # noqa: E402
from test_job import FakeClient  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "nwps")
NOW = datetime(2026, 10, 28, 18, 0, tzinfo=timezone.utc)
SPC = open(os.path.join(FIX, "51003.spc2d.txt"), encoding="ascii").read()
STATIONS = {"hfo": {"51003": {"office": "hfo", "region": "pr"}},
            "mtr": {"46026": {"office": "mtr", "region": "wr"}, "46012": {"office": "mtr", "region": "wr"}}}


def spectra_for(run_dt):
    """The real fixture's six hours, moved to start at run_dt."""
    def shift(m):
        t = datetime.strptime(m.group(1), "%Y%m%d.%H%M%S") - datetime(2026, 10, 28) + run_dt
        return t.strftime("%Y%m%d.%H%M%S")
    return re.sub(r"(?m)^(\d{8}\.\d{6})", shift, SPC).encode("ascii")


def page(entries):
    """A NOMADS directory page: [(name, modified datetime, size)]."""
    rows = ['<a href="/pub/data/nccf/com/nwps/prod/">Parent Directory</a>']
    rows += [f'<a href="{n}">{n}</a>                         {t:%d-%b-%Y %H:%M}   {s}' for n, t, s in entries]
    return ("<html><pre>\n" + "\n".join(rows) + "\n</pre></html>").encode()


class FakeNoaa:
    """NOMADS for the job: runs {(office, YYYYMMDDHH): {"sids": [...], "posted": datetime, "grib": bool}}."""

    def __init__(self, runs, broken=(), throttle=(), wrong_start=()):
        self.runs, self.broken, self.throttle, self.wrong_start = runs, set(broken), set(throttle), set(wrong_start)
        self.calls = []

    def __call__(self, url, rng=None):
        self.calls.append(url)
        for office in self.throttle:
            if f"/{office}/" in url:
                raise N.Throttled(url)
        m = re.search(r"/([a-z]{2})\.(\d{8})/([a-z]{3})/(?:(\d{2})/(CG1/)?(.*))?$", url)
        if not m:
            raise N.NotReady(url)
        _reg, day, office, hh, cg1, name = m.groups()
        runs = {k[1]: v for k, v in self.runs.items() if k[0] == office and k[1].startswith(day)}
        if hh is None:
            if not runs:
                raise N.NotReady(url)
            return 200, page([(f"{r[8:]}/", v["posted"], "-") for r, v in sorted(runs.items())])
        run = day + hh
        if run not in runs:
            raise N.NotReady(url)
        v = runs[run]
        if not cg1:
            return 200, page([("CG1/", v["posted"], "-")])
        files = [(f"nwps.t{hh}z.spc2d_{sid}_CG1.{office}.txt", v["posted"], "961K") for sid in v["sids"]]
        if v.get("grib", True):
            files.append((f"{office}_nwps_CG1_{day}_{hh}00.grib2", v["posted"], "94M"))
        if not name:
            return 200, page(files)
        sid = re.search(r"spc2d_([A-Za-z0-9]+)_CG1", name)
        if not sid or sid.group(1) not in v["sids"]:
            raise N.NotReady(url)
        if (office, sid.group(1)) in self.broken:
            raise RuntimeError("connection reset")
        start = datetime.strptime(run, "%Y%m%d%H")
        if (office, sid.group(1)) in self.wrong_start:
            start += timedelta(hours=1)
        return 200, spectra_for(start)


def runs_basic():
    return {("hfo", "2026102800"): {"sids": ["51003"], "posted": NOW - timedelta(hours=10)},
            ("mtr", "2026102812"): {"sids": ["46026", "46012"], "posted": NOW - timedelta(hours=1)}}


def store():
    return Pub.Store(FakeClient(), "bucket")


def go(args, fake, st, now=NOW):
    return J.main(args, request=fake, store=st, now=now, stations=STATIONS)


@pytest.fixture(autouse=True)
def _quiet(monkeypatch):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)

    def no_network(*a, **k):
        raise AssertionError("a test reached the real NOMADS")
    monkeypatch.setattr(N.G, "_open", no_network)                     # every NOMADS request goes through glwu_fetch


def test_publishes_every_office_station_files_then_index_then_pointer():
    st = store()
    assert go([], FakeNoaa(runs_basic()), st) == 0
    puts = [k for op, k in st.client.log if op == "put"]
    latest = Pub.latest_key(J.PREFIX)
    assert puts[-1] == latest
    for office, run, sids in (("hfo", "2026102800", ["51003"]), ("mtr", "2026102812", ["46012", "46026"])):
        keys = [J.station_key(office, run, s) for s in sids]
        i = puts.index(J.index_key(office, run))
        assert all(puts.index(k) < i for k in keys) and puts.index(latest, i) > i
        assert st.client.objects[keys[0]]["cc"] == Pub.IMMUTABLE
    ptr = st.get_json(latest)
    assert ptr["format"] == "nwps-pointer-v1" and st.client.objects[latest]["cc"] == Pub.POINTER
    assert ptr["offices"]["mtr"]["run"] == "2026102812" and ptr["offices"]["mtr"]["stations"] == ["46012", "46026"]
    assert ptr["offices"]["hfo"]["runs"] == ["2026102800"] and ptr["offices"]["hfo"]["region"] == "pr"
    doc = st.get_json(J.station_key("hfo", "2026102800", "51003"))
    assert doc["format"] == "nwps-v1" and doc["run"] == "2026102800" and doc["start_utc"] == "2026-10-28T00:00:00Z"
    assert doc["hours"] == list(range(6)) and len(doc["hs"]) == len(doc["parts"]) == len(doc["n"]) == 6
    assert doc["point"] == {"lat": 19.289, "lon": -160.569} and doc["swan_version"] == "41.10"
    assert doc["hs"][0] == pytest.approx(2.10, abs=0.01) and doc["parts"][0][0][1] == pytest.approx(13.6, abs=0.5)
    assert all(0 <= p[2] < 360 and isinstance(p[2], int) for hour in doc["parts"] for p in hour)
    idx = st.get_json(J.index_key("mtr", "2026102812"))
    body = st.client.objects[J.station_key("mtr", "2026102812", "46026")]["body"]
    assert idx["stations"]["46026"] == {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}


def test_already_live_publishes_nothing_but_a_heartbeat_once_an_hour():
    st = store()
    fake = FakeNoaa(runs_basic())
    assert go([], fake, st) == 0
    n = len(st.client.log)
    assert go([], fake, st) == 0 and len(st.client.log) == n           # nothing new, the pointer is fresh
    ptr = st.get_json(Pub.latest_key(J.PREFIX))
    ptr["checked_utc"] = "2026-10-28T10:00:00Z"
    st.put(Pub.latest_key(J.PREFIX), json.dumps(ptr).encode(), "application/json", Pub.POINTER)
    n = len(st.client.log)
    assert go([], fake, st) == 0
    assert [k for op, k in st.client.log[n:] if op == "put"] == [Pub.latest_key(J.PREFIX)]


def test_a_run_still_being_written_is_skipped_for_the_older_settled_one():
    runs = runs_basic()
    runs[("hfo", "2026102812")] = {"sids": ["51003"], "posted": NOW - timedelta(minutes=2)}          # not settled
    runs[("mtr", "2026102818")] = {"sids": ["46026"], "posted": NOW - timedelta(hours=1), "grib": False}   # no grib
    st = store()
    assert go([], FakeNoaa(runs), st) == 0
    ptr = st.get_json(Pub.latest_key(J.PREFIX))
    assert ptr["offices"]["hfo"]["run"] == "2026102800" and ptr["offices"]["mtr"]["run"] == "2026102812"
    runs[("hfo", "2026102812")]["posted"] = NOW - timedelta(minutes=6)
    assert go([], FakeNoaa(runs), st) == 0
    assert st.get_json(Pub.latest_key(J.PREFIX))["offices"]["hfo"]["runs"] == ["2026102812", "2026102800"]


def test_yesterdays_run_is_found_when_today_has_none():
    runs = {("hfo", "2026102712"): {"sids": ["51003"], "posted": NOW - timedelta(hours=26)}}
    st = store()
    assert go(["--office", "hfo"], FakeNoaa(runs), st) == 0
    assert st.get_json(Pub.latest_key(J.PREFIX))["offices"]["hfo"]["run"] == "2026102712"


def test_a_missing_or_wrong_station_file_is_left_out_the_rest_published():
    runs = runs_basic()
    runs[("mtr", "2026102812")]["sids"] = ["46026"]
    st = store()
    assert go([], FakeNoaa(runs), st) == 0
    idx = st.get_json(J.index_key("mtr", "2026102812"))
    assert list(idx["stations"]) == ["46026"] and idx["left_out"] == {"46012": "no spectra file in this run"}
    st2 = store()
    assert go([], FakeNoaa(runs_basic(), wrong_start={("mtr", "46012")}), st2) == 0
    left = st2.get_json(J.index_key("mtr", "2026102812"))["left_out"]
    assert left["46012"].startswith("unreadable: first time 2026102813 is not the run 2026102812")


def test_throttling_stops_the_tick_and_keeps_what_was_published():
    st = store()
    fake = FakeNoaa(runs_basic(), throttle={"mtr"})
    assert go([], fake, st) == 0                                      # hfo published: a tick with progress is green
    ptr = st.get_json(Pub.latest_key(J.PREFIX))
    assert list(ptr["offices"]) == ["hfo"] and not st.get_json(J.index_key("mtr", "2026102812"))
    st2 = store()
    fresh = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ptr = {"offices": {"mtr": {"run": "2026102812", "region": "wr", "published_utc": fresh, "stations": [],
                               "runs": ["2026102812"]}}, "checked_utc": fresh}
    st2.put(Pub.latest_key(J.PREFIX), json.dumps(ptr).encode(), "application/json", Pub.POINTER)
    fake = FakeNoaa(runs_basic(), throttle={"hfo"})
    assert go([], fake, st2) == 3                                     # nothing published: the next tick retries
    assert not any("/mtr/" in c for c in fake.calls)                  # the tick stopped at the throttle
    assert go([], FakeNoaa(runs_basic(), throttle={"hfo"}), store()) == 1   # nothing ever live: the stale alarm


def test_an_office_that_fails_is_recorded_and_retried_after_an_hour():
    st = store()
    fake = FakeNoaa(runs_basic(), broken={("mtr", "46026")})
    assert go([], fake, st) == 0                                      # hfo published
    rec = st.get_json(Pub.failed_key("mtr-2026102812", J.PREFIX))
    assert rec["attempts"] == 1 and "RuntimeError" in rec["last_error"]
    assert J._skip_failed(st, "mtr", "2026102812")
    calls = len(fake.calls)
    assert go([], fake, st) == 3                                      # waiting after the failure
    assert not any("spc2d" in c and "/mtr/" in c for c in fake.calls[calls:])
    st3 = store()
    assert go(["--office", "mtr"], FakeNoaa(runs_basic(), broken={("mtr", "46026")}), st3) == 1   # every office failed


@pytest.mark.parametrize("attempts,hours_ago,skipped", [(5, 2, False), (6, 2, True), (1, 0.5, True), (1, 1.1, False)])
def test_failed_retry_rule(attempts, hours_ago, skipped):
    st = store()
    when = (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    st.put(Pub.failed_key("hfo-2026102800", J.PREFIX),
           json.dumps({"attempts": attempts, "last_attempt_utc": when}).encode(), "application/json", Pub.POINTER)
    assert J._skip_failed(st, "hfo", "2026102800") is skipped


def test_prune_keeps_four_runs_per_office_and_their_records():
    st = store()
    runs = {("hfo", "20261027" + hh): {"sids": ["51003"], "posted": NOW - timedelta(hours=30 - k)}
            for k, hh in enumerate(("00", "06", "12", "18"))}
    for when in ("20261027" + hh for hh in ("00", "06", "12", "18")):
        assert go(["--office", "hfo", "--run", when], FakeNoaa(runs), st) == 0
    for name in ("hfo-2026102700", "hfo-2026102618", "mtr-2026102600"):
        st.put(Pub.failed_key(name, J.PREFIX), b"{}", "application/json", Pub.POINTER)
    st.put(f"{J.PREFIX}/mtr/2026102600/index.json", b"{}", "application/json", Pub.IMMUTABLE)
    runs[("hfo", "2026102800")] = {"sids": ["51003"], "posted": NOW - timedelta(hours=10)}
    assert go(["--office", "hfo"], FakeNoaa(runs), st) == 0
    ptr = st.get_json(Pub.latest_key(J.PREFIX))
    assert ptr["offices"]["hfo"]["runs"] == ["2026102800", "2026102718", "2026102712", "2026102706"]
    keys = set(st.client.objects)
    assert not any(k.startswith(f"{J.PREFIX}/hfo/2026102700/") for k in keys)
    assert J.index_key("hfo", "2026102706") in keys and f"{J.PREFIX}/mtr/2026102600/index.json" in keys
    assert Pub.failed_key("hfo-2026102700", J.PREFIX) not in keys
    assert Pub.failed_key("hfo-2026102618", J.PREFIX) not in keys
    assert Pub.failed_key("mtr-2026102600", J.PREFIX) in keys         # another office's record: untouched


def test_the_pointer_never_moves_back_and_the_arguments():
    st = store()
    runs = runs_basic()
    runs[("hfo", "2026102712")] = {"sids": ["51003"], "posted": NOW - timedelta(days=1)}
    assert go([], FakeNoaa(runs), st) == 0
    assert go(["--office", "hfo", "--run", "2026102712"], FakeNoaa(runs), st) == 0
    assert st.get_json(Pub.latest_key(J.PREFIX))["offices"]["hfo"]["run"] == "2026102800"
    assert not st.get_json(J.index_key("hfo", "2026102712"))
    for args in (["--run", "2026102800"], ["--office", "xyz"], ["--office", "hfo", "--run", "20261028"],
                 ["--out", "x"]):
        assert go(args, FakeNoaa(runs), st) == 2, args


def test_dry_run_publishes_nothing_and_writes_out(tmp_path):
    assert J.main(["--dry-run", "--out", str(tmp_path)], request=FakeNoaa(runs_basic()), now=NOW,
                  stations=STATIONS) == 0
    doc = json.load(open(tmp_path / "mtr" / "2026102812" / "46026.json"))
    assert doc["station"] == "46026" and doc["office"] == "mtr"
    assert J.main(["--dry-run"], request=FakeNoaa({}), now=NOW, stations=STATIONS) == 3


def test_stale_alarm_and_office_warnings(capsys):
    st = store()
    old = (datetime.now(timezone.utc) - timedelta(hours=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ptr = {"format": "nwps-pointer-v1", "checked_utc": old,
           "offices": {"hfo": {"run": "2026102500", "region": "pr", "published_utc": old, "stations": ["51003"],
                               "runs": ["2026102500"]}}}
    st.put(Pub.latest_key(J.PREFIX), json.dumps(ptr).encode(), "application/json", Pub.POINTER)
    assert go(["--office", "hfo"], FakeNoaa({}), st) == 1                # nothing for 30 h: the alarm
    assert "WARNING: NWPS hfo: no new run since 2026102500" in capsys.readouterr().out
    assert go(["--office", "hfo"], FakeNoaa({}), st) == 3                # at most once every 6 h, then exit 3


def test_station_doc_handles_nodata_hours_and_refuses_an_empty_file():
    text = SPC.replace(SPC[SPC.index("20261028.010000"):SPC.index("20261028.020000")],
                       "20261028.010000                         date and time\nNODATA\n")
    doc = J.station_doc(text, "hfo", "pr", datetime(2026, 10, 28, tzinfo=timezone.utc), "51003", "t")
    assert doc["hs"][1] is None and doc["parts"][1] is None and doc["n"][1] is None and doc["hs"][0] is not None
    nodata = open(os.path.join(FIX, "46240_nodata.txt"), "rb").read()
    with pytest.raises(ValueError):
        J.station_doc(nodata, "mtr", "wr", datetime(2026, 10, 28, tzinfo=timezone.utc), "46240", "t")


def test_workflow_trigger_keepalive_and_tests_paths():
    wf = os.path.join(ROOT, ".github", "workflows")
    text = open(os.path.join(wf, "model-nwps.yml"), encoding="utf-8").read()
    assert "group: model-nwps" in text and "python tools/model_frames/nwps.py" in text
    assert 'cron: "*/10 * * * *"' in text and 'if [ "$rc" -eq 3 ]' in text and "timeout-minutes: 30" in text
    assert text.split("create-args: >-", 1)[1].split("cache-environment", 1)[0] == \
        open(os.path.join(wf, "model-glwu.yml"), encoding="utf-8").read().split("create-args: >-", 1)[1].split(
            "cache-environment", 1)[0], "one cached environment for every publisher"
    uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
    assert uses and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses)
    keep = open(os.path.join(wf, "model-frames-keepalive.yml"), encoding="utf-8").read()
    assert "gh workflow enable model-nwps.yml" in keep
    tests = open(os.path.join(wf, "model-frames-tests.yml"), encoding="utf-8").read()
    assert tests.count('".github/workflows/model-nwps.yml"') == 2 and tests.count('"tests/fixtures/nwps/**"') == 2
    assert tests.count('"nwps_stations.json"') == 2
    cfg = open(os.path.join(ROOT, "tools", "model_frames", "trigger", "wrangler.jsonc"), encoding="utf-8").read()
    assert ('"GH_WORKFLOW": "model-frames.yml,model-points.yml,model-ecmwf.yml,model-glwu.yml,model-gfswinds.yml,'
            'model-nwps.yml"' in cfg)
    assert Pub.NWPS_PREFIX == "nwps/v1" and J.PREFIX == Pub.NWPS_PREFIX


def test_the_real_station_file_loads_by_office():
    by = J.load_stations()
    assert sum(len(v) for v in by.values()) == 112 and list(by["hfo"]) == ["51003"]


def test_the_temporary_dry_run_workflow_is_gone():
    assert not os.path.exists(os.path.join(ROOT, ".github", "workflows", "nwps-dryrun.yml"))
