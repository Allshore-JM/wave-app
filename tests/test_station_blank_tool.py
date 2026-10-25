"""The blank-bulletin scan (tools/build_station_blank.py, plan section 52 C): the rule on real bulletins, the
two-run removal rule, a missing bulletin never counting, --apply, and the server's answer for a removed station."""
import importlib.util
import json
import os
import re
import sys

import pytest

import app as A

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
spec = importlib.util.spec_from_file_location("build_station_blank", os.path.join(ROOT, "tools", "build_station_blank.py"))
T = importlib.util.module_from_spec(spec)
spec.loader.exec_module(T)

REAL = open(os.path.join(HERE, "fixtures", "gfswave_51201_2026100112.bull"), encoding="ascii").read()


def blank_bulletin(station="44062", rows=385):
    """A bulletin as NOAA writes it for a sheltered point: a literal 0 in the Hst cell, every swell cell empty."""
    head = REAL.split("\n +-------+")[0].replace("51201      (21.67N 158.12W)", "%-10s (38.56N  76.41W)" % station)
    sep = " +-------+-----------+" + "-----------------+" * 6
    lines = [head, sep,
             " | day & |  Hst  n x |" + "    Hs   Tp  dir |" * 6,
             " |  hour |  (m)  - - |" + "    (m)  (s) (d) |" * 6, sep]
    for i in range(rows):
        lines.append(" | %2d %2d |       0   |" % (24 + i // 24 % 7, i % 24) + "                 |" * 6)
    return "\n".join(lines) + "\n" + sep + "\n"


def test_the_rule_on_bulletins():
    assert T.classify(REAL) == "data"
    assert T.classify(blank_bulletin()) == "blank"
    # one row with a combined height above zero, or one swell group with its three numbers, is data
    b = blank_bulletin().splitlines()
    i = next(k for k, l in enumerate(b) if l.startswith(" | 24 12 |"))
    assert T.classify("\n".join(b[:i] + [b[i].replace("       0   |", "    0.21   |", 1)] + b[i + 1:])) == "data"
    assert T.classify("\n".join(b[:i] + [b[i].replace("                 |", "   0.30  7.1 320 |", 1)] + b[i + 1:])) == "data"
    # a star-marked value counts as the parser reads it (the star stripped); a cell with two tokens does not
    assert T.classify("\n".join(b[:i] + [b[i].replace("       0   |", "   0.50*   |", 1)] + b[i + 1:])) == "data"
    assert T.classify("\n".join(b[:i] + [b[i].replace("                 |", "   0.30  7.1     |", 1)] + b[i + 1:])) == "blank"
    # no data rows at all (an error page, an empty body): neither
    assert T.classify("") is None and T.classify("<html>404</html>") is None
    assert T.classify(blank_bulletin(rows=0)) is None


def test_removed_only_when_blank_in_both_of_the_two_newest_runs():
    runs = {"2026102406": {"blank": ["44062", "44061", "46088"]}, "2026102412": {"blank": ["44062", "46088", "99999"]}}
    assert T.removed_ids(runs) == ["44062", "46088"]
    assert T.removed_ids({"2026102412": {"blank": ["44062"]}}) == [], "one run decides nothing"
    # the two NEWEST by name, whatever the file's order; an older run's blanks do not count
    runs["2026102300"] = {"blank": ["44061", "44062", "46088", "44039"]}
    assert T.removed_ids(runs) == ["44062", "46088"]
    # a station absent (404) or failed in a run is not blank in it: never removed by that run
    runs["2026102418"] = {"blank": ["44062"], "absent": ["46088"], "failed": ["44039"]}
    assert T.removed_ids(runs) == ["44062"]


def test_merge_keeps_the_newest_runs_and_recomputes():
    doc = {}
    for r in ("2026102300", "2026102306", "2026102312", "2026102318", "2026102400"):
        doc = T.merge(doc, r, {"blank": ["44062", r[-2:]], "absent": [], "failed": [], "scanned": 734})
    assert sorted(doc["runs"]) == ["2026102306", "2026102312", "2026102318", "2026102400"]
    assert doc["removed"] == ["44062"] and doc["rule"] == T.RULE
    # a re-scan of a recorded run replaces it; a removed station NOT read in it (no "back") stays removed (G35 F1)
    doc = T.merge(doc, "2026102400", {"blank": [], "absent": [], "failed": [], "scanned": 734})
    assert doc["removed"] == ["44062"]
    # back (with data) in one of the two newest runs: still removed; in both: back on the list
    doc = T.merge(doc, "2026102400", {"blank": [], "back": ["44062"], "scanned": 734})
    assert doc["removed"] == ["44062"], "one cycle restores nothing"
    doc = T.merge(doc, "2026102406", {"blank": [], "back": ["44062"], "scanned": 734})
    assert doc["removed"] == []


def test_a_removed_station_is_read_every_time_and_stays_removed_until_back_twice():
    """G35 F1: the scan reads the listed ids AND the removed ones, so the next run never forgets them."""
    before = ["44061", "44062", "46088"]
    listed = ["3FYT", "51201", "44061"]                     # 44061 listed again by hand: read once
    assert T.scan_ids(listed, before) == ["3FYT", "51201", "44061", "44062", "46088"]
    r = {"blank": ["44062"], "absent": ["46088"], "failed": [], "scanned": 5}
    assert T.came_back(r, before) == ["44061"], "absent never counts as data"
    assert T.came_back({"blank": [], "failed": ["44062"]}, before) == ["44061", "46088"]
    # the file's next two scans of a still-blank removed station keep it removed (old semantics forgot it)
    doc = {"runs": {}, "removed": ["44062"]}
    for run in ("2026102500", "2026102506"):
        res = {"blank": ["44062"], "absent": [], "failed": [], "scanned": 723}
        res["back"] = T.came_back(res, doc["removed"])
        doc = T.merge(doc, run, res)
        assert doc["removed"] == ["44062"], run
    # a removed station absent (404) in both newest runs stays removed: a missing bulletin restores nothing
    doc = T.merge(doc, "2026102512", {"blank": [], "absent": ["44062"], "back": []})
    doc = T.merge(doc, "2026102518", {"blank": [], "absent": ["44062"], "back": []})
    assert doc["removed"] == ["44062"]
    # fewer than two runs: what was removed stays removed
    assert T.removed_ids({"2026102500": {"blank": []}}, ["44062"]) == ["44062"]


def test_sea_ice_regions_are_kept():
    """Owner (2026-10-24): a point that may be blank because of sea ice stays on the list."""
    for lat, lon in ((72.0, -140.0), (-62.0, -58.0), (50.0, -123.0), (-50.0, 10.0), (39.0, 119.5), (45.5, 142.0),
                     (47.5, -60.0), (46.2, 36.5), (45.5, 502.0)):
        assert T.ice_region(lat, lon), (lat, lon)
    for lat, lon in ((38.56, -76.41), (48.33, -123.16), (41.0, -72.0), (21.67, -158.12), (49.9, -123.0), (36.9, 119.5),
                     ("x", 0), (None, None)):
        assert not T.ice_region(lat, lon), (lat, lon)
    coords = {"Alaska_NS3": {"lat": 72.0, "lon": -140.0}, "44062": {"lat": 38.56, "lon": -76.41}}
    r = T.split_ice({"blank": ["44062", "Alaska_NS3", "nocoord"], "absent": ["x"], "scanned": 3}, coords)
    assert r["blank"] == ["44062", "nocoord"] and r["ice"] == ["Alaska_NS3"] and r["absent"] == ["x"]
    # an ice-blank station in both runs is never removed
    runs = {"2026102406": T.split_ice({"blank": ["44062", "Alaska_NS3"]}, coords),
            "2026102412": T.split_ice({"blank": ["44062", "Alaska_NS3"]}, coords)}
    assert T.removed_ids(runs) == ["44062"]


def test_apply_keeps_the_order_and_drops_only_the_removed():
    assert T.apply_removal(["3FYT", "44061", "44062", "51201", "44063"], ["44062", "44061", "nope"]) == ["3FYT", "51201", "44063"]


def test_apply_restores_a_comeback_at_its_old_place():
    order = ["3FYT", "13130", "44039", "44040", "44058", "51201", "Alaska_NS3"]   # station_coords.json's order
    listed = ["3FYT", "13130", "44058", "51201", "Alaska_NS3"]
    assert T.apply_removal(listed, ["44040"], ["44039"], order) == ["3FYT", "13130", "44039", "44058", "51201", "Alaska_NS3"]
    assert T.apply_removal(listed, [], ["44040", "44039"], order) == [
        "3FYT", "13130", "44039", "44040", "44058", "51201", "Alaska_NS3"]
    assert T.apply_removal(listed, [], ["zzz"], order)[-1] == "zzz", "an id without a place goes last"
    assert T.apply_removal(listed, ["44039"], ["44039"], order) == listed, "still removed wins"
    assert T.apply_removal(listed, [], ["51201"], order) == listed, "never twice"
    # the real files: the list is station_coords.json's order filtered, so a comeback lands where it was
    coords = list(json.load(open(os.path.join(ROOT, "station_coords.json"), encoding="utf-8")))
    ids = json.load(open(os.path.join(ROOT, "station_list.json"), encoding="utf-8"))
    pos = {s: i for i, s in enumerate(coords)}
    assert [pos[s] for s in ids] == sorted(pos[s] for s in ids), "the list keeps the coordinates' order"


def _tool_dir(tmp_path, listed, removed, coords_ids):
    """A scratch copy of the tool's three files: the list, the coordinates (the list's order) and the blank file."""
    (tmp_path / "station_list.json").write_text(json.dumps(listed), encoding="utf-8")
    (tmp_path / "station_coords.json").write_text(
        json.dumps({s: {"lat": 38.5, "lon": -76.4} for s in coords_ids}), encoding="utf-8")
    (tmp_path / "station_blank.json").write_text(json.dumps({"rule": "", "runs": {}, "removed": removed}), encoding="utf-8")
    return tmp_path


def _run_main(monkeypatch, d, run, blank, apply):
    """The tool's real main() with the network faked: the tar answers 'blank' for `blank`, data for the rest."""
    asked = []

    def fake_tar(http, r, ids):
        asked[:] = list(ids)
        return {"blank": sorted(s for s in ids if s in blank), "absent": [], "failed": [], "scanned": len(ids),
                "source": "fake"}

    monkeypatch.setattr(T, "ROOT", str(d))
    monkeypatch.setattr(T, "LIST_PATH", str(d / "station_list.json"))
    monkeypatch.setattr(T, "OUT_PATH", str(d / "station_blank.json"))
    monkeypatch.setattr(T, "tar_scan", fake_tar)
    monkeypatch.setattr(T, "newest_run", lambda http: run)
    monkeypatch.setattr(T, "_session", lambda: None)
    assert T.main(["--run", run] + (["--apply"] if apply else [])) == 0
    doc = json.loads((d / "station_blank.json").read_text(encoding="utf-8"))
    lst = json.loads((d / "station_list.json").read_text(encoding="utf-8"))
    return doc, lst, asked


@pytest.mark.parametrize("steps", [
    # (run, --apply?) ; 44062 has data again from the first run on and stays so (re-check R1-1's four orders)
    [("2026102418", False), ("2026102500", True), ("2026102506", False), ("2026102512", True)],
    [("2026102418", True), ("2026102500", False), ("2026102506", True), ("2026102512", False), ("2026102518", True)],
    [("2026102418", True), ("2026102500", True), ("2026102506", True)],
    [("2026102418", False), ("2026102500", False), ("2026102506", False), ("2026102512", True)],
], ids=["scan-then-apply", "apply-then-scan", "every-run-apply", "scans-then-one-apply"])
def test_a_comeback_is_never_lost_whatever_the_order_of_scans_and_apply(tmp_path, monkeypatch, steps):
    """Re-check R1-1: a removed station back with data in both newest runs waits in "restore" (scanned every run)
    until --apply puts it back at its old place; before the fix a plain scan completing the comeback lost it."""
    order = ["3FYT", "44061", "44062", "51201"]
    d = _tool_dir(tmp_path, ["3FYT", "51201"], ["44061", "44062"], order)
    for run, apply in steps:
        doc, lst, asked = _run_main(monkeypatch, d, run, {"44061"}, apply)
        assert "44062" in asked, (run, "every run reads it")
        assert ("44062" in doc["removed"]) + ("44062" in doc["restore"]) + ("44062" in lst) == 1, (run, doc, lst)
        assert doc["removed"] == ["44061"] or run == steps[0][0], "44061 stays blank: stays removed"
        assert "44061" not in lst and "44061" in asked
    assert lst == ["3FYT", "44062", "51201"], "back at its old place"
    assert doc["restore"] == [] and doc["removed"] == ["44061"]
    # and later scans keep it listed (it is a listed station now) without a trace in "restore"
    doc, lst, _ = _run_main(monkeypatch, d, "2026102600", {"44061"}, False)
    assert lst == ["3FYT", "44062", "51201"] and doc["restore"] == []


def test_a_station_waiting_to_come_back_that_goes_blank_again_is_removed_again(tmp_path, monkeypatch):
    d = _tool_dir(tmp_path, ["51201"], ["44062"], ["44062", "51201"])
    for run in ("2026102418", "2026102500"):
        doc, lst, _ = _run_main(monkeypatch, d, run, set(), False)
    assert doc["restore"] == ["44062"] and doc["removed"] == [] and lst == ["51201"]
    doc, lst, asked = _run_main(monkeypatch, d, "2026102506", {"44062"}, False)
    assert "44062" in asked and doc["restore"] == ["44062"], "one blank run: still waiting"
    doc, lst, _ = _run_main(monkeypatch, d, "2026102512", {"44062"}, True)
    assert doc["removed"] == ["44062"] and doc["restore"] == [] and lst == ["51201"], "blank in both: removed again"


def test_a_station_put_back_by_hand_leaves_restore(tmp_path, monkeypatch):
    d = _tool_dir(tmp_path, ["51201"], ["44062"], ["44062", "51201"])
    for run in ("2026102418", "2026102500"):
        doc, _, _ = _run_main(monkeypatch, d, run, set(), False)
    assert doc["restore"] == ["44062"]
    (d / "station_list.json").write_text(json.dumps(["44062", "51201"]), encoding="utf-8")
    doc, lst, _ = _run_main(monkeypatch, d, "2026102506", set(), False)
    assert doc["restore"] == [] and lst == ["44062", "51201"]


def test_the_committed_file_and_the_list_agree():
    """station_blank.json names the removed ids; none is in station_list.json; all keep their coordinates and zones."""
    with open(os.path.join(ROOT, "station_blank.json"), encoding="utf-8") as f:
        doc = json.load(f)
    assert doc["removed"] == T.removed_ids(doc["runs"], doc["removed"]) and len(doc["removed"]) >= 10
    # every removed id was READ in both of the two newest runs (G35 F1): blank, back, ice, absent or failed there
    for run in sorted(doc["runs"])[-2:]:
        r = doc["runs"][run]
        seen = set().union(*(r.get(k, []) for k in ("blank", "back", "ice", "absent", "failed")))
        assert set(doc["removed"]) <= seen, run
    assert all(re.fullmatch(r"\d{10}", r) for r in doc["runs"]) and len(doc["runs"]) <= T.KEEP_RUNS
    ids = set(json.load(open(os.path.join(ROOT, "station_list.json"), encoding="utf-8")))
    coords = json.load(open(os.path.join(ROOT, "station_coords.json"), encoding="utf-8"))
    for s in doc["removed"]:
        assert s not in ids and s in coords, s
    # every id a recorded run named blank or back is accounted for: listed, removed, or waiting in "restore"
    # (re-check R1-1: never on neither list)
    restore = set(doc.get("restore", []))
    assert restore.isdisjoint(ids) and restore.isdisjoint(doc["removed"]) and restore <= set(coords)
    for run, r in doc["runs"].items():
        for s in set(r.get("blank", [])) | set(r.get("back", [])):
            assert s in ids or s in doc["removed"] or s in restore, (run, s)
    assert {"44061", "44062", "44039", "44087"} <= set(doc["removed"])
    assert "Alaska_NS3" in ids and "Alaska_NS3" not in doc["removed"], "blank under sea ice: kept (owner)"
    assert all("Alaska_NS3" in r.get("ice", []) for r in doc["runs"].values())
    assert A.BLANK_STATIONS == frozenset(doc["removed"])


def test_the_server_reads_the_removed_ids_only(tmp_path):
    f = tmp_path / "b.json"
    f.write_text(json.dumps({"runs": {"2026102412": {"blank": ["44062", "46088"]}}, "removed": ["44062", 7]}), encoding="utf-8")
    assert A._load_blank_stations(str(f)) == frozenset(["44062"]), "removed, never a run's own blank list; strings only"
    assert A._load_blank_stations(str(tmp_path / "missing.json")) == frozenset()


@pytest.fixture
def client():
    A.app.config["TESTING"] = True
    with A.app.test_client() as c:
        yield c


def test_a_removed_station_answers_the_note_without_a_table(client, monkeypatch):
    calls = []
    monkeypatch.setattr(A, "parse_bull", lambda *a, **k: calls.append(a) or (None, None, None, None, "UTC", "never"))
    r = client.get("/api/forecast?station=44062")
    assert r.status_code == 200
    d = r.get_json()
    assert d["error"] == A.BLANK_STATION_NOTE and d["final"] is True and d["reason"] == "sheltered"
    assert d["table_html"] is None and d["graph_data"] is None and d["model"] == "GFS"
    assert calls == [], "no bulletin asked for"
    pos = A.load_station_coords()["44062"]                 # where it was: the page offers its live buoy (G35 B-8)
    assert (d["lat"], d["lon"]) == (pos["lat"], pos["lon"])
    # the page itself renders (the old link opens), the id in the select as an unknown option
    body = client.get("/?station=44062").get_data(as_text=True)
    assert '<option value="44062" data-unknown selected>44062</option>' in body
    # a listed station is untouched by the rule
    monkeypatch.setattr(A, "parse_bull", lambda *a, **k: (None, None, None, None, "UTC", "the parser ran"))
    assert client.get("/api/forecast?station=51201").get_json()["error"] == "the parser ran"
