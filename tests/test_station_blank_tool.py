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
    # a re-scan of a recorded run replaces it
    doc = T.merge(doc, "2026102400", {"blank": [], "absent": [], "failed": [], "scanned": 734})
    assert doc["removed"] == []


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


def test_the_committed_file_and_the_list_agree():
    """station_blank.json names the removed ids; none is in station_list.json; all keep their coordinates and zones."""
    with open(os.path.join(ROOT, "station_blank.json"), encoding="utf-8") as f:
        doc = json.load(f)
    assert doc["removed"] == T.removed_ids(doc["runs"]) and len(doc["removed"]) >= 10
    assert all(re.fullmatch(r"\d{10}", r) for r in doc["runs"]) and len(doc["runs"]) <= T.KEEP_RUNS
    ids = set(json.load(open(os.path.join(ROOT, "station_list.json"), encoding="utf-8")))
    coords = json.load(open(os.path.join(ROOT, "station_coords.json"), encoding="utf-8"))
    for s in doc["removed"]:
        assert s not in ids and s in coords, s
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
    # the page itself renders (the old link opens), the id in the select as an unknown option
    body = client.get("/?station=44062").get_data(as_text=True)
    assert '<option value="44062" data-unknown selected>44062</option>' in body
    # a listed station is untouched by the rule
    monkeypatch.setattr(A, "parse_bull", lambda *a, **k: (None, None, None, None, "UTC", "the parser ran"))
    assert client.get("/api/forecast?station=51201").get_json()["error"] == "the parser ran"
