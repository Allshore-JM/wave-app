"""The RRFS job (plan section 99): idx selection, ranged reads, decode + products against an independent eccodes read
of real NOAA records (tests/fixtures/rrfs, captured by tests/capture_rrfs_fixtures.py), the precipitation rules, the
publish order, the pointer's keep rule, prune, and the exit codes."""
import io
import json
import math
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, ROOT)
import publish as P  # noqa: E402
import rrfs as J  # noqa: E402

ec = pytest.importorskip("eccodes")

FIX = os.path.join(ROOT, "tests", "fixtures", "rrfs")
RUN = datetime(2026, 11, 17, 12, tzinfo=timezone.utc)
CYC = datetime(2026, 11, 17, 13, tzinfo=timezone.utc)
VAR = {"u": "UGRD", "v": "VGRD", "gust": "GUST", "tmp": "TMP", "dpt": "DPT", "crain": "CRAIN", "csnow": "CSNOW",
       "cfrzr": "CFRZR", "cicep": "CICEP", "tcdc": "TCDC", "apcp": "APCP", "apcp_total": "APCP", "ltng": "LTNG"}
COORDS = {"51201": (21.671, -158.118), "51202": (21.417, -157.678), "OUT": (28.18, -15.82)}


def _read(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return f.read()


class FakeNoaa:
    """Serves the fixture .idx files and ranged reads made of the fixture records at their .idx offsets (HI step 6 of
    the run 2026111712 and of the hourly cycle 2026111713). Anything else answers 404."""

    def __init__(self):
        self.calls = []
        self.files = {}
        for cyc, prefix in ((RUN, "hi_f006"), (CYC, "hi_subh_t13z_f006")):
            url = f"{J.S3}/{J.file_key(cyc, 'hi', 6)}"
            idx = _read(f"{prefix}.idx")
            rows = J.parse_idx(idx.decode("ascii"))
            recs = J.select_records(rows, cyc, 6)
            pieces = {a: _read(f"{prefix}_{VAR[name]}.grib2") for name, (a, b) in recs.items()}
            self.files[url] = (idx, pieces)

    def __call__(self, url, rng=None):
        self.calls.append((url, rng))
        if url.endswith(".idx") and url[:-4] in self.files:
            return self.files[url[:-4]][0]
        if url in self.files and rng:
            pieces = self.files[url][1]
            out, pos = b"", rng[0]
            while pos <= rng[1]:
                if pos not in pieces:
                    raise AssertionError(f"range {rng} not made of whole records (at {pos})")
                out += pieces[pos]
                pos += len(pieces[pos])
            assert len(out) == rng[1] - rng[0] + 1, (rng, len(out))
            return out
        raise J.NotReady(f"404 {url}")


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
        page, nxt = keys[start:start + 3], start + 3
        return {"Contents": [{"Key": k} for k in page], "IsTruncated": nxt < len(keys),
                "NextContinuationToken": str(nxt) if nxt < len(keys) else None}

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)
            self.log.append(("del", o["Key"]))
        return {}


@pytest.fixture(autouse=True)
def _one_step(monkeypatch):
    """Sources of one step (6): the fixtures hold step 6 only."""
    monkeypatch.setattr(J, "RUN_STEPS", [6])
    monkeypatch.setattr(J, "CYCLE_STEPS", [6])
    monkeypatch.setattr(J.time, "sleep", lambda s: None)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)


def store():
    return P.Store(FakeClient(), "bucket")


def ecc(prefix, var):
    g = ec.codes_new_from_message(_read(f"{prefix}_{var}.grib2"))
    try:
        return ec.codes_get_double_array(g, "values").reshape(ec.codes_get(g, "Ny"), ec.codes_get(g, "Nx"))
    finally:
        ec.codes_release(g)


def head_hi_only(url):
    """complete(): every grid's last-step idx exists (the tests then build Hawaii only)."""
    return True


# ------------------------------------------------------------------------------------------------- idx and records

def test_wanted_records_per_kind_and_step():
    run6 = dict(J.wanted(RUN, 6))
    assert run6["apcp"] == ("APCP", "surface", "5-6 hour acc fcst")
    assert run6["ltng"] == ("LTNG", "entire atmosphere", "5-6 hour max fcst")
    assert run6["tcdc"][2] == "6 hour fcst" and "apcp_total" not in run6
    run0 = dict(J.wanted(RUN, 0))
    assert run0["u"][2] == "anl" and "apcp" not in run0 and "ltng" not in run0
    cyc6 = dict(J.wanted(CYC, 6))
    assert cyc6["apcp_total"] == ("APCP", "surface", "0-6 hour acc fcst")
    assert "tcdc" not in cyc6 and "ltng" not in cyc6 and "apcp" not in cyc6


def test_select_records_on_the_real_idx_files():
    for cyc, name in ((RUN, "hi_f006.idx"), (CYC, "hi_subh_t13z_f006.idx")):
        rows = J.parse_idx(_read(name).decode("ascii"))
        recs = J.select_records(rows, cyc, 6)
        assert set(recs) == {n for n, _ in J.wanted(cyc, 6)}
        assert all(b is not None and b >= a for a, b in recs.values())


def test_parse_idx_crlf_and_last_record():
    text = "1:0:d=2026111712:A:surface:6 hour fcst:\r\n2:100:d=2026111712:B:surface:6 hour fcst:\r\n\r\n"
    rows = J.parse_idx(text)
    assert rows[0][1:3] == (0, 99) and rows[1][2] is None
    assert J.parse_idx(text, 250)[1][2] == 249
    with pytest.raises(ValueError):
        J.parse_idx("1:0:d=x:A\n")


def test_select_records_refuses_missing_twice_and_another_cycle():
    rows = J.parse_idx(_read("hi_f006.idx").decode("ascii"))
    with pytest.raises(ValueError, match="0 records"):
        J.select_records([r for r in rows if r[4] != "GUST"], RUN, 6)
    gust = [r for r in rows if r[4] == "GUST" and r[6] == "6 hour fcst"][0]
    with pytest.raises(ValueError, match="2 records"):
        J.select_records(rows + [gust], RUN, 6)
    with pytest.raises(ValueError, match="d=2026111712"):
        J.select_records(rows, datetime(2026, 11, 17, 18, tzinfo=timezone.utc), 6)


def test_ranges_merge_only_adjacent_records():
    recs = {"u": (100, 199), "v": (200, 299), "g": (400, 449), "t": (450, 499), "x": (1000, 1009)}
    assert J.ranges_of(recs) == [(100, 299, ["u", "v"]), (400, 499, ["g", "t"]), (1000, 1009, ["x"])]


def test_split_grib_exact_messages():
    a, b = _read("hi_f006_UGRD.grib2"), _read("hi_f006_VGRD.grib2")
    assert J.split_grib(a + b, ["u", "v"]) == {"u": a, "v": b}
    with pytest.raises(ValueError):
        J.split_grib(a + b + b"x", ["u", "v"])
    with pytest.raises(ValueError):
        J.split_grib(a[:-1] + b, ["u", "v"])
    with pytest.raises(ValueError):
        J.split_grib(b"XXXX" + a[4:], ["u"])


def test_file_keys():
    assert J.file_key(RUN, "conus", 84) == "rrfs.20261117/12/rrfs.t12z.2dfld.3km.f084.conus.grib2"
    assert J.file_key(CYC, "hi", 18) == "rrfs.20261117/13/rrfs.t13z.2dfld.2p5km.subh.f018.hi.grib2"


def test_complete_and_newest_complete(monkeypatch):
    monkeypatch.setattr(J, "RUN_STEPS", list(range(85)))
    monkeypatch.setattr(J, "CYCLE_STEPS", list(range(1, 19)))
    seen = []

    def head(url):
        seen.append(url)
        return "t13z" in url or "t12z" in url

    now = datetime(2026, 11, 17, 16, 40, tzinfo=timezone.utc)
    assert J.newest_complete(now, "cycle", head) == CYC           # 16, 15, 14 missing; 13 there
    assert J.newest_complete(now, "run", head) == RUN
    assert any("f018.conus.grib2.idx" in u for u in seen) and any("f084.ak.grib2.idx" in u for u in seen)
    assert J.newest_complete(now, "cycle", lambda u: False) is None


# ----------------------------------------------------------------------------------------------- products vs eccodes

def _build(cyc, tmp_path):
    files, docs = J.build(cyc, str(tmp_path), COORDS, deadline=1e18, get=FakeNoaa(), log=lambda s: None,
                          grids=("hi",))
    return files, docs


def test_run_products_equal_an_independent_eccodes_read(tmp_path):
    files, docs = _build(RUN, tmp_path)
    cyc = "2026111712"
    doc = docs[J.cells_index_key(cyc, "hi")]
    assert doc["record_bytes"] == 14 and doc["nx"] == 321 and doc["ny"] == 225 and doc["kind"] == "run"
    raw = np.fromfile(files[J.cells_key(cyc, "hi")], dtype=J.CELL_DTYPE).reshape(225 * 321, 1)
    assert J._sha256(files[J.cells_key(cyc, "hi")]) == doc["sha256"]
    U, V, GU, T, D = (ecc("hi_f006", v) for v in ("UGRD", "VGRD", "GUST", "TMP", "DPT"))
    AP, TC, CR, LT = (ecc("hi_f006", v) for v in ("APCP", "TCDC", "CRAIN", "LTNG"))
    rng = np.random.default_rng(7)
    for _ in range(500):
        j, i = int(rng.integers(0, 225)), int(rng.integers(0, 321))
        r = raw[j * 321 + i, 0]
        s = math.hypot(U[j, i], V[j, i])
        assert abs(r["spd"] / 100 - s) <= 0.0051
        if s > 0.5:
            d = (math.degrees(math.atan2(-U[j, i], -V[j, i])) + 360) % 360       # Mercator: no rotation
            assert abs((r["dir"] / 10 - d + 180) % 360 - 180) <= 0.051
        assert abs(r["gust"] / 100 - GU[j, i]) <= 0.0051
        assert abs(r["tmp"] / 10 - T[j, i]) <= 0.051 and abs(r["dpt"] / 10 - D[j, i]) <= 0.051
        assert abs(r["apcp"] / 100 - AP[j, i]) <= 0.0051
        assert r["cloud"] == round(TC[j, i])
        want = 0 if AP[j, i] < 0.1 else 5 if LT[j, i] > 0 else 1 if CR[j, i] >= 0.5 else r["type"]
        assert r["type"] == want


def test_station_product_equals_the_cell(tmp_path):
    files, docs = _build(RUN, tmp_path)
    st = docs[J.stations_index_key("2026111712")]
    assert set(st["stations"]) == {"51201", "51202"}                # OUT is outside every grid
    slot, grid, j, i = st["stations"]["51201"]
    assert grid == "hi" and (j, i) == J.G.cell("hi", *COORDS["51201"])
    body = np.fromfile(files[J.stations_key("2026111712")], dtype="<u2")
    rec = body[slot * 3:(slot + 1) * 3]
    cell = np.fromfile(files[J.cells_key("2026111712", "hi")], dtype=J.CELL_DTYPE)[j * 321 + i]
    assert list(rec) == [cell["spd"], cell["dir"], cell["gust"]]


def test_hourly_cycle_product_has_no_cloud_and_the_total_as_its_first_hour(tmp_path):
    files, docs = _build(CYC, tmp_path)
    raw = np.fromfile(files[J.cells_key("2026111713", "hi")], dtype=J.CELL_DTYPE)
    assert (raw["cloud"] == J.MISSING8).all()
    TOT = ecc("hi_subh_t13z_f006", "APCP")
    # one step: no earlier total in this build, so the hour's amount is the run total itself (rule tested below)
    assert np.allclose(raw["apcp"] / 100, np.round(TOT.ravel() * 100) / 100, atol=0.0051)


def test_hourly_amount_is_the_difference_of_totals_clipped_at_zero():
    shape = (2, 2)
    base = {k: np.zeros(shape) for k in ("u", "v", "gust", "tmp", "dpt", "crain", "csnow", "cfrzr", "cicep")}
    rec1, t1 = J.step_record(dict(base, apcp_total=np.array([[1.0, 2.0], [0.0, 5.0]])), "hi", np.zeros(shape))
    rec2, t2 = J.step_record(dict(base, apcp_total=np.array([[1.5, 1.9], [0.0, 9.0]])), "hi", np.zeros(shape), t1)
    assert list(rec1["apcp"]) == [100, 200, 0, 500]
    assert list(rec2["apcp"]) == [50, 0, 0, 400]                   # 2.0 -> 1.9 (a model wobble) clips to 0


def test_f000_has_no_hour_behind_it():
    shape = (1, 2)
    f = {k: np.zeros(shape) for k in ("u", "v", "gust", "tmp", "dpt", "crain", "csnow", "cfrzr", "cicep", "tcdc")}
    rec, _ = J.step_record(f, "hi", np.zeros(shape))
    assert (rec["apcp"] == J.MISSING).all() and (rec["type"] == J.MISSING8).all()


@pytest.mark.parametrize("flags, ltng, want", [
    ((1, 0, 0, 0), 0, J.TYPE_RAIN), ((0, 1, 0, 0), 0, J.TYPE_SNOW), ((1, 1, 0, 0), 0, J.TYPE_MIX),
    ((1, 1, 1, 0), 0, J.TYPE_ICE), ((0, 0, 0, 1), 0, J.TYPE_ICE), ((1, 0, 0, 0), 3.0, J.TYPE_THUNDER),
    ((0, 0, 0, 0), 0, J.TYPE_RAIN)])
def test_precip_type_order(flags, ltng, want):
    a = lambda x: np.array([float(x)])                             # noqa: E731
    assert J.precip_type(a(1.0), *(a(x) for x in flags), a(ltng))[0] == want
    assert J.precip_type(a(0.05), *(a(x) for x in flags), a(ltng))[0] == J.TYPE_NONE
    assert J.precip_type(a(np.nan), *(a(x) for x in flags), a(ltng))[0] == J.MISSING8


def test_rotation_is_applied_for_lambert():
    f = {k: np.zeros((1, 1)) for k in ("gust", "tmp", "dpt", "crain", "csnow", "cfrzr", "cicep")}
    f.update(u=np.array([[0.0]]), v=np.array([[10.0]]))              # grid north
    rec, _ = J.step_record(f, "conus", np.array([[-124.53]]))
    a = math.degrees(float(J.G.rotation_angle("conus", -124.53)))   # about -16.8: the wind turns to come from ~163
    assert rec["dir"][0] / 10 == pytest.approx((180 + a) % 360, abs=0.06)


def test_geometry_change_fails_the_decode():
    """Puerto Rico's record read as Hawaii's: the geometry pin refuses it (NOAA changing a grid fails loud)."""
    with pytest.raises(J.G.GeometryError):
        J.decode(_read("pr_f006_UGRD.grib2"), "hi")
    assert J.decode(_read("hi_f006_UGRD.grib2"), "hi").shape == (225, 321)


# ----------------------------------------------------------------------------------- publish, pointer, prune, main

def test_keep_sources_rule():
    srcs = [{"cycle": f"20261117{h:02d}", "kind": "run" if h % 6 == 0 else "cycle", "stations": "s",
             "cells": {"hi": "c"}} for h in range(0, 24)]
    kept = J.keep_sources(srcs)
    assert [s["cycle"][-2:] for s in kept] == ["23", "22", "21", "20", "19", "18", "17", "16", "15", "12"]
    assert [s["cycle"][-2:] for s in kept if s["cells"]] == ["23", "22", "21", "18", "12"]


def test_main_publishes_cycle_and_run_pointer_last_and_prunes(tmp_path, monkeypatch):
    st = store()

    def fake_build(c, work, coords, deadline, get=None, log=None, grids=None):
        return _orig_build(c, work, COORDS, deadline, get=FakeNoaa(), log=lambda s: None, grids=("hi",))

    monkeypatch.setattr(J, "build", fake_build)
    now = datetime(2026, 11, 17, 16, 0, tzinfo=timezone.utc)
    head = lambda url: ("t13z" in url) or ("t12z" in url)                # noqa: E731
    rc = J.main([], head=head, store=st, now=now, coords=COORDS, log=lambda s: None)
    assert rc == 0
    log = [k for op, k in st.client.log if op == "put"]
    assert log[-1] == "rrfs/v1/latest.json"
    ptr = st.get_json("rrfs/v1/latest.json")
    assert [s["cycle"] for s in ptr["sources"]] == ["2026111713", "2026111712"]
    assert ptr["sources"][0]["kind"] == "cycle" and ptr["sources"][1]["kind"] == "run"
    assert ptr["run"] == "2026111713"
    # each source: its bins before its documents, its documents before the pointer that names it
    first_ptr = log.index("rrfs/v1/latest.json")
    assert log.index("rrfs/v1/2026111713/hi.bin") < log.index("rrfs/v1/2026111713/hi.json") < first_ptr
    # again: nothing new
    assert J.main([], head=head, store=st, now=now, coords=COORDS, log=lambda s: None) == 0
    # a stray older cycle not named by the pointer is pruned at the next publish
    st.put("rrfs/v1/2026111701/stations.bin", b"x", "application/octet-stream", P.IMMUTABLE)
    J.prune(st, ptr)
    assert "rrfs/v1/2026111701/stations.bin" not in st.client.objects
    assert "rrfs/v1/2026111712/hi.bin" in st.client.objects


_orig_build = J.build


def test_main_not_served_is_exit_3_with_a_record(monkeypatch):
    st = store()
    now = datetime(2026, 11, 17, 16, 0, tzinfo=timezone.utc)
    head = lambda url: "t13z" in url                                    # noqa: E731
    monkeypatch.setattr(J, "build", lambda *a, **k: (_ for _ in ()).throw(J.NotReady("404 x")))
    st.put("rrfs/v1/latest.json", json.dumps({"published_utc": datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"), "sources": []}).encode(), "application/json", P.POINTER)
    assert J.main([], head=head, store=st, now=now, coords=COORDS, log=lambda s: None) == 3
    assert st.get_json("rrfs/v1/notready/2026111713.json")["count"] == 1


def test_main_bad_data_is_exit_1_with_a_failure_record_then_waits(monkeypatch):
    st = store()
    now = datetime(2026, 11, 17, 16, 0, tzinfo=timezone.utc)
    head = lambda url: "t13z" in url                                    # noqa: E731
    monkeypatch.setattr(J, "build", lambda *a, **k: (_ for _ in ()).throw(ValueError("layout")))
    st.put("rrfs/v1/latest.json", json.dumps({"published_utc": datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"), "sources": []}).encode(), "application/json", P.POINTER)
    assert J.main([], head=head, store=st, now=now, coords=COORDS, log=lambda s: None) == 1
    assert st.get_json("rrfs/v1/failed/2026111713.json")["attempts"] == 1
    assert J.main([], head=head, store=st, now=now, coords=COORDS, log=lambda s: None) == 3   # waits, no new attempt
    assert st.get_json("rrfs/v1/failed/2026111713.json")["attempts"] == 1


def test_main_stale_alarm_when_nothing_new_for_hours():
    st = store()
    st.put("rrfs/v1/latest.json", json.dumps({"published_utc": "2026-11-16T00:00:00Z", "run": "2026111600",
                                              "sources": [{"cycle": "2026111713", "kind": "cycle"},
                                                          {"cycle": "2026111712", "kind": "run"}]}).encode(),
           "application/json", P.POINTER)
    now = datetime(2026, 11, 17, 16, 0, tzinfo=timezone.utc)
    head = lambda url: ("t13z" in url) or ("t12z" in url)                # noqa: E731
    assert J.main([], head=head, store=st, now=now, coords=COORDS, log=lambda s: None) == 1
    assert J.main([], head=head, store=st, now=now, coords=COORDS, log=lambda s: None) == 0   # once every 6 h


def test_dry_run_flags():
    assert J.main(["--out", "x"], store=store(), log=lambda s: None) == 2
    assert J.main(["--dry-run", "--grids", "mars"], log=lambda s: None) == 2
    assert J.main(["--dry-run", "--cycle", "2026"], log=lambda s: None) == 2
