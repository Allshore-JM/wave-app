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

try:
    import eccodes as ec                                         # the laptop and the publisher env have it; the tests CI not
except ImportError:                                              # (pragma: no cover)
    ec = None
needs_ec = pytest.mark.skipif(ec is None, reason="eccodes not installed (the model-frames-tests CI installs none)")

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
    WARMED.clear()                                                  # no network: the warm-up reads are recorded
    monkeypatch.setattr(J, "warm_cells", lambda keys, get=None, log=None: WARMED.append(list(keys)) or {})


WARMED = []


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


@needs_ec
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


@needs_ec
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


@needs_ec
def test_run_products_equal_an_independent_eccodes_read(tmp_path):
    files, docs = _build(RUN, tmp_path)
    cyc = "2026111712"
    doc = docs[J.cells_index_key(cyc, "hi")]
    assert doc["record_bytes"] == 14 and doc["nx"] == 321 and doc["ny"] == 225 and doc["kind"] == "run"
    raw = np.fromfile(files[J.cells_key(cyc, "hi")], dtype=J.CELL_DTYPE).reshape(225 * 321, 1)
    assert J._sha256(files[J.cells_key(cyc, "hi")]) == doc["sha256"]
    U, V, GU, T, D = (ecc("hi_f006", v) for v in ("UGRD", "VGRD", "GUST", "TMP", "DPT"))
    AP, TC, CR, LT = (ecc("hi_f006", v) for v in ("APCP", "TCDC", "CRAIN", "LTNG"))
    SN, FR, IP = (ecc("hi_f006", v) for v in ("CSNOW", "CFRZR", "CICEP"))
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
        if AP[j, i] < 0.1:
            want = 0
        elif LT[j, i] > 0:
            want = 5
        elif FR[j, i] >= 0.5 or IP[j, i] >= 0.5:
            want = 4
        elif CR[j, i] >= 0.5 and SN[j, i] >= 0.5:
            want = 3
        elif SN[j, i] >= 0.5:
            want = 2
        else:
            want = 1
        assert r["type"] == want


@needs_ec
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


@needs_ec
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


@needs_ec
def test_geometry_change_fails_the_decode():
    """Puerto Rico's record read as Hawaii's: the geometry pin refuses it (NOAA changing a grid fails loud)."""
    with pytest.raises(J.G.GeometryError):
        J.decode(_read("pr_f006_UGRD.grib2"), "hi")
    assert J.decode(_read("hi_f006_UGRD.grib2"), "hi").shape == (225, 321)


# ------------------------------------------------------------------------------- identity, calm, multi-step wiring

@needs_ec
def test_every_fixture_record_passes_its_identity_check_and_a_swap_fails():
    for cyc, prefix in ((RUN, "hi_f006"), (CYC, "hi_subh_t13z_f006")):
        for name, _ in J.wanted(cyc, 6):
            J.decode(_read(f"{prefix}_{VAR[name]}.grib2"), "hi", (name, cyc, 6))
    with pytest.raises(ValueError, match="record tmp"):                 # DPT where TMP should be (G66a P2-2)
        J.decode(_read("hi_f006_DPT.grib2"), "hi", ("tmp", RUN, 6))
    with pytest.raises(ValueError):                                     # the run total read as the hour's amount
        J.decode(_read("hi_subh_t13z_f006_APCP.grib2"), "hi", ("apcp", CYC, 6))
    with pytest.raises(ValueError):                                     # another step
        J.decode(_read("hi_f006_UGRD.grib2"), "hi", ("u", RUN, 7))
    with pytest.raises(ValueError):                                     # another cycle
        J.decode(_read("hi_f006_UGRD.grib2"), "hi", ("u", datetime(2026, 11, 17, 18, tzinfo=timezone.utc), 6))


def test_a_calm_has_no_direction():
    f = {k: np.zeros((1, 2)) for k in ("gust", "tmp", "dpt", "crain", "csnow", "cfrzr", "cicep")}
    f.update(u=np.array([[0.0, 0.0]]), v=np.array([[0.0, -3.0]]))
    rec, _ = J.step_record(f, "hi", np.zeros((1, 2)))
    assert rec["dir"][0] == J.MISSING and rec["spd"][0] == 0
    assert rec["dir"][1] == 0 and rec["spd"][1] == 300                  # 3 m/s from the north


def _fake_fields(monkeypatch, order=None):
    """fetch_step / decode / grid_lonlat replaced: every field of step s holds known values (no eccodes, no network):
    tmp = 250 + s K; the hourly cycle's precipitation total = s * (s + 1) / 2 mm (so the hour's amount is s mm); the
    run's hourly amount = s / 10 mm. `order` records the steps in the order they are decoded."""
    g = J.G.GRIDS["hi"]
    shape = (g["Ny"], g["Nx"])
    monkeypatch.setattr(J, "grid_lonlat", lambda msg, grid: (np.zeros(shape), np.zeros(shape)))
    monkeypatch.setattr(J, "fetch_step", lambda c, grid, step, get=None: {n: (n, step) for n, _ in J.wanted(c, step)})

    def decode(token, grid, expect=None):
        name, step = token
        assert expect is not None and expect[0] == name and expect[2] == step
        if order is not None and name == "u":
            order.append(step)
        val = {"tmp": 250.0 + step, "apcp_total": step * (step + 1) / 2, "apcp": step / 10, "u": 1.0, "v": 0.0,
               "gust": 2.0, "dpt": 240.0, "tcdc": 50.0}.get(name, 0.0)
        return np.full(shape, val)

    monkeypatch.setattr(J, "decode", decode)


def test_build_grid_wiring_over_several_fetch_windows(tmp_path, monkeypatch):
    """20 hourly-cycle steps with workers=2 (a window of 4): every column holds its own step, in order, and the hour's
    precipitation is each total minus the one before (G66a P3: the one-step fixtures could not see this)."""
    monkeypatch.setattr(J, "CYCLE_STEPS", list(range(1, 21)))
    order = []
    _fake_fields(monkeypatch, order)
    path, st = J.build_grid(CYC, "hi", {"51201": (100, 100)}, str(tmp_path), 1e18, log=lambda s: None, workers=2)
    assert order == list(range(1, 21))
    arr = np.fromfile(path, dtype=J.CELL_DTYPE).reshape(-1, 20)
    assert list(arr[12345]["tmp"]) == [round((250 + s) * 10) for s in range(1, 21)]
    assert list(arr[12345]["apcp"]) == [s * 100 for s in range(1, 21)]
    assert (arr["cloud"] == J.MISSING8).all()
    assert list(st["51201"]["spd"]) == [100] * 20 and list(st["51201"]["dir"]) == [2700] * 20


def test_build_grid_run_steps_from_f000(tmp_path, monkeypatch):
    monkeypatch.setattr(J, "RUN_STEPS", list(range(0, 21)))
    _fake_fields(monkeypatch)
    path, _ = J.build_grid(RUN, "hi", {}, str(tmp_path), 1e18, log=lambda s: None, workers=2)
    arr = np.fromfile(path, dtype=J.CELL_DTYPE).reshape(-1, 21)
    assert list(arr[7]["tmp"]) == [round((250 + s) * 10) for s in range(0, 21)]
    assert arr[7]["apcp"][0] == J.MISSING and list(arr[7]["apcp"][1:]) == [s * 10 for s in range(1, 21)]
    assert (arr["cloud"] == 50).all()


def test_build_grid_budget_runs_out(tmp_path, monkeypatch):
    monkeypatch.setattr(J, "CYCLE_STEPS", list(range(1, 21)))
    _fake_fields(monkeypatch)
    with pytest.raises(J.BudgetExceeded):
        J.build_grid(CYC, "hi", {}, str(tmp_path), deadline=0.0, log=lambda s: None, workers=2)


# ----------------------------------------------------------------------------------- publish, pointer, prune, main

def test_keep_sources_rule():
    """Station tables 3 days + margin (13 runs, 64 hourly cycles); the large cell files only the newest 2 runs and 3
    cycles (plan section 99)."""
    srcs = []
    for d in range(14, 19):                                              # five days of every hour
        for h in range(24):
            srcs.append({"cycle": f"202611{d:02d}{h:02d}", "kind": "run" if h % 6 == 0 else "cycle", "stations": "s",
                         "cells": {"hi": "c"}})
    kept = J.keep_sources(srcs)
    runs = [s["cycle"] for s in kept if s["kind"] == "run"]
    cycles = [s["cycle"] for s in kept if s["kind"] == "cycle"]
    assert len(runs) == 13 and len(cycles) == 64
    assert runs[0] == "2026111818" and runs[-1] == "2026111518"           # 13 runs = 3 days + one
    assert cycles[0] == "2026111823" and cycles[-1] == "2026111520"       # 64 hourly cycles = 3 days + 4
    assert [s["cycle"] for s in kept] == sorted([s["cycle"] for s in kept], reverse=True)
    assert [s["cycle"][-4:] for s in kept if s["cells"]] == ["1823", "1822", "1821", "1818", "1812"]


def _fake_build(c, work, coords, deadline, get=None, log=None, grids=None):
    """A build without eccodes or network: tiny files under the real keys."""
    cyc = J.cycle_name(c)
    files, docs = {}, {}
    for key in (J.cells_key(cyc, "hi"), J.stations_key(cyc)):
        path = os.path.join(work, key.rsplit("/", 1)[-1])
        with open(path, "wb") as f:
            f.write(cyc.encode())
        files[key] = path
    docs[J.cells_index_key(cyc, "hi")] = {"format": "rrfs-cells-v1", "cycle": cyc}
    docs[J.stations_index_key(cyc)] = {"format": "rrfs-stations-v1", "cycle": cyc, "count": 1}
    return files, docs


def _ptr(st):
    return st.get_json("rrfs/v1/latest.json")


def _fresh_pointer(st, sources, published="2026-11-17T15:00:00Z"):
    st.put("rrfs/v1/latest.json", json.dumps({"published_utc": published, "sources": sources}).encode(),
           "application/json", P.POINTER)


NOW = datetime(2026, 11, 17, 16, 0, tzinfo=timezone.utc)
BOTH = lambda url: ("t13z" in url) or ("t12z" in url)                     # noqa: E731


def test_main_publishes_cycle_and_run_pointer_last(tmp_path, monkeypatch):
    st = store()
    monkeypatch.setattr(J, "build", _fake_build)
    assert J.main([], head=BOTH, store=st, now=NOW, coords=COORDS, log=lambda s: None) == 0
    puts = [k for op, k in st.client.log if op == "put"]
    ptr = _ptr(st)
    assert [s["cycle"] for s in ptr["sources"]] == ["2026111713", "2026111712"]
    assert [s["kind"] for s in ptr["sources"]] == ["cycle", "run"] and ptr["newest"] == "2026111713"
    first = puts.index("rrfs/v1/latest.json")
    assert puts.index("rrfs/v1/2026111713/hi.bin") < puts.index("rrfs/v1/2026111713/hi.json") < first
    assert puts.index("rrfs/v1/2026111712/hi.bin") > first and puts[-1] == "rrfs/v1/latest.json"
    cc = {k: v["cc"] for k, v in st.client.objects.items()}
    assert cc["rrfs/v1/2026111713/hi.bin"] == "no-store"                        # ranged reads go straight to R2
    assert cc["rrfs/v1/2026111713/stations.bin"] == P.IMMUTABLE
    assert cc["rrfs/v1/2026111713/hi.json"] == P.IMMUTABLE and cc["rrfs/v1/latest.json"] == P.POINTER
    assert J.main([], head=BOTH, store=st, now=NOW, coords=COORDS, log=lambda s: None) == 0   # nothing new


def test_the_run_waits_when_the_cycle_used_the_budget(monkeypatch):
    st = store()
    monkeypatch.setattr(J, "build", _fake_build)
    monkeypatch.setattr(J, "RUN_MIN_LEFT_S", J.BUILD_BUDGET_S + 1)       # never enough left after a cycle
    _fresh_pointer(st, [{"cycle": "2026111706", "kind": "run", "stations": "x"}])
    assert J.main([], head=BOTH, store=st, now=NOW, coords=COORDS, log=lambda s: None) == 0
    assert [s["cycle"] for s in _ptr(st)["sources"]] == ["2026111713", "2026111706"]
    assert J.main([], head=BOTH, store=st, now=NOW, coords=COORDS, log=lambda s: None) == 0   # next tick: the run
    assert [s["cycle"] for s in _ptr(st)["sources"]] == ["2026111713", "2026111712", "2026111706"]


def test_prune_keeps_what_either_pointer_names_and_newer_cycles():
    st = store()
    for k in ("rrfs/v1/2026111701/stations.bin", "rrfs/v1/2026111710/hi.bin", "rrfs/v1/2026111710/hi.json",
              "rrfs/v1/2026111711/stations.json", "rrfs/v1/2026111799/hi.bin"):
        st.put(k, b"x", "application/octet-stream", P.IMMUTABLE)
    new = {"newest": "2026111711", "sources": [{"cycle": "2026111711", "kind": "cycle",
                                                 "stations": "rrfs/v1/2026111711/stations.json", "cells": None}]}
    old = {"newest": "2026111710", "sources": [{"cycle": "2026111710", "kind": "cycle",
                                                 "stations": "rrfs/v1/2026111710/stations.json",
                                                 "cells": {"hi": "rrfs/v1/2026111710/hi.json"}}]}
    J.prune(st, new, old)
    left = set(st.client.objects)
    assert "rrfs/v1/2026111701/stations.bin" not in left                    # named by neither: gone
    assert {"rrfs/v1/2026111710/hi.bin", "rrfs/v1/2026111710/hi.json"} <= left   # the previous pointer's: kept
    assert "rrfs/v1/2026111711/stations.json" in left
    assert "rrfs/v1/2026111799/hi.bin" in left                              # newer than the pointer: a build writing
    J.prune(st, new)                                                        # once the old pointer is gone too
    assert "rrfs/v1/2026111710/hi.bin" not in st.client.objects


def test_publish_then_prune_drops_cells_only_after_the_next_publish(tmp_path, monkeypatch):
    st = store()
    monkeypatch.setattr(J, "build", _fake_build)
    hours = [7, 8, 9, 10, 11]
    for h in hours:
        c = datetime(2026, 11, 17, h, tzinfo=timezone.utc)
        assert J.main(["--cycle", J.cycle_name(c)], head=lambda u: True, store=st, now=NOW, coords=COORDS,
                      log=lambda s: None) in (0, 1)
    with_cells = [s["cycle"] for s in _ptr(st)["sources"] if s["cells"]]
    assert with_cells == ["2026111711", "2026111710", "2026111709"]
    # 08's cells left the pointer at the 11 publish but the pointer before it (published at 10) named them: kept
    assert "rrfs/v1/2026111708/hi.bin" in st.client.objects
    assert "rrfs/v1/2026111707/hi.bin" not in st.client.objects


def test_not_served_is_exit_3_then_loud_at_the_sixth(monkeypatch):
    st = store()
    _fresh_pointer(st, [{"cycle": "2026111712", "kind": "run", "stations": "x"},
                        {"cycle": "2026111712", "kind": "cycle", "stations": "x"}])
    head = lambda url: "t13z" in url                                        # noqa: E731
    monkeypatch.setattr(J, "build", lambda *a, **k: (_ for _ in ()).throw(J.NotReady("404 x")))
    codes = [J.main([], head=head, store=st, now=NOW, coords=COORDS, log=lambda s: None) for _ in range(7)]
    assert codes == [3, 3, 3, 3, 3, 1, 3]
    assert st.get_json("rrfs/v1/notready/2026111713.json")["count"] == 7


def test_bad_data_is_exit_1_with_a_failure_record_then_waits(monkeypatch):
    st = store()
    _fresh_pointer(st, [{"cycle": "2026111712", "kind": "run", "stations": "x"},
                        {"cycle": "2026111712", "kind": "cycle", "stations": "x"}])
    head = lambda url: "t13z" in url                                        # noqa: E731
    monkeypatch.setattr(J, "build", lambda *a, **k: (_ for _ in ()).throw(ValueError("layout")))
    assert J.main([], head=head, store=st, now=NOW, coords=COORDS, log=lambda s: None) == 1
    assert st.get_json("rrfs/v1/failed/2026111713.json")["attempts"] == 1
    assert J.main([], head=head, store=st, now=NOW, coords=COORDS, log=lambda s: None) == 3   # waits, no new attempt
    assert st.get_json("rrfs/v1/failed/2026111713.json")["attempts"] == 1


def test_stale_per_kind():
    hourly_ok = [{"cycle": "2026111713", "kind": "cycle"}]
    assert J.stale_kinds({"sources": hourly_ok + [{"cycle": "2026111712", "kind": "run"}]}, NOW) == []
    old_run = J.stale_kinds({"sources": hourly_ok + [{"cycle": "2026111612", "kind": "run"}]}, NOW)
    assert [(k, c) for k, c, _ in old_run] == [("run", "2026111612")]          # 28 h: the hourly cycles hide nothing
    assert [k for k, c, _ in J.stale_kinds({"sources": [{"cycle": "2026111706", "kind": "run"}]}, NOW)] == ["cycle"]
    assert [k for k, c, _ in J.stale_kinds({}, NOW)] == ["cycle", "run"]


def test_stale_alarm_once_every_6_hours_even_while_cycles_publish(monkeypatch):
    st = store()
    monkeypatch.setattr(J, "build", _fake_build)
    _fresh_pointer(st, [{"cycle": "2026111612", "kind": "run", "stations": "x"}], published="2026-11-17T15:55:00Z")
    head = lambda url: "t13z" in url                                        # the hourly cycles go on; no run
    assert J.main([], head=head, store=st, now=NOW, coords=COORDS, log=lambda s: None) == 1
    later = datetime(2026, 11, 17, 17, 0, tzinfo=timezone.utc)
    assert J.main([], head=head, store=st, now=later, coords=COORDS, log=lambda s: None) == 0
    much_later = datetime(2026, 11, 17, 22, 30, tzinfo=timezone.utc)
    assert J.main([], head=lambda u: False, store=st, now=much_later, coords=COORDS, log=lambda s: None) == 1


def test_dry_run_flags():
    assert J.main(["--out", "x"], store=store(), log=lambda s: None) == 2
    assert J.main(["--dry-run", "--grids", "mars"], log=lambda s: None) == 2
    assert J.main(["--dry-run", "--cycle", "2026"], log=lambda s: None) == 2


# ------------------------------------------------------------------------------------------- workflow and trigger pins

def _text(*parts):
    with open(os.path.join(ROOT, *parts), encoding="utf-8") as f:
        return f.read()


def test_workflow_pins():
    wf = _text(".github", "workflows", "model-rrfs.yml")
    glwu = _text(".github", "workflows", "model-glwu.yml")
    env = lambda s: s[s.index("create-args:"):s.index("cache-environment")]      # noqa: E731
    assert env(wf) == env(glwu)                                    # the pinned env that decodes the RRFS grids
    assert "group: model-rrfs" in wf and "cancel-in-progress: false" in wf and "timeout-minutes: 40" in wf
    assert 'cron: "*/10 * * * *"' in wf and "tools/model_frames/rrfs.py" in wf
    assert '[ "$rc" -eq 3 ]' in wf and 'ARGS+=(--grids "$GRIDS" --dry-run)' in wf
    assert "micromamba-releases/releases/download/2.10.0-0/" in wf
    assert J.BUILD_BUDGET_S < 40 * 60


def test_trigger_keepalive_and_tests_paths():
    assert '"GH_WORKFLOW": "' in _text("tools", "model_frames", "trigger", "wrangler.jsonc")
    assert ",model-rrfs.yml\"" in _text("tools", "model_frames", "trigger", "wrangler.jsonc")
    assert 'gh workflow enable model-rrfs.yml --repo "$GITHUB_REPOSITORY"' in _text(
        ".github", "workflows", "model-frames-keepalive.yml")
    tests = _text(".github", "workflows", "model-frames-tests.yml")
    assert tests.count('".github/workflows/model-rrfs.yml"') == 2 and tests.count('"tests/fixtures/rrfs/**"') == 2
    assert tests.count('"rrfs_grid.py"') == 2
    assert "python -m pytest -q tests/model_frames tests/test_rrfs_grid.py" in tests       # the grid tests run in CI
    assert P.RRFS_PREFIX == "rrfs/v1"


def test_u16_boundaries():
    """65535 is the missing code: a value that would round to it is missing, never a real 655.35 (G66a P3)."""
    assert list(J._u16(np.array([655.34, 655.35, 700.0, -0.001, -0.01, np.nan]), 100)) == [65534, 65535, 65535, 0,
                                                                                        65535, 65535]


# ------------------------------------------------------------------------------------------- the warm-up reads (2026-11-19)

def test_warm_cells_reads_14_bytes_of_each_file_through_the_public_host():
    import rrfs as JJ
    real = JJ.__dict__.get("_real_warm_cells") or JJ.warm_cells
    calls = []

    def get(url, rng=None, tries=4):
        calls.append((url, rng, tries))
        if "ak.bin" in url:
            raise JJ.TransportError("timed out")
        return b"x" * 14

    lines = []
    out = real(["rrfs/v1/2026111900/conus.bin", "rrfs/v1/2026111900/ak.bin"], get=get, log=lines.append)
    assert sorted(calls) == [(J.PUBLIC_ROOT + "/rrfs/v1/2026111900/ak.bin", (0, 13), 2),
                             (J.PUBLIC_ROOT + "/rrfs/v1/2026111900/conus.bin", (0, 13), 2)]
    assert out["rrfs/v1/2026111900/ak.bin"] is None and out["rrfs/v1/2026111900/conus.bin"] >= 0
    assert any("warm-up rrfs/v1/2026111900/ak.bin: TransportError" in x for x in lines), "a failure only logs"
    assert real([], get=get, log=lines.append) == {}
    assert J.PUBLIC_ROOT == "https://models.allshoresurf.com"


def test_publish_warms_the_cell_files_before_the_pointer(tmp_path, monkeypatch):
    st = store()
    monkeypatch.setattr(J, "build", _fake_build)
    seen = []
    monkeypatch.setattr(J, "warm_cells", lambda keys, get=None, log=None: seen.append((list(keys), [k for op, k in st.client.log if op == "put"])) or {})
    assert J.main(["--cycle", "2026111713"], head=lambda u: True, store=st, now=NOW, coords=COORDS, log=lambda s: None) in (0, 1)
    keys, puts_then = seen[0]
    assert keys == ["rrfs/v1/2026111713/hi.bin"], "the cell files only (not stations.bin, not frames)"
    assert "rrfs/v1/2026111713/hi.bin" in puts_then and "rrfs/v1/latest.json" not in puts_then
