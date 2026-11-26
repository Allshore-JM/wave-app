"""The Nearshore Analysis publisher (plan section 109, stage 5): the frame format, the tide, the tiles, the job's order
of writes. A made-up table and spectrum; no network, no secrets."""
import hashlib
import io
import json
import math
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import fetch  # noqa: E402
import nearshore as N  # noqa: E402
import nearshore_engine as E  # noqa: E402
import publish as P  # noqa: E402
from test_gfs_spectral import FakeClient  # noqa: E402

RUN = "2026112600"
RUN_DT = datetime(2026, 11, 26, 0, tzinfo=timezone.utc)
F = [0.05, 0.0625, 0.08, 0.1]
D = [300.0, 315.0, 330.0, 345.0, 0.0]
NY, NX = 100, 240


def table_npz():
    """A plane beach 2.4 km long facing north-west near Laniakea; every bin's waves arrive 40 deg off the normal."""
    dep = np.repeat((0.03 * (np.arange(NY) - 8) * 10.0)[:, None], NX, axis=1)
    nb = len(F) * len(D); buf = io.BytesIO()
    np.savez_compressed(buf, F=np.array(F), D=np.array(D), dx=10.0, alpc=45.0, xpc=-900.0, ypc=-1500.0, lon0=-158.09, lat0=21.62,
                        version="test-1", dep_cm=np.rint(dep * 100).astype(np.int16), K=E.Table.encode_k(np.ones((nb, NY, NX))),
                        DIR=np.full((nb, NY, NX), int(round(355.0 * 256 / 360)), np.uint8))
    return buf.getvalue()


def spec_npz(hours=range(0, 12), hs=2.0, run=RUN):
    f, d, S = E.jonswap(hs, 14.0, 330.0)
    buf = io.BytesIO()
    np.savez_compressed(buf, format=np.array("gfsspec2d-v1"), run=np.array(run), freqs=f.astype(np.float32), dirs=d.astype(np.float32),
                        hours=np.array(list(hours), np.int16), E=np.stack([S * (1.0 + 0.02 * h) for h in hours]).astype(np.float32),
                        u10=np.full(len(list(hours)), 5.0, np.float32), udir=np.full(len(list(hours)), 60.0, np.float32))
    return buf.getvalue()


def tides_doc():
    t0 = RUN_DT.timestamp() - 6 * 3600
    return {"station": "1612668", "datum": "MLLW", "msl_minus_mllw": 0.2835,
            "hilo": [[t0 + k * 6.2 * 3600, 0.6 if k % 2 else 0.0] for k in range(80)]}


@pytest.fixture()
def data(tmp_path):
    files = {"table.npz": table_npz(), "atlas.json": json.dumps({"version": "test-1", "calibrated": False, "texts": ["Best here: buoy 4-6 ft"], "cells": [[3, 4, 0]]}).encode(),
             "tides.json": json.dumps(tides_doc()).encode()}
    for name, body in files.items():
        (tmp_path / name).write_bytes(body)
    (tmp_path / "manifest.json").write_text(json.dumps({"version": "test-1", "sha256": {k: hashlib.sha256(v).hexdigest() for k, v in files.items()}}))
    return str(tmp_path)


@pytest.fixture(autouse=True)
def _few_steps(monkeypatch):
    monkeypatch.setattr(N, "STEPS", [0, 1, 2, 3, 6, 9])
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)


def store_with(spec=True, run=RUN):
    st = P.Store(FakeClient(), "bucket")
    st.put(P.latest_key(N.SPEC_PREFIX), json.dumps({"run": run}).encode(), "application/json", P.POINTER)
    if spec:
        st.put(N.spec2d_key(run), spec_npz(run=run), "application/octet-stream", P.IMMUTABLE)
    st.client.log.clear()
    return st


def test_the_steps_are_the_wave_overlays():
    src = open(os.path.join(ROOT, "tools", "model_frames", "nearshore.py"), encoding="utf-8").read()
    own = eval(re.search(r"^STEPS = (.+?)\s+#", src, re.M).group(1))            # the module's own value (the fixture replaces it here)
    assert own == fetch.STEPS and len(own) == 209 and own[-1] == 384
    assert P.NEARSHORE_PREFIX == "gfswave/nearshore/v1" and N.PREFIX == P.NEARSHORE_PREFIX and N.KEEP == 4


def test_the_tide_is_a_cosine_between_high_and_low_against_mean_sea_level():
    td = tides_doc(); t0 = td["hilo"][0][0]
    at = lambda h: N.tide_msl(td, datetime.fromtimestamp(t0 + h * 3600, timezone.utc))   # noqa: E731
    assert at(0) == pytest.approx(0.0 - 0.2835) and at(6.2) == pytest.approx(0.6 - 0.2835) and at(3.1) == pytest.approx(0.3 - 0.2835)
    assert at(1.0) < 0.3 - 0.2835 - 0.1                               # a cosine, flat near the turn
    naive = datetime.fromtimestamp(t0 + 3600, timezone.utc).replace(tzinfo=None)
    assert N.tide_msl(td, naive) == pytest.approx(at(1.0))             # a naive time is read as UTC
    with pytest.raises(ValueError):
        N.tide_msl(td, datetime(2031, 1, 1, tzinfo=timezone.utc))


def test_the_tiles_cover_the_band_and_every_pixel_knows_its_cell(data):
    table, _, _, _ = N.load_data(data)
    maps, box = N.tile_maps(table)
    assert 1 <= len(maps) <= 6 and box[0] < box[2] and box[1] < box[3] and -158.2 < box[0] < -158.0 and 21.5 < box[1] < 21.7
    assert maps == sorted(maps, key=lambda m: (m[1], m[0]))
    seen = np.zeros(NY * NX, bool)
    for tx, ty, idx in maps:
        assert idx.shape == (256, 256)
        j, i = np.nonzero(idx >= 0); k = len(j) // 2; cell = idx[j[k], i[k]]
        plon, plat = N.tile_pixels(tx, ty)
        lon, lat = table.lonlat((cell % NX) * 10.0, (cell // NX) * 10.0)
        assert abs(lon - plon[i[k]]) * 104000 < 8 and abs(lat - plat[j[k]]) * 111000 < 8   # the cell centre is under the pixel
        seen[idx[idx >= 0]] = True
    wet = table.dep.reshape(-1) > 0
    assert seen[wet].mean() > 0.6                                     # z14 pixels (8.9 m) reach most 10 m cells
    x, y = table.xy(*table.lonlat(1234.0, 567.0)); assert (x, y) == (pytest.approx(1234.0, abs=1e-6), pytest.approx(567.0, abs=1e-6))


def test_a_frame_round_trips_heights_lines_and_the_header(data):
    table, _, tides, _ = N.load_data(data)
    maps, box = N.tile_maps(table)
    f, d, S = E.jonswap(2.0, 14.0, 330.0)
    png, stats = N.build_frame(table, maps, box, f, d, S, (5.0, 60.0), 0.2)
    assert png[:8] == b"\x89PNG\r\n\x1a\n" and len(png) < 60000
    fr = N.decode_frame(png)
    assert fr["version"] == 1 and fr["tiles"] == len(maps) and fr["tide_m"] == pytest.approx(0.2)
    assert fr["buoy"][0] == pytest.approx(2.0, abs=0.05) and fr["buoy"][1] == pytest.approx(14.0, abs=0.6) and fr["buoy"][2] == pytest.approx(330, abs=6)
    assert fr["planes"].max() > 0 and fr["planes"].max() * N.HEIGHT_STEP_M == pytest.approx(stats["hmax_m"], abs=0.2)
    assert 1.5 < stats["hmax_m"] < 3.5                                # 1.27 x 2 m at the line, less inside
    v = fr["vertices"]; assert len(v) == stats["vertices"] > 100
    assert np.all((v["flags"] & 1) == 1) and int(v["flags"][0]) & 128 and np.all(((v["flags"] >> 4) & 7) == 2)   # rights, in the ideal band
    assert np.allclose(v["peel"] & 127, 40, atol=2) and len(set((v["peel"] >> 7).tolist())) == 1 and v["ride"].max() == 255 and np.allclose(v["hb"] * N.HEIGHT_STEP_M, 1.27 * 2.0, atol=0.15)
    assert np.all(v["share"] >= 98) and v["score"].max() == 70        # open-water waves on a gentle (spilling) beach
    lon = box[0] + v["x"] / 65535.0 * (box[2] - box[0]); lat = box[1] + v["y"] / 65535.0 * (box[3] - box[1])
    gx, gy = table.xy(lon, lat)
    depth = 0.03 * (gy - 80.0) + 0.2                                  # the line sits where set height = 0.9 x (depth + tide)
    assert np.allclose(depth, 1.27 * 2.0 / 0.9, atol=0.5)
    calm, st0 = N.build_frame(table, maps, box, f, d, S * 0.0, (0.0, 0.0), 0.0)
    assert N.decode_frame(calm)["planes"].max() == 0 and st0["vertices"] == 0


def test_main_publishes_the_frames_the_index_and_the_pointer_last(data):
    st = store_with()
    assert N.main(["--data", data], store=st) == 0
    puts = [k for op, k in st.client.log if op == "put"]
    assert puts[0] == N.atlas_key("test-1") and puts[-1] == P.latest_key(N.PREFIX) and puts[-2] == N.index_key(RUN, "test-1")
    assert puts[1:-2] == [N.frame_key(RUN, "test-1", s) for s in (0, 1, 2, 3, 6, 9)]
    objs = st.client.objects
    assert all(objs[k]["cc"] == P.IMMUTABLE for k in puts[:-1]) and objs[puts[-1]]["cc"] == P.POINTER
    doc = json.loads(objs[N.index_key(RUN, "test-1")]["body"])
    assert doc["format"] == "nearshore-v1" and doc["run"] == RUN and doc["steps"] == [0, 1, 2, 3, 6, 9] and doc["z"] == 14
    assert doc["table"] == "test-1" and doc["atlas"] == N.atlas_key("test-1") and len(doc["tiles"]) >= 1 and doc["vertex_bytes"] == 10
    assert json.loads(objs[P.latest_key(N.PREFIX)]["body"]) | {"published_utc": ""} == {"run": RUN, "index": N.index_key(RUN, "test-1"), "steps": 6, "table": "test-1", "published_utc": ""}
    assert N.decode_frame(objs[N.frame_key(RUN, "test-1", 9)]["body"])["buoy"][0] > N.decode_frame(objs[N.frame_key(RUN, "test-1", 0)]["body"])["buoy"][0]
    st.client.log.clear()
    assert N.main(["--data", data], store=st) == 0 and st.client.log == []          # already live: nothing written


def test_no_spectrum_is_exit_3_and_a_broken_one_is_a_failure(data, capsys):
    st = store_with(spec=False)
    assert N.main(["--data", data], store=st) == 3 and st.client.log == []
    assert "kept no spectrum" in capsys.readouterr().out
    st.put(N.spec2d_key(RUN), spec_npz(run="2026112518"), "application/octet-stream", P.IMMUTABLE)   # another run's spectrum
    assert N.main(["--data", data], store=st) == 1 and "FAILED" in capsys.readouterr().out
    rec = P.failed_key(N.failed_label(RUN, "test-1"), N.PREFIX)
    assert st.get_json(rec)["attempts"] == 1                                                         # counted (review round 1, B3)
    st.put(N.spec2d_key(RUN), spec_npz(hours=range(0, 2)), "application/octet-stream", P.IMMUTABLE)   # most steps missing
    assert N.main(["--data", data], store=st) == 3 and "failed recently" in capsys.readouterr().out   # an hour before the next try
    st.client.objects.pop(rec)
    assert N.main(["--data", data], store=st) == 1 and st.get_json(rec)["attempts"] == 1
    assert not any(k.startswith(N.PREFIX + "/" + RUN) for k in st.client.objects)                    # nothing of a failed build is published
    empty = P.Store(FakeClient(), "bucket")
    assert N.main(["--data", data], store=empty) == 3                                # the spectral job has no live run


def test_a_bad_data_folder_is_refused(data, tmp_path):
    with open(os.path.join(data, "tides.json"), "ab") as f:
        f.write(b" ")
    assert N.main(["--data", data], store=store_with()) == 1
    assert N.main(["--data", data, "--run", "20261126"], store=store_with()) == 2


def test_an_older_run_is_never_published_over_a_newer_one_and_a_new_table_rebuilds(data):
    st = store_with()
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": "2026112606", "table": "test-1"}).encode(), "application/json", P.POINTER)
    st.client.log.clear()
    assert N.main(["--data", data], store=st) == 0 and st.client.log == []
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": RUN, "table": "older-table"}).encode(), "application/json", P.POINTER)
    assert N.main(["--data", data], store=st) == 0
    assert json.loads(st.client.objects[P.latest_key(N.PREFIX)]["body"])["table"] == "test-1"


def test_dry_run_writes_files_and_publishes_nothing(data, tmp_path):
    spec = tmp_path / "s.npz"; spec.write_bytes(spec_npz())
    out = tmp_path / "out"
    assert N.main(["--data", data, "--spec", str(spec), "--out", str(out), "--steps", "0,3"]) == 0
    assert sorted(os.listdir(out)) == ["f000.png", "f003.png", "index.json"]
    doc = json.load(open(out / "index.json")); assert doc["steps"] == [0, 3] and doc["stats"]["3"]["vertices"] > 100
    st = store_with()
    assert N.main(["--data", data, "--dry-run"], store=st) == 0 and st.client.log == []
    assert N.main(["--data", data, "--out", str(out)], store=st) == 2


def test_prune_keeps_four_runs_and_leaves_one_being_written():
    st = P.Store(FakeClient(), "bucket")
    for run in ["2026112400", "2026112406", "2026112412", "2026112418", "2026112500", "2026112506"]:
        st.put(N.frame_key(run, "t", 0), b"x", "image/png", P.IMMUTABLE)
        st.put(N.index_key(run, "t"), b"{}", "application/json", P.IMMUTABLE)
    st.put(N.frame_key("2026112512", "t", 0), b"x", "image/png", P.IMMUTABLE)       # being written
    st.put(P.failed_key(N.failed_label("2026112400", "t"), N.PREFIX), b"{}", "application/json", P.POINTER)   # an old record
    st.put(P.failed_key(N.failed_label("2026112506", "t"), N.PREFIX), b"{}", "application/json", P.POINTER)   # a kept run's
    st.put(N.atlas_key("test-1"), b"{}", "application/json", P.IMMUTABLE)
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": "2026112506"}).encode(), "application/json", P.POINTER)
    assert N.prune(st) == ["2026112400", "2026112406"]
    keys = set(st.client.objects)
    assert N.index_key("2026112412", "t") in keys and N.frame_key("2026112512", "t", 0) in keys and N.atlas_key("test-1") in keys
    assert P.failed_key(N.failed_label("2026112400", "t"), N.PREFIX) not in keys and P.failed_key(N.failed_label("2026112506", "t"), N.PREFIX) in keys


def test_the_spectrum_can_come_straight_from_noaas_archive(data):
    from test_gfs_spectral import FakeNoaa, archive, spec_text
    import gfs_winds as W
    run_dt = datetime(2026, 11, 1, 12, tzinfo=timezone.utc)
    arc = archive({"41113": spec_text("41113", steps=range(3)), "51201": spec_text("51201", steps=range(12)), "46026": spec_text("46026", steps=range(3))})
    raw = N.spec_from_archive(run_dt, opener=FakeNoaa({"2026110112": arc}))
    z = np.load(io.BytesIO(raw)); assert str(z["run"]) == "2026110112" and list(z["hours"]) == list(range(12))
    with pytest.raises(ValueError):
        N.spec_from_archive(run_dt, opener=FakeNoaa({"2026110112": archive({"41113": spec_text("41113", steps=range(3))})}))
    assert W.STEPS == 385


def test_the_workflow_runs_the_nearshore_step_after_the_tables_and_never_fails_them():
    text = open(os.path.join(ROOT, ".github", "workflows", "model-gfsspec.yml"), encoding="utf-8").read()
    spectral = text.index("python tools/model_frames/gfs_spectral.py"); near = text.index("python tools/model_frames/nearshore.py")
    assert spectral < near and "--data nearshore-data/pilot --from-archive" in text
    step = text[text.index("- name: Nearshore data"):]
    assert step.count("continue-on-error: true") == 2 and step.rstrip().endswith("exit 0")
    assert re.search(r"ref: [0-9a-f]{40} ", step) and "path: nearshore-data" in step            # the data at a pinned commit
    assert 'NSDATA: ${{ steps.nsdata.outcome }}' in step and 'if [ "$NSDATA" != "success" ]' in step
    assert step.count("        if: steps.env.outcome == 'success'\n") == 2                    # the repo's rule for every step
    assert 'NEARSHORE_ONLY: ${{ github.event.inputs.nearshore_only }}' in text and 'if [ "$NEARSHORE_ONLY" = "true" ]' in text
    uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
    assert uses and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses)


# ---------------------------------------------------------------- review round 1 (stage 7)

def test_another_table_never_rewrites_a_runs_objects(data):
    """B1: the table's version is part of the path. The same run from another table is another set of objects."""
    st = store_with()
    old = {N.frame_key(RUN, "older-table", 0): b"old frame", N.index_key(RUN, "older-table"): b'{"table":"older-table"}'}
    for k, v in old.items():
        st.put(k, v, "image/png", P.IMMUTABLE)
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": RUN, "table": "older-table", "index": N.index_key(RUN, "older-table")}).encode(), "application/json", P.POINTER)
    st.client.log.clear()
    assert N.main(["--data", data], store=st) == 0
    puts = [k for op, k in st.client.log if op == "put"]
    assert all("/test-1/" in k for k in puts if k.startswith(N.PREFIX + "/" + RUN)) and not set(puts) & set(old)
    assert all(st.client.objects[k]["body"] == v for k, v in old.items())
    ptr = json.loads(st.client.objects[P.latest_key(N.PREFIX)]["body"])
    assert ptr["table"] == "test-1" and ptr["index"] == N.index_key(RUN, "test-1") == f"gfswave/nearshore/v1/{RUN}/test-1/index.json"
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": RUN, "table": "older-table"}).encode(), "application/json", P.POINTER)   # the pointer set back by hand
    st.client.log.clear()
    assert N.main(["--data", data], store=st) == 0 and st.client.log == []           # already published: nothing rewritten


def test_a_failed_build_waits_an_hour_and_stops_after_six(data, capsys):
    """B3 / B4: a run whose build fails is not rebuilt (nor NOAA's archive read again) on every ten-minute tick."""
    st = store_with()
    st.put(N.spec2d_key(RUN), spec_npz(hours=range(0, 2)), "application/octet-stream", P.IMMUTABLE)
    rec = P.failed_key(N.failed_label(RUN, "test-1"), N.PREFIX)
    assert N.main(["--data", data], store=st) == 1 and N.main(["--data", data], store=st) == 3
    doc = st.get_json(rec); assert doc["attempts"] == 1 and "steps in the spectrum" in doc["last_error"]
    doc["last_attempt_utc"] = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    st.put(rec, json.dumps(doc).encode(), "application/json", P.POINTER)
    assert N.main(["--data", data], store=st) == 1 and st.get_json(rec)["attempts"] == 2      # an hour on: one more try
    doc = st.get_json(rec); doc["attempts"] = 6; doc["last_attempt_utc"] = "2026-11-01T00:00:00Z"
    st.put(rec, json.dumps(doc).encode(), "application/json", P.POINTER)
    assert N.main(["--data", data], store=st) == 3                                            # six tries: no more
    st.put(N.spec2d_key(RUN), spec_npz(), "application/octet-stream", P.IMMUTABLE)
    assert N.main(["--data", data, "--dry-run"], store=st) == 0                               # a dry run is never held back
    capsys.readouterr()


def test_the_archive_fallback_gives_up_at_its_deadline():
    from test_gfs_spectral import FakeNoaa, archive, spec_text
    import gfs_winds as W
    arc = archive({"41113": spec_text("41113", steps=range(3)), "51201": spec_text("51201", steps=range(3))})
    with pytest.raises(W.BudgetExceeded):
        N.spec_from_archive(datetime(2026, 11, 1, 12, tzinfo=timezone.utc), opener=FakeNoaa({"2026110112": arc}), budget_s=-1.0)
    assert N.ARCHIVE_BUDGET_S <= 600
    text = open(os.path.join(ROOT, ".github", "workflows", "model-gfsspec.yml"), encoding="utf-8").read()
    assert "timeout-minutes: 12" in text.split("- name: Publish the nearshore analysis", 1)[1]


def test_past_the_tide_files_end_steps_use_mean_sea_level_and_the_summary_warns(data):
    table, _, tides, _ = N.load_data(data)
    short = dict(tides, hilo=[hl for hl in tides["hilo"] if hl[0] < RUN_DT.timestamp() + 7 * 3600])   # the last turn is at +6.4 h
    spec = np.load(io.BytesIO(spec_npz()))
    frames, doc, stats = N.build_run(table, short, spec, RUN_DT, [0, 1, 2, 3, 6, 9])
    assert doc["steps_without_tide"] == 1 and sorted(frames) == [0, 1, 2, 3, 6, 9] and stats[9]["tide_m"] == 0.0 and stats[0]["tide_m"] != 0.0
    assert doc["tides_end_utc"].startswith("2026-11-2")
    assert "ended" in N.tide_warning(short, RUN_DT) and "Rebuild the data folder" in N.tide_warning(short, RUN_DT)
    far = dict(tides, hilo=tides["hilo"] + [[RUN_DT.timestamp() + 400 * 86400, 0.3]])
    assert N.tide_warning(far, RUN_DT) is None
    soon = dict(tides, hilo=tides["hilo"] + [[RUN_DT.timestamp() + 40 * 86400, 0.3]])
    assert "days of forecasts left" in N.tide_warning(soon, RUN_DT)
    assert "empty" in N.tide_warning({"hilo": []}, RUN_DT)


def test_the_vertex_cap_drops_whole_lines_the_weakest_first(data, monkeypatch):
    """B5: a capped frame never joins the ends of two lines."""
    table, _, _, _ = N.load_data(data)
    _, box = N.tile_maps(table)
    n = 200; one = np.ones(2 * n)
    lines = dict(x=np.concatenate((np.arange(n) * 10.0, np.arange(n) * 10.0)), y=np.concatenate((np.full(n, 300.0), np.full(n, 600.0))),
                 line=np.concatenate((np.zeros(n), np.ones(n))), score=np.concatenate((np.full(n, 10.0), np.full(n, 80.0))),
                 side=one, mixed=0 * one, breaker=one.astype(int), band=2 * one.astype(int), peel=40 * one, fwd=one.astype(bool),
                 ride_m=100 * one, hb=2 * one, share=one)
    raw, count = N.pack_vertices(lines, table, box)
    assert count == 400
    monkeypatch.setattr(N, "MAX_VERTICES", 300)
    raw, count = N.pack_vertices(lines, table, box)
    v = np.frombuffer(raw, dtype=[("x", "<u2"), ("y", "<u2"), ("peel", "u1"), ("flags", "u1"), ("ride", "u1"), ("hb", "u1"), ("score", "u1"), ("share", "u1")])
    assert count == 200 and len(v) == 200 and np.all(v["score"] == 80)               # the line with the better ride is the one kept
    assert int((v["flags"] & 128 > 0).sum()) == 1 and int(v["flags"][0]) & 128       # one line, begun once
    step = np.hypot(np.diff(v["x"].astype(float)), np.diff(v["y"].astype(float)))
    assert step.max() < 3 * np.median(step)                                          # no straight join across a gap
    monkeypatch.setattr(N, "MAX_VERTICES", 100)
    assert N.pack_vertices(lines, table, box) == (b"", 0)                            # no line fits: none, rather than a piece of one
