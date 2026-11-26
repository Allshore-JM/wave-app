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
    import nearshore
    assert list(nearshore.__dict__["STEPS"]) != [] and fetch.STEPS == list(range(0, 121)) + list(range(123, 385, 3))
    src = open(os.path.join(ROOT, "tools", "model_frames", "nearshore.py"), encoding="utf-8").read()
    assert "STEPS = list(range(0, 121)) + list(range(123, 385, 3))" in src
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
    assert puts[0] == N.atlas_key("test-1") and puts[-1] == P.latest_key(N.PREFIX) and puts[-2] == N.index_key(RUN)
    assert puts[1:-2] == [N.frame_key(RUN, s) for s in (0, 1, 2, 3, 6, 9)]
    objs = st.client.objects
    assert all(objs[k]["cc"] == P.IMMUTABLE for k in puts[:-1]) and objs[puts[-1]]["cc"] == P.POINTER
    doc = json.loads(objs[N.index_key(RUN)]["body"])
    assert doc["format"] == "nearshore-v1" and doc["run"] == RUN and doc["steps"] == [0, 1, 2, 3, 6, 9] and doc["z"] == 14
    assert doc["table"] == "test-1" and doc["atlas"] == N.atlas_key("test-1") and len(doc["tiles"]) >= 1 and doc["vertex_bytes"] == 10
    assert json.loads(objs[P.latest_key(N.PREFIX)]["body"]) | {"published_utc": ""} == {"run": RUN, "index": N.index_key(RUN), "steps": 6, "table": "test-1", "published_utc": ""}
    assert N.decode_frame(objs[N.frame_key(RUN, 9)]["body"])["buoy"][0] > N.decode_frame(objs[N.frame_key(RUN, 0)]["body"])["buoy"][0]
    st.client.log.clear()
    assert N.main(["--data", data], store=st) == 0 and st.client.log == []          # already live: nothing written


def test_no_spectrum_is_exit_3_and_a_broken_one_is_a_failure(data, capsys):
    st = store_with(spec=False)
    assert N.main(["--data", data], store=st) == 3 and st.client.log == []
    assert "kept no spectrum" in capsys.readouterr().out
    st.put(N.spec2d_key(RUN), spec_npz(run="2026112518"), "application/octet-stream", P.IMMUTABLE)   # another run's spectrum
    assert N.main(["--data", data], store=st) == 1 and "FAILED" in capsys.readouterr().out
    st.put(N.spec2d_key(RUN), spec_npz(hours=range(0, 2)), "application/octet-stream", P.IMMUTABLE)   # most steps missing
    assert N.main(["--data", data], store=st) == 1
    assert not any(k.startswith(N.PREFIX) for k in st.client.objects)
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
        st.put(N.frame_key(run, 0), b"x", "image/png", P.IMMUTABLE)
        st.put(N.index_key(run), b"{}", "application/json", P.IMMUTABLE)
    st.put(N.frame_key("2026112512", 0), b"x", "image/png", P.IMMUTABLE)            # being written
    st.put(N.atlas_key("test-1"), b"{}", "application/json", P.IMMUTABLE)
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": "2026112506"}).encode(), "application/json", P.POINTER)
    assert N.prune(st) == ["2026112400", "2026112406"]
    keys = set(st.client.objects)
    assert N.index_key("2026112412") in keys and N.frame_key("2026112512", 0) in keys and N.atlas_key("test-1") in keys


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
