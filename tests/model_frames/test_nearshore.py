"""The Nearshore Analysis publisher (plan section 109): the regions and what each owns, the frame format, the tide, the
tiles, the job's order of writes and its failure rules. Made-up tables and spectrum; no network, no secrets."""
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
import nearshore_data as ND  # noqa: E402
import nearshore_engine as E  # noqa: E402
import publish as P  # noqa: E402
from test_gfs_spectral import FakeClient  # noqa: E402

RUN = "2026112600"
RUN_DT = datetime(2026, 11, 26, 0, tzinfo=timezone.utc)
F = [0.05, 0.0625, 0.08, 0.1]
D = [300.0, 315.0, 330.0, 345.0, 0.0]
SET = "t-1"
ROWS, DX = 100, 10.0
# two regions on one straight beach facing north-west (alpha 45): "west" is listed first and owns the 400 m both cores share
REGIONS = [dict(id="west", ref=[-158.10, 21.60], alpha=45.0, x=[-1200, 1200], y0=-80, core=[-1000, 1000], rows=ROWS),
           dict(id="east", ref=[-158.0892, 21.6102], alpha=45.0, x=[-1200, 1200], y0=-80, core=[-1000, 1000], rows=ROWS)]
STEPS = [0, 1, 2, 3, 6, 9]


def table_npz(facts):
    """A plane beach: every bin's waves arrive 40 degrees off the shore normal."""
    nx = int((facts["x"][1] - facts["x"][0]) / DX) + 1; a = math.radians(facts["alpha"])
    dep = np.repeat((0.03 * (np.arange(ROWS) * DX + facts["y0"]))[:, None], nx, axis=1)
    nb = len(F) * len(D); buf = io.BytesIO()
    np.savez_compressed(buf, F=np.array(F), D=np.array(D), dx=DX, alpc=facts["alpha"], lon0=facts["ref"][0], lat0=facts["ref"][1],
                        xpc=facts["x"][0] * math.cos(a) - facts["y0"] * math.sin(a), ypc=facts["x"][0] * math.sin(a) + facts["y0"] * math.cos(a),
                        version=f"{SET}/{facts['id']}", dep_cm=np.rint(dep * 100).astype(np.int16), K=E.Table.encode_k(np.ones((nb, ROWS, nx))),
                        DIR=np.full((nb, ROWS, nx), int(round(355.0 * 256 / 360)), np.uint8))
    return buf.getvalue()


def tables(setname=SET):
    """{key: bytes} of a table set as nearshore_build publishes it: the regions' tables and the manifest."""
    out, regs = {}, []
    for r in REGIONS:
        body = table_npz(r); key = N.tables_key(setname, f"{r['id']}/table.npz"); out[key] = body
        regs.append(dict(r, table=key, sha256=hashlib.sha256(body).hexdigest(), bytes=len(body), bins=len(F) * len(D)))
    out[N.tables_key(setname)] = json.dumps(dict(format="nearshore-tables-v1", set=setname, dx=DX, freqs=F, dirs=D, regions=regs)).encode()
    return out


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


def data_folder(tmp_path, setname=SET, objs=None):
    objs = objs or tables(setname); tides = json.dumps(tides_doc()).encode()
    (tmp_path / "tides.json").write_bytes(tides)
    (tmp_path / "manifest.json").write_text(json.dumps({"version": setname, "tables": {"set": setname, "manifest_sha256": hashlib.sha256(objs[N.tables_key(setname)]).hexdigest()},
                                                        "sha256": {"tides.json": hashlib.sha256(tides).hexdigest()}}))
    return str(tmp_path)


@pytest.fixture()
def data(tmp_path):
    return data_folder(tmp_path)


@pytest.fixture(autouse=True)
def _few_steps(monkeypatch):
    monkeypatch.setattr(N, "STEPS", STEPS)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)


def store_with(spec=True, run=RUN, objs=None):
    st = P.Store(FakeClient(), "bucket")
    st.put(P.latest_key(N.SPEC_PREFIX), json.dumps({"run": run}).encode(), "application/json", P.POINTER)
    for key, body in (tables() if objs is None else objs).items():
        st.put(key, body, "application/octet-stream", P.IMMUTABLE)
    if spec:
        st.put(N.spec2d_key(run), spec_npz(run=run), "application/octet-stream", P.IMMUTABLE)
    st.client.log.clear()
    return st


def fetch_of(objs):
    return lambda key: objs.get(key)


def doc_of(objs, setname=SET):
    return json.loads(objs[N.tables_key(setname)])


def swell_trains(hs=2.0):
    f, d, S = E.jonswap(hs, 14.0, 330.0)
    return N.trains_of(f, d, S, (5.0, 60.0), F, D)


def test_the_steps_are_the_wave_overlays():
    src = open(os.path.join(ROOT, "tools", "model_frames", "nearshore.py"), encoding="utf-8").read()
    own = eval(re.search(r"^STEPS = (.+?)\s+#", src, re.M).group(1))            # the module's own value (the fixture replaces it here)
    assert own == fetch.STEPS and len(own) == 209 and own[-1] == 384
    assert P.NEARSHORE_PREFIX == "gfswave/nearshore/v1" and N.PREFIX == P.NEARSHORE_PREFIX and N.KEEP == 4 and N.FORMAT == "nearshore-v2"
    assert N.frame_key(RUN, "ns-1", "kaena", 3) == f"gfswave/nearshore/v1/{RUN}/ns-1/kaena/f003.png" and N.index_key(RUN, "ns-1") == f"gfswave/nearshore/v1/{RUN}/ns-1/index.json"
    assert N.tables_key("ns-1") == "static/nearshore/ns-1/manifest.json"


def test_the_tide_is_a_cosine_between_high_and_low_against_mean_sea_level():
    td = tides_doc(); t0 = td["hilo"][0][0]
    at = lambda h: N.tide_msl(td, datetime.fromtimestamp(t0 + h * 3600, timezone.utc))   # noqa: E731
    assert at(0) == pytest.approx(0.0 - 0.2835) and at(6.2) == pytest.approx(0.6 - 0.2835) and at(3.1) == pytest.approx(0.3 - 0.2835)
    assert at(1.0) < 0.3 - 0.2835 - 0.1                               # a cosine, flat near the turn
    naive = datetime.fromtimestamp(t0 + 3600, timezone.utc).replace(tzinfo=None)
    assert N.tide_msl(td, naive) == pytest.approx(at(1.0))             # a naive time is read as UTC
    with pytest.raises(ValueError):
        N.tide_msl(td, datetime(2031, 1, 1, tzinfo=timezone.utc))


def test_the_data_folder_pins_the_tides_and_the_table_set(tmp_path):
    folder = data_folder(tmp_path); tides, man = N.load_data(folder)
    assert man["tables"]["set"] == SET and len(tides["hilo"]) == 80
    with open(os.path.join(folder, "tides.json"), "ab") as f:
        f.write(b" ")
    with pytest.raises(ValueError, match="tides.json"):
        N.load_data(folder)
    (tmp_path / "manifest.json").write_text(json.dumps({"version": "x", "sha256": {"tides.json": hashlib.sha256(open(os.path.join(folder, "tides.json"), "rb").read()).hexdigest()}}))
    with pytest.raises(ValueError, match="names no table set"):
        N.load_data(folder)


def test_the_data_folder_builder_writes_what_the_job_accepts(tmp_path, capsys):
    objs = tables(); raw = objs[N.tables_key(SET)]
    (tmp_path / "tables.json").write_bytes(raw); (tmp_path / "old.json").write_text(json.dumps(tides_doc()))
    out = str(tmp_path / "ns")
    assert ND.main([out, "ns-test", "--tables", str(tmp_path / "tables.json"), "--tides", str(tmp_path / "old.json")]) == 0
    assert sorted(os.listdir(out)) == ["manifest.json", "tides.json"], "the tables stay in the bucket"
    tides, man = N.load_data(out)
    assert man["version"] == "ns-test" and man["tables"] == {"set": SET, "manifest_sha256": hashlib.sha256(raw).hexdigest(), "regions": ["west", "east"]}
    assert tides == tides_doc() and "west, east" in capsys.readouterr().out
    assert N.load_tables(fetch_of(objs), man)["set"] == SET, "the pin is the bucket's manifest, byte for byte"
    for bad in (b"{}", json.dumps({"format": "nearshore-tables-v1", "set": SET, "regions": []}).encode(),
                json.dumps({"format": "nearshore-tables-v1", "set": SET, "regions": [{"id": "west", "table": "k"}]}).encode()):
        with pytest.raises(ValueError):
            ND.check_tables(bad)


def test_the_tides_come_from_noaa_year_by_year():
    asked = []

    class Reply(io.BytesIO):
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def opener(url, timeout):
        y = int(re.search(r"begin_date=(\d{4})", url).group(1)); asked.append(y)
        t0 = datetime(y, 1, 1, tzinfo=timezone.utc)
        return Reply(json.dumps({"predictions": [{"t": (t0 + timedelta(hours=6.2 * k)).strftime("%Y-%m-%d %H:%M"), "v": "0.5", "type": "H"}
                                                 for k in range(1400)]}).encode())
    doc = ND.fetch_tides(2026, years=2, opener=opener)
    assert asked == [2026, 2027] and len(doc["hilo"]) == 2800 and doc["hilo"] == sorted(doc["hilo"]) and doc["station"] == "1612668"
    with pytest.raises(ValueError, match="only 2800"):
        ND.fetch_tides(2026, years=3, opener=lambda url, timeout: Reply(b'{"predictions": []}') if "begin_date=2028" in url else opener(url, timeout))


def test_the_tables_are_read_by_the_pinned_manifest_and_their_own_sha(data):
    objs = tables(); _, man = N.load_data(data)
    doc = N.load_tables(fetch_of(objs), man)
    assert [r["id"] for r in doc["regions"]] == ["west", "east"]
    reg = N.open_region(fetch_of(objs), doc, 0)
    assert reg["id"] == "west" and reg["table"].version == "t-1/west" and reg["own"].shape == (ROWS, 241) and len(reg["maps"]) >= 1
    with pytest.raises(ValueError, match="no manifest"):
        N.load_tables(fetch_of({}), man)
    other = dict(objs); other[N.tables_key(SET)] = objs[N.tables_key(SET)] + b" "
    with pytest.raises(ValueError, match="not the pinned one"):
        N.load_tables(fetch_of(other), man)
    broken = dict(objs); broken[doc["regions"][1]["table"]] = b"another table"
    with pytest.raises(ValueError, match="not the manifest's"):
        N.open_region(fetch_of(broken), doc, 1)
    gone = {k: v for k, v in objs.items() if k != doc["regions"][0]["table"]}
    with pytest.raises(ValueError, match="no table in the bucket"):
        N.open_region(fetch_of(gone), doc, 0)


def test_where_two_cores_overlap_the_earlier_region_owns_the_water(data):
    objs = tables(); doc = doc_of(objs)
    west = N.open_region(fetch_of(objs), doc, 0); east = N.open_region(fetch_of(objs), doc, 1)
    xw = np.arange(241) * DX - 1200                                    # a region's own alongshore metres
    assert west["own"][:, (xw >= -1000) & (xw <= 1000)].all() and not west["own"][:, (xw < -1000) | (xw > 1000)].any()
    # east's ref is 1592 m along the beach from west's and 11 m further out: its core -1000..1000 is west's 592..2592, and
    # west owns up to 1000 (east's outermost rows lie beyond west's band, so those stay east's)
    assert not east["own"][:-3, xw <= -620].any() and east["own"][:, (xw >= -570) & (xw <= 1000)].all()
    assert 200 * ROWS * 0.95 < west["own"].sum() <= 201 * ROWS and 158 * ROWS < east["own"].sum() < 162 * ROWS
    for tx, ty, idx in west["maps"]:                                   # no map pixel is given to both regions
        plon, plat = N.tile_pixels(tx, ty); jj, ii = np.nonzero(idx >= 0)
        assert not east["owns"](plon[ii], plat[jj]).any()
        mine = np.zeros((256, 256), bool)
        for ex, ey, eidx in east["maps"]:
            if (ex, ey) == (tx, ty):
                mine = eidx >= 0
        assert not (mine & (idx >= 0)).any()                           # the same pixel of the same tile is never in both pictures
    assert N.in_core(REGIONS[0], DX, *west["table"].lonlat(1200.0, 300.0)) and not N.in_core(REGIONS[0], DX, *west["table"].lonlat(2300.0, 300.0))
    x, y = N.region_xy(REGIONS[1], *east["table"].lonlat(1200.0, 80.0)); assert (x, y) == (pytest.approx(0.0, abs=0.01), pytest.approx(0.0, abs=0.01))


def test_the_tiles_cover_the_band_and_every_pixel_knows_its_cell(data):
    objs = tables(); reg = N.open_region(fetch_of(objs), doc_of(objs), 0); table = reg["table"]
    maps, box = N.tile_maps(table)
    assert 1 <= len(maps) <= 6 and box[0] < box[2] and box[1] < box[3] and -158.2 < box[0] < -158.0 and 21.5 < box[1] < 21.7
    assert maps == sorted(maps, key=lambda m: (m[1], m[0]))
    seen = np.zeros(table.ny * table.nx, bool)
    for tx, ty, idx in maps:
        assert idx.shape == (256, 256)
        j, i = np.nonzero(idx >= 0); k = len(j) // 2; cell = idx[j[k], i[k]]
        plon, plat = N.tile_pixels(tx, ty)
        lon, lat = table.lonlat((cell % table.nx) * DX, (cell // table.nx) * DX)
        assert abs(lon - plon[i[k]]) * 104000 < 8 and abs(lat - plat[j[k]]) * 111000 < 8   # the cell centre is under the pixel
        seen[idx[idx >= 0]] = True
    assert seen[table.dep.reshape(-1) > 0].mean() > 0.6               # z14 pixels (8.9 m) reach most 10 m cells
    owned_px = sum(int((idx >= 0).sum()) for _, _, idx in reg["maps"]); all_px = sum(int((idx >= 0).sum()) for _, _, idx in maps)
    assert 0.75 < owned_px / all_px < 0.9                              # the owned core is 2000 of the grid's 2400 m
    x, y = table.xy(*table.lonlat(1234.0, 567.0)); assert (x, y) == (pytest.approx(1234.0, abs=1e-6), pytest.approx(567.0, abs=1e-6))


def test_a_frame_round_trips_heights_lines_and_the_header(data):
    objs = tables(); reg = N.open_region(fetch_of(objs), doc_of(objs), 0); table = reg["table"]
    trains, buoy, lost = swell_trains()
    png, stats = N.build_frame(table, reg["maps"], reg["box"], trains, buoy, lost, 0.2, reg["own"], reg["owns"])
    assert png[:8] == b"\x89PNG\r\n\x1a\n" and len(png) < 60000
    fr = N.decode_frame(png)
    assert fr["version"] == 1 and fr["tiles"] == len(reg["maps"]) and fr["tide_m"] == pytest.approx(0.2)
    assert fr["buoy"][0] == pytest.approx(2.0, abs=0.05) and fr["buoy"][1] == pytest.approx(14.0, abs=0.6) and fr["buoy"][2] == pytest.approx(330, abs=6)
    assert fr["planes"].max() > 0 and fr["planes"].max() * N.HEIGHT_STEP_M == pytest.approx(stats["hmax_m"], abs=0.2)
    assert 1.5 < stats["hmax_m"] < 3.5                                # 1.27 x 2 m at the line, less inside
    v = fr["vertices"]; assert len(v) == stats["vertices"] > 100
    assert np.all((v["flags"] & 1) == 1) and int(v["flags"][0]) & 128 and np.all(((v["flags"] >> 4) & 7) == 2)   # rights, in the ideal band
    assert np.allclose(v["peel"] & 127, 40, atol=2) and len(set((v["peel"] >> 7).tolist())) == 1 and v["ride"].max() == 255
    assert np.allclose(v["hb"] * N.HEIGHT_STEP_M, 1.27 * 2.0, atol=0.15) and np.all(v["share"] >= 98) and v["score"].max() == 70
    lon = reg["box"][0] + v["x"] / 65535.0 * (reg["box"][2] - reg["box"][0]); lat = reg["box"][1] + v["y"] / 65535.0 * (reg["box"][3] - reg["box"][1])
    gx, gy = table.xy(lon, lat)
    assert np.allclose(0.03 * (gy - 80.0) + 0.2, 1.27 * 2.0 / 0.9, atol=0.5)      # the line sits where set height = 0.9 x (depth + tide)
    assert gx.min() >= 200 - 6 and gx.max() <= 2200 + 6                # only the owned core's 2000 m (grid x 200..2200)
    calm, st0 = N.build_frame(table, reg["maps"], reg["box"], [], None, 0.0, 0.0, reg["own"], reg["owns"])
    assert N.decode_frame(calm)["planes"].max() == 0 and st0["vertices"] == 0


def test_a_line_cut_by_the_regions_edge_becomes_separate_lines():
    t = E.Table([0.07], [0.0], np.ones((1, 4, 30)), np.zeros((1, 4, 30)), np.ones((4, 30)), DX, 0.0)
    own = np.ones((4, 30), bool); own[:, 10:15] = False                # a gap the region does not own
    n = 30; one = np.ones(n)
    lines = dict(x=np.arange(n) * DX, y=np.full(n, 10.0), line=np.concatenate((np.zeros(20), np.ones(10))), score=one, side=one, mixed=0 * one,
                 breaker=one, band=one, peel=one, fwd=one.astype(bool), ride_m=one, hb=one, share=one, depth=one, speed=one, xi=one, ride=one)
    keep = own[0, np.rint(lines["x"] / DX).astype(int)]
    out = N.clip_lines(lines, keep)
    assert out["x"].size == 25 and list(np.unique(out["line"])) == [0.0, 1.0, 2.0]
    assert list(out["x"][out["line"] == 0]) == list(np.arange(0, 10) * DX) and list(out["x"][out["line"] == 1]) == list(np.arange(15, 20) * DX)
    assert list(out["x"][out["line"] == 2]) == list(np.arange(20, 30) * DX)
    assert N.clip_lines(lines, np.ones(n, bool)) is lines and N.clip_lines(lines, None) is lines
    assert N.clip_lines(lines, np.zeros(n, bool))["x"].size == 0


def test_main_publishes_every_regions_frames_the_index_and_the_pointer_last(data):
    st = store_with()
    assert N.main(["--data", data], store=st) == 0
    puts = [k for op, k in st.client.log if op == "put"]
    assert puts[-1] == P.latest_key(N.PREFIX) and puts[-2] == N.index_key(RUN, SET)
    assert puts[:-2] == [N.frame_key(RUN, SET, rid, s) for rid in ("east", "west") for s in STEPS]
    objs = st.client.objects
    assert all(objs[k]["cc"] == P.IMMUTABLE for k in puts[:-1]) and objs[puts[-1]]["cc"] == P.POINTER
    doc = json.loads(objs[N.index_key(RUN, SET)]["body"])
    assert doc["format"] == "nearshore-v2" and doc["run"] == RUN and doc["set"] == SET and doc["steps"] == STEPS and doc["z"] == 14
    assert [r["id"] for r in doc["regions"]] == ["west", "east"] and all(len(r["box"]) == 4 and r["tiles"] and r["vertices_max"] > 50 for r in doc["regions"])
    assert doc["vertex_bytes"] == 10 and doc["band_names"] == ["closeout", "fast", "ideal", "easy", "soft"] and doc["steps_without_tide"] == 0
    ptr = json.loads(objs[P.latest_key(N.PREFIX)]["body"])
    assert ptr | {"published_utc": ""} == {"run": RUN, "index": N.index_key(RUN, SET), "steps": 6, "set": SET, "regions": 2, "published_utc": ""}
    west9 = N.decode_frame(objs[N.frame_key(RUN, SET, "west", 9)]["body"]); west0 = N.decode_frame(objs[N.frame_key(RUN, SET, "west", 0)]["body"])
    assert west9["buoy"][0] > west0["buoy"][0] and west9["tiles"] == len(doc["regions"][0]["tiles"])
    st.client.log.clear()
    assert N.main(["--data", data], store=st) == 0 and st.client.log == []          # already live: nothing written


def test_no_spectrum_is_exit_3_and_a_broken_one_is_a_failure(data, capsys):
    st = store_with(spec=False)
    assert N.main(["--data", data], store=st) == 3 and st.client.log == []
    assert "kept no spectrum" in capsys.readouterr().out
    st.put(N.spec2d_key(RUN), spec_npz(run="2026112518"), "application/octet-stream", P.IMMUTABLE)   # another run's spectrum
    assert N.main(["--data", data], store=st) == 1 and "FAILED" in capsys.readouterr().out
    rec = P.failed_key(N.failed_label(RUN, SET), N.PREFIX)
    assert st.get_json(rec)["attempts"] == 1                                                         # counted
    st.put(N.spec2d_key(RUN), spec_npz(hours=range(0, 2)), "application/octet-stream", P.IMMUTABLE)   # most steps missing
    assert N.main(["--data", data], store=st) == 3 and "failed recently" in capsys.readouterr().out   # an hour before the next try
    st.client.objects.pop(rec)
    assert N.main(["--data", data], store=st) == 1 and st.get_json(rec)["attempts"] == 1
    assert not any(k.startswith(N.PREFIX + "/" + RUN) for k in st.client.objects)                    # nothing of a failed build is published
    empty = P.Store(FakeClient(), "bucket")
    assert N.main(["--data", data], store=empty) == 3                                # the spectral job has no live run


def test_tables_that_are_not_the_pinned_ones_fail_the_build_and_nothing_is_published(data, capsys):
    objs = tables(); doc = doc_of(objs)
    bad = dict(objs); bad[doc["regions"][1]["table"]] = table_npz(dict(REGIONS[1], y0=-90))        # another table under the same key
    st = store_with(objs=bad)
    assert N.main(["--data", data], store=st) == 1 and "not the manifest's" in capsys.readouterr().out
    assert not any(k.startswith(N.PREFIX + "/" + RUN) for k in st.client.objects)
    st = store_with(objs={k: v for k, v in objs.items() if k != N.tables_key(SET)})
    assert N.main(["--data", data], store=st) == 1 and "table set t-1: no manifest" in capsys.readouterr().out


def test_a_bad_data_folder_is_refused(data):
    with open(os.path.join(data, "tides.json"), "ab") as f:
        f.write(b" ")
    assert N.main(["--data", data], store=store_with()) == 1
    assert N.main(["--data", data, "--run", "20261126"], store=store_with()) == 2


def test_an_older_run_is_never_published_over_a_newer_one(data):
    st = store_with()
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": "2026112606", "set": SET, "index": N.index_key("2026112606", SET)}).encode(), "application/json", P.POINTER)
    st.client.log.clear()
    assert N.main(["--data", data], store=st) == 0 and st.client.log == []


def test_another_table_set_never_rewrites_a_runs_objects(tmp_path):
    """The table set is part of the path. The same run from another set is another set of objects."""
    objs = tables("t-2"); data2 = data_folder(tmp_path, "t-2", objs); st = store_with(objs=objs)
    old = {N.frame_key(RUN, SET, "west", 0): b"old frame", N.index_key(RUN, SET): b'{"set":"t-1"}'}
    for k, v in old.items():
        st.put(k, v, "image/png", P.IMMUTABLE)
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": RUN, "set": SET, "index": N.index_key(RUN, SET)}).encode(), "application/json", P.POINTER)
    st.client.log.clear()
    assert N.main(["--data", data2], store=st) == 0
    puts = [k for op, k in st.client.log if op == "put"]
    assert all("/t-2/" in k for k in puts if k.startswith(N.PREFIX + "/" + RUN)) and not set(puts) & set(old)
    assert all(st.client.objects[k]["body"] == v for k, v in old.items())
    ptr = json.loads(st.client.objects[P.latest_key(N.PREFIX)]["body"])
    assert ptr["set"] == "t-2" and ptr["index"] == N.index_key(RUN, "t-2") == f"gfswave/nearshore/v1/{RUN}/t-2/index.json"
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": RUN, "set": SET}).encode(), "application/json", P.POINTER)   # the pointer set back by hand
    st.client.log.clear()
    assert N.main(["--data", data2], store=st) == 0 and st.client.log == []          # already published: nothing rewritten


def test_a_pointer_of_an_older_layout_is_not_this_run_live(data):
    """The pointer must name this run's and this set's own index; another path (an older layout) is replaced."""
    st = store_with()
    old_index = f"{N.PREFIX}/{RUN}/pilot-1/index.json"
    st.put(old_index, b'{"old":true}', "application/json", P.IMMUTABLE)
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": RUN, "table": "pilot-1", "index": old_index}).encode(), "application/json", P.POINTER)
    st.client.log.clear()
    assert N.main(["--data", data], store=st) == 0
    ptr = json.loads(st.client.objects[P.latest_key(N.PREFIX)]["body"])
    assert ptr["index"] == N.index_key(RUN, SET) and st.client.objects[old_index]["body"] == b'{"old":true}'
    assert N.prune(st) == [] and old_index in st.client.objects                      # the run is live: its folder is kept whole


def test_a_failed_build_waits_an_hour_and_stops_after_six(data, capsys):
    st = store_with()
    st.put(N.spec2d_key(RUN), spec_npz(hours=range(0, 2)), "application/octet-stream", P.IMMUTABLE)
    rec = P.failed_key(N.failed_label(RUN, SET), N.PREFIX)
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


def test_dry_run_writes_files_and_publishes_nothing_and_can_read_a_local_copy_of_the_tables(data, tmp_path):
    spec = tmp_path / "s.npz"; spec.write_bytes(spec_npz())
    mirror = tmp_path / "mirror"
    for key, body in tables().items():
        path = mirror / key[len(N.tables_key(SET, "")):]; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
    out = tmp_path / "out"
    assert N.main(["--data", data, "--spec", str(spec), "--tables", str(mirror), "--out", str(out), "--steps", "0,3"]) == 0
    assert sorted(os.listdir(out)) == ["east", "index.json", "west"] and sorted(os.listdir(out / "west")) == ["f000.png", "f003.png"]
    doc = json.load(open(out / "index.json")); assert doc["steps"] == [0, 3] and doc["stats"]["west/3"]["vertices"] > 100
    st = store_with()
    assert N.main(["--data", data, "--dry-run"], store=st) == 0 and st.client.log == []
    assert N.main(["--data", data, "--out", str(out)], store=st) == 2


def test_prune_keeps_four_runs_and_leaves_one_being_written():
    st = P.Store(FakeClient(), "bucket")
    for run in ["2026112400", "2026112406", "2026112412", "2026112418", "2026112500", "2026112506"]:
        st.put(N.frame_key(run, "t", "west", 0), b"x", "image/png", P.IMMUTABLE)
        st.put(N.index_key(run, "t"), b"{}", "application/json", P.IMMUTABLE)
    st.put(N.frame_key("2026112512", "t", "west", 0), b"x", "image/png", P.IMMUTABLE)   # being written
    st.put(P.failed_key(N.failed_label("2026112400", "t"), N.PREFIX), b"{}", "application/json", P.POINTER)   # an old record
    st.put(P.failed_key(N.failed_label("2026112506", "t"), N.PREFIX), b"{}", "application/json", P.POINTER)   # a kept run's
    st.put(N.tables_key("t"), b"{}", "application/json", P.IMMUTABLE)
    st.put(P.latest_key(N.PREFIX), json.dumps({"run": "2026112506"}).encode(), "application/json", P.POINTER)
    assert N.prune(st) == ["2026112400", "2026112406"]
    keys = set(st.client.objects)
    assert N.index_key("2026112412", "t") in keys and N.frame_key("2026112512", "t", "west", 0) in keys and N.tables_key("t") in keys
    assert P.failed_key(N.failed_label("2026112400", "t"), N.PREFIX) not in keys and P.failed_key(N.failed_label("2026112506", "t"), N.PREFIX) in keys


def test_the_spectrum_can_come_straight_from_noaas_archive_within_its_budget():
    from test_gfs_spectral import FakeNoaa, archive, spec_text
    import gfs_winds as W
    run_dt = datetime(2026, 11, 1, 12, tzinfo=timezone.utc)
    arc = archive({"41113": spec_text("41113", steps=range(3)), "51201": spec_text("51201", steps=range(12)), "46026": spec_text("46026", steps=range(3))})
    raw = N.spec_from_archive(run_dt, opener=FakeNoaa({"2026110112": arc}))
    z = np.load(io.BytesIO(raw)); assert str(z["run"]) == "2026110112" and list(z["hours"]) == list(range(12))
    with pytest.raises(ValueError):
        N.spec_from_archive(run_dt, opener=FakeNoaa({"2026110112": archive({"41113": spec_text("41113", steps=range(3))})}))
    with pytest.raises(W.BudgetExceeded):
        N.spec_from_archive(run_dt, opener=FakeNoaa({"2026110112": arc}), budget_s=-1.0)
    assert N.ARCHIVE_BUDGET_S <= 600 and W.STEPS == 385


def test_past_the_tide_files_end_steps_use_mean_sea_level_and_the_summary_warns(data):
    objs = tables(); tides, man = N.load_data(data)
    short = dict(tides, hilo=[hl for hl in tides["hilo"] if hl[0] < RUN_DT.timestamp() + 7 * 3600])   # the last turn is at +6.4 h
    spec = np.load(io.BytesIO(spec_npz()))
    frames, doc, stats = N.build_run(doc_of(objs), fetch_of(objs), short, spec, RUN_DT, STEPS, log=lambda s: None)
    assert doc["steps_without_tide"] == 1 and sorted({s for _, s in frames}) == STEPS and stats[("west", 9)]["tide_m"] == 0.0 and stats[("west", 0)]["tide_m"] != 0.0
    assert doc["tides_end_utc"].startswith("2026-11-2")
    assert "ended" in N.tide_warning(short, RUN_DT) and "Rebuild the data folder" in N.tide_warning(short, RUN_DT)
    far = dict(tides, hilo=tides["hilo"] + [[RUN_DT.timestamp() + 400 * 86400, 0.3]])
    assert N.tide_warning(far, RUN_DT) is None
    soon = dict(tides, hilo=tides["hilo"] + [[RUN_DT.timestamp() + 40 * 86400, 0.3]])
    assert "days of forecasts left" in N.tide_warning(soon, RUN_DT)
    assert "empty" in N.tide_warning({"hilo": []}, RUN_DT)


def test_the_vertex_cap_drops_whole_lines_the_weakest_first(data, monkeypatch):
    objs = tables(); reg = N.open_region(fetch_of(objs), doc_of(objs), 0); table, box = reg["table"], reg["box"]
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


def test_the_workflow_runs_the_nearshore_step_after_the_tables_and_never_fails_them():
    text = open(os.path.join(ROOT, ".github", "workflows", "model-gfsspec.yml"), encoding="utf-8").read()
    spectral = text.index("python tools/model_frames/gfs_spectral.py"); near = text.index("python tools/model_frames/nearshore.py")
    assert spectral < near and "--data nearshore-data/ns --from-archive" in text
    step = text[text.index("- name: Nearshore data"):]
    assert step.count("continue-on-error: true") == 2 and step.rstrip().endswith("exit 0")
    assert re.search(r"ref: [0-9a-f]{40} ", step) and "path: nearshore-data" in step            # the data at a pinned commit
    assert 'NSDATA: ${{ steps.nsdata.outcome }}' in step and 'if [ "$NSDATA" != "success" ]' in step
    assert step.count("        if: steps.env.outcome == 'success'\n") == 2                    # the repo's rule for every step
    assert 'NEARSHORE_ONLY: ${{ github.event.inputs.nearshore_only }}' in text and 'if [ "$NEARSHORE_ONLY" = "true" ]' in text
    assert "timeout-minutes: 20" in text.split("- name: Publish the nearshore analysis", 1)[1]
    uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
    assert uses and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses)
