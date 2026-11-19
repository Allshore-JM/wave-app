"""The RRFS overlay frames (plan section 99 D1): the region boxes, the region index (the tables' grid priority), the
frame values against the cell records and an independent eccodes read, the merged manifest's newest-source rule, the
manifest + pointer write (idempotent, repaired by a quiet tick), the frame prune, the keep rule and the size guard."""
import io
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import encode as E  # noqa: E402
import publish as P  # noqa: E402
import rrfs as J  # noqa: E402
import test_rrfs as T  # noqa: E402  (its fixtures and fakes)

needs_ec = T.needs_ec
_one_step = T._one_step                                          # autouse: the fixture records are step 6 only


def _png(data):
    return np.array(Image.open(io.BytesIO(data)))


# ------------------------------------------------------------------------------------------------------- the boxes

@pytest.mark.parametrize("region", list(J.REGIONS))
def test_region_box_shape_and_node_cap(region):
    g = J.region_grid(region)
    assert g["cols"] * g["rows"] <= J.MAX_REGION_NODES == 1 << 20
    assert g["dlat"] < 0 < g["dlon"] and -180 <= g["lon0"] < 180
    assert g["registration"] == "center" and g["lon_periodic"] is False
    assert region in J.G.GRIDS                                    # a region is drawn from the RRFS grid of its name


@pytest.mark.parametrize("region", list(J.REGIONS))
def test_region_box_holds_every_interior_node_with_a_node_to_spare(region):
    """No point on the box's outermost rows / columns (sampled at a fifth of a node) is served by the region's grid,
    while the box has served nodes; the served set is one connected patch, so all of it lies inside the box with a node
    to spare. And the box is tight: a served node within 3 nodes of each edge."""
    g = J.region_grid(region)
    lat_n, lon_w = g["lat0"], g["lon0"]
    lat_s = lat_n + (g["rows"] - 1) * g["dlat"]
    lon_e = lon_w + (g["cols"] - 1) * g["dlon"]
    d = g["dlon"] / 5
    lons = np.arange(lon_w, lon_e + d / 2, d)
    lats = np.arange(lat_s, lat_n + d / 2, d)
    ring = [(np.full(lons.shape, lat_n), lons), (np.full(lons.shape, lat_s), lons),
            (lats, np.full(lats.shape, lon_w)), (lats, np.full(lats.shape, lon_e))]
    for la, lo in ring:
        assert not J._interior(region, la, lo)[0].any()
    flat, mask = J.region_index(region)
    assert mask.sum() > 1000
    rows, cols = np.nonzero(mask | _later_mask(region))
    assert rows.min() <= 3 and cols.min() <= 3 and rows.max() >= g["rows"] - 4 and cols.max() >= g["cols"] - 4


def _later_mask(region):
    """The nodes of the region's own grid that an earlier grid serves (masked out of the region; still its extent)."""
    g = J.region_grid(region)
    lat = g["lat0"] + g["dlat"] * np.arange(g["rows"])
    lon = g["lon0"] + g["dlon"] * np.arange(g["cols"])
    la, lo = np.meshgrid(lat, lon, indexing="ij")
    return J._interior(region, la, lo)[0]


@pytest.mark.parametrize("region", list(J.REGIONS))
def test_region_index_is_rrfs_grid_locate(region):
    """Every drawn node is the cell locate() gives (the tables' cell), and a node locate() gives another grid (or
    none) is not drawn: CONUS before Alaska where they overlap."""
    g = J.region_grid(region)
    flat, mask = J.region_index(region)
    rng = np.random.default_rng(5)
    nx = J.G.GRIDS[region]["Nx"]
    for _ in range(1500):
        r, c = int(rng.integers(0, g["rows"])), int(rng.integers(0, g["cols"]))
        lat, lon = g["lat0"] + r * g["dlat"], g["lon0"] + c * g["dlon"]
        lon = (lon + 180) % 360 - 180
        hit = J.G.locate(lat, lon)
        if mask[r, c]:
            assert hit is not None and hit[0] == region and flat[r, c] == hit[1] * nx + hit[2], (r, c, hit)
        else:
            assert hit is None or hit[0] != region, (r, c, hit)


def test_alaska_leaves_the_conus_overlap_to_conus():
    lat, lon = 48.4, -124.7                                       # off Washington: in both grids (rrfs_grid.ORDER)
    assert J.G.cell("ak", lat, lon) is not None and J.G.locate(lat, lon)[0] == "conus"
    for region, want in (("ak", False), ("conus", True)):
        g = J.region_grid(region)
        r = round((lat - g["lat0"]) / g["dlat"])
        c = round(((lon - g["lon0"]) % 360) / g["dlon"])
        assert bool(J.region_index(region)[1][r, c]) is want


def test_alaska_crosses_the_date_line():
    g = J.region_grid("ak")
    assert g["lon0"] + (g["cols"] - 1) * g["dlon"] > 180          # one box from 150 E across 180 to 94 W
    r = round((52.0 - g["lat0"]) / g["dlat"])                     # the Aleutians at 178 W and 178 E are both drawn
    for lon in (182.0, 178.0):
        c = round((lon - g["lon0"]) / g["dlon"])
        assert J.region_index("ak")[1][r, c]


# ------------------------------------------------------------------------------------------------ values + encoding

def test_region_values_from_the_cell_records():
    flat, mask = J.region_index("hi")
    n = J.G.GRIDS["hi"]["Nx"] * J.G.GRIDS["hi"]["Ny"]
    rec = np.zeros(n, dtype=J.CELL_DTYPE)
    rec["spd"] = 1234
    rec["dir"] = 2705
    k = flat[mask][0]
    rec["spd"][k], rec["dir"][k] = J.MISSING, J.MISSING           # missing speed -> missing node
    k2 = flat[mask][-1]
    rec["spd"][k2], rec["dir"][k2] = 0, J.MISSING                 # a calm: speed 0, no direction
    v = J.region_values("hi", rec)
    assert np.isnan(v["wind"][~mask]).all() and np.isnan(v["wdir"][~mask]).all()
    inside = mask & (flat != k) & (flat != k2)
    assert np.allclose(v["wind"][inside], 12.34) and np.allclose(v["wdir"][inside], 270.5)
    assert np.isnan(v["wind"][mask & (flat == k)]).all()
    assert (v["wind"][mask & (flat == k2)] == 0).all() and np.isnan(v["wdir"][mask & (flat == k2)]).all()


def test_frame_encoding_is_the_gfs_frames():
    vals = np.array([[0.0, 10.0, np.nan], [30.0, 61.73, 100.0]])
    q = _png(J.encode_region_frame(vals, "wind"))
    lo, hi = E.FIELDS["wind"]["lo"], E.FIELDS["wind"]["hi"]
    assert q.dtype == np.uint8 and q.shape == (2, 3) and q[0, 2] == 0 and q[1, 2] == 255
    back = E.dequantize(q, lo, hi)
    assert np.nanmax(np.abs(back[:, :2] - vals[:, :2])) <= E.quantum(lo, hi) / 2 + 1e-9
    d = _png(J.encode_region_frame(np.array([[0.0, 5.0, 359.9, 5.0, 180.0, 5.0, np.nan], [9.0] * 7]), "wdir"))
    assert d.shape == (1, 4) and list(d[0]) == [1, 1, 128, 0]      # half: every second node; circular: 0 = 360 = code 1
    assert J.region_grid_half("hi")["cols"] == (J.region_grid("hi")["cols"] + 1) // 2 and J.region_grid_half("ak")["dlon"] == 0.14


@needs_ec
def test_hawaii_run_frames_equal_an_independent_eccodes_read(tmp_path):
    files, docs = T._build(T.RUN, tmp_path)
    cyc = "2026111712"
    fs = docs[J.frameset_key(cyc)]
    assert fs["regions"] == {"hi": ["wind", "wdir"]} and fs["template"] == J.frame_template(cyc) and "dropped" not in fs
    assert fs["precip"] == {"regions": ["hi"], "range": [0.1, 75.0, "full", "rrfs-precip-v1"]} and fs["ranges"] == J.frame_ranges()
    g = J.region_grid("hi")
    assert _check_precip_against_eccodes(_png(open(files[J.frame_key(cyc, "hi", "precip", 6)], "rb").read()), g) > 0   # G68a P2-2
    q = _png(open(files[J.frame_key(cyc, "hi", "wind", 6)], "rb").read())
    qd = _png(open(files[J.frame_key(cyc, "hi", "wdir", 6)], "rb").read())
    gh = J.region_grid_half("hi")
    assert q.shape == (g["rows"], g["cols"]) and qd.shape == (gh["rows"], gh["cols"])
    U, V = T.ecc("hi_f006", "UGRD"), T.ecc("hi_f006", "VGRD")
    lo, hi = E.FIELDS["wind"]["lo"], E.FIELDS["wind"]["hi"]
    rng = np.random.default_rng(3)
    checked = 0
    while checked < 300:
        r, c = int(rng.integers(0, g["rows"])), int(rng.integers(0, g["cols"]))
        hit = J.G.locate(g["lat0"] + r * g["dlat"], g["lon0"] + c * g["dlon"])
        if hit is None:
            assert q[r, c] == 0 and (r % 2 or c % 2 or qd[r // 2, c // 2] == 0)
            continue
        _, j, i = hit
        s = math.hypot(U[j, i], V[j, i])
        assert abs(float(E.dequantize(q[r:r + 1, c:c + 1], lo, hi)[0, 0]) - s) <= E.quantum(lo, hi) / 2 + 0.006
        if s > 0.5:
            want = (math.degrees(math.atan2(-U[j, i], -V[j, i])) + 360) % 360
            if r % 2 or c % 2:                                    # the direction lives on every second node
                checked += 1
                continue
            got = (int(qd[r // 2, c // 2]) - 1) * 360 / 254
            assert abs((got - want + 180) % 360 - 180) <= 360 / 508 + 0.06
        checked += 1


def test_build_grid_writes_every_steps_frames(tmp_path, monkeypatch):
    """20 steps through the encoders (workers=2): each step's frame is its own records."""
    monkeypatch.setattr(J, "CYCLE_STEPS", list(range(1, 21)))
    T._fake_fields(monkeypatch)
    frames = {}
    J.build_grid(T.CYC, "hi", {}, str(tmp_path), 1e18, log=lambda s: None, workers=2, frames=frames)
    assert sorted(frames) == list(range(1, 21))
    mask = J.region_index("hi")[1]
    for s in (1, 20):
        q = _png(open(frames[s]["wind"][0], "rb").read())
        assert (q[~mask] == 0).all() and (q[mask] == q[mask][0]).all() and q[mask][0] > 1   # 1 m/s everywhere
        d = _png(open(frames[s]["wdir"][0], "rb").read())
        assert (d[mask[::2, ::2]] == 1 + round(270 * 254 / 360) % 254).all()             # u = 1, v = 0: from 270 (half grid)


def test_an_oversize_frame_drops_the_sources_frames(tmp_path):
    p = tmp_path / "f.png"
    p.write_bytes(b"x")
    frames = {"hi": {6: {"wind": (str(p), J.FRAME_MAX_BYTES + 1), "wdir": (str(p), 10), "precip": (str(p), 10)}}}
    files, doc = J.frameset(T.RUN, frames, log=lambda s: None)
    assert files == {} and doc["regions"] == {} and "over" in doc["dropped"]
    assert J.frames_entry({J.frameset_key("2026111712"): doc}, "2026111712") is None
    frames["hi"][6]["wind"] = (str(p), 100)
    files, doc = J.frameset(T.RUN, frames, log=lambda s: None)
    assert set(files) == {J.frame_key("2026111712", "hi", f, 6) for f in ("wind", "wdir", "precip")}
    frames["hi"][6]["precip"] = (str(p), J.FRAME_MAX_BYTES + 1)            # an oversize precip frame drops the precip only (G68a P3-3)
    files, doc = J.frameset(T.RUN, frames, log=lambda s: None)
    assert set(files) == {J.frame_key("2026111712", "hi", f, 6) for f in ("wind", "wdir")}
    assert doc["regions"] == {"hi": ["wind", "wdir"]} and "precip" not in doc and "precip of hi dropped" in doc["dropped"]


# --------------------------------------------------------------------------------------------- the merged manifest

def _src(cycle, kind, frames=True, grids=None, precip=False):
    steps = list(range(0, 85)) if kind == "run" else list(range(1, 19))
    fr = {"doc": J.frameset_key(cycle), "template": J.frame_template(cycle),
          "regions": {r: ["wind", "wdir"] for r in J.REGIONS},
          "grids": grids or {r: dict(J.region_grid(r)) for r in J.REGIONS}, "ranges": J.frame_ranges()} if frames else None
    if fr and precip:
        fr["precip"] = {"regions": sorted(J.REGIONS), "range": list(J.PRECIP_RANGE)}
    return {"cycle": cycle, "kind": kind, "steps": steps, "frames": fr}


NOW = datetime(2026, 11, 17, 16, 0, tzinfo=timezone.utc)


def test_manifest_newest_source_per_hour():
    ptr = {"sources": [_src("2026111713", "cycle"), _src("2026111712", "run"), _src("2026111711", "cycle")]}
    m = J.frames_manifest(ptr, NOW)
    assert m["schema"] == "rrfs-frames-v1" and m["run"] == "2026111713" and m["run_utc"] == "2026-11-17T13:00:00Z"
    fr = m["frames"]
    assert fr[0]["valid_utc"] == "2026-11-17T14:00:00Z" and fr[0]["step"] == 1 and fr[0]["src"] == "2026111713"
    assert [f["src"] for f in fr[:18]] == ["2026111713"] * 18
    assert fr[18]["valid_utc"] == "2026-11-18T08:00:00Z" and fr[18]["src"] == "2026111712" and fr[18]["src_step"] == 20
    assert fr[-1]["valid_utc"] == "2026-11-21T00:00:00Z" and fr[-1]["step"] == 83 and len(fr) == 83
    steps = [f["step"] for f in fr]
    assert steps == sorted(set(steps)) and steps == list(range(1, 84))           # hourly, no gap
    assert fr[0]["files"]["conus"]["wind"] == "rrfs/v1/frames/2026111713/conus/wind/f001.png"
    assert fr[-1]["files"]["ak"]["wdir"] == "rrfs/v1/frames/2026111712/ak/wdir/f084.png"
    assert [s["cycle"] for s in m["sources"]] == ["2026111713", "2026111712"]   # 11Z adds no hour
    assert set(m["regions"]) == set(J.REGIONS) and m["regions"]["ak"]["grid"] == J.region_grid("ak")
    assert m["regions"]["hi"]["fields"]["wind"]["lo"] == E.FIELDS["wind"]["lo"]
    assert m["regions"]["hi"]["fields"]["wind"]["hi"] == E.FIELDS["wind"]["hi"]
    assert m["regions"]["hi"]["fields"]["wdir"]["circular"] is True
    json.dumps(m, allow_nan=False)


def test_manifest_without_the_newest_cycles_frames_uses_the_one_before():
    ptr = {"sources": [_src("2026111713", "cycle", frames=False), _src("2026111712", "run"),
                       _src("2026111711", "cycle")]}
    m = J.frames_manifest(ptr, NOW)
    assert m["run"] == "2026111712" and m["frames"][0]["valid_utc"] == "2026-11-17T12:00:00Z"
    assert m["frames"][0]["src"] == "2026111712" and m["frames"][0]["step"] == 0     # the run is newer than 11Z
    assert {f["src"] for f in m["frames"]} == {"2026111712"}


def test_manifest_a_run_newer_than_the_cycles_wins_its_hours():
    ptr = {"sources": [_src("2026111712", "run"), _src("2026111711", "cycle")]}
    m = J.frames_manifest(ptr, NOW)
    assert m["run"] == "2026111712" and len(m["frames"]) == 85


def test_manifest_leaves_stale_sources_out():
    ptr = {"sources": [_src("2026111713", "cycle"), _src("2026111712", "run")]}
    later = datetime(2026, 11, 17, 21, 30, tzinfo=timezone.utc)   # 13Z cycle 8.5 h old: out; run 9.5 h: in
    m = J.frames_manifest(ptr, later)
    assert m["run"] == "2026111712" and {f["src"] for f in m["frames"]} == {"2026111712"}
    assert J.frames_manifest(ptr, datetime(2026, 11, 18, 7, tzinfo=timezone.utc)) is None   # run 19 h old
    assert J.frames_manifest({"sources": []}, NOW) is None


# ---------------------------------------------------------------------------------------- sync, prune, keep, main

def test_sync_writes_manifest_then_pointer_and_is_idempotent():
    st = T.store()
    ptr = {"sources": [_src("2026111712", "run")]}
    new, prev = J.sync_frames(st, ptr, NOW, log=lambda s: None)
    assert prev is None and new["run"] == "2026111712" and new["frames"] == 85 and new["complete"] is True
    assert new["encoding"] == E.ENCODING and new["sources"] == ["2026111712"]
    puts = [k for op, k in st.client.log if op == "put"]
    assert puts == [new["manifest"], "rrfs/v1/frames/latest.json"]
    assert new["manifest"].startswith("rrfs/v1/frames/2026111712/manifest-")
    assert st.client.objects["rrfs/v1/frames/latest.json"]["cc"] == P.POINTER
    assert st.client.objects[new["manifest"]]["cc"] == P.IMMUTABLE
    again, prev2 = J.sync_frames(st, ptr, NOW + timedelta(minutes=10), log=lambda s: None)
    assert again == new and prev2 == new and len(st.client.log) == 2             # nothing written
    ptr2 = {"sources": [_src("2026111713", "cycle")] + ptr["sources"]}
    new2, prev3 = J.sync_frames(st, ptr2, NOW + timedelta(minutes=20), log=lambda s: None)
    assert prev3 == new and new2["run"] == "2026111713" and new2["manifest"] != new["manifest"]


def test_prune_frames_keeps_kept_sources_and_named_manifests():
    st = T.store()
    keys = ["rrfs/v1/frames/2026111706/hi/wind/f001.png",       # no longer kept anywhere: gone
            "rrfs/v1/frames/2026111711/hi/wind/f001.png",       # named only by the live manifest's sources: kept
            "rrfs/v1/frames/2026111712/hi/wind/f001.png",       # a kept source: kept
            "rrfs/v1/frames/2026111712/manifest-20261117T150000Z.json",   # old manifest: gone
            "rrfs/v1/frames/2026111712/manifest-20261117T160000Z.json",   # the previous pointer's: kept
            "rrfs/v1/frames/2026111713/manifest-20261117T170000Z.json",   # the new pointer's: kept
            "rrfs/v1/frames/2026111799/hi/wind/f001.png",       # newer than the pointer: a build writing
            "rrfs/v1/frames/latest.json"]
    for k in keys:
        st.put(k, b"x", "image/png", P.IMMUTABLE)
    ptr = {"newest": "2026111713", "sources": [_src("2026111713", "cycle", frames=False), _src("2026111712", "run")]}
    fptr = {"manifest": keys[5], "sources": ["2026111712", "2026111711"]}
    fprev = {"manifest": keys[4], "sources": ["2026111712"]}
    dead = J.prune_frames(st, ptr, None, fptr, fprev)
    assert sorted(dead) == sorted([keys[0], keys[3]])
    assert set(st.client.objects) == set(keys) - {keys[0], keys[3]}


def test_keep_sources_drops_frames_after_two_runs_and_three_cycles():
    srcs = []
    for h in range(24):
        srcs.append(dict(_src(f"20261117{h:02d}", "run" if h % 6 == 0 else "cycle"), stations="s", cells={"hi": "c"}))
    kept = J.keep_sources(srcs)
    assert [s["cycle"][-2:] for s in kept if s["frames"]] == ["23", "22", "21", "18", "12"]


def _fake_build_frames(c, work, coords, deadline, get=None, log=None, grids=None):
    """T._fake_build plus one tiny frame per region and field of the first step, and the frame set."""
    files, docs = T._fake_build(c, work, coords, deadline, get, log, grids)
    cyc = J.cycle_name(c)
    frames = {}
    for r in ("hi",):
        frames[r] = {}
        for s in J.steps_of(c):
            frames[r][s] = {}
            for f in J.FRAME_FIELDS:
                p = os.path.join(work, f"{r}-{f}-{s}.png")
                with open(p, "wb") as fh:
                    fh.write(b"png" + cyc.encode())
                frames[r][s][f] = (p, 9)
    ff, docs[J.frameset_key(cyc)] = J.frameset(c, frames, log=lambda s: None)
    files.update(ff)
    return files, docs


def test_main_publishes_frames_then_the_pointers(monkeypatch):
    st = T.store()
    monkeypatch.setattr(J, "build", _fake_build_frames)
    assert J.main([], head=T.BOTH, store=st, now=T.NOW, coords=T.COORDS, log=lambda s: None) == 0
    puts = [k for op, k in st.client.log if op == "put"]
    ptr = T._ptr(st)
    assert all(s["frames"]["regions"] == {"hi": ["wind", "wdir"]} for s in ptr["sources"])
    assert all(s["frames"]["precip"]["regions"] == ["hi"] for s in ptr["sources"])
    fkey = "rrfs/v1/frames/2026111713/hi/wind/f006.png"
    assert st.client.objects[fkey]["ct"] == "image/png" and st.client.objects[fkey]["cc"] == P.IMMUTABLE
    first_ptr = puts.index("rrfs/v1/latest.json")
    first_fptr = puts.index("rrfs/v1/frames/latest.json")
    assert puts.index(fkey) < puts.index(J.frameset_key("2026111713")) < first_ptr < first_fptr
    fp = st.get_json("rrfs/v1/frames/latest.json")
    # one step each (the fixtures): the run's f006 (18Z) is before the cycle's first hour (19Z), so only 13Z is used
    assert fp["sources"] == ["2026111713"] and fp["frames"] == 1
    m = st.get_json(fp["manifest"])
    assert m["frames"][0]["files"]["hi"]["wind"] == fkey
    n = len(st.client.log)
    assert J.main([], head=T.BOTH, store=st, now=T.NOW, coords=T.COORDS, log=lambda s: None) == 0   # nothing new
    assert len([1 for op, _ in st.client.log[n:] if op == "put"]) == 0


def test_a_quiet_tick_repairs_a_missing_manifest(monkeypatch):
    st = T.store()
    monkeypatch.setattr(J, "build", _fake_build_frames)
    assert J.main([], head=T.BOTH, store=st, now=T.NOW, coords=T.COORDS, log=lambda s: None) == 0
    st.client.objects.pop("rrfs/v1/frames/latest.json")
    assert J.main([], head=T.BOTH, store=st, now=T.NOW, coords=T.COORDS, log=lambda s: None) == 0
    assert st.get_json("rrfs/v1/frames/latest.json")["sources"] == ["2026111713"]


def test_dropped_frames_publish_the_tables_and_fail_loud(monkeypatch):
    st = T.store()

    def big(c, work, coords, deadline, get=None, log=None, grids=None):
        files, docs = _fake_build_frames(c, work, coords, deadline, get, log, grids)
        cyc = J.cycle_name(c)
        files = {k: v for k, v in files.items() if not k.startswith(J.FRAMES_PREFIX + "/")}
        docs[J.frameset_key(cyc)] = dict(docs[J.frameset_key(cyc)], regions={}, dropped="1 frames over 2097152 B")
        return files, docs

    monkeypatch.setattr(J, "build", big)
    monkeypatch.setattr(J, "_stale", lambda *a, **k: None)          # the exit 1 must come from the drop (G67a P3-9a)
    summary = []
    monkeypatch.setattr(J, "_summary", summary.append)
    assert J.main(["--cycle", "2026111713"], head=lambda u: True, store=st, now=T.NOW, coords=T.COORDS,
                  log=lambda s: None) == 1
    assert any(x.startswith("WARNING: RRFS 2026111713 published without some overlay frames") for x in summary)
    ptr = T._ptr(st)
    assert ptr["sources"][0]["cycle"] == "2026111713" and ptr["sources"][0]["frames"] is None
    assert ptr["sources"][0]["cells"]                               # the tables' products are live
    assert st.get_json("rrfs/v1/frames/latest.json") is None        # no frames anywhere: no manifest


def test_prune_after_publish_keeps_the_live_manifests_frames(monkeypatch):
    st = T.store()
    monkeypatch.setattr(J, "build", _fake_build_frames)
    for h in (7, 8, 9, 10, 11):
        c = datetime(2026, 11, 17, h, tzinfo=timezone.utc)
        J.main(["--cycle", J.cycle_name(c)], head=lambda u: True, store=st, now=T.NOW, coords=T.COORDS,
               log=lambda s: None)
    fp = st.get_json("rrfs/v1/frames/latest.json")
    m = st.get_json(fp["manifest"])
    for fr in m["frames"]:
        for fs in fr["files"].values():
            for k in fs.values():
                assert k in st.client.objects
    assert not any(k.startswith("rrfs/v1/frames/2026111707/") and k.endswith(".png") for k in st.client.objects)
    assert "rrfs/v1/frames/2026111707/frames.json" in st.client.objects     # a kept source's record stays (G67a P3-6)
    manifests = [k for k in st.client.objects if "/manifest-" in k]
    assert len(manifests) <= 2


# --------------------------------------------------------------------------------------------- G67a fixes, pinned

def test_signature_is_the_manifests_identity_when_the_run_repeats():
    """G67a P1-1: a run published after a newer hourly cycle keeps `run` (the cycle) but changes hours +19..+84; the
    signature (the browser's adoption key) changes, the publish time does not count."""
    a = J.frames_manifest({"sources": [_src("2026111713", "cycle"), _src("2026111706", "run")]}, NOW)
    b = J.frames_manifest({"sources": [_src("2026111713", "cycle"), _src("2026111712", "run"), _src("2026111706", "run")]}, NOW)
    assert a["run"] == b["run"] == "2026111713"
    assert J._signature(a) != J._signature(b)
    assert J._signature(a) == J._signature(dict(a, published_utc="x"))
    st = T.store()
    p1, _ = J.sync_frames(st, {"sources": [_src("2026111713", "cycle"), _src("2026111706", "run")]}, NOW, log=lambda s: None)
    p2, _ = J.sync_frames(st, {"sources": [_src("2026111713", "cycle"), _src("2026111712", "run")]}, NOW + timedelta(minutes=1),
                          log=lambda s: None)
    assert p1["run"] == p2["run"] and p1["signature"] != p2["signature"] and p1["manifest"] != p2["manifest"]
    assert st.get_json(p2["manifest"])["signature"] == p2["signature"]


def test_a_source_drawn_on_another_grid_is_never_paired_with_todays():
    """G67a P1-2: after a grid change, an older source's frames (drawn on the old grid) are left out; so is a source
    whose frames have other ranges; a region dropped from REGIONS is never named."""
    old = {r: dict(J.region_grid(r)) for r in J.REGIONS}
    old["conus"]["cols"] = 1000
    m = J.frames_manifest({"sources": [_src("2026111713", "cycle"), _src("2026111712", "run", grids=old)]}, NOW)
    assert {f["src"] for f in m["frames"] if "conus" in f["files"]} == {"2026111713"}
    late = [f for f in m["frames"] if f["src"] == "2026111712"]
    assert late and all("conus" not in f["files"] and "hi" in f["files"] for f in late)
    r = _src("2026111712", "run")
    r["frames"]["ranges"] = {"wind": [0, 50], "wdir": [0, 360]}
    assert J.frames_manifest({"sources": [r]}, NOW) is None
    r = _src("2026111712", "run")
    r["frames"]["regions"]["gone"] = ["wind", "wdir"]
    r["frames"]["grids"]["gone"] = J.region_grid("hi")
    m = J.frames_manifest({"sources": [r]}, NOW)
    assert "gone" not in m["regions"] and all("gone" not in f["files"] for f in m["frames"])
    assert set(m["regions"]) == set(J.REGIONS)


def test_every_source_stale_withdraws_the_pointer_once():
    """G67a P2-3: no usable source: the overlay pointer says complete false (no manifest), written once."""
    st = T.store()
    ptr = {"sources": [_src("2026111712", "run")]}
    p1, _ = J.sync_frames(st, ptr, NOW, log=lambda s: None)
    assert p1["complete"] is True
    later = datetime(2026, 11, 18, 7, tzinfo=timezone.utc)                     # the run 19 h old
    p2, prev = J.sync_frames(st, ptr, later, log=lambda s: None)
    assert prev == p1 and p2["complete"] is False and "manifest" not in p2 and p2["sources"] == []
    n = len(st.client.log)
    p3, _ = J.sync_frames(st, ptr, later, log=lambda s: None)
    assert p3 == p2 and len(st.client.log) == n
    dead = J.prune_frames(st, {"newest": "2026111712", "sources": []}, None, p2, None)
    assert p1["manifest"] in dead                                              # the withdrawn manifest goes


def test_a_corrupt_overlay_pointer_is_rewritten():
    st = T.store()
    st.put("rrfs/v1/frames/latest.json", b"{not json", "application/json", P.POINTER)
    p, prev = J.sync_frames(st, {"sources": [_src("2026111712", "run")]}, NOW, log=lambda s: None)
    assert prev is None and p["complete"] is True and st.get_json("rrfs/v1/frames/latest.json") == p


def test_the_stale_limits_are_inclusive():
    """G67a P3-9c: a cycle exactly 8 h and a run exactly 18 h after their cycle time are still used; a second more is not."""
    for kind, cyc, h in (("cycle", "2026111713", 8), ("run", "2026111712", 18)):
        t = J._cdt(cyc) + timedelta(hours=h)
        assert J.frames_manifest({"sources": [_src(cyc, kind)]}, t) is not None
        assert J.frames_manifest({"sources": [_src(cyc, kind)]}, t + timedelta(seconds=1)) is None


def test_a_failing_tick_still_syncs_the_overlay(monkeypatch):
    """G67a P2-4: a tick whose build fails (exit 1) still withdraws / rewrites the overlay pointer; a sync failure after
    a publish is never a failure record of the live source (P2-5)."""
    st = T.store()
    _ptrs = [_src("2026111712", "run")]
    st.put("rrfs/v1/latest.json", json.dumps({"newest": "2026111712", "sources": [dict(s, stations="x") for s in _ptrs]}).encode(),
           "application/json", P.POINTER)

    def broken(*a, **k):
        raise ValueError("a bad record")

    monkeypatch.setattr(J, "build", broken)
    monkeypatch.setattr(J, "_summary", lambda line: None)
    assert J.main(["--cycle", "2026111713"], head=lambda u: True, store=st, now=NOW, coords=T.COORDS, log=lambda s: None) == 1
    assert st.get_json("rrfs/v1/frames/latest.json")["sources"] == ["2026111712"]

    st2 = T.store()
    monkeypatch.setattr(J, "build", _fake_build_frames)

    def boom(*a, **k):
        raise RuntimeError("R2 read timed out")

    monkeypatch.setattr(J, "sync_frames", boom)
    assert J.main(["--cycle", "2026111713"], head=lambda u: True, store=st2, now=T.NOW, coords=T.COORDS, log=lambda s: None) == 1
    assert not any(k.startswith("rrfs/v1/failed/") for k in st2.client.objects), "no failure record of a live source"
    assert T._ptr(st2)["sources"][0]["cycle"] == "2026111713"


def test_each_steps_frame_is_its_own_step(tmp_path, monkeypatch):
    """G67a P3-9b: u = step m/s at step s: frame f<s> decodes to s m/s (the encoder never writes a neighbour's records)."""
    monkeypatch.setattr(J, "CYCLE_STEPS", list(range(1, 21)))
    g = J.G.GRIDS["hi"]
    shape = (g["Ny"], g["Nx"])
    monkeypatch.setattr(J, "grid_lonlat", lambda msg, grid: (np.zeros(shape), np.zeros(shape)))
    monkeypatch.setattr(J, "fetch_step", lambda c, grid, step, get=None: {n: (n, step) for n, _ in J.wanted(c, step)})
    monkeypatch.setattr(J, "decode", lambda tok, grid, expect=None: np.full(shape, float(tok[1]) if tok[0] == "u" else 0.0))
    frames = {}
    J.build_grid(T.CYC, "hi", {}, str(tmp_path), 1e18, log=lambda s: None, workers=2, frames=frames)
    mask = J.region_index("hi")[1]
    lo, hi = E.FIELDS["wind"]["lo"], E.FIELDS["wind"]["hi"]
    for s in range(1, 21):
        q = _png(open(frames[s]["wind"][0], "rb").read())
        v = E.dequantize(q[mask][:1], lo, hi)[0]
        assert abs(v - s) <= E.quantum(lo, hi) / 2 + 1e-9, (s, v)


def test_one_oversize_region_drops_that_region_only(tmp_path):
    p = tmp_path / "f.png"
    p.write_bytes(b"x")
    frames = {"hi": {6: {"wind": (str(p), J.FRAME_MAX_BYTES + 1), "wdir": (str(p), 10), "precip": (str(p), 10)}},
              "pr": {6: {"wind": (str(p), 10), "wdir": (str(p), 10), "precip": (str(p), 10)}}}
    files, doc = J.frameset(T.RUN, frames, log=lambda s: None)
    assert doc["regions"] == {"pr": ["wind", "wdir"]} and "hi" in doc["dropped"] and doc["precip"]["regions"] == ["pr"]
    assert set(files) == {J.frame_key("2026111712", "pr", f, 6) for f in ("wind", "wdir", "precip")}
    assert doc["grids"] == {"pr": J.region_grid("pr")} and doc["ranges"] == J.frame_ranges()


@pytest.mark.parametrize("region", list(J.REGIONS))
def test_cover_holds_every_drawn_node_and_nothing_else(region):
    """The coverage mask (the browser fetches a region only near its data): every drawn node's 1-deg cell is set, and
    every set cell has a drawn node."""
    c = J.region_cover(region)
    bits = bin(int(c["hex"], 16))[2:].zfill(len(c["hex"]) * 4)[:c["rows"] * c["cols"]]
    cells = np.array([b == "1" for b in bits]).reshape(c["rows"], c["cols"])
    g = J.region_grid(region)
    mask = J.region_index(region)[1]
    lat = g["lat0"] + g["dlat"] * np.arange(g["rows"])
    lon = g["lon0"] + g["dlon"] * np.arange(g["cols"])
    jj, ii = np.nonzero(mask)
    ri = np.minimum(((c["lat0"] - lat[jj]) // c["res"]).astype(int), c["rows"] - 1)
    ci = np.minimum(((lon[ii] - c["lon0"]) // c["res"]).astype(int), c["cols"] - 1)
    want = np.zeros_like(cells)
    want[ri, ci] = True
    assert (want == cells).all()
    if region == "ak":                                                       # off California: Alaska has no data
        r, k = int((c["lat0"] - 41.0) // 1), int((235.5 - c["lon0"]) // 1)       # 124.5 W in the 0-360 frame
        assert not cells[r, k]


def test_regions_come_from_every_source_used_and_files_change_the_signature():
    top = _src("2026111713", "cycle")
    top["frames"]["regions"] = {"hi": ["wind", "wdir"]}                       # the newest source has Hawaii only
    m = J.frames_manifest({"sources": [top, _src("2026111712", "run")]}, NOW)
    assert set(m["regions"]) == set(J.REGIONS)                               # the run's later hours name all four
    assert set(m["frames"][0]["files"]) == {"hi"} and set(m["frames"][-1]["files"]) == set(J.REGIONS)
    m2 = json.loads(json.dumps(m))
    m2["frames"][5]["files"]["hi"]["wind"] = "rrfs/v1/frames/2026111799/hi/wind/f001.png"
    assert J._signature(m2) != J._signature(m)


def test_sources_with_the_first_full_resolution_direction_are_never_paired():
    """The first production sources (2026-11-18 04-06 UTC) published wdir on the full grid: their frame ranges lack the
    resolution, so a manifest never names them beside today's half-resolution direction (the browser would refuse the
    size)."""
    old = _src("2026111803", "cycle")
    old["frames"]["ranges"] = {"wind": [J.FRAME_DEFS["wind"]["lo"], J.FRAME_DEFS["wind"]["hi"]], "wdir": [0.0, 360.0]}
    m = J.frames_manifest({"sources": [old, _src("2026111800", "run")]}, NOW)
    assert {f["src"] for f in m["frames"]} == {"2026111800"}
    assert J.frame_ranges()["wdir"][2] == "half" and m["regions"]["conus"]["fields"]["wdir"]["resolutions"] == ("half",)
    assert m["regions"]["ak"]["grid_half"] == J.region_grid_half("ak")


# ------------------------------------------------------------------------------------- precipitation (plan section 102 A)

def _check_precip_against_eccodes(q, g, n=600):
    """The precip frame at random nodes vs an independent eccodes read of the step's APCP, the type flags and lightning:
    the type range and the level of the amount (from the cells' 0.01-mm rounding of the amount)."""
    AP, CR, SN, FR, IP, LT = (T.ecc("hi_f006", v) for v in ("APCP", "CRAIN", "CSNOW", "CFRZR", "CICEP", "LTNG"))
    rng = np.random.default_rng(11)
    wet = checked = 0
    while checked < n:
        r, c = int(rng.integers(0, g["rows"])), int(rng.integers(0, g["cols"]))
        hit = J.G.locate(g["lat0"] + r * g["dlat"], g["lon0"] + c * g["dlon"])
        checked += 1
        if hit is None:
            assert q[r, c] == 0
            continue
        _, j, i = hit
        a = AP[j, i]
        if a < 0.1:
            assert q[r, c] == J.PRECIP_DRY
            continue
        if LT[j, i] > 0:
            t = "thunder"
        elif FR[j, i] >= 0.5 or IP[j, i] >= 0.5:
            t = "ice"
        elif CR[j, i] >= 0.5 and SN[j, i] >= 0.5:
            t = "mix"
        elif SN[j, i] >= 0.5:
            t = "snow"
        else:
            t = "rain"
        c0, c1 = J.PRECIP_CODES[t]
        assert c0 <= q[r, c] <= c1, (t, q[r, c])
        assert int(q[r, c]) - c0 == int(J.precip_level(round(a * 100) / 100.0))
        wet += 1
    return wet


def test_precip_levels_pinned_by_number():
    """G68a P2-2: a few levels by number (not through precip_level itself): 0.1 -> 0, 1 -> 17, 10 -> 34, 75 -> 49."""
    assert [int(J.precip_level(v)) for v in (0.1, 0.5, 1.0, 2.5, 10.0, 25.0, 75.0, 1000.0)] == [0, 12, 17, 24, 34, 41, 49, 49]
    assert int(J.precip_level(float("nan"))) == 0                                      # G68a P3-2


def test_precip_code_table_round_trips():
    assert J.PRECIP_CODES == {"rain": [2, 51], "snow": [52, 101], "mix": [102, 151], "ice": [152, 201],
                              "thunder": [202, 251]}
    assert J.precip_value(0) != J.precip_value(0) and J.precip_value(252) != J.precip_value(252)   # NaN: never written
    assert J.precip_value(J.PRECIP_DRY) == 0.0
    for name, (c0, c1) in J.PRECIP_CODES.items():
        assert c1 - c0 + 1 == J.PRECIP_PER
        assert J.precip_value(c0) == pytest.approx(0.1) and J.precip_value(c1) == pytest.approx(75.0)
        for code in range(c0, c1 + 1):
            assert c0 + int(J.precip_level(J.precip_value(code))) == code          # code -> value -> code
    ratio = J.precip_value(3) / J.precip_value(2)
    assert ratio == pytest.approx(750 ** (1 / 49)) and 1.14 < ratio < 1.15         # 14.5 % steps
    v = np.exp(np.linspace(np.log(0.1), np.log(75), 5000))                         # any amount: within half a step
    back = np.array([J.precip_value(2 + k) for k in J.precip_level(v)])
    assert np.max(np.abs(np.log(back / v))) <= math.log(ratio) / 2 + 1e-12
    assert int(J.precip_level(0.1)) == 0 and int(J.precip_level(500.0)) == 49 and int(J.precip_level(0.04)) == 0


def test_precip_codes_from_cell_records():
    apcp = np.array([0, 5, 10, 10, 10, 10, 10, 7500, 60000, J.MISSING, 10, 10, J.MISSING], dtype="<u2")
    typ = np.array([J.TYPE_NONE, J.TYPE_NONE, J.TYPE_RAIN, J.TYPE_SNOW, J.TYPE_MIX, J.TYPE_ICE, J.TYPE_THUNDER,
                    J.TYPE_RAIN, J.TYPE_SNOW, J.TYPE_RAIN, J.MISSING8, J.TYPE_RAIN, J.TYPE_NONE], dtype="u1")
    mask = np.ones(13, bool)
    mask[11] = False
    q = J.precip_codes(apcp, typ, mask)
    assert q.dtype == np.uint8
    assert list(q) == [1, 1, 2, 52, 102, 152, 202, 51, 101, 0, 0, 0, 0]   # last: "none" with an unknown amount = missing (G68a P3-1)


def test_region_values_carry_the_precip_codes():
    flat, mask = J.region_index("hi")
    n = J.G.GRIDS["hi"]["Nx"] * J.G.GRIDS["hi"]["Ny"]
    rec = np.zeros(n, dtype=J.CELL_DTYPE)
    rec["spd"] = 100
    rec["apcp"] = 250                                                       # 2.5 mm
    rec["type"] = J.TYPE_SNOW
    k = flat[mask][0]
    rec["type"][k] = J.TYPE_NONE
    v = J.region_values("hi", rec)["precip"]
    assert v.dtype == np.uint8 and (v[~mask] == 0).all()
    want = 52 + int(J.precip_level(2.5))
    assert (v[mask & (flat != k)] == want).all() and (v[mask & (flat == k)] == J.PRECIP_DRY).all()
    q = _png(J.encode_region_frame(v, "precip"))
    assert q.shape == v.shape and (q == v).all()                            # full resolution, the codes as they are


def test_a_runs_f000_has_no_precip_frame(tmp_path, monkeypatch):
    monkeypatch.setattr(J, "RUN_STEPS", [0, 1, 2])
    T._fake_fields(monkeypatch)
    frames = {}
    J.build_grid(T.RUN, "hi", {}, str(tmp_path), 1e18, log=lambda s: None, workers=2, frames=frames)
    assert set(frames[0]) == {"wind", "wdir"} and set(frames[1]) == {"wind", "wdir", "precip"}
    mask = J.region_index("hi")[1]
    q1 = _png(open(frames[1]["precip"][0], "rb").read())                    # hour 1 of the run: 0.1 mm of rain
    q2 = _png(open(frames[2]["precip"][0], "rb").read())                    # 0.2 mm
    assert (q1[mask] == 2).all() and (q1[~mask] == 0).all()
    assert (q2[mask] == 2 + int(J.precip_level(0.2))).all()
    files, doc = J.frameset(T.RUN, {"hi": frames}, log=lambda s: None)
    cyc = "2026111712"
    assert J.frame_key(cyc, "hi", "precip", 0) not in files and J.frame_key(cyc, "hi", "wind", 0) in files
    assert J.frame_key(cyc, "hi", "precip", 1) in files and doc["regions"] == {"hi": ["wind", "wdir"]}
    assert doc["precip"] == {"regions": ["hi"], "range": [0.1, 75.0, "full", "rrfs-precip-v1"]}


def test_cycle_precip_frames_are_each_hours_amount(tmp_path, monkeypatch):
    """The hourly cycle's amount is the difference of its totals (s mm at step s): frame f<s> holds the level of s mm."""
    monkeypatch.setattr(J, "CYCLE_STEPS", list(range(1, 7)))
    T._fake_fields(monkeypatch)
    frames = {}
    J.build_grid(T.CYC, "hi", {}, str(tmp_path), 1e18, log=lambda s: None, workers=2, frames=frames)
    mask = J.region_index("hi")[1]
    for s in range(1, 7):
        q = _png(open(frames[s]["precip"][0], "rb").read())
        assert (q[mask] == 2 + int(J.precip_level(float(s)))).all(), s


def test_manifest_names_precip_and_skips_a_runs_f000():
    run = _src("2026111712", "run", precip=True)
    m = J.frames_manifest({"sources": [run]}, NOW)
    assert m["frames"][0]["step"] == 0 and m["frames"][0]["files"]["hi"] == {
        "wind": "rrfs/v1/frames/2026111712/hi/wind/f000.png", "wdir": "rrfs/v1/frames/2026111712/hi/wdir/f000.png"}
    assert "precip" not in m["frames"][0]                                    # a run's f000: no hour behind it
    assert m["frames"][1]["precip"]["hi"] == "rrfs/v1/frames/2026111712/hi/precip/f001.png"
    assert all(set(fs) == {"wind", "wdir"} for f in m["frames"] for fs in f["files"].values())   # G68a P0-1: `files` never changes
    d = m["regions"]["conus"]["fields"]["precip"]
    assert d["kind"] == "typed" and d["encoding"] == "rrfs-precip-v1" and d["types"] == J.PRECIP_CODES
    assert d["dry"] == 1 and d["lo"] == 0.1 and d["hi"] == 75.0 and d["per_type"] == 50 and d["resolutions"] == ("full",)
    json.dumps(m, allow_nan=False)


def test_sources_built_before_precip_keep_their_wind():
    """Plan section 102 A1: a source published before the precip field (no precip block) stays in the manifest for wind
    + direction; its hours name no precip. A precip of another encoding, or of a region whose wind is unusable, is left
    out; precip of a region never names it in `files`."""
    new = _src("2026111713", "cycle", precip=True)
    old = _src("2026111712", "run")
    m = J.frames_manifest({"sources": [new, old]}, NOW)
    assert {f["src"] for f in m["frames"]} == {"2026111713", "2026111712"} and len(m["frames"]) == 83
    assert all(set(f["precip"]) == set(J.REGIONS) for f in m["frames"] if f["src"] == "2026111713")
    assert all("precip" not in f for f in m["frames"] if f["src"] == "2026111712")
    assert "precip" in m["regions"]["hi"]["fields"]
    odd = _src("2026111712", "run", precip=True)
    odd["frames"]["precip"]["range"] = [0.1, 75.0, "full", "rrfs-precip-v0"]
    m = J.frames_manifest({"sources": [odd]}, NOW)
    assert all("precip" not in f for f in m["frames"]) and "precip" not in m["regions"]["hi"]["fields"]
    only_old = J.frames_manifest({"sources": [old]}, NOW)                  # no source with precip: no precip field at all
    assert "precip" not in only_old["regions"]["hi"]["fields"]
    grid = _src("2026111712", "run", precip=True)
    grid["frames"]["grids"]["conus"] = dict(J.region_grid("conus"), cols=1000)   # conus wind unusable: no conus precip either
    m = J.frames_manifest({"sources": [grid]}, NOW)
    assert all("conus" not in f.get("precip", {}) for f in m["frames"]) and "conus" not in m["regions"]


def test_old_browsers_and_an_old_job_never_see_the_precip():
    """G68a P0-1 / P2-1: the frame set's `regions` and `ranges` and every manifest frame's `files` are exactly what the
    job wrote before section 102 (the live browser refuses unknown keys in `files`; a rolled-back job compares the whole
    ranges), so shipping the job first, keeping old tabs, or rolling the job back changes nothing for the wind."""
    assert J.frame_ranges() == {"wind": [J.FRAME_DEFS["wind"]["lo"], J.FRAME_DEFS["wind"]["hi"], "full"],
                                "wdir": [0.0, 360.0, "half"]}
    m = J.frames_manifest({"sources": [_src("2026111713", "cycle", precip=True), _src("2026111712", "run", precip=True)]}, NOW)
    for f in m["frames"]:
        assert set(f) <= {"valid_utc", "step", "src", "src_step", "files", "precip"}
        assert all(set(v) == {"wind", "wdir"} for v in f["files"].values())
    plain = J.frames_manifest({"sources": [_src("2026111713", "cycle"), _src("2026111712", "run")]}, NOW)
    sig_plain = J._signature(plain)
    body = [plain["run_utc"], plain["regions"], [[f["valid_utc"], f["step"], f["files"]] for f in plain["frames"]]]
    import hashlib
    assert sig_plain == hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()[:16]   # unchanged without precip
    assert J._signature(m) != J._signature(dict(m, frames=[dict(f, precip={}) for f in m["frames"]]))
