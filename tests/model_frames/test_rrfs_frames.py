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
    d = _png(J.encode_region_frame(np.array([[0.0, 359.9, 180.0, np.nan]]), "wdir"))
    assert list(d[0]) == [1, 1, 128, 0]                           # circular: 0 and 360 share code 1


@needs_ec
def test_hawaii_run_frames_equal_an_independent_eccodes_read(tmp_path):
    files, docs = T._build(T.RUN, tmp_path)
    cyc = "2026111712"
    fs = docs[J.frameset_key(cyc)]
    assert fs["regions"] == {"hi": ["wind", "wdir"]} and fs["template"] == J.frame_template(cyc) and "dropped" not in fs
    g = J.region_grid("hi")
    q = _png(open(files[J.frame_key(cyc, "hi", "wind", 6)], "rb").read())
    qd = _png(open(files[J.frame_key(cyc, "hi", "wdir", 6)], "rb").read())
    assert q.shape == qd.shape == (g["rows"], g["cols"])
    U, V = T.ecc("hi_f006", "UGRD"), T.ecc("hi_f006", "VGRD")
    lo, hi = E.FIELDS["wind"]["lo"], E.FIELDS["wind"]["hi"]
    rng = np.random.default_rng(3)
    checked = 0
    while checked < 300:
        r, c = int(rng.integers(0, g["rows"])), int(rng.integers(0, g["cols"]))
        hit = J.G.locate(g["lat0"] + r * g["dlat"], g["lon0"] + c * g["dlon"])
        if hit is None:
            assert q[r, c] == 0 and qd[r, c] == 0
            continue
        _, j, i = hit
        s = math.hypot(U[j, i], V[j, i])
        assert abs(float(E.dequantize(q[r:r + 1, c:c + 1], lo, hi)[0, 0]) - s) <= E.quantum(lo, hi) / 2 + 0.006
        if s > 0.5:
            want = (math.degrees(math.atan2(-U[j, i], -V[j, i])) + 360) % 360
            got = (int(qd[r, c]) - 1) * 360 / 254
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
        assert (d[mask] == 1 + round(270 * 254 / 360) % 254).all()                         # u = 1, v = 0: from 270


def test_an_oversize_frame_drops_the_sources_frames(tmp_path):
    p = tmp_path / "f.png"
    p.write_bytes(b"x")
    frames = {"hi": {6: {"wind": (str(p), J.FRAME_MAX_BYTES + 1), "wdir": (str(p), 10)}}}
    files, doc = J.frameset(T.RUN, frames, log=lambda s: None)
    assert files == {} and doc["regions"] == {} and "over" in doc["dropped"]
    assert J.frames_entry({J.frameset_key("2026111712"): doc}, "2026111712") is None
    frames["hi"][6]["wind"] = (str(p), 100)
    files, doc = J.frameset(T.RUN, frames, log=lambda s: None)
    assert set(files) == {J.frame_key("2026111712", "hi", f, 6) for f in ("wind", "wdir")}


# --------------------------------------------------------------------------------------------- the merged manifest

def _src(cycle, kind, frames=True, grids=None):
    steps = list(range(0, 85)) if kind == "run" else list(range(1, 19))
    fr = {"doc": J.frameset_key(cycle), "template": J.frame_template(cycle),
          "regions": {r: ["wind", "wdir"] for r in J.REGIONS},
          "grids": grids or {r: J.region_grid(r) for r in J.REGIONS}, "ranges": J.frame_ranges()} if frames else None
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
    frames = {"hi": {6: {"wind": (str(p), J.FRAME_MAX_BYTES + 1), "wdir": (str(p), 10)}},
              "pr": {6: {"wind": (str(p), 10), "wdir": (str(p), 10)}}}
    files, doc = J.frameset(T.RUN, frames, log=lambda s: None)
    assert doc["regions"] == {"pr": ["wind", "wdir"]} and "hi" in doc["dropped"]
    assert set(files) == {J.frame_key("2026111712", "pr", f, 6) for f in ("wind", "wdir")}
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
