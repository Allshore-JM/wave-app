"""The nearshore table build (plan section 109): the regions, the grid frame, the unit swell bin, the SWAN input, the
chunks and the region's assembled table. No SWAN and no network here: the runs themselves are checked by the workflow's
smoke test and by the offshore-height rule at assembly."""
import io
import json
import math
import os
import re
import sys

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
import nearshore_build as B  # noqa: E402
import nearshore_engine as E  # noqa: E402
import publish as P  # noqa: E402
from test_gfs_spectral import FakeClient  # noqa: E402

CFG = B.config()


def test_the_regions_run_from_kaena_to_kahuku_and_the_chunks_share_the_bins():
    ids = [r["id"] for r in CFG["regions"]]
    assert ids == ["laniakea", "sunset", "haleiwa", "mokuleia", "kaena", "kahuku"] and CFG["set"] == "ns-1" and CFG["dx"] == 10.0
    lons = [r["ref"][0] for r in CFG["regions"]]
    assert min(lons) < -158.24 and max(lons) > -158.02                 # Kaena Point's side to Kahuku Point's
    for r in CFG["regions"]:
        assert r["x"][0] + 150 <= r["core"][0] < r["core"][1] <= r["x"][1] - 150, r["id"]   # a margin against the grid's ends
        assert 200 <= r["rows"] <= 500 and r["y0"] < 0 and (r["x"][1] - r["x"][0]) % 10 == 0
    allb = B.bins(CFG); assert len(allb) == 156 == len(CFG["freqs"]) * len(CFG["dirs"])
    parts = [B.chunk_bins(CFG, k) for k in range(CFG["chunks"])]
    assert sorted(b for p in parts for b in p) == sorted(allb) and all(len(p) == 26 for p in parts)
    assert all(len({b[0] for b in p}) == len(CFG["freqs"]) for p in parts), "every chunk holds every period (like run times)"
    plan = [{"region": r, "chunk": k} for r in ids for k in range(6)]
    assert len(plan) == 36


def test_laniakeas_frame_is_the_pilot_grid():
    """The pilot table was built on this grid (corners from its grid file): the new frame lands on the same nodes."""
    fr = B.frame(CFG, B.region_of(CFG, "laniakea"))
    assert (fr["mx"], fr["my"], fr["alpc"], fr["dx"]) == (560, 750, 45.0, 10.0)
    lon, lat = B.nodes_lonlat(fr)
    assert lon.shape == (751, 561)
    for (j, i), (wlon, wlat) in (((0, 0), (-158.10035270369568, 21.601544365225834)), ((0, 560), (-158.06204651442022, 21.637155634774167)),
                                 ((750, 560), (-158.11334944648559, 21.684849299347825)), ((750, 0), (-158.15165563576105, 21.649238029799495))):
        assert abs(lon[j, i] - wlon) * 103400 < 0.2 and abs(lat[j, i] - wlat) * 111200 < 0.2      # within 20 cm
    t = E.Table([0.07], [0.0], np.ones((1, 3, 3)), np.zeros((1, 3, 3)), np.ones((3, 3)), 10.0, fr["alpc"])
    t.geo = dict(lon0=fr["lon0"], lat0=fr["lat0"], xpc=fr["xpc"], ypc=fr["ypc"])
    glon, glat = t.lonlat(2300.0, 500.0)                               # the engine's own frame: Laniakea's marker is at x 2300, y 500
    assert abs(glon - -158.08804) < 2e-6 and abs(glat - 21.61935) < 2e-6


def test_the_unit_bin_is_a_one_metre_swell_in_one_frequency_and_one_sector():
    f, d, E2 = B.unit_spectrum(0.0667, 315.0)
    flow, fhigh, f2, d2 = B.spectral_grid(0.0667)
    assert len(f) == 5 and f[2] == pytest.approx(0.0667, rel=2e-3) and len(d) == 72
    m0 = (E2.sum(1) * f * math.log(fhigh / flow) / B.MSC).sum() * 5.0  # SWAN's own integration on its log grid
    assert 4 * math.sqrt(m0) == pytest.approx(1.0, abs=1e-9)
    assert np.count_nonzero(E2.sum(1)) == 1 and E2[2].argmax() in (62, 63, 64)
    lit = d[E2[2] > 0]; assert len(lit) == 3 and np.all(np.abs(((lit - 315.0 + 180) % 360) - 180) <= 7.5)
    f0, d0, E0 = B.unit_spectrum(0.04, 0.0); lit = d0[E0[2] > 0]
    assert sorted(lit) == [0.0, 5.0, 355.0]                            # the sector wraps through north
    text = B.spec_text(f, d, E2); rows = text.splitlines()
    assert rows[0] == "SWAN   1" and "AFREQ" in rows and "NDIR" in rows and rows[rows.index("FACTOR") + 1].strip().endswith(("E-05", "E-06", "E-04", "E-07"))
    assert max(int(v) for v in rows[-3].split()) == 99999


def test_the_swan_input_is_linear_with_the_offshore_side_and_both_ends_fed():
    fr = B.frame(CFG, B.region_of(CFG, "sunset")); text = B.input_text(fr, 0.0625)
    assert "CGRID REGULAR" in text and f" {fr['mx']} {fr['my']} CIRCLE 72 " in text and "EXC -5.0" in text and "READINP BOTTOM 1. 'bot.dat' 3 0 FREE" in text
    assert text.count("BOUNDSPEC SEGMENT IJ") == 3 and f"BOUNDSPEC SEGMENT IJ 0 {fr['my']} {fr['mx']} {fr['my']} CONSTANT FILE 'bnd0.sp2' 1" in text
    for off in ("OFF WINDG", "OFF QUAD", "OFF WCAP", "OFF BREA"):
        assert off in text
    assert "HSIGN" in text and "DIR" in text and text.rstrip().endswith("STOP") and "GEN3" not in text and "TRIAD" not in text
    assert (fr["mx"], fr["my"]) == (810, 750)


def test_depth_codes_follow_the_bathymetry_tiles():
    d = B.decode_depth(np.array([0, 1, 1001, 41001, 41011, 62001, 62002, 65401, 65402]))
    assert np.isnan(d[0]) and np.isnan(d[-1])
    assert np.allclose(d[1:-1], [-10.0, 0.0, 400.0, 401.0, 2500.0, 2502.5, 11000.0])
    assert B.tile_xy(-158.088, 21.619, 14) == (997, 7183)


# ---------------------------------------------------------------- assembling a region

SMALL = dict(CFG, set="t-1", freqs=[0.05, 0.1], dirs=[330, 45], chunks=2,
             regions=[dict(id="tiny", ref=[-158.1, 21.6], alpha=45.0, x=[-100, 100], core=[-50, 50], y0=-50, rows=4)])


def write_chunk(tmp, k, bins, boundary=1.0, setname="t-1", smoke=False, dep=None, drop=()):
    fr = B.frame(SMALL, SMALL["regions"][0]); nx = fr["mx"] + 1
    dep = np.full((4, nx), 250, np.int16) if dep is None else dep
    keep = [b for b in bins if b not in drop]
    meta = [dict(f0=b[0], th0=b[1], seconds=10.0 + n, rc=0, boundary=boundary if not callable(boundary) else boundary(b), iterations=6) for n, b in enumerate(keep)]
    np.savez_compressed(os.path.join(tmp, f"tiny_{B.chunk_name(k)}"), set=np.array(setname), region=np.array("tiny"), chunk=np.array(k), smoke=np.array(smoke),
                        bins=np.array(keep, float).reshape(-1, 2), K=np.stack([np.full((4, nx), 100 + int(b[0] * 100) + (b[1] == 45), np.uint8) for b in keep]),
                        DIR=np.stack([np.full((4, nx), int(b[1] * 256 / 360), np.uint8) for b in keep]), dep_cm=dep,
                        frame=np.array(json.dumps({q: fr[q] for q in ("lon0", "lat0", "alpc", "dx", "mx", "my", "xpc", "ypc")})),
                        meta=np.array(json.dumps(meta)), grid=np.array("{}"))


def test_a_regions_chunks_become_the_table_the_engine_reads(tmp_path):
    tmp = str(tmp_path)
    for k in range(2):
        write_chunk(tmp, k, B.chunk_bins(SMALL, k))
    path, facts = B.assemble(SMALL, "tiny", tmp, tmp, log=lambda s: None)
    t = E.Table.from_npz(path)
    assert t.version == "t-1/tiny" and list(t.F) == [0.05, 0.1] and list(t.D) == [330.0, 45.0] and t.dep.shape == (4, 21)
    code = np.load(path)["K"]; assert [int(c[0, 0]) for c in code] == [105, 106, 110, 111]      # F x D order whatever the chunks' order
    assert facts["sha256"] == __import__("hashlib").sha256(open(path, "rb").read()).hexdigest() and facts["bins"] == 4
    assert facts["table"] == "static/nearshore/t-1/tiny/table.npz" and facts["core"] == [-50, 50] and facts["shape"] == [4, 21]
    assert t.geo["lon0"] == -158.1 and t.alpc == 45.0


def test_assembly_refuses_a_missing_bin_a_wrong_height_another_grid_and_a_smoke_chunk(tmp_path):
    tmp = str(tmp_path); c0, c1 = B.chunk_bins(SMALL, 0), B.chunk_bins(SMALL, 1)
    quiet = dict(log=lambda s: None)
    write_chunk(tmp, 0, c0); write_chunk(tmp, 1, c1, drop=(c1[0],))
    with pytest.raises(ValueError, match="1 bins missing"):
        B.assemble(SMALL, "tiny", tmp, tmp, **quiet)
    assert {b[1] for b in c0} == {330} and {b[1] for b in c1} == {45}
    write_chunk(tmp, 0, c0, boundary=0.9); write_chunk(tmp, 1, c1)
    with pytest.raises(ValueError, match="offshore height off"):
        B.assemble(SMALL, "tiny", tmp, tmp, **quiet)
    write_chunk(tmp, 0, c0); write_chunk(tmp, 1, c1, boundary=0.8)               # a sector that runs along the coast may fall short
    B.assemble(SMALL, "tiny", tmp, tmp, **quiet)
    write_chunk(tmp, 1, c1, dep=np.full((4, 21), 260, np.int16))
    with pytest.raises(ValueError, match="another grid"):
        B.assemble(SMALL, "tiny", tmp, tmp, **quiet)
    write_chunk(tmp, 1, c1, smoke=True)
    with pytest.raises(ValueError, match="not a whole chunk"):
        B.assemble(SMALL, "tiny", tmp, tmp, **quiet)
    write_chunk(tmp, 1, c1, setname="t-2")
    with pytest.raises(ValueError, match="not a whole chunk"):
        B.assemble(SMALL, "tiny", tmp, tmp, **quiet)
    assert B.check_boundary([dict(f0=0.05, th0=240, boundary=0.7), dict(f0=0.05, th0=315, boundary=1.019), dict(f0=0.05, th0=0, boundary=1.03)]) == [(0.05, 0, 1.03)]


def test_a_published_set_is_never_rewritten(tmp_path, capsys):
    st = P.Store(FakeClient(), "bucket")
    st.put(B.manifest_key(CFG), b"{}", "application/json", P.IMMUTABLE)
    (tmp_path / "laniakea_table.npz").write_bytes(b"x")
    assert B.main(["put-table", "--region", "laniakea", "--dir", str(tmp_path)], store=st) == 2
    assert B.main(["put-manifest", "--dir", str(tmp_path)], store=st) == 2
    assert B.table_key(CFG, "laniakea") not in st.client.objects and "immutable" in capsys.readouterr().out
    assert B.chunk_key(CFG, "kaena", 3) == "static/nearshore/ns-1/build/kaena/c3.npz" and B.manifest_key(CFG) == "static/nearshore/ns-1/manifest.json"


def test_a_bad_config_is_refused(tmp_path):
    for patch in (dict(set="NS 1"), dict(regions=CFG["regions"] + [CFG["regions"][0]]), dict(regions=[dict(CFG["regions"][0], id="Bad Id")])):
        p = tmp_path / "c.json"; p.write_text(json.dumps(dict(CFG, **patch)))
        with pytest.raises(ValueError):
            B.config(str(p))
    with pytest.raises(ValueError):
        B.region_of(CFG, "waikiki")


def test_the_workflow_is_manual_pinned_and_publishes_only_when_asked():
    text = open(os.path.join(ROOT, ".github", "workflows", "nearshore-table.yml"), encoding="utf-8").read()
    head = text.split("\npermissions:", 1)[0]
    assert "workflow_dispatch:" in head and "schedule:" not in head and "push:" not in head
    assert "swan=41.51" in text and "micromamba-releases/releases/download/2.10.0-0/micromamba-linux-64" in text
    uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
    assert len(uses) >= 5 and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses)
    assert "if: env.UPLOAD == 'true'" in text and "github.event.inputs.upload == 'true'" in text.split("  assemble:", 1)[1]
    assert "put-manifest" in text.split("  assemble:", 1)[1] and "fail-fast: false" in text and "timeout-minutes: 345" in text
    assert "contents: read" in text and "secrets.R2_BUCKET" in text and "secrets." not in text.split("  build:", 1)[0]
