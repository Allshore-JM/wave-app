"""Coast data builder (tools/coast/build_coast.py): GSHHG reading, ring normalisation, cell
clipping, the coast-v1 encoding and the --check gate. Synthetic inputs only (no network)."""
import io
import json
import os
import re
import struct
import sys
import zipfile

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "coast"))

import build_coast as B  # noqa: E402

Q = B.Q


def gshhg_record(pid, level, pts_deg, area_km2=10.0, greenwich=0, p=1):
    pts = np.round(np.asarray(pts_deg, np.float64) * 1e6).astype(np.int64)
    flag = level | (15 << 8) | (greenwich << 16) | (1 << 24) | (p << 26)
    xs, ys = pts[:, 0], pts[:, 1]
    head = B.GSHHG_HEADER.pack(pid, len(pts), flag, int(xs.min()), int(xs.max()), int(ys.min()), int(ys.max()),
                               int(round(area_km2 * 10 ** p)), int(round(area_km2 * 10 ** p)), -1, -1)
    return head + b"".join(struct.pack(">ii", int(x), int(y)) for x, y in pts)


def winding(px, py, rings, scale=1.0):
    """Winding number of points (px, py) over rings given as (x, y) arrays (divided by scale)."""
    w = np.zeros(len(px), np.int64)
    for rx, ry in rings:
        x = np.asarray(rx, np.float64) / scale; y = np.asarray(ry, np.float64) / scale
        xn, yn = np.roll(x, -1), np.roll(y, -1)
        for a, b, c, d in zip(x, y, xn, yn):
            up = (b <= py) & (d > py); down = (b > py) & (d <= py)
            cross = (c - a) * (py - b) - (px - a) * (d - b)
            w += (up & (cross > 0)).astype(np.int64) - (down & (cross < 0)).astype(np.int64)
    return w


def star(cx, cy, r0, r1, n=24, seed=0):
    rng = np.random.default_rng(seed)
    ang = np.linspace(0, 2 * np.pi, n, endpoint=False)
    rad = np.where(np.arange(n) % 2 == 0, r1, r0) * rng.uniform(0.8, 1.0, n)
    return cx + rad * np.cos(ang), cy + rad * np.sin(ang)


# ---------------------------------------------------------------- reader

def test_read_gshhg_levels_area_scale_and_greenwich():
    buf = (gshhg_record(0, 1, [(200, 20), (201, 20), (201, 21)], area_km2=12.5, greenwich=2, p=2)
           + gshhg_record(1, 2, [(10, 10), (11, 10), (11, 11)])            # lake: skipped
           + gshhg_record(2, 6, [(10, -80), (11, -80), (11, -79)])         # grounding line: skipped
           + gshhg_record(3, 5, [(10, -70), (11, -70), (11, -69)]))        # ice front: kept
    polys = list(B.read_gshhg(buf))
    assert [p["id"] for p in polys] == [0, 3]
    assert polys[0]["level"] == 1 and polys[0]["greenwich"] == 2 and abs(polys[0]["area_km2"] - 12.5) < 1e-9
    assert np.allclose(polys[0]["x"], [200, 201, 201]) and np.allclose(polys[0]["y"], [20, 20, 21])
    with pytest.raises(ValueError):
        list(B.read_gshhg(buf[:-3]))                                       # truncated points


# ---------------------------------------------------------------- normalisation

def test_normalize_ring_stored_0_360_across_greenwich_is_unwrapped_not_split():
    x = np.array([359.0, 1.0, 1.0, 359.0]); y = np.array([10.0, 10.0, 12.0, 12.0])
    rings = B.normalize_ring(x, y)
    assert len(rings) == 1
    rx, ry = rings[0]
    assert np.isclose(rx.min(), -1.0) and np.isclose(rx.max(), 1.0)
    assert B.signed_area(rx, ry) > 0 and np.isclose(B.signed_area(rx, ry), 4.0)


def test_normalize_ring_splits_at_the_antimeridian_and_keeps_area():
    x = np.array([178.0, 182.0, 182.0, 178.0, 178.0]); y = np.array([60.0, 60.0, 62.0, 62.0, 60.0])  # explicitly closed
    rings = B.normalize_ring(x[::-1], y[::-1])                             # clockwise input
    assert len(rings) == 2
    assert all(r[0].min() >= -180 and r[0].max() <= 180 for r in rings)
    assert all(B.signed_area(*r) > 0 for r in rings)
    assert np.isclose(sum(B.signed_area(*r) for r in rings), 8.0)
    east = [r for r in rings if r[0].max() == 180.0][0]; west = [r for r in rings if r[0].min() == -180.0][0]
    assert np.isclose(east[0].min(), 178.0) and np.isclose(west[0].max(), -178.0)


def test_normalize_ring_closes_a_ring_that_winds_around_the_south_pole():
    lon = np.linspace(180.0, -180.0, 73); lat = -70.0 + 2.0 * np.sin(np.radians(lon) * 3)
    rings = B.normalize_ring(lon, lat)
    assert len(rings) == 1
    rx, ry = rings[0]
    assert rx.min() == -180.0 and rx.max() == 180.0 and ry.min() == -90.0
    assert B.signed_area(rx, ry) > 0
    px = np.array([0.0, 100.0, -150.0, 0.0]); py = np.array([-85.0, -80.0, -89.9, -60.0])
    assert list(winding(px, py, rings) != 0) == [True, True, True, False]


def test_normalize_ring_drops_degenerate_input():
    assert B.normalize_ring([1.0, 2.0], [1.0, 2.0]) == []


# ---------------------------------------------------------------- clipping

def test_clip_half_concave_ring_area_and_membership():
    # U shape: two prongs joined at the bottom; clip away the bottom so two lobes remain
    x = np.array([0, 3, 3, 2, 2, 1, 1, 0], np.float64); y = np.array([0, 0, 3, 3, 1, 1, 3, 3], np.float64)
    top = B.clip_half(x, y, 1, 2.0, False)                                 # keep y >= 2
    assert top is not None and np.isclose(B.signed_area(*top), 2.0)       # two 1x1 lobes
    rng = np.random.default_rng(3)
    px = rng.uniform(-0.5, 3.5, 4000); py = rng.uniform(-0.5, 3.5, 4000)
    expect = (winding(px, py, [(x, y)]) != 0) & (py >= 2)
    got = winding(px, py, [top]) != 0
    on_line = np.isclose(py, 2.0)
    assert np.array_equal(expect[~on_line], got[~on_line])
    assert B.clip_half(x, y, 0, -1.0, True) is None                        # nothing left
    full = B.clip_half(x, y, 0, 10.0, True)
    assert full[0] is x and full[1] is y                                   # untouched when all inside


def test_split_cells_conserves_area_membership_and_bounds():
    x, y = star(7.3, -2.2, 3.0, 9.0, n=40, seed=5)
    if B.signed_area(x, y) < 0:
        x, y = x[::-1], y[::-1]
    pieces = B.split_cells(x, y, 5)
    assert len(pieces) > 4
    for lat0, lon0, px, py in pieces:
        assert px.min() >= lon0 - 1e-9 and px.max() <= lon0 + 5 + 1e-9
        assert py.min() >= lat0 - 1e-9 and py.max() <= lat0 + 5 + 1e-9
        assert B.signed_area(px, py) > 0
    assert np.isclose(sum(B.signed_area(p[2], p[3]) for p in pieces), B.signed_area(x, y), rtol=1e-12)
    rng = np.random.default_rng(9)
    qx = rng.uniform(-3, 18, 3000); qy = rng.uniform(-12, 8, 3000)
    qx = qx[(np.abs(qx / 5 - np.round(qx / 5)) > 1e-6)]; qy = qy[: len(qx)]
    qy = np.where(np.abs(qy / 5 - np.round(qy / 5)) > 1e-6, qy, qy + 0.01)
    w_src = winding(qx, qy, [(x, y)]); w_pcs = winding(qx, qy, [(p[2], p[3]) for p in pieces])
    assert np.array_equal(w_src, w_pcs)                                    # same region, no overlaps


def test_cell_range_boundaries():
    assert B.cell_range(5.0, 10.0, 5) == (1, 1)                            # touches both edges of cell 1 only
    assert B.cell_range(4.9, 10.0, 5) == (0, 1)
    assert B.cell_range(-180.0, -175.0, 5) == (-36, -36)
    assert B.cell_range(-90.0, -60.0, 30) == (-3, -3)


def test_quantize_ring_removes_duplicates_and_degenerates():
    r = B.quantize_ring(np.array([0, 0.00001, 1, 1, 0]), np.array([0, 0.00001, 0, 1, 0]))
    assert r is not None and len(r[0]) == 3                               # 1e-5 merges into the first vertex; closure dropped
    assert B.quantize_ring(np.array([0, 1, 2.0]), np.array([0, 1, 2.0])) is None   # collinear: zero area


# ---------------------------------------------------------------- coast-v1

def reference_leb128(values):
    out = bytearray()
    for v in values:
        v = int(v)
        while True:
            b = v & 0x7F; v >>= 7
            out.append(b | (0x80 if v else 0))
            if not v:
                break
    return bytes(out)


def test_varints_and_zigzag_match_a_reference_encoder():
    vals = np.array([0, 1, 127, 128, 300, 16383, 16384, 2 ** 21, 2 ** 28 - 1, 2 ** 28, 2 ** 34], np.uint64)
    assert B.varints(vals) == reference_leb128(vals)
    zz = B.zigzag([0, -1, 1, -2, 2, -1800000, 1800000])
    assert zz.tolist() == [0, 1, 2, 3, 4, 3599999, 3600000]


def test_encode_decode_roundtrip_and_header():
    ra = (np.array([-1800000, -1790000, -1790000]), np.array([-900000, -900000, -890000]))
    rb = (np.array([1795000, 1800000, 1800000, 1795000]), np.array([100000, 100000, 150000, 150000]))
    rc = (np.array([0, 3, 3]), np.array([0, 0, 3]))
    buf = B.encode_file([[ra], [rb, rc]], 5)
    d = B.decode_file(buf)
    assert d["cell"] == 5 and d["q"] == Q and d["bbox"] == [-1800000, -900000, 1800000, 150000]
    assert len(d["pieces"]) == 2 and d["pieces"][0]["b"] == [-1800000, -900000, -1790000, -890000]
    got = [r for pc in d["pieces"] for r in pc["rings"]]
    for (ex, ey), (gx, gy) in zip([ra, rb, rc], got):
        assert np.array_equal(ex, gx) and np.array_equal(ey, gy)
    assert B.decode_file(B.encode_file([], 30))["pieces"] == []
    with pytest.raises(ValueError):
        B.decode_file(b"XXXX" + buf[4:])
    with pytest.raises(ValueError):
        B.decode_file(buf + b"\x05")                                       # trailing value breaks the counts


# ---------------------------------------------------------------- build + check on a synthetic archive

def fixture_zip(path):
    """Oahu-like island (cell 20_-160, stored 0..360), a ring across the antimeridian, an Antarctic
    ring winding around the pole, a lake (ignored) and a tiny islet (dropped from tier 0 only)."""
    ox, oy = star(-158.0 + 360.0, 21.5, 0.2, 0.35, n=30, seed=1)
    island = gshhg_record(0, 1, np.c_[ox, oy], area_km2=1500)
    across = gshhg_record(1, 1, [(178, 64), (182, 64), (182, 67), (178, 67)], area_km2=40000)
    lon = np.linspace(180.0, -180.0, 145)
    ant = gshhg_record(2, 5, np.c_[lon, -70.0 + np.cos(np.radians(lon))], area_km2=1.4e7, greenwich=3)
    lake = gshhg_record(3, 2, [(30, 0), (31, 0), (31, 1)])
    islet = gshhg_record(4, 1, [(10.0, 10.0), (10.001, 10.0), (10.001, 10.001)], area_km2=0.01)
    body = island + across + ant + lake + islet
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("gshhs_i.b", body)
        z.writestr("gshhs_f.b", body)
    return path


def test_build_and_check_on_a_synthetic_archive(tmp_path):
    zp = fixture_zip(str(tmp_path / "g.zip"))
    out = str(tmp_path / "v1")
    index = B.build(zp, out, log=lambda *a: None)
    assert B.check(out, log=lambda *a: None) == []
    cells = set(index["tier1"]["cells"])
    assert "20_-160" in cells and "10_10" in cells                          # islet kept at full resolution
    assert {"60_175", "65_175", "60_-180", "65_-180"} <= cells               # split across the antimeridian
    assert all("-90_%d" % lon in cells for lon in range(-180, 180, 5))      # Antarctica reaches the pole everywhere
    assert not any(n.startswith("0_30") for n in cells)                      # the lake is not land
    t0 = B.decode_file(open(os.path.join(out, "world-i.bin"), "rb").read())
    assert not any(pc["b"][0] == 10 * Q and pc["b"][1] == 10 * Q for pc in t0["pieces"])   # islet dropped in tier 0
    assert index["tier0"]["cell"] == 30 and index["tier0"]["max_zoom"] == 6 and index["tier1"]["min_zoom"] == 7
    assert index["source"] == "GSHHG 2.3.7" and "LGPL" in index["license"]
    # the --check gate catches a corrupted cell and a missing file
    name = os.path.join(out, "f", "20_-160.bin")
    raw = bytearray(open(name, "rb").read()); raw[-1] = 0x81
    open(name, "wb").write(bytes(raw))
    assert B.check(out, log=lambda *a: None)
    assert B.main(["--check", out]) == 1
    assert B.main(["--out", out, "--upload", "static/coast/v1"]) == 1          # refuses before touching boto3


def test_coast_workflow_is_pinned():
    text = open(os.path.join(ROOT, ".github", "workflows", "coast-build.yml"), encoding="utf-8").read()
    uses = re.findall(r"uses:[ ]*([^ \n]+)", text)
    assert uses and all(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+@[0-9a-f]{40}", u) for u in uses), uses
    pip = re.findall(r"pip install ([^\n]+)", text)
    assert pip and all(re.fullmatch(r"[a-z0-9-]+==[0-9][0-9A-Za-z.]*", p) for line in pip for p in line.split()), pip
    assert "28600e8f7a08645aab43079326df6504212ec5ccb2b4bcf3b5f4f12ed60e82bc" in text      # source archive pinned by hash
    assert "workflow_dispatch" in text and "--upload static/coast/v1" in text


# ---------------------------------------------------------------- G5 additions

def test_normalize_ring_real_pole_storage_and_near_dateline_vertices():
    """GSHHG stores the Antarctic ice front -180..180 from (180, y) to (-180, y) with a closing edge
    360 degrees long. Vertices within rounding noise of the dateline are NOT snapped (the builder must
    reproduce the published v1 bytes; a snap belongs to a new prefix) but quantise onto it, so the
    dateline seam is exact."""
    lon = np.linspace(180.0, -180.0, 201); lat = -70.0 + np.cos(np.radians(lon))
    lon[1] = 179.9999999999; lon[-2] = -179.9999999999                                   # rounding noise
    rings = B.normalize_ring(lon, lat)
    assert len(rings) == 1
    rx, ry = rings[0]
    assert rx.max() == 180.0 and rx.min() == -180.0 and ry.min() == -90.0 and B.signed_area(rx, ry) > 0
    assert 179.9999999999 in set(rx.tolist()) and -179.9999999999 in set(rx.tolist())    # kept as stored
    qx, qy = B.quantize_ring(rx, ry)
    assert qx.max() == 180 * Q and qx.min() == -180 * Q
    assert not np.any(np.abs(qx) == 180 * Q - 1) and np.count_nonzero(qx == 180 * Q) >= 2 and np.count_nonzero(qx == -180 * Q) >= 2   # the noise vertices sit ON the dateline
    # a 0..360 ring with several Greenwich jump edges (Eurasia-like): one ring, area kept, no split
    x = np.array([350.0, 5.0, 10.0, 355.0, 359.0, 2.0, 4.0, 352.0]); y = np.array([50.0, 50.0, 55.0, 55.0, 58.0, 58.0, 62.0, 62.0])
    r = B.normalize_ring(x, y)
    assert len(r) == 1 and r[0][0].min() >= -10.0 and r[0][0].max() <= 10.0
    assert np.isclose(B.signed_area(*r[0]), abs(B.signed_area(x - 360.0 * (x > 180), y)))


def test_split_cells_seam_invariant_survives_quantisation():
    """The two pieces of a ring split by a cell line cover identical intervals of that line
    (what the client's nonzero union relies on), before and after quantisation."""
    x, y = star(20.0, 60.0, 1.0, 2.5, n=48, seed=11)
    if B.signed_area(x, y) < 0:
        x, y = x[::-1], y[::-1]
    pieces = B.split_cells(x, y, 5)
    assert len(pieces) >= 2

    def line_cover(rings, xline):
        spans = []
        for ix, iy in rings:
            n = len(ix)
            for i in range(n):
                a, b = (ix[i], iy[i]), (ix[(i + 1) % n], iy[(i + 1) % n])
                if a[0] == xline and b[0] == xline and a[1] != b[1]:
                    spans.append((min(a[1], b[1]), max(a[1], b[1])))
        return sorted(spans)
    west = [B.quantize_ring(px, py) for lat0, lon0, px, py in pieces if lon0 == 15]
    east = [B.quantize_ring(px, py) for lat0, lon0, px, py in pieces if lon0 == 20]
    assert west and east
    lw, le = line_cover(west, 20 * Q), line_cover(east, 20 * Q)
    assert lw and lw == le                                                           # exact same spans on the line


def test_check_catches_bad_cells_counts_and_format_key(tmp_path):
    zp = fixture_zip(str(tmp_path / "g.zip"))
    out = str(tmp_path / "v1")
    B.build(zp, out, log=lambda *a: None)
    assert B.check(out, log=lambda *a: None) == []
    idx = json.load(open(os.path.join(out, "index.json")))
    name = "20_-160"
    d = B.decode_file(open(os.path.join(out, "f", name + ".bin"), "rb").read())
    ix, iy = d["pieces"][0]["rings"][0]
    bad = B.encode_file([[(ix[::-1].copy(), iy[::-1].copy())]], 5)                          # clockwise
    open(os.path.join(out, "f", name + ".bin"), "wb").write(bad)
    probs = B.check(out, log=lambda *a: None)
    assert any("not counter-clockwise" in p for p in probs)
    thinned = B.encode_file([[(ix[::2].copy(), iy[::2].copy())]], 5)                       # every other vertex: count != index
    open(os.path.join(out, "f", name + ".bin"), "wb").write(thinned)
    probs = B.check(out, log=lambda *a: None)
    assert any("vertex count != index" in p for p in probs) and any("size mismatch" in p for p in probs)
    shifted = B.encode_file([[(ix + 10 * Q, iy)]], 5)                                    # moved a cell east
    open(os.path.join(out, "f", name + ".bin"), "wb").write(shifted)
    assert any("outside its cell" in p for p in B.check(out, log=lambda *a: None))
    idx2 = dict(idx, format_key="coast-v1|something-else")
    json.dump(idx2, open(os.path.join(out, "index.json"), "w"))
    assert any("format_key" in p for p in B.check(out, log=lambda *a: None))


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.log = []

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        self.objects[Key] = {"body": Body, "ct": ContentType, "cc": CacheControl}
        self.log.append(Key)

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key]["body"])}


def test_upload_publishes_licence_first_index_last_and_refuses_a_different_build(tmp_path):
    zp = fixture_zip(str(tmp_path / "g.zip"))
    out = str(tmp_path / "v1")
    B.build(zp, out, log=lambda *a: None)
    s3 = FakeS3()
    msgs = []
    assert B.upload(out, "static/coast/v1", log=msgs.append, s3=s3, bucket="b") is True
    assert s3.log[0] == "static/coast/v1/LICENSE.txt" and s3.log[-1] == "static/coast/v1/index.json"
    assert all(o["cc"] == "public, max-age=31536000, immutable" for o in s3.objects.values())
    assert b"GNU LESSER GENERAL PUBLIC LICENSE" in s3.objects["static/coast/v1/LICENSE.txt"]["body"]
    assert s3.objects["static/coast/v1/LICENSE.txt"]["ct"].startswith("text/plain")
    n_first = len(s3.log)
    assert B.upload(out, "static/coast/v1", log=msgs.append, s3=s3, bucket="b") is True      # the same build again
    assert s3.log[n_first:] == ["static/coast/v1/LICENSE.txt", "static/coast/v1/index.json"]
    idx = json.load(open(os.path.join(out, "index.json")))
    assert len(idx["tier1"]["sha256"]) == 64
    same_len = dict(idx); same_len["tier1"] = dict(idx["tier1"], sha256="1" * 64)             # a content change that keeps every file length
    json.dump(same_len, open(os.path.join(out, "index.json"), "w"))
    n = len(s3.log)
    assert B.upload(out, "static/coast/v1", log=msgs.append, s3=s3, bucket="b") is False     # refused on the content hash alone
    assert len(s3.log) == n
    legacy = dict(idx); legacy["tier1"] = {k: v for k, v in idx["tier1"].items() if k != "sha256"}
    assert B.same_build(legacy, idx) and not B.same_build(idx, legacy)                        # a published index from before the hash: cell map only; a hash-less LOCAL index never passes
    idx["tier0"]["sha256"] = "0" * 64
    json.dump(idx, open(os.path.join(out, "index.json"), "w"))
    n = len(s3.log)
    assert B.upload(out, "static/coast/v1", log=msgs.append, s3=s3, bucket="b") is False     # a different build: refused
    assert len(s3.log) == n and any("refusing" in m for m in msgs)
    assert B.upload(out, "static/coast/v1", replace=True, log=msgs.append, s3=s3, bucket="b") is True   # --replace: every data file again
    assert s3.log[n:].count("static/coast/v1/world-i.bin") == 1 and s3.log[-1] == "static/coast/v1/index.json"
    assert B.main(["--out", out, "--upload", "static/coast/v1"]) == 1                          # --check fails on the tampered index


def test_redact_and_reader_guards():
    os.environ["R2_BUCKET"] = "allshore-model-frames"
    try:
        msg = B.redact("Could not connect to https://0123456789abcdef0123456789abcdef.r2.cloudflarestorage.com/allshore-model-frames/x key fedcba9876543210fedcba9876543210 bucket allshore-model-frames")
    finally:
        del os.environ["R2_BUCKET"]
    assert "cloudflarestorage" not in msg and "0123456789abcdef" not in msg and "allshore-model-frames" not in msg
    assert "<r2-endpoint>" in msg and "<redacted>" in msg and "<bucket>" in msg
    good = gshhg_record(0, 1, [(10, 10), (11, 10), (11, 11)])
    with pytest.raises(ValueError):
        list(B.read_gshhg(bytes([255]) * 68))                                            # garbage: an absurd point count
    other_version = bytearray(gshhg_record(1, 1, [(10, 10), (11, 10), (11, 11)]))
    other_version[10] ^= 1                                                               # the version byte of the big-endian flag
    with pytest.raises(ValueError):
        list(B.read_gshhg(good + bytes(other_version)))                                   # version changes between records
    assert list(B.read_gshhg(gshhg_record(2, 1, [(10, 10), (11, 10), (11, 11)]) + good)) != []
