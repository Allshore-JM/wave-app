"""Great Lakes frame job (plan section 50): offline tests (no network, no eccodes, no R2).

Real fixtures (tests/capture_glwu_fixtures.py, run 2026102119): the .idx lines of four steps, the eccodes metadata of
step 0's records with eccodes' own coordinates at a few source nodes, and one bulletin. They pin the record identity
(ws / wdir / swh / perpw / dirpw, surface level 1), the Lambert grid (spherical, scanning mode 64), the adjacency of
records 1-7 in every step, and the bulletin layout (150 hourly rows, space-padded hours).
"""
import io
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import decode as D        # noqa: E402
import encode as E        # noqa: E402
import fetch as F         # noqa: E402
import glwu as GL         # noqa: E402
import glwu_fetch as G    # noqa: E402
import publish as P       # noqa: E402
import run as R           # noqa: E402
from test_job import FakeClient, _env   # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "glwu")
RUN = datetime(2026, 10, 21, 19, tzinfo=timezone.utc)
META = json.load(open(os.path.join(FIX, "meta.json"), encoding="utf-8"))
IDX = open(os.path.join(FIX, "run.idx"), encoding="ascii").read()
BULL = open(os.path.join(FIX, "45007.bull"), encoding="ascii").read()
GPRE = P.GLWU_PREFIX
GLATEST = P.latest_key(GPRE)
NX, NY = D.GLWU_SRC["Nx"], D.GLWU_SRC["Ny"]


def _full_idx(run_dt=RUN, steps=G.STEPS):
    """A synthetic .idx of the whole run in GLWU's layout (19 records a step, the needed seven first)."""
    names = list(G.RECORDS) + ["WVHGT:surface", "SWELL:1 in sequence"] + [f"X{k}:surface" for k in range(10)]
    lines, off, n = [], 0, 1
    for s in steps:
        label = "anl" if s == 0 else f"{s} hour fcst"
        for name in names:
            var, lev = name.split(":")
            lines.append(f"{n}:{off}:d={run_dt:%Y%m%d%H}:{var}:{lev}:{label}:")
            off += 1000
            n += 1
    return "\n".join(lines) + "\n"


def _grib(payload=b"\0" * 20):
    n = 16 + len(payload) + 4
    return b"GRIB" + b"\0\0" + b"\0" + b"\2" + n.to_bytes(8, "big") + payload + b"7777"


def _inverse_lcc(col, row, src=D.GLWU_SRC, radius=D.EARTH_R6):
    """An independent inverse of the spherical Lambert projection (Snyder 1987, eqs. 14-10, 15-1..15-5, 14-4)."""
    phi1 = np.radians(src["Latin1InDegrees"])
    n = np.sin(phi1)
    f = np.cos(phi1) * np.tan(np.pi / 4 + phi1 / 2) ** n / n
    rho0 = radius * f / np.tan(np.pi / 4 + np.radians(src["LaDInDegrees"]) / 2) ** n
    la1, lo1 = np.radians(src["latitudeOfFirstGridPointInDegrees"]), src["longitudeOfFirstGridPointInDegrees"]
    rho1 = radius * f / np.tan(np.pi / 4 + la1 / 2) ** n
    th1 = n * np.radians(((lo1 - src["LoVInDegrees"] + 180) % 360) - 180)
    x = rho1 * np.sin(th1) + col * src["DxInMetres"]
    y = rho0 - rho1 * np.cos(th1) + row * src["DyInMetres"]
    rho = np.hypot(x, rho0 - y)
    lat = np.degrees(2 * np.arctan((radius * f / rho) ** (1 / n)) - np.pi / 2)
    lon = src["LoVInDegrees"] + np.degrees(np.arctan2(x, rho0 - y) / n)
    return lat, lon


# ------------------------------- .idx, spans, messages, discovery ------------------------------

def test_idx_fixture_steps_records_and_spans():
    recs = G.parse_idx(IDX)
    assert {r[2] for r in recs} == {0, 1, 148, 149} and len(recs) == 76 and {r[3] for r in recs} == {"2026102119"}
    for step in (0, 1, 148, 149):
        keys = [r[1] for r in recs if r[2] == step]
        assert keys[:7] == list(G.RECORDS) and len(keys) == 19
        lo, hi = G.span(recs, step)
        first = next(r[0] for r in recs if r[2] == step)
        after = next(r[0] for r in recs if r[2] == step and r[1] == "WVHGT:surface")
        assert (lo, hi) == (first, after - 1)
    assert G.span(recs, 0) == (0, 314179 - 1) or G.span(recs, 0)[0] == 0
    assert not G.is_complete(recs, RUN)                                  # four steps only
    assert G.is_complete(G.parse_idx(_full_idx()), RUN)
    assert not G.is_complete(G.parse_idx(_full_idx()), RUN - timedelta(hours=6))   # another cycle's records


def test_span_refuses_reordered_or_unbounded_records():
    recs = G.parse_idx(_full_idx(steps=[0]))
    swapped = list(recs)
    swapped[4], swapped[5] = swapped[5], swapped[4]
    with pytest.raises(RuntimeError):
        G.span(swapped, 0)
    gap = recs[:6] + [(recs[5][0] + 1, "XTRA:surface", 0, recs[0][3])] + recs[6:]
    with pytest.raises(RuntimeError):
        G.span(gap, 0)                                                   # in order but not adjacent: one range would read it too
    with pytest.raises(RuntimeError):
        G.span(recs[:7], 0)                                              # nothing after DIRPW bounds the range
    with pytest.raises(ValueError):
        G.parse_idx("not an idx line\n")


def test_split_messages():
    a, b = _grib(b"a" * 10), _grib(b"b" * 30)
    assert G.split_messages(a + b) == [a, b]
    with pytest.raises(RuntimeError):
        G.split_messages(a + b[:-1])                                     # truncated
    with pytest.raises(RuntimeError):
        G.split_messages(a[:-4] + b"xxxx")                               # no 7777
    with pytest.raises(RuntimeError):
        G.split_messages(b"GRIB\0\0\0\1" + a[8:])                        # edition 1


def test_candidate_runs_and_latest_complete_run(monkeypatch):
    now = datetime(2026, 10, 21, 13, 40, tzinfo=timezone.utc)
    assert [c.hour for c in G.candidate_runs(now)] == [13, 7, 1, 19] and G.candidate_runs(now)[3].day == 20
    full07 = G.parse_idx(_full_idx(now.replace(hour=7, minute=0)))
    asked = []

    def idx(run_dt):
        asked.append(run_dt.hour)
        if run_dt.hour == 13:
            raise F.NotReady("404")
        return full07 if run_dt.hour == 7 else []
    monkeypatch.setattr(G, "fetch_idx", idx)
    run_dt, recs = G.latest_complete_run(now)
    assert run_dt.hour == 7 and recs is full07 and asked == [13, 7]
    monkeypatch.setattr(G, "fetch_idx", lambda run_dt: G.parse_idx(_full_idx(run_dt, steps=G.STEPS[:-1])))
    assert G.latest_complete_run(now) == (None, None)                    # step 149 missing everywhere
    monkeypatch.setattr(G, "fetch_idx", lambda run_dt: (_ for _ in ()).throw(G.Throttled("429")))
    with pytest.raises(G.Throttled):
        G.latest_complete_run(now)


def test_request_paces_throttles_and_maps_errors(monkeypatch):
    import urllib.error
    sleeps, calls = [], []
    monkeypatch.setattr(G.time, "sleep", lambda s: sleeps.append(s))

    def err(code, headers=None):
        return urllib.error.HTTPError("u", code, "x", headers or {}, None)
    seq = [err(429), err(503, {"Retry-After": "7"}), (206, b"ok")]

    def opener(url, rng=None, timeout=60):
        calls.append(rng)
        x = seq.pop(0)
        if isinstance(x, Exception):
            raise x
        return x
    monkeypatch.setattr(G, "_open", opener)
    G._last[0] = 0.0
    assert G.request("u", rng="bytes=0-9") == (206, b"ok") and calls == ["bytes=0-9"] * 3
    assert 5 in sleeps and 7.0 in sleeps                                  # the ladder, then the server's Retry-After
    for code in (403, 404, 416):                                         # 403: a NOMADS block; 416: a file still being written
        monkeypatch.setattr(G, "_open", lambda url, rng=None, timeout=60, code=code: (_ for _ in ()).throw(err(code)))
        with pytest.raises(F.NotReady):
            G.request("u")
    sleeps.clear()
    seq[:] = [err(429, {"Retry-After": "999"}), (206, b"ok")]
    monkeypatch.setattr(G, "_open", opener)
    G.request("u")
    assert [x for x in sleeps if x >= 1] == [G.RETRY_AFTER_MAX_S] == [60]   # a huge Retry-After is capped (the rest: the pacer)
    sleeps.clear()
    monkeypatch.setattr(G, "_open", lambda url, rng=None, timeout=60: (_ for _ in ()).throw(err(429)))
    with pytest.raises(G.Throttled):
        G.request("u")
    assert [x for x in sleeps if x >= 1] == list(G.THROTTLE_WAITS_S) == [5, 10, 20, 30, 45, 60]   # the whole ladder first
    monkeypatch.setattr(G, "_open", lambda url, rng=None, timeout=60: (_ for _ in ()).throw(err(500)))
    with pytest.raises(F.TransportError):
        G.request("u")


def test_pacer_spaces_requests(monkeypatch):
    t = [100.0]
    sleeps = []
    monkeypatch.setattr(G.time, "monotonic", lambda: t[0])

    def sleep(s):
        sleeps.append(round(s, 3))
        t[0] += s
    monkeypatch.setattr(G.time, "sleep", sleep)
    monkeypatch.setattr(G, "_open", lambda url, rng=None, timeout=60: (200, b""))
    G._last[0] = 0.0
    G.request("a")
    G.request("b")
    t[0] += 0.2
    G.request("c")
    assert sleeps == [G.PACE_S, round(G.PACE_S - 0.2, 3)]


def test_fetch_step_reads_one_range_into_seven_messages(monkeypatch):
    recs = G.parse_idx(_full_idx(steps=[0, 1]))
    lo, hi = G.span(recs, 1)
    blobs = [_grib(bytes([k]) * 5) for k in range(7)]
    body = b"".join(blobs)
    seen = []
    monkeypatch.setattr(G, "request", lambda url, rng=None: (seen.append((url, rng)) or (206, body)))
    monkeypatch.setattr(G, "span", lambda recs, step: (lo, lo + len(body) - 1))
    out = G.fetch_step(RUN, recs, 1)
    assert list(out) == list(G.RECORDS) and out["HTSGW:surface"] == blobs[4]
    assert seen == [(G.grib_url(RUN), f"bytes={lo}-{lo + len(body) - 1}")]
    assert G.grib_url(RUN).endswith("/glwu.20261021/glwu.grlc_2p5km.t19z.grib2")
    monkeypatch.setattr(G, "request", lambda url, rng=None: (200, body))
    with pytest.raises(F.NotReady):
        G.fetch_step(RUN, recs, 1)                                       # not a partial answer: still being written
    monkeypatch.setattr(G, "request", lambda url, rng=None: (206, body[:-5]))
    with pytest.raises(F.NotReady):
        G.fetch_step(RUN, recs, 1)                                       # short


# ------------------------------- grid, identity, projection, regrid ---------------------------

def test_fixture_records_pass_identity_and_geometry():
    assert set(D.EXPECT_GLWU) < set(META["records"]) == set(G.RECORDS)    # UGRD / VGRD come in the range, unused
    for key in D.EXPECT_GLWU:
        m = META["records"][key]
        D.check_identity(m, key, RUN, 0, D.EXPECT_GLWU)
        D.check_geometry_glwu(m)
    m = dict(META["records"]["HTSGW:surface"])
    for k, v in (("Nx", 580), ("LoVInDegrees", 262.5), ("jScansPositively", 0), ("shapeOfTheEarth", 1), ("gridType", "regular_ll")):
        with pytest.raises(ValueError):
            D.check_geometry_glwu(dict(m, **{k: v}))
    with pytest.raises(ValueError):
        D.check_identity(dict(m, shortName="perpw"), "HTSGW:surface", RUN, 0, D.EXPECT_GLWU)
    assert META["records"]["PERPW:surface"]["shortName"] == "perpw" and META["records"]["WIND:surface"]["shortName"] == "ws"
    assert META["water_cells"] == 44863


def test_lambert_matches_eccodes_coordinates_and_an_independent_inverse():
    for c, r, lat, lon in META["nodes"]:                                 # eccodes' own coordinates
        col, row = D.lambert_colrow(lat, lon)
        assert abs(col - c) < 1e-6 and abs(row - r) < 1e-6, (c, r)
    jj, ii = np.mgrid[0:NY, 0:NX]
    lat, lon = _inverse_lcc(ii.astype(float), jj.astype(float))
    for c, r, la, lo in META["nodes"]:
        assert abs(lat[r, c] - la) < 1e-7 and abs(((lon[r, c] - lo + 180) % 360) - 180) < 1e-7
    assert D.check_glwu_coords(lat.ravel(), lon.ravel()) < 1e-6
    with pytest.raises(ValueError):
        D.check_glwu_coords(lat.ravel(), lon.ravel() + 0.01)             # ~0.3 of a cell off


def test_glwu_index_takes_the_nearest_cell():
    flat, inside = D.glwu_index()
    g = D.GLWU_GRID
    assert flat.shape == inside.shape == (g["rows"], g["cols"]) and g["lon_periodic"] is False
    assert 300_000 < inside.sum() < g["rows"] * g["cols"]                # the grid's corners lie outside the Lambert grid
    lat = g["lat0"] + g["dlat"] * np.arange(g["rows"])
    lon = g["lon0"] + g["dlon"] * np.arange(g["cols"])
    jj, ii = np.mgrid[0:NY, 0:NX]
    slat, slon = _inverse_lcc(ii.astype(float), jj.astype(float))
    slon = ((slon + 180) % 360) - 180
    rng = np.random.default_rng(3)
    for _ in range(300):
        i, j = rng.integers(g["rows"]), rng.integers(g["cols"])
        if not inside[i, j]:
            continue
        r, c = divmod(int(flat[i, j]), NX)
        col, row = D.lambert_colrow(lat[i], lon[j])
        assert abs(col - c) <= 0.5 + 1e-9 and abs(row - r) <= 0.5 + 1e-9
        d = np.hypot(slat - lat[i], (slon - lon[j]) * np.cos(np.radians(lat[i])))
        assert d[r, c] <= d.min() + 0.004                                 # the nearest by distance, or a near tie (< 0.4 km)


# ------------------------------- the source: decode, encode, stations ------------------------

def _fake_decode(values, calls=None):
    """D.decode_glwu stand-in: values[key] (flat source arrays); records carry the key name."""
    def dec(blob, key, run_dt, step, coords=False):
        if calls is not None:
            calls.append((key, step, coords))
        v = np.array(values[key], dtype=np.float64)
        if coords:
            jj, ii = np.mgrid[0:NY, 0:NX]
            lat, lon = _inverse_lcc(ii.astype(float), jj.astype(float))
            return v, {}, (lat.ravel(), lon.ravel())
        return v, {}
    return dec


def _lake_values(step=0):
    """Source fields: water in a band of rows, land (NaN) elsewhere; one water cell without a period (ice)."""
    water = np.zeros((NY, NX), bool)
    water[150:200, 100:500] = True
    hs = np.where(water, 0.5 + 0.001 * np.arange(NX)[None, :] + 0.01 * step, np.nan).ravel()
    tp = np.where(water, 4.0, np.nan).ravel()
    ice = 170 * NX + 300
    tp[ice] = np.nan
    hs[160 * NX + 200:160 * NX + 260] = 0.05                              # calm water: a height, a period the job drops
    pdir = np.where(water, 365.0, np.nan).ravel()                         # wraps to 5
    ws = np.where(water, 6.0, np.nan).ravel()
    wd = np.where(water, 359.6, np.nan).ravel()
    return {"HTSGW:surface": hs, "PERPW:surface": tp, "DIRPW:surface": pdir, "WIND:surface": ws, "WDIR:surface": wd}, ice


def test_decode_regrids_blanks_ice_wraps_directions_and_samples_winds(monkeypatch):
    vals, ice = _lake_values()
    calls = []
    monkeypatch.setattr(D, "decode_glwu", _fake_decode(vals, calls))
    monkeypatch.setattr(G, "STATIONS", {"in": tuple(_inverse_lcc(300.0, 175.0)), "off": (44.49, -73.34)})
    src = GL.GlwuSource()
    grids = src.decode({k: b"" for k in G.RECORDS}, RUN, 0)
    assert set(grids) == {"hs", "tp", "pdir"} and all(g.shape == (401, 831) and g.dtype == np.float32 for g in grids.values())
    assert [c for c in calls if c[2]] == [("HTSGW:surface", 0, True)]    # the coordinates are checked once per build
    flat, inside = D.glwu_index()
    want = np.where(inside, vals["HTSGW:surface"][flat], np.nan)
    want = np.where(inside & ~np.isnan(vals["PERPW:surface"][flat]), want, np.nan)
    np.testing.assert_array_equal(np.isnan(grids["hs"]), np.isnan(want))
    np.testing.assert_allclose(grids["hs"][~np.isnan(want)], want[~np.isnan(want)], rtol=1e-6)
    at_ice = flat == ice
    assert at_ice.any() and np.isnan(grids["hs"][at_ice]).all() and np.isnan(grids["pdir"][at_ice]).all()
    calm = inside & (vals["HTSGW:surface"][flat] == 0.05)
    assert calm.any() and (grids["hs"][calm] == np.float32(0.05)).all()   # calm water keeps its height (owner, G33a P2-1)
    assert np.isnan(grids["tp"][calm]).all() and np.isnan(grids["pdir"][calm]).all()
    waves = inside & (vals["HTSGW:surface"][flat] >= GL.CALM_M) & ~at_ice
    assert not np.isnan(grids["tp"][waves]).any() and not np.isnan(grids["pdir"][waves]).any()
    assert np.nanmin(grids["pdir"]) == np.nanmax(grids["pdir"]) == 5.0
    assert src._winds[0] == {"in": [6.0, 0], "off": None}                 # 359.6 rounds to 360 = 0; Lake Champlain: no cell
    src.decode({k: b"" for k in G.RECORDS}, RUN, 1)
    assert sum(1 for c in calls if c[2]) == 1


def test_station_cells_prefers_its_own_cell_then_the_nearest_water(monkeypatch):
    lat, lon = _inverse_lcc(200.0, 100.0)
    monkeypatch.setattr(G, "STATIONS", {"a": (float(lat), float(lon)), "far": (44.49, -73.34)})
    water = np.zeros((NY, NX), bool)
    water[100, 200] = True
    assert GL.station_cells(water.ravel()) == {"a": 100 * NX + 200, "far": None}
    water[100, 200] = False
    water[102, 201] = water[100, 203] = True                              # both within 3 cells: the nearer one
    assert GL.station_cells(water.ravel())["a"] == 102 * NX + 201
    water[:] = False
    water[104, 200] = True                                                # 4 cells away: none
    assert GL.station_cells(water.ravel())["a"] is None


def test_encode_publishes_one_resolution_with_a_two_node_fill():
    g = np.full((401, 831), np.nan, np.float32)
    g[200, 300:310] = 1.0
    out = GL.GlwuSource().encode(g, "hs")
    assert set(out) == {"full", "stats"}
    q = np.array(Image.open(io.BytesIO(out["full"])))
    assert q.shape == (401, 831) and q[200, 300] == E.quantize(np.array([1.0]), 0.0, E.HS_TOP)[0]
    assert q[202, 305] > 0 and q[203, 305] == 0 and q[200, 297] == 0 and q[200, 298] > 0
    assert out["stats"]["valid_points"] == 10 and out["stats"]["filled_points"] == int(np.count_nonzero(q)) - 10
    assert GL.FIELDS["hs"]["lo"] == E.FIELDS["hs"]["lo"] and GL.FIELDS["hs"]["hi"] == E.FIELDS["hs"]["hi"]
    assert {n: f["resolutions"] for n, f in GL.FIELDS.items()} == {"hs": ("full",), "tp": ("full",), "pdir": ("full",)}
    assert E.FIELDS["hs"]["resolutions"] == ("full", "half")                # the global fields are untouched


def test_bulletin_check():
    GL.check_bulletin(BULL, "45007", RUN)
    rows = [ln for ln in BULL.splitlines() if GL.BULL_ROW_RE.match(ln)]
    assert len(rows) == 150 and any(re.search(r"\|\s*\d+\s{2}\d\s*\|", r) for r in rows)   # "| 22  0 |"
    with pytest.raises(ValueError):
        GL.check_bulletin(BULL, "45007", RUN - timedelta(hours=6))
    with pytest.raises(ValueError):
        GL.check_bulletin(BULL, "45002", RUN)
    with pytest.raises(ValueError):
        GL.check_bulletin("\n".join(BULL.splitlines()[:-10]), "45007", RUN)
    extra = BULL.replace(rows[-1], rows[-1] + "\n" + rows[-1])
    with pytest.raises(ValueError):
        GL.check_bulletin(extra, "45007", RUN)                            # one row too many
    with pytest.raises(ValueError):
        GL.check_bulletin(BULL.replace("(42.67N  87.03W)", "(42.77N  87.03W)"), "45007", RUN)


def _bulletin_for(sid, run_dt=RUN):
    lat, lon = G.STATIONS[sid]
    text = re.sub(r"Location : 45007\s+\([^)]*\)", f"Location : {sid}   ({lat:.2f}N  {-lon:.2f}W)", BULL)
    return text.replace("Cycle    : 20261021 19 UTC", f"Cycle    : {run_dt:%Y%m%d %H} UTC")


# ------------------------------- the whole job, offline -----------------------------------------

COARSE = dict(D.GLWU_GRID, cols=167, rows=81, dlon=0.1, dlat=-0.1)


@pytest.fixture
def offline_glwu(monkeypatch):
    vals, _ice = _lake_values()
    monkeypatch.setattr(GL, "GRID", COARSE)
    monkeypatch.setattr(GL, "ALLOW_ALL", np.ones((COARSE["rows"], COARSE["cols"]), bool))
    monkeypatch.setattr(D, "decode_glwu", _fake_decode(vals))
    monkeypatch.setattr(G, "latest_complete_run", lambda now=None: (RUN, G.parse_idx(_full_idx())))
    monkeypatch.setattr(G, "fetch_step", lambda run_dt, recs, step: {k: b"x" for k in G.RECORDS})
    monkeypatch.setattr(G, "fetch_bulletin", lambda run_dt, sid: _bulletin_for(sid, run_dt))
    src = GL.GlwuSource()
    monkeypatch.setattr(GL, "SOURCE", src)
    return src


def test_complete_build_publishes_frames_bulletins_winds_manifest_then_pointer(offline_glwu, monkeypatch):
    c = FakeClient()
    _env(monkeypatch, c)
    assert GL.main([]) == 0
    puts = [k for op, k in c.log if op == "put"]
    run = "2026102119"
    assert puts[-1] == GLATEST and puts[-2].startswith(f"{GPRE}/{run}/manifest-") and puts[-3].startswith(f"{GPRE}/{run}/stats-")
    frames = [k for k in puts if k.endswith(".png")]
    assert len(frames) == 150 * 3 and f"{GPRE}/{run}/hs/f149.png" in frames and not any("/half/" in k for k in frames)
    bulls = [k for k in puts if k.endswith(".bull")]
    assert len(bulls) == len(G.STATIONS) == 72
    last_frame = max(puts.index(k) for k in frames)
    assert all(puts.index(k) > last_frame for k in bulls) and puts.index(f"{GPRE}/{run}/winds.json") > last_frame
    man = json.loads(c.objects[puts[-2]]["body"])
    assert man["grid"] == COARSE and "grid_half" not in man and man["files"]["res"] == {"full": ""}
    assert man["files"]["template"] == f"{GPRE}/{run}/{{res}}{{field}}/f{{step:03d}}.png"
    assert set(man["fields"]) == {"hs", "tp", "pdir"} and man["fields"]["hs"]["resolutions"] == ["full"]
    assert man["fields"]["tp"]["lo"] == E.FIELDS["tp"]["lo"] and man["fields"]["pdir"]["circular"] is True
    assert man["frame_schedule"] == [[0, 149, 1]] and man["expected_frames"] == 150 and len(man["frames"]) == 150
    assert man["frames"][149]["valid_utc"] == "2026-10-28T00:00:00Z" and man["complete"] is True
    assert man["fill"] == GL.FILL_INFO and man["model"]["name"].startswith("NOAA/NCEP GLWU")
    assert man["bulletins"] == {"dir": f"{GPRE}/{run}/bull", "ids": sorted(G.STATIONS)}
    assert man["winds"] == f"{GPRE}/{run}/winds.json"
    winds = json.loads(c.objects[man["winds"]]["body"])
    assert winds["steps"] == G.STEPS and set(winds["stations"]) <= set(G.STATIONS) and "45195" not in winds["stations"]
    assert all(len(v) == 150 for v in winds["stations"].values())
    latest = json.loads(c.objects[GLATEST]["body"])
    assert latest["run"] == run and latest["frames"] == 150 and latest["manifest"] == puts[-2]
    assert c.objects[bulls[0]]["cc"] == P.IMMUTABLE and c.objects[bulls[0]]["ct"].startswith("text/plain")
    assert not any(k.startswith(P.PREFIX) or k.startswith(P.ECMWF_PREFIX) for k in c.objects)     # only its own prefix
    assert GL.main([]) == 0 and len([k for op, k in c.log if op == "put"]) == len(puts)          # already live: nothing new


def test_missing_bulletins_are_left_out_up_to_the_limit(offline_glwu, monkeypatch, capsys):
    c = FakeClient()
    _env(monkeypatch, c)
    gone = sorted(G.STATIONS)[:G.BULLETIN_MAX_MISSING]

    def fetch(run_dt, sid):
        if sid in gone:
            raise F.NotReady("404")
        return _bulletin_for(sid, run_dt)
    monkeypatch.setattr(G, "fetch_bulletin", fetch)
    assert GL.main([]) == 0
    man = json.loads(c.objects[json.loads(c.objects[GLATEST]["body"])["manifest"]]["body"])
    assert man["bulletins"]["ids"] == sorted(set(G.STATIONS) - set(gone))
    assert "bulletin left out" in capsys.readouterr().out


def test_bad_bulletins_never_hold_the_frames_back(offline_glwu, monkeypatch):
    c = FakeClient()
    _env(monkeypatch, c)
    summary = []
    monkeypatch.setattr(R, "_summary", lambda line: summary.append(line))
    monkeypatch.setattr(G, "fetch_bulletin", lambda run_dt, sid: _bulletin_for(sid, run_dt - timedelta(hours=6)))
    assert GL.main([]) == 0                                              # every bulletin is from another cycle
    man = json.loads(c.objects[json.loads(c.objects[GLATEST]["body"])["manifest"]]["body"])
    assert man["bulletins"]["ids"] == [] and len(man["frames"]) == 150
    assert any(l.startswith("WARNING: 72 of 72 GLWU bulletins left out") for l in summary)
    assert P.failed_key("2026102119", GPRE) not in c.objects


def test_unreachable_and_throttled_bulletins_are_left_out(offline_glwu, monkeypatch):
    c = FakeClient()
    _env(monkeypatch, c)
    asked = []

    def fetch(run_dt, sid):
        asked.append(sid)
        if sid == "45002":
            raise F.TransportError("reset")
        if sid == "45008":
            raise G.Throttled("429")
        return _bulletin_for(sid, run_dt)
    monkeypatch.setattr(G, "fetch_bulletin", fetch)
    assert GL.main([]) == 0
    order = list(G.STATIONS)
    assert asked == order[:order.index("45008") + 1]                      # nothing asked after NOMADS throttled
    man = json.loads(c.objects[json.loads(c.objects[GLATEST]["body"])["manifest"]]["body"])
    assert man["bulletins"]["ids"] == sorted(s for s in order[:order.index("45008")] if s != "45002")


def test_an_outage_mid_build_is_not_ready(offline_glwu, monkeypatch):
    c = FakeClient()
    _env(monkeypatch, c)
    monkeypatch.setattr(G, "fetch_step", lambda run_dt, recs, step: (_ for _ in ()).throw(F.TransportError("502")))
    assert GL.main([]) == 3
    assert GLATEST not in c.objects and P.failed_key("2026102119", GPRE) not in c.objects
    assert json.loads(c.objects[P.notready_key("2026102119", GPRE)]["body"])["count"] == 1


def test_a_partial_build_stores_no_bulletins_or_winds(offline_glwu, monkeypatch):
    c = FakeClient()
    _env(monkeypatch, c)
    assert GL.main(["--steps", "0,1", "--allow-partial"]) == 0
    assert not any(k.endswith(".bull") or k.endswith("winds.json") for k in c.objects)
    part = [k for k in c.objects if "/partial-" in k]
    assert part and "bulletins" not in json.loads(c.objects[part[0]]["body"])


def test_build_budget_ends_the_tick_quietly(monkeypatch):
    monkeypatch.setattr(GL, "_started", [GL.time.monotonic() - GL.BUILD_BUDGET_S - 1])
    assert GL.BUILD_BUDGET_S == 20 * 60
    with pytest.raises(F.NotReady):
        GL.GlwuSource().fetch(RUN, 7)
    assert (GL.GlwuSource.failed_retry_after_s, GL.GlwuSource.failed_max_attempts, GL.GlwuSource.stale_fail_h) == (1800, 6, 18)


def test_no_run_and_a_stale_live_run(monkeypatch, capsys):
    c = FakeClient()
    _env(monkeypatch, c)
    monkeypatch.setattr(G, "latest_complete_run", lambda now=None: (None, None))
    assert GL.main([]) == 1                                              # nothing live at all: the stale alarm, once
    assert "no complete GLWU run on NOMADS in the last 24 h" in capsys.readouterr().out
    assert GL.main([]) == 3                                              # then exit 3 until the next alarm
    monkeypatch.setattr(G, "latest_complete_run", lambda now=None: (_ for _ in ()).throw(G.Throttled("429")))
    assert GL.main([]) == 3 and "did not answer" in capsys.readouterr().out
    monkeypatch.setattr(G, "latest_complete_run", lambda now=None: (_ for _ in ()).throw(F.TransportError("502")))
    assert GL.main([]) == 3                                              # an outage: quiet until the stale alarm
    assert "did not answer" in capsys.readouterr().out and P.failed_key("2026102119", GPRE) not in c.objects


def test_steps_argument_and_dry_run(offline_glwu, monkeypatch, capsys):
    assert GL.main(["--dry-run", "--steps", "0,149"]) == 0
    assert GL.main(["--dry-run", "--steps", "150"]) == 2
    assert "hourly 0..149" in capsys.readouterr().out


def test_rebuild_guard_compares_the_lake_fields_and_fill():
    c = FakeClient()
    store = P.Store(c, "b")
    man = {"run": "2026102119", "encoding": E.ENCODING, "complete": True, "frames": [{}] * 150,
           "published_utc": "2026-10-21T20:00:00Z", "fill": GL.FILL_INFO,
           "fields": {n: {"lo": f["lo"], "hi": f["hi"], "circular": f.get("circular", False)} for n, f in GL.FIELDS.items()}}
    c.objects[f"{GPRE}/2026102119/manifest-20261021T200000Z.json"] = {"body": json.dumps(man).encode(), "ct": "", "cc": ""}
    assert R.fill_guard(store, "2026102119", "2026102113", False, GL.SOURCE) is None
    man["fill"] = dict(GL.FILL_INFO, cells=3)
    c.objects[f"{GPRE}/2026102119/manifest-20261021T200000Z.json"]["body"] = json.dumps(man).encode()
    assert R.fill_guard(store, "2026102119", "2026102119", False, GL.SOURCE) == 2


# ------------------------------- the schedule ---------------------------------------------------

def test_workflow_trigger_keepalive_and_tests_paths():
    wf = os.path.join(ROOT, ".github", "workflows")
    text = open(os.path.join(wf, "model-glwu.yml"), encoding="utf-8").read()
    assert "group: model-glwu" in text and "python tools/model_frames/glwu.py" in text and "timeout-minutes: 30" in text
    assert 'cron: "*/10 * * * *"' in text and 'if [ "$rc" -eq 3 ]' in text and "eccodes=2.48.0" in text
    assert text.split("create-args: >-", 1)[1].split("cache-environment", 1)[0] == \
        open(os.path.join(wf, "model-ecmwf.yml"), encoding="utf-8").read().split("create-args: >-", 1)[1].split("cache-environment", 1)[0]
    keep = open(os.path.join(wf, "model-frames-keepalive.yml"), encoding="utf-8").read()
    assert "gh workflow enable model-glwu.yml" in keep
    tests = open(os.path.join(wf, "model-frames-tests.yml"), encoding="utf-8").read()
    assert tests.count('".github/workflows/model-glwu.yml"') == 2
    cfg = open(os.path.join(ROOT, "tools", "model_frames", "trigger", "wrangler.jsonc"), encoding="utf-8").read()
    assert '"GH_WORKFLOW": "model-frames.yml,model-points.yml,model-ecmwf.yml,model-glwu.yml"' in cfg


def test_stations_are_valid_site_ids_inside_their_lakes():
    assert len(G.STATIONS) == 72 and not any("Rip" in s for s in G.STATIONS)
    assert all(re.fullmatch(r"[A-Za-z0-9_-]{1,32}", s) for s in G.STATIONS)          # the site's station-id rule
    for sid, (lat, lon) in G.STATIONS.items():
        assert 41.0 < lat < 49.2 and -92.5 < lon < -73.0, sid


# ------------------------------- the decode glue, the station-wind guard ------------------------

class _FakeEccodes:
    """Just enough of eccodes for decode_glwu: one handle per message, keys from a dict."""
    def __init__(self, meta, values, lats=None, lons=None):
        self.meta, self.values, self.lats, self.lons = meta, values, lats, lons

    def codes_new_from_message(self, blob):
        return 1

    def codes_get(self, h, k):
        return self.meta[k]

    def codes_get_values(self, h):
        return self.values

    def codes_get_array(self, h, k):
        return self.lats if k == "latitudes" else self.lons

    def codes_release(self, h):
        pass


def test_decode_glwu_glue_maps_missing_and_enforces_identity_grid_and_size(monkeypatch):
    meta = dict(META["records"]["HTSGW:surface"])
    vals = np.full(NX * NY, 9999.0)
    vals[5] = 1.25
    monkeypatch.setitem(sys.modules, "eccodes", _FakeEccodes(meta, vals, np.zeros(3), np.ones(3)))
    out, m = D.decode_glwu(b"", "HTSGW:surface", RUN, 0)
    assert out[5] == 1.25 and np.isnan(out[6]) and np.count_nonzero(~np.isnan(out)) == 1
    _, _, (la, lo) = D.decode_glwu(b"", "HTSGW:surface", RUN, 0, coords=True)
    assert la.tolist() == [0, 0, 0] and lo.tolist() == [1, 1, 1]
    for bad in ({"shortName": "perpw"}, {"stepRange": "3"}, {"Nx": 580}, {"LoVInDegrees": 262.5}):
        monkeypatch.setitem(sys.modules, "eccodes", _FakeEccodes(dict(meta, **bad), vals))
        with pytest.raises(ValueError):
            D.decode_glwu(b"", "HTSGW:surface", RUN, 0)
    monkeypatch.setitem(sys.modules, "eccodes", _FakeEccodes(meta, vals[:-1]))
    with pytest.raises(ValueError):
        D.decode_glwu(b"", "HTSGW:surface", RUN, 0)                       # a value array of the wrong size


def test_a_station_without_a_wind_value_at_a_step_reads_none(monkeypatch):
    vals, _ice = _lake_values()
    lat, lon = _inverse_lcc(300.0, 175.0)
    monkeypatch.setattr(G, "STATIONS", {"in": (float(lat), float(lon))})
    src = GL.GlwuSource()
    monkeypatch.setattr(D, "decode_glwu", _fake_decode(vals))
    src.decode({k: b"" for k in G.RECORDS}, RUN, 0)
    later = dict(vals, **{"WIND:surface": np.where(np.isnan(vals["WIND:surface"]), np.nan, np.nan)})
    later["WIND:surface"] = vals["WIND:surface"].copy()
    later["WIND:surface"][src._cells["in"]] = np.nan                     # the cell's wind missing at a later step
    monkeypatch.setattr(D, "decode_glwu", _fake_decode(later))
    src.decode({k: b"" for k in G.RECORDS}, RUN, 1)
    assert src._winds[0]["in"] == [6.0, 0] and src._winds[1]["in"] is None    # never a NaN in winds.json


def test_the_real_grid_is_the_documented_one():
    assert GL.GRID == D.GLWU_GRID == {"cols": 831, "rows": 401, "lon0": -92.4, "lat0": 49.2, "dlon": 0.02, "dlat": -0.02,
                                       "registration": "center", "lon_periodic": False}
    assert GL.ALLOW_ALL.shape == (401, 831) and GL.CALM_M == 0.1 and GL.FILL_CELLS == 2


def test_the_temporary_dry_run_workflow_is_gone():
    assert not os.path.exists(os.path.join(ROOT, ".github", "workflows", "glwu-dryrun.yml"))
