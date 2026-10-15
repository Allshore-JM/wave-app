"""ECMWF overlay frame job (plan section 43): offline tests (no network, no eccodes, no R2).

Real fixtures (tests/capture_ecmwf_fixtures.py, run 2026101412 step 150): the two .index files and, per record, the
eccodes metadata and the values at a few cells. They pin the record identity (surface level 0, not GFS's 1), the
grid (columns from 180 E = -180 E, so NO roll: central Oahu's land cell sits at the same (row, col) as in the GFS
frames), the index spans, the FROM convention of mwd (trade-wind swell south of Oahu from the ENE, as the wind) and
three sea-ice cells (G30a F1: Arctic and Antarctic pack with swh ~0.007 m and a 26-29 s "peak period", and waves of
2.1 m under thin ice in the marginal ice zone).
"""
import io
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import decode as D        # noqa: E402
import ecmwf as X         # noqa: E402
import ecmwf_fetch as EF  # noqa: E402
import encode as E        # noqa: E402
import fetch as F         # noqa: E402
import publish as P       # noqa: E402
import run as R           # noqa: E402
from test_job import FakeClient, _env, _listing_xml   # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "ecmwf")
RUN = datetime(2026, 10, 14, 12, tzinfo=timezone.utc)
META = json.load(open(os.path.join(FIX, "meta_150.json"), encoding="utf-8"))
REC = META["records"]
EPRE = P.ECMWF_PREFIX
ELATEST = P.latest_key(EPRE)


@pytest.fixture(autouse=True)
def _aws_up():
    EF._aws_down_until[0] = 0.0
    yield
    EF._aws_down_until[0] = 0.0


def _index(stream):
    return open(os.path.join(FIX, f"{stream}_150.index"), encoding="utf-8").read()


# ------------------------------- steps, keys, run discovery -----------------------------------

def test_steps_schedule_and_object_keys():
    assert len(EF.STEPS) == 85 and EF.STEPS[:3] == [0, 3, 6] and EF.STEPS[48] == 144 and EF.STEPS[49] == 150 and EF.STEPS[-1] == 360
    assert all(b - a == 3 for a, b in zip(EF.STEPS[:49], EF.STEPS[1:49])) and all(b - a == 6 for a, b in zip(EF.STEPS[49:], EF.STEPS[50:]))
    exp = []
    for first, last, every in EF.STEP_SCHEDULE:
        exp += list(range(first, last + 1, every))
    assert exp == EF.STEPS and X.SOURCE.steps == EF.STEPS and X.SOURCE.schedule == EF.STEP_SCHEDULE
    assert EF.stem(RUN, 150, "wave") == "20261014/12z/ifs/0p25/wave/20261014120000-150h-wave-fc"
    assert EF.stem(RUN.replace(hour=0), 0, "oper") == "20261014/00z/ifs/0p25/oper/20261014000000-0h-oper-fc"
    keys = EF.needed_keys(RUN)
    assert len(keys) == 85 * 4 == len(set(keys))
    assert "20261014/12z/ifs/0p25/oper/20261014120000-360h-oper-fc.index" in keys


def test_candidate_runs_are_00z_and_12z_only_newest_first():
    c = EF.candidate_runs(datetime(2026, 10, 14, 19, 40, tzinfo=timezone.utc))
    assert [d.strftime("%Y%m%d%H") for d in c] == ["2026101412", "2026101400", "2026101312", "2026101300"]
    c = EF.candidate_runs(datetime(2026, 10, 15, 5, 59, tzinfo=timezone.utc), lookback_cycles=2)
    assert [d.strftime("%Y%m%d%H") for d in c] == ["2026101500", "2026101412"]
    assert all(d.hour in EF.CYCLES for d in EF.candidate_runs(lookback_cycles=12))
    noon = datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc)
    assert EF.candidate_runs(noon, 1) == [noon] and EF.candidate_runs(noon.replace(hour=11, minute=59), 1) == [noon.replace(hour=0)]


def test_completeness_needs_every_object_of_both_streams_from_the_ecmwf_bucket(monkeypatch):
    keys = EF.needed_keys(RUN)
    asked = []

    def listing(missing=()):
        def fake(url, rng=None, **kw):
            asked.append(url)
            assert url.startswith(EF.S3 + "/?list-type=2&prefix=")
            prefix = url.split("prefix=", 1)[1].split("&", 1)[0]
            return 200, _listing_xml([k for k in keys if k.startswith(prefix) and k not in missing] + [prefix + "README.txt"])
        return fake
    monkeypatch.setattr(F, "_request", listing())
    assert EF.run_is_complete(RUN) and len(asked) == 2
    gone = "20261014/12z/ifs/0p25/oper/20261014120000-357h-oper-fc.grib2"
    assert gone not in keys                                          # 357 h is not a step: the 6-hourly tail is 150, 156 .. 360
    for k in ("20261014/12z/ifs/0p25/wave/20261014120000-0h-wave-fc.index",
              "20261014/12z/ifs/0p25/oper/20261014120000-360h-oper-fc.grib2"):
        monkeypatch.setattr(F, "_request", listing(missing=(k,)))
        assert EF.missing_objects(RUN) == [k]


def test_latest_complete_run_walks_back_and_raises_on_transport(monkeypatch):
    now = datetime(2026, 10, 14, 19, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(EF, "run_is_complete", lambda dt: dt.hour == 0)
    assert EF.latest_complete_run(now) == datetime(2026, 10, 14, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(EF, "run_is_complete", lambda dt: False)
    assert EF.latest_complete_run(now) is None

    def boom(dt):
        raise F.TransportError("down")
    monkeypatch.setattr(EF, "run_is_complete", boom)
    with pytest.raises(F.TransportError):
        EF.latest_complete_run(now)


# ------------------------------- the index and the ranged reads -----------------------------------

def test_index_selects_the_real_spans():
    for stream, params in EF.STREAMS.items():
        spans = EF.select(EF.parse_index(_index(stream)), params, RUN, 150, stream)
        assert set(spans) == set(params)
        for p in params:
            assert list(spans[p]) == REC[p]["span"], p


@pytest.mark.parametrize("field,value,match", [
    ("step", "144", "is not wave"), ("date", "20261013", "is not wave"), ("time", "0000", "is not wave"),
    ("levtype", "pl", "is not wave"), ("stream", "oper", "is not wave"), ("type", "cf", "is not wave"),
    ("_offset", -1, "bad span"), ("_length", 0, "bad span"), ("_length", "771718", "bad span"),
])
def test_index_guards(field, value, match):
    idx = EF.parse_index(_index("wave"))
    for rec in idx:
        if rec["param"] == "swh":
            rec[field] = value
    with pytest.raises(RuntimeError, match=match):
        EF.select(idx, EF.WAVE_PARAMS, RUN, 150, "wave")


def test_index_duplicate_or_missing_record_and_malformed_lines():
    idx = EF.parse_index(_index("wave"))
    with pytest.raises(RuntimeError, match="duplicate"):
        EF.select(idx + [dict(r) for r in idx if r["param"] == "mwd"], EF.WAVE_PARAMS, RUN, 150, "wave")
    with pytest.raises(RuntimeError, match="not in the wave index"):
        EF.select([r for r in idx if r["param"] != "pp1d"], EF.WAVE_PARAMS, RUN, 150, "wave")
    assert EF.parse_index("\n" + _index("wave") + "\n\n") == idx
    with pytest.raises(ValueError):
        EF.parse_index('{"param": "swh"}\n[1, 2]\n')
    with pytest.raises(ValueError):
        EF.parse_index('{"param": "swh"\n')


def _grib(n):
    return b"GRIB" + b"\0" * (n - 8) + b"7777"


def test_fetch_records_reads_the_exact_spans(monkeypatch):
    calls = []

    def fake(url, rng=None, **kw):
        calls.append((url, rng))
        if url.endswith(".index"):
            return 200, _index("wave").encode()
        a, b = (int(x) for x in rng.split("=")[1].split("-"))
        return 206, _grib(b - a + 1)
    monkeypatch.setattr(F, "_request", fake)
    out = EF.fetch_records(RUN, 150, "wave", EF.WAVE_PARAMS)
    assert set(out) == set(EF.WAVE_PARAMS) and all(len(out[p]) == REC[p]["span"][1] for p in out)
    stem = f"{EF.S3}/{EF.stem(RUN, 150, 'wave')}"
    assert calls[0] == (stem + ".index", None)
    off, n = REC["swh"]["span"]
    assert (stem + ".grib2", f"bytes={off}-{off + n - 1}") in calls and len(calls) == 4


@pytest.mark.parametrize("answer,match", [
    (lambda n: (200, _grib(n)), "expected 206"),
    (lambda n: (206, _grib(n - 1)), "short/long read"),
    (lambda n: (206, b"XXXX" + _grib(n)[4:]), "not a single GRIB"),
])
def test_fetch_records_guards(monkeypatch, answer, match):
    def fake(url, rng=None, **kw):
        if url.endswith(".index"):
            return 200, _index("oper").encode()
        a, b = (int(x) for x in rng.split("=")[1].split("-"))
        return answer(b - a + 1)
    monkeypatch.setattr(F, "_request", fake)
    with pytest.raises(RuntimeError, match=match):
        EF.fetch_records(RUN, 150, "oper", EF.OPER_PARAMS)


def test_fetch_falls_back_to_data_ecmwf_int_and_keeps_the_aws_error(monkeypatch):
    hosts = []

    def aws_down(url, rng=None, **kw):
        hosts.append(url.split("/")[2])
        if url.startswith(EF.S3):
            raise F.TransportError("aws 503")
        if url.endswith(".index"):
            return 200, _index("oper").encode()
        a, b = (int(x) for x in rng.split("=")[1].split("-"))
        return 206, _grib(b - a + 1)
    monkeypatch.setattr(F, "_request", aws_down)
    assert set(EF.fetch_records(RUN, 150, "oper", EF.OPER_PARAMS)) == {"10u", "10v"}
    assert hosts.count("data.ecmwf.int") == 3 and EF.HTTP == "https://data.ecmwf.int/forecasts"

    EF._aws_down_until[0] = 0.0                                      # the AWS failure above started the cooldown

    def both_gone(url, rng=None, **kw):
        if url.startswith(EF.S3):
            raise F.NotReady("404 aws")
        raise F.TransportError("ecmwf 503")
    monkeypatch.setattr(F, "_request", both_gone)
    with pytest.raises(F.NotReady, match="404 aws"):                 # the job exits 3 (not ready), never "failed"
        EF.fetch_records(RUN, 150, "oper", EF.OPER_PARAMS)


# ------------------------------- decode: identity, geometry, orientation -----------------------------------

def test_identity_of_the_real_records_and_the_gfs_table_untouched():
    for p, r in REC.items():
        D.check_identity(r["meta"], p, RUN, 150, D.EXPECT_ECMWF)
        with pytest.raises(ValueError):
            D.check_identity(r["meta"], p, RUN, 144, D.EXPECT_ECMWF)
        with pytest.raises(ValueError):
            D.check_identity(r["meta"], p, RUN.replace(hour=0), 150, D.EXPECT_ECMWF)
    with pytest.raises(ValueError):                                  # swh at level 0 is not GFS's HTSGW (level 1)
        D.check_identity(REC["swh"]["meta"], "HTSGW:surface", RUN, 150)
    with pytest.raises(ValueError):                                  # mwd delivered where swh was asked for
        D.check_identity(REC["mwd"]["meta"], "swh", RUN, 150, D.EXPECT_ECMWF)
    assert D.EXPECT["HTSGW:surface"] == ("swh", "surface", 1) and D.ECMWF_CIRCULAR == ("mwd",)


def test_geometry_of_the_real_records_and_its_guards():
    for r in REC.values():
        assert D.check_geometry_ecmwf(r["meta"]) == 0.25
    m = REC["swh"]["meta"]
    for k, v in (("longitudeOfFirstGridPointInDegrees", 0.0), ("jScansPositively", 1), ("iScansNegatively", 1),
                 ("latitudeOfFirstGridPointInDegrees", 89.75), ("latitudeOfLastGridPointInDegrees", -89.75),
                 ("jPointsAreConsecutive", 1), ("alternativeRowScanning", 1), ("iDirectionIncrementInDegrees", 0.1),
                 ("Ni", 1439), ("gridType", "reduced_gg")):
        with pytest.raises(ValueError):
            D.check_geometry_ecmwf(dict(m, **{k: v}))
    with pytest.raises(ValueError):                                  # the GFS check refuses ECMWF's 180 E start
        D.check_geometry(m)
    assert D.check_geometry_ecmwf(dict(m, longitudeOfFirstGridPointInDegrees=-180.0)) == 0.25
    tenth = dict(m, Ni=3600, Nj=1801, iDirectionIncrementInDegrees=0.1, jDirectionIncrementInDegrees=0.1)
    assert D.check_geometry_ecmwf(tenth) == 0.1


def test_no_roll_central_oahu_is_land_and_the_sea_south_of_it_carries_ene_trade_swell():
    cells = {(r, c): v for r, c, v in REC["swh"]["cells"]}
    assert cells[(274, 88)] is None                                  # 21.5 N 158.0 W: central Oahu, the GFS frames' land node
    assert cells[(284, 87)] > 1.0                                    # 19.0 N 158.25 W: open water south of Oahu
    assert D.NJ == 721 and 90 - 274 * 0.25 == 21.5 and -180 + 88 * 0.25 == -158.0
    vals = np.full((721, 1440), 9999.0)
    for r, c, v in REC["swh"]["cells"]:
        if v is not None:
            vals[r, c] = v
    g = D.to_site_grid_ecmwf(vals.ravel(), REC["swh"]["meta"])
    assert g.dtype == np.float32 and g.shape == (721, 1440)
    assert np.isnan(g[274, 88]) and g[284, 87] == np.float32(cells[(284, 87)]) and g[300, 0] == np.float32(cells[(300, 0)])
    # FROM: mean wave direction and the wind both come from the ENE there (TO would read WSW)
    mwd = {(r, c): v for r, c, v in REC["mwd"]["cells"]}[(284, 87)]
    u = {(r, c): v for r, c, v in REC["10u"]["cells"]}[(284, 87)]
    v = {(r, c): v for r, c, v in REC["10v"]["cells"]}[(284, 87)]
    wdir = float(D.wind_dir_from(np.array([u], np.float32), np.array([v], np.float32))[0])
    assert 45 <= mwd <= 90 and 45 <= wdir <= 90


def test_to_site_grid_ecmwf_missing_and_non_finite():
    m = REC["pp1d"]["meta"]
    vals = np.full(721 * 1440, 9999.0)
    vals[5] = 12.5
    g = D.to_site_grid_ecmwf(vals, m)
    assert g[0, 5] == 12.5 and np.isnan(g).sum() == 721 * 1440 - 1
    vals[6] = np.inf
    with pytest.raises(ValueError, match="non-finite"):
        D.to_site_grid_ecmwf(vals, m)


# ------------------------------- the 0.1-deg fallback (owner 2026-10-14) -----------------------------------

TENTH = dict(REC["swh"]["meta"], Ni=3600, Nj=1801, iDirectionIncrementInDegrees=0.1, jDirectionIncrementInDegrees=0.1,
             longitudeOfLastGridPointInDegrees=179.9)


def _tenth_field(fn):
    lat = 90 - 0.1 * np.arange(1801)[:, None]
    lon = -180 + 0.1 * np.arange(3600)[None, :]
    return fn(lat, lon)


def test_a_tenth_degree_field_is_regridded_onto_the_site_grid():
    lin = _tenth_field(lambda lat, lon: 3.0 + 0.02 * lat + 0.0 * lon)          # linear: bilinear reproduces it exactly
    g = D.to_site_grid_ecmwf(lin.ravel(), TENTH)
    lat = 90 - 0.25 * np.arange(721)[:, None]
    assert g.shape == (721, 1440) and np.allclose(g, np.broadcast_to(3.0 + 0.02 * lat, (721, 1440)), atol=1e-5)
    # exact at shared nodes (every second site column), halfway between the others; site column 1439 = 179.75 E lies
    # between the source's 179.7 and 179.8, so nothing reads across the -180 / 180 seam
    f = _tenth_field(lambda lat, lon: lon + 0 * lat)
    g = D.regrid_to_site(f)
    assert g[100, 0] == -180.0 and abs(g[100, 2] + 179.5) < 1e-4 and abs(g[100, 1] + 179.75) < 1e-4 and abs(g[100, 1439] - 179.75) < 1e-4
    seam = np.zeros((1801, 3600))
    seam[:, 0] = 10.0
    assert D.regrid_to_site(seam)[300, 0] == 10.0 and D.regrid_to_site(seam)[300, 1439] == 0.0


def test_regrid_missing_rule_and_angles_are_never_averaged():
    f = np.full((1801, 3600), 4.0)
    f[:, 3] = np.nan                                                 # site column 1 sits at source column 2.5: half its weight absent
    g = D.regrid_to_site(f)
    assert g[10, 0] == 4.0 and g[10, 1] == 4.0                       # the present half (weight 0.5) carries it
    f[11 * 5 // 2 + 1, 2] = np.nan                                   # row 11 sits at source row 27.5: now 3 of its 4 neighbours absent
    assert np.isnan(D.regrid_to_site(f)[11, 1]) and D.regrid_to_site(f)[10, 1] == 4.0
    f[:, 2] = np.nan
    assert np.isnan(D.regrid_to_site(f)[10, 1])
    ang = np.full((1801, 3600), 350.0)
    ang[:, 3::5] = 10.0                                              # site column 1 sits at source 2.5: 2 and 3 equally near
    out = D.regrid_to_site(ang, circular=True)
    assert set(np.unique(out[:, 1])) <= {350.0, 10.0}                 # a neighbour's angle, never their mean (180)
    vals = _tenth_field(lambda lat, lon: (lon % 360) + 0 * lat)
    g = D.to_site_grid_ecmwf(vals.ravel(), dict(TENTH, shortName="mwd"), circular=True)
    assert not np.any((g > 1) & (g < 359) & (np.abs(g - (np.arange(1440) * 0.25 - 180) % 360) > 0.06))


def test_decode_ecmwf_passes_the_circular_flag_for_mwd_only(monkeypatch):
    ang = np.full((1801, 3600), 350.0)
    ang[:, 3::5] = 10.0                                              # site column 1 sits halfway between 350 and 10
    for p in ("mwd", "swh"):
        meta = dict(TENTH, shortName=p, stepRange="150", dataDate=20261014, dataTime=1200, gridType="regular_ll")
        monkeypatch.setattr(D, "_read", lambda blob, keys, meta=meta: (meta, ang.ravel()))
        g, _ = D.decode_ecmwf(b"x", p, RUN, 150)
        assert (g[:, 1] == 180.0).any() == (p == "swh"), p          # a linear mean of 350 and 10 is 180: never for an angle


# ------------------------------- the build: the real pipeline under the ECMWF prefix -----------------------------------

@pytest.fixture
def ecmwf_offline(monkeypatch):
    rows, cols = 73, 144
    small = np.tile(np.linspace(0.5, 9.5, cols, dtype=np.float32), (rows, 1))
    small[:, 100:120] = np.nan
    seen = []
    monkeypatch.setattr(EF, "fetch_records", lambda run_dt, step, stream, params: {p: p.encode() for p in params})

    def dec(blob, param, run_dt, step):
        seen.append((blob.decode(), param, step))
        return small, {}
    monkeypatch.setattr(D, "decode_ecmwf", dec)
    monkeypatch.setattr(E, "fill_allow", lambda: np.ones((rows, cols), bool))
    monkeypatch.setattr(F, "fetch_records", lambda *a: (_ for _ in ()).throw(AssertionError("the GFS fetch was used")))
    return seen


def test_complete_ecmwf_build_publishes_under_its_own_prefix_and_pointer(ecmwf_offline):
    c = FakeClient()
    gfs_latest = b'{"run":"2026101418","complete":true}'
    c.objects[P.LATEST_KEY] = {"body": gfs_latest, "ct": "", "cc": ""}
    m = R.build_and_publish(P.Store(c, "b"), RUN, EF.STEPS, log=lambda *a: None, src=X.SOURCE)
    puts = [k for op, k in c.log if op == "put"]
    assert c.objects[P.LATEST_KEY]["body"] == gfs_latest                         # the GFS pointer is never touched
    assert all(k.startswith(f"{EPRE}/2026101412/") for k in puts[:-1]) and puts[-1] == ELATEST
    assert puts[-2].startswith(f"{EPRE}/2026101412/manifest-") and puts[-3].startswith(f"{EPRE}/2026101412/stats-")
    assert len(puts) == 85 * (4 * 2 + 1) + 3
    assert f"{EPRE}/2026101412/half/wdir/f360.png" in puts and f"{EPRE}/2026101412/hs/f147.png" not in puts
    latest = json.loads(c.objects[ELATEST]["body"])
    assert latest == {"run": "2026101412", "manifest": puts[-2], "complete": True, "encoding": E.ENCODING,
                      "published_utc": m["published_utc"], "frames": 85}
    man = json.loads(c.objects[latest["manifest"]]["body"])
    assert man["complete"] and man["expected_frames"] == 85 and man["frame_schedule"] == [[0, 144, 3], [150, 360, 6]]
    assert man["files"]["template"] == f"{EPRE}/2026101412/{{res}}{{field}}/f{{step:03d}}.png"
    assert man["frames"][49] == {"step": 150, "valid_utc": "2026-10-20T18:00:00Z"} and man["frames"][-1]["valid_utc"] == "2026-10-29T12:00:00Z"
    assert man["model"]["name"] == "ECMWF IFS wave (ecWAM) + IFS" and "CC BY 4.0" in man["model"]["attribution"]
    assert "modified" in man["model"]["attribution"] and man["model"]["licence"] == "https://creativecommons.org/licenses/by/4.0/"
    assert man["model"]["fields"]["pdir"]["definition"].startswith("Mean wave direction") and man["model"]["fields"]["tp"]["grib"] == "pp1d"
    assert man["fields"] == json.loads(json.dumps({n: {k: f[k] for k in R.FIELD_KEYS if k in f} for n, f in E.FIELDS.items()}))
    assert man["fill"] == E.FILL_INFO and man["grid"] == R.GRID_FULL and man["encoding"] == E.ENCODING
    # every field from its own record, in the GFS order
    assert ecmwf_offline[:5] == [("swh", "swh", 0), ("pp1d", "pp1d", 0), ("mwd", "mwd", 0), ("10u", "10u", 0), ("10v", "10v", 0)]


def test_a_partial_ecmwf_build_never_points(ecmwf_offline):
    c = FakeClient()
    m = R.build_and_publish(P.Store(c, "b"), RUN, [0, 150], log=lambda *a: None, src=X.SOURCE)
    assert m["complete"] is False and ELATEST not in c.objects
    assert any(k.startswith(f"{EPRE}/2026101412/partial-") for k in c.objects)


def test_ecmwf_wind_fields_come_from_10u_and_10v(monkeypatch):
    monkeypatch.setattr(EF, "fetch_records", lambda run_dt, step, stream, params: {p: p.encode() for p in params})
    u = np.full((721, 1440), 3.0, np.float32)
    v = np.full((721, 1440), -4.0, np.float32)
    base = np.full((721, 1440), 1.0, np.float32)
    monkeypatch.setattr(D, "decode_ecmwf", lambda blob, p, run_dt, step: ({"10u": u, "10v": v}.get(p, base), {}))
    g = X.SOURCE.decode(X.SOURCE.fetch(RUN, 0), RUN, 0)
    assert list(g) == ["hs", "tp", "pdir", "wind", "wdir"]
    assert np.allclose(g["wind"], 5.0) and np.allclose(g["wdir"], np.degrees(np.arctan2(-3.0, 4.0)) % 360)


# ------------------------------- main() with the ECMWF source -----------------------------------

def _man(c, run, stamp, body):
    key = f"{EPRE}/{run}/manifest-{stamp}.json"
    c.objects[key] = {"body": json.dumps(body).encode(), "ct": "", "cc": ""}
    return key


def test_main_skips_a_live_ecmwf_run_and_never_reads_the_gfs_pointer(monkeypatch, capsys):
    monkeypatch.setattr(EF, "latest_complete_run", lambda: RUN)
    c = FakeClient(); _env(monkeypatch, c)
    fresh = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    c.objects[ELATEST] = {"body": json.dumps({"run": "2026101412", "complete": True, "published_utc": fresh}).encode(), "ct": "", "cc": ""}
    c.objects[P.LATEST_KEY] = {"body": b'{"run":"2026101500","complete":true}', "ct": "", "cc": ""}   # a NEWER gfs run: irrelevant
    n = len(c.log)
    assert X.main([]) == 0 and "already live" in capsys.readouterr().out and len(c.log) == n


def test_main_exit_3_without_a_run_and_records_failures_under_its_prefix(monkeypatch, capsys, ecmwf_offline):
    monkeypatch.setattr(EF, "latest_complete_run", lambda: None)
    assert X.main(["--dry-run"]) == 3 and "no complete ECMWF run" in capsys.readouterr().out
    monkeypatch.setattr(EF, "latest_complete_run", lambda: RUN)
    c = FakeClient(); _env(monkeypatch, c)

    def broken(run_dt, step, stream, params):
        raise RuntimeError("index garbled")
    monkeypatch.setattr(EF, "fetch_records", broken)
    with pytest.raises(RuntimeError):
        X.main([])
    assert P.failed_key("2026101412", EPRE) in c.objects and P.failed_key("2026101412") not in c.objects
    assert X.main([]) == 0 and "failed recently" in capsys.readouterr().out     # backed off under its own record

    def gone(run_dt, step, stream, params):
        raise F.NotReady("404")
    monkeypatch.setattr(EF, "fetch_records", gone)
    c2 = FakeClient(); _env(monkeypatch, c2)
    assert X.main([]) == 3 and P.notready_key("2026101412", EPRE) in c2.objects


def test_main_publishes_and_prunes_only_its_own_prefix(monkeypatch, capsys, ecmwf_offline):
    monkeypatch.setattr(EF, "latest_complete_run", lambda: RUN)
    c = FakeClient(); _env(monkeypatch, c)
    for run in ("2026101000", "2026101012", "2026101100", "2026101112", "2026101200"):
        _man(c, run, "x", {"run": run})
    gfs_old = f"{P.PREFIX}/2026100100/manifest-x.json"
    c.objects[gfs_old] = {"body": b"{}", "ct": "", "cc": ""}
    assert X.main(["--keep", "4"]) == 0
    out = capsys.readouterr().out
    assert "pruned: ['2026101000', '2026101012']" in out and "legacy" not in out
    assert gfs_old in c.objects and json.loads(c.objects[ELATEST]["body"])["run"] == "2026101412"
    assert not any(k.startswith(f"{EPRE}/2026101000/") for k in c.objects)


def test_main_never_regresses_and_steps_must_be_model_hours(monkeypatch, capsys):
    monkeypatch.setattr(EF, "latest_complete_run", lambda: RUN)
    c = FakeClient(); _env(monkeypatch, c)
    c.objects[ELATEST] = {"body": b'{"run":"2026101500","complete":true}', "ct": "", "cc": ""}
    assert X.main([]) == 0 and "not regressing" in capsys.readouterr().out
    assert X.main(["--dry-run", "--steps", "0,147"]) == 2
    assert "every 3 h 0..144, then every 6 h to 360" in capsys.readouterr().out
    assert X.main(["--steps", "0,150"]) == 2                                     # a subset is never published live


def test_the_fill_guard_reads_and_repairs_the_ecmwf_prefix(monkeypatch, capsys, ecmwf_offline):
    monkeypatch.setattr(EF, "latest_complete_run", lambda: RUN)
    c = FakeClient(); _env(monkeypatch, c)
    frames = [{"step": s, "valid_utc": "x"} for s in EF.STEPS]
    other = dict(E.FILL_INFO, version=E.FILL_VERSION + 1)
    mkey = _man(c, "2026101412", "20261014T200000Z", {"run": "2026101412", "complete": True, "encoding": E.ENCODING,
                                                       "frames": frames, "published_utc": "2026-10-14T20:00:00Z", "fill": other})
    assert X.main([]) == 0 and "pointer repaired" in capsys.readouterr().out
    assert json.loads(c.objects[ELATEST]["body"])["manifest"] == mkey
    assert R.fill_guard(P.Store(c, "b"), "2026101412", "2026101412", False, X.SOURCE) == 2     # the live run is never rewritten
    assert R.fill_guard(P.Store(c, "b"), "2026101412", None, False) is None                    # the GFS prefix has no such run


# ------------------------------- workflow, trigger, keepalive -----------------------------------

def test_the_ecmwf_workflow_mirrors_the_frames_workflow():
    wf = os.path.join(ROOT, ".github", "workflows")
    gfs = open(os.path.join(wf, "model-frames.yml"), encoding="utf-8").read()
    ec = open(os.path.join(wf, "model-ecmwf.yml"), encoding="utf-8").read()
    assert "python tools/model_frames/ecmwf.py" in ec and "group: model-ecmwf" in ec and "cron: \"*/10 * * * *\"" in ec
    env = lambda t: t.split("create-args: >-", 1)[1].split("cache-environment", 1)[0].split()      # noqa: E731
    assert env(ec) == env(gfs)                                       # the pinned env that decodes CCSDS (eccodes 2.48)
    uses = lambda t: sorted(set(l.split("uses:", 1)[1].split()[0] for l in t.splitlines() if "uses:" in l))   # noqa: E731
    assert uses(ec) == uses(gfs) and "runs-on: ubuntu-24.04" in ec
    keep = open(os.path.join(wf, "model-frames-keepalive.yml"), encoding="utf-8").read()
    assert "gh workflow enable model-ecmwf.yml" in keep
    tests = open(os.path.join(wf, "model-frames-tests.yml"), encoding="utf-8").read()
    assert tests.count('".github/workflows/model-ecmwf.yml"') == 2
    cfg = open(os.path.join(ROOT, "tools", "model_frames", "trigger", "wrangler.jsonc"), encoding="utf-8").read()
    assert '"GH_WORKFLOW": "model-frames.yml,model-points.yml,model-ecmwf.yml"' in cfg


# ------------------------------- G30a fix round -----------------------------------

def _cell(p, r, c):
    return {(rr, cc): v for rr, cc, v in REC[p]["cells"]}[(r, c)]


def test_g30a_f1_sea_ice_noise_is_left_empty_and_waves_under_thin_ice_are_kept():
    cells = [(22, 1329), (638, 630), (596, 1042), (284, 87), (300, 0)]
    grids = {n: np.full((721, 1440), np.nan, np.float32) for n in ("hs", "tp", "pdir")}
    for r, c in cells:
        grids["hs"][r, c], grids["tp"][r, c], grids["pdir"][r, c] = _cell("swh", r, c), _cell("pp1d", r, c), _cell("mwd", r, c)
    grids["wind"] = np.full((721, 1440), 7.0, np.float32)
    assert _cell("pp1d", 22, 1329) > 22 and _cell("swh", 22, 1329) < 0.01             # the noise the reviewer found
    assert X.mask_calm(grids) == 2
    for r, c in ((22, 1329), (638, 630)):                                              # the pack: empty, like GFS
        assert np.isnan(grids["hs"][r, c]) and np.isnan(grids["tp"][r, c]) and np.isnan(grids["pdir"][r, c])
    for r, c in ((596, 1042), (284, 87), (300, 0)):                                    # real waves (incl. 2.1 m under ice)
        assert grids["hs"][r, c] == np.float32(_cell("swh", r, c)) and grids["tp"][r, c] == np.float32(_cell("pp1d", r, c))
    assert np.all(grids["wind"] == 7.0) and all(grids[n].dtype == np.float32 for n in ("hs", "tp", "pdir"))


def test_mask_calm_rule_edges_and_direction_wrap():
    hs = np.array([[0.0999, 0.1, np.nan, 3.0, 2.0]], np.float32)
    g = {"hs": hs, "tp": np.full((1, 5), 9.0, np.float32), "pdir": np.array([[10, 20, 30, 360.0002, -0.5]], np.float32)}
    assert X.mask_calm(g) == 1                                                         # 0.0999 emptied; NaN was already empty
    assert np.isnan(g["hs"][0, 0]) and g["hs"][0, 1] == np.float32(0.1) and np.isnan(g["tp"][0, 2]) and np.isnan(g["pdir"][0, 2])
    assert 0 <= g["pdir"][0, 3] < 0.001 and abs(g["pdir"][0, 4] - 359.5) < 1e-4
    assert X.CALM_M == R.PDIR_WAVES_M == 0.1


def test_the_source_applies_the_calm_mask(monkeypatch):
    monkeypatch.setattr(EF, "fetch_records", lambda run_dt, step, stream, params: {p: p.encode() for p in params})
    hs = np.full((721, 1440), 2.0, np.float32)
    hs[:40] = 0.02
    other = np.full((721, 1440), 25.0, np.float32)
    monkeypatch.setattr(D, "decode_ecmwf", lambda blob, p, run_dt, step: (hs if p == "swh" else other, {}))
    g = X.SOURCE.decode(X.SOURCE.fetch(RUN, 0), RUN, 0)
    assert np.isnan(g["tp"][:40]).all() and np.isnan(g["pdir"][:40]).all() and (g["tp"][40:] == 25.0).all()
    assert (g["wind"][:40] > 0).all()                                                  # wind keeps the ice


def test_g30a_f4_the_credit_carries_ecmwfs_notice_block():
    a = X.SOURCE.model["attribution"]
    year = datetime.now(timezone.utc).year
    assert a.startswith(f"\u00a9 {year} European Centre for Medium-Range Weather Forecasts (ECMWF). Source: www.ecmwf.int.")
    for part in ("Licence: CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/)", "modified by Allshore Surf",
                 "ECMWF does not accept any liability whatsoever for any error or omission in the data, their availability, "
                 "or for any loss or damage arising from their use", "does not endorse this product"):
        assert part in a, part
    assert X.SOURCE.model["fields"] == X.MODEL["fields"] and X.SOURCE.model["licence"] == X.LICENCE

def test_the_copyright_year_follows_the_clock(monkeypatch):
    monkeypatch.setattr(X, "_now", lambda: datetime(2031, 1, 2, tzinfo=timezone.utc))
    assert X.SOURCE.model["attribution"].startswith("© 2031 European Centre")


def test_g30a_f6_a_failing_mirror_is_tried_twice_then_skipped_for_ten_minutes(monkeypatch):
    calls = []

    def fake(url, rng=None, tries=4, **kw):
        calls.append((url.split("/")[2], tries))
        if url.startswith(EF.S3):
            raise F.TransportError("aws 503")
        return 200, _index("wave").encode()
    monkeypatch.setattr(F, "_request", fake)
    clock = [1000.0]
    monkeypatch.setattr(EF.time, "monotonic", lambda: clock[0])
    EF._get("x.index")
    assert calls == [("ecmwf-forecasts.s3.eu-central-1.amazonaws.com", 2), ("data.ecmwf.int", 4)]
    calls.clear()
    EF._get("y.index")
    assert calls == [("data.ecmwf.int", 4)]                                            # skipped while down
    clock[0] += EF.AWS_COOLDOWN_S + 1
    calls.clear()
    EF._get("z.index")
    assert calls[0][0] == "ecmwf-forecasts.s3.eu-central-1.amazonaws.com"             # tried again afterwards


def test_an_aws_404_still_tries_the_second_host_without_a_cooldown(monkeypatch):
    calls = []

    def fake(url, rng=None, **kw):
        calls.append(url.split("/")[2])
        if url.startswith(EF.S3):
            raise F.NotReady("404 aws")
        return 200, b"ok"
    monkeypatch.setattr(F, "_request", fake)
    assert EF._get("a") == (200, b"ok") and EF._get("b") == (200, b"ok")
    assert calls == ["ecmwf-forecasts.s3.eu-central-1.amazonaws.com", "data.ecmwf.int"] * 2


def test_record_trailer_span_cap_and_an_offset_of_zero(monkeypatch):
    def fake(url, rng=None, **kw):
        if url.endswith(".index"):
            return 200, _index("oper").encode()
        a, b = (int(x) for x in rng.split("=")[1].split("-"))
        return 206, _grib(b - a + 1)[:-4] + b"7776"
    monkeypatch.setattr(F, "_request", fake)
    with pytest.raises(RuntimeError, match="not a single GRIB"):
        EF.fetch_records(RUN, 150, "oper", EF.OPER_PARAMS)
    idx = EF.parse_index(_index("wave"))
    first = [r for r in idx if r["_offset"] == 0][0]
    assert EF.select(idx, (first["param"],), RUN, 150, "wave") == {first["param"]: (0, first["_length"])}
    big = [dict(r, _length=64 * 2 ** 20) if r["param"] == "swh" else r for r in idx]
    with pytest.raises(RuntimeError, match="bad span"):
        EF.select(big, EF.WAVE_PARAMS, RUN, 150, "wave")


def test_decode_ecmwf_enforces_the_record_identity(monkeypatch):
    meta = dict(REC["swh"]["meta"])
    monkeypatch.setattr(D, "_read", lambda blob, keys: (meta, np.full(721 * 1440, 9999.0)))
    D.decode_ecmwf(b"x", "swh", RUN, 150)
    for when, step, p in ((RUN, 144, "swh"), (RUN.replace(hour=0), 150, "swh"), (RUN, 150, "mwd")):
        with pytest.raises(ValueError):
            D.decode_ecmwf(b"x", p, when, step)


def _pointer(c, published):
    c.objects[ELATEST] = {"body": json.dumps({"run": "2026101300", "complete": True, "published_utc": published}).encode(), "ct": "", "cc": ""}


def test_g30a_f2_a_long_stretch_without_a_run_fails_loudly(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(EF, "latest_complete_run", lambda: None)
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    now = datetime.now(timezone.utc)
    c = FakeClient(); _env(monkeypatch, c)
    _pointer(c, (now - timedelta(hours=20)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert X.main([]) == 3 and not summary.exists()                    # a healthy gap between runs
    _pointer(c, (now - timedelta(hours=31)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert X.main([]) == 1 and "31 h old" in summary.read_text() and "WARNING" in summary.read_text()
    c2 = FakeClient(); _env(monkeypatch, c2)
    c2.objects[P.LATEST_KEY] = {"body": b'{"run":"2026101500","complete":true,"published_utc":"2026-10-15T05:00:00Z"}', "ct": "", "cc": ""}
    assert X.main([]) == 1 and "nothing is live" in capsys.readouterr().out    # a live GFS pointer is not the ECMWF one
    assert X.main(["--dry-run"]) == 3                                  # a dry run never fails on age


def test_gfs_without_a_run_never_reads_the_store_and_keeps_its_messages(monkeypatch, capsys):
    monkeypatch.setattr(R.F, "latest_complete_run", lambda: None)
    monkeypatch.setattr(R, "_store_from_env", lambda: (_ for _ in ()).throw(AssertionError("store read")))
    assert R.main([]) == 3 and "no complete run on S3 in the last 48 h" in capsys.readouterr().out
    assert R.main(["--dry-run", "--steps", "0,2,121"]) == 2
    assert "hourly 0..120, then every 3 h to 384" in capsys.readouterr().out


def test_the_temporary_dry_run_plumbing_is_gone():
    """The branch-only runner proof (plan section 43, step 1) never reaches production."""
    assert not os.path.exists(os.path.join(ROOT, ".github", "workflows", "ecmwf-dryrun.yml"))
    assert not os.path.exists(os.path.join(ROOT, "tools", "model_frames", "ecmwf_dryrun_check.py"))


def _ago(h):
    return (datetime.now(timezone.utc) - timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_g30a_r3_the_alarm_also_fires_when_the_stale_run_is_still_the_newest(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(EF, "latest_complete_run", lambda: datetime(2026, 10, 13, 0, tzinfo=timezone.utc))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "s.md"))
    c = FakeClient(); _env(monkeypatch, c)
    c.objects[ELATEST] = {"body": json.dumps({"run": "2026101300", "complete": True, "published_utc": _ago(29)}).encode(), "ct": "", "cc": ""}
    assert X.main([]) == 0 and R.stale_alarm_key(EPRE) not in c.objects              # 29 h: a slow day, not an alarm
    c.objects[ELATEST] = {"body": json.dumps({"run": "2026101300", "complete": True, "published_utc": _ago(31)}).encode(), "ct": "", "cc": ""}
    assert X.main([]) == 1 and "no newer ECMWF run than the live 2026101300" in capsys.readouterr().out
    rec = json.loads(c.objects[R.stale_alarm_key(EPRE)]["body"])
    assert rec["last_utc"] and "31 h old" in rec["message"] and c.objects[R.stale_alarm_key(EPRE)]["cc"] == P.POINTER
    assert X.main([]) == 0 and "already reported" in capsys.readouterr().out          # throttled: one mail per 6 h, not 36
    rec["last_utc"] = _ago(R.STALE_ALARM_EVERY_H + 0.1)
    c.objects[R.stale_alarm_key(EPRE)]["body"] = json.dumps(rec).encode()
    assert X.main([]) == 1                                                             # and again after 6 h
    assert not any(k.startswith(P.PREFIX) for k in c.objects)                          # nothing under the GFS prefix


def test_g30a_r3_the_no_run_alarm_is_throttled_too(monkeypatch, capsys):
    monkeypatch.setattr(EF, "latest_complete_run", lambda: None)
    c = FakeClient(); _env(monkeypatch, c)
    assert X.main([]) == 1 and X.main([]) == 3 and "already reported" in capsys.readouterr().out


def test_g30a_r4_an_unreadable_pointer_without_a_run_is_a_failure(monkeypatch, capsys):
    monkeypatch.setattr(EF, "latest_complete_run", lambda: None)

    class Broken(FakeClient):
        def get_object(self, Bucket, Key):
            raise OSError("R2 down")
    _env(monkeypatch, Broken())
    assert X.main([]) == 1 and "could not read the live pointer (OSError)" in capsys.readouterr().out
