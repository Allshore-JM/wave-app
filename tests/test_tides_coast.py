"""Plan section 113 C: FES2022 model tides along the coasts with no tide point near. The coast points
(tools/tides/build_coast_points.py -> tools/tides/coast_points.json) and the job's part (tools/tides/fes_points.py):
the vectorised nearest-ocean search, the point sets and their checks, names, the parallel upload and the removal of
point files no longer listed."""
import hashlib
import json
import math
import os
import sys

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools", "tides"))
import build_coast_points as C  # noqa: E402
import build_global as BG  # noqa: E402
import fes_points as F  # noqa: E402
import tide_global as G  # noqa: E402


# ---------------------------------------------------------------------------------------------- the coast points

def test_walk_spacing_first_vertex_and_the_date_line():
    # a line along the equator, 1 degree = 111.2 km: points at 0, 25, 50 ... km
    pts = C.walk([[(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]], spacing=25.0)
    lons = [p[1] for p in pts]
    assert pts[0] == (0.0, 0.0) and len(pts) == 1 + int(BG.km(0, 0, 0, 2) // 25.0)
    gaps = [BG.km(0, a, 0, b) for a, b in zip(lons, lons[1:])]
    assert all(abs(g - 25.0) < 0.05 for g in gaps), gaps
    # a small island: its first vertex only
    assert C.walk([[(10.0, 5.0), (10.01, 5.0), (10.0, 5.01), (10.0, 5.0)]]) == [(5.0, 10.0)]
    # across the date line: 179.9 -> -179.9 is 22 km, not 39,900
    pts = C.walk([[(179.9, 0.0), (-179.9, 0.0), (-179.5, 0.0)]], spacing=25.0)
    assert all(-180 <= p[1] <= 180 for p in pts) and len(pts) == 3
    assert all(abs(p[1]) > 179.0 or p[1] < -179.0 for p in pts)


def test_select_drops_near_taken_near_kept_polar_and_duplicates():
    taken = [(10.0, 20.0)]
    samples = [(10.0, 20.1),                                     # 11 km from a station: dropped
               (10.0, 20.2),                                     # 22 km: kept
               (10.0, 20.25),                                    # 5 km from the kept one: dropped
               (66.0, 0.0), (-66.5, 0.0),                        # polar: dropped
               (40.0, 0.0), (40.0, 0.0)]                         # the same spot twice: once
    kept = C.select(samples, taken)
    assert [(lat, lon) for _, lat, lon in kept] == [(10.0, 20.2), (40.0, 0.0)]
    assert all(G.valid_id(pid) and pid.startswith("model:c") for pid, _, _ in kept)


def test_point_ids_are_stable_and_valid():
    assert C.point_id(-8.812, 115.087) == "model:c0811929509"
    assert C.point_id(-8.8121, 115.0871) == C.point_id(-8.812, 115.087), "the same 1/100 degree: the same id"
    assert C.point_id(10.0, 180.0) == C.point_id(10.0, -180.0)
    for lat, lon in [(-65.99, -179.99), (65.99, 179.99), (0.0, 0.0)]:
        assert G.valid_id(C.point_id(lat, lon))


def test_the_coastline_must_be_the_pinned_file(tmp_path):
    bad = tmp_path / "coast.geojson"
    bad.write_text(json.dumps({"features": []}), encoding="utf-8")
    with pytest.raises(SystemExit, match="pinned"):
        C.coastline(str(bad))
    assert len(C.NE_SHA256) == 64 and C.NE_COMMIT in C.NE_URL


def test_the_committed_coast_points():
    doc = json.load(open(C.OUT, encoding="utf-8"))
    rows = [dict(zip(doc["fields"], r)) for r in doc["points"]]
    assert 15000 < len(rows) < 20000
    assert len({r["id"] for r in rows}) == len(rows) and all(G.valid_id(r["id"]) for r in rows)
    assert all(abs(r["lat"]) < 66 and -180 <= r["lon"] <= 180 and r["tz"] for r in rows)
    assert all(r["tz"] == "UTC" for r in rows if r["lat"] < -60), "Antarctica in UTC"
    # 15 km clear of every tide station and forecast point (a sample of 400: the full check is the build's own)
    grid = BG.Grid(C.taken_points())
    for r in rows[::max(1, len(rows) // 400)]:
        assert not grid.near(r["lat"], r["lon"], C.NEAR_KM - 0.01), r


# ---------------------------------------------------------------------------------------------- the job

def _old_nearest(lat, lon, amp, pha, plat, plon):
    """The cell-by-cell search fes_points.sample used before section 113 C (the reference)."""
    dlat = lat[1] - lat[0]
    i0 = int(math.floor((plat - lat[0]) / dlat))
    j0 = int(math.floor(((plon % 360.0) - lon[0]) / (lon[1] - lon[0])))
    r_cells = int(math.ceil(F.COAST_KM / (111.2 * abs(dlat)))) + 1
    ii = np.arange(max(0, i0 - r_cells), min(len(lat), i0 + r_cells + 2))
    jj = np.arange(j0 - r_cells, j0 + r_cells + 2) % len(lon)
    best = None
    for a in ii:
        for b in jj:
            if np.isfinite(amp[a, b]):
                d = F.km(plat, plon, lat[a], ((lon[b] + 180.0) % 360.0) - 180.0)
                if d <= F.NEAREST_KM and (best is None or d < best[0]):
                    best = (d, a, b)
    return best


def test_the_vectorised_nearest_ocean_cell_is_the_loops_choice():
    rng = np.random.default_rng(7)
    lat = 20.0 + np.arange(61) / 30.0
    lon = 200.0 + np.arange(61) / 30.0
    for trial in range(30):
        amp = rng.uniform(0.1, 1.0, (61, 61))
        pha = rng.uniform(0, 360, (61, 61))
        amp[rng.uniform(size=(61, 61)) < rng.uniform(0.3, 0.97)] = np.nan          # patchy "land"
        plat, plon = 20.5 + rng.uniform(0, 1), -160.0 + 0.5 + rng.uniform(0, 1)     # lon 200.5..201.5 E
        v, d, coast = F.sample(lat, lon, amp, pha, plat, plon)
        ref = _old_nearest(lat, lon, amp, pha, plat, plon)
        i0, j0 = int(math.floor((plat - lat[0]) * 30)), int(math.floor(((plon % 360) - lon[0]) * 30))
        bilinear = all(np.isfinite(amp[a, b]) for a, b in [(i0, j0), (i0 + 1, j0), (i0, j0 + 1), (i0 + 1, j0 + 1)])
        if bilinear:
            assert d == 0.0
        elif ref is None:
            assert v is None and d is None
        else:
            dd, a, b = ref
            assert abs(d - dd) < 1e-9
            assert v == amp[a, b] * np.exp(-1j * np.radians(pha[a, b])), trial


def test_all_points_and_the_checks_per_set(tmp_path, monkeypatch):
    path = tmp_path / "coast.json"
    path.write_text(json.dumps({"fields": ["id", "lat", "lon", "tz"], "points": [
        ["model:c1", 10.0, 20.0, "Africa/Lagos"], ["model:51201", 21.0, -158.0, "Pacific/Honolulu"]]}), encoding="utf-8")
    monkeypatch.setattr(F, "forecast_points", lambda: [{"id": "model:51201", "src": "51201", "lat": 21.0, "lon": -158.0,
                                                        "tz": "Pacific/Honolulu"}])
    pts = F.all_points(str(path))
    assert [(p["id"], p.get("set", "forecast")) for p in pts] == [("model:51201", "forecast"), ("model:c1", "coast")]
    assert F.coast_points(str(tmp_path / "none.json")) == []
    # 10 forecast points (5% = 0.5) and 10 coast points (50% = 5)
    points = [{"id": "f%d" % i} for i in range(10)] + [{"id": "c%d" % i, "set": "coast"} for i in range(10)]
    F.check_sets(points, {"f0"} | {"c%d" % i for i in range(5)})
    with pytest.raises(SystemExit, match="coast points"):
        F.check_sets(points, {"f0"} | {"c%d" % i for i in range(4)})
    with pytest.raises(SystemExit, match="forecast points"):
        F.check_sets(points, {"c%d" % i for i in range(10)})


def test_point_names_are_place_names_or_positions():
    places = BG.Grid([(10.1, 20.1, "Some Town", 5000)])
    assert F.point_name({"lat": 10.0, "lon": 20.0, "set": "coast"}, places) == "Off Some Town"
    assert F.point_name({"lat": -40.0, "lon": 100.0, "set": "coast"}, places) == "Coast -40.00, 100.00"
    assert F.point_name({"lat": -40.0, "lon": 100.0}, places) == "Sea -40.00, 100.00"


class FakeS3:
    def __init__(self, listed, fail=None):
        self.puts, self.deleted, self.listed, self.fail = [], [], listed, fail

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        if self.fail and self.fail in Key:
            raise RuntimeError("boom")
        self.puts.append(Key)

    def list_objects_v2(self, **kw):
        keys, start = self.listed, int(kw.get("ContinuationToken") or 0)
        page = keys[start:start + 2]
        out = {"Contents": [{"Key": k} for k in page], "IsTruncated": start + 2 < len(keys)}
        if out["IsTruncated"]:
            out["NextContinuationToken"] = str(start + 2)
        return out

    def delete_objects(self, Bucket, Delete):
        self.deleted += [o["Key"] for o in Delete["Objects"]]


def test_upload_index_last_then_stale_files_removed(tmp_path, monkeypatch):
    out = tmp_path / "out"
    (out / "p").mkdir(parents=True)
    index = [["model:c1", "Off A", 1, 2, "UTC"], ["model:51201", "Off B", 3, 4, "UTC"]]
    for row in index:
        (out / "p" / (row[0].split(":", 1)[1] + ".json")).write_text("{}", encoding="utf-8")
    (out / "points.json").write_text("{}", encoding="utf-8")
    listed = ["tides/fes/v1/p/c1.json", "tides/fes/v1/p/old1.json", "tides/fes/v1/p/51201.json", "tides/fes/v1/p/old2.json",
              "tides/fes/v1/p/old3.json"]
    s3 = FakeS3(listed)
    import types
    fake_boto3 = types.SimpleNamespace(client=lambda *a, **k: s3)
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    for k in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"):
        monkeypatch.setenv(k, "x")
    F.upload(str(out), index)
    assert sorted(s3.puts[:2]) == ["tides/fes/v1/p/51201.json", "tides/fes/v1/p/c1.json"]
    assert s3.puts[-1] == "tides/fes/v1/points.json", "the index last"
    assert sorted(s3.deleted) == ["tides/fes/v1/p/old1.json", "tides/fes/v1/p/old2.json", "tides/fes/v1/p/old3.json"]
    # a point file that fails: no index, nothing removed
    s3b = FakeS3(listed, fail="c1.json")
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda *a, **k: s3b))
    with pytest.raises(RuntimeError):
        F.upload(str(out), index)
    assert "tides/fes/v1/points.json" not in s3b.puts and s3b.deleted == []
