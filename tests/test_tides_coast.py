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
    assert 15000 < len(rows) < 20000 and doc["fields"] == ["id", "lat", "lon", "tz", "name"]
    assert len({r["id"] for r in rows}) == len(rows) and all(G.valid_id(r["id"]) for r in rows)
    assert all(abs(r["lat"]) < 66 and -180 <= r["lon"] <= 180 and r["tz"] for r in rows)
    assert all(r["tz"] == "UTC" for r in rows if r["lat"] < -60), "Antarctica in UTC"
    # 15 km clear of every tide station (all of them: G80-8)
    grid = BG.Grid(C.taken_points())
    for r in rows:
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
    path.write_text(json.dumps({"fields": ["id", "lat", "lon", "tz", "name"], "points": [
        ["model:c1", 10.0, 20.0, "Africa/Lagos", "Off Somewhere"], ["model:51201", 21.0, -158.0, "Pacific/Honolulu", "x"]]}),
        encoding="utf-8")
    monkeypatch.setattr(F, "forecast_points", lambda: [{"id": "model:51201", "src": "51201", "lat": 21.0, "lon": -158.0,
                                                        "tz": "Pacific/Honolulu"}])
    pts = F.all_points(str(path))
    assert [(p["id"], p.get("set", "forecast")) for p in pts] == [("model:51201", "forecast"), ("model:c1", "coast")]
    assert pts[1]["name"] == "Off Somewhere"
    # G80-5: a missing coast file stops the job (it must not empty the coast set and delete its files)
    with pytest.raises(SystemExit, match="missing"):
        F.coast_points(str(tmp_path / "none.json"))
    assert F.coast_points(str(tmp_path / "none.json"), required=False) == []
    # 10 forecast points (5% = 0.5) and 10 coast points (50% = 5)
    points = [{"id": "f%d" % i} for i in range(10)] + [{"id": "c%d" % i, "set": "coast"} for i in range(10)]
    F.check_sets(points, {"f0"} | {"c%d" % i for i in range(5)})
    assert F.set_failures(points, {"f0"} | {"c%d" % i for i in range(4)}).keys() == {"coast"}
    with pytest.raises(SystemExit, match="coast points"):
        F.check_sets(points, {"f0"} | {"c%d" % i for i in range(4)})
    with pytest.raises(SystemExit, match="forecast points"):
        F.check_sets(points, {"c%d" % i for i in range(10)})


def test_point_names_are_place_names_or_positions():
    places = BG.Grid([(10.1, 20.1, "Some Town", 5000)])
    assert F.point_name({"lat": 10.0, "lon": 20.0, "set": "coast", "name": "40 km N of Elsewhere"}, places) == "40 km N of Elsewhere"
    assert F.point_name({"lat": 10.0, "lon": 20.0}, places) == "Off Some Town"
    assert F.point_name({"lat": -40.0, "lon": 100.0}, places) == "Sea -40.00, 100.00"


def test_g80_1_coast_names_are_unique_place_names():
    pts = [("model:a", 57.41, -6.10), ("model:b", 57.30, -5.80), ("model:c", 57.0, -5.0), ("model:d", -15.9, -145.06),
           ("model:e", 57.415, -6.105)]
    places = [(57.4129, -6.1942, "Portree", 2500)]
    got = C.names(pts, places)
    near_a, near_e = BG.km(57.4129, -6.1942, 57.41, -6.10), BG.km(57.4129, -6.1942, 57.415, -6.105)
    first, second = ("model:e", "model:a") if near_e < near_a else ("model:a", "model:e")
    assert got[first] == "Off Portree", got                    # the nearest point keeps "Off Portree"
    assert got[second].endswith("km E of Portree"), got        # the other: its distance and direction
    assert got["model:b"] == "%d km SE of Portree" % round(BG.km(57.4129, -6.1942, 57.30, -5.80))
    assert got["model:c"].endswith("SE of Portree")
    assert got["model:d"] == "Coast -15.90, -145.06", "no town within 300 km: the position"
    assert len(set(got.values())) == len(got)
    assert C.bearing(0, 0, 1, 0) == "N" and C.bearing(0, 0, 0, 1) == "E" and C.bearing(0, 0, -1, -1) == "SW"
    doc = json.load(open(C.OUT, encoding="utf-8"))
    names = [dict(zip(doc["fields"], r))["name"] for r in doc["points"]]
    assert len(set(names)) == len(names) and all(n for n in names)
    assert not any("surf" in n.lower() for n in names)


def test_g80_9_only_coastline_features_and_normalised_first_vertices():
    doc = {"features": [
        {"properties": {"featurecla": "Coastline"}, "geometry": {"type": "LineString", "coordinates": [[180.0, 10.0], [179.9, 10.0]]}},
        {"properties": {"featurecla": "Null island"}, "geometry": {"type": "LineString", "coordinates": [[0, 0], [0.01, 0]]}}]}
    lines = C.lines_of(doc)
    assert len(lines) == 1
    assert C.walk(lines)[0] == (10.0, -180.0), "the first vertex's longitude in [-180, 180)"


def _coast_grid(path):
    """A FES-like file (lat 20-22, lon 200-202 E): ocean west of lon 201 E, land east of it, and a 'lake' at
    21.5-21.6 N, 201.3-201.4 E (no value)."""
    import h5py
    lat = 20.0 + np.arange(61) / 30.0
    lon = 200.0 + np.arange(61) / 30.0
    amp = np.full((61, 61), 50.0, dtype=np.float32)
    pha = np.full((61, 61), 30.0, dtype=np.float32)
    amp[:, 30:] = np.nan
    pha[:, 30:] = np.nan
    with h5py.File(path, "w") as h:
        h["lat"] = lat
        h["lon"] = lon
        h["amplitude"] = amp
        h["phase"] = pha


def test_g80_3_7_a_coast_point_needs_the_sea_within_6_km_and_no_land_test(tmp_path):
    _coast_grid(tmp_path / "m2_fes2022.nc")
    pts = [{"id": "model:c1", "lat": 21.0, "lon": -158.99 - 0.02, "set": "coast"},     # on the shore: sea 1-2 km
           {"id": "model:c2", "lat": 21.0, "lon": -158.9, "set": "coast"},             # 10 km inland (a lake): none
           {"id": "model:c3", "lat": 20.5, "lon": -159.5, "set": "coast"},             # out at sea, land 52 km away
           {"id": "model:f1", "lat": 20.5, "lon": -159.5}]                              # a forecast point there: no
    partial = F.new_partial(pts)
    logs = []
    F.add_file(pts, partial, str(tmp_path / "m2_fes2022.nc"), log=logs.append)
    kept = F.finished(partial)
    assert set(kept) == {"model:c1", "model:c3"}, kept
    assert any("coast points: 2 of 3 with ocean within 6 km" in l for l in logs), logs


def test_g80_7_coast_points_near_a_kept_forecast_point_go():
    keep = [{"id": "model:51201", "lat": 21.0, "lon": -158.0},
            {"id": "model:c1", "lat": 21.05, "lon": -158.0, "set": "coast"},               # 5.6 km: goes
            {"id": "model:c2", "lat": 21.3, "lon": -158.0, "set": "coast"}]                # 33 km: stays
    assert [p["id"] for p in F.drop_coast_near_forecast(keep)] == ["model:51201", "model:c2"]


def test_g80_1_the_cache_key_follows_positions_and_the_version_not_names(monkeypatch):
    a = [{"id": "model:c1", "lat": 1.0, "lon": 2.0, "name": "Off A"}]
    b = [{"id": "model:c1", "lat": 1.0, "lon": 2.0, "name": "Off B"}]
    c = [{"id": "model:c1", "lat": 1.0, "lon": 2.1, "name": "Off A"}]
    assert F.cache_key(a) == F.cache_key(b) != F.cache_key(c)
    k = F.cache_key(a)
    monkeypatch.setattr(F, "CONST_VERSION", F.CONST_VERSION + "x")
    assert F.cache_key(a) != k and k.startswith("fes-consts-v2-")


def test_g80_2_one_set_short_is_cached_and_carried_over_the_other_published(tmp_path, monkeypatch):
    pts = [{"id": "model:51201", "src": "51201", "lat": 21.67, "lon": -158.12, "tz": "Pacific/Honolulu"},
           {"id": "model:c0000000001", "src": "coast", "lat": 30.0, "lon": 10.0, "tz": "UTC", "set": "coast", "name": "A"},
           {"id": "model:c0000000002", "src": "coast", "lat": 31.0, "lon": 10.0, "tz": "UTC", "set": "coast", "name": "B"}]
    monkeypatch.setattr(F, "all_points", lambda *a, **k: pts)
    monkeypatch.setattr(F.BG, "load_geonames", lambda path: BG.Grid([]))
    partial = F.new_partial(pts)
    partial["done"] = ["m2"]
    partial["consts"]["model:51201"] = {"c": [["m2", 0.3, 40.0], ["k1", 0.2, 10.0]], "km": 0.0, "coast": True}
    partial["dead"] = ["model:c0000000001", "model:c0000000002"]                      # the coast set: 0 of 2
    ppath, cpath, out = tmp_path / "partial.json", tmp_path / "consts.json", tmp_path / "out"
    ppath.write_text(json.dumps(partial), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["fes_points.py", "--partial", str(ppath), "--consts", str(cpath), "--out", str(out)])
    with pytest.raises(SystemExit, match="not published: only 0 of 2 coast points"):
        F.main()
    assert cpath.exists(), "the constants are cached although the coast set was short"
    rows = json.load(open(out / "points.json", encoding="utf-8"))["points"]
    assert [r[0] for r in rows] == ["model:51201"] and rows[0][5] == "forecast"
    # every set short: a broken land test, nothing cached
    partial["dead"] = [p["id"] for p in pts]
    partial["consts"]["model:51201"] = {"c": [], "km": 0.0, "coast": False}
    ppath.write_text(json.dumps(partial), encoding="utf-8")
    cpath.unlink()
    with pytest.raises(SystemExit, match="forecast points.*coast points"):
        F.main()
    assert not cpath.exists()


class FakeS3:
    def __init__(self, listed, old=None, fail=None, refuse=None):
        self.puts, self.deleted, self.listed, self.fail, self.old, self.refuse = [], [], listed, fail, old, refuse
        self.batches = []

    def get_object(self, Bucket, Key):
        if self.old is None:
            raise RuntimeError("An error occurred (NoSuchKey) when calling the GetObject operation")
        import io
        return {"Body": io.BytesIO(json.dumps({"points": self.old}).encode())}

    def put_object(self, Bucket, Key, Body, ContentType, CacheControl):
        if self.fail and self.fail in Key:
            raise RuntimeError("boom")
        self.puts.append(Key)
        if Key.endswith("points.json"):
            self.sent_index = json.loads(Body.decode("utf-8"))

    def list_objects_v2(self, **kw):
        keys, start = self.listed, int(kw.get("ContinuationToken") or 0)
        page = keys[start:start + 2]
        out = {"Contents": [{"Key": k} for k in page], "IsTruncated": start + 2 < len(keys)}
        if out["IsTruncated"]:
            out["NextContinuationToken"] = str(start + 2)
        return out

    def delete_objects(self, Bucket, Delete):
        self.batches.append(len(Delete["Objects"]))
        if self.refuse:
            return {"Errors": [{"Key": o["Key"], "Code": "InternalError"} for o in Delete["Objects"]]}
        self.deleted += [o["Key"] for o in Delete["Objects"]]
        return {}


def _out(tmp_path, index):
    out = tmp_path / "out"
    (out / "p").mkdir(parents=True, exist_ok=True)
    for row in index:
        (out / "p" / (row[0].split(":", 1)[1] + ".json")).write_text("{}", encoding="utf-8")
    (out / "points.json").write_text(json.dumps({"fields": ["id", "name", "lat", "lon", "tz", "set"], "points": index}),
                                     encoding="utf-8")
    return out


def test_g80_5_upload_index_last_and_only_files_neither_index_lists_go(tmp_path):
    index = [["model:c0000000001", "Off A", 1, 2, "UTC", "coast"], ["model:51201", "Off B", 3, 4, "UTC", "forecast"]]
    out = _out(tmp_path, index)
    old = [["model:51201", "Off B", 3, 4, "UTC"], ["model:old1", "x", 0, 0, "UTC"]]           # old1 dropped just now
    listed = ["tides/fes/v1/p/c0000000001.json", "tides/fes/v1/p/old1.json", "tides/fes/v1/p/51201.json",
              "tides/fes/v1/p/old2.json", "tides/fes/v1/p/old3.json"]
    s3 = FakeS3(listed, old=old)
    F.upload(str(out), index, s3=s3, bucket="b")
    assert sorted(s3.puts[:2]) == ["tides/fes/v1/p/51201.json", "tides/fes/v1/p/c0000000001.json"]
    assert s3.puts[-1] == "tides/fes/v1/points.json", "the index last"
    assert sorted(s3.deleted) == ["tides/fes/v1/p/old2.json", "tides/fes/v1/p/old3.json"], "old1 waits a run"
    # a point file that fails: no index, nothing removed
    s3b = FakeS3(listed, old=old, fail="c0000000001.json")
    with pytest.raises(RuntimeError):
        F.upload(str(out), index, s3=s3b, bucket="b")
    assert "tides/fes/v1/points.json" not in s3b.puts and s3b.deleted == []
    # batches of 250; R2's errors raise
    many = ["tides/fes/v1/p/x%d.json" % i for i in range(600)]
    s3c = FakeS3(many, old=[])
    F.upload(str(out), index, s3=s3c, bucket="b")
    assert s3c.batches == [250, 250, 100]
    with pytest.raises(SystemExit, match="refused"):
        F.upload(str(out), index, s3=FakeS3(many, old=[], refuse=True), bucket="b")
    # no index online yet: nothing carried, nothing kept for it
    s3d = FakeS3(listed, old=None)
    F.upload(str(out), index, s3=s3d, bucket="b")
    assert sorted(s3d.deleted) == ["tides/fes/v1/p/old1.json", "tides/fes/v1/p/old2.json", "tides/fes/v1/p/old3.json"]


def test_g80_2_a_short_set_keeps_its_rows_online(tmp_path):
    index = [["model:51201", "Off B", 3, 4, "UTC", "forecast"]]
    out = _out(tmp_path, index)
    old = [["model:51201", "Off B", 3, 4, "UTC"], ["model:c0000000009", "Off C", 5, 6, "UTC"]]   # an old index: no set column
    listed = ["tides/fes/v1/p/51201.json", "tides/fes/v1/p/c0000000009.json"]
    s3 = FakeS3(listed, old=old)
    F.upload(str(out), index, carry={"coast"}, s3=s3, bucket="b")
    assert [r[0] for r in s3.sent_index["points"]] == ["model:51201", "model:c0000000009"]
    assert s3.puts == ["tides/fes/v1/p/51201.json", "tides/fes/v1/points.json"], "the carried row's file is not sent"
    assert s3.deleted == []
    assert F.row_set(["model:c0000000009", "x", 0, 0, "UTC"]) == "coast" and F.row_set(["model:cape", "x", 0, 0, "UTC"]) == "forecast"


def test_g80_4_the_site_switch_hides_the_coast_points(monkeypatch):
    base = "https://bucket.example/tides/fes/v1"
    doc = {"fields": ["id", "name", "lat", "lon", "tz", "set"],
           "points": [["model:51201", "Off Haleiwa", 21.67, -158.12, "Pacific/Honolulu", "forecast"],
                      ["model:c1234567890", "Off Somewhere", 10.0, 20.0, "UTC", "coast"],
                      ["model:c1234567891", "Off Old", 11.0, 20.0, "UTC"]]}
    get = lambda url, headers=None, data=None: json.dumps(doc).encode()      # noqa: E731
    for env, want in (("1", 3), ("0", 1), (None, 3)):
        if env is None:
            monkeypatch.delenv("FES_COAST", raising=False)
        else:
            monkeypatch.setenv("FES_COAST", env)
        svc = G.GlobalTideService({}, get=get, model_base=base + "/")
        svc.refresh_models()
        assert len(svc.model_rows()) == want, env


def test_g80_8_the_vectorised_search_at_the_grid_wrap_and_edges():
    rng = np.random.default_rng(11)
    lat = -80.0 + np.arange(31) / 30.0                       # the grid's southern edge
    lon = np.arange(10800) / 30.0                            # 0 .. 359.967 E: the wrap
    for trial in range(40):
        amp = np.full((31, 10800), np.nan)
        pha = np.full((31, 10800), np.nan)
        cols = np.r_[np.arange(10790, 10800), np.arange(0, 10)]
        amp[:, cols] = rng.uniform(0.1, 1.0, (31, 20))
        pha[:, cols] = rng.uniform(0, 360, (31, 20))
        amp[rng.uniform(size=amp.shape) < 0.85] = np.nan
        plat, plon = -80.0 + rng.uniform(0, 0.4), rng.uniform(-0.3, 0.3)
        v, d, coast = F.sample(lat, lon, amp, pha, plat, plon)
        ref = _old_nearest(lat, lon, amp, pha, plat, plon)
        if d == 0.0:
            continue
        if ref is None:
            assert v is None
        else:
            assert abs(d - ref[0]) < 1e-9 and v == amp[ref[1], ref[2]] * np.exp(-1j * np.radians(pha[ref[1], ref[2]]))


def test_g80_2_7_main_drops_coast_points_by_kept_forecast_points_and_hands_a_short_set_to_the_upload(tmp_path, monkeypatch):
    pts = [{"id": "model:51201", "src": "51201", "lat": 21.67, "lon": -158.12, "tz": "Pacific/Honolulu"},
           {"id": "model:c0000000001", "src": "coast", "lat": 21.70, "lon": -158.12, "tz": "UTC", "set": "coast", "name": "A"},
           {"id": "model:c0000000002", "src": "coast", "lat": 22.5, "lon": -158.12, "tz": "UTC", "set": "coast", "name": "B"}]
    monkeypatch.setattr(F, "all_points", lambda *a, **k: pts)
    monkeypatch.setattr(F.BG, "load_geonames", lambda path: BG.Grid([]))
    sent = []
    monkeypatch.setattr(F, "upload", lambda out, index, carry=(): sent.append((list(index), set(carry))))
    good = {"c": [["m2", 0.3, 40.0], ["k1", 0.2, 10.0]], "km": 0.0, "coast": True}
    partial = F.new_partial(pts)
    partial["done"] = ["m2"]
    for p in pts:
        partial["consts"][p["id"]] = dict(good)
    ppath, cpath, out = tmp_path / "partial.json", tmp_path / "consts.json", tmp_path / "out"
    ppath.write_text(json.dumps(partial), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["fes_points.py", "--partial", str(ppath), "--consts", str(cpath), "--out", str(out),
                                      "--upload"])
    F.main()
    index, carry = sent[-1]
    assert [r[0] for r in index] == ["model:51201", "model:c0000000002"], "the coast point 3 km from 51201 goes"
    assert carry == set()
    # the coast set short (both dead): published without it, handed over to be carried, then the job fails
    partial["dead"] = ["model:c0000000001", "model:c0000000002"]
    ppath.write_text(json.dumps(partial), encoding="utf-8")
    cpath.unlink()
    with pytest.raises(SystemExit, match="not published"):
        F.main()
    index, carry = sent[-1]
    assert [r[0] for r in index] == ["model:51201"] and carry == {"coast"}
