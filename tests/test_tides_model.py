"""Plan section 112, step 4: FES2022b model points. The GitHub job (tools/tides/fes_points.py) on a small synthetic
FES-like grid, and the site's reading of the published predictions (tide_global.GlobalTideService, model ids)."""
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
import fes_points as F  # noqa: E402
import tide_global as G  # noqa: E402
import tide_harmonic as H  # noqa: E402
import tide_sources as T  # noqa: E402

NOW = 1795700000.0


def _grid_file(path, amp_cm, pha_deg, land=None):
    """A FES-like file: 1/30-deg cells over 20-21 N, 200-201 E (lon 0..360), amplitude cm / phase deg, land NaN."""
    import h5py
    lat = 20.0 + np.arange(31) / 30.0
    lon = 200.0 + np.arange(31) / 30.0
    amp = np.full((31, 31), amp_cm, dtype=np.float32)
    pha = np.full((31, 31), pha_deg, dtype=np.float32)
    if land is not None:
        amp[land] = np.nan
        pha[land] = np.nan
    with h5py.File(path, "w") as h:
        h["lat"] = lat
        h["lon"] = lon
        h["amplitude"] = amp
        h["phase"] = pha


def test_sample_bilinear_nearest_and_coast(tmp_path):
    land = (slice(0, 31), slice(20, 31))                       # east of 200.667 E is land
    _grid_file(tmp_path / "m2_fes2022.nc", 50.0, 30.0, land)
    lat, lon, amp, pha = F.read_grid(str(tmp_path / "m2_fes2022.nc"))
    assert np.nanmax(amp) == pytest.approx(0.5)                # cm -> m
    v, d, coast = F.sample(lat, lon, amp, pha, 20.5, -159.8)    # 200.2 E: open water, land 15 km east
    assert abs(v) == pytest.approx(0.5, abs=1e-6) and math.degrees(-np.angle(v)) == pytest.approx(30.0)
    assert d == 0.0 and coast
    v, d, coast = F.sample(lat, lon, amp, pha, 20.5, -159.32)   # just on land: the nearest ocean cell
    assert v is not None and 0 < d <= F.NEAREST_KM
    v, d, coast = F.sample(lat, lon, amp, pha, 20.5, -159.05)   # deep inland: none within 15 km
    assert v is None


def test_sample_far_from_land_is_not_coast(tmp_path):
    _grid_file(tmp_path / "m2_fes2022.nc", 50.0, 30.0)
    lat, lon, amp, pha = F.read_grid(str(tmp_path / "m2_fes2022.nc"))
    v, d, coast = F.sample(lat, lon, amp, pha, 20.5, -159.5)
    assert v is not None and not coast


def test_extract_keeps_coastal_points_with_values(tmp_path):
    land = (slice(0, 31), slice(20, 31))
    _grid_file(tmp_path / "m2_fes2022.nc", 50.0, 30.0, land)
    _grid_file(tmp_path / "s2_fes2022.nc", 20.0, 60.0, land)
    pts = [{"id": "model:a", "lat": 20.5, "lon": -159.8}, {"id": "model:b", "lat": 20.5, "lon": -159.05}]
    got = F.extract(pts, str(tmp_path), log=lambda *a: None)
    assert set(got) == {"model:a"}
    names = {c[0] for c in got["model:a"]["c"]}
    assert names == {"m2", "s2"}


def test_predict_point_cm_above_lat():
    prep = H.prepare([("m2", 1.0, 0.0), ("s2", 0.3, 45.0)], convention="fes")
    begin = int(T.begin_of(NOW)) - 86400
    doc = F.predict_point(prep, 1.4, begin, days=3)
    assert doc["step"] == 1800 and len(doc["v"]) == 3 * 48 + 1 and doc["begin"] == begin
    assert all(isinstance(x, int) for x in doc["v"]) and min(doc["v"]) >= 0
    assert [k for _, _, k in doc["hilo"]][:2] in (["H", "L"], ["L", "H"])


def _model_doc(begin, days=47):
    prep = H.prepare([("m2", 1.0, 0.0)], convention="fes")
    return F.predict_point(prep, 1.05, begin, days=days)


class FakeGet:
    def __init__(self, files):
        self.files, self.calls = files, []

    def __call__(self, url, headers=None, data=None):
        self.calls.append(url)
        if url not in self.files:
            raise RuntimeError("404")
        v = self.files[url]
        if isinstance(v, Exception):
            raise v
        return json.dumps(v).encode()


class Clock:
    def __init__(self, t=NOW):
        self.t = t

    def __call__(self):
        return self.t


BASE = "https://bucket.example/tides/fes/v1"
POINTS = {"fields": ["id", "name", "lat", "lon", "tz"],
          "points": [["model:51201", "Off Haleiwa", 21.67, -158.12, "Pacific/Honolulu"],
                     ["bad id", "x", 0, 0, "UTC"]]}


def test_model_list_refresh_hourly_and_keeps_on_failure():
    clock = Clock()
    get = FakeGet({BASE + "/points.json": POINTS})
    svc = G.GlobalTideService({}, get=get, now=clock, model_base=BASE + "/")
    assert svc.refresh_models() is True and [r[0] for r in svc.model_rows()] == ["model:51201"]
    assert svc.model_rows()[0][4] == "M"
    assert svc.refresh_models() is False and len(get.calls) == 1          # within the hour: not asked
    clock.t += G.MODEL_LIST_TTL_S + 1
    get.files[BASE + "/points.json"] = RuntimeError("down")
    assert svc.refresh_models() is False and svc.models                 # a failure keeps the list in hand
    assert G.GlobalTideService({}, get=get, now=clock).refresh_models() is False      # no base: nothing


def test_model_forecast_slices_the_window():
    begin = int(T.begin_of(NOW))
    file_begin = begin - 2 * 86400
    doc = _model_doc(file_begin)
    get = FakeGet({BASE + "/points.json": POINTS, BASE + "/p/51201.json": doc})
    svc = G.GlobalTideService({}, get=get, now=Clock(), model_base=BASE)
    svc.refresh_models()
    status, p = svc.forecast("model:51201")
    assert status == "ok" and p["method"] == "model" and p["datum"] == "LAT (computed)" and p["type"] == "M"
    assert p["begin"] == begin - T.LEAD_S and len(p["v"]) == T.POINTS
    i0 = (p["begin"] - file_begin) // 1800
    assert p["v"][0] == doc["v"][i0] / 100.0
    assert all(begin - 2 * T.LEAD_S <= t <= begin + T.SPAN_S + 2 * T.LEAD_S for t, _, _ in p["hilo"])
    assert "FES2022b" in p["source"]


def test_model_file_too_old_is_an_error():
    begin = int(T.begin_of(NOW))
    doc = _model_doc(begin - 20 * 86400, days=40)                        # ends before the window does
    get = FakeGet({BASE + "/points.json": POINTS, BASE + "/p/51201.json": doc})
    svc = G.GlobalTideService({}, get=get, now=Clock(), model_base=BASE)
    svc.refresh_models()
    assert svc.forecast("model:51201")[0] == "error"


def test_route_lists_model_points(monkeypatch):
    import app as A
    monkeypatch.setattr(A, "_TIDES", {"svc": None, "list": None, "global": None, "base": None})
    monkeypatch.setattr(A, "_points_root", lambda: "https://bucket.example")
    begin = int(T.begin_of(1795700000.0))
    files = {BASE + "/points.json": POINTS, BASE + "/p/51201.json": _model_doc(begin - 2 * 86400)}
    monkeypatch.setattr(A, "_tide_agency_get", FakeGet(files))
    c = A.app.test_client()
    rows = c.get("/api/tides/stations").get_json()["stations"]
    assert any(r[0] == "model:51201" and r[4] == "M" for r in rows)


def test_g76_add_file_one_at_a_time_equals_extract(tmp_path):
    land = (slice(0, 31), slice(20, 31))
    _grid_file(tmp_path / "m2_fes2022.nc", 50.0, 30.0, land)
    _grid_file(tmp_path / "s2_fes2022.nc", 20.0, 60.0, land)
    pts = [{"id": "model:a", "lat": 20.5, "lon": -159.8}, {"id": "model:b", "lat": 20.5, "lon": -159.05}]
    part = F.new_partial(pts)
    for f in ("s2_fes2022.nc", "m2_fes2022.nc", "m2_fes2022.nc"):          # any order; a file twice counts once
        F.add_file(pts, part, str(tmp_path / f), log=lambda *a: None)
    assert part["done"] == ["s2", "m2"]
    norm = lambda d: {k: dict(v, c=sorted(v["c"])) for k, v in d.items()}
    assert norm(F.finished(part)) == norm(F.extract(pts, str(tmp_path), log=lambda *a: None))
    with pytest.raises(SystemExit):
        F.finished(F.new_partial(pts))                                 # no M2: refused


def _mask_file(path, land, n=31, extrapolated=None):
    """AVISO+'s mask_fes2022B.nc on the test grid: 0 ocean, 1 extrapolated, 2 land."""
    import h5py
    m = np.zeros((n, n), dtype=np.float32)
    m[land] = 2.0
    if extrapolated is not None:
        m[extrapolated] = 1.0
    with h5py.File(path, "w") as h:
        h["lat"] = 20.0 + np.arange(n) / 30.0
        h["lon"] = 200.0 + np.arange(n) / 30.0
        h["mask"] = m


def test_run45958228397_extrapolated_grids_need_the_mask(tmp_path):
    # the extrapolated grids have a value in every cell, land included: read alone, no point ever sees land and the
    # job keeps nothing (run 45958228397 passed with ~no constants); with the mask the coastal point is kept
    land = (slice(0, 31), slice(20, 31))
    _grid_file(tmp_path / "m2_fes2022.nc", 50.0, 30.0)                 # no NaN anywhere = extrapolated
    _grid_file(tmp_path / "s2_fes2022.nc", 20.0, 60.0)
    _mask_file(tmp_path / "mask.nc", land, extrapolated=(slice(0, 31), slice(18, 20)))
    pts = [{"id": "model:a", "lat": 20.5, "lon": -159.8}, {"id": "model:b", "lat": 20.5, "lon": -159.05}]
    assert F.extract(pts, str(tmp_path), log=lambda *a: None) == {}
    mask = F.read_mask(str(tmp_path / "mask.nc"))
    got = F.extract(pts, str(tmp_path), log=lambda *a: None, mask=mask)
    assert set(got) == {"model:a"} and {c[0] for c in got["model:a"]["c"]} == {"m2", "s2"}
    lat, lon, amp, pha = F.read_grid(str(tmp_path / "m2_fes2022.nc"), mask)
    assert np.isnan(amp[:, 18:]).all() and np.isfinite(amp[:, :18]).all()   # extrapolated + land: no value


def test_mask_on_another_grid_is_refused(tmp_path):
    _grid_file(tmp_path / "m2_fes2022.nc", 50.0, 30.0)
    _mask_file(tmp_path / "mask.nc", (slice(0, 1), slice(0, 1)), n=30)
    with pytest.raises(SystemExit):
        F.read_grid(str(tmp_path / "m2_fes2022.nc"), F.read_mask(str(tmp_path / "mask.nc")))


def test_almost_no_points_kept_stops_the_job():
    with pytest.raises(SystemExit):
        F.check_kept(3, 3958)
    with pytest.raises(SystemExit):
        F.check_kept(197, 3958)                       # 5% of 3958 = 197.9
    F.check_kept(198, 3958)
    F.check_kept(452, 3958)                           # run 46037126451
    F.check_kept(0, 0)


def test_making_constants_without_the_mask_is_refused(tmp_path, monkeypatch):
    _grid_file(tmp_path / "m2_fes2022.nc", 50.0, 30.0)
    monkeypatch.setattr(sys, "argv", ["fes_points.py", "--add-file", str(tmp_path / "m2_fes2022.nc"),
                                      "--partial", str(tmp_path / "p.json")])
    with pytest.raises(SystemExit) as e:
        F.main()
    assert "--mask" in str(e.value) and not (tmp_path / "p.json").exists()


def test_g76_a_listed_model_point_opens(monkeypatch):
    """G76 P0-1: the window of a listed model point answered 404 (the route looked in the stations only)."""
    import app as A
    monkeypatch.setattr(A, "_TIDES", {"svc": None, "list": None, "global": None, "base": None})
    monkeypatch.setattr(A, "_points_root", lambda: "https://bucket.example")
    import time as _t
    begin = int(T.begin_of(_t.time()))
    files = {BASE + "/points.json": POINTS, BASE + "/p/51201.json": _model_doc(begin - 2 * 86400)}
    monkeypatch.setattr(A, "_tide_agency_get", FakeGet(files))
    c = A.app.test_client()
    r = c.get("/api/tides/model:51201")                               # before any list request: the list is read
    assert r.status_code == 200 and r.get_json()["method"] == "model"
    assert c.get("/api/tides/model:99999").status_code == 404
