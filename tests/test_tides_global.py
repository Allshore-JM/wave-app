"""Plan section 112: the worldwide tide stations (tide_global.py): TICON gauges predicted locally, the agencies' official
predictions through tide_providers/, dedup, caching, failures, and the routes in app.py."""
import json
import math
import os
import sys
import threading
import types

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import tide_global as G  # noqa: E402
import tide_sources as T  # noqa: E402
from tide_providers import TideFetchError  # noqa: E402

NOW = 1795700000.0                       # 2026-11-26T12:53:20Z
BREST = "ticon:brest-822-fra-uhslc-fd"


class Clock:
    def __init__(self, t=NOW):
        self.t = t

    def __call__(self):
        return self.t


@pytest.fixture(scope="module")
def stations():
    s, _ = G.load()
    return s


def _fake_module(name, fetch, max_requests=2):
    m = types.ModuleType("tide_providers." + name)
    m.PREFIX, m.NAME, m.CREDIT, m.DATUM, m.MAX_REQUESTS = "qld", "Fake agency", "Data: Fake (CC BY 4.0)", "LAT", max_requests
    m.page_url = lambda st: "https://example.org/" + st["id"]
    m.fetch = fetch
    return m


@pytest.fixture
def fake_agency(monkeypatch):
    calls = []
    state = {"mode": "curve"}

    def fetch(st, begin, end, get):
        calls.append((st["id"], begin, end))
        if state["mode"] == "fail":
            raise TideFetchError("down")
        step = 600 if state["mode"] == "curve" else 3600
        ts = np.arange(begin, end + 1, step)
        curve = [(int(t), 1.5 + math.cos(2 * math.pi * (t - begin) / 44712.0)) for t in ts]
        hilo = [(int(begin + k * 22356), 2.5 if k % 2 == 0 else 0.5, "H" if k % 2 == 0 else "L")
                for k in range(int((end - begin) // 22356) + 1)]
        if state["mode"] == "hilo":
            curve = []
        return {"curve": curve if state["mode"] != "hourly" else curve, "hilo": hilo}
    mod = _fake_module("fakeag", fetch)
    monkeypatch.setitem(sys.modules, "tide_providers.fakeag", mod)
    monkeypatch.setitem(G.PROVIDERS, "qld", "fakeag")
    return mod, calls, state


def _official_station(sid="qld:gold-coast", lat=-27.94, lon=153.43):
    return {"id": sid, "name": "Gold Coast", "lat": lat, "lon": lon, "tz": "Australia/Brisbane", "type": "O",
            "obs": False, "local": {}}


# ------------------------------------------------------------------ TICON gauges

def test_load_counts_and_types(stations):
    gauges = [s for s in stations.values() if s["type"] == "G"]
    assert len(gauges) > 700 and all(G.valid_id(s["id"]) for s in gauges)     # 797 after the official stations (3 km)
    assert all(s["lat0"] > 0 for s in gauges)                          # LAT lies below the mean level
    assert stations[BREST]["tz"] == "Europe/Paris"
    official = [s for s in stations.values() if s["type"] == "O"]
    assert len(official) > 1500 and {s["id"].split(":")[0] for s in official} == set(G.PROVIDERS)


def test_ticon_payload_shape(stations):
    status, p = G.GlobalTideService(stations, now=Clock()).forecast(BREST)
    assert status == "ok" and p["datum"] == "LAT (computed)" and p["method"] == "ticon" and p["type"] == "G"
    begin = T.begin_of(NOW)
    assert p["begin"] == begin - T.LEAD_S and p["window"] == [begin, begin + T.SPAN_S] and p["step"] == T.STEP_S
    assert len(p["v"]) == T.POINTS and all(v is not None for v in p["v"])
    assert min(p["v"]) >= -0.05                                       # above LAT (a hair below where the hour misses)
    kinds = [k for _, _, k in p["hilo"]]
    assert all(a != b for a, b in zip(kinds, kinds[1:]))              # alternating
    assert len(p["hilo"]) > 100 and all(t % 60 == 0 for t, _, _ in p["hilo"])
    highs = [h for _, h, k in p["hilo"] if k == "H"]
    assert 5.5 < max(highs) < 8.5                                     # Brest's spring highs above LAT


def test_ticon_curve_agrees_with_its_extremes(stations):
    _, p = G.GlobalTideService(stations, now=Clock()).forecast(BREST)
    for t, h, k in p["hilo"][2:10]:
        i = round((t - p["begin"]) / p["step"])
        assert abs(p["v"][i] - h) < 0.25                                # the 30-min curve passes near each extreme


def test_ticon_cached_per_utc_day(stations):
    clock = Clock()
    svc = G.GlobalTideService(stations, now=clock)
    a = svc.forecast(BREST)[1]
    assert svc.forecast(BREST)[1] is a
    clock.t += 86400
    assert svc.forecast(BREST)[1] is not a


def test_unknown_and_bad_ids(stations):
    svc = G.GlobalTideService(stations, now=Clock())
    assert svc.forecast("ticon:nope")[0] == "final"
    assert svc.forecast("TICON:Brest")[0] == "final"
    assert not G.valid_id("ticon:") and not G.valid_id("ticon:a b") and not G.valid_id("xyz:abc")


# ------------------------------------------------------------------ official agencies

def test_official_curve_payload(stations, fake_agency):
    mod, calls, state = fake_agency
    st = _official_station()
    svc = G.GlobalTideService(dict(stations, **{st["id"]: st}), get=lambda *a, **k: b"", now=Clock())
    status, p = svc.forecast(st["id"])
    assert status == "ok" and p["method"] == "official" and p["datum"] == "LAT" and p["agency"] == "Fake agency"
    assert p["source"] == mod.CREDIT and p["link"] == "https://example.org/qld:gold-coast"
    begin = T.begin_of(NOW)
    assert calls == [(st["id"], begin - 2 * T.LEAD_S, begin + T.SPAN_S + 2 * T.LEAD_S)]
    assert all(v is not None for v in p["v"])
    t0 = p["begin"]
    assert abs(p["v"][0] - (1.5 + math.cos(2 * math.pi * (t0 - (begin - 2 * T.LEAD_S)) / 44712.0))) < 0.002
    svc.forecast(st["id"])
    assert len(calls) == 1                                            # once per UTC day


def test_official_hilo_only_uses_the_cosine(stations, fake_agency):
    mod, calls, state = fake_agency
    state["mode"] = "hilo"
    st = _official_station()
    p = G.GlobalTideService({st["id"]: st}, get=lambda *a, **k: b"", now=Clock()).forecast(st["id"])[1]
    assert p["method"] == "official-cosine" and any(v is not None for v in p["v"])


def test_official_failure_remembered_a_minute(stations, fake_agency):
    mod, calls, state = fake_agency
    state["mode"] = "fail"
    st = _official_station()
    clock = Clock()
    svc = G.GlobalTideService({st["id"]: st}, get=lambda *a, **k: b"", now=clock)
    assert svc.forecast(st["id"])[0] == "error"
    assert svc.forecast(st["id"])[0] == "error" and len(calls) == 1
    clock.t += G.ERROR_TTL_S + 1
    state["mode"] = "curve"
    assert svc.forecast(st["id"])[0] == "ok" and len(calls) == 2


def test_official_budget_and_no_get(stations, fake_agency):
    st = _official_station()

    class Empty:
        def take(self, n=1):
            return False
    assert G.GlobalTideService({st["id"]: st}, get=lambda *a, **k: b"", now=Clock(),
                               buckets={"qld": Empty()}).forecast(st["id"])[0] == "busy"
    assert G.GlobalTideService({st["id"]: st}, get=None, now=Clock()).forecast(st["id"])[0] == "error"


def test_second_visitor_waits_for_the_first(stations, fake_agency, monkeypatch):
    mod, calls, state = fake_agency
    st = _official_station()
    gate = threading.Event()
    real = mod.fetch

    def slow(*a):
        gate.wait(5)
        return real(*a)
    monkeypatch.setattr(mod, "fetch", slow)
    svc = G.GlobalTideService({st["id"]: st}, get=lambda *a, **k: b"", now=Clock())
    out = {}
    th = threading.Thread(target=lambda: out.setdefault("a", svc.forecast(st["id"])))
    th.start()
    for _ in range(100):
        if svc._building:
            break
        threading.Event().wait(0.01)
    tb = threading.Thread(target=lambda: out.setdefault("b", svc.forecast(st["id"])))
    tb.start()
    threading.Event().wait(0.05)
    gate.set()
    th.join(5)
    tb.join(5)
    assert out["a"][0] == "ok" and out["b"][0] in ("ok", "busy")
    assert len(calls) == 1


def test_curve_grid_linear_cubic_and_gaps():
    start = 0
    lin = G.curve_grid([(t, t / 1000.0) for t in range(0, 7200, 600)], start, n=5)
    assert lin[:4] == [0.0, 1.8, 3.6, 5.4] and lin[4] is None          # beyond the series: None
    hourly = [(t, math.sin(2 * math.pi * t / 44712.0)) for t in range(0, 20 * 3600, 3600)]
    grid = G.curve_grid(hourly, 0, n=30)
    for i, v in enumerate(grid[2:28], start=2):
        assert abs(v - math.sin(2 * math.pi * i * 1800 / 44712.0)) < 0.01   # the cubic follows the tops
    gap = [(0, 1.0), (3600, 1.0), (5 * 3600, 1.0)]
    g = G.curve_grid(gap, 0, n=10)
    assert g[2] == 1.0 and g[3] is None


def test_ticon_dropped_next_to_an_official_station(tmp_path, stations):
    brest = stations[BREST]
    nat = {"fields": ["id", "name", "lat", "lon", "tz", "local"],
           "stations": [["qld:x", "X", brest["lat"] + 0.005, brest["lon"], "Europe/Paris", {}]]}
    p = tmp_path / "nat.json"
    p.write_text(json.dumps(nat), encoding="utf-8")
    s, _ = G.load(national_path=str(p))
    assert BREST not in s and s["qld:x"]["type"] == "O"
    nat["stations"][0][2] = brest["lat"] + 0.05                       # 5.5 km away: both stay
    p.write_text(json.dumps(nat), encoding="utf-8")
    s, _ = G.load(national_path=str(p))
    assert BREST in s


def test_national_file_unknown_agency_ignored(tmp_path):
    nat = {"fields": ["id", "name", "lat", "lon", "tz", "local"],
           "stations": [["zzz:x", "X", 1.0, 2.0, "UTC", {}], ["qld:y", "Y", 1.0, 2.0, "UTC", {}]]}
    p = tmp_path / "nat.json"
    p.write_text(json.dumps(nat), encoding="utf-8")
    assert set(G.load_national(str(p))) == {"qld:y"}


# ------------------------------------------------------------------ routes

@pytest.fixture
def client(monkeypatch):
    import app as A
    monkeypatch.setattr(A, "_TIDES", {"svc": None, "list": None, "global": None, "base": None})
    return A.app.test_client()


def test_routes_list_and_station(client):
    r = client.get("/api/tides/stations")
    d = r.get_json()
    types_ = {row[4] for row in d["stations"]}
    assert r.status_code == 200 and {"R", "S", "G"} <= types_ and len(d["sources"]) == 2
    r = client.get("/api/tides/" + BREST)
    assert r.status_code == 200 and r.get_json()["datum"] == "LAT (computed)"
    assert client.get("/api/tides/ticon:nope").status_code == 404
    assert client.get("/api/tides/ticon:Brest").status_code == 404
    assert client.get("/api/tides/" + BREST + "/observed").get_json().get("t") == []
