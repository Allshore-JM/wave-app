"""tools/rrfs_verify_stations.py (plan section 99, the gate at the site's wind stations): the networks' parsers and
units, the station filter, GFS's bilinear sampling, RRFS / GFS forecasts at stations from fakes, and the section end to
end through rrfs_verify.station_gate."""
import json
import os
import sys
from datetime import datetime, timedelta
from types import SimpleNamespace

import numpy as np
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
sys.path.insert(0, ROOT)
import rrfs_verify as V  # noqa: E402
import rrfs_verify_stations as S  # noqa: E402

T = datetime(2026, 11, 18, 1)


def test_parse_nws_units_and_gaps():
    doc = {"features": [
        {"properties": {"timestamp": "2026-11-18T01:00:00+00:00", "windSpeed": {"value": 36.0},
                        "windDirection": {"value": 70}, "windGust": {"value": 54.0}}},
        {"properties": {"timestamp": "2026-11-18T00:50:00+00:00", "windSpeed": {"value": None}}},
        {"properties": {"timestamp": "2026-11-18T00:40:00Z", "windSpeed": {"value": 18.0}, "windDirection": {"value": None},
                        "windGust": {"value": None}}}]}
    obs = S.parse_nws(doc)
    assert obs[T] == pytest.approx((10.0, 70, 15.0)) and datetime(2026, 11, 18, 0, 50) not in obs
    assert obs[datetime(2026, 11, 18, 0, 40)] == pytest.approx((5.0, None, None))


def test_parse_metar_knots_and_variable_wind():
    rows = [{"icaoId": "PHNL", "obsTime": int(datetime(2026, 11, 18, 0, 53).replace(tzinfo=__import__("datetime").timezone.utc).timestamp()),
             "wspd": 19, "wdir": 70, "wgst": 29},
            {"icaoId": "PHNL", "obsTime": 1794958380, "wspd": 4, "wdir": "VRB", "wgst": None},
            {"icaoId": "PHOG", "obsTime": 1794958380, "wspd": None}]
    obs = S.parse_metar(rows)
    assert set(obs) == {"PHNL"}
    hit = obs["PHNL"][datetime(2026, 11, 18, 0, 53)]
    assert hit[0] == pytest.approx(19 * S.KT) and hit[1] == 70 and hit[2] == pytest.approx(29 * S.KT)
    assert any(v[1] is None for v in obs["PHNL"].values())                  # VRB: no direction


def test_parse_coops():
    obs = S.parse_coops({"data": [{"t": "2026-11-18 01:00", "s": "5.0", "d": "109.0", "g": "7.7"},
                                  {"t": "2026-11-18 01:06", "s": "", "d": "1", "g": "1"}, {"t": "bad", "s": "1"}]})
    assert obs == {T: (5.0, 109.0, 7.7)}
    assert S.parse_coops({"error": {"message": "No data"}}) == {}


def test_load_stations_keeps_the_four_networks_inside_the_grids(tmp_path):
    p = tmp_path / "ws.json"
    p.write_text(json.dumps({"stations": [
        ["nws:001HE", "x", 20.8, -156.5, "utility", "nws"], ["metar:PHNL", "x", 21.3, -157.9, "airport", "metar"],
        ["iem:013NH", "x", 42.9, -70.8, "road", "iem"], ["metar:EGLL", "x", 51.5, -0.45, "airport", "metar"],
        ["coops:1612340", "x", 21.3, -157.86, "gauge", "coops"], ["ndbc:51003", "x", 19.15, -160.6, "buoy", "ndbc"]]}))
    import rrfs_grid as G
    st = S.load_stations(str(p), G.locate)
    assert [s[0] for s in st] == ["nws:001HE", "metar:PHNL", "coops:1612340", "ndbc:51003"]
    assert st[0][4] == "001HE" and st[3][3] == "ndbc"


def test_is_buoy_with_network_prefix():
    assert V.is_buoy("ndbc:51003") and V.is_buoy("51201")
    assert not V.is_buoy("metar:PHNL") and not V.is_buoy("coops:16123") and not V.is_buoy("ndbc:KWHH1")


def test_bilinear():
    g = np.zeros((721, 1440))
    g[276, 808] = 4.0                          # 21.0 N, 202.0 E
    assert S.bilinear(g, 21.0, -158.0) == pytest.approx(4.0)
    assert S.bilinear(g, 21.125, -158.0) == pytest.approx(2.0)              # half a cell north
    g2 = np.zeros((721, 1440))
    g2[:, 1439] = 2.0
    assert S.bilinear(g2, 0.0, 359.875) == pytest.approx(1.0)               # across the 0/360 seam


class FakeReader:
    def __init__(self):
        self.srcs = [{"cycle": "2026111800", "kind": "run", "cells": {"hi": "x", "conus": "y"}},
                     {"cycle": "2026111723", "kind": "cycle", "cells": {"hi": "x"}}]

    def sources(self, need_cells=False):
        return self.srcs

    def _cell_series(self, src, grid, j, i):
        import rrfs_wind
        steps = [0, 1, 2] if src["kind"] == "run" else [1, 2]
        arr = np.zeros(len(steps), dtype=np.dtype(rrfs_wind.CELL_FIELDS))
        arr["spd"] = 600 if src["kind"] == "run" else 400
        arr["dir"], arr["gust"] = 900, 800
        return steps, arr


def test_rrfs_at_stations_reads_every_cell_source():
    import rrfs_grid as G
    st = [("metar:PHNL", 21.33, -157.94, "metar", "PHNL"), ("x:FAR", 51.5, -0.45, "metar", "EGLL")]
    fc = S.rrfs_at_stations(FakeReader(), st, G.locate, log=lambda s: None)
    assert len(fc) == 5 and {f[3] for f in fc} == {"2026111800", "2026111723"}
    assert ("metar:PHNL", datetime(2026, 11, 18, 1), 1, "2026111800", 6.0, 90.0, 8.0) in fc


def test_gfs_at_stations_reads_steps_in_the_window():
    asked = []
    fm = SimpleNamespace(STEPS=list(range(0, 121)) + list(range(123, 385, 3)),
                         atmos_url=lambda run_dt, h: f"{run_dt:%Y%m%d%H}/{h}",
                         fetch_records=lambda url, keys: asked.append(url) or {keys[0]: b"u", keys[1]: b"v"})
    u = np.full((721, 1440), 3.0)
    v = np.full((721, 1440), -4.0)
    st = [("metar:PHNL", 21.33, -157.94, "metar", "PHNL")]
    fc = S.gfs_at_stations(st, T - timedelta(hours=2), T, fm, lambda b: u if b == b"u" else v, log=lambda s: None,
                           max_lead=84)
    assert all(T - timedelta(hours=2) <= f[1] <= T for f in fc)
    assert {f[2] for f in fc if f[3] == "2026111800"} == {0, 1}              # the 00Z run's steps in the window
    assert fc[0][4] == pytest.approx(5.0) and fc[0][5] == pytest.approx((np.degrees(np.arctan2(-3, 4)) + 360) % 360)
    assert len({f[3] for f in fc}) == 15                                     # runs from 84 h before the window


def test_station_gate_end_to_end(tmp_path, monkeypatch):
    ws = tmp_path / "ws.json"
    ws.write_text(json.dumps({"stations": [["metar:PHNL", "Honolulu", 21.33, -157.94, "airport", "metar"]]}))
    obs_rows = [{"icaoId": "PHNL", "obsTime": int((datetime(2026, 11, 18, h).timestamp() - 0)), "wspd": 10, "wdir": 90,
                 "wgst": 15} for h in (0, 1, 2)]

    def get(url):
        if url.startswith(S.METAR_API):
            # datetime(...).timestamp() above is local time on this machine: rebuild in UTC
            from datetime import timezone
            rows = [dict(r, obsTime=int(datetime(2026, 11, 18, k, tzinfo=timezone.utc).timestamp())) for k, r in enumerate(obs_rows)]
            return json.dumps(rows).encode()
        return None

    fm = SimpleNamespace(STEPS=list(range(0, 121)), atmos_url=lambda run_dt, h: "x",
                         fetch_records=lambda url, keys: {keys[0]: b"u", keys[1]: b"v"})
    u, v = np.full((721, 1440), 0.0), np.full((721, 1440), -5.0)
    monkeypatch.setattr(V.time, "sleep", lambda s: None)
    monkeypatch.setattr(S.time, "sleep", lambda s: None)
    a = SimpleNamespace(base="https://m", out=str(tmp_path), no_height=False)
    sec = V.station_gate(a, datetime(2026, 11, 18, 2), get, lambda s: None, stations_path=str(ws), reader=FakeReader(),
                         fetch_mod=fm, decode=lambda b: u if b == b"u" else v)
    assert sec["with_obs"] == 1 and sec["networks"] == "metar 1"
    hi = sec["result"]["hi|all"]
    assert hi["rrfs"]["n"] >= 2 and hi["gfs"]["mae"] == pytest.approx(abs(5.0 - 10 * S.KT), abs=0.01)
    assert sec["gate"]["hi"][0] == "not enough"
