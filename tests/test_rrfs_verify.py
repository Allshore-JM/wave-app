"""tools/rrfs_verify.py (plan section 99, the gate): NDBC parsing, the 10-m scaling, pairing by valid hour and lead
band, the matched comparison and the verdicts, and both products' decoding from fake buckets."""
import json
import os
import struct
import sys
from datetime import datetime, timedelta

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import rrfs_verify as V  # noqa: E402

NDBC = """#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE
#yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft
2026 11 18 00 50 170  4.0  5.0   2.8    MM   9.4 303 1018.2    MM  18.4    MM   MM   MM    MM
2026 11 18 00 40  MM  3.0   MM    MM    MM    MM  MM 1018.3    MM  18.4    MM   MM   MM    MM
2026 11 18 00 30 170   MM  6.0    MM    MM    MM  MM 1018.3    MM  18.4    MM   MM   MM    MM
"""


def test_parse_ndbc():
    obs = V.parse_ndbc(NDBC)
    assert obs[datetime(2026, 11, 18, 0, 50)] == (4.0, 170.0, 5.0)
    assert obs[datetime(2026, 11, 18, 0, 40)] == (3.0, None, None)
    assert datetime(2026, 11, 18, 0, 30) not in obs                           # no speed: not an observation
    assert V.parse_ndbc("") == {} and V.parse_ndbc("#YY MM DD hh mm WVHT\n2026 11 18 00 50 1.0") == {}


def test_nearest_obs_within_ten_minutes():
    s = {datetime(2026, 11, 18, 0, 50): (4.0, 170.0, 5.0), datetime(2026, 11, 18, 2, 10): (9.0, 10.0, 10.0)}
    assert V.nearest_obs(s, datetime(2026, 11, 18, 1)) == (4.0, 170.0, 5.0)
    assert V.nearest_obs(s, datetime(2026, 11, 18, 2)) == (9.0, 10.0, 10.0)
    assert V.nearest_obs(s, datetime(2026, 11, 18, 3)) is None


def test_lead_bands_and_buoys():
    assert [V.band_of(h) for h in (0, 6, 7, 18, 19, 48, 49, 84, 85)] == [
        "0-6 h", "0-6 h", "7-18 h", "7-18 h", "19-48 h", "19-48 h", "49-84 h", "49-84 h", None]
    assert V.is_buoy("51201") and not V.is_buoy("KWHH1") and not V.is_buoy("MOKH1")
    assert V.BUOY_TO_10M == pytest.approx(1.103, abs=0.001)


T = datetime(2026, 11, 18, 1)


def test_pairing_scales_buoys_and_skips_slack_directions():
    obs = {"51201": {T: (4.0, 90.0, 6.0)}, "KWHH1": {T: (4.0, 90.0, 6.0)}, "11111": {T: (2.0, 90.0, None)}}
    fc = [("51201", T, 3, "c", 5.0, 100.0, 8.0), ("KWHH1", T, 3, "c", 5.0, 100.0, 8.0),
          ("11111", T, 3, "c", 5.0, 200.0, 8.0), ("51201", T, 90, "c", 5.0, 100.0, 8.0),
          ("NOOBS", T, 3, "c", 5.0, 100.0, 8.0)]
    p = V.pair(fc, obs)
    (se, de, ge), = p[("51201", T, "0-6 h")]
    assert se == pytest.approx(5.0 - 4.0 * V.BUOY_TO_10M) and de == 10.0 and ge == pytest.approx(8.0 - 6.0 * V.BUOY_TO_10M)
    assert p[("KWHH1", T, "0-6 h")] == [(1.0, 10.0, 2.0)]                       # C-MAN: as measured
    assert p[("11111", T, "0-6 h")][0][1] is None                               # obs < 3 m/s: no direction error
    assert len(p) == 3                                                          # lead 90 and no-obs left out
    raw = V.pair(fc, obs, height=False)
    assert raw[("51201", T, "0-6 h")][0][0] == pytest.approx(1.0)


def test_compare_uses_matched_station_hours_and_averages_sources():
    t2 = T + timedelta(hours=1)
    rr = {("51201", T, "0-6 h"): [(1.0, 10.0, 0.5), (3.0, 30.0, 1.5)],         # two sources: averaged to 2.0
          ("51201", t2, "0-6 h"): [(1.0, None, None)]}                          # GFS has no t2: not compared
    gf = {("51201", T, "0-6 h"): [(-4.0, 40.0, None)], ("46026", T, "0-6 h"): [(1.0, 1.0, None)]}
    res = V.compare(rr, gf, {"51201": "hi", "46026": "conus"})
    hi = res["hi|0-6 h"]
    assert hi["rrfs"] == {"n": 1, "mae": 2.0, "bias": 2.0, "rmse": 2.0, "dir_mae": 20.0, "dir_n": 1, "gust_mae": 1.0,
                          "gust_bias": 1.0, "gust_n": 1}
    assert hi["gfs"]["mae"] == 4.0 and hi["gfs"]["bias"] == -4.0
    assert "conus|all" not in res and res["all|all"]["rrfs"]["n"] == 1


def test_gate_verdicts():
    res = {"hi|all": {"rrfs": {"n": 300, "mae": 1.2}, "gfs": {"n": 300, "mae": 1.5}},
           "ak|all": {"rrfs": {"n": 250, "mae": 2.0}, "gfs": {"n": 250, "mae": 1.6}},
           "pr|all": {"rrfs": {"n": 20, "mae": 1.0}, "gfs": {"n": 20, "mae": 2.0}},
           "all|all": {"rrfs": {"n": 570, "mae": 1.5}, "gfs": {"n": 570, "mae": 1.6}},
           "hi|0-6 h": {"rrfs": {"n": 10, "mae": 9.0}, "gfs": {"n": 10, "mae": 1.0}}}
    g = V.gate(res)
    assert g["hi"][0] == "pass" and g["ak"][0] == "owner's call" and g["pr"][0] == "not enough"
    assert set(g) == {"hi", "ak", "pr"}


def _fake_bucket():
    """One RRFS source (12Z run, 3 steps, two stations) and one GFS winds run (12Z of the 17th, 385 steps)."""
    objs = {}
    steps = [0, 1, 2]
    st = {"51201": [0, "hi", 1, 1], "46026": [1, "conus", 2, 2]}
    body = b""
    for slot in range(2):
        vals = []
        for h in steps:
            vals += [500 + h, 900, 700] if not (slot == 1 and h == 1) else [V.MISSING, V.MISSING, V.MISSING]
        body += struct.pack(f"<{3 * len(steps)}H", *vals)
    objs["rrfs/v1/latest.json"] = json.dumps({"sources": [{"cycle": "2026111712", "kind": "run",
                                                           "stations": "rrfs/v1/2026111712/stations.json"}]})
    objs["rrfs/v1/2026111712/stations.json"] = json.dumps({"steps": steps, "stations": st, "count": 2,
                                                            "record_bytes": 6 * len(steps)})
    objs["rrfs/v1/2026111712/stations.bin"] = body
    gsteps = list(range(385))
    g = b""
    for slot in range(1):
        g += struct.pack(f"<{2 * 385}H", *sum(([400 + (h % 50), 1800] for h in gsteps), []))
    objs["gfswave/winds/v1/2026111712/index.json"] = json.dumps({"steps": gsteps, "stations": {"51201": 0},
                                                                  "count": 1, "record_bytes": 1540})
    objs["gfswave/winds/v1/2026111712/winds.bin"] = g

    def get(url):
        key = url.split("models.example/", 1)[-1]
        v = objs.get(key)
        return v.encode() if isinstance(v, str) else v
    return get


def test_decoding_both_products():
    get = _fake_bucket()
    grids, rr = V.rrfs_forecasts(get, "https://models.example")
    assert grids == {"51201": "hi", "46026": "conus"}
    assert ("51201", datetime(2026, 11, 17, 13), 1, "2026111712", 5.01, 90.0, 7.0) in rr
    assert len(rr) == 5                                                         # 46026's missing step left out
    gf = V.gfs_forecasts(get, {"51201", "46026"}, datetime(2026, 11, 18, 1), "https://models.example", back=4)
    assert len(gf) == 85 and gf[0] == ("51201", datetime(2026, 11, 17, 12), 0, "2026111712", 4.0, 180.0, None)
    assert max(f[2] for f in gf) == 84


def test_main_writes_the_report(tmp_path, monkeypatch):
    get = _fake_bucket()

    def get_all(url):
        if url.startswith(V.NDBC):
            sid = url.rsplit("/", 1)[1][:-4]
            if sid != "51201":
                return None
            lines = [NDBC.splitlines()[0], NDBC.splitlines()[1]]
            for h in range(12, 20):
                lines.append(f"2026 11 17 {h:02d} 00 100  4.0  6.0 MM MM MM MM MM MM MM MM MM MM MM")
            return "\n".join(lines).encode()
        return get(url)

    monkeypatch.setattr(V.time, "sleep", lambda s: None)
    rc = V.main(["--out", str(tmp_path), "--base", "https://models.example"], get=get_all,
                now=datetime(2026, 11, 18, 1), log=lambda s: None)
    assert rc == 0
    rep = json.loads((tmp_path / "report.json").read_text())
    assert rep["meta"]["rrfs_sources"] == 1 and rep["meta"]["obs_stations"] == 1
    assert "hi" in rep["gate"] and rep["gate"]["hi"][0] == "not enough"
    assert "| hi | all |" in (tmp_path / "report.md").read_text()
