"""RRFS wind in the site's forecast tables (plan section 99, step 6), with RRFS_WIND on and a fake RRFS reader: the
row contract (wind 20-21, gust 22, source 23, combined last), the GFS merge at stations and points, the table (Gust
column, source line, handover borders, units), the summary's gusts, the cache keys and /healthz. With the flag off
every table is byte for byte as before (the golden tests of the rest of the suite run with it off)."""
import os
import re
from datetime import datetime, timedelta

import pytest

import app as A
import point_forecast as PF
import rrfs_wind

FIX = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
BULL = open(os.path.join(FIX, "gfswave_51201_2026100112.bull"), encoding="utf-8").read()
RUN = datetime(2026, 10, 1, 12)


class FakeRrfs:
    """station / point answer RRFS 13Z for +1..+18 h after 13Z and RRFS 12Z to +30 h of 12Z (the reader's rule)."""

    def __init__(self, base=RUN, fail=False):
        self.base, self.fail, self.asked = base, fail, []

    def _map(self):
        out = {}
        for h in range(0, 31):
            out[self.base + timedelta(hours=h)] = (10.0, 90, 14.0, "RRFS 12Z")
        for h in range(1, 19):
            out[self.base + timedelta(hours=1 + h)] = (5.0, 45, 7.5, "RRFS 13Z")
        return out

    def station(self, sid):
        self.asked.append(("station", sid))
        if self.fail:
            raise IOError("bucket down")
        return self._map()

    def point(self, lat, lon):
        self.asked.append(("point", lat, lon))
        return self._map()

    def newest(self):
        return "2026100113"

    def health(self):
        return {"pointer_ok": 1}


@pytest.fixture
def on(monkeypatch):
    monkeypatch.setenv("RRFS_WIND", "1")
    fake = FakeRrfs()
    monkeypatch.setattr(A, "RRFS", fake)
    return fake


def _gfs_map():
    return {RUN + timedelta(hours=h): (3.0, 180) for h in range(0, 385)}


def test_row_cells_off_two_on_four(monkeypatch):
    t = RUN + timedelta(hours=5)
    monkeypatch.delenv("RRFS_WIND", raising=False)
    assert A._wind_row_cells({t: (3.0, 180)}, t) == [3.0, 180]
    monkeypatch.setenv("RRFS_WIND", "1")
    assert A._wind_row_cells({t: (3.0, 180)}, t) == [3.0, 180, None, None]
    assert A._wind_row_cells({t: (5.0, 45, 7.5, "RRFS 13Z")}, t) == [5.0, 45, 7.5, "RRFS 13Z"]
    assert A._wind_row_cells({t: (5.0, 45, float("nan"), "RRFS 13Z")}, t) == [5.0, 45, None, "RRFS 13Z"]
    assert A._wind_row_cells({}, t) == [None, None, None, None]
    assert A._wind_row_cells({t: (float("inf"), 45, 7.5, "RRFS 13Z")}, t) == [None, None, None, None]


def test_station_merge_and_failure(on, monkeypatch):
    m = A._with_rrfs_station("51201", _gfs_map())
    assert m[RUN + timedelta(hours=2)] == (5.0, 45, 7.5, "RRFS 13Z")
    assert m[RUN + timedelta(hours=25)] == (10.0, 90, 14.0, "RRFS 12Z")
    assert m[RUN + timedelta(hours=40)] == (3.0, 180, None, None)
    monkeypatch.setattr(A, "RRFS", FakeRrfs(fail=True))
    g = _gfs_map()
    assert A._with_rrfs_station("51201", g) is g                      # RRFS unreadable: GFS as is, nothing raised
    monkeypatch.delenv("RRFS_WIND")
    assert A._with_rrfs_station("51201", g) is g


def _rows(tz="UTC"):
    wind = A._with_rrfs_station("51201", _gfs_map())
    c, loc, run, rows, tz_eff, err = A._parse_bull_text(BULL, "51201", tz, "20261001", "12", wind)
    assert err is None
    return c, loc, run, rows, tz_eff


def test_bulletin_rows_carry_gust_and_source_and_keep_combined_last(on):
    _, _, _, rows, _ = _rows()
    off_rows = None
    assert all(len(r) == 25 for r in rows)
    by_time = {(r[0], r[1]): r for r in rows}
    first = rows[0]
    assert first[23] in ("RRFS 12Z", "RRFS 13Z") and first[22] is not None
    late = [r for r in rows if r[23] is None]
    assert late and all(r[20] == 3.0 and r[22] is None for r in late)      # GFS after RRFS's +30 h here
    assert all(isinstance(r[-1], float) or r[-1] is None for r in rows)    # combined Hs is still the last cell
    del off_rows, by_time


def test_table_gust_column_source_line_and_handovers(on):
    c, loc, run, rows, tz = _rows()
    html = A.build_html_table(c, loc, run, rows, tz, "US", compact=True)
    assert html.count("Gust<br>(mph)") == 1 and 'colspan="3" scope="colgroup"' in html
    cap = re.search(r'<caption class="wind-source"[^>]*>(.*?)</caption>', html).group(1)
    assert cap.startswith("Wind: RRFS 12Z to ") and "RRFS 13Z to " in cap and cap.endswith("then GFS")
    assert " &middot; " in cap and all(ord(ch) < 128 for ch in cap)        # ASCII on the page, as every table
    assert html.count("border-top:2px solid #546E7A") == 3 * 3            # 12Z->13Z, 13Z->12Z, 12Z->GFS: 3 cells each
    # gust in mph: 14.0 m/s -> 31, 7.5 -> 17
    assert ">31</td>" in html and ">17</td>" in html
    metric = A.build_html_table(c, loc, run, rows, tz, "SI", compact=False)
    assert "Gust<br>(km/h)" in metric and ">50</td>" in metric           # 14.0 m/s = 50 km/h
    assert metric.count("<caption") == 1


def test_a_table_without_rrfs_hours_is_unchanged_with_the_flag_on(on, monkeypatch):
    monkeypatch.setattr(A, "RRFS", FakeRrfs(base=datetime(2020, 1, 1)))   # RRFS hours that the table never reaches
    c, loc, run, rows, tz = _rows()
    html_on = A.build_html_table(c, loc, run, rows, tz, "US", compact=True)
    monkeypatch.delenv("RRFS_WIND")
    c2, loc2, run2, rows2, tz2, _ = A._parse_bull_text(BULL, "51201", "UTC", "20261001", "12", _gfs_map())
    assert html_on == A.build_html_table(c2, loc2, run2, rows2, tz2, "US", compact=True)


def test_summary_shows_peak_gusts(on):
    c, loc, run, rows, tz = _rows("Pacific/Honolulu")
    times = [A._row_datetime(r) for r in rows]
    ann = A.sky.annotate_rows(times, 21.67, -158.12, "Pacific/Honolulu")
    dates = sorted({t.date() for t in times if t is not None})
    html = A.build_summary_html(rows, ann, A.sky.day_summary(21.67, -158.12, "Pacific/Honolulu", dates), "US")
    assert "gusts 31" in html                                               # 14.0 m/s, the day's peak, in mph


def test_point_rows_extra_wind():
    class Codes(list):
        pass
    PFM = PF.pointfmt()
    names = PFM.FIELD_NAMES
    steps = [0, 1, 2]
    codes = [[PFM.MISSING] * 3 for _ in names]
    ix = {n: i for i, n in enumerate(names)}
    codes[ix["wind"]] = [300, 300, PFM.MISSING]
    codes[ix["wdir"]] = [180, 180, PFM.MISSING]
    over = {RUN + timedelta(hours=1): (5.0, 45, 7.5, "RRFS 13Z")}
    plain = PF.point_rows(codes, steps, RUN, A.UTC)
    rows = PF.point_rows(codes, steps, RUN, A.UTC, wind_over=over, extra_wind=True)
    assert [len(r) for r in plain] == [23] * 3 and [len(r) for r in rows] == [25] * 3
    assert rows[0][20:24] == [plain[0][20], plain[0][21], None, None]
    assert rows[1][20:24] == [5.0, 45, 7.5, "RRFS 13Z"]
    assert rows[2][20:24] == [None, None, None, None]
    assert [r[-1] for r in rows] == [r[-1] for r in plain]


def test_cache_keys(on, monkeypatch):
    assert A._bull_key("51201", "UTC") == ("51201", "UTC", "GFS", "2026100113")
    monkeypatch.delenv("RRFS_WIND")
    assert A._bull_key("51201", "UTC") == ("51201", "UTC", "GFS")


def test_healthz_reports_rrfs(on, monkeypatch):
    client = A.app.test_client()
    j = client.get("/healthz").get_json()
    assert j["rrfs"] == {"pointer_ok": 1, "enabled": True}
    monkeypatch.delenv("RRFS_WIND")
    assert client.get("/healthz").get_json()["rrfs"] == {"enabled": False}


def test_merge_helper_is_the_readers():
    assert A.rrfs_wind.merge is rrfs_wind.merge


# ------------------------------------------------------------------- the land-weather RRFS tab (plan section 99, step 7)

import weather_sources as WS  # noqa: E402

LAND = "pt_21450N_158000W"                    # central Oahu (the land-weather tests' point)
NOW = datetime.utcnow().replace(minute=0, second=0, microsecond=0)


def _cells(base=None, hours=30, cycle_label="RRFS 18Z", kind="run", cloud=40):
    base = base or NOW - timedelta(hours=26)
    out = {}
    for h in range(hours + 26):
        t = base + timedelta(hours=h)
        out[t] = {"spd": 6.0, "dir": 70, "gust": 9.0, "tmp_k": 298.15, "dpt_k": 291.15, "apcp_mm": 0.5 if h % 2 else 0.0,
                  "cloud": cloud, "type": 1 if h % 2 else 0, "label": cycle_label, "kind": kind,
                  "cycle": (base).strftime("%Y%m%d%H")}
    return out


class FakeCells(FakeRrfs):
    def __init__(self, cells):
        super().__init__()
        self.cells = cells

    def point_cells(self, lat, lon):
        self.asked.append(("cells", lat, lon))
        return self.cells


def test_rrfs_rows():
    rows = WS.rrfs_rows(_cells(), NOW)
    assert rows[0]["t"] == NOW and all(r["step"] == 1 for r in rows)
    r = rows[0]
    assert r["temp"] == 25.0 and r["dew"] == 18.0 and 64 < r["rh"] < 66            # 25 C / 18 C dew point: ~65 %
    assert r["wind"] == 6.0 and r["gust"] == 9.0 and r["dir"] == 70 and r["cloud"] == 40 and r["src"] == "RRFS 18Z"
    wet = [x for x in rows if x["p"] == 0.5]
    assert wet and all(x["type"] == "Rain" for x in wet)
    assert all(x["type"] == "" for x in rows if x["p"] == 0.0)
    assert all(x["p24"] == 6.0 for x in rows)                                        # 12 wet hours of 0.5 in every 24
    gap = _cells()
    del gap[NOW - timedelta(hours=3)]
    assert WS.rrfs_rows(gap, NOW)[0]["p24"] is None                                   # a missing hour: no total
    assert WS.rh_from(20.0, 20.0) == 100.0 and WS.rh_from(None, 1.0) is None


@pytest.fixture
def wx(monkeypatch):
    monkeypatch.setenv("LAND_WEATHER", "1")
    monkeypatch.setenv("RRFS_WEATHER", "1")
    fake = FakeCells(_cells())
    monkeypatch.setattr(A, "RRFS", fake)
    return fake


def _blank(station=LAND):
    return {"station": station, "error": None, "table_html": None, "model": "GFS"}


def test_rrfs_tab_offered_inside_the_grids_only(wx, monkeypatch):
    out = A._weather_payload(_blank(), LAND, None, "US", "GFS")
    assert out["weather_models"] == ["GFS", "ECMWF", "ICON", "AIFS", "RRFS", "NWS"]
    paris = "pt_48850N_2350E"
    out = A._weather_payload(_blank(paris), paris, None, "US", "RRFS")
    assert "RRFS" not in out["weather_models"] and out["model"] == "GFS" and "om_url" in out
    monkeypatch.delenv("RRFS_WEATHER")
    out = A._weather_payload(_blank(), LAND, None, "US", "RRFS")
    assert out["weather_models"] == ["GFS", "ECMWF", "ICON", "AIFS", "NWS"] and out["model"] == "GFS"


def test_rrfs_tab_is_built_by_the_server_without_open_meteo(wx):
    out = A._weather_payload(_blank(), LAND, None, "US", "RRFS")
    assert out["model"] == "RRFS" and out["error"] is None and "om_url" not in out
    assert out["table_html"] and "NOAA RRFS" in out["weather"]["credit"]
    assert out["graph_header"]["cycle"] == "Weather · NOAA RRFS 18Z"
    assert wx.asked == [("cells", 21.45, -158.0)]
    assert out["weather"]["first"].endswith(":00 AM") or out["weather"]["first"].endswith(":00 PM")


def test_rrfs_tab_sources_line_and_failure(wx, monkeypatch):
    cells = _cells()
    for t in list(cells)[:40]:
        cells[t] = dict(cells[t], label="RRFS 19Z")
    monkeypatch.setattr(A, "RRFS", FakeCells(cells))
    out = A._weather_payload(_blank(), LAND, None, "US", "RRFS")
    assert out["graph_header"]["cycle"].startswith("Weather · NOAA RRFS 19Z to ")
    assert out["graph_header"]["cycle"].endswith(", then RRFS 18Z")
    monkeypatch.setattr(A, "RRFS", FakeCells({}))
    out = A._weather_payload(_blank(), LAND, None, "US", "RRFS")
    assert out["error"] == A.WEATHER_DOWN and not out.get("table_html") and "om_url" not in out


def test_rrfs_is_never_posted(wx):
    r = A.app.test_client().post(f"/api/forecast/weather?station={LAND}&model=RRFS", data=b"{}",
                                 headers={"Content-Type": "text/plain"})
    assert r.status_code == 400


def test_browser_open_meteo_failures_are_counted(monkeypatch):
    monkeypatch.setattr(A, "_OM_FAILS", {})
    c = A.app.test_client()
    for reason in ("timeout", "network", "http-503", "timeout"):
        assert c.post(f"/api/weather-fail?reason={reason}").status_code == 204
    for bad in ("", "x" * 40, "http-99", "%3Cscript%3E", "timeout%26x"):
        assert c.post("/api/weather-fail?reason=" + bad).status_code == 400, bad
    assert c.get("/api/weather-fail?reason=timeout").status_code == 405
    w = c.get("/healthz").get_json()["weather"]
    assert w["browser_om_fails"] == {"timeout": 2, "network": 1, "http-503": 1}
