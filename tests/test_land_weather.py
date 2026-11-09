"""Weather at land forecast points (plan section 91): weather_sources on captured real answers, the app's weather
payload, the model rules and the flag. Fixtures: tests/fixtures/weather (tests/capture_weather_fixtures.py)."""
import json
import math
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import weather_sources as W  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "weather")
INIT = datetime(2026, 11, 9, 0, 0)                 # the runs the capture holds (00Z of 2026-11-09)
NOW = datetime(2026, 11, 9, 8, 20)                 # about when it was captured
OAHU = (21.45, -158.0)
POINT = "pt_21450N_158000W"


def raw(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return f.read()


@pytest.fixture(scope="module")
def doc():
    return W.parse_open_meteo(raw("om_oahu.json"))


@pytest.fixture(scope="module")
def hourly():
    return json.loads(raw("om_oahu.json"))["hourly"]


def grid():
    return json.loads(raw("nws_grid_oahu.json"))["properties"]


class Status(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.status = status


class Fake:
    """A fetch for the service: Open-Meteo's meta files and forecast, the NWS points and grid (Oahu; 404 elsewhere)."""

    def __init__(self, init=INIT, om_status=None):
        self.calls = []
        self.init = init
        self.om_status = om_status

    def __call__(self, url, max_bytes, headers=None):
        self.calls.append(url)
        if "/static/meta.json" in url:
            if self.init is None:
                raise IOError("meta down")
            ep = int(self.init.replace(tzinfo=timezone.utc).timestamp())
            return json.dumps({"last_run_initialisation_time": ep}).encode()
        if "/v1/forecast" in url:
            if self.om_status:
                raise Status(self.om_status)
            return raw("om_oahu.json")
        if "/points/" in url:
            if url.split("/points/")[1].startswith("21."):
                return raw("nws_points_oahu.json")
            raise Status(404)
        if "/gridpoints/" in url:
            return raw("nws_grid_oahu.json")
        raise IOError(url)

    def count(self, part):
        return sum(1 for u in self.calls if part in u)


def service(fake=None, **kw):
    mono = [1000.0]
    s = W.WeatherService(fake or Fake(), now=lambda: datetime(2026, 11, 9, 8, tzinfo=timezone.utc).timestamp(),
                         mono=lambda: mono[0], **kw)
    s._mono = mono
    return s


# ----------------------------------------------------------------------------- reading Open-Meteo

def test_open_meteo_answer_parsed(doc):
    assert doc["elev"] == 207.0
    assert doc["times"][0] == datetime(2026, 11, 8, 0) and len(doc["times"]) == 408
    assert set(doc["m"]) == set(W.OM_MODELS)
    for m in W.OM_MODELS:
        assert set(doc["m"][m]) == set(W.OM_VARS)
    # AIFS gives no gusts at all (the table then has no gust column)
    assert all(math.isnan(x) for x in doc["m"]["AIFS"]["wind_gusts_10m"])


def test_open_meteo_refusal_and_empty_answers():
    with pytest.raises(W.WeatherError):
        W.parse_open_meteo(b'{"error": true, "reason": "Parameter models invalid"}')
    with pytest.raises(W.WeatherError):
        W.parse_open_meteo(b'{"hourly": {"time": []}}')


@pytest.mark.parametrize("model, h, hour, step", [
    ("GFS", 1, 1, 1), ("GFS", 120, 0, 1), ("GFS", 121, 1, None), ("GFS", 123, 3, 3),
    ("ICON", 78, 6, 1), ("ICON", 79, 7, None), ("ICON", 81, 9, 3),
    ("ECMWF", 3, 3, 3), ("ECMWF", 4, 4, None), ("ECMWF", 144, 0, 3), ("ECMWF", 147, 3, None), ("ECMWF", 150, 6, 6),
    ("AIFS", 6, 6, 6), ("AIFS", 3, 3, None),
])
def test_native_steps(model, h, hour, step):
    t = INIT + timedelta(hours=h)
    assert t.hour == hour
    assert W.om_step_h(model, INIT, t) == step


def test_native_steps_unknown_model():
    with pytest.raises(ValueError):
        W.om_step_h("UKMO", INIT, INIT)


@pytest.mark.parametrize("model, span_h, first_after", [("GFS", 1, 120), ("ICON", 1, 78), ("ECMWF", 3, 144),
                                                         ("AIFS", 6, None)])
def test_rows_at_the_models_own_steps(doc, model, span_h, first_after):
    rows = W.om_rows(doc, model, INIT, NOW)
    assert rows[0]["t"] == datetime(2026, 11, 9, 8) if span_h == 1 else rows[0]["t"].hour % span_h == 0
    assert rows[0]["t"] >= datetime(2026, 11, 9, 8)
    for a, b in zip(rows, rows[1:]):
        assert b["t"] > a["t"]
        assert b["step"] == round((b["t"] - a["t"]).total_seconds() / 3600)   # rows follow their own steps
    if first_after is not None:
        late = [r for r in rows if (r["t"] - INIT).total_seconds() / 3600 > first_after]
        assert late and all(r["step"] > span_h for r in late)


def test_step_and_24h_precipitation_are_the_hourly_sums(doc, hourly):
    name = W.OM_NAME["GFS"]
    times = [datetime.fromisoformat(s) for s in hourly["time"]]
    pr = dict(zip(times, hourly[f"precipitation_{name}"]))
    rows = W.om_rows(doc, "GFS", INIT, NOW)
    seen = 0
    for r in rows:
        step = [pr.get(r["t"] - timedelta(hours=k)) for k in range(r["step"])]
        day = [pr.get(r["t"] - timedelta(hours=k)) for k in range(24)]
        assert r["p"] == (None if None in step else round(sum(step), 2))
        assert r["p24"] == (None if None in day else round(sum(day), 2))
        seen += bool(r["p"])
    assert seen > 5                                              # the capture has rain to sum


def test_aifs_rows_have_no_gust(doc):
    rows = W.om_rows(doc, "AIFS", INIT, NOW)
    assert rows and all(r["gust"] is None for r in rows)
    assert all(r["step"] == 6 for r in rows[1:])


@pytest.mark.parametrize("mm, codes, rain, snow, want", [
    (0.05, [95], 0, 0, ""), (None, [61], 1, 0, ""), (1, [95, 61], 1, 0, "Thunder"), (1, [66, 71], 0, 1, "Ice"),
    (1, [61, 71], 0.5, 0.5, "Mix"), (1, [73], 0, 1, "Snow"), (1, [61], 1, 0, "Rain"), (1, [], 0, 0.2, "Snow"),
])
def test_precipitation_type(mm, codes, rain, snow, want):
    assert W.precip_type(mm, codes, rain, snow) == want


# ----------------------------------------------------------------------------- NWS

def test_nws_box():
    assert W.nws_box(*OAHU) and W.nws_box(39.74, -104.99) and W.nws_box(18.4, -66.1)
    assert not W.nws_box(48.85, 2.35) and not W.nws_box(-33.9, 151.2)


def test_nws_rows_from_the_grid():
    g = grid()
    rows = W.nws_rows(g, datetime(2026, 11, 9, 17))
    assert rows[0]["t"] == datetime(2026, 11, 9, 17)
    assert all(b["t"] > a["t"] for a, b in zip(rows, rows[1:]))
    # speeds are km/h in the grid, m/s in the rows
    kmh = next(v for v in g["windSpeed"]["values"]
               if W._iso(v["validTime"].split("/")[0]) <= rows[0]["t"] < W._iso(v["validTime"].split("/")[0])
               + W._dur(v["validTime"].split("/")[1]))["value"]
    assert rows[0]["wind"] == pytest.approx(kmh / 3.6, abs=0.01)
    # a QPF block shows in the row where it ends
    blocks = [(W._iso(v["validTime"].split("/")[0]) + W._dur(v["validTime"].split("/")[1]), v["value"])
              for v in g["quantitativePrecipitation"]["values"] if v["value"] is not None]
    checked = 0
    for prev, r in zip(rows, rows[1:]):                          # a block counts once: in the row after the last one
        ends = [x for b, x in blocks if prev["t"] < b <= r["t"]]
        assert r["p"] == (round(sum(ends), 2) if ends else None)
        checked += bool(ends)
    assert checked >= 5


def test_nws_outside_its_grids_is_none():
    s = service()
    assert s.table("NWS", 48.85, 2.35) is None                     # the box test: never asked
    assert s.fetch.count("/points/") == 0


def test_nws_point_404_is_none():
    s = service()
    assert s.nws_grid(41.0, -70.0) is None                        # inside a box, the NWS answers 404
    assert s.nws_grid(41.0, -70.0) is None                        # remembered: not asked again
    assert s.fetch.count("/points/") == 1


# ----------------------------------------------------------------------------- the service

def test_one_open_meteo_call_per_model_run():
    fake = Fake()
    s = service(fake)
    t1 = s.table("GFS", *OAHU, now=NOW)
    t2 = s.table("ECMWF", *OAHU, now=NOW)
    assert fake.count("/v1/forecast") == 1
    assert t1["run"] == INIT and t2["run"] == INIT and t1["source"] == "open-meteo" and t1["elev"] == 207.0
    s._mono[0] += W.META_TTL_S + 1                                 # metadata read again: same runs, no new call
    s.table("ICON", *OAHU, now=NOW)
    assert fake.count("/v1/forecast") == 1
    fake.init = INIT + timedelta(hours=6)                         # a new run: asked again
    s._mono[0] += W.META_TTL_S + 1
    assert s.table("GFS", *OAHU, now=NOW)["run"] == INIT + timedelta(hours=6)
    assert fake.count("/v1/forecast") == 2


def test_a_failure_is_remembered_a_minute():
    fake = Fake(om_status=500)
    s = service(fake)
    with pytest.raises(W.WeatherError):
        s.table("GFS", *OAHU, now=NOW)
    with pytest.raises(W.WeatherError):
        s.table("GFS", *OAHU, now=NOW)
    assert fake.count("/v1/forecast") == 1
    fake.om_status = None
    s._mono[0] += W.FAIL_TTL_S + 1
    assert s.table("GFS", *OAHU, now=NOW)["rows"]


def test_429_blocks_and_answers_busy():
    fake = Fake(om_status=429)
    s = service(fake)
    with pytest.raises(W.WeatherBusy):
        s.table("GFS", *OAHU, now=NOW)
    fake.om_status = None
    with pytest.raises((W.WeatherBusy, W.WeatherError)):
        s.table("GFS", 21.3, -157.8, now=NOW)                    # another point while blocked: not asked
    assert fake.count("/v1/forecast") == 1


def test_daily_budget():
    s = service()
    s._day = (datetime(2026, 11, 9).date(), W.OM_DAILY_LIMIT - W.OM_COST + 0.5)
    with pytest.raises(W.WeatherBusy):
        s.table("GFS", *OAHU, now=NOW)
    assert s.fetch.count("/v1/forecast") == 0
    assert s.health()["busy"] >= 1


def test_nws_table():
    s = service()
    t = s.table("NWS", *OAHU, now=datetime(2026, 11, 9, 17))
    assert t["source"] == "nws" and t["rows"] and t["run"] == datetime(2026, 11, 9, 16, 1, 51)
    assert t["elev"] == pytest.approx(171.9, abs=0.1)


# ----------------------------------------------------------------------------- the app

@pytest.fixture
def A(monkeypatch):
    import app as A
    monkeypatch.setenv("LAND_WEATHER", "1")
    s = service()
    monkeypatch.setattr(A, "WEATHER", s)
    real = W.WeatherService.table

    def table(self, model, lat, lon, now=None):                    # the capture's own morning
        return real(self, model, lat, lon, now=now or (NOW if model != "NWS" else datetime(2026, 11, 9, 17)))
    monkeypatch.setattr(W.WeatherService, "table", table)
    return A


def blank():
    return {"station": POINT, "error": None, "table_html": None, "model": "GFS"}


def test_weather_payload(A):
    out = A._weather_payload(blank(), POINT, None, "US", "GFS")
    assert out["kind"] == "weather" and out["error"] is None and out["model"] == "GFS"
    assert out["weather_models"] == ["GFS", "ECMWF", "ICON", "AIFS", "NWS"]
    assert out["tz_label"] == "Pacific/Honolulu" and out["point"]["land"] is True
    rows = W.om_rows(A.WEATHER.open_meteo(*OAHU)[0], "GFS", INIT, NOW)
    th = out["table_html"]
    assert th.count('<tr class="sky-') == len(rows) and "wx-table" in th and "Gust" in th and "Sun/Moon" in th
    assert th.isascii()
    assert "&deg;F" in th and "(in)" in th and "(mph)" in th
    gd = out["graph_data"]
    hours = int((rows[-1]["t"] - rows[0]["t"]).total_seconds() // 3600) + 1
    assert gd["kind"] == "weather" and len(gd["labels"]) == hours == len(gd["temp"]) == len(gd["precip"])
    assert sum(x is not None for x in gd["temp"]) == len(rows)
    assert out["summary_html"].count("sum-day") >= 15
    assert "Open-Meteo" in out["weather"]["credit"]
    assert out["graph_header"]["cycle"].endswith("run 2026-11-09 00 UTC")
    assert "679 ft" in out["graph_header"]["location"]
    total = sum(r["p"] for r in rows if r["p"] is not None)
    assert out["weather"]["total"] == f"{total / 25.4:.2f}"


def test_weather_payload_metric_and_aifs(A):
    out = A._weather_payload(blank(), POINT, None, "Metric", "AIFS")
    th = out["table_html"]
    assert out["model"] == "AIFS" and "&deg;C" in th and "(mm)" in th and "(km/h)" in th
    assert "Gust" not in th and out["graph_data"]["gust"] is None
    assert "207 m" in out["graph_header"]["location"]


def test_weather_payload_nws(A):
    out = A._weather_payload(blank(), POINT, None, "US", "NWS")
    assert out["model"] == "NWS" and "National Weather Service" in out["weather"]["credit"]
    assert out["graph_header"]["cycle"].startswith("Weather · NWS grid forecast")


def test_nws_not_offered_abroad(A):
    paris = "pt_48850N_2350E"
    out = A._weather_payload(dict(blank(), station=paris), paris, None, "US", "NWS")
    assert "NWS" not in out["weather_models"] and out["model"] == "GFS"


def test_a_tz_link_wins(A):
    out = A._weather_payload(blank(), POINT, "America/Los_Angeles", "US", "GFS")
    assert out["tz_label"] == "America/Los_Angeles"
    out = A._weather_payload(blank(), POINT, "Not/AZone", "US", "GFS")
    assert out["tz_label"] == "Pacific/Honolulu"


def test_failures_are_not_final(A, monkeypatch):
    def down(self, *a, **k):
        raise W.WeatherError("x")

    def busy(self, *a, **k):
        raise W.WeatherBusy("x")
    monkeypatch.setattr(W.WeatherService, "table", down)
    out = A._weather_payload(blank(), POINT, None, "US", "GFS")
    assert out["error"] == A.WEATHER_DOWN and not out.get("final") and not out.get("table_html")
    monkeypatch.setattr(W.WeatherService, "table", busy)
    out = A._weather_payload(blank(), POINT, None, "US", "GFS")
    assert out["error"] == A.WEATHER_BUSY and out["busy"] is True and not out.get("final")


def land_refusal(station, tz=None):
    import app as A
    return A._point_failure(A.point_forecast.REFUSALS["land"], "land")


def test_land_click_answers_the_weather(A, monkeypatch):
    monkeypatch.setattr(A, "point_forecast_data", land_refusal)
    out = A.compute_forecast_payload(POINT, None, "US", "ECMWF", compact=True)
    assert out["kind"] == "weather" and out["model"] == "ECMWF" and out["table_html"]
    assert not out.get("final") and not out.get("reason")


def test_flag_off_keeps_the_refusal(A, monkeypatch):
    monkeypatch.setattr(A, "point_forecast_data", land_refusal)
    monkeypatch.setenv("LAND_WEATHER", "0")
    out = A.compute_forecast_payload(POINT, None, "US", "GFS", compact=True)
    assert out.get("final") is True and out.get("reason") == "land" and "kind" not in out
    assert A.WEATHER.fetch.calls == []


def test_sheltered_water_stays_refused(A, monkeypatch):
    monkeypatch.setattr(A, "point_forecast_data",
                        lambda s, tz=None: A._point_failure(A.point_forecast.REFUSALS["sheltered"], "sheltered"))
    out = A.compute_forecast_payload(POINT, None, "US", "GFS", compact=True)
    assert out.get("reason") == "sheltered" and "kind" not in out


def test_models_never_weather_at_a_station(A):
    for m in ("ECMWF", "ICON", "AIFS", "NWS"):
        assert A.resolve_model("51201", m) == "GFS"
        assert A.resolve_model(POINT, m) == m


def test_healthz_reports_weather(A):
    body = A.app.test_client().get("/healthz").get_json()
    assert body["weather"]["enabled"] is True and "om_calls" in body["weather"]


def test_page_state_names_the_flag(A, monkeypatch):
    html = A.app.test_client().get("/?station=51201").get_data(as_text=True)
    assert '"land_weather": true' in html
    monkeypatch.setenv("LAND_WEATHER", "")
    html = A.app.test_client().get("/?station=51201").get_data(as_text=True)
    assert '"land_weather": false' in html
