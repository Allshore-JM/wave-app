"""Weather at land forecast points (plan section 91, 91c): weather_sources on captured real answers, the app's weather
payload, the browser-side Open-Meteo route, the model rules and the flag. Fixtures: tests/fixtures/weather
(tests/capture_weather_fixtures.py). Since section 91c the server never asks Open-Meteo's forecast: the visitor's
browser does and POSTs the answer to /api/forecast/weather, where it is checked and built."""
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


def om_body(**change):
    """The captured Open-Meteo answer, with top-level keys or hourly series changed."""
    d = json.loads(raw("om_oahu.json"))
    for k, v in change.items():
        if k in d:
            d[k] = v
        else:
            d["hourly"][k] = v
    return json.dumps(d).encode()


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

    def __init__(self, init=INIT):
        self.calls = []
        self.init = init

    def __call__(self, url, max_bytes, headers=None):
        self.calls.append(url)
        if "/static/meta.json" in url:
            if self.init is None:
                raise IOError("meta down")
            ep = int(self.init.replace(tzinfo=timezone.utc).timestamp())
            return json.dumps({"last_run_initialisation_time": ep}).encode()
        if "/v1/forecast" in url:
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
    assert (doc["lat"], doc["lon"]) == (21.49691, -157.96875)          # the answer's own grid point
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


def test_the_url_the_browser_asks():
    u = W.om_url(21.4567, -158.00049)
    assert u.startswith(W.OM_FORECAST + "?latitude=21.457&longitude=-158.0&")
    q = dict(p.split("=", 1) for p in u.split("?", 1)[1].split("&"))
    assert q["hourly"].split(",") == list(W.OM_VARS)
    assert q["models"].split(",") == [W.OM_NAME[m] for m in W.OM_MODELS]
    assert q["timezone"] == "GMT" and q["wind_speed_unit"] == "ms"
    assert q["forecast_days"] == str(W.OM_DAYS) and q["past_days"] == str(W.OM_PAST_DAYS)


# ----------------------------------------------------------------------------- the checks of a handed-in answer

def test_the_capture_passes_the_checks(doc):
    assert W.check_open_meteo(doc, *OAHU) is doc


@pytest.mark.parametrize("change, why", [
    ({"latitude": 22.5}, "another place"),                    # a degree north of the click
    ({"longitude": -157.5}, "another place"),                 # half a degree east
    ({"latitude": None}, "position"),
    ({"longitude": "x"}, "position"),
])
def test_an_answer_for_another_place_is_refused(change, why):
    d = W.parse_open_meteo(om_body(**change))
    with pytest.raises(W.WeatherError, match=why):
        W.check_open_meteo(d, *OAHU)


def test_the_date_line_is_one_place():
    d = W.parse_open_meteo(om_body(latitude=-17.0, longitude=179.95))
    for m in W.OM_MODELS:                                     # temperatures stay; the place is what is checked
        assert any(x == x for x in d["m"][m]["temperature_2m"])
    assert W.check_open_meteo(d, -17.0, -179.9) is d          # 0.15 degrees apart across 180


def test_hours_must_be_hourly_and_not_too_many(hourly):
    times = list(hourly["time"])
    times[10], times[11] = times[11], times[10]
    with pytest.raises(W.WeatherError, match="hourly"):
        W.check_open_meteo(W.parse_open_meteo(om_body(time=times)), *OAHU)
    long = [f"2026-10-{1 + i // 24:02d}T{i % 24:02d}:00" for i in range(W.OM_MAX_HOURS + 1)]
    with pytest.raises(W.WeatherError, match="too long"):
        W.parse_open_meteo(json.dumps({"latitude": 21.5, "longitude": -158.0, "hourly": {"time": long}}).encode())


@pytest.mark.parametrize("var, value", [
    ("temperature_2m", 61.0), ("temperature_2m", -91.0), ("dew_point_2m", 41.0), ("relative_humidity_2m", 101.0),
    ("cloud_cover", -1.0), ("precipitation", 501.0), ("rain", -0.1), ("showers", 900.0), ("snowfall", 201.0),
    ("weather_code", 100.0), ("wind_speed_10m", 121.0), ("wind_direction_10m", 361.0), ("wind_gusts_10m", -2.0),
])
def test_a_value_out_of_bounds_is_refused(hourly, var, value):
    key = f"{var}_{W.OM_NAME['ICON']}"
    series = list(hourly[key])
    series[30] = value
    with pytest.raises(W.WeatherError, match=var):
        W.check_open_meteo(W.parse_open_meteo(om_body(**{key: series})), *OAHU)


def test_none_values_are_no_data_not_refused(hourly):
    key = f"wind_speed_10m_{W.OM_NAME['GFS']}"
    series = list(hourly[key])
    series[30] = None
    d = W.parse_open_meteo(om_body(**{key: series}))
    assert W.check_open_meteo(d, *OAHU) is d and math.isnan(d["m"]["GFS"]["wind_speed_10m"][30])


def test_an_answer_without_temperatures_is_refused(hourly):
    change = {f"temperature_2m_{W.OM_NAME[m]}": [None] * len(hourly["time"]) for m in W.OM_MODELS}
    with pytest.raises(W.WeatherError, match="temperatures"):
        W.check_open_meteo(W.parse_open_meteo(om_body(**change)), *OAHU)


# ----------------------------------------------------------------------------- the rows

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


def test_nws_table():
    s = service()
    t = s.table("NWS", *OAHU, now=datetime(2026, 11, 9, 17))
    assert t["source"] == "nws" and t["rows"] and t["run"] == datetime(2026, 11, 9, 16, 1, 51)
    assert t["elev"] == pytest.approx(171.9, abs=0.1)


# ----------------------------------------------------------------------------- the service: never Open-Meteo's forecast

def test_an_open_meteo_table_needs_the_handed_answer(doc):
    s = service()
    with pytest.raises(W.WeatherError, match="no Open-Meteo answer"):
        s.table("GFS", *OAHU, now=NOW)
    t = s.table("ECMWF", *OAHU, now=NOW, om=doc)
    assert t["run"] == INIT and t["source"] == "open-meteo" and t["elev"] == 207.0 and t["rows"]
    assert s.fetch.count("/v1/forecast") == 0                    # the server never asks it (section 91c)
    assert s.health()["om_built"] == 1


def test_om_key_follows_the_runs():
    fake = Fake()
    s = service(fake)
    ep = int(INIT.replace(tzinfo=timezone.utc).timestamp())
    n = sum(len(v) for v in W.OM_META_NAMES.values())           # every metadata file read (GFS has two)
    assert s.om_key() == "-".join([str(ep)] * n)
    s._mono[0] += 60                                             # within ten minutes: the same key, not read again
    reads = fake.count("/static/meta.json")
    assert s.om_key() == "-".join([str(ep)] * n) and fake.count("/static/meta.json") == reads
    fake.init = INIT + timedelta(hours=6)                        # a new run: another key after the next read
    s._mono[0] += W.META_TTL_S + 1
    assert s.om_key() == "-".join([str(ep + 6 * 3600)] * n)


def test_om_key_without_metadata():
    s = service(Fake(init=None))
    assert s.om_key() == "-".join(["x"] * sum(len(v) for v in W.OM_META_NAMES.values()))


def test_a_refused_answer_is_counted_without_its_body():
    s = service()
    s.refused("Open-Meteo answer for another place")
    h = s.health()
    assert h["om_refused"] == 1 and h["last_error"] == "refused: Open-Meteo answer for another place"


def test_capped_get_keeps_a_429s_words(monkeypatch):
    """The refusal's body (first 300 characters, one line) rides on the error (the NWS and the metadata use it)."""
    import io
    import app as A2
    body = b'{"reason":"Daily API request limit\n exceeded. Please try again tomorrow.","error":true}' + b" " * 900

    class R:
        status_code = 429
        headers = {}
        class raw:                                   # urllib3's HTTPResponse.read signature
            buf = io.BytesIO(body)

            @classmethod
            def read(cls, amt=None, decode_content=None):
                return cls.buf.read(amt)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False
    session = type("S", (), {"get": staticmethod(lambda url, **k: R())})()
    with pytest.raises(A2.HTTPStatusError) as got:
        A2._capped_get("https://api.open-meteo.com/v1/forecast", 1000, None, (1, 1), 5, session=session)
    assert got.value.status == 429 and "Daily API request limit exceeded" in got.value.detail
    assert len(got.value.detail) <= 300 and "\n" not in got.value.detail and "Daily" in str(got.value)


# ----------------------------------------------------------------------------- the app

@pytest.fixture
def A(monkeypatch):
    import app as A
    monkeypatch.setenv("LAND_WEATHER", "1")
    s = service()
    monkeypatch.setattr(A, "WEATHER", s)
    real = W.WeatherService.table

    def table(self, model, lat, lon, now=None, om=None):           # the capture's own morning
        return real(self, model, lat, lon, now=now or (NOW if model != "NWS" else datetime(2026, 11, 9, 17)), om=om)
    monkeypatch.setattr(W.WeatherService, "table", table)
    return A


def blank():
    return {"station": POINT, "error": None, "table_html": None, "model": "GFS"}


def test_the_get_hands_the_browser_the_request(A):
    out = A._weather_payload(blank(), POINT, None, "US", "ECMWF")
    assert out["kind"] == "weather" and out["model"] == "ECMWF" and out["error"] is None
    assert out["weather_models"] == ["GFS", "ECMWF", "ICON", "AIFS", "NWS"]
    assert out["om_url"] == W.om_url(*OAHU) and out["om_key"] == A.WEATHER.om_key()
    assert out["table_html"] is None and "graph_data" not in out and "weather" not in out
    assert out["tz_label"] == "Pacific/Honolulu" and out["point"]["land"] is True
    assert A.WEATHER.fetch.count("/v1/forecast") == 0             # never asked by the server


def test_weather_payload(A):
    out = A._weather_payload(blank(), POINT, None, "US", "GFS", raw("om_oahu.json"))
    assert out["kind"] == "weather" and out["error"] is None and out["model"] == "GFS"
    assert "om_url" not in out
    rows = W.om_rows(W.parse_open_meteo(raw("om_oahu.json")), "GFS", INIT, NOW)
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
    assert out["graph_header"]["location"] == "21.450N 158.000W (elevation 679 ft)"   # as a swell point's line + height
    total = sum(r["p"] for r in rows if r["p"] is not None)
    assert out["weather"]["total"] == f"{total / 25.4:.2f}"
    assert A.WEATHER.fetch.count("/v1/forecast") == 0


def test_weather_payload_metric_and_aifs(A):
    out = A._weather_payload(blank(), POINT, None, "Metric", "AIFS", raw("om_oahu.json"))
    th = out["table_html"]
    assert out["model"] == "AIFS" and "&deg;C" in th and "(mm)" in th and "(km/h)" in th
    assert "Gust" not in th and out["graph_data"]["gust"] is None
    assert out["graph_header"]["location"] == "21.450N 158.000W (elevation 207 m)"


def test_weather_payload_nws(A):
    out = A._weather_payload(blank(), POINT, None, "US", "NWS")
    assert out["model"] == "NWS" and "National Weather Service" in out["weather"]["credit"]
    assert out["graph_header"]["cycle"].startswith("Weather · NWS grid forecast") and "om_url" not in out


def test_nws_not_offered_abroad(A):
    paris = "pt_48850N_2350E"
    out = A._weather_payload(dict(blank(), station=paris), paris, None, "US", "NWS")
    assert "NWS" not in out["weather_models"] and out["model"] == "GFS"
    assert out["om_url"] == W.om_url(48.85, 2.35)


def test_nws_without_a_grid_hands_over_gfs(A, monkeypatch):
    monkeypatch.setattr(W.WeatherService, "nws_grid", lambda self, lat, lon: None)
    out = A._weather_payload(blank(), POINT, None, "US", "NWS")
    assert out["model"] == "GFS" and "NWS" not in out["weather_models"]
    assert out["om_url"] == W.om_url(*OAHU) and out["error"] is None and not out.get("table_html")


def test_a_tz_link_wins(A):
    out = A._weather_payload(blank(), POINT, "America/Los_Angeles", "US", "GFS")
    assert out["tz_label"] == "America/Los_Angeles"
    out = A._weather_payload(blank(), POINT, "Not/AZone", "US", "GFS")
    assert out["tz_label"] == "Pacific/Honolulu"


@pytest.mark.parametrize("body", [
    b"", b"not json", b"[1, 2]", b'{"error": true, "reason": "x"}', b'{"hourly": {"time": ["x"]}}',
    "OM_FAR", "OM_HOT",
])
def test_a_refused_answer_is_a_passing_error(A, body):
    if body == "OM_FAR":
        body = om_body(latitude=23.0)
    elif body == "OM_HOT":
        h = json.loads(raw("om_oahu.json"))["hourly"]
        key = f"temperature_2m_{W.OM_NAME['GFS']}"
        series = list(h[key])
        series[50] = 99.0
        body = om_body(**{key: series})
    out = A._weather_payload(blank(), POINT, None, "US", "GFS", body)
    assert out["error"] == A.WEATHER_DOWN and not out.get("final") and not out.get("table_html")
    assert A.WEATHER.health()["om_refused"] == 1


def test_failures_are_not_final(A, monkeypatch):
    def down(self, *a, **k):
        raise W.WeatherError("x")

    def busy(self, *a, **k):
        raise W.WeatherBusy("x")
    monkeypatch.setattr(W.WeatherService, "table", down)
    out = A._weather_payload(blank(), POINT, None, "US", "GFS", raw("om_oahu.json"))
    assert out["error"] == A.WEATHER_DOWN and not out.get("final") and not out.get("table_html")
    monkeypatch.setattr(W.WeatherService, "table", busy)
    out = A._weather_payload(blank(), POINT, None, "US", "NWS")
    assert out["error"] == A.WEATHER_BUSY and out["busy"] is True and not out.get("final")


def land_refusal(station, tz=None):
    import app as A
    return A._point_failure(A.point_forecast.REFUSALS["land"], "land")


def test_land_click_answers_the_weather(A, monkeypatch):
    monkeypatch.setattr(A, "point_forecast_data", land_refusal)
    out = A.compute_forecast_payload(POINT, None, "US", "ECMWF", compact=True)
    assert out["kind"] == "weather" and out["model"] == "ECMWF" and out["om_url"] and not out["table_html"]
    assert not out.get("final") and not out.get("reason")
    out = A.compute_forecast_payload(POINT, None, "US", "ECMWF", compact=True, om=raw("om_oahu.json"))
    assert out["kind"] == "weather" and out["table_html"] and out["model"] == "ECMWF"


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


# ----------------------------------------------------------------------------- the POST route

def post(A, body, station=POINT, model="GFS", **q):
    qs = "&".join(f"{k}={v}" for k, v in dict(station=station, model=model, unit="US", compact="1", **q).items())
    return A.app.test_client().post("/api/forecast/weather?" + qs, data=body,
                                    headers={"Content-Type": "text/plain"})


def test_post_builds_the_table(A, monkeypatch):
    monkeypatch.setattr(A, "point_forecast_data", land_refusal)
    r = post(A, raw("om_oahu.json"), model="ICON")
    assert r.status_code == 200 and r.headers["Cache-Control"] == "no-store"
    d = r.get_json()
    assert d["kind"] == "weather" and d["model"] == "ICON" and d["table_html"] and d["error"] is None
    want = A.compute_forecast_payload(POINT, None, "US", "ICON", compact=True, om=raw("om_oahu.json"))
    assert d["table_html"] == want["table_html"] and d["summary_html"] == want["summary_html"]
    assert d["graph_data"] == json.loads(json.dumps(want["graph_data"]))
    assert A.WEATHER.fetch.count("/v1/forecast") == 0


def test_post_is_never_cached(A, monkeypatch):
    monkeypatch.setattr(A, "point_forecast_data", land_refusal)
    calls = []
    real = A._weather_payload

    def counted(*a, **k):
        calls.append(1)
        return real(*a, **k)
    monkeypatch.setattr(A, "_weather_payload", counted)
    for _ in range(2):
        assert post(A, raw("om_oahu.json")).get_json()["table_html"]
    assert len(calls) == 2


@pytest.mark.parametrize("station, model", [("51201", "GFS"), (POINT, "NWS"), (POINT, "SWAN"), ("../x", "GFS")])
def test_post_needs_a_point_and_an_open_meteo_model(A, station, model):
    r = post(A, raw("om_oahu.json"), station=station, model=model)
    assert r.status_code == 400 and r.get_json()["final"] is True


def test_post_with_the_flag_off(A, monkeypatch):
    monkeypatch.setenv("LAND_WEATHER", "")
    assert post(A, raw("om_oahu.json")).status_code == 400


def test_post_body_cap(A, monkeypatch):
    monkeypatch.setattr(A, "point_forecast_data", land_refusal)
    r = post(A, b" " * (A.WEATHER_POST_MAX + 1))
    assert r.status_code == 413 and r.get_json()["error"] == A.WEATHER_DOWN


def test_post_at_sheltered_water_answers_the_refusal(A, monkeypatch):
    monkeypatch.setattr(A, "point_forecast_data",
                        lambda s, tz=None: A._point_failure(A.point_forecast.REFUSALS["sheltered"], "sheltered"))
    d = post(A, raw("om_oahu.json")).get_json()
    assert d.get("reason") == "sheltered" and "kind" not in d and not d.get("table_html")


def test_a_refused_body_is_never_logged(A, monkeypatch, caplog):
    monkeypatch.setattr(A, "point_forecast_data", land_refusal)
    secret = b'{"latitude": 21.5, "longitude": -158.0, "hourly": {"time": ["2026-11-09T00:00"]}, "z": "SECRET-777"}'
    with caplog.at_level("DEBUG"):
        d = post(A, secret).get_json()
    assert d["error"] == A.WEATHER_DOWN
    assert "SECRET-777" not in caplog.text and "SECRET-777" not in str(A.WEATHER.health())


def test_render_full_says_scripts_are_needed(A, monkeypatch):
    monkeypatch.setattr(A, "point_forecast_data", land_refusal)
    html = A.app.test_client().get(f"/?station={POINT}&render=full").get_data(as_text=True)
    assert A.WEATHER_NEEDS_JS in html
    assert A.WEATHER.fetch.count("/v1/forecast") == 0


# ----------------------------------------------------------------------------- the page

def test_healthz_reports_weather(A):
    body = A.app.test_client().get("/healthz").get_json()
    w = body["weather"]
    assert w["enabled"] is True and "om_built" in w and "om_refused" in w and "om_calls" not in w


def test_page_state_names_the_flag(A, monkeypatch):
    html = A.app.test_client().get("/?station=51201").get_data(as_text=True)
    assert '"land_weather": true' in html
    monkeypatch.setenv("LAND_WEATHER", "")
    html = A.app.test_client().get("/?station=51201").get_data(as_text=True)
    assert '"land_weather": false' in html
