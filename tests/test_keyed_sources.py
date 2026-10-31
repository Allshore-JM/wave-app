"""Plan section 69: the feeds that need a key (UK CCO and Taiwan CWA buoys; KNMI, MET Norway Frost and Taiwan CWA wind
stations). Nothing here reaches the network: CWA's answers are real captures of the keyless copies
(tests/capture_keyed_fixtures.py, turned into the REST answer's shape); CCO's, KNMI's and Frost's are built from their
documented shapes (their answers need a key). Every test that touches a key checks it never comes out."""
import base64
import io
import json
import os
import struct
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app as A  # noqa: E402
import buoy_sources as B  # noqa: E402
import wind_keyed as K  # noqa: E402
import wind_more as M  # noqa: E402
import wind_sources as W  # noqa: E402

FX = os.path.join(os.path.dirname(__file__), "fixtures", "keyed")
SECRET = "sk-test-KEY-0123456789abcdef"


def fx(name):
    with open(os.path.join(FX, name), encoding="utf-8") as f:
        return json.load(f)


class Clock:
    """buoy_sources' time module: .time() frozen at .t, everything else real."""

    def __init__(self, t):
        self.t = t

    def time(self):
        return self.t

    def __getattr__(self, name):
        import time as _t
        return getattr(_t, name)


@pytest.fixture
def clock(monkeypatch):
    c = Clock(1793500000)                               # 2026-11-01T01:46:40Z
    monkeypatch.setattr(B, "time", c)
    return c


# ============================================================================================== keys

def test_keys_come_from_the_environment_and_empty_ones_are_left_out():
    env = {"KNMI_API_KEY": " abc ", "FROST_CLIENT_ID": "", "CWA_API_KEY": "CWA-1"}
    assert K.keys_from_env(env) == {"knmi": "abc", "cwa": "CWA-1"}
    assert K.keys_from_env({}) == {}


def test_redact_hides_keys_query_keys_and_signed_urls():
    msg = ("ConnectionError: HTTPSConnectionPool(host='opendata.cwa.gov.tw'): Max retries exceeded with url: "
           "/api/v1/rest/datastore/O-A0003-001?format=JSON&Authorization=CWA-SECRET-1 (Caused by x)")
    out = K.redact(msg)
    assert "CWA-SECRET-1" not in out and "Authorization=[key]" in out
    assert K.redact("bad https://s3.example/f.nc?X-Amz-Signature=zzz&X-Amz-Credential=yyy end") == \
        "bad https://s3.example/f.nc?[signed] end"
    assert SECRET not in K.redact("Basic %s was refused" % SECRET, (SECRET,))
    assert B._redact_key("x?Authorization=abc&y=1 abc", "abc") == "x?Authorization=[key]&y=1 [key]"


def test_frost_auth_is_basic_with_an_empty_password_and_knmi_is_the_raw_key():
    h = K.frost_auth("client-id")
    assert base64.b64decode(h["Authorization"].split(" ", 1)[1]) == b"client-id:"
    assert K.knmi_auth("k1")["Authorization"] == "k1"


# ============================================================================================== CWA wind

def test_cwa_wind_parser_reads_the_rest_answer_and_the_keyless_file():
    doc = fx("cwa_wind.json")
    st, obs = K.parse_cwa_wind(doc)
    assert {"466940", "467410", "467490"} <= set(st)
    s = st["466940"]
    assert s["lat"] == 25.1333 and s["lon"] == 121.7405           # the WGS84 pair, not TWD67
    r = obs["466940"][0]
    raw = next(x for x in doc["records"]["Station"] if x["StationId"] == "466940")
    assert r["t"] == K.cwa_time(raw["ObsTime"]["DateTime"]) and r["g"] is None   # no gust (the day's highest so far)
    assert r["s"] == round(float(raw["WeatherElement"]["WindSpeed"]), 1)
    st2, obs2 = K.parse_cwa_wind({"cwaopendata": {"dataset": {"Station": doc["records"]["Station"]}}})
    assert st2 == st and obs2 == obs


def test_cwa_wind_missing_values_are_none():
    raw = json.loads(json.dumps(fx("cwa_wind.json")))
    e = raw["records"]["Station"][0]["WeatherElement"]
    e["WindSpeed"], e["WindDirection"], e["AirTemperature"] = "-99", "-99", "-99"
    st, obs = K.parse_cwa_wind(raw)
    assert raw["records"]["Station"][0]["StationId"] in st
    assert raw["records"]["Station"][0]["StationId"] not in obs      # no speed: no reading
    assert K.cwa_time("2026-11-01T01:50:00+08:00") == 1793469000 and K.cwa_time("nonsense") is None


class FakeFetch:
    """fetch(url, max_bytes, headers=None): answers by URL substring; records every call."""

    def __init__(self, routes):
        self.routes = routes
        self.calls = []

    def __call__(self, url, max_bytes, headers=None):
        self.calls.append((url, dict(headers or {})))
        for key, val in self.routes.items():
            if key in url:
                if isinstance(val, Exception):
                    raise val
                if callable(val):
                    return val(url)
                return val if isinstance(val, bytes) else json.dumps(val).encode("utf-8")
        raise IOError("HTTP 404")


def cwa_snapshot_stations():
    st, _ = K.parse_cwa_wind(fx("cwa_wind.json"))
    return {k: {"id": "cwa:" + k, "name": v["name"], "lat": v["lat"], "lon": v["lon"], "kind": "met", "src": "cwa",
                "tz": "Asia/Taipei"} for k, v in st.items()}


def test_cwa_wind_feed_reads_with_the_key_and_keeps_the_day(clock):
    clock.t = K.cwa_time("2026-11-01T02:00:00+08:00")
    f = FakeFetch({"O-A0003-001": fx("cwa_wind.json")})
    p = K.CwaWindProvider(cwa_snapshot_stations(), f, SECRET)
    lst = p.list_stations()
    assert {e["id"] for e in lst} >= {"cwa:466940"} and "Authorization=" + SECRET in f.calls[0][0]
    assert p.hist("466940", 0) and p.status()["last_error"] is None


def test_a_failing_cwa_request_never_puts_the_key_in_status_or_logs(clock, caplog):
    url = K.CWA_API.format(id=K.CWA_WIND, key=SECRET)
    f = FakeFetch({"O-A0003-001": IOError("Max retries exceeded with url: %s" % url)})
    p = K.CwaWindProvider(cwa_snapshot_stations(), f, SECRET)
    p.list_stations()
    st = json.dumps(p.status())
    assert SECRET not in st and "Authorization=[key]" in st
    assert SECRET not in caplog.text


# ============================================================================================== Frost

FROST_SOURCES = {"data": [
    {"id": "SN18700", "name": "OSLO - BLINDERN", "geometry": {"@type": "Point", "coordinates": [10.72, 59.9423]}},
    {"id": "SN47300", "name": "UTSIRA FYR", "geometry": {"coordinates": [4.8817, 59.3065]}},
    {"id": "SN62480", "name": "RØROS", "geometry": {"coordinates": [11.3837, 62.5773]}},          # inland: out
    {"id": "SN99840", "name": "SVALBARD LUFTHAVN", "geometry": {"coordinates": [15.5015, 78.2453]}},
    {"id": "bad", "name": "X", "geometry": {"coordinates": [1, 1]}}]}


def frost_obs(ids, t="2026-11-01T01:40:00.000Z", gust=True):
    out = []
    for i, sid in enumerate(ids):
        obs = [{"elementId": "wind_speed", "value": 3.0 + i, "unit": "m/s"},
               {"elementId": "wind_from_direction", "value": 200 + i, "unit": "degrees"}]
        if gust:
            obs.append({"elementId": K.FROST_GUST, "value": 9.5, "unit": "m/s"})
        out.append({"sourceId": sid + ":0", "referenceTime": t, "observations": obs})
    return {"data": out}


def mask():
    return K.CoastMask.load(K.MASK_PATH)


def test_the_committed_coast_mask_keeps_the_coast_and_drops_the_inland():
    m = mask()
    assert m.keep(59.3065, 4.8817) and m.keep(60.38, 5.33) and m.keep(78.2453, 15.5015)    # Utsira, Bergen, Svalbard
    assert not m.keep(62.5773, 11.3837) and not m.keep(69.0, 23.0)                       # Roros, Kautokeino
    assert not m.keep(10.0, 10.0)                                                        # outside the mask's box


def test_frost_names_and_sources():
    assert K.frost_name("OSLO - BLINDERN") == "Oslo - Blindern" and K.frost_name("Utsira Fyr") == "Utsira Fyr"
    st = K.parse_frost_sources(FROST_SOURCES)
    assert set(st) == {"SN18700", "SN47300", "SN62480", "SN99840"} and st["SN47300"]["lat"] == 59.3065


def test_frost_feed_keeps_the_coastal_sources_and_reads_them_in_batches(clock, monkeypatch):
    clock.t = 1793497200 + 300                                          # five minutes after the readings
    monkeypatch.setattr(K, "FROST_BATCH", 2)
    seen = []

    def obs(url):
        ids = url.split("sources=", 1)[1].split("&", 1)[0].split(",")
        seen.append(ids)
        return json.dumps(frost_obs(ids)).encode()
    f = FakeFetch({"/sources/": FROST_SOURCES, "/observations/": obs})
    p = K.FrostProvider(f, SECRET, mask())
    lst = p.list_stations()
    ids = {e["id"] for e in lst}
    assert ids == {"frost:SN18700", "frost:SN47300", "frost:SN99840"}, "Roros is inland"
    assert [len(x) for x in seen] == [2, 1]
    assert all(c[1]["Authorization"].startswith("Basic ") for c in f.calls)
    assert SECRET not in json.dumps(f.calls[0][0])                       # never in a URL
    e = next(x for x in lst if x["id"] == "frost:SN47300")
    assert e["wind"] == [4.0, 9.5, 201]
    v, extra = p.extra_stations()
    assert v == 1 and {s["id"] for s in extra} == ids and next(s for s in extra if s["id"] == "frost:SN99840")["tz"] == \
        "Arctic/Longyearbyen"
    p.list_stations()                                                    # same sources: no new version
    assert p.extra_stations()[0] == 1


def test_frost_drops_the_gust_element_when_refused(clock):
    clock.t = 1793497200 + 300
    calls = []

    def obs(url):
        calls.append(url)
        if "wind_speed_of_gust" in url:
            raise IOError("HTTP 400")
        ids = url.split("sources=", 1)[1].split("&", 1)[0].split(",")
        return json.dumps(frost_obs(ids, gust=False)).encode()
    p = K.FrostProvider(FakeFetch({"/sources/": FROST_SOURCES, "/observations/": obs}), SECRET, mask())
    lst = p.list_stations()
    assert lst and all(e["wind"][1] is None for e in lst) and p._gust is None


def test_frost_history_asks_the_window_and_never_shows_the_key():
    now = 1793469600
    f = FakeFetch({"/observations/": frost_obs(["SN47300"], t="2026-11-01T00:00:00Z")})
    rows = K.history("frost", "SN47300", now - 86400, now, f, {"frost": SECRET})
    assert rows and rows[0]["s"] == 3.0 and "referencetime=" in f.calls[0][0]
    bad = FakeFetch({"/observations/": IOError("401 for Basic %s" % SECRET)})
    with pytest.raises(W.WindError) as e:
        K.history("frost", "SN47300", now - 86400, now, bad, {"frost": SECRET})
    assert SECRET not in str(e.value)
    assert K.history("frost", "SN47300", 0, now, f, {}) is None          # no key: no window


# ============================================================================================== KNMI

KNMI_T = 1793491200                                     # 2026-11-01T00:00Z


def knmi_hdf5(t=KNMI_T, fill=-9999.0):
    import h5py
    buf = io.BytesIO()
    with h5py.File(buf, "w") as h:
        h["station"] = np.array([b"06201", b"06260", b"06310"])
        h["stationname"] = np.array([b"D15-FA-1", b"DE BILT AWS", b"VLISSINGEN AWS"])
        h["lat"] = np.array([54.3256, 52.0989, 51.4414])
        h["lon"] = np.array([2.9358, 5.1797, 3.5958])
        tm = h.create_dataset("time", data=np.array([float(t + 631152000)]))
        tm.attrs["units"] = b"seconds since 1950-01-01 00:00:00"
        for name, vals in (("ff", [12.3, 3.1, fill]), ("dd", [250.0, 180.0, 90.0]), ("gff", [16.0, fill, 5.0]),
                           ("ta", [11.0, 9.5, 10.2]), ("rh", [88.0, 95.0, 90.0]), ("pp", [1003.1, 1004.0, fill])):
            d = h.create_dataset(name, data=np.array(vals).reshape(3, 1))
            d.attrs["_FillValue"] = fill
    return buf.getvalue()


def cdf_file():
    """A minimal CDF-1 file: dims station=2, strlen=6, time=unlimited (1 record); vars station(char), lat, lon (fixed),
    time and ff (record variables)."""
    def s32(x): return struct.pack(">i", x)
    def name(s):
        b = s.encode()
        return s32(len(b)) + b + b"\0" * (-len(b) % 4)
    dims = [("station", 2), ("strlen", 6), ("time", 0)]

    def att(units):
        b = units.encode()
        return s32(12) + s32(1) + name("units") + s32(2) + s32(len(b)) + b + b"\0" * (-len(b) % 4)
    no_att = s32(0) + s32(0)
    station = b"06201\0" + b"06260\0"
    fixed = [("station", [0, 1], 2, station, no_att), ("lat", [0], 6, struct.pack(">2d", 54.3, 52.1), no_att),
             ("lon", [0], 6, struct.pack(">2d", 2.9, 5.2), no_att)]
    recs = [("time", [2], 6, struct.pack(">d", 2393971200.0), att("seconds since 1950-01-01 00:00:00")),
            ("ff", [2, 0], 5, struct.pack(">2f", 7.5, 2.5), no_att)]

    def header(begins):
        h = b"CDF\x01" + s32(1) + s32(10) + s32(len(dims)) + b"".join(name(n) + s32(v) for n, v in dims)
        h += no_att + s32(11) + s32(len(fixed) + len(recs))
        for (nm, ids, t, data, a), b0 in zip(fixed + recs, begins):
            pad = len(data) + (-len(data) % 4)
            h += name(nm) + s32(len(ids)) + b"".join(s32(i) for i in ids) + a + s32(t) + s32(pad) + s32(b0)
        return h
    size = len(header([0] * 5))
    begins, pos = [], size
    for _, _, _, data, _ in fixed:
        begins.append(pos)
        pos += len(data) + (-len(data) % 4)
    rec0 = pos
    for _, _, _, data, _ in recs:
        begins.append(pos)
        pos += len(data) + (-len(data) % 4)
    out = header(begins)
    for _, _, _, data, _ in fixed + recs:
        out += data + b"\0" * (-len(data) % 4)
    assert begins[3] == rec0
    return out


def test_knmi_reader_reads_a_netcdf4_file():
    st, obs = K.parse_knmi(knmi_hdf5(), "KMDS__OPER_P___10M_OBS_L2_202610310000.nc")
    assert st["06260"] == {"name": "DE BILT AWS", "lat": 52.0989, "lon": 5.1797}
    r = obs["06201"][0]
    assert r["t"] == KNMI_T and r["s"] == 12.3 and r["g"] == 16.0 and r["d"] == 250
    assert r["at"] == 11.0 and r["rh"] == 88 and r["p"] == 1003.1
    assert obs["06260"][0]["g"] is None                         # the fill value is no gust
    assert "06310" not in obs and "06310" in st                 # no speed: a station, no reading


def test_knmi_reader_takes_a_fill_value_inside_the_plausible_range_as_missing():
    st, obs = K.parse_knmi(knmi_hdf5(fill=5.0), "KMDS__OPER_P___10M_OBS_L2_202610310000.nc")
    assert "06310" not in obs and "06310" in st                 # speed 5.0 = the fill value: no reading
    assert obs["06260"][0]["g"] is None and obs["06201"][0]["s"] == 12.3


def test_knmi_columns_take_the_newest_time_on_either_axis():
    a = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])            # [time, station]: 3 stations
    assert K._col(a, 1, 3) == 5.0 and K._col(a.T, 1, 3) == 5.0   # [station, time] the same reading
    assert K._col(np.array([7.0, np.nan]), 1, 2) is None and K._col(np.array(3.0), 0, 1) == 3.0


def test_knmi_reader_reads_a_classic_netcdf_file():
    v = K.read_netcdf(cdf_file())
    assert list(v["station"][0]) == ["06201", "06260"] and v["time"][1]["units"].startswith("seconds since")
    st, obs = K.parse_knmi(cdf_file())
    assert obs["06201"][0]["s"] == 7.5 and obs["06260"][0]["s"] == 2.5 and st["06201"]["lat"] == 54.3


def test_knmi_reader_names_what_it_misses():
    import h5py
    buf = io.BytesIO()
    with h5py.File(buf, "w") as h:
        h["windspeed"] = np.array([1.0])
    with pytest.raises(K.NcError) as e:
        K.parse_knmi(buf.getvalue())
    assert "windspeed" in str(e.value) and "id" in str(e.value)
    with pytest.raises(K.NcError):
        K.read_netcdf(b"<html>not a file</html>")


def test_knmi_feed_reads_the_newest_file_then_back_fills_and_finds_its_stations(clock, monkeypatch):
    base = KNMI_T
    clock.t = base + 120
    names = ["KMDS__OPER_P___10M_OBS_L2_%s.nc" % __import__("time").strftime("%Y%m%d%H%M", __import__("time").gmtime(base - 600 * i))
             for i in range(20)]
    monkeypatch.setattr(K, "KNMI_BACKFILL", 3)
    files = {n: knmi_hdf5() for n in names}
    signed = {}

    def url_for(u):
        name = u.split("/files/", 1)[1].split("/url", 1)[0]
        signed[name] = "https://s3.example/%s?X-Amz-Signature=sig" % name
        return json.dumps({"temporaryDownloadUrl": signed[name]}).encode()
    f = FakeFetch({"/files?": {"files": [{"filename": n} for n in names]}, "/url": url_for,
                   "s3.example": lambda u: files[u.split("/")[-1].split("?")[0]]})
    p = K.KnmiProvider(f, SECRET)
    lst = p.list_stations()
    assert {e["id"] for e in lst} == {"knmi:06201", "knmi:06260"}
    assert len(p._read) == 4 and names[0] in p._read                 # the newest and three older ones
    api = [c for c in f.calls if "dataplatform" in c[0]]
    assert all(c[1]["Authorization"] == SECRET for c in api)
    assert all("Authorization" not in c[1] for c in f.calls if "s3.example" in c[0])
    assert p.filling() and p.extra_stations()[0] == 1
    assert {s["id"] for s in p.extra_stations()[1]} == {"knmi:06201", "knmi:06260", "knmi:06310"}


def test_knmi_errors_never_show_the_signed_address(clock):
    clock.t = 1793469600
    f = FakeFetch({"/files?": {"files": [{"filename": "KMDS__OPER_P___10M_OBS_L2_202611010000.nc"}]},
                   "/url": {"temporaryDownloadUrl": "https://s3.example/f.nc?X-Amz-Signature=SECRETSIG"},
                   "s3.example": IOError("failed https://s3.example/f.nc?X-Amz-Signature=SECRETSIG")})
    p = K.KnmiProvider(f, SECRET)
    p.list_stations()
    s = json.dumps(p.status())
    assert "SECRETSIG" not in s and SECRET not in s and "every KNMI file failed" in s


# ============================================================================================== the site's station list

class FakeDiscovered(K.DiscoveredProvider):
    source, key, tz = "FROST", "frost", "Europe/Oslo"

    def _readings(self):
        return {}


def test_found_stations_join_the_site_list_and_the_history_route(monkeypatch):
    base, doc = {"ndbc:51003": {"id": "ndbc:51003", "name": "Buoy", "lat": 19.1, "lon": -160.6, "kind": "buoy",
                                "src": "ndbc", "tz": "Pacific/Honolulu", "alias": None, "wx": None, "net": None}}, None
    doc = {"captured": "x", "source": "test", "fields": ["id", "name", "lat", "lon", "kind", "src", "tz", "alias", "wx", "net"],
           "stations": [["ndbc:51003", "Buoy", 19.1, -160.6, "buoy", "ndbc", "Pacific/Honolulu", None, None, None]]}
    p = FakeDiscovered(lambda *a, **k: b"", SECRET)
    p._set_stations({"SN47300": {"name": "Utsira Fyr", "lat": 59.3, "lon": 4.9}})
    svc = W.WindHistory(dict(base), lambda *a, **k: b"")
    w = {"svc": svc, "list": A._json_payload_and_etag(W.client_list(doc)), "providers": [p], "stations": dict(base),
         "base_stations": base, "base_doc": doc, "extra_key": None}
    monkeypatch.setattr(A, "WIND_ENABLED", True)
    monkeypatch.setattr(A, "_WINDS", w)
    c = A.app.test_client()
    r = c.get("/api/wind/stations").get_json()
    assert ["frost:SN47300", "Utsira Fyr", 59.3, 4.9, "met", "frost", "Europe/Oslo", None, None, None] in r["stations"]
    assert "frost:SN47300" in w["stations"] and svc.stations is w["stations"]
    assert A._wind_sync_extras(w) is False                           # unchanged: nothing rebuilt
    p._set_stations({"SN47300": {"name": "Utsira Fyr", "lat": 59.3, "lon": 4.9},
                     "SN18700": {"name": "Oslo", "lat": 59.9, "lon": 10.7}})
    assert A._wind_sync_extras(w) is True and "frost:SN18700" in w["stations"]
    assert c.get("/api/wind/frost:SN99999/history").status_code == 404


def test_the_keyed_providers_are_built_only_with_their_keys():
    groups = {"cwa": {k: v for k, v in cwa_snapshot_stations().items()}}
    assert K.make_providers(groups, lambda *a: b"", {}) == []
    out = K.make_providers(groups, lambda *a: b"", {"knmi": "a", "frost": "b", "cwa": "c"})
    assert [p.source for p in out] == ["KNMI", "FROST", "CWA"]
    assert K.make_providers({}, lambda *a: b"", {"cwa": "c"}) == []   # no CWA station in the snapshot: no feed
    for p in out:
        assert p.attribution_text and "c" != p.attribution_text


def test_ids_and_attribution_cover_the_new_sources():
    for sid in ("knmi:06260", "knmi:0-20000-0-06260", "frost:SN47300", "cwa:466940", "cwa:C0A520"):
        assert W.valid_id(sid), sid
    assert not W.valid_id("frost:sn47300") and not W.valid_id("knmi:")
    for src in W.KEYED_SOURCES:
        assert W.ATTRIBUTION[src] and W.LICENCE_URL[src]


def test_snapshot_holds_the_coastal_cwa_stations():
    stations, _ = W.load_stations(A.WIND_STATIONS_PATH)
    cwa = [s for k, s in stations.items() if k.startswith("cwa:")]
    assert 200 <= len(cwa) <= 330 and all(s["tz"] == "Asia/Taipei" and s["kind"] == "met" for s in cwa)
    assert not any(k.startswith(("knmi:", "frost:")) for k in stations)   # found at run time, never in the snapshot


# ============================================================================================== CCO buoys

def cco_doc(rows):
    return {"type": "FeatureCollection", "features": [
        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]},
         "properties": dict({"sensor": name, "date": date, "hs": 1.2, "hmax": 2.1, "tp": 11.1, "tz": 6.0, "pdir": 205,
                             "spread": 28, "sst": 14.2, "QCflag": 0}, **extra)}
        for name, lat, lon, date, extra in rows]}


class Resp:
    def __init__(self, status, doc):
        self.status_code = status
        self._doc = doc
        self.content = json.dumps(doc).encode()

    def json(self):
        return self._doc


class FakeHttp:
    def __init__(self, answer):
        self.answer = answer
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append((url, dict(headers or {})))
        a = self.answer(url) if callable(self.answer) else self.answer
        if isinstance(a, Exception):
            raise a
        return a


def test_cco_parser_reads_the_features_and_applies_the_quality_flag():
    out = B.parse_cco_features(cco_doc([("Dawlish", 50.57, -3.43, "20261031#120000", {}),
                                        ("Bad Tp", 50.1, -5.0, "2026-10-31T12:00:00Z", {"QCflag": 2}),
                                        ("Bad Hs", 50.2, -5.1, "20261031#120000", {"QCflag": 9})]))
    d = out[0][4]
    assert out[0][:4] == ("Dawlish", "Dawlish", 50.57, -3.43) and d["time_utc"] == "2026-10-31T12:00:00Z"
    assert d["hs_m"] == 1.2 and d["tp_s"] == 11.1 and d["mean_period_s"] == 6.0 and d["dir_deg"] == 205
    assert out[1][4]["tp_s"] is None and out[1][4]["hs_m"] == 1.2 and out[1][4]["dir_deg"] is None
    assert out[2][4]["hs_m"] is None and out[2][4]["hmax_m"] is None and out[2][4]["sst_c"] is None
    assert B._cco_time("2026-10-31T13:00:00+01:00") == "2026-10-31T12:00:00Z" and B._cco_time("x") is None


def test_cco_feed_sends_the_key_in_a_header_with_the_site_as_referer(clock):
    clock.t = 1793450000
    http = FakeHttp(lambda u: Resp(200, cco_doc([("Dawlish", 50.57, -3.43, "20261031#120000", {}),
                                                 ("Bad Hs", 50.2, -5.1, "20261031#120000", {"QCflag": 9})])))
    p = B.CcoProvider(SECRET, http=http)
    lst = p.list_stations()
    assert [e["id"] for e in lst] == ["cco:Dawlish"]
    url, headers = http.calls[0]
    assert SECRET not in url and headers["X-API-Key"] == SECRET and headers["Referer"] == "https://allshoresurf.com/"
    d = p.detail("Dawlish")
    assert d["latest"]["hs_m"] == 1.2 and "sensor=Dawlish" in http.calls[-1][0] and "duration=24" in http.calls[-1][0]
    assert p.attribution_text.startswith("Real time data displayed on this page are from the Regional Coastal")


def test_cco_window_keeps_only_its_own_sensor(clock):
    clock.t = 1793450000
    def answer(u):
        if "sensor=" in u:
            return Resp(200, cco_doc([("Dawlish", 50.57, -3.43, "20261031#110000", {"hs": 1.0}),
                                      ("Dawlish", 50.57, -3.43, "20261031#120000", {}),
                                      ("Chesil", 50.6, -2.5, "20261031#123000", {"hs": 3.3})]))
        return Resp(200, cco_doc([("Dawlish", 50.57, -3.43, "20261031#120000", {}),
                                  ("Chesil", 50.6, -2.5, "20261031#120000", {"hs": 3.0})]))
    p = B.CcoProvider(SECRET, http=FakeHttp(answer))
    p.list_stations()
    d = p.detail("Dawlish")
    assert d["latest"]["hs_m"] == 1.2 and [r["hs_m"] for r in d["recent"]] == [1.2, 1.0]


def test_cco_failure_keeps_the_key_out_of_status(clock):
    clock.t = 1793450000
    http = FakeHttp(lambda u: IOError("401 Missing or unknown API key %s" % SECRET))
    p = B.CcoProvider(SECRET, http=http)
    p.list_stations()
    assert SECRET not in json.dumps(p.status()) and p.status()["last_error"]


# ============================================================================================== CWA buoys

def test_cwa_buoy_parsers_read_the_station_list_and_the_48_hours():
    st = B.parse_cwa_buoy_stations(fx("cwa_buoy_stations.json"))
    assert set(st) == {"46694A", "46761F"}, "the tide gauge has no wave sensor"
    assert st["46761F"]["name"] == "Chenggong Wave Station"
    obs = B.parse_cwa_buoy_obs(fx("cwa_buoy_obs.json"))
    rows = obs["46694A"]
    assert rows == sorted(rows, key=lambda o: o["time_utc"]) and len(rows) >= 20
    newest = rows[-1]
    assert newest["time_utc"] == "2026-10-31T17:00:00Z" and newest["hs_m"] == 1.3 and newest["mean_period_s"] == 5.1
    assert newest["tp_s"] is None and newest["dir_deg"] == 78.0 and newest["wind_speed_ms"] == 4.4


def test_cwa_buoy_feed_serves_the_list_and_the_windows_from_one_answer(clock):
    clock.t = 1793469600

    def ans(url):
        return Resp(200, fx("cwa_buoy_stations.json") if "O-B0076-001" in url else fx("cwa_buoy_obs.json"))
    http = FakeHttp(ans)
    p = B.CwaBuoyProvider(SECRET, http=http)
    lst = p.list_stations()
    assert {e["id"] for e in lst} == {"cwa:46694A", "cwa:46761F"}
    n = len(http.calls)
    d = p.detail("46694A")
    assert d["latest"]["hs_m"] == 1.3 and d["recent"] and len(http.calls) == n      # no new request for a window


def test_cwa_buoy_failure_redacts_the_key(clock):
    clock.t = 1793469600
    url = B.CwaBuoyProvider.API % ("O-B0076-001", SECRET)
    p = B.CwaBuoyProvider(SECRET, http=FakeHttp(lambda u: IOError("Max retries exceeded with url: " + url)))
    p.list_stations()
    s = json.dumps(p.status())
    assert SECRET not in s and "Authorization=[key]" in s


def test_dedup_prefers_cco_over_cefas_and_cwa_sits_with_the_national_feeds():
    assert B._station_priority({"source": "CCO"}) > B._station_priority({"source": "CEFAS"})
    assert B._station_priority({"source": "CWA"}) == 60


def test_the_buoy_feeds_are_built_only_with_their_keys(monkeypatch):
    monkeypatch.setattr(A, "_BUOY_PROVIDERS", None)
    monkeypatch.delenv("CCO_API_KEY", raising=False)
    monkeypatch.delenv("CWA_API_KEY", raising=False)
    assert not {p.source for p in A._build_buoy_providers()} & {"CCO", "CWA"}
    monkeypatch.setattr(A, "_BUOY_PROVIDERS", None)
    monkeypatch.setenv("CCO_API_KEY", "a")
    monkeypatch.setenv("CWA_API_KEY", "b")
    assert {"CCO", "CWA"} <= {p.source for p in A._build_buoy_providers()}
    monkeypatch.setattr(A, "_BUOY_PROVIDERS", None)
