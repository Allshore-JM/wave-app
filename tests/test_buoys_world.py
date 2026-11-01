"""Plan section 72: the CMEMS feed widened (Indian Ocean, Lesser Antilles / Guyane, Canaries; VGHS buoys; drifters;
two days of a platform; impossible peak periods), Brazil's SiMCosta and Korea's KMA (WMO WIS2), on real answers
captured once by tests/capture_buoys_world_fixtures.py (2026-10-31)."""
import copy
import json
import os
import sys
from datetime import datetime, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import buoy_sources as B  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "buoys_world")
NOW = datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc).timestamp()     # just after the capture


class _Resp:
    def __init__(self, body, status=200):
        self.content = body if isinstance(body, bytes) else body.encode("utf-8")
        self.status_code = status
        self.headers = {}

    def iter_lines(self, decode_unicode=False):
        for ln in self.content.decode("utf-8").splitlines():
            yield ln

    def json(self):
        return json.loads(self.content.decode("utf-8"))

    def close(self):
        pass


class FileHTTP:
    """Answers each URL from a fixture file chosen by `route(url)`; None = 404."""
    def __init__(self, route):
        self.route, self.calls = route, []

    def get(self, url, **kw):
        self.calls.append(url)
        name = self.route(url)
        if name is None:
            return _Resp(b"<Error>NoSuchKey</Error>", 404)
        if isinstance(name, Exception):
            raise name
        if isinstance(name, bytes):
            return _Resp(name)
        return _Resp(open(os.path.join(FIX, name), "rb").read())


class Clock:
    def __init__(self, now=NOW):
        self.now = now

    def time(self):
        return self.now


@pytest.fixture(autouse=True)
def _clock(monkeypatch):
    monkeypatch.setattr(B, "time", Clock())


def _cmems_route(url):
    if url.endswith("index_latest.txt"):
        return "cmems_index.txt"
    name = url.rsplit("/", 1)[-1]
    return name if os.path.exists(os.path.join(FIX, name)) else None


# ----------------------------------------------------------------------------------------------- CMEMS

def test_cmems_boxes_vghs_drifters_and_saildrones():
    p = B.CopernicusProvider(http=FileHTTP(_cmems_route))
    ids = {s["local_id"]: s for s in p._fetch_stations()}
    assert "GL_TS_MO_2300453" in ids                    # NIOT, Indian Ocean, VGHS only
    assert "GL_TS_DB_7810896" in ids                    # INCOIS / NIO drifter
    assert ids["GL_TS_DB_7810896"]["name"] == "Drifting buoy 7810896"
    assert "GL_TS_MO_6100217" in ids                    # Italy's RON, Europe, VGHS only
    assert "GL_TS_MO_6200064" in ids                    # a national VHM0 platform still listed
    assert "GL_TS_SD_1801600" not in ids                # a Saildrone (Caribbean box): left out
    assert "GL_TS_MO_52215" not in ids                  # Micronesia: outside every box


def test_cmems_index_filter_needs_a_wave_parameter():
    rows = [ln for ln in open(os.path.join(FIX, "cmems_index.txt"), encoding="utf-8") if ln.startswith("#")]
    real = [ln for ln in open(os.path.join(FIX, "cmems_index.txt"), encoding="utf-8")
            if "GL_TS_MO_2300453" in ln][-1]
    stripped = real.replace(" VGHS", "").replace(" VTPK", "")
    body = "".join(rows) + stripped
    p = B.CopernicusProvider(http=FileHTTP(lambda u: body.encode("utf-8")))
    assert [s["local_id"] for s in p._fetch_stations()] == []


def test_cmems_drifter_keeps_its_newest_position(monkeypatch):
    """A moored platform's newest position far from two agreeing earlier ones is an outlier; a drifter's is not."""
    lines = open(os.path.join(FIX, "cmems_index.txt"), encoding="utf-8").read().splitlines()
    head = [ln for ln in lines if ln.startswith("#")]
    base = [ln for ln in lines if "GL_TS_DB_7810896" in ln]
    base.sort(key=lambda ln: ln.split(",")[7])
    newest = base[-1].split(",")
    # two older single-position files at one place, the newest 50 km away
    def row(day, la, lo, t):
        r = list(newest)
        r[1] = r[1].rsplit("/", 2)[0] + "/%s/GL_TS_DB_7810896_%s.nc" % (day, day)
        r[2] = r[3] = str(la)
        r[4] = r[5] = str(lo)
        r[6] = r[7] = t
        return ",".join(r)
    rows = [row("20261029", 6.0, 76.0, "2026-10-29T12:00:00Z"), row("20261030", 6.0, 76.0, "2026-10-30T12:00:00Z"),
            row("20261031", 6.45, 76.0, "2026-10-31T20:00:00Z")]
    body = "\n".join(head + rows) + "\n"
    st = B.CopernicusProvider(http=FileHTTP(lambda u: body.encode("utf-8")))._fetch_stations()
    assert len(st) == 1 and abs(st[0]["lat"] - 6.45) < 1e-6        # moved: taken as it is
    moored = body.replace("GL_TS_DB_7810896", "GL_TS_MO_7810896")
    st = B.CopernicusProvider(http=FileHTTP(lambda u: moored.encode("utf-8")))._fetch_stations()
    assert abs(st[0]["lat"] - 6.0) < 1e-6                          # moored: the earlier agreeing position


def test_cmems_detail_reads_the_relays_depth_column_and_the_day_before():
    p = B.CopernicusProvider(http=FileHTTP(_cmems_route))
    p._fetch_stations()
    d = p.detail("GL_TS_MO_2300453")
    newest = d["latest"]
    assert newest["hs_m"] == pytest.approx(0.99, abs=0.005)        # VGHS in the DEPTH column 1 (DEPH 0)
    assert newest["tp_s"] == pytest.approx(13.3, abs=0.05)
    assert newest["dir_deg"] == pytest.approx(106.0) and newest["dir_kind"] == "from"
    assert newest["mean_period_s"] == pytest.approx(11.1, abs=0.05)
    days = {o["time_utc"][:10] for o in d["recent"]}
    assert days == {"2026-10-30", "2026-10-31"} or "2026-10-31" in days
    files = [u.rsplit("/", 1)[-1] for u in p.http.calls if u.endswith(".nc") and "2300453" in u]
    assert files == ["GL_TS_MO_2300453_20261031.nc", "GL_TS_MO_2300453_20261030.nc"]


def test_cmems_detail_without_the_day_before_still_answers():
    def route(url):
        return None if url.endswith("_20261030.nc") else _cmems_route(url)
    p = B.CopernicusProvider(http=FileHTTP(route))
    p._fetch_stations()
    d = p.detail("GL_TS_MO_2300453")
    assert d["latest"] is not None and all(o["time_utc"].startswith("2026-10-31") for o in d["recent"])


def test_cmems_meteo_france_relay_peak_period_dropped():
    p = B.CopernicusProvider(http=FileHTTP(_cmems_route))
    p._fetch_stations()
    rows = p.detail("GL_TS_MO_6200001")["recent"]
    assert rows and all(o["tp_s"] is None for o in rows)            # VTPK 0-2 s beside a 10 s mean
    assert all(o["mean_period_s"] for o in rows)


def test_cmems_prev_day_name():
    f = "X/latest/20261101/GL_TS_MO_1_20261101.nc"
    assert B._cmems_prev_day(f) == "X/latest/20261031/GL_TS_MO_1_20261031.nc"
    assert B._cmems_prev_day("X/latest/20260301/A_20260301.nc") == "X/latest/20260228/A_20260228.nc"
    assert B._cmems_prev_day("X/latest/20261101/A_20261031.nc") is None
    assert B._cmems_prev_day("X/monthly/A_202611.nc") is None


@pytest.mark.parametrize("tp,tm,want", [
    (2.0, 10.0, None),      # Meteo-France relay beside a 10 s mean (the ratio)
    (1.0, 4.0, None),
    (0.9, None, None),      # below 1 s
    (1.9, None, 1.9),       # a calm Bothnian Sea chop (G46 F2: the floor was 2 s)
    (2.1, 2.1, 2.1),        # Italy RON in a calm
    (3.7, 3.9, 3.7),        # a little below the mean: two systems
    (18.2, 3.8, 18.2),      # long swell under a small wind sea
    (25.0, 4.9, None),      # KMA 2200307
    (17.4, 9.8, 17.4),
    (12.0, None, 12.0),
    (None, 5.0, None),
])
def test_peak_ok(tp, tm, want):
    assert B._peak_ok(tp, tm) == want


# ----------------------------------------------------------------------------------------------- SiMCosta

def _sim_route(url):
    if url.endswith("/api/equipamentos/"):
        return "simcosta_list.json"
    if "boiaID=12&" in url:
        return "simcosta_12.json"
    if "boiaID=518&" in url:
        return "simcosta_518.json"
    return b"[]"                                       # the others: no wave height today


def test_simcosta_platform_filter():
    doc = json.load(open(os.path.join(FIX, "simcosta_list.json"), encoding="utf-8"))
    st = B.parse_simcosta_stations(doc, NOW, B.SimcostaProvider.LIVE_S)
    names = {v["name"].split(" (")[0] for v in st.values()}
    assert {"RJ-4", "CDRJ-1", "RS-4"} <= names
    assert not any(n.upper().endswith(("_OLD", "_INT")) or n.upper().startswith("ADCP") for n in names)
    assert "SC-1" not in names                          # status 3 (not operating)
    assert st[518]["name"] == "CDRJ-1 (Ilha do Pai, Niterói)"
    # an operating, fresh retired copy is still left out (old and new ids return the same readings)
    fresh = {"boia_status": 1, "atualizacao": "2026-10-31T22:00:00.000Z", "latitude": -22.9, "longitude": -43.1}
    extra = [dict(fresh, id=9001, boia="RJ-9_OLD"), dict(fresh, id=9002, boia="RJ-9_INT"),
             dict(fresh, id=9003, boia="ADCP RJ-9"), dict(fresh, id=9004, boia="RJ-9")]
    got = B.parse_simcosta_stations(extra, NOW, B.SimcostaProvider.LIVE_S)
    assert set(got) == {9004}
    with pytest.raises(ValueError):
        B.parse_simcosta_stations({"x": 1}, NOW, 1)


def test_simcosta_provider_lists_only_platforms_with_waves():
    p = B.SimcostaProvider(http=FileHTTP(_sim_route))
    st = {s["local_id"]: s for s in p._fetch_stations()}
    assert set(st) == {"12", "518"}
    rj = p.detail("12")["latest"]
    assert rj["hs_m"] > 0 and rj["tp_s"] > 0 and rj["dir_kind"] == "from" and "partitions" not in rj
    cd = p.detail("518")
    o = cd["latest"]
    labels = sorted(q["label"] for q in o["partitions"])
    assert labels == ["Swell", "Wind sea"]
    big = max(o["partitions"], key=lambda q: q["hs_m"])
    assert o["tp_s"] == big["period_s"]                 # no total Tp: the higher part's
    assert len(cd["recent"]) > 20


def test_simcosta_one_platform_failing_keeps_what_it_had():
    p = B.SimcostaProvider(http=FileHTTP(_sim_route))
    p._fetch_stations()
    held = p._obs["518"]

    def route(url):
        return RuntimeError("down") if "boiaID=518&" in url else _sim_route(url)
    p.http = FileHTTP(route)
    st = {s["local_id"] for s in p._fetch_stations()}
    assert st == {"12", "518"} and p._obs["518"] == held


def test_simcosta_nothing_answers_raises():
    p = B.SimcostaProvider(http=FileHTTP(lambda u: "simcosta_list.json" if u.endswith("/") else b"[]"))
    with pytest.raises(RuntimeError):
        p._fetch_stations()
    p = B.SimcostaProvider(http=FileHTTP(lambda u: b"<html>"))
    with pytest.raises(RuntimeError):
        p._fetch_stations()


def test_simcosta_obs_rules():
    doc = [{"timestamp": "2026-10-31T22:21:40.000Z", "Hsig": 1.3, "Tp": 9.0, "Avg_Wv_Dir_N": 170},
           {"timestamp": "2026-10-31T22:51:40.000Z", "Hsig": None, "Tp": 9.0},                 # no height
           {"timestamp": "2026-11-01T03:00:00.000Z", "Hsig": 1.0},                            # 3 h ahead
           {"timestamp": "bad", "Hsig": 1.0},
           {"timestamp": "2026-10-31T21:51:40.000Z", "Hsig": 99.0},                           # impossible
           {"timestamp": "2026-10-31T20:00:00.000Z", "HM0": 1.2, "Hsig_sea": 1.0, "Tp_sea": 7.0,
            "Hsig_swell": 0.5, "Tp_swell": 12.0}]
    rows = B.parse_simcosta_obs(doc, NOW)
    assert [r["time_utc"] for r in rows] == ["2026-10-31T20:00:00Z", "2026-10-31T22:21:40Z"]
    assert rows[0]["tp_s"] == 7.0 and rows[1]["tp_s"] == 9.0


# ----------------------------------------------------------------------------------------------- KMA (WIS2)

def _kma_route(url):
    for var in B.WIS2_WAVE_VARS:
        if "name=%s&" % var in url:
            return "kma_%s.json" % var
    return None


def test_kma_provider_on_real_answers():
    p = B.KmaBuoyProvider(http=FileHTTP(_kma_route))
    st = {s["local_id"]: s for s in p._fetch_stations()}
    assert set(st) == {"0-410-0-21229", "0-22000-0-2200307", "0-410-0-22101"}
    assert st["0-410-0-22101"]["name"] == "KMA buoy 22101"
    o = p.detail("0-410-0-21229")["latest"]
    assert o["hs_m"] > 0 and o["dir_kind"] == "from" and o["mean_period_s"] > 0
    assert o["sst_c"] is not None and 0 < o["sst_c"] < 35        # an interval phenomenonTime: joined by reportTime
    assert all(r["tp_s"] is None or r["tp_s"] <= 5 * r["mean_period_s"]
               for r in p._obs["0-22000-0-2200307"] if r["mean_period_s"])
    assert len(p.detail("0-410-0-22101")["recent"]) >= 20
    assert len(p.http.calls) == len(B.WIS2_WAVE_VARS)


def test_kma_heights_failing_raises_others_failing_do_not():
    p = B.KmaBuoyProvider(http=FileHTTP(lambda u: None if "significant" in u else _kma_route(u)))
    with pytest.raises(RuntimeError):
        p._fetch_stations()
    p = B.KmaBuoyProvider(http=FileHTTP(lambda u: _kma_route(u) if "significant" in u else None))
    st = p._fetch_stations()
    assert len(st) == 3 and p.detail(st[0]["local_id"])["latest"]["tp_s"] is None


def test_wis2_features_rules():
    def feat(name, sid, t, v, units="m", lon=126.0, lat=37.0):
        return {"geometry": {"coordinates": [lon, lat]},
                "properties": {"name": name, "wigos_station_identifier": sid, "reportTime": t,
                               "phenomenonTime": t, "value": v, "units": units}}
    doc = {"features": [
        feat("significant_wave_height", "A", "2026-10-31T20:00:00Z", 1.0),
        feat("sea_surface_temperature", "A", "2026-10-31T20:00:00Z", 293.15, units="K"),
        feat("spectral_peak_wave_period", "A", "2026-10-31T20:00:00Z", 8.0, units="s"),
        feat("significant_wave_height", "B", "2026-10-31T20:00:00Z", 1.0, lon=500),        # bad position
        feat("spectral_peak_wave_period", "C", "2026-10-31T20:00:00Z", 8.0),               # no height
        feat("significant_wave_height", "D", "2026-11-01T05:00:00Z", 1.0),                 # ahead of the clock
        feat("air_temperature", "A", "2026-10-31T20:00:00Z", 15.0)]}
    out = B.parse_wis2_features([doc], NOW)
    assert set(out) == {"A"}
    a = out["A"]["rows"][0]
    assert a["sst_c"] == pytest.approx(20.0) and a["tp_s"] == 8.0
    with pytest.raises(ValueError):
        B.parse_wis2_features([{"type": "x"}], NOW)


# ----------------------------------------------------------------------------------------------- dedup

def test_national_feeds_win_over_cmems_relays():
    kma = {"source": "KMA", "capabilities": {}}
    sim = {"source": "SIMCOSTA", "capabilities": {}}
    cm = {"source": "CMEMS", "capabilities": {}}
    assert B._station_priority(kma) > B._station_priority(cm)
    assert B._station_priority(sim) > B._station_priority(cm)
    assert B._station_priority(kma) < B._station_priority({"source": "CEFAS", "capabilities": {}})


def test_cmems_copy_of_a_numbered_ndbc_buoy_merges_under_ndbc():
    # section 72 test site: the Antilles box brought CMEMS's GL_TS_MO_41040 / 41044 / 42060, 3-13 km from NDBC's own
    ndbc = {"id": "ndbc:42060", "source": "NDBC", "name": "Caribbean Valley", "lat": 16.41, "lon": -63.21,
            "capabilities": {"spectra": True}}
    copy = {"id": "cmems:GL_TS_MO_42060", "source": "CMEMS", "name": "42060", "lat": 16.43, "lon": -63.33,
            "capabilities": {}}
    other = {"id": "cmems:MO_TS_MO_61499", "source": "CMEMS", "name": "61499", "lat": 39.5, "lon": 2.7,
             "capabilities": {}}
    assert B._buoy_number(copy) == "42060" and B._buoy_number(other) == "61499"
    assert B._buoy_number({"id": "cmems:GL_TS_MO_2300453", "source": "CMEMS"}) == ""
    kept = B.merge_stations([[ndbc], [copy, other]])
    assert [k["id"] for k in kept] == ["ndbc:42060", "cmems:MO_TS_MO_61499"]
    assert kept[0]["also_sources"] == ["CMEMS"]


# ----------------------------------------------------------------------------------------------- G46 fix round

def _nc_copy(name, tmp_path, edit):
    """A copy of a fixture .nc with `edit(h5py file)` applied; returns its bytes."""
    import shutil
    import h5py
    dst = tmp_path / name
    shutil.copy(os.path.join(FIX, name), dst)
    with h5py.File(dst, "r+") as f:
        edit(f)
    return dst.read_bytes()


@pytest.mark.parametrize("values,code", [
    ([0, 1, 2, 2, 1], True),             # Meteo-France 6200001
    ([0, 0, 0], True),                   # 6100002
    ([2, 3, 3, 2, 1], True),             # 6100001 on 2026-10-30 (beside a 3-4 s mean)
    ([2, 3, 3, 2], False),               # whole seconds but none 0 or 1: could be a calm buoy's periods
    ([0, 1, 4], False),                  # a 4: not the relays' 0-3 code
    ([1.8, 1.9, 2.1, 3.6], False),       # the Bothnian Sea
    ([13.3, 13.7], False),
    ([float("nan")], False),
    ([], False),
])
def test_vtpk_code_rule(values, code):
    assert B._vtpk_is_code(values) is code


def test_cmems_relay_code_dropped_even_beside_a_short_mean(tmp_path):
    """G46 F1: 6100001's VTPK 2-3 beside a 3-4 s mean passed the ratio; the file's column is a code: none shown."""
    def edit(f):
        tp = f["VTPK"][...]
        tp[...] = 3.0
        tp.flat[0] = 1.0
        f["VTPK"][...] = tp
        tm = f["VGTA"][...]
        tm[...] = 3.0
        f["VGTA"][...] = tm
    body = _nc_copy("GL_TS_MO_6200001_20261031.nc", tmp_path, edit)

    def route(url):
        return body if url.endswith("GL_TS_MO_6200001_20261031.nc") else _cmems_route(url)
    p = B.CopernicusProvider(http=FileHTTP(route))
    p._fetch_stations()
    rows = p.detail("GL_TS_MO_6200001")["recent"]
    assert rows and all(o["tp_s"] is None for o in rows)


def test_cmems_national_file_values_in_the_first_depth_column(tmp_path):
    """The column rule reads the column that holds values, the first as well as the second (mutant: always 1)."""
    def edit(f):
        for name in ("VGHS", "VTPK", "VGTA", "VDIR"):
            a = f[name][...]
            a[:, 0] = a[:, 1]
            a[:, 1] = f[name].attrs["_FillValue"]
            f[name][...] = a
    body = _nc_copy("GL_TS_MO_2300453_20261031.nc", tmp_path, edit)

    def route(url):
        return body if url.endswith("GL_TS_MO_2300453_20261031.nc") else _cmems_route(url)
    p = B.CopernicusProvider(http=FileHTTP(route))
    p._fetch_stations()
    o = p.detail("GL_TS_MO_2300453")["latest"]
    assert o["hs_m"] == pytest.approx(0.99, abs=0.005) and o["tp_s"] == pytest.approx(13.3, abs=0.05)


def test_cmems_bad_time_in_the_day_before_keeps_today(tmp_path):
    """G46 F7: a fill-value TIME in the day-before file blanked today's window too."""
    def edit(f):
        t = f["TIME"][...]
        t[-1] = 9.96921e36
        f["TIME"][...] = t
    body = _nc_copy("GL_TS_MO_2300453_20261030.nc", tmp_path, edit)

    def route(url):
        return body if url.endswith("GL_TS_MO_2300453_20261030.nc") else _cmems_route(url)
    p = B.CopernicusProvider(http=FileHTTP(route))
    p._fetch_stations()
    d = p.detail("GL_TS_MO_2300453")
    assert d["latest"] is not None and d["latest"]["hs_m"] == pytest.approx(0.99, abs=0.005)


def test_cmems_drifter_drawn_at_its_newest_fix():
    """G46 F3: the index gives the day's box (the drifter's middle 11.9 km off); the marker is the file's newest fix."""
    p = B.CopernicusProvider(http=FileHTTP(_cmems_route))
    st = {s["local_id"]: s for s in p._fetch_stations()}
    d = st["GL_TS_DB_7810896"]
    assert d["lat"] == pytest.approx(6.1340, abs=1e-3) and d["lon"] == pytest.approx(76.2731, abs=1e-3)
    calls = [u for u in p.http.calls if "7810896" in u]
    p._fetch_stations()                                      # read once per newest file
    assert [u for u in p.http.calls if "7810896" in u] == calls
    assert st["GL_TS_MO_2300453"]["lat"] == pytest.approx(8.07, abs=0.02)        # a moored buoy: the index


def test_cmems_drifter_unreadable_file_keeps_the_box_middle():
    def route(url):
        return None if "GL_TS_DB_" in url and url.endswith(".nc") else _cmems_route(url)
    p = B.CopernicusProvider(http=FileHTTP(route))
    st = {s["local_id"]: s for s in p._fetch_stations()}
    assert st["GL_TS_DB_7810896"]["lat"] == pytest.approx(6.2406, abs=1e-3)


def test_simcosta_height_is_hm0_first():
    """G46 F6: RJ-4 carries both; HM0 (spectral, as every other network) first; Hsig when HM0 is absent or bad."""
    doc = [{"timestamp": "2026-10-31T20:00:00.000Z", "Hsig": 1.25, "HM0": 1.34},
           {"timestamp": "2026-10-31T20:30:00.000Z", "Hsig": 1.25},
           {"timestamp": "2026-10-31T21:00:00.000Z", "Hsig": 1.20, "HM0": -9999}]
    assert [r["hs_m"] for r in B.parse_simcosta_obs(doc, NOW)] == [1.34, 1.25, 1.20]


def test_simcosta_a_timeout_on_one_platform_keeps_the_others():
    """G46 F4: a requests timeout is not a RuntimeError; one platform's timeout must not fail the feed."""
    import requests
    p = B.SimcostaProvider(http=FileHTTP(_sim_route))
    p._fetch_stations()
    held = p._obs["518"]

    def route(url):
        return requests.exceptions.ReadTimeout("slow") if "boiaID=518&" in url else _sim_route(url)
    p.http = FileHTTP(route)
    assert {s["local_id"] for s in p._fetch_stations()} == {"12", "518"} and p._obs["518"] == held


def test_simcosta_list_timeout_keeps_the_list_in_hand(monkeypatch):
    import requests
    p = B.SimcostaProvider(http=FileHTTP(_sim_route))
    p._fetch_stations()
    p._stations_at -= 7 * 3600                               # the list is due again

    def route(url):
        return requests.exceptions.ConnectionError("down") if url.endswith("/api/equipamentos/") else _sim_route(url)
    p.http = FileHTTP(route)
    assert {s["local_id"] for s in p._fetch_stations()} == {"12", "518"}


def test_simcosta_every_platform_failing_is_a_failed_refresh():
    """G46 F5: with rows in hand, every platform failing republished them as a good refresh, forever."""
    import requests
    p = B.SimcostaProvider(http=FileHTTP(_sim_route))
    p._fetch_stations()
    p.http = FileHTTP(lambda u: "simcosta_list.json" if u.endswith("/") else requests.exceptions.ReadTimeout("x"))
    with pytest.raises(RuntimeError):
        p._fetch_stations()
    p.http = FileHTTP(lambda u: "simcosta_list.json" if u.endswith("/") else None)        # HTTP 404s
    with pytest.raises(RuntimeError):
        p._fetch_stations()


def test_simcosta_two_day_freshness_and_naive_times():
    fresh = {"boia_status": 1, "latitude": -22.9, "longitude": -43.1, "id": 9004, "boia": "RJ-9"}
    keep = B.parse_simcosta_stations([dict(fresh, atualizacao="2026-10-30T01:00:00.000Z")], NOW, 2 * 86400)
    drop = B.parse_simcosta_stations([dict(fresh, atualizacao="2026-10-29T23:00:00.000Z")], NOW, 2 * 86400)
    assert 9004 in keep and drop == {}
    assert B._iso_z("2026-10-31T22:21:40") is None and B._iso_z("2026-10-31T22:21:40Z") == "2026-10-31T22:21:40Z"


def test_kma_a_timeout_on_a_secondary_variable_keeps_the_heights():
    import requests
    p = B.KmaBuoyProvider(http=FileHTTP(
        lambda u: requests.exceptions.ReadTimeout("x") if "sea_surface_temperature" in u else _kma_route(u)))
    st = p._fetch_stations()
    assert len(st) == 3 and p.detail("0-410-0-21229")["latest"]["sst_c"] is None


def test_kma_asks_the_last_26_hours():
    p = B.KmaBuoyProvider(http=FileHTTP(_kma_route))
    p._fetch_stations()
    since = datetime.fromtimestamp(NOW - 26 * 3600, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert all("datetime=%s/.." % since in u for u in p.http.calls)


def test_wis2_units_other_than_the_expected_are_left_out():
    """G46 F7: 25 cm read as 25 m; a height in another unit is no height (the node's change fails loudly)."""
    def feat(name, t, v, units):
        return {"geometry": {"coordinates": [126.0, 37.0]},
                "properties": {"name": name, "wigos_station_identifier": "A", "reportTime": t,
                               "phenomenonTime": t, "value": v, "units": units}}
    t = "2026-10-31T20:00:00Z"
    got = B.parse_wis2_features([{"features": [feat("significant_wave_height", t, 25, "cm")]}], NOW)
    assert got == {}
    got = B.parse_wis2_features([{"features": [
        feat("significant_wave_height", t, 1.0, "m"), feat("spectral_peak_wave_period", t, 8.0, "min"),
        feat("sea_surface_temperature", t, 20.0, "degF")]}], NOW)
    row = got["A"]["rows"][0]
    assert row["hs_m"] == 1.0 and row["tp_s"] is None and row["sst_c"] is None


def test_cmems_drifter_fix_read_again_when_the_day_file_grows():
    """A day's file keeps its name while it grows: the newest fix is read again when the index's end time moves."""
    p = B.CopernicusProvider(http=FileHTTP(_cmems_route))
    p._fetch_stations()
    n0 = sum(1 for u in p.http.calls if "7810896" in u)
    body = open(os.path.join(FIX, "cmems_index.txt"), encoding="utf-8").read()
    rows = [ln for ln in body.splitlines() if "GL_TS_DB_7810896" in ln]
    newest = max(rows, key=lambda ln: ln.split(",")[7])
    r = newest.split(",")
    r[7] = r[7].replace("T20:", "T21:")                     # the same file, an hour longer
    grown = body.replace(newest, ",".join(r))
    p.http = FileHTTP(lambda u: grown.encode("utf-8") if u.endswith("index_latest.txt") else _cmems_route(u))
    p._fetch_stations()
    assert sum(1 for u in p.http.calls if "7810896" in u) == 1 and n0 == 1


def test_cmems_drifter_fix_forgotten_when_the_drifter_leaves_the_list():
    p = B.CopernicusProvider(http=FileHTTP(_cmems_route))
    p._fetch_stations()
    assert any("7810896" in k[0] for k in p._fixes)
    body = "\n".join(ln for ln in open(os.path.join(FIX, "cmems_index.txt"), encoding="utf-8").read().splitlines()
                     if "GL_TS_DB_7810896" not in ln) + "\n"
    p.http = FileHTTP(lambda u: body.encode("utf-8") if u.endswith("index_latest.txt") else _cmems_route(u))
    p._fetch_stations()
    assert not any("7810896" in k[0] for k in p._fixes)
