"""Plan section 110: New Zealand councils' Hilltop servers (GW, Tasman), Environment Canterbury, the Pacific Community
(SPC) through WMO WIS2's global cache (BUFR read without eccodes), Puerto Arica (Chile); on real answers captured once
by tests/capture_buoys_pacific_fixtures.py (2026-11-26)."""
import json
import os
import sys
import urllib.parse
from datetime import datetime, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import buoy_sources as B  # noqa: E402

FIX = os.path.join(HERE, "fixtures", "buoys_pacific")
NOW = datetime(2026, 11, 26, 19, 30, tzinfo=timezone.utc).timestamp()     # just after the capture
SOUTHB = "Wellington Harbour at Baring Head Wave Buoy (South (B))"
TUATEA = "HY Tuatea Buoy at Tasman Bay"


def fx(name):
    return open(os.path.join(FIX, name), "rb").read()


class _Resp:
    def __init__(self, body, status=200):
        self.content = body if isinstance(body, bytes) else body.encode("utf-8")
        self.status_code = status
        self.headers = {}

    def json(self):
        return json.loads(self.content.decode("utf-8"))


class FileHTTP:
    """Answers each URL from `route(url)`: a fixture name, bytes, an Exception to raise, or None (404)."""
    def __init__(self, route):
        self.route, self.calls, self.headers = route, [], []

    def get(self, url, **kw):
        self.calls.append(url)
        self.headers.append(kw.get("headers") or {})
        name = self.route(url)
        if name is None:
            return _Resp(b"<Error><Code>NoSuchKey</Code></Error>", 404)
        if isinstance(name, Exception):
            raise name
        return _Resp(name if isinstance(name, bytes) else fx(name))


class Clock:
    def __init__(self, now=NOW):
        self.now = now

    def time(self):
        return self.now


@pytest.fixture(autouse=True)
def _clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(B, "time", clock)
    return clock


# ------------------------------------------------------------------ Hilltop

GW_DATA = {"Significant Wave Height (Hsig)": "gw_hs.xml", "Significant Wave Period (Tsig)": "gw_tsig.xml",
           "Wave Direction": "gw_dir.xml", "Maximum Wave Height (Hmax)": "gw_hmax.xml",
           "Water Temperature": "gw_sst.xml"}
TDC_DATA = {"Height3": "tdc_hs.xml", "Average Wave Period": "tdc_period.xml",
            "Average Wave Direction": "tdc_dir.xml", "Max Wave Height": "tdc_hmax.xml"}


def hilltop_route(url):
    q = {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).items()}
    gw = "hilltop.gw.govt.nz" in url
    req = q.get("Request")
    if req == "SiteList":
        return "gw_sites.xml" if gw else "tdc_sites.xml"
    if req == "MeasurementList":
        if gw:
            return "gw_meas_southb.xml" if q["Site"] == SOUTHB else "gw_meas_old.xml"
        return "tdc_meas_tuatea.xml"
    if req == "GetData":
        if gw and q["Site"] == SOUTHB:
            return GW_DATA.get(q["Measurement"])
        if not gw and q["Site"] == TUATEA:
            return TDC_DATA.get(q["Measurement"])
        return "gw_old_hs.xml"                       # every other site: a series that ended in 2020 (quiet)
    return None


def test_hilltop_site_list_filter():
    names = [n for n, _, _ in B.parse_hilltop_sites(fx("gw_sites.xml"))]
    assert SOUTHB in names and "Taputeranga Marine Reserve Wave Buoy" in names
    assert not any(n.endswith("-Offline") for n in names)                       # the "-Offline" copies
    assert "Wellington Harbour at Baring Head Wave Buoy 2" not in names          # no position
    assert "Abbots Creek at D/S Donalds Crk Conf" not in names                   # not a wave site


def test_hilltop_offline_copy_with_a_position_is_left_out():
    body = (b'<HilltopServer><Site Name="X Wave Buoy-Offline"><Latitude>-41.4</Latitude><Longitude>174.8</Longitude>'
            b'</Site><Site Name="X Wave Buoy"><Latitude>-41.4</Latitude><Longitude>174.8</Longitude></Site>'
            b'<Site Name="Y Buoy"><Latitude>0</Latitude><Longitude>0</Longitude></Site></HilltopServer>')
    assert B.parse_hilltop_sites(body) == [("X Wave Buoy", -41.4, 174.8)]


def test_hilltop_measurements_and_units():
    f = B.parse_hilltop_measurements(fx("gw_meas_southb.xml"))
    assert f == {"hs_m": "Significant Wave Height (Hsig)", "mean_period_s": "Significant Wave Period (Tsig)",
                 "dir_deg": "Wave Direction", "hmax_m": "Maximum Wave Height (Hmax)", "sst_c": "Water Temperature"}
    assert "tp_s" not in f                     # Tsig is not a peak period; Tz (msecs) is never used
    body = fx("gw_meas_southb.xml").replace(b"<Units>m</Units>", b"<Units>cm</Units>")
    assert B.parse_hilltop_measurements(body) == {}                             # a height in cm: not this site
    t = B.parse_hilltop_measurements(fx("tdc_meas_tuatea.xml"))
    assert t["hs_m"] == "Height3" and t["mean_period_s"] == "Average Wave Period"


def test_hilltop_times_are_nzst():
    pts = B.parse_hilltop_data(fx("gw_hs.xml"))
    first_local = fx("gw_hs.xml").split(b"<T>")[1].split(b"</T>")[0].decode()
    d = datetime.strptime(first_local, "%Y-%m-%dT%H:%M:%S")
    assert pts[0][0] == (d.replace(tzinfo=timezone.utc).timestamp() - 12 * 3600 and
                         datetime.fromtimestamp(d.replace(tzinfo=timezone.utc).timestamp() - 12 * 3600,
                                                timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert pts == sorted(pts)


def test_hilltop_refuses_a_doctype():
    with pytest.raises(ValueError):
        B.parse_hilltop_data(b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "b">]><Hilltop>&a;</Hilltop>')
    with pytest.raises(ValueError):
        B.parse_hilltop_data(b"<Hilltop><Error>No data</Error></Hilltop>")


def test_hilltop_provider_on_real_answers():
    http = FileHTTP(hilltop_route)
    p = B.HilltopWaveProvider(http=http)
    st = p._fetch_stations()
    assert [s["local_id"] for s in st] == ["gw:wellington-harbour-at-baring-head-wave-buoy-south-b",
                                           "tdc:hy-tuatea-buoy-at-tasman-bay"]
    assert st[0]["name"] == "Baring Head South (Wellington)" and st[1]["name"] == "Tuatea buoy (Tasman Bay)"
    d = p.detail(st[0]["local_id"])
    o = d["latest"]
    assert o["time_utc"] == st[0]["latest_time"] and o["dir_kind"] == "from" and o["tp_s"] is None
    assert o["hs_m"] > 0 and o["mean_period_s"] > 0 and 0 <= o["dir_deg"] <= 360 and o["hmax_m"] >= o["hs_m"]
    assert 5 < o["sst_c"] < 25
    assert "Greater Wellington" in d["attribution_text"] and "CC BY 4.0" in d["attribution_text"]
    assert len(d["recent"]) >= 24 and d["recent"][0] == o      # newest first; a day thinned to <= 48 rows (_recent_window)
    t = p.detail(st[1]["local_id"])
    assert "Tasman District Council" in t["attribution_text"] and "CC BY" not in t["attribution_text"]
    # Tuatea's "Max Wave Height" series ended 2025-06-06: its day of old values joins no reading of today
    assert t["latest"]["hmax_m"] is None and all(o["hmax_m"] is None for o in t["recent"])


def test_hilltop_quiet_sites_are_asked_again_only_with_a_new_site_list(_clock):
    http = FileHTTP(hilltop_route)
    p = B.HilltopWaveProvider(http=http)
    p._fetch_stations()
    first = [u for u in http.calls if "GetData" in u and "Baring%20Head%20Wave%20Buoy%20%28North" in u]
    assert len(first) == 1                                                   # the quiet North (A) asked once
    http.calls.clear()
    _clock.now += 1800
    p._fetch_stations()
    assert not [u for u in http.calls if "SiteList" in u or "MeasurementList" in u]       # site list kept 6 h
    assert not [u for u in http.calls if "North" in u]                                     # quiet: not asked
    assert [u for u in http.calls if "South%20%28B%29" in u]
    http.calls.clear()
    _clock.now += B.HILLTOP_SITES_TTL
    p._fetch_stations()
    assert [u for u in http.calls if "SiteList" in u]
    assert [u for u in http.calls if "GetData" in u and "North" in u]       # asked again with the new site list


def test_hilltop_one_site_failing_keeps_what_it_had_and_all_failing_raises(_clock):
    p = B.HilltopWaveProvider(http=FileHTTP(hilltop_route))
    good = p._fetch_stations()

    def broken(url):
        if "GetData" in url and "South%20%28B%29" in url:
            return RuntimeError("GW timed out")
        return hilltop_route(url)
    p.http = FileHTTP(broken)
    _clock.now += 600
    again = p._fetch_stations()
    assert [s["local_id"] for s in again] == [s["local_id"] for s in good]
    assert p.detail(good[0]["local_id"])["latest"]["time_utc"] == good[0]["latest_time"]
    p.http = FileHTTP(lambda url: RuntimeError("down") if "GetData" in url else hilltop_route(url))
    with pytest.raises(RuntimeError):
        p._fetch_stations()


def test_hilltop_a_secondary_field_failing_keeps_the_heights():
    def no_dir(url):
        return RuntimeError("x") if "Measurement=Wave%20Direction" in url else hilltop_route(url)
    p = B.HilltopWaveProvider(http=FileHTTP(no_dir))
    st = p._fetch_stations()
    o = p.detail(st[0]["local_id"])["latest"]
    assert o["hs_m"] > 0 and o["dir_deg"] is None


def test_hilltop_site_list_failure_keeps_the_held_list(_clock):
    p = B.HilltopWaveProvider(http=FileHTTP(hilltop_route))
    p._fetch_stations()
    p.http = FileHTTP(lambda url: RuntimeError("x") if "SiteList" in url else hilltop_route(url))
    _clock.now += B.HILLTOP_SITES_TTL + 1
    assert len(p._fetch_stations()) == 2


def test_hilltop_two_live_series_at_one_buoy_show_once():
    def twin(url):
        q = {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).items()}
        if "hilltop.gw.govt.nz" in url and q.get("Request") == "GetData":
            return GW_DATA.get(q["Measurement"])                      # every GW site answers South (B)'s rows
        if "hilltop.gw.govt.nz" in url and q.get("Request") == "MeasurementList":
            return "gw_meas_southb.xml"
        return hilltop_route(url)
    st = B.HilltopWaveProvider(http=FileHTTP(twin))._fetch_stations()
    gw = [s for s in st if s["local_id"].startswith("gw:")]
    # Taputeranga is 9 km away (its own marker); the three Baring Head series sit within 1 km: one marker
    assert len([s for s in gw if "baring-head" in s["local_id"]]) == 1
    assert len([s for s in gw if "taputeranga" in s["local_id"]]) == 1


# ------------------------------------------------------------------ ECan

def test_ecan_parse_matches_the_pages_latest():
    doc = json.loads(fx("ecan.json"))
    rows = B.parse_ecan_waves(doc, NOW)
    meta = doc["SiteMeta"]
    o = rows[-1]
    want = datetime.fromisoformat(meta["lastSample"]).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert o["time_utc"] == want
    assert (o["hs_m"], o["hmax_m"], o["mean_period_s"], o["dir_deg"]) == (
        round(meta["height"], 2), meta["heightMax"], meta["timings"], o["dir_deg"])
    assert abs(o["dir_deg"] - meta["direction"]) < 1                        # SiteMeta rounds the direction
    assert o["tp_s"] is None and o["dir_kind"] == "from"
    assert rows == sorted(rows, key=lambda r: r["time_utc"])


def test_ecan_provider_sends_the_pages_header():
    http = FileHTTP(lambda url: "ecan.json")
    p = B.EcanWaveProvider(http=http)
    st = p._fetch_stations()
    assert st[0]["local_id"] == "banks" and (st[0]["lat"], st[0]["lon"]) == (-43.75, 173.3333)
    assert http.headers[0]["X-Requested-With"] == "XMLHttpRequest"
    d = p.detail("banks")
    assert d["latest"]["time_utc"] == st[0]["latest_time"] and len(d["recent"]) >= 20
    assert p.detail("other") == {"latest": None, "recent": []}


def test_ecan_without_a_height_series_raises():
    with pytest.raises(ValueError):
        B.parse_ecan_waves({"direction": {"data": []}}, NOW)
    p = B.EcanWaveProvider(http=FileHTTP(lambda url: b"<html>maintenance</html>"))
    with pytest.raises(RuntimeError):
        p._fetch_stations()


# ------------------------------------------------------------------ Puerto Arica

def epa_route(url):
    for a in ("wave-height", "wave-direction", "status"):
        if "action=%s&" % a in url:
            return "epa_%s.json" % a
    return None


def test_arica_times_are_utc_and_directions_join():
    h, d = json.loads(fx("epa_wave-height.json")), json.loads(fx("epa_wave-direction.json"))
    rows = B.parse_arica(h, d, NOW)
    newest = max(h["data"], key=lambda e: e["timestamp"])
    assert rows[-1]["time_utc"] == newest["timestamp"].replace(" ", "T") + "Z"
    assert rows[-1]["hs_m"] == round(newest["value"], 2)
    dn = next(e for e in d["data"] if e["timestamp"] == newest["timestamp"])
    assert rows[-1]["dir_deg"] == dn["direction"] and rows[-1]["dir_spread_deg"] == dn["spread"]
    assert rows[-1]["tp_s"] is None and "mean_period_s" not in rows[-1]


def test_arica_provider_position_and_a_missing_direction(_clock):
    p = B.PuertoAricaProvider(http=FileHTTP(epa_route))
    st = p._fetch_stations()
    s = json.loads(fx("epa_status.json"))
    assert (st[0]["lat"], st[0]["lon"]) == (s["latitude"], s["longitude"])
    assert len(p.detail("arica")["recent"]) >= 30                 # 72 twenty-minute rows thinned to <= 48
    far = json.dumps({"latitude": -18.0, "longitude": -70.0}).encode()
    p2 = B.PuertoAricaProvider(http=FileHTTP(lambda u: far if "status" in u else
                                             (RuntimeError("x") if "wave-direction" in u else epa_route(u))))
    st2 = p2._fetch_stations()
    assert (st2[0]["lat"], st2[0]["lon"]) == (B.PuertoAricaProvider.LAT, B.PuertoAricaProvider.LON)   # a bad fix
    assert p2.detail("arica")["latest"]["dir_deg"] is None and p2.detail("arica")["latest"]["hs_m"] > 0
    p3 = B.PuertoAricaProvider(http=FileHTTP(lambda u: None))
    with pytest.raises(RuntimeError):
        p3._fetch_stations()


def test_arica_a_broken_direction_answer_keeps_the_heights():
    p = B.PuertoAricaProvider(http=FileHTTP(lambda u: b'{"error": "x"}' if "wave-direction" in u else epa_route(u)))
    st = p._fetch_stations()
    assert st and p.detail("arica")["latest"]["dir_deg"] is None


# ------------------------------------------------------------------ SPC / BUFR

SPC_FILES = sorted(n for n in os.listdir(FIX) if n.startswith("spc_WIGOS_"))


def test_bufr_decoder_equals_eccodes_on_real_messages():
    exp = json.loads(fx("spc_expected.json"))
    assert len(SPC_FILES) == 3
    for n in SPC_FILES:
        vals = B.decode_bufr_wave(fx(n))[0]
        e = exp[n[len("spc_"):]]
        first = {d: v for d, v in reversed(vals)}
        assert first[22070] == e["#1#significantWaveHeight"]
        assert first[22071] == pytest.approx(e["#1#spectralPeakWavePeriod"])
        assert first[22076] == e["#1#directionFromWhichDominantWavesAreComing"]
        assert first[5001] == pytest.approx(e["latitude"]) and first[6001] == pytest.approx(e["longitude"])
        assert (first[4001], first[4002], first[4003], first[4004], first[4005]) == (
            e["year"], e["month"], e["day"], e["hour"], e["minute"])
        assert first[1128] == e["wigosLocalIdentifierCharacter"]


def _with_desc(msg, new_desc):
    """The message with its first unexpanded descriptor replaced."""
    b = bytearray(msg)
    p = 8 + int.from_bytes(b[8:11], "big")
    f, x, y = new_desc // 100000, (new_desc // 1000) % 100, new_desc % 1000
    b[p + 7:p + 9] = ((f << 14) | (x << 8) | y).to_bytes(2, "big")
    return bytes(b)


def test_bufr_decoder_refuses_anything_else():
    m = fx(SPC_FILES[0])
    with pytest.raises(ValueError):
        B.decode_bufr_wave(_with_desc(m, 301011))                      # another template
    with pytest.raises(ValueError):
        B.decode_bufr_wave(m[:-6] + b"7777")                           # cut short
    bad = bytearray(m)
    bad[7] = 3
    with pytest.raises(ValueError):
        B.decode_bufr_wave(bytes(bad))                                 # edition 3
    p = 8 + int.from_bytes(m[8:11], "big")
    comp = bytearray(m)
    comp[p + 6] |= 0x40
    with pytest.raises(ValueError):
        B.decode_bufr_wave(bytes(comp))                                # compressed
    # a data section 4 bytes longer than the template reads: refused (the template was misread)
    l4p = p + int.from_bytes(m[p:p + 3], "big")
    l4 = int.from_bytes(m[l4p:l4p + 3], "big")
    longer = bytearray(m[:-4]) + b"\0\0\0\0" + b"7777"
    longer[4:7] = (len(longer)).to_bytes(3, "big")
    longer[l4p:l4p + 3] = (l4 + 4).to_bytes(3, "big")
    with pytest.raises(ValueError):
        B.decode_bufr_wave(bytes(longer))
    with pytest.raises(ValueError):
        B.decode_bufr_wave(b"GRIB" + m[4:])


def test_bufr_reading_fields():
    wsi, lat, lon, o = B.bufr_wave_reading(B.decode_bufr_wave(fx(SPC_FILES[-1]))[0], NOW)
    assert wsi == "0-22000-0-5202533" and (lat, lon) == (-9.42227, 159.94742)
    assert o["dir_kind"] == "from" and o["hs_m"] == 0.17 and o["tp_s"] == 9.3
    assert B.bufr_wave_reading([(1125, 0)], NOW) is None


def spc_route(url):
    if "list-type=2" in url:
        return "spc_list.xml"
    name = url.rsplit("/", 1)[-1]
    return "spc_" + name if ("spc_" + name) in SPC_FILES else None      # files not captured: 404 (asked again)


def test_spc_provider_fetches_only_new_files(_clock):
    http = FileHTTP(spc_route)
    p = B.SpcWis2Provider(http=http)
    st = p._fetch_stations()
    assert [(s["local_id"], s["name"]) for s in st] == [("0-22000-0-5202533", "Honiara (Solomon Islands)")]
    gets = [u for u in http.calls if "list-type" not in u]
    # only files of the last 26 h are asked (the listing holds two days)
    assert gets and all(u.rsplit("_", 1)[-1][:15] >= "20261125T173000" for u in gets)
    assert len(p.detail("0-22000-0-5202533")["recent"]) == 3
    http.calls.clear()
    _clock.now += 900
    p._fetch_stations()
    again = [u for u in http.calls if "list-type" not in u]
    assert not any(("spc_" + u.rsplit("/", 1)[-1]) in SPC_FILES for u in again)    # decoded files not asked again
    assert len(again) == len(gets) - 3                                                # the 404s are asked again


def test_spc_unreadable_file_not_asked_again(_clock):
    junk = {SPC_FILES[0][len("spc_"):]}

    def route(url):
        if url.rsplit("/", 1)[-1] in junk:
            return b"BUFR not really"
        return spc_route(url)
    http = FileHTTP(route)
    p = B.SpcWis2Provider(http=http)
    p._fetch_stations()
    http.calls.clear()
    p._fetch_stations()
    assert not [u for u in http.calls if u.rsplit("/", 1)[-1] in junk]
    assert len(p.detail("0-22000-0-5202533")["recent"]) == 2


def test_spc_listing_pages_and_a_failed_listing_raises():
    page1 = (b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>true</IsTruncated>'
             b'<NextContinuationToken>tok/1</NextContinuationToken>'
             b'<Contents><Key>data/x/WIGOS_0-22000-0-5202533_20261126T162300.bufr4</Key></Contents>'
             b'</ListBucketResult>')
    page2 = (b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated>'
             b'<Contents><Key>data/x/WIGOS_0-22000-0-5202533_20261126T172300.bufr4</Key></Contents>'
             b'</ListBucketResult>')

    def route(url):
        if "list-type=2" in url:
            return page2 if "continuation-token=tok%2F1" in url else page1
        return spc_route(url)
    p = B.SpcWis2Provider(http=FileHTTP(route))
    assert len(p._list_keys()) == 2
    st = p._fetch_stations()
    assert len(p.detail(st[0]["local_id"])["recent"]) == 2
    with pytest.raises(RuntimeError):
        B.SpcWis2Provider(http=FileHTTP(lambda u: None))._fetch_stations()


# ------------------------------------------------------------------ dedup / priority

def test_priorities():
    pr = B._station_priority
    for src in ("HILLTOP", "ECAN", "EPA"):
        assert pr({"source": src}) == 54
    assert pr({"source": "SPC"}) == 44
    assert pr({"source": "SPC"}) < min(pr({"source": s}) for s in ("CDIP", "NDBCW", "NDBC", "AODN"))


def test_an_spc_relay_of_a_cdip_buoy_merges_under_cdip():
    cdip = {"id": "cdip:197", "source": "CDIP", "name": "Ipan, Guam", "lat": 13.354, "lon": 144.788,
            "capabilities": B._caps(bulk=True, recent_history=True), "is_stale": False, "dup_of": None}
    spc = {"id": "spc:0-22000-0-5201197", "source": "SPC", "name": "SPC buoy 5201197", "lat": 13.355, "lon": 144.789,
           "capabilities": B._caps(bulk=True, recent_history=True, directional=True), "is_stale": False,
           "dup_of": None}
    out = B.merge_stations([[spc], [cdip]])
    kept = [s for s in out if not s.get("dup_of")]
    assert [s["id"] for s in kept] == ["cdip:197"]
