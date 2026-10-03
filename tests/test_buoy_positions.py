"""Live-buoy positions (2026-10-03): AODN buoys are drawn at their newest OBSERVED position (the data layer), not
at the map layer's per-site summary point; a CMEMS platform at its newest file's position unless that one alone
lies more than 1 km from two agreeing earlier files (then at the earlier position).

Found on allshoresurf.com: "Crowdy" (NSW) drawn in central Australia, Coral Bay 164 km and Cable Beach 140 km off,
48 of 60 AODN buoys more than 200 m from where they are; VillajoyosaBuoy (Spain) 6.5 km inland because its newest
CMEMS file alone reported another position.
"""
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import buoy_sources as B  # noqa: E402
import capture_aodn_golden as GA  # noqa: E402
import capture_cmems_golden as GC  # noqa: E402

AODN_SAMPLE = open(GA.SAMPLE, encoding="utf-8", newline="").read()


def _positions_csv(rows):
    head = "FID,site_name,TIME,LATITUDE,LONGITUDE\n"
    return head + "".join("f.%d,%s,%s,%s,%s\n" % (i, *r) for i, r in enumerate(rows))


class RoutedHTTP:
    """The data layer (positions) answers one body, the map layer another."""
    def __init__(self, positions, mapbody=AODN_SAMPLE, pos_status=200):
        self.positions, self.mapbody, self.pos_status, self.calls = positions, mapbody, pos_status, []

    def get(self, url, **kw):
        self.calls.append((url, kw))
        if B.AODNProvider.POS_LAYER in url:
            return GA._Resp(self.positions, self.pos_status)
        return GA._Resp(self.mapbody)


def test_observed_positions_newest_per_site():
    body = _positions_csv([("Bob", "2026-10-02T01:00:00", "-38.60", "142.30"),
                           ("Bob", "2026-10-02T05:00:00", "-38.643", "142.3096"),     # newest wins
                           ("Bob", "2026-10-02T03:00:00", "-38.70", "142.40"),
                           ("Albany", "2026-10-02T04:00:00", "NaN", "117.72"),         # no position: skipped
                           ("Albany", "2026-10-02T02:00:00", "-35.2", "117.72"),
                           ("Far", "2026-10-02T02:00:00", "-10.0", "190.0"),           # 0..360 longitude
                           ("", "2026-10-02T02:00:00", "-10.0", "100.0")])
    http = RoutedHTTP(body)
    pos = B.AODNProvider(http=http)._observed_positions()
    assert pos == {"Bob": (-38.643, 142.3096), "Albany": (-35.2, 117.72), "Far": (-10.0, -170.0)}
    url, kw = http.calls[0]
    assert kw.get("stream") is True and "LATITUDE" in url and "CQL_FILTER=TIME%20%3E%3D%20%27" in url
    since = url.split("%27")[1].replace("%3A", ":")
    age = datetime.now(timezone.utc) - datetime.strptime(since, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    assert timedelta(days=B.AODNProvider.POS_DAYS) - timedelta(minutes=5) < age < timedelta(days=B.AODNProvider.POS_DAYS, minutes=5)


@pytest.mark.parametrize("body,status", [("", 200), ("FID,site_name,TIME,LATITUDE,LONGITUDE\n", 200),
                                         ("<ows:ExceptionReport>boom</ows:ExceptionReport>", 200),
                                         ("x", 500), ("FID,site,TIME\nf,a,b\n", 200)])
def test_observed_positions_fail_loudly(body, status):
    with pytest.raises(Exception):
        B.AODNProvider(http=RoutedHTTP(body, pos_status=status))._observed_positions()


def test_sites_take_their_observed_position_and_a_site_without_one_is_dropped():
    sites = ["Lakes Entrance", "Augusta Offshore", "Apollo Bay", "Bengello", "Albany", "Bob"]
    rows = [(s, "2026-10-02T05:00:00", "%.4f" % (-30.0 - i), "%.4f" % (140.0 + i)) for i, s in enumerate(sites) if s != "Albany"]
    p = B.AODNProvider(http=RoutedHTTP(_positions_csv(rows)))
    out = p._fetch_stations()
    by = {s["local_id"]: (s["lat"], s["lon"]) for s in out}
    assert "Albany" not in by and len(by) == 5
    for i, s in enumerate(sites):
        if s != "Albany":
            assert by[s] == (-30.0 - i, 140.0 + i)
    assert p._latest_by_id.get("Bob") is not None                   # observations still from the map layer


def test_a_positions_failure_keeps_the_last_good_list():
    rows = [("Bob", "2026-10-02T05:00:00", "-38.643", "142.3096")]
    http = RoutedHTTP(_positions_csv(rows))
    p = B.AODNProvider(http=http)
    first, v1, _ = p.list_stations_versioned()
    assert [s["id"] for s in first] == ["aodn:Bob"] and first[0]["lat"] == -38.643
    http.pos_status = 500                                            # the positions query fails
    with p._lock:
        p._list_ts = time.time() - p.list_ttl_sec - 1                # expire the list
    again, v2, _ = p.list_stations_versioned()
    assert again == first and v2 == v1                               # kept, not emptied


def _index(lines):
    head = ("# product_id,file_name,geospatial_lat_min,geospatial_lat_max,geospatial_lon_min,geospatial_lon_max,"
            "time_coverage_start,time_coverage_end,institution,date_update,data_mode,parameters\n")
    out = []
    for pid, day, la, lo in lines:
        out.append("COP-IR-01,X/latest/%s/%s_%s.nc,%s,%s,%s,%s,%sT00:00:00Z,%sT17:00:00Z,inst,%sT23:00:00Z,R,VHM0 VTPK\n"
                   % (day.replace("-", ""), pid, day.replace("-", ""), la, la, lo, lo, day, day, day))
    return head + "".join(out)


def test_cmems_platform_sits_where_most_of_its_files_put_it():
    today = datetime.now(timezone.utc).date()
    d = [(today - timedelta(days=k)).isoformat() for k in range(5, -1, -1)]
    body = _index([("IR_TS_MO_VillajoyosaBuoy", d[0], "38.4971", "-0.2040"),
                   ("IR_TS_MO_VillajoyosaBuoy", d[1], "38.4972", "-0.2041"),
                   ("IR_TS_MO_VillajoyosaBuoy", d[2], "38.4971", "-0.2040"),
                   ("IR_TS_MO_VillajoyosaBuoy", d[3], "38.72885", "0.07580"),       # the newest file alone: inland
                   ("IR_TS_MO_Moved", d[0], "40.000", "1.000"),
                   ("IR_TS_MO_Moved", d[3], "40.500", "1.500"),                     # 1 vote each: the newer wins
                   ("IR_TS_MO_Steady", d[0], "41.1234", "2.1234"),
                   ("IR_TS_MO_Steady", d[4], "41.12345", "2.12345"),                # same cluster: newest exact
                   ("IR_TS_MO_Relocated", d[0], "42.000", "3.000"),
                   ("IR_TS_MO_Relocated", d[1], "42.000", "3.000"),
                   ("IR_TS_MO_Relocated", d[4], "42.100", "3.100"),
                   ("IR_TS_MO_Relocated", d[5], "42.1001", "3.1001")])            # a move seen in 2 files: taken
    p = B.CopernicusProvider(http=GC.FakeHTTP(body))
    by = {s["local_id"]: (s["lat"], s["lon"]) for s in p._fetch_stations()}
    assert by["IR_TS_MO_VillajoyosaBuoy"] == (38.4971, -0.204)
    assert by["IR_TS_MO_Moved"] == (40.5, 1.5)
    assert by["IR_TS_MO_Steady"] == (41.12345, 2.12345)
    assert by["IR_TS_MO_Relocated"] == (42.1001, 3.1001)
    assert p._file_by_id["IR_TS_MO_VillajoyosaBuoy"].endswith(d[3].replace("-", "") + ".nc")   # data: still the newest
