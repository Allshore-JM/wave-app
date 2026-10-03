"""Live-buoy positions (2026-10-03): AODN buoys are drawn at their newest OBSERVED position (the data layer), not
at the map layer's per-site summary point; a CMEMS platform at its newest file's position unless that one alone
lies more than 1 km from two agreeing earlier files (then at the earlier position).

Found on allshoresurf.com: "Crowdy" (NSW) drawn in central Australia, Coral Bay 164 km and Cable Beach 140 km off,
48 of 60 AODN buoys more than 200 m from where they are; VillajoyosaBuoy (Spain) 6.5 km inland because its newest
CMEMS file alone reported another position.
"""
import csv
import io
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


SAMPLE_SITES = ["Lakes Entrance", "Augusta Offshore", "Apollo Bay", "Bengello", "Albany", "Bob", "Odd Site", "Bad End",
                "No Geom"]


class RoutedHTTP:
    """The data layer (positions) answers one body, the map layer another, THREDDS a status per spectra code."""
    def __init__(self, positions, mapbody=AODN_SAMPLE, pos_status=200, missing_spectra=()):
        self.positions, self.mapbody, self.pos_status, self.calls = positions, mapbody, pos_status, []
        self.missing_spectra = set(missing_spectra)

    def get(self, url, **kw):
        self.calls.append((url, kw))
        if url.startswith(B.AODNProvider.THREDDS):
            code = url[len(B.AODNProvider.THREDDS):].split("/")[0]
            return GA._Resp("Dataset {}", 500 if code in self.missing_spectra else 200)
        if B.AODNProvider.POS_LAYER in url:
            return GA._Resp(self.positions, self.pos_status)
        return GA._Resp(self.mapbody)


def _all_sites(skip=()):
    return [(s, "2026-10-02T05:00:00", "%.4f" % (-30.0 - i), "%.4f" % (140.0 + i))
            for i, s in enumerate(SAMPLE_SITES) if s not in skip]


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
    assert B.AODNProvider.POS_DAYS == 7                               # = the map layer's window (review: 1 day drops sites)
    assert "maxFeatures=%d" % B.AODNProvider.POS_MAX_ROWS in url


def test_observed_positions_refuse_a_capped_answer(monkeypatch):
    monkeypatch.setattr(B.AODNProvider, "POS_MAX_ROWS", 3)
    rows = [("Bob", "2026-10-02T0%d:00:00" % h, "-38.6", "142.3") for h in range(3)]
    with pytest.raises(RuntimeError):
        B.AODNProvider(http=RoutedHTTP(_positions_csv(rows)))._observed_positions()


@pytest.mark.parametrize("body,status", [("", 200), ("FID,site_name,TIME,LATITUDE,LONGITUDE\n", 200),
                                         ("<ows:ExceptionReport>boom</ows:ExceptionReport>", 200),
                                         ("x", 500), ("FID,site,TIME\nf,a,b\n", 200)])
def test_observed_positions_fail_loudly(body, status):
    with pytest.raises(Exception):
        B.AODNProvider(http=RoutedHTTP(body, pos_status=status))._observed_positions()


def test_sites_take_their_observed_position_and_a_site_without_one_is_dropped():
    p = B.AODNProvider(http=RoutedHTTP(_positions_csv(_all_sites(skip=("Albany",)))))
    out = p._fetch_stations()
    by = {s["local_id"]: (s["lat"], s["lon"]) for s in out}
    assert "Albany" not in by and "Bob" in by and "No Geom" in by     # a position, not the map's geom, decides
    for i, s in enumerate(SAMPLE_SITES):
        if s in by:
            assert by[s] == (-30.0 - i, 140.0 + i)
    assert p._latest_by_id.get("Bob") is not None                   # observations still from the map layer


def test_too_few_positions_fail_the_refresh_and_keep_the_last_good_list():
    """A positions answer that ended early (no error) covers few sites: refused, the last list stays."""
    http = RoutedHTTP(_positions_csv(_all_sites()))
    p = B.AODNProvider(http=http)
    first, v1, _ = p.list_stations_versioned()
    assert {"aodn:Bob", "aodn:Albany"} <= {s["id"] for s in first}
    latest = dict(p._latest_by_id)
    for body, status in ((_positions_csv(_all_sites()[:2]), 200), ("x", 500)):
        http.positions, http.pos_status = body, status
        with p._lock:
            p._list_ts = time.time() - p.list_ttl_sec - 1            # expire the list
        again, v2, _ = p.list_stations_versioned()
        assert again == first and v2 == v1                           # kept, not emptied or thinned
        assert p._latest_by_id == latest                             # positions come first: caches untouched


def test_observation_times_are_the_rows_utc_time():
    """TIME is UTC (2026-10-03: Wilsons Prom TIME 19:50 = AusWaves' 19:50Z); time_end, 10 h behind, is ignored."""
    p = B.AODNProvider(http=RoutedHTTP(_positions_csv(_all_sites())))
    p._fetch_stations()
    rows = [r for r in csv.DictReader(io.StringIO(AODN_SAMPLE)) if r["site_name"] == "Bob"]
    newest = max(r["TIME"] for r in rows)
    assert p._latest_by_id["Bob"]["time_utc"] == newest + "Z"
    assert all(o["time_utc"] <= newest + "Z" for o in p._recent_by_id["Bob"])


def test_spectra_rank_only_where_this_months_file_exists():
    """A SPECTRA_SITES code without its THREDDS file is a plain bulk buoy (it hid the richer AusWaves marker)."""
    http = RoutedHTTP(_positions_csv(_all_sites()), missing_spectra=("BENGELLO",))
    p = B.AODNProvider(http=http)
    by = {s["local_id"]: s for s in p._fetch_stations()}
    assert not by["Bengello"].get("capabilities", {}).get("spectra")
    assert by["Apollo Bay"]["capabilities"]["spectra"] and by["Bob"]["capabilities"]["spectra"]
    probes = [u for u, _ in http.calls if u.startswith(B.AODNProvider.THREDDS)]
    assert len(probes) == len(B.AODNProvider.SPECTRA_SITES)
    p._fetch_stations()                                              # cached: no second round of probes
    assert len([u for u, _ in http.calls if u.startswith(B.AODNProvider.THREDDS)]) == len(probes)


def test_aodn_markers_merge_with_another_network_on_the_site_name():
    """AODN names are "<site> - <institution>": matched on the site name (Storm Bay 2.16 km apart: one buoy)."""
    def st(src, sid, name, lat, lon, caps=None):
        return {"id": sid, "source": src, "name": name, "lat": lat, "lon": lon, "capabilities": caps or {}}
    aodn = [st("AODN", "aodn:Storm Bay", "Storm Bay - IMOS Coastal Wave Buoys Facility", -43.211, 147.4554,
               {"spectra": True}),
            st("AODN", "aodn:Dongara Offshore", "Dongara Offshore - The University of Western Australia", -29.1971, 114.8314)]
    aus = [st("AusWaves", "auswaves:16000", "Storm Bay", -43.1924, 147.4475),
           st("AusWaves", "auswaves:10058", "Dongara Inshore (~22m depth)", -29.1809, 114.8622)]
    kept = {k["id"] for k in B.merge_stations([aodn, aus], radius_km=1.0)}
    assert kept == {"aodn:Storm Bay", "aodn:Dongara Offshore", "auswaves:10058"}


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
                   ("IR_TS_MO_Relocated", d[5], "42.1001", "3.1001"),             # a move seen in 2 files: taken
                   ("IR_TS_MO_Unsure", d[0], "43.000", "4.000"),
                   ("IR_TS_MO_Unsure", d[1], "43.100", "4.100"),                    # the two older ones disagree
                   ("IR_TS_MO_Unsure", d[4], "43.300", "4.300")])
    p = B.CopernicusProvider(http=GC.FakeHTTP(body))
    by = {s["local_id"]: (s["lat"], s["lon"]) for s in p._fetch_stations()}
    assert by["IR_TS_MO_VillajoyosaBuoy"] == (38.4971, -0.204)
    assert by["IR_TS_MO_Moved"] == (40.5, 1.5)
    assert by["IR_TS_MO_Steady"] == (41.12345, 2.12345)
    assert by["IR_TS_MO_Relocated"] == (42.1001, 3.1001)
    assert by["IR_TS_MO_Unsure"] == (43.3, 4.3)
    assert p._file_by_id["IR_TS_MO_VillajoyosaBuoy"].endswith(d[3].replace("-", "") + ".nc")   # data: still the newest


def test_cmems_wide_boxes_are_no_evidence():
    """Cerema rows: a full-day file's box can span 3-19 km; its midpoint is nowhere the buoy was. Only single-position
    files vote, so a correct single-point newest file is not 'corrected' to an old midpoint (review P1-1)."""
    today = datetime.now(timezone.utc).date()
    d = [(today - timedelta(days=k)).isoformat() for k in range(3, -1, -1)]
    head = ("# product_id,file_name,geospatial_lat_min,geospatial_lat_max,geospatial_lon_min,geospatial_lon_max,"
            "time_coverage_start,time_coverage_end,institution,date_update,data_mode,parameters\n")
    row = ("COP-GL-01,X/latest/{d}/GL_TS_MO_6100023_{d}.nc,{a},{b},{c},{e},{day}T00:00:00Z,{day}T17:00:00Z,inst,"
           "{day}T23:00:00Z,R,VHM0 VTPK\n")
    boxes = [("41.325", "41.41528", "8.8733", "9.04722")] * 3 + [("41.41528", "41.41528", "9.04722", "9.04722")]
    body = head + "".join(row.format(d=x.replace("-", ""), day=x, a=a, b=b, c=c, e=e) for x, (a, b, c, e) in zip(d, boxes))
    by = {s["local_id"]: (s["lat"], s["lon"]) for s in B.CopernicusProvider(http=GC.FakeHTTP(body))._fetch_stations()}
    assert by["GL_TS_MO_6100023"] == (41.41528, 9.04722)
