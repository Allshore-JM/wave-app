"""NWPS tables, step 1 (plan section 59): the SWAN spectral-file reader, the 2-D swell partitioner, the NOMADS listing
parser and the station file. Offline: real fixtures in tests/fixtures/nwps (tests/capture_nwps_fixtures.py)."""
import json
import math
import os
import sys
from datetime import datetime

import numpy as np
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "tools", "model_frames"))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import nwps_fetch as N  # noqa: E402
import partition2d as P  # noqa: E402
import spc2d  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "nwps")


def fixture(name):
    return open(os.path.join(FIX, name), "rb").read()


# ----------------------------------------------------------------------------------------------- the reader

def test_reader_on_the_real_file():
    sp = spc2d.read(fixture("51003.spc2d.txt"))
    assert len(sp.freqs) == 37 and sp.freqs[0] == 0.035 and sp.freqs[-1] == 1.5
    assert all(b > a for a, b in zip(sp.freqs, sp.freqs[1:]))
    assert len(sp.dirs) == 36 and sp.dirs[0] == 265.0 and sp.dirs[-1] == 275.0 and all(0 <= d < 360 for d in sp.dirs)
    assert (round(sp.lon, 3), round(sp.lat, 3)) == (-160.569, 19.289)        # NOAA writes 199.431 E
    assert sp.exception == -99.0
    assert [t for t, _ in sp.steps] == [datetime(2026, 10, 28, h) for h in range(6)]
    rows = sp.steps[0][1]
    assert len(rows) == 37 and all(len(r) == 36 for r in rows)
    # the integers are scaled by the FACTOR (hour 0's is 0.22267703E-04): the largest value = the largest integer x it
    block = fixture("51003.spc2d.txt").decode().split("20261028.000000", 1)[1].split("20261028.010000")[0]
    ints = [int(t) for line in block.splitlines()[3:] for t in line.split()]
    assert max(max(r) for r in rows) == pytest.approx(max(ints) * 0.22267703e-4, rel=1e-6)
    hs = 4 * math.sqrt(float((np.array(rows).sum(1) * P.bin_widths(sp.freqs) * 10).sum()))
    assert 2.0 < hs < 2.2                                                   # the GFS bulletin's Hst was 2.00 m


def test_reader_nodata_and_zero():
    sp = spc2d.read(fixture("46240_nodata.txt"))
    assert sp.steps == [(datetime(2026, 10, 28, 0), None)] and round(sp.lon, 3) == -121.907
    text = fixture("51003.spc2d.txt").decode()
    head, _ = text.split("20261028.000000", 1)
    sp = spc2d.read(head + "20261028.000000  date and time\nZERO\n20261028.010000  date and time\nNODATA\n")
    assert sp.steps[0][1] == [[0.0] * 36 for _ in range(37)] and sp.steps[1][1] is None


def test_reader_exception_value_and_refusals():
    text = fixture("51003.spc2d.txt").decode()
    head, rest = text.split("20261028.000000", 1)
    block = "20261028.000000" + rest.split("20261028.010000")[0]
    one = head + block.replace("    0    0    0    0    2    9    7", " -99 -99    0    0    2    9    7", 1)
    sp = spc2d.read(one)
    assert math.isnan(sp.steps[0][1][2][0]) and math.isnan(sp.steps[0][1][2][1])
    for bad, why in (
        (b"WAVEWATCH" + text.encode()[8:], "magic"),
        (head.encode() + b"20261028.000000 date and time\nBOGUS\n", "keyword"),
        (head.encode() + block.encode()[:-200], "short"),
        ((head + block + "20261028.000000  date and time\nNODATA\n").encode(), "times"),
        (head.replace("    37", "     3", 1).encode() + block.encode(), "counts"),
        (b"x" * (spc2d.MAX_BYTES + 1), "size"),
        (text.replace("0.0350", "0.0500", 1).encode(), "ascending"),
    ):
        with pytest.raises(ValueError):
            spc2d.read(bad)


# -------------------------------------------------------------------------------------------- the partitioner

FREQS = [0.035 * 1.1 ** k for k in range(37)]
DIRS = [(265 - 10 * k) % 360 for k in range(36)]


def gaussian(hs, tp, direction, sf=0.08, sd=18.0):
    """A spectrum with one system of height hs (m), peak period tp (s), from `direction` (deg): a Gaussian in log f and
    in direction (wrapping), scaled so 4 sqrt(m0) = hs."""
    f = np.array(FREQS)
    d = np.array(DIRS, float)
    lf = np.log(f)[:, None] - math.log(1 / tp)
    dd = ((d[None, :] - direction + 180) % 360) - 180
    E = np.exp(-0.5 * (lf / sf) ** 2) * np.exp(-0.5 * (dd / sd) ** 2)
    m0 = float((E.sum(1) * P.bin_widths(f) * 10).sum())
    return E * (hs / 4) ** 2 / m0


def test_one_system_is_one_partition_with_its_numbers():
    for hs, tp, dr in ((2.0, 14.0, 320.0), (0.8, 7.0, 45.0), (3.5, 17.5, 180.0)):
        r = P.partition(FREQS, DIRS, gaussian(hs, tp, dr))
        assert r["n"] == 1 and len(r["parts"]) == 1
        p = r["parts"][0]
        assert p["hs"] == pytest.approx(hs, rel=0.01) and r["hs"] == pytest.approx(hs, rel=0.01)
        assert p["tp"] == pytest.approx(tp, rel=0.03)              # the parabolic fit between the 10 % bins
        assert abs(((p["dir"] - dr + 180) % 360) - 180) < 1.0 and p["share"] == pytest.approx(1.0)


def test_two_separated_systems_are_two_partitions_by_height():
    E = gaussian(2.0, 14.0, 320.0) + gaussian(1.0, 7.0, 45.0)
    r = P.partition(FREQS, DIRS, E)
    assert r["n"] == 2 and [round(p["hs"], 1) for p in r["parts"]] == [2.0, 1.0]
    assert r["parts"][0]["tp"] == pytest.approx(14.0, rel=0.03) and r["parts"][1]["tp"] == pytest.approx(7.0, rel=0.03)
    assert abs(r["parts"][0]["dir"] - 320) < 2 and abs(r["parts"][1]["dir"] - 45) < 2
    assert r["hs"] == pytest.approx(math.sqrt(2.0 ** 2 + 1.0 ** 2), rel=0.01)   # the whole spectrum's energy adds up


def test_a_system_straddling_north_keeps_one_partition_and_its_mean():
    r = P.partition(FREQS, DIRS, gaussian(1.5, 12.0, 2.0))
    assert r["n"] == 1 and abs(((r["parts"][0]["dir"] - 2.0 + 180) % 360) - 180) < 1.0
    r = P.partition(FREQS, DIRS, gaussian(1.5, 12.0, 357.0))
    assert r["n"] == 1 and abs(((r["parts"][0]["dir"] - 357.0 + 180) % 360) - 180) < 1.0


def test_a_weak_speck_is_merged_never_dropped():
    E = gaussian(2.0, 14.0, 320.0)
    E[30, 5] += 2e-5                                          # a lone high-frequency bin, far from the swell
    r = P.partition(FREQS, DIRS, E)
    assert r["n"] == 1 and r["parts"][0]["share"] == pytest.approx(1.0)
    assert r["parts"][0]["m0"] == pytest.approx(float((E.sum(1) * P.bin_widths(np.array(FREQS)) * 10).sum()))


def test_the_bulletins_six_and_the_count_before_it():
    E = sum(gaussian(2.0 - 0.2 * k, 18.0 / 1.35 ** k, 45 * k, sf=0.06, sd=10.0) for k in range(8))   # well apart
    r = P.partition(FREQS, DIRS, E)
    assert r["n"] == 8 and len(r["parts"]) == 6 and r["parts"][0]["hs"] > r["parts"][-1]["hs"]
    assert len(P.partition(FREQS, DIRS, E, max_parts=8)["parts"]) == 8


def test_empty_nan_and_negative_spectra():
    Z = np.zeros((37, 36))
    assert P.partition(FREQS, DIRS, Z) == {"hs": 0.0, "n": 0, "parts": []}
    E = gaussian(1.0, 10.0, 90.0)
    E[0, 0] = float("nan")
    E[1, 1] = -1.0
    r = P.partition(FREQS, DIRS, E)
    assert r["n"] == 1 and r["parts"][0]["hs"] == pytest.approx(1.0, rel=0.02)
    with pytest.raises(ValueError):
        P.partition(FREQS[:-1], DIRS, E)


def test_parabolic_peak_lands_between_bins_and_falls_back_at_the_ends():
    f = np.array(FREQS)
    e1 = np.exp(-0.5 * ((np.log(f) - math.log(0.0742)) / 0.05) ** 2)   # a peak between 0.0727 and 0.0800 Hz
    k = int(np.argmax(e1))
    assert 0.0727 < P._parabolic_peak(f, e1, k) < 0.0800 and abs(P._parabolic_peak(f, e1, k) - 0.0742) < 0.0005
    assert P._parabolic_peak(f, e1, 0) == f[0] and P._parabolic_peak(f, e1, 36) == f[36]


def test_determinism_and_the_real_fixture():
    sp = spc2d.read(fixture("51003.spc2d.txt"))
    E = np.array(sp.steps[0][1])
    a = P.partition(sp.freqs, sp.dirs, E)
    b = P.partition(list(sp.freqs), list(sp.dirs), E.copy())
    assert a == b
    # pinned on 2026-10-28 (hour 0 of hfo's 00Z run; NOAA's GFS table the same hour: 2.00 m, 1.19 m @ 13.6 s FROM 322)
    assert a["hs"] == pytest.approx(2.096, abs=0.005) and a["n"] >= 3
    top = a["parts"][0]
    assert top["hs"] == pytest.approx(1.26, abs=0.05) and top["tp"] == pytest.approx(13.6, abs=0.5)
    assert abs(top["dir"] - 320) < 8


# ------------------------------------------------------------------------------------------ nwps_fetch, the file

def test_listing_parser_on_the_real_page():
    files = N.parse_listing(fixture("listing_CG1.html").decode())
    names = [n for n, _, _ in files]
    assert "nwps.t00z.spc2d_51003_CG1.hfo.txt" in names and "hfo_nwps_CG1_20261028_0000.grib2" in names
    assert all(not n.startswith("/") for n in names)
    when = dict((n, t) for n, t, _ in files)["nwps.t00z.spc2d_51003_CG1.hfo.txt"]
    assert when.year == 2026 and when.tzinfo is not None
    assert N.spc_url("pr", datetime(2026, 10, 28).date(), "hfo", 0, "51003").endswith(
        "/pr.20261028/hfo/00/CG1/nwps.t00z.spc2d_51003_CG1.hfo.txt")
    assert N.grib_name("hfo", datetime(2026, 10, 28).date(), 0) == "hfo_nwps_CG1_20261028_0000.grib2"


def test_head_info_reads_position_and_data_flag():
    lon, lat, data = N.head_info(fixture("51003.spc2d.txt").decode()[:6000])
    assert (round(lon, 3), round(lat, 3), data) == (-160.569, 19.289, True)
    lon, lat, data = N.head_info(fixture("46240_nodata.txt").decode())
    assert (round(lon, 3), data) == (-121.907, False)
    assert N.head_info("<html>") is None and N.head_info("SWAN   1\n") is None


def test_offices_and_regions():
    assert len(N.OFFICES) == 36 and set(N.OFFICES.values()) == {"ar", "er", "pr", "sr", "wr"}
    assert N.OFFICES["hfo"] == "pr" and N.OFFICES["lox"] == "wr" and set(N.OFFICE_NAMES) == set(N.OFFICES)


def test_station_tool_rules():
    import build_nwps_stations as B
    table = "# id|a|b|c|name|d|19.196 N 160.639 W (19&#176;11'46\" N 160&#176;38'20\" W)|x\n51003|a|b|c|n|d|19.196 N 160.639 W (x)|x\n"
    assert B.ndbc_positions(table) == {"51003": (19.196, -160.639)}
    heads = {"u1": "SWAN   1\n$\nTIME\n1\nLONLAT\n1\n  199.431000   19.289000\nAFREQ\n...\n20261028.000000 date and time\nFACTOR\n",
             "u2": "SWAN   1\n$\nTIME\n1\nLONLAT\n1\n  199.0   19.0\nAFREQ\n...\n20261028.000000 date and time\nFACTOR\n",
             "u3": "SWAN   1\n$\nTIME\n1\nLONLAT\n1\n  238.093   36.626\nAFREQ\n...\n20261028.000000 date and time\nNODATA\n"}
    found = {"51003": [("hfo", "pr", None, 0, "u1"), ("gum", "pr", None, 0, "u2")],
             "46240": [("mtr", "wr", None, 0, "u3")], "99999": [("mtr", "wr", None, 0, "u1")]}
    pos = {"51003": (19.196, -160.639), "46240": (36.62, -121.91)}
    out, left = B.build(found, pos, read_head=lambda u: heads[u], log=lambda *a: None)
    assert list(out) == ["51003"] and out["51003"]["office"] == "hfo" and out["51003"]["km"] == pytest.approx(12.7, abs=0.3)
    assert left["46240"] == "mtr: nodata" and left["99999"] == "no NDBC position"
    assert B.MAX_KM == 25.0 and B.km_between(0, 0, 0, 1) == pytest.approx(111.19, abs=0.1)


def test_the_station_file():
    path = os.path.join(ROOT, "nwps_stations.json")
    if not os.path.exists(path):
        pytest.skip("nwps_stations.json not built yet")
    doc = json.load(open(path, encoding="utf-8"))
    assert doc["format"] == "nwps-stations-v1" and doc["max_km"] == 25.0
    st = doc["stations"]
    listed = set(json.load(open(os.path.join(ROOT, "station_list.json"), encoding="utf-8")))
    assert 100 <= len(st) <= 120 and set(st) <= listed
    for sid, e in st.items():
        assert e["office"] in N.OFFICES and e["region"] == N.OFFICES[e["office"]]
        assert 0 <= e["km"] <= 25.0 and -90 <= e["lat"] <= 90 and -180 <= e["lon"] <= 180
    assert "46240" not in st and "42023" not in st and "44018" not in st and "46085" not in st
    assert st["51003"]["office"] == "hfo" and st["46026"]["office"] == "mtr"
    assert "46240" in doc["left_out"] and "nodata" in doc["left_out"]["46240"]
