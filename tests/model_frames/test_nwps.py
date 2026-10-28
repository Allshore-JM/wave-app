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
        ((head + block.replace("    0    0    0    0    2    9    7", "    0    0    0    0    2    9    7    1", 1)).encode(), "a wide row"),
        (b"x" * (spc2d.MAX_BYTES + 1), "size"),
        (text.replace("0.0350", "0.0500", 1).encode(), "ascending"),
        (b"", "empty"),
        (head.encode(), "no steps"),
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


def test_a_weak_speck_is_merged_not_listed_alone():
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
    assert r["hs"] == pytest.approx(1.0, rel=0.02) and r["parts"][0]["share"] == pytest.approx(1.0)   # nan = nothing
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


def test_rotating_the_directions_rotates_the_answer():
    """The smoothing and the watershed wrap in direction: a spectrum rolled by k bins gives the same partitions with the
    directions moved by 10 k degrees, wherever the seam falls."""
    E = gaussian(2.0, 14.0, 320.0) + gaussian(0.9, 8.0, 200.0) + gaussian(0.6, 11.0, 95.0)
    base = P.partition(FREQS, DIRS, E)
    for k in (1, 7, 18, 31):
        rolled = P.partition(FREQS, DIRS, np.roll(E, k, axis=1))
        assert rolled["n"] == base["n"] and rolled["hs"] == pytest.approx(base["hs"])
        for a, b in zip(base["parts"], rolled["parts"]):
            assert b["hs"] == pytest.approx(a["hs"]) and b["tp"] == pytest.approx(a["tp"])
            assert abs(((b["dir"] - a["dir"] + 10 * k + 180) % 360) - 180) < 1e-6   # DIRS step down by 10 per bin


def test_a_speck_joins_the_neighbour_it_touches_most():
    E = gaussian(2.0, 14.0, 320.0) + gaussian(0.8, 7.0, 45.0)
    base = P.partition(FREQS, DIRS, E)
    k = int(np.argmin(np.abs(np.array(FREQS) - 1 / 7.0)))
    j = DIRS.index(45)
    E2 = E.copy()
    E2[k + 3, j] += 3e-4                                      # a lone bin beside the small system only
    r = P.partition(FREQS, DIRS, E2)
    assert r["n"] == 2 and r["parts"][0]["m0"] == pytest.approx(base["parts"][0]["m0"])
    assert r["parts"][1]["m0"] > base["parts"][1]["m0"]       # the speck's energy went to its neighbour, the small one
    assert P._merge_target({1: 5, 2: 3}, {1: 0.1, 2: 9.0}) == 1   # the most shared boundary wins ...
    assert P._merge_target({1: 4, 2: 4}, {1: 0.1, 2: 9.0}) == 2   # ... the bigger partition on a tie


def test_a_small_but_real_system_is_listed():
    r = P.partition(FREQS, DIRS, gaussian(2.0, 14.0, 320.0) + gaussian(0.3, 9.0, 120.0))
    assert r["n"] == 2 and r["parts"][1]["hs"] == pytest.approx(0.3, rel=0.03)
    assert P.MIN_HS == 0.05


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
             "u3": "SWAN   1\n$\nTIME\n1\nLONLAT\n1\n  238.093   36.626\nAFREQ\n...\n20261028.000000 date and time\nNODATA\n",
             "u4": "SWAN   1\n$\nTIME\n1\nLONLAT\n1\n  199.431   19.000\nAFREQ\n...\n20261028.000000 date and time\nFACTOR\n",
             "u5": "SWAN   1\n$\nTIME\n1\nLONLAT\n1\n  199.431   19.600\nAFREQ\n...\n20261028.000000 date and time\nFACTOR\n"}
    found = {"51003": [("sju", "sr", None, 0, "u4"), ("gum", "pr", None, 0, "u2"), ("hfo", "pr", None, 0, "u1")],
             "46240": [("mtr", "wr", None, 0, "u3")], "99999": [("mtr", "wr", None, 0, "u1")],
             "46001": [("aer", "ar", None, 0, "u5")]}
    pos = {"51003": (19.196, -160.639), "46240": (36.62, -121.91), "46001": (19.196, -160.639)}
    out, left = B.build(found, pos, read_head=lambda u: heads[u], log=lambda *a: None)
    assert list(out) == ["51003"] and out["51003"]["km"] == pytest.approx(12.7, abs=0.3)
    assert out["51003"]["office"] == "hfo"                     # gum's point is far; sju's (u4) is in range (22 km) but farther
    assert left["46240"] == "mtr: nodata" and left["99999"] == "no NDBC position"
    assert left["46001"].startswith("aer: far (")              # 45 km north of the buoy
    found["51003"] = [("sju", "sr", None, 0, "u4")]
    out, _ = B.build(found, pos, read_head=lambda u: heads[u], log=lambda *a: None)
    assert out["51003"]["office"] == "sju" and 5 < out["51003"]["km"] < 25
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


def test_reader_values_are_integers_and_factors_sane():
    """G41a P3-3 / P3-2: only integers (or the exception value) in the rows, a finite factor, at most MAX_STEPS steps."""
    text = fixture("51003.spc2d.txt").decode()
    head, rest = text.split("20261028.000000", 1)
    block = "20261028.000000" + rest.split("20261028.010000")[0]
    row = "    0    0    0    0    2    9    7"
    for tok in ("  2.5", "  nan", "  inf", "1e999", "1_000", "   -5"):
        with pytest.raises(ValueError):
            spc2d.read(head + block.replace(row, "    0    0    0    0    2    9" + tok, 1))
    factor = block.split("FACTOR\n", 1)[1].split("\n", 1)[0]
    for bad in ("  inf", "  nan", " 1E300", " -1E-5"):
        with pytest.raises(ValueError):
            spc2d.read(head + block.replace(factor, bad, 1))
    zero = "".join(f"20261028.{h // 60:02d}{h % 60:02d}00  date and time\nZERO\n" for h in range(spc2d.MAX_STEPS + 1))
    with pytest.raises(ValueError, match="time steps"):
        spc2d.read(head + zero)
    assert len(spc2d.read(head + "".join(zero.splitlines(True)[:2 * spc2d.MAX_STEPS])).steps) == spc2d.MAX_STEPS


# pinned 2026-10-28 by the fixed partitioner (G41a): every hour of the fixture (hfo 51003, 00Z), whole-spectrum Hs, the
# count, and each listed partition (Hs m, Tp s, FROM deg). Catches a change of the smoothing, the tie rule, the small-
# partition rule, the shallow-pass switch, the per-Hz frequency spectrum behind Tp, the bin widths.
GOLDEN = [
    (2.096, 5, [(1.252, 14.07, 323.3), (1.153, 4.96, 92.7), (0.912, 8.74, 356.3), (0.799, 15.86, 187.6), (0.154, 10.03, 139.3)]),
    (2.1, 5, [(1.259, 13.98, 323.5), (1.165, 4.91, 92.8), (0.896, 8.69, 356.9), (0.8, 15.72, 187.4), (0.153, 10.02, 139.3)]),
    (2.09, 4, [(1.256, 13.89, 323.4), (1.165, 4.89, 94.4), (0.89, 8.69, 357.2), (0.801, 15.64, 187.3)]),
    (2.089, 4, [(1.252, 13.8, 323.3), (1.175, 4.84, 96.0), (0.883, 8.68, 357.7), (0.798, 15.58, 187.2)]),
    (2.094, 5, [(1.254, 13.71, 323.3), (1.025, 4.62, 95.1), (0.857, 8.65, 359.1), (0.792, 15.54, 187.0), (0.633, 7.26, 98.1)]),
    (2.097, 5, [(1.248, 13.63, 323.3), (1.087, 4.61, 93.9), (0.855, 8.64, 359.6), (0.784, 15.49, 186.8), (0.558, 7.35, 98.5)]),
]


def test_the_real_fixture_every_hour_pinned():
    sp = spc2d.read(fixture("51003.spc2d.txt"))
    got = []
    for _t, rows in sp.steps:
        r = P.partition(sp.freqs, sp.dirs, np.array(rows))
        got.append((round(r["hs"], 3), r["n"], [(round(p["hs"], 3), round(p["tp"], 2), round(p["dir"], 1))
                                               for p in r["parts"]]))
    assert got == GOLDEN


def test_the_rules_constants():
    assert (P.SADDLE, P.SMALL_FRACTION, P.SMALL_HS, P.MAX_PARTS, P.MIN_HS) == (1.0, 0.02, 0.15, 6, 0.05)


def test_the_small_rule_needs_both_a_small_share_and_a_small_height():
    """A system is merged only when it is under 2 % of the energy AND under 0.15 m: one condition alone keeps it."""
    big = gaussian(3.0, 14.0, 320.0)
    # 0.14 m on 3.0 m: 0.2 % of the energy and under 0.15 m -> merged into its neighbour
    assert P.partition(FREQS, DIRS, big + gaussian(0.14, 6.0, 140.0))["n"] == 1
    # 0.16 m on 3.0 m: 0.3 % but over 0.15 m -> kept
    assert P.partition(FREQS, DIRS, big + gaussian(0.16, 6.0, 140.0))["n"] == 2
    # 0.12 m on 0.6 m: under 0.15 m but 4 % of the energy -> kept
    assert P.partition(FREQS, DIRS, gaussian(0.6, 14.0, 320.0) + gaussian(0.12, 6.0, 140.0))["n"] == 2


def test_ties_are_rounding_proof_and_go_to_the_higher_index():
    """G41a P3-1: two sums equal but for float rounding compare equal; a plateau's bins follow its highest flat index."""
    a = np.array([0.1 + 0.2, 0.3])
    assert a[0] != a[1] and P._quantize(a)[0] == P._quantize(a)[1]
    assert P._quantize(np.array([0.0, 1e-300]))[0] == 0.0 and P._quantize(np.array([1e-300]))[0] > 0
    S = np.zeros((5, 8))
    S[2, 3] = S[2, 4] = 1.0                                  # a two-bin plateau
    lab = P._labels(S)
    assert lab[2, 3] == lab[2, 4] == 2 * 8 + 4


def test_directions_must_go_evenly_round_the_circle():
    E = gaussian(2.0, 14.0, 320.0)
    rev = P.partition(FREQS, DIRS[::-1], E[:, ::-1])          # the other sense is fine
    assert rev["n"] == 1 and rev["parts"][0]["dir"] == pytest.approx(320.0, abs=1.0)
    shuffled = DIRS[:]
    shuffled[3], shuffled[7] = shuffled[7], shuffled[3]
    with pytest.raises(ValueError):
        P.partition(FREQS, shuffled, E)
    with pytest.raises(ValueError):
        P.partition(FREQS, DIRS[:-1] + [DIRS[-1] + 5], E)


def test_a_system_from_due_north_reads_0_not_360():
    r = P.partition(FREQS, DIRS, gaussian(2.0, 14.0, 0.0))
    assert 0.0 <= r["parts"][0]["dir"] < 360.0 and min(r["parts"][0]["dir"], 360 - r["parts"][0]["dir"]) < 0.5


def test_tp_comes_from_the_per_hz_spectrum():
    """On log-spaced bins a per-bin sum would move the peak to longer periods: the per-Hz spectrum keeps it."""
    r = P.partition(FREQS, DIRS, gaussian(2.0, 6.0, 90.0, sf=0.15))
    assert r["parts"][0]["tp"] == pytest.approx(6.0, rel=0.02)


def test_real_ties_follow_the_rule_not_float_rounding():
    """G41a P3-1, the two real cases: aer 46001 (a bin with two neighbours of the same integer sum) and aer 46060 (two
    equal frequency peaks: the lower frequency, Tp 2.24 s, not 1.89 s). The values equal an exact-integer reference."""
    def parts(name):
        sp = spc2d.read(fixture(name))
        r = P.partition(sp.freqs, sp.dirs, np.array(sp.steps[0][1]))
        return [(round(p["hs"], 3), round(p["tp"], 2), round(p["dir"], 1)) for p in r["parts"]]
    assert parts("aer_46001_tie.txt") == [(0.966, 8.53, 255.5), (0.737, 9.46, 169.6), (0.554, 6.12, 330.1),
                                          (0.259, 18.09, 194.8)]
    assert parts("aer_46060_tptie.txt") == [(0.429, 9.06, 178.4), (0.237, 2.19, 102.0), (0.18, 2.44, 193.0),
                                            (0.108, 2.24, 338.3)]


def test_reader_header_limits():
    text = fixture("51003.spc2d.txt").decode()
    head, rest = text.split("20261028.000000", 1)
    block = "20261028.000000" + rest.split("20261028.010000")[0]
    with pytest.raises(ValueError, match="over"):
        spc2d.read((head + block).encode() + b"\n" * spc2d.MAX_BYTES)          # bytes over the cap
    with pytest.raises(ValueError, match="line"):
        spc2d.read(head.replace("$", "$" + "x" * spc2d.MAX_LINE, 1) + block)
    freq = head.split("AFREQ", 1)[1].split("\n")[3]                           # the 2nd frequency
    with pytest.raises(ValueError, match="ascending"):
        spc2d.read(head.replace(freq, head.split("AFREQ", 1)[1].split("\n")[2], 1) + block)
    with pytest.raises(ValueError, match="frequency count"):
        spc2d.read(head.replace("    37", "     1", 1) + block)
