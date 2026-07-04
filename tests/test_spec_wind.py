"""Pure tests for the GFS .spec wind parser, join helper, compass, and the
Wind column rendering -- no network.

Fixture: tests/fixtures/gfswave_spec_sample.spec -- a REAL trimmed
gfswave.51201.spec (12z 2026-07-04 cycle): WW3 header (incl. its own quoted
title line) + 8 hourly timestep blocks, spectra truncated to 2 lines each.
Blocks 1-7 carry the real run-together negative-lon station line
('51201     '  21.67-158.12 ...); block 8 was hand-edited to a positive-lon
form to cover both layouts.
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app import (_parse_spec_wind_text, _wind_row_cells, _compass16,  # noqa: E402
                 build_html_table)

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures",
                       "gfswave_spec_sample.spec")


def _load_map():
    with open(FIXTURE) as f:
        return _parse_spec_wind_text(f)


def test_parses_all_timesteps():
    d = _load_map()
    assert len(d) == 8
    assert datetime(2026, 7, 4, 12) in d
    assert datetime(2026, 7, 4, 19) in d
    assert all(isinstance(k, datetime) for k in d)


def test_known_u10_udir_values():
    d = _load_map()
    assert d[datetime(2026, 7, 4, 12)] == (8.09, 71.6)
    assert d[datetime(2026, 7, 4, 13)] == (8.03, 68.8)


def test_negative_lon_run_together():
    # Blocks with '21.67-158.12' (no space) must still yield U10/Udir as float
    # indices 3/4 -- the float regex splits the run-together pair correctly.
    d = _load_map()
    u10, udir = d[datetime(2026, 7, 4, 12)]
    assert u10 == 8.09 and udir == 71.6  # NOT lat/lon/depth values


def test_positive_lon_layout():
    d = _load_map()
    assert d[datetime(2026, 7, 4, 19)] == (12.34, 245.0)


def test_direction_from_convention_no_flip():
    # Udir is degrees FROM true north (verified vs live buoy obs). The join
    # helper must pass it through -- 71.6 -> 72, explicitly NOT 252.
    cells = _wind_row_cells(_load_map(), datetime(2026, 7, 4, 12))
    assert cells == [8.09, 72]
    assert cells[1] != 252


def test_header_quoted_line_ignored():
    # The WW3 title line is quoted too, but precedes any datetime line.
    with open(FIXTURE) as f:
        header_only = [l for l in f][:10]  # header lines, no datetime yet
    header_only = [l for l in header_only if not l.strip()[:8].isdigit()]
    assert _parse_spec_wind_text(header_only) == {}


def test_spectra_lines_tolerated():
    d = _parse_spec_wind_text([
        "20260704 120000",
        "'X         '  21.67-158.12   472.7   5.00  90.0   0.0  0.0",
        "  0.176E-09  0.760E-08  0.231E-08",   # spectra junk
        "  123 456 789",                        # more junk (not a datetime)
    ])
    assert d == {datetime(2026, 7, 4, 12): (5.0, 90.0)}


def test_only_first_quoted_line_per_block():
    d = _parse_spec_wind_text([
        "20260704 120000",
        "'X'  21.67-158.12  472.7  5.00  90.0  0.0  0.0",
        "'Y'  21.67-158.12  472.7  9.99  180.0  0.0  0.0",  # must be ignored
    ])
    assert d == {datetime(2026, 7, 4, 12): (5.0, 90.0)}


def test_malformed_spec_fails_soft():
    assert _parse_spec_wind_text([]) == {}
    assert _parse_spec_wind_text(["garbage", "'quoted junk'", "99999999 999999"]) == {}
    # station line with too few floats -> skipped, no raise
    assert _parse_spec_wind_text(["20260704 120000", "'X' 1.0 2.0"]) == {}


def test_bytes_lines_tolerated():
    # iter_lines can yield bytes when the server sends no charset.
    d = _parse_spec_wind_text([
        b"20260704 120000",
        b"'X'  21.67-158.12  472.7  5.00  90.0  0.0  0.0",
    ])
    assert d == {datetime(2026, 7, 4, 12): (5.0, 90.0)}


def test_compass16_edges():
    assert _compass16(0) == "N"
    assert _compass16(11.2) == "N"
    assert _compass16(11.25) == "NNE"
    assert _compass16(68) == "ENE"
    assert _compass16(190) == "S"
    assert _compass16(348.74) == "NNW"
    assert _compass16(348.75) == "N"
    assert _compass16(359) == "N"
    assert _compass16(720) == "N"  # modulo


def test_wind_row_cells_join():
    wind = {datetime(2026, 7, 4, 12): (8.09, 71.6)}
    # exact-hour hit (minutes/seconds floored)
    assert _wind_row_cells(wind, datetime(2026, 7, 4, 12, 0)) == [8.09, 72]
    assert _wind_row_cells(wind, datetime(2026, 7, 4, 12, 30, 15)) == [8.09, 72]
    # missing hour / empty map
    assert _wind_row_cells(wind, datetime(2026, 7, 4, 13)) == [None, None]
    assert _wind_row_cells({}, datetime(2026, 7, 4, 12)) == [None, None]
    assert _wind_row_cells(wind, None) == [None, None]
    # non-finite u10 rejected
    bad = {datetime(2026, 7, 4, 12): (float("inf"), 90.0)}
    assert _wind_row_cells(bad, datetime(2026, 7, 4, 12)) == [None, None]
    # non-finite udir must NOT raise (int(round(inf)) -> OverflowError); u10 kept,
    # direction blanked.
    bad_dir = {datetime(2026, 7, 4, 12): (5.0, float("inf"))}
    assert _wind_row_cells(bad_dir, datetime(2026, 7, 4, 12)) == [5.0, None]


def _mk_row(wspd, wdir, combined=4.17):
    # [date, time, 6x(hs,tp,dir)=18, wspd, wdir, combined] = 23 cols
    return (["Friday, July 4, 2026", "2:00 PM"]
            + [3.9, 7.9, 46] + [None] * 15
            + [wspd, wdir, combined])


def test_build_html_table_wind_rendering_us():
    html = build_html_table("Cycle : x", "Location : x", None,
                            [_mk_row(8.09, 72)], "Pacific/Honolulu", "US")
    assert 'colspan="23"' in html
    assert ">Wind</th>" in html
    assert "Spd<br>(mph)" in html
    assert ">18</td>" in html                 # 8.09 m/s -> 18 mph
    assert "72&deg; ENE" in html


def test_build_html_table_wind_rendering_metric():
    html = build_html_table("Cycle : x", "Location : x", None,
                            [_mk_row(8.09, 72)], "Pacific/Honolulu", "Metric")
    assert "Spd<br>(km/h)" in html
    assert ">29</td>" in html                 # 8.09 m/s -> 29 km/h


def test_build_html_table_blank_wind_cells():
    html = build_html_table("Cycle : x", "Location : x", None,
                            [_mk_row(None, None)], "Pacific/Honolulu", "US")
    # combined still renders; wind cells empty
    assert ">4.17</td>" in html
    assert "&deg;" not in html.split("</thead>")[1]  # no direction in tbody
