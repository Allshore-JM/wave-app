"""Fixture tests for the PacIOOS SWAN bulletin parser (_parse_swan_table_text).

Pure-function tests: no network. The fixture is a truncated real
buoy.cdip106.table (station 51201, Waimea Bay) with NaN hand-injected into the
14:00 row's PT06 columns to exercise the NaN path (real in the P1_buoy flavor
and a guard against format drift).

Run:  pytest tests/
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import _parse_swan_table_text  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "fixtures", "swan_buoy_sample.table")
M_TO_FT = 3.28084
TZ = "Pacific/Honolulu"


def _parse(updated=datetime(2026, 7, 3, 23)):
    with open(FIXTURE) as f:
        text = f.read()
    return _parse_swan_table_text(text, "51201", TZ, updated)


def test_parses_cleanly():
    cycle, loc, model_run, rows, tz, err = _parse()
    assert err is None
    assert rows
    assert tz == TZ
    assert model_run is None  # never rendered; parser returns None


def test_spinup_skipped_by_timestamp():
    # Fixture holds 15 hourly rows 06:00-20:00 UTC. Rows with
    # t < first_ts + 6h (06:00-11:00) must be dropped -> 9 rows kept.
    _, _, _, rows, _, _ = _parse()
    assert len(rows) == 9
    # First kept row is 12:00 UTC == 2:00 AM HST, Friday July 3 2026.
    assert rows[0][1] == "2:00 AM"
    assert rows[0][0] in ("Friday, July 3, 2026", "Friday, July 03, 2026")  # win fallback


def test_row_shape_matches_gfs_contract():
    # [date, time, 6 x (hs, tp, dir), wind_spd, wind_dir, combined] == 23
    # columns, like parse_bull. Wind sits at 20-21 so combined stays row[-1].
    _, _, _, rows, _, _ = _parse()
    assert all(len(r) == 23 for r in rows)
    assert all(isinstance(r[0], str) and isinstance(r[1], str) for r in rows)


def test_empty_windsea_slot_compacts_left():
    # PT01 in the fixture is the unused wind-sea slot (0.0 Hs / 0.0 Tp). Empty
    # partitions are dropped and the rest compact LEFT, so Swell 1 carries the
    # dominant partition (PT02) and the trailing group renders blank -- the
    # GFS .bull convention of energy-ordered groups starting at column 1.
    _, _, _, rows, _, _ = _parse()
    r = rows[0]  # 12:00 UTC: PT01 empty, PT02-06 real -> s1-s5 filled, s6 blank
    assert r[2] is not None            # Swell 1 = PT02 (dominant)
    assert r[2 + 5 * 3] is None        # Swell 6 blank after compaction


def test_meters_to_feet_and_rounding():
    # 12:00 UTC row: PT02 (-> Swell 1 after compaction):
    # Hs 1.27611 m -> 4.19 ft (2dp); Tp 7.9022 -> 7.9 (1dp).
    _, _, _, rows, _, _ = _parse()
    r = rows[0]
    assert r[2] == round(1.27611 * M_TO_FT, 2) == 4.19
    assert r[3] == 7.9
    # Combined = Hsig 1.31091 m -> 4.3 ft.
    assert r[-1] == round(1.31091 * M_TO_FT, 2)


def test_direction_not_flipped():
    # SWAN reports direction FROM true north; the GFS .bull (+180)%360 flip
    # must NOT be applied. 12:00 UTC row (post-compaction):
    # Swell 1 (PT02) 36.79deg -> 37 (not ~217); Swell 2 (PT03) 308.244 -> 308
    # (not ~128).
    _, _, _, rows, _, _ = _parse()
    r = rows[0]
    assert r[4] == 37
    assert r[7] == 308
    # And an integer, matching the GFS rounding convention.
    assert isinstance(r[4], int)


def test_nan_partition_is_blank():
    # 14:00 UTC row has NaN injected into all three PT06 columns; PT01 is the
    # empty wind-sea slot -> 4 real partitions -> s1-s4 filled, s5+s6 blank.
    _, _, _, rows, _, _ = _parse()
    r = rows[2]  # 12:00, 13:00, 14:00 -> index 2
    assert r[1] == "4:00 AM"  # 14:00 UTC == 4:00 AM HST
    assert r[2 + 3 * 3] is not None    # Swell 4 real
    assert r[2 + 4 * 3] is None        # Swell 5 blank
    assert r[2 + 5 * 3] is None        # Swell 6 blank


def test_location_hemisphere_formatting():
    # station_coords.json stores 51201 as lat 21.67, lon -158.12 (signed).
    # The header must render "(21.67N 158.12W)" -- never a raw negative.
    _, loc, _, _, _, _ = _parse()
    assert "(21.67N 158.12W)" in loc
    assert "-158" not in loc


def test_cycle_label_from_updated_timestamp():
    cycle, _, _, _, _, _ = _parse(updated=datetime(2026, 7, 3, 23))
    assert cycle == "Cycle : PacIOOS SWAN updated 20260703 23 UTC"
    # Fail-soft when Last-Modified was unavailable.
    cycle2, _, _, _, _, _ = _parse(updated=None)
    assert cycle2 == "Cycle : PacIOOS SWAN (latest run)"


def test_malformed_table_fails_soft():
    bad = "% not a swan header\n20260703.060000 1.0\n"
    cycle, loc, mr, rows, tz, err = _parse_swan_table_text(bad, "51201", TZ, None)
    assert rows is None
    assert err and "unexpected format" in err


def _fixture_lines():
    with open(FIXTURE) as f:
        return f.read().splitlines()


def test_nan_direction_alone_keeps_partition_blank_dir():
    # A partition with valid Hs/Tp but NaN direction must survive (not drop the
    # whole partition) and render a blank Dir cell. Exercises the
    # `int(round(dr)) if dr is not None else None` arm, unreachable via the
    # all-NaN fixture row.
    lines = _fixture_lines()
    for i, l in enumerate(lines):
        if l.startswith("20260703.150000"):
            parts = l.split()
            parts[19] = "NaN"  # DrPT02: 6 lead cols + Hs*6 + Tp*6 -> index 18; DrPT02 = 19
            lines[i] = "  ".join(parts)
    _, _, _, rows, _, err = _parse_swan_table_text("\n".join(lines), "51201", TZ, None)
    assert err is None
    r = rows[3]  # 15:00 UTC row (12,13,14,15 -> index 3)
    assert r[1] == "5:00 AM"
    # After compaction, Swell 1 = PT02: Hs/Tp kept, Dir blanked.
    assert r[2] is not None and r[3] is not None
    assert r[4] is None


def test_infinite_value_rejected_not_crash():
    # A corrupt 'inf' direction must NOT raise (int(round(inf)) -> OverflowError);
    # the partition degrades to a blank cell like NaN. Guards the fail-soft
    # invariant for the no-SLA PacIOOS feed.
    lines = _fixture_lines()
    for i, l in enumerate(lines):
        if l.startswith("20260703.150000"):
            parts = l.split()
            parts[19] = "inf"  # DrPT02
            lines[i] = "  ".join(parts)
    _, _, _, rows, _, err = _parse_swan_table_text("\n".join(lines), "51201", TZ, None)
    assert err is None
    r = rows[3]
    assert r[4] is None  # inf direction -> blank, no crash


def test_spinup_skip_is_timestamp_based_across_gaps():
    # With hours missing INSIDE the spin-up window, the 6h cutoff must key on
    # timestamps (t < first_ts + 6h), not row count. Rows: 06,07,10,11,12,13,14
    # UTC (08:00/09:00 missing) -> keep exactly the >=12:00 rows (12,13,14).
    hdr = [l for l in _fixture_lines() if l.strip().startswith("%")]
    body = [l for l in _fixture_lines()
            if l.strip() and not l.strip().startswith("%")]
    tmpl = body[0]  # any full-width data row as a template

    def at(hour):
        parts = tmpl.split()
        parts[0] = f"20260703.{hour:02d}0000"
        return "  ".join(parts)

    gapped = "\n".join(hdr + [at(h) for h in (6, 7, 10, 11, 12, 13, 14)])
    _, _, _, rows, _, err = _parse_swan_table_text(gapped, "51201", TZ, None)
    assert err is None
    # 3 kept (12,13,14 UTC = 2/3/4 AM HST); a row-count skip would keep only 1.
    assert len(rows) == 3
    assert [r[1] for r in rows] == ["2:00 AM", "3:00 AM", "4:00 AM"]


def test_truncated_row_skipped_silently():
    # A short row (partial write / truncated download of the once-daily bulletin)
    # is dropped, not a crash (guard: len(parts) < len(header_cols)).
    text = "\n".join(_fixture_lines())
    text += "\n20260703.210000  1.35  6.4  27.0  8.2  42.5  0.0  1.28\n"  # 8 tokens
    _, _, _, rows, _, err = _parse_swan_table_text(text, "51201", TZ, None)
    assert err is None
    assert len(rows) == 9  # truncated row skipped; the 9 well-formed rows unaffected


def test_wind_join_populates_swan_cells():
    # A wind dict covering some fixture hours populates row[20]/row[21] for
    # those rows; uncovered hours stay blank. Fixture rows are 12:00-20:00 UTC
    # July 3 2026 (after the 6h spin-up skip).
    from datetime import datetime
    wind = {
        datetime(2026, 7, 3, 12): (8.09, 71.6),
        datetime(2026, 7, 3, 13): (8.03, 68.8),
    }
    with open(FIXTURE) as f:
        text = f.read()
    _, _, _, rows, _, err = _parse_swan_table_text(text, "51201", TZ, None, wind=wind)
    assert err is None
    assert rows[0][20] == 8.09 and rows[0][21] == 72   # 12:00 UTC covered
    assert rows[1][20] == 8.03 and rows[1][21] == 69   # 13:00 UTC covered
    assert rows[2][20] is None and rows[2][21] is None  # 14:00 UTC uncovered
    # combined unaffected by the join
    assert rows[0][-1] == round(1.31091 * M_TO_FT, 2)


def test_no_wind_blank_cells():
    # Default wind=None -> every row has blank wind cells (models the
    # pre-cycle SWAN hindcast hours and a total spec-fetch failure).
    _, _, _, rows, _, err = _parse()
    assert err is None
    assert all(r[20] is None and r[21] is None for r in rows)
