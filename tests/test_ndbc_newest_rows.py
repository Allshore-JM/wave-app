"""Release G2: the NDBC components route parses only the rows it uses; results are unchanged.

Reference = the OLD helpers copied here verbatim (full-file parse), so the equivalence does not
depend on the helpers still living in app.py. Plus the existing components golden
(tests/test_ndbc_components_parallel.py) which must remain byte-identical.
"""
import os
import random
import re
import sys
import tracemalloc
from datetime import datetime

import pytest
import pytz

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, HERE)

import app as A  # noqa: E402


# ----------------------------- OLD implementation (reference) -----------------------------

def _old_parse_file(text):
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 7:
            continue
        try:
            yy = int(parts[0]); mm = int(parts[1]); dd = int(parts[2])
            hh = int(parts[3]); minute = int(parts[4])
        except Exception:
            continue
        year = 2000 + yy if yy < 100 else yy
        timestamp = datetime(year, mm, dd, hh, minute, tzinfo=pytz.utc)
        pairs = re.findall(r"([-+]?\d+(?:\.\d+)?|MM)\s*\(([-+]?\d+(?:\.\d+)?)\)", line)
        freqs, vals = [], []
        for value, freq in pairs:
            if value == "MM":
                continue
            try:
                vals.append(float(value)); freqs.append(float(freq))
            except Exception:
                continue
        if freqs and vals:
            rows.append({"timestamp_utc": timestamp, "freqs": freqs, "values": vals})
    return rows


def _old_latest(rows):
    return max(rows, key=lambda r: r["timestamp_utc"]) if rows else None


def _old_match(density_row, direction_rows):
    for row in direction_rows:
        if row["timestamp_utc"] == density_row["timestamp_utc"]:
            return row
    return _old_latest(direction_rows)


def _old_route_semantics(density_text, optional_text):
    """What the old components route derived: latest density row, matched optional row."""
    rows = _old_parse_file(density_text)
    density = _old_latest(rows) if rows else None
    if density is None:
        return None, None
    orows = _old_parse_file(optional_text) if optional_text else []
    return density, (_old_match(density, orows) if orows else None)


def _new_route_semantics(density_text, optional_text):
    density = A._latest_spectral_row_from_text(density_text)
    if density is None:
        return None, None
    return density, (A._match_direction_row_from_text(optional_text, density["timestamp_utc"])
                     if optional_text else None)


# ----------------------------- synthetic files -----------------------------------------

def _line(ts, kind, rng):
    head = f"{ts.year} {ts.month:02d} {ts.day:02d} {ts.hour:02d} {ts.minute:02d} 9.999"
    if kind == "ok":
        return head + " " + " ".join(f"{rng.random():.3f} ({0.025 + 0.005 * i:.3f})" for i in range(rng.randint(1, 47)))
    if kind == "mm":                                   # all missing -> NOT a valid row
        return head + " " + " ".join(f"MM ({0.025 + 0.005 * i:.3f})" for i in range(20))
    if kind == "short":
        return head                                    # < 7 fields
    if kind == "badint":
        return "20x6 09 09 12 00 9.999 0.1 (0.025)"
    if kind == "comment":
        return "#YY  MM DD hh mm Sep_Freq  < spec_1 (freq_1) ..."
    if kind == "blank":
        return "   "
    raise ValueError(kind)


def _synthetic(rng):
    base = datetime(2026, 9, 9, 21, 0, tzinfo=pytz.utc)
    stamps = [base.replace(hour=h % 24, day=9 - h // 24) for h in range(rng.randint(1, 30))]
    if rng.random() < 0.5:
        rng.shuffle(stamps)                            # not newest-first
    if rng.random() < 0.5 and len(stamps) > 2:
        stamps.insert(1, stamps[0])                    # duplicate timestamp
    lines = ["#YY  MM DD hh mm Sep_Freq  < spec_1 (freq_1) spec_2 (freq_2) ... >"]
    kinds = ["ok"] * 6 + ["mm", "short", "badint", "comment", "blank"]
    for ts in stamps:
        lines.append(_line(ts, rng.choice(kinds), rng))
    if rng.random() < 0.3:
        lines.append(_line(stamps[0], "ok", rng))      # a late valid row for the first stamp
    return "\n".join(lines) + ("\n" if rng.random() < 0.5 else "")


@pytest.mark.parametrize("seed", range(200))
def test_equivalent_to_old_full_parse(seed):
    rng = random.Random(seed)
    density = _synthetic(rng)
    optional = _synthetic(random.Random(seed + 10_000)) if rng.random() < 0.9 else ""
    if rng.random() < 0.2 and optional:                # optional file shares the density stamps
        optional = density.replace("9.999", "8.888")
    assert _new_route_semantics(density, optional) == _old_route_semantics(density, optional)


def test_no_valid_rows_and_empty():
    assert A._latest_spectral_row_from_text("") is None
    assert A._latest_spectral_row_from_text("#header only\n") is None
    only_mm = "2026 09 09 21 00 9.999 MM (0.025) MM (0.030)\n"
    assert A._latest_spectral_row_from_text(only_mm) is None
    assert _old_latest(_old_parse_file(only_mm)) is None


def test_invalid_calendar_date_raises_like_the_old_parser():
    bad = "2026 13 40 21 00 9.999 0.1 (0.025)\n"
    with pytest.raises(ValueError):
        _old_parse_file(bad)
    with pytest.raises(ValueError):
        A._latest_spectral_row_from_text(bad)


def test_real_file_peak_is_small():
    txt = open(os.path.join(HERE, "fixtures", "ndbc_51201", "51201.data_spec"), encoding="utf-8").read()
    # inflate the 12-line fixture to the real ~2,000-row size by repeating older rows
    lines = txt.splitlines()
    body = lines[1:]
    big = [lines[0]]
    for k in range(2000 // len(body) + 1):
        for l in body:
            big.append(l.replace("2026 09 09", "2026 09 %02d" % max(1, 9 - k)))
    big_txt = "\n".join(big) + "\n"
    assert len(big_txt) > 1_000_000
    tracemalloc.start(); tracemalloc.reset_peak()
    old = _old_latest(_old_parse_file(big_txt)); old_peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.reset_peak()
    new = A._latest_spectral_row_from_text(big_txt); new_peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    assert new == old
    assert old_peak > 5_000_000, old_peak
    # new path holds only the split lines (~file size) + one timestamp per line
    assert new_peak < 3_000_000 and new_peak < old_peak / 2, (new_peak, old_peak)
