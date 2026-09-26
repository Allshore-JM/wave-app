"""The compact forecast table for the forecast window (plan section 25) and its opt-in on /api/forecast.
The classic table (the page as served today) is pinned by tests/test_spec_wind.py and the page golden."""
import re

import pytest

import app as A


def _row(date="Saturday, September 26, 2026", time="2:00 PM", wind=(7.5, 76)):
    r = [date, time]
    for _ in range(6):
        r += [3.2, 12.5, 305]
    r += [wind[0], wind[1], 4.1]                    # row[20] wind m/s, row[21] wind dir, row[-1] combined
    return r


@pytest.mark.parametrize("s, out", [
    ("Saturday, September 26, 2026", "Sat 9/26"),
    ("Wednesday, January 7, 2026", "Wed 1/7"),
    ("Tuesday, December 31, 2030", "Tue 12/31"),
    ("9/26/26", "9/26/26"),                         # anything else passes through unchanged
    ("", ""),
    (None, None),
])
def test_short_date(s, out):
    assert A._short_date(s) == out


def test_compact_table_drops_info_rows_padding_and_long_dates():
    rows = [_row(), _row(time="9:00 PM")]
    classic = A.build_html_table("Cycle    : 20260926 12 UTC", "Location : 51201", None, rows, "Pacific/Honolulu", "US")
    compact = A.build_html_table("Cycle    : 20260926 12 UTC", "Location : 51201", None, rows, "Pacific/Honolulu", "US",
                                 compact=True)
    # the classic table is untouched
    assert 'colspan="23" class="forecast-info"' in classic and "padding:4px 8px" in classic
    assert ">Saturday, September 26, 2026</td>" in classic and "Dir<br>(d)" in classic
    # compact
    assert '<table class="table table-bordered table-sm forecast-compact">' in compact
    assert "forecast-info" not in compact and "Cycle" not in compact and "Location" not in compact
    assert "padding:" not in compact
    assert compact.count('title="Saturday, September 26, 2026">Sat 9/26</td>') == 2
    assert compact.count("Dir<br>(&deg;)") == 6 and "Dir<br>(d)" not in compact
    # the same cells otherwise: bold day row, dashed night row, values, wind
    assert "border:1px solid #000;" in compact and "border:1px dashed #999;" in compact
    assert compact.count(">3.20</td>") == 12 and ">17</td>" in compact and "76&deg; ENE" in compact
    strip = lambda h: re.sub(r"<[^>]+>", "|", h)
    assert strip(compact).count("|") < strip(classic).count("|")          # the info rows are gone, nothing added


def test_compact_title_is_escaped():
    html = A.build_html_table("", "", None, [_row(date='x" onmouseover="alert(1)')], "UTC", "US", compact=True)
    assert 'onmouseover="alert' not in html and "&quot;" in html


def test_api_forecast_compact_is_opt_in(monkeypatch):
    rows = [_row()]

    def parse(station, tz):
        return ("Cycle    : 20260926 12 UTC", "Location : 51201      (21.67N 158.12W)", None, [list(r) for r in rows],
                "Pacific/Honolulu", None)
    monkeypatch.setattr(A, "parse_bull", parse)
    c = A.app.test_client()
    classic = c.get("/api/forecast?station=51201").get_json()
    compact = c.get("/api/forecast?station=51201&compact=1").get_json()
    assert "forecast-info" in classic["table_html"] and "forecast-compact" not in classic["table_html"]
    assert "forecast-compact" in compact["table_html"] and "Sat 9/26" in compact["table_html"]
    assert compact["graph_header"] == classic["graph_header"] and compact["graph_header"]["cycle"] == "20260926 12 UTC"
    assert compact["graph_data"]["labels"] == classic["graph_data"]["labels"]      # graph labels keep the long form
    assert compact["graph_data"]["labels"][0].startswith("Saturday, September 26, 2026")
