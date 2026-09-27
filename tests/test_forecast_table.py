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

def test_compact_table_abbreviates_the_combined_header_only():
    compact = A.build_html_table("c", "l", "", [], "Pacific/Honolulu", "US", compact=True)
    full = A.build_html_table("c", "l", "", [], "Pacific/Honolulu", "US")
    assert '<abbr title="Combined sea" aria-label="Combined">Comb.</abbr>' in compact and ">Combined</th>" not in compact
    assert ">Combined</th>" in full and "Comb." not in full

def _sparse_row(present, hs=2.5):
    """A 23-column row with swell groups only at the given indices (0-5)."""
    r = ["Saturday, September 26, 2026", "2:00 PM"]
    for g in range(6):
        r += [hs, 11.0 + g, 300 - g] if g in present else [None, None, None]
    return r + [6.0, 80, 3.9]


def test_groups_limit_the_compact_table_to_the_swells_present_keeping_their_colours_and_numbers():
    rows = [_sparse_row({0, 2}), _sparse_row({0})]
    html = A.build_html_table("c", "l", "", rows, "Pacific/Honolulu", "US", compact=True, groups=[0, 2])
    assert re.findall(r">Swell (\d)</th>", html) == ["1", "3"]           # bulletin partition numbers kept
    assert "#C00000" in html and "#FFC000" in html and "#ED7D31" not in html  # each group keeps its own colour
    body = html.split("<tbody>")[1].split("</tr>")[0]
    assert body.count("<td") == 2 + 3 * 2 + 1 + 2                        # date, time, 2 groups, combined, wind
    assert ">13.0<" in body and ">2.50<" in body                         # group 3's Tp read from its own columns
    full = A.build_html_table("c", "l", "", rows, "Pacific/Honolulu", "US")   # the classic table keeps all six
    assert re.findall(r">Swell (\d)</th>", full) == ["1", "2", "3", "4", "5", "6"]


def test_the_payload_names_the_swells_present_for_the_window_only(monkeypatch):
    rows = [_sparse_row({0, 1}), _sparse_row({0, 3})]
    monkeypatch.setattr(A, "parse_bull", lambda station, tz: ("Cycle : 20260926 06 UTC", "Location : 21.67N 158.12W", "", rows, "Pacific/Honolulu", None))
    d = A.compute_forecast_payload("46001", None, "US", "GFS", compact=True)
    assert d["graph_data"]["swells"] == ["s1", "s2", "s4"]
    assert re.findall(r">Swell (\d)</th>", d["table_html"]) == ["1", "2", "4"]
    classic = A.compute_forecast_payload("46001", None, "US", "GFS")
    assert len(re.findall(r">Swell (\d)</th>", classic["table_html"])) == 6
    monkeypatch.setattr(A, "parse_bull", lambda station, tz: ("Cycle : x", "Location : y", "", [_sparse_row(set())], "UTC", None))
    assert A.compute_forecast_payload("46001", None, "US", "GFS", compact=True)["graph_data"]["swells"] == ["s1"]   # never none at all

def test_the_table_takes_the_swell_groups_of_the_first_7_days_the_graphs_every_group(monkeypatch):
    from datetime import datetime, timedelta
    t0 = datetime(2026, 9, 26, 14, 0)
    def row(h, present):
        t = t0 + timedelta(hours=h)
        r = _sparse_row(present)
        r[0] = t.strftime("%A, %B %d, %Y").replace(" 0", " "); r[1] = t.strftime("%I:%M %p").lstrip("0")
        return r
    rows = [row(h, {0, 1}) for h in range(0, 168)] + [row(h, {0, 1, 2, 3, 4, 5}) for h in range(168, 385, 3)]
    monkeypatch.setattr(A, "parse_bull", lambda station, tz: ("Cycle : x", "Location : y", "", rows, "UTC", None))
    d = A.compute_forecast_payload("46001", None, "US", "GFS", compact=True)
    assert re.findall(r">Swell (\d)</th>", d["table_html"]) == ["1", "2"], "components only after day 7 earn no column"
    assert d["graph_data"]["swells"] == ["s1", "s2", "s3", "s4", "s5", "s6"], "the graphs keep every component"
    rows[167] = row(167, {0, 1, 2})                                   # the last hour of day 7 counts
    assert re.findall(r">Swell (\d)</th>", A.compute_forecast_payload("46001", None, "US", "GFS", compact=True)["table_html"]) == ["1", "2", "3"]
    assert A._swell_groups([["?", "?"] + [1.0, 2, 3] + [None] * 15 + [0, 0, 1]] * 3, days=7) == [0]   # unreadable times: by position
