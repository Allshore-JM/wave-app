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
    # the same cells otherwise (no sky given: the clock rule's day row and night row, as classes for the page CSS;
    # the window's cells carry no inline weight or border), values, wind
    assert '<tr class="sky-day"><td class="col-date"' in compact and '<tr class="sky-night"><td class="col-date"' in compact
    assert "border:" not in compact.split("<tbody>")[1] and "font-weight" not in compact.split("<tbody>")[1]
    assert compact.count(">3.20</td>") == 12 and ">17</td>" in compact and "76&deg; ENE" in compact
    no_arrows = re.sub(r' <span class="dir-arrow"[^>]*>[^<]*</span>', "", compact)
    strip = lambda h: re.sub(r"<[^>]+>", "|", h)
    assert strip(no_arrows).count("|") < strip(classic).count("|")        # the info rows are gone, only arrows added


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

def test_the_combined_column_is_named_significant_wave_height():
    """'Comb.' was the significant wave height of the combined seas (bulletin Hst, SWAN Hsig, point HTSGW)."""
    compact = A.build_html_table("c", "l", "", [], "Pacific/Honolulu", "US", compact=True)
    full = A.build_html_table("c", "l", "", [], "Pacific/Honolulu", "US")
    assert ('<abbr title="Significant wave height of the combined seas" aria-label="Significant wave height">'
            'Sig. Wave<br>Height</abbr>') in compact
    assert ">Significant Wave Height</th>" in full
    for html in (compact, full):
        assert "Comb." not in html and "Combined<" not in html

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


# ------------------------------ the real sky (plan section 35) ------------------------------
def _sky(state, events=(), moon=None, day_first=False, now=False):
    from datetime import datetime
    return {"state": state, "day_first": day_first, "now": now, "moon": moon,
            "events": [{"kind": k, "text": t, "name": n, "time": datetime(2026, 9, 26, h, m)} for k, t, n, h, m in events]}


def test_compact_table_with_sky_shades_rows_by_the_real_sky_and_adds_the_sun_column():
    rows = [_row(time="5:00 AM"), _row(time="6:00 AM"), _row(time="2:00 PM"), _row(time="9:00 PM")]
    moon = {"glyph": "\U0001F314", "pct": 72, "name": "Waxing gibbous"}
    sky = [_sky("night", moon=moon, day_first=True), _sky("night", [("dawn", "\u25D0 6:02", "First light", 6, 2),
                                                                    ("sunrise", "\u2600\u2191 6:24", "Sunrise", 6, 24)]),
           _sky("day", now=True), _sky("night", moon=moon)]
    html = A.build_html_table("c", "l", "", rows, "Pacific/Honolulu", "US", compact=True, sky=sky)
    trs = re.findall(r"<tr[^>]*>", html.split("<tbody>")[1])
    assert trs == ['<tr class="sky-night day-first" data-t="2026-09-26T05:00">',
                   '<tr class="sky-night" data-t="2026-09-26T06:00">',             # 6 AM holds first light but starts before it
                   '<tr class="sky-day now-row" data-t="2026-09-26T14:00">',
                   '<tr class="sky-night" data-t="2026-09-26T21:00">']
    assert '<th rowspan="2" scope="col" class="col-sun" title="Sun and moon">Sun/Moon</th>' in html
    body = html.split("<tbody>")[1]
    assert "border:" not in body and "font-weight" not in body           # the classes carry the look
    assert ('<span class="sun-ev ev-dawn" title="First light 6:02 AM">\u25D0 6:02</span><br>'
            '<span class="sun-ev ev-sunrise" title="Sunrise 6:24 AM">\u2600\u2191 6:24</span>') in body
    assert body.count('class="moon-phase" title="Waxing gibbous, 72% illuminated">\U0001F314 72%</span>') == 2
    first = body.split("</tr>")[0]
    assert first.count("<td") == 2 + 1 + 3 * 6 + 2 + 1                 # date, time, height, six swells, wind, sun/moon
    assert ('<td class="col-date" title="Saturday, September 26, 2026">Sat 9/26</td><td class="col-time">5:00 AM</td>'
            '<td style="background-color:#EDE9F4; text-align:right;">4.10</td>') in first   # Sig. Wave Height first after Time
    assert first.endswith('<td class="col-sun"><span class="moon-phase" title="Waxing gibbous, 72% illuminated">'
                          '🌔 72%</span></td>')                    # Sun/Moon last


def test_without_a_matching_sky_the_compact_table_keeps_the_clock_rule():
    rows = [_row(), _row(time="9:00 PM"), _row(time="7:30 PM")]
    for sky in (None, [], [_sky("day")]):                               # none, or not one per row
        html = A.build_html_table("c", "l", "", rows, "UTC", "US", compact=True, sky=sky)
        assert "col-sun" not in html and "data-t=" not in html and "day-first" not in html
        trs = re.findall(r"<tr[^>]*>", html.split("<tbody>")[1])
        assert trs == ['<tr class="sky-day">', '<tr class="sky-night">', '<tr>']   # 6 AM - 7 PM, 8 PM - 5 AM, between
    classic = A.build_html_table("c", "l", "", rows, "UTC", "US", sky=[_sky("day"), _sky("night"), _sky("day")])
    assert "col-sun" not in classic and "sky-" not in classic           # the classic table never takes it
    assert "border:1px solid #000;" in classic and "border:1px dashed #999;" in classic   # its inline clock rule
    assert A._clock_state("6:00 AM") == A._clock_state("7:00 PM") == "day"
    assert A._clock_state("8:00 PM") == A._clock_state("5:00 AM") == "night"
    assert A._clock_state("7:30 PM") is None and A._clock_state("5:30 AM") is None and A._clock_state("x") is None


def test_directions_get_compass_letters_and_an_arrow_pointing_where_the_waves_go():
    compact = A.build_html_table("c", "l", "", [_row()], "UTC", "US", compact=True)
    classic = A.build_html_table("c", "l", "", [_row()], "UTC", "US")
    swell = ('305&deg; NW <span class="dir-arrow" style="transform:rotate(125deg)" aria-hidden="true">&#8593;</span>')
    wind = ('76&deg; ENE <span class="dir-arrow" style="transform:rotate(256deg)" aria-hidden="true">&#8593;</span>')
    assert compact.count(swell) == 6 and compact.count(wind) == 1
    assert "dir-arrow" not in classic and classic.count(">305</td>") == 6 and "76&deg; ENE</td>" in classic
    assert A._dir_cell(0).count("rotate(180deg)") == 1 and A._dir_cell(200).count("rotate(20deg)") == 1


def _day_rows(start_hour=0, hours=30):
    from datetime import datetime, timedelta
    t0 = datetime(2026, 10, 3, start_hour)
    out = []
    for h in range(hours):
        t = t0 + timedelta(hours=h)
        r = _row(date=t.strftime("%A, %B %d, %Y").replace(" 0", " "), time=t.strftime("%I:%M %p").lstrip("0"))
        out.append(r)
    return out


def test_the_payload_carries_the_sky_for_the_table_and_the_graphs(monkeypatch):
    rows = _day_rows()
    monkeypatch.setattr(A, "parse_bull", lambda station, tz: ("Cycle : 20261003 00 UTC",
                                                              "Location : 51201      (21.67N 158.12W)", "",
                                                              [list(r) for r in rows], "Pacific/Honolulu", None))
    monkeypatch.setattr(A, "load_station_coords", lambda: {})          # the bulletin's own coordinates are used
    d = A.compute_forecast_payload("51201", None, "US", "GFS", compact=True)
    html, g = d["table_html"], d["graph_data"]
    assert "col-sun" in html and "sky-day" in html and "sky-night" in html and "sky-twilight" not in html
    assert "\u2600\u2191 6:24" in html and "\u2600\u2193 6:18" in html     # Honolulu, 3 Oct 2026 (USNO 6:24 / 6:18)
    assert len(g["sky"]) == len(g["labels"]) == 30
    assert set(g["sky"]) == {"day", "night"} and "sun_events" not in g      # the charts shade by it; no strip
    assert g["sky"][6] == "night" and g["sky"][7] == "day" and g["sky"][18] == "day" and g["sky"][19] == "night"   # first light 6:02, last light 6:40 PM
    classic = A.compute_forecast_payload("51201", None, "US", "GFS")
    assert "col-sun" not in classic["table_html"] and classic["graph_data"]["sky"] is None
    # a failure in the sky costs nothing else: the clock rule, no graph sky, the same numbers
    def boom(*a, **k):
        raise RuntimeError("no sky")
    monkeypatch.setattr(A.sky, "annotate_rows", boom)
    plain = A.compute_forecast_payload("51201", None, "US", "GFS", compact=True)
    assert "col-sun" not in plain["table_html"] and '<tr class="sky-day"><td class="col-date"' in plain["table_html"]
    assert "data-t=" not in plain["table_html"] and plain["graph_data"]["sky"] is None
    for k in ("labels", "height", "period", "direction", "units", "swells"):
        assert plain["graph_data"][k] == g[k]


def test_the_window_table_puts_the_significant_height_first_and_sun_moon_last():
    """Owner (plan section 35): Date, Time, Sig. Wave Height, the swells, Wind, Sun/Moon. The classic table keeps its
    order (the height after the swells, no Sun/Moon)."""
    rows = [_row(time="5:00 AM"), _row(time="6:00 AM")]
    sky = [_sky("night", day_first=True), _sky("day")]
    html = A.build_html_table("c", "l", "", rows, "Pacific/Honolulu", "US", compact=True, groups=[0, 1], sky=sky)
    head1, head2 = re.findall(r"<tr>(.*?)</tr>", html.split("<thead>")[1].split("</thead>")[0])
    names = [re.sub(r"<[^>]+>", " ", c).split() for c in re.findall(r"<th[^>]*>(.*?)</th>", head1)]
    assert [" ".join(n) for n in names] == ["Date", "Time", "Sig. Wave Height", "Swell 1", "Swell 2", "Wind", "Sun/Moon"]
    assert re.findall(r">([^<]+)<br>", head2) == ["Hs", "Hs", "Tp", "Dir", "Hs", "Tp", "Dir", "Spd"]
    cells = re.findall(r"<td[^>]*>(.*?)</td>", html.split("<tbody>")[1].split("</tr>")[0])
    assert cells[:4] == ["Sat 9/26", "5:00 AM", "4.10", "3.20"] and cells[-2].startswith("76&deg; ENE")
    classic = A.build_html_table("c", "l", "", rows, "Pacific/Honolulu", "US", groups=None)
    order = re.findall(r">(Swell 6|Significant Wave Height|Wind)</th>", classic)
    assert order == ["Swell 6", "Significant Wave Height", "Wind"]
    without_sky = A.build_html_table("c", "l", "", rows, "UTC", "US", compact=True)
    assert re.findall(r"<td[^>]*>(.*?)</td>", without_sky.split("<tbody>")[1])[2] == "4.10"   # first after Time, sky or not
