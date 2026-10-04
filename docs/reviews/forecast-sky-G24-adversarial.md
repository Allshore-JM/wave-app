# G24 — adversarial review of the forecast table and graphs upgrade (plan section 35)

Reviewed: `feat/forecast-sky` @ 21b5b50 (UI asset 1.16.5; `sky.py` new, `app.py`, `static_ui/forecast.js`,
`templates/index.html`, `requirements.txt`, tests, README) against production `Live-Buoy-Update` @ 28d5573, on the test
site wave-app-clean.onrender.com (branch `test` @ e7535d2). Two fresh-context reviewers (Fable 5.1, MAX), 2026-10-04,
after the owner's step-6 changes (no twilight look; daylight rows bold with solid borders, night rows dashed; the Date
column bold only in daylight rows; the Summary's Day column stays bold). Reviewer A: the server and the data (no
browser); reviewer B: the test site as a visitor sees it, plus the client code. Their reports and artefacts are in the
session scratch directory `g24/a/` and `g24/b/`.

**Result: 0 P0, 3 P1, 1 P2, 15 P3** (19 distinct findings; A-2 and B-2 are the same defect). Every number the reviewers
recomputed independently matched (astronomy, states, slots, the summary maths, the classic answers); the three P1s are
a performance regression in the table build, a misleading Summary note on 3-hourly days, and a lost sticky-column offset
after a load in Summary mode.

## Findings and outcomes

| # | P | Finding (reviewer's evidence) | Outcome |
|---|---|---|---|
| A-1 | P1 | The compact table build takes ~250 ms instead of ~8 ms per request: the Sun/Moon cells' literal glyphs (◐ ☀ ☾ and the moon emoji) make the accumulated `html +=` string 2-4 bytes per character, so each of the ~5,800 appends copies the 1.5-3 MB buffer. Measured by A (265.6 vs 8.0 ms; with ASCII entities 11.1 ms) and confirmed by the author (250.3 / 8.4 / 10.3 ms on the repo's real 385-row bulletin). On the test site the compact answer took 0.66-1.04 s against 0.23-0.35 s for the classic one on the same instance; production compact ≈ classic ≈ 0.25 s. Every window open, pick, zone or unit change paid it; the server's compact headroom shrank ~25x. | FIX: `_sun_cell` and `build_summary_html` emit the glyphs as numeric character references (the browser renders the same text); a test pins the compact table HTML as pure ASCII. |
| A-2 = B-2 | P1 | The Summary's "from 8:00 AM" / "until 5:00 PM" note appears on WHOLE 3-hourly days: `summary_days` compares the first/last DAYLIGHT ROW with sunrise + 1 h / sunset − 1 h, and a point's 3-hourly rows (days 6-16) routinely start 1.5-2.5 h after sunrise. Sydney point: 11 of 16 days "from 8:00 AM"; Oahu point 11 of 17; any zone whose offset ≡ 2 mod 3 h; the `elif` hides the second note. The numbers in those rows are right. | FIX: the note only when the forecast cuts the day — the day's FIRST ROW of any state later than sunrise + 1 h ("from" the first daylight row) and/or its LAST ROW earlier than sunset − 1 h ("until"); both notes possible; tests with 3-hourly whole days and cut first/last days. |
| B-1 | P1 | The frozen Time column covers the Date column after a forecast lands while the detailed table is hidden: `placeSticky()` runs from `apply`, `setView`, `onMode` and `onResize` but not from `setTableMode`, and `--date-w` cannot be measured on a hidden table (width 0 → the write is skipped). Repro: Summary → reload (or a station landing in Summary, or a minimised landing expanded in Summary) → Detailed → scroll sideways: the Time cell sits at `left: 0` over the Date cell (B's screenshot `B1_date_column_lost_after_summary_reload_tab16.jpg`); a stale offset also follows a maximise to ≥ 1,500 px while in Summary. A resize heals it, which is why the author's checks never saw it. Mutants m11/m12 (no `placeSticky` in `apply`/`setView`) survive the Node suite. | FIX: `placeSticky()` whenever the detailed table becomes visible (inside `applyPanels`, after the visibility changes); tests: a forecast landing in Summary mode then Detailed reads a measured `--date-w`; B's m11/m12 pinned. |
| A-3 | P2 | The moon's NAME in the hover titles follows 8 equal octants: "Last quarter" at 35-56 %, "New moon" at 1-3 %, "First quarter" at 30-62 %, "Full moon" at 96 % — 63 of 123 place-days disagree with USNO's names; the glyph and the lit % are right (within 1 point on 123/123 days). | FIX: a principal name (new, first quarter, full, last quarter) only within ±12 h of the instant (|phase − k/4| < 0.017 of the cycle), else waxing/waning crescent/gibbous by the phase; the glyph keeps its octant rule; tests on the days A listed. |
| A-4 | P3 | Equal-ended height ranges are not collapsed ("1.8–1.8 ft", 15 of 227 live ranges) while the wind cell collapses. | FIX: collapse to "1.8 ft". |
| A-5 | P3 | `sky.day_summary` with a span `events()` refuses (> 20 days) builds all-None rows labelled "midnight sun" / "polar night" instead of returning None (dormant: a forecast spans 17 dates). | FIX: return None when `events()` does. |
| A-6 | P3 | Above ~89° N the single yearly sunset/sunrise is missed by `_search` (ephem raises AlwaysUp/NeverUp for the daily search and the 12-h skip walks past the one seasonal crossing); 84° N is fine (every crossing found). No forecast point can be served that far north (the map ends at 84° N). | ACCEPTED: documented in `sky.py`'s docstring. |
| A-7 | P3 | Legacy "Hr" bulletin parser: the Hst fix is right (1.27 / 1.28 / 1.28 m on the fixture), but `idx_base = 6` starts the swells at swell 2's Tp on the fixture's own layout (swell 1 starts at token 2). Unreachable with NOAA's current "day & hour" format. | FIX: the swell start index from the header too (`index("Hs")`), the fixture test extended to swell 1's values. |
| A-8 | P3 | Docstrings misstate behaviour: `to_utc` ("standard time" — it is the DST reading), `_short_clock` (a `short=` that does not exist), `summary_days` ("columns" — the code pools systems); the plan's §35 design text still describes the twilight look and the sky strip / now line. | FIX: the docstrings; a "superseded by the progress log" note at the top of the plan's section 35. |
| A-9 | P3 | 27 of 70 mutants survive: the trend rules (thirds vs halves, the 1.10 / 0.90 bands, a peak at the window's end), the power weighting, the systems' 20 % / 1.5 s / 40° windows, Hs² × Tp, the re-sort, hs_max, the "until" margin, wind rounding, the compass 11.25° boundary, the now interval, the polar label threshold, None-state rows, `_search`'s 12-h skip and 1-min de-dup. None is a defect today (every rule checked on live data). | FIX: parametrised tests on both sides of each threshold. |
| A-10 | P3 | Event texts carry no AM/PM ("☾↑ 12:40"); in a 3-hourly row that crosses midnight (an 11 PM row holding 12:40 AM) the text is ambiguous; only the title says "Moonrise 12:40 AM". | FIX: the suffix shown only when the event's half-day differs from the row's ("☾↑ 12:40 AM" in the 11 PM row). |
| B-3 | P3 | The Summary's Moon (local noon) differs from the table's badge (the first night row, ~7 PM): 15 of 16 days differ by 1-3 points, 10/4 🌗 35 % vs 🌘 32 % (different glyphs). | FIX: the Summary's moon at nightfall (last light, else noon), the badge's instant within an hour. |
| B-4 | P3 | The "falling" glyph ↘ (U+2198) renders as a colour emoji in Windows Chrome while ↗ stays a text arrow. | FIX: U+FE0E text-presentation selectors on both arrows. |
| B-5 | P3 | A day row's black box is open at the table's right edge and the last row's bottom (the table draws those in #dee2e6; the old collapsed scheme gave every day cell four black sides). | FIX: the server marks each row's last cell and the header's right-edge cells (`col-last`); those draw the right edge in the row's style; the last body row draws the bottom; the table's own right/bottom border goes. |
| B-6 | P3 | Sunrise/sunset spans are weight 600 inside 700 day rows (lighter than the row); the emphasis inverts. | FIX: the 600 only in night rows (day rows inherit 700). |
| B-7 | P3 | The sun-event colours fall under WCAG AA on the night tint: #8a6d1f 4.14:1, #b4530a 4.24:1 at 11.5 px (hover 3.35 / 3.44). | FIX: darker colours (≥ 4.5:1 on the night tint). |
| B-8 | P3 | After a failed load of another station, returning to the previous station does not reveal the now row (`revealedFor` keeps the old station; `ui.clear` does not reset it). | FIX: reset `revealedFor` when a load fails / the table is cleared; test. |
| B-9 | P3 | In a zone far from the station's (51201 in UTC) a summary row pairs a sunset (4:19 AM UTC = Oct 2 evening HST) with the next day's sunrise, and a daylight period is split over two calendar-date rows. | ACCEPTED (by design: one row per calendar date of the chosen zone; the rows and the Sun/Moon column move with it). On record for the owner. |
| B-10 | P3 | Mutants: m07 (`--date-w` rounded instead of floored), m11/m12 (`placeSticky` in `apply`/`setView`), m18 (`viewRange` floor/ceil swapped), m20 (the mode swap ignoring the view) survive; `setTableMode(tableMode)` at init writes the session key on every load. | FIX: pins for m11/m12/m18/m20 (m07 is equivalent in practice); the key written on a choice only. |

## Confirmed correct (the reviewers' evidence, summarised)

- **Astronomy vs USNO** (A: 123 place-days, 738 event comparisons at Honolulu, Sydney across its spring-forward, Boston,
  Biscay across its fall-back, Tromsø, Reykjavik, Longyearbyen, the Bering Sea, the equator, the date line, Tonga, Cape
  Town, Valparaíso): 731 identical to the minute, 6 off by one minute at rounding boundaries, 1 USNO fixed-offset artefact
  on Sydney's DST day (the module's wall time is right); lit % within 1 point on every day; polar labels and the last
  sunset / first sunrise around Longyearbyen's polar night found; the sun's crossings also checked without ephem (NOAA's
  solar algorithm, 16 windows: 0 missed, 0 extra); `_search` terminates at the poles and for absurd coordinates.
- **State and slot rules** (A: 14 live payloads, states re-derived from the altitude; B: 51201, 62001, the Sydney point on
  the page): 0 state mismatches; every event exactly once in the row whose slot holds it; the texts and titles right; the
  badge on the first row of each night run only; `day-first`, `now-row`, `data-t` right; no inline weight or border in the
  compact table; `graph_data.sky` one state per label (385 for a point's 209 rows) with 0 mismatches; DST rows in order.
- **Classic answers byte-identical to production** except the one relabel (9 stations/points, 0.5-1.1 MB each);
  compact numbers equal production's in every row (the ranking unchanged); `summary_html` null on the classic path and on
  any sky error; invalid/land points keep `final` / `reason`; `render=full` inlines the summary; exceptions never 500
  (the clock-rule fallback); no payload-level cache (unit and zone per request); coordinates file → point → header.
- **Summary maths recomputed from the detailed rows** (A: 11 payloads / 180 day rows, 0 mismatching cells; B: 17/17 Sig and
  wind ranges, 15/15 sunrise/sunset = the Sun/Moon column, 0 mismatches at six stations/points): the daylight window, the
  ranges, the trends, the systems (grouping and power ranking), the directions, the wind, the moon, the cut-day notes on
  true partial days, a daylight-less last date left out, single-row days.
- **The look on the page** (B, 1:1 pixel probes): 193 day / 192 night rows, 0 twilight, no amber rule; day rows 700 +
  1 px solid #000 (the frozen cells too), night rows 400 + 1 px dashed #999 + the tint; the Date cell bold only in day
  rows; the line under a day row solid, night lines dashed; the 2 px divider; the now-row tint and accent; hover tints on
  the frozen cells; sunrise/first light in night rows, sunset/last light in day rows; the badge mirrored south; 12.5 →
  13.5 px text at a 1,500-px body. **Frozen edges** clean at four scroll positions (no slivers, single 1-px lines, the
  header's bottom line under every cell incl. the rowspans). **Sizes**: 1280×800 (157 px sideways when maximised, known),
  1700×950 (fits), a 430-px west-edge resize (the table stretches, `--date-w` re-measured), phone 375×812 (full screen,
  wrapped toolbar, no page scroll, the now row revealed). **Modes**: Detailed by default, 17/8/16 summary rows, the
  buttons hidden in Graph view and on error, sessionStorage only, per-panel places with real clicks, the reveal once per
  station and deferred while minimised / in Summary. **Graphs**: `nightShade` only, "Significant Wave Height", the compass
  axis, bands at the table's rows, 7 d / 3 d, tooltip sync, no growth over round trips, no console output. **Zones and
  units** move everything together; one request per change; the error path with Retry; rapid clicks; the live-buoy window
  and the overlay alive; the a11y attributes; the no-JS page and the print rules; 667 pytest + 357 Node green in both
  reviews; the worktree clean afterwards.

## Fix round scope (step 7)

Server (`sky.py`, `app.py`, tests): 1 A-1 ASCII entities for the Sun/Moon cells and the summary + the pure-ASCII pin;
2 A-2/B-2 the cut-day notes from the day's first/last row of any state, both notes; 3 A-3 principal moon names within
±12 h only; 4 B-3 the Summary's moon at nightfall; 5 A-4 collapsed equal ranges; 6 A-5 `day_summary` None on a refused
span; 7 A-7 header-driven swell start in the legacy parser; 8 A-8 docstrings + the plan note; 9 A-9 parametrised pins;
10 A-10 the AM/PM suffix when the half-day differs; 11 B-4 text-presentation arrows. Page (`forecast.js`, `index.html`,
tests): 12 B-1 `placeSticky` in `applyPanels` + tests; 13 B-5 `col-last` right/bottom edges per row; 14 B-6 the
sunrise/sunset weight; 15 B-7 the colours; 16 B-8 `revealedFor` reset; 17 B-10 pins + the session key written on a choice.
Then UI 1.16.6 + fixture + golden (own commit), the test site, a short fresh re-check at MAX, the owner's go-ahead for
production (tag `prod-pre-sky` @ 28d5573).

Accepted / on record: A-6 (above 89° N), B-9 (a zone far from the station), the equivalent mutants (A's S2/S6/S7/S22,
B's m07), the 1280-px sideways scroll (told to the owner at step 5).
