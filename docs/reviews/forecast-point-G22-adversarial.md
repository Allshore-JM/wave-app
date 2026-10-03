# G22 — the Forecast Point tool: the reader and the page (plan section 31, step 7)

Two fresh-context reviewers at MAX effort, 2026-10-02, on `feat/forecast-point` @ `1fbdc89` (UI 1.13.0, five commits on
top of production `4d37b8e`) and on the test site (`wave-app-clean.onrender.com`, branch `test` @ `e34e29e`, model run
2026100212). The author's check of both reports and the owner's decisions followed the same day.

- **Reviewer A** covered the code and the data, with Python and Node on private copies and no browser: the ids on both
  sides, the HTTP surface, the nearest cell against a brute force on the live masks, the source under hostile and
  failing objects, the service limits with real threads and sockets, the tracking on 402 real cells, the rows against
  225 NOAA bulletins with a parser of its own, the page's own script blocks in a small DOM, the graph helpers under nine
  browser time zones, base against head, 124 server and 60 client mutants, and the planned land refusal on real coast
  data.
- **Reviewer B** covered the test site in the browser pane, desktop layout at 1280 x 800 and the phone preset: 44
  places worldwide added with real clicks, a point against the station at the same coordinates, the saved points'
  whole life, errors and races, the interplay with the other tools and windows, the graphs, speed, words and
  accessibility, and 105 land / nearshore-water cases for the land refusal.

**Result: 0 P0, 4 P1, 10 P2, 24 P3** (A: 2 P1 + 6 P2 + 13 P3; B: 2 P1 + 5 P2 + 14 P3; four findings overlap), plus one
P2 the author found before the launch (K-8). No stored value, unit, direction convention, valid time or cell choice was
found wrong, the stations' answers are byte-identical to production, and no saved point was lost. The four P1 are about
WHAT IS SHOWN: a table that hides a swell column, water served from beyond land, a graph whose days are not the same
width, and time zones nobody can read.

## What the reviewers confirmed

- **Ids.** A: 164,761 numeric and 114,996 string inputs give the same answer in Python and JavaScript; all 540,001
  canonical ids are accepted on both sides and nothing else; no coordinate pair has two spellings.
- **The nearest sea cell.** A: 163,500 points on the live masks (the seams at 52.2 N and 12.6 S, the dateline, 70-86 N,
  60-80 S, the Dutch pocket, 6,673 exact ties) against a brute force with another distance formula: 0 disagreements in
  cell, grid, coordinates and distance. B: the seams choose the right grid on each side with continuous heights.
- **The rows against NOAA.** A, own parser, 225 bulletins of one run (113 g16, 56 s25, 56 n25): every row's valid time
  is the bulletin's, also across +120 -> +123 h; the direction convention is right (87.4 % of bulletin systems of 0.3 m
  or more match a product partition when turned by 180 degrees, 7.4 % when not); combined height within 0.008 m on
  average at the 198 stations within 10 km of their cell; wind at 21 stations within 0.21 m/s and 1.7 degrees; the New
  South Wales clock change (no 2 AM row on October 4, offsets +10 then +11); the Metric and US tables equal the
  product's codes in all rows.
- **Bounds.** A: 6,000 random points through the reader: RSS 83-84 MB, the tile cache at its 24 MB cap with accounted
  bytes equal to real bytes, 256 cells, 3 masks; 400 requests from 8 threads: no answer carried another point's data,
  no refusal cached, both build slots free at the end. Every answer for a point is HTTP 200, `private, max-age=0`,
  `DYNAMIC` at the edge; the page's own cache keeps neither a busy answer nor a refusal.
- **Injection.** A: besides K-8 no name, id, label or message reaches `innerHTML`, an attribute or a URL unescaped; a
  link cannot carry a name. B: `pt_<b>bold</b>` in the address is written as text.
- **Nothing else changed.** A: 66 answers of base and head with the same fake upstream: station answers identical in
  bytes and headers; pages differ only by the template's own diff. B: four station answers and `stations.json`
  byte-identical on test and production.
- **The page in use.** B: 37 of 44 places served and 7 correctly refused, each with consistent title, address, meta
  line, id, star and marker; nothing lost across reload, two tabs or damaged storage; quick switching between a point
  and a station ends consistent; Retry after a network failure loads and keeps the point; one request per point,
  4.9-18 KB on the wire; no console error in any flow.

## Findings and outcomes

### P1

| # | Finding | Outcome |
|---|---|---|
| A-1 | The compact table applies the stations' "a column needs a value in the first 7 days" rule to a point's TRAIN columns: a swell that starts after day 7 has no table column. `pt_46250S_96000E`, Fri 10/16 9 PM: three empty swell cells beside Comb. 9.99 m; 4.2 % of 1,200 cells hide a column. Reproduced by the author on the test site. | Fixed by the owner's column decision below (rows in rank order: a hidden column can then only hold the weakest system of its hour, as at a station). |
| A-2 | Water behind land is served from the cell beyond the land: Long Island Sound from the Atlantic 28 km away, San Francisco Bay from the Pacific 31 km, Tampa Bay, the Solent, Pearl Harbor. 11.1 % of 1,574 served nearshore water points have 0.5 km or more of land between point and cell. Reproduced by the author. | Owner: refuse. The cell must be reachable from the point over water (fix round, item 1). |
| B-1 | A point's graphs give each ROW the same width: days 1-5 (hourly) are 123 px each, days 6-16 (3-hourly) 41 px, with no cue; a station's days are 67 px each. | A point's graph data goes on hourly slots (385 labels, empty between the 3-hourly rows); the table keeps its 209 rows. Removes B-14 (day line and night shading up to 2 h late) and A-15. |
| B-2 | Points away from the coast show "Etc/GMT+11"-style zones (the name means UTC-11) and not the zone the station at the same place uses (46006: station America/Los_Angeles, point Etc/GMT+9; 340 km north of Oahu: Etc/GMT+11 beside Pacific/Honolulu). Reproduced by the author. | Owner: the nearest station's zone. Raw "Etc/" names are never shown ("UTC-11"). |

### P2

| # | Finding | Outcome |
|---|---|---|
| A-3 | A bad tile body is cached before it is checked: the tile stays "temporarily unavailable" until the run changes; Retry cannot help. | Check before caching; a test that asks again after the bucket recovers. |
| A-4 (= B-15) | A point's "Swell n" is a swell TRAIN, a station's is the hour's rank: at a point "Swell 1" holds the hour's highest system in 50.7 % of rows and is empty beside another system in 16.6 %. | Owner: one rank per hour for points and stations alike, by height squared x period (below). The tracking goes. |
| A-5 | North of 52.25 N the product's grid is NOAA's interpolated 1/4 degree grid: the site's systems found in the bulletin 93.5 % (native grids 98-99 %), period p99 1.3 s (0.2 s), 44 rows with a partition above the combined height. | A limit of NOAA's file, told to the owner; README. Follow-up for the job: NOAA's native Arctic grid. |
| A-6 | A bucket that hangs: 31 s per object (three attempts of 10 s), a build slot held 31-93 s, the pointer read outside the slots, "busy" after up to 3.9 s. | One attempt, a wall-clock cap per object, busy at once when both slots are held, one deadline per request. |
| A-7 (= B-3) | A point that cannot be kept (the 51st, a storage error) is dropped with a screen-reader line only; with storage blocked the line says "Kept". | A visible message; the fallback storage reports "not saved". |
| A-8 | No test runs the page's keep logic, picker rows or markers (14 of 15 template mutants survive); 44 of 124 server mutants survive, among them a wrong cell read from a tile that lacks it, a whole tile kept per cell, the longitude window without latitude, the 6-hour limit restarting. | Page-block tests in Node; server pins for each listed survivor; the reviewers' mutants re-run after the fixes. |
| B-4 | The coordinates are what gets cut: phone bar "21.279N 158.2…", a named point's name pushes them out of the list row and the title. | Cut the name, never the coordinates; the heading gives the title the room the run text has. |
| B-5 | The no-script page of a point shows "3FYT — 3FYT" in the select above the point's table. | The server renders the point's own option. |
| B-6 | The picker by keyboard: arrows do not move between rows; three Tab stops a row. | Arrow keys, Home and End move between rows. |
| B-7 | After a failed load the previous table stays under the new title (already so for stations on production). | A failed load clears the table and the meta line. |
| K-8 (author) | The point marker's tooltip renders the name as HTML (`bindTooltip(string)` is `innerHTML`): a name `<img onerror>` ran script on the test site. Own input only. A-20: the live-buoy markers do the same with names from ten outside providers, since before this change. | Text nodes for both. |

### P3 (each fixed in the fix round unless marked)

A-9 the buoy time-zone cache grows by one entry per point without limit · A-10 `RecursionError` from deeply nested JSON
is not caught (`render=full` answers HTTP 500) · A-11 at a run change a request holding the old manifest wipes the new
run's kept rows · A-12 = B-9 a point added in another tab is a dead row here (no `storage` listener) · A-13 / B-12 the
star keeps a refused point; a tool click is lost when anything else is picked before its forecast lands · A-14 the
title's tooltip keeps the old name; bidi control characters stay in names · A-15 the time-based 7 d / 3 d window is one
row off across a clock change, now for stations too · A-16 without a bucket the tool still shows and the answer offers
Retry; the `render=full` seed lacks `final` · A-17 white space after the id in a link · A-18 the tracked columns are
more broken up than NOAA's order (29.8 against 17.1 segments a table); no other rule does better (moot: the tracking
goes) · A-19 words that are not true (docstrings; "numpy only with points": `timezonefinder` loads it anyway) · A-21 a
land test without a shore band would refuse surf spots · B-8 a point marker swallows the tools' clicks · B-10 a
right-to-left name scrambles the coordinates · B-11 an emoji is cut in half at 40 UTF-16 units · B-13 Escape with focus
in the minimised window does not end the tool (K-4's sibling) · B-14 see B-1 · B-17 "My points" unticked: a new point
gets no marker · B-18 Remove has no confirmation · B-19 words and names ("model cell" is jargon, the list's label, one
message for three different refusals) · B-20 small targets (phone: the 14 px markers) · B-21 new-region requests take
0.6-1.3 s, not 0.17-0.74 s.

Accepted, on record: B-16 the nearest cell can differ from the station (46026 lies exactly between two cells: 0.64 ft
on average) and from the overlay near coasts: the rule is the nearest sea cell, not a blend · B-17 starting another
tool clears a locked exposure window (by design), a point made on a station's dot covers the dot · B-20 the tool bar's
close and fold buttons on the desktop layout (as before) · the known items K-2 (an unkept linked point has no marker:
fixed), K-3 (phone heading: fixed with B-4), K-4 (fixed with B-13), K-5 (an error shown twice: fixed), K-7 (wording:
fixed).

## The owner's decisions (2026-10-02)

1. **Land is refused.** Not snapped, not served from a sea cell across the island.
2. **Columns: rank per hour by wave power, height squared x peak period, SITE-WIDE.** "It should be based on the swell
   energy, not necessarily by swell height. Swell with largest energy should be swell 1, second swell 2, etc. it is ok
   if swells transition across columns as they grow"; then, after the measurement below: "The forecast point and the
   added points must match in how they rank the swells. If we rank by energy the existing forecast point data should
   be resorted", and "Yes to both, rank by height² × period site-wide".
   Measured on 225 bulletins (53,577 rows): NOAA lists the systems in HEIGHT order in 100.00 % of rows, with no empty
   column left of a filled one; an order by height squared x period differs in 35.6 % of rows and names another
   Swell 1 in 11.2 % (station 13130: 1.26 m at 6.7 s is listed before 1.05 m at 12.5 s, which carries 30 % more
   power). Height squared x period is the energy arriving per metre of crest (deep water) and the only wave quantity
   in the usual breaker-height formula (Komar and Gaughan), so it ranks the swells by the surf they make.
   ONE rule for every forecast table: added points, NOAA's stations (re-sorted: their numbers stay, the columns they
   sit in change) and the SWAN stations; rows packed from the left; the graphs follow the table. The live-buoy swell
   components (today: swell before wind sea, then the longest period) take the same order. The swell tracking of
   step 4 is removed.
3. **Water behind land is refused** when no model cell within reach can be reached over water.
4. **Time zone: the nearest station's.** Far from every station a plain offset ("UTC-11").

## Fix round scope (step 8)

1. **Where a point's data comes from (server).**
   - A coast reader in the reader module: `static/coast/v1/index.json` and the 5-degree cells of the same bucket,
     through the source's fetch with size caps, checked, decoded once, an LRU by bytes. It must answer as the page's
     `decodeCoastLL` / `inLand` do (reviewer A's plain-Python port agrees at 3,000 of 3,000 points; a shared fixture
     checked on both sides).
   - **Water** = the land test says water, or water lies within 300 m, at the ID's coordinates (the band takes in
     beaches, piers, the coast data's own error and the rounding of the id grid: the Pipeline id of the brief is "land"
     by 70 m, 11 of 596 live instruments are within the band). Land -> a final answer of its own.
   - **The cell** = the nearest sea cell within reach whose straight path from the point holds no stretch of land
     (sampled every 250 m; the band at the point and the cell's own half-width left out). None -> a final answer of its
     own (sheltered water has no forecast).
   - Coast data that cannot be read -> "temporarily unavailable" with Retry: a point is never served untested.
   - Three distinct messages: land; no model cell reachable over water; no model data (ice, lakes, outside coverage).
   - Measured before and after on B's 105 cases, A's 3,000 near-coast points and named cases, the 596 live
     instruments and the 45 stations of `check_points.py` (which cells change, and why).
   - The star cannot keep a refused point; a saved point that is now refused stays in My points, says why when opened,
     and can be removed.
2. **The tool waits for the answer.** A click asks the server first ("Checking..." in the tool bar). A forecast: the
   tool ends, the window opens (the answer already in the page's cache), the point is kept. A refusal or a failure: the
   message in the tool bar, the tool stays on for another click, the window and the forecast on screen untouched. The
   pending / keep-later logic goes with A-13's edge cases. "Not kept" (50 points, storage refused) is said visibly.
3. **Rows and graphs.** ONE ranking function (height squared x peak period, descending; ties by height, then the
   source's order; the wind sea among the swells; packed from the left) applied to the rows of all three sources
   (points, NOAA bulletins, SWAN) after parsing, and to the live-buoy components; the tracking code, its constants,
   tests and words removed; the table's 7-day column rule unchanged (a hidden column then holds only the weakest
   system of its hour). The stations' answers change on purpose (same numbers, other columns in about a third of
   rows): fixtures and the golden's fake rows follow, and the re-check covers the station tables. A point's graph
   data on hourly slots; `rangeWindow` counts rows again.
4. **Time zone.** The point's own civil zone when the lookup gives one; else the nearest station's zone
   (`station_timezones.json`) within about 1,000 km; else the ring search; else the nautical zone. "Etc/GMT+N" shown
   as "UTC-N" (display only: station answers keep their bytes). One helper for the table and for the overlay's
   valid-time zone; its cache capped.
5. **Robustness.** A-3, A-6, A-9, A-10, A-11, A-16, A-17; "Invalid station id" without Retry.
6. **The page.** Tooltips as text (points and live buoys); visible "not kept" messages; names cut before coordinates;
   the no-script option; picker arrows; a failed load clears the old table; point markers forward the tools' clicks; a
   `storage` listener; bidi isolation and control characters; code-point cut and the limit in the prompt; the title's
   tooltip after a rename; Escape and focus (B-13, K-4); the layer ticked again when a point is added; a marker for
   the unkept point on screen; the error once; a confirmation before removing a named point; words (B-19, K-7); a
   larger hit area for the markers on touch screens.
7. **Tests.** The page's My-points block, picker rows and markers run in Node (reviewer A's harness shows how); every
   listed mutant survivor pinned; A's 184 mutants re-run and new ones for the new code. UI 1.14.0, the golden in its
   own commit, README.
8. **Re-check** by a fresh reviewer at MAX on the fix set and the test site, with the bulletin comparison re-run on a
   live run.

## What will still be wrong after the fix round (told to the owner)

- Shores the coast data puts more than 300 m seaward of the real one (reef flats, tidal flats, river mouths) are
  refused as land; atoll islets and reefs missing from the data count as water.
- Sea ice is not in the coast data: the run's own mask decides, so pack ice within 40 km of open water is served from
  the open cell and the answer can change from run to run.
- North of 52.25 N the systems come from NOAA's interpolated grid (A-5).
- A point reads the nearest sea cell; NOAA's station bulletins are interpolated to the buoy. Near coasts the two can
  differ by a few tenths of a foot (B-16).

## Not checked

The busy answer on the service (a burst of 8 new points gave 8 tables in 1.0-3.9 s), the "N h old" text on a real stale
run, a new run arriving under an open page, the first point after a deploy, real touch input and a real phone, a real
no-script render and print preview, screen-reader speech, memory and timing on the 512 MB instance itself.

## Re-check of the fix round (step 8c, 2026-10-02)

Two fresh reviewers at MAX on `060535e` (UI 1.14.0) and the test site (`test` @ `0782008`):
- **A, code and data**, without a browser. A re-ran G22 A's 124 server mutants and added 64 of its own; a helper ran
  130 page mutants (111 finished).
- **B, the test site and its use**, with real clicks on desktop and phone. B checked 32 of the 105 land and water
  cases by hand and all 105 through the API. B also covered grids over six sheltered waters, 30 lagoons and harbours,
  12 station tables against production, and 11 points at station coordinates.

**Counts.**

| Reviewer | P0 | P1 | P2 | P3 | Findings |
|---|---|---|---|---|---|
| A | 0 | 1 | 4 | 20 | R-A1 .. R-A25 |
| B | 0 | 2 | 1 | 10 | R-B1 .. R-B13 |
| **Distinct** | **0** | **2** | **5** | **26** | |

Overlaps: R-A1 = R-B2 (b, c); R-B4 is part of R-A17; R-B5 and R-B6 are part of R-A14; R-B11 = R-A22.

### Confirmed

- **The station re-rank lost nothing.**
  - A: 225 bulletins (53,577 rows) through the site's own parser in both trees. The same systems in every row;
    30.6 % of rows re-ordered, another Swell 1 in 9.6 %; date, time, wind and combined height untouched.
  - B: 12 station tables against production. 4,200 of 4,200 rows keep the same numbers, 1,776 re-ordered, none out
    of power order.
  - The live SWAN table (175 rows) and the live-buoy components: the same.
- **A point's rows and graphs.** 420 payloads of real cells: 209 rows each, and 385 hourly labels in the zone asked
  for (Sydney's clock change and three zones that fall back included). Every series is empty exactly between rows.
- **The land test.** Server, page and builder agree at 54,585 points, apart from points lying exactly on a coast edge
  (within 1.6e-9 m). Against raw GSHHG, 48,655 points agree within 5.9 m (the published rounding).
- **Time zones.** All 4,036 station positions take their station's zone. B's 11 points at station coordinates take
  the station's zone and its hourly slots.
- **Limits.** Never more than two builds; no slot leaked; a run change keeps nothing of the old run; a
  `RecursionError` answers 200.
- **The fix round's claims (A's verdicts):**

  | Verdict | G22 findings |
  |---|---|
  | Fixed (30) | B-13 among them, with a regression (R-A7) |
  | Partly fixed (8) | A-2, A-6, A-8, A-16, A-19, B-7, B-11, B-17 |
  | Accepted, as before | A-5, B-21 |
  | Moot | A-18 (the tracking is gone) |
  | Need a browser | B-20, K-3. B found K-3 still cut on 360-375 px phones (R-B3) |

### The author's own checks

- **R-A3:** confirmed on the test site. `pt_54500N_167000E` and `pt_52000N_162000E` answer `America/Adak`.
- **R-A4:** confirmed in the browser. On a point's Full graph from day 6, no tooltip shows over about half of each
  3-hour gap.
- **R-A2:** read from A's measurements, which cover both behaviours. The timer's close waits for the reader's lock; a
  shutdown from the timer ends a trickle at 10 s.
- **R-A1 = R-B2 (b, c):** reproduced on the offline product, and measured against the candidate rules below.

### P1

- **R-A1 = R-B2 (b, c) Sheltered water is still served across land.**
  - Two causes: the path check never looks at land within a quarter cell of the cell's centre (±4.6 km on g16,
    ±6.9 km on s25/n25), and its 250 m samples miss spits.
  - Venice, the Solent (Lymington), Townsville, Pamlico Sound and the Indian River Lagoon are served from the open
    sea. The Cable Beach buoy, in open sea, is served from Roebuck Bay across 3.4 km of the Broome peninsula.
  - 77 of 1,512 served near-coast points (5.1 %) have at least 0.5 km of land on the path (1fbdc89: 15.5 %).
  - FIX: the path rule below.
- **R-B1 The coast data lies 1-2.3 km seaward of the real shore at some breaks.**
  - Puerto Escondido: water up to 1.3 km out is refused as land, 1.6 km out is served.
  - Peahi: 100 m off the cliffs is refused, Jaws 800 m out is served.
  - Honolua Bay: the break reads as land.
  - A data limit: owner decision 5.
- **R-B2 (a) Water that sees an open-sea cell through a mouth is served** (Southampton Water, the Solent at Cowes,
  lower Tampa Bay, just inside the Golden Gate). This is the rule as worded to the owner: owner decision 6.

### P2

- **R-A2 The 8 s cap does not stop a slow body that has a Content-Length.**
  - This covers every tile, mask and coast cell: measured over 40 s.
  - The "cut short" check never fires on a real response: urllib3 raises first.
  - FIX: the timer shuts the socket down, and the clock starts before the request. A trickle then ends at 10 s
    (Content-Length) or 8 s (chunked).
- **R-A3 Near the date line a point takes a station's zone from the other side.** Off the Commander Islands
  (UTC+12) a point gets America/Adak (UTC-9), a calendar day off. 116 of 1,562 grid points near 180 are affected.
  FIX: accept a station's zone only within 3 hours of the point's nautical offset.
- **R-A4 A point's graphs lose the tooltip and the three-chart sync between the 3-hourly dots** from day 6.
  FIX: each series only at its rows.
- **R-A5 Mutant survivors that change what a visitor gets.**
  - Server: M49 (only four of six columns ranked), S61 / S62 (a tile of another position or grid accepted), S124
    (one run's values under the other run's times at a run change), M43 / M44 (busy rules), M24 (no land beyond
    180 on a path), M56 (the live-buoy zone cache cap), M02 / M03 (the band's rings).
  - Page: X32 (`textTip` parsing HTML would pass every test), F43 / F44 (3 d and 7 d windows), F04 / F06
    (`prefetch` taking an error body), T07, T18 / T19 (Escape and focus), X16 / X18 / X39 / X41 (refusals from a
    load, the star), X28 / X29 / X34 (the unkept marker), X23 (a throwing storage).
  - Mutant totals:
    - G22 A's 124 server mutants: 57 killed, 25 survive, 42 no longer apply.
    - A's own 64: 51 killed, 13 survive.
    - The page: 111 of 130 run, 43 survive, 22 of which change what a scenario sees.
- **R-B3 On 360-375 px phones an unnamed point's coordinates are still cut.**

### P3

All are fixed in fix round 2 unless marked.

- **Point loading and refusals:**
  - R-A6: a late answer opens over a later pick.
  - R-A8: a refusal is never forgotten.
  - R-A9: the "not kept" note outlives its point.
  - R-A10: the run text and model bar stay after a failed load.
  - R-A11: the title reads "Select a station" after a refusal.
- **Keys and focus:**
  - R-A7: Escape with the favourites list open ends the tool.
  - R-A16: focus is lost on another tab's change.
  - R-B8: focus after Retry.
- **Names and words:**
  - R-A12: the cut splits emoji.
  - R-A13: a stored name that is not a string stops the window.
  - R-A14 with R-B5 and R-B6: words. An HTTP error is told as a network one, the folded bar says "Computing…"
    while checking, and the removal words.
  - R-A21: docs.
- **The map:**
  - R-A15: a point kept with the star while the layer is unticked has no marker.
  - R-A18: the tool shows without a points bucket.
- **Zone labels:** R-A17 with R-B4. A raw "Etc/GMT+N" still reaches the no-script page and the classic table, and
  the window says "UTC−11" while the overlay says "GMT-11".
- **The no-script page:** R-B12. An invalid id still shows "3FYT".
- **Server and data:**
  - R-A19: the coast data is not checked against its name, and the index is kept after a size mismatch.
  - R-A20: the South Pole reads "no model data".
  - R-A23: `rank_groups` drops a group that has no height.
  - R-A24 / R-A25: page and server agree only off the coastline, and no shared test pins it.
  - R-B7: a click on a spit is told "sheltered".
- **Accepted:**
  - R-B13: a metric rounding nit.
  - R-A22 = R-B11, R-B9, R-B10: owner awareness, told.

### The path rule, measured

Prototypes (`proto_path.py`, `proto_rules.py` in the scratch folder) ran on the offline product and the published
coast. The sets:
- 36 surf spots from B's water cases;
- 19 sheltered waters;
- 11 waters that see the sea through a mouth;
- the 596 live-buoy positions.

| Rule | Surf served | Sheltered refused | Through a mouth served | Buoys served |
|---|---|---|---|---|
| 060535e (sampled; the cell's inner box skipped) | 36/36 | 13/19 | 11/11 | 554 |
| A's proposal (sampled; forgive the land run reaching a land centre) | 36/36 | 11/19 | 11/11 | 555 |
| Exact crossings, any land run of 0.1 km or more blocks, the run reaching the centre forgiven up to half a cell | 35/36 | 19/19 | 11/11 | 550 |
| The same, with the path starting at the nearest coast-data water inside the 300 m band (**chosen**) | 35/36 | 19/19 | 11/11 | 551 |

- A's proposal forgives any land run that reaches a centre on land, so San Francisco Bay, Sydney Harbour and Alcatraz
  would be served again.
- The chosen rule refuses the inner Honolua Bay point. The coast data barely has the bay, and the break itself reads
  as land under every rule (R-B1).
- The three buoys it newly refuses are all in sheltered water: Cockburn Sound, the Helsinki archipelago and
  Lymington.

### The owner's decisions on the re-check (2026-10-02)

5. **Coastline data that lies off the real shore (R-B1): keep the 300 m band, say it better** ("300 m + clearer
   message").
   - A land refusal near the coast will say that the coastline data reads the point as land, and suggest clicking a
     little farther out.
   - Measured on B's cases: 3 of 59 nearshore water points are refused at 300 m (1 km would serve 24 of 39 checked
     land points, against 11 now).
   - A more accurate coastline (OpenStreetMap) is not planned.
6. **Water that sees an open-sea model point through a mouth (R-B2 a): serve as now** ("Serve as now").
   - The header names the model point and its distance.
   - Surf spots in coves and at bay mouths stay served.

### Told to the owner (no change)

- **R-B9:** the live-buoy "Swell & wind-sea (last 24 h)" table now shows the most powerful system of each kind.
- **R-B10:** far from every station a point can take a distant zone (35 N 150 E reads Asia/Magadan, from a grid
  station 720 km away). Points either side of 180 read a day apart.
- **R-A22 = R-B11:** after day 7 a hidden column holds the hour's weakest system by power, which can be taller than a
  shown one.
  - Stations: by up to 0.95 ft.
  - One point cell: 2.5 m hidden beside 3.47 m and 3.35 m shown.

### Fix round 2 scope (UI 1.15.0)

**Server**

1. **The chosen path rule.**
   - The path is judged by exact crossings with the coast edges: longitude unwrapped from the point, the copies on
     both sides of 180 included. `PATH_STEP_KM`, `PATH_LAND_RUN` and `PATH_CELL_SKIP` go.
   - A click on land inside the band with no reachable cell answers "land", not "sheltered" (R-B7).
   - The land message names the coastline data and suggests a click farther out (decision 5).
   - Tests: a spit inside the cell's box, a barrier island, a path across 180 with land beyond it (M24), a centre on
     an islet.
2. A coast cell must lie inside its named box, and a size mismatch drops the kept index (R-A19). The ring samples
   are clamped at the poles (R-A20).
3. `rank_groups` keeps a group that has a period or a direction but no height (R-A23).
4. The fetch cap: the timer shuts the socket down, and the clock starts before the request. The dead "cut short"
   check and its test go (R-A2).
5. Time zone: a station's zone only within 3 hours of the point's nautical offset (R-A3).
6. No raw "Etc/GMT+N" anywhere, and one form ("UTC−11") on the screen (R-A17, R-B4).
7. The tool shows only when a points bucket is set (R-A18). The no-script page of an invalid id is fixed (R-B12).
8. Docs (R-A21).

**Page**

9. A point's graph series carry only their rows (`{x: slot, y}` on the hourly axis), so the tooltip and the sync
   follow the nearest row (R-A4).
10. Loading and keys:
    - A late answer is dropped when the visitor picked something else since the click (R-A6).
    - Escape with the favourites list open closes only the list (R-A7).
    - A refusal is forgotten when a forecast for the id lands (R-A8).
    - The note, the run text and the model bar are cleared on a failed load (R-A9, R-A10).
    - The title keeps the point's label (R-A11).
11. Names, words and focus:
    - Names are cut by graphemes (R-A12).
    - A stored name that is not a string is dropped (R-A13).
    - Words (R-A14, R-B5, R-B6).
    - The star ticks the layer on (R-A15).
    - Focus is kept across another tab's change (R-A16) and goes to the window after Retry (R-B8).
12. On 360-375 px phones an unnamed point's coordinates are never cut (R-B3).

**Tests**

13. Pins for the survivors that matter (R-A5). One land fixture is checked by both the page's `inLand` and the
    server's `land_parity` (R-A24, R-A25). The mutants are re-run on the new code.
14. Release steps:
    - UI 1.15.0 with `ui_assets.json`, the golden in its own commit, and the README.
    - Push, and the test site.
    - A short MAX re-check.
    - Production with the owner's go-ahead: tag `prod-pre-point` @ `4d37b8e`. The release note says the station
      tables change order on purpose.

**Follow-up (older code, not in this round):** the stations' `.spec` download uses the same timer pattern as R-A2.

### Still wrong after fix round 2

- Coast data that lies more than 300 m off the real shore: refused, with the clearer message.
- Water that sees an open-sea model point through a mouth: served from that point (decision 6).
- Atoll islets missing from the coast data, and sea ice: as before.
- North of 52.25 N: NOAA's interpolated grid.
- A point reads the nearest sea cell, while stations are interpolated to the buoy.

## Fix round 2 (2026-10-02, UI 1.15.0)

Commits on `feat/forecast-point`: `e51cbdc` (code and tests), `d1c8440` (golden tests: forecast points follow their
own address), `ffb51d2` (golden, its own commit), then a follow-up commit with the mutation pins below.

### What changed

**Server**

| Item | Finding | Change |
|---|---|---|
| 1 | R-A1 = R-B2 (b, c), R-B7, owner decision 5 | The path to the cell is judged by exact crossings (`path_crossings`, `land_runs`, `path_blocked`); it starts at the nearest water inside the 300 m band (`water_origin`: rings every 50 m, 32 directions); runs that touch at a cell line are one run; a click on the band's land with no reachable cell is "land"; the land message names the coastline data |
| 2 | R-A19, R-A20 | A coast cell must lie in its own box; a size mismatch reads the index again; the band's samples are clamped at the poles |
| 3 | R-A23 | `rank_groups` keeps a group without a height, after the ranked ones |
| 4 | R-A2 | The fetch timer shuts the socket down; the clock starts before the request; the dead "cut short" check and its fake test are gone |
| 5 | R-A3 | A station's zone only within 3 hours of the point's nautical offset (`_zone_near_nautical`) |
| 6 | R-A17, R-B4 | `zone_label`: no raw "Etc/" zone on the no-script page or in the classic table |
| 7 | R-A18, R-B12 | The tool shows only where a points bucket is set; an id that is no station keeps its own option |
| 8 | R-A21 | README and docstrings corrected |

**Page**

| Item | Finding | Change |
|---|---|---|
| 9 | R-A4 | A Chart.js interaction mode of the page's own (`allshoreRow`) takes the nearest ROW for the tooltip and the three-chart sync on a point's graphs; stations keep `index` |
| 10 | R-A6 .. R-A11 | A late answer after another pick ends the tool; a refusal is forgotten when a forecast lands; a failed load clears the note, the run text and the model bar; the title keeps an id without an option; Escape with the list open closes the list |
| 11 | R-A12 .. R-A16, R-B5, R-B6, R-B8 | Names are cut by graphemes; a stored non-text name is dropped; the star shows the layer; focus stays on its row when another tab changes My points; Retry keeps the focus in the window; HTTP errors and network failures are told apart; the folded bar says "Checking that point…" and offers no Clear |
| 12 | R-B3 | On phones the run text gives way first (`flex: 0 1000 auto`) |

### Measured

- **Path rule**, the repo's code against the prototype on the same sets (offline run, published coast):
  - B's 105 cases, B's 30 lagoons, the 596 live-buoy positions and A's 3,000 near-coast points.
  - It differs only where intended:
    - clicks on the shore band's land now answer "land" instead of "sheltered" (R-B7);
    - two buoys and one near-coast point take a nearer cell. A centre on land that a cell line cut into two runs is
      now one forgiven run.
  - Sheltered waters 19/19 refused, surf spots 35/36 served, buoys 551 served.
  - 1.4-6.6 ms a point.
- **Graphs**, with the real Chart.js 4.4.2 in a local preview:
  - A point (`pt_24547N_157896W`): a tooltip on the nearest row at 175 of 175 pointer positions across days 6-16,
    and the direction chart synced at the same row 175 times.
  - A station (51201) keeps `index` and `nearest`, with tooltips at 37 of 37 positions.
- **Suites**:
  - pytest: 586 passed before the golden; the golden tests pass after it.
  - Node: 342 passed, the CI list.
  - A shared land fixture (`tests/fixtures/coast/land_parity.json`, 500 points) gets the same answers from the
    server's `land_parity` and the page's `inLand`.
- **Mutation:** 59 mutants of this round's code (server and page), run against the repo's own suites in a full
  sandbox copy: 59 killed.
  - At first N40 survived (the model bar after a failed load): the test's bar was already hidden before the failure.
    It was reordered.
  - N15 survived too (a cell across 180, its end not unwrapped from the point). It is now pinned by a locate test at
    179.85 E.
  - Two mutants first looked killed only because the sandbox lacked `static_icons`. They were rerun in a complete
    copy and are killed by their own tests.
- **Suites after the round:** pytest 589 passed, Node 342 passed.

### Still open after fix round 2

The items of the re-check's "Still wrong after fix round 2" list. Also the follow-up for the stations' `.spec`
download timer (older code; R-A2's note).


### The owner's decision on the location line (2026-10-02)

7. **A point's Location line shows only the clicked point's coordinates** ("it can be eliminated and just show the
   gps coordinates for the location of the click"). The table, the graphs and the no-script page read "Location :
   21.700N 158.200W". The model point's position and distance stay in the payload's `point` (`cell_lat`, `cell_lon`,
   `cell_km`) for the API, but the page no longer shows them. Decision 6's note that "the header names the model
   point and its distance" no longer holds.

## Test-site verification of fix round 2 (2026-10-02/03, UI 1.15.0 then 1.15.1)

`test` @ `12ebe57` (1.15.0), then @ `c8d48b7` (1.15.1, two fixes found here). The served `/ui/*.js` match the repo byte
for byte. Done in the in-app browser at 1280x800, 375x812 and 360x780, with real clicks, keys and mouse moves where
the pane allowed. Where the pane reported the page hidden, the steps were driven by script through the page's own
code.

**Stations unchanged.** Seven station answers captured before and after the deploy, on the same GFS run 2026100218,
are byte-identical in their tables and graph data. The checked answers: 51201 (GFS, SWAN, Metric), 46026, 13130,
41001, and 46001 in UTC.

**The server, live:**
- **Refused as sheltered:** Pearl Harbor, San Francisco Bay, Venice, the Solent, Pamlico Sound, Southampton Water,
  Chichester Harbour, Alcatraz and the Lymington gauge.
- **Refused as land, inside the shore band:** Indian River Lagoon, inner Kaneohe Bay, Venice north and the Sandy Hook
  spit.
- **Served:**
  - The Cable Beach buoy, from the open-sea cell (18.0 S 122.0 E).
  - Tokyo Bay, the Kaiwi Channel, Cloudbreak, Mundaka, Pipeline, Teahupoo, Hossegor and Mavericks.
  - Through a mouth (decision 6): Golden Gate inside and Cowes Roads.
- **Refused with the new land message:** Puerto Escondido (1.3 km out), the Honolua break, Peahi and central Oahu.
- **Time zones:**
  - Off the Commander Islands and off Kamchatka: Asia/Kamchatka.
  - 340 km north of Oahu: Pacific/Honolulu. Near 46006: America/Los_Angeles.
  - 35 N 150 E: Asia/Magadan (the rule as decided).
  - Southern Ocean: shown as "UTC-8" in the classic table and on the no-script page.
- **Other:** the Black Sea answers "nodata"; the unknown id keeps its own option; the tool shows with the bucket set.
- **Location line (decision 7):** the click's coordinates only. Requests took 0.4-1.9 s, and 6.2 s for a cold region.

**The page, live:**
- **The Forecast point tool:**
  - A land click gets the new message in the bar, and the tool stays on.
  - A water click opens the window: title "23.624N 158.599W", meta "Location : 23.624N 158.599W".
- **Graphs:** 24 of 24 real hovers across days 6-16 show a tooltip on the nearest row, and the other two charts follow
  it (R-A4).
- **Tool states:**
  - While a point is checked, the folded bar says so and offers no Clear (R-B5).
  - A late answer after a pick from the list opens nothing and ends the tool (R-A6).
  - An HTTP 503 reads "The forecast server could not answer (HTTP 503)"; a failed connection reads "Could not reach
    the server" (R-B6).
- **Keys:** Escape with the favourites list open on the minimised window closes the list only; the next Escape ends the
  tool (R-A7, B-13).
- **A failed load** clears the run text, the SWAN/GFS bar, the table and the meta line, and Retry leaves the focus in
  the window body (R-A10, R-B8).
- **My points:**
  - The star with "My points" unticked ticks it again and shows the markers (R-A15).
  - A point added in a second real tab rebuilds the list and keeps the focus on the same row's Rename (R-A16).
  - A rename with a family emoji at the 40th character keeps it whole (R-A12).
- **Ids that are no station:** `NOPE9` and `bad!id` keep their names in the title. "Invalid station id" has no Retry.
- **Phones:** at 375 and 360 px, in the bar and full screen, an unnamed point's coordinates are whole (133 of 133 px; R-B3).
- **Console:** no errors.

**Found and fixed during the verification (1.15.1, `7045add` and golden `452ee30`):**
- The overlay's valid-time zone and the live-buoy panel still used Intl's "GMT-8" beside the window's "UTC-8". The
  page's `tzAbbr` now writes a nautical zone as the window does; checked live.
- The folded tool bar said "Checking that point..." twice, as title and line. The line now goes when folded; checked
  live.

**Noted, not changed:**
- An id that passes the id rule but has no NOAA bulletin (`NOPE9`) gets NOAA's old wording ("No .bull file found for
  NOPE9", with Retry). This predates the forecast points; the station code is unchanged.
- The test branch's `test_importing_the_app_does_not_load_numpy` fails there already at `0782008`: its reef module
  imports numpy.
