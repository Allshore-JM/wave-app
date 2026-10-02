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
| A-1 | The compact table applies the stations' "a column needs a value in the first 7 days" rule to a point's TRAIN columns: a swell that starts after day 7 has no table column. `pt_46250S_96000E`, Fri 10/16 9 PM: three empty swell cells beside Comb. 9.99 m; 4.2 % of 1,200 cells hide a column. Reproduced by the author on the test site. | Fixed by the owner's column decision below (rows in rank order: a hidden column can then only hold the smallest system of its hour, as at a station). |
| A-2 | Water behind land is served from the cell beyond the land: Long Island Sound from the Atlantic 28 km away, San Francisco Bay from the Pacific 31 km, Tampa Bay, the Solent, Pearl Harbor. 11.1 % of 1,574 served nearshore water points have 0.5 km or more of land between point and cell. Reproduced by the author. | Owner: refuse. The cell must be reachable from the point over water (fix round, item 1). |
| B-1 | A point's graphs give each ROW the same width: days 1-5 (hourly) are 123 px each, days 6-16 (3-hourly) 41 px, with no cue; a station's days are 67 px each. | A point's graph data goes on hourly slots (385 labels, empty between the 3-hourly rows); the table keeps its 209 rows. Removes B-14 (day line and night shading up to 2 h late) and A-15. |
| B-2 | Points away from the coast show "Etc/GMT+11"-style zones (the name means UTC-11) and not the zone the station at the same place uses (46006: station America/Los_Angeles, point Etc/GMT+9; 340 km north of Oahu: Etc/GMT+11 beside Pacific/Honolulu). Reproduced by the author. | Owner: the nearest station's zone. Raw "Etc/" names are never shown ("UTC-11"). |

### P2

| # | Finding | Outcome |
|---|---|---|
| A-3 | A bad tile body is cached before it is checked: the tile stays "temporarily unavailable" until the run changes; Retry cannot help. | Check before caching; a test that asks again after the bucket recovers. |
| A-4 (= B-15) | A point's "Swell n" is a swell TRAIN, a station's is the hour's rank: at a point "Swell 1" holds the hour's highest system in 50.7 % of rows and is empty beside another system in 16.6 %. | Owner: rank per hour, as the stations (below). The tracking goes. |
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
2. **Columns: rank per hour, as the stations.** "It should be based on the swell energy ... Swell with largest energy
   should be swell 1, second swell 2, etc. it is ok if swells transition across columns as they grow. this matches how
   the stations swells are ranked". Measured on 225 bulletins (53,577 rows): NOAA lists the systems in height order in
   100.00 % of rows, with no empty column left of a filled one. A system's energy goes with its height squared, so
   energy order and height order are the same order; the period plays no part (an order by height squared x period
   would differ in 35.6 % of rows and name another Swell 1 in 11.2 %). The point's rows follow the same rule, so a
   point at a station reads like the station. The swell tracking of step 4 is removed.
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
3. **Rows and graphs.** Rows in rank order per hour (height descending; the wind sea among the swells, as in a
   bulletin), packed from the left; the tracking code, its constants, tests and words removed; the table's 7-day
   column rule as for the stations. Graph data on hourly slots; `rangeWindow` counts rows again (exact for stations as
   on production).
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
