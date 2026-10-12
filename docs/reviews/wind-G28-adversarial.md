# G28 — adversarial review of "Live wind stations" (plan section 39)

- Target: `feat/wind-stations` @ 7aeb072 (merge of production b497661 into the branch); review diff `b497661..7aeb072`
  (44 files). Test site `test` @ 03181b2 = the same site files + 3 test-only logo lines.
- Reviewers (fresh context, Opus 5.5 at MAX; the owner chose Opus over Fable to save tokens, with a deeper brief for
  the concurrency code and the short re-check kept): **A** code + data (no browser; own detached worktree; staged
  experiments over the real code and snapshot; a 113-mutant pass), **B** test site + use (headless Edge, trusted mouse /
  touch / key input, 1280x720 .. 1920x1080, tablets, phones portrait and landscape).
- Brief: requirements (14 owner decisions), the author's claims, known items K-1..K-13, rules (no repo edits, gentle
  with the agencies, never kill processes by name).
- Reports (scratch): `g28/a/g28-a-report.md` (P0 0 · P1 1 · P2 5 · P3 4), `g28/b/g28-b-report.md` (P0 0 · P1 1 · P2 5 ·
  P3 7). Overlaps: A-7 = B-9 (duplicates), A-10 "My points" = B-5, A-10 "5-minute refresh vs 10-minute cache" = B-3,
  A-10 README drift + B-10.

**Combined: 0 P0, 2 P1, 10 P2, 10 P3.**

## Findings

### P1
- **A-1 — the CO-OPS history path has no request budget.** `WindHistory._coops` + `_coops_weather`: one
  `product=wind&range=24` request plus one per `wx` letter (up to 5 per gauge window, sequential); `_metar` / `_nws` take
  a token first, `_coops` / `_ndbc` take none. Staged (232 real gauges, 4 request threads, 0.15-0.3 s fake latency):
  395-776 CO-OPS requests a minute from ONE client, 67-130 in the busiest 10 s — the burst pattern that got the test
  server's address blocked by NOAA in step 5 (and a block also breaks the tide windows; the tide routes share the gap,
  pre-existing). Author: confirmed in the code.
- **B-4 — offshore NDBC buoys read "No recent reading" up to ~45 % of every hour.** `latest_obs.txt` holds ONE line per
  station; when the newest is a wave-only report (WSPD "MM") `parse_latest_obs` skips it and `NdbcWindProvider` rebuilds
  its list from the file, so the station's reading vanishes until its next wind line. Monitor (25 samples over an hour):
  51001, 46026, 41013, 45012 without a reading in 44 % of samples, 51003 28 %; their windows show "18 mph from NW · 54
  min ago" meanwhile. Author: confirmed in the code (no per-station keep in the NDBC feed).

### P2
- **A-2 — a 403 / 429 does not stop the slice in flight** (`PacedProvider._fetch_stations`: `pool.map` submits all
  24 / 25 requests; the refusal is acted on after they all returned). Staged: 24 of 24 still sent after a 403 on the first.
- **A-3 — METAR visibility "6+" (the international 9999) served as 16.0 km** (`metar_visibility_km`: `max(km, 16.0)` for
  any "+"). RJTT / EHAM rows on the test site; ~1/3 of cache reports. "10+" also loses its "or more".
- **A-4 — any CO-OPS error answer counts as "no data"**: the feed drops the kept reading without counting a failure; the
  history answers "No wind readings in the last 24 hours", cached 600 s, without trying the NDBC relay; the weather
  products likewise. Section 38 already learned NOAA sends passing messages (only "no data was found" is final).
- **A-5 — the wind refreshes share the live buoys' 3-worker runner**: staged, three hung wind fetches left 0 of 10
  live-buoy lists published; `/healthz` `fetch_stuck` counts only the live providers, so it stays green. (Exceptions are
  isolated — verified; slow / hung feeds are not.)
- **A-6 — a gauge window holds a request thread for up to 5 sequential CO-OPS requests** (~45 s at the per-request
  cap); wind (2) + tide (2) + points (2) build pools exceed gunicorn's 4 threads, and CO-OPS slowness hits wind and tide
  windows together: a few visitors during a NOAA slowdown can hold every request thread (`/healthz` included).
- **B-1 — a solid ring (and sometimes the arrow base / the number) opens the FORECAST point when a forecast dot is 7-12 px
  away** (touch; 7-8 px with a mouse). `windDotHit` sends a click within 8 px (12 px touch) of the dot to the forecast
  point, but the ring is drawn hollow only within 7 px: the most obvious target opens something else with no visual
  cue (44009, 41024, La Jolla, 41008; trusted taps / clicks, shots).
- **B-2 — landscape phones: the parked wind bar is squeezed to 27-42 px** under the tide bar (812x375, 667x375, 844x390):
  the desktop `max-height` of `.fwin.wind-win` still applies to `.fw-min` in the phone block.
- **B-3 (= A-10 note) — the window's headline reading is often older than the flag just clicked**: history cached 600 s
  on the server + `max-age=300` in the browser; the window uses the history's last row and the flag's reading only when
  the history is empty. Monitor: behind the flag in 19 % of samples (Honolulu 12 of 25 minutes).
- **B-5 (= A-10 note) — My points under / beside a flag cannot be tapped on phones** (every tap within ~15 px opens the
  wind window; desktop loses the centre): `windHollowSet` / `windDotHit` consider only the yellow forecast dots.
- **B-13 — four windows: the "every window keeps an edge to click" rule breaks at 1440x900, 1920x1080 and on tablets**:
  the top-right wind window (since 5b) covers the strip the live window's top relied on and the tide window (same z,
  later in the DOM) covers the rest, so the live window has no visible part whenever another window is in front (and the
  tide window none behind the forecast window on tablets / 1920x1080). Holds at 1280x720, 1280x800, 1366x768. Author:
  confirmed by the default boxes' geometry (the earlier sweeps checked pairs, not three windows over one).

### P3
- **A-7 = B-9 — the same station twice**: 13 pairs within 100 m; 7 offshore platforms as both `metar:` and `ndbc:` (KATP,
  KBQX, KGBK, KGHB, KGVW, KSPR, KVAF; different readings), two airport ids at one spot (CWGR/CYGR, CXZV/CYZV), NDBC pairs
  (CDEA2/CDXA2, CSPA2/CSXA2) and gauge/NDBC near-twins (9410230/LJPC1, 8452951/NAXR1, 9468756/NMXA2). From zoom 13 both
  flags are drawn on top of each other.
- **A-8 — METAR pressure mixes sea-level pressure and the altimeter setting** (`p = slp if present else altim`; US SPECIs
  carry no SLP): the trend (threshold 0.5 hPa) is computed across the two kinds (KEWR, KLGA).
- **A-9 — mutation pass: 113 mutants, 91 killed, 22 survived, 11 of them real test gaps** (the "6+" path, a computed
  humidity overwriting a reported one, `conditions()`'s 3-h cut, the trend's ±30-min tolerance, `_num_in`'s lower bound,
  weather products asked with no wind rows, `/healthz` complete all->any, the partial-poll cap, the chart's gap threshold,
  the coarser-image LRU, the wind chip in `--chips-h`); a template change can HANG the page harness instead of failing it.
- **A-10 — small items**: the zoom-18 tide pad returns at 7 co-located gauges (snapshots 1.1-1.4 m apart: match by id, not
  pixels); `WindView.retry()` drops the flag's fallback reading; an NWS station whose 2-h window is empty loses its kept
  reading at once (decision 12: kept 6 h); a CO-OPS reading always wins over a newer NDBC relay reading (outages only);
  `metar_weather_words("+")` -> "Heavy"; `TokenBucket._lock` not reset after a fork (theoretical); `warm_rank` unused;
  NDBC / METAR all-capital names title-cased word by word ("Attu Is, Ak", "Kotzebue Mxak, Ak"); a partial answer after a
  deploy turns every flag grey (`windReadings = info.readings`); 20 seasonal NDBC stations without wind; 2 relays not in
  the snapshot.
- **B-6 — phone readout text clipped** (375 px: "Air 77°F" cut; the box covers the axis labels).
- **B-7 — at <= 1280 px the wind window covers the overlay panel's timeline and colour key.**
- **B-8 (design) — with the wind window open, the clicked flag and most neighbours are under it** (1280x800 Honolulu z11:
  8 of 40 flags still clickable; the tide window has the same pattern).
- **B-10 (+ A-10) — docs**: README "three BuoyProviders" (four), "every station from zoom 11 (... WIND_ALL_ZOOM)" (13),
  "227 relays / 1,797 airports" (224 / 1,778), the touch disc "33 px" (36); the NWS source link opens raw JSON.
- **B-11 — pans in dense views get janky**: every moveend that changes the drawn set rebuilds every flag (8-13 long tasks
  up to 76 ms per 30 drags with the layer on vs 1 off; `rebuildWindMarkers` up to 23.6 ms for ~57 flags).
- **B-12 — an open / parked wind window polls its history every 5 minutes in a hidden tab.**

## Held up (both reviewers, with numbers in the reports)
Every compared value equals the agency's at the same timestamp except A-3 (latest table: NDBC 125, METAR 1,544,
CO-OPS 15, NWS 14; full 24-h histories with every weather field for 8 gauges / 6 NDBC / 5 METAR / 5 NWS); routes,
headers, 304s, id validation; snapshot integrity (ids, zones, wx letters 6/6, NWS kinds 296/296, NDBC part rebuilt
identically, 30-km rule 0 errors in 450 airports); flags: 2,436 real flags on 19 coasts = an independent band / number /
arrow rule, 2,880 synthetic flags with the number never under the arrow; thinning monotone and in rank order; date line;
in-place updates keep elements + focus; keyboard + accessible names; gate / legend / credit; the feed's cadence (every
5 min, never hidden or below the gate); fault handling; station lists 304; zoom 18 never shows Esri's placeholder;
tools work through the flags; no memory leak (12 min of panning; Python soak flat); exception isolation of the wind
feeds; fork resets; the section-40 merge; window boxes = production's at 1280x800.

## Owner decisions (2026-10-11, AskUserQuestion; all as recommended)
1. **B-1 ring vs dot**: the ring is drawn HOLLOW whenever the forecast dot would take the tap (within 8 px with a mouse,
   12 px on touch); a solid ring, its arrow and its number always open the wind window.
2. **B-7 / B-8 window spot**: the wind window stays at the top right but its left edge never covers the overlay panel
   (narrower up to ~1440 px).
3. **B-13 four windows**: while the wind window is open, the live and tide windows' default boxes start below it, so
   every open window always shows a strip to click.
4. **A-7 / B-9 duplicates**: one flag per spot, METAR first (drop the NDBC copy of a METAR platform and the second id of
   an airport); offshore platforms stay (this also settles K-13).

## Fix round scope (step 7)
Split in two parts, each with its own cue; then the test-site verification (MAX) and a short fresh re-check (MAX).

**7a — server + snapshot**
1. A-1: one CO-OPS request budget (token bucket) for on-demand requests shared by the tide service and the wind histories
   (the weather products count); empty -> busy (503 + Retry-After, the pages already retry); a modest bucket for the NDBC
   realtime2 histories.
2. A-6: one build cap shared by forecast points, tide windows and wind histories (2 builds at once in all, so 2 of the 4
   request threads stay free); a gauge's weather products fetched concurrently under one per-build deadline (the wind rows
   are served without a product that did not arrive).
3. A-5: the wind feeds on their own refresh runner (the live buoys' 3 workers never wait for a wind fetch); `/healthz`
   reports wind fetches that are stuck (without making `ok` depend on the wind feeds).
4. A-2: the first 403 / 429 stops the rest of the slice before it is sent.
5. A-4: only NOAA's "no data was found" answer is "no data"; any other message is a failure (feed: the reading is kept and
   counted; history: an error -> 60 s and the NDBC relay); the same rule for the weather products.
6. B-4: the NDBC feed keeps each station's last reading (per station, like the paced feeds; dropped after the keep window),
   so a wave-only newest line never blanks a buoy.
7. A-3: METAR visibility keeps its "or more": "6+" / "10+" served as the lower bound with a flag, shown "6+ mi" /
   "10+ km" (not 16 km).
8. A-8: METAR pressure from one quantity per station (the altimeter setting, always present) so the trend compares like
   with like.
9. A-10 server items: an NWS station keeps its reading for the keep window when its 2-h answer is empty; the newer of a
   gauge's CO-OPS reading and its NDBC relay wins; `metar_weather_words("+")`; the TokenBucket lock reset after a fork;
   `warm_rank` removed or used; NDBC / METAR all-capital names through the same case rules as NWS names.
10. A-7 / B-9 (owner 4): the snapshot tool keeps one station per spot, METAR first (NDBC copies of METAR platforms and the
    second id of an airport dropped; the gauge / NDBC near-twins 9410230 / LJPC1, 8452951 / NAXR1, 9468756 / NMXA2 folded
    as aliases); snapshot rebuilt from the caches.
11. Tests for every item + A-9's 11 server / client gaps; docs (B-10, A-10 README drift).

**7b — page + client**
12. B-1 (owner 1): hollow ring whenever the dot takes the tap (8 px mouse / 12 px touch).
13. B-5: My points under the same hollow / dot-tap rule as the forecast dots.
14. B-2: the parked wind bar on phones is never capped (landscape included).
15. B-3: the window's headline = the newer of the history's last row and the flag's latest reading (added to the chart /
    table when newer); the history's browser cache shortened.
16. B-7 / B-8 (owner 2): the wind window's left edge right of the overlay panel on desktops.
17. B-13 (owner 3): while the wind window is open, the live and tide windows' default boxes start below it; a four-window
    sweep over many sizes checks the UNION of the others over each window (not pairs).
18. B-6: the phone readout shortened to fit; B-11: a pan rebuilds only the flags that change; B-12: the window does not
    poll while hidden or parked (it catches up when shown).
19. A-10 client items: the zoom-18 tide pad matched by id, not pixels; Retry keeps the flag's fallback reading; a partial
    answer keeps the readings of the feeds it names as missing.
20. K-1: Metric temperatures always with one decimal; K-2: the overlay timing test on fake timers; the page harness gets
    its own deadline (A-9's hang); the NWS source link to a readable page.
21. UI version bump, golden re-captured in its own commit, README.

## Fix round 7a — server + station list (2026-10-11, Opus 5.5 HIGH) @ 1f43e25 + f55e790 (local)
Every item of 7a done as scoped. Deviations / details:
- Budgets: CO-OPS on-demand 30 a minute, 15 at once (`COOPS_ON_DEMAND_*`), ONE bucket in `app.py` (`_COOPS_BUDGET`) passed to
  the tide service (`TideService(bucket=)`: every `_json` takes a token, else `TideBusy` -> busy) and the wind history
  (`WindHistory(coops_bucket=)`: a gauge window takes 1 + its products' tokens before the first request); NDBC histories
  30 a minute. Reviewer A's `exp/coops_burst.py` re-run on the fix: 232 gauge windows at once -> 15 CO-OPS requests in
  all (was 547-655), the rest busy (the page retries).
- Build cap: `buoy_sources.SharedCap(2)` = `_REQUEST_BUILDS` = `_POINT_BUILDS`, passed to both services (`cap=`); reset
  after a fork (it was never reset before for the points either). Weather products asked together (a small pool),
  `WX_DEADLINE_S` 10 s for all; none asked without wind rows.
- Runner: `BuoyProvider.refresh_runner()` hook; `WindProvider` -> `wind_sources.get_wind_runner()` (ThreadRunner,
  `WIND_REFRESH_WORKERS` 1-4, default 2; reset after a fork). Reviewer A's `exp/runner_starve.py` re-run: with three wind
  fetches hanging, 10 of 10 live-buoy lists published (was 0). `/healthz` `wind.runner` + `wind.fetch_stuck` (reported).
- Paced slices: a shared stop flag checked before each request; at most `workers - 1` more after the refusal; the cursor
  rewinds to the first station not asked.
- CO-OPS messages: `coops_no_data()` ("No data was found"); feed: any other message = a failure (reading kept);
  history: `WindError` (1 min, NDBC relay); products: logged, nothing added.
- NDBC: `_kept` per station (newer only), cut at `NDBC_KEEP_S` 6 h.
- METAR: `metar_visibility()` -> (km, more) and `vg` = 1 in rows, payload arrays (`WX_FLAGS`) and `conditions` (only from the
  visibility's own row); pressure = altim, else slp.
- A-10: NWS `NO_NEW` sentinel (keep the reading); relay newer wins; intensity-only weather groups dropped; bucket locks
  reset after a fork; `warm_rank` removed.
- Station list (owner decision 4): `one_per_spot` (100 m; coops > metar > ndbc; reporting now; id) before the NWS part;
  `_fix_case(places=True)` for NDBC / METAR names. Rebuilt 2026-10-11: 2,727 stations (gauge 232, buoy 210, station 160,
  cman 54, airport 1,777, NWS 294): 14 left out by the rule (CYGR, CYZV, CDXA2, CSXA2, KATP, KBQX, KGBK, KGHB, KGRY,
  KSPR, KVAF NDBC copies, LJPC1, NAXR1, NMXA2), 12 names fixed, the rest daily drift (airports and seasonal buoys).
  The gauge / NDBC near-twins were left OUT rather than folded as second aliases (one alias per gauge in the snapshot).
- Tests: `tests/test_wind_g28.py` 28 + 3 tool tests (incl. "no two snapshot stations within 100 m"); 4 tests updated
  on purpose (NWS keeps a reading, METAR altim + vg, the scheduler fixture records the wind runner). pytest 1,055, Node
  582. Mutation: 37 of 37 killed (`scratch g28/fix7a/mut7a.py`). Reviewer A's W35 survivor (conditions' break ->
  continue) is equivalent: the rows are sorted.

## Fix round 7b — page + client (2026-10-11, Opus 5.5 HIGH) @ 80ad49b + golden b840c04 (local)
Every item of 7b done (UI 1.21.0). Deviations / details:
- B-1 / B-5 (owner 1): `windHollowSet` collects the forecast dots (while that layer shows) and My points (while theirs
  shows); within the tap distance (`WIND_DOT_HIT_PX` 8, `WIND_DOT_HIT_TOUCH_PX` 12 on `(pointer: coarse)`) the ring is
  hollow and a click on the arrow / number within that distance of the dot goes to the dot (`windDotHit` -> `pickDot`:
  a forecast point as its own click, one of My points through `allshore:station`). The old 7 px / 20 px two-mode rule is
  gone. The distance is in the drawn signature.
- B-2: phones `.fwin.wind-win { max-height: none; }`.
- B-3: `setReading` / `joinFallback` in winds.js (only a strictly newer reading, never one older than the kept one; the
  chart and table redrawn); the page calls it on every feed answer for the open station; `WIND_HISTORY_MAX_AGE` 300 -> 60 s.
- B-7 / B-8 / B-13 (owner 2 + 3) — DEVIATION: the owner's option 3 ("live and tide start below the wind window") was
  measured first: at 1280x800 it leaves the live / tide windows ~60 px tall, so it was not built. Built instead
  (`syncWindWidth`, measured by the page; the rule searched over 180 screen sizes in scratch `g28/fix7b/layouts.py`):
  the wind window's left edge right of the logo and the overlay panel (`--map-topleft-r`); while the live-buoy or tide
  window is open its right edge 60 px in from the margin (`--ww-gap`) and, when its own top is not at least 30 px above
  one of them, 30 px wider than that window on the left (`--ww-min-w`). Every open window then keeps a 24 x 24 px area
  outside every window above it, whichever is in front, at all 180 sizes (the old test checked pairs; the new one
  subtracts the union), except the known short-screen case (forecast in front of wind under 570 px tall: Escape). COST
  against owner decision 2: with a live or tide window open, on screens up to ~1,280 px wide the wind window can reach
  over the overlay panel's right part (up to ~108 px at 1180 px, 8 px at 1280x768); from 1,366 px never (pinned).
- B-6: `READOUT_NARROW` 420 px: time · speed compass G-gust · air. B-11: `windByKey` keeps unchanged flags (state = active
  + dot), removes only the ones no longer drawn. B-12: the tick refreshes only while the window shows and the tab is
  visible; `show()` and `visibilitychange` catch up.
- A-10: the tide pad matched by `coops:` id (or < 2 px); Retry passes the kept reading; a partial feed answer keeps the
  readings of the sources it names as missing; NWS source link -> `weather.gov/wrh/timeseries?site=`.
- K-1 Metric temperature `toFixed(1)`; A-3 visibility "6+ mi"; K-2 the three playback relief tests on a fake clock;
  A-9 T10: the page harness's fake fetch has a 100-request budget (a re-ask loop now fails in milliseconds instead of
  spinning the process); A-9 J08 / J12 / B12 pins.
- Tests: pytest 1,055 (1,044 + 11 golden), Node 593; a behaviour test of `syncWindWidth`; the hollow test's marker
  finder made exact (it matched "Honolulu Intl" for "Honolulu" once B-11 kept markers, which hid mutant T04).
  Mutation: 34 of 34 killed (`scratch g28/fix7b/mut7b.py`; 9 survived the first run and got tests).
- NEXT: test-site verification at MAX (cherry-pick 1f43e25, f55e790, 80ad49b, b840c04 onto `test`; the record commits
  stay local), then a short fresh re-check at MAX, then production (tag `prod-pre-wind`).

## Test-site verification of fix rounds 7a + 7b (2026-10-11/12, Opus 5.5 MAX)
`test` @ b5a7952 (cherry-picks of 1f43e25, f55e790, 80ad49b, b840c04; README kept as test's copy; site files = the
feature branch except test's 3 logo lines). Pushed 23:00:05 UTC, the new instance served from 23:01:07: 578+ polls of
`/robots.txt`, `/healthz` and `/api/wind/latest` across the switch, none failed; the wind table complete 43 s after the
start (partial before, as designed). Served UI assets (winds, forecast, tides, basemap, tools, livelist, graticule) = the
repo at 1.21.0, immutable + nosniff; the 1.20.0 URL 404. Test branch: Node 592 pass; pytest 911 + the 4 known failures
(3 test-logo goldens, the reef module's numpy import). Scripts: scratch `g28/verify7` (watch7.py, data7.py, memwatch.py)
and `wind/verify5/vhl` w30-w36 (headless Edge, trusted mouse / touch / keys).
- **Server (7a)**: snapshot 2,727 stations, 0 pairs within 100 m (A-7/B-9); NDBC/METAR names end in "AK" (A-10). History
  answers `public, max-age=60` for all four sources (B-3). METAR against AWC's own cache and API: 732 snapshot airports
  report "6+"; at SBFL, SYCJ, NGTA, PHNL, KSFO, KJFK every common row has the agency's visibility with the "or more" flag
  exactly and the pressure = the altimeter setting (A-3, A-8; 0 mismatches). CO-OPS burst (A-1/A-6): 12 gauge windows
  with weather products + 3 tide windows at once: 5 answered at once, 10 "busy, Retry-After 5", all served within 80 s by
  retrying like the page; during it `/robots.txt` p50 0.14 s (max 0.28) and the live list p50 0.14 s (max 1.96), no
  NOAA 403, the CO-OPS feed without an error. Feeds over 44 min: 0 errors, 0 pauses (CO-OPS 230 / NWS 286 readings);
  B-4: reviewer B's buoys 51001 46026 41013 45012 51003 present in all 578 polls (B: absent in 44 %); tide canary 73/73;
  `/healthz` wind runner 2 workers, `fetch_stuck` false (A-5).
- **Layout (7b B-7/B-8/B-13)**: all four windows open (forecast 51201, live 51201, tide and wind Honolulu = the tallest
  wind window), 10 sizes 1024x768-2560x1440, each window in front in turn: every other window has a visible area of at
  least 24 x 24 px and a TRUSTED click there brings it forward (0 failures). Exact sweep of the real boxes (largest
  visible rectangle, compressed coordinates) over 190 sizes: >= 24 px everywhere except 540 px tall (forecast in front
  of the wind window: 12 px; the known short-screen case, Escape). Tightest: 24 px at 640-740 px tall (the tide window's
  strip below the wind window while the live window is in front: the wind window's height cap ends 42 px above the
  bottom, the tide window 18 px). Overlay panel covered by the wind window (with a live / tide window open): 1024 wide
  194-254 px, 1100 118, 1180 38, from 1280 none (800 tall); none at 1080 tall. At those widths the live and tide windows
  (production since section 38) already reach over the panel (left edges 186 / 126 at 1024).
- **Rings (B-1/B-5)**, buoy 46092 beside its forecast dot: mouse, 6.4 px -> hollow: the number 3.6 px from the dot loads
  forecast 46092, the number 9.5 px away opens the wind window, the dot itself loads the forecast; 9.2 px -> solid: the
  ring opens the wind window, the dot beside it loads the forecast. Touch (375x812): 9.2 and 11.4 px -> hollow, a tap on
  the number near the dot loads the forecast. One of My points 2 px from Honolulu airport's flag: hollow; the arrow 5.4
  px from it opens the point; the number 9.5 px away opens the wind window.
- **Window (B-3, A-10, K-1, B-10)**: the history answered without its 3 newest rows (as an older cached copy) -> the
  headline and the table's first row are the flag's newer reading (23 min). 22107 (no 24-hour file) with its first answer
  failed (injected 500) -> error + Retry; a trusted Retry -> the flag's reading and "No 24-hour history for this station".
  A feed answer without the CO-OPS rows and naming COOPS partial -> the 230 gauge readings and their flags kept, the note
  "loading more…". Metric "Air 28.5°C · Water 28.9°C"; PHNL "Visibility 10+ mi"; the NWS source link opens NOAA's
  Time Series Viewer.
- **Polling (B-12)**: the window parked 6 min in a visible tab -> 0 history requests, expanded -> 1 at once; another tab
  in front 6 min (visibilityState hidden) -> 0, shown -> 1 at once.
- **Phones**: A-10 at zoom 18 at 3 gauges whose tide and wind entries are 0.37-0.55 m apart: the tide icon has no pad, a
  tap 14 px from the centre opens the wind window, the icon the tide window. B-6 readout "4:48 AM · 2 mph E G3 · 76°F"
  inside the 351-px chart (desktop keeps the long line). B-2 sideways 812x375 with all four parked: the wind bar 51 px,
  whole, `max-height: none`, the bars stacked; it opens full screen (z 3500). The map keeps its 180-px minimum, so with
  four bars it runs 11 px under the wind bar (the documented limit).
- **Pans (B-11)**: 30 trusted drags, Honolulu z11 (51 flags): 0 long tasks (B on the old code: 8-13 up to 76 ms),
  rebuild median 2.6 ms / max 5.4 (was up to 23.6), 49 of 51 flags kept per pan; South Florida z9 (26 flags): 0 long
  tasks, 1.8 / 4.9 ms, all kept.
- Console clean in every run (the only error is the 500 injected for the Retry check).
- **Memory**: 107 MB at the switch -> 182 MB after the first hour of heavy test traffic (~50 page loads, 25 histories with
  weather, bursts), then flat at 177 MB for 11 idle minutes while the feeds published every minute (CO-OPS / NWS
  versions 59 -> 70): the caches are bounded and the feeds do not grow.
- Findings (none blocks):
  - V-1 (P3) the 24-px strip at 640-740 px tall is exactly the rule. Optional: the wind window's bottom reserve 50 -> 56 px
    makes it 30 px (the window then scrolls 6 px sooner on short screens).
  - V-2 (owner awareness, decision 2's cost as measured above) the overlay panel at tablet widths.
  - V-3 (documented limit) a phone held sideways with all four windows parked: the map keeps its 180-px minimum and runs
    11 px under the wind bar.
- NEXT: a short fresh re-check at MAX (code of 7a + 7b and the test site), then production (tag `prod-pre-wind`).

## Re-check of fix rounds 7a + 7b (2026-10-12, Opus 5.5 MAX, two fresh reviewers)
- Target: `feat/wind-stations` @ 890a901 (code b840c04) / `test` @ b5a7952 (UI 1.21.0). Brief: scratch `g28/recheck/brief.md`.
  R1 code + data (no browser, own worktree, staged experiments over the real services, 54 own mutants):
  `g28/recheck/r1/recheck-r1-report.md` (P0 0 · P1 0 · P2 3 · P3 5). R2 test site + use (headless Edge, trusted mouse /
  touch / keys, 4 opening orders, parked mixes, drags, 125 / 150 % zoom, tablets, phones): `g28/recheck/r2/recheck-r2-report.md`
  (P0 0 · P1 0 · P2 4 · P3 2). Overlaps: R1-N3 = R2-4, R1-N7 = R2-5.
- **Combined: 0 P0, 0 P1, 6 P2, 6 P3.**
- G28 verdicts: FIXED A-1 (232 windows at once -> 15 CO-OPS requests; real wiring: busiest minute 44 <= 45, 10 s 19 <= 20),
  A-2 (24 staged refusals: <= workers-1 in flight, none after, cursor right), A-4, A-5 (10 / 10 live lists with three hung
  wind fetches), A-6 (a gauge window holds a thread 2 s / 11 s / 18 s at 1 s / hanging products / the fetch cap), A-8,
  A-9 (9 of A's 11 gap mutants killed, 2 covered), B-2, B-3 (the window never older than the flag at 4 stations), B-4,
  B-6, B-10, B-11 (focus kept through pans and repaints), B-12 (visible: one refresh at 308 s; parked / hidden 5.5 min:
  0, then 1), K-1, K-2; FIXED with new edges B-1 (R2-3, R2-4, R2-6) and B-5 (R2-4); PARTLY A-3 (R2-5), A-7 (R1-N6),
  A-10 (names, R1-N8), B-13 (R2-1, R2-2); B-7 fixed at the stated cost, which reaches to ~1,256 px wide (where the wind
  window becomes wider than the tide window: 254 px at 1024, 178 at 1100x820, 126 at 1152x648, 49 at 1229x691 @125 %,
  0 from 1240), not "~1,180" as the brief said; B-8 accepted. Nothing else (gear, tools, legend, chips) covered in any run.

### P2
- **R2-1 — decision 17 broken after an ordinary park**: `syncWindWidth` listens to the window events but is registered
  BEFORE the listeners that set `body.live-chip / tide-chip / wind-chip` and run `syncChips()`, so it measures the boxes
  from before the change; the forecast window's park (`body.fw-chip`, `syncForecastChip`) does not run it at all. Parking
  the live window (1180-1255 px wide) or the forecast window (below 1180 px, 787-939 px tall) raises the tide window to
  the wind window's top, the stick-out is not recomputed, and with the tide window in front the OPEN wind window has no
  visible part (1200x800, 1180x820, 1100x820, 1024x819 @125 %, 1152x864; 11 px at 1240x900). Author: confirmed in the code.
- **R2-2 — the live window hidden under the tide window when both are raised by the same rule**: below 1180 px with the
  forecast window parked (a new tab's default; pre-existing since section 38, RC-13) both get the same bottom and, up to
  ~840 px tall, the same capped top; the tide window is wider and later in the page, so it covers the live window
  completely (1024x768 with only live + tide = production's layout; 1093x614 @125 %, 1152x648, 1100x820). Author: the same
  pattern ALSO occurs on desktops with the WIND window parked (`body.wind-chip` gives both the same bottom and cap): the
  author's own w30 run ended with live [442,188,820,541] inside tide [382,188,880,541] at 1280x800, missed in the
  test-site verification; this variant is new with section 39.
- **R2-3 — phones: a visible forecast dot 12-18 px from a station cannot be tapped** (regression of the B-1 fix): the
  solid ring's invisible 36-px touch disc covers it and opens the wind window (390x844, 46092: 12.8 / 15.0 / 16.3 px).
- **R1-N3 = R2-4 — ticking / unticking "Forecast points" or "My points" does not redraw the flags**: a hollow ring stays
  hollow over a hidden dot (its number / arrow opens the HIDDEN forecast point or point), a re-shown dot sits under a
  solid ring (B-1 again) until the next pan or zoom.
- **R1-N1 — a forecast point answers "busy with other forecast points" while two tide / gauge windows build** (the build
  cap is shared since 7a; the points' fast-busy rule sees only point builds; the forecast window shows the error with a
  manual Retry, no quiet retry).
- **R1-N4 — slow NOAA + ~15 pending tide / gauge windows: the cap's 2-s waits fill the free request threads** (staged
  gthread model: a /healthz-like request p50 5.1 s, max 12 s; 6 windows: max 4 s; healthy NOAA: max 1.6 s).

### P3
- R1-N2 the shared CO-OPS budget starves gauge wind windows (all-at-once take(1 + products)) behind tide windows (one
  token at a time): wind median 20.6 s / max 110.6 s under contention, tide max 7.5 s.
- R1-N5 R1's mutation pass: 54 mutants, 41 killed, 2 equivalent, 11 survivors (R25 a gauge without products takes no
  token; R57 the kept flag's state without the dot's identity; R71 `setSavedPoints` not redrawing; R44 a refresh on
  every tab show; R54 the hollow search's date-line wrap; R66 the ResizeObserver; R47 "Calm" in the short readout; R43
  the headline with an empty history; R27 `fetch_stuck`'s threshold; R39 `_fix_case`'s state rule; R04 `NDBC_KEEP_S`).
- R1-N6 A-7 residual: metar:KGVW + ndbc:KGVW (332 m) still both drawn; CXRB/CYRB (335 m) and CXAT/CYAB (358 m) airport
  pairs; the snapshot cannot be reproduced from caches (NDBC / METAR / "reporting now" are read live).
- R1-N7 = R2-5 Metric shows the international 9999 as "Visibility 9.7+ km" (scope: "10+ km").
- R1-N8 docs: README "/api/wind/<id>/history (5 min)" (60 s), "two request threads always free" (builds only); three
  names still "Mxak" (KOZA2, PAUA2, SGXA2).
- R2-6 the 8 / 12-px hollow distance is computed on unrounded projected pixels (up to ~1.4 px of slop at the boundary).
- Observations (not counted): an NWS history pair (016HE, 003HI) answered "busy" five times in ~25 s at 00:58-01:00 UTC
  and opened in 0.6-0.7 s 20 min later (a slow api.weather.gov build holding its key lock is the likely cause; the
  page's retries covered it); the maximised forecast window covers the station windows by design (section 26); a tide
  window dragged 60 px up can hide the live window (any free drag; production too); B-4 residual: right after a server
  start a buoy whose newest line is wave-only reads "no reading" until its next wind line (7 of 424 at 01:16 UTC).

## Fix round 7c scope
**Server**
1. R1-N4 / R1-N1: `SharedCap` keeps when each slot was taken; an `acquire` with a timeout answers at once when every slot
   has been held longer than that timeout (the points' `_POINT_BUILD_SINCE` rule moved into the cap, for all three
   services); the busy text no longer names forecast points only.
2. R1-N2: a gauge window takes one token for its wind rows; each weather product only if a token is free at once (a
   product without one is left out, as one past the deadline is).
3. R1-N6 / R1-N8 names: the snapshot tool drops an NDBC copy of a kept METAR id at any distance and treats two airports of
   one name within 500 m as one spot; "MXAK" kept in capitals; the rules applied to the committed snapshot by a
   deterministic pass (no live refetch).

**Page**
4. R2-1: `syncWindWidth` runs after every change of the boxes (at the end of `syncChips()` and `syncForecastChip()`;
   resize and the corner observer as now).
5. R2-2 (both variants): where the live and tide windows are raised by the same rule (`body.fw-chip` below 1180 px,
   `body.wind-chip`) the tide window's height cap is 32 px below the live window's, so the live window's top shows above
   it; the four-window sweep gains the parked states (forecast, wind, live, tide parked) and a real-box sweep on the test
   site.
6. R2-3: on touch a solid ring's disc never covers a visible dot (forecast dots and My points while their layer shows):
   per flag the disc ends before the nearest dot.
7. R1-N3 / R2-4: the flags are redrawn when "Forecast points" or "My points" is ticked or unticked.
8. R2-6: the hollow distance from rounded pixel positions (as drawn).
9. R1-N1 client: the forecast window (and the point tool's prefetch) retries a busy answer quietly a few times before
   showing the error with Retry.
10. R1-N7 / R2-5: in Metric an "or more" 9.7 km (9999) reads "10+ km".
11. Tests for every item + R1-N5's survivors; README (R1-N8, the cap rule); UI 1.22.0; golden in its own commit;
    mutation pass; then the test-site verification (MAX) incl. R2's parked paths and a phone tap sweep.

## Fix round 7c — the re-check's fixes (2026-10-12, Opus 5.5 HIGH) @ f86763b + golden 1bee857 (local)
Pins for the survivors @ 335ccc2. UI 1.22.0. Every item of the scope above; numbers by item.

**Server**
1. R1-N4 / R1-N1: `SharedCap` (buoy_sources.py) keeps each thread's acquire times; `stuck(wait_s)` is true when every slot
   has been held longer than `wait_s`, and then forecast points, tide windows and wind histories answer busy AT ONCE (no
   2-s wait in a request thread). The points' own `_POINT_BUILD_SINCE` bookkeeping is gone (one rule in the cap for all
   three services); the points' busy text is "The server is busy right now; try again in a moment". Tests: the cap's
   rule and marks; each of the three services answers busy at once while tide or wind builds hold both slots; the
   points' cap is back to 0 after every build.
2. R1-N2: a gauge window takes ONE CO-OPS token for its wind rows (none: busy, nothing sent); each weather product only
   if a token is free at once (`take(1)`); a product left out is logged and treated like one past the deadline (the
   window shows the rest). A gauge with no weather products still takes its one token (R25). Tests: one token for the
   wind rows, products optional, the emptied-bucket busy case.
3. R1-N6 / R1-N8: `one_per_spot` drops NDBC's copy of a kept METAR id at any distance (KGVW: 332 m) and treats two METAR
   sites within `AIRPORT_SPOT_M` 500 m as one airfield; "MXAK" is kept in capitals; `--respot` applies the rules to a
   built snapshot without asking any agency and keeps its capture time. `wind_stations.json` respotted 2,727 -> 2,724:
   `ndbc:KGVW`, `metar:CYAB` (kept CXAT) and `metar:CYRB` (kept CXRB) left out; KOZA2, PAUA2, SGXA2 renamed "... MXAK".

**Page / client**
4. R2-1: `syncWindWidth()` runs at the end of `syncChips()` and `syncForecastChip()`, i.e. after the chip classes and
   boxes change (resize and the corner observer as before).
5. R2-2, both variants: where one rule raises the live and tide windows (`body.fw-chip` on tablets, `body.wind-chip`)
   the tide window's height cap is 32 px below the live window's (tablet: `100vh - 40px - var(--tw-bottom) - ...`, was
   8 px; wind-chip: `100vh - 66px - ...`, was 34 px), so the live window's top always shows above the wider tide window.
   AUTHOR'S FINDING while writing the sweep: the RAISED forecast box (section 38's live-chip / tide-chip rules and
   section 39's wind-chip rule) reached up to the station windows' capped top on short screens, so with the forecast
   window in front an open live or tide window had no visible part (with the wind parked up to 781 px tall; with the
   live or tide window parked, production's own section-38 layout, on short desktops). Fixed: a raised forecast box
   stops 64 px below the corner-capped top (heights `100vh - 142px` (was 78) with one station chip, `- 150px` (was 86)
   with both, wind-chip `- 102px` (was 38)). `test_live_and_tide_raised_together_keep_every_window_clickable` = an
   exact largest-visible-square sweep (compressed coordinates) over state A (tablets, forecast parked: 9 widths x 12
   heights, with and without the wind window), B (wind parked: 9 x 14) and C / D (live or tide parked, with and without
   the wind window: 7 x 12): every open window keeps a 24-px square whichever is in front (K-9's forecast-over-wind below
   570 px tall stays the known exception: Escape).
6. R2-3: on touch screens each solid ring's disc is sized per flag (`--wind-disc`): it ends 1 px before the nearest
   visible forecast dot (radius 4) or My point (radius 7) within reach, 13 px when none; `windDotState(e)` (kind, id,
   distance, disc) is part of the drawn signature and the flag's state, so a change redraws.
7. R1-N3 / R2-4: ticking or unticking "Forecast points" or "My points" runs `rebuildWindMarkers(false)` (the overlay
   events on the map); `setSavedPoints` redraws too (R71).
8. R2-6: the hollow / disc distances are measured between ROUNDED pixel positions (as Leaflet draws them), with the
   world wrap; `windHollowSet` returns hollow entries `{f, kind, lng, r}` or disc entries `{..., disc}`.
9. R1-N1 client: the forecast window asks a busy answer again quietly `BUSY_TRIES` 5 times `BUSY_WAIT_MS` 4 s apart before
   it shows the error with Retry; the point tool's prefetch likewise; a newer request ends the waiting.
10. R1-N7 / R2-5: an "or more" distance in Metric is rounded to a whole number ("10+ km").
11. Tests (R1-N5's survivors): R25 (a gauge without products takes a token), R57 (the flag's state carries the dot's
   identity), R71 (`setSavedPoints` redraws), R44 (no refresh on every tab show), R54 (the hollow search's date-line wrap,
   pxPerDeg 256*512/360), R66 / R71 (page pins), R47 ("Calm" in the short readout), R43 (the headline with an empty
   history), R27 (`fetch_stuck` below its threshold + LIVE_FETCH_STUCK_SEC), R39 (`_fix_case`'s comma rule), R04
   (NDBC_KEEP_S). README: the points' busy text and quiet retries, the budget's token rule, the cap's stuck rule,
   history 60 s, the touch disc, the 1.22.0 heading.
- Suites: pytest 1,065 incl. the golden (exit 0, full run), Node 599.
- Found by the full run (not in the file alone): `test_provider_keep_last_list.py::test_golden_replay_still_identical`
  (older than section 39) called the memo module's golden test without that module's autouse isolation, so
  `run_sequence`'s direct assignments (`buoy_sources.time` frozen, `app.get_buoy_providers`, `app._buoy_tz_cached`)
  stayed for the rest of the session; the new cap pin then patched `monotonic` on the frozen clock and the services'
  stuck test saw an unpatched clock. Fixed @ 721565c (the replay records and restores the three; the pin patches
  `buoy_sources.time` itself); a full run with a watcher on the clock modules shows the real clock throughout.
- Mutation pass (scratch `g28/fix7c/mut7c.py`): 34 mutants of the 7c code (cap rule, the three services' stuck checks,
  tokens, busy text, spot rules, respot, legend toggle, both syncWindWidth calls, the four raised caps, the disc formula,
  dot radii, reach, rounding, the signature, the disc style, the CSS inset, the client's retries, the Metric rounding):
  31 killed at first; S02 (`all` -> `any`: one old slot and one just taken), C02 (the prefetch's retry) and C04 (a
  superseded wait asking again) survived and are pinned @ 335ccc2: 34 of 34 killed.
- Next: the test-site deploy and verification (MAX): R2's parked paths (live / forecast parked with tide in front, both
  raised variants), the four-window real-box sweep incl. the raised forecast box, a phone tap sweep 12-26 px from a
  flag, the legend toggles, the forecast busy retry; then the owner's call (another short check or production).

