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
