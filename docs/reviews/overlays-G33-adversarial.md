# G33: adversarial review of plan section 50's client and lake tables (Great Lakes waves under GFS, ECMWF's lake cells, lake forecast tables)

Date: 2026-10-22. Scope: `feat/great-lakes` @ 8d18c4b on top of production 34a0101. Production already carries the GLWU
frames job (reviewed in G33a), the bulletin-cycle hotfix and the calm-water fill fix. The diff is
`git diff 34a0101..8d18c4b`: 10 commits, 27 files (+3,992 / -108). It touches:
- `static_overlay/overlay.js`: the composite lake sampler, the lake store, two lake frame kinds in the scheduler, the view
  gate, the lake particles, the credit;
- `app.py`: the GLWU tables read from R2, `resolve_model`, the station plumbing, `GLWU_OVERLAY`;
- `glwu_stations.json` + `tools/build_glwu_stations.py`;
- `static_ui/forecast.js` ("GLWU · run");
- the gated page block, the README, tests and fixtures.

Reviewer A also covered the calm-fill job fix live on production since 17:55 UTC (`git diff 0bde4d5..34a0101`).

The test site was wave-app-clean @ ae8bee7: overlay 2.21.4, UI 1.24.0, `GLWU_OVERLAY=1`, both models, SST, `GEO_BASE`.

Two fresh-context reviewers ran at effort MAX. The brief is in the session's scratch (`g33/brief.md`: the owner's nine
decisions, the author's claims C1-C10, known items K-1..K-9).
- **A** (Fable 5.1): code and data, no browser, its own detached worktree.
  - A scheduler fuzz on the playback harness: 2,600 seeds over the field, direction, lake and lake-direction kinds plus the
    pressure frames. Decodes were held, failed and hung; the operations included model and field switches, Update, a newer
    lake run, view, zoom and resize moves, and Off.
  - Independent checks of the sampler, the coverage, the particles, the lake store, the view gate, the tables and the job
    fix; 87 mutants.
- **B** (Opus 5.5): the test site in headless Chrome with trusted input, at 1280x800, 1920x1080, a 1024x768 tablet, a
  375x812 phone and the phone sideways.
  - Drawing against an independent decode, playback at every speed, the view gate by drags, wheel, keys and rotation.
  - Failure paths by DevTools interception, the lake tables for all 72 stations, keyboard and touch.
  - Timed cells against production, after A finished; one K-1 attempt; the first fill-version-2 lake run (19Z).

**Result: 0 P0, 0 P1, 0 P2, 9 P3 findings + 4 notes** (A: 5 P3 incl. one nit, 2 notes, a list of test gaps; B: 4 P3,
2 notes).

## What held up
- **Drawing.**
  - `sampleLake` matched an independent bilinear at 4,718,592 synthetic samples (A). That covered absent nodes, the last
    column and row, world copies, the coverage gate, and the readout and contour-lattice calls.
  - On the site the drawn pixel matched the readout at every lake-tile pixel, shore included: 0 mismatches in 2.8 M
    pixels (B).
  - Against a PIL decode with bilinear sampling, 0 mismatches in 9,829 values and 2,622 no-value samples (B).
  - The one alpha rule `max(255 - land, lake)` and the all-land path held. Without a lake, `composeTile` is
    byte-identical to production (A).
  - Islands are blank: the client's coverage matched an independent even-odd point-in-polygon at 960 / 960 pixels around
    Madeline Island, Isle Royale, Manitoulin and the Erie islands (A).
  - "No data" appears before the lake run, past +149 h and with no run.
  - Under ECMWF the lakes show its own cells on 100 % of lake water, with no `/glwu/` request (B).
- **Particles.** `lakeVec` matched the spec at 19,374 nodes. `flowField` on ocean inputs equals production's at 210,232
  lattice points, compared with `Object.is` (A).
- **Scheduler.** Over the fuzz every brief invariant held except F1, F1b and F2 below:
  - in flight never above `_maxInflight()`;
  - the caches stayed within the ring;
  - no lake frame under wind, SST, ECMWF or out of reach;
  - the 1-s cap held, and the first picture never waits;
  - no outage from lake failures;
  - a clean Off.

  On the site (B), a paint recorder logged 0 wrong, stray or bare lake paints at snail, fish and shark. That covered
  the +149 h edge, an Update while playing and a newer lake run adopted while playing.
- **The view gate.** `lakeNearView` matched a brute force at 20,000 views (A).
  - On the site (B): 0 lake frames out of reach.
  - On arrival the shown step's lake appeared in 56-116 ms.
  - A step held for its lake frames landed 39-41 ms after the lakes left reach.
- **Failure paths (B).** The pointer returning 503 or hanging, the manifest 404, every lake frame 404 or hanging, and the
  coast set 404 each lose only the lakes, with no alarm and an outage counter of 0. Playback stays at 8 fps, except
  1 step/s when every lake frame hangs, which is the 1-s cap.
- **Tables.**
  - `_parse_bull_text` equals production's parser on 14 bulletins x 6 zones x 3 wind maps: 648 / 648 (A).
  - All 72 stations equal the raw 13Z bulletin plus winds.json row for row (B); 10 stations independently (A).
  - The checks also covered hostile pointers, manifests, winds and bulletins, `resolve_model`, the cache keys,
    `/stations.json` (806 entries), the month rollover, the picker, graphs, summary, Metric / UTC, `?model=SWAN`, and the
    star and shaka on a lake station.
- **Timed cells** (B, warm edge, PREGC, test against production back to back): every cell ran at 8 fps with every
  landing painted. Live memory against production: wave height +7.7 and +8.3 MB, peak period +9.2, phone +7.1,
  Hawaii -0.6. All are inside the owner's +10 MB line. Peaks were inside the line except one peak-period sample 2.6 MB
  over: GC timing, as in K-2.
- **The calm-fill fix** (production). The encode ordering is safe: every encode of a step completes before the next
  decode, and `allow` is never mutated (A). On the first fill-version-2 run, 2026102219:
  - A: interior calm cells carrying a period fell from 777 (fill version 1) to 43 at f000; these are K-4's
    both-fills shore cells.
  - B: 6 of 2,454 calm points coloured under peak period, against 62 of 2,446 on 13Z.
  - The author: the published steps 0 and 75 equal a local fixed-job build, with 0 codes differing.
- **Suites at 8d18c4b** (A): pytest 1,241 passed, Node 886 passed.

## Findings

| id | P | finding | reviewer |
|---|---|---|---|
| F1 | P3 | `_syncLakeGate` records a reach transition but returns before acting when `frameIndex === null` (an Update's pending first landing; the error panel after a failed Update): the target's lake fetches run 2 over the cap, the step waits up to the 1-s cap for frames it will not draw, their decodes are cached out of reach, the old picture's lake stays until the next landing | A |
| F1b | P3 | `zoomend` / resize run `_checkRes()` before `_syncLakeGate()`; a resolution-change `_goto` that adopts a pending lake run refreshes `_lakeNear`, so the gate misses the out-of-reach transition until that landing (the gate relies on nobody recomputing `_lakeNear` between the view change and its compare: mutant V7) | A |
| F2 | P3 | After a lake-run adoption, the new run's lake DIRECTION of the shown step passes `_ensureLakeDir`'s guard while the layer still draws the old run's lake frame: the particles mix two runs (same valid time) until the new run's field frame for that step lands | A |
| F3 | P3 | A mount more than 30 min after the lake run was read re-reads the pointer with `status = 'loading'` although a run is in hand: `_wantLake()` is false for the round trip (a Retry blanks the drawn lake until the pointer answers; a field switch starts its lake fetches one round trip late) | A |
| N1 | P3 (nit) | `LakeStore.load()` takes any run that differs from the one in hand, an older one included (`recheck()` requires a newer one) | A |
| B-1 | P3 | A lake pointer, manifest or lakes coast set that fails once at page load is never retried in the visit (only a field / model switch, Off-On or a reload); with the coast set failed the lake frames are still fetched and decoded for nothing (258 frames in B's run) | B |
| B-2 | P3 (owner's call) | Under peak period, calm water reads "no data", the same words as past the horizon or with no lake run (at z9 over the Thousand Islands 0 of 242,482 lake pixels coloured); suggested: "calm" | B |
| B-3 | P3 | The author's note "Chicago -> 45174" is wrong for the geo Worker's Chicago (41.850, -87.650): the page opens on WhihalaBch (Whiting, IN, 16.0 km); 45174 (Wilmette) is 31.1 km | B |
| B-4 | P3 (record) | Step 6 called the ~0.35 % of blank lake pixels "calm cells": they are channels inside the GSHHG lake polygons GLWU does not model (Thousand Islands, St Marys, Niagara, Keweenaw waterway, Bay of Quinte), blank with "no data" at every step | B |
| N2 | note | The one-screen margin counts the lakes as near from Hawaii at zoom <= 5 on desktops (<= 3 on a phone): one notch out from the default view fetches lake frames (owner decision 8) | A |
| N3 | note | `_glwu_latest` treats a newest manifest without a `bulletins` block as unreadable without trying the kept runs (only partial builds lack it, and the pointer never names one: theoretical) | A |
| B-5 | note | The " · Lakes: NOAA GLWU" credit shows while the lakes are within the gate's one-screen margin but off screen | B |
| B-6 | note | The first shark loop over the lakes after each new lake run runs at ~6.8 steps/s while the lake frames are cold at the edge (p50 324 ms), 8.0 once warm; a phone on emulated 4G 7.6 fps (production 8.0); never a wrong or bare lake | B |

Test gaps (A's mutation pass: 87 mutants, 68 killed, 19 survived; 10 equivalent; the 9 others are no defect at 8d18c4b,
but no repo test would notice a regression):
- G2: the lake wait is not counted per step across a resolution change (1.6 s instead of 1.0).
- G6: a lake landing resets the outage counter.
- G18: the lake caches are not re-limited to the ring (a fetch-evict-fetch cycle at 4x).
- G21 / P7: a second animator rebuild per step. The repo's "one rebuild" pin cannot see it: the harness animator has no
  canvas.
- V7: the gate's dependence on `_lakeNear` (= F1b).
- S4: a negative column position in `sampleLake`.
- L4: the store's per-step watchdog.
- Q6: a 3-long wind cell.

## The author's checks
- A's reproductions re-run on 8d18c4b in a detached worktree (`g33/a/fuzz/repro.js`, `repro_f3.js`): F1, F1's 404 variant,
  F1b, F2 and F3 behave exactly as reported.
- F1, F1b, F2, F3, N1 and B-1 confirmed in the code:
  - `_syncLakeGate`'s early return on `frameIndex === null`.
  - The `zoomend` handler order (`_checkRes(); _syncLakeGate();`).
  - `_ensureLakeDir` checks the store's run, not the drawn lake's.
  - `LakeStore.load` sets `status = 'loading'` with a run in hand, and accepts any other run.
  - `lake.load()` / `lakeCoast.load()` run only from `_loadLakes` (mount), and `recheck()` needs status 'ok'.
- B-3: the distances recomputed from `glwu_stations.json` (WhihalaBch 16.0 km, 45174 31.1 km). No lake station lies in
  Chicago proper; the next are IndDunesNP 43.9 km and 45170 57.2 km.
- B-4: accepted. The step-6 log entry's "0.35 % blank = calm cells" was wrong; corrected in the plan.
- The 19Z lake run: steps 0 and 75 published = a local build of the fixed job (0 codes differing); every model calm cell
  keeps its height with no period or direction.

## The owner's decisions (2026-10-22)
- B-2: under peak period, calm lake water reads **"calm"** on hover. "No data" stays for past +149 h, no lake run, and
  the channels GLWU does not model.
- B-3: **keep the nearest station** (Chicago opens on Whihala Beach): no special case. The author's note is corrected.
- B-5: the lakes credit shows **only while lake water with GLWU data is on screen**. The frames are still fetched one
  screen ahead.
- **Fix every finding and add the missing tests**, then a short test-site check at MAX, then the production merge.
- Unchanged: N2 (owner decision 8, the one-screen margin) and B-6 (the first loop over a cold edge).

## Fix round scope (overlay asset 2.21.5)
1. F1: `_syncLakeGate` acts on a transition whatever `frameIndex` is. Out of reach it aborts the lake kinds in flight
   (this settles `_lakeWait`), empties `lcache` / `ldcache`, clears the layer's lake and the animator's lake data.
   In reach it starts the shown or pending step's lake frames through the plan.
2. F1b: the gate compares against the state it last ACTED on (`_lakeGateNear`, set only by the gate). `_adoptLake` /
   `_loadLakes` reset only the geometry cache.
3. F2: the lake data carries its run. The animator takes a lake direction only with the lake height frame of the SAME
   run and step. After an adoption, the new run's direction waits for the new run's height frame.
4. F3: a store re-reading its pointer with a run in hand stays usable: `_wantLake` holds and the drawn lake stays.
5. N1: `LakeStore.load` takes the pointer's run only when it is newer than the one in hand.
6. B-1:
   - A failed lake store or lakes coast set is tried again at the next landing once its cooldown has passed.
   - `_wantLake` requires the lakes' coverage, so no lake frame is fetched while it cannot be drawn.
   - When the coverage arrives, the shown step's lake frames start.
7. B-2: under peak period a hover on lake water without a period reads "calm" when the shown step's lake height is under
   0.1 m. The height frame of the shown step is fetched on demand, one per hovered step, never drawn. "No data"
   otherwise.
8. B-5: the credit follows the lake water with data on screen (margin 0), re-judged on every move.
9. N3 (`app.py`): a newest manifest without a `bulletins` block counts as listing no station, and the kept runs are
   tried.
10. Tests for the gaps (G2, G6, G18, G21 / P7 with a canvas on the harness animator, V7, S4, L4, Q6) and for every fix
    above; mutants on the fixes; README; the asset pin.
11. Records: B-3's corrected note and B-4's corrected step-6 entry (the blank lake pixels are unmodelled channels) in the
    plan and memory.

## Fix round (step 8, overlay asset 2.21.5)
Every scope item was built. The changes, by item:
- **F1, F1b.** `_syncLakeGate` compares the reach with `_lakeGateNear`, the reach it last acted on. That state is seeded
  by the first `_lakeNearNow` and re-seeded when a lake run loads (its grid is judged afresh). The gate acts whatever
  `frameIndex` is. Out of reach, `_dropLakes` aborts the lake fetches (a waiting step lands at once), empties both
  caches, and drops the drawn lake, the animator's lake data and the hover's height frame.
- **F2.** The lake data carries `run`. `_flowLakeData` gives the animator a direction only beside the drawn height frame of
  the store's current run.
- **F3, N1.** `LakeStore.load` keeps status 'ok' while it re-reads with a run in hand. `abortAll` always clears the load's
  promise. A same or older pointer keeps the run in hand.
  - FOUND BY THE FUZZ: a re-read that found a newer run swapped it in itself, outside the scheduler's adoption. The newer
    run now waits in `next`, and `_loadLakes` adopts it through `_adoptLake`.
- **B-1.**
  - `_wantLake` is false while the lakes coast set has failed.
  - `_retryLakes` asks a failed lake run or coast set again at a landing once its cooldown has passed.
  - The coast set's arrival starts the shown step's lake frames.
  - A failed coast set drops the lakes. FOUND BY THE FUZZ: the downloads must then be re-trimmed to the smaller cap
    (`_prefetch`).
- **B-2.** `_lakeWord`: under peak period, lake water without a value reads "calm" where the shown step's lake height is
  present and under `LAKE_CALM_M` = 0.2 m. The height frame is fetched on demand, one per step, and never drawn or
  cached as a lake frame; until it lands the hover reads "…". `layer.lakeCodeAt` samples it exactly as the tile samples
  its own.
- **B-5.** The credit needs `_lakeOnScreen` (the drawn lake grid meets the view itself). The gate re-credits on every move.
- **N3** (`app.py`). A newest manifest without a `bulletins` block lists no station, and the kept runs are tried.
- **Tests.**
  - `tests/overlay/lake.test.js` +4: store F3 / N1 / L4 and a re-read's newer run in `next`; S4; the word; `lakeCodeAt`.
  - `tests/overlay/playback.test.js` +14: F1 (pending and on the error panel), F1b, F2, F3 with the re-read's adoption,
    B-1 (lake run, coast set, the cap), B-2, B-5, G2, G6, G18, G21 / P7. One existing test now goes through
    `_loadLakes`.
  - `tests/test_parse_glwu.py` +2: N3, Q6.
  - Pytest 1,243 passed, Node 904 passed.
- **Reviewer A's scripts on the fixed code.**
  - `repro.js` / `repro_f3.js`: F1, F1b, F2 and F3 no longer reproduce.
  - `extra.js`: 8 of 8 pass.
  - The fuzz: 1,300 seeds (800 lake-only, 500 all-kind), 0 failures after the two fuzz-found fixes. The seeds that had
    failed on the first patch all pass, and seed 100357 passes at 8d18c4b as well.
- **Mutation.** 31 mutants on scratch copies (`g33/fix/mut8.py`): 29 killed, 2 equivalent.
  - The coast set's arrival start is redundant: the lake store's own callback starts the same frames while the set loads.
  - `lakeCodeAt`'s coverage test is redundant: `sampleLake` writes only under coverage.
  - The reviewer's eight test-gap mutants (S4, L4, G2, G6, G18, G21, P7, Q6) are all killed, as is N3.
- **Records.** B-3's corrected note and B-4's corrected step-6 entry are in the plan and memory.

## Test-site verification of the fix round (2026-10-22, Opus 5.5 MAX)
The site: `test` @ 6308ef3 (the cherry-pick of 1d844de; README kept as test's, no record), live from 21:20 UTC. The served
`overlay.js` / `overlay.css` 2.21.5 = the repo; the 2.21.4 URL answers 404. The overlay Node suites on the test branch:
465 passed.

The method: headless Chrome with the GPU and its own profile (the sign-in check found no account), on the live runs (GFS
2026102212, lake 2026102219). Each scenario was staged with DevTools Fetch interception on the frames host (scratch
`glwu/v6/s/v16_g33.js`, outputs `glwu/v6/out/v16_g33_*.txt`). Results:
- **F1, pending.** `update()` with its frames held: frameIndex null, the target's lake fetches in flight. The view then
  moved to Hawaii. The lake fetches went at once, the drawn lake and the lake particles were dropped, and in flight 2 = the
  new cap 2. Released: the new run's step landed, both lake caches 0, 0 lake requests after the move.
- **F1, error panel.** The Update's first frame answered 404. Leaving the lakes left 0 lake fetches and no running wait
  timer.
- **F1b.** At zoom 3 over the Pacific (-40, 150) the older lake run 2026102201 was drawn and the newer one noted
  (`next`). A setView to zoom 6 (half to full resolution) adopted 2026102219 and dropped the drawn lake, the particles' lake
  data and every lake fetch in the same event.
- **F2.** The older run drawn with its particles, the newer one noted, 9 s of 1x playback through the adoption: 38
  animator lake updates, 0 pairing a direction with another run's heights. At the end the layer and the animator both
  hold 2026102219.
- **F3, N1.** A remount 30 min on with the lake pointer held: status ok, the lake kept on the landing. The pointer then
  answered an OLDER run (01Z): the run in hand stayed (2026102219), `next` null. Answered with the same run: unchanged.
- **B-1, lake pointer.** The pointer answered 503 at the page load: no lakes, 0 lake frame requests over 3 steps, a
  landing inside the cooldown asked nothing. After 61 s a landing asked again (1 request) and the lakes joined.
- **B-1, coast set.** The lakes coast set's index answered 404: the same, with the coast set retried after 61 s.
- **B-2, the hover.**
  - On a calm interior node off Ontario's north shore (43.80, -78.52; 33,634 calm interior nodes at the shown step), a
    trusted hover under peak period read "calm"; the same point under wave height read "0.0 ft".
  - At the Thousand Islands (z10) the probe found, among lake-coverage pixels with no period, 463 without a lake height
    (channels) and 282 with one. A hover read "no data" on a channel pixel and "calm" on a calm pixel.
  - (A first probe at 44.33, -75.93 had read stale text from a hidden readout: the point is off the lake coverage.)
- **B-5, the credit.** On the lakes: "Lakes: NOAA GLWU". With the view at (44.8, -54.2), the lakes off screen but within
  one screen (lake frames still wanted): no lake credit. Back on the lakes: credited.
- **Regression.** Lakes drawn 83,020 of 83,298 px, no stale tile, readout = drawn at 350 / 350 samples. 30,691 drawn
  pixels outside the lake coverage, as at step 6: the Atlantic in the north-east tile. Shark for 8 s over the lakes: 65
  landings, 0 with another step's lake. ECMWF: 0 lake frame requests, 83,298 of 83,298 lake pixels from its own cells.
- **Console.** Clean in every part. Storage of both origins emptied; the browser stopped and its profile removed.

Timed cells (PREGC, frames warm at the edge for both Origins, 45-s idles; test 2.21.5 vs production 2.20.3):

| cell | test fps (p50/p90 ms) | unpainted | live MB test / prod | peak MB test / prod | lake steps own / bare / wrong |
|---|---|---|---|---|---|
| lakes shark 1080p (1) | 8 (124/145) | 0/226 | +35.1 / +27.0 | +63.5 / +60.0 | 154 / 0 / 0 |
| lakes shark 1080p (2) | 8 (125/136) | 0/224 | +35.2 / +29.2 | +49.3 / +40.0 | 152 / 0 / 0 |
| Hawaii shark 1080p | 8 (126/135) | 0/224 | +31.9 / +32.4 | +45.7 / +48.7 | (lakes out of reach: none fetched) |
| phone lakes shark | 8 (126/134) | 0/144 | +20.2 / +13.8 | +29.0 / +22.0 | 126 / 0 / 0 |

- Every live set is within production + 10 MB (+8.1, +6.0, -0.5, +6.4). Every peak is within production's range + 10.
- The first lakes pair ran in a slower machine state on both sites: renders of 100-140 ms, long tasks 48 % / 29 %.
- No GPU stall (K-1) in the phone cell this time.

## Re-check of the fix round (2026-10-22, Opus 5.5 MAX)
One fresh reviewer, on the code (`8d18c4b..1d844de`) and the test site (test @ 6308ef3, overlay 2.21.5) with trusted input.
Report: scratch `g33/recheck/recheck-report.md`. Nothing was changed on any branch, the test site, production or the bucket.

**0 P0, 0 P1, 0 P2, 7 P3, plus 3 notes.**

### Findings
- **P3-1 (a regression from F3): a lake run already in hand is adopted a second time.**
  - `LakeStore.load` captures `was` (the run in hand) when the re-read starts. With `next` already set by the run check,
    a mount re-reads, and the mount's own `_goto` adopts `next` before the pointer answers. The answer is newer than
    `was` but not newer than the run now in hand, so it goes back into `next`, and `_loadLakes` adopts it again.
  - The second adoption aborts the lake fetches, empties both lake caches and drops the drawn lake.
  - Site (`s/rc.js double`): the older run served at load, the newer one noted, then a trusted field switch: two
    adoptions 323 ms apart, the drawn lake blanked once, 20 lake frame requests instead of about 10. At 8d18c4b: one.
  - Trigger: a page left 30 minutes after a new lake run published, then a field switch, Retry or Off/On.
- **P3-2 (a new path from F1b): the lake callback leaves the gate's state null.**
  - `_loadLakes`'s callback sets `_lakeGateNear = null` before its early returns (an Update pending, another manifest).
    The next move out of reach then finds `was === null` and only records it.
  - Site (`updatepan`): a re-read answering during a pending Update, then a move to Hawaii: 2 lake fetches still in
    flight (4 in flight against a cap of 2), the old picture's lake still drawn, 1 + 1 lake frames cached out of reach.
- **P3-3 (a gap F1 did not close; also at 8d18c4b): under wind and under SST, leaving the lakes keeps their frames.**
  - Under wind nothing seeds the gate (wind wants no lake frames); under SST the gate records the reach but returns
    before acting (`mode !== 'model'`). A field switch does not empty the lake caches.
  - Node (`mine.js windPanOut`, `sstPanOut`): wave height on the lakes, then wind or SST, a move to Hawaii, back to wave
    height: 5 + 5 lake frames (about 3.2 MB; up to 9 + 9 at shark speed) still cached out of reach.
  - The reviewer's stricter fuzz (`fuzz2.js`: "no lake frame cached out of reach" as a hard check, SST added) fails 9 of
    1,300 seeds, all on this.
- **P3-4 (new with B-2): the readout's re-show starts the next step's height frame during playback.**
  - When the hover's height frame lands, `onReady` re-runs the readout, which asks for the CURRENT step's frame. If the
    step has moved on, a new fetch starts, and so on.
  - Site (`hoverslow`, height frames delayed 300 ms): one trusted move 1 s into shark playback, then the cursor at rest:
    23 height frames in 8 s and "…" in all 81 samples. With a warm edge (about 60 ms) it does not start. 8 fps held.
- **P3-5: the lakes credit follows the lake grid's box, not lake water.**
  - `_lakeOnScreen` tests the grid's node box (41.2-49.2 N, 92.4-75.8 W): most of Michigan's Lower Peninsula, eastern
    Wisconsin and southern Ontario.
  - Site (`credit`): Lansing MI and Madison WI at zoom 10 are credited with no lake pixel on screen.
- **P3-6 (new about B-1): a lakes coast set retry fetches lake frames while it loads.**
  - `_wantLake` blocks lake frames only while the coast set's status is `failed`; a retry sets `loading`.
  - Site (`coastretry`): the coast index 404 at load, a retry 62 s later that hangs: 18 lake frame requests over 5
    steps, all dropped when the retry fails again.
- **P3-7: test strength.** 34 new mutants of the fix round on scratch copies: 18 killed, 16 survived. 9 survivors are
  behaviour changes no test notices:
  - R03: the gate's `_prefetch` after a drop (reviewer A's fuzz on that mutant: 9 of 200 seeds over the cap).
  - R13: the lake callback's `_lakeNear = null` (a move during the first lake load: the lakes never come).
  - R27 / R28: the readout's re-show through `_bindReadout` (the B-2 tests call `_lakeWord` with their own callback).
  - R06 / R08: `_dropLakes` / `_dropLakeHs` keep or never abort the hover's height frame.
  - R12: a failed coast set keeps the caches and the drawn lake (only the cap re-trim is tested).
  - R14: a failed re-read is asked again at every mount.
  - R22: the height frame's grid is not validated.
  - Equivalent or harmless: R04 (anchor not found), R07, R09, R23, R26, R30, R33.

### Notes
- N-1: the author's site check of B-2 moved the mouse a second time before reading "calm", so it never tested the
  re-show. The reviewer did: with a 1-s height frame, "…" at 500 ms, then "calm" at 2.5 s with no further move.
- N-2: `g33/fix/mut8_out.txt` lists 3 survivors; it predates the last B-1 test. At 1d844de that mutant is killed (the
  reviewer's R29), so the fix-round record's 29 killed / 2 equivalent stands.
- N-3: the hover's height fetch has no stall watchdog: a hung frame reads "…" until the step changes.

### What held
- Suites at 1d844de: Node 904, pytest 1,243 (the fix round's counts).
- Reviewer A's reproductions of F1, F1's 404 variant, F1b, F2 and F3 no longer reproduce; A's `extra.js` 8 / 8.
- A's fuzz over 2,700 seeds (1,400 new): 0 failures. The stricter fuzz with "F1 during a pending Update" as a hard
  check: 0 such failures.
- Site: a newer lake run adopted during 1x playback with 24 trusted drags on and off the lakes: 85 landings, none with
  another time's lake, none bare, no two-run particle pairing, nothing held at the out-of-reach checkpoints.
- "calm" by mouse hover (1280x800), by long-press on a 375x812 phone, and at world copies +-360.
- The real run 2026102219: every water node without a period has a height of 0.09 m or less. Of 3.1 M sub-node samples
  with no period, 43 read "no data" at 0.20-0.34 m (boundary pixels): within K-4.
- The credit through peak period, wind, SST, wave height, Off, ECMWF and GFS: right in 9 of 9.
- N3 and Q6 (`app.py`) hold. Console clean everywhere; the harness browser had no sign-in before or after.
- Not covered: a timed pair against production and memory; a real newer lake run (staged with the 01Z pointer); iPad,
  WebKit and real touch hardware; K-1; a real slow network for P3-4; a real partial GLWU build.

### The author's checks
- The reviewer's Node reproductions (`g33/recheck/fuzz/mine.js`) re-run on 1d844de's own `overlay.js` (from git) with the
  repository's fixtures: P3-1 (both the quick and the slow-pointer case: the drawn lake dropped and 10 lake frames
  fetched again), P3-2, P3-3 (wind and SST), P3-4 and P3-6 reproduce exactly. The R13 case holds on the real code.
- P3-5 in the code: `_lakeOnScreen` = `lakeNearView(view, ll.grid, 0)`, the grid's box.
- P3-6 in the code: `CoastStore.load` sets `loading` on a retry and keeps `failedAt`; `_wantLake` tests `failed` only.

## Fix round 2 scope (overlay asset 2.21.6)
1. P3-1: the re-read judges its answer against the run in hand when it answers (at the pointer and at the result).
   `adoptNewer` refuses a run that is not newer than the one in hand. `_adoptLake` aborts and empties only when an
   adoption happened.
2. P3-2 and P3-3: leaving the lakes drops everything of them whatever the mode or the gate's history: whenever the gate
   finds the lakes out of reach and anything is held (a lake fetch, either lake cache, the drawn lake, the animator's
   lake data, the hover's height frame), `_dropLakes` runs, and `_prefetch` re-trims in model mode. The lake callback
   re-judges the reach at once instead of leaving it null. The in-reach start keeps its transition rule.
3. P3-4: the hover's height frame re-shows the readout only while it is still the shown step's. N-3: a stall watchdog
   (15 s) turns a hung height frame into "no data".
4. P3-5: the credit follows lake water on screen: a tile in view whose lake coverage reaches into the visible part, with
   a lake frame drawn.
5. P3-6: no lake frames while the lakes coast set loads again after a failure. The first load keeps its no-wait start;
   the coast set's arrival starts the shown step's frames (now load-bearing, so pinned).
6. Tests: one per finding, and the reviewer's gaps R03, R13, R27 / R28, R06 / R08, R12, R14 and R22. The reviewer's
   stricter fuzz run on the fixed code (0 failures required); mutants on the fixes; the asset pin.
7. Then a short test-site check at MAX: the reviewer's site scenarios (`s/rc.js double`, `updatepan`, `hoverslow`,
   `credit`, `coastretry`) and the regression set, and a timed lakes pair against production.
