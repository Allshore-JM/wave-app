# G31: adversarial review of the sea surface temperature overlay client (plan section 44)

Date: 2026-10-16. Scope: `feat/sst-overlay` @ c3bec41 on top of production 9d601f9 (the SST tiles job, reviewed in G31a):
`git diff 9d601f9..c3bec41`, 17 files (+1,634 / -66): `static_overlay/overlay.js` (the SST store, the sampler, the layer's
pluggable source, value contours, the observation mode and its panel), the page's gated block (`var SST`, the option,
`FIELDS.sst`), `app.py` (`SST_OVERLAY`, overlay asset 2.18.2), the README section, tests. Test site wave-app-clean @ 22c92d8
(cherry-picks of the three client commits) with `SST_OVERLAY=1` on the test service. Production carries the job only.

Two fresh-context reviewers, effort MAX:
- **A** (Fable 5.1): code, Node and Python, no browser. An independent sampler (its own PNG decoder and the manifest's
  geometry), readout = drawn pixel, the contours (wave height / period byte-identical to production), the store as a state
  machine with held answers and a seeded fuzz, the observation mode with a page-level fuzz running the template's own script
  against the real module, 88 mutants.
- **B** (Opus 5.5): the test site in its own headless Edge (GPU for timing, trusted input). 54 in-situ stations read through
  the visible readout, 665 ERDDAP pixels near surf spots, 36 views from zoom 2 to 12 (harbours, fjords, lagoons, the date
  line, the poles, lakes), every switch path, failure paths by interception, slow links, performance and memory (after A's
  CPU-heavy runs), accessibility, a fresh-target console.

**Result: 0 P0, 0 P1, 3 P2, 16 P3.**

## What held up
- Values: A's independent sampler = the client's `sampleGrid` at 463 queries (full tiles at all three tiers, contour lattices,
  exact node and edge positions, world copies, the date line, 80 N / 85 S cells, holes, a cell beyond the fixed range): 0
  mismatches (worst 8e-13 code units). B: 664 of 665 ERDDAP pixels near surf spots within half a step (the one outlier a land
  node the job fills by design and GSHHG clips).
- Readout = the drawn pixel: A at every pixel of real and synthetic tiles at tiers 2 / 1 / 0 and their unwrapped copies, the
  land threshold, contour pixels, right after a landing; B through the visible readout text at 54 stations and 36 spots, and
  identical at lon x and x + 360.
- Wave height / period contours byte-identical to production after the `_contours` refactor (576 tile draws, 672,670 inked
  pixels). Seams at the 5- and 20-degree cell edges and the date line as smooth as inside a cell.
- The store: aborts at every stage, the 15-s watchdog, failure kinds, the in-flight cap, eviction never dropping a needed
  (own, pad or world-copy) cell, a newer day adopted while old cells are held (they never land); 750 + 600 fuzz seeds.
- The observation mode: no model frame ever painted over SST (B, a hooked `setFrame`, paused / playing at 4x / a pending far
  seek / loading / both error panels / ECMWF / wind / Off-On / reload / back / a hidden tab); the saved session field always
  equals the field shown and a reload restores it (A's page fuzz, 360 seeds, with the 2.18.2 fix).
- Panel at 7 viewports (320x568 to 1920x1080), the stale and coastline warnings, loading and error panels without the model
  dropdown; data: the first view 502 KB with the cache cleared, a warm reload 0 KB; a fresh-target console clean.
- Accuracy through the site's own readout (B, 2026-10-15 day vs in-situ daily means): offshore buoys median 0.29 C (79 %
  within 0.5); nearshore surf-zone buoys median 0.26; piers / harbours median 0.67 with the tiles 0.37 colder.

## Findings
| ID | Sev | Finding | Author's check | Outcome (fix round, step 7) |
|---|---|---|---|---|
| G31-1 (A-F1) | P2 | The date-line tiles (x = 0 and x = n-1) never request, watch or protect the cell across the date line that their contour pad samples: `cellsForTile` clamps the padded box at +-180 instead of wrapping. The isotherms break up to one lattice step (4 px) short of the 180 meridian while that cell is not in hand, and stay broken after it lands (not redrawn). | Confirmed in code (`j0` / `j1` clamped at +-180 / cell) and by A's repro on c3bec41 (column 0 isotherm pixels 0 without the cell, unchanged after it lands); B saw it live (52 N 180, z9). | Wrap the padded box across +-180 in `cellsForTile` (one place, three callers). |
| G31-2 (B-P2-1) | P2 | After any failed SST load the error panel's Retry, a re-pick of SST and Off / On do nothing for 60 s (`load()` answers null with no request during the store's cooldown). Wave height's Retry tries again at once. | Confirmed in code. The author's own step-5 Retry check had reset `failedAt` to get past the cooldown, which hid this. | A mount (Retry, a pick, On) bypasses the cooldown; only automatic re-loads keep it. |
| G31-3 (B-P2-2) | P2 | On a slow link (250 and 150 kbit/s) the first SST view fails where wave height still shows: ONE 15-s timer covers the pointer, the manifest and the 217-KB frame, while the 1.33-MB coastline download shares the link. | Confirmed in code; B's measurements (400 kbit/s ready 17.6 s; 250 error at 18.6 s; 150 error at 20.9 s; wave height ready on all three). | Per-request timers that measure a STALL (no bytes for 15 s), not the total time, as the frames do. |
| G31-4 (B-P3-2) | P3 | A stalled cell download has no watchdog: it holds one of the four download slots until the browser gives up, and the tiles needing it stay on the coarser tier (200 s observed). | Confirmed in code (`_pump`: no timer per cell). | A per-cell stall timer: abort, mark transient, cooldown and retry. |
| G31-5 (B-P3-3) | P3 | In Metric the 0 C isotherm is never drawn (`contourTile` skips level 0 for every field; right for wave height, where 0 means no waves). The 32 F line is drawn. | Confirmed in code. | Skip level 0 only for wave height and period. |
| G31-6 (A-F2) | P3 | Tiles entirely on land still request, download and decode their cells (`tierFor` runs before the all-land return in `_draw`). | Confirmed in code. | Land first: an all-land tile asks for and needs nothing. |
| G31-7 (A-F3) | P3 | After a reload while on SST, a switch to a model field restores the time but not the play state (the pause in `_mountObs` saved `playing: false`). In the same tab the switch back resumes playing. | Reproduced (A's obs_check O-i). | `_mountObs` saves the carried play state. |
| G31-8 (A-F4) | P3 | A newer model run announced by the Update banner is adopted silently on the way back from SST (in SST mode `frameIndex` is null, so the "after Off" adoption fires), against the standing rule: while a time is held, a newer run is only announced. | Reproduced (A's obs_check O-d). | The carried time pins the run: the banner and the run survive a visit to SST. |
| G31-9 (K-1) | P3 | Up to 24 MB of decoded SST cells stay in memory while a model field is shown (wave height + kept cells +50.9 MB vs +31.7 MB alone, author; 64.1 vs 46.3 MB, B). | Measured by the author and B; both reviewers recommend clearing. | Owner's call (below). |
| G31-10 (A-F5) | P3 | Cell eviction drains the 0.05-degree tier before touching any 1-km cell (least recently used within a tier only); the README says least recently used. | Confirmed in code. | One least-recently-used order across tiers. |
| G31-11 (B-P3-1) | P3 | The screen-reader status keeps "Loading sea surface temperature…" for the whole SST session (`_renderObs` never updates it). | Confirmed in code. | Clear it when the SST panel renders. |
| G31-12 (B-P3-6) | P3 | The field select and the phone sheet are still named "Model overlay" while they show an observation. | Confirmed. | "Map overlay". |
| G31-13 (B-P3-7) | P3 | The first legend tick is left-aligned at its value (meant for wave height's 0 at the bar's edge), so "40" sits half a label right of its mark, crowding "50" (4.4 px apart on a 320-px phone). | Confirmed in the CSS. | Left-align the first tick only when it sits at the bar's edge. |
| G31-14 (A-F8, B-P3-4) | P3 | The README's accuracy statement is one-sided (it says the tiles run colder in harbours; at open surf beaches MUR read 0.6-1.1 C warmer at all four Gold Coast / Tweed buoys) and nothing says the value is a night-time foundation temperature that the afternoon surf zone can exceed. | B's measurements; the README read. | README paragraph + the credit link's title. |
| G31-15 (B, K-6) | P3 | On a tab left open on SST the stale warning cannot appear (the 30-min check only notes a newer day and never re-renders). | Code reading. | Re-render the SST panel when its day crosses the stale line. |
| G31-16 (A-F7) | P3 | The model's 60-s run-line timer keeps ticking while SST is shown (a no-op). | Confirmed in code. | Cleared on entering SST. |
| G31-17 (A-F6) | P3 | 13 mutants survive the repo suites and are killed only by the reviewers' scripts; the most useful: a pad-only cell landing must redraw the tile (S15 / R03), `_draw` must trace the isotherms at the tile's tier (R04). | A's classification re-read. | Repo tests for S15 / R03 and R04 first, then S03, S07, S20, S24, S27, P04, P06, R02, C03, O30. |
| G31-18 (B-P3-5) | P3 | Light isotherms nearly vanish over the greens (17-21 C) and the orange (26-29 C): contrast about 1.6-1.8 to 1. | B's computation from the live LUT. | Owner's call (below). |
| G31-19 (K-2) | P3 | The isotherms look busy at zoom 4-6 (ragged small loops over flat water and the 8-bit terraces). | Seen by the author and B. | Owner's call (below). |

Not counted (by design or accepted): K-3 isotherms on the 8-bit terraces (they sit exactly where the drawn colour
changes); K-5 SST redraws about 1.5 x wave height's per tile (paid on view changes only; phone long tasks comparable); K-6
a newer day only noted until the next mount; K-7 some warm Arctic coastal water flagged as ice by MUR.

Observations, not SST-client defects: where GSHHG's coastline lies off the real one (Puerto Escondido) the colour stops 1-2
km offshore with the clip, as wave height's does; during a drag the newly exposed map stays blank until the drag ends
(`updateWhenIdle`, every field).

Not covered: real devices (iOS Safari incl. its canvas decode path, iPad, Android); a real screen reader; the live day change
(simulated by interception; the 2026-10-16 day is due about 11-13 UTC on 2026-10-17); in-situ water temperature in Europe and
South Africa (checked against ERDDAP only).

## Owner's calls
1. Readout precision (G31-14): the readout shows one decimal ("82.4 F", "28.0 C") for a value good to about 0.5 C offshore and
   1 C at the shore. Keep one decimal (the analysis value, as the buoy windows show theirs), or round.
2. Busy isotherms at zoom 4-6 (G31-19): keep, double the interval below zoom 7 (2 C / 4 F), or smooth the lattice (more work;
   the readout = drawn rule must hold).
3. Isotherm contrast (G31-18): keep the light ink, or choose the ink by contrast per level.
4. Kept cells (G31-9): clear the SST cells on leaving SST (the next visit re-decodes the view's cells from the browser cache,
   about 0.1-0.2 s), or keep them for an instant return.

## Owner's decisions (2026-10-16)
1. Readout precision: keep one decimal (G31-14 is fixed in the README and the credit's title only).
2. Busy isotherms: double the SST interval below tile zoom 7 (2 C / 4 F; 1 C / 2 F from zoom 7 in) (G31-19).
3. Isotherm contrast: keep the light lines (G31-18 accepted as designed).
4. Kept cells: clear the SST cells on leaving SST; the day's frame and manifest stay (G31-9).

## Fix round scope (step 7, overlay asset 2.18.3)
G31-1 wrap the padded box across +-180; G31-2 a mount bypasses the store's cooldown; G31-3 per-request stall timers (no
bytes for 15 s) for the pointer, the manifest and the frame; G31-4 a per-cell stall timer (transient: cooldown and retry);
G31-5 level 0 drawn for SST; G31-6 all-land tiles ask for and need no cells; G31-7 the carried play state saved by
`_mountObs`; G31-8 the carried time pins the model's run; G31-9 the cells cleared on leaving SST; G31-10 one
least-recently-used order across tiers; G31-11 the status cleared by the SST panel; G31-12 "Map overlay"; G31-13 the first
tick centred unless at the bar's edge; G31-14 README + the credit's title (foundation temperature, both-way coastal error);
G31-15 the SST panel re-rendered when its day crosses the stale line; G31-16 the run-line timer cleared on entering SST;
G31-17 repo tests for the reviewers' surviving mutants; G31-19 the SST interval doubled below tile zoom 7.

## Fix round (step 7, overlay asset 2.18.3)
Every scope item is built:
- G31-1: `cellsForTile` wraps a padded box across +-180 (pad 0, the coast rule, is unchanged); the date-line tiles now
  request, watch and keep the cell beyond 180.
- G31-2: `SstStore.load(force)`; `_mountObs` (a pick, Retry, On) loads with force, so only automatic re-loads keep the
  60-s cooldown.
- G31-3: the day's load re-arms its 15-s timer at each step (the pointer answers, the manifest answers, every chunk of the
  frame's bytes). `decodeFrame` takes an optional progress callback (`readBody`); the model frames pass none and are
  unchanged.
- G31-4: each cell download has its own controller and stall timer (`cellStallMs` 15 s). When the timer fires, the slot is
  freed at once and the cell gets a cooldown and a retry; the dropped download never lands.
- G31-5: `contourTile(..., zero)`; the SST contour settings carry `zero: true` (the 0 C line is drawn).
- G31-6: an all-land tile asks for no cells; `_redrawSrc` and `_srcNeeded` skip it.
- G31-7: `_mountObs` saves the carried play state with the field.
- G31-8: a carried time pins the model's run, in both the manifest loader and its wrapper.
- G31-9: `SstStore.release()` runs on leaving SST (the cells go; the day's frame and manifest stay); Off = release + the
  load's abort.
- G31-10: a shared use clock across both tiers; `_evict` drops the oldest unneeded cells first.
- G31-11: `_renderObs` clears the status.
- G31-12: "Map overlay" (the select and the phone sheet).
- G31-13: the first tick is left-aligned only below 3 % of the bar.
- G31-14: README accuracy paragraph and the credit's title.
- G31-15: the 30-min check re-renders the SST panel when its day crosses the stale line.
- G31-16: `_mountObs` clears the run-line timer.
- G31-17: new tests: a pad-only landing redraws its tile, and `_draw` traces the isotherms at its drawn tier.
- G31-19: `coarseBelow: 7` for SST.

Tests: `sst.test.js` +9 (25), `playback.test.js` +3 (101), `panel.test.js` +1 (60). Node overlay + UI 714 pass; pytest 1,178
pass. The one failure in the full run (`test_job.py::test_an_upload_failure_stops_the_build_before_any_manifest_or_pointer`,
a job test; the job is untouched) came while the mutation run loaded the machine, and it passes 3 of 3 alone.
Mutation: 19 mutants, each undoing one fix (`scratchpad/sst/client/mut7.py`, run on a scratch copy): 19 killed.
