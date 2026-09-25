# Overlays — G8 adversarial review (overlay state, wave-height contrast, contours) — 2026-09-25

Gate G8 of plan section 21 (Phase A), before the client production merge. Reviewed on `feat/overlays-restore` @ d438322
(four commits on production 78b54de; the test site runs the same code as asset 2.7.3):

| Commit | Asset | Change |
|---|---|---|
| fc00e99 | 2.7.0 | A1: the overlay's layer, valid time and play state survive a reload (another forecast point, Update); restored after load + idle, only while visible; time/play only from a save under 30 min old; no resume under reduced motion |
| 1623137 | 2.7.1 | Off stays Off across a reload (the unmount's pause had re-saved the layer as on; found on the test site) |
| 257db6f | 2.7.2 | A2: wave-height value knots (0–3 m ≈ 47 % of the legend), value-linear LUT, knotted legend and ticks, owner-picked palette A |
| d438322 | 2.7.3 | A3: optional contour lines (checkbox on the Opacity row; 2 ft / 0.5 m, 2 s; RGB only; apron-joined across tiles) |

Owner decisions (binding): restore field + valid time + play state; checkboxes off by default and remembered per tab;
contours every 2 ft / 0.5 m and 2 s; palette A.

Two fresh-context reviewers at MAX effort (Opus 5.5), no access to the author's reasoning:
- **A — client code** (Node harness, real frames): persistence paths and races, palette/legend consistency and contrast,
  contour geometry, apron continuity, readout invariant, performance, tests, hygiene.
- **B — owner requirements on the test site** (browser pane, desktop and phone): forecast-point switches, Off, reloads,
  contrast judged on real frames, contours at zooms 3–11, regressions (clip, readout, markers, sheet, teardown).

## Findings

Reports: scratch `g8a-report.md` (A: **0 P0, 0 P1, 3 P2, 8 P3**) and `g8b-report.md` (B: **0 P0, 0 P1, 4 P2, 6 P3**).
Both verified every owner decision for A1 on real flows, the A2 contrast (adjacent 0.5 m steps from 0 to 3 m went from
ΔE2000 4–7 to 11–22), readout = drawn pixel with contours on (0 mismatches in 294,912 samples per field and unit under
the real coast clip), untouched alpha, and continuous tile seams, world copies and the dateline.

| # | Sev | Finding | Outcome (asset 2.7.4) |
|---|---|---|---|
| A-P2-1 = B-P2-4 | P2 | A paused overlay loses its valid time on the next forecast point after 30 min on the page: the save's time stamp moved only on a frame landing, play or pause. | Fixed: hiding the page saves even when paused, and a `pagehide` listener saves too (removed on Off). The 30-min rule now counts from when the page was left. Test: 31 min paused, then another point, restores +48 h. |
| A-P2-2 | P2 | The peak-period "jump over 2 s per cell" suppression used the cell width only; a Mercator cell is sec(lat) taller, so north-south jumps passed (8–52 % of tp line pixels crossed such jumps at 45–90° N). | Fixed by design change: the jump is counted between model nodes (cells marked when an edge jumps more than 2 s), identical in both directions at every latitude. Real frames: tp line pixels inside a jump cell 0 at z3/z4/z6/z8 (2.7.3: 15–22 %). Test at 0° and 60° N, both axes, with a 1.5 s control. |
| A-P2-3 | P2 | Tests did not pin several A1–A3 behaviours (11 of 20 mutants survived); the maxJump assertion was vacuous; the 512-px seam and readout-with-contours tests were missing; the bootstrap had only string checks. | Fixed: Node tests 52 → 78, incl. a bootstrap suite that runs the template's own script against a fake DOM (pytest pins that script to what Flask renders) and real-frame tests; 33 of 33 mutants killed (Fix round below). |
| B-P2-1 | P2 | Contours traced the 8-bit terraces: grid-aligned staircase lines in flat seas (about half the line pixels in open-ocean Hawaii views). | Fixed: lines are traced on a smoothed copy of the frame (masked [1,2,1]² over the model nodes, computed lazily per block of nodes, once per frame, only while contours are on; ~5 MB at full resolution, dropped when unticked). Colours, alpha, the clip and the readout keep the raw values. Peak-period neighbours across a > 2 s jump are left out, so swell regimes are never blended. Before/after crops: scratch `g8fix/*.png`. |
| B-P2-2 | P2 | False line pixels at the foot or top of steep ramps (no crossing within 2 px): tp 9.1 % (z3), 14.1 % (z4), 6.7 % (z5); hs up to 4.4 % at z3/z4. | Fixed: a pixel's distance to the level is measured along the slope that leads to it (one-sided differences), so a flat side never counts. Real frames, line pixels with no raw crossing within about one model cell: tp z4 8.1 % → 0.4 %, tp z3 5.5 % → 0.3 %. Test: plateau next to a ramp, both orientations. |
| B-P2-3 | P2 | Frame step with contours at or over the 60 ms line on a 1080p desktop (59.1 ms average, 66.8 max) because the contour pass added ~80 % to a redraw. | Fixed: the pass samples the smoothed field every 2–4 px at world-aligned pixels and skips 16-px blocks that provably hold no line pixel (the skip test is exact: 0 bytes differ from drawing every block over 146 real tiles). Contour cost per tile (Node, this machine, first draw of a frame incl. smoothing): z8 +0.05–0.29 ms (2.7.3 +0.62–0.86), z6 +0.20–0.36 (+0.68–0.83), z3/z4 +0.89–1.16 (+0.66–0.91). Test-site numbers below. |
| A-P3-1 = B-P3-3 | P3 | Palette A: for deuteranopes and protanopes 2.5 m and 3 m nearly merge (ΔE2000 3.6–3.7); 0.5 m and 9 m look alike out of context. | Owner decision (palette A was the owner's pick); reported, unchanged. The legend ticks at 2/3/4 m still separate the bands. |
| A-P3-2 | P3 | Lines are ~2.25 px wide, not the documented 1.5 px; ink was chosen per pixel, so the 4 m line changed ink along its width. | Ink is now chosen once per level from the LUT colour of the level's own value. Width kept at ~2.2–2.3 px (what the owner approved on the A3 preview), isotropic over 24 angles (test); docs say ~2 px. |
| A-P3-3 | P3 | A non-object value in the session key disabled persistence for the tab (a number, a string or an array was written back unchanged). | Fixed in both the module and the page: anything but a plain object counts as empty; the field must be a string. Tests in both suites. |
| A-P3-4 | P3 | A back/forward-cache return did not re-sync the key with the page shown. | Fixed: `pageshow` (persisted) writes the shown page's field and state back. Test. |
| A-P3-5 = B-P3-6a | P3 | The saved time applied only to the first mount: a field switch before the first restored frame, or Retry after a failed restore, fell back to "now". | Fixed: the saved state is kept until the first frame lands or the layer goes Off, and used by any mount before that. Tests for both paths. |
| A-P3-6 | P3 | The checkbox's accessible name ("Contour lines every 2 ft") did not contain its visible label and ignored the doubling below zoom 4. | Fixed: "Contours, every 2 ft (4 ft below zoom 4)" (and the Metric / period equivalents). |
| A-P3-7 | P3 | The wave-height knots were hard-coded to a 12 m legend while the legend comes from the manifest. | Fixed: the knots stretch to whatever legend the manifest carries (today's 0–12 m is unchanged, byte for byte); the legend top is always the top colour. Tests for 0–10, 0–14 and 0–20 m. |
| A-P3-8 | P3 | The restore waited for the window `load` event with the select already showing the field and no feedback; a stalled subresource postponed it indefinitely. | Fixed: "Loading…" at once; the restore starts at the load event or after 5 s, whichever comes first. Tests. |
| B-P3-1 | P3 | Hairline seams between overlay canvas tiles under fractional zoom and zoom animation (pre-existing). | Deferred. Leaflet's `plus-lighter` blend on the canvases would also add old and new tiles together during zoom cross-fades and needs an isolated layer; a separate change with its own check. |
| B-P3-2 | P3 | The new palette makes the 8-bit terraces faintly visible in the fill of flat seas at zoom ≥ 7 (per-code ΔE2000 up to ~4). | Deferred, owner awareness (options: ordered dithering of the LUT index, or finer frames). The contour lines no longer follow the terraces. |
| B-P3-4 | P3 | Phones: the Contours control sits below the fold of the sheet, and is unreachable on short landscape maps (pre-existing layout, G5 C#5). | Deferred (pre-existing; the remembered setting still applies). |
| B-P3-5 | P3 | On a cold cache the restore's first frame can coincide with the deferred forecast table's injection. | Fixed: the restore waits for the deferred table (3 s at most). Test. |
| B-P3-6b | P3 | Off, On again, then another forecast point before the first frame landed: the new page restored the time saved before the Off. | Fixed: a layer picked from Off drops the saved time and play state. Test. |

## Fix round (asset 2.7.4)

`feat/overlays-restore` @ 3900027 (test @ a1db379; `overlay.js` served immutable with sha256 c60560a3…, equal to the
commit). Node tests 52 → 78, pytest 381; the Node client suites now also run in the `coast-build` CI job, incl. the new
bootstrap suite.

**Mutation check** (scratch `g8fix/mutate.py`): 33 mutants (22 on the module, 11 on the page bootstrap and variants),
each breaking one fixed behaviour; all 33 killed against a clean baseline (reviewer A's M10, the colour table cached by
field only, is an equivalent mutant since 2.7.4: the table no longer depends on the legend range). The first run had a
harness defect (the
frame fixtures were not copied, so two decode tests failed on every run and every mutant looked killed); the corrected
run found 3 survivors (the one-sided distance at a slope kink, block skipping near real extrema, the look-around for
cells narrower than the sampling), each now pinned by a test (the last two on the real frames in `tests/fixtures`).

**Real frames** (Node, run 2026092500 f000, the client code itself; scratch `g8fix/eval.js`):

| Check | 2.7.3 | 2.7.4 |
|---|---|---|
| Readout = drawn pixel with contours on, Oahu z8 under the real clip (hs, tp; US, Metric) | 0 mismatches | 0 mismatches in 294,912 samples each |
| Alpha bytes changed by the lines | 0 | 0 |
| Block skipping vs drawing every block (146 tiles here; 193 in the committed test; 2,970 in the re-review) | — | 0 bytes differ |
| Four tiles vs one 512-px computation (8 views incl. the dateline, z2 half, z11) | 0 | 0 bytes differ |
| World copies (438 tile pairs) | 0 | 0 bytes differ |
| tp line pixels inside a cell whose nodes jump > 2 s (z3, z4, z6, z8) | 22.2 / 15.4 / 18.0 / 15.1 % | 0 at every zoom (also z1–z2) |
| Line pixels with no raw crossing of their level within about one cell: tp z3 / z4 | 5.5 / 8.1 % | 0.3 / 0.4 % |
| Integrated line width over 24 angles × 5 offsets | 2.00–2.34 px | 2.20–2.32 px |

In flat seas the lines now leave the terrace edges by design (hs z8: 1.3 % of line pixels have no raw crossing within
a cell, against 0 before). (Superseded: near islands the unbounded smoothing also moved and removed real contours, see
the re-review below; 2.7.5 bounds it.) Before/after crops:
scratch `g8fix/g8_contours_before_after.png` (Kauai z7, open sea z8, W Pacific tp z4, NE Atlantic tp z6).

**Cost of the contour pass per tile** (Node on the dev machine, first draw of a frame, incl. smoothing):

| View | 2.7.3 | 2.7.4 |
|---|---|---|
| hs z8 Oahu (clip) / tp z8 Oahu / hs z8 open sea | +0.76 / +0.74 / +0.62 ms | +0.29 / +0.05 / +0.19 ms |
| hs z6 Hawaii / tp z6 NE Atlantic | +0.68 / +0.83 ms | +0.20 / +0.36 ms |
| hs z3 N Pacific (half) / tp z4 W Pacific | +0.66 / +0.91 ms | +0.89 / +1.16 ms |

Low zooms cost a little more. (Corrected by the re-review: the cost is the 2-px sampling, the per-block fills and the
line pass itself; the smoothing is under 1 % of it. A 1080p map at z3-z5 needs 18-27 tiles, not 12: frame steps with
contours 51-57 ms there, see R2 P3-1.)

**Test site** (asset 2.7.4, 2026-09-25):
- 1280×800, hs z8 open sea NW of Kauai, 18 tiles: 5,952 line pixels, 0 alpha changes, readout = drawn at 27,000 random
  samples (0 mismatches); the checkbox reads "Contours, every 2 ft (4 ft below zoom 4)", Metric "every 0.5 m (1 m below
  zoom 4)" with the 0.5 m interval.
- 1920×1080 (map 1879×458, 27 tiles), tp z8 Oahu with contours: full redraw 28.2 → 33.0 ms average (max 34.0); frame
  step 42.7 → 47.7 ms average (max 55.2) with the ring prefetched. Reviewer B on 2.7.3 (24 tiles): step 59.1 average,
  max 66.8.
- Paused at +48 h, the save aged to 31 min, then another forecast point: leaving the page refreshed the save (2 s old)
  and the next page restored peak period at +48 h, paused, contours on; "Loading…" showed at once.
- Off, then Wave height: the saved time was dropped and the layer landed on the usual first frame. Off, reload: Off,
  no overlay request, module not loaded. No console errors.

**Deferred** (owner awareness): B-P3-1 tile seams, B-P3-2 terraces in the fill, B-P3-4 phone controls below the fold;
A-P3-1 / B-P3-3 palette under colour-vision deficiency stays the owner's call.

## Re-review of 2.7.4 and fix round 2 (asset 2.7.5)

Two fresh-context reviewers at MAX effort (Opus 5.5) on `feat/overlays-restore` @ 3900027, started 2026-09-25, paused
overnight by the owner, resumed with their context: **R1 — code** (Node, real frames, own mutations; report scratch
`g8r1-report.md`): **0 P0, 0 P1, 1 P2, 5 P3**. **R2 — the test site** (browser pane with real clicks, offline renders by
the served code, byte-identical to the live canvases; report scratch `g8r2-report.md`): **0 P0, 0 P1, 1 P2, 5 P3**.
Both confirmed every first-round finding fixed (staircases 44.6-54.1 % → 9.8-19.8 % of line pixels on grid lines;
ramp-foot outlines tp z4 10.4 % → 0.2 %; 0 tp line pixels in a jump cell over ~1 M; readout = drawn; block skipping exact
over 2,970 real + 1,080 adversarial tiles; seams, world copies, memory release, the state flows with real clicks
including 31 min paused then another point, Retry, reduced motion, cold cache).

| # | Sev | Finding | Outcome (asset 2.7.5) |
|---|---|---|---|
| R1-P2-1 = R2-P2-1 | P2 | The unbounded [1,2,1]² smoothing moved real features, not just the 8-bit terraces: near the islands wave-height lines sat up to 1.6 intervals off what the colours and the readout show, closed contours of 1-3 cells vanished (the 1.5 m lee of Lanai), and at z10-z11 off Kaena no line was drawn at all (R2: 13.8-29.6 % of colour-level crossings at z8 had no line within a cell, 100 % at z10-z11). | Fixed: each smoothed node stays within half a code of its own value (its quantisation bin). R1 proposed one code; R2 measured that one code still loses 18.4 % (z8) and 62.7 % (z10) of the crossings on run 06 (a patch less than a code above its level loses its line). Half a code: 0 % at z8/z9/z10 on both runs, and the staircases stay away. Table below. |
| R1-P3-1 | P3 | Unpinned: the smoothed copy refreshed per frame, the ±1 look-around marking, unmount dropping the saved state, the visibility pause, the exact 2 s threshold; reviewer A's M10 dropped from the list. | Tests added (next section); M10 is an equivalent mutant since 2.7.4 (the colour table does not depend on the legend range any more). |
| R1-P3-2 = R2-P3-2 | P3 | Switching layers while the saved one still waits to be restored ("Loading…") dropped the saved time and play state; a moment later the same switch kept them. | Fixed in the page: a switch during the wait is a field switch (keeps time and play state); only a pick from Off starts fresh. Verified on the test site: the switch made with the module not yet loaded landed on +48 h. |
| R1-P3-3 | P3 | The coast-build CI job did not run on template-only changes, although the bootstrap suite tests the template. | Fixed: `templates/index.html` added to its paths. |
| R1-P3-4 = R2-P3-1 | P3 | Overview zooms did not get faster: a 1080p frame step with contours at z3-z5 is 51-57 ms (max 61; R2), contours add about 60 %. Playback cadence unaffected (4× median 129 ms either way). | Accepted for now (the §21 budget names z8: 38-40 ms). Options if wanted: 4-px sampling from ~4 px per cell, one fill per tile when most blocks draw. |
| R1-P3-5 | P3 | Record and README statements: 146 vs 193 tiles, the low-zoom cost explanation, "a few small closed contours", the README on smoothing. | Corrected above and in the README (lines "still sit where the colours and the hover readout put their level"). |
| R2-P3-3 | P3 | Peak period at z3-z5: short dashes remain along swell-regime edges where the node jump hovers around 2 s (172 fragments in a z4 view, 1,199 in 2.7.3). | Owner awareness; a hysteresis on the jump test is the option if wanted. |
| R2-P3-4 | P3 | The most common Hawaii levels (6 ft, 1.5-2 m, 14 s) get light ink at a line-to-fill contrast of only ~1.3:1. | Owner decision (e.g. stronger light ink); the look the owner approved is unchanged. |
| R2-P3-5 | P3 | Lines are polylines with a corner at every model-cell edge (bilinear), visible from z9 up. | Owner awareness; a C1 interpolant (Catmull-Rom) of the smoothed nodes would round them. |

**Choosing the bound** (R2's metrics with R2's scripts, the author's variants of the working tree; colour-level
crossings with no line of that level within one model cell, and line pixels on grid axes/diagonals; hs US unless noted):

| View | 2.7.3 | ±1 code | ±0.75 code | ±0.5 code |
|---|---|---|---|---|
| run 06 z10 Kaena: no line within a cell | 0 % | 62.7 % | 62.7 % | **0 %** |
| run 06 z9 Kaena | 0 % | 25.3 % | 25.3 % | **0 %** |
| run 06 z8 Oahu | 0 % | 18.4 % | 18.4 % | **0 %** |
| run 06 z7 Kauai | 0 % | 10.3 % | 10.3 % | 1.9 % |
| run 00 tp z10 Kaena | 0 % | 100 % | 100 % | **0 %** |
| staircase share, two open-sea views at z8, both runs | 43.7-56.4 % | 3.0-19.8 % | 3.0-19.8 % | 5.1-19.8 % |
| line core vs the drawn value, Hawaii z7-z11 (R1's script), max | 0.11 | 0.21 | 0.17 | 0.17 interval |

Views where every bound leaves the same residue (open sea NW of Kauai 7.9 %, 24.5 N 158.5 W 31.1 % on run 00) are flat
seas where the smoothing moves a terrace-edge crossing by more than a cell, which is the staircase fix itself.

**Fix round 2**: `feat/overlays-restore` @ 19bdc7e (test @ 7d30448; `overlay.js` served immutable, sha256 f78f8846…,
equal to the commit). Node tests 78 → 86: a one-code bump keeps its closed contour (the case the ±1 bound loses);
real-frame line cores within 0.3 interval of the drawn value around the islands (fixture frames: max 0.20 with the bound,
0.79 without); one layer through frame / resolution / field switches equals fresh layers; lazily smoothed blocks equal
the whole frame, including a single tile whose samples end at a node-block seam; the 2 s threshold through the layer
(17 codes smoothed, 18 a jump); restore pending → Off → pick lands on the usual first frame; a hidden tab pauses and
resumes. pytest 381. **Mutation: 43 of 43 killed** against a clean baseline (the 33 of the first round, R1's R1/R2/R3/
R4/R5/R7/R22, the missing bound and the one-code bound, and the page's switch rule).

Real frames (Node, run 2026092500) on 2.7.5: readout = drawn 0 mismatches in 294,912 samples per field and unit at
Oahu z8 under the real clip; 0 alpha changes; block skipping 0 bytes over 146 tiles; seams 0 bytes in 8 views; world
copies 0 bytes over 438 pairs; tp line pixels in a jump cell 0 at every zoom; line pixels with no raw crossing within
about a cell: hs z8 1.3 % → 0.1 %, tp z8 2.3 % → 0 %. Before/after sheet (2.7.3 vs 2.7.5, six views incl. Maui Nui
+48 h): scratch `g8fix/g8_contours_before_after_275.png`.

Test site on 2.7.5 (2026-09-25, run 2026092512): z10 off Kaena (drawn 5.58-7.94 ft) now draws its 6 ft line (1,141 px;
2.7.4 drew nothing there); z8 Oahu: every level the colours cross (4, 6, 8 ft) has its line, readout = drawn 0
mismatches in 27,000 samples; a switch to Peak period while the page still showed "Loading…" (module not loaded) kept
the saved +48 h; Off and reload clean; no console messages from the page.

**G8 closed** (0 P0/P1 in either round; every P2 fixed and re-measured with the reviewers' own scripts). Deferred for the
owner: B-P3-1 tile seams, B-P3-2 terraces in the fill, B-P3-4 phone controls below the fold, the palette under
colour-vision deficiency, and R2's P3-3/P3-4/P3-5 above. Next: the owner's go-ahead for the client production merge
(tag `prod-pre-restore` @ 78b54de first).

## Owner follow-up: thinner lines (asset 2.7.6)

2026-09-25, owner: "slightly thinner, at wider zooms in particular the thickness of the lines is somewhat
distracting". The line width now follows the tile zoom (full coverage up to `core` px from the level, fading to none at
`edge` px; width = core + edge): **1.05 px at zoom 3 and below, 1.25 px at 4-5, 1.5 px at 6-7, 1.8 px at 8 and closer**
(2.25 px at every zoom before). The block-skip bound and the early cut use the zoom's fading edge, so skipping stays
exact for any profile. `feat/overlays-restore` @ 8f21f8b, test @ 55a2ad8 (`overlay.js` served immutable, sha256
510b4c0e…, equal to the commit). Tests: the width of every profile over 24 angles × 5 offsets, each zoom drawing with
its own profile, exact coverage on the fading edge; Node 87, pytest 381. Mutants killed: the profile not passed, the
early cut at the core, one profile for every zoom, the old width; the bound computed from the core instead of the edge
is equivalent for these profiles (it could only differ if edge − core exceeded 1 px). Test site: zoom 4 and zoom 8 draw
with their profiles, readout = drawn pixel at 28,800 samples, no console errors. Preview (2.7.5 vs 2.7.6 at zooms 3-8):
scratch `g8fix/g8_contour_width_275_vs_276.png`.
