# G63a: adversarial review of the bathymetry lidar builder (plan section 96, stage 1b)

Scope: `432e44f..268c7b7` on `feat/lidar`: `tools/bathy/bathy_sources.py` (the four lidar regions, `LIDAR`, `EGM2008`),
`tools/bathy/build_bathy.py` (fill sources, the ring, per-region `zmin` and `margin`, the 24-px blend, projected VRT
footprints, the geoid per block), the notice, README and workflow region list. One fresh reviewer (session model, HIGH),
no browser, its own detached worktree; real data read from NOAA's open bucket and PROJ's CDN.

Result: **0 P0, 0 P1, 2 P2, 6 P3**, plus 4 information items. The data held up everywhere the reviewer read it back.

## What held up

- C1 fill sources never drive the blocks and never earn zoom 16 or 17; the ring is clipped to the box. Real builds: every
  tile holds lidar water, no fill-only tile, check 0 problems.
- C2 `zmin` 16 for Hawaii / Guam / Puerto Rico (Niihau: only z16 and z17 written), 8 for the NW Islands (z8-z17 written).
- C3 merge order: the newer survey wins (Niihau north-east: 1,905,737 overlapping pixels, every one NGS 2021's id 1); geoid,
  then the offset; one geoid warp per block.
- C4 the blend: monotone, the lidar id kept with 0x40, never at a seam between two lidar surveys, never where the fill has
  no value; margin 32 = the 24-px band + the coarsest apron's 8. (Superseded twice on 2026-11-14: the margin is 72 since
  F-3's rule, and the seam between two lidar surveys now fades too, by the owner's decision after step 5: see "The
  hand-over between surveys" below.)
- C5 projected footprints: the reviewer's own pyproj edge-sampled rectangles agree; sources a block does not touch are
  never opened.
- C6 `region_config` refusals (mutants M02-M04 killed).
- C7 the reviewer's own boxes (Niihau north-east, Nihoa) read back with its own EGM2008 conversion: median 0.000 m;
  land and water agree.
- C8 the stage-1a regions byte-identical under the new code; the live `hawaii` v2 tiles pixel-identical.
- C9 notice, README and workflow: credits, survey years, licences, the ellipsoid wording, the blend sentence; the
  workflow's region list = `S.REGIONS`.

## Findings and outcomes

| # | Rank | Finding | Outcome |
|---|---|---|---|
| F-1 | P2 risk | `hawaii-lidar` might not fit the runner's 300-min build or its disk (~2,409 blocks with the ring; ~50 s a block here) | **Measured, no change.** The runner dry run (run 43563738358, 4 vCPU, 84 GB free): hawaii-lidar 36:44, 32,762 tiles, 1.25 GB, max RSS 863 MB; guam 4:00 / 2,749; pr 26:24 / 37,350; nwhi 17:31 / 49,216; every check 0 problems. No split by island needed. |
| F-2 | P2 test gap | The margin-32 path that places every lidar tile was unpinned (mutants M22, M23, M25 passed): the synthetic build had constant depths and an all-deep fill | **Fixed (tests).** `test_the_blend_sees_a_lidar_edge_in_the_neighbouring_block` (a lidar ending 10 px past a block edge: the block west of it blends its last pixels); `test_a_build_keeps_a_sloping_survey_in_place_and_a_lagoon_joined_across_a_block_edge` (a survey deepening 1 m per 0.001 deg read back at both blocks within 3 cm; a walled lagoon joined only by a channel across the block edge; a walled pond 3-11 px west of the edge stays land). |
| F-3 | P3 effect | The 24-px blend rings every interior lidar hole, not only the outer edge (Kaneohe: 2.8-40.6 % of a tile's lidar pixels blended) | **Fixed (owner's call, 2026-11-14: option C).** The fade's width follows the gap's own size (2-24 px); see "F-3: the owner's choice" below. |
| F-4 | P3 | `pr-lidar` had no fill east of 65.25 W (Culebra's east end, Culebrita): no blend there, and the water rule had only the lidar | **Fixed.** `cudem-usvi` (11) and `cudem3-usvi` (21) added to pr-lidar's fill. Pinned. |
| F-5 | P3 hardening | The geoid grid was warped without a nodata value and the junk filter ran before the subtraction | **Fixed.** The geoid is subtracted first, then the 12,000-m filter; a geoid value beyond `GEOID_SANE_M` (200 m) counts as missing; a block whose raw values are all junk never reads the geoid. Pinned (a geoid grid with -32767 north of 21.23 N). |
| F-6 | P3 record | `to_lonlat` is not antimeridian-safe | **Recorded.** No region is near 180. |
| F-7 | P3 test gaps | M27 (check accepting `zmin - 1`), M20, M11/M12 (`_hits`), M01 (fill-id guard), M36 (lidar `mhhw`), M38 (a box holding its own footprints) | **Fixed (tests).** The check refuses a tile one zoom below `zmin`; `_hits` needs a real overlap (touching edges never read); a fill id must be above every own id; each lidar region's `mhhw` = the regional one beneath it; the outside-box guard (below). |
| F-8 | P3 | NGS Guam 2020 tiles west of 144.5 E (offshore banks) fell outside `guam-lidar`'s box and were dropped without a word | **Fixed.** The box starts at 144.4 E; and a build now stops when an own source's footprint reaches outside the region's configured box (`outside_box`, tolerance = the footprint pad), whatever `--box` narrowed. |

Information (no action asked): I-1 NGS 2021 Niihau is +0.21 m above USACE 2013 after the 0.3-m offset (inside the
notice's "about 0.3 m either way"); I-2 French Frigate Shoals was not built locally (the dry run built all of nwhi-lidar,
check 0 problems); I-3 the absence pin named only `bathy-dryrun.yml` (now also `lidar-dryrun.yml`, removed); I-4 the
NGS / USACE seam at Niihau is a hard ~0.2-m step by decision (two lidar surveys are never blended). Superseded on
2026-11-14: the owner chose the gap-size fade between surveys too ("The hand-over between surveys" below).

## Mutation

- Reviewer: 38 mutants on a copy of the worktree, full `tests/bathy` each: 26 killed, 2 equivalent; the real survivors
  are the gaps listed under F-2 and F-7.
- Fix round: 17 mutants on a scratch copy against `tests/bathy/test_bathy_lidar.py`: 16 killed. The pass-1 core offset
  survived until the walled pond was added (it now stops the build). One equivalent: a second junk filter before the
  geoid as well as after it (it can only drop raw values beyond 12,000 m, which no lidar survey holds).

## Suites

`tests/bathy` 146 passed (venv with rasterio / scipy); the whole repository suite on the system Python (bathy tests
skipped there) green.

## F-3: the owner's choice (2026-11-14)

The question: where the lidar has a gap (its outer edge, or a hole inside the survey where the fill grid shows through),
how wide should the fade from the fill to the lidar be? Three options were built with the real builder on the same three
boxes and shown to the owner on one sheet (pictures at z17 over the imagery, plus the numbers below):

- A: 24 px (~28 m at z17) round every gap, the outer edge and every hole alike (as reviewed in G63a).
- B: 24 px at the outer edge only; holes get a hard edge.
- C: the fade's width follows the gap's own size: the full 24 px at the outer edge and round big holes, a few pixels
  round a small hole.

Why not a hard edge anywhere: lidar and the regional grid differ at the gaps (mode-A builds, one sample per edge pixel):

| Place | median step | p90 | over 0.5 m |
|---|---|---|---|
| Pipeline - Sunset | 0.26 m | 0.68 m | 18 % |
| Kaneohe Bay | 0.29 m | 1.64 m | 32 % |
| Rincon | 0.21 m | 0.70 m | 17 % |

Share of lidar pixels at z17 that are blended:

| Place | A | B | C (sheet) | C as built |
|---|---|---|---|---|
| Pipeline - Sunset | 2.62 % | 1.48 % | 1.70 % | 1.78 % |
| Kaneohe Bay | 14.42 % | 6.98 % | 9.35 % | 8.94 % |
| Rincon | 9.14 % | 5.37 % | 5.89 % | 6.03 % |

**Owner's decision: C** ("Go with C, then proceed").

### The rule as built (`build_bathy.feather`)

- A gap pixel (fill only, no lidar) has a depth = the largest distance from a fill-only pixel to the lidar within
  `FEATHER_PX` (24) of it, a square window.
- A lidar pixel at distance d from its nearest gap pixel q blends when d < width, where width = q's gap depth, clipped
  to `FEATHER_MIN_PX` (2) .. `FEATHER_PX` (24). The weight is linear: d / width of the lidar, the rest the fill. The
  pixel keeps the lidar's source id and is flagged blended (0x40).

Deviation from the sheet's C: the sheet measured each gap's size per connected gap (its largest inner distance). That
value depends on the whole gap, which a block read cannot always see: a gap cut by the read area's edge would get a
different fade in each of the two blocks it touches (a seam). The built rule measures the gap's depth near q only, in a
window of `FEATHER_PX`, so every quantity is found within 3 x `FEATHER_PX` of the pixel. The shares above show the two
agree within half a point.

Margin: a region with fill sources reads 72 px (3 x 24) round each block (was 32); `region_config` refuses less, so the
blend is the same on both sides of a block edge. The NW Islands (no fill) keep 32.

### Evidence

- Tests (`tests/bathy/test_bathy_lidar.py`): small holes fade narrow (a radius-3 hole reaches 2-3.2 px), big holes and
  the outer edge fade the full 24 px, every blended depth lies between the two sources, the rest is untouched, fewer
  pixels blended than with A; crops of a synthetic field read with a 72-px margin give exactly the values of the whole
  field (>= 16 crops), a 24-px margin does not; the margin rule in `region_config`. Bathy suite 148 passed.
- Mutants (`mut_c.py`, on scratch copies): 11; 10 killed; the survivor (the window filter's edge mode "reflect" for
  "nearest") changes only pixels within 24 px of the read area's edge, never a block's core: equivalent.
- Real data, one Kaneohe block read with a 72-px margin and with a 144-px margin: the blend widths and the chosen gap
  pixels are identical at every pixel; the merged depths agree within 0.0000076 m (rounding inside GDAL's warp, far
  below the 1-cm storage step). With the old 8-px margin the same block differed by up to 1.2 m at 5,013 pixels: the
  block-edge seam the wider margin removes.
- Real builds with the committed code: Pipeline 352 tiles, Kaneohe 1,397, Rincon 235; check 0 problems on each.

## Step 5: the test site with locally built lidar tiles (2026-11-14)

Set-up: the four lidar regions built on the laptop with the committed code (936088a) and served to the test site
(wave-app-clean, overlay 2.27.6, `BATHY_OVERLAY=1`) through DevTools Fetch. The page's `regions.json` gained the four
regions (version 1, priority 20); their index, idx and tile requests were answered from the local build; everything else
came from the live bucket. Headless Chrome with the GPU, its own profile, trusted mouse and touch input. Nothing was
uploaded.

### What was checked

| Check | Result |
|---|---|
| Views: Pipeline, Sunset, Waimea, Waikiki, Kaneohe, Tumon, Rincon, Nihoa at zooms 13-18 | 26 views at 1280x800 and 18 at 1920x1080. The readout = the drawn pixel at every sampled point (1,440-3,240 a view). 0 problems |
| Hover text | 12 of 12 = the drawn sample, US and Metric (e.g. "20.7 ft · USACE lidar 2013 · approximate") |
| Phones | 375x812 and 812x375 at Pipeline and Tumon z17: readout = drawn (480-720 a view), page width = window width, "Sources here" names the lidar |
| NW Islands (Nihoa z8-z17) | lidar tiles from z9, GEBCO beneath; z15-z17 0 % blended (that region has no fill) |
| Tile edges | Between two tiles of one survey the step is the same as between two pixels inside a tile (Pipeline z16 p95 0.43 m vs 0.34 m inside; Rincon z17 0.13 m vs 0.16 m). Where a lidar tile meets a 3-m-grid tile: p95 0.01-0.07 m |
| Credits | Pipeline "Bathymetry: USACE · NOAA", Tumon "NOAA", Rincon "NOAA · USACE", Nihoa "NOAA · GEBCO"; "Sources here" gives each survey and year; production's /notice/bathy has the lidar citations |

First view at 1280x800 (bytes from the bucket + from the local lidar build; today = production's tiles only):

| View | today | with lidar | decoded store, today -> lidar |
|---|---|---|---|
| Pipeline z16 | 217 kB, 11 requests | 216 + 695 kB, 10 + 22 requests | 1.8 MB (7 tiles) -> 6.9 MB (27) |
| Pipeline z17 | 154 kB | 154 + 643 kB, 8 + 22 requests | 1.3 -> 6.3 MB |
| Tumon z17 | 151 kB | 150 + 551 kB | 1.3 -> 6.1 MB |
| Kaneohe z16 | 335 kB | 335 + 980 kB, 13 + 26 requests | 2.5 -> 8.6 MB (34 tiles) |

A lidar view costs 0.55-1.0 MB more the first time (the plan estimated 1-1.5 MB). The local tiles came from the laptop,
so their load times say nothing about the bucket's.

Memory, 40 pan steps at z17: the decoded store peaked at 23.9 MB (its cap is 32 MB), 94 tiles; the page's heap peaked at
97 MB (today 10.7 MB, 42 tiles, 58 MB); readout = drawn at 1,860 of 1,860 points.

Pan cost (frame gaps while dragging, lidar vs today): p50 and p95 17 ms on both; the slowest frame 67-117 ms vs
50-83 ms; frames over 100 ms 0-1 vs 0; over 250 ms none on either; one long task (55-124 ms) vs one or two (58-103 ms).

### Finding: the step where two lidar surveys meet

Where the newer survey ends and the older one takes over (Rincon: NOAA NGS 2019 -> USACE 2018; Niihau: NGS 2021 ->
USACE 2013), the builder switches with no fade. That is by design (the fade only runs from a lidar to its 3-m fill;
G63a C4 and I-4), so the two surveys' differences show as a step:

| Place | newer minus older, where both have data: median | abs p90 | p99 | max |
|---|---|---|---|---|
| Rincon | +0.42 m | 0.87 m | 1.90 m | 6.25 m |
| Niihau north-east | +0.10 m | 0.60 m | 1.50 m | 4.10 m |

The differences grow with depth at Rincon (about -0.1 m near the shore to -0.4..-0.7 m at 15-30 m in the step-1 check),
so they are survey differences, not a datum offset.

Options studied (each built with the real builder on the same boxes; the step measured as the jump between neighbouring
pixels across the hand-over line, p50 / p90 / p99 / max in metres):

- 1. Keep the hard step (today).
- 2. Rule C between surveys: the older survey fades in at the newer survey's edge the way the fill does, the fade as wide
  as the gap (2-24 px).
- 2'. The full 24-px fade at every hand-over.
- 3. A difference surface: the older survey shifted by the smoothed difference between the two where they overlap (the
  older survey's depths move; a larger change).
- 4. One survey per area: the older lidar dropped where the newer one exists, the 3-m grid in the newer survey's holes
  (the older survey's 1-m detail there is lost).

| Rincon z17 (86,252 hand-over pairs; jumps inside a survey: p90 0.14, p99 0.41) | jumps | pixels changed by > 1 cm | lidar pixels blended |
|---|---|---|---|
| 1. today | 0.09 / 0.54 / 1.26 / 6.25 | - | 6.03 % |
| 2. rule C | 0.08 / 0.34 / 1.06 / 3.44 | 2.62 % | 6.98 % |
| 2'. 24 px | 0.08 / 0.35 / 1.09 / 3.44 | 6.19 % (one by 10.07 m) | 10.55 % |

| Niihau z17 (21,750 pairs; jumps inside a survey: p90 0.20, p99 0.53) | jumps | pixels changed by > 1 cm | lidar pixels blended |
|---|---|---|---|
| 1. today | 0.09 / 0.38 / 0.97 / 4.10 | - | - |
| 2. rule C | 0.08 / 0.25 / 0.57 / 2.78 | 0.16 % | 1.62 % |
| 2'. 24 px | 0.08 / 0.23 / 0.50 / 2.78 | 0.97 % | 2.57 % |

- By depth at Rincon (p90, today / C / 24 px): 0-5 m 0.48 / 0.33 / 0.25; 5-10 m 0.40 / 0.29 / 0.30; 10-20 m 0.57 /
  0.36 / 0.36; 20-40 m 0.62 / 0.31 / 0.32.
- By the width rule C gives (p50 / p90, today -> C -> 24 px): 2-4 px 0.45 / 1.09 -> 0.24 / 0.71 -> 0.13 / 0.62;
  4-8 px 0.40 / 0.91 -> 0.13 / 0.55 -> 0.11 / 0.53; 8-16 px 0.46 / 0.85 -> 0.09 / 0.39 -> 0.08 / 0.39; 16-24 px
  0.47 / 0.79 -> 0.07 / 0.26 -> 0.07 / 0.27. A third of Rincon's hand-over pixels (35.7 %) get a width under 8 px;
  at Niihau 95.8 %.
- Tiles: rule C writes the same tiles (Rincon 235). The 24-px fade dropped two Niihau tiles (z17 7206/57360, z16
  3603/28680): it pushed 11 shoreline pixels near sea level above the +0.4-m water line, so they became land.
- In the real client (the test site, a z17 view centred on the hand-over, today vs rule C): 4.3 % of the pixels change
  visibly at Rincon (1.2 % at z16), 0.9 % at Niihau. At normal size the difference is small; at 2x rule C removes the
  dark jagged streaks and the kinked contours along the hand-over.
- Neither fade brings the hand-over down to the roughness inside a survey (p90 0.34 / 0.25 against 0.14 / 0.20).

Author's recommendation: option 2 (rule C between surveys). It removes the staircase, halves the largest step, cuts the
p90 by about 35-40 % and touches few pixels (Rincon 2.6 %, Niihau 0.16 %) with no tile lost. The 24-px fade gains little
more at narrow holes, changes 2.4 to 6 times as many pixels and pushed a beach sliver over the water line. Options 3 and
4 are larger changes for a seam that two places show.

If option 2 is chosen, the build work: the real rule in `read_field` (each own source after the first fades in by rule
C), the 72-px margin also for regions with two or more own sources, C4 / I-4 above updated, the notice and README, the
`feather` docstring ("3 px" -> "3 x px"), tests and mutants, a review, then a job-only merge with a new rollback tag
before any upload.

### Owner awareness

- Cloudflare R2 behind models.allshoresurf.com answered HTTP 500 "Internal Error" now and then on requests not yet in
  the edge cache (about 20:00 UTC 2026-11-14): 2 of 20 uncached `regions.json` requests, one hawaii v2 z15 tile and one
  GFS direction frame; all 200 when asked again. The client retries by itself; once the panel showed "Some depth files
  could not be loaded; trying again shortly". Not our code.
- The notice says the fade runs "within 24 pixels"; with rule C most gaps fade over fewer (still true, less specific).
- Niihau 2021 sits +0.21 m above 2013 after the conversion (I-1); French Frigate Shoals was not checked for water marked
  as land; footprints are not antimeridian-safe (F-6); the NW Islands' lidar meets GEBCO with no fill; the test site's
  /notice/bathy still shows the old text (production has the new one).

## The hand-over between surveys (2026-11-14)

### The owner's choice

After step 5 the owner chose "Fade by gap size (Recommended)": where two of a region's own lidar surveys meet
(Rincon: NOAA NGS 2019 and USACE 2018; Niihau: NGS 2021 and USACE 2013), the older survey fades in under the newer one by
the same rule as the fade to the 3-m grid (rule C: the width follows the gap's own size, 2-24 px).

### The rule as built

- `read_field` merges each own source in id order (the newer survey first). When an own source marked `survey_fade`
  overlaps what is already merged, it goes through `feather(merged, merged ids, older, older id)` instead of the plain
  first-valid merge: it fills the newer surveys' gaps, and inside the newer survey's edge the depth blends toward it.
  Faded pixels keep the newer survey's id with the blended flag (0x40), as at the lidar's edge.
- Surveys that never overlap (different islands) are merged as before: `feather` is not called for them, so a block
  calls it once, for the fill beneath.
- `survey_fade=True` is set on the `LIDAR` template only. The stage-1a CUDEM pairs (1/9" over 1/3" in hawaii, marianas,
  samoa, prvi: several own sources, no fill, margin 8) keep the first-valid merge, so their published builds are
  unchanged (a test pins that every lidar source is marked and no CUDEM grid is; another that unmarked surveys keep the
  plain merge and the small margin).
- `region_config` requires the 72-px margin (3 x FEATHER_PX) for a region with fill OR with two or more `survey_fade`
  sources; hawaii-lidar and pr-lidar have both, guam-lidar has fill only, nwhi-lidar one survey and no fill (32).
- The licence text of a region with two or more marked surveys gains "Where two lidar surveys meet, the older one blends
  in under the newer one the same way (marked blended)."; /notice/bathy and the README say the same.

### Evidence

- Real rebuilds with the builder (the step-5 boxes) are byte-identical to the step-5 prototype of rule C between
  surveys: Rincon 57 + 177 tiles (digest adfe7606e1ebb6c6), Niihau 78 + 278 (digest 7be183dc80e42dec); check 0 problems.
- Hand-over jumps (neighbouring pixels across the hand-over line, today's build vs the real rule-C build, the step-5
  metric; p50 / p90 / p99 / max in metres):

| Place | today | rule C | pixels changed > 1 cm | blended share of water, today -> rule C |
|---|---|---|---|---|
| Rincon (86,267 hand-over pixels) | 0.09 / 0.54 / 1.26 / 6.25 | 0.08 / 0.34 / 1.06 / 3.44 | 2.62 % | 4.44 % -> 6.98 % |
| Niihau north-east (21,750) | 0.09 / 0.38 / 0.97 / 4.10 | 0.08 / 0.25 / 0.57 / 2.78 | 0.16 % | 1.46 % -> 1.62 % |

  The roughness inside a survey (lidar more than 60 px from a hand-over): p90 0.14 m Rincon, 0.20 m Niihau.
- Tests (tests/bathy/test_bathy_lidar.py): `test_an_older_survey_fades_in_under_the_newer_by_the_gap_size` (a 6-px hole
  in the newer survey: a narrow fade round it, the full 20-24 px where the newer survey ends, every faded pixel between
  the two surveys with the newer id + blended), `test_surveys_that_never_overlap_are_merged_as_before` (feather called
  once a block, for the fill), `test_the_survey_fade_sees_an_edge_in_the_neighbouring_block` (the block beside the newer
  survey's edge fades its last pixels; the same pixels read from the next block agree),
  `test_unmarked_surveys_keep_the_plain_merge_and_the_small_margin`, `test_every_lidar_source_is_marked_and_no_cudem_grid_is`;
  the existing seam test now expects the blend between the surveys, the hole test checks the licence sentence, and the margin test refuses two
  marked surveys below 72 px.
- Mutants: 10 on a scratch copy, all killed (one, the licence sentence's survey count, first survived and was pinned).
- Suites: `tests/bathy` 153 passed (venv); the repository suite 2,847 passed + 1 skipped on the system Python, with one
  test deselected: `test_capped_fetch_cuts_a_trickle_smaller_than_one_chunk` (the section-63 buoy fetch) fails on this
  Windows machine even alone and on the unchanged code (a shut socket does not wake a waiting read on Windows: section
  63's note); it is untouched here and runs on CI's Linux.
