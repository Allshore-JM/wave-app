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
  no value; margin 32 = the 24-px band + the coarsest apron's 8.
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
NGS / USACE seam at Niihau is a hard ~0.2-m step by decision (two lidar surveys are never blended).

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
