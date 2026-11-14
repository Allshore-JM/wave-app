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
| F-3 | P3 effect | The 24-px blend rings every interior lidar hole, not only the outer edge (Kaneohe: 2.8-40.6 % of a tile's lidar pixels blended) | **Open: the owner's call.** As built and documented ("also round the lidar's holes"). Options on record: a band scaled to the hole's size, or the outer edge only. Not changed without the owner's word. |
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
