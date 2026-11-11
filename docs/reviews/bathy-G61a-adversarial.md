# G61a — bathymetry tile builder (plan section 93b, stage 1a)

Reviewed: `feat/bathy` @ ff6c90d (the builder: `tools/bathy/*`, `.github/workflows/bathy-build.yml`, `tests/bathy/*`).
One fresh-context reviewer, then a fix round by the author. Fixes @ 6bc2d48.

## Counts
P0 0, P1 0, P2 2, P3 6 groups. Every P2 fixed; every P3 fixed or recorded below.

## What held up (the reviewer's own measurements)
- Values: five real-data boxes (Pipeline, Honolulu + Pearl Harbor, Rincon PR, Pago Pago, Tumon Guam) against an
  independent reader (own mercator maths, exact area-weighted mean of the native CUDEM cells): every error within half
  of its code step (median 2.5-4.6 mm; max 5.0 mm in the 1-cm band, 5.0 cm in the 0.1-m band); full-grid comparison of
  every wet z15 pixel in four boxes, max 0.050 m. No nodata ever became a depth.
- Aprons: all 8 directions, codes and source plane, every zoom: 0 mismatches over 6,696 strips, including block edges.
- File format: all 837 PNGs 258 x 774, 8-bit grey, not interlaced, IHDR / IDAT / IEND only.
- Depth code boundaries exact; the decode table the inverse to half a step.
- Halving: the wet rule (2 of 4) and the source rule 0 mismatches over 2.7 M parent pixels.
- Joins across block edges: nothing connected was dropped in any box; named harbours wet.
- Index files: an independent decode equals the files exactly.
- GEBCO: the real 2026 set is 8 quadrant files on 90-degree lines; every z7 tile lies inside exactly one.
- Upload order (files, licence, index, regions.json) and the different-build refusal held on a fake store.
- Workflow: tests on push, build only on dispatch after the tests, actions pinned by SHA, certifi on the one curl.

## Findings and outcomes
| # | Finding | Outcome |
|---|---|---|
| P2-1 | Water connectivity was decided on a 2 x 2-pooled mask: a pond behind a one-pixel wall (or a two-pixel wall astride a cell edge, or a diagonal touch) joined the sea. Real data: 2,403 px of inland water published wet in the Honolulu box | FIXED: connectivity at the region's finest zoom, 4-neighbour, each block labelled alone and joined across block edges with a union-find (the same result as labelling the whole region at once). Test with ponds behind 1- and 2-px walls at both parities |
| P2-2 | GEBCO water = elevation < 0 published below-sea-level land and lake beds (Qattara, Dead Sea, Caspian, Baikal, Great Lakes basins) as sea depth | FIXED: GEBCO water = below 0 m AND not on GSHHG land (the site's own coast-v1 file `world-i.bin`, its sha256 pinned in `bathy_sources.LAND`, rasterised per tile); a lake or basin inside the land stays land. Tests |
| P3-1 | The water threshold was MHHW + 0.5 cm, and pass 1 and pass 2 disagreed on it (394 px in the Honolulu box) | FIXED: both passes test the stored depth code |
| P3-2 | `--box` starting with a minus sign was refused by argparse | FIXED: the README gives `--box=...`; a test drives `main` with it |
| P3-3a | Re-uploading an older identical build moved regions.json back silently | FIXED: refused unless `--replace` |
| P3-3b | `--replace` overwrote immutable objects while the old index stayed live, and left the old build's extra objects | FIXED: a replace deletes the old index.json first, writes the new files, deletes the old build's files the new one lacks, then writes LICENSE.txt and index.json |
| P3-3c | The "same build" log claimed files it did not write | FIXED: says it rewrote only regions.json |
| P3-4 | Workflow: concurrency covered the push tests; `version` "01" passed; free-text switches | FIXED: concurrency on the build job only; version `^[1-9][0-9]*$` (the line run under bash in a test); `upload` / `replace` are booleans |
| P3-4 | Inputs checked after the pip install; the GEBCO zip's sha256 logged, not pinned | ACCEPTED (harmless; GEBCO publishes one zip a year, the quadrant check guards a damaged set) |
| P3-5 | An undeclared fill value could become a 10-km "trench"; a GEBCO set missing a quadrant built a hole; `check` passed an empty build; LICENSE.txt outside the digest; tiles not checked against their zoom's range; sources merged in list order | FIXED: nodata required per source, any value of 12,000 m or more either way dropped (sources and GEBCO); the eight quadrants each once or the build stops; an empty build refused; the digest covers LICENSE.txt; every zoom inside the region's range and every tile inside its zoom; sources sorted by id |
| P3-5 | A coastal pixel averages land heights into its depth (centimetres at z15, metres at GEBCO z7) | ACCEPTED: shore pixels are approximate (the panel says so); GEBCO is context only |
| P3-6 | 13 surviving mutants (two-source order, fill values, idx edits, plane agreement, unknown ids, read-backs, interlace, workflow exit, same-size edits) | FIXED: tests for each (24 new tests in `tests/bathy/test_bathy_g61a.py`) |

## Found by the new tests
- The builder wrote tiles one zoom below a region's `zmin` (a test region with zmin 10 got z9 tiles). Fixed: pass 2
  never writes below zmin; a region's zmin / zmax are range-checked when the region is read. The real regions (zmin 8)
  were not affected.

## Checks after the fix round
- Suites: `tests/bathy` + `test_tls_pins` + `test_ci_robust`: 122 passed.
- Mutants of the fix round: 25 of 25 killed (scratch `g61a/fix/mut61a.py`, on a scratch copy).
- Real data, Honolulu box (-158.02,21.25,-157.80,21.40): 271 tiles, 8.2 MB, 41 s, `check` 0 problems. The water read
  back against an independent whole-box labelling (scipy, 4-neighbour, one warp of CUDEM at z15): 37,748,736 pixels,
  8,139,623 water, 0 differ; 887,576 low pixels in 1,520 areas dropped as not connected. Pearl Harbor, Honolulu Harbor,
  Kewalo Basin and Ke'ehi Lagoon wet; the Ala Wai Canal dry (a culvert at its mouth in the data); the Ala Wai Boat
  Harbor basin wet.

## For the owner
- GEBCO's real values (Qattara, Dead Sea, Caspian) were not read by the reviewer (a 4.24-GB download); the land mask
  is pinned by synthetic tests and the real header.
- NOAA's files label Samoa's and CNMI's heights as NAVD88 (not possible there; a label error in NOAA's metadata).
  The builder applies no vertical transform, so it has no effect.
- The Kaneohe fishponds were not built; by the fixed rule a walled pond stays dry unless its wall has a gap in the data.
