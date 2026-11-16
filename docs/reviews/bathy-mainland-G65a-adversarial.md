# G65a: the US mainland and Great Lakes bathymetry builder (plan section 98)

One fresh reviewer, read-only, on `feat/mainland` @ 1c974d6 (`git diff 5043821..1c974d6 -- tools/bathy tests/bathy
.github/workflows README.md`). The reviewer checked the datum grids against CO-OPS's own answers, read BlueTopo tiles
across the UTM zone lines, built small boxes with the real sources, attacked the lake-archive extraction, and ran 56
mutants.

**Result: 0 P0, 1 P1, 2 P2, 7 P3.** Seven are fixed, three are recorded for the owner.

## Findings and outcomes

| # | Level | Finding | Outcome |
|---|---|---|---|
| F1 | P1 | The St Johns River west of -81.6 (downtown Jacksonville, Mandarin, Doctors Lake, Green Cove) was drawn by neither region. In `us-gulf-east` the river has no seed (its mouth is east of -81.6), so it became land; `us-fl-east` reached it through Mayport but cut everything west of -81.6. A box build showed "pass 2: 0 tiles". | Fixed. Each US region reads 0.3 degrees past its box for the water rule (`reach`) and cuts back to its own box only after the rule (`keep_box`). The St Johns box rebuilt: 822 tiles, 8.7 MB, z15 water 531,554 px (river, Doctors Lake, St Marys). |
| F2 | P3 | `us-sc-ga` and `us-gulf-east` overlapped (-82.0..-81.6, 30.7..31.2). Gulf-east marked the St Marys and Satilla reaches as land there, and its land mark would hide sc-ga's water. | Fixed. Gulf-east leaves sc-ga's box out, but only AFTER the water rule (`exclude_late`), so the corner's water still joins the St Marys to the sea. Tested at every zoom. |
| F3 | P3 | The Raritan River's tidal reach (Perth Amboy to New Brunswick) lay in no box. | Fixed. `us-sne-ny` starts at -74.5 (Hudson and Raritan). `us-me-nh` reaches south to Cape Ann (42.8). |
| F4 | P2 | The MSL-above-NAVD88 grid over Lake Pontchartrain was pulled up 0.3-0.5 m by a Mississippi River stage gauge (Carrollton, 1.773 m). Every depth in the lake came out 0.3-0.5 m too deep, and fringe marsh up to ~0.6 m counted as low. The same, smaller, near other river gauges. | Fixed. `drop_outliers` removes, worst first, a station whose leave-one-out error exceeds 0.3 m (13 river-stage gauges). MSL leave-one-out after: median 0.022 m, p90 0.079 m, max 0.294 m. `us_msl_navd88.tif` re-pinned. The MHHW grid is unchanged. |
| F5 | P3 | The MHHW grid smears ocean and lagoon tides across barrier islands: Rodanthe's ocean side gets 0.127 m (the ocean's is ~0.55 m), the Indian River Lagoon at Vero 0.351 m (its gauges 0.07-0.14). It only decides which intertidal pixels count as low. | For the owner. Documented in the README. |
| F6 | P3 | Lakes held above sea level behind locks (Lake Washington, Lake Union) join Puget Sound through the locks and are drawn as sea, with depths below mean sea level. | For the owner. Documented in the README. |
| F7 | P3 | The Great Lakes' connecting waters lay outside every lake box: the St Marys River with the Sault harbour, the Thousand Islands, the Niagara River at Grand Island. Where a box edge crosses a river the datum steps (0.9 m in the Detroit River, 1.6 m at the head of the St Clair River). | Partly fixed. Huron now reaches 46.6 N (the St Marys) and leaves Ontario and Superior out; Erie reaches 43.1 N (Grand Island) and leaves St Clair and Huron out. Still open, for the owner: the Thousand Islands (no box), the two datum steps. |
| F8 | P3 | A `clip` box was block-granular: a clipped survey was used up to a block (~9 km) past its clip line. | Fixed. A clipped source is cut per pixel after its warp (`clip_area`). |
| F9 | P3 | The README said BlueTopo is read through "one VRT per UTM zone"; the builder reads the tiles one by one. | Fixed (README). |
| F10 | P2 | The tests did not pin the region cut, the clip, BlueTopo, the US boxes or the source table's datum and zoom values: 24 of 55 mutants survived. | Fixed. New tests in `tests/bathy/test_bathy_mainland_g65.py` (50): the reach and the cut at every zoom, `exclude_late`, the clip per pixel, BlueTopo's tile choice and order over the reach, the boxes and source table pinned, the station drop. |

## Checks after the fixes

- Tests: `tests/bathy` (venv) and the full suite (system python), recorded in the commit.
- Mutants (scratch copy): 38 of 38 fix-round mutants killed. The first run left survivors in the cut, clip and table
  families (M18, M20-M22, M24-M30, M41-M43, M45-M47, M53); the second in the reach / `exclude_late` / station-drop code
  (N19, N23, N23b, N23c, N44, N60-N74). Each was pinned by a new test and re-run: all killed.

## Runner dry run (temporary workflow `mainland-dryrun.yml`, removed after; run 43793687426, 4 cores, the code before the fixes)

| Region | Tiles | Size | Time |
|---|---|---|---|
| us-gulf-west | 127,285 | 2.14 GB | 102.3 min |
| us-gulf-east | 68,411 | | 77.6 min |
| us-nj-ches | 66,038 | | 61.2 min |
| us-ca-south | 65,180 | 1.68 GB | |
| us-or-wa | 52,916 | | |
| us-va-nc | 49,275 | | |
| us-sne-ny | 46,759 | | |
| us-ca-north | 42,327 | | |
| us-sc-ga | 39,859 | | |
| us-me-nh | 38,172 | | |
| us-fl-south | 30,778 | | |
| us-fl-east | 22,908 | | |
| Lake grids (five) | 39-1,000 each | | under 1 min each |
| Lake surveys (five "-dem") | 454-8,282 each | | |
| All 24 jobs | 674,228 | 13.76 GB | 9.7 runner-hours |

Every region's check found 0 problems; max RSS ~637 MB; the 300-minute job limit was never near. The fixed code reads
0.3 degrees more on each side of each US box, so the real builds will take somewhat longer and write a few more tiles
(the cut keeps each region to its own box).

## Held up (from the reviewer)

- The datum's sign and size: 12 CO-OPS stations fetched today agree with the committed grid within 0.016 m; a rebuild
  from the committed station list reproduces both pinned hashes; a built San Diego tile = -(survey NAVD88 - grid) within
  0.005 m.
- The MHHW grid is used per pixel in every pass and stops outside its extent.
- BlueTopo: 6,083 tiles meet the US boxes, all "UTM + navd88" with NaN nodata; reads across the -84 and -78 zone lines
  agree with each tile sampled directly within 0.01-0.05 m.
- The source order and deep rules match the README; the lowest id wins.
- The sea seed holds for Pamlico, Albemarle, Pontchartrain, SF Bay and Puget Sound; quarries and the shelf stay out.
- Great Lakes: the archives match their pins; tar attacks on the extraction are all refused; the lake offsets have the
  stated signs; St Clair's -0.9 m from Erie's grid is supported by the survey comparison (evidence, not proof).
- The client stacks the "-dem" regions above the grids as the live lidar regions do; uploads are unchanged and the
  34 regions fit the client's limit of 64.
