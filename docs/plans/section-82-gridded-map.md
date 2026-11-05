# Section 82: the gridded satellite wave-height map (plan, 2026-11-05, awaiting approval)

Follows section 81 (the satellite layer, merged to production as 584a772 on 2026-11-05; its plan copy
`section-81-satellite.md` holds the assessment this plan comes from). The standing rules of section 81 apply unchanged
(one cue; stop only for an owner call and before each production merge, a rollback tag + the owner's explicit
go-ahead each; the test site by cherry-pick, never force-push, never rebase a pushed branch; asset bumps; `python -I`;
headless Chrome; the security and pacing rules; Copernicus anonymous S3 + credit with the DOI).

## Context
The owner (2026-11-05): "I'm wondering if we can create a full interpolated wave height map based on the satellite
readings (in the satellite waves overlay). This should be selectable to turn on or off by the user." The assessment
(section 81's plan copy, "Assessment") weighed our own interpolation of the along-track readings against Copernicus
Marine's gridded level-4 product and recommended the latter: an official, credited field made with proper space-time
weighting, for about two days of work, at the cost of a one-day lag.

Facts checked on 2026-11-05 (the bucket and one real file):
- Product `WAVE_GLO_PHY_SWH_L4_NRT_014_003`, DOI 10.48670/moi-00180, dataset
  `cmems_obs-wave_glo_phy-swh_nrt_multi-l4-0.5deg_PT6H-i_202607`: `VAVH_INST`, "an estimate of the instantaneous wave
  field every 6 h at 0.5 degrees, using all available level-3 along-track measurements and accounting for their
  spatial and temporal proximity" (Sentinel-6A, Jason-3, Sentinel-3A/B, SARAL, CryoSat-2, CFOSAT, SWOT, HY-2B/C).
- Anonymous on the same CloudFerro bucket as the level-3 products, under
  `native/WAVE_GLO_PHY_SWH_L4_NRT_014_003/<dataset>/YYYY/MM/`: one ~9 MB NetCDF a day named
  `..._<day>T120000Z_P<production>Z.nc` holding the four steps 00 / 06 / 12 / 18Z of that day, produced about 07:30Z
  the NEXT day (`20261103T120000Z_P20261104T073010Z`, last modified 08:31Z). So the newest step is 13-37 h behind
  the clock. The older production of a day is purged when a newer lands (the level-3 pattern).
- Grid: 720 x 359 cells, centres at longitudes -180 .. 179.5 and latitudes -89.5 .. 89.5: exactly the model's
  half-resolution grid (0.5 degrees, centre registration, lon0 -180) without its two pole rows. int16 x 0.001 m,
  fill -32767, 0 .. 11.6 m in the file seen; 143,307 valid cells a step (55 % of the grid: open ocean only, blank
  within ~50 km of coasts and under ice). `VAVH_INST_FLAG` = 1 where the value was invalid and thresholded to 0 m
  (1,179 cells): treated as absent.
- A 2-degree daily mean dataset sits beside it; not used.

## Decisions proposed (the owner confirms or changes; nothing is built before)
1. **Source: Copernicus L4 0.5-degree 6-hourly** (not our own interpolation; see the assessment).
2. **Placement: a "Gridded map" toggle in the Satellite waves panel**, off by default, remembered per tab, drawn
   UNDER the dots / arrows in the wave-height colours (the same legend). Not a dropdown entry of its own.
3. **Which step: the newest available**, named in the panel with its age ("Gridded map: CMEMS L4 · Nov 3, 18:00 UTC
   (31 h ago)"). A step picker (the day's four steps) is left for later if wanted.
4. **Hover:** the grid value joins the reading's hover ("… · grid 8.1 ft"); over water with no reading under the
   cursor the grid value alone ("grid 8.1 ft · Nov 3, 18:00 UTC").
5. **Flag:** `ALT_GRID=1` (needs `ALT_OVERLAY=1`), test service first, each with its own go-ahead.
6. **Credit:** the panel's credit line gains the DOI 10.48670/moi-00180 while the map is on; the map credit stays
   "Overlay: Copernicus Marine" (the title carries the three DOIs).

## Design
**A. Job** `tools/model_frames/altgrid.py` (workflow `model-altgrid.yml`, group `model-altgrid`): on the frames
machinery (`publish.py`, `encode.py`), not the chunk machinery, since the product is a field.
- Discovery as the level-3 jobs: STAC for the dataset's native prefix (cached in the pointer 6 h), else the bucket
  listing; the listing of this month and last (`max-keys` paged), the newest production per day kept.
- Download the newest day's file (and the day before when its production is newer than the pointer's: a day is
  re-produced); `read_l4(path)` checks units, scale, fill, the grid (720 x 359, -180 / -89.5 starts, 0.5 steps) and
  the time axis (days since 1950) and returns the four steps as float32 (absent = NaN, flag 1 = NaN).
- Encode each step on the model's half grid: the 359 rows placed into the 361-row half grid (one absent row at each
  pole), `encode.quantize` on `encode.FIELDS["hs"]` (lo 0, hi 22.86: a value above is clamped, the stats say how
  many), no coast fill (the product's blanks are honest), `to_png` → `altgrid/v1/<day YYYYMMDD>/hs-half-<step 00|06|12|18>.png`.
- Manifest `altgrid/v1/<day>/manifest-<production>.json`: `{format "altgrid-v1", day, production_utc, grid (the half
  grid: cols 720, rows 361, lat0 90, lon0 -180, dlat -0.5, dlon 0.5, registration center, lon_periodic true),
  fields {hs: FIELDS.hs}, files {template, res {half}} (half only), frames [{step, valid_utc, file, valid_cells}],
  source {name, doi, credit}}`; the pointer `altgrid/v1/latest.json` written LAST: `{format, day, manifest, newest_utc,
  production_utc, published_utc, checked_utc, datasets, datasets_utc, credit}`.
- Prune: days older than 3 kept days; stale alarm (exit 1, once per 12 h) when the newest step is older than 48 h
  (the normal lag is 13-37 h); failure records and back-off as the level-3 jobs.
- Schedule: cron `"17,47 7-10 * * *"` (the file lands 07:30-08:30Z) and `"17 */3 * * *"` otherwise; dispatched by
  the trigger Worker too if the owner wants (every 10 min is more than a daily product needs: the cron suffices).
- Tests `tests/model_frames/test_altgrid.py`: a fake STAC / listing / file written with h5py (the real layout), the
  grid placement (a cell's value lands at its latitude), the clamp and NaN rules, the pointer last, prune, stale;
  the real-file check in step 1 against the file downloaded on 2026-11-05.

**B. Client** (`static_overlay/overlay.js`, obs mode, the Satellite waves entry):
- a `GridStore` for `altgrid/v1` (the pointer with `no-cache`, the manifest, the newest step's PNG through the frames'
  decoder; re-read every 12 min while on; a cooldown on failure; the samples kept when the toggle goes off);
- a second `ModelGridLayer` in the field pane under `obsPane`, fed the manifest + the step's frame; the layer takes a
  manifest whose `files.res` has only `half` (a small change: `wantFull` is false when no full resolution exists, so
  the half frame draws at every zoom); the hs LUT and `SAT_FDEF` (the same scale as the dots);
- the panel: the "Gridded map" checkbox (aria, per-tab memory `satGrid`), the line "Gridded map: CMEMS L4 · <valid
  time> (<age>)" ("loading…" / "unavailable" / "older than 48 h" states), the credit line with the third DOI;
- the hover: `readoutAt` of the grid layer appended to `_satReadout`, or alone over empty water (the own entry
  currently shows nothing there);
- Off / another field: the layer goes (the store keeps the frame); a saved toggle with the flag off starts off;
- tests in `tests/overlay/sat.test.js` (the store, the half-only manifest, the toggle, the hover, the credit) +
  mutants; `bootstrap.test.js` / `test_overlay_flag.py` for the page flag; asset bump; README.

**C. Page / app**: `ALT_GRID` beside `ALT_OVERLAY` / `SARSWELL` (`_grid_overlay_enabled()`, `var GRID`, the create
option `grid: GRID`); the flag-off page byte-identical.

## Steps (one cue)
1. Job + tests + the real-file check (the grid placement verified against the NetCDF at a few cells) + a local build
   with `--local`; the dispatch dry run after the merge (GitHub registers workflows from the default branch).
2. One fresh reviewer on the job. STOP for the job-only merge (tag `prod-pre-altgrid-job` + go-ahead). The first
   live pointer verified in the bucket.
3. Client + page flag + tests + mutants + asset bump + README.
4. Test site (`test`, by cherry-pick; the owner sets `ALT_GRID=1` there): desktop + phone; a sheet for the owner.
5. Two fresh reviewers (code via Fable; the test site); fixes.
6. STOP for the client merge (tag `prod-pre-altgrid` + go-ahead). Then `ALT_GRID=1` on production (its own go-ahead).

Cue for section 82: "Cue: select Fable 5.1 and set effort to HIGH for section 82 (the gridded satellite wave map:
job, client, test site, review, fixes)."

## Known limitations
- 13-37 h behind the clock (one production a day at ~07:30Z covering the day before); the panel says the step's age.
- 0.5-degree cells (~55 km); blank within ~50 km of the coast and under ice: no use at a break, context only.
- Four steps a day: between them the field is the newest step, not an interpolation in time.
- Heights above the scale's 22.86 m (75 ft) clamp (none seen).

## §82 progress
- 2026-11-05: planned from the section-81 assessment and the real file; awaiting the owner's decisions 1-6 and the cue.
