# Section 82: the gridded satellite wave-height map (Copernicus L4) in the Satellite waves entry (plan copy)

Approved by the owner on 2026-11-05 (ExitPlanMode) in the cloud session that built section 81; copied here so any
session (cloud or desktop) has it. Branch `feat/gridded` off production `Live-Buoy-Update` @ 584a772.

## Context
Section 81 shipped the satellite layer (production `Live-Buoy-Update` @ 584a772, flags on). The owner found the
"Satellite readings" box over the model unhelpful (removed, asset 2.23.5) and asked for "a full interpolated wave
height map based on the satellite readings (in the satellite waves overlay), selectable on/off". The assessment in
`docs/plans/section-81-satellite.md` recommended Copernicus Marine's level-4 gridded product over our own
interpolation. Decisions taken by the owner in planning (2026-11-05):
1. Source: Copernicus L4 0.5-degree 6-hourly (`WAVE_GLO_PHY_SWH_L4_NRT_014_003`, DOI 10.48670/moi-00180).
2. Placement: a **"Gridded map"** checkbox inside the Satellite waves panel, off by default, remembered per tab,
   drawn UNDER the dots and the arrows (both kinds) in the wave-height colours (the same legend).
3. Step shown: the newest available, named with its age; times in the page's time zone (as the dots' hover).
4. Stale: shown with a warning past 48 h, HIDDEN past 72 h (the panel says so).
5. Two production merge gates as in section 81 (job-only, then client), each a rollback tag + explicit go-ahead.
6. Leftover of section 81 folded in: the Swell readings line reads "Readings: 24 h to 12:23 PM EST (7 h ago) ·
   2 satellites" (no data change).
Flag `ALT_GRID=1` (needs `ALT_OVERLAY=1`), test service first, then production, each with its own go-ahead.
Standing rules of section 81 apply (one cue; stop only for owner calls and the merges; the test site by
cherry-pick; never force-push / rebase a pushed branch; asset bumps; `python -I`; headless Chrome; the security and
pacing rules; Copernicus anonymous S3 + credit with the DOI). Work on a new branch `feat/gridded` off production.

The data (checked on the bucket and one real file, 2026-11-05): dataset
`cmems_obs-wave_glo_phy-swh_nrt_multi-l4-0.5deg_PT6H-i_202607`, anonymous under
`https://s3.waw3-1.cloudferro.com/mdl-native-05/native/WAVE_GLO_PHY_SWH_L4_NRT_014_003/<dataset>/YYYY/MM/`, one ~9 MB
NetCDF-4 a day `cmems_obs-wave_glo_phy-swh_nrt_multi_l4_0.5deg_PT6H-i_<day>T120000Z_P<production>Z.nc`:
`time (4,) float32 days since 1950-01-01` (00/06/12/18Z of `<day>`; float32 quantum ~2.8 min at 2026: round to the
hour), `latitude (359,) -89.5..89.5` ascending, `longitude (720,) -180..179.5`, `VAVH_INST (4,359,720) int16 x 0.001 m
fill -32767`, `VAVH_INST_FLAG` (1 = invalid, thresholded to 0: absent). Produced ~07:30Z the next day (the day's
older production purged): the newest step is 13-37 h behind. The grid is the model's half grid (`run.GRID_HALF`:
720 x 361, lat0 90, lon0 -180, 0.5 deg, centre, periodic) without its two pole rows: one absent row padded at each
pole (`out[1:360] = step`, rows flipped north-up) makes it pass the client's `validateGrid`.

## Design

### A. Job `tools/model_frames/altgrid.py` (workflow `model-altgrid.yml`)
A standalone daily job on the pattern of `sst.py` (own pointer, day keys, prune, failure back-off, stale alarm,
injectable `main(argv, get, store, reader, now)`), NOT `run.main` (it needs hs + pdir grids). Imports, never copies:
- `alt.py`: `Product` (for the keys / `stac_id` / `dataset_re` / `stale_h`), `stac_datasets`, `bucket_datasets`,
  `discover_datasets` (pointer-cached 6 h), `parse_listing`, `list_prefix`, `months_touching`, `FileRec`, `download`,
  `_scaled`, `_attr`, `parse_stamp`, `utc`, `parse_iso`, `put_object`, `write_pointer`, `record_failure`,
  `skip_failed`, `alarm_once`, `stale_check`, `_summary`, `_get`, `_store_from_env`.
- `encode.py`: `FIELDS["hs"]`, `encode_frame(g, "hs", fill=False, resolutions=("full",))` -> `out["full"]` (the PNG)
  + `out["stats"]` (min/max/valid/clamped); never the coast fill (`fill_allow()` is pinned to 721 x 1440).
- `publish.py`: `Store`, `IMMUTABLE`, `POINTER`, `redact`, `r2_client`. `run.py`: `GRID_HALF` (minus "derivation").
Constants: `PREFIX altgrid/v1`, `FORMAT altgrid-v1`, `STAC_ID`, `DATASET_RE r"swh_nrt_(multi)-l4-0\.5deg_PT6H-i"`,
`FILE_RE ^cmems_obs-wave_glo_phy-swh_nrt_multi_l4_0\.5deg_PT6H-i_(?P<day>\d{8})T120000Z_P(?P<prod>\d{8}T\d{6})Z\.nc$`,
`SOURCE` (name, doi, url, credit), `STEPS (0, 6, 12, 18)`, `MIN/MAX_FILE_BYTES 1 MB / 40 MB`, `KEEP_DAYS 3`,
`LOOKBACK_DAYS 4`, `STALE_H 48`, `SUPERSEDED_H 2`, `PRODUCT = A.Product(...)`.
Functions:
- `parse_file(name) -> (day, prod_dt, prod_stamp) | None`; `list_days(datasets, now, get) -> {day: FileRec}` (this
  month and last via `months_touching`, the newest production per day, sizes bounded).
- `read_l4(path, day) -> [(valid_dt, float32 (359, 720) north-up, NaN absent) x 4]`: checks time units (days since
  1950; rounded to the hour, refused if moved > 5 min; hours exactly 0/6/12/18 of `day`), the axes (359 ascending
  -89.5..89.5, 720 from -180, 0.5 steps), `_scaled(..., expect_scale 0.001, expect_fill -32767, expect_units "m")`,
  flag != 0 -> NaN, v < 0 or > 30 -> NaN (counted).
- `place_half(step) -> (361, 720)` (poles absent); `encode_step(g) -> (png, stats)`.
- `build(...)`: frames `altgrid/v1/<day>/<prod stamp>/hs-<HH>.png` (immutable), manifest
  `altgrid/v1/<day>/manifest-<stamp>.json` `{format, day, run (YYYYMMDD), production, production_utc, build, complete
  true, published_utc, encoding "u8-linear-v2", encoding_spec, grid GRID_HALF, fields {hs: FIELDS.hs}, fill null,
  frames [{step, valid_utc, file, valid_cells, max_m, clamped_high}], newest_utc, source, credit, stats}`, stats
  sidecar; `publish(...)`: stats -> manifest -> pointer LAST (`altgrid/v1/latest.json`, max-age 300) `{format, day,
  run, manifest, complete, newest_utc, production, production_utc, published_utc, checked_utc, datasets,
  datasets_utc, credit}`.
- `prune(store, pointer, keep=3, now)`: the newest 3 complete days + the pointer's day; never a manifest-less day
  newer than the newest complete; old production dirs of the pointer's day after `SUPERSEDED_H`; failure records of
  days older than the oldest kept; only under `altgrid/v1/`.
- `main`: `--dry-run`, `--local DIR`, `--day YYYYMMDD`, `--file PATH` (with `--day`: the real-file check), `--force`,
  `--keep N`; pointer format guard; `discover_datasets`; `list_days`; the newest day; same (day, production) =
  heartbeat (`checked_utc`); never regress; `skip_failed` / `record_failure` (3 attempts, 2-h back-off); download ->
  `reader` -> `build` -> `publish` -> `prune` -> summary; scheduled live runs end in `stale_check` (newest step >
  48 h: `STALE:` + exit 1 once per 12 h); dry / local / `--day` runs return 1 on their own failure; exit 2 bad args.
- `--force` re-uploads identical bytes to the same production keys (deterministic); an encoding change = a new PREFIX.
Workflow `.github/workflows/model-altgrid.yml` (copy of `model-sst.yml`'s shape with `model-alt.yml`'s pins): crons
`"17,47 7-10 * * *"` and `"17 */3 * * *"`, inputs day / dry_run / force, group `model-altgrid`, timeout 30, h5py.
NOT in the trigger Worker (daily product). Pins: `model-frames-tests.yml` paths (x2), keepalive `gh workflow enable`,
README, DEPLOYMENT.md; `wrangler.jsonc` unchanged (the test asserts it is absent).
Tests `tests/model_frames/test_altgrid.py` (FakeClient of `test_job.py`; FakeWeb / listing_xml of `test_alt.py`;
`write_l4` fixture in the real layout): parse_file / dataset_re; read_l4's checks and refusals (units, scale, 358
rows, descending lat, an 03Z step, the float32 time rounding), flags and fill -> absent, the clamp; place_half row
order (lat 89.5 -> row 1, -89.5 -> row 359, poles code 0); main end to end (put order, IMMUTABLE / POINTER, the
shapes, newest_utc 18Z); heartbeat / a re-produced day under new keys / supersession grace / never regress; prune;
the stale alarm once per 12 h on scheduled runs only; failures recorded with back-off; `--local` and `--file`;
the workflow pins (crons, group, sha-pinned uses, pinned pkgs incl. h5py, keepalive, tests.yml count 2, wrangler
without it, the constants).
Step 1 real-file check: `python -I tools/model_frames/altgrid.py --day 20261103 --file <the 2026-11-05 download>
--local DIR`, a few cells compared between the dequantized PNG and the NetCDF; the dispatch dry run after the merge.

### B. Client `static_overlay/overlay.js` (+ `overlay.css`, page flag, tests)
- Constants (by the sat block): `GRID_PREFIX 'altgrid/v1'`, `GRID_FORMAT 'altgrid-v1'`, `GRID_WARN_H 48`,
  `GRID_HIDE_H 72`, `GRID_DOI '10.48670/moi-00180'`, `GRID_EXPECT {cols 720, rows 361}`, `MAX_GRID_MANIFEST_BYTES
  64 KB`; `SAT_CREDIT` title gains "gridded wave height WAVE_GLO_PHY_SWH_L4_NRT_014_003 (doi:10.48670/moi-00180)"
  (visible credit unchanged, <= 29 chars); `CLIP_FIELDS.sat = true` (field name 'sat': no lake mask, no contours,
  the coast clip, FIXED_OPACITY fallback); `satLut()` = cached `buildLut('hs', SAT_FDEF.legend)` (SatLayer uses it
  too); `FOCUS_PARTS` gets 'ov-sat-box' back.
- `GridStore(root)` modelled on `SstStore` (l.940-1013) without the tiers: `validGridPointer` (format, complete,
  8-digit run, manifest key, newest_utc), `validGridManifest` (format, run match, encoding, `grid` deep-equals the
  half grid, `fields.hs.lo/hi === SAT_FDEF` (a range change refused, never mis-coloured), 1..4 frames with ascending
  valid_utc and file keys under the prefix); `load(force)` (pointer `no-cache` <= 64 KB with a 15-s re-armed watchdog
  -> manifest -> the NEWEST frame via `decodeFrame(url, sig, GRID_EXPECT, null, false, kick)` -> `validateGrid(m.grid,
  frame, SAT_FDEF)`; unchanged manifest key = resolve in place; a failed re-read keeps the frame; cooldown
  `COAST_RETRY_MS` unless forced); `recheck()` (pointer only -> `newer`); `newestMs()`, `abortAll()` (keeps the
  frame); singleton `gridStore(root)`.
- Controller: ctor `this.grid = opts.sat && opts.grid ? gridStore(root) : null; this.satGrid = !!(this.grid &&
  s.satGrid === true)`; `_wantGrid()` (grid, satGrid, obs, field sat, layer); `setSatGrid(on)` (save `{satGrid}`,
  the box, `_syncGrid(true)`, re-render when ready); `_syncGrid(force)` (not wanted or hidden by age > 72 h ->
  `layer.clear()` + line + attribution; wanted -> 'loading…' then `grid.load(force)` -> `_showGrid`); `_showGrid(st)`
  (skip if the entry is drawn; `_coastFirst(coast.load())` as `_loadSst`; `layer.setCoast`, `setContours(null,
  true)`, `layer.setFrame(st.frame, st.grid, 'sat', SAT_FDEF, satLut(), st.entry)`; line; attribution); hooks:
  `_loadSat` (after `_attachSat`), `setSatKind` (the frame stays), `_mountObs` / `_leaveObs` (already `clear()`),
  `unmount` (`grid.abortAll()`), `_satRecheck` (+ `_gridRecheck`: a failed store re-asked, a newer manifest adopted
  in place), `_checkRun`'s sat branch (+ `_gridLineSync`: the age and the 48 / 72-h flips without a user action).
- Panel `_renderSat`: segment -> legend -> the row (the c960ce7 template: `div.ov-row.ov-satrow` > `label.ov-sat-label`
  > `input.ov-sat-box` + "Gridded map"; `span.ov-sat-note#ovGridNote` + `aria-describedby`; `change ->
  setSatGrid`; `ui.gridBox / ui.gridNote`) -> readings line (Swell wording "Readings: 24 h to <time> (<age>) · n
  satellites") -> `SAT_KEY` -> credit line `+ ', doi:10.48670/moi-00180'` while on -> the sat stale warn -> the grid
  warn "The gridded map is 52 h old." (48..72 h) or "The gridded map is 75 h old: hidden." (> 72 h, the layer
  cleared). `_gridLineText()`: '' off; 'loading…'; 'unavailable'; "CMEMS L4 · Nov 3, 1:00 PM EST (31 h ago)". CSS:
  restore `.ov-satrow / .ov-sat-label / .ov-satrow input / .ov-sat-note` and the coarse-pointer 20-px box.
- Hover (`_bindReadout` show(), l.~6204): `v = layer.readoutAt()` is the grid value when drawn (null over land /
  absent); a dot hit -> `_satReadout(hit, latlng, z, v)` appends `' · grid ' + fmt(v)` when `_wantGrid()` and v
  present (heights and swell texts alike); no hit in the sat entry -> the line that hides everything becomes: grid
  drawn and v present -> `_gridReadout(v)` = "grid 8.1 ft · Nov 3, 1:00 PM EST" (≥ at the scale's top), else hidden.
- Page: `app.py` `_grid_overlay_enabled()` (ALT_GRID=1 and `_sat_overlay_enabled()`), `_overlay_context`
  `overlay_grid`; `templates/index.html` `var GRID = {{ overlay_grid|tojson }}`, `grid: GRID` in create;
  `tests/test_overlay_flag.py` needles (`var GRID = false;`, `sat: SAT, swell: SWELL, grid: GRID`), the on/off
  comparison extended, the contract needles (`GridStore`, `'altgrid/v1'`, `setSatGrid`, `10.48670/moi-00180`), the
  template substitution; `tests/capture_index_golden.py` pops `ALT_GRID` (the golden is not recaptured: the flag-off
  page is unchanged); `tests/overlay/bootstrap.test.js` SUBS `overlay_grid` + the section-81 test's new needles.
- `_internals`: `GridStore, gridStore, validGridPointer, validGridManifest, GRID_* , satLut`.
- Tests `tests/overlay/sat.test.js` (`ctl(w, { grid: true })`; the frame via a stubbed `ov.grid._decode` or real PNG
  bytes as `sst.test.js`): GridStore (pointer / manifest / newest step; every rejection; 503 -> failed + cooldown; a
  failed re-read keeps the frame; recheck notes a newer manifest); the box (off by default, saves `satGrid`; On
  draws through `this.layer` in modelPane with field 'sat', the newest entry, after the coast; the credit line gains
  the third DOI; the frame survives the Heights|Swell switch; Off clears the layer and keeps the store's frame; the
  next On refetches nothing); a saved `satGrid` without `opts.grid` starts off with no row; the line states and the
  48-h warn / 72-h hide; the hover (dot + grid, swell point + grid, grid alone, hidden over land / absent / off);
  the 12-min recheck adopts a newer production in place; SST / a model field clears it; unmount aborts; the Swell
  line wording. Mutants by hand: the `out[1:360]` offset, the hide line, `CLIP_FIELDS.sat`, the DOI condition, the
  `satGrid` key, the `fields.hs.hi` check. Asset bump `OVERLAY_ASSET_VERSION` 2.24.0 + `tests/fixtures/overlay_assets.json`.
- README: the jobs section (altgrid) and the client section (the Gridded map box); DEPLOYMENT.md (the workflow);
  the owner's sheet `docs/plans/section-82-test-site-sheet.md` in step 4.

## Model and credits (owner, 2026-11-05)
Build with **Opus 5.5 at HIGH effort** (the job, the client, the test site, the fixes); the two review passes of
step 5 run as **Fable 5.1** subagents while credits last, else Opus. When the cloud credits run out, stop at the
nearest clean, pushed state (commit + push, the plan's progress list updated) and the work returns to the desktop.

## Steps (one cue: "Cue: select Opus 5.5 and set effort to HIGH for section 82 (the gridded satellite wave map:
job, client, test site, review, fixes)." — reviews via Fable subagents)
1. Branch `feat/gridded` off production. Job + workflow + pins + tests + README job section; `tests/model_frames`
   green; the real-file `--file --local` check; after the merge a `workflow_dispatch` dry run.
2. One fresh reviewer on the job; fixes. STOP: tag `prod-pre-altgrid-job` at production's tip (pushed as a branch
   too while the proxy refuses tags) + the owner's explicit go-ahead -> the job-only merge; the first live pointer
   and frames verified on the bucket.
3. Client + page flag + tests + mutants + asset 2.24.0 + README; the Swell line wording.
4. Test site: cherry-pick the client paths to `test` (README `--ours`); the owner sets `ALT_GRID=1` there; desktop +
   phone (360 px: the row fits, the fold rule) with headless Chromium; the owner's sheet.
5. Two fresh reviewers (code via Fable; the test site); fixes, each with an asset bump; CI green on both branches.
6. STOP: tag `prod-pre-altgrid` + the owner's explicit go-ahead -> the client merge; `ALT_GRID=1` on production with
   its own go-ahead; production verified.

## Verification
- Job: `python -I -m pytest tests/model_frames/test_altgrid.py` and the whole `tests/model_frames` suite; a real
  local build against the live bucket; the PNG dequantized at sample cells vs the NetCDF (`h5py`); the pointer /
  manifest shapes; after the merge the dry run, then `curl https://models.allshoresurf.com/altgrid/v1/latest.json`
  and the newest frame decoded.
- Client: `node --test` on `sat.test.js` + `bootstrap.test.js`, then the whole coast-build list locally (the lesson
  of coast-build #584: every suite, not just the satellite one); `python -I -m pytest tests/test_overlay_flag.py`;
  headless Chromium on the test site: the box on/off, the field under the dots and arrows at z3 / z5 / z7, the hover
  texts, the line's age, the credit line, a reload, the phone panel (no horizontal scroll), console / network clean;
  the flag-off page byte-identical.

## Risks (where the code resists)
- `validateGrid` needs rows reaching exactly ±90: the pole padding must be exactly `out[1:360]`; `decodeFrame`'s
  expect `{720, 361}` (a 359-row PNG is refused).
- The readout hide line in show() for the sat entry becomes the grid branch; "nothing over empty water" must hold
  with the box off.
- The sat entry's `ModelGridLayer` persists across mounts: `_showGrid` re-applies `setCoast` / `setContours(null)`
  before `setFrame`; `CLIP_FIELDS.sat` also lets `_coastLate` attach a late coast on an empty layer (harmless).
- The grid hover appears only once the tile's land mask exists (as SST).
- The float32 time axis: round to the hour, never compare exactly.
- Phone panel: one more row; verify the credit line and the warn still show at 360 x 640 without the fold.

## §82 progress
- 2026-11-05: planned (the owner's decisions 1-6, the model choice Opus 5.5 HIGH with Fable reviews, the credits
  rule) and approved; branch `feat/gridded` created off 584a772; nothing built. NEXT: the cue, then step 1 (the job).
- 2026-11-05 (Opus 5.5, HIGH): step 1 built: `tools/model_frames/altgrid.py` (alt.py's discovery / listing / download /
  alarms imported; `L4Product.parse_file` makes one record per day; `read_l4` with the checks; `place_half`;
  `encode_frame(fill=False)`; manifest + pointer last; prune 3 days + the 2-h supersession grace, also on quiet runs;
  failures stamped with the run's time), `model-altgrid.yml` (crons 17,47 7-10 and 17 */3; not on the Worker), the
  pins (tests.yml x2, keepalive), README job section, DEPLOYMENT note, `tests/model_frames/test_altgrid.py` (9).
  Real-file check on the 2026-11-03 file: 2,000 cells a step within half a quantum (0.045 m) of the NetCDF, exact
  latitude placement, poles absent, the blank mask = the product's valid-and-unflagged mask exactly; a live local
  build against the bucket in 8 s (production 20261104T073010Z, newest step 18Z Nov 3). model_frames suite 502 pass.
  NEXT: step 2 (one fresh Fable reviewer on the job), then STOP for the job-only merge.
- 2026-11-05: step 3 (the client, asset 2.24.0) built while the job's reviewer runs: `GridStore` / `gridStore` /
  `validGridPointer` / `validGridManifest`, `satLut()`, `CLIP_FIELDS.sat`; the controller's `_wantGrid`, `setSatGrid`,
  `_syncGrid`, `_showGrid` (the entry's ModelGridLayer, field 'sat', after the coast), `_clearGrid` (only the grid),
  `_gridRecheck` (on the 12-min satellite timer), `_gridLineText` / `_gridLineSync` (the note, the 48-h warning, the
  72-h hide), `_gridReadout` and the hover's " · grid" tail; the "Gridded map" row in `_renderSat`, the credit line's
  third DOI, the title's; the Swell line "Readings: 24 h to …"; CSS rows restored; page flag `ALT_GRID`
  (`_grid_overlay_enabled`, `var GRID`, `grid: GRID`), flag / bootstrap / golden-capture tests; README client section.
  Tests: sat.test.js 14 (3 new: the store and its rules; the box, draw, hover through the readout handler, Swell,
  Off, another entry, saved state; the ages, the recheck adopting a newer production, unavailable); the coast-build
  Node list 1,073 pass (isobars.test.js cancelled by this container's 20-s file timeout, as before). Mutants 18 of 20
  killed; the 2 survivors are behaviour-equivalent (the hover's box guard: the layer is already clear with the box
  off; the SST-source reset: every observation mount already clears it). NEXT: the job reviewer's record, fixes, then
  STOP for the job-only merge.
- 2026-11-05: step 2 review (one fresh Fable reviewer; record `docs/reviews/gridded-G53-job.md`): 0 P0, 1 P1 (`--force`
  regressed the live pointer), 4 P2 (a manual `--day` retry skipped while backed off; an all-fill production accepted;
  prune errors escaping `main`; download outages charged to the production), 4 P3 + nits. All fixed (the record's fix
  table); `test_altgrid.py` 11; 9 of the reviewer's 10 defect proofs now fail. NEXT: STOP for the job-only merge.
