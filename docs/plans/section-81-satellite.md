# Section 81: the satellite layer (plan copy for the cloud session)

Copied from the owner's local plan file on 2026-11-04 so a cloud session has it. Section 81 is the plan to build;
section 45 is its base design (job, chunk format, AltStore / AltLayer, obs mode, the Wave height checkbox, the ±3 h
rule, the hover). Section 45's line numbers are stale: step 1 starts with a fresh survey of the code at d973c76.

## Standing rules (owner)
- One model / effort cue per section, printed once: "Cue: select Fable 5.1 and set effort to HIGH for section 81
  (satellite layer: jobs, client, test site, review, fixes)." Then build, test, review and fix without stopping.
- Stop only for an owner call and before each production merge. A production merge needs a rollback tag and the
  owner's explicit go-ahead ("continue" is not a go-ahead). Two merges here: job-only (tag prod-pre-alt-job @ the
  production head) and client (tag prod-pre-alt). Env flags ALT_OVERLAY=1 / SARSWELL=1 are set on Render by the owner,
  each with its own go-ahead, test service first.
- Production branch: Live-Buoy-Update (head d973c76 at this copy). Test site: branch `test`, by cherry-pick
  (README `--ours`; test keeps its 3 logo lines and its own page golden). Never force-push. Never rebase a pushed branch.
- Commits end with "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>". Golden re-captures in their own commit.
  Bump OVERLAY_ASSET_VERSION / UI_ASSET_VERSION on every static change and every push to test.
- Use `python -I`. Test harnesses use headless Chrome with their own profile, never Edge.
- Security: never create accounts, enter passwords or keys, or read key values; keys never in code, logs, answers,
  /healthz or pages. Never disable TLS verification. No user e-mail in User-Agents.
- Data sources are read-only and paced; the bucket is written only by the GitHub jobs (secrets already in GitHub).
- Copernicus Marine: anonymous S3 access is the primary route (the owner has an account; credentials only as GitHub
  secrets entered by the owner, if ever needed). Credit "E.U. Copernicus Marine Service Information" + DOI.

## 81. Satellite layer: wave-height dots and Sentinel-1 swell arrows — PLAN (2026-11-04, awaiting approval; supersedes §74's format)

### Context
Sections 45 (2026-10-17) and 74 (2026-10-31) planned a satellite overlay, and neither was built. The owner was unsure
about parts of the format and asked for fresh options, with no earlier decision carried over. A sketch of four formats
was shown: A height dots, B model-error dots, C a blended field, D swell arrows.
The owner's decisions (2026-11-04, AskUserQuestion, all fresh):
1. **Purpose: both.** An observation map of its own, and a check of the model.
2. **Format: A + D.** Altimeter wave-height dots along the tracks, in the wave-height colours, older ones faded.
   Sentinel-1 swell arrows: direction of travel, colour by the dominant swell's height, length by its peak period.
   There are no model-error dots (B) and no blended field (C).
3. **Placement: both.**
   - Its own field-menu entry, like SST: no player, no model choice.
   - A checkbox under Wave height that draws the readings over the animated model.
4. **Window: the last 24 hours** in the layer's own entry, faded by age.
5. **Over the model: readings within 3 h of the frame's time.** Frames further ahead show none.
6. **Comparison:** the dots keep the model's own colour scale, so a dot whose colour differs from the field under it
   stands out. The hover gives both numbers and the difference ("6.2 ft · Sentinel-6A · 11:40 AM · model 5.4 ft
   (+0.8)").
7. **Own entry:** a switch, "Heights" | "Swell". One kind is shown at a time.
8. **Over the model: height dots only.** The swell arrows show only in the layer's own entry.

These match §45's design for the heights (job, chunk format, AltStore / AltLayer, obs mode, the checkbox, ±3 h, hover),
so §45's design stands for that half. What §74 added changes as follows:
- the swell points are drawn only in the layer's own entry, behind the Heights | Swell switch, never over the model;
- Jason-3 OGDR stays as the fourth height source.

### Design (deltas from §45; §45's text is the base, its line numbers are stale: step 1 starts with a fresh code survey
at the production head)
**A. Heights job** (`tools/model_frames/alt.py` + `altfmt.py`, workflow `model-alt.yml` on the trigger Worker):
- as §45, with NOAA Jason-3 OGDR as a fourth source (dropped as a duplicate of CMEMS's Jason-3 pass by mission + time);
- collocation against GFS-Wave hs frames as §45. ECMWF is compared only by the frame-mode hover, as §45.

**B. Swell job** (`tools/model_frames/sarswell.py`, its own pointer `sarswell/v1/`):
- source: CMEMS WAVE_GLO_PHY_SPC-FWK_L3_NRT_014_002 (S1A / S1C, CFOSAT when flowing), the last 24 h;
- per point: t, lat, lon, n, then n x (hs, tp, dir FROM);
- the storm-source ("fireworks") files are left out;
- the chunk binary is §45's ALT1 pattern (its own magic), with the pointer published last and prune / stale as §45;
- step 1 first checks on real files that the period is a PEAK period and that direction means FROM. A mean period
  stops the swell half (the MFWAM lesson).

**C. Client** (`static_overlay/overlay.js`):
- one field-menu entry, "Satellite waves", in obs mode, with the switch "Heights" | "Swell" in its panel (remembered
  per tab);
  - Heights = the AltLayer dots (24 h, faded);
  - Swell = a second store + layer on the same chunk decoder, drawing arrows: colour = the dominant partition's height
    on the hs scale; length from its peak period; drawn newest on top;
- hover:
  - on a height dot: the reading, mission, time, and the collocated model value with the difference;
  - on a swell point: every partition ("Swell 1: 1.8 m · 14 s · from 215° (Sentinel-1C, 6 h ago)");
- the Wave height checkbox ("Satellite readings"): height dots within ±3 h of the shown frame, in the model's colours,
  with the hover's model value read from the drawn field (§45's frame mode). No swell arrows there;
- the credit and env flags `ALT_OVERLAY=1` and `SARSWELL=1` (the Swell switch is shown only with its data live);
- the readout = the drawn-dot invariant, and Off / model switch / Update drops, as §45.

### Steps (one cue; I stop only for owner calls and before each production merge)
1. Fresh code survey (overlay.js at the production head: obs mode, SstStore, the scheduler's landing hook, the panel).
   Then the two jobs with tests and real-data checks: heights vs NetCDF, swell variables / period type / direction.
   Then a dispatch dry run for each.
2. One fresh reviewer on the jobs. Then STOP for the job-only merge (tag `prod-pre-alt-job` + the owner's go-ahead).
   The first published runs are verified in the bucket.
3. Client: stores, layers, the switch, the checkbox, hover, credit; tests + mutants; asset bump; README.
4. Test site (the owner sets the env flags there), desktop + phone:
   - tracks and arrows at several zooms, ages, hovers;
   - the checkbox while scrubbing and at shark speed (frame rate unchanged);
   - Off asks nothing;
   - a sheet for the owner.
5. G-review (two fresh reviewers: code via `model: fable` and the test site); fixes; a short re-check if the store /
   mount path changed.
6. STOP for the client production merge (tag `prod-pre-alt` + the owner's go-ahead). Then the env flags on production,
   each with its own go-ahead.

**Cue for section 81: "Cue: select Fable 5.1 and set effort to HIGH for section 81 (satellite layer: jobs, client, test
site, review, fixes)."**

### Known limitations
- Tracks, not a field: dots lie a few hundred km apart, 2-6 h behind real time.
- Coastal readings are unreliable (hollow dots).
- Sentinel-1 swell points are open-ocean, in daily batches, about 7-10 h old.
- Frames more than ~3 h past the newest reading show no dots over the model.

### §81 progress
- 2026-11-04: planned from the owner's fresh decisions (eight above). Nothing built; production @ d973c76.
- 2026-11-04: PLAN APPROVED by the owner (ExitPlanMode). NEXT: the section cue and WAIT; then branch
  `feat/satellite` off `Live-Buoy-Update` @ d973c76 in C:\Users\joshm\wave-app-job3, step 1 (fresh code survey + jobs).
- 2026-11-04: PAUSED BY THE OWNER before step 1 ("we will come back to this later"), after weighing a move to a cloud
  session (the cloud copy lacks the local plan / memory / scratch harness; credits expire in days). Nothing built; no
  branch. RESUME: re-print the section-81 cue and WAIT; if a cloud session is wanted then, use move_to_cloud with a
  summary carrying section 81's decisions.
- 2026-11-04 (cloud session, branch `feat/satellite`): step 1 code survey done; `altfmt.py` (ALT1 / SWL1 chunks),
  `alt.py` (STAC + bucket listing, the plan by inputs digest, downloads, `read_l3`, the GFS-Wave collocation, chunks,
  pointer last, prune, stale; Jason-3 OGDR behind `ALT_JASON3_INDEX`, off until verified), `sarswell.py` (the swell
  product on alt.py's machinery; the reader refuses a mean period or a "towards" direction), workflows `model-alt.yml`
  / `model-sarswell.yml`, the trigger / keepalive / tests pins, `tests/model_frames/test_alt.py` (18 tests; the whole
  model_frames suite 492 pass). BLOCKED: the session's network policy denies every data host (s3.waw3-1.cloudferro.com,
  stac.marine.copernicus.eu, documentation.marine.copernicus.eu, models.allshoresurf.com, www.ncei.noaa.gov), so the
  real-file checks (the SWH file's variables, the SPC-FWK datasets / file names / variables, the peak-period and FROM
  checks, the Jason-3 OGDR index) and the dispatch dry run are still to do; the owner was asked to allow the hosts.
  The swell dataset pattern (`_PT15S`), its file-name pattern and the variable name lists are the documented ones and
  are UNVERIFIED: nothing is published while they fail (the job then only alarms), but they must be checked on real
  data before the job-only merge.
- 2026-11-04: step 2 review (one fresh reviewer, Opus; record below in `docs/reviews/satellite-G50-jobs.md`): 1 P0 (the
  swell direction check took "towards ... clockwise from north" for FROM), 2 P1 (a superseded chunk pruned in the run that
  superseded it; a frames-host outage charged to the windows' digests for good), 11 P2, nits. All fixed: the conventions
  are read from standard_name first, the reference phrase stripped, contradictions refused; the pointer's `superseded`
  list dates the grace; the frames outage is charged to no window; the failure back-off retries every 2 h; the live run
  joins the newest windows' digest; mission silence from the pointer's `missions[].newest_utc`; `--window` failures exit
  1; STAC read every 6 h and only the wanted items; the range alarm once per 12 h; bounded inflate, stricter decode
  checks; OGDR dropped by pass coverage; failed/ records pruned; the reader's refusal in the summary. 18 job tests pass;
  NEXT: the real-data checks (blocked on the hosts), then STOP for the job-only merge (tag `prod-pre-alt-job`).
- 2026-11-04: step 3 (the client, overlay asset 2.23.0) built: `decodeSatChunk` / `inflateBytes`, `SatStore` (+ adopt
  without a flash, stall timers, the cooldown re-ask), `SatIndex`, `SatLayer` + `composeDots` / `hitAt` (dots and swell
  arrows), `viewRangeOf` shared with ModelGridLayer, `OBS_DEFS` / `hasObs` / `_mountObs(kind)` / `_loadSst` / `_loadSat`,
  `_obsShell` + `_renderSat` (the Heights | Swell switch, the readings line in place, the credit line, the stale line),
  the "Satellite readings" box under wave height (`setSatOn`, `_syncSatAttach`, `_syncSat` at `_goto`'s landing, the
  entry guard, the notes), the hover texts (own entry and frame mode), the credits; page + `app.py` (`ALT_OVERLAY`,
  `SARSWELL`), CSS, `tests/overlay/sat.test.js` (12), bootstrap / flag tests, README. Every overlay Node suite passes
  (480 + 12); `tests/test_overlay_flag.py` passes but for its Node-runner test, which fails here only because
  `isobars.test.js` exceeds the 20-s file timeout on this slow container (checked at d973c76 too: pre-existing here, not
  on CI). NEXT: mutants, then the test-site cherry-pick (step 4) once the owner sets the flags on Render.
- 2026-11-04: step 5 reviewer A (code, Fable; record `docs/reviews/satellite-G51-client.md`): 1 P0 (the hover took the
  nearest dot centre, not the top-most drawn), 3 P1 (Off during a re-read latched the store; a deferred adoption left
  the own window behind; the credit one landing behind), 9 P2, nits: all fixed (asset 2.23.1); 12 Node tests + the
  review's cases; 20 of 22 mutants caught (two behaviour-equivalent). Reviewer B (the test site) waits for the data and the flags. NEXT: the
  owner allows the data hosts -> the real-file checks and the dispatch dry runs -> STOP for the job-only merge.
- 2026-11-04: step 1b real-data checks done (the owner allowed the hosts). SWH: the files are exactly as `read_l3`
  expects (token `s6a_lr` etc.). SPC-FWK: the real layout differed from the documented one and the swell job was
  rewritten for it: observation files `dataset-wav-sar-l3-spc-nrt-global-<sat>_<t0>Z_<t1>Z_P<prod YYYYMMDDTHHMM>Z-3H-nrt.nc`
  (the fireworks files in the same folder never match), the group `obs_params` with one row per partition (`VAVH`,
  `VTPK` = PEAK by its CF name, `VPED` = FROM by its CF name and its description, time in days since 1950, the
  product's `L2_partition_quality_flag`: very_good / good / medium kept), rows grouped into points; the 014_002 DOI is
  10.48670/moi-00178 (fixed in the job, the client's credits, the README; asset 2.23.2). One batch a day lands ~23Z
  covering the 36 h up to ~18Z, so the swell product keeps 56 h (19 windows; MAX_CHUNKS / MAX_SAT_CHUNKS 20): the 24 h
  before its newest point stay in reach (OWNER TO CONFIRM: the "last 24 h" of swell are 5-31 h behind the clock).
  Jason-3 OGDR: verified against CMEMS's file of the same pass (3594 readings match to the second, median 3 mm), the
  quality flag is `swh_ocean_compression_qual`, rain / ice flags applied, NOAA's directory of cycles discovered
  (`JASON3_INDEX` default, ON by default; `ALT_JASON3_INDEX` overrides, `off` disables): no repository variable needed.
  Real local builds of both jobs against the live sources: heights 10 windows / 433k readings / 61 s, obs - model
  median +0.13..0.17 m; swell 154 points, chunks read back equal to the NetCDF. 18 job tests pass (fdc5fa5).
- 2026-11-04: the dispatch dry run cannot run before the merge: GitHub answers 404 to a workflow_dispatch of a
  workflow that exists only on a feature branch (it is registered from the default branch), so it is the first thing
  after the job-only merge (Actions -> model-alt / model-sarswell -> Run workflow, dry_run = true; then a real run).
  The job-only merge is prepared as branch `feat/satellite-jobs` @ 60102d7 off `Live-Buoy-Update` @ d973c76 (19
  files: the jobs, workflows, pins, tests, the README's job section, the G50 record, this plan; nothing on the page;
  293 model_frames tests pass on it). STOPPED for the owner's go-ahead: on it, tag `prod-pre-alt-job` at d973c76,
  merge `feat/satellite-jobs` into `Live-Buoy-Update` (no force, no rebase), push, dispatch the dry run, watch the
  first live runs (`alt/v1/latest.json`, `sarswell/v1/latest.json`), then step 4 (the test site: cherry-pick
  `feat/satellite`'s client commits to `test`; the owner sets `ALT_OVERLAY=1` / `SARSWELL=1` on the test service).
- 2026-11-04 22:55Z: JOB-ONLY MERGE DONE on the owner's explicit go-ahead ("merge the jobs to production"):
  `feat/satellite-jobs` @ 60102d7 merged into `Live-Buoy-Update` (merge 8cd6eba, no force, no rebase, the tree equal
  to the branch). The rollback point is d973c76: the tag `prod-pre-alt-job` exists locally but the session's git proxy
  refuses tag pushes ("the remote end hung up") and the API proxy refuses ref writes, so the same point is on the remote
  as BRANCH `prod-pre-alt-job` (d973c76); the owner can add the tag from their machine (`git tag prod-pre-alt-job
  d973c76 && git push origin prod-pre-alt-job`). Dispatch dry runs on the runner, both success: model-alt #1 (10
  windows, 437,591 readings, 11 datasets, 31 frames, OGDR duplicates dropped) and model-sarswell #1 (10 windows in the
  56-h reach, 2,739 points, Sentinel-1C + CFOSAT, 24 s). The trigger Worker still needs the owner's deploy for the
  10-minute dispatches (`wrangler.jsonc` GH_WORKFLOW gained both; DEPLOYMENT.md); until then the crons (:07/:37,
  :09/:39) run the jobs every 30 min. NEXT: watch the first live pointers land, then step 4.
- 2026-11-04 23:01Z: step 4, the test site: `test` @ 81f13b6 carries the client (the client paths of efb6cd0,
  e6a9495, 5ed656e, b644b7e, fdc5fa5 as one commit, since the first jobs commit also held 892 client lines; README
  kept as test's, test's logo lines kept, its own golden kept; coast-build runs sat.test.js). On the worktree the 12 +
  27 Node tests pass; `tests/test_overlay_flag.py` has the same 5 failures on test's own tip (its golden is stale
  there since section 77: pre-existing, not from this change). The owner's sheet: `section-81-test-site-sheet.md`
  (set `ALT_OVERLAY=1` then `SARSWELL=1` on the TEST service, each with its own go-ahead; the checks).
- 2026-11-04 23:07Z: BOTH PRODUCTS LIVE on the bucket, published by runs #2 of each workflow (dispatched at 23:06
  by the trigger Worker: it already dispatches both). `alt/v1/latest.json`: build 20261104T230624Z, 10 chunks
  20261103T15..20261104T18 (30-48k readings each), 9 missions, newest 20:46Z, model runs 12/06/00/18Z, max-age 300;
  `sarswell/v1/latest.json`: 9 chunks 20261102T15..20261103T15 (56-524 points), keep_h 56, Sentinel-1C + CFOSAT,
  newest 2026-11-03 16:04Z. The newest chunk of each read back from the bucket decodes with n equal to its record
  (heights obs - model median +0.135 m; the swell header carries the quality rule).
- 2026-11-04 23:51Z: the owner set `ALT_OVERLAY=1` on the TEST service (wave-app-clean.onrender.com serves
  `var SAT = true`, `SWELL = false`, asset 2.23.2). My own pass with headless Chromium (scratch `satcheck.js`,
  `satcheck2.js`; screenshots sent to the owner): the entry lists "Satellite waves"; the own entry loads 339,946
  readings (complete) with the panel "Readings: last 24 h, newest Nov 4, 04:51 PM EST (2 h ago) · 9 satellites" and
  the credit line; the tracks draw at z3 / z5 / z7 in the scale's colours, hollow near land; hover "8.6 ft · SARAL ·
  Nov 4, 11:19 AM EST · model 8.0 ft (+0.6)" and "14.0 ft · Sentinel-3B · 02:50 PM EST · model 13.9 ft (+0.1)"; the map
  credit "Overlay: Copernicus Marine"; the box under Wave height draws 1,854 readings within 3 h of the frame with the
  note "within 3 h of the frame" and the credit gains " · Copernicus Marine"; peak period detaches the layer; a reload
  keeps the box on; the phone layout folds to the panel at the bottom with the dots drawn; no console errors on either
  viewport. Reviewer B (fresh agent, the test site) started: record to be `docs/reviews/satellite-G52-test-site.md`.
  WAITING on the owner's separate go-ahead for `SARSWELL=1` on the TEST service (the Swell switch).
- 2026-11-05 00:02Z: the owner set `SARSWELL=1` on the TEST service. My pass (scratch `swellcheck.js`, `probe.js`;
  screenshots sent): the Heights | Swell switch shows with aria-pressed; Swell loads 1,962 points (17 chunks on the
  bucket now: the Nov 4 batch landed at ~23:17Z), the arrows draw at z3 / z5 with colour by height and length by period,
  hover "Swell 1: 6.6 ft · 15 s · from 304° (CFOSAT, 9 h ago)", the credit line carries both DOIs, the title too; the
  kind is kept across a reload and across a visit to Wave height; no console errors, no failed requests, on either
  viewport. Two observations, both the data, not bugs: (1) the readings line says "1 satellite" because the swell
  window hangs off the NEWEST point (CFOSAT, Nov 4 17:23Z) while Sentinel-1C's newest is Nov 3 16:04Z, outside the
  24 h: when one satellite's batch lags the other's, the window shows one satellite (OWNER: widen the swell window to
  48 h, or hang it off the clock, if that reads wrong); (2) at a frame whose 3-h window lies beyond the newest height
  reading (01Z with readings to 21:51Z) the box says "no readings within 3 h of this time", as designed. Reviewer B
  told about the switch. WAITING: reviewer B's record.
- 2026-11-05 00:35Z: reviewer B's record `docs/reviews/satellite-G52-test-site.md`: 0 P0, 1 P1 (a `model_edge`
  reading's hover showed the under-ice model value as a +10 ft miss), 3 P2 (hollow dots unexplained; "9 satellites"
  counting Jason-3 twice; a plain tap shows nothing on the phone), 5 P3 (the difference computed before rounding; a
  persistent 503 / a 404 unnamed on the readings line; the hollow rings beside the station rings; the swell line's
  "last 24 h"). Fix round (asset 2.23.3): 1-7 fixed as the record's fix table says (edge / near land in the hover, the
  key line, spacecraft counted once, the difference of the printed values, retrying / missing windows named, a tap on
  a mark shows its readout for 4 s); 8 and 9 left to the owner. 12 Node tests pass with the new cases. NEXT: cherry-pick
  the fix round to `test`, the owner's look, then STOP for the client merge (tag `prod-pre-alt`).
- 2026-11-05 00:40Z: the fix round is on `feat/satellite` @ bfae515 and on `test` @ ce6afe2 (the client paths; asset
  2.23.3 served once Render deploys). STEP 6 PREPARED AND STOPPED: the client merge `feat/satellite` -> `Live-Buoy-Update`
  is built locally as merge commit 1de5702 (README and the plan copy taken from the branch, supersets of production's;
  the merged tree equals the branch's tree exactly; 15 files, +2122 / -57 against production: the client, the page flags,
  the tests, the README's client section). On the owner's explicit go-ahead: the rollback point is production's tip
  8cd6eba (tag `prod-pre-alt`, pushed as a branch too if the proxy still refuses tags), fast-forward `Live-Buoy-Update`
  to 1de5702, push; nothing shows on production until the owner sets `ALT_OVERLAY=1` / `SARSWELL=1` there, each with
  its own go-ahead.
- 2026-11-05 00:50Z: coast-build #584 (feat/satellite @ bfae515) and #585 (test @ ce6afe2) FAILED: the fix round's
  readout flag used `el.removeAttribute('data-sat')`, which the lake / playback test worlds' fake elements lack
  (`TypeError: el.removeAttribute is not a function` in lake.test.js:697 and playback.test.js:3683 / 3721); sat.test.js's
  sandbox has the attribute methods, so the satellite suite alone did not catch it. Fixed as a closure flag `satShown`
  (asset 2.23.4); the whole coast-build list run locally before the push this time. The prepared merge 1de5702 is
  superseded: rebuilt from the branch's tip at the go-ahead.
- 2026-11-05 00:50Z: coast-build #586 / #587 green on the fix (a16206f / 4708b04).
- 2026-11-05 01:00Z: OWNER: "the satellite reading overlays on the wave heights aren't very helpful, since it only
  covers a few frames": the "Satellite readings" box under Wave height REMOVED (asset 2.23.5): `setSatOn`,
  `_wantSatFrame`, `_syncSatAttach`, `_syncSat`, `_clearSatWindow`, the notes, the frame-mode hover and credit, the
  row and its CSS, the saved `sat` state, the box's test; the own entry is untouched (decision 6 of the design is
  withdrawn; the G51 / G52 records keep their history). The owner also asked for an assessment of a full
  interpolated wave-height map from the readings, on / off inside the Satellite waves entry: below, "Assessment".

- 2026-11-05 01:05Z: CLIENT MERGE DONE on the owner's explicit go-ahead ("merge to production"): coast-build #588
  (feat/satellite @ c960ce7) and #589 (test @ 0a18647) green; the test site at asset 2.23.5 checked once more (no box
  under Wave height, the own entry 342,383 readings, peak period detaches the layer, no errors); rollback point
  8cd6eba on the remote as BRANCH `prod-pre-alt` (the proxy still refuses tag pushes; the tag exists locally: the
  owner can push it from their machine); `feat/satellite` @ c960ce7 merged into `Live-Buoy-Update` as 584a772 (no
  force, no rebase; README and the plan copy from the branch; the merged tree equals the branch's). Nothing shows on
  production until the owner sets `ALT_OVERLAY=1` and `SARSWELL=1` on the PRODUCTION service, each with its own
  go-ahead. Section 81 is complete but for those flags; the gridded map is proposed as section 82 (the assessment).


## Assessment (2026-11-05): a full interpolated wave-height map from the satellite readings, on / off in "Satellite waves"

**What the readings can and cannot give.** Nine altimeters measure 1 Hz along their tracks (a reading every ~7 km along
a line a few km wide). In 24 h the tracks lie roughly 1.5-3 degrees apart at mid-latitudes and nothing is measured
between them; a swell field moves ~1,000 km a day, so a map stitched from 24 h of tracks mixes times as well as
places. Every "full map" from these readings is therefore an interpolation that is mostly guesswork between tracks,
and a smooth picture hides that. It can still be useful as context (where the big seas are, how the model compares
over a basin), not as a nowcast to read a spot from.

**Option A (recommended): the Copernicus gridded product, not our own interpolation.** CMEMS publishes exactly this
map, made properly: `WAVE_GLO_PHY_SWH_L4_NRT_014_003`, dataset `cmems_obs-wave_glo_phy-swh_nrt_multi-l4-0.5deg_PT6H-i`
= `VAVH_INST`, "an estimate of the instantaneous wave field every 6 h at 0.5 degrees, using all available level-3
along-track measurements and accounting for their spatial and temporal proximity", with a flag field; and a 2-degree
daily mean beside it. Checked on the bucket on 2026-11-05: anonymous under `native/WAVE_GLO_PHY_SWH_L4_NRT_014_003/`,
one ~9 MB NetCDF a day holding the four steps 00 / 06 / 12 / 18Z of the day before (`..._20261103T120000Z_
P20261104T073010Z.nc`, produced 07:30Z the next day), int16 x 0.001 m on a 720 x 359 grid, 55 % of the cells valid
(open ocean; blank within ~50 km of coasts and under ice). So the newest step is 13-37 h behind the clock. Same
licence and credit as the other CMEMS products (its own DOI in the credit line).
  - Job: a third product on `alt.py`'s machinery (download the daily file, one PNG frame per step in the frames'
    encoding on its own 0.5-degree grid, a manifest naming the steps and their production time, `altgrid/v1/`, prune
    and stale as the others): about a day, the GLWU job being the precedent for a layer on its own grid.
  - Client: a "Gridded map" toggle in the Satellite waves panel (off by default, remembered per tab) that draws the
    step nearest the window's reference under the dots / arrows through the existing `ModelGridLayer` (it already draws
    a non-model grid for the lakes), with the step's valid time and age in the panel ("CMEMS gridded map · Nov 3,
    18:00 UTC (31 h ago)") and the credit; a hover gives the gridded value beside the reading's: about a day, plus a
    review round, the test site and the two merges.
  - Limits: a day old, 55-km cells, nothing within ~50 km of the coast (so no use near a break), four steps a day.

**Option B: our own interpolation from the readings in hand.** The heights job has every reading of the last 30 h,
the newest 1-2 h old; a Gaussian-weighted space-time average on a 0.5-degree grid (scales ~300 km / 12 h), blank
where no reading lies within ~400 km / 12 h, with an age field, is 1-2 days of job work and the same client work.
It would be fresher than CMEMS's by a day, but it is an unvalidated product: streaks along the tracks, false
smoothness between them, no error estimate, and the owner's name on it. A sounder variant interpolates the
obs - model differences onto the GFS-Wave field (a "model corrected by the satellites" map), which is a real
design in its own right (section 45's discrepancy view, reworked) and not a quick addition.

**Advice.** Build option A as a separate section (82) after section 81's client merge: it answers the question
("a full wave-height map from the satellites, on or off") with an official, credited product for about two days of
work, and its one-day lag is the honest state of the art for a gridded satellite field. Revisit option B only if the
lag proves unacceptable in use; do not ship a home-made full map before that.


---

# Base design: section 45 (2026-10-17)

## 45. Wave Height Altimetry: satellite wave-height readings and the model-discrepancy view — PLAN (2026-10-17, awaiting approval)

### Context
Owner (2026-10-17): a fifth overlay option, "Wave Height Altimetry", showing the latest satellite altimeter significant wave
height readings on the wave-height colour scale, like the SST observation layer; and a way to lay the readings over the
wave-height forecast at the corresponding time to show where a swell runs ahead of or behind the model. Owner decisions
(2026-10-17, AskUserQuestion): **last 24 h, faded by age**; the discrepancy view colours the readings on the **same scale as
the model** (matching colours = agreement; the hover gives the numbers); readings **within ±3 h of the shown frame**; **one
release** for both parts.

### Feasibility: yes, at $0, with no new account in practice
- **Source: Copernicus Marine `WAVE_GLO_PHY_SWH_L3_NRT_014_001`**, 1 Hz along-track SWH (one reading every ~7 km along each
  satellite's track), 8 missions live (Sentinel-6A, Jason-3, Sentinel-3A/B, SARAL, CryoSat-2, CFOSAT, SWOT nadir; HY-2B 1–2
  days late, HY-2C frozen, Sentinel-6B joins 2026-11-24), buoy-calibrated and cross-calibrated, EMD-filtered (`VAVH`). Files
  per mission per 3-hour window (00, 03 … 21 UTC), 100–215 KB (~7,000 readings), regenerated every 30 min while data arrives
  (the older version purged), appearing 1–4 h after the window; the newest reading is typically 2–6 h old. ~10 MB and
  ~350,000 readings a day. **Anonymous HTTPS on the same open bucket host the site already reads European buoys from**
  (`s3.waw3-1.cloudferro.com`, bucket `mdl-native-05`; listing and GET verified without credentials; the bucket/prefix per
  mission comes from the STAC metadata). NetCDF-4 = HDF5: h5py (the SST job's reader). Licence: free, "any purpose",
  commercial allowed; credit "Generated using E.U. Copernicus Marine Service Information" + DOI 10.48670/moi-00179 visible on
  the page. The licence assumes an account: the owner registers a free one and keeps the `copernicusmarine` toolbox as the
  fallback should anonymous access close (credentials as GitHub secrets, entered by the owner).
- Rejected: NASA PO.DAAC / EUMETSAT (accounts, raw 1.5–12 MB L2 granules, uncalibrated, ~1 h fresher only), NOAA STAR LSA
  daily files (no account but ~1 day late, uncalibrated; kept on record as a backup source), the gridded L4 products (2°
  daily or 0.5° 6-hourly, ~1.5 days behind: no better than the model's analysis).
- **What the picture is:** tracks, not a field. In 24 h the tracks are a few hundred km apart at low latitudes (~15 % of
  0.25° cells touched; ~85 % of 2° cells); the newest 3-h window holds 1/8 of that. Accuracy ~0.3–0.5 m RMS vs buoys (bias
  ≤ ~0.2 m); readings within ~10–20 km of land are unreliable (the product does not exclude them). Convention (Stormsurf):
  colour the readings on the model's scale, show those within ±3 h of each frame.

### Design

**A. Job `tools/model_frames/alt.py` + `altfmt.py`** (copies `sst.py`'s shape; `publish.py`, `encode.py`, `sst.py` untouched)
- Missions from STAC each run (`product.stac.json` → dataset ids `cmems_obs-wave_glo_phy-swh_nrt_<m>-l3_PT1S_*`, each
  dataset's `assets.native.href` = bucket + prefix; cached in the pointer; module `DEFAULT_DATASETS` as the last fallback).
- Listing per mission (`?list-type=2&prefix=<native>/YYYY/MM/&start-after=…`, namespace-agnostic XML as `fetch.py`), file
  regex `global_vavh_l3_rt_(\w+?)_(\d{8}T\d{6})_(\d{8}T\d{6})_(\d{8}T\d{6})\.nc$` (tokens `s6a_lr`, `swot`), newest
  production per (mission, window). Windows with `t1 ≥ now − KEEP_H` (30 h = 24 h + the ±3 h margin + slack) are rebuilt when
  their inputs digest (sorted `file:bytes` + `FORMAT_KEY`) differs from the pointer's; nothing changed → the pointer is
  rewritten with `checked_utc` only (heartbeat). Downloads 8 at a time (4 tries, size = listing); a file that fails is dropped
  from this build and recorded (`alt/v1/failed/<WINDOW>.json`, back-off after 3 attempts on the same digest).
- `read_l3`: h5py; verify `VAVH` scale 0.001 / fill −32767, lat/lon scale 1e-6, time units "seconds since 2000-01-01"; keep
  0 ≤ swh ≤ 30 m inside the window ± 60 s; lon → −180..180.
- **Collocation** (`Frames`, HTTPS from `models.allshoresurf.com`, in-process cache): the GFS-Wave hs frames; per reading the
  NEWEST run whose cycle ≤ t (candidates R0, −6, −12, −18 h; a 404 frame → the next older run), linear in time between the
  bracketing hourly frames, bilinear over the present 0.25° nodes in code space with the client's `sampleRow` absent rule
  (w < 0.25 → none), `E.dequantize`; `model_edge` when a neighbour was absent; `near_land` from `fill_allow.png` (within one
  0.25° cell of GSHHG land). The manifest's hs lo/hi must equal `encode.FIELDS["hs"]` (else exit 1: a range change needs a
  deliberate alt change). ECMWF not collocated in v1 (the frame-mode hover samples whatever frame is shown).
- **Product `alt/v1`** (all keys immutable but the pointer):
  ```
  alt/v1/w/<WINDOW YYYYMMDDTHH>/<BUILD YYYYMMDDTHHMMSSZ>.bin   one 3-h window, every mission; a rebuild = a new key
  alt/v1/stats/<BUILD>.json · alt/v1/failed/<WINDOW>.json · alt/v1/stale-alarm.json · alt/v1/latest.json (max-age 300, LAST)
  ```
  Pointer: `{format "alt-v1", build, published_utc (last chunk change), checked_utc (every run), newest_utc, window_s 10800,
  keep_h, encoding "deflate", missions [{id, short, name, dataset, newest_utc}], chunks [{key, window, t0, t1, n, bytes,
  build, missions, newest_utc, inputs}] (ascending, disjoint, ≤ 12), model {name, hs [lo, hi], runs}, columns, stats, source
  {name, doi, credit}}`.
  Chunk `.bin`: `"ALT1"` · u32 header length · header JSON (`{format, window, t0, t1, build, n, columns, missions, segments
  [[mission index, start, count] … time-sorted inside a segment], runs, model {field, lo, hi, rule}, inputs, credit}`) ·
  ONE zlib stream (RFC 1950 → `DecompressionStream('deflate')`) of the columns concatenated, little-endian:
  `t u32` (s since 2000-01-01) · `lat i32` ×1e-6 · `lon i32` ×1e-6 (−180..180) · `swh u16` ×0.001 m (65535 none) ·
  `model u16` ×0.001 m (65535 none) · `mission u8` (index) · `run u8` (index into `runs`, 255 none) · `flags u8`
  (near_land 1, model_edge 2). 19 B per reading → ~44k readings per window ≈ 0.85 MB raw, ~0.5 MB zlib; 24 h + margin =
  10 chunks ≈ 5–6 MB compressed, every object ≪ 2 MB (the job refuses a chunk > 2 MB, warns at 1 MB). No thinning column:
  the client thins by stride within a mission segment. (Reconciled from the two designs: i32 positions, not i16, so a dot
  sits on its track at every zoom; `65535` = none for both heights; no separate `coast`/`rank` columns.)
- Prune: windows older than KEEP_H + 6 h; within kept windows, builds not named by the pointer and older than 2 h; failed
  records of pruned windows; stats not named and older than 24 h. Stale: newest reading > 12 h old → summary `STALE:` +
  exit 1 at most once per 12 h (`stale-alarm.json`); a mission > 48 h silent → a summary WARNING (96 h for HY-2B).
- Cadence: `.github/workflows/model-alt.yml` (the SST pins incl. h5py; cron `7,37 * * * *` as the fallback; concurrency
  `model-alt`; timeout 30; inputs window / dry_run / force) **added to the trigger Worker's `GH_WORKFLOW`**
  (`tools/model_frames/trigger/wrangler.jsonc`, DEPLOYMENT.md; the Worker builds from the production branch on merge) and to
  the keepalive and the tests workflow's paths. A quiet run ≈ 5–10 s, a typical run 20–40 s, the first cold run 1–2 min.
- Backup source (NOAA STAR LSA, ~1 day late, uncalibrated, has Sentinel-6B) and the credentialed toolbox fallback: on
  record as a second `Source` object; not in this release.

**B. Client (`static_overlay/overlay.js`, asset 2.19.0; SST behaviour byte-identical)**
- Constants beside the SST block: `ALT_PREFIX 'alt/v1'`, `ALT_WINDOW_MS 24 h`, `ALT_FRAME_WINDOW_MS 3 h`, `ALT_RECHECK_MS
  12 min`, `ALT_STALE_H 12`, `ALT_FDEF {lo 0, hi 22.86, legend [0, 18.288]}` (= SCALES.hs), `ALT_HEAD 'Satellite altimetry
  (CMEMS, last 24 h)'`, `ALT_ATTRIBUTION` "Overlay: Copernicus Marine" (26 chars; the full credit + DOI 10.48670/moi-00179
  in the link title AND as a line in the panel, which the licence requires to be visible), `ALT_FADE [1, 0.35]`,
  `MAX_ALT_CHUNKS 12`, `MAX_ALT_CHUNK_BYTES 2 MB`, `MAX_ALT_SAMPLES 600k`, `MAX_ALT_INFLIGHT 3`, `ALT_BUCKET_DEG 0.5`.
- **`AltStore`** (after `SstStore`; `altStore(root)` one per root): `load(force)` = the pointer (no-cache, the SST state
  machine: watchdog per step, cooldown, never rejects); `ensure(t0, t1, ref)` queues the chunks overlapping the window (nearest
  to `ref` first, ≤ 3 in flight, per-chunk stall timers, 404/decode permanent, else cooldown + retry) through `readBody` →
  `inflateBytes` → `decodeAltChunk` (strict: magic, lengths, ranges, times non-decreasing per segment); a newer pointer
  (`recheck()` every 12 min while attached) becomes `next` and is adopted only when the chunks the current window needs
  have landed (no empty picture in between); `_rebuild()` (one task per burst) concatenates the loaded chunks into a flat
  time-sorted store (`t Float64 ms, lat/lon/swh/model Float32 (NaN = none), mission/flags/seg u8`) + `AltIndex` (CSR over
  0.5° buckets, counting-sorted so each bucket is time-ascending; `collect(box, i0, i1, stride)` walks buckets or the time
  slice, whichever is smaller, and sorts the result; `nearest(lat, lon, z, rPx, i0, i1, stride)` ties to the newest);
  `range/count/complete(t0, t1)`; `idle()` (stops downloads, keeps samples), `abortAll()`. ≤ ~10 MB flat; no LRU.
- **`AltLayer`** = a second `L.GridLayer` in a new pane `obsPane` (z 260: above the field tiles, below the particles):
  per-tile canvases (world copies, keepBuffer, the visible-first/buffer-later split: `ModelGridLayer._viewRange` becomes the
  shared `viewRangeOf(layer)`), `setStore`, `setWindow({t0, t1, ref, span})`, `count()`, `complete()`. Pure, Node-tested
  helpers: `altStrideFor(z)` (every 2^(7−z)-th reading of a segment, z ≥ 7 all), `altRadius(z)` (1.5 → 3 px), `altAlpha(frac)`
  (1.0 newest → 0.35 at the window's far end; frame mode: |t − ref| / 3 h), `dotStamp(r)` (supersampled disc + a dark rim),
  `composeDots(data, coords, flat, idx, n, win, lut, legend, stamp)`: colour index `round((swh − L0) × 255 / (L1 − L0))` =
  `composeTile`'s expression, so a dot is `buildLut('hs', [0, 18.288])` at the observed height exactly as the field would
  draw that value; drawn in time order (newest on top); `near_land` readings hollow (rim only). An empty→empty tile touches
  nothing (most tiles in frame mode). `hitAt(lat, lng, z)` → the top-most drawn reading within `altRadius + 3` px, or −1:
  **hover = what is drawn** by construction (same window, stride and order).
- **Obs mode generalised** (smallest change): `OBS_DEFS {sst, alt}` + `hasObs(f)`; `mount(f)` → `_mountObs(kind, restore)`
  whose prologue is today's code with `'sst'` → `kind` (the carry, pause, save, pane, abort, layer clear, particles off,
  credit, look) then `_loadSst(sig)` (today's lines verbatim) or `_loadAlt(sig)` (store load → `_attachAlt()` → `setStore`
  → `setWindow(newest − 24 h … newest)` → ready). Leaving SST for alt releases SST's cells (G31-9); alt → model: `_leaveObs`
  + `_syncAltAttach()`. `_renderAlt` reuses `_renderObs`'s fold shell (factored into `_obsShell`): head `ALT_HEAD`; details:
  `buildLegend('hs', ALT_FDEF, unit)` (0 3 6 10 15 20 30 60+ ft), "Readings: last 24 h, newest 11:40 AM HST (2 h ago) ·
  7 satellites" ("loading 3 of 8…" until complete), the credit line, a warning past 12 h, the debug pre; folded
  "Altimetry · newest 11:40 AM". `_checkRun` on alt re-renders across the 12-h line; `_persist` unchanged (no time saved in
  obs); `_attribution` per `OBS_DEFS`; `unmount` aborts the store and detaches the layer. Readout (`_bindReadout.show`): an
  `altLayer.hitAt` hit first → "6.2 ft · Sentinel-6A · 11:40 AM HST · model 5.4 ft (+0.8)" (the chunk's collocated value;
  "model —" when none); no hit in alt mode → nothing; touch long-press unchanged.
- **The discrepancy toggle** (wave height only, both models): a checkbox row "Altimetry readings" + a muted note in the hs
  panel (built with `mk()`; `ui.altBox/altNote`, in `FOCUS_PARTS`); `setAltOn(on)` saves `{alt}` (off by default, remembered
  per tab); `_syncAltAttach()` attaches the layer in frame mode when `altOn && mode === 'model' && field === 'hs'` (store
  load + `setStore`), detaches on tp/wind/Off (`alt.idle()`: no requests, samples kept). `_syncAlt(idx)` at `_goto`'s landing
  (beside `_syncFlow`): the window `[valid − 3 h, valid + 3 h]` of the SHOWN entry, cleared when `layer.entry !== entry`
  (never old dots under a new time); `_dropRun` / `_fail` clear it. Note text by state: "within 3 h of the frame" / "loading
  readings…" / "readings unavailable" / "no readings within 3 h of this time" / "not supported in this browser" (no
  DecompressionStream: the box disabled). Hover with the toggle on: a hit → "obs 6.2 ft · 11:40 AM HST · model (frame
  12:00 PM) 5.4 ft (+0.8)" with the model value from `layer.readoutAt` (the field's own drawn-pixel sampler), else the
  field's readout (readout = drawn pixel holds for the field). ECMWF: the same by valid time. Readings before the run's
  f000 − 3 h never appear in frame mode (no frame for them); the standalone layer shows them.
- **Page + server:** env `ALT_OVERLAY=1` → `overlay_alt` → `var ALT` → the option "Wave Height Altimetry", `FIELDS.alt`,
  `create({…, alt: ALT})` (the SST pattern; flag-off golden unchanged); `app.py _alt_overlay_enabled`; CSS `.ov-alt` row /
  `.ov-readout-alt`; `OVERLAY_ASSET_VERSION` 2.19.0 + the sha pin; `tests/capture_index_golden.py` pops the env; README
  "Wave Height Altimetry" section.

### Files
- NEW `tools/model_frames/alt.py`, `tools/model_frames/altfmt.py`, `.github/workflows/model-alt.yml`,
  `tests/model_frames/test_alt.py`, `tests/overlay/alt.test.js`, `tests/overlay/alt_timers.test.js`.
- `tools/model_frames/trigger/wrangler.jsonc` (+ DEPLOYMENT.md), `.github/workflows/model-frames-keepalive.yml`,
  `.github/workflows/model-frames-tests.yml`, `tests/model_frames/test_sst.py:794` (the wrangler pin), `README.md`.
- `static_overlay/overlay.js` (constants after 157; `AltStore`/`AltIndex`/`decodeAltChunk`/`inflateBytes` after 1155;
  `viewRangeOf` + `AltLayer` after 1784; the controller edits at 2557, 2625, 2646-2652, 2677, 2701-2754, 2579, 2971, 2891,
  3055, 3230, 3329, 2904, 3514-3525, 3941-3956, 4010, 4098-4136, 4153, 4196), `static_overlay/overlay.css`,
  `templates/index.html` (gated block 3911-3999), `app.py` (287, 314-318, 2969-2978), `tests/fixtures/overlay_assets.json`,
  `tests/test_overlay_flag.py`, `tests/overlay/{panel,playback,bootstrap}.test.js`, `.github/workflows/coast-build.yml`
  (the two new Node files), `tests/capture_index_golden.py`.
- Record `docs/reviews/overlays-G32-adversarial.md` (G32a job, G32 client).

### Tests
- Job (pytest, offline): STAC parsing (incl. s6b, a non-wave dataset); the listing parser (namespace present/absent, the
  `s6a_lr`/`swot` tokens, two productions of one window, a partial file); the plan (digest equality, `--force`, a failed
  record, a listing outage carries the old chunk over); `read_l3` on an h5py file in tmp (scales, fill rows, window edges,
  lon 0–360, a wrong scale refused); collocation vs a hand-computed value (two synthetic frames with a gradient; absent
  neighbour → model_edge; none; a reading before the oldest run; a 404 frame → the older run); chunk round trip + the
  limits (magic, truncated stream, oversize claim); `main` publishes the pointer last and only changed windows (FakeClient;
  heartbeat vs published_utc); prune; stale once per 12 h and never with `--window`; a local build never touches the
  bucket; the workflow/trigger/keepalive/tests-path pins and the constants.
- Client (Node): `decodeAltChunk` round trip (`node:zlib`) + every rejection; `validAltPointer`; the store with held
  fetches (window queueing nearest-first, ≤ 3 in flight, 404 final / 503 cooldown, a 15-s stall frees the slot, a newer
  pointer adopted without an empty revision, `abortAll` keeps the flat store, `idle` stops the queue); `AltIndex` bucket
  walk = slice scan (date line, poles), `nearest` ties to the newest; `composeDots` determinism, the colour pin against
  `composeTile` of a one-pixel frame, age alpha endpoints, hollow coast, newest on top, world-copy identity; `AltLayer`
  split draw + empty→empty untouched + `hitAt` = the top-most drawn; the controller (`mount('alt')`: mode obs, layer
  attached, no particles, credit, no time saved, the carry kept; alt → hs at the carried time playing; alt ↔ sst; Off;
  failure → Retry; `_checkRun`); frame mode (one `setWindow` per landing through play at 4x on fake timers; tp detaches;
  Off → no `/alt/` fetches; reload restores `altOn`; the note strings; `count() === 0` past newest + 3 h); the panel
  (head, ticks, readings line, stale, folded, no player; the toggle row only for hs and only with `alt`); bootstrap + flag
  needles; every SST test unchanged. Mutants: `round`→`floor` in the colour index; alpha endpoints swapped; stride `>=`/`>`;
  draw order reversed; window bounds; `hitAt` without +3; `_syncAlt` on `frameIndex` not `idx`; the `entry` guard dropped;
  `_adopt` before the chunks land; `save({alt})` omitted; `_detachAlt` leaving the timer; `hasObs('constructor')`.

### Steps, model and effort (I print the cue naming BOTH and WAIT at every boundary, incl. right after approval)
Branch `feat/altimetry` off `Live-Buoy-Update` @ 00f5d0e; test site by cherry-pick (README `--ours`); asset bump on every
static_overlay change and every push to test; tags `prod-pre-alt-job` / `prod-pre-alt` + the owner's go-ahead before each
production push; never force-push. Reviewers are fresh-context agents; every P0/P1 fixed and re-checked before the merge.
| # | Step | Model | Effort | Cue |
|---|---|---|---|---|
| 1 | Job: `altfmt.py`, `alt.py` (STAC, listing, plan, read, collocation, chunks, pointer, prune, stale), workflow + trigger + keepalive + tests paths, pytest; a real-data local build (the last 30 h) read back against the NetCDF + the obs−model histogram; a dispatch dry run on the runner | Opus 5.5 | HIGH | "Cue: select Opus 5.5 and set effort to HIGH for step 1 (the altimetry job)" |
| 2 | G32a: one fresh reviewer on the job (values vs the NetCDF, the collocation maths vs an independent interpolation, window dedupe/churn, prune/stale, prefix isolation, the licence credit); then the JOB-ONLY merge (tag `prod-pre-alt-job`), the Worker redeploy, the first runs verified (pointer, chunks inflated in a browser console, counts per mission) | Opus 5.5 | HIGH, then MEDIUM for the merge | "Cue: keep Opus 5.5 and set effort to HIGH for step 2 (G32a and the job-only merge)" |
| 3 | Client core: `decodeAltChunk`/`inflateBytes`, `AltStore` (+ adopt-without-flash, stall timers), `AltIndex`, `AltLayer` + `composeDots`/`hitAt`, the obs-mode generalisation (`OBS_DEFS`, `_mountObs(kind)`, `_loadAlt`), the `_goto` landing hook `_syncAlt`; their Node tests and mutants | Fable 5.1 | HIGH | "Cue: select Fable 5.1 and set effort to HIGH for step 3 (the altimetry store, layer and mount path)" |
| 4 | Client wiring: `_renderAlt` + `_obsShell`, the readout texts, the toggle row + `setAltOn` + `_syncAltAttach` + the note states, the page/env/app.py, CSS, bootstrap/flag/panel tests, asset 2.19.0, README | Opus 5.5 | HIGH | "Cue: select Opus 5.5 and set effort to HIGH for step 4 (panel, toggle and page)" |
| 5 | Test-site deploy (owner sets `ALT_OVERLAY=1` on the test service) + verification, desktop + phone: tracks at z2–z10, age fade, hover/long-press texts in both units, the readings line, credit, the toggle on hs with GFS and ECMWF while scrubbing and at shark speed (frame rate unchanged, dots step with the frames, no flicker during pans), tp/wind no row, Off → no `/alt/` requests, reload restore, console; a sheet for the owner | Opus 5.5 | MAX | "Cue: keep Opus 5.5 and set effort to MAX for step 5 (test-site verification)" |
| 6 | G32: two fresh reviewers (A code via `model: fable`: store adoption/abort races with held chunks, the hover = drawn invariant, the landing hook under play/seek/update/setModel/Off, mutants; B test site: the owner's use — is a storm inside or outside the model — on desktop + phone, dense tracks, both models, failure paths, performance, the credit) | Opus 5.5 (A on Fable) | MAX | "Cue: keep Opus 5.5 and set effort to MAX for step 6 (G32)" |
| 7 | G32 fixes; a short fresh re-check only if a P0/P1 or a fix in the store / layer / mount path | Opus 5.5 (Fable for a P0/P1 there) | HIGH | "Cue: keep Opus 5.5 and set effort to HIGH for step 7 (G32 fixes)" |
| 8 | Production: tag `prod-pre-alt`, fast-forward (env unset: no option, byte-identical behaviour), verify; then `ALT_OVERLAY=1` on production (the owner, or me in the owner's browser on request), verify live | Opus 5.5 | MEDIUM | "Cue: keep Opus 5.5 and set effort to MEDIUM for step 8 (the production merge)" |
Why: step 3 is a new drawing layer with a pinned invariant (hover = the drawn reading), a store that must swap data sets
without a flash, and an edit to the mount path (the G30/G31 class) — Fable at HIGH and a Fable fuzz reviewer; the job's
collocation maths is well-specified and checked against real data by a reviewer (Opus at HIGH); the panel, toggle and page
copy the SST pattern (Opus at HIGH); verification and the gate at MAX by the standing rule. Opus throughout is workable with
an extra fix round expected at step 3.

### Verification
- Suites: `python -I -m pytest -q` (site + job), `node --test --test-timeout=20000 tests/overlay/*.test.js tests/ui/*.test.js`.
- Data: the local build's chunks decoded with `altfmt.decode_chunk`: 20 random readings per mission = the NetCDF (lat/lon
  ≤ 1e-6°, swh exact, t ≤ 1 s); per-mission counts; obs − model histogram over the open ocean (expect median |diff| ≈
  0.2–0.4 m, bias near 0, a wider tail for `near_land`); every chunk < 1 MB; the first live runs in the bucket (pointer
  heartbeat, the chunk keys churning only for windows still receiving data, prune).
- Test site and production as in steps 5 and 8; the flag-off golden byte-identical; `/healthz` unchanged (no new provider).

### Budgets and risks
- Data: pointer ≤ 3 KB per 12 min; ~5–6 MB per 24 h on the first standalone visit (immutable keys stay in the browser
  cache; ~0.5 MB per changed window on an open tab); frame mode paused ≤ 3 chunks. Phones: the same bytes, paid once a day;
  if it bites, an optional per-chunk coarse subset (rank ≥ 3, ~1/8 the bytes) loaded first is the planned follow-up.
  Memory ≤ ~10 MB flat + the tile canvases while attached. Draw: ≤ ~3 ms per playback landing at 1080p.
- Upstream regenerates files every 30 min while data arrives (chunks churn ~4 h after each window: bounded by the 10-min
  cadence); the CloudFerro listing or anonymous access could change (the listing parser fails → the stale alarm; a 403 →
  exit 1 every tick until the credentialed fallback is built); a GFS range change makes the job refuse to run (loud).
- Coastal readings unreliable (the `near_land` proxy over-flags up to ~45 km: hollow dots, the safe direction); old Safari
  (≤ 16.3) / Firefox (< 113) lack `DecompressionStream`: an honest message (the job publishes zlib; "none" would double the
  bytes for everyone); the ±3 h window at a run's start; a model switch mid-playback with the toggle on (the `entry` guard).

### Known limitations (to state in the step-5 report)
- Readings are 2–6 h behind real time (HY-2B 1–2 days, so it rarely shows); tracks a few hundred km apart: the picture is
  dots along lines, not a field. ~0.3–0.5 m RMS vs buoys; hollow dots near the coast are estimates. The model value in the
  standalone hover is GFS-Wave's shortest-lead run interpolated to the reading's time (ECMWF only through the frame-mode
  hover). The frame-mode window shows nothing for frames more than ~3 h past the newest reading or before the run's start.

### §45 progress
- 2026-10-17: PLAN WRITTEN (four owner decisions recorded above), PAUSED BY THE OWNER before approval ("pause and we will
  resume later"; ExitPlanMode was declined). Nothing built; no branch; production @ 00f5d0e (§44 complete, SST live).
  RESUME: re-read §45, present it for approval (ExitPlanMode); on approval print the step-1 cue "Cue: select Opus 5.5 and
  set effort to HIGH for step 1 (the altimetry job)" and WAIT; then branch `feat/altimetry` off `Live-Buoy-Update` @ 00f5d0e.
  Owner to-do before step 1: register a free Copernicus Marine account (the licence assumes one; credentials only as GitHub
  secrets, entered by the owner, for the credentialed fallback). Open from §44: verify the second MUR SST day (20261016) in
  the bucket (~11-13 UTC 2026-10-17).
- 2026-10-17: SET ASIDE by the owner ("put the altimetry plan aside for now, we may revisit it later"); §46 planned instead.



---

# Superseded format: section 74 (2026-10-31), for reference

## 74. Satellite layer: altimeter wave heights and Sentinel-1 swell partitions — PLAN (2026-10-31, awaiting approval)

### Context
Section 45 planned a "Wave Height Altimetry" overlay in full and was set aside on 2026-10-17. Its owner decisions still
hold: the last 24 h faded by age, the model's colour scale, readings within ±3 h of the shown frame, and one release.
Section 71 shows satellites are the only observed data for most of Africa, South America and the Indian Ocean. It also
found a second product on the same open storage: CMEMS's Sentinel-1 swell partitions (WAVE_GLO_PHY_SPC-FWK_L3_NRT_014_002),
with height, PEAK period and direction for each swell, about 7-10 h old.

### Design
**Base:** §45 exactly as written (its job `alt.py` / `altfmt.py`, the client `AltStore` / `AltLayer`, the obs-mode
generalisation, the discrepancy toggle), with two changes:
- **CMEMS access:** the owner already has an account (§68). Anonymous S3 access is the primary route (verified again
  10-31).
- **NOAA's Jason-3 OGDR** is added as a fourth source (public domain, ~25 min latency). Its NetCDF is read by the same
  `read_l3`-style reader with its own variable names. It fills the newest hours before CMEMS's file appears; duplicates
  of the same Jason-3 pass are dropped by mission and time.

**Addition: swell partitions.**
- **Job** (`tools/model_frames/sarswell.py`):
  - Reads the S1A / S1C (and CFOSAT when flowing) SPC-FWK files of the last 24 h.
  - Keeps per observation point: time, position, and up to 3 partitions (Hs, peak period, direction FROM).
  - The propagated "fireworks" storm-source files are left out of v1. They are forecasts and would need their own
    display rule: owner's call later.
  - Publishes `sarswell/v1/` chunks in the §45 `ALT1`-style binary: per point t, lat, lon, n, then n x (hs, tp, dir).
    The pointer goes last; prune and stale rules as §45.
  - Step 1 first confirms the variable names, the period definition (peak) and the direction convention on a real
    file. A mean period stops the work (the MFWAM lesson).
- **Client:**
  - The altimetry option gains a sub-choice: "Heights (altimeter)" | "Swell (Sentinel-1)".
  - Swell points are drawn as small arrows (direction TO), coloured by the dominant partition's height (the hs scale),
    with the arrow length set by its peak period.
  - The hover lists every partition: "Swell 1: 1.8 m · 14 s · from 215° (Sentinel-1C, 6 h ago)".
  - Same store / layer pattern as §45 (a second store, the same chunk decoder, its own dot stamp).
  - In frame mode (the discrepancy toggle on wave height), swell points within ±3 h show over the model with the
    model's own value in the hover, as §45 does for heights.

### Steps, model and effort (one cue for the section)
§45's own step table holds, with the swell job built in its step 1 and the swell client in its steps 3-4. It mixes
models (Opus for the job and the panel, Fable for the store / layer / mount path). Per the standing rule (one cue per
section) the cue is set by the riskiest part.
**Cue for section 74: "Cue: select Fable 5.1 and set effort to HIGH for section 74 (build, test site, review, fixes)."**
- The job-only merge comes first (its own tag `prod-pre-alt-job` and go-ahead), then the client merge (tag
  `prod-pre-alt` and go-ahead).
- Env flags `ALT_OVERLAY=1` and `SARSWELL=1` are set on each service only after its data is live.

### Known limitations
- §45's limitations hold (tracks, not a field; 2-6 h behind; coastal readings unreliable).
- Sentinel-1 swell points come in daily batches from a few passes. They are offshore (the SAR wave mode samples the open
  ocean every 100 km along the orbit), so coasts see swell approaching rather than at the beach.

