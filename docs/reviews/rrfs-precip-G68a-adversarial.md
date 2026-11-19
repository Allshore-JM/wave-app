# G68a: adversarial review of aaf641a "RRFS job: typed hourly precipitation frames (section 102 A1)"

Scope: `tools/model_frames/rrfs.py` and `tests/model_frames/test_rrfs_frames.py` at aaf641a (branch feat/rrfs).
Line numbers are those of rrfs.py at aaf641a. The uncommitted browser work in the worktree was read (not changed) only to
check what the live and the coming browser accept.

Baseline: `python -m pytest tests/model_frames/test_rrfs_frames.py tests/model_frames/test_rrfs.py -q -p no:cacheprovider`
= 93 passed locally (eccodes installed). Without eccodes (as the model-frames-tests CI runs): 85 passed, 8 skipped.

Summary: P0 1, P1 0, P2 2, P3 6. 28 mutants: 25 killed, 3 survived (2 of the 3 are equivalent in practice).

## P0: breaks production

### P0-1: the live browser rejects every manifest that names a precip frame (deploy order)
- Where: rrfs.py:1100-1103 (`frames_manifest` writes `files[r]["precip"]`), against the live browser's
  `validateRegionalManifest` (overlay.js at HEAD / origin/Live-Buoy-Update c065697, line 2170):
  `for (var f in ff) if ((f !== 'wind' && f !== 'wdir') || ...) throw new Error('bad frame entry ' + i);`
- Failure: the RRFS job runs every 10 minutes from the default branch (model-rrfs.yml, `schedule`). Once this commit
  reaches production, the next source the job builds is written with precip frames. As soon as that source is the newest
  for any valid hour, the merged manifest's `frames[i].files.<region>` holds `precip`. The production overlay.js then
  throws `bad frame entry` in `_fetchManifest`. The whole RRFS Wind-speed overlay is unavailable for every visitor
  (new mounts get the error panel; open tabs fail as soon as they adopt the new signature). The extra field in
  `regions[r].fields` is harmless to the old browser. The extra key in `files` is not.
- The uncommitted overlay.js (line 2299) adds `f !== 'precip'`, so the fix exists. It just has to reach production
  first. If both ship in one merge, tabs opened before the deploy still run the old script (`overlay.js?v=VERSION` is
  loaded once per page load) and fail when they adopt the next manifest. That window ends when they reload.
- Suggested fix, either of:
  - (a) Ship the browser's acceptance of `precip` in `files` (and its `precipDefOk`) to production first, then this job
    commit in a later merge.
  - (b) Gate the precip frames in the job behind an env switch (for example `RRFS_PRECIP_FRAMES`, off by default; when
    off, FRAME_FIELDS = BASE_FIELDS and frame_ranges() omits precip). Turn it on after the browser has been live for a
    day.

  Option (b) also gives a kill switch, which other overlay layers have (COASTAL_ALERTS, BATHY_OVERLAY, ...) and this
  one lacks.

## P1: wrong output

None found. The code arithmetic matches the approved design:
- codes 2-51, 52-101, 102-151, 152-201, 202-251; 252-255 never written; max 2 + 50*4 + 49 = 251, so the uint8 cast cannot
  wrap;
- level = clip(round(49 ln(v/0.1)/ln 750), 0, 49): 0.1 mm gives 0, 75 mm and above give 49;
- MISSING amount or MISSING8 type gives 0;
- outside the mask gives 0;
- dry = TYPE_NONE with a known amount gives 1.

Checked and correct:
- f000 of a run: `_write_frames`, `frameset` and `frames_manifest` all use `has_frame` with the same kind. `frameset`'s
  `sorted(by_step) != steps` check still holds because f000 keeps wind and wdir.
- Old sources keep their wind (`_usable_regions` per field).
- `prune_frames` is per cycle, not per field, so a kept source keeps its precip PNGs and a dropped cycle loses them.
- `region_index` is still built on the build thread before the encoders start. `precip_codes` touches no shared state.
- The FRAME_DEFS tuples serialise as lists. `frame_ranges()` round-trips through the pointer JSON with exact equality
  (0.1, 75.0, 61.733333333333334 parse back to the same floats).

## P2: fragile

### P2-1: a rollback of this job commit silently drops the newer sources from the overlay
- Where: the previous code's `_usable_regions` (`if (fr.get("ranges") or {}) != frame_ranges(): return {}`).
- Failure: after this commit is live, every new source's `ranges` contains `precip`. If production is rolled back to the
  pre-precip tag, the old code drops every new-format source whole. The manifest falls back to whatever pre-deploy
  sources are still within FRAMES_STALE_H (8 h cycles, 18 h runs). If none is fresh, the pointer is withdrawn and RRFS
  shows as unavailable until the old code has built a new cycle (about 1 h). The hours past +18 h stay missing until it
  builds a new run (up to about 6 h). This commit makes the job tolerant of fewer fields. The code before it is not
  tolerant of more.
- Suggested fix: split the deploy. First ship only the per-field `_usable_regions` (and `has_frame`), which accept and
  ignore extra fields. Then ship the precip frames. A rollback to the first step is then clean. An env switch as in P0-1
  (b) does the same job: switching it off is the rollback, with no code revert.

### P2-2: tests that pin the precip frame against independent GRIB data never run in CI
- Where: test_rrfs_frames.py:151 (`_check_precip_against_eccodes`, inside the `@needs_ec`
  test_hawaii_run_frames_equal_an_independent_eccodes_read).
- Facts:
  - The model-frames-tests CI installs no eccodes (test_rrfs.py:24), so the only end-to-end check of the precip PNG
    against an independent read is skipped there.
  - Locally it checks 600 random nodes, but only 58 of them are wet, and the hi_f006 fixture holds rain only (codes
    3-40). No snow, mix, ice or thunder comes from real data.
  - The level it compares against is `J.precip_level` itself, so it is not independent of the formula.
  - The helper returns `wet`, but the caller does not assert `wet > 0`, so a fixture change to a dry hour would pass
    silently.
- Mitigation already present: with eccodes simulated absent, the five mutants this test killed first are still killed
  by the synthetic tests (see the table). The risk is therefore moderate, not high.
- Suggested fix:
  - assert `wet > 0` (or `wet >= 20`) in the caller;
  - add one hard-coded level, for example `precip_level(1.0) == 17` and `precip_level(10.0) == 34`, so the formula is
    pinned by numbers rather than by itself.

## P3: minor

1. rrfs.py:633: a cell with type NONE and an unknown amount is "missing" (code 0), not dry. Surviving mutant M5 shows no
   test pins this. It happens when an APCP value is at or below -0.005 mm: `precip_type` says NONE (`apcp < 0.1`) while
   `_u16` writes MISSING. The tables show the same "unknown", so overlay and tables still agree. Suggested: add the case
   `(apcp=MISSING, typ=TYPE_NONE) -> 0` to test_precip_codes_from_cell_records so the rule is pinned either way.
2. rrfs.py:609-613: `precip_level(NaN)` returns -9223372036854775808 (with a RuntimeWarning on the cast). The clip runs
   before the int cast and NaN passes through it. `precip_codes` cannot pass NaN (u16 input), so this is a hazard of the
   public helper only. Suggested: `np.nan_to_num(v, nan=PRECIP_LO)` or document "finite input only".
3. rrfs.py:884-887: an oversize precip frame drops the whole region, wind and direction included, although precip is
   called "an optional extra". The likelihood is low, since a CONUS frame of typed codes compresses like the wind frame
   and is far under 2 MB. Suggested: on an oversize precip frame drop only precip from that region (regions[r] without
   precip) and keep its wind.
4. rrfs.py:668: `_write_frames(..., kind="cycle")` defaults silently. A future caller that forgets `kind` for a run
   writes an all-missing precip f000 PNG, which frameset then leaves unlisted. That is harmless but wasted work. Mutant
   M18 is killed, so the current caller is pinned. Suggested: make `kind` required.
5. rrfs.py:1077-1103: when a run is the newest source, the slider's first hour is the run's f000 and has no precip. The
   older hourly cycle's f001 holds that same hour, but the "newest source per valid hour" rule does not use it. This is
   accepted in the design ("the manifest names none for that step"). The browser's `nofield` path has to show it as
   "no precipitation data", not as dry.
6. Volume: a run now uploads 85 x 4 x 3 - 4 = 1,016 PNGs instead of 680 (and a cycle 216 instead of 144), with no new
   timing measured against the 25-min job budget. The precip PNGs are mostly code 1 and should be small and fast.
   Suggested: log the precip totals on the first real run (`frameset` already logs per-field max sizes) and note the
   upload time.

Not defects, noted for the browser commit:
- `regions[r].fields` is the union over the used sources. A region can list precip while the hours from a pre-precip
  source (and a run's f000) name no precip file. That is the design, and the uncommitted overlay.js handles it
  (`nofield`).
- Thunder exists only in run hours (the hourly cycles carry no LTNG), so the type can change at the cycle/run seam.
  This is documented in the PRECIP comment.

## Mutation testing

Command per mutant:
`python -m pytest tests/model_frames/test_rrfs_frames.py tests/model_frames/test_rrfs.py -q -p no:cacheprovider -x`.
The exit code was read directly from the process, and the file was restored with
`git checkout -- tools/model_frames/rrfs.py` after each mutant. `git diff --stat tools/model_frames/rrfs.py` is empty at
the end.

The column "CI (no eccodes)" re-runs the mutants first killed by the eccodes test, with eccodes made unimportable (as
in the model-frames-tests CI).

| # | Mutant | Result (eccodes present) | CI (no eccodes) |
|---|--------|--------------------------|-----------------|
| M1 | PRECIP_CODES start `2 + 50t` -> `1 + 50t` | KILLED: test_hawaii_run_frames_equal_an_independent_eccodes_read | KILLED: test_precip_code_table_round_trips |
| M2 | precip_level `np.round` -> `np.floor` | KILLED: test_precip_code_table_round_trips | |
| M3 | precip_level `np.round` -> `np.ceil` | KILLED: test_precip_code_table_round_trips | |
| M4 | PRECIP_DRY 1 -> 0 | KILLED: test_precip_code_table_round_trips | KILLED: same |
| M5 | dry also where the amount is MISSING (type NONE) | SURVIVED (see P3-1) | |
| M6 | has_frame always True | KILLED: test_a_runs_f000_has_no_precip_frame | |
| M7 | frames_manifest ignores has_frame | KILLED: test_manifest_names_precip_and_skips_a_runs_f000 | |
| M8 | _usable_regions compares the whole ranges again | KILLED: test_sources_built_before_precip_keep_their_wind | |
| M9 | drop the BASE_FIELDS-in-region check | KILLED: test_sources_built_before_precip_keep_their_wind | |
| M10 | drop the per-field range check for precip | KILLED: test_sources_built_before_precip_keep_their_wind | |
| M11 | swap snow / mix in PRECIP_TYPES | KILLED: test_precip_code_table_round_trips | |
| M12 | type offset `- TYPE_RAIN` -> `- TYPE_NONE` | KILLED: test_hawaii_run_frames_equal_an_independent_eccodes_read | KILLED: test_precip_codes_from_cell_records |
| M13 | thunder excluded (`typ < TYPE_THUNDER`) | KILLED: test_precip_codes_from_cell_records | |
| M14 | level clip top 49 -> 50 | KILLED: test_precip_code_table_round_trips | |
| M15 | no floor at PRECIP_LO in precip_level | SURVIVED: equivalent (the clip at 0 gives the same result for v < 0.1, 0 and -inf) | |
| M16 | precip_value modulo 50 -> 49 | KILLED: test_precip_code_table_round_trips | |
| M17 | frameset ignores has_frame (all steps) | KILLED: test_a_runs_f000_has_no_precip_frame | |
| M18 | build_grid passes no kind (default "cycle") | KILLED: test_a_runs_f000_has_no_precip_frame | |
| M19 | precip range without the encoding name | KILLED: test_a_runs_f000_has_no_precip_frame | |
| M20 | precip encoded through quantize (no special case) | KILLED: test_hawaii_run_frames_equal_an_independent_eccodes_read | KILLED: test_region_values_carry_the_precip_codes |
| M21 | amount scale `/100` -> `/10` | KILLED: test_hawaii_run_frames_equal_an_independent_eccodes_read | KILLED: test_precip_codes_from_cell_records |
| M22 | dry by amount (`apcp < 10`) instead of by type | KILLED: test_region_values_carry_the_precip_codes | |
| M23 | has_frame kind "run" -> "cycle" | KILLED: test_a_runs_f000_has_no_precip_frame | |
| M24 | precip_codes ignores the mask | KILLED: test_hawaii_run_frames_equal_an_independent_eccodes_read | KILLED: test_precip_codes_from_cell_records |
| M25 | precip_codes ignores a MISSING amount | KILLED: test_precip_codes_from_cell_records | |
| M26 | _write_frames ignores has_frame | KILLED: test_a_runs_f000_has_no_precip_frame | KILLED: same |
| M27 | precip_value upper bound 251 -> 255 | KILLED: test_precip_code_table_round_trips | |
| M28 | has_frame ignores kind (`step == 0` only) | SURVIVED: equivalent today (hourly cycles have no step 0) | |

Survivors:
- M5 is a real but low-impact gap (P3-1).
- M15 is equivalent.
- M28 is equivalent while CYCLE_STEPS starts at 1. A test asserting `has_frame("cycle", "precip", 0)` would pin the
  intent if hourly cycles ever gained an f000.

## Fix round (2026-11-19, the building session)

All findings fixed or recorded. The job's design changed for P0-1: the precipitation rides BESIDE the wind frames instead
of as a third field among them.

| Finding | Outcome |
|---|---|
| P0-1 live browser refuses `files.<region>.precip` | FIXED. The frame set keeps `regions` / `ranges` exactly as before section 102 (wind + wdir) and carries precip in its own block `precip: {regions, range: PRECIP_RANGE}`; the pointer's frames entry passes it on; each manifest frame names its precip keys in a separate map `frames[i].precip = {region: key}` (absent at a run's f000); `files` never changes. Verified: production's overlay.js (origin/Live-Buoy-Update c065697) validates a manifest merged from the real dry run (cycle 2026111901 + run 2026111818, all four regions, 77 frames with precip) and sees only wind; the new client offers Precipitation from the same manifest. Test `test_old_browsers_and_an_old_job_never_see_the_precip`. The job can ship before the browser; tabs opened before any deploy keep working. |
| P2-1 a rollback drops the newer sources | FIXED by the same change: `frame_ranges()` is the pre-102 value again (pinned by number in the test above), so the pre-102 `_usable_regions` accepts the new sources; it ignores the precip block. |
| P2-2 real-data check never in CI, never asserts wet > 0 | FIXED as far as CI allows: the eccodes test asserts `wet > 0`; `test_precip_levels_pinned_by_number` pins 0.1 / 0.5 / 1 / 2.5 / 10 / 25 / 75 / 1000 mm to levels 0 / 12 / 17 / 24 / 34 / 41 / 49 / 49 without precip_level's own formula. Plus the dry-run check below. |
| P3-1 "none" with an unknown amount | PINNED: code 0 (missing), in `test_precip_codes_from_cell_records`. |
| P3-2 precip_level(NaN) | FIXED: NaN -> level 0 (pinned). |
| P3-3 oversize precip drops the region's wind | FIXED: an oversize precip frame drops that region's precip only; `dropped` says "precip of <r> dropped". |
| P3-4 `_write_frames(kind="cycle")` default | FIXED: `kind` required. |
| P3-5 run-newest f000 hour has no precip | RECORDED (design: the newest source owns its hours). |
| P3-6 1,016 PNGs a run, timing | RECORDED: the 336 extra PNGs are 2-61 KB each (dry run: largest 60,782 B, CONUS); measured after STOP A1 on the first production run's log. |

Dry-run check (laptop, real data, the committed job): cycle 2026111901 (18 steps) and run 2026111818 (85 steps), all
four regions. Every precip PNG decoded and compared with the source's own cell files with the code rule rewritten
independently: 244,800 nodes, 0 mismatches; nothing outside the region masks; no code over 251; no f000 frame for the run.
Largest precip PNG 60,782 B (CONUS), means 3-51 KB. Wet node-steps seen: rain, snow, ice and thunder (no mix in these two
sources).

Survivors after the fix round: M15 and M28 (equivalent, as the review says); M5 now killed (P3-1 pinned).
