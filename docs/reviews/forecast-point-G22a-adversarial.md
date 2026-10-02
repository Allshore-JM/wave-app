# G22a — the forecast-point job (plan section 31, step 3)

One fresh-context reviewer at HIGH effort, 2026-10-01, on `feat/forecast-point` @ `267f67f` (the job, its workflow, the
trigger Worker change and 47 tests on top of production `be536c7`). The review is job-only: no site file is in the change.
The reviewer had the full real NOAA run 2026100112 (627 gridded files), the product the job code built from it, and
instructions to check independently and to treat the author's tools as suspect.

**Result: 0 P0, 1 P1, 2 P2, 16 P3.** The stored values are right (every one of them was compared). The P1 is a coverage
hole in the choice of latitude bands; it was fixed before anything was published.

## What the reviewer confirmed

- **Every stored value equals NOAA's.** All 2,890 tiles and 3 masks read with the reviewer's own reader equal the build's
  scratch files (2,741,613,930 values, 0 differences), and all 627 NOAA files decoded with the reviewer's own GRIB2 section
  parser, records chosen by its own code-number table, quantised with integer arithmetic, equal the scratch files
  (2,741,613,930 values, 0 differences).
- **Record identity, units, conventions.** The code-number table agrees with NOAA's `.idx` names and with eccodes' short
  names. Against NOAA's point bulletins of the same run at 25 stations in all three grids: combined height within
  0.00-0.05 m at open-ocean buoys on the native grids, partition heights within a few centimetres and PEAK periods within
  0.1 s, product direction = bulletin direction + 180 (FROM) for swell and wind sea.
- **The runner.** GitHub's dry run of the same cycle (eccodes 2.48.0, four worker processes) reported the digest
  `3a1d4bbf8949733f`; the reviewer recomputed it from the local scratch files with its own code: identical. HEAD run through
  `main()` with real spawned worker processes on six real steps matches the verified product; a NotReady raised in a
  worker arrives as exit 3 with a notready record; a worker that dies gives a failure record and exit 1.
- **The sea mask** did not move within the run (0 cells over all 627 grid-steps); wind is present in every sea cell.
- **Isolation.** No frames or site file is in the diff; the two prefixes never list or delete each other; the frames
  tests pass unchanged. The Worker dispatches the frames first, so the points workflow cannot hold them back.
- **Costs.** About 585,000 class A operations a month for both jobs (free tier 1 M), under 50,000 class B, about 3 GB at
  peak beside the frames' 1.5 GB (free tier 10 GB); 627 anonymous GETs and 5.9 GB from NOAA's open-data bucket per run.

## Findings and outcomes

| # | P | Finding | Outcome |
|---|---|---|---|
| P1-1 | P1 | **No wave data from 12.75 S to 14.85 S at any longitude** (Samoa, Bahia, Peru south of Lima, northern Madagascar had no cell within reach), and the rows at 52.5 N / 52.25 N served from a cell 33 km away. NOAA's `global.0p16` FILE spans 52.5 N .. 15 S but holds waves on rows 2..390 only (52.167 N .. 12.5 S); the bands were cut at the file's bounds. The same edge rows are empty in two other cycles. The tests pinned the hole. | **Fixed.** `s25` stored from row 9 (12.75 S), `n25` to row 151 (52.25 N); `check_band` fails the build if the rows with waves are not the expected ones (`pointfmt.GRIDS[...]["data"]`); the manifest's `data_rows`, `lat_north`, `lat_south` are those of the rows with data; the test now asserts no gap wider than one row and no overlap. Rebuilt product: Pago Pago, Apia, Salvador, Cerro Azul, northern Madagascar each have a sea cell within 0.44 cells; a scan every 0.05 degree along five meridians finds no missing latitude other than land and ice. |
| P2-1 | P2 | The decoders (which will run in the web server) were not usefully bounded: a 79 KB tile could make them hold 1 GB, a 233-byte tile naming a 1 GiB xz dictionary allocated it, `Infinity` and deep JSON raised OverflowError / RecursionError, floats and strings were converted, legacy `.lzma` was accepted. | **Fixed.** At most 16 MB unpacked (a real tile is 3.6 MB), checked before the payload is touched; `FORMAT_XZ` only with `memlimit` 64 MB; header at most 4 KB; plain ints only, `fields` a list of names; only `ValueError` leaves; a reader can pass the manifest's steps and fields and the tile must match. Tests measure memory for the bombs. |
| P2-2 | P2 | No deterministic test held "every tile and mask is stored before the manifest": with the final drain deleted the suite passed in 3 of 4 runs. | **Fixed.** Tests with slow uploads (order of the store's log), with a slow failure of the LAST objects (`n25/mask.bin`, a tile in the final backlog), and through `main` (failure record). The two mutants are killed in 5 of 5 runs. |
| P3-1 | P3 | The format's bytes were not pinned: writer and reader could change together. | **Fixed.** Golden test: a fixed tile and mask read by a reader written without numpy or `pointfmt` (bit order, byte order, plane order, magic, header). |
| P3-2 | P3 | Mutants the suite did not kill (pointer repair conditions, pruning after a partial build, upload back-pressure, plausibility limits per kind, geometry tolerances, decoder limits, manifest texts, digest, a throwing fetch in the Worker, workflow text). | **Fixed** (tests added for each); see "Mutation" below for what is left. |
| P3-3 | P3 | `read_message` (the eccodes path) and the worker processes had no test. | **Partly fixed.** A round trip of `read_message` on a message eccodes itself builds (skipped where eccodes is not installed: the offline test workflow). The process pool stays covered by the runner dry runs. |
| P3-4 | P3 | A failed pointer write waited 3 hours for a one-PUT repair (`_skip_failed` ran before `existing_guard`). | **Fixed** (order swapped; test). |
| P3-5 | P3 | One transient NOAA error cost a 3-hour back-off. | **Fixed in part.** A failed or damaged download is tried three times (each try already retries its request four times). The 3-hour back-off for a build failure stays. |
| P3-6 | P3 | Drift of the sea mask was published with only a line in the summary. | **Fixed.** The manifest's `sea_cells` block says the rule and the counts; beyond 1 % of a grid's cells at any step the build fails before anything is uploaded. |
| P3-7 | P3 | Statements that were not true (bands; "NOAA sorts the partitions" is not always so on the interpolated grid, where the wind sea can exceed the combined height; 1.2 GB; "the pointer never moves back" without mentioning `--force`; renamed NOAA files do not fail loudly; what MISSING means). | **Fixed** in README and docstrings (the plan file after the re-check). |
| P3-8 | P3 | The manifest did not say the bitmap's bit order or the grid priority. | **Fixed** (`bitmap`, `nearest`, `data_rows`). |
| P3-9 | P3 | The scratch-space check did not add up the grids (sparse files). | **Fixed** (summed before any file is made; test). |
| P3-10 | P3 | A late missing file was seen only after everything before it was decoded. | **Not fixed; accepted.** `as_completed` raises the first failure when it happens and drops the rest, but the jobs still start in order: the re-check measured 357 files decoded before a missing `f120` was asked for (360 before). It needs a listed file to vanish; it costs about 3 minutes and 3 GB per tick while it lasts. NotReady has no back-off (as the frames job). |
| P3-11 | P3 | `dry_run` / `force` were free text: `True` ran a real publish. | **Fixed** in `model-points.yml` (`type: boolean`). `model-frames.yml` has the same inputs and is not part of this change: noted for the owner. |
| P3-12 | P3 | `--steps ","` was a traceback. | **Fixed** (exit 2). |
| P3-13 | P3 | Keepalive: a failing enable stopped the self re-enable. | **Fixed** (itself first). |
| P3-14 | P3 | The pointer was not re-read before it was flipped. | **Fixed** (a newer live run keeps the pointer unless `--force`; test). The concurrency group stays the lock against two builds at once. |
| P3-15 | P3 | Operational notes (a queued manual dispatch is replaced by the next tick; `failed/` and `notready/` records are never pruned; a killed build leaves no record; a disabled points workflow makes the Worker's tick an error; the Worker's fetch has no timeout; Node 20 actions and the ubuntu-latest move to Ubuntu 26 from 2026-10-19; the tests workflow did not run on a change of the pinned workflow texts). | The last one **fixed** (paths added). The rest **accepted**, on record here. |
| P3-16 | P3 | The temporary dry-run workflow was still on the branch. | **Removed**; a test asserts it is not there. |

## Evidence after the fixes

- **The same NOAA cycle built twice, by two decoders.** GitHub runner (eccodes, worker processes; run 36953854393): 2,969
  objects, 1,024.8 MB, 22 min 03 s, max RSS 4.2 GB, digest `e122ebfb962cf177`. Local build with the job code and an
  independent GRIB reader: 2,969 objects, 1,024,803,751 bytes, digest `e122ebfb962cf177`. (Before the fix both gave
  `3a1d4bbf8949733f` with 2,893 objects: the fix adds 11,303 cells to `s25` and 1,085 to `n25`.)
- **Grids of the rebuilt product:** `g16` rows 2..390, 546,583 cells; `s25` rows 9..272, 265,641 cells; `n25` rows 17..151,
  74,682 cells; no drift, no ragged partition.
- **Tests:** `tests/model_frames` 139 passed at the final state (the points file 71, was 47); trigger Worker 6 Node tests.
- **Mutation** (the author's lists, against `test_points.py`): 127 before the review (124 killed) and 68 after it, made
  from the reviewer's survivors and the new code: 62 killed at `7621527` (64 after the re-check's fixes). Left: the tile header's row / column / step limits (the byte
  cap and the blob's length refuse the same objects), `Infinity` in a header (refused as "not a plain int" either way), the
  `RecursionError` clause (Python 3.13 parses 4 KB of nesting; 3.11 raises), one stale (pinned by a constant test).

## Not checked (the reviewer's list, still true)

eccodes decoding of the values on a development machine (covered by the two runner digests); real R2, Cloudflare and
GitHub behaviour with credentials; Linux-only paths (sparse files, what `/usr/bin/time` counts); the tile BYTES produced
on the runner (a dry run uploads nothing: object count, total size and value digest matched); other seasons (ice moving
within a run) and GFS v17; the reader and the site.

## Re-check of the fix set (one fresh reviewer, 2026-10-01, on `7621527`)

**0 P0, 0 P1, 0 P2, 7 P3; nothing stops the merge.** The P1 and both P2 are confirmed fixed with the reviewer's own
tools on the real run and the rebuilt product:

- **The bands.** NOAA's `global.0p16` has waves on rows 2..390 in this run and in five other cycles (January, February,
  March, July, September), so `check_band` cannot fail on ice. No latitude is stored twice and none is missing between
  the bands: of the 582,520 sea cells of NOAA's global grid, 582,516 have a product cell within one cell. The newly
  stored rows equal NOAA (38.8 million values, 0 differences); at 54 bulletin stations inside the new southern rows the
  height matches to the centimetre and the directions are bulletin + 180.
- **The whole rebuilt product** equals NOAA through the reviewer's own GRIB reader (2,780,450,310 values, 0
  differences); its digest computed from NOAA's files is `e122ebfb962cf177`, as the manifest and the runner's dry run.
- **Decoders.** 70 hostile objects raise only `ValueError`; the worst accepted object costs 35-64 MiB (was about 1 GB);
  no real tile or mask is refused (the largest real tile is 3.44 MiB of the 16 MiB cap, and needs an 8 MiB xz limit).
- **Publish-order tests.** Ten mutants of the final drain were each killed in 5 of 5 runs under load; no false failure
  in 52 loaded runs.
- HEAD through `main()` with real spawned worker processes on six real steps: 79.8 million values, 0 differences.

| # | Finding (all P3) | Outcome |
|---|---|---|
| R-1 | P3-10 was recorded as fixed but a late missing file still costs the same work (357 files before a missing `f120`). | Record, docstring and README corrected; **accepted** (see P3-10 above). |
| R-2 | The "rest is dropped" half of `_results` was no longer tested (the author's mutant survived). | **Fixed** (counted after the pool has finished). |
| R-3 | A pocket inside `g16`'s band: off the Dutch coast (51.5-52.0 N, 3.5-3.75 E) NOAA's `global.0p16` has no waves on a few cells, and four cells of water are more than 1.5 cells from any stored cell (Domburg). The same in four other cycles. | **For the reader** (plan step 4): a reach in kilometres or two cells. Stated in the README. |
| R-4 | Decoder residuals: a mask claim of 8192 x 4096 cost 63 MiB; the tile header's `run` / `grid` / `tile` / `row0` / `col0` were not validated; duplicate field names; a docstring. | **Fixed**: masks up to 4096 x 2048 (the largest real grid is 2160 x 721; under 24 MiB); identity keys typed; duplicate names refused. Whether a tile is the one asked for stays the reader's check. |
| R-5 | A stalled download could take about 37 minutes per file before failing. | **Fixed**: 3 downloads x 2 request tries x 120 s (about 13 minutes). |
| R-6 | Untrue statements (the plan file not updated; rows 0 and 405 carry no wind either; 22 minutes, not 21). | **Fixed** (the plan file too). |
| R-7 | Small test gaps (`--force` in `existing_guard`, the tests workflow's pull_request paths, three constants; the stats sidecar's step list in completion order). | **Fixed** (tests; the list is sorted). |

Mutation at the final state (the author's second list, grown to 75 mutants): 70 killed; left are the three redundant
decoder limits and the Python-version-dependent `RecursionError` clause named above; one stale (its check was
rewritten; the decoder cases cover it).
