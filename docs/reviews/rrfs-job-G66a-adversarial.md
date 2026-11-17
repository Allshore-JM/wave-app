# G66a: adversarial review of the RRFS job (plan section 99, steps 1-4)

Reviewed: feat/rrfs b38a930..4217483 (`rrfs_grid.py`, `tools/model_frames/rrfs.py`, `model-rrfs.yml`, the Worker list,
tests). Fresh-context reviewer, read-only, 6 idx reads from NOAA's bucket, mutants on a scratch copy. 2026-11-17.

Result: 0 P0, 2 P1, 4 P2, 6 P3. All fixed or recorded below; re-verified the same day.

| # | Finding | Outcome |
|---|---|---|
| P1-1 | A run that never builds (budget, a file never served) stays silent: the stale check read the pointer's publish time, which every hourly cycle refreshes; not-ready counts ignored | FIXED: `stale_kinds` per kind against the cycle time (hourly cycle 8 h, run 24 h), one alarm per 6 h (run.py's record); the 6th not-ready / budget exit in a row for a cycle fails the run once (run.py's `NOTREADY_WARN`). Tests: stale per kind, alarm while cycles publish, 3,3,3,3,3,1,3 |
| P1-2 | The file-backed memmap dirtied every page of the 2.3 GB file at every step (risk on a runner) | FIXED: each grid is built in RAM and written once; per-grid seconds and download waits logged. The four-grid laptop dry run went from 234 s to 178 s, with output byte-identical except at calms (below) |
| P2-1 | The run shared what the hourly cycle left of the 25-min budget and fetches paused while each batch decoded | FIXED: a run starts in a tick only with `RUN_MIN_LEFT_S` (15 min) left, else it waits for the next tick (no record); fetches run in a sliding window ahead of the decoding |
| P2-2 | No check that a decoded record is the field asked for | FIXED: `check_identity` compares shortName, level, start / end step (hours), date and time with what `wanted()` asked; every real record of the four grids passed it in the dry run; swaps (DPT as TMP, the run total as the hour's amount, another step / cycle) fail |
| P2-3 | Prune deleted a dropped source's files while a site could still hold the previous pointer | FIXED: prune keeps what the new OR the previous pointer names |
| P2-4 | The plan's LAND mask per grid is not published | RECORDED (plan change): not needed. The tables and points keep the site's own sea / land rules and the weather tab reads any cell; the cell files hold every cell |
| P3 | Surviving mutants (column index past the first batch, the precipitation carry, prune's newer-cycle guard, `_u16` `>=`) | FIXED: multi-step wiring tests (20 steps over windows of 4, run from f000), prune tests. 11 mutants now: 10 killed; `_u16` `>=` -> `>` is EQUIVALENT (a value rounding to 65535 is stored as 65535 either way), with a boundary test added |
| P3 | CI never ran the grid tests; eccodes tests skipped there | FIXED: `model-frames-tests.yml` runs `tests/test_rrfs_grid.py` too; the main-flow tests use a fake build (no eccodes); the decode tests stay local (eccodes) |
| P3 | Weak type assertion (passed by definition for snow / mix / ice) | FIXED: the full order written out independently |
| P3 | `ORDER`'s comment said the grids do not overlap (CONUS and Alaska do, off Washington / BC) | FIXED: the comment, and every user must go through `locate()` (CONUS wins) |
| P3 | A calm got direction 180 | FIXED: missing direction where the speed rounds to 0 (84 cells in the 17Z cycle: the only bytes that changed) |
| P3 | `content_length` without retries; `head_ok` slept after its last try; the pointer's `"run"` named an hourly cycle | FIXED: retries; no last sleep; the field is `"newest"` |

Open (not a finding): the job has not yet run on a GitHub runner (the `gh` CLI is not signed in on the laptop). The
first production ticks after STOP 1 give the runner's time and memory (`/usr/bin/time -v` in the summary); the laptop
estimate for an 84-h run is ~13 min of build plus ~4.7 GB of uploads.
