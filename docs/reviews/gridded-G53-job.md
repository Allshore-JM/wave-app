# G53: the gridded satellite wave-height job (plan section 82, step 2) — adversarial review record

Reviewer: one fresh agent (Fable 5.1), 2026-11-05, on `feat/gridded` @ 7168267: `tools/model_frames/altgrid.py`,
`.github/workflows/model-altgrid.yml`, `tests/model_frames/test_altgrid.py` (the alt.py machinery it imports read
alongside). Method: the code and the tests read end to end; the suite run (`python3 -I -m pytest
tests/model_frames/test_altgrid.py -q`: 9 pass); the real 2026-11-03 file inspected with h5py (time `[28065.0, .25,
.5, .75]` float32, exactly on the hour; latitude ascending; 1,179 cells flagged, every one a 0 m value; no flag where
the height is fill) and built with `--file --local`, then every cell of the four PNGs dequantized and compared with the
NetCDF (`scratchpad/revG53/compare_real.py`: the blank mask equals the product's valid-and-unflagged mask exactly, the
error at every present cell <= 0.045 m = half a quantum, row `(90 - lat) / 0.5` at every latitude, the poles absent);
the live bucket and STAC read anonymously twice (one production per day listed; the STAC collection links the two
datasets the pattern must split); twelve scratch tests in `scratchpad/revG53/test_revG53.py` (FakeWeb + FakeClient),
each of which PASSES when the defect it names is present (12 / 12 pass at 7168267).

| # | Sev | Finding | Evidence | Fix |
|---|---|---|---|---|
| 1 | P1 | **`--force` regresses the live pointer.** The never-regress check (`main`, `day < live_run or ...`) is skipped whole under `--force`, and `publish` always writes the pointer. So `--day 20261102 --force` (the only way to rebuild an older day's frames, since `--day` alone is a heartbeat) flips production's `latest.json` to Nov 2 and `newest_utc` back a day; a plain `--force` during a partial listing outage (this month's listing 503s on a month straddle, `A.list_files` skips it and keeps last month's) does the same with no operator intent. The README's "an older day never regresses the pointer" is false under `--force`. | `test_F1_force_day_older_regresses_the_live_pointer`, `test_F1b_force_with_the_newest_day_unlisted_regresses` | In `main`, split "rebuild" from "advance": `advance = not live_run or day > live_run or (day == live_run and prod >= (live_prod or ""))`; `--force` keeps forcing the rebuild and the `skip_failed` bypass, but `publish(...)` takes `pointer=advance` and, when false, writes the stats and manifest only and prints "pointer stays at <live>" (prune then runs with the unchanged pointer); README wording |
| 2 | P2 | **A manual `--day` retry while backed off is skipped with exit 0.** `skip_failed` applies to every live run without `--force`; after three failures an operator's `workflow_dispatch day=…` prints "waiting 2 h" and returns `stale(...)` = 0 for a manual run, publishing nothing. alt.py's `--window` bypasses the back-off; here only `--force` does. | `test_F2_manual_day_retry_is_skipped_while_backed_off` | `main`: `if live and not a.force and not a.day and A.skip_failed(...)` (and the help of `--day`: "retries a backed-off day") |
| 3 | P2 | **An all-fill production is published as complete and fresh.** `read_l4` / `build` accept a file whose four steps hold no value (every cell fill or flagged): `encode_frame` returns `valid_points 0`, `max None`, the manifest and pointer are written `complete: true` with a fresh `newest_utc`, so the client draws a blank map for a day and no alarm fires. That CMEMS ever produces such a day is unproven; the job's acceptance of it is proven. The real days carry ~143,000 cells (55 %). | `test_F3_an_all_fill_production_is_published_as_complete` | `read_l4`: `MIN_VALID_CELLS` (say 5 % of 359 x 720 = 12,924) per step, `ValueError("step HH holds N cells")` below it: refused, recorded as a failure, backed off; the count in the summary |
| 4 | P2 | **A prune error escapes `main`.** Both prune calls (`heartbeat` and after `publish`) are unguarded; alt.py's `main` wraps its own. An R2 `DeleteObjects` error (`Store.delete_keys` raises `RuntimeError("delete errors ...")`) ends a heartbeat with a traceback on EVERY scheduled run while the superseded production is due (the stale check skipped, up to 16 red runs a day), and turns a successful publish red after the pointer is already written. | `test_F4_prune_errors_escape_main` (three raises, the pointer at 20261104 after the last) | `main`: `try: ... prune(...) except Exception as exc: print(f"prune failed: {exc.__class__.__name__}: {exc}")` in `heartbeat` and after `publish`, as alt.py line 1312 |
| 5 | P2 | **Transient download failures are charged to the production.** `A.download` raising (503 x 4 tries, a reset, a truncation) is caught by the big `try` and `record_failure`d against the production stamp; three such runs (the 07:17 / 07:47 / 08:17 slots during a 1-h CloudFerro outage) back the day off 2 h although the file downloads fine afterwards. The 404 purge race is harmless (the next listing names the new production: a new digest). | `test_F6_transient_download_outage_is_charged_to_the_production` (the host back, the run still returns 1, no pointer) | `main`: download outside the recording `try` (or `except IOError` from `A.download` -> print, `return quiet("the file cannot be downloaded")`, nothing recorded); keep the record for reader / encode / publish faults |
| 6 | P3 | **A corrupt or non-object pointer crashes every run** with a traceback (`json.JSONDecodeError` from `store.get_json`, `AttributeError` on `"x"`, `TypeError` comparing `day < 20261103` when `run` is a number) until fixed by hand; the format guard only sees a dict with a wrong `format`. | `test_F5_corrupt_pointer_crashes` | `main`: wrap the pointer read (`except ValueError`), require `isinstance(pointer, dict)` and `isinstance(live_run, str)` / `live_prod` str-or-None, else "pointer unreadable: refusing to touch it", exit 1 |
| 7 | P3 | `--keep 1` deletes the day the previous pointer named in the run that supersedes it (a client on the old pointer, <= 17 min, 404s on its manifest and frames). Operator-only (`--keep` is not a workflow input); the default 3 is safe. | `test_F7_keep_1_deletes_the_previous_pointers_day_at_once` | `prune`: also protect the day of `pointer_prev` (pass the previous pointer's `run` from `main`), or `--keep >= 2` in the argument check |
| 8 | P3 | `--file` without `--local` / `--dry-run` is a LIVE publish (`live = not dry_run and local is None`; the env's R2 credentials are wanted). The help says only "build from this local L4 file"; the plan's real-file check always adds `--local`. Not wrong, but an operator running the documented check without `--local` on a machine with the secrets publishes. | `test_F8_file_alone_is_live` | help text "(live unless --local / --dry-run)"; or require one of them with `--file` unless `--force` |
| 9 | P3 | Test gap: the float32 time rounding and `TIME_SLACK_S` are never exercised. 00 / 06 / 12 / 18Z are exact in float32 (`.0 / .25 / .5 / .75` of an integer day), in the fixture and in the real file, so the "rounds to the hour" assertion in `test_read_l4_...` tests nothing; the code is right (a 2-min offset rounds, 6 min is refused). | `test_F9_time_rounding_is_untested_but_works` | add the two offsets to the reader test |
| N | nit | The two crons meet at 09:17 (`17,47 7-10` and `17 */3`): two runs queue under the group, the second a heartbeat (harmless; `17,47 7-8,10` + `17 */3` avoids it). `test_the_workflow_...` asserts `"dry_run" in text and "force" in text and "day" in text` (true of any workflow with these words; assert the `inputs:` keys and the `--day "$DAY"` wiring). The `--force` help "rebuild even when the pointer names this production" undersells #1. `heartbeat` lists the whole prefix on every quiet run (small; fine). | — | wording / test tightening |

No P0 found: nothing publishes wrong data or a pointer naming a missing object on the scheduled path.

## Fix round (2026-11-05, Opus 5.5)

| # | Fix |
|---|---|
| 1 | `main`: an older day or an older production of the live day never moves the pointer; under `--force` it is rebuilt (frames, stats, manifest) beside the live one, `publish(advance=False)` writes no pointer. |
| 2 | `main`: the back-off applies to scheduled runs only; a manual `--day` retry goes ahead. |
| 3 | `read_l4`: a step with fewer than `MIN_VALID_CELLS` (50,000; a real step ~143,000) valid cells refuses the file (recorded, backed off, the stale alarm if it lasts). |
| 4 | `safe_prune`: a prune error is a summary WARNING on the heartbeat and after a publish; the run's exit code is the stale check's. |
| 5 | `main`: a failed download is a summary WARNING and a quiet return (scheduled: the stale check; manual: 1), never a record against the production. |
| 6 | `main`: a pointer that is not JSON, not an object, or with a non-string `run` / `production` is refused (exit 1), nothing written over it. |
| 7 | `--keep` must be >= 2 (`prune` refuses 1): the day the previous pointer named stays for its readers. |
| 8 | `--file` help: "a LIVE publish to the bucket unless --local or --dry-run is given" (the behaviour stays). |
| 9 | `test_read_l4_...`: 2 min off the hour rounds, 6 min is refused; and the near-empty step refused. |
| nit | The second cron is `17 0,3,6,12,15,18,21 * * *` (never 09:17 twice). |

Tests: `test_altgrid.py` 11 (2 new: the G53 findings 1, 2, 4, 5 end to end; the corrupt pointers and `--keep 1`). The
reviewer's scratch proofs `scratchpad/revG53/test_revG53.py`: 9 of the 10 defect proofs now FAIL (the defects gone);
F8 still passes by design (the help now says it); C1 / C2 (checked-fine confirmations) pass.

## Checked and fine

- **The flip and placement**: `read_l4` verifies latitude ascending from -89.5 by 0.5 (359) before `hs[:, ::-1]`;
  `place_half` puts north-up row 0 (89.5 N) at row 1: every cell of the real file lands on row `(90 - lat) / 0.5`, rows
  0 and 360 absent (`compare_real.py`, all four steps). Longitude needs no shift (file -180 .. 179.5 = GRID lon0 -180,
  centre registration, 720 columns); `GRID` is pinned to `run.GRID_HALF` by the test.
- **Quantisation and the clamp**: `encode_frame(fill=False)` on `FIELDS["hs"]` (0 .. 22.86 m): 0 m is code 1 (a value),
  NaN code 0, above the top clamped to 255 and counted (`clamped_high`), `MAX_VALUE_M` 30 drops faults; identical input
  gives identical PNG bytes (`test_C1`), so `--force` re-uploading the same immutable keys is honest.
- **The refusals**: units, scale, fill, axes (count, direction, step), the time units, the step count and their hours
  against the NAMED day, the flag shape, `VAVH_INST` shape; the 2-degree daily dataset is never listed (`DATASET_RE`,
  `test_main_...`); the STAC collection really links both datasets (read live). Flag != 0 -> absent (the real file's
  1,179 flagged cells are all 0 m, so nothing of value is lost).
- **Time axis**: `days since 1950-01-01` checked, float64 from float32, rounded to the hour within 300 s (never
  compared exactly); the real values are exact.
- **Write order and immutability**: 4 frames -> stats -> manifest -> pointer LAST (`test_main_publishes_...`), frames
  / stats / manifest `IMMUTABLE` under keys that carry the production or the publish stamp, the pointer `max-age 300`.
  A client holding the previous pointer for 17 min: a superseded production and its manifest live 2 h measured from
  the pointer's `published_utc` (the right clock: when the pointer left them), the grace also ending on quiet runs; a
  superseded DAY stays as one of the 3 newest complete days. Two flips within 2 h keep both predecessors (conservative).
- **Never regress on the scheduled path**: lexicographic `YYYYMMDD` and `YYYYMMDDTHHMMSSZ` compares are sound; a same-day
  production OLDER than the pointer's is a heartbeat (`test_C2`); a newer day with any production is taken; a listing
  missing the live day is a heartbeat; `--day <older>` without `--force` is a heartbeat.
- **Prune**: only under `altgrid/v1/`; `failed/`, `latest.json`, `stale-alarm.json` are not day names; a manifest-less
  day newer than the newest complete one (a build cut short) is kept, an older one deleted; failure records only of
  days older than every kept day (the newest day failing keeps its record); nothing deleted while the pointer is empty.
- **Failures and back-off**: `record_failure` writes alt.py's record shape (`window`, `attempts`, `digest`,
  `last_attempt_utc`, `last_error` redacted) stamped with the run's `now`, the digest = the production stamp (a new
  production resets the count); `skip_failed` reads it with the same `now`; 3 attempts then 2 h; while backed off a
  scheduled run downloads nothing and returns the stale check (0 while the map is fresh, 1 once per 12 h with none).
- **The stale alarm**: `altgrid/v1/stale-alarm.json` is this job's own record (`PRODUCT.stale_key()`), `last_alarm_utc`
  shared with nothing; 48 h on `newest_utc` (normal lag 13.5-37.5 h), once per 12 h, scheduled live runs only
  (`--day`, `--file`, `--dry-run`, `--local` never alarm and return 1 on their own failure); a publish of an already
  stale day still alarms (right: the map is stale).
- **`--dry-run` / `--local`**: `live` false -> no store read or write, no failure record, no pointer, no prune (the
  test's injected store receives no put); `--local` writes every object under the folder, the pointer last.
- **Exceptions**: STAC down -> the pointer's cached list -> the bucket listing, all caught; a listing month failing is
  skipped by `A.list_files`; a truncated download (`have != size`) retried then `IOError`; `h5py` errors and refusals
  caught and recorded; a store outage on the pointer read propagates (as alt.py; acceptable).
- **The workflow**: both crons as the plan; `workflow_dispatch` `day` validated by regex before the job (`exit 2`),
  `dry_run` / `force` compared to `"true"`; `concurrency` group without cancel; `permissions: contents: read`; the four
  R2 secrets only; actions pinned by full sha; packages pinned (h5py in); `timeout-minutes: 30`; not on the trigger
  Worker; the keepalive and the tests workflow pin it (asserted). `/usr/bin/time -v` as the sibling workflows.
- **Tests**: the put order, cache policies, pointer / manifest shapes, the southern-rows-land-north-up check, the
  re-produced day under new keys with the grace, the never-regress case, prune's keep / live / in-progress / other
  prefix rules, the alarm cadence, the back-off and retry, `--local` / `--file` / bad arguments, the pins: all pin what
  they claim. Gaps beyond #9: #1, #2, #4, #6 are untested paths.
