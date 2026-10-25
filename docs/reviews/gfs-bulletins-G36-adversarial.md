# G36: GFS bulletins and station winds without NOMADS (plan section 53)

## G36a: the GFS winds job (step 2), 2026-10-25

One fresh-context reviewer at HIGH on `feat/gfs-bulletins` @ b839a80: `tools/model_frames/gfs_winds.py`, its workflow, the
trigger / keepalive / tests-path wiring, the tests and the README section. The site side (step 1, 84be491) was out of
scope.

**0 P0, 0 P1, 2 P2, 7 P3.**

### What held up (the reviewer's own evidence)
- **C1 values:** the whole archive of run 2026102512, 918 stations x 385 hours = 353,430 cells, matched the site's
  `app._parse_spec_wind_text` and an independent parser of the reviewer's own, with 0 differences. Its winds.bin sha256
  (`db1ef486...`) equals the author's local and runner builds. 12 stations matched their NOMADS `.spec`, 0 differences.
- **C2-C6, C8, C9:**
  - encoding round trip, steps, publish order (the pointer last), prune rules, hostile archives, discovery on the
    scheduled path, workflow pins and secrets;
  - memory: 53 MB peak working set on a 600-member build.
- **C7 only in part:** a stall is cut off at STALL_S but was filed as a budget exit (P2-1).

### Findings and outcomes

| # | Finding | Outcome |
|---|---|---|
| P2-1 | On Python 3.10+ `socket.timeout` IS `TimeoutError`, so a stalled download was taken for the build budget: exit 3 and no failure record (a local stalling server: rc 3, no record). | FIXED: the budget has its own `BudgetExceeded`; a stall is a recorded failure (exit 1). Test with a stream that times out like a socket. |
| P2-2 | While a newer run could not be built (budget, stall, NotReady, 6 failed attempts) every tick exited 3 without the stale check: a 30-h-old live run gave rc 3 and no alarm, every workflow run green. | FIXED: `_not_now` runs the stale check on every such exit; the budget exit is also counted under `notready/`. Tests for budget / notready / failed x6. |
| P3-1 | `--run` skipped the never-regress rule (the pointer moved 12Z -> 06Z) and rewrote a published run's immutable index.json. | FIXED: `--run` of an older run is refused (exit 2); a run with an index is never rewritten (exit 0, nothing written). |
| P3-2 | The gzip CRC32 / ISIZE were never checked (tar stream mode stops at the end-of-archive block): a flipped CRC or a cut trailer built fine. | FIXED: the gzip stream is read to its end after the tar (inside the budget). |
| P3-3 | Nothing tied a member to the run: the previous cycle's archive under this run's name built, 6 h shifted. | FIXED: at least 90 % of members must start at the run's hour (one member missing its first hour still builds; tested). |
| P3-4 | `FLOAT_RE` was quadratic on a long digit run in the unbounded point-line group (40 KB: 1.9 s; 1 MB: ~20 min by extrapolation), with no budget check inside a member. Hostile input only. | FIXED: the point line is bounded (`POINT_LINE_MAX` 400 B after a quoted name of at most 64 B; real lines ~62 B); a longer line is no point line. |
| P3-5 | One transient 503 / reset on the GET is a failure with a 1-h wait, while a stall used to be retried at once. | ACCEPTED: the frames jobs' policy (NOMADS's `.spec` covers the hour). The two now agree: both are failures. |
| P3-6 | Test gaps: 12 of 34 mutants survived (prune's pointer protection, failed/notready records, the budget exit code, the already-live stale alarm, step 385, the finiteness guard, `--run` without the size check). The site-parity test never runs in CI (no Flask). No test pinned the removal of the dry-run workflow. | FIXED: tests for each; the reviewer's 34 mutants re-run: 3 survive, all equivalent (M02: a minus sign never changes which float is 4th / 5th; M17: non-run folders sort after every run and are never deleted; M33: a short member read is tarfile's ReadError first). The parity test runs in the site suite (it has Flask); CI checks the captured real member's values. The dry-run workflow is removed, with a test. |
| P3-7 | Nits: `head_size` slept after its last try; failed/notready records never pruned; the Worker and wrangler comments did not name the fifth workflow; `--out` silently ignored in live mode. | FIXED: no sleep after the last try; records of runs older than the oldest kept run are pruned; comments; `--out` without a dry run is refused (exit 2). |

The fix round's own changes: 15 mutants, 15 killed. Job suite 333 passed; the trigger Worker's Node tests pass.

## Step 5: test-site verification, 2026-10-25

Effort MAX. The test service (wave-app-clean) ran `test` @ 163d262: the site commits of steps 1 and 4 (84be491, a9a6c6f)
and the two job files the reader's tests import (`tools/model_frames/gfs_winds.py`, `publish.py`; the test branch carries
no job workflow). Every NOAA answer below is run 2026102512 unless named.

### What was checked
- **The deploy:** live 22:29:44 UTC, 44 polls of `/robots.txt` + `/healthz` across the switch, 0 failed.
- **Answers with NOMADS on:** 65 `/api/forecast` answers captured before the deploy (the old code, every bulletin and
  Wind column from NOMADS) and after it: 65 of 65 identical. The set: 30 listed GFS stations (compact), 6 classic,
  5 Metric, 5 UTC, the 12 SWAN stations, 3 Great Lakes stations, 2 forecast points, 2 removed stations. Only the
  clock-driven parts were normalised (the table's current-hour marker, a point's run age).
- **Answers with NOMADS off:** at the owner's request `NOMADS_DISABLED=1` was set on the test service in the owner's
  Chrome (Render: Environment, Save, rebuild and deploy); live 22:35:23, 40 polls 0 failed. The same 65 queries:
  - 53 identical (every GFS table and its Wind column, the lake tables, the points, the removed stations);
  - the 12 SWAN tables differ only in the two Wind cells of their first 24 rows (blank) and in `wind_complete`
    (false); every other cell and the graph data identical (finding V5-1 below).
- **`/healthz` `gfs` with NOMADS off:** bulletins from the archive (`reads.ok` 37, every other reason 0, NOMADS
  fallbacks 0, no layout warning); winds from R2 (`r2` 43, `nomads` 0, errors 0); `run_missing` counts the SWAN
  back-fill's run (2026102412), which the job never published (its first run was 2026102512).
- **The browser** (headless Chrome, own profile, trusted input; desktop 1280x800, phone 375x812):
  - 51201 and 46026: 385 rows, every row with a wind speed and direction, the run line "GFS · run 20261025 12 UTC";
  - 51201 under SWAN: 175 rows, 24 without wind (V5-1);
  - the window expands; no sideways page scroll; the phone's window full screen;
  - `/healthz`: no NOMADS read during the session; console 0.
- **Timing (NOMADS off):** forecast answers 0.26-1.25 s for a station's first build after the restart, then from the
  server's cache. Memory: RSS 155 MB at 10 minutes.
- **The 18Z run switch with NOMADS off:** NOAA's 18Z bulletin archive appeared between 23:24:36
  and 23:25:39 UTC, its spectra archive by 23:32:58. The production winds job (the trigger's 23:36 dispatch) published
  2026102518 at 23:37:44. The test site's run check (cached up to 30 minutes) moved to 18Z at 23:40:18. The winds
  were out 2.5 minutes before the switch, so the 18Z tables had wind at once. With NOMADS off, a switch before the
  publish leaves the new run's Wind column blank until the job has published it (12.5 minutes after the archive this
  time; the site's run check can move at any moment in the 30 minutes after the archive appears), plus up to 5 minutes
  for the blank answer's short cache. Not seen this time.
  After the switch, with NOMADS still off:
  - 30 of 30 listed GFS stations on 18Z: 385 rows, wind in every row;
  - the 12 SWAN tables built under 18Z (a fresh cache key, `tz=UTC`): all 2,100 wind cells = my own decoding of the
    R2 table of their run (72 back-fill cells from 12Z, 2,028 from 18Z);
  - NOAA directly (not through the site), 10 listed stations: the 18Z winds table = NOMADS's `.spec` through the
    site's parser at all 385 hours, and the archive bulletins = NOMADS's files byte for byte (the committed index
    holds for a new run; this was the job's second published run).
  PacIOOS's SWAN update at 23:04 UTC moved the SWAN tables' first hour to 12Z of 10-25, a run the job holds: from
  then the 12 SWAN tables had wind in every row with NOMADS off.
- **NOMADS on again:** `NOMADS_DISABLED` removed in the owner's Chrome (the row deleted, the other nine
  variables checked unchanged, Save, rebuild and deploy at 23:47 UTC). The new instance served from 23:48:11, 70 polls,
  0 failed. The 65 answers on 18Z with NOMADS on vs with NOMADS off:
  - 53 identical;
  - the 12 SWAN tables differ only in wind cells: the NOMADS-off capture served SWAN tables cached before the 18Z
    switch (every row from 12Z), the new instance built them under 18Z; every cell of both captures equals the R2
    table of its run (2,100 + 2,100).
  A cold instance answered the 65 queries (median 0.64 s, max 4.1 s) with 0 NOMADS reads for bulletins and winds
  (`/healthz`).

### Finding

| # | Finding | Outcome |
|---|---|---|
| V5-1 | P2. The job keeps 4 runs (24 h), but the SWAN tables' wind back-fill reads the GFS run of the SWAN table's first shown hour (12Z of PacIOOS's publish day) for as long as that table is shown, about a day (PacIOOS updates near 23 UTC). Just before each update that run is the 5th newest GFS run, and the 6th when the update comes after the 18Z run (about 23:30 UTC). With 4 kept, the back-fill falls to NOMADS's `.spec` for about 5-6 hours a day; when NOMADS refuses, the first 24 rows of every SWAN table have no wind. Today it shows already: the job started at 2026102512, so 2026102412 was never published. The README, `gfs_winds.py` and `app.py` say the back-fill reads "a run 6-12 h old"; it can be 30 h old. | FIX (step 7): `KEEP = 8` in the job (48 h; ~1.4 MB a run), the three texts corrected, a test pinning the retention against the back-fill's age. A job-only change: its own production push. |

### Notes (no change asked)
- A SWAN table is cached up to 30 minutes and its key holds no GFS run, so after a GFS run switch its Wind column can
  show the previous GFS run for up to 30 minutes. Older than section 53; unchanged.
- One HTTP 500 from models.allshoresurf.com on the winds pointer (`latest.json`) in 68 one-minute polls (23:48:42), fine
  a minute later. The site never reads the pointer: it asks the run's `index.json` and `winds.bin` by name, and a
  failure there means a 60-s wait and NOMADS.
