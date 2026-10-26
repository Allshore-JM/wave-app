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

## G36: the site side (steps 1 and 4), 2026-10-26

Two fresh-context reviewers at MAX on `feat/gfs-bulletins` @ 512f2db (`git diff c8c7057..512f2db`): reviewer A (on
Fable) the code and its data; reviewer B the running system (the test site @ 163d262, production for comparison, and a
local instance behind its own CONNECT proxy for failure injection). Neither changed any setting or branch.

**0 P0, 0 P1, 2 P2, 12 P3** (A 0/0/0/9, B 0/0/2/4; A's P3-1 and one of B's are the same item).

### What held up (the reviewers' own evidence)
- **The archive and its index:** 7 runs (A) and 16 runs (B) all 49,356,800 B. A rebuilt the index from run 2026102518
  with the real tool: identical to the committed one. All 918 members of 10-25 18Z pass the header and text checks
  (both reviewers, separately). NOAA's header, as written: `ustar\0` `00`, typeflag `0`, empty prefix, pax with atime
  and ctime only.
- **The checks:** 1,022 of 1,024 single-bit flips of a real header refused (the 2 accepted flip the checksum field's
  own terminator, which POSIX treats as a space); 75 header, 41 text and 15 body cases reach every reason; 50 hostile
  index documents and 24 hostile tars handled; 34 hostile R2 tables refused.
- **Same answers:** test site (archive + R2) vs production (NOMADS for everything) on the same run: 76 of 76 answers
  byte-identical (37 GFS stations incl. the archive's first and last members, Metric, UTC, the 12 SWAN stations, 3 Great
  Lakes, 2 points, 2 removed). Archive vs NOMADS text for 10 stations: identical bytes, identical tables and payloads.
  The R2 table vs NOMADS's `.spec`: 385 of 385 hours. The SWAN back-fill: 2,100 of 2,100 cells = the reviewer's own
  decoding of R2 (72 from 12Z).
- **Fallbacks (local instance, B):** archive down + NOMADS up: 6 of 6 answers identical; R2 down + NOMADS up: 1,890 of
  1,890 Wind cells identical; R2 down + NOMADS off: blank Wind, one R2 try then 60 s, recovery 5 minutes after the
  blank answer; R2 404: the 120-s rule; a stall in the archive or R2 cut inside the caps (15 s / 8 s); everything down:
  the old error texts and the 2-minute negative cache.
- **The rest:** the 206 / exact-length rule on a local server in 16 modes; the run probe in 200 answer combinations
  (1 HEAD normally, 16 at worst); singleflight 40 threads -> 1 fetch; the R2 LRU; the fork reset by a direct call; the
  browser on desktop and phone (0 console problems); the whole suite 1,446 passed, none reaching S3, NOMADS or R2;
  fresh builds on the warm test site median 0.80 s vs production 1.36 s (SWAN 0.37 vs 2.41 s).

### Findings and outcomes

| # | Finding | Outcome |
|---|---|---|
| B-1 | P2, new with section 53. The archive reads go through a plain `requests.Session()` with no retry. NOAA's bucket closes an idle keep-alive connection after about 5.6-5.8 s; a read sent into that window gets `RemoteDisconnected`, is counted as `transport` and goes to NOMADS, or with NOMADS refusing shows "Could not download .bull for X" although the archive is healthy. Test site: 3 times (0.5 % of fresh builds at visitor-like spacing); local, NOMADS off: 5 of 42 builds at 5-6 s spacing. The old code read AWS through `app.HTTP` (retries) and recovered from exactly this. Author re-check: a plain session failed 3 of 15 reads after 5.75-5.9 s gaps, the retrying session 0 of 15. | FIX: one retry, only for a connection the server dropped before answering (not for timeouts), inside the read's wall-clock cap, on the archive read and the R2 winds read. |
| B-2 | P2 (P1 if NOMADS ever refuses by going silent). Each probe HEAD goes through `app.HTTP` (3 attempts x 10 s): a silent NOMADS costs 31 s per cycle newer than the newest archive, about 124 s per probe between 00:00 and 05:25 UTC, every 30 minutes, and every waiting visitor runs their own probe (no singleflight). Four visitors held all four of production's threads for 130 s; `/healthz` did not answer within 30 s. The mechanism is older than section 53 (production's code took 255 s, then "No recent run found."). Author re-check: the same per-cycle HEAD loop on `app.HTTP` with timeout 10 in c8c7057. | FIX: the probe asks the archive for every candidate cycle first, then NOMADS only for cycles newer than the newest archive; probe HEADs one attempt with a short timeout, a host that times out is not asked again in that probe; one probe at a time (the others wait for it); a failed probe keeps serving the last run found (bounded age) instead of "No recent run found." |
| A P3-1 = B-3b | `layout_suspect` never clears, and every probe that sees another archive length adds one to `reads.layout` (no read behind it). | FIX: probe events counted apart from reads; the flag says whether the layout looks changed now, with the time of the last event. |
| A P3-2 | README (~966-970) and the probe docstring say an archive of another length means the bulletins come from NOMADS; in fact each member is still read and checked (an appended station leaves every existing member served from the archive). | FIX: wording. |
| A P3-3 | Test gaps (20 surviving mutants): the another-length probe test passes through NOMADS; yesterday's same cycle never tested; header prefix / name by suffix / larger size / decimal digits; Location by prefix; index header gap, alignment, size 0, 4 fields; an out-of-range slot and a long body in the R2 check; R2 403; NOMADS counted on an empty answer; the fork reset with a lock in flight; the index tool's `--check`, `main()`, stride, size-0 member. | FIX: tests for each. |
| A P3-4 | `_capped_get` does not compare `Content-Range`: a 206 of the right length from another offset reaches the header check (which refuses it, as `layout`). | FIX: a byte range must come back with the asked `Content-Range`; else `transport`. |
| A P3-5 | `tools/build_bull_index.py --check` on a run NOAA has not written yet dies with a traceback. | FIX: one line naming the run and the HTTP status, exit 1. |
| A P3-6 | `check_text` is a heuristic: a spliced body (the station's first lines, another's rows) would pass. Only S3 serving other bytes under the same key could produce it; every real member passed. | No change (recorded). |
| A P3-7 | Older than section 53, comment only: the GFS forecast cache key holds no run (a station's table can show the previous run up to 30 minutes after a switch, the GFS twin of K-2); `_live_after_fork` does not reset older in-flight dicts (no preload, so no fork after import); with `NOMADS_DISABLED=1` an unindexed station says "Could not download" (diagnostic mode). | No change (recorded). |
| B-3a | Any unknown station id typed into the address overwrites `bulletins.last_error`. | FIX: `unknown` is counted but does not replace the last error. |
| B-3c | An R2 403 is reported as `run_missing` ("not published yet"); the bucket answers 404 for a missing run, a 403 means the client was refused. | FIX: 404 = not published, 403 = an error with its detail. |
| B-3d | `winds.last_run` names the run of the last station read (after a SWAN back-fill: the older run). | FIX: the newest run served; listed in the README. |
| A note | `tests/test_cache_policy.py::test_explicit_public_headers_are_unchanged` (older) needs a live NDBC answer. | No change (recorded; outside section 53). |

### Fix round (step 7) scope
1. V5-1 (step 5): the job keeps 8 runs; "6-12 h" corrected in the README, `gfs_winds.py` and `app.py`; a test pinning the
   retention against the back-fill's age. A job-only change with its own production push.
2. B-1: the one-retry rule for a dropped connection on the archive and R2 winds reads; tests with a session that drops
   the first attempt.
3. B-2: the probe order, one-attempt short HEADs, stop asking a silent host, singleflight, the last run kept on a failed
   probe; tests for each (incl. 4 concurrent cold requests and a silent NOMADS).
4. A P3-1 / B-3b, A P3-2, A P3-4, A P3-5, B-3a, B-3c, B-3d as above, with tests.
5. A P3-3's tests; the reviewers' surviving mutants re-run.
6. Then the test site (MAX), a short fresh re-check (the archive read and the probe change), and step 8.
