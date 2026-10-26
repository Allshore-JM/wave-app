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

### Fix round (step 7), 2026-10-26

Every item of the scope above, on `feat/gfs-bulletins` (commit below). No template, static or client change.

| Finding | What changed |
|---|---|
| V5-1 | The GFS winds job keeps 8 runs (`tools/model_frames/gfs_winds.py` `KEEP`); the README's retention text and the site's comment on the 4 runs it holds say why (the SWAN back-fill reads the run of the SWAN table's first hour, up to about 30 h older than the newest). A test pins `KEEP >= 7` and the default prune. Job-only: it reaches production with its own push. |
| B-1 | `_capped_get(..., retry_dropped=True)`: a connection the server dropped before any answer (`RemoteDisconnected`, a reset, an abort or a broken pipe anywhere in the exception's chain) is asked once more on a new connection, inside the same wall-clock cap and only with at least 1 s left; a timeout, a refusal or any answer is never repeated. Used by the archive's range read and the two R2 winds reads; the probe's HEAD asks a dropped connection once more too. |
| B-2 | The probe asks the archive for each cycle, newest first, until one is there (a silent archive host is not asked again); NOMADS only for the cycles newer than that archive (all of them when no archive was found); a silent NOMADS is not asked again; a cycle younger than 3 h is not asked. Its HEADs go through their own session without the retry adapter, (3, 5) s. One probe at a time: the requests that arrive meanwhile wait for its answer. A failed probe keeps serving the last run found for up to 12 h and is asked again after 2 minutes. The fork reset re-creates the probe's lock. |
| A P3-1 / B-3b | The probe's verdict on the archive's size is kept apart from the member reads: `layout_suspect` (cleared by a probe that sees the index's size) and `probe_layouts`; a member refused by its header counts under `reads.layout` with `last_layout_read` (its UTC time and what differed). |
| A P3-2 | README and the probe's docstring: an archive of another length is still read member by member. |
| A P3-4 | A byte range must come back with the asked `Content-Range` (`bytes start-end/`), else the read is a transport failure. |
| A P3-5 | `tools/build_bull_index.py --check`: a run NOAA has not written prints one line with the run and the HTTP status; an unreadable member is printed and counted; exit 1 with the count. |
| B-3a | An id not in the index is counted under `unknown` and no longer replaces `last_error`. |
| B-3c | An R2 404 = not published yet (asked again after 2 minutes); any other failure, a 403 included, is an error with its detail (1 minute). |
| B-3d | `gfs.winds.last_run` is the newest run served (a back-fill read of an older run does not move it back). |
| A P3-3 | Tests for every listed gap (below). |

Tests added or changed:
- `tests/test_bull_sources.py`: a dropped keep-alive on the archive read asked once more and served (no NOMADS); only a dropped connection is repeated (timeouts, refusals, other errors once); no retry without the flag or with under 1 s left; a 206 from another offset or without `Content-Range` is a transport failure, not a layout one; NOMADS asked only for cycles newer than the archive (never for an older one); a silent NOMADS asked once per probe (also with three newer cycles); a silent archive asked once and NOMADS decides; the 3-hour rule at 17:40, 15:00 (the boundary), 14:59 and 02:00; yesterday's same cycle; a dropped probe HEAD asked again; the HEADs are one attempt with the short timeout on their own session; four concurrent cold requests = one probe; the last run kept through a failed probe, the 2-minute retry and the 12-hour bound; a forked child gets a fresh probe lock. The probe tests now count the exact HEADs.
- `tests/test_gfs_bulletins.py`: a header as tarfile writes it passes; a ustar prefix, a name matching only by suffix (both ways), `../`, a larger and a smaller size, a 9, a `+` and a `0o` in the size field and an 8 in the checksum are refused; the Location is compared whole (both ways); index documents with a member inside the previous one's header room, a half-block offset, size 0, four fields or a size over the cap are refused; `unknown` keeps `last_error`; the probe's verdict clears and counts; the tool refuses a size-0 member, records the one member size and stride when there are such, checks a run of the same layout (the six-sample rule), names a run of another length, names a run NOAA has not written (one line, exit 1), counts an unreadable member; its command line refuses a bad hour, a short run and a build from two runs without touching the network, and passes `--check` / `--out` on.
- `tests/test_gfs_winds_reader.py`: an out-of-range slot, a boolean slot and a longer body with its own digest are refused; a 403 is an error, not a run still to come; `last_run` stays on the newest; both objects are read with `retry_dropped`; NOMADS is counted only when it served wind; a forked child drops a run read in flight and reads it itself.
- `tests/model_frames/test_gfs_winds.py`: the runs kept cover the SWAN back-fill.
- `tests/conftest.py`: every test starts with an empty run cache and the probe's session refused (no test reaches NOAA).
- `tests/test_point_forecast.py`: the socket timer's pin allows the remaining time (the cap now runs from one deadline across the two attempts).

Results: full suite 1,504 passed (then the six touched files again, 281 passed). Mutants of the fix round on a scratch
copy: 44, 42 killed. One first survived and was a test gap: the dropped-HEAD retry (the probe test also passed
through NOMADS; it now has no NOMADS copy), killed after the change. Two are equivalent: removing
`RemoteDisconnected` from the dropped-connection list (it is itself a `ConnectionResetError`), and `len(m) < 3` in
the index check (a four-field member still fails the unpacking with a ValueError).

## Test-site verification of the fix round, 2026-10-26

`test` @ 22def43 (the cherry-pick of c4a2ceb; the README stays the test branch's own, the record and the job's own test
file are left off), live from 02:32:38 UTC, 42 polls across the switch, none failed; the new `/healthz` keys
(`probe_layouts`, `last_layout_read`) show the new code. Then `test` @ 254857d with the comment fix below, live from
02:48:36 UTC (42 polls, none failed; 51201, 46026 and 51201 SWAN then equal production's answers). The site files on `test` = `feat/gfs-bulletins` @ 4a425aa apart from the test branch's three logo
lines. Scripts and logs: scratch `g36/v7`.

- **Answers**: the 65 of step 5 (30 GFS stations compact, 6 classic, 5 Metric, 5 UTC, the 12 SWAN stations, 3 Great
  Lakes stations, 2 forecast points, 2 removed stations) on run 2026102518: production, the test site before and the test
  site after are identical, 65 of 65. The first request on the new instance (its run probe included) took 2.0 s.
- **The test branch's suite**: 1,108 passed and the five failures it had before (the three logo goldens, the models-env
  golden, the numpy import).
- **B-1 against NOAA's real bucket** (in-process, the feature branch's code; two sessions at once, archive range reads
  spaced 5.55-5.95 s, every body checked by its ustar header and its text): with `retry_dropped=False` (the reads before
  the fix) 9 of 45 failed with `RemoteDisconnected`; the site's own read (`_bull_range`) 45 of 45 ok, 16 of them asked
  again after a dropped connection. About half the reads at this spacing need a new connection either way (14 of 36 ok
  reads without the retry took over 0.85 s, 28 of 45 with it); a retry costs one new TLS connection (at most 1.42 s).
- **B-1 on the test site**: 80 fresh forecast builds (stations not built since the deploy), paced so the server's own
  connection to the bucket sat idle about 5-7 s between its archive reads: 80 answers ok, the archive reads went up by 80
  `ok`, 0 `transport`, 0 NOMADS fallbacks; a build took 1.10 s at the median, 2.10 s at most.
- **B-2 against NOAA's real servers** (in-process; "silent" = a local host that accepts and never answers; "pretend
  04:30" = the probe's clock at 2026-10-26 04:30 UTC, when 00Z is old enough to ask and its archive is not written yet):

  | Case | HEADs | Time | Result |
  |---|---|---|---|
  | now (02:36 UTC) | 1 (the 18Z archive) | 0.68 s | 2026102518 |
  | pretend 04:30 | 3 (archive 00Z 404, 18Z 200; NOMADS 00Z only, 404) | 1.10 s | 18Z |
  | pretend 04:30, NOMADS silent | 3 (one 5-s read timeout) | 5.66 s | 18Z |
  | pretend 04:30, the archive host silent | 3 (the archive asked once; NOMADS 00Z 404, 18Z 200) | 5.31 s | 18Z |
  | a good probe, then both hosts silent | 2 after the good probe's 1 (archive, then NOMADS: one timeout each) | 10.0 s | still 18Z, `failed` set, "still serving 20261025 18Z" logged |
  | pretend 04:30, NOMADS silent, 4 cold requests at once | 3, one probe | 5.98 s for each | 18Z for all four |

  The same cases on production's code (c8c7057): NOMADS silent took 253 s for 16 HEADs (8 cycles, 31 s for each NOMADS
  HEAD) and found no run ("No recent run found."); four cold requests ran four probes, 64 HEADs, 253 s for each caller,
  no run for any. (Production's code reads AWS's per-station files, gone since 2026-10-22: with NOMADS silent it has no
  GFS tables at all.)
- **Browser** (headless Chrome with its own profile, desktop 1280x800 and phone 375x812): 51201 and 46026 with 385 of 385
  rows carrying a wind speed and direction, the run line "GFS · run 20261025 18 UTC", 51201 SWAN with 175 of 175 rows
  with wind, no sideways page scroll, no transport failure, NOMADS bulletin fallback or NOMADS wind read during the
  smoke, console clean. (The script's two failures are its own expectation of 177 SWAN rows from step 5; since
  PacIOOS's 23:04 UTC update the table has 175, the same as production's answer.) `/healthz` `gfs.winds.last_run` is
  2026102518 with 2026102512 also held (before the deploy it read 2026102512 after a SWAN back-fill: B-3d).
- **Found and fixed**: the archive session's comment still said "one attempt each"; it now names the dropped-connection
  rule (4a425aa, comment only).
- **Not done**: the next run switch (00Z) was not watched (owner: report without it); the winds job's KEEP 8 reaches
  production with its own job-only push.
- **Cleanup**: the browser closed over DevTools and its profile removed; the test and production origins' storage
  emptied; the local silent host stopped.

## Re-check of the fix round, 2026-10-26

One fresh reviewer (Opus 5.5, effort MAX), 03:07-04:40 UTC, on `c9677fb..4a425aa` in its own detached worktree and on
the test site @ 254857d; report: scratch `g36/recheck/recheck-report.md`, its scripts and logs under
`g36/recheck/work`. The 00Z run switch was left out at the owner's request (its watcher stopped by its pid at 04:33).
**0 P0, 0 P1, 0 P2, 6 P3.**

What held up (the reviewer's own evidence): the full suite (1,506 passed); B-1 against NOAA's real bucket (archive
reads 5.45-6.05 s apart: 40 of 40 served with the retry, 34 of 40 without it; all 13 dropped connections
`RemoteDisconnected`, 5-122 ms after the request, a retry back in at most 1.36 s) and on the test site (30 paced fresh
builds, 0 transport failures, 0 NOMADS fallbacks); the probe on the real hosts (3 HEADs in 1.63 s now, 9 at most in
1.85 s; NOMADS answers **403** for a cycle it has not written, taken as "not there"); one probe for four waiting callers,
the kept run, the 3-hour rule; the failure-mode table on local servers (a close or a reset after the request and a
reset in the TLS handshake asked again; read and connect timeouts, a refused connection, `BadStatusLine`,
`SSLEOFError` and a body cut after the headers not); S3's real `Content-Range`; R2's 404 for a run not published;
`unknown` leaving `last_error` alone, live; KEEP 8 against the SWAN back-fill's real first row; 54 of 54 of its own
queries identical to production on the same runs.

| # | Finding | Outcome |
|---|---|---|
| F-1 | New with the fix round. The probe's HEADs left `app.HTTP`, whose adapter also retried 429/5xx statuses. Now one transient 5xx on the newest archive's HEAD is taken as "not there": the probe finds the previous cycle, NOMADS is asked only for the newer one, and when NOMADS cannot confirm it (off, refusing, silent) the probe succeeds with the older run, cached 30 minutes. Reviewer: real HTTP on a local stand-in, 18Z -> 12Z (the code before the fix round asked again and stayed on 18Z). Author: reproduced in-process (probe 2 -> 2026102512, `failed` False). | FIX: a 429/5xx answer to a probe HEAD is asked once more; in the archive loop only a 404 means "not there, try the older cycle", any other answer stops the archive scan (NOMADS decides; with nothing confirmed the probe fails and the last run is kept); in the NOMADS loop 403 and 404 mean "not there", any other answer stops asking NOMADS. |
| F-2 | The dropped-connection retry keeps the full connect and read timeouts, so a server that holds a request and then drops it takes the read past its cap (11.4 s for 8 s, 19.0 s for 15 s). Real S3 drops come 5-122 ms after the request. Author: reproduced (a 4-s cap took 5.24 s). | FIX: each attempt's connect and read timeouts clipped to the time left; the wording says what the cap covers. |
| F-3 = K-4 | Nothing bounds the probe as a whole and the waiting callers wait without a bound: a local host answering every HEAD after 4.8 s kept the probe and all four callers 76.9 s (153.9 s with drops); a HEAD whose headers trickle has no bound. Neither NOAA host behaves so today. | FIX: a probe budget (15 s: no HEAD started past it, each HEAD's timeouts clipped to it; out of time = what was found so far, else a failed probe); a caller that finds a probe running and holds a run younger than `_RUN_KEEP` gets that run at once; only a process with no run waits. |
| F-4 | Test gaps: 12 of the reviewer's 63 valid mutants survive and are not equivalent, five against the whole suite (D1 a reset no longer retried, D8 real timeouts retried, C6 the retry given a fresh full limit, P11 a 403 archive HEAD counted as found, W5 `_points_fetch` dropping `retry_dropped`); also D2/D3 (abort, broken pipe), R4 (`bytes 15360-` for a request from 1536), F3 (the stats lock after a fork), S3 (a `content` refusal setting `last_layout_read`), T2/T3 (the tool's `--check`). | FIX: a test for each, with the real exception chains urllib3 raises; the reviewer's survivors re-run. |
| F-5 | `tools/build_bull_index.py --check` still lets `http.client.IncompleteRead` (a cut body) and `ValueError` (a bad `Content-Length`) escape as tracebacks. | FIX: both caught at the HEAD and at a member. |
| F-6 | README: "each HEAD is one attempt" (a dropped HEAD is asked twice) and "within the same time limit" (F-2); `_probe_head`'s last `return` is unreachable; the record's "apart from the three logo lines" (`test` also differs in wsgi.py and the reef files: app.py, gfs_bulletins.py, the index and the tests are identical, so the measurements stand); `/healthz` does not say when the site serves a kept run. | FIX: the wording; the line removed; `/healthz` `gfs.run` (the run served, whether the last probe failed, the age of the last good probe). |

### Fix round 2 scope
1. F-1 with tests: a 503 archive HEAD once (the newest found) and twice (the run kept, NOMADS off); a 403 archive HEAD
   (the scan stops); NOMADS's 403 (not there) and 5xx (stop asking).
2. F-2 with a test: the second attempt's timeouts within the time left (fake clock).
3. F-3 with tests: the budget (no HEAD after it, the timeouts clipped, what was found is served); a caller that finds a
   probe running gets the held run at once; a process with no run waits for it.
4. F-4's tests; the reviewer's 12 survivors and new mutants re-run (scratch copy, after pytest).
5. F-5 and F-6.
Then the test site (MAX) and a short fresh check (MAX: the probe changes again), then step 8.

## Fix round 2, 2026-10-26

Opus 5.5, effort HIGH, on `feat/gfs-bulletins` @ e34ded1 (one commit over 2e00593: app.py, the bulletin tool, README,
three test files; no client file). Every item of the scope:

- **F-1** `_probe_head` asks a 429/500/502/503/504 answer once more (`_PROBE_RETRY_STATUS`). In the archive loop only a
  404 means "not there, try the older cycle"; any other status (a refusal, a 5xx asked twice, no answer) stops the
  archive scan, so an older run is never served as the newest. In the NOMADS loop 403 and 404 mean "not there"
  (NOMADS answers 403 for a cycle it has not written); anything else stops asking NOMADS for this probe. With nothing
  confirmed the probe fails and the kept run is served.
- **F-2** `_capped_get`'s second attempt after a dropped connection gets each part of its timeouts clipped to the time
  left (`_clip_timeout`: at most the time left, at least 0.1 s); the docstring and README say the cap covers both.
- **F-3** A probe budget `_PROBE_BUDGET_S` 15 s: no HEAD starts past it (the probe records `out of time` and serves
  what it found, else fails) and every HEAD's timeouts are clipped to it. `get_latest_run` takes the probe lock
  without blocking when it holds a run younger than `_RUN_KEEP` (`_held_run`): a caller that finds a probe running gets
  that run at once; only a process with no run (or one older than 12 h) waits for the probe.
- **F-4** Tests with urllib3's real exception chains for a reset, an abort and a broken pipe (asked again) and for
  connect and read timeouts (not asked again); the second attempt inside the time left (fake clock); the
  `Content-Range` prefix refused for a range of the same start and another end; a 403 archive HEAD stops the scan; the
  stats lock replaced in a forked child; a `content` refusal never sets `last_layout_read`; the tool surviving
  `IncompleteRead`, `ValueError` and `TimeoutError`; the R2 winds read retried end to end through the real
  `_points_fetch`.
- **F-5** `tools/build_bull_index.py`: `UNREADABLE = (URLError, OSError, http.client.HTTPException, ValueError)` at the
  HEAD and at every member: one line, no traceback.
- **F-6** README (the probe paragraph: the held run, one retry for a dropped connection or a 5xx, the 15-s budget, the
  404/403 rule; the dropped-connection bullet: the timeouts clipped); `_probe_head`'s unreachable `return` removed;
  `/healthz` gains `gfs.run` = `{run, failed, probe_age_s, good_run, good_age_s}` (the run served, whether the last
  probe failed, the age of the last good probe). The record's "apart from the three logo lines" wording stands
  corrected by F-6 above (the measured files are identical).

Tests: the full suite 1,528 passed (299.6 s). Mutants (scratch `g36/fix2/mut8.py`, on a scratch copy, after pytest):
25: the re-check's survivors (D1, D2, D3, D8, C6, P11, W5, R4, F3, S3, T2/T3 as one) plus C6b (the retry given a fresh
deadline) and the new code's 13 (N1-N13: the 5xx retry dropped, the archive/NOMADS stop rules widened or narrowed, no
budget in either loop, HEAD timeouts not clipped, callers always or never waiting, the held run ignoring `_RUN_KEEP`,
`/healthz` `failed` constant, 503 left out of the retry statuses, a 5xx asked again on the second attempt too). 24
killed on the first pass; N10 (the held run ignoring `_RUN_KEEP`) survived and is now pinned (a run older than 12 h
while a probe runs: the caller waits); 25 of 25 killed.
Re-runs on the fixed code: F-1 (a 503 on the newest archive's HEAD once: the newest found; twice with NOMADS off: the
probe fails and the kept run is served, 06Z never asked); F-2 (a 4-s cap with a held-then-dropped read: the second
attempt's read timeout is 2.5 s, not 10).

Next: the test site (MAX: a 503 stand-in for F-1, the budget and the held run against a slow local host, `/healthz`
`gfs.run`, the answers = production), a short fresh check (MAX: the probe changed again), then step 8 production and the
job-only KEEP 8 push.

## Test-site verification of fix round 2, 2026-10-26

`test` @ d5db21e (the cherry-pick of e34ded1; the README stays the test branch's own, the record is left off), live from
05:09:59 UTC: the old instance answered until 05:09:49, 40 polls across the switch, none failed; the new `/healthz` key
`gfs.run` shows the new code. The site files on `test` = `feat/gfs-bulletins` @ 536b4ea (app.py, gfs_bulletins.py, the
index, the tool and the tests compared blob for blob). Scripts and logs: scratch `g36/v8`. The "before" columns below
ran on the test branch at 254857d (the code after the first fix round), the same machine, minutes earlier.

- **Answers**: the 65 of step 5 (30 GFS stations compact, 6 classic, 5 Metric, 5 UTC, the 12 SWAN stations, 3 Great
  Lakes stations, 2 forecast points, 2 removed stations) on run 2026102518: production, the test site before and the test
  site after are identical, 65 of 65. The first request on the new instance (its run probe included) took 2.5 s.
  `/healthz` `gfs.run` then read `{run 2026102518, failed false, probe_age_s 59, good_run 2026102518, good_age_s 59}`.
- **The test branch's suite**: 1,130 passed and the five failures it had before (the three logo goldens, the models-env
  golden, the numpy import); the 22 new tests account for 1,108 -> 1,130.
- **F-1 with real HTTP** (in-process; a local server stands in for both NOAA hosts; the probe's clock at 2026-10-26
  04:30 UTC, so 26/00 is asked and not written (archive 404, NOMADS 403) and 25/18 and older are written; the cache holds
  25/18 from an earlier probe unless "nothing held"):

  | Case | Before | After |
  |---|---|---|
  | Baseline, NOMADS on | 18Z | 18Z |
  | Archive 18Z 503 once, NOMADS off | 12Z, as a success | 18Z (asked again) |
  | Archive 18Z 503 twice, NOMADS off | 12Z, as a success | 18Z kept, the probe failed; 12Z never asked |
  | Archive 18Z 503 twice, NOMADS on | 18Z (from NOMADS) | 18Z (NOMADS: 00Z 403 = not there, 18Z 200) |
  | Archive 18Z 503 twice, NOMADS 00Z 503 twice | 18Z (NOMADS 18Z) | 18Z kept, the probe failed |
  | Archive 18Z 429 once, NOMADS off | 12Z, as a success | 18Z (asked again) |
  | Archive 18Z 403, NOMADS off | 12Z, as a success | 18Z kept, the probe failed |
  | Archive 00Z 503 twice, NOMADS off | 18Z | 18Z kept, the probe failed (18Z not asked) |
  | Archive 18Z 503 twice, NOMADS off, nothing held | 12Z, as a success | no run; the probe failed (asked again after 2 min) |

- **F-2 with real HTTP** (a local server holds the first try and then drops it unanswered, then holds the second try):
  the archive read (cap 15 s; first try held 9 s) took 19.03 s before and 15.02 s after; the R2 read (cap 8 s; held 4 s)
  10.03 s before and 8.01 s after; a probe HEAD with 6 s of probe left (held 4.5 s) ends after 6.02 s.
- **F-3 with real HTTP** (a local server answering every HEAD after 4.8 s; four callers 50 ms apart; the probe's clock at
  2026-10-26 23:30 UTC, 8 cycles to ask; "held" = the cache holds 26/12, 1 hour old, due for a new probe):

  | Case | Before | After |
  |---|---|---|
  | Every HEAD slow, nothing held | 16 HEADs; every caller 76.8 s; no run | 4 HEADs; every caller 15.0 s; no run |
  | Every HEAD slow, a run held | 16 HEADs; every caller 76.8 s | 4 HEADs; the probing caller 15.0 s, the other three the held run in 0.0 s |
  | Every other connection dropped too, nothing held | 32 HEADs; 153.9 s | 4 HEADs; 15.0 s |
  | The same, a run held | (not run) | the probing caller 15.0 s, the other three 0.0 s |

  One HEAD whose header bytes trickle (six bytes 4 s apart) still takes 24.0 s, past the budget: a read timeout never
  fires on a trickle (the residual the re-check named; callers holding a run do not wait for it).
- **The probe on NOAA's real servers** (the new code): now 3 HEADs in 1.06 s (archive 00Z 404, 18Z 200; NOMADS 00Z 404);
  with the clock at 2026-10-26 23:30 (the most HEADs a probe makes against the real hosts) 9 HEADs in 2.12 s, 18Z.
  NOMADS answered 403 for the cycles it has not started and 404 for 00Z (its folder there, the file not yet).
- **B-1 against NOAA's real bucket** (the new code with the clipped retry; archive range reads 5.55-5.95 s apart): with
  `retry_dropped=False` 12 of 45 failed with `RemoteDisconnected`; the site's own read 45 of 45 ok, 13 asked again, the
  slowest 1.40 s.
- **B-1 on the test site**: 60 fresh forecast builds (stations not built since the deploy), paced so the server's own
  connection to the bucket sat idle about 5-7 s between its archive reads: 60 answers ok, the archive reads went up
  by 60 `ok`, 0 `transport`, 0 NOMADS fallbacks; a build took 1.17 s at the median, 1.86 s at most.
- **F-5**: `tools/build_bull_index.py --check` against a local stand-in: a body cut short, a `Content-Length` of "abc" and
  a reset connection each give one line ("not readable (...)") and the check ends "1 problem(s)", no traceback.
- **Browser** (headless Chrome with its own profile, desktop 1280x800 and phone 375x812): 35 of 35 checks: 51201 and
  46026 with 385 of 385 rows carrying a wind speed and direction, the run line "GFS · run 20261025 18 UTC", 51201 SWAN with
  175 of 175 rows with wind, no sideways page scroll; no transport failure, NOMADS bulletin fallback or NOMADS wind read
  during the smoke; console clean; no sign-in page, no account entry.
- **Not done**: the 00Z run switch. A watcher sampled the test site every minute from 05:14 to 05:21 UTC (51201, 46026,
  51201 SWAN, `/healthz` `gfs`): 2026102518 throughout, not failed, no transport failure or NOMADS fallback; NOAA's 00Z
  archive, spectra and NOMADS copy were all still 404 at 05:21. The test site would switch when its run cache expires
  (about 05:41); the owner prefers not to wait for run switches.
- **Observation for the owner (V8-1)**: the last F-1 case: on a fresh start (a deploy or a restart: nothing held), the
  newest archive answering a 5xx twice while NOMADS cannot confirm (off, refusing, silent) leaves the GFS tables on "No
  recent run found." until the next probe 2 minutes later. Before this round the previous cycle's run was served instead,
  as a success, for 30 minutes (that code never reached production); production's own code has no run at all whenever
  NOMADS is unavailable. A small option, if wanted: a process with nothing held serves the older archive it finds while
  the probe stays marked failed (asked again after 2 minutes).
- **Cleanup**: the browser closed over DevTools, its profile removed; the test and production origins' storage emptied;
  the local stand-ins ended with their scripts.

## Short fresh check of fix round 2, 2026-10-26

One fresh reviewer (Opus 5.5, effort MAX), 05:32-06:04 UTC, on `2e00593..42ec2d3` in its own detached worktrees (42ec2d3,
and 2e00593 for the "before" runs; both removed) and on the test site @ d5db21e across the 00Z run switch; report:
scratch `g36/fix2check/fix2check-report.md`, its scripts and logs under `g36/fix2check/work`. **0 P0, 0 P1, 0 P2, 4 P3.**

What held up (the reviewer's own evidence): the full suite (1,528 passed); on a local real-HTTP stand-in for both NOAA
hosts, every archive answer other than 404 (403, 301, 410, 501, a 5xx twice, two drops) stops the archive scan, nothing
older is asked and the held run is served, a 503 once finds the newest, NOMADS's 403/404 move on and its 503 twice stops
it; the old code (2e00593) served the older cycle as a success in every one of those cases; the retry stays inside its
cap (8.02 s for 8, 15.00 s for 15, 6.02 s with 6 s of probe left); the budget stops a slow probe at 15.0 s after 4
HEADs; four callers: a run held 1 h or 11.9 h -> the prober 15.0 s, the others the held run in 0.0 s, 12.1 h or nothing
held -> all wait; the tool against 14 raw-socket cases never printed a traceback; real exception chains from requests
2.32.3 / urllib3 2.4.0 have the tests' shapes; NOAA's real answers (00Z archive 200 at 05:27:54 with the index's size,
06Z 404, NOMADS 00Z 200, 06Z 403); the test site switched to 00Z by itself at 05:41:23 (3.0 s, 0 transport, 0 NOMADS
fallbacks, all wind from R2, `gfs.run` right before, at and after the switch); 49 of its own queries (30 GFS stations
outside the author's set, classic, Metric, UTC, SWAN, Great Lakes, a point, a removed station) identical to production
on 00Z, 42 at once and the other 7 once the test site's pre-switch cache entries (K-3) had expired; K-2 behaves as
recorded.

| # | Finding | Outcome |
|---|---|---|
| F1 | The new tests pin less than "25 of 25 mutants" suggests: of the reviewer's 55 mutants on the four bulletin test files 15 non-equivalent ones survive, and 14 of them applied together pass the whole suite (1,528). Unpinned: 429/500/502/504 in `_PROBE_RETRY_STATUS` (only 503 is tested); the probe's retry re-clipped to the time left; the archive taking a 301/400/410 and NOMADS any 3xx/4xx as "not there"; the `/healthz` `gfs.run` fields (the one test has value = good and failed = True); `_capped_get`'s retry body within the same `max_s`; the Content-Range end (near-equivalent: the length is checked). Author: nine of the same kind (429 or 504 not asked again, the retry's first clip kept, archive 301/400/410 and NOMADS 3xx/4xx as "not there", `failed` always true, `run` = `good_run`, the retry's body given a fresh `max_s`, the Content-Range end unchecked), each against the bulletin, wind and point-forecast test files (254 tests): all nine survive; all nine together pass the whole suite (1,528) | FIX (tests) |
| F2 | `_probe_head`'s second attempt (after a drop or a 429/5xx) has no deadline check, so a retry can start past the 15-s budget (with 0.1-s timeouts). Cost in practice: one HEAD of ~0.2 s after a slow connect; same outcome. Author: reproduced in-process (deadline 1 s behind, a 503 first: a second HEAD with (0.1, 0.1) whose 200 is taken). | FIX: no retry once the deadline has passed; a clocked test |
| F3 | Wording: README and both probe docstrings say "a 429 / 5xx" is asked again (only 429/500/502/503/504 are; a 501 stops at once); the README's "only a process that holds no run yet waits" leaves out a run older than 12 h; `/healthz` `gfs.run`'s "the run served is then the one kept" leaves out that `run` is null past 12 h; app.py's "as app.HTTP's adapter did" (that adapter retries twice, with a pause); the tool prints a garbage status line with its CRLF, so its "one line" is two (author: reproduced, "not readable on AWS (HELLO THERE\r\n)"); the record's description of the Content-Range test (a different start, 15360 vs 1536, not a different end). | FIX: the wording; the tool prints `str(exc).strip()` |
| F4 | Latent: the probe takes an archive 403 as a refusal (stop), the bulletin reader as "missing" (gfs_bulletins.py's docstring: "403: S3 for a key that is not there"). NOAA's bucket answers 404 for a missing key today (it allows public listing; the reviewer's HEAD on 06Z: 404). If it ever answered 403, every probe between 3 h and ~5.3 h after a cycle would stop at the unwritten newest archive and lean on NOMADS (K-2 for hours, for a process with nothing held and NOMADS down). Author: confirmed by reading both. | FIX: the comments made to agree (the probe's on purpose: a 403 can be a refusal of the site's address; the reader's counter only) |

Not checked by the reviewer: the stand-ins on Linux (they ran on Windows over plain HTTP); a real NOAA 5xx (none
occurred); the trickle case (K-1) and DNS stalls, which no read timeout bounds.

### Fix round 3 scope (proposed; the owner's call: this round, or production with these P3s on record)
1. F2: `_probe_head` asks once more only while the probe has time left (the same "at least 1 s left" rule as
   `_capped_get`, one shared constant); clocked tests: a 503 and a drop answered past the deadline make one HEAD only, and
   the archive scan then stops.
2. F1: tests that pin what the reviewer's survivors changed: the retry for each of 429/500/502/503/504 and none for a
   501; a first HEAD dropped late gives the retry the time left; archive 301 / 410 stop the scan, NOMADS 301 and a 500
   twice stop NOMADS; `/healthz` `gfs.run` with `failed` false, with the kept run expired (`run` null, `good_run` set) and
   before any probe; a retry whose body trickles past the original `max_s` is cut at it; a `Content-Range` of the same
   start and another end refused. The reviewer's 55 mutants re-run.
3. F3: the README's probe paragraph and `/healthz` line, both probe docstrings, the comment on `_PROBE_RETRY_STATUS`; the
   tool prints `str(exc).strip()`; this record corrects fix round 2's Content-Range sentence (above).
4. F4: the probe's comment says why an archive 403 stops the scan (S3 answers 403 for a missing key only without public
   listing; NOAA's bucket lists publicly and answers 404, so a 403 more likely refuses the site's address), the reader's
   docstring that its 403 = `missing` is a counter only (both outcomes fall back to NOMADS).
Then the test site (MAX: the reviewer's fake-clock probe edge and stand-ins against the site's code, `/healthz`, the
answers unchanged), then step 8 production and the job-only KEEP 8 push.

## Fix round 3, 2026-10-26

Opus 5.5, effort HIGH, on `feat/gfs-bulletins` @ 69a79bf (app.py, gfs_bulletins.py, tools/build_bull_index.py, README,
two test files; no client file). Every item of the scope:

- **F2** One rule for both retries: `_RETRY_MIN_LEFT_S` 1 s and `_time_left_to_retry(deadline)`. `_capped_get` uses it
  where it had the literal 1.0; `_probe_head` now asks a dropped HEAD or a 429 / 500 / 502 / 503 / 504 once more only
  while a second of the probe is left, so no HEAD starts past the budget.
- **F1** Tests for what the fresh check's survivors changed: the second ask for each of 429 / 500 / 502 / 503 / 504;
  an archive 301 / 400 / 410 / 501 stops the scan (nothing older asked, NOMADS off: the probe fails); NOMADS 301 / 410
  and a 500 twice stop NOMADS; a late drop gives the retry the time left (`(3, 5)` then `(2.0, 2.0)`); a drop or a 503
  with 0.5 s or less of the probe left is one HEAD, and through the probe a late 503 on the newest archive ends the
  scan; a retry's body that trickles past the first `max_s` is cut ("too slow"); a `Content-Range` with the same start
  and another end is refused; `/healthz` `gfs.run` after a good probe (each field from its own source), with the kept
  run expired (`run` null, `good_run` set) and before any probe (all null, `failed` false); the clip's 0.1-s floor and
  its cap.
- **F3** README (the probe paragraph: the five statuses, the second of time left, "no HEAD is started after 15 s", the
  12-hour waiters, the archive 403; the dropped-connections line; `gfs.run`: none past 12 hours, null before the first
  probe), both probe docstrings, the comment on `_PROBE_RETRY_STATUS`; the tool prints each error through `_why()` (the
  HTTP code, else the text with its line breaks folded), so a garbage status line is one line. Correction to "Fix round
  2" above: its Content-Range test is a range from another start (15360 for 1536), not another end; the new test above
  is the one with another end.
- **F4** The probe's archive loop says why only a 404 is "not there" (NOAA's bucket lists publicly and answers 404 for a
  missing key; a 403 more likely refuses the site's address); `archive_bulletin`'s docstring says its 403 = `missing` is
  a counter only (the caller asks NOMADS after `missing` and `transport` alike).

Tests: the two bulletin test files 155 passed; the full suite 1,552 passed. Mutants (scratch copies, never during a
full run): the author's 13 (`g36/fix3/mut10.py`: the fresh check's nine kinds plus this round's own: a 5xx or a drop
asked again without the time check, no minimum left, the tool's raw error) 13 killed; the reviewer's 55
(`g36/fix3/rmut/scripts/mutants.py`, its anchors moved to this round's lines) 53 killed; the 2 left are the two the
reviewer called equivalent (L5: `cycles[:None]` is every cycle; D9: the exception's args already carry every real
drop). Its K1/K2 (the clip's 0.1-s floor) needed the new clip test, its T4 a new anchor (the tool's print line moved).

Next: the test site (MAX: the reviewer's fake-clock probe edge and stand-ins against the site's code, `/healthz`, the
answers unchanged), then step 8 production and the job-only KEEP 8 push.

## Test-site verification of fix round 3, 2026-10-26

`test` @ 3054cb2 (the cherry-pick of 69a79bf; the README stays the test branch's own, the record is left off), live from
16:28:36 UTC: the old instance answered until 16:28:25, 40 polls across the switch, none failed. The site files on
`test` = `feat/gfs-bulletins` @ 2682693 (app.py, gfs_bulletins.py, the index, the tool and the tests compared blob for
blob). Scripts and logs: scratch `g36/v9`. "Before" = the test branch at d5db21e (fix round 2), the same machine,
minutes earlier.

- **Answers**: the 65 of step 5 on run 2026102606: production, the test site before and the test site after are
  identical, 65 of 65. The first request on the new instance (a cold start: its run probe, the first archive read, the
  first winds table) took 7.1 s (2.5 s at fix round 2's deploy); the other 64 a median of 0.76 s (0.64 s before the
  deploy; production 1.47 s). `/healthz` `gfs.run` then `{run 2026102606, failed false, probe_age_s 64, good_run
  2026102606, good_age_s 64}`; the archive reads 35 `ok`, 0 `transport`, 0 NOMADS fallbacks; winds 54 from R2, 0 from
  NOMADS.
- **F2, the probe's retry** (the fresh check's fake-clock script, `g36/fix2check/work/scripts/probe_edge.py`, and the
  author's `g36/fix2check/author/f2_tool.py`):

  | Case | Before | After |
  |---|---|---|
  | A 503 that comes 1 s past the budget | a second HEAD at +1.0 s with (0.1, 0.1); its 200 taken | one HEAD; the 503 stands |
  | A drop that comes 0.5 s past the budget | a second HEAD at +0.5 s | one HEAD; the drop stands |
  | A 503 with 0.2 s of the budget left | a second HEAD with (0.2, 0.2) | one HEAD (less than a second left) |
  | `_probe_head` called 1 s past its deadline, a 503 first | two HEADs, (200, ...) | one HEAD, (503, ...) |

- **F3, the tool's line**: a garbage status line at the HEAD printed "not readable on AWS (HELLO THERE\r\n)" (two lines)
  before, "not readable on AWS (HELLO THERE)" (one line) after.
- **Regression, local real-HTTP stand-ins on the new code**: the same answers as at fix round 2.
  - F1's nine cases (NOMADS off unless named):
    - one 503 or 429 on the newest archive is asked once more and its run served;
    - two 503s or a 403 on the newest archive, or two 503s on the newer cycle not yet written, fail the probe and serve
      the held run;
    - after two 503s on the archive, NOMADS confirms its run (NOMADS on); with NOMADS answering 503 twice too, the
      probe fails and the held run is served;
    - with nothing held, a failing probe serves nothing.
  - F2: the archive read is cut at 15.02 s (cap 15), the R2 read at 8.00 s (cap 8), the probe HEAD at 6.01 s (6 s
    left), each with a second connection after the drop.
  - F3: a slow 404, and a drop that ends in a 404, take 4 HEADs and end at 15.0 s; with a run held, the three other
    callers answer at once (0.0 s).
  - The reviewer's `standin.py` (`g36/fix2check/work/scripts`) against the site's code: all 72 result lines as at fix
    round 2, but for millisecond timings.
- **The probe on NOAA's real servers**: 06Z both times.
  - Now: 3 HEADs in 1.89 s (archive 12Z 404, 06Z 200; NOMADS 12Z 404).
  - With the clock set to 2026-10-26 23:30 UTC: 5 HEADs in 1.41 s (archive 18Z 404, 12Z 404, 06Z 200; NOMADS 18Z 403,
    12Z 404).
- **B-1 against NOAA's real bucket** (the shared retry rule): 45 range reads through a plain session (no retry) and 45
  through the site's `_bull_range`, side by side, with gaps past S3's idle drop.
  - Plain session: 32 of 45 (13 `RemoteDisconnected`).
  - The site's read: 45 of 45 (13 asked again, each with at least a second of its read left).
- **B-1 on the test site**: 30 fresh builds of stations not yet cached, paced past the keep-alive gap.
  - All 30 answered with wind; median 1.13 s, max 2.33 s.
  - `/healthz`: archive reads +32 `ok`, 0 `transport`, 0 NOMADS fallbacks.
- **Browser**: headless Chrome with its own profile, never signed in, at desktop 1280x800 and phone 375x812; 35 of 35
  checks passed.
  - 51201 and 46026 GFS: 385 rows, every one with wind; the run line names 06Z.
  - 51201 SWAN: 175 rows, every one with wind.
  - No sideways scroll.
  - During the smoke: no transport failure, no NOMADS bulletin or wind read, 0 console problems.
- **The test branch's suite**: 1,154 passed (fix round 2's 1,130 plus this round's 24 new tests), and the five failures
  it had before (the three logo goldens, the models-env golden, the numpy import).
- **Cleanup**: Chrome closed over DevTools (no process left; sign-in pages 0, account entries 0) and its profile
  removed. The storage of the test and production origins emptied. The local stand-ins ended with their scripts.

Findings: none. KEEP 8 (the job part of c4a2ceb) is in `feat/gfs-bulletins`, and production's head c8c7057 is an
ancestor of it, so the step-8 fast-forward carries the job change: no separate job-only push is needed.

Next: step 8, production, after the owner's go-ahead. Tag `prod-pre-bulls` @ c8c7057, then fast-forward
`Live-Buoy-Update` to `feat/gfs-bulletins`. Check the page probes, the tables unchanged on the same run, `/healthz`
`gfs`, CI, and the next winds publish keeping 8 runs. NOMADS stays enabled.
