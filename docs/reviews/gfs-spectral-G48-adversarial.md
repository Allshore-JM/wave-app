# Section 77: "GFS Spectral", our swell split as the default GFS table — reviews (G48a job, G48 site)

Branch `feat/gfs-spectral` (off production 93cd22f). The job (`tools/model_frames/gfs_spectral.py`, `ww3spec.py`,
`partition2d.partition_v2`) publishes `gfswave/spectral/v1/<RUN>/spectral.bin` + `index.json` + the pointer; the site
reads it in the background and serves our split as the GFS table, NOAA's own split as the "GFS (NOAA split)" button.

## Step 2: the job's dry run on the runner (2026-11-01 UTC)

A temporary push-triggered workflow (removed before the merge; a test pins its absence) built run 2026110112 on a
GitHub runner from the real archive: success, 6 min 5 s of wall, the build 348.7 s on `nproc` 4 with 4 workers; 723
listed stations, 0 left out; `spectral.bin` sha256 ffa03e63ada27ce247ac98226d1153bb9a32e8f77bcb8b0817e9e706a044a3a7 =
the local build's; the nine sampled station digests equal (13130, 23173, 41036, 42002, 42089, 44029, 44251, 46066,
46138). Max RSS not readable from the API (the step summary is private); the local build of the same run peaks under
1 GB. So the plan's budget (25 min on 4 cores) holds with room; the speed pass was not needed.

## G48a: one fresh reviewer on the job and reader (b9236e0; report in the session's scratch `g48a/`)

**0 P0, 0 P1, 1 P2, 6 P3.** NOAA: two ranged GETs of the bulletin archive. The reviewer's own line-based reader equals
`ww3spec.read` on both fixtures (energies at rtol 1e-12); the total height = the bulletins' Hst within 0.5 % over 46
hours (a transposed reshape misses by over 3.6 %); the DIRECTION convention settled against NOAA's own bulletin for
51201 (our 326.1 and 42.1 deg = the bulletin's 146 and 222 + 180: the bulletin's column is the propagation direction,
which the site's parser also turns round; 70 matched parts at 13130 within -7..+4 deg); the grid kept in the file's
order. `partition_v2` = the scratch split of section 76e on 46 fixture hours and 300 perturbed real spectra (0 differ);
its constants = the measured settings. The bounded pool: the stream never ran more than the cap ahead (5 members with
cap 4 and every worker blocked); a real spawn Pool on a 510-member local archive gave the same bytes with 1 and 4
workers. Publish order, no-regress, already-live, prune confined to the prefix, the failure / notready records and the
stale alarm as designed; the winds job's output byte-identical after the factoring (the pinned sha, and a real-member
archive); 2,000 random records round-trip; 1,500 fuzzed members raise only ValueError (worst 11 ms); 32 MB floods
refused within 0.7 s. 60 mutants: 44 killed, 5 equivalent, 11 gaps (below).

| # | finding | outcome |
|---|---|---|
| P2-1 | The build lacked the winds job's "members start at the run's hour" guard: an EARLIER run's archive under this run's name was accepted (379 of 385 steps inside the window) and would have been published, immutable, under the newer run's name. | FIXED @ 957a31d: the worker reports the member's first step time; more than a tenth of the listed members off the run's hour fails the build (a later, an earlier and a one-member case pinned). |
| P3-1 | A worker that dies (OOM kill, segfault) answers neither callback: the build waited out the whole 25-min budget, every tick. | FIXED: a task with no answer within TASK_MAX_S (300 s) of its turn is that station's failure (left out); a lost task terminates the pool (its entry never leaves the pool's cache: join() would hang, found by the new test). |
| P3-2 | The feeder's wait for a slot had no deadline: hung tasks filling the cap held it past the budget. | FIXED: the wait honours the deadline (BudgetExceeded). |
| P3-3 | `members()` refuses a duplicate of ANY member; the old winds build refused only a twin with wind. | Recorded in a comment (no real archive carries a twin; the product bytes of a plain archive are pinned unchanged). |
| P3-4 | The fixture's "parts" are made by `partition_v2` itself: the in-repo pin is a regression pin. | Noted in the capture script; the independent evidence is the reviewer's run above. |
| P3-5 | Reader test gaps (a scrambled grid, bad frequencies, the grid tolerance). | FIXED: five more hostile cases. |
| P3-6 | Job test gaps (`--run` on a published / newer / older run, the stale alarm on the not-now paths, drain's deadline, a direction rounding to 360, the pass rule, the 0.05 m boundary). | FIXED: one test each. |

Mutants of the fix round (a scratch copy; the three job suites; the session's scratch `spec77/mut_fix.py`): 18, 16
killed (the lost-task join by its hang, as designed), 2 equivalent: the cover-once check (a sequence of nd nodes
stepping by one each covers the circle once: the order check holds it) and the count check's `<` (an extra value makes
the reshape raise the same ValueError). The reviewer's survivors R10 / R11 / R13 / R14 / P3 / F2 / F8 / B6 / U5 / U6 /
U7 are the killed ones above.

## Step 4: the site and page (725a310, golden 237c242)

Built as planned, with these deviations from the plan text (measured, better):
- The spectral table is served whenever the held spectral run S >= the site's run R, or the site has no run at all
  (then ours is the only table); NOAA's split (the bulletin path's own cached answer, wrapped under its own key for
  60 s) when S is behind R, with "Swell split: NOAA's (ours arrives shortly)" while the lag is one cycle (or nothing is
  held and the reads have not failed for 30 min), else "Swell split: NOAA's".
- The body (16 MB cap) is fetched by one daemon thread from `_live_tick`; a request thread never waits on it; a slow
  bucket leaves every request fast (pinned).
- `_old_run_wind_final` gains the GFS clause (a run 48 h or older: blank early wind cells are final), so the NWPS
  suite's old pin ("a GFS table: unchanged") now asserts the final state at 49 h and the retry at 10 h.
- The page caches an answer carrying NOAA's split with a note for 60 s only (the server re-judges the run comparison
  every minute), so ours shows within a minute of landing.

## Step 3: the job-only merge (2026-11-02 02:43 UTC)

Tag `prod-pre-spectral-job` @ 93cd22f; production fast-forwarded to 06cee72 (`job/gfsspec`: 23 job / test / workflow / doc
files, no site file). Render restarted; the three page probes byte-identical to the pre-merge capture; model-frames-tests
green; the trigger Worker's 02:56 tick dispatched all seven workflows, including the first model-gfsspec run (success, ~6 min).
First publish 03:02:15 UTC, run 2026110118: 723 stations, 0 left out, nproc 4, build 349.8 s, 11,134,200 B; the bucket's
spectral.bin byte-identical to a local build of the same run with the production code (sha 17f7be7e...), the station map
and ten sampled records identical; the index / binary immutable with CORS for the site; the winds product untouched.

## Step 5: the test site with GFS_SPECTRAL=1 (2026-11-02 03:06-03:35 UTC)

`test` @ 5ce07a3 (the site commits + the five job modules the site tests import); GFS_SPECTRAL=1 on the test service from
03:23 UTC, run 2026110118 held 30 s after the start. Scratch spec77/v5.

- 65 answers production vs test on the same run: the 19 SWAN / GLWU / point / removed answers differ only by the new key
  `gfsb_available` (true at 51201's SWAN answer, a GFS station; false at points and GLWU); the 46 GFS answers differ in the
  swell columns (table_html, graph_data, summary_html), `split` "spectral", `gfsb_available` true; three also in
  graph_header.location (the spectral Location line "31052 (8.15S 34.57W)" against the bulletin's padded "31052      ( 8.15S
  34.57W)"; the page collapses the spaces). Test's 48 GFSB answers equal production's GFS answers exactly (the new keys
  aside); GFSB at a Great Lakes station answers GLWU, at a point GFS.
- Independent rows check (rows_check2.py: the bucket's spectral.bin decoded with the job's own decoder, feet, FROM, ranked by
  Hs^2 x Tp, six at most, against graph_data per hour): 35 answers (30 compact + 5 UTC) x 385 hours, 0 mismatches in any
  swell height / period / direction or the combined height; the Wind columns equal the GFSB answer's cell for cell; the
  combined height within 0.04 ft of NOAA's Hst everywhere; the 5 Metric answers equal the feet answers converted (0.002 m).
- Browser (headless Chrome 155, own profile, trusted clicks / taps; desktop 1280x800 + phone 375x812): 51201 default = the
  spectral rows, the bar "GFS* | GFS (NOAA split) | SWAN", one request; the button -> "GFS (NOAA split) · run ...",
  ?model=GFSB in the address, one request, NOAA's rows; SWAN and back; 46026 "GFS* | GFS (NOAA split) | NWPS"; a point and
  45007 (GLWU) show no bar; the links ?station=51201&model=GFSB and ?station=46001&model=GFSB open NOAA's split with one
  request (the head's query = the loader's); render=full GFSB 385 rows; no sideways scroll on the phone; console clean.
- /healthz gfs.spectral: enabled, pointer 2026110118, held [2026110118], 723 stations, no error.
- Pending at the time of writing: the 00Z run switch (the lag window: NOAA's split with "ours arrives shortly", then ours
  within 60 s of the publish) and the second published run against a local build; recorded below when seen.
