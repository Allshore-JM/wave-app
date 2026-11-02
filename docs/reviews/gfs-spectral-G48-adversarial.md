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

## G48: the site and page (two fresh reviewers on 94ec35b / test 5ce07a3, 2026-11-02 03:45-04:30 UTC)

Reviewer A (code, no browser; model Fable, effort max; report in the session's scratch `g48/a/g48-a-report.md`, its
harness `g48/a/harness/`, 70 mutants over a detached copy) and reviewer B (the test site with trusted input through
DevTools, desktop 1280x800 / iPad 1024x1366 / phone 375x812; report `g48/b/g48-b-report.md`, scripts b1-b9, captures
`cap/`, screenshots `shots/`). **Counts: P0 0, P1 0, P2 3, P3 9, P4 3** (A 0/0/1/6/3, B 0/0/2/3; B's P3-2 = A's P3-5; B's P3-3 came with its final report).

### Findings and outcomes

| # | Finding | Outcome |
|---|---|---|
| A P2-1 | two pins in tests/test_point_forecast.py failed at 94ec35b (the rank_rows call count; the station answer's key set gains `gfsb_available`): the step-5 "suite green" claim was wrong | FIXED (the two pins updated; the full suite run again below) |
| A P3-1 | across a run switch the fallback served NOAA's split of the OLD run under "(ours arrives shortly)": parse_bull's own entry carries no run and lives 30 min (a "GFS (NOAA split)" press within the previous half hour at that station) | FIXED: `_drop_old_bull` drops parse_bull's entry when its cycle run is behind the site's, before the fallback build and before the GFSB dispatch; pinned by a test that moves the site's run and asks the button first |
| A P3-2 | a pointer older than the two held runs was fetched in full (11 MB) at every pointer read and dropped at once | FIXED: a run behind the held ones is not fetched (one log line); the pointer-back pin asks for the pointer alone |
| A P3-3 | the "GFS (NOAA split)" table ignored the 48-h wind rule (wind_complete False for an old run with blank wind: a 5-min retry) | FIXED: GFSB in the rule's tuple; a payload-level test for GFS and GFSB at 49 h and 10 h |
| A P3-4 | /api/forecast's exception answer omitted `gfsb_available`, so the page hid the button after a later network failure | FIXED: the answer carries `gfsb_offered(station)` |
| A P3-5 | "(ours arrives shortly)" could be permanent: a station the held run lacks (the reads succeed, `failing_since` never set), and GFS_SPECTRAL=1 without a frames root (the button offered with nothing behind it) | FIXED: the note is plain when the newest held run is the site's or newer and lacks the station; `gfsb_offered` needs the frames root; both pinned |
| A P3-6 | the 60-s fallback TTL, the fallback key's (S, R) and 13 more rules survived mutation (no test pinned them) | FIXED: tests for every real gap (below) |
| A P4-1 | flag-off answers are not byte-identical: every station answer gains `gfsb_available: false`; a 48-h-old GFS run with blank wind answers wind_complete True / ttl 1800 (was False / 300) | RECORDED in the README (one added key; the wind rule reaching the bulletin path is the intended C4) |
| A P4-2 | after a failed FIRST load at a lake or unlisted station the page showed the button the server had hidden (`syncBar` fell back to the flag rule) | FIXED: `lastAvail` seeded from the page's initial state; pinned (a lake station whose first load fails keeps the bar hidden) |
| A P4-3 | the fallback's key must never be parse_bull's (the build calls parse_bull under the key's lock: the same key deadlocks every GFS request thread), unstated | FIXED: a comment at the key |
| B P2-1 | render=full showed the "GFS (NOAA split)" button at Great Lakes and retired stations: the page's seed dropped `gfsb_available`, and `syncBar` fell back to the flag rule | FIXED: the seed forwards `gfsb_available` (and the two below); pinned on the inline path at 45007 |
| B P2-2 | render=full with scripts on lost the "Swell split: NOAA's ..." note and kept NOAA's table 10 min (the seed dropped `split` / `split_note`, so `ttlOf` gave the 10-min life) | FIXED: the seed forwards `split` and `split_note`; pinned (the note in #fwOld; the seed of a substituted split re-asked after a minute) |
| B P3-1 | on a phone the header's "GFS (NOAA split) · run ..." truncates to "GFS (...", one character from "GFS ·..." (the station field takes the width; the pressed button below and the title attribute carry the full text) | RECORDED (owner awareness; a short header form on phones is a later refinement if wanted) |
| B P3-2 | a station the job leaves out of a run was served NOAA's split with "(ours arrives shortly)" for ever | = A P3-5, FIXED |
| B P3-3 | for the 00Z and 06Z runs the bulletin writes the cycle hour as `%2d` (`20261102  0 UTC`), the spectral table as `00`; `_GFS_CYCLE_RE` and the page's `pageCycle` want two digits, so neither matches the bulletin's form (the 48-h wind clause never applied to a bulletin table of a 00Z / 06Z run; pageCycle's miss is pre-existing: the overlay's run tail never showed for those runs) | FIXED in round 2 (below): both regexes accept a one- or two-digit hour; the spellings stay as each source writes them |

### The tests added for reviewer A's 16 surviving mutants

tests/test_gfs_spectral_site.py: the new site run served at once and the note judged again within a minute of the
failing clock passing half an hour (ttl_1800, ttl_none, fallback_key_no_run, the P3-1 corner); one station in two zones
(spectral_key_no_tz); the flag holds the scheduler read even when the pointer is due (flag_ignored_root); a stale NWPS /
SWAN table falls to our split with its keys (nwps_stale_to_bull, swan_stale_to_bull); the payload-level 48-h wind rule
for GFS and GFSB (wind_complete_no_gfs); parts stored out of power order ranked on the way out (rows_unranked).
tests/test_index_page.py: the bar at a plain GFS station (shown for the new button alone with the flag on, hidden with
it off: tpl_bar_hidden_rule); render=full's meta-line note and state keys (note_not_on_page, state_without_split).
tests/ui/window.test.js: the bar follows the answer's `gfsb_available` and remembers it through a failed load; the
first judgement is the server's (syncbar_lastavail_no_gfsb, syncbar_gfsb_from_flag_only, syncbar_gfsb_known_ignored,
A P4-2); `ttlOf` keeps NOAA's split asked for (GFSB, no note) the usual 10 minutes and a substituted split a minute
(ttl_any_noaa); the render=full seed (B P2-1 / P2-2).

### Confirmed by the reviewers (nothing re-measured by the author)

A: the fallback matrix (15 cases: S = R, S > R, S held with R unknown -> ours; S < R by one cycle -> NOAA's split +
"shortly", by two -> plain; nothing held -> "shortly" until the reads have failed 30 min; 25 damaged indexes and 9
damaged bodies / pointers refused, nothing held, the good objects held afterwards); the background read never on a
request thread (23 ms with the body blocked), one read at a time, four concurrent requests = one build; the model
matrix, 192 server cases and 240 page cases (the head's early query = the loader's; `resolve_model` = the payload =
the state); C4's 48-h clause; the 385 x 23 rows, the Location line read back by the sky's parser; the flag off = 06cee72
on the same fake inputs (80 answers; the only difference the added key); `_parse_gfs_spectral_uncached` 12.6 ms per
station, the refresh 10 ms at the product's size; the bucket's index (723 stations = station_list.json, 278,355 steps).
B: 65 answers production vs test on run 2026110118 (the 19 SWAN / GLWU / point / removed answers differ only by the new
key; the 46 GFS answers in the swell columns; test's 48 GFSB answers = production's GFS answers exactly); ours vs NOAA's
row by row at 40 stations x 385 rows (date, time, wind and sun / moon cells identical in all 15,400 rows; the combined
height within 0.04 ft; the Summary's sun / moon cells identical in 667 day rows); the owner's use at 51201 and 15
stations on three sizes (the bar per station kind, one request per press / link, Graph / Summary / Metric / UTC / the
live-buoy link under both splits, render=full, the no-script select); the overlay's run line under both; every failure
path (the pointer / body refused, a wrong sha, a held body: NOAA's split + the note, /healthz naming the error, recovery
within the negative TTL + the 60-s key; a 503, a late answer never replacing a newer press); a11y (a native button in
the Model group, Tab / Space / Enter, contrast 4.69:1 / 4.48:1); answer times (GFS prod 0.32 s / test 0.38 s; a fresh
spectral answer 6-9 ms after the winds table's first read).

### The fix round's checks

- Suites: pytest 2,228 passed, 1 skipped, with only the four golden tests failing before the re-capture (16 flag tests
  pass after it); Node UI 572 (+2), overlay 500. The page golden re-captured in its own commit (the UI version line;
  the template unchanged). Commits b201c28 (the fix round, UI 1.28.2), 0477608 (the golden), c003ab3 (three more pins).
- Mutants of the fix round (scratch g48/fix/mut_b.py on a detached copy): 18 applied (the seed's keys, the initial
  lastAvail, the drop of the old bulletin entry on both paths, the pointer-back rule, GFSB in the wind rule, the flag's
  hold on the root, the lag rule, the fallback key's (S, R) and 60-s life, the spectral key's zone, rank_rows, the stale
  NWPS fall-through, the template's hidden rule, the state's split_note, the older-run note case, the frames root, the
  exception answer's key): 15 killed at once, 3 survived as real gaps and were pinned (c003ab3): 18 of 18 killed.
- The 00Z lag window on the test site (K3; reviewer B's out/lag.log and the author's scratch spec77/v5/lag.log, one request
  every 30 s): the site's run moved from 2026110118 to 2026110200 between 05:35:39 and 05:36:10 UTC; from then every GFS
  answer was NOAA's split OF THE NEW RUN (cycle `20261102  0 UTC`) with "Swell split: NOAA's (ours arrives shortly)" (the GFS
  and GFSB rows 385/385 identical at 51201 and 46026; never the 18Z spectral table); the job published 00Z at 05:42:27; the
  site held both runs and served ours from 05:42:59 (B) / by the author's 05:43:19 poll; the window lasted under 7 minutes,
  labelled throughout; RSS 265 -> 293 MB with the second run held. Reproduced locally with the stand-in's pointer one cycle
  behind (B's b7.log).
- The test site after the fixes (test @ 956286f = the two code commits cherry-picked, README and golden the test
  branch's own; UI 1.28.2 served from 04:48:44 UTC, the new instance holding run 2026110118 17 s after its start):
  reviewer B's own scripts re-run from the author's harness (headless Chrome 156, own profile; scratch spec77/v5
  out/b2b.log, out/b9.log): B P2-1 fixed, the bar HIDDEN with every button hidden throughout the first 5-12 s on
  `?station=45007&model=GFSB&render=full`, `45007&render=full`, `44062&render=full`, `IndDunesNP&render=full` (0 API
  requests: the inline path) and on the plain JS path at 45007 (4 s, 0 flashes); A P4-2 / B's second exposure fixed:
  the first answer at 45007 refused with a 503 (Fetch interception) leaves the bar hidden beside "Could not load the
  forecast. Retry", and Retry's GLWU answer (150 rows) keeps it hidden; no console problems. B P2-2 (the note on a
  render=full page with scripts on) is pinned by the Node test; the server's half (the meta line + the state keys)
  by the page test; live it needs a substituted split, i.e. the lag window (below).

## Round 2: the owner's two comments on the test site (2026-11-01/02)

The owner, looking at the test site after the fix round: "The merged split is not acceptable, this is only slightly better
than the NOAA Split, and not representative of the initial results presented in the investigation tests. We need to improve
this to assure a correct split of swells." And: rename the button from "GFS (NOAA split)" to "GFS (Bulletin)".

### The rename (854cccc, golden e693a78; UI 1.28.3)
The button, the header ("GFS (Bulletin) · run ..."), the no-script option, the title text and the README say
"GFS (Bulletin)"; the internal model id stays GFSB (addresses with `?model=GFSB` keep working). This record keeps the
reviewers' wording ("GFS (NOAA split)") where it quotes their reports.

### K1 diagnosed: a broad sea on a swell's flank has no peak of its own
At the merged hours of run 2026110118 at Waimea (Mon 2 Nov 10 AM and 2-7 PM HST) the raw, unsmoothed watershed finds ONE
maximum: the NE sea (8-9.5 s, from 5-25 deg) is a broad plateau whose ridge rises monotonically into the NW groundswell's
flank (57 -> 59 -> 61 -> 67 -> 68 ... -> 405 at 14.5 s / 325 deg), while at the split hours a 3 % dip (70 vs 68 at 10.4 s /
335 deg) gives the second peak. No merge rule of the partitioner is to blame; the knife edge is sub-percent. The 76e
page had shown the same run's neighbour (12Z) split throughout: the edge lies on other runs' hours.

Rejected: f^k weighting applied to the whole detection field (k = 2, 3, 4): it finds the sea at every hour but splits
wind-sea tails (4-5 s pieces) and swell tails (10.7 s at 332 deg) everywhere: extra systems per hour 0.16 -> 0.34-0.52,
flicker 0.198 -> 0.215-0.307.

### The shoulder split (rules v2.1; partition2d.py `_shoulder_split`)
After the merges, inside each piece of at least 2 x sub_min_hs (0.6 m) the watershed is run again on Es x f^3 (the
equilibrium range flattened, so a shorter-period hump stands out); parts under 0.3 m or under 20 % of the piece's energy
fold into the touching part nearest in (direction / 50 deg + ln-period / 0.2); then the least distinct touching pair
merges until every pair is distinct, where distinct = the peak bins at least 50 deg AND 0.2 in ln(f) apart, vetoed when
both parts lie at least 30 % inside the wind-sea region (one young sea turning with the wind); the biggest part keeps the
piece's label, the others get new ones, and the piece tables are rebuilt before the wind-sea join. Settings: sub_fpow 3,
sub_dmax 50, sub_lnr 0.2, sub_min_hs 0.3, sub_min_share 0.2, sub_ws_share 0.3 (variant P3d50s20 of the K1 study,
scratch swell76/k1study.py; the "or 100 deg apart" clause of the first draft decided 0 of 9,240 real hours and was removed).

Measured on NOAA's spectra against NOAA's bulletins (hours with a distinct pair = two shown systems at least 40 deg and
2 s apart; kept = NOAA's systems of 0.5 m+ matched; extra/h = our systems of 0.5 m+ NOAA lacks; ph = systems per hour;
flicker = systems appearing or disappearing per hour):
| set | hours with a pair: v2 / v2.1 / NOAA | kept | extra/h v2 -> v2.1 | ph v2 -> v2.1 (NOAA) | flicker v2 -> v2.1 (NOAA) |
|---|---|---|---|---|---|
| runs 12Z + 18Z of 11-01, 8 stations (16 station-runs, 6,160 h) | 4,802 / 4,877 / 4,661 | 0.995 / 0.995 | 0.16 -> 0.20 | 2.85 -> 2.89 (2.59) | 0.198 -> 0.207 (0.184) |
| run 00Z of 11-02, 8 stations (3,080 h) | 2,097 / 2,190 / 1,944 | 0.995 / 0.995 | 0.15 -> 0.22 | - | 0.196 -> 0.209 |
Waimea's 18Z run: all 385 hours hold a distinct pair (v2: 374; NOAA 241). The K1 hour 20:00Z reads 2.19 m 9.0 s from
16 (wind sea) + 1.79 m 14.5 s from 329 (v2: one 2.82 m 14.5 s from 355). A sample of the rule's own extra pairs (41025
10.1 s / 66 + 4.9 s / 12 under an 8 m/s NNW wind; 51201 15 s / 347 + 5.5-6 s / 40 trade sea) reads as physically genuine.
One-hour pair toggles: +3 at 41025 12Z only. Each clause measured on the 9,240 real hours of the three runs (scratch
spec77/k1clauses.py): the wind-sea veto decides 131 hours, the nearest-neighbour fold 221, the 2 x sub_min_hs gate 10,
the far-direction clause 0 (removed).

The whole run rebuilt locally (the job's `--dry-run`, 723 stations, 4 workers of 12: 463 s; the v2 build took 278 s here
and 350 s on the runner, so ~580 s is expected there, inside the 25-min budget) against the bucket's published v2 binary
of 2026110118: 10,451 of 278,355 station-hours differ (3.75 %), 9,777 with more parts, 1 with fewer, 673 reordered; the
total height never differs; 576 of 723 stations change somewhere (most at V14011, 46206, 46139: 90-96 hours each).
Waimea's +26 h and +30..+35 h now show the NW groundswell beside the NE sea, each between its neighbours' values.

Parity and speed: the repo port equals the scratch rows at 6,148 of 6,160 hours (the 12 differ only by a 0.300-m rounding
at the 0.3-m floor); 3.1 ms median per spectrum (p90 4.3, max 10.3; the scratch 14 ms) with the piece tables updated
incrementally after a merge.

### Tests and mutants
tests/model_frames/test_partition2d_v2.py: the constants pin the sub_* settings and rules v2.1; the K1 synthetic case
(a tall 14-s swell with a broad 9-s sea on its flank: one part on the plain watershed at 333 deg, two with the split);
Waimea's merged hour (+26 h) apart like its neighbours on the real member fixture gfswave_51201_member3.spec (hour 0 +
hours 25-27 of 2026110118); one wind sea in two humps stays one, a swell's own tail never splits off, the split off with
sub_fpow 0, determinism; and the mutant pins on the real fixture gfswave_51201_k1pins.spec (hour 0 + 33, 184, 185):
the fold to the NEAREST part (+33 h: folded into the biggest the sea would read 1.55 m from 13 and the swell 1.96 m
from 345), the wind-sea veto (+184/185 h, and a synthetic pair 60 deg apart under one wind), the share floor and the
height floor each on its own, the 2 x sub_min_hs gate. The job's pinned-rows test covers both fixtures through
gfsspec_expect.json (regenerated locally: NOAA's archive no longer holds the 13130 run, so the capture script's network
mode was not usable; the existing members' rows are unchanged under v2.1). test_ww3spec accepts a 4-step member;
test_gfs_spectral's pinned-rows test asks for the member's own hour set. Mutants of the split (scratch spec77/k1mut.py,
on a scratch copy): 18, 17 killed; M19 (the new labels overwrite the kept part's too) is equivalent (only label numbers
change). Job suite: see the round-2 checks.
