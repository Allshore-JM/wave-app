# Section 59: NWPS forecast tables (G41a job, G41 site)

Plan: `swirling-spinning-snowflake.md`, section 59. Branch `feat/nwps-tables` off `Live-Buoy-Update` @ d008d98.
NOAA NWPS (the NWS offices' nearshore SWAN) publishes hourly 2-D wave spectra to +144 h at named buoys; the tables are
built from them by our own swell partitioning and chosen with a model button, like the PacIOOS SWAN tables.

## Step 1: the station set, the spectral-file reader and the partitioner (2026-10-28, Fable 5.1 at HIGH)

Commits f892aa7 (the pieces) and b653932 (the pins). Scratch: `nwps/` (survey.json, grids.json, spectra_check.json,
validate.py, validate2.log, obs2.py, mut.py).

### What was built

- `tools/model_frames/spc2d.py`: a strict reader of SWAN standard spectral files (`SWAN   1`, AFREQ / NDIR / QUANT,
  `FACTOR` blocks scaled from the integers, `NODATA` and `ZERO` times, the exception value as nan, 2 MB / 4 KB line
  caps, counts and order checked). Pure Python, shared by the job and the tests.
- `tools/model_frames/partition2d.py`: the watershed of Hanson and Phillips: a 3x3 smoothing that wraps in direction,
  steepest ascent to a local maximum (8 neighbours, wrap in direction, clamp in frequency; a plateau's ties go to one
  bin), a small partition (under 2 % of the energy and 0.15 m) merged into the neighbour it touches most, a shallow-pass
  merge that is OFF (below); per partition Hs = 4 sqrt(m0), the peak period from a log-parabola through the peak bin
  and its neighbours on log f, the energy-weighted circular mean of the FROM directions; the bulletins' six by height,
  the count before the cap, and the whole spectrum's Hs. 145 hours partition in 0.22 s.
- `tools/model_frames/nwps_fetch.py`: the NOMADS paths, the 36 offices and their regions, the listing parser, the
  spectra-head reader; every request through `glwu_fetch`'s pacer.
- `tools/build_nwps_stations.py` -> `nwps_stations.json`: the survey of the last 3 days of every office; a station is
  kept when its CG1 spectra file's point lies within 25 km of NDBC's current buoy position and its first time holds a
  spectrum; the office whose point lies nearest wins. **112 stations kept**; left out: 46240 (NODATA on mtr's CG1, a
  land cell), 42023 (134 km from the buoy), 44018 (38 km), 46085 (25.x km).
- `tests/model_frames/test_nwps.py` (19) on real fixtures captured by `tests/capture_nwps_fixtures.py` (51003's file cut
  to 6 hours, 46240's NODATA head, a CG1 listing page). Mutation check (scratch `mut.py`, 27 mutants of the reader, the
  partitioner and the tool's rules): 27 of 27 killed after six pins. The job suite: 362 passed.

### The merge rule, measured

The shallow-pass merge (Hanson and Phillips's peak-to-saddle test) was swept at 51003 against NOAA's own GFS swell
table on the same 96 hours (the tables come from WAVEWATCH III's partitioning of its own spectrum, so they are the
closest thing to a reference): with SADDLE 0.70 / 0.85 / 0.95 / off, NOAA's systems of 0.5 m or more matched by an NWPS
partition within 2 s and 30 deg were 211 / 223 / 227 / 242 of 315, the partitions per hour 4.0 / 4.7 / 5.0 / 5.6
(NOAA 6.0), and the matched heights' bias +0.26 / +0.22 / +0.21 / +0.18 m. No merging matched NOAA best, so SADDLE is
1.0 (off) and kept as a constant.

### Validation (scratch `validate.py`, 30 stations across 30 offices, 2026-10-28)

The newest run of each office; the partitions at every hour against NOAA's GFS bulletin of the GFS cycle at or before
the NWPS cycle (from NOAA's AWS archive, section 53), at the same valid times: **4,350 hours**.

| measure | result |
|---|---|
| combined Hs, NWPS vs the bulletin's Hst | mean |diff| 0.19 m, bias +0.08 m, p90 0.44 m |
| partitions per hour, NWPS minus NOAA | +0.1 |
| NOAA's systems >= 0.5 m matched within 2 s and 30 deg | 4,650 of 5,289 (88 %) |
| the matched systems: height / period / direction | |dHs| 0.20 m (bias +0.12), |dTp| 0.3 s, |ddir| 5 deg |

Per station the combined height's bias runs from -0.21 m (41009) to +0.51 m (46073, the Bering Sea) and +0.45 m
(46012, 52200): NWPS runs higher than GFS at the Alaska and open-Pacific buoys, lower at some Atlantic ones (different
models, different winds: the NWS forecaster grids). Stations with few NOAA systems (44013: 3) say little.

### What this proves and what it does not

- The reader decodes NOAA's files exactly (the FACTOR scaling pinned against the raw integers; Hs from the decoded
  spectrum = NOAA's bulletin within 0.1 m at 51003).
- The partitioner finds the same swell systems as NOAA's own partitioning, with their periods and directions, in 88 % of
  the cases where NOAA's table lists a system of 0.5 m or more; the rest are NOAA's finer splits of the wind sea and
  systems NWPS's grid does not hold. The heights of matched systems run ~0.12 m higher (NWPS's partitions hold a little
  more energy each).
- It does not prove the tables right against the sea: the hour-0 comparison with the buoys' own readings is below.

### Hour 0 vs NDBC's observed swell split (scratch `obs2.py`)

Hour 0 of each run (its cycle, an analysis more than a forecast) against the buoy's own reading nearest in time
(NDBC's `.spec` summary: WVHT, SwH / SwP, WWH / WWP), 19 of the 30 buoys reporting: **|Hs - WVHT| 0.22 m, bias +0.01**.
The swell / wind-sea split cannot be compared like for like: NDBC splits the spectrum at a fixed frequency (near 0.125
Hz, 8 s), the partitions split by peaks. Where one broad system sits near 8-10 s (41004, 41013, 41114, 41025, 41009,
46029: a single 8.6-10.6 s partition of 1.6-4.2 m), NDBC reports part of it as a 6-10 s "wind sea" of 1.7-2.8 m, so the
split differs by 0.6-0.9 m while the total agrees. Where the systems are apart (51003 14.1 s / 5.0 s vs 13.8 / 5.3 s,
46011 16.7 / 7.5 vs 16.0 / 7.1, 46014 15.5 / 6.9 vs 13.8 / 5.9) the partitions match the buoy's split. Summary over the
19: swell |dH| 0.62 (bias +0.27), wind sea |dH| 0.87 (bias -0.60): the fixed-frequency split, not a partition error.

### For G41a

- The partitioner's one judgement call is the small-partition rule (2 % / 0.15 m) and MIN_HS 0.05 m; the shallow-pass
  merge is off by measurement, kept as a constant.
- A station's office is chosen once by the tool (the nearest point); the 14 stations two offices publish are served by
  one of them.
- Tp from 37 log-spaced bins: the log-parabola is exact for a Gaussian peak; NOAA's own tables show |dTp| 0.3 s.

## Step 2: the job (2026-10-28, Opus 5.5 at HIGH)

Commit 6afc031 (the job), a temporary dry-run workflow (cc72330, a81a70d) removed afterwards with a test pinning its
absence.

### What was built

- `tools/model_frames/nwps.py`: per office the newest settled CG1 run of the last two days (the CG1 grib present and the
  directory's newest file at least 5 min old); the stations' spectra files fetched through the NOMADS pacer, split into
  swells per hour and published as `nwps/v1/<office>/<run>/<sid>.json`, then the run's `index.json`, then
  `nwps/v1/latest.json` last (per office: run, region, stations, the kept runs). A station file whose first time is not
  the run, or that cannot be read, is left out and listed in the index. Four runs kept per office; records older than
  the oldest kept run pruned. A failed office build is recorded and retried after an hour, at most six times. A throttle
  or the 20-min budget stops the tick (exit 3; what was published stays). The pointer never moves back and is rewritten
  once an hour as a heartbeat. No office updated in 24 h raises the stale alarm; an office without a run for 48 h is a
  summary warning.
- `nwps_fetch.py`: every request takes the injected request function (found while testing: discovery had reached the
  real NOMADS from the tests); a throttle is never taken for "not ready".
- `model-nwps.yml` (every 10 min, the publishers' pinned environment), the trigger Worker, the keepalive, the tests
  workflow's paths, the TLS pins.

### Checked

- Tests: `tests/model_frames/test_nwps_job.py` 19 against a fake NOMADS and a fake R2 (no network: a fixture refuses any
  real request). Job suite 380.
- Mutation: 22 of 22 mutants of the job's rules killed (the settled rule, the grib check, newest-first, the start check,
  keep 4 and its order, pointer last, prune kept / newer / records / foreign keys, the retry rule, the pointer never
  back, throttle and budget stop, the heartbeat, the failure record, exit 1, no prune, an empty build).
- Runner dry run (eccodes env, every office, nothing published): 35 office runs, 112 station files, 838,687 bytes,
  2 min 33-37 s, max RSS 60 MB. hfo 2026102800, mtr 2026102800 and lox 2026102806 built locally gave the same files as
  the runner (digest of every file without its build time: 22 stations, identical).

## G41a: one fresh reviewer on the job (2026-10-28, Opus 5.5 at HIGH)

Brief and report: scratch `g41a/` (brief.md, g41a-report.md, scripts/, data/). The reviewer wrote its own spectral
reader and watershed, checked 2,030 real hours from 14 files of 10 offices, ran 65 mutants and a job harness with a
virtual clock; 34 NOMADS requests. **0 P0, 0 P1, 2 P2, 9 P3.**

Held up: the reader decodes exactly (2,030 spectra = an independent reader; 3,000 corrupted copies raised only
ValueError); given the same smoothed field the independent watershed matches 2,028 of 2,030 hours; energy conserved to
5e-15; Tp always within the peak bin's neighbours; NOMADS's listing times are UTC (the settle rule measures what it says);
all 112 stations are GFS stations within 25 km of today's NDBC positions (max 16.2 km) at the stored points; 46028 (two
offices) goes to the nearer, mtr; the workflow's pins equal the other publishers'; suite 381.

Findings and outcomes (all fixed in the commit after c67f2a2):
- P2-1 a silent NOMADS outran the budget: discovery was outside it and a listing that did not answer only moved on to the
  next office (35 offices x 254 s = 148 min a tick, past the 30-min job limit). FIXED: the budget is checked before every
  office and every listing; a NOMADS that does not answer stops the tick (exit 3).
- P2-2 one station file failing with HTTP 500 threw the whole office build away every tick (no record, no back-off). FIXED:
  that station is left out ("not served: ...") and the rest published; two failing in one office build = NOMADS is
  failing: the tick stops. The test fake now raises what the real request raises (TransportError).
- P3-1 ties were decided by float rounding (89 of 2,030 hours differed from an exact-integer reference; the direction
  axis' orientation mattered). FIXED: the smoothed field and the frequency spectra are compared after rounding each value's
  mantissa to 40 bits (`_quantize`), so equal integer sums compare equal and the documented rule applies (a plateau's
  highest flat index; of two equal frequency peaks the lower frequency). Re-run of the reviewer's comparison: the
  partition count now agrees in all 2,030 hours, everything in 2,025; the 5 left are equal-peak Tp ties of tiny wind-sea
  parts (< 0.08 m) where the reference picks the other peak. The two real cases are fixtures (aer 46001: 0.966 / 0.554 m;
  aer 46060: Tp 2.24 s, not 1.89 s). The step-1 validation numbers move by less than their rounding.
- P3-2 the reader's memory was unbounded (ZERO steps): FIXED, at most 400 steps.
- P3-3 the reader accepted nan / inf / 2.5 / 1_000 / negative values and any factor: FIXED, integer rows only (or the
  exception value), a factor in 0..1e3.
- P3-4 partitions under MIN_HS are not listed (85 in 2,030 hours, <= 0.08 % of the energy): documented (their energy
  stays in the whole spectrum's Hs); the "merged, never dropped" wording corrected.
- P3-5 directions were assumed evenly round the circle: FIXED, checked (ValueError otherwise); a peak due north reads 0.
- P3-6 discovery listed the CG1 directory of cycles already live (~25k requests a day): FIXED, the live cycle and older
  are never listed (`candidate(..., after=live run)` returns LIVE).
- P3-7 test gaps (30 of 65 mutants survived): tests added for every real one (a pin of all six fixture hours, the small
  rule's two conditions, the per-Hz Tp, ties, the reader's limits, the budget, a silent NOMADS, a failing / 404 station,
  an empty build, exit 1 only with nothing waiting, the summary, stalest-first, office warnings, prune edges). The
  reviewer's mutants (anchors moved by the fixes adjusted) + 13 new ones: **68 of 77 killed**; the 9 left are equivalent
  on NOAA's files or redundant guards (end-bin width, 10-deg width, zero bins labelled, the parabola's vertex range, any
  grib name, kept runs unsorted, hour 145, `run <= live` now also guarded by discovery) or a rare merge order (the
  biggest small partition first).
- P3-8 the tests workflow's paths: + `tools/build_nwps_stations.py`, `station_list.json`.
- P3-9 wording: 35 offices hold stations; the pointer-following comment; a failure now shows in the summary even when
  another office published; office warnings only for offices serving a station. CORRECTION to the step-2 section: no
  test reached the budget then (the "budget break" mutant was caught for another reason); one does now.

Suite: job suite 402. Not checked by the review: same-cycle reruns by NOAA (never republished by the job), whether NOMADS's
limit is shared with the other publishers.

## Step 4: the site reader (2026-10-28, Opus 5.5 at HIGH)

Commits 1713a8e and the one after it (a tighter run-choice test). app.py only, no page change (step 5).

- `parse_nwps` / `_parse_nwps_uncached`: the pointer `nwps/v1/latest.json` (format and every office entry checked;
  cached 5 min; the last good one serves 6 h when it cannot be read), the station's office from `nwps_stations.json`,
  the office's newest run when it lists the station, else its kept runs newest first; each station file checked
  (format, office, run, station, at most 145 ascending hours, equal lengths, Hs 0-50 m, at most six partitions of
  [0-50 m, 0-40 s, 0-359 deg], booleans refused); a file that fails is skipped like a missing one.
- Rows in the GFS table's shape: heights in feet, Tp, FROM directions, the site's rank (`rank_rows`: height squared x
  period); an hour without a spectrum left out; the station's zone (or the asked one).
- Wind: `_gfs_wind_cover` (the GFS station winds of the latest run, plus the run whose cycle holds the first row
  when the latest starts after it; the newest wins), now shared with the SWAN back-fill (its behaviour unchanged).
- `resolve_model`: "NWPS" honoured only at the 112 stations; payload `nwps_available`; `/healthz` key `nwps`
  (stations, pointer age, offices, last error; reported only); the pointer lock reset in a forked child.
- Live, in-process against the bucket (run of 21:52 UTC): 51003, 46026, 44025, 41004, 52200: 145 rows each, the Wind
  column complete, zones Honolulu / Los Angeles / New York / New York / Guam, 3.7 s for the first (cold), then 0.4-0.5 s.
- Tests: `tests/test_parse_nwps.py` 23 (the real published file of 51003 as the fixture); pins updated (the model
  list, three parsers through `rank_rows`, the payload keys). Mutants of the reader: 31 of 31 killed (two first
  survived: the run-choice test compared lists exactly and dates both runs share; tightened). Whole suite 1,692.

## Step 5: the model button and the page (2026-10-28, Opus 5.5 at HIGH)

Commits e9f2bc8 (code, UI 1.26.5, overlay 2.22.6) and e840aeb (the page snapshot, its own commit).

- forecast.js: `modelOf` (GFS / SWAN / NWPS, anything else GFS); the loader never asks NWPS off the NWPS stations
  (`nwpsStations` from the page's state); the server's echo is authoritative; the model bar shows GFS plus the
  station's other model only (SWAN at the 12 PacIOOS stations, NWPS at the 112), hidden where there is neither; the
  header reads "NWPS · run 20261028 06 UTC (NWS HFO)".
- The page: the state carries `nwps_available` / `nwps_stations`; the NWPS button (hidden where not offered) and the
  no-script option; the head's early request applies the loader's rule (pinned by early.test.js over the address /
  settings cases, NWPS cases added); `pageCycle` names an NWPS table before testing for SWAN (NOAA NWPS is a SWAN model).
- overlay.js: the run line says "the forecast table is a NOAA NWPS (nearshore) run" under an NWPS table.
- The snapshot's diff: the versions, the NWPS button, the two station lists, the early rule; the 46001 page (an NWPS
  station) now offers NWPS.
- Tests: window +2, early cases +6, page +1 (the old SWAN-bar test moved to a station with neither model: 46001 is an
  NWPS station). Mutants of the client: 9 of 10 killed (the survivor, `modelOf` again inside the loader's normalise, is
  redundant: every caller passes the model through it). Whole suite green.

## Step 6: test-site deploy and verification (2026-10-29, Opus 5.5 at MAX)

`test` @ 0773a42: cherry-picks of 1713a8e (433b275), c1645e7's test change (1d441ba; this record left off the test
branch), e9f2bc8 (360a5f4; README the test branch's own), e840aeb (bfe2b4a, the snapshot), and 0773a42 = `nwps_stations.json`
from production a01c039 (the test branch carries no job files; the site reads that file). Site files = the feature
branch's except the test branch's 3 logo lines. Live 01:14:27 UTC, 0 failed polls across the switch; forecast.js /
winds.js (UI 1.26.5) and overlay.js / .css (2.22.6) served = the commit, the 1.26.4 / 2.22.5 URLs 404. Test-branch suites:
pytest 1,227 passed + the 5 failures the test branch always has (its logo lines vs the page snapshot x4, the numpy
import); Node 1,052. Scripts and outputs: scratch `nwps/v6`.

- **Nothing else moved.** 65 forecast answers (30 GFS stations compact, 6 classic, 5 Metric, 5 UTC, the 12 SWAN tables,
  3 Great Lakes, 2 points, 2 removed stations) on GFS run 2026102818: production = the test site before = the test site
  after, the new `nwps_available` key aside (true in the 14 answers of the 7 NWPS stations in the set, false elsewhere,
  0 wrong).
- **All 112 NWPS tables** (`nwps_check.py`, the site's answer against the job's file read from the bucket and rebuilt
  without the site's code: newest office run listing the station, feet, the site's rank, labels in the station's zone):
  112 of 112 agree in every label, combined height and swell (height, period, direction); 145 rows each, 35 offices;
  model NWPS, `nwps_available` true, `swan_available` false, the cycle naming the run and the office, `wind_complete`
  true, the Wind column = the same station's GFS table at every common local time and never empty. First (cold) answer
  per station median 0.57 s, max 1.55 s. Metric 14 of 14, UTC 14 of 14.
- **One mismatch, in my checker, not the site:** at 46206 (off Vancouver Island) the site's times from 2 AM on Sunday
  1 November ran an hour ahead of mine. The time-zone data Render installs (pytz 2026.5, tzdata 2026e) has British
  Columbia staying on UTC-7 from 2026-11-01; this laptop's pytz 2025.2 (2025b) still falls back. Re-checked with
  tzdata 2026e: 112 of 112. (The site's row builder follows whichever library it runs with: locally, with 2025b, it
  gives the old fall-back.) Chrome 156 and Node 24.22.1 (tzdata 2026e) agree with the server (UTC-7 at 09Z on
  1 November), so the page's clock and the table agree.
- **Wind, every row** (`wind_check.py`, 8 stations, the GFS station winds decoded here from the bucket): 0 mismatches,
  including the first 6-12 rows that come from the GFS run before the latest (51003's NWPS run starts at 06Z, the latest
  GFS run at 18Z) and 46206 across its zone change.
- **The model each request gets** (13 cases): NWPS only at the 112 (`model=nwps` too); SWAN or GLWU asked at an NWPS
  station -> GFS; NWPS asked at 51201 / 13130 / a point / 46240 / 42023 (both left out of the station set) -> GFS; at
  45007 -> GLWU; at 44062 -> the sheltered note.
- **The page as served:** 51003 -> GFS | NWPS (SWAN hidden), NWPS pressed with `model=NWPS`; 51201 -> GFS | SWAN with NWPS
  hidden, also with `model=NWPS`; 13130 -> no bar; the no-script select offers NWPS only where it exists (selected with
  `model=NWPS`); `render=full` with `model=NWPS` inlines the 145-row NWPS table with its meta line.
- **Browser** (headless Chrome 156, own profile, never signed in; trusted clicks and taps), desktop 1280x800: a fresh
  51003 opens on GFS with GFS | NWPS; a click on NWPS = one request, 145 rows, "NWPS · run 20261028 06 UTC (NWS HFO)",
  the address `?station=51003&model=NWPS`; Summary 6 days; Graph 145 labels, six swell series; Metric and UTC one request
  each; 51201 next -> GFS with GFS | SWAN; 13130 -> no bar; back to 51003 -> GFS from the page's cache; 46026 -> NWPS
  "run 20261028 12 UTC (NWS MTR)" with the zone's fall-back on Sunday 1 November; GFS again from the cache. Links: six
  addresses (NWPS, lower-case nwps, NWPS at 51201 and 13130, SWAN at 51003, NWPS with Metric + UTC) each make exactly ONE
  request (the head's early one), with the right model, and the address cleaned where NWPS is not offered. The overlay
  panel: "the forecast table is a NOAA NWPS (nearshore) run" under an NWPS table, nothing under a GFS table of the same
  run, the SWAN and GLWU notes unchanged. 51003's live window "Forecast point 51003 · 14 mi away" brings the NWPS table
  forward. Phone 375x812: a tap on NWPS = one request, the 145-row table full screen, no sideways scroll. Console clean in
  every part; the test origin's storage emptied; the browser stopped and its profile removed.
- **Failure paths on real data** (`fail_local.py`: the site's code in-process against a stand-in bucket passing every
  request through to models.allshoresurf.com except the file a case changes): 20 of 20. The newest run not listing the
  station -> the kept run, the newest file never asked; listed but 404, another station's file, a file cut short, the
  older run's file under the newer name -> refused, the kept run; the pointer answering 503 after a good read -> the last
  good one served, `/healthz` names the error, the next request does not ask again within the minute; refused at a cold
  start or with the last good one older than 6 h -> "Could not reach the NWPS forecast" with Retry (not final); the office
  missing or no run listing the station -> "No NWPS forecast found"; a damaged pointer entry -> the whole pointer refused;
  no wind anywhere -> the waves still served, `wind_complete` false; 46028 / 44009 / 44017 / 41004 (published by two
  offices) read from their chosen office only.
- **A run switch, live** (`runswitch.py`): NWS AJK's 00Z run was published by the job at 01:47:09 UTC, seen in the
  pointer at 01:47:35, and the test site served it for 46083 at 01:47:36 ("NOAA NWPS 20261029 00 UTC (NWS AJK)"; the
  station's table was no longer in the site's 30-minute cache). That table = the independent rebuild from the new run's
  file (145 rows; Alaska's fall-back hour on 1 November appears twice, as in the GFS table).
- `/healthz` on the test site: `nwps` 112 stations, 35 offices, pointer 31 s old, no error; memory 204 MB after the
  checks (production 222 MB after 4 h).

For the owner (none blocks G41):
- British Columbia's permanent UTC-7 (tzdata 2026e): the site already shows it (the server's library and Chrome / Node
  agree); this laptop's library is older (no test depends on it).
- A new NWPS run reaches a table up to about 35 minutes after the job publishes it (the site re-reads the run list every
  5 minutes and keeps a built table 30 minutes, as for GFS).
- On phones the window's header hides the run text (as for every model); the pressed NWPS button and the line above the
  table name it. The model buttons are 31 px tall there (the SWAN bar's size since section 25).
- My own slip: a `chrome.exe --version` call printed "Opening in existing browser session" (the section-50 lesson): it
  may have opened a blank window in the owner's Chrome. Nothing else touched it.

## G41: two fresh reviewers on the site (2026-10-29, Opus 5.5 at MAX; reviewer A on Fable 5.1)

Brief and reports: scratch `g41/` (brief.md, a/g41-a-report.md, b/g41-b-report.md; scripts and logs beside them).
Reviewer A read the code and checked the data with its own tools (no browser); reviewer B used the test site @ 0773a42
as a visitor would, with trusted clicks and taps in headless Chrome. **0 P0, 0 P1, 1 P2, 11 P3** (A 0/0/0/7, B 0/0/1/4).

Held up:
- **Tables = the job's files.** A rebuilt all 112 tables from the bucket with its own reader, run choice, rank and zone
  labels: 112 of 112 identical, 16,240 rows, every cell of the compact table included; 40 more answers in other units
  and zones identical. B did the same at tz=UTC: 112 of 112, 0 mismatches.
- **Rank.** Every row re-ranked independently = the site's; equal-power pairs keep the source order.
- **Wind.** A decoded the GFS winds tables of six runs itself: every wind cell of every table matched, the 61 back-filled
  stations and the repeated hour on 1 November included. B: 15,878 station-hours equal the GFS table's, the 362 earlier
  rows all carry wind.
- **Model choice.** A ran 672 cases (14 station kinds x 12 spellings of `model` x 4 unit/zone settings): the server, the
  payload, the parser called, the page's state, the head's early request and the loader's first request agree in all.
  B: 21 links, one request each, the address cleaned where NWPS is not offered.
- **Hostile bucket.** 71 station-file cases and 28 pointer cases refused or accepted as designed; the last good pointer
  kept; one request per key at a time; the fork hook re-creates the lock.
- **Nothing else changes.** Production vs test on the same GFS run: 45 answers (A) and 27 pairs (B) identical once
  `nwps_available` is set aside; 6 pages differ only by the intended lines.
- **Partitions through the site's output.** A's own reader and watershed at 5 stations: combined height equal to 2 dp in
  25 of 25 hours, the main systems equal in 21 (in the other 4 the site keeps 1-2 small extra systems that A's coarser
  merge folds in; energy consistent). B: over all 112 files the parts keep 0.93-1.04 of the energy; NWPS vs the buoys'
  own observed height, median ratio 0.88-1.16 at nine buoys.
- **Times.** NWPS answers median 0.44 s, worst 1.43 s (A); first request 0.40 s mean, repeats 0.17 s (B).
- **Page.** Keyboard, the accessibility tree, no-script, stars and default, links and history, map dots, five window
  sizes, late or out-of-order answers never landing, the overlay's run line, the Summary and Graph views, memory flat
  over 40 switches.
- **Suites.** pytest 1,692 + one failure (A P3-7: a CRLF checkout), Node 1,053. Mutants: A's 79 (+7 re-run or
  re-crafted): 13 real test gaps survived (P3-6), 9 equivalent, every other one killed.

Findings (the author's check in brackets):
- **B-1 (P2)** Pressing NWPS keeps the GFS table's scroll in pixels, so the NWPS table opens on other hours. Example:
  at 46083 / 44099 (00Z runs) the current row ends up above the view; at 51003 the view opens 13 h in the past. The
  now-row reveal runs only when the station changes (forecast.js `apply`: `st.station !== revealedFor`). Production's
  GFS -> SWAN at 51201 does the same; NWPS puts it on the main path to 112 tables. [Confirmed in code.]
- **B-2 (P3)** At the 12 SWAN stations the SWAN button lost its rounded right corners: the hidden NWPS button is now the
  group's last child, and Bootstrap squares every button that is not last. [Confirmed in the template, lines 1104-1107.]
- **B-3 (P3)** The NWPS Location line names the GFS bulletin point (station_coords), not the NWPS point the numbers are
  for. At 51003 they are 27 km apart; the plan's text named the NWPS point. [Confirmed: 19.22 N 160.82 W vs the file's
  19.289 N 160.569 W.]
- **B-4 (P3)** A failed NWPS load hides the whole model bar (`ui.clear`), so the GFS table the visitor just had is
  reachable only by Retry or a reload. The same is true of SWAN today. [Confirmed, forecast.js line 1008.]
- **B-5 (P3)** NWPS needs more GFS winds runs held: the latest, plus the back-fill runs of the 06Z, 12Z, 14Z and 18Z
  offices, plus SWAN's back-fill. That is 5 once GFS 00Z is the latest, but the site holds 4 (`_GFS_WINDS_KEEP_RUNS`).
  The cost is extra R2 reads of 1.4 MB on cold builds, not wrong data. [Confirmed: the cap is 4 and the per-station
  cache is 128.]
- **A P3-1** A 10-digit pointer run that is not a date gets past the check (a regex only), and the parser's
  `strptime` then raises. The visitor sees the generic "temporarily unavailable" message, and the bad pointer is kept
  for 5 min. Only a corrupted bucket can do this. [Confirmed.]
- **A P3-2** No cap on the kept runs tried: 50 listed runs = 51 bucket requests for one visitor request (the job keeps 4).
- **A P3-3** A failed pointer read at a cold start is asked again on every NWPS request, and several threads read it at
  the TTL edge. This is the GLWU reader's shape too.
- **A P3-4** One NWPS request can hold a thread up to ~40 s against a trickling bucket (8-s cap x 5 reads). NWPS builds
  are outside the shared build cap; GLWU likewise.
- **A P3-5 (the K-7 corollary)** On a stale run, the hours before the latest GFS winds run have no wind once the
  back-fill cycle is older than the 8 runs the winds job keeps. The table is then gappy: the server keeps it 5 min, the
  page 1 min. [Correction to the report: the page does not poll. A gappy table is kept 60 s in the page's cache instead
  of 10 min, so the next visit after a minute asks again (forecast.js `ttlOf`; no timer).]
- **A P3-6** Test gaps (mutants that survive): the back-fill cycle's rounding (a 14Z run), a sixth swell dropped, a bad
  run inside `runs`, the after-TTL re-ask count, the fork lock, the error path's `nwps_available`, `init` passing the
  NWPS list to the loader, the head's `trim`, the overlay's NWPS branch (only its text is pinned), and value-range edges.
- **A P3-7** Notes. The rank is computed on feet rounded to 2 dp, so 42 of 16,240 near-tie rows differ from a rank in
  metres; GFS does the same site-wide (no change). On a Great Lakes page the hidden model bar has no pressed button
  (production pressed GFS). The job's reader test fails on a fresh Windows checkout with core.autocrlf=true: the
  fixture becomes CRLF. An unknown `tz` gets its own cache key (pre-existing, no change). [CRLF confirmed: the repo's
  .gitattributes names only image types.]

K-7 and K-8 weighed:
- **K-7 (no age limit on the NWPS run served).** A: P3 (offices run 2-3 times a day, so a multi-day silence means an
  office outage). B: "P2 when it happens", worse on phones, where the header hides the run.
  - When an office stops, its tables open on past days with no note, and once every row is past, no row is "now".
  - Normal ages matter for the threshold. HFO posts its runs 8-14 h after their labels, so 51003's table is routinely
    up to ~26-32 h old before the next run lands. A note must start above that.
  - OWNER'S DECISION (2026-10-29, AskUserQuestion): "Note at 36 h, hide at 72 h". Past 36 h the run text says
    "· N h old"; past 72 h the station does not offer NWPS (GFS only, the button hidden) until its office publishes
    again.
- **K-8 (the shared 64-entry forecast cache).** Performance only; a rebuild costs 0.44 s. Not raised: a cached answer
  is 0.36 MB (NWPS) to 0.87 MB (GFS) of JSON, so 64 more entries could cost tens of MB on the 512-MB service.

Fix round (step 8) scope:
1. B-1: the now row revealed again when the model changes (`revealedFor` keyed by station + model; the scroll reset;
   unit and zone changes still keep the place).
2. B-2: the last visible model button keeps its rounded corners (a CSS rule on `#modelBar`).
3. B-3: the NWPS Location line shows the NWPS point (the file's `point`).
4. B-4: a failed load keeps the model bar with the station's models, so GFS stays one press away (shared by SWAN).
5. B-5: `_GFS_WINDS_KEEP_RUNS` 4 -> 8 (+5.6 MB) and `_WIND_CACHE_MAX` 128 -> 256.
6. P3-1: every pointer run date-checked (`strptime`) in `_nwps_pointer_ok` (the whole pointer refused).
7. P3-2: at most the newest 8 kept runs tried.
8. P3-3: a failed cold pointer read remembered 60 s (asked again after it, not on every request) and one pointer read
   at a time.
9. P3-4: the run loop stops at a transport failure (a 404 still moves on to the next run) and has a total time budget.
10. K-7 / P3-5 (the owner's decision): the run's age carried in the NWPS payload; past 36 h the header's run text and
    the line above the table add "· N h old" (as forecast points do); past 72 h the station is served as not offering
    NWPS: `nwps_available` false and a request for NWPS answered with the GFS table (the server's model echo, as for any
    station without NWPS), until a newer run is published. The GFS path never waits on R2 for this: it uses the held
    pointer only. A table whose missing wind lies only before the oldest kept GFS winds run is kept the full time (no
    1-minute re-ask).
11. P3-7: the Great Lakes page's hidden bar presses GFS again (`selected_model not in ('SWAN', 'NWPS')`); the NWPS
    fixtures kept byte-exact on checkout (`.gitattributes`), the reader test made CRLF-safe too.
12. P3-6: a test for every real survivor; the reviewer's mutants re-run.
13. UI and overlay version bumps, the page golden in its own commit, README.
