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
