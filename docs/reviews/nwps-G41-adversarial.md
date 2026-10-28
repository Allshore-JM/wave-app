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
