# G42 — live buoys, the US widen (plan section 63)

Three more feeds for the "Live buoys" layer: NDBC's bulk report for stations without a spectral file (NDBCW), the Great
Lakes Observing System's Seagull API (GLOS) and the IOOS sensors ERDDAP (IOOS). Branch `feat/buoys-widen` off production
a4560b1. The owner set one model / effort cue for the whole section (Opus 5.5, HIGH) and asked for build, test site,
review and fixes without stopping, then a stop before production.

## Step 3: test-site verification (before the review)

- Live merge of all thirteen feeds: 567 markers before, 656 after (GLOS 34, IOOS 45, NDBCW 17 kept after the dedup).
- Finding V3-1, fixed before the review @ e8fa5e8: 16 of the first 101 new markers opened an empty window (platforms
  whose wave sensor was off while wind and temperature reported). Now a marker is listed only when its feed had a wave
  height recently (one ERDDAP "newest wave height" probe per station; GLOS through its mirror, IOOS on the sensors
  ERDDAP). After it: 0 empty windows of 96; the agency comparison 22 of 24 (both differences were timing).
- Windows on desktop and phone by trusted clicks and taps; the refresh button sends `X-Allshore-Refresh: 1`; console
  clean; 0 failed polls across both deploys.
- Memory found and fixed @ 2f04538: the GLOS parameter list (3.6 MB) and the IOOS index / allDatasets tables were
  parsed whole (peaks of ~25 MB each). Streamed now: transient ~19 / ~22 MB, under 1 MB held. On the test site the
  instance settled at 166 MB at 40 minutes (the build before the stream fix: 190 MB at the same uptime); the first
  complete list 89 s after a start (the free instance).

## G42: one fresh reviewer (code + light site checks)

**0 P0, 0 P1, 5 P2, 12 P3.** Report and scripts: scratch `g42/r/` (g42-report.md, scripts/, raw/, mut_results.txt).

| # | Rank | Finding | Outcome |
|---|---|---|---|
| F1 | P2 | A refresh that lists nothing after a good list kept the markers (keep-last) but had already emptied GLOS's / IOOS's station-to-dataset maps: every kept marker opened an empty window for up to 6 h | Fixed: the refresh raises before the maps are replaced (`"... listed nothing after N stations"`) |
| F2 | P2 | With no GLOS parameter map, every window open downloaded the 3.6 MB list on the visitor's thread through the retrying session (~3 x 60 s if GLOS hangs) | Fixed: the list refresh needs the map (keep-last when it cannot be had); `detail()` never fetches it |
| F3 | P2 | No parser refused future times (one 2027 row made itself newest and cut the 24-h window to itself; a marker never went stale) or impossible values (-5, 99, -9999, 9.97e36); GLOS time offsets cut off and read as UTC | Fixed: `_sane_reading` for the three feeds and the probe (more than 1 h ahead or a height outside 0-30 m: row left out; other values outside their ranges blanked); `_glos_naive_utc` applies offsets |
| F4 | P2 | One probe answer the CSV reader refuses (a field over 128 KB) failed the whole GLOS / IOOS refresh, and every retry sent every probe again | Fixed: any probe error is "unknown"; `_wave_probes` turns a raising future into "unknown" |
| F5 | P2 | The GLOS layer hung on its ERDDAP mirror (~30 h behind): a mirror 3 days behind would answer "no" for every probe and hide every GLOS buoy | Fixed: no judging by a mirror whose newest dataset is over 2 days old (all activated platforms listed, no probe); a batch answering "no" for over 80 % of 10+ platforms is not believed |
| F6 | P3 | ERDDAP's 404 "Currently unknown datasetID" (a reload) read as "no wave height" | Fixed: "no" only for "no matching results" |
| F7 | P3 | No deadline for a refresh's probe batch | Fixed: `PROBE_BATCH_S` 150 s; late answers count as unknown; each probe's download 1 MB / 30 s |
| F8 | P3 | Byte caps checked after the whole body was downloaded | Fixed: `_capped_fetch` reads in chunks under the cap and a time limit (lists 180 s, windows 20 s), honouring Content-Length |
| F9 | P3 | IOOS kept 18.5 MB of CSV buffers, GLOS its 9.2 MB roster, through the ~50 s of probes | Fixed: released before the probes |
| F10 | P3 | ndbcw:62144 / 62146 drawn 6-7 km from cefas:10 Clipper / cefas:305 Lomond (production shows the same pairs with CMEMS copies) | Fixed: `_BUOY_ALIAS` (the page's `LINK_ALIAS`) gives the CEFAS ids NDBC's numbers in the merge |
| F11 | P3 | Staleness fixed at list time: a dead IOOS buoy non-grey up to ~11 h; GLOS markers carry no time; a quiet GLOS buoy's window started yesterday and showed nothing | Partly fixed: a GLOS platform whose newest wave height at the mirror is over 36 h old gets a window starting a day before it. Recorded: the grey look of GLOS / IOOS markers (the mirror's ~30-h lag gives no honest freshness time) |
| F12 | P3 | NDBCW "Public domain" over UK Met Office / ECCC buoys; IOOS one generic licence over datasets with their own texts | Fixed: labels and credits name the operators' terms |
| F13 | P3 | GLOS positions not range-checked | Fixed |
| F14 | P3 | NDBCW windows fetched NDBC's file for any id | Fixed: listed stations only |
| F15 | P3 | Korea's placeholders left out by a fixed id list (a new id with the same values listed) | Fixed: any report with a 0-s period left out (only the KMA placeholders report one) |
| F16 | P3 | README refresh times (measured 45-51 s, not 22-24 s); "mean direction" for NDBC's MWD; untrimmed names | Fixed |
| F17 | P3 | 22 of the reviewer's 40 mutants survived, 12 hiding real defects | Fixed: tests for each; re-run on the fixed code: every real-defect survivor killed (X21 and X33 now equivalent: the reading check drops a row without a time and a partition without a height) |

What held: the three section test files and the full suite; live spot-checks of one window per feed against the
agency (0 mismatches: ndbcw:44042 vs realtime2, glos:SPOT-1360 vs the Seagull API, ioos:41065 vs its ERDDAP CSV); the
300-km number rule never merged two different buoys on the real lists (largest same-buoy gap 125 km, 42058); the "no"
verdicts checked against GLOS's API; refresh peaks IOOS 20.4 MB, GLOS 13.3 MB, NDBCW 0.3 MB.

## Fix round (@ a9d22b8, test @ 47175a8)

- `tests/test_buoys_widen_g42.py`: 36 tests, one or more per finding, incl. a real local socket for the capped reads
  (announced oversize, chunked oversize, a trickle past its time limit).
- Mutants: 32 of the fix round's own, all killed (scratch `g42/fix/mut_g42.py`); the reviewer's 40 re-run on the fixed
  code (scratch `g42/fix/mutg42_rerun.py`): every real-defect survivor killed; the rest equivalent or moved anchors.
- Suites: pytest 1,825, Node 1,061.
- The early-start rule (F11) first fired on every GLOS window because the mirror always lags ~30 h; narrowed to "older
  than 36 h" before the commit.

## Test-site verification of the fix round (test @ 47175a8, 2026-10-30)

- Deploy: the new instance answered from 06:32 UTC with no failed poll; the first complete list 95 s after the start
  (655 markers, 147 MB).
- `/healthz`: every feed published with no error; GLOS's refresh took 52.9 s and IOOS's 48.3 s.
- Counts after the dedup: GLOS 33, IOOS 45, NDBCW 15. NDBCW 62144, 62146 and 62304 are now merged under CEFAS's markers
  (Clipper, Lomond, Sandettie LV). GLOS has one fewer: Great Lakes buoys are coming out of the water.
- Every new marker's window, opened through the API: 0 empty of 93. Three IOOS markers are older than 6 h (41053,
  41056 23 h, SIPF1 10 h): their operators' newest data, shown grey as stale.
- The agencies' own data (scratch `sources62/v3/v63_data.py`): 24 of 24 windows (8 per feed) equal the agency's newest
  wave height and time. ioos:46036 shows 25 rows although ERDDAP refuses a plain 24-hour query on that dataset, which is
  the stated-end rule working.
- Browser, headless Chrome with its own profile, trusted clicks and taps:
  - desktop 1280x800: four windows (GLOS Marquette, NDBCW 42357, GLOS Atwater, IOOS Capers) each with 24-25 history
    rows and the refresh header;
  - phone 375x812: no problems.
  The harness was stopped and its profile removed.
- Memory on the stream-parsing build (the 40-minute watch before the fix round): it settled at 166 MB, against 190 MB at
  the same uptime for the build before it.

## F11 completed (@ 2bbc8ec, test @ b67a89a)

- IOOS: the list refreshes every hour (`list_ttl_sec` 3600); the category index is read every 6 h and a failed read keeps
  the held one up to a day. A marker's time is its newest wave height (the probe's answer), so a buoy whose wave sensor
  stopped turns grey 6 h later.
- GLOS: a marker's time is its newest wave height at the ERDDAP mirror; markers turn grey after 2 days (the mirror runs
  ~30 h behind) and leave the map after 3; a stalled mirror gives no times. A quiet platform's window starts a day before
  its last wave height when that is over 36 h old.
- On the test site (@ b67a89a): 649 markers, GLOS 32 all with times and none grey; IOOS 3 grey (41053, 41056, SIPF1).

## Re-check of the fix round (one fresh reviewer, 2f04538..2bbc8ec)

**0 P0, 0 P1, 1 P2, 6 P3.** Report and scripts: scratch `g42/recheck/` (recheck-report.md, scripts/, mut_results.txt).
Verdicts: F1-F6, F9 and F12-F17 FIXED; F7 and F8 PARTLY (N4); F10 NOT FIXED (N2); F11 FIXED with caveats (N1, N6).

| # | Rank | Finding | Outcome |
|---|---|---|---|
| N1 | P2 | The "no"-share rule (more than 80 % of 10+ answers = a mirror problem) locks in when the season makes most "no" answers real: as the Great Lakes buoys come out, the met-only platforms answer "no" every hour, no verdict is ever stored, and they stay listed all winter with empty windows and no time | Fixed: the share rule is gone; a "no" is believed only for a dataset the mirror lists (its own row there), however many answer so |
| N2 | P3 | The CMEMS copies of Clipper and Lomond (6200144 / 6200146) stood alone 6-7 km from the CEFAS markers after F10 | Fixed: `_buoy_number` reads CMEMS's `XX_TS_MO_6200NNN` as 62NNN (the page's `linkId` rule) |
| N3 | P3 | A probe whose newest row was refused (in the future, or an impossible height) answered "no": one bad row hid a station (a row stamped 2027 for a year) | Fixed: both probe queries ask ERDDAP for rows up to an hour ahead and heights 0-30 m only; an answer with only refused rows is "unknown" (NaN-only rows stay "no") |
| N4 | P3 | `max_s` was checked only between 64-KB chunks, so it never bounded a body under 64 KB that dripped (window and probe bodies are 1-5 KB); leaked probe threads piled up | Fixed: each socket read gets the time left as its timeout (a stall ends at `max_s`) and a timer at `max_s` shuts the socket (a drip ends with its next byte); the socket is found under the answer's file when the connection has none (a server that closes after its answer). Probe threads now end at the probe's limit, so they cannot pile up |
| N5 | P3 | The F1 guard compared with the provider's own map, never cleared, so after the 6-h keep window a real empty list was refused for good (an error and 5-min retries all winter) | Fixed: the guard reads the base's published list; once the base publishes the empty list, the next refresh accepts it |
| N6 | P3 | One unreadable mirror answer dropped the held "no" verdicts and every GLOS marker time for that hour | Fixed: the last good mirror read is kept up to 6 h (`MIRROR_HOLD_S`); while the mirror cannot be read, it judges with the verdicts in hand and asks no new probe |
| N7 | P3 | Four of the reviewer's mutants survived (Y36 one-shot, Y46 IOOS csv.Error, Y26 future maxTime, Y37 close on refusal) | Fixed: a test each |

FOUND WHILE FIXING N4 (Windows only): shutting a socket from another thread does not end a read already waiting on it
(the read waits in `select`), and for a server that closes after its answer the connection holds no socket (`sock` is
None; the socket sits under the answer's file). Hence the per-read timeout: a read that waits ends at the limit on any system,
and the shut socket ends a drip with its next byte. app.py's `_capped_get` (section 22/53) has the same two limits; recorded for a later change
(Render runs Linux, where the shutdown works; R2 / S3 / NOMADS keep their connections open).

## Fix round 2 (@ b281971, test @ 87cd7cf)

- Code: `buoy_sources.py` (`_wave_verdicts(cands, listed, ask)`, `_mirror_state` -> (silent, listed, mode) with
  `MIRROR_HOLD_S` 6 h, the probe queries' sane-row constraints and the "refused rows = unknown" rule, `_capped_fetch`'s
  per-read timeout and shutdown timer with `_answer_socket`, the guards on `_list_cache`, `_CMEMS_WMO_RE`), README.
- Tests: `tests/test_buoys_widen_g42.py` 53 (+17): the winter batch, "no" only for a listed dataset (unit and through the
  list), held judgement, a stall after a good read, the future maxTime, the probe queries, refused-only answers, the
  empty-list guard on the base's own refresh cycle, a silent trickle and a drip on a real socket (both under 2 s with a
  0.5-s limit), one-shot, close on refusal, IOOS csv.Error, the CMEMS numbers.
- Mutants (scratch `g42/fix/mut_n.py`): 24 of the round's own, all killed after two list-level tests were added for the
  first survivors (N02, N24 / N06). One survivor (N19, a 10-ms timeout set by the timer) showed that step was redundant:
  removed. The earlier sets re-run: G42's own 26 of 26 still applicable killed (G12 re-targeted and killed; G16, G32
  covered code removed with the share rule; G21, G22 replaced by N20, N21), F11's 8 of 8, the extra 2 of 2. G03 (the
  between-chunk time check) now survives: the socket cut ends the same reads first; kept as a backup for an answer
  whose socket cannot be found.
- The reviewer's reproduction scripts on the fixed code: winter 11 of 13 no-wave platforms left out every hour (was 0);
  the 2027 row and the spike give "unknown" (was "no"); the empty list accepted after 6 h, refresh hourly (was an error
  and 5-min retries); a 503 mirror keeps the held verdicts and times; drips cut at 1.0 / 1.3 s with a 1-s limit (was
  9.8 / 10.1 s); probe threads with the limit at 2 s: never more than 4 at once, 0 left after (was 4 / 8 / 12, exit held).
- Suites: pytest 1,844, Node 1,061.

## Test-site verification of fix round 2 (test @ 87cd7cf, 2026-10-30)

- Deploy: the new instance answered from 17:19:51 UTC with 0 failed polls; every one of the 13 feeds published with no
  error (GLOS 48 s, IOOS 49 s).
- Markers: 652 (GLOS 34, IOOS 45, NDBCW 16). N2 live: cefas:10 and cefas:305 now carry `also_sources` NDBCW and CMEMS;
  no CMEMS copy of Clipper or Lomond stands alone. GLOS: 33 of 34 markers with a time; glos:45175's probe was "unknown"
  at that refresh (the mirror answered "yes" when asked again: kept listed without a time, asked again next refresh).
  Three IOOS markers grey (41053, 41056 7 h; SIPF1 21 h), as before.
- Every new marker's window: 0 empty of 95. The agencies' own data: 24 of 24 windows (8 per feed) equal the newest wave
  height and time.
- Browser (headless Chrome with its own profile, trusted clicks and taps): desktop 1280x800 and phone 375x812, one window
  per feed with 24-25 history rows and the refresh header; no problems; the harness stopped and its profile removed.
