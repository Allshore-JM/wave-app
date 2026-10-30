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
