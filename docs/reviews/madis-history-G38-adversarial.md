# Section 55: 24 hours of MADIS readings for the Hawaii NWS wind stations: review record

Plan section 55 (plan file swirling-spinning-snowflake.md). The NWS API's observation feed has been stalled since
2026-10-22 13:20 UTC, so the wind window of the 294 Hawaii NWS stations (HECO / HELCO / MECO, UH, RAWS, CWOP, HADS) had
at most one reading (MADIS's newest). Owner decision (2026-10-26): keep the 24 hours in the site server, no new job.

## Step 1: the build (Opus 5.5 HIGH)

`feat/madis-history` @ 9650730 (code), d102b5c (golden), 48c814a (a pin), fb9a7d8 (the windless rule), UI 1.26.2.
`parse_madis_rows` (every record of a station, ascending), `MadisSeries` (compact arrays), the live poll by receipt
time (`time=0&timefilter=1&minbck=-59&recwin=4`), the start's back-fill by observation time (twelve 2-hour windows,
`time=YYYYMMDD_HH00&minbck=-119`, newest first, one request a refresh), the gap rule, the history merge in
`WindHistory` ("via" "madis" / "nws+madis", `filling` with a one-minute cache), `/healthz` keys, the window's notes.
Real run in-process: 24 h filled in 514 s (14 requests), 285 stations / 43,442 readings, ~3.5 MB; 30 of 30 sampled
points equal an independent parse of the same answers.

## Step 2: test-site verification (Opus 5.5 MAX)

`test` @ d02d2c9, then 053a23a (the V2-1 fix). UI 1.26.2 = repo. Headless Chrome (own profile, trusted input).
- Fill 1 (the deploy): done 11.6 min after the start, 12 windows, 0 given up; 285 stations, 44,338 readings, 24.8 h.
- Windows: during the fill 016HE showed its last 40 minutes and "The earlier hours are still loading."; after it 016HE
  143 rows (10 min), 011HI 281 (5 min), AU956 222, HOFH1 23 (hourly), each with speed / gust lines, arrows, the
  temperature chart, the table, the MADIS note and the disclaimer; phone 375x812 full screen, no sideways scroll.
- Independent check against NOAA (three windows and the live answer fetched directly, own CSV parser): 12 stations, 526
  readings, 0 mismatches, 0 missing, 0 extra.
- The latest table vs production at the same moment: 2,522 rows each, 285 fed by MADIS each, no same-time difference.
- A clean restart of the idle free instance back-filled again (done at uptime 749 s); memory = the old code's profile
  until the fill, +7-10 MB at its end.
- V2-1 (found, fixed @ b2d8813): MADIS answers a window only when its query is done (20-55 s); the live fetch's 45-s
  read timeout cut windows (saved by the retries). The windows got their own fetch (100 s a read, 120 s in all). Fill 3:
  9.6 min, every window on its first try.

## G38: one fresh reviewer (Opus 5.5 HIGH), on 0a404bc..b2d8813 + light test-site reads

**0 P0, 0 P1, 3 P2, 6 P3.** Every finding reproduced by the author before the fix (the reviewer's four probe tests pass
on b2d8813 and fail on the fix; P3-2 / P3-4 / P3-6 by reading, P3-3 and P3-5 by the reviewer's data and mutants).

| # | Finding | Outcome |
|---|---|---|
| P2-1 | A window that gets MADIS's known bad answers (header only; every wind -99999) was marked done and never asked again: its 2 hours stayed empty for all stations | FIXED: such an answer raises inside the window's try, so it is tried again (3 tries in all) and logged like the live poll's |
| P2-2 | The 30-s pause between windows was measured from the START of the last refresh; a window takes 20-55 s and each publish woke the scheduler, so the windows ran back to back (one query in flight at MADIS for the whole fill and through every retry) | FIXED: the pause runs from the END of the refresh (15 s after a window, 2 minutes after a failed one); a window publishes nothing unless the list changed (P3-1), so it no longer wakes the scheduler |
| P2-3 | Live polls that failed (or brought no wind) from the start left the hours between the start and the first good poll unasked (the gap rule needed a first good poll) | FIXED: the start's queue sets the gap rule's reference to the start; windows given up while the live poll was failing too (MADIS down) are asked again, once, when it answers (within the 24 hours) |
| P3-1 | Window refreshes counted as good refreshes of the feed: they cleared the live poll's error in `/healthz`, reset its good age and bumped the list version (a new ETag every window) | FIXED: a window's refresh is not the feed's: the live poll's error and good age stay; a new version only when the list changed |
| P3-2 | The NWS API's request budget gated stations MADIS fully covers ("busy" with 24 h of MADIS rows in hand) | FIXED (author's call, reversible): with the budget used up and MADIS rows in hand, the window shows MADIS's rows and is asked again a minute later (the one-minute cache answers G35 D15's concern, a one-row answer cached 10 minutes); "busy" when MADIS has nothing |
| P3-3 | MADIS latency exceeded the 60 min the gap margin assumed (a record 71 min late) | FIXED: the gap rule asks from 2 hours before the last good poll |
| P3-4 | `_hist_lock` not renewed after a fork | FIXED: `MadisProvider._reset_after_fork` |
| P3-5 | Weak pins: the gap margin, the 50-min boundary, the queue boundary, the 25-h cut, the window's future cut, NWS failure with history but no latest reading | FIXED: a test each |
| P3-6 | The heavier live query kept the old 45-s read timeout (the test site's `/healthz` showed one ReadTimeout at 45.05 s) | FIXED: 75 s a read (90 s in all unchanged) |

Held up (the reviewer's evidence): the receipt-time filter and the observation-time windows on the real viewer; one
request a refresh; keep-last; the merge (one row per time, the NWS API's at a tie); memory (285 x 155 readings 2.92 MB;
a 722-KB window parsed with a 3.8 MB peak in 0.47 s); the page notes text-only; the test site's histories continuous
after the fill. Mutants: 14 of 22 killed, 2 equivalent; the 6 real survivors are the P3-5 pins.

### The fix round (Opus 5.5 HIGH)

`wind_sources.py`: `MadisProvider._fetch_and_publish` (the live poll through the base class; a window turn publishes
only a changed list and leaves the error, good age and due time alone), `_live_turn`, `_window` (bad answers raise;
returns read / failed), the pause from the refresh's end (`MADIS_BACKFILL_TTL_S` 15, `MADIS_BACKFILL_RETRY_S` 120),
`_live_bad` / `_lost` / `_add` (windows lost in an outage asked again), `MADIS_GAP_MARGIN_S` 2 h, `_live_ok` at the
start's queue, `_reset_after_fork`, `status()` `backfill_lost`; `WindHistory._build` (the NWS budget used up with MADIS
rows: MADIS, a one-minute cache). `app.py`: `WIND_MADIS_TIMEOUT` (10, 75). Tests: the section-55 tests updated to the
new rules (the default window answer now carries one record with wind: an empty answer is a failed window), 8 new
tests (each finding, the boundaries, the fork lock, NWS down with history but no latest). The reviewer's four probes
fail on the fix. Mutants of the fix: 20 of 20 killed. README section "Wind stations" (the 24 hours) updated.
Fill time with the pauses: about 12-15 minutes after a start (was 10-12 back to back).
