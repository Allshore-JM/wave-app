# Radar layer (plan section 85): review gate G56

## G56a: the job

One fresh reviewer on `git diff e51c2eb..67ac1c2` (the job: `radar.py`, `mrms.py`, `opera.py`, the workflow, the Worker's list, tests).
Its result: **0 P0, 2 P1, 7 P2, 10 P3**. 59 mutants run, 33 survived. No wrong pixels on real files; deletes stay inside `radar/v1/<slot>/`.
The author re-ran its scripts on the same code and reproduced every failure before fixing anything.

### What it checked and found right
- C1: every pixel of one real file per domain (5 domains, about 49 M points) against eccodes: value difference 0, lattice mismatches 0,
  including Alaska's date-line side and Guam.
- C2: the corners by pyproj land on the grid's outer corners within 1 mm; 162 full rows of OPERA (1.59 M points, 74.995 N and 31.005 N
  included): index and value mismatches 0, forward vs pyproj 0.0000 m.
- C3 / C4: the code table, t0 and t1 against a naive block sort, every pixel of every written cell by the lat/lon formula, the bitsets: 0 wrong.
- C5: a chaos run (400 ticks, 10 kills, random put, delete and source failures): no pointer naming a missing object. C6: a retired slot
  cannot refill in the same tick; the delete prefix is exact. C8 / C9: pins, secrets scope, licence words in the credit.

### Findings and outcomes
| # | Finding | Outcome |
|---|---|---|
| P1-1 | A reader error other than ValueError (bad deflate, PNG bomb, damaged HDF5 chunk, string dataset, `data` as a group, array attributes) ended the whole tick: Europe starved for up to 24 h, exit 0, no alarm | Fixed: `_reading` turns anything a file does to a reader into ValueError; a test per row |
| P1-2 | A format break in one region stopped every later region and every older slot of that source | Fixed: at most `BAD_PER_TICK` (2) bad files of a region a tick, then that region alone is left; the others and older slots go on. A bad file counts toward a gap only when newer frames of the region read well, so a real format change burns no slots |
| P2-1 | A connection closed mid-body returned short data: a false "layout changed" mail and the source stopped | Fixed: the length is compared with `Content-Length`; short = asked again, then Down |
| P2-2 | HTTP 403 counted as "never published": a blocked runner gave up the whole day in three ticks | Fixed: only 404 is Missing (both buckets answer 404 for an absent key, checked live) |
| P2-3 | Alarms outside the try: a bucket refusing writes failed every run after 6 h, up to 144 mails a day | Fixed: an alarm that cannot be recorded is a summary line and a failed run only around 00:00 and 12:00 UTC (`rated`) |
| P2-4 | The 5-minute deadline was not tied to the workflow's limit (prune unbounded, boto 5 attempts x 60 s, the fetch timeout per read) | Fixed: `DEADLINE_S` checked before each region and each delete, at most `PRUNE_PER_TICK` (6) deletes, uploads stop at `HARD_S` (7 min), a whole file `FETCH_MAX_S` (2 min), the job's own boto client (10 s connect, 30 s read, 2 attempts; other jobs unchanged), workflow timeout 12 min |
| P2-5 | `gone` grew without limit and without an alarm when deletes failed | Fixed: one alarm when the oldest has waited over 6 h |
| P2-6 | A region going silent never alarmed (the alarm was per source) | Fixed, per region, on the author's call (the reviewer left it to the owner): one alarm per 12 h per region; told to the owner at the job-merge stop |
| P2-7 | 33 surviving mutants | Tests added for every real gap: see below |
| P3-1 | The first-ever tick alarmed | Fixed: the pointer's `started` is the clock for a region never seen |
| P3-2 | Corrupt state: uncaught or exit 2 every tick | Fixed: `good_pointer` checks shapes; a refused pointer fails only around 00:00 and 12:00 UTC; a corrupt alarm record is rated |
| P3-3 | A NaN corner passed the check | Fixed |
| P3-4 | A punctual cron run froze a frame at + 0 | Fixed: no MRMS frame before slot + 4:30 (`MRMS_READY_S`) |
| P3-5 | Lag text wrong | Fixed: 13-96 s, CONUS the latest |
| P3-6 | Objects of a slot never recorded stay forever | Recorded (very small; a monthly sweep would cover it) |
| P3-7 | Slots with `r: {}` in the pointer | Recorded in the job's header; the client must not count them as frames (step 3) |
| P3-8 | Nearest-source regridding never samples 7.6 % of OPERA's echo pixels (the lattice is coarser than 1 km north-south) | Recorded; for the owner at the job-merge stop |
| P3-9 | Dead constant, header wording, User-Agent "satellite waves", one shared `format` alarm kind | Fixed: the job has its own `_get` and User-Agent (no e-mail); alarm kinds per region |
| P3-10 | CI runs pillow 11.2.1 / numpy 2.2.6, the job 12.3.0 / 2.4.6 | Recorded (the repo's convention); the runner dry run used the job's own environment |

Author's addition during the fix round: `mrms.read` also treats Pillow's `DecompressionBombError` as a damaged picture, and `main` guards the alarm pass itself (rated).

### After the fix round
- The reviewer's scripts on the fixed code: reader rows, one region's break (other regions and older slots publish), a cut body (Down, no alarm), a 403 (0 of 138 slots given up), a refused bucket (0 failed runs off the half-day), first tick, corrupt pointers, `gone`.
- Chaos: 700 ticks, 29 kills: 0 violations, 0 orphan prefixes, 0 leaks.
- Read-back on a fresh real slot (202611060210): 0 mismatches over 6 regions and 3 tiers (12,000 t0 + 1,380 t1 + 6,000 t2 samples).
- Tests: `test_radar.py` 91 -> 186; the job suite 697 + 1 skipped (one wall-clock test of the spectral job failed once under the full run's load and passes alone, as in section 83).
- Mutants: 55 of the fix round, all killed (4 first survived and were pinned: the delete limit in `main`, the bomb at the reader, a retired slot on a quiet tick, a local build's alarms). The reviewer's set: 52 of the 59 still apply, 51 killed after one more pin; 1 survivor equivalent (`raw <= nodata`: values below it already read as no coverage); 7 no longer match (their code was replaced; the fix-round set covers it). The author's first set: 38 of 39 applicable killed; 1 equivalent (the slot-level deadline, now also checked before each region).

For the job-only merge: writes ~135-138 objects a slot on a quiet night (~580k a month beside ~800k today, so ~1.4 M: ~$1.7 a month); the ceiling 410 a slot (1.77 M a month, ~$7).

## Step 4: the client on the test site

Test site `test` @ 4c86460, overlay asset 2.25.3, RADAR_OVERLAY=1; served overlay.js = the repo's by sha. Headless Chrome with the GPU, own profile, trusted mouse, touch and keys; desktop 1280x800 and 1920x1080, phone 375x812 and sideways. Owner's pick from the sheet: palette A (classic radar colours), tint 16 %.

### Held
- Opens paused on the newest frame (index 12 of 13), no sideways scroll, console clean, at every size.
- Pace on the desktop: snail 500/514 ms, fish 250/265, shark 129/135, 0 skips. Real pan and zoom while playing keep playing. Phone shark 127/135. Long tasks 0-3 ms/s.
- Loops 2 / 6 / 12 / 24 h = 13 / 19 / 25 / 25 frames, an independent list; gaps 10 / 20 / 30 / 60 min plus one 10-min step to the newest frame; reload opens paused on the newest, the loop kept.
- Hover = the drawn pixel: 15,600 samples over 13 views incl. world copies, 0 bad. Trusted hover texts "no radar coverage", "no echo", "29 dBZ · light rain · 7:24 PM".
- Readout against the published PNG by the browser's own decode: 3,558 + 499 echo samples at t0, t1 and t2 in ten views, 0 wrong. (One first "bad" sample was the check's own: at a region overlap the pixel belongs to the other region; `readoutAt` names the owner. Checked by owner afterwards; the 15 samples of that kind were left out.)
- Life: no model frame while on radar; back on wave height at its own time; hidden tab pauses and stops the pointer timer; Off leaves nothing (0 radar requests in 70 s); reload restores radar.
- Failure: a refused pointer gives "Overlay unavailable" with Retry within 0.8-5.8 s, the map alive; Retry works at once (309-328 ms); a new slot joins while playing; refused files say "Hawaii: the radar pictures are not answering. Trying again." and clear when they land.
- Sideways phone: a tap on Play works (the one earlier failure was the check's own: its long press had opened a forecast window over the button).
- Suites at b6e1951: pytest 2,473 passed + 1 skipped, Node 1,138.

### Found and fixed during it
| # | Finding | Outcome |
|---|---|---|
| V4-1 | Refused files left a healthy-looking panel over an empty map: a dry map and a failed one looked alike | Fixed in 2.25.2: the panel names the region and says the pictures are not answering |
| V4-2 | The speed picture's `src` was set on every step of every layer (`img.src` reads back absolute): a request per step with the cache off, an ordinary cache hit otherwise | Fixed in 2.25.3: `shownSrc`; 0 requests in 10 s of shark on test and production with the normal cache |

### Costs (browser cache off, the night of 2026-11-05 UTC)
| View | First picture | Whole 2-h loop | Store |
|---|---|---|---|
| Oahu, opening zoom | 1.5 s, 13 files, 0.02 MB | 51 files, 0.08 MB | 9.1 MB |
| US rain z6.5 | 1.9 s, 34 files, 0.27 MB | 53 files, 0.38 MB | 10.6 MB |
| US rain z9.5 | 2.6 s, 13 files, 0.15 MB | 66 files, 0.98 MB | 13.8 MB |
| Europe rain z5 | 1.3 s, 9 files, 0.14 MB | 14 files, 0.23 MB | 5.3 MB |
| Europe rain z9 | 1.6 s, 18 files, 0.35 MB | 53 files, 1.38 MB | 14.7 MB |

Heap vs Off 7.8 MB: radar 21-39 MB (47-51 at long loops), wave height at shark 62-82 MB. Store 20-27 MB in US rain; the 32-MB cap holds. R2 writes over the last 24 h: mean 138 objects a slot, max 149 = 0.60 M a month.

### On record for the reviewers and the owner
- At Hawaii's opening zoom 5 of 40 cell pieces flip tier up to 12 times in a loop: a t2 cell listed on some steps and not others draws t1 on the others (near T2_MIN 40). US z6.5: 0.
- The sideways phone's sheet keeps play, slider and speed, but has no loop select or legend.
- Italy, Austria and Greece read "no radar coverage" (OPERA holds no data from them).

## The owner's two requests during G56: sharper when zoomed out, playback kept

Owner: "improve the resolution at wider zoom extents", "but assure normal playback and performance is maintained". Built in 2.25.4: t1 from tile zoom 4 and t2 from 7 (were 6 and 9); `radarPic` keeps only a box round the rain, so the sharper tiers fit the memory; store cap 48 MB (32 MB under 768 px). Timed with the frames cold (gap p50/p90 ms, 0 skips): Europe z7 2-h 128/134 -> 127/135; US z7 126/135 -> 128/133; US z5 24-h 126/137 -> 124/132; Hawaii z8.8 24-h 130/137 -> 124/133.

## G56: two fresh reviewers on 2.25.3

A (code, Node on the real module, on Fable): 0 P0, 1 P1, 2 P2, 9 P3. B (test site, trusted input): 0 P0, 2 P1, 4 P2, 7 P3. Records: scratch `g56/a/g56-a-report.md`, `g56/b/g56-b-report.md`.

| # | Finding | Outcome |
|---|---|---|
| A-1 / B-1 (P1) | Paused mid-loop, a new slot arrives: the map shows the next frame under the old label | Fixed 2.25.4: the player keeps the shown time across a join |
| B-2 / A-2 (P1 / P2) | A step with no picture looks dry and reads "no echo", no note | Fixed: "no radar picture for this time" + the panel note |
| A-3 (P2) | A pointer time far ahead of the clock made the re-read a hot loop | Fixed: re-read delay bounded |
| B-3 (P2) | Phone long-press readout ran off the right edge | Fixed 2.25.5 (clamped, flips left; x 30/100/190/340 inside 375 px) |
| B-4 (P2) | While pictures load nothing says so; hover said "no radar coverage" | Fixed: "Loading the radar pictures.", hover "radar pictures not in yet" |
| B-5 (P2) | Hawaii's rain 2-4.6 times too large at tile zooms below t2 | Fixed by the new tier zooms + the job's T1_MIN / T2_MIN |
| B-6 (P2) | Play just after a view lands: fine picture drops to t0 for seconds | Fixed in two steps: 2.25.5 hold, 2.25.6 the newest frame stays through the hold (below) |
| B-7 (P3) | Refused pointer: no recovery in an open tab | Fixed 2.25.5: quiet re-ask each minute (recovered in 60.3 s) |
| B-8 (P3) | Damaged file final until the layer is left | Fixed: retried after 5 min |
| B-9, B-10 (P3) | Label vs readout time; "1307 min"; word vs rounded number | Fixed (label from the file's time; hours; word from the rounded dBZ; the note too in 2.25.7) |
| B-11, B-12, B-13 (P3) | Notice wording; sideways scale clipped; 28-px loop select | Fixed ("Modified by Allshore Surf"; folds when the scale cannot fit; 36 px) |
| A-4 / K1 (P3) | Tier flips in light showers, in the data | Owner: "write them all": job change T1_MIN, T2_MIN = 1, 1 on `job/radar-tmin` |
| A-5 .. A-10, A-12 (P3) | Pointer watchdog after Off; change-test field; box change; area bound; redraw on every pointer; withdrawn pending slot; FIFO comment | Fixed in 06d6160 |
| A-11 (P3) | Europe z7 long loops stay t1: past the memory budget | Recorded, owner told |
| Not changed | "rain" said over snow (owner awareness) | Recorded |

K1 measured on real frames, 3 slots: t2 cells per slot at need 40 / 10 / 1 = 105 / 112 / 114.7, +5.4 KB a slot; t2 writes 0.45 -> 0.50 M a month; t1 at 6 / 1 = 24 / 25.

## Re-check of the fixes on 2.25.5-2.25.7 (B's own scripts)

Found while re-checking:
- **R-1 (2.25.6).** `play()` moved to frame 0 before the hold, so the hold kept the tier rule on a frame holding only t0: Europe on 4G still went 10 -> 25 -> 100 ppd. Now the newest frame stays on the map until the stacks are in (3 s at most), then the loop starts at frame 0. Europe 4G stays 100 ppd throughout; Europe 1080p 100 ppd. Mutants 6/6.
- **R-2 (2.25.7).** The note counted from the slot, the label from the file: "43 min ago" beside "47 min old". Both count from the file's own time.

Held on 2.25.6 / 2.25.7, trusted input:
- Refused pointer: "Overlay unavailable" + Retry, recovers alone in 60.3 s. Slow pointer 10 s fine; 20 s the error panel with Retry. Unknown format and an HTML body: the error, no crash.
- Stale slots 2 / 4 / 12 / 60 / 130: notes in hours from 60 min. Frames gone from the bucket: the region's note, hover "radar pictures not in yet", recovered in 56 s once the pointer told the truth.
- A region's files refused: note, hover, playing goes on, recovered in 55.5 s. Newest 3 refused: "no radar picture for this time" there, steps back draw. A truncated t0: "cannot be read", other frames draw. Mid-loop gaps: "Hawaii shows 5:40 AM." then its own frames.
- Play at once on slow files: waits 3 s on the frame shown ("Loading the radar pictures."), then frame 0 with pictures (before: 10 steps with nothing drawn).
- Slot join paused at 2 h and 24 h: the label = the picture, hover = the label, a key steps one back.
- Markup in a region name shows as text. Credit opens above the phone sheet, links reachable; the notice page opens; the map does not move under the slider.

Timing on the served 2.25.7 (shark, cold frames, gap p50/p90 ms once loaded, 0 skips):
| View | Gaps | Long tasks | Store |
|---|---|---|---|
| 1920x1080 Hawaii z8.8 2 h | 128/133 | 0 ms/s | 3 MB |
| 1920x1080 US z7 2 h | 126/134 | 0 | 40.2 MB |
| 1920x1080 Europe z7 2 h | 129/135 | 0 | 32.1 MB |
| 1920x1080 US z5 24 h | 125/133 | 55 | 46.3 MB |
| 1920x1080 Hawaii z8.8 24 h | 125/135 | 99 | 5.4 MB |
| Phone Hawaii z8.7 2 h | 128/134 | 0 | 1 MB |
| Phone Europe z5 2 h | 130/133 | 0 | 13.9 MB |

While loading, in rain, the first 8 s show 28 steps instead of 43: the hold and the short step wait, by design. Tier flips remain at 3-44 cells until `job/radar-tmin` is live and the old slots age out (24 h).

Not checked: Guam in rain, rain crossing a seam, echoes above 48 dBZ (none on the nights tested), a real iPhone or a real slow link.

## Fresh re-check of the fixes (one reviewer on 22787f4), and the last fix round (overlay 2.25.8, f5837d8)

Asked for by the plan's step 5, because the store, layer and player had changed. **0 P0, 0 P1, 1 P2, 6 P3.**

| # | Sev | Finding | Outcome |
|---|---|---|---|
| R-1 | P2 | Pause leaves the frame on the map at its playing tier (Europe z7 6-h: 70 pieces at 25 per degree) | Fixed: pause plans again and redraws (69 pieces at 100) |
| R-2 | P3 | The first plan's "doesn't fit" was never revisited, so a loop picked before Play stayed coarse | Fixed: Play plans again (US z4 24-h playing keeps 25 per degree, the stack on; was all 10) |
| R-3 | P3 | Retry could fail during recovery's own read | Fixed: a read stopped and asked again; Retry -> the layer within 5 s |
| R-4 | P3 | A filled hole + a slot join: the label and the map could differ | Fixed: the join compares against the step key before it |
| R-5 | P3 | A hidden tab went on retrying | Fixed: hidden asks nothing, shown asks again |
| R-6 | P3 | "No picture for" gave the slot's time, not the file's | Fixed: every note uses the file's own time |
| R-7 | P3 | Changing the loop during Play dropped to the coarsest tier at once | Partly: the hold restarts. See below |

**R-7 as it stands.** Europe z7, files slowed to 500 ms, 2 h -> 6 h while playing: the frame on the map stays at 25/100 per
degree for 3.25 s (the hold's cap), then 10 per degree for about 2.4 s, then 25 for the rest of the loop. Before the fix the
same change showed 10 per degree for about 5 s straight away. Accepted: the hold is bounded on purpose, so Play never looks
dead, and new frames that have not landed can only draw their coarse tier. On a normal link the gap is shorter.

Held, per the reviewer:
- the hold is never stranded;
- recovery stays quiet;
- the readout equals the drawn pixel at 61,980 samples;
- the memory caps are 48 / 32 MB;
- the readout's placement is right.

The reviewer could not check a real iPhone, iPad or Safari, a real slow link, a real slot arriving during a hold, a live 5-minute damaged-file retry, or real heap.

### 2.25.8 on the test site
- Served since 17:31:16 UTC, sha = repo.
- Mutants of the round: 14 of 14 killed.
- Full suites on 2.25.8: pytest 2,473 passed, 1 skipped; Node 1,166 passed.
- A fresh page, desktop and phone: opens paused on the newest frame, Play works, no sideways scroll, 0 console problems.
- Timing (shark, cold frames, gap p50/p90 ms, 0 skips): Hawaii z8.8 126/135, Europe z7 129/136, phone Hawaii 129/133.

### On record for the owner
- **Europe z4, 24-h, on 1080p plays at 10 per degree.** The stack estimate is cautious near the cap (a later plan finds it would fit at 45.4 MB). At zoom 4 a 10-per-degree node is about one screen pixel, so this was accepted, with A-11 (Europe z7 long loops stay at the middle tier).
- The rain word is used over snow too.
- Tier flips last until `job/radar-tmin` is live and the old slots age out (24 h).

## The job-only merge of the tier rule (2026-11-06 17:51 UTC)

The owner's "Write them all": the job writes a sharp picture for any cell with rain at 10 dBZ or more (`T1_MIN, T2_MIN = 1, 1`).
Tag `prod-pre-radar-tmin` @ 952a029; `Live-Buoy-Update` fast-forwarded to 8d4a640 (two files: the job and its tests).

- The three page probes are byte-identical to the pre-merge capture.
- `model-frames-tests` is green on 8d4a640, and every publisher's ticks since are green.
- Three new frames (17:50, 18:00, 18:10 UTC) hold 151-156 objects; the frames before them hold 141-146 (t2 111-117 -> 121-125, t1 unchanged). That is about 10 more files a frame, the estimate held, and about 0.65 M writes a month at this rain.
- The 18:00 frame mirrored from the bucket (151 objects, every pointer key answered) and read back against NOAA's and OPERA's files with eccodes + pyproj: 0 mismatches in 12,000 t0, 1,440 t1 and 7,260 t2 samples.
- The rule, on that frame, from NOAA's files directly: Hawaii 3, Guam 4, Caribbean 5, Alaska 10 and CONUS 42 cells with rain at 10 dBZ or more all have a picture, and no cell has a picture without rain. Europe's cells were covered only by the read-back above, not by this count.

Old frames keep their old sparse lists until they age out (24 h), so tier flips can still show in long loops until then.
