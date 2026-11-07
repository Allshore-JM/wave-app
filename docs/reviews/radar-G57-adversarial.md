# Radar stage 2 (Japan and Taiwan): record of plan section 86, gate G57

## Step 3: the readers (author, feat/radar2 @ 59e171c)
- `tools/model_frames/jma.py`, `cwa.py`, rows in `radar.py`, `tools/tls_check.py`, fixtures, `tests/model_frames/test_radar_asia.py` (70 tests).
- Read-backs against the sources with independent code: Japan 1,200 t2 points 0 wrong, 600 t0 coverage pixels 0 contradictions; Taiwan 2,000 points 0 wrong, 1,000 points 0 missed, 1,000 t0 pixels 0 wrong.
- Mutants 50 of 52 killed (one equivalent; one closed by a tightened test).
- Runner dry run: 3 slots of each source, 6 frames in 9 s. Japan ~43-44 KB a frame, t1 4 cells, t2 13-15.
- The rain pixels Japan's mask drops (810-1,032 a frame): measured on one frame, all 1,032 lie in 122.0-122.5 E, 23.3-25.2 N, the designed cut beside Taiwan. None elsewhere.
- Slip on record: a piped test run hid a failure, 931a5eb went out with a stale pin and sent the owner one "Run failed" mail.

## Step 4: the client (author, feat/radar2 @ cc66d05, overlay 2.25.9; test @ 6e22f60)
Suites: overlay Node + site pytest green; client mutants 26 of 26 killed.

Test site, headless Chrome + GPU, trusted input. Japan's and Taiwan's frames served from a local job folder by DevTools Fetch (scratch radar2/v4: `s/v86.js`, `expect86.py` = an independent read of the PNGs):
- 835 sampled points at each tier (z3 t0, z5.5 t1, z8.4 t2), both regions: layer readout = the frames, drawn pixel = the colour table (premultiplied rounding <= 3). Points where the other region owns the pixel counted apart (K4).
- Japan: every drawn pixel one of the class colours or the tint, at z3, z5.5, z8 (up to 1.2 M pixels).
- Mouse hovers: 40 of 40 say the file's class. Long press on a phone says the class.
- No coverage reads "no radar coverage", covered and dry "no echo", the tint's alpha as designed.
- Credit, head and class note follow the view at 1280x800, 1920x1080, 375x812 (Hawaii "Radar: MRMS"; Japan "Radar: JMA"; Japan + Taiwan "Radar: JMA · CWA"; + Guam "Radar: 3 sources"; world "Radar: 4 sources"; open ocean "Radar: sources"). The longest head fits its panel.
- 2-h loop 13 frames, 6-h 19, 12-h and 24-h 25; plays, pauses, keeps its time across a loop change.
- Taiwan behind by one slot: label = its own frame, no note. Behind two: the 30-min warning. Three: nothing drawn, the warning stays.
- A region joining an open tab: 127.7 s (the settle read at publish + 130 s), one pointer read. Credit, head and hover follow.
- A refused Japan t0: "Japan: the radar pictures are not answering. Trying again."; Taiwan still reads.
- The US, Europe, Hawaii and the Caribbean against production 2.25.8 on the same pointer: 156 tiles, 0 different.
- World 24 h with every slot holding Japan and Taiwan: store 31.3 MB of the 48-MB cap (1920x1080), 14.4 of 32 (phone); no failed file; plays.
- `/notice/radar` carries the four sources' wording. Fresh-page console clean (only injected refusals).
- Wrong expectations of my own, not defects: tier zooms from before the stage-1 resolution change; Taiwan t0 points beside dry pixels (re-run with whole 3x3 echo: 265 points, 0 wrong).

## G57: two fresh reviewers on cc66d05 (overlay 2.25.9; test @ 6e22f60)
A (code and data, no browser, Fable): reports scratch `g57\a\g57-a-report.md`. B (the test site in headless Chrome + GPU, trusted
input; Japan and Taiwan served from local job folders by DevTools Fetch): `g57\b\g57-b-report.md`.
**No P0. 3 P1, 1 P2, 12 P3** (A: 1 P1, 1 P2, 6 P3; B: 2 P1, 1 P2, 6 P3; A's P1 = B's P1-2, A's P2 = B's P2-1).

| # | Finding | Outcome |
|---|---|---|
| P1 A-1 = B-2 | A region written later in the same tick never reached an open tab until the next tick: the job stamps one `published_utc` per tick and `_take` compared the stamp only (latent since stage 1 for Europe) | Fixed (2.25.10): `_take`'s signature = the regions + slots; a regions block alone counts as a change |
| P1 B-1 | Rolling the job back after Japan and Taiwan are in the bucket would blank ALL radar on every new page for up to 24 h: the old job rewrites `regions` and leaves slot entries naming regions the client refuses | Fixed both sides: the job's `drop_unlisted` (23275a7) strips such entries on its first tick and writes the pointer; the client ignores slot entries of unlisted regions (2.25.10). README runbook: stop a source with `RADAR_SOURCES`; a rollback is safe from the commit that added `drop_unlisted`. A 2.25.8 tab still refuses such a pointer: the strip must reach production before the job merge (the ship order) |
| P2 A-1 = B-1 | In the 6/12/24-h loops a region one slot behind drew an older mark frame although a fresher file existed, and the panel said "X shows ..." most of the time | Fixed (2.25.10): the loop's newest step resolves to the region's newest frame within the hold, its cells are asked for and wait like any shown frame |
| P3 B-1 | A pointer dropping a region on screen left it painted, mislabelled; a hover could throw | Fixed: `_boxesMoved` drops its tables, coverage and pictures; `covRev` / `rev` bump; one redraw; `readoutAt` silent for a tile from an older pointer; `_radarReadout` null for an unlisted region |
| P3 B-2 | The class note was unhidden after the sheet was measured (an inner scrollbar at 320x568 / 360x640) | Fixed: the note is built with its text before the measure; a compact sheet re-lays once when it appears or goes |
| P3 A-3 | Japan's outline check was an area band only (an extra ring could flip the mask) | Fixed (27b7c1e): Tokyo, Sapporo, Fukuoka and Okinawa must be inside, three far corners outside |
| P3 A-6 | `cwa.read`'s header check passed `nan` | Fixed (27b7c1e) |
| P3 A-4 | Two test gaps (an RGBA tile with alpha 1; the two sources' zero offset; the credit) | Fixed (27b7c1e): tests |
| P3 B-5 | The notice's "Modified" line read as if Japan had dBZ | Fixed: Japan's rain-rate classes named apart |
| P3 A-1 | The settle read doubles an open tab's pointer reads (~18 an hour) | Accepted (60-s pointer, ~1 KB) |
| P3 A-2 | "Radar: sources" when nothing in view has frames | Accepted (wording) |
| P3 A-5 | A JMA frame can overrun its 45-s spend by one frame + one tile | Accepted (bounded) |
| P3 B-3 | Head, credit and notes follow the box, not what is drawn | Accepted (over-crediting harmless) |
| P3 B-4 | An unknown scale value reads as dBZ | Accepted (only if the job adds a scale) |
| P3 B-6 | A 2.25.8 tab open across the job merge shows Japan as blended dBZ with the old credit | Accepted (the ship order: site first, then 24 h, then the job) |
| K4 | Taiwan's echo west of 122.5 E is hidden under Japan's mask: a straight wall from zoom 9 (about 2 % of Taiwan's t2 echo; up to 64 px >= 30 dBZ in a frame) | Owner awareness |

Held (with numbers, both reports): hover = canvas in ~40,000 hovers at z2-z11, 0 dBZ text in Japan, every Japan pixel one of the 8 classes; the
seam at 122.5 E without a gap; credit 10-29 characters at every view and size; playback snail / fish / shark, out-of-order 0; failures (503,
404, refused, garbage, slow) fall back by tier with the right note; US / Europe tiles 1,128 identical to production; real JMA tiles and a real
CWA file decoded independently (0 wrong); radar.py isolation 19 checks; a worst-case pointer 73,841 B.

## Fix round (feat/radar2 @ 27b7c1e, 23275a7, dc489d7 = overlay 2.25.10; test @ 47d5646)
Suites at dc489d7: pytest 2,503 + 1 skipped, Node 1,187. Mutants: client 15/15 + 16/16, job 3/3 (scratch `g57\fix\mut2.py`, `mut_client.py`).
One test (a frame's file redraws) had passed only at some hours of the day: its end time is now pinned off the hour marks.

## Live results on the test site
On 2.25.10 (Japan and Taiwan from a fresh local build, 20 slots):
- The rolled-back pointer: a new page and an open tab keep their radar; production 2.25.8 refuses it, as expected (hence the ship order).
- Sheets 320x568 to 1024x768: no inner scrollbar; fold and unfold fine; console 0.
- Long press with the class text at a rain point: 16 of 17. FOUND: the one miss = a stale hide timer. A lift's 1.5-s hide timer, still
  pending, hid the next long press's readout (`s\lp.js`: gaps of 300 and 700 ms between presses lose the readout). Also on production 2.25.8.

Fixed in overlay 2.25.11 @ 5531b3a: one hide timer at a time (a new press cancels it, each lift replaces it). Test 'G57: one hide timer at a
time' fails on 2.25.10. test @ 0f1cff0 serves 2.25.11 (sha = repo, 2.25.10 URL 404).

On 2.25.11:
- Long press: shown at gaps 300 / 700 / 1,100 / 1,700 ms; class text 17 of 17 (phone, tablet, sideways).
- A late region under the same stamp: Japan and Taiwan join on the settle read (108 s), their files asked 6 ms later, head and credit follow.
- Taiwan one slot behind in the 2 / 6 / 12 / 24-h loops: no "shows" note, the label gives the held frame's own time, the age warning only past
  30 minutes.
- Against production on the same pointer: 1,128 tiles identical (804 desktop, 324 phone); first-view requests equal; fresh-page console 0.
- Playback at snail / fish / shark in every loop (out-of-order 0), slider, keys, reload, hidden tab, Off and back to wave height.
- Both sites' storage emptied; the browser stopped.

## Fresh re-check of the fix round (one reviewer, Fable; cc66d05..5531b3a + the test site on 2.25.11)
Report: scratch `g57\recheck\recheck-report.md`. **No P0, P1 or P2; 4 P3.** Claims R1-R12 held under every attack (Node 84/84, pytest 286;
on the test site the rolled-back pointer is taken and dropping Japan, Japan + Taiwan or Taiwan while on screen is clean, no TypeError).

| # | Finding | Outcome |
|---|---|---|
| P3-1 | The stack budget counted the held frame's cells twice (in the 2-h loop the held frame is always a loop frame), so a view near the cap fell to a coarser tier early | Fixed (2.25.12 @ 883246c), wider than proposed: the shown frame's cells were counted twice as well (the reviewer's one-line fix left a 504,008-B gap). Each cell now counts once; the test checks the smallest working cap = the planned bytes at the newest and the first step |
| P3-2 | A region 3+ slots behind at the newest step: the mark fallback draws a frame 40-80 min behind in the 6/12/24-h loops, labelled "X shows ..." | Accepted (bounded, labelled; stage-1 behaviour) |
| P3-3 | Long press, lift, then a touch the browser cancels: the readout stayed on screen (nothing bound to touchcancel) | Fixed (2.25.12): touchcancel ends a touch like touchend, and is unbound with the others |
| P3-4 | Japan's outline check is point-based: an extra ring over Shikoku alone (no sanity point) passes | Recorded (contrived) |

Mutants of the 2.25.12 fixes 4/4 killed (each cell once: the planned-set check and the counted-set check; touchcancel bound and unbound).
Suites at 883246c: Node 1,189 / 1,189, overlay page + flag pytest 63. One test bug found on the way: the touch test first called the harness's
stubbed unbind; it now calls the real one.
