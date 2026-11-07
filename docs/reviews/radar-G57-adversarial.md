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
