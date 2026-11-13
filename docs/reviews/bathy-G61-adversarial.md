# G61: the bathymetry overlay client (plan section 93b, step 6)

Target: `feat/bathy-client` @ 4b348a3 (overlay 2.27.0), test site @ 61280c9 with `BATHY_OVERLAY=1`.
Reviewers, fresh context: A = code and Node harnesses (model fable); B = the test site with trusted input (headless Chrome).
Reports: A scratch `g61/a/g61-a-report.md`, B scratch `g61/b/g61-b-report.md` (scripts, outputs and screenshots beside them).

## Step 5 (the author's test-site check before the review)
- 13 views at 1280x800 (Pipeline z15/z16, Kaneohe z13, Guam z13, Samoa z12, PR z12, USVI z11, Hawaii z8/z7/z6, Big Sur
  z9 GEBCO only, Bali z10, world z3) and 5 at 1920x1080: readout = drawn pixel, 0 mismatches in 26,700 samples.
- Hover text = the drawn sample 72/72 (US + Metric); long press 3/3 upright, 5/5 sideways.
- First view at Pipeline z15: 25 requests, 525 kB, 5 s. The 32-MiB store cap held. Contour levels by zoom and unit as
  designed. Slider stored. `/notice/bathy` 200. Off: 0 requests while panning. Phone sheet fits. Console 0.
- Found: V5-1 (= K1 below) a straight edge in deep water where a 3-m grid ends; V5-2 (= A8 / B-10) a duplicated source line.

## Counts
A: 0 P0, 2 P1, 2 P2, 6 P3. B: 0 P0, 1 P1, 4 P2, 7 P3.
Overlaps: K1 = B-2, A5 = B-8, A4 = B-12, A6 = B-6, A8 = B-10, A7 is part of B-5.
Combined: **0 P0, 3 P1, 4 P2, 10 P3.**

## P1
- **A1 (A)** The store's eviction (`_evict`) protects only tiles a map tile has already drawn. Once the tiles in use pass the
  cap (126 data tiles), each tile that lands is evicted in the same call, asked for again, decoded again, without end;
  those map tiles never show the regional depths. A 4K screen passes the cap in view at 11 of 25 measured views; 1440p and
  1080p with Leaflet's kept ring at a few. Confirmed by reading the code.
- **A2 (A)** GEBCO's shallows (under 30 m) are switched off for a whole map tile when any regional tile covers it, also
  where the region has no data. Holes in open water: 14 % of GEBCO water off the BVI at z11, 21 % on the shelf west of
  Puerto Rico, the reef off eastern Upolu (outside the Samoa box). Confirmed by reading the code.
- **B-1 (B)** GEBCO is drawn on land inside regional tiles. The builder writes depth code 0 both for "land" and for
  "outside the source"; where CUDEM says land but the GSHHG coastline says water, the sampler falls through to GEBCO.
  Koko Head cliffs read "~472 ft (GEBCO, regional estimate)"; on Tutuila's north coast GSHHG lies up to ~1 km inland, so
  GEBCO colours the hills between the bays; ~2,475 such pixels at z11 across Hawaii. Breaks owner decision 2 (water only).
  A2 and B-1 have one cause: the client cannot tell land from "no data".

## P2
- **K1 / B-2** Straight edges where NOAA's 1/9" grid ends: every missing 0.25-degree NOAA tile (21.25 N off Diamond Head and
  Waikiki, 20.5 N through Kaho'olawe, Kaneohe) and every region-box edge (22.25 N off Kaua'i north). The contrast is mostly
  GEBCO's hillshade (exaggeration 16 at its zoom 7; slopes away from the light go black), plus a depth step of a few per
  cent. A per-tile distance feather is not sound (continuity across tiles; code 0 is also land, so it would blend GEBCO
  into every reef). Sound designs: A's coarse-tile weight with an open-water mask, needing B-1 fixed first; or builder-side.
- **A3 (A)** One retry timer, armed only when a failure happens: a second failure inside the first one's minute is never
  retried until some unrelated redraw (also the region index files).
- **B-3 (B)** On GEBCO-only coasts the colour stops up to one GEBCO node (~1.2 km) short of the shore, in blocky steps
  (Santa Monica Bay, Uluwatu): GEBCO's land nodes are code 0 and the sampler needs a quarter of the weight.
- **B-8 / A5** Every zoom blanks the layer: `createTile` reports a tile ready before it is drawn, so Leaflet removes the old
  level at once; the new tiles fill in over ~1.4 s.

## P3
- A4 / B-12 contours jog by 1-2 px along a map-tile edge (apron 1 px for two smoothing passes).
- A6 / B-6 `/notice/bathy`: GEBCO's citation lacks its DOI and the non-endorsement line; CUDEM's citation names CIRES; the
  quantisation sentence leaves out the 10-cm band.
- A7 / B-5 "Bathymetry: 1 sources"; numbers without a thousands separator ("~16404 ft", "5000+ m"); a decimal on GEBCO
  estimates; raw source names in "Sources here".
- A8 / B-10 the same CUDEM line twice (PR + USVI, Guam + CNMI); the readout cannot tell them apart.
- A9 27 of 54 mutants survive the suite (14 hide real behaviour); a radar test is load-sensitive.
- B-4 sideways phone: the legend's last label cut ("16,400+ f").
- B-7 GEBCO pixels contoured with the reef-scale lists at close zooms.
- B-9 "Sources here: none in view" while the first tiles load; failures silent for their minute.
- B-11 phone: the opened credit lies over the sheet.

## Held up
- A: the depth table bit-exact at all 65,536 codes; 9,820 pixels / 183 tiles = an independent Python read; presence bits
  195/195; 404 / 410 final, 403 / 500 / 503 retried; Off leaves nothing; contour levels; coast hooks handed back; credit
  subsets <= 29 characters; the flag-off page unchanged.
- B: five sizes without sideways scroll; slider by mouse and touch, kept across reload; units; hover and long press =
  readoutAt, 0 mismatches over ~30,000 samples incl. GEBCO, world copies and across 180; 0 requests Off or under Radar; the
  wave layer's coastline unchanged afterwards; console clean; cold first views 1.2-2 s; 0 long tasks while panning (Radar 6);
  store fixed at 32 MiB over 60 views, heap flat.

## Decisions and fix round
Owner decisions (2026-11-12, AskUserQuestion):
1. Rebuild the data with a land mark; all five regions become v2, each upload with the owner's go-ahead.
2. Fill the 1/9" gaps with NOAA's 1/3" CUDEM, and calm GEBCO's shading where it meets finer data.
3. On GEBCO-only coasts carry GEBCO one or two cells onto land (flagged filled) so the coastline decides the edge.
4. Fix the zoom blink in the bathymetry layer only, now (wave and radar later).

Builder (`feat/bathy` @ 4eadaa4, data v2):
- The source plane marks the region's own land (`LAND_ID` 30) apart from "outside the survey" (0); `land_id` in each
  index. Fixes A2 and B-1 at the cause.
- NOAA's 1/3 arc-second CUDEM as each region's second source (to zoom 14): the 21.25 N, 20.5 N and Kaneohe gaps are now
  survey data, not GEBCO (K1 / B-2, with the softer GEBCO shade below).
- GEBCO water carried two cells onto its land (filled + coarse) at zoom 7, wrapping at the date line (B-3).
- 21 new builder tests, 26 mutants killed; a local Oahu box read back against NOAA's grids.

Client (`feat/bathy-client`, overlay 2.27.1):
- A1: `_evict` also keeps every tile a map tile still wants and the tile that just landed; over the cap until one can go.
- A2 / B-1: with a land mark, land is land (nothing drawn, nothing below it) and code 0 without it lets GEBCO through.
  GEBCO stays out of the shallows only while a region's index, presence file or tile is on the way or cooling down, and
  over a v1 tile. The hold is judged per map tile from what the region has there.
- K1 / B-2: the hillshade at 35 % on GEBCO and other coarse pixels (60 % on survey grids).
- A3: one retry timer that re-arms for the next failure still cooling down; a stalled download counts as a failure.
- B-8 / A5: a new map tile reports ready after its first full draw (none of its files still on the way) or after 2 s;
  never twice.
- A4 / B-12: a 3-px apron read from the neighbouring data tiles (own edge clamped where none is held); a neighbour that
  lands later redraws the tile.
- B-7: GEBCO and coarse pixels take the sparse zoom-0 levels at every zoom; no line where the two kinds meet.
- A6 / B-6: `/notice/bathy` names CIRES and NCEI, the 1/3" grid, GEBCO's DOI and its no-endorsement line, the three
  quantisation bands and the GEBCO-onto-shore step.
- A7 / B-5 / A8 / B-10: "1 source"; thousands separators; whole numbers from 100 up and on GEBCO estimates; sources by
  label and grid size, each printed text once.
- B-9: "loading…" while files are on the way; a note while a tile in view waits out a retry.
- B-4: the legend's last label ends 6 px in from the sheet edge. B-11: the opened credit gets an opaque box over the sheet.
- A9: `tests/overlay/bathy_v2.test.js` (40 tests on three v2 tiles built locally by the builder: land mark, 1/3" fill,
  apron, ready rule, hold, seam, words, store cap / eviction / retry / stall / Off). 28 targeted mutants, all killed
  (two first survived: a tile landing when every older tile is in use, a number between 100 and 1,000; tests added).

Suites at the fix round: Node overlay + UI 1,286 (one load-sensitive wind-particle timing test failed once in the full run
and passes alone 3/3; untouched code); pytest builder + flag + page 109 passed, 3 skipped; full pytest 2,883 passed, 4 skipped, 1 failed: the
section-63 trickle test (`test_capped_fetch_cuts_a_trickle_smaller_than_one_chunk`, a local socket read timing out under
load; untouched code; passes alone 6 of 8 with a review agent running beside it).

Still to do: one fresh reviewer on the builder (4eadaa4); then the job-only merge and the v2 uploads (each with the owner's
go-ahead); then the client on the test site against v2 and a short re-check; then the site merge.
