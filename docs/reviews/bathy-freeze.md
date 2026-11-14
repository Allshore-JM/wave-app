# Bathymetry freeze around Puerto Rico (2026-11-13)

Owner report: "loading of the bathymetry is freezing when I zoom around Puerto Rico. Hawaii loads fine."

## Cause
Production (overlay 2.27.3) redrew waiting tiles from an idle callback with a 500-ms patience limit. When the page stayed
busy past that limit (a pinch zoom), the timed-out callback drew EVERY waiting tile in one task: 40-54 tiles, 437-731 ms
at San Juan zoom 11. Puerto Rico's view holds more tiles with data than Hawaii's typical views, so the task ran long enough
to be seen as a freeze.

## Fix
- 2.27.4 (fc201b0): tiles drawn in slices of at most 12 ms (a running average of a tile's cost), in-view tiles first; in
  idle time a slice lasts while the idle period has room for another tile. Measured: no bathymetry task over 68 ms, but
  Chrome ran the back-to-back slices without painting between them (32 slices inside one 0.93-s frame gap).
- 2.27.5 (53cb65a): on a busy page the next slice runs in a task posted from the next animation frame (after it is
  painted); a 100-ms timer posts it when no frame comes (hidden tab). The layer has its own cancel. Tests: frame pacing,
  fallback, cancel, one slice per frame over a whole redraw; mutants 10/10 (the 2.27.4 set 9/9 still killed). Overlay
  Node 671 pass; flag tests 22 pass.

## Measured (headless Chrome, GPU, 1280x800, San Juan z11, 4 short + 3 long continuous pinch rounds)
| Run | Version | Longest bathymetry task | Long tasks (max) | Frame gap max | Gaps > 500 ms |
|---|---|---|---|---|---|
| prodB | 2.27.3 | 695 ms (54 tiles) | 8 (694 ms) | 1,533 ms | 4 |
| prodC | 2.27.3 | 658 ms (48 tiles) | 6 (657 ms) | 750 ms | 3 |
| testD | 2.27.5 | 27 ms | 1 (69 ms) | 1,267 ms | 4 |
| testE | 2.27.5 | 23 ms | 1 (55 ms) | 550 ms | 2 |
| testF | 2.27.5 | 26 ms | 4 (87 ms) | 733 ms | 2 |
| testG | 2.27.5 | 23 ms | 2 (67 ms) | 1,050 ms | 2 |
| testOff2 | no overlay | - | 1 (55 ms) | 700 ms | 1 |

No long task in any 2.27.5 run came from the bathymetry layer. The remaining frame gaps (up to 1,267 ms, also with no
overlay at all) were first put down to the headless browser; the owner did not accept that, and a trace found the real
cause (below).

Regression on the test site (2.27.5): 48 views desktop + phone, readout = drawn pixel, 0 mismatches; land points, hole
fill, Big Sur shore, hover, contour levels, slider, notice, Off (0 requests) as before; 0 console problems.

Not checked: a real phone / iPad pinch (the owner's device).

## Second cause: the border lines (owner, 2026-11-13: "The freezes are still unacceptable")
Chrome traces of the same pinches (GPU process, main thread) showed the stalls in the GPU's canvas raster, not in any page
task: at San Juan one raster task took up to 539 ms and 18.0 s of raster in all; frame gaps reached 1,826 ms. Switching
features off one at a time (localStorage, same pinches): borders off -> raster max 73 ms, frame gaps max 124 ms (none over
250 ms); gridlines off or names off -> no change (884 / 741 ms gaps). Waikiki: raster max 13 ms.

The border layer (static_ui/places.js) drew the WHOLE of every line of each 10-degree piece on every 256-px tile that touched
the piece, the parts far outside the tile included. State and disputed lines are dashed, so each tile asked the GPU for
thousands of dashes; the lines of the cell around Puerto Rico (and its neighbours) made that cost seen. Hawaii's cells hold
almost no lines.

Fix, UI 1.30.5 (3372481; golden 8f9c443): each line is clipped to its tile, grown by 4 px (Liang-Barsky per segment); a line
that leaves and re-enters becomes separate runs; each run carries the line's length before it, and a dashed run is stroked
with that as its dash offset, so the dash pattern runs on unbroken across tile edges (the same pattern as drawing the whole
line). Solid lines are one path as before. Tests: clipping (a long state line, an out-and-back line), the dash phase equal on
both sides of a shared tile edge, drawTile's offsets; the old "every step kept" test rewritten; 49 places tests pass; the
mutants (clip, offset never counted, run start) killed, two redundant guards equivalent.

## Measured after the second fix (test site, UI 1.30.5 + overlay 2.27.5; production = UI 1.30.4 + overlay 2.27.3; same
## machine, headless Chrome with the GPU, 1280x800, storage cleared before each run, the pinches above)
| Run | Long tasks (max) | Frame gap max | Gaps > 250 ms |
|---|---|---|---|
| San Juan, bathymetry, production | 5 (444 ms) | 850 ms | 7 |
| San Juan, bathymetry, test | 0 | 33 ms | 0 |
| San Juan, no overlay, production | 0 | 433 ms | 5 |
| San Juan, no overlay, test | 0 | 33 ms | 0 |
| San Juan, wave height, production (3 runs) | 11-16 (92-122 ms) | 434-833 ms | 3-7 |
| San Juan, wave height, test (2 warm runs) | 10 (78-90 ms) | 83-500 ms | 0-1 |
| Rincon / USVI / Guam / Waikiki, bathymetry, test | 0-1 (64 ms) | 18-50 ms | 0 |

The first test run with wave height after the deploy (cold frames for the test site's origin) had 28 long tasks up to 279 ms;
the warm repeats above did not. Traces of the default page (wave height) at San Juan: GPU raster 18.0 s -> 7.2-7.5 s,
longest raster task 539 ms -> 27-35 ms, frame gaps max 1,826 ms -> 80-115 ms, none over 250 ms.

Borders look the same: screenshots of production (whole lines, the dash phase right by construction) and the test site at
Germany, Kashmir and the central US (z6.4) differ by at most 31 of 255 in any pixel (anti-aliasing); Puerto Rico z11 only where
a live wind flag changed between the captures. Dashed state lines cross the tile edges unbroken.

Not checked: a real phone / iPad pinch (the owner's device).

## G62: a global freeze review (owner, 2026-11-13: "do a global review to make sure similar freezing behavior does not
## occur elsewhere. fix any issues found before going to production")
One fresh reviewer (G62 A, read-only, Node timings on the module's own functions with real fixtures; d5ce74e) looked for
the same "everything in one task" pattern across the map. Ten findings:

| # | Finding | Severity | Outcome |
|---|---|---|---|
| 1 | Wave, satellite and radar layers drew every new tile of a zoom in Leaflet's one task (30-84 tiles; 40-300 ms of compute at 1280x800-1920x1080 with contours) | P0/P1 | Fixed: the bathymetry layer's sliced drawer shared by all four layers (tiles in view first, 12-ms slices, a frame painted between them); a new tile reports ready after its first draw, or after 2 s at most |
| 2 | Playback kept stepping during a pan or pinch; each step redrew every tile in range in one task (115-500 ms in a pinch out at 1920x1080) | P1 | Fixed: a step waits from movestart / zoomstart to moveend (polled every 50 ms; never longer than 5 s) |
| 3 | Exposure tool: the swell window's veil was a Leaflet polygon of 60,000-300,000 points in three world copies, projected on every zoom end | P1 (tool open) | Fixed: only the world copies near the view; the veil's point spacing by zoom band; a pan, zoom or resize that changes either redraws |
| 4 | Touch screens: the live-dot touch pads checked every live dot against every forecast station in one task | P1 (touch) | Fixed: an index per list (32-px squares) |
| 5 | Live-buoy dots: every one rebuilt as a DOM marker after a pan | P2 | Fixed: only the dots that change are rebuilt |
| 6 | A timed-out idle callback drew every waiting buffer tile in one task | P2 | Fixed by finding 1's drawer: a timed-out callback draws one 12-ms slice, then waits for the next frame |
| 7 | Coast data landing redrew a whole view in one task | P2 | Fixed: the tiles are marked stale and drawn by the sliced drawer |
| 8 | Gridline labels: a forced layout per label on every move frame | P3 | Fixed: sizes cached, labels hidden during a zoom |
| 9 | Satellite dots at low zoom | P2/P3 | No change: bounded (7-33 ms at the reading cap in Node); revisit only if a browser trace shows it |
| 10 | Window resize ran enforceSingleWorld on every event; a height-only resize rebuilt all marker layers | P3 | Fixed: at most once per 100 ms; a height-only resize goes through the debounced marker refresh |

Commits on fix/bathy-freeze: da74bcd (the fixes, overlay 2.27.6, UI 1.30.6), 3ec7caa (golden), 139d22c (two pins added
after the mutation pass below). test @ bb1d891.

Mutants on a scratch copy (each fix's own tests; tests added where one survived):
| Finding | Mutants | Killed |
|---|---|---|
| 1 | C1-C7 (createTile draws at once / queues nothing, no 2-s ready timer, view order ignored, slice unbounded, drawn tiles redrawn, leftover never queued) | 7 / 7 (C2 by the 139d22c pin) |
| 2 | H1-H6 (movestart / zoomstart set no hold, moveend keeps it, the tick ignores it, no 5-s cap, a held tick never retried) | 6 / 6 (by the 139d22c pin) |
| 3 | F1-F12 | 12 / 12 |
| 4 | F01-F16 | 15 / 16 (F07, the pole fallback, changes speed only) |
| 5 | L1-L9 | 9 / 9 (L9 after a pin) |
| 7 | R01-R16 | 16 / 16 (after pins) |
| 8 | G1-G10 (9 mutants) | 9 / 9 (G10 after a pin) |
| 10 | source-text pin (tests/test_index_page.py) | - |

Suites at 3ec7caa: pytest 2,884 passed + 4 skipped; Node UI 639, overlay 677. 139d22c adds the two pins (overlay.test.js +
playback.test.js 238 pass; on the test branch 237).

Live, test site (headless Chrome with the GPU, 1280x800): the swell window (exposure tool) left open, then zoomed out and
pinched (production overlay 2.27.3 / UI 1.30.4 against the test site 2.27.6 / UI 1.30.6):
| View | Long tasks | Longest | Sum | Gaps > 250 ms |
|---|---|---|---|---|
| zoom 3, pinch | 100 -> 2 | 381 -> 53 ms | 8.6 s -> under 0.11 s | 5 -> 0 |
| world, pinch | 212 -> 29 | 466 -> 99 ms | 22.3 s -> 1.9 s | 21 -> 0 |
| zoom 6, pinch | 15 -> 0 | 257 ms -> - | - | - |
The veil's holes as drawn: 1 (the copy in view) instead of 3; 0 console problems.

Full sweep, test site, before (sweep_all1: overlay 2.27.5 / UI 1.30.5) and after (sweep_all2: 2.27.6 / UI 1.30.6): headless
Chrome with the GPU, 1280x800, storage cleared per field; 8 fields x 12 places (San Juan, Honolulu, North Sea, Tokyo, New
York, Lake Michigan, the Keys, Bali, Norway, the world view, Southern California, Biscay); at each place 6 pinch rounds,
two long pinches, 4 wheel zooms and 3 drags (scratch b93c/v/hl/sweep.js; comparison cmp_sweep.py). Per field, the 12 places
summed:
| Field | Gaps > 250 ms | Long tasks | Longest | Long-task time | Places with a task > 150 ms |
|---|---|---|---|---|---|
| none (no overlay) | 0 -> 0 | 99 -> 93 | 120 -> 123 ms | 6.5 s -> 6.1 s | 0 -> 0 |
| wave height | 6 -> 3 | 305 -> 193 | 329 -> 242 ms | 28.9 s -> 13.0 s | 10 -> 1 |
| peak period | 5 -> 0 | 417 -> 34 | 326 -> 101 ms | 37.9 s -> 2.1 s | 12 -> 0 |
| wind | 4 -> 0 | 282 -> 1 | 510 -> 71 ms | 26.5 s -> 0.07 s | 7 -> 0 |
| sea surface temperature | 10 -> 0 | 232 -> 0 | 354 -> 0 ms | 24.9 s -> 0 | 10 -> 0 |
| satellite waves | 13 -> 0 | 301 -> 1 | 261 -> 55 ms | 24.6 s -> 0.06 s | 1 -> 0 |
| radar | 7 -> 0 | 367 -> 3 | 300 -> 53 ms | 33.4 s -> 0.16 s | 8 -> 0 |
| bathymetry | 0 -> 0 | 209 -> 1 | 205 -> 65 ms | 15.0 s -> 0.07 s | 1 -> 0 |
0 console problems in either run.

Left (after the fixes): wave height at the WORLD view (zoom 2.6-3.2) still has 134 long tasks, five over 150 ms (longest
242 ms) and two frame gaps of 267 ms; the other 11 places under wave height are 0-12 long tasks under 90 ms. Peak period at
the same place: 8 long tasks, longest 89 ms; no overlay: 64, longest 116 ms (the page's own cost at that zoom). Tokyo under
wave height had one 367-ms frame gap with no long task (the browser or the GPU, not page code). Not yet traced.

Not checked: a real phone / iPad (the owner's devices).
