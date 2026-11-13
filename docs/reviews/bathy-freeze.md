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
