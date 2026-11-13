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

The remaining frame gaps are the headless browser during long pinches: the same pinches with no overlay at all stall up
to 700 ms, and the 1,267-ms gap in testD held only 82 ms of bathymetry drawing. No long task in any 2.27.5 run came from
the bathymetry layer.

Regression on the test site (2.27.5): 48 views desktop + phone, readout = drawn pixel, 0 mismatches; land points, hole
fill, Big Sur shore, hover, contour levels, slider, notice, Off (0 requests) as before; 0 console problems.

Not checked: a real phone / iPad pinch (the owner's device).
