# G32: adversarial review of plan section 46's client and page (isobars, particles slider, window box, IP open, default star, no Home)

Date: 2026-10-18. Scope: `feat/isobars-geo` @ 72b75a3 on top of production 1a0d06e (which already carries steps 1-2: the
frames job's `pres` field and the geo Worker, reviewed in G32a): `git diff 1a0d06e..72b75a3`, 24 files (+2,188 / -255):
`static_overlay/overlay.js` (the isobar tracer and layer, the third frame kind `pres`, the particles' strength),
`templates/index.html` (the gear rows, the forecast window's wide box, the Home button removed, the head's geo race, the
first view, the star cycle), `static_ui/forecast.js` (the deferred first load), `app.py` (`GEO_BASE`, asset versions), the
README, tests. Test site wave-app-clean @ aa9e8ad (overlay 2.19.2, UI 1.23.0) with `GEO_BASE=https://geo.allshoresurf.com`
(set 2026-10-18 02:26 UTC); the geo Worker live at geo.allshoresurf.com, the WAF rule "geo flood" since ~02:50 UTC.

Two fresh-context reviewers, effort MAX, brief in the session's scratch (`g32/brief.md`: the owner's eight decisions, the
author's claims C1-C10, known items K-1..K-13):
- **A** (Fable 5.1): code and Node, no browser, its own detached worktree. A three-kind scheduler fuzz (field / direction /
  pressure decodes held and released in random order, failures, corrupt bodies, manifests with and without `pres`, model
  switches, Update, resolution changes, hidden tabs, reduced motion, SST) on the main-thread, worker and canvas decode
  paths; an independent marching squares; the IsobarLayer lifecycle; the gear rows, the head, the deferred load, the first
  view and the star through the repo's harnesses; 87 mutants.
- **B** (Opus 5.5): the test site in its own headless Edge (GPU for timing, trusted DevTools input) at 1280-2560 px, a tablet
  and phones; isobars at every zoom against an independent rebuild, playback and timed cells (after A finished), the gear,
  the window box, the IP open with the real Worker (12 requests) and 37 intercepted cases, the star, failure paths,
  accessibility, first-load timing against production.

**Result: 0 P0, 1 P1, 3 P2, 14 P3** (distinct; A 0 / 1 / 1 / 5, B 0 / 1 / 2 / 10; A-P1-1 = B-5 and A-P3-3 = B-7).

## What held up
- The scheduler with three frame kinds (A, ~900 fuzz runs, ~170,000 operations, all three decode paths): in flight never
  above `_maxInflight()` (5 of 5 at shark), every cache within the speed ring's decoded budget, ~22,000 pressure downloads
  and ~1,300 pressure failures (404 / 500 / 503 / network / corrupt) with no outage and an exact outage counter, no stuck
  step, nothing after Off, the section-42 paint-gate checks unchanged; the isobar layer never showed another step's lines at
  a paint except the one-frame window of A-P3-5.
- The tracer (A): an independent marching squares equals `traceIsolines` on 123 random fields and on the real PRMSL fixture
  at 7 views (segments, chaining, loops closed, open ends on the window edge or a missing node, the date-line column, world
  copies, `isoSmooth`, `isoStride`, labels incl. the 4-px gap except B-2's case, draw order, `valueAt`). On the site (B): the
  drawn set = a rebuild in 152 views at 1280x800 and 68 at 1920x1080 (8 regions incl. the Guam typhoon and the date line,
  the minimum zoom to 11.3, both poles, zoom 18), the 8 / 4 / 2 hPa spacing by zoom, no label within 8 px of an edge, the
  canvas exactly over the map.
- The wind look: with the isobars off the test site's wind picture = production's pixel for pixel (B, reduced motion, the
  same run and step); with them on only line and pill pixels differ.
- Playback at the default view (B, after A, PREGC=1, frames warm for both Origins, 150-s idles): 1080p wind shark 8.0 fps,
  gaps 125/137 ms, 0 unpainted, one pressure download per landing, in flight 5/5; the memory rule at that cell holds
  (live +23.3 / +23.6 vs production +19.8 / +19.9; one peak 1.6 MB over production's range + 10, GC timing, K-11).
- The gear rows (A: 37 garbage stored values, throwing storage; B: mouse, keys, touch, the short-screen panel, a reload, a new
  tab): the checkbox and the slider behave and persist as designed; particles 0 = no direction downloads.
- The window box (B): exact at 11 sizes from 1280x501 to 2560x1440 (left = the overlay panel + 16, right = the legend + 16,
  top 96, bottom 16); 1279 px and narrower keep the bottom-right box; maximise, drag, resize, chips, a new tab = the chip;
  nothing else moved (18 controls compared with production at six sizes) apart from Home and the (i).
- The IP open (B): the real Worker from three form factors (Mililani -> 51211 Pearl Harbor, the map hidden until the
  first view, the address written after the load); 24 intercepted cities incl. the date line, the far south and inland
  places (the station in view in every case); malformed answers, HTML, 429 / 503, no CORS, slow (cap) and failing Workers:
  51201 + Hawaii; reload / link / default: no Worker request. A: the head's query = the module's for 3,000 fuzzed
  combinations; `nearest()` = brute force incl. the date line and the poles; the deferred load's address hold under every
  path; the first view on 500 random cases.
- The star (A and B): the cycle on stations and points, demotion, the default listed first, two tabs, the 100-favourite
  guard, damaged stored values.

## Findings
| id | sev | finding | author's check |
|---|---|---|---|
| A-P1-1 = B-5 | P1 | Wind -> sea surface temperature keeps the last wind step's isobars (lines + pills) on the SST map for the whole SST session, redrawn on every pan and zoom (B: 10 lines / 25 pills over the "NASA MUR" legend), and on an empty map in the SST error state: `_mountObs` calls `_ensureFlow()` but never `_ensureIso()`; `_fail` clears the lines only while `layer.hasFrame()`. | CONFIRMED (code: `_mountObs` ~3074, `_fail` ~3352; B's live screenshots; A's fuzz seeds). |
| A-P2-1 | P2 | `update()` refreshes the particles (`_ensureFlow`) but not the isobar layer: an Update from a run without `pres` to one with it downloads every pressure frame, waits for them, and draws nothing until a field switch / Off+On / reload. Exactly ECMWF's first pressure run (K-4) and any GFS tab opened before run 2026101718. | CONFIRMED (code: `update()` ~3465; `_syncIsobars` and `_ensurePres` need `this.iso`). Not seen live (no newer run during the review). |
| B-13 | P2 | At the widest views the isobars break the memory and frame-rate rules at 1080p shark: the 1920-px whole-world view (zoom 2.907, ~1,350 lines) 7.6 fps (p90 151 ms), main thread ~97 %, peaks +156 / +182 MB vs production +46 / +54, live ~12 MB over; with the gear's isobars off = production. | CONFIRMED (author's Node benchmark on the real fixture, `scratch g32/author/bench_world.js`: 1920x1080 world 1,470 paths, 19.8 ms p50, 18.6 MB allocated per redraw; 1280x800 world (8 hPa) 666 paths, 13.6 ms, 9.9 MB; the default zoom 6 view 13 paths, 1.8 ms, 1.1 MB). |
| B-1 | P2 | Every map tool now minimises the expanded forecast window on desktops >= 1280 px: the wide box always reaches under the tools bar (`measureTopRight` leaves `.tools-bar` out of `--map-topright-w`), and the tools' `onLayout` minimises an overlapping window when a tool starts or the bar grows; production's bottom-right box sits below the bar and stays open. | CONFIRMED (code: `measureTopRight` ~1456, `onLayout` ~4408). |
| A-P3-1 | P3 | `isobars.test.js`'s z3 timing assertion (`best < 40 ms`) flaps under the parallel runner (27.6 / 37.5 / 44.3 ms). | agreed |
| A-P3-2 | P3 | Four readers of `allshore.defaultStation.v1` with three id gates; a hand-edited id costs one wasted request. | agreed |
| A-P3-3 = B-7 | P3 | A default station no longer in the station list opens an error on every fresh visit; the star is disabled and the list has no row to clear it. | agreed (B: "No .bull file found for 99999") |
| A-P3-4 | P3 | The deferred first load chains `.then` on a bare thenable's result (hardening). | agreed |
| A-P3-5 | P3 | A landing whose microtasks run between the animation-frame callbacks and the paint shows one frame (<= 16 ms) of the previous step's isobars under the new tiles (2 of 120 playback fuzz runs; the particles share the window). | agreed; inherent to drawing in a rAF |
| B-2 | P3 | Label pills can touch: `placeLabels` files a kept pill under the cell of its left edge only, and a 33-px pill one cell further left is never compared ("1016" / "1020", Atlantic z 4.3; 1 of 4,927 pills). | CONFIRMED (code: `keep` / `hits` ~2557-2572) |
| B-3 | P3 | Below zoom ~3.5 (every other node traced) the readout's hPa can disagree with the line under the cursor by up to 12 hPa near a deep low (the Guam typhoon at z 3.3); from z 3.5 within +-1. | agreed (stride 2 + the full-resolution readout) |
| B-4 | P3 | Gridlines (pane 350) are drawn over the isobar pills (280); lines crossing the view at z >= 12 get no pill. | agreed |
| B-6 | P3 | A reload during the geo race keeps the map at Hawaii for the session while the forecast follows the IP: the provisional Hawaii view is saved on its moveend before the Worker answers. | CONFIRMED (code: `handleMapMoveEnd` ~1554 saves every view) |
| B-8 | P3 | A click on a map control during the race (the gear, the layers box) counts as "the visitor moved the map" (the listeners sit on the container); acting does not reveal the hidden map early. | CONFIRMED (code: the first-view block's `act`) |
| B-9 | P3 | The opening view can put the nearest station under the overlay panel (Invercargill -> Bass Strait at 1280x800). | agreed (the fit ignores the controls) |
| B-10 | P3 | A stalled pressure download holds each wind step ~14 s (the download watchdog: 3 x 5 s) though the wind frame is on hand (0.2 fps at shark with every pressure request stalled). | agreed (the same rule as the direction frames) |
| B-11 | P3 | At 1920 px the whole-world view (zoom 2.907) misses the 8-hPa spacing (switch at 2.9): 4 hPa, ~1,300-1,470 lines; 1906 px (the owner's screenshot) gets 8. | CONFIRMED (log2(1920 / 256) = 2.907) |
| B-12 | P3 | The forecast chip names Waimea Bay (the server's 51201) through the race until the IP station's forecast arrives. | agreed |

Mutation (A): 87 mutants: 67 killed by the repo suites; 8 only by A's checkers (test gaps: S7 a pressure abort never ending
the wait; T9 the projector's sub-node rows; T11 the 4-px gap between pills; T12 the 150-px rule on the length in view; L1
setData rescheduling the same frame; L6 the DPR cap; L7 `valueAt`'s row range; H4 the haversine cosines); 12 survived
everything: 6 equivalent, 3 near-equivalent, 3 harmless gaps (S2 a pressure request for a step whose field is unavailable,
S17 the readout's hPa binding, G6 the slider width).

Not covered: ECMWF's first pressure run (00Z, ~07:40 UTC) and an Update to a run with pressure live (no newer run during the
review; B's scripts `t18_update2.js` / `t26_update_nopres.js` are ready); a real phone or iPad; screen readers; the IP open
from another network.

## Owner's calls (2026-10-18, all four as recommended)
1. B-11 / B-13: the whole-world view gets 8-hPa spacing on any screen width (while the view spans the whole world);
   zoomed-in views keep today's rule (8 hPa to zoom 2.9, 4 to 4.9, 2 after).
2. B-1: while a map tool is open, the forecast window's wide box narrows so its right edge clears the tools bar (it widens
   again when the tool closes); the window is no longer minimised for it.
3. B-10: a wind step waits at most 1 s for its pressure frame; after that it shows without lines, and the lines appear when
   the frame arrives (never another step's lines).
4. B-3: the readout stays the exact model pressure at the cursor (wide views are a coarse picture): accepted, no change.

## Fix round scope
Client (`static_overlay/overlay.js`, overlay asset 2.19.3):
1. A-P1-1 / B-5: `_mountObs` calls `_ensureIso()` beside `_ensureFlow()` and empties the pressure cache; `_fail` clears the
   lines whenever no frame of the run is on the map. Pins: wind -> SST leaves no isobar layer or canvas and nothing redraws
   on a move; the SST error state likewise.
2. A-P2-1: `update()` calls `_ensureIso()` beside `_ensureFlow()`. Pins: an Update from a run without `pres` to one with it
   draws the shown step's lines; the reverse clears them and asks for nothing.
3. B-13 with owner call 1: the interval is 8 hPa whenever the view spans the whole world, else today's zoom rule; the
   pipeline allocates little per redraw (reused typed buffers for the node values and the smoothing, integer edge keys and
   typed segment storage in `traceIsolines`, one Float64Array per projected path, integer grid keys in `placeLabels`, no
   per-path arrays), with outputs identical to today's (reviewer A's independent tracer as the golden). Targets: the 1920-px
   world view at most ~5 MB and ~8 ms per redraw; the wide 1080p shark cell joins the acceptance set (8 fps, 0 unpainted,
   the memory rule against production).
4. Owner call 3 (B-10): a step waits at most 1 s for its pressure, then lands without lines, which appear when the frame
   arrives. Pins with a held pressure decode.
5. B-2: a kept label is filed in every grid cell it covers. Pin: two 33-px pills straddling a cell boundary.
6. A-P3-1: the z3 timing test becomes a soft ceiling (the time logged, the assertion generous).
7. Test gaps from A's mutants: S7, T9, T11, T12, L1, L6, L7, S17; S2 fixed (no pressure request for a step whose field is
   unavailable) and pinned.
8. README: the wide-view spacing, the 1-s pressure cap, the one-frame window (A-P3-5).
Page (`templates/index.html`, `static_ui/forecast.js` UI asset 1.23.1):
9. Owner call 2 (B-1): while the tools bar is shown, `--map-topright-w` counts it, so the wide box narrows instead of being
   minimised. Page test: the wide box and the open bar never overlap at 1280-2560 px.
10. B-6: the map view is not saved while the first view is pending, so a reload during the race asks again.
11. B-8: the first view's "acted" ignores events inside the map's controls and the overlay sheet; acting reveals the map at
    once.
12. A-P3-2: one id gate for the default station in the head, the module, the first view and the picker.
13. A-P3-3 / B-7: a default that is neither in the station list nor a valid point id is forgotten once the list is in (the
    next visit uses the IP), said once in the window's note.
14. A-P3-4: `Promise.resolve(earlyStation)`.
15. B-12: the chip names the race's station as soon as the race answers.
Accepted, no change: B-3 (owner call 4), B-4 (gridlines over pills; the gear turns gridlines off), B-9 (a far station can
sit under the overlay panel), A-P3-5 (one frame, documented).
Release: overlay 2.19.3, UI 1.23.1, fixtures, the golden in its own commit; the test site at MAX (incl. ECMWF's first
pressure run and an Update to a run with pressure if published, B's t18 / t26 scripts, the wide cell back to back with
production); a short fresh re-check at MAX (the fixes touch `update`, `_mountObs`, `_goto`'s wait, the tracer, the head and
the first view).

## Fix round (step 11, Opus 5.5 HIGH, 2026-10-18; overlay 2.19.3, UI 1.23.1)

### The owner's presentation request (with the step-11 go)
"The pressure callouts are way too pronounced ... never more than one pressure display on an isobar ... the callouts are
also too large ... at full global extent the map is way too busy ... Windy also presents smoother isobars, that is a better
presentation, even if less accurate ... high and low centers designated with H and L ... the isobar lines slightly fainter
... generally I want a similar appearance [to windy.com]". Windy studied at 1400x850 (zoom 2, 3, 5, 7: scratch
iso/v9/shots/w_z*.png): thin semi-transparent white lines, smooth; no line labels at the world view; one small pill per line
from about zoom 4, rotated along it; H / L as plain letters with the pressure under them; 4-hPa spacing to about zoom 6,
2 hPa from zoom 7. Built (static_overlay/overlay.js, the isobar block):
- Node spacing by screen: one traced node per `stride` grid nodes so traced nodes are at least ISO_NODE_PX 7 px apart (zoom 6:
  every node, zoom 4: every 2nd, zoom 3: every 3rd); each traced node = the mean of its stride x stride block, centred on the
  node (an even stride counts the block's end rows / columns half); a whole-world window takes a stride that divides the 720
  columns (the seam column is the first again).
- Smoothing: ISO_BLUR 2 passes of [1, 2, 1] across and down (missing nodes left out, wrap across the date line in a
  whole-world window). Replaces `isoSmooth` (the half-code bound). Measured on the real PRMSL fixture (|lat| <= 60, 2 hPa):
  the true isobars of the large systems (lines >= 30 nodes) lie within 1 node (55 km) of ours for 95.0 % of their points and
  within 2 nodes for 98.6 %; the coded field's 1,745 lines become 488 (the truth has 1,715: most of the difference are small
  rings from mountain pressure reductions and 8-bit terraces); mean turn per vertex 0.112 rad (raw codes 0.386): smooth.
  One pass measured 97.3 % / 99.4 % with 608 lines (0.157 rad); two chosen for the look the owner asked for.
- Interval: ISO_INTERVAL [[2.9, 8], [5.9, 4], [99, 2]] (Windy's 4 hPa to zoom ~6; the step-9 rule had 2 hPa from zoom 5)
  and 8 hPa whenever the view spans >= ISO_WORLD_DEG 300 deg of longitude (owner call 1, any screen width).
- Labels: at most ONE pill per polyline (each isobar is one polyline in the window), tried at 50 %, 35 %, 65 %, 20 %, 80 % of
  its length inside the view, rotated along the line (text kept upright), 9-px semibold text in a white capsule at 82 %;
  none on a line under ISO_LABEL_MIN_PX 120 px in view; only from ISO_LABEL_MIN_Z 4 and never while the whole world is in
  view; clear of the H / L marks and of each other (4-px gap; each box filed in every 32-px cell it covers: B-2).
- H / L: a node lower / higher than every node within ~ISO_HL_PX 110 px on screen (ties to the earlier node), at least
  max(1 hPa, interval / 4) from the mean of that square's border, not at the window edge (the window is the view plus a
  margin of 118 px so a centre near the view's edge sees its whole neighbourhood) and not at the field's clamped ends: an
  "H" / "L" (15 px, white with a soft shadow) and the pressure rounded to the hPa under it, in every world copy.
- Lines: one stroke of all halos (rgba(0,0,0,0.14), 2.4 px) then one of all cores (rgba(255,255,255,0.72), 1.2 px; were
  0.35 / 2.6 and 0.85 / 1.2); no cut under the pills.
- The tracer's storage typed (crossing points in Float64Array / Int32Array, two neighbour slots per point, one output
  buffer): identical output to the previous tracer on 900 random fields (74,149 lines; scratch g32/fix/tracecmp.js).
- Cost (Node, the real fixture, scratch g32/author/bench2.js): the 1920-px world view 7.4 ms and 3.0 MB per redraw (was
  19.8 ms / 18.6 MB; target ~8 ms / <= 5 MB), the 1280-px world 4.6 ms / 2.0 MB (13.6 / 9.9), the z6 default view 1.4 ms /
  0.5 MB (1.8 / 1.1).

### The fifteen G32 items
1. `_mountObs` calls `_ensureIso()` and empties the pressure cache (wind -> SST: no isobar layer, no frames); `_fail` clears
   the lines whenever no frame of the run is on the map.
2. `update()` calls `_ensureIso()` (an Update to the first run with pressure draws the shown step's lines).
3. Done by the redesign (above): 8 hPa at the world view, typed buffers, ~3 MB / 7 ms.
4. A step waits at most PRES_WAIT_MS 1,000 ms for its pressure (`capWait`), then lands without lines; `_ensurePres` delivers
   the late frame to the step on the map.
5. Labels filed in every cell they cover (`keep`), pinned by a 40-view property test (4,800 lines, no overlap, no pill on a
   mark, edge distance kept, one pill per line).
6. The world-view timing test is a soft ceiling (100 ms; the time is logged).
7. Pins: S7 (`_kindReady` settles at the abort), T9 (rows between nodes), T11/T12 (gap, length in view: the label tests), L1
   (no redraw of the frame already drawn), L6 (DPR capped at 2), L7 (`valueAt` below the grid), S17 (`_hpaAt`: the readout's
   pressure only from the step on the map; extracted from the readout closure so it can be tested), S2 (no pressure or
   direction download for a step whose field frame is unavailable: `_prefetch` now skips them; `_goto` already did).
8. README: the isobars' look, the spacing, the 1-s cap, the one-frame window (A-P3-5); the page fixes.
9. `measureTopRight` counts the tools bar in `--map-topright-w` while it is shown: the wide box narrows beside the bar and
   `onLayout`'s overlap test no longer minimises it.
10. `saveMapView` keeps nothing while the map is `geo-pending`, and the first-view block forgets the synchronous Hawaii view
    when it starts waiting (`forgetMapView`): a reload during the race asks again.
11. `act` ignores events inside `.leaflet-control, .ov-sheet`; acting on the map removes `geo-pending` at once; the four map
    listeners are no longer `once` (an ignored event must not use them up) and are removed when the race ends.
12. One id rule (`/^[A-Za-z0-9_-]{1,32}$/` on the trimmed id) in the head, the module (`DEFAULT_ID`), the first view, the
    picker's `readDefault` (returns the trimmed id) and the My points block.
13. `forgetRetiredDefault(list)` after the station list loads: a default neither in the list nor a point id is removed and the
    window says so once ("... is no longer offered, so it was cleared: the site will open near you next time"; again on the
    first `allshore:forecast` if the window was not up); an empty or failed list decides nothing.
14. `Promise.resolve(earlyStation)`.
15. The module dispatches `allshore:stationshown` after the deferred `syncSelects()`; the picker refreshes its chip on it.
Tests: overlay Node 390 + 2 (isobars 25, playback + 6), UI Node + 7 (window, firstview, early, mypoints), page pins.

## Test-site verification of the fix round (step 11, Opus 5.5 MAX, 2026-10-18)

### Found and fixed during it
- **V11-1 (overlay 2.19.4 @ fa04b91):** the H / L marks were read on the smoothed grid. At the world view a 956-hPa typhoon
  read "L 1000", and a 991-hPa East Pacific hurricane and a 986-hPa South Pacific cyclone had no mark. The marks now come
  from the frame's own codes: the exact centre and central pressure.
- **V11-2 (overlay 2.19.5):** at zoom 4 a 997.7-hPa low, 10 hPa deep, in the Bering Sea (55 N, 174.5 E) had no L.
  - Cause: three minima lie along one trough, 8 deg apart each: B 997.7, A 997.7 one row north of B, C 997.2. The rule
    "lowest within ~110 px, ties to the earlier node" dropped A because of C, and dropped B only because of its 8-bit tie
    with A.
  - Tried first and rejected: candidates from every 3x3 extreme, with suppression. It gave 78 marks at the 1920-px world
    view (the radius rule gave 26) and took 26 ms, so it was too busy and too slow.
  - Built instead:
    - An equal code counts against a node only among its 8 neighbours, where the first in raster order stands.
    - A candidate's patch of equal codes (8-connected) is taken once for its kind, with one mark at the patch's middle.
      There is no mark for a patch that runs down to a lower value (a shelf), holds more than 4,096 nodes or spans half
      the world.
    - Of two candidates of one kind within the radius of each other (they are always equal), the first in raster order
      stands.
  - Measured on the stored fixture and on the test site's frame of the finding (run 2026101806 f011):
    - Marks per view are within +2 of the radius rule (1920-px world view 28 vs 26 and 29 vs 28; 1280-px 19/19, 17/17;
      North Pacific z3 15/13, 13/13).
    - Flat-topped highs now sit at their patches' middles, 1-2 deg from before.
    - B is marked.
    - Time at the world view 3.7-5.3 ms (the radius rule 2.7-4.6 ms), under 0.5 ms from zoom 4.
  - Tests:
    - An independent brute force equals the code on 36 random fields x 2-3 windows x 4 radii, including plateaus, holes,
      seams and the poles. It uses sliding-window square extremes and breadth-first patches.
    - New pins: the B / A / C trough, a shelf, one mark per patch, the 4,096-node and half-world limits at their
      boundaries, two equal lows across the date line, and a radius wider than the world.
    - Mutants: 23 of 25 killed. The two survivors are equivalent:
      - deepest-first versus raster order (two candidates within the radius of each other are equal);
      - the radius clamp (a radius of 360 nodes already covers the whole world).
  - Found while testing:
    - The first build compared every kept centre with every candidate, which took seconds on an artificial whole-world
      radius-1 window. Kept centres are now looked up by bands of radius + 1 rows and columns.
    - Patches are now followed whole and marked per kind, so no patch is followed twice.

### Results on the test site (overlay 2.19.5, test @ b93e2e3, live 17:38 UTC; served files = the commit, 2.19.4 404)
Headless Edge with the GPU (Iris Xe through ANGLE D3D11), trusted input. Scripts are in scratch g32/v11/s.
- **Look (v1_look desk / wide / phone):** GFS 2026101812, shown step 6; ECMWF 2026101800, step 18.
  - At 9 desktop, 5 wide and 3 phone views per model, the drawn layer = an independent rebuild of it. No pill overlaps,
    sits on a mark, comes within 8 px of the edge, is off its line, or has a bad text; no line has two pills.
  - Every strong raw low or high in view (>= 4 hPa from its 110-px square) has its mark: none missed.
  - The readout's "· N hPa" = the exact value at 10 points per model.
  - The old mark-position check (built for marks at nodes) flagged 8 desktop marks. A replay on the same frames
    explains each one: an equal-valued node the check finds first, or a deeper node just past the candidate's radius
    once the mark sits at its patch's middle. Two examples: the Bering Sea pair (B 998 / C 997) in the 12Z run, both
    marked, and a 987-hPa Kara Sea low, 27 hPa deep, at the 1920-px world view that 2.19.4 missed.
- **Marks:** 4 frames x 3 screen sizes x 10 views, 1,039 drawn vs 909 under 2.19.4 (+14 %); the world views changed by
  +0 to +2.
  - In view (4 frames x 2 sizes x 7 views): 50 are the same centre moved to its patch's middle, and 126 are new.
  - Of the new ones, 44 are at least 6 hPa deep, 46 are 3-6 hPa and 36 under 3 hPa; 0 were lost.
- **Scheduler (v2):**
  - A (wind -> SST -> wind): PASS.
  - B: a step whose pressure frame is held lands at 1,042 ms without lines, and the lines come when the frame does:
    PASS.
  - C refined (v2c): for a step whose wind frame answers 404, the direction and pressure frames start 32-47 ms before
    the 404 arrives (asked in the same ring pass). None start after it, through a seek onto the step and shark
    playback across it: PASS x2. The first C script expected no extras at all, which parallel ring requests cannot give.
- **Page (v3):** all PASS.
  - P1: the tools bar narrows the wide box at 1280 / 1440 / 1920 / 2560 px (the window ends 16 px left of the bar)
    and the box comes back when the bar closes.
  - P2: a reload during the race asks again and opens at the visitor's area.
  - P3a: a click on the gear during the race is not "moving the map", so the map goes to the visitor's area. The
    first script delayed the answer 2.2 s, past the head's 2-s cap; re-run at 1.4 s.
  - P3b: a drag during the race stays.
  - P4: the default-id rule, three cases.
  - P5: a retired default is cleared with the note, and the next visit uses the IP.
  - P6: the chip names the race's station at 1,764 ms, before the table at 4,458 ms.
- **Update (GFS 06Z -> 12Z):**
  - t18, while playing at shark with isobars: 201 landings on the new run; every drawn frame carried its own lines; no
    draw of another run or step; largest gap 145 ms; playback continued: PASS.
  - t26, STRIP=1 (the old run made to look pressure-free): lines right after the Update, following the steps, and
    kept through wave height -> wind: PASS.
- **Timed cells** (G29's acceptance script plus pressure counting): GFS 2026101812, frames warm at the edge for both
  Origins, browser cache off, PREGC=1, 155-s idles, production 2.18.4 run back to back with each test cell, GPU.
  Every cell passes: 8 frames/s at shark (2 at snail), 0 skipped, 0 unpainted, in flight never over the cap, one
  pressure download per landing under wind and none under wave height.

  | Cell | Site | Gap p50/p90 (ms) | Render p50 | Main ms/s (script) | Live / peak MB |
  |---|---|---|---|---|---|
  | wind shark 1920x1080, whole-world view (G32 B-13) | test | 125/136 | 81 | 898 (90) | +29.8 / +58.2 |
  | | production | 125/135 | 31 | 808 (29) | +26.2 / +47.1 |
  | wind shark 1920x1080 (z6) | test | 125/134 | 40 | 598 (37) | +22.7 / +38.9 |
  | | production | 125/136 | 40 | 565 (28) | +19.9 / +29.6 |
  | wave height shark 1920x1080 | test | 125/137 | 40 | 657 (27) | +31.8 / +45.7 |
  | | production | 125/135 | 40 | 658 (28) | +31.8 / +48.5 |
  | wind shark phone, whole-world view | test | 126/135 | 17 | 314 (56) | +13.6 / +23.3 |
  | | production | 126/134 | 17 | 253 (25) | +10.8 / +24.1 |
  | wind shark phone (z6) | test | 124/135 | 17 | 274 (29) | +13.7 / +26.0 |
  | | production | 123/134 | 17 | 229 (24) | +10.9 / +21.5 |
  | wind shark 1280x800 | test | 125/134 | 21 | 447 (38) | +18.2 / +32.7 |
  | | production | 125/133 | 21 | 403 (32) | +15.4 / +23.8 |
  | wind snail 1920x1080 | test | 501/507 | 42 | 193 (29) | +20.2 / +27.8 |
  | | production | 500/508 | 42 | 182 (26) | +17.6 / +25.8 |

  - The G32 B-13 cell now passes: on 2.19.2 it ran 7.6 frames/s with p90 150-151 ms, a render p50 of 126-127 ms and
    peaks of +156 / +182 MB.
  - The isobars cost about 8 ms of main-thread script per landing at the whole-world view, and about 1 ms at zoom 6.
- **The whole-world cell again** (production, test, test, production): test peaks +53.0 and +48.9 MB with live +29.8,
  against production peaks +44.9 and +43.3 with live +25.8 / +25.4. All at 8 frames/s, p90 at most 137 ms, every
  frame painted.
  - Over the three pairs, test peaks are +48.9 to +58.2 against production +43.3 to +47.1 this session. One sample is
    1.1 MB above production's range + 10. It is within the rule against G32's production samples (+45.9 / +53.6).
    Owner awareness.
- **ECMWF Update (00Z -> 12Z, published 19:44 UTC):**
  - t26, STRIP=1 (the 00Z run made to look pressure-free): no isobar layer before the Update; lines right after it,
    following the steps; wave height -> wind keeps them: PASS.
  - t26 without STRIP (00Z with pressure): PASS.
- **Fresh-tab console smoke:** GFS wind playing, ECMWF (lines + H / L), SST, wave height, wind again, Off (no canvas,
  no layer). No console error or warning.
- The free test service restarted at about 19:46 UTC (healthz uptime 71 s at 19:47); one page load answered 503 during
  the switch, and the next loads gave 200 in 0.2-0.6 s. Not related to the overlay, which runs in the browser.
- Cleanup: both origins' storage emptied in the browser profile, and the browser closed by DevTools (no process left);
  profile removed.

## Short fresh re-check of the fix round (Opus 5.5 MAX, 2026-10-18)
Target: `cfbd966..7f876e7` (overlay 2.19.5, UI 1.23.1) and the test site at b93e2e3. Two fresh reviewers worked from the brief
in scratch `g32/recheck/brief.md`:
- R1 checked code and data, with no browser. Report: `g32/recheck/r1/recheck-r1-report.md`.
- R2 worked on the test site with trusted mouse, keyboard and touch input in its own headless Edge. Report:
  `g32/recheck/r2/recheck-r2-report.md`; screenshots in `r2/shots`.

**Result:** R1 0 P0, 0 P1, 2 P2, 8 P3; R2 0 P0, 1 P1, 2 P2, 4 P3 + 2 notes. Combined and de-duplicated: **0 P0, 1 P1 (fixed
during the re-check), 3 P2, 10 P3, 2 owner-awareness notes.**

### What held
- **RC1, the look.** Held.
  - R1: the typed tracer equals the previous one on 1,800 cases; the block means and blur are bit-exact against an
    independent version on 3,840 cases; switch points tabulated.
  - R1: pills on 630 real views: 0 overlaps, none on a mark or an edge, one per line.
  - R2: in 114 views (both models, three sizes) drawn = rebuild, with no pill defects. Smooth lines across the date line.
- **RC2, H / L as written.** The code does exactly what its rule says.
  - R1's own implementation (a different method throughout) matches `findCentres` on 4.75 M synthetic centres and 59,976
    real-frame views. The block prefilter and the bands lookup are exact.
  - Of the marks lost against 2.19.4, all 2,141 are shelves.
- **The rule itself fails the owner's "sensible H and L, real centres, nothing silly"** in the ways listed under Findings.
- **RC3, the fifteen G32 items.** Held (one corner of the 1-s cap: P3), except item 13 for point defaults (F3, P1, fixed).
- **Other confirmations.**
  - The readout equals an exact value at 120 of 120 trusted hovers.
  - In shark playback every landing showed its own lines: 0 other-step lines, no blank frames, during drags, wheel zooms
    and pinches.
  - The gear, the wide window box beside every tool's bar at 1280-2560 px, and the IP open with the real Worker all held,
    as did 37 intercepted cases.
  - Failure paths, Updates in both directions, reduced motion: held. No leaks; clean consoles.
  - R1's three-kind scheduler fuzz: 3,000 runs, about 289,000 operations; a late pressure frame always reached the step on
    the map, never another step.
- **Timing.** R2's one slow cell (1920 z4 at shark) was R1's CPU load; it ran 8.0 frames/s on both sites once R1 was done.

### Findings
| Id | Severity | From | Finding |
|---|---|---|---|
| F3 | P1, fixed | R2 (the author found it at the same time) | `forgetRetiredDefault` (item 13) erased a forecast-point default whenever the station list arrived before forecast.js had run: the point test needs `window.AllshoreForecast`. R2 measured it: deleted in 8 of 9 cold loads, and 6 of 6 with forecast.js held 1.5 s. Every point-default visitor would be hit on the first visit after each UI release. |
| H-1 | P2 | R2 F1 | H marks are fixed to mountains, from the sea-level pressure reduction over high ground. In the default Hawaii view an "H" sits on the Big Island at every sampled step of both models (GFS 27 of 27, ECMWF 29 of 29), often as the only mark. At zoom 6's radius there are 61 permanent sites per model (Taiwan, Luzon, New Guinea, Java, Ethiopia, Tibet, Hispaniola, ...). H and L pairs sit on mountain slopes, e.g. 37 px apart in Ethiopia / Sudan at the world view. |
| H-2 | P2 | R1 P2-1, R2 F5 | A low below about 921 hPa (codes 1-2) is never a centre. Live: ECMWF 12Z's super typhoon east of Luzon is code 1 from f045 to f099 and has no L at any zoom; GFS's 928-hPa copy has one. The readout at the eye says "920 hPa" where it should say "≤ 920". The same rule would drop a winter Siberian high of 1060 hPa or more. |
| H-3 | P2 | R1 P2-2, R2 F6 | Deep, broad highs and lows lose their mark when zoomed in, because the depth is measured against a ring at 110 px: 5 nodes at zoom 6, 2 at 7, 1 from 8. Unmarked deep centres: highs 9.5 % at zoom 6-6.5, 15 % at 7-9; lows 40 % at 8-9. A ring at 8 nodes or more fixes 2,808 of 2,857 (marks +0.6 %). |
| H-4 | P3 | R1 P3-1, R2 F4 | About half the marks new in 2.19.5 are one-code "shoulders" or dimples. Examples: two H's inside one 1028 high; "L 1009" three times along the monsoon trough. 12.6 % of all marks stand 0.55 hPa or less above their saddle, and in playback 1-2 marks appear or vanish per step. A higher minimum depth doesn't separate them; a prominence (saddle) rule does, and keeps V11-2's B (1.1 hPa above its saddle). |
| H-5 | P3 | R1 P3-2 | H and L marks overlap: 546 pairs under 32 px, all over mountains. There is no mark-to-mark rule. |
| H-6 | P3 | R1 P3-4 | Two marks of equal pressure can sit 87-110 px apart (320 pairs): the near rule compares nodes, the marks sit at patch middles. |
| H-7 | P3 | R2 F2 | A strong low in view goes unmarked when its equal twin within the radius is kept but drawn off screen. |
| H-8 | P3 | R2 F7 | A deep cyclone's L and value are illegible inside the packed rings and particles. |
| S-1 | P3 | R1 P3-3 | The 1-s pressure cap restarts on every new goto of the same step (a zoom across the half / full threshold, a re-issued tick), giving waits of 1.3-1.8 s. |
| P-1 | P3 | R1 P3-5 | `forgetRetiredDefault` would delete valid defaults if `/stations.json` ever serves `get_station_list`'s 10-buoy fallback list (latent). |
| D-1 | P3 | R1 P3-6, P3-8 | A comment is misplaced. 11 of 68 mutants survive the repo suites (test gaps), and 5 more die only on source-text pins. |
| O-0 | P3, confirm | R1 P3-7 | The 4 -> 2 hPa switch is at zoom 5.9 (Windy's spacing, from the step-11 request); owner call 1's text says 4.9. |

Owner-awareness notes from R2:
- O1: one pill per polyline is not one per isobar; 9 % of pills share an isobar that leaves the view and comes back.
- O2: marks can sit under the logo and the panels.

### The author's checks
Scratch `g32/recheck/author`: my own PNG decoder (node:zlib) plus the module's `findCentres` / `projectCentres`, fed exactly
the layer's inputs.
- **H-1.** The Big Island H at 19.5 N 155.5 W is in every sampled step of both models at zooms 5-6; Kauai gets one in a
  few steps.
  - Its prominence above the saddle to a higher node is 4.4-7.2 hPa. Taiwan's is 1.7-5.5 hPa and Luzon's 3.3-5.0 hPa,
    which is as much as many real highs. A prominence threshold alone cannot remove these marks; it needs high ground.
- **H-2.** The ECMWF f072 / f096 eye is code 1, about 85 hPa below its ring 8 nodes out, with no L at any zoom. GFS f072
  draws "L 928".
- **H-3.** Confirmed, with R1's narrative zooms corrected. R1's examples used a 60-px "near the centre" test, while the
  mark sits at the patch's middle.
  - The Tasman high keeps its H to zoom 5.5 (83 px off-centre) and loses it from zoom 6.
  - The 969-hPa Greenland Sea low and the 976-hPa low at 56 S keep their L at zoom 6 and lose it from zoom 7.
  - R1's percentages test by patch identity, not by distance, so they stand.
- **F3.** I found this independently in the local browser check of the owner's requests (below). It was fixed in 11c37e0:
  a `pt_` default is judged once the page has parsed, and kept if the module never comes. On the test site a default point
  was kept and opened in 5 of 5 loads with the cache off and 5 of 5 with forecast.js held 1.5 s.
- **S-1, P-1, D-1.** Confirmed by reading the code:
  - `capWait` makes a fresh timer per `_goto`;
  - `get_station_list` falls back to `DEFAULT_STATIONS`;
  - the comment sits above `capWait`.

### The owner's requests during the re-check (2026-10-18), on the test site
- 7d127c7: a visitor on Oahu opens on Waimea Bay 51201. Each saved row in the dropdown gets a Favorite | Default choice and
  a ✕.
- 11c37e0: F3 fixed. Saved rows wrap on phones, so a point's coordinates are never cut.
- Goldens: 03ea062, ce210da.
- feat/isobars-geo was fast-forwarded to ce210da; test is at 939ac4e (live 23:08:46 UTC).

Verified on the test site (headless Edge, trusted input, scratch `iso/v9/v12_oahu.js`):
- The real geo Worker (Mililani) opens on 51201 on desktop and phone, the map on the visitor with Waimea Bay in view.
- Intercepted locations: Honolulu, Haleiwa and Kailua open on 51201; Lihue 51208, Hilo 51206, San Francisco 46237.
- The dropdown worked by clicks, Tab / Shift+Tab / Enter and phone taps.
- On the phone the labels are whole, the halves are 44 px and there is no sideways scroll.
- No console problems. Storage emptied afterwards.
- Suites: pytest 1,189; Node UI 427 and overlay 397.

### The owner's calls (2026-10-18, all four as recommended)
1. H-1, mountains: no H or L over high ground. A static elevation map is built once from NOAA's model terrain; no mark
   where the terrain is above about 500 m (threshold tuned on real runs). Lowland and ocean systems are unchanged.
2. H-3 / H-4, the rule: rank by prominence. A mark needs at least about 1 hPa above the saddle to any stronger neighbour;
   one mark per 110-px area, and the most prominent wins.
3. H-2, typhoons below 920 hPa: the clamped eye gets "L ≤920" and the hover reads "≤ 920 hPa". The same holds at the
   top: "H ≥1060".
4. O-0, the spacing: keep zoom 6 (Windy's): 8 hPa at the world view, 4 hPa to zoom 5.9, 2 hPa beyond.

### Fix round 3 scope (overlay asset 2.20.0)
Client (`static_overlay/overlay.js`):
1. **H and L by prominence** (call 2; fixes H-3, H-4, H-6, H-7). One pass per kind over the view's window plus a margin:
   nodes in code order, union-find over 8-neighbours, merge-tree persistence. A centre's prominence is the level at which
   it joins a stronger centre minus its own code; the strongest in the window is unbounded.
   - A mark needs prominence >= 2 codes (1.1 hPa: keeps V11-2's B and drops one-code shoulders).
   - The patch rules stay (at most 4,096 nodes, under half the world's width); the mark sits at the patch middle.
   - Exclusivity is greedy by prominence, then depth, then raster order: a centre is dropped when a kept centre of its kind
     lies within `rad` nodes of its patch middle.
   - Exclusivity runs only among centres whose marks are drawn in the view, so a twin kept off screen no longer hides a
     visible one.
   - The 110-px ring-depth test goes, so a broad high keeps its H at every zoom.
2. **High ground** (call 1; H-1, H-5). A static 0.5-degree mask, `static_overlay/terrain_half.png`:
   - A node is high when the highest NOAA GFS model terrain (HGT:surface, 0.25 degrees, public domain) over its footprint
     is at or above the threshold.
   - Built once by `tools/model_frames/make_terrain_mask.py` and versioned and pinned with the overlay assets.
   - No mark on a high node. Without the mask (it failed to load) the marks are drawn as before.
   - Then a cross-kind pass: an H and an L whose boxes overlap keep the more prominent.
   - Threshold: chosen on the 22 live frames to remove the persistent sites (R2's 61 per model) while keeping lowland
     systems; reported with the counts.
3. **Clamped ends** (call 3; H-2).
   - A patch at code 1 or 255 can be a centre when it is compact (at most 16 nodes) and within 70 degrees of the equator;
     its text is "≤920" or "≥1060". Codes 2 and 254 are ordinary values.
   - The readout writes "≤ 920 hPa" / "≥ 1060 hPa" within half a code of the clamp.
4. **Legibility** (H-8): a mark clears the line strokes under its box, as a pill does.
5. **The pressure cap** (S-1): the 1-s wait is kept per step. A re-issued goto of the same step waits only the rest of it,
   and the record is cleared when the step lands or the target changes.
6. **Tests:** the new rule pinned against an independent implementation (persistence by brute force); the Big Island,
   typhoon, Tasman high and V11-2 cases on real frames (fixtures); R1's eleven surviving mutants, adapted to the new rule
   where they apply; the cap's timer after Off.

Page (`templates/index.html`):
7. P-1: `forgetRetiredDefault` decides only on a list of 100 entries or more (734 today; the server's fallback has 10).
8. D-1: the comment moved; the source-text-only page pins (P2 / P5 / P7 / P11 / P12) become behaviour tests where the
   harness allows (P2: a real `closest()` over two classes).

Docs: README (the H / L rule, the high-ground mask, ≤920 / ≥1060, the cap); call 4 recorded (zoom 5.9 stays).

Accepted, no change: O1 (one pill per polyline: an isobar leaving and re-entering the view gets two), O2 (a mark under a
panel is simply hidden by it).

Release:
- overlay 2.20.0 with the mask asset and the fixtures; the golden in its own commit (the template).
- The test site at MAX: the H / L survey on both models (Big Island, typhoon, broad highs at zoom 6-9, dimples, V11-2's
  B), the whole-world timed cell against production, playback.
- A short fresh check at MAX (a new rule).
- Then step 12.
