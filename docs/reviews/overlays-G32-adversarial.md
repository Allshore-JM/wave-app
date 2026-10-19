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

## Fix round 3 (Fable 5.1 HIGH, 2026-10-18; overlay 2.20.0)
feat/isobars-geo @ 39e6992 (the code), e083c10 (the golden, its own commit), 32e6418, 636e446 (the page pins by
behaviour), 414bebe (the mutants' test gaps pinned). The owner's four calls of the re-check built as scoped; the deviations are named.

### The eight items
1. **H / L by prominence** (`findCentres`, `prominenceMap`, `projectCentres`). Candidates by the square test (no node undercuts
   / overtops within `rad` nodes; an equal code counts only among the 8 neighbours, the earlier in raster order stands) and the
   patch rules (8-connected equal codes; a shelf, a patch over 4,096 nodes or one spanning half the world marks nothing).
   Prominence = the merge tree of the window widened by `margin` = max(2 rad, 16) nodes: a counting sort by code, union-find
   over 8-neighbour blocks, the weaker centre absorbed at level t with prominence t - its code; the strongest of the window
   is unbounded (`ISO_HL_PROM_CAP`). Ties by the GLOBAL raster order (`earlier()`), never the window's, so a view never changes
   a centre's rank; equal-code sets share one representative (an absorbed member had come back with 0: found by a test).
   A mark needs ISO_HL_PROM_CODES 2 codes (1.1 hPa). DEVIATION (cost): the pass runs on blocks of the lines' stride, coarsened
   until the window holds at most ISO_PROM_NODES_MAX 16,000 blocks (`blockExtremes` = one scan for both kinds); the 1920-px
   world view 10.3 -> 6.5 ms for the whole H / L pass (2.19.5: 3.5 ms).
   Exclusivity on the DRAWN marks in `projectCentres`: sorted by prominence, then depth, then index; a mark of the same kind
   within ISO_HL_PX 110 px of a kept one is dropped; an H and an L whose boxes (+ ISO_LABEL_GAP_PX) overlap keep the more
   prominent. A twin beyond the view hides nothing (H-6, H-7). The 110-px ring-depth test is gone (H-3).
2. **High ground** (`static_overlay/terrain_half.png`, 720 x 361, 8-bit, 20 m a code = the highest 0.25-degree node of the
   NOAA GFS model terrain (HGT:surface, f000, run 2026101818) over each half node's 3 x 3 footprint; built by
   `tools/model_frames/make_terrain_mask.py`, served through `_OVERLAY_ASSETS`, pinned raw with the other assets). A
   candidate at or above ISO_TERRAIN_M / 20 at its own node or at its patch's middle marks nothing. Loaded once per page by
   `loadTerrain` (outside the frame scheduler; a failure resets the cache and the marks draw as before), `IsobarLayer.
   setTerrain`. THRESHOLD: 400 m, not the owner's "about 500": on the 22 live frames of the re-check 500 m still left
   Guadalcanal, Socotra and New Britain; 400 m leaves no persistent mountain site on either model (GFS 47 -> 0, ECMWF 71 ->
   0 of R2's sites). The Big Island "H" (11 of 11 GFS and 10 of 10 ECMWF steps at the default view) is gone (H-1).
3. **Clamped ends** (H-2): a compact patch (at most ISO_CLAMP_PATCH_MAX 16 nodes) of code 1 / 255 within ISO_CLAMP_LAT 70
   degrees of the equator is a centre with the text "≤920" / "≥1060" (codes 2 and 254 ordinary); `_hpaText` writes
   "≤ 920 hPa" / "≥ 1060 hPa" within half a code of the range's ends.
4. **Legibility** (H-8): `drawIsobars` clears a rounded box (ISO_HL_CLEAR_PX 2 around the mark box) with `destination-out`
   before the letters, the way a pill does (`roundRectPath` shared).
5. **The pressure cap per step** (S-1): `_presWait = {key, since, timer}`; a goto of the same step issued again (a zoom across
   the half / full threshold) waits only the rest of the second; the record goes at the landing, on a target change and in
   `abortAll()` (R1 S3: no timer after Off).
6. **Tests**: `tests/overlay/isobars.test.js` rewritten around an INDEPENDENT oracle (`bruteCentres`: the square test, patches,
   clamps, terrain and a Dijkstra-minimax prominence on its own block reduction with a binary heap; `refValues` for the
   block means and the blur): random fields, windows across the seam and the poles, strides 2-7, margins; the reviewers'
   cases on the real PRMSL frame; the layer's own inputs (R1 H25 / H26); R1's test gaps H22, H24, L3, L5, L6, L12, S3; the
   terrain layer and the cleared boxes; 36 tests. `playback.test.js` + 4 (the cap per step on a fake clock, the timer after
   Off, the clamp readout, the terrain wiring incl. a 503; the harness keeps the terrain request out of `w.fetches`).
   Flag tests: the sixth asset served and pinned, the mask's geometry, the needles (`ISO_HL_MIN_HPA` gone).
7. **P-1**: `forgetRetiredDefault` decides only on a list of 100 entries or more (`tests/ui/firstview.test.js`: a 123-entry
   list judges, 99 and the 10-buoy fallback keep; `test_index_page.py` pins the floor).
8. **D-1**: the comment moved; R1's five source-text-only page pins (P2 P5 P7 P11 P12) are behaviour tests: the firstview
   harness's `closest()` matches real classes (a marker on the map counts as acting; `.leaflet-control` and `.ov-sheet`
   do not); `saveMapView`, `measureTopRight` and the stations-load call site run from the template (nothing kept while
   geo-pending; the tools bar counted while shown and left out when hidden; the list kept, drawn, judged, then the pending
   focus spent); the My points block trims a padded default and refuses one with a space. The five mutants die on the Node
   suites alone (scratch g32/fix3/mut_d1.py).
README: the H / L paragraph (prominence, the 400-m mask, ≤920, the cleared boxes, the per-step cap). Accepted as scoped:
O1 (one pill per polyline), O2 (a mark under a panel).

### Measured on R1's 22 live frames (scratch g32/fix3/survey.js, both models, the re-check's views)
- Persistent mountain sites (R2's 61 per model at zoom 6): 0 on both models at 400 m.
- The typhoon (ECMWF code 1): "L ≤920" at every zoom; GFS's 928-hPa copy "L 928".
- Broad systems: the Tasman high keeps its H at zoom 5-7 and beyond, the Greenland Sea low its L to zoom 9, the 56 S low at
  6-9, the NW Pacific high at 6 (H-3 cases of the re-check).
- Marks per frame: the 1920-px world view 27.4 -> 19.8, the tropics at zoom 4 14.4 -> 5.2, Hawaii zoom 6 1.7 -> 0.1 (the
  Big Island H gone; a real centre draws when there is one).
- Same-kind pairs under 110 px and H / L pairs under 32 px: 0 (were H-4 / H-6).

### Suites and mutants
pytest 1,190 passed (full run); Node overlay 408, UI 430. Mutants on a scratch copy (scratch
g32/fix3/mut_fix3.py, --test-force-exit + a subprocess timeout): 37 mutants of the new rule (prominence ties / alias / at(), the flip, 4- vs 8-connectivity, the cap, the margin, the threshold, the coarsening; the clamp patch limit / latitude / texts; the terrain at the middle / threshold / size; the pixel pass order, the same-kind and cross-kind rules, the clamp text, the depth; the cleared boxes; the layer's terrain / redraw / radius; the per-step cap, its clearing at the landing and at Off, the readout clamp and call site, the terrain fetched once and dropped on failure; the page's floor) + the five D-1 page mutants: 33 killed by behaviour tests after five test gaps were pinned (P5 the world stride: blocks of 7 across the seam lost 58 of 165 real-frame centres; C6 the inclusive threshold; C7 a high terrain of another size; L3 the rounded radius at zoom 4.5; O1 the S-1 test re-issued the goto without a res change, so the first goto's own timer landed the step under the mutant too: it now re-issues as `_checkRes` does), 1 by a needle (O5 the readout's `_hpaText` call site: the readout closure is not driven by the Node harness), 2 equivalent (C1 a code-255 "low" is the window's maximum, prominence 0 or a patch too large to be a centre; O3 on Off the aborted pressure load resolves the race and `capWait`'s `done` clears the timer, the S3 test shows 0 timers either way); the five page mutants 5 of 5 by behaviour. Commits 414bebe (the pins).

### Owner awareness
- A real centre over high ground gets no mark either (a Mongolian winter high, a Kansas low above 400 m): the mask
  cannot tell a reduction bump from a real system.
- The H / L pass costs ~3 ms more than 2.19.5 at the 1920-px world view (6.5 vs 3.5 ms); the timed cell measures it.
- A deep but narrow low that rises less than 1.1 hPa before meeting a stronger centre has no L (the prominence rule).

### Release
Overlay 2.20.0 (js, css, the three animals, `terrain_half.png`); the golden re-captured once (the template's floor).
Next: the test site at MAX (the H / L survey on both models live incl. the Big Island, the typhoon, broad highs at zoom
6-9; the whole-world timed cell vs production; playback; the per-step cap; the terrain asset served), a short fresh check
at MAX (a new rule), then step 12.

## Test-site verification of fix round 3 (Opus 5.5 MAX, 2026-10-19)
`test` @ 9a24257 (cherry-picks of 39e6992, e083c10, 32e6418, 636e446, 414bebe; the site files equal feat/isobars-geo's apart
from the test branch's three logo lines). Pushed 02:51 UTC; the new instance answered from 02:52:28 with 0 failed requests
across the switch (polls every 2 s). The six overlay assets (overlay.js, overlay.css, the three animals, terrain_half.png)
are byte-identical to the commit, immutable, nosniff, with their types; the 2.19.5 URLs answer 404; the UI assets stay 1.23.1.
Live runs: GFS 2026101818 (209 frames) and ECMWF 2026101812 (85), both with `pres` 920-1060 hPa. GFS 18Z is new to the rule
(it was tuned on R1's 22 frames of the 12Z runs). Scripts and outputs: scratch g32/v3 (s/, out/, shots/).

### The rule on the live runs (Node, every pressure frame of both runs: 294)
- **The oracle:** the module that the test site serves equals the test file's independent brute force at the layer's own
  inputs (radius, the coarsened stride, margin, the 400-m mask) in 256 of 256 cases (8 steps x 16 views x 2 models; 1,601
  centres).
- **Invariants of the drawn marks**, every frame at 16 views (23,254 marks): 0 with a prominence under 2 codes, 0 on high
  ground, 0 pairs of one kind within 110 px, 0 H / L boxes overlapping, 0 within the edge margin, 0 wrong texts; the 116
  clamped marks all sit on code 1.
- **Mountain sites** (R2's method: the whole grid at zoom 6's radius, a site = a mark within 1 node in at least 70 % of a
  run's steps): GFS 32 -> 0, ECMWF 50 -> 0.
- **The default Hawaii view** (1280x800, zoom 6): the Big Island H in 209 of 209 GFS steps and 85 of 85 ECMWF steps without
  the mask -> 0 with it; Kauai 4 -> 0 and 23 -> 0; marks in view 1.36 -> 0.34 and 1.42 -> 0.15 per step.
- **The typhoon:** GFS's (928.8 hPa at its deepest) is marked at its centre at zoom 4 and 6 at every step until it reaches
  Taiwan. ECMWF's is at code 1 in steps 42-105 and reads "L ≤920" at its centre at zoom 4 and 6 in each of them.
- **Marks per frame** (the averages): world 1920 px 20.0, world 1280 px 14.2, North Pacific z4 5.4, Atlantic z4 4.8,
  W Pacific z4 5.2, Hawaii z6 0.3, Oahu z9 0.

### On the test site (headless Edge with the GPU, trusted input)
- **The layer = its rebuild** (s/check.js): 36 views at 1280x800 (GFS f010 and f116, ECMWF f072, 12 views each) and 12 on a
  375x812 phone. Lines, pills and marks are counted equal everywhere. With a 1-px anti-aliasing tolerance the canvas has
  0 pixels of ink the rebuild lacks and 0 the other way, in every view.
  - An exact pixel match is not possible across rasterizers. Every exact difference equals a control's: the same rebuild
    drawn on a canvas read on the CPU. A canvas Chrome has read back is drawn on the CPU from then on.
- **The page's centres = the Node brute force** on the same frame bytes in 36 of 36 desktop and 12 of 12 phone views
  (226 centres). The page's decoded terrain mask has the Node decode's checksum (707475197).
- **The cleared boxes (H-8):** every mark's box holds only its own text, in every view. The lines ran under 1-10 boxes per
  view (up to 924 line pixels): the clearing is what removes them.
- **The typhoon** (ECMWF f072): "L ≤920" at zoom 3-8; the readout on the eye node reads "≤ 920 hPa" at zoom 8 by a mouse
  hover and by a phone long-press.
- **Taiwan** (ECMWF, zoom 6): f120 "L 935" east of the island, f126 no mark (969.6 hPa over the island: the 400-m rule), f135
  "L 983" over the strait.
- **The terrain asset:** asked once per page (field and model switches, ~40 views). Refused by interception: the marks are
  drawn as before (the Big Island "H 1020" is back), the layer = its rebuild without a mask, the debug line says no-terrain,
  and the only console line is the refused request. Allowed again: Off -> wind asks once more, it loads, and the H goes.
- **S-1, the pressure wait per step:** one step's pressure frame held, the step sought, and the zoom taken across 7.5
  600 ms later (the wind switches to full resolution and the goto is issued again). The step landed 1,003 and 1,045 ms
  after the first goto (2.19.5: ~1.6 s); a control without the zoom landed at 1,015 ms. There were no lines while the
  frame was held, and they came on release. Off during a wait left nothing behind and logged no error.
- **Playback** (shark, 1920x1080, a cold edge): 148 landings at zoom 6 and 171 at the whole-world view; 0 frames showed
  another step's lines or marks, and 0 blinks. The world view drew 13-27 marks a step; the isobar redraw (the H / L pass
  included) took p50 13.6 ms, max 28.2.
- **The page:** P-1: a stored default "OLD99" is kept when the server's 10-buoy fallback list arrives (served by
  interception), with no note; with the real 734-entry list it is cleared and the window says so. A fresh-tab smoke (GFS
  wind playing, ECMWF, SST, wave height, wind, Off) logged no console problem, and Off leaves no layer or canvas.

- **Timed cells** (G29's acceptance script with pressure counting, as in step 11; overlay 2.20.0): GFS 2026101818, warm
  at the edge for both Origins (production's was 1,244 of 1,254 cold before), browser cache off, PREGC=1, 155-s idles,
  GPU, production 2.18.4 back to back. Every test cell runs at 8 frames/s (2 at snail) with 0 skipped, 0 unpainted, in
  flight never over the cap, and one pressure download per landing under wind (none under wave height).

  | Cell | Site | Gap p50/p90 (ms) | Render p50 | Main ms/s (script) | Live / peak MB |
  |---|---|---|---|---|---|
  | wind shark 1920x1080, whole-world view (G32 B-13) | test | 125/133 | 80 | 905 (112) | +30.5 / +51.8 |
  | | production | 125/133 | 32 | 793 (28) | +25.5 / +39.6 |
  | the same, second pair (production first) | production | 125/133 | 30 | 792 (27) | +25.5 / +40.4 |
  | | test | 125/134 | 78 | 905 (113) | +31.4 / +60.4 |
  | wind shark 1920x1080 (z6) | test | 125/136 | 40 | 599 (48) | +23.4 / +38.1 |
  | | production (7.7 fps: one 1.3-s fetch stall) | 125/134 | 39 | 523 (27) | +20.5 / +28.9 |
  | wind shark phone, whole-world view | test | 127/134 | 17 | 313 (71) | +14.5 / +24.3 |
  | | production | 129/134 | 17 | 230 (21) | +10.9 / +23.1 |
  | wind shark phone (z6) | test | 126/135 | 17 | 249 (25) | +14.5 / +26.5 |
  | | production | 126/134 | 17 | 210 (19) | +10.9 / +21.7 |
  | wind shark 1280x800 | test | 126/133 | 21 | 444 (43) | +18.7 / +34.0 |
  | | production | 125/133 | 21 | 397 (31) | +15.4 / +23.8 |
  | wave height shark 1920x1080 (no isobars: the control) | test | 125/134 | 38 | 653 (27) | +31.8 / +45.7 |
  | | production | 126/137 | 38 | 641 (27) | +31.8 / +45.7 |

  - Live sets are within production + 10 MB in every cell (largest +5.9).
  - The isobars cost about 10.5 ms of script per landing at the whole-world view (step 11: about 7.6) and 2.6 ms at z6
    (step 11: about 1.1).
  - Peaks are single GC-timed samples:
    - the whole-world cell is +51.8 and +60.4 against production's +39.6 / +40.4 (this session's range + 10 = +50.4);
    - the 1280 cell is +34.0 against +33.8.
    - Step 11 (2.19.5) had the same pattern in the same cells (+48.9 to +58.2 against +43.3 to +47.1).
    - The new H / L pass allocates what 2.19.5's did per redraw: 0.59 vs 0.56 MB at the whole-world view
      (scratch g32/v3/bench_hl.js). So the peaks are GC timing, not fix round 3. Owner awareness, as in step 11.
  - The wave-height control is identical on both sites (+45.7 / +31.8), so the machine was in the same state for both.
  - 2.20.1 (V3-3, below) adds no measurable cost: the H / L pass within run-to-run noise at every view in Node, with
    allocation equal or lower. These cells stand for it.

### Findings
- **V3-3 (P2, found and fixed: overlay 2.20.1 @ 063afc4): a broad flat high lost its H at zoom 8-9.**
  - Cause: the zoom-9 window is a few nodes high, and a broad high's patch of equal codes ran out of it. Every node
    inside deferred to an equal neighbour earlier in raster order, back to the patch's first node outside the window,
    so no candidate was left (traced, scratch g32/v3/trace_z9.js).
  - Out of sample on the live runs (156 deep highs and 423 deep lows, >= 4 hPa prominent, 20 frames), at zoom 6 / 7 /
    8 / 9:
    - 2.20.0: highs 100 / 100 / 98.7 / 89.1 %, lows 100 / 100 / 100 / 99.8 %;
    - 2.19.5: highs 69 / 1.3 / 0 / 0 %, lows 99.8 / 91.5 / 49 / 49 %;
    - 2.20.1: 100 % for both at every zoom (scratch g32/v3/broad.js).
  - Fix: the tie rule counts an equal neighbour only inside the window. The terrain test and the clamp's latitude use the
    patch's first node in raster order and its middle, not the candidate that found it (which one does depends on the
    view).
  - The test file's oracle states the same rule, with three pins (a cut window keeps the mark; the terrain and the
    clamp's latitude follow the first node).
  - Verification: the new test file fails on 2.20.0 (both pins plus the random brute-force test). Mutants 9 of 10
    killed; the wrap shortcut is equivalent. The oracle = the module in 256 of 256 cases; 23,255 marks, 0 violations.
    pytest 1,190, Node overlay 410, UI 430.
- **V3-4 (P3, efficiency): the H / L pass costs about 10x 2.19.5's at zoom 6.** Warm in Node: 0.85 ms vs 0.15-0.3 ms
  at 1920x1080; in the browser about 1.5 ms more per landing.
  - In a CPU profile, the prominence pass takes 44 % and `patch` 41 %. Most of `patch`'s share is allocating and zeroing
    a whole-grid `Uint8Array(rows * cols)` (260 KB) on every call, whatever the window size.
  - Inside the frame budget (8 fps held). A later improvement: keep that array in the layer's scratch and clear only the
    nodes it touched.
- **V3-1 (owner's call, not a defect of the build): a typhoon crossing a mountainous island loses its L.** The 400-m mask
  removes real centres too. On the live runs the typhoon is unmarked while over Taiwan for about 10 h: GFS steps 114-123,
  963-985 hPa; ECMWF steps 123-132, 961-980 hPa. The same holds for Luzon, Hispaniola, Japan or New Guinea.
  - The mountain artefacts the mask exists for are at most 11 hPa prominent (GFS) and 13.8 hPa (ECMWF); p99 9.4 / 10.5.
    The typhoon over Taiwan is 31-48 hPa prominent.
  - An exception "over high ground a mark needs >= 15 hPa of prominence" brings back 0 artefact marks on either run, and
    keeps the typhoon. It also brings back other marks: 912 over the GFS run and 191 over ECMWF.
    - Some are real (the Scandinavian high over 1,520 m).
    - Many are reductions over plateaus that move with temperature: a 1,046-hPa "high" over Tibet at 5,100 m, the
      Antarctic plateau at 3,720 m, the Altai at 2,380 m.
    - A height cap (for example the exception only below 2,000 m: Taiwan's nodes are 620-1,880 m) would keep the
      islands and drop the plateaus.
  - Measured with scratch g32/v3/terrain_prom.js; no code changed.
  - Options for the owner:
    - (a) keep the rule as it is;
    - (b) mark a centre over high ground when it is at least 15 hPa prominent and the ground is below 2,000 m (islands
      and coastal ranges; not the plateaus);
    - (c) the same for lows only. Every persistent mountain site on both live runs is a high (GFS 32 H / 0 L, ECMWF
      50 H / 0 L, scratch g32/v3/persist_kind.js). At >= 15 hPa this brings back 0 artefacts and 83 (GFS) / 26
      (ECMWF) low marks over the runs, the typhoon among them; 8 / 2 of those sit over 2,000 m or more (plateau
      lows), so 75 / 24 remain with the height cap.
- **V3-2 (P3, owner awareness): near a clamped eye the readout's value is an upper bound.** The frame stores everything
  below 920 hPa as 920. The readout interpolates between the eye node and its neighbours, so 2-3 px off the node it reads
  920-941 hPa while the truth is lower. The "≤" shows only within half a code of the node.
  - Option: show "≤" whenever one of the four nodes around the cursor is clamped.

### 2.20.1 on the test site (test @ f8d59bd: the cherry-pick of 063afc4; live 04:27:23 UTC)
- The free instance was waking from idle when the deploy began (one `/healthz` poll timed out at 04:26:24, the page took
  14.5 s); the switch itself had no failed request. The six assets are byte-identical to the commit; 2.20.0's URL 404.
- The four V3-3 cases (GFS f046, f092 and f246, ECMWF f027) show their H at the view's centre at zoom 7, 8 and 9. The layer
  = its rebuild, there is no stray ink and every box is clean. The page = the Node oracle in 12 of 12 views.
- Regression, the desktop check again (GFS f010 and f116, ECMWF f072, 12 views each): all 36 views pass; 149 mark boxes
  clean (lines ran under 75 of them); 0 invariant violations. The page = the Node oracle in 36 of 36 views (193 centres).
  - The typhoon "L ≤920" at zoom 4-8, and "≤ 920 hPa" on the eye node at zoom 8.
  - Taiwan as before (V3-1). The terrain asset asked once.
- Playback (shark, 1920x1080, the edge warm): 201 landings at zoom 6 and 201 at the whole-world view, 8.04 frames/s, gaps
  125 / 134 ms; 0 frames with another step's lines or marks, 0 blinks; 13-26 marks a step at the whole-world view.
- The page smoke again: P-1 kept / cleared as designed, the phone long-press "≤ 920 hPa" at zoom 8, a fresh tab with no
  console problem, and Off leaves nothing.
- Cleanup: the browser closed by DevTools and its profile removed. The test origin's storage was emptied by the last
  script; production's lived only in that removed profile.
- Sheet for the owner: scratch g32/v3/shots/isobars_fix3_test_site.jpg.

### Next
- The owner's call on V3-1 (below are the options).
- A short fresh check at MAX: a new rule (fix round 3) plus the V3-3 change to the candidates.
- Then step 12.

### The owner's call on V3-1 (2026-10-19)
"Lows only, if strong": a low over high ground keeps its mark when it is at least 15 hPa prominent and the ground is
below 2,000 m (at the patch's first node and its middle); highs over high ground stay masked (every persistent mountain
site on both live runs is a high). Fix round 4 (overlay 2.20.2) scope:
- `findCentres`: for an L whose first node or middle is high ground (>= 400 m), the prominence is computed and the mark
  kept when it is at least ISO_TERRAIN_LOW_HPA (15 hPa, in codes) and both nodes are below ISO_TERRAIN_LOW_MAX_M (2,000 m);
  an H on high ground, or an L below the threshold or above the cap, marks nothing as before.
- The test file's oracle states the same rule; pins: a deep low over mid-height ground is marked, a shallow one is not,
  a deep low over >= 2,000 m is not, a deep high over mid-height ground is not; the threshold and the cap at their
  boundaries.
- On the live runs: the typhoon keeps its L over Taiwan (GFS steps 114-123, ECMWF 123-132), the persistent mountain sites
  stay 0, the low marks returned counted (expected about 75 / 24 over the runs).
- README; asset 2.20.2 + the pin; the test site at MAX; then the short fresh check at MAX; then step 12.

## Fix round 4 and the dropdown redesign (Opus 5.5 HIGH, 2026-10-19)

### The rule (overlay 2.20.2 @ 1ab1f31)
- `findCentres` takes `strongLowHpa` and `terrainCap`; the layer passes `ISO_STRONG_LOW_HPA` (15 hPa) and
  `ISO_TERRAIN_CAP_M` (2,000 m, code 100). The names differ from the plan text above (ISO_TERRAIN_LOW_HPA /
  ISO_TERRAIN_LOW_MAX_M); the values are the owner's.
- Over high ground (the patch's first node or its middle at or above 400 m) a candidate marks only when it is an L, its
  prominence times the code step is at least 15 hPa, and the ground at BOTH nodes is below the cap. Highs over high ground,
  weaker lows, and lows over 2,000 m or more stay unmarked. Without the two options the rule is fix round 3's.
- A low alone in its region has the cap prominence (never absorbed), so it counts as strong. On the live frames this
  marked no mountain site (below).
- The test file's oracle states the same rule. New pins: 800 m and 1,980 m marked, 2,000 m not; no exception without either
  option; a high never; a 10-hPa low beside a deeper one not (alone it would be the region's only low); 28 codes marked and
  27 not (15 hPa = 27.2 codes); either node over the cap, none. The random oracle test runs the exception in its
  variants (none, some, every low; caps over and under the ground). The layer test: a 20-hPa low over 800 m keeps its L,
  2,200 m takes it.

### The dropdown and the top of the window (owner, 2026-10-18/19; template only, UI asset unchanged)
- Every saved row: the label, then the shaka (title "Default: site opens here"), the star ("Favorite"), Rename for a kept
  point, and the cross ("Remove"). Off: hollow and grey; on: the shaka skin toned (#f3c79c, edge #a86a43; the owner's own
  drawing, traced to a 24-unit SVG path from the owner's image: scratch g32/v4/trace_shaka.py, build_shaka.py), the star
  gold. The hint line under the rows is gone.
- The toggles are independent. A default need not be a favorite (it stays listed); a point let go with the star stays
  listed while it is the default, and its label still opens it. A row keeps its place until the list closes, also once
  both toggles are off (one click undoes a slip); the next opening lists the default first.
- The top of the forecast window (owner: "use the shaka at the top of the window as well for the default") @ 554bed8:
  the shaka before the star in the trigger box, the same pair as the rows (aria-pressed toggles). The three-step star cycle
  (empty, gold, blue) is gone; the star no longer touches the default. A named point let go with the star is asked first
  and keeps its name for the way back on this page. The top star is grey when hollow (it was gold), as the rows.

### Suites and mutants
- pytest 1,190; Node 841 (UI + overlay). Mutation: 16 of 16 targeted mutants killed (the rule's type, both-node and cap
  conditions, the boundary, the layer's options; the picker's per-opening rows, default-first, focus, ✕, option-less
  points, the name round trip, aria-pressed, Rename only when kept).
- LESSON: a failing `assert.equal` on two fake-DOM nodes made Node run out of memory printing the diff (it looked like an
  infinite loop): compare DOM nodes with `assert.ok(a === b)`.

## Test-site verification of fix round 4 and the dropdown (Opus 5.5 MAX, 2026-10-19)
test @ 692d584 (cherry-picks of 1ab1f31, 882da39, 554bed8, 0a135e6; site files = the feature branch except the three
test-logo lines), pushed 06:18:39 UTC; the new instance answered from 06:20:06 UTC (the old one until 06:20:03), no poll
failed across the switch (the one failed poll, before the push, was the free instance waking up). The six overlay assets
at `?v=2.20.2` = the commit byte for byte, immutable; 2.20.1 answers 404.

### The rule on the live runs (Node; GFS 2026101900 is out of sample, ECMWF 2026101812)
- The module = the test file's oracle at the layer's own inputs in 256 of 256 frame x view cases (1,557 centres).
- Lows back over high ground (whole grid, zoom-6 radius): GFS 52 at 39 sites, ECMWF 26 at 23 sites; 0 highs; no site on
  25 % of the steps or more. Persistent mountain sites with the rule: 0 on both models; the Big Island H 0 of 209 / 0 of 85.
- Over 294 frames x 16 views: 23,456 marks, 302 over high ground, every one a strong L below the cap; 0 other
  invariant violations.
- The typhoon over Taiwan: ECMWF f123 961 hPa (860 m), f126 970 hPa (1,880 m), f129 975 hPa, f132 980 hPa all marked; the
  GFS 00Z run has moved past Taiwan (the saved 18Z run kept its L at f114-f123).

### On the test site (headless Edge with the GPU, trusted input)
- The look, desktop 1280x800 and phone 375x812 (GFS f007, f116; ECMWF f072; 12 / 4 views): every view = its independent
  re-render (0 stray or missing ink), mark boxes clean, 0 invariant violations, strong lows counted apart (GFS f116: the
  Pacific z3, the Bolivian lowlands z5, the world view). Taiwan at zoom 6: f120 "L935", f126 "L970" (2.20.1: none),
  f135 "L983". The typhoon "L≤920" at zoom 4-8. The terrain asset asked once.
- Playback (shark, 1920x1080, the new GFS run streamed from a cold edge): 0 frames with another step's lines, 0 blinks.
- The top pair and the dropdown, 31 checks, all pass: hollow grey at first; the star gold with no default; the shaka
  skin toned (computed fill rgb(243, 199, 156)) with the position; the star off leaves the default; the row shows the same
  states and titles, no hint line; the default first; a row shaka moves the default with the rows in place and the focus
  kept; a row star off keeps the row until the list closes; ✕ clears both; Shift+Tab from the name reaches the star then
  the shaka, Space toggles, Escape closes the list; phones 375x812 and 360x640: the bar and the full screen keep a point's
  coordinates whole, 44-px targets, no sideways scroll, taps on both toggles; no console problem.
- Timed cells (GPU, the edge warmed for the test origin, the browser cache off, 155-s idles, a forced collection before
  play; GFS 2026101900, shark 4x): wind 1920x1080 8 of 8 frames/s, gaps 126 / 136 ms, 0 of 226 unpainted, 0 skipped,
  pressure 1.02 downloads a landing, in flight <= 5 / 5, heap peak +35.8 / live +23.1 MB; the whole-world view 8 of 8,
  125 / 134 ms, 0 of 224 unpainted, render p50 84 ms, peak +54.5 / live +31.3 MB; phone 375x812 8 of 8, 125 / 134 ms,
  0 of 145 unpainted, peak +25.7 / live +14.1 MB. The same as fix round 3's test cells (the rule adds one comparison per
  candidate over high ground).
- The test branch's suite: 931 passed, 5 failed = the test site's own logo lines against the page snapshot (4 snapshot
  tests) and the reef module's numpy import (known).

### Next
- A short fresh check at MAX (the new rule and the picker's new state), then step 12 (production). (Done: below.)

## Short fresh check of fix round 4 and the dropdown (Opus 5.5 MAX, 2026-10-19)
One fresh-context reviewer (code + data + the test site with trusted input; brief scratch g32/v4/fr4check/brief.md, report
fr4check-report.md, scripts in its `s`, outputs in `out`) on feat/isobars-geo @ ad5efab / test @ 692d584. Before launching
it the author measured K-1 (below) and gave it to the reviewer with a proposed fix. **0 P0, 0 P1, 3 P2 (K-1, R1, U1), 11
P3.**

### The author's own finding (K-1, P2, CONFIRMED by the reviewer)
The strength test read the prominence the H / L pass already computes, which is measured inside the VIEW's window (and on
coarsened blocks at wide views). A low alone in a zoomed-in window gets the cap prominence and so passed as "15 hPa
prominent". Over 294 live frames x 22 high-ground places x zooms 5-10 (1280x800, the page's own inputs), the exception
drew 508 / 447 / 337 / 92 / 17 / 3 marks, of which 365 / 367 / 302 / 79 / 13 / 1 are under 15 hPa over the whole grid
(median 6.6 hPa): e.g. a 1,006-hPa Sudan low (460 m, 5.5 hPa) at zooms 5-7 over Ethiopia in most frames. The reviewer's
own 40 random places: from zoom 7 on every mark the exception adds is weak. On the live test site (GFS 2026101900, 180
views at zooms 5-7) 28 of 31 such marks were weak; a 4-degree pan made "L 1007" at Durban (5.0 hPa) appear and go.

### The reviewer's findings
- R1 (P2, CONFIRMED on a prototype): the proposed fix counted its exploration cap (20,000 nodes) as strong: that re-admits
  K-1's own example (11 of 37,061 high-ground low patches, all cap hits: the Sudan / Ethiopia low in 5 ECMWF frames, a
  Zambia low). Counted as WEAK: 0 wrong decisions against the whole-grid prominence (no strong low needs 20,000 nodes).
- R2 (P3, CONFIRMED on a prototype): with only the strength test replaced, the coarse windowed prominence still feeds the
  >= 2-codes check and the declutter order: a 984-hPa Yukon low (31.4 hPa) has no L at zoom 3. A strong low takes
  max(window, search) for those: 0 strong lows lost in 3,892 views.
- R3 (P3, CONFIRMED): today 5 near-threshold strong lows over high ground (15.4-16 hPa) lose their L at zooms 2.3-4.5
  (coarse blocks read 1-3 codes low; the 994-hPa Bolivian lowlands low unmarked from the world view to zoom 4). R1 + R2
  cure it. The Taiwan typhoon keeps its L in every view tested.
- R4 (P3, CONFIRMED): the test oracle transliterates the module's exception on the same windowed prominence (it pins the
  implementation and reproduces K-1); one new pin states K-1 as expected ("alone in its region it is never absorbed").
- R5 (P3, CONFIRMED): README "on the same 294 live frames: 78 GFS and 26 ECMWF lows back" (78 was the 18Z GFS run; the
  current run gives 52); "a weaker low there has no mark" is false from zoom 5 (K-1).
- U1 (P2, CONFIRMED on the test site): on phones the 44-px top shaka takes a named point's NAME in the bar and the
  full-screen header: 360 px: 0 px of name for every name ("— 21.667N 158.054W"); 375 px: 15 px ("B..") vs 59 with the
  star alone; 390: 30 vs 74; 414: 54 vs 86-98. Coordinates stay whole; the author's phone check used an unnamed point.
- U2 (P3, CONFIRMED on the site): after a ✕ a row added in the same opening lands mid-list: `listed()` ranks by
  `seen.size`, which shrinks on `seen.delete()`.
- U3 (P3, CONFIRMED on the site): the top shaka is enabled while a linked point's forecast loads: one click made a land
  point (Denver) the default before its refusal; every new visit then opens on land (the old cycle had the same race).
- U4 (P3, CONFIRMED in code): the two name memories (rows: `seen.p`; top: `goneNames`) do not meet: a named point let go
  with one star and kept with the other comes back unnamed.
- U5 (P3, CONFIRMED fake DOM): the 'allshore:points' focus restore maps the focused button by its INDEX in the row;
  Rename appears / disappears with "kept", so another tab's change moves the focus from Rename to ✕ (Enter then removes the
  row and clears the default) or back.
- U6 (P3, CONFIRMED fake DOM): with storage refusing writes the row star announces "Added favorite" and stays hollow
  (`writeFavs` swallows the error; the old top star had the same pattern).
- U7 (P3, CONFIRMED): test gaps: the row star's 100-favorite guard (without it a 101st favorite silently drops the oldest),
  its refused-point check, `listed()`'s `seen.p` update; 2 harmless survivors.
- U8 (P3, CONFIRMED by computation): the shaka's on / off states differ in luminance only by the fill: #f3c79c is 1.56:1
  on white (1.40:1 on the hover background), under WCAG 1.4.11's 3:1.
- U9 (P3, pre-existing, CONFIRMED fake DOM): the favorites / default 'storage' listener rebuilds an open list without
  restoring the focus.
- Held up: pytest 1,190, Node 841; the record's 52 / 26 lows back; rule mutants 12 / 14 (2 equivalent); picker mutants
  24 / 29 by behaviour (5 = U7); the author's site checks (titles, colours, 44-px targets, Tab order, coordinates whole, no
  console problem); the shaka trace follows the owner's drawing; the clamped ends (no code-1 patch over 400-2,000 m; 16
  clamped H over Tibet, none marked). Not checked: real phones, Safari, screen readers; U5 across two live tabs.

### The author's check of the findings
K-1 was the author's own measurement (scratch g32/v4/verify/strong_zoom.js) and the reviewer's agrees to within 3 marks
(it reads the patch's own node, not the rounded middle). U1 seen on the reviewer's screenshot (fr4_phone375_bar.png:
"B.. — 21.667N 158.054W"). U2, U3, U4, U5, U6 and U9 are evident in the code (the rank from `seen.size`; the shaka's
disabled rule without a "served" state; two maps; `acts.indexOf(a)`; `writeFavs`' empty catch; the storage listener's bare
`renderFavs()`). U8: luminance recomputed (1.56:1). R1 / R2 concern the proposed fix: adopted as stated.

### The owner's calls (2026-10-19)
- U1: "Pair in the open window": on phones the bar (and the full-screen header) shows the name only; the shaka and the
  star sit in the opened window's toolbar row.
- U8: "A. Light (keep)": the skin tone stays #f3c79c (accepted; the star beside it and aria-pressed carry the state).

### Fix round 5 scope (overlay 2.20.3; template, golden in its own commit)
1. The strength test (K-1, R1, R2, R3): a view-independent minimax search from the candidate's patch over the whole grid
   (8 neighbours, columns wrap, rows clamped): the heap's minimum is popped; it stops at a node lower than the patch (or an
   equal code outside it with an earlier raster key) -> prominence = the popped level minus the patch's code; strong when
   the POPPED level reaches the patch's code + ceil(15 / k) (28 codes); 20,000 explored nodes = weak. A strong low's
   prominence for the >= 2-codes check and the declutter = max(window, search). The result reset per candidate; cached
   per frame and patch (pans and zooms then cost nothing).
2. The test oracle (R4): an independent whole-grid minimax prominence for the strong test (not the windowed one); pins:
   a weak low alone in a zoomed-in window is not marked at any zoom; the same strong low at every zoom; the cap is weak; the
   R2 case; the popped-level and reset pitfalls; the K-1 pin removed.
3. README (R5) with the numbers measured under the new rule.
4. U1 (owner): a second pair in the forecast window's toolbar, shown only in phone mode; the trigger box's pair hidden
   there; one state for both pairs.
5. U2 a rank counter; U3 a point's toggles disabled until its forecast has been served on this page (unless kept or the
   default); U4 one name memory for rows and top; U5 + U9 focus restored by the button's role and the row's id, for both
   listeners; U6 a refused write says so and changes nothing; U7 the three tests.
6. Then the test site at MAX (the reviewer's K-1 and site_names scripts: 0 weak additions, names whole on phones), and a
   short fresh check of the search at MAX if the build changes anything beyond this scope.

## Fix round 5 (Opus 5.5 HIGH, 2026-10-19)
feat/isobars-geo @ d3073d1 (code), eaf2a26 (golden), then the mutant pins; overlay asset 2.20.3; NOT on test or production yet.

### The rule (K-1, R1-R3)
- `strongLowSearch(frame, first, need, cap, ss)`: from the patch's FIRST node over the whole grid (8 neighbours, columns
  wrap, rows end at the poles), level by level: a queue per code, and since the greatest code along the way only rises,
  each node is queued once at its final level (no heap). It ends at the first node TAKEN that is lower than the patch, or
  of its code and earlier than the patch's first node in raster order -> weak, prominence = that level minus the patch's
  code. It is strong once the level being taken reaches the code + need (need = ceil(15 hPa / k) = 28 codes); a node
  merely queued that high proves nothing. More than `ISO_STRONG_NODES_MAX` = 20,000 nodes taken counts weak (R1). A strong
  result reports `need` (all it proves; the cap when nothing lower can be reached at all).
- In `findCentres` the search runs only for a low over high ground below the cap, once per frame and patch (`strongOf`:
  a Map on the layer's scratch keyed by the patch's first node, reset when the frame's codes, need or cap change), and a
  strong low's prominence for the 2-code check and the declutter is max(window, search) (R2). `sres` is set per candidate.
- Saved live frames (scratch g32/v4/verify/strong_zoom.js: 294 frames x 22 high-ground places x zooms 5-10): drawn marks
  the exception adds 508 / 447 / 337 / 92 / 17 / 3 -> 145 / 80 / 35 / 13 / 4 / 2; under 15 hPa over the whole grid
  365 / 367 / 302 / 79 / 13 / 1 -> 0. The script still flags 3 / 2 / 1 at zooms 5-7: one low (ECMWF f120, 998.3 hPa,
  45.25 N 106.25 W, 1,240 m), which an independent threshold flood finds 30 codes (16.5 hPa) prominent; the script reads
  the prominence at the rounded middle of a two-node patch, a node off the patch (scratch g32/fix5/check_case.js).
- Lows back over high ground (whole-grid window, zoom-6 radius; scratch g32/v3/strong_low.js): GFS 2026101900 52, ECMWF
  2026101812 26, GFS 2026101818 77 (78 before: one low the window counted strong is weak over the grid); 0 highs; 0
  persistent sites; the Taiwan typhoon keeps its L at every step it did.

### The oracle and the pins (R4)
- The test file's brute force judges the strength by threshold floods over the whole grid (from the patch's first node,
  one flood per level up to code + need - 1; nothing shared with the module's search); its random test passes
  `strongNodesMax: 1e9` so the cap never decides there (the cap has its own pin).
- New pins: a WEAK low (12 codes) over 800 m alone in a zoomed-in window is never marked at zooms 3-10, centred or panned
  (over the sea the window's cap prominence marks it: the K-1 case); a STRONG low (40 codes) is marked at zooms 2.3-10
  with prominence >= need; the R2 ridge case (a stride-6 window reads 0 codes; over high ground the mark keeps `need`, over
  the sea the coarse rule drops it as before, at stride 1 its own 100 codes); the cap (weak below the nodes it needs); the
  taken level (a wall of 200 beside the pit proves nothing while a 10-code saddle leads lower); ties (an equal patch earlier
  in raster order is lower, a later one not); the per-candidate reset; the per-frame cache (one search, the same result
  object for another window, a new frame searches again); the date line; a patch cut by the window (the search starts at
  its first node). The K-1 comment is gone.

### README (R5)
The strong-low paragraph states the whole-grid judgement and the measured numbers above.

### The picker (U1-U7, U9)
- U1 (owner): `#fwPair` (the shaka and the star) at the start of `#fwToolbar`; the CSS shows it only in phone mode
  (`max-width: 500px` or `max-height: 500px`), where the trigger box's pair is hidden: the bar and the full-screen header
  show the name alone. One state: `refreshCurrent` writes both pairs; the same handlers (`favClick`, `dflClick`); Escape
  on any of the four closes the list.
- U2: ranks from a counter reset at each opening, never `seen.size`.
- U3: `served` = the stations whose forecast this page showed ('allshore:forecast' with `ok`); a point not kept, not the
  default and not served has its pair off ("Waiting for this point's forecast").
- U4: one `goneNames` Map for the rows and the top; the rows' star records the name when it lets a point go and uses the
  memory first when it keeps it again.
- U5 + U9: `focusMark` / `focusBack`: the focused control's role (`fav-select`, `sr-dflt`, `sr-fav`, `pt-ren`, `sr-rem`)
  and its row's id, restored after both listeners' rebuilds (Rename going away puts the focus on the row's label, never on
  ✕).
- U6: `writeFavs`, `toggleFav`, `removeFav`, `clearDefault` return whether the browser stored it; every caller says
  "This browser could not change the favorites (its storage is switched off)." and changes nothing.
- U7 + new tests: tests/ui/mypoints.test.js +7 (the pair; U2; U3 with a refusal, a kept point, the default; U4 both ways
  and a default point; U5/U9; U6 rows, ✕, top star, a row adding; U7 the 100 guard, the refused point, seen.p after a
  rename); page pins for the markup, the phone CSS and the new lines.

### Checks
- Suites: Node 855 (isobars 47, mypoints 34); pytest 1,190 (the 4 golden tests fail at the code
  commit by design, pass after the re-capture).
- On the pre-fix template the U1-U6 tests fail (6 of 7; U7 pins existing behaviour).
- Mutants (scratch g32/fix5/mut5.py, a scratch worktree, after pytest): search 15 / 15 killed (S8 columns wrap and S15
  the patch's first node survived the first pass: pinned); picker 19 / 20 (P6 and P10 survived the first pass: pinned;
  P2 = the counter not reset at an opening, equivalent: ranks are compared within one opening only).

## Test-site verification of fix round 5 (Opus 5.5 MAX, 2026-10-19)
test @ 631bef1 (cherry-picks of d3073d1, eaf2a26, 3ed0b3c; the record file left off the test branch; site files = the
feature's but the 3 test-logo lines). Test-branch suites: Node 854 (the known one less), pytest 931 + the 5 known failures
(4 page snapshots on the logo lines, the reef module's numpy import). Pushed 20:49:23 UTC; the old instance answered until
20:50:37, the new one from 20:50:40: 52 polls (robots, healthz, the page) 0 failed. Served: the six overlay assets at
?v=2.20.3 = the commit (immutable, nosniff), the 2.20.2 URLs 404; the UI assets 1.23.1 unchanged; the page names 2.20.3 and
carries #fwPair. GPU headless Edge (scratch g32/v4/verify: hl on port 9651; s/v5_*.js; out/v5_*), trusted mouse, keys and
touch; the live runs GFS 2026101912 and ECMWF 2026101912 (out of sample: newer than every frame the rule was tuned or tested on).

### The rule
- K-1 on the site (s/v5_k1.js: the short fresh check's 180 views over high ground (10 steps x 6 places x zooms 5-7) + 72 at
  zooms 8-10, the page's own module at the layer's inputs, an independent in-page threshold flood for each low): drawn lows
  over 400+ m under 15 hPa: 0 (2.20.2: 28 of 31 on the same places); layer counts = projectCentres in every view.
- The served module on today's 294 frames (fix5/oracle_live.js): = the test file's brute force (now with the flood) in 256
  of 256 frame x view cases (1,688 centres; the oracle has no node cap, so the 20,000-node cap changed no answer there);
  23,608 marks, 295 over high ground, all strong lows; 0 persistent mountain sites on either model; the Big Island H 0.
- The rule per low (fix5/taiwan_scan.js, the whole-world window at the zoom-6 radius, every frame): 22,827 lows over 400+ m;
  marked if and only if both nodes are under 2,000 m and an independent flood finds 28+ codes: 22,827 agree, 0 disagree
  (111 kept, 22,716 dropped, 7,393 of them over the cap).
- The positive control: today's typhoon crosses Taiwan (GFS f102-f104 over 1,000 m, 972-975 hPa; ECMWF f096 / f099, 960 /
  972 hPa): on the site its L is drawn at every zoom 3-10 on both models (s/v5_typhoon.js: 40 of 40; flood 28+ codes;
  window prominence 65-91 codes at zoom 3, 79-82 at zoom 4 (the cap on ECMWF f096), the cap from zoom 5). The weak lows that
  follow it over the hills of south-east China (996-1,009 hPa, at most 26 codes = 14.3 hPa) have no L.
- Cost: the H / L pass per new frame (Node, warm, the layer's inputs, fix5/hl_cost.js): whole world 1920 10.1 -> 11.3 ms p50
  (11.8 -> 13.7 p90; 3.7 searches per frame on average, at most 11), 1280 world, Asia z3, W Pacific z4 unchanged, Hawaii z6
  +0.2 ms.

### The picker
- Phones (s/v5_names.js, the short fresh check's six names, 360 / 375 / 390 / 414 px + a landscape 812x375): the bar and
  the full-screen header without the pair, the coordinates whole everywhere; the name in the bar 86-88 px at 360 (2.20.2:
  0), 103 at 375 (15), 118 at 390 (30), 142 at 414 (54): names up to 12 characters whole on every phone, 15 from 390 px,
  20 at 414; longer names are cut, never the coordinates. Full screen the toolbar pair at the toolbar's start, 44 x 44 px
  (32 on the landscape phone), the shaka first, in the trigger pair's state; desktop 1280x800: no toolbar pair; a short
  window (1280x480, the window's phone mode) the phone rule. 133 of 133.
- Interaction (s/v5_pair.js): desktop: the fix-round-4 sequence (hollow / gold / skin tone, titles, row buttons, Shift+Tab,
  Space, Escape) with the toolbar pair following every change; U2 on the site (a row added after a ✕ goes last). Phones
  375 / 360 / 812x375 with a NAMED kept point: no pair in the bar; full screen the toolbar shaka makes it the default (skin
  tone, its position), the toolbar star asks and lets it go, keeps it again WITH its name; the list opened from full screen;
  its row star lets it go and the toolbar star brings it back named (U4); minimised: the bar without the pair; no sideways
  scroll. 51 of 51.
- U3 (s/v5_u3.js, the point's /api/forecast held by the DevTools Fetch domain, the head's early request included): a linked
  point not kept has both toggles off while its forecast loads ("Waiting for this point's forecast: 21.700N 158.200W"), a
  trusted click stores nothing (2.20.2: the shaka was enabled 122 ms after load); released, both act; the default point and a
  kept point act while loading; a land point is refused: off, "No forecast here". Desktop and the phone's toolbar pair: 18 of 18.

### Regressions
- The look (s/b1_look.js): 36 desktop and 12 phone views (both models) = their re-renders, no flagged view; the ECMWF
  typhoon (938 hPa) L at zooms 4 / 6 / 8, the hover "938 hPa" over its eye; the terrain asset once per page.
- Playback with isobars (s/b4_play.js, 1920x1080, wind at shark): 0 frames with another step's lines or marks, 0 blinks.
- Console: no problems in any fresh page (only the checks' own canvas readbacks warn).
### Timed playback (wind at shark speed, GPU headless Edge, frames warm at the edge for both origins, a collected heap
before play, 150-s idles, the test site and production back to back; scratch g32/v4/verify run5.sh / run5b.sh / run5c.sh,
out/v5_cells*.log)
The machine changed state during the session: the browser's own render time per frame ran 34-87 ms (as in fix round 4) in
some runs and 95-158 ms in others (the owner's apps open: AutoCAD LT, Chrome; AC power, total CPU 9-17 %), on BOTH sites
alike. In the slow state production misses the frame-rate line too.

| cell | site | render p50 | fps | gap p50 / p90 ms | unpainted | heap peak / live (MB over Off) |
|---|---|---|---|---|---|---|
| 1080p, the default view | test 2.20.3 | 40 / 43 | 8.0 / 8.0 | 125/135, 126/135 | 0 | +40.7 / +27.3, +39.6 / +25.5 |
| | production 2.18.4 | 42 / 40 | 8.0 / 7.5 | 124/138, 125/135 | 0 | +30.7 / +19.9, +32.2 / +20.0 |
| | test (slow state) | 104 | 7.8 | 126/155 | 0 | +46.8 / +28.4 |
| | production (slow state) | 95 | 7.9 | 123/155 | 0 | +41.8 / +21.6 |
| whole world (zoom 2.9) | test | 84 / 87 | 8.0 / 7.9 | 125/135, 125/135 | 0 | +57.0 / +33.5, +58.7 / +33.5 |
| | production | 34 / 110 | 8.0 / 7.4 | 125/135, 131/162 | 0 | +39.6 / +26.8, +44.4 / +25.8 |
| | test (slow state) | 143 / 129 | 4.9 / 5.6 | 200/250, 169/224 | 0 | +77.5 / +33.8, +98.8 / +34.0 |
| | production (slow state) | 158 / 158 | 5.5 / 5.4 | 181/218, 185/235 | 0 | +51.4 / +26.9, +56.1 / +27.2 |
| phone 375x812 | test | 17 | 8.0 | 124/142 | 0 | +29.0 / +16.8 |
| | production | 17 | 8.0 | 126/142 | 0 | +22.5 / +11.0 |

- In the fast state every test cell passes (8 frames/s, p50 <= 130, p90 <= 150, every landing painted, 0 skipped); its
  tile redraw (48.4-51.6 ms at 1080p, 75.3-76.6 at the whole world) and render time equal fix round 4's 2.20.2 (48.4,
  75.9; 38, 84 ms). In the slow state both sites miss the line.
- Live set: test 5.5-7.7 MB over production in every pair (the isobars, the pressure ring, the search's 2 MB of reusable
  arrays): within the owner's +10 MB.
- Peaks: within production's range + 10 MB at 1080p (both states) and at the whole world in the fast state (+57.0 / +58.7
  vs production's +39.6-56.1). In the slow state the whole-world peaks (+77.5, +98.8) exceed it: the main thread is busy 71-
  85 % there and the collector runs late. Not from fix round 5: the H / L pass allocates the same per new frame as 2.20.2's
  (fix5/alloc.js, Node, a large semi-space: whole world 114-312 KB vs 141-298 KB, Hawaii z6 296-305 vs 264 KB; the
  search reuses its arrays, a Map and a result object per patch are all it adds) and fix round 4's 2.20.2 whole-world peak
  in the fast state was +54.5. Owner awareness: on a machine whose GPU is busy, the whole-world wind view at shark speed
  plays at ~5 frames/s on both sites, and the page's heap can peak ~40 MB higher than production's before it is collected.

### Cleanup and the next step
The headless Edge stopped by DevTools (its throwaway profile removed, no process left); the test origin's storage emptied
after every script. Production untouched (1a0d06e). feat/isobars-geo fast-forwards production (41 commits, 28 files).
NEXT: the owner's go-ahead for step 12 (tag prod-pre-iso at 1a0d06e; fast-forward Live-Buoy-Update to feat/isobars-geo;
production GEO_BASE needs its own go-ahead). The scope's short fresh check of the search is conditional on changes beyond
the scope: none were made.

## Production (step 12, 2026-10-19)

- Owner's go-ahead ("proceed", Opus 5.5 MEDIUM). Rollback tag `prod-pre-iso` @ 1a0d06e pushed; `Live-Buoy-Update`
  fast-forwarded 1a0d06e..95db4c1. Last old answer 22:39:10 UTC, first new one 22:39:12; 50 polls across the switch,
  0 failed.
- Assets: the six overlay assets at 2.20.3 and the UI assets at 1.23.1 equal the commit; the 2.20.2 URLs answer 404.
- Page: `VERSION = "2.20.3"`, `#fwPair` present, test-only logo count 0, `var GEO = ""` (production GEO_BASE unset: the
  site opens as before; the IP open needs the owner's own go-ahead). The three page probes (51201, 51201 graph, 51003)
  show the same change set as each other.
- Browser smoke on allshoresurf.com (GPU headless Edge): the overlay mounts at 2.20.3; wind isobars drawn; the GFS
  typhoon over Taiwan (f103) keeps its L at zooms 4, 6 and 9, and the layer's marks equal `projectCentres`; desktop:
  the shaka + star pair in the trigger box, none in the toolbar; phone 375x812: no pair in the bar, the toolbar pair
  44 px, no sideways scroll; 0 console problems. 8 of 8 checks pass. Storage emptied.
- CI on 95db4c1: tests, client-tests and the trigger Worker's build green; the geo Worker's build was running.
- Rollback: re-push `prod-pre-iso`.
