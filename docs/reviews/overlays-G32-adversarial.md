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
