# Section 113 A: tide icons step clear of forecast dots, live dots and My points: adversarial review (G78)

Scope: `git diff df14c74 feat/tides-113` (3453251 + 7b49964): `templates/index.html`, `tests/ui/tides-page.test.js`,
`tests/test_index_page.py`, `tests/fixtures/index_golden.json`. Reviewer: one fresh adversarial reviewer, 2026-11-27.
No code was changed. Probes ran from a scratch copy of the Node test harness (the real STATE / NOTE / LIVE / BLOCK
slices, with a Mercator projection, a non-zero pane origin and Leaflet-style rounding added).

**0 P0, 0 P1, 1 P2, 7 P3.**

## What held up

- **Real data, whole world.** I used the real tide list (`_tide_list()`: 5,859 stations), plus model points at every
  forecast point more than 10 km from a station (3,958; `tools/tides/fes_points.py` puts the FES points on forecast points),
  and the 4,036 real forecast points. I tiled every 1600 x 1000 view (also 3840 x 2160) that has a tide station within
  40 px of a forecast dot, at zooms 9 to 13, mouse and touch. The result: **no tide box over a forecast dot, none inside the
  8 / 12 px gap, no two icons stacked.** Before the change, 3,701 to 3,960 icons covered a dot at each zoom.
- **Pads against the dots' tap circles on touch.** The canvas hit radius is 3 + 0.5 + COARSE_TOL = 11.5 px. The closest
  pad edge was 11.38 to 12.0 px from a dot's centre, with a non-zero pane origin and rounded container points. The only
  shortfall is about 0.1 px of rounding. The world-to-container shift through `tideDrawn[0]` is right, because Leaflet's
  `project` does not wrap and the per-point rounding is at most 0.5 px.
- **Live dots against tide pads.** At the 12 px touch gap, the live finger target reaches 13.5 px and the tide pad starts
  at 15.5 px or more, so they do not overlap. The microtask order works: the first `liveTargets` sees the old nudges, and
  the tide rebuild asks for it again, so it runs on the new ones before paint.
- **Date line and world copies.** A gauge at -179.999 with a forecast point at 179.999 steps aside in every copy that
  holds both.
- **Timing.**
  - `requestTideRebuild` → `rebuildTideMarkers` does not recurse, and a round's own tide rebuild cancels a pending request.
  - Every caller of the forecast, point and live rebuilds is an event handler or async, and the layers are added before
    the handler at line 1728. So the `var tideRebuildDue = false` assignment cannot drop a request made earlier in the
    script, and nothing reaches a `let` in its TDZ.
  - A live refresh with unchanged nudges only re-runs `tideTargets` (no marker re-creation).
- **Speed.** On a 3840 x 2160 view at zoom 9, a warm rebuild of 448 icons takes 10.0 ms (6.8 ms before).
- **Other checks.**
  - The golden fixture differs from df14c74 only by this template diff (109 lines, in each of the 5 captures).
  - All 650 Node tests and 73 Python tests pass.

## Findings

| # | Sev | Where | Finding | Suggested fix |
|---|---|---|---|---|
| G78-1 | P2 | templates/index.html:3596-3615 | When dots block the candidate spots, the tide-to-tide nudge is lost and co-located stations stack exactly (one unreachable). Before, the twin moved 12 px. | Give a tide-clear fallback, and as a last resort keep the old 12 / 24 ring |
| G78-2 | P3 | templates/index.html:3596-3615 (with `thinTideCopies`) | Below zoom 11, a step can bring two drawn tide icons to 12-15 px apart (boxes overlap), which defeats the 24 px thinning gap. Seen on real NOAA stations. | Below TIDE_ALL_ZOOM, require a candidate to be TIDE_GAP_PX (Euclidean) from the drawn tide icons |
| G78-3 | P3 | templates/index.html:2029, 3714 vs :560 | On touch, the tide pad stops 8 px before a My point, but a My point takes taps 12 px beyond its edge. In a 4 px ring, a tap meant for the point opens the tide window. | Make the room per kind: point 12, forecast 8 (live 8 + 2 is already covered) |
| G78-4 | P3 | templates/index.html:3596, 3566 | Opening an icon moves it under the pointer (J-Bay 21 → 23 px, touch 25 → 27). Opening a live buoy moves the tide icons beside it (-25 → -28). | Compute the step with the opened size (TIDE_HALF_ACTIVE, and the active live box) for every icon, so opening never moves it |
| G78-5 | P3 | templates/index.html:3590 (DIRS east first) | The steps ignore wind flags. A NOAA gauge with its own flag and a forecast dot steps 12-30 px east, over the flag's speed number when the wind is from the east (trades). | When the station's own flag is drawn, try the downwind side first, or add the number's box as a soft obstacle |
| G78-6 | P3 | templates/index.html:3564-3566; tests/ui/tides-page.test.js:703-720 | The live dot's box is taken as size/2 + border (7.5 / 10.5). Under Bootstrap's border-box the dot is 11 / 15 px including the border (`liveDotAt` uses 5.5 / 7.5). The test repeats the same numbers. | Use `liveDotAt`'s geometry (or say the extra 2-3 px is margin on purpose) and fix the comment |
| G78-7 | P3 | templates/index.html:1728, 2164, 2194 | Test gap: the new triggers (forecast list, My points, layer toggles) are not in any Node slice or Python pin. Removing all three keeps every test green. | Pin the lines in test_index_page.py, or slice `rebuildForecastMarkers` / `rebuildPointMarkers` and the handler into a Node test |
| G78-8 | P3 | templates/index.html:3710-3713; tests/ui/tides-page.test.js:56-57 | Test gap: in the stub the container equals the world (no pan offset, no rounding), so the `sx` / `sy` shift is never exercised. Dropping it passes all 33 tide tests. Test 4's "one rebuild after the round" is not counted either. | Give the stub a pane origin (and rounding); count rebuilds in test 4 |

## Details

### G78-1 (P2): the fallback drops the tide-to-tide nudge, so twins stack exactly

How `tideNudges` handles an icon that is not free:

- **When it touches a dot (`need > 0`):** it tries the rings `need`, `need+12`, `need+24`, each spot clear of the dots and
  the tides. If none is free, it tries the same rings counting only the dots (`free(..., false)`). If that fails too,
  `off` stays null.
- **When it touches only a tide (`need = 0`):** it tries the old 12 / 24 rings, but each spot must now also be clear of the
  dots. There is no fallback.

Either way, an icon that finds no spot is drawn at its own place, on top of a co-located twin. Before the change, the
old 12 / 24 ring counted only tides and always separated the twins.

Probes (`p1_ring.js`, `p3_cases.js`, run against both templates):

- **Two stations at one spot, with 8 forecast dots 22 px around them.** The pair's own spot is clear (gap 9 px), but
  every 12 / 24 px candidate is near a dot.
  - Old: S1 [0,0], S2 [12,0].
  - New: S1 [0,0], S2 [0,0], stacked exactly.
- **Twins on a forecast dot, where the only dot-clear spot is 21 px east.**
  - Old: a [0,0], b [12,0].
  - New: a [21,0], b [21,0]. b's tide-clear candidates are all blocked, so the dots-only fallback takes the first
    dot-clear spot, which is a's.
- **Any forecast grid closer than 42 px (mouse) / 50 px (touch).** No spot is 21 / 25 px from every dot. Every
  co-located pair (144 to 400 placements per grid) stacked in the new code and none in the old.

Rarity: with the real forecast list (no live dots), the world sweep found no stacks, so this is an edge case. Live dots,
My points and busy harbours make it more likely. The code comment accepts it ("a tide icon on a tide icon still opens one
of them"). But B is drawn after A at the same z-index, so A cannot be clicked at all. That breaks G27 A-F7 (stations NOAA
lists at one spot each open on their own click).

Suggested fix: in the dots-only fallback, skip spots whose centre is inside a placed tide box. When everything fails,
apply the old 12 / 24 tide-only ring before giving up.

### G78-2 (P3): steps undo the thinning gap below zoom 11

`thinTideCopies` keeps drawn icons 24 px apart (Euclidean) below TIDE_ALL_ZOOM, using the stations' own positions. The
tide-tide test for a stepped icon is only "centre outside the other's box + 1" (10 px). Real NOAA stations and forecast
points at zoom 9 (`p2_world.js`, SHOW=1):

- Annapolis (US Naval Academy) steps [-21,0] to 12.0 px from Gingerville Creek, South River.
- Sunset Beach Pier [-21,0] and Little River Neck, north end [-12,0] end up 15.2 px from each other and from Nixon
  Crossroads.

Little River Neck's -12 is a tide-tide nudge below zoom 11 that is not beside the opened icon. The thinning was built to
rule that case out. Pairs closer than 18 px at zoom 9: 17 now, 6 before (the old 6 are 17-18 px, Euclidean distance ≥ 24).
Both icons can still be clicked.

Suggested fix: below TIDE_ALL_ZOOM, require a candidate to be TIDE_GAP_PX (Euclidean) from the placed tide icons, and
thin the icon when there is no such spot.

### G78-3 (P3): touch pad vs a My point's finger target

`.my-pt::after { inset: -12px }` (line 560) means a My point takes taps 7 + 12 = 19 px from its centre. Probe (b): a
gauge on a My point, touch, steps [28,0] with pad 4, so its pad reaches 15 px from the point's centre. The tide marker's
z-index is 400 higher at the same latitude, so it sits on top. Taps 15-19 px from the point on that side therefore open
the tide window. `TIDE_DOT_ROOM_TOUCH = 8` matches COARSE_TOL (forecast dots) only. The comment at line 2027 says the
pad "ends before a dot's own tap area", which is false for My points.

This is still far better than before, when the icon covered the point.

Suggested fix: per-kind room, `{ forecast: 8, point: 12, live: 10 }` (the live target is --live-tap ≤ 8 plus 2).

### G78-4 (P3): opening moves the icon

`h` is 11 for the opened icon and 9 otherwise, and `need = ceil(h + q.h + gap)`. Probe (a):

- J-Bay with a mouse: closed [21,0], opened [23,0].
- J-Bay on touch: closed [25,0], opened [27,0].

Probe (d): a gauge beside a live dot steps [-25,0], and [-28,0] once the buoy is opened (the active live box is 10.5,
centre +7.5).

Each change of nudge re-creates the whole tide layer. In a crowded spot the larger box can also change direction, a jump
of 40 px or more under the finger. The block's own comment ("opening a station moves no other icon unless that box needs
the room", F-2) aimed for stable icons.

Suggested fix: compute the clearance with TIDE_HALF_ACTIVE for every icon (and the active live size for every live
dot), so the step is the same open or closed.

### G78-5 (P3): the step can cover the station's own wind number

The flag's speed number sits 14 px upwind of the ring (`static_ui/winds.js` NUM_OFFSET), about 7-21 px from the centre.
DIRS tries east first, so a NOAA gauge whose own `coops:` flag shares its spot with a forecast dot steps to 12-30 px
east. The tide marker (z 400) sits over the flag (z 300), so an easterly wind's digits are hidden.

Count (real wind and tide lists): 8 gauges at zoom 9 (Moku O Loe, Annapolis, CBBT, Duck Pier, Oregon Inlet Marina,
La Jolla ...), 1 at zoom 11, 0 at zoom 13. Hawaii's trades put the number on the east side.

Suggested fix: when the station's own flag is drawn, order DIRS downwind first, or add the number's box as an
obstacle that only counts while a free spot elsewhere exists.

### G78-6 (P3): the live dot's geometry in `tideObstacles`

Bootstrap 5 (line 133) sets `*{box-sizing:border-box}`, so `.live-buoy-dot` is 11 px (15 opened) including its 2 / 3 px
border. Its centre is (+6.5, -0.5) from the coordinate (opened: +7.5, -0.5) and its half size is 5.5 / 7.5, as
`liveDotAt` has it. `tideObstacles` uses 7.5 / 10.5 ("plus the border"), so live dots get 2-3 px more clearance than
forecast dots, and opening a buoy moves its neighbours further (G78-4). The test at tests/ui/tides-page.test.js:703 checks
the same 6.5 / 7.5 numbers, so it does not compare against the real CSS.

This is harmless, but it is wrong as documentation.

### G78-7 (P3): the new redraw triggers are untested

Mutation `m3` removed `requestTideRebuild` from `rebuildForecastMarkers` (2164) and `rebuildPointMarkers` (2194), and
removed the overlay handler (1728). The tide suite passed 33 of 33. No Node slice covers those lines and no Python pin
names them (`grep requestTideRebuild tests/` finds nothing). Only the live trigger (3447) is caught, by test 4.

So the forecast list arriving after the tides, a My point added or moved, and ticking or unticking the forecast, points
or live layer could all lose the step without a test noticing.

### G78-8 (P3): the world-to-container shift is untested

In the stub, `latLngToContainerPoint` equals `project` at the current zoom (no pane origin, no rounding), so `sx = sy = 0`.
Mutation `m1` replaced `q.x + sx, q.y + sy` with `q.x, q.y` and the tide suite still passed 33 of 33.

On the real map the shift is the pane position minus the pixel origin, hundreds of thousands of pixels. Without it, every
dot-limited pad would be wrong. My Mercator probe with a non-zero origin shows the shipped code is right; the tests just do
not prove it.

Test 4's title also says "one rebuild after the round", but the test only checks the final step, not how many rebuilds
ran.

Suggested fix: add `origin` to the stub's `latLngToContainerPoint` (and round like Leaflet), and count `clearLayers` calls
in test 4.

## Fix round 1 (2026-11-27)

All eight findings fixed in `templates/index.html` (`tideObstacles`, `tideWindMarks`, `tideNudges`, `tideTargets`):

| Finding | Fix | Test (tests/ui/tides-page.test.js unless noted) |
|---|---|---|
| G78-1 | A step off a dot tries, per ring (need, +12, +24, +36): clear of dots + tide icons + wind readings, then dots + icons, then dots + the box rule; else the dot-clear spot farthest from every icon (never an exact stack). A tide-on-tide step tries 12/24 px clear of the dots first, then the old tide-only ring. | "G78-1: stations on one spot never stack", "G78-1: when every spot clear of the dots is near another icon" |
| G78-2 | Below TIDE_ALL_ZOOM a step off a dot keeps TIDE_GAP_PX (24 px) from tide icons, both those placed and those still to come that stay put (`ahead`, `stays` computed once). | "G78-2: below zoom 11 ...", "G78-2: a step that would pass the 12-px rule ..." |
| G78-3 | The pad's room per dot kind on touch: forecast 8, My point 12, live 10 (mouse 2). | "G78-3 / G78-8: touch pads stop at each dot kind's own tap margin" |
| G78-4 | The step is sized for the opened icon (HD = TIDE_HALF_ACTIVE) and a live dot is the box of both its sizes, so opening a station or a buoy moves nothing. | J-Bay test (opens it at every zoom), live/point test (opens the buoy) |
| G78-5 | Wind rings and speed numbers (read from the drawn flag's `.wind-num`) are soft obstacles for a step off a dot. | "G78-5: ... keeps off a wind flag's speed number" |
| G78-6 | Live dot: centre +7.5, half 7.5 (Bootstrap border-box; the union of 11 px at +6.5 and 15 px at +7.5). | "G78-6: the step off a live dot is sized by both its drawn sizes" (exact 27 / 31 px) |
| G78-7 | The forecast, My points, live and layer-toggle triggers pinned by exact strings. | tests/test_index_page.py (tide wiring needles) |
| G78-8 | The harness has a pane offset (`o.pane`) and counts redraws (`tideLayer.clears`). | "G78-3 / G78-8" (pane moved), "one tide redraw after the round" |

Mutants (scratchpad mut.py): 10 of 11 caught; the survivor (the coalescing guard removed) only adds a no-op rebuild
(same signature: no redraw), equivalent in output. 40 tide-page tests, 657 UI tests, 73 page/overlay tests pass.
