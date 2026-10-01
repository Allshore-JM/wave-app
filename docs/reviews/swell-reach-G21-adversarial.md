# G21 — the swell window projected on the map (plan section 30)

Two fresh-context reviewers at MAX effort, 2026-09-30, on `feat/swell-reach` @ `f6509f2` (UI 1.12.5, 12 commits on top of
production `b5958b6`, UI 1.11.5). The author's check of both reports followed on 2026-10-01.

- **Reviewer A** covered the code and the geometry, with Node and Python on copies of the code and no browser: the reach
  against an independently computed first coast crossing, the +-180 seam and the builder's cell lines, the latitude limits,
  the drawn shapes (also through Leaflet 1.9.4's own clip and simplify code), the small-island rule, the readout's numbers,
  cost, 1,679 staged races and 6,500 fuzzed sequences on the page controller, the fan against production, the suites and
  203 mutants.
- **Reviewer B** covered the live test site (`wave-app-clean.onrender.com`, `/ui/tools.js?v=1.12.5`) and the user
  experience at 1280 x 800, on the phone preset (375 x 812, also 812 x 375 and 768 x 1024) and against production: the
  owner's use case at 21 spots, the picture against the readout pixel by pixel, an independent first-land walk on raw
  GSHHG, Lock, the world copies, the interplay with the overlay, the windows and the markers, robustness, performance and
  accessibility.

**Result: 0 P0, 0 P1, 8 P2, 24 P3** (A: 2 P2 + 13 P3; B: 6 P2 + 11 P3; six findings overlap). Nothing blocks on
correctness: the picture, the readout and the geography are right, and the fan is unchanged. The P2s are in the extras
(rings and their labels), the phone flow, one readout wording, text clipping, one first-use task and the tests.

## What the reviewers confirmed

- **The reach is right on real data.** A: 363,600 rays from 505 spots (105 named, 400 random near-shore; both sides of
  180, 74-75 N, 60-75 S, eight enclosed seas, atolls) equal an independently computed first coast crossing, except 43 rays
  (0.012 %) that graze land within 47 m of the true great circle; with 1 km steps in place of 25 km, the same code matches
  on every ray. `reachEnd` matches on all. B: the reach AS DRAWN equals B's own walk on raw GSHHG for 15,111 of 15,120
  rays at 21 spots (7 grazes, 2 rays at a window's edge).
- **The seam and the cell lines.** No real coast is cancelled and no artificial edge survives within reach (79.2 M
  both-sides samples); 201,027 rays across and along +-180 differ only at chord grazes; tier 0 equals raw GSHHG to its
  1e-4 degree rounding (about 10 M points).
- **The limits.** `limitKm` equals a numeric scan on 42,840 triples to 5e-8 km.
- **The picture agrees with the readout.** B: 7,440 sampled pixels (zoom 1.9 to 11, three worlds each way, the dateline,
  the Arctic, the antipode, a phone), 0 contradictions away from a boundary; bearing, distance and travel times equal B's
  own formulas at all of them. A: 12.6 M points, point-in-ring differs from `probe().visible` at 0.026 %, all within
  0.42 km of the boundary below 65 degrees.
- **Rays, rings, strips, the cursor line** are where the code says (B: every ring vertex on its distance, the rays 5, 15,
  25 ..., the lighter strips equal B's own implementation of the rule at 17 spots, 300 cursor lines on their great circles).
- **The fan is unchanged.** A: 6,517 clicks in 13 regions plus 653 on tier 0 only, zero differences in placement,
  fetches, levels, texts and SVG. B: 17 spots identical on the test site and production; the measure tools too.
- **The controller.** A: no stale draw, leak or inconsistent Lock state in 1,679 staged races and 6,500 fuzzed sequences,
  one exception (A-5). B: Lock, the world copies (+-3 worlds), dateline spots, ten spots in two seconds, coast files
  unreadable, a 30-spot session: as claimed, no leak.
- **Leaflet's clip and simplify** move the fill by at most 0.93 px (A, real 1.9.4 code, 4,536 polygon/view pairs).
- **Cost.** B, this desktop: first use 127 ms click to fan and 145 ms fan to projection; pane redraw 0.2-2.4 ms; the
  mousemove handler 0.9 ms median, 4.3 ms worst; the projection adds no long task while panning, zooming or playing the
  overlay. A: world index 45.5 ms in one go, `computeReach` 6.1 ms median (worst 25.7).
- **Suites.** pytest 456, Node 278 in 7 normal and 4 shifted-clock runs. Mutation (A): 142 of 203 killed.

## Reviewer B: test site and use

| # | Sev | Finding | Outcome |
|---|---|---|---|
| B-1 | P2 | Ring labels sit at a fixed bearing (335 degrees). At 12 of 15 spots no label is inside the window (Waikiki looking at the South Pacific: 1 of 15 labels on screen, cut off at the top edge). | **Fix round:** a ring's label goes on the middle of the widest run of rays that reach it (a second label on any other run 15 degrees or wider). |
| B-2 | P2 | Rings are missing or stop far short of the window for narrow windows and enclosed seas: the extent is the 90th percentile of all 720 rays and the smallest ring is 1,000 nm. Santa Barbara harbor (37 rays to 19,500 km), Lofoten, Nice, the Gulf of Mexico: no ring. Same as A-7. | **Fix round**, look per the owner's call below: rings out to where the window reaches, and a finer step (500 / 250 / 100 nm; 1,000 / 500 / 200 km) where fewer than two rings would fit. |
| B-3 | P2 | Phone: the hint says "Tap a wedge for details" while the fan is a compass, and a tap on the compass moves the spot (657 km in the test), with no undo. | **Fix round:** a touch tap inside the compass picks the wedge, as on the full fan; it no longer moves the spot. |
| B-4 | P2 | Inside a lighter small-island strip the readout says "Blocked by land ...: its swell cannot reach it", word for word what the full veil says. The picture says "partly", the text says "cannot". | **Fix round:** `probe` tells the strip apart (beyond the ray's own reach, within the bridged reach): "Partly blocked by a small island N from the spot", with the travel times. |
| B-5 | P2 | The readout line is `height: 4.5em; overflow: hidden`. At a browser text size of 18 px the longest readout loses its last line, at 24 px every readout does; at 16 px it fits with no line to spare. | **Fix round:** `min-height` without the clip (the bar may grow a line at large text sizes), and shorter sentences. |
| B-6 | P2 | Phone: the tool bar is 290 x 251-273 px on a 375 x 751 map and the spot is parked right under it; zoomed out, a north-facing spot's window is behind the bar. The bar cannot be folded. | **Fix round**, per the owner's call below. |
| B-7 | P3 | While the map is dragged, the veil, rays and rings end at the edge of the pre-drag canvas (Leaflet's renderer padding 0.1): a 450 px drag leaves 322 px bare until release. | **Fix round:** the pane gets its own canvas renderer with padding 0.5. |
| B-8 | P3 | A result that lands after the view moved to another world copy is drawn in the old copy (no fan on screen; two worlds away, nothing). Same as A-5. | **Fix round:** the fan's longitude takes the copy nearest the view when the result lands. |
| B-9 | P3 | Escape right after pressing Lock (focus is on the button) clears the spot. | **Fix round:** while locked, Escape in the bar only unlocks. |
| B-10 | P3 | Locked, a live-buoy window opened from the map covers the readout, and at 1280 x 800 the Unlock and Clear buttons. | **Accepted.** The window has its own minimise and close and can be dragged; raising the bar above it would hide those buttons. |
| B-11 | P3 | Lock is announced as "Unlock, pressed"; the bar's live region is rewritten on every mousemove; after Clear, focus drops to the page. Same as A-12. | **Fix round:** the label alone says the action (no `aria-pressed`), the bar is updated line by line and a hovered readout is not announced, focus goes to the bar's close button after Clear. The readout stays pointer-only. |
| B-12 | P3 | "Beyond the map's polar limit" is shown for points in the middle of the map whose great circle from the spot passes north of 84 N. | **Fix round:** "Not in the window: its path to the spot crosses the map's polar limit". |
| B-13 | P3 | Wording: "its swell cannot reach it" needs a second look; "In view, but in a shadowed direction" does not say that swell arrives; nothing explains the veil, the strips or the rings; the desktop hint does not say that a click moves the spot. | **Fix round:** "Blocked by land N from the spot: no swell from here"; "In the window (partly / mostly shadowed direction)"; one legend line that covers the map; the desktop hint names the click and Lock. |
| B-14 | P3 | An unlocked click anywhere still replaces the spot, with no way back. | **By design** (owner: Lock is the protection). |
| B-15 | P3 | A hover on the tool bar where a compass lies under it picks wedges; the compass sits under the buoy markers; the dotted yellow cursor line is hard to see over yellow wave heights; inside the compass a hover gives wedge text. | **Fix round:** controls are tested before the fan; a dark casing under the cursor line. The marker order and the compass hover are by design. |
| B-16 | P3 | The world tier leaves out small land: 9 of 4,320 rays pass where GSHHG high resolution blocks (Tuamotus, Marshalls, Bahama cays). Atoll REEFS are in no tier at all. | **Accepted, documented.** README note. On record for later: a reef and atoll mask, so the Tuamotu and Marshall shadows stop reading too open. |
| B-17 | P3 | First use: one long task at the 50 ms line; `worldEdges` is not sliced. Same as A-2. | **Fix round** (see A-2). |

## Reviewer A: code and geometry

| # | Sev | Finding | Outcome |
|---|---|---|---|
| A-1 | P2 | The suite cannot see the drawing. A mutant that never adds the projection to the map passes all 67 tool tests; 10 of 34 drawing mutants are killed. Survivors: holes not shifted to the fan's copy, strips never drawn, veil 0.55 -> 0.95, a ray on every ray, rays to 3,000 km whatever their reach. The strip assertion accepts 0 or 3 polygons. | **Fix round:** the fake map records what is on the map; assertions on the group, the holes' longitudes after a world shift, a synthetic small island that requires exactly three strips, the style constants, each ray's end point. Target: every survivor A lists as a real defect is killed. |
| A-2 | P2 | `worldEdges()` and the first 40,000-edge slice run in the task that draws the session's first fan: 31.5 ms median, 47.9 max on A's runs (66.7 max on the author's re-run), against 6.6 ms later; 108 ms under `node --jitless`. | **Fix round:** the world build starts in its own task and the edge pass is sliced too. |
| A-3 | P3 | A failed world-index build is never retried (`_worldP` is cleared only on success). | **Fix round.** |
| A-4 | P3 | An exception while drawing the projection is swallowed silently by the chain's `.catch`; one before the fan reads "Coastline data unavailable". | **Fix round:** `console.error`, and a line in the bar when the window could not be drawn. |
| A-5 | P3 | After a pan of more than half a world while computing, the fan stays in the click's world copy. | **Fix round** (see B-8). |
| A-6 | P3 | A range ring that passes behind a pole has a one-degree gap (61-110 km on the ground) where it is cut. | **Fix round:** the two pieces share the cut point. |
| A-7 | P3 | No rings at all in enclosed seas. | **Fix round** (see B-2). |
| A-8 | P3 | Radial edges are drawn with points 100 km apart: 0.2 km off the great circle at 45 degrees, 0.73 at 75 (3.6 and 37 px at zoom 11); in a half-degree gap near the spot the two sides can cross (1 of 105 named spots, 1 of 1,500 random). | **Fix round:** point spacing by distance (5 km near the spot to 100 km) and by latitude, shared by the ring, the rays and the cursor line. |
| A-9 | P3 | What the small-island rule lightens: 2,496 of 4,903 runs are islets and islands under 2,000 km2; 1,659 are short notches of a jagged coast; 307 are large islands seen from far (Hawaii's Big Island from California); Futuna at 306 km (3.0 degrees wide) stays dark. | **Owner's call** below. |
| A-10 | P3 | The straight 25 km step moves 0.01-0.2 % of rays at grazes (largest 1,948 km, an islet 3 m from the true ray). | **Accepted:** the hit is within 47 m of the true ray, far inside the data's ~1 km. |
| A-11 | P3 | A comment says "every 2.5 degrees" (it is 5); the README says the drawing is below the stations, but the ring labels are tooltips above them; the touch hint under a compass; the polar wording. | **Fix round:** comment; the labels move into the projection's own pane; B-3; B-12. |
| A-12 | P3 | Accessibility: the live region on every mousemove; "Unlock, pressed". | **Fix round** (see B-11). |
| A-13 | P3 | After Unlock on a touch screen the tapped readout and its line stay. | **Fix round:** Unlock clears a tapped readout. |
| A-14 | P3 | Other surviving mutants: the seam crossing's latitude (every seam test runs along the equator), the 19,500 km cap, the wrap at ray 0 in the small-island rule, the fan not redrawn on a compass switch, a resize with a compass, the thresholds, double-click zoom after Unlock, the touch paths. | **Fix round:** tests for each. |
| A-15 | P3 | The touch guard compares wall-clock times: a clock set back after a touch blocks a real mouse. | **Fix round:** `performance.now()`. |

## The author's check of the reports (2026-10-01, Fable 5.1 at MAX)

- A-1 re-run: `node mut_one.js 117` (the group never added to the map): 67 of 67 tests pass. Confirmed.
- A-2 re-run: `node first_task.js`: the first fan's task 34.3 ms median, 66.7 ms max; later ones 6.9 ms. Confirmed, and
  over the long-task line on this run.
- B-1 to B-6, B-8, B-9, B-12, B-15, A-3, A-5, A-6 and A-13 confirmed in the code (`tools.js` at f6509f2) and, for B-1,
  B-2 and B-6, in B's screenshots.
- The items A could not do without a browser are covered by B on the site: the actual pixels (7,440 samples), the drag
  (B-7), the real cost of the mousemove readout, resize and rotation, whether the longest readout fits (B-5).
- Not checked by anyone: a real phone (touch gestures, iOS Safari, a phone's CPU). Only the browser's phone preset and
  synthetic touch events exist here. A short list for the owner's own phone goes with the fix round.
- Two more items from the author, for the fix round: a hovered readout goes stale after a keyboard pan or zoom (recompute
  it at the remembered pointer position on `moveend`); the exposed constants for the rings change with the ring rework
  and need their own pins.

## Owner decisions for the fix round

Pending (asked 2026-10-01, with a sheet of ring options rendered by the real client on the test site): how the rings are
drawn (B-1, B-2), the tool bar on phones (B-6), and what counts as a small island (A-9).
