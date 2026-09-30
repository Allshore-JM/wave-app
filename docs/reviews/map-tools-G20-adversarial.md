# G20: map tools (plan section 29), adversarial review

Two fresh-context reviewers at MAX effort reviewed branch `feat/map-tools` @ 3c3bdb3 (UI asset 1.10.2) on 2026-09-27.
The branch is on the test site (wave-app-clean).

- **Reviewer A** covered code and geometry. It used a Node harness on the published GSHHG coast-v1 files (byte-identical
  to the local build), a fake-Leaflet UI harness, real Leaflet 1.9.4 in the Browser pane, and 83 mutants of `tools.js`.
- **Reviewer B** covered the test site, UX and geography. It worked on desktop 1280×800, the native 610×417 pane, a
  375×812 phone and an 812×375 landscape phone. It tested about 70 spots and edge cases worldwide through the UI's exact
  code path, and ran offshore transects.

**Totals: 1 P0, 3 P1, 16 P2, 28 P3** (A: 0 / 0 / 8 / 15; B: 1 / 3 / 8 / 13). Several findings overlap and are listed
once below.

Both reviewers confirmed the core maths:
- ray crossing, bucketing and the near/far switch;
- geodesy, the dateline and area;
- the golden invariant;
- XSS escaping;
- `gen` races;
- the marker hand-off;
- performance: dense coasts 9-38 ms, a cold worst case 152 ms, downloads 300-600 ms.

They also confirmed the owner's examples: the North Shore's south is dark and Kauai light; east-facing coasts are dark
to the west; the Gulf of Mexico, the Mediterranean, the Black Sea and the Persian Gulf are open.

## Findings and outcomes

| # | Sev | Finding | Outcome |
|---|---|---|---|
| B P0-1 | P0 | With a tool open, the tools menu and the gear's settings panel open UNDER the tool bar. The bar is a later Leaflet control at the same z-index 800, so tools cannot be switched and Units/Time zone cannot be changed. | **Fixed** @ 48c1732: the settings control stacks above the bar. |
| B P1-1 / A P2-3 / A P3-10 | P1 | On short maps (landscape phone, short windows, the 610×417 pane) the new fan is panned off the bottom. `keepFanVisible` only moves down, and `radius()` ignores the height. | **Fixed** @ 48c1732: obstacle-aware placement (the tool bar, corner controls, the minimised chips) that picks the nearest position with the whole fan inside the map. The radius comes from `min(width, height)`, shrinking when nothing fits. |
| B P1-2 | P1 | The overlay panel's Show/Hide-details button re-renders, so its click reaches the map. The distance tool gets a stray vertex, or the fan jumps. | **Fixed** @ 48c1732: ignore map clicks whose path contains a Leaflet control or a floating window, or whose target is detached. |
| B P1-3 / A P2-6 / A P2-5 | P1 | Near the shore the adaptive reference flips between ~200 km and 3,000 km within tens of metres. It is the 90th percentile of ALL rays, so an open-ocean spot whose window is under 36° drops to the floor. Snapped points sit 20 m off the shore, where micro-geometry blocks wedges. Evidence: Rincon 17-23 wedges, Bells 16-18, Noosa 2 vs 13 open. | **Fixed** @ 48c1732: an open-ocean spot (at least one wedge's worth of rays reaching the cap) keeps the full ocean scale. Otherwise use the percentile over rays that leave the spot's own coast. Evaluate at least ~150 m off the shoreline for clicks on land and on water. |
| A P2-1 | P2 | Area: a double-click on the first point closes the polygon, then the second click throws it away. | **Fixed** @ 48c1732: the second click of the closing double-click is ignored. |
| A P2-2 / B P2-2 | P2 | Escape (capture phase, stopPropagation) is swallowed page-wide while a tool is active. The gear panel, the windows and the station search never get it. | **Fixed** @ 48c1732: the tool takes Escape and Backspace only when focus is on the body, the map or the tool bar. |
| A P2-4 / B P3-9 | P2 | A land click near a 5° cell line is refused although water is close. The nearest "edge" is a clip border inside land (Gaviota 61 of 176). A point exactly on an edge is also refused. | **Fixed** @ 48c1732: the snap uses the cleaned edge list (next row), and a point on an edge steps along the normal. |
| A P3-1 | P3 | `onCellLine` also skips 87 real coast segments; rays leak (Greenland 947 km instead of 0.56). | **Fixed** @ 48c1732: on-line edges cancel by net directed coverage per line. Clip borders, bridges and the ±180 split cancel; real coast survives. |
| A P2-7 | P2 | The Pensacola regression test passes with the fix removed (the bridge is beyond the near phase). | **Fixed** @ 48c1732: a real zero-width bridge inside 50 km. The test fails without the fix. |
| A P2-8 | P2 | 54 of 83 mutants survive: no `init()` coverage, and core geometry mutants change 22-34 wedges at real spots. | **Fixed** @ 48c1732: a fake-Leaflet UI suite and a real-geometry fixture (a Hawaii crop of the published coast data) with pinned wedges; boundary tests. |
| B P2-1 | P2 | An open live-buoy window (z 2000) covers the tool bar's details and Clear. | **Fixed** @ 48c1732: starting a tool minimises an expanded window that overlaps the bar. |
| B P2-3 | P2 | Leaflet's `resize` never reaches the tool (`enforceSingleWorld` sets `_sizeChanged` first), so after rotation the fan keeps its old size. | **Fixed** @ 48c1732: listen to the window resize / orientation change and a ResizeObserver on the map. |
| B P2-4 | P2 | Placement ignores the bar's later growth and other controls (the overlay panel, the chips). | **Fixed** @ 48c1732: covered by the obstacle-aware placement, plus a stable bar height (the wedge line keeps a fixed height). |
| B P2-5 | P2 | Over ocean imagery "open" and "dark" wedges look alike (1.26:1); 72 white spokes dominate. | **Fixed** @ 48c1732 (owner pick): open windows get a bright rim, the greys are lighter, and separators only where the shading changes. |
| B P2-6 / P2-7 | P2 | The caveat mentions only islands, so point breaks look blocked. Reefs are not in the data (Kaneohe, Cairns). | **Fixed** @ 48c1732: the caveat names headlands and reefs; README. |
| B P2-8 / A P3-6 | P2 | Small seas show no open window (Marmara: the 200 km floor). Lakes are land in the data, so the message is wrong. | **Fixed** @ 48c1732: a lower reference floor, and the refusal says lakes aren't covered. |
| B P3-1 | P3 | Land ~1,000 km away shades light at open-ocean spots (Hatteras NNE). | **Fixed** @ 48c1732 (owner pick): land beyond ~1,000 km never shades. |
| B P3-2 | P3 | Tiny distant islands make light wedges (Farallones, Berlengas). | Accepted: geometrically real (the Farallones do shadow Ocean Beach slightly). |
| B P3-3 | P3 | Fragmented window lists in island groups (8 windows at Taveuni). | **Fixed** @ 48c1732: list the widest windows and add "+N more". |
| B P3-4 / A P3-9 | P3 | Hover lags (the canvas throttles mousemove) and rebuilds all 72 paths plus a layout per wedge change. | **Fixed** @ 48c1732: an un-throttled container mousemove; restyle the one selected path; layout only when the bar's height changes. |
| B P3-5 / A P3-15 | P3 | Focus is lost after picking a tool, and the bar has no accessible name. | **Fixed** @ 48c1732: focus moves to the bar, which is labelled by its title. |
| B P3-6 | P3 | Touch: area closes only within 10 px; texts say "Click"; no hint about moving the point. | **Fixed** @ 48c1732: 22 px on touch, "Tap" wording, and "tap outside the fan to move it". |
| B P3-7 | P3 | A double-click on a live-buoy icon does not finish a measurement. | Accepted: the Finish button, or a double-click on the map. |
| B P3-8 | P3 | Clicks near a marker land on the marker's position (Leaflet behaviour for markers). | Accepted (documented). |
| B P3-10 | P3 | The legend lacks "clear = open" and "from"; the odd "1,864 mi". | **Fixed** @ 48c1732: wording, compass names in the windows, "open ocean for 1,800+ mi". |
| B P3-11 / A P3-7 | P3 | Self-intersecting polygons; a polygon around a pole returns the complement; antipodal points zigzag. | **Fixed** @ 48c1732: a note for crossing outlines, the smaller of the two areas, no densify for antipodes. |
| B P3-12 | P3 | A barrier-island click can snap to the sound side. | **Fixed** @ 48c1732: the hint says how far the point moved (the fan shows where). |
| B P3-13 | P3 | At surf zooms the fan hides the break. | **Fixed** @ 48c1732: a ring (inner radius ~28 px) keeps the lineup visible. |
| A P3-2 | P3 | Tier seam at 50 km: 5 of 28,080 rays stop falsely. | Accepted (documented). |
| A P3-3 | P3 | The far window is too narrow for poleward rays (1-9 rays, no level changes). | **Fixed** @ 48c1732: the exact maximum longitude reach. |
| A P3-4 | P3 | The fan jumps one world when a snap crosses the antimeridian. | **Fixed** @ 48c1732: the wrapped offset. |
| A P3-5 | P3 | `CoastSource.near`: no in-flight dedupe; the cache is FIFO. | **Fixed** @ 48c1732: a promise per cell; true LRU. |
| A P3-8 | P3 | Unit-format boundaries ("10.00 mi", "640 acres", "100.0 ha"). | **Fixed** @ 48c1732: round first, then choose unit and precision. |
| A P3-11 / P3-12 / P3-13 / P3-14 | P3 | Stale header comment and dead `blockedRays`; the decoder allocates before validating; a test SyntaxWarning, and the golden tests don't clear `COAST_BASE`; the tool's dependence on the frames path is undocumented. | **Fixed** @ 48c1732: all four. |

## Owner decisions for the fix round (2026-09-27)
- **Stand-off:** 150 m. Clicks on land, or within 150 m of the shore, are evaluated 150 m out. Land clicks move up to
  2 km, water clicks up to 300 m.
- **Far land:** land beyond 1,000 km never shades an open-ocean spot.
- **Fan look:** open wedges stay clear with a bright cyan rim; the greys are lighter; separators appear only where the
  shading changes; the centre is a clear ring.
- **Windows:** starting a tool minimises an expanded window that covers the tool bar.

## Verification of the fix round
The fixes are in UI asset 1.11.0 @ 48c1732, with the golden @ 11a6165.
- **Tests:** 456 pytest; 238 Node tests, including `tests/ui/tools.test.js` (19) and the new `tests/ui/tools-ui.test.js`
  (8, fake Leaflet).
- **Targeted mutants:** 17 of 18 are killed. The survivor removes the decoder's pre-allocation check, and the decoder
  still throws on the same input later, so the mutant is behaviourally equivalent.
- **Real data (published coast files):**
  - Rincon gives the same result from a beach click, 20 m out and a point exactly on the edge: "Open: SSE
    (140°–165°), W (250°–275°)".
  - Bells Beach at 30 m and at 300 m agree.
  - Noosa moves 541 m to open water: "Open: NE (010°–065°)".
  - The Gaviota land click now snaps.
  - The Sea of Marmara has open windows (reference 75 km).
  - Cape Hatteras's NNE is no longer shaded by New England.
  - The Greenland on-line coast stops rays again.
  - Pipeline, Waikiki and Haleiwa are unchanged in character: the North Shore's south is dark and Kauai is light.
  - 5-47 ms per spot.

## Re-review of the fix round (UI 1.11.0 @ fbaeeb5, 2026-09-27, two fresh reviewers at MAX)
- **R1 (code):** 0 P0, 0 P1, 5 P2, 6 P3. 76 of 110 mutants killed. It confirmed:
  - the P0 and the click-leak fix;
  - the cell-line cancellation: all 83 real on-line segments survive, and the Greenland leak is closed (0.556 km);
  - no false refusals near cell lines;
  - Rincon stable from 20 m to 300 m out.
- **R2 (test site and geography, 93 spots, in the page and in Node byte-for-byte):** 0 P0, 0 P1, 7 P2, 5 P3. 19 G20 rows
  are confirmed fixed and 4 partially fixed. Wedge changes between clicks at 30 m and 300 m fell from 4.86 to 1.63 on
  average. The owner's examples hold.

| # | Sev | New finding | Outcome |
|---|---|---|---|
| R2 P2-A / R1 P3-5 | P2 | Small desktop windows (the 610×417 pane with the overlay details shown): nothing fits, so a 50-px fan sits under the bar or the panel. | fixed: the overlay details fold on maps under 550 px; least-overlap fallback, never centred under the bar |
| R2 P2-B / R1 P2-1 | P2 | On a landscape phone the taller bar overflows the map: Clear and the caveat end up under the forecast bar (regression). | fixed: the bar is capped at the map's bottom edge, actions under the title, the text scrolls |
| R2 P2-C / R1 P2-3 | P2 | A window that overlaps only the GROWN bar (the default forecast box at 1280×800) is not minimised. | fixed: the page minimises an overlapped window whenever the bar grows |
| R2 P2-D / R1 P2-2 | P2 | A resize or rotation pans the map back to a fan the user has panned away from (6,515 px in one case). | fixed: only a fan still on screen is placed again; otherwise it is only resized |
| R2 P2-E | P2 | "Land beyond 1,000 km never shades" was built as a compressed curve. A fully blocked wedge reads open from ~430 km, and land at 50-70 km drops from dark to light (193 wedges at 43 of 87 spots against a literal cut). | fixed (owner): the earlier curve (zero at F_ref), faded out linearly from 600 to 1,000 km |
| R2 P2-F | P2 | The 50 km reference floor makes small bays and sounds read open (SF Bay, Long Island Sound, Pamlico Sound from a Rodanthe click). | fixed: floor 100 km |
| R2 P2-G | P2 | Naming wide windows by their centre misleads ("N (185°–180°)", Hatteras "ESE (015°–230°)"). | fixed: both ends from 45°, "Open except …" from 300°, repeated names shared |
| R1 P2-4 | P2 | `placeOrigin` can evaluate a land click exactly on the coastline, giving an all-dark fan with a "moved to open water" hint (4% of land clicks on Honolua's east headland). | fixed: a point within 5 m of a coast is never used |
| R1 P2-5 | P2 | Touch: a double-tap on the first point still discards the outline (the 4 px dedupe against the 22 px close); a double-tap to finish a distance adds a point. | fixed: 16 px on touch for both guards |
| R2 P3-A | P3 | A press on a control that is released over the map counts as a map click. | fixed: a capture pointerdown remembers a press on a control or window |
| R2 P3-B | P3 | Over the wave-height overlay the rim and the light wedges have little contrast; the rim colour is in the palette. | fixed: a 7 px #0b2536 underlay beneath the 4 px rim |
| R2 P3-C | P3 | The desktop bar still shifts 5 px while hovering. | fixed: 3em (two lines) |
| R2 P3-D / R1 P3-1 | P3 | Escape is ignored with focus on the gear or tools button, or after a live-marker click. | fixed: the tools button, the gear (panel closed) and markers own the keys; the opened menu takes focus |
| R2 P3-E | P3 | Pans can be very long on mid-size windows. | fixed: a spot within a third of the map is preferred to a larger fan farther away |
| R1 P3-2 | P3 | The reference flips at 9 vs 10 rays reaching open ocean. | fixed: a geometric blend from 5 to 15 capped rays |
| R1 P3-3 | P3 | A tier-1 chunk that loads but fails to decode is never retried. | fixed: the in-flight entry is dropped on a failed fetch OR decode |
| R1 P3-4 | P3 | A water click in a channel under ~40 m wide can walk through a thin spit. | fixed: a water click never crosses a coast; a land click stops at the far shore once it has its water |
| R1 P3-6 / mutants | P3 | Test titles overclaim, and 24 meaningful mutants survive (the leaving-ray percentile, one-direction on-line coast, placement nearest/shrink/windows, hover restyle, resize re-placement, the antimeridian fan, chunk retry, the rim over 180°, cos(lat) in the stand-off). | fixed: 23 of the 24 re-targeted survivors killed plus 27 of 28 new mutants (P9 kept as a guard, see below) |

## Fix round 2 (UI 1.11.1, 2026-09-29)

Owner picks: far land = the earlier curve faded out between 600 and 1,000 km; short maps = fold the overlay panel's
details when the exposure tool starts, then the least-overlap placement, never under the tool bar.

- **Shading:** `rayShadow` is back to zero at F_ref, multiplied by a linear fade from 600 km (1) to 1,000 km (0). Land
  at 432 km in a fully blocked wedge reads light again (0.37); Kauai from the North Shore is 0.57 (light).
- **Reference:** floor 100 km (the 90 km test bay now reads "No open swell window"; a 220 km Marmara-sized sea stays
  open all round); the full reach from 15 capped rays, a geometric blend from the spot's own percentile from 5 rays
  (each extra ray ×(3000/own)^0.1).
- **Window names:** centre for windows under 45°, both ends above ("NNE–SW (015°–230°)"), "Open except S
  (180°–185°)" when the open windows cover 300° or more, neighbours with one name share it ("SSW (195°–200°,
  205°–215°)"). Pipeline now reads "Open: W (250°–275°), WNW–NE (295°–045°)".
- **Where a click is evaluated:** no point within 5 m of a coast (a synthetic two-cove grid: 444 of 7,130 clicks were
  evaluated on the coastline before, 0 now); a water click never crosses a coast (the channel-and-spit case stays in its
  channel); a land click walks on through a cove too narrow to use and stops at the far shore once it has its water.
  Real data (R1's snapscan, 13 areas): false refusals 0 before and after; 4 more legitimate refusals in 2,062 land
  clicks; placement time unchanged (max 7.3 ms).
- **Placement:** `placeFan` prefers a spot within `maxPan` (a third of the map) at a smaller radius to a larger fan
  farther away, then searches without the limit, then `leastOverlap` (disc samples: outside the map 1, an obstacle 1,
  the tool bar 10; never centred on the bar). A resize re-places only a fan whose centre is on the map.
- **Bar:** actions under the title, the body scrolls, `max-height` = the map's bottom − the bar's top − 8 px; the
  actions row hides when empty; `onLayout(bar, grew)` lets the page minimise a window whenever the bar grows;
  `onStart(bar, tool)` folds the overlay panel's details for the exposure tool on maps under 550 px.
- **Interaction:** touch double-tap tolerance 16 px (mouse 4); a capture `pointerdown` on the document remembers a press
  on a control or window, and the map click that follows is ignored; the keys are also owned with focus on the tools
  button, the gear (panel closed) or a marker; the opened tools menu focuses its first item.
- **Look:** a 7 px `#0b2536` underlay beneath the 4 px cyan rim; the wedge line is 3em (two lines, no 5 px jitter).
- **Coast source:** a chunk that fails to decode leaves the in-flight map and is fetched again next time.
- **Tests:** `tests/ui/tools.test.js` 20, `tests/ui/tools-ui.test.js` 16 (shared encoder `tests/ui/coastenc.js`;
  `fakedom` matches `tag[attr="v"]`). Mutation run (52 mutants: R1's 24 survivors re-targeted + 28 for the new logic):
  51 killed. Survivor P9 (least overlap without the "never centred on the bar" exclusion) is equivalent in every
  geometry tried, because the bar's weight of 10 already keeps the centre off it; the exclusion stays as a guarantee.
- **Also:** three overlay scheduler tests in `tests/overlay/playback.test.js` started failing on 2026-09-28 on the
  unchanged HEAD (they seek 30 frames past "now" in a fixed 2026-09-22 run); they now run on that run's day with a
  clock that still advances.

## Re-check of fix round 2 (UI 1.11.1 @ 9883f51, 2026-09-29, MAX)

Two halves: R3, a fresh-context code reviewer (read-only, Node/Python, the published coast; report and scripts in the
session scratchpad `g20/r3/`), and the author on the test site (browser pane, DOM-dispatched events, 1280×800, 812×375,
610×417, 683×657, 375×812 and the pane's native 457×309; `tools.js?v=1.11.1` byte-identical to eeab4ce; no console
errors). Suites at 9883f51: pytest 456, Node 247.

**Confirmed on both sides:** the far-land rule is exactly the earlier curve below 600 km (all 166 wedges that changed at
the 93 spots moved towards more shading; every change is explained, the evaluated point is identical at all 87 evaluated
spots, and the owner's examples hold); window names ("Open except", shared names; 0 contradictions in 20,000 random fans);
the blend (0 wedge changes at the 93 spots); the spit rule; touch 16 px (a real double-tap on the phone kept the outline);
the rim underlay; the 3em line (one bar height over all 72 wedges); the bar cap (812×375: Clear reachable, the text
scrolls); the chunk retry; the keys (gear, tools button, a live marker); the press released over the map; the overlay
details folding (610×417: 0 of 120 disc samples covered in six clicks, the earlier review found 60-91 of 108); the pan
limit (683×657: pans 0-197 px; one 361 px pan remains where nothing fits within a third of the map); the golden (the
intended template change and four version bumps only).

| # | Sev | Finding | Outcome |
|---|---|---|---|
| R3 P2-A | P2 | Land clicks near narrow water walk on through land: the 5 m rule skips near-shore samples and the walk continues up to 2 km. Haleiwa (21.5931, −158.1065) moves 1,469 m into an embayment ("No open swell window"; 1.11.0: 429 m, W and NW open); Hilo (19.7273, −155.0629) 1,250 m; Honolua's headland (21.0175, −156.6400) refused with usable water 140 m away. 8 broken and 4 falsely refused in 41,338 land clicks at 83 spots. | fixed (round 3) |
| R3 P2-B | P2 | A resize judges "on screen" after the resize: Leaflet keeps the centre, so a rotation that pushes a visible fan off the map no longer re-places it (375×764 → 812×327: a 51 px sliver). | fixed (round 3) |
| R3 P2-C | P2 | Six more tests in `tests/overlay/playback.test.js` step from "now" through the fixed run and fail from 2026-10-02 (checked to 2027-09 with a shifted clock; nothing else in the Node suites is date-bound). | fixed (round 3) |
| R3 P2-D | P2 | The overlay's phone sheet (`div.ov-sheet`, not a `.leaflet-control`) is not an obstacle, not UI for the press rule, and now owns the keys; on a 375×812 phone a low click leaves ~24 px of the fan under it. | fixed (round 3) |
| Site P2 / R3 P3-2 | P2 | Every exposure result reports `grew` (the bar shrinks to "Computing…" first), so a window the user expands while the tool is open is minimised again at the next click, and the forecast window's minimise moves focus out of the map. | fixed (round 3) |
| Site P1 / R3 P3-7 | P1 (owner decision) | Cape Hatteras's NNE: the chosen option promised it "still opens"; 015°–025° read light (New England at 621-716 km: 0.27, 0.22). A fade from 500 km opens 020° only, from 450 km opens 015° at exactly 0.20; "land beyond 600 km reads open, fading to nothing at 1,000 km" opens it (0.16, 0.14) and differs from the shipped rule in 10 wedges at 7 of the 93 spots. | owner: "Open beyond 600 km"; fixed (round 3) |
| R3 P3-1 | P3 | A water click in very narrow water is kept at the click, whatever its clearance (Honolua cove 0.3 m); the docs say "never". | fixed (round 3) |
| R3 P3-3 | P3 | A name can repeat across north ("N …, E …, N …"); "Open except" gaps are not in bearing order. | fixed (round 3) |
| R3 P3-4 | P3 | The floor of 100 km leaves Long Island Sound and Pamlico Sound (Rodanthe) open (their reference is 101-104 km); the record said "fixed". | record corrected here: partial by design |
| R3 P3-5 | P3 | `placeFan` takes 116 ms at 3840×2160 when nothing fits (`leastOverlap` alone 83 ms). | fixed (round 3) |
| R3 P3-6 | P3 | 14 meaningful mutants survive (the unlimited second pass, `leastOverlap`'s weights and tie-break, the 45°/300° edges, "except" with 3+ windows, "+N more" with shared names, the underlay order, the 120 px minimum, vertical visibility on resize); two test titles overclaim; the record's "51 of 52" is not reproducible with an independent set. | fixed (round 3), survivors listed below |
| Site P3 | P3 | The bar's 120 px minimum exceeds the room on a very short map (457×309: 6 px past the map). | fixed (round 3) |

## Fix round 3 (UI 1.11.2 @ c289526, 2026-09-29, HIGH)

Owner decision (re-check): **land beyond 600 km reads open**. `rayShadow` keeps the earlier curve to 600 km; beyond it a
ray counts at most `FAR_OPEN_MAX` 0.18 (under the open line) and fades to 0 at 1,000 km. Cape Hatteras reads open from
015° again (its 015°/020° wedges 0.16/0.14).

- **Where a click is evaluated (R3 P2-A, P3-1).** `placeOrigin` rewritten:
  - a land click walks to the nearest coast edge and, once through it, turns along that edge's normal on the side it
    crossed into, straight out to sea; a water click within the stand-off walks away from the nearest edge (at most
    twice the stand-off); a click exactly on the coastline starts on the water side;
  - unless that walk reached a point 150 m out that looks out (3 of 16 directions run 2 km without meeting a coast:
    a bay beats a pond behind the shore, as at Hilo), 24 directions (15°) are searched: the nearest such point, else the
    nearest point 150 m out, else the clearest water; no walk crosses a coast beyond its own water;
  - every candidate is checked against the coast data before it counts; a land click never settles within 5 m of a
    coast, and with no such water within 2 km it is refused.
  Real data (R3's scans on the published coast, 1.11.0 → 1.11.2):
  - refusals: dense grids 35 → 0, 83-spot grids 2 → 0, R1's areas 1,057 → 1,055 (one legitimate new refusal at Chiba,
    no water within reach); evaluations within 5 m of a coast: 194 → 0, 5 → 0, 4 → 0; water clicks evaluated across a
    coast: 0; the V-cove water clicks: 0 of 7,500 kept on a flank;
  - results "a window → no window": 8 in 41,338 land clicks at the 83 spots (2 Bora Bora: the lagoon, sheltered by its
    reef; 3 Honolulu Harbor 5° slivers; 3 Taveuni), against 19-24 in the intermediate versions; worldwide (3,000 random
    coastal clicks) 62 fixed, 3 broken, 0 newly refused;
  - Haleiwa (21.5931, −158.1065) 640 m, "W (265°–275°), WNW–N (300°–000°)" (1.11.1: 1,469 m, none); Hilo 420 m, "N–ENE"
    (1.11.1: 1,250 m, none); Honolua's headland 480 m, "W, N" (1.11.1: refused); the Stockholm skerries all placed;
  - time: median 12 ms, p99 31 ms, max 44 ms over 2,000 clicks in the densest areas (alone on the machine).
- **Resize (R3 P2-B).** Whether the fan was on the map is judged at its pre-resize point (Leaflet keeps the centre: the
  current point shifted back by half the size change, against the previous size).
- **Date-bound tests (R3 P2-C).** The six more run-A tests run on the run's day; the Node suites pass with the clock at
  2026-10-05 and 2027-09-01.
- **The overlay's phone sheet (R3 P2-D).** `.ov-sheet` is an obstacle, UI for the press rule, and keeps its keys.
- **Window re-minimising (site P2 / R3 P3-2).** `grew` only above the tallest bar height since the tool started; the
  page's minimise keeps keyboard focus where it was.
- **Names (R3 P3-3).** The first and last windows share a name across north; "Open except" gaps in bearing order.
- **Placement cost (R3 P3-5).** `leastOverlap` scans at most ~5,000 positions (the step grows with the map).
- **Bar (site P3).** Minimum 60 px instead of 120.
- **Tests (R3 P3-6).** tools.test.js 20, tools-ui.test.js 20: the far-land rule and Hatteras; names across north, gap
  order, the 45° and 300° edges, 3+ gaps, "+N more" with shared names; the underlay order; the second placement pass,
  `leastOverlap`'s weights and tie-break and a 4K map; water clicks in the coves; a corner turn; a pond behind the shore;
  the Haleiwa land click pinned on the Hawaii fixture; the bar's height rule; the phone sheet; rotation. Suites: pytest
  456, Node 251. Mutation (33 mutants of the round-3 logic): 26 killed. Survivors: the turn and its side (G1, G2: the
  direction search reaches the same answers in the tests), the probe's coast window (G6b), land never settling within
  5 m (G7: a better point always exists in the tests), vertical-only resize shift (F2), the bar-centre guard (C5, kept
  as a guarantee), and the `leastOverlap` step (C4, performance only).

## Re-check of fix round 3 (UI 1.11.2 @ f31509f, 2026-09-29, MAX)

Two halves: R4, a fresh-context code reviewer (read-only, Node/Python, the published coast; report and scripts in the
session scratchpad `g20/r4/`), and the author on the test site. Suites: pytest 456, Node 251 (also with the clock at
2026-10-05, 2027-06-01 and 2027-12-31). About 176,000 placement clicks, including dateline and 74-75° grids: 0 false or
new refusals against 1.11.1, 0 evaluated points on land, 0 land clicks within 5 m of a coast, 0 water clicks moved more
than 300 m or across a coast. The 93 spots, 1.11.1 → 1.11.2: 31 wedges at 14 spots, all explained (21 by the new
evaluation point at 7 spots, 10 by the far-land rule at 7 spots, every changed ray at 604-756 km); the owner's examples
hold. Confirmed: far land (Cape Hatteras open from 015°), resize, date-bound tests, the phone sheet, `grew`, names,
`placeFan` cost, bar minimum, golden, V-cove water clicks.

**Test site (author).**
Test site (UI 1.11.2, bytes = c289526), browser pane. The pane was hidden during this check, so the browser paused
animation frames and did not fire window resize events by itself: resize events were dispatched by hand, the tool's
pans were read from a `panBy` log (an animated pan does not play while hidden), and the overlay's phone sheet was built
by presenting the page as visible to the overlay's code. No console errors.

| Item | Verdict | Evidence |
|---|---|---|
| Rotation (R3 P2-B) | confirmed | 375×812, fan at (190, 600); rotate to 812×375: Leaflet keeps the centre (its own pan −219, 215) and pushes the fan to (409, 385), below the 322 px map; 177 ms later the tool pans (7, 183): target (402, 202), the whole disc inside the map |
| Phone sheet (R3 P2-D) | confirmed | 812×375 with the real `.ov-sheet` (50-812 × 287-322): a fan clicked low is placed with its disc bottom at 284 (sheet top 287), clear of the bar; a press on the sheet's toggle released over the map adds no point; Escape with focus on the toggle leaves the measurement |
| Window re-minimising (site P2 / R3 P3-2) | confirmed | 1280×800: the expanded forecast window is minimised at the first result (the bar grows to 265 px) and focus stays on the tool's ✕; re-opened by the user, it stays open through two more results (265 px, 242 px) |
| Bar minimum (site P3) | confirmed | 457×309 (map 248): bar 134-240, max-height 106 px, Clear hit-tests to itself |
| Far land / Hatteras (owner) | confirmed | "Open: NNE–SW (015°–230°)" |
| Land clicks (R3 P2-A) | confirmed | Haleiwa "W, WNW–N" (0.4 mi), Honolua headland "W, N" (0.3 mi), Pipeline unchanged |
| "Open except" order | confirmed | west of Kauai: "Open except ENE (070°–085°), ESE (090°–115°)" |

| # | Sev | Finding | Outcome |
|---|---|---|---|
| R4 P2-1 | P2 | Placement scans every edge of a 10×10 km window for every 20 m step of up to 25 walks: in dense estuaries it takes 100-230 ms, in the click's own task, before "Computing…" can paint. Kennebec mouth, Maine (43.818, −69.785; 5,287 edges): median 63-106 ms, max 205-233 ms (1.11.1: max 22 ms); with the rays up to 364 ms on a laptop, an estimated 0.7-0.9 s frozen on a phone. The record's "max 44 ms" held only for windows under ~1,400 edges. | fixed (round 4) |
| R4 P2-2 | P2 | A land click whose nearest coast point is a cove-apex vertex: the first walk only grazes the tip, counts no crossing and runs on through land (Honolua Bay's head, 21.0169, −156.64062: evaluated 1,681 m away in the next bay, the search would give 460 m). 7 of 41,338 spot-grid land clicks, 55 in the Honolua dense grid. | fixed (round 4) |
| R4 P3-1 | P3 | The look-out test (3 of 16 directions free for 2 km) and the clearest-water fallback sometimes pick worse water: a Hilo land click (19.7270, −155.0692) goes into a pond ("No open swell window"; 1.11.1: the bay, "N–ENE"); NW Scotland; Honolulu Harbor water clicks moved into the basin; Hvaler sounds. Far more gains than losses overall. The record's "8 in 41,338" is right for R3's spot grid (checked against both 1.11.0 and 1.11.1); R4's own grids give 24-25 land and 71 water clicks losing a window against 1.11.1 (2,348 gained). | fixed (round 4, measured) |
| R4 P3-2 | P3 | A tool switch at the same bar height skips the start-minimise and the next shrink reports growth; the template's focus restore skips focus on the page body. | fixed (round 4) |
| R4 P3-3 | P3 | The placement rewrite is mostly unpinned: 20 of 32 placement mutants survive (the turn disabled, its side, parity, the on-coastline rule, the look-out threshold, the probe window, land within 5 m), plus the resize's y shift. | fixed (round 4), survivors listed |
| R4 P3-4 | P3 | README and a code comment describe the search and its fallback more narrowly than the code behaves. | fixed (round 4) |
| R4 note | P3 | `playback.test.js` "coast: a download that hangs past the watchdog" (40 ms watchdog) fails under load: its first "still loading" check can run after the watchdog. | fixed (round 4) |

## Fix round 4 (UI 1.11.3 @ c14bab0, 2026-09-29, HIGH)

- **Placement speed (R4 P2-1).** The coast edges within reach go into 100 m buckets; a 20 m step, a look-out probe
  (in 100 m pieces) and a nearest-edge search (growing boxes until the best edge is inside) test only nearby edges.
  Kennebec mouth (43.818, −69.785; 5,287 edges), 400 clicks: median 4 ms, p99 11 ms, max 14 ms (1.11.2: max 205 ms);
  2,000 clicks in the densest areas: median 3.6 ms, p99 20 ms, max 26 ms. `exposureAt` yields once (a macrotask)
  before placement, so "Computing…" paints first.
- **The cove-tip graze (R4 P2-2).** A land click's first walk gives up two steps past its nearest coast point when it
  is still on land; after any crossing the coast data (not the crossing count) says which side the walk is on, except
  on the coastline itself; a step owns [a, b), so a crossing that falls exactly on a sample point is counted once
  (a synthetic channel whose samples landed on its banks had walked on through the next strip of land). Honolua Bay's
  head (21.0169, −156.64062): 460 m, "W (260°–275°), N (340°–005°)" (1.11.2: 1,681 m over the headland), pinned on a
  Maui crop of the published coast (`tests/fixtures/coast/maui-t1.bin`, identical placements to the full chunk).
- **Which water wins (R4 P3-1), measured.** Six rules were run over 45,166 clicks (R3's spot and dense grids, R4's
  Hilo/Honolulu grid, 3,000 random coastal clicks), windows lost / gained against 1.11.1:

  | Rule | Land lost / gained | Water lost / gained |
  |---|---|---|
  | 1.11.2 | 78 / 2,704 | 102 / 150 |
  | round 4, ranking as 1.11.2 | 93 / 2,717 | 104 / 151 |
  | + relative look-out (0.6 × the best) | 56 / 3,101 | 104 / 151 |
  | + fallback: looks out first, then the nearest ≥ 80 % as clear | 56 / 3,100 | 49 / 162 |
  | same without the turn out to sea | 69 / 3,128 | 49 / 162 |
  | **looks out well (6 of 16) first, else looks out (3 of 16)** | **32 / 3,601** | **49 / 162** |

  The relative rule moved the Honolua head click 1,080 m past its bay (the bay looked out 6 of 16, water beyond it 10);
  "looks out well" keeps it in the bay and keeps Hilo's clicks on the bay rather than the pond. A 300 m cap on how much
  further a well-looking point may be cost ~200 gains in the dense grids and was dropped. Chosen: the nearest point
  150 m out that looks out well (6 of 16 directions free for 2 km), else the nearest that looks out (3 of 16), else the
  nearest 150 m out; the search is skipped when the first walk's point already looks out 8 of 16; the fallback takes the
  nearest water that looks out (5 m clear at least), else the nearest at least 80 % as clear as the clearest. A point on
  the coastline is never used (a click exactly on a coast vertex had been kept with 0 m of clearance).
- **Placement safety (the final code, R3's scanner).** Refusals: dense grids 35 → 0, spot grids 2 → 0, R1's areas
  1,057 → 1,055 (one legitimate new refusal at Chiba); evaluations within 5 m of a coast: 0 everywhere (1.11.0: 194 in the
  dense grids); water clicks across a coast: 0, except 136 in R4's dateline grids, where this scanner does not wrap
  longitudes (1.11.0: 137; R4's wrap-aware scanner found 0); worldwide sample 73 fixed, 2 broken, 0 newly refused.
- **Also:** a tool switch always reports the bar (`s.barH = -1`); a page minimise releases focus when it was on the page
  itself; README and comments describe the search as it is; the overlay watchdog test runs with 150 ms (c96a8cd).
- **Tests.** tools.test.js 22, tools-ui.test.js 22: an island field behind a coast (16,200 edges; 1.11.2 took 165-196 ms
  a click, fails the 80 ms limit), the Honolua Bay pin (fails on 1.11.2: 1.68 km), a long lake that looks out only east
  and west (fails with a look-out minimum of 1), a sharp-V refusal, rotation judged by height, tool switching; the
  Haleiwa example's pinned levels now end open (the point is 100 m out instead of 80 m; Kauai still light). pytest 456,
  Node 255 (also with the clock at 2026-10-05 and 2027-09-01). Mutation (22 of the round-4 logic): 10 killed; the
  survivors are layers with a second guard (the coastline click is both excluded and held to 5 m; the first walk's give-up
  and the search both stop the graze; the side from the coast data and the half-open steps), the paint yield (browser
  only), the turn and its side (the search reaches the same answers in the tests; the measurement shows the turn helps),
  the nearest-edge box test, and the fallback's order.

## Re-check of fix round 4 (UI 1.11.3 @ e4da450, 2026-09-29, MAX)

Two halves: R5, a fresh-context code reviewer (read-only, Node/Python, the published coast; report and scripts in the
session scratchpad `g20/r5/`), and the author on the test site. Suites: pytest 456, Node 255 (also with the clock at
2026-10-05 and 2027-09-01). **No P0, P1 or P2; 7 P3.**

**R5 (code).**
- **Bucket index: exact.** `nearest()` against brute force: 0 wrong distances in 1,739,340 calls. `cuts()`: 15 of
  34.3 M calls differ, all axis-aligned steps touching a vertex at exactly the click's latitude or longitude (lattice
  grids only). With placements, those touches and ties at shared vertices move 31 of 30,968 points and change no window.
- **Speed.** Kennebec mouth, 441 clicks: median 4.3 ms, p99 11.5 ms, max 17 ms. The 22 densest places, 9,702 clicks:
  max 33 ms. The worst whole path is about 100 ms on a laptop.
- **The ranking table reproduces exactly** on the four grid files, which hold 45,166 clicks. Against 1.11.2: land lost
  66 / gained 1,009, water 16 / 81. Seed 11 (not in the author's set): 9 / 61 against 1.11.1.
- **Safety, 69,302 clicks** (dateline, 74-75°, R1's areas, dense grids, two random seeds, a quarter of the spot
  grids): 0 false or new refusals, 0 points on land, 0 land clicks within 5 m, 0 water clicks moved more than 300 m or
  across a coast. Multi-edge crossings belong only to R4's known classes: lattice vertex touches, slivers under 20 m,
  and one graze 1.11.2 shared.
- **93 spots, 1.11.2 → 1.11.3:** 15 wedges at 4 spots (Haleiwa, Noosa, Honolulu Harbor, Bora Bora), every one from a
  changed evaluation point; 0 unexplained. The owner's examples hold.
- **Also confirmed.**
  - Ten interventions during the new pause never leave a stale fan.
  - Start/switch/stop orders report growth correctly.
  - The golden differs only by the version and the three template lines.
  - `maui-t1.bin` is an exact crop: identical placements over 6,561 clicks.
  - The 150 ms watchdog passed 13 of 13 runs under load.
  - The new tests fail on 1.11.2 where claimed.
  - Mutation: 22 of 50 killed.

**Test site (author).** Test site (test @ ff58a05), browser pane visible, desktop 1280×800 and phone 375×812. The
served tools.js, forecast.js and graticule.js equal the committed blobs. Input goes through the real path (pointer and
mouse events on the map, Leaflet's own click). At the site's maximum zoom (11, about 70 m a pixel) a click lands up to
~47 m from a reference coordinate; the browser's evaluation point equals Node's for the same click point.

| Item | Verdict | Evidence |
|---|---|---|
| Owner's spots | confirmed | Pipeline "W (250°–275°), WNW–NE (295°–045°)"; Haleiwa "W (265°–275°), WNW–N (300°–005°)"; Waikiki "SE–W (145°–275°)"; Hanalei "WNW–N (285°–010°)"; Rincon "SSE (150°–165°), W (250°–275°)"; Cape Hatteras "NNE–SW (015°–230°)"; Pensacola "SE–SW (125°–235°)"; Nice "SSE–SSW (165°–210°)" |
| Round-4 pins | confirmed | Honolua head 540 m "W (255°–275°), N (340°–005°)" (the clicked pixel's point; 460 m at the exact coordinate); Hilo land "N–ENE (355°–070°)"; Honolulu Harbor water 160 m "SSW–WSW (200°–245°)" |
| Speed and "Computing…" | confirmed | 64-146 ms click to result including new coast chunks; Kennebec 91-98 ms; Bergen with a fresh dense chunk 201 ms, cached 108 ms; "Computing…" shown every time, 3-19 frames drawn before the result, 0 long tasks inside any computation |
| Races in the new pause | confirmed | two clicks in one task → one fan (the second); ✕ during the pause → no fan; tool switch → no fan; unit change → the result in the new unit; Clear after a result → no fan |
| Same-height switch, focus | confirmed | area (83 px) → exposure (83 px) minimises an expanded forecast window over the bar, focus on the tool's ✕; with focus on the page, a result that grows the bar to 265 px minimises the window and focus stays on the page |
| Phone | confirmed | taps 77-99 ms, 212 px fans wholly on the map below the bar; a tap under the bar is not a map tap |
| Console | confirmed | no errors |

| # | Sev | Finding | Outcome |
|---|---|---|---|
| R5 P3-1 | P3 | The pause before placement is a 0 ms timer, which usually runs before the next frame, so "Computing…" is not guaranteed to paint first. The bucket index, not the pause, fixed R4 P2-1. | fixed (round 5) |
| R5 P3-2 | P3 | When tier-1 coast data fails to load, placement falls back to tier 0, and a long tier-0 edge fills every 100 m bucket of its bounding box. The worst, 104 km off Somalia, fills 544,019. The author reproduced it at (0.364, 43.246): 82-166 ms and 75-91 MB a placement (1.11.2: 2-4 ms, 1 MB), even for an open-water click, because the buckets are filled first. Guerrero: 47-50 ms, 54-56 MB. | fixed (round 5) |
| R5 P3-3 | P3 | The island-field timing test (< 80 ms) is load-sensitive: 18-24 ms on a quiet machine, but 5 of 12 runs failed with 12 busy loops. | fixed (round 5) |
| R5 P3-4 | P3 | The graze fix is not pinned. Removing both the give-up and the new side check passes all 44 tools tests, yet without the give-up (21.01525, −156.64300) goes 1,684 m over the headland. Also unpinned: `nearest()`'s exactness, the turn (the corner test passes without it), the 8-of-16 skip, the ranking tiers, the fallback order. | fixed (round 5) |
| R5 P3-5 | P3 | A water click in the innermost ~100 × 150 m of a narrow bay head now stays at the click (1.11.2 took the clearest water). Honolua's head loses 1,478 open wedges on R3's ±90 m grid (4 all-or-nothing losses); over the whole bay the net is −108 against 1.11.2 (+534 against 1.11.1), e.g. (21.01650, −156.64225): 1.11.2 "W, N", 1.11.3 none. | accepted (owner) |
| R5 P3-6 | P3 | "Looks out well first" places 34 % of land clicks beyond the nearest point 150 m out: 11 % more than 500 m further, 3 % more than 1 km. Where they are over 500 m apart, it gains a window at 2,658 clicks and loses one at 5. Honolulu Harbor shore clicks go 1.4-1.6 km to open water. NW Scotland (58.10485, −5.29741) still has no window: its inlet water looks out 1-2 of 16 and reads "NW (315°–325°)", and the pick, 600 m out at 6 of 16, reads none. | accepted (owner) |
| R5 P3-7 | P3 | Record and doc nits: "48,000" is 45,166; "tools.test.js 23" is 22; the README's "No walk crosses a coast beyond its own water" skips slivers under one 20 m step; one 161-character comment line. | fixed (round 5) |
| R5 note | P3 | The template's focus restore leaves focus on the window's opener when the saved element was hidden or detached; `if (document.activeElement !== had) document.activeElement.blur()` after the restore closes it. | fixed (round 5) |

## Fix round 5 (UI 1.11.5, 2026-09-29, HIGH)

**Owner decisions (2026-09-29).** Fix the P3s before production. Bay-head water clicks stay at the click (R5 P3-5), and
land clicks keep preferring water that looks out well (R5 P3-6); NW Scotland keeps "No open swell window".

- **Buckets (R5 P3-2).** An edge is filed only under the 100 m buckets it crosses (row by row, a small tolerance at
  each bucket line), and only inside the placement window, which every query stays within. Off Somalia (0.364, 43.246),
  in the tier-0 fallback: 3-4 ms and about 1 MB a placement (1.11.3: 82-166 ms and 75-91 MB); Guerrero 1-3 ms
  (47-50 ms). The most bucket entries in any placement over the four grid files: 4,096.
- **Ties (found in this round).** Where a walk runs exactly through a coast vertex, two edges give the same crossing
  parameter, and the one visited first set the turn; the nearest-edge search had the same order dependence. Ties now
  go to the lower edge, so the answer no longer depends on the bucket order. Over R5's four grid files (45,166 clicks;
  every click whose placement changed and one in seven of the rest checked against testing every edge): 0 differ from
  testing every edge. 29 placements differ from 1.11.3: all clicks where 1.11.3 did not match testing every edge
  (lattice vertex touches and ties; a Sochi click was the one left before the tie rule). At their evaluation points 20
  read the same and 9 change. 8 move one window edge by 5-10° (e.g. Hilo Bay "N–E (355°–080°)" → "N–ENE (355°–075°)",
  Steamer Lane "S–SW (175°–230°)" → "S–WSW (175°–240°)"). At the Honolua Bay mouth (21.023, −156.64; the point moves
  885 m) "NNW–E (335°–080°)" becomes "NNW–NE (340°–035°)", 45° off its east end (corrected after R6). No window appears
  or disappears.
  Mean placement time unchanged (0.6-3.0 ms per grid), the slowest 44 ms (dense grids; 1.11.3: 76 ms).
- **Paint (R5 P3-1).** Placement waits for an animation frame (then a timer), with a 100 ms timer for a hidden tab, so
  "Computing…" is painted first.
- **Focus (R5 note).** After minimising a window, the page returns focus to the element that had it; when that element
  is now hidden or gone (or focus was on the page), focus leaves the window's opener. Focus that was inside the window
  stays with its opener, as before.
- **Tests (R5 P3-3, P3-4).**
  - The dense-coast test counts edge tests (the same placement as testing every edge, with over 20× fewer tests)
    instead of timing.
  - One synthetic coast for each rule, built so that the rule decides the answer:
    - the give-up: 880 m in the bay; without it, 1,682 m past the headland;
    - the turn: the north coast at 660 m; without it, 1,760 m down the cove;
    - the tiers: the bay mouth, not the inner bay.
  - On the Maui crop:
    - R5's graze point: 360 m in the bay; without the give-up, 1,684 m.
    - The bay-head water click you kept: 0 m; without the fallback's look-out step, 220 m.
    - 64 clicks match testing every edge, with over 50× fewer edge tests.
  - A 160 km diagonal edge: fewer than 1,000 bucket entries, and the same placements as testing every edge.
  - The frame wait: nothing placed before the frame; placed after 100 ms when frames never run.
  - The template's own onLayout on a small DOM with browser focus rules.
  - The new tests fail on the old code: the frame test on 1.11.3's tools.js, the focus test on its template.
  - Targeted mutants: 7 of 9 killed:
    - the give-up, the turn, the tiers and the fallback's look-out step;
    - `nearest()` stopping early (R5's B7);
    - 1.11.3's bounding-box buckets;
    - a shrunk row band.
  - The 2 survivors change the threshold for skipping the search (6, or never). In every coast built, the first walk's
    point was also the nearest point that looks out well, so the threshold only saves work (accepted).
  - The tie rule has no synthetic pin. In the synthetic coasts, both edges at the tip share buckets in edge order, so
    the old order agreed. It is pinned by the real-data comparison above.
- **Docs (R5 P3-7).** The round-4 section now says 45,166 clicks and tools.test.js 22. README: a walk on land steps over
  water narrower than its 20 m step; an edge is filed only in the buckets it crosses. The long comment line is wrapped.
- **Suites.** pytest 456; Node 258, also with the clock at 2026-10-05 and 2027-09-01.

## Final check of fix round 5 (UI 1.11.5 @ edec939, 2026-09-29, MAX)

Two halves: R6, a fresh-context code reviewer (read-only, Node/Python, the published coast; report and scripts in the
session scratchpad `g20/r6/`), and the author on the test site. **No P0, P1 or P2; 5 P3.**

**R6 (code).**
- **Bucket fill: complete.** 24,224 adversarial synthetic edges produced 0 missed buckets. They included:
  - edges on bucket lines, ending on corners, zero-length or 20,000 km long;
  - edges across the window's edge;
  - near-horizontal edges down to dy 5e-324;
  - 1e-4° lattice edges at 0-75°.

  The checker is sensitive: without the tolerance it misses buckets on 5,778 of them.
- **The index equals testing every edge.**
  - `cuts()`: 0 differences in 89.9 M real calls.
  - `nearest()`: 585 of 4.98 M calls differ, all where the true nearest coast is at least 5.1 km away, beyond the
    window. Such an answer is only compared with 150 m, or ends in an inland refusal.
  - Placements: 74,469 clicks, 0 differ from testing every edge or from the reversed bucket order.
- **Speed.**
  - Tier-0 fallback beside the 8 longest tier-0 edges: 0.3-2.2 ms and at most 1.7 MB (1.11.3: 18-148 ms, 27-90 MB).
  - Kennebec: max 19 ms. The 22 densest places: max 37 ms (1.11.3 in the same run: 33 ms).
- **Ties:** the 29 placements that differ from 1.11.3 are all 1.11.3's own deviations from testing every edge.
- **Frame wait:** 15 interventions left exactly one placement per surviving click and never two fans. A hidden tab
  is placed by the timer.
- **Focus:** 10 of 48 cases change, each from the station trigger to the page (the tool keeps Escape). None is worse.
- **Real data.**
  - Safety scan, 69,302 clicks: clean.
  - Windows against 1.11.1 are unchanged from 1.11.3 (land 32 / 3,601, water 49 / 162); against 1.11.3, 0 lost and
    0 gained.
  - The 93 spots and the owner's examples are identical to 1.11.3.
  - Noted (rounds 3-4, not round 5): at one Ross Sea site (−74.88, 163.88) a one-wedge "SE (140°–145°)" window of
    1.11.1 reads none.
- **Golden and suites.** The golden differs from d1f7c8a only by the version and the focus lines. pytest 456; Node 258
  (also with the clock at 2026-10-05 and 2027-09-01). Mutation: 24 of 49 killed.

**Test site (author).** Test site (test @ 4aadf78, UI 1.11.5), browser pane visible, desktop 1280×800 and phone
375×812. The served tools.js, forecast.js and graticule.js equal the committed blobs (tools.js sha256 a5eea2bc…).

| Item | Verdict | Evidence |
|---|---|---|
| Owner's spots | confirmed | Pipeline "W (250°–275°), WNW–NE (295°–045°)"; Haleiwa "W (265°–275°), WNW–N (300°–005°)"; Waikiki "SE–W (145°–275°)"; Honolua head (exact coordinate, through the tool's click path) "W (260°–275°), N (340°–005°)"; Hilo land "N–ENE (355°–070°)"; Cape Hatteras "NNE–SW (015°–230°)"; Nice "SSE–SSW (165°–210°)"; clicks 52-185 ms, 0 long tasks; phone fans (212 px) wholly on the map below the bar |
| Frame wait (R5 P3-1) | confirmed | the page's requestAnimationFrame held: still "Computing…" at 60 ms and nothing placed; frames never released (a hidden tab): the result at 193-204 ms (the 100 ms timer, then the rays); the frame released at 30 ms: 126-131 ms; normal frames: 60-69 ms (4 runs each) |
| Tier-0 fallback (R5 P3-2) | confirmed | tier-1 chunks refused, in the page's engine: near Somalia (0.364, 43.246) 0.2-1.5 ms, no measurable heap, 178 bucket entries; Guerrero 0.3-1.7 ms, 140; side by side with 1.11.3's code (loaded from the public repo at e4da450): Somalia 80-112 ms and +38-42 MB, Guerrero 26-55 ms and up to +24 MB, 1.11.5 0-1 ms and +0 MB |
| Ties | confirmed | the Sochi click (43.5815, 39.7207) through the tool's click path: 43.579941, 39.716897 "WSW–WNW (240°–295°)", the every-edge answer; Hilo Bay (19.7357, −155.0722): 19.736695, −155.071092, the every-edge answer |
| Focus (R5 note) | confirmed | a result growing the bar over the expanded windows: focus inside the forecast window → its opener (here the tool's ✕); both windows, focus inside the forecast window → the same; focus on the live window's close button → kept (visible in its chip); focus on the page → the page |
| Console | confirmed | no errors |

| # | Sev | Finding | Outcome |
|---|---|---|---|
| R6 P3-1 | P3 | The frame test cannot see a placement made before the paint: it checks `busy`, which stays true while the rays run. A 0 ms fallback timer, or placing inside the frame callback, passes every test. | follow-up |
| R6 P3-2 | P3 | Still unpinned, although each changes real placements (of 45,166): the turn's side (5,838), its length (5,619), a 3 km bucket window (705) and the tie rule (16). A tie pin exists on the Maui crop at (21.023, −156.64). | follow-up |
| R6 P3-3 | P3 | The two tests that compare the index with testing every edge take about 13 s (33-35 s under load). This is suite time only; Node's timeout does not stop a synchronous test. | follow-up |
| R6 P3-4 | P3 | The crossing count's 1e-7 dedupe can still depend on visiting order, for three crossings within about 2 µm. It never happened in 89.9 M real calls. | follow-up |
| R6 P3-5 | P3 | The round-5 section said all 9 changed windows move one edge by 5-10°; the Honolua Bay mouth window loses 45° off its east end. Also `tools.js:463` still says "48,000 clicks", and `:476` says "every query stays inside it" (`nearest()`'s boxes grow past the window, harmlessly). | record fixed; comments follow-up |
