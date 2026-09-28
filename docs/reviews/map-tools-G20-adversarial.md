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
