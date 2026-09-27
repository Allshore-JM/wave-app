# Map-first layout (plan section 26, D2) — G18b adversarial review — 2026-09-26

Scope: `feat/ui-layout` (c888d93..HEAD; UI asset 1.9.0 -> 1.9.3): the top bar gone and the map the page, the brand and
the settings gear as map controls, the favourites picker as the forecast window's heading, the live-buoy panel as a
floating window, the credits behind an (i). Two fresh-context reviewers at MAX: A the code (diff, suites, 39 mutants on
a scratch copy, findings confirmed on the test site), B the running test site (1580x900, 1280x800, 1024x768, 375x812,
812x375). The owner reviewed the test site in parallel and reported two defects, fixed during the gate.

## Findings: 0 P0, 1 P1, 2 P2, 13 P3 — every P0-P2 fixed; P3s fixed or accepted as noted

| # | Sev | Finding | Outcome |
|---|---|---|---|
| owner / A-P1 | P1 | The map's top-right corner (legend + gear) had been raised above the windows (so the gear panel was never covered): a maximised window's minimise / close buttons landed UNDER the legend and the window could not be closed. | The corner outranks the windows only while the gear panel is open (`.settings-open`, a MutationObserver on the panel's `hidden`); the maximised forecast window starts below the corner (`--map-topright-h` measured in `enforceSingleWorld`; 115 + 8 px); the live-buoy window has no maximised state at all (owner: no gain) — button removed, `FloatingWindow` `canMax: false` (setMode / double-click / a saved max). |
| owner | P2 | The minimised chip hid the start of the run text ("SWAN · updated …") behind the station picker box, which did not shrink inside the chip. | The picker box shrinks (its label ellipsises at 48ch), the run text never shrinks, the forecast chip grows to `min(800px, 60vw - 40px)` and the live chip to `min(420px, 40vw - 40px)`; the phone bar gives the name priority (the run text shrinks four times as fast). Verified 51208 / SWAN and 46001 at 1580 (chip 616 / 754 px, the run text whole, 394 px between the chips). |
| A-P2-1 | P2 | `.fwin { min-width: 360px }` beat the chips' max-widths: below ~930 px wide (a tablet) both chips were 360 px and overlapped by 85 px. | `.fwin.fw-min { min-width: 0 }`; pinned. |
| A-P2-2 | P2 | The live chip (bottom-right 12 px, z 2000) parked on the forecast window's default resize corner (right / bottom 16 px, z 1500 until touched): the first pointerdown on the corner hit the chip. | A parked chip adds `body.live-chip`: the forecast window's DEFAULT box rises to `bottom: 70px` (a dragged window keeps its inline position); pinned. |
| A-P2-3 | P2 | Twelve template mutants and two module mutants survived (page pins missing for the live-window assignment, the bar guard, `--fw-bar-h`, `place()`, the re-measure, the live z, the observer filter, the max-button rule, the gear's propagation guards, the brand corner; the chip's ▴ never asserted; the live key could equal the forecast key). | All pinned (`test_g18b_pins`; window tests). |
| B-P3-1 | P3 | Landscape phone with a parked live chip and Wave Height on: the overlay panel's colour scale sits under the live bar (the overlay client sizes its panel from the map at mount). | Accepted for this release (overlay client; a later overlay release can cap the panel at the map height). |
| B-P3-2 / A | P3 | The Home menu touched the chip by 1 px (`bottom: 52px`). | 62 px. |
| B-P3-3 | P3 | A phone-width load clamped a saved desktop geometry to 360 px and the next save kept it (pre-existing). | Phone mode leaves a saved geometry alone (neither applied nor cut); test. |
| A | P3 | `notify()` ran from `onMode` inside the constructor with `fw` undefined (swallowed). | Guarded. |
| A | P3 | A rotation while the forecast window was full screen kept the other orientation's bar height until the next resize while minimised. | The page re-measures on the window's mode changes (`onMode`) in phone mode. |
| A | P3 | A double-click on the favourites list's padding maximised; a pointerdown on it started a drag; a click inside the list on the chip expanded it. | The header guards skip `.station-results`; test. |
| A | P3 | `place()` ignores a parked live bar on phones (the open list covers it); the inline max-height overrides the phone `50vh`; desktop viewports 501-560 px tall put the live window's default top over the gear; the label `for` names the trigger "Station". | Accepted (cosmetic / rare / pre-existing). |
| A | P3 | Stale docs (module header, README, the golden docstring). | Updated. |
| A / B | P3 | Two `reading 'baseVal'` console errors during the reviewers' emulated touch runs. | Reviewer B's own synthetic `mousemove` on `document` (reproduced by them; no in-page error across every real load and interaction). No action. |

Verified by the reviewers: nothing throws in the map script's top-level run; the corner order (brand, overlay selector;
legend, gear; credits, (i), Home); `map.attributionControl` alive through `setPosition` with the overlay's line;
`liveDetailSeq` bumped on every close; every buoy source opens through `showLiveBuoyPanelLoading -> liveWin.open()`;
Escape closes an open live window first and leaves a parked chip alone; the unit change follows into the table, the
overlay legend and the live panel (even while it was a chip); the picker's keyboard (Tab, ArrowDown, Escape closes the
list only, Escape again minimises) and its list placement (below the heading, upward from the chip and the bar, full
width on phones); drag / resize / clamp / restore of both windows, an old-layout geometry and an off-screen box clamped
inside; the phone maths (bars 61 + 51 px, the map above them, full-screen windows z 3500 with every header button
hit-testable, map drag intact); no-JS (`render=full`: the head row, the server table, the station select, the Go
button, the live window hidden) and print; no in-page errors at any size.

Mutants (reviewer A, scratch copy): module 16 -> 12 killed + 2 equivalent + 2 pinned now; template 23 -> 11 killed +
12 pinned now.
