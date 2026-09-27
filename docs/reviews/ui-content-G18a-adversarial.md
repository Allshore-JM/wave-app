# UI content (plan section 26, D1) — G18a adversarial review — 2026-09-26

Scope: `feat/ui-content` (2514a07..e300742; UI asset 1.8.0 -> 1.8.2): taller graphs with only the swells present,
the Table-view width cap, swell groups per forecast (table: first 7 days; graphs: the whole run), Wave Height on a
tab's first open, the Leaflet prefix removed, lat/long gridlines. One fresh-context reviewer at HIGH (code, 37
mutants on a scratch copy, the test site at desktop and 375x812).

## Findings: 0 P0, 0 P1, 3 P2, 10 P3 — fixed in UI asset 1.8.3 unless noted

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P2-1 | P2 | The width cap was written into the viewer's saved geometry (`setMaxWidth` -> `clamp` -> `_save`; a move used the capped width): after a narrower table (SWAN) the window never grew back, and a Graph-view width was lost after a trip through Table view. | The cap is applied by CSS `max-width` only; `geom.w` stays the viewer's; `clampGeometry` clamps x with the width on screen; resizes start from the shown width and stop at the table; a move with no saved geometry uses the CSS default width. Tests: the saved width survives a new cap, a clamp and Graph -> Table -> Graph; a wider table widens the window again. |
| P2-2 | P2 | Flat date labels overlapped on narrow charts (phones: 16 labels 18 px apart). | `dateTick`: every n-th midnight in view, n from the scale's width (>= 44 px per label) and the range shown; test at 1100 / 300 px and the 3-day range. |
| P2-3 | P2 | Phones never showed longitude labels (the overlay selector and the layer list cover the top edge). | `lonTop`: a longitude under a SHORT top control (bottom <= 90 px) drops just below it; under a tall one (the desktop overlay panel) it stays hidden. |
| P3 | P3 | Labels stale when a control changes size (overlay mount / Off). | A ResizeObserver on the map's control corners re-places them. |
| P3 | P3 | The phone overlay sheet (`.ov-sheet`) was not treated as a control. | Included in the control selector. |
| P3 | P3 | 1.8.2 lines blurred at 125 % / 150 % display scaling (snapped in CSS pixels). | `snapPx`: the middle of a DEVICE pixel; tested at DPR 1, 1.25, 1.5, 2. |
| P3 | P3 | `TABLE_CHROME` assumed an 18 px scrollbar. | The scrollbar is measured (`offsetWidth - clientWidth`; 18 when hidden). |
| P3 | P3 | 1.8.2: 2-degree lines are 700-2,900 px apart at zoom 9-11 (often none in view). | Owner's rule (5 degrees, 2 degrees close in); accepted. |
| P3 | P3 | The table can hide a swell that appears only after day 7 (the graphs show it). | Owner's rule; accepted (no current case at 51201 / 46026). |
| P3 | P3 | Pinch zoom re-shows labels mid-gesture with the new zoom's step; ~1.5 ms per pan frame for labels; the 3-day range's first partial day unlabelled; blocked sessionStorage always starts on Wave height. | Accepted (transient / cosmetic / no storage to remember Off). |

Mutants: the survivors (`fitWidth` phone check, `setMaxWidth` clamp, drag cap, `formatMD` hour, title, 10 % pad,
graticule margin / stale labels / zoomstart hide / label zoom floor / top-row rule) are pinned where the behaviour is the
owner's (width cap, labels, snapping); the rest are cosmetic. Verified by the reviewer: `present`/`swells` for short and
empty rows, SWAN vs GFS, Metric, the classic table untouched; the overlay default and Off persistence; attribution with the
overlay credits; gridline pixels within 2 px of the projection at zooms 2.2-11 incl. the dateline and 70 N; markers
clickable through the layer; no console errors.
