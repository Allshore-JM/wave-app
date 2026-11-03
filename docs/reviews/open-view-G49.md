# Section 79: the opening view — review G49 (2026-11-02)

Owner (2026-11-02): the site opened too far out (zoom 6 on every fresh open); open "approximately as shown" (a screenshot:
zoom 9.0 on a 1908 x 906 map, Kauai to Maui, centred on Oahu). Owner decision: "fill the screen" — the opening zoom shows the
middle of a 5.2 x 2.3 degree area around the opening point, never more of it (`openZoomFor`, floored to a tenth). Used by the
Hawaii fallback (now centred 21.5 N 158.0 W), the default station's view and the IP view (still lowered so the nearest
station shows). Commits 3f9de8f (code + tests), 961b6b9 (golden), 65ffbbd (README).

## Test site (test @ b0e2569, headless Chrome, location answering from Oahu)
| Screen | Zoom | View |
|---|---|---|
| 1908 x 906 | 9.0 | 160.64-155.40 W, 20.29-22.60 N (= the screenshot; production: zoom 6) |
| 1440 x 900 | 9.0 | 159.99-156.04 W |
| 1366 x 768 | 8.7 | 160.32-155.70 W |
| 1280 x 720 | 8.6 | 160.34-155.70 W |
| 375 x 812 phone | 8.7 | 158.65-157.38 W (Oahu) |
| 812 x 375 sideways | 7.7 | 160.76-155.27 W |
| 1024 x 1366 iPad | 9.6 | 158.94-157.09 W |
A `?station=46001` link opened Oahu at 9.0; a default station (46026) opened on it at 9.0; a reload kept zoom 7; no console
problems.

## Review (one fresh reviewer): 0 P0, 0 P1, 0 P2, 3 P3 + test gaps
- P3 README precedence line still said "zoom 6" for the default station — FIXED.
- P3 the default station's latitude was not clamped before the zoom (a station above ~84.9 N opened a little closer) —
  FIXED (`openZoom(clampLat(dflt.lat))`, as geoView).
- P3 phones: the first view assumes a 48-px forecast bar; the real bar is measured later and only the height changes (the
  screen stays filled) — accepted, no change.
- Test gaps (mutants surviving: the fallback reverted to zoom 6, the old centre, width/height swapped in the wrapper, span 5.1,
  round for floor) — FIXED by source pins of the fallback and wrapper, a sideways-phone case (width decides) and a width-limited
  case just under a tenth; the four survivors now fail the suite.
Fixes @ the commit after 65ffbbd + golden. Node UI 584, page/flag pytest 59.
