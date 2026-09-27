# Forecast window — G16 adversarial review (plan section 25) — 2026-09-26

The forecast table left the page flow for a floating window (drag / resize / minimise / maximise; minimised on load; a
chip at the bottom-left on desktops, a bar along the bottom on phones; full screen when expanded on phones), View and
Model moved into its toolbar, the top bar became brand · station picker · settings gear (Time Zone, Units), and every
change applies in place from `/api/forecast` with the address bar kept in sync (no reloads, the Update button gone). The
table is the compact form (short dates, no info rows). Branch `feat/forecast-window` @ 2cd77c1 (PR A plumbing already on
production @ 96bf03c); test site = branch `test` @ 8b68a7b (+ its logo).

Two fresh-context reviewers at max effort (Fable 5.1): A the code (diff, suites, 25 mutants on a scratch copy, two
findings confirmed in Chromium), B the running test site (desktop 1280×800, phone 375×812 and 812×375, production
read-only for comparison).

## Findings: 1 P0, 0 P1, 9 P2, 13 P3 — all P0/P2 fixed, P3s fixed or accepted as noted

| # | Sev | Finding | Outcome |
|---|---|---|---|
| A-P0-1 | P0 | On phones the top bar (`z-index: 3000`, needed so its dropdowns beat the window on desktops) painted OVER the full-screen sheets (window 1500, live panel 2000): the window could not be minimised by touch and the live panel could not be closed. | In phone mode `.forecast-win:not(.fw-min), #liveBuoyPanel { z-index: 3500 }`. Pinned by `test_phone_sheets_sit_above_the_top_bar…`; verified by real taps on the test site. |
| A-P2-1 | P2 | In Graph view a table-less error (every server-side failure is HTTP 200 + `error`) showed nothing: the message went into the hidden table area and the error box was hidden. | A table-less error goes to the visible box in both views, with Retry. |
| A-P2-2 | P2 | Error payloads were cached for 10 min with no Retry (the server itself never caches a failed build). | Never cached; Retry re-fetches. |
| A-P2-3 | P2 | A newer forecast landing while Chart.js was still loading was never drawn (and stale charts survived). | The render loops to the latest data; null data while loading destroys the charts. |
| A-P2-4 / B-P2-4 | P2 | The address bar dropped `tz=` / `unit=` when they equalled the defaults, but a reload reads an absent parameter as "use the saved settings": a map pick (Buoy Local) or an explicit `?tz=&unit=US` reloaded into the viewer's saved zone / unit. | `urlFor` names tz and unit whenever they differ from the saved settings (else the defaults), so the address always reloads to the view on screen; round-trip test against `resolveInitialState`. |
| A-P2-5 | P2 | Without JS the settings panel stayed hidden (Bootstrap's `[hidden] { display: none !important }`). | `!important` on the no-JS rule; the table forced visible and the charts hidden without JS. |
| B-P2-1 / A-P3-4 | P2 | The overlay's valid-time zone and its run-line note were page-load constants: after an in-place zone / station / model change the table and the overlay disagreed (production reloaded on every change). | The module dispatches `allshore:forecast` {station, tz, model, view} on every landed forecast; the gated overlay block sets `overlay.opts.tz` and calls `overlay.refresh()` (pinned in the flag tests and the bootstrap Node test). |
| B-P2-2 | P2 | After a favourites pick, Escape did not return focus to the picker trigger: the opener was the hidden list button. | Only a visible opener (`offsetParent`) takes the focus back; else the trigger. |
| B-P2-3 | P2 | The Home-view menu's last item was under the desktop chip. | The menu opens 52 px up in chip mode. |
| A-P3-1 | P3 | The no-JS refresh carried `view=Graph` (a hidden table and empty canvases). | Always `view=Table`. |
| A-P3-2 | P3 | The no-JS card inherited the chip's 560 px `max-width`. | `max-width: none` without JS. |
| A-P3-3 | P3 | A failed first fetch left the "Loading forecast…" spinner beside the error. | The placeholder goes with the error. |
| A-P3-5 | P3 | Storage accessors unguarded (`init()` threw with cookies blocked; `saveMapView` on every `moveend`). | `safeStorage` fallback; the moveend save in a try. |
| A-P3-6 | P3 | `parseLabel` never matched the server's "Saturday, September 26, 2026 2:00 PM" and relied on the browser's Date parser (JavaScriptCore rejects it: night shading wrong on Safari; pre-existing). | The long form parsed exactly (month table). |
| A-P3-7 | P3 | Double-click on the chip expanded AND maximised; `--topbar-h` only refreshed on a mode change; the header named the old station during a failed load; a garbage `?tz=` is persisted; `_forecast_is_cached` is dead; two weak tests; a stale comment. | Double-click ignored within 600 ms of expanding; the bar height follows a resize; an error names the station the address bar shows; the tests strengthened (an injected error, string pins with intent for the page's own script); the comment. Accepted: the garbage tz (text-only, server-added option), the dead helper (kept for the tz tests). |
| B-P3-1 | P3 | Long station names clipped the run label in the chip. | The cycle never shrinks; titles on both. |
| B-P3-3 | P3 | Escape on the ★ toggle left the favourites list open. | Handled. |
| B-P3-4 | P3 | An unknown `?station=` shows the raw parser message and the picker's first option. | Accepted (invalid link). |
| B-P3-5 / B-P3-7 | P3 | Landscape phone (812×375): the chip overlapped the overlay's sheet; the maximise button on phones is a no-op. | "Phone mode" is width OR height ≤ 500 px (module query, CSS, the map's reserve): a bar + full screen there; no maximise button in phone mode. |
| B-P3-6 | P3 | A minimised window printed no forecast. | The print rules show the toolbar and body. |

Tests after the fixes: pytest 424, Node 171. Mutants: the author's 22/22; reviewer A's 25 → 22 killed, 3 equivalent
(M4 selects synced by the station handler; M7 left+width win the over-constrained box; M17 the drag guard blocks the
handle in `max` too). The page golden was re-baselined again at the fix commit. `--test-timeout=20000` on every Node run
(a promise-hanging regression fails instead of stalling CI).

Verified by the reviewers (before the fixes, unchanged since): security (only `table_html` is HTML; every other string
is text; `initial_state|tojson` escaped; the compact date cell escaped); the loader's sequencing, abort, cache key, SWAN
normalisation and model echo; graph re-entrancy and listener counts; the window's guards, clamps and persistence; the
overlay contract (`#forecastMeta` first, `Cycle :` text, SWAN detection, the `#forecastLoading` poll, `#unit` events,
the globals); the flag-off golden invariants; the server's compact table and `render=full`. On the test site: one
request per change, no reloads, three quick picks → the last wins, favourites recentre at zoom 8, Metric/UTC flow through
the table, the live panel and the overlay, SWAN 175 rows / GFS fallback, drag / resize / maximise / minimise with the
geometry restored, clamping into a 900×600 window, the picker and settings above a dragged-up window, the overlay
restoring after a reload, the phone bar clear of Home / attribution / the overlay sheet, keyboard order, no console errors,
`?render=full` without a fetch.

## Re-review of the fix set (one fresh-context reviewer, HIGH)

Every G16 finding confirmed fixed on the code and the test site, with one gap and four small items:

| # | Sev | Finding | Outcome |
|---|---|---|---|
| N-P2-1 | P2 | The overlay's FIRST mount still took the server's zone: the module's `allshore:forecast` fires before the overlay exists (the restore waits for the forecast), and the new listener returned early, so a viewer with a saved zone saw the table in it and the overlay in the station's zone until the next forecast. | The gated block remembers the zone of every landed forecast (`lastTz`) and creates the overlay with it; bootstrap test + string pin. |
| N-P3-1 | P3 | Landscape phone (812×375): the map's 280 px floor put the bar over the map's bottom 29 px (Home, the sheet's legend). | The floor is 180 px in phone mode. |
| N-P3-2 | P3 | The live panel's z 3500 applied in short-but-wide windows where it is not full screen, over the bar's dropdowns. | z 3500 only under the panel's own full-screen rule (≤ 500 px wide). |
| N-P3-3 | P3 | A `render=full` page with a server error was seeded with whitespace as its table: no Retry, cached 10 min. | Seeded without a table when the page carries an error; `seed()` never caches a table-less error. |
| N-P3-4 | P3 | Printing a minimised window in Graph view printed empty chart boxes. | Print shows the table and hides the charts. |
| N-P3-5 | P3 | Surviving mutants of the fix code: the placeholder removal on a transport failure, the header rename on error, settings saved before the load. | Pinned (a minimise-side `visible()` check stays as defence). |

Design notes from the re-review, accepted: after a gear choice the address is minimal, so a copied link does not carry
the zone to another viewer (the old page's URL always did); a units change refreshes the overlay panel twice (harmless).

After the re-review fixes: pytest 424, Node 173; UI asset 1.5.0; the golden re-baselined once more.

## Owner's test-site report (2026-09-26, after the re-review)

The owner found the test site slow, cut off, then without a basemap, markers, picker or particles, and pannable past
the poles. Two page defects, both fixed on `feat/forecast-window` and re-verified on the test site:

| # | Sev | Finding | Outcome |
|---|---|---|---|
| O-1 | P1 | Leaflet measured `#map` at its 460 px CSS height when created, and `enforceSingleWorld`'s `invalidateSize()` ran before the first view (ignored until the map is loaded): the map kept 458 px while the container was 817, so the lower part had no tiles and the overlay stopped short. | The container is re-measured before and after the first view. |
| O-2 | P0 | The latitude clamp corrected in degrees and called `setView`: near a pole the correction converges geometrically, and once it fell under one pixel Leaflet truncated it to nothing, fired `moveend` again and the handler recursed until the stack overflowed. Pre-existing on production (a polar pan throws there too) but harmless with a 460 px map; with the viewport-filling map a saved view near a pole put the overflow INSIDE the map's first view: Leaflet's `load` never fired, so the imagery and the marker groups (added before the view) never attached, the picker and the live layer (wired after `enforceSingleWorld()`) never ran, and the animator's later `moveend` listener never resumed after a pan. Reproduced on the test site with `sessionStorage.mapView = {lat: 78, z: 3}`. | Pixel-space clamp by whole pixels with a re-entrancy guard (`tests/ui/mapclamp.test.js` runs the page's own block against Mercator maths); the minimum zoom also fits the polar band to the map height, so the poles are the farthest extent on portrait maps; the handler reports instead of dying. |
