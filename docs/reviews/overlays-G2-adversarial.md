# Overlays — G2 adversarial review (Phase 2: one static frame)

Date: 2026-09-22. Scope: `feat/overlays-2` @ 3a895d3 (asset v2.0.4) as served on wave-app-clean.onrender.com
with `MODEL_OVERLAYS=1` (frames from the run 2026092212 bucket). Three fresh-context reviewers at max
effort: (1) projection sampler, decode and tile isolation by independent re-implementation; (2) isolation,
lifecycle, caching and security; (3) owner requirements and UX on the test site (desktop, 375 px phone,
landscape phone with a 180 px map). **No P0.** Every P1/P2 below is fixed in the fix batch (asset v2.1.0);
P3s are fixed unless marked deferred.

## Verified correct (reviewer 1)
- Grid registration (centre, +90 N / −180 E origin), row/column mapping, latitude edges at ±85.05°.
- Longitude periodicity: Leaflet passes wrapped tile coords to `createTile` and stores unwrapped coords in
  `_tiles`; the modulo in the sampler makes both identical (tile hashes identical across world copies).
- Half grid (`full[::2, ::2]`) sampled with its own `grid_half` metadata; LUT/legend mapping; byte-exact PNG
  decode (R channel equals the published codes).
- End-to-end: independently rendered tile hashes equal the layer's tiles for every tested (z, x, y).
- Performance: 1.8–2.4 ms per 256² tile (bilinear); `setFrame` redraws 25–40 tiles synchronously (≈ 50–90 ms).

## Findings
| # | Sev | Rev | Finding | Fix (asset v2.1.0 unless noted) |
|---|---|---|---|---|
| 1 | P1 | 1 | Readout sampled the nearest cell while the drawn pixel is bilinear: disagreement up to 1.585 m (Hs) / 6.9 m/s (wind); 1.93 % of coastal pixels drawn but readout null | One sampler `ModelGridLayer._code(r, cpos)` feeds both `tileCodes` (drawing) and `valueAt` (readout); Node tests assert per-pixel equality on full and half grids, wrapped tiles, pole tiles and missing cells |
| 2 | P1 | 2 | Bucket served from `r2.dev` (rate-limited, dev-only per Cloudflare) | Not a code fix: custom domain before G4 (plan §9 item 1, owner decision pending) |
| 3 | P1 | 3 | Mobile placement deviated from the owner decision (top-left folded panel instead of a bottom-edge sheet); the expanded panel overlapped zoom/Home on a 180 px landscape map (78 px) | Phones/short maps (container width < 576 px, height < 260 px, or height < 400 px on a coarse pointer) render the details as a sheet on the map's bottom edge to the RIGHT of the zoom/Home column (`--ov-sheet-left` = that corner's width + 6 px; the column never moves), one line by default; only the attribution corner is lifted above it (`--ov-sheet-h`); the WHOLE sheet is clamped to 40 % of the map's real height (container clientHeight — Leaflet's cached getSize() lags the site's script resize) with the details scrolling inside, and under 40 px of room it stays single-row. The desktop panel's details are ≤ 40 % AND the control ends above the measured zoom/Home stack. Verified on the test site: 375 px phone (258 px map) collapsed 31 px / expanded ≤ 40 %, no rect intersections with the stack, selector or attribution; 812×375 landscape (178 px map) single-row sheet; 1200×800 desktop panel, one-line attribution |
| 4 | P1 | 3 | Attribution suffix wrapped over the zoom "−" button on phones | Suffix shortened to "Overlay: NOAA GFS-Wave/GFS" (26 chars); while On the attribution box is capped at map width − 60 px via `--ov-attr-max` (a percentage max-width resolved against its own shrink-to-fit corner and forced wraps on desktop); the full NOAA sentence (from the manifest) is in the panel note |
| 5 | P2 | 1 | `setFrame` trusted the frame/grid pair | `validateGrid`: cols/rows/q length, centre registration, dlat < 0, periodic, cols·dlon = 360, lat/lon extent, lo < hi, legend inside [lo, hi] → error state with Retry |
| 6 | P2 | 2 | `/overlay/<name>` served any `?v` (a wrong or missing version could be edge-cached for a year) | 404 + `Cache-Control: no-store` unless `?v` equals `OVERLAY_ASSET_VERSION`; the route 404s while the flag is off; `X-Content-Type-Options: nosniff`; conditional responses so 304s carry both cache headers (tested) |
| 7 | P2 | 2 | `addAttribution` not idempotent (Hs→Tp→Off left it); `this.field` set before the frame landed (readout mislabelled) | Attribution added once (`_attributed`) and removed on unmount; the layer carries the drawn field/frame (`layer.field/fdef/entry`) and the readout/panel read from the layer; a field change clears the layer first |
| 8 | P2 | 2 | Half/full resolution chosen only at mount | Re-evaluated on `zoomend`/`resize` with hysteresis (`wantHalf`/`wantFull`), the old frame kept until the new resolution lands |
| 9 | P2 | 2 | Tests: no exact `var TZ` assertions; asset route absent from the cache-policy suite | Exact `var TZ` for inline, Graph, deferred, valid `?tz`, invalid `?tz`, coordinate fallback and UTC; `/overlay/overlay.js` in `tests/test_cache_policy.py`; 304 header test; empty-base test |
| 10 | P2 | 3 | A plain tap showed the readout (iOS synthesised mousemove) | Hover bound only under `(hover: hover) and (pointer: fine)`; mouse ignored for 1 s after any touch; long-press only (450 ms) on touch; press timer cleared on unbind; control/sheet targets ignored |
| 11 | P2 | 3 | Manifest 64.6 KB uncompressed on r2.dev | Job schema 3 (slim manifest + stats sidecar, `feat/overlays-job2` @ bbfa2cd, ≈ 4.6 KB); the client accepts schema 2 and 3 (`frameKey`) |
| 12 | P2 | 3 | Legend ticks were quarter-range fractions (e.g. 9.8 ft) | Nice ticks per unit: 0·10·20·30·39+ ft, 0·3·6·9·12+ m, ≤4·8·12·16·20·22+ s, 0·20·40·60·69+ mph, 0·25·50·75·100·111+ km/h, positioned proportionally; the wind note states the 60 kt top |
| 13 | P3 | 2,3 | Off during load left "Loading…"; a retry appended a duplicate `<link>` | Off clears the panel; the stylesheet link is de-duplicated |
| 14 | P3 | 2,3 | `pageCycle` evaluated once at create; Graph view and SWAN pages not handled | Passed as a function and read from the table or the Graph header on each render; SWAN pages get "the forecast table is a PacIOOS SWAN run" |
| 15 | P3 | 2 | Manifest cached for the page lifetime | `latest.json` re-read on mount when the cached pointer is older than 30 min; while a frame is on the map the session stays pinned and a newer run is announced (Update button in Phase 3) |
| 16 | P3 | 2 | A throw in `render` after `setFrame` left the panel stale | `_fail` guards every chain; the error state has a Retry button |
| 17 | P3 | 2,3 | Readout over controls; inline style attributes; no `autocomplete=off`; a11y | Control/sheet targets ignored; selector styles in a gated `<style>`; `autocomplete="off"` + value reset on load; `aria-expanded/aria-controls`, range `aria-label`, `aria-live` |
| 18 | P3 | 2 | Empty `MODEL_FRAMES_BASE` rendered the block with `""` | Empty base = feature off (page byte-identical, tested) |
| 19 | P3 | 2 | `_overlay_context` zone precedence differed from the parsers (unvalidated `?tz`) | `_effective_tz_name` mirrors `parse_bull`/`parse_swan`: valid `?tz` → station zone → coordinate lookup → UTC |
| 20 | P3 | 1 | `createImageBitmap` without colour-space options; HiDPI canvases; `setFrame` redraw chunking | `premultiplyAlpha: 'none', colorSpaceConversion: 'none'` + `<img>` fallback; HiDPI and rAF chunking deferred to Phase 3 (animation) |
| 21 | P3 | 3 | Wind over land, r2.dev, README | README section added; the other two remain owner decisions (plan §9) |
| 22 | P3 | 2 | `overlay.js` used `innerHTML` for the meta line | Every label is built with `textContent` (test asserts no `innerHTML` in the module) |

## Measured on the test site (before the batch)
latest.json 184 B · manifest 64.6 KB (schema 2) · frame full 154 KB / half 56 KB · overlay.js 18.7 KB (6.8 KB gz) ·
flag-on page +3.2 KB raw HTML · wind full-res loop 33.6 MB (half 10.4 MB) — Phase 4 half-res policy.

## Re-verification after the batch (test site, asset v2.1.0 → v2.1.3, 2026-09-22)
- v2.1.0: desktop 1200×800 → the map is 382 px tall (the site caps it at 48 % of the viewport) and my height-only
  compact rule put the phone sheet on a desktop; the attribution wrapped at 300 px (percentage max-width against its
  own corner); on the phone Leaflet reported 458 px for a 260 px map, so the sheet grew to 73 % and the lifted zoom
  stack reached the selector. → v2.1.1 (container dimensions, sheet beside the zoom column, only the attribution
  corner lifted, `--ov-attr-max`).
- v2.1.1/2.1.2: phone expanded sheet 56 % then 41 % (details-only cap; 40 px floor + wrapping title); 812×375 mouse
  window (178 px map) expanded the desktop panel over the zoom stack. → v2.1.3 (whole-sheet clamp from the measured
  layout, short title on phones, desktop details limited by the stack, maps < 260 px use the sheet).
- Verified on v2.1.x: readout equals the drawn value (Node tests, 13 pass; long-press on the phone shows 6.0 ft =
  `valueAt` at the touch point); Off tears everything down (0 tiles, 0 layers, attribution and panel restored, no
  readout, no sheet); asset route: `?v` exact → 200 immutable + nosniff, 304 keeps both cache headers, wrong/missing
  `?v` → 404 `no-store`; production page has no overlay markup (count 0). pytest 344 passed.
- Final v2.1.3 numbers are recorded in the plan file §13.
