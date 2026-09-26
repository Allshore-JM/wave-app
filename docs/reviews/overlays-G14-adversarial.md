# Overlays — G14 adversarial review (the wind look, plan section 23) — 2026-09-26

Owner request: under the wind overlay, no near-white low end over land and no earth-toned imagery beneath it; a relief
basemap consistent with the sea, like windy.com; the imagery stays for no overlay, wave height and peak period. The owner
picked look **A + W1** from a preview sheet (the real client on the live 06Z run: today, A+W1, A+W2, B+W1, B+W2 at
Hawaii z6, Oahu-Maui z8, US West z5, Mexico z5): Esri World_Hillshade under the wind with the model pane multiplied into
it (wind 0.9 opacity, the waves keep 0.65, each remembered), the GSHHG coastlines drawn into the wind tiles from the
per-tile land masks (never clipped), a muted windy-like palette with a blue calm end.

- Asset 2.11.0 @ 8194035 on `feat/overlays-windlook` (off production a225a09); test @ b63f02d.
- Fixes, asset 2.11.1 @ 840e006; test @ 5dbcbe5.

## G14 — one fresh-context reviewer at high effort (Opus 5.5), code + test site: 0 P0, 1 P1, 1 P2, 9 P3

| # | Sev | Finding | Outcome |
|---|---|---|---|
| P1-1 | P1 | Every basemap swap showed the bare map for ~200 ms: Leaflet 1.9.4 fires 'load' as the last tile STARTS its 200-ms fade-in, and the old basemap was removed at that moment (measured on the test site: 9/9 new tiles at opacity < 0.5, mean 0.016). | The old basemap leaves `BASE_FADE_MS` (300 ms) after 'load', only if nothing switched meanwhile. Test site: at all six removals of a wind/hs/tp/Off cycle the shown basemap's tiles were at opacity 1 (6/6 tiles each); 20-ms sampling: imagery -> both -> relief, never none. |
| P2-1 | P2 | 16 of 28 single-line mutations of the new wiring survived (no wind coastlines at all, wind stuck on the tier-0 stand-in, a stale edge cache, east-west coasts lost, the threshold, no `_syncLook` at a field change / unmount, `hasFrame` ignored, the per-field slider, the attribution gate, the coast wait contracts). | Tests added through the real controller (mount / field change / unmount / Update, the wind coast joining after the frame, a new layer keeping a loaded store, `_cellsNeeded` for wind at z >= 7), the tier-0 -> tier-1 edge swap through `_redrawIncomplete`, an east-west coast, the threshold, the GSHHG credit, the fade wait, blend-only-when-sole, the cooldown. Re-run of the reviewer's mutants against 2.11.1: 30 of 32 killed; left: the timer's base re-check (redundant with its `hasLayer` check) and the slider value (the harness cannot render the panel; verified on the test site). |
| P3 | P3 | The blend was set while the relief was still loading (the wind multiplied into the dark imagery on a first, uncached use). | The blend is on only while the relief is the sole basemap. |
| P3 | P3 | `layer.setOpacity` on every landing (every playback frame). | Only on a change. |
| P3 | P3 | One all-error relief load disabled it for the session. | Retried after 10 minutes; a late 'load' after switching away is not a failure. |
| P3 | P3 | `_reliefLayer` copied undefined options over Leaflet's defaults (latent). | Copied only when defined. |
| P3 | P3 | An Update before the wind's coast load finished left wind without coastlines until the next mount. | The wind's coast attach is not tied to the mount's signal. |
| P3 | P3 | Coastline ink at tile seams: one-sided differences lose some ink at seam pixels (e.g. Norway z9: 26 seam pixels that should be > 0.3 got < 0.1); a coast lying exactly on a tile edge gets no line. Not visible at screenshot scale; no false line at 180 degrees. | Accepted (owner awareness); a 1-px mask apron is the fix if it is ever seen. |
| P3 | P3 | Owner awareness: the Hillshade service's full credit is longer than the page's "© Esri & contributors"; a tab's old shared `opacity` now applies to the waves only (wind starts at 0.9); on flat coasts only the ink line separates land from sea (both ~250 in the relief); iOS Safari blending not verified (Chromium only). | Accepted as noted. |

Verified by the reviewer: the swap state machine through every transition (hs/tp/wind, Off, unmount, restore, Retry,
Update, `_fail`, relief failure, stale handlers); rapid live switching never left zero basemaps; the blend isolates to the
model pane over the tile pane only (particles, markers, popups, the panel unaffected); nothing else on the page uses the
imagery layer; the attribution once; wind never clipped, readout = `valueAt` over land; hs/tp keep their clip and their
first-draw wait; `coastEdges` ~0.2 ms per tile (z3 full redraw 13.7 vs 12.9 ms, z9 7.1 vs 5.8 ms); per-field opacity on
the desktop panel and the phone sheet; GSHHG credit under wind; World_Hillshade serves z11; no console errors; flag-off
page unchanged; version and fixture.

After the fixes: Node 132, pytest 398; asset 2.11.1 served on the test site with sha256 = the fixture.
