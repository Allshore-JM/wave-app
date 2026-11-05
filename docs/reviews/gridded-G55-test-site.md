# G55: the gridded satellite map on the TEST SITE (plan section 82) — adversarial test record

Reviewer: one fresh agent (Fable 5.1, reviewer B: the live test site), 2026-11-05 (04:40Z onwards), against
https://wave-app-clean.onrender.com (page `var GRID = true`, `var SAT = true`, `var SWELL = true`; the served
`/overlay/overlay.js?v=2.24.1` is byte-identical to `static_overlay/overlay.js` of `feat/gridded` @ 2171a7a, checked by
diff at 04:42Z). Data: `https://models.allshoresurf.com/altgrid/v1/latest.json` -> day 20261103, production
20261104T073010Z, manifest `manifest-20261105T020207Z.json` (1,971 B, immutable), newest step `2026-11-03T18:00Z`
(frame `hs-18.png`, 34,901 B, immutable; 143,046 valid cells). The age at the start of the review: 34.7 h (the 48-h warn
flips at 2026-11-05 18:00Z, the hide at 11-06 18:00Z, unless the 07:30Z production lands first).

Harness: own headless Google Chrome 156.0.8078.80 (`--headless=new --remote-debugging-port=9551`, a fresh profile folder
`scratchpad/g55/b/chromeprof-g55-<random>` per run, `--no-first-run --disable-sync --no-default-browser-check
--enable-gpu --use-angle=d3d11`; no sign-in of any kind; never Edge) driven over the DevTools protocol from Node 26
(`scratchpad/g55/b/cdp.mjs`): trusted input only (`Input.dispatchMouseEvent` / `dispatchKeyEvent` /
`dispatchTouchEvent`), the native overlay `<select>` moved by arrow keys with the focus on it, the box clicked at its
centre. Read-only towards every server; `Browser.close` at the end of every run, PIDs recorded in `b/pids.txt`, the
profile folders deleted at the end; the test origin's session / local storage cleared at the end of every run.
Reference for the values: `b/ref_sample.py` (my own bilinear sample of the published `hs-18.png`, PIL + numpy, the
sampler written from the manifest's `encoding_spec`: q 0 absent, value = (q - 1) / 254 * 22.86 m, bilinear over the
present neighbours, dropped when their weight is under 0.25; sampled at the centre of the drawn tile pixel).

## Findings

| # | Sev | Finding | What I did / saw / expected | Evidence | Likely cause |
|---|---|---|---|---|---|
| B-1 | P3 | **The warning line's hour count goes stale between panel renders.** With the map 50 h old the panel says "The gridded map is 50 h old."; six hours later (the 30-min timer's callback `_checkRun` and the 12-min `_satRecheck`, the clock moved +6 h, the readings still fresh) the note beside the box reads "(56 h ago)" while the warning line still says "50 h old". The panel re-renders only on a STATE change (fresh / warn / hide) or for another reason (the readings' own stale flip, a toggle, an entry switch), so the warn line can sit a day behind the note: a map 71 h old can be captioned "48 h old" until it hides. Expected: the warn's number to follow the note (or the line to carry no number, "The gridded map is over 48 h old."). | `b/t8_out.json` "warn line: 50 h, then +6 h by the timers" (`at50.warns` = ["The gridded map is 50 h old."], `at56.note` "(56 h ago)", `at56.warns` = ["The gridded map is 50 h old."]); `b/shots/t8_02_warn_stale.png`; the same at 48 -> 49 h in `b/t4_out.json` ("G 47.5 h -> +1 h"). In `b/t7_out.json` (b) the number DID move (50 -> 66 h) only because the readings flipped stale at the same time and re-rendered the panel. | `overlay.js` l.5109-5115 `_gridLineSync`: updates `ui.gridNote` in place but renders the panel only when `state !== this._gridWarnShown`; the warn line's text is built once in `_renderSat` l.7176 from `Math.round(self.grid.ageH())`. Fix: in `_gridLineSync`, also update the warn element's text in place (keep a `ui.gridWarn` handle as for `gridNote`), or render when `Math.round(ageH)` changed while in the warn state. |
| B-2 | P3 | **The owner's sheet promises a tap on open water shows the grid value on a phone; the site needs a long press.** On 375 x 812 and 360 x 640 (mobile + touch emulation), a quick tap (80 ms) on open water away from any dot shows nothing; a 700-ms press shows "grid 8.8 ft · Nov 3, 08:00 AM HST". A quick tap on a dot shows the dot's text with the grid tail for 4 s (as section 81). The code is consistent with section 81 (the field's readout on touch is the long press; a plain tap only on a mark), but `docs/plans/section-82-test-site-sheet.md` l.30-31 ("a tap on open water shows the grid value (long press for the field elsewhere on the map, as before)") and the G55 brief's attack 6 say a tap. Either the sheet is wrong (fix the wording) or the owner wants a tap on water to read the grid while the entry is drawn (a code change in `te()`); the owner's call. | `b/t8_out.json` "p375 quick tap on open water" {hidden: true}, "p375 long press on open water" {text: "grid 8.8 ft · …"}, the same at p360; `b/t9_out.json` "quick tap on a dot" "4.5 ft · SARAL · … · grid 5.7 ft" for 4 s; `b/shots/t8_p375_press.png`. | `overlay.js` l.6470-6481 `te()`: a quick tap calls `show()` only when `sat.hitAt(...) >= 0`; otherwise `el.hidden = true` after 1,500 ms; the long press (`ts()`, 450 ms) is the only path to `show()` over open water. |
| B-3 | P3 | **At 360 x 640 the grid's warning line is half below the sheet's details box.** With the box on and the map past 48 h (a faked pointer 50 h old, the readings fresh, the clock untouched), the sheet's `.ov-details` is capped at 191 px (`maxHeight` 191px, `scrollHeight` 201): the credit line is fully visible (540-568) but the warn line "The gridded map is 50 h old." sits at 572-591 with the box's bottom at 581 — the lower half of the line is cut off; it is reachable by scrolling inside the panel (`overflow-y: auto`), which nothing signals. With both warnings (the readings stale too) the grid's line is entirely below (595-614). At 375 x 812 everything fits (224 of 260 px). The plan's own risk item asked exactly this ("verify the credit line and the warn still show at 360 x 640 without the fold"): the credit does, the warn does not quite. Expected: the whole line visible at 360 x 640, or the cap a line taller, or the note's age (already right beside the box) considered the phone's warning and the line dropped there. | `b/t9_out.json` "360 x 640, one warn" (details top 390 / bottom 581 / sh 201 / ch 191; warn top 572 / bottom 591); `b/shots/t9_p360_one_warn.png`; the two-warn case `b/t8_out.json` "p360 details box with the warns" and `b/shots/t8_p360_warns.png` (the cut line visible at the sheet's bottom). | The sheet's cap (`_obsShell` l.7127-7134: `cap` from the map height; 191 px at a 587-px map) was sized before the "Gridded map" row (20 px) and its wrapped note (another 21 px) were added: 41 px more content at the smallest phone. `overlay.css` `.ov-details { overflow-y: auto }` hides the overflow with no cue. |

No P0, no P1, no P2. The picture is the product's at every one of 1,250 points over 14 views, nothing is drawn where the
frame is absent or over land, the box and its memory behave, the ages flip and hide on the timers, a newer production is
taken in place without a flash, every refusal ends in "unavailable" with nothing drawn, G54's P1 fix holds live, and the
phone row fits with no horizontal scroll.

Observations outside the box's scope (section 81 behaviour met on the way; recorded, not counted):
- O-1: On the Satellite waves entry a Heights -> Swell switch whose swell pointer is refused leaves the error panel with
  a "Retry" only: there is no way back to Heights from it, and `satKind` 'swell' is remembered, so a reload meets the
  same error until the swell feed recovers (the viewer can pick another entry and return, which still restores Swell).
  Pre-existing (section 81). `b/t5_out.json` "P1: back to Heights" ("no Heights button on the error panel").
- O-2: On a sideways phone (812 x 375) the sheet's details box is capped at 85 px and the credit line is below the fold
  with the box off and on (scrollHeight 110 / 130 vs 85): pre-existing for the whole satellite panel; the grid row adds
  20-41 px to it. `b/t6_out.json` land812.
- O-3: A frame past 72 h is still downloaded (35 KB) and kept in the store, although it can never be shown: harmless.

## Held up

### Attack 1: the box (desktop 1280 x 800, `b/t1_box.mjs`, 04:45Z; `b/t1_out.json`, shots `b/shots/t1_*.png`)
- Fresh tab: the page opens on Wave height; `Satellite waves` picked by keyboard -> the panel shows the row "Gridded map"
  with the box OFF, note empty, no request to `altgrid/` at all (0 of them before the tick), session
  `{"field":"sat"}` only.
- Hover over open water with the box off: the readout stays hidden.
- Tick (trusted click at the box's centre, 13 x 13 px at 1280 wide): the note reads "loading…" at once; exactly three
  requests: `altgrid/v1/latest.json` (200, 992 B on the wire, `max-age=300`), the manifest (200, 1,285 B, immutable),
  `hs-18.png` (200, 35,288 B, immutable); the field drawn 508 ms after the click; `layer.field === 'sat'`, the drawn
  entry = the manifest's newest frame file; the note "CMEMS L4 · Nov 3, 08:00 AM HST (35 h ago)" = the page's own
  `fmtTime`/`tzAbbr` of the manifest's `newest_utc` in the page's zone (Pacific/Honolulu) with
  round((now - newest) / 3.6e6) = 35 (34.76 h); the credit line gains ", doi:10.48670/moi-00180"; the focus stays on
  the box through the re-render (`document.activeElement.className === 'ov-sat-box'`).
- Hovers: open water "grid 9.8 ft · Nov 3, 08:00 AM HST"; Honolulu (land): hidden; two more water points 10.9 ft.
- Untick: synchronously after the click `_gridShown() === false`, `layer.hasFrame() === false`, the note '', the readout
  hidden, the store keeps its frame (`status ok`), the credit line loses the third DOI; 0 new requests in the next 1.2 s;
  session `{"field":"sat","satGrid":false}`.
- Tick again: drawn in 814 ms (the coast wait), 0 new requests to `altgrid/`.
- Keyboard: Space on the focused box toggles it off and on; the focus kept.
- Reload: the box comes back ticked and the map drawn; the pointer re-read (200, 281 B on the wire = a 304-style
  revalidation), the manifest and the frame from the browser cache (0 B on the wire).
- A NEW tab starts on Wave height; Satellite waves there: the box off, no `altgrid/` request.
- Console: no errors, no warnings; no exceptions; the only failed requests are the page's own aborted
  `gfswave/0p25/v1/latest.json` reads (canceled on the switch away from Wave height, as before).

### Attack 2: drawn values (`b/t2_values.mjs` + `b/ref_sample.py`, 04:48-04:51Z; `b/t2_out.json`, shots `b/shots/t2_*.png`)
- 14 views x 125 screen points on an 80-px lattice (the panel's corner skipped): Pacific z2.5 (tile zoom 3), Hawaii z4 / z6,
  Maui z8, Atlantic z4 / z6, Southern Ocean z4, Drake Passage z6, the date line z4 (0 N 180) and z6 (Fiji, 179.5 E), the
  Arctic z4 (75 N) and the Mercator clamp z6 (82 N 20 E), the Antarctic ice z5 (72 S), Okhotsk z8: 1,750 probes, 1,251 with
  a readout shown.
- 1,250 of 1,250 grid readouts ("grid N ft …" or "… · grid N ft") equal my own bilinear sample of the published `hs-18.png`
  at the centre of the drawn tile pixel, to the displayed 0.1 ft (the one other shown probe was a CFOSAT dot's text with
  no grid tail where the pixel is clipped by land: right). The page's `layer.readoutAt` and `valueAt` agree with my values.
- 1,250 of 1,250 drawn tile pixels (read from the tile canvases' `ImageData`) have exactly the RGB of the page's `satLut()`
  entry at `round(v * 255 / 18.288)` for MY value, alpha 255 over open water: the picture is the product's.
- 381 probes where my sample is absent (code 0, or the present neighbours' weight under 0.25: the ice edge, the poles'
  rows, the coast band): no readout and the pixel alpha 0 in every case (no colour where the frame is absent). 29 probes
  over land by the tile's land mask: nothing shown. The Antarctic ice view drew 1 pixel of 125 probes (open water at
  its edge).
- The date line: the two views straddling 180 read continuous values on both sides (the columns wrap); the Arctic clamp
  view (82 N) draws only where the product has values; nothing drawn above the world's edge.
- 89 probes with a value under the cursor and no readout: all on the page's own controls (the attribution toggle at the
  bottom left, the station bar `#stationCurrent` and the forecast window's minimise button along y = 760, the top-right
  button row): `overControl()` hides the readout over controls for every field; `document.elementFromPoint` confirmed
  it (`b/t5_out.json`, "elements at the edge points"). Not a grid matter.
- Console clean, no failed requests, no exceptions across the 14 views.

### Attack 3: hover texts (`b/t3_hover.mjs`, 04:52Z; `b/t3_out.json`, shots `b/shots/t3_*.png`)
- Box off, a dot: "16.5 ft · CFOSAT · Nov 3, 06:37 PM HST · model 15.6 ft (+0.9)" (no grid tail).
- Box on, 8 dots at 48.6 N: the same text + " · grid 27.7 ft" etc. (the page's `readoutAt` at the dot, 8.43 m); the grid
  is the 18Z Nov 3 step while the reading is 10 h later, so the difference is the product's, as the owner's sheet says.
- Swell: the field stays under the arrows (same drawn file, same note); a swell point: "Swell 1: 23.4 ft · 15 s · from
  285° (CFOSAT, 35 h ago) · grid 23.9 ft"; two partitions: "Swell 1: … | Swell 2: … (CFOSAT, 13 h ago) · grid 15.4 ft";
  the readings line "Readings: 24 h to Nov 4, 07:23 AM HST (11 h ago) · 2 satellites"; the credit line lists the three
  DOIs. Back to Heights: the field kept.
- Metric (the gear's unit select, keyboard): the legend "0 1 2 3 4 6 9 18+ m", open water "… · grid 2.4 m", a dot "5.0 m ·
  CFOSAT · … · model 4.7 m (+0.3) · grid 8.4 m"; the note unchanged (a time); back to US.
- The gear's time zone: "UTC" -> the note "CMEMS L4 · Nov 3, 06:00 PM UTC (35 h ago)", the readings line and the hover
  times in UTC; Honolulu -> HST; New York -> EST (`b/t5_out.json`: the zone travels through the forecast table's reload
  and lands a few seconds after the change; five arrow presses inside 1.5 s had not landed yet in t3, and had 6 s later
  in t5); Denver -> MST. A forecast dot clicked on the map (a GFS point at 10 N 125 W): the zone becomes `Etc/GMT+8` and
  the note "Nov 3, 10:00 AM UTC−8 (35 h ago)".
- The ">=" label at the scale's top cannot occur with the live frame (max code 108 = 9.63 m; `SAT_FDEF.hi` = 22.86 m): not
  exercised live (Not done).

### Attack 4: ages and refusals (`b/t4_ages.mjs`, 04:53-04:55Z; a faked bucket through `Fetch.requestPaused` on
`models.allshoresurf.com/altgrid/*` only; `b/t4_out.json`, shots `b/shots/t4_*.png`)
- 50.9 h: drawn, the note "CMEMS L4 · Nov 2, 04:00 PM HST (51 h ago)", the warn line "The gridded map is 51 h old.", the
  hover tails present. 48.4 h: "49 h old" warn, drawn. 72.9 h: not drawn, `layer.hasFrame() === false`, the note
  "hidden: 73 h old", the warn "The gridded map is 73 h old: hidden."; the frame still fetched and kept in the store
  (3 requests, 35 KB) although it can never be shown — harmless.
- A 503 pointer: the note "unavailable", nothing drawn, one request; box off / on: one forced retry (503 again),
  "unavailable"; the store `failed`. The retry after the 60-s cooldown: attack 7 (a).
- A manifest with `fields.hs.hi` 30: the pointer and the manifest fetched, the frame NOT fetched, "unavailable", nothing
  drawn (never mis-coloured). A 404 frame: "unavailable". A 1 x 1 PNG frame: "unavailable". Only the browser's own
  "Failed to load resource" lines in the console (503 / 404), no exceptions.
- The newer production taken in place: with the 30-h step drawn (grid 8.0 ft at 15 N 140 W), a pointer naming run
  20261104 and a 12-h step (the real `hs-12.png` served under the new key) on the 12-min timer's callback
  (`_satRecheck()`): pointer -> manifest -> frame fetched; across 72 animation frames the layer was never without a
  frame (`blank 0`), `clear()` called 0 times, `setFrame()` once, the field 'sat' throughout, one grid layer on the map;
  the readout became "grid 7.7 ft", the note "CMEMS L4 · Nov 4, 06:00 AM HST (13 h ago)". No flash, no second field.
- An older run (20261102) on the next recheck: the pointer read, nothing else, the drawn file unchanged.
- The same production republished under a new manifest key: the pointer and the manifest read, the frame not fetched,
  `setFrame` 0 / `clear` 0, the store's manifest key updated (G54-6 live).
- The 30-min timer's callback with the clock moved: 48 -> 49 h keeps the map; +25 h (73 h): the map comes off, the
  readout hidden, the note "hidden: 73 h old", the warn "The gridded map is 73 h old: hidden." (and the readings' own
  27-h warning, right for the moved clock).

### Attack 5: interplay (`b/t5_interplay.mjs`, 04:55-04:57Z; `b/t5_out.json`, shots `b/shots/t5_*.png`)
- Satellite waves + box -> Wave height: the layer is 'hs' with its own frame, no "Gridded map" row, `_wantGrid()` false;
  playback runs (frame 11 -> 17). Back to Satellite waves: the map back at once, 0 new `altgrid/` requests, the note
  kept. -> SST: the layer 'sst'; back: 0 requests. -> Off: no grid layer on the map, the panel empty, the session keeps
  `satGrid: true`; back: drawn, 0 requests. Wave height playback afterwards: frame 18 -> 22 in 2 s, the layer's LUT is
  not the sat LUT object; `_hi` 22.86 / legend [0, 18.288] are Wave height's own (the same scale by design).
- The legend panel, the gear panel, the tools panel opened over the field: the field stays. A live-buoy marker clicked:
  the field stays, the note unchanged. A forecast dot clicked: the forecast window loads it, the zone follows (above).
- G54's P1 live: the box on with the grid frame held back at the network, Heights -> Swell with the swell pointer refused
  (503): the error panel "Overlay unavailable: satellite readings cannot be loaded right now [Retry]"; the frame then
  released and decoded (the store `ok` with the entry): `layer.field` null, `hasFrame()` false, `_wantGrid()` false —
  nothing drawn under the error panel. Fixed as recorded.
- Attribution while drawn: "Overlay: Copernicus Marine · GSHHG" (the clip's link); under the error panel "Overlay:
  Copernicus Marine" only.

### Attack 6: phones (`b/t6_phone.mjs`, `b/t8_followups.mjs`, `b/t9_dottap.mjs`, 04:58-05:04Z; mobile + touch emulation,
DPR 2; `b/t6_out.json`, `b/t8_out.json`, `b/t9_out.json`; shots `b/shots/t6_*.png`, `t8_p*`, `t9_*`)
- 375 x 812: the panel is the bottom sheet (compact, `pointer: coarse`, no hover); the row "Gridded map" 307 px wide in a
  309-px details box, the box 20 x 20 px (the coarse-pointer rule), the note wraps UNDER the label (the row 20 -> 41 px
  high, the note one line of 216 px); `scrollWidth` 375 = `innerWidth` before and after the tick, after the warn and after
  a touch pan (no horizontal scroll); the credit line visible (details 178 of 260 px); with the 49-h warn (the clock
  moved) both the credit and the warn visible (224 of 260 px), `collapsed` false.
- 360 x 640: the row fits (292 px), the note wraps, no horizontal scroll; the credit visible; the warn: B-3.
- 812 x 375 (sideways): the sheet, the row fits (744 px), the note beside the label; no horizontal scroll; the details
  box capped at 85 px (O-2).
- Touch: a quick tap on open water shows nothing, a long press shows "grid 8.8 ft · Nov 3, 08:00 AM HST" (B-2); a quick
  tap on a dot shows "4.5 ft · SARAL · Nov 3, 07:11 PM HST · model 5.9 ft (-1.4) · grid 5.7 ft" for 4 s then hides; a
  touch pan hides the readout and the field follows. The fold toggle folds the sheet (the box and note nulled, the map
  stays drawn) and unfolds with the box still ticked.
- The box ticked by a tap: drawn; the sat panel's own 16-h readings warning appears with the moved clock (right).
- Console clean on all three sizes.

### Attack 7: cost (`b/t7_cost.mjs`, `b/t8_followups.mjs`, 04:59-05:02Z; desktop 1280 x 800; `b/t7_out.json`, `b/t8_out.json`)
- The first tick: 3 requests, 37,277 B on the wire (pointer 763 + manifest 1,235 + frame 35,279, all 200, none from the
  cache); on a reload the pointer alone travels (281 B: a revalidation), the manifest and the frame come from the cache.
- Heap (`performance.memory` after `HeapProfiler.collectGarbage`): 34.1 MB before the tick, 43.6 MB after (41.2 MB after
  the pans' GC): the frame's 259,920-code array plus one 256-KB `ImageData` per drawn tile (reused per tile). After the
  untick 41.2 MB (the tiles keep their buffers, the store its frame); 20 toggles in 3 s: 41.4 MB, 0 requests, one grid
  layer on the map.
- Redraw of a pan at z5 (two trusted drags of 1,131 px, 33 new tiles): box off 2.6 ms of `_draw` in all (empty tiles);
  box on 52 ms in all = 1.6 ms per tile (the coast clip `_landFor` 0.6 ms, the sampling `tileCodes` 0.4 ms, the compose
  the rest); no animation frame over 50 ms in either case (max 42 vs 47 ms); a wheel zoom in and out with the field: 44
  tiles in 83 ms, two frames over 50 ms (max 92 ms, the zoom animation). The failed store's retry: after a 503 the
  12-min recheck's `load(false)` is blocked by the 60-s cooldown (`b/t4_out.json` "C recover", 20 s after the failure:
  nothing fetched) and goes through once the cooldown has passed (`b/t7_out.json` (a), the clock +61 s: pointer ->
  manifest -> frame, drawn, "CMEMS L4 · Nov 3, 12:00 PM HST (31 h ago)").
- Console: only the browser's own "Failed to load resource" lines for the faked 503 / 404; no exceptions in any run; no
  failed request other than the page's own aborted `gfswave` pointer reads on the switch away from Wave height.

### The scale's ends (`b/t8_followups.mjs`, a modified copy of `hs-18.png` served through Fetch: a 9 x 9 block of code
255 at 30 N 150 W and an 8 x 8 block of code 1 at 20 N 140 W)
- Over the 255 block: "grid ≥ 75.0 ft · Nov 3, 01:00 PM HST" (US), "grid ≥ 22.9 m · …" (Metric); the tile pixel's RGB =
  the LUT's top entry [76, 29, 149]. Over the code-1 block: "… · grid 0.0 ft" on a dot there, the pixel = the LUT's
  first entry [29, 58, 138]. `b/shots/t8_01_ge_block.png`.

## Not done

- The real 12-min and 30-min timers were not waited for: their callbacks (`_satRecheck`, `_checkRun`) were called as the
  timers would, and the clock moved with `Date.now` replaced in the page; `Emulation.setVirtualTimePolicy` not used.
- The 48-h and 72-h flips on the LIVE data (due 2026-11-05 18:00Z and 11-06 18:00Z) and the next real production
  (~07:30Z) were not observed; both were exercised with a faked pointer / manifest through `Fetch.requestPaused`.
- The 1920 x 1080 desktop size was not run (1280 x 800 only); the three phone sizes were.
- The production site (allshoresurf.com, flag off) was not opened for the "no Gridded map row" comparison.
- The live buoy window: a buoy marker was clicked and the field stayed, but the window's own element was not identified
  (the selector guess returned null), so "the live window over the field" rests on the screenshot `t5_05_live_window.png`.
- Drawn values were checked against the newest frame only (`hs-18.png`); the other three steps are never drawn.
- No check of the product itself against Copernicus (the job's side, G53).
- Safari / iOS and a real slow link: not available to this harness.
- The harness's own PIDs and profile folders are listed in `b/pids.txt`; all stopped and deleted at 05:05Z (none of the
  owner's 13 Chrome processes touched).

## Counts
P0 0 · P1 0 · P2 0 · P3 3 (B-1 the warn line's stale hour count; B-2 the sheet's "tap on water" vs the site's long press;
B-3 the warn line half below the 360 x 640 sheet) · 3 observations outside the box's scope (O-1..O-3). Review finished
2026-11-05 05:06Z; the harness closed, its profiles deleted, the test origin's storage cleared at the end of every run.

## Fix round (desktop session, Opus 5.5, overlay asset 2.24.2)

| # | Outcome |
|---|---|
| B-1 | FIXED: `_renderSat` keeps the warning element (`ui.gridWarn`); `_gridLineSync` rewrites its text in place from `_gridWarnText()`, so its hour count follows the note between renders. Test (sat.test.js, the 49 -> 56 h case) fails without the line. |
| B-2 | DOCS: the owner's sheet now says a LONG PRESS on open water reads the grid (the site's touch rule for every field); a quick tap on a dot shows its reading with the grid tail. A tap-to-read over water is the owner's call; not built. |
| B-3 | FIXED: the warning line sits right under the "Gridded map" row (before the readings line, the key and the credit), so it is the first thing below the box and visible in a 360-px sheet's details box. Test: the newest render orders the box, the warning, then "Readings:" and the credit. |

Suites after the fixes: `tests/overlay/sat.test.js` 15 / 15; the whole client-tests list (34 files) 1,102 / 1,102;
`tests/test_overlay_flag.py` 17 / 17 (asset pin re-made for 2.24.2).
