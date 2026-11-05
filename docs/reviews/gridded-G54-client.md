# G54: the gridded satellite wave-height map, the client (plan section 82, step 3) — adversarial code review record

Reviewer: one fresh agent (Fable 5.1, reviewer A: the code), 2026-11-05, on `feat/gridded` @ 2b5aa28 against production
06a965f: `static_overlay/overlay.js` (+ `overlay.css`), `app.py` (`_grid_overlay_enabled`, `_overlay_context`),
`templates/index.html` (`var GRID`, `grid: GRID`), `tests/overlay/sat.test.js`, `tests/overlay/bootstrap.test.js`,
`tests/test_overlay_flag.py`, `tests/capture_index_golden.py`. Method: the diff and the surrounding code read end to end
(`GridStore` against `SstStore` / `SatStore`, `_showGrid` against `_loadSst`, the readout's `show()`, `_fail`,
`_mountObs` / `_leaveObs` / `unmount`, `_coastFirst` / `_coastLate`, `ModelGridLayer.setFrame` / `clear` / `readoutAt`);
the suites run (`node --test tests/overlay/sat.test.js tests/overlay/bootstrap.test.js`: 41 pass; `python3 -I -m pytest
tests/test_overlay_flag.py`: 16 pass, 1 pre-existing failure, see the note at the end); the LIVE pointer, manifest and
newest frame read once from the public bucket and fed through the module's own `validGridPointer`, `validGridManifest`,
`decodePngGrey` and `validateGrid` in the Node sandbox (`scratchpad/revG54/live_check.js`); thirteen scratch tests in
`scratchpad/revG54/test_revG54.js` (the suite's sandbox helpers sliced into `helpers.js`), of which the five F-tests each
PASS while the defect they name is present and the eight C-tests confirm what is fine; the suite re-run with the wall
clock three days ahead (`shim_future.js`); and a battery of 17 hand mutants of `overlay.js` applied to a scratch copy
with the suite run against each (`mutants.js`: 14 survive, 3 killed). Scratch root:
`/tmp/claude-0/-home-user-wave-app/ddff7080-5763-58b4-b5ba-6c83938be3ca/scratchpad/revG54/`.

| # | Sev | Finding | Evidence | Fix |
|---|---|---|---|---|
| 1 | P1 | **The gridded map is drawn under the error panel.** `_wantGrid()` is true whenever the mode is `obs`, the field `sat` and a layer exists, whatever the panel shows. With the box on and the grid's manifest or frame still on its way, a Heights -> Swell switch whose pointer fails (`setSatKind` -> `store.load(true)` null -> `_fail`) detaches the readings, clears the timer and shows "Overlay unavailable"; the grid load then lands, `_syncGrid`'s `.then` finds `_wantGrid()` true and `_showGrid` draws the Copernicus map: a picture with no readings over it, under a panel that says the overlay is unavailable, with no box to turn it off (the error panel has none), no 12-min re-check (`satTimer` is gone), and only the 30-min `_checkRun` ever hiding it (at 72 h). `_fail`'s own rule ("the own entry failed: no dots under the error panel") is broken for the map. | `test_revG54.js` F1 (`last.state === 'error'`, `satLayer === null`, `layer.hasFrame() === true`, field `sat`, no `ui.gridBox`); the same sequence printed step by step in `dbg_f1.js` | `_wantGrid`: also require `this.satLayer` (the readings attached: `_loadSat` attaches before `_syncGrid`, `_fail` / `_detachSat` / `_leaveObs` drop it), so a load landing after a failure draws nothing; a frame already drawn is cleared by `_fail` today (`frameIndex === null && layer.hasFrame()`), which stays right |
| 2 | P2 | **The two new controller tests are wall-clock bombs.** Their fixture's newest step is `2026-11-03T18:00Z` and the draw path (`_showGrid` -> `_gridState()` -> `ageH()` -> `Date.now()`) runs at the REAL clock inside `until(() => ov.layer.hasFrame())` (the suite's `atAge` pins the clock only around a synchronous body). From 2026-11-06T18:00Z the step is past `GRID_HIDE_H`, `_showGrid` clears instead of drawing, and `controller: the "Gridded map" box ...` and `controller: the gridded map's age ...` fail: CI turns red on both branches in about 38 h, before the merge gate of step 6. | `node -r scratchpad/revG54/shim_future.js --test tests/overlay/sat.test.js` (the clock +3 days): 12 pass, 2 fail (tests 12 and 13); the same suite at today's clock: 14 pass | `sat.test.js`: pin the clock across the whole async body (an `atAge` that restores `Date.now` in a `finally` after `await fn()`, as `test_revG54.js` does), or build `gridPointer` / `gridManifest` times from `Date.now()` (e.g. 30 h ago) |
| 3 | P3 | **Test gaps (mutants that survive the suite).** M4: the 12-min wiring `_satRecheck -> _gridRecheck` deleted (the tests call `_gridRecheck` directly); M5: `_checkRun`'s `_gridLineSync` deleted (called directly); M6: `_clearGrid` without its `_gridShown()` guard (nothing checks a model field's picture survives `setSatGrid(false)`); M12: `_wantGrid` without `this.layer`; M1: `cache: 'no-cache'` dropped from `GridStore._json` although the test's title says "(no-cache)" (the sandbox records URLs only); M2 / M3: `frames.length > 4` and `typeof fr.step` unchecked; M7: `recheck` adopting an older `run`; M8: the "same manifest -> resolve in place" branch removed (every 12-min load re-fetches the manifest and the frame); M9: `_showGrid`'s own hide check removed; M17: `newer` never cleared on success (the 12-min gate bypassed for ever); M16: `'ov-sat-box'` out of `FOCUS_PARTS` (the sandbox's `querySelector` returns null: focus is untestable there). Killed: M11 (`CLIP_FIELDS.sat`), M13 (the hide text), M14 (`abortAll` dropping the frame). | `mutants.js` output: 14 SURVIVES / 3 killed | `sat.test.js`: drive the timer callbacks (`_satRecheck`, `_checkRun`) as `test_revG54.js` C1 does; a model-field world asserting `layer.clear` is never called (C4); a fetch stub that records the options for the no-cache claim; the rejection list extended (C6); a pointer re-read within the 12 min asserting no manifest fetch (C5) |
| 4 | P3 | **The hover readout stays on screen after the map is cleared.** `_clearGrid` clears the layer but not `this.readout`; a cursor resting over water shows "grid 8.2 ft · …" after the box is turned off or after the 72-h hide, until the next mouse move. `_dropRun` and `_fail` hide it in the same situation. | `test_revG54.js` F3 | `_clearGrid`: `if (this.readout) this.readout.hidden = true;` |
| 5 | P3 | One toggle of the box renders the panel twice when the age state differs from the panel's (the box turned on with the step already past 48 h: `_syncGrid` -> `_gridLineSync` renders the warn line, then `setSatGrid` renders again). Harmless (the same panel twice, the focus kept) but wasteful on a phone. | `test_revG54.js` F5 (1 render at a fresh age, 2 at 50 h) | `setSatGrid`: render first, then `_syncGrid(true)` (the render sets `_gridWarnShown`, so `_gridLineSync` finds no change), or skip the second render when `_gridWarnShown` already equals `_gridState()` |
| 6 | P3 | A manifest republished under a new key with the SAME newest frame file re-downloads and re-decodes the identical frame and redraws the map. `load()` compares the manifest key only (`was.manifest === ptr.manifest`) and `_showGrid` dedups by entry object identity. The live bucket shows the case today: production `20261104T073010Z` is served by `manifest-20261105T020207Z.json` (re-published 02:02Z, the job's `--force` re-uploading identical bytes to the same production keys); every reader with the step in hand fetched the 35 KB frame again and redrew. | `test_revG54.js` F6 (`_decode` called twice for the same URL); the live pointer / manifest in `live/` | `GridStore.load`: when `was` and `entry.file === this.entry.file`, take the new pointer / manifest / entry without decoding (keep `this.frame`); `_showGrid`: dedup by `entry.file` |
| 7 | P3 | On every change the pointer is read twice: `recheck()` reads it (no-cache) and notes `newer`, then `load(false)` reads it again before the manifest. `SstStore` has the same shape, so this is consistent, but the plan's "load() then takes it in place" suggested starting from `newer`. | `test_revG54.js` F7 (two `latest.json` fetches per change) | `load`: when `this.newer` is set and `force` is false, start from it (validated again) instead of `_pointer(sig)` |
| N | nit | `_gridReadout` names the STORE's entry (`this.grid.entry.valid_utc`), not the drawn one (`this.layer.entry`): between a newer manifest's decode and its `setFrame` (one microtask after the first load, up to `COAST_FIRST_MS` only on a first draw) the hover could pair the old value with the new time — unproven in practice. The sat panel shows no coast note when the coastline failed (the map then unclipped; the model panels show `coastNote`); the product is blank near coasts anyway. `recheck()` runs with no signal or watchdog (as SST): a hung read piles up one request per 12 min, harmless. The sat test title "every rejection" covers 9 of the 14 manifest rules. | — | wording / small changes |

No P0: the picture is the product's (the live frame decodes to the right grid, poles absent, land absent, 4.59 m
mid-Pacific), never mis-coloured (the field range is pinned to `SAT_FDEF`) and never under another field's label.

Out of scope, noted for the fix round: `tests/test_overlay_flag.py::test_overlay_module_unit_tests` fails on the
branch because `tests/overlay/isobars.test.js` passes its 12 tests and then never exits (a lingering handle; the
file-level 20-s timeout cancels it). It does the same at production 06a965f (checked in a scratch extract with the
whole `tests` tree), and the diff touches neither the file nor the isobar code.

## Fix round (2026-11-05, Opus 5.5; overlay asset 2.24.1)

| # | Fix |
|---|---|
| 1 | `_wantGrid` also requires `this.satLayer`: a grid load landing after the entry failed draws nothing under the error panel. |
| 2 | The two controller tests run on a pinned clock (`Date.now` = the fixture's newest step + 31 h, restored in `finally`); the suite passes with the wall clock 3 days ahead (`shim_future.js`). |
| 3 | A new test (G54) pins: the 12-min `_satRecheck -> _gridRecheck` wiring, the 30-min `_checkRun -> _gridLineSync` wiring (readings kept fresh so only the grid can move the panel), `cache: 'no-cache'` on the wire (the sandbox's fetch now records options), > 4 frames, a non-numeric step, `recheck`'s run order, the manifest in hand, `newer` cleared, `_clearGrid`'s guard, `FOCUS_PARTS` (exported), the 48 / 72-h boundaries (strictly past), the draw refusing a hidden step. The reviewer's battery: 13 of 16 applicable mutants killed before the new pins; after them only M9 (the draw's first hide check: a second identical check runs after the coast wait) and M15 (the SST-source reset: every observation mount already clears it) survive, both behaviour-equivalent; M12 targets a line the fix changed. |
| 4 | `_clearGrid` hides `this.readout`. |
| 5 | `setSatGrid` syncs quietly (`_syncGrid(force, quiet)` -> `_gridLineSync(quiet)`) and renders once. |
| 6 | `GridStore.load`: a new manifest whose newest frame file is the one in hand keeps the entry and the frame (no download, no redraw). |
| 7 | `GridStore.load` takes the pointer `recheck()` noted (`newer`) as is: one read per change. |
| nit | `_gridReadout` names the drawn step (`layer.entry`) when the map is shown. |
| new | Found while fixing 5: with the store mid re-read (status `loading`), turning the box on waited for the re-read; `_syncGrid` now draws the frame in hand at once (it stays valid until a newer one lands). |

The reviewer's proofs (`scratchpad/revG54/test_revG54.js`): F1, F3, F5, F6, F7 now FAIL (the defects gone); C1-C8 pass.

## Checked and fine

- **The live product through the client's rules** (`live_check.js`, 2026-11-05 04:13Z): the pointer (692 B, `max-age
  300`, so `no-cache` revalidates at the edge) passes `validGridPointer`; the manifest (1,971 B, immutable; grid =
  `GRID_HALF` key by key, `fields.hs` = `SAT_FDEF`, four ascending frames under the prefix) passes `validGridManifest`;
  the newest frame (34,901 B, immutable) decodes through `decodePngGrey` to 720 x 361, passes `validateGrid`, holds
  143,046 present cells (55.0 %, the job's own count), max code 108 = 9.63 m, both pole rows empty; through a
  `ModelGridLayer` it reads 4.59 m at 40 S 150 W, 1.89 m at 0 N 180, `null` in the Sahara. The caps (64 KB pointer and
  manifest, `MAX_FRAME_BYTES` for the frame) are far off.
- **Races.** The entry left while `_showGrid` waits for a loading coastline (SST, a model field, Off): nothing is drawn
  under the other label, the pending wait finds `_wantGrid()` false; back on `sat` the frame is drawn at once and
  clipped, the GSHHG link appended (C2). The box toggled off and on while a load is in flight: one draw, no second
  fetch, `load()` hands back the one promise (C3). A re-check landing with the box off adopts in the store only; the
  next On draws the new production without a fetch (C3). Heights | Swell mid-load keeps the frame (the suite). Unmount
  mid-load: `abortAll` aborts, the load owns nothing (`loadAbort !== ctrl`), the store ends `idle`, a remount asks
  afresh; `abortAll` on a re-read keeps the frame and `ok` (C8, the suite). A superseded load can never land
  (`self.loadAbort !== ctrl || sig.aborted` -> `abortError`).
- **Never another field's picture.** `_clearGrid` acts only on `layer.field === 'sat'` with a frame (C4: a wave-height
  layer's `clear` never called by `setSatGrid` / `_syncGrid` / `_clearGrid`); `_mountObs` and `_leaveObs` clear the
  layer on every entry change; `_showGrid` drops an SST source (`setSource(null)`) and the contours before `setFrame`;
  `CLIP_FIELDS.sat` clips, `LAKE_UNDER` leaves the lake mask off, `_checkRes` is inert under `obs` (`frameIndex` null),
  the opacity is `FIXED_OPACITY.hs`'s 0.65, the pane `modelPane` (z 250) under the dots' `obsPane` (260).
- **The store's status machine.** `idle -> loading -> ok | failed | idle`; `was` (an `ok` store with a frame) keeps the
  step through a failed or timed-out re-read (`ok`, `loadedAt` refreshed); a first failure `failed` with a 60-s cooldown
  for automatic loads only (`force` from a viewer's action tries at once); the watchdog is re-armed by every step and
  every body chunk; `newer` is set only for a different manifest of a `run >=` the one in hand (C5: an older run noted
  as nothing) and cleared on success; a 12-min load whose pointer names the manifest in hand fetches nothing more (C5).
- **48 / 72 h with no user action** (C1, driven through the real timer callback `_satRecheck`): at 50 h the warn line
  "The gridded map is 50 h old." appears and the map stays; at 73 h the map comes off, the note reads "hidden: 73 h
  old", the warn line "The gridded map is 73 h old: hidden."; a fresher day on the next tick is taken in place (the
  old step stays until the new one is decoded), drawn, and the lines follow (49 h: the warn line again, correctly).
  `_gridLineSync` runs every 12 min (`_gridRecheck`'s unchanged pointer) and every 30 min (`_checkRun`'s sat branch);
  `document.hidden` skips the 12-min read as it does the readings'. No render loop: `_renderSat` records the state it
  rendered (`_gridWarnShown`) and `_gridLineSync` renders only on a change.
- **Validation** (the suite + C6): the pointer's format, `complete`, 8-digit `run`, manifest key under `altgrid/v1/`
  with `sstKey`'s charset and no `..`, parseable `newest_utc`; the manifest's format, `run` match, `complete`, the
  encoding, the half grid key by key (an extra key such as `derivation` passes, right), the field range (another code
  space refused, never mis-coloured), 1..4 frames with strictly ascending times, a numeric `step`, every file under the
  prefix (a frame key with `..` refused); the pointer document itself as a manifest refused; the decoded frame checked
  against the grid (`validateGrid`) after `decodeFrame`'s own `{720, 361}` expectation.
- **The readout.** A dot or swell point under the cursor gets " · grid x ft" only while the box is on and the map is
  drawn (`_wantGrid() && _gridShown()`); open water with no reading shows "grid x ft · <time>" (≥ at the scale's top);
  land and absent cells give nothing (`readoutAt`'s clip mask, and the frame's code 0 there); the box off shows nothing
  on the entry (the suite); model fields keep their own text (C4: "5.9 ft", no "grid"); the value is the drawn pixel's
  (`readoutAt`, the tile's land mask), as the fields. Touch: the long-press shows it, a plain tap only on a mark (as
  before).
- **Focus and accessibility.** The box is `input.ov-sat-box` inside `label.ov-sat-label` (its name "Gridded map"),
  `aria-describedby="ovGridNote"` on the note; `'ov-sat-box'` is in `FOCUS_PARTS`, so the toggle's re-render gives the
  focus back to the new box (`_refocus` by class); the fold nulls `gridBox` / `gridNote` so a folded panel is never
  written to; the coarse-pointer rule makes the box 20 px. One `ovGridNote` per page (one host at a time).
- **The page flag.** `_grid_overlay_enabled` needs `ALT_OVERLAY` and `ALT_GRID`; `var GRID` and `grid: GRID` live in
  the gated block only (`MODEL_OVERLAYS` unset: no `var GRID`, the golden untouched); the on / off pages are equal
  modulo the three vars (`test_sat_overlay_env_and_the_page`); GRID without SAT builds no store and no row (C7); a
  saved `satGrid` without the setting starts off (the suite).
- **The credit.** The panel's credit line adds `doi:10.48670/moi-00180` while the box is on; the map credit's title
  lists the three DOIs always; the GSHHG link appears when the map is clipped. The Swell readings line reads
  "Readings: 24 h to <time> (<age> ago) · n satellites" (the suite).
- **Tests that do pin what they claim**: the newest step and the half grid (`_decode` URL and `expect`), the
  cooldown and the forced retry, the failed re-read keeping the frame, `abortAll` keeping it, the off-by-default box
  and its save key, the field layer's `field` / `lut` / `fdef` / pane, the hover texts through the map's own
  `mousemove` handler, the credit line with and without the DOI, Off clearing and the store keeping, SST clearing,
  the warn and hide lines at 49 / 73 h, the new production adopted in place, "unavailable" on a 503, `CLIP_FIELDS.sat`
  (killed M11), the hide text (M13), `abortAll` (M14).
