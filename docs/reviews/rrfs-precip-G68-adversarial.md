# G68 adversarial review: RRFS Precipitation overlay (commit f33980f, section 102 A2, asset 2.29.0)

Reviewed: `git show f33980f` on branch `feat/rrfs` (worktree `C:\Users\joshm\wave-app-rrfs`): `static_overlay/overlay.js`,
`app.py` (RRFS_PRECIP), `templates/index.html` (menu + FIELDS), `tests/overlay/rrfs_precip.test.js` (new),
`tests/overlay/bootstrap.test.js`, `tests/overlay/overlay.test.js`, `tests/test_overlay_flag.py`, the asset pin.
Line numbers are those of `overlay.js` at f33980f.

## Test runs (unmodified tree)

- `node --test tests/overlay/<file>.test.js` for all 17 test files in `tests/overlay`: every exit code 0 (bathy, bathy_v2,
  bootstrap, coast, flow, isobars, lake, lakeoutlines, overlay, panel, playback, radar, rrfs, rrfs_precip, sat, sst,
  sst_timers).
- `python -m pytest tests/test_overlay_flag.py -q -p no:cacheprovider`: 23 passed (119 s).
- Extra scenario runs (scratch only, not committed): the playback harness of `tests/overlay/playback.test.js`
  (lines 1-225, with its RRFS frame stub widened to `precip` keys) driving the real `Overlay` through precip mounts and
  switches. Results are quoted under the findings they support.

## What holds (checked, no finding)

- Sampler arithmetic. `sampleRegionsTyped` (2225-2262) reuses `sampleRegions`' per-column precompute
  (same `regionLon`, same `C1 = cc + 1 < cl ? cc + 1 : cc` edge clamp, same row clamp), so the last row / column and the
  Alaska date-line columns sample exactly as the wind does. The wet value is `2 + 50 * bt + acc / w` where `acc / w` is a
  weighted mean of levels in [0, 49], so a pixel can never leave its type's code range (no 51.6 / 101.9 values exist;
  the maximum is `2 + 50t + 49`, and float noise such as 51.000000000000014 still rounds to LUT index 102 and
  `precipOf` clamps the level). Values strictly between 1 and 2 are never produced (0, exactly 1, or >= 2).
- LUT. `precipLut` has `2 * 251 + 2 = 504` entries; the largest index used is `round(251 * 2) = 502`; index 2 is the
  dry tint, 0-3 otherwise transparent; the half-code entries between types (e.g. 103 = value 51.5) are never sampled and
  would still map to the lower type's top level.
- Readout = drawn pixel. `readoutAt`'s typed branch (3260-3264) calls the same sampler with the tile's own `p.px / p.py`
  at the tile zoom, so its value equals `tileCodes` at that pixel (the new test checks five points; mutant M11 below is
  killed).
- Cache keys. `_key` (8200-8210) puts `this.field` into every regional bundle key and `manifest.signature` too, so a
  wind bundle can never be served for precip (or the reverse) and a manifest change can never reuse a bundle; `rcache`
  keys are `model + '/' + fileKey` and the precip file keys (`.../precip/fNNN.png`) differ from the wind ones; `_adopt`
  and `_dropRun` clear both caches. Scratch run: wind -> precip -> wind -> precip on one RRFS manifest drew the right
  field each time (`layer._typed` true / false / true), the precip region frame came back from `rcache` (same object),
  and a burst `mount('wind'); mount('precip'); mount('wind')` while loading ended on wind with `_typed === false`.
- `_typed` follows `setFrame` (set from `fdef.kind`) and `clear()`; a wind frame after precip resets it (test + scratch).
- Look, particles, isobars. Precip is not `wind`, so `_syncLook` puts the imagery back and turns the blend off;
  `DIR_FIELDS.precip` and `PRES_UNDER.precip` are undefined, so `_ensureFlow` / `_ensureIso` detach the animator and the
  isobars (scratch: `o.flow === null`, `o.iso === null` under precip). No coastline is drawn over precip
  (`_masked()` is false: precip is in neither CLIP_FIELDS nor OUTLINE_FIELDS), and no GSHHG credit is added.
- `Overlay.prototype._lut`: the precip key `'precip|' + PRECIP_LOOK` cannot collide with `field|legend` or `sst|...`
  keys; PRECIP_LOOK is fixed per page load.
- Model gating. `modelOffers` is called only from `setModel` (guarded by `mode !== 'obs'`), `_leaveModelFor`, `mount`
  (after the `hasObs` early return) and `_modelChoices` (whose result is unused in obs mode). No observation entry
  (sst, sat, radar, bathy) and no lake path calls it, so the new explicit GFS / ECMWF lists `['hs', 'tp', 'wind']` change
  nothing for existing fields. Scratch: GFS hs -> Precipitation switched to RRFS (`_modelChoices()` = `['rrfs']`) and
  Precipitation -> hs switched back to GFS; a tab saved as `{model: 'rrfs', field: 'precip'}` restores and draws.
- Page. `PRECIP` requires both the flag and `'rrfs'` in MODELS; the option and `FIELDS.precip` exist only then, so a
  saved `precip` with the flag off (or without RRFS) starts Off (bootstrap test).
- Manifest. `precipDefOk` matches the job's `PRECIP_DEF` (`tools/model_frames/rrfs.py` 148-156: lo 0.1, hi 75.0 ->
  JSON 75, per_type 50, dry 1, the five ranges, resolutions `['full']`); a non-exact def leaves the wind offered and sets
  `precipBad` (test + scratch: mount says "precipitation frames unreadable", Wind speed still loads).
- Attribution: `MODEL_DEFS.rrfs.attribution` does contain `10-m wind`, so the replace changes the title text.

## Findings

### P0

None. Nothing in the commit changes what the live RRFS wind or the GFS / ECMWF fields draw; the asset bump is the only
cross-cutting change.

### P1

None found.

### P2

**P2-1. Precipitation is stuck on "no precipitation frames in this update yet" while a newer manifest with precip is
announced; Retry never takes it.** `overlay.js` 7488 (mount's precip throw) with 8787 / 8800 (`_loadManifest`) and
8183-8188 (`_retry`).
Scenario: the viewer is on RRFS Wind speed (a frame on the map, so `frameIndex !== null`), on a manifest whose regions do
not name `precip` (any hour where no used source has a precip block: a source built before section 102, a source whose
precip block was dropped, a stale-source fallback). The next hourly manifest has precip; `_checkRun` announces it
(`newerRun`). The viewer picks Precipitation: `mount('precip')` gets the pinned manifest from `_loadManifest`
(`frameIndex !== null` keeps the run pinned and only sets `newerRun`, 8800), throws "no precipitation frames in this
update yet". The error panel has no Update button, and Retry runs the same path: `frameIndex` is still the wind's index
(mount never clears it, `_fail` only clears the picture), so `_loadManifest` returns the same pinned manifest forever.
Reproduced in the scratch harness: first error, Retry, and Retry after forcing the pointer re-read window
(`pointerAt = 0`) all end `error: no precipitation frames in this update yet`, `manifest.signature` unchanged,
`frameIndex` 84, nothing drawn. Only Off -> Precipitation (unmount clears `frameIndex`, so the `_loadManifest` wrapper
adopts `newerRun`) or Wind speed -> Update escapes it. The message itself tells the viewer to wait for an update that
the page then refuses to take.
Fix: when the manifest in hand lacks `fields.precip` and `this.newerRun` is set, let go of the pin for this mount (set
`this.frameIndex = null` / mark a free carry before `_loadManifest`, as `_retry` does for the carried-time case with
`_carry.free`), or have `_retry` free the pin when the last error was the precip one. Add a playback-harness test for it.

**P2-2. No end-to-end test of the precip field through the controller.** `tests/overlay/rrfs_precip.test.js` tests the
pure helpers and a hand-built `ModelGridLayer`; nothing drives `Overlay.mount('precip')`, the model switch to / from
RRFS, the precip / wind bundle keys, the blank `nofield` step and its word, `precipBad` at mount, `update()` under
precip, or the readout's typed text path (`_bindReadout` 9608). The playback harness already serves RRFS manifests and
frames (`tests/overlay/playback.test.js` 87-110, 204-225; its frame stub only needs `precip` in the
`(wind|wdir)` alternation). The mutation run below shows the gap. Two survivors would put WRONG COLOURS on every precip
tile and still pass every test: M20 (drop the precip branch of `Overlay.prototype._lut`, 8410, so the layer gets
`buildLut('precip', [0.1, 75])`, a linear rain ramp indexed by code) and M23 (`_drawTile` calls `composeTile` instead of
`composeTyped`, 3334, so typed codes go through the linear legend mapping). Nothing draws a precip tile through
`_drawTile`, and nothing builds the LUT through the controller. Also surviving: the mount error text (M19), the `nofield`
count (M12), the `_regionWord` precondition (M13), the attribution replace (M16), half-level rounding into the LUT (M10).
Fix: add playback-harness tests for (a) wind -> precip -> wind on one RRFS manifest (fetched URLs, `_typed`, keys,
`o._lut() === precipLut()` contents), (b) GFS hs -> precip -> hs, (c) a step without precip keys (`_regionWord` =
"no precipitation frame for this hour"), (d) `precipBad` mount message with Wind speed still loading, (e) P2-1,
(f) `_attribution()` under precip; plus one `_drawTile` call on a fake canvas checking a known wet pixel's RGBA against
`precipLut()` at `round(v * 2)`, with a fractional level such as 24.6 (kills M10, M20, M23).

### P3

**P3-1. US readout says "0.00 in/h" where it is raining.** `precipAmountText` 334-337: levels 0 to about 1.8 (0.10 to
0.127 mm/h) round to `0.00 in`, so the readout reads "0.00 in/h rain" / "0.00 in/h snow (water)" on drawn, coloured
pixels; the new test pins exactly this (`rrfs_precip.test.js` 257). Fix: `inch < 0.005 ? '< 0.01 in'` (or three
decimals under 0.01).

**P3-2. The legend's "+" tick sits at 84 % of the bar.** `PRECIP_TICKS` 332 / `buildPrecipLegend` 10267-10281: the last
tick is labelled "1+ in/h" (US) or "25+ mm/h" (Metric) but is placed at its own amount (precipLegPos(25.4) = 0.836,
precipLegPos(25) = 0.834) and right-aligned there (`ov-last`), while the bar continues to 75 mm/h (about 3 in/h). The
"N+" reads as "the end of the scale" but colours keep changing to its right. Fix: label the bar's end ("3+ in/h",
"75+ mm/h") at pos 1, or drop the "+" from the 1 in / 25 mm tick.

**P3-3. "≥ 75 mm/h" covers 70-75 mm/h.** `precipReadout` 343 marks level 49 as "≥ 75 mm/h" (US "≥ 3.0 in/h"), but the
job's `precip_level` rounds, so level 49 holds every amount from about 70.1 mm/h up
(0.1 * 750^(48.5/49)). Fix: say "≥ 70 mm/h" for the top level, or accept as wording.

**P3-4. Update to a manifest without readable precip leaves the old picture under a generic error.** `update()` 8900:
`!m.fields.precip` throws `layer "precip" is not in run <run>` (scratch: error state with the old precip frame still
drawn, because `_fail` only clears when `frameIndex === null`). The mount path has the precip-specific wording; Update
does not. Fix: reuse the mount's message (`precipBad` -> "precipitation frames unreadable").

**P3-5. A malformed `frames[i].precip` refuses the whole manifest, wind included.** `validateRegionalManifest`
2301-2304 throws `bad frame entry` for an unknown region or a key outside the prefix in the precip map, while a bad precip
DEF only drops precip (`precipBad`). Today the job cannot write such keys (`_usable_precip` regions are a subset of the
wind regions; one template), so it is fragility, not a live break; but it contradicts the "wind kept" rule the def check
follows, and the deployed client (which ignores `precip`) is more tolerant than this one. Fix: on a bad precip map set
`precipBad = true` and `delete fr.precip` instead of throwing (the security property, no key outside the prefix ever
fetched, is kept).

**P3-6. Precipitation then Wind speed leaves a GFS-wind viewer on RRFS wind.** `_leaveModelFor` (7423) moves GFS wind
-> RRFS for Precipitation; back on Wind speed RRFS offers wind, so the model stays RRFS: outside the four regions the map
is blank with "outside RRFS". Same rule as section 99 D2 (deliberate), noted because Precipitation will be picked by
viewers far from the RRFS regions. Fix (optional): remember the model left by `_leaveModelFor` and return to it when
the next field is offered by it.

**P3-7. `models` without RRFS gives a misleading message.** With `models = ['gfswave', 'ecmwf']`, `mount('precip')`
falls back to DEFAULT_MODEL and fails with "no precipitation frames in this update yet" (scratch). The page never offers
Precipitation then (PRECIP needs RRFS in MODELS), so this is defence in depth only. Fix: in `mount`, when no offered
model offers the field, fail with "precipitation needs RRFS".

**P3-8. Test assertions that cannot fail.** `rrfs_precip.test.js` 246 `readoutAt(45, -100, z) === null, 'no tile
there'`: the point is outside the synthetic storm grid, so the sampler returns 0 whether or not a tile exists (typed
`readoutAt` does not look at `this._tiles` at all). The independent reference (`refOne`) copies the client's 0.25
thresholds and first-of-equals tie rule, but no fixture puts a pixel exactly on a 0.25 weight or a top-weight tie, so the
documented rules are not pinned (mutants M1, M2, M2b survive). The storm test's "hole of missing nodes"
(`storm()` rows > rows - 6, i.e. south of 38.3 N) lies outside the sampled tile (38.8-41.0 N), so the dry-vs-missing rule
at a hole's edge is never exercised: M4 (`cov >= 0.25` -> `cov > 0`, dry spreading over missing data) survives; the
"none" pixels the test counts all come from east of the grid box. The `odd` precip-def list has no `lo` edit (M7b
survives). Fix: move the hole inside the tile (or the tile onto the hole); add a 2 x 2 grid sampled at
`fc = fr = 0.5` (all weights 0.25: one wet node -> wet; rain NW vs snow SE -> rain by "first of equals"); add
`(d) => { d.lo = 0.2; }` to `odd`.

## Mutation testing

Each mutant was one exact string replacement in `static_overlay/overlay.js` (applied only when the original occurred once).
For each one, `node --test` ran on `rrfs_precip`, `rrfs`, `overlay`, `playback` and `panel` (exit codes checked
directly), and the file was then restored with `git checkout -- static_overlay/overlay.js`. The asset-pin Python test was
not run on mutants. After the run, `git status --short` shows only this report (untracked).

| # | Mutant | Result | Killed by |
|---|---|---|---|
| M1 | wet rule `w >= 0.25` -> `w > 0.25` (2259) | SURVIVED | - |
| M2 | type tie NW: `wa > best` -> `wa >= best` | SURVIVED | - |
| M2b | type tie SE: `wd > best` -> `wd >= best` | SURVIVED | - |
| M3 | dry branch (`cov >= 0.25` -> 1) removed | killed | rrfs_precip |
| M4 | dry rule `cov >= 0.25` -> `cov > 0` | SURVIVED | - |
| M5 | precipDefOk: `dry` check dropped | killed | rrfs_precip |
| M6 | precipDefOk: type-count check dropped | killed | rrfs_precip |
| M7 | precipDefOk: `resolutions` check dropped | killed | rrfs_precip |
| M7b | precipDefOk: `lo` check dropped | SURVIVED | - |
| M8 | regionFrameKey precip branch removed (precip read from `files`) | killed | rrfs_precip |
| M9 | GFS `fields` list removed (GFS offers precip) | killed | rrfs_precip, overlay |
| M9b | ECMWF list without `wind` | killed | overlay, playback |
| M10 | composeTyped `Math.round(c * 2)` -> `Math.floor(c * 2)` | SURVIVED | - |
| M11 | readoutAt typed branch removed (falls to valueAt at z20) | killed | rrfs_precip |
| M12 | `nofield` inverted (counts regions WITH the key) | SURVIVED | - |
| M13 | `_regionWord`: any `nofield` (not all regions) | SURVIVED | - |
| M14 | PRECIP_K without `+ 1` (level 0 not wet) | killed | rrfs_precip |
| M15 | precip offered despite `precipBad` | killed | rrfs_precip |
| M16 | attribution replace removed | SURVIVED | - |
| M17 | LUT dry tint at index 0-1 instead of 2 | killed | rrfs_precip |
| M18 | `clear()` keeps `_typed` | SURVIVED | - (near-equivalent: with `_frame` null the typed readout returns null and the next setFrame resets it) |
| M19 | mount's precip error line removed (generic `layer "precip" is not in this run`) | SURVIVED | - |
| M20 | `_lut` precip branch removed (linear `buildLut('precip')`) | SURVIVED | - (wrong colours on every tile) |
| M21 | readout `>= hi` -> `> hi` | killed | rrfs_precip |
| M22 | tileCodes typed -> untyped `sampleRegions` | killed | rrfs_precip |
| M23 | `_drawTile`: `composeTile` instead of `composeTyped` | SURVIVED | - (wrong colours on every tile) |
| M24 | manifest precip-key prefix check dropped | killed | rrfs_precip |
| M25 | precipLut alpha floor `RADAR_ALPHA[1]` | killed | rrfs_precip |
| M26 | `_leaveModelFor` takes the LAST offering model | killed | playback |

Totals: 29 mutants, 16 killed, 13 survived (one of them, M18, near-equivalent). The pure helpers (code table, def check,
LUT entries, readout words) are well pinned; the controller wiring (LUT dispatch, tile composition, mount messages, the
`nofield` word, attribution) is not (P2-2), and the sampler's boundary rules (0.25 thresholds, ties, the missing-node
edge) are not (P3-8).

## Summary

| Severity | Count | Items |
|---|---|---|
| P0 | 0 | - |
| P1 | 0 | - |
| P2 | 2 | P2-1 Precipitation stuck on a pinned pre-precip manifest (Retry never adopts the announced one); P2-2 no controller-level tests (M20 / M23 wrong-colour mutants survive) |
| P3 | 8 | "0.00 in/h"; "N+" tick mid-bar; "≥ 75" covers 70-75; Update error wording; malformed precip map refuses the wind too; GFS wind -> Precipitation -> Wind stays on RRFS; no-RRFS message; vacuous / missing test fixtures |

## Fix round (2026-11-19, the building session)

| Finding | Outcome |
|---|---|
| P2-1 Precipitation stuck on "no precipitation frames in this update yet" while a newer manifest is announced | FIXED: mount releases the pin (carries the shown time, `_carry.free`) when Precipitation meets a regional manifest without the field and `newerRun` is set, so that mount adopts the newer one. Test "G68 P2-1 ... the newer one is adopted" (playback.test.js). |
| P2-2 no controller-level precip tests | FIXED: six controller tests in playback.test.js (RRFS frame regex now serves `precip`): mount from GFS wind, LUT and credit, wind <-> precip bundles, the model return, steps without the field (all regions and one region), the P2-1 adoption, unreadable precip at mount and at Update, a restored tab. A whole tile through `_drawTile` in rrfs_precip.test.js (every pixel = composeTyped with the precip LUT). |
| P3 "0.00 in/h" for 0.10-0.12 mm | FIXED: under 0.0095 in three decimals ("0.004 in/h"). |
| P3 legend "1+ in/h" / "25+ mm/h" mid-bar | FIXED: the last tick is a plain value with the unit ("1 in/h", "25 mm/h"). |
| P3 "≥ 75 mm/h" covers ~70 up | FIXED: the top level reads its lower edge, "≥ 70 mm/h" / "≥ 2.8 in/h". |
| P3 update() to unreadable precip: generic message | FIXED: "precipitation frames unreadable" / "no precipitation frames in this update". |
| P3 malformed `frames[i].precip` refuses the whole manifest | FIXED: it marks precip unreadable (`precipBad`); the wind stays. |
| P3 GFS wind -> Precipitation -> Wind speed stays on RRFS | FIXED: `_autoFrom` remembers the model before the FIRST automatic switch; a later field that model offers goes back to it; the viewer's own model choice clears it. |
| P3 models list without RRFS: misleading message | RECORDED: the page offers Precipitation only with RRFS among the models (bootstrap test). |
| P3 the "no tile there" assertion cannot fail; M4 | FIXED: the storm fixture's hole of missing nodes now lies inside the tested tile; a pixel inside it is blank with no readout. |
| M13 "no precipitation frame" only when every region lacks it | CHANGED: judged per region (the region under the pointer has no key for the step); tested with one region missing. |

Survivors after the round, re-run with a runner that restores the file's own text: M1 (`w > 0.25`), M2 / M2b (tie `>=`) are
equivalent in practice (a pixel centre at exactly a quarter weight or an exact tie has measure zero on the Mercator grid);
every other survivor of the review is killed (M4, M7b, M10, M12, M16, M18, M19, M20, M23 by the new tests; M13 by the
per-region test).

Note: the review's mutant runner restored overlay.js with `git checkout`, which also dropped the uncommitted fix round
once; it was re-applied and the runner now writes the original text back.
